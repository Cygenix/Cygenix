// netlify/functions/lib/org-store.js
//
// Organisation-level server-side state for RBAC (spec Section 10, adapted
// from its PostgreSQL schema to the Netlify Blobs store this product
// already runs on — the same store projects.js uses, so no new
// infrastructure is required for Phase 0).
//
// Layout (single org per deployment today; keys leave room for more):
//   rbac/users            { users: { [oid]: { email, name, isActive,
//                                             firstSeenAt, lastSeenAt } } }
//   rbac/assignments      { assignments: [ { id, oid, role, scope,
//                             grantedBy, grantedAt, revokedAt } ] }
//   rbac/classifications  { byKey: { 'server|db': { environment,
//                             classifiedBy, classifiedAt } } }
//   audit/head            { seq, hash }
//   audit/e/<seq 10-pad>  one chained entry (rbac.chainEntry shape)
//
// The audit append is read-head → chain → write-entry → write-head with a
// short retry on head movement. Netlify Blobs has no transactions, so a
// simultaneous append can in principle race the head; at this product's
// traffic (single-digit operators) the retry closes the realistic window,
// and verifyChain reports any break rather than hiding it. Revisit with a
// stronger store when multi-tenant load arrives.
//
// Every read is cached for a short window in the warm Lambda so the RBAC
// gate does not add a blob round-trip to every db-connect call — page
// loads were hard-won fast and stay that way. Writes invalidate the cache.

'use strict';

const { getStore } = require('@netlify/blobs');
const rbac = require('./rbac');

const CACHE_TTL_MS = 30 * 1000;
const _cache = { at: 0, users: null, assignments: null, classifications: null };

function orgStore() {
  const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
  const token  = process.env.NETLIFY_API_TOKEN;
  if (!siteID || !token) {
    const e = new Error('authorisation store unavailable (NETLIFY_SITE_ID / NETLIFY_API_TOKEN not set)');
    e.statusCode = 503;
    throw e;
  }
  return getStore({ name: 'cygenix-org', siteID, token });
}

function invalidate() { _cache.at = 0; }

async function loadAll(store) {
  if (Date.now() - _cache.at < CACHE_TTL_MS && _cache.users) return _cache;
  const [users, assignments, classifications] = await Promise.all([
    store.get('rbac/users', { type: 'json' }),
    store.get('rbac/assignments', { type: 'json' }),
    store.get('rbac/classifications', { type: 'json' }),
  ]);
  _cache.users = (users && users.users) || {};
  _cache.assignments = (assignments && assignments.assignments) || [];
  _cache.classifications = (classifications && classifications.byKey) || {};
  _cache.at = Date.now();
  return _cache;
}

// ── Actor resolution ──────────────────────────────────────────────────────
//
// From a verified token: upsert the user record, bootstrap the first
// signer (spec Section 12 step 1), and resolve effective roles as the
// union of stored assignments and validated token app roles.
async function resolveActor(store, authed, tokenClaims) {
  const oid = authed.oid || authed.sub;
  if (!oid) { const e = new Error('token carries no oid'); e.statusCode = 401; throw e; }
  const state = await loadAll(store);
  let user = state.users[oid];
  let bootstrapped = false;

  const now = new Date().toISOString();
  if (!user) {
    user = { email: authed.email, name: authed.name || '', isActive: true, firstSeenAt: now, lastSeenAt: now };
    const firstSigner = Object.keys(state.users).length === 0;
    state.users[oid] = user;
    await store.setJSON('rbac/users', { users: state.users });
    if (firstSigner) {
      // First signer becomes the organisation: OW+PA so the tenant is not
      // locked out, ML+EN so the working single-operator product keeps
      // working. The collapsed segregation this creates is surfaced, not
      // hidden (SoD-7) — see rbac.collapsedSegregation.
      for (const role of rbac.BOOTSTRAP_ROLES) {
        state.assignments.push({
          id: 'ra_' + Date.now() + '_' + role, oid, role,
          scope: 'organisation', grantedBy: 'bootstrap', grantedAt: now, revokedAt: null,
        });
      }
      await store.setJSON('rbac/assignments', { assignments: state.assignments });
      bootstrapped = true;
      await appendAudit(store, {
        actorOid: oid, actorEmail: authed.email,
        action: 'org.bootstrap', resourceType: 'organisation', outcome: 'allowed',
        severity: 'high', detail: { roles: rbac.BOOTSTRAP_ROLES },
      });
    }
    invalidate();
  } else {
    // Refresh last-seen lazily — at most once per cache window, and never
    // as a blocking write on the request path.
    if (!user.lastSeenAt || Date.parse(now) - Date.parse(user.lastSeenAt) > 10 * 60 * 1000) {
      user.lastSeenAt = now;
      store.setJSON('rbac/users', { users: state.users }).catch(() => {});
    }
  }

  const storedRoles = state.assignments
    .filter(a => a.oid === oid && !a.revokedAt)
    .map(a => a.role);
  const tokenRoles = rbac.rolesFromClaims(tokenClaims || {});
  const roles = rbac.effectiveRoles(storedRoles, tokenRoles, user);

  return { oid, email: authed.email, name: user.name, user, roles, isActive: user.isActive !== false, bootstrapped };
}

// ── Classification lookup (Section 7.1) ───────────────────────────────────
async function classificationFor(store, server, database) {
  const state = await loadAll(store);
  const rec = state.classifications[rbac.connKey(server, database)];
  return rec ? rec.environment : rbac.DEFAULT_ENVIRONMENT;   // unclassified = PROD
}

// ── Append-only audit (Section 10) ────────────────────────────────────────
const pad = (n) => String(n).padStart(10, '0');

async function appendAudit(store, evt) {
  for (let attempt = 0; attempt < 3; attempt++) {
    const head = (await store.get('audit/head', { type: 'json' })) || { seq: 0, hash: '' };
    const seq = head.seq + 1;
    const entry = rbac.chainEntry(head.hash, {
      seq, occurredAt: new Date().toISOString(),
      actorOid: evt.actorOid || null, actorEmail: evt.actorEmail || null,
      effectiveRoles: evt.effectiveRoles || [],
      action: evt.action, resourceType: evt.resourceType || null,
      resourceId: evt.resourceId || null, environment: evt.environment || null,
      outcome: evt.outcome, severity: evt.severity || 'info',
      detail: evt.detail || null,
    });
    await store.setJSON('audit/e/' + pad(seq), entry);
    // Confirm the head has not moved beneath us before advancing it.
    const check = (await store.get('audit/head', { type: 'json' })) || { seq: 0, hash: '' };
    if (check.seq === head.seq) {
      await store.setJSON('audit/head', { seq, hash: entry.entryHash });
      return entry;
    }
    await store.delete('audit/e/' + pad(seq)).catch(() => {});
  }
  // Fail open on the WRITE only: a lost audit write must not block the
  // user's action after it was authorised — but say so in the logs.
  console.error('[org-store] audit append lost after retries');
  return null;
}

async function readAudit(store, { limit = 200, selfOid = null } = {}) {
  const head = (await store.get('audit/head', { type: 'json' })) || { seq: 0 };
  const from = Math.max(1, head.seq - Math.max(limit * (selfOid ? 4 : 1), limit) + 1);
  const keys = [];
  for (let s = head.seq; s >= from; s--) keys.push('audit/e/' + pad(s));
  const entries = (await Promise.all(keys.map(k => store.get(k, { type: 'json' }).catch(() => null))))
    .filter(Boolean);
  const filtered = selfOid ? entries.filter(e => e.actorOid === selfOid) : entries;
  return { total: head.seq, entries: filtered.slice(0, limit) };
}

async function verifyChain(store, { limit = 2000 } = {}) {
  const head = (await store.get('audit/head', { type: 'json' })) || { seq: 0 };
  const from = Math.max(1, head.seq - limit + 1);
  const entries = [];
  for (let s = from; s <= head.seq; s++) {
    const e = await store.get('audit/e/' + pad(s), { type: 'json' }).catch(() => null);
    if (!e) return { ok: false, brokenAt: s, reason: 'entry missing' };
    entries.push(e);
  }
  if (from > 1 && entries.length) {
    // Partial walk: anchor on the first entry's own hash rather than ''.
    const sub = entries.slice(1);
    let prev = entries[0].entryHash;
    for (const e of sub) {
      if ((e.prevHash || '') !== prev) return { ok: false, brokenAt: e.seq, reason: 'prev-hash mismatch' };
      if (rbac.hashEntry(e.prevHash, e) !== e.entryHash) return { ok: false, brokenAt: e.seq, reason: 'entry hash mismatch' };
      prev = e.entryHash;
    }
    return { ok: true, count: entries.length, partial: true };
  }
  return rbac.verifyEntries(entries);
}

module.exports = {
  orgStore, loadAll, invalidate, resolveActor, classificationFor,
  appendAudit, readAudit, verifyChain,
};
