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
const schema = require('./audit-schema');
const astate = require('./audit-state');

const CACHE_TTL_MS = 30 * 1000;
const _cache = { at: 0, users: null, assignments: null, classifications: null };

// The capture configuration is read on EVERY append, so it is cached on the
// same short window as the RBAC state. A stale read here can at worst
// record thirty seconds of events during a pause that has just begun — the
// opposite error, dropping events during a pause that has just ended, is
// prevented by resolveState() interpreting the timestamp rather than
// trusting the flag.
const AUDIT_CONFIG_KEY = 'audit/config';
const _auditCfg = { at: 0, value: null };

// How long a quiet period has to be before the next authenticated request
// counts as the start of a new working session. Ten minutes is the existing
// last-seen refresh window; reusing it means the session event costs the same
// single write that was already happening, rather than a new one.
const SESSION_GAP_MS = 10 * 60 * 1000;

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
    if (!user.lastSeenAt || Date.parse(now) - Date.parse(user.lastSeenAt) > SESSION_GAP_MS) {
      const gapFrom = user.lastSeenAt || null;
      user.lastSeenAt = now;
      store.setJSON('rbac/users', { users: state.users }).catch(() => {});
      // ── The nearest honest thing to a sign-in event ────────────────────
      //
      // The brief asks for sign-in, sign-out and failed sign-in in the
      // always-on `security` category. Two of those three cannot be
      // observed here and pretending otherwise would put a fiction in the
      // evidence chain:
      //
      //   A FAILED sign-in never reaches this code. Authentication is Entra
      //   External ID's; a caller who fails it has no token, so no function
      //   of ours is invoked and there is nothing to record. Recording it
      //   would need Entra's own sign-in logs, which is a different
      //   integration, not a line of code here.
      //
      //   A SIGN-OUT is a client-side MSAL call plus a cleared cache. There
      //   is no request to observe, and a browser closed mid-session makes
      //   no call at all — so a "signed out" entry would be absent exactly
      //   when it mattered.
      //
      // What IS observable is this: an authenticated request arriving after
      // a gap, which is the first request of a working session. It is named
      // for what it is rather than for what it resembles.
      appendAudit(store, {
        actorOid: oid, actorEmail: authed.email, effectiveRoles: [],
        action: 'auth.session.start', category: 'security',
        outcome: 'allowed', severity: 'info',
        summary: 'First authenticated request after a gap of at least '
                 + (SESSION_GAP_MS / 60000) + ' minutes',
        detail: { lastSeenAt: gapFrom, derivedFrom: 'lastSeenAt gap, not an identity-provider event' },
      }).catch(() => {});
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

// ── Capture configuration ─────────────────────────────────────────────────
//
// One record: the state (recording/paused/off) and the settings under it.
// Held in the same blob store as the chain it governs, so there is no
// second system that can be up while this one is down.
async function loadAuditConfig(store, { fresh = false } = {}) {
  if (!fresh && Date.now() - _auditCfg.at < CACHE_TTL_MS && _auditCfg.value) return _auditCfg.value;
  const raw = await store.get(AUDIT_CONFIG_KEY, { type: 'json' }).catch(() => null);
  const cfg = raw ? { ...astate.defaultConfig(), ...raw } : astate.defaultConfig();
  // Re-apply the always-on lock on every read. Stored state that claims
  // `security: false` — an older version, a bad migration, a hand-edited
  // blob — must not be able to switch security auditing off just by
  // existing.
  cfg.settings = astate.normaliseSettings(cfg.settings);
  _auditCfg.value = cfg;
  _auditCfg.at = Date.now();
  return cfg;
}

async function saveAuditConfig(store, cfg) {
  const next = { ...cfg, settings: astate.normaliseSettings(cfg.settings) };
  await store.setJSON(AUDIT_CONFIG_KEY, next);
  _auditCfg.value = next;
  _auditCfg.at = Date.now();
  return next;
}

function invalidateAuditConfig() { _auditCfg.at = 0; }

// ── Append-only audit (Section 10) ────────────────────────────────────────
const pad = (n) => String(n).padStart(10, '0');

// Every append also writes a compact projection into a per-month index.
// readAudit() costs one blob GET per entry, so filtering a year of events by
// actor and category that way is thousands of round-trips inside a
// 26-second function. Queries read the index; only the rows actually shown
// are fetched whole.
//
// The index is a cache, not the record. It is rebuilt from the chain if it
// is ever lost, and a failure to write it never fails the append — losing
// the search index costs a slow query, losing the entry costs the evidence.
async function appendIndex(store, entry, seq) {
  const key = schema.indexKeyFor(entry.occurredAt);
  try {
    const page = (await store.get(key, { type: 'json' })) || { rows: [] };
    page.rows.push(schema.indexRow(entry, seq));
    await store.setJSON(key, page);
  } catch (e) {
    console.error('[org-store] audit index write failed for ' + key + ': ' + e.message);
  }
}

// `evt` may be either the flat shape every existing caller uses
// (actorOid/actorEmail/effectiveRoles/action/…) or an entry already built by
// audit-schema.buildEntry. Anything without an `id` is run through
// buildEntry here, which is what gives the RBAC events that predate this
// module their category, their index row and their redaction without a
// single call site changing.
//
// Options:
//   required   throw instead of returning null if the write is lost. Used
//              for PROD writes, where the brief is explicit that if the
//              audit write fails the action fails with it — an unrecorded
//              Production change is worse than a refused one.
//   internal   this append is itself part of resolving the capture state,
//              so do not re-enter that resolution. Without it, writing the
//              audit.resume that ends an expired pause would try to resolve
//              the expired pause again, forever.
async function appendAudit(store, evt, opts) {
  opts = opts || {};
  const built = evt && evt.id && evt.occurredAt
    ? evt
    : schema.buildEntry(
        { ...evt, environment: evt.environment || null },
        {
          actor: {
            oid: evt.actorOid || null, email: evt.actorEmail || null,
            name: evt.actorName || null, roles: evt.effectiveRoles || [],
          },
          tenantId: (evt.detail && evt.detail.tenantId) || null,
          route: (evt.detail && evt.detail.route) || null,
          source: 'server',
        });

  if (!opts.internal) {
    const decision = await captureDecision(store, built.category);
    if (!decision.record) {
      return { dropped: true, reason: decision.reason, category: built.category };
    }
  }

  for (let attempt = 0; attempt < 3; attempt++) {
    const head = (await store.get('audit/head', { type: 'json' })) || { seq: 0, hash: '' };
    const seq = head.seq + 1;
    const entry = rbac.chainEntry(head.hash, { seq, ...built });
    await store.setJSON('audit/e/' + pad(seq), entry);
    // Confirm the head has not moved beneath us before advancing it.
    const check = (await store.get('audit/head', { type: 'json' })) || { seq: 0, hash: '' };
    if (check.seq === head.seq) {
      await store.setJSON('audit/head', { seq, hash: entry.entryHash });
      await appendIndex(store, entry, seq);
      return entry;
    }
    await store.delete('audit/e/' + pad(seq)).catch(() => {});
  }
  // Fail open on the WRITE only: a lost audit write must not block the
  // user's action after it was authorised — but say so in the logs. The one
  // exception is a caller that passed `required`, where the whole point is
  // that the action does not survive an unrecorded one.
  console.error('[org-store] audit append lost after retries: ' + built.action);
  if (opts.required) {
    const e = new Error('the audit record could not be written, so the action was refused');
    e.statusCode = 503;
    e.auditFailure = true;      // callers word their 503 from this
    throw e;
  }
  return null;
}

// Resolve the capture state against the clock, persist an expired pause and
// write its audit.resume, then decide whether this category survives.
//
// The resume is written here rather than by a scheduled function on
// purpose: a scheduler is a moving part that can fail silently, and between
// the expiry and its next tick the stored state is a lie. Resolving lazily
// at write time (and again at read time) means there is no instant at which
// the log believes it is paused after the pause has run out.
async function resolveCapture(store) {
  const cfg = await loadAuditConfig(store);
  let resolved = astate.resolveState(cfg, Date.now());

  if (resolved.expired) {
    const saved = await saveAuditConfig(store, {
      ...cfg, state: 'recording', pausedUntil: null,
      reason: null, changedBy: 'system', changedAt: new Date().toISOString(),
    });
    await appendAudit(store, schema.buildEntry({
      action: 'audit.resume', category: 'audit', outcome: 'allowed', severity: 'notice',
      actorType: 'system',
      summary: 'Capture resumed automatically — the pause expired',
      detail: { pausedUntil: resolved.pausedUntil, pauseReason: resolved.reason,
                pausedBy: resolved.changedBy },
    }, { actor: { oid: 'system', email: 'system', roles: [] }, source: 'server' }),
    { internal: true }).catch(() => null);
    resolved = astate.resolveState(saved, Date.now());
  }
  return resolved;
}

async function captureDecision(store, category) {
  const resolved = await resolveCapture(store);
  return astate.shouldRecord(category, resolved, resolved.settings);
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

// ── Filtered queries (the Events tab) ─────────────────────────────────────
//
// Reads the monthly index rather than the chain, then fetches whole entries
// only for the rows actually on the page. The alternative — walking
// audit/e/<seq> backwards and filtering in memory — is one blob GET per
// event, which a year of events turns into an operation that cannot finish
// inside a function timeout.
//
// FALLBACK: entries written before the index existed have no index rows, so
// an empty index is not proof of an empty log. When no index page covers
// the window, this walks the chain the old way and says so in `indexed:
// false`, which the UI shows rather than hides — a list that silently omits
// the first year of a deployment's history is worse than a slow one.

function monthKeysBetween(fromIso, toIso, maxMonths) {
  const start = new Date(fromIso);
  const end = new Date(toIso);
  const keys = [];
  let y = start.getUTCFullYear(), m = start.getUTCMonth();
  for (let i = 0; i < (maxMonths || 24); i++) {
    const d = new Date(Date.UTC(y, m, 1));
    if (d > end) break;
    keys.push('audit/idx/' + d.toISOString().slice(0, 7));
    m++; if (m > 11) { m = 0; y++; }
  }
  return keys;
}

function rowMatches(r, f) {
  if (f.fromMs && Date.parse(r.ts) < f.fromMs) return false;
  if (f.toMs && Date.parse(r.ts) > f.toMs) return false;
  if (f.actor && String(r.a || '').toLowerCase() !== f.actor) return false;
  if (f.category && r.c !== f.category) return false;
  if (f.action && r.ac !== f.action) return false;
  if (f.outcome && r.o !== f.outcome) return false;
  if (f.env && r.e !== f.env) return false;
  if (f.source && r.s !== f.source) return false;
  if (f.projectId && r.p !== f.projectId) return false;
  // Target rows written before `tg` existed fall back to the general search
  // text. That over-matches slightly on old history — a target filter could
  // catch a row whose ACTION contained the word — and under-matching would
  // be worse: it would make every entry from before this field look as
  // though it had no target at all.
  if (f.target) {
    const hay = r.tg !== undefined ? String(r.tg) : String(r.t || '');
    if (hay.indexOf(f.target) === -1) return false;
  }
  if (f.q && String(r.t || '').indexOf(f.q) === -1) return false;
  return true;
}

// The values the column filters offer. Built from the rows in the current
// WINDOW, not from the page on screen, and not from the user directory:
// "people who appear in this trail" is the useful list, and a dropdown
// limited to the fifty rows currently rendered silently hides the person
// somebody is looking for. Computed from index rows that are already in
// memory, so it costs nothing extra.
function facetsFrom(rows) {
  const actors = new Map(), actions = new Map();
  for (const r of rows) {
    if (r.a) actors.set(r.a, (actors.get(r.a) || 0) + 1);
    if (r.ac) actions.set(r.ac, (actions.get(r.ac) || 0) + 1);
  }
  const top = (m, n) => [...m.entries()]
    .sort((x, y) => y[1] - x[1] || x[0].localeCompare(y[0]))
    .slice(0, n)
    .map(([value, count]) => ({ value, count }))
    .sort((x, y) => x.value.localeCompare(y.value));
  return { actors: top(actors, 100), actions: top(actions, 100) };
}

async function queryAudit(store, opts) {
  const o = opts || {};
  const limit = Math.min(200, Math.max(1, parseInt(o.limit, 10) || 50));
  const now = Date.now();
  const fromMs = o.from ? Date.parse(o.from) : 0;
  const toMs = o.to ? Date.parse(o.to) : 0;
  const f = {
    fromMs: Number.isNaN(fromMs) ? 0 : fromMs,
    toMs: Number.isNaN(toMs) ? 0 : toMs,
    actor: o.actor ? String(o.actor).toLowerCase() : '',
    category: o.category || '', action: o.action || '',
    outcome: o.outcome || '', env: o.env || '', projectId: o.projectId || '',
    source: o.source || '',
    target: o.target ? String(o.target).toLowerCase() : '',
    q: o.q ? String(o.q).toLowerCase() : '',
  };

  const head = (await store.get('audit/head', { type: 'json' })) || { seq: 0 };
  if (!head.seq) {
    return { total: 0, entries: [], indexed: true, nextCursor: null,
             facets: { actors: [], actions: [] } };
  }

  const keys = monthKeysBetween(
    new Date(f.fromMs || (now - 365 * 86400000)).toISOString(),
    new Date(f.toMs || now).toISOString(), 24);
  const pages = await Promise.all(keys.map(k => store.get(k, { type: 'json' }).catch(() => null)));
  const haveIndex = pages.some(Boolean);

  let rows, inWindow;
  if (haveIndex) {
    inWindow = [];
    for (const p of pages) if (p && Array.isArray(p.rows)) inWindow.push(...p.rows);
  } else {
    // Unindexed history. Bounded so it cannot run away: the most recent
    // 1000 entries, which is what the old Users & Roles view showed anyway.
    const scan = await readAudit(store, { limit: 1000 });
    inWindow = scan.entries
      .map((e, i) => schema.indexRow(e, e.seq != null ? e.seq : (head.seq - i)));
  }
  rows = inWindow.filter(r => rowMatches(r, f));

  // Facets come from the window BEFORE the column filters are applied, and
  // only the date range is honoured. Otherwise picking a person would empty
  // the action list of every action that person did not perform, and there
  // would be no way back to the others without clearing the filter you just
  // set — a dropdown that removes its own options as you use it.
  const dateOnly = { fromMs: f.fromMs, toMs: f.toMs };
  const facets = facetsFrom(inWindow.filter(r => rowMatches(r, dateOnly)));

  rows.sort((a, b) => b.seq - a.seq);
  const cursor = o.cursor ? parseInt(o.cursor, 10) : 0;
  const startAt = cursor ? rows.findIndex(r => r.seq < cursor) : 0;
  const slice = startAt === -1 ? [] : rows.slice(startAt, (startAt < 0 ? 0 : startAt) + limit);

  const entries = (await Promise.all(
    slice.map(r => store.get('audit/e/' + pad(r.seq), { type: 'json' }).catch(() => null))
  )).filter(Boolean);

  const consumed = startAt === -1 ? rows.length : startAt + slice.length;
  return {
    total: rows.length,
    chainTotal: head.seq,
    entries,
    indexed: haveIndex,
    facets,
    nextCursor: consumed < rows.length && slice.length ? slice[slice.length - 1].seq : null,
  };
}

// KPI counts for the status row. Index-only — it never fetches an entry —
// so the four tiles cost a handful of blob reads regardless of chain size.
async function auditStats(store, { windowMs = 24 * 3600 * 1000, now = Date.now() } = {}) {
  const since = now - windowMs;
  const keys = monthKeysBetween(new Date(since).toISOString(), new Date(now).toISOString(), 3);
  const pages = await Promise.all(keys.map(k => store.get(k, { type: 'json' }).catch(() => null)));
  const rows = [];
  for (const p of pages) if (p && Array.isArray(p.rows)) rows.push(...p.rows);
  const recent = rows.filter(r => Date.parse(r.ts) >= since);
  return {
    events: recent.length,
    actors: new Set(recent.map(r => r.a).filter(Boolean)).size,
    prodChanges: recent.filter(r => r.e === 'PROD' || r.c === 'prod').length,
    deniedOrFailed: recent.filter(r => r.o === 'denied' || r.o === 'failed').length,
    windowMs, indexed: pages.some(Boolean),
  };
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
  loadAuditConfig, saveAuditConfig, invalidateAuditConfig,
  resolveCapture, captureDecision, queryAudit, auditStats, appendIndex,
  AUDIT_CONFIG_KEY,
};
