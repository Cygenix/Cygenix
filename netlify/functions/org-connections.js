// netlify/functions/org-connections.js
//
// The organisation's connection register — Phase A of
// docs/design/server-held-connections.md.
//
//   GET  ?includeRetired=1            list the organisation's connections
//   POST { op:'create', connection }  add one (metadata only; no secret)
//   POST { op:'update', id, patch }   rename, alias, or reclassify
//   POST { op:'retire', id }          retire (refused while a profile binds it)
//
// ── Why this is a Netlify function in front of an Azure action ────────────
//
// The data lives in Cosmos, which only the Function App reaches. The ROLES
// live in Netlify Blobs (rbac-admin.js, the Users & Roles page,
// scripts/grant-role.js), which the Function App cannot read. So the
// decision is made here, where the roles are, and the write is made there,
// where the data is: this function verifies the token, resolves the actor
// and the tenant, asks rbac.can(), records the act in the audit chain, and
// only then calls the Function App with the host key and two headers —
// x-user-id (the actor) and x-cygenix-tenant (the tenant). The Function App
// refuses the register actions without that tenant header, and the data
// proxy never sets it, so the browser cannot reach them any other way.
//
// This is the same shape the Phase C grant will generalise: authorisation
// decided where the roles are, enforced where the thing is.
//
// ── What it will not do ───────────────────────────────────────────────────
//
// Hold a secret. The register is metadata. A body carrying a password, a
// connection string or a key is refused by the Function App's validator, and
// nothing here reads such a field. The secret half arrives in Phase B, by a
// write-only endpoint of its own.
'use strict';

const authz = require('./lib/authz');
const rbac  = require('./lib/rbac');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type': 'application/json',
};
const ok   = (body, status) => ({ statusCode: status || 200, headers: CORS, body: JSON.stringify(body) });
const fail = (msg, status, code) => ({ statusCode: status || 500, headers: CORS, body: JSON.stringify({ error: msg, code: code || null }) });

const API_BASE = process.env.CYGENIX_DATA_API_BASE
  || 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/data';
const FN_KEY = process.env.CYGENIX_DATA_FN_KEY || '';

// The profile store's environment classes → the RBAC model's environments.
// PROD is the safe default for anything unclassified, as everywhere else.
function rbacEnv(envClass) {
  switch (String(envClass || '').toUpperCase()) {
    case 'DEV': case 'SANDBOX': return 'DEV';
    case 'TEST': return 'TEST';
    case 'UAT': return 'STAGING';
    case 'PRD': default: return 'PROD';
  }
}

// One hop to the Function App. The caller's Authorization goes with it so
// the Function App's own enforceAuth sees a real token once
// REQUIRE_TOKEN_AUTH is on. 5xx from upstream becomes 502 with the detail in
// band, because Flex Consumption has no log anyone can read.
async function fn(event, ctx, action, method, body, qs) {
  const url = API_BASE + '/' + action + '?code=' + encodeURIComponent(FN_KEY) + (qs ? '&' + qs : '');
  const headers = {
    'Content-Type': 'application/json',
    'x-user-id': ctx.actor.email,
    'x-cygenix-tenant': ctx.tenant.id,
  };
  const bearer = event.headers.authorization || event.headers.Authorization;
  if (bearer) headers.Authorization = bearer;
  let res;
  try {
    res = await fetch(url, { method, headers, body: body ? JSON.stringify(body) : undefined });
  } catch (e) {
    return { status: 502, json: { error: 'Could not reach the Function App: ' + e.message } };
  }
  const text = await res.text().catch(() => '');
  let json = null;
  try { json = JSON.parse(text); } catch { json = { error: text.slice(0, 500) || ('HTTP ' + res.status) }; }
  return { status: res.status, json };
}

// Pass the Function App's answer through on 2xx and 4xx (a 409 lock is an
// answer the page needs verbatim); wrap 5xx as 502 with the message.
function relay(r) {
  if (r.status >= 200 && r.status < 300) return ok(r.json);
  if (r.status >= 400 && r.status < 500) return fail(r.json && r.json.error || ('HTTP ' + r.status), r.status, 'upstream-refused');
  return fail('The register service failed: ' + ((r.json && r.json.error) || ('HTTP ' + r.status)), 502, 'upstream');
}

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };
  if (!FN_KEY) {
    console.error('[org-connections] CYGENIX_DATA_FN_KEY is not set — refusing');
    return fail('CYGENIX_DATA_FN_KEY is not configured for this deployment.', 503, 'no-fn-key');
  }

  let ctx;
  try {
    ctx = await authz.authorize(event, { route: 'org-connections', action: null });
  } catch (e) {
    return authz.errorResponse(e, CORS);
  }
  const { actor, audit } = ctx;

  const denied = async (action, decision, detail) => {
    await audit({ action, outcome: 'denied', severity: decision.severity || 'notice',
                  resourceType: 'connection', resourceId: (detail && detail.id) || null,
                  category: 'connections',
                  detail: { reason: decision.reason, ...(detail || {}) } });
    return fail('Not permitted: ' + decision.reason, 403, 'denied');
  };

  try {
    if (event.httpMethod === 'GET') {
      const d = rbac.can(actor, 'connection.view', {});
      if (!d.allow) return denied('connection.view', d);
      const q = event.queryStringParameters || {};
      return relay(await fn(event, ctx, 'org-connection-list', 'GET', null, q.includeRetired === '1' ? 'includeRetired=1' : ''));
    }

    if (event.httpMethod !== 'POST') return fail('Method not allowed', 405);
    let body = {};
    try { body = JSON.parse(event.body || '{}'); } catch { return fail('Invalid JSON body', 400); }
    const op = String(body.op || '');

    if (op === 'create') {
      const c = body.connection && typeof body.connection === 'object' ? body.connection : {};
      const env = rbacEnv(c.envClass);
      const d = rbac.can(actor, 'connection.create', { mutating: true, environment: env });
      if (!d.allow) return denied('connection.create', d, { name: c.name, envClass: c.envClass });
      const r = await fn(event, ctx, 'org-connection-create', 'POST', { connection: c });
      if (r.status >= 200 && r.status < 300 && r.json && r.json.connection) {
        const rec = r.json.connection;
        await audit({ action: 'connection.create', outcome: 'allowed', severity: d.severity,
                      category: 'connections', environment: env,
                      resourceType: 'connection', resourceId: rec.id,
                      summary: 'Created organisation connection ' + rec.name + ' (' + rec.kind + ', ' + rec.side + ', ' + rec.envClass + ')',
                      detail: { id: rec.id, name: rec.name, side: rec.side, kind: rec.kind, envClass: rec.envClass,
                                server: rec.server, database: rec.database, endpoint: rec.endpoint, userName: rec.userName } });
      }
      return relay(r);
    }

    const id = String(body.id || '').trim();
    if (!id) return fail('id is required', 400);

    if (op === 'update') {
      const patch = body.patch && typeof body.patch === 'object' ? body.patch : {};
      // The environment is the existing record's, because that is what the
      // edit is being made against; reclassifying is its own permission.
      const cur = await fn(event, ctx, 'org-connection-get', 'POST', { id });
      if (!(cur.status >= 200 && cur.status < 300)) return relay(cur);
      const existing = cur.json.connection || {};
      const env = rbacEnv(existing.envClass);
      const action = ('envClass' in patch) ? 'connection.classify' : 'connection.edit';
      const d = rbac.can(actor, action, { mutating: true, environment: env });
      if (!d.allow) return denied(action, d, { id, patch: Object.keys(patch) });
      const r = await fn(event, ctx, 'org-connection-update', 'POST', { id, patch });
      if (r.status >= 200 && r.status < 300) {
        await audit({ action, outcome: 'allowed', severity: d.severity, category: 'connections', environment: env,
                      resourceType: 'connection', resourceId: id,
                      summary: (action === 'connection.classify' ? 'Reclassified ' : 'Edited ') + (existing.name || id),
                      detail: { id, changed: (r.json && r.json.changed) || Object.keys(patch),
                                before: Object.fromEntries(Object.keys(patch).map((k) => [k, existing[k]])),
                                after: patch } });
      }
      return relay(r);
    }

    if (op === 'retire') {
      const cur = await fn(event, ctx, 'org-connection-get', 'POST', { id });
      if (!(cur.status >= 200 && cur.status < 300)) return relay(cur);
      const existing = cur.json.connection || {};
      const env = rbacEnv(existing.envClass);
      const d = rbac.can(actor, 'connection.retire', { mutating: true, environment: env });
      if (!d.allow) return denied('connection.retire', d, { id });
      const r = await fn(event, ctx, 'org-connection-retire', 'POST', { id });
      if (r.status >= 200 && r.status < 300 && !(r.json && r.json.alreadyRetired)) {
        await audit({ action: 'connection.retire', outcome: 'allowed', severity: d.severity, category: 'connections', environment: env,
                      resourceType: 'connection', resourceId: id,
                      summary: 'Retired organisation connection ' + (existing.name || id),
                      detail: { id, name: existing.name, kind: existing.kind, side: existing.side, envClass: existing.envClass } });
      } else if (r.status === 409) {
        // A refused retirement is worth a line: it is the lock rule doing
        // its job, and somebody may need to know who still holds it.
        await audit({ action: 'connection.retire', outcome: 'denied', severity: 'notice', category: 'connections',
                      resourceType: 'connection', resourceId: id,
                      summary: 'Retire refused: ' + ((r.json && r.json.error) || 'locked'),
                      detail: { id, reason: 'locked' } });
      }
      return relay(r);
    }

    return fail('Unknown op: ' + (op || '(none)'), 400);
  } catch (e) {
    console.error('[org-connections]', e);
    return fail(e.message || String(e), 500);
  }
};
