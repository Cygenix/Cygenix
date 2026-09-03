// netlify/functions/data-proxy.js
//
// THE ONE AUTHENTICATED DOOR TO /api/data.
//
// WHAT THIS FIXES
//
// The Azure Function that holds every user's projects, jobs, connections and
// subscription record derived the caller's identity from a request header:
//
//     function getUserId(req) { return req.headers.get('x-user-id') || ... }
//
// A header is whatever the caller says it is. The only thing standing in
// front of it was the Function App's host key — which was hard-coded into
// six files served to every browser, and committed to the repository. Read
// the page source, copy the key, set the header to somebody else's email,
// and their data was yours to read or overwrite. `requireAdmin` then read
// the role from the record belonging to that same spoofable id, so the
// bypass reached the admin endpoints too.
//
// The Function App already has Entra verification (azure-function/src/
// entra-auth.js) but its rollout is two-phase and phase 2 never landed,
// because the front end never started attaching a token. So enforcement
// could not be switched on without breaking every sync call.
//
// HOW THIS CLOSES IT
//
// The browser no longer talks to the Function App. It calls this proxy with
// its Entra ID token and nothing else. This function:
//
//   1. verifies the token — signature against Entra's JWKS, audience,
//      issuer — using the same verifier every other Netlify function uses;
//   2. derives the identity from the VERIFIED claims, never from the request;
//   3. calls the Function App with the host key read from the environment,
//      setting x-user-id to that verified identity;
//   4. forwards the caller's Authorization header, so the Function App's own
//      enforceAuth sees a real token and REQUIRE_TOKEN_AUTH can finally be
//      turned on without breaking anything.
//
// Any x-user-id the caller sends is dropped on the floor. There is no code
// path here that reads an identity from the request body, the query string
// or a header.
//
// WHAT THIS DOES NOT FIX, AND CANNOT
//
// The two host keys that were published are still valid until somebody
// rotates them in Azure. Anyone who copied one can still call the Function
// App directly and spoof x-user-id until BOTH of these are done:
//
//   * rotate the host keys in the Function App (Portal → App keys), and
//   * set REQUIRE_TOKEN_AUTH=true in the Function App settings, which makes
//     azure-function/src/entra-auth.js reject any request without a valid
//     token instead of falling back to the header.
//
// Neither is a code change. Removing the keys from the client stops NEW
// exposure; only rotation revokes what is already out.

'use strict';

const { verifyAuthHeader } = require('./lib/entra-auth');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, x-anthropic-key',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
  'Content-Type': 'application/json',
};
// Errors carry a machine-readable `code` alongside the prose. The client
// cannot act sensibly on "Auth error: …" as a string — it needs to know
// whether this deployment is misconfigured (nobody can fix it by retrying,
// and it says nothing about whether the user has data) or the token expired
// (sign in again) or the far side fell over (retry later). Codes are stable;
// the prose is not.
const fail = (msg, status, code) => ({
  statusCode: status || 500,
  headers: CORS,
  body: JSON.stringify({ error: msg, code: code || 'error' }),
});

// The Function App base and key both come from the environment. A missing
// key is a deployment fault and says so, rather than silently falling back
// to an embedded default — an embedded default is how the last one leaked.
const API_BASE = process.env.CYGENIX_DATA_API_BASE
  || 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/data';
const FN_KEY = process.env.CYGENIX_DATA_FN_KEY || '';

// Actions the proxy will forward. An allow-list rather than a pass-through:
// this function holds a privileged key, so what it can be pointed at is a
// decision made here and not by the caller. Adding an action is deliberate.
const ALLOWED = new Set([
  // Sync + identity
  'load', 'save', 'ping', 'whoami', 'subscription', 'user-create',
  // Job versioning
  'versions', 'version-list', 'version-create', 'restore-version',
  // Billing
  'checkout-status', 'usage', 'billing-portal',
  // Reporting
  'report', 'reports', 'project-summary-document',
  // Profiling
  'profile-build', 'profile-list', 'profile-save', 'profile-delete',
  'profile-load', 'profile-classify-subjects',
  // Data quality
  'quality-add-rel', 'quality-delete-rel', 'quality-list-rels', 'quality-suggest-rels',
  'quality-get-report', 'quality-list-reports', 'quality-save-report',
  'quality-rename-report', 'quality-delete-report',
  'quality-load-scope', 'quality-save-scope', 'quality-group', 'quality-review',
  // Administration — gated again on the Function App by requireAdmin, which
  // is only trustworthy now that the identity reaching it is a verified one.
  'admin-users', 'extend-membership', 'delete-account',
  // 'invite' is gone: it called Netlify Identity's GoTrue admin endpoint,
  // which made that a second identity provider. Invitations are issued by
  // the product now — see netlify/functions/lib/tenancy.js.
  'audit', 'waitlist-list',
]);

// `waitlist` is deliberately absent: it is the one genuinely anonymous
// action, it is served straight from the register page, and routing it
// through a function that demands a token would break sign-ups.

// The agentive-migration page calls a second family of routes on the same
// Function App, behind a second host key that was also embedded in the
// client. Same problem, same fix — but a path rather than an action, so it
// is allow-listed by shape: /me exactly, or anything under /agent/.
// Traversal is rejected outright rather than normalised.
const AGENT_ROOT = String(API_BASE).replace(/\/data\/?$/, '');
function agentPathAllowed(p) {
  if (typeof p !== 'string' || !p.startsWith('/')) return false;
  if (p.includes('..') || p.includes('//')) return false;
  const bare = p.split('?')[0];
  return bare === '/me' || bare === '/narrative' || /^\/agent(\/[A-Za-z0-9_.-]+)*$/.test(bare);
}

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };

  const q = event.queryStringParameters || {};
  const action = q.action || '';
  const agentPath = q.path || '';

  // ── health ──────────────────────────────────────────────────────────────
  // Deliberately unauthenticated and deliberately first, before the key check
  // and before token verification. It answers one question — is this
  // deployment configured well enough to reach the Function App — which is
  // exactly the question nobody could answer during the three-week outage.
  //
  // It reports the PRESENCE of configuration, never its content: no key, no
  // fragment of a key, no key length. `apiBase` is a public hostname that is
  // already in this file as a literal and in every network tab.
  if (action === 'health') {
    const ok = !!FN_KEY;
    return {
      statusCode: ok ? 200 : 503,
      headers: CORS,
      body: JSON.stringify({
        ok,
        code: ok ? 'ok' : 'no-fn-key',
        // The one thing an operator needs to be told, in the words of the
        // thing they have to go and set.
        detail: ok ? 'data-proxy is configured'
                   : 'CYGENIX_DATA_FN_KEY is not set for this deployment; every '
                     + 'call to /api/data will fail until it is.',
        apiBaseConfigured: !!process.env.CYGENIX_DATA_API_BASE,
        checkedAt: new Date().toISOString(),
      }),
    };
  }

  if (agentPath) {
    if (!agentPathAllowed(agentPath)) return fail('Unsupported path: ' + agentPath, 400, 'bad-path');
  } else if (!ALLOWED.has(action)) {
    return fail('Unknown or unsupported action: ' + action, 400, 'bad-action');
  }
  if (!FN_KEY) {
    // Loud, not silent. A proxy that cannot authenticate to the Function App
    // must not degrade into an unauthenticated one.
    //
    // console.error, not warn: this is the line that should have been paging
    // somebody for three weeks. Netlify surfaces error-level function logs,
    // and a 503 with no log entry is how a deployment fault stays invisible.
    console.error('[data-proxy] CYGENIX_DATA_FN_KEY is not set — refusing action:', action || agentPath);
    return fail('CYGENIX_DATA_FN_KEY is not configured for this deployment.', 503, 'no-fn-key');
  }

  // ── Identity: verified claims only ──────────────────────────────────────
  let identity;
  try {
    const authed = await verifyAuthHeader(event);
    // The Function App keys its Cosmos records by email (see `email: userId`
    // in azure-function/src/index.js), so that is the claim to send. `sub`
    // is the fallback for a token that carries no email.
    identity = String(authed.email || authed.sub || '').trim().toLowerCase();
    if (!identity) throw new Error('token carries no email or subject claim');
  } catch (e) {
    return fail('Auth error: ' + e.message, 401, 'auth');
  }

  // ── Forward ─────────────────────────────────────────────────────────────
  // Only the query parameters the caller passed BESIDES `action`, so a
  // caller cannot smuggle `?userId=someone-else` past step 2 — the Function
  // App reads that as a fallback identity.
  const extra = Object.entries(q)
    .filter(([k]) => k !== 'action' && k !== 'path' && k !== 'code' && k !== 'userId')
    .map(([k, v]) => encodeURIComponent(k) + '=' + encodeURIComponent(v));
  const qs = ['code=' + encodeURIComponent(FN_KEY)].concat(extra).join('&');
  const url = agentPath
    // The agent routes carry their own path (and may carry their own query
    // string, e.g. /agent/scope?projectId=…), so append rather than encode.
    ? AGENT_ROOT + agentPath + (agentPath.includes('?') ? '&' : '?') + qs
    : API_BASE + '/' + encodeURIComponent(action) + '?' + qs;

  const headers = {
    'Content-Type': 'application/json',
    // The verified identity. Not the caller's header — this line is the fix.
    'x-user-id': identity,
  };
  // Pass the token through as well, so the Function App's own verification
  // has something real to check once REQUIRE_TOKEN_AUTH is switched on.
  const bearer = event.headers.authorization || event.headers.Authorization;
  if (bearer) headers.Authorization = bearer;

  // The caller's OWN Anthropic key, for the routes that reach Claude
  // (narrative, quality-suggest-rels, profile-classify-subjects). Forwarded
  // verbatim and never inspected, logged or stored: this proxy is a pipe for
  // it, not a holder of it. Cygenix has no key of its own to substitute, so a
  // request that arrives without one is refused downstream rather than
  // quietly succeeding on somebody else's account.
  const userKey = event.headers['x-anthropic-key'] || event.headers['X-Anthropic-Key'];
  if (userKey) headers['x-anthropic-key'] = userKey;

  try {
    const res = await fetch(url, {
      method: event.httpMethod,
      headers: headers,
      body: (event.httpMethod === 'GET' || event.httpMethod === 'DELETE') ? undefined : event.body,
    });
    const text = await res.text();
    return {
      statusCode: res.status,
      headers: CORS,
      // Pass the body through unchanged so the client sees the Function
      // App's own error text, which is the only diagnostic surface this
      // deployment has.
      body: text || '{}',
    };
  } catch (e) {
    console.error('[data-proxy] upstream call failed for', action || agentPath, '-', e.message);
    return fail('Upstream error: ' + e.message, 502, 'upstream');
  }
};
