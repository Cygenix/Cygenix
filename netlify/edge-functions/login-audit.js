// netlify/edge-functions/login-audit.js
//
// Records one entry per interactive sign-in, with where it came from.
//
// ── Why this is an EDGE function and not a Netlify function ───────────────
//
// The point of the feature is the location, and location is the one thing
// only the edge has. A Netlify function can read the caller's IP from
// x-nf-client-connection-ip, but nothing downstream of the CDN knows the
// city, region or country behind that IP — an Azure Function certainly does
// not. Netlify's edge runtime resolves it for free and hands it over as
// `context.geo`. Doing the geo lookup anywhere else would mean paying a
// third party to answer a question the request already carried.
//
// So the shape is: browser → here (verify, enrich) → Azure Function → Cosmos.
//
// ── What this endpoint is trusted for, and what it is not ─────────────────
//
// It is trusted to say where a request came from, because it is the only
// participant that can observe that.
//
// It is NOT trusted to say who signed in. The identity comes from the Entra
// token, verified here against the same JWKS, audience and issuer list the
// Netlify functions use. A request without a valid token is refused with 401
// and nothing is written — the address bar is public and this endpoint
// writes to an audit container, so an unauthenticated POST must not be able
// to put a row in it.
//
// The Azure side checks a shared secret on top of that (x-audit-ingest-key),
// because the browser must never be able to reach the write action directly.
// A valid user token proves a person; the ingest key proves the request came
// through here, with the geo this function resolved rather than geo a caller
// made up.
//
// ── Failure is never the user's problem ───────────────────────────────────
//
// The caller is a sign-in that has already succeeded. Whatever happens in
// here, the person is signed in and must stay signed in: the client fires
// this and does not wait for it. A forwarding failure returns 502 with the
// reason in the body — in-band, because Flex Consumption has no Application
// Insights and an error that only exists in a log nobody can read is an
// error nobody can fix.

import { verifyRequestClaims } from './_lib/verify-entra.js';

// Registered here, not in netlify.toml. Adding blocks to that file has broken
// the site before, and an inline config keeps the route beside the code that
// serves it.
export const config = { path: '/api/login-audit' };

const TTL_SECONDS = 2592000;          // 30 days, as a Cosmos per-item TTL
const MAX_UA = 300;

const env = (k) => (typeof Netlify !== 'undefined' && Netlify.env.get(k))
  || (typeof Deno !== 'undefined' && Deno.env.get(k))
  || '';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};
const json = (status, obj) => new Response(JSON.stringify(obj), {
  status, headers: { ...CORS, 'Content-Type': 'application/json' },
});

// The entry, from verified claims and the edge's view of the request.
// Exported and pure so the rules can be tested: every geo field may be
// missing and becomes null rather than disappearing, the user agent is
// capped, and `idp` falls back to 'local' when Entra itself held the
// credential rather than a federated provider.
export function buildSigninEntry(claims, ctx, userAgent, nowIso) {
  const c = claims || {};
  const geo = (ctx && ctx.geo) || {};
  const emails = Array.isArray(c.emails) && c.emails.length ? c.emails[0] : '';
  const email = String(c.email || c.preferred_username || c.upn || emails || '')
    .trim().toLowerCase();
  return {
    type: 'signin',
    timestamp: nowIso,
    userId: String(c.oid || c.sub || '').trim(),
    email: email || null,
    idp: c.idp ? String(c.idp) : 'local',
    ip: (ctx && ctx.ip) || null,
    city: geo.city || null,
    region: (geo.subdivision && geo.subdivision.name) || null,
    country: (geo.country && geo.country.name) || null,
    countryCode: (geo.country && geo.country.code) || null,
    userAgent: String(userAgent || '').slice(0, MAX_UA) || null,
    ttl: TTL_SECONDS,
  };
}

export default async function handler(request, context) {
  if (request.method === 'OPTIONS') return new Response('', { status: 200, headers: CORS });
  if (request.method !== 'POST') return json(405, { error: 'Method not allowed' });

  // 1. Who. From the token, never from the body.
  let claims;
  try {
    claims = await verifyRequestClaims(request);
  } catch (e) {
    return json(401, { error: 'Not authenticated: ' + e.message });
  }
  const userId = String(claims.oid || claims.sub || '').trim();
  if (!userId) return json(401, { error: 'Token carries no subject' });

  // 2. Where. Every one of these can be missing — a corporate VPN, an IPv6
  //    range Netlify cannot place, a local dev request. A missing field is
  //    null and the entry is still written: knowing that someone signed in
  //    and not knowing from where is worth more than no record at all.
  const entry = buildSigninEntry(
    claims, context, request.headers.get('user-agent'), new Date().toISOString());

  // 3. Forward. The base and key are the ones the data proxy already uses,
  //    so this needs no new Azure configuration; AZURE_FUNC_BASE/KEY are
  //    accepted as alternatives for a deployment that separates them.
  const base = (env('CYGENIX_DATA_API_BASE') || env('AZURE_FUNC_BASE')
    || 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/data').replace(/\/+$/, '');
  const fnKey = env('CYGENIX_DATA_FN_KEY') || env('AZURE_FUNC_KEY');
  const ingestKey = env('AUDIT_INGEST_KEY');

  if (!ingestKey) {
    return json(502, { error: 'AUDIT_INGEST_KEY is not set on this site — the entry was not written.' });
  }

  const url = base + '/audit-signin' + (fnKey ? '?code=' + encodeURIComponent(fnKey) : '');
  let res;
  try {
    res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-audit-ingest-key': ingestKey,
        // The caller's own token goes with it. The Function App's
        // enforceAuth reads this header, and when REQUIRE_TOKEN_AUTH is
        // eventually turned on (it is off by default and tracked as an open
        // question) a request arriving without one is refused. Forwarding it
        // means this path keeps working the day that flag flips, instead of
        // becoming the one endpoint that silently stopped recording.
        Authorization: request.headers.get('authorization') || '',
      },
      body: JSON.stringify(entry),
    });
  } catch (e) {
    return json(502, { error: 'Could not reach the Function App: ' + e.message });
  }
  if (!res.ok) {
    let detail = '';
    try { detail = (await res.text()).slice(0, 500); } catch { /* body unreadable */ }
    return json(502, { error: 'Function App returned ' + res.status, detail });
  }

  // Nothing to say and nothing to read: the client is not waiting.
  return new Response(null, { status: 204, headers: CORS });
}
