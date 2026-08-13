// netlify/edge-functions/_lib/verify-entra.js
//
// Entra ID (CIAM) JWT verification for Netlify EDGE functions (Deno).
// Mirrors netlify/functions/lib/entra-auth.js, using `jose` because
// jsonwebtoken/jwks-rsa are Node-only.
//
// Both AI endpoints (/api/analyse, /api/coworker) were previously
// unauthenticated proxies to the Anthropic API — anyone who found the URL
// could run up unbounded spend on the site's API key. Every request must now
// carry a verified Cygenix user token.

import { createRemoteJWKSet, jwtVerify } from 'https://esm.sh/jose@5.9.6';

const TENANT_ID      = Deno.env.get('ENTRA_TENANT_ID')      || 'fc8dfc7a-645f-4a5c-8f59-6762f97c803f';
const CLIENT_ID      = Deno.env.get('ENTRA_CLIENT_ID')      || 'f3478996-b2b5-4b21-9a23-a6b97a0e5b13';
const AUTHORITY_HOST = Deno.env.get('ENTRA_AUTHORITY_HOST') || 'cygenix.ciamlogin.com';

const VALID_ISSUERS = [
  `https://${AUTHORITY_HOST}/${TENANT_ID}/v2.0`,
  `https://${TENANT_ID}.ciamlogin.com/${TENANT_ID}/v2.0`,
  `https://login.microsoftonline.com/${TENANT_ID}/v2.0`,
];

// Module-scoped — jose caches the JWKS and refreshes on unknown kid.
const JWKS = createRemoteJWKSet(
  new URL(`https://${AUTHORITY_HOST}/${TENANT_ID}/discovery/v2.0/keys`)
);

// Verify the request's Authorization header. Returns { email, name, oid }
// from verified claims, or throws.
export async function verifyRequestAuth(request) {
  const raw = request.headers.get('authorization') || '';
  const m = /^Bearer\s+(.+)$/i.exec(raw.trim());
  if (!m) throw new Error('Missing Authorization header — sign in first');

  const { payload } = await jwtVerify(m[1].trim(), JWKS, {
    audience: CLIENT_ID,
    issuer: VALID_ISSUERS,
  });

  const email = String(
    payload.email || payload.preferred_username || payload.upn || ''
  ).trim().toLowerCase();

  return {
    email,
    name: payload.name || '',
    oid:  payload.oid || payload.sub || '',
  };
}
