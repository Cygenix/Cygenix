// netlify/functions/lib/entra-auth.js
//
// Shared Entra ID (CIAM) JWT verification for all Netlify Functions.
// Extracted from reports.js so every endpoint authenticates the same way:
// signature verified against Entra's published JWKS, audience + issuer
// checked, identity taken from VERIFIED claims only.
//
// Usage:
//   const { verifyAuthHeader } = require('./lib/entra-auth');
//   const authed = await verifyAuthHeader(event);   // throws on failure
//   // authed = { email, name, oid, sub }
//
// Never trust a client-supplied header (x-user-id) or an unverified
// base64-decoded payload for identity — that allows trivial impersonation.

const jwt        = require('jsonwebtoken');
const jwksClient = require('jwks-rsa');

const TENANT_ID      = process.env.ENTRA_TENANT_ID      || 'fc8dfc7a-645f-4a5c-8f59-6762f97c803f';
const CLIENT_ID      = process.env.ENTRA_CLIENT_ID      || 'f3478996-b2b5-4b21-9a23-a6b97a0e5b13';
const AUTHORITY_HOST = process.env.ENTRA_AUTHORITY_HOST || 'cygenix.ciamlogin.com';

const JWKS_URI = `https://${AUTHORITY_HOST}/${TENANT_ID}/discovery/v2.0/keys`;

// Entra External ID can issue tokens with several issuer values depending on
// the user flow and federation. Accept the standard CIAM patterns.
const VALID_ISSUERS = [
  `https://${AUTHORITY_HOST}/${TENANT_ID}/v2.0`,
  `https://${TENANT_ID}.ciamlogin.com/${TENANT_ID}/v2.0`,
  `https://login.microsoftonline.com/${TENANT_ID}/v2.0`,
];

// Module-level JWKS client — caches signing keys across warm invocations so
// we hit Entra's discovery endpoint infrequently. Keys rotate ~daily and the
// library auto-refreshes.
const _jwks = jwksClient({
  jwksUri: JWKS_URI,
  cache: true,
  cacheMaxEntries: 5,
  cacheMaxAge: 10 * 60 * 1000,
  rateLimit: true,
  jwksRequestsPerMinute: 10,
});

function getSigningKey(header, callback) {
  _jwks.getSigningKey(header.kid, (e, key) => {
    if (e) return callback(e);
    callback(null, key.getPublicKey());
  });
}

// Verify the Authorization header on a Netlify Function event.
// Returns { email, name, oid, sub } from VERIFIED claims, or throws.
async function verifyAuthHeader(event) {
  const h = event.headers || {};
  const raw = h.authorization || h.Authorization || '';
  if (!raw) throw new Error('Missing Authorization header');

  const m = /^Bearer\s+(.+)$/i.exec(raw.trim());
  if (!m) throw new Error('Authorization header must be "Bearer <token>"');
  const token = m[1].trim();

  const decoded = await new Promise((resolve, reject) => {
    jwt.verify(token, getSigningKey, {
      audience: CLIENT_ID,
      issuer:   VALID_ISSUERS,
      algorithms: ['RS256'],
      // Tolerate small clock differences between the user's machine and the
      // function host. Without this, a workstation a minute fast rejects
      // freshly issued tokens with "jwt expired" and the user cannot use the
      // app at all. 60s is the usual allowance and costs nothing securitywise.
      clockTolerance: 60,
    }, (e, payload) => {
      if (e) return reject(e);
      resolve(payload);
    });
  });

  // Entra puts the email in different claims depending on the IdP: Google
  // SSO populates `email`, direct sign-ups usually `preferred_username`.
  const email = String(
    decoded.email ||
    decoded.preferred_username ||
    decoded.upn ||
    ''
  ).trim().toLowerCase();

  return {
    email,
    name: decoded.name || '',
    oid:  decoded.oid  || decoded.sub || '',
    sub:  decoded.sub  || '',
    // App roles from the VERIFIED payload only (spec §9.5: never from a
    // decoded copy the client holds). Empty until the app registration
    // defines Cygenix.* app roles; the RBAC layer unions these with the
    // server-side assignment store either way.
    roles: Array.isArray(decoded.roles) ? decoded.roles : [],
    tid:   decoded.tid || '',
  };
}

module.exports = { verifyAuthHeader, VALID_ISSUERS, JWKS_URI };
