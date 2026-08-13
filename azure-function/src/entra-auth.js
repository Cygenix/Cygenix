// azure-function/src/entra-auth.js
//
// Entra ID (CIAM) token verification for the Function App.
//
// Historically every endpoint trusted the client-supplied `x-user-id` header
// as the caller's identity — which lets any caller read/write any other
// user's data by changing one header. The front-end now attaches a real
// `Authorization: Bearer <Entra ID token>` to API calls; this module
// verifies it (signature via Entra's JWKS, audience, issuer) and, when a
// valid token is present, OVERRIDES x-user-id with the verified email so
// every downstream getUserId() picks up the authenticated identity.
//
// Rollout is two-phase, controlled by the REQUIRE_TOKEN_AUTH app setting:
//   unset / 'false'  → log-only. Valid tokens override x-user-id; requests
//                      without a token still work off the legacy header,
//                      with a warning logged. Deploy front-end + backend,
//                      watch the logs until "no Bearer token" warnings stop.
//   'true'           → enforced. Requests without a valid token get 401.
//                      Server-to-server dispatch (Netlify scheduler →
//                      run-migration/profile-build) instead authenticates
//                      with the x-dispatch-key header matching the
//                      DISPATCH_KEY app setting (set the same value in the
//                      Netlify env; see checkDispatchKey below).
//
// Uses jsonwebtoken + jwks-rsa (same as the Netlify functions' verifier).

const jwt        = require('jsonwebtoken');
const jwksClient = require('jwks-rsa');

const TENANT_ID      = process.env.ENTRA_TENANT_ID      || 'fc8dfc7a-645f-4a5c-8f59-6762f97c803f';
const CLIENT_ID      = process.env.ENTRA_CLIENT_ID      || 'f3478996-b2b5-4b21-9a23-a6b97a0e5b13';
const AUTHORITY_HOST = process.env.ENTRA_AUTHORITY_HOST || 'cygenix.ciamlogin.com';

const VALID_ISSUERS = [
  `https://${AUTHORITY_HOST}/${TENANT_ID}/v2.0`,
  `https://${TENANT_ID}.ciamlogin.com/${TENANT_ID}/v2.0`,
  `https://login.microsoftonline.com/${TENANT_ID}/v2.0`,
];

// Module-level JWKS client — caches signing keys across invocations.
const _jwks = jwksClient({
  jwksUri: `https://${AUTHORITY_HOST}/${TENANT_ID}/discovery/v2.0/keys`,
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

function verifyJwt(token) {
  return new Promise((resolve, reject) => {
    jwt.verify(token, getSigningKey, {
      audience: CLIENT_ID,
      issuer:   VALID_ISSUERS,
      algorithms: ['RS256'],
    }, (e, payload) => e ? reject(e) : resolve(payload));
  });
}

function requireTokenAuth() {
  return process.env.REQUIRE_TOKEN_AUTH === 'true';
}

function authFailure(message) {
  return {
    ok: false,
    response: {
      status: 401,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
      body: JSON.stringify({ error: message }),
    },
  };
}

// Call at the top of an HTTP handler (never for OPTIONS — let CORS preflight
// through first):
//   const auth = await enforceAuth(req, ctx);
//   if (!auth.ok) return auth.response;
// On success with a verified token, x-user-id is overwritten with the
// verified email, so existing getUserId() call sites need no changes.
async function enforceAuth(req, ctx) {
  if (req.method === 'OPTIONS') return { ok: true };

  const raw = req.headers.get('authorization') || '';
  const m = /^Bearer\s+(.+)$/i.exec(raw.trim());

  if (m) {
    try {
      const claims = await verifyJwt(m[1].trim());
      const email = String(
        claims.email || claims.preferred_username || claims.upn || ''
      ).trim().toLowerCase();
      if (!email) throw new Error('token contains no email claim');
      try { req.headers.set('x-user-id', email); } catch { /* immutable headers — getVerified below */ }
      return { ok: true, email, claims };
    } catch (e) {
      ctx.log.warn('[auth] token verification failed:', e.message);
      if (requireTokenAuth()) return authFailure('Invalid token: ' + e.message);
      // log-only mode: fall through to the legacy header
    }
  } else if (requireTokenAuth()) {
    return authFailure('Authorization Bearer token required');
  } else if (req.headers.get('x-user-id')) {
    ctx.log.warn('[auth] no Bearer token on request; trusting legacy x-user-id header (REQUIRE_TOKEN_AUTH is off)');
  }
  return { ok: true };
}

// Server-to-server dispatch auth for the anonymous runner endpoints
// (run-migration, profile-build). The Netlify scheduler sends
// x-dispatch-key; when DISPATCH_KEY is configured AND REQUIRE_TOKEN_AUTH
// is on, the key must match. Returns the same {ok, response} shape.
function checkDispatchKey(req, ctx) {
  if (!requireTokenAuth()) return { ok: true };
  const expected = process.env.DISPATCH_KEY || '';
  if (!expected) {
    ctx.log.warn('[auth] REQUIRE_TOKEN_AUTH is on but DISPATCH_KEY is not set — dispatch endpoints are unprotected');
    return { ok: true };
  }
  const got = req.headers.get('x-dispatch-key') || '';
  if (got === expected) return { ok: true };
  return authFailure('Valid x-dispatch-key required');
}

module.exports = { enforceAuth, checkDispatchKey, requireTokenAuth };
