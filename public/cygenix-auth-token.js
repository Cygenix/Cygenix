// cygenix-auth-token.js
//
// Tiny helper that returns the current user's Entra ID token (a JWT string)
// for attaching to API calls as `Authorization: Bearer <token>`.
//
// How it works:
//   MSAL stores the user's ID token in localStorage under a key whose value
//   is a JSON object with `credentialType: 'IdToken'` and `secret: '<jwt>'`.
//   We scan localStorage for that record (matching the Cygenix tenant) and
//   return the freshest non-expired one.
//
// Why not msalInstance.acquireTokenSilent()?
//   We could, but every page that wants to call the API would need to spin up
//   an MSAL instance just to read a value already sitting in localStorage.
//   This helper does the read directly (5 ms) and is identical in result to
//   what acquireTokenSilent returns when the token is still valid.
//
// Returns: JWT string, or '' if no valid token is found.
//
// Usage:
//   const token = getCygenixIdToken();
//   fetch(url, { headers: { Authorization: 'Bearer ' + token } });

(function () {
  'use strict';

  // Tenant marker — MSAL keys for our tenant contain '.ciamlogin.com-' or
  // the tenant id. Matching on the authority host is the most reliable.
  const TENANT_MARKER = '.ciamlogin.com-';

  function decodeJwtPayload(jwt) {
    try {
      const parts = jwt.split('.');
      if (parts.length !== 3) return null;
      // Base64url → base64 → string → JSON
      const b64 = parts[1].replace(/-/g, '+').replace(/_/g, '/');
      const padded = b64 + '='.repeat((4 - (b64.length % 4)) % 4);
      return JSON.parse(atob(padded));
    } catch {
      return null;
    }
  }

  window.getCygenixIdToken = function getCygenixIdToken() {
    const nowSec = Math.floor(Date.now() / 1000);
    let bestToken = '';
    let bestExp = 0;

    try {
      for (let i = 0; i < localStorage.length; i++) {
        const k = localStorage.key(i);
        if (!k || !k.includes(TENANT_MARKER)) continue;
        const raw = localStorage.getItem(k);
        if (!raw || raw[0] !== '{') continue;

        let obj;
        try { obj = JSON.parse(raw); } catch { continue; }
        if (!obj || obj.credentialType !== 'IdToken' || !obj.secret) continue;

        const payload = decodeJwtPayload(obj.secret);
        if (!payload) continue;

        // Skip expired tokens. ID tokens from Entra typically last 1 hour.
        if (payload.exp && payload.exp <= nowSec + 30) continue;

        // Prefer the token with the latest expiry — handles the case where
        // the user has signed in more than once and old records remain.
        if ((payload.exp || 0) > bestExp) {
          bestExp = payload.exp || 0;
          bestToken = obj.secret;
        }
      }
    } catch (e) {
      console.warn('[cygenix-auth-token] localStorage scan failed:', e);
    }

    return bestToken;
  };

  // Convenience wrapper for fetch — adds Authorization header automatically.
  // Use this anywhere you want to call a Cygenix API endpoint that requires
  // authentication. Falls back to a normal fetch if no token is found, so
  // the server gets a clean 401 rather than an opaque network error.
  //
  // Usage:
  //   const r = await cygenixFetch('/.netlify/functions/reports', {
  //     method: 'POST',
  //     headers: { 'Content-Type': 'application/json' },
  //     body: JSON.stringify({ action: 'list' }),
  //   });
  window.cygenixFetch = function cygenixFetch(url, opts) {
    const o = opts || {};
    const headers = Object.assign({}, o.headers || {});
    const token = window.getCygenixIdToken();
    if (token) headers['Authorization'] = 'Bearer ' + token;
    return fetch(url, Object.assign({}, o, { headers }));
  };
})();
