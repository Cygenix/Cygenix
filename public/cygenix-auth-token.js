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
// Tenant marker — the previous version of this file matched only
// '.ciamlogin.com-' which missed records that MSAL stores under the
// 'login.windows.net' environment (this happens for some federation
// flows and after token refreshes). The dashboard's own MSAL-account
// scan in dashboard.html matches BOTH markers; this helper now does
// the same so it can find tokens regardless of which environment
// string MSAL chose to use.
//
// Why not msalInstance.acquireTokenSilent()?
//   We could, but every page that wants to call the API would need to spin up
//   an MSAL instance just to read a value already sitting in localStorage.
//   This helper does the read directly (5 ms) and is identical in result to
//   what acquireTokenSilent returns when the token is still valid.
//
// Diagnostics — set window.CYGENIX_AUTH_DEBUG = true (or add
// `?authdebug=1` to the URL) to log a one-line summary to the console
// every time getCygenixIdToken is called. Useful for diagnosing 401s
// from the reports endpoint without having to instrument the page.
//
// Returns: JWT string, or '' if no valid token is found.
//
// Usage:
//   const token = getCygenixIdToken();
//   fetch(url, { headers: { Authorization: 'Bearer ' + token } });

(function () {
  'use strict';

  // Match either of MSAL's possible environment strings for our tenant.
  // CIAM tenants typically use 'ciamlogin.com' but some token records
  // (especially after silent refreshes or federation flows) get stored
  // under 'login.windows.net'. Match both.
  const TENANT_MARKERS = ['.ciamlogin.com-', '-login.windows.net-'];

  // Enable diagnostics either via global flag or ?authdebug=1 in the URL.
  function debugEnabled() {
    try {
      if (typeof window !== 'undefined' && window.CYGENIX_AUTH_DEBUG) return true;
      if (typeof window !== 'undefined' && window.location && /[?&]authdebug=1/.test(window.location.search)) return true;
    } catch {}
    return false;
  }

  function dbg() {
    if (!debugEnabled()) return;
    try { console.log.apply(console, ['[cygenix-auth-token]'].concat([].slice.call(arguments))); } catch {}
  }

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

  // Returns true if the localStorage key matches one of our tenant markers.
  function keyMatchesTenant(k) {
    if (!k) return false;
    for (let i = 0; i < TENANT_MARKERS.length; i++) {
      if (k.indexOf(TENANT_MARKERS[i]) !== -1) return true;
    }
    return false;
  }

  // Memoised across calls: the full scan below walks every localStorage key
  // and JSON-parses each tenant record, and the fetch wrapper runs it on
  // EVERY API request. The token itself only changes when MSAL writes a new
  // one, roughly hourly — so serve the last result until 30s before its
  // expiry and rescan then. A sign-out clears MSAL's keys, and the renewal
  // path below overwrites the memo when it acquires a fresh token.
  let _tokenMemo = { token: '', exp: 0 };

  window.getCygenixIdToken = function getCygenixIdToken() {
    const nowSec = Math.floor(Date.now() / 1000);
    if (_tokenMemo.token && _tokenMemo.exp - 30 > nowSec) return _tokenMemo.token;
    let bestToken = '';
    let bestExp = 0;

    // Diagnostics counters — surfaced at the end if debug is on.
    let scannedKeys = 0;
    let tenantMatches = 0;
    let idTokenRecords = 0;
    let expiredSkipped = 0;
    let parseFailures = 0;

    try {
      for (let i = 0; i < localStorage.length; i++) {
        const k = localStorage.key(i);
        scannedKeys++;
        if (!keyMatchesTenant(k)) continue;
        tenantMatches++;
        const raw = localStorage.getItem(k);
        if (!raw || raw[0] !== '{') continue;

        let obj;
        try { obj = JSON.parse(raw); } catch { parseFailures++; continue; }
        if (!obj || obj.credentialType !== 'IdToken' || !obj.secret) continue;
        idTokenRecords++;

        const payload = decodeJwtPayload(obj.secret);
        if (!payload) { parseFailures++; continue; }

        // Skip expired tokens. ID tokens from Entra typically last 1 hour.
        if (payload.exp && payload.exp <= nowSec + 30) { expiredSkipped++; continue; }

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

    if (debugEnabled()) {
      dbg('scan complete', {
        scannedKeys,
        tenantMatches,
        idTokenRecords,
        expiredSkipped,
        parseFailures,
        foundValidToken: !!bestToken,
        expiresInSec: bestToken ? (bestExp - nowSec) : null,
      });
      if (!bestToken && tenantMatches === 0) {
        console.warn('[cygenix-auth-token] No localStorage records matched the tenant. '
          + 'Are you signed in? Try signing out and back in. '
          + 'Markers checked: ' + TENANT_MARKERS.join(', '));
      } else if (!bestToken && idTokenRecords === 0) {
        console.warn('[cygenix-auth-token] Tenant records found but no IdToken among them. '
          + 'MSAL may have stored credentials under a different credentialType. '
          + 'Try signing out and back in.');
      } else if (!bestToken && expiredSkipped > 0) {
        console.warn('[cygenix-auth-token] All ' + expiredSkipped + ' IdToken record(s) are expired. '
          + 'Refresh the page or sign out and back in to renew.');
      }
    }

    _tokenMemo = { token: bestToken, exp: bestExp || 0 };
    return bestToken;
  };

  // ── Silent token renewal ─────────────────────────────────────────────────
  // Entra ID tokens expire after about an hour. getCygenixIdToken() only READS
  // the MSAL cache, so once the cached token expires it returns '' — and every
  // API call then goes out unauthenticated and 401s, even though the user is
  // still signed in and MSAL holds a valid refresh token. Symptom: the app
  // works for an hour, then quietly stops talking to its own backend.
  //
  // Fix: when the cached token is missing or expired, renew it through MSAL's
  // acquireTokenSilent (which uses the refresh token, no user interaction).
  // msal-browser is loaded from the CDN only if the page doesn't already have
  // it, and only at the moment a renewal is actually needed — so the common
  // path (valid cached token) still costs nothing.
  const MSAL_CDN   = 'https://cdn.jsdelivr.net/npm/@azure/msal-browser@3.27.0/lib/msal-browser.min.js';
  const MSAL_CFG   = {
    auth: {
      clientId:  'f3478996-b2b5-4b21-9a23-a6b97a0e5b13',
      authority: 'https://cygenix.ciamlogin.com/',
      knownAuthorities: ['cygenix.ciamlogin.com'],
      redirectUri: window.location.origin + '/login',
      navigateToLoginRequestUrl: false,
    },
    cache: { cacheLocation: 'localStorage', storeAuthStateInCookie: false },
    system: { allowNativeBroker: false },
  };
  const MSAL_SCOPES = ['openid', 'profile', 'email'];

  function loadMsal() {
    if (typeof window.msal !== 'undefined') return Promise.resolve(true);
    return new Promise((resolve) => {
      let s = document.getElementById('cygenix-msal-js');
      if (!s) {
        s = document.createElement('script');
        s.id = 'cygenix-msal-js'; s.src = MSAL_CDN;
        document.head.appendChild(s);
      }
      s.addEventListener('load',  () => resolve(typeof window.msal !== 'undefined'), { once: true });
      s.addEventListener('error', () => resolve(false), { once: true });
    });
  }

  let _msalApp = null;
  async function getMsalApp() {
    if (_msalApp) return _msalApp;
    if (!(await loadMsal())) return null;
    const app = new window.msal.PublicClientApplication(MSAL_CFG);
    if (typeof app.initialize === 'function') await app.initialize();   // required in msal-browser v3
    _msalApp = app;
    return app;
  }

  // True when a JWT is missing, undecodable, or at/past its exp claim.
  // The 30s margin keeps us from shipping a token that expires in flight.
  function jwtExpired(tok) {
    const claims = decodeJwtPayload(tok);
    if (!claims || !claims.exp) return true;
    return (claims.exp * 1000) <= (Date.now() + 30000);
  }

  let _renewing = null;   // de-dupes concurrent renewals across call sites
  // force = true skips the cheap cached attempt and goes straight to the token
  // endpoint. Used when the SERVER has rejected a token the client believed
  // was fine, so the client can recover without the user signing in again.
  function renewIdToken(force) {
    if (_renewing && !force) return _renewing;
    _renewing = (async () => {
      try {
        const app = await getMsalApp();
        if (!app) { dbg('renew: MSAL unavailable'); return ''; }
        const accounts = app.getAllAccounts() || [];
        if (!accounts.length) { dbg('renew: no MSAL account cached'); return ''; }

        // acquireTokenSilent serves a CACHED result while the ACCESS token is
        // still valid — and that cached result carries the old ID token, which
        // may already be expired. Renewing that way returns the same dead
        // token and the server answers "jwt expired". So: try the cheap call
        // first, then force a real round trip to the token endpoint if what
        // came back is not actually usable.
        let res = force
          ? await app.acquireTokenSilent({ scopes: MSAL_SCOPES, account: accounts[0], forceRefresh: true })
          : await app.acquireTokenSilent({ scopes: MSAL_SCOPES, account: accounts[0] });
        let tok = (res && res.idToken) || '';
        if (jwtExpired(tok)) {
          dbg('renew: cached ID token is stale — forcing a refresh');
          res = await app.acquireTokenSilent({ scopes: MSAL_SCOPES, account: accounts[0], forceRefresh: true });
          tok = (res && res.idToken) || '';
        }
        // Never hand back a token we can already see is expired: doing so just
        // moves the failure to the server and hides the real cause.
        if (jwtExpired(tok)) {
          dbg('renew: still expired after forceRefresh — interactive sign-in required');
          try {
            window.dispatchEvent(new CustomEvent('cygenix:auth-expired', { detail: { reason: 'renewal-returned-expired-token' } }));
          } catch {}
          return '';
        }
        if (tok) {
          dbg('renew: acquired a fresh ID token');
          // Keep the legacy keys other modules read in step with the renewal.
          try {
            const claims = res.idTokenClaims || {};
            const exp = claims.exp ? claims.exp * 1000 : (Date.now() + 3600000);
            localStorage.setItem('cygenix_token', tok);
            sessionStorage.setItem('cygenix_token', tok);
            localStorage.setItem('cygenix_expires', String(exp));
            sessionStorage.setItem('cygenix_expires', String(exp));
          } catch {}
        }
        return tok;
      } catch (e) {
        // InteractionRequiredAuthError means the refresh token is gone too —
        // the user genuinely has to sign in again. Surface it rather than
        // failing silently, but never redirect from here: that would yank a
        // user out of a page mid-task on one unlucky background call.
        dbg('renew failed:', e && e.message);
        try {
          window.dispatchEvent(new CustomEvent('cygenix:auth-expired', { detail: { error: e && e.message } }));
        } catch {}
        return '';
      } finally {
        setTimeout(() => { _renewing = null; }, 0);
      }
    })();
    return _renewing;
  }

  // Async counterpart to getCygenixIdToken(): returns a cached token if it is
  // still valid, otherwise renews silently. Prefer this anywhere you can await.
  window.getCygenixIdTokenAsync = async function getCygenixIdTokenAsync() {
    const cached = window.getCygenixIdToken();
    if (cached) return cached;
    return await renewIdToken();
  };

  // Force a fresh token from the token endpoint, ignoring every cache. Call
  // this after a server 401 so a rejected token can be replaced without
  // making the user sign in again.
  window.renewCygenixIdToken = function renewCygenixIdToken() { return renewIdToken(true); };

  // Convenience wrapper for fetch — adds Authorization header automatically.
  // Use this anywhere you want to call a Cygenix API endpoint that requires
  // authentication. Falls back to a normal fetch if no token is found, so
  // the server gets a clean 401 rather than an opaque network error.
  //
  // When debug is enabled, logs a single line per call describing whether
  // the Authorization header was attached.
  //
  // Usage:
  //   const r = await cygenixFetch('/.netlify/functions/reports', {
  //     method: 'POST',
  //     headers: { 'Content-Type': 'application/json' },
  //     body: JSON.stringify({ action: 'list' }),
  //   });
  window.cygenixFetch = async function cygenixFetch(url, opts) {
    const o = opts || {};
    const headers = Object.assign({}, o.headers || {});
    const token = await window.getCygenixIdTokenAsync();   // renews if expired
    if (token) {
      headers['Authorization'] = 'Bearer ' + token;
      dbg('fetch', url, '→ Authorization header attached');
    } else {
      dbg('fetch', url, '→ NO TOKEN AVAILABLE (renewal failed), request will 401');
    }
    return fetch(url, Object.assign({}, o, { headers }));
  };

  // ── Transparent auth for same-origin API calls ───────────────────────────
  // The Netlify Functions and edge functions now verify the Entra ID token
  // server-side. Rather than editing every fetch() call site across ~15
  // pages, wrap window.fetch once: any same-origin request to
  // /.netlify/functions/* or /api/* gets the Authorization header attached
  // automatically (unless the caller already set one).
  //
  // Same-origin by default: adding an Authorization header to cross-origin
  // calls changes their CORS preflight requirements. The Azure Function App
  // is opt-in via SEND_TOKEN_TO_AZURE — flip it to true ONLY AFTER the
  // Function App has deployed the entra-auth changes (its CORS allowlist
  // must include Authorization first, or every preflight to it will fail).
  // Rollout: 1) deploy everything with this false; 2) confirm the Azure app
  // is live (its /api responses allow Authorization); 3) flip to true and
  // redeploy the site; 4) once logs show no legacy-header warnings, set
  // REQUIRE_TOKEN_AUTH=true on the Function App.
  const SEND_TOKEN_TO_AZURE = false;
  const AZURE_API = /^https:\/\/cygenix-db-api-[^/]+\.azurewebsites\.net\//i;
  const AUTH_PATH = /^\/(\.netlify\/functions|api)\//;
  const _origFetch = window.fetch.bind(window);
  window.fetch = function (input, init) {
    try {
      const url = typeof input === 'string' ? input
                : (input && typeof input.url === 'string') ? input.url : '';
      // Resolve relative URLs; only touch same-origin function/API paths.
      const path = url.startsWith('/') ? url
                 : url.startsWith(window.location.origin) ? url.slice(window.location.origin.length)
                 : '';
      if (AUTH_PATH.test(path) || (SEND_TOKEN_TO_AZURE && AZURE_API.test(url))) {
        const opts = init || {};
        const headers = new Headers(opts.headers || (input instanceof Request ? input.headers : undefined));
        if (headers.has('Authorization')) return _origFetch(input, Object.assign({}, opts, { headers }));

        // Fast path: a valid cached token keeps this synchronous in spirit.
        const cached = window.getCygenixIdToken();
        if (cached) {
          headers.set('Authorization', 'Bearer ' + cached);
          return _origFetch(input, Object.assign({}, opts, { headers }));
        }
        // Expired or missing — renew before sending, rather than firing off a
        // request we know will 401. fetch already returns a promise, so
        // awaiting here is invisible to callers.
        return window.getCygenixIdTokenAsync().then((tok) => {
          if (tok) headers.set('Authorization', 'Bearer ' + tok);
          return _origFetch(input, Object.assign({}, opts, { headers }));
        });
      }
    } catch (e) { /* fall through to the untouched fetch on any surprise */ }
    return _origFetch(input, init);
  };
})();
