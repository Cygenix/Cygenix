/* ============================================================================
   cygenix-data-api.js — the browser's only route to /api/data.
   ----------------------------------------------------------------------------
   WHAT THIS REPLACES

   Six files each held their own copy of this:

       const API_BASE  = 'https://cygenix-db-api-….azurewebsites.net/api/data';
       const FUNC_CODE = '<the Function App host key>';
       fetch(`${API_BASE}/whoami?code=${FUNC_CODE}`, {
         headers: { 'x-user-id': userId }
       });

   Two things were wrong with that, and they compounded.

   The key was a real credential shipped to every visitor and committed to
   the repository. And `x-user-id` was whatever the caller cared to send —
   the Function App read it straight off the request and used it as the
   Cosmos partition key. Together: read the page source, take the key, set
   the header to somebody else's email, and their projects, jobs, saved
   connections and subscription record were readable and writable.

   Now the browser holds no key and asserts no identity. It sends its Entra
   ID token to /.netlify/functions/data-proxy, which verifies the token and
   derives the identity from the verified claims before calling the Function
   App with a key held server-side.

   WHY A SHARED MODULE RATHER THAN SIX EDITS

   Six copies is how one leaked key became six. There is now one place that
   knows the URL, one place that attaches the token, and one place to change
   if any of it moves.
   ========================================================================== */
(function (root) {
  'use strict';

  var ENDPOINT = '/.netlify/functions/data-proxy';

  function token() {
    try {
      return (typeof root.getCygenixIdToken === 'function') ? (root.getCygenixIdToken() || '') : '';
    } catch (e) { return ''; }
  }

  /**
   * Call one /api/data action.
   *
   * @param action  the action name, e.g. 'whoami', 'load', 'save'
   * @param opts    { method, body, query, signal }
   * @returns the parsed JSON body
   * @throws  on a non-2xx response, with the upstream error text attached —
   *          callers decide what a failure means, and several of them
   *          deliberately fail open. Swallowing the error here would take
   *          that decision away from them.
   */
  async function call(action, opts) {
    var o = opts || {};
    var t = token();
    if (!t) {
      var e = new Error('Not signed in');
      e.code = 'no-token';
      throw e;
    }

    var qs = ['action=' + encodeURIComponent(action)];
    Object.keys(o.query || {}).forEach(function (k) {
      var v = o.query[k];
      if (v !== undefined && v !== null && v !== '') {
        qs.push(encodeURIComponent(k) + '=' + encodeURIComponent(String(v)));
      }
    });

    var res = await fetch(ENDPOINT + '?' + qs.join('&'), {
      method: o.method || 'GET',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + t },
      body: o.body ? JSON.stringify(o.body) : undefined,
      signal: o.signal,
    });

    if (!res.ok) {
      var detail = '';
      try { detail = (await res.text()).slice(0, 500); } catch (e2) { /* body unreadable */ }
      var err = new Error('data-proxy ' + action + ' returned ' + res.status);
      err.status = res.status;
      err.detail = detail;
      throw err;
    }
    try { return await res.json(); } catch (e3) { return null; }
  }

  /** The common case: return the payload, or null on any failure. Used by the
   *  call sites that deliberately fail open (the tier gate, the user pill) —
   *  a billing lookup that 500s must not lock a paying user out of the app. */
  async function callOrNull(action, opts) {
    try { return await call(action, opts); }
    catch (e) {
      if (root.console && e.code !== 'no-token') {
        console.warn('[CygenixDataApi]', action, e.status || '', e.detail || e.message);
      }
      return null;
    }
  }

  function isSignedIn() { return !!token(); }

  root.CygenixDataApi = { call: call, callOrNull: callOrNull, isSignedIn: isSignedIn, ENDPOINT: ENDPOINT };
})(typeof window !== 'undefined' ? window : this);
