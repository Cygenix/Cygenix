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

  /* ── Telling failures apart ────────────────────────────────────────────────
     For three weeks every /api/data call returned 503, because
     CYGENIX_DATA_FN_KEY was missing from the Netlify deployment. Nothing above
     this file could tell that apart from "you have no data yet": the only
     reporting channel was a console.warn, and callOrNull returned the same
     null for a misconfigured deployment, an expired token and a genuinely
     empty account.

     A caller cannot make a safe decision without knowing which of those it is.
     Overwriting local state is right after a verified empty response and
     catastrophic after a 503. So failures now carry a classification, and the
     sync layer takes the branch that matches. */

  function DataLayerError(message, fields) {
    var e = new Error(message);
    e.name = 'DataLayerError';
    e.status = fields.status || 0;
    e.code = fields.code || 'unknown';
    e.retryable = !!fields.retryable;
    e.serverCode = fields.serverCode || '';
    e.detail = fields.detail || '';
    return e;
  }

  /**
   * Turn a transport outcome into one of a small, closed set of codes.
   *
   * 'config'   the deployment is broken — the proxy has no key to call the
   *            Function App with. Not the user's fault, not retryable by
   *            them, and NEVER a reason to treat their account as empty.
   * 'auth'     the token is missing, expired or rejected. Signing in again
   *            fixes it. Also not a statement about what is stored.
   * 'network'  the request never completed. Retryable.
   * 'server'   the far side failed. Retryable.
   * 'client'   we sent something wrong. A bug here, not retryable.
   *
   * `serverCode` is the machine-readable `code` that data-proxy now puts on
   * its error bodies, preferred over guessing from the status when present.
   */
  function classify(status, serverCode) {
    if (serverCode === 'no-fn-key') return { code: 'config', retryable: false };
    if (status === 0)   return { code: 'network', retryable: true };
    if (status === 401 || status === 403) return { code: 'auth', retryable: false };
    if (status === 503) return { code: 'config', retryable: false };
    if (status >= 500)  return { code: 'server', retryable: true };
    if (status >= 400)  return { code: 'client', retryable: false };
    return { code: 'unknown', retryable: false };
  }

  /* The proxy reports its own faults with a `code` field. Read it if it is
     there; a body that is not JSON, or carries no code, just means we fall
     back to classifying by status. */
  function serverCodeOf(bodyText) {
    try {
      var parsed = JSON.parse(bodyText);
      return (parsed && typeof parsed.code === 'string') ? parsed.code : '';
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
    var r = await callResult(action, opts);
    if (!r.ok) throw r.error;
    return r.data;
  }

  /**
   * The same call, as a result rather than an exception.
   *
   *   { ok: true,  data }                  the proxy answered. `data` may
   *                                        legitimately be null or {} — that
   *                                        is a VERIFIED empty account, which
   *                                        is a fact a caller may act on.
   *   { ok: false, error: DataLayerError } it did not. Nothing may be
   *                                        concluded about what is stored.
   *
   * The distinction is the whole point: `ok` says whether the answer is
   * trustworthy, `data` says what the answer was. Never throws, so a caller
   * cannot skip the check by forgetting a try/catch.
   */
  async function callResult(action, opts) {
    var o = opts || {};
    var t = token();
    if (!t) {
      return { ok: false, error: DataLayerError('Not signed in', {
        status: 0, code: 'no-token', retryable: false,
      }) };
    }

    var qs = ['action=' + encodeURIComponent(action)];
    Object.keys(o.query || {}).forEach(function (k) {
      var v = o.query[k];
      if (v !== undefined && v !== null && v !== '') {
        qs.push(encodeURIComponent(k) + '=' + encodeURIComponent(String(v)));
      }
    });

    var res;
    try {
      res = await fetch(ENDPOINT + '?' + qs.join('&'), {
        method: o.method || 'GET',
        headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + t },
        body: o.body ? JSON.stringify(o.body) : undefined,
        signal: o.signal,
      });
    } catch (netErr) {
      // Offline, DNS, CORS, an aborted signal. Status 0 — classify() reads
      // that as 'network', which is retryable.
      var c0 = classify(0, '');
      return { ok: false, error: DataLayerError('data-proxy ' + action + ' did not complete: ' + netErr.message, {
        status: 0, code: c0.code, retryable: c0.retryable,
      }) };
    }

    if (!res.ok) {
      var detail = '';
      try { detail = (await res.text()).slice(0, 500); } catch (e2) { /* body unreadable */ }
      var sc = serverCodeOf(detail);
      var c = classify(res.status, sc);
      return { ok: false, error: DataLayerError('data-proxy ' + action + ' returned ' + res.status, {
        status: res.status, code: c.code, retryable: c.retryable, serverCode: sc, detail: detail,
      }) };
    }

    // A 2xx with an unreadable or empty body is still a verified answer: the
    // proxy reached the Function App and it said nothing. That is an empty
    // account, not a failure, and the caller is entitled to treat it as one.
    try { return { ok: true, data: await res.json() }; }
    catch (e3) { return { ok: true, data: null }; }
  }

  /** Return the payload, or null on any failure.
   *
   *  FAIL-OPEN BY DESIGN, AND ONLY SAFE WHERE FAILING OPEN IS RIGHT. The
   *  three call sites are the tier gate, the user pill and the billing portal
   *  link: a billing lookup that 500s must not lock a paying user out of the
   *  app, so "couldn't tell" and "nothing there" leading to the same lenient
   *  outcome is correct there.
   *
   *  It is NOT safe anywhere the null would be read as a fact about stored
   *  data. That is the shape of the outage this file was hardened after: the
   *  sync layer called this, got null from a 503, and could not tell it from
   *  an empty account. Anything that writes, clears or decides on the basis
   *  of what came back must use callResult() and check `ok`.
   *
   *  Deliberately left fail-open rather than made to throw: ~40 scripts
   *  depend on this module, and turning a swallowed failure into an
   *  exception at three sites whose comments say they must not fail closed
   *  would trade a silent-data bug for a lockout. */
  async function callOrNull(action, opts) {
    var r = await callResult(action, opts);
    if (r.ok) return r.data;
    if (root.console && r.error.code !== 'no-token') {
      console.warn('[CygenixDataApi]', action, r.error.code, r.error.status || '', r.error.detail || r.error.message);
    }
    return null;
  }

  function isSignedIn() { return !!token(); }

  root.CygenixDataApi = {
    call: call,
    callResult: callResult,
    callOrNull: callOrNull,
    classify: classify,
    isSignedIn: isSignedIn,
    ENDPOINT: ENDPOINT,
  };
})(typeof window !== 'undefined' ? window : this);
