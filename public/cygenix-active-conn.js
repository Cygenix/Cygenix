/* ============================================================================
   cygenix-active-conn.js — ONE answer to "what am I connected to right now?"
   ----------------------------------------------------------------------------
   WHAT WENT WRONG

   Four different functions answered this question, in four different ways,
   and they disagreed. readCurrentConnections and impGetConn in the
   dashboard, wiReadConns further down the same file, resolveConn on the
   Validation page, plus a dozen pages reading CygenixConnections.get()
   straight and composing the URL themselves. The live blob genuinely holds
   two shapes — a legacy flat one and a per-user block — and the readers
   split on which to prefer, so two screens in the same tab could be talking
   to two different databases.

   Worse, every one of those places built the Function App URL the same
   wrong way:

       fnUrl + (fnUrl.includes('?') ? '&' : '?') + 'code=' + key

   That is fine exactly once. It is not fine when fnUrl ALREADY carries a
   code — which it does, because CygenixConnections.srcConn composes one and
   callers store the composed value back. The second append produces
   ...?code=A&code=B, the Function App takes the LAST one, and the answer is
   401. That is the Schema Explorer failure: not a missing key, a doubled
   one.

   WHAT THIS IS

   getActiveConnection('src'|'tgt') → one descriptor, resolved in one order:

     1. the active profile's binding for that side, and the saved connection
        it names, merged with that connection's local secret;
     2. the live pair (cygenix_project_connections), per-user block;
     3. the same blob's legacy top-level fields, last.

   and connUrl('src'|'tgt') → the single string a caller passes as "the
   connection", composed through the URL API so a code can never be added
   twice.

   WHY THE FALLBACKS STAY

   Because a browser that has never seen a profile still has to work, and
   because the legacy flat fields are still sitting in real users' storage.
   Nothing here writes; it only reads, and it reads the newest shape first.

   NO NETWORK, ALMOST

   Resolution is pure storage reading — no request, so nothing to guard.
   The one exception is testConnection(), which makes a single lightweight
   call and carries the guards this codebase requires: one in flight at a
   time, three seconds minimum between calls, and a result cache. Its
   in-flight flag is cleared in a finally, never by the fetch callback, so
   a slow answer cannot let a second call start.
   ========================================================================== */
(function (root) {
  'use strict';

  var LS_ACTIVE = 'cygenix_project_connections';
  var TEST_MIN_INTERVAL_MS = 3000;

  function isHttpUrl(v) { return /^https?:\/\//i.test(String(v || '').trim()); }
  function str(v) { return typeof v === 'string' ? v.trim() : ''; }

  // ── The composer ────────────────────────────────────────────────────────
  // The whole reason this file exists. Never concatenate: parse, look, set
  // only if absent. A url that already carries a code is returned untouched,
  // because the code it carries is the one its owner meant.
  function compose(fnUrl, fnKey) {
    var u = str(fnUrl);
    if (!u || !isHttpUrl(u)) return '';        // "API" is not an address
    var parsed;
    try { parsed = new URL(u); } catch (e) { return ''; }
    if (parsed.searchParams.has('code')) return parsed.toString();
    var k = str(fnKey);
    if (k) parsed.searchParams.set('code', k);
    return parsed.toString();
  }

  // Is this string usable as a connection at all?
  function usable(v) {
    var s = str(v);
    if (!s) return false;
    if (isHttpUrl(s)) return true;
    // A direct connection string. Anything with a separator will do; a bare
    // word like "API" filed in the URL box is not a connection.
    return /[=;:@/]/.test(s) && s.length > 8;
  }

  // ── Collaborators, looked up at call time ───────────────────────────────
  function profilesApi() { return root.CygenixProfiles; }
  function connsApi() { return root.CygenixConnections; }
  function secretsApi() { return root.CygenixSavedConnSecrets; }
  function fnKeysApi() { return root.CygenixFnKeys; }

  // The product's own Function App is reached with a key nobody types: it is
  // fetched and written into the LIVE pair by ensureFnKeys. A profile-resolved
  // side reads the SAVED entry, whose fnKey is legitimately empty for that
  // host, so without this the resolver handed back a keyless URL and the
  // probe answered 401 while the rest of the product worked. Borrowing the
  // key here is a read of storage, never a fetch.
  function borrowProductKey(side, fnUrl, fnKey) {
    if (fnKey) return fnKey;
    var K = fnKeysApi();
    if (!K || typeof K.needsProductKey !== 'function') return fnKey;
    try {
      if (!K.needsProductKey(fnUrl, fnKey)) return fnKey;
      return (typeof K.keyForSide === 'function' && K.keyForSide(side)) || fnKey;
    } catch (e) { return fnKey; }
  }

  function activeProfile() {
    var P = profilesApi();
    if (!P || typeof P.cpLoad !== 'function') return null;
    var store;
    try { store = P.cpLoad(); } catch (e) { return null; }
    var id = store && store.settings && store.settings.activeProfileId;
    if (!id || !store.profiles) return null;
    for (var i = 0; i < store.profiles.length; i++) {
      var p = store.profiles[i];
      if (p && p.id === id && p.status !== 'retired') return p;
    }
    return null;
  }

  // The saved connection a profile names, with its local secret merged back
  // in. The saved entry itself is never edited — this is a copy.
  function savedWithSecret(connId) {
    var C = connsApi();
    if (!C || !connId || typeof C.savedGetById !== 'function') return null;
    var rec;
    try { rec = C.savedGetById(connId); } catch (e) { return null; }
    if (!rec) return null;
    var out = {};
    for (var k in rec) if (Object.prototype.hasOwnProperty.call(rec, k)) out[k] = rec[k];
    var S = secretsApi();
    if (S && typeof S.get === 'function') {
      var sec = null;
      try { sec = S.get(connId); } catch (e) { sec = null; }
      if (sec) {
        if (sec.connString && !out.connString) out.connString = sec.connString;
        if (sec.fnKey && !out.fnKey) out.fnKey = sec.fnKey;
        if (sec.secret && !out.secret) out.secret = sec.secret;
      }
    }
    return out;
  }

  // Shape, not the stored mode: a connection string filed in the URL box is
  // still a connection string. Mirrors normaliseSide in connections.js.
  function shapeOf(connString, fnUrl, fnKey) {
    var cs = str(connString), url = str(fnUrl), key = str(fnKey);
    if (url && !isHttpUrl(url)) { if (!cs) cs = url; url = ''; }
    if (cs && isHttpUrl(cs)) { if (!url) url = cs; cs = ''; }
    return { connString: cs, fnUrl: url, fnKey: key, mode: url ? 'azure' : 'direct' };
  }

  function liveBlob() {
    try { return JSON.parse(root.localStorage.getItem(LS_ACTIVE) || '{}') || {}; }
    catch (e) { return {}; }
  }
  function currentUser() {
    var C = connsApi();
    if (C && typeof C.currentUserTag === 'function') {
      try { var t = C.currentUserTag(); if (t) return t; } catch (e) {}
    }
    try { return root.localStorage.getItem('cygenix_active_user') || ''; } catch (e) { return ''; }
  }

  // ── The resolver ────────────────────────────────────────────────────────
  function getActiveConnection(side) {
    var s = (side === 'tgt' || side === 'target') ? 'tgt' : 'src';
    var out = { side: s, mode: 'direct', connString: '', fnUrl: '', fnKey: '',
                connId: null, profileId: null, source: 'none', ok: false, why: 'Nothing is configured for this side.' };

    // 1. The active profile's binding.
    var p = activeProfile();
    if (p) {
      out.profileId = p.id;
      var connId = s === 'src' ? p.srcConnId : p.tgtConnId;
      if (connId) {
        out.connId = connId;
        var rec = savedWithSecret(connId);
        if (rec) {
          var sh = shapeOf(rec.connString, rec.fnUrl, rec.fnKey);
          sh.fnKey = borrowProductKey(s, sh.fnUrl, sh.fnKey);
          if (usable(sh.connString) || compose(sh.fnUrl, sh.fnKey)) {
            out.mode = sh.mode; out.connString = sh.connString;
            out.fnUrl = sh.fnUrl; out.fnKey = sh.fnKey;
            out.source = 'profile'; out.ok = true; out.why = '';
            return out;
          }
          // Named, found, but this browser holds no credential for it. Say
          // which one, so the page can name it rather than look empty.
          out.source = 'profile';
          out.why = 'The credential for "' + (rec.name || connId) + '" is not on this device.';
          out.needsSecret = true;
          out.connName = rec.name || connId;
          return out;
        }
        out.why = 'The profile names a connection (' + connId + ') that is not in the saved list.';
        return out;
      }
    }

    // 2. The live pair, per-user block.
    var blob = liveBlob();
    var uid = currentUser();
    var mine = (uid && blob[uid] && typeof blob[uid] === 'object') ? blob[uid] : null;
    if (mine) {
      var sh2 = shapeOf(mine[s + 'ConnString'], mine[s + 'FnUrl'], mine[s + 'FnKey']);
      if (usable(sh2.connString) || compose(sh2.fnUrl, sh2.fnKey)) {
        out.mode = sh2.mode; out.connString = sh2.connString;
        out.fnUrl = sh2.fnUrl; out.fnKey = sh2.fnKey;
        out.source = 'live'; out.ok = true; out.why = '';
        return out;
      }
    }

    // 3. The same blob's legacy top-level fields. Last, because nothing has
    // written them for a long time and they drift from the per-user block.
    var sh3 = shapeOf(blob[s + 'ConnString'], blob[s + 'FnUrl'], blob[s + 'FnKey']);
    if (usable(sh3.connString) || compose(sh3.fnUrl, sh3.fnKey)) {
      out.mode = sh3.mode; out.connString = sh3.connString;
      out.fnUrl = sh3.fnUrl; out.fnKey = sh3.fnKey;
      out.source = 'legacy'; out.ok = true; out.why = '';
      return out;
    }

    return out;
  }

  // The one string a caller passes as "the connection". Empty when this
  // side cannot be connected, which callers must treat as "do not call".
  function connUrl(side) {
    var c = getActiveConnection(side);
    if (!c.ok) return '';
    return c.mode === 'azure' ? compose(c.fnUrl, c.fnKey) : c.connString;
  }

  // ── The live test ───────────────────────────────────────────────────────
  // A real call, because a status light built from saved configuration says
  // "green" for a password that was changed last week. One in flight, three
  // seconds apart, and the answer cached so a re-render does not re-test.
  var _testInflight = {};
  var _testAt = { src: 0, tgt: 0 };
  var _testLast = { src: null, tgt: null };

  // The result also survives a page change, because the status bar is on
  // every page and a fresh test per navigation would be a query per click.
  // Keyed by the connection's identity, so changing profile invalidates it
  // without anyone having to remember to clear it. Five minutes: long
  // enough that browsing costs nothing, short enough that a password
  // changed in the morning is noticed before lunch.
  var TEST_CACHE_KEY = 'cygenix_conn_test_v1';
  var TEST_TTL_MS = 5 * 60 * 1000;

  // A short digest, so the cache key never holds the credential itself.
  function identity(conn) {
    var h = 2166136261, s = String(conn || '');
    for (var i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = (h * 16777619) >>> 0; }
    return h.toString(36);
  }
  function readCache() {
    try { return JSON.parse(root.sessionStorage.getItem(TEST_CACHE_KEY) || '{}') || {}; }
    catch (e) { return {}; }
  }
  function cacheGet(side, id) {
    var c = readCache()[side];
    if (!c || c.id !== id) return null;
    if (Date.now() - (c.at || 0) > TEST_TTL_MS) return null;
    return c.result || null;
  }
  function cachePut(side, id, result) {
    try {
      var all = readCache();
      all[side] = { id: id, at: Date.now(), result: result };
      root.sessionStorage.setItem(TEST_CACHE_KEY, JSON.stringify(all));
    } catch (e) { /* the in-memory copy still stands for this page */ }
  }

  // Run the shared key step, but only when this side actually needs it: the
  // product's own host with nothing on it. Never throws, and resolves to
  // nothing useful on purpose — the caller re-reads storage afterwards.
  function maybeEnsureKey(c) {
    try {
      var K = fnKeysApi();
      if (!K || typeof K.ensure !== 'function') return Promise.resolve(null);
      if (!K.needsProductKey(c.fnUrl, c.fnKey)) return Promise.resolve(null);
      return K.ensure().catch(function () { return null; });
    } catch (e) { return Promise.resolve(null); }
  }
  function isProductHost(url) {
    try {
      var K = fnKeysApi();
      if (!K || typeof K.productHost !== 'function') return false;
      return new URL(String(url || '')).host.toLowerCase() === K.productHost();
    } catch (e) { return false; }
  }

  function testConnection(side, opts) {
    var s = (side === 'tgt' || side === 'target') ? 'tgt' : 'src';
    var o = opts || {};
    if (_testInflight[s]) return _testInflight[s];
    if (!o.force && _testLast[s] && (Date.now() - _testAt[s]) < TEST_MIN_INTERVAL_MS) {
      return Promise.resolve(_testLast[s]);
    }
    var c = getActiveConnection(s);
    if (!c.ok) {
      _testLast[s] = { ok: false, state: c.needsSecret ? 'password-needed' : 'not-configured',
                       message: c.why, side: s, connName: c.connName || null };
      _testAt[s] = Date.now();
      return Promise.resolve(_testLast[s]);
    }
    var conn = c.mode === 'azure' ? compose(c.fnUrl, c.fnKey) : c.connString;
    var ident = identity(conn);
    if (!o.force) {
      var cached = cacheGet(s, ident);
      if (cached) { _testLast[s] = cached; return Promise.resolve(cached); }
    }
    _testAt[s] = Date.now();                    // stamped BEFORE the request
    _testInflight[s] = (function () {
      var done = function (res) { _testLast[s] = res; cachePut(s, ident, res); return res; };
      var p;
      if (c.mode === 'azure') {
        // THE KEY STEP, which this probe used to skip.
        //
        // The product's own Function App is reached with a key that is
        // fetched, not typed. connections.js runs that step for every real
        // call; this probe built its own request and did not, so it went out
        // bare and Azure answered 401 — a status bar saying the target was
        // unreachable while every query against it worked.
        //
        // ensure() IS ensureFnKeys, so this shares its in-flight promise and
        // its interval rather than adding a second set of guards. A real call
        // starting while this is in flight waits for the same fetch.
        p = maybeEnsureKey(c).then(function () {
          // Re-resolve: the key, if one arrived, is now in the live pair.
          var fresh = getActiveConnection(s);
          var url = fresh.ok ? compose(fresh.fnUrl, fresh.fnKey) : conn;
          var keyless = isProductHost(fresh.fnUrl || c.fnUrl) && !(fresh.fnKey || c.fnKey);
          return root.fetch(url || conn, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'test' }),
          }).then(function (r) {
            return r.json().catch(function () { return {}; }).then(function (d) {
              if (r.ok && d && d.success !== false) return done({ ok: true, state: 'ok', message: d.version || 'Connected', side: s });
              // A 401 from OUR OWN host, with no key on the request, is not a
              // credentials problem for a person to solve — it is the key
              // step not having completed. Say that instead of a bare code.
              if ((r.status === 401 || r.status === 403) && keyless) {
                return done({ ok: false, state: 'key-pending', message: 'key not loaded', side: s });
              }
              return done({ ok: false, state: 'failed', message: (d && (d.error || d.message)) || ('HTTP ' + r.status), side: s });
            });
          });
        });
      } else {
        // A read, and the smallest one there is. Deliberately an `execute`
        // of SELECT 1 rather than a bespoke "test" action: the product's
        // guards assert that every db-connect call is a read, and a status
        // light is not a good reason to make an exception to that.
        p = root.fetch('/.netlify/functions/db-connect', {
          method: 'POST',
          headers: Object.assign({ 'Content-Type': 'application/json' }, authHeader()),
          body: JSON.stringify({ action: 'execute', connectionString: conn, sql: 'SELECT 1' }),
        }).then(function (r) {
          return r.json().catch(function () { return {}; }).then(function (d) {
            if (r.ok && d && d.success !== false && !d.error) return done({ ok: true, state: 'ok', message: d.version || 'Connected', side: s });
            return done({ ok: false, state: 'failed', message: (d && (d.error || d.message)) || ('HTTP ' + r.status), side: s });
          });
        });
      }
      return p.catch(function (e) {
        return done({ ok: false, state: 'failed', message: 'Could not reach the server: ' + (e && e.message || e), side: s });
      });
    })();
    return _testInflight[s].then(
      function (r) { _testInflight[s] = null; return r; },
      function (e) { _testInflight[s] = null; throw e; }
    );
  }

  function authHeader() {
    try {
      var t = (typeof root.getCygenixIdToken === 'function') ? root.getCygenixIdToken() : '';
      return t ? { Authorization: 'Bearer ' + t } : {};
    } catch (e) { return {}; }
  }

  function lastTest(side) {
    var s = (side === 'tgt' || side === 'target') ? 'tgt' : 'src';
    return _testLast[s];
  }

  // ── Warm-up: keyed and awake before anyone asks ─────────────────────────
  //
  // Two problems this solves, both reported from the live site.
  //
  // THE FALSE 401. The bar used to be built from saved configuration, which
  // says "green" for a password changed last week; it now carries the result
  // of a real query. But the probe skipped the key step, so against the
  // product's own Function App it reported 401 while the product worked.
  // testConnection runs that step now, and this runs it BEFORE the probe so
  // the first answer is the true one.
  //
  // THE TWO ATTEMPTS. The Function App is Flex Consumption: the first request
  // after an idle period wakes it and is slow, and if that request also
  // raced the key fetch it failed outright. The three-second guards then held
  // the retry back, which is what made it feel like "try twice and wait".
  // Warming once, early, puts the cold start before the person rather than in
  // front of their first click.
  //
  // ONCE PER SESSION, PER PROFILE. The flag is written when the warm-up
  // STARTS and is never cleared by its own callback — a flag a callback
  // resets is a flag that lets the work start twice when the callback is
  // slow, which against a key endpoint is a request storm. Switching profile
  // changes the flag's value, so the new profile warms once and no more.
  var WARM_KEY = 'cygenix_conn_warm_v1';
  var COLD_RETRY_MS = 4000;
  var _warming = false;

  function warmStamp() {
    var c = getActiveConnection('tgt');
    return (c.profileId || 'none') + '::' + (c.connId || 'live');
  }
  function warmedAlready(stamp) {
    try { return root.sessionStorage.getItem(WARM_KEY) === stamp; } catch (e) { return false; }
  }
  function markWarming(stamp) {
    try { root.sessionStorage.setItem(WARM_KEY, stamp); } catch (e) { /* in-memory guard still holds */ }
  }

  // A cold start looks like a timeout or a dropped connection. A 401, a 403
  // or a real database error does not, and retrying those would be a loop
  // against a server that has already given its answer.
  function looksCold(r) {
    if (!r || r.ok) return false;
    if (r.state === 'key-pending') return false;
    return /Could not reach the server|timeout|timed out|network|Failed to fetch/i.test(r.message || '');
  }

  function reportSide(side, r) {
    var bar = root.CygenixStatusHairline;
    if (!bar || typeof bar.report !== 'function') return;
    var word = side === 'src' ? 'Source' : 'Target';
    if (r && r.ok) { bar.report('conn-' + side, null); return; }
    if (r && r.state === 'key-pending') {
      // Not a credentials problem anyone can act on, and not worth a red bar
      // on a page that is still starting up.
      bar.report('conn-' + side, 'amber', word + ': key not loaded yet');
      return;
    }
    bar.report('conn-' + side, 'red', word + ': ' + ((r && r.message) || 'could not connect'));
  }

  function warmSide(side) {
    var c = getActiveConnection(side);
    if (!c.ok && !c.needsSecret) return Promise.resolve(null);   // nothing configured
    // The key first, so the probe is never the bare request that answers 401.
    return maybeEnsureKey(c)
      .then(function () { return testConnection(side, { force: true }); })
      .then(function (r) {
        if (!looksCold(r)) { reportSide(side, r); return r; }
        // ONE retry, once, for the cold start. Scheduled rather than looped,
        // and it goes through testConnection's own guards like any other call.
        return new Promise(function (resolve) {
          root.setTimeout(function () {
            testConnection(side, { force: true }).then(function (r2) {
              reportSide(side, r2); resolve(r2);
            }, function () { reportSide(side, r); resolve(r); });
          }, COLD_RETRY_MS);
        });
      })
      .catch(function () { return null; });
  }

  // Non-blocking by construction: nothing awaits this, and it returns before
  // the network does.
  function warmUp(opts) {
    var o = opts || {};
    if (_warming) return;
    var stamp = warmStamp();
    if (!o.force && warmedAlready(stamp)) return;
    _warming = true;                       // set BEFORE the work it guards
    markWarming(stamp);                    // and persisted before it too
    var both = ['src', 'tgt'].map(warmSide);
    // The in-memory latch drops only once every side has settled, so a second
    // trigger during the warm-up joins nothing rather than starting again.
    Promise.all(both).then(function () { _warming = false; }, function () { _warming = false; });
  }

  // Kept as the old name for anything that called it.
  function verifyForStatus() { warmUp(); }

  if (root.document && typeof root.addEventListener === 'function') {
    // After the cloud load, so a profile that only just arrived is the one
    // warmed. Falls back to DOMContentLoaded where the sync layer is absent.
    root.addEventListener('cygenix-sync-loaded', function () { root.setTimeout(warmUp, 1200); }, { once: true });
    if (root.document.readyState === 'loading') {
      root.document.addEventListener('DOMContentLoaded', function () { root.setTimeout(warmUp, 2500); }, { once: true });
    } else {
      root.setTimeout(warmUp, 2500);
    }
    // Switching profile changes the stamp, so this warms the NEW profile once
    // and is a no-op for a re-render. warmUp never dispatches this event, so
    // it cannot call itself.
    root.addEventListener('cygenix:profiles-changed', function () { root.setTimeout(warmUp, 400); });
  }

  root.CygenixActiveConn = {
    getActiveConnection: getActiveConnection,
    verifyForStatus: verifyForStatus, warmUp: warmUp, warmStamp: warmStamp, looksCold: looksCold,
    get: getActiveConnection,
    connUrl: connUrl,
    compose: compose,
    isHttpUrl: isHttpUrl,
    usable: usable,
    testConnection: testConnection,
    lastTest: lastTest,
    _reset: function () {
      _testInflight = {}; _testAt = { src: 0, tgt: 0 }; _testLast = { src: null, tgt: null }; _warming = false;
      try { root.sessionStorage.removeItem(TEST_CACHE_KEY); root.sessionStorage.removeItem(WARM_KEY); } catch (e) {}
    },
  };
})(typeof window !== 'undefined' ? window : this);
