// cygenix-audit.js — recording an event from the browser.
//
//   CygenixAudit.record({ action, category, target, env, changes, summary, outcome })
//   CygenixAudit.diff(before, after, fields)  → [{field, before, after}]
//
// Two things about this module are worth reading before using it.
//
// ── 1. It is the second-best way to record something ──────────────────────
//
// The best way is for the Netlify function that performs the change to
// record it, because then the record is a fact the server observed. This
// module exists for the changes that never reach a function at all — an
// export built in the page, a job reordered in localStorage, a bulk generate
// that runs entirely in the browser. Those are real events and losing them
// would leave holes in the trail, so they are recorded; but the server
// stamps them source:'client' so an auditor can tell the two apart, and it
// refuses any action outside its allowlist however this is called.
//
// If you are adding a call to this from code that also hits a function,
// move the record into the function instead.
//
// ── 2. Failing to record must not break the user's work ───────────────────
//
// Every call is fire-and-forget. A dropped connection, a 500, a tab closed
// mid-flight: none of them surface to the user or throw into the caller,
// because the alternative — an export that refuses to download because its
// audit row did not land — is a worse product and would teach people to
// distrust the log rather than the network.
//
// The exception is env:'PROD'. There the brief is explicit and correct: if
// the audit write fails, the action fails with it. This module cannot undo
// a change the page has already made, so what it does instead is tell the
// truth loudly — record() resolves to {ok:false, fatal:true} and the caller
// is expected to surface it. Callers that pass PROD and ignore the result
// are the bug.
//
// ── The queue ─────────────────────────────────────────────────────────────
//
// Offline, or a failed post, parks the event in memory and retries: on the
// browser's `online` event, and on a backoff timer. In memory and not in
// localStorage, deliberately — an audit event is not something to leave
// lying around on a shared machine after the tab is gone, and a queue that
// survives a reload would replay events whose context no longer exists. A
// closed tab loses at most a handful of client-side events, and the server
// already holds everything that mattered.

(function (root) {
  'use strict';

  var ENDPOINT = '/.netlify/functions/audit';
  var MAX_QUEUE = 50;          // beyond this the oldest are dropped, loudly
  var RETRY_MS = [3000, 10000, 30000, 60000];

  var queue = [];
  var retryAt = 0;
  var timer = null;
  var dropped = 0;

  function token() {
    try {
      return (typeof root.getCygenixIdToken === 'function') ? root.getCygenixIdToken() : '';
    } catch (e) { return ''; }
  }

  // ── diff ────────────────────────────────────────────────────────────────
  //
  // Changed fields only. The server redacts by field name before storage, so
  // a caller never has to remember to leave a password out — but it is still
  // better not to put one in, and `fields` exists so a caller can name the
  // handful that matter rather than handing over a whole settings object.
  function diff(before, after, fields) {
    var b = before || {}, a = after || {};
    var keys = (fields && fields.length) ? fields.slice()
      : Object.keys(b).concat(Object.keys(a)).filter(function (k, i, arr) { return arr.indexOf(k) === i; });
    var out = [];
    for (var i = 0; i < keys.length; i++) {
      var f = keys[i];
      var bv = b[f] === undefined ? null : b[f];
      var av = a[f] === undefined ? null : a[f];
      try {
        if (JSON.stringify(bv) === JSON.stringify(av)) continue;
      } catch (e) { /* unserialisable: treat as changed */ }
      out.push({ field: f, before: bv, after: av });
    }
    return out;
  }

  // ── record ──────────────────────────────────────────────────────────────
  function record(evt) {
    evt = evt || {};
    if (!evt.action) return Promise.resolve({ ok: false, reason: 'no action' });

    var payload = {
      op: 'record',
      action: evt.action,
      category: evt.category || null,
      target: evt.target || null,
      environment: evt.env || evt.environment || null,
      outcome: evt.outcome || 'allowed',
      changes: evt.changes || null,
      summary: evt.summary || null,
      projectId: evt.projectId || null,
      detail: evt.detail || null,
      actorType: evt.actorType || 'user',
      sessionId: evt.sessionId || null,
    };
    var fatal = payload.environment === 'PROD';
    return send(payload, fatal);
  }

  // Ask Cygenix acting for the signed-in user. A separate entry point rather
  // than a flag on record(), because the thing that makes this correct is
  // that the page cannot forget to set it — an assistant action recorded as
  // if a person did it is a worse record than no record.
  function recordAssistant(evt) {
    evt = evt || {};
    return record(Object.assign({}, evt, { actorType: 'assistant' }));
  }

  function send(payload, fatal) {
    var t = token();
    if (!t) {
      // Signed out. There is no identity to attribute this to, and the
      // server would refuse it anyway, so it is dropped rather than queued.
      return Promise.resolve({ ok: false, reason: 'not signed in' });
    }
    if (root.navigator && root.navigator.onLine === false) {
      return Promise.resolve(enqueue(payload, fatal, 'offline'));
    }
    return fetch(ENDPOINT, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + t },
      body: JSON.stringify(payload),
    }).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (d) {
        if (r.ok) {
          // `recorded: false` is a SUCCESS: the server received the event and
          // decided, per the capture state, not to store it. That is the
          // server making the drop decision, which is the whole design. It is
          // not a failure to retry.
          return { ok: true, recorded: d.recorded !== false, reason: d.reason || null, id: d.id || null };
        }
        if (r.status === 400 || r.status === 403) {
          // The server refused this event on its merits — the action is not
          // one the browser may assert, or the caller may not record. Retrying
          // will refuse identically, so it is dropped and reported.
          if (root.console) root.console.warn('[audit] refused: ' + (d.error || r.status));
          return { ok: false, fatal: fatal, refused: true, reason: d.error || ('HTTP ' + r.status) };
        }
        return enqueue(payload, fatal, d.error || ('HTTP ' + r.status));
      });
    }).catch(function (e) {
      return enqueue(payload, fatal, e.message || 'network error');
    });
  }

  function enqueue(payload, fatal, reason) {
    if (queue.length >= MAX_QUEUE) {
      queue.shift();
      dropped++;
      if (root.console) {
        root.console.warn('[audit] queue full — ' + dropped + ' event(s) dropped without being recorded');
      }
    }
    queue.push(payload);
    schedule();
    return { ok: false, queued: true, fatal: fatal, reason: reason };
  }

  function schedule() {
    if (timer || !queue.length) return;
    var wait = RETRY_MS[Math.min(retryAt, RETRY_MS.length - 1)];
    timer = setTimeout(function () { timer = null; flush(); }, wait);
  }

  function flush() {
    if (!queue.length) { retryAt = 0; return Promise.resolve({ flushed: 0 }); }
    var t = token();
    if (!t) { queue.length = 0; retryAt = 0; return Promise.resolve({ flushed: 0 }); }

    // One at a time and in order, so the chain reflects the order things
    // happened rather than the order the network recovered in.
    var batch = queue.slice();
    queue.length = 0;
    var sent = 0;
    return batch.reduce(function (chain, payload) {
      return chain.then(function () {
        return send(payload, false).then(function (r) { if (r.ok) sent++; });
      });
    }, Promise.resolve()).then(function () {
      retryAt = queue.length ? retryAt + 1 : 0;
      schedule();
      return { flushed: sent, remaining: queue.length };
    });
  }

  if (root.addEventListener) {
    root.addEventListener('online', function () { retryAt = 0; flush(); });
  }

  root.CygenixAudit = {
    record: record,
    recordAssistant: recordAssistant,
    diff: diff,
    flush: flush,
    // Exposed for tests and for a diagnostics panel; not part of the
    // recording contract.
    pending: function () { return queue.length; },
    droppedCount: function () { return dropped; },
  };

  if (typeof module === 'object' && module.exports) {
    module.exports = root.CygenixAudit;
  }
})(typeof window !== 'undefined' ? window : globalThis);
