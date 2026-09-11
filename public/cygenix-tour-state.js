// cygenix-tour-state.js — the tour's single source of truth.
//
// Pure: state in, state out. No DOM, no storage, no assistant. That is the
// whole point of splitting it out, because the bug this module exists to fix
// was caused by tour position being derived from the DOM.
//
// ── The bug ───────────────────────────────────────────────────────────────
//
// A user on step 5 (Connections) typed "can you connect and test these
// connections". The assistant answered, and the tour card came back showing
// step 4 (Project). Their place was lost.
//
// The cause was not the interruption. It was that a step's IDENTITY was its
// index into a list recomputed from the live DOM on every read:
//
//     liveSteps()  = STEPS.filter(stepAvailable)   // queries the document
//     stepAt(i)    = liveSteps()[i]
//     renderPanel  = ... live[row.i] ...           // re-resolved every paint
//
// stepAvailable() asks whether a step's target element exists and has a
// non-zero box. That answer changes constantly and for reasons that have
// nothing to do with the tour: a sidebar group collapses, the assistant
// panel's own targets come and go, a nav item appears a moment after paint
// once the user's roles resolve. Every one of those silently renumbers the
// itinerary underneath a stored integer.
//
// So `tour.i` stayed 5 the whole time and still pointed at a different step,
// because the list had shifted. show() made it worse by clamping —
// `if (tour.i >= live.length) tour.i = live.length - 1` — which let a
// re-render REWIND the user's position, the exact thing the brief says must
// only happen through a transition.
//
// ── The fix ───────────────────────────────────────────────────────────────
//
// The itinerary is frozen when the tour starts: `plan` is an array of step
// ids, decided once, from the DOM as it was at that moment. After that:
//
//   * position is a stepId, and stepIndex is derived from it;
//   * availability is never consulted again for numbering;
//   * only the transitions below may change position — not a re-render, not
//     a route change, not an agent action, not a clamp.
//
// A step in the plan whose target has since disappeared is still IN the
// plan and still counted; the renderer draws its card without a spotlight
// rather than renumbering everything after it. Showing a card that points at
// nothing costs the user a shrug. Renumbering costs them their place.

(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixTourState) root.CygenixTourState = api;
})(typeof window !== 'undefined' ? window : this, function () {
  'use strict';

  var TOUR_ID = 'welcome-v1';
  var TOUR_VERSION = 1;

  var STATUSES = ['active', 'paused', 'completed', 'exited'];
  var PAUSE_REASONS = ['question', 'action', 'closed', 'navigation'];

  function nowIso(now) {
    return new Date(now === undefined ? Date.now() : now).toISOString();
  }

  // ── Creation ────────────────────────────────────────────────────────────
  //
  // `plan` is the frozen itinerary: the ids of the steps this run will visit,
  // in order, decided once. Passing it in rather than computing it here is
  // what keeps this module free of the DOM.
  function create(plan, startIndex, now) {
    var p = (plan || []).slice();
    var i = clampIndex(startIndex || 0, p);
    return {
      tourId: TOUR_ID,
      tourVersion: TOUR_VERSION,
      plan: p,
      stepIndex: i,
      stepId: p[i] || null,
      status: 'active',
      pausedReason: null,
      updatedAt: nowIso(now),
    };
  }

  function clampIndex(i, plan) {
    var n = (plan || []).length;
    if (!n) return 0;
    i = parseInt(i, 10);
    if (isNaN(i) || i < 0) return 0;
    return i > n - 1 ? n - 1 : i;
  }

  // Every transition goes through here, so there is exactly one place that
  // writes stepIndex/stepId/updatedAt and exactly one place to look when they
  // are wrong.
  function moveTo(st, index, now) {
    var i = clampIndex(index, st.plan);
    return Object.assign({}, st, {
      stepIndex: i,
      stepId: st.plan[i] || null,
      updatedAt: nowIso(now),
    });
  }

  // ── Transitions ─────────────────────────────────────────────────────────
  //
  // The complete list. Anything not here may not change position, and the
  // regression test asserts that by calling everything else and checking the
  // step did not move.

  function next(st, now) {
    if (!st || st.status === 'completed' || st.status === 'exited') return st;
    if (st.stepIndex >= st.plan.length - 1) return complete(st, now);
    var out = moveTo(st, st.stepIndex + 1, now);
    out.status = 'active';
    out.pausedReason = null;
    return out;
  }

  function back(st, now) {
    if (!st || st.status === 'completed' || st.status === 'exited') return st;
    if (st.stepIndex <= 0) return st;
    var out = moveTo(st, st.stepIndex - 1, now);
    out.status = 'active';
    out.pausedReason = null;
    return out;
  }

  // Jumping is still a transition — it is how "go to connections" works — but
  // it is explicit and it is the caller who resolved the target.
  function jump(st, index, now) {
    if (!st || st.status === 'completed' || st.status === 'exited') return st;
    var out = moveTo(st, index, now);
    out.status = 'active';
    out.pausedReason = null;
    return out;
  }

  // Pause holds the position exactly. This is the one that had to be right:
  // an interruption must be a status change and nothing else.
  function pause(st, reason, now) {
    if (!st || st.status !== 'active') return st;
    return Object.assign({}, st, {
      status: 'paused',
      pausedReason: PAUSE_REASONS.indexOf(reason) !== -1 ? reason : 'question',
      updatedAt: nowIso(now),
    });
  }

  function resume(st, now) {
    if (!st || st.status !== 'paused') return st;
    return Object.assign({}, st, {
      status: 'active',
      pausedReason: null,
      updatedAt: nowIso(now),
    });
  }

  function exit(st, now) {
    if (!st) return st;
    return Object.assign({}, st, { status: 'exited', pausedReason: null, updatedAt: nowIso(now) });
  }

  function complete(st, now) {
    if (!st) return st;
    return Object.assign({}, st, {
      status: 'completed', pausedReason: null,
      stepIndex: Math.max(0, st.plan.length - 1),
      stepId: st.plan[st.plan.length - 1] || st.stepId,
      updatedAt: nowIso(now),
    });
  }

  // ── Input routing ───────────────────────────────────────────────────────
  //
  // Case-insensitive, whitespace-trimmed, and EXACT — "yes, but what does
  // this page do?" is a question, not a Y. That is the difference between a
  // tour that can be interrupted and one that eats the interruption.
  //
  // Two tables, because the same word means different things in the two
  // states. While the tour is running, "no" is a way of stopping it. While it
  // is PAUSED mid-conversation, "no" is the user answering the assistant, and
  // routing it to exit would end the tour for saying no to a question.

  var ACTIVE_NEXT = /^(y|yes|next|continue|ok|go)$/;
  var ACTIVE_BACK = /^(b|back|previous|prev)$/;
  var ACTIVE_EXIT = /^(exit|quit|stop|end|x|n|no|cancel)$/;
  var JUMP = /^(?:go to|skip to|jump to|tour)\s+(.+)$/;

  var PAUSED_RESUME = /^(y|yes|resume|continue|next|ok|go)$/;
  var PAUSED_EXIT = /^(exit|quit|stop|end)$/;

  /**
   * route(text, status) -> { kind, term? }
   *   kind: 'next' | 'back' | 'exit' | 'resume' | 'jump' | 'ask'
   * 'ask' means: this is free text, hand it to the model. Everything else is
   * local and must never cost an API call.
   */
  function route(text, status) {
    var low = String(text == null ? '' : text).trim().toLowerCase();

    if (status === 'paused') {
      if (PAUSED_RESUME.test(low)) return { kind: 'resume' };
      if (PAUSED_EXIT.test(low)) return { kind: 'exit' };
      // Deliberately NOT treating '' as resume: a stray Enter while reading an
      // answer should do nothing, not silently restart the tour.
      if (low === '') return { kind: 'noop' };
      return { kind: 'ask', text: String(text).trim() };
    }

    if (low === '' || ACTIVE_NEXT.test(low)) return { kind: 'next' };
    if (ACTIVE_BACK.test(low)) return { kind: 'back' };
    if (ACTIVE_EXIT.test(low)) return { kind: 'exit' };
    var m = low.match(JUMP);
    if (m) return { kind: 'jump', term: m[1] };
    return { kind: 'ask', text: String(text).trim() };
  }

  function isCommand(text, status) {
    var k = route(text, status).kind;
    return k !== 'ask';
  }

  // ── Resolving a saved state against the current steps ───────────────────
  //
  // Versioning, per the brief: match by stepId. A saved position whose step no
  // longer exists falls back to the nearest surviving step in the same plan
  // rather than throwing or silently restarting.
  function resolve(st, stepsById) {
    if (!st || !st.plan || !st.plan.length) return { ok: false, reason: 'no plan' };
    var has = function (id) { return !!(stepsById && stepsById[id]); };

    if (st.stepId && has(st.stepId)) {
      // The id is what counts. If the index drifted — an older build, a
      // hand-edited blob — the id wins and the index is corrected to match.
      var at = st.plan.indexOf(st.stepId);
      return { ok: true, index: at >= 0 ? at : st.stepIndex, stepId: st.stepId, moved: at !== st.stepIndex };
    }

    // The saved step is gone. Walk outward from where it was for the nearest
    // one that still exists, so a deleted step costs a neighbour rather than
    // the whole position.
    for (var d = 1; d < st.plan.length; d++) {
      var before = st.stepIndex - d, after = st.stepIndex + d;
      if (after < st.plan.length && has(st.plan[after])) {
        return { ok: true, index: after, stepId: st.plan[after], moved: true, fellBack: true };
      }
      if (before >= 0 && has(st.plan[before])) {
        return { ok: true, index: before, stepId: st.plan[before], moved: true, fellBack: true };
      }
    }
    return { ok: false, reason: 'no step in the saved plan still exists' };
  }

  // The counter the card shows. Frozen with the plan, so it cannot wobble
  // between paints — "5 / 24" then "5 / 23" is the reader wondering what they
  // missed.
  function total(st) { return st && st.plan ? Math.max(0, st.plan.length - 1) : 0; }

  function isRunnable(st) {
    return !!st && (st.status === 'active' || st.status === 'paused');
  }

  return {
    TOUR_ID: TOUR_ID, TOUR_VERSION: TOUR_VERSION,
    STATUSES: STATUSES, PAUSE_REASONS: PAUSE_REASONS,
    create: create, clampIndex: clampIndex,
    next: next, back: back, jump: jump,
    pause: pause, resume: resume, exit: exit, complete: complete,
    route: route, isCommand: isCommand,
    resolve: resolve, total: total, isRunnable: isRunnable,
  };
});
