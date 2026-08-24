/* ============================================================================
   cygenix-busy.js — one way of saying "this is working".
   ----------------------------------------------------------------------------
   THE PROBLEM

   The slowest things in this console are the ones with the least feedback. An
   AI mapping call takes ten to thirty seconds; reading a wide schema can take
   longer. Until now each place invented its own signal — a button that said
   "Loading…", a status line, or nothing at all — and several said nothing
   because the icon sweep replaced their glyph with an empty string, leaving a
   blank disabled button for the whole call. A user watching a blank button has
   no way to tell working from hung.

   WHAT THIS PROVIDES

   Three layers, because they answer three different questions:

     "is anything happening?"  → a thin indeterminate bar across the top of the
                                 window, visible from anywhere on the page
     "what is happening?"      → the button that started it shows a spinner and
                                 keeps its label
     "is it stuck?"            → after a few seconds an elapsed counter appears;
                                 a spinner alone reads as hung once it has been
                                 turning for twenty seconds

   Determinate work (n of m tables) can call progress() and the top bar fills
   properly instead of sweeping.

   Reference-counted: three overlapping operations share one bar, and it
   disappears when the last finishes. Every start() MUST be matched by done(),
   so callers use try/finally — a leaked token would leave the bar up forever.

   Node-requirable: the label, elapsed formatting and ref-counting are testable
   without a browser. The DOM half only wakes up when there is a document.
   ========================================================================== */
(function (root, factory) {
  var api = factory(root);
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixBusy) root.CygenixBusy = api;
})(typeof window !== 'undefined' ? window : this, function (root) {
'use strict';

/* Long enough that a quick call never flashes a number at you, short enough
   that a slow one starts reassuring you before you reach for the reload. */
var ELAPSED_AFTER_MS = 2500;
var TICK_MS = 500;

/* A very fast operation should not flash the bar — a 90ms flicker reads as a
   glitch, not as progress. Nothing is shown until the work outlives this. */
var SHOW_AFTER_MS = 180;

var _seq = 0;
var _live = {};          // token -> record
var _tickTimer = null;
var _revealTimer = null; // the one-shot that reveals the bar at SHOW_AFTER_MS

function liveCount() { return Object.keys(_live).length; }

/* "8s", "1m 04s" — seconds up to a minute, then minutes. No decimals: a
   counter that twitches ten times a second is noise, not information. */
function formatElapsed(ms) {
  var s = Math.floor(ms / 1000);
  if (s < 60) return s + 's';
  var m = Math.floor(s / 60);
  var r = s % 60;
  return m + 'm ' + (r < 10 ? '0' : '') + r + 's';
}

/* The label a busy button shows. The original label is kept — "Working…" on
   its own loses which button you pressed — and the elapsed time is appended
   once the work has run long enough to be worth timing. */
function busyLabel(base, ms, showElapsed) {
  var label = base || 'Working';
  if (showElapsed && ms >= ELAPSED_AFTER_MS) return label + ' · ' + formatElapsed(ms);
  return label;
}

/* Aggregate progress across everything running: determinate only if EVERY
   live operation is determinate, because one unknown makes the total unknown
   and a bar that claims 60% while something unmeasured runs is a lie. */
function aggregate(now) {
  var tokens = Object.keys(_live);
  if (!tokens.length) return { active: false, determinate: false, fraction: 0, oldestMs: 0 };
  var done = 0, total = 0, determinate = true, oldest = 0;
  for (var i = 0; i < tokens.length; i++) {
    var r = _live[tokens[i]];
    if (r.total > 0) { done += r.done; total += r.total; }
    else determinate = false;
    var age = now - r.startedAt;
    if (age > oldest) oldest = age;
  }
  return {
    active: true,
    determinate: determinate && total > 0,
    fraction: total > 0 ? Math.max(0, Math.min(1, done / total)) : 0,
    oldestMs: oldest,
  };
}

/* ── DOM ─────────────────────────────────────────────────────────────────── */

var hasDom = typeof document !== 'undefined';
var el = { bar: null, fill: null };

function injectStyles() {
  if (!hasDom || document.getElementById('cyg-busy-css')) return;
  var css = document.createElement('style');
  css.id = 'cyg-busy-css';
  css.textContent = [
    /* Above modals and the assistant panel: "something is running" outranks
       whatever is on top of the page, or it is invisible exactly when a modal
       is the thing waiting. */
    '.cygbusy{position:fixed;top:0;left:0;right:0;height:3px;z-index:500;',
    '  pointer-events:none;opacity:0;transition:opacity .15s ease}',
    '.cygbusy.on{opacity:1}',
    '.cygbusy-fill{height:100%;background:var(--accent,#4a5bd6);',
    '  box-shadow:0 0 8px var(--accent-glow,rgba(74,91,214,.5))}',
    /* Indeterminate: a shuttle that sweeps. Determinate: a width we set. */
    '.cygbusy.indet .cygbusy-fill{width:35%;animation:cygbusy-sweep 1.15s ease-in-out infinite}',
    '.cygbusy.det .cygbusy-fill{transition:width .25s ease}',
    '@keyframes cygbusy-sweep{0%{margin-left:-35%}100%{margin-left:100%}}',
    /* The in-button spinner. currentColor so it works on every button style
       the console has without a variant per colour. */
    '.cygbusy-spin{display:inline-block;width:10px;height:10px;margin-right:6px;',
    '  vertical-align:-1px;border:1.5px solid currentColor;border-right-color:transparent;',
    '  border-radius:50%;animation:cygbusy-rot .7s linear infinite;opacity:.85}',
    '@keyframes cygbusy-rot{to{transform:rotate(360deg)}}',
    /* A busy button must not look clickable, but must stay readable — the
       usual .45 disabled fade makes the spinner hard to see. */
    '.cygbusy-btn{cursor:progress!important;opacity:.9!important}',
    /* Reduced motion: keep the signals, drop the movement. The bar becomes a
       steady tint and the spinner a steady ring; the elapsed counter still
       ticks, which is the part that actually says "not stuck". */
    '@media (prefers-reduced-motion:reduce){',
    '  .cygbusy.indet .cygbusy-fill{animation:none;width:100%;opacity:.55}',
    '  .cygbusy-spin{animation:none}}',
  ].join('\n');
  document.head.appendChild(css);
}

function ensureBar() {
  if (!hasDom || el.bar) return;
  injectStyles();
  var bar = document.createElement('div');
  bar.className = 'cygbusy';
  bar.id = 'cygBusyBar';
  bar.setAttribute('role', 'progressbar');
  bar.setAttribute('aria-hidden', 'true');
  var fill = document.createElement('div');
  fill.className = 'cygbusy-fill';
  bar.appendChild(fill);
  (document.body || document.documentElement).appendChild(bar);
  el.bar = bar; el.fill = fill;
}

function paint() {
  if (!hasDom) return;
  ensureBar();
  if (!el.bar) return;
  var now = Date.now();
  var agg = aggregate(now);

  // Nothing running, or nothing running long enough to be worth showing.
  if (!agg.active || agg.oldestMs < SHOW_AFTER_MS) {
    el.bar.classList.remove('on', 'indet', 'det');
    el.bar.setAttribute('aria-hidden', 'true');
    el.bar.removeAttribute('aria-valuenow');
    if (!agg.active) el.fill.style.width = '';
    return;
  }

  el.bar.classList.add('on');
  el.bar.setAttribute('aria-hidden', 'false');
  if (agg.determinate) {
    el.bar.classList.add('det'); el.bar.classList.remove('indet');
    el.fill.style.width = Math.round(agg.fraction * 100) + '%';
    el.bar.setAttribute('aria-valuenow', String(Math.round(agg.fraction * 100)));
  } else {
    el.bar.classList.add('indet'); el.bar.classList.remove('det');
    el.fill.style.width = '';
    el.bar.removeAttribute('aria-valuenow');
  }

  // Repaint every busy button's label so its elapsed counter advances.
  var tokens = Object.keys(_live);
  for (var i = 0; i < tokens.length; i++) {
    var r = _live[tokens[i]];
    if (!r.btn) continue;
    var ms = now - r.startedAt;
    var text = busyLabel(r.label, ms, true);
    if (r._painted === text) continue;
    r._painted = text;
    r.btn.innerHTML = '<span class="cygbusy-spin"></span>' + escapeHtml(text);
  }
}

function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
    return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
  });
}

function startTicking() {
  if (typeof setInterval === 'undefined') return;
  if (!_tickTimer) _tickTimer = setInterval(paint, TICK_MS);
  // The interval alone is not enough. paint() hides the bar until the work has
  // outlived SHOW_AFTER_MS, and with a 500ms tick the first repaint after that
  // threshold could be half a second late — so a 700ms operation showed the
  // bar only as it finished, or never. This one-shot lands exactly on the
  // threshold, which is when the bar is supposed to appear.
  if (!_revealTimer) {
    _revealTimer = setTimeout(function () { _revealTimer = null; paint(); }, SHOW_AFTER_MS + 10);
  }
}
function stopTicking() {
  if (liveCount() > 0) return;
  if (_tickTimer) { clearInterval(_tickTimer); _tickTimer = null; }
  if (_revealTimer) { clearTimeout(_revealTimer); _revealTimer = null; }
}

/* ── public API ──────────────────────────────────────────────────────────── */

/**
 * Mark the start of a slow operation.
 *
 *   const t = CygenixBusy.start('Mapping columns', { button: '#ai-map-btn-0' });
 *   try { ... } finally { t.done(); }
 *
 * opts.button  — element or selector; gets a spinner, keeps its label, is
 *                disabled for the duration and restored exactly on done().
 * opts.label   — button label while busy (defaults to its current text).
 * opts.total   — for determinate work; call t.progress(n) as it advances.
 */
function start(what, opts) {
  var o = opts || {};
  var token = 'b' + (++_seq);
  var btn = null;
  if (hasDom && o.button) {
    btn = (typeof o.button === 'string') ? document.querySelector(o.button) : o.button;
  }
  var rec = {
    token: token,
    what: what || 'Working',
    startedAt: Date.now(),
    total: Number(o.total) > 0 ? Number(o.total) : 0,
    done: 0,
    btn: btn,
    // A blank label is what the icon sweep left behind on several buttons, so
    // fall back to the operation name rather than showing an empty spinner.
    label: o.label || (btn ? (btn.textContent || '').trim() : '') || what || 'Working',
    _restore: btn ? { html: btn.innerHTML, disabled: btn.disabled } : null,
    _painted: null,
  };
  _live[token] = rec;

  if (btn) {
    btn.disabled = true;
    btn.classList.add('cygbusy-btn');
    btn.setAttribute('aria-busy', 'true');
    rec._painted = busyLabel(rec.label, 0, true);
    btn.innerHTML = '<span class="cygbusy-spin"></span>' + escapeHtml(rec._painted);
  }

  startTicking();
  paint();

  return {
    token: token,
    /** Advance determinate work. progress(3) or progress(3, 10). */
    progress: function (doneCount, total) {
      var r = _live[token];
      if (!r) return;
      if (Number(total) > 0) r.total = Number(total);
      r.done = Math.max(0, Number(doneCount) || 0);
      paint();
    },
    /** Change what the button says mid-flight ("Sampling…" → "Scoring…"). */
    label: function (text) {
      var r = _live[token];
      if (!r) return;
      r.label = text || r.label;
      r._painted = null;
      paint();
    },
    /** Always call this, in a finally. */
    done: function () { finish(token); },
  };
}

function finish(token) {
  var r = _live[token];
  if (!r) return;
  delete _live[token];
  if (r.btn && r._restore) {
    r.btn.innerHTML = r._restore.html;
    r.btn.disabled = r._restore.disabled;
    r.btn.classList.remove('cygbusy-btn');
    r.btn.removeAttribute('aria-busy');
  }
  paint();
  stopTicking();
}

/** Wrap a promise-returning function. Returns its result; always clears. */
function around(what, opts, fn) {
  if (typeof opts === 'function') { fn = opts; opts = {}; }
  var t = start(what, opts);
  var out;
  try {
    out = fn(t);
  } catch (e) {
    t.done();
    throw e;
  }
  if (out && typeof out.then === 'function') {
    return out.then(
      function (v) { t.done(); return v; },
      function (e) { t.done(); throw e; }
    );
  }
  t.done();
  return out;
}

/* Last-resort cleanup: a token leaked by a caller that threw outside its own
   try/finally would otherwise leave the bar up until the page reloads. */
function clearAll() {
  Object.keys(_live).forEach(finish);
}

if (hasDom) {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', ensureBar);
  } else {
    ensureBar();
  }
  // A navigation abandons every in-flight operation on this page.
  window.addEventListener('pagehide', clearAll);
}

return {
  start: start,
  around: around,
  clearAll: clearAll,
  isBusy: function () { return liveCount() > 0; },
  count: liveCount,
  /* pure helpers, exported for the tests */
  __core: {
    formatElapsed: formatElapsed,
    busyLabel: busyLabel,
    aggregate: aggregate,
    _live: _live,
    ELAPSED_AFTER_MS: ELAPSED_AFTER_MS,
    SHOW_AFTER_MS: SHOW_AFTER_MS,
  },
};
});
