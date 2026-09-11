/* ============================================================================
   cygenix-tour.js — the guided walkthrough, driven by a script and not a model.
   ----------------------------------------------------------------------------
   WHY THIS IS NOT THE ASSISTANT DOING IT
   The obvious build is to let the model drive: give it a navigate action and
   ask it to show the user around. Four reasons that is the wrong answer here.

     1. A new user has no API key. The assistant's first and only message to
        them today is "No API key set" — so the one moment a tour is worth most
        is the one moment an LLM-driven tour could not run at all. This engine
        runs with no key, no network and no tokens.
     2. It would cost money on every run, for a walkthrough whose content never
        changes.
     3. It would be different every time, which is the opposite of what a
        first-run experience needs.
     4. It could click things. This engine physically cannot: it navigates,
        expands sidebar groups, scrolls and draws overlays. There is no code
        path from here to an action in cygenix-assistant-actions.js.

   The model stays optional and additive: if a key IS set, a question typed
   mid-tour is answered with the current step as context, and the tour carries
   on. Without one, it says so and offers to continue.

   SURVIVING NAVIGATION
   This is a multi-page app, so every step that changes page is a full reload
   and everything in memory is lost. Tour state lives in sessionStorage and is
   picked up again by whichever page loads next — the engine re-opens the
   panel, re-renders the transcript and re-draws the current step. lastStep
   goes to localStorage instead, so "resume" still works tomorrow.

   WHAT IT TOUCHES IN THE ASSISTANT
   Four small hooks, added to cygenix-assistant.js rather than reached into:
   an input interceptor, a key interceptor, an extra-panel renderer, and an
   open/refresh pair. The assistant's own message array is left alone — tour
   cards are NOT pushed into it, because that array is the Anthropic
   conversation and a card in it would be sent to the model as a user turn.
   ========================================================================== */
(function () {
'use strict';

var STEPS = (typeof window !== 'undefined' && window.CygenixTourSteps) || [];

var SESSION_KEY = 'cyg_tour';            // active run — dies with the tab
var LAST_KEY    = 'cygenix_tour_last';   // for `resume`, survives the session
var SEEN_KEY    = 'cygenix_tour_offered';

/* The intent the brief specifies, matched before the API-key gate. Deliberately
   anchored on whole words: "detour" and "contour" are not requests for a tour. */
var INTENT = /\b(tour|show me around|walk ?me ?through|what can you do|getting started|how does this work)\b/i;

var REDUCED = typeof matchMedia === 'function'
  && matchMedia('(prefers-reduced-motion: reduce)').matches;

var A = null;                 // window.CygenixAssistant, once it exists
var SB = null;                // window.CygenixSidebar
var tour = null;              // { active, i, transcript:[], restore:{collapsed} }
var openedGroups = [];        // groups THIS tour expanded, to put back
var hiToken = 0;              // guards against a slow highlight from a past step

/* ── storage ────────────────────────────────────────────────────────────── */

function readSession() {
  try {
    var raw = sessionStorage.getItem(SESSION_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch (e) { return null; }
}
function writeSession() {
  try {
    if (tour && tour.active) sessionStorage.setItem(SESSION_KEY, JSON.stringify(tour));
    else sessionStorage.removeItem(SESSION_KEY);
  } catch (e) { /* a private window still gets a tour, it just will not resume */ }
}
function setLast(i) { try { localStorage.setItem(LAST_KEY, String(i)); } catch (e) {} }
function getLast() {
  try {
    var v = parseInt(localStorage.getItem(LAST_KEY), 10);
    return isNaN(v) ? null : v;
  } catch (e) { return null; }
}

/* ── which steps are actually available ─────────────────────────────────────
   A target that is not in the DOM, or is in it at zero size, belongs to a
   feature this user's role or feature flags have hidden. Showing a step that
   points at nothing would be worse than skipping it — so it is skipped, AND it
   is left out of the denominator, because "12 / 24" against a rail that only
   has 20 items visible is a promise the tour cannot keep.

   Only steps whose target lives in the persistent chrome (the sidebar, the
   assistant) can be judged from any page — a page-specific target is not in
   the DOM until you are on that page, which is not the same as hidden. So the
   test is deliberately narrow: it only drops a step whose selector is a
   sidebar or assistant selector AND is absent. */
function isChromeSelector(sel) {
  return /^\[data-(key|parent)=|^\.cyg-drive-btn|^\.cyga-/.test(sel || '');
}
function stepAvailable(s) {
  if (!s.target || !isChromeSelector(s.target)) return true;
  var el = document.querySelector(s.target);
  if (!el) return false;
  var r = el.getBoundingClientRect();
  // A collapsed group's children have zero height but are not hidden: their
  // parent row is what gets spotlighted, and that is always measurable.
  return r.width > 0 || !!el.closest('.cyg-nav-children');
}
function liveSteps() { return STEPS.filter(stepAvailable); }

/* Index arithmetic runs over the LIVE list, so skipped steps never appear in
   the counter and never need a "step 7 of 24 (some hidden)" caveat. */
function stepAt(i) { var l = liveSteps(); return l[Math.max(0, Math.min(i, l.length - 1))]; }
function total() { return Math.max(0, liveSteps().length - 1); }   // step 0 is the intro

/* ── sidebar handling ───────────────────────────────────────────────────── */

function expandGroupFor(el) {
  if (!el) return;
  var parentRow = el.matches('.cyg-nav-parent')
    ? el
    : (el.closest('.cyg-nav-children')
        ? el.closest('.cyg-nav-children').previousElementSibling : null);
  if (!parentRow) return;
  var key = parentRow.dataset.parent;
  var kids = document.querySelector('.cyg-nav-children[data-children="' + key + '"]');
  if (!kids || kids.classList.contains('open')) return;
  kids.classList.add('open');
  parentRow.setAttribute('aria-expanded', 'true');
  var chev = parentRow.querySelector('.cyg-nav-chev');
  if (chev) chev.textContent = '▾';
  // Remembered so it can be put back: the tour is a visit, not a rearrangement
  // of someone's sidebar.
  if (openedGroups.indexOf(key) === -1) openedGroups.push(key);
}

function collapseTourGroups(keep) {
  openedGroups.slice().forEach(function (key) {
    if (key === keep) return;
    var row = document.querySelector('.cyg-nav-parent[data-parent="' + key + '"]');
    var kids = document.querySelector('.cyg-nav-children[data-children="' + key + '"]');
    // A group holding the page you are actually on stays open — the sidebar's
    // own rule, and overriding it would hide the current location.
    if (row && row.classList.contains('child-active')) return;
    if (kids) kids.classList.remove('open');
    if (row) {
      row.setAttribute('aria-expanded', 'false');
      var chev = row.querySelector('.cyg-nav-chev');
      if (chev) chev.textContent = '▸';
    }
    openedGroups.splice(openedGroups.indexOf(key), 1);
  });
}

/* The rail has to be open to point at anything in it. Whatever it was before
   is recorded once, at the start, and restored when the tour ends. */
function ensureRailOpen() {
  if (!SB || typeof SB.isCollapsed !== 'function') return;
  if (!SB.isCollapsed()) return;
  SB.setCollapsed(false);
  document.body.classList.remove('cyg-collapsed');
}

/* ── navigation ─────────────────────────────────────────────────────────────
   Through the sidebar's own handler, never a synthetic click: handleClick
   knows which keys are dashboard views (stash cyg_goto, go to /dashboard#goto=)
   and which are separate pages, and a faked click would have to re-derive that
   and then drift from it. */
function currentPageKey() {
  var mount = document.getElementById('cyg-sidebar-mount');
  if (mount && mount.dataset.active) return mount.dataset.active;
  var activeEl = document.querySelector('.cyg-sidebar .cyg-nav-item.active[data-key]');
  return activeEl ? activeEl.dataset.key : null;
}

function navigateTo(key) {
  if (!key || key === currentPageKey()) return false;
  if (SB && typeof SB.navigate === 'function') { SB.navigate(key); return true; }
  return false;
}

/* ── overlays ───────────────────────────────────────────────────────────── */

function injectStyles() {
  if (document.getElementById('cyg-tour-css')) return;
  var s = document.createElement('style');
  s.id = 'cyg-tour-css';
  /* Tokens only — the tour has no palette of its own, so it retones with the
     rest of the console when somebody picks a different theme. z-index sits
     under the assistant (290) on purpose: the panel holds the text that
     explains the stop, and dimming the explanation would be absurd. */
  s.textContent = [
    '#cygTourSpot{position:fixed;border-radius:9px;pointer-events:none;z-index:180;',
    '  box-shadow:0 0 0 9999px rgba(10,12,28,.45);opacity:0;',
    '  transition:all .35s cubic-bezier(.4,0,.2,1),opacity .2s}',
    '#cygTourSpot.on{opacity:1}',
    '#cygTourSpot::after{content:"";position:absolute;inset:-4px;border:2px solid var(--accent);',
    '  border-radius:11px;animation:cygTourPulse 1.6s infinite}',
    '#cygTourRegion{position:fixed;border:2px dashed var(--accent);border-radius:12px;',
    '  pointer-events:none;z-index:181;transition:all .35s;opacity:0}',
    '#cygTourRegion.on{opacity:1}',
    '#cygTourCallout{position:fixed;z-index:182;background:var(--bg2);color:var(--text);',
    '  border:1px solid var(--border2);border-radius:8px;padding:6px 11px;font-size:12.5px;',
    '  font-weight:600;box-shadow:var(--shadow-strong,0 6px 20px rgba(0,0,0,.25));',
    '  pointer-events:none;transition:all .35s;opacity:0;white-space:nowrap;max-width:40vw;',
    '  overflow:hidden;text-overflow:ellipsis}',
    '#cygTourCallout.on{opacity:1}',
    '@keyframes cygTourPulse{0%{opacity:1;transform:scale(1)}70%{opacity:0;transform:scale(1.06)}100%{opacity:0}}',
    /* A target inside the panel gets an outline instead: it is already above
       the dimming layer, so a spotlight there would cut a hole in nothing. */
    '.cyg-tour-outline{outline:2px solid var(--accent);outline-offset:-2px;border-radius:8px}',
    /* Step card */
    /* flex:0 0 auto is load-bearing. The assistant body is a column flex
       container, so by default every card is a flex ITEM and shrinks once the
       transcript is taller than the panel — and because the card is
       overflow:hidden, shrinking clips its own text mid-sentence. Eight stops
       in, the cards had been squeezed down to their header bars and the step
       you were actually on was cut in half by the input box. */
    '.cyg-tour-card{border:1px solid var(--border);border-radius:10px;overflow:hidden;',
    '  background:var(--bg2);margin-top:4px;flex:0 0 auto}',
    '.cyg-tour-card .tc-head{display:flex;align-items:center;gap:8px;padding:8px 11px;',
    '  background:var(--bg3);border-bottom:1px solid var(--border)}',
    '.cyg-tour-card .tc-sec{font-family:var(--mono,monospace);font-size:10px;letter-spacing:.08em;',
    '  text-transform:uppercase;color:var(--accent)}',
    '.cyg-tour-card .tc-count{margin-left:auto;font-family:var(--mono,monospace);font-size:10px;',
    '  color:var(--text3);font-variant-numeric:tabular-nums}',
    '.cyg-tour-card .tc-bar{height:3px;background:var(--bg4)}',
    '.cyg-tour-card .tc-bar i{display:block;height:100%;background:var(--accent);transition:width .3s}',
    '.cyg-tour-card .tc-body{padding:11px}',
    '.cyg-tour-card h4{margin:0 0 5px;font-size:13.5px;font-weight:600}',
    '.cyg-tour-card p{margin:0;font-size:13px;line-height:1.6;color:var(--text2)}',
    '.cyg-tour-card .tc-ctl{display:flex;gap:7px;align-items:center;padding:0 11px 11px;flex-wrap:wrap}',
    '.cyg-tour-card .tc-prompt{width:100%;font-size:11.5px;color:var(--text3);margin-bottom:2px}',
    '.cyg-tour-card.past{opacity:.5}',
    '.cyg-tour-card.past .tc-ctl{display:none}',
    '.cyg-tour-pill{background:var(--accent);color:#fff;border-radius:10px;padding:1px 8px;',
    '  font-weight:600;font-size:10px;letter-spacing:.04em}',
    '@media (prefers-reduced-motion:reduce){',
    '  #cygTourSpot,#cygTourRegion,#cygTourCallout{transition:none}',
    '  #cygTourSpot::after{animation:none}}',
  ].join('');
  document.head.appendChild(s);
}

function overlay(id) {
  var e = document.getElementById(id);
  if (!e) { e = document.createElement('div'); e.id = id; e.setAttribute('aria-hidden', 'true'); document.body.appendChild(e); }
  return e;
}

function place(el, target, pad) {
  var r = target.getBoundingClientRect();
  el.style.left = (r.left - pad) + 'px';
  el.style.top = (r.top - pad) + 'px';
  el.style.width = (r.width + pad * 2) + 'px';
  el.style.height = (r.height + pad * 2) + 'px';
}

function clearOverlays() {
  hiToken++;
  ['cygTourSpot', 'cygTourRegion', 'cygTourCallout'].forEach(function (id) {
    var e = document.getElementById(id);
    if (e) e.classList.remove('on');
  });
  Array.prototype.forEach.call(document.querySelectorAll('.cyg-tour-outline'), function (e) {
    e.classList.remove('cyg-tour-outline');
  });
}

function highlight(step) {
  clearOverlays();
  var tok = ++hiToken;                    // anything from an earlier step is now stale
  if (!step.target) return;
  var t = document.querySelector(step.target);
  if (!t) return;

  if (t.closest('.cyga')) { t.classList.add('cyg-tour-outline'); return; }

  ensureRailOpen();
  expandGroupFor(t);
  // Only scroll when the target is actually out of view. scrollIntoView on
  // something already visible still animates the nearest scroller by a few
  // pixels, and those pixels arrive AFTER the spotlight is placed — which
  // reads as a spotlight sitting slightly off the thing it is pointing at.
  var vr = t.getBoundingClientRect();
  var offscreen = vr.top < 0 || vr.bottom > (window.innerHeight || 0);
  if (offscreen) {
    try { t.scrollIntoView({ block: 'nearest', behavior: REDUCED ? 'auto' : 'smooth' }); }
    catch (e) { t.scrollIntoView(); }
  }

  setTimeout(function () {
    if (tok !== hiToken) return;          // the user pressed Y again; drop this one
    var r = t.getBoundingClientRect();
    if (!r.width || !r.height) return;
    var spot = overlay('cygTourSpot');
    place(spot, t, 4);
    spot.classList.add('on');
    var callout = overlay('cygTourCallout');
    callout.textContent = String(step.title || '').replace(/&amp;/g, '&');
    callout.style.left = (r.right + 14) + 'px';
    callout.style.top = (r.top + r.height / 2 - 16) + 'px';
    callout.classList.add('on');
    // scrollIntoView's smooth behaviour is still animating at this point, so
    // the rect just measured can be tens of pixels stale by the time it stops.
    // One more pass once it has settled, guarded by the same token so a step
    // the user has already left cannot move the current spotlight.
    if (!REDUCED && offscreen) setTimeout(function () { if (tok === hiToken) reposition(); }, 420);
  }, (REDUCED || !offscreen) ? 0 : 180);

  if (step.region) {
    setTimeout(function () {
      if (tok !== hiToken) return;
      var rg = document.querySelector(step.region);
      if (!rg) return;
      var rr = rg.getBoundingClientRect();
      if (!rr.height) return;
      var reg = overlay('cygTourRegion');
      place(reg, rg, 8);
      reg.classList.add('on');
    }, REDUCED ? 0 : 240);
  }
}

function reposition() {
  if (!tour || !tour.active) return;
  var s = stepAt(tour.i);
  if (!s || !s.target) return;
  var t = document.querySelector(s.target);
  if (t && !t.closest('.cyga')) {
    var spot = document.getElementById('cygTourSpot');
    if (spot && spot.classList.contains('on')) {
      place(spot, t, 4);
      var r = t.getBoundingClientRect();
      var c = document.getElementById('cygTourCallout');
      if (c) { c.style.left = (r.right + 14) + 'px'; c.style.top = (r.top + r.height / 2 - 16) + 'px'; }
    }
  }
  if (s.region) {
    var rg = document.querySelector(s.region);
    var reg = document.getElementById('cygTourRegion');
    if (rg && reg && reg.classList.contains('on')) place(reg, rg, 8);
  }
}

/* ── the transcript ─────────────────────────────────────────────────────────
   Its own list, rendered into the assistant body through a hook. Deliberately
   NOT state.messages: that array is the Anthropic conversation, and a step card
   pushed into it would be sent to the model as a user turn on the next
   question — which is both wasteful and wrong. */

function pushCard(step, index) {
  tour.transcript.push({ kind: 'step', id: step.id, i: index });
  writeSession();
}
function pushNote(html) {
  tour.transcript.push({ kind: 'note', html: html });
  writeSession();
}
function pushSaid(text) {
  tour.transcript.push({ kind: 'user', text: text });
  writeSession();
}

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
    return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
  });
}

function cardHtml(step, index, isCurrent) {
  var n = index;
  var tot = total();
  var pct = tot ? Math.round(n / tot * 100) : 0;
  var ctl;
  if (!isCurrent) ctl = '';
  else if (step.final) {
    ctl = '<button class="cyga-btn primary" data-tour-act="finish">Finish tour</button>'
        + '<button class="cyga-btn" data-tour-act="back">B · Back</button>';
  } else {
    ctl = '<div class="tc-prompt">'
        + (n === 0 ? 'Press <kbd>Y</kbd> to start, or type <b>Exit</b> to stop.'
                   : 'Press <kbd>Y</kbd> to continue, <kbd>B</kbd> to go back, or type <b>Exit</b> to stop.')
        + '</div>'
        + '<button class="cyga-btn primary" data-tour-act="next">' + (n === 0 ? 'Y · Start' : 'Y · Continue') + '</button>'
        // Labelled with its key, like Continue: a button that says only "Back"
        // does not tell you B works, and the keyboard route is the faster one
        // once someone has done two stops.
        + '<button class="cyga-btn" data-tour-act="back"' + (n === 0 ? ' disabled' : '') + '>B · Back</button>'
        + '<button class="cyga-btn" data-tour-act="exit">Exit</button>';
  }
  return '<div class="cyg-tour-card' + (isCurrent ? '' : ' past') + '">'
    + '<div class="tc-head"><span class="tc-sec">' + step.section + '</span>'
    + '<span class="tc-count">' + (n === 0 ? 'Intro' : n + ' / ' + tot) + '</span></div>'
    + '<div class="tc-bar"><i style="width:' + pct + '%"></i></div>'
    + '<div class="tc-body"><h4>' + step.title + '</h4><p>' + step.body + '</p></div>'
    + '<div class="tc-ctl">' + ctl + '</div></div>';
}

/* Handed to the assistant, which appends it under the conversation. */
function renderPanel() {
  if (!tour || !tour.transcript.length) return '';
  var live = liveSteps();
  var last = tour.transcript.length - 1;
  return tour.transcript.map(function (row, idx) {
    if (row.kind === 'user') return '<div class="cyga-msg user">' + esc(row.text) + '</div>';
    if (row.kind === 'note') return '<div class="cyga-msg assistant">' + row.html + '</div>';
    var step = live[row.i] || STEPS.filter(function (s) { return s.id === row.id; })[0];
    if (!step) return '';
    return cardHtml(step, row.i, tour.active && idx === last);
  }).join('');
}

/* ── the engine ─────────────────────────────────────────────────────────── */

function show() {
  var live = liveSteps();
  if (tour.i >= live.length) tour.i = live.length - 1;
  var s = live[tour.i];
  if (!s) return end('done');

  pushCard(s, tour.i);
  setLast(tour.i);
  writeSession();

  // Navigating reloads the page, so the card must be in sessionStorage before
  // we go: the next page reads it back and renders the transcript we just
  // added to. Everything after this line may never run.
  var moved = navigateTo(s.page);
  if (moved) return;

  if (A) A.refresh();
  collapseTourGroups(null);
  requestAnimationFrame(function () { highlight(s); });
}

function start(from) {
  if (!STEPS.length) return;
  injectStyles();
  // However the tour was reached — a chip, a typed sentence, the first-run
  // modal — a full-screen scrim over the thing being pointed at makes the
  // spotlight meaningless. Close it.
  var onboarding = document.getElementById('onboarding-modal');
  if (onboarding && onboarding.style.display !== 'none') {
    if (typeof window.dismissOnboarding === 'function') window.dismissOnboarding();
    else onboarding.style.display = 'none';
  }
  tour = {
    active: true,
    i: Math.max(0, Math.min(from || 0, liveSteps().length - 1)),
    transcript: (tour && tour.transcript) || [],
    restore: { collapsed: SB && typeof SB.isCollapsed === 'function' ? SB.isCollapsed() : false }
  };
  openedGroups = [];
  if (A) { A.open(); A.setTourMode(true); }
  emit('tour_started', { step: tour.i });
  show();
}

function next() {
  if (!tour || !tour.active) return;
  var live = liveSteps();
  if (tour.i < live.length - 1) { tour.i++; emit('tour_step_viewed', { step: tour.i }); show(); }
  else end('done');
}
function back() {
  if (!tour || !tour.active || tour.i <= 0) return;
  tour.i--;
  show();
}
function jump(i) {
  if (!tour || !tour.active) return start(i);
  tour.i = i;
  show();
}

function end(reason) {
  if (!tour) return;
  var atTitle = (stepAt(tour.i) || {}).title || '';
  setLast(tour.i);
  clearOverlays();
  collapseTourGroups(null);
  // Put the rail back the way it was found.
  if (tour.restore && tour.restore.collapsed && SB && typeof SB.setCollapsed === 'function') {
    SB.setCollapsed(true);
    document.body.classList.add('cyg-collapsed');
  }
  tour.active = false;
  pushNote(reason === 'done'
    ? 'Tour complete. Type <b>tour</b> any time to go round again.'
    : 'Tour stopped at <b>' + atTitle + '</b>. Type <b>resume</b> to pick up from there, '
      + 'or <b>tour</b> to start over.');
  emit(reason === 'done' ? 'tour_completed' : 'tour_exited', { step: tour.i });
  try { sessionStorage.removeItem(SESSION_KEY); } catch (e) {}
  if (A) { A.setTourMode(false); A.refresh(); }
}

/* Analytics is optional and must never be a dependency: if nothing is
   listening, this is a no-op rather than a missing-function error. */
function emit(name, detail) {
  try { window.dispatchEvent(new CustomEvent('cygenix:' + name, { detail: detail || {} })); } catch (e) {}
}

/* ── input routing ──────────────────────────────────────────────────────── */

function findStepIndex(term) {
  term = String(term || '').toLowerCase().trim();
  if (!term) return -1;
  var live = liveSteps();
  for (var i = 0; i < live.length; i++) {
    var s = live[i];
    var hay = (s.title + ' ' + s.section).toLowerCase().replace(/&amp;/g, '&');
    if (hay.indexOf(term) !== -1) return i;
  }
  return -1;
}

/* Returns true when the tour has consumed the input, so the assistant does not
   also send it to the model. Called from submit(), BEFORE the API-key gate —
   which is the whole point: the people who most need this have no key. */
function handleInput(text) {
  text = String(text || '').trim();
  var low = text.toLowerCase();

  if (tour && tour.active) {
    if (/^(y|yes|next|continue|ok|go)$/.test(low) || low === '') { next(); return true; }
    pushSaid(text);
    if (/^(b|back|previous|prev)$/.test(low)) { back(); return true; }
    if (/^(exit|quit|stop|end|x|n|no|cancel)$/.test(low)) { end('exit'); return true; }
    var m = low.match(/^(?:go to|skip to|jump to|tour)\s+(.+)$/);
    if (m) {
      var k = findStepIndex(m[1]);
      if (k >= 0) { jump(k); return true; }
    }
    // Anything else is a real question. With a key the assistant answers it and
    // the tour stays up; without one, say so rather than failing silently.
    if (A && A.hasKey && A.hasKey()) {
      if (A) A.refresh();
      return false;                      // let the model answer; tour mode persists
    }
    pushNote('I can answer questions once an API key is added in '
      + '<a href="/dashboard#goto=project-settings" style="color:var(--accent)">Settings → General</a>. '
      + 'For now: press <kbd>Y</kbd> to continue, or type <b>Exit</b> to stop.');
    if (A) A.refresh();
    return true;
  }

  if (/^resume$/.test(low)) {
    var last = getLast();
    if (last !== null) { start(last); return true; }
  }
  if (INTENT.test(text)) {
    var jm = text.match(/\btour\s+(?:of\s+)?(.+)$/i);
    var idx = jm ? findStepIndex(jm[1]) : -1;
    start(idx > 0 ? idx : 0);
    return true;
  }
  return false;
}

/* Y and B are single keys, so they must only fire on an empty box — otherwise
   nobody could type the word "yesterday". */
function handleKey(e, inputValue) {
  if (!tour || !tour.active) return false;
  if (e.key === 'Escape') { e.preventDefault(); end('exit'); return true; }
  if (inputValue !== '') return false;
  if (e.key === 'y' || e.key === 'Y') { e.preventDefault(); next(); return true; }
  if (e.key === 'b' || e.key === 'B') { e.preventDefault(); back(); return true; }
  if (e.key === 'Enter') { e.preventDefault(); next(); return true; }
  return false;
}

function handleAct(act) {
  if (act === 'next') next();
  else if (act === 'back') back();
  else if (act === 'exit') end('exit');
  else if (act === 'finish') end('done');
}

/* ── boot ───────────────────────────────────────────────────────────────── */

function boot() {
  SB = window.CygenixSidebar || null;
  A = window.CygenixAssistant || null;
  if (!A || typeof A.setTourHooks !== 'function') return;   // assistant too old; stay silent

  injectStyles();
  A.setTourHooks({
    onInput: handleInput,
    onKey: handleKey,
    onAct: handleAct,
    render: renderPanel,
    isActive: function () { return !!(tour && tour.active); },
    onClose: function () { if (tour && tour.active) end('exit'); },
  });

  var saved = readSession();
  if (saved && saved.active) {
    tour = saved;
    openedGroups = [];
    A.open();
    A.setTourMode(true);
    A.refresh();
    // The page we wanted is the page we are on now, so draw the step rather
    // than navigating again — otherwise a step whose page matches the current
    // one would loop.
    var s = stepAt(tour.i);
    if (s) requestAnimationFrame(function () { collapseTourGroups(null); highlight(s); });
  }

  window.addEventListener('resize', reposition);
  var nav = document.querySelector('.cyg-sidebar .cyg-nav');
  if (nav) nav.addEventListener('scroll', reposition, { passive: true });
}

window.CygenixTour = {
  start: start, next: next, back: back, end: end,
  handleInput: handleInput,
  isActive: function () { return !!(tour && tour.active); },
  // For tests: the derived list, so a spec can assert what a role actually sees.
  __liveSteps: liveSteps,
  __findStepIndex: findStepIndex,
  __INTENT: INTENT,
  markOffered: function () { try { localStorage.setItem(SEEN_KEY, '1'); } catch (e) {} },
  wasOffered: function () { try { return localStorage.getItem(SEEN_KEY) === '1'; } catch (e) { return false; } },
};

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
else boot();
})();
