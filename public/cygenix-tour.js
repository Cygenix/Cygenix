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

/* ── Where the tour's two halves live ──────────────────────────────────────
   They are split deliberately, because they have different lifetimes.

   SESSION_KEY holds the VISIBLE half — the transcript of cards and answers in
   this tab — and dies with the tab. It is also the continuation marker: the
   tour moves between steps by navigating, which reloads the page, and finding
   this on boot is what tells the next page "you are mid-step, carry on"
   rather than "offer to resume".

   STATE_KEY holds the POSITION, per user, in localStorage, so it survives a
   reload, a closed panel and tomorrow morning. Finding a position with no
   session transcript means the user has come back rather than moved a step,
   and that is the case the restore prompt is for.

   TODO (cross-machine): this is localStorage only. cygenix-cosmos-sync.js
   mirrors a declared SYNC_KEYS list to Cosmos and would carry this across
   machines, but every key on that list is customer WORK — jobs, mappings,
   scripts — synced on a 3s write-behind with a merge strategy per field.
   Tour progress is neither customer work nor worth a merge strategy, so it
   is not on that list. If it should follow a user between machines, add
   'cygenix_tour_state' to SYNC_KEYS with a last-write-wins strategy and a
   classification in scripts/storage-inventory.js. */
var SESSION_KEY = 'cyg_tour';            // transcript + continuation marker
var STATE_KEY   = 'cygenix_tour_state';  // position, per user, durable
var LAST_KEY    = 'cygenix_tour_last';   // legacy index, read for migration only
var SEEN_KEY    = 'cygenix_tour_offered';
var CREDIT_KEY  = 'cygenix_tour_credit_notice';   // shown once per session

/* A mid-tour answer is a sentence or two in a side panel, to somebody who is
   usually on their first day. Capped so a curious question has a predictable
   cost. */
var TOUR_ANSWER_TOKENS = 700;

/* The intent the brief specifies, matched before the API-key gate. Deliberately
   anchored on whole words: "detour" and "contour" are not requests for a tour. */
var INTENT = /\b(tour|show me around|walk ?me ?through|what can you do|getting started|how does this work)\b/i;

var REDUCED = typeof matchMedia === 'function'
  && matchMedia('(prefers-reduced-motion: reduce)').matches;

var A = null;                 // window.CygenixAssistant, once it exists
var SB = null;                // window.CygenixSidebar
var T = null;                 // window.CygenixTourState — the transition rules

/* tour = { st, transcript, restore }
   `st` is the TourState: tourId, plan, stepIndex, stepId, status, updatedAt.
   It is REPLACED by a transition, never edited in place, so a stale closure
   holding an old one can be out of date but cannot corrupt the current one. */
var tour = null;
var offering = false;         // the restore prompt is up; the tour is not running
var openedGroups = [];        // groups THIS tour expanded, to put back
var hiToken = 0;              // guards against a slow highlight from a past step

/* ── storage ────────────────────────────────────────────────────────────── */

function userTag() {
  try {
    var raw = localStorage.getItem('cygenix_user') || sessionStorage.getItem('cygenix_user');
    var u = raw ? JSON.parse(raw) : null;
    var id = (u && (u.email || u.name)) || localStorage.getItem('cygenix_active_user') || '';
    return String(id).trim().toLowerCase() || 'anon';
  } catch (e) { return 'anon'; }
}
function stateKey() { return STATE_KEY + '::' + userTag() + '::' + (T ? T.TOUR_ID : 'welcome-v1'); }

function readSession() {
  try {
    var raw = sessionStorage.getItem(SESSION_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch (e) { return null; }
}

/* Called after EVERY transition, which is the brief's requirement and also
   the only way a reload mid-tour can land on the right step. The transcript
   goes to the tab, the position goes to the user. */
function persist() {
  if (!tour) return;
  try {
    if (T.isRunnable(tour.st)) {
      sessionStorage.setItem(SESSION_KEY,
        JSON.stringify({ transcript: tour.transcript, restore: tour.restore }));
    } else {
      sessionStorage.removeItem(SESSION_KEY);
    }
  } catch (e) { /* a private window still gets a tour; it just will not resume */ }
  try { localStorage.setItem(stateKey(), JSON.stringify(tour.st)); } catch (e) {}
}

/* One release of the tour stored a bare index in localStorage. Reading it
   means somebody mid-tour when this shipped keeps their place instead of
   being sent back to the intro. */
function getLegacyLast() {
  try {
    var v = parseInt(localStorage.getItem(LAST_KEY), 10);
    return isNaN(v) ? null : v;
  } catch (e) { return null; }
}

function readState() {
  try {
    var raw = localStorage.getItem(stateKey());
    var st = raw ? JSON.parse(raw) : null;
    if (st && st.plan && st.plan.length) return st;
  } catch (e) {}
  return null;
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

/* ── Position is an id, not an index into a live list ──────────────────────

   This is the fix for the reported bug. liveSteps() queries the DOM, so the
   list it returns changes for reasons that have nothing to do with the tour —
   a sidebar group collapsing, the assistant's own targets appearing, a nav
   item that shows up once the user's roles resolve. The old code stored an
   INTEGER into that list and re-resolved it on every paint, so the itinerary
   renumbered underneath the user and step 5 came back as step 4.

   Now liveSteps() is consulted exactly once, at start(), to decide the
   itinerary. After that the plan is frozen and a position is a step id.

   A step in the plan whose target has since disappeared stays in the plan and
   stays counted; highlight() simply draws no spotlight for it. A card
   pointing at nothing costs the reader a shrug. Renumbering costs them their
   place, which is what this whole change is about. */
function stepById(id) {
  for (var i = 0; i < STEPS.length; i++) if (STEPS[i].id === id) return STEPS[i];
  return null;
}
function stepsById() {
  var m = {};
  STEPS.forEach(function (s) { m[s.id] = s; });
  return m;
}
function stepAt(i) {
  if (!tour || !tour.st) return null;
  return stepById(tour.st.plan[T.clampIndex(i, tour.st.plan)]);
}
function total() { return tour && tour.st ? T.total(tour.st) : 0; }
function current() { return tour && tour.st ? tour.st.stepIndex : 0; }

/* `offering` is the state where a saved tour is being OFFERED but has not
   been accepted: the card is on screen, the tour is not running. It has to
   suppress both of these, or the panel goes into tour mode, the spotlight
   comes back and Y starts advancing a tour the user has not agreed to
   restart — which is the relaunch-over-your-work the brief rules out. */
function isActive() { return !offering && !!tour && !!tour.st && tour.st.status === 'active'; }
function isPaused() { return !offering && !!tour && !!tour.st && tour.st.status === 'paused'; }

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
    /* The resume prompt. Deliberately quieter than a step card — it is a
       question about the tour, not a stop on it, and making it look like a
       card would have the reader counting it as one. */
    '.cyg-tour-resume{border:1px dashed var(--accent);border-radius:10px;padding:10px 12px;',
    '  background:var(--accent-glow);margin-top:4px;flex:0 0 auto}',
    '.cyg-tour-resume .tr-head{font-size:12.5px;color:var(--text);margin-bottom:8px}',
    '.cyg-tour-resume .tr-ctl{display:flex;gap:6px;flex-wrap:wrap}',
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
  if (!tour || !isActive()) return;
  var s = stepAt(current());
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

/* Each card carries the number it was drawn with. The old renderer looked the
   step up again on every paint and recomputed the denominator from the live
   DOM, so a card already on screen could change what it said. A transcript
   row is a record of something that was shown; re-deriving it is how it ends
   up disagreeing with itself. */
function pushCard(step, index) {
  tour.transcript.push({ kind: 'step', id: step.id, i: index, tot: total() });
  persist();
}
function pushNote(html) {
  tour.transcript.push({ kind: 'note', html: html });
  persist();
}
function pushSaid(text) {
  tour.transcript.push({ kind: 'user', text: text });
  persist();
}

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
    return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
  });
}

function cardHtml(step, index, isCurrent, frozenTotal) {
  var n = index;
  var tot = frozenTotal === undefined ? total() : frozenTotal;
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
  var last = tour.transcript.length - 1;
  return tour.transcript.map(function (row, idx) {
    if (row.kind === 'user') return '<div class="cyga-msg user">' + esc(row.text) + '</div>';
    if (row.kind === 'note') return '<div class="cyga-msg assistant">' + row.html + '</div>';
    // BY ID. The old line here was `live[row.i]` — an index into a list
    // recomputed from the DOM on every paint — and it is what showed the user
    // step 4 when they were on step 5.
    var step = stepById(row.id);
    if (!step) return '';
    // Only the last card is live, and only while the tour is actually
    // running. While paused for a question the controls come from the resume
    // prompt instead, so the card below it must not also offer Y/B/Exit.
    return cardHtml(step, row.i, isActive() && idx === last, row.tot);
  }).join('');
}

/* ── the engine ─────────────────────────────────────────────────────────── */

/* Draws the CURRENT step. It does not decide which step that is — that is
   what the transitions are for — and in particular it no longer clamps the
   index against a DOM-derived length, which is how a re-render used to be
   able to rewind somebody. */
function show() {
  var s = stepAt(current());
  if (!s) return end('done');

  pushCard(s, current());
  persist();

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
  offering = false;
  // However the tour was reached — a chip, a typed sentence, the first-run
  // modal — a full-screen scrim over the thing being pointed at makes the
  // spotlight meaningless. Close it.
  var onboarding = document.getElementById('onboarding-modal');
  if (onboarding && onboarding.style.display !== 'none') {
    if (typeof window.dismissOnboarding === 'function') window.dismissOnboarding();
    else onboarding.style.display = 'none';
  }
  /* The itinerary is decided HERE and never again. liveSteps() asks the DOM
     which steps have a target worth pointing at; asking it once means the
     numbering is stable for the whole run, which is the difference between a
     tour that keeps its place and one that does not. */
  tour = {
    st: T.create(liveSteps().map(function (x) { return x.id; }), from || 0),
    transcript: (tour && tour.transcript) || [],
    restore: { collapsed: SB && typeof SB.isCollapsed === 'function' ? SB.isCollapsed() : false }
  };
  openedGroups = [];
  if (A) { A.open(); A.setTourMode(true); }
  persist();
  emit('tour_started', { step: current() });
  show();
}

/* ── The transitions ───────────────────────────────────────────────────────
   These, and only these, move the step. Each one replaces tour.st through
   cygenix-tour-state.js and persists immediately, so a reload at any instant
   lands on the position the user last saw. */

function next() {
  if (!tour || !isActive()) return;
  if (current() >= tour.st.plan.length - 1) return end('done');
  tour.st = T.next(tour.st);
  persist();
  emit('tour_step_viewed', { step: current() });
  show();
}
function back() {
  if (!tour || !isActive() || current() <= 0) return;
  tour.st = T.back(tour.st);
  persist();
  show();
}
function jump(i) {
  if (!tour || !T.isRunnable(tour.st)) return start(i);
  tour.st = T.jump(tour.st, i);
  persist();
  show();
}

/* ── Pause and resume ──────────────────────────────────────────────────────
   The interruption path. Pausing changes STATUS and nothing else — the whole
   point of the bug fix — and resuming redraws the step that was paused,
   navigating back to its page first if an agent action moved the user. */

function pauseFor(reason) {
  if (!tour || !isActive()) return;
  tour.st = T.pause(tour.st, reason);
  persist();
  emit('tour_paused', { step: current(), reason: reason });
  // The pill stays up — the tour has not gone anywhere — but the input is the
  // assistant's while this lasts, and says so.
  if (A) { A.setTourMode('paused'); A.refresh(); }
}

function resumeTour() {
  if (!tour || !isPaused()) return;
  tour.st = T.resume(tour.st);
  persist();
  emit('tour_resumed', { step: current() });
  if (A) { A.open(); A.setTourMode(true); }
  // show() redraws the current step and navigates to its page if the
  // assistant wandered off it. It cannot advance — it has no opinion about
  // which step is current, it only draws whichever one is.
  show();
}

function end(reason) {
  if (!tour) return;
  var atTitle = (stepAt(current()) || {}).title || '';
  clearOverlays();
  collapseTourGroups(null);
  // Put the rail back the way it was found.
  if (tour.restore && tour.restore.collapsed && SB && typeof SB.setCollapsed === 'function') {
    SB.setCollapsed(true);
    document.body.classList.add('cyg-collapsed');
  }
  /* Exited and completed are different endings and the brief asks for them to
     stay different: an exited tour must not nag with a resume prompt on the
     next load, a completed one has nothing left to resume. Both are recorded
     rather than deleted, so "have they done the tour" is answerable. */
  tour.st = reason === 'done' ? T.complete(tour.st) : T.exit(tour.st);
  offering = false;
  pushNote(reason === 'done'
    ? 'Tour complete. Type <b>tour</b> any time to go round again.'
    : 'Tour stopped at <b>' + esc(atTitle) + '</b>. Type <b>resume</b> to pick up from there, '
      + 'or <b>tour</b> to start over.');
  emit(reason === 'done' ? 'tour_completed' : 'tour_exited', { step: current() });
  persist();
  try { sessionStorage.removeItem(SESSION_KEY); } catch (e) {}
  if (A) { A.setTourMode(false); A.refresh(); }
}

/* Analytics is optional and must never be a dependency: if nothing is
   listening, this is a no-op rather than a missing-function error. */
function emit(name, detail) {
  try { window.dispatchEvent(new CustomEvent('cygenix:' + name, { detail: detail || {} })); } catch (e) {}
}

/* ── input routing ──────────────────────────────────────────────────────── */

/* "go to connections" — resolved against the RUNNING tour's frozen plan when
   there is one, so a jump lands on the same numbering every other part of the
   tour is using. Only when no tour is running does it fall back to the live
   list, which is the list start() would freeze anyway. */
function findStepIndex(term) {
  term = String(term || '').toLowerCase().trim();
  if (!term) return -1;
  var ids = (tour && tour.st && tour.st.plan && tour.st.plan.length)
    ? tour.st.plan
    : liveSteps().map(function (x) { return x.id; });
  for (var i = 0; i < ids.length; i++) {
    var s = stepById(ids[i]);
    if (!s) continue;
    var hay = (s.title + ' ' + s.section).toLowerCase().replace(/&amp;/g, '&');
    if (hay.indexOf(term) !== -1) return i;
  }
  return -1;
}

/* Returns true when the tour has consumed the input, so the assistant does not
   also send it to the model. Called from submit(), BEFORE the API-key gate —
   which is the whole point: the people who most need this have no key. */
/* The resume prompt the brief specifies, shown after an answer or an action
   finishes. Several questions in a row are expected, so this is drawn again
   each time rather than once. */
function resumePromptHtml() {
  var s = stepAt(current()) || {};
  return '<div class="cyg-tour-resume">'
    + '<div class="tr-head">Paused at step <b>' + current() + ' / ' + total() + '</b>'
    + ' — ' + esc(s.title || '') + '</div>'
    + '<div class="tr-ctl">'
    + '<button class="cyga-btn primary" data-tour-act="resume">Y · Resume tour</button>'
    + '<button class="cyga-btn" data-tour-act="ask">Ask another question</button>'
    + '<button class="cyga-btn" data-tour-act="exit">Exit</button>'
    + '</div></div>';
}

/* Questions cost money; Y / B / Exit never do. Said once a session, because a
   notice on every question is a notice nobody reads.

   ── On "wire this into the credit metering" ──────────────────────────────
   There is none, and that is deliberate rather than missing. Cygenix holds no
   Anthropic key at all — not in an environment variable, not as a fallback —
   and every call is billed directly to the key the operator entered in
   Settings. tests/anthropic-billing.test.js exists to keep it that way, after
   nine call sites were found spending a Cygenix-owned key on users' behalf.

   So the single source of truth the brief asks for is the user's own
   Anthropic account, and there is no balance this code could check without
   inventing a ledger that would then disagree with it. What IS wired is the
   consequence: a quota-exhausted call comes back through cygenix-model.js's
   error mapping ("Rate limit or quota exhausted — check usage limits and
   billing"), onTurnEnd shows that with the resume prompt, and the tour
   carries on. The user is told the true thing by the system that actually
   knows it. */
function creditNoticeOnce() {
  try {
    if (sessionStorage.getItem(CREDIT_KEY) === '1') return;
    sessionStorage.setItem(CREDIT_KEY, '1');
  } catch (e) { /* private window: show it, it is only a sentence */ }
  pushNote('Questions during the tour use AI credits. Moving through the tour '
    + '(<kbd>Y</kbd>, <kbd>B</kbd>, Exit) does not.');
}

function handleInput(text) {
  text = String(text || '').trim();
  var low = text.toLowerCase();

  if (tour && T.isRunnable(tour.st)) {
    var r = T.route(text, tour.st.status);

    if (r.kind === 'noop') return true;
    if (r.kind === 'resume') { pushSaid(text || 'Y'); resumeTour(); return true; }
    if (r.kind === 'next') { next(); return true; }

    pushSaid(text);
    if (r.kind === 'back') { back(); return true; }
    if (r.kind === 'exit') { end('exit'); return true; }
    if (r.kind === 'jump') {
      var k = findStepIndex(r.term);
      if (k >= 0) { jump(k); return true; }
    }

    /* Everything else is a real question or a real request, and the tour gets
       out of the way rather than eating it. Pausing FIRST is what makes the
       position survive whatever the assistant does next — including an agent
       action that navigates to another page and reloads this script. */
    if (A && A.hasKey && A.hasKey()) {
      pauseFor('question');
      creditNoticeOnce();
      if (A) A.refresh();
      return false;                      // let the model answer
    }
    /* No key. The tour is the one thing on this screen a brand-new user can
       actually do, so it must survive not being able to answer. */
    pushNote('I can answer questions once an API key is added in '
      + '<a href="/dashboard#goto=project-settings" style="color:var(--accent)">Settings → General</a>. '
      + 'You can keep going with the tour — press <kbd>Y</kbd> to continue, or type <b>Exit</b> to stop.');
    if (A) A.refresh();
    return true;
  }

  if (/^resume$/.test(low)) {
    /* Typed by the user, so it works on ANY saved position — including one
       they exited. `exited` is about not nagging them with an unsolicited
       prompt; it was never meant to refuse them when they ask. end() itself
       tells them to type this, so refusing here would make the tour a liar. */
    var saved = readState();
    if (saved && saved.plan && saved.plan.length) { restore(saved); return true; }
    var last = getLegacyLast();
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
  if (!tour) return false;
  if (isPaused()) {
    // While paused the single keys are dangerous: the user is mid-conversation
    // and 'y' may be the first letter of a sentence. Only an empty box with Y
    // resumes, and Escape still exits.
    if (e.key === 'Escape') { e.preventDefault(); end('exit'); return true; }
    if (inputValue !== '') return false;
    if (e.key === 'y' || e.key === 'Y' || e.key === 'Enter') {
      e.preventDefault(); resumeTour(); return true;
    }
    return false;
  }
  if (!isActive()) return false;
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
  /* Two different resumes share this act name, and they are not the same
     thing: accepting an OFFER restarts a tour that is not running, while
     resuming from a question un-pauses one that is. Routing the offer's
     button to the second did nothing at all, because the tour was not
     paused — it was not running. */
  else if (act === 'resume') { if (offering) restore(tour.st); else resumeTour(); }
  else if (act === 'restart') start(0);
  else if (act === 'dismiss') dismissOffer();
  else if (act === 'ask') focusInput();
}

function focusInput() {
  var box = document.getElementById('cygaInput');
  if (box) box.focus();
}

/* ── The assistant's turn has finished ────────────────────────────────────
   Called from the assistant's single settle() — the one place a turn ends,
   whether it answered, errored or was broken out of. If the tour is parked,
   this is where it puts its hand back up.

   A failed call must never end or corrupt the tour, so an error gets a
   friendly sentence and the same resume prompt as a success. */
function onTurnEnd(status, error) {
  if (!tour || !isPaused()) return;
  if (status === 'error') {
    pushNote('Ask Cygenix could not answer that'
      + (error ? ' — ' + esc(String(error)) : '')
      + '. You can keep going with the tour.');
  }
  pushNote(resumePromptHtml());
  if (A) A.refresh();
}

/* ── Coming back to a tour that was left running ──────────────────────────
   Shown when a durable position exists but this tab has no transcript: a
   reload, a closed panel, or tomorrow. Deliberately an OFFER — the brief is
   explicit that the tour must not relaunch itself over whatever the user is
   now doing. */
function firstRunModalUp() {
  var m = document.getElementById('onboarding-modal');
  return !!m && m.style.display && m.style.display !== 'none';
}

function offerRestore(saved) {
  var res = T.resolve(saved, stepsById());
  if (!res.ok) { clearState(); return; }
  /* Stand down while the first-run modal is up. That modal already offers the
     tour as its first row, and two competing offers for the same thing — one
     of them behind a full-screen scrim — is worse than one. The saved
     position is untouched, so the offer returns on the next load. */
  if (firstRunModalUp()) return;
  var step = stepById(res.stepId) || {};
  offering = true;
  clearOverlays();          // nothing is running, so nothing should be lit
  tour = { st: saved, transcript: [], restore: { collapsed: false } };
  pushNote('<div class="cyg-tour-resume"><div class="tr-head">'
    + 'You were on step <b>' + res.index + ' / ' + T.total(saved) + '</b> — '
    + esc(step.title || '') + '.'
    + (res.fellBack ? ' That step has changed since; this is the nearest one.' : '')
    + '</div><div class="tr-ctl">'
    + '<button class="cyga-btn primary" data-tour-act="resume">Resume</button>'
    + '<button class="cyga-btn" data-tour-act="restart">Start over</button>'
    + '<button class="cyga-btn" data-tour-act="dismiss">Dismiss</button>'
    + '</div></div>');
  // The offer must not put the panel into tour mode: until they accept, the
  // input belongs to the assistant and typing should reach the model.
  if (A) A.refresh();
}

function dismissOffer() {
  if (!offering) return;
  offering = false;
  // Dismissing is not exiting. The position stays, so `resume` still works
  // and the offer returns next session — the user said "not now", which is a
  // different answer from "never".
  tour = null;
  if (A) { A.setTourMode(false); A.refresh(); }
}

/* Accepting the offer. The saved state may be paused (interrupted last time)
   or active (reload mid-step); either way the user asked for it now, so it
   goes active and draws. */
function restore(saved) {
  var res = T.resolve(saved, stepsById());
  if (!res.ok) { clearState(); return start(0); }
  offering = false;
  injectStyles();
  tour = {
    st: T.jump(Object.assign({}, saved, { status: 'active' }), res.index),
    transcript: (tour && tour.transcript) || [],
    restore: { collapsed: SB && typeof SB.isCollapsed === 'function' ? SB.isCollapsed() : false },
  };
  openedGroups = [];
  if (A) { A.open(); A.setTourMode(true); }
  persist();
  show();
}

function clearState() { try { localStorage.removeItem(stateKey()); } catch (e) {} }

/* ── boot ───────────────────────────────────────────────────────────────── */

function boot() {
  SB = window.CygenixSidebar || null;
  A = window.CygenixAssistant || null;
  T = window.CygenixTourState || null;
  if (!A || typeof A.setTourHooks !== 'function') return;   // assistant too old; stay silent
  if (!T) return;                                            // state module missing; stay silent

  injectStyles();
  A.setTourHooks({
    onInput: handleInput,
    onKey: handleKey,
    onAct: handleAct,
    onTurnEnd: onTurnEnd,
    render: renderPanel,
    isActive: function () { return isActive(); },
    // A mid-tour answer is short by design; see TOUR_ANSWER_TOKENS.
    maxTokens: function () { return isPaused() ? TOUR_ANSWER_TOKENS : 0; },
    onClose: function () {
      // Closing the panel is a pause, not an exit. The user shut a drawer;
      // they did not say they were finished, and the offer on the way back
      // is friendlier than losing their place.
      if (isActive() || isPaused()) {
        if (isActive()) pauseFor('closed');
        persist();
      }
    },
  });

  /* The tour tells the model where the user is, so "tell me more about this"
     works without them naming the topic. It rides the existing context
     provider list, which buildSystemPrompt already serialises whole — no new
     plumbing, and it disappears from the prompt the moment no tour is
     running. */
  if (typeof A.registerContext === 'function') {
    A.registerContext(function () {
      if (!tour || !T.isRunnable(tour.st)) return null;
      var s = stepAt(current()) || {};
      return {
        tour: {
          id: tour.st.tourId,
          step: current(),
          of: total(),
          stepId: tour.st.stepId,
          section: s.section || null,
          title: s.title || null,
          about: String(s.body || '').replace(/<[^>]*>/g, ''),
          page: s.page || null,
        },
        tourGuidance:
          'The user is on step ' + current() + ' of ' + total() + ' of the Cygenix '
          + 'welcome tour, looking at "' + (s.title || '') + '" in the '
          + (s.section || '') + ' section. Answer their question about this area, or '
          + 'carry out the action they ask for, using the normal tools and guardrails. '
          + 'Keep answers concise — two or three sentences unless they ask for more. '
          + 'They will be offered the tour again afterwards, so do not offer to resume '
          + 'it yourself.',
      };
    });
  }

  var saved = readSession();
  var durable = readState();

  if (saved && saved.transcript && durable && T.isRunnable(durable)) {
    /* Same tab, mid-step: the tour navigated here itself. Carry on silently —
       an offer here would interrupt the tour with a prompt about the tour. */
    tour = { st: durable, transcript: saved.transcript, restore: saved.restore || {} };
    openedGroups = [];
    A.open();
    A.setTourMode(isActive());
    A.refresh();
    // The page we wanted is the page we are on now, so draw the step rather
    // than navigating again — otherwise a step whose page matches the current
    // one would loop.
    var s = stepAt(current());
    if (s && isActive()) requestAnimationFrame(function () { collapseTourGroups(null); highlight(s); });
  } else if (durable && T.isRunnable(durable)) {
    /* A position with no transcript in this tab: a reload, a closed panel, or
       another day. Offer, never launch. */
    offerRestore(durable);
  }

  window.addEventListener('resize', reposition);
  var nav = document.querySelector('.cyg-sidebar .cyg-nav');
  if (nav) nav.addEventListener('scroll', reposition, { passive: true });
}

window.CygenixTour = {
  start: start, next: next, back: back, end: end,
  resume: resumeTour,
  handleInput: handleInput,
  isActive: function () { return isActive(); },
  isPaused: function () { return isPaused(); },
  // For tests and for the smoke: the state as the tour currently holds it.
  __state: function () { return tour ? tour.st : null; },
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
