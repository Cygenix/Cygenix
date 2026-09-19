/* ============================================================================
   cygenix-status-hairline.js — which database is this session pointed at
   ----------------------------------------------------------------------------
   WHAT WAS HERE BEFORE, AND WHY IT HAD TO GO

   A fixed 22px bar at the top of all 27 console pages, naming the active
   connection profile. It was the right idea — the only other clue to which
   database a page is talking to is a saved-connection id like
   hgji9oppmecudpiwmhqjuq — and it was built wrong in two ways.

     1. It cost 22 permanent pixels on every screen, for a fact that matters
        intensely twice a day and not at all the rest of the time.
     2. It sat at z-index 2000, over the Ask Cygenix panel (z-index 290),
        which is also fixed at top 0. The panel's New and ✕ buttons start at
        y=12, so the top 10px of both was under the bar: clicking there
        navigated to /profiles instead of pressing the button.

   So the bar is now a 2px hairline that swells to the full 22px on hover and
   settles back when the pointer leaves — except when the session is against
   production, or nothing is selected and writes are therefore blocked, where
   it stays open and the page concedes the space.

   THE STACKING, DECIDED DELIBERATELY

     500  #cygBusyBar          in-flight operations, above everything here
     290  .cyga                the assistant panel — its header is never
                               covered again, and this file does not raise
                               itself above it
      90  .cyg-sidebar
      55  #cyg-envbar          the line itself
      54  #cyg-envbar-hit      the transparent strip that catches the hover

   The busy bar already sat at 500 and needed no change: a 3px indeterminate
   sweep covering a 2px hairline while something is running is the right
   precedence, because "something is running" outranks "which database".

   WHY TWO ELEMENTS

   A 2px hover target is not reliably hittable with a mouse, and is hopeless
   with a trackpad. #cyg-envbar-hit is 10px of transparent nothing whose only
   job is to notice the pointer. The line itself is pointer-events:none while
   collapsed, so events fall through to the strip AND so a stray click near
   the top edge — reaching for the address bar, closing a tab — cannot
   navigate the user away from their work.

   EXPANDING MUST NOT REFLOW

   body padding-top reserves the RESTING height (2px, or 3px for amber), and
   the expansion is overlay only. If expanding pushed the page down, the whole
   document would jump every time the pointer crossed the top edge. The one
   place a reflow is correct is the locked red state: production should own
   that space, so padding returns to 22px and the assistant panel starts
   below the bar rather than under it.

   WHAT THIS CAN AND CANNOT KNOW

   The states asked for were "production or connection unreachable" for red
   and "validation incomplete, sync stale, or jobs failed" for amber. Two of
   those are not determinable from the client without inventing something:

     connection unreachable  nothing polls the source or target database, and
                             adding a ping on every page load across 27 pages
                             is a new cost and a new failure mode
     validation incomplete   there is no passive signal; pfConfidence() needs
                             inputs a page has to supply

   Rather than guess, both are left to report(): any module that learns
   something bad — a failed db call, a preflight that came back short — pushes
   it in, and the line escalates. Nothing calls it yet, and that is honest.
   What IS resolved here comes from facts already in the browser: the profile
   store, CygenixSync.getHealth(), and the job list.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixStatusHairline) {
    root.CygenixStatusHairline = api;
  }
})(typeof window !== 'undefined' ? window : this, function () {
  'use strict';

  /* ── Tunables ───────────────────────────────────────────────────────────
     Named, at the top, because these are the numbers that get tuned by feel
     after a week of using the thing. OPEN_DELAY is what stops the line
     opening when somebody sweeps the pointer up to the browser's address
     bar; CLOSE_DELAY is the grace period that lets them travel from the
     hairline down into the expanded bar without it shutting underneath
     them. */
  var OPEN_DELAY_MS = 140;
  var CLOSE_DELAY_MS = 320;
  var AMBER_PEEK_MS = 4000;      /* how long a fresh amber shows itself */

  var H_GREEN = 2;               /* resting height, px */
  var H_AMBER = 3;               /* amber rests a pixel taller — noticed, not shouted */
  var H_OPEN = 22;               /* the old bar's height, unchanged when open */
  var HIT_H = 10;                /* the transparent hover catcher */

  var Z_BAR = 55;
  var Z_HIT = 54;

  /* ── The status palette is OURS, not the page's ─────────────────────────
     These were var(--green)/var(--amber)/var(--red) — the console's design
     tokens — which was wrong, and visibly so. 26 of the 27 pages carrying
     this bar redefine those three tokens in their own inline :root, and they
     do not agree: three different greens (#22c97a, #22c55e, #3F7D4E), three
     ambers and three reds. So the bar rendered bright mint on the dashboard
     and forest green on Reports, and amber on one page was very nearly the
     green of another.

     For a colour whose whole job is to be recognised at a glance, on any
     screen, that is a defect rather than a theme. The three values below are
     the console's own tokens from cygenix-theme.css, pinned here so no
     page's style block can reach them, and applied through custom properties
     set INLINE on the root element — an inline declaration beats any
     stylesheet :root rule, including one loaded after us. The rail's profile
     chip reads the same three, so the dot and the line can never disagree. */
  // The console's status trio (cygenix-console.css --state-ok/warn/fail).
  // Written here as literals as well because this module sets them INLINE
  // on <html> so that no page's own :root can disagree with the line.
  var PALETTE = { green: '#3f6b52', amber: '#9a6b1f', red: '#9c3f38' };

  /* Read through the literal below, not through this constant: the storage
     inventory scanner (scripts/storage-inventory.js) matches a literal inside
     a getItem/setItem call, so a key reached only through a variable drops
     out of docs/storage-inventory.md entirely. The constant is kept for the
     event listeners, which compare against it. */
  var STORE_KEY = 'cygenix_profiles_v1';
  var SEEN_KEY = 'cygenix_hairline_level';   /* sessionStorage: last level ANNOUNCED */

  var LEVELS = { green: 0, amber: 1, red: 2 };

  /* =======================================================================
     The one state function.

     Pure: everything it needs is an argument, so the rules can be tested
     without a browser and nothing else in the product has to work out what
     the status is. Returns the level, the text, and the geometry that
     follows from them.

       { level, label, detail, text, locked, restHeight, reasons }

     level 'off' means the product has no profiles yet — the hairline does
     not render at all and the console behaves exactly as it did before
     profiles existed. That is the same migration posture cygenix-profiles.js
     takes everywhere else: nothing changes until the first profile is made.
     ======================================================================= */
  function resolveStatus(input) {
    var o = input || {};
    var st = o.store;
    var adopted = !!(st && st.profiles && st.profiles.length);
    if (!adopted) {
      return { level: 'off', label: '', detail: '', text: '', locked: false, restHeight: 0, reasons: [] };
    }

    var id = st.settings && st.settings.activeProfileId;
    var profile = null;
    for (var i = 0; i < st.profiles.length; i++) {
      if (st.profiles[i] && st.profiles[i].id === id) profile = st.profiles[i];
    }

    var level = 'green';
    var reasons = [];
    var raise = function (lv, why) {
      if (why) reasons.push(why);
      if (LEVELS[lv] > LEVELS[level]) level = lv;
    };

    var label;
    if (!profile) {
      /* Profiles are in force and none is selected. Every write in the
         product is blocked in this state — cpGuardWrite refuses outright —
         so it is a failure, not a warning, and it gets the locked bar. */
      label = 'NO PROFILE SELECTED';
      raise('red', 'writes are blocked until you choose one');
    } else {
      var env = String(profile.envClass || 'UNKNOWN').toUpperCase();
      label = profile.id + ' · ' + env
        + (profile.name && profile.name !== profile.id ? ' · ' + profile.name : '');
      if (env === 'PRD') {
        /* No extra words: the label already says PRD, and a red 22px bar
           that cannot be dismissed is the message. */
        raise('red', '');
      } else if (env === 'UNKNOWN') {
        raise('amber', 'environment unclassified — writes blocked');
      }
    }

    /* Sync. getHealth() is the existing source of truth and already
       distinguishes "the call failed" from "this account is empty". */
    var h = o.health;
    if (h) {
      if (h.degraded) raise('amber', 'cloud sync ' + (h.reason ? '(' + h.reason + ')' : 'is degraded'));
      else if (h.pendingSaves > 0) {
        raise('amber', h.pendingSaves + ' unsaved change' + (h.pendingSaves === 1 ? '' : 's'));
      }
    }

    /* Jobs. Same bucketing the analytics module uses, inlined rather than
       depended on: this file must work on pages that do not load it. */
    var jobs = o.jobs;
    if (jobs && jobs.length) {
      var failed = 0;
      for (var j = 0; j < jobs.length; j++) {
        var s = String((jobs[j] && (jobs[j].executionStatus || jobs[j].status)) || '').toLowerCase();
        if (s === 'failed' || s === 'fail' || s === 'error') failed++;
      }
      if (failed) raise('amber', failed + ' job' + (failed === 1 ? '' : 's') + ' failed');
    }

    /* Anything pushed in by report() — a db call that came back unreachable,
       a preflight that fell short. See the header: this is the seam those
       two conditions live behind rather than being guessed at here. */
    var ov = o.overrides || {};
    Object.keys(ov).forEach(function (k) {
      var r = ov[k];
      if (r && LEVELS[r.level] != null) raise(r.level, r.label || k);
    });

    var detail = reasons.filter(Boolean)[0] || '';
    return {
      level: level,
      label: label,
      detail: detail,
      text: label + (detail ? ' — ' + detail : ''),
      /* Red locks open. Amber and green rest as a line and swell on hover. */
      locked: level === 'red',
      restHeight: level === 'red' ? H_OPEN : (level === 'amber' ? H_AMBER : H_GREEN),
      reasons: reasons.filter(Boolean),
    };
  }

  /* =======================================================================
     Everything below this line needs a DOM. Kept apart so the rules above
     can be required from a Node test.
     ======================================================================= */
  var hasDom = typeof document !== 'undefined';

  var el = {};                 /* bar, hit, txt, live */
  var _open = false;
  var _openTimer = null, _closeTimer = null, _peekTimer = null;
  var _state = null;
  var _overrides = {};
  var _tapMode = false;        /* set from matchMedia('(hover: none)') */

  function css() {
    return [
      /* The reserved height is a variable so the body, the sidebar and the
         assistant all follow it from one write. */
      ':root{--cyg-hairline-h:0px}',

      '#cyg-envbar{position:fixed;top:0;left:0;right:0;height:var(--cyg-hairline-rest,2px);',
      '  z-index:' + Z_BAR + ';overflow:hidden;display:flex;align-items:center;justify-content:center;',
      '  text-decoration:none;color:#fff;pointer-events:none;',
      '  transition:height .18s cubic-bezier(.4,0,.2,1)}',
      '#cyg-envbar .cyg-envbar-txt{opacity:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;',
      '  padding:0 8px 0 12px;min-width:0;transition:opacity .12s ease;',
      '  font:600 10.5px/1 var(--mono,"IBM Plex Mono",ui-monospace,monospace);letter-spacing:0.06em}',

      /* Open — by hover, by focus, by tap, or permanently when locked. The
         line is only clickable once it is open: while it is 2px tall a click
         near the top edge must do nothing at all. */
      '#cyg-envbar.is-open,#cyg-envbar.is-locked{height:' + H_OPEN + 'px;pointer-events:auto;cursor:pointer}',
      '#cyg-envbar.is-open .cyg-envbar-txt,#cyg-envbar.is-locked .cyg-envbar-txt{opacity:1}',
      '#cyg-envbar:focus-visible{outline:2px solid #fff;outline-offset:-3px}',

      /* --cyg-status-* are set inline on the root element by render(), so a
         page's own :root cannot reach them. See PALETTE above. */
      '#cyg-envbar.lv-green{background:var(--cyg-status-green,' + PALETTE.green + ')}',
      '#cyg-envbar.lv-amber{background:var(--cyg-status-amber,' + PALETTE.amber + ')}',
      '#cyg-envbar.lv-red{background:var(--cyg-status-red,' + PALETTE.red + ')}',

      /* Dismiss. There is no button while collapsed — there is nothing to
         dismiss — and none while locked, because production owns that space
         and must not be closable. It earns its place on touch, where moving
         the pointer away is not a gesture that exists. */
      /* Beside the text, in flow, NOT pinned to the right edge. Pinned right
         it lands under the Ask Cygenix panel whenever that is open — 420px
         of z-index 290 over a bar at 55 — and pinned left it lands under the
         sidebar. The bar's content is centred, so the middle is the one
         strip of it that nothing else covers. */
      '#cyg-envbar .cyg-envbar-x{display:none;flex:0 0 auto;align-items:center;',
      '  height:100%;padding:0 4px 0 2px;margin-left:-4px;border:0;background:none;cursor:pointer;',
      '  color:rgba(255,255,255,.72);font:600 12px/1 var(--mono,monospace);}',
      '#cyg-envbar .cyg-envbar-x:hover{color:#fff}',
      '#cyg-envbar.is-open .cyg-envbar-x{display:flex}',
      '#cyg-envbar.is-locked .cyg-envbar-x{display:none}',
      '#cyg-envbar .cyg-envbar-x:focus-visible{outline:2px solid #fff;outline-offset:-3px}',

      '#cyg-envbar-hit{position:fixed;top:0;left:0;right:0;height:' + HIT_H + 'px;',
      '  z-index:' + Z_HIT + ';pointer-events:auto;background:transparent}',
      /* Locked: there is nothing to reveal, so the catcher is inert rather
         than swallowing clicks meant for the page. */
      '#cyg-envbar-hit.is-off{pointer-events:none;display:none}',

      /* The reserved strip. Overlay expansion never touches this, so nothing
         on the page moves when the pointer crosses the top edge. */
      /* The masthead (cygenix-sidebar.js, 60px) sits under the hairline and
         the rail sits under the masthead. cygenix-console.css pads the body
         by masthead + hairline; this rule exists for a page that loads the
         hairline without that stylesheet and must say the same sum. */
      'body.cyg-envbar-pad{padding-top:calc(var(--cyg-hairline-h) + var(--cx-masthead-h,0px))}',
      'body.cyg-envbar-pad .cyg-sidebar{top:calc(var(--cyg-hairline-h) + var(--cx-masthead-h,0px));',
      '  height:auto}',
      /* Only when locked does the assistant move: two fixed panels at top 0
         is exactly the collision this file exists to end, and at 2px it does
         not matter. The selector is body-qualified so it outranks .cyga's own
         top:0 without editing cygenix-assistant.js. */
      'body.cyg-envbar-locked .cyga{top:var(--cyg-hairline-h)}',

      '.cyg-envbar-live{position:absolute;width:1px;height:1px;margin:-1px;padding:0;',
      '  overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap;border:0}',

      '@media (prefers-reduced-motion: reduce){',
      '  #cyg-envbar,#cyg-envbar .cyg-envbar-txt{transition:none}}',
    ].join('');
  }

  function ensureDom() {
    if (el.bar) return;
    var style = document.createElement('style');
    style.id = 'cyg-envbar-css';
    style.textContent = css();
    document.head.appendChild(style);

    var hit = document.createElement('div');
    hit.id = 'cyg-envbar-hit';
    hit.setAttribute('aria-hidden', 'true');

    var bar = document.createElement('a');
    bar.id = 'cyg-envbar';
    bar.href = '/profiles';
    bar.title = 'The connection profile governing this session. Click to manage profiles.';
    var txt = document.createElement('span');
    txt.className = 'cyg-envbar-txt';
    bar.appendChild(txt);

    /* Inside the anchor, so it sits at the bar's right edge — but it is a
       real <button>, and its click never reaches the link. */
    var x = document.createElement('button');
    x.type = 'button';
    x.className = 'cyg-envbar-x';
    x.setAttribute('aria-label', 'Collapse the status bar');
    x.title = 'Collapse';
    x.textContent = '✕';
    bar.appendChild(x);

    var live = document.createElement('div');
    live.className = 'cyg-envbar-live';
    live.setAttribute('aria-live', 'polite');

    document.body.appendChild(hit);
    document.body.appendChild(bar);
    document.body.appendChild(live);
    el = { bar: bar, hit: hit, txt: txt, live: live, x: x };

    wire();
  }

  function teardown() {
    if (!el.bar) return;
    [el.bar, el.hit, el.live].forEach(function (n) { if (n && n.parentNode) n.parentNode.removeChild(n); });
    var s = document.getElementById('cyg-envbar-css');
    if (s && s.parentNode) s.parentNode.removeChild(s);
    document.body.classList.remove('cyg-envbar-pad', 'cyg-envbar-locked');
    document.documentElement.style.removeProperty('--cyg-hairline-h');
    document.documentElement.style.removeProperty('--cyg-hairline-rest');
    clearTimers();
    el = {}; _open = false; _state = null;
  }

  function clearTimers() {
    if (_openTimer) { clearTimeout(_openTimer); _openTimer = null; }
    if (_closeTimer) { clearTimeout(_closeTimer); _closeTimer = null; }
  }

  function setOpen(on) {
    if (!el.bar || (_state && _state.locked)) return;
    _open = !!on;
    el.bar.classList.toggle('is-open', _open);
  }

  /* Hover in and hover out are two halves of one decision, so they share the
     timers rather than each owning one.

     The handover matters: moving from the 10px catcher into the now-open
     22px bar fires mouseleave on the catcher and mouseenter on the bar in
     the same turn. The leave schedules a close; the enter cancels it before
     it can run. Without that pairing the bar would shut in the user's face
     the moment they moved into it. */
  function onEnter() {
    if (!el.bar || (_state && _state.locked)) return;
    if (_closeTimer) { clearTimeout(_closeTimer); _closeTimer = null; }
    if (_open || _openTimer) return;
    _openTimer = setTimeout(function () { _openTimer = null; setOpen(true); }, OPEN_DELAY_MS);
  }
  function onLeave() {
    if (!el.bar || (_state && _state.locked)) return;
    if (_openTimer) { clearTimeout(_openTimer); _openTimer = null; }
    if (!_open || _closeTimer) return;
    _closeTimer = setTimeout(function () { _closeTimer = null; setOpen(false); }, CLOSE_DELAY_MS);
  }

  function wire() {
    /* Touch has no hover, so on those devices the line would never open.
       Tap the strip to toggle, tap anywhere else to dismiss. Read once and
       kept live, because a 2-in-1 can gain and lose a pointer. */
    var mq = (typeof matchMedia === 'function') ? matchMedia('(hover: none)') : null;
    var syncMode = function () {
      _tapMode = !!(mq && mq.matches);
      clearTimers();
      if (_tapMode) setOpen(false);
    };
    syncMode();
    if (mq && mq.addEventListener) mq.addEventListener('change', syncMode);
    else if (mq && mq.addListener) mq.addListener(syncMode);

    [el.hit, el.bar].forEach(function (n) {
      n.addEventListener('mouseenter', function () { if (!_tapMode) onEnter(); });
      n.addEventListener('mouseleave', function () { if (!_tapMode) onLeave(); });
    });

    el.hit.addEventListener('click', function (e) {
      if (!_tapMode) return;
      e.preventDefault(); e.stopPropagation();
      setOpen(!_open);
    });
    document.addEventListener('click', function (e) {
      if (!_tapMode || !_open) return;
      if (el.bar.contains(e.target) || el.hit.contains(e.target)) return;
      setOpen(false);
    }, true);

    /* Keyboard. The anchor stays in the tab order at 2px — pointer-events
       does not affect focusability — so focusing it must show what it says,
       or tabbing lands on an invisible link to nowhere. */
    /* Dismiss. preventDefault as well as stopPropagation: the button is
       inside the anchor, so without both a click on it would also navigate
       to /profiles — which is the opposite of "put this away". The pointer
       is still over the bar afterwards, so the hover timers are cleared too;
       otherwise nothing would re-open it until the pointer left and came
       back, which reads as the control having jammed. */
    el.x.addEventListener('click', function (e) {
      e.preventDefault(); e.stopPropagation();
      clearTimers();
      setOpen(false);
      el.x.blur();
    });

    /* focusin/focusout rather than focus/blur: the dismiss button lives
       INSIDE the anchor, so tabbing from one to the other fires blur on the
       anchor — which under a plain blur handler collapsed the bar and took
       the button out of the document mid-tab. focusout carries where focus
       went, so leaving for somewhere still inside the bar is not leaving. */
    el.bar.addEventListener('focusin', function () { clearTimers(); setOpen(true); });
    el.bar.addEventListener('focusout', function (e) {
      if (e.relatedTarget && el.bar.contains(e.relatedTarget)) return;
      setOpen(false);
    });
    el.bar.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') { setOpen(false); el.bar.blur(); }
    });
  }

  /* What the browser can tell us right now. Each source is optional: this
     file loads on pages that do not have all of them. */
  function gather() {
    var store = null, health = null, jobs = null;
    try { store = JSON.parse(localStorage.getItem('cygenix_profiles_v1') || 'null'); } catch (e) { store = null; }
    try {
      var S = (typeof window !== 'undefined') && window.CygenixSync;
      if (S && typeof S.getHealth === 'function') health = S.getHealth();
    } catch (e) { health = null; }
    try { jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || 'null'); } catch (e) { jobs = null; }
    return { store: store, health: health, jobs: jobs, overrides: _overrides };
  }

  /* A fresh amber shows itself once and then gets out of the way. Held in
     sessionStorage rather than a module variable because every navigation
     reloads this script: without it, every page in a degraded session would
     flash its bar for four seconds, which is how a warning becomes noise. */
  function lastAnnounced() {
    try { return sessionStorage.getItem('cygenix_hairline_level') || ''; } catch (e) { return ''; }
  }
  function rememberAnnounced(level) {
    try { sessionStorage.setItem('cygenix_hairline_level', level); } catch (e) { /* private window */ }
  }

  function render() {
    if (!hasDom || !document.body) return null;
    var s = resolveStatus(gather());

    if (s.level === 'off') { teardown(); return s; }
    ensureDom();

    var was = _state;
    _state = s;

    el.bar.className = 'lv-' + s.level + (s.locked ? ' is-locked' : '');
    el.txt.textContent = '● ' + s.text;
    el.bar.setAttribute('aria-label', 'Connection profile: ' + s.text + '. Open the Profiles page.');
    el.hit.classList.toggle('is-off', !!s.locked);

    var root = document.documentElement;
    root.style.setProperty('--cyg-hairline-rest', s.restHeight + 'px');
    root.style.setProperty('--cyg-hairline-h', s.restHeight + 'px');
    /* Set inline, every render, so a page whose own :root declares --green
       cannot change what a status level looks like. See PALETTE. */
    root.style.setProperty('--cyg-status-green', PALETTE.green);
    root.style.setProperty('--cyg-status-amber', PALETTE.amber);
    root.style.setProperty('--cyg-status-red', PALETTE.red);
    document.body.classList.add('cyg-envbar-pad');
    document.body.classList.toggle('cyg-envbar-locked', !!s.locked);

    if (s.locked) { clearTimers(); _open = false; }

    /* Escalation is announced once per transition, not once per page. */
    var prev = (was && was.level) || lastAnnounced();
    if (s.level !== prev) {
      if (s.level === 'red') {
        el.live.textContent = 'Warning: ' + s.text;
        rememberAnnounced(s.level);
      } else if (s.level === 'amber' && LEVELS[s.level] > (LEVELS[prev] || 0)) {
        rememberAnnounced(s.level);
        peek();
      } else {
        rememberAnnounced(s.level);
      }
    }

    /* The rail's profile chip renders from this, so there is one state
       function and not two. Published as an event rather than a call into
       the sidebar, because this file must not know whether a sidebar
       exists — half the console's pages mount one and the rest do not. */
    try {
      window.dispatchEvent(new CustomEvent('cygenix:profile-status', { detail: s }));
    } catch (e) { /* a listener throwing must not stop the bar rendering */ }
    return s;
  }

  /* Open on our own for a moment, then settle. The timer handle is cleared
     BEFORE setOpen so the callback cannot leave a stale one behind — a
     one-shot that re-arms itself from inside its own callback is how this
     codebase got an infinite rerender once already. */
  function peek() {
    if (_peekTimer) { clearTimeout(_peekTimer); _peekTimer = null; }
    setOpen(true);
    _peekTimer = setTimeout(function () {
      _peekTimer = null;
      setOpen(false);
    }, AMBER_PEEK_MS);
  }

  /* The seam for the two conditions this file cannot determine on its own.
     Anything that learns the target is unreachable, or that validation has
     not been run, calls report('db', 'red', 'target unreachable') and the
     line escalates; report('db', null) clears it. Deliberately keyed, so two
     sources cannot overwrite each other's reason. */
  function report(key, level, label) {
    if (!key) return;
    if (!level) delete _overrides[key];
    else _overrides[key] = { level: level, label: label || '' };
    render();
  }

  function boot() {
    if (!hasDom) return;
    render();
    window.addEventListener('storage', function (e) {
      if (e.key === STORE_KEY || e.key === 'cygenix_jobs') render();
    });
    window.addEventListener('cygenix:profiles-changed', render);
    window.addEventListener('cygenix-sync-health', render);
  }

  if (hasDom) {
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
    else boot();
  }

  return {
    TIMING: { OPEN_DELAY_MS: OPEN_DELAY_MS, CLOSE_DELAY_MS: CLOSE_DELAY_MS, AMBER_PEEK_MS: AMBER_PEEK_MS },
    HEIGHTS: { H_GREEN: H_GREEN, H_AMBER: H_AMBER, H_OPEN: H_OPEN, HIT_H: HIT_H },
    Z: { BAR: Z_BAR, HIT: Z_HIT },
    resolveStatus: resolveStatus,
    render: render,
    report: report,
    /* The last resolved state, for anything that mounts after this file has
       already rendered — the sidebar, which cannot rely on script order. */
    current: function () { return _state; },
    /* for the tests and the console */
    __dom: function () { return el; },
    __isOpen: function () { return _open; },
    __state: function () { return _state; },
  };
});
