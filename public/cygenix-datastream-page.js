/* ============================================================================
   cygenix-datastream-page.js — the boot every Data Stream screen shares.
   ----------------------------------------------------------------------------
   Load after cygenix-datastream.js and cygenix-datastream-ui.js.

   All five screens need the same four things, and each one is a place they
   could quietly disagree:

     the project      which project's streams am I looking at
     the state        loaded once, seeded on first visit, saved on change
     the tick         one 2s timer, one call to the engine, one repaint
     the URL          query parameters in, history.replaceState out

   Running the tick per screen would mean the Monitor advancing the world
   twice as fast as the Streams list when both are open in two tabs, and the
   figures would stop agreeing across a tab switch. So the tick is here, it
   runs once, and it writes through to storage — a second tab picks the state
   up from there rather than simulating its own.

   A screen registers a paint function and gets called after every tick. It
   does not own the clock.
   ========================================================================== */
(function (root) {
  'use strict';
  var DS = root.CygenixDataStream;
  if (!DS) return;                       // engine missing: the page shows its empty state

  var listeners = [];
  var timer = null;
  var state = null;
  var projectId = null;
  var paused = false;                    // the live tail can stop the world's repaint, not its clock

  function resolveProject() {
    try { return localStorage.getItem('cygenix_active_project_id') || 'default'; }
    catch (e) { return 'default'; }
  }

  /* First visit to the module seeds the demo world. Afterwards the stored
     state wins — otherwise a paused stream would un-pause itself on every
     reload, which is the opposite of the guarantee this module makes. */
  function boot() {
    projectId = resolveProject();
    state = DS.load(projectId);
    if (!state.seeded && !state.streams.length) {
      state = DS.seedDemo(projectId);
      DS.save(state);
    }
    // First observation of the active profile, so a later change is a change
    // and not the first reading. Then park anything already broken.
    var p0 = activeProfile();
    lastProfileId = p0 ? p0.id : null;
    if (checkAttention()) DS.save(state);
    return state;
  }

  /* Returns whether the write actually landed. The global pause reads this:
     a stream reported as paused that could not be persisted would come back
     running on the next reload, having told the operator otherwise. */
  function persist() { return state ? DS.save(state) : false; }

  function start() {
    if (timer) return;
    // A seeded demo carries a fixed clock so its figures are reproducible;
    // the tick below uses the real one. Rebasing once, here, is where those
    // two clocks meet — without it every "how long ago" on the screen is out
    // by the gap between the seed date and today.
    if (state) { DS.rebaseToNow(state); persist(); }
    timer = setInterval(function () {
      if (!state) return;
      // Before the tick, not after: a stream whose profile has gone must not
      // get one more tick of pretend capture before it is parked.
      checkAttention();
      DS.tick(state);
      persist();
      emit();
    }, DS.TICK_MS);
  }
  function stop() { if (timer) { clearInterval(timer); timer = null; } }

  function emit() {
    listeners.forEach(function (fn) {
      try { fn(state); } catch (e) { if (root.console) console.error('[data stream]', e); }
    });
  }

  /* A screen's paint function. Called once immediately so the first frame is
     never blank, then after every tick. */
  function onTick(fn) {
    listeners.push(fn);
    if (state) { try { fn(state); } catch (e) { /* the screen reports its own errors */ } }
    return function () { listeners = listeners.filter(function (f) { return f !== fn; }); };
  }

  /* ── URL state ──────────────────────────────────────────────────────────
     Query parameters, never a hash route, and only identifiers. setUrl uses
     replaceState so the back button still means "the previous screen" rather
     than "the previous filter" — a filter change is not a navigation. */
  function query() { return DS.parseQuery(root.location ? root.location.search : ''); }
  function setUrl(patch, opts) {
    if (!root.history || !root.location) return;
    var next = Object.assign({}, query(), patch || {});
    Object.keys(next).forEach(function (k) {
      if (next[k] === null || next[k] === undefined || next[k] === '') delete next[k];
    });
    var url = root.location.pathname + DS.buildQuery(next);
    if ((opts && opts.push) ? false : true) root.history.replaceState(null, '', url);
    else root.history.pushState(null, '', url);
  }

  /* ── Announcements ──────────────────────────────────────────────────────
     A screen that changes under a screen-reader user without saying so is a
     screen they cannot use. Polite, so it never interrupts. */
  function announce(msg) {
    var el = document.getElementById('ds-live');
    if (!el) return;
    el.textContent = '';
    setTimeout(function () { el.textContent = msg; }, 30);
  }

  /* ── Copy buttons ───────────────────────────────────────────────────────
     One delegated listener rather than a handler per rendered row: the tables
     re-render on the tick, and per-row listeners would leak with them. */
  function wireCopy() {
    document.addEventListener('click', function (e) {
      var btn = e.target && e.target.closest && e.target.closest('.ds-copy');
      if (!btn) return;
      e.stopPropagation();
      var text = btn.getAttribute('data-copy') || '';
      if (root.navigator && navigator.clipboard) navigator.clipboard.writeText(text).catch(function () {});
      btn.classList.add('ok');
      announce('Copied.');
      setTimeout(function () { btn.classList.remove('ok'); }, 900);
    });
  }

  /* ── Row menus ──────────────────────────────────────────────────────────
     Opened next to the button that asked for them, closed by the next click
     or Escape. One menu element for the page. */
  var menuEl = null;
  function openMenu(anchor, items) {
    closeMenu();
    menuEl = document.createElement('div');
    menuEl.className = 'ds-menu';
    menuEl.setAttribute('role', 'menu');
    items.forEach(function (it) {
      if (it === '-') {
        var sep = document.createElement('div');
        sep.className = 'ds-menu-sep';
        menuEl.appendChild(sep);
        return;
      }
      var b = document.createElement('button');
      b.type = 'button';
      b.setAttribute('role', 'menuitem');
      b.innerHTML = (it.icon ? '<i class="ic ic-' + it.icon + '"></i>' : '') + '<span></span>';
      b.querySelector('span').textContent = it.label;   // label is text, never markup
      if (it.danger) b.className = 'danger';
      if (it.disabled) b.disabled = true;
      // The reason a disabled item is disabled, where the pointer already is.
      if (it.title) b.title = it.title;
      b.addEventListener('click', function (ev) {
        ev.stopPropagation();
        closeMenu();
        it.run();
      });
      menuEl.appendChild(b);
    });
    document.body.appendChild(menuEl);
    var r = anchor.getBoundingClientRect();
    var top = r.bottom + window.scrollY + 4;
    var left = Math.max(8, r.right + window.scrollX - menuEl.offsetWidth);
    // Flip above the button when there is not room below it.
    if (r.bottom + menuEl.offsetHeight + 12 > window.innerHeight) {
      top = r.top + window.scrollY - menuEl.offsetHeight - 4;
    }
    menuEl.style.top = top + 'px';
    menuEl.style.left = left + 'px';
  }
  function closeMenu() {
    if (menuEl && menuEl.parentNode) menuEl.parentNode.removeChild(menuEl);
    menuEl = null;
  }
  document.addEventListener('click', function () { closeMenu(); });
  document.addEventListener('keydown', function (e) { if (e.key === 'Escape') closeMenu(); });

  /* ── Command palette ────────────────────────────────────────────────────
     The console's standard Ctrl/Cmd-K. Built here rather than per screen so
     the same keystroke finds the same things everywhere in the module: every
     stream, every streamed table, and every screen. Selecting a table goes to
     the stream that carries it, because that is the question being asked. */
  function paletteItems() {
    var out = [];
    (state && state.streams || []).forEach(function (s) {
      out.push({ kind: 'stream', label: s.name, sub: s.capture.connectionLabel,
        href: '/data-stream' + DS.buildQuery({ stream: s.id }) });
      (s.objects || []).forEach(function (o) {
        out.push({ kind: 'table', label: o.table, sub: s.name,
          href: '/data-stream-events' + DS.buildQuery({ stream: s.id, table: o.table }) });
      });
    });
    out.push({ kind: 'screen', label: 'Streams', sub: 'the module’s landing screen', href: '/data-stream' });
    out.push({ kind: 'screen', label: 'Stream Store', sub: 'what the buffer is holding', href: '/data-stream-store' });
    out.push({ kind: 'screen', label: 'Change Events', sub: 'the live tail', href: '/data-stream-events' });
    out.push({ kind: 'screen', label: 'Stream Monitor', sub: 'charts and alerts', href: '/data-stream-monitor' });
    return out;
  }
  var palSel = 0, palRows = [];
  function paletteOpen() {
    var box = document.getElementById('ds-palette');
    if (!box) return;
    box.classList.add('show');
    var input = document.getElementById('ds-palette-input');
    input.value = '';
    paletteFilter();
    input.focus();
  }
  function paletteClose() {
    var box = document.getElementById('ds-palette');
    if (box) box.classList.remove('show');
  }
  function paletteFilter() {
    var q = (document.getElementById('ds-palette-input') || {}).value || '';
    var t = q.trim().toLowerCase();
    palRows = paletteItems().filter(function (i) {
      return !t || i.label.toLowerCase().indexOf(t) !== -1 || String(i.sub).toLowerCase().indexOf(t) !== -1;
    }).slice(0, 40);
    palSel = 0;
    paletteDraw();
  }
  function paletteDraw() {
    var list = document.getElementById('ds-palette-list');
    if (!list) return;
    if (!palRows.length) { list.innerHTML = '<div class="ds-palette-item">Nothing matches.</div>'; return; }
    list.innerHTML = palRows.map(function (r, i) {
      return '<div class="ds-palette-item' + (i === palSel ? ' on' : '') + '" role="option"'
        + ' aria-selected="' + (i === palSel) + '" data-i="' + i + '">'
        + '<span class="ds-palette-kind">' + r.kind + '</span>'
        + '<span>' + String(r.label).replace(/[<>&]/g, '') + '</span>'
        + '<span class="ds-palette-sub">' + String(r.sub || '').replace(/[<>&]/g, '') + '</span></div>';
    }).join('');
    Array.prototype.forEach.call(list.children, function (el) {
      el.addEventListener('click', function () { paletteGo(Number(el.getAttribute('data-i'))); });
    });
  }
  function paletteGo(i) {
    var r = palRows[i];
    if (r) root.location.href = r.href;
  }
  function paletteKey(e) {
    if (e.key === 'Escape') { paletteClose(); return; }
    if (e.key === 'ArrowDown') { palSel = Math.min(palRows.length - 1, palSel + 1); paletteDraw(); e.preventDefault(); }
    if (e.key === 'ArrowUp')   { palSel = Math.max(0, palSel - 1); paletteDraw(); e.preventDefault(); }
    if (e.key === 'Enter')     { paletteGo(palSel); e.preventDefault(); }
  }
  document.addEventListener('keydown', function (e) {
    if ((e.metaKey || e.ctrlKey) && (e.key === 'k' || e.key === 'K')) {
      if (document.getElementById('ds-palette')) { e.preventDefault(); paletteOpen(); }
    }
  });

  /* ── Profile ownership ──────────────────────────────────────────────────
     Every screen scopes what it shows to the ACTIVE profile by default — the
     one the top bar names — with a "This profile / All profiles" toggle that
     is remembered per user and reset to "This profile" whenever the active
     profile changes. The rules live in the engine (DS.scopeStreams,
     DS.attentionCheck); this half supplies the two things the engine cannot
     read for itself — the profile store and the saved connections — and
     listens for the profile changing.

     Listening, not polling, and no flag is reset by its own callback: the
     hairline broadcasts cygenix:profile-status on every change it renders,
     the Profiles page dispatches cygenix:profiles-changed, and another tab's
     change arrives as a storage event. All three land in onProfileEvent(),
     which compares the active profile's id with the last one it saw. That
     comparison IS the state; there is no one-shot to arm. */
  var SCOPE_KEY = 'cygenix_datastream_scope';
  var scopeMode = null;          // 'this' | 'all', lazily read
  var lastProfileId = undefined; // undefined = not yet observed

  function profileStore() {
    try { return (root.CygenixJobProfile && root.CygenixJobProfile.load()) || null; }
    catch (e) { return null; }
  }
  function activeProfile() {
    try { return root.CygenixJobProfile ? root.CygenixJobProfile.activeProfile(profileStore()) : null; }
    catch (e) { return null; }
  }
  function savedConns() {
    try {
      if (root.CygenixConnections && root.CygenixConnections.savedGetAll) {
        return root.CygenixConnections.savedGetAll() || [];
      }
    } catch (e) { /* module absent on this page */ }
    return [];
  }
  /* Per user: two people sharing a browser profile is rare, but the choice is
     a preference and preferences are personal. */
  function scopeKey() {
    var who = '';
    try { var u = JSON.parse(localStorage.getItem('cygenix_user') || 'null'); who = (u && u.email) || ''; }
    catch (e) { /* unknown user */ }
    return SCOPE_KEY + (who ? '::' + who : '');
  }
  function scope() {
    if (scopeMode) return scopeMode;
    try { scopeMode = localStorage.getItem(scopeKey()) === 'all' ? 'all' : 'this'; }
    catch (e) { scopeMode = 'this'; }
    return scopeMode;
  }
  function setScope(mode) {
    scopeMode = mode === 'all' ? 'all' : 'this';
    try { localStorage.setItem(scopeKey(), scopeMode); } catch (e) { /* preference only */ }
    return scopeMode;
  }
  /* The streams a screen shows. */
  function visible(st) {
    var s = st || state;
    if (!s) return [];
    var p = activeProfile();
    return DS.scopeStreams(s.streams, { mode: scope(), profileId: p ? p.id : null });
  }
  /* The ids of those streams, for screens that filter events/points. */
  function visibleIds(st) {
    var ids = {};
    visible(st).forEach(function (x) { ids[x.id] = true; });
    return ids;
  }
  function checkAttention() {
    if (!state) return false;
    var changed = DS.attentionCheck(state, profileStore(), savedConns());
    return changed.length > 0;
  }
  function onProfileEvent() {
    var p = activeProfile();
    var id = p ? p.id : null;
    var switched = lastProfileId !== undefined && id !== lastProfileId;
    lastProfileId = id;
    if (switched) setScope('this');
    var parked = checkAttention();
    if (parked) persist();
    if (switched || parked) emit();
  }
  root.addEventListener('cygenix:profile-status', onProfileEvent);
  root.addEventListener('cygenix:profiles-changed', onProfileEvent);

  /* ── Cross-tab ──────────────────────────────────────────────────────────
     Another tab paused a stream; this one should show that, not its own stale
     copy. Reload the state rather than trying to merge — the writer's copy is
     always the newer one. */
  root.addEventListener('storage', function (e) {
    if (!state || !e.key) return;
    if (e.key === DS.storeKey(projectId)) {
      state = DS.load(projectId);
      emit();
    } else if (e.key === 'cygenix_active_project_id') {
      boot();
      emit();
    } else if (e.key === 'cygenix_profiles_v1' || e.key === 'cygenix_saved_connections') {
      onProfileEvent();
    }
  });

  /* Pausing the world when the tab is hidden: a background tab simulating
     events nobody is watching burns battery and inflates the figures. The
     tick resumes on return. */
  document.addEventListener('visibilitychange', function () {
    if (document.hidden) stop(); else if (!paused) start();
  });

  root.CygenixDataStreamPage = {
    boot: boot,
    start: start,
    stop: stop,
    persist: persist,
    onTick: onTick,
    emit: emit,
    query: query,
    setUrl: setUrl,
    announce: announce,
    wireCopy: wireCopy,
    openMenu: openMenu,
    closeMenu: closeMenu,
    paletteOpen: paletteOpen,
    paletteClose: paletteClose,
    paletteFilter: paletteFilter,
    paletteKey: paletteKey,
    get state() { return state; },
    get projectId() { return projectId; },
    // profile ownership
    profileStore: profileStore, activeProfile: activeProfile, savedConns: savedConns,
    scope: scope, setScope: setScope, visible: visible, visibleIds: visibleIds,
    checkAttention: checkAttention,
    setPaused: function (v) { paused = !!v; if (paused) stop(); else start(); },
    isPaused: function () { return paused; },
  };
})(typeof window !== 'undefined' ? window : this);
