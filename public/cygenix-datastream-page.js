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
    return state;
  }

  function persist() { if (state) DS.save(state); }

  function start() {
    if (timer) return;
    timer = setInterval(function () {
      if (!state) return;
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
    setPaused: function (v) { paused = !!v; if (paused) stop(); else start(); },
    isPaused: function () { return paused; },
  };
})(typeof window !== 'undefined' ? window : this);
