/* ============================================================================
   cygenix-sql-windows.js — the SQL Editor's windows, as data.
   ----------------------------------------------------------------------------
   The editor was one query at a time. Comparing two result sets, or keeping a
   lookup query beside the one being written, meant retyping — so people kept
   the other query in a text file, which is the tell that the tool was missing
   something.

   WHAT LIVES HERE AND WHAT DOES NOT
   This module owns the LIST: which windows exist, their order, which is
   active, what each is called, and what survives a reload. It owns no DOM and
   no editor. That separation is the point — the ordering, the close-and-reopen
   stack, the cap and the persistence are the parts with real edge cases, and
   they are testable without a browser only if they are kept away from one.

   The editor text is held twice on purpose: a Monaco model is the live copy
   while the page is open, and `sql` here is the copy that survives a reload.
   They are synced on a debounce, and on every window switch.

   WHAT IS NOT PERSISTED
   Result sets. A result is a photograph of a database at a moment, and
   restoring one after a reload would show somebody yesterday's rows under
   today's query with nothing saying so. Reloading returns each window to its
   "Run a query" state, which is honest about what is known.

   Nor is anything with a credential in it: the connection is stored as which
   SIDE was selected (source/target), never as a connection string.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object') root.CygenixSqlWindows = api;
})(typeof window !== 'undefined' ? window : this, function () {
'use strict';

/* Twenty is not a technical limit — it is the point past which a tab strip
   stops being navigable and the window menu is doing all the work anyway. */
var MAX_WINDOWS = 20;
var CLOSED_STACK = 10;
var KEY_PREFIX = 'cygenix_sql_windows::';

function nowId() {
  return 'w' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
}

function blank(n) {
  return {
    id: nowId(),
    name: 'Query ' + n,
    sql: '',
    savedSql: '',          // what `sql` was when last saved / loaded — dirty is the difference
    conn: 'source',
    script: null,          // { type:'script'|'job'|'drive', id, name } when opened from one
    // Live-only, never persisted:
    results: null,         // { rows, cols, ms, rowCount } or null
    error: null,
    history: [],
    running: false,
    viewState: null,       // Monaco cursor/selection/scroll
  };
}

/* The shape that goes to storage. Everything else is either reconstructible or
   deliberately not kept — see the header. */
function durable(w) {
  return { id: w.id, name: w.name, sql: w.sql, savedSql: w.savedSql,
           conn: w.conn, script: w.script || null };
}

function rehydrate(d, n) {
  var w = blank(n);
  w.id = d.id || w.id;
  w.name = d.name || w.name;
  w.sql = typeof d.sql === 'string' ? d.sql : '';
  w.savedSql = typeof d.savedSql === 'string' ? d.savedSql : w.sql;
  w.conn = d.conn === 'target' ? 'target' : 'source';
  w.script = d.script || null;
  return w;
}

function makeStore(opts) {
  opts = opts || {};
  var storage = opts.storage || (typeof localStorage !== 'undefined' ? localStorage : null);
  var projectId = opts.projectId || 'default';

  var windows = [];
  var activeId = null;
  var closed = [];          // stack of { window, index } for reopen
  var seq = 0;              // only ever counts up, so names do not collide
  var listeners = [];

  function key() { return KEY_PREFIX + projectId; }
  function emit() { listeners.forEach(function (fn) { try { fn(); } catch (e) {} }); }

  function save() {
    if (!storage) return;
    try {
      storage.setItem(key(), JSON.stringify({
        v: 1, activeId: activeId, seq: seq,
        windows: windows.map(durable),
      }));
    } catch (e) { /* quota or private mode: the session still works */ }
  }

  function load() {
    if (!storage) return false;
    var raw;
    try { raw = storage.getItem(key()); } catch (e) { return false; }
    if (!raw) return false;
    var doc;
    try { doc = JSON.parse(raw); } catch (e) { return false; }
    if (!doc || !Array.isArray(doc.windows) || !doc.windows.length) return false;
    windows = doc.windows.slice(0, MAX_WINDOWS).map(function (d, i) { return rehydrate(d, i + 1); });
    seq = typeof doc.seq === 'number' ? doc.seq : windows.length;
    activeId = windows.some(function (w) { return w.id === doc.activeId; })
      ? doc.activeId : windows[0].id;
    return true;
  }

  function get(id) {
    for (var i = 0; i < windows.length; i++) if (windows[i].id === id) return windows[i];
    return null;
  }
  function indexOf(id) {
    for (var i = 0; i < windows.length; i++) if (windows[i].id === id) return i;
    return -1;
  }
  function active() { return get(activeId); }

  /** True when the text differs from what was last saved or loaded. */
  function isDirty(w) {
    if (!w) return false;
    return (w.sql || '') !== (w.savedSql || '');
  }
  function anyDirty() { return windows.some(isDirty); }
  function dirtyNames() { return windows.filter(isDirty).map(function (w) { return w.name; }); }

  function create(init) {
    if (windows.length >= MAX_WINDOWS) {
      return { ok: false, reason: 'That is ' + MAX_WINDOWS + ' windows open, which is as many as ' +
        'the strip can show clearly. Close one you have finished with and this will open.' };
    }
    seq++;
    var w = blank(seq);
    if (init) {
      if (typeof init.sql === 'string') { w.sql = init.sql; w.savedSql = init.savedSql != null ? init.savedSql : ''; }
      if (init.name) w.name = init.name;
      if (init.conn) w.conn = init.conn === 'target' ? 'target' : 'source';
      if (init.script) { w.script = init.script; if (init.savedSql == null) w.savedSql = w.sql; }
    }
    windows.push(w);
    activeId = w.id;
    save(); emit();
    return { ok: true, window: w };
  }

  /* Closing keeps enough to put it back exactly where it was — including its
     position in the strip, because reopening into the wrong place is its own
     small annoyance. */
  function close(id) {
    var i = indexOf(id);
    if (i === -1) return { ok: false };
    var w = windows[i];
    windows.splice(i, 1);
    closed.push({ window: w, index: i });
    if (closed.length > CLOSED_STACK) closed.shift();

    if (activeId === id) {
      // The neighbour to the right, as a browser does; the left one if there
      // is no right one.
      activeId = windows.length ? (windows[Math.min(i, windows.length - 1)] || windows[0]).id : null;
    }
    if (!windows.length) { seq++; var fresh = blank(seq); windows.push(fresh); activeId = fresh.id; }
    save(); emit();
    return { ok: true, closed: w };
  }

  function reopen() {
    if (!closed.length) return { ok: false };
    var entry = closed.pop();
    if (windows.length >= MAX_WINDOWS) return { ok: false, reason: 'Too many windows are open.' };
    var at = Math.min(entry.index, windows.length);
    windows.splice(at, 0, entry.window);
    activeId = entry.window.id;
    save(); emit();
    return { ok: true, window: entry.window };
  }

  function closeOthers(id) {
    var keep = get(id);
    if (!keep) return { ok: false };
    windows.filter(function (w) { return w.id !== id; })
      .forEach(function (w, i) { closed.push({ window: w, index: i }); });
    while (closed.length > CLOSED_STACK) closed.shift();
    windows = [keep];
    activeId = id;
    save(); emit();
    return { ok: true };
  }

  /** Everything that is saved or was never touched — the tidy-up nobody minds. */
  function closeUnmodified() {
    var removed = 0;
    windows.slice().forEach(function (w) {
      if (isDirty(w)) return;
      if (!w.sql && !w.script && windows.length === 1) return;   // never leave zero
      if (windows.length === 1) return;
      close(w.id);
      removed++;
    });
    return { ok: true, removed: removed };
  }

  function closeAll() {
    windows.slice().forEach(function (w) { close(w.id); });
    return { ok: true };
  }

  function activate(id) {
    if (!get(id)) return { ok: false };
    activeId = id;
    save(); emit();
    return { ok: true };
  }

  function activateIndex(n) {
    if (n < 0 || n >= windows.length) return { ok: false };
    return activate(windows[n].id);
  }

  function cycle(delta) {
    if (!windows.length) return { ok: false };
    var i = indexOf(activeId);
    if (i === -1) i = 0;
    var next = (i + delta + windows.length) % windows.length;
    return activate(windows[next].id);
  }

  function rename(id, name) {
    var w = get(id);
    if (!w) return { ok: false };
    var clean = String(name == null ? '' : name).replace(/\s+/g, ' ').trim().slice(0, 60);
    if (!clean) return { ok: false };          // an empty name would give a nameless tab
    w.name = clean;
    save(); emit();
    return { ok: true };
  }

  /** Drag to reorder. from/to are indices in the current order. */
  function move(from, to) {
    if (from < 0 || from >= windows.length) return { ok: false };
    var t = Math.max(0, Math.min(to, windows.length - 1));
    if (t === from) return { ok: true };
    var w = windows.splice(from, 1)[0];
    windows.splice(t, 0, w);
    save(); emit();
    return { ok: true };
  }

  /* Text changes constantly; everything else changes rarely. Both go through
     here so there is one place that knows a change has to be persisted. */
  function update(id, patch) {
    var w = get(id);
    if (!w) return { ok: false };
    for (var k in patch) if (Object.prototype.hasOwnProperty.call(patch, k)) w[k] = patch[k];
    // Live-only fields do not need a write; text and identity do.
    if ('sql' in patch || 'name' in patch || 'conn' in patch ||
        'script' in patch || 'savedSql' in patch) save();
    emit();
    return { ok: true, window: w };
  }

  /** Called when a window is saved: its text becomes the new baseline. */
  function markSaved(id, script) {
    var w = get(id);
    if (!w) return { ok: false };
    w.savedSql = w.sql;
    if (script !== undefined) w.script = script;
    save(); emit();
    return { ok: true };
  }

  function addHistory(id, entry) {
    var w = get(id);
    if (!w) return { ok: false };
    w.history.unshift(entry);
    if (w.history.length > 50) w.history.length = 50;
    emit();                                   // history is live-only, no save
    return { ok: true };
  }

  function init() {
    if (!load()) { seq = 0; create(); }
    return { windows: windows, activeId: activeId };
  }

  return {
    init: init, list: function () { return windows; }, get: get, active: active,
    activeId: function () { return activeId; }, indexOf: indexOf,
    create: create, close: close, reopen: reopen, closeOthers: closeOthers,
    closeUnmodified: closeUnmodified, closeAll: closeAll,
    activate: activate, activateIndex: activateIndex, cycle: cycle,
    rename: rename, move: move, update: update, markSaved: markSaved,
    addHistory: addHistory,
    isDirty: isDirty, anyDirty: anyDirty, dirtyNames: dirtyNames,
    canReopen: function () { return closed.length > 0; },
    onChange: function (fn) { listeners.push(fn); },
    save: save,
    MAX_WINDOWS: MAX_WINDOWS,
  };
}

return { makeStore: makeStore, MAX_WINDOWS: MAX_WINDOWS, KEY_PREFIX: KEY_PREFIX, __blank: blank };
});
