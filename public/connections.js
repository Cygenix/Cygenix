// connections.js
// ─────────────────────────────────────────────────────────────────────────────
// Central helper for the Dashboard > Connections feature. Exposes a
// module-level `CygenixConnections` (NOT attached to window — treat as a
// script-global) so pages can read/save the "currently active" source/target
// connection plus a list of saved connection recipes.
//
// ── PER-USER ISOLATION (Session of 2026-04-19) ─────────────────────────────
// Previously stored everything under flat keys like `cygenix_src_conn_string`
// and `cygenix_saved_connections`. If User A saved connections on a shared
// device and User B signed in, User B saw User A's connection strings,
// passwords and all. Fixed by namespacing all connection storage by the
// current user's email from `cygenix_entra_account`.
//
// Effective keys become:
//    cygenix_src_conn_string :: foo@x.com   (etc.)
//
// When nobody is signed in, reads return empty and writes are dropped. This
// matches the intent "a user only sees their own connections."
//
// SCOPE OF THIS FILE: connections only. Projects, jobs, plans, etc. are
// handled elsewhere and still shared today — those need their own per-user
// work in future sessions.
// ─────────────────────────────────────────────────────────────────────────────

let CygenixConnections = (function () {
  // ── Per-user key building ────────────────────────────────────────────────
  // `cygenix_entra_account` is written by login.html / MSAL flow and holds
  // `{email, userId, name}`. Email is the stable per-user handle we key off.
  // If no account present (not signed in, or legacy session) → empty uid →
  // reads return empty and writes are no-ops.
  function currentUserTag() {
    try {
      const raw = sessionStorage.getItem('cygenix_entra_account')
               || localStorage.getItem('cygenix_entra_account');
      if (!raw) return '';
      const acc = JSON.parse(raw);
      const email = (acc && (acc.email || acc.userId)) || '';
      // Lowercase + trim to avoid two slightly-different tags for the same user.
      return email.toLowerCase().trim();
    } catch {
      return '';
    }
  }

  // Build a per-user storage key. Returns '' when signed out so callers can
  // no-op cleanly without having to branch.
  function keyFor(base) {
    const uid = currentUserTag();
    if (!uid) return '';
    return base + '::' + uid;
  }

  // Namespaced sessionStorage getters/setters. Transparent about when the
  // user isn't signed in — return '' rather than fabricating defaults.
  function ssGet(base) {
    const k = keyFor(base);
    if (!k) return '';
    try { return sessionStorage.getItem(k) || ''; } catch { return ''; }
  }
  function ssSet(base, val) {
    const k = keyFor(base);
    if (!k) return;  // not signed in: silently drop — avoids cross-user leakage
    try {
      if (val == null || val === '') sessionStorage.removeItem(k);
      else                           sessionStorage.setItem(k, String(val));
    } catch {}
  }
  function lsGet(base) {
    const k = keyFor(base);
    if (!k) return null;
    try { return localStorage.getItem(k); } catch { return null; }
  }
  function lsSet(base, val) {
    const k = keyFor(base);
    if (!k) return;
    try {
      if (val == null) localStorage.removeItem(k);
      else             localStorage.setItem(k, String(val));
    } catch {}
  }

  // ── Public API ───────────────────────────────────────────────────────────

  // Read the "currently active" connection pair. Returns a plain object
  // with the legacy field names so existing callers don't need changes.
  function get() {
    return {
      srcConnString : ssGet('cygenix_src_conn_string'),
      srcConnMode   : ssGet('cygenix_src_conn_mode') || 'direct',
      tgtConnString : ssGet('cygenix_tgt_conn_string'),
      tgtConnMode   : ssGet('cygenix_tgt_conn_mode') || 'direct',
      tgtFnUrl      : ssGet('cygenix_fn_url'),
      tgtFnKey      : ssGet('cygenix_fn_key'),
    };
  }

  // Persist the currently active pair. Called by dashboard Save button.
  // Caller typically writes directly to sessionStorage first (the raw values)
  // and then calls save() to trigger any side-effects. We now also enforce
  // per-user scoping: we copy any FLAT (unscoped) keys the caller may have
  // set into the per-user slots, and clear the flat versions. That way old
  // code paths that still write flat keys don't bypass isolation.
  function save() {
    const uid = currentUserTag();
    if (!uid) return;  // not signed in — refuse to persist anything
    // Canonical keys we persist under.
    const canonical = ['cygenix_src_conn_string','cygenix_src_conn_mode','cygenix_tgt_conn_string',
                       'cygenix_tgt_conn_mode','cygenix_fn_url','cygenix_fn_key'];
    canonical.forEach(flat => {
      let v = '';
      try { v = sessionStorage.getItem(flat); } catch {}
      if (v) {
        ssSet(flat, v);
        try { sessionStorage.removeItem(flat); } catch {}
      }
    });
    // Legacy aliases used by some older dashboard code:
    //   cygenix_conn_string  → cygenix_tgt_conn_string (target, direct-mode)
    //   cygenix_conn_mode    → cygenix_tgt_conn_mode   (target mode)
    // Absorb them so we don't silently lose the value just because the caller
    // used the old name.
    const aliasMap = {
      'cygenix_conn_string': 'cygenix_tgt_conn_string',
      'cygenix_conn_mode'  : 'cygenix_tgt_conn_mode',
    };
    for (const [legacy, target] of Object.entries(aliasMap)) {
      let v = '';
      try { v = sessionStorage.getItem(legacy); } catch {}
      if (v) {
        ssSet(target, v);
        try { sessionStorage.removeItem(legacy); } catch {}
      }
    }
  }

  // Force-rehydrate from storage. Kept for API compatibility — reads are
  // already live so this is a no-op, but some callers expect the function
  // to exist.
  function load() {
    return get();
  }

  // Wipe the currently active pair for THIS user only. Does not touch other
  // users' data or the saved-connection list.
  function clear() {
    ['cygenix_src_conn_string','cygenix_src_conn_mode','cygenix_tgt_conn_string',
     'cygenix_tgt_conn_mode','cygenix_fn_url','cygenix_fn_key']
      .forEach(k => ssSet(k, ''));
  }

  // ── Saved connections list (per-user) ────────────────────────────────────
  // The dashboard has its own sconnGetAll/sconnSetAll helpers that use the
  // LS_SAVED key below. Those call into these functions now so they inherit
  // per-user scoping. See dashboard.html for UI of the saved-chip row.
  const LS_SAVED = 'cygenix_saved_connections';

  function savedGetAll() {
    const raw = lsGet(LS_SAVED);
    if (!raw) return [];
    try {
      const arr = JSON.parse(raw);
      return Array.isArray(arr) ? arr : [];
    } catch { return []; }
  }
  function savedSetAll(list) {
    // Cap at 50 — matches the existing dashboard cap
    const capped = Array.isArray(list) ? list.slice(0, 50) : [];
    lsSet(LS_SAVED, JSON.stringify(capped));
  }
  // Add a new saved entry. Assigns an id if caller didn't supply one.
  function savedAdd(entry) {
    if (!currentUserTag()) return null;  // not signed in
    if (!entry || typeof entry !== 'object') return null;
    const e = Object.assign({}, entry);
    if (!e.id) e.id = 'sconn_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7);
    if (!e.savedAt) e.savedAt = new Date().toISOString();
    const all = savedGetAll();
    all.push(e);
    savedSetAll(all);
    return e.id;
  }
  // Delete by id. Returns true if something was deleted.
  function savedDelete(id) {
    if (!currentUserTag()) return false;
    if (!id) return false;
    const all = savedGetAll();
    const next = all.filter(c => c.id !== id);
    if (next.length === all.length) return false;
    savedSetAll(next);
    return true;
  }
  // Update one entry (used by rename). Returns true on success.
  function savedUpdate(id, patch) {
    if (!currentUserTag() || !id || !patch) return false;
    const all = savedGetAll();
    const idx = all.findIndex(c => c.id === id);
    if (idx < 0) return false;
    all[idx] = Object.assign({}, all[idx], patch);
    savedSetAll(all);
    return true;
  }
  function savedGetById(id) {
    return savedGetAll().find(c => c.id === id) || null;
  }

  // ── Event surface ────────────────────────────────────────────────────────
  // Minimal: we don't emit real events today. Consumers that expected `.on`
  // should treat it as a no-op until the event system is put back.
  function on()       { /* no-op */ }
  function off()      { /* no-op */ }
  function onChange() { /* no-op */ }
  function pingAll()  { /* no-op — reserved for live-refresh in a later session */ }

  // ── Back-compat nibs ─────────────────────────────────────────────────────
  // Some older code reads CygenixConnections.srcConn / .tgtConn as strings.
  // Expose them as computed getters over the current user's active pair.
  // Returning '' when signed out matches what `get()` does.
  const api = {
    get, save, load, clear,
    // Saved list (per-user)
    savedGetAll, savedAdd, savedDelete, savedUpdate, savedGetById,
    // Events (no-op today, reserved)
    on, off, onChange, pingAll,
    // Current-user helpers (exposed so pages can show "logged in as X" hints
    // and know whether any persistence will actually happen)
    currentUserTag,
  };
  Object.defineProperty(api, 'srcConn', {
    get() { return get().srcConnString || ''; },
  });
  Object.defineProperty(api, 'tgtConn', {
    get() {
      const c = get();
      if (c.tgtFnUrl) {
        return c.tgtFnKey
          ? c.tgtFnUrl + (c.tgtFnUrl.includes('?') ? '&' : '?') + 'code=' + encodeURIComponent(c.tgtFnKey)
          : c.tgtFnUrl;
      }
      return c.tgtConnString || '';
    },
  });
  return api;
})();
