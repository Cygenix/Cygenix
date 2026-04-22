// connections.js
// ─────────────────────────────────────────────────────────────────────────────
// Central helper for Dashboard > Connections. Exposes `CygenixConnections`
// (module-level, not attached to window — treat as script-global) for pages
// to read/save the currently active source/target connection plus a list of
// saved connection recipes.
//
// ── STORAGE MODEL (Session of 2026-04-22) ─────────────────────────────────
// Previously stored the active pair in sessionStorage under per-user
// namespaced keys (`cygenix_src_conn_string::uid`, etc). Two problems:
//   1. sessionStorage is tab-scoped — connections evaporated on tab close.
//   2. The Cosmos sync watches localStorage — so connections never reached
//      the cloud, which meant they also never restored on a new device.
//
// Now we store everything in localStorage under two keys the sync already
// watches:
//   cygenix_project_connections  — active src/tgt pair for every signed-in
//                                   user, keyed by MSAL localAccountId:
//     { "<uid-guid>": { srcConnString, srcConnMode, tgtConnString,
//                       tgtConnMode, tgtFnUrl, tgtFnKey }, ... }
//   cygenix_saved_connections    — saved connection recipes, also keyed by
//                                   MSAL localAccountId:
//     { "<uid-guid>": [ { id, name, ... }, ... ], ... }
//
// Per-user isolation is preserved *inside* the blob rather than by
// namespacing the key — that way the sync's existing allowlist picks up
// both keys without any dynamic-key matching.
//
// ── IDENTITY ───────────────────────────────────────────────────────────────
// Unified across dashboard.html, connections.js, and cygenix-cosmos-sync.js:
// MSAL `localAccountId` (stable GUID, same across email sign-in and Google
// SSO for the same Entra user). Previous behaviour read from
// `cygenix_entra_account` — which nothing populates — so every read/write
// silently no-oped. That's been fixed.
//
// SCOPE: connections only. Projects, jobs, plans are still handled
// elsewhere and are still shared across users on the same device. That
// needs its own work in a later session.
// ─────────────────────────────────────────────────────────────────────────────

let CygenixConnections = (function () {

  // ── MSAL config (must match login.html and cygenix-cosmos-sync.js) ───────
  const MSAL_CLIENT_ID = 'f3478996-b2b5-4b21-9a23-a6b97a0e5b13';
  const MSAL_AUTHORITY = 'https://cygenix.ciamlogin.com/';
  const MSAL_KNOWN_AUTHORITIES = ['cygenix.ciamlogin.com'];

  // ── Storage keys (watched by cygenix-cosmos-sync.js) ─────────────────────
  const LS_ACTIVE = 'cygenix_project_connections';
  const LS_SAVED  = 'cygenix_saved_connections';

  // ── Identity: MSAL-first, unified ───────────────────────────────────────
  // Returns the current user's stable identifier (MSAL localAccountId GUID)
  // or '' if nobody's signed in. Must match getUserId() in the sync file —
  // if they disagree, one side writes under one uid and the other reads
  // under another, and data appears to vanish.
  function currentUserTag() {
    try {
      if (typeof msal === 'undefined') return '';
      // Reuse MSAL's existing localStorage cache; construct a lightweight
      // PublicClientApplication just to read accounts. This is safe to call
      // repeatedly — MSAL de-dupes internally.
      const msalApp = new msal.PublicClientApplication({
        auth: {
          clientId: MSAL_CLIENT_ID,
          authority: MSAL_AUTHORITY,
          knownAuthorities: MSAL_KNOWN_AUTHORITIES,
        },
        cache: { cacheLocation: 'localStorage' },
      });
      const accounts = msalApp.getAllAccounts() || [];
      if (!accounts.length) return '';
      // localAccountId is the OID — stable across IdPs, not user-editable.
      const id = accounts[0].localAccountId || '';
      return id.toLowerCase().trim();
    } catch {
      return '';
    }
  }

  // ── Blob readers/writers ────────────────────────────────────────────────
  // These operate on the whole top-level object. Per-user slicing happens
  // in get()/save()/savedGetAll()/savedSetAll(), not here.
  function readBlob(key) {
    try {
      const raw = localStorage.getItem(key);
      if (!raw) return {};
      const parsed = JSON.parse(raw);
      // Defensive: older code may have written a flat object (not keyed by
      // uid). Treat anything that doesn't look like a uid-keyed map as
      // empty — safer than silently exposing one user's data to another.
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {};
      return parsed;
    } catch { return {}; }
  }
  function writeBlob(key, obj) {
    try { localStorage.setItem(key, JSON.stringify(obj || {})); } catch {}
  }

  // ── Active pair: get / save / clear ──────────────────────────────────────

  // Read the active src/tgt pair for the current user.
  // Returns legacy field names so existing callers (dashboard, impGetConn,
  // updateConnDots) don't need changes.
  function get() {
    const uid = currentUserTag();
    if (!uid) {
      return {
        srcConnString: '', srcConnMode: 'direct',
        tgtConnString: '', tgtConnMode: 'direct',
        tgtFnUrl: '', tgtFnKey: '',
      };
    }
    const blob = readBlob(LS_ACTIVE);
    const mine = (blob[uid] && typeof blob[uid] === 'object') ? blob[uid] : {};
    return {
      srcConnString: mine.srcConnString || '',
      srcConnMode  : mine.srcConnMode   || 'direct',
      tgtConnString: mine.tgtConnString || '',
      tgtConnMode  : mine.tgtConnMode   || 'direct',
      tgtFnUrl     : mine.tgtFnUrl      || '',
      tgtFnKey     : mine.tgtFnKey      || '',
    };
  }

  // Write the full active pair. Preferred API going forward — dashboard
  // should call setActive({...}) rather than writing sessionStorage keys
  // directly.
  function setActive(fields) {
    const uid = currentUserTag();
    if (!uid) return false;
    if (!fields || typeof fields !== 'object') return false;
    const blob = readBlob(LS_ACTIVE);
    blob[uid] = {
      srcConnString: String(fields.srcConnString || ''),
      srcConnMode  : fields.srcConnMode === 'azure' ? 'azure' : 'direct',
      tgtConnString: String(fields.tgtConnString || ''),
      tgtConnMode  : fields.tgtConnMode   === 'azure' ? 'azure' : 'direct',
      tgtFnUrl     : String(fields.tgtFnUrl || ''),
      tgtFnKey     : String(fields.tgtFnKey || ''),
    };
    writeBlob(LS_ACTIVE, blob);
    return true;
  }

  // Back-compat save(). Old dashboard code wrote flat sessionStorage keys
  // first and then called save() to commit. We still honour that path so
  // the transition doesn't require all dashboard code to change in one go:
  // sweep recognised sessionStorage keys into the active blob for this
  // user, then clear them. Once all callers migrate to setActive() this
  // fallback can be deleted.
  function save() {
    const uid = currentUserTag();
    if (!uid) return;
    const cur = get(); // current persisted state — we merge, not overwrite
    const pullFlat = (k) => { try { return sessionStorage.getItem(k) || ''; } catch { return ''; } };
    // Recognised legacy flat keys. `cygenix_conn_string` / `cygenix_conn_mode`
    // are legacy aliases for target-direct.
    const srcString = pullFlat('cygenix_src_conn_string');
    const srcMode   = pullFlat('cygenix_src_conn_mode');
    const tgtString = pullFlat('cygenix_tgt_conn_string') || pullFlat('cygenix_conn_string');
    const tgtMode   = pullFlat('cygenix_tgt_conn_mode')   || pullFlat('cygenix_conn_mode');
    const fnUrl     = pullFlat('cygenix_fn_url');
    const fnKey     = pullFlat('cygenix_fn_key');
    const merged = {
      srcConnString: srcString || cur.srcConnString,
      srcConnMode  : srcMode   || cur.srcConnMode,
      tgtConnString: tgtString || cur.tgtConnString,
      tgtConnMode  : tgtMode   || cur.tgtConnMode,
      tgtFnUrl     : fnUrl     || cur.tgtFnUrl,
      tgtFnKey     : fnKey     || cur.tgtFnKey,
    };
    setActive(merged);
    // Now clear the flat sessionStorage keys we just absorbed — same
    // behaviour as the old code, so nothing downstream sees stale flats.
    ['cygenix_src_conn_string','cygenix_src_conn_mode','cygenix_tgt_conn_string',
     'cygenix_tgt_conn_mode','cygenix_fn_url','cygenix_fn_key',
     'cygenix_conn_string','cygenix_conn_mode'].forEach(k => {
      try { sessionStorage.removeItem(k); } catch {}
    });
  }

  // Kept for API compat — reads are already live so this is a no-op.
  function load() { return get(); }

  // Wipe the active pair for the current user only. Does not touch other
  // users' data or the saved-connection list.
  function clear() {
    const uid = currentUserTag();
    if (!uid) return;
    const blob = readBlob(LS_ACTIVE);
    if (blob[uid]) {
      delete blob[uid];
      writeBlob(LS_ACTIVE, blob);
    }
  }

  // ── Saved connections list (per-user) ────────────────────────────────────

  function savedGetAll() {
    const uid = currentUserTag();
    if (!uid) return [];
    const blob = readBlob(LS_SAVED);
    const arr = blob[uid];
    return Array.isArray(arr) ? arr : [];
  }
  function savedSetAll(list) {
    const uid = currentUserTag();
    if (!uid) return;
    const capped = Array.isArray(list) ? list.slice(0, 50) : [];
    const blob = readBlob(LS_SAVED);
    blob[uid] = capped;
    writeBlob(LS_SAVED, blob);
  }
  function savedAdd(entry) {
    if (!currentUserTag()) return null;
    if (!entry || typeof entry !== 'object') return null;
    const e = Object.assign({}, entry);
    if (!e.id)      e.id      = 'sconn_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7);
    if (!e.savedAt) e.savedAt = new Date().toISOString();
    const all = savedGetAll();
    all.push(e);
    savedSetAll(all);
    return e.id;
  }
  function savedDelete(id) {
    if (!currentUserTag() || !id) return false;
    const all = savedGetAll();
    const next = all.filter(c => c.id !== id);
    if (next.length === all.length) return false;
    savedSetAll(next);
    return true;
  }
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

  // ── Event surface (no-op, reserved) ──────────────────────────────────────
  function on()       { /* no-op */ }
  function off()      { /* no-op */ }
  function onChange() { /* no-op */ }
  function pingAll()  { /* no-op */ }

  // ── Public API + back-compat nibs ────────────────────────────────────────
  const api = {
    get, setActive, save, load, clear,
    savedGetAll, savedAdd, savedDelete, savedUpdate, savedGetById,
    on, off, onChange, pingAll,
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
