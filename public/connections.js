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

  // ── Identity: unified with cygenix-cosmos-sync.js ───────────────────────
  // Returns the current user's email (lowercase), or '' if nobody's signed
  // in. This MUST match getUserId() in the sync file — if they disagree,
  // one side writes under one uid and the other reads under another, and
  // data appears to vanish.
  //
  // HISTORY: Previously read MSAL.localAccountId (a GUID) and required the
  // msal-browser library to be loaded as a global on the page. But many
  // pages (including dashboard.html) don't load msal-browser, so this
  // silently returned '' everywhere, and every setActive()/save() call
  // bailed out without persisting. Rewritten 2026-04-23 to use the same
  // fallback chain as the sync file's getUserId() — which works whether
  // MSAL is loaded or not by reading MSAL's localStorage cache directly.
  //
  // NOTE: Now returns an email (e.g. 'user@example.com') not a GUID. The
  // readBlob() callers below have been left unchanged — they read a
  // per-user-keyed blob, and the key value has changed from GUID to email,
  // but the structural shape is identical, so older GUID-keyed entries
  // simply become orphaned. migrateLegacyBlob() below handles one-time
  // rescue of data stored under the old GUID key.
  function currentUserTag() {
    // Method 1: MSAL library loaded as a global (works when pages include
    // msal-browser, e.g. login.html).
    try {
      if (typeof msal !== 'undefined') {
        const msalApp = new msal.PublicClientApplication({
          auth: {
            clientId:         MSAL_CLIENT_ID,
            authority:        MSAL_AUTHORITY,
            knownAuthorities: MSAL_KNOWN_AUTHORITIES,
          },
          cache: { cacheLocation: 'localStorage' },
        });
        const accounts = msalApp.getAllAccounts() || [];
        if (accounts.length) {
          const a = accounts[0];
          const id = (a.username || a.idTokenClaims?.email || a.idTokenClaims?.preferred_username || '').trim().toLowerCase();
          if (id) return id;
        }
      }
    } catch {}

    // Method 2: Legacy custom-shape session/local entry
    try {
      const entraRaw = sessionStorage.getItem('cygenix_entra_account')
                    || localStorage.getItem('cygenix_entra_account');
      if (entraRaw) {
        const u = JSON.parse(entraRaw);
        const id = (u.email || u.userId || '').trim().toLowerCase();
        if (id) return id;
      }
    } catch {}

    // Method 3: cygenix_user object (Netlify Identity era)
    try {
      const raw = sessionStorage.getItem('cygenix_user') || localStorage.getItem('cygenix_user');
      if (raw) {
        const u = JSON.parse(raw);
        const email = u.email || u.user?.email;
        if (email) return email.trim().toLowerCase();
      }
    } catch {}

    // Method 4: decode cygenix_token JWT directly — handles URL-safe base64
    // and missing padding.
    try {
      const token = sessionStorage.getItem('cygenix_token') || localStorage.getItem('cygenix_token');
      if (token && token.split('.').length === 3) {
        let b64 = token.split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
        while (b64.length % 4) b64 += '=';
        const claims = JSON.parse(atob(b64));
        if (claims?.email)              return claims.email.trim().toLowerCase();
        if (claims?.preferred_username) return claims.preferred_username.trim().toLowerCase();
        if (claims?.sub && String(claims.sub).includes('@')) return String(claims.sub).trim().toLowerCase();
      }
    } catch {}

    return '';
  }

  // ── One-time migration of GUID-keyed entries ────────────────────────────
  // Before 2026-04-23 this module keyed its blobs by MSAL localAccountId
  // (a GUID). Now it keys by email. Entries saved under the old GUID would
  // be orphaned. On first call for a given key, if we find exactly one
  // non-email-looking top-level key in the blob AND the current user's
  // email slot is empty, we move the data across. Conservative: doesn't
  // run if the user has data under both keys, doesn't run if blob shape
  // is ambiguous.
  const _migratedKeys = new Set();
  function migrateLegacyBlob(key) {
    if (_migratedKeys.has(key)) return;
    _migratedKeys.add(key);
    const email = currentUserTag();
    if (!email) return;
    let blob;
    try { blob = JSON.parse(localStorage.getItem(key) || '{}'); } catch { return; }
    if (!blob || typeof blob !== 'object' || Array.isArray(blob)) return;
    const topKeys = Object.keys(blob);
    if (!topKeys.length) return;
    // Already migrated — user's email slot is populated
    if (blob[email]) return;
    // If there's exactly one top-level key and it's a GUID-shaped value,
    // migrate it under the user's email.
    const GUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    const guidKeys = topKeys.filter(k => GUID_RE.test(k));
    if (guidKeys.length === 1) {
      blob[email] = blob[guidKeys[0]];
      try {
        localStorage.setItem(key, JSON.stringify(blob));
        console.log(`[CygenixConnections] Migrated ${key} from GUID to email`);
      } catch {}
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
    migrateLegacyBlob(LS_ACTIVE);
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
    migrateLegacyBlob(LS_SAVED);
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
    savedGetAll, savedSetAll, savedAdd, savedDelete, savedUpdate, savedGetById,
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
