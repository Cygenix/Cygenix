/**
 * cygenix-cosmos-sync.js  v1.1
 * Syncs Cygenix localStorage to/from Azure Cosmos DB.
 * Auto-injected via nav.js on every page.
 */
const CygenixSync = (() => {

  const API_BASE  = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/data';
  const FUNC_CODE = 'WjSmoWxgtNdGnO_I5nKIspRUQqKCR1knsXgVmJr3dyYuAzFu-or-5Q==';

  const SYNC_KEYS = [
    'cygenix_jobs','cygenix_project_settings','cygenix_project_plan',
    'cygenix_project_connections','cygenix_saved_connections',
    'cygenix_performance','cygenix_validation_sources',
    'cygenix_wasis_rules','cygenix_sql_scripts','cygenix_issues','cygenix_inventory',
    'cygenix_sys_params',
  ];

  const FIELD_MAP = {
    jobs:'cygenix_jobs', project_settings:'cygenix_project_settings',
    project_plan:'cygenix_project_plan',
    // save() emits payload keys by mechanically stripping the 'cygenix_' prefix,
    // so cygenix_project_connections becomes 'project_connections' in the
    // payload — NOT 'connections'. Previously FIELD_MAP said
    //   connections: 'cygenix_project_connections'
    // which meant save() uploaded as 'project_connections' but load() looked
    // for 'connections' and found nothing. Connection blobs were silently
    // one-way: upstream only, never coming back down. Fixed to match.
    project_connections:'cygenix_project_connections',
    saved_connections:'cygenix_saved_connections',
    performance:'cygenix_performance', validation_sources:'cygenix_validation_sources',
    wasis_rules:'cygenix_wasis_rules', sql_scripts:'cygenix_sql_scripts',
    issues:'cygenix_issues', inventory:'cygenix_inventory',
    sys_params:'cygenix_sys_params',
  };

  // Keys in SYNC_KEYS whose contents are ALREADY per-user-scoped internally
  // (top-level object keyed by user id). These are NOT wiped by the
  // user-switch protection in init() — doing so would nuke other users'
  // data that legitimately lives in the same blob. See connections.js for
  // how the scoping works.
  const SELF_SCOPED_KEYS = new Set([
    'cygenix_project_connections',
    'cygenix_saved_connections',
  ]);

  // Extract userId — MSAL-first (authoritative post-migration), with legacy
  // fallbacks for back-compat. Critical that this returns a stable value: the
  // init() flow uses it to decide whether to wipe localStorage as part of the
  // user-switch protection, so instability here can cause data loss.
  function getUserId() {
    // Method 1: MSAL account cache (authoritative after Entra sign-in).
    //   Nothing in the app currently populates cygenix_entra_account, so
    //   before this fallback existed, the function was falling all the way
    //   through to JWT decode — which failed on URL-safe base64.
    try {
      if (typeof msal !== 'undefined') {
        const msalApp = new msal.PublicClientApplication({
          auth: {
            clientId:  'f3478996-b2b5-4b21-9a23-a6b97a0e5b13',
            authority: 'https://cygenix.ciamlogin.com/',
            knownAuthorities: ['cygenix.ciamlogin.com'],
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

    // Method 2: Entra External ID session (legacy custom key)
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
        const at = u.access_token;
        if (at && at.split('.').length === 3) {
          const claims = decodeJwt(at);
          if (claims?.email) return claims.email.trim().toLowerCase();
          if (claims?.sub && claims.sub.includes('@')) return claims.sub.trim().toLowerCase();
        }
      }
    } catch {}
    // Method 4: decode cygenix_token JWT directly. URL-safe base64 must be
    //   normalised before atob() or this silently throws — the root cause of
    //   init() retrying for 20s and then giving up entirely for some users.
    try {
      const token = sessionStorage.getItem('cygenix_token') || localStorage.getItem('cygenix_token');
      if (token && token.split('.').length === 3) {
        const claims = decodeJwt(token);
        if (claims?.email) return claims.email.trim().toLowerCase();
        if (claims?.preferred_username) return claims.preferred_username.trim().toLowerCase();
        if (claims?.sub && claims.sub.includes('@')) return claims.sub.trim().toLowerCase();
      }
    } catch {}
    return null;
  }

  // Decode a JWT payload, handling URL-safe base64 (-/_ instead of +//) and
  // missing padding. Returns null on any failure — callers handle nulls.
  function decodeJwt(token) {
    try {
      let b64 = token.split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
      while (b64.length % 4) b64 += '=';
      return JSON.parse(atob(b64));
    } catch { return null; }
  }

  async function callApi(action, method, body) {
    const userId = getUserId();
    if (!userId) return null;
    try {
      const res = await fetch(`${API_BASE}/${action}?code=${FUNC_CODE}`, {
        method: method || 'GET',
        headers: { 'Content-Type': 'application/json', 'x-user-id': userId },
        body: body ? JSON.stringify(body) : undefined
      });
      if (!res.ok) { console.warn('[CygenixSync]', action, res.status); return null; }
      return await res.json();
    } catch (e) { console.warn('[CygenixSync] error:', e.message); return null; }
  }

  async function save() {
    if (!getUserId()) return null;
    const payload = {};
    SYNC_KEYS.forEach(key => {
      try { const v = localStorage.getItem(key); if (v) payload[key.replace('cygenix_','')] = JSON.parse(v); } catch {}
    });
    if (!Object.keys(payload).length) return null;
    // NOTE on multi-user blobs (cygenix_project_connections,
    // cygenix_saved_connections): these are self-scoped JSON objects keyed
    // internally by MSAL localAccountId. This save() uploads whatever the
    // local blob contains, which will include OTHER users who have signed
    // in on this device. That's correct for this device, but if a different
    // device with a different subset of users also saves, each device's
    // view of the blob can diverge. If that becomes a real problem, the
    // Azure Function needs a server-side merge that unions the per-uid
    // entries instead of replacing the whole blob. Out of scope for now
    // (sole-user setup).
    const r = await callApi('save','POST',payload);
    if (r?.saved) console.log('[CygenixSync] Saved to Cosmos DB', r.updatedAt);
    return r;
  }

  async function load() {
    const data = await callApi('load','GET');
    if (!data || !Object.keys(data).length) { console.log('[CygenixSync] No cloud data yet'); return false; }
    let n = 0;
    Object.entries(FIELD_MAP).forEach(([f,k]) => {
      const v = data[f];
      if (v !== undefined && v !== null) { try { localStorage.setItem(k, JSON.stringify(v)); n++; } catch {} }
    });
    console.log('[CygenixSync] Loaded', n, 'keys from Cosmos DB');
    return n > 0;
  }

  async function forceLoad() {
    const data = await callApi('load','GET');
    // Symmetric with load(): if cloud returns null or an empty object, bail
    // without touching local. Previously forceLoad would happily "force-load"
    // zero keys over a valid local copy — effectively a no-op, but the
    // asymmetry was confusing and made recovery logic harder to reason about.
    if (!data || !Object.keys(data).length) {
      console.log('[CygenixSync] Force-load: no cloud data, keeping local');
      return false;
    }
    let n = 0;
    Object.entries(FIELD_MAP).forEach(([f,k]) => {
      const v = data[f];
      if (v !== undefined && v !== null) { try { localStorage.setItem(k, JSON.stringify(v)); n++; } catch {} }
    });
    console.log('[CygenixSync] Force-loaded', n, 'keys from Cosmos DB');
    return n > 0;
  }

  async function ensureUser() {
    const userId = getUserId(); if (!userId) return null;
    let name = '';
    // Prefer MSAL's account cache for the display name
    try {
      if (typeof msal !== 'undefined') {
        const msalApp = new msal.PublicClientApplication({
          auth: {
            clientId:  'f3478996-b2b5-4b21-9a23-a6b97a0e5b13',
            authority: 'https://cygenix.ciamlogin.com/',
            knownAuthorities: ['cygenix.ciamlogin.com'],
          },
          cache: { cacheLocation: 'localStorage' },
        });
        const acc = (msalApp.getAllAccounts() || [])[0];
        if (acc) name = acc.name || acc.idTokenClaims?.name || '';
      }
    } catch {}
    // Fall back to legacy cygenix_user shape if MSAL didn't give us a name
    if (!name) {
      try {
        const u = JSON.parse(sessionStorage.getItem('cygenix_user') || localStorage.getItem('cygenix_user') || '{}');
        name = u.user_metadata?.full_name || u.name || '';
      } catch {}
    }
    return callApi('user-create','POST',{ email: userId, name });
  }

  async function ping() { return callApi('ping','GET'); }
  async function getSubscription() { return callApi('subscription','GET'); }

  // Debounced auto-save on localStorage writes — shared timer so the manual
  // saveNow() can cancel pending writes and flush immediately.
  let _saveTimer = null;

  // Public-facing immediate save. Cancels any pending debounced save, flushes
  // straight to Cosmos, and returns a structured result so UI callers can show
  // accurate success / failure state. Use this for "Save" buttons — the
  // auto-save debounce is fine for background writes but a button click
  // should feel immediate and surface errors.
  async function saveNow() {
    if (_saveTimer) { clearTimeout(_saveTimer); _saveTimer = null; }
    if (!getUserId()) return { ok: false, error: 'not-signed-in' };
    try {
      const r = await save();
      if (r?.saved) return { ok: true, updatedAt: r.updatedAt };
      return { ok: false, error: 'no-response' };
    } catch (e) {
      return { ok: false, error: e.message || String(e) };
    }
  }

  // Auto-save on localStorage writes
  const _orig = localStorage.setItem.bind(localStorage);
  localStorage.setItem = function(k, v) {
    _orig(k, v);
    if (SYNC_KEYS.includes(k) && getUserId()) {
      if (_saveTimer) clearTimeout(_saveTimer);
      _saveTimer = setTimeout(save, 3000);
    }
  };

  // Init with retry — waits until user is logged in
  let _done = false, _retries = 0;
  async function init() {
    if (_done) return;
    const userId = getUserId();
    if (!userId) {
      if (_retries++ < 20) setTimeout(init, 1000); // retry every second for 20s
      return;
    }
    _done = true;
    console.log('[CygenixSync] User:', userId);

    // ── Check if localStorage belongs to a DIFFERENT user ──────────────────
    // Normalise both sides before comparing — a casing or whitespace mismatch
    // here was previously enough to trigger a full local wipe. If they really
    // differ, snapshot the old non-self-scoped data into sessionStorage first
    // so the user has a recovery path within the same tab session.
    //
    // Self-scoped keys (cygenix_project_connections, cygenix_saved_connections)
    // are NOT wiped on user-switch: they contain entries for every user who's
    // ever signed in on this device, keyed internally by MSAL localAccountId.
    // Wiping them here would nuke data that legitimately belongs to other
    // users on the shared device.
    const storedUserId = (localStorage.getItem('cygenix_active_user') || '').trim().toLowerCase();
    const currentUserId = userId.trim().toLowerCase();
    if (storedUserId && storedUserId !== currentUserId) {
      console.log('[CygenixSync] Different user detected — snapshotting and clearing legacy per-device data. Was:', storedUserId, 'Now:', currentUserId);
      const snapshot = { wipedAt: new Date().toISOString(), wipedFrom: storedUserId, wipedFor: currentUserId, data: {} };
      SYNC_KEYS.forEach(k => {
        if (SELF_SCOPED_KEYS.has(k)) return; // preserve — self-scoped internally
        const v = localStorage.getItem(k);
        if (v) snapshot.data[k] = v;
        localStorage.removeItem(k);
      });
      localStorage.removeItem('cygenix_active_project');
      try { sessionStorage.setItem('cygenix_wiped_snapshot', JSON.stringify(snapshot)); } catch {}
    }
    // Store current user (normalised) so future user-switch checks are stable
    localStorage.setItem('cygenix_active_user', currentUserId);

    await ensureUser();

    const hasLocal = SYNC_KEYS.some(k => localStorage.getItem(k) !== null);
    if (!hasLocal) {
      // No local data — load from cloud (may also be empty for new users)
      const loaded = await load();
      if (loaded) {
        console.log('[CygenixSync] Reloading with cloud data...');
        setTimeout(() => location.reload(), 500);
      } else {
        console.log('[CygenixSync] New user — starting fresh');
      }
    } else {
      // Local data exists and belongs to this user — push to cloud
      if (_saveTimer) clearTimeout(_saveTimer);
      _saveTimer = setTimeout(save, 3000);
    }
  }

  // Start after a short delay to let auth complete
  setTimeout(init, 800);

  return { init, save, saveNow, load, forceLoad, ensureUser, ping, getSubscription, getUserId };
})();
