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
    project_plan:'cygenix_project_plan', connections:'cygenix_project_connections',
    saved_connections:'cygenix_saved_connections',
    performance:'cygenix_performance', validation_sources:'cygenix_validation_sources',
    wasis_rules:'cygenix_wasis_rules', sql_scripts:'cygenix_sql_scripts',
    issues:'cygenix_issues', inventory:'cygenix_inventory',
    sys_params:'cygenix_sys_params',
  };

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
    if (!data) return false;
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
    // differ, snapshot the old data into sessionStorage first so the user has
    // a recovery path within the same tab session.
    const storedUserId = (localStorage.getItem('cygenix_active_user') || '').trim().toLowerCase();
    const currentUserId = userId.trim().toLowerCase();
    if (storedUserId && storedUserId !== currentUserId) {
      console.log('[CygenixSync] Different user detected — snapshotting and clearing local data. Was:', storedUserId, 'Now:', currentUserId);
      const snapshot = { wipedAt: new Date().toISOString(), wipedFrom: storedUserId, wipedFor: currentUserId, data: {} };
      SYNC_KEYS.forEach(k => {
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

    // ── Per-key gap-fill from cloud ────────────────────────────────────────
    // Previously this was all-or-nothing: if ANY SYNC_KEY had local data, the
    // cloud load was skipped entirely and all other keys stayed empty until
    // the user happened to wipe localStorage. That's how the System
    // Parameters and Was/Is pages rendered blank for 30+ minutes post-
    // sign-in even though Cosmos had the data — the `.some()` short-circuit
    // meant "local authoritative" for keys that had never been populated on
    // this browser.
    //
    // New policy: ALWAYS fetch from cloud on init, fill only the gaps (keys
    // where localStorage is null). Local values win on collision — that's
    // deliberate; it preserves any edits made offline or before init
    // completed, and avoids a class of clobber bugs. Then kick the debounced
    // save so anything purely-local propagates up.
    //
    // No page reload needed. Views that read localStorage after this point
    // will see the filled-in values; views that already rendered should
    // re-read via their existing load hooks — see the cygenix-sync-loaded
    // event dispatched below.
    const cloud = await callApi('load', 'GET');
    let filled = 0;
    if (cloud && typeof cloud === 'object') {
      for (const [cloudField, localKey] of Object.entries(FIELD_MAP)) {
        const cloudVal = cloud[cloudField];
        if (cloudVal === undefined || cloudVal === null) continue;
        if (localStorage.getItem(localKey) !== null) continue; // local wins
        try {
          localStorage.setItem(localKey, JSON.stringify(cloudVal));
          filled++;
        } catch (e) {
          console.warn('[CygenixSync] Failed to fill', localKey, e.message);
        }
      }
    }
    if (filled > 0) {
      console.log('[CygenixSync] Filled', filled, 'missing keys from Cosmos DB');
      // Notify any views already rendered that they should re-read
      // localStorage. Views that aren't listening will pick up the values
      // naturally on their next load. Keeps this non-breaking for any page
      // that doesn't know about the event.
      try {
        window.dispatchEvent(new CustomEvent('cygenix-sync-loaded', {
          detail: { filled, source: 'init' }
        }));
      } catch {}
    } else {
      console.log('[CygenixSync] No cloud gaps to fill');
    }

    // Push any local-only data back up (merge on the server preserves
    // cloud-only fields, so this is safe). Debounced so multiple pages
    // loading in quick succession don't each fire their own save.
    if (_saveTimer) clearTimeout(_saveTimer);
    _saveTimer = setTimeout(save, 3000);
  }

  // Start after a short delay to let auth complete
  setTimeout(init, 800);

  return { init, save, saveNow, load, forceLoad, ensureUser, ping, getSubscription, getUserId };
})();
