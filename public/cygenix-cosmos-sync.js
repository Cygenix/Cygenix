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
    'cygenix_project_connections','cygenix_performance','cygenix_validation_sources',
    'cygenix_wasis_rules','cygenix_sql_scripts','cygenix_issues','cygenix_inventory',
  ];

  const FIELD_MAP = {
    jobs:'cygenix_jobs', project_settings:'cygenix_project_settings',
    project_plan:'cygenix_project_plan', connections:'cygenix_project_connections',
    performance:'cygenix_performance', validation_sources:'cygenix_validation_sources',
    wasis_rules:'cygenix_wasis_rules', sql_scripts:'cygenix_sql_scripts',
    issues:'cygenix_issues', inventory:'cygenix_inventory',
  };

  // Try multiple sources to get the logged-in user's ID
  function getUserId() {
    try {
      const raw = sessionStorage.getItem('cygenix_user');
      if (raw) {
        const u = JSON.parse(raw);
        const id = u.email || u.sub || u.id || u.user?.email;
        if (id) return id;
      }
    } catch {}
    try {
      const token = sessionStorage.getItem('cygenix_token');
      if (token) {
        const parts = token.split('.');
        if (parts.length === 3) {
          const payload = JSON.parse(atob(parts[1]));
          if (payload.email) return payload.email;
          if (payload.sub)   return payload.sub;
        }
      }
    } catch {}
    return null;
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
    try {
      const u = JSON.parse(sessionStorage.getItem('cygenix_user') || '{}');
      return callApi('user-create','POST',{ email: u.email || userId, name: u.user_metadata?.full_name || u.name || '' });
    } catch { return null; }
  }

  async function ping() { return callApi('ping','GET'); }
  async function getSubscription() { return callApi('subscription','GET'); }

  // Auto-save on localStorage writes
  let _saveTimer = null;
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
    await ensureUser();
    const hasLocal = SYNC_KEYS.some(k => localStorage.getItem(k) !== null);
    if (!hasLocal) {
      const loaded = await load();
      if (loaded) { console.log('[CygenixSync] Reloading with cloud data...'); setTimeout(() => location.reload(), 500); }
    } else {
      if (_saveTimer) clearTimeout(_saveTimer);
      _saveTimer = setTimeout(save, 3000);
    }
  }

  // Start after a short delay to let auth complete
  setTimeout(init, 800);

  return { init, save, load, forceLoad, ensureUser, ping, getSubscription, getUserId };
})();
