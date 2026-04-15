/**
 * cygenix-cosmos-sync.js  v1.0
 *
 * Syncs Cygenix localStorage data to/from Azure Cosmos DB.
 * Add to every page AFTER connections.js and nav.js:
 *   <script src="/cygenix-cosmos-sync.js"></script>
 *
 * How it works:
 *   - On page load: loads cloud data into localStorage (if localStorage is empty)
 *   - On every localStorage write: auto-saves to Cosmos DB after 3 seconds
 *   - userId: taken from the logged-in user's email (from sessionStorage)
 */

const CygenixSync = (() => {

  // ── Config ──────────────────────────────────────────────────────────────────
  const API_BASE  = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api';
  const FUNC_CODE = 'WjSmoWxgtNdGnO_I5nKIspRUQqKCR1knsXgVmJr3dyYuAzFu-or-5Q==';

  // localStorage keys that get synced to Cosmos DB
  const SYNC_KEYS = [
    'cygenix_jobs',
    'cygenix_project_settings',
    'cygenix_project_plan',
    'cygenix_project_connections',
    'cygenix_performance',
    'cygenix_validation_sources',
    'cygenix_wasis_rules',
    'cygenix_sql_scripts',
    'cygenix_issues',
    'cygenix_inventory',
  ];

  // Keys deliberately NOT synced (device/session specific)
  // cygenix_api_key            — Anthropic key, stays local
  // cygenix_onboarded          — UI state, stays local
  // cygenix_ask_conversations  — chat history, stays local
  // cygenix_backup_history     — local backup log
  // cygenix_integrations       — Smartsheet/Jira tokens, stays local

  // ── Get userId ──────────────────────────────────────────────────────────────
  function getUserId() {
    try {
      const raw = sessionStorage.getItem('cygenix_user');
      if (!raw) return null;
      const user = JSON.parse(raw);
      // Use email as the stable user ID
      return user.email || user.sub || user.id || null;
    } catch { return null; }
  }

  // ── API helper ──────────────────────────────────────────────────────────────
  async function callApi(action, method = 'GET', body = null) {
    const userId = getUserId();
    if (!userId) return null;  // not logged in

    const url = `${API_BASE}/data/${action}?code=${FUNC_CODE}`;
    const opts = {
      method,
      headers: {
        'Content-Type': 'application/json',
        'x-user-id':    userId
      }
    };
    if (body !== null) opts.body = JSON.stringify(body);

    try {
      const res = await fetch(url, opts);
      if (!res.ok) {
        console.warn(`[CygenixSync] ${action} → HTTP ${res.status}`);
        return null;
      }
      return await res.json();
    } catch (e) {
      console.warn('[CygenixSync] Network error on', action, ':', e.message);
      return null;
    }
  }

  // ── SAVE — push all localStorage keys to Cosmos ─────────────────────────────
  async function save() {
    const userId = getUserId();
    if (!userId) return null;

    const payload = {};
    SYNC_KEYS.forEach(key => {
      try {
        const raw = localStorage.getItem(key);
        if (raw !== null) {
          // Strip 'cygenix_' prefix → Cosmos field name
          const field = key.replace('cygenix_', '');
          payload[field] = JSON.parse(raw);
        }
      } catch {}
    });

    if (!Object.keys(payload).length) return null;

    const result = await callApi('save', 'POST', payload);
    if (result?.saved) {
      console.log('[CygenixSync] Saved to Cosmos DB ✓', result.updatedAt);
    }
    return result;
  }

  // ── LOAD — pull from Cosmos into localStorage ────────────────────────────────
  async function load() {
    const data = await callApi('load', 'GET');
    if (!data || !Object.keys(data).length) {
      console.log('[CygenixSync] No cloud data found for this user yet');
      return false;
    }

    // Map Cosmos fields back to localStorage keys
    const MAP = {
      jobs:               'cygenix_jobs',
      project_settings:   'cygenix_project_settings',
      project_plan:       'cygenix_project_plan',
      connections:        'cygenix_project_connections',
      performance:        'cygenix_performance',
      validation_sources: 'cygenix_validation_sources',
      wasis_rules:        'cygenix_wasis_rules',
      sql_scripts:        'cygenix_sql_scripts',
      issues:             'cygenix_issues',
      inventory:          'cygenix_inventory',
    };

    let count = 0;
    Object.entries(MAP).forEach(([field, lsKey]) => {
      const val = data[field];
      if (val !== undefined && val !== null) {
        try {
          // Preserve existing localStorage if it has content
          // Cloud data only fills gaps on first load
          if (!localStorage.getItem(lsKey)) {
            localStorage.setItem(lsKey, JSON.stringify(val));
            count++;
          }
        } catch {}
      }
    });

    console.log(`[CygenixSync] Loaded ${count} keys from Cosmos DB`);
    return count > 0;
  }

  // ── FORCE LOAD — overwrite localStorage with cloud data ─────────────────────
  // Call this when user explicitly clicks "Load from cloud"
  async function forceLoad() {
    const data = await callApi('load', 'GET');
    if (!data || !Object.keys(data).length) return false;

    const MAP = {
      jobs:               'cygenix_jobs',
      project_settings:   'cygenix_project_settings',
      project_plan:       'cygenix_project_plan',
      connections:        'cygenix_project_connections',
      performance:        'cygenix_performance',
      validation_sources: 'cygenix_validation_sources',
      wasis_rules:        'cygenix_wasis_rules',
      sql_scripts:        'cygenix_sql_scripts',
      issues:             'cygenix_issues',
      inventory:          'cygenix_inventory',
    };

    let count = 0;
    Object.entries(MAP).forEach(([field, lsKey]) => {
      const val = data[field];
      if (val !== undefined && val !== null) {
        try { localStorage.setItem(lsKey, JSON.stringify(val)); count++; } catch {}
      }
    });

    console.log(`[CygenixSync] Force-loaded ${count} keys from Cosmos DB`);
    return count > 0;
  }

  // ── ENSURE USER record exists in Cosmos ──────────────────────────────────────
  async function ensureUser() {
    const userId = getUserId();
    if (!userId) return null;
    try {
      const raw  = sessionStorage.getItem('cygenix_user');
      const user = raw ? JSON.parse(raw) : {};
      return await callApi('user-create', 'POST', {
        email: user.email || userId,
        name:  user.user_metadata?.full_name || user.name || ''
      });
    } catch (e) {
      console.warn('[CygenixSync] ensureUser failed:', e.message);
      return null;
    }
  }

  // ── GET subscription status ──────────────────────────────────────────────────
  async function getSubscription() {
    return await callApi('subscription', 'GET');
  }

  // ── PING — test Cosmos connectivity ─────────────────────────────────────────
  async function ping() {
    const result = await callApi('ping', 'GET');
    if (result?.ok) {
      console.log('[CygenixSync] Cosmos DB connected ✓', result.database);
    } else {
      console.warn('[CygenixSync] Cosmos DB ping failed');
    }
    return result;
  }

  // ── AUTO-SAVE — debounced, triggered by localStorage writes ─────────────────
  let _saveTimer = null;
  function scheduleAutoSave() {
    if (_saveTimer) clearTimeout(_saveTimer);
    _saveTimer = setTimeout(() => {
      save().catch(e => console.warn('[CygenixSync] Auto-save failed:', e.message));
    }, 3000); // 3 seconds after last change
  }

  // Intercept localStorage.setItem for Cygenix keys only
  const _origSetItem = localStorage.setItem.bind(localStorage);
  localStorage.setItem = function(key, value) {
    _origSetItem(key, value);
    if (SYNC_KEYS.includes(key)) scheduleAutoSave();
  };

  // ── INIT — call once on each page load ───────────────────────────────────────
  let _initDone = false;
  async function init() {
    if (_initDone) return;
    _initDone = true;

    const userId = getUserId();
    if (!userId) return;  // not logged in, nothing to do

    console.log('[CygenixSync] Initialising for user:', userId);

    // 1. Ensure user record exists (creates on first login)
    await ensureUser();

    // 2. Load cloud data into localStorage if localStorage is empty
    //    (if user already has local data we leave it alone — auto-save will
    //     push it to the cloud within 3s of the next write)
    const hasLocalData = SYNC_KEYS.some(k => localStorage.getItem(k) !== null);
    if (!hasLocalData) {
      console.log('[CygenixSync] No local data found — loading from cloud...');
      await load();
    } else {
      console.log('[CygenixSync] Local data exists — scheduling initial save to cloud...');
      // Push existing local data up to cloud on first load
      scheduleAutoSave();
    }
  }

  // Auto-init when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => init());
  } else {
    // DOM already ready
    setTimeout(() => init(), 100);
  }

  // ── Public API ───────────────────────────────────────────────────────────────
  return {
    init,
    save,
    load,
    forceLoad,
    ensureUser,
    getSubscription,
    ping,
    getUserId
  };

})();
