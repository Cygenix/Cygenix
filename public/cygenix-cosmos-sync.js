/**
 * cygenix-cosmos-sync.js  v1.1
 * Syncs Cygenix localStorage to/from Azure Cosmos DB.
 * Auto-injected via nav.js on every page.
 */
const CygenixSync = (() => {

  const API_BASE  = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/data';
  const FUNC_CODE = 'WjSmoWxgtNdGnO_I5nKIspRUQqKCR1knsXgVmJr3dyYuAzFu-or-5Q==';

  // Capture the unmonkey-patched setItem early. We override localStorage.setItem
  // below to trigger auto-save, and several internal codepaths need to write
  // localStorage WITHOUT re-triggering that — using _orig avoids the loop.
  const _orig = localStorage.setItem.bind(localStorage);

  const SYNC_KEYS = [
    'cygenix_jobs','cygenix_project_settings','cygenix_project_plan',
    'cygenix_project_connections','cygenix_saved_connections',
    'cygenix_performance','cygenix_validation_sources',
    'cygenix_wasis_rules','cygenix_sql_scripts','cygenix_issues','cygenix_inventory',
    'cygenix_sys_params',
    // Multi-project model: array of { id, name, client, status, ... }. Until
    // recently nothing wrote to this key so it was effectively unused; with
    // projects.html and the dashboard Projects card it's now the source of
    // truth for the user's project list, so include it in sync.
    'cygenix_projects',
  ];

  const FIELD_MAP = {
    jobs:'cygenix_jobs', project_settings:'cygenix_project_settings',
    project_plan:'cygenix_project_plan', connections:'cygenix_project_connections',
    saved_connections:'cygenix_saved_connections',
    performance:'cygenix_performance', validation_sources:'cygenix_validation_sources',
    wasis_rules:'cygenix_wasis_rules', sql_scripts:'cygenix_sql_scripts',
    issues:'cygenix_issues', inventory:'cygenix_inventory',
    sys_params:'cygenix_sys_params',
    projects:'cygenix_projects',
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
      if (!res.ok) {
        // Capture the response body — without Application Insights or Live
        // Log Stream available on this Azure plan, the browser console is
        // the only diagnostic surface for server errors. The Azure Function
        // is wrapped to return err.message + err.stack in 500 bodies.
        let detail = '';
        try { detail = (await res.text()).slice(0, 500); } catch {}
        console.warn('[CygenixSync]', action, res.status, detail);
        return null;
      }
      return await res.json();
    } catch (e) { console.warn('[CygenixSync] error:', e.message); return null; }
  }

  // Build the payload to push to Cosmos. CRITICAL: this used to read
  // localStorage and push directly, which overwrote any cloud-only records
  // (e.g. jobs created server-side by the agentive migration backend, or by
  // other browsers since the last load). Now it fetches cloud first and
  // merges array-of-{id} fields by ID, with localStorage winning on
  // collision. Object fields are still local-wins (those are user-edited
  // config like project_settings, not lists).
  //
  // Why this matters: the auto-save below fires 3s after ANY localStorage
  // write to a sync key. Without merge logic, any backend-side write to
  // jobs[] gets clobbered within seconds.
  async function buildMergedPayload() {
    // Read all local sync keys into `local`, keyed by the CLOUD field name
    // (not the prefix-stripped localStorage name). Iterating FIELD_MAP
    // rather than SYNC_KEYS means save and load agree on field names —
    // previously this used `key.replace('cygenix_','')` which produced
    // 'project_connections' from 'cygenix_project_connections', but the
    // gap-fill loop in init() reads `cloud['connections']`. Result: every
    // save pushed connections under the wrong field name and every load
    // looked under the right one and found nothing. Same applied to any
    // FIELD_MAP entry where the cloud field name differed from the
    // prefix-stripped localStorage key (currently just `connections`,
    // but worth keeping the loop FIELD_MAP-driven so future entries
    // can't hit this).
    const local = {};
    for (const [cloudField, localKey] of Object.entries(FIELD_MAP)) {
      try {
        const v = localStorage.getItem(localKey);
        if (v) local[cloudField] = JSON.parse(v);
      } catch {}
    }
    if (!Object.keys(local).length) return null;

    // Pull current cloud state so we can preserve anything cloud-only
    const cloud = await callApi('load','GET');
    if (!cloud) return local; // can't fetch cloud — fall back to old behaviour

    // For each field, pick a merge strategy
    const merged = {};
    for (const [field, localVal] of Object.entries(local)) {
      const cloudVal = cloud[field];
      merged[field] = mergeField(field, localVal, cloudVal);
    }
    return merged;
  }

  // Decide how to merge cloud and local for a given field.
  // - Both arrays of objects with `id` → union by id, local wins on collision
  // - Otherwise → local wins entirely (preserves existing behaviour)
  function mergeField(field, localVal, cloudVal) {
    if (cloudVal === undefined || cloudVal === null) return localVal;
    if (!Array.isArray(localVal) || !Array.isArray(cloudVal)) return localVal;

    // Check both are arrays-of-objects-with-id. If not, local wins.
    const isIdArray = arr => arr.length === 0 || (typeof arr[0] === 'object' && arr[0] !== null && 'id' in arr[0]);
    if (!isIdArray(localVal) || !isIdArray(cloudVal)) {
      // Safety net: if cloud has more items than local for an array field
      // without ids, we're about to silently overwrite cloud-only data.
      // That's the wasis_rules class of regression — rules added on one
      // device disappearing on another. Warn so the next instance is
      // caught by browser devtools rather than by a user. Empty-cloud is
      // expected (first-time push) so we skip the warning then.
      if (cloudVal.length > 0 && cloudVal.length > localVal.length) {
        console.warn(
          '[CygenixSync] mergeField: "' + field + '" — local (' + localVal.length +
          ' items) overwriting cloud (' + cloudVal.length + ' items). ' +
          'Field has no id-shape; consider adding stable ids so merge can union them.'
        );
      }
      return localVal;
    }

    // Union by id, local wins on collision
    const byId = new Map();
    for (const item of cloudVal) {
      if (item && item.id != null) byId.set(item.id, item);
    }
    for (const item of localVal) {
      if (item && item.id != null) byId.set(item.id, item);
    }
    const result = Array.from(byId.values());

    // Preserve order: local items first (in their original order), then any
    // cloud-only items not in local (in their original order). Maintains the
    // existing newest-first convention since localStorage usually has them
    // ordered that way.
    const localIds = new Set(localVal.filter(i => i && i.id != null).map(i => i.id));
    const ordered = [
      ...localVal.filter(i => i && i.id != null),
      ...cloudVal.filter(i => i && i.id != null && !localIds.has(i.id)),
    ];
    return ordered;
  }

  // Same lifecycle as save() but returns a structured result so saveNow
  // callers can distinguish failure modes. save() returns null for several
  // unrelated reasons (not signed in, empty payload, network error,
  // server-rejected) which made debugging "no-response" errors impossible.
  // This wraps each branch with a specific failure tag.
  async function saveDetailed() {
    if (!getUserId()) return { ok: false, error: 'not-signed-in' };
    const payload = await buildMergedPayload();
    if (!payload || !Object.keys(payload).length) {
      return { ok: false, error: 'no-local-data' };
    }
    let r;
    try {
      r = await callApi('save', 'POST', payload);
    } catch (e) {
      return { ok: false, error: 'call-threw: ' + (e.message || e) };
    }
    if (!r) {
      // callApi returned null — already logged the HTTP status or network
      // error to console with the [CygenixSync] prefix. Surface that to UI
      // with enough detail that the user can search the console.
      return { ok: false, error: 'call-failed (check console for [CygenixSync])' };
    }
    if (!r.saved) {
      // Server responded but explicitly didn't save. Surface its reason
      // verbatim if it gave one, so we don't lose the diagnostic detail.
      return { ok: false, error: 'server-rejected: ' + JSON.stringify(r) };
    }
    // Success — mirror cloud back to localStorage so the UI sees any
    // cloud-only records that the merge brought in. Same writeback as save().
    try {
      for (const [field, val] of Object.entries(payload)) {
        const key = FIELD_MAP[field];
        if (key) _orig(key, JSON.stringify(val));
      }
    } catch (e) {
      console.warn('[CygenixSync] post-save localStorage update failed:', e.message);
    }
    console.log('[CygenixSync] Saved to Cosmos DB', r.updatedAt);
    return { ok: true, updatedAt: r.updatedAt };
  }

  async function save() {
    if (!getUserId()) return null;
    const payload = await buildMergedPayload();
    if (!payload || !Object.keys(payload).length) return null;
    const r = await callApi('save','POST',payload);
    if (r?.saved) console.log('[CygenixSync] Saved to Cosmos DB', r.updatedAt);
    // After saving the merged result, sync localStorage with what we just
    // pushed so the user sees the cloud-only records too. Without this, the
    // localStorage stays at its old value until next page load.
    if (r?.saved) {
      try {
        for (const [field, val] of Object.entries(payload)) {
          const key = FIELD_MAP[field];
          if (key) {
            // Use the underlying setItem to avoid re-triggering the auto-save
            // that called us in the first place (would loop).
            _orig(key, JSON.stringify(val));
          }
        }
      } catch (e) {
        console.warn('[CygenixSync] post-save localStorage update failed:', e.message);
      }
    }
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
    try {
      return await saveDetailed();
    } catch (e) {
      return { ok: false, error: 'saveNow-threw: ' + (e.message || String(e)) };
    }
  }

  // Auto-save on localStorage writes. _orig is hoisted to the top of the
  // module so save() can use it too without re-triggering the auto-save.
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

        // Treat empty-array localStorage values as "missing" for gap-fill.
        // Helpers like CygenixWasis initialise their own storage to "[]"
        // before the sync runs, which previously tripped the "local wins"
        // branch and caused cloud data (e.g. wasis rules from another
        // browser session) to never be pulled down. Only the empty-array
        // shape is treated as missing — empty objects, empty strings, and
        // other "empty-ish" values are left alone because they're less
        // common here and broadening the check risks clobbering user data.
        const localRaw = localStorage.getItem(localKey);
        let localIsMissing = localRaw === null;
        if (!localIsMissing) {
          try {
            const parsed = JSON.parse(localRaw);
            if (Array.isArray(parsed) && parsed.length === 0) {
              localIsMissing = true;
            }
          } catch {
            // Unparseable local value — leave it alone, don't overwrite.
          }
        }
        if (!localIsMissing) continue; // local wins

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
