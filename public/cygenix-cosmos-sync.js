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
    // Active project blob (legacy single-project model, still used as the
    // "currently-open project" state across the app). Object, not array, so
    // mergeField's non-array short-circuit makes this local-wins by default
    // — correct for a user-edited working blob. Added 25-May-2026 after we
    // found the active project state was local-only: any machine switch
    // showed jobs without their parent project context.
    'cygenix_conv_project',
    // Project history snapshots (array of recent project blobs). Local-wins
    // via 'replace' strategy below so deletions/trims propagate. Same fix
    // batch as cygenix_conv_project — previously local-only.
    'cygenix_last_snapshots',
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
    // Added 25-May-2026. Cloud field names kept snake_case to match the
    // existing convention (jobs/project_settings/etc.); these are passed
    // verbatim to the Azure Function /api/data/save endpoint which is
    // expected to be field-agnostic. If save succeeds but these don't
    // round-trip back on load, the backend needs a matching schema update.
    conv_project:   'cygenix_conv_project',
    last_snapshots: 'cygenix_last_snapshots',
  };

  // Per-field merge strategy. Two options:
  //
  //   'union'   — union-by-id. Cloud-only items survive a local save (good
  //               for fields where deletion is rare and accidentally losing
  //               cloud data would be costly).
  //   'replace' — local wins entirely. The user's current view IS the truth
  //               at save time, so deletions propagate immediately.
  //
  // Default for unlisted array fields is 'replace'. This is deliberate:
  // 'union' silently swallows deletes (the bug that prompted this map's
  // creation), so making it opt-in means new fields can't regress that
  // way without an explicit declaration here.
  //
  // Non-array fields (config blobs like project_settings) ignore strategy
  // entirely — mergeField short-circuits on `!Array.isArray` and returns
  // local. Their semantics are unchanged.
  const MERGE_STRATEGY = {
    // Long-running migration jobs. Never delete via the cross-device path —
    // a tab that hasn't synced shouldn't wipe a job another tab created.
    jobs: 'union',
    // Everything else is replace by default. Listed explicitly so the
    // intent is auditable; matches default behaviour but documents it.
    validation_sources: 'replace',
    wasis_rules:        'replace',
    saved_connections:  'replace',
    projects:           'replace',
    sql_scripts:        'replace',
    issues:             'replace',
    inventory:          'replace',
    last_snapshots:     'replace',
    // conv_project is an OBJECT not array — mergeField short-circuits on
    // non-arrays and returns local. No strategy needed (would be ignored).
  };
  function strategyFor(field) {
    return MERGE_STRATEGY[field] || 'replace';
  }

  // Extract userId — MSAL-first (authoritative post-migration), with legacy
  // fallbacks for back-compat. Critical that this returns a stable value: the
  // init() flow uses it to decide whether to wipe localStorage as part of the
  // user-switch protection, so instability here can cause data loss. The
  // userId is also used as the Cosmos partition key, so any drift between
  // machines for the same human user causes their data to split across
  // partitions and silently appear empty on one of them.
  //
  // Identity resolution policy (post 25-May-2026 fix):
  //   - Lead with the OIDC `preferred_username` claim from the id_token.
  //     This is the standard OIDC field for the user's principal name (the
  //     email, in our tenant) and is stable across machines and IdPs.
  //   - MSAL's `account.username` field is NOT reliable under Entra External
  //     ID with federated IdPs (e.g. Google SSO). For federated sign-ins,
  //     MSAL frequently populates `username` with the user's object ID in
  //     UPN form: `{oid}@{tenant}.onmicrosoft.com`. That is a stable
  //     identifier but a DIFFERENT STRING from the email — which means the
  //     same user ends up reading/writing different Cosmos partitions
  //     depending on which machine they signed in from. Bug observed on
  //     25-May-2026: account showed `demo@cygenix.onmicrosoft.com` on the
  //     normal machine, `36f15260-…@cygenix.onmicrosoft.com` on a fresh
  //     machine, producing two partitions for one user.
  //   - We therefore explicitly reject anything that looks like the GUID
  //     form (`{8-4-4-4-12 hex}@…`) when falling back to `username`.
  function isGuidUpn(id) {
    // Matches {8}-{4}-{4}-{4}-{12} hex anywhere before the @ — covers the
    // Entra OID-as-UPN case without false-positiving genuine emails.
    return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}@/i.test(id || '');
  }
  function pickEmail(...candidates) {
    for (const raw of candidates) {
      if (!raw) continue;
      const id = String(raw).trim().toLowerCase();
      if (!id) continue;
      if (isGuidUpn(id)) continue;     // reject {oid}@tenant form
      if (!id.includes('@')) continue; // must look like an email/UPN
      return id;
    }
    return null;
  }
  function getUserId() {
    // Method 1: MSAL account cache (authoritative after Entra sign-in).
    //   Order matters: idTokenClaims.preferred_username / email come from
    //   the actual JWT payload and are reliable. account.username is a
    //   last-resort fallback because of the OID-as-UPN behaviour above.
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
          const c = a.idTokenClaims || {};
          const id = pickEmail(
            c.preferred_username,
            c.email,
            c.upn,
            a.username                  // last resort — may be {oid}@tenant
          );
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
        const id = pickEmail(u.email, u.userId);
        if (id) return id;
      }
    } catch {}
    // Method 3: cygenix_user object (Netlify Identity era)
    try {
      const raw = sessionStorage.getItem('cygenix_user') || localStorage.getItem('cygenix_user');
      if (raw) {
        const u = JSON.parse(raw);
        const email = pickEmail(u.email, u.user?.email);
        if (email) return email;
        const at = u.access_token;
        if (at && at.split('.').length === 3) {
          const claims = decodeJwt(at);
          const id = pickEmail(claims?.email, claims?.preferred_username, claims?.sub);
          if (id) return id;
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
        const id = pickEmail(claims?.preferred_username, claims?.email, claims?.upn, claims?.sub);
        if (id) return id;
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
  //   'union'   strategy + id-shape arrays → union by id, local wins on collision
  //   'replace' strategy or non-id arrays  → local wins entirely (deletions propagate)
  //   Non-array values                     → local wins (existing behaviour for blobs)
  function mergeField(field, localVal, cloudVal) {
    if (cloudVal === undefined || cloudVal === null) return localVal;
    if (!Array.isArray(localVal) || !Array.isArray(cloudVal)) return localVal;

    const strategy = strategyFor(field);

    // Replace strategy: local is the truth. This makes deletions work.
    // Note that this is also what we want for non-id-shape arrays — there's
    // no useful way to "union" them.
    if (strategy === 'replace') return localVal;

    // Union strategy from here. Both sides need id-shape, otherwise we
    // can't union — fall through to local-wins with a warning so the next
    // such regression is visible in devtools.
    const isIdArray = arr => arr.length === 0 || (typeof arr[0] === 'object' && arr[0] !== null && 'id' in arr[0]);
    if (!isIdArray(localVal) || !isIdArray(cloudVal)) {
      if (cloudVal.length > 0 && cloudVal.length > localVal.length) {
        console.warn(
          '[CygenixSync] mergeField: "' + field + '" — local (' + localVal.length +
          ' items) overwriting cloud (' + cloudVal.length + ' items). ' +
          'Field declared union-strategy but lacks id-shape; either add ids or switch to replace.'
        );
      }
      return localVal;
    }

    // Union by id, local wins on collision. Order: local items in their
    // original order, then any cloud-only items not in local.
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
    // v1.3 change (26-May-2026): pure local→cloud upload — see save() above
    // for the full rationale. Mirrors save() exactly, but returns a structured
    // result so saveNow() callers can distinguish failure modes.
    const payload = {};
    for (const [cloudField, localKey] of Object.entries(FIELD_MAP)) {
      try {
        const v = localStorage.getItem(localKey);
        if (v !== null) payload[cloudField] = JSON.parse(v);
      } catch (e) {
        console.warn('[CygenixSync] saveDetailed: skipping unparseable', localKey, e.message);
      }
    }
    if (!Object.keys(payload).length) {
      return { ok: false, error: 'no-local-data' };
    }
    let r;
    try {
      r = await callApi('save', 'POST', payload);
    } catch (e) {
      return { ok: false, error: 'call-threw: ' + (e.message || e) };
    }
    if (!r) {
      return { ok: false, error: 'call-failed (check console for [CygenixSync])' };
    }
    if (!r.saved) {
      return { ok: false, error: 'server-rejected: ' + JSON.stringify(r) };
    }
    console.log('[CygenixSync] Saved to Cosmos DB', r.updatedAt);
    return { ok: true, updatedAt: r.updatedAt };
  }

  async function save() {
    if (!getUserId()) return null;
    // v1.3 change (26-May-2026): pure local→cloud upload. No load-then-merge,
    // no writeback to local. Whatever is in local for each FIELD_MAP key gets
    // sent to Cosmos verbatim. Cloud's existing values for those fields are
    // replaced wholesale by the backend's `merged[key] = body[key]` logic.
    //
    // Rationale: yesterday's debugging session (25-May-2026) showed that the
    // load-then-merge-then-writeback flow was the primary source of data
    // pollution across machines. Any stale machine that opened the page would
    // pull cloud, merge its stale local with cloud's correct values, and
    // write the union back — both to Cosmos and to local. The result was a
    // monotonically growing pollution set: every machine's stale data
    // accumulated in Cosmos and propagated to every other machine.
    //
    // The new contract: local is the truth. If you want to delete jobs,
    // delete them locally and the next save will remove them from Cosmos.
    // If you want cloud to be authoritative on a fresh page load, init()
    // now always calls forceLoad() first — see init() below.
    const payload = {};
    for (const [cloudField, localKey] of Object.entries(FIELD_MAP)) {
      try {
        const v = localStorage.getItem(localKey);
        if (v !== null) payload[cloudField] = JSON.parse(v);
      } catch (e) {
        console.warn('[CygenixSync] save: skipping unparseable', localKey, e.message);
      }
    }
    if (!Object.keys(payload).length) return null;
    const r = await callApi('save', 'POST', payload);
    if (r?.saved) console.log('[CygenixSync] Saved to Cosmos DB', r.updatedAt);
    return r;
  }

  async function load() {
    // ── RECOVERY MODE: load disabled ─────────────────────────────────────
    // Remove this block and restore the original load() once jobs have been
    // restored and verified. See dashboard.html recovery mode comment.
    console.warn('[CygenixSync] RECOVERY MODE: load() disabled — Cosmos will not overwrite localStorage');
    return false;
    // ── END RECOVERY MODE ─────────────────────────────────────────────────
  }

  async function forceLoad() {
    // ── RECOVERY MODE: forceLoad disabled ────────────────────────────────
    console.warn('[CygenixSync] RECOVERY MODE: forceLoad() disabled — Cosmos will not overwrite localStorage');
    return false;
    // ── END RECOVERY MODE ─────────────────────────────────────────────────
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

  // ── Per-key on-demand fetch ──────────────────────────────────────────────
  // Pages that need a specific localStorage key to reflect the cloud BEFORE
  // they read it (e.g. Object Mapping opening a job that the Agentive
  // backend just created in Cosmos) call this. It bypasses the gap-fill
  // policy in init() — gap-fill only runs when local is missing/empty,
  // which doesn't catch "local has *some* jobs but not THIS one." This
  // unconditionally fetches cloud, then OVERWRITES the local key for the
  // matching FIELD_MAP entry.
  //
  // Important: this clobbers local-only items in lists — it's "cloud is
  // truth for this key right now." Callers that need merge semantics
  // should use saveNow() (which merges) instead.
  //
  // Returns true if the local key was updated, false otherwise. Never
  // throws — failures log and return false so callers can proceed with
  // whatever they have locally.
  //
  // De-duplicates concurrent calls per key, so two views opening at
  // once don't fire two parallel cloud loads.
  const _ensureKeyInflight = new Map(); // localKey -> Promise<boolean>
  async function ensureKey(localKey) {
    if (typeof localKey !== 'string' || !localKey) return false;
    // Reverse-lookup: which cloud field corresponds to this localStorage key?
    const cloudField = Object.entries(FIELD_MAP).find(([, k]) => k === localKey)?.[0];
    if (!cloudField) {
      console.warn('[CygenixSync] ensureKey: not a sync key:', localKey);
      return false;
    }
    if (_ensureKeyInflight.has(localKey)) return _ensureKeyInflight.get(localKey);

    const p = (async () => {
      try {
        const data = await callApi('load', 'GET');
        if (!data || typeof data !== 'object') return false;
        const cloudVal = data[cloudField];
        if (cloudVal === undefined || cloudVal === null) return false;
        try {
          // Use _orig to avoid re-triggering the auto-save debounce — this is
          // a cloud-to-local hydration, not a user edit, so there's nothing
          // to push back up.
          _orig(localKey, JSON.stringify(cloudVal));
          return true;
        } catch (e) {
          console.warn('[CygenixSync] ensureKey: write failed for', localKey, e.message);
          return false;
        }
      } finally {
        _ensureKeyInflight.delete(localKey);
      }
    })();
    _ensureKeyInflight.set(localKey, p);
    return p;
  }

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

  // ── Local backup/restore helpers ────────────────────────────────────────
  // Console-callable: CygenixSync.exportBackup() / CygenixSync.importBackup(json)
  //
  // Why these exist: every cross-machine sync conversation comes down to
  // "if the cloud writes the wrong thing first, the local state is the only
  // surviving copy". These give the user a one-liner to capture or restore
  // that local state without having to remember the right localStorage
  // incantations under stress.
  //
  // exportBackup() downloads a JSON file containing every cygenix_* key
  // currently in localStorage. Not just SYNC_KEYS — captures everything
  // including local-only diagnostics, prefs, etc.
  //
  // importBackup(jsonOrObject) restores keys from a previously-exported
  // backup. Uses _orig so the auto-save doesn't fire mid-restore. Caller
  // is expected to reload the page afterwards to re-render views. Existing
  // keys not present in the backup are LEFT ALONE — restore is additive,
  // not destructive. If you want to wipe-then-restore, clear localStorage
  // first then import.
  function exportBackup() {
    const dump = {};
    Object.keys(localStorage).filter(k => k.startsWith('cygenix_')).forEach(k => {
      dump[k] = localStorage.getItem(k);
    });
    const meta = {
      _backup_meta: {
        exportedAt: new Date().toISOString(),
        user: localStorage.getItem('cygenix_active_user') || '(unknown)',
        keyCount: Object.keys(dump).length,
        appVersion: 'cygenix-cosmos-sync.js v1.2 (25-May-2026)'
      }
    };
    const payload = JSON.stringify({ ...meta, ...dump }, null, 2);
    try {
      const blob = new Blob([payload], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'cygenix_backup_' + new Date().toISOString().replace(/[:.]/g, '-') + '.json';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (e) {
      console.error('[CygenixSync] exportBackup download failed:', e.message);
      return payload; // return the raw JSON so the user can copy it manually
    }
    console.log('[CygenixSync] Exported', Object.keys(dump).length, 'keys');
    return { ok: true, keys: Object.keys(dump).length };
  }

  function importBackup(input) {
    let obj;
    if (typeof input === 'string') {
      try { obj = JSON.parse(input); }
      catch (e) { console.error('[CygenixSync] importBackup: invalid JSON:', e.message); return { ok: false, error: 'invalid-json' }; }
    } else if (input && typeof input === 'object') {
      obj = input;
    } else {
      console.error('[CygenixSync] importBackup: expected JSON string or object');
      return { ok: false, error: 'bad-input' };
    }
    const keys = Object.keys(obj).filter(k => k.startsWith('cygenix_'));
    if (!keys.length) {
      console.error('[CygenixSync] importBackup: no cygenix_* keys found in backup');
      return { ok: false, error: 'no-keys' };
    }
    let restored = 0;
    for (const k of keys) {
      try {
        const v = obj[k];
        if (typeof v === 'string') { _orig(k, v); restored++; }
        else if (v !== null && v !== undefined) { _orig(k, JSON.stringify(v)); restored++; }
      } catch (e) {
        console.warn('[CygenixSync] importBackup: failed to restore', k, e.message);
      }
    }
    console.log('[CygenixSync] Restored', restored, 'keys. Reload the page to re-render views.');
    return { ok: true, restored };
  }

  // ── Recovery helpers ─────────────────────────────────────────────────────
  // Console-callable: CygenixSync.resetToCloud() / CygenixSync.nuke(opts)
  //
  // These exist for the multi-machine pollution recovery scenario that arose
  // on 25-May-2026. The v1.3 init() rewrite (cloud-authoritative on every
  // page load) prevents new pollution, but a one-off cleanup is needed to
  // remove the accumulated junk from prior sessions.
  //
  // resetToCloud(): wipes all sync-key local storage, then forces a fresh
  //   cloud load. Use this on a machine that has stale local state to bring
  //   it back into agreement with cloud. Equivalent to a hard-refresh under
  //   the v1.3 init contract, but doesn't require a page reload.
  //
  // nuke(opts): WIPES COSMOS for the current user, replacing it with a
  //   minimal clean state. Use ONCE from any single machine to clean up the
  //   user's Cosmos partition. After nuke, every other machine will pick up
  //   the clean state automatically on its next page load.
  //
  //   Required opts:
  //     opts.confirm === 'YES'  — must be literally this string. Guard
  //                                against accidentally calling nuke() from
  //                                muscle memory or copy-paste.
  //
  //   Optional opts:
  //     opts.keepProject = {id, name, client, ...}  — project to keep as
  //                                                    the sole active one.
  //                                                    Defaults to whatever
  //                                                    cygenix_conv_project
  //                                                    currently is locally.
  async function resetToCloud() {
    if (!getUserId()) {
      console.error('[CygenixSync] resetToCloud: not signed in');
      return { ok: false, error: 'not-signed-in' };
    }
    console.log('[CygenixSync] resetToCloud: wiping local sync keys...');
    SYNC_KEYS.forEach(k => localStorage.removeItem(k));
    localStorage.removeItem('cygenix_active_project_id');

    const cloud = await callApi('load', 'GET');
    if (!cloud) {
      console.warn('[CygenixSync] resetToCloud: callApi returned null, local is now empty');
      return { ok: false, error: 'load-failed', local_now_empty: true };
    }
    let n = 0;
    for (const [cloudField, localKey] of Object.entries(FIELD_MAP)) {
      const v = cloud[cloudField];
      if (v !== undefined && v !== null) {
        try { _orig(localKey, JSON.stringify(v)); n++; } catch {}
      }
    }
    console.log('[CygenixSync] resetToCloud: loaded', n, 'keys from cloud');
    try {
      window.dispatchEvent(new CustomEvent('cygenix-sync-loaded', {
        detail: { filled: n, source: 'resetToCloud' }
      }));
    } catch {}
    console.log('[CygenixSync] resetToCloud: done. Reload page or refresh views to see new state.');
    return { ok: true, loaded: n };
  }

  async function nuke(opts) {
    if (!opts || opts.confirm !== 'YES') {
      console.error('[CygenixSync] nuke: requires opts.confirm === "YES". Aborting.');
      console.error('[CygenixSync] Example: CygenixSync.nuke({confirm: "YES"})');
      return { ok: false, error: 'confirmation-required' };
    }
    if (!getUserId()) {
      console.error('[CygenixSync] nuke: not signed in');
      return { ok: false, error: 'not-signed-in' };
    }

    // Determine which project to keep. Default: current local conv_project.
    let keepProject = opts.keepProject;
    if (!keepProject) {
      try {
        const local = JSON.parse(localStorage.getItem('cygenix_conv_project') || '{}');
        if (local && local.id && local.name) keepProject = local;
      } catch {}
    }
    if (!keepProject || !keepProject.id || !keepProject.name) {
      console.error('[CygenixSync] nuke: no usable project to keep. Pass opts.keepProject = {id, name, client, ...}.');
      return { ok: false, error: 'no-keep-project' };
    }

    const now = new Date().toISOString();
    const projectRecord = {
      id:           keepProject.id,
      name:         keepProject.name,
      client:       keepProject.client      || '',
      ref:          keepProject.ref         || '',
      analyst:      keepProject.analyst     || '',
      pm:           keepProject.pm          || '',
      contact:      keepProject.contact     || '',
      description:  keepProject.description || '',
      type:         keepProject.type        || 'other',
      srcSystem:    keepProject.srcSystem   || '',
      tgtSystem:    keepProject.tgtSystem   || '',
      phase:        keepProject.phase       || 'active',
      status:       keepProject.status      || 'active',
      start:        keepProject.start       || now.slice(0,10),
      end:          keepProject.end         || '',
      rows:         keepProject.rows        || '',
      notes:        keepProject.notes       || '',
      statusManual: true,
      created:      keepProject.created     || now,
      modified:     now,
      dbHistory:    keepProject.dbHistory   || [],
      groups:       keepProject.groups      || [],
    };
    const projectsList = [{
      id:       projectRecord.id,
      name:     projectRecord.name,
      client:   projectRecord.client,
      status:   projectRecord.status,
      created:  projectRecord.created,
      modified: projectRecord.modified,
    }];

    console.log('[CygenixSync] nuke: wiping Cosmos for user', getUserId());
    console.log('[CygenixSync] nuke: keeping project', projectRecord.id, projectRecord.name);

    // Build the clean payload — every SYNCABLE field reset to empty/clean.
    const cleanPayload = {
      jobs:               [],
      project_settings:   {},
      project_plan:       {},
      connections:        {},
      saved_connections:  [],
      performance:        {},
      validation_sources: [],
      wasis_rules:        [],
      sql_scripts:        [],
      issues:             [],
      inventory:          {},
      sys_params:         {},
      projects:           projectsList,
      conv_project:       projectRecord,
      last_snapshots:     {},
    };

    const r = await callApi('save', 'POST', cleanPayload);
    if (!r || !r.saved) {
      console.error('[CygenixSync] nuke: save failed:', r);
      return { ok: false, error: 'save-failed', response: r };
    }
    console.log('[CygenixSync] nuke: Cosmos wiped clean. Saved at', r.updatedAt);

    // Also reset local on this machine so the UI updates immediately.
    for (const [cloudField, localKey] of Object.entries(FIELD_MAP)) {
      const v = cleanPayload[cloudField];
      if (v !== undefined) {
        try { _orig(localKey, JSON.stringify(v)); } catch {}
      }
    }
    _orig('cygenix_active_project_id', projectRecord.id);

    try {
      window.dispatchEvent(new CustomEvent('cygenix-sync-loaded', {
        detail: { filled: Object.keys(cleanPayload).length, source: 'nuke' }
      }));
    } catch {}

    console.log('[CygenixSync] nuke: done. Reload page to fully re-render. Other machines will catch up on next page load.');
    return { ok: true, updatedAt: r.updatedAt, project: projectRecord };
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
    // If a different user signs in on this machine, snapshot the old user's
    // data to sessionStorage (in-tab recovery only) and clear local sync
    // keys before loading the new user's data from cloud.
    //
    // Normalise both sides — a casing or whitespace mismatch here was
    // previously enough to trigger a full local wipe.
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
    // v1.3 change (26-May-2026): cloud-authoritative init. Always pull the
    // full cloud state for sync keys and overwrite local. No gap-fill, no
    // "local wins" — local is for in-session edits only.
    //
    // Why: yesterday's session showed that any stale machine opening Cygenix
    // would re-pollute Cosmos. The old gap-fill logic ("if local has data,
    // skip cloud") meant stale local survived page loads, and the post-init
    // auto-save then merged that stale local into Cosmos, polluting every
    // other machine. New rule: cloud wins on page load. Period.
    //
    // Trade-off: a user who edits locally in tab A, doesn't wait for the
    // debounced save, and immediately hard-refreshes tab A will lose the
    // unsaved edit (cloud will overwrite). That window is ~3 seconds and
    // is an acceptable cost to stop the multi-machine pollution.
    const cloud = await callApi('load', 'GET');
    let n = 0;
    if (cloud && typeof cloud === 'object') {
      for (const [cloudField, localKey] of Object.entries(FIELD_MAP)) {
        const cloudVal = cloud[cloudField];
        if (cloudVal === undefined || cloudVal === null) {
          // Cloud has nothing for this field — also clear local, otherwise
          // a stale local value sits forever. The exception is during
          // first-ever sign-in when cloud is empty across the board; in
          // that case we'd be clearing legitimately local-only data. We
          // detect that via Object.keys(cloud).length: if cloud is wholly
          // empty (no fields at all), don't clear anything.
          if (Object.keys(cloud).length === 0) continue;
          try { _orig(localKey, JSON.stringify(Array.isArray(cloud[cloudField]) ? [] : null)); } catch {}
          continue;
        }
        try {
          // Use _orig to avoid re-triggering the auto-save debounce — we
          // don't want page load to schedule a save of cloud-just-written
          // data back to the cloud.
          _orig(localKey, JSON.stringify(cloudVal));
          n++;
        } catch (e) {
          console.warn('[CygenixSync] init: failed to load', localKey, e.message);
        }
      }
    }
    console.log('[CygenixSync] Loaded', n, 'keys from Cosmos DB (cloud-authoritative)');

    // Notify views that they should re-read localStorage. The dispatched
    // event is the same one used by the legacy gap-fill path so any handler
    // listening for it keeps working.
    try {
      window.dispatchEvent(new CustomEvent('cygenix-sync-loaded', {
        detail: { filled: n, source: 'init-v1.3' }
      }));
    } catch {}

    // v1.3 change: NO post-init save kick. Saves only fire from genuine
    // user edits via the localStorage.setItem monkey-patch. This eliminates
    // the "page load polluted Cosmos" failure mode.
  }

  // Start after a short delay to let auth complete
  setTimeout(init, 800);

  return {
    init, save, saveNow, load, forceLoad, ensureKey, ensureUser, ping, getSubscription, getUserId,
    // Console-callable backup/restore helpers — see definitions above.
    exportBackup, importBackup,
    // v1.3 recovery helpers — see definitions above.
    resetToCloud, nuke,
    // Exposed for other modules (e.g. cygenix-project-summary.js) that need to
    // call the Function with the same auth as the rest of the dashboard.
    // Keep this the SINGLE source of truth — never duplicate the function key
    // into another file or into localStorage. If it ever needs rotating, the
    // change happens here and propagates to every consumer automatically.
    apiBase:  API_BASE,
    funcCode: FUNC_CODE,
  };
})();

// Expose to window so other modules can read CygenixSync without depending
// on script-tag ordering. `const` at module scope does NOT auto-attach to
// window, so we do it explicitly here.
if (typeof window !== 'undefined') window.CygenixSync = CygenixSync;
