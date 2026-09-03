/**
 * cygenix-cosmos-sync.js  v1.1
 * Syncs Cygenix localStorage to/from Azure Cosmos DB.
 * Auto-injected via nav.js on every page.
 */
const CygenixSync = (() => {

  // The Function App host key used to live here, as a literal, in a file
  // served to every visitor. Combined with the Function App reading identity
  // straight off an `x-user-id` request header, that made every user's data
  // readable and writable by anyone who viewed source.
  //
  // Calls now go through /.netlify/functions/data-proxy, which verifies the
  // caller's Entra token and derives the identity from the verified claims.
  // The key lives in Netlify's environment and never reaches the browser.
  //
  // API_BASE stays only because other modules read it to build display URLs;
  // nothing authenticates with it any more.
  const API_BASE  = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/data';

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
  // Memoised — identity only changes on sign-in/out (which reloads the page),
  // and this is called from the localStorage.setItem monkey-patch on every
  // sync-key write. The uncached version constructed a fresh
  // msal.PublicClientApplication (a full MSAL cache parse) per call.
  let _userIdCache = null;
  function getUserId() {
    if (_userIdCache) return _userIdCache;
    _userIdCache = _resolveUserId();
    return _userIdCache;
  }
  function _resolveUserId() {
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

  // One shared GET /load per page-boot window. init() and ensureKey() both
  // fetch the WHOLE user blob; on an object_mapping ?edit= load they used to
  // download it twice, and the editor awaited the second copy. A 5s TTL is
  // enough to cover a boot without ever serving genuinely stale data to a
  // user-triggered refresh later in the session.
  let _loadShared = null;        // { at, promise }  — promise of a RESULT

  /** The shared load, as a result. A failed load is never cached: retrying
   *  after a 503 must actually retry. */
  function sharedLoadResult() {
    const now = Date.now();
    if (_loadShared && now - _loadShared.at < 5000) return _loadShared.promise;
    const promise = callApiResult('load', 'GET');
    _loadShared = { at: now, promise };
    promise.then((r) => { if (!r.ok) _loadShared = null; },
                 ()  => { _loadShared = null; });
    return promise;
  }

  /** Load the whole user blob, as a result. Every path that overwrites or
   *  clears local storage goes through this rather than callApi, so it can
   *  refuse to act on an answer that never came. */
  function loadResult() { return sharedLoadResult(); }

  async function callApi(action, method, body) {
    if (action === 'load' && (method || 'GET') === 'GET' && !body) {
      const r = await sharedLoadResult();
      return r.ok ? r.data : null;
    }
    return callApiRaw(action, method, body);
  }

  /* ── Health ────────────────────────────────────────────────────────────────
     What the last call to the data layer actually did. This exists because
     for three weeks it did nothing at all and no surface said so: every save
     returned null, every load returned null, and the only trace was a
     console.warn nobody was reading. A user kept working, and none of it was
     kept.

     `verified` is the important field. It is true only when the proxy
     answered — not when it 503'd, not when the token expired. Nothing in this
     module may overwrite or clear local data on the strength of an
     unverified response. */
  const _health = {
    verified: false,        // did the last load come back from a real answer
    degraded: false,        // is the data layer currently not working
    reason: '',             // 'config' | 'auth' | 'network' | 'server' | …
    lastError: '',          // human-readable, never carries a credential
    pendingSaves: 0,        // edits made but not yet accepted by the cloud
    lastSaveAt: null,
    lastLoadAt: null,
  };

  function setHealth(patch) {
    const before = _health.degraded + '|' + _health.reason + '|' + _health.pendingSaves;
    Object.assign(_health, patch);
    const after = _health.degraded + '|' + _health.reason + '|' + _health.pendingSaves;
    if (before === after) return;
    // The banner listens for this. Anything else that wants to show sync
    // state can too — that is why it is an event and not a direct call into
    // a UI module this file should not know about.
    try {
      window.dispatchEvent(new CustomEvent('cygenix-sync-health', { detail: getHealth() }));
    } catch {}
  }

  function getHealth() { return Object.assign({}, _health); }

  /**
   * The call every other function here goes through, as a result.
   *
   *   { ok: true, data }  the proxy answered; data may be legitimately empty
   *   { ok: false, … }    it did not; nothing may be concluded from this
   */
  async function callApiResult(action, method, body) {
    // No userId and no key are sent. The proxy establishes both from the
    // verified token — anything this file asserted about identity would be
    // exactly the header the Function App used to trust.
    if (!window.CygenixDataApi || !window.CygenixDataApi.callResult) {
      console.warn('[CygenixSync] cygenix-data-api.js is not loaded; sync is disabled on this page.');
      return { ok: false, code: 'no-data-api', retryable: false, message: 'data api not loaded' };
    }
    const r = await window.CygenixDataApi.callResult(action, { method: method || 'GET', body: body });
    if (r.ok) {
      setHealth({ degraded: false, reason: '', lastError: '' });
      return { ok: true, data: r.data };
    }
    // 'no-token' is not a fault: it is a signed-out page, and this module
    // already declines to do anything without an identity.
    if (r.error.code !== 'no-token') {
      setHealth({
        degraded: true,
        reason: r.error.code,
        // The message carries a status and an action, never a key: the proxy
        // does not echo one and this does not construct one.
        lastError: r.error.message,
      });
      console.error('[CygenixSync]', action, 'failed:', r.error.code, r.error.message);
    }
    return { ok: false, code: r.error.code, retryable: r.error.retryable, message: r.error.message };
  }

  async function callApiRaw(action, method, body) {
    // The payload-or-null shape the older internal callers were written
    // against. Kept so those call sites are unchanged, but everything that
    // WRITES or CLEARS local data now uses callApiResult and checks `ok` —
    // null here still cannot be told apart from a verified empty answer,
    // which is exactly the confusion that lost three weeks of saves.
    const r = await callApiResult(action, method, body);
    return r.ok ? r.data : null;
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
    const r = await callApiResult('save', 'POST', payload);
    if (!r.ok) {
      // The code is the useful part: 'config' means somebody has to set an
      // environment variable, 'auth' means sign in again, 'network' means
      // try later. "call-failed (check console)" told a user none of that.
      return { ok: false, error: r.message, code: r.code, retryable: r.retryable };
    }
    if (!r.data || !r.data.saved) {
      return { ok: false, error: 'server-rejected: ' + JSON.stringify(r.data), code: 'rejected' };
    }
    console.log('[CygenixSync] Saved to Cosmos DB', r.data.updatedAt);
    _dirtyKeys.clear();
    _saveFailures = 0;
    _health.lastSaveAt = new Date().toISOString();
    setHealth({ pendingSaves: 0, degraded: false, reason: '', lastError: '' });
    return { ok: true, updatedAt: r.data.updatedAt };
  }

  // localStorage keys written since the last successful flush.
  const _dirtyKeys = new Set();

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
    // Only the keys that actually changed since the last flush. The server
    // overwrites per present field ("merged[key] = body[key]"), so a partial
    // payload is safe — and a one-key edit stops re-serialising and shipping
    // all fifteen fields (jobs with generated SQL, snapshots, inventory)
    // every three seconds while someone types. An empty dirty set — a
    // direct save() call from the console or a restore helper — falls back
    // to pushing everything, which was the old contract.
    const dirty = _dirtyKeys.size ? new Set(_dirtyKeys) : null;
    const payload = {};
    for (const [cloudField, localKey] of Object.entries(FIELD_MAP)) {
      if (dirty && !dirty.has(localKey)) continue;
      try {
        const v = localStorage.getItem(localKey);
        if (v !== null) payload[cloudField] = JSON.parse(v);
      } catch (e) {
        console.warn('[CygenixSync] save: skipping unparseable', localKey, e.message);
      }
    }
    if (!Object.keys(payload).length) { if (dirty) dirty.forEach(k => _dirtyKeys.delete(k)); return null; }
    const r = await callApiResult('save', 'POST', payload);
    if (r.ok && r.data && r.data.saved) {
      console.log('[CygenixSync] Saved to Cosmos DB', r.data.updatedAt,
        dirty ? '(' + Object.keys(payload).length + ' changed field(s))' : '(full)');
      // Clear only what this flush carried — keys dirtied while the POST
      // was in flight stay marked for the next one.
      if (dirty) dirty.forEach(k => _dirtyKeys.delete(k));
      else _dirtyKeys.clear();
      _saveFailures = 0;
      _health.lastSaveAt = new Date().toISOString();
      setHealth({ pendingSaves: _dirtyKeys.size, degraded: false, reason: '', lastError: '' });
      return r.data;
    }

    /* The save did not land.
       This is the failure that cost three weeks. It used to return null here
       and nothing else happened: no retry, no record, no surface. The keys
       stayed dirty — which was correct — but the only thing that would ever
       flush them again was the user happening to make another edit, and that
       flush hit the same 503. Every edit in between was kept nowhere but this
       browser, and the user had no way to know.

       Now the dirty keys stay marked AND a retry is scheduled AND the pending
       count is published, so the banner can say how many edits are unsaved. */
    _saveFailures++;
    setHealth({ pendingSaves: _dirtyKeys.size });
    if (r.ok) {
      // A verified answer that refused the write — a server-side rejection,
      // not a transport fault. Retrying the identical payload will not help.
      console.error('[CygenixSync] save rejected by the server:', JSON.stringify(r.data));
      setHealth({ degraded: true, reason: 'rejected',
        lastError: 'The server refused the save.' });
    } else if (r.retryable !== false) {
      scheduleRetry();
    }
    return r.ok ? r.data : null;
  }

  /* Retry with backoff, capped. A misconfigured deployment is not fixed by
     hammering it, so this backs off to a couple of minutes and stays there
     until either a save succeeds or the page goes away. The point is not to
     out-wait the outage; it is that when someone sets the missing variable,
     the queued edits go up on their own rather than needing a lucky keystroke. */
  let _saveFailures = 0;
  let _retryTimer = null;
  const RETRY_STEPS = [5000, 15000, 45000, 120000];

  function scheduleRetry() {
    if (_retryTimer || !_dirtyKeys.size) return;
    const wait = RETRY_STEPS[Math.min(_saveFailures - 1, RETRY_STEPS.length - 1)] || RETRY_STEPS[0];
    console.warn('[CygenixSync]', _dirtyKeys.size, 'unsaved change(s); retrying in', Math.round(wait / 1000) + 's');
    _retryTimer = setTimeout(() => { _retryTimer = null; save(); }, wait);
  }

  /** Flush the queue now — what the banner's Retry button calls. */
  async function retryPending() {
    if (_retryTimer) { clearTimeout(_retryTimer); _retryTimer = null; }
    _saveFailures = 0;
    return saveNow();
  }

  /* ── The one place cloud state becomes local state ─────────────────────────

     Everything that writes localStorage from a cloud response goes through
     here. There used to be four such places — load(), forceLoad(), init() and
     resetToCloud() — each with slightly different rules about when to clear a
     key, and only one of them (init) had any guard at all.

     Two rules, and both were learned the hard way.

     RULE 1: AN OMITTED FIELD IS NOT A DELETION.
     init() used to call localStorage.removeItem() for every FIELD_MAP key the
     cloud response did not carry, guarded only against a WHOLLY empty object.
     A partial response therefore deleted local data for every field it left
     out. That is not hypothetical: FIELD_MAP's own note on conv_project and
     last_snapshots says these may not round-trip until the backend schema
     catches up — and the code deleted local state for exactly the fields that
     did not come back.

     A genuine deletion is not an omission. save() serialises whatever is in
     localStorage, so a user who deletes all their jobs sends `jobs: []` — a
     present, empty value. Absence means the field never made the round trip.
     So: a present value is applied, an absent one leaves local alone.

     RULE 2: NOTHING WIPES EVERYTHING AT ONCE.
     A verified response that would empty every key that currently holds real
     data is a backend fault, not fifteen simultaneous deletions. Per-field
     emptying is still allowed — that IS how a deletion propagates — but not
     all of it at once. When this trips, local is kept and the health state
     goes degraded so the banner says so rather than the user finding out by
     looking at an empty screen.

     @param cloud   a VERIFIED response body. Callers must have checked ok
                    first; there is no path here that can be reached with the
                    null from a failed call.
     @returns { applied, skipped, refused }
  */
  function isMeaningful(raw) {
    if (raw === null || raw === undefined) return false;
    const s = String(raw).trim();
    return s !== '' && s !== '[]' && s !== '{}' && s !== 'null' && s !== '""';
  }

  function applyCloud(cloud, source) {
    if (!cloud || typeof cloud !== 'object') return { applied: 0, skipped: 0, refused: false };

    const present = [];   // [cloudField, localKey, value]
    const absent  = [];
    for (const [cloudField, localKey] of Object.entries(FIELD_MAP)) {
      const v = cloud[cloudField];
      if (v === undefined || v === null) absent.push(localKey);
      else present.push([cloudField, localKey, v]);
    }

    // Rule 2. Would this leave every key that currently holds something
    // meaningful holding nothing?
    const localMeaningful = Object.values(FIELD_MAP).filter((k) => {
      try { return isMeaningful(localStorage.getItem(k)); } catch { return false; }
    });
    if (localMeaningful.length) {
      const survives = localMeaningful.some((k) => {
        const hit = present.find(([, lk]) => lk === k);
        // Absent → local is left alone, so it survives. Present → it survives
        // only if what is arriving is itself meaningful.
        return hit ? isMeaningful(JSON.stringify(hit[2])) : true;
      });
      if (!survives) {
        console.error('[CygenixSync] applyCloud(' + source + '): refusing — the response would '
          + 'empty all ' + localMeaningful.length + ' populated keys at once. Keeping local data.');
        setHealth({ degraded: true, reason: 'suspect-empty',
          lastError: 'Cloud returned an empty state for every key while this device holds data. '
            + 'Local data kept.' });
        return { applied: 0, skipped: 0, refused: true };
      }
    }

    let applied = 0;
    for (const [, localKey, value] of present) {
      try {
        // _orig, not localStorage.setItem: this is cloud-to-local hydration,
        // not a user edit, and must not schedule a save of what we just read.
        _orig(localKey, JSON.stringify(value));
        applied++;
      } catch (e) {
        console.warn('[CygenixSync] applyCloud: failed to write', localKey, e.message);
      }
    }
    if (absent.length) {
      console.log('[CygenixSync] applyCloud(' + source + '):', absent.length,
        'field(s) absent from the response — local left untouched:', absent.join(', '));
    }
    _health.lastLoadAt = new Date().toISOString();
    setHealth({ verified: true });
    try {
      window.dispatchEvent(new CustomEvent('cygenix-sync-loaded', {
        detail: { filled: applied, source: source }
      }));
    } catch {}
    return { applied: applied, skipped: absent.length, refused: false };
  }

  // load() and forceLoad() keep their boolean contract — roughly forty
  // scripts call them and several branch on the result. The structured
  // version is loadDetailed(); it was added alongside rather than by
  // repurposing these.
  async function load() {
    const d = await loadDetailed();
    return d.ok && d.applied > 0;
  }

  async function forceLoad() {
    const d = await loadDetailed({ force: true });
    return d.ok && d.applied > 0;
  }

  /**
   * Load, with the reason when it does not work.
   *
   *   { ok:true,  verified:true,  applied, skipped }
   *   { ok:false, verified:false, code, message }   — local data untouched
   */
  async function loadDetailed(opts) {
    if (opts && opts.force) _loadShared = null;
    const r = await loadResult();
    if (!r.ok) {
      console.warn('[CygenixSync] load failed (' + r.code + ') — local data left as it is');
      return { ok: false, verified: false, code: r.code, message: r.message, applied: 0 };
    }
    const cloud = r.data;
    if (!cloud || !Object.keys(cloud).length) {
      // Verified and empty. A real fact — this account has nothing stored —
      // but not a licence to clear anything: on a first sign-in the local
      // data is the only copy there is.
      console.log('[CygenixSync] Cloud is empty for this account (verified); local data kept');
      _health.lastLoadAt = new Date().toISOString();
      setHealth({ verified: true, degraded: false, reason: '' });
      return { ok: true, verified: true, empty: true, applied: 0, skipped: 0 };
    }
    const res = applyCloud(cloud, (opts && opts.source) || 'load');
    console.log('[CygenixSync] Loaded', res.applied, 'keys from Cosmos DB');
    return { ok: true, verified: true, applied: res.applied, skipped: res.skipped, refused: res.refused };
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
    // FETCH FIRST, WIPE SECOND. This used to be the other way round: it
    // removed every sync key and then called the API, so a failed load left
    // the machine with nothing and said so by returning
    // `{local_now_empty: true}` — a recovery helper that destroyed the only
    // surviving copy of the data whenever the thing it was recovering from
    // was still broken. The order is the entire fix.
    _loadShared = null;                       // a genuine refresh, not a boot
    const r = await loadResult();
    if (!r.ok) {
      console.warn('[CygenixSync] resetToCloud: load failed (' + r.code + ') — local data untouched');
      return { ok: false, error: 'load-failed', code: r.code, local_untouched: true };
    }
    const cloud = r.data;
    if (!cloud || !Object.keys(cloud).length) {
      console.warn('[CygenixSync] resetToCloud: cloud is empty for this account — '
        + 'refusing to wipe local, since that would leave no copy anywhere. '
        + 'Use CygenixSync.exportBackup() first if you intend to start clean.');
      return { ok: false, error: 'cloud-empty', local_untouched: true };
    }

    console.log('[CygenixSync] resetToCloud: cloud verified, wiping local sync keys...');
    SYNC_KEYS.forEach(k => localStorage.removeItem(k));
    localStorage.removeItem('cygenix_active_project_id');

    const res = applyCloud(cloud, 'resetToCloud');
    console.log('[CygenixSync] resetToCloud: loaded', res.applied, 'keys from cloud');
    console.log('[CygenixSync] resetToCloud: done. Reload page or refresh views to see new state.');
    return { ok: true, loaded: res.applied };
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

    // READ THE CLOUD BEFORE OVERWRITING IT. nuke() pushes a near-empty
    // document over the user's entire Cosmos partition. It used to do that
    // without ever looking at what was there — so running it while the data
    // layer was broken (the case where someone is most likely to reach for a
    // recovery helper) destroyed the cloud copy on the strength of a local
    // state that may itself have been empty for the same reason.
    const probe = await callApiResult('load', 'GET');
    if (!probe.ok) {
      console.error('[CygenixSync] nuke: cannot read the cloud (' + probe.code + '). '
        + 'Refusing to overwrite a partition whose contents are unknown.');
      return { ok: false, error: 'cloud-unreadable', code: probe.code };
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
      _dirtyKeys.add(k);
      // Publish immediately, so "3 unsaved changes" is true from the moment
      // the edit is made rather than from the moment a save fails.
      setHealth({ pendingSaves: _dirtyKeys.size });
      if (_saveTimer) clearTimeout(_saveTimer);
      _saveTimer = setTimeout(save, 3000);
    }
  };

  // Init with retry — waits until user is logged in. The retry starts at
  // 100ms and backs off: getUserId() reads storage synchronously, so when
  // identity is already there (the common case) the first tick finds it and
  // cloud data starts loading immediately instead of after a fixed delay.
  let _done = false, _retries = 0;
  async function init() {
    if (_done) return;
    const userId = getUserId();
    if (!userId) {
      if (_retries++ < 24) setTimeout(init, Math.min(100 * 2 ** Math.min(_retries, 4), 1600));
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

    // ensureUser writes the user record; the load reads the data blob. They
    // are independent, and each can be seconds on an Azure cold start, so
    // paying them serially doubled the wait for first data. Start both now;
    // the load's result is picked up below.
    const _loadP = loadResult();
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
    // v1.4 change (3-Sep-2026): the apply is guarded, and it is the same
    // guarded apply every other path uses. What used to be here walked
    // FIELD_MAP and called localStorage.removeItem() for any field the
    // response did not carry — guarded only against a response with NO
    // fields at all. A partial response therefore deleted local data for
    // every field it omitted, and FIELD_MAP's own note says two fields may
    // not round-trip until the backend schema catches up. See applyCloud()
    // for why an omission is now never read as a deletion.
    const r = await _loadP;
    if (!r.ok) {
      // Unverified. Local is the only copy of anything unsaved, so it is
      // kept exactly as it is, and the health state says why — this is the
      // state that lasted three weeks in silence.
      console.warn('[CygenixSync] init: cloud unavailable (' + r.code + ') — '
        + 'working from local data only. Nothing has been cleared.');
      setHealth({ verified: false });
    } else if (!r.data || !Object.keys(r.data).length) {
      console.log('[CygenixSync] init: cloud is empty for this account (verified)');
      _health.lastLoadAt = new Date().toISOString();
      setHealth({ verified: true, degraded: false, reason: '' });
    } else {
      const res = applyCloud(r.data, 'init-v1.4');
      console.log('[CygenixSync] Loaded', res.applied, 'keys from Cosmos DB (cloud-authoritative)');
    }

    // Anything already queued from a previous page — the dirty set survives
    // in localStorage terms because the edits do — gets a flush attempt now.
    if (_dirtyKeys.size) scheduleRetry();

    // v1.3 change: NO post-init save kick. Saves only fire from genuine
    // user edits via the localStorage.setItem monkey-patch. This eliminates
    // the "page load polluted Cosmos" failure mode.
  }

  // Start immediately: init() polls for identity with backoff, so there is
  // nothing a fixed head-start delay buys — it was 800ms of dead time on
  // every page for the signed-in common case.
  init();

  return {
    init, save, saveNow, load, forceLoad, ensureKey, ensureUser, ping, getSubscription, getUserId,
    // Structured counterparts. Added ALONGSIDE load()/forceLoad() rather
    // than by changing what those return: roughly forty scripts call them
    // and branch on a boolean.
    loadDetailed, getHealth, retryPending,
    // Console-callable backup/restore helpers — see definitions above.
    exportBackup, importBackup,
    // v1.3 recovery helpers — see definitions above.
    resetToCloud, nuke,
    // The Function host, for modules that build a display URL. There is no
    // longer a `funcCode` to expose: the key is held by the Netlify proxy,
    // and any module that needs to CALL the API uses CygenixDataApi rather
    // than assembling a URL with a key in it.
    apiBase:  API_BASE,
  };
})();

// Expose to window so other modules can read CygenixSync without depending
// on script-tag ordering. `const` at module scope does NOT auto-attach to
// window, so we do it explicitly here.
if (typeof window !== 'undefined') window.CygenixSync = CygenixSync;
