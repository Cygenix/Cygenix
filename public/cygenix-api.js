/* cygenix-api.js
 * ----------------
 * Shared helper for talking to the Cygenix Azure Function backend
 * (cygenix-db-api on Azure App Service in UK South). Used by both
 * dashboard.html and object_mapping.html for version history calls,
 * and is the right place to add any future site-wide /api/data/*
 * action call.
 *
 * IMPORTANT CONTEXT — read this before changing endpoint resolution:
 *
 *   The Cygenix backend (Cosmos sync, version history, schedules, the
 *   /api/data/{action} dispatcher) is a FIXED endpoint baked into the
 *   deployment. It is NOT user-configurable. cygenix-cosmos-sync.js
 *   has the URL and function key hardcoded as constants on every page;
 *   we mirror those constants here.
 *
 *   This is DELIBERATE and separate from "Dashboard → Connections →
 *   Target", which configures the user's own SQL warehouse (the
 *   destination database that conversion jobs read from and write to).
 *   That's their data; this is Cygenix infrastructure.
 *
 *   An earlier version of this file tried to read the API URL from the
 *   Connections panel. That was wrong — Connections has nothing to do
 *   with Cygenix's own backend, and any user whose Target wasn't an
 *   Azure Function (which is most of them, since SQL Server is more
 *   common) would see "Azure Function not configured" forever. Do not
 *   reintroduce that lookup.
 *
 *   When Cygenix moves the function key out of static JS (planned
 *   security fix — see chat 2538d8bf about cygenix-cosmos-sync.js
 *   line 9), update both cygenix-cosmos-sync.js and the resolve()
 *   below together so they stay in sync.
 */
(function () {
  'use strict';

  // ── Cygenix backend endpoint (matches cygenix-cosmos-sync.js) ─────────────
  // Hardcoded by design — see header comment.
  const CYGENIX_API_BASE = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api';
  const CYGENIX_API_CODE = 'WjSmoWxgtNdGnO_I5nKIspRUQqKCR1knsXgVmJr3dyYuAzFu-or-5Q==';

  // ── Identity ──────────────────────────────────────────────────────────────
  // Mirrors currentCygenixEmail() in dashboard.html. Reads the Entra account
  // object from sessionStorage (preferred) or localStorage, then extracts the
  // email/userId. Lowercased and trimmed so it matches how the backend's
  // x-user-id header dispatch and the audit container key things.
  function getEmail() {
    try {
      const rawAcc = sessionStorage.getItem('cygenix_entra_account')
                  || localStorage.getItem('cygenix_entra_account');
      if (!rawAcc) return '';
      const acc = JSON.parse(rawAcc);
      return ((acc.email || acc.userId) || '').toLowerCase().trim();
    } catch { return ''; }
  }

  // ── Endpoint resolution ───────────────────────────────────────────────────
  // Returns { base, key }. Prefers a runtime override from CygenixSync if
  // that module ever exposes one (forward compatibility for the planned
  // server-side-key fix); otherwise returns the hardcoded constants.
  //
  // We intentionally do NOT throw — the endpoint is always available
  // because it's compiled in. The only way this can fail is if both
  // CYGENIX_API_BASE and CYGENIX_API_CODE above are blanked out, in which
  // case throwing is correct.
  function resolveApi() {
    try {
      if (window.CygenixSync && typeof window.CygenixSync.getApiConfig === 'function') {
        const cfg = window.CygenixSync.getApiConfig();
        if (cfg && cfg.base && cfg.key) return { base: cfg.base, key: cfg.key };
      }
    } catch { /* fall through to constants */ }

    if (!CYGENIX_API_BASE || !CYGENIX_API_CODE) {
      throw new Error('Cygenix API constants are missing. This is a build/deploy issue, not a user-configurable setting.');
    }
    return { base: CYGENIX_API_BASE, key: CYGENIX_API_CODE };
  }

  // ── Generic call ──────────────────────────────────────────────────────────
  /**
   * Make a call into the Cygenix function app.
   * @param {string} path   - e.g. 'data/version-create' or 'data/version-list?jobId=X'
   * @param {object|null} body  - JSON body for non-GET; ignored on GET
   * @param {'GET'|'POST'|'PUT'|'DELETE'} [method='POST']
   * @returns {Promise<object>}  - parsed JSON response
   */
  async function call(path, body, method) {
    const email = getEmail();
    if (!email) throw new Error('Please sign in first.');
    const { base, key } = resolveApi();

    // Use & if the path already has a ? (e.g. 'data/version-list?jobId=X'),
    // otherwise use ? to introduce the query string. Without this guard a
    // GET with query params builds a malformed URL like '?jobId=X?code=Y'
    // and Azure rejects it 401 because the function key never gets parsed.
    const sep = path.includes('?') ? '&' : '?';
    const url = `${base}/${path}${sep}code=${encodeURIComponent(key)}`;

    const opts = {
      method: method || 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-user-id':    email,
      },
    };
    if (body && opts.method !== 'GET') opts.body = JSON.stringify(body);

    const r = await fetch(url, opts);
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || ('HTTP ' + r.status));
    return data;
  }

  // ── Version control wrappers ──────────────────────────────────────────────
  // These mirror the handlers in azure-function/src/index.js around
  // line 1099 (version-create), 1159 (version-list), 1174 (version-get).

  /**
   * Snapshot a job. Backend dedupes by content hash, so calling on every
   * save is safe — duplicate snapshots return { created: false }.
   *
   * Backend response shapes:
   *   created → { created: true,  version: <int>, id: 'ver_<jobId>_<ts>' }
   *   dup     → { created: false, reason: 'duplicate', version: <int> }
   */
  function versionCreate(jobId, snapshot, label, note) {
    return call('data/version-create', {
      jobId,
      snapshot,
      label: label || 'manual',
      note:  note  || ''
    });
  }

  /**
   * List all versions for a job, newest first. Backend returns lightweight
   * summaries (no snapshot blob) — call versionGet for the full payload.
   *
   * Backend response shape: { versions: [...] }
   */
  function versionList(jobId) {
    return call('data/version-list?jobId=' + encodeURIComponent(jobId), null, 'GET');
  }

  /**
   * Fetch a single version document including the full snapshot.
   *
   * NOTE: the backend handler reads `id` from the query string, not
   * `versionId`. The version doc's id field follows the pattern
   * `ver_<jobId>_<timestamp>` — that's what we pass here, NOT the
   * integer version number.
   */
  function versionGet(jobId, versionDocId) {
    return call(
      'data/version-get?jobId=' + encodeURIComponent(jobId) +
      '&id=' + encodeURIComponent(versionDocId),
      null,
      'GET'
    );
  }

  // ── App-prefs read helper ─────────────────────────────────────────────────
  // Lets pages know whether auto-snapshot is enabled. Default ON matches
  // DEFAULT_APP_PREFS in dashboard.html.
  function isAutoSnapshotEnabled() {
    try {
      const p = JSON.parse(localStorage.getItem('cygenix_app_prefs') || '{}');
      return (p.autoSnapshot || 'on') === 'on';
    } catch { return true; }
  }

  window.CygenixAPI = {
    call,
    versionCreate,
    versionList,
    versionGet,
    isAutoSnapshotEnabled,
    getEmail,
  };
})();
