/* cygenix-api.js
 * ----------------
 * Shared Azure Function API helper. Both dashboard.html and object_mapping.html
 * (and any future page) include this script and use window.CygenixAPI for
 * calls into the Cosmos-backed function app.
 *
 * Replaces the dashboard-only ta_resolveApi / ta_apiCall / ta_data helpers.
 * The dashboard's ta_data wrapper can now delegate to CygenixAPI.call(),
 * so the schedule code keeps working unchanged.
 *
 * Conventions:
 *   - The Azure Function URL and key are stored under cygenix_connections.target
 *     by the Connections page on the dashboard.
 *   - The signed-in user's email is read from cygenix_user.email (set by the
 *     auth-gate at page load).
 *   - All non-GET requests send Content-Type: application/json and an
 *     x-user-id header so the function can attribute the action.
 */
(function () {
  'use strict';

  // Mirrors currentCygenixEmail() in dashboard.html. Reads the Entra account
  // object from sessionStorage (preferred) or localStorage, then extracts the
  // email/userId. Lowercased and trimmed to match how the rest of the app
  // normalises identifiers.
  function getEmail() {
    try {
      const rawAcc = sessionStorage.getItem('cygenix_entra_account')
                  || localStorage.getItem('cygenix_entra_account');
      if (!rawAcc) return '';
      const acc = JSON.parse(rawAcc);
      return ((acc.email || acc.userId) || '').toLowerCase().trim();
    } catch { return ''; }
  }

  // Mirrors ta_resolveApi() in dashboard.html — but extended to use the same
  // multi-source lookup chain as impGetConn() (line ~11620), because
  // ta_resolveApi alone fails on pages where CygenixConnections has not yet
  // populated the flat session/local-storage keys.
  //
  // Priority (highest to lowest):
  //   1. window.CygenixConnections.get() → tgtFnUrl/fnUrl + tgtFnKey/fnKey
  //   2. localStorage.cygenix_project_connections (JSON wrapper) →
  //        cygenix_fn_url / cygenix_fn_key fields
  //   3. sessionStorage/localStorage flat keys cygenix_fn_url / cygenix_fn_key
  //
  // Strips a trailing slash and ensures the URL contains /api so we can return
  // a base ending in /api. Throws with a user-facing message if nothing is
  // available — the History modal displays that message in the version list pane.
  function resolveApi() {
    const ss = k => { try { return sessionStorage.getItem(k) || localStorage.getItem(k) || ''; } catch { return ''; } };

    // Source 1: live CygenixConnections helper, if loaded.
    let c = {};
    try {
      if (window.CygenixConnections && typeof window.CygenixConnections.get === 'function') {
        c = window.CygenixConnections.get() || {};
      }
    } catch { c = {}; }

    // Source 2: project-connections JSON wrapper in localStorage.
    let pc = {};
    try { pc = JSON.parse(localStorage.getItem('cygenix_project_connections') || '{}') || {}; } catch { pc = {}; }
    const pcGet = k => (pc[k] != null ? String(pc[k]) : '');

    // Compose with source 3 as final fallback.
    let fnUrl = c.tgtFnUrl || c.fnUrl || pcGet('cygenix_fn_url') || ss('cygenix_fn_url');
    const key = c.tgtFnKey || c.fnKey || pcGet('cygenix_fn_key') || ss('cygenix_fn_key');

    if (!fnUrl) throw new Error('Azure Function not configured. Go to Connections → Target → Azure Function and save a URL + key.');
    if (!key)   throw new Error('Azure Function key missing. Go to Connections → Target → Azure Function and save the function key.');

    // Guard: rule out non-HTTP values that have been observed mis-saved under
    // cygenix_fn_url (e.g. raw connection strings like "mssql://host:port/db").
    // Without this check we'd try to fetch() that string as an HTTPS endpoint
    // and get a confusing network error.
    if (!/^https?:\/\//i.test(fnUrl)) {
      throw new Error('Azure Function URL is not a valid HTTP(S) URL. Go to Connections → Target → Azure Function and re-save.');
    }

    fnUrl = fnUrl.replace(/\/+$/, '');
    const apiIdx = fnUrl.toLowerCase().lastIndexOf('/api');
    if (apiIdx < 0) throw new Error('Azure Function URL does not contain /api path');
    const base = fnUrl.slice(0, apiIdx) + '/api';
    return { base, key };
  }

  /**
   * Generic call into the function app.
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
    // otherwise use ? to introduce the query string. Without this, a GET
    // with query params produces a malformed URL like '?jobId=X?code=Y'
    // and Azure rejects it with 401 because the function key isn't parsed.
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

  // ── Version control wrappers ────────────────────────────────────────────────
  // Thin convenience wrappers around the existing data/version-* endpoints.

  /**
   * Create a snapshot of a job. Backend dedupes by content hash, so calling
   * this on every save is safe — duplicate snapshots aren't written.
   * @returns {Promise<{created: boolean, version: number, id?: string}>}
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
   * List all versions for a job, newest first. Returns lightweight summaries
   * (no snapshot blob) — call versionGet for the full payload.
   */
  function versionList(jobId) {
    return call('data/version-list?jobId=' + encodeURIComponent(jobId), null, 'GET');
  }

  /**
   * Fetch a single version document including the full snapshot.
   */
  function versionGet(jobId, versionId) {
    return call(
      'data/version-get?jobId=' + encodeURIComponent(jobId) +
      '&versionId=' + encodeURIComponent(versionId),
      null,
      'GET'
    );
  }

  // ── App-prefs read helper ──────────────────────────────────────────────────
  // Pages that want to know whether auto-snapshot is enabled.
  function isAutoSnapshotEnabled() {
    try {
      const p = JSON.parse(localStorage.getItem('cygenix_app_prefs') || '{}');
      // Default ON if the pref hasn't been written yet — matches the dashboard
      // DEFAULT_APP_PREFS in dashboard.html.
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
