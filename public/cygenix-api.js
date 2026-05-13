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

  function getEmail() {
    try {
      const u = JSON.parse(localStorage.getItem('cygenix_user') || '{}');
      return u.email || '';
    } catch { return ''; }
  }

  function resolveApi() {
    // Connections are stored as { target: { azureFunctionUrl, azureFunctionKey }, ... }
    // matching the shape written by the Connections page.
    let conn = {};
    try { conn = JSON.parse(localStorage.getItem('cygenix_connections') || '{}'); } catch {}
    const t = conn.target || {};
    let fnUrl = t.azureFunctionUrl || '';
    const key = t.azureFunctionKey || '';
    if (!fnUrl) throw new Error('Azure Function URL missing. Go to Connections → Target and save the function URL.');
    if (!key)   throw new Error('Azure Function key missing. Go to Connections → Target and save the function key.');

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
