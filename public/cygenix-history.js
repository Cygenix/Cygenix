/* cygenix-history.js
 * --------------------
 * Shared "Version History" modal. Included on dashboard.html and
 * object_mapping.html. Exposes:
 *
 *   CygenixHistory.open(jobId)
 *     Opens the modal for the given job, lists all versions, lets the user
 *     view each snapshot and revert to one.
 *
 *   CygenixHistory.autoSnapshot(job, { note })
 *     Called by saveAsJob after a successful save. Fires a non-blocking
 *     version-create with label='auto'. The backend dedupes by hash, so
 *     this is safe to call on every save. Errors are logged, not surfaced.
 *
 * Revert flow:
 *   1. Take a "pre-revert" snapshot of the current job state (so revert
 *      itself is undoable).
 *   2. Overwrite the job in cygenix_jobs localStorage with the chosen
 *      snapshot. Preserve the job id.
 *   3. If CygenixSync is available, push the change to Cosmos so other
 *      browsers/devices see the revert.
 *   4. Refresh the modal list (the pre-revert snapshot now appears as v(n+1))
 *      and fire a 'cygenix:job-reverted' window event so host pages can
 *      re-render their job list / re-hydrate the editor.
 */
(function () {
  'use strict';

  const STORAGE_KEY = 'cygenix_jobs';

  let _rootEl     = null;   // Modal root element (created once)
  let _state      = null;   // { jobId, versions, selected, currentJob }

  // ── DOM bootstrap ──────────────────────────────────────────────────────────
  function ensureMounted() {
    if (_rootEl) return _rootEl;
    _rootEl = document.createElement('div');
    _rootEl.className = 'cygenix-history-modal';
    _rootEl.innerHTML = `
      <div class="ch-card" role="dialog" aria-modal="true" aria-label="Version History">
        <div class="ch-header">
          <div>
            <h3 class="ch-title">Version history</h3>
            <div class="ch-sub" id="ch-job-id"></div>
          </div>
          <button class="ch-close" type="button">✕ Close</button>
        </div>
        <div class="ch-body">
          <div class="ch-list" id="ch-list"></div>
          <div class="ch-detail" id="ch-detail">
            <div class="ch-empty">Select a version on the left to view its snapshot.</div>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(_rootEl);

    // Click on backdrop closes; clicks inside the card do not.
    _rootEl.addEventListener('click', (e) => {
      if (e.target === _rootEl) close();
    });
    _rootEl.querySelector('.ch-close').addEventListener('click', close);
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && _rootEl.classList.contains('open')) close();
    });
    return _rootEl;
  }

  // ── Local storage helpers ──────────────────────────────────────────────────
  function readJobs() {
    try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]'); }
    catch { return []; }
  }
  function writeJobs(jobs) {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(jobs.slice(0, 100)));
  }
  function findJob(jobId) {
    return readJobs().find(j => j.id === jobId) || null;
  }

  // ── Public: open the modal ────────────────────────────────────────────────
  async function open(jobId) {
    if (!jobId) { alert('No job selected.'); return; }
    ensureMounted();
    _rootEl.classList.add('open');

    const job = findJob(jobId);
    _state = { jobId, versions: [], selected: null, currentJob: job };

    _rootEl.querySelector('#ch-job-id').textContent =
      (job ? job.name + ' · ' : '') + jobId;

    const listEl = _rootEl.querySelector('#ch-list');
    listEl.innerHTML = '<div class="ch-empty">Loading versions…</div>';

    try {
      const res = await window.CygenixAPI.versionList(jobId);
      _state.versions = (res && res.versions) || res || [];
      renderList();
      if (_state.versions.length > 0) {
        selectVersion(_state.versions[0]);
      } else {
        _rootEl.querySelector('#ch-detail').innerHTML =
          '<div class="ch-empty">No versions yet. Save the job to create the first snapshot.</div>';
      }
    } catch (err) {
      listEl.innerHTML =
        '<div class="ch-empty">Failed to load versions: ' + escapeHtml(err.message) + '</div>';
    }
  }

  function close() {
    if (!_rootEl) return;
    _rootEl.classList.remove('open');
    _state = null;
  }

  // ── Rendering ──────────────────────────────────────────────────────────────
  function renderList() {
    const listEl = _rootEl.querySelector('#ch-list');
    if (!_state.versions.length) {
      listEl.innerHTML = '<div class="ch-empty">No versions yet.</div>';
      return;
    }
    listEl.innerHTML = _state.versions.map(v => {
      const isActive = _state.selected && _state.selected.id === v.id;
      const created = formatDate(v.createdAt);
      const label   = v.label || 'manual';
      const noteHtml = v.note ? `<div class="ch-note">${escapeHtml(v.note)}</div>` : '';
      return `
        <div class="ch-row ${isActive ? 'active' : ''}" data-version-id="${escapeAttr(v.id)}">
          <div class="ch-row-top">
            <span class="ch-ver">v${v.version}</span>
            <span class="ch-label ${label}">${label}</span>
          </div>
          <div class="ch-meta">${created} · ${escapeHtml(v.userId || '')}</div>
          ${noteHtml}
        </div>
      `;
    }).join('');

    listEl.querySelectorAll('.ch-row').forEach(row => {
      row.addEventListener('click', () => {
        const id = row.getAttribute('data-version-id');
        const v = _state.versions.find(x => x.id === id);
        if (v) selectVersion(v);
      });
    });
  }

  async function selectVersion(v) {
    _state.selected = v;
    renderList(); // Re-render to update the .active highlight

    const detailEl = _rootEl.querySelector('#ch-detail');
    detailEl.innerHTML = `
      <div class="ch-detail-header">
        <div>
          <h4 class="ch-detail-title">v${v.version} · ${escapeHtml(v.label || 'manual')}</h4>
          <div class="ch-sub">${formatDate(v.createdAt)} · ${escapeHtml(v.userId || '')}</div>
        </div>
        <div style="display:flex;gap:6px">
          <button class="ch-btn primary" id="ch-revert-btn">↶ Revert to this version</button>
        </div>
      </div>
      <div class="ch-snapshot" id="ch-snapshot-pane">Loading snapshot…</div>
    `;
    detailEl.querySelector('#ch-revert-btn').addEventListener('click', () => doRevert(v));

    try {
      const full = await window.CygenixAPI.versionGet(_state.jobId, v.id);
      const snapshot = (full && full.snapshot) || full;
      detailEl.querySelector('#ch-snapshot-pane').textContent =
        JSON.stringify(snapshot, null, 2);
      // Stash the fully-loaded snapshot on the version object so revert
      // doesn't need to re-fetch.
      v._fullSnapshot = snapshot;
    } catch (err) {
      detailEl.querySelector('#ch-snapshot-pane').textContent =
        'Failed to load snapshot: ' + err.message;
    }
  }

  // ── Revert ─────────────────────────────────────────────────────────────────
  async function doRevert(v) {
    if (!_state || !_state.jobId) return;
    const jobId = _state.jobId;

    if (!confirm(
      'Revert this job to v' + v.version + '?\n\n' +
      'The current state will be saved as a "pre-revert" snapshot first, so you can undo this.'
    )) return;

    const revertBtn = _rootEl.querySelector('#ch-revert-btn');
    if (revertBtn) { revertBtn.disabled = true; revertBtn.textContent = 'Reverting…'; }

    try {
      // 1. Pre-revert snapshot of current state.
      const currentJob = findJob(jobId);
      if (currentJob) {
        try {
          await window.CygenixAPI.versionCreate(
            jobId, currentJob, 'pre-revert',
            'Auto-saved before reverting to v' + v.version
          );
        } catch (e) {
          // If the pre-revert snapshot fails, abort — we don't want to lose
          // the current state with no backstop.
          if (!confirm(
            'Could not save a pre-revert snapshot: ' + e.message +
            '\n\nProceed with the revert anyway? Current state will NOT be recoverable.'
          )) {
            if (revertBtn) { revertBtn.disabled = false; revertBtn.textContent = '↶ Revert to this version'; }
            return;
          }
        }
      }

      // 2. Make sure we have the full snapshot for the target version.
      let snapshot = v._fullSnapshot;
      if (!snapshot) {
        const full = await window.CygenixAPI.versionGet(jobId, v.id);
        snapshot = (full && full.snapshot) || full;
      }
      if (!snapshot) throw new Error('Snapshot payload was empty.');

      // 3. Overwrite the job in localStorage. Preserve the id (snapshot
      //    should already have the same id but be defensive).
      const reverted = Object.assign({}, snapshot, { id: jobId });
      const jobs = readJobs();
      const idx = jobs.findIndex(j => j.id === jobId);
      if (idx > -1) jobs[idx] = reverted;
      else          jobs.unshift(reverted);
      writeJobs(jobs);

      // 4. Push to Cosmos if available.
      if (window.CygenixSync && typeof window.CygenixSync.pushKey === 'function') {
        try { await window.CygenixSync.pushKey(STORAGE_KEY); } catch (_) {}
      }

      // 5. Notify host page so it can refresh whatever it's showing.
      window.dispatchEvent(new CustomEvent('cygenix:job-reverted', {
        detail: { jobId, versionId: v.id, versionNumber: v.version }
      }));

      // 6. Reload version list so the new pre-revert snapshot shows up.
      try {
        const res = await window.CygenixAPI.versionList(jobId);
        _state.versions = (res && res.versions) || res || [];
        renderList();
      } catch (_) {}

      alert('Reverted to v' + v.version + '. The version list has been refreshed.');
    } catch (err) {
      alert('Revert failed: ' + err.message);
    } finally {
      if (revertBtn) { revertBtn.disabled = false; revertBtn.textContent = '↶ Revert to this version'; }
    }
  }

  // ── Auto-note diff helpers ────────────────────────────────────────────────
  // We label every auto-snapshot with a short description of what changed.
  // The diff is computed against a localStorage-cached copy of the previous
  // saved snapshot for this job, so it costs zero network calls. First save
  // has no prior, so we label it "Initial version".
  //
  // Storage shape:
  //   cygenix_last_snapshots: { [jobId]: <full job object> }
  //
  // Deliberately kept tiny — only the most recent snapshot per job. We don't
  // need history here (that lives in Cosmos); this cache exists solely to
  // diff against on the next save.

  const SNAPSHOT_CACHE_KEY = 'cygenix_last_snapshots';

  // Fields that are part of a "change" worth reporting. Order matters —
  // changes get reported in this order, so "Mapping" lands before
  // "Joins" in the resulting label, which reads more naturally.
  //
  // Each entry maps the job-object field name to a short display label.
  // Sub-cases (e.g. single-map columnMapping vs OTM tables) are handled
  // in describeChange below.
  const TRACKED_FIELDS = [
    ['name',            'Renamed'],
    ['jobType',         'Mode'],
    ['sourceTable',     'Source table'],
    ['targetTable',     'Target table'],
    ['target',          'Targets'],      // OTM target list (comma-joined string)
    ['columnMapping',   'Mapping'],
    ['tables',          'OTM tables'],
    ['srcWhere',        'WHERE clause'],
    ['joinState',       'Joins'],
    ['wasisRules',      'Was/is rules'],
    ['insertSQL',       'Insert SQL'],
    ['schemaSQL',       'Schema SQL'],
    ['verifySQL',       'Verify SQL'],
  ];

  // Fields we deliberately ignore — they change on every save (created
  // timestamp), are derived from other fields (warnings), or are
  // bookkeeping that doesn't represent a user intent change.
  // Listed here for documentation; not actually referenced.
  // Ignored: created, status, totalRows, warnings, id, projectId

  // Stable equality check that handles arrays, objects, primitives, and nulls.
  // For arrays/objects we serialise after sorting keys deeply — two values
  // that differ only in key insertion order count as equal.
  function deepEqual(a, b) {
    if (a === b) return true;
    if (a == null || b == null) return a === b;
    if (typeof a !== typeof b) return false;
    if (typeof a !== 'object') return false;
    try {
      return JSON.stringify(sortDeep(a)) === JSON.stringify(sortDeep(b));
    } catch { return false; }
  }
  function sortDeep(v) {
    if (Array.isArray(v)) return v.map(sortDeep);
    if (v && typeof v === 'object') {
      return Object.keys(v).sort().reduce((acc, k) => { acc[k] = sortDeep(v[k]); return acc; }, {});
    }
    return v;
  }

  // Produce a label describing what changed between prev and next.
  // Returns '' if nothing tracked changed (caller decides what to do then;
  // the backend will dedupe by content hash regardless, but a noteless save
  // is fine).
  function describeChange(prev, next) {
    if (!prev) return 'Initial version';

    const parts = [];
    for (const [field, label] of TRACKED_FIELDS) {
      if (!deepEqual(prev[field], next[field])) parts.push(label);
    }
    if (!parts.length) return '';
    return parts.join(', ') + ' changed';
  }

  // Read/write the last-snapshot cache. Stored as one combined object so a
  // browser with many jobs doesn't end up with many localStorage keys.
  function loadSnapshotCache() {
    try { return JSON.parse(localStorage.getItem(SNAPSHOT_CACHE_KEY) || '{}') || {}; }
    catch { return {}; }
  }
  function saveSnapshotCache(cache) {
    try { localStorage.setItem(SNAPSHOT_CACHE_KEY, JSON.stringify(cache)); }
    catch (e) { console.warn('[cygenix-history] could not write snapshot cache:', e); }
  }

  // ── Public: auto-snapshot after save ──────────────────────────────────────
  /**
   * Fire-and-forget snapshot of a freshly-saved job. Called by saveAsJob.
   * Honours the "auto-snapshot on save" pref — if it's off, this is a no-op.
   * All errors are logged, never surfaced — the save itself already
   * succeeded; a version-create failure should not block the user.
   *
   * Before sending, computes a high-level diff label against the previously
   * cached snapshot for this job and uses it as the version note. The
   * History modal then displays that note in the version list so users can
   * see at a glance what each version changed without opening it.
   */
  function autoSnapshot(job, opts) {
    try {
      if (!job || !job.id) return Promise.resolve();
      if (!window.CygenixAPI || !window.CygenixAPI.isAutoSnapshotEnabled()) return Promise.resolve();

      // Compute the auto-note. Caller-supplied note (rare; pre-revert path
      // uses its own label) takes precedence.
      let note = (opts && opts.note) || '';
      if (!note) {
        const cache = loadSnapshotCache();
        const prev  = cache[job.id] || null;
        note = describeChange(prev, job);
      }

      return window.CygenixAPI.versionCreate(job.id, job, 'auto', note)
        .then(result => {
          // Only update the cache if the backend actually wrote a new version.
          // If it was a duplicate (no content change), prev is still the most
          // recent saved state, so leave the cache alone.
          if (result && result.created) {
            const cache = loadSnapshotCache();
            cache[job.id] = job;
            saveSnapshotCache(cache);
          }
          return result;
        })
        .catch(err => { console.warn('[cygenix-history] auto-snapshot failed:', err); });
    } catch (err) {
      console.warn('[cygenix-history] auto-snapshot threw:', err);
      return Promise.resolve();
    }
  }

  // ── Utilities ──────────────────────────────────────────────────────────────
  function escapeHtml(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }
  function escapeAttr(s) { return escapeHtml(s); }
  function formatDate(iso) {
    if (!iso) return '';
    try {
      const d = new Date(iso);
      return d.toLocaleString('en-GB', {
        day: '2-digit', month: 'short', year: 'numeric',
        hour: '2-digit', minute: '2-digit'
      });
    } catch { return iso; }
  }

  window.CygenixHistory = { open, close, autoSnapshot };
})();
