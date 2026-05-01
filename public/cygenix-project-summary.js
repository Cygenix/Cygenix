/* cygenix-project-summary.js — v2 (fetch + blob, x-user-id auth)
 *
 * Drop-in module. Add to dashboard.html with:
 *   <script src="/cygenix-project-summary.js"></script>
 *
 * Why v2:
 *   v1 used a hidden iframe to load the document. That can't add headers,
 *   which breaks for Cygenix because the API requires x-user-id. v2 uses
 *   fetch() (which CAN add headers), gets the HTML as a string, turns it
 *   into a blob URL, opens it in a new window, and triggers print there.
 *
 * Endpoint: /api/data/project-summary-document?jobId=<id>
 * Auth:     x-user-id header (matches the other /api/data/* endpoints)
 */

(function () {
  'use strict';

  const VIEW_ID   = 'view-project-summary-document';
  const VIEW_NAME = 'project-summary-document';
  const ENDPOINT  = '/api/data/project-summary-document';

  // ─── Public API ─────────────────────────────────────────────────────────
  window.CygenixPSD = {
    generate: function (jobId, opts) {
      opts = opts || {};
      if (!jobId) { toast('No job selected', 'error'); return; }
      generate(jobId, opts);
    },
    refresh: renderJobList,
  };

  // ─── Bootstrap ──────────────────────────────────────────────────────────
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  function init() {
    injectStyles();
    injectView();
    patchShowView();

    if (location.hash.indexOf('goto=' + VIEW_NAME) !== -1) {
      setTimeout(() => {
        if (typeof window.showView === 'function') window.showView(VIEW_NAME);
      }, 100);
    }
  }

  // ─── Inject the view's markup ───────────────────────────────────────────
  function injectView() {
    if (document.getElementById(VIEW_ID)) return;

    const main = document.querySelector('.main');
    if (!main) {
      console.warn('[CygenixPSD] .main container not found — feature inactive');
      return;
    }

    const view = document.createElement('div');
    view.id = VIEW_ID;
    view.className = 'view';
    view.innerHTML = renderShell();
    main.appendChild(view);

    const refreshBtn = view.querySelector('#cyg-psd-refresh');
    if (refreshBtn) refreshBtn.addEventListener('click', renderJobList);

    const filter = view.querySelector('#cyg-psd-project-filter');
    if (filter) filter.addEventListener('change', renderJobList);
  }

  function renderShell() {
    return `
      <div class="panel-header">
        <div>
          <div class="panel-title">Project Summary Document</div>
          <div class="panel-sub">
            Generate a polished, sign-off-ready PDF for any completed migration job.
            Includes KPI summary, transformations, reconciliation, run timeline, and a sign-off page.
          </div>
        </div>
      </div>

      <div class="cyg-psd-controls" style="display:flex;gap:0.5rem;align-items:center;margin-bottom:1rem;flex-wrap:wrap">
        <label class="cyg-psd-master-wrap" style="display:inline-flex;align-items:center;gap:0.4rem;font-size:12px;color:var(--text2);cursor:pointer;user-select:none">
          <input type="checkbox" id="cyg-psd-select-all" class="cyg-psd-checkbox" title="Select all visible">
          <span>Select all</span>
        </label>
        <button id="cyg-psd-delete-selected" class="btn btn-danger btn-sm" disabled
          style="opacity:0.5;cursor:not-allowed"
          title="Delete the selected jobs">🗑 Delete selected</button>
        <span id="cyg-psd-select-count" style="font-size:12px;color:var(--text3)"></span>
        <span style="font-size:12px;color:var(--text3);margin-left:auto">
          Generated on demand — not stored. Click <strong>Generate</strong> on any job to download a fresh PDF.
        </span>
        <select id="cyg-psd-project-filter" class="cyg-psd-select">
          <option value="">All projects</option>
        </select>
        <button id="cyg-psd-refresh" class="btn btn-ghost btn-sm" title="Reload job list">↻ Refresh</button>
      </div>

      <div id="cyg-psd-job-list"></div>
    `;
  }

  // ─── Hook showView ──────────────────────────────────────────────────────
  function patchShowView() {
    const original = window.showView;
    window.showView = function (v) {
      if (typeof original === 'function') {
        original.apply(this, arguments);
      }
      if (v === VIEW_NAME) {
        const view = document.getElementById(VIEW_ID);
        if (view) {
          view.classList.add('active');
          const mainEl = document.querySelector('.main');
          if (mainEl) mainEl.scrollTop = 0;
          if (window.CygenixSidebar && window.CygenixSidebar.setActive) {
            window.CygenixSidebar.setActive(VIEW_NAME);
          }
          renderJobList();
        }
      }
    };
  }

  // ─── Selection state ────────────────────────────────────────────────────
  // Set of job IDs currently checked. Survives re-renders so the user can
  // change the project filter without losing their selection. Selections
  // for jobs that no longer exist (deleted, filtered out by project) are
  // simply not rendered — they stay in the set harmlessly until the next
  // delete call cleans them up.
  const selectedIds = new Set();

  function updateSelectionUI() {
    const view = document.getElementById(VIEW_ID);
    if (!view) return;

    const deleteBtn = view.querySelector('#cyg-psd-delete-selected');
    const countEl   = view.querySelector('#cyg-psd-select-count');
    const masterCb  = view.querySelector('#cyg-psd-select-all');

    // Count only selections that match a currently-visible row, so the
    // "Selected: 3" label tracks what the user can actually see and act on.
    const visibleIds = [...view.querySelectorAll('.cyg-psd-row[data-id]')].map(r => r.dataset.id);
    const visibleSelected = visibleIds.filter(id => selectedIds.has(id));
    const n = visibleSelected.length;

    if (countEl) countEl.textContent = n > 0 ? `${n} selected` : '';
    if (deleteBtn) {
      deleteBtn.disabled = n === 0;
      deleteBtn.style.opacity = n === 0 ? '0.5' : '1';
      deleteBtn.style.cursor  = n === 0 ? 'not-allowed' : 'pointer';
    }

    // Master checkbox tri-state: checked when all visible rows are selected,
    // indeterminate when some are, unchecked when none are.
    if (masterCb) {
      if (visibleIds.length === 0) {
        masterCb.checked = false;
        masterCb.indeterminate = false;
        masterCb.disabled = true;
      } else {
        masterCb.disabled = false;
        masterCb.indeterminate = n > 0 && n < visibleIds.length;
        masterCb.checked = n === visibleIds.length;
      }
    }
  }

  // ─── Render the job list ────────────────────────────────────────────────
  function renderJobList() {
    const view = document.getElementById(VIEW_ID);
    if (!view) return;

    const list = view.querySelector('#cyg-psd-job-list');
    const filter = view.querySelector('#cyg-psd-project-filter');
    if (!list || !filter) return;

    const jobs = readJobs();
    const projects = readProjects();

    const current = filter.value;
    filter.innerHTML = '<option value="">All projects</option>' +
      projects.map(p => `<option value="${attr(p.id)}">${esc(p.name || p.id)}</option>`).join('');
    filter.value = current;

    const selected = filter.value;
    const filtered = jobs.filter(j => !selected || j.projectId === selected);

    if (!filtered.length) {
      list.innerHTML = renderEmpty(jobs.length === 0);
      updateSelectionUI();
      return;
    }

    list.innerHTML = filtered.map(renderRow).join('');

    list.querySelectorAll('[data-generate]').forEach(b =>
      b.addEventListener('click', () => window.CygenixPSD.generate(b.dataset.generate))
    );
    list.querySelectorAll('[data-preview]').forEach(b =>
      b.addEventListener('click', () => window.CygenixPSD.generate(b.dataset.preview, { openInTab: true }))
    );
    // Per-row checkbox: clicking toggles selection without triggering
    // anything else on the row.
    list.querySelectorAll('.cyg-psd-row-checkbox').forEach(cb => {
      cb.addEventListener('click', e => e.stopPropagation());
      cb.addEventListener('change', e => {
        const id = e.target.dataset.id;
        if (!id) return;
        if (e.target.checked) selectedIds.add(id);
        else selectedIds.delete(id);
        updateSelectionUI();
      });
    });

    // Master checkbox — wired here (not in init) because the controls live
    // inside the same view shell as the list, but we also want them to
    // re-bind cleanly if anything ever rebuilds the shell.
    const masterCb = view.querySelector('#cyg-psd-select-all');
    if (masterCb && !masterCb.dataset.bound) {
      masterCb.dataset.bound = '1';
      masterCb.addEventListener('change', () => {
        const visibleIds = [...view.querySelectorAll('.cyg-psd-row[data-id]')].map(r => r.dataset.id);
        if (masterCb.checked) {
          visibleIds.forEach(id => selectedIds.add(id));
        } else {
          visibleIds.forEach(id => selectedIds.delete(id));
        }
        // Reflect in row checkboxes
        view.querySelectorAll('.cyg-psd-row-checkbox').forEach(cb => {
          cb.checked = selectedIds.has(cb.dataset.id);
        });
        updateSelectionUI();
      });
    }

    const deleteBtn = view.querySelector('#cyg-psd-delete-selected');
    if (deleteBtn && !deleteBtn.dataset.bound) {
      deleteBtn.dataset.bound = '1';
      deleteBtn.addEventListener('click', () => deleteSelectedJobs());
    }

    updateSelectionUI();
  }

  function renderEmpty(noJobsAtAll) {
    if (noJobsAtAll) {
      return `
        <div style="padding:3rem 2rem;text-align:center;background:var(--bg2);border:1px dashed var(--border);border-radius:8px;color:var(--text2)">
          <div style="font-size:2.5rem;margin-bottom:1rem">📄</div>
          <h3 style="color:var(--text);margin:0 0 0.5rem 0;font-size:1.1rem">No jobs yet</h3>
          <p style="margin:0;font-size:0.9rem;line-height:1.5">
            Once you've run a migration job, it'll appear here and you can generate a Project Summary Document.
          </p>
          <p style="margin-top:1rem">
            <a href="/project-builder.html" style="color:var(--accent);text-decoration:none;font-weight:600">Go to Execute →</a>
          </p>
        </div>`;
    }
    return `
      <div style="padding:3rem 2rem;text-align:center;background:var(--bg2);border:1px dashed var(--border);border-radius:8px;color:var(--text2)">
        <h3 style="color:var(--text);margin:0 0 0.5rem 0;font-size:1.1rem">No jobs match this project</h3>
        <p style="margin:0;font-size:0.9rem">Try clearing the project filter above, or pick a different one.</p>
      </div>`;
  }

  function renderRow(job) {
    const status = String(job.status || 'pending').toLowerCase();
    const statusClass = {
      success:'success', complete:'success', completed:'success',
      failed:'danger', error:'danger',
      running:'info',
      pending:'neutral',
    }[status] || 'neutral';

    const lastRun = job.runCompletedAt || job.lastRun || job.updatedAt || job.createdAt;
    const lastRunLabel = lastRun ? formatDate(lastRun) : '—';

    const tableCount = (job.tables && job.tables.length) || 0;
    const rowCount = (job.tables || []).reduce((s, t) => s + (t.targetRows || t.sourceRows || 0), 0);

    const id = job.id || '';
    const isChecked = selectedIds.has(id);

    // Allow generation for any job — the document still produces a useful
    // overview even for in-progress or partial jobs. The status badge tells
    // the truth about what's inside.
    return `
      <div class="cyg-psd-row" data-id="${attr(id)}" style="display:flex;justify-content:space-between;align-items:center;gap:1rem;padding:1rem 1.25rem;background:var(--bg2);border:1px solid var(--border);border-radius:8px;margin-bottom:0.5rem;transition:border-color 0.15s">
        <input type="checkbox" class="cyg-psd-checkbox cyg-psd-row-checkbox" data-id="${attr(id)}" ${isChecked ? 'checked' : ''}
          title="Select this job" style="flex-shrink:0;cursor:pointer">
        <div style="flex:1;min-width:0">
          <div style="font-size:14px;font-weight:600;color:var(--text);margin-bottom:0.35rem;display:flex;align-items:center;gap:0.5rem;flex-wrap:wrap">
            ${esc(job.name || job.id || 'Untitled job')}
            <span class="cyg-psd-badge cyg-psd-badge-${statusClass}">${esc(status)}</span>
          </div>
          <div style="font-size:12px;color:var(--text3);display:flex;gap:1rem;flex-wrap:wrap">
            <span title="Job ID"><code style="font-family:var(--mono,'JetBrains Mono',monospace);background:rgba(255,255,255,0.05);padding:0.1rem 0.4rem;border-radius:3px;font-size:11px">${esc(job.id || '—')}</code></span>
            ${job.version ? `<span>Version ${esc(job.version)}</span>` : ''}
            ${tableCount ? `<span>${tableCount} table${tableCount === 1 ? '' : 's'}</span>` : ''}
            ${rowCount ? `<span>${formatCompact(rowCount)} rows</span>` : ''}
            <span>Last run: ${esc(lastRunLabel)}</span>
          </div>
        </div>
        <div style="display:flex;gap:0.5rem;flex-shrink:0">
          <button class="btn btn-ghost btn-sm" data-preview="${attr(job.id)}" title="Preview in a new tab">👁 Preview</button>
          <button class="btn btn-primary btn-sm" data-generate="${attr(job.id)}" title="Generate and download as PDF">📄 Generate</button>
        </div>
      </div>`;
  }

  // ─── Delete handler ─────────────────────────────────────────────────────
  // Removes the selected jobs from the local cygenix_jobs list, then asks
  // CygenixSync to push the new (shorter) list up to Cosmos. The /api/data
  // 'save' action accepts a top-level `jobs` array and replaces the stored
  // value, so the deletion propagates server-side as part of the next sync.
  //
  // We don't try to do server-side deletes per-job — there's no /jobs DELETE
  // endpoint in this app. The "save the new array" pattern matches how the
  // rest of the app persists changes (project edits, mappings, wasis rules).
  async function deleteSelectedJobs() {
    const view = document.getElementById(VIEW_ID);
    if (!view) return;

    // Snapshot the IDs that exist in the current jobs list AND are selected.
    // Stale selections (jobs already deleted, or filter-hidden jobs that
    // were never visible) are ignored so a stray entry in selectedIds
    // doesn't blow up the count or accidentally affect unrelated jobs.
    const allJobs = readJobs();
    const ids = allJobs.filter(j => j && selectedIds.has(j.id)).map(j => j.id);
    if (!ids.length) {
      toast('Nothing selected', 'info');
      return;
    }

    const msg = ids.length === 1
      ? `Delete this job? This removes it from the Project Summary list and from your synced data. The underlying migration audit trail is preserved separately and can't be recovered through this screen — but the job record itself is gone.`
      : `Delete ${ids.length} jobs? This removes them from the Project Summary list and from your synced data. The underlying migration audit trail is preserved separately and can't be recovered through this screen — but the job records themselves are gone.`;

    // Plain confirm() — keeps the change small and matches the rest of the
    // app's pattern. If a custom modal dialog is added later, this is the
    // single place that needs updating.
    if (!window.confirm(msg)) return;

    const deleteBtn = view.querySelector('#cyg-psd-delete-selected');
    if (deleteBtn) {
      deleteBtn.disabled = true;
      deleteBtn.textContent = '⏳ Deleting…';
    }

    try {
      const idSet = new Set(ids);
      const remaining = allJobs.filter(j => !idSet.has(j.id));

      // Persist locally first so the UI reflects the change immediately,
      // even if the network sync is slow or fails.
      try {
        localStorage.setItem('cygenix_jobs', JSON.stringify(remaining));
      } catch (e) {
        console.error('[CygenixPSD] failed to write cygenix_jobs:', e);
        toast('Local save failed: ' + (e.message || 'unknown error'), 'error');
        if (deleteBtn) {
          deleteBtn.disabled = false;
          deleteBtn.textContent = '🗑 Delete selected';
        }
        return;
      }

      // Push to server. We deliberately do NOT use CygenixSync.save() or
      // saveNow() here because both run a load-merge-save cycle that uses
      // *union* semantics for arrays — they fetch the server's current
      // jobs[] and merge it with the local jobs[] before writing back.
      // Union of "list with X" and "list without X" always equals "list
      // with X", so deletes are impossible to express through that path:
      // the deleted job comes back on every save.
      //
      // Instead we POST directly to /api/data/save with only the `jobs`
      // field. The server's save action whitelists fields and replaces
      // the value of any field present in the body — no merge, no union,
      // the server ends up with exactly the array we just sent. This is
      // the same endpoint and field name that CygenixSync uses, just
      // skipping the client-side merge step that breaks deletes.
      let synced = false;
      try {
        if (window.CygenixSync && typeof window.CygenixSync.getUserId === 'function') {
          const userId = window.CygenixSync.getUserId();
          const apiBase = (window.CygenixSync.apiBase || '').replace(/\/$/, '');
          const fnKey = window.CygenixSync.funcCode || '';
          if (userId && apiBase) {
            const params = new URLSearchParams();
            if (fnKey) params.set('code', fnKey);
            const url = apiBase + '/save' + (params.toString() ? '?' + params.toString() : '');
            const r = await fetch(url, {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                'x-user-id': userId
              },
              body: JSON.stringify({ jobs: remaining })
            });
            if (r.ok) {
              synced = true;
            } else {
              const errText = await r.text().catch(() => '');
              console.error('[CygenixPSD] direct save returned ' + r.status + ':', errText.substring(0, 200));
            }
          }
        }
      } catch (e) {
        console.error('[CygenixPSD] direct save after delete failed:', e);
      }

      // Clear from selection set so the count resets cleanly
      ids.forEach(id => selectedIds.delete(id));

      // Restore the button label before re-render — updateSelectionUI()
      // handles the disabled/opacity, but the textContent is ours to
      // manage. Without this the button gets stuck saying "Deleting…"
      // even after the operation finished.
      if (deleteBtn) deleteBtn.textContent = '🗑 Delete selected';

      // Re-render to remove the rows. renderJobList() calls
      // updateSelectionUI() at the end, so the master checkbox / count /
      // delete-button states all reset together.
      renderJobList();

      if (synced) {
        toast(ids.length === 1 ? 'Job deleted' : `${ids.length} jobs deleted`, 'success');
      } else {
        toast(
          'Deleted locally — but the change couldn\u2019t reach the server. They\u2019ll re-sync next time you load the dashboard.',
          'warn'
        );
      }
    } catch (e) {
      console.error('[CygenixPSD] delete failed:', e);
      toast('Delete failed: ' + (e.message || 'unknown error'), 'error');
      if (deleteBtn) {
        deleteBtn.disabled = false;
        deleteBtn.textContent = '🗑 Delete selected';
      }
    }
  }

  // ─── Data access ────────────────────────────────────────────────────────
  function readJobs() {
    const keys = ['cygenix_jobs', 'cygenix_all_jobs', 'cyg_jobs'];
    for (const k of keys) {
      try {
        const raw = localStorage.getItem(k);
        if (!raw) continue;
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed) && parsed.length) return parsed;
        if (parsed && Array.isArray(parsed.jobs)) return parsed.jobs;
      } catch { /* keep trying */ }
    }
    return [];
  }

  function readProjects() {
    const keys = ['cygenix_projects', 'cyg_projects'];
    for (const k of keys) {
      try {
        const raw = localStorage.getItem(k);
        if (!raw) continue;
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) return parsed;
        if (parsed && Array.isArray(parsed.projects)) return parsed.projects;
      } catch { /* keep trying */ }
    }
    return [];
  }

  function getUserId() {
    // Prefer CygenixSync — it's the single source of truth for who's signed
    // in (it does the JSON-parsing and email-extraction we'd otherwise have
    // to repeat here). Fall back to localStorage only if CygenixSync hasn't
    // loaded yet (which can happen if this module runs first).
    if (window.CygenixSync && typeof window.CygenixSync.getUserId === 'function') {
      const v = window.CygenixSync.getUserId();
      if (v) return v;
    }

    // Fallback path — read directly from cygenix_user
    const raw = readLocalString('cygenix_user');
    if (!raw) return '';
    try {
      const obj = JSON.parse(raw);
      if (obj && typeof obj === 'object') {
        return String(obj.email || obj.userId || obj.id || '').trim();
      }
    } catch { /* not JSON — assume plain string */ }
    return raw;
  }

  function getFunctionKey() {
    // CygenixSync.funcCode is the canonical source. Never hardcode the key
    // here — if it ever rotates, both files would need editing. By going
    // through CygenixSync, the rotation only happens once.
    if (window.CygenixSync && window.CygenixSync.funcCode) {
      return window.CygenixSync.funcCode;
    }
    return '';
  }

  function getFunctionBase() {
    // Prefer CygenixSync.apiBase. It's the canonical Function host (with
    // /api/data already appended). Strip /api/data because our ENDPOINT
    // constant already starts with /api/data/...
    if (window.CygenixSync && window.CygenixSync.apiBase) {
      return window.CygenixSync.apiBase.replace(/\/api\/data\/?$/, '').replace(/\/$/, '');
    }
    return 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net';
  }

  function readLocalString(key) {
    try { const v = localStorage.getItem(key); return v ? String(v).trim() : ''; }
    catch { return ''; }
  }

  // ─── Generation: fetch HTML, render in iframe, print it ────────────────
  // We previously used window.open() to display the document, but Chrome
  // blocks popups opened after an `await` (the click is no longer
  // considered "user-initiated"). Instead we:
  //   1. fetch the HTML with the right auth headers
  //   2. inject it into a hidden iframe via srcdoc (same-origin, no header
  //      problem because the iframe doesn't fetch — it just renders the
  //      HTML we already have)
  //   3. call print() on the iframe's contentWindow once it loads
  //   4. clean up after print closes
  async function generate(jobId, opts) {
    const userId = getUserId();
    if (!userId) {
      toast('Not signed in — please log in first', 'error');
      return;
    }

    const overlay = showOverlay();
    const base = getFunctionBase();
    const fnKey = getFunctionKey();

    // Build URL with optional ?code= for function key
    const params = new URLSearchParams({ jobId });
    if (fnKey) params.set('code', fnKey);
    const url = `${base}${ENDPOINT}?${params.toString()}`;

    try {
      const resp = await fetch(url, {
        method: 'GET',
        headers: {
          'x-user-id': userId,
          'Accept': 'text/html, application/json',
        },
      });

      const contentType = resp.headers.get('Content-Type') || '';
      if (!resp.ok) {
        let msg = `HTTP ${resp.status}`;
        try {
          const errData = await resp.json();
          if (errData && errData.error) msg = errData.error;
        } catch { /* response wasn't JSON */ }
        throw new Error(msg);
      }
      if (!contentType.includes('text/html')) {
        throw new Error('Server did not return HTML');
      }

      const htmlText = await resp.text();

      if (opts.openInTab) {
        // Preview path: open in a new tab via blob. Most browsers allow
        // this if the user-gesture context is preserved across the await
        // (Chrome is lenient here, blocks only window.open with a "_blank"
        // target and no URL). We also fall back to data: URL if blob fails.
        const blob = new Blob([htmlText], { type: 'text/html' });
        const blobUrl = URL.createObjectURL(blob);
        const win = window.open(blobUrl, '_blank');
        if (!win) {
          URL.revokeObjectURL(blobUrl);
          // Fallback: render inline so the user at least sees the document
          renderInIframe(htmlText, /*autoPrint=*/false);
        } else {
          setTimeout(() => URL.revokeObjectURL(blobUrl), 30000);
        }
        return;
      }

      // Print path: hidden iframe + auto-print. No popup, no blocking.
      renderInIframe(htmlText, /*autoPrint=*/true);

    } catch (e) {
      console.error('[CygenixPSD] generation failed:', e);
      toast('Couldn\u2019t generate document: ' + (e.message || 'unknown error'), 'error');
    } finally {
      hideOverlay(overlay);
    }
  }

  // Render an HTML document inside a hidden iframe and (optionally) trigger
  // print on it once it's loaded. The iframe is removed after the user
  // closes the print dialog (or after a long timeout as a safety net).
  function renderInIframe(htmlText, autoPrint) {
    const iframe = document.createElement('iframe');
    iframe.style.cssText = 'position:fixed;right:0;bottom:0;width:0;height:0;border:0;visibility:hidden;';
    iframe.setAttribute('aria-hidden', 'true');

    let printed = false;
    const cleanup = () => {
      if (iframe && iframe.parentNode) iframe.parentNode.removeChild(iframe);
    };

    iframe.onload = function () {
      if (!autoPrint) return;
      // Tiny delay so layout settles before print() snapshots the page
      setTimeout(() => {
        try {
          iframe.contentWindow.focus();
          iframe.contentWindow.print();
          printed = true;
          // Clean up shortly after — print dialog has its own copy
          setTimeout(cleanup, 2000);
        } catch (e) {
          console.warn('[CygenixPSD] iframe print failed:', e);
          cleanup();
        }
      }, 300);
    };

    document.body.appendChild(iframe);

    // srcdoc renders the HTML inline — same-origin, no fetch, no header issues
    iframe.srcdoc = htmlText;

    // Safety net: if something goes wrong and print never fires, clean up
    // after 60s so we don't leak iframes
    setTimeout(() => { if (!printed) cleanup(); }, 60000);
  }

  // ─── UI helpers ─────────────────────────────────────────────────────────
  function showOverlay() {
    const el = document.createElement('div');
    el.style.cssText = `
      position:fixed;inset:0;background:rgba(10,14,26,0.7);backdrop-filter:blur(4px);
      z-index:9999;display:flex;align-items:center;justify-content:center;
      font-family:Inter,-apple-system,sans-serif;color:#fff;
    `;
    el.innerHTML = `
      <div style="text-align:center">
        <div style="width:48px;height:48px;margin:0 auto 16px;border:3px solid rgba(99,102,241,0.3);border-top-color:#6366f1;border-radius:50%;animation:cygPsdSpin 0.8s linear infinite"></div>
        <div style="font-size:15px;font-weight:600;margin-bottom:4px">Generating Project Summary Document</div>
        <div style="font-size:12px;color:#94a3b8">Pulling job, audit log, and reconciliation data\u2026</div>
      </div>`;
    document.body.appendChild(el);
    return el;
  }

  function hideOverlay(el) {
    if (el && el.parentNode) el.parentNode.removeChild(el);
  }

  function toast(msg, kind) {
    if (window.CygenixToast && window.CygenixToast.show) {
      window.CygenixToast.show(msg, kind || 'info'); return;
    }
    // Colour by kind. Default (info) stays the dark slate from the original
    // implementation. Success and warn join error in actually changing the
    // background — without this, a "Job deleted" success and a "delete
    // failed" error look identical, which is a confusing UX after a
    // destructive action.
    const bg = kind === 'error'   ? '#ef4444'
             : kind === 'warn'    ? '#d97706'
             : kind === 'success' ? '#10b981'
             : '#1a1d24';
    const el = document.createElement('div');
    el.style.cssText = `
      position:fixed;bottom:24px;left:50%;transform:translateX(-50%);
      background:${bg};color:#fff;
      padding:10px 18px;border-radius:6px;font-family:Inter,sans-serif;
      font-size:13px;z-index:10000;box-shadow:0 8px 24px rgba(0,0,0,0.3);transition:opacity 0.3s;
    `;
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => { el.style.opacity = '0'; setTimeout(() => el.remove(), 300); }, 4000);
  }

  // ─── Formatting ─────────────────────────────────────────────────────────
  function esc(v) {
    if (v == null) return '';
    return String(v).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
  }
  function attr(v) { return esc(v); }

  function formatCompact(n) {
    if (n == null) return '—';
    if (n >= 1e9) return (n/1e9).toFixed(2)+'B';
    if (n >= 1e6) return (n/1e6).toFixed(2)+'M';
    if (n >= 1e3) return (n/1e3).toFixed(1)+'K';
    return String(n);
  }

  function formatDate(iso) {
    if (!iso) return '—';
    try {
      const d = new Date(iso);
      if (isNaN(d.getTime())) return String(iso);
      return d.toLocaleDateString('en-GB', { day:'2-digit', month:'short', year:'numeric' }) +
        ', ' + d.toLocaleTimeString('en-GB', { hour:'2-digit', minute:'2-digit' });
    } catch { return String(iso); }
  }

  // ─── Styles ─────────────────────────────────────────────────────────────
  function injectStyles() {
    if (document.getElementById('cyg-psd-styles')) return;
    const s = document.createElement('style');
    s.id = 'cyg-psd-styles';
    s.textContent = `
      @keyframes cygPsdSpin { to { transform: rotate(360deg); } }
      .cyg-psd-row:hover { border-color: var(--accent) !important; }
      .cyg-psd-select {
        background: var(--bg2);
        color: var(--text);
        border: 1px solid var(--border);
        padding: 0.4rem 0.7rem;
        border-radius: 6px;
        font-size: 12px;
        font-family: inherit;
        cursor: pointer;
      }
      .cyg-psd-badge {
        display: inline-block;
        padding: 0.1rem 0.5rem;
        border-radius: 3px;
        font-size: 10px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.05em;
      }
      .cyg-psd-badge-success { background: rgba(16,185,129,0.15); color: #10b981; }
      .cyg-psd-badge-danger  { background: rgba(239,68,68,0.15);  color: #ef4444; }
      .cyg-psd-badge-info    { background: rgba(99,102,241,0.15); color: #6366f1; }
      .cyg-psd-badge-neutral { background: rgba(148,163,184,0.15); color: #94a3b8; }
      /* Checkbox styling — sized to match the rest of the controls. We use
         accent-color rather than custom backgrounds so the checkbox follows
         the OS / browser theme (dark mode in Chrome, light in Safari, etc).
         Saves us implementing focus/disabled/indeterminate states by hand. */
      .cyg-psd-checkbox {
        width: 16px;
        height: 16px;
        margin: 0;
        accent-color: #6366f1;
        cursor: pointer;
      }
      .cyg-psd-checkbox:disabled { cursor: not-allowed; opacity: 0.4; }
      /* Danger button — used for "Delete selected". Falls back to a sensible
         red palette so we don't depend on the host app having defined a
         .btn-danger style for us. */
      .btn.btn-danger {
        background: rgba(239,68,68,0.12);
        color: #ef4444;
        border: 1px solid rgba(239,68,68,0.35);
      }
      .btn.btn-danger:hover:not(:disabled) {
        background: rgba(239,68,68,0.2);
        border-color: rgba(239,68,68,0.55);
      }
      .btn.btn-danger:disabled { background: rgba(148,163,184,0.08); color: #94a3b8; border-color: var(--border); }
    `;
    document.head.appendChild(s);
  }
})();
