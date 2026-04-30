/* cygenix-project-summary.js — Project Summary Document feature.
 *
 * Drop-in module. Add to dashboard.html with:
 *   <script src="/cygenix-project-summary.js"></script>
 *
 * SAFE INTEGRATION with the existing dashboard:
 *   • Injects a <div id="view-project-summary-document" class="view"> directly
 *     into the .main container — same convention as every other view.
 *   • Hooks window.showView so when v === 'project-summary-document' we render
 *     our list. For everything else we delegate to the original showView, which
 *     already does the right "remove .active from all .view elements" dance.
 *     This means navigating AWAY from our view automatically hides it because
 *     showView strips .active from every .view element.
 *   • Uses the dashboard's existing CSS variables and class names so it looks
 *     native (no theme drift).
 *
 * Data:
 *   - localStorage: cygenix_jobs / cygenix_active_project / cygenix_fn_url / cygenix_fn_key
 *   - Function endpoint: /api/project-summary-document?jobId=<id>
 *
 * Errors:
 *   - In-band debugging — Function returns JSON {error,stack} on 500 so it's
 *     readable in the browser Network tab. We don't need Application Insights.
 */

(function () {
  'use strict';

  const VIEW_ID   = 'view-project-summary-document';
  const VIEW_NAME = 'project-summary-document';
  const ENDPOINT  = '/api/project-summary-document';
  const PRINT_TIMEOUT_MS = 20000;

  // ─── Public API ─────────────────────────────────────────────────────────
  window.CygenixPSD = {
    generate: function (jobId, opts) {
      opts = opts || {};
      if (!jobId) { toast('No job selected', 'error'); return; }
      const url = buildUrl(jobId);
      if (!url) { toast('Azure Function URL not configured', 'error'); return; }
      if (opts.openInTab) { window.open(url, '_blank', 'noopener'); return; }
      generateInIframe(url);
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

    // If page loaded with #goto=project-summary-document (sidebar from another page),
    // open the view shortly after dashboard finishes its own startup.
    if (location.hash.indexOf('goto=' + VIEW_NAME) !== -1) {
      setTimeout(() => {
        if (typeof window.showView === 'function') window.showView(VIEW_NAME);
      }, 100);
    }
  }

  // ─── Inject the view's markup ───────────────────────────────────────────
  function injectView() {
    if (document.getElementById(VIEW_ID)) return; // idempotent

    const main = document.querySelector('.main');
    if (!main) {
      console.warn('[CygenixPSD] .main container not found — feature inactive');
      return;
    }

    const view = document.createElement('div');
    view.id = VIEW_ID;
    view.className = 'view'; // matches the dashboard convention; .active controls visibility
    view.innerHTML = renderShell();
    main.appendChild(view);

    // Wire static controls
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
        <span style="font-size:12px;color:var(--text3)">
          Generated on demand — not stored. Click <strong>Generate</strong> on any job to download a fresh PDF.
        </span>
        <select id="cyg-psd-project-filter" class="cyg-psd-select" style="margin-left:auto">
          <option value="">All projects</option>
        </select>
        <button id="cyg-psd-refresh" class="btn btn-ghost btn-sm" title="Reload job list">↻ Refresh</button>
      </div>

      <div id="cyg-psd-job-list"></div>
    `;
  }

  // ─── Hook showView ──────────────────────────────────────────────────────
  // We wrap (don't replace) the dashboard's showView. The original handles
  // every existing view; ours handles project-summary-document. Crucially,
  // the original's first line — querySelectorAll('.view').forEach(remove
  // 'active') — runs BEFORE we add our active class, so navigation away
  // from our view also works correctly without us touching it.
  function patchShowView() {
    const original = window.showView;
    window.showView = function (v) {
      if (typeof original === 'function') {
        original.apply(this, arguments);
      }
      if (v === VIEW_NAME) {
        // The original showView (or a stub) has already cleared .active from
        // every .view. We just need to add it to ours and render.
        const view = document.getElementById(VIEW_ID);
        if (view) {
          view.classList.add('active');
          // Mirror what showView does for known views
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

  // ─── Render the job list ────────────────────────────────────────────────
  function renderJobList() {
    const view = document.getElementById(VIEW_ID);
    if (!view) return;

    const list = view.querySelector('#cyg-psd-job-list');
    const filter = view.querySelector('#cyg-psd-project-filter');
    if (!list || !filter) return;

    const jobs = readJobs();
    const projects = readProjects();

    // Populate project filter (preserve current selection)
    const current = filter.value;
    filter.innerHTML = '<option value="">All projects</option>' +
      projects.map(p => `<option value="${attr(p.id)}">${esc(p.name || p.id)}</option>`).join('');
    filter.value = current;

    const selected = filter.value;
    const filtered = jobs.filter(j => !selected || j.projectId === selected);

    if (!filtered.length) {
      list.innerHTML = renderEmpty(jobs.length === 0);
      return;
    }

    list.innerHTML = filtered.map(renderRow).join('');

    list.querySelectorAll('[data-generate]').forEach(b =>
      b.addEventListener('click', () => window.CygenixPSD.generate(b.dataset.generate))
    );
    list.querySelectorAll('[data-preview]').forEach(b =>
      b.addEventListener('click', () => window.CygenixPSD.generate(b.dataset.preview, { openInTab: true }))
    );
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

    const isComplete = ['success','complete','completed'].includes(status);

    return `
      <div class="cyg-psd-row" style="display:flex;justify-content:space-between;align-items:center;gap:1rem;padding:1rem 1.25rem;background:var(--bg2);border:1px solid var(--border);border-radius:8px;margin-bottom:0.5rem;transition:border-color 0.15s">
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
          ${isComplete ? `
            <button class="btn btn-ghost btn-sm" data-preview="${attr(job.id)}" title="Preview in a new tab">👁 Preview</button>
            <button class="btn btn-primary btn-sm" data-generate="${attr(job.id)}" title="Generate and download as PDF">📄 Generate</button>
          ` : `
            <span style="font-size:12px;color:var(--text3);font-style:italic">Available once job completes</span>
          `}
        </div>
      </div>`;
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

  function buildUrl(jobId) {
    const fnUrl =
      readLocalString('cygenix_fn_url') ||
      readLocalString('cyg_fn_url') ||
      'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net';
    if (!fnUrl) return null;

    const fnKey = readLocalString('cygenix_fn_key') || '';
    const params = new URLSearchParams({ jobId });
    if (fnKey) params.set('code', fnKey);

    return `${fnUrl.replace(/\/$/, '')}${ENDPOINT}?${params.toString()}`;
  }

  function readLocalString(key) {
    try { const v = localStorage.getItem(key); return v ? String(v).trim() : ''; }
    catch { return ''; }
  }

  // ─── Iframe-based PDF generation ────────────────────────────────────────
  function generateInIframe(url) {
    const overlay = showOverlay();
    const iframe = document.createElement('iframe');
    iframe.style.cssText = 'position:fixed;width:0;height:0;border:0;visibility:hidden;';
    iframe.src = url;

    let printed = false;
    const cleanup = () => {
      if (overlay && overlay.parentNode) overlay.parentNode.removeChild(overlay);
      if (iframe && iframe.parentNode) iframe.parentNode.removeChild(iframe);
    };

    iframe.onload = function () {
      try {
        const doc = iframe.contentDocument;
        const looksLikeError = doc && doc.body &&
          doc.body.innerText && doc.body.innerText.indexOf('"error"') === 0;
        if (looksLikeError) throw new Error(doc.body.innerText.slice(0, 300));

        setTimeout(() => {
          try {
            iframe.contentWindow.focus();
            iframe.contentWindow.print();
            printed = true;
          } catch (e) {
            console.warn('[CygenixPSD] iframe print blocked, opening in tab', e);
            window.open(iframe.src, '_blank', 'noopener');
          } finally {
            setTimeout(cleanup, 1000);
          }
        }, 250);
      } catch (err) {
        console.warn('[CygenixPSD] falling back to new tab:', err);
        toast('Opening document in new tab\u2026', 'info');
        window.open(url, '_blank', 'noopener');
        cleanup();
      }
    };

    iframe.onerror = function () {
      toast('Couldn\u2019t generate document. Check Function is running.', 'error');
      cleanup();
    };

    document.body.appendChild(iframe);

    setTimeout(() => {
      if (!printed && iframe.parentNode) {
        window.open(url, '_blank', 'noopener');
        cleanup();
      }
    }, PRINT_TIMEOUT_MS);
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

  function toast(msg, kind) {
    if (window.CygenixToast && window.CygenixToast.show) {
      window.CygenixToast.show(msg, kind || 'info'); return;
    }
    const el = document.createElement('div');
    el.style.cssText = `
      position:fixed;bottom:24px;left:50%;transform:translateX(-50%);
      background:${kind === 'error' ? '#ef4444' : '#1a1d24'};color:#fff;
      padding:10px 18px;border-radius:6px;font-family:Inter,sans-serif;
      font-size:13px;z-index:10000;box-shadow:0 8px 24px rgba(0,0,0,0.3);transition:opacity 0.3s;
    `;
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => { el.style.opacity = '0'; setTimeout(() => el.remove(), 300); }, 3500);
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
    `;
    document.head.appendChild(s);
  }
})();
