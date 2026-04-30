/* cygenix-project-summary.js — Project Summary Document feature for the
 * Cygenix dashboard. SELF-CONTAINED: injects its own dashboard view markup,
 * registers itself as a view handler, and handles document generation.
 *
 * Drop this file in next to dashboard.html and add ONE line near the bottom
 * of dashboard.html:
 *
 *     <script src="/cygenix-project-summary.js"></script>
 *
 * The sidebar already routes `view: 'project-summary-document'` here (see
 * the matching nav item in cygenix-sidebar.js).
 *
 * What this module does on load:
 *   1. Injects a <section id="view-project-summary-document"> into the
 *      dashboard's main content area, hidden by default.
 *   2. Patches window.showView so navigating to 'project-summary-document'
 *      shows the new view (and hides the others).
 *   3. Reads completed jobs from the same localStorage keys the rest of
 *      Cygenix already uses, and renders them as a list with a "Generate"
 *      button per row.
 *   4. On click, fetches the styled HTML from the Azure Function endpoint
 *      and prints it via a hidden iframe (browser "Save as PDF" dialog).
 *
 * Data sources:
 *   - localStorage 'cygenix_jobs'           (list of all jobs)
 *   - localStorage 'cygenix_active_project' (filter by current project)
 *   - localStorage 'cygenix_fn_url'         (Azure Function base URL)
 *   - localStorage 'cygenix_fn_key'         (optional function key)
 *
 * Falls back gracefully if any are missing — empty state with helpful copy.
 *
 * In-band debugging (per Curtis's Azure plan constraint where Application
 * Insights, Live Log Stream, and Kudu are unavailable): all fetch errors
 * surface the response body to the toast/console so you can see what went
 * wrong from the browser DevTools alone.
 */

(function () {
  'use strict';

  // ───────────────────────────────────────────────────────────────────────
  // Config
  // ───────────────────────────────────────────────────────────────────────
  const VIEW_ID = 'view-project-summary-document';
  const VIEW_NAME = 'project-summary-document';
  const ENDPOINT_PATH = '/api/project-summary-document';
  const PRINT_TIMEOUT_MS = 20000;

  // ───────────────────────────────────────────────────────────────────────
  // Public API: window.CygenixPSD
  // ───────────────────────────────────────────────────────────────────────
  window.CygenixPSD = {
    /**
     * Generate and print the Project Summary Document for a given job.
     * @param {string} jobId
     * @param {Object} [opts]
     * @param {boolean} [opts.openInTab=false] — open in new tab instead of auto-print
     */
    generate: function (jobId, opts) {
      opts = opts || {};
      if (!jobId) {
        toast('No job selected', 'error');
        return;
      }
      const url = buildUrl(jobId);
      if (!url) {
        toast('Azure Function URL not configured. Open Settings → Connections.', 'error');
        return;
      }
      if (opts.openInTab) {
        window.open(url, '_blank', 'noopener');
        return;
      }
      generateInIframe(url, jobId);
    },

    /** Re-render the view. Useful after job data changes. */
    refresh: renderView,

    /** Force-show the view (navigation usually does this for you). */
    show: showView,
  };

  // ───────────────────────────────────────────────────────────────────────
  // Bootstrap
  // ───────────────────────────────────────────────────────────────────────
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  function init() {
    injectStyles();
    injectViewMarkup();
    patchShowView();

    // If the dashboard was loaded with #goto=project-summary-document
    // (e.g. user clicked the sidebar from another page), open the view now.
    if (location.hash.indexOf('goto=' + VIEW_NAME) !== -1) {
      // Defer so the dashboard's own showView setup runs first.
      setTimeout(showView, 50);
    }
  }

  // ───────────────────────────────────────────────────────────────────────
  // Inject the view's HTML into the dashboard
  // ───────────────────────────────────────────────────────────────────────
  function injectViewMarkup() {
    // If already injected (e.g. hot reload), skip.
    if (document.getElementById(VIEW_ID)) return;

    // Find a sensible parent — try common dashboard content selectors,
    // fall back to <main> or <body>.
    const parent =
      document.querySelector('.dashboard-content') ||
      document.querySelector('.cyg-content') ||
      document.querySelector('main') ||
      document.body;

    const section = document.createElement('section');
    section.id = VIEW_ID;
    section.className = 'cyg-view cyg-psd-view';
    section.style.display = 'none'; // hidden until showView('project-summary-document')
    section.innerHTML = renderShell();
    parent.appendChild(section);

    // Wire up the static buttons (refresh, project filter)
    wireShellControls(section);
  }

  function renderShell() {
    return `
      <div class="cyg-psd-header">
        <div>
          <h1 class="cyg-psd-title">Project Summary Document</h1>
          <p class="cyg-psd-sub">
            Generate a polished, sign-off-ready PDF for any completed migration job.
            Includes KPI summary, transformations, reconciliation, run timeline, and a sign-off page.
          </p>
        </div>
        <div class="cyg-psd-controls">
          <select id="cyg-psd-project-filter" class="cyg-psd-select">
            <option value="">All projects</option>
          </select>
          <button id="cyg-psd-refresh" class="cyg-psd-btn cyg-psd-btn-secondary" title="Reload job list">
            ↻ Refresh
          </button>
        </div>
      </div>

      <div class="cyg-psd-info-banner">
        <strong>Tip:</strong>
        Generated documents are produced on demand from your latest job data — they're not stored.
        Click <em>Generate</em> on any job below to create and download a fresh copy.
      </div>

      <div id="cyg-psd-job-list" class="cyg-psd-job-list">
        <!-- populated by renderView() -->
      </div>
    `;
  }

  function wireShellControls(section) {
    const refreshBtn = section.querySelector('#cyg-psd-refresh');
    if (refreshBtn) refreshBtn.addEventListener('click', renderView);

    const filter = section.querySelector('#cyg-psd-project-filter');
    if (filter) filter.addEventListener('change', renderView);
  }

  // ───────────────────────────────────────────────────────────────────────
  // Patch dashboard's showView so it knows about us
  // ───────────────────────────────────────────────────────────────────────
  function patchShowView() {
    const original = window.showView;

    window.showView = function (name) {
      if (name === VIEW_NAME) {
        showView();
        return;
      }
      if (typeof original === 'function') {
        original.apply(this, arguments);
      }
    };
  }

  function showView() {
    // Hide every other top-level dashboard view, show ours.
    // Different dashboards use different conventions — try a few selectors.
    const selectors = ['.cyg-view', '[id^="view-"]', '.dashboard-view', '.view'];
    selectors.forEach(sel => {
      document.querySelectorAll(sel).forEach(el => {
        if (el.id !== VIEW_ID) el.style.display = 'none';
      });
    });

    const view = document.getElementById(VIEW_ID);
    if (view) view.style.display = '';

    if (window.CygenixSidebar && typeof window.CygenixSidebar.setActive === 'function') {
      window.CygenixSidebar.setActive(VIEW_NAME);
    }

    renderView();
  }

  // ───────────────────────────────────────────────────────────────────────
  // Render the job list inside the view
  // ───────────────────────────────────────────────────────────────────────
  function renderView() {
    const view = document.getElementById(VIEW_ID);
    if (!view) return;

    const list = view.querySelector('#cyg-psd-job-list');
    const filter = view.querySelector('#cyg-psd-project-filter');
    if (!list || !filter) return;

    const jobs = readJobs();
    const projects = readProjects();

    // Populate the project filter dropdown (preserve current selection).
    const currentValue = filter.value;
    filter.innerHTML = '<option value="">All projects</option>' +
      projects.map(p => `<option value="${attr(p.id)}">${escapeHtml(p.name || p.id)}</option>`).join('');
    filter.value = currentValue;

    const selectedProject = filter.value;
    const filtered = jobs.filter(j => {
      if (!selectedProject) return true;
      return j.projectId === selectedProject;
    });

    if (!filtered.length) {
      list.innerHTML = renderEmptyState(jobs.length === 0);
      return;
    }

    list.innerHTML = filtered.map(renderJobRow).join('');

    // Wire each row's Generate button.
    list.querySelectorAll('[data-generate]').forEach(btn => {
      btn.addEventListener('click', () => {
        window.CygenixPSD.generate(btn.dataset.generate);
      });
    });
    list.querySelectorAll('[data-preview]').forEach(btn => {
      btn.addEventListener('click', () => {
        window.CygenixPSD.generate(btn.dataset.preview, { openInTab: true });
      });
    });
  }

  function renderEmptyState(noJobsAtAll) {
    if (noJobsAtAll) {
      return `
        <div class="cyg-psd-empty">
          <div class="cyg-psd-empty-icon">📄</div>
          <h3>No jobs yet</h3>
          <p>Once you've run a migration job, it'll appear here and you can generate a Project Summary Document for it.</p>
          <p style="margin-top:1rem">
            <a href="/project-builder.html" class="cyg-psd-link">Go to Execute →</a>
          </p>
        </div>
      `;
    }
    return `
      <div class="cyg-psd-empty">
        <h3>No jobs match this project</h3>
        <p>Try clearing the project filter above, or pick a different one.</p>
      </div>
    `;
  }

  function renderJobRow(job) {
    const status = (job.status || 'pending').toLowerCase();
    const statusClass = {
      success: 'success',
      complete: 'success',
      completed: 'success',
      failed: 'danger',
      error: 'danger',
      running: 'info',
      pending: 'neutral',
    }[status] || 'neutral';

    const lastRun = job.runCompletedAt || job.lastRun || job.updatedAt || job.createdAt;
    const lastRunLabel = lastRun ? formatDate(lastRun) : '—';

    const tableCount = (job.tables && job.tables.length) || 0;
    const rowCount = (job.tables || []).reduce((s, t) => s + (t.targetRows || t.sourceRows || 0), 0);

    const isComplete = ['success', 'complete', 'completed'].includes(status);

    return `
      <div class="cyg-psd-job-row">
        <div class="cyg-psd-job-main">
          <div class="cyg-psd-job-name">
            ${escapeHtml(job.name || job.id || 'Untitled job')}
            <span class="cyg-psd-status-badge cyg-psd-status-${statusClass}">${escapeHtml(status)}</span>
          </div>
          <div class="cyg-psd-job-meta">
            <span title="Job ID"><code>${escapeHtml(job.id || '—')}</code></span>
            ${job.version ? `<span>Version ${escapeHtml(job.version)}</span>` : ''}
            ${tableCount ? `<span>${tableCount} table${tableCount === 1 ? '' : 's'}</span>` : ''}
            ${rowCount ? `<span>${formatCompact(rowCount)} rows</span>` : ''}
            <span>Last run: ${escapeHtml(lastRunLabel)}</span>
          </div>
        </div>
        <div class="cyg-psd-job-actions">
          ${isComplete ? `
            <button class="cyg-psd-btn cyg-psd-btn-secondary" data-preview="${attr(job.id)}" title="Preview in a new tab">
              👁 Preview
            </button>
            <button class="cyg-psd-btn cyg-psd-btn-primary" data-generate="${attr(job.id)}" title="Generate and download as PDF">
              📄 Generate
            </button>
          ` : `
            <span class="cyg-psd-disabled-hint">Available once job completes</span>
          `}
        </div>
      </div>
    `;
  }

  // ───────────────────────────────────────────────────────────────────────
  // Data access — reads from the same keys the rest of Cygenix uses
  // ───────────────────────────────────────────────────────────────────────
  function readJobs() {
    // Try a couple of known keys; first one with valid data wins.
    const keys = ['cygenix_jobs', 'cygenix_all_jobs', 'cyg_jobs'];
    for (const k of keys) {
      try {
        const raw = localStorage.getItem(k);
        if (!raw) continue;
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed) && parsed.length) return parsed;
        // Some pages store as { jobs: [...] }
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
    // Read the configured Azure Function base from the same place Connections saves it.
    const fnUrl =
      readLocalString('cygenix_fn_url') ||
      readLocalString('cyg_fn_url') ||
      // Hard-coded fallback: Curtis's known Function host.
      'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net';

    if (!fnUrl) return null;

    const fnKey = readLocalString('cygenix_fn_key') || '';
    const params = new URLSearchParams({ jobId: jobId });
    if (fnKey) params.set('code', fnKey);

    // Strip trailing slash for tidy concat.
    const base = fnUrl.replace(/\/$/, '');
    return `${base}${ENDPOINT_PATH}?${params.toString()}`;
  }

  function readLocalString(key) {
    try {
      const v = localStorage.getItem(key);
      return v ? String(v).trim() : '';
    } catch {
      return '';
    }
  }

  // ───────────────────────────────────────────────────────────────────────
  // Iframe-based PDF generation
  // ───────────────────────────────────────────────────────────────────────
  function generateInIframe(url, jobId) {
    const overlay = showLoadingOverlay();

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
        // Same-origin policy: this works because the dashboard and the Function
        // are on different subdomains. If the iframe document isn't accessible,
        // we'll catch and fall back to opening in a new tab.
        const doc = iframe.contentDocument;
        const looksLikeError = doc && (doc.title.indexOf('Error') !== -1 ||
          (doc.body && doc.body.innerText && doc.body.innerText.indexOf('"error"') === 0));

        if (looksLikeError) {
          throw new Error(doc.body.innerText.slice(0, 300));
        }

        setTimeout(() => {
          try {
            iframe.contentWindow.focus();
            iframe.contentWindow.print();
            printed = true;
          } catch (printErr) {
            console.warn('[Cygenix PSD] iframe print blocked, opening in tab', printErr);
            window.open(iframe.src, '_blank', 'noopener');
          } finally {
            // Give the print dialog a moment before tearing down.
            setTimeout(cleanup, 1000);
          }
        }, 250);
      } catch (err) {
        // Same-origin fail OR error response from server — open in tab so
        // the user can see what's going on.
        console.warn('[Cygenix PSD] falling back to new tab:', err);
        toast('Opening document in new tab…', 'info');
        window.open(url, '_blank', 'noopener');
        cleanup();
      }
    };

    iframe.onerror = function () {
      toast('Couldn\u2019t generate document. Check your Azure Function is running.', 'error');
      cleanup();
    };

    document.body.appendChild(iframe);

    // Hard timeout — in case the Function is cold-starting and takes too long.
    setTimeout(() => {
      if (!printed && iframe.parentNode) {
        console.warn('[Cygenix PSD] timeout — opening in new tab');
        window.open(url, '_blank', 'noopener');
        cleanup();
      }
    }, PRINT_TIMEOUT_MS);
  }

  // ───────────────────────────────────────────────────────────────────────
  // UI helpers
  // ───────────────────────────────────────────────────────────────────────
  function showLoadingOverlay() {
    const el = document.createElement('div');
    el.className = 'cyg-psd-overlay';
    el.innerHTML = `
      <div class="cyg-psd-overlay-inner">
        <div class="cyg-psd-spinner"></div>
        <div class="cyg-psd-overlay-title">Generating Project Summary Document</div>
        <div class="cyg-psd-overlay-sub">Pulling job, audit log, and reconciliation data\u2026</div>
      </div>
    `;
    document.body.appendChild(el);
    return el;
  }

  function toast(msg, kind) {
    if (window.CygenixToast && typeof window.CygenixToast.show === 'function') {
      window.CygenixToast.show(msg, kind || 'info');
      return;
    }
    const el = document.createElement('div');
    el.className = 'cyg-psd-toast cyg-psd-toast-' + (kind || 'info');
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => {
      el.style.opacity = '0';
      setTimeout(() => el.remove(), 300);
    }, 3500);
  }

  // ───────────────────────────────────────────────────────────────────────
  // Formatting
  // ───────────────────────────────────────────────────────────────────────
  function escapeHtml(v) {
    if (v == null) return '';
    return String(v)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }
  function attr(v) { return escapeHtml(v); }

  function formatCompact(n) {
    if (n == null) return '—';
    if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
    if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return String(n);
  }

  function formatDate(iso) {
    if (!iso) return '—';
    try {
      const d = new Date(iso);
      if (isNaN(d.getTime())) return String(iso);
      return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' }) +
        ', ' + d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
    } catch {
      return String(iso);
    }
  }

  // ───────────────────────────────────────────────────────────────────────
  // Styles (scoped to .cyg-psd-* classes — won't affect anything else)
  // ───────────────────────────────────────────────────────────────────────
  function injectStyles() {
    if (document.getElementById('cyg-psd-styles')) return;
    const style = document.createElement('style');
    style.id = 'cyg-psd-styles';
    style.textContent = `
      .cyg-psd-view {
        padding: 2rem;
        max-width: 1200px;
        margin: 0 auto;
        color: var(--text, #e2e8f0);
        font-family: Inter, -apple-system, BlinkMacSystemFont, sans-serif;
      }
      .cyg-psd-header {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 2rem;
        margin-bottom: 1.5rem;
        flex-wrap: wrap;
      }
      .cyg-psd-title {
        font-size: 1.6rem;
        font-weight: 800;
        letter-spacing: -0.02em;
        margin: 0 0 0.25rem 0;
        color: var(--text, #e2e8f0);
      }
      .cyg-psd-sub {
        font-size: 0.9rem;
        color: var(--text2, #94a3b8);
        max-width: 60ch;
        margin: 0;
        line-height: 1.5;
      }
      .cyg-psd-controls {
        display: flex;
        gap: 0.5rem;
        align-items: center;
      }
      .cyg-psd-select, .cyg-psd-btn {
        background: var(--bg2, #1a1d24);
        color: var(--text, #e2e8f0);
        border: 1px solid var(--border, #2d3748);
        padding: 0.5rem 0.85rem;
        border-radius: 6px;
        font-size: 0.85rem;
        font-family: inherit;
        cursor: pointer;
        transition: border-color 0.15s, background 0.15s;
      }
      .cyg-psd-btn:hover { border-color: var(--accent, #6366f1); }
      .cyg-psd-btn-primary {
        background: var(--accent, #6366f1);
        border-color: var(--accent, #6366f1);
        color: #fff;
        font-weight: 600;
      }
      .cyg-psd-btn-primary:hover {
        background: var(--accent-hover, #5558e3);
        border-color: var(--accent-hover, #5558e3);
      }
      .cyg-psd-btn-secondary { font-weight: 500; }
      .cyg-psd-info-banner {
        background: var(--bg2, #1a1d24);
        border: 1px solid var(--border, #2d3748);
        border-left: 3px solid var(--accent, #6366f1);
        border-radius: 6px;
        padding: 0.75rem 1rem;
        margin-bottom: 1.5rem;
        font-size: 0.85rem;
        color: var(--text2, #94a3b8);
        line-height: 1.5;
      }
      .cyg-psd-info-banner strong { color: var(--text, #e2e8f0); }
      .cyg-psd-info-banner em { color: var(--accent, #6366f1); font-style: normal; font-weight: 600; }
      .cyg-psd-job-list {
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
      }
      .cyg-psd-job-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 1rem;
        padding: 1rem 1.25rem;
        background: var(--bg2, #1a1d24);
        border: 1px solid var(--border, #2d3748);
        border-radius: 8px;
        transition: border-color 0.15s, transform 0.15s;
      }
      .cyg-psd-job-row:hover {
        border-color: var(--accent, #6366f1);
      }
      .cyg-psd-job-main { flex: 1; min-width: 0; }
      .cyg-psd-job-name {
        font-size: 0.95rem;
        font-weight: 600;
        color: var(--text, #e2e8f0);
        margin-bottom: 0.35rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
        flex-wrap: wrap;
      }
      .cyg-psd-job-meta {
        font-size: 0.8rem;
        color: var(--text2, #94a3b8);
        display: flex;
        gap: 1rem;
        flex-wrap: wrap;
      }
      .cyg-psd-job-meta code {
        font-family: 'JetBrains Mono', 'SF Mono', Menlo, monospace;
        background: rgba(255, 255, 255, 0.05);
        padding: 0.1rem 0.4rem;
        border-radius: 3px;
        font-size: 0.75rem;
      }
      .cyg-psd-job-actions {
        display: flex;
        gap: 0.5rem;
        flex-shrink: 0;
      }
      .cyg-psd-status-badge {
        display: inline-block;
        padding: 0.1rem 0.5rem;
        border-radius: 3px;
        font-size: 0.65rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.05em;
      }
      .cyg-psd-status-success { background: rgba(16, 185, 129, 0.15); color: #10b981; }
      .cyg-psd-status-danger { background: rgba(239, 68, 68, 0.15); color: #ef4444; }
      .cyg-psd-status-info { background: rgba(99, 102, 241, 0.15); color: #6366f1; }
      .cyg-psd-status-neutral { background: rgba(148, 163, 184, 0.15); color: #94a3b8; }
      .cyg-psd-disabled-hint {
        font-size: 0.8rem;
        color: var(--text3, #64748b);
        font-style: italic;
      }
      .cyg-psd-empty {
        padding: 4rem 2rem;
        text-align: center;
        background: var(--bg2, #1a1d24);
        border: 1px dashed var(--border, #2d3748);
        border-radius: 8px;
        color: var(--text2, #94a3b8);
      }
      .cyg-psd-empty-icon { font-size: 2.5rem; margin-bottom: 1rem; }
      .cyg-psd-empty h3 {
        color: var(--text, #e2e8f0);
        margin: 0 0 0.5rem 0;
        font-size: 1.1rem;
      }
      .cyg-psd-empty p {
        margin: 0;
        font-size: 0.9rem;
        line-height: 1.5;
      }
      .cyg-psd-link {
        color: var(--accent, #6366f1);
        text-decoration: none;
        font-weight: 600;
      }
      .cyg-psd-link:hover { text-decoration: underline; }

      /* Loading overlay */
      .cyg-psd-overlay {
        position: fixed; inset: 0;
        background: rgba(10, 14, 26, 0.7);
        backdrop-filter: blur(4px);
        z-index: 9999;
        display: flex;
        align-items: center;
        justify-content: center;
        font-family: Inter, -apple-system, sans-serif;
        color: #fff;
      }
      .cyg-psd-overlay-inner { text-align: center; }
      .cyg-psd-spinner {
        width: 48px; height: 48px;
        margin: 0 auto 16px;
        border: 3px solid rgba(99, 102, 241, 0.3);
        border-top-color: #6366f1;
        border-radius: 50%;
        animation: cygPsdSpin 0.8s linear infinite;
      }
      @keyframes cygPsdSpin { to { transform: rotate(360deg); } }
      .cyg-psd-overlay-title {
        font-size: 15px; font-weight: 600; margin-bottom: 4px;
      }
      .cyg-psd-overlay-sub {
        font-size: 12px; color: #94a3b8;
      }

      /* Toast */
      .cyg-psd-toast {
        position: fixed;
        bottom: 24px; left: 50%;
        transform: translateX(-50%);
        background: #1a1d24;
        color: #fff;
        padding: 10px 18px;
        border-radius: 6px;
        font-family: Inter, sans-serif;
        font-size: 13px;
        z-index: 10000;
        box-shadow: 0 8px 24px rgba(0, 0, 0, 0.3);
        transition: opacity 0.3s;
      }
      .cyg-psd-toast-error { background: #ef4444; }
      .cyg-psd-toast-info { background: #1a1d24; border: 1px solid var(--accent, #6366f1); }
    `;
    document.head.appendChild(style);
  }
})();
