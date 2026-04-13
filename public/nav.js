// ── Cygenix Global Navigation ─────────────────────────────────────────────────
// Injects the sidebar nav into every page automatically.
// Usage: <script src="/nav.js"></script> in <head> — runs after DOM ready.

(function() {
  'use strict';

  const NAV_CSS = `
  .cyg-nav-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:198}
  .cyg-nav-overlay.open{display:block}
  .cyg-sidebar{
    position:fixed;top:0;left:0;bottom:0;width:220px;
    background:#13161d;border-right:0.5px solid rgba(255,255,255,0.07);
    z-index:199;display:flex;flex-direction:column;overflow:hidden;
    transition:transform 0.25s ease;
    font-family:'Syne',system-ui,sans-serif;
    -webkit-font-smoothing:antialiased;
  }
  .cyg-sidebar.hidden{transform:translateX(-220px)}
  body.nav-open .cyg-sidebar{transform:translateX(0)}
  .cyg-sidebar-inner{flex:1;overflow-y:auto;padding:0.75rem 0 1rem;overscroll-behavior:contain}
  .cyg-sidebar-inner::-webkit-scrollbar{width:3px}
  .cyg-sidebar-inner::-webkit-scrollbar-thumb{background:rgba(255,255,255,0.08);border-radius:2px}
  .cyg-nav-logo{padding:0.75rem 1.25rem 0.5rem;display:flex;align-items:center;justify-content:space-between;border-bottom:0.5px solid rgba(255,255,255,0.07);flex-shrink:0}
  .cyg-nav-logo a{font-size:1.05rem;font-weight:700;color:#e8eaf0;text-decoration:none;letter-spacing:-0.02em}
  .cyg-nav-logo a span{color:#3d7eff}
  .cyg-nav-section{margin-bottom:0.25rem}
  .cyg-nav-label{font-size:9px;font-weight:600;color:#555a6a;text-transform:uppercase;letter-spacing:0.1em;padding:0.75rem 1.25rem 0.25rem;display:block}
  .cyg-nav-item{display:flex;align-items:center;gap:9px;padding:0.42rem 1.25rem;font-size:12.5px;color:#8b90a0;cursor:pointer;transition:all 0.12s;border-left:2px solid transparent;text-decoration:none;white-space:nowrap;overflow:hidden}
  .cyg-nav-item:hover{color:#e8eaf0;background:rgba(255,255,255,0.03)}
  .cyg-nav-item.active{color:#3d7eff;background:rgba(61,126,255,0.08);border-left-color:#3d7eff}
  .cyg-nav-item svg{width:13px;height:13px;opacity:0.65;flex-shrink:0}
  .cyg-nav-item.active svg{opacity:1}
  .cyg-nav-count{margin-left:auto;background:#1a1e28;color:#555a6a;font-size:9px;padding:1px 5px;border-radius:100px;font-family:'IBM Plex Mono',monospace}
  .cyg-sub-label{font-size:9px;font-weight:600;color:#555a6a;padding:0.5rem 1.25rem 0.2rem 1.1rem;text-transform:uppercase;letter-spacing:0.08em;display:flex;align-items:center;gap:0.35rem;cursor:pointer;user-select:none;transition:color 0.12s}
  .cyg-sub-label:hover{color:#8b90a0}
  .cyg-sub-label .cyg-chev{font-size:7px;transition:transform 0.2s;margin-left:auto;color:#555a6a}
  .cyg-sub-label.open .cyg-chev{transform:rotate(90deg)}
  .cyg-sub{overflow:hidden;transition:max-height 0.22s ease}
  .cyg-sub.collapsed{max-height:0!important}
  .cyg-sub .cyg-nav-item{padding-left:2.1rem;font-size:12px}
  .cyg-sidebar-footer{padding:0.75rem 0;border-top:0.5px solid rgba(255,255,255,0.07);flex-shrink:0}
  .cyg-conn-bar{padding:0.4rem 1rem;display:flex;align-items:center;gap:0.4rem;font-size:10px;font-family:'IBM Plex Mono',monospace;color:#555a6a;flex-wrap:wrap;row-gap:2px}
  .cyg-conn-dot{width:6px;height:6px;border-radius:50%;flex-shrink:0;background:#f04646}
  /* Push page content right of nav */
  body.cyg-nav-active{padding-left:220px}
  body.cyg-nav-active .topbar{left:220px;right:0}
  @media(max-width:768px){
    body.cyg-nav-active{padding-left:0}
    body.cyg-nav-active .topbar{left:0}
    .cyg-sidebar{transform:translateX(-220px)}
    body.nav-open .cyg-sidebar{transform:translateX(0)}
  }
  `;

  // Detect current page for active state
  const page = window.location.pathname.split('/').pop() || 'index.html';

  function isActive(href) {
    return page === href ? 'cyg-nav-item active' : 'cyg-nav-item';
  }

  function jobCount() {
    try { return JSON.parse(localStorage.getItem('cygenix_jobs')||'[]').length; } catch { return 0; }
  }

  function connStatus() {
    if (typeof CygenixConnections === 'undefined') return { srcOk: false, tgtOk: false };
    const c = CygenixConnections.get();
    return { srcOk: !!c.srcConnString, tgtOk: !!(c.tgtFnUrl || c.tgtConnString) };
  }

  function toggleSub(id) {
    const sub   = document.getElementById('cyg-sub-' + id);
    const label = document.getElementById('cyg-lbl-' + id);
    if (!sub || !label) return;
    const open = !sub.classList.contains('collapsed');
    sub.classList.toggle('collapsed', open);
    label.classList.toggle('open', !open);
  }

  function buildNav() {
    const { srcOk, tgtOk } = connStatus();
    const jobs = jobCount();
    const connAll = srcOk && tgtOk;
    const connDot = `<span class="cyg-conn-dot" style="background:${connAll?'#22c97a':srcOk||tgtOk?'#f59e0b':'#f04646'}"></span>`;

    // Determine which sub-menus should start open
    // Both sub-menus always pinned open
    const configOpen = 'open';
    const configCollapsed = '';
    const mappingOpen = 'open';
    const mappingCollapsed = '';
    const analysisOpen = ['insights.html','validation.html'].includes(page) ? 'open' : 'open';
    const analysisCollapsed = '';
    const reportsOpen = ['issues.html'].includes(page) ? 'open' : 'open';
    const reportsCollapsed = '';

    return `
<div class="cyg-nav-logo">
  <a href="/dashboard.html">Cyge<span>nix</span></a>
</div>
<div class="cyg-sidebar-inner">

<div class="cyg-nav-section">
  <span class="cyg-nav-label">Migrate</span>
  <a class="${isActive('dashboard.html')}" href="/dashboard.html">
    <svg viewBox="0 0 16 16" fill="none"><rect x="1" y="1" width="6" height="6" rx="1.5" stroke="currentColor" stroke-width="1.2"/><rect x="9" y="1" width="6" height="6" rx="1.5" stroke="currentColor" stroke-width="1.2"/><rect x="1" y="9" width="6" height="6" rx="1.5" stroke="currentColor" stroke-width="1.2"/><rect x="9" y="9" width="6" height="6" rx="1.5" stroke="currentColor" stroke-width="1.2"/></svg>
    Dashboard
  </a>

  <!-- Configure sub-menu -->
  <div class="cyg-sub-label ${configOpen}" id="cyg-lbl-configure" onclick="cygToggleSub('configure')">
    <svg viewBox="0 0 12 12" fill="none" style="width:9px;height:9px"><path d="M6 1v1M6 10v1M1 6h1M10 6h1M2.5 2.5l.7.7M8.8 8.8l.7.7M2.5 9.5l.7-.7M8.8 3.2l.7-.7" stroke="currentColor" stroke-width="1.1" stroke-linecap="round"/><circle cx="6" cy="6" r="2" stroke="currentColor" stroke-width="1.1"/></svg>
    Configure
    <span class="cyg-chev">▶</span>
  </div>
  <div class="cyg-sub ${configCollapsed}" id="cyg-sub-configure" style="max-height:${configOpen?'200px':'0'}">
    <a class="${isActive('dashboard.html#project-settings')||page==='dashboard.html'?'cyg-nav-item':'cyg-nav-item'}" href="/dashboard.html" onclick="sessionStorage.setItem('cyg_goto','project-settings')" style="color:#f59e0b">
      <svg viewBox="0 0 16 16" fill="none"><rect x="2" y="2" width="12" height="12" rx="2" stroke="currentColor" stroke-width="1.2"/><path d="M5 6h6M5 9h4" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>
      Project Settings
    </a>
    <a class="cyg-nav-item" href="/dashboard.html" onclick="sessionStorage.setItem('cyg_goto','connections')" style="color:#22c97a">
      <svg viewBox="0 0 16 16" fill="none"><circle cx="4" cy="8" r="2.5" stroke="currentColor" stroke-width="1.2"/><circle cx="12" cy="8" r="2.5" stroke="currentColor" stroke-width="1.2"/><path d="M6.5 8h3" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>
      Connections ${connDot}
    </a>
    <a class="cyg-nav-item" href="/dashboard.html" onclick="sessionStorage.setItem('cyg_goto','backup')" style="color:#a78bfa">
      <svg viewBox="0 0 16 16" fill="none"><path d="M8 2v8M5 7l3 3 3-3" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/><rect x="2" y="11" width="12" height="3" rx="1" stroke="currentColor" stroke-width="1.1"/></svg>
      Backup &amp; Restore
    </a>
    <a class="${isActive('performance.html')}" href="/performance.html" style="color:#2dd4bf">
      <svg viewBox="0 0 16 16" fill="none"><path d="M2 12 L5 7 L8 9 L11 4 L14 6" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/><circle cx="14" cy="6" r="1.5" fill="currentColor"/></svg>
      Performance
    </a>
  </div>

  <!-- Develop sub-menu -->
  <div class="cyg-sub-label ${mappingOpen}" id="cyg-lbl-mapping" onclick="cygToggleSub('mapping')">
    <svg viewBox="0 0 12 12" fill="none" style="width:9px;height:9px"><path d="M1 3h4M1 6h4M1 9h4M7 3h4M7 6h4M7 9h4" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>
    Develop
    <span class="cyg-chev">▶</span>
  </div>
  <div class="cyg-sub ${mappingCollapsed}" id="cyg-sub-mapping" style="max-height:${mappingOpen?'200px':'0'}">
    <a class="${isActive('mapper.html')}" href="/mapper.html" style="color:#2dd4bf">
      <svg viewBox="0 0 16 16" fill="none"><path d="M2 4h5M2 8h5M2 12h5" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/><path d="M9 4h5M9 8h5M9 12h5" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/><path d="M7 4l2 4-2 4" stroke="currentColor" stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round"/></svg>
      Mapper
    </a>
    <a class="${isActive('sql-editor.html')}" href="/sql-editor.html" style="color:#2dd4bf">
      <svg viewBox="0 0 16 16" fill="none"><rect x="1" y="2" width="14" height="12" rx="2" stroke="currentColor" stroke-width="1.2"/><path d="M4 6l3 2-3 2M9 10h3" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
      SQL Editor
    </a>
  </div>

  <!-- Analysis sub-menu -->
  <div class="cyg-sub-label ${analysisOpen}" id="cyg-lbl-analysis" onclick="cygToggleSub('analysis')">
    <svg viewBox="0 0 12 12" fill="none" style="width:9px;height:9px"><path d="M1 9l3-3 2 2 3-4 2 2" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
    Analysis
    <span class="cyg-chev">▶</span>
  </div>
  <div class="cyg-sub ${analysisCollapsed}" id="cyg-sub-analysis" style="max-height:${analysisOpen?'160px':'0'}">
    <a class="${isActive('insights.html')}" href="/insights.html" style="color:#a78bfa">
      <svg viewBox="0 0 16 16" fill="none"><circle cx="8" cy="4" r="2.5" stroke="currentColor" stroke-width="1.2"/><path d="M3 14c0-2.76 2.24-5 5-5s5 2.24 5 5" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>
      Data Insights
    </a>
    <a class="${isActive('validation.html')}" href="/validation.html" style="color:#f59e0b">
      <svg viewBox="0 0 16 16" fill="none"><circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="1.2"/><path d="M8 5v3.5L10 10" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>
      Validation
    </a>
  </div>

  <a class="${isActive('project-builder.html')}" href="/project-builder.html" style="color:#a78bfa">
    <svg viewBox="0 0 16 16" fill="none"><rect x="1" y="2" width="14" height="12" rx="2" stroke="currentColor" stroke-width="1.2"/><path d="M4 6h8M4 9h5" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/><circle cx="12" cy="10" r="2.5" fill="#13161d" stroke="currentColor" stroke-width="1.1"/><path d="M11.3 10l.5.5.9-.9" stroke="currentColor" stroke-width="1" stroke-linecap="round" stroke-linejoin="round"/></svg>
    Execute Jobs
  </a>
  <a class="cyg-nav-item" href="/dashboard.html" onclick="sessionStorage.setItem('cyg_goto','jobs')">
    <svg viewBox="0 0 16 16" fill="none"><path d="M2 4h12M2 8h12M2 12h8" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>
    All Jobs
    ${jobs ? `<span class="cyg-nav-count">${jobs}</span>` : ''}
  </a>
  <a class="cyg-nav-item" href="/dashboard.html" onclick="sessionStorage.setItem('cyg_goto','inventory')">
    <svg viewBox="0 0 16 16" fill="none"><path d="M2 3h12v2H2zM2 7h12v2H2zM2 11h8v2H2z" stroke="currentColor" stroke-width="1.1" fill="none"/></svg>
    Inventory
  </a>
</div>

<div class="cyg-nav-section">
  <span class="cyg-nav-label">Outputs</span>

  <!-- Reports sub-menu -->
  <div class="cyg-sub-label ${reportsOpen}" id="cyg-lbl-reports" onclick="cygToggleSub('reports')">
    <svg viewBox="0 0 12 12" fill="none" style="width:9px;height:9px"><rect x="1.5" y="1" width="9" height="10" rx="1.5" stroke="currentColor" stroke-width="1.1"/><path d="M3.5 4h5M3.5 6h5M3.5 8h3" stroke="currentColor" stroke-width="1" stroke-linecap="round"/></svg>
    Reports
    <span class="cyg-chev">▶</span>
  </div>
  <div class="cyg-sub ${reportsCollapsed}" id="cyg-sub-reports" style="max-height:${reportsOpen?'160px':'0'}">
    <a class="cyg-nav-item" href="/dashboard.html" onclick="sessionStorage.setItem('cyg_goto','reports')" style="color:#a78bfa">
      <svg viewBox="0 0 16 16" fill="none"><rect x="2" y="1" width="12" height="14" rx="2" stroke="currentColor" stroke-width="1.2"/><path d="M5 5h6M5 8h6M5 11h3" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>
      Conversion Report
    </a>
    <a class="${isActive('issues.html')}" href="/issues.html" style="color:#f04646">
      <svg viewBox="0 0 16 16" fill="none"><circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="1.2"/><path d="M8 5v4M8 11v.5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>
      Issue Management
    </a>
  </div>

  <a class="cyg-nav-item" href="/dashboard.html" onclick="sessionStorage.setItem('cyg_goto','audit')">
    <svg viewBox="0 0 16 16" fill="none"><rect x="2" y="1.5" width="12" height="13" rx="1.5" stroke="currentColor" stroke-width="1.2"/><path d="M5 5.5h6M5 8h6M5 10.5h3" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>
    Audit Log
  </a>
</div>

</div>

<div class="cyg-sidebar-footer">
  <div class="cyg-conn-bar">
    <span class="cyg-conn-dot" id="cyg-src-dot" style="background:${srcOk?'#22c97a':'#f04646'}"></span>
    <span style="overflow:hidden;text-overflow:ellipsis;max-width:80px">${srcOk?'Src ✓':'Src —'}</span>
    <span class="cyg-conn-dot" id="cyg-tgt-dot" style="background:${tgtOk?'#22c97a':'#f04646'}"></span>
    <span>${tgtOk?'Tgt ✓':'Tgt —'}</span>
  </div>
  <a class="cyg-nav-item" href="/help.html" target="_blank" style="font-size:11px;color:#3d7eff">
    <svg viewBox="0 0 16 16" fill="none"><circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="1.2"/><path d="M6.5 6c0-1 .75-1.5 1.5-1.5s1.5.5 1.5 1.5c0 .75-.5 1.25-1.5 1.5V9" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/><circle cx="8" cy="11" r="0.6" fill="currentColor"/></svg>
    Help & Guide
  </a>
</div>
`;
  }

  function cygToggleSub(id) {
    const sub   = document.getElementById('cyg-sub-' + id);
    const label = document.getElementById('cyg-lbl-' + id);
    if (!sub || !label) return;
    const open = !sub.classList.contains('collapsed');
    sub.classList.toggle('collapsed', open);
    label.classList.toggle('open', !open);
    if (!open) sub.style.maxHeight = '200px';
    else sub.style.maxHeight = '0';
  }
  window.cygToggleSub = cygToggleSub;

  function injectNav() {
    // Skip login, index, dashboard and project-builder (these have their own built-in sidebars)
    const skipPages = ['login.html','index.html','','dashboard.html','project-builder.html','dashboard'];
    if (skipPages.includes(page) || page === '' || document.getElementById('cyg-sidebar')) return;

    // Inject CSS
    const style = document.createElement('style');
    style.textContent = NAV_CSS;
    document.head.appendChild(style);

    // Create sidebar
    const sidebar = document.createElement('div');
    sidebar.className = 'cyg-sidebar';
    sidebar.id = 'cyg-sidebar';
    sidebar.innerHTML = buildNav();
    document.body.insertBefore(sidebar, document.body.firstChild);

    // Create overlay for mobile
    const overlay = document.createElement('div');
    overlay.className = 'cyg-nav-overlay';
    overlay.id = 'cyg-overlay';
    overlay.onclick = () => document.body.classList.remove('nav-open');
    document.body.insertBefore(overlay, document.body.firstChild);

    // Push content right
    document.body.classList.add('cyg-nav-active');

    // Handle cyg_goto — navigate to dashboard sub-view
    const goto = sessionStorage.getItem('cyg_goto');
    if (goto && page === 'dashboard.html' && typeof showView === 'function') {
      sessionStorage.removeItem('cyg_goto');
      setTimeout(() => showView(goto), 100);
    }

    // Update connection dots live
    setTimeout(() => {
      const { srcOk, tgtOk } = connStatus();
      const sd = document.getElementById('cyg-src-dot');
      const td = document.getElementById('cyg-tgt-dot');
      if (sd) sd.style.background = srcOk ? '#22c97a' : '#f04646';
      if (td) td.style.background = tgtOk ? '#22c97a' : '#f04646';
    }, 500);
  }

  // Run after DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', injectNav);
  } else {
    injectNav();
  }
})();
