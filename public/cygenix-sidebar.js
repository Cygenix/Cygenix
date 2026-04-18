/* cygenix-sidebar.js — shared left nav for every Cygenix page.
 *
 * Usage on any page:
 *   <div id="cyg-sidebar-mount" data-active="sql-editor"></div>
 *   <script src="/cygenix-sidebar.js"></script>
 *
 * data-active values correspond to the nav item keys listed in NAV_ITEMS
 * below (e.g. "dashboard", "project-builder", "sql-editor", "system-parameters").
 *
 * The helper reads localStorage.cygenix_sidebar_collapsed BEFORE injecting
 * markup, so collapsed pages don't flash at full width.
 *
 * Cross-page navigation:
 *   - If the target is a page (href starts with `/`), navigates there.
 *   - If the target is a dashboard "view" (key), stashes cyg_goto in
 *     sessionStorage and navigates to /dashboard.html. Dashboard already
 *     picks this up and calls showView(name) on load.
 *   - If we're ON dashboard.html and the target is a view, calls
 *     window.showView(name) directly (no reload).
 */
(function(){
  'use strict';

  const STORAGE_KEY   = 'cygenix_sidebar_collapsed';
  const MOUNT_ID      = 'cyg-sidebar-mount';
  const ICON_BY_KEY   = {}; // populated from NAV_ITEMS
  const WIDTH_OPEN    = 230;
  const WIDTH_CLOSED  = 54;

  // ── Nav structure ───────────────────────────────────────────────────────
  // `key` is the identifier used for both `data-active` matching and dashboard showView.
  // `href` means it's a separate HTML page; `view` means it's a dashboard embedded view.
  // Only ONE of href/view is set per item.
  const NAV = [
    { section: 'Migrate', items: [
      { key:'dashboard', label:'Dashboard', view:'dashboard', icon: iconDashboard() }
    ]},
    { section: 'Configure', group:'configure', items: [
      { key:'project-settings',  label:'Project Settings',  view:'project-settings',   color:'var(--amber)',  icon: iconSettings() },
      { key:'connections',       label:'Connections',       view:'connections',        color:'var(--green)',  icon: iconPlug() },
      { key:'performance',       label:'Performance',       href:'/performance.html',  color:'var(--teal)',   icon: iconChart() },
      { key:'system-parameters', label:'System Parameters', view:'system-parameters',  color:'var(--accent)', icon: iconParams() },
      { key:'privacy-security',  label:'Governance',        view:'privacy-security',   color:'var(--red)',    icon: iconShield() },
      { key:'integrations',      label:'Integrations',      view:'integrations',       color:'var(--teal)',   icon: iconIntegrations() },
    ]},
    { section: 'Plan', group:'plan', items: [
      { key:'project-plan',      label:'Project Planner',   href:'/project-plan.html', color:'var(--green)',  icon: iconCalendar() },
    ]},
    { section: 'Develop', group:'develop', items: [
      { key:'object-mapping',    label:'Object Mapping',    href:'/object_mapping.html', color:'var(--teal)',   icon: iconArrows() },
      { key:'sql-editor',        label:'SQL Editor',        href:'/sql-editor.html',    color:'var(--teal)',   icon: iconCode() },
    ]},
    { section: 'Analysis', group:'analysis', items: [
      { key:'insights',          label:'Data Insights',     href:'/insights.html',      color:'var(--purple)', icon: iconInsights() },
      { key:'data-cleansing',    label:'Data Cleansing',    href:'/data-cleansing.html',color:'var(--teal)',   icon: iconClean() },
      { key:'validation',        label:'Validation',        href:'/validation.html',    color:'var(--amber)',  icon: iconCheck() },
    ]},
    { section: null, items: [
      { key:'project-builder',   label:'Project Execution', href:'/project-builder.html', color:'var(--purple)', icon: iconPlay() },
      { key:'jobs',              label:'All Jobs',          view:'jobs',                icon: iconList() },
      { key:'inventory',         label:'Inventory',         view:'inventory',           icon: iconGrid() },
    ]},
    { section: 'Outputs', group:'reports', items: [
      { key:'reports',           label:'Conversion Report', view:'reports',             color:'var(--purple)', icon: iconReport() },
    ]},
    { section: 'Service', items: [
      { key:'ask',               label:'Ask',               href:'/ask.html',           color:'var(--purple)', icon: iconAsk() },
    ]},
  ];

  // ── Icons (returns SVG string) ──────────────────────────────────────────
  function svg(body){ return '<svg class="cyg-nav-icon" viewBox="0 0 16 16" fill="none">'+body+'</svg>'; }
  function iconDashboard(){    return svg('<rect x="2" y="2" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="9" y="2" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="2" y="9" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="9" y="9" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/>'); }
  function iconSettings(){     return svg('<rect x="2" y="3" width="12" height="2" rx="0.5" fill="currentColor" opacity="0.3"/><rect x="2" y="7" width="12" height="2" rx="0.5" fill="currentColor" opacity="0.3"/><rect x="2" y="11" width="12" height="2" rx="0.5" fill="currentColor" opacity="0.3"/><circle cx="10" cy="4" r="1.3" fill="currentColor"/><circle cx="5" cy="8" r="1.3" fill="currentColor"/><circle cx="11" cy="12" r="1.3" fill="currentColor"/>'); }
  function iconPlug(){         return svg('<path d="M3 8h10" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/><circle cx="3.5" cy="8" r="1.5" stroke="currentColor" stroke-width="1.2"/><circle cx="12.5" cy="8" r="1.5" stroke="currentColor" stroke-width="1.2"/>'); }
  function iconDownload(){     return svg('<path d="M8 2v8m0 0L5 7m3 3l3-3M3 13h10" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconChart(){        return svg('<path d="M2 13l4-4 3 3 5-6" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconParams(){       return svg('<circle cx="8" cy="8" r="2" stroke="currentColor" stroke-width="1.2"/><path d="M8 1v2M8 13v2M1 8h2M13 8h2M3.5 3.5l1.5 1.5M11 11l1.5 1.5M3.5 12.5l1.5-1.5M11 5l1.5-1.5" stroke="currentColor" stroke-width="1.1" stroke-linecap="round"/>'); }
  function iconShield(){       return svg('<path d="M8 2 L3 4 V8 C3 11 5 13 8 14 C11 13 13 11 13 8 V4 Z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/><path d="M6 8l1.5 1.5L10.5 7" stroke="currentColor" stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconIntegrations(){ return svg('<circle cx="4" cy="4" r="2" stroke="currentColor" stroke-width="1.2"/><circle cx="12" cy="4" r="2" stroke="currentColor" stroke-width="1.2"/><circle cx="4" cy="12" r="2" stroke="currentColor" stroke-width="1.2"/><circle cx="12" cy="12" r="2" stroke="currentColor" stroke-width="1.2"/><path d="M6 4h4M6 12h4M4 6v4M12 6v4" stroke="currentColor" stroke-width="1.1" stroke-linecap="round"/>'); }
  function iconCalendar(){     return svg('<rect x="2" y="3" width="12" height="11" rx="1" stroke="currentColor" stroke-width="1.2"/><path d="M2 6h12M5 2v2M11 2v2" stroke="currentColor" stroke-width="1.2"/>'); }
  function iconArrows(){       return svg('<path d="M3 5h8l-2-2M13 11H5l2 2" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconCode(){         return svg('<rect x="2" y="3" width="12" height="10" rx="1" stroke="currentColor" stroke-width="1.2"/><path d="M6 7l-2 1 2 1M10 7l2 1-2 1" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconInsights(){     return svg('<circle cx="8" cy="6" r="3" stroke="currentColor" stroke-width="1.2"/><path d="M4 14c0-2 2-3 4-3s4 1 4 3" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/>'); }
  function iconClean(){        return svg('<path d="M4 2v4M4 10v4M2 6h4M2 12h4M10 3l3 3-6 6-3-3z" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconCheck(){        return svg('<circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="1.2"/><path d="M5.5 8l2 2 3-4" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconPlay(){         return svg('<rect x="2" y="3" width="12" height="10" rx="1" stroke="currentColor" stroke-width="1.2"/><path d="M7 6l3 2-3 2z" fill="currentColor"/>'); }
  function iconList(){         return svg('<path d="M2 4h12M2 8h12M2 12h12" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>'); }
  function iconGrid(){         return svg('<rect x="2" y="2" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="9" y="2" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="2" y="9" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="9" y="9" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/>'); }
  function iconReport(){       return svg('<path d="M4 2h6l3 3v9H4z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/><path d="M10 2v3h3M6 8h4M6 11h4" stroke="currentColor" stroke-width="1.1" stroke-linecap="round"/>'); }
  function iconAlert(){        return svg('<path d="M8 2l6 11H2z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/><path d="M8 6v3M8 11v0.5" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>'); }
  function iconAsk(){          return svg('<circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="1.2"/><path d="M6 6c0-1 1-2 2-2s2 1 2 2-1 2-2 2v1M8 11v0.5" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/>'); }

  // ── State ───────────────────────────────────────────────────────────────
  function isCollapsed(){
    try { return localStorage.getItem(STORAGE_KEY) === '1'; } catch { return false; }
  }
  function setCollapsed(on){
    try { localStorage.setItem(STORAGE_KEY, on ? '1' : '0'); } catch {}
  }

  // ── Style injection (once) ──────────────────────────────────────────────
  function injectStyles(){
    if (document.getElementById('cyg-sidebar-styles')) return;
    const style = document.createElement('style');
    style.id = 'cyg-sidebar-styles';
    style.textContent = `
      .cyg-sidebar{
        background:var(--bg2);
        border-right:0.5px solid var(--border);
        padding:0.5rem 0 1rem;
        width:${WIDTH_OPEN}px;
        display:flex;
        flex-direction:column;
        overflow:hidden;
        transition:width 0.2s ease;
        position:fixed;
        top:0;
        left:0;
        bottom:0;
        height:100vh;
        z-index:90;
      }
      .cyg-sidebar.collapsed{ width:${WIDTH_CLOSED}px; }

      .cyg-sidebar-head{
        display:flex;
        align-items:center;
        justify-content:flex-end;
        padding:0.25rem 0.6rem 0.5rem;
        flex-shrink:0;
      }
      .cyg-sidebar-toggle{
        background:none;border:none;cursor:pointer;
        color:var(--text3);
        padding:4px 7px;border-radius:6px;
        font-size:15px;line-height:1;
        transition:color 0.15s,background 0.15s;
      }
      .cyg-sidebar-toggle:hover{ color:var(--text); background:var(--bg3); }
      .cyg-sidebar.collapsed .cyg-sidebar-head{ justify-content:center; }

      .cyg-sidebar-scroll{
        flex:1 1 auto;
        overflow-y:auto;
        overflow-x:hidden;
        scrollbar-width:thin;
        scrollbar-color:var(--border2) transparent;
      }
      .cyg-sidebar-scroll::-webkit-scrollbar{ width:6px; }
      .cyg-sidebar-scroll::-webkit-scrollbar-thumb{ background:var(--border2); border-radius:3px; }
      .cyg-sidebar-scroll::-webkit-scrollbar-track{ background:transparent; }

      .cyg-nav-section{ margin-bottom:1.25rem; }
      .cyg-nav-label{
        font-size:10px;font-weight:500;color:var(--text3);
        text-transform:uppercase;letter-spacing:0.1em;
        padding:0 1.25rem;margin-bottom:0.35rem;
      }
      .cyg-nav-sub-label{
        font-size:10px;font-weight:600;color:var(--text3);
        padding:0.4rem 1.25rem 0.2rem 1.5rem;
        text-transform:uppercase;letter-spacing:0.07em;
        display:flex;align-items:center;gap:0.4rem;
        cursor:pointer;user-select:none;transition:color 0.15s;
      }
      .cyg-nav-sub-label:hover{ color:var(--text2); }
      .cyg-nav-sub-label .cyg-chevron{ font-size:8px;transition:transform 0.2s;color:var(--text3); }
      .cyg-nav-sub-label.open .cyg-chevron{ transform:rotate(90deg); }
      .cyg-nav-sub{ overflow:hidden;transition:max-height 0.25s ease;max-height:300px; }
      .cyg-nav-sub.collapsed{ max-height:0 !important; }
      .cyg-nav-sub .cyg-nav-item{ padding-left:2rem;font-size:12px; }

      .cyg-nav-item{
        display:flex;align-items:center;gap:10px;
        padding:0.5rem 1.25rem;
        font-size:13px;color:var(--text2);
        cursor:pointer;
        transition:color 0.15s,background 0.15s;
        border-left:2px solid transparent;
        user-select:none;
        white-space:nowrap;
      }
      .cyg-nav-item:hover{ color:var(--text); background:rgba(255,255,255,0.03); }
      .cyg-nav-item.active{ color:var(--accent); background:var(--accent-glow); border-left-color:var(--accent); }
      .cyg-nav-icon{ width:16px;height:16px;opacity:0.7;flex-shrink:0; }
      .cyg-nav-item.active .cyg-nav-icon{ opacity:1; }

      /* Collapsed state: hide labels + section headers, center icons */
      .cyg-sidebar.collapsed .cyg-nav-label,
      .cyg-sidebar.collapsed .cyg-nav-sub-label,
      .cyg-sidebar.collapsed .cyg-nav-item-label{ display:none; }
      .cyg-sidebar.collapsed .cyg-nav-item{ padding:0.55rem 0;justify-content:center; }
      .cyg-sidebar.collapsed .cyg-nav-sub{ max-height:none !important; }
      .cyg-sidebar.collapsed .cyg-nav-sub .cyg-nav-item{ padding-left:0; }
      .cyg-sidebar.collapsed .cyg-nav-section{ margin-bottom:0.4rem; }
    `;
    document.head.appendChild(style);
  }

  // ── Markup ──────────────────────────────────────────────────────────────
  function buildHTML(activeKey){
    const sections = NAV.map(sec => {
      const itemsHTML = sec.items.map(item => {
        ICON_BY_KEY[item.key] = item.icon;
        const cls  = 'cyg-nav-item' + (item.key === activeKey ? ' active' : '');
        const styleAttr = item.color ? ` style="color:${item.color}"` : '';
        return (
          '<div class="'+cls+'" data-key="'+item.key+'"'+styleAttr+' title="'+esc(item.label)+'">' +
            item.icon +
            '<span class="cyg-nav-item-label">'+esc(item.label)+'</span>' +
          '</div>'
        );
      }).join('');
      if (sec.group){
        // Sub-menu section with label + collapsible body
        return (
          '<div class="cyg-nav-section">' +
            (sec.section ? '<div class="cyg-nav-sub-label open" data-group="'+sec.group+'"><span class="cyg-chevron">▸</span>'+esc(sec.section)+'</div>' : '') +
            '<div class="cyg-nav-sub" data-sub="'+sec.group+'">' + itemsHTML + '</div>' +
          '</div>'
        );
      }
      // Plain section without a group
      return (
        '<div class="cyg-nav-section">' +
          (sec.section ? '<div class="cyg-nav-label">'+esc(sec.section)+'</div>' : '') +
          itemsHTML +
        '</div>'
      );
    }).join('');

    return (
      '<div class="cyg-sidebar-head">' +
        '<button class="cyg-sidebar-toggle" id="cyg-sidebar-toggle" title="Collapse / expand sidebar">❮</button>' +
      '</div>' +
      '<div class="cyg-sidebar-scroll">' + sections + '</div>'
    );
  }

  function esc(s){
    return String(s==null?'':s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }

  // ── Behaviour ───────────────────────────────────────────────────────────
  function wireItemClicks(sidebarEl){
    sidebarEl.querySelectorAll('.cyg-nav-item').forEach(el => {
      el.addEventListener('click', (ev) => {
        ev.stopPropagation();
        const key = el.dataset.key;
        const item = findItem(key);
        if (!item) return;
        handleClick(item);
      });
    });

    sidebarEl.querySelectorAll('.cyg-nav-sub-label').forEach(el => {
      el.addEventListener('click', (ev) => {
        ev.stopPropagation();
        const group = el.dataset.group;
        const sub = sidebarEl.querySelector('.cyg-nav-sub[data-sub="'+group+'"]');
        if (!sub) return;
        sub.classList.toggle('collapsed');
        el.classList.toggle('open');
      });
    });

    const toggle = sidebarEl.querySelector('#cyg-sidebar-toggle');
    if (toggle){
      toggle.addEventListener('click', () => {
        const nowCollapsed = !sidebarEl.classList.contains('collapsed');
        sidebarEl.classList.toggle('collapsed', nowCollapsed);
        setCollapsed(nowCollapsed);
        document.body.classList.toggle('cyg-collapsed', nowCollapsed);
        toggle.textContent = nowCollapsed ? '❯' : '❮';
      });
    }
  }

  function findItem(key){
    for (const sec of NAV){
      for (const it of sec.items){ if (it.key === key) return it; }
    }
    return null;
  }

  function handleClick(item){
    // If the item is a dashboard view AND we're on dashboard, call showView directly.
    // Otherwise, stash cyg_goto and navigate to dashboard (if view) or the page (if href).
    const onDashboard = /\/dashboard\.html?$|^\/$/.test(location.pathname);
    if (item.view){
      if (onDashboard && typeof window.showView === 'function'){
        window.showView(item.view);
        // Update active highlight in place
        updateActive(item.key);
      } else {
        try { sessionStorage.setItem('cyg_goto', item.view); } catch {}
        window.location.href = '/dashboard.html';
      }
      return;
    }
    if (item.href){
      window.location.href = item.href;
      return;
    }
  }

  function updateActive(key){
    const sidebar = document.querySelector('.cyg-sidebar');
    if (!sidebar) return;
    sidebar.querySelectorAll('.cyg-nav-item').forEach(el => {
      el.classList.toggle('active', el.dataset.key === key);
    });
  }

  // ── Mount ───────────────────────────────────────────────────────────────
  function mount(){
    injectStyles();
    const host = document.getElementById(MOUNT_ID);
    if (!host){
      console.warn('[cygenix-sidebar] No mount point found (expected #'+MOUNT_ID+')');
      return;
    }
    const activeKey = host.dataset.active || '';
    const aside = document.createElement('aside');
    const collapsed = isCollapsed();
    aside.className = 'cyg-sidebar' + (collapsed ? ' collapsed' : '');
    aside.innerHTML = buildHTML(activeKey);
    host.replaceWith(aside);
    document.body.classList.toggle('cyg-collapsed', collapsed);

    // Update toggle icon to match state
    const toggle = aside.querySelector('#cyg-sidebar-toggle');
    if (toggle) toggle.textContent = collapsed ? '❯' : '❮';

    wireItemClicks(aside);
  }

  // Public API (useful for dashboard to call on showView)
  window.CygenixSidebar = {
    mount,
    setActive: updateActive,
    isCollapsed,
    setCollapsed: (on) => {
      const el = document.querySelector('.cyg-sidebar');
      if (el) el.classList.toggle('collapsed', !!on);
      setCollapsed(on);
      const toggle = el && el.querySelector('#cyg-sidebar-toggle');
      if (toggle) toggle.textContent = on ? '❯' : '❮';
    }
  };

  // Auto-mount on DOMContentLoaded (or immediately if already past)
  if (document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', mount);
  } else {
    mount();
  }
})();
