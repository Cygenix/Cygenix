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
 *
 * Feature gating:
 *   - Items with `requiresAiEnabled: true` are filtered out when the
 *     localStorage flags `cygenix_feature_flags` indicate AI is off.
 *     Defaults to visible if flags are unset (so dev work isn't blocked).
 */
(function(){
  'use strict';

  const STORAGE_KEY   = 'cygenix_sidebar_collapsed';
  const FLAGS_KEY     = 'cygenix_feature_flags';
  const MOUNT_ID      = 'cyg-sidebar-mount';
  const ICON_BY_KEY   = {}; // populated from NAV_ITEMS
  // Keep these in lockstep with the hard-coded content offsets on app pages
  // (padding-left / left:230px when open, 54px when collapsed). Changing them
  // here without updating every page's offset would misalign the content.
  const WIDTH_OPEN    = 230;
  const WIDTH_CLOSED  = 54;

  // ── Nav structure ───────────────────────────────────────────────────────
  // `key` is the identifier used for both `data-active` matching and dashboard showView.
  // `href` means it's a separate HTML page; `view` means it's a dashboard embedded view.
  // `action` is a named client-side action (e.g. cookie-preferences, open-help).
  // Only ONE of href/view/action is set per item.
  //
  // `badgeId` (optional) renders a small status dot next to the label. Dashboard
  // code updates these dots by ID — same element IDs the legacy inline sidebar
  // used, so existing update code keeps working unchanged.
  //
  // `requiresAiEnabled` (optional) hides the item when AI features are disabled
  // for this tenant or user.
  const NAV = [
    { section: 'Migrate', items: [
      { key:'dashboard', label:'Dashboard', view:'dashboard', icon: iconDashboard() },
      { key:'search',    label:'Search',    view:'search',    icon: iconSearch() },
    ]},
    { section: 'Configure', group:'configure', items: [
      { key:'project-settings',  label:'Settings',           view:'project-settings',   color:'var(--amber)',  icon: iconSettings() },
      { key:'connections',       label:'Connections',       view:'connections',        color:'var(--green)',  icon: iconPlug() },
      { key:'performance',       label:'Performance',       href:'/performance.html',  color:'var(--teal)',   icon: iconChart() },
      { key:'system-parameters', label:'System Parameters', view:'system-parameters',  color:'var(--accent)', icon: iconParams() },
      { key:'privacy-security',  label:'Governance',        view:'privacy-security',   color:'var(--red)',    icon: iconShield() },
      { key:'integrations',      label:'Integrations',      view:'integrations',       color:'var(--teal)',   icon: iconIntegrations() },
    ]},
    { section: 'Plan', group:'plan', items: [
      { key:'project-plan',      label:'Project Planner',   href:'/project-plan.html', color:'var(--green)',  icon: iconCalendar(), badgeId:'cyg-badge-project-plan' },
    ]},
    { section: 'Develop', group:'develop', items: [
      { key:'object-mapping',     label:'Object Mapping',     href:'/object_mapping.html',     color:'var(--teal)',   icon: iconArrows() },
      { key:'sql-editor',         label:'SQL Editor',         href:'/sql-editor.html',         color:'var(--teal)',   icon: iconCode() },
      { key:'agentive-migration', label:'Agentive Migration', href:'/agentive_migration.html', color:'var(--accent)', icon: iconHand(), requiresAiEnabled: true },
      { key:'coworker',           label:'Co-Worker',          href:'/coworker.html',           color:'var(--accent)', icon: svg('<path d="M2.5 3.5h11a1 1 0 0 1 1 1v5a1 1 0 0 1-1 1H6l-3 2.5V10.5H2.5a1 1 0 0 1-1-1v-5a1 1 0 0 1 1-1Z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/><path d="M5.5 6.4h5M5.5 8.2h3" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/>') },
    ]},
    { section: 'Data Analysis', group:'analysis', items: [
      { key:'data-quality',      label:'Data Quality Review', href:'/data-quality.html',  color:'var(--green)',  icon: iconQuality() },
      { key:'insights',          label:'Data Insights',       href:'/insights.html',      color:'var(--purple)', icon: iconInsights() },
      { key:'data-cleansing',    label:'Data Cleansing',      href:'/data-cleansing.html',color:'var(--teal)',   icon: iconClean() },
      { key:'validation',        label:'Validation',          href:'/validation.html',    color:'var(--amber)',  icon: iconCheck() },
    ]},
    { section: 'Projects', group:'projects', items: [
      { key:'jobs',              label:'All Jobs',          view:'jobs',                icon: iconList() },
      { key:'project-builder',   label:'Execute',           href:'/project-builder.html', color:'var(--purple)', icon: iconPlay() },
    ]},
    { section: 'Advanced', group:'advanced', items: [
      { key:'server-migration',  label:'Server Migration',  view:'server-migration',   color:'var(--accent)', icon: iconServerMigration() },
    ]},
    { section: null, items: [
      { key:'inventory',         label:'Project Artifacts',         view:'inventory',           icon: iconGrid() },
      { key:'task-agent',        label:'Task Agent',        view:'task-agent',          color:'var(--yellow)', icon: iconClock() },
    ]},
    { section: 'Reporting', group:'reports', items: [
      { key:'report-builder',             label:'Report Builder',            href:'/reports.html',              color:'var(--amber)',  icon: iconReportBuilder() },
      { key:'reports',                    label:'Conversion Report',         view:'reports',                    color:'var(--purple)', icon: iconReport() },
      { key:'project-summary-document',   label:'Project Summary Document',  view:'project-summary-document',   color:'var(--purple)', icon: iconDocument() },
    ]},
    { section: 'More', group:'more', items: [
      { key:'audit',             label:'Audit Log',         view:'audit',               color:'var(--text2)', icon: iconAuditLog() },
      { key:'supported',         label:'Supported Formats', view:'supported',           color:'var(--text2)', icon: iconInfo() },
      { key:'diagnostics',       label:'Diagnostics',       view:'diagnostics',         color:'var(--text2)', icon: iconPulse() },
      { key:'help',              label:'Help Guide',      action:'open-help',         color:'var(--accent)', icon: iconHelp() },
    ]},
    { section: null, items: [
      { key:'cookie-prefs',      label:'Cookie preferences', action:'cookie-preferences', color:'var(--text3)',  icon: iconCookie() },
    ]},
  ];

  // ── Icons (returns SVG string) ──────────────────────────────────────────
  function svg(body){ return '<svg class="cyg-nav-icon" viewBox="0 0 16 16" fill="none">'+body+'</svg>'; }
  function iconDashboard(){    return svg('<rect x="2" y="2" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="9" y="2" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="2" y="9" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="9" y="9" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/>'); }
  function iconSettings(){     return svg('<rect x="2" y="3" width="12" height="2" rx="0.5" fill="currentColor" opacity="0.3"/><rect x="2" y="7" width="12" height="2" rx="0.5" fill="currentColor" opacity="0.3"/><rect x="2" y="11" width="12" height="2" rx="0.5" fill="currentColor" opacity="0.3"/><circle cx="10" cy="4" r="1.3" fill="currentColor"/><circle cx="5" cy="8" r="1.3" fill="currentColor"/><circle cx="11" cy="12" r="1.3" fill="currentColor"/>'); }
  function iconPlug(){         return svg('<path d="M3 8h10" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/><circle cx="3.5" cy="8" r="1.5" stroke="currentColor" stroke-width="1.2"/><circle cx="12.5" cy="8" r="1.5" stroke="currentColor" stroke-width="1.2"/>'); }
  // Two stacked server racks with an arrow between them — represents
  // server-to-server replication (logins, jobs, SSIS). Distinct from
  // iconShield (Governance) and iconArrows (Object Mapping) by being
  // explicitly server-shaped.
  function iconServerMigration(){ return svg('<rect x="2" y="2" width="12" height="4" rx="0.5" stroke="currentColor" stroke-width="1.2"/><circle cx="4" cy="4" r="0.6" fill="currentColor"/><rect x="2" y="10" width="12" height="4" rx="0.5" stroke="currentColor" stroke-width="1.2"/><circle cx="4" cy="12" r="0.6" fill="currentColor"/><path d="M8 6.5v3M6.5 8.5L8 10l1.5-1.5" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconDownload(){     return svg('<path d="M8 2v8m0 0L5 7m3 3l3-3M3 13h10" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconChart(){        return svg('<path d="M2 13l4-4 3 3 5-6" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconParams(){       return svg('<circle cx="8" cy="8" r="2" stroke="currentColor" stroke-width="1.2"/><path d="M8 1v2M8 13v2M1 8h2M13 8h2M3.5 3.5l1.5 1.5M11 11l1.5 1.5M3.5 12.5l1.5-1.5M11 5l1.5-1.5" stroke="currentColor" stroke-width="1.1" stroke-linecap="round"/>'); }
  function iconShield(){       return svg('<path d="M8 2 L3 4 V8 C3 11 5 13 8 14 C11 13 13 11 13 8 V4 Z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/><path d="M6 8l1.5 1.5L10.5 7" stroke="currentColor" stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconIntegrations(){ return svg('<circle cx="4" cy="4" r="2" stroke="currentColor" stroke-width="1.2"/><circle cx="12" cy="4" r="2" stroke="currentColor" stroke-width="1.2"/><circle cx="4" cy="12" r="2" stroke="currentColor" stroke-width="1.2"/><circle cx="12" cy="12" r="2" stroke="currentColor" stroke-width="1.2"/><path d="M6 4h4M6 12h4M4 6v4M12 6v4" stroke="currentColor" stroke-width="1.1" stroke-linecap="round"/>'); }
  function iconCalendar(){     return svg('<rect x="2" y="3" width="12" height="11" rx="1" stroke="currentColor" stroke-width="1.2"/><path d="M2 6h12M5 2v2M11 2v2" stroke="currentColor" stroke-width="1.2"/>'); }
  function iconArrows(){       return svg('<path d="M3 5h8l-2-2M13 11H5l2 2" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconCode(){         return svg('<rect x="2" y="3" width="12" height="10" rx="1" stroke="currentColor" stroke-width="1.2"/><path d="M6 7l-2 1 2 1M10 7l2 1-2 1" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconHand(){         return svg('<path d="M7.5 8h1.5a1.3 1.3 0 0 0 0-2.6H7c-.4 0-.75.13-.93.4L2 9" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/><path d="M4.5 12l1-.9c.2-.27.55-.4.93-.4h2.65c.73 0 1.4-.27 1.86-.8L14 6.95a1.3 1.3 0 0 0-1.8-1.9l-2.75 2.55" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/><path d="M1.5 8.5l4 4" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/>'); }
  function iconInsights(){     return svg('<circle cx="8" cy="6" r="3" stroke="currentColor" stroke-width="1.2"/><path d="M4 14c0-2 2-3 4-3s4 1 4 3" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/>'); }
  function iconClean(){        return svg('<path d="M4 2v4M4 10v4M2 6h4M2 12h4M10 3l3 3-6 6-3-3z" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>'); }
  // Clipboard with a tick on its page — used for Data Quality Review.
  // Distinct from iconCheck (plain circle + tick, used for Validation) and
  // iconReport (single-page document, used for Conversion Report).
  function iconQuality(){      return svg('<rect x="3" y="3" width="10" height="11" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="6" y="1.5" width="4" height="2.5" rx="0.4" stroke="currentColor" stroke-width="1.2" fill="none"/><path d="M5.5 8.5l1.6 1.6 3.4-3.6" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconCheck(){        return svg('<circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="1.2"/><path d="M5.5 8l2 2 3-4" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconPlay(){         return svg('<rect x="2" y="3" width="12" height="10" rx="1" stroke="currentColor" stroke-width="1.2"/><path d="M7 6l3 2-3 2z" fill="currentColor"/>'); }
  function iconList(){         return svg('<path d="M2 4h12M2 8h12M2 12h12" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>'); }
  function iconGrid(){         return svg('<rect x="2" y="2" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="9" y="2" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="2" y="9" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="9" y="9" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/>'); }
  function iconClock(){        return svg('<circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="1.2"/><path d="M8 4v4l2.5 1.5" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconReport(){       return svg('<path d="M4 2h6l3 3v9H4z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/><path d="M10 2v3h3M6 8h4M6 11h4" stroke="currentColor" stroke-width="1.1" stroke-linecap="round"/>'); }
  // Document with embedded bar chart — Report Builder produces ad-hoc tabular
  // and visual reports, distinct from iconReport (Conversion Report, fixed
  // layout) and iconDocument (Project Summary, signed deliverable).
  function iconReportBuilder(){return svg('<path d="M4 2h6l3 3v9H4z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/><path d="M10 2v3h3" stroke="currentColor" stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round"/><rect x="6" y="10" width="1.2" height="2" fill="currentColor"/><rect x="8" y="8.5" width="1.2" height="3.5" fill="currentColor"/><rect x="10" y="9.5" width="1.2" height="2.5" fill="currentColor"/>'); }
  // Document with signature line — used for the Project Summary Document, the
  // "deliverable" output (signed off, sent to clients) vs iconReport which is
  // the day-to-day Conversion Report.
  function iconDocument(){     return svg('<path d="M4 1.5h5.5L13 5v9.5H4z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/><path d="M9.5 1.5V5H13" stroke="currentColor" stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round"/><path d="M6 7.5h5M6 9.5h5" stroke="currentColor" stroke-width="1.1" stroke-linecap="round"/><path d="M6 12h3.5" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>'); }
  function iconAlert(){        return svg('<path d="M8 2l6 11H2z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/><path d="M8 6v3M8 11v0.5" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>'); }
  function iconCookie(){       return svg('<circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="1.2"/><circle cx="5.5" cy="6" r="0.8" fill="currentColor"/><circle cx="9.5" cy="5.5" r="0.6" fill="currentColor"/><circle cx="10.5" cy="9" r="0.7" fill="currentColor"/><circle cx="6" cy="10" r="0.5" fill="currentColor"/><circle cx="8.5" cy="11.5" r="0.6" fill="currentColor"/>'); }
  function iconSearch(){       return svg('<circle cx="7" cy="7" r="5" stroke="currentColor" stroke-width="1.3"/><path d="M10.5 10.5L14 14" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>'); }
  function iconAuditLog(){     return svg('<rect x="2" y="1.5" width="12" height="13" rx="1.5" stroke="currentColor" stroke-width="1.2"/><path d="M5 5.5h6M5 8h6M5 10.5h3" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/>'); }
  function iconInfo(){         return svg('<circle cx="8" cy="8" r="6.5" stroke="currentColor" stroke-width="1.2"/><path d="M8 7v5M8 5v1" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/>'); }
  function iconPulse(){        return svg('<path d="M2 8h3l2-5 3 10 2-5h2" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconHelp(){         return svg('<circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="1.2"/><path d="M6.5 6c0-1 .75-1.5 1.5-1.5s1.5.5 1.5 1.5c0 .75-.5 1.25-1.5 1.5V9" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/><circle cx="8" cy="11" r="0.6" fill="currentColor"/>'); }

  // ── State ───────────────────────────────────────────────────────────────
  function isCollapsed(){
    try { return localStorage.getItem(STORAGE_KEY) === '1'; } catch { return false; }
  }
  function setCollapsed(on){
    try { localStorage.setItem(STORAGE_KEY, on ? '1' : '0'); } catch {}
  }

  // Read AI feature flags from localStorage. Defaults to "enabled" when
  // flags are unset so the feature is visible during development before
  // the /api/me endpoint is wired up. dashboard.html should call
  // refreshFeatureFlags() on login to populate these.
  function isAiEnabled(){
    try {
      const flags = JSON.parse(localStorage.getItem(FLAGS_KEY) || '{}');
      if (flags.tenantAgentiveMigrationEnabled === false) return false;
      if (flags.aiAgentiveEnabled === false) return false;
      return true;
    } catch {
      return true;
    }
  }

  function isItemVisible(item){
    if (item.requiresAiEnabled && !isAiEnabled()) return false;
    return true;
  }

  // ── Style injection (once) ──────────────────────────────────────────────
  function injectStyles(){
    if (document.getElementById('cyg-sidebar-styles')) return;
    const style = document.createElement('style');
    style.id = 'cyg-sidebar-styles';
    style.textContent = `
      /* ── Cygenix redesigned console sidebar ────────────────────────────
         Ink-dark rail, independent of the workspace theme so it reads the
         same in light and dark. Brand mark up top, grouped nav, indigo
         active rail, collapsible to an icon-only strip. */
      .cyg-sidebar{
        --cyg-ink:#14161f;
        --cyg-fg:rgba(255,255,255,0.62);
        --cyg-fg-strong:#ffffff;
        --cyg-muted:rgba(255,255,255,0.32);
        background:var(--cyg-ink);
        border-right:1px solid rgba(255,255,255,0.06);
        padding:0 0 0.5rem;
        width:${WIDTH_OPEN}px;
        display:flex;
        flex-direction:column;
        overflow:hidden;
        transition:width 0.22s cubic-bezier(.4,0,.2,1);
        position:fixed;
        top:0;
        left:0;
        bottom:0;
        height:100vh;
        z-index:90;
        font-family:'IBM Plex Sans','Helvetica Neue',Arial,sans-serif;
        -webkit-font-smoothing:antialiased;
      }
      .cyg-sidebar.collapsed{ width:${WIDTH_CLOSED}px; }

      /* Brand header */
      .cyg-sidebar-head{
        display:flex;
        align-items:center;
        gap:8px;
        height:64px;
        min-height:64px;
        padding:0 10px 0 14px;
        flex-shrink:0;
        overflow:hidden;
      }
      .cyg-sidebar.collapsed .cyg-sidebar-head{ justify-content:center;padding:0; }
      .cyg-brand{ display:flex;align-items:center;gap:10px;min-width:0;flex:1;overflow:hidden;text-decoration:none; }
      .cyg-brand-mark{
        width:34px;height:34px;min-width:34px;border-radius:9px;
        background:linear-gradient(140deg,#5a6ef0,#4a5bd6 55%,#3aa6b6);
        display:flex;align-items:center;justify-content:center;
        box-shadow:0 4px 14px -4px rgba(74,91,214,0.6);
        font-family:'IBM Plex Mono',monospace;font-weight:600;font-size:18px;color:#fff;
      }
      .cyg-brand-word{ display:flex;flex-direction:column;line-height:1.15;overflow:hidden; }
      .cyg-brand-word b{ font-size:16px;font-weight:700;letter-spacing:-0.01em;color:var(--cyg-fg-strong); }
      .cyg-brand-word span{ font-size:10px;font-weight:500;letter-spacing:0.08em;text-transform:uppercase;color:var(--cyg-muted);white-space:nowrap; }
      .cyg-sidebar.collapsed .cyg-brand-word{ display:none; }

      .cyg-sidebar-toggle{
        margin-left:auto;
        background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.09);cursor:pointer;
        color:rgba(255,255,255,0.6);
        width:28px;height:28px;border-radius:8px;
        display:flex;align-items:center;justify-content:center;
        font-size:13px;line-height:1;
        transition:color 0.15s,background 0.15s;
      }
      .cyg-sidebar-toggle:hover{ color:#fff; background:rgba(255,255,255,0.12); }
      .cyg-sidebar.collapsed .cyg-sidebar-toggle{ display:none; }

      .cyg-sidebar-scroll{
        flex:1 1 auto;
        overflow-y:auto;
        overflow-x:hidden;
        padding:2px 12px 12px;
        scrollbar-width:thin;
        scrollbar-color:rgba(255,255,255,0.14) transparent;
      }
      .cyg-sidebar-scroll::-webkit-scrollbar{ width:6px; }
      .cyg-sidebar-scroll::-webkit-scrollbar-thumb{ background:rgba(255,255,255,0.14); border-radius:3px; }
      .cyg-sidebar-scroll::-webkit-scrollbar-track{ background:transparent; }

      .cyg-nav-section{ margin-top:14px; }
      .cyg-nav-section:first-child{ margin-top:6px; }
      .cyg-nav-label{
        font-size:10px;font-weight:600;color:var(--cyg-muted);
        text-transform:uppercase;letter-spacing:0.09em;
        padding:0 12px 7px;
      }

      .cyg-nav-item{
        display:flex;align-items:center;gap:13px;
        padding:9px 12px;
        margin:2px 0;
        border-radius:9px;
        font-size:13.5px;font-weight:500;color:var(--cyg-fg);
        cursor:pointer;
        transition:color 0.15s,background 0.15s;
        position:relative;
        user-select:none;
        white-space:nowrap;
      }
      .cyg-nav-item:hover{ color:#fff; background:rgba(255,255,255,0.09); }
      .cyg-nav-item.active{ color:var(--cyg-fg-strong); background:rgba(255,255,255,0.10); font-weight:600; }
      .cyg-nav-item.active::before{
        content:'';position:absolute;left:-12px;top:50%;transform:translateY(-50%);
        width:3px;height:20px;border-radius:0 3px 3px 0;background:var(--accent);
      }
      .cyg-nav-icon{ width:18px;height:18px;opacity:0.9;flex-shrink:0;color:currentColor; }
      .cyg-nav-item:hover .cyg-nav-icon{ opacity:1; }
      .cyg-nav-item.active .cyg-nav-icon{ opacity:1;color:#fff; }

      .cyg-sidebar.collapsed .cyg-nav-label,
      .cyg-sidebar.collapsed .cyg-nav-item-label{ display:none; }
      .cyg-sidebar.collapsed .cyg-nav-item{ justify-content:center;padding:9px 0;gap:0; }
      .cyg-sidebar.collapsed .cyg-nav-item.active::before{ left:-12px; }

      /* Notification badge — small red pill with count, anchored to the right */
      .cyg-nav-badge{
        margin-left:auto;
        min-width:19px;height:19px;
        padding:0 5px;
        border-radius:10px;
        background:var(--red,#e5484d);
        color:#fff;
        font-size:10.5px;font-weight:600;
        line-height:19px;
        text-align:center;
        display:none;
        flex-shrink:0;
      }
      .cyg-nav-badge.show{ display:inline-block; }
      /* Collapsed sidebar: show as a small dot in the top-right corner of the icon */
      .cyg-sidebar.collapsed .cyg-nav-badge{
        position:absolute;
        top:5px;right:16px;
        margin:0;padding:0;
        min-width:8px;width:8px;height:8px;
        border-radius:50%;
        font-size:0;line-height:0;
      }

      body.cyg-collapsed{ --cyg-sidebar-w:${WIDTH_CLOSED}px; }
      body:not(.cyg-collapsed){ --cyg-sidebar-w:${WIDTH_OPEN}px; }

      /* Pinned footer — user chip + accessibility control (always visible). */
      .cyg-sidebar-foot{
        flex-shrink:0;
        border-top:1px solid rgba(255,255,255,0.07);
        padding:10px 12px;
        display:flex;flex-direction:column;gap:4px;
      }
      .cyg-user-chip{
        display:flex;align-items:center;gap:11px;
        padding:8px;border-radius:10px;cursor:pointer;
        transition:background 0.15s;text-decoration:none;
      }
      .cyg-user-chip:hover{ background:rgba(255,255,255,0.07); }
      .cyg-user-av{
        width:32px;height:32px;min-width:32px;border-radius:8px;
        background:#4a5bd6;
        display:flex;align-items:center;justify-content:center;
        font-size:12.5px;font-weight:600;color:#fff;
      }
      .cyg-user-meta{ display:flex;flex-direction:column;line-height:1.25;overflow:hidden;flex:1;min-width:0; }
      .cyg-user-meta b{ font-size:12.5px;font-weight:600;color:#fff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis; }
      .cyg-user-meta span{ font-size:10.5px;color:rgba(255,255,255,0.42);white-space:nowrap; }
      .cyg-sidebar.collapsed .cyg-user-meta{ display:none; }
      .cyg-sidebar.collapsed .cyg-user-chip{ justify-content:center;padding:8px 0; }

      .cyg-a11y-btn{
        display:flex;align-items:center;gap:11px;width:100%;
        padding:8px 10px;border:none;background:none;cursor:pointer;
        font:inherit;font-size:12.5px;color:rgba(255,255,255,0.6);border-radius:9px;
        text-align:left;transition:color 0.12s,background 0.12s;
      }
      .cyg-a11y-btn:hover{ color:#fff; background:rgba(255,255,255,0.07); }
      .cyg-a11y-btn .cyg-nav-icon{ width:18px;height:18px;flex-shrink:0; }
      .cyg-sidebar.collapsed .cyg-a11y-btn{ justify-content:center;padding:9px 0; }
      .cyg-sidebar.collapsed .cyg-a11y-btn .cyg-nav-item-label{ display:none; }
    `;
    document.head.appendChild(style);
  }

  // Make sure the accessibility engine (panel + API) is loaded on the page.
  function ensureA11y(){
    if (window.CygenixA11y || document.getElementById('cygenix-a11y-js')) return;
    const s = document.createElement('script');
    s.id = 'cygenix-a11y-js'; s.src = '/cygenix-a11y.js';
    document.head.appendChild(s);
  }

  function buildFooter(){
    const icon = svg('<circle cx="8" cy="8" r="6.6" stroke="currentColor" stroke-width="1.2"/><circle cx="8" cy="4.6" r="0.95" fill="currentColor"/><path d="M4.3 6.1c1.2.6 2.4.8 3.7.8s2.5-.2 3.7-.8M8 6.9V10m0 0l-1.5 2.3M8 10l1.5 2.3" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/>');
    return `<div class="cyg-sidebar-foot">
      <a class="cyg-user-chip" href="/dashboard.html#goto=project-settings" title="Account">
        <span class="cyg-user-av" id="cyg-user-av">CY</span>
        <span class="cyg-user-meta"><b id="cyg-user-name">Account</b><span id="cyg-user-sub">Migration Console</span></span>
      </a>
      <button type="button" class="cyg-a11y-btn a11y-trigger" aria-label="Accessibility options" aria-expanded="false"
              onclick="event.stopPropagation(); if(window.CygenixA11y){window.CygenixA11y.toggle();}">
        ${icon}<span class="cyg-nav-item-label">Accessibility</span>
      </button>
    </div>`;
  }

  // ── HTML build ──────────────────────────────────────────────────────────
  function buildHTML(activeKey){
    const head = `<div class="cyg-sidebar-head">
      <a class="cyg-brand" href="/dashboard.html" aria-label="Cygenix — Migration Console">
        <span class="cyg-brand-mark">C</span>
        <span class="cyg-brand-word"><b>Cygenix</b><span>Migration Console</span></span>
      </a>
      <button id="cyg-sidebar-toggle" class="cyg-sidebar-toggle" aria-label="Collapse sidebar">❮</button>
    </div>`;
    const body = NAV.map(sec => buildSection(sec, activeKey)).join('');
    return head + `<div class="cyg-sidebar-scroll">${body}</div>` + buildFooter();
  }

  // Read the signed-in user (stored by auth flow as cygenix_user) and fill the
  // footer chip. Falls back to a neutral label so the chip never shows blanks.
  function populateUser(root){
    const nameEl = root.querySelector('#cyg-user-name');
    const subEl  = root.querySelector('#cyg-user-sub');
    const avEl   = root.querySelector('#cyg-user-av');
    if (!nameEl) return;
    let name = 'Account', email = '', plan = 'Migration Console';
    try {
      const raw = sessionStorage.getItem('cygenix_user') || localStorage.getItem('cygenix_user')
                || localStorage.getItem('cygenix_active_user');
      if (raw){
        const u = JSON.parse(raw);
        name  = (u.user_metadata && u.user_metadata.full_name) || (u.email ? u.email.split('@')[0] : name);
        email = u.email || '';
        if (u.plan || (u.user_metadata && u.user_metadata.plan)) plan = u.plan || u.user_metadata.plan;
      }
    } catch {}
    const initials = name.trim().split(/\s+/).map(w => w[0]).join('').slice(0,2).toUpperCase() || 'CY';
    nameEl.textContent = name;
    if (subEl) subEl.textContent = email || plan;
    if (avEl) avEl.textContent = initials;
  }

  function buildSection(sec, activeKey){
    const visibleItems = (sec.items || []).filter(isItemVisible);
    if (!visibleItems.length) return '';
    const labelHtml = sec.section
      ? `<div class="cyg-nav-label">${sec.section}</div>`
      : '';
    const itemsHtml = visibleItems.map(it => buildItem(it, activeKey)).join('');
    return `<div class="cyg-nav-section">${labelHtml}${itemsHtml}</div>`;
  }

  function buildItem(item, activeKey){
    const isActive = item.key === activeKey ? ' active' : '';
    // Oracle look: monochrome nav icons (neutral grey), red only on the
    // active item. We intentionally ignore the per-item accent colour so the
    // sidebar reads as one calm, professional column rather than a rainbow.
    const styleAttr = '';
    const badgeHtml = item.badgeId
      ? `<span class="cyg-nav-badge" id="${item.badgeId}" aria-live="polite"></span>`
      : '';
    return `
      <div class="cyg-nav-item${isActive}"
           data-key="${item.key}"
           tabindex="0"${styleAttr}>
        ${item.icon || ''}
        <span class="cyg-nav-item-label">${escapeHtml(item.label)}</span>
        ${badgeHtml}
      </div>`;
  }

  function escapeHtml(s){
    return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }

  // ── Wiring ──────────────────────────────────────────────────────────────
  function wireItemClicks(sidebarEl){
    sidebarEl.querySelectorAll('.cyg-nav-item').forEach(el => {
      el.addEventListener('click', () => {
        const key = el.dataset.key;
        const item = findItem(key);
        if (item) handleClick(item);
      });
      el.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' || e.key === ' '){
          e.preventDefault();
          el.click();
        }
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
    if (item.action === 'cookie-preferences'){
      if (typeof window.openCookiePreferences === 'function') window.openCookiePreferences();
      return;
    }
    if (item.action === 'open-help'){
      window.open('/help.html', '_blank');
      return;
    }
    if (item.view){
      if (onDashboard && typeof window.showView === 'function'){
        window.showView(item.view);
        // Update active highlight in place
        updateActive(item.key);
      } else {
        // Belt-and-braces: stash in sessionStorage AND pass in the URL hash.
        // sessionStorage can be wiped by auth-gate redirects; the hash survives
        // as long as the redirect preserves it. Dashboard reads either.
        try { sessionStorage.setItem('cyg_goto', item.view); } catch {}
        window.location.href = '/dashboard.html#goto=' + encodeURIComponent(item.view);
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
    populateUser(aside);
    startBadgeUpdater();
    ensureA11y();
  }

  // ── Live status badges ──────────────────────────────────────────────────
  // Project Planner badge: count of items needing attention today, minus
  // anything the user has already "seen" (by visiting /project-plan.html).
  //
  // Attention items:
  //   - Calendar events (calEvents) with date === today
  //   - Tasks with due === today AND status !== 'done'
  //   - Overdue tasks (due < today AND status !== 'done')
  //
  // Dismiss behaviour:
  //   When the user visits /project-plan.html, project-plan.html itself calls
  //   CygenixSidebar.markProjectPlanSeen() — which snapshots the current set
  //   of attention item IDs into localStorage['cygenix_pp_seen']. The badge
  //   then counts only items NOT in that set, so visiting the page clears
  //   the badge until something new appears (a new task, midnight rollover,
  //   etc). Marking a task done or deleting an event also clears it because
  //   it falls out of the attention set entirely.
  //
  // Reads localStorage['cygenix_project_plan'] (the key project-plan.html writes to).
  const PLAN_STORAGE_KEY = 'cygenix_project_plan';
  const SEEN_STORAGE_KEY = 'cygenix_pp_seen';

  function todayIsoDate(){
    const d = new Date();
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }

  // Returns array of stable IDs for items currently needing attention.
  // IDs are prefixed so task/event namespaces never collide.
  function getAttentionItemIds(){
    try {
      const raw = localStorage.getItem(PLAN_STORAGE_KEY);
      if (!raw) return [];
      const data = JSON.parse(raw);
      const tasks = Array.isArray(data.tasks) ? data.tasks : [];
      const calEvents = Array.isArray(data.calEvents) ? data.calEvents : [];
      const today = todayIsoDate();
      const ids = [];
      for (const e of calEvents){
        if (e && e.id && e.date === today) ids.push('event:' + e.id);
      }
      for (const t of tasks){
        if (!t || !t.id || t.status === 'done') continue;
        if (t.due === today) ids.push('task:' + t.id);
        else if (t.due && t.due < today) ids.push('task:' + t.id);
      }
      return ids;
    } catch { return []; }
  }

  // Read the "seen" set. Stored as { date: 'YYYY-MM-DD', ids: [...] }.
  // The date stamp lets us auto-expire seen entries at midnight — anything
  // marked seen yesterday is ignored today, so a task that was overdue
  // yesterday and is *still* overdue today re-surfaces in the badge.
  function getSeenSet(){
    try {
      const raw = localStorage.getItem(SEEN_STORAGE_KEY);
      if (!raw) return new Set();
      const data = JSON.parse(raw);
      if (!data || data.date !== todayIsoDate()) return new Set();
      return new Set(Array.isArray(data.ids) ? data.ids : []);
    } catch { return new Set(); }
  }

  function refreshProjectPlanBadge(){
    const el = document.getElementById('cyg-badge-project-plan');
    if (!el) return;
    const allIds = getAttentionItemIds();
    const seen = getSeenSet();
    const unseenCount = allIds.filter(id => !seen.has(id)).length;
    if (unseenCount > 0){
      el.textContent = unseenCount > 99 ? '99+' : String(unseenCount);
      el.classList.add('show');
      el.title = `${unseenCount} new item${unseenCount === 1 ? '' : 's'} due today or overdue`;
    } else {
      el.textContent = '';
      el.classList.remove('show');
      el.removeAttribute('title');
    }
  }

  // Public: called by project-plan.html on load to mark all current
  // attention items as "seen" for the rest of today.
  function markProjectPlanSeen(){
    try {
      const ids = getAttentionItemIds();
      localStorage.setItem(SEEN_STORAGE_KEY, JSON.stringify({
        date: todayIsoDate(),
        ids
      }));
    } catch {}
    refreshProjectPlanBadge();
  }

  function startBadgeUpdater(){
    refreshProjectPlanBadge();
    // Refresh when the project plan key (or seen key) changes in another tab
    window.addEventListener('storage', (ev) => {
      if (ev.key === PLAN_STORAGE_KEY || ev.key === SEEN_STORAGE_KEY) {
        refreshProjectPlanBadge();
      }
    });
    // Refresh when the project plan page dispatches an in-tab update event
    window.addEventListener('cygenix-plan-changed', refreshProjectPlanBadge);
    // Periodic refresh — catches the date rolling over at midnight without a reload
    setInterval(refreshProjectPlanBadge, 60 * 1000);
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
    },
    // Mark all current Project Planner attention items as "seen" for today.
    // Called by project-plan.html on load to dismiss the sidebar badge.
    markProjectPlanSeen,
    // Re-render after feature flags change (call this from dashboard.html
    // after refreshFeatureFlags() updates localStorage).
    refresh: () => {
      const existing = document.querySelector('.cyg-sidebar');
      if (!existing) return;
      const activeKey = existing.querySelector('.cyg-nav-item.active');
      const key = activeKey ? activeKey.dataset.key : '';
      const collapsed = existing.classList.contains('collapsed');
      const replacement = document.createElement('aside');
      replacement.className = 'cyg-sidebar' + (collapsed ? ' collapsed' : '');
      replacement.innerHTML = buildHTML(key);
      existing.replaceWith(replacement);
      const toggle = replacement.querySelector('#cyg-sidebar-toggle');
      if (toggle) toggle.textContent = collapsed ? '❯' : '❮';
      wireItemClicks(replacement);
      populateUser(replacement);
      refreshProjectPlanBadge();
    }
  };

  // Auto-mount on DOMContentLoaded (or immediately if already past)
  if (document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', mount);
  } else {
    mount();
  }
})();
