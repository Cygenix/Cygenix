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
  // ── Structure (2026-08 navigation review) ───────────────────────────────
  // One organising axis — the migration lifecycle: Connect → Map & Build →
  // Run → Validate → Report & Govern. Duplicate clusters (jobs, quality,
  // reports, monitoring, settings) collapse into a parent with sub-items, so
  // every existing destination keeps its page and its key; only grouping,
  // order and labels change. Utilities are docked in a footer (see
  // FOOTER_NAV) and cookie preferences live in the account menu, so the
  // workflow rail fits without scrolling.
  //
  // Parents with `children` are pure expanders (no href/view/action). All
  // child keys are unchanged from the old flat rail, so pages' data-active
  // attributes and dashboard badge/view wiring keep working.
  const NAV = [
    { section: null, items: [
      { key:'dashboard', label:'Home',   view:'dashboard', icon: iconDashboard() },
      { key:'search',    label:'Search', view:'search',    icon: iconSearch() },
    ]},
    { section: 'Connect', group:'connect', items: [
      { key:'connections',  label:'Connections',  view:'connections',  color:'var(--green)', icon: iconPlug() },
      { key:'integrations', label:'Integrations', view:'integrations', color:'var(--teal)',  icon: iconIntegrations() },
      { key:'settings-group', label:'Settings', icon: iconSettings(), children: [
        { key:'project-settings',  label:'General',           view:'project-settings',  color:'var(--amber)',  icon: iconSettings() },
        { key:'notifications',     label:'Notifications',     view:'notifications',     color:'var(--teal)',   icon: iconBell() },
        { key:'system-parameters', label:'System Parameters', view:'system-parameters', color:'var(--accent)', icon: iconParams() },
      ]},
    ]},
    { section: 'Map & Build', group:'build', items: [
      { key:'object-mapping',     label:'Object Mapping', href:'/object_mapping.html',     color:'var(--teal)',   icon: iconArrows() },
      { key:'sql-editor',         label:'SQL Editor',     href:'/sql-editor.html',         color:'var(--teal)',   icon: iconCode() },
      { key:'agentive-migration', label:'AI Assist',      href:'/agentive_migration.html', color:'var(--accent)', icon: iconHand(), requiresAiEnabled: true },
      { key:'coworker',           label:'AI Workspace',   href:'/coworker.html',           color:'var(--accent)', icon: svg('<path d="M2.5 3.5h11a1 1 0 0 1 1 1v5a1 1 0 0 1-1 1H6l-3 2.5V10.5H2.5a1 1 0 0 1-1-1v-5a1 1 0 0 1 1-1Z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/><path d="M5.5 6.4h5M5.5 8.2h3" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/>') },
    ]},
    { section: 'Run', group:'run', items: [
      { key:'jobs-group', label:'Jobs', icon: iconPlay(), children: [
        { key:'jobs',            label:'All Jobs', view:'jobs',                  icon: iconList() },
        { key:'project-builder', label:'Execute',  href:'/project-builder.html', color:'var(--purple)', icon: iconPlay() },
      ]},
      // Project Planner was removed along with its page. With one child left
      // the "Planner & Schedules" expander earned nothing, so this is a plain
      // top-level item. The key stays `task-agent` — pages set
      // data-active="task-agent" and dashboard.html routes showView on it.
      { key:'task-agent', label:'Task Manager', view:'task-agent', color:'var(--yellow)', icon: iconClock() },
      { key:'server-migration', label:'Server Migration', view:'server-migration', color:'var(--accent)', icon: iconServerMigration() },
    ]},
    { section: 'Validate', group:'validate', items: [
      { key:'quality-group', label:'Data Quality', icon: iconQuality(), children: [
        { key:'data-quality',   label:'Quality Review', href:'/data-quality.html',   color:'var(--green)', icon: iconQuality() },
        { key:'data-cleansing', label:'Cleansing',      href:'/data-cleansing.html', color:'var(--teal)',  icon: iconClean() },
        { key:'validation',     label:'Validation',     href:'/validation.html',     color:'var(--amber)', icon: iconCheck() },
      ]},
      { key:'insights', label:'Data Insights', href:'/insights.html', color:'var(--purple)', icon: iconInsights() },
    ]},
    { section: 'Report & Govern', group:'govern', items: [
      { key:'reports-group', label:'Reports', icon: iconReport(), children: [
        { key:'report-builder',           label:'Report Builder',    href:'/reports.html',            color:'var(--amber)',  icon: iconReportBuilder() },
        { key:'reports',                  label:'Conversion Report', view:'reports',                  color:'var(--purple)', icon: iconReport() },
        { key:'project-summary-document', label:'Project Summary',   view:'project-summary-document', color:'var(--purple)', icon: iconDocument() },
      ]},
      { key:'inventory',        label:'Project Artifacts', view:'inventory',        icon: iconGrid() },
      { key:'privacy-security', label:'Governance',        view:'privacy-security', color:'var(--red)',   icon: iconShield() },
      { key:'audit',            label:'Audit Log',         view:'audit',            color:'var(--text2)', icon: iconAuditLog() },
      { key:'monitoring-group', label:'Monitoring', icon: iconChart(), children: [
        { key:'performance', label:'Performance', href:'/performance.html', color:'var(--teal)',  icon: iconChart() },
        { key:'diagnostics', label:'Diagnostics', view:'diagnostics',       color:'var(--text2)', icon: iconPulse() },
      ]},
    ]},
  ];

  // Utility destinations that live in the ACCOUNT MENU rather than the rail
  // (with Cookie preferences and Subscription), keeping the workflow rail to
  // workflow only. Rendered by buildUserMenu; still reachable through
  // findItem so keys resolve for tests and setActive.
  //
  // `navClass:'a11y-trigger'` must survive the move: cygenix-a11y.js's
  // outside-click handler skips elements matching it, otherwise the same
  // click that opens the panel would immediately close it again.
  const ACCOUNT_NAV = [
    { key:'help',          label:'Help Guide',    action:'open-help',     color:'var(--accent)', icon: iconHelp() },
    { key:'accessibility', label:'Accessibility', action:'accessibility', color:'var(--text3)',  icon: iconA11y(), navClass:'a11y-trigger' },
  ];

  // ── Icons (returns SVG string) ──────────────────────────────────────────
  function svg(body){ return '<svg class="cyg-nav-icon" viewBox="0 0 16 16" fill="none">'+body+'</svg>'; }
  function iconDashboard(){    return svg('<rect x="2" y="2" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="9" y="2" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="2" y="9" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="9" y="9" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/>'); }
  function iconBell(){         return svg('<path d="M8 2.2a3.6 3.6 0 0 0-3.6 3.6c0 2.6-.9 3.6-1.4 4.1a.5.5 0 0 0 .35.85h9.3a.5.5 0 0 0 .35-.85c-.5-.5-1.4-1.5-1.4-4.1A3.6 3.6 0 0 0 8 2.2Z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/><path d="M6.6 13a1.5 1.5 0 0 0 2.8 0" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/>'); }
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
  // Accessibility "person" mark — matches the footer trigger it replaces.
  function iconA11y(){         return svg('<circle cx="8" cy="8" r="6.6" stroke="currentColor" stroke-width="1.2"/><circle cx="8" cy="4.6" r="0.95" fill="currentColor"/><path d="M4.3 6.1c1.2.6 2.4.8 3.7.8s2.5-.2 3.7-.8M8 6.9V10m0 0l-1.5 2.3M8 10l1.5 2.3" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/>'); }
  // Hard-drive glyph for the permanent Drive shortcut at the top of the rail.
  function iconDrive(){        return svg('<rect x="1.8" y="4.6" width="12.4" height="6.8" rx="1.4" stroke="currentColor" stroke-width="1.2"/><path d="M4 8h4.5" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/><circle cx="11.4" cy="8" r="0.95" fill="currentColor"/>'); }

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
      /* When collapsed, the brand steps aside so the expand toggle is the
         single, always-visible control in the header (otherwise there is no
         way to re-open the menu). */
      .cyg-sidebar.collapsed .cyg-brand{ display:none; }
      .cyg-brand{ display:flex;align-items:center;gap:10px;min-width:0;flex:1;overflow:hidden;text-decoration:none; }
      .cyg-brand-mark{
        width:34px;height:34px;min-width:34px;border-radius:9px;
        background:linear-gradient(140deg,#6d5df2,#4a7cf3);
        display:flex;align-items:center;justify-content:center;
        box-shadow:0 4px 14px -4px rgba(74,91,214,0.6);
      }
      .cyg-brand-mark svg{ width:21px;height:21px;display:block; }
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
      /* Stay visible & centered when collapsed so the menu can always be
         re-expanded (the arrow flips to ❯ via JS). */
      .cyg-sidebar.collapsed .cyg-sidebar-toggle{ margin-left:0;width:32px;height:32px;font-size:14px; }

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
        display:flex;align-items:center;gap:11px;width:100%;
        padding:8px;border:none;background:none;border-radius:10px;cursor:pointer;
        font:inherit;text-align:left;
        transition:background 0.15s;text-decoration:none;
      }
      .cyg-user-chip:hover{ background:rgba(255,255,255,0.07); }
      .cyg-user-chev{ margin-left:auto;display:flex;color:rgba(255,255,255,0.4);flex-shrink:0; }
      .cyg-user-chev svg{ width:16px;height:16px; }
      .cyg-sidebar.collapsed .cyg-user-chev{ display:none; }

      /* Account menu — light popover, appended to <body> so it escapes the
         sidebar's overflow:hidden and can open above the footer chip. */
      .cyg-user-menu{
        position:fixed;z-index:1000;min-width:222px;
        background:#fff;border:1px solid #e4e7ee;border-radius:12px;
        box-shadow:0 14px 36px -10px rgba(20,24,40,0.38);
        padding:6px;display:none;
        font-family:'IBM Plex Sans','Helvetica Neue',Arial,sans-serif;
      }
      .cyg-user-menu.open{ display:block;animation:cygMenuIn .12s ease; }
      @keyframes cygMenuIn{ from{opacity:0;transform:translateY(4px)} to{opacity:1;transform:translateY(0)} }
      .cyg-user-menu-email{ padding:9px 10px 7px;font-size:12px;color:#7a8090;white-space:nowrap;overflow:hidden;text-overflow:ellipsis; }
      .cyg-user-menu-sep{ height:1px;background:#f0f1f5;margin:4px 2px; }
      .cyg-user-menu-item{ display:flex;align-items:center;justify-content:space-between;gap:10px;width:100%;
        padding:9px 10px;border:none;background:none;cursor:pointer;font:inherit;font-size:13px;
        color:#2a2e3a;border-radius:8px;text-align:left;text-decoration:none; }
      .cyg-user-menu-item:hover{ background:#f4f5f8;color:#2a2e3a; }
      .cyg-user-menu-item.danger{ color:#c0392b; }
      .cyg-user-menu-item.danger:hover{ background:rgba(192,57,43,0.08);color:#c0392b; }
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

      /* Permanent Drive shortcut — pinned under the brand header. */
      /* ── Expandable groups (nav redesign) ─────────────────────────── */
      .cyg-nav-parent .cyg-nav-chev{
        margin-left:auto;font-size:10px;color:var(--cyg-fg-dim,#8b90a0);
        transition:transform 0.15s;
      }
      .cyg-nav-parent.child-active{ color:var(--cyg-fg-strong,#fff); }
      .cyg-nav-children{ display:none; }
      .cyg-nav-children.open{ display:block; }
      .cyg-nav-child{
        padding-left:38px;font-size:13px;
      }
      .cyg-nav-child::before{
        content:'';position:absolute;left:22px;top:0;bottom:0;width:1px;
        background:rgba(255,255,255,0.10);
      }
      .cyg-sidebar.collapsed .cyg-nav-children{ display:none !important; }
      .cyg-sidebar.collapsed .cyg-nav-parent .cyg-nav-chev{ display:none; }
      /* ── Project switcher ─────────────────────────────────────────── */
      .cyg-proj-area{ flex-shrink:0;padding:8px 12px 0; }
      .cyg-proj-btn{
        display:flex;align-items:center;gap:9px;width:100%;
        background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.12);
        border-radius:9px;padding:8px 10px;
        font-size:12.5px;font-weight:500;color:var(--cyg-fg,#cbd0dc);
        cursor:pointer;text-align:left;font-family:inherit;
      }
      .cyg-proj-btn:hover{ background:rgba(255,255,255,0.09); }
      .cyg-proj-btn .cyg-nav-item-label{ flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap; }
      .cyg-proj-chev{ color:var(--cyg-fg-dim,#8b90a0);font-size:10px; }
      .cyg-proj-ic{ font-size:12px; }
      .cyg-sidebar.collapsed .cyg-proj-area{ padding:8px 6px 0; }
      .cyg-sidebar.collapsed .cyg-proj-btn{ justify-content:center;padding:8px 0;gap:0; }
      .cyg-sidebar.collapsed .cyg-proj-btn .cyg-nav-item-label,
      .cyg-sidebar.collapsed .cyg-proj-btn .cyg-proj-chev{ display:none; }
      .cyg-drive-area{ flex-shrink:0;padding:6px 12px 2px; }
      .cyg-drive-btn{
        display:flex;align-items:center;gap:12px;width:100%;box-sizing:border-box;
        padding:11px 13px;border-radius:11px;
        border:1px solid rgba(255,255,255,0.12);
        background:rgba(255,255,255,0.04);
        color:var(--cyg-fg-strong);
        font-size:14px;font-weight:600;letter-spacing:-0.01em;
        text-decoration:none;cursor:pointer;
        transition:background 0.15s,border-color 0.15s;
      }
      .cyg-drive-btn:hover{ background:rgba(255,255,255,0.10);border-color:rgba(255,255,255,0.22); }
      .cyg-drive-btn .cyg-nav-icon{ width:18px;height:18px;flex-shrink:0;opacity:0.95; }
      .cyg-sidebar.collapsed .cyg-drive-area{ padding:6px 0 2px; }
      .cyg-sidebar.collapsed .cyg-drive-btn{ justify-content:center;padding:11px 0;gap:0; }
      .cyg-sidebar.collapsed .cyg-drive-btn .cyg-nav-item-label{ display:none; }
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

  // Make sure the shared Drive overlay module is loaded, then run `cb`. This
  // lets the Drive button open the Co-Worker Drive on top of ANY page instead
  // of navigating away. coworker.html has its own richer Drive, so we never
  // need the overlay there (see wireDriveButton).
  function ensureDriveModal(cb){
    if (window.CygenixDriveModal){ if (cb) cb(); return; }
    let s = document.getElementById('cygenix-drive-modal-js');
    if (!s){
      s = document.createElement('script');
      s.id = 'cygenix-drive-modal-js'; s.src = '/cygenix-drive-modal.js';
      document.head.appendChild(s);
    }
    if (cb) s.addEventListener('load', cb, { once: true });
  }

  function buildFooter(){
    const chev = svg('<path d="M5 6.5l3-3 3 3M5 9.5l3 3 3-3" stroke="currentColor" stroke-width="1.3" fill="none" stroke-linecap="round" stroke-linejoin="round"/>');
    // Utilities (Help, Accessibility, Cookies) live in the account menu — the
    // rail below the workflow groups is now just the account chip.
    return `<div class="cyg-sidebar-foot">
      <button type="button" class="cyg-user-chip" id="cyg-user-chip" aria-haspopup="menu" aria-expanded="false" title="Account">

        <span class="cyg-user-av" id="cyg-user-av">CY</span>
        <span class="cyg-user-meta"><b id="cyg-user-name">Account</b><span id="cyg-user-sub">Migration Console</span></span>
        <span class="cyg-user-chev">${chev}</span>
      </button>
    </div>`;
  }

  // Permanent Files shortcut, pinned at the very top of the rail (under the
  // brand header). Opens the shared virtual Drive from anywhere in the app.
  // Labelled "Files" (nav review): "Drive" collided with the Co-Worker page
  // and read as a branded term; this is literally where your files live.
  function buildDriveButton(){
    return `<div class="cyg-drive-area">
      <a class="cyg-drive-btn" id="cyg-drive-btn" href="/coworker.html#drive" title="Your files — the shared Drive, available on every machine you sign in on">
        ${iconDrive()}<span class="cyg-nav-item-label">Files</span>
      </a>
    </div>`;
  }

  // ── Project switcher (nav review) ───────────────────────────────────────
  // Pinned context: which project am I acting on? Reads the same
  // cygenix_projects / cygenix_active_project_id keys the dashboard and
  // projects.html use. Selecting a project sets the active id and reloads so
  // every page picks up the new context; "All projects…" goes to the manager.
  function activeProjectName(){
    try {
      const projects = JSON.parse(localStorage.getItem('cygenix_projects') || '[]');
      const id = localStorage.getItem('cygenix_active_project_id') || '';
      const p = projects.find(x => x && x.id === id) || projects[0];
      return p && p.name ? p.name : '';
    } catch { return ''; }
  }
  function buildProjectSwitcher(){
    const name = activeProjectName();
    return `<div class="cyg-proj-area">
      <button type="button" class="cyg-proj-btn" id="cyg-proj-btn" aria-haspopup="menu" aria-expanded="false"
              title="Active project — everything below acts on this project">
        <span class="cyg-proj-ic">🏢</span>
        <span class="cyg-nav-item-label" id="cyg-proj-name">${escapeHtml(name || 'No project selected')}</span>
        <span class="cyg-proj-chev">▾</span>
      </button>
    </div>`;
  }
  function wireProjectSwitcher(root){
    const btn = root.querySelector('#cyg-proj-btn');
    if (!btn) return;
    btn.addEventListener('click', (e) => {
      e.preventDefault(); e.stopPropagation();
      let menu = document.getElementById('cyg-proj-menu');
      if (menu && menu.classList.contains('open')) { menu.classList.remove('open'); return; }
      if (!menu){
        menu = document.createElement('div');
        menu.id = 'cyg-proj-menu';
        menu.className = 'cyg-user-menu';   // same look as the account menu
        document.body.appendChild(menu);
      }
      let projects = [];
      try { projects = JSON.parse(localStorage.getItem('cygenix_projects') || '[]'); } catch {}
      const activeId = localStorage.getItem('cygenix_active_project_id') || '';
      menu.innerHTML =
        (projects.length
          ? projects.map(p =>
              `<button class="cyg-user-menu-item" type="button" data-proj="${escapeHtml(p.id)}">` +
              `${escapeHtml(p.name || '(unnamed project)')}${p.id === activeId ? ' ✓' : ''}</button>`
            ).join('')
          : '<div class="cyg-user-menu-email">No projects yet</div>') +
        '<div class="cyg-user-menu-sep"></div>' +
        '<a class="cyg-user-menu-item" href="/projects.html">All projects…</a>';
      const r = btn.getBoundingClientRect();
      menu.style.left = Math.max(8, r.left) + 'px';
      menu.style.top = (r.bottom + 6) + 'px';
      menu.style.bottom = 'auto';
      menu.classList.add('open');
      menu.querySelectorAll('[data-proj]').forEach(el => {
        el.addEventListener('click', () => {
          try { localStorage.setItem('cygenix_active_project_id', el.dataset.proj); } catch {}
          menu.classList.remove('open');
          // Reload so every page (not just the dashboard) re-reads project context.
          window.location.reload();
        });
      });
      setTimeout(() => {
        const close = (ev) => { if (!menu.contains(ev.target) && ev.target !== btn){ menu.classList.remove('open'); document.removeEventListener('click', close); } };
        document.addEventListener('click', close);
      }, 0);
    });
  }

  // ── HTML build ──────────────────────────────────────────────────────────
  function buildHTML(activeKey){
    const head = `<div class="cyg-sidebar-head">
      <a class="cyg-brand" href="/dashboard.html" aria-label="Cygenix — Migration Console">
        <span class="cyg-brand-mark"><svg viewBox="0 0 32 32" fill="none" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M9 10.5 14 16 9 21.5" stroke="#fff" stroke-opacity=".45"/><path d="M13 10.5 18 16 13 21.5" stroke="#fff" stroke-opacity=".72"/><path d="M17 10.5 22 16 17 21.5" stroke="#fff"/></svg></span>
        <span class="cyg-brand-word"><b>Cygenix</b><span>Migration Console</span></span>
      </a>
      <button id="cyg-sidebar-toggle" class="cyg-sidebar-toggle" aria-label="Collapse sidebar">❮</button>
    </div>`;
    const body = NAV.map(sec => buildSection(sec, activeKey)).join('');
    return head + buildProjectSwitcher() + buildDriveButton()
      + `<div class="cyg-sidebar-scroll">${body}</div>` + buildFooter(activeKey);
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
      const raw = sessionStorage.getItem('cygenix_user') || localStorage.getItem('cygenix_user');
      if (raw){
        const u = JSON.parse(raw);
        name  = (u.user_metadata && u.user_metadata.full_name) || (u.email ? u.email.split('@')[0] : name);
        email = u.email || '';
        if (u.plan || (u.user_metadata && u.user_metadata.plan)) plan = u.plan || u.user_metadata.plan;
      } else {
        // cygenix_active_user holds a PLAIN email string, not JSON — the old
        // code JSON.parse'd it, always threw, and showed "Account" instead.
        const activeEmail = (localStorage.getItem('cygenix_active_user') || '').trim();
        if (activeEmail) { email = activeEmail; name = activeEmail.split('@')[0]; }
      }
    } catch {}
    const initials = name.trim().split(/\s+/).map(w => w[0]).join('').slice(0,2).toUpperCase() || 'CY';
    nameEl.textContent = name;
    if (subEl) subEl.textContent = email || plan;
    if (avEl) avEl.textContent = initials;
  }

  // ── Account menu (moved here from the old top-right topbar pill) ──────────
  // Built once and appended to <body> so it can sit above the footer chip
  // without being clipped by the sidebar's overflow:hidden.
  function buildUserMenu(){
    let menu = document.getElementById('cyg-user-menu');
    if (menu) return menu;
    menu = document.createElement('div');
    menu.className = 'cyg-user-menu';
    menu.id = 'cyg-user-menu';
    menu.setAttribute('role', 'menu');
    menu.innerHTML =
      '<div class="cyg-user-menu-email" id="cyg-user-menu-email"></div>' +
      '<div class="cyg-user-menu-sep"></div>' +
      '<a class="cyg-user-menu-item" role="menuitem" href="/projects.html">My projects</a>' +
      '<button class="cyg-user-menu-item" role="menuitem" type="button" id="cyg-user-menu-sub">Subscription</button>' +
      '<div class="cyg-user-menu-sep"></div>' +
      // Help / Accessibility, moved out of the rail. Rendered from
      // ACCOUNT_NAV so the item definitions (and the a11y-trigger class the
      // accessibility panel depends on) stay in one place.
      ACCOUNT_NAV.filter(isItemVisible).map(it =>
        `<button class="cyg-user-menu-item${it.navClass ? ' ' + it.navClass : ''}" role="menuitem" type="button"` +
        `${it.action === 'accessibility' ? ' aria-haspopup="dialog" aria-expanded="false"' : ''}` +
        ` data-acct-key="${it.key}">${escapeHtml(it.label)}</button>`
      ).join('') +
      '<button class="cyg-user-menu-item" role="menuitem" type="button" id="cyg-user-menu-cookies">Cookie preferences</button>' +
      '<div class="cyg-user-menu-sep"></div>' +
      '<button class="cyg-user-menu-item danger" role="menuitem" type="button" id="cyg-user-menu-signout">Sign out</button>';
    document.body.appendChild(menu);
    menu.querySelectorAll('[data-acct-key]').forEach(el => {
      el.addEventListener('click', (e) => {
        e.preventDefault();
        const item = findItem(el.dataset.acctKey);
        // Close the menu BEFORE acting: the accessibility panel anchors to the
        // viewport and would otherwise open behind an already-open menu.
        closeUserMenu();
        if (item) handleClick(item);
      });
    });
    menu.querySelector('#cyg-user-menu-cookies').addEventListener('click', (e) => {
      e.preventDefault(); closeUserMenu();
      if (typeof window.openCookiePreferences === 'function') window.openCookiePreferences();
    });
    menu.querySelector('#cyg-user-menu-sub').addEventListener('click', (e) => {
      e.preventDefault(); closeUserMenu();
      if (typeof window.openBillingPortal === 'function') { try { window.openBillingPortal(e.currentTarget); return; } catch(_){} }
      window.location.href = '/pick-plan.html';
    });
    menu.querySelector('#cyg-user-menu-signout').addEventListener('click', (e) => {
      e.preventDefault(); closeUserMenu(); sidebarSignOut();
    });
    return menu;
  }

  function positionUserMenu(menu, chip){
    const r = chip.getBoundingClientRect();
    menu.style.left   = Math.max(8, r.left) + 'px';
    menu.style.bottom = (window.innerHeight - r.top + 8) + 'px';
    menu.style.top    = 'auto';
    menu.style.maxWidth = Math.max(180, window.innerWidth - r.left - 16) + 'px';
  }

  function openUserMenu(chip){
    const menu = buildUserMenu();
    const sub = document.getElementById('cyg-user-sub');
    const emailEl = menu.querySelector('#cyg-user-menu-email');
    if (emailEl) emailEl.textContent = (sub && sub.textContent) || 'Account';
    positionUserMenu(menu, chip);
    menu.classList.add('open');
    chip.setAttribute('aria-expanded', 'true');
    setTimeout(() => document.addEventListener('click', outsideUserMenu), 0);
    window.addEventListener('resize', repositionUserMenu);
  }

  function closeUserMenu(){
    const menu = document.getElementById('cyg-user-menu');
    if (menu) menu.classList.remove('open');
    const chip = document.getElementById('cyg-user-chip');
    if (chip) chip.setAttribute('aria-expanded', 'false');
    document.removeEventListener('click', outsideUserMenu);
    window.removeEventListener('resize', repositionUserMenu);
  }

  function outsideUserMenu(e){
    const menu = document.getElementById('cyg-user-menu');
    const chip = document.getElementById('cyg-user-chip');
    if (!menu) return;
    if (menu.contains(e.target) || (chip && chip.contains(e.target))) return;
    closeUserMenu();
  }
  function repositionUserMenu(){
    const menu = document.getElementById('cyg-user-menu');
    const chip = document.getElementById('cyg-user-chip');
    if (menu && chip && menu.classList.contains('open')) positionUserMenu(menu, chip);
  }
  function toggleUserMenu(chip){
    const menu = document.getElementById('cyg-user-menu');
    if (menu && menu.classList.contains('open')) closeUserMenu();
    else openUserMenu(chip);
  }

  // Self-contained sign-out so it works on every page (most pages don't load
  // the MSAL library). Delegates to a page-provided window.signOut() when one
  // exists (e.g. dashboard's MSAL logout), otherwise clears session + redirects
  // through the Entra logout endpoint — mirrors dashboard's own fallback path.
  function sidebarSignOut(){
    if (typeof window.signOut === 'function'){ try { window.signOut(); return; } catch(_){} }
    try { sessionStorage.setItem('cygenix_just_signed_out', '1'); } catch(_){}
    ['cygenix_token','cygenix_user','cygenix_expires','cygenix_active_project',
     'cygenix_entra_account','cygenix_active_user'].forEach(k => {
      try { sessionStorage.removeItem(k); localStorage.removeItem(k); } catch(_){}
    });
    const CLIENT_ID = 'f3478996-b2b5-4b21-9a23-a6b97a0e5b13';
    try {
      Object.keys(localStorage).forEach(k => {
        if (k.includes('msal') || k.includes(CLIENT_ID) || k[0] === '{') localStorage.removeItem(k);
      });
    } catch(_){}
    window.location.href =
      'https://cygenix.ciamlogin.com/fc8dfc7a-645f-4a5c-8f59-6762f97c803f/oauth2/v2.0/logout'
      + '?post_logout_redirect_uri=' + encodeURIComponent(window.location.origin + '/login.html');
  }

  // Hide the legacy top-right user pill on pages where the sidebar renders —
  // its menu now lives on the sidebar chip. Pages without a sidebar keep their
  // own pill untouched.
  function hideTopbarUserPill(){
    document.querySelectorAll('#user-pill, .user-pill').forEach(el => { el.style.display = 'none'; });
  }

  function wireUserChip(root){
    const chip = root.querySelector('#cyg-user-chip');
    if (!chip) return;
    chip.addEventListener('click', (e) => {
      e.preventDefault(); e.stopPropagation();
      toggleUserMenu(chip);
    });
  }

  // Open the Co-Worker Drive as an OVERLAY on top of the current page, rather
  // than navigating to coworker.html. On the co-worker page itself we use its
  // own native Drive (richer — it wires into the workspace canvas); everywhere
  // else we use the shared overlay module. The href stays as a no-JS fallback.
  function wireDriveButton(root){
    const btn = root.querySelector('#cyg-drive-btn');
    if (!btn) return;
    btn.addEventListener('click', (e) => {
      const onCoworker = /\/coworker\.html?$/.test(location.pathname);
      if (onCoworker && typeof window.openDrive === 'function'){
        e.preventDefault(); window.openDrive(); return;
      }
      if (window.CygenixDriveModal){
        e.preventDefault(); window.CygenixDriveModal.open(); return;
      }
      // Module not loaded yet — load it, then open. Falls back to navigating
      // to coworker.html#drive only if the module fails to load.
      e.preventDefault();
      ensureDriveModal(() => {
        if (window.CygenixDriveModal) window.CygenixDriveModal.open();
        else window.location.href = '/coworker.html#drive';
      });
    });
  }

  function buildSection(sec, activeKey){
    const visibleItems = (sec.items || []).filter(isItemVisible);
    if (!visibleItems.length) return '';
    const labelHtml = sec.section
      ? `<div class="cyg-nav-label">${sec.section}</div>`
      : '';
    const itemsHtml = visibleItems.map(it =>
      it.children ? buildParent(it, activeKey) : buildItem(it, activeKey)
    ).join('');
    return `<div class="cyg-nav-section">${labelHtml}${itemsHtml}</div>`;
  }

  // ── Expandable groups (secondary navigation) ────────────────────────────
  // Parents render a chevron and toggle their children open/closed; the open
  // set persists per browser. A group holding the active page is always
  // forced open so the current location is never hidden.
  const OPEN_KEY = 'cygenix_sidebar_open_groups';
  function getOpenGroups(){
    try { const a = JSON.parse(localStorage.getItem(OPEN_KEY) || '[]'); return Array.isArray(a) ? a : []; }
    catch { return []; }
  }
  function setGroupOpen(key, open){
    const set = new Set(getOpenGroups());
    if (open) set.add(key); else set.delete(key);
    try { localStorage.setItem(OPEN_KEY, JSON.stringify([...set])); } catch {}
  }

  function buildParent(item, activeKey){
    const kids = (item.children || []).filter(isItemVisible);
    if (!kids.length) return '';
    const childActive = kids.some(k => k.key === activeKey);
    const open = childActive || getOpenGroups().includes(item.key);
    const chev = `<span class="cyg-nav-chev">${open ? '▾' : '▸'}</span>`;
    const kidsHtml = kids.map(k => buildItem(k, activeKey, true)).join('');
    return `
      <div class="cyg-nav-item cyg-nav-parent${childActive ? ' child-active' : ''}"
           data-parent="${item.key}" tabindex="0"
           aria-expanded="${open ? 'true' : 'false'}">
        ${item.icon || ''}
        <span class="cyg-nav-item-label">${escapeHtml(item.label)}</span>
        ${chev}
      </div>
      <div class="cyg-nav-children${open ? ' open' : ''}" data-children="${item.key}">
        ${kidsHtml}
      </div>`;
  }

  function buildItem(item, activeKey, isChild){
    const isActive = item.key === activeKey ? ' active' : '';
    const childCls = isChild ? ' cyg-nav-child' : '';
    // Oracle look: monochrome nav icons (neutral grey), red only on the
    // active item. We intentionally ignore the per-item accent colour so the
    // sidebar reads as one calm, professional column rather than a rainbow.
    const styleAttr = '';
    const badgeHtml = item.badgeId
      ? `<span class="cyg-nav-badge" id="${item.badgeId}" aria-live="polite"></span>`
      : '';
    // Optional extra class (e.g. `a11y-trigger` so the accessibility panel's
    // outside-click handler recognises this item as its own toggle).
    const extraCls  = item.navClass ? ' ' + item.navClass : '';
    const extraAttr = item.action === 'accessibility' ? ' aria-haspopup="dialog" aria-expanded="false"' : '';
    return `
      <div class="cyg-nav-item${isActive}${extraCls}${childCls}"
           data-key="${item.key}"
           tabindex="0"${styleAttr}${extraAttr}>
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
        // Group parents toggle their children rather than navigating.
        const parentKey = el.dataset.parent;
        if (parentKey){
          // In the collapsed rail there is nowhere to show children —
          // expand the rail first so the toggle has a visible effect.
          if (sidebarEl.classList.contains('collapsed')){
            sidebarEl.classList.remove('collapsed');
            setCollapsed(false);
            document.body.classList.remove('cyg-collapsed');
            const t = sidebarEl.querySelector('#cyg-sidebar-toggle');
            if (t) t.textContent = '❮';
          }
          const kids = sidebarEl.querySelector(`.cyg-nav-children[data-children="${parentKey}"]`);
          const nowOpen = kids ? !kids.classList.contains('open') : true;
          if (kids) kids.classList.toggle('open', nowOpen);
          el.setAttribute('aria-expanded', nowOpen ? 'true' : 'false');
          const chev = el.querySelector('.cyg-nav-chev');
          if (chev) chev.textContent = nowOpen ? '▾' : '▸';
          setGroupOpen(parentKey, nowOpen);
          return;
        }
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
      for (const it of sec.items){
        if (it.key === key) return it;
        if (it.children){
          for (const c of it.children){ if (c.key === key) return c; }
        }
      }
    }
    for (const it of ACCOUNT_NAV){ if (it.key === key) return it; }
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
    if (item.action === 'accessibility'){
      // The a11y engine is loaded on mount via ensureA11y(); toggle its panel.
      if (window.CygenixA11y){ window.CygenixA11y.toggle(); }
      else { ensureA11y(); setTimeout(() => { if (window.CygenixA11y) window.CygenixA11y.toggle(); }, 150); }
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
    // If the newly-active item is inside a group, force that group open and
    // flag its parent — the current location must never be hidden.
    sidebar.querySelectorAll('.cyg-nav-parent').forEach(p => p.classList.remove('child-active'));
    const activeEl = sidebar.querySelector(`.cyg-nav-item[data-key="${key}"]`);
    const kidsWrap = activeEl && activeEl.closest('.cyg-nav-children');
    if (kidsWrap){
      kidsWrap.classList.add('open');
      const parent = sidebar.querySelector(`.cyg-nav-parent[data-parent="${kidsWrap.dataset.children}"]`);
      if (parent){
        parent.classList.add('child-active');
        parent.setAttribute('aria-expanded', 'true');
        const chev = parent.querySelector('.cyg-nav-chev');
        if (chev) chev.textContent = '▾';
      }
    }
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
    wireUserChip(aside);
    wireDriveButton(aside);
    wireProjectSwitcher(aside);
    hideTopbarUserPill();
    ensureA11y();
    // Preload the Drive overlay so the first click is instant (skip the
    // co-worker page, which ships its own native Drive).
    if (!/\/coworker\.html?$/.test(location.pathname)) ensureDriveModal();
  }

  // The Project Planner badge module lived here. It counted plan items due
  // today, tracked which the user had "seen", listened on two storage keys and
  // a custom event, and re-checked on a 60-second interval. All of it existed
  // for one nav item, and that item and its page are gone — so it ran forever
  // on every page to update a badge element that no longer exists.
  //
  // localStorage['cygenix_project_plan'] is deliberately left alone: it is the
  // user's own data, and deleting a feature is not a reason to destroy it.

  // Public API (useful for dashboard to call on showView)
  window.CygenixSidebar = {
    mount,
    setActive: updateActive,
    isCollapsed,
    // Exposed for structural tests (tests/sidebar-nav.test.js): the nav tree
    // and footer as data, so key coverage can be asserted without a DOM.
    __nav: NAV, __accountNav: ACCOUNT_NAV, __findItem: findItem,
    setCollapsed: (on) => {
      const el = document.querySelector('.cyg-sidebar');
      if (el) el.classList.toggle('collapsed', !!on);
      setCollapsed(on);
      const toggle = el && el.querySelector('#cyg-sidebar-toggle');
      if (toggle) toggle.textContent = on ? '❯' : '❮';
    },
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
      wireUserChip(replacement);
      wireDriveButton(replacement);
      hideTopbarUserPill();
    }
  };

  // ── Instant navigation ────────────────────────────────────────────────────
  // These pages are large static HTML documents (the dashboard is ~1MB), so a
  // normal click pays the full download + parse cost every time. We warm the
  // browser cache for a page the moment the user shows intent (hovers/taps a
  // link), so the actual click has little or nothing left to fetch — navigation
  // feels near-instant. We PREFETCH (cache the HTML) rather than PRERENDER
  // (execute it) so no page's on-load logic runs early. Fully progressive:
  // browsers without either feature simply navigate as before.
  function setupInstantNav(){
    try {
      if (window.__cygInstantNav) return; window.__cygInstantNav = true;
      // Don't prefetch links to auth flows, the API, file downloads, external
      // sites, new tabs, or anything explicitly opted out with data-no-prefetch.
      var SKIP = /^\/(api|\.netlify)\//i;
      var SKIP_EXT = /\.(zip|sql|csv|tsv|pdf|xlsx?|json|bak|gz)$/i;

      // Modern path: the Speculation Rules API (Chrome/Edge) prefetches on
      // hover intent, with the browser managing concurrency and eviction.
      if (typeof HTMLScriptElement !== 'undefined' && HTMLScriptElement.supports && HTMLScriptElement.supports('speculationrules')) {
        var rules = { prefetch: [{
          source: 'document',
          eagerness: 'moderate',
          where: { and: [
            { href_matches: '/*' },
            { not: { href_matches: '/api/*' } },
            { not: { href_matches: '/.netlify/*' } },
            { not: { selector_matches: '[data-no-prefetch]' } },
            { not: { selector_matches: '[target="_blank"]' } },
            { not: { selector_matches: '[download]' } }
          ] }
        }] };
        var sr = document.createElement('script');
        sr.type = 'speculationrules';
        sr.textContent = JSON.stringify(rules);
        (document.head || document.documentElement).appendChild(sr);
        return;
      }

      // Fallback (Safari/Firefox): add <link rel="prefetch"> on hover / touch.
      var seen = {};
      function warm(e){
        var a = e.target && e.target.closest && e.target.closest('a[href]');
        if (!a || a.target === '_blank' || a.hasAttribute('download') || a.hasAttribute('data-no-prefetch')) return;
        var url; try { url = new URL(a.href, location.href); } catch (_) { return; }
        if (url.origin !== location.origin) return;
        if (url.pathname === location.pathname) return;
        if (SKIP.test(url.pathname) || SKIP_EXT.test(url.pathname)) return;
        if (seen[url.href]) return; seen[url.href] = 1;
        var l = document.createElement('link');
        l.rel = 'prefetch'; l.as = 'document'; l.href = url.href;
        (document.head || document.documentElement).appendChild(l);
      }
      document.addEventListener('pointerover', warm, { passive: true });
      document.addEventListener('touchstart', warm, { passive: true });
    } catch (_) {}
  }
  setupInstantNav();

  // Auto-mount on DOMContentLoaded (or immediately if already past)
  if (document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', mount);
  } else {
    mount();
  }

  // ── Background Drive sync ─────────────────────────────────────────────────
  // The Drive lives behind the sidebar button on every page, so its cloud
  // sync belongs at the same scope: load it once the page is idle so files
  // added on another machine are already pulled by the time the user opens
  // the Drive, and so CygenixDriveSync.diagnose() is available everywhere.
  //
  // Idle + after load, never during first paint. Loads the token helper
  // first on the pages that don't carry it; both scripts self-guard against
  // double-loading, so pages with static tags are unaffected.
  function loadDriveSyncWhenIdle(){
    function inject(id, src, done){
      if (document.getElementById(id)) { if (done) done(); return; }
      var s = document.createElement('script');
      s.id = id; s.src = src;
      if (done) { s.addEventListener('load', done, { once:true }); s.addEventListener('error', done, { once:true }); }
      (document.head || document.documentElement).appendChild(s);
    }
    function go(){
      try {
        if (window.CygenixDriveSync) return;
        if (typeof window.getCygenixIdToken === 'function') {
          inject('cygenix-drive-sync-js', '/cygenix-drive-sync.js');
        } else {
          inject('cygenix-auth-token-js', '/cygenix-auth-token.js', function(){
            inject('cygenix-drive-sync-js', '/cygenix-drive-sync.js');
          });
        }
      } catch (_) {}
    }
    var start = function(){
      if (window.requestIdleCallback) window.requestIdleCallback(go, { timeout: 4000 });
      else setTimeout(go, 2000);
    };
    if (document.readyState === 'complete') start();
    else window.addEventListener('load', start, { once:true });
  }
  loadDriveSyncWhenIdle();
})();
