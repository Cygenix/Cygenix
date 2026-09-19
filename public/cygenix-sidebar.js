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
 *     sessionStorage and navigates to /dashboard. Dashboard already
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
  // (padding-left / left:216px when open, 54px when collapsed) and with the
  // --cx-rail-w / --cx-masthead-h tokens in cygenix-console.css. Changing one
  // without the others misaligns every page; tests/console-design.test.js
  // pins all three to each other.
  const WIDTH_OPEN    = 216;
  const WIDTH_CLOSED  = 54;
  const MASTHEAD_H    = 60;

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
  // ── Structure (Sep-2026 design review) ──────────────────────────────────
  // FIVE GROUPS, NO EXPANDERS. The previous rail carried roughly thirty
  // destinations across seven sections and six fold-out groups, and the
  // comment history on it — Configurator, Packages, Insight moved up,
  // Planner flattened — was the symptom: a rail being asked to carry
  // orientation that the landing screen should provide. The review's
  // finding 03 named the cost. Analytics, Assurance, Quality Review,
  // Validation and Data Quality all promised the same answer; Reports,
  // Report Builder, Conversion Report and Project Artifacts all promised a
  // document. A person cannot choose between names that mean the same
  // thing, so they click through several.
  //
  // So the rail is now one destination per job. Every former expander child
  // becomes a TAB inside its destination screen (see TABS below), which
  // means no URL and no data-active key disappears: a page that mounts
  // with data-active="validation" still resolves, still highlights the
  // right rail item (Assurance, through ALIASES), and still shows Validation
  // as the current tab on that screen.
  //
  // MONOCHROME, deliberately. Items used to carry a per-item `color:` — teal,
  // green, amber, purple, red — purely as decoration, while the same five
  // hues carry meaning in badges and charts. When green means both
  // "Connections" and "passed", it has stopped signalling. The rail is ink;
  // hue is reserved for state. This is finding 04 and it is a data change:
  // there is no `color` property on any item any more, and a test refuses
  // one being added back.
  //
  // `key` is the identifier used for data-active matching and dashboard
  // showView. `href` means a separate page; `view` a dashboard view. Icons
  // are kept for the collapsed 54px rail only — the open rail is text.
  const NAV = [
    { section: null, group: 'home', items: [
      { key:'dashboard', label:'Home', view:'dashboard', icon: iconDashboard() },
    ]},
    // PLAN sits directly below Home, before Connect, because it is where an
    // engagement starts: the Configurator decides which modules are in scope
    // and sizes the work, and the Project plan is built from that output.
    // The handoff filed both as tabs under Reports, and nobody found them
    // there — a plan is not a report, and the person who asked for these to
    // sit at the top of the rail in August was right. Keys unchanged:
    // effort-estimator and project-plan-grid are what the pages mount with.
    { section: 'Plan', group:'plan', items: [
      { key:'effort-estimator',  label:'Configurator', href:'/configurator', icon: iconEstimator() },
      { key:'project-plan-grid', label:'Project plan', href:'/project-plan', icon: iconPlanGrid() },
    ]},
    { section: 'Connect', group:'connect', items: [
      { key:'connections', label:'Connections',             view:'connections', icon: iconPlug() },
      { key:'profiles',    label:'Profiles & integrations', href:'/profiles',   icon: iconShield() },
    ]},
    { section: 'Model', group:'model', items: [
      { key:'schema-explorer', label:'Schema explorer', href:'/schema-explorer', icon: iconGraph() },
      { key:'object-mapping',  label:'Object mapping',  href:'/object-mapping',  icon: iconArrows() },
      { key:'sql-editor',      label:'SQL editor',      href:'/sql-editor',      icon: iconCode() },
    ]},
    { section: 'Run', group:'run', items: [
      { key:'jobs',        label:'Jobs & packages', view:'jobs',        icon: iconPlay() },
      { key:'data-stream', label:'Data stream',     href:'/data-stream', icon: iconStream() },
      // The key stays `task-agent` — pages set data-active on it and the
      // dashboard routes showView on it. Only the label has ever changed.
      { key:'task-agent',  label:'Schedules',       view:'task-agent',  icon: iconClock() },
    ]},
    { section: 'Quality', group:'quality', items: [
      { key:'assurance',      label:'Assurance',              href:'/assurance',      icon: iconCheck() },
      { key:'data-cleansing', label:'Cleansing & enrichment', href:'/data-cleansing', icon: iconClean() },
    ]},
    { section: 'Govern', group:'govern', items: [
      { key:'report-builder', label:'Reports',   href:'/reports', icon: iconReport() },
      { key:'audit',          label:'Audit log', view:'audit',    icon: iconAuditLog(), requiresAuditRead:true },
    ]},
  ];

  // ── Where every former destination went ─────────────────────────────────
  // TABS: one entry per rail destination that absorbed others. Rendered as a
  // tab strip at the top of the destination screen (into #cyg-subnav-mount,
  // or above the first header if a page has not declared one). Each tab is a
  // real destination with its own key, so `findItem`, `navigate` and the
  // structural tests treat it exactly like a rail item — it has simply moved
  // from the rail into the screen it belongs to.
  //
  // A tab marked `away:true` leaves the current page rather than switching a
  // panel in place; it renders in accent-700 so the reader knows before
  // clicking (the Data generator tab on Connections is the model for this).
  const TABS = {
    'profiles': [
      { key:'profiles',     label:'Profiles',     href:'/profiles' },
      { key:'integrations', label:'Integrations', view:'integrations' },
    ],
    'object-mapping': [
      { key:'object-mapping',       label:'Object mapping',       href:'/object-mapping' },
      { key:'conversion-templates', label:'Conversion templates', href:'/conversion-templates' },
      // AI Assist is an action inside Object Mapping in the redesign, not a
      // destination. Until that page absorbs it, it stays reachable here and
      // keeps its feature flag.
      { key:'agentive-migration',   label:'AI assist',            href:'/agentive-migration', requiresAiEnabled:true },
    ],
    'jobs': [
      { key:'jobs',             label:'Jobs',             view:'jobs' },
      { key:'project-builder',  label:'Packages',         href:'/project-builder' },
      { key:'server-migration', label:'Server migration', view:'server-migration' },
      // Analytics reports on runs, so it sits with them. Home carries the
      // readiness figure itself; the analysis stays on its own page.
      { key:'analytics',        label:'Analytics',        href:'/analytics' },
    ],
    'data-stream': [
      { key:'data-stream',         label:'Streams',        href:'/data-stream' },
      { key:'data-stream-store',   label:'Stream store',   href:'/data-stream-store' },
      { key:'data-stream-events',  label:'Change events',  href:'/data-stream-events' },
      { key:'data-stream-monitor', label:'Stream monitor', href:'/data-stream-monitor' },
    ],
    'assurance': [
      { key:'assurance',    label:'Assurance',      href:'/assurance' },
      { key:'data-quality', label:'Quality review', href:'/data-quality' },
      { key:'validation',   label:'Validation',     href:'/validation' },
    ],
    'data-cleansing': [
      { key:'data-cleansing',  label:'Cleansing',  href:'/data-cleansing' },
      { key:'data-enrichment', label:'Enrichment', href:'/data-enrichment' },
    ],
    'report-builder': [
      { key:'report-builder',    label:'Report builder',    href:'/reports' },
      { key:'reports',           label:'Conversion report', view:'reports' },
      { key:'inventory',         label:'Project artifacts', view:'inventory' },
      // The Configurator and the Project plan were tabs here for a while;
      // they are rail items in the PLAN group now, and a key lives in one
      // place only.
    ],
    'audit': [
      { key:'audit',       label:'Audit log',   view:'audit' },
      { key:'performance', label:'Performance', href:'/performance' },
      { key:'diagnostics', label:'Diagnostics', view:'diagnostics' },
    ],
  };

  // ALIASES: former key → the rail item that now owns it, so setActive(key)
  // on any page that mounts with an old data-active still lights the right
  // row. Keys that resolve to a TABS entry are derived; the ones listed here
  // are the destinations that moved somewhere with no tab of their own.
  const ALIASES = {
    'search':                   'dashboard',   // the masthead field
    'project-summary-document': 'dashboard',
    'insights':                 'schema-explorer',
    // The Data Analyser was briefly its own rail item, at /data-analyser. It
    // lives INSIDE Connections, on the Data import tab: that tab is where a
    // file arrives, and "what is this file and what will break" is the
    // question to answer before importing it. /data-analyser still redirects
    // there (scripts/build-routes.js), so nothing bookmarked breaks. Do not
    // add it back as an item — one place for files.
    'data-analyser':            'connections',
  };

  // Settings and governance live in the ACCOUNT MENU rather than the rail,
  // with Help, Accessibility, Cookies and Subscription. Still reachable
  // through findItem so keys resolve for tests, setActive and the tour.
  //
  // `navClass:'a11y-trigger'` must survive: cygenix-a11y.js's outside-click
  // handler skips elements matching it, otherwise the same click that opens
  // the panel would immediately close it again.
  const ACCOUNT_NAV = [
    { key:'project-settings',  label:'General settings',  view:'project-settings',  icon: iconSettings() },
    { key:'notifications',     label:'Notifications',     view:'notifications',     icon: iconBell() },
    { key:'system-parameters', label:'System parameters', view:'system-parameters', icon: iconParams() },
    { key:'user-roles',        label:'Users & roles',     href:'/user-roles',       icon: iconUsers() },
    { key:'privacy-security',  label:'Governance',        view:'privacy-security',  icon: iconShield() },
    { key:'help',              label:'Help guide',        action:'open-help',       icon: iconHelp() },
    { key:'accessibility',     label:'Accessibility',     action:'accessibility',   icon: iconA11y(), navClass:'a11y-trigger' },
  ];

  // The masthead's region label. The Function App, Cosmos account and blob
  // storage are in UK South; the residency claim is the product's, and this
  // is where the product states it.
  const REGION_LABEL = 'UK South';

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
  // Data Stream. Three lanes flowing right with an arrowhead on two of them —
  // continuous and directional, the two things that separate a stream from a
  // batch. Distinct from iconArrows (Object Mapping, one arrow each way) and
  // iconServerMigration (two racks) at rail size.
  function iconStream(){       return svg('<path d="M2 4.2h7.5M2 8h11M2 11.8h7.5" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/><path d="M11.4 5.9 13.4 4.2l-2-1.7M11.4 13.5l2-1.7-2-1.7" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>'); }
  // Stream Store — a database cylinder, because that is what it is: a
  // retained, queryable buffer, not a folder.
  function iconStore(){        return svg('<ellipse cx="8" cy="4" rx="5" ry="2" stroke="currentColor" stroke-width="1.2"/><path d="M3 4v8c0 1.1 2.2 2 5 2s5-.9 5-2V4" stroke="currentColor" stroke-width="1.2"/><path d="M3 8c0 1.1 2.2 2 5 2s5-.9 5-2" stroke="currentColor" stroke-width="1.2"/>'); }
  // Change Events — a bolt, for the live tail.
  function iconBolt(){         return svg('<path d="M9 1.8 4 9h3.4l-.4 5.2L12 7H8.6z" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconParams(){       return svg('<circle cx="8" cy="8" r="2" stroke="currentColor" stroke-width="1.2"/><path d="M8 1v2M8 13v2M1 8h2M13 8h2M3.5 3.5l1.5 1.5M11 11l1.5 1.5M3.5 12.5l1.5-1.5M11 5l1.5-1.5" stroke="currentColor" stroke-width="1.1" stroke-linecap="round"/>'); }
  function iconShield(){       return svg('<path d="M8 2 L3 4 V8 C3 11 5 13 8 14 C11 13 13 11 13 8 V4 Z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/><path d="M6 8l1.5 1.5L10.5 7" stroke="currentColor" stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconIntegrations(){ return svg('<circle cx="4" cy="4" r="2" stroke="currentColor" stroke-width="1.2"/><circle cx="12" cy="4" r="2" stroke="currentColor" stroke-width="1.2"/><circle cx="4" cy="12" r="2" stroke="currentColor" stroke-width="1.2"/><circle cx="12" cy="12" r="2" stroke="currentColor" stroke-width="1.2"/><path d="M6 4h4M6 12h4M4 6v4M12 6v4" stroke="currentColor" stroke-width="1.1" stroke-linecap="round"/>'); }
  function iconCalendar(){     return svg('<rect x="2" y="3" width="12" height="11" rx="1" stroke="currentColor" stroke-width="1.2"/><path d="M2 6h12M5 2v2M11 2v2" stroke="currentColor" stroke-width="1.2"/>'); }
  function iconArrows(){       return svg('<path d="M3 5h8l-2-2M13 11H5l2 2" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/>'); }
  // Three nodes joined by two edges — a schema graph. Deliberately unlike
  // iconArrows (the mapping arrows it sits beside) so the two children of the
  // Object Mapping group are told apart at a glance.
  function iconGraph(){        return svg('<circle cx="3.5" cy="4" r="1.8" stroke="currentColor" stroke-width="1.2"/><circle cx="12.5" cy="4" r="1.8" stroke="currentColor" stroke-width="1.2"/><circle cx="8" cy="12" r="1.8" stroke="currentColor" stroke-width="1.2"/><path d="M4.8 5.4 6.9 10.4M11.2 5.4 9.1 10.4" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/>'); }
  function iconCode(){         return svg('<rect x="2" y="3" width="12" height="10" rx="1" stroke="currentColor" stroke-width="1.2"/><path d="M6 7l-2 1 2 1M10 7l2 1-2 1" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconHand(){         return svg('<path d="M7.5 8h1.5a1.3 1.3 0 0 0 0-2.6H7c-.4 0-.75.13-.93.4L2 9" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/><path d="M4.5 12l1-.9c.2-.27.55-.4.93-.4h2.65c.73 0 1.4-.27 1.86-.8L14 6.95a1.3 1.3 0 0 0-1.8-1.9l-2.75 2.55" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/><path d="M1.5 8.5l4 4" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/>'); }
  function iconClean(){        return svg('<path d="M4 2v4M4 10v4M2 6h4M2 12h4M10 3l3 3-6 6-3-3z" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>'); }
  // Clipboard with a tick on its page — used for Data Quality Review.
  // Distinct from iconCheck (plain circle + tick, used for Validation) and
  // iconReport (single-page document, used for Conversion Report).
  function iconQuality(){      return svg('<rect x="3" y="3" width="10" height="11" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="6" y="1.5" width="4" height="2.5" rx="0.4" stroke="currentColor" stroke-width="1.2" fill="none"/><path d="M5.5 8.5l1.6 1.6 3.4-3.6" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconCheck(){        return svg('<circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="1.2"/><path d="M5.5 8l2 2 3-4" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/>'); }
  /* a record card with a small "+" sparkle — per the enrichment spec */
  function iconEnrich(){       return svg('<rect x="2" y="3.5" width="9" height="9" rx="1.2" stroke="currentColor" stroke-width="1.2"/><path d="M4 6.5h5M4 9h3.5" stroke="currentColor" stroke-width="1.1" stroke-linecap="round"/><path d="M12.5 2v4M10.5 4h4" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>'); }
  function iconPlay(){         return svg('<rect x="2" y="3" width="12" height="10" rx="1" stroke="currentColor" stroke-width="1.2"/><path d="M7 6l3 2-3 2z" fill="currentColor"/>'); }
  function iconList(){         return svg('<path d="M2 4h12M2 8h12M2 12h12" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>'); }
  function iconGrid(){         return svg('<rect x="2" y="2" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="9" y="2" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="2" y="9" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/><rect x="9" y="9" width="5" height="5" rx="1" stroke="currentColor" stroke-width="1.2"/>'); }
  function iconClock(){        return svg('<circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="1.2"/><path d="M8 4v4l2.5 1.5" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconReport(){       return svg('<path d="M4 2h6l3 3v9H4z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/><path d="M10 2v3h3M6 8h4M6 11h4" stroke="currentColor" stroke-width="1.1" stroke-linecap="round"/>'); }
  // Document with embedded bar chart — Report Builder produces ad-hoc tabular
  // and visual reports, distinct from iconReport (the fixed-layout Conversion
  // Report).
  function iconReportBuilder(){return svg('<path d="M4 2h6l3 3v9H4z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/><path d="M10 2v3h3" stroke="currentColor" stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round"/><rect x="6" y="10" width="1.2" height="2" fill="currentColor"/><rect x="8" y="8.5" width="1.2" height="3.5" fill="currentColor"/><rect x="10" y="9.5" width="1.2" height="2.5" fill="currentColor"/>'); }
  function iconAlert(){        return svg('<path d="M8 2l6 11H2z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/><path d="M8 6v3M8 11v0.5" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>'); }
  // Gantt bars in a frame — the Project Plan task-planning grid.
  function iconPlanGrid(){     return svg('<rect x="2" y="2" width="12" height="12" rx="1" stroke="currentColor" stroke-width="1.2"/><path d="M2 5.5h12" stroke="currentColor" stroke-width="1"/><rect x="4" y="7" width="5" height="1.6" fill="currentColor"/><rect x="6.5" y="10" width="5.5" height="1.6" fill="currentColor"/>'); }
  // Sigma over a baseline — the Effort Estimator's function-point roll-up.
  function iconEstimator(){    return svg('<path d="M4 3h8M4 3l4 5-4 5M4 13h8" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/>'); }
  function iconCookie(){       return svg('<circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="1.2"/><circle cx="5.5" cy="6" r="0.8" fill="currentColor"/><circle cx="9.5" cy="5.5" r="0.6" fill="currentColor"/><circle cx="10.5" cy="9" r="0.7" fill="currentColor"/><circle cx="6" cy="10" r="0.5" fill="currentColor"/><circle cx="8.5" cy="11.5" r="0.6" fill="currentColor"/>'); }
  function iconSearch(){       return svg('<circle cx="7" cy="7" r="5" stroke="currentColor" stroke-width="1.3"/><path d="M10.5 10.5L14 14" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>'); }
  // Two figures — Users & Roles, the access-control directory.
  function iconUsers(){        return svg('<circle cx="6" cy="5.5" r="2.2" stroke="currentColor" stroke-width="1.2"/><path d="M2.5 13c0-2 1.6-3.4 3.5-3.4S9.5 11 9.5 13" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/><circle cx="11" cy="6.5" r="1.7" stroke="currentColor" stroke-width="1.1"/><path d="M10.6 9.8c1.7 0.2 2.9 1.5 2.9 3.2" stroke="currentColor" stroke-width="1.1" stroke-linecap="round"/>'); }
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

  // ── Role gating for the Audit Log ───────────────────────────────────────
  //
  // The Audit Log shows every act in the organisation, so the roles that
  // reach it organisation-wide are the ones that answer for it: Organisation
  // Owner, Platform Administrator, and the Auditor — whose entire role is
  // reading this screen. The delivery roles (Lead, Engineer, Approver, Data
  // Owner, Validator) hold the matrix's self-only audit.read grant and still
  // reach their own entries by direct link; the item is simply not
  // advertised to them, because a menu entry that lands on somebody else's
  // idea of the page is worse than no menu entry.
  //
  // Hiding is a courtesy and never a control. netlify/functions/audit.js
  // refuses on the server whatever the nav shows, and a refused view is
  // itself written to the trail as audit.view.denied. The point of hiding it
  // is tidiness, not security.
  //
  // The roles arrive asynchronously, and the sidebar renders synchronously.
  // So the default before they are known is HIDDEN rather than shown: an
  // administrator sees the item appear a moment after first paint (and
  // immediately on every later page, from the five-minute session cache),
  // whereas the opposite default would flash a restricted screen at every
  // engineer on every cold load. The one that flickers should be the one
  // that is allowed in.
  const AUDIT_ROLES = ['OW', 'PA', 'AU'];
  const RBAC_CACHE_KEY = 'cygenix_rbac_me';      // written by cygenix-rbac.js
  const RBAC_CACHE_MS = 5 * 60 * 1000;
  let _auditVisible = null;                       // null = not yet known
  let _auditFetching = false;

  function rolesFromCache(){
    try {
      const raw = sessionStorage.getItem(RBAC_CACHE_KEY);
      if (!raw) return null;
      const rec = JSON.parse(raw);
      if (!rec || Date.now() - rec.at > RBAC_CACHE_MS) return null;
      return (rec.me && Array.isArray(rec.me.roles)) ? rec.me.roles : [];
    } catch { return null; }
  }

  function resolveAuditVisibility(){
    if (_auditVisible !== null) return _auditVisible;
    const cached = rolesFromCache();
    if (cached) {
      _auditVisible = cached.some(r => AUDIT_ROLES.indexOf(r) !== -1);
      return _auditVisible;
    }
    if (!_auditFetching) {
      _auditFetching = true;
      fetchAuditVisibility();
    }
    return false;
  }

  function fetchAuditVisibility(){
    let token = '';
    try { token = (typeof getCygenixIdToken === 'function') ? getCygenixIdToken() : ''; } catch {}
    // Signed out, or the token helper is not on this page: leave it hidden
    // and leave _auditVisible unresolved, so a later page with a token still
    // asks. auth-gate.js has already decided whether the page may render.
    if (!token || typeof fetch !== 'function') { _auditFetching = false; return; }
    fetch('/.netlify/functions/rbac-admin?what=me', { headers: { Authorization: 'Bearer ' + token } })
      .then(r => (r.ok ? r.json() : null))
      .then(me => {
        if (!me) return;
        try { sessionStorage.setItem(RBAC_CACHE_KEY, JSON.stringify({ at: Date.now(), me })); } catch {}
        const next = Array.isArray(me.roles) && me.roles.some(r => AUDIT_ROLES.indexOf(r) !== -1);
        if (next === _auditVisible) return;
        _auditVisible = next;
        // Only rebuild when the answer actually changes the rail, which for
        // the overwhelming majority of loads it does not.
        if (next && window.CygenixSidebar && window.CygenixSidebar.refresh) {
          window.CygenixSidebar.refresh();
        }
      })
      .catch(() => {})
      .then(() => { _auditFetching = false; });
  }

  function isItemVisible(item){
    if (item.requiresAiEnabled && !isAiEnabled()) return false;
    if (item.requiresAuditRead && !resolveAuditVisibility()) return false;
    return true;
  }

  // ── Style injection (once) ──────────────────────────────────────────────
  function injectStyles(){
    if (document.getElementById('cyg-sidebar-styles')) return;
    const style = document.createElement('style');
    style.id = 'cyg-sidebar-styles';
    style.textContent = `
      /* ── Console chrome: masthead + rail (design review, Sep-2026) ──────
         The rail used to be an ink-dark column carrying the brand, a
         project switcher, a Drive button, a profile chip, seven sections of
         coloured items and an account footer. The brand, the project, the
         search, the Files button and the account now live in a 60px
         masthead across the top; the rail beneath it is monochrome text on
         the page ground with a 2px accent bar on the active item — hue is
         reserved for state, so the only colour on the rail is the profile
         chip's status square, which IS state. All values come from the
         tokens in cygenix-console.css; the fallbacks here exist so the rail
         still renders on a page that has not loaded that file. */
      .cx-sr{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap}

      .cx-masthead{
        position:fixed;top:var(--cyg-hairline-h,0px);left:0;right:0;height:${MASTHEAD_H}px;
        /* Under the status hairline (z 54/55), which sits at the very top and
           must stay hoverable; over page content. The rail does not overlap
           it, so their order does not matter. */
        z-index:50;
        background:var(--color-accent-900,#1d2d3d);color:var(--color-bg,#f2f2f3);
        display:flex;align-items:center;gap:18px;padding:0 20px 0 16px;
        font-family:var(--font-body,'Barlow',system-ui,sans-serif);
        -webkit-font-smoothing:antialiased;
      }
      .cx-mh-brand{display:flex;align-items:center;gap:10px;text-decoration:none;color:inherit;flex:0 0 auto}
      .cx-logo{width:26px;height:26px;display:block;flex:0 0 auto;border-radius:7px}
      .cx-wordmark{font-family:var(--font-heading,'Barlow Condensed',sans-serif);font-weight:600;font-size:21px;
        letter-spacing:.18em;text-transform:uppercase;color:var(--color-bg,#f2f2f3);line-height:1}
      .cx-mh-div{width:1px;height:24px;background:rgba(255,255,255,.25);flex:0 0 auto}
      .cx-mh-proj{display:flex;align-items:center;gap:8px;background:none;border:0;color:inherit;cursor:pointer;
        font:inherit;padding:6px 8px;min-width:0;text-align:left}
      .cx-mh-proj:hover{background:rgba(255,255,255,.06)}
      .cx-mh-proj-lbl{opacity:.7;font-size:14px;white-space:nowrap}
      .cx-mh-proj-name{font-family:var(--font-heading,'Barlow Condensed',sans-serif);font-weight:600;font-size:17px;
        white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:280px}
      .cx-mh-proj-chev{opacity:.6;font-size:10px}
      .cx-mh-spacer{flex:1 1 auto;min-width:8px}
      .cx-mh-form{display:flex;margin:0}
      .cx-mh-search{width:250px;height:32px;border:1px solid rgba(255,255,255,.3);background:transparent;
        color:var(--color-bg,#f2f2f3);font:inherit;font-size:14px;padding:0 10px;border-radius:0}
      .cx-mh-search::placeholder{color:rgba(255,255,255,.75)}
      .cx-mh-search::-webkit-search-cancel-button{-webkit-appearance:none}
      .cx-mh-search:focus{outline:2px solid var(--color-accent,#5980a6);outline-offset:2px;border-color:rgba(255,255,255,.6)}
      .cx-mh-btn{height:32px;padding:0 12px;border:1px solid rgba(255,255,255,.3);background:transparent;
        color:var(--color-bg,#f2f2f3);font-family:var(--font-heading,'Barlow Condensed',sans-serif);font-weight:600;
        font-size:14px;cursor:pointer;display:inline-flex;align-items:center;gap:6px;text-decoration:none;border-radius:0}
      .cx-mh-btn:hover{background:rgba(255,255,255,.08)}
      .cx-mh-region{font-size:14px;opacity:.8;white-space:nowrap}
      .cx-mh-av{width:30px;height:30px;border:1px solid rgba(255,255,255,.35);background:transparent;
        color:var(--color-bg,#f2f2f3);font-family:var(--font-heading,'Barlow Condensed',sans-serif);font-weight:600;
        font-size:13px;display:flex;align-items:center;justify-content:center;cursor:pointer;padding:0;border-radius:0;flex:0 0 auto}
      .cx-mh-av:hover{background:rgba(255,255,255,.08)}
      .cx-masthead :focus-visible{outline:2px solid var(--color-accent,#5980a6);outline-offset:2px}

      /* ── The rail ── */
      .cyg-sidebar{
        background:var(--color-bg,#f2f2f3);
        border-right:1px solid var(--color-divider,rgba(29,31,32,.16));
        padding:8px 0 24px;
        width:${WIDTH_OPEN}px;
        display:flex;flex-direction:column;overflow:hidden;
        transition:width .22s cubic-bezier(.4,0,.2,1);
        position:fixed;left:0;bottom:0;
        top:calc(var(--cyg-hairline-h,0px) + ${MASTHEAD_H}px);
        z-index:90;
        font-family:var(--font-body,'Barlow',system-ui,sans-serif);
        -webkit-font-smoothing:antialiased;
      }
      .cyg-sidebar.collapsed{ width:${WIDTH_CLOSED}px; }

      /* One control in the head: the collapse toggle. The brand is in the
         masthead now, so the head has nothing else to hold. */
      .cyg-sidebar-head{display:flex;align-items:center;justify-content:flex-end;padding:0 8px 4px;flex-shrink:0}
      .cyg-sidebar.collapsed .cyg-sidebar-head{justify-content:center;padding:0 0 4px}
      .cyg-sidebar-toggle{
        background:transparent;border:1px solid var(--color-divider,rgba(29,31,32,.16));cursor:pointer;
        color:var(--color-neutral-700,#5d5d60);width:26px;height:26px;border-radius:0;
        display:flex;align-items:center;justify-content:center;font-size:11px;line-height:1;
      }
      .cyg-sidebar-toggle:hover{color:var(--color-text,#1d1f20);background:color-mix(in srgb,var(--color-text,#1d1f20) 4%,transparent)}

      .cyg-sidebar-scroll{flex:1 1 auto;overflow-y:auto;overflow-x:hidden;padding:0 0 12px;
        scrollbar-width:thin;scrollbar-color:var(--color-neutral-300,#d4d4d7) transparent}
      .cyg-sidebar-scroll::-webkit-scrollbar{width:6px}
      .cyg-sidebar-scroll::-webkit-scrollbar-thumb{background:var(--color-neutral-300,#d4d4d7)}

      .cyg-nav-section{margin:0}
      .cyg-nav-label{
        font-family:var(--font-heading,'Barlow Condensed',sans-serif);font-weight:600;font-size:13px;line-height:1.2;
        letter-spacing:.16em;text-transform:uppercase;color:var(--color-neutral-700,#5d5d60);
        padding:16px 10px 5px;
      }
      .cyg-nav-item{
        display:flex;align-items:center;gap:10px;
        padding:7px 10px;margin:0;border-radius:0;
        border-left:2px solid transparent;
        font-size:14px;line-height:1.2;font-weight:400;color:var(--color-neutral-700,#5d5d60);
        cursor:pointer;position:relative;user-select:none;white-space:nowrap;
        transition:background .12s,color .12s;
      }
      .cyg-nav-item:hover{color:var(--color-text,#1d1f20);background:color-mix(in srgb,var(--color-text,#1d1f20) 4%,transparent)}
      .cyg-nav-item.active{color:var(--color-accent-900,#1d2d3d);background:var(--color-accent-100,#eef6ff);border-left-color:var(--color-accent,#5980a6)}
      .cyg-nav-item:focus-visible{outline:2px solid var(--color-accent,#5980a6);outline-offset:-2px}
      /* Icons are for the 54px rail only; the open rail is text. */
      .cyg-nav-icon{width:18px;height:18px;flex-shrink:0;color:currentColor;display:none}
      .cyg-sidebar.collapsed .cyg-nav-icon{display:block}
      .cyg-sidebar.collapsed .cyg-nav-label,
      .cyg-sidebar.collapsed .cyg-nav-item-label{display:none}
      .cyg-sidebar.collapsed .cyg-nav-item{justify-content:center;padding:9px 0;gap:0;border-left-width:2px}

      /* Count badge on an item — a failure count, which is state. */
      .cyg-nav-badge{
        margin-left:auto;min-width:19px;height:19px;padding:0 5px;border-radius:0;
        background:var(--state-fail,#9c3f38);color:#fff;font-size:12px;font-weight:600;line-height:19px;
        text-align:center;display:none;flex-shrink:0;font-variant-numeric:tabular-nums;
      }
      .cyg-nav-badge.show{display:inline-block}
      .cyg-sidebar.collapsed .cyg-nav-badge{position:absolute;top:5px;right:12px;margin:0;padding:0;
        min-width:8px;width:8px;height:8px;font-size:0;line-height:0}

      body.cyg-collapsed{--cyg-sidebar-w:${WIDTH_CLOSED}px}
      body:not(.cyg-collapsed){--cyg-sidebar-w:${WIDTH_OPEN}px}
      /* Applied only when mount() had to create its own mount point: the page
         has no padding rule of its own, so keep its content clear of the rail. */
      body.cyg-sidebar-autopad{padding-left:var(--cyg-sidebar-w);transition:padding-left .2s ease}

      /* ── Account and project menus: popovers below the masthead ── */
      .cyg-user-menu{
        position:fixed;z-index:1000;min-width:232px;
        background:var(--color-bg,#f2f2f3);border:1px solid var(--color-divider,rgba(29,31,32,.16));border-radius:0;
        box-shadow:var(--shadow-strong,0 12px 32px rgba(43,43,45,.22));
        padding:4px 0;display:none;
        font-family:var(--font-body,'Barlow',system-ui,sans-serif);
      }
      .cyg-user-menu.open{display:block;animation:cygMenuIn .12s ease}
      @keyframes cygMenuIn{from{opacity:0;transform:translateY(-4px)}to{opacity:1;transform:translateY(0)}}
      .cyg-user-menu-email{padding:9px 14px 7px;font-size:13px;color:var(--color-neutral-700,#5d5d60);
        white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
      .cyg-user-menu-label{padding:10px 14px 3px;font-family:var(--font-heading,'Barlow Condensed',sans-serif);
        font-weight:600;font-size:13px;letter-spacing:.16em;text-transform:uppercase;color:var(--color-neutral-700,#5d5d60)}
      .cyg-user-menu-sep{height:1px;background:var(--color-divider,rgba(29,31,32,.16));margin:4px 0}
      .cyg-user-menu-item{display:flex;align-items:center;justify-content:space-between;gap:10px;width:100%;
        padding:8px 14px;border:none;background:none;cursor:pointer;font:inherit;font-size:14px;
        color:var(--color-text,#1d1f20);border-radius:0;text-align:left;text-decoration:none}
      .cyg-user-menu-item:hover{background:color-mix(in srgb,var(--color-text,#1d1f20) 4%,transparent);color:var(--color-text,#1d1f20)}
      .cyg-user-menu-item.danger{color:var(--state-fail,#9c3f38)}

      /* ── Profile chip: which databases this session is pointed at ──
         Pinned above the nav where nothing can fold it away. Its square is
         the one coloured thing on the rail, because it is the one thing on
         the rail that is state. Colours come from the hairline's inline
         variables so the dot and the line can never disagree. */
      .cyg-prof-area{flex-shrink:0;padding:4px 10px 6px}
      .cyg-prof-chip{
        display:flex;align-items:center;gap:8px;width:100%;box-sizing:border-box;
        padding:6px 8px;border-radius:0;text-decoration:none;
        border:1px solid var(--color-divider,rgba(29,31,32,.16));background:transparent;
        font-family:var(--mono,'IBM Plex Mono',ui-monospace,monospace);
        font-size:12px;letter-spacing:.02em;color:var(--color-text,#1d1f20);
        transition:background .15s,border-color .15s;
      }
      .cyg-prof-chip:hover{background:color-mix(in srgb,var(--color-text,#1d1f20) 4%,transparent)}
      .cyg-prof-dot{flex:0 0 auto;width:8px;height:8px;border-radius:0;background:var(--cyg-status-green,#3f6b52)}
      .cyg-prof-id{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:500}
      .cyg-prof-env{
        flex:0 0 auto;padding:1px 6px;border-radius:0;
        font-family:var(--font-heading,'Barlow Condensed',sans-serif);font-weight:600;font-size:12px;letter-spacing:.1em;
        border:1px solid var(--color-divider,rgba(29,31,32,.16));color:var(--color-neutral-700,#5d5d60);
      }
      .cyg-prof-chip.lv-green .cyg-prof-dot{background:var(--cyg-status-green,#3f6b52)}
      .cyg-prof-chip.lv-amber .cyg-prof-dot{background:var(--cyg-status-amber,#9a6b1f)}
      .cyg-prof-chip.lv-red   .cyg-prof-dot{background:var(--cyg-status-red,#9c3f38)}
      .cyg-prof-chip.lv-amber .cyg-prof-env{color:var(--cyg-status-amber,#9a6b1f);border-color:var(--cyg-status-amber,#9a6b1f)}
      .cyg-prof-chip.lv-red   .cyg-prof-env{color:var(--cyg-status-red,#9c3f38);border-color:var(--cyg-status-red,#9c3f38)}
      .cyg-prof-chip.lv-red{border-color:var(--cyg-status-red,#9c3f38)}
      .cyg-sidebar.collapsed .cyg-prof-area{padding:4px 6px 6px}
      .cyg-sidebar.collapsed .cyg-prof-chip{justify-content:center;padding:8px 0;gap:0;border-color:transparent}
      .cyg-sidebar.collapsed .cyg-prof-id,
      .cyg-sidebar.collapsed .cyg-prof-env{display:none}
      .cyg-sidebar.collapsed .cyg-prof-dot{width:9px;height:9px}

      /* ── Sidebar favourites ("Pinned") ── */
      .cyg-sidebar-scroll .cyg-nav-item > .cyg-nav-item-label{flex:1 1 auto;min-width:0;overflow:hidden;text-overflow:ellipsis}
      .cyg-fav-star{
        flex:0 0 auto;margin-left:auto;width:20px;height:20px;padding:0;
        border:0;border-radius:0;background:transparent;color:var(--color-neutral-500,#98989b);
        font-size:13px;line-height:18px;text-align:center;cursor:pointer;
        opacity:0;transition:opacity .12s,color .12s,background .12s;
      }
      .cyg-nav-item:hover > .cyg-fav-star,
      .cyg-nav-item:focus-visible > .cyg-fav-star,
      .cyg-fav-star:focus-visible,
      .cyg-fav-star.on{opacity:1}
      .cyg-fav-star:hover{color:var(--color-accent-700,#416180)}
      .cyg-fav-star.on{color:var(--color-accent-700,#416180)}
      @media (hover: none){ .cyg-sidebar-scroll .cyg-fav-star{opacity:.55} }
      .cyg-fav-item{cursor:pointer}
      .cyg-fav-item.cyg-fav-dragging{opacity:.4}
      .cyg-fav-item.cyg-fav-drop-before{box-shadow:inset 0  2px 0 0 var(--color-accent,#5980a6)}
      .cyg-fav-item.cyg-fav-drop-after {box-shadow:inset 0 -2px 0 0 var(--color-accent,#5980a6)}
      .cyg-sidebar.collapsed .cyg-fav-star,
      .cyg-sidebar.collapsed .cyg-fav-section{margin-bottom:6px;padding-bottom:8px;border-bottom:1px solid var(--color-divider,rgba(29,31,32,.16))}
      @media (prefers-reduced-motion: reduce){ .cyg-fav-star{transition:none} }

      /* Below ~900px the rail is a drawer (cygenix-mobile.css) opened from a
         button the masthead leaves room for; the search and the region go. */
      @media (max-width: 820px){
        .cx-masthead{padding-left:56px;gap:10px}
        .cx-mh-form,.cx-mh-region,.cx-mh-div,.cx-mh-proj-lbl{display:none}
        .cx-mh-proj-name{max-width:150px}
      }
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
  // lets the Drive button open the Drive on top of ANY page instead of
  // navigating away.
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

  // ── The masthead ────────────────────────────────────────────────────────
  // 60px, accent-900 ground. Left to right: the logo (favicon.svg, unmodified,
  // keeping its own indigo — it is the brand mark and does not take the
  // theme), the wordmark, a divider, the project switcher, then pushed right
  // the search field, the Files button, the region and the account avatar.
  // Everything that used to be pinned at the top and the bottom of the rail
  // is here, which is what lets the rail be nothing but the five groups.
  function buildMasthead(){
    const name = activeProjectName();
    return `<header class="cx-masthead" id="cx-masthead" role="banner">
      <a class="cx-mh-brand" href="/dashboard" aria-label="Cygenix — Home">
        <svg class="cx-logo" viewBox="0 0 32 32" aria-hidden="true">
          <defs><linearGradient id="cxLogoBg" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#6d5df2"/><stop offset="1" stop-color="#4a7cf3"/></linearGradient></defs>
          <rect width="32" height="32" rx="7" fill="url(#cxLogoBg)"/>
          <g fill="none" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round">
            <path d="M9 10.5 14 16 9 21.5" stroke="#ffffff" stroke-opacity="0.45"/>
            <path d="M13 10.5 18 16 13 21.5" stroke="#ffffff" stroke-opacity="0.72"/>
            <path d="M17 10.5 22 16 17 21.5" stroke="#ffffff"/>
          </g>
        </svg>
        <span class="cx-wordmark">Cygenix</span>
      </a>
      <span class="cx-mh-div" aria-hidden="true"></span>
      <button type="button" class="cx-mh-proj" id="cyg-proj-btn" aria-haspopup="menu" aria-expanded="false"
              title="Active project — everything below acts on this project">
        <span class="cx-mh-proj-lbl">Project</span>
        <span class="cx-mh-proj-name" id="cyg-proj-name">${escapeHtml(name || 'No project selected')}</span>
        <span class="cx-mh-proj-chev" aria-hidden="true">▾</span>
      </button>
      <span class="cx-mh-spacer"></span>
      <form class="cx-mh-form" id="cx-mh-search-form" role="search">
        <input class="cx-mh-search" id="cx-mh-search" type="search" placeholder="Search objects, jobs, runs"
               aria-label="Search objects, jobs and runs" autocomplete="off">
      </form>
      <a class="cx-mh-btn cyg-drive-btn" id="cyg-drive-btn" href="/dashboard#drive"
         title="Your files — the shared Drive, available on every machine you sign in on">${iconDrive()}Files</a>
      <span class="cx-mh-region" id="cx-mh-region" title="Where this console and its data are hosted">${REGION_LABEL}</span>
      <button type="button" class="cx-mh-av" id="cyg-user-chip" aria-haspopup="menu" aria-expanded="false" title="Account">
        <span id="cyg-user-av">CY</span>
        <span class="cx-sr" id="cyg-user-name">Account</span>
        <span class="cx-sr" id="cyg-user-sub"></span>
      </button>
    </header>`;
  }

  /* The masthead search. It is a way INTO the dashboard's Search view rather
     than a second search: the query is stashed and the view opens with it
     already run, so the one implementation of "search everything" stays the
     one implementation. On the dashboard itself the view switches in place. */
  function wireSearch(root){
    const form = root.querySelector('#cx-mh-search-form');
    const input = root.querySelector('#cx-mh-search');
    if (!form || !input) return;
    form.addEventListener('submit', (e) => {
      e.preventDefault();
      const q = input.value.trim();
      if (!q) return;
      try { sessionStorage.setItem('cyg_search_q', q); } catch {}
      const item = findItem('search');
      if (item) handleClick(item);
    });
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
        '<a class="cyg-user-menu-item" href="/projects">All projects…</a>';
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
  // The rail: a collapse toggle, the profile chip, the five groups. Nothing
  // else — see buildMasthead for where the rest went.
  function buildHTML(activeKey){
    const head = `<div class="cyg-sidebar-head">
      <button id="cyg-sidebar-toggle" class="cyg-sidebar-toggle" aria-label="Collapse sidebar" title="Collapse the rail">❮</button>
    </div>`;
    const body = NAV.map(sec => buildSection(sec, activeKey)).join('');
    return head + buildProfilePill() + `<div class="cyg-sidebar-scroll">${body}</div>`;
  }

  /* ── Which databases is this session pointed at ─────────────────────────
     The status hairline at the top of the window is 2px most of the time,
     and a 2px green line and a line that failed to render look identical.
     So the fact itself lives here as well, in words, permanently: profile
     id, environment class, and a dot carrying the same level the hairline
     is showing.

     PINNED ABOVE THE SCROLL AREA, deliberately. The project switcher sits
     inside the Project nav group, which a user can collapse — fine for a
     project name, not for the thing that says which database a run will
     touch. This sits between the brand and the Drive button, where nothing
     can fold it away, and it survives the collapsed rail as the dot alone.

     It renders from CygenixStatusHairline's one state function rather than
     reading the profile store a second time: two implementations of "what
     environment am I in" is how one of them ends up wrong. */
  function buildProfilePill(){
    return `<div class="cyg-prof-area" id="cyg-prof-area" hidden>
      <a class="cyg-prof-chip" id="cyg-prof-chip" href="/profiles"
         title="The connection profile governing this session. Click to manage profiles.">
        <span class="cyg-prof-dot" aria-hidden="true"></span>
        <span class="cyg-prof-id" id="cyg-prof-id"></span>
        <span class="cyg-prof-env" id="cyg-prof-env"></span>
      </a>
    </div>`;
  }

  function paintProfilePill(root, s){
    const area = (root || document).querySelector('#cyg-prof-area');
    if (!area) return;
    // 'off' is the pre-adoption state: no profiles defined anywhere, and the
    // console behaves exactly as it did before profiles existed — including
    // showing nothing here.
    if (!s || s.level === 'off'){ area.hidden = true; return; }
    area.hidden = false;
    const chip = area.querySelector('#cyg-prof-chip');
    const id   = area.querySelector('#cyg-prof-id');
    const env  = area.querySelector('#cyg-prof-env');
    // The label is "ID · ENV · name"; the chip wants the first two apart so
    // the environment can carry the colour on its own.
    const bits = String(s.label || '').split(' · ');
    const envText = (bits[1] || '').trim();
    chip.className = 'cyg-prof-chip lv-' + s.level;
    id.textContent = bits[0] || '';
    env.textContent = envText;
    env.hidden = !envText;
    chip.setAttribute('aria-label', 'Connection profile: ' + (s.text || s.label) + '. Open the Profiles page.');
    chip.title = (s.text || s.label) + ' — click to manage profiles.';
  }

  function wireProfilePill(root){
    const H = window.CygenixStatusHairline;
    // Script order is not guaranteed across 27 pages, so take whatever has
    // been resolved already AND subscribe for the next one.
    if (H && typeof H.current === 'function') paintProfilePill(root, H.current());
    window.addEventListener('cygenix:profile-status', (e) => paintProfilePill(null, e.detail));
  }

  // Read the signed-in user (stored by auth flow as cygenix_user) and fill the
  // masthead avatar. The name and email go into visually-hidden spans so the
  // account menu (and a screen reader) can read them off the same element.
  function populateUser(root){
    const nameEl = root.querySelector('#cyg-user-name');
    const subEl  = root.querySelector('#cyg-user-sub');
    const avEl   = root.querySelector('#cyg-user-av');
    if (!avEl && !nameEl) return;
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
    if (nameEl) nameEl.textContent = name;
    if (subEl) subEl.textContent = email || plan;
    if (avEl) avEl.textContent = initials;
    const chip = root.querySelector('#cyg-user-chip');
    if (chip) chip.title = 'Account — ' + (email || name);
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
    const acct = (keys) => ACCOUNT_NAV.filter(it => keys.indexOf(it.key) !== -1).filter(isItemVisible).map(it =>
      `<button class="cyg-user-menu-item${it.navClass ? ' ' + it.navClass : ''}" role="menuitem" type="button"` +
      `${it.action === 'accessibility' ? ' aria-haspopup="dialog" aria-expanded="false"' : ''}` +
      ` data-acct-key="${it.key}">${escapeHtml(it.label)}</button>`
    ).join('');
    menu.innerHTML =
      '<div class="cyg-user-menu-email" id="cyg-user-menu-email"></div>' +
      '<div class="cyg-user-menu-sep"></div>' +
      '<a class="cyg-user-menu-item" role="menuitem" href="/projects">My projects</a>' +
      '<button class="cyg-user-menu-item" role="menuitem" type="button" id="cyg-user-menu-sub">Subscription</button>' +
      // Sync exists on the dashboard only (it is that page's own cloud pull);
      // elsewhere the item would be a dead button, so it is not rendered.
      (typeof window.syncFromCloud === 'function'
        ? '<button class="cyg-user-menu-item" role="menuitem" type="button" id="cyg-user-menu-sync">Sync from cloud</button>' : '') +
      '<a class="cyg-user-menu-item" role="menuitem" href="/admin" id="cyg-user-menu-admin" hidden>Admin panel</a>' +
      '<div class="cyg-user-menu-sep"></div>' +
      // Settings: the four former rail children, as the review moved them.
      '<div class="cyg-user-menu-label">Settings</div>' +
      acct(['project-settings', 'notifications', 'system-parameters', 'user-roles', 'privacy-security']) +
      '<div class="cyg-user-menu-sep"></div>' +
      acct(['help', 'accessibility']) +
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
    const sync = menu.querySelector('#cyg-user-menu-sync');
    if (sync) sync.addEventListener('click', (e) => { e.preventDefault(); closeUserMenu(); try { window.syncFromCloud(); } catch(_){} });
    menu.querySelector('#cyg-user-menu-sub').addEventListener('click', (e) => {
      e.preventDefault(); closeUserMenu();
      if (typeof window.openBillingPortal === 'function') { try { window.openBillingPortal(e.currentTarget); return; } catch(_){} }
      window.location.href = '/pick-plan';
    });
    menu.querySelector('#cyg-user-menu-signout').addEventListener('click', (e) => {
      e.preventDefault(); closeUserMenu(); sidebarSignOut();
    });
    return menu;
  }

  // Below the avatar, right-aligned to it — the menu opens from the masthead
  // now, not up from a footer chip.
  function positionUserMenu(menu, chip){
    const r = chip.getBoundingClientRect();
    menu.style.top    = (r.bottom + 6) + 'px';
    menu.style.right  = Math.max(8, window.innerWidth - r.right) + 'px';
    menu.style.left   = 'auto';
    menu.style.bottom = 'auto';
    menu.style.maxWidth = Math.max(180, r.right - 16) + 'px';
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

  // Open the Drive as an OVERLAY on top of the current page. (The Co-Worker
  // page's own native Drive went with that page; the overlay module is the
  // one Drive everywhere now.) The href stays as a no-JS fallback.
  function wireDriveButton(root){
    const btn = root.querySelector('#cyg-drive-btn');
    if (!btn) return;
    btn.addEventListener('click', (e) => {
      if (window.CygenixDriveModal){
        e.preventDefault(); window.CygenixDriveModal.open(); return;
      }
      // Module not loaded yet — load it, then open. Falls back to navigating
      // to dashboard.html#drive only if the module fails to load.
      e.preventDefault();
      ensureDriveModal(() => {
        if (window.CygenixDriveModal) window.CygenixDriveModal.open();
        else window.location.href = '/dashboard#drive';
      });
    });
    // #drive in the URL opens the overlay on arrival — this is where the old
    // coworker.html#drive bookmarks (and the no-JS fallback above) land.
    if (/#drive\b/.test(location.hash || '')) {
      ensureDriveModal(() => { if (window.CygenixDriveModal) window.CygenixDriveModal.open(); });
    }
  }

  function buildSection(sec, activeKey){
    const visibleItems = (sec.items || []).filter(isItemVisible);
    if (!visibleItems.length) return '';
    const labelHtml = sec.section
      ? `<div class="cyg-nav-label">${escapeHtml(sec.section)}</div>`
      : '';
    // The rail highlights the item that OWNS the current key: a page mounted
    // with data-active="validation" lights Assurance (see railKeyFor).
    const rail = railKeyFor(activeKey);
    const itemsHtml = visibleItems.map(it => buildItem(it, rail)).join('');
    return `<div class="cyg-nav-section" data-group="${escapeHtml(sec.group || '')}">${labelHtml}${itemsHtml}</div>`;
  }

  function buildItem(item, activeKey){
    const isActive = item.key === activeKey ? ' active' : '';
    const badgeHtml = item.badgeId
      ? `<span class="cyg-nav-badge" id="${item.badgeId}" aria-live="polite"></span>`
      : '';
    return `
      <div class="cyg-nav-item${isActive}"
           data-key="${item.key}"
           tabindex="0" role="link" title="${escapeHtml(item.label)}">
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
    for (const railKey in TABS){
      for (const t of TABS[railKey]){ if (t.key === key) return t; }
    }
    for (const it of ACCOUNT_NAV){ if (it.key === key) return it; }
    // The masthead search is not an item anyone renders, but it is a
    // destination the tour and the search form navigate to.
    if (key === 'search') return { key:'search', label:'Search', view:'search' };
    return null;
  }

  function handleClick(item){
    // If the item is a dashboard view AND we're on dashboard, call showView directly.
    // Otherwise, stash cyg_goto and navigate to dashboard (if view) or the page (if href).
    // Addresses are extensionless — /dashboard, not /dashboard.html — and this
    // test was written before they were. It matched only ".htm"/".html" and the
    // bare root, so on the real URL it was ALWAYS false: every one of the
    // fifteen view: items fell through to the navigate branch below and did
    // nothing, while the twenty href: items carried on working. That split is
    // what the bug looked like from the outside.
    //
    // Normalised the same way auth-gate.js does it, so the two agree about
    // what page this is. The root is the landing page now, not the dashboard,
    // so it is deliberately not matched here.
    const here = location.pathname.replace(/\.html$/, '').replace(/\/+$/, '') || '/';
    const onDashboard = here === '/dashboard';
    if (item.action === 'cookie-preferences'){
      if (typeof window.openCookiePreferences === 'function') window.openCookiePreferences();
      return;
    }
    if (item.action === 'open-help'){
      window.open('/help', '_blank');
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
        // as long as the redirect preserves it. dashboard-app.js reads either.
        try { sessionStorage.setItem('cyg_goto', item.view); } catch {}
        const url = '/dashboard#goto=' + encodeURIComponent(item.view);
        if (here === '/dashboard') {
          // Already here, so assigning a URL that differs only in the hash
          // updates the address bar and nothing else — and clicking the same
          // item twice would not even fire a hashchange. Reaching this branch
          // on the dashboard means showView is missing, i.e. the page did not
          // finish loading; a real reload is the honest recovery.
          window.location.href = url;
          window.location.reload();
        } else {
          window.location.href = url;
        }
      }
      return;
    }
    if (item.href){
      window.location.href = item.href;
      return;
    }
  }

  // Which rail item owns a key. A rail item owns itself, every tab under it,
  // and every alias that points at it. Anything else — the account-menu
  // items, an unknown key — owns nothing, and the rail shows no highlight
  // rather than a wrong one.
  function railKeyFor(key){
    if (!key) return '';
    for (const sec of NAV){ for (const it of sec.items){ if (it.key === key) return key; } }
    for (const railKey in TABS){ if (TABS[railKey].some(t => t.key === key)) return railKey; }
    return ALIASES[key] || '';
  }

  function tabsFor(key){
    const rail = railKeyFor(key);
    const tabs = rail && TABS[rail];
    return tabs ? tabs.filter(isItemVisible) : null;
  }

  let _activeKey = '';

  /* The tab strip. Rendered into #cyg-subnav-mount when the current key
     belongs to a destination that absorbed others; cleared otherwise. A page
     that has not declared a mount gets none — a strip guessed into the wrong
     place is worse than a strip that is missing, and every page that needs
     one declares one (tests/console-design.test.js checks). */
  function renderSubnav(key){
    const host = document.getElementById('cyg-subnav-mount');
    if (!host) return;
    const tabs = tabsFor(key);
    if (!tabs || tabs.length < 2){ host.innerHTML = ''; host.hidden = true; return; }
    host.hidden = false;
    host.innerHTML = '<nav class="cx-subnav" aria-label="Sections of this screen">' + tabs.map(t => {
      const href = t.href || ('/dashboard#goto=' + encodeURIComponent(t.view || ''));
      const cls = (t.key === key ? 'on' : '') + (t.away ? ' away' : '');
      return `<a href="${href}" data-key="${t.key}" class="${cls.trim()}"${t.key === key ? ' aria-current="page"' : ''}>${escapeHtml(t.label)}</a>`;
    }).join('') + '</nav>';
    host.querySelectorAll('a[data-key]').forEach(a => {
      a.addEventListener('click', (e) => {
        const item = findItem(a.dataset.key);
        if (!item) return;
        e.preventDefault();
        handleClick(item);
      });
    });
  }

  function updateActive(key){
    _activeKey = key || '';
    const rail = railKeyFor(key);
    const sidebar = document.querySelector('.cyg-sidebar');
    if (sidebar){
      sidebar.querySelectorAll('.cyg-nav-item[data-key]').forEach(el => {
        el.classList.toggle('active', el.dataset.key === rail);
      });
    }
    renderSubnav(key);
  }

  // ── Mount ───────────────────────────────────────────────────────────────
  // ── Mobile drawer ───────────────────────────────────────────────────────
  // Below 820px the rail becomes an off-canvas drawer: a hamburger opens
  // it, a backdrop and Escape close it, and following a link closes it so
  // the destination page isn't hidden behind the menu. All of the styling
  // lives in cygenix-mobile.css inside a media query, and every element
  // added here is inert at desktop width — the desktop sidebar keeps the
  // exact behaviour it had.
  const MOBILE_Q = '(max-width: 820px)';

  function wireMobileDrawer(aside){
    if (document.getElementById('cyg-mobile-menu-btn')) return;

    const btn = document.createElement('button');
    btn.id = 'cyg-mobile-menu-btn';
    btn.className = 'cyg-mobile-menu-btn';
    btn.type = 'button';
    btn.setAttribute('aria-label', 'Open menu');
    btn.setAttribute('aria-expanded', 'false');
    btn.innerHTML = '<i class="ic ic-menu"></i>';

    const backdrop = document.createElement('div');
    backdrop.className = 'cyg-mobile-backdrop';
    backdrop.setAttribute('aria-hidden', 'true');

    const setOpen = (open) => {
      document.body.classList.toggle('cyg-mobile-open', open);
      btn.setAttribute('aria-expanded', String(open));
      btn.setAttribute('aria-label', open ? 'Close menu' : 'Open menu');
      btn.innerHTML = open ? '✕' : '<i class="ic ic-menu"></i>';
    };

    btn.addEventListener('click', () => setOpen(!document.body.classList.contains('cyg-mobile-open')));
    backdrop.addEventListener('click', () => setOpen(false));
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && document.body.classList.contains('cyg-mobile-open')) setOpen(false);
    });
    // Navigating away: close so the new page isn't behind the drawer.
    aside.addEventListener('click', (e) => {
      if (e.target.closest('a[href]')) setOpen(false);
    });

    document.body.appendChild(backdrop);
    document.body.appendChild(btn);

    // Only claim header space (and only exist as a control) on mobile.
    const mq = window.matchMedia(MOBILE_Q);
    const sync = () => {
      document.body.classList.toggle('cyg-has-mobile-menu', mq.matches);
      if (!mq.matches) setOpen(false);      // leaving mobile clears the drawer state
    };
    sync();
    if (mq.addEventListener) mq.addEventListener('change', sync);
    else if (mq.addListener) mq.addListener(sync);
  }

  // ── Environment banner ────────────────────────────────────────────────
  // MOVED OUT, Sep-2026. `#cyg-envbar` used to be built here: a fixed 22px
  // bar at z-index 2000 naming the active connection profile. Two faults
  // ended it. It cost 22 permanent pixels on all 27 console pages for a fact
  // that matters intensely twice a day; and at z-index 2000 it painted over
  // the Ask Cygenix panel (z-index 290, also fixed at top 0), so the top
  // 10px of that panel's New and ✕ buttons navigated to /profiles instead of
  // pressing the button.
  //
  // It is now a 2px hairline that swells on hover and locks open only for
  // production or a blocked session, and it lives in its own module —
  // public/cygenix-status-hairline.js — which self-mounts, owns its own
  // stylesheet, and listens to the same storage and cygenix:profiles-changed
  // events this file used to. The only thing left here is the sidebar's own
  // offset, and that now follows the --cyg-hairline-h variable the hairline
  // publishes, so the rail no longer hard-codes the bar's height either.
  //
  // tests/status-hairline.test.js asserts every page that loads this file
  // also loads that one.

  function mount(){
    injectStyles();
    let host = document.getElementById(MOUNT_ID);
    // Self-heal: a page that loads this script WANTS the rail, so a missing
    // mount point must not cost it the whole navigation (projects.html shipped
    // that way and simply lost its menu). Create one, and pad the body from
    // here since such a page has no padding rule of its own.
    if (!host){
      console.warn('[cygenix-sidebar] No #' + MOUNT_ID + ' found — mounting at the top of <body>.');
      host = document.createElement('div');
      host.id = MOUNT_ID;
      document.body.insertBefore(host, document.body.firstChild);
      document.body.classList.add('cyg-sidebar-autopad');
    }
    const activeKey = host.dataset.active || '';
    _activeKey = activeKey;

    // The masthead, once, as the first thing in <body>. It is fixed, so
    // where it sits in the DOM only matters for reading order.
    let masthead = document.getElementById('cx-masthead');
    if (!masthead){
      const tmp = document.createElement('div');
      tmp.innerHTML = buildMasthead();
      masthead = tmp.firstElementChild;
      document.body.insertBefore(masthead, document.body.firstChild);
    }

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
    populateUser(masthead);
    wireUserChip(masthead);
    wireDriveButton(masthead);
    wireProjectSwitcher(masthead);
    wireSearch(masthead);
    wireProfilePill(aside);
    hideTopbarUserPill();
    ensureA11y();
    wireMobileDrawer(aside);
    renderSubnav(activeKey);
    // The status hairline mounts itself — see the note above renderEnvBanner's
    // grave. Nudged here only so a page that finished its own DOM after the
    // hairline booted still gets the rail offset applied.
    if (window.CygenixStatusHairline) { try { window.CygenixStatusHairline.render(); } catch (e) {} }
    // Preload the Drive overlay so the first click is instant. Idle-scheduled
    // the same way loadDriveSyncWhenIdle already is: injecting 53KB of modal at
    // DOMContentLoaded competed with every page's first paint, and a click
    // that beats the idle callback still works — the click path calls
    // ensureDriveModal(cb) itself and waits for the script's load event.
    {
      const warm = () => ensureDriveModal();
      if (window.requestIdleCallback) window.requestIdleCallback(warm, { timeout: 6000 });
      else setTimeout(warm, 2500);
    }
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
    // Navigate to a nav key exactly as clicking the item would. Exposed for
    // the guided tour, which must not fake clicks: handleClick is the one
    // place that knows which keys are dashboard views (stash cyg_goto, go to
    // /dashboard#goto=…) and which are their own pages, and a second copy of
    // that decision would drift from this one the first time a page moved.
    navigate: (key) => {
      const item = findItem(key);
      if (item) handleClick(item);
      return !!item;
    },
    // Exposed for structural tests (tests/sidebar-nav.test.js): the nav tree
    // and footer as data, so key coverage can be asserted without a DOM.
    __nav: NAV, __accountNav: ACCOUNT_NAV, __findItem: findItem,
    __tabs: TABS, __aliases: ALIASES, railKeyFor, tabsFor,
    // Re-render the tab strip for the current key — for a page whose mount
    // point appears after the rail booted.
    renderSubnav: () => renderSubnav(_activeKey),
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
      const collapsed = existing.classList.contains('collapsed');
      const replacement = document.createElement('aside');
      replacement.className = 'cyg-sidebar' + (collapsed ? ' collapsed' : '');
      replacement.innerHTML = buildHTML(_activeKey);
      existing.replaceWith(replacement);
      const toggle = replacement.querySelector('#cyg-sidebar-toggle');
      if (toggle) toggle.textContent = collapsed ? '❯' : '❮';
      wireItemClicks(replacement);
      wireProfilePill(replacement);
      renderSubnav(_activeKey);
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

/* ==========================================================================
   Cygenix — Sidebar favourites ("Pinned")
   --------------------------------------------------------------------------
   Append this block to the END of cygenix-sidebar.js, after
   window.CygenixSidebar is assigned. It is purely additive: it does not
   change any existing function, and it reuses the sidebar's own nav config,
   markup and click handling rather than duplicating them.

   Pairs with the rules in cygenix-favourites.css.
   ========================================================================== */
(function () {
  'use strict';

  var SB = window.CygenixSidebar;
  if (!SB) return;

  var MAX_PINS   = 8;
  var STORE_BASE = 'cygenix_sidebar_pinned_v1';

  /* ---------- store (namespaced per signed-in user) ---------------------- */

  function storeKey() {
    var u = '';
    try { u = localStorage.getItem('cygenix_active_user') || ''; } catch (e) {}
    return STORE_BASE + (u ? '::' + u : '');
  }

  function getPins() {
    try {
      var raw = JSON.parse(localStorage.getItem(storeKey()) || '[]');
      return Array.isArray(raw)
        ? raw.filter(function (k) { return typeof k === 'string'; }).slice(0, MAX_PINS)
        : [];
    } catch (e) { return []; }
  }

  function savePins(list) {
    try { localStorage.setItem(storeKey(), JSON.stringify(list.slice(0, MAX_PINS))); } catch (e) {}
    apply();
  }

  function togglePin(key) {
    var pins = getPins(), i = pins.indexOf(key);
    if (i > -1) { pins.splice(i, 1); }
    else { if (pins.length >= MAX_PINS) pins.shift(); pins.push(key); }
    savePins(pins);
  }


  /* ---------- helpers ---------------------------------------------------- */

  function scrollEl() { return document.querySelector('.cyg-sidebar-scroll'); }

  // The one real nav row for a key. Pinned rows carry data-favkey, never
  // data-key, so this can never match a clone.
  function realItem(key) {
    var k = (window.CSS && CSS.escape) ? CSS.escape(key) : key;
    return document.querySelector('.cyg-sidebar-scroll .cyg-nav-item[data-key="' + k + '"]');
  }

  function labelOf(el) {
    var l = el && el.querySelector('.cyg-nav-item-label');
    return l ? l.textContent.trim() : '';
  }

  // Navigate by delegating to the real row: whatever handleClick() does for
  // href items, view items, instant-nav or active-project scoping keeps
  // working, with no second copy of that logic here.
  function go(key) {
    var target = realItem(key);
    if (target) target.click();
  }

  /* ---------- the Pinned section ----------------------------------------- */

  function buildPinNode(key) {
    var src = realItem(key);
    // Not rendered right now (AI disabled, role/project gating). Keep the
    // pin stored so it comes back when the row does.
    if (!src) return null;

    var node = src.cloneNode(true);
    node.removeAttribute('data-key');
    node.removeAttribute('id');
    node.className = 'cyg-nav-item cyg-fav-item';
    node.setAttribute('data-favkey', key);
    node.setAttribute('tabindex', '0');
    node.setAttribute('role', 'link');
    node.setAttribute('draggable', 'true');
    node.title = labelOf(src);

    var chev = node.querySelector('.cyg-nav-chev');   if (chev) chev.remove();
    var dupe = node.querySelector('.cyg-fav-star');   if (dupe) dupe.remove();

    var unpin = document.createElement('button');
    unpin.type = 'button';
    unpin.className = 'cyg-fav-star on';
    unpin.textContent = '★';
    unpin.tabIndex = -1;
    unpin.title = 'Unpin';
    unpin.setAttribute('aria-label', 'Unpin ' + node.title);
    unpin.addEventListener('click', function (e) {
      e.preventDefault(); e.stopPropagation(); togglePin(key);
    });
    node.appendChild(unpin);

    // stopPropagation keeps the sidebar's own delegated click handler out of
    // this clone, so a pin can never fire navigation twice.
    node.addEventListener('click', function (e) {
      if (e.target.closest('.cyg-fav-star')) return;
      e.preventDefault(); e.stopPropagation(); go(key);
    });
    node.addEventListener('keydown', function (e) {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); e.stopPropagation(); go(key); }
    });

    wireDrag(node, key);
    return node;
  }

  function renderPins() {
    var host = scrollEl();
    if (!host) return;

    var pins = getPins();
    var sec  = host.querySelector('.cyg-fav-section');

    // Nothing pinned: no section at all. The block used to open with a
    // "hover and click the star" hint on every page for everyone who had
    // never pinned anything, which is most people most of the time. The
    // star still appears on hover, so pinning is still discoverable; the
    // section appears the moment the first pin lands.
    if (!pins.length) { if (sec) sec.remove(); return; }

    if (!sec) {
      sec = document.createElement('div');
      sec.className = 'cyg-nav-section cyg-fav-section';
      host.insertBefore(sec, host.firstChild);
    }
    sec.textContent = '';

    var head = document.createElement('div');
    head.className = 'cyg-nav-label';
    head.textContent = 'Pinned';
    sec.appendChild(head);

    pins.forEach(function (key) {
      var node = buildPinNode(key);
      if (node) sec.appendChild(node);
    });

  }

  /* ---------- the star on every real row --------------------------------- */

  function syncStars() {
    var pins = getPins();
    var rows = document.querySelectorAll('.cyg-sidebar-scroll .cyg-nav-item[data-key]');
    Array.prototype.forEach.call(rows, function (item) {
      // Adaptation for this sidebar: group expanders (rows with a chevron)
      // are not destinations — no star, nothing to pin.
      if (item.querySelector('.cyg-nav-chev')) return;
      var key = item.getAttribute('data-key');
      var btn = item.querySelector('.cyg-fav-star');
      if (!btn) {
        btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'cyg-fav-star';
        btn.tabIndex = -1;
        btn.addEventListener('click', function (e) {
          e.preventDefault(); e.stopPropagation(); togglePin(key);
        });
        item.appendChild(btn);
      }
      var on = pins.indexOf(key) > -1;
      btn.classList.toggle('on', on);
      btn.textContent = on ? '★' : '☆';
      btn.title = (on ? 'Unpin from top' : 'Pin to top') + ' (P)';
      btn.setAttribute('aria-label', (on ? 'Unpin ' : 'Pin ') + labelOf(item));
    });
  }

  // Mirror .active onto the pinned copy of the current page.
  function syncActive() {
    var clones = document.querySelectorAll('.cyg-fav-item');
    Array.prototype.forEach.call(clones, function (n) {
      var r = realItem(n.getAttribute('data-favkey'));
      n.classList.toggle('active', !!(r && r.classList.contains('active')));
    });
  }

  /* ---------- drag to reorder -------------------------------------------- */

  var dragKey = null;

  function clearDropMarks() {
    var marked = document.querySelectorAll('.cyg-fav-drop-before, .cyg-fav-drop-after');
    Array.prototype.forEach.call(marked, function (n) {
      n.classList.remove('cyg-fav-drop-before', 'cyg-fav-drop-after');
    });
  }

  function dropsAfter(node, e) {
    var r = node.getBoundingClientRect();
    return (e.clientY - r.top) > r.height / 2;
  }

  function wireDrag(node, key) {
    node.addEventListener('dragstart', function (e) {
      dragKey = key;
      node.classList.add('cyg-fav-dragging');
      try { e.dataTransfer.effectAllowed = 'move'; e.dataTransfer.setData('text/plain', key); } catch (err) {}
    });
    node.addEventListener('dragend', function () {
      dragKey = null; node.classList.remove('cyg-fav-dragging'); clearDropMarks();
    });
    node.addEventListener('dragover', function (e) {
      if (!dragKey || dragKey === key) return;
      e.preventDefault();
      clearDropMarks();
      node.classList.add(dropsAfter(node, e) ? 'cyg-fav-drop-after' : 'cyg-fav-drop-before');
    });
    node.addEventListener('drop', function (e) {
      if (!dragKey || dragKey === key) return;
      e.preventDefault(); e.stopPropagation();
      var after = dropsAfter(node, e);
      var pins  = getPins();
      var from  = pins.indexOf(dragKey);
      if (from < 0) { clearDropMarks(); return; }
      pins.splice(from, 1);
      var to = pins.indexOf(key);
      if (to < 0) to = pins.length - 1;
      pins.splice(after ? to + 1 : to, 0, dragKey);
      clearDropMarks();
      savePins(pins);
    });
  }

  /* ---------- keep it alive across sidebar rebuilds ----------------------- */

  var mo = null, applying = false, pending = false;

  function observe() {
    var aside = document.querySelector('.cyg-sidebar');
    if (mo && aside) {
      mo.observe(aside, { childList: true, subtree: true, attributes: true, attributeFilter: ['class'] });
    }
  }

  function apply() {
    if (applying) return;
    applying = true;
    if (mo) mo.disconnect();                 // our own writes must not re-trigger us
    try { renderPins(); syncStars(); syncActive(); }
    catch (e) { if (window.console) console.warn('[cyg-fav]', e); }
    finally { observe(); applying = false; }
  }

  function schedule() {
    if (pending || applying) return;
    pending = true;
    requestAnimationFrame(function () { pending = false; apply(); });
  }

  // mount()/refresh()/setActive() rebuild or restyle the nav — re-apply after.
  ['mount', 'refresh', 'setActive', 'setCollapsed'].forEach(function (fn) {
    if (typeof SB[fn] !== 'function') return;
    var orig = SB[fn];
    SB[fn] = function () { var out = orig.apply(this, arguments); schedule(); return out; };
  });

  // Keyboard: P toggles the pin on the focused nav row (no extra tab stops).
  document.addEventListener('keydown', function (e) {
    if (e.key !== 'p' && e.key !== 'P') return;
    if (e.ctrlKey || e.metaKey || e.altKey) return;
    var el = document.activeElement;
    if (!el || !el.classList || !el.classList.contains('cyg-nav-item')) return;
    var key = el.getAttribute('data-key') || el.getAttribute('data-favkey');
    if (!key) return;
    e.preventDefault();
    togglePin(key);
  });

  // Pinned in another tab? Follow along.
  window.addEventListener('storage', function (e) {
    if (e.key && e.key.indexOf(STORE_BASE) === 0) schedule();
  });

  var tries = 0;
  function init() {
    if (!document.querySelector('.cyg-sidebar-scroll')) {
      if (tries++ > 60) return;              // sidebar mounts async on some pages
      return void setTimeout(init, 100);
    }
    mo = new MutationObserver(schedule);
    apply();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();

  /* ---------- small public surface --------------------------------------- */
  SB.getPins     = getPins;
  SB.togglePin   = togglePin;
  SB.refreshPins = apply;
})();
