/* ============================================================================
   cygenix-tour-steps.js — what the guided tour says, and nothing about how.
   ----------------------------------------------------------------------------
   Content only. The engine is cygenix-tour.js; this file is here so the copy
   can be corrected by someone who has never read the engine, which is the
   normal case — the person who knows whether "Task Manager" is described
   accurately is not the person maintaining a spotlight.

   WHY THE SELECTORS ARE WHAT THEY ARE
   Every target below is a selector the sidebar already emits. Parent groups
   render `data-parent="jobs-group"`; leaf items render `data-key="jobs"`; the
   Drive shortcut is `.cyg-drive-btn`; the assistant's footer is `.cyga-foot`.
   The brief asked for new `data-tour="…"` hooks on all of those, and they
   would have been a second set of names to keep in step with the first. The
   one hook that genuinely had no selector — the Home cards area — is the one
   that was added.

   ON THE DESCRIPTIONS
   These were drafted from the menu labels and then corrected against what each
   screen actually does. Two were wrong in the draft and are worth naming, so
   nobody "fixes" them back:

     · Task Manager is the SCHEDULER — cron triggers, chained jobs, run
       history. It is not a to-do list, which is what the menu name suggests
       and what the draft said.
     · Server Migration moves SERVER-LEVEL OBJECTS — logins, SQL Agent jobs,
       SSIS projects, linked servers. "Whole servers and databases" oversells
       it; the tables are the rest of the product.

   A step whose target is missing or zero-size is skipped by the engine and
   does not count toward the total, so an item hidden by role never leaves a
   gap in the numbering.

   Schema: { id, section, title, body, page, target, region?, final? }
     page   — a sidebar data-key; the engine routes through the sidebar's own
              navigation rather than faking a click.
     body   — 1–3 sentences, second person. <b> and <kbd> only.
   ========================================================================== */
(function (root, factory) {
  var steps = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = steps;
  if (root && typeof root === 'object') root.CygenixTourSteps = steps;
})(typeof window !== 'undefined' ? window : this, function () {
'use strict';

return [
  { id: 'welcome', section: 'Welcome', title: 'Welcome to Cygenix', page: 'dashboard', target: null,
    body: "I'll walk you through the console, one area at a time — about five minutes. "
        + "I'll only move around and point things out; nothing gets changed." },

  { id: 'home', section: 'Start here', title: 'Home — Migration Overview', page: 'dashboard',
    target: '[data-key="dashboard"]', region: '[data-tour="home-cards"]',
    body: "Your landing page. A blocked stream or other live alert appears at the top, then your "
        + "cutover readiness score, then your projects, schedules and the status of the active project." },

  { id: 'files', section: 'Start here', title: 'Files', page: 'dashboard', target: '.cyg-drive-btn',
    body: "Your shared Drive. Anything saved here is available on every machine you sign in on — "
        + "mapping sheets, scripts, exports." },

  { id: 'search', section: 'Start here', title: 'Search', page: 'search', target: '[data-key="search"]',
    body: "Searches your own work — jobs, projects, artifacts and saved reports — rather than the "
        + "databases you are migrating. Useful when you know a name but not where it lives." },

  { id: 'project', section: 'Start here', title: 'Project', page: 'effort-estimator',
    target: '[data-parent="project-group"]',
    body: "Everything is scoped to one active project, and you switch it here. <b>Configurator</b> "
        + "sizes the migration and estimates the effort; <b>Project Plan</b> lays out the tasks and milestones." },

  { id: 'connections', section: 'Connect', title: 'Connections', page: 'connections',
    target: '[data-key="connections"]',
    body: "Where you register the source and target databases. Most migrations start here — Cygenix "
        + "needs somewhere to read from and somewhere to load into before anything else works." },

  { id: 'profiles', section: 'Connect', title: 'Profiles', page: 'profiles', target: '[data-key="profiles"]',
    body: "Saved connection profiles, so the same server and settings can be reused across projects "
        + "instead of being typed in again each time." },

  { id: 'integrations', section: 'Connect', title: 'Integrations', page: 'integrations',
    target: '[data-key="integrations"]',
    body: "Connects Cygenix to the other tools your team uses for notifications and file exchange." },

  { id: 'settings', section: 'Connect', title: 'Settings', page: 'project-settings',
    target: '[data-parent="settings-group"]',
    body: "<b>General</b> is where you add your Anthropic API key — that one field switches on me and "
        + "every other AI feature. Also here: Notifications, System Parameters, and Users &amp; Roles." },

  { id: 'mapping', section: 'Map &amp; Build', title: 'Object Mapping', page: 'object-mapping',
    target: '[data-parent="objmap-group"]',
    body: "<b>Mapping</b> is where source columns are matched to target columns, with a confidence "
        + "score on each match. <b>Schema Explorer</b> browses both schemas side by side, including "
        + "relationships it infers rather than reads." },

  { id: 'sql', section: 'Map &amp; Build', title: 'SQL Editor', page: 'sql-editor', target: '[data-key="sql-editor"]',
    body: "Write and run queries against any connection you have registered — handy for checking data "
        + "before a load and proving it after one." },

  { id: 'ai', section: 'Map &amp; Build', title: 'AI Assist', page: 'agentive-migration',
    target: '[data-key="agentive-migration"]',
    body: "Suggests mappings, transformations and fixes for you to review before anything is applied. "
        + "It runs on your own API key, so nothing here spends Cygenix's money or starts without you." },

  { id: 'jobs', section: 'Run', title: 'Jobs', page: 'jobs', target: '[data-parent="jobs-group"]',
    body: "<b>All Jobs</b> lists every migration job, run or not. <b>Packages</b> bundles jobs into a "
        + "unit you can run, schedule and resume from a checkpoint." },

  { id: 'stream', section: 'Run', title: 'Data Stream', page: 'data-stream',
    target: '[data-parent="datastream-group"]',
    body: "Continuous change capture, for keeping a target in step during a long cutover. "
        + "<b>Streams</b> carry the changes; <b>Change Events</b> and <b>Stream Monitor</b> show what "
        + "is flowing, what is queued and what the destination rejected." },

  { id: 'tasks', section: 'Run', title: 'Task Manager', page: 'task-agent', target: '[data-key="task-agent"]',
    body: "The scheduler. Run a job on a cron schedule, chain one job to start when another succeeds, "
        + "and see the history of what ran and what failed." },

  { id: 'server', section: 'Run', title: 'Server Migration', page: 'server-migration',
    target: '[data-key="server-migration"]',
    body: "Moves the things that live outside the tables — logins, SQL Agent jobs, SSIS projects and "
        + "linked servers — which a table-by-table migration leaves behind." },

  { id: 'dq', section: 'Validate', title: 'Data Quality', page: 'assurance',
    target: '[data-parent="quality-group"]',
    body: "Proving the data arrived intact. <b>Assurance</b> turns validation rules into checks that "
        + "keep running; <b>Quality Review</b> and <b>Validation</b> are the one-off passes; "
        + "<b>Cleansing</b> and <b>Enrichment</b> fix and fill records on the way through." },

  { id: 'analytics', section: 'Insight', title: 'Analytics', page: 'analytics', target: '[data-key="analytics"]',
    body: "Delivery, quality and portfolio metrics across every job, stream and project. It is "
        + "read-only: every figure links through to the screen that can act on it." },

  { id: 'reports', section: 'Report &amp; Govern', title: 'Reports', page: 'report-builder',
    target: '[data-parent="reports-group"]',
    body: "<b>Report Builder</b> composes your own reports from the migration's data; "
        + "<b>Conversion Report</b> is the standard document produced for sign-off." },

  { id: 'artifacts', section: 'Report &amp; Govern', title: 'Project Artifacts', page: 'inventory',
    target: '[data-key="inventory"]',
    body: "Every document and output the project has produced, kept in one place rather than scattered "
        + "across the screens that made them." },

  { id: 'gov', section: 'Report &amp; Govern', title: 'Governance', page: 'privacy-security',
    target: '[data-key="privacy-security"]',
    body: "Who can do what, and to which environments. Roles, the classification that marks a "
        + "connection as production, and the two-person rule on destructive operations." },

  { id: 'audit', section: 'Report &amp; Govern', title: 'Audit Log', page: 'audit', target: '[data-key="audit"]',
    body: "A record of who changed what, and when. Entries are hash-chained to each other, so a "
        + "missing or altered one shows up as a break rather than disappearing quietly." },

  { id: 'monitoring', section: 'Report &amp; Govern', title: 'Monitoring', page: 'performance',
    target: '[data-parent="monitoring-group"]',
    body: "<b>Performance</b> shows throughput and timings; <b>Diagnostics</b> is where you go to work "
        + "out why something is slow or failing." },

  { id: 'assistant', section: 'Finish', title: 'Ask Cygenix', page: 'dashboard', target: '.cyga-foot',
    body: "That's me. Open me on any screen with <kbd>Ctrl</kbd>+<kbd>/</kbd>, then ask a question or "
        + "say what you want done. <b>Guardrails</b> below decides whether I ask before each change — "
        + "and I ask by default." },

  { id: 'done', section: 'Finish', title: "You're all set", page: 'dashboard', target: null, final: true,
    body: "A good first path: <b>1.</b> add your API key in Settings → General, <b>2.</b> register a "
        + "Connection, <b>3.</b> create a project. Type <b>tour</b> any time to go round again, or "
        + "<b>tour data stream</b> to jump straight to one area." },
];
});
