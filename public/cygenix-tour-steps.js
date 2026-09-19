/* ============================================================================
   cygenix-tour-steps.js — what the guided tour says, and nothing about how.
   ----------------------------------------------------------------------------
   Content only. The engine is cygenix-tour.js; this file is here so the copy
   can be corrected by someone who has never read the engine, which is the
   normal case — the person who knows whether "Schedules" is described
   accurately is not the person maintaining a spotlight.

   WHY THE SELECTORS ARE WHAT THEY ARE
   Every target below is a selector the console chrome already emits. Rail
   items render `data-key="jobs"`; the masthead's controls carry ids —
   `#cx-mh-search`, `#cyg-proj-btn`, `#cyg-user-chip` — and its Files button
   the class `.cyg-drive-btn`; the assistant's footer is `.cyga-foot`. The
   brief once asked for `data-tour="…"` hooks on all of those, and they would
   have been a second set of names to keep in step with the first. The one
   hook that genuinely had no selector — the Home cards area — is the one
   that was added.

   THE RAIL IS FIVE GROUPS (console redesign, Sep-2026)
   Connect · Model · Run · Quality · Govern, thirteen items. What used to be
   a fold-out group's children — Packages, Server migration, Change events,
   Validation, Conversion report and the rest — are tabs inside the screen
   they belong to, so a stop that used to spotlight a group now spotlights
   the item and names its tabs in the body. Search is the masthead field,
   the project switcher and the account menu (Settings, Users & roles,
   Governance) are in the masthead too. The tour walks the rail top to
   bottom, in rail order, because a highlight that jumps back up the
   sidebar reads as broken rather than guided.

   ON THE DESCRIPTIONS
   These were drafted from the menu labels and then corrected against what each
   screen actually does. Two were wrong in the draft and are worth naming, so
   nobody "fixes" them back:

     · Schedules (formerly Task Manager) is the SCHEDULER — cron triggers,
       chained jobs, run history. It is not a to-do list.
     · Server migration moves SERVER-LEVEL OBJECTS — logins, SQL Agent jobs,
       SSIS projects, linked servers. "Whole servers and databases" oversells
       it; the tables are the rest of the product.

   A step whose target is missing or zero-size is skipped by the engine and
   does not count toward the total, so an item hidden by role never leaves a
   gap in the numbering.

   Schema: { id, section, title, body, page, target, region?, final? }
     page   — a navigation key (rail item, tab or account item); the engine
              routes through the sidebar's own navigation rather than faking
              a click.
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

  { id: 'home', section: 'Start here', title: 'Home — what now', page: 'dashboard',
    target: '[data-key="dashboard"]', region: '[data-tour="home-cards"]',
    body: "Your landing page answers one question: is anything broken, and what needs you. The plan "
        + "and the readiness score sit on the left; approvals, breaches, failed jobs and blocked "
        + "streams queue on the right, most urgent first." },

  { id: 'files', section: 'Start here', title: 'Files', page: 'dashboard', target: '.cyg-drive-btn',
    body: "Your shared Drive, up in the masthead. Anything saved here is available on every machine "
        + "you sign in on — mapping sheets, scripts, exports." },

  { id: 'search', section: 'Start here', title: 'Search', page: 'search', target: '#cx-mh-search',
    body: "Searches your own work — jobs, projects, artifacts and saved reports — rather than the "
        + "databases you are migrating. Type a name and press <kbd>Enter</kbd>." },

  { id: 'project', section: 'Start here', title: 'Project', page: 'dashboard', target: '#cyg-proj-btn',
    body: "Everything is scoped to one active project, and you switch it here. The <b>Configurator</b> "
        + "that sizes a migration and the <b>Project plan</b> live under Reports." },

  { id: 'account', section: 'Start here', title: 'Your account', page: 'project-settings', target: '#cyg-user-chip',
    body: "<b>General settings</b> is where you add your Anthropic API key — that one field switches on "
        + "me and every other AI feature. Also here: Notifications, System parameters, Users &amp; roles "
        + "and Governance." },

  { id: 'connections', section: 'Connect', title: 'Connections', page: 'connections',
    target: '[data-key="connections"]',
    body: "Where you register the source and target databases. Most migrations start here — Cygenix "
        + "needs somewhere to read from and somewhere to load into before anything else works." },

  { id: 'profiles', section: 'Connect', title: 'Profiles &amp; integrations', page: 'profiles',
    target: '[data-key="profiles"]',
    body: "Saved connection profiles, classified by environment, so the same server can be reused across "
        + "projects and a production database is never touched by accident. The <b>Integrations</b> tab "
        + "connects the tools your team uses for notifications and file exchange." },

  { id: 'schema-explorer', section: 'Model', title: 'Schema explorer', page: 'schema-explorer',
    target: '[data-key="schema-explorer"]',
    body: "Browses both schemas side by side, including relationships it infers rather than reads. "
        + "Start here when you do not yet know what is in a database." },

  { id: 'mapping', section: 'Model', title: 'Object mapping', page: 'object-mapping',
    target: '[data-key="object-mapping"]',
    body: "Where source columns are matched to target columns, with a confidence score on each match. "
        + "<b>Conversion templates</b> decide which tables exist to be mapped; <b>AI assist</b> drafts "
        + "for you to review before anything runs." },

  { id: 'sql', section: 'Model', title: 'SQL editor', page: 'sql-editor', target: '[data-key="sql-editor"]',
    body: "Write and run queries against any connection you have registered — handy for checking data "
        + "before a load and proving it after one." },

  { id: 'jobs', section: 'Run', title: 'Jobs &amp; packages', page: 'jobs', target: '[data-key="jobs"]',
    body: "<b>Jobs</b> lists every migration job, run or not. <b>Packages</b> bundles jobs into a unit "
        + "you can run, schedule and resume from a checkpoint; <b>Server migration</b> moves logins, "
        + "SQL Agent jobs and SSIS projects; <b>Analytics</b> reports on all of it." },

  { id: 'stream', section: 'Run', title: 'Data stream', page: 'data-stream', target: '[data-key="data-stream"]',
    body: "Continuous change capture, for keeping a target in step during a long cutover. "
        + "<b>Streams</b> carry the changes; <b>Change events</b> and <b>Stream monitor</b> show what "
        + "is flowing, what is queued and what the destination rejected." },

  { id: 'tasks', section: 'Run', title: 'Schedules', page: 'task-agent', target: '[data-key="task-agent"]',
    body: "The scheduler. Run a job on a cron schedule, chain one job to start when another succeeds, "
        + "and see the history of what ran and what failed." },

  { id: 'dq', section: 'Quality', title: 'Assurance', page: 'assurance', target: '[data-key="assurance"]',
    body: "Proving the data arrived intact. <b>Assurance</b> turns validation rules into checks that "
        + "keep running; <b>Quality review</b> and <b>Validation</b> are the one-off passes." },

  { id: 'cleansing', section: 'Quality', title: 'Cleansing &amp; enrichment', page: 'data-cleansing',
    target: '[data-key="data-cleansing"]',
    body: "<b>Cleansing</b> finds near-duplicate rows before they migrate; <b>Enrichment</b> fills the "
        + "gaps in a record from the providers you allow. Nothing is written until you review it." },

  { id: 'reports', section: 'Govern', title: 'Reports', page: 'report-builder',
    target: '[data-key="report-builder"]',
    body: "<b>Report builder</b> composes your own reports from the migration's data; the "
        + "<b>Conversion report</b> is the standard document produced for sign-off; <b>Project artifacts</b> "
        + "keeps every output in one place." },

  { id: 'audit', section: 'Govern', title: 'Audit log', page: 'audit', target: '[data-key="audit"]',
    body: "A record of who changed what, and when. Entries are hash-chained to each other, so a missing "
        + "or altered one shows up as a break rather than disappearing quietly. <b>Performance</b> and "
        + "<b>Diagnostics</b> sit beside it." },

  { id: 'assistant', section: 'Finish', title: 'Ask Cygenix', page: 'dashboard', target: '.cyga-foot',
    body: "That's me. Open me on any screen with <kbd>Ctrl</kbd>+<kbd>/</kbd>, then ask a question or "
        + "say what you want done. <b>Guardrails</b> below decides whether I ask before each change — "
        + "and I ask by default." },

  { id: 'done', section: 'Finish', title: "You're all set", page: 'dashboard', target: null, final: true,
    body: "A good first path: <b>1.</b> add your API key under your account → General settings, "
        + "<b>2.</b> register a Connection, <b>3.</b> create a project. Type <b>tour</b> any time to go "
        + "round again, or <b>tour data stream</b> to jump straight to one area." },
];
});
