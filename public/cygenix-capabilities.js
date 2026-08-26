/* ============================================================================
   cygenix-capabilities.js — the one place a capability claim is made.
   ----------------------------------------------------------------------------
   WHY THIS FILE EXISTS

   Four contradictions were found by reading the public pages against the code:

     1. MySQL / MariaDB is sold on Pro, Business and Enterprise. There is no
        MySQL driver in this repository. package.json has mssql and pg.
     2. PostgreSQL is sold from Pro upward and IS fully implemented — but
        help.html still says Cygenix connects "to your source and target SQL
        Server databases", so a technical evaluator reading the docs concludes
        the opposite of the truth.
     3. Server-level migration is the lead feature on the homepage and a
        2,900-line module in the product. The help documentation mentions it
        once, in passing.
     4. The site claims role-based access. The help page tells an
        administrator to set a role by editing app_metadata in NETLIFY
        IDENTITY — a product this codebase no longer uses at all, in front of
        a ten-role permission matrix it does not mention.

   Each on its own is small. Together they read as a product described ahead
   of where it is, which is the most expensive impression to leave with an
   enterprise buyer — and two of them are the reverse: capability that exists
   and is not claimed.

   Correcting the copy once does not hold. It drifted here and it would drift
   again. So the claims live in one machine-readable place, and three things
   read from it: the UI that offers a choice, the server that admits a job,
   and the documentation, which is generated. A test fails the build when a
   page asserts something this file does not support.

   HOW TO CHANGE A CLAIM

   Edit this file. Then run `node scripts/build-capability-docs.js` and commit
   what it writes. Do not edit the generated block in help.html — the next run
   overwrites it, and tests/capabilities.test.js fails while the two disagree.

   STATUS VALUES

     ga           shipped, supported, safe to sell
     beta         shipped and usable, not yet proven across enough estates
     preview      demonstrable, but not against real systems yet
     planned      intended, not built. MUST be labelled as such wherever it
                  appears on a public page
     unsupported  will not work, and the reason is part of the answer

   Node-requirable, because the tests and the docs generator both read it.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixCapabilities) root.CygenixCapabilities = api;
})(typeof window !== 'undefined' ? window : this, function () {
'use strict';

var STATUSES = ['ga', 'beta', 'preview', 'planned', 'unsupported'];

var STATUS_LABEL = {
  ga:          'Available',
  beta:        'Beta',
  preview:     'Preview',
  planned:     'Planned',
  unsupported: 'Not supported',
};

/* A claim is safe to make on a public page without a qualifier only when it
   is ga or beta. Everything else has to say what it is. */
function isSellable(status) { return status === 'ga' || status === 'beta'; }

/* ══ Sources ═══════════════════════════════════════════════════════════════
   `evidence` names what in the repository makes the status true. It is not
   decoration: it is what a reviewer checks when the status looks optimistic. */
var SOURCES = [
  { id: 'mssql', label: 'SQL Server', status: 'ga', versions: '2012+',
    detail: 'On-premises, Azure VM, AWS RDS, Google Cloud SQL.',
    evidence: 'netlify/functions/db-connect.js — mssql driver' },
  { id: 'azuresqldb', label: 'Azure SQL Database', status: 'ga',
    evidence: 'netlify/functions/db-connect.js — mssql driver' },
  { id: 'azuresqlmi', label: 'Azure SQL Managed Instance', status: 'beta',
    detail: 'Connects and migrates as SQL Server. Not yet proven across enough estates to call it GA.',
    evidence: 'netlify/functions/db-connect.js — mssql driver' },
  { id: 'postgres', label: 'PostgreSQL', status: 'ga',
    detail: 'Self-hosted, RDS, Aurora, Azure Database, Cloud SQL, Supabase, Neon.',
    evidence: 'netlify/functions/db-connect.js — pg driver, dialect detection, SSL handling' },
  { id: 'files', label: 'CSV, Excel and text', status: 'ga',
    detail: '.csv, .xlsx, .tsv, .txt — multi-sheet workbooks handled.',
    evidence: 'public/dashboard-app.js — file import' },
  { id: 'blob', label: 'Azure Blob Storage', status: 'ga',
    detail: 'Files in a container you supply a SAS for.',
    evidence: 'azure-function/src/blob-helpers.js' },
  { id: 'mysql', label: 'MySQL / MariaDB', status: 'planned',
    detail: 'No driver is present. Sold on the pricing page until 25 Aug 2026; withdrawn until it ships.',
    evidence: 'none — package.json declares mssql and pg only' },
];

/* ══ Targets ══════════════════════════════════════════════════════════════ */
var TARGETS = [
  { id: 'mssql', label: 'SQL Server', status: 'ga', versions: '2012+',
    engineEditions: [1, 2, 3, 4],
    evidence: 'public/project-builder-app.js — staging + set-based load' },
  { id: 'azuresqldb', label: 'Azure SQL Database', status: 'ga',
    engineEditions: [5],
    evidence: 'public/project-builder-app.js' },
  { id: 'azuresqlmi', label: 'Azure SQL Managed Instance', status: 'beta',
    engineEditions: [8],
    evidence: 'public/project-builder-app.js' },
  { id: 'postgres', label: 'PostgreSQL', status: 'ga',
    evidence: 'netlify/functions/db-connect.js — pg driver' },
  { id: 'mysql', label: 'MySQL / MariaDB', status: 'planned',
    detail: 'No driver is present.', evidence: 'none' },
];

/* ══ Pairs ════════════════════════════════════════════════════════════════
   What a given source→target combination can actually do. Absent means the
   pair has not been assessed; `pairStatus` reports that honestly rather than
   assuming it works. */
var PAIRS = [
  { source: 'mssql', target: 'mssql',      status: 'ga',
    serverObjects: 'supported',           verification: 'full' },
  { source: 'mssql', target: 'azuresqldb', status: 'ga',
    serverObjects: 'requires_equivalent', verification: 'full' },
  { source: 'mssql', target: 'azuresqlmi', status: 'beta',
    serverObjects: 'supported',           verification: 'full' },
  { source: 'mssql', target: 'postgres',   status: 'beta',
    serverObjects: 'unsupported',         verification: 'full',
    detail: 'Cross-engine. Type mapping is flagged for lossy conversions rather than silently applied.' },
  { source: 'postgres', target: 'postgres',   status: 'ga',
    serverObjects: 'unsupported',         verification: 'full' },
  { source: 'postgres', target: 'mssql',      status: 'beta',
    serverObjects: 'unsupported',         verification: 'full' },
  { source: 'postgres', target: 'azuresqldb', status: 'beta',
    serverObjects: 'unsupported',         verification: 'full' },
  { source: 'files', target: 'mssql',      status: 'ga', serverObjects: 'not_applicable', verification: 'full' },
  { source: 'files', target: 'azuresqldb', status: 'ga', serverObjects: 'not_applicable', verification: 'full' },
  { source: 'files', target: 'postgres',   status: 'ga', serverObjects: 'not_applicable', verification: 'full' },
  { source: 'blob',  target: 'mssql',      status: 'ga', serverObjects: 'not_applicable', verification: 'full' },
  { source: 'blob',  target: 'azuresqldb', status: 'ga', serverObjects: 'not_applicable', verification: 'full' },
];

/* ══ Server-level objects ══════════════════════════════════════════════════
   The matrix that makes this feature honest. Target capability differs
   enormously and the product must say so BEFORE it runs, not fail at apply
   time. A migration to Azure SQL Database that silently drops logins and
   Agent jobs is worse than one that refuses and explains.

   Verdicts: supported · supported_with_changes · requires_equivalent ·
             unsupported · not_applicable */
var SERVER_OBJECTS = [
  { id: 'logins',      label: 'Server logins',
    mssql: 'supported', azuresqlmi: 'supported',
    azuresqldb: { verdict: 'requires_equivalent',
      reason: 'Azure SQL Database has no server-level principals.',
      equivalent: 'Contained database users — CREATE USER … WITH PASSWORD, or FROM EXTERNAL PROVIDER for Entra.' },
    postgres: { verdict: 'unsupported', reason: 'A different engine with a different principal model.',
      equivalent: 'CREATE ROLE … LOGIN, scripted by hand.' } },

  { id: 'server_roles', label: 'Server roles',
    mssql: 'supported', azuresqlmi: 'supported',
    azuresqldb: { verdict: 'requires_equivalent',
      reason: 'No server-level roles exist.',
      equivalent: 'Database roles, plus the built-in Azure SQL roles.' },
    postgres: { verdict: 'unsupported', reason: 'Different engine.', equivalent: 'Group roles.' } },

  { id: 'agent_jobs',  label: 'SQL Agent jobs',
    mssql: 'supported', azuresqlmi: 'supported',
    azuresqldb: { verdict: 'requires_equivalent',
      reason: 'There is no SQL Agent in Azure SQL Database.',
      equivalent: 'Elastic Jobs, or an Azure Automation runbook. Cygenix scripts the job for review; it does not translate it.' },
    postgres: { verdict: 'unsupported', reason: 'Different engine.', equivalent: 'pg_cron, or an external scheduler.' } },

  { id: 'ssis',        label: 'SSIS projects',
    mssql: 'supported', azuresqlmi: 'supported',
    azuresqldb: { verdict: 'requires_equivalent',
      reason: 'No SSISDB catalog exists in Azure SQL Database.',
      equivalent: 'Azure-SSIS Integration Runtime, or Azure Data Factory.' },
    postgres: { verdict: 'unsupported', reason: 'Different engine.', equivalent: 'None.' } },

  { id: 'linked_servers', label: 'Linked servers',
    mssql: 'supported',
    azuresqlmi: { verdict: 'supported_with_changes',
      reason: 'Managed Instance supports a narrower set of providers than boxed SQL Server.',
      equivalent: null },
    azuresqldb: { verdict: 'requires_equivalent',
      reason: 'Azure SQL Database has no linked servers.',
      equivalent: 'External data sources — CREATE EXTERNAL DATA SOURCE, elastic query.' },
    postgres: { verdict: 'unsupported', reason: 'Different engine.', equivalent: 'postgres_fdw.' } },

  { id: 'credentials', label: 'Credentials and proxies',
    mssql: 'supported',
    azuresqlmi: { verdict: 'supported_with_changes',
      reason: 'Managed Instance supports credentials but not every proxy subsystem.', equivalent: null },
    azuresqldb: { verdict: 'unsupported',
      reason: 'Neither credentials nor Agent proxies exist in Azure SQL Database.', equivalent: null },
    postgres: { verdict: 'unsupported', reason: 'Different engine.', equivalent: null } },
];

/* ══ Features ══════════════════════════════════════════════════════════════
   `claims` are the phrases that assert this capability on a public page. The
   CI check looks for them: a phrase whose feature is not ga or beta must
   carry a qualifier, or the build fails. This is the mechanism that stops the
   copy drifting ahead of the code again. */
var FEATURES = [
  { id: 'ai_mapping', label: 'AI column mapping', status: 'ga', minTier: 'starter',
    claims: ['AI column mapping', 'AI-assisted mapping'],
    evidence: 'public/cygenix-model.js, the mapping generator' },

  { id: 'server_objects', label: 'Server-level object migration', status: 'beta', minTier: 'business',
    claims: ['server-level object', 'Server Migration'],
    detail: 'Logins with hashed passwords and SIDs, Agent jobs, SSIS projects and linked servers. '
      + 'Target support varies — see the server-object matrix.',
    evidence: 'public/server-migration.js — verify, discover, preview, execute, audit' },

  { id: 'agentive', label: 'Agentive migration', status: 'beta', minTier: 'pro',
    claims: ['Agentive migration', 'agentive mode'],
    evidence: 'public/agentive_migration.html, azure-function/src/agent*.js' },

  { id: 'verification', label: 'Validation and preflight', status: 'ga', minTier: 'starter',
    detail: 'Rule-based validation, preflight rejection forecasting, and per-job verify SQL.',
    claims: ['validation engine', 'preflight'],
    evidence: 'public/cygenix-preflight.js, public/cygenix-assurance.js' },

  { id: 'reconciliation_row_level', label: 'Row-level reconciliation', status: 'planned', minTier: 'business',
    detail: 'Row-hash diff across a source/target pair with a signed report. Not built. '
      + 'What exists today is validation and preflight — see above.',
    claims: ['row-level reconciliation', 'row-by-row proof', 'reconciliation report'],
    evidence: 'none' },

  { id: 'audit_trail', label: 'Tamper-evident audit trail', status: 'beta', minTier: 'business',
    detail: 'Hash-chained, append-only, with a verification endpoint. Covers access control and '
      + 'gated actions; AI provenance is not yet recorded.',
    claims: ['audit trail', 'tamper-evident'],
    evidence: 'netlify/functions/lib/rbac.js — chainEntry/verifyEntries; lib/org-store.js' },

  { id: 'rbac', label: 'Role-based access control', status: 'ga', minTier: 'business',
    detail: 'Ten roles, a permission matrix, separation of duties, and environment classification — '
      + 'all enforced server-side.',
    claims: ['role-based access', 'RBAC'],
    evidence: 'netlify/functions/lib/rbac.js' },

  { id: 'restore', label: 'Restore database', status: 'beta', minTier: 'business',
    claims: ['restore database'],
    evidence: 'public/cygenix-restore.js' },

  { id: 'data_stream', label: 'Data Stream', status: 'preview', minTier: 'business',
    detail: 'Capture, Stream Store and delivery, running against seeded demo data. '
      + 'No connectivity to a real database or broker yet.',
    claims: ['data stream', 'change data capture'],
    evidence: 'public/cygenix-datastream.js — demo world, marked as such' },

  { id: 'cutover_planning', label: 'Cutover planning and delta re-sync', status: 'planned', minTier: 'business',
    detail: 'Downtime estimation and a bounded incremental pass. Not built.',
    claims: ['cutover planner', 'delta re-sync', 'downtime estimate'],
    evidence: 'none' },

  { id: 'tenancy', label: 'Shared team workspaces', status: 'beta', minTier: 'business',
    detail: 'Projects belong to a workspace and colleagues join by invitation. Jobs, schedules and '
      + 'saved scripts are still personal — those live in Cosmos, partitioned per user, and moving '
      + 'them needs a container rebuild.',
    claims: ['shared workspace', 'team workspace'],
    evidence: 'netlify/functions/lib/tenancy.js, lib/authz.js, projects.js keyed on the tenant' },

  { id: 'guardrails', label: 'Change guardrails and the two-person rule', status: 'beta',
    minTier: 'business',
    detail: 'Enforced server-side against a hash of the exact statement, so skipping the interface '
      + 'does not skip the guardrail. Covers writes to PROD and STAGING targets through the console.',
    claims: ['two-person rule', 'change guardrails'],
    evidence: 'netlify/functions/lib/tenancy.js — requirementFor/consumeApproval; db-connect.js' },
];

/* ══ Tiers ════════════════════════════════════════════════════════════════
   Read server-side at job admission. Never sent from the client. */
var TIERS = [
  { id: 'starter',    label: 'Starter',    concurrentMigrations: 1,    maxRowsPerJob: 1000000,
    monthlySubmissions: 50,   retentionDays: 30 },
  { id: 'pro',        label: 'Pro',        concurrentMigrations: 3,    maxRowsPerJob: 10000000,
    monthlySubmissions: 500,  retentionDays: 90 },
  { id: 'business',   label: 'Business',   concurrentMigrations: 10,   maxRowsPerJob: 100000000,
    monthlySubmissions: 5000, retentionDays: 365 },
  { id: 'enterprise', label: 'Enterprise', concurrentMigrations: null, maxRowsPerJob: null,
    monthlySubmissions: null, retentionDays: 365 },
];
var TIER_ORDER = TIERS.map(function (t) { return t.id; });

/* ══ Engine detection ══════════════════════════════════════════════════════
   One probe, one classification, so the Restore module, Server Migration and
   anything added later agree about what they are pointed at.
   SERVERPROPERTY('EngineEdition'): 5 = Azure SQL Database, 8 = Managed
   Instance, 1–4 = boxed SQL Server. */
var ENGINE_PROBE_SQL =
  "SELECT CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) AS edition, "
  + "CAST(SERVERPROPERTY('EngineEdition') AS INT) AS engine_edition, "
  + "CAST(SERVERPROPERTY('ProductMajorVersion') AS INT) AS major_version";

function classifyEngine(engineEdition) {
  var e = Number(engineEdition);
  for (var i = 0; i < TARGETS.length; i++) {
    var t = TARGETS[i];
    if (t.engineEditions && t.engineEditions.indexOf(e) !== -1) {
      return { id: t.id, label: t.label, status: t.status };
    }
  }
  return { id: 'unknown', label: 'Unknown engine (' + engineEdition + ')', status: 'unsupported' };
}

/* ══ Lookups ══════════════════════════════════════════════════════════════ */
function find(list, id) {
  for (var i = 0; i < list.length; i++) if (list[i].id === id) return list[i];
  return null;
}
function source(id) { return find(SOURCES, id); }
function target(id) { return find(TARGETS, id); }
function feature(id) { return find(FEATURES, id); }
function tier(id)    { return find(TIERS, id); }

/* Is this pair usable, and if not, what does the user need to be told?
   An unassessed pair says so rather than being assumed to work — that is the
   difference between a manifest and a guess. */
function pairStatus(sourceId, targetId) {
  var s = source(sourceId), t = target(targetId);
  if (!s) return { status: 'unsupported', reason: 'Unknown source "' + sourceId + '".' };
  if (!t) return { status: 'unsupported', reason: 'Unknown target "' + targetId + '".' };
  if (!isSellable(s.status)) {
    return { status: s.status, reason: s.label + ' is ' + STATUS_LABEL[s.status].toLowerCase()
      + ' as a source.' + (s.detail ? ' ' + s.detail : '') };
  }
  if (!isSellable(t.status)) {
    return { status: t.status, reason: t.label + ' is ' + STATUS_LABEL[t.status].toLowerCase()
      + ' as a target.' + (t.detail ? ' ' + t.detail : '') };
  }
  for (var i = 0; i < PAIRS.length; i++) {
    var p = PAIRS[i];
    if (p.source === sourceId && p.target === targetId) {
      return { status: p.status, reason: p.detail || null,
        serverObjects: p.serverObjects, verification: p.verification };
    }
  }
  return { status: 'unsupported',
    reason: s.label + ' to ' + t.label + ' has not been assessed. '
      + 'It is not blocked because it is known to fail — it is blocked because nobody has proved it works.' };
}

/* What happens to one class of server-level object against one target. This
   is the answer Server Migration needs BEFORE it discovers anything. */
function serverObjectSupport(objectId, targetId) {
  var o = find(SERVER_OBJECTS, objectId);
  if (!o) return { verdict: 'unsupported', reason: 'Unknown object class "' + objectId + '".' };
  var v = o[targetId];
  if (v === undefined) {
    return { verdict: 'unsupported', label: o.label,
      reason: 'Support for ' + o.label.toLowerCase() + ' on this target has not been assessed.' };
  }
  if (typeof v === 'string') return { verdict: v, label: o.label, reason: null, equivalent: null };
  return { verdict: v.verdict, label: o.label, reason: v.reason, equivalent: v.equivalent || null };
}

/* The whole matrix for one target, which is what the UI shows before a run. */
function serverObjectPlan(targetId) {
  var t = target(targetId);
  var rows = SERVER_OBJECTS.map(function (o) {
    var s = serverObjectSupport(o.id, targetId);
    return { id: o.id, label: o.label, verdict: s.verdict, reason: s.reason, equivalent: s.equivalent };
  });
  var movable = rows.filter(function (r) {
    return r.verdict === 'supported' || r.verdict === 'supported_with_changes'; });
  return {
    target: t ? t.label : targetId,
    targetId: targetId,
    rows: rows,
    movableCount: movable.length,
    blockedCount: rows.length - movable.length,
    // The headline a screen shows before anyone presses a button.
    verdict: movable.length === 0 ? 'none'
           : movable.length === rows.length ? 'all' : 'partial',
  };
}

/* Tier gating. `null` on a tier limit means unlimited. */
function tierAllows(tierId, featureId) {
  var f = feature(featureId);
  if (!f) return { allowed: false, reason: 'Unknown feature "' + featureId + '".' };
  if (!isSellable(f.status)) {
    return { allowed: false, status: f.status,
      reason: f.label + ' is ' + STATUS_LABEL[f.status].toLowerCase() + '.'
        + (f.detail ? ' ' + f.detail : '') };
  }
  var have = TIER_ORDER.indexOf(tierId);
  var need = TIER_ORDER.indexOf(f.minTier || 'starter');
  if (have === -1) return { allowed: false, reason: 'Unknown plan "' + tierId + '".' };
  if (have < need) {
    return { allowed: false, status: f.status, minTier: f.minTier,
      reason: f.label + ' is available from the ' + (tier(f.minTier) || {}).label + ' plan.' };
  }
  return { allowed: true, status: f.status };
}

function limitFor(tierId, limit) {
  var t = tier(tierId);
  if (!t) return null;
  return t[limit];
}

/* Job admission, server-side. Returns null when the job may run, or the
   reason it may not — phrased for the operator, naming the limit and the
   plan rather than failing generically. */
function admitJob(tierId, job) {
  var t = tier(tierId);
  if (!t) return { ok: false, reason: 'Unknown plan "' + tierId + '".' };
  var j = job || {};

  var pair = pairStatus(j.source, j.target);
  if (!isSellable(pair.status)) {
    return { ok: false, code: 'pair_unsupported',
      reason: pair.reason || 'That source and target combination is not supported.' };
  }
  if (t.maxRowsPerJob != null && j.rows > t.maxRowsPerJob) {
    return { ok: false, code: 'rows_exceeded',
      reason: 'This job moves ' + Number(j.rows).toLocaleString() + ' rows. The '
        + t.label + ' plan allows ' + t.maxRowsPerJob.toLocaleString() + ' per job.' };
  }
  if (t.concurrentMigrations != null && j.running >= t.concurrentMigrations) {
    return { ok: false, code: 'concurrency_exceeded',
      reason: 'The ' + t.label + ' plan runs ' + t.concurrentMigrations
        + ' migration(s) at once. Wait for one to finish, or upgrade.' };
  }
  if (t.monthlySubmissions != null && j.submittedThisMonth >= t.monthlySubmissions) {
    return { ok: false, code: 'quota_exceeded',
      reason: 'The ' + t.label + ' plan allows ' + t.monthlySubmissions.toLocaleString()
        + ' job submissions a month, and this month is used up.' };
  }
  return { ok: true };
}

return {
  STATUSES: STATUSES, STATUS_LABEL: STATUS_LABEL, isSellable: isSellable,
  SOURCES: SOURCES, TARGETS: TARGETS, PAIRS: PAIRS,
  SERVER_OBJECTS: SERVER_OBJECTS, FEATURES: FEATURES, TIERS: TIERS, TIER_ORDER: TIER_ORDER,
  ENGINE_PROBE_SQL: ENGINE_PROBE_SQL, classifyEngine: classifyEngine,
  source: source, target: target, feature: feature, tier: tier,
  pairStatus: pairStatus,
  serverObjectSupport: serverObjectSupport, serverObjectPlan: serverObjectPlan,
  tierAllows: tierAllows, limitFor: limitFor, admitJob: admitJob,
};
});
