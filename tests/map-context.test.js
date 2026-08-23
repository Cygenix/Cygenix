// Tests for cygenix-map-context.js — the read-only resolver that gives
// Assurance its scope from the project's Object Mapping instead of the
// connection catalogue — and for the engine's converter that turns its
// derived-check specs into executable rules.
//
// The fixture mirrors the handoff brief's Demo project, including its two
// documented traps: a source/target pair with runs of different ages
// (definition and run must resolve separately, or a 76-row discrepancy is
// invented), and a composite wrapper saved after the run (whose timestamp
// must never mark the step stale). One deviation: the fixture adds a money
// column so recon.column_aggregate derivation is exercised too.
const mc = require('../public/cygenix-map-context.js');
const A = require('../public/cygenix-assurance.js');
const MC = mc.CygenixMapContext;

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Assurance — scope from the map\n');

const addrCols = [
  { srcCol: 'id', tgtCol: 'id', transform: 'NONE' },
  { srcCol: 'line1', tgtCol: 'contact_name', transform: 'NONE' },
  { srcCol: 'line1', tgtCol: 'address_line1', transform: 'NONE' },
  { srcCol: 'city', tgtCol: 'city', transform: 'NONE' },
  { srcCol: '', tgtCol: 'state', transform: 'NONE' },
  { srcCol: '', tgtCol: 'phone', transform: 'NONE' },
  { srcCol: '', tgtCol: 'email', transform: 'NONE' },
];
const caseCols = [
  { srcCol: 'id', tgtCol: 'id', transform: 'NONE' },
  { srcCol: 'caseno', tgtCol: 'caseno', transform: 'NONE' },
  { srcCol: '', tgtCol: 'CaseName', transform: 'LITERAL', literalValue: '@@date' },
  { srcCol: 'opened', tgtCol: 'created_date', transform: 'NONE' },
  { srcCol: 'fee_total', tgtCol: 'fee_total', transform: 'NONE' },
];
const CASES_VERIFY = "SELECT 'Conversion_DM.dbo.Cases' AS [Table], COUNT(*) AS [Rows]\n"
  + "FROM [dbo].[Cases] WHERE caseno like '%z%'\nUNION ALL\nSELECT 'dbo.cases', COUNT(*) FROM [dbo].[cases];";

const JOBS = [
  { id: 'job_1786903412202_ib7g', jobType: 'composite', name: 'Demo run', projectId: 'proj_demo',
    lastModified: '2026-08-16T18:03:00Z',                    /* trap 4.2: saved AFTER the run */
    childSteps: [
      { type: 'migration', jobId: 'job_1785097178031', name: 'dbo.addresses → dbo.addresses',
        srcTable: 'dbo.addresses', tgtTable: 'dbo.addresses', srcWhere: '',
        columnMapping: addrCols, targetPkCol: 'id', targetPkKind: 'pk', totalRows: 200 },
      { type: 'migration', jobId: 'job_1785146862818', name: 'dbo.Cases → dbo.cases',
        srcTable: 'dbo.Cases', tgtTable: 'dbo.cases', srcWhere: "caseno like '%z%'",
        columnMapping: caseCols, targetPkCol: 'id', targetPkKind: 'pk', totalRows: 100 },
    ] },
  { id: 'job_1785097178031', jobType: 'simple-map', type: 'migration', name: 'dbo.addresses → dbo.addresses',
    projectId: 'proj_demo', sourceTable: 'dbo.addresses', targetTable: 'dbo.addresses',
    srcWhere: '', columnMapping: addrCols, totalRows: 200,
    created: '2026-07-27T10:00:00Z', lastModified: '2026-07-27T10:00:00Z' },
  { id: 'job_1785146862818', jobType: 'simple-map', type: 'migration', name: 'dbo.Cases → dbo.cases',
    projectId: 'proj_demo', sourceTable: 'dbo.Cases', targetTable: 'dbo.cases',
    srcWhere: "caseno like '%z%'", columnMapping: caseCols, totalRows: 100,
    verifySQL: CASES_VERIFY,
    created: '2026-07-28T10:00:00Z', lastModified: '2026-08-16T09:00:00Z' },
  { id: 'job_dead', jobType: 'simple-map', type: 'migration', _deleted: true,
    sourceTable: 'dbo.zzz', targetTable: 'dbo.zzz', columnMapping: [{ srcCol: 'a', tgtCol: 'a' }] },
];

const PROJECTS = [{
  id: 'proj_demo', name: 'Demo', phase: 'planning',
  dbHistory: [
    { role: 'src', server: 's1', database: 'son_db_hk', firstSeen: '2026-08-18T00:00:00Z', lastSeen: '2026-08-23T13:36:00Z' },
    { role: 'src', server: 's1', database: 'Conversion_DM', firstSeen: '2026-07-26T00:00:00Z', lastSeen: '2026-08-19T00:00:00Z' },
    { role: 'tgt', server: 's1', database: 'hgji9oppmecudpiwmhqjuq', firstSeen: '2026-08-18T00:00:00Z', lastSeen: '2026-08-23T13:36:00Z' },
    { role: 'tgt', server: 'az', database: 'conversion_dm_tgt', firstSeen: '2026-07-26T00:00:00Z', lastSeen: '2026-08-19T00:00:00Z' },
  ],
  groups: [{ id: 'g1', name: 'Wave 1', steps: [
    { type: 'migration', jobId: 'job_1785097178031', name: 'dbo.addresses → dbo.addresses',
      srcTable: 'dbo.addresses', tgtTable: 'dbo.addresses', srcWhere: '', columnMapping: addrCols,
      totalRows: 200, rowsInserted: 200, rowsExcluded: 0, status: 'passed',
      startedAt: '2026-08-14T09:00:00Z', finishedAt: '2026-08-14T09:05:00Z', runId: 'r_addr_1' },
    { type: 'migration', jobId: 'job_1785146862818', name: 'dbo.Cases → dbo.cases',
      srcTable: 'dbo.Cases', tgtTable: 'dbo.cases', srcWhere: '', columnMapping: caseCols,
      totalRows: 100, rowsInserted: 100, rowsExcluded: 0, status: 'passed',
      startedAt: '2026-08-14T09:10:00Z', finishedAt: '2026-08-14T09:15:00Z', runId: 'r_case_1' },
    { type: 'migration', jobId: 'job_1785146862818', name: 'dbo.Cases → dbo.cases',
      srcTable: 'dbo.Cases', tgtTable: 'dbo.cases', srcWhere: "caseno like '%z%'", columnMapping: caseCols,
      totalRows: 100, rowsInserted: 24, rowsExcluded: 76, status: 'passed',
      startedAt: '2026-08-16T09:50:00Z', finishedAt: '2026-08-16T09:57:00Z', runId: 'r_case_2' },
  ] }],
}];

/* the resolver's `global` is module.exports under node — wire its world */
mc.localStorage = {
  getItem: (k) => ({
    cygenix_projects: JSON.stringify(PROJECTS),
    cygenix_jobs: JSON.stringify(JOBS),
    cygenix_current_project: 'proj_stale_x',                 /* the stale-id trap */
  })[k] || null,
};
mc.CygenixSchemaGraph = { activeProjectId: () => 'proj_demo' };

// ── 1. Scope resolution ─────────────────────────────────────────────────────
const scope = MC.resolveMigrationScope();
check('the stale cygenix_current_project is overridden by the graph resolver',
  scope.project.id === 'proj_demo' && scope.project.name === 'Demo');
check('2 mapped tables — soft-deleted jobs excluded',
  scope.mappedTableCount === 2, String(scope.mappedTableCount));
const cases = scope.tables.find(t => t.tgtTable === 'dbo.cases');
const addr = scope.tables.find(t => t.tgtTable === 'dbo.addresses');
check('definition and run resolve separately: newest predicate WITH newest run (24 in, 76 out — never 100 vs 24)',
  cases.predicate === "caseno like '%z%'" && cases.run.inserted === 24 && cases.run.excluded === 76,
  JSON.stringify({ p: cases.predicate, in: cases.run.inserted, ex: cases.run.excluded }));
check('the composite wrapper timestamp never marks a step stale (trap 4.2)',
  cases.definitionNewerThanRun === false);
check('224 rows landed across the map', scope.mappedRowsLanded === 224, String(scope.mappedRowsLanded));
check('hand-written verify SQL attaches to its job — 1 of 2, matching the dashboard',
  scope.jobsWithVerifySQL === 1 && !!cases.verifySQL && !addr.verifySQL);
check('keys, fan-out, literals and unsourced columns all resolve from the mapping',
  cases.key === 'id' && addr.key === 'id'
  && addr.fanOut.length === 1 && addr.fanOut[0].targets.length === 2
  && cases.literals.length === 1 && cases.literals[0].column === 'CaseName'
  && addr.unsourced.join(',') === 'state,phone,email');
check('the connection context distinguishes the mapped pair from the selected pair',
  scope.connections.mappedSrc.database === 'Conversion_DM'
  && scope.connections.selectedSrc.database === 'son_db_hk');

// ── 2. Derived-check specs ──────────────────────────────────────────────────
const specs = MC.deriveChecks(scope);
check('the profiler collapses to the brief\'s 13 checks (+1 for the fixture\'s money column)',
  specs.length === 14, specs.map(s => s.id + ':' + s.check).join(', '));
check('freshness is dormant while the phase is planning',
  specs.filter(s => s.check === 'drift.freshness').every(s => s.state === 'dormant'));
check('this map carries no FK across, so no referential.orphan is fabricated',
  !specs.some(s => s.check === 'referential.orphan'));

// ── 3. Specs → executable rules ─────────────────────────────────────────────
const conv = A.asDraftsFromScope(scope, specs, { now: 1000 });
check('rowsExcluded is an accounted-for figure, not a rule',
  conv.informational.length === 1 && conv.informational[0].excluded === 76
  && /accounted for, not lost/.test(conv.informational[0].proves));
check('the UNION-count verify SQL collapses into the recon pair rather than double-counting',
  !conv.drafts.some(d => d.check === 'custom.sql' && /Verify/.test(d.name))
  && /verify SQL is this same comparison/.test(
      conv.drafts.find(d => d.id.endsWith('/S2') && /cases/.test(d.name)).provenance.origin));
const rc = conv.drafts.find(d => d.check === 'recon.rowcount' && d.binding.targets[0].table === 'Cases');
const rcSrc = A.asCompile(rc, rc.binding.targets[0], 'sqlserver', 10);
const rcTgt = A.asCompile(rc, rc.binding.targets[1], 'sqlserver', 10);
check('the structured srcWhere lands on the SOURCE count only — 24 vs 24',
  rc.params.srcPredicate === "caseno like '%z%'"
  && /WHERE \(caseno like '%z%'\)/.test(rcSrc.countSql) && !/WHERE/.test(rcTgt.countSql));
const litRule = conv.drafts.find(d => /CaseName/.test(d.name));
check('the @@date literal column is excluded from source-vs-target comparison',
  litRule.check === 'completeness.not_null' && /excluded from source-vs-target/.test(litRule.proves));
const fan = conv.drafts.find(d => /Fan-out/.test(d.name));
check('fan-out targets get an agreement check, per dialect',
  /IS DISTINCT FROM/.test(fan.params.sql.postgres) && /CASE WHEN/.test(fan.params.sql.sqlserver));
const nul = conv.drafts.find(d => d.check === 'drift.null_rate');
check('unsourced columns get a stay-empty check across all three, armed (not dormant) by the resolver\'s call',
  nul.binding.targets.length === 3 && nul.thresholds.baseline.enabled === true
  && nul.dormantUntilCutover === false);
const agg = conv.drafts.find(d => d.check === 'recon.column_aggregate');
check('the money column pairs src and tgt columns from the mapping, filter applied',
  agg && agg.binding.targets[0].column === 'fee_total' && agg.params.srcPredicate === "caseno like '%z%'");
const fresh = conv.drafts.filter(d => d.check === 'drift.freshness');
check('freshness derives only where a date-bearing column exists, born dormant',
  fresh.length === 1 && fresh[0].dormantUntilCutover === true
  && conv.skipped.some(s => /freshness/.test(s.why)));
check('every draft fires on job completion except the cutover-dormant freshness',
  conv.drafts.every(d => d.check === 'drift.freshness'
    ? !d.schedule.onJobCompletion : d.schedule.onJobCompletion));
check('ids and provenance name the job step',
  conv.drafts.every(d => /^job_\d+/.test(d.id) && d.provenance.source === 'derived' && d.provenance.jobId));

// ── 4. Sync into the store ──────────────────────────────────────────────────
const store = A.asNewStore(1000);
const s1 = A.asSyncMapRules(store, conv.drafts, 'sync', 2000);
const s2 = A.asSyncMapRules(store, conv.drafts, 'sync', 3000);
check('first sync creates every rule; a second sync with the same map is a no-op',
  s1.created === conv.drafts.length && s2.created === 0 && s2.updated === 0
  && s2.unchanged === conv.drafts.length, JSON.stringify([s1, s2]));
check('derived rules keep their proves and dormancy through asSaveRule',
  store.rules.every(r => r.proves)
  && store.rules.filter(r => r.dormantUntilCutover).length === 1);

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
