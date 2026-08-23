// Tests for the Assurance engine — the deterministic core of the module.
//
// The stakes here are the spec's constraints: rules compile to per-dialect
// SQL rather than storing it, binding resolves by tag at run time, verdicts
// come from thresholds the engine computed, a breach is one incident across
// many runs, and suppression can never be silent or permanent. The page and
// the live SQL round-trip are covered by the browser smoke; this file owns
// the logic.
const A = require('../public/cygenix-assurance.js');
const fs = require('fs');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};
const throws = (fn, re) => {
  try { fn(); return false; } catch (e) { return re ? re.test(String(e.message)) : true; }
};

console.log('Assurance — engine\n');

// ── 1. Dialects ─────────────────────────────────────────────────────────────
check('the dialect is detected the same way db-connect detects it',
  A.asDialectOf('Server=x;Database=y;') === 'sqlserver'
  && A.asDialectOf('postgres://u:p@h/db') === 'postgres'
  && A.asDialectOf('postgresql://h/db') === 'postgres'
  && A.asDialectOf('Driver=Postgres;Host=h') === 'postgres');

const T = { db: 'tgt', schema: 'dbo', table: 'gl_posting', column: 'cost_centre' };
const lookupRule = { check: 'domain.lookup', params: {
  reference: { kind: 'table', ref: 'ref.cost_centre.code' }, normalise: 'trim_upper', nulls: 'ignore' } };

const ms = A.asCompile(lookupRule, T, 'sqlserver', 50);
const pg = A.asCompile(lookupRule, T, 'postgres', 50);
check('sqlserver quotes with brackets and samples with TOP',
  /\[dbo\]\.\[gl_posting\]/.test(ms.countSql) && /^SELECT TOP 50 /.test(ms.sampleSql), ms.sampleSql);
check('postgres quotes with doubles and samples with LIMIT',
  /"dbo"\."gl_posting"/.test(pg.countSql) && / LIMIT 50$/.test(pg.sampleSql), pg.sampleSql);
check('both count first — sample SQL is separate, fetched only on a hit',
  /^SELECT COUNT\(\*\) AS n/.test(ms.countSql) && /^SELECT COUNT\(\*\) AS n/.test(pg.countSql));
check('trim_upper normalises both sides of the lookup',
  /UPPER\(LTRIM\(RTRIM\(\[cost_centre\]\)\)\)/.test(ms.countSql)
  && /UPPER\(BTRIM\("cost_centre"\)\)/.test(pg.countSql), ms.countSql);
check('nulls:ignore keeps NULL out of the failure set',
  /\[cost_centre\] IS NOT NULL AND/.test(ms.countSql));
check('nulls:fail counts NULL as a failure',
  /\[cost_centre\] IS NULL OR/.test(A.asCompile({ check: 'domain.lookup', params: {
    reference: { kind: 'inline', values: ['A'] }, nulls: 'fail' } }, T, 'sqlserver').countSql));
check('an inline reference normalises its values at compile time',
  /'GBP'/.test(A.asCompile({ check: 'domain.enum', params: { values: [' gbp '], normalise: 'trim_upper' } },
    T, 'sqlserver').countSql));

// Every check that compiles must compile on BOTH dialects (regex excepted).
const BOTH = {
  'completeness.not_null': {}, 'completeness.blank_string': {},
  'completeness.required_if': { predicate: "status = 'POSTED'" },
  'domain.lookup': lookupRule.params,
  'domain.enum': { values: ['GBP', 'USD'] },
  'domain.range': { min: 0, max: 100 },
  'domain.length': { max: 35 },
  'domain.date_sanity': { futureToleranceDays: 1 },
  'format.pattern': { like: '[A-Z][A-Z]%' },
  'format.max_length': { declaredMax: 50 },
  'format.control_chars': {}, 'format.whitespace': {}, 'format.case_consistency': {},
  'uniqueness.key': {}, 'uniqueness.composite': { columns: ['a', 'b'] },
  'uniqueness.fuzzy': { blockingKey: 'postcode', column: 'name' },
  'referential.orphan': { parent: { schema: 'ref', table: 'cost_centre', column: 'code' } },
  'referential.childless_parent': { child: { table: 'invoice_line', column: 'invoice_id' } },
  'aggregate.balance': { amount: 'amount', groupBy: ['journal_id'] },
  'aggregate.parent_child': { key: 'id', headerTotal: 'total', child: { table: 'line', key: 'invoice_id', amount: 'amount' } },
  'aggregate.tolerance': { expression: 'qty * unit_price', epsilon: 0.01 },
  'drift.rowcount': {}, 'drift.freshness': {}, 'drift.null_rate': {},
  'recon.rowcount': {}, 'recon.column_aggregate': { fn: 'sum' },
  'custom.sql': { sql: { any: 'SELECT * FROM x WHERE broken = 1' } },
};
let compiled = 0, failedCompiles = [];
Object.keys(BOTH).forEach(k => {
  ['sqlserver', 'postgres'].forEach(dl => {
    try { A.asCompile({ check: k, params: BOTH[k] }, T, dl, 10); compiled++; }
    catch (e) { failedCompiles.push(k + '@' + dl + ': ' + e.message); }
  });
});
check('every shipped check compiles on both dialects (' + Object.keys(BOTH).length + ' × 2)',
  failedCompiles.length === 0, failedCompiles.join(' | '));
check('the registry lists them all', A.asChecks().length >= Object.keys(BOTH).length + 1);

// Explicit adapter differences.
check('freshness uses DATEDIFF on sqlserver and EPOCH arithmetic on postgres',
  /DATEDIFF\(hour/.test(A.asCompile({ check: 'drift.freshness', params: {} }, T, 'sqlserver').countSql)
  && /EXTRACT\(EPOCH/.test(A.asCompile({ check: 'drift.freshness', params: {} }, T, 'postgres').countSql));
check('regex compiles on postgres and refuses on sqlserver with a way forward',
  /~ '\^\[A-Z\]'/.test(A.asCompile({ check: 'domain.regex', params: { pattern: '^[A-Z]' } }, T, 'postgres').countSql)
  && throws(() => A.asCompile({ check: 'domain.regex', params: { pattern: '^[A-Z]' } }, T, 'sqlserver'),
      /format\.pattern|enum/));
check('a bracket inside an identifier cannot break out',
  /\[odd\]\]name\]/.test(A.asCompile({ check: 'completeness.not_null', params: {} },
    { db: 'tgt', schema: 'dbo', table: 'x', column: 'odd]name' }, 'sqlserver').countSql));
check('a quote inside a literal cannot break out',
  /'O''Brien'/.test(A.asCompile({ check: 'domain.enum', params: { values: ["O'Brien"] } },
    T, 'sqlserver').countSql));

// The guard rails.
check('an unblocked fuzzy match is refused — O(n²) never reaches the database',
  throws(() => A.asCompile({ check: 'uniqueness.fuzzy', params: { column: 'name' } }, T, 'sqlserver'),
    /blockingKey/));
check('custom.sql must be a SELECT',
  throws(() => A.asCompile({ check: 'custom.sql', params: { sql: { any: 'DELETE FROM x' } } }, T, 'sqlserver'),
    /SELECT/));
check('uniqueness counts excess rows, not duplicate groups',
  /SUM\(dup_n - 1\)/.test(A.asCompile({ check: 'uniqueness.composite', params: { columns: ['k'] } },
    T, 'sqlserver').countSql));
check('drift checks are measures — no sample query to run',
  A.asCompile({ check: 'drift.rowcount', params: {} }, T, 'sqlserver').sampleSql === null
  && A.asCompile({ check: 'drift.rowcount', params: {} }, T, 'sqlserver').kind === 'measure');

// ── 2. Tags ─────────────────────────────────────────────────────────────────
const col = (column, dataType, extra) => Object.assign({
  db: 'tgt', schema: 'dbo', table: 'invoice_line', column, dataType, siblings: [] }, extra || {});
const props = A.asProposeTags([
  col('amount_base', 'decimal(19,4)'),
  col('amount', 'decimal(19,4)', { siblings: ['currency_code'] }),
  col('currency_code', 'varchar(3)'),
  col('created_at', 'datetime2'),
  col('email', 'nvarchar(200)'),
  col('postcode', 'varchar(10)'),
  col('invoice_id', 'int', { isPk: true }),
  col('cost_centre', 'varchar(12)'),
  col('real_estate', 'varchar(50)'),
], ['cost_centre']);
const tagOf = (c) => props.filter(p => p.column === c).map(p => p.tag);
check('a _base amount is money:base', tagOf('amount_base').includes('money:base'));
check('an amount with a currency sibling is money, more confidently',
  tagOf('amount').includes('money')
  && props.find(p => p.column === 'amount' && p.tag === 'money').confidence > 0.9);
check('a 3-char currency column is currency', tagOf('currency_code').includes('currency'));
check('created_at is an audit date, not an event date', tagOf('created_at').includes('date:audit'));
check('email and postcode are PII',
  tagOf('email').includes('pii:email') && tagOf('postcode').includes('pii:postcode'));
check('a column matching a reference table proposes lookup:*',
  tagOf('cost_centre').includes('lookup:cost_centre'));
check('every proposal explains itself and marks itself auto',
  props.every(p => p.rationale && p.source === 'auto' && p.confidence > 0 && p.confidence <= 1));

// ── 3. Binding resolution ───────────────────────────────────────────────────
const CATALOG = {
  tables: [
    { db: 'tgt', schema: 'dbo', table: 'gl_posting', rowCount: 100, columns: ['id', 'cost_centre', 'amount'] },
    { db: 'tgt', schema: 'dbo', table: 'invoice', rowCount: 50, columns: ['id', 'currency_code'] },
    { db: 'src', schema: 'dbo', table: 'legacy_gl', rowCount: 100, columns: ['id', 'cost_centre'] },
  ],
  tags: [
    { db: 'tgt', schema: 'dbo', table: 'gl_posting', column: 'cost_centre', tag: 'lookup:cost_centre' },
    { db: 'src', schema: 'dbo', table: 'legacy_gl', column: 'cost_centre', tag: 'lookup:cost_centre' },
  ],
};
const byTag = A.asResolveBinding({ binding: { mode: 'tag', tag: 'lookup:cost_centre', databases: [] } }, CATALOG);
check('tag binding finds every tagged column across databases', byTag.length === 2);
check('the database scope limiter narrows it',
  A.asResolveBinding({ binding: { mode: 'tag', tag: 'lookup:cost_centre', databases: ['tgt'] } }, CATALOG).length === 1);
const byPattern = A.asResolveBinding({ binding: { mode: 'pattern', pattern: '*.dbo.*.cost_centre' } }, CATALOG);
check('pattern binding globs over db.schema.table.column', byPattern.length === 2,
  JSON.stringify(byPattern));
check('a three-segment pattern binds tables, not columns',
  A.asResolveBinding({ binding: { mode: 'pattern', pattern: 'tgt.dbo.*' } }, CATALOG)
    .filter(t => t.column === null).length === 2);
check('explicit binding passes targets through',
  A.asResolveBinding({ binding: { mode: 'explicit', targets: [{ db: 'tgt', schema: 'dbo', table: 'x', column: 'y' }] } },
    CATALOG).length === 1);
check('a tag nothing carries resolves to zero targets — which the run flags',
  A.asResolveBinding({ binding: { mode: 'tag', tag: 'lookup:nothing' } }, CATALOG).length === 0);

// ── 4. Thresholds and baselines ─────────────────────────────────────────────
const ruleTh = { thresholds: { warnAbove: { unit: 'rows', value: 0 }, failAbove: { unit: 'rows', value: 100 } } };
check('rows thresholds grade warn then fail',
  A.asEvaluate(ruleTh, { rowsFailed: 0, rowsScanned: 1000 }).status === 'pass'
  && A.asEvaluate(ruleTh, { rowsFailed: 5, rowsScanned: 1000 }).status === 'warn'
  && A.asEvaluate(ruleTh, { rowsFailed: 500, rowsScanned: 1000 }).status === 'fail');
const rulePct = { thresholds: { warnAbove: { unit: 'percent', value: 1 }, failAbove: { unit: 'percent', value: 5 } } };
check('percent thresholds use scanned rows as the base',
  A.asEvaluate(rulePct, { rowsFailed: 20, rowsScanned: 1000 }).status === 'warn'
  && A.asEvaluate(rulePct, { rowsFailed: 60, rowsScanned: 1000 }).status === 'fail');
const ruleBase = { thresholds: { warnAbove: { unit: 'rows', value: 1e9 }, failAbove: { unit: 'rows', value: 1e9 },
  baseline: { enabled: true, window: '7d', worseByPercent: 10 } } };
check('a value inside the baseline band passes',
  A.asEvaluate(ruleBase, { rowsFailed: 0, rowsScanned: 0, measure: 104 }, [100, 100, 100]).status === 'pass');
check('a value outside the band warns and says by how much',
  A.asEvaluate(ruleBase, { rowsFailed: 0, rowsScanned: 0, measure: 130 }, [100, 100, 100]).status === 'warn'
  && /\+30\.0% vs the 7d baseline/.test(A.asEvaluate(ruleBase, { rowsFailed: 0, rowsScanned: 0, measure: 130 },
      [100, 100, 100]).why));
check('a drop breaches the band too — losing rows is drift as much as gaining them',
  A.asEvaluate(ruleBase, { rowsFailed: 0, rowsScanned: 0, measure: 70 }, [100, 100, 100]).status === 'warn');
check('freshness fails on age, not rows',
  A.asEvaluateMeasure({ check: 'drift.freshness', params: { maxAgeHours: 26 } }, 38).status === 'fail'
  && A.asEvaluateMeasure({ check: 'drift.freshness', params: { maxAgeHours: 26 } }, 3).status === 'pass');
check('reconciliation compares the two sides within a tolerance',
  A.asEvaluatePair({ params: { tolerance: 0 } }, 100, 100).status === 'pass'
  && A.asEvaluatePair({ params: { tolerance: 0 } }, 100, 99).status === 'fail'
  && A.asEvaluatePair({ params: { tolerance: 2 } }, 100, 99).status === 'pass');

// ── 5. Rules are versioned, never hard-deleted ──────────────────────────────
const now = 1000000000000;
const store = A.asNewStore(now);
const r1 = A.asSaveRule(store, { name: 'Cost centre in reference set', category: 'setups',
  check: 'domain.lookup', params: lookupRule.params,
  binding: { mode: 'tag', tag: 'lookup:cost_centre', databases: [] }, severity: 'critical' }, 'user1', now);
check('a new rule gets a category-prefixed id', /^SET-\d{3}$/.test(r1.id), r1.id);
check('continuous defaults to TRUE — the retention flag', r1.continuous === true);
check('it lands in a matching default group', r1.schedule.groupId === 'grp_nightly_close');
A.asSaveRule(store, { id: r1.id, severity: 'warning' }, 'user2', now + 1);
check('editing bumps the version and keeps the old one',
  store.rules[0].version === 2 && store.ruleVersions.length === 1
  && store.ruleVersions[0].severity === 'critical');
A.asDisableRule(store, r1.id, 'user2', now + 2);
check('disabling versions too — nothing is hard-deleted',
  store.rules.length === 1 && store.rules[0].enabled === false && store.ruleVersions.length === 2);
check('every mutation wrote an audit event',
  store.events.filter(e => /^rule\./.test(e.type)).length === 3);
store.rules[0].enabled = true;

// ── 6. The breach lifecycle ─────────────────────────────────────────────────
const run = (id, status, rows, at) => ({ id, ruleId: r1.id, startedAt: at, status,
  rowsFailed: rows, rowsScanned: 1000, targetsMatched: 2, severity: 'critical', category: 'setups' });
A.asRecordRun(store, run('run1', 'fail', 1204, now + 10), now + 10);
check('the first fail opens a breach', store.breaches.length === 1
  && store.breaches[0].state === 'open' && store.breaches[0].peakRowsFailed === 1204);
A.asRecordRun(store, run('run2', 'fail', 900, now + 20), now + 20);
check('a second failing run continues the SAME incident',
  store.breaches.length === 1 && store.breaches[0].runCount === 2
  && store.breaches[0].currentRowsFailed === 900 && store.breaches[0].peakRowsFailed === 1204);
A.asAcknowledge(store, store.breaches[0].id, 'user1', now + 30);
check('acknowledging records who', store.breaches[0].state === 'acknowledged'
  && store.breaches[0].assignee === 'user1');
A.asRecordRun(store, run('run3', 'pass', 0, now + 40), now + 40);
check('a passing run resolves it', store.breaches[0].state === 'resolved'
  && store.breaches[0].resolvedAt === now + 40);
// Flap guard: failing again 5 minutes later reopens the same incident.
A.asRecordRun(store, run('run4', 'fail', 50, now + 40 + 5 * 60000), now + 40 + 5 * 60000);
check('a fail inside the flap window reopens rather than opening a twin',
  store.breaches.length === 1 && store.breaches[0].state === 'open'
  && store.breaches[0].reopened === 1);
A.asResolve(store, store.breaches[0].id, 'user1', 'reference set updated', now + 3600000);
// After the flap window a new fail is a NEW incident.
A.asRecordRun(store, run('run5', 'fail', 10, now + 3600000 + 31 * 60000), now + 3600000 + 31 * 60000);
check('a fail after the flap window opens a fresh breach', store.breaches.length === 2);
check('the breach trail is in the audit events',
  store.events.some(e => e.type === 'breach.opened')
  && store.events.some(e => e.type === 'breach.reopened')
  && store.events.some(e => e.type === 'breach.resolved'));
// ── 7. Suppression can never be silent or permanent ─────────────────────────
check('no reason, no suppression',
  throws(() => A.asSuppress(store, r1.id, { expiresAt: now + 86400000 }, 'u', now), /reason/i));
check('no expiry, no suppression',
  throws(() => A.asSuppress(store, r1.id, { reason: 'maintenance' }, 'u', now), /expiry/i));
check('a past expiry is refused too',
  throws(() => A.asSuppress(store, r1.id, { reason: 'x', expiresAt: now - 1 }, 'u', now), /expiry/i));
A.asSuppress(store, r1.id, { reason: 'planned maintenance', expiresAt: now + 7200000 + 86400000 }, 'user1', now + 7200000);
check('a valid suppression marks the open breach and logs reason + expiry',
  store.breaches[1].state === 'suppressed'
  && store.events.some(e => e.type === 'breach.suppressed' && e.reason === 'planned maintenance'));
A.asRecordRun(store, run('run7', 'pass', 0, now + 7200000 + 120000), now + 7200000 + 120000);
A.asRecordRun(store, run('run8', 'fail', 10, now + 7200000 + 86400000 * 2), now + 7200000 + 86400000 * 2);
check('a fail after the suppression expires opens an UNsuppressed breach',
  store.breaches[store.breaches.length - 1].state === 'open');
check('a run that matches no targets writes its own warning event',
  (A.asRecordRun(store, Object.assign(run('run9', 'pass', 0, now + 7200000 + 86400000 * 2 + 1),
     { targetsMatched: 0 }), now + 7200000 + 86400000 * 2 + 1),
   store.events.some(e => e.type === 'run.no_targets')));

// ── 8. Diagnosis prose from the six signals ─────────────────────────────────
const prose = A.asDiagnoseProse({
  newValues: ['CC-8840', 'CC-8841', 'CC-8842'],
  insertWindow: { from: '23:40', to: '23:44' },
  writtenBy: 'svc_finance_etl',
  schemaChanges: [],
  referenceAgeDays: 41,
  correlatedBreaches: [],
});
check('the diagnosis is prose, not JSON',
  /3 values appeared/.test(prose) && /CC-8840/.test(prose) && !/[{}\[]/.test(prose), prose);
check('it reads the old-reference/new-codes pattern as a rollout, not corruption',
  /rollout that never reached the lookup/.test(prose));
check('it says when no other rule broke alongside', /No other rule/.test(prose));
check('and degrades honestly with nothing to say',
  /No diagnostic signals/.test(A.asDiagnoseProse({})));

// ── 9. Suggestions, and dismissals that stick ───────────────────────────────
const sugStore = A.asNewStore(now);
sugStore.tags.push({ db: 'tgt', schema: 'dbo', table: 'gl_posting', column: 'cost_centre',
  tag: 'lookup:cost_centre', referenceRef: 'ref.cost_centre.code' });
const sugCatalog = { tables: [
  { db: 'tgt', schema: 'dbo', table: 'fact_txn', rowCount: 1000, columns: ['id', 'load_ts'], primaryKeys: ['id'] },
  { db: 'tgt', schema: 'dbo', table: 'empty_tbl', rowCount: 0, columns: ['updated_at'] },
] };
const sugs = A.asSuggest(sugStore, sugCatalog, now);
check('an audit-date column proposes a freshness rule',
  sugs.some(s => s.check === 'drift.freshness' && /fact_txn/.test(s.title)));
check('a declared key proposes a uniqueness rule',
  sugs.some(s => s.check === 'uniqueness.composite'));
check('an untended lookup tag proposes a lookup rule bound to the TAG',
  sugs.some(s => s.check === 'domain.lookup' && s.draft.binding.mode === 'tag'));
check('an empty table earns no freshness rule', !sugs.some(s => /empty_tbl/.test(s.title)));
check('every suggestion carries a ready-to-review draft rule',
  sugs.every(s => s.draft && s.draft.check && s.draft.binding));
const freshSug = sugStore.suggestions.find(s => s.check === 'drift.freshness');
A.asDismissSuggestion(sugStore, freshSug.id, 'user1', now + 1);
const again = A.asSuggest(sugStore, sugCatalog, now + 86400000);
check('a dismissed suggestion never comes back',
  !again.some(s => s.key === freshSug.key));

// ── 10. Health and the report ───────────────────────────────────────────────
const hStore = A.asNewStore(now);
const hr = A.asSaveRule(hStore, { name: 'r', category: 'finance', check: 'completeness.not_null' }, 'u', now);
for (let i = 0; i < 8; i++) {
  A.asRecordRun(hStore, { id: 'h' + i, ruleId: hr.id, startedAt: now + i * 3600000,
    status: i === 3 ? 'fail' : (i === 5 ? 'warn' : 'pass'),
    rowsFailed: i === 3 ? 10 : 0, rowsScanned: 100, targetsMatched: 1,
    severity: 'critical', category: 'finance' }, now + i * 3600000);
}
A.asResolve(hStore, hStore.breaches[0].id, 'u', 'fixed', now + 4 * 3600000);
const health = A.asHealth(hStore, 30 * 86400000, now + 9 * 3600000);
check('health is the graded pass rate — warn counts half',
  health.score === Math.round(1000 * (6 + 0.5) / 8) / 10, String(health.score));
check('per-category scores come back keyed by category',
  health.perCategory.finance === health.score);
check('mean time to fix comes from resolved breaches',
  health.meanTimeToFixMs === 3600000, String(health.meanTimeToFixMs));
check('the health series gives one value per day, null where nothing ran',
  A.asHealthSeries(hStore, 3, now + 9 * 3600000).length === 3);

const report = A.asMonthlyReport(hStore, now - 86400000, now + 10 * 3600000);
check('the report aggregates the month — the warn opened its own incident',
  report.checksRun === 8 && report.breachesCaught === 2
  && report.breachesResolved === 2 && report.rulesAdded === 1,
  JSON.stringify([report.checksRun, report.breachesCaught, report.breachesResolved, report.rulesAdded]));
check('and narrates each catch in plain terms',
  report.narrative.length === 2 && /resolved in/.test(report.narrative[0]), report.narrative[0]);

// ── 11. Cron ────────────────────────────────────────────────────────────────
const base = Date.UTC(2026, 7, 24, 3, 30);            // Mon 24 Aug 2026 03:30 UTC
check('0 4 * * * fires at the next 04:00',
  A.asCronNext('0 4 * * *', base) === Date.UTC(2026, 7, 24, 4, 0));
check('0 * * * * fires at the top of the next hour',
  A.asCronNext('0 * * * *', base) === Date.UTC(2026, 7, 24, 4, 0));
check('0 */6 * * * fires at the next six-hour mark',
  A.asCronNext('0 */6 * * *', base) === Date.UTC(2026, 7, 24, 6, 0));
check('0 5 * * 0 fires on Sunday',
  new Date(A.asCronNext('0 5 * * 0', base)).getUTCDay() === 0);
check('a group is due when its next fire time has passed',
  A.asCronDue('0 4 * * *', Date.UTC(2026, 7, 23, 4, 0), Date.UTC(2026, 7, 24, 4, 1)) === true
  && A.asCronDue('0 4 * * *', Date.UTC(2026, 7, 24, 4, 0), Date.UTC(2026, 7, 24, 5, 0)) === false);

// ── 12. Wiring ──────────────────────────────────────────────────────────────
const PAGE = fs.existsSync(__dirname + '/../public/assurance.html')
  ? fs.readFileSync(__dirname + '/../public/assurance.html', 'utf8') : '';
const NAV = fs.readFileSync(__dirname + '/../public/cygenix-sidebar.js', 'utf8');
check('the Assurance page exists and loads the engine',
  /<script src="\/cygenix-assurance\.js[^"]*" defer><\/script>/.test(PAGE));
check('it mounts the sidebar under the assurance key',
  /data-active="assurance"/.test(PAGE));
check('the sidebar offers Assurance first in the Data Quality group',
  /key:'assurance'/.test(NAV)
  && NAV.indexOf("key:'assurance'") < NAV.indexOf("key:'data-quality'"));
check('the six views are all present',
  ['as-v-health', 'as-v-rules', 'as-v-tags', 'as-v-breaches', 'as-v-sched', 'as-v-report']
    .every(id => PAGE.includes(id)),
  ['as-v-health', 'as-v-rules', 'as-v-tags', 'as-v-breaches', 'as-v-sched', 'as-v-report']
    .filter(id => !PAGE.includes(id)).join(','));
check('the rule builder is a drawer with a dry-run gate before save',
  /as-drawer/.test(PAGE) && /asDryRun/.test(PAGE));
check('the generated SQL is logged with the run so a DBA can reproduce it',
  /countSql/.test(PAGE) && /generatedSql/.test(PAGE));
check('breach hand-off goes to the existing Cleansing module',
  /data-cleansing\.html/.test(PAGE));
check('promotion imports from the existing Validation module, read-only',
  /cygenix_validation_sources/.test(PAGE));
check('suppression in the page asks for reason and expiry',
  /asSuppress/.test(PAGE) && /reason/i.test(PAGE));
check('notification routing goes through the existing integrations emit',
  /CygenixIntegrations/.test(PAGE) && /emit\(/.test(PAGE));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
