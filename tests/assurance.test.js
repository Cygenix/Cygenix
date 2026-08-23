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

// ── 12. sha256 and the run record (redesign §8) ────────────────────────────
check('sha256 matches the FIPS vector for "abc"',
  A.sha256('abc') === 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
check('and for the empty string',
  A.sha256('') === 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855');

const mkRun = (id, status, at) => ({ id, ruleId: 'SET-001', ruleVersion: 3, startedAt: at,
  durationMs: 1500, status, rowsFailed: status === 'fail' ? 7 : 0, rowsScanned: 500,
  targetsMatched: 1, severity: 'warning', category: 'setups',
  perTarget: [{ target: 'tgt.dbo.gl_posting', generatedSql: 'SELECT COUNT(*) AS n FROM x',
    sampleRows: [{ id: 1 }], rowsFailed: status === 'fail' ? 7 : 0 }] });
const recStore = A.asNewStore(now);
A.asSaveRule(recStore, { name: 'rec', category: 'setups', check: 'completeness.not_null' }, 'u', now);
recStore.rules[0].id = 'SET-001';
const rec1 = A.asRecordRun(store0 = recStore, mkRun('run_a1', 'fail', now), now) && recStore.runs[0];
check('recording a run finalizes it: ISO timestamps, result, counts, query text, connections',
  rec1.startedAtIso === new Date(now).toISOString()
  && rec1.finishedAtIso === new Date(now + 1500).toISOString()
  && rec1.result === 'breach'
  && rec1.counts.compared === 500 && rec1.counts.matched === 493 && rec1.counts.failed === 7
  && rec1.queryText === 'SELECT COUNT(*) AS n FROM x'
  && rec1.connectionIds.length === 1 && rec1.connectionIds[0] === 'tgt',
  JSON.stringify(rec1.counts));
check('pass maps to pass and error to error — the record vocabulary is fixed',
  A.asFinalizeRun(mkRun('x1', 'pass', now)).result === 'pass'
  && A.asFinalizeRun(mkRun('x2', 'error', now)).result === 'error');
check('the run hash is deterministic',
  A.asFinalizeRun(mkRun('run_a1', 'fail', now)).runHash === rec1.runHash);
check('and covers the substance — a different query is a different hash',
  (() => { const r = mkRun('run_a1', 'fail', now);
    r.perTarget[0].generatedSql = 'SELECT COUNT(*) AS n FROM y';
    return A.asFinalizeRun(r).runHash !== rec1.runHash; })());
check('but deliberately EXCLUDES sample rows, so ageing them keeps the record valid',
  (() => { const r = mkRun('run_a1', 'fail', now); delete r.perTarget[0].sampleRows;
    return A.asFinalizeRun(r).runHash === rec1.runHash; })());
A.asRecordRun(recStore, mkRun('run_a2', 'pass', now + 60000), now + 60000);
const man = A.asManifest(recStore.runs);
check('a sweep manifest hashes the sorted run hashes',
  man.count === 2 && man.hash === A.sha256([recStore.runs[0].runHash, recStore.runs[1].runHash]
    .sort().join('\n')));
const agedN = A.asAgeSamples(recStore, 30, now + 40 * 86400000);
check('sample ageing strips old samples, stamps when, and writes an audit event',
  agedN === 2 && !recStore.runs[0].perTarget[0].sampleRows
  && recStore.runs[0].perTarget[0].samplesAgedAt === now + 40 * 86400000
  && recStore.events.some(e => e.type === 'runs.samples_aged'));
check('and the aged run still verifies against its original hash',
  recStore.runs[0].runHash === rec1.runHash);

// ── 13. The status object and its state machine (redesign §2) ──────────────
const stEmpty = A.asNewStore(now);
check('no rules → no-checks', A.asStatus(stEmpty, { connections: 2 }).state === 'no-checks');
A.asSaveRule(stEmpty, { name: 'n', category: 'finance', check: 'completeness.not_null' }, 'u', now);
const stNever = A.asStatus(stEmpty, { connections: 2 });
check('rules but no runs → never-run, and the counts are there',
  stNever.state === 'never-run' && stNever.checksWritten === 1 && stNever.checksRun === 0
  && stNever.lastRunAt === null && stNever.connections === 2);
A.asRecordRun(stEmpty, { id: 'sr1', ruleId: stEmpty.rules[0].id, startedAt: now + 1, status: 'fail',
  rowsFailed: 3, rowsScanned: 10, targetsMatched: 1, severity: 'warning', category: 'finance' }, now + 1);
check('an open breach → breaches', A.asStatus(stEmpty, {}).state === 'breaches'
  && A.asStatus(stEmpty, {}).openBreaches === 1);
A.asRecordRun(stEmpty, { id: 'sr2', ruleId: stEmpty.rules[0].id, startedAt: now + 2, status: 'pass',
  rowsFailed: 0, rowsScanned: 10, targetsMatched: 1, severity: 'warning', category: 'finance' }, now + 2);
const stPass = A.asStatus(stEmpty, { connections: 2 });
check('all clear → passing, with the last run time',
  stPass.state === 'passing' && stPass.lastRunAt === now + 2 && stPass.checksRun === 1);
check('a skipped run does not count as having run',
  (() => { const s = A.asNewStore(now);
    A.asSaveRule(s, { name: 'n', category: 'finance', check: 'completeness.not_null' }, 'u', now);
    s.runs.push({ id: 'k', ruleId: s.rules[0].id, startedAt: now, status: 'skipped' });
    return A.asStatus(s, {}).checksRun === 0 && A.asStatus(s, {}).state === 'never-run'; })());

// ── 14. The ribbon derives from the status object, no new state (§4) ────────
const rb = A.asRibbon(A.asStatus(stEmpty, { connections: 2 }));
check('four steps, keyed and labelled', rb.length === 4
  && rb.map(s => s.key).join(',') === 'connect,write,run,publish');
check('a passing estate stands at Publish',
  rb[0].state === 'done' && rb[1].state === 'done' && rb[2].state === 'done'
  && rb[3].state === 'now' && rb[3].note === 'Ready');
check('no connections puts you at step one',
  A.asRibbon(A.asStatus(A.asNewStore(now), { connections: 0 }))[0].state === 'now');
const rbNever = A.asRibbon(stNever);
check('written-but-never-run stands at Run them, marked "You are here"',
  rbNever[2].state === 'now' && rbNever[2].note === 'You are here'
  && rbNever[3].note === 'Needs one clean run');

// ── 15. proves templates and human schedules (§7) ───────────────────────────
const provesOf = (check_, params, targets) => A.asProvesFor({ check: check_, params: params || {},
  binding: { mode: 'explicit', targets: targets || [{ db: 'tgt', schema: 'dbo', table: 'client', column: 'customer_num' }] } });
check('uniqueness proves one row per key',
  /client keeps exactly one row per customer_num/.test(provesOf('uniqueness.composite', { columns: ['customer_num'] })));
check('recon.rowcount proves the landed count',
  /The same number of rows landed in the target as left the source for client\./
    .test(provesOf('recon.rowcount')));
check('referential.orphan names the parent',
  /still points at a cost_centre row that exists/.test(
    provesOf('referential.orphan', { parent: { table: 'cost_centre' } })));
check('freshness proves arrival within the window',
  /within 60 hours/.test(provesOf('drift.freshness', { maxAgeHours: 60 })));
check('custom.sql has no template — the author must write the sentence',
  provesOf('custom.sql', { sql: { any: 'SELECT 1' } }) === null);
check('a saved rule with a template gets proves filled in automatically',
  (() => { const s = A.asNewStore(now);
    const r = A.asSaveRule(s, { name: 'u', category: 'duplicates', check: 'uniqueness.composite',
      params: { columns: ['id'] }, binding: { mode: 'explicit',
        targets: [{ db: 'tgt', schema: 'dbo', table: 'client', column: null }], databases: [] } }, 'u', now);
    return /client keeps exactly one row per id/.test(r.proves); })());
check('cron renders as a human schedule, never shown raw in the table',
  A.asCronHuman('0 * * * *') === 'Hourly' && A.asCronHuman('0 */6 * * *') === 'Every 6 hours'
  && A.asCronHuman('0 4 * * *') === 'Daily 04:00' && A.asCronHuman('0 5 * * 0') === 'Weekly, Sunday 05:00');

// ── 16. Coverage of the migration (§5) ──────────────────────────────────────
const covCatalog = { tables: [
  { db: 'tgt', schema: 'fin', table: 'frx_period_bal', rowCount: 500, columns: ['id', 'amount'] },
  { db: 'tgt', schema: 'fin', table: 'frx_tran_detail', rowCount: 20000, columns: ['id', 'note'] },
  { db: 'tgt', schema: 'ref', table: 'country', rowCount: 200, columns: ['code'], referencedBy: 3 },
  { db: 'tgt', schema: 'ref', table: 'colour', rowCount: 12, columns: ['code'] },
  { db: 'src', schema: 'fin', table: 'frx_period_bal', rowCount: 500, columns: ['id', 'amount'] },
], tags: [] };
const covStore = A.asNewStore(now);
A.asSaveRule(covStore, { name: 'c', category: 'finance', check: 'aggregate.balance',
  params: { amount: 'amount' }, binding: { mode: 'explicit',
    targets: [{ db: 'tgt', schema: 'fin', table: 'frx_period_bal', column: null }], databases: [] } }, 'u', now);
const cov = A.asCoverage(covStore, covCatalog, { db: 'tgt', labelFn: t => t.schema === 'fin' ? 'Finance' : 'Reference' });
check('coverage sorts scope into checked / material-unchecked / low-risk',
  cov.inScope === 4 && cov.checked.length === 1
  && cov.materialUnchecked.length === 2 && cov.lowRisk.length === 1,
  JSON.stringify([cov.inScope, cov.checked.length, cov.materialUnchecked.length, cov.lowRisk.length]));
check('material means money, referenced, or big',
  cov.materialUnchecked.some(t => t.table === 'frx_tran_detail')      /* big */
  && cov.materialUnchecked.some(t => t.table === 'country')           /* referenced */
  && cov.lowRisk.some(t => t.table === 'colour'));
check('the gaps come back grouped and exampled for the prose line',
  cov.gaps.length === 2 && cov.gaps.every(g => g.label && g.tables > 0 && g.examples.length > 0));
check('the size threshold is configurable',
  A.asCoverage(covStore, covCatalog, { db: 'tgt', materialRows: 50000 })
    .materialUnchecked.every(t => t.table !== 'frx_tran_detail'));

// ── 17. Suggestion bundles (§6) ─────────────────────────────────────────────
const bunCatalog = { tables: [
  { db: 'tgt', schema: 'dbo', table: 'client', rowCount: 900, columns: ['customer_num', 'name'],
    primaryKeys: ['customer_num'] },
  { db: 'tgt', schema: 'dbo', table: 'invoice', rowCount: 5000,
    columns: ['id', 'customer_num', 'total_amount'], primaryKeys: ['id'],
    fks: [{ column: 'customer_num', refSchema: 'dbo', refTable: 'client', refColumn: 'customer_num' }] },
  { db: 'src', schema: 'dbo', table: 'client', rowCount: 900, columns: ['customer_num', 'name'] },
  { db: 'src', schema: 'dbo', table: 'invoice', rowCount: 5000, columns: ['id', 'customer_num', 'total_amount'] },
], tags: [] };
const bunStore = A.asNewStore(now);
const bun = A.asSuggestBundles(bunStore, bunCatalog);
const bunKeys = bun.bundles.map(b => b.key);
check('the profiler output groups into the four template bundles',
  bunKeys.includes('keys-unique') && bunKeys.includes('rowcounts-match')
  && bunKeys.includes('references-resolve') && bunKeys.includes('money-agrees'),
  bunKeys.join(','));
check('the total is the sum of the bundle counts',
  bun.total === bun.bundles.reduce((a, b) => a + b.count, 0));
check('every bundle carries plain English, an estimate, and ready drafts with proves pre-filled',
  bun.bundles.every(b => b.plain && b.estMsPerSweep > 0
    && b.items.length === b.count && b.items.every(d => d.proves && d.check && d.binding)));
check('recon drafts bind BOTH sides explicitly',
  bun.bundles.find(b => b.key === 'rowcounts-match').items
    .every(d => d.binding.targets.length === 2
      && d.binding.targets[0].db === 'src' && d.binding.targets[1].db === 'tgt'));
check('the money bundle found the money column',
  bun.bundles.find(b => b.key === 'money-agrees').items
    .some(d => d.binding.targets[0].column === 'total_amount'));
const addedRules = A.asAcceptBundle(bunStore, bun.bundles.find(b => b.key === 'keys-unique'), 'user1', now);
check('accepting a bundle saves every draft and logs one event',
  addedRules.length === 2 && bunStore.rules.length === 2
  && bunStore.events.some(e => e.type === 'bundle.accepted' && e.rules === 2));
const bunAfter = A.asSuggestBundles(bunStore, bunCatalog);
check('covered tables drop out of the bundle on the next pass',
  !bunAfter.bundles.some(b => b.key === 'keys-unique'));
bunStore.dismissals['bundle|references-resolve|tgt.dbo.invoice'] = { at: now };
check('a dismissed bundle item stays dismissed',
  !A.asSuggestBundles(bunStore, bunCatalog).bundles.some(b => b.key === 'references-resolve'));

// ── 18. The Evidence view data (§9) ─────────────────────────────────────────
const evStore = A.asNewStore(now);
const evR1 = A.asSaveRule(evStore, { name: 'GL balances', category: 'finance', check: 'aggregate.balance',
  params: { amount: 'amount' }, severity: 'critical', binding: { mode: 'explicit',
    targets: [{ db: 'tgt', schema: 'fin', table: 'gl', column: null }], databases: [] } }, 'u', now);
const evR2 = A.asSaveRule(evStore, { name: 'Client dupes', category: 'duplicates', check: 'uniqueness.composite',
  params: { columns: ['customer_num'] }, binding: { mode: 'explicit',
    targets: [{ db: 'tgt', schema: 'dbo', table: 'client', column: null }], databases: [] } }, 'u', now);
A.asRecordRun(evStore, { id: 'ev1', ruleId: evR1.id, startedAt: now, durationMs: 100, status: 'pass',
  rowsFailed: 0, rowsScanned: 100, targetsMatched: 1, severity: 'critical', category: 'finance',
  perTarget: [{ target: 'tgt.fin.gl', generatedSql: 'SELECT 1' }] }, now);
A.asRecordRun(evStore, { id: 'ev2', ruleId: evR2.id, startedAt: now + 1000, durationMs: 100, status: 'fail',
  rowsFailed: 12, rowsScanned: 900, targetsMatched: 1, severity: 'warning', category: 'duplicates',
  perTarget: [{ target: 'tgt.dbo.client', generatedSql: 'SELECT 2' }] }, now + 1000);
evStore.sweeps.push({ at: now + 1000, runIds: ['ev1', 'ev2'],
  manifestHash: A.asManifest(evStore.runs).hash });
const ev = A.asEvidence(evStore, now + 3600000);
check('the statement numbers come from the latest run per rule',
  ev.checksRun === 2 && ev.passed === 1);
check('open items are named with impact prose, owner, age and severity chip — never a bare count',
  ev.open.length === 1 && /12 rows do not currently satisfy/.test(ev.open[0].impact)
  && ev.open[0].owner === 'Unassigned' && ev.open[0].ageMs > 0
  && /^Open · /.test(ev.open[0].severity), JSON.stringify(ev.open[0]));
check('integrity reports edits since the first run — the "no check has been edited" claim is checked',
  ev.integrity.editsSince === 0
  && (A.asSaveRule(evStore, { id: evR1.id, severity: 'warning' }, 'u', now + 2000),
      A.asEvidence(evStore, now + 86400000).integrity.editsSince === 1));
check('tiles compute what they can and leave null what they cannot',
  ev.tiles.passingPct === 50 && ev.tiles.unbrokenDays === 1
  && ev.tiles.meanTimeToFixMs === null && ev.tiles.tablesCovered === null);
check('the manifest of the last sweep rides along', ev.manifest
  && ev.manifest.count === 2 && ev.manifest.hash === A.asManifest(evStore.runs).hash);
check('claims group by category and carry query, counts, run id and hash',
  Object.keys(ev.claims).length === 2
  && ev.claims['Finance'][0].query === 'SELECT 1'
  && ev.claims['Finance'][0].runId === 'ev1'
  && ev.claims['Finance'][0].hash === evStore.runs[0].runHash);
check('what this does not cover ships verbatim, all six items',
  ev.notCovered.length === 6 && ev.notCovered.some(x => /Correctness of the source data/.test(x))
  && ev.notCovered.some(x => /Third-party reports/.test(x)));

// ── 19. Store migration v1 → v2 ─────────────────────────────────────────────
const oldStore = A.asNewStore(now);
A.asSaveRule(oldStore, { name: 'templated', category: 'duplicates', check: 'uniqueness.composite',
  params: { columns: ['id'] }, binding: { mode: 'explicit',
    targets: [{ db: 'tgt', schema: 'dbo', table: 'client', column: null }], databases: [] } }, 'u', now);
A.asSaveRule(oldStore, { name: 'bespoke', category: 'custom', check: 'custom.sql',
  params: { sql: { any: 'SELECT 1' } } }, 'u', now);
A.asRecordRun(oldStore, { id: 'old1', ruleId: oldStore.rules[0].id, startedAt: now, status: 'pass',
  rowsFailed: 0, rowsScanned: 5, targetsMatched: 1, severity: 'warning', category: 'duplicates' }, now);
// strip it back to v1 shape
oldStore.v = 1;
delete oldStore.sweeps;
delete oldStore.settings.materialRowThreshold;
oldStore.rules.forEach(r => { delete r.proves; });
oldStore.runs.forEach(r => { delete r.runHash; delete r.result; delete r.counts; });
A.asMigrateStore(oldStore, now + 5000);
check('migration fills proves from the template where one applies',
  /client keeps exactly one row per id/.test(oldStore.rules[0].proves));
check('and flags the rest for manual authoring rather than leaving a blank',
  !oldStore.rules[1].proves && oldStore.rules[1].provesNeedsAuthor === true);
check('old runs gain their hash and record fields',
  oldStore.runs[0].runHash && oldStore.runs[0].result === 'pass' && oldStore.runs[0].counts.compared === 5);
check('the store gains sweeps, the threshold, the version bump and an audit event',
  Array.isArray(oldStore.sweeps) && oldStore.settings.materialRowThreshold === A.MATERIAL_ROWS_DEFAULT
  && oldStore.v === A.STORE_VERSION
  && oldStore.events.some(e => e.type === 'store.migrated'));
check('a store already at v2 passes through untouched',
  (() => { const s = A.asNewStore(now); const before = JSON.stringify(s);
    return A.asMigrateStore(s, now) === s && JSON.stringify(s) === before; })());

// ── 20. Wiring ──────────────────────────────────────────────────────────────
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

// §9: the audience split replaces the six equal-weight tabs.
check('the page splits into Engineer and Evidence views — the old six tabs are gone',
  PAGE.includes('as-v-eng') && PAGE.includes('as-v-ev')
  && !PAGE.includes('as-v-health') && !PAGE.includes('as-v-report'));
// §2 + §3: one status line, one primary CTA in the header.
check('one status line and one primary CTA live in the page header',
  PAGE.includes('as-status-line') && PAGE.includes('id="as-cta"')
  /* filled buttons: header CTA, bundle Add-all (§6 spec), drawer save */
  && (PAGE.match(/class="btn btn-primary"/g) || []).length === 3
  && !/as-head[\s\S]{0,400}\+ New rule/.test(PAGE));
check('the state machine covers all four states with the brief\'s copy',
  PAGE.includes('No checks written yet') && PAGE.includes('Nothing has been proven yet')
  && PAGE.includes('Review breaches') && PAGE.includes('Publish evidence pack')
  && PAGE.includes('so there is no evidence to show anyone'));
// §4: the ribbon derives from the same status object.
check('the four-step ribbon renders from asRibbon, not its own state',
  PAGE.includes('as-ribbon') && /asRibbon\(st\)/.test(PAGE));
// §3: renamed, demoted buttons.
check('Import from Validation is a ghost button; Re-profile and + New rule are secondary',
  /btn-ghost[^>]*onclick="asImportValidation/.test(PAGE)
  && PAGE.includes('↻ Re-profile') && !PAGE.includes('Promote from Validation')
  /* one + New rule BUTTON, in the Your-checks card header (prose mentions aside) */
  && (PAGE.match(/onclick="asDrawerOpen\(\)">\+ New rule/g) || []).length === 1);
// §5: coverage instead of a column of zeros.
check('the coverage card replaces the category report',
  PAGE.includes('Coverage of the migration') && /asCoverage\(/.test(PAGE)
  && PAGE.includes('Biggest gaps:') && PAGE.includes('material but unchecked'));
check('categories survive as filter chips on the search row',
  PAGE.includes('as-catchips') && !PAGE.includes('as-cats'));
// §6: bundles are the default unit of work.
check('suggestions render as template bundles with Add all and Review',
  /asSuggestBundles\(/.test(PAGE) && PAGE.includes('asBundleAddAll')
  && PAGE.includes('asBundleReview') && PAGE.includes('grouped into'));
// §1: the two empty states can no longer contradict the counts.
check('a filtered-empty library says so and offers Show all — never "No rules yet"',
  PAGE.includes('No rules ') && PAGE.includes('Show all ')
  && /if \(!store\.rules\.length\)/.test(PAGE));
check('the truly-empty state hides the Showing-N note',
  /note\.style\.display = 'none'/.test(PAGE));
// §7: the table leads with what each rule proves, on a human schedule.
check('the rule table leads with What it proves and a human schedule',
  PAGE.includes('<th>What it proves</th>') && PAGE.includes('<th>Scope</th>')
  && PAGE.includes('<th>Runs</th>') && PAGE.includes('<th>Last result</th>')
  && /asCronHuman\(cron\)/.test(PAGE));
check('the drawer requires a proves sentence when no template fits',
  PAGE.includes('as-dw-proves') && /asProvesFor\(draft\)/.test(PAGE));
// §8: sweeps carry a manifest hash; the store migrates v1 → v2 on load.
check('every sweep records a manifest hash over the run hashes',
  /recordSweep/.test(PAGE) && /asManifest\(/.test(PAGE) && /manifestHash/.test(PAGE));
check('a v1 store is migrated on load',
  /asMigrateStore\(store/.test(PAGE));
// §9: the Evidence view is real — statement, tiles, open items, exports.
check('the Evidence view carries the statement, signature strip and four tiles',
  PAGE.includes('Statement of checks') && PAGE.includes('as-ev-sig')
  && PAGE.includes('Prepared by Cygenix Migration Console (automated)')
  && PAGE.includes('as-ev-tiles'));
check('what-this-does-not-cover ships and open items are named',
  PAGE.includes('What this does not cover') && PAGE.includes('never a bare count'));
check('the evidence pack is real exports, not print-this-page',
  PAGE.includes('asExportStatementPdf') && PAGE.includes('asExportResultsCsv')
  && PAGE.includes('asExportSqlManifest') && PAGE.includes('asExportRunLog')
  && !PAGE.includes('window.print()'));

// Behaviours that must survive the redesign.
check('the rule builder is a drawer with a dry-run gate before save',
  /as-drawer/.test(PAGE) && /asDryRun/.test(PAGE));
check('the generated SQL is logged with the run so a DBA can reproduce it',
  /countSql/.test(PAGE) && /generatedSql/.test(PAGE));
check('breach hand-off goes to the existing Cleansing module',
  /data-cleansing\.html/.test(PAGE));
check('the import still reads the Validation module read-only',
  /cygenix_validation_sources/.test(PAGE));
check('suppression in the page asks for reason and expiry',
  /asSuppress/.test(PAGE) && /reason/i.test(PAGE));
check('notification routing goes through the existing integrations emit',
  /CygenixIntegrations/.test(PAGE) && /emit\(/.test(PAGE));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
