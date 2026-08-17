// tests/preflight.test.js — the Preflight Simulator engine.
//
// The promise is a forecast: what will break, where, why, projected over
// the full load. These tests pin the type maths (the varchar(50) that
// should have been 200, the DECIMAL that overflows, the date that never
// parses), the projection arithmetic, the verdicts, the generated SQL,
// and the Cutover Confidence roll-up — plus a full orchestrator run over
// a stubbed wire.

'use strict';

const P = require('../public/cygenix-preflight.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Preflight Simulator — engine\n');

// ── Type parsing ──────────────────────────────────────────────────────────
check('VARCHAR(50) parses to an 8-bit string of length 50',
  (() => { const t = P.pfParseType('VARCHAR(50)'); return t.family === 'string' && !t.unicode && t.length === 50; })());
check('NVARCHAR(MAX) is unicode and unbounded',
  (() => { const t = P.pfParseType('NVARCHAR(MAX)'); return t.unicode && t.length === Infinity; })());
check('DECIMAL(10,2) carries precision and scale',
  (() => { const t = P.pfParseType('DECIMAL(10,2)'); return t.family === 'decimal' && t.precision === 10 && t.scale === 2; })());
check('INT and TINYINT carry their ranges',
  P.pfParseType('INT').max === 2147483647 && P.pfParseType('TINYINT').min === 0);
check('DATETIME floors at 1753, SMALLDATETIME is windowed',
  P.pfParseType('DATETIME').minYear === 1753 && P.pfParseType('SMALLDATETIME').maxYear === 2079);

// ── Per-value checks — the review scenario ───────────────────────────────
const v50 = Object.assign(P.pfParseType('VARCHAR(50)'), { nullable: true });
check('a 60-char value into varchar(50) is a truncation reject',
  P.pfCheckValue('x'.repeat(60), v50).code === 'truncation');
check('a 50-char value fits exactly', P.pfCheckValue('x'.repeat(50), v50) === null);
check('unicode into varchar is silent corruption (amber)',
  P.pfCheckValue('Zürich ☃', v50).code === 'unicode-loss' && P.pfSeverity('unicode-loss') === 'amber');
check('but the same into NVARCHAR fits',
  P.pfCheckValue('Zürich ☃', P.pfParseType('NVARCHAR(50)')) === null);

const d102 = P.pfParseType('DECIMAL(10,2)');
check('99999999.99 fits DECIMAL(10,2); 100000000 overflows',
  P.pfCheckValue(99999999.99, d102) === null && P.pfCheckValue(100000000, d102).code === 'overflow');
check('three decimals into scale 2 rounds silently (amber)',
  P.pfCheckValue('12.345', d102).code === 'scale-round' && P.pfSeverity('scale-round') === 'amber');
check('a non-numeric string into a numeric column rejects',
  P.pfCheckValue('N/A', d102).code === 'not-numeric');

const iSpec = P.pfParseType('INT');
check('an out-of-range number overflows INT',
  P.pfCheckValue(3000000000, iSpec).code === 'overflow');
check('a fraction into INT is silent loss (amber)',
  P.pfCheckValue(3.7, iSpec).code === 'fraction-loss');

const dtSpec = P.pfParseType('DATETIME');
check('an ISO date parses; garbage does not',
  P.pfCheckValue('2026-08-17', dtSpec) === null && P.pfCheckValue('not a date', dtSpec).code === 'bad-date');
check('dd/mm/yyyy and mm/dd/yyyy both parse; 45/45/2020 does not',
  P.pfCheckValue('17/08/2026', dtSpec) === null && P.pfCheckValue('08/17/2026', dtSpec) === null
  && P.pfCheckValue('45/45/2020', dtSpec).code === 'bad-date');
check('1690 rejects for DATETIME (range starts 1753)',
  P.pfCheckValue('1690-01-01', dtSpec).code === 'date-range');
check('a GUID column rejects a non-GUID',
  P.pfCheckValue('nope', P.pfParseType('UNIQUEIDENTIFIER')).code === 'bad-guid'
  && P.pfCheckValue('6F9619FF-8B86-D011-B42D-00C04FC964FF', P.pfParseType('UNIQUEIDENTIFIER')) === null);

// ── Column aggregation and null handling ─────────────────────────────────
const nn = Object.assign(P.pfParseType('VARCHAR(10)'), { nullable: false, isIdentity: false, hasDefault: false });
check('nulls into NOT NULL are counted with examples',
  (() => { const f = P.pfCheckColumn(['a', null, null, 'b'], nn, {});
           return f.length === 1 && f[0].code === 'null-violation' && f[0].count === 2; })());
check('a literal on the mapping suppresses the null check',
  P.pfCheckColumn([null, null], nn, { hasLiteral: true }).length === 0);
check('a nullable target accepts nulls silently',
  P.pfCheckColumn([null], Object.assign(P.pfParseType('VARCHAR(10)'), { nullable: true }), {}).length === 0);
check('mixed failures aggregate by code with capped examples',
  (() => { const f = P.pfCheckColumn(['ok', 'x'.repeat(11), 'y'.repeat(12), 'z'.repeat(13), 'w'.repeat(14)],
             Object.assign(P.pfParseType('VARCHAR(10)'), { nullable: true }), {});
           return f.length === 1 && f[0].count === 4 && f[0].examples.length === 3; })());

// ── Unmapped required columns ─────────────────────────────────────────────
const tgtCols = [
  { name: 'id',      type: 'INT',          nullable: false, isIdentity: true },
  { name: 'name',    type: 'VARCHAR(50)',  nullable: false, isIdentity: false, default: null },
  { name: 'created', type: 'DATETIME',     nullable: false, isIdentity: false, default: 'getdate()' },
  { name: 'notes',   type: 'VARCHAR(MAX)', nullable: true,  isIdentity: false, default: null },
];
check('a NOT NULL column with no mapping, default or identity is flagged',
  (() => { const f = P.pfUnmappedRequired(tgtCols, [{ srcCol: 'n', tgtCol: 'notes' }]);
           return f.length === 1 && f[0].column === 'name'; })());
check('identity, defaulted and mapped columns are not',
  P.pfUnmappedRequired(tgtCols, [{ srcCol: 'nm', tgtCol: 'name' }]).length === 0);
check('a literal feeding the column counts as fed',
  P.pfUnmappedRequired(tgtCols, [{ tgtCol: 'name', literalValue: 'X' }]).length === 0);

// ── Projection ────────────────────────────────────────────────────────────
check('3 failures in a 1,000-row sample of 2m rows projects ~6,000',
  P.pfProject(3, 1000, 2000000).projected === 6000);
check('projection never exceeds the total or undercuts the observed',
  P.pfProject(5, 5, 3).projected === 3 && P.pfProject(1, 1000, 500000).projected >= 1);
check('zero failures projects zero', P.pfProject(0, 1000, 1e6).projected === 0);

// ── Verdicts ──────────────────────────────────────────────────────────────
check('any red finding makes the job red',
  P.pfJobVerdict([{ code: 'scale-round' }, { code: 'truncation' }]) === 'red');
check('ambers alone make it amber; none make it green',
  P.pfJobVerdict([{ code: 'unicode-loss' }]) === 'amber' && P.pfJobVerdict([]) === 'green');
check('the overall verdict is the worst job',
  P.pfOverallVerdict([{ verdict: 'green' }, { verdict: 'red' }]) === 'red'
  && P.pfOverallVerdict([{ verdict: 'green' }, { verdict: 'amber' }]) === 'amber');

// ── Runtime estimate ──────────────────────────────────────────────────────
check('the estimate derates measured read throughput for the write path',
  (() => { const e = P.pfRuntimeEstimate(1000000, 1000, 1000);   // 1k rows/s read
           return e.seconds > 1000 / 0.5 && e.seconds <= 1000000 / 50; })());
check('durations format for humans',
  P.pfFmtDuration(45) === '45s' && P.pfFmtDuration(150) === '2m 30s' && P.pfFmtDuration(7300) === '2h 1m');

// ── Generated SQL ─────────────────────────────────────────────────────────
check('the sample SELECT quotes columns and folds the WHERE in',
  P.pfSampleSql('dbo.src', ['a', 'b]c'], 'WHERE x = 1', 500)
  === 'SELECT TOP (500) [a], [b]]c] FROM dbo.src WHERE x = 1');
check('the count query honours the same WHERE',
  P.pfCountSql('dbo.src', 'x = 1') === 'SELECT COUNT_BIG(*) AS n FROM dbo.src WHERE x = 1');
check('string literals in the FK probe are escaped',
  P.pfFkProbeSql(["O'Brien", 5], 'dbo.parent', 'id', '').includes("N'O''Brien'")
  && P.pfFkProbeSql(["O'Brien", 5], 'dbo.parent', 'id', '').includes('(5)'));

// ── Cutover Confidence ────────────────────────────────────────────────────
check('no data at all yields no score, not a fake one',
  (() => { const c = P.pfConfidence({ jobs: [] }); return c.score === null && c.grade === 'no-data'; })());
check('a clean project scores green',
  (() => {
    const c = P.pfConfidence({
      jobs: [{ status: 'complete', columnMapping: [{ tgtCol: 'a', match: 96 }, { tgtCol: 'b', match: 92 }] }],
      preflight: { generatedAt: new Date().toISOString(), jobs: [{ verdict: 'green' }] },
    });
    return c.score >= 85 && c.grade === 'green';
  })());
check('a red preflight drags the score down hard',
  (() => {
    const c = P.pfConfidence({
      jobs: [{ status: 'complete', columnMapping: [{ tgtCol: 'a', match: 95 }] }],
      preflight: { generatedAt: new Date().toISOString(), jobs: [{ verdict: 'red' }, { verdict: 'red' }] },
    });
    return c.score < 60 && c.grade === 'red';
  })());
check('a stale preflight is penalised and labelled',
  (() => {
    const old = new Date(Date.now() - 10 * 86400000).toISOString();
    const a = P.pfConfidence({ jobs: [], preflight: { generatedAt: old, jobs: [{ verdict: 'green' }] } });
    const b = P.pfConfidence({ jobs: [], preflight: { generatedAt: new Date().toISOString(), jobs: [{ verdict: 'green' }] } });
    const pa = a.parts.find(p => p.key === 'preflight');
    return pa.score < b.parts.find(p => p.key === 'preflight').score && /stale/.test(pa.note);
  })());
check('components without data are excluded from the weighting',
  (() => {
    const c = P.pfConfidence({ jobs: [{ status: 'complete', columnMapping: [] }] });  // only delivery has data
    return c.score === 100 && c.parts.filter(p => p.hasData).length === 1;
  })());
check('editor grades (HIGH/MEDIUM) and evidence scores count as mapping data',
  (() => {
    const c = P.pfConfidence({ jobs: [{ status: 'draft', columnMapping: [
      { tgtCol: 'a', match: 'HIGH' }, { tgtCol: 'b', match: 'MEDIUM' },
      { tgtCol: 'c', match: 'LOW', evidence: { score: 44 } },
    ] }] });
    const m = c.parts.find(p => p.key === 'mapping');
    return m.hasData && m.score === Math.round((95 + 70 + 44) / 3);
  })());

// ── Orchestrator over a stubbed wire ──────────────────────────────────────
(async () => {
  const calls = [];
  const dbCall = async (conn, body) => {
    calls.push({ conn, action: body.action, sql: body.sql });
    if (body.action === 'schema-columns') {
      return { table: {
        schema: 'dbo', name: body.tableName,
        columns: [
          { name: 'id',    type: 'INT',         nullable: false, isIdentity: true },
          { name: 'name',  type: 'VARCHAR(10)', nullable: false, isIdentity: false, default: null },
          { name: 'amt',   type: 'DECIMAL(5,2)',nullable: true,  isIdentity: false },
          { name: 'ref',   type: 'INT',         nullable: true,  isIdentity: false },
        ],
        primaryKeys: ['id'],
        foreignKeys: [{ column: 'ref', references: 'dbo.parent(id)' }],
      } };
    }
    if (/COUNT_BIG/.test(body.sql)) return { recordset: [{ n: 10000 }] };
    if (/^SELECT TOP/.test(body.sql)) return { recordset: [
      { nm: 'short', amount: 12.34, r: 1 },
      { nm: 'way too long for ten', amount: 999.99, r: 2 },
      { nm: null, amount: 1234.5, r: 3 },          // null → NOT NULL; 1234.5 overflows DECIMAL(5,2)
    ] };
    if (/VALUES/.test(body.sql)) return { recordset: [{ hits: 2 }] };   // 3 distinct refs, 2 parents → 1 orphan
    return { recordset: [] };
  };

  const steps = [{
    name: 'Load names', type: 'migration', srcTable: 'src.people', tgtTable: 'dbo.people',
    srcWhere: '', columnMapping: [
      { srcCol: 'nm', tgtCol: 'name', match: 90 },
      { srcCol: 'amount', tgtCol: 'amt', match: 88 },
      { srcCol: 'r', tgtCol: 'ref', match: 95 },
    ],
  }];
  const rep = await P.pfRunPreflight(steps, { srcConn: 'S', tgtConn: 'T', dbCall, sampleSize: 100 });
  const j = rep.jobs[0];
  const codes = j.findings.map(f => f.code).sort();

  check('the orchestrator forecasts truncation, null violation, overflow and the FK orphan',
    codes.includes('truncation') && codes.includes('null-violation')
    && codes.includes('overflow') && codes.includes('fk-orphan'), JSON.stringify(codes));
  check('every finding projects to full-load row counts',
    j.findings.every(f => f.projected >= f.sampleFails), JSON.stringify(j.findings));
  check('the job and report are red', j.verdict === 'red' && rep.verdict === 'red');
  check('projected failures roll up across red findings only', rep.projectedFailures > 0);
  check('and never exceed the job row count — one row rejects once',
    rep.projectedFailures <= rep.jobs.reduce((a, j) => a + j.totalRows, 0), String(rep.projectedFailures));
  check('all issued SQL is read-only',
    calls.filter(c => c.sql).every(c => /^SELECT/i.test(c.sql.trim())), JSON.stringify(calls.map(c => c.sql)));
  check('a SQL-script job is skipped honestly, not guessed at',
    (await P.pfRunPreflight([{ name: 'x', type: 'sql', jobType: 'sql-script' }],
      { srcConn: 'S', tgtConn: 'T', dbCall })).jobs[0].skipped !== undefined);

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
