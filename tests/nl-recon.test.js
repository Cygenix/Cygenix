// tests/nl-recon.test.js — plain-English reconciliation tests.
//
// The pitch phrase is the headline: "check no customer lost their opening
// balance" must become a per-customer source/target SUM comparison with a
// drillable list of the customers that differ — generated entirely from
// the job's own column mapping, no SQL typed by anyone.

'use strict';

const N = require('../public/cygenix-nl-recon.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('NL reconciliation — engine\n');

const JOBS = [
  { id: 'j1', name: 'Customer load', sourceTable: 'src.customers', targetTable: 'dbo.clients',
    srcWhere: "status = 'ACTIVE'",
    columnMapping: [
      { srcCol: 'cust_ref', tgtCol: 'client_id' },
      { srcCol: 'opn_bal', tgtCol: 'opening_balance' },
      { srcCol: 'full_name', tgtCol: 'display_name' },
      { srcCol: 'row_id', tgtCol: 'sys_id', _isIdentity: true },
    ] },
  { id: 'j2', name: 'Invoice load', sourceTable: 'src.invoices', targetTable: 'dbo.invoices',
    columnMapping: [
      { srcCol: 'inv_no', tgtCol: 'invoice_number' },
      { srcCol: 'net_amt', tgtCol: 'net_amount' },
    ] },
];

// ── The pitch phrase ──────────────────────────────────────────────────────
const r1 = N.nrParse('check no customer lost their opening balance', JOBS);
check('the pitch phrase parses', r1.ok, JSON.stringify(r1));
check('as a per-key value-preservation check on the customer job',
  r1.ok && r1.spec.kind === 'perkey' && r1.spec.jobId === 'j1', JSON.stringify(r1.spec));
check('with the balance column resolved through the mapping (opn_bal → opening_balance)',
  r1.ok && r1.spec.col.srcCol === 'opn_bal' && r1.spec.col.tgtCol === 'opening_balance');
check('grouped by the customer key pair (cust_ref → client_id)',
  r1.ok && r1.spec.key.srcCol === 'cust_ref' && r1.spec.key.tgtCol === 'client_id');

const sql1 = N.nrBuildSql(r1.spec);
check('the generated pair aggregates per key on each side, source WHERE honoured',
  /SUM\(CAST\(\[opn_bal\] AS FLOAT\)\)/.test(sql1.srcSql) && /GROUP BY \[cust_ref\]/.test(sql1.srcSql)
  && /WHERE status = 'ACTIVE'/.test(sql1.srcSql)
  && /SUM\(CAST\(\[opening_balance\] AS FLOAT\)\)/.test(sql1.tgtSql) && /GROUP BY \[client_id\]/.test(sql1.tgtSql),
  sql1.srcSql + ' | ' + sql1.tgtSql);
check('both sides are single read-only SELECTs',
  /^SELECT /.test(sql1.srcSql) && /^SELECT /.test(sql1.tgtSql));

const cmp1 = N.nrCompare(r1.spec,
  [{ k: 'C1', v: 100 }, { k: 'C2', v: 250.5 }, { k: 'C3', v: 80 }],
  [{ k: 'C1', v: 100 }, { k: 'C2', v: 200.5 }, { k: 'C4', v: 9 }]);
check('the delta list names exactly who differs and how',
  !cmp1.pass && cmp1.deltas.length === 3
  && cmp1.deltas.some(d => d.key === 'C2' && /Δ -50/.test(d.note))
  && cmp1.deltas.some(d => d.key === 'C3' && d.note === 'missing from target')
  && cmp1.deltas.some(d => d.key === 'C4' && d.note === 'not in source'),
  JSON.stringify(cmp1.deltas));
check('agreement passes with a headline that says so',
  N.nrCompare(r1.spec, [{ k: 'C1', v: 100 }], [{ k: 'C1', v: 100 }]).pass);

// ── Other phrasings ───────────────────────────────────────────────────────
const r2 = N.nrParse('row counts match for customers', JOBS);
check('"row counts match" is a rowcount check',
  r2.ok && r2.spec.kind === 'rowcount' && r2.spec.jobId === 'j1');
check('rowcount SQL is COUNT_BIG on both sides',
  /COUNT_BIG/.test(N.nrBuildSql(r2.spec).srcSql));
check('exact by default: 100 vs 99 fails; 100 vs 100 passes',
  !N.nrCompare(r2.spec, [{ v: 100 }], [{ v: 99 }]).pass
  && N.nrCompare(r2.spec, [{ v: 100 }], [{ v: 100 }]).pass);

const r3 = N.nrParse('invoice totals match within 0.5%', JOBS);
check('"invoice totals match within 0.5%" is a tolerant total on the invoice job',
  r3.ok && r3.spec.kind === 'total' && r3.spec.jobId === 'j2' && r3.spec.tolPct === 0.005,
  JSON.stringify(r3.spec));
check('the tolerance is honoured in comparison',
  N.nrCompare(r3.spec, [{ v: 1000 }], [{ v: 1004 }]).pass
  && !N.nrCompare(r3.spec, [{ v: 1000 }], [{ v: 1010 }]).pass);

const r4 = N.nrParse('no duplicates on invoice number', JOBS);
check('"no duplicates on invoice number" checks the target for dupes',
  r4.ok && r4.spec.kind === 'duplicates' && r4.spec.key.tgtCol === 'invoice_number');
check('duplicate groups come back as the drill list',
  (() => { const c = N.nrCompare(r4.spec, [{ v: 0 }], [{ k: 'INV-9', v: 3 }]);
           return !c.pass && c.deltas[0].key === 'INV-9' && /3×/.test(c.deltas[0].note); })());

const r5 = N.nrParse('every customer exists in the target', JOBS);
check('"every customer exists in the target" is a containment probe on the key',
  r5.ok && r5.spec.kind === 'exists' && r5.spec.key.srcCol === 'cust_ref');
check('its probe quotes and escapes sampled keys',
  N.nrExistsProbeSql(r5.spec, ["O'Brien", 5]).includes("N'O''Brien'"));
check('missing keys are the drill list',
  (() => { const c = N.nrCompare(r5.spec, [{ k: 'C1' }, { k: 'C2' }], [{ k: 'C2' }]);
           return !c.pass && c.deltaCount === 1 && c.deltas[0].key === 'C2'; })());

const r6 = N.nrParse('no missing display name in customers', JOBS);
check('"no missing display name" is a null check on the mapped column',
  r6.ok && r6.spec.kind === 'nulls' && r6.spec.col.tgtCol === 'display_name', JSON.stringify(r6.spec));
check('new NULLs fail; matching or fewer NULLs pass',
  !N.nrCompare(r6.spec, [{ v: 0 }], [{ v: 4 }]).pass
  && N.nrCompare(r6.spec, [{ v: 4 }], [{ v: 4 }]).pass);

// ── Honest failure modes ──────────────────────────────────────────────────
check('gibberish fails with a helpful hint, not a guess',
  (() => { const r = N.nrParse('purple monkey dishwasher', JOBS);
           return !r.ok && /phrasings|entity|job/i.test(r.error + r.hint); })());
check('an empty job list refuses rather than inventing tables',
  !N.nrParse('row counts match', []).ok);
check('a single-job project needs no entity words',
  (() => { const r = N.nrParse('row counts match', [JOBS[1]]);
           return r.ok && r.spec.jobId === 'j2'; })());

// ── Descriptions ──────────────────────────────────────────────────────────
check('the interpretation is shown in plain words before running',
  /SUM\(opn_bal\) per cust_ref/.test(N.nrDescribe(r1.spec))
  && /Row counts must match/.test(N.nrDescribe(r2.spec)));

// ── Orchestrator over a stubbed wire ──────────────────────────────────────
(async () => {
  const issued = [];
  const dbCall = async (conn, body) => {
    issued.push({ conn, sql: body.sql });
    if (conn === 'S') return { recordset: [{ k: 'C1', v: 100 }, { k: 'C2', v: 50 }] };
    return { recordset: [{ k: 'C1', v: 100 }, { k: 'C2', v: 49 }] };
  };
  const res = await N.nrRun(r1.spec, { srcConn: 'S', tgtConn: 'T', dbCall });
  check('the orchestrator runs the pair and drills the difference',
    !res.pass && res.deltas.length === 1 && res.deltas[0].key === 'C2', JSON.stringify(res.deltas));
  check('every statement issued is a SELECT',
    issued.every(c => /^SELECT/i.test(c.sql.trim())), JSON.stringify(issued.map(c => c.sql)));

  const issued2 = [];
  const dbCall2 = async (conn, body) => {
    issued2.push(body.sql);
    if (conn === 'S') return { recordset: [{ k: 'C1' }, { k: 'C2' }] };
    return { recordset: [{ k: 'C1' }] };            // probe: C1 missing
  };
  const res2 = await N.nrRun(r5.spec, { srcConn: 'S', tgtConn: 'T', dbCall: dbCall2 });
  check('the exists probe runs client-side across the two servers',
    !res2.pass && res2.probed === 2 && issued2[1].includes('NOT EXISTS'), JSON.stringify(res2));

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
