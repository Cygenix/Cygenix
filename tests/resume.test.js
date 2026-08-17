// tests/resume.test.js — self-healing resume.
//
// The promise: a job fails at row 4M, the cause is diagnosed from the
// error text AND the failing rows themselves, a mapping patch is proposed
// using the editor's own safe transforms, and the run resumes from the
// exact failure point — but only when that is provably safe. The unsafe
// cases must refuse with the reason, never resume into duplicates.

'use strict';

const R = require('../public/cygenix-resume.js');
const P = require('../public/cygenix-preflight.js');   // real checkers, injected

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Self-healing resume — engine\n');

// ── Error classification ──────────────────────────────────────────────────
check('a modern truncation message names the column',
  (() => { const c = R.rsClassifyError("String or binary data would be truncated in table 'db.dbo.people', column 'surname'. Truncated value: 'Fitzwil'.");
           return c.code === 'truncation' && c.column === 'surname'; })());
check('an old truncation message still classifies, column unknown',
  (() => { const c = R.rsClassifyError('String or binary data would be truncated.');
           return c.code === 'truncation' && c.column === null; })());
check('a NULL violation names its column',
  R.rsClassifyError("Cannot insert the value NULL into column 'name', table 'x'; column does not allow nulls.").column === 'name');
check('date conversion, overflow and numeric-cast failures classify',
  R.rsClassifyError('Conversion failed when converting date and/or time from character string.').code === 'bad-date'
  && R.rsClassifyError('Arithmetic overflow error converting numeric to data type numeric.').code === 'overflow'
  && R.rsClassifyError('Error converting data type nvarchar to numeric.').code === 'not-numeric');
check('FK and PK violations carry the constraint name',
  R.rsClassifyError('The INSERT statement conflicted with the FOREIGN KEY constraint "FK_orders_customers".').constraint === 'FK_orders_customers'
  && R.rsClassifyError("Violation of PRIMARY KEY constraint 'PK_people'. Cannot insert duplicate key.").code === 'pk-violation');
check('deadlocks and timeouts are transient — retry, not patch',
  R.rsClassifyError('Transaction was deadlocked on lock resources with another process.').transient
  && R.rsClassifyError('Timeout expired. The timeout period elapsed.').transient);
check('an unrecognised message is unknown, not guessed',
  R.rsClassifyError('some entirely new failure').code === 'unknown');

// ── Value scan of the failing rows ────────────────────────────────────────
const TGT = {
  surname: { type: 'VARCHAR(10)', nullable: true },
  amount:  { type: 'DECIMAL(5,2)', nullable: true },
  dob:     { type: 'DATETIME', nullable: true },
};
const tgtByName = Object.fromEntries(Object.entries(TGT).map(([k, v]) => [k, { name: k, ...v }]));
const MAPPING = [
  { srcCol: 's', tgtCol: 'surname' }, { srcCol: 'a', tgtCol: 'amount' }, { srcCol: 'd', tgtCol: 'dob' },
];
const ROWS = [
  { s: 'ok', a: 12.5, d: '1990-01-01' },
  { s: 'far too long for ten chars', a: 99999.99, d: 'yesterday-ish' },
  { s: 'fine', a: 1, d: '1990-01-01' },
];
const scan = R.rsScanRows(ROWS, MAPPING, tgtByName, P);
check('the failing rows are scanned with the preflight checkers and name each culprit',
  scan.some(f => f.code === 'truncation' && f.tgtCol === 'surname')
  && scan.some(f => f.code === 'overflow' && f.tgtCol === 'amount')
  && scan.some(f => f.code === 'bad-date' && f.tgtCol === 'dob'), JSON.stringify(scan));
check('findings carry example values for the card',
  scan.find(f => f.code === 'bad-date').examples.includes('yesterday-ish'));

// ── Patch proposal ────────────────────────────────────────────────────────
check('truncation proposes SAFE_TRUNC on the named column',
  (() => { const p = R.rsProposePatch({ code: 'truncation', column: 'surname' }, scan);
           return p.kind === 'patch' && p.transform === 'SAFE_TRUNC' && p.tgtCol === 'surname' && p.srcCol === 's'; })());
check('a vague message is refined by the scan — the top finding names the column',
  (() => { const p = R.rsProposePatch({ code: 'unknown', column: null }, scan);
           return p.kind === 'patch' && p.tgtCol && p.transform; })());
check('bad dates propose DATE_CAST; numeric failures propose SAFE_NUMERIC',
  R.rsProposePatch({ code: 'bad-date', column: 'dob' }, scan).transform === 'DATE_CAST'
  && R.rsProposePatch({ code: 'not-numeric', column: 'amount' }, scan).transform === 'SAFE_NUMERIC');
check('transient failures propose plain retry, no patch',
  R.rsProposePatch({ code: 'deadlock', transient: true }, []).kind === 'retry');
check('NULL violations advise a fixed value instead of a lossy transform',
  (() => { const p = R.rsProposePatch({ code: 'null-violation', column: 'name' }, []);
           return p.kind === 'advise' && /Fixed value/.test(p.why); })());
check('FK violations advise loading the parent first',
  /parent/.test(R.rsProposePatch({ code: 'fk-violation', constraint: 'FK_x' }, []).why));

// ── Resume plan — the safety core ─────────────────────────────────────────
const ok100 = (n) => Array.from({ length: n }, (_, i) => ({ index: i, success: true, rowsAffected: 100 }));

check('backend stopped at the failing statement → resume exactly there, no cleanup',
  (() => { const plan = R.rsResumePlan({ pageStartOffset: 4000000, batchSize: 100, stoppedEarly: true, keyed: false,
             results: [...ok100(3), { index: 3, success: false, error: 'x' }] });
           return plan.resumable && plan.resumeOffset === 4000300 && plan.cleanupKeysFrom === null; })());
check('backend ran past the failure but rows are keyed → cleanup closes the hole, resume at failure',
  (() => { const plan = R.rsResumePlan({ pageStartOffset: 1000, batchSize: 100, stoppedEarly: false, keyed: true,
             results: [ok100(1)[0], { index: 1, success: false }, { index: 2, success: true, rowsAffected: 100 }] });
           return plan.resumable && plan.resumeOffset === 1100 && plan.cleanupKeysFrom === 100; })());
check('backend ran past the failure and rows have NO key → refuse, with the reason',
  (() => { const plan = R.rsResumePlan({ pageStartOffset: 1000, batchSize: 100, stoppedEarly: false, keyed: false,
             results: [ok100(1)[0], { index: 1, success: false }, { index: 2, success: true, rowsAffected: 100 }] });
           return !plan.resumable && /hole/.test(plan.reason); })());
check('a thrown call with keyed rows → clean the whole page by key, resume at page start',
  (() => { const plan = R.rsResumePlan({ pageStartOffset: 3999500, batchSize: 100, thrown: true, keyed: true });
           return plan.resumable && plan.resumeOffset === 3999500 && plan.cleanupKeysFrom === 0; })());
check('a thrown call with unkeyed rows → refuse rather than risk duplicates',
  !R.rsResumePlan({ pageStartOffset: 0, batchSize: 100, thrown: true, keyed: false }).resumable);

// ── Cleanup SQL ───────────────────────────────────────────────────────────
check('cleanup deletes by run id and source key, escaped and chunked',
  (() => { const sql = R.rsCleanupSql('dm_staging.[dbo__people]', 'run_1', ["O'Brien", 'K2']);
           return sql.length === 1 && /dm_run_id\] = N'run_1'/.test(sql[0]) && /N'O''Brien'/.test(sql[0])
             && /^DELETE FROM dm_staging/.test(sql[0]); })());
check('large key sets chunk into multiple statements',
  R.rsCleanupSql('t', 'r', Array.from({ length: 1200 }, (_, i) => 'k' + i)).length === 3);

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
