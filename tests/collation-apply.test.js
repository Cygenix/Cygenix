// tests/collation-apply.test.js — collation matching, Stage C: applying the fix.
//
// Stage A saved a decision. Stage B found the clashes. Stage C acts: the
// generators emit COLLATE where the SQL is built, a person can apply a fix to
// their own query after seeing what it would do, a run can be refused, and a
// script built under settings that have since changed says so.
//
// This is the stage that changes SQL, so the assertions that matter most are
// the ones about restraint:
//
//   · a profile whose collations already agree produces SQL BYTE-IDENTICAL to
//     what it produced before this feature existed. Section 1 proves that
//     against the real staging generator, not against a helper;
//   · warn mode changes nothing;
//   · the fix goes in where the SQL is BUILT, never by rewriting a finished
//     script — the one exception being a query a person typed, which is
//     rewritten only after they have seen a before and an after;
//   · blocking a run is opt-in per rule, and an acknowledged finding is the
//     operator's decision and stops counting.
//
// The eight numbered sections are the brief's eight verification points.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

const R = require(path.join(ROOT, 'public', 'cygenix-collation-rules.js'));
const AR = require(path.join(ROOT, 'azure-function', 'src', 'collation-rules.js'));
const ST = require(path.join(ROOT, 'public', 'cygenix-template-staging.js'));
const RULES_SRC = read('public', 'cygenix-collation-rules.js');
const CARD = read('public', 'cygenix-collation.js');

console.log('Collation matching — Stage C: applying the fix\n');

/* The two profiles every section below compares: one whose databases
   disagree, and one whose databases already agree. */
const MISMATCHED = {
  source: { database: 'SRC', dbCollation: 'Latin1_General_CS_AS', tempdbCollation: 'Latin1_General_CS_AS' },
  target: { database: 'TGT', dbCollation: 'SQL_Latin1_General_CP1_CI_AS', tempdbCollation: 'SQL_Latin1_General_CP1_CI_AS' },
  strategy: 'target', tempTables: 'resolved', generatedSqlMode: 'apply', userSqlMode: 'offer_fix',
};
const MATCHED = {
  source: { database: 'SRC', dbCollation: 'Latin1_General_CI_AS', tempdbCollation: 'Latin1_General_CI_AS' },
  target: { database: 'TGT', dbCollation: 'Latin1_General_CI_AS', tempdbCollation: 'Latin1_General_CI_AS' },
  strategy: 'target', tempTables: 'resolved', generatedSqlMode: 'apply', userSqlMode: 'offer_fix',
};
const COLS = [
  { side: 'src', schema: 'dbo', table: 'A', column: 'Code', collation: 'Latin1_General_CS_AS', dataType: 'varchar' },
  { side: 'tgt', schema: 'dbo', table: 'B', column: 'Code', collation: 'SQL_Latin1_General_CP1_CI_AS', dataType: 'varchar' },
];

/* The REAL staging generator, driven the way the pages drive it. */
const TPL = { modules: [{ inScope: true, included: true, tables: [{
  stagingTable: 'Stg_Client',
  columns: [{ name: 'Code', type: 'varchar(20)' }, { name: 'Name', type: 'nvarchar(100)' }, { name: 'Qty', type: 'int' }],
}] }] };
const baseType = (t) => String(t || '').replace(/\(.*$/, '').trim().toLowerCase();
const stagingSql = (model) => ST.stagingPlan(TPL, {
  dialect: 'mssql', schema: 'dbo',
  collateFor: model ? ((c) => R.collateForTempColumn(model, baseType(c.type))) : undefined,
  marker: model ? R.MARKER : '',
}).tables[0].sql;

/* ── 1. apply mode ──────────────────────────────────────────────────────── */
section('1. Apply mode: the clause, the header and the markers');
{
  const sql = stagingSql(MISMATCHED);
  check('the generated DDL carries the COLLATE clause',
    /\[Code\] nvarchar\(max\) COLLATE SQL_Latin1_General_CP1_CI_AS NULL/.test(sql)
    || /\[Code\] varchar\(20\) COLLATE SQL_Latin1_General_CP1_CI_AS NULL/.test(sql), sql);
  check('every fixed line is marked, so a reader can see what this feature touched',
    (sql.match(/-- cyg:collation/g) || []).length === 2, sql);
  check('AND A NON-TEXT COLUMN IS LEFT ALONE', /\[Qty\] int NULL/.test(sql) && !/int COLLATE/.test(sql));
  check('the marker is the one the brief names', R.MARKER === '-- cyg:collation');

  const header = R.headerComment('Conv_DM to Azure', 2, 'SQL_Latin1_General_CP1_CI_AS');
  check('the header comment reads as the brief specifies',
    header === '-- Collation fixes applied from profile Conv_DM to Azure: 2 (resolved collation SQL_Latin1_General_CP1_CI_AS)',
    header);
  check('a script with no fixes gets no header at all',
    /if \(!n\) return '';/.test(CARD));
}

/* ── 2. warn mode ───────────────────────────────────────────────────────── */
section('2. Warn mode changes nothing');
{
  const warnModel = Object.assign({}, MISMATCHED, { generatedSqlMode: 'warn' });
  check('WARN MODE PRODUCES THE SAME SQL AS NO SETTINGS AT ALL',
    stagingSql(warnModel) === stagingSql(null), stagingSql(warnModel));
  check('and the clause helpers all return nothing',
    R.collateForTempColumn(warnModel, 'varchar') === ''
    && R.collateForColumn(warnModel, 'src', 'dbo', 'A', 'Code', 'Latin1_General_CS_AS') === ''
    && R.collateForComparison(warnModel, COLS[0], COLS[1]).left === '');
  check('but the clash is still FOUND in warn mode — warn means report, not ignore',
    R.findClashes('SELECT 1 FROM SRC.dbo.A a JOIN TGT.dbo.B b ON a.Code = b.Code',
      { model: warnModel, columns: COLS }).length === 1);
}

/* ── 3. Temp tables, both settings ──────────────────────────────────────── */
section('3. Temp tables: resolved, or DATABASE_DEFAULT');
{
  const dbDefault = Object.assign({}, MISMATCHED, { tempTables: 'database_default' });
  check('with database_default, temp text columns use the pseudo-collation',
    R.collateForTempColumn(dbDefault, 'varchar') === ' COLLATE DATABASE_DEFAULT'
    && /COLLATE DATABASE_DEFAULT/.test(stagingSql(dbDefault)));
  check('with resolved, they use the named collation',
    R.collateForTempColumn(MISMATCHED, 'varchar') === ' COLLATE SQL_Latin1_General_CP1_CI_AS'
    && /COLLATE SQL_Latin1_General_CP1_CI_AS/.test(stagingSql(MISMATCHED)));
  check('the two settings really do produce different SQL',
    stagingSql(dbDefault) !== stagingSql(MISMATCHED));
  check('and the setting reaches the generator through an option, not a global',
    /o\.collateFor/.test(read('public', 'cygenix-template-staging.js'))
    && /collateFor: o\.collateFor/.test(read('public', 'cygenix-template-staging.js')));
}

/* ── 4. Column overrides ────────────────────────────────────────────────── */
section('4. A column override wins, for that column only');
{
  const over = Object.assign({}, MISMATCHED, {
    columnOverrides: { 'dbo.A.Code': { collation: 'Latin1_General_BIN2', note: 'sorted as bytes downstream' } },
  });
  check('THE OVERRIDDEN COLUMN USES THE OVERRIDE',
    R.collateForColumn(over, 'src', 'dbo', 'A', 'Code', 'Latin1_General_CS_AS') === ' COLLATE Latin1_General_BIN2');
  check('and every other column still uses the strategy',
    R.collateForColumn(over, 'src', 'dbo', 'A', 'Other', 'Latin1_General_CS_AS') === ' COLLATE SQL_Latin1_General_CP1_CI_AS');
  check('an override applies even where the databases already agree — it was asked for explicitly',
    R.collateForColumn(Object.assign({}, MATCHED, { columnOverrides: over.columnOverrides }),
      'src', 'dbo', 'A', 'Code', 'Latin1_General_CI_AS') === ' COLLATE Latin1_General_BIN2');
  check('and it is matched however the person capitalised it',
    R.resolveWith(over, 'src', 'DBO', 'a', 'code') === 'Latin1_General_BIN2');
}

/* ── 5. User SQL: preview, then apply ───────────────────────────────────── */
section('5. A query a person wrote is rewritten only after a preview');
{
  const sql = 'SELECT *\nFROM SRC.dbo.A a\nJOIN TGT.dbo.B b ON a.Code = b.Code';
  const clashes = R.findClashes(sql, { model: MISMATCHED, columns: COLS });
  const res = R.applyFix(sql, clashes, MISMATCHED);
  check('the fix adds the clause to the side that is not already the resolved collation',
    res.sql === 'SELECT *\nFROM SRC.dbo.A a\nJOIN TGT.dbo.B b ON a.Code COLLATE SQL_Latin1_General_CP1_CI_AS = b.Code',
    res.sql);
  check('and reports how many it applied', res.applied === 1 && res.skipped === 0);
  check('THE RESULT IS CLEAN — re-linting the fixed SQL finds nothing left',
    R.findClashes(res.sql, { model: MISMATCHED, columns: COLS }).length === 0);
  check('applying to SQL with nothing to fix returns it unchanged',
    R.applyFix('SELECT 1', [], MISMATCHED).sql === 'SELECT 1'
    && R.applyFix('SELECT 1', [], MISMATCHED).applied === 0);
  check('a clash that cannot be fixed by adding a suffix is counted as skipped, not mangled',
    R.applyFix('x', [{ kind: 'union', fixCollation: 'X' }], MISMATCHED).skipped === 1
    && R.applyFix('x', [{ kind: 'union', fixCollation: 'X' }], MISMATCHED).applied === 0);
  check('several fixes in one statement all land, back to front so offsets stay valid',
    /edits\.sort\(function \(a, b\) \{ return b\.at - a\.at; \}\)/.test(RULES_SRC));

  const ED = read('public', 'sql-editor-app.js');
  check('the editor offers the fix only in offer_fix mode',
    /userMode\(\) === 'offer_fix'/.test(ED));
  check('the offer is a button, and the dialog shows a Before and an After',
    /Apply collation fix<\/button>/.test(ED) && />Before</.test(ED) && />After</.test(ED));
  check('the SQL is written ONLY by the Apply handler',
    (ED.match(/editor\.value = res\.sql;/g) || []).length === 1);
  check('and Cancel closes without touching it',
    /sqled-collation-cancel'\)\.addEventListener\('click', close\)/.test(ED));
  check('the copy says so in as many words',
    /Your SQL is not changed until you confirm/.test(ED)
    && /Nothing is written until you choose Apply/.test(ED));
}

/* ── 6. The run gate ────────────────────────────────────────────────────── */
section('6. Blocking a run, on all three paths');
{
  const highs = [
    { id: 'dbo.B.Code|case_unique', severity: 'high', issueCode: 'case_unique', object: 'dbo.B.Code', issue: 'case risk' },
    { id: 'dbo.B.Name|codepage', severity: 'high', issueCode: 'codepage', object: 'dbo.B.Name', issue: 'code page risk' },
  ];
  const scanned = (over) => Object.assign({}, MISMATCHED, { lastScan: { at: 'x', findings: highs } }, over || {});
  check('caseRule=block with an unacknowledged High case finding refuses',
    R.gateWith(scanned({ caseRule: 'block' })).ok === false);
  check('ACKNOWLEDGING IT LETS THE RUN THROUGH',
    R.gateWith(scanned({ caseRule: 'block', acknowledged: ['dbo.B.Code|case_unique'] })).ok === true);
  check('with both rules on warn the run is allowed and the warnings are still reported',
    R.gateWith(scanned()).ok === true && R.gateWith(scanned()).warnings.length === 2);

  const PB = read('public', 'project-builder-app.js');
  const AM = read('public', 'agentive_migration.html');
  const RUN = read('azure-function', 'src', 'run-migration.js');
  // Both of these are ORDERING claims, so each is measured inside the run
  // function itself. Measured over the whole file they would find the
  // helper's own definition, which sits above the caller, and read the
  // order backwards — the same trap two older tests in this repo fell into.
  const runProject = PB.slice(PB.indexOf('async function runProject()'));
  check('the Packages run button gates before it starts the run',
    /function cygCollationGate\(whatFor\)/.test(PB)
    && runProject.indexOf("cygCollationGate('this migration')") > 0
    && runProject.indexOf("cygCollationGate('this migration')") < runProject.indexOf('isRunning = true'));
  const startRun = AM.slice(AM.indexOf('async function startRun()'), AM.indexOf('async function cancelRun()'));
  check('Agentive Migration gates before it starts the agent',
    /window\.cygCollation\.gate\(\)/.test(startRun)
    && startRun.indexOf('Collation check blocked this migration') < startRun.indexOf('stopConnectionWatcher()'));
  check('the Task Agent gates before its first statement',
    RUN.indexOf('collationRules.gateWith(collationModel)') > 0
    && RUN.indexOf('collationRules.gateWith(collationModel)') < RUN.indexOf('for (let i = 0; i < stepsToRun.length'));
  check('each refusal names the reasons and where to fix them',
    /Collation check blocked/.test(PB) && /Collation check blocked/.test(AM) && /Collation check blocked/.test(RUN)
    && /Collation card/.test(PB) && /Collation card/.test(AM) && /Collation card/.test(RUN));
  check('A GATE THAT THROWS NEVER STOPS WORK',
    /catch \(e\) \{\s*\/\/ A gate that throws must not be a gate that stops work\.\s*return true;/.test(PB)
    && /catch \(e\) \{ collGate = \{ ok: true, reasons: \[\], warnings: \[\] \}; \}/.test(RUN));
  check('and the page that gates also loads the rules',
    /cygenix-collation-rules\.js/.test(AM) && /cygenix-collation-rules\.js/.test(read('public', 'project-builder.html')));
}

/* ── 7. Settings changed → regenerate ───────────────────────────────────── */
section('7. A script built under other settings says so');
{
  const a = R.settingsStamp(MISMATCHED);
  const b = R.settingsStamp(Object.assign({}, MISMATCHED, { tempTables: 'database_default' }));
  const c = R.settingsStamp(Object.assign({}, MISMATCHED, { strategy: 'source' }));
  check('the stamp changes when any setting that affects output changes',
    a && b && c && a !== b && a !== c && b !== c, [a, b, c].join(' '));
  check('and does NOT change for a setting that does not affect output',
    R.settingsStamp(Object.assign({}, MISMATCHED, { caseRule: 'block' })) === a);
  check('it is short and stable', /^[0-9a-f]{8}$/.test(a) && R.settingsStamp(MISMATCHED) === a);
  check('both copies stamp the same', AR.settingsStamp(MISMATCHED) === a);

  const OM = read('public', 'object-mapping-app.js');
  check('every job-save site records the stamp',
    (OM.match(/collationStamp:/g) || []).length === 3);
  const DA = read('public', 'dashboard-app.js');
  check('a job whose stamp is out of date reads as Regenerate rather than SQL ready',
    /function jobCollationStale\(j\)/.test(DA)
    && /if \(jobCollationStale\(j\)\) return \{ key: 'regenerate', word: 'Regenerate' \};/.test(DA));
  check('and the selected-job column says what changed and what to do',
    /Collation settings changed — regenerate/.test(DA) && /Open mapping/.test(DA));
  check('NOTHING IS REGENERATED AUTOMATICALLY',
    /Never auto-regenerated/.test(DA) && !/autoRegenerate|regenerateAll\(/.test(DA));
  check('a job that predates the feature is not nagged about',
    /A job with no stamp predates the feature/.test(DA));
}

/* ── 8. The restraint that matters most ─────────────────────────────────── */
section('8. Matching collations produce byte-identical SQL');
{
  const before = stagingSql(null);         // the generator with no collation option at all
  const after = stagingSql(MATCHED);       // the same generator, with a matching profile
  check('THE REAL STAGING GENERATOR PRODUCES EXACTLY THE SAME BYTES',
    before === after, JSON.stringify({ before: before.slice(0, 160), after: after.slice(0, 160) }));
  check('no COLLATE appears anywhere in it', after.indexOf('COLLATE') === -1);
  check('and no marker either', after.indexOf(R.MARKER) === -1);
  check('every helper agrees there is nothing to do',
    R.applies(MATCHED) === false && R.needsWork(MATCHED) === false
    && R.collateForTempColumn(MATCHED, 'varchar') === ''
    && R.collateForComparison(MATCHED, COLS[0], COLS[1]).left === '');
  check('a profile with NO settings at all is the same story',
    R.applies(null) === false && R.collateForTempColumn(null, 'varchar') === '');
  check('two columns that already agree with each other are left alone even off-strategy',
    R.collateForComparison(MISMATCHED,
      { side: 'src', schema: 'dbo', table: 'A', column: 'Code', collation: 'Latin1_General_CS_AS' },
      { side: 'tgt', schema: 'dbo', table: 'B', column: 'Code', collation: 'Latin1_General_CS_AS' }).left === '');
}

/* ── 9. The two copies still agree ──────────────────────────────────────── */
section('9. One rule, two copies — still');
check('azure-function/src/collation-rules.js is byte-for-byte public/cygenix-collation-rules.js',
  read('azure-function', 'src', 'collation-rules.js') === RULES_SRC);
check('and every apply case runs the same in both',
  R.APPLY_CASES.every((c) => R[c.fn].apply(null, c.args) === AR[c.fn].apply(null, c.args)));
R.APPLY_CASES.forEach((c) => {
  check(c.name, R[c.fn].apply(null, c.args) === c.want,
    JSON.stringify(R[c.fn].apply(null, c.args)));
});

/* ── 10. Where the fix goes in ──────────────────────────────────────────── */
section('10. Built in, not bolted on');
{
  const sites = [
    ['Export package staging DDL', 'public/dashboard-app.js', /\[\'\+c\.name\+\'\] \'\+c\.type\+cl\+\' \'/],
    ['Project Builder staging DDL', 'public/project-builder-app.js', /c\.type \+ stageColl\(c\) \+ ' NULL'/],
    ['Conversion Templates staging DDL', 'public/cygenix-template-staging.js', /typeFor\(c, dialect\)\.sql \+ cl \+ ' NULL'/],
  ];
  sites.forEach(([name, file, re]) => {
    check(name + ' adds the clause where the column is declared', re.test(read(...file.split('/'))), file);
  });
  check('each one is wrapped, so a missing collation module cannot break a generator',
    [['public', 'dashboard-app.js'], ['public', 'project-builder-app.js'], ['public', 'conversion-templates.html']]
      .every((f) => /try \{[\s\S]{0,200}window\.cygCollation[\s\S]{0,200}catch \(e\) \{ return ''; \}/.test(read(...f))));
  check('and the staging module itself stays pure — it asks, it does not reach for a global',
    !/window\./.test(read('public', 'cygenix-template-staging.js')));
}

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
