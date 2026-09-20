// tests/collation.test.js — collation matching, Stage A: the rules, from Node.
//
// Sep-2026. Two SQL Server databases can hold the same text and disagree
// about what it means, and until now Cygenix handled that in exactly one
// place (the COLLATE DATABASE_DEFAULT in _setupDiffSQL). Stage A detects the
// collations on both sides, grades the differences and saves the decision on
// the connection profile. It changes no SQL anywhere — that is Stage C.
//
// This file pins what a browser cannot usefully pin:
//
//   · the collation-name parser, which is where every severity rule starts.
//     Its first version used one global regular expression and silently
//     dropped the trailing token, so Latin1_General_CI_AS parsed as a
//     DIFFERENT LANGUAGE from Latin1_General_CS_AS. Every case below that
//     names a language exists because of that bug;
//   · the Low/Medium boundary, against the brief's own worked example:
//     Latin1_General_CI_AS against SQL_Latin1_General_CP1_CI_AS is Medium,
//     because a Windows collation and a legacy SQL_ one compare varchar
//     differently; only accent or collation-version differences are Low;
//   · each severity rule in the brief, as a separate case with the inputs
//     that trigger it and one that does not;
//   · that the detection SQL is read-only, that the server query takes no
//     parameters at all, and that a table name NEVER reaches the statement
//     text — it travels as a bound parameter, which is why the Netlify
//     execute action had to learn what the Azure one already knew;
//   · that nothing secret can reach the saved shape, and that the profile is
//     read-modify-written rather than upserted wholesale;
//   · the wiring: the mount, the script order, the tab hook, the storage
//     keys and their classification.
//
// The browser half — the card renders, Detect populates it, Scan grades it,
// Save survives a reload, a repointed connection raises the banner — is
// tests/browser/collation.smoke.js.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

const C = require(path.join(ROOT, 'public', 'cygenix-collation.js'));
const SRC = read('public', 'cygenix-collation.js');
const DASH = read('public', 'dashboard.html');
const APP = read('public', 'dashboard-app.js');
const SYNC = read('public', 'cygenix-cosmos-sync.js');
const FN = read('netlify', 'functions', 'db-connect.js');
const AZ = read('azure-function', 'src', 'index.js');
const INV = read('scripts', 'storage-inventory.js');

console.log('Collation matching — Stage A rules\n');

/* ── 1. The collation-name parser ───────────────────────────────────────── */
const p = C.parseCollation;
check('a Windows collation parses into language, case, accent and lineage',
  p('Latin1_General_CI_AS').language === 'LATIN1_GENERAL'
  && p('Latin1_General_CI_AS').cs === false && p('Latin1_General_CI_AS').accent === true
  && p('Latin1_General_CI_AS').lineage === 'WINDOWS' && p('Latin1_General_CI_AS').version === '');
check('THE TRAILING TOKEN SURVIVES — a case-sensitive and a case-insensitive collation are the SAME language',
  p('Latin1_General_CS_AS').language === p('Latin1_General_CI_AS').language,
  p('Latin1_General_CS_AS').language + ' vs ' + p('Latin1_General_CI_AS').language);
check('a legacy SQL_ collation keeps its code-page token in the language and is marked legacy',
  p('SQL_Latin1_General_CP1_CI_AS').lineage === 'SQL' && p('SQL_Latin1_General_CP1_CI_AS').legacy === true
  && p('SQL_Latin1_General_CP1_CI_AS').language === 'LATIN1_GENERAL_CP1');
check('a versioned collation separates the version from the language',
  p('Latin1_General_100_CI_AS').version === '100' && p('Latin1_General_100_CI_AS').language === 'LATIN1_GENERAL');
check('UTF-8 and supplementary-character tokens are read, not folded into the language',
  p('Latin1_General_100_CI_AS_SC_UTF8').utf8 === true
  && p('Latin1_General_100_CI_AS_SC_UTF8').language === 'LATIN1_GENERAL'
  && p('Latin1_General_CI_AS').utf8 === false);
check('a binary collation is case- and accent-sensitive by definition, with no CS or AS token to read',
  p('Latin1_General_BIN2').bin === true && p('Latin1_General_BIN2').cs === true && p('Latin1_General_BIN2').accent === true
  && p('Latin1_General_BIN').bin === true);
check('a non-Latin language parses too — nothing here is English-only',
  p('Finnish_Swedish_CI_AS').language === 'FINNISH_SWEDISH' && p('Japanese_XJIS_100_CS_AS').cs === true);
check('an empty or unknown name is reported as unknown rather than guessed',
  p('').known === false && p(null).known === false);

/* ── 2. How two collations differ, and how hard ─────────────────────────── */
const D = C.collationDiff;
check('identical collations differ in nothing', D('Latin1_General_CI_AS', 'Latin1_General_CI_AS').length === 0);
check("THE BRIEF'S OWN EXAMPLE grades as a real difference, not a cosmetic one: " +
      'Latin1_General_CI_AS against SQL_Latin1_General_CP1_CI_AS',
  D('Latin1_General_CI_AS', 'SQL_Latin1_General_CP1_CI_AS').length > 0
  && C.isSoftDiff(D('Latin1_General_CI_AS', 'SQL_Latin1_General_CP1_CI_AS')) === false,
  D('Latin1_General_CI_AS', 'SQL_Latin1_General_CP1_CI_AS').join(','));
check('a case difference is named as case', D('Latin1_General_CS_AS', 'Latin1_General_CI_AS').indexOf('case') >= 0);
check('a UTF-8 target against a non-UTF-8 source is named as encoding',
  D('Latin1_General_CI_AS', 'Latin1_General_100_CI_AS_SC_UTF8').indexOf('encoding') >= 0);
check('accent alone, and collation version alone, are the soft pair',
  C.isSoftDiff(D('Latin1_General_CI_AS', 'Latin1_General_CI_AI')) === true
  && C.isSoftDiff(D('Latin1_General_CI_AS', 'Latin1_General_100_CI_AS')) === true);
check('but a soft difference plus a hard one is not soft',
  C.isSoftDiff(D('Latin1_General_CI_AS', 'Latin1_General_100_CS_AI')) === false);
check('an unknown collation on either side yields no claim at all',
  D('', 'Latin1_General_CI_AS').length === 0 && D('Latin1_General_CI_AS', null).length === 0);

/* ── 3. Which types the rules apply to ──────────────────────────────────── */
check('only char, varchar and text can lose a character to a code page — nvarchar cannot',
  C.isNonUnicodeText('varchar') && C.isNonUnicodeText('char') && C.isNonUnicodeText('text')
  && !C.isNonUnicodeText('nvarchar') && !C.isNonUnicodeText('nchar') && !C.isNonUnicodeText('int'));
check('the text types the scan looks at include the Unicode ones',
  C.isTextType('nvarchar') && C.isTextType('varchar') && C.isTextType('sysname') && !C.isTextType('int'));

/* ── 4. The resolved collation, per strategy ────────────────────────────── */
const model = () => Object.assign(C.defaults(), {
  source: Object.assign(C.defaults().source, { dbCollation: 'Latin1_General_CS_AS', codePage: 1252 }),
  target: Object.assign(C.defaults().target, { dbCollation: 'SQL_Latin1_General_CP1_CI_AS', codePage: 1252, tempdbCollation: 'SQL_Latin1_General_CP1_CI_AS' }),
});
check('the default strategy is the target database collation',
  C.defaults().strategy === 'target' && C.resolvedCollation(model()) === 'SQL_Latin1_General_CP1_CI_AS');
check('preserve-source resolves to the source collation',
  C.resolvedCollation(Object.assign(model(), { strategy: 'source' })) === 'Latin1_General_CS_AS');
check('explicit resolves to what was typed, and to nothing when nothing was',
  C.resolvedCollation(Object.assign(model(), { strategy: 'explicit', explicitCollation: 'Finnish_Swedish_CI_AS' })) === 'Finnish_Swedish_CI_AS'
  && C.resolvedCollation(Object.assign(model(), { strategy: 'explicit', explicitCollation: null })) === '');

/* ── 5. The severity rules, one case each ───────────────────────────────── */
const col = (o) => Object.assign({ schema: 'dbo', table: 'T', column: 'C', dataType: 'varchar',
  collation: 'Latin1_General_CI_AS', codePage: 1252, inUniqueKey: false, maxLength: 50 }, o);
const findingsFor = (src, tgt, over) => C.buildFindings(Object.assign({
  model: model(), pairs: [{ job: 'J', usedBy: ['object-mapping'], src: col(src), tgt: col(tgt) }],
}, over || {}));
const only = (list, code) => list.filter((f) => f.issueCode === code);

// HIGH — a non-Unicode target on a different code page.
{
  const f = findingsFor({ codePage: 1252 }, { dataType: 'varchar', codePage: 1250 });
  check('HIGH: a varchar target on a different code page — characters become "?"',
    only(f, C.ISSUE.CODEPAGE).length === 1 && only(f, C.ISSUE.CODEPAGE)[0].severity === 'high'
    && /stored as "\?"/.test(only(f, C.ISSUE.CODEPAGE)[0].issue));
  check('and the recommendation names both ways out: nvarchar, or a collation on the source code page',
    /nvarchar/.test(only(f, C.ISSUE.CODEPAGE)[0].recommendation) && /1252/.test(only(f, C.ISSUE.CODEPAGE)[0].recommendation));
}
check('but an NVARCHAR target on a different code page is NOT a code page finding — it stores UTF-16 whatever the collation says',
  only(findingsFor({ codePage: 1252 }, { dataType: 'nvarchar', codePage: 1250 }), C.ISSUE.CODEPAGE).length === 0);
check('and matching code pages raise nothing',
  only(findingsFor({ codePage: 1252 }, { dataType: 'varchar', codePage: 1252 }), C.ISSUE.CODEPAGE).length === 0);

// HIGH — case-sensitive source into a case-insensitive target, on a unique key.
{
  const f = findingsFor({ collation: 'Latin1_General_CS_AS' }, { collation: 'Latin1_General_CI_AS', inUniqueKey: true });
  check('HIGH: case-sensitive source into a case-insensitive target on a unique or primary key',
    only(f, C.ISSUE.CASE_UNIQUE).length === 1 && only(f, C.ISSUE.CASE_UNIQUE)[0].severity === 'high'
    && /collide/.test(only(f, C.ISSUE.CASE_UNIQUE)[0].issue));
}
check('a binary source counts as case-sensitive for that rule',
  only(findingsFor({ collation: 'Latin1_General_BIN2' }, { collation: 'Latin1_General_CI_AS', inUniqueKey: true }), C.ISSUE.CASE_UNIQUE).length === 1);
check('the SAME case difference on a column that is NOT in a unique key is not High',
  only(findingsFor({ collation: 'Latin1_General_CS_AS' }, { collation: 'Latin1_General_CI_AS', inUniqueKey: false }), C.ISSUE.CASE_UNIQUE).length === 0);
check('and the reverse direction — insensitive source into a sensitive target — is not that risk',
  only(findingsFor({ collation: 'Latin1_General_CI_AS' }, { collation: 'Latin1_General_CS_AS', inUniqueKey: true }), C.ISSUE.CASE_UNIQUE).length === 0);

// MEDIUM — UTF-8 target, non-UTF-8 source, non-Unicode type.
{
  const f = findingsFor({ collation: 'Latin1_General_CI_AS' },
    { dataType: 'varchar', collation: 'Latin1_General_100_CI_AS_SC_UTF8', maxLength: 50 });
  check('MEDIUM: a UTF-8 varchar target counts bytes, so non-ASCII values can overflow the declared length',
    only(f, C.ISSUE.UTF8).length === 1 && only(f, C.ISSUE.UTF8)[0].severity === 'medium'
    && /50 bytes/.test(only(f, C.ISSUE.UTF8)[0].issue));
}
check('an NVARCHAR target is not a UTF-8 length risk',
  only(findingsFor({}, { dataType: 'nvarchar', collation: 'Latin1_General_100_CI_AS_SC_UTF8' }), C.ISSUE.UTF8).length === 0);

// MEDIUM — the collations simply differ. LOW — only accent or version.
check('MEDIUM: mapped columns with different collations raise the conflict-error finding',
  only(findingsFor({ collation: 'Latin1_General_CI_AS' }, { collation: 'SQL_Latin1_General_CP1_CI_AS' }), C.ISSUE.MISMATCH).length === 1
  && /Cannot resolve the collation conflict/.test(only(findingsFor({ collation: 'Latin1_General_CI_AS' }, { collation: 'SQL_Latin1_General_CP1_CI_AS' }), C.ISSUE.MISMATCH)[0].issue));
check('LOW: an accent-only difference loses nothing and is graded low',
  only(findingsFor({ collation: 'Latin1_General_CI_AS' }, { collation: 'Latin1_General_CI_AI' }), C.ISSUE.ACCENT).length === 1
  && only(findingsFor({ collation: 'Latin1_General_CI_AS' }, { collation: 'Latin1_General_CI_AI' }), C.ISSUE.ACCENT)[0].severity === 'low');
check('matching collations raise nothing at all', findingsFor({}, {}).filter((f) => f.object !== 'tempdb').length === 0);
check('a High finding suppresses the Medium mismatch for the same column — one column, one verdict',
  findingsFor({ codePage: 1252 }, { dataType: 'varchar', codePage: 1250, collation: 'SQL_Latin1_General_CP1_CI_AS' })
    .filter((f) => f.object !== 'tempdb').length === 1);

// MEDIUM — tempdb.
{
  const m = model();
  m.target.tempdbCollation = 'Latin1_General_CI_AS';     // differs from the resolved target collation
  const f = C.buildFindings({ model: m, pairs: [] });
  check('MEDIUM: tempdb disagreeing with the resolved collation is its own finding',
    only(f, C.ISSUE.TEMPDB).length === 1 && only(f, C.ISSUE.TEMPDB)[0].severity === 'medium'
    && /Temp tables inherit tempdb/.test(only(f, C.ISSUE.TEMPDB)[0].issue));
  check('and it names the modules that build temp tables',
    only(f, C.ISSUE.TEMPDB)[0].usedBy.length > 0);
}
check('a tempdb that matches the resolved collation raises nothing',
  C.buildFindings({ model: model(), pairs: [] }).length === 0);

/* ── 6. Ordering, acknowledgement and the chip ──────────────────────────── */
{
  const pairs = [
    { job: 'J', usedBy: [], src: col({ collation: 'Latin1_General_CI_AS' }), tgt: col({ column: 'Low', collation: 'Latin1_General_CI_AI' }) },
    { job: 'J', usedBy: [], src: col({ codePage: 1252 }), tgt: col({ column: 'High', dataType: 'varchar', codePage: 1250 }) },
    { job: 'J', usedBy: [], src: col({ collation: 'Latin1_General_CI_AS' }), tgt: col({ column: 'Med', collation: 'SQL_Latin1_General_CP1_CI_AS' }) },
  ];
  const f = C.buildFindings({ model: model(), pairs: pairs });
  check('findings come back most serious first',
    f.map((x) => x.severity).join(',') === 'high,medium,low', f.map((x) => x.severity).join(','));
  check('the chip counts the worst severity present',
    C.chipOf(Object.assign(model(), { lastScan: { at: 'x' } }), f).word === 'High risk (1)');

  const m2 = Object.assign(model(), { lastScan: { at: 'x' }, acknowledged: [f[0].id] });
  const f2 = C.buildFindings({ model: m2, pairs: pairs });
  check('an acknowledged finding is still listed but no longer counted',
    f2.length === 3 && f2.filter((x) => x.acknowledged).length === 1
    && C.summarise(f2).high === 0 && C.summarise(f2).acknowledged === 1);
  check('and the chip drops to the next severity down rather than to Match',
    C.chipOf(m2, f2).word === 'Warnings (2)');
  check('a scan with nothing to report reads as Match, and no scan at all as Not checked',
    C.chipOf(Object.assign(model(), { lastScan: { at: 'x' } }), []).word === 'Match'
    && C.chipOf(model(), []).word === 'Not checked');
  check('the acknowledgement key is stable across a re-scan — object plus issue code, not a row number',
    /^dbo\.T\.High\|codepage$/.test(f[0].id));
}

/* ── 7. The detection SQL ───────────────────────────────────────────────── */
check('the server-level query reads exactly what the brief asks for',
  /SERVERPROPERTY\('Collation'\)/.test(C.SERVER_QUERY) && /DATABASEPROPERTYEX\(DB_NAME\(\),'Collation'\)/.test(C.SERVER_QUERY)
  && /FROM sys\.databases WHERE name = 'tempdb'/.test(C.SERVER_QUERY) && /COLLATIONPROPERTY\(/.test(C.SERVER_QUERY)
  && /SERVERPROPERTY\('ProductVersion'\)/.test(C.SERVER_QUERY) && /DB_NAME\(\) AS database_name/.test(C.SERVER_QUERY));
check('and takes no parameters at all, which is the simplest proof nothing caller-supplied reaches it',
  !/@\w/.test(C.SERVER_QUERY));
const writeWords = /\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|MERGE|EXEC|CREATE)\b/i;
check('every detection query is read-only',
  !writeWords.test(C.SERVER_QUERY) && !writeWords.test(C.VALID_COLLATIONS_QUERY)
  && !writeWords.test(C.columnQuery([{ schema: 'dbo', name: 'T' }]).sql));
{
  const q = C.columnQuery([{ schema: 'dbo', name: 'Ledger' }, { schema: 'fin', name: 'ledger_entry' }]);
  check('the column query reads the columns the brief lists, including the unique-key flag',
    /collation_name/.test(q.sql) && /COLLATIONPROPERTY\(c\.collation_name,'CodePage'\)/.test(q.sql)
    && /in_unique_key/.test(q.sql) && /is_unique = 1 OR i\.is_primary_key = 1/.test(q.sql)
    && /c\.collation_name IS NOT NULL/.test(q.sql));
  check('A TABLE NAME NEVER REACHES THE STATEMENT TEXT — it travels as a bound parameter',
    q.sql.indexOf('Ledger') === -1 && q.sql.indexOf('ledger_entry') === -1 && q.sql.indexOf('fin') === -1
    && q.params.length === 4 && q.params[1].value === 'Ledger' && q.params[3].value === 'ledger_entry',
    q.params.map((x) => x.name + '=' + x.value).join(' '));
  check('and the predicate is one bound pair per table', (q.sql.match(/@s\d+/g) || []).length === 2);
  check('an empty table list produces no query rather than a query that matches everything',
    C.columnQuery([]) === null && C.columnQuery(null) === null);
  check('the scan batches its table list so one call never approaches the lambda cap', C.TABLE_BATCH > 0 && C.TABLE_BATCH <= 100);
}

/* ── 8. The saved shape ─────────────────────────────────────────────────── */
{
  const d = C.defaults();
  check('the saved object has every field the brief names, at version 1',
    d.version === 1 && d.source && d.target && d.fingerprint && d.strategy && d.tempTables
    && d.generatedSqlMode && d.userSqlMode && d.caseRule && d.codePageRule
    && d.columnOverrides && Array.isArray(d.acknowledged) && d.lastScan === null);
  check('the defaults are the ones the brief specifies',
    d.strategy === 'target' && d.tempTables === 'resolved' && d.generatedSqlMode === 'apply'
    && d.userSqlMode === 'offer_fix' && d.caseRule === 'warn' && d.codePageRule === 'warn');
  check('NOTHING IN THE SAVED SHAPE CAN HOLD A CREDENTIAL — the fingerprint is server and database only',
    JSON.stringify(d).toLowerCase().indexOf('password') === -1
    && JSON.stringify(d).toLowerCase().indexOf('connstring') === -1
    && Object.keys(d.source).every((k) => !/pass|secret|key|token|conn/i.test(k)));
  check('the fingerprint is built from a host and a database name and drops the credential',
    C.fingerprintOf('mssql://sa:Sup3rSecret@src.example.internal:1433/SRC') === 'src.example.internal|SRC'
    && C.fingerprintOf('mssql://sa:Sup3rSecret@src.example.internal:1433/SRC').indexOf('Sup3rSecret') === -1
    && C.fingerprintOf('Server=tcp:h,1433;Database=D;User Id=u;Password=p;') === 'h|D');
  check('an Azure function URL fingerprints on its host, with no key',
    C.fingerprintOf('https://app.azurewebsites.net/api/db?code=KEY123') === 'app.azurewebsites.net|');
}
check('a stored object from an older build is filled in rather than thrown on',
  C.normalise(null).strategy === 'target' && C.normalise({ strategy: 'nonsense' }).strategy === 'target'
  && C.normalise({ columnOverrides: 'not an object' }).columnOverrides && typeof C.normalise({}).acknowledged.length === 'number');
check('and a stored object keeps the settings it did have',
  C.normalise({ strategy: 'explicit', explicitCollation: 'X', caseRule: 'block' }).caseRule === 'block'
  && C.normalise({ strategy: 'explicit', explicitCollation: 'X' }).explicitCollation === 'X');

/* ── 9. The profile write ───────────────────────────────────────────────── */
check('the profile is READ-MODIFY-WRITTEN: one field on one profile, then the store is saved',
  /var st = P\.cpLoad\(\);/.test(SRC) && /p\.collation = JSON\.parse\(JSON\.stringify\(model\)\)/.test(SRC)
  && /P\.cpSave\(st\)/.test(SRC) && !/cpSaveProfile/.test(SRC));
check('and updatedAt is bumped, because the sync merge picks the newer copy of a profile by id',
  /p\.updatedAt = Date\.now\(\)/.test(SRC));
check('a missing profile is refused with a sentence that says where to make one, not a stack trace',
  /No connection profile is selected\. Create or select one on Profiles & integrations\./.test(SRC));

/* ── 10. The guards ─────────────────────────────────────────────────────── */
check('Detect and Scan each have an in-flight guard and a three-second minimum interval',
  C.MIN_RUN_INTERVAL_MS === 3000
  && /if \(state\.detecting\) return false;/.test(SRC) && /if \(state\.scanning\) return false;/.test(SRC)
  && (SRC.match(/now - state\.last\w+At < MIN_RUN_INTERVAL_MS/g) || []).length === 2);
check('and each flag is cleared in its own finally, never from a callback that could re-enter it',
  (SRC.match(/state\.detecting = false/g) || []).length === 1
  && (SRC.match(/state\.scanning = false/g) || []).length === 1
  && /\} finally \{[\s\S]{0,200}state\.detecting = false;/.test(SRC)
  && /\} finally \{[\s\S]{0,200}state\.scanning = false;/.test(SRC));
check('every database call is bounded by a timeout', C.CALL_TIMEOUT_MS === 20000 && /withTimeout\(root\.impDbCall\(conn, body\), CALL_TIMEOUT_MS\)/.test(SRC));
check('every step is wrapped and the real message is shown rather than swallowed',
  (SRC.match(/state\.error = \(e && e\.message\) \|\| String\(e\)/g) || []).length >= 3
  && /cx-attn-fail/.test(SRC));
check('the module never logs', !/console\.(log|warn|error|info|debug)\(/.test(SRC));

/* ── 11. SQL Server only ────────────────────────────────────────────────── */
check('a PostgreSQL connection is told, in the brief\'s own words, that this is SQL Server only',
  /Collation matching supports SQL Server connections only/.test(SRC)
  && C.isSqlServer('mssql') === true && C.isSqlServer('azure') === true && C.isSqlServer('postgres') === false);
check('the engine is read from the connection value', C.engineOf('postgres://a:b@c/d') === 'postgres'
  && C.engineOf('Server=x;Database=y;') === 'mssql' && C.engineOf('https://x/api/db') === 'azure');
check('nothing here is specific to one customer\'s schema', !/3E|Elite|Aderant|NxUnit|HBM_/i.test(SRC));

/* ── 12. The clash-point registry ───────────────────────────────────────── */
check('the registry names the modules the inventory found, each with what it does and where it runs',
  C.CLASH_POINTS.length >= 12
  && C.CLASH_POINTS.every((c) => c.id && c.label && /^(generates|user|both)$/.test(c.kind)
      && /^(browser|netlify|azure)$/.test(c.where) && /^(mapping|compare|target|any)$/.test(c.uses)));
check('the modules that consume an Object Mapping column pair are marked as such',
  ['object-mapping', 'validate', 'task-agent', 'export-package', 'conversion-templates', 'preflight', 'project-builder']
    .every((id) => (C.CLASH_POINTS.filter((c) => c.id === id)[0] || {}).uses === 'mapping'));
check('the SQL editor is the one that runs what the user wrote',
  (C.CLASH_POINTS.filter((c) => c.id === 'sql-editor')[0] || {}).kind === 'user');
check('the Task Agent runner is marked as running in Azure, because its settings must come from Cosmos in Stage B',
  (C.CLASH_POINTS.filter((c) => c.id === 'task-agent')[0] || {}).where === 'azure');
check('a finding carries the module labels rather than raw ids',
  C.labelsFor(['object-mapping', 'task-agent']).join(', ') === 'Object Mapping, Task Agent runs');

/* ── 13. The backend ────────────────────────────────────────────────────── */
{
  const exec = FN.slice(FN.indexOf("case 'execute': {"), FN.indexOf("case 'fetch-page': {"));
  check('the Netlify execute action now binds parameters, as the Azure one already did',
    /Array\.isArray\(body\.params\) \? body\.params : \[\]/.test(exec) && /rq\.input\(prm\.name, prm\.value\)/.test(exec)
    && /const r = await rq\.query\(sqlToRun\)/.test(exec));
  check('and the two backends bind them the same way, so a query is portable between the two paths',
    /rq\.input\(prm\.name, prm\.value\)/.test(AZ) && /Array\.isArray\(body\.params\) \? body\.params : \[\]/.test(AZ));
  check('the destructive-statement guard on execute is untouched',
    /Destructive statement blocked/.test(exec));
}
check('a SELECT still gates as a read, so detection needs no write permission',
  /\^\(SELECT\|WITH\|SET\\s\+/.test(read('netlify', 'functions', 'lib', 'rbac.js')));

/* ── 14. The wiring ─────────────────────────────────────────────────────── */
check('the card mounts inside the Database connections tab, below the source and target panels',
  DASH.indexOf('<div id="cyg-collation-mount"></div>') > DASH.indexOf('id="tgt-conn-panel"')
  && DASH.indexOf('<div id="cyg-collation-mount"></div>') < DASH.indexOf('id="conn-tab-restore"'));
check('and no new tab was added to the Connections tab bar',
  (DASH.match(/data-tab="/g) || []).length === 5);
check('the module loads after dashboard-app.js, deferred and cache-busted',
  DASH.indexOf('cygenix-collation.js') > DASH.indexOf('<script src="/dashboard-app.js')
  && /cygenix-collation\.js\?v=[0-9a-f]+" defer/.test(DASH));
check('switching to the Database connections tab mounts it',
  /if \(tab === 'databases' && window\.cygCollation\)/.test(APP)
  && /window\.cygCollation\.init\('cyg-collation-mount'\)/.test(APP));
// The bug this pins: Database connections is the DEFAULT tab, so opening the
// Connections view shows it without anyone clicking a tab, and switchConnTab
// never fires. Mounting only from there left the card invisible to anybody
// who navigated straight to the page — which is everybody.
check('AND SO DOES SIMPLY OPENING THE CONNECTIONS VIEW, because that tab is the default',
  (APP.match(/window\.cygCollation\.init\('cyg-collation-mount'\)/g) || []).length === 2
  && /connRenderEnv\(\);\s*\/\/ The Collation card mounts here as well as in switchConnTab\./.test(APP));
check('the card carries the stable anchor other modules will deep-link to, and an open() that scrolls to it',
  /id="cyg-collation-card"/.test(SRC) && /function openCard\(\)/.test(SRC) && /scrollIntoView/.test(SRC));
check('the header carries the four buttons the brief names',
  /Detect collations/.test(SRC) && />Scan</.test(SRC) && /Export \(Excel\)/.test(SRC) && /Save to profile/.test(SRC));
check('and the collapsed head still shows the title, the chip and the resolved collation',
  /col-title">Collation/.test(SRC) && /id="col-chip"/.test(SRC) && /class="col-resolved"/.test(SRC)
  && /#cyg-collation-card\.closed \.col-body\{ display:none/.test(SRC));
check('the card opens itself when there is an unacknowledged High finding or the connection has moved',
  /if \(s\.high > 0 \|\| driftedSides\(\)\.length\) state\.open = true;/.test(SRC));
check('a repointed connection raises the amber banner in the brief\'s own words',
  /Connection changed since collations were detected/.test(SRC) && /function driftedSides\(\)/.test(SRC));

/* ── 15. Storage keys ───────────────────────────────────────────────────── */
check('the card\'s own view state is a declared key, wrapped in try/catch on both read and write',
  C.UI_KEY === 'cygenix_collation_ui_v1'
  && /function uiRead\(\) \{\s*try \{/.test(SRC) && /function uiWrite\(patch\) \{\s*try \{/.test(SRC));
check('and it is in SYNC_KEYS with a field mapping',
  /'cygenix_collation_ui_v1',/.test(SYNC) && /collation_ui: 'cygenix_collation_ui_v1'/.test(SYNC));
check('and in BACKUP_KEYS, along with the profile store that now carries the collation settings',
  /'cygenix_collation_ui_v1'/.test(APP.slice(APP.indexOf('const BACKUP_KEYS'), APP.indexOf('function quickBackupNow')))
  && /'cygenix_profiles_v1'/.test(APP.slice(APP.indexOf('const BACKUP_KEYS'), APP.indexOf('function quickBackupNow'))));
check('and classified in the storage inventory', /'cygenix_collation_ui_v1':\s*\['C'/.test(INV));

/* ── 16. Export ─────────────────────────────────────────────────────────── */
check('the export has a header row and one row per finding, in the brief\'s column order',
  /\['Severity', 'Object', 'Source collation', 'Target collation', 'Issue', 'Recommendation', 'Where used', 'Acknowledged'\]/.test(SRC));
check('it writes a real workbook when SheetJS is on the page and a CSV Excel opens natively when it is not',
  /var X = root\.XLSX;/.test(SRC) && /X\.writeFile\(wb, name \+ '\.xlsx'\)/.test(SRC) && /'\\ufeff' \+ body/.test(SRC));

/* ── 17. A saved connection value is not a connection ───────────────────────
   Found live on a real profile: the field cpConnValue reads held the word
   "API". The card posted it to the SQL driver and printed "Invalid
   connection string: Could not find server/host in connection string" — a
   true sentence about the wrong problem — while the ambient connection on
   the same page was a perfectly good mssql:// URL that the old fallback
   refused to use, because it only ran when the profile value was EMPTY and
   a junk word is not empty. Then the single try/catch around both sides
   meant that one failure blanked the target as well.

   These cases are that bug, in four parts: the validator, the fallback, the
   engine, and one side not killing the other.                             */
const LC = C.looksLikeConnection;
check('a label word is not a connection, however truthy it is',
  LC('API') === false && LC('direct') === false && LC('  ') === false && LC('') === false
  && LC(null) === false && LC(undefined) === false);
check('AN mssql:// URL IS — it has no semicolons and no server=, and it is what half the real profiles hold',
  LC('mssql://sa:pw@host.example.internal:1433/SRC') === true
  && LC('sqlserver://host/db') === true);
check('so is an Azure Function endpoint, which is how the Azure side of a profile is configured',
  LC('https://cygenix-db-api.azurewebsites.net/api/db') === true
  && LC('http://localhost:7071/api/db') === true);
check('and so is a key=value connection string, in any of the spellings SQL Server accepts',
  LC('Server=tcp:h,1433;Database=D;') === true && LC('Data Source=h;Initial Catalog=D;') === true
  && LC('host=h dbname=d') === true && LC('Address=h;Database=D;') === true);
check('engineOf answers "no engine" for anything it cannot dial, instead of assuming SQL Server',
  C.engineOf('API') === '' && C.engineOf('direct') === '' && C.engineOf('') === ''
  && C.isSqlServer(C.engineOf('API')) === false);
check('and every engine it could read before, it still reads',
  C.engineOf('mssql://sa:pw@h:1433/DB') === 'mssql' && C.engineOf('Server=x;Database=y;') === 'mssql'
  && C.engineOf('https://x/api/db') === 'azure' && C.engineOf('postgres://a:b@c/d') === 'postgres'
  && C.engineOf('host=h dbname=d') === 'postgres');

/* The same four parts, executed rather than read. refresh() and detect()
   need a browser's globals and nothing else, so they are stubbed here: a
   profile whose source connection holds "API" and whose target is an Azure
   Function App, which is exactly the live profile that failed. */
const GOOD_SRC = 'mssql://sa:pw@src.example.internal:1433/SRC';
const AZURE_TGT = 'https://cygenix-db-api.azurewebsites.net/api/db';
const store = {
  profiles: [{ id: 'P1', name: 'FIN to Azure', srcConnId: 'c_src', tgtConnId: 'c_tgt' }],
  settings: { activeProfileId: 'P1' },
};
const conns = [{ id: 'c_src', connString: 'API' }, { id: 'c_tgt', mode: 'azure', fnUrl: AZURE_TGT }];
globalThis.document = { getElementById: () => null };
globalThis.localStorage = { getItem: () => null, setItem: () => {} };
globalThis.CygenixProfiles = {
  cpLoad: () => store,
  cpConnValue: (e) => (e.mode === 'azure' && e.fnUrl ? e.fnUrl : (e.connString || '')),
};
globalThis.sconnGetAll = () => conns;
globalThis.impGetConn = (side) => (side === 'src' ? GOOD_SRC : '');

C.refresh();
const st = C._state;
check('THE JUNK VALUE IS REJECTED AND THE AMBIENT CONNECTION IS USED INSTEAD',
  st.conns.src === GOOD_SRC && st.conns.srcRejected === true && st.conns.srcFallback === true,
  JSON.stringify(st.conns));
check('and the card can tell the two apart: the Azure target came from the profile, unflagged',
  st.conns.tgt === AZURE_TGT && st.conns.tgtRejected === false && st.conns.tgtFallback === false);
check('THE AZURE SIDE IS A SQL SERVER SIDE, so a profile pointing at the Function App is detectable',
  st.engines.src === 'mssql' && st.engines.tgt === 'azure'
  && C.isSqlServer(st.engines.src) && C.isSqlServer(st.engines.tgt));
check('nothing secret leaks into the fingerprint of either side',
  C.fingerprintOf(GOOD_SRC) === 'src.example.internal|SRC'
  && C.fingerprintOf(GOOD_SRC).indexOf('pw') === -1
  && C.fingerprintOf(AZURE_TGT) === 'cygenix-db-api.azurewebsites.net|');

/* One side fails, the other must still be read. */
const dialled = [];
globalThis.impDbCall = async (conn) => {
  dialled.push(conn);
  if (conn === GOOD_SRC) throw new Error('Login failed for user.');
  return { recordset: [{ server_collation: 'Latin1_General_CI_AS', db_collation: 'Latin1_General_CI_AS',
    tempdb_collation: 'Latin1_General_CI_AS', code_page: 1252, database_name: 'TGT', product_version: '16.0.1' }] };
};
(async () => {
  st.lastDetectAt = 0;
  await C.detect();
  check('ONE BROKEN SIDE NO LONGER BLANKS THE OTHER — both were dialled',
    dialled.length === 2 && dialled[1] === AZURE_TGT, JSON.stringify(dialled));
  check('the Azure target detected in full even though the source threw',
    st.model.target.dbCollation === 'Latin1_General_CI_AS' && !!st.model.target.detectedAt
    && st.model.source.dbCollation === '');
  check('and the failure is reported against the side it belongs to, with the real message',
    /^Source: Login failed for user\.$/.test(st.error), st.error);

  /* Neither side usable: each says which fault it has, and neither claims
     the other's. */
  globalThis.impGetConn = () => '';
  store.profiles[0].tgtConnId = 'gone';
  C.refresh();
  st.lastDetectAt = 0;
  await C.detect();
  check('a rejected side and a missing side are two different sentences',
    /Source: the connection saved on this profile is not a usable connection string or endpoint\./.test(st.error)
    && /Target: no connection is configured\./.test(st.error), st.error);
  check('and the card says so per side rather than printing a driver error',
    /isn\\'t a usable connection string or endpoint/.test(SRC)
    && /No ' \+ label\.toLowerCase\(\) \+ ' connection is configured\./.test(SRC));

  /* The guards the brief insists on are untouched by any of this. */
  check('the in-flight guard, the minimum interval and the one-shot rule all survive',
    /if \(state\.detecting\) return false;/.test(SRC)
    && /if \(now - state\.lastDetectAt < MIN_RUN_INTERVAL_MS\) return false;/.test(SRC)
    && /\} finally \{[\s\S]{0,200}state\.detecting = false;/.test(SRC));
  check('the version is bumped, so a stale cached copy is obvious', C.VERSION === 2);

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
