// tests/collation-rules.test.js — collation matching, Stage B: the rules that
// ship twice, and the modules that ask them.
//
// Stage A detected collations and saved a decision. Stage B makes the rest of
// the product ask about it. Two things had to be true for that to be worth
// anything:
//
//   1. the browser and the Function App have to answer identically. A
//      scheduled run that thought a join was fine while the screen said it
//      was not would be worse than no check at all. So the rules are one
//      file that ships twice, driven by ONE set of fixtures that both copies
//      are run against here — "behaviourally identical" as a test rather
//      than a promise;
//
//   2. the scanner has to be quiet. The brief's last verification line is
//      that matching collations produce no false warnings anywhere, and a
//      scanner that guesses at an unqualified column name, or reads an
//      operator inside a string literal, fails that on the first real query.
//      Every negative case below is there because a plausible
//      implementation gets it wrong.
//
// Stage B reports and changes nothing. The assertions at the end pin that:
// no module rewrites SQL, and every caller is wrapped so a linter cannot
// break the screen it lints.
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

const RULES_SRC = read('public', 'cygenix-collation-rules.js');
const R = require(path.join(ROOT, 'public', 'cygenix-collation-rules.js'));
const AR = require(path.join(ROOT, 'azure-function', 'src', 'collation-rules.js'));
const CARD = read('public', 'cygenix-collation.js');
const C = require(path.join(ROOT, 'public', 'cygenix-collation.js'));

console.log('Collation matching — Stage B: shared rules, and who asks them\n');

/* ── 1. The same bytes run in both places ───────────────────────────────── */
section('1. One rule, two copies');

check('azure-function/src/collation-rules.js is byte-for-byte public/cygenix-collation-rules.js',
  read('azure-function', 'src', 'collation-rules.js') === RULES_SRC,
  'edit one and copy it over the other — the deploy zips azure-function/ alone');
check('the server copy loads under Node and exposes the three the brief names',
  typeof AR.resolveWith === 'function' && typeof AR.findClashes === 'function' && typeof AR.gateWith === 'function');
check('it is a plain module: it registers no Azure Function of its own',
  !/app\.(http|timer|storageQueue)\(/.test(RULES_SRC) && !/@azure\/functions/.test(RULES_SRC));
check('and it depends on nothing, so the two copies cannot drift through a dependency',
  !/\brequire\s*\(/.test(RULES_SRC));
check('no function.json folder was added — the app stays on the v4 programming model',
  !fs.existsSync(path.join(ROOT, 'azure-function', 'collation-rules'))
  && !fs.existsSync(path.join(ROOT, 'azure-function', 'src', 'function.json')));
check('host.json still has no functionTimeout', !/functionTimeout/.test(read('azure-function', 'host.json')));

/* ── 2. The shared cases, run against BOTH copies ──────────────────────── */
section('2. The shared cases — every one run twice');

check('the fixtures travel with the rules, so both copies are driven by the same list',
  Array.isArray(R.SHARED_CASES) && R.SHARED_CASES.length >= 18
  && JSON.stringify(R.SHARED_CASES) === JSON.stringify(AR.SHARED_CASES));

R.SHARED_CASES.forEach((c) => {
  const browser = R.findClashes(c.sql, c.context).map((x) => x.kind);
  const server = AR.findClashes(c.sql, c.context).map((x) => x.kind);
  const want = JSON.stringify(c.expect);
  check(c.name + ' — browser', JSON.stringify(browser) === want, 'got ' + JSON.stringify(browser));
  check(c.name + ' — Function App', JSON.stringify(server) === want, 'got ' + JSON.stringify(server));
  check(c.name + ' — both copies agree exactly, field for field',
    JSON.stringify(R.findClashes(c.sql, c.context)) === JSON.stringify(AR.findClashes(c.sql, c.context)));
  if (c.line !== undefined) {
    const got = R.findClashes(c.sql, c.context)[0];
    check(c.name + ' — reported on line ' + c.line, !!got && got.line === c.line, got && got.line);
  }
});

/* ── 2b. Matching two values where there is no SQL ──────────────────────── */
section('2b. The matching cases — every one run twice');

check('the matching fixtures travel with the rules as well',
  Array.isArray(R.MATCH_CASES) && R.MATCH_CASES.length >= 12
  && JSON.stringify(R.MATCH_CASES) === JSON.stringify(AR.MATCH_CASES));

R.MATCH_CASES.forEach((c) => {
  check(c.name + ' — browser', R.keysEqual(c.a, c.b, c.model) === c.equal);
  check(c.name + ' — Function App', AR.keysEqual(c.a, c.b, c.model) === c.equal);
  check(c.name + ' — both copies fold to the same string',
    R.foldKey(c.a, c.model) === AR.foldKey(c.a, c.model)
    && R.foldKey(c.b, c.model) === AR.foldKey(c.b, c.model));
});

/* The property the four call sites depend on: with nothing detected, the
   folder is the identity and `applied` is false, so a screen that wires it
   unconditionally behaves exactly as it did before it existed. */
const NO_MODEL = { source: { dbCollation: '' }, target: { dbCollation: '' }, strategy: 'target' };
const noFold = R.keyFolder(NO_MODEL);
check('WITH NO COLLATION THE FOLDER CHANGES NOTHING, and says it changed nothing',
  noFold.applied === false && noFold.known === false && noFold.label === ''
  && noFold.fold('AbC  ') === 'AbC  ' && noFold.fold(null) === '');
check('and keyFolder(null) is safe, because a caller may have no profile at all',
  R.keyFolder(null).fold('AbC') === 'AbC' && R.keyFolder(null).applied === false);

const ciFold = R.keyFolder({ source: { dbCollation: 'Latin1_General_CI_AS' },
                             target: { dbCollation: 'Latin1_General_CI_AS' }, strategy: 'target' });
check('a folding profile says which rule it applied, in words a report can carry',
  ciFold.applied === true && /ignoring case/.test(ciFold.label)
  && /Latin1_General_CI_AS/.test(ciFold.label), ciFold.label);
const CI_MODEL = { source: { dbCollation: 'Latin1_General_CI_AS' },
                   target: { dbCollation: 'Latin1_General_CI_AS' }, strategy: 'target' };
check('the folder and the one-shot function agree, so a caller can use either',
  ciFold.fold('ACC001') === R.foldKey('ACC001', CI_MODEL)
  && ciFold.equal('ACC001', 'acc001') === R.keysEqual('ACC001', 'acc001', CI_MODEL));
check('a folded key is only ever used for matching — folding is not claimed to ORDER anything',
  !/localeCompare|\.sort\(/.test(RULES_SRC.slice(RULES_SRC.indexOf('function foldKey'),
                                                 RULES_SRC.indexOf('function keysEqual'))));
check('the resolved collation is what drives it, not either side on its own',
  R.keyFolder({ source: { dbCollation: 'Latin1_General_CS_AS' },
                target: { dbCollation: 'Latin1_General_CI_AS' }, strategy: 'target' }).caseInsensitive === true
  && R.keyFolder({ source: { dbCollation: 'Latin1_General_CS_AS' },
                   target: { dbCollation: 'Latin1_General_CI_AS' }, strategy: 'source' }).caseInsensitive === false,
  'strategy target resolves to the target collation; strategy source to the source one');

/* ── 3. What a clash carries ────────────────────────────────────────────── */
section('3. The shape of a finding');

const MODEL = {
  source: { database: 'SRC', dbCollation: 'Latin1_General_CS_AS', tempdbCollation: 'Latin1_General_CS_AS' },
  target: { database: 'TGT', dbCollation: 'SQL_Latin1_General_CP1_CI_AS', tempdbCollation: 'SQL_Latin1_General_CP1_CI_AS' },
  strategy: 'target', tempTables: 'resolved',
};
const COLS = [
  { side: 'src', schema: 'dbo', table: 'A', column: 'Code', collation: 'Latin1_General_CS_AS', dataType: 'varchar' },
  { side: 'tgt', schema: 'dbo', table: 'B', column: 'Code', collation: 'SQL_Latin1_General_CP1_CI_AS', dataType: 'varchar' },
];
const CTX = { model: MODEL, columns: COLS };
const one = R.findClashes('SELECT 1 FROM SRC.dbo.A a JOIN TGT.dbo.B b ON a.Code = b.Code', CTX)[0];

check('every field the brief names is present: line, expression, both collations and a fix',
  !!one && typeof one.line === 'number' && !!one.expression
  && one.leftCollation === 'Latin1_General_CS_AS' && one.rightCollation === 'SQL_Latin1_General_CP1_CI_AS'
  && /^COLLATE SQL_Latin1_General_CP1_CI_AS/.test(one.fix));
check('the expression is the SQL that clashes, not the whole statement',
  !!one && /a\.Code = b\.Code/.test(one.expression) && one.expression.length < 40, one && one.expression);
check('the fix names the side to change — the one that is NOT already the resolved collation',
  !!one && one.fixSide === 'left' && /on a\.Code$/.test(one.fix), one && one.fix);
check('and it says what will happen if nobody does',
  !!one && /Cannot resolve the collation conflict/.test(one.note));
check('a join is told from a plain comparison, because the reader looks for them differently',
  one.kind === 'join'
  && R.findClashes('SELECT 1 FROM SRC.dbo.A a, TGT.dbo.B b WHERE a.Code = b.Code', CTX)[0].kind === 'comparison');
check('with no resolved collation there is no fix invented',
  /Detect the collations first/.test(R.findClashes('SELECT 1 FROM SRC.dbo.A a JOIN TGT.dbo.B b ON a.Code = b.Code',
    { model: { source: MODEL.source, target: { database: 'TGT', dbCollation: '' }, strategy: 'target' }, columns: COLS })[0].fix));
check('a summary line reads as a sentence',
  /^1 collation clash across 1 line — 1 would fail at runtime$/.test(R.summariseClashes([one])),
  R.summariseClashes([one]));

/* ── 4. The scanner keeps quiet ─────────────────────────────────────────── */
section('4. No false warnings — every case a guesser gets wrong');

const quiet = (label, sql, ctx) => check(label, R.findClashes(sql, ctx || CTX).length === 0,
  JSON.stringify(R.findClashes(sql, ctx || CTX).map((x) => x.kind + ':' + x.expression)));

quiet('an operator inside a single-quoted literal', "SELECT 'a.Code = b.Code' FROM SRC.dbo.A a");
quiet('an operator inside an N-prefixed literal', "SELECT N'a.Code = b.Code' FROM SRC.dbo.A a");
quiet('a doubled quote inside a literal does not end it',
  "SELECT 'it''s a.Code = b.Code' FROM SRC.dbo.A a");
quiet('a line comment', '-- a.Code = b.Code\nSELECT 1 FROM SRC.dbo.A a');
quiet('a block comment', '/* a.Code = b.Code */ SELECT 1 FROM SRC.dbo.A a');
quiet('a comparison to a number', 'SELECT 1 FROM SRC.dbo.A a WHERE a.Code = 42');
quiet('a comparison to a parameter', 'SELECT 1 FROM SRC.dbo.A a WHERE a.Code = @p');
quiet('two columns on the SAME side', 'SELECT 1 FROM SRC.dbo.A a WHERE a.Code = a.Code');
quiet('a column this scan has never heard of', 'SELECT 1 FROM SRC.dbo.A a WHERE a.Nope = b.AlsoNope');
quiet('matching collations, in every construct at once',
  'SELECT a.Code FROM SRC.dbo.A a JOIN TGT.dbo.B b ON a.Code = b.Code '
  + 'WHERE a.Code IN (SELECT b.Code FROM TGT.dbo.B b) GROUP BY a.Code '
  + 'UNION ALL SELECT b.Code FROM TGT.dbo.B b',
  { model: { source: { database: 'SRC', dbCollation: 'Latin1_General_CI_AS', tempdbCollation: 'Latin1_General_CI_AS' },
             target: { database: 'TGT', dbCollation: 'Latin1_General_CI_AS', tempdbCollation: 'Latin1_General_CI_AS' },
             strategy: 'target', tempTables: 'resolved' },
    columns: COLS.map((c) => Object.assign({}, c, { collation: 'Latin1_General_CI_AS' })) });
check('an unqualified name that means two different things is dropped, not guessed at',
  R.findClashes('SELECT 1 FROM SRC.dbo.A, TGT.dbo.B WHERE Code = Code', CTX).length === 0);
check('a column that is not a text type cannot clash',
  R.findClashes('SELECT 1 FROM SRC.dbo.A a JOIN TGT.dbo.B b ON a.Code = b.Qty',
    { model: MODEL, columns: COLS.concat([{ side: 'tgt', schema: 'dbo', table: 'B', column: 'Qty', collation: '', dataType: 'int' }]) }).length === 0);
check('a keyword after a table name is not mistaken for its alias',
  R.findClashes('SELECT 1 FROM SRC.dbo.A WHERE 1=1', CTX).length === 0);
check('nothing at all is not an error',
  R.findClashes(null, CTX).length === 0 && R.findClashes('   ', CTX).length === 0
  && R.findClashes('SELECT 1', null).length === 0);

/* ── 5. What it does find ───────────────────────────────────────────────── */
section('5. The constructs the brief lists');

const kinds = (sql, ctx) => R.findClashes(sql, ctx || CTX).map((x) => x.kind);
check('a join predicate', kinds('SELECT 1 FROM SRC.dbo.A a JOIN TGT.dbo.B b ON a.Code = b.Code').indexOf('join') >= 0);
check('a WHERE equality and an inequality',
  kinds('SELECT 1 FROM SRC.dbo.A a, TGT.dbo.B b WHERE a.Code = b.Code').indexOf('comparison') >= 0
  && kinds('SELECT 1 FROM SRC.dbo.A a, TGT.dbo.B b WHERE a.Code <> b.Code').indexOf('comparison') >= 0);
check('a LIKE', kinds('SELECT 1 FROM SRC.dbo.A a, TGT.dbo.B b WHERE a.Code LIKE b.Code').indexOf('comparison') >= 0);
check('an IN subquery', kinds('SELECT 1 FROM SRC.dbo.A a WHERE a.Code IN (SELECT b.Code FROM TGT.dbo.B b)').indexOf('in') >= 0);
check('a UNION, column by column',
  kinds('SELECT a.Code FROM SRC.dbo.A a UNION SELECT b.Code FROM TGT.dbo.B b').indexOf('union') >= 0);
check('a CASE whose arms disagree',
  kinds('SELECT CASE WHEN 1=1 THEN a.Code ELSE b.Code END FROM SRC.dbo.A a, TGT.dbo.B b').indexOf('case') >= 0);
check('GROUP BY, but only in a statement that already crosses sides',
  kinds('SELECT a.Code FROM SRC.dbo.A a JOIN TGT.dbo.B b ON a.Code = b.Code GROUP BY a.Code').indexOf('ordering') >= 0
  && kinds('SELECT a.Code FROM SRC.dbo.A a GROUP BY a.Code').indexOf('ordering') === -1);
{
  const tempCtx = { model: { source: MODEL.source, strategy: 'target', tempTables: 'resolved',
    target: { database: 'TGT', dbCollation: 'SQL_Latin1_General_CP1_CI_AS', tempdbCollation: 'Latin1_General_CS_AS' } }, columns: COLS };
  const t = R.findClashes('CREATE TABLE #s (Code varchar(20), Qty int)', tempCtx);
  check('a temp table declaring a text column with no COLLATE', t.length === 1 && t[0].kind === 'temptable');
  check('and only the text column — an int has no collation to get wrong',
    t.length === 1 && /Code/.test(t[0].left));
  check('the fix follows the temp-table setting',
    /COLLATE SQL_Latin1_General_CP1_CI_AS/.test(t[0].fix)
    && /COLLATE DATABASE_DEFAULT/.test(R.findClashes('CREATE TABLE #s (Code varchar(20))',
        Object.assign({}, tempCtx, { model: Object.assign({}, tempCtx.model, { tempTables: 'database_default' }) }))[0].fix));
  check('a table variable is the same risk and is reported the same way',
    R.findClashes('DECLARE @s TABLE (Code varchar(20))', tempCtx).length === 1);
  check('but nothing is said when tempdb already agrees with the resolved collation',
    R.findClashes('CREATE TABLE #s (Code varchar(20))', CTX).length === 0);
}

/* ── 6. resolve ─────────────────────────────────────────────────────────── */
section('6. resolve — which collation to write');

const withOverride = Object.assign({}, MODEL, {
  columnOverrides: { 'dbo.B.Code': { collation: 'Latin1_General_BIN2', note: 'sorted as bytes downstream' } },
});
check('with no override, the strategy decides',
  R.resolveWith(MODEL, 'tgt', 'dbo', 'B', 'Code') === 'SQL_Latin1_General_CP1_CI_AS'
  && R.resolveWith(Object.assign({}, MODEL, { strategy: 'source' }), 'tgt', 'dbo', 'B', 'Code') === 'Latin1_General_CS_AS');
check('AN OVERRIDE BEATS THE STRATEGY, for that one column only',
  R.resolveWith(withOverride, 'tgt', 'dbo', 'B', 'Code') === 'Latin1_General_BIN2'
  && R.resolveWith(withOverride, 'tgt', 'dbo', 'B', 'Other') === 'SQL_Latin1_General_CP1_CI_AS');
check('and it matches however the person capitalised it',
  R.resolveWith(withOverride, 'tgt', 'DBO', 'b', 'code') === 'Latin1_General_BIN2');
check('an explicit strategy resolves to what was typed',
  R.resolveWith(Object.assign({}, MODEL, { strategy: 'explicit', explicitCollation: 'Finnish_Swedish_CI_AS' }),
    'tgt', 'dbo', 'B', 'Code') === 'Finnish_Swedish_CI_AS');
check('both copies resolve the same',
  AR.resolveWith(withOverride, 'tgt', 'dbo', 'B', 'Code') === R.resolveWith(withOverride, 'tgt', 'dbo', 'B', 'Code'));

/* ── 7. gate ────────────────────────────────────────────────────────────── */
section('7. gate — may a job run?');

const highs = [
  { id: 'dbo.B.Code|case_unique', severity: 'high', issueCode: 'case_unique', object: 'dbo.B.Code', issue: 'case risk' },
  { id: 'dbo.B.Name|codepage', severity: 'high', issueCode: 'codepage', object: 'dbo.B.Name', issue: 'code page risk' },
];
const scanned = (over) => Object.assign({}, MODEL, { lastScan: { at: 'x', findings: highs } }, over || {});

check('with both rules on warn the run is allowed, and the warnings are still reported',
  R.gateWith(scanned()).ok === true && R.gateWith(scanned()).warnings.length === 2
  && R.gateWith(scanned()).reasons.length === 0);
check('caseRule=block refuses, and names the column and the reason',
  R.gateWith(scanned({ caseRule: 'block' })).ok === false
  && /dbo\.B\.Code/.test(R.gateWith(scanned({ caseRule: 'block' })).reasons[0]));
check('codePageRule=block refuses on its own finding, independently',
  R.gateWith(scanned({ codePageRule: 'block' })).ok === false
  && R.gateWith(scanned({ codePageRule: 'block' })).reasons.length === 1
  && /dbo\.B\.Name/.test(R.gateWith(scanned({ codePageRule: 'block' })).reasons[0]));
check('ACKNOWLEDGING A FINDING LETS THE RUN THROUGH — that is what acknowledging means',
  R.gateWith(scanned({ caseRule: 'block', acknowledged: ['dbo.B.Code|case_unique'] })).ok === true);
check('a Medium finding never blocks, whatever the rules say',
  R.gateWith(Object.assign({}, MODEL, { caseRule: 'block', codePageRule: 'block',
    lastScan: { at: 'x', findings: [{ id: 'x|mismatch', severity: 'medium', issueCode: 'mismatch', object: 'x' }] } })).ok === true);
check('no scan at all allows the run and says so, rather than blocking on ignorance',
  R.gateWith(MODEL).ok === true && R.gateWith(MODEL).checked === false);
check('both copies gate the same',
  JSON.stringify(AR.gateWith(scanned({ caseRule: 'block' }))) === JSON.stringify(R.gateWith(scanned({ caseRule: 'block' }))));

/* ── 8. The browser API ─────────────────────────────────────────────────── */
section('8. What the rest of the product calls');

check('window.cygCollation exposes the four the brief names',
  ['settings', 'resolve', 'findClashes', 'gate'].every((k) => typeof C[k] === 'function'));
check('plus the helpers that let a module report a clash in three lines',
  ['lint', 'bannerHtml', 'renderBanner', 'badgeFor', 'summaryLine'].every((k) => typeof C[k] === 'function'));
check('the card re-exports the shared rules rather than keeping a second copy',
  /var Rules = \(typeof module === 'object' && module\.exports\)/.test(CARD)
  && /require\('\.\/cygenix-collation-rules\.js'\)/.test(CARD)
  && /root\.CygenixCollationRules/.test(CARD)
  && C.parseCollation === R.parseCollation && C.defaults === R.defaults);
check('and refuses to define itself if the rules did not load, rather than reporting nothing',
  /throw new Error\('cygenix-collation\.js needs cygenix-collation-rules\.js, which must load first\.'\)/.test(CARD));
check('findClashes never throws, whatever it is handed',
  C.findClashes(undefined, undefined).length === 0 && C.findClashes('SELECT 1', { model: null }).length === 0);
check('the banner links to the Collation card and says nothing was changed',
  /Nothing has been changed\./.test(C.bannerHtml([one]))
  && C.bannerHtml([one]).indexOf('/dashboard#goto=connections/databases') > 0);
check('a clash that would fail at runtime is drawn in the fail colour, a softer one in warn',
  /cx-attn-fail/.test(C.bannerHtml([one]))
  && /cx-attn-warn/.test(C.bannerHtml([Object.assign({}, one, { severity: 'low' })])));
check('an empty list draws nothing at all', C.bannerHtml([]) === '' && C.bannerHtml(null) === '');
check('the scan now records the columns it read, so the linter can be precise',
  /columns: scannedColumns,/.test(CARD) && /var scannedColumns = \[\];/.test(CARD));

/* ── 9. Who asks, and where ─────────────────────────────────────────────── */
section('9. Every clash point in the inventory');

const WIRED = [
  ['Object Mapping', 'public/object-mapping-app.js', /omCollationBadge\(m\)/, /window\.cygCollation\.badgeFor/],
  ['Object Mapping summary', 'public/object-mapping-app.js', /omRenderCollationSummary\(\)/, /window\.cygCollation\.summaryLine/],
  ['Object Mapping SQL panel', 'public/object-mapping-app.js', /omLintSQL\(/, /renderBanner\('om-collation-banner'/],
  /* mapper.html is the older Object Mapping screen. It is still routed and
     still generates cross-database INSERT…SELECT, and it was left out of
     the Stage B wiring — so the part of the product that exists to say
     "these two columns will not compare cleanly" was silent on a page whose
     whole job is mapping columns between two databases. */
  ['Mapper badges', 'public/mapper.html', /mapCollationBadge\(m\)/, /window\.cygCollation\.badgeFor/],
  ['Mapper summary', 'public/mapper.html', /mapRenderCollationSummary\(\)/, /window\.cygCollation\.summaryLine/],
  ['Mapper SQL panel', 'public/mapper.html', /mapLintSQL\(/, /renderBanner\('map-collation-banner'/],
  ['SQL editor', 'public/sql-editor-app.js', /sqlEdLintCollation\(sql\)/, /window\.cygCollation\.renderBanner/],
  ['Validate', 'public/dashboard-app.js', /_cygLintCollation\('validate-collation-banner'/, /window\.cygCollation\.lint/],
  ['Export package', 'public/dashboard-app.js', /_cygLintCollation\('pkg-collation-banner'/, /window\.cygCollation\.bannerHtml/],
  ['Migration runs', 'public/project-builder-app.js', /pbLintCollation\(sql, context\)/, /window\.cygCollation\.lint/],
  ['Preflight', 'public/project-builder-app.js', /report\.jobs \|\| \[\]\)\.map\(j => j && j\.probeSql\)/, /window\.cygCollation\.bannerHtml/],
  ['Conversion Templates', 'public/conversion-templates.html', /renderBanner\('ct-collation-banner'/, /window\.cygCollation/],
  ['Balancing', 'public/balancing.html', /balCollationBanner\(/, /window\.cygCollation\.lint/],
  ['Assurance', 'public/assurance.html', /asCollationBanner\(sql\)/, /window\.cygCollation\.lint/],
  ['Task Agent runs', 'azure-function/src/run-migration.js', /lintCollation\(step\.insertSQL \|\| step\.sql, collationModel/, /collationRules\.findClashes/],
];
WIRED.forEach(([name, file, callRe, apiRe]) => {
  const src = read(...file.split('/'));
  check(name + ' asks', callRe.test(src) && apiRe.test(src), file);
});

const MOUNTS = [
  ['public/object_mapping.html', 'om-collation-banner'], ['public/object_mapping.html', 'om-collation-summary'],
  ['public/sql-editor.html', 'sqled-collation-banner'],
  ['public/dashboard.html', 'validate-collation-banner'], ['public/dashboard.html', 'pkg-collation-banner'],
  ['public/project-builder.html', 'pb-collation-banner'],
  ['public/conversion-templates.html', 'ct-collation-banner'],
  ['public/mapper.html', 'map-collation-banner'], ['public/mapper.html', 'map-collation-summary'],
];
MOUNTS.forEach(([file, id]) => {
  check('and ' + file.replace('public/', '') + ' has a mount for it that survives a re-render: #' + id,
    new RegExp('id="' + id + '"').test(read(...file.split('/'))));
});
check('every page that asks also loads the rules, before the card that needs them',
  ['object_mapping', 'mapper', 'sql-editor', 'project-builder', 'conversion-templates', 'balancing', 'assurance', 'dashboard']
    .every((p) => {
      const h = read('public', p + '.html');
      const r = h.indexOf('cygenix-collation-rules.js'), c = h.indexOf('cygenix-collation.js?');
      const c2 = c === -1 ? h.indexOf('cygenix-collation.js') : c;
      return r > 0 && c2 > 0 && r < c2;
    }));

/* ── 10. Stage B reports. It does not act. ──────────────────────────────── */
section('10. Reporting only — Stage B changes no SQL');

// Stage C changed this rule, and the assertion changing with it is the
// point: the editor IS written now, but only from inside the Apply handler
// of the before/after dialog. Linting still writes nothing.
{
  const ED = read('public', 'sql-editor-app.js');
  const lintFn = ED.slice(ED.indexOf('function sqlEdLintCollation'), ED.indexOf('function sqlEdOfferCollationFix'));
  check('linting rewrites nothing — the offer is a button, not an edit',
    !/editor\.value\s*=/.test(lintFn));
  check('and the one write to the editor sits inside the Apply handler, after a preview',
    (ED.match(/editor\.value = res\.sql;/g) || []).length === 1
    && /sqled-collation-apply'\)\.addEventListener\('click', \(\) => \{\s*editor\.value = res\.sql;/.test(ED));
  check('Cancel closes without touching it',
    /sqled-collation-cancel'\)\.addEventListener\('click', close\)/.test(ED)
    && /const close = \(\) => \{ try \{ wrap\.remove\(\)/.test(ED));
}
check('no generator rewrites finished SQL — the clause goes in where the SQL is built',
  !/insertSQL\s*=\s*.*COLLATE/.test(read('public', 'object-mapping-app.js')));
check('the generators still emit no COLLATE of their own — that is Stage C',
  !/COLLATE/.test(read('public', 'object-mapping-app.js').slice(
    read('public', 'object-mapping-app.js').indexOf('function generateSingleSQL'),
    read('public', 'object-mapping-app.js').indexOf('function generateOTMSQL'))));
// Stage C added the gate, so this rule changed with it: the runner still
// reports every clash it lints, and now also refuses a run when — and only
// when — an operator set a rule to block and a matching High finding is
// unacknowledged.
{
  const RUN_SRC = read('azure-function', 'src', 'run-migration.js');
  check('the Task Agent still lints every statement without refusing on that alone',
    /Reporting only/.test(RUN_SRC) && /lintCollation\(step\.insertSQL \|\| step\.sql/.test(RUN_SRC));
  check('and the gate refuses BEFORE the first statement rather than part way through',
    RUN_SRC.indexOf('collationRules.gateWith(collationModel)') > 0
    && RUN_SRC.indexOf('collationRules.gateWith(collationModel)') < RUN_SRC.indexOf('for (let i = 0; i < stepsToRun.length'));
  check('a gate that throws does not stop work',
    /catch \(e\) \{ collGate = \{ ok: true, reasons: \[\], warnings: \[\] \}; \}/.test(RUN_SRC));
}
check('EVERY CALLER IS WRAPPED — a linter cannot break the screen it lints',
  [['public', 'object-mapping-app.js'], ['public', 'sql-editor-app.js'], ['public', 'project-builder-app.js'],
   ['public', 'balancing.html'], ['public', 'assurance.html'], ['public', 'conversion-templates.html']]
    .every((f) => {
      const src = read(...f);
      const calls = (src.match(/window\.cygCollation\.\w+\(/g) || []).length;
      const tries = (src.match(/try \{[\s\S]{0,700}?window\.cygCollation/g) || []).length;
      return calls === 0 || tries > 0;
    }));
check('and every caller checks the module is there at all',
  (read('public', 'object-mapping-app.js').match(/if \(!window\.cygCollation/g) || []).length >= 2
  && /if \(!window\.cygCollation \|\| !window\.cygCollation\.renderBanner\)/.test(read('public', 'sql-editor-app.js')));

/* ── 11. The server reads Cosmos, not a browser ─────────────────────────── */
section('11. The Function App gets its settings from the record, not the page');

const RUN = read('azure-function', 'src', 'run-migration.js');
check('the runner reads the profile store out of the projects document',
  /getCosmosContainer\('projects'\)\.item\(userId, userId\)\.read\(\)/.test(RUN)
  && /resource\.connection_profiles/.test(RUN));
check('and picks the active profile, falling back to whichever one carries settings',
  /settings\.activeProfileId/.test(RUN) && /x\.status === 'active' && x\.collation/.test(RUN));
// The word appears once, in the comment explaining where the browser keeps
// the same settings. What must not appear is an ACCESS: there is no
// localStorage in a Function App, so a read would be a crash rather than a
// wrong answer, and the comment is the thing that stops someone adding one.
check('nothing on that path reads localStorage — there is none to read',
  !/localStorage\s*\.\s*\w/.test(RUN) && /localStorage/.test(RUN));
check('the settings are read once per run, not once per statement',
  /_collationCache/.test(RUN) && /COLLATION_CACHE_MS/.test(RUN));
check('a statement too large to be worth scanning is skipped rather than parsed',
  /COLLATION_LINT_MAX_CHARS/.test(RUN));
check('a missing document is a user who never saved settings, not a fault',
  /e\.code !== 404/.test(RUN));
check('run-migration requires the twin that ships in azure-function/src',
  /require\('\.\/collation-rules'\)/.test(RUN)
  && fs.existsSync(path.join(ROOT, 'azure-function', 'src', 'collation-rules.js')));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
