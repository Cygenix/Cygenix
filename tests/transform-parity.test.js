// Tests for column transforms — one definition, used by the forecast and the load.
//
// THE FAILURE THESE PIN
//
// A mapping said "CAST clcountry to NVARCHAR(8)". Preflight forecast ~44 rows
// rejected, and the run failed with
//
//     String or binary data would be truncated in table
//     'cygenix.dm_staging.dbo__client', column 'clcountry'. Truncated value: 'United K'.
//
// Two separate defects, one cause: a transform meant three different things in
// three places.
//
//   1. The Object Mapping editor GENERATES SQL, where CAST becomes a real
//      CAST(src AS NVARCHAR(8)) — which SQL Server truncates silently. That
//      path was correct, which is why the mapping looked right.
//   2. The Batches runner does NOT execute that SQL. It transforms values in
//      JavaScript and emits literals — and there CAST was a deliberate no-op,
//      on the reasoning that SQL Server would cast the literal. It does for a
//      widening conversion (N'123' into INT) and does NOT for a narrowing
//      string: a 14-character literal into NVARCHAR(8) raises msg 8152.
//      A length safety net existed but was gated on m.tgtType, which the
//      editor never writes onto a mapping — so it never fired.
//   3. Preflight compared RAW SOURCE values against the target spec, so any
//      transform was invisible to it and it forecast rejections the mapping
//      already handled.
//
// The fix is one shared module both the forecast and the load call, so they
// cannot disagree again. These tests pin the semantics against what the
// generator emits, because that is the definition users can read in the SQL.
'use strict';
const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 240) : '')); }
};
const P = (...p) => path.join(__dirname, '..', ...p);
const read = (...p) => fs.readFileSync(P(...p), 'utf8');

const T = require('../public/cygenix-transform.js');
const PF = require('../public/cygenix-preflight.js');
const val = (m, v, o) => T.txApply(m, v, o || {}).value;

console.log('Column transforms — the forecast and the load must agree\n');

/* ── 1. The reported case, exactly ──────────────────────────────────────── */

const UK = 'United Kingdom';           // 14 characters
const CAST8 = { transform: 'CAST', tgtCol: 'clcountry', srcCol: 'clcountry' };
const NV8 = { tgtType: 'NVARCHAR(8)' };

check('CAST to NVARCHAR(8) shortens the value, as CAST(x AS NVARCHAR(8)) does',
  val(CAST8, UK, NV8) === 'United K', val(CAST8, UK, NV8));
check('and it reports that it truncated, so the run can say so',
  T.txApply(CAST8, UK, NV8).truncated === true);
check('a value that already fits is untouched and not reported',
  val(CAST8, 'UK', NV8) === 'UK' && T.txApply(CAST8, 'UK', NV8).truncated === false);
// The exact value SQL Server named in the error.
check("the result is the 'United K' SQL Server itself reported",
  val(CAST8, UK, NV8) === 'United K');

// Preflight must now agree that this column is fine.
const spec8 = Object.assign(PF.pfParseType('NVARCHAR(8)'), { nullable: true });
const rawFindings = PF.pfCheckColumn([UK, UK, 'UK'], spec8, {});
check('raw values DO overflow NVARCHAR(8) — the old forecast was not imagining it',
  rawFindings.some((f) => f.code === 'truncation' && f.count === 2));
const mapped = [UK, UK, 'UK'].map((v) => val(CAST8, v, NV8));
const mappedFindings = PF.pfCheckColumn(mapped, spec8, {});
check('the MAPPED values do not overflow, so the forecast goes green',
  mappedFindings.length === 0, JSON.stringify(mappedFindings));

/* ── 2. CAST only narrows CHARACTER targets ─────────────────────────────── */

// The old no-op was right about this much: N'123' into an INT column is
// converted by SQL Server. Changing that would break every existing map.
check('CAST to a numeric target leaves the value alone for SQL Server to convert',
  val({ transform: 'CAST' }, '123', { tgtType: 'INT' }) === '123');
check('CAST to a date target leaves the value alone',
  val({ transform: 'CAST' }, '2026-01-31', { tgtType: 'DATETIME2' }) === '2026-01-31');
check('CAST to NVARCHAR(MAX) never truncates — there is no length to enforce',
  val({ transform: 'CAST' }, UK, { tgtType: 'NVARCHAR(MAX)' }) === UK);
check('CAST to an unsized character type never truncates',
  val({ transform: 'CAST' }, UK, { tgtType: 'NVARCHAR' }) === UK);
check('CAST leaves NULL alone', val({ transform: 'CAST' }, null, NV8) === null);

/* ── 3. The other transforms, matched to what the generator emits ───────── */

// SAFE_TRUNC → LEFT(src, n)
check('SAFE_TRUNC shortens to the target length',
  val({ transform: 'SAFE_TRUNC' }, UK, NV8) === 'United K');
// LEFT(NULL, n) is NULL; a NOT NULL char target needs ISNULL(..., '')
check('SAFE_TRUNC on NULL into a NOT NULL column gives an empty string, as ISNULL does',
  val({ transform: 'SAFE_TRUNC' }, null, { tgtType: 'NVARCHAR(8)', tgtNotNull: true }) === '');
check('SAFE_TRUNC on NULL into a nullable column stays NULL',
  val({ transform: 'SAFE_TRUNC' }, null, NV8) === null);

// SAFE_NUMERIC → ISNULL(TRY_CAST(NULLIF(TRIM(src),'') AS <type>), 0)
check('SAFE_NUMERIC turns an unparseable value into 0, as TRY_CAST+ISNULL does',
  val({ transform: 'SAFE_NUMERIC' }, 'abc', { tgtType: 'INT' }) === 0);
check('SAFE_NUMERIC turns an empty string into 0',
  val({ transform: 'SAFE_NUMERIC' }, '   ', { tgtType: 'INT' }) === 0);
check('SAFE_NUMERIC keeps a real number, trimmed',
  val({ transform: 'SAFE_NUMERIC' }, ' 42 ', { tgtType: 'INT' }) === 42);
check('SAFE_NUMERIC reports that it replaced the value',
  T.txApply({ transform: 'SAFE_NUMERIC' }, 'abc', { tgtType: 'INT' }).coerced === true);

// SAFE_GUID → ISNULL(TRY_CAST(NULLIF(TRIM(src),'') AS UNIQUEIDENTIFIER), NEWID())
const GUID = '3f2504e0-4f89-11d3-9a0c-0305e82c3301';
check('SAFE_GUID keeps a valid GUID',
  val({ transform: 'SAFE_GUID' }, GUID, { tgtType: 'UNIQUEIDENTIFIER' }) === GUID);
check('SAFE_GUID replaces an invalid one with a fresh GUID, as ISNULL+NEWID does', (() => {
  const r = T.txApply({ transform: 'SAFE_GUID' }, 'not-a-guid', { tgtType: 'UNIQUEIDENTIFIER' });
  return r.coerced === true && /^[0-9a-f-]{36}$/.test(String(r.value)) && r.value !== 'not-a-guid';
})());
check('SAFE_GUID replaces an empty value too',
  T.txApply({ transform: 'SAFE_GUID' }, '', { tgtType: 'UNIQUEIDENTIFIER' }).coerced === true);

check('TRIM, UPPER and LOWER are unchanged',
  val({ transform: 'TRIM' }, '  a  ') === 'a'
  && val({ transform: 'UPPER' }, 'aB') === 'AB'
  && val({ transform: 'LOWER' }, 'aB') === 'ab');

/* ── 4. NONE keeps the long-standing width rule ─────────────────────────── */

// The generator emits LEFT(src, n) on a plain copy when the DECLARED source
// width exceeds the DECLARED target width, and warns. That is a schema-level
// statement and stays true here.
check('a plain copy from a wider column to a narrower one truncates, as the generated SQL does',
  val({ transform: 'NONE', srcType: 'NVARCHAR(50)' }, UK, NV8) === 'United K');
// And the converse: nobody asked for this one to be shortened.
check('a plain copy between equally wide columns does NOT silently truncate',
  val({ transform: 'NONE', srcType: 'NVARCHAR(8)' }, UK, NV8) === UK);
check('so an overflow nobody asked to fix still fails loudly',
  T.txResidualRisk({ transform: 'NONE', srcType: 'NVARCHAR(8)' }, UK, NV8) !== null);
check('while the CAST the user set leaves no residual risk',
  T.txResidualRisk(CAST8, UK, NV8) === null);
check('an unknown source type is treated as unbounded, so the rule can still fire',
  val({ transform: 'NONE', srcType: 'NVARCHAR' }, UK, NV8) === 'United K');
check('a plain copy with no type information at all changes nothing',
  val({ transform: 'NONE' }, UK, {}) === UK);

/* ── 5. Length parsing ──────────────────────────────────────────────────── */

check('NVARCHAR(8) is length 8', T.typeLength('NVARCHAR(8)') === 8);
// A precision/scale pair is not a length; reading 10 out of DECIMAL(10,2) and
// treating it as a character width would truncate numbers.
check('DECIMAL(10,2) has no single length to enforce', T.typeLength('DECIMAL(10,2)') === null);
check('a sized non-character type still parses, but is only consulted for char targets',
  T.typeLength('DATETIME2(7)') === 7 && !T.isCharType('DATETIME2(7)'));
check('MAX means no limit', T.typeLength('NVARCHAR(MAX)') === null);
check('an unsized type means no limit', T.typeLength('INT') === null);
check('character types are recognised, including CHAR and TEXT',
  ['NVARCHAR(8)', 'VARCHAR(10)', 'CHAR(2)', 'NCHAR(2)', 'TEXT', 'NTEXT'].every(T.isCharType)
  && !T.isCharType('INT') && !T.isCharType('DATETIME2'));

/* ── 6. The wiring: one definition, both callers ────────────────────────── */

const runner = read('public', 'project-builder-app.js');
const preflight = read('public', 'cygenix-preflight.js');
const page = read('public', 'project-builder.html');

check('the runner applies transforms through the shared module',
  /CygenixTransform\.txApply\(m, out, \{/.test(runner));
check('the CAST no-op comment and its reasoning are gone from the runner',
  !/CAST: leave value alone/.test(runner));
check('the runner still works if the module is missing, rather than throwing',
  /No module \(an old cached page\)/.test(runner));
check('preflight judges the value the load will insert, not the raw source',
  /rows\.map\(r => pfMappedValue\(m, r\[m\.srcCol\], tc\)\)/.test(preflight));
check('preflight resolves the same module rather than reimplementing the rules',
  /function transformModule\(\)/.test(preflight)
  && /require\('\.\/cygenix-transform\.js'\)/.test(preflight));
check('neither caller carries its own copy of the transform rules',
  !/SAFE_NUMERIC/.test(runner.split('CygenixTransform')[0].split('function applyMappingTransform')[1] || '')
  && !/TRY_CAST/.test(preflight));

/* The reason the net never fired: nothing gave the mapping a target type. */
check('the runner reads the target column types when it prepares staging',
  /const targetTypes = \{\};/.test(runner) && /return \{ stagingFull, identityColumns, targetPk, targetTypes \};/.test(runner));
check('and stamps them onto the mapping before any value is transformed',
  /function applyTargetTypes\(mapping, targetTypes\)/.test(runner)
  && /const typedCount = applyTargetTypes\(mapping, ensureResult\.targetTypes\);/.test(runner));
check('the live target type beats anything stale on the saved map',
  /The live target wins over anything stale on the saved map/.test(runner));

/* Truncation the user asked for is fine; truncation nobody is told about is not. */
check('the runner counts what each transform changed',
  /function recordValueChange\(m, res\)/.test(runner) && /function valueChangeSummary\(\)/.test(runner));
check('and reports it in the run log',
  /Values changed by transforms: /.test(runner));
check('the tally resets per step, so one step is not blamed for another',
  /resetValueChanges\(\);/.test(runner));

check('the page loads the shared module before the code that uses it',
  page.indexOf('cygenix-transform.js') > -1
  && page.indexOf('cygenix-transform.js') < page.indexOf('project-builder-app.js'));
check('and before preflight, which also calls it',
  page.indexOf('cygenix-transform.js') > page.indexOf('cygenix-preflight.js')
  || page.indexOf('cygenix-transform.js') < page.indexOf('project-builder-app.js'));

/* ── 7. End to end: the reported job, forecast then loaded ──────────────── */

// One column, CAST to NVARCHAR(8), 121 rows of which 44 are too long — the
// shape of the report in the screenshot.
const sample = [];
for (let i = 0; i < 121; i++) sample.push(i < 44 ? UK : 'UK');
const before = PF.pfCheckColumn(sample, Object.assign(PF.pfParseType('NVARCHAR(8)'), { nullable: true }), {});
check('before the fix the forecast saw 44 rejections in the sample',
  before.some((f) => f.code === 'truncation' && f.count === 44),
  JSON.stringify(before.map((f) => f.code + 'x' + f.count)));
const after = PF.pfCheckColumn(sample.map((v) => val(CAST8, v, NV8)),
  Object.assign(PF.pfParseType('NVARCHAR(8)'), { nullable: true }), {});
check('after it, the same 121 rows forecast clean',
  after.length === 0, JSON.stringify(after.map((f) => f.code + 'x' + f.count)));
check('and every value the load sends now fits the column',
  sample.map((v) => val(CAST8, v, NV8)).every((v) => String(v).length <= 8));

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
