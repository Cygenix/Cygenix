// Tests the Conversion Report's column-mapping enrichment: fixed values,
// @@parameters (with the value they resolved to) and Was/Is rules.
//
// The block is extracted from public/report.html and run against stubs, so
// the logic under test is the shipped code. The data shape mirrors a real
// Cases → cases map: one plain column, one fixed value, one parameter, one
// target with no source at all.
const fs = require('fs');
const vm = require('vm');

const html = fs.readFileSync(__dirname + '/../public/report.html', 'utf8');
const start = html.indexOf('// ── Mapping logic: fixed values, parameters and Was/Is');
const end   = html.indexOf('const mappingConfig =');
if (start < 0 || end < 0 || end < start) {
  console.log('FAIL: could not locate the mapping-logic block in report.html');
  process.exit(1);
}

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

// Build the sandbox fresh per scenario: the block reads `d`, srcFallback and
// tgtTableName from its enclosing scope.
function build(d, opts = {}) {
  const sb = {
    console, JSON, String, Array, Number, RegExp,
    d,
    srcFallback: opts.srcFallback || 'dbo.Cases',
    tgtTableName: opts.tgtTableName || 'dbo.cases',
  };
  vm.createContext(sb);
  vm.runInContext(html.slice(start, end), sb);
  // `const mappingData` is a lexical binding, so it is NOT a property of the
  // context object — it has to be read by evaluating inside the context.
  return { mappingData: vm.runInContext('mappingData', sb) };
}

const byTgt = (rows, col) => rows.find(r => r.tgtCol === col);

console.log('Conversion report — mapping logic\n');

// Mirrors the screenshot: id copied, CaseNo a fixed value, CaseDescription a
// @@date parameter, and a target column with no source.
const d = {
  columnMapping: [
    { srcTable:'dbo.Cases', srcCol:'id',       tgtCol:'id',              tgtType:'INT(10,0)',      transform:'NONE' },
    { srcTable:'dbo.Cases', srcCol:'CaseNo',   tgtCol:'CaseNo',          tgtType:'NVARCHAR(100)',  transform:'NONE', literalValue:"'Fixed Param'" },
    { srcTable:'dbo.Cases', srcCol:'CaseName', tgtCol:'CaseName',        tgtType:'NVARCHAR(100)',  transform:'NONE' },
    { srcTable:'dbo.Cases', srcCol:'CaseDescription', tgtCol:'CaseDescription', tgtType:'NVARCHAR(500)', transform:'NONE', literalValue:'@@date' },
    { srcTable:'dbo.Cases', srcCol:'',         tgtCol:'state',           tgtType:'NVARCHAR(50)',   transform:'NONE' },
  ],
  paramUsage: [
    { paramToken:'@@date', resolved:"'''20260101'''", tgtCol:'CaseDescription' },
  ],
  wasisRules: [
    { srcTable:'dbo.cases', srcField:'casename', oldVal:'A', newVal:'B' },
    { srcTable:'dbo.cases', srcField:'casename', oldVal:'C', newVal:'D' },
    { srcTable:'',          srcField:'',         oldVal:'X', newVal:'Y' },   // applies to all
  ],
};
let rows = build(d).mappingData;

// ── Fixed values ─────────────────────────────────────────────────────────
check('a fixed value appears in the mapping logic',
  byTgt(rows,'CaseNo').logic === "'Fixed Param'", byTgt(rows,'CaseNo').logic);
check('a fixed value is classified as such', byTgt(rows,'CaseNo').valueFrom === 'Fixed value');
check('the raw fixed value is exported too', byTgt(rows,'CaseNo').fixedValue === "'Fixed Param'");

// ── Parameters ───────────────────────────────────────────────────────────
check('a parameter shows the token AND what it resolved to',
  /@@date/.test(byTgt(rows,'CaseDescription').logic) &&
  /20260101/.test(byTgt(rows,'CaseDescription').logic),
  byTgt(rows,'CaseDescription').logic);
check('a parameter is classified as Parameter, not a plain fixed value',
  byTgt(rows,'CaseDescription').valueFrom === 'Parameter');

// ── Plain source columns ─────────────────────────────────────────────────
check('a plain column shows its source column',
  byTgt(rows,'id').logic === '[id]', byTgt(rows,'id').logic);
check('a plain column is classified as Source column',
  byTgt(rows,'id').valueFrom === 'Source column');

// ── Unmapped ─────────────────────────────────────────────────────────────
check('a target with no source is blank in the logic column', byTgt(rows,'state').logic === '');
check('a target with no source is flagged Not mapped',
  byTgt(rows,'state').valueFrom === 'Not mapped');
check('a target with no source is not counted as mapped', byTgt(rows,'state').mapped === 0);

// A fixed value genuinely feeds the target, so it counts as mapped.
check('a fixed-value column counts as mapped', byTgt(rows,'CaseNo').mapped === 1);
check('a parameter column counts as mapped', byTgt(rows,'CaseDescription').mapped === 1);

// ── Was/Is ───────────────────────────────────────────────────────────────
// CaseName has 2 specific rules + 1 wildcard = 3.
check('Was/Is rules are matched to the column they apply to',
  byTgt(rows,'CaseName').wasisCount === 3, String(byTgt(rows,'CaseName').wasisCount));
check('Was/Is is rendered as a readable count',
  byTgt(rows,'CaseName').wasis === '3 rules');
check('a column with no source has no Was/Is', byTgt(rows,'state').wasisCount === 0);
check('the wildcard rule alone still applies to other columns',
  byTgt(rows,'id').wasisCount === 1);

// Table names are stored schema-qualified in some jobs and bare in others.
rows = build({ ...d, wasisRules: [{ srcTable:'cases', srcField:'casename' }] }).mappingData;
check('Was/Is matches a bare table name against a qualified one',
  byTgt(rows,'CaseName').wasisCount === 1);

// A row that carries its own resolved rules wins — it records what actually
// ran, rather than what the current global rule list would imply.
rows = build({
  columnMapping: [{ srcTable:'dbo.Cases', srcCol:'CaseName', tgtCol:'CaseName', wasisCount: 7 }],
  wasisRules: [],
}).mappingData;
check('per-column Was/Is from the run takes precedence',
  byTgt(rows,'CaseName').wasisCount === 7);

// ── No data at all ───────────────────────────────────────────────────────
rows = build({ columnMapping: [{ srcCol:'a', tgtCol:'b' }] }).mappingData;
check('works with no paramUsage or wasisRules present',
  rows[0].logic === '[a]' && rows[0].wasisCount === 0 && rows[0].valueFrom === 'Source column');

// An unresolved parameter must still be shown, not swallowed.
rows = build({ columnMapping: [{ srcCol:'', tgtCol:'x', literalValue:'@@unknown' }], paramUsage: [] }).mappingData;
check('an unresolved parameter is still shown as a parameter',
  rows[0].logic === '@@unknown' && rows[0].valueFrom === 'Parameter');

// ── Legacy `fixedValue` field ────────────────────────────────────────────
// The editor writes literalValue, but the runner's transform pipeline also
// honours fixedValue. A map carrying only the legacy field must not report
// as an unmapped column.
rows = build({ columnMapping: [{ srcCol:'', tgtCol:'CaseNo', fixedValue:"'Fixed Param'" }] }).mappingData;
check('a legacy fixedValue is shown in the mapping logic',
  rows[0].logic === "'Fixed Param'", rows[0].logic);
check('a legacy fixedValue is classified as a fixed value',
  rows[0].valueFrom === 'Fixed value', rows[0].valueFrom);
check('a legacy fixedValue counts as mapped', rows[0].mapped === 1);

rows = build({
  columnMapping: [{ srcCol:'', tgtCol:'CaseDescription', fixedValue:'@@date' }],
  paramUsage: [{ paramToken:'@@date', resolved:"'''20260101'''" }],
}).mappingData;
check('a legacy fixedValue holding a parameter still resolves',
  /@@date/.test(rows[0].logic) && /20260101/.test(rows[0].logic) &&
  rows[0].valueFrom === 'Parameter', rows[0].logic);

// literalValue wins when both are present — it is what the editor writes now.
rows = build({ columnMapping: [{ srcCol:'', tgtCol:'x', literalValue:"'new'", fixedValue:"'old'" }] }).mappingData;
check('literalValue takes precedence over the legacy field',
  rows[0].logic === "'new'", rows[0].logic);


// ── Source filter (WHERE) in the conversion report ──────────────────────────
// A migration with a WHERE clause loads fewer rows than the source table
// holds. That is expected, but the report only carried the clause in a hover
// tooltip — invisible on paper and in the Excel export, so a client reading
// the report saw an unexplained shortfall.
{
  const rpt = fs.readFileSync(__dirname + '/../public/report.html', 'utf8');

  check('the step row shows the filter instead of hiding it in a tooltip',
    /const whereTxt = String\(s\.srcWhere \|\| ''\)/.test(rpt)
    && /route\+whereHtml/.test(rpt));
  check('only migration steps with a clause show one',
    /s\.type === 'migration' && whereTxt/.test(rpt));
  check('a leading WHERE keyword is not doubled up',
    (rpt.match(/replace\(\/\^WHERE\\s\+\/i, ''\)/g) || []).length >= 2);
  check('the clause is escaped — it is user SQL, full of < > and quotes',
    /const escSql = \(t\) =>[\s\S]{0,160}&amp;/.test(rpt)
    && /escSql\(whereTxt\)/.test(rpt));

  check('a summary note explains the shortfall before the numbers',
    /steps-filter-note/.test(rpt) && /applied a source filter/.test(rpt)
    && /never selected for migration/.test(rpt));
  check('the note sits above the table, not after it',
    /const stepsTable = document\.querySelector\('#steps-section table'\)/.test(rpt));
  check('the note escapes the clause too',
    /const escN = \(t\) =>[\s\S]{0,160}&amp;/.test(rpt) && /escN\(String\(x\.srcWhere\)/.test(rpt));
  check('the note is rebuilt, not duplicated, on re-render',
    /const prev = document\.getElementById\('steps-filter-note'\);\s*if \(prev\) prev\.remove\(\);/.test(rpt));
  check('and it pluralises on the step total, not the filtered count',
    /migCount === 1 \? '' : 's'/.test(rpt));

  // Excel: the filter belongs on the same row as the count it explains.
  check('the Excel export gains a Source filter column',
    /'Source filter \(WHERE\)'/.test(rpt));
  check('with a value in every row',
    /String\(s\.srcWhere \|\| ''\)\.trim\(\)\.replace\(\/\^WHERE/.test(rpt));
  check('and the header style range widened for the extra column',
    /styleRange\(wsLog, 0, 0, 0, 13, STYLE\.tableHeader\)/.test(rpt));
  check('and a column width for it',
    /\{wch:46\},\s*\/\/ Source filter \(WHERE\)/.test(rpt));

  // Both report builders must supply the field the renderer now displays.
  const rm = fs.readFileSync(__dirname + '/../azure-function/src/run-migration.js', 'utf8');
  check('the scheduled-run report carries srcWhere per step',
    /srcWhere:\s+step\.srcWhere \|\| ''/.test(rm));
  const pb = fs.readFileSync(__dirname + '/../public/project-builder-app.js', 'utf8');
  check('and so does the in-browser report', /srcWhere:\s+job\.srcWhere \|\| ''/.test(pb));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
