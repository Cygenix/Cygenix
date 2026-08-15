// Tests the project-level conversion report's column-mapping payload.
//
// A project report is assembled in public/project-builder.html by rebuilding
// each column-mapping row field-by-field from the migration steps. That rebuild
// is the bug surface: any field it forgets is simply absent from the report,
// with no error anywhere. A column pinned to a fixed value in the Object
// Mapping editor was reported as a plain source-column copy because
// `literalValue` was not carried across.
//
// The block is extracted from the shipped page and run against stubs.
const fs = require('fs');
const vm = require('vm');

const html = (fs.readFileSync(__dirname + '/../public/project-builder.html', 'utf8') + '\n' + fs.readFileSync(__dirname + '/../public/project-builder-app.js', 'utf8'));
const start = html.indexOf('  const allMappings = [];');
const end   = html.indexOf('  // ── Aggregate Was/Is rules across all migration steps');
if (start < 0 || end < 0 || end < start) {
  console.log('FAIL: could not locate the allMappings block in project-builder.html');
  process.exit(1);
}

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

// `migrationSteps` is the only binding the block reads from its enclosing
// scope. `const allMappings` is lexical, so it is not a property of the
// context object and has to be read by evaluating inside it.
function build(migrationSteps) {
  const sb = { console, JSON, String, Array, Number, migrationSteps };
  vm.createContext(sb);
  vm.runInContext(html.slice(start, end), sb);
  return vm.runInContext('allMappings', sb);
}

const byTgt = (rows, col) => rows.find(r => r.tgtCol === col);

console.log('Project report — column mapping payload\n');

// Mirrors the reported case: a Cases → cases step where CaseNo is pinned to a
// fixed value and CaseDescription to a @@date parameter.
const rows = build([
  {
    srcTable: 'dbo.Cases', tgtTable: 'dbo.cases',
    columnMapping: [
      { srcCol:'id',     tgtCol:'id',     tgtType:'INT',           transform:'NONE' },
      { srcCol:'CaseNo', tgtCol:'CaseNo', tgtType:'NVARCHAR(100)', transform:'NONE', literalValue:"'Fixed Param'" },
      { srcCol:'CaseDescription', tgtCol:'CaseDescription', transform:'NONE', literalValue:'@@date' },
      { srcCol:'Legacy', tgtCol:'Legacy', transform:'NONE', fixedValue:"'Old Field'" },
      { srcCol:'CaseName', tgtCol:'CaseName', transform:'UPPER', transformExpr:null,
        wasisRules:[{ oldVal:'A', newVal:'B' }, { oldVal:'C', newVal:'D' }] },
      { srcCol:'dropme',  tgtCol:'' },   // no target — excluded from the report
    ],
  },
]);

check('rows without a target column are excluded', rows.length === 5, String(rows.length));

// ── The regression this file exists for ──────────────────────────────────
check('a fixed value survives into the report payload',
  byTgt(rows,'CaseNo').literalValue === "'Fixed Param'", byTgt(rows,'CaseNo').literalValue);
check('a @@parameter survives into the report payload',
  byTgt(rows,'CaseDescription').literalValue === '@@date', byTgt(rows,'CaseDescription').literalValue);
check('the legacy fixedValue field survives too',
  byTgt(rows,'Legacy').fixedValue === "'Old Field'", byTgt(rows,'Legacy').fixedValue);

// A plain column must not acquire a phantom literal — report.html treats any
// non-empty literal as overriding the source column.
check('a plain column carries an empty literal, not undefined',
  byTgt(rows,'id').literalValue === '' && byTgt(rows,'id').fixedValue === '');

// ── Everything else the report grid renders ──────────────────────────────
check('the source table comes from the step', byTgt(rows,'id').srcTable === 'dbo.Cases');
check('the target table comes from the step', byTgt(rows,'id').tgtTable === 'dbo.cases');
check('the target type is carried', byTgt(rows,'CaseNo').tgtType === 'NVARCHAR(100)');
check('the transform is carried', byTgt(rows,'CaseName').transform === 'UPPER');
check('a missing transform defaults to NONE', byTgt(rows,'Legacy').transform === 'NONE');

// ── Was/Is ───────────────────────────────────────────────────────────────
check('per-column Was/Is rules are carried',
  Array.isArray(byTgt(rows,'CaseName').wasisRules) && byTgt(rows,'CaseName').wasisRules.length === 2);
check('Was/Is count is derived from the rules when absent',
  byTgt(rows,'CaseName').wasisCount === 2, String(byTgt(rows,'CaseName').wasisCount));
check('a column with no Was/Is reports a zero count',
  byTgt(rows,'id').wasisCount === 0);

// An explicit count recorded by the runner wins over re-deriving it.
const counted = build([{ srcTable:'a', tgtTable:'b',
  columnMapping:[{ srcCol:'c', tgtCol:'c', wasisCount: 7 }] }]);
check('an explicit wasisCount from the run is preserved', counted[0].wasisCount === 7);

// ── Multiple steps ───────────────────────────────────────────────────────
const multi = build([
  { srcTable:'dbo.addresses', tgtTable:'dbo.addresses', columnMapping:[{ srcCol:'id', tgtCol:'id' }] },
  { srcTable:'dbo.Cases', tgtTable:'dbo.cases',
    columnMapping:[{ srcCol:'', tgtCol:'CaseNo', literalValue:"'Fixed Param'" }] },
]);
check('mappings from every migration step are included', multi.length === 2);
check('each row keeps its own step\'s tables',
  multi[0].srcTable === 'dbo.addresses' && multi[1].srcTable === 'dbo.Cases');
check('a fixed value with no source column still carries its literal',
  multi[1].literalValue === "'Fixed Param'" && multi[1].srcCol === '');

// ── Degenerate input ─────────────────────────────────────────────────────
check('a step with no columnMapping is skipped',
  build([{ srcTable:'a', tgtTable:'b' }]).length === 0);
check('no migration steps yields no mappings', build([]).length === 0);

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
