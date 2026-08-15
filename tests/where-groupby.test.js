// Tests the Object Mapping editor's WHERE condition list and GROUP BY.
//
// The Options panel used to offer one free-text WHERE box. Conditions are now
// a list joined by AND/OR, and there is a GROUP BY list beside it. The
// combined strings are still written into the hidden #src-where and
// #src-groupby inputs, because those are what every other consumer reads —
// both SQL generators, project-builder's row-by-row runner, the Audit Source
// query, the run-report tooltip and job version history. These tests pin that
// contract: the list is an editor over a string, and one condition must
// combine to exactly the string it would have been before this change.
//
// The module is extracted from public/object_mapping.html and run against a
// DOM stub, so what is under test is the shipped code.
const fs = require('fs');
const vm = require('vm');

const html = (fs.readFileSync(__dirname + '/../public/object_mapping.html', 'utf8') + '\n' + fs.readFileSync(__dirname + '/../public/object-mapping-app.js', 'utf8'));
const start = html.indexOf('// ══════════════════════════════════════════════════════════════════════════\n// WHERE CONDITIONS + GROUP BY');
const end   = html.indexOf('// Also add + Fixed value button to add a literal-only row');
if (start < 0 || end < 0 || end < start) {
  console.log('FAIL: could not locate the WHERE/GROUP BY module in object_mapping.html');
  process.exit(1);
}

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

// Minimal DOM: the module writes into hidden inputs and two note elements,
// and renders rows into two containers.
function build({ columnMapping = [], srcTable = null } = {}) {
  const els = {};
  ['src-where','src-groupby','where-rows','groupby-rows','where-note','groupby-note','src-col-list']
    .forEach(id => { els[id] = { id, value:'', innerHTML:'', style:{ display:'', color:'' } }; });

  const sb = {
    console, JSON, String, Array, Number, RegExp, Boolean,
    $: id => els[id] || null,
    esc: s => String(s == null ? '' : s).replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])),
    escAttr: s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),
    tryAutoGenSQL: () => {},
    renderMappingTable: () => {},
    showStatus: (m) => { sb.__status = m; },
    columnMapping,
    srcTable,
  };
  vm.createContext(sb);
  vm.runInContext(html.slice(start, end), sb);
  const get = expr => vm.runInContext(expr, sb);
  const set = (name, val) => { sb.__tmp = val; vm.runInContext(name + ' = __tmp;', sb); };
  return { sb, els, get, set };
}

// Set the condition list, then read the combined WHERE the generators use.
function whereFrom(conds, opts) {
  const t = build(opts);
  t.set('whereConds', conds);
  t.get('syncClauses()');
  return t.els['src-where'].value;
}
function groupFrom(cols, opts) {
  const t = build(opts);
  t.set('groupByCols', cols);
  t.get('syncClauses()');
  return t.els['src-groupby'].value;
}

console.log('Object Mapping — WHERE conditions and GROUP BY\n');

// ── One condition must be byte-identical to the old single box ───────────
// Saved jobs and their generated SQL depend on this: adding the list must not
// silently rewrite every existing map's WHERE clause.
check('a single condition combines to itself, unwrapped',
  whereFrom([{ connector:'AND', text:"caseno like '%z%'" }]) === "caseno like '%z%'",
  whereFrom([{ connector:'AND', text:"caseno like '%z%'" }]));
check('an empty list produces no WHERE',
  whereFrom([{ connector:'AND', text:'' }]) === '');
check('whitespace-only is not a condition',
  whereFrom([{ connector:'AND', text:'   ' }]) === '');

// ── Multiple conditions ──────────────────────────────────────────────────
check('two conditions are joined with AND and bracketed',
  whereFrom([{ connector:'AND', text:'active = 1' }, { connector:'AND', text:"status = 'O'" }])
    === "(active = 1) AND (status = 'O')");
check('the connector on each row is honoured',
  whereFrom([{ connector:'AND', text:'a = 1' }, { connector:'OR', text:'b = 2' }])
    === '(a = 1) OR (b = 2)');
check('three conditions chain',
  whereFrom([{ connector:'AND', text:'a = 1' }, { connector:'AND', text:'b = 2' }, { connector:'OR', text:'c = 3' }])
    === '(a = 1) AND (b = 2) OR (c = 3)');
check('the first row\'s connector is ignored — nothing precedes it',
  whereFrom([{ connector:'OR', text:'a = 1' }, { connector:'AND', text:'b = 2' }])
    === '(a = 1) AND (b = 2)');

// A blank row in the middle must not leave a dangling AND.
check('a blank row between two conditions is skipped cleanly',
  whereFrom([{ connector:'AND', text:'a = 1' }, { connector:'AND', text:'' }, { connector:'AND', text:'b = 2' }])
    === '(a = 1) AND (b = 2)');
check('blank rows around a single condition leave it unwrapped',
  whereFrom([{ connector:'AND', text:'' }, { connector:'AND', text:'a = 1' }, { connector:'AND', text:'  ' }])
    === 'a = 1');

// ── Normalisation ────────────────────────────────────────────────────────
// Users paste "WHERE x = 1" or a trailing semicolon; either would produce
// "WHERE WHERE x = 1" in the generated SQL.
check('a pasted WHERE keyword is stripped',
  whereFrom([{ connector:'AND', text:'WHERE active = 1' }]) === 'active = 1');
check('the keyword is stripped case-insensitively',
  whereFrom([{ connector:'AND', text:'where active = 1' }]) === 'active = 1');
check('a trailing semicolon is stripped',
  whereFrom([{ connector:'AND', text:'active = 1;' }]) === 'active = 1');
check('a WHERE inside the condition text is left alone',
  whereFrom([{ connector:'AND', text:'id IN (SELECT id FROM t WHERE x = 1)' }])
    === 'id IN (SELECT id FROM t WHERE x = 1)');

// ── GROUP BY ─────────────────────────────────────────────────────────────
check('a bare column name is bracketed', groupFrom(['CaseNo']) === '[CaseNo]');
check('several columns are comma-separated',
  groupFrom(['CaseNo','CaseName']) === '[CaseNo], [CaseName]');
check('an expression is passed through as typed',
  groupFrom(['YEAR([Opened])']) === 'YEAR([Opened])');
check('an already-bracketed name is not double-bracketed',
  groupFrom(['[CaseNo]']) === '[CaseNo]');
check('a qualified name is passed through',
  groupFrom(['s1.CaseNo']) === 's1.CaseNo');
check('blank rows are dropped', groupFrom(['CaseNo','','  ']) === '[CaseNo]');
check('an empty list produces no GROUP BY', groupFrom([]) === '');
check('a trailing comma the user typed is removed',
  groupFrom(['CaseNo,']) === '[CaseNo]');

// ── The un-aggregated column check ───────────────────────────────────────
// GROUP BY makes every un-grouped, un-aggregated column illegal — SQL Server
// rejects the whole statement, so the editor has to name them first.
const MAP = [
  { srcCol:'CaseNo',   tgtCol:'CaseNo' },
  { srcCol:'CaseName', tgtCol:'CaseName' },
  { srcCol:'Amount',   tgtCol:'Amount', literalValue:'SUM([Amount])' },
  { srcCol:'',         tgtCol:'Tenant', literalValue:"'acme'" },
];
let t = build({ columnMapping: MAP });
t.set('groupByCols', ['CaseNo']);
let ung = t.get('ungroupedColumns()');
check('a grouped column is not flagged', !ung.includes('CaseNo'));
check('an un-grouped plain column is flagged', ung.includes('CaseName'), ung.join(','));
check('a column with an aggregate in its fixed value is not flagged',
  !ung.includes('Amount'), ung.join(','));
check('a fixed-value column with no source is not flagged',
  !ung.includes('Tenant'), ung.join(','));
check('only the genuine offender is reported', ung.length === 1, ung.join(','));

t = build({ columnMapping: MAP });
t.set('groupByCols', ['CaseNo','CaseName']);
check('grouping every plain column clears the warning',
  t.get('ungroupedColumns()').length === 0);

// Matching is case-insensitive — GROUP BY [caseno] covers CaseNo.
t = build({ columnMapping: [{ srcCol:'CaseNo', tgtCol:'CaseNo' }] });
t.set('groupByCols', ['caseno']);
check('grouped-column matching ignores case', t.get('ungroupedColumns()').length === 0);

// The warning drives a UI note; it must appear and clear.
t = build({ columnMapping: MAP });
t.set('groupByCols', ['CaseNo']);
t.get('syncClauses()');
check('an offending column raises the GROUP BY note',
  t.els['groupby-note'].style.display === 'block' &&
  /CaseName/.test(t.els['groupby-note'].innerHTML));
t.set('groupByCols', []);
t.get('syncClauses()');
check('clearing GROUP BY hides the note', t.els['groupby-note'].style.display === 'none');

// ── The wand ─────────────────────────────────────────────────────────────
// One click has to turn an invalid grouped statement into a valid one, and it
// must preserve the grouping the user asked for: MAX() on the rest, not
// "add everything to GROUP BY" (which would give a row per distinct
// combination and quietly undo the grouping).
const wandMap = () => ([
  { srcCol:'id',       tgtCol:'id' },
  { srcCol:'CaseNo',   tgtCol:'CaseNo' },
  { srcCol:'CaseName', tgtCol:'CaseName' },
  { srcCol:'',         tgtCol:'Tenant', literalValue:"'acme'" },
]);

t = build({ columnMapping: wandMap() });
t.set('groupByCols', ['CaseNo']);
t.get('syncClauses()');
check('the wand appears in the warning when columns need fixing',
  /gb-wand/.test(t.els['groupby-note'].innerHTML));
check('the wand says how many it will fix',
  /Fix 2 columns/.test(t.els['groupby-note'].innerHTML), t.els['groupby-note'].innerHTML);

t.get('fixGroupBy()');
check('the wand wraps each offending column in MAX()',
  t.sb.columnMapping[0].literalValue === 'MAX([id])' &&
  t.sb.columnMapping[2].literalValue === 'MAX([CaseName])',
  JSON.stringify(t.sb.columnMapping.map(m => m.literalValue)));
check('the wand leaves the grouped column alone',
  t.sb.columnMapping[1].literalValue === undefined);
check('the wand leaves an existing fixed value alone',
  t.sb.columnMapping[3].literalValue === "'acme'");
check('the GROUP BY itself is unchanged — the grouping the user chose is kept',
  t.els['src-groupby'].value === '[CaseNo]');
check('the warning clears after the wand runs',
  t.get('ungroupedColumns()').length === 0);
check('the note drops back to the informational message',
  !/gb-wand/.test(t.els['groupby-note'].innerHTML) &&
  t.els['groupby-note'].style.color === 'var(--text3)');
check('the wand reports what it changed',
  /MAX/.test(t.sb.__status) && /id, CaseName/.test(t.sb.__status), t.sb.__status);

// Running it again must be a no-op, not a double-wrap into MAX(MAX(...)).
const before = JSON.stringify(t.sb.columnMapping);
t.get('fixGroupBy()');
check('running the wand twice changes nothing',
  JSON.stringify(t.sb.columnMapping) === before);

// A joined column is referenced through its alias — MAX([col]) on an
// ambiguous bare name would not compile.
t = build({ columnMapping: [
  { srcCol:'CaseNo', tgtCol:'CaseNo' },
  { srcCol:'orders.total', tgtCol:'Total', fromJoin:'orders', alias:'s1' },
] });
t.set('groupByCols', ['CaseNo']);
t.get('fixGroupBy()');
check('a joined column is wrapped through its alias',
  t.sb.columnMapping[1].literalValue === 'MAX(s1.[total])',
  t.sb.columnMapping[1].literalValue);

// Nothing to fix → the wand must not touch the mapping.
t = build({ columnMapping: [{ srcCol:'CaseNo', tgtCol:'CaseNo' }] });
t.set('groupByCols', ['CaseNo']);
t.get('fixGroupBy()');
check('the wand is a no-op when nothing is wrong',
  t.sb.columnMapping[0].literalValue === undefined);

// ── isAggregateExpr ──────────────────────────────────────────────────────
// The mapping row strikes through the source column for a hard-coded value.
// An aggregate still reads that column, so it must not be struck through.
const isAgg = e => build().get('isAggregateExpr(' + JSON.stringify(e) + ')');
check('MAX is recognised as an aggregate',   isAgg('MAX([a])') === true);
check('SUM is recognised as an aggregate',   isAgg('SUM([a])') === true);
check('COUNT is recognised as an aggregate', isAgg('COUNT(*)') === true);
check('lower-case aggregates are recognised', isAgg('max([a])') === true);
check('an aggregate with spacing is recognised', isAgg('MAX ( [a] )') === true);
check('a plain literal is not an aggregate',  isAgg("'Fixed Param'") === false);
check('a parameter token is not an aggregate', isAgg('@@date') === false);
check('an empty value is not an aggregate',   isAgg('') === false);
check('a column merely named MAXIMUM is not an aggregate',
  isAgg('[MAXIMUM]') === false);

// ── The mixed AND/OR note ────────────────────────────────────────────────
// AND binds tighter than OR, so a mixed list does not read the way the rows
// suggest. The note shows the grouping SQL will actually apply.
t = build();
t.set('whereConds', [{ connector:'AND', text:'a = 1' }, { connector:'OR', text:'b = 2' }]);
t.get('syncClauses()');
check('an all-OR list raises no precedence note',
  t.els['where-note'].style.display === 'none');

t = build();
t.set('whereConds', [{ connector:'AND', text:'a=1' }, { connector:'AND', text:'b=2' }, { connector:'OR', text:'c=3' }]);
t.get('syncClauses()');
check('mixing AND and OR raises the precedence note',
  t.els['where-note'].style.display === 'block');
check('the note shows the real bracketing',
  t.get('bracketPreview()') === '(… AND …) OR …', t.get('bracketPreview()'));

t = build();
t.set('whereConds', [{ connector:'AND', text:'a=1' }, { connector:'AND', text:'b=2' }]);
t.get('syncClauses()');
check('an all-AND list raises no note', t.els['where-note'].style.display === 'none');

// ── Round-tripping a saved job ───────────────────────────────────────────
// A job saved before the list existed carries only the combined string.
t = build();
t.get('loadClausesFromJob(' + JSON.stringify({ srcWhere: "caseno like '%z%'" }) + ')');
check('a legacy job becomes one condition holding its clause',
  t.get('JSON.stringify(whereConds)') === JSON.stringify([{ connector:'AND', text:"caseno like '%z%'" }]));
check('a legacy job re-combines to the identical string',
  t.els['src-where'].value === "caseno like '%z%'");
check('a legacy job has no GROUP BY', t.els['src-groupby'].value === '');

t = build();
t.get('loadClausesFromJob(' + JSON.stringify({
  srcWhere: '(a = 1) AND (b = 2)',
  srcWhereConditions: [{ connector:'AND', text:'a = 1' }, { connector:'AND', text:'b = 2' }],
  srcGroupByCols: ['CaseNo'],
}) + ')');
check('a saved list restores its rows', t.get('whereConds.length') === 2);
check('a restored list re-combines to what was saved',
  t.els['src-where'].value === '(a = 1) AND (b = 2)', t.els['src-where'].value);
check('a saved GROUP BY restores', t.els['src-groupby'].value === '[CaseNo]');

// A job with nothing at all must still leave one empty editable row.
t = build();
t.get('loadClausesFromJob({})');
t.get('renderWhereRows()');
check('a job with no clause still shows one empty row', t.get('whereConds.length') === 1);
check('a job with no clause combines to empty', t.els['src-where'].value === '');

// An OR connector must survive the round trip — it changes the result set.
t = build();
t.get('loadClausesFromJob(' + JSON.stringify({
  srcWhereConditions: [{ connector:'AND', text:'a = 1' }, { connector:'OR', text:'b = 2' }],
}) + ')');
check('a saved OR connector is restored, not defaulted to AND',
  t.els['src-where'].value === '(a = 1) OR (b = 2)', t.els['src-where'].value);

// ── Row editing ──────────────────────────────────────────────────────────
t = build();
t.get('addWhereCond()');
check('adding a condition appends a row', t.get('whereConds.length') === 2);
t.get('setWhereText(0,"a = 1")');
t.get('setWhereText(1,"b = 2")');
check('typing into rows updates the combined clause',
  t.els['src-where'].value === '(a = 1) AND (b = 2)');
t.get('delWhereCond(0)');
check('deleting a row drops its condition',
  t.els['src-where'].value === 'b = 2', t.els['src-where'].value);
t.get('delWhereCond(0)');
check('deleting the last row leaves one empty row, not zero',
  t.get('whereConds.length') === 1 && t.els['src-where'].value === '');

t = build();
t.get('addGroupBy()');
t.get('setGroupBy(0,"CaseNo")');
check('adding and typing a GROUP BY column works',
  t.els['src-groupby'].value === '[CaseNo]');
t.get('delGroupBy(0)');
check('deleting the GROUP BY column clears it',
  t.els['src-groupby'].value === '' && t.get('groupByCols.length') === 0);

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
