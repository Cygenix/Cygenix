// tests/schema-export.test.js — getting the schema diagram out of the browser.
//
// Requested: a print-to-PDF button for the diagram, and "other useful ways to
// export this which include tables and columns (ie Excel, text)".
//
// Three things make this worth pinning rather than eyeballing once:
//
//   * SCOPE. The report came from a database showing "12,216 tables hidden by
//     filters" with 28 on the canvas. An export of 28 tables that does not say
//     so will be read as the estate — by a colleague, months later, with no
//     way to tell. Every file carries the count, the hidden count and the
//     date, and that is asserted per format rather than trusted to the one
//     function that writes it.
//   * EXCEL. A CSV cell beginning = + - @ is executed as a formula on open.
//     These cells are database identifiers this product did not choose.
//   * THE PICTURE. The exported SVG is drawn from a positioned model rather
//     than from the DOM, so the geometry has to come from the page or the two
//     will drift. The checks here are that it consumes what it is given and
//     crops nothing — a viewBox drawn to the boxes alone clips the bezier
//     control points, which is the classic way an ERD export loses its lines.
'use strict';

const fs = require('fs');
const path = require('path');
const X = require('../public/cygenix-schema-export.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

console.log('Schema export — the diagram, off the screen\n');

const AT = Date.UTC(2026, 8, 14, 10, 0, 0);
const model = () => ({
  database: 'Conv_DM', side: 'tgt', generatedAt: AT,
  hiddenCount: 12216, focusDepth: null,
  geom: { boxW: 210, rowH: 15, headH: 25, maxRows: 9 },
  pos: { 'dbo.TimeType': { x: 0, y: 0 }, 'dbo.Timecard': { x: 300, y: 40 } },
  tables: [
    { schema: 'dbo', name: 'TimeType', key: 'dbo.TimeType', kind: 'table', rowCount: 15,
      primaryKeys: ['Code'], colour: '#7c5cff',
      columns: [{ name: 'Code', type: 'NVARCHAR(10)', nullable: false, ordinal: 1 },
                { name: '=Desc', type: 'NVARCHAR(200)', nullable: true, ordinal: 2, default: 'N/A' }],
      shownColumns: [{ name: 'Code', type: 'NVARCHAR(10)', pk: true, fk: false },
                     { name: '=Desc', type: 'NVARCHAR(200)', pk: false, fk: false }],
      moreCount: 0 },
    { schema: 'dbo', name: 'Timecard', key: 'dbo.Timecard', kind: 'table', rowCount: 573106,
      primaryKeys: ['Id'], colour: '#3aa6b6', columns: null,
      shownColumns: [{ name: 'TimeType', type: '', pk: false, fk: true }], moreCount: 0 },
  ],
  edges: [
    { from: 'dbo.Timecard', to: 'dbo.TimeType', fromColumn: 'TimeType', toColumn: 'Code',
      name: 'FK_Timecard_TimeType', inferred: false, a: { x: 405, y: 72 }, b: { x: 105, y: 32 } },
    { from: 'dbo.Timecard', to: 'dbo.TimeType', fromColumn: 'AltType', toColumn: 'Code',
      name: '', inferred: true, tier: 'likely', confidence: 0.82, orphanRate: 0.0031,
      a: { x: 405, y: 90 }, b: { x: 105, y: 32 } },
  ],
});

/* ── 1. Scope, on every file ────────────────────────────────────────────── */

const ALL = {
  columns: X.columnsCsv(model()),
  tables: X.tablesCsv(model()),
  relationships: X.relationshipsCsv(model()),
  markdown: X.dictionaryMarkdown(model()),
  svg: X.diagramSvg(model()),
};
const noScope = Object.entries(ALL).filter(([, s]) => !/12,216 hidden by filters/.test(s)).map(([k]) => k);
check('every format states how many tables were LEFT OUT, not just how many are in',
  noScope.length === 0, noScope.join(', '));
const noDate = Object.entries(ALL).filter(([, s]) => !/2026-09-14/.test(s)).map(([k]) => k);
check('and every format is dated, so a stale copy is identifiable', noDate.length === 0, noDate.join(', '));
const noDb = Object.entries(ALL).filter(([, s]) => !/Conv_DM/.test(s)).map(([k]) => k);
check('and names the database', noDb.length === 0, noDb.join(', '));
check('the scope sentence says which side of the migration it is',
  /# side,target/.test(ALL.columns) && /side: target/i.test(ALL.markdown)
  && /· target/.test(ALL.svg));
check('a focused diagram says so too, because it is a third kind of subset',
  /focused to 2 hops/.test(X.columnsCsv(Object.assign(model(), { focusDepth: 2 }))));

/* ── 2. Excel ───────────────────────────────────────────────────────────── */

const cell = X.__core.csvCell;
check('a value that Excel would run as a formula is neutralised, not just quoted',
  cell('=cmd|calc') === "'=cmd|calc" && cell('+1') === "'+1"
  && cell('-x') === "'-x" && cell('@SUM') === "'@SUM",
  [cell('=cmd|calc'), cell('+1')].join(' / '));
check('the real column named =Desc survives into the file that way',
  /'=Desc/.test(ALL.columns));
check('an ordinary value is untouched', cell('Code') === 'Code' && cell('NVARCHAR(10)') === 'NVARCHAR(10)');
check('commas, quotes and newlines are still quoted and doubled',
  cell('a,b') === '"a,b"' && cell('say "hi"') === '"say ""hi"""' && cell('a\nb') === '"a\nb"');
// Windows Excel reads a BOM-less UTF-8 CSV as the system code page, which
// turns any non-ASCII identifier into mojibake.
const csvs = ['columns', 'tables', 'relationships'];
check('every CSV starts with a UTF-8 BOM so Excel does not mangle non-ASCII names',
  csvs.every(k => ALL[k].charCodeAt(0) === 0xFEFF));
// Provenance goes in LEADING # rows, which is the convention every reader
// that has an opinion supports (pandas comment='#', R, csvkit). Excel shows
// them as five rows to delete, which is the cost of the scope statement
// travelling with the file — and the dialog says so before you download.
const firstDataRow = (k) => ALL[k].replace(/^﻿/, '').split('\n')
  .filter(l => l && !l.startsWith('#'))[0];
check('the provenance is comment rows, and the real header is the first row that is not one',
  firstDataRow('columns').startsWith('schema,table,kind,')
  && firstDataRow('tables').startsWith('schema,table,kind,')
  && firstDataRow('relationships').startsWith('origin,constraint,'),
  csvs.map(firstDataRow).join(' | '));
check('and the dialog warns that they are there, rather than surprising Excel',
  /five commented lines/.test(read('public', 'schema_explorer.html')));

/* ── 3. Columns: the dictionary ─────────────────────────────────────────── */

const colLines = ALL.columns.split('\n').filter(l => l && !l.startsWith('#') && !l.startsWith('schema,'));
check('one row per column', colLines.filter(l => /,Code,/.test(l)).length === 1);
check('it carries type, nullability and the primary key',
  /dbo,TimeType,table,15,1,Code,NVARCHAR\(10\),NO,YES,NO,,/.test(ALL.columns), colLines[1]);
// A table on the diagram whose columns never loaded must still appear.
// Dropping it would read as "this table has nothing in it".
check('a table whose columns did not load still gets a row, saying why',
  /dbo,Timecard,table,573106,,,,,,,,columns not loaded/.test(ALL.columns));
check('and when the fetch actually failed it says that instead',
  /columns unavailable: permission denied/.test(X.columnsCsv((() => {
    const m = model(); m.tables[1].columnsError = 'permission denied'; return m;
  })())));
check('a foreign key column names what it points at',
  /Timecard.*TimeType/.test(X.columnsCsv((() => {
    const m = model();
    m.tables[1].columns = [{ name: 'TimeType', type: 'NVARCHAR(10)', nullable: true, ordinal: 1 }];
    return m;
  })())));

/* ── 4. Relationships: a guess is never printed as a constraint ─────────── */

check('declared and inferred edges are both present',
  /^declared,FK_Timecard_TimeType/m.test(ALL.relationships)
  && /^inferred,/m.test(ALL.relationships));
// The diagram keeps inferred edges out of g.edges for this reason; an export
// that flattened them would hand someone a hypothesis as a fact, in a file
// that has left the product.
check('origin is its own column, so a reader can drop every hypothesis in one move',
  ALL.relationships.split('\n').find(l => l.startsWith('origin,')).startsWith('origin,constraint,'));
check('an inferred edge carries its confidence and orphan rate',
  /inferred,,dbo,Timecard,AltType,dbo,TimeType,Code,82%,0\.31%/.test(ALL.relationships),
  ALL.relationships.split('\n').filter(l => /^inferred/.test(l))[0]);

/* ── 5. Tables: the inventory ───────────────────────────────────────────── */

check('row counts and key are carried, unformatted so Excel sees numbers',
  /dbo,Timecard,table,573106,/.test(ALL.tables) && !/573,106/.test(ALL.tables));
check('and the degree in each direction, which is what makes a hub findable',
  /dbo,TimeType,table,15,2,Code,0,2/.test(ALL.tables),
  ALL.tables.split('\n').filter(l => /TimeType/.test(l)).pop());

/* ── 6. Markdown ────────────────────────────────────────────────────────── */

check('a heading per table and a column table under it',
  /## dbo\.TimeType/.test(ALL.markdown) && /\| Column \| Type \| Null \| Key \| References \|/.test(ALL.markdown));
check('a pipe inside an identifier is escaped rather than breaking the table',
  /\\\|/.test(X.dictionaryMarkdown((() => {
    const m = model(); m.tables[0].columns[0].name = 'we|ird'; return m;
  })())));
check('a table with no columns says so rather than rendering an empty grid',
  /_Columns not loaded\._/.test(ALL.markdown));

/* ── 7. The picture ─────────────────────────────────────────────────────── */

check('it is a standalone SVG — namespace, viewBox, own size',
  /^<svg xmlns="http:\/\/www\.w3\.org\/2000\/svg"/.test(ALL.svg)
  && /viewBox="/.test(ALL.svg) && /width="\d/.test(ALL.svg));
// A var(--text) resolves to nothing outside the app and a missing IBM Plex
// reflows every label, so neither may appear.
check('it carries no CSS variable and no external reference',
  !/var\(--/.test(ALL.svg) && !/<link|@import|url\(/.test(ALL.svg));
check('the font stack ends in a generic family', /sans-serif/.test(ALL.svg));
check('it paints its own background rather than relying on the page',
  /<rect[^>]*fill="#ffffff"/.test(ALL.svg));
check('every table on the model is drawn, with its row count',
  /TimeType/.test(ALL.svg) && /Timecard/.test(ALL.svg) && /573,106/.test(ALL.svg));
check('columns are drawn from shownColumns, capped at the same maxRows the diagram uses',
  (X.diagramSvg((() => {
    const m = model();
    m.tables[0].shownColumns = Array.from({ length: 20 }, (_, i) => ({ name: 'c' + i, type: 'INT' }));
    m.tables[0].moreCount = 11;
    return m;
  })()).match(/>c\d+</g) || []).length === 9);
check('and a truncated box says how many more there are',
  />\+11 more</.test(X.diagramSvg((() => {
    const m = model(); m.tables[0].moreCount = 11; return m;
  })())));
// An inferred edge drawn solid presents a guess as a declared constraint, in
// a file that is about to be pasted into a document.
check('inferred edges stay dashed in the export, as they are on screen',
  /stroke-dasharray/.test(ALL.svg)
  && (ALL.svg.match(/stroke-dasharray/g) || []).length === 1);
check('identifiers are XML-escaped', /&amp;/.test(X.diagramSvg((() => {
  const m = model(); m.tables[0].name = 'A&B'; return m;
})())));

/* The crop test. Bounds must include the edge anchors, not only the boxes:
   the bezier control points reach outside both boxes it joins, and a viewBox
   drawn to the boxes alone cuts the curves off — which is how an exported ERD
   ends up with no lines on it. */
{
  const m = model();
  m.edges[0].a = { x: 900, y: 700 };          // an anchor well outside every box
  const b = X.__core.bounds(m);
  check('the viewBox covers the edges as well as the boxes, so nothing is cropped',
    b.x + b.w >= 900 && b.y + b.h >= 700, JSON.stringify(b));
  const empty = X.__core.bounds({ tables: [], edges: [], pos: {} });
  check('an empty diagram still yields a usable box rather than NaN',
    empty.w > 0 && empty.h > 0 && isFinite(empty.x), JSON.stringify(empty));
}

/* ── 8. File names ──────────────────────────────────────────────────────── */

check('files are named for the database and dated',
  X.fileName(model(), 'columns', 'csv') === 'cygenix_Conv_DM_columns_2026-09-14.csv',
  X.fileName(model(), 'columns', 'csv'));
// A database can legally be called `a/b`, and a download named that fails
// silently in some browsers.
check('a database name that is not a safe filename is made into one',
  X.fileName({ database: 'a/b c:d', generatedAt: AT }, 'diagram', 'svg')
    === 'cygenix_a-b-c-d_diagram_2026-09-14.svg',
  X.fileName({ database: 'a/b c:d', generatedAt: AT }, 'diagram', 'svg'));
check('and a missing database name still produces a file name',
  /^cygenix_schema_diagram_/.test(X.fileName({}, 'diagram', 'svg')));

/* ── 9. Nothing throws on an empty or half-loaded model ─────────────────── */

check('an empty model produces valid, empty-but-labelled files',
  ['columnsCsv', 'tablesCsv', 'relationshipsCsv', 'dictionaryMarkdown', 'diagramSvg']
    .every(fn => typeof X[fn]({}) === 'string' && X[fn]({}).length > 0));
check('and so does no model at all',
  ['columnsCsv', 'diagramSvg'].every(fn => typeof X[fn]() === 'string'));

/* ── 10. Wiring ─────────────────────────────────────────────────────────── */

const PAGE = read('public', 'schema_explorer.html');
check('the Schema Explorer loads the module and offers the button',
  /<script src="\/cygenix-schema-export\.js\?v=[a-f0-9]{10}" defer><\/script>/.test(PAGE)
  && /id="sm-export-btn"/.test(PAGE));
check('all six exports are reachable from the dialog',
  ['print', 'svg', 'columns', 'tables', 'rels', 'markdown']
    .every(k => PAGE.indexOf("smExportRun('" + k + "')") > -1));
// The whole reason this module takes a positioned model: smAnchor and
// smShownCols already decide the geometry, and deriving it twice is how the
// exported picture stops matching the drawn one.
check('the page hands over its own geometry rather than letting the export recompute it',
  /a = smAnchor\(e\.from, e\.fromColumn\), b = smAnchor\(e\.to, e\.toColumn\)/.test(PAGE)
  && /shownColumns: smShownCols\(t\)/.test(PAGE)
  && /geom: \{ boxW: BOX_W, rowH: ROW_H, headH: HEAD_H, maxRows: MAX_ROWS \}/.test(PAGE));
// Columns are lazy — only the first 60 tables are prefetched. An export that
// skipped the rest would produce a dictionary with unexplained holes.
check('the column exports wait for the columns to load first',
  /if \(spec && spec\.cols\) await smExportEnsureColumns\(tables\)/.test(PAGE)
  && /columnsFor\(SM\.side, need/.test(PAGE));
check('and report progress while they do, rather than freezing the toolbar',
  /CygenixBusy\.start\('Reading columns'/.test(PAGE));
check('the export is scoped to the filtered diagram, not the whole graph',
  /const tables = smTables\(\);[\s\S]{0,200}shown = new Set\(tables\.map/.test(PAGE));
// Printing the page as it stands prints a crop: #sm-viewport is
// overflow:hidden and the boxes sit under a pan/zoom transform.
check('print renders the standalone SVG into its own container rather than printing the viewport',
  /host\.innerHTML = window\.CygenixSchemaExport\.diagramSvg\(model\)/.test(PAGE)
  && /id="se-print"/.test(PAGE)
  && /body > \*\{display:none !important\}/.test(PAGE));
check('and the page is put back afterwards, on afterprint rather than straight away',
  /window\.addEventListener\('afterprint', cleanup\)/.test(PAGE)
  && /window\.removeEventListener\('afterprint', cleanup\)/.test(PAGE));
check('the print page is landscape', /@page\{size:landscape/.test(PAGE));
check('opening the export panel closes the other two, which share its position',
  /smFiltersToggle\(\);\s*\n\s*smInferClose\(\);/.test(PAGE));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
