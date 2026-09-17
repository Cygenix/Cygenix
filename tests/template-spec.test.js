// tests/template-spec.test.js — Conversion Templates Phase 2: the client
// specification workbook and the staging DDL.
//
// The decisions this phase fixed, pinned as decisions:
//
//   * both artefacts are built from the STORED column snapshot, never from a
//     live read — that is what makes a published version reproducible;
//   * staging is a landing zone: every column nullable, no identity, no
//     computed columns, no defaults, no keys, no indexes, types exactly as
//     the target has them;
//   * a computed column never reaches a populate sheet or the DDL, because
//     nobody can supply one; an identity column stays, marked;
//   * Excel's sheet-name rules are obeyed — 31 characters, no [ ] : * ? / \,
//     case-insensitively unique — and the mapping is recorded in Tables;
//   * the schema read is guarded by an in-flight flag cleared only by the
//     call that set it, and a three-second minimum interval;
//   * schema 1 documents forward-migrate and keep working.
'use strict';

const fs = require('fs');
const path = require('path');
const TM = require('../public/cygenix-template-model.js');
const SPEC = require('../public/cygenix-template-spec.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

console.log('Conversion Templates — client specification and staging DDL\n');

/* A template with column detail on two tables and none on a third. */
const COLS = [
  { name: 'VchrIndex', ordinal: 1, type: 'INT', nullable: false, isIdentity: true, baseType: 'int' },
  { name: 'Descr', ordinal: 2, baseType: 'nvarchar', maxLength: 128, nullable: true },
  { name: 'Body', ordinal: 3, baseType: 'nvarchar', maxLength: -1, nullable: true },
  { name: 'Amount', ordinal: 4, baseType: 'decimal', precision: 18, scale: 2, nullable: false },
  { name: 'TotalCalc', ordinal: 5, baseType: 'decimal', precision: 18, scale: 2, nullable: true, isComputed: true },
  { name: 'Stamp', ordinal: 6, baseType: 'datetime2', scale: 7, nullable: true, default: '(getdate())' },
];
const tpl = () => {
  const t = TM.tmNewTemplate({ name: 'Finance modules', projectId: 'p1', profileId: 'FIN_3E_UAT', createdBy: 'me' });
  TM.tmSyncScope(t, ['AP', 'Matters'], 'me');
  const a = TM.tmAddTable(t, 'AP', { targetTable: 'VchrDetail', notes: 'voucher lines' }, 'me');
  TM.tmAddTable(t, 'AP', { targetTable: 'Vchr', required: false }, 'me');
  TM.tmAddTable(t, 'Matters', { targetTable: 'Matter' }, 'me');
  TM.tmSetTableColumns(t, 'AP', a.id, COLS, { primaryKeys: ['VchrIndex'], by: 'me' });
  return t;
};

/* ── 1. The model: the snapshot, coverage, migration ────────────────────── */
{
  const t = tpl();
  const col = TM.tmFindModule(t, 'AP').tables[0].columns;
  check('a snapshot normalises to the documented fields, sorted by ordinal',
    col.length === 6 && col[0].name === 'VchrIndex' && col[5].name === 'Stamp'
    && TM.TM_COLUMN_FIELDS.every(f => f in col[0]));
  check('nullable becomes isNullable, identity and computed survive, the PK list becomes a flag',
    col[0].isNullable === false && col[0].isIdentity === true && col[0].isPrimaryKey === true
    && col[4].isComputed === true && col[1].isPrimaryKey === false);
  check('the default is kept as information', col[5].defaultDefinition === '(getdate())');
  check('a fetch stamp is written', !!TM.tmFindModule(t, 'AP').tables[0].columnsFetchedAt);
  check('an absent nullable reads as nullable — a specification must not invent a constraint',
    TM.tmNormaliseColumn({ name: 'x' }, 0, []).isNullable === true);

  const cov = TM.tmColumnCoverage(t);
  check('coverage counts in-scope tables only and names the ones with no detail',
    cov.tablesInScope === 3 && cov.tablesWithColumns === 1 && cov.totalColumns === 6
    && cov.tablesMissing.length === 2 && cov.tablesMissing[0].targetTable === 'Vchr', JSON.stringify(cov.tablesMissing));
  const s = TM.tmSummary(t);
  check('the summary carries the same numbers for the header line',
    s.totalColumns === 6 && s.tablesWithColumns === 1 && s.tablesMissingColumns === 2);
  check('an out-of-scope module is not counted',
    (() => { const u = tpl(); TM.tmSyncScope(u, ['AP'], 'me'); return TM.tmColumnCoverage(u).tablesInScope === 2; })());

  check('publish still BLOCKS only on the old rule, and warns separately about columns',
    TM.tmCanPublish(t) === true && TM.tmPublishWarnings(t).length === 1
    && /2 tables have no column detail/.test(TM.tmPublishWarnings(t)[0].message)
    && TM.tmPublishWarnings(t)[0].level === 'warning');
  check('a full snapshot warns about nothing',
    (() => { const u = tpl(); const m = TM.tmFindModule(u, 'AP'); TM.tmSetTableColumns(u, 'AP', m.tables[1].id, COLS.slice(0, 2), {});
      TM.tmSetTableColumns(u, 'Matters', TM.tmFindModule(u, 'Matters').tables[0].id, COLS.slice(0, 2), {});
      return TM.tmPublishWarnings(u).length === 0; })());
  check('a module with no tables still blocks publish, exactly as before',
    (() => { const u = TM.tmNewTemplate({ name: 'x', profileId: 'P' }); TM.tmSyncScope(u, ['AP']); return TM.tmCanPublish(u) === false; })());

  // The migration: a schema-1 document, as the live draft is.
  const v1 = JSON.parse(JSON.stringify(tpl()));
  v1.schema = 1;
  v1.modules.forEach(m => m.tables.forEach(x => { delete x.columns; delete x.columnsFetchedAt; }));
  const migrated = TM.tmMigrate(v1);
  check('a schema-1 document migrates: schema 2, every table has an empty columns array, no stamp',
    migrated.schema === 2 && migrated.modules.every(m => m.tables.every(x => Array.isArray(x.columns) && !x.columns.length && !x.columnsFetchedAt)));
  check('and it still summarises and validates', TM.tmSummary(migrated).tableCount === 3 && TM.tmValidate(migrated).length === 0);
  check('the schema version is 2 and the column fields are published', TM.TM_SCHEMA_VERSION === 2 && TM.TM_COLUMN_FIELDS.length === 11);
  check('publishing carries the snapshot with it',
    (() => { const u = tpl(); const f = TM.tmPublish(u, 'me'); return f && TM.tmFindModule(f, 'AP').tables[0].columns.length === 6; })());
}

/* ── 2. Types ───────────────────────────────────────────────────────────── */
{
  const T = (c) => SPEC.sqlType(c);
  check('nvarchar with a length declares in characters, not bytes', T({ dataType: 'nvarchar', maxLength: 128 }) === 'nvarchar(64)');
  check('varchar keeps its length as given', T({ dataType: 'varchar', maxLength: 50 }) === 'varchar(50)');
  check('length -1 is max', T({ dataType: 'nvarchar', maxLength: -1 }) === 'nvarchar(max)');
  check('decimal takes precision and scale', T({ dataType: 'decimal', precision: 18, scale: 2 }) === 'decimal(18,2)');
  check('decimal with no scale reads as zero', T({ dataType: 'numeric', precision: 9 }) === 'numeric(9,0)');
  check('datetime2 takes its scale only', T({ dataType: 'datetime2', scale: 7 }) === 'datetime2(7)');
  check('int takes nothing — the INT(10,0) bug stays fixed', T({ dataType: 'int', precision: 10, scale: 0 }) === 'int');
  check('bit, uniqueidentifier and money take nothing',
    T({ dataType: 'bit', precision: 1 }) === 'bit' && T({ dataType: 'uniqueidentifier' }) === 'uniqueidentifier' && T({ dataType: 'money', precision: 19, scale: 4 }) === 'money');
  check('an already-assembled type is passed through', T({ dataType: 'NVARCHAR(64)' }) === 'nvarchar(64)');

  check('the Populate? verdict is derived from the column, not typed',
    SPEC.populateVerdict({ isIdentity: true }) === 'No — identity'
    && SPEC.populateVerdict({ isComputed: true }) === 'No — computed'
    && SPEC.populateVerdict({ isNullable: false }) === 'Required'
    && SPEC.populateVerdict({ isNullable: true }) === 'Optional');
  check('identity beats computed when a column is somehow both', SPEC.populateVerdict({ isIdentity: true, isComputed: true }) === 'No — identity');
}

/* ── 3. Sheet names ─────────────────────────────────────────────────────── */
{
  const taken = {};
  check('a short name passes through', SPEC.specSheetName('STG_Vchr', taken) === 'STG_Vchr');
  check('Excel\'s forbidden characters are replaced', SPEC.specSheetName('STG_a[b]c:d*e?f/g\\h', {}) === 'STG_a_b_c_d_e_f_g_h');
  const long = SPEC.specSheetName('STG_' + 'A'.repeat(60), {});
  check('a long name is truncated to 31', long.length === 31);
  const t2 = {};
  const a = SPEC.specSheetName('STG_Same', t2), b = SPEC.specSheetName('stg_same', t2), c = SPEC.specSheetName('STG_SAME', t2);
  check('names are de-duplicated case-insensitively, as Excel compares them',
    a === 'STG_Same' && b === 'stg_same_2' && c === 'STG_SAME_3');
  const t3 = {};
  const x1 = SPEC.specSheetName('B'.repeat(31), t3), x2 = SPEC.specSheetName('B'.repeat(31), t3);
  check('a suffix makes room for itself rather than pushing past the cap', x1.length === 31 && x2.length === 31 && x2 !== x1);
  check('an empty name still yields a legal sheet name', SPEC.specSheetName('   ', {}) === 'Table');
}

/* ── 4. The rows ────────────────────────────────────────────────────────── */
{
  const t = tpl();
  const rows = SPEC.buildSpecRows(t);
  check('one Tables row per in-scope table, with its sheet name and column count',
    rows.tables.length === 3 && rows.tables[0].stagingTable === 'STG_VchrDetail' && rows.tables[0].columnCount === 6
    && rows.tables[0].sheetName === 'STG_VchrDetail' && rows.tables[1].columnCount === 0);
  check('one Columns row per column across the in-scope tables', rows.columns.length === 6);
  check('the Columns count equals the sum of the Tables column counts — the workbook\'s own check',
    rows.columns.length === rows.tables.reduce((n, x) => n + x.columnCount, 0));
  check('a table with no snapshot is listed as missing rather than dropped',
    rows.missing.length === 2 && rows.tables.some(x => x.targetTable === 'Vchr'));
  check('Load Order is sorted by load order then module',
    rows.loadOrder.map(x => x.targetTable).join(',') === 'VchrDetail,Matter,Vchr', rows.loadOrder.map(x => x.loadOrder + ':' + x.targetTable).join(','));
  check('"Required tables only" drops the optional table',
    SPEC.buildSpecRows(t, { requiredOnly: true }).tables.length === 2);
  const oos = tpl(); TM.tmSyncScope(oos, ['AP'], 'me');
  check('an out-of-scope module contributes nothing', SPEC.buildSpecRows(oos).tables.every(x => x.module !== 'Matters'));

  const ps = SPEC.populateSheetRows(rows.tables[0]);
  check('the populate sheet omits computed columns entirely and marks identity ones',
    ps[0].length === 5 && !ps[0].some(h => /TotalCalc/.test(h)) && /VchrIndex \(do not populate\)/.test(ps[0][0]));
  check('row 2 is the type hint, in the same order', ps[1][0] === 'int · identity' && ps[1][1] === 'nvarchar(64) · optional' && ps[1][3] === 'decimal(18,2) · required');

  const rm = SPEC.readMeRows(t, rows, { by: 'me', now: new Date(Date.UTC(2026, 8, 17, 9, 0, 0)) });
  const flat = rm.map(r => r.join(' ')).join('\n');
  check('the Read Me names the template, version, profile and the generation time in UTC',
    /Finance modules/.test(flat) && /v1 \(draft\)/.test(flat) && /FIN_3E_UAT/.test(flat) && /2026-09-17T09:00:00.000Z/.test(flat));
  check('…explains what a staging database is, and says to leave identity and computed columns alone',
    /staging database is a copy/.test(flat) && /Leave identity and computed columns alone/.test(flat));
  check('…and lists the tables whose columns could not be read', /Tables not found in target \(2\)/.test(flat) && /Vchr/.test(flat));
}

/* ── 5. The workbook ────────────────────────────────────────────────────── */
{
  // A SheetJS stand-in: enough to record what was written where.
  const fake = () => ({ utils: {
    aoa_to_sheet: (a) => ({ aoa: a }),
    book_new: () => ({ SheetNames: [], Sheets: {} }),
    book_append_sheet: (wb, ws, name) => { wb.SheetNames.push(name); wb.Sheets[name] = ws; },
  } });
  const t = tpl();
  const wb = SPEC.buildSpecWorkbook(fake(), t, { by: 'me' });
  check('the four fixed sheets come first, in order',
    wb.SheetNames.slice(0, 4).join('|') === 'Read Me|Tables|Columns|Load Order', wb.SheetNames.join('|'));
  check('one populate sheet per table that has columns — and none for a table without',
    wb.SheetNames.length === 5 && wb.SheetNames[4] === 'STG_VchrDetail');
  check('every sheet name is within Excel\'s limit and unique, case-insensitively',
    wb.SheetNames.every(n => n.length <= 31) && new Set(wb.SheetNames.map(n => n.toLowerCase())).size === wb.SheetNames.length);
  check('the Columns sheet has the sixteen documented headings, ending with the client\'s own column',
    wb.Sheets['Columns'].aoa[0].length === 16 && wb.Sheets['Columns'].aoa[0][0] === 'Module'
    && wb.Sheets['Columns'].aoa[0][14] === 'Populate?' && wb.Sheets['Columns'].aoa[0][15] === 'Client Notes');
  check('its rows carry the declarable types, including nvarchar(max) and decimal(18,2)',
    wb.Sheets['Columns'].aoa.some(r => r[5] === 'nvarchar(max)') && wb.Sheets['Columns'].aoa.some(r => r[5] === 'decimal(18,2)'));
  check('the identity column reads "No — identity" and the computed one "No — computed"',
    wb.Sheets['Columns'].aoa.some(r => r[4] === 'VchrIndex' && r[14] === 'No — identity')
    && wb.Sheets['Columns'].aoa.some(r => r[4] === 'TotalCalc' && r[14] === 'No — computed'));
  check('the Tables sheet records the original-to-sheet-name mapping',
    wb.Sheets['Tables'].aoa[0][6] === 'Populate Sheet' && wb.Sheets['Tables'].aoa[1][6] === 'STG_VchrDetail'
    && /no columns read/.test(wb.Sheets['Tables'].aoa[2][6]));
  check('the fixed sheets freeze their header row and the populate sheets freeze both',
    wb.Sheets['Tables']['!freeze'].ySplit === 1 && wb.Sheets['STG_VchrDetail']['!freeze'].ySplit === 2);
  check('spec-only omits every populate sheet', SPEC.buildSpecWorkbook(fake(), t, { perTableSheets: false }).SheetNames.length === 4);
  check('the file name names the profile, the template and the version',
    SPEC.specFileName(t, 'xlsx') === 'FIN_3E_UAT-Finance-modules-v1-spec.xlsx' && SPEC.specFileName(t, 'sql').endsWith('.sql'));
}

/* ── 6. The DDL ─────────────────────────────────────────────────────────── */
{
  const sql = SPEC.buildStagingDdl(tpl(), { by: 'me', now: new Date(Date.UTC(2026, 8, 17)) });
  // The header and the per-table comments describe the rules in prose, so
  // asserting "no DEFAULT" against the whole file matches the word in its own
  // explanation. The statements are what has to be clean.
  const stmts = sql.replace(/\/\*[\s\S]*?\*\//g, '');
  check('a header comment carries the template, version, profile and generation time',
    /Finance modules/.test(sql) && /v1 \(draft\)/.test(sql) && /FIN_3E_UAT/.test(sql) && /2026-09-17T00:00:00.000Z/.test(sql));
  check('each table is wrapped so re-running is a no-op rather than an error',
    /IF OBJECT_ID\(N'dbo\.STG_VchrDetail', N'U'\) IS NULL/.test(sql) && /BEGIN[\s\S]*END/.test(sql));
  check('every column is NULL, whatever the target says',
    (stmts.match(/^\s+\[[^\]]+\] .+ NULL,?$/gm) || []).length === 5 && !/NOT NULL/.test(stmts));
  check('the computed column is absent — the client cannot supply it', !/TotalCalc/.test(sql));
  check('the identity column is present, without IDENTITY', /\[VchrIndex\] int NULL/.test(stmts) && !/IDENTITY/i.test(stmts));
  check('no defaults, no keys, no indexes, no foreign keys',
    !/DEFAULT/i.test(stmts) && !/PRIMARY KEY/i.test(stmts) && !/CREATE INDEX/i.test(stmts) && !/FOREIGN KEY/i.test(stmts));
  check('types and lengths are the target\'s exactly',
    /\[Descr\] nvarchar\(64\) NULL/.test(sql) && /\[Body\] nvarchar\(max\) NULL/.test(sql) && /\[Amount\] decimal\(18,2\) NULL/.test(sql));
  check('every identifier is bracketed and the schema defaults to dbo', /\[dbo\]\.\[STG_VchrDetail\]/.test(sql));
  check('a table with no column detail is not created, and the file says which',
    !/STG_Vchr\b/.test(stmts.replace(/STG_VchrDetail/g, '')) && /Not created — no column detail/.test(sql) && /AP \/ Vchr/.test(sql));
  check('the comment explains why nothing is enforced here',
    /landing zone/.test(sql) && /partial first load fail/.test(sql));
  check('a bracket inside an identifier is doubled, not left to break the statement',
    (() => { const u = tpl(); const m = TM.tmFindModule(u, 'AP');
      TM.tmSetTableColumns(u, 'AP', m.tables[0].id, [{ name: 'Od]d', ordinal: 1, baseType: 'int' }], {});
      return /\[Od\]\]d\] int NULL/.test(SPEC.buildStagingDdl(u, {})); })());
}

/* ── 7. The guarded schema read ─────────────────────────────────────────── */
{
  SPEC._resetFetchStateForTests();
  const t = tpl();
  let calls = 0, inFlightPeak = 0, live = 0;
  const graph = {
    hasConnection: () => true,
    load: async () => ({ ok: true, tables: [{ schema: 'dbo', name: 'VchrDetail' }, { schema: 'dbo', name: 'Vchr' }] }),
    columns: async (side, schema, name) => {
      calls++; live++; inFlightPeak = Math.max(inFlightPeak, live);
      await new Promise(r => setTimeout(r, 5));
      live--;
      return { columns: [{ name: 'A', ordinal: 1, baseType: 'int', nullable: true }], primaryKeys: [] };
    },
  };
  return (async () => {
    const r = await SPEC.fetchColumns(t, { TM, graph, by: 'me', now: Date.now() });
    check('the read covers in-scope tables only, and reports what it read',
      r.ok && r.total === 3 && r.read === 2 && calls === 2, JSON.stringify({ r, calls }));
    check('a table the target does not have is recorded, not thrown',
      r.failed.length === 1 && r.failed[0].targetTable === 'Matter' && /not found in target/.test(r.failed[0].reason));
    check('what it read is stored on the template with a stamp',
      TM.tmFindModule(t, 'AP').tables[0].columns.length === 1 && !!TM.tmFindModule(t, 'AP').tables[0].columnsFetchedAt);
    check('no more than four requests are in flight at once', inFlightPeak <= SPEC.FETCH_CONCURRENCY, 'peak=' + inFlightPeak);

    const again = await SPEC.fetchColumns(t, { TM, graph, now: Date.now() });
    check('a second read inside three seconds is refused with a reason, not queued',
      !again.ok && /wait a moment/i.test(again.reason) && calls === 2);

    // The in-flight guard: start one and try to start another while it runs.
    SPEC._resetFetchStateForTests();
    const slow = Object.assign({}, graph, { columns: async () => { await new Promise(r2 => setTimeout(r2, 40)); return { columns: [], primaryKeys: [] }; } });
    const first = SPEC.fetchColumns(t, { TM, graph: slow, now: Date.now() });
    const second = await SPEC.fetchColumns(t, { TM, graph: slow, now: Date.now() });
    check('a second read while one is in flight is refused', !second.ok && /Already reading/i.test(second.reason));
    await first;
    check('and the flag is cleared by the call that set it, so the next read is allowed',
      SPEC.fetchState().busy === false);
    check('with no connection the read says so rather than failing silently',
      (await SPEC.fetchColumns(t, { TM, graph: { hasConnection: () => false, columns: () => {} }, now: Date.now() + 99999 })).reason === 'No target connection on the active profile.');

    /* ── 8. Wiring ──────────────────────────────────────────────────────── */
    const spec = read('public', 'cygenix-template-spec.js');
    check('the spec module names no system\'s module, table or column',
      !/VchrDetail|Elite|3E\b|Matter\b/.test(spec.replace(/\/\*[\s\S]*?\*\//g, '')));
    check('it never reads the target at build time — only fetchColumns touches the graph',
      (spec.match(/graph\./g) || []).length > 0 && !/buildSpec[\s\S]{0,400}graph\./.test(spec));
    check('the in-flight flag is cleared in a finally, and nowhere else',
      /_fetching = true;/.test(spec) && /finally \{\s*\n\s*_fetching = false;/.test(spec)
      /* three: the declaration, the test reset, and the finally — nowhere else */
      && (spec.match(/_fetching = false/g) || []).length === 3
      && !/onProgress[\s\S]{0,200}_fetching/.test(spec));

    const page = read('public', 'conversion-templates.html');
    check('the page loads the spec module after the model and the io module',
      page.indexOf('cygenix-template-spec.js?v=') > page.indexOf('cygenix-template-io.js?v=')
      && /cygenix-template-spec\.js\?v=[0-9a-f]{10}/.test(page));
    check('the export menu offers the workbook and the DDL under a divider',
      /ct-menu-sep/.test(page) && /ctOpenSpec\(\)/.test(page) && /ctExportDdl\(\)/.test(page));
    check('the dialog has both options and a version selector defaulting to the published version',
      /id="ct-spec-sheets" checked/.test(page) && /id="ct-spec-required"/.test(page)
      && /id="ct-spec-version"/.test(page) && /sel\.value = pubs\.length \? pubs\[0\]\.id : ''/.test(page));
    check('Refresh columns sits beside Refresh scope and is disabled on a published version',
      /id="ct-refresh-cols"/.test(page) && page.indexOf('id="ct-refresh-cols"') > page.indexOf('id="ct-refresh"')
      && /rc\.disabled = ro \|\| !!CT\.inflight \|\| !!CT\.readingColumns/.test(page));
    check('the page\'s own in-flight flag is set before the await and cleared in its finally',
      /CT\.readingColumns = true;[\s\S]{0,400}await SPEC\.fetchColumns/.test(page)
      && /finally \{\s*\n\s*CT\.readingColumns = false;/.test(page));
    check('the header line shows column coverage', /columns read for /.test(page));
    check('a document is migrated on the way in, and a published one is left alone',
      /TM\.tmMigrate\(r\.data\.template\)/.test(page) && /r\.data\.kind === 'published'\) \? r\.data\.template/.test(page));
    check('publish warns about missing columns without blocking',
      /TM\.tmPublishWarnings\(CT\.tpl\)/.test(page) && !/tmPublishWarnings[\s\S]{0,200}return;/.test(page));
    check('both exports are audited', /audit\('template\.export-spec'/.test(page) && /audit\('template\.export-ddl'/.test(page));
    check('the published spec is generated from the fetched document, never from the draft',
      /async function ctSpecDoc\(\)/.test(page) && /if \(!id\) return CT\.tpl;/.test(page));

    const audit = read('netlify', 'functions', 'lib', 'audit-schema.js');
    check('both export actions are on the client allowlist, under Data out',
      /'template\.export-spec':\s*'data'/.test(audit) && /'template\.export-ddl':\s*'data'/.test(audit));

    const actions = read('public', 'cygenix-assistant-actions.js');
    check('the assistant can run both, as reads, scoped to the page',
      /name: 'template_refresh_columns'/.test(actions) && /name: 'template_export_spec'/.test(actions)
      && (actions.match(/effect: 'read', page: 'conversion-templates'/g) || []).length === 2);
    check('and the page exports the two functions they call across the block boundary',
      /window\.ctRefreshColumns = ctRefreshColumns;/.test(page) && /window\.ctOpenSpec = ctOpenSpec;/.test(page));

    const db = read('netlify', 'functions', 'db-connect.js');
    check('schema-columns gained a computed flag and the raw type parts, in both dialects',
      /sys\.computed_columns cc/.test(db) && /is_generated = 'ALWAYS'/.test(db)
      && (db.match(/isComputed:/g) || []).length === 2 && (db.match(/baseType:/g) || []).length === 2);
    check('…and the assembled `type` field is unchanged, so existing callers are untouched',
      (db.match(/let type = /g) || []).length === 4);
    const az = read('azure-function', 'src', 'index.js');
    check('the Azure copy gained the same two', /IsComputed/.test(az) && /isComputed: c\.is_computed === 1/.test(az));

    console.log('\n' + pass + ' passed, ' + fail + ' failed');
    process.exit(fail ? 1 : 0);
  })();
}
