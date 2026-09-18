// tests/template-io.test.js — Conversion Templates: import / export.
//
// The rules the brief fixed, pinned:
//   * export is header + one row per table, one blank row per empty module;
//   * CSV is RFC 4180 with a BOM, and round-trips through the parser;
//   * headers match by alias, ignoring case, spaces and punctuation; only
//     Module and Target Table are required; NULL is empty;
//   * import is ADD ONLY — present rows are skipped untouched, duplicates in
//     the file count once, blank targets are skipped silently;
//   * unknown tables and out-of-scope modules are imported AND flagged,
//     and every flag is an error in the publish panel; an unreadable
//     schema marks rows "not validated" instead of blocking;
//   * nothing in the feature knows a module or a table name.
'use strict';

const fs = require('fs');
const path = require('path');
const TM = require('../public/cygenix-template-model.js');
const IO = require('../public/cygenix-template-io.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

console.log('Conversion Templates — import / export of the mapping\n');

const tpl = () => {
  const t = TM.tmNewTemplate({ name: 'Finance modules', projectId: 'p1', profileId: 'FIN_3E_UAT' });
  TM.tmSyncScope(t, ['AP', 'Matters', 'WIP'], 'me');
  TM.tmAddTable(t, 'AP', { targetTable: 'VchrDetail', notes: 'lines, "quoted", with a comma' }, 'me');
  TM.tmAddTable(t, 'AP', { targetTable: 'Vchr' }, 'me');
  TM.tmAddTable(t, 'Matters', { targetTable: 'Matter', required: false }, 'me');
  // Schema 4: Include is opt-in, and these checks are about a template that
  // has been set up to publish. Import/export itself ignores the tick — the
  // mapping grid round-trips everything in scope — but tmCanPublish does not.
  ['AP', 'Matters', 'WIP'].forEach(m => TM.tmSetModuleIncluded(t, m, true, 'me'));
  return t;
};

/* ── 1. Export ──────────────────────────────────────────────────────────── */
{
  const rows = IO.exportRows(tpl());
  check('header row is row 1, in the fixed order', JSON.stringify(rows[0]) === JSON.stringify(['Module', 'Target Table', 'Staging Table', 'Required', 'Load Order', 'Notes']));
  check('one row per table plus one blank row for the module with none: 3 tables + WIP = 4 rows',
    rows.length === 5 && rows[4][0] === 'WIP' && rows[4][1] === '' && rows.filter(r => r[0] === 'AP').length === 2);
  check('Required exports as Yes / No', rows[1][3] === 'Yes' && rows[3][3] === 'No');
  check('staging names and load order are carried', rows[1][2] === 'STG_VchrDetail' && rows[1][4] === '1' && rows[2][4] === '2');
  const t2 = tpl(); TM.tmSyncScope(t2, ['AP'], 'me');
  check('a module dropped from scope is not exported', IO.exportRows(t2).every(r => r[0] !== 'Matters' && r[0] !== 'WIP'));

  const csv = IO.toCsv(rows);
  check('CSV starts with a BOM and uses CRLF', csv.charCodeAt(0) === 0xFEFF && /\r\n/.test(csv));
  check('a note with a comma and a quote is quoted with the quote doubled', /"lines, ""quoted"", with a comma"/.test(csv));
  check('and parses back to the same rows', JSON.stringify(IO.parseCsv(csv)) === JSON.stringify(rows));
  check('the parser copes with LF only, embedded newlines and a BOM',
    JSON.stringify(IO.parseCsv('﻿a,b\n"x\ny",2\n')) === JSON.stringify([['a', 'b'], ['x\ny', '2']]));
  check('the file name is cygenix-template-<name>-<profile>-<YYYYMMDD>.<ext> with non-alphanumerics replaced',
    IO.fileName('Finance modules (v2)', 'FIN_3E_UAT', 'csv', new Date(2026, 8, 17)) === 'cygenix-template-Finance-modules-v2-FIN-3E-UAT-20260917.csv');
  // A minimal SheetJS stand-in: enough to prove the sheet name and widths.
  const fakeXLSX = { utils: { aoa_to_sheet: (a) => ({ a }), book_new: () => ({ SheetNames: [], Sheets: {} }),
    book_append_sheet: (wb, ws, name) => { wb.SheetNames.push(name); wb.Sheets[name] = ws; } } };
  const wb = IO.buildWorkbook(fakeXLSX, rows);
  check('the workbook has one sheet named Template Mapping with six column widths',
    wb.SheetNames.length === 1 && wb.SheetNames[0] === 'Template Mapping' && wb.Sheets['Template Mapping']['!cols'].length === 6);
}

/* ── 2. Headers ─────────────────────────────────────────────────────────── */
{
  const h = IO.mapHeaders(['ENTITY', 'Production_Table', ' staging ', 'Mandatory', 'Seq', 'Comments']);
  check('aliases match case-insensitively, ignoring spaces and underscores',
    h.ok && h.map.module === 0 && h.map.targetTable === 1 && h.map.stagingTable === 2 && h.map.required === 3 && h.map.loadOrder === 4 && h.map.notes === 5);
  const two = IO.mapHeaders(['entity', 'production_table']);
  check('a two-column file (entity, production_table) is enough', two.ok && two.missing.length === 0);
  const bad = IO.mapHeaders(['Name', 'Owner', 'Target']);
  check('missing Module is refused, naming what was found', !bad.ok && bad.missing[0] === 'module' && bad.seen.join(',') === 'Name,Owner,Target' && bad.map.targetTable === 2);
  const fh = IO.findHeader([['', ''], ['Some title'], ['Module', 'Table'], ['AP', 'Vchr']]);
  check('the header can sit below a title or blank lines', fh.index === 2);
  // A query result pasted straight out of a database tool: two columns, no
  // header, data from line 1. That is the file people actually have.
  const raw = IO.findHeader([['AP', 'Vchr'], ['AR', 'NULL'], ['WIP', 'TimeCard']]);
  check('a two-column file with no header is read as Module, Target Table, and says it assumed so',
    raw.index === -1 && raw.assumed === true && raw.headers.ok && raw.headers.map.module === 0 && raw.headers.map.targetTable === 1);
  const rawRecs = IO.normaliseRows([['AP', 'Vchr'], ['AR', 'NULL'], ['WIP', 'TimeCard']], raw.headers.map, -1);
  check('…and every line, including the first, is a row; NULL is still blank',
    rawRecs.length === 3 && rawRecs[0].module === 'AP' && rawRecs[0].targetTable === 'Vchr' && rawRecs[0].rowNo === 1 && rawRecs[1].targetTable === '');
  const wide = IO.findHeader([['AP', 'Vchr', 'x', 'y', 'z'], ['WIP', 'TimeCard', 'a', 'b', 'c']]);
  check('a wider file with no header is still refused — no way to know which column is the table', wide.index === -1 && !wide.assumed);
}

/* ── 3. Values ──────────────────────────────────────────────────────────── */
{
  check('Required accepts Yes/No, Y/N, True/False, 1/0 in any case',
    ['yes', 'Y', 'TRUE', '1'].every(v => IO.parseRequired(v) === true) && ['No', 'n', 'false', '0'].every(v => IO.parseRequired(v) === false) && IO.parseRequired('maybe') === null && IO.parseRequired('') === null);
  check('NULL, null and blank are all empty', IO.isEmpty('NULL') && IO.isEmpty('null') && IO.isEmpty('  ') && !IO.isEmpty('0'));
  const recs = IO.normaliseRows([['entity', 'production_table'], ['AP', 'Vchr'], ['AP', 'NULL'], ['', ''], ['Matters', ' Matter ']], { module: 0, targetTable: 1 }, 0);
  check('a two-column file normalises with defaults: no staging, required unsaid, no order, NULL empty, blank lines dropped',
    recs.length === 3 && recs[0].targetTable === 'Vchr' && recs[0].stagingTable === '' && recs[0].required === null && recs[0].loadOrder === null
    && recs[1].targetTable === '' && recs[2].targetTable === 'Matter' && recs[2].rowNo === 5);
}

/* ── 4. The plan: add only ──────────────────────────────────────────────── */
{
  const t = tpl();
  const scope = ['AP', 'Matters', 'WIP'];
  const tables = ['VchrDetail', 'Vchr', 'Matter', 'MattDate', 'TimeCard'];
  // Re-importing the export changes nothing.
  const back = IO.normaliseRows(IO.parseCsv(IO.toCsv(IO.exportRows(t))), IO.mapHeaders(IO.COLUMNS).map, 0);
  const p0 = IO.plan(t, back, { scope, tables });
  check('re-importing the exact export: 0 to add, 3 already present, 1 blank skipped',
    p0.counts.toAdd === 0 && p0.counts.alreadyPresent === 3 && p0.counts.blankSkipped === 1 && p0.add.length === 0, JSON.stringify(p0.counts));

  const recs = [
    { rowNo: 2, module: 'ap', targetTable: 'vchrdetail', stagingTable: 'X', required: true, loadOrder: 9, notes: 'ignored' },   // present (case-insensitive)
    { rowNo: 3, module: 'Matters', targetTable: 'MattDate', stagingTable: '', required: null, loadOrder: null, notes: '' },
    { rowNo: 4, module: 'Matters', targetTable: 'MATTDATE', stagingTable: '', required: null, loadOrder: null, notes: '' },   // duplicate in file
    { rowNo: 5, module: 'WIP', targetTable: '', stagingTable: '', required: null, loadOrder: null, notes: '' },               // blank
    { rowNo: 6, module: 'WIP', targetTable: 'TimeCardX', stagingTable: 'STG_TC', required: false, loadOrder: 2, notes: 'n' }, // unknown table
    { rowNo: 7, module: 'Trust', targetTable: 'TimeCard', stagingTable: '', required: null, loadOrder: null, notes: '' },     // out of scope
  ];
  const p = IO.plan(t, recs, { scope, tables });
  check('counts: 3 to add, 1 present, 1 blank, 1 duplicate, 1 unknown table, 1 out of scope',
    p.counts.toAdd === 3 && p.counts.alreadyPresent === 1 && p.counts.blankSkipped === 1 && p.counts.duplicateInFile === 1
    && p.counts.unknownTable === 1 && p.counts.outOfScope === 1 && p.counts.notValidated === 0, JSON.stringify(p.counts));
  check('the flagged rows carry their reasons', p.add[1].flags[0].code === 'unknown-table' && /TimeCardX/.test(p.add[1].flags[0].reason)
    && p.add[2].flags[0].code === 'out-of-scope' && /Trust/.test(p.add[2].flags[0].reason));
  check('the plan is pure — the template is untouched until apply', TM.tmSummary(t).tableCount === 3);

  const pn = IO.plan(t, recs, { scope, tables: null });
  check('with no schema, rows are "not validated" rather than refused, and nothing is called unknown',
    pn.validated === false && pn.counts.notValidated === 3 && pn.counts.unknownTable === 0 && pn.add.every(r => r.flags.some(f => f.code === 'not-validated')));

  const r = IO.apply(t, p, TM, 'me');
  check('apply adds the three rows, creating the out-of-scope module OUT of scope',
    r.added === 3 && r.modulesCreated[0] === 'Trust' && TM.tmFindModule(t, 'Trust').inScope === false && TM.tmSummary(t).tableCount === 5 /* in-scope: AP 2, Matters 2, WIP 1; Trust is out of scope */);
  const md = TM.tmFindModule(t, 'Matters').tables.find(x => x.targetTable === 'MattDate');
  const tc = TM.tmFindModule(t, 'WIP').tables.find(x => x.targetTable === 'TimeCardX');
  check('defaults: staging derived, required No when unsaid, order assigned; a supplied staging name is kept',
    md.stagingTable === 'STG_MattDate' && md.required === false && md.loadOrder === 2 && tc.stagingTable === 'STG_TC' && tc.loadOrder === 2);
  check('the present row was not updated', TM.tmFindModule(t, 'AP').tables[0].stagingTable === 'STG_VchrDetail' && TM.tmFindModule(t, 'AP').tables[0].notes !== 'ignored');
  check('flags sit on the row under importFlags, beside the model\'s fields', Array.isArray(tc.importFlags) && tc.importFlags[0].code === 'unknown-table' && !md.importFlags);

  const issues = IO.flagIssues(t);
  check('every flag is an ERROR in the publish panel, naming the module and the fix',
    issues.length === 2 && issues.every(i => i.level === 'error') && issues.some(i => i.module === 'WIP' && /Correct the name/.test(i.message))
    && issues.some(i => i.module === 'Trust' && /Tick the module/.test(i.message)));
  check('the model still says it can publish — the page merges the two lists, which is the point', TM.tmCanPublish(t) && IO.hasFlagErrors(t));
  check('moduleImportedOutOfScope tells an imported module from a dropped one',
    IO.moduleImportedOutOfScope(TM.tmFindModule(t, 'Trust')) && (() => { TM.tmSyncScope(t, ['AP', 'Matters'], 'me'); return !IO.moduleImportedOutOfScope(TM.tmFindModule(t, 'WIP')); })());

  // Re-checks.
  TM.tmSyncScope(t, ['AP', 'Matters', 'WIP', 'Trust'], 'me');
  const n1 = IO.refreshFlags(t, { tables: ['VchrDetail', 'Vchr', 'Matter', 'MattDate', 'TimeCard', 'TimeCardX'] });
  check('ticking the module and the table appearing in the schema clear both flags', n1 === 2 && IO.flagIssues(t).length === 0);
  const t3 = tpl();
  IO.apply(t3, IO.plan(t3, [{ rowNo: 2, module: 'WIP', targetTable: 'Ghost', stagingTable: '', required: null, loadOrder: null, notes: '' }], { scope: ['AP', 'Matters', 'WIP'], tables: null }), TM, 'me');
  IO.refreshFlags(t3, { tables: ['VchrDetail'] });
  check('a "not validated" row becomes "unknown table" once the schema can be read and lacks it',
    IO.flagsOf(TM.tmFindModule(t3, 'WIP').tables[0]).map(f => f.code).join() === 'unknown-table');
}

/* ── 5. Target-agnostic, and the page ───────────────────────────────────── */
{
  const io = read('public', 'cygenix-template-io.js');
  check('the rules file names no module and no table of any system',
    !/VchrDetail|Matter\b|Timekeeper|Elite|3E\b/.test(io.replace(/\/\*[\s\S]*?\*\//g, '')));
  check('and touches no DOM, network or storage', !/document\.|fetch\(|localStorage|XMLHttpRequest|require\(/.test(io.replace(/\/\*[\s\S]*?\*\//g, '')));

  const page = read('public', 'conversion-templates.html');
  check('the page loads the rules file after the model, stamped', page.indexOf('cygenix-template-io.js?v=') > page.indexOf('cygenix-template-model.js?v=') && /cygenix-template-io\.js\?v=[0-9a-f]{10}/.test(page));
  check('the sheet library is pinned, from jsdelivr, and not in the initial load',
    /const XLSX_SRC = 'https:\/\/cdn\.jsdelivr\.net\/npm\/xlsx@0\.18\.5\/dist\/xlsx\.full\.min\.js'/.test(page) && !/<script src="https:\/\/cdn\.jsdelivr/.test(page));
  check('the loader\'s state is written by the load and never reset by it',
    /CT\.xlsx = ok \? 'ready' : 'failed'/.test(page) && !/CT\.xlsx = 'none'[\s\S]{0,40}settle/.test(page.slice(page.indexOf('function loadXlsx'))));
  check('Export offers Excel and CSV; Import takes .xlsx, .xls and .csv',
    /ctExport\('xlsx'\)/.test(page) && /ctExport\('csv'\)/.test(page) && /accept="\.xlsx,\.xls,\.csv/.test(page));
  check('the schema is read once per import through the existing single-flight reader, and nothing per row',
    (page.slice(page.indexOf('async function ctImportFile')).split('function renderImportPreview')[0].match(/await ensureGraph\(\)/g) || []).length === 1
    && !/CygenixSchemaGraph\.load\('tgt'\)[\s\S]{0,200}forEach/.test(page));
  check('the preview commits only on Import; Cancel drops the plan', /function ctImportCancel\(\)\{\s*\n\s*CT\.pendingImport = null;/.test(page) && /IO\.apply\(CT\.tpl, pi\.plan, TM, userName\(\)\)/.test(page));
  check('an import marks the template unsaved and never saves', /touchDirty\(\); render\(\);\s*\n\s*const c = pi\.plan\.counts;/.test(page) && !/ctImportCommit[\s\S]{0,1200}api\('template-save'/.test(page));
  check('flagged rows are marked in the grid with the reason, and the publish gate reads the merged list',
    /class="flagged"/.test(page) && /ic-warning/.test(page) && /function canPublishNow\(\)/.test(page) && /const issues = allIssues\(\);/.test(page));
  check('no new data action and no Netlify function', !/template-(import|export)/.test(read('netlify', 'functions', 'data-proxy.js')) && !fs.existsSync(path.join(ROOT, 'netlify', 'functions', 'template-import.js')));
  check('every icon the feature uses exists', ['ic-download', 'ic-upload', 'ic-warning'].every(c => read('public', 'cygenix-icons.css').indexOf('.' + c) !== -1));
}

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
