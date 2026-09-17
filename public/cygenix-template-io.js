/* ============================================================================
   cygenix-template-io.js — Conversion Templates: import / export of the
   module-to-target-table mapping
   ----------------------------------------------------------------------------
   Requested (Sep-2026). A template's table list is the thing a client is
   asked to agree to, and the person who knows which tables belong to which
   module usually has that knowledge in a spreadsheet or a query result, not
   in a browser grid. So the mapping goes out as a sheet and comes back in as
   one — including a half-filled sheet: the export writes one blank row for
   every module that has no tables yet, so it doubles as the worksheet.

   THIS FILE IS THE RULES, AND ONLY THE RULES. No DOM, no network, no
   storage, no library. The page (conversion-templates.html) owns the file
   picker, the download, the SheetJS load and the preview modal; the model
   (cygenix-template-model.js) owns the document. Everything that can be
   wrong about a file is decided here, in functions a Node test can call
   with a string.

   ADD ONLY. An import never removes or rewrites a row the template already
   has. A file row whose (module, target table) is already present is
   counted and skipped — its staging name, order and notes are NOT applied,
   because the template is the record of decisions and the file is a
   suggestion. Blank target tables are skipped silently: that is what an
   unfilled worksheet row looks like, not a mistake.

   FLAG, NEVER REJECT. A target table the target database does not have, or
   a module the Configurator has not put in scope, is still imported —
   marked, so it cannot be published by accident, and visible in the grid
   and the "Ready to publish?" panel until somebody resolves it. The flags
   live on the table row under `importFlags`, a field ADDED beside the
   model's own (the model's shape is otherwise untouched, and its own
   validation ignores the field). If the target schema could not be read at
   all, every row is marked "not validated" instead, and the import goes
   ahead — a schema read timing out is not a reason to lose an afternoon's
   spreadsheet.

   TARGET-AGNOSTIC. Nothing here knows a module name or a table name. The
   header aliases are the only vocabulary, and they are words like "table"
   and "order", not any system's.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixTemplateIO) root.CygenixTemplateIO = api;
})(typeof globalThis !== 'undefined' ? globalThis : (typeof window !== 'undefined' ? window : this), function () {
'use strict';

/* ── Vocabulary ────────────────────────────────────────────────────────── */
var COLUMNS = ['Module', 'Target Table', 'Staging Table', 'Required', 'Load Order', 'Notes'];
var FIELDS  = ['module', 'targetTable', 'stagingTable', 'required', 'loadOrder', 'notes'];
var ALIASES = {
  module:       ['module', 'entity'],
  targetTable:  ['targettable', 'productiontable', 'table', 'target'],
  stagingTable: ['stagingtable', 'staging'],
  required:     ['required', 'mandatory'],
  loadOrder:    ['loadorder', 'order', 'seq'],
  notes:        ['notes', 'comment', 'comments'],
};
var REQUIRED_FIELDS = ['module', 'targetTable'];
var SHEET_NAME = 'Template Mapping';
var FLAG = {
  UNKNOWN_TABLE: 'unknown-table',
  OUT_OF_SCOPE:  'out-of-scope',
  NOT_VALIDATED: 'not-validated',
};
var FLAG_TEXT = {};
FLAG_TEXT[FLAG.UNKNOWN_TABLE] = 'Unknown target table';
FLAG_TEXT[FLAG.OUT_OF_SCOPE]  = 'Module out of scope';
FLAG_TEXT[FLAG.NOT_VALIDATED] = 'Not validated';

function str(v) { return v == null ? '' : String(v); }
function trim(v) { return str(v).trim(); }
function lower(v) { return trim(v).toLowerCase(); }
/* Case, spaces, underscores and punctuation all ignored: "Target_Table",
   "target table" and "TARGET-TABLE" are one header. */
function normHeader(s) { return lower(s).replace(/[^a-z0-9]/g, ''); }
/* Empty cells and the literal NULL are both "nothing here". */
function isEmpty(v) { var t = trim(v); return t === '' || t.toLowerCase() === 'null'; }

/* ── Headers ───────────────────────────────────────────────────────────── */
/* Which column holds which field. Returns the map, what was recognised, and
   which required fields are missing — so a refusal can name what it saw. */
function mapHeaders(headerRow) {
  var cells = Array.isArray(headerRow) ? headerRow : [];
  var map = {}, found = [];
  cells.forEach(function (cell, i) {
    var n = normHeader(cell);
    if (!n) return;
    for (var f = 0; f < FIELDS.length; f++) {
      var field = FIELDS[f];
      if (map[field] !== undefined) continue;                 // first matching column wins
      if (ALIASES[field].indexOf(n) !== -1) { map[field] = i; found.push(trim(cell)); return; }
    }
  });
  var missing = REQUIRED_FIELDS.filter(function (f) { return map[f] === undefined; });
  return { ok: missing.length === 0, map: map, found: found, missing: missing,
    seen: cells.map(trim).filter(Boolean) };
}
/* The first row that looks like a header. Files pasted from a tool can
   carry a blank line or two first. */
function findHeader(rows, maxScan) {
  var n = Math.min(rows.length, maxScan || 10);
  for (var i = 0; i < n; i++) {
    var h = mapHeaders(rows[i]);
    if (h.ok) return { index: i, headers: h, assumed: false };
  }
  var first = rows.length ? mapHeaders(rows[0]) : mapHeaders([]);
  /* No header anywhere. A query result pasted straight out of a database
     tool has none — two columns, module then table, from the first line.
     When EVERY row has at most two filled cells and at least one has two,
     that is what the file is, and it is read as Module, Target Table with
     `assumed` set so the page can say so in the preview. A file with more
     columns and no header is still refused: there is no way to know which
     of five columns is the table. */
  var twoCol = rows.length > 0 && rows.every(function (r) {
    return (r || []).filter(function (c) { return trim(c) !== ''; }).length <= 2;
  }) && rows.some(function (r) { return (r || []).filter(function (c) { return trim(c) !== ''; }).length === 2; });
  if (twoCol) {
    return { index: -1, assumed: true,
      headers: { ok: true, map: { module: 0, targetTable: 1 }, found: [], missing: [], seen: first.seen } };
  }
  return { index: -1, assumed: false, headers: first };
}

/* ── Values ────────────────────────────────────────────────────────────── */
/* Yes/No, Y/N, True/False, 1/0, any case. Anything else, or nothing, is
   "not said", which the caller turns into the default. */
function parseRequired(v) {
  var t = lower(v);
  if (['yes', 'y', 'true', '1'].indexOf(t) !== -1) return true;
  if (['no', 'n', 'false', '0'].indexOf(t) !== -1) return false;
  return null;
}
function parseOrder(v) {
  if (isEmpty(v)) return null;
  var n = Number(trim(v));
  return isFinite(n) && n > 0 ? Math.round(n) : null;
}

/* File rows → plain records, header-mapped. rowNo is the 1-based row in the
   file, for the preview. */
function normaliseRows(rows, map, headerIndex) {
  var out = [];
  var start = (headerIndex == null ? 0 : headerIndex) + 1;
  for (var i = start; i < rows.length; i++) {
    var r = rows[i] || [];
    var cell = function (field) { var k = map[field]; return k === undefined ? '' : (isEmpty(r[k]) ? '' : trim(r[k])); };
    var rec = {
      rowNo: i + 1,
      module: cell('module'),
      targetTable: cell('targetTable'),
      stagingTable: cell('stagingTable'),
      required: map.required === undefined ? null : parseRequired(r[map.required]),
      loadOrder: map.loadOrder === undefined ? null : parseOrder(r[map.loadOrder]),
      notes: cell('notes'),
    };
    // A wholly empty line (a trailing newline, a spacer) is not a row.
    if (!rec.module && !rec.targetTable && !rec.stagingTable && !rec.notes) continue;
    out.push(rec);
  }
  return out;
}

/* ── The plan: what an import WOULD do, before anything is touched ─────── */
/* ctx: { scope: [module names in scope] | null,
          tables: [target table names] | null   (null = schema not readable) }
   Returns rows to add (each with its flags) and every count the preview
   shows. Pure: the template is only read. */
function plan(tpl, records, ctx) {
  var c = ctx || {};
  var scopeSet = null;
  if (Array.isArray(c.scope)) { scopeSet = {}; c.scope.forEach(function (m) { scopeSet[lower(m)] = true; }); }
  var tableSet = null;
  if (Array.isArray(c.tables)) { tableSet = {}; c.tables.forEach(function (t) { tableSet[lower(t)] = true; }); }

  var present = {};
  ((tpl && tpl.modules) || []).forEach(function (m) {
    ((m && m.tables) || []).forEach(function (t) { present[lower(m.module) + ' ' + lower(t.targetTable)] = true; });
  });
  var seenInFile = {};
  var counts = { toAdd: 0, alreadyPresent: 0, blankSkipped: 0, duplicateInFile: 0, unknownTable: 0, outOfScope: 0, notValidated: 0, noModule: 0 };
  var add = [];
  (records || []).forEach(function (rec) {
    if (!rec.targetTable) { counts.blankSkipped++; return; }
    if (!rec.module) { counts.noModule++; return; }
    var key = lower(rec.module) + ' ' + lower(rec.targetTable);
    if (present[key]) { counts.alreadyPresent++; return; }
    if (seenInFile[key]) { counts.duplicateInFile++; return; }
    seenInFile[key] = true;
    var flags = [];
    if (tableSet === null) { flags.push({ code: FLAG.NOT_VALIDATED, reason: 'The target schema could not be read, so this table was not checked.' }); counts.notValidated++; }
    else if (!tableSet[lower(rec.targetTable)]) { flags.push({ code: FLAG.UNKNOWN_TABLE, reason: 'Table "' + rec.targetTable + '" is not in the target database.' }); counts.unknownTable++; }
    if (scopeSet !== null && !scopeSet[lower(rec.module)]) { flags.push({ code: FLAG.OUT_OF_SCOPE, reason: 'Module "' + rec.module + '" is not in the current Configurator scope.' }); counts.outOfScope++; }
    add.push({ rowNo: rec.rowNo, module: rec.module, targetTable: rec.targetTable, stagingTable: rec.stagingTable,
      required: rec.required, loadOrder: rec.loadOrder, notes: rec.notes, flags: flags });
    counts.toAdd++;
  });
  return { add: add, counts: counts, validated: tableSet !== null };
}

/* ── Apply the plan to the document, through the model ─────────────────── */
/* TM is the model. A module the template does not have is created out of
   scope (it was not in the Configurator, or it would already be there),
   so tmSyncScope leaves it exactly where the import put it. Returns what
   happened, for the summary line. */
function apply(tpl, thePlan, TM, who) {
  var added = 0, created = [];
  (thePlan && thePlan.add || []).forEach(function (r) {
    var mod = TM.tmFindModule(tpl, r.module);
    if (!mod) {
      mod = TM.tmNewModule(r.module);
      mod.inScope = false;
      tpl.modules.push(mod);
      created.push(r.module);
    }
    var opts = { targetTable: r.targetTable, required: r.required === null ? false : !!r.required, notes: r.notes };
    if (r.stagingTable) opts.stagingTable = r.stagingTable;
    if (r.loadOrder) opts.loadOrder = r.loadOrder;
    var t = TM.tmAddTable(tpl, mod.module, opts, who);
    if (!t) return;
    // tmAddTable derives the staging name from the prefix whenever the
    // template has one; a name the file supplied is a decision and stays.
    if (r.stagingTable) t.stagingTable = r.stagingTable;
    if (r.flags && r.flags.length) t.importFlags = r.flags.map(function (f) { return { code: f.code, reason: f.reason }; });
    added++;
  });
  return { added: added, modulesCreated: created };
}

/* ── Flags: reading, re-checking, reporting ────────────────────────────── */
function flagsOf(t) { return (t && Array.isArray(t.importFlags)) ? t.importFlags : []; }
function flagText(t) {
  return flagsOf(t).map(function (f) { return FLAG_TEXT[f.code] || f.code; }).join(', ');
}
/* Bring the flags up to date with what is now true: a module back in scope
   clears "out of scope"; a table that now exists in the (readable) schema
   clears "unknown"; a schema that can now be read replaces "not validated"
   with a real answer. Never adds a flag to a row that was not imported. */
function refreshFlags(tpl, ctx) {
  var c = ctx || {};
  var tableSet = null;
  if (Array.isArray(c.tables)) { tableSet = {}; c.tables.forEach(function (t) { tableSet[lower(t)] = true; }); }
  var changed = 0;
  ((tpl && tpl.modules) || []).forEach(function (m) {
    ((m && m.tables) || []).forEach(function (t) {
      var before = flagsOf(t);
      if (!before.length) return;
      var after = before.filter(function (f) {
        if (f.code === FLAG.OUT_OF_SCOPE) return m.inScope === false;
        if (f.code === FLAG.UNKNOWN_TABLE) return tableSet ? !tableSet[lower(t.targetTable)] : true;
        if (f.code === FLAG.NOT_VALIDATED) return tableSet === null;
        return true;
      });
      if (tableSet && before.some(function (f) { return f.code === FLAG.NOT_VALIDATED; }) && !tableSet[lower(t.targetTable)]) {
        after.push({ code: FLAG.UNKNOWN_TABLE, reason: 'Table "' + t.targetTable + '" is not in the target database.' });
      }
      if (after.length !== before.length) changed++;
      if (after.length) t.importFlags = after; else delete t.importFlags;
    });
  });
  return changed;
}
/* One issue per flagged row, as errors, in the shape tmValidate uses — so
   the page can show one list and gate Publish on it. */
function flagIssues(tpl) {
  var out = [];
  ((tpl && tpl.modules) || []).forEach(function (m) {
    ((m && m.tables) || []).forEach(function (t) {
      flagsOf(t).forEach(function (f) {
        out.push({ level: 'error', module: m.module, code: f.code,
          message: (FLAG_TEXT[f.code] || f.code) + ' — "' + t.targetTable + '": ' + f.reason
            + (f.code === FLAG.OUT_OF_SCOPE ? ' Tick the module in the Configurator and Refresh scope, or remove the row.'
              : f.code === FLAG.UNKNOWN_TABLE ? ' Correct the name, or remove the row.'
              : ' Re-import once the target is reachable, or remove the flag by editing the row.') });
      });
    });
  });
  return out;
}
function hasFlagErrors(tpl) { return flagIssues(tpl).length > 0; }
function moduleImportedOutOfScope(m) {
  return !!m && m.inScope === false && ((m.tables || []).some(function (t) {
    return flagsOf(t).some(function (f) { return f.code === FLAG.OUT_OF_SCOPE; }); }));
}

/* ── Export ────────────────────────────────────────────────────────────── */
/* Header + one row per table across the in-scope modules; a module with no
   tables gets one row with the target blank, so the sheet is the worksheet. */
function exportRows(tpl) {
  var rows = [COLUMNS.slice()];
  ((tpl && tpl.modules) || []).forEach(function (m) {
    if (!m || m.inScope === false) return;
    var tables = (m.tables || []).slice().sort(function (a, b) {
      return (Number(a.loadOrder) || 0) - (Number(b.loadOrder) || 0) || str(a.targetTable).localeCompare(str(b.targetTable));
    });
    if (!tables.length) { rows.push([m.module, '', '', '', '', '']); return; }
    tables.forEach(function (t) {
      rows.push([m.module, str(t.targetTable), str(t.stagingTable), t.required === false ? 'No' : 'Yes',
        t.loadOrder ? String(t.loadOrder) : '', str(t.notes)]);
    });
  });
  return rows;
}
/* RFC 4180: quote a field that holds a comma, a quote or a line break;
   double the quotes inside. CRLF between rows. A BOM first, so Excel reads
   the file as UTF-8 instead of guessing. */
function csvField(v) {
  var s = str(v);
  return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}
function toCsv(rows) {
  return '﻿' + (rows || []).map(function (r) { return r.map(csvField).join(','); }).join('\r\n') + '\r\n';
}
/* The reverse: quotes, doubled quotes, embedded line breaks, CR/LF/CRLF,
   an optional BOM. Returns an array of arrays of strings. */
function parseCsv(text) {
  var s = str(text);
  if (s.charCodeAt(0) === 0xFEFF) s = s.slice(1);
  var rows = [], row = [], field = '', i = 0, inQ = false;
  while (i < s.length) {
    var ch = s[i];
    if (inQ) {
      if (ch === '"') {
        if (s[i + 1] === '"') { field += '"'; i += 2; continue; }
        inQ = false; i++; continue;
      }
      field += ch; i++; continue;
    }
    if (ch === '"') { inQ = true; i++; continue; }
    if (ch === ',') { row.push(field); field = ''; i++; continue; }
    if (ch === '\r' || ch === '\n') {
      row.push(field); field = '';
      rows.push(row); row = [];
      if (ch === '\r' && s[i + 1] === '\n') i++;
      i++; continue;
    }
    field += ch; i++;
  }
  if (field !== '' || row.length) { row.push(field); rows.push(row); }
  return rows.filter(function (r) { return r.some(function (c) { return trim(c) !== ''; }); });
}
/* cygenix-template-<name>-<profile>-<YYYYMMDD>.<ext>, anything that is not
   a letter or digit in the name parts replaced by '-'. */
function slug(s) { return trim(s).replace(/[^A-Za-z0-9]+/g, '-').replace(/^-+|-+$/g, '') || 'template'; }
function fileName(tplName, profileId, ext, date) {
  var d = date instanceof Date ? date : new Date();
  var pad = function (n) { return String(n).padStart(2, '0'); };
  var stamp = d.getFullYear() + pad(d.getMonth() + 1) + pad(d.getDate());
  return 'cygenix-template-' + slug(tplName) + '-' + slug(profileId || 'no-profile') + '-' + stamp + '.' + ext;
}
/* A SheetJS workbook from the rows. XLSX is the loaded library — passed in,
   never required here — so this file stays free of it. Widths are fixed and
   sensible; the community build of SheetJS does not write cell styles, so
   the header cannot be bolded from here (see the page). */
function buildWorkbook(XLSX, rows) {
  var ws = XLSX.utils.aoa_to_sheet(rows);
  ws['!cols'] = [{ wch: 22 }, { wch: 30 }, { wch: 30 }, { wch: 10 }, { wch: 11 }, { wch: 40 }];
  var wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, SHEET_NAME);
  return wb;
}
/* Rows out of a SheetJS workbook's first sheet, as arrays of strings. */
function rowsFromWorkbook(XLSX, wb) {
  var name = (wb && wb.SheetNames && wb.SheetNames[0]) || null;
  if (!name) return [];
  var aoa = XLSX.utils.sheet_to_json(wb.Sheets[name], { header: 1, raw: false, defval: '' });
  return (aoa || []).map(function (r) { return (r || []).map(str); })
    .filter(function (r) { return r.some(function (c) { return trim(c) !== ''; }); });
}

return {
  COLUMNS: COLUMNS, FIELDS: FIELDS, ALIASES: ALIASES, SHEET_NAME: SHEET_NAME, FLAG: FLAG, FLAG_TEXT: FLAG_TEXT,
  normHeader: normHeader, isEmpty: isEmpty, mapHeaders: mapHeaders, findHeader: findHeader,
  parseRequired: parseRequired, parseOrder: parseOrder, normaliseRows: normaliseRows,
  plan: plan, apply: apply,
  flagsOf: flagsOf, flagText: flagText, refreshFlags: refreshFlags, flagIssues: flagIssues,
  hasFlagErrors: hasFlagErrors, moduleImportedOutOfScope: moduleImportedOutOfScope,
  exportRows: exportRows, csvField: csvField, toCsv: toCsv, parseCsv: parseCsv,
  slug: slug, fileName: fileName, buildWorkbook: buildWorkbook, rowsFromWorkbook: rowsFromWorkbook,
};
});
