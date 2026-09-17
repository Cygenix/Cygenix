/* ============================================================================
   cygenix-template-spec.js — the client specification workbook and the
   staging DDL, built from a Conversion Template's stored column snapshot
   ----------------------------------------------------------------------------
   Phase 2 (Sep-2026). Phase 1 decided WHICH target tables each module maps
   to. This turns that list into the two things a client actually receives:

     the specification workbook   what to build, column by column, with a
                                  sheet per table for them to populate
     the staging DDL              the CREATE TABLE statements for it

   Both come from one place — `table.columns`, the snapshot the model stores
   — and never from a live read at generation time. That is what makes a
   published version reproducible: regenerating the specification for v1 next
   year produces v1's schema, not whatever the target looks like by then.
   fetchColumns() is the only function here that touches the target, and it
   writes what it reads into the template through the model.

   STAGING IS A LANDING ZONE, and the DDL says so: every column nullable
   whatever the target says, no identity, no computed columns, no defaults,
   no keys, no indexes. The types and lengths are the target's exactly.
   Requiredness is communicated in the WORKBOOK, not enforced in the DDL —
   a NOT NULL here would make the client's first partial load fail, which
   teaches them nothing except to dread the next one.

   TARGET-AGNOSTIC. Nothing here knows a module, a table or a column of any
   particular system. Every name comes from the template or the target.

   XLSX is passed IN. The page owns the lazy CDN load and its failure
   message; this file takes the library as an argument so it stays pure
   enough to test in Node with a stand-in.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixTemplateSpec) root.CygenixTemplateSpec = api;
})(typeof globalThis !== 'undefined' ? globalThis : (typeof window !== 'undefined' ? window : this), function () {
'use strict';

var SHEET_READ_ME = 'Read Me';
var SHEET_TABLES = 'Tables';
var SHEET_COLUMNS = 'Columns';
var SHEET_LOAD_ORDER = 'Load Order';
var SHEET_LIMIT = 31;                     // Excel's cap on a sheet name
var FETCH_CONCURRENCY = 4;
var FETCH_MIN_INTERVAL_MS = 3000;

var TABLES_HEADER = ['Module', 'Target Table', 'Staging Table', 'Required', 'Load Order', 'Column Count', 'Populate Sheet', 'Notes'];
var COLUMNS_HEADER = ['Module', 'Staging Table', 'Target Table', 'Ordinal', 'Column', 'Data Type', 'Length',
  'Precision', 'Scale', 'Nullable', 'Identity', 'Primary Key', 'Computed', 'Default', 'Populate?', 'Client Notes'];
var LOAD_ORDER_HEADER = ['Load Order', 'Module', 'Staging Table', 'Target Table', 'Required', 'Column Count'];

function str(v) { return v == null ? '' : String(v); }
function trim(v) { return str(v).trim(); }
function lower(v) { return trim(v).toLowerCase(); }
function yesNo(v) { return v ? 'Yes' : 'No'; }

/* ── Sheet names ───────────────────────────────────────────────────────────
   Excel caps a name at 31 characters, forbids [ ] : * ? / \, and treats two
   names differing only in case as the same name. A staging table name can
   break all three rules at once, so: sanitise, truncate, then de-duplicate
   with a numeric suffix that is made room for rather than appended past the
   limit. `taken` is a map the caller keeps across the workbook; the original
   name is recorded in the Tables sheet so nothing is ambiguous. */
function specSheetName(stagingTable, taken) {
  var seen = taken || {};
  var base = trim(stagingTable).replace(/[[\]:*?\/\\]/g, '_').replace(/^'+|'+$/g, '');
  if (!base) base = 'Table';
  base = base.slice(0, SHEET_LIMIT);
  var name = base;
  var n = 2;
  while (seen[lower(name)]) {
    var suffix = '_' + n;
    name = base.slice(0, SHEET_LIMIT - suffix.length) + suffix;
    n++;
  }
  seen[lower(name)] = true;
  return name;
}

/* ── Types ─────────────────────────────────────────────────────────────────
   The column record back to a type a client can declare. Length -1 means
   MAX. Only the families that actually take parameters get them — an
   INT(10,0) is what happens when that rule is not written down, and it has
   broken a CREATE TABLE in this product before. */
var LENGTH_TYPES = ['char', 'varchar', 'nchar', 'nvarchar', 'binary', 'varbinary'];
var PRECISION_SCALE_TYPES = ['decimal', 'numeric', 'dec'];
var PRECISION_ONLY_TYPES = ['datetime2', 'datetimeoffset', 'time'];

function sqlType(col) {
  var c = col || {};
  var t = lower(c.dataType || c.type);
  if (!t) return '';
  if (t.indexOf('(') > 0) return t;                    // already assembled
  if (LENGTH_TYPES.indexOf(t) !== -1 && c.maxLength != null) {
    var len = Number(c.maxLength);
    // nvarchar and nchar report bytes; the declaration is in characters.
    if (len === -1) return t + '(max)';
    if (t === 'nvarchar' || t === 'nchar') len = Math.floor(len / 2) || len;
    return t + '(' + len + ')';
  }
  if (PRECISION_SCALE_TYPES.indexOf(t) !== -1 && c.precision != null) {
    return t + '(' + c.precision + ',' + (c.scale == null ? 0 : c.scale) + ')';
  }
  if (PRECISION_ONLY_TYPES.indexOf(t) !== -1 && c.scale != null) return t + '(' + c.scale + ')';
  return t;
}

/* What the client is being asked to do with this column, derived rather
   than typed: an identity or computed column is theirs to leave alone, and
   everything else is required or optional by the target's nullability. */
function populateVerdict(col) {
  var c = col || {};
  if (c.isIdentity) return 'No — identity';
  if (c.isComputed) return 'No — computed';
  return c.isNullable === false ? 'Required' : 'Optional';
}

/* ── The rows ──────────────────────────────────────────────────────────────
   Everything the workbook shows, as plain arrays, from the snapshot alone.
   A table with no snapshot is not skipped: it appears in Tables with a zero
   count and in `missing`, and the Read Me names it. Silence would be worse
   — the client would build to a specification with a hole in it. */
function buildSpecRows(tpl, opts) {
  var o = opts || {};
  var taken = {};
  var tables = [], columns = [], missing = [];
  ((tpl && tpl.modules) || []).forEach(function (m) {
    if (!m || m.inScope === false) return;
    var list = (m.tables || []).slice().sort(function (a, b) {
      return (Number(a.loadOrder) || 0) - (Number(b.loadOrder) || 0) || str(a.targetTable).localeCompare(str(b.targetTable));
    });
    list.forEach(function (t) {
      if (o.requiredOnly && t.required === false) return;
      var cols = Array.isArray(t.columns) ? t.columns : [];
      var sheet = specSheetName(t.stagingTable || t.targetTable, taken);
      tables.push({ module: m.module, targetTable: str(t.targetTable), stagingTable: str(t.stagingTable),
        required: t.required !== false, loadOrder: Number(t.loadOrder) || 0, columnCount: cols.length,
        sheetName: sheet, notes: str(t.notes), columns: cols, fetchedAt: str(t.columnsFetchedAt) });
      if (!cols.length) missing.push({ module: m.module, targetTable: str(t.targetTable) });
      cols.forEach(function (c) {
        columns.push({ module: m.module, stagingTable: str(t.stagingTable), targetTable: str(t.targetTable),
          ordinal: c.ordinal, name: str(c.name), sqlType: sqlType(c), maxLength: c.maxLength,
          precision: c.precision, scale: c.scale, isNullable: c.isNullable !== false,
          isIdentity: !!c.isIdentity, isComputed: !!c.isComputed, isPrimaryKey: !!c.isPrimaryKey,
          defaultDefinition: str(c.defaultDefinition), populate: populateVerdict(c) });
      });
    });
  });
  var loadOrder = tables.slice().sort(function (a, b) {
    return (a.loadOrder || 0) - (b.loadOrder || 0) || String(a.module).localeCompare(String(b.module))
      || String(a.stagingTable).localeCompare(String(b.stagingTable));
  });
  return { tables: tables, columns: columns, loadOrder: loadOrder, missing: missing };
}

/* ── The workbook ──────────────────────────────────────────────────────── */
function readMeRows(tpl, rows, opts) {
  var o = opts || {};
  var now = o.now instanceof Date ? o.now : new Date();
  var out = [
    ['Conversion Template — client specification'],
    [],
    ['Template', str(tpl.name)],
    ['Version', 'v' + (tpl.version || 1) + (tpl.status === 'published' ? ' (published)' : ' (draft)')],
    ['Target type', str(tpl.targetType)],
    ['Connection profile', str(tpl.profileId)],
    ['Staging prefix', str(tpl.stagingPrefix)],
    ['Generated at (UTC)', now.toISOString()],
    ['Generated by', str(o.by)],
    [],
    ['Modules', String(new Set(rows.tables.map(function (t) { return t.module; })).size)],
    ['Tables', String(rows.tables.length)],
    ['Columns', String(rows.columns.length)],
    [],
    ['What this is'],
    ['A staging database is a copy of the parts of the target system this conversion touches, in the target’s own shape.'],
    ['You build it, you populate it, and you hand it back. Because it matches the target table for table and column for'],
    ['column, the same standard import scripts run for every conversion, so nothing has to be written specially for yours.'],
    [],
    ['How to use this workbook'],
    ['1. Tables lists every table to create. Load Order is the sequence the import will run in.'],
    ['2. Columns lists every column, with its type and whether you need to populate it.'],
    ['3. There is one sheet per table, named after the staging table, for you to populate. Row 1 is the column names;'],
    ['   row 2 is a type hint and should be deleted before you return the workbook.'],
    ['4. Leave identity and computed columns alone. The target generates those values; anything you put there is'],
    ['   discarded, and in the case of a computed column the load will reject it.'],
    ['5. Populate? tells you what is expected: Required means the target will not accept a row without it.'],
    [],
    ['Return the completed workbook, or the populated staging database, to the person who sent you this file.'],
  ];
  if (rows.missing.length) {
    out.push([]);
    out.push(['Tables not found in target (' + rows.missing.length + ')']);
    out.push(['These tables are in the template but their columns could not be read. Their sheets are not included;']);
    out.push(['ask for an updated workbook before building them.']);
    rows.missing.forEach(function (m) { out.push([m.module, m.targetTable]); });
  }
  if (o.perTableSheets === false) out.push([], ['Per-table populate sheets were not included in this export.']);
  if (o.requiredOnly) out.push([], ['This export covers REQUIRED tables only.']);
  return out;
}

/* The populate sheet for one table: real column headers in ordinal order,
   a greyed type hint beneath, panes frozen below it. Computed columns are
   absent entirely — the client cannot supply them and a column they must
   not fill is a question they will ask. An identity column stays, marked,
   because its absence would make the sheet disagree with the table. */
function populateSheetRows(t) {
  var cols = (t.columns || []).filter(function (c) { return !c.isComputed; });
  var head = cols.map(function (c) { return str(c.name) + (c.isIdentity ? ' (do not populate)' : ''); });
  var hint = cols.map(function (c) {
    return sqlType(c) + (c.isIdentity ? ' · identity' : (c.isNullable === false ? ' · required' : ' · optional'));
  });
  return [head, hint];
}

function buildSpecWorkbook(XLSX, tpl, opts) {
  var o = opts || {};
  var rows = buildSpecRows(tpl, o);
  var wb = XLSX.utils.book_new();
  var add = function (name, aoa, cols, freeze) {
    var ws = XLSX.utils.aoa_to_sheet(aoa);
    if (cols) ws['!cols'] = cols;
    if (freeze) ws['!freeze'] = freeze;
    XLSX.utils.book_append_sheet(wb, ws, name);
    return ws;
  };

  add(SHEET_READ_ME, readMeRows(tpl, rows, o), [{ wch: 26 }, { wch: 100 }]);

  add(SHEET_TABLES, [TABLES_HEADER].concat(rows.tables.map(function (t) {
    return [t.module, t.targetTable, t.stagingTable, yesNo(t.required), t.loadOrder || '',
      t.columnCount, t.columnCount ? t.sheetName : '(no columns read)', t.notes];
  })), [{ wch: 24 }, { wch: 30 }, { wch: 30 }, { wch: 10 }, { wch: 11 }, { wch: 13 }, { wch: 31 }, { wch: 40 }],
    { xSplit: 0, ySplit: 1 });

  add(SHEET_COLUMNS, [COLUMNS_HEADER].concat(rows.columns.map(function (c) {
    return [c.module, c.stagingTable, c.targetTable, c.ordinal, c.name, c.sqlType,
      c.maxLength == null ? '' : c.maxLength, c.precision == null ? '' : c.precision,
      c.scale == null ? '' : c.scale, yesNo(c.isNullable), yesNo(c.isIdentity),
      yesNo(c.isPrimaryKey), yesNo(c.isComputed), c.defaultDefinition, c.populate, ''];
  })), [{ wch: 20 }, { wch: 28 }, { wch: 26 }, { wch: 8 }, { wch: 28 }, { wch: 18 }, { wch: 9 },
    { wch: 10 }, { wch: 7 }, { wch: 9 }, { wch: 9 }, { wch: 12 }, { wch: 10 }, { wch: 22 }, { wch: 16 }, { wch: 34 }],
    { xSplit: 0, ySplit: 1 });

  add(SHEET_LOAD_ORDER, [LOAD_ORDER_HEADER].concat(rows.loadOrder.map(function (t) {
    return [t.loadOrder || '', t.module, t.stagingTable, t.targetTable, yesNo(t.required), t.columnCount];
  })), [{ wch: 11 }, { wch: 24 }, { wch: 30 }, { wch: 30 }, { wch: 10 }, { wch: 13 }], { xSplit: 0, ySplit: 1 });

  if (o.perTableSheets !== false) {
    rows.tables.forEach(function (t) {
      if (!t.columnCount) return;            // nothing to head the sheet with
      add(t.sheetName, populateSheetRows(t), null, { xSplit: 0, ySplit: 2 });
    });
  }
  return wb;
}

/* ── The DDL ───────────────────────────────────────────────────────────── */
function bracket(name) { return '[' + str(name).replace(/]/g, ']]') + ']'; }

function buildStagingDdl(tpl, opts) {
  var o = opts || {};
  var now = o.now instanceof Date ? o.now : new Date();
  var rows = buildSpecRows(tpl, o);
  var schema = trim(o.schema || tpl.stagingSchema) || 'dbo';
  var nl = '\r\n';
  var out = [];
  out.push('/* =========================================================================');
  out.push('   Staging database for: ' + str(tpl.name));
  out.push('   Version:        v' + (tpl.version || 1) + ' (' + str(tpl.status) + ')');
  out.push('   Target type:    ' + str(tpl.targetType));
  out.push('   Profile:        ' + str(tpl.profileId));
  out.push('   Generated:      ' + now.toISOString() + (o.by ? ' by ' + str(o.by) : ''));
  out.push('   Tables:         ' + rows.tables.filter(function (t) { return t.columnCount; }).length);
  out.push('');
  out.push('   Every column is NULLABLE and there are no keys, indexes, defaults,');
  out.push('   identity or computed columns. This is a landing zone: the import');
  out.push('   validates what arrives. Making the staging tables strict would only');
  out.push('   make a partial first load fail at the door.');
  out.push('');
  out.push('   Re-running this script is safe — each table is created only if it does');
  out.push('   not already exist. It never drops or alters anything.');
  out.push('   ========================================================================= */');
  out.push('');
  if (rows.missing.length) {
    out.push('/* Not created — no column detail was available for these tables:');
    rows.missing.forEach(function (m) { out.push('     ' + m.module + ' / ' + m.targetTable); });
    out.push(' */');
    out.push('');
  }
  rows.loadOrder.forEach(function (t) {
    if (!t.columnCount) return;
    var cols = (t.columns || []).filter(function (c) { return !c.isComputed; });
    if (!cols.length) return;
    var full = bracket(schema) + '.' + bracket(t.stagingTable);
    out.push('/* ' + t.module + ' · ' + t.targetTable + (t.loadOrder ? ' · load order ' + t.loadOrder : '') + ' */');
    out.push("IF OBJECT_ID(N'" + schema.replace(/'/g, "''") + '.' + str(t.stagingTable).replace(/'/g, "''") + "', N'U') IS NULL");
    out.push('BEGIN');
    out.push('    CREATE TABLE ' + full + ' (');
    out.push(cols.map(function (c, i) {
      return '        ' + bracket(c.name) + ' ' + sqlType(c) + ' NULL' + (i < cols.length - 1 ? ',' : '');
    }).join(nl));
    out.push('    );');
    out.push('END');
    out.push('');
  });
  return out.join(nl);
}

/* ── File names ────────────────────────────────────────────────────────── */
function specFileName(tpl, ext) {
  // Underscores are kept: a profile id is FIN_3E_UAT and a person looking
  // for that file in a folder is looking for that string. Everything else
  // that is not a letter or a digit becomes a hyphen.
  var slug = function (s) { return trim(s).replace(/[^A-Za-z0-9_]+/g, '-').replace(/^[-_]+|-+$/g, ''); };
  var profile = slug(tpl && tpl.profileId) || 'no-profile';
  var name = slug(tpl && tpl.name) || 'conversion-template';
  return profile + '-' + name + '-v' + ((tpl && tpl.version) || 1) + '-spec.' + (ext || 'xlsx');
}

/* ── Reading the target ────────────────────────────────────────────────────
   The one function here that goes near a network, and the only one that is
   not pure. It reads IN-SCOPE tables only, four at a time, through the
   schema reader the Schema Explorer and Object Mapping already use — that
   reader batches per table, caches on its own node and de-duplicates
   in-flight requests, so there is no second path and no raised timeout.

   THE GUARDS, in one place so they can be read together:
     · `_fetching` is the in-flight flag. It is set before the first await
       and cleared in the finally of the SAME call that set it — never by a
       callback, never by a later one. A second click while it is set is
       refused with a reason, not queued.
     · `_lastFetchAt` enforces the three-second minimum interval.
   Neither is reset anywhere else in this file. */
var _fetching = false;
var _lastFetchAt = 0;

function fetchState() { return { busy: _fetching, lastAt: _lastFetchAt }; }
function _resetFetchStateForTests() { _fetching = false; _lastFetchAt = 0; }

async function fetchColumns(tpl, opts) {
  var o = opts || {};
  var TM = o.TM || (typeof window !== 'undefined' ? window.CygenixTemplateModel : null);
  var graph = o.graph || (typeof window !== 'undefined' ? window.CygenixSchemaGraph : null);
  if (!TM) return { ok: false, reason: 'The template model is not loaded.' };
  if (!graph || typeof graph.columns !== 'function') return { ok: false, reason: 'The schema reader is not loaded on this page.' };
  if (_fetching) return { ok: false, reason: 'Already reading the target schema — one moment.' };
  var now = o.now || Date.now();
  if (now - _lastFetchAt < FETCH_MIN_INTERVAL_MS) {
    return { ok: false, reason: 'Please wait a moment before reading the schema again.' };
  }
  if (typeof graph.hasConnection === 'function' && !graph.hasConnection('tgt')) {
    return { ok: false, reason: 'No target connection on the active profile.' };
  }

  _fetching = true;
  _lastFetchAt = now;
  var read = 0, failed = [], total = 0;
  try {
    // The graph's own table list has to exist before columns() can find a
    // node to hang them on; load() is cached and single-flight.
    var g = null;
    if (typeof graph.load === 'function') g = await graph.load('tgt');
    var known = {};
    if (g && g.ok) (g.tables || []).forEach(function (t) { known[lower(t.name)] = t; });

    var jobs = [];
    ((tpl && tpl.modules) || []).forEach(function (m) {
      if (!m || m.inScope === false) return;
      (m.tables || []).forEach(function (t) {
        if (o.onlyMissing && Array.isArray(t.columns) && t.columns.length) return;
        jobs.push({ module: m.module, table: t });
      });
    });
    total = jobs.length;

    var i = 0;
    var worker = async function () {
      while (i < jobs.length) {
        var job = jobs[i++];
        var target = trim(job.table.targetTable);
        var node = known[lower(target)];
        if (!node && g && g.ok) {
          // The target list was read and this table is not in it. Not a
          // failure of the export — a fact about the template.
          failed.push({ module: job.module, targetTable: target, reason: 'not found in target' });
          if (o.onProgress) { try { o.onProgress({ done: read + failed.length, total: total, table: target }); } catch (e) {} }
          continue;
        }
        try {
          var res = await graph.columns('tgt', (node && node.schema) || 'dbo', (node && node.name) || target);
          var cols = (res && res.columns) || [];
          if (!cols.length && res && res.columnsError) throw new Error(res.columnsError);
          TM.tmSetTableColumns(tpl, job.module, job.table.id, cols,
            { primaryKeys: (res && res.primaryKeys) || [], by: o.by });
          read++;
        } catch (e) {
          failed.push({ module: job.module, targetTable: target, reason: (e && e.message) || 'could not read' });
        }
        if (o.onProgress) { try { o.onProgress({ done: read + failed.length, total: total, table: target }); } catch (e2) {} }
      }
    };
    var workers = [];
    for (var w = 0; w < Math.min(FETCH_CONCURRENCY, jobs.length || 1); w++) workers.push(worker());
    await Promise.all(workers);
    return { ok: true, read: read, total: total, failed: failed };
  } catch (e) {
    return { ok: false, reason: (e && e.message) || 'The schema could not be read.', read: read, total: total, failed: failed };
  } finally {
    _fetching = false;
  }
}

return {
  SHEET_READ_ME: SHEET_READ_ME, SHEET_TABLES: SHEET_TABLES, SHEET_COLUMNS: SHEET_COLUMNS,
  SHEET_LOAD_ORDER: SHEET_LOAD_ORDER, SHEET_LIMIT: SHEET_LIMIT,
  TABLES_HEADER: TABLES_HEADER, COLUMNS_HEADER: COLUMNS_HEADER, LOAD_ORDER_HEADER: LOAD_ORDER_HEADER,
  FETCH_CONCURRENCY: FETCH_CONCURRENCY, FETCH_MIN_INTERVAL_MS: FETCH_MIN_INTERVAL_MS,
  specSheetName: specSheetName, sqlType: sqlType, populateVerdict: populateVerdict,
  buildSpecRows: buildSpecRows, readMeRows: readMeRows, populateSheetRows: populateSheetRows,
  buildSpecWorkbook: buildSpecWorkbook, buildStagingDdl: buildStagingDdl,
  specFileName: specFileName, fetchColumns: fetchColumns,
  fetchState: fetchState, _resetFetchStateForTests: _resetFetchStateForTests,
};
});
