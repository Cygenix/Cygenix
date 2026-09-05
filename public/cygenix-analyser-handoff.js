/* ============================================================================
   cygenix-analyser-handoff.js — how a profiled file becomes a source.
   ----------------------------------------------------------------------------
   WHAT THIS IS FOR

   The Data Analyser profiles a file entirely in the browser and stops. Object
   Mapping maps a SOURCE onto a target — and until this module, a source was
   always a database object reached through a connection: connectSrc() builds
   a connection string, fetches the live schema, and srcTable is whatever the
   user picked from that list. A file has no connection, so it had no way in.

   This module is the bridge, in two halves that both pages share:

     buildFileSourceJob(result, opts)   Data Analyser → a job record
     fileSourceToSrcTable(job)          job record → the srcTable shape Object
                                        Mapping's existing code already reads

   Object Mapping installs the second one into srcAllTables and its ordinary
   restore path — find by value, selectTable('src'), restoreJobMapping — then
   runs unchanged. That is deliberate: the fewer places that know a source can
   be a file, the fewer places can forget.

   WHAT THE RECORD CARRIES, AND WHAT IT NEVER CARRIES

   The Data Analyser's page says "nothing leaves this page", and that is the
   reason a prospect can drop real data on it. A job record is synced to the
   account like any other job. So the record carries the column PROFILE —
   names, inferred SQL types, nullability, key candidates, the issue codes the
   profiler raised — and NOT ONE ROW. No samples, no min/max of a value, no
   distinct-value list. tests/data-analyser-handoff.test.js feeds this a file
   with recognisable cell values and asserts none of them appear anywhere in
   the serialised job. If a future field needs a value from the data, that
   test is the conversation to have first.

   WHY THE TYPES ARE T-SQL STRINGS

   Object Mapping reasons about a source column's type as a SQL type string:
   isCharType(), parseTypeLen(), isDateType() and the truncation warnings all
   read NVARCHAR(50) and INT, not 'string' and 'integer'. The engine already
   knows how to say a profile in T-SQL — DIALECTS.sqlserver.type(col) — and
   the console's lingua franca is SQL Server, so that is what a file column
   is described as. The engine's own type name travels alongside as
   analyserType, for anything that wants the finer distinction (email, uuid,
   money) that SQL flattens.

   The mapping happens through an injected sqlType function rather than a
   require, so this file has no dependency on the engine and can be tested
   with a stub. The page passes CygenixAnalyser.dialects.sqlserver.type.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixAnalyserHandoff) root.CygenixAnalyserHandoff = api;
})(typeof window !== 'undefined' ? window : this, function () {
  'use strict';

  /* The value that stands where a schema.table name would. The "file:" prefix
     cannot collide with anything a database schema fetch produces, and it is
     what checkEditMode matches on to find the installed entry. */
  var SOURCE_PREFIX = 'file:';

  var SEVERITY_ORDER = { high: 0, medium: 1, low: 2 };

  /* A SQL-safe identifier from a file name: orders-2024 (final).csv → orders_2024_final.
     Used only for display and for the job's auto-name — the runner never
     SELECTs from it, because the runner refuses file jobs. */
  function identifierFrom(fileName) {
    var stem = String(fileName || 'file').replace(/\.[A-Za-z0-9]+$/, '');
    var id = stem.replace(/[^A-Za-z0-9_]+/g, '_').replace(/^_+|_+$/g, '');
    if (!id) id = 'file';
    if (/^[0-9]/.test(id)) id = 'f_' + id;
    return id.slice(0, 120);
  }

  function humanFormat(format) {
    var F = {
      csv: 'CSV', 'csv-semicolon': 'CSV (semicolon)', tsv: 'TSV', psv: 'pipe-delimited',
      'fixed-width': 'fixed-width text', json: 'JSON', ndjson: 'NDJSON', xml: 'XML',
      'html-table': 'HTML table', yaml: 'YAML', ini: 'INI', 'sql-insert': 'SQL INSERT dump',
      xlsx: 'Excel', xls: 'Excel', text: 'text',
    };
    return F[format] || String(format || 'file').toUpperCase();
  }

  /**
   * Turn the Data Analyser's result into a job record.
   *
   * @param result  { dataset, columns } as the widget's `result` getter returns
   * @param opts    { name, sqlType(col) => 'NVARCHAR(50)', projectId, now }
   */
  function buildFileSourceJob(result, opts) {
    var o = opts || {};
    if (!result || !result.dataset || !Array.isArray(result.columns)) {
      throw new Error('buildFileSourceJob: needs a result with a dataset and profiled columns');
    }
    if (typeof o.sqlType !== 'function') {
      throw new Error('buildFileSourceJob: opts.sqlType(col) is required — pass the engine\'s dialect type function');
    }
    var ds = result.dataset;
    var name = String(o.name || ds.meta && ds.meta.name || 'pasted_data');
    var now = o.now || new Date().toISOString();
    var rowsProfiled = Array.isArray(ds.rows) ? ds.rows.length : 0;
    var rowsInFile = (ds.meta && typeof ds.meta.truncatedFrom === 'number') ? ds.meta.truncatedFrom : rowsProfiled;

    var columns = result.columns.map(function (c) {
      return {
        name: String(c.name),
        // What Object Mapping reads. A SQL type string, not a category.
        type: o.sqlType(c),
        // What the engine actually inferred, for anything that wants it.
        analyserType: c.type,
        confidence: c.confidence,
        nullable: !!c.nullable,
        primaryKeyCandidate: !!c.primaryKeyCandidate,
        keyEvidenceWeak: !!c.keyEvidenceWeak,
        maxLength: typeof c.maxLength === 'number' ? c.maxLength : null,
        // Codes only. The messages are re-derived on display; the values
        // that raised them stay in the browser that profiled the file.
        issues: (c.issues || []).map(function (i) { return { code: i.code, severity: i.severity }; }),
      };
    });

    var primaryKeys = columns.filter(function (c) { return c.primaryKeyCandidate; }).map(function (c) { return c.name; });

    // The warnings a job carries are human sentences shown on the job card.
    // Highest severity first, then in column order; one line per finding.
    var findings = [];
    result.columns.forEach(function (c) {
      (c.issues || []).forEach(function (i) {
        findings.push({ sev: SEVERITY_ORDER[i.severity] == null ? 3 : SEVERITY_ORDER[i.severity], text: c.name + ': ' + i.code + ' — ' + i.message });
      });
    });
    findings.sort(function (a, b) { return a.sev - b.sev; });
    var warnings = findings.map(function (f) { return f.text; });
    (ds.warnings || []).forEach(function (w) {
      if (w && w.code) warnings.push('File: ' + w.code + (w.message ? ' — ' + w.message : ''));
    });

    var sourceRef = SOURCE_PREFIX + name;
    var id = 'job_' + (o.now ? Date.parse(o.now) : Date.now());

    return {
      id: id,
      name: name + ' → (choose a target)',
      jobType: 'simple-map',
      type: 'migration',
      projectId: o.projectId || '',
      // The discriminator. Everything that must treat this job differently
      // — Object Mapping's source loader, saveAsJob, the runner — asks this
      // one field, through isFileJob() below.
      sourceKind: 'file',
      source: sourceRef,
      sourceTable: sourceRef,
      sourceObjectType: 'FILE',
      sourceBaseObjects: [],
      sourceSystem: 'File (' + humanFormat(ds.format) + ')',
      fileSource: {
        name: name,
        format: ds.format,
        formatLabel: humanFormat(ds.format),
        confidence: ds.detection && typeof ds.detection.confidence === 'number' ? ds.detection.confidence : null,
        rowCount: rowsInFile,
        rowsProfiled: rowsProfiled,
        analysedAt: now,
        columns: columns,
        primaryKeys: primaryKeys,
        fileWarnings: (ds.warnings || []).map(function (w) { return w.code; }).filter(Boolean),
      },
      target: '',
      targetTable: '',
      columnMapping: [],
      wasisRules: [],
      totalRows: rowsInFile,
      status: 'draft',
      created: now,
      fromAnalyser: true,
      warnings: warnings,
    };
  }

  function isFileJob(job) {
    return !!(job && job.sourceKind === 'file' && job.fileSource && Array.isArray(job.fileSource.columns));
  }

  /**
   * The srcTable Object Mapping expects — the same shape connectSrc() builds
   * for a database object, so selectTable(), renderSrcColList(),
   * getAllSourceCols() and the save path read it without knowing.
   */
  function fileSourceToSrcTable(job) {
    if (!isFileJob(job)) return null;
    var fs = job.fileSource;
    var ident = identifierFrom(fs.name);
    return {
      value: job.sourceTable,
      label: fs.name,
      schema: 'file',
      name: ident,
      fullName: job.sourceTable,
      rowCount: typeof fs.rowCount === 'number' ? fs.rowCount : null,
      // Not 'table' and not 'view'. Code that asks objType === 'view' gets
      // false, which is right: there are no base objects and no deferred
      // row count. Code that wants to know it is a file asks isFile.
      objType: 'file',
      isFile: true,
      columns: fs.columns.map(function (c) {
        return { name: c.name, type: c.type || '', nullable: !!c.nullable, analyserType: c.analyserType };
      }),
      primaryKeys: (fs.primaryKeys || []).slice(),
      foreignKeys: [],
    };
  }

  /** The source banner, in the words connectSrc() would use for a database. */
  function describeFileSource(job) {
    if (!isFileJob(job)) return '';
    var fs = job.fileSource;
    var n = fs.columns.length;
    var rows = typeof fs.rowCount === 'number' ? fs.rowCount.toLocaleString() + ' row' + (fs.rowCount === 1 ? '' : 's') : 'rows not counted';
    return 'Source: ' + fs.name + ' · ' + (fs.formatLabel || humanFormat(fs.format)) + ' · '
      + n + ' column' + (n === 1 ? '' : 's') + ' · ' + rows + ' · profiled by the Data Analyser';
  }

  /** The reason the runner gives when asked to run one. One sentence, with the way forward. */
  function runRefusal(job) {
    var nm = job && job.fileSource && job.fileSource.name ? job.fileSource.name : 'this file';
    return 'This job\'s source is ' + nm + ', profiled by the Data Analyser. The console holds its column '
      + 'profile, not its rows, so there is nothing to read from. Load the file through Data import '
      + '(Connections → Import) to stage it, then run from there.';
  }

  return {
    SOURCE_PREFIX: SOURCE_PREFIX,
    buildFileSourceJob: buildFileSourceJob,
    fileSourceToSrcTable: fileSourceToSrcTable,
    isFileJob: isFileJob,
    describeFileSource: describeFileSource,
    runRefusal: runRefusal,
    identifierFrom: identifierFrom,
    humanFormat: humanFormat,
  };
});
