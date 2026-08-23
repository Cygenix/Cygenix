/* ============================================================================
   cygenix-assurance.js — the Assurance engine
   ----------------------------------------------------------------------------
   Every validation rule written to prove a migration was correct becomes a
   scheduled check that runs against live data for as long as the data matters.
   This file is the deterministic core of that product: the rule model, the
   check registry that compiles declarative rules to per-dialect SQL, semantic
   tags and their heuristic proposals, binding resolution, threshold and
   baseline evaluation, the breach lifecycle, suggestions, health arithmetic
   and the monthly report.

   No DOM, no fetch, no storage: assurance.html owns the chrome and the
   connections and hands this module a schema catalogue and an execute
   function. Everything here is node-requirable and tested directly.

   Four constraints, carried from the spec and enforced in code:

   1. Engine-agnostic — no rule stores raw SQL except the custom.sql escape
      hatch; a dialect adapter compiles each check for sqlserver or postgres.
   2. Schema-agnostic — rules bind by tag first, pattern or explicit selection
      second. Binding resolves at run time, so new columns matching a tag are
      picked up without editing the rule.
   3. Additive — Quality Review / Cleansing / Validation keep working;
      promotion IMPORTS from Validation, it does not touch it.
   4. Auditable — every mutation goes through asEvent(); rules are versioned
      and never hard-deleted.

   Verdicts are computed here, deterministically, from counts SQL returned.
   The AI layer in the spec proposes tags, prose and drafts; its deterministic
   fallbacks are what this module implements, so nothing in the assurance path
   ever depends on a model being reachable.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (typeof window !== 'undefined') window.CygenixAssurance = api;
})(this, function () {
'use strict';

var STORE_VERSION = 1;
var SAMPLE_CAP_DEFAULT = 1000;
var RUN_HISTORY_CAP = 6000;      /* summaries kept; oldest dropped past this */
var FLAP_MINUTES_DEFAULT = 30;

var CATEGORIES = [
  { key: 'setups',         label: 'Setups & reference', prefix: 'SET' },
  { key: 'finance',        label: 'Finance',            prefix: 'FIN' },
  { key: 'quality',        label: 'Data quality',       prefix: 'DQ'  },
  { key: 'duplicates',     label: 'Duplicates',         prefix: 'DUP' },
  { key: 'referential',    label: 'Referential',        prefix: 'REF' },
  { key: 'drift',          label: 'Drift & freshness',  prefix: 'DRF' },
  { key: 'reconciliation', label: 'Reconciliation',     prefix: 'REC' },
  { key: 'custom',         label: 'Custom SQL',         prefix: 'CUS' },
];
var SEVERITIES = ['info', 'warning', 'critical'];

/* =======================================================================
   Dialects
   ----------------------------------------------------------------------
   Two engines, matching what db-connect actually speaks. The differences
   the spec calls out — identifier quoting, TOP vs LIMIT, date arithmetic,
   trimming, regex — live here and nowhere else.
   ======================================================================= */
var DIALECTS = {
  sqlserver: {
    ident: function (s) { return '[' + String(s).replace(/]/g, ']]') + ']'; },
    top: function (sql, n) { return sql.replace(/^SELECT /, 'SELECT TOP ' + n + ' '); },
    len: function (e) { return 'LEN(' + e + ')'; },
    trim: function (e) { return 'LTRIM(RTRIM(' + e + '))'; },
    now: 'GETUTCDATE()',
    ageHours: function (e) { return 'DATEDIFF(hour, ' + e + ', GETUTCDATE())'; },
    addDays: function (e, n) { return 'DATEADD(day, ' + (n | 0) + ', ' + e + ')'; },
    /* PATINDEX over a binary collation is the closest T-SQL has to a
       character-class scan; enough for the control-character check. */
    controlChars: function (e) {
      return 'PATINDEX(\'%[\' + CHAR(1) + \'-\' + CHAR(31) + \']%\', ' + e
           + ' COLLATE Latin1_General_BIN) > 0';
    },
    regex: null,                    /* T-SQL has no regex — compile refuses */
    soundex: function (e) { return 'SOUNDEX(' + e + ')'; },
  },
  postgres: {
    ident: function (s) { return '"' + String(s).replace(/"/g, '""') + '"'; },
    top: function (sql, n) { return sql + ' LIMIT ' + n; },
    len: function (e) { return 'LENGTH(' + e + ')'; },
    trim: function (e) { return 'BTRIM(' + e + ')'; },
    now: 'now()',
    ageHours: function (e) { return 'FLOOR(EXTRACT(EPOCH FROM (now() - ' + e + ')) / 3600)'; },
    addDays: function (e, n) { return '(' + e + " + interval '" + (n | 0) + " days')"; },
    controlChars: function (e) { return e + " ~ '[\\x01-\\x1F]'"; },
    regex: function (e, pattern) { return e + ' ~ ' + lit(pattern); },
    /* needs the fuzzystrmatch extension; the run surfaces the engine's own
       error if it is absent, which is the honest answer */
    soundex: function (e) { return 'SOUNDEX(' + e + ')'; },
  },
};

/* Detect the dialect the same way db-connect does, from the string alone. */
function asDialectOf(connectionString) {
  var s = String(connectionString || '').trim();
  if (/^(postgres|postgresql):\/\//i.test(s)) return 'postgres';
  if (/(^|;|\s)driver\s*=\s*postgres/i.test(s)) return 'postgres';
  return 'sqlserver';
}

function lit(v) { return "'" + String(v == null ? '' : v).replace(/'/g, "''") + "'"; }
function num(v, dflt) { var n = Number(v); return isFinite(n) ? n : dflt; }

/* target: { db:'src'|'tgt', schema, table, column? } */
function qTable(d, t) { return d.ident(t.schema || 'dbo') + '.' + d.ident(t.table); }
function qCol(d, name) { return d.ident(name); }

function colExpr(d, t, params) {
  var e = qCol(d, t.column);
  var norm = params && params.normalise;
  if (norm === 'trim') e = d.trim(e);
  else if (norm === 'trim_upper') e = 'UPPER(' + d.trim(e) + ')';
  return e;
}

/* =======================================================================
   Check registry
   ----------------------------------------------------------------------
   Each entry: { category, needsColumn, params:[...], compile(target,
   params, d) -> { countSql, sampleSql } }. countSql returns one row with
   one column n; sampleSql returns failing rows, capped by the caller.
   Adding a check means adding one entry — nothing else in the module
   knows the list.
   ======================================================================= */
function whereWrap(d, t, predicate, cap) {
  var from = ' FROM ' + qTable(d, t) + ' WHERE ' + predicate;
  return {
    countSql: 'SELECT COUNT(*) AS n' + from,
    sampleSql: d.top('SELECT *' + from, cap),
  };
}

/* Reference for lookup checks: a table.column or an inline list. */
function refPredicate(d, t, params) {
  var e = colExpr(d, t, params);
  var ref = params.reference || {};
  if (ref.kind === 'inline') {
    var vals = (ref.values || []).map(function (v) {
      var s = String(v);
      if (params.normalise === 'trim_upper') s = s.trim().toUpperCase();
      else if (params.normalise === 'trim') s = s.trim();
      return lit(s);
    });
    if (!vals.length) throw new Error('domain.lookup: inline reference has no values');
    return e + ' NOT IN (' + vals.join(', ') + ')';
  }
  var parts = String(ref.ref || '').split('.');
  if (parts.length < 2) throw new Error('domain.lookup: reference must be schema.table.column');
  var refCol = parts.pop(), refTable = parts.pop(), refSchema = parts.pop() || 'dbo';
  var re = qCol(d, refCol);
  if (params.normalise === 'trim') re = d.trim(re);
  else if (params.normalise === 'trim_upper') re = 'UPPER(' + d.trim(re) + ')';
  return e + ' NOT IN (SELECT ' + re + ' FROM ' + d.ident(refSchema) + '.' + d.ident(refTable)
       + ' WHERE ' + qCol(d, refCol) + ' IS NOT NULL)';
}

function nullGuard(t, d, params, predicate) {
  var c = qCol(d, t.column);
  if (params && params.nulls === 'fail') return '(' + c + ' IS NULL OR (' + predicate + '))';
  return '(' + c + ' IS NOT NULL AND (' + predicate + '))';
}

var REGISTRY = {
  /* ---- completeness ---- */
  'completeness.not_null': {
    category: 'quality', needsColumn: true,
    compile: function (t, p, d, cap) {
      return whereWrap(d, t, qCol(d, t.column) + ' IS NULL', cap);
    },
  },
  'completeness.blank_string': {
    category: 'quality', needsColumn: true,
    compile: function (t, p, d, cap) {
      return whereWrap(d, t, qCol(d, t.column) + " IS NOT NULL AND " + d.trim(qCol(d, t.column)) + " = ''", cap);
    },
  },
  'completeness.required_if': {
    category: 'quality', needsColumn: true,
    /* p.predicate is a SQL predicate over the table's own columns — the one
       place short of custom.sql where the author writes SQL, and it is a
       predicate only, wrapped in a read-only SELECT like everything else. */
    compile: function (t, p, d, cap) {
      if (!p.predicate) throw new Error('completeness.required_if needs params.predicate');
      return whereWrap(d, t, '(' + p.predicate + ') AND ' + qCol(d, t.column) + ' IS NULL', cap);
    },
  },

  /* ---- validity / domain ---- */
  'domain.lookup': {
    category: 'setups', needsColumn: true,
    compile: function (t, p, d, cap) {
      return whereWrap(d, t, nullGuard(t, d, p, refPredicate(d, t, p)), cap);
    },
  },
  'domain.enum': {
    category: 'setups', needsColumn: true,
    compile: function (t, p, d, cap) {
      return REGISTRY['domain.lookup'].compile(t,
        Object.assign({}, p, { reference: { kind: 'inline', values: p.values || [] } }), d, cap);
    },
  },
  'domain.range': {
    category: 'quality', needsColumn: true,
    compile: function (t, p, d, cap) {
      var c = qCol(d, t.column), parts = [];
      if (p.min != null) parts.push(c + ' <' + (p.minInclusive === false ? '=' : '') + ' ' + num(p.min, 0));
      if (p.max != null) parts.push(c + ' >' + (p.maxInclusive === false ? '=' : '') + ' ' + num(p.max, 0));
      if (!parts.length) throw new Error('domain.range needs min and/or max');
      return whereWrap(d, t, nullGuard(t, d, p, parts.join(' OR ')), cap);
    },
  },
  'domain.length': {
    category: 'quality', needsColumn: true,
    compile: function (t, p, d, cap) {
      var e = d.len(qCol(d, t.column)), parts = [];
      if (p.min != null) parts.push(e + ' < ' + num(p.min, 0));
      if (p.max != null) parts.push(e + ' > ' + num(p.max, 0));
      if (!parts.length) throw new Error('domain.length needs min and/or max');
      return whereWrap(d, t, nullGuard(t, d, p, parts.join(' OR ')), cap);
    },
  },
  'domain.date_sanity': {
    category: 'quality', needsColumn: true,
    compile: function (t, p, d, cap) {
      var c = qCol(d, t.column), parts = [];
      if (p.after) parts.push(c + ' < ' + lit(p.after));
      if (p.before) parts.push(c + ' > ' + lit(p.before));
      var tol = num(p.futureToleranceDays, 0);
      if (p.noFuture !== false) parts.push(c + ' > ' + d.addDays(d.now, tol));
      if (!parts.length) throw new Error('domain.date_sanity needs after/before/noFuture');
      return whereWrap(d, t, nullGuard(t, d, p, parts.join(' OR ')), cap);
    },
  },
  'domain.regex': {
    category: 'quality', needsColumn: true,
    compile: function (t, p, d, cap) {
      if (!d.regex) {
        throw new Error('domain.regex is not available on SQL Server — use format.pattern (LIKE) '
          + 'or domain.enum instead');
      }
      if (!p.pattern) throw new Error('domain.regex needs params.pattern');
      return whereWrap(d, t, nullGuard(t, d, p, 'NOT (' + d.regex(colExpr(d, t, p), p.pattern) + ')'), cap);
    },
  },

  /* ---- format ---- */
  'format.pattern': {
    /* LIKE-based, so it compiles on both engines; the rows that FAIL the
       pattern are the failures. */
    category: 'quality', needsColumn: true,
    compile: function (t, p, d, cap) {
      if (!p.like) throw new Error('format.pattern needs params.like');
      return whereWrap(d, t, nullGuard(t, d, p, colExpr(d, t, p) + ' NOT LIKE ' + lit(p.like)), cap);
    },
  },
  'format.max_length': {
    /* truncation suspects: values sitting exactly at the declared max */
    category: 'quality', needsColumn: true,
    compile: function (t, p, d, cap) {
      var max = num(p.declaredMax, 0);
      if (!max) throw new Error('format.max_length needs params.declaredMax');
      return whereWrap(d, t, d.len(qCol(d, t.column)) + ' = ' + max, cap);
    },
  },
  'format.control_chars': {
    category: 'quality', needsColumn: true,
    compile: function (t, p, d, cap) {
      return whereWrap(d, t, nullGuard(t, d, p, d.controlChars(qCol(d, t.column))), cap);
    },
  },
  'format.whitespace': {
    category: 'quality', needsColumn: true,
    compile: function (t, p, d, cap) {
      var c = qCol(d, t.column);
      return whereWrap(d, t, nullGuard(t, d, p, c + ' <> ' + d.trim(c)), cap);
    },
  },
  'format.case_consistency': {
    /* values that differ from another value only by case */
    category: 'quality', needsColumn: true,
    compile: function (t, p, d, cap) {
      var c = qCol(d, t.column);
      var from = ' FROM (SELECT UPPER(' + c + ') AS u FROM ' + qTable(d, t)
               + ' WHERE ' + c + ' IS NOT NULL GROUP BY UPPER(' + c + ')'
               + ' HAVING COUNT(DISTINCT ' + c + ') > 1) x';
      return {
        countSql: 'SELECT COUNT(*) AS n' + from,
        sampleSql: d.top('SELECT *' + from, cap),
      };
    },
  },

  /* ---- uniqueness ---- */
  'uniqueness.key': {
    category: 'duplicates', needsColumn: true,
    compile: function (t, p, d, cap) {
      return REGISTRY['uniqueness.composite'].compile(
        Object.assign({}, t, { column: null }),
        Object.assign({}, p, { columns: [t.column] }), d, cap);
    },
  },
  'uniqueness.composite': {
    category: 'duplicates',
    compile: function (t, p, d, cap) {
      var cols = (p.columns || []).map(function (c) { return qCol(d, c); });
      if (!cols.length) throw new Error('uniqueness.composite needs params.columns');
      var list = cols.join(', ');
      var from = ' FROM (SELECT ' + list + ', COUNT(*) AS dup_n FROM ' + qTable(d, t)
               + ' GROUP BY ' + list + ' HAVING COUNT(*) > 1) x';
      return {
        /* excess rows, not duplicate groups: 3 identical rows = 2 failures */
        countSql: 'SELECT COALESCE(SUM(dup_n - 1), 0) AS n' + from,
        sampleSql: d.top('SELECT *' + from, cap),
      };
    },
  },
  'uniqueness.fuzzy': {
    category: 'duplicates',
    compile: function (t, p, d, cap) {
      /* An unblocked fuzzy match is O(n²); the blocking key is mandatory. */
      if (!p.blockingKey) throw new Error('uniqueness.fuzzy requires params.blockingKey');
      if ((p.algorithm || 'soundex') !== 'soundex') {
        throw new Error('uniqueness.fuzzy: only the soundex algorithm compiles to SQL here; '
          + 'levenshtein and jaro_winkler need the engine-side extension pass (deferred)');
      }
      var col = qCol(d, p.column || t.column), bk = qCol(d, p.blockingKey);
      var T = qTable(d, t);
      var join = ' FROM ' + T + ' a JOIN ' + T + ' b ON a.' + bk + ' = b.' + bk
               + ' AND ' + d.soundex('a.' + col) + ' = ' + d.soundex('b.' + col)
               + ' AND a.' + col + ' <> b.' + col
               + ' AND a.' + col + ' < b.' + col;
      return {
        countSql: 'SELECT COUNT(*) AS n' + join,
        sampleSql: d.top('SELECT a.' + col + ' AS value_a, b.' + col + ' AS value_b, a.' + bk
                 + ' AS blocking_key' + join, cap),
      };
    },
  },

  /* ---- referential ---- */
  'referential.orphan': {
    category: 'referential', needsColumn: true,
    compile: function (t, p, d, cap) {
      var parent = p.parent || {};
      if (!parent.table || !parent.column) throw new Error('referential.orphan needs params.parent {schema,table,column}');
      var c = qCol(d, t.column);
      var P = d.ident(parent.schema || 'dbo') + '.' + d.ident(parent.table);
      var pc = qCol(d, parent.column);
      var from = ' FROM ' + qTable(d, t) + ' c LEFT JOIN ' + P + ' p ON c.' + c + ' = p.' + pc
               + ' WHERE c.' + c + ' IS NOT NULL AND p.' + pc + ' IS NULL';
      return {
        countSql: 'SELECT COUNT(*) AS n' + from,
        sampleSql: d.top('SELECT c.*' + from, cap),
      };
    },
  },
  'referential.childless_parent': {
    category: 'referential', needsColumn: true,
    compile: function (t, p, d, cap) {
      var child = p.child || {};
      if (!child.table || !child.column) throw new Error('referential.childless_parent needs params.child');
      var c = qCol(d, t.column);
      var C = d.ident(child.schema || 'dbo') + '.' + d.ident(child.table);
      var cc = qCol(d, child.column);
      var from = ' FROM ' + qTable(d, t) + ' p LEFT JOIN ' + C + ' c ON p.' + c + ' = c.' + cc
               + ' WHERE c.' + cc + ' IS NULL';
      return {
        countSql: 'SELECT COUNT(*) AS n' + from,
        sampleSql: d.top('SELECT p.*' + from, cap),
      };
    },
  },

  /* ---- aggregate / finance ---- */
  'aggregate.balance': {
    category: 'finance',
    compile: function (t, p, d, cap) {
      if (!p.amount) throw new Error('aggregate.balance needs params.amount');
      var amount = qCol(d, p.amount);
      var groups = (p.groupBy || []).map(function (c) { return qCol(d, c); });
      var tol = num(p.tolerance, 0);
      var from, inner;
      if (groups.length) {
        inner = 'SELECT ' + groups.join(', ') + ', SUM(' + amount + ') AS balance FROM '
              + qTable(d, t) + ' GROUP BY ' + groups.join(', ')
              + ' HAVING ABS(SUM(' + amount + ')) > ' + tol;
      } else {
        inner = 'SELECT SUM(' + amount + ') AS balance FROM ' + qTable(d, t)
              + ' HAVING ABS(SUM(' + amount + ')) > ' + tol;
      }
      from = ' FROM (' + inner + ') x';
      return { countSql: 'SELECT COUNT(*) AS n' + from, sampleSql: d.top('SELECT *' + from, cap) };
    },
  },
  'aggregate.parent_child': {
    category: 'finance',
    compile: function (t, p, d, cap) {
      var ch = p.child || {};
      if (!p.headerTotal || !ch.table || !ch.amount || !ch.key || !p.key) {
        throw new Error('aggregate.parent_child needs headerTotal, key, child {table, key, amount}');
      }
      var tol = num(p.tolerance, 0);
      var P = qTable(d, t), C = d.ident(ch.schema || 'dbo') + '.' + d.ident(ch.table);
      var from = ' FROM ' + P + ' h JOIN (SELECT ' + qCol(d, ch.key) + ' AS k, SUM('
               + qCol(d, ch.amount) + ') AS line_total FROM ' + C + ' GROUP BY ' + qCol(d, ch.key)
               + ') l ON h.' + qCol(d, p.key) + ' = l.k'
               + ' WHERE ABS(h.' + qCol(d, p.headerTotal) + ' - l.line_total) > ' + tol;
      return {
        countSql: 'SELECT COUNT(*) AS n' + from,
        sampleSql: d.top('SELECT h.' + qCol(d, p.key) + ' AS key_value, h.' + qCol(d, p.headerTotal)
                 + ' AS header_total, l.line_total' + from, cap),
      };
    },
  },
  'aggregate.tolerance': {
    /* recomputed expression vs stored column — the rounding-drift check */
    category: 'finance', needsColumn: true,
    compile: function (t, p, d, cap) {
      if (!p.expression) throw new Error('aggregate.tolerance needs params.expression');
      var eps = num(p.epsilon, 0.01);
      var pred = 'ABS((' + p.expression + ') - ' + qCol(d, t.column) + ') > ' + eps;
      return whereWrap(d, t, nullGuard(t, d, p, pred), cap);
    },
  },

  /* ---- drift (SQL side; the judgment happens against the baseline) ---- */
  'drift.rowcount': {
    category: 'drift', kind: 'measure',
    compile: function (t, p, d) {
      return { countSql: 'SELECT COUNT(*) AS n FROM ' + qTable(d, t), sampleSql: null };
    },
  },
  'drift.freshness': {
    category: 'drift', kind: 'measure', needsColumn: true,
    compile: function (t, p, d) {
      return {
        countSql: 'SELECT ' + d.ageHours('MAX(' + qCol(d, t.column) + ')') + ' AS n FROM ' + qTable(d, t),
        sampleSql: null,
      };
    },
  },
  'drift.null_rate': {
    category: 'drift', kind: 'measure', needsColumn: true,
    compile: function (t, p, d) {
      var c = qCol(d, t.column);
      return {
        countSql: 'SELECT COUNT(*) AS total, SUM(CASE WHEN ' + c + ' IS NULL THEN 1 ELSE 0 END) AS n'
                + ' FROM ' + qTable(d, t),
        sampleSql: null,
      };
    },
  },

  /* ---- reconciliation (two queries; the engine compares) ---- */
  'recon.rowcount': {
    category: 'reconciliation', kind: 'pair',
    compile: function (t, p, d) {
      return { countSql: 'SELECT COUNT(*) AS n FROM ' + qTable(d, t), sampleSql: null };
    },
  },
  'recon.column_aggregate': {
    category: 'reconciliation', kind: 'pair', needsColumn: true,
    compile: function (t, p, d) {
      var fn = { sum: 'SUM', min: 'MIN', max: 'MAX', count: 'COUNT' }[p.fn || 'sum'];
      if (!fn) throw new Error('recon.column_aggregate: fn must be sum|min|max|count');
      return { countSql: 'SELECT ' + fn + '(' + qCol(d, t.column) + ') AS n FROM ' + qTable(d, t), sampleSql: null };
    },
  },

  /* ---- escape hatch ---- */
  'custom.sql': {
    category: 'custom',
    compile: function (t, p, d, cap) {
      var sql = (p.sql && (p.sql[d === DIALECTS.postgres ? 'postgres' : 'sqlserver'] || p.sql.any)) || '';
      sql = String(sql).trim().replace(/;+\s*$/, '');
      if (!sql) throw new Error('custom.sql needs params.sql for this dialect');
      if (!/^select[\s(]/i.test(sql)) throw new Error('custom.sql must be a SELECT returning failing rows');
      return {
        countSql: 'SELECT COUNT(*) AS n FROM (' + sql + ') x',
        sampleSql: d.top('SELECT * FROM (' + sql + ') x', cap),
      };
    },
  },
};

function asChecks() { return Object.keys(REGISTRY).sort(); }

function asCompile(rule, target, dialectName, sampleCap) {
  var entry = REGISTRY[rule.check];
  if (!entry) throw new Error('Unknown check: ' + rule.check);
  var d = DIALECTS[dialectName];
  if (!d) throw new Error('Unknown dialect: ' + dialectName);
  if (entry.needsColumn && !target.column && !(rule.params && rule.params.column)) {
    throw new Error(rule.check + ' needs a column on its target');
  }
  var out = entry.compile(target, rule.params || {}, d, sampleCap || SAMPLE_CAP_DEFAULT);
  out.kind = entry.kind || 'count';
  return out;
}

/* =======================================================================
   Semantic tags — proposals are heuristic and deterministic (the spec's
   A1 fallback). Confidence expresses how many signals agreed, not a
   model's opinion. The closed vocabulary lives here.
   ======================================================================= */
var TAG_FAMILIES = ['money', 'money:base', 'money:txn', 'currency', 'date', 'date:event',
  'date:audit', 'business_key', 'surrogate_key', 'pii:email', 'pii:name', 'pii:phone',
  'pii:postcode', 'text', 'fact_table', 'dimension'];

function words(name) {
  return String(name == null ? '' : name)
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .toLowerCase().split(/[^a-z0-9]+/).filter(Boolean);
}

/* col: { db, schema, table, column, dataType, isPk, rowCount, siblings:[names] }
   Returns proposals [{ ...ref, tag, confidence, rationale }]. */
function asProposeTags(cols, refTables) {
  var out = [];
  var refNames = (refTables || []).map(function (t) { return words(t).join(''); });
  (cols || []).forEach(function (c) {
    var w = words(c.column), joined = w.join('');
    var type = String(c.dataType || '').toLowerCase();
    var sib = (c.siblings || []).map(function (s) { return words(s).join(''); });
    var add = function (tag, confidence, rationale) {
      out.push({ db: c.db, schema: c.schema, table: c.table, column: c.column,
                 tag: tag, confidence: confidence, rationale: rationale, source: 'auto' });
    };

    /* the harvest abbreviates types to three characters, so match prefixes */
    if (/^(dec|num|mon|flo|rea|sma?llmoney)/.test(type)) {
      if (/base$/.test(joined)) add('money:base', 0.9, 'decimal type with a _base suffix');
      else if (/(amount|amt|total|value|price|cost|net|gross)/.test(joined)) {
        add('money', sib.some(function (s) { return /currency|ccy/.test(s); }) ? 0.92 : 0.78,
          'decimal type named like an amount' + (sib.some(function (s) { return /currency|ccy/.test(s); })
            ? ', with a currency sibling' : ''));
      }
    }
    if (/(^|)(currency|ccy)(code)?$/.test(joined) && /^(cha|var|nch|nva|tex|str)/.test(type)) {
      add('currency', 0.9, 'name says currency, character type');
    }
    if (/^(dat|tim|sma)/.test(type) || /date|time/.test(type)) {
      if (/(createdat|updatedat|loadts|loadedat|modifiedat|audit)/.test(joined)) {
        add('date:audit', 0.85, 'audit-style name on a date column');
      } else if (/(date|at|on|ts)$/.test(joined) || /date/.test(joined)) {
        add('date', 0.7, 'date type');
      }
    }
    if (c.isPk) {
      add(/id$|no$|num$|code$/.test(joined) && !/^(id)$/.test(joined) ? 'business_key' : 'surrogate_key',
        0.8, c.isPk ? 'primary key' : '');
    }
    if (/email|eml/.test(joined)) add('pii:email', 0.85, 'name says email');
    if (/(firstname|lastname|surname|fullname|givenname)/.test(joined)) add('pii:name', 0.8, 'name column');
    if (/(postcode|postalcode|zip)/.test(joined)) add('pii:postcode', 0.85, 'postcode column');
    if (/phone|mobile|tel$/.test(joined)) add('pii:phone', 0.8, 'phone column');

    /* lookup:* — a column whose name matches a reference table in the estate */
    refNames.forEach(function (rn, i) {
      if (!rn) return;
      if (joined === rn || joined === rn + 'code' || joined === rn + 'id' || joined === 'cd' + rn) {
        add('lookup:' + words(refTables[i]).join('_'), 0.82,
          'name matches reference table ' + refTables[i]);
      }
    });

    if (/^(cha|var|nch|nva|tex|str)/.test(type) && !out.some(function (p) {
      return p.column === c.column && p.table === c.table && p.tag !== 'text';
    })) {
      add('text', 0.6, 'character column with no stronger signal');
    }
  });
  return out;
}

function tagKey(t) { return [t.db, t.schema, t.table, t.column || '', t.tag].join('|'); }

/* =======================================================================
   Binding resolution — at run time, never at author time
   ----------------------------------------------------------------------
   catalog: { tables: [{ db, schema, table, rowCount, columns:[names] }],
              tags:   [tag records] }
   ======================================================================= */
function globToRe(pattern) {
  return new RegExp('^' + String(pattern).split('*').map(function (s) {
    return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }).join('.*') + '$', 'i');
}

function asResolveBinding(rule, catalog) {
  var b = rule.binding || {};
  var dbs = b.databases || [];
  var inScope = function (db) { return !dbs.length || dbs.indexOf(db) >= 0; };
  var out = [];

  if (b.mode === 'tag') {
    (catalog.tags || []).forEach(function (t) {
      if (t.tag !== b.tag || !inScope(t.db)) return;
      out.push({ db: t.db, schema: t.schema, table: t.table, column: t.column || null });
    });
  } else if (b.mode === 'pattern') {
    var re = globToRe(b.pattern || '');
    (catalog.tables || []).forEach(function (t) {
      if (!inScope(t.db)) return;
      (t.columns || []).forEach(function (c) {
        var name = [t.db, t.schema, t.table, c].join('.');
        if (re.test(name)) out.push({ db: t.db, schema: t.schema, table: t.table, column: c });
      });
      /* a table-level pattern (three segments) binds the table itself */
      if (String(b.pattern || '').split('.').length === 3
          && globToRe(b.pattern).test([t.db, t.schema, t.table].join('.'))) {
        out.push({ db: t.db, schema: t.schema, table: t.table, column: null });
      }
    });
  } else {
    (b.targets || []).forEach(function (t) {
      if (inScope(t.db)) out.push({ db: t.db, schema: t.schema, table: t.table, column: t.column || null });
    });
  }

  /* dedupe */
  var seen = {}, uniq = [];
  out.forEach(function (t) {
    var k = [t.db, t.schema, t.table, t.column || ''].join('|');
    if (!seen[k]) { seen[k] = 1; uniq.push(t); }
  });
  return uniq;
}

/* =======================================================================
   Evaluation — thresholds, baselines, verdicts
   ======================================================================= */
function overThreshold(th, failed, scanned) {
  if (!th || th.value == null) return false;
  if (th.unit === 'percent') {
    if (!scanned) return false;
    return (failed / scanned) * 100 > th.value;
  }
  return failed > th.value;
}

/* baselineValues: prior comparable measurements (newest last). */
function asEvaluate(rule, totals, baselineValues) {
  var th = rule.thresholds || {};
  var failed = totals.rowsFailed || 0, scanned = totals.rowsScanned || 0;
  var status = 'pass', why = null;

  if (overThreshold(th.failAbove, failed, scanned)) { status = 'fail'; why = 'over the fail threshold'; }
  else if (overThreshold(th.warnAbove, failed, scanned)) { status = 'warn'; why = 'over the warn threshold'; }

  var base = th.baseline;
  if (base && base.enabled && baselineValues && baselineValues.length) {
    var mean = baselineValues.reduce(function (a, b) { return a + b; }, 0) / baselineValues.length;
    var measure = totals.measure != null ? totals.measure : failed;
    if (mean > 0) {
      var deltaPct = ((measure - mean) / mean) * 100;
      var band = num(base.worseByPercent, 10);
      if (Math.abs(deltaPct) > band) {
        if (status === 'pass') status = 'warn';
        why = (deltaPct > 0 ? '+' : '') + deltaPct.toFixed(1) + '% vs the ' + (base.window || '7d') + ' baseline';
      }
    }
  }
  return { status: status, why: why };
}

/* Drift and recon checks fail on the MEASURE, not on failing rows. */
function asEvaluateMeasure(rule, measure, baselineValues) {
  var p = rule.params || {};
  if (rule.check === 'drift.freshness') {
    var maxAge = num(p.maxAgeHours, 26);
    return measure > maxAge
      ? { status: 'fail', why: measure + 'h stale (limit ' + maxAgeHours(p) + 'h)' }
      : { status: 'pass', why: null };
  }
  return asEvaluate(rule, { rowsFailed: 0, rowsScanned: 0, measure: measure }, baselineValues);
  function maxAgeHours(pp) { return num(pp.maxAgeHours, 26); }
}

/* recon pair: compare source and target numbers with a tolerance */
function asEvaluatePair(rule, a, b) {
  var tol = num((rule.params || {}).tolerance, 0);
  var diff = Math.abs(num(a, 0) - num(b, 0));
  return diff > tol
    ? { status: 'fail', why: 'source ' + a + ' vs target ' + b + ' (Δ ' + diff + ')' }
    : { status: 'pass', why: null };
}

/* =======================================================================
   Store — one JSON document, shaped for localStorage but owned here
   ======================================================================= */
function asNewStore(now) {
  return {
    v: STORE_VERSION, createdAt: now || 0, seq: {},
    rules: [], ruleVersions: [], tags: [], tagProposals: [],
    groups: asDefaultGroups(), runs: [], breaches: [], events: [],
    baselines: {}, suggestions: [], dismissals: {}, suppressions: [], reports: [],
    settings: {
      quietHours: { from: '22:00', to: '06:00', criticalStillPages: true },
      flapMinutes: FLAP_MINUTES_DEFAULT,
      freezeWindows: [],
      sampleCap: SAMPLE_CAP_DEFAULT,
      sampleRetentionDays: 90,
      routing: {
        critical: { channels: ['webhook', 'email'], escalateMinutes: 15 },
        warning:  { channels: ['webhook'], escalateMinutes: null },
        info:     { channels: [], escalateMinutes: null },
      },
    },
  };
}

function asDefaultGroups() {
  return [
    { id: 'grp_nightly_close',   name: 'Nightly financial close', match: ['finance', 'reconciliation'], cron: '0 4 * * *' },
    { id: 'grp_hourly_fresh',    name: 'Hourly freshness',        match: ['drift'],                     cron: '0 * * * *' },
    { id: 'grp_core_integrity',  name: 'Core integrity',          match: ['referential', 'duplicates'], cron: '0 */6 * * *' },
    { id: 'grp_weekly_deep',     name: 'Weekly deep scan',        match: ['quality'],                   cron: '0 5 * * 0' },
    { id: 'grp_profile',         name: 'Profile & suggest',       match: [],                            cron: '0 2 * * *' },
  ];
}

function asEvent(store, ev, now) {
  store.events.push(Object.assign({ at: now || 0 }, ev));
  return ev;
}

function categoryOf(key) {
  for (var i = 0; i < CATEGORIES.length; i++) if (CATEGORIES[i].key === key) return CATEGORIES[i];
  return CATEGORIES[CATEGORIES.length - 1];
}

function asNewRuleId(store, category) {
  var prefix = categoryOf(category).prefix;
  store.seq[prefix] = (store.seq[prefix] || 0) + 1;
  var n = store.seq[prefix];
  return prefix + '-' + (n < 10 ? '00' + n : n < 100 ? '0' + n : n);
}

function defaultGroupFor(store, category) {
  var g = (store.groups || []).filter(function (x) { return (x.match || []).indexOf(category) >= 0; })[0];
  return g ? g.id : (store.groups[0] && store.groups[0].id) || null;
}

function asSaveRule(store, draft, user, now) {
  var existing = draft.id && store.rules.filter(function (r) { return r.id === draft.id; })[0];
  if (existing) {
    /* versioned, never overwritten in place */
    store.ruleVersions.push(JSON.parse(JSON.stringify(existing)));
    var next = Object.assign({}, existing, draft, { version: (existing.version || 1) + 1 });
    store.rules[store.rules.indexOf(existing)] = next;
    asEvent(store, { type: 'rule.updated', ruleId: next.id, by: user, version: next.version }, now);
    return next;
  }
  var rule = Object.assign({
    id: asNewRuleId(store, draft.category || 'custom'),
    name: 'Untitled rule', category: 'custom', check: 'custom.sql', params: {},
    binding: { mode: 'explicit', tag: null, pattern: null, targets: [], databases: [] },
    thresholds: { warnAbove: { unit: 'rows', value: 0 }, failAbove: { unit: 'rows', value: 0 },
                  baseline: { enabled: false, window: '7d', worseByPercent: 10 } },
    severity: 'warning', owner: user || null,
    continuous: true,                              /* the retention flag — default TRUE */
    schedule: { groupId: null, cron: null },
    onBreach: 'notify',
    provenance: { source: 'authored', projectId: null, createdAt: now || 0 },
    enabled: true, version: 1,
  }, draft);
  if (!rule.schedule.groupId) rule.schedule.groupId = defaultGroupFor(store, rule.category);
  store.rules.push(rule);
  asEvent(store, { type: 'rule.created', ruleId: rule.id, by: user, name: rule.name }, now);
  return rule;
}

function asDisableRule(store, ruleId, user, now) {
  var r = store.rules.filter(function (x) { return x.id === ruleId; })[0];
  if (!r) return null;
  store.ruleVersions.push(JSON.parse(JSON.stringify(r)));
  r.enabled = false;
  r.version = (r.version || 1) + 1;
  asEvent(store, { type: 'rule.disabled', ruleId: ruleId, by: user }, now);
  return r;
}

/* =======================================================================
   Runs and the breach lifecycle
   ----------------------------------------------------------------------
   One breach is one incident across many runs. It opens on the first
   fail, stays open while runs keep failing, and resolves when a run
   passes — unless it resolved so recently that reopening would be flap.
   ======================================================================= */
function asRecordRun(store, run, now) {
  store.runs.push(run);
  if (store.runs.length > RUN_HISTORY_CAP) store.runs.splice(0, store.runs.length - RUN_HISTORY_CAP);

  /* rolling baseline per rule: the measure when there is one, else failures */
  var key = run.ruleId;
  var b = store.baselines[key] || (store.baselines[key] = []);
  b.push({ at: run.startedAt, value: run.measure != null ? run.measure : (run.rowsFailed || 0) });
  if (b.length > 60) b.splice(0, b.length - 60);

  if (run.targetsMatched === 0) {
    asEvent(store, { type: 'run.no_targets', ruleId: run.ruleId, runId: run.id,
      note: 'targets_matched: 0 — the rule no longer binds to anything' }, now);
  }
  return asBreachUpdate(store, run, now);
}

function openBreachFor(store, ruleId) {
  return store.breaches.filter(function (b) {
    return b.ruleId === ruleId && b.state !== 'resolved';
  })[0] || null;
}

function activeSuppression(store, ruleId, now) {
  return (store.suppressions || []).filter(function (s) {
    return s.ruleId === ruleId && (!s.expiresAt || s.expiresAt > now);
  })[0] || null;
}

function asBreachUpdate(store, run, now) {
  now = now || run.startedAt || 0;
  var open = openBreachFor(store, run.ruleId);
  var failing = run.status === 'fail' || run.status === 'warn';

  if (failing) {
    var sup = activeSuppression(store, run.ruleId, now);
    if (open) {
      open.currentRowsFailed = run.rowsFailed || 0;
      open.peakRowsFailed = Math.max(open.peakRowsFailed || 0, run.rowsFailed || 0);
      open.lastRunId = run.id;
      open.runCount = (open.runCount || 1) + 1;
      return open;
    }
    /* flap guard: a breach that resolved moments ago continues instead of
       opening a twin */
    var flapMs = (store.settings.flapMinutes || FLAP_MINUTES_DEFAULT) * 60000;
    var recent = store.breaches.filter(function (b) {
      return b.ruleId === run.ruleId && b.state === 'resolved'
          && b.resolvedAt != null && (now - b.resolvedAt) <= flapMs;
    }).sort(function (a, b) { return b.resolvedAt - a.resolvedAt; })[0];
    if (recent) {
      recent.state = 'open';
      recent.resolvedAt = null;
      recent.resolution = null;
      recent.reopened = (recent.reopened || 0) + 1;
      recent.currentRowsFailed = run.rowsFailed || 0;
      recent.lastRunId = run.id;
      asEvent(store, { type: 'breach.reopened', breachId: recent.id, ruleId: run.ruleId,
        runId: run.id, note: 'reopened inside the flap window' }, now);
      return recent;
    }
    var br = {
      id: 'br_' + now.toString(36) + '_' + Math.floor((store.breaches.length + 1)),
      ruleId: run.ruleId, openedAt: now, firstRunId: run.id, lastRunId: run.id,
      state: sup ? 'suppressed' : 'open', suppressionId: sup ? sup.id : null,
      severity: run.severity || 'warning',
      peakRowsFailed: run.rowsFailed || 0, currentRowsFailed: run.rowsFailed || 0,
      runCount: 1, assignee: null, resolvedAt: null, resolution: null,
      context: run.context || null,
    };
    store.breaches.push(br);
    asEvent(store, { type: 'breach.opened', breachId: br.id, ruleId: run.ruleId, runId: run.id,
      rows: run.rowsFailed || 0, suppressed: !!sup }, now);
    return br;
  }

  if (run.status === 'pass' && open) {
    open.state = 'resolved';
    open.resolvedAt = now;
    open.resolution = open.resolution || 'cleared';
    open.currentRowsFailed = 0;
    asEvent(store, { type: 'breach.resolved', breachId: open.id, ruleId: run.ruleId, runId: run.id }, now);
    return open;
  }
  return open;
}

function asAcknowledge(store, breachId, user, now) {
  var b = store.breaches.filter(function (x) { return x.id === breachId; })[0];
  if (!b) return null;
  b.state = 'acknowledged';
  b.assignee = b.assignee || user || null;
  asEvent(store, { type: 'breach.acknowledged', breachId: breachId, by: user }, now);
  return b;
}

function asResolve(store, breachId, user, resolution, now) {
  var b = store.breaches.filter(function (x) { return x.id === breachId; })[0];
  if (!b) return null;
  b.state = 'resolved';
  b.resolvedAt = now || 0;
  b.resolution = resolution || 'resolved';
  asEvent(store, { type: 'breach.resolved', breachId: breachId, by: user, resolution: b.resolution }, now);
  return b;
}

/* Suppression REQUIRES a reason and an expiry — a silent permanent mute is
   how assurance products quietly become worthless. */
function asSuppress(store, ruleId, opts, user, now) {
  if (!opts || !opts.reason || !String(opts.reason).trim()) {
    throw new Error('Suppression requires a reason');
  }
  if (!opts.expiresAt || opts.expiresAt <= (now || 0)) {
    throw new Error('Suppression requires a future expiry');
  }
  var sup = { id: 'sup_' + (now || 0).toString(36) + '_' + (store.suppressions.length + 1),
    ruleId: ruleId, reason: String(opts.reason).trim(), expiresAt: opts.expiresAt,
    createdBy: user || null, createdAt: now || 0 };
  store.suppressions.push(sup);
  var open = openBreachFor(store, ruleId);
  if (open) { open.state = 'suppressed'; open.suppressionId = sup.id; }
  asEvent(store, { type: 'breach.suppressed', ruleId: ruleId, by: user,
    reason: sup.reason, expiresAt: sup.expiresAt }, now);
  return sup;
}

/* =======================================================================
   Diagnosis — §7's six signals rendered as prose, deterministically.
   The model version of this constrains itself to the same six; this is
   the fallback that must always work, so it IS the implementation here.
   ======================================================================= */
function asDiagnoseProse(signals) {
  var s = signals || {};
  var parts = [];
  if (s.newValues && s.newValues.length) {
    parts.push((s.newValues.length === 1 ? 'One value appeared' : s.newValues.length + ' values appeared')
      + ' that previous passing runs never saw: ' + s.newValues.slice(0, 5).join(', ') + '.');
  }
  if (s.insertWindow && s.insertWindow.from) {
    parts.push('Every failing row was written between ' + s.insertWindow.from + ' and '
      + (s.insertWindow.to || s.insertWindow.from)
      + (s.writtenBy ? ' by ' + s.writtenBy : '') + '.');
  } else if (s.writtenBy) {
    parts.push('The failing rows were written by ' + s.writtenBy + '.');
  }
  if (s.schemaChanges === 0 || (Array.isArray(s.schemaChanges) && !s.schemaChanges.length)) {
    parts.push('No schema change touched the table in the last 48 hours.');
  } else if (Array.isArray(s.schemaChanges) && s.schemaChanges.length) {
    parts.push('The table\'s schema changed recently: ' + s.schemaChanges.join('; ') + '.');
  }
  if (s.referenceAgeDays != null) {
    parts.push('The reference set last changed ' + s.referenceAgeDays + ' day'
      + (s.referenceAgeDays === 1 ? '' : 's') + ' ago'
      + (s.referenceAgeDays > 14 ? ' — new codes in the data with an old reference set points at a rollout that never reached the lookup, not corrupted data' : '') + '.');
  }
  if (s.correlatedBreaches && s.correlatedBreaches.length) {
    parts.push('Broke alongside ' + s.correlatedBreaches.join(', ') + ' in the same window.');
  } else if (s.correlatedBreaches) {
    parts.push('No other rule on this table broke in the same window.');
  }
  return parts.length ? parts.join(' ')
    : 'No diagnostic signals could be gathered for this breach — the failing rows themselves are below.';
}

/* =======================================================================
   Suggestions — the nightly profiler's deterministic proposals (§8)
   ======================================================================= */
function asSuggest(store, catalog, now) {
  var out = [];
  var dismissed = store.dismissals || {};
  var have = {};
  store.rules.forEach(function (r) { have[r.check + '|' + JSON.stringify(r.binding)] = 1; });
  var propose = function (sug) {
    var key = sug.key;
    if (dismissed[key]) return;
    if (store.suggestions.some(function (s) { return s.key === key; })) return;
    out.push(Object.assign({ id: 'sug_' + (now || 0).toString(36) + '_' + (out.length + 1),
      key: key, proposedAt: now || 0, state: 'proposed' }, sug));
  };

  (catalog.tables || []).forEach(function (t) {
    var cols = t.columns || [];
    /* audit-date column → freshness */
    var auditCol = cols.filter(function (c) { return /^(updated_at|created_at|load_ts|loaded_at|modified|lastmodified|updatedate|loaddate)$/i.test(String(c).replace(/[^a-z_]/gi, '')); })[0]
      || cols.filter(function (c) { return /(_at|_ts|date)$/i.test(c) && /load|updat|creat|modif/i.test(c); })[0];
    if (auditCol && t.rowCount > 0) {
      propose({
        key: 'freshness|' + t.db + '.' + t.schema + '.' + t.table,
        check: 'drift.freshness', category: 'drift',
        title: t.table + ' has an audit date — watch its freshness',
        detail: 'max(' + auditCol + ') age against a 26h limit.',
        draft: { name: 'Freshness — ' + t.table, category: 'drift', check: 'drift.freshness',
          params: { maxAgeHours: 26 },
          binding: { mode: 'explicit', targets: [{ db: t.db, schema: t.schema, table: t.table, column: auditCol }], databases: [] },
          severity: 'warning' },
      });
    }
    /* unique/pk → uniqueness */
    if (t.primaryKeys && t.primaryKeys.length) {
      propose({
        key: 'unique|' + t.db + '.' + t.schema + '.' + t.table,
        check: 'uniqueness.composite', category: 'duplicates',
        title: t.table + ' has a declared key — keep it unique',
        detail: 'Composite uniqueness over ' + t.primaryKeys.join(', ') + '.',
        draft: { name: 'Unique key — ' + t.table, category: 'duplicates', check: 'uniqueness.composite',
          params: { columns: t.primaryKeys },
          binding: { mode: 'explicit', targets: [{ db: t.db, schema: t.schema, table: t.table, column: null }], databases: [] },
          severity: 'critical' },
      });
    }
  });

  /* lookup:* — tagged columns without a lookup rule on that tag */
  (store.tags || []).forEach(function (tg) {
    if (!/^lookup:/.test(tg.tag)) return;
    var bound = store.rules.some(function (r) {
      return r.check === 'domain.lookup' && r.binding && r.binding.mode === 'tag' && r.binding.tag === tg.tag;
    });
    if (bound) return;
    propose({
      key: 'lookup|' + tg.tag,
      check: 'domain.lookup', category: 'setups',
      title: 'Columns tagged ' + tg.tag + ' have no lookup rule',
      detail: 'Bind one domain.lookup rule to the tag and every tagged column is covered.',
      draft: { name: 'Lookup — ' + tg.tag.replace(/^lookup:/, ''), category: 'setups', check: 'domain.lookup',
        params: { reference: { kind: 'table', ref: tg.referenceRef || '' }, normalise: 'trim_upper', nulls: 'ignore' },
        binding: { mode: 'tag', tag: tg.tag, databases: [] }, severity: 'critical' },
    });
  });

  out.forEach(function (s) { store.suggestions.push(s); });
  return out;
}

function asDismissSuggestion(store, sugId, user, now) {
  var s = store.suggestions.filter(function (x) { return x.id === sugId; })[0];
  if (!s) return null;
  s.state = 'dismissed';
  store.dismissals[s.key] = { by: user || null, at: now || 0 };  /* remembered forever */
  asEvent(store, { type: 'suggestion.dismissed', key: s.key, by: user }, now);
  return s;
}

/* =======================================================================
   Health, streaks, MTTF
   ======================================================================= */
function asHealth(store, windowMs, now) {
  now = now || 0;
  var from = now - (windowMs || 30 * 86400000);
  var runs = store.runs.filter(function (r) { return r.startedAt >= from && r.status !== 'error' && r.status !== 'skipped'; });
  var byCat = {};
  var pass = 0, warn = 0, fail = 0;
  runs.forEach(function (r) {
    var cat = r.category || 'custom';
    var c = byCat[cat] || (byCat[cat] = { pass: 0, warn: 0, fail: 0 });
    if (r.status === 'pass') { pass++; c.pass++; }
    else if (r.status === 'warn') { warn++; c.warn++; }
    else { fail++; c.fail++; }
  });
  var total = pass + warn + fail;
  var score = function (p, w, f) {
    var t = p + w + f;
    return t ? Math.round(1000 * (p + 0.5 * w) / t) / 10 : null;
  };
  var perCategory = {};
  Object.keys(byCat).forEach(function (k) {
    perCategory[k] = score(byCat[k].pass, byCat[k].warn, byCat[k].fail);
  });

  /* clean streak: days since the last fail, capped at the window */
  var lastFail = 0;
  store.runs.forEach(function (r) { if (r.status === 'fail') lastFail = Math.max(lastFail, r.startedAt); });
  var streakDays = lastFail ? Math.floor((now - lastFail) / 86400000) : null;

  /* mean time to fix over resolved breaches in the window */
  var fixed = store.breaches.filter(function (b) {
    return b.state === 'resolved' && b.resolvedAt && b.resolvedAt >= from;
  });
  var mttfMs = fixed.length
    ? fixed.reduce(function (a, b) { return a + (b.resolvedAt - b.openedAt); }, 0) / fixed.length
    : null;

  return {
    score: score(pass, warn, fail),
    runs: total, pass: pass, warn: warn, fail: fail,
    perCategory: perCategory,
    openBreaches: store.breaches.filter(function (b) { return b.state === 'open' || b.state === 'acknowledged'; }).length,
    criticalOpen: store.breaches.filter(function (b) {
      return (b.state === 'open' || b.state === 'acknowledged') && b.severity === 'critical';
    }).length,
    streakDays: streakDays,
    meanTimeToFixMs: mttfMs,
    activeRules: store.rules.filter(function (r) { return r.enabled; }).length,
  };
}

/* Daily health series for the sparkline. */
function asHealthSeries(store, days, now) {
  var out = [];
  for (var i = days - 1; i >= 0; i--) {
    var dayEnd = now - i * 86400000;
    var dayStart = dayEnd - 86400000;
    var p = 0, w = 0, f = 0;
    store.runs.forEach(function (r) {
      if (r.startedAt < dayStart || r.startedAt >= dayEnd) return;
      if (r.status === 'pass') p++; else if (r.status === 'warn') w++; else if (r.status === 'fail') f++;
    });
    var t = p + w + f;
    out.push(t ? Math.round(1000 * (p + 0.5 * w) / t) / 10 : null);
  }
  return out;
}

/* =======================================================================
   Monthly report — aggregates plus a deterministic narrative (A7 fallback)
   ======================================================================= */
function asMonthlyReport(store, fromMs, toMs) {
  var runs = store.runs.filter(function (r) { return r.startedAt >= fromMs && r.startedAt < toMs; });
  var caught = store.breaches.filter(function (b) { return b.openedAt >= fromMs && b.openedAt < toMs; });
  var resolved = caught.filter(function (b) { return b.state === 'resolved'; });
  var p = 0, w = 0, f = 0;
  runs.forEach(function (r) {
    if (r.status === 'pass') p++; else if (r.status === 'warn') w++; else if (r.status === 'fail') f++;
  });
  var t = p + w + f;
  var mttf = resolved.length
    ? resolved.reduce(function (a, b) { return a + ((b.resolvedAt || toMs) - b.openedAt); }, 0) / resolved.length
    : null;
  var added = store.rules.filter(function (r) {
    return r.provenance && r.provenance.createdAt >= fromMs && r.provenance.createdAt < toMs;
  });
  var narrative = caught.slice(0, 6).map(function (b) {
    var rule = store.rules.filter(function (r) { return r.id === b.ruleId; })[0] || { name: b.ruleId };
    return (b.peakRowsFailed ? b.peakRowsFailed.toLocaleString('en-GB') + ' rows — ' : '')
      + rule.name + ' (' + b.ruleId + ')'
      + (b.state === 'resolved' ? ', resolved in ' + fmtDuration(b.resolvedAt - b.openedAt) : ', still open');
  });
  return {
    from: fromMs, to: toMs,
    meanHealth: t ? Math.round(1000 * (p + 0.5 * w) / t) / 10 : null,
    checksRun: t, breachesCaught: caught.length, breachesResolved: resolved.length,
    meanTimeToFixMs: mttf,
    rulesAdded: added.length,
    rulesTotal: store.rules.filter(function (r) { return r.enabled; }).length,
    narrative: narrative,
  };
}

function fmtDuration(ms) {
  if (ms == null) return '—';
  var h = ms / 3600000;
  if (h < 1) return Math.round(ms / 60000) + 'm';
  if (h < 48) return (Math.round(h * 10) / 10) + 'h';
  return Math.round(h / 24) + 'd';
}

/* =======================================================================
   Cron — enough of the five fields for the shipped schedules
   ======================================================================= */
function cronField(spec, min, max) {
  if (spec === '*') return null;                       /* any */
  var out = {};
  String(spec).split(',').forEach(function (part) {
    var m = part.match(/^\*\/(\d+)$/);
    if (m) { for (var v = min; v <= max; v += Number(m[1])) out[v] = 1; return; }
    var r = part.match(/^(\d+)-(\d+)$/);
    if (r) { for (var v2 = Number(r[1]); v2 <= Number(r[2]); v2++) out[v2] = 1; return; }
    out[Number(part)] = 1;
  });
  return out;
}

function asCronNext(expr, fromMs) {
  var f = String(expr || '').trim().split(/\s+/);
  if (f.length !== 5) return null;
  var minute = cronField(f[0], 0, 59), hour = cronField(f[1], 0, 23);
  var dom = cronField(f[2], 1, 31), mon = cronField(f[3], 1, 12), dow = cronField(f[4], 0, 6);
  var t = new Date(fromMs);
  t.setUTCSeconds(0, 0);
  t.setUTCMinutes(t.getUTCMinutes() + 1);
  for (var i = 0; i < 366 * 24 * 60; i++) {
    var ok = (!minute || minute[t.getUTCMinutes()])
          && (!hour || hour[t.getUTCHours()])
          && (!dom || dom[t.getUTCDate()])
          && (!mon || mon[t.getUTCMonth() + 1])
          && (!dow || dow[t.getUTCDay()]);
    if (ok) return t.getTime();
    t.setUTCMinutes(t.getUTCMinutes() + 1);
  }
  return null;
}

/* Is a group due, given when it last swept? */
function asCronDue(expr, lastRunMs, nowMs) {
  var next = asCronNext(expr, lastRunMs || 0);
  return next != null && next <= nowMs;
}

/* =======================================================================
   Public
   ======================================================================= */
return {
  STORE_VERSION: STORE_VERSION,
  CATEGORIES: CATEGORIES,
  SEVERITIES: SEVERITIES,
  TAG_FAMILIES: TAG_FAMILIES,
  SAMPLE_CAP_DEFAULT: SAMPLE_CAP_DEFAULT,

  asDialectOf: asDialectOf,
  asChecks: asChecks,
  asCompile: asCompile,

  asProposeTags: asProposeTags,
  tagKey: tagKey,

  asResolveBinding: asResolveBinding,
  asEvaluate: asEvaluate,
  asEvaluateMeasure: asEvaluateMeasure,
  asEvaluatePair: asEvaluatePair,

  asNewStore: asNewStore,
  asDefaultGroups: asDefaultGroups,
  asEvent: asEvent,
  asNewRuleId: asNewRuleId,
  asSaveRule: asSaveRule,
  asDisableRule: asDisableRule,

  asRecordRun: asRecordRun,
  asAcknowledge: asAcknowledge,
  asResolve: asResolve,
  asSuppress: asSuppress,

  asDiagnoseProse: asDiagnoseProse,
  asSuggest: asSuggest,
  asDismissSuggestion: asDismissSuggestion,

  asHealth: asHealth,
  asHealthSeries: asHealthSeries,
  asMonthlyReport: asMonthlyReport,
  fmtDuration: fmtDuration,

  asCronNext: asCronNext,
  asCronDue: asCronDue,
};
});
