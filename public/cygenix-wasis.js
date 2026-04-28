/**
 * cygenix-wasis.js  v1.0
 *
 * Shared Was/Is rules helper. Both dashboard.html and object_mapping.html
 * call this module so they can never drift on storage key, field shape, or
 * CASE WHEN generation. Cosmos sync is unaffected — cygenix-cosmos-sync.js
 * still mirrors cygenix_wasis_rules to/from Cosmos, and this module reads
 * from the hydrated localStorage the sync layer maintains.
 *
 * Storage of record: localStorage['cygenix_wasis_rules']  (mirrored to Cosmos)
 * Fallback sources if the primary key is empty:
 *   - any inventory doc with type === 'wasis' (legacy CSV/TSV upload path)
 *
 * Public API:
 *   CygenixWasis.getRules()                                → normalised rule[]
 *   CygenixWasis.saveRules(rules)                          → persist
 *   CygenixWasis.rulesFor(srcTable, srcField)              → filtered rule[]
 *   CygenixWasis.wrapExpr(srcExpr, srcTable, srcField, fmt)→ CASE WHEN SQL or original
 *   CygenixWasis.subscribe(fn)                             → fn called on change
 */
(function (global) {
  'use strict';

  var WASIS_KEY = 'cygenix_wasis_rules';
  var INV_KEY   = 'cygenix_inventory';

  var subscribers = [];

  // Generate a stable rule id. Same shape as the existing sconn_ ids and
  // job_ ids elsewhere in the app — collision-resistant enough for per-user
  // data, no UUID dependency. Exposed via the public API so dashboard.html
  // (the only place that creates rules) can use it directly.
  function genId() {
    return 'wir_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7);
  }

  function safeParse(raw, fallback) {
    try {
      var v = JSON.parse(raw || 'null');
      return v == null ? fallback : v;
    } catch (e) { return fallback; }
  }

  function normaliseRule(r) {
    if (!r || typeof r !== 'object') return null;
    // Preserve an existing id; generate one if missing. This is the migration
    // path — the moment any old rule is read, it picks up an id that survives
    // the next save. After init() pulls cloud data into localStorage and
    // wiLoad() runs through this normaliser, every rule has one.
    return {
      id:       (r.id != null && String(r.id).trim()) ? String(r.id) : genId(),
      srcTable: (r.srcTable != null ? String(r.srcTable) : '').toLowerCase().trim(),
      srcField: (r.srcField != null ? String(r.srcField) : '').toLowerCase().trim(),
      oldVal:   r.oldVal != null ? String(r.oldVal) : '',
      newVal:   r.newVal != null ? String(r.newVal) : '',
      desc:     r.desc   != null ? String(r.desc)   : ''
    };
  }

  // Parse a CSV/TSV wasis inventory doc (legacy path from dashboard.html
  // getWasisRules) into rule objects with the same shape as the primary key.
  function parseInventoryDoc(doc) {
    if (!doc || !doc.isText || !doc.content) return [];
    var delim = doc.ext === 'tsv' ? '\t' : ',';
    var lines = String(doc.content).split(/\r?\n/).filter(function (l) { return l.trim(); });
    if (!lines.length) return [];

    // Detect header row. We accept the columns produced by wiExportCSV:
    //   table,field,oldVal,newVal,description
    // …and common aliases.
    var header = lines[0].toLowerCase().split(delim).map(function (s) { return s.trim().replace(/^"|"$/g, ''); });
    var hasHeader = header.some(function (h) {
      return h === 'table' || h === 'srctable' || h === 'field' || h === 'srcfield' ||
             h === 'oldval' || h === 'old' || h === 'newval' || h === 'new';
    });
    var idx = {};
    if (hasHeader) {
      header.forEach(function (h, i) {
        if (h === 'table' || h === 'srctable')  idx.srcTable = i;
        else if (h === 'field' || h === 'srcfield')  idx.srcField = i;
        else if (h === 'oldval' || h === 'old' || h === 'was')  idx.oldVal = i;
        else if (h === 'newval' || h === 'new' || h === 'is')   idx.newVal = i;
        else if (h === 'description' || h === 'desc' || h === 'note') idx.desc = i;
      });
    } else {
      idx = { srcTable: 0, srcField: 1, oldVal: 2, newVal: 3, desc: 4 };
    }

    var dataLines = hasHeader ? lines.slice(1) : lines;
    return dataLines.map(function (line) {
      // Minimal quoted-CSV split: handles "a,b","c" style values
      var cells = [];
      var cur = '', inQ = false;
      for (var i = 0; i < line.length; i++) {
        var ch = line[i];
        if (inQ) {
          if (ch === '"' && line[i + 1] === '"') { cur += '"'; i++; }
          else if (ch === '"') inQ = false;
          else cur += ch;
        } else {
          if (ch === '"') inQ = true;
          else if (ch === delim) { cells.push(cur); cur = ''; }
          else cur += ch;
        }
      }
      cells.push(cur);
      return normaliseRule({
        srcTable: cells[idx.srcTable],
        srcField: cells[idx.srcField],
        oldVal:   cells[idx.oldVal],
        newVal:   cells[idx.newVal],
        desc:     idx.desc != null ? cells[idx.desc] : ''
      });
    }).filter(function (r) { return r && (r.oldVal || r.newVal); });
  }

  function getInventoryFallback() {
    var inv = safeParse(localStorage.getItem(INV_KEY), []);
    if (!Array.isArray(inv)) return [];
    var out = [];
    inv.forEach(function (doc) {
      if (doc && doc.type === 'wasis') {
        out = out.concat(parseInventoryDoc(doc));
      }
    });
    return out;
  }

  function getRules() {
    var primary = safeParse(localStorage.getItem(WASIS_KEY), []);
    if (!Array.isArray(primary)) primary = [];

    // Migration: are any stored rules missing an id? If so, we'll persist
    // the normalised (now-id'd) array back so subsequent reads are stable
    // and the sync intercept propagates the new ids to Cosmos. Detect with
    // a cheap pre-check rather than always writing — this function is on
    // the hot path (called from object_mapping.html on every render) so
    // the steady-state cost matters.
    var needsMigration = primary.some(function (r) {
      return r && typeof r === 'object' && (r.id == null || String(r.id).trim() === '');
    });

    var rules = primary.map(normaliseRule).filter(Boolean);

    if (needsMigration && rules.length) {
      try {
        localStorage.setItem(WASIS_KEY, JSON.stringify(rules));
      } catch (e) { /* quota / privacy mode — non-fatal, ids re-assigned next read */ }
    }

    // Only fall back to inventory when primary is genuinely empty.
    // This preserves dashboard edits as authoritative.
    if (!rules.length) rules = getInventoryFallback();
    return rules;
  }

  function saveRules(rules) {
    var arr = (rules || []).map(normaliseRule).filter(Boolean);
    try { localStorage.setItem(WASIS_KEY, JSON.stringify(arr)); } catch (e) {}
    notify();
    return arr;
  }

  // Filter rules that apply to a given source (table, field).
  // Empty srcTable / srcField on a rule matches any.
  function rulesFor(srcTable, srcField) {
    var t = String(srcTable || '').toLowerCase();
    var f = String(srcField || '').toLowerCase();
    return getRules().filter(function (r) {
      return (r.srcTable === '' || r.srcTable === t) &&
             (r.srcField === '' || r.srcField === f);
    });
  }

  // Default SQL literal formatter — quotes strings, passes numbers through.
  // Callers (object_mapping.html) can pass their own formatSQLVal for symmetry.
  function defaultFormatSQLVal(v) {
    if (v == null || v === '') return 'NULL';
    if (/^-?\d+(\.\d+)?$/.test(String(v).trim())) return String(v).trim();
    return "'" + String(v).replace(/'/g, "''") + "'";
  }

  // Wrap a source expression in a CASE WHEN ... END block for the rules
  // that apply to (srcTable, srcField). Returns the original expression if
  // no rules apply, so it's safe to call unconditionally.
  //
  //   srcExpr  — e.g. "[CurrencyCode]" or "@CurrencyCode" (OTM variable)
  //   srcTable — source table name (lowercase match)
  //   srcField — source column name (lowercase match)
  //   opts     — { formatSQLVal, indent, onApplied(count) }
  function wrapExpr(srcExpr, srcTable, srcField, opts) {
    opts = opts || {};
    var fmt = opts.formatSQLVal || defaultFormatSQLVal;
    var indent = opts.indent != null ? opts.indent : '        ';
    var applicable = rulesFor(srcTable, srcField);
    if (!applicable.length) return srcExpr;

    var whens = applicable.map(function (r) {
      return 'WHEN ' + srcExpr + ' = ' + fmt(r.oldVal) + ' THEN ' + fmt(r.newVal);
    }).join('\n' + indent);

    if (typeof opts.onApplied === 'function') opts.onApplied(applicable.length);
    return 'CASE\n' + indent + whens + '\n' + indent + 'ELSE ' + srcExpr + '\n' + indent.slice(0, -2) + 'END';
  }

  // Notify subscribers of any rule change (UI refresh, counters, etc.)
  function notify() {
    subscribers.forEach(function (fn) {
      try { fn(); } catch (e) { /* keep others alive */ }
    });
  }
  function subscribe(fn) {
    if (typeof fn === 'function') subscribers.push(fn);
    return function unsub() {
      subscribers = subscribers.filter(function (s) { return s !== fn; });
    };
  }

  // React to cross-tab writes to cygenix_wasis_rules (dashboard edits in
  // another tab, Cosmos sync restoring on load, etc.)
  global.addEventListener && global.addEventListener('storage', function (e) {
    if (e.key === WASIS_KEY || e.key === INV_KEY) notify();
  });

  global.CygenixWasis = {
    getRules:   getRules,
    saveRules:  saveRules,
    rulesFor:   rulesFor,
    wrapExpr:   wrapExpr,
    subscribe:  subscribe,
    genId:      genId,
    // Exposed for testing / debugging only.
    _normalise: normaliseRule,
    _key:       WASIS_KEY
  };
})(typeof window !== 'undefined' ? window : this);
