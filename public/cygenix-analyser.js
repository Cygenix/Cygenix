/* ============================================================================
   cygenix-analyser.js — the Data Analyser's engine.
   ----------------------------------------------------------------------------
   WHAT THIS IS

   A drop-in data format recogniser and profiler. Given the bytes of a file it
   says what the file is and how sure it is, parses it to one canonical shape,
   profiles every column, flags what will break on load, and emits a target
   schema. Nothing here opens a socket: no upload, no server, no key. That is
   the claim the Data Analyser page makes to the user, and tests/
   data-analyser.test.js pins it against this file rather than trusting it.

   WHY IT IS NOT CALLED cygenix-transform.js

   It arrived under that name, and this repository already has a
   public/cygenix-transform.js — a different module that defines what a column
   TRANSFORM does to a value inside the migration runner. Both files assigned
   a global called CygenixTransform. The existing one guards its assignment
   with `!root.CygenixTransform`; this one overwrote unconditionally. Which
   module a page got would have depended on script order, and the Pipelines
   screen, which loads the runner's module, would have been the first casualty.

   So the file and its global were renamed on the way in: cygenix-analyser.js,
   exposing CygenixAnalyser. Nothing else about the engine changed, and the
   vendor's own header follows so the provenance is visible. Update it by
   diffing against upstream and re-applying the two renames — there is one
   assignment and one comment line, and the engine test will fail loudly if
   the global comes back under the old name.
   ========================================================================== */
/*!
 * cygenix-analyser.js — universal data format recogniser & transformer
 * Zero dependencies. UMD. Runs entirely in the browser; no server, no API key.
 *
 * Optional progressive enhancements, used automatically if already on the page:
 *   window.XLSX  (SheetJS)  -> .xlsx / .xls workbook support
 *   window.jsyaml (js-yaml) -> full YAML support (a small built-in subset is used otherwise)
 *
 * Everything parses down to one canonical shape:
 *   Dataset = { columns: string[], rows: any[][], format: string, meta: {...}, warnings: [...] }
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else if (typeof define === 'function' && define.amd) define([], factory);
  else root.CygenixAnalyser = factory();
}(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  var VERSION = '1.0.0';

  /* ------------------------------------------------------------------ *
   * Small helpers
   * ------------------------------------------------------------------ */

  var NULL_TOKENS = ['', 'null', 'NULL', 'Null', 'nil', 'N/A', 'n/a', 'NA', '#N/A', '-', '--', 'none', 'None', 'NaN'];

  function stripBOM(s) { return s.charCodeAt(0) === 0xFEFF ? s.slice(1) : s; }
  function isBlank(v) { return v === null || v === undefined || (typeof v === 'string' && v.trim() === ''); }
  function isNullToken(v, tokens) {
    if (v === null || v === undefined) return true;
    if (typeof v !== 'string') return false;
    var t = v.trim();
    for (var i = 0; i < tokens.length; i++) if (t === tokens[i]) return true;
    return false;
  }
  function uniq(a) { var s = {}, o = []; for (var i = 0; i < a.length; i++) if (!s[a[i]]) { s[a[i]] = 1; o.push(a[i]); } return o; }
  function clamp(n, lo, hi) { return n < lo ? lo : n > hi ? hi : n; }
  function lines(s, limit) {
    var out = s.split(/\r\n|\n|\r/);
    if (limit && out.length > limit) out = out.slice(0, limit);
    return out;
  }
  function nonEmptyLines(s, limit) {
    return lines(s, limit ? limit * 4 : 0).filter(function (l) { return l.trim() !== ''; }).slice(0, limit || Infinity);
  }
  // Split on newlines that are NOT inside a quoted field, so a CSV with embedded
  // newlines still sniffs as one record per logical line.
  function logicalLines(s, quote, limit) {
    quote = quote || '"';
    var out = [], cur = '', inQ = false;
    for (var i = 0; i < s.length; i++) {
      var ch = s[i];
      if (ch === quote) { if (inQ && s[i + 1] === quote) { cur += quote + quote; i++; continue; } inQ = !inQ; cur += ch; continue; }
      if (!inQ && (ch === '\n' || ch === '\r')) {
        if (ch === '\r' && s[i + 1] === '\n') i++;
        if (cur.trim() !== '') out.push(cur);
        cur = '';
        if (limit && out.length >= limit) return out;
        continue;
      }
      cur += ch;
    }
    if (cur.trim() !== '') out.push(cur);
    return out;
  }
  function median(nums) {
    if (!nums.length) return 0;
    var a = nums.slice().sort(function (x, y) { return x - y; });
    var m = Math.floor(a.length / 2);
    return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2;
  }
  function hasDOM() { return typeof DOMParser !== 'undefined'; }

  function makeUniqueNames(names) {
    var seen = {}, out = [];
    for (var i = 0; i < names.length; i++) {
      var base = String(names[i] === null || names[i] === undefined || names[i] === '' ? 'column_' + (i + 1) : names[i]).trim();
      if (base === '') base = 'column_' + (i + 1);
      var name = base, n = 2;
      while (Object.prototype.hasOwnProperty.call(seen, name.toLowerCase())) { name = base + '_' + n; n++; }
      seen[name.toLowerCase()] = 1;
      out.push(name);
    }
    return out;
  }

  function dataset(format, columns, rows, meta, warnings) {
    return {
      format: format,
      columns: makeUniqueNames(columns || []),
      rows: rows || [],
      meta: meta || {},
      warnings: warnings || [],
      get rowCount() { return this.rows.length; },
      toObjects: function () { return rowsToObjects(this); }
    };
  }

  function rowsToObjects(ds) {
    var out = [];
    for (var r = 0; r < ds.rows.length; r++) {
      var o = {};
      for (var c = 0; c < ds.columns.length; c++) o[ds.columns[c]] = ds.rows[r][c] === undefined ? null : ds.rows[r][c];
      out.push(o);
    }
    return out;
  }

  /* ------------------------------------------------------------------ *
   * Flattening of nested records (JSON / XML / YAML)
   * ------------------------------------------------------------------ */

  function flattenRecord(obj, prefix, out, depth, opts) {
    out = out || {};
    prefix = prefix || '';
    depth = depth || 0;
    var maxDepth = (opts && opts.maxDepth) || 6;
    for (var k in obj) {
      if (!Object.prototype.hasOwnProperty.call(obj, k)) continue;
      var v = obj[k], key = prefix ? prefix + '.' + k : k;
      if (v === null || v === undefined) { out[key] = null; }
      else if (Array.isArray(v)) {
        var scalarOnly = v.every(function (x) { return x === null || typeof x !== 'object'; });
        if (scalarOnly) out[key] = v.join('; ');
        else if (depth < maxDepth) {
          for (var i = 0; i < v.length; i++) {
            if (v[i] && typeof v[i] === 'object') flattenRecord(v[i], key + '[' + i + ']', out, depth + 1, opts);
            else out[key + '[' + i + ']'] = v[i];
          }
        } else out[key] = JSON.stringify(v);
      }
      else if (typeof v === 'object') {
        if (depth < maxDepth) flattenRecord(v, key, out, depth + 1, opts);
        else out[key] = JSON.stringify(v);
      }
      else out[key] = v;
    }
    return out;
  }

  function recordsToDataset(records, format, meta, warnings, opts) {
    var flat = [], colOrder = [], seen = {};
    for (var i = 0; i < records.length; i++) {
      var rec = records[i];
      if (rec === null || typeof rec !== 'object' || Array.isArray(rec)) rec = { value: rec };
      var f = (opts && opts.flatten === false) ? rec : flattenRecord(rec, '', {}, 0, opts);
      flat.push(f);
      for (var k in f) if (Object.prototype.hasOwnProperty.call(f, k) && !seen[k]) { seen[k] = 1; colOrder.push(k); }
    }
    var rows = flat.map(function (f) {
      return colOrder.map(function (c) { return Object.prototype.hasOwnProperty.call(f, c) ? f[c] : null; });
    });
    return dataset(format, colOrder, rows, meta, warnings);
  }

  /* ------------------------------------------------------------------ *
   * Delimited text (CSV / TSV / PSV / semicolon) — RFC 4180 state machine
   * ------------------------------------------------------------------ */

  function parseDelimited(text, delimiter, quote) {
    quote = quote || '"';
    var rows = [], row = [], field = '', inQuotes = false, i = 0, n = text.length;
    while (i < n) {
      var ch = text[i];
      if (inQuotes) {
        if (ch === quote) {
          if (text[i + 1] === quote) { field += quote; i += 2; continue; }
          inQuotes = false; i++; continue;
        }
        field += ch; i++; continue;
      }
      if (ch === quote && field === '') { inQuotes = true; i++; continue; }
      if (ch === delimiter) { row.push(field); field = ''; i++; continue; }
      if (ch === '\r') { if (text[i + 1] === '\n') i++; row.push(field); rows.push(row); row = []; field = ''; i++; continue; }
      if (ch === '\n') { row.push(field); rows.push(row); row = []; field = ''; i++; continue; }
      field += ch; i++;
    }
    if (field !== '' || row.length) { row.push(field); rows.push(row); }
    return rows;
  }

  var DELIMITER_CANDIDATES = [
    { ch: ',', name: 'csv' }, { ch: '\t', name: 'tsv' }, { ch: ';', name: 'csv-semicolon' },
    { ch: '|', name: 'psv' }, { ch: '', name: 'soh-delimited' }, { ch: '~', name: 'tilde-delimited' }
  ];

  // Count occurrences outside quoted regions, per line.
  function countOutsideQuotes(line, ch, quote) {
    var c = 0, inQ = false;
    for (var i = 0; i < line.length; i++) {
      var x = line[i];
      if (x === quote) { if (inQ && line[i + 1] === quote) { i++; continue; } inQ = !inQ; continue; }
      if (!inQ && x === ch) c++;
    }
    return c;
  }

  function sniffDelimiter(text) {
    var sample = logicalLines(text, '"', 60);
    if (!sample.length) return null;
    var best = null;
    for (var d = 0; d < DELIMITER_CANDIDATES.length; d++) {
      var cand = DELIMITER_CANDIDATES[d];
      var counts = sample.map(function (l) { return countOutsideQuotes(l, cand.ch, '"'); });
      var med = median(counts);
      if (med < 1) continue;
      var consistent = counts.filter(function (c) { return c === med; }).length / counts.length;
      // score favours consistency first, then field count
      var score = consistent * 100 + Math.min(med, 40);
      if (!best || score > best.score) best = { delimiter: cand.ch, name: cand.name, fields: med + 1, consistency: consistent, score: score };
    }
    return best;
  }

  function looksLikeHeader(rows) {
    if (rows.length < 2) return true;
    var head = rows[0], body = rows.slice(1, 21);
    // header is a header if its cells are all non-numeric and non-empty, and the body has at least one column that is consistently numeric/date
    var headerAllText = head.every(function (h) { return h !== '' && !/^[+-]?\d+(\.\d+)?$/.test(String(h).trim()); });
    if (!headerAllText) return false;
    if (uniq(head.map(function (h) { return String(h).toLowerCase(); })).length !== head.length) {
      // duplicate names are common in real headers but also common in data; keep it as a header anyway
    }
    var bodyHasTyped = false;
    for (var c = 0; c < head.length; c++) {
      var vals = body.map(function (r) { return r[c]; }).filter(function (v) { return !isBlank(v); });
      if (!vals.length) continue;
      var typed = vals.filter(function (v) { return /^[+-]?\d+(\.\d+)?$/.test(String(v).trim()) || /^\d{4}-\d{2}-\d{2}/.test(String(v).trim()); }).length;
      if (typed / vals.length > 0.8) { bodyHasTyped = true; break; }
    }
    return headerAllText && (bodyHasTyped || true);
  }

  function delimitedToDataset(text, opts) {
    opts = opts || {};
    text = stripBOM(text);
    var sniff = opts.delimiter ? { delimiter: opts.delimiter, name: 'delimited' } : sniffDelimiter(text);
    if (!sniff) return null;
    var grid = parseDelimited(text, sniff.delimiter, opts.quote || '"');
    grid = grid.filter(function (r) { return !(r.length === 1 && String(r[0]).trim() === ''); });
    if (!grid.length) return null;
    var warnings = [];
    // A single line can never be proven to be a header — treat it as data.
    var hasHeader = opts.header !== undefined ? opts.header : (grid.length > 1 && looksLikeHeader(grid));
    var columns = hasHeader ? grid[0].map(function (h) { return String(h).trim(); })
                            : grid[0].map(function (_, i) { return 'column_' + (i + 1); });
    var body = hasHeader ? grid.slice(1) : grid;
    var width = columns.length, ragged = 0;
    var rows = body.map(function (r) {
      if (r.length !== width) ragged++;
      var out = r.slice(0, width);
      while (out.length < width) out.push(null);
      return out;
    });
    if (ragged) warnings.push({ code: 'RAGGED_ROWS', message: ragged + ' row(s) did not have ' + width + ' fields; padded or truncated to match the header.', count: ragged });
    if (!hasHeader) warnings.push({ code: 'NO_HEADER', message: 'No header row detected — columns were named positionally.' });
    return dataset(sniff.name, columns, rows, { delimiter: sniff.delimiter, hasHeader: hasHeader, consistency: sniff.consistency }, warnings);
  }

  /* ------------------------------------------------------------------ *
   * Single-column text (a list, one value per line)
   * ------------------------------------------------------------------ */

  function isTypedToken(s) {
    s = String(s).trim();
    var p = PATTERNS;
    return p.integer.test(s) || p.decimal.test(s) || p.float.test(s) || p.isoDateTime.test(s) ||
           p.isoDate.test(s) || p.slashDate.test(s) || p.time.test(s) || p.uuid.test(s) ||
           p.email.test(s) || p.url.test(s) || p.boolWord.test(s) || p.money.test(s) || p.thousands.test(s);
  }

  // A one-column file has a header only if the first line is a plausible name and
  // the values below it are consistently typed. Prose must not become a header.
  function singleColumnHeader(ls, nullTokens) {
    if (ls.length < 3) return false;
    var head = String(ls[0]).trim();
    if (head === '' || head.length > 64) return false;
    if (!/^[A-Za-z_][\w .()\/-]*$/.test(head)) return false;
    if (isTypedToken(head)) return false;
    var body = ls.slice(1).filter(function (l) { return !isNullToken(l, nullTokens || NULL_TOKENS); });
    if (!body.length) return false;
    var hits = body.filter(isTypedToken).length;
    return hits / body.length >= 0.8;
  }

  function singleColumnToDataset(text, opts) {
    opts = opts || {};
    var ls = lines(stripBOM(text));
    while (ls.length && ls[ls.length - 1].trim() === '') ls.pop();
    if (!ls.length) return null;
    var hasHeader = opts.header !== undefined ? opts.header : singleColumnHeader(ls, opts.nullTokens);
    var name = hasHeader ? String(ls[0]).trim() : 'line';
    var body = hasHeader ? ls.slice(1) : ls;
    return dataset(hasHeader ? 'single-column' : 'text', [name],
      body.map(function (l) { return [l]; }),
      { hasHeader: hasHeader },
      hasHeader ? [] : [{ code: 'UNSTRUCTURED', message: 'No delimiter or structure was recognised — each line became one row.' }]);
  }

  /* ------------------------------------------------------------------ *
   * JSON / NDJSON
   * ------------------------------------------------------------------ */

  function jsonToDataset(text, opts) {
    var data = JSON.parse(stripBOM(text));
    return jsonValueToDataset(data, 'json', opts);
  }

  function jsonValueToDataset(data, format, opts) {
    var warnings = [];
    if (Array.isArray(data)) return recordsToDataset(data, format, { root: 'array' }, warnings, opts);
    if (data && typeof data === 'object') {
      // Find the most plausible record array inside a wrapper object ({data:[...]}, {results:[...]}, ...)
      var bestKey = null, bestLen = -1;
      for (var k in data) {
        if (Array.isArray(data[k]) && data[k].length > bestLen) { bestLen = data[k].length; bestKey = k; }
      }
      if (bestKey !== null && bestLen > 0) {
        warnings.push({ code: 'UNWRAPPED', message: 'Records were read from the "' + bestKey + '" property of the root object.' });
        return recordsToDataset(data[bestKey], format, { root: bestKey }, warnings, opts);
      }
      return recordsToDataset([data], format, { root: 'object' }, warnings, opts);
    }
    return dataset(format, ['value'], [[data]], { root: 'scalar' }, warnings);
  }

  function ndjsonToDataset(text, opts) {
    var recs = [], bad = 0;
    nonEmptyLines(stripBOM(text)).forEach(function (l) {
      try { recs.push(JSON.parse(l)); } catch (e) { bad++; }
    });
    var w = bad ? [{ code: 'BAD_LINES', message: bad + ' line(s) were not valid JSON and were skipped.', count: bad }] : [];
    return recordsToDataset(recs, 'ndjson', {}, w, opts);
  }

  /* ------------------------------------------------------------------ *
   * XML / HTML tables
   * ------------------------------------------------------------------ */

  function xmlNodeToObject(node) {
    var obj = {}, hasChildElements = false;
    if (node.attributes) {
      for (var a = 0; a < node.attributes.length; a++) {
        obj['@' + node.attributes[a].name] = node.attributes[a].value;
      }
    }
    for (var i = 0; i < node.childNodes.length; i++) {
      var ch = node.childNodes[i];
      if (ch.nodeType !== 1) continue;
      hasChildElements = true;
      var val = xmlNodeToObject(ch);
      var name = ch.nodeName;
      if (Object.prototype.hasOwnProperty.call(obj, name)) {
        if (!Array.isArray(obj[name])) obj[name] = [obj[name]];
        obj[name].push(val);
      } else obj[name] = val;
    }
    if (!hasChildElements) {
      var text = (node.textContent || '').trim();
      if (Object.keys(obj).length === 0) return text === '' ? null : text;
      if (text !== '') obj['#text'] = text;
    }
    return obj;
  }

  function xmlToDataset(text, opts) {
    if (!hasDOM()) throw new Error('XML parsing needs a DOM (browser) environment.');
    var doc = new DOMParser().parseFromString(text, 'application/xml');
    var err = doc.getElementsByTagName('parsererror');
    if (err && err.length) throw new Error('Malformed XML: ' + (err[0].textContent || '').slice(0, 200));
    var root = doc.documentElement;
    // Choose the repeating child element name that appears most often — that is the record.
    var counts = {}, order = [];
    for (var i = 0; i < root.childNodes.length; i++) {
      var c = root.childNodes[i];
      if (c.nodeType !== 1) continue;
      if (!counts[c.nodeName]) { counts[c.nodeName] = 0; order.push(c.nodeName); }
      counts[c.nodeName]++;
    }
    var recordName = null, max = 0;
    order.forEach(function (n) { if (counts[n] > max) { max = counts[n]; recordName = n; } });
    var records = [];
    if (recordName) {
      for (var j = 0; j < root.childNodes.length; j++) {
        var ch = root.childNodes[j];
        if (ch.nodeType === 1 && ch.nodeName === recordName) {
          var v = xmlNodeToObject(ch);
          records.push(v && typeof v === 'object' ? v : { value: v });
        }
      }
    } else {
      var rv = xmlNodeToObject(root);
      records.push(rv && typeof rv === 'object' ? rv : { value: rv });
    }
    return recordsToDataset(records, 'xml', { root: root.nodeName, record: recordName }, [], opts);
  }

  function htmlTableToDataset(text, opts) {
    if (!hasDOM()) throw new Error('HTML table parsing needs a DOM (browser) environment.');
    var doc = new DOMParser().parseFromString(text, 'text/html');
    var tables = doc.getElementsByTagName('table');
    if (!tables.length) throw new Error('No <table> element found.');
    var idx = (opts && opts.tableIndex) || 0;
    var table = tables[Math.min(idx, tables.length - 1)];
    var trs = table.getElementsByTagName('tr'), grid = [];
    for (var i = 0; i < trs.length; i++) {
      var cells = trs[i].querySelectorAll('th,td'), row = [];
      for (var j = 0; j < cells.length; j++) row.push((cells[j].textContent || '').trim());
      if (row.length) grid.push(row);
    }
    if (!grid.length) throw new Error('The table had no rows.');
    var headerRow = table.getElementsByTagName('th').length > 0;
    var columns = headerRow ? grid[0] : grid[0].map(function (_, i2) { return 'column_' + (i2 + 1); });
    var body = headerRow ? grid.slice(1) : grid;
    var width = columns.length;
    var rows = body.map(function (r) { var o = r.slice(0, width); while (o.length < width) o.push(null); return o; });
    return dataset('html-table', columns, rows, { tables: tables.length, tableIndex: idx }, []);
  }

  /* ------------------------------------------------------------------ *
   * YAML (js-yaml if present, otherwise a pragmatic subset)
   * ------------------------------------------------------------------ */

  function parseYamlSubset(text) {
    // Handles: list of maps, map of scalars, nested maps. Not anchors/multiline blocks.
    var src = lines(stripBOM(text)).filter(function (l) { return !/^\s*#/.test(l) && l.trim() !== '' && l.trim() !== '---'; });
    var root = null, stack = [];

    function scalar(v) {
      v = v.trim();
      if (v === '') return null;
      if ((v[0] === '"' && v[v.length - 1] === '"') || (v[0] === "'" && v[v.length - 1] === "'")) return v.slice(1, -1);
      if (/^(true|yes|on)$/i.test(v)) return true;
      if (/^(false|no|off)$/i.test(v)) return false;
      if (/^(null|~)$/i.test(v)) return null;
      if (/^[+-]?\d+$/.test(v)) return parseInt(v, 10);
      if (/^[+-]?\d*\.\d+([eE][+-]?\d+)?$/.test(v)) return parseFloat(v);
      return v;
    }

    for (var i = 0; i < src.length; i++) {
      var line = src[i].replace(/\t/g, '  ');
      var indent = line.match(/^ */)[0].length;
      var body = line.trim();
      var isItem = body.indexOf('- ') === 0 || body === '-';
      if (isItem) body = body.replace(/^-\s*/, '');

      while (stack.length && stack[stack.length - 1].indent >= indent + (isItem ? 1 : 0) && stack.length > 1) stack.pop();

      var m = body.match(/^([^:#]+):\s*(.*)$/);
      if (isItem) {
        if (root === null) { root = []; stack = [{ indent: -1, node: root }]; }
        var parent = stack[stack.length - 1].node;
        if (!Array.isArray(parent)) {
          // parent is a map awaiting a list value — should not happen in the subset
          parent = root;
        }
        if (m) {
          var item = {};
          item[m[1].trim()] = m[2] === '' ? {} : scalar(m[2]);
          parent.push(item);
          stack.push({ indent: indent, node: item });
        } else {
          parent.push(scalar(body));
        }
        continue;
      }
      if (m) {
        if (root === null) { root = {}; stack = [{ indent: -1, node: root }]; }
        var target = stack[stack.length - 1].node;
        if (Array.isArray(target)) target = target[target.length - 1];
        var key = m[1].trim(), val = m[2];
        if (val === '' || val === '{}' || val === '[]') {
          var child = val === '[]' ? [] : {};
          target[key] = child;
          stack.push({ indent: indent, node: child });
        } else {
          target[key] = scalar(val);
        }
      }
    }
    return root === null ? {} : root;
  }

  function yamlToDataset(text, opts) {
    var data, subset = false;
    if (typeof jsyaml !== 'undefined' && jsyaml && jsyaml.load) data = jsyaml.load(text);
    else if (typeof window !== 'undefined' && window.jsyaml && window.jsyaml.load) data = window.jsyaml.load(text);
    else { data = parseYamlSubset(text); subset = true; }
    var ds = jsonValueToDataset(data, 'yaml', opts);
    if (subset) ds.warnings.push({ code: 'YAML_SUBSET', message: 'Parsed with the built-in YAML subset. Load js-yaml on the page for anchors, block scalars and multi-document files.' });
    return ds;
  }

  /* ------------------------------------------------------------------ *
   * Fixed-width text
   * ------------------------------------------------------------------ */

  function detectFixedWidthColumns(sample) {
    var width = Math.max.apply(null, sample.map(function (l) { return l.length; }));
    var spaceEverywhere = [];
    for (var c = 0; c < width; c++) {
      var allSpace = true;
      for (var r = 0; r < sample.length; r++) {
        var ch = sample[r][c];
        if (ch !== undefined && ch !== ' ') { allSpace = false; break; }
      }
      spaceEverywhere.push(allSpace);
    }
    // A boundary is the start of a run of >=2 shared spaces followed by non-space
    var cuts = [0], run = 0;
    for (var i = 0; i < width; i++) {
      if (spaceEverywhere[i]) run++;
      else { if (run >= 2) cuts.push(i); run = 0; }
    }
    cuts.push(width);
    return uniq(cuts).sort(function (a, b) { return a - b; });
  }

  function fixedWidthToDataset(text, opts) {
    var all = nonEmptyLines(stripBOM(text));
    if (all.length < 2) return null;
    var sample = all.slice(0, 200);
    var cuts = detectFixedWidthColumns(sample);
    if (cuts.length < 3) return null;
    function slice(line) {
      var out = [];
      for (var i = 0; i < cuts.length - 1; i++) out.push(line.slice(cuts[i], cuts[i + 1]).trim());
      return out;
    }
    var grid = all.map(slice).filter(function (r) { return r.join('').trim() !== ''; });
    var hasHeader = looksLikeHeader(grid);
    var columns = hasHeader ? grid[0] : grid[0].map(function (_, i) { return 'column_' + (i + 1); });
    var rows = (hasHeader ? grid.slice(1) : grid);
    return dataset('fixed-width', columns, rows, { cuts: cuts, hasHeader: hasHeader },
      [{ code: 'FIXED_WIDTH', message: 'Column boundaries were inferred from shared whitespace at positions ' + cuts.join(', ') + '. Check them against the source layout before migrating.' }]);
  }

  /* ------------------------------------------------------------------ *
   * SQL INSERT dumps
   * ------------------------------------------------------------------ */

  function splitSqlTuple(s) {
    var out = [], cur = '', inQ = false, q = "'", depth = 0;
    for (var i = 0; i < s.length; i++) {
      var ch = s[i];
      if (inQ) {
        if (ch === q) { if (s[i + 1] === q) { cur += q; i++; continue; } inQ = false; cur += ch; continue; }
        if (ch === '\\' && (s[i + 1] === q || s[i + 1] === '\\')) { cur += s[i + 1]; i++; continue; }
        cur += ch; continue;
      }
      if (ch === "'") { inQ = true; cur += ch; continue; }
      if (ch === '(') { depth++; cur += ch; continue; }
      if (ch === ')') { depth--; cur += ch; continue; }
      if (ch === ',' && depth === 0) { out.push(cur.trim()); cur = ''; continue; }
      cur += ch;
    }
    if (cur.trim() !== '') out.push(cur.trim());
    return out;
  }

  function sqlLiteral(tok) {
    var t = tok.trim();
    if (/^null$/i.test(t)) return null;
    if (/^'.*'$/s.test(t)) return t.slice(1, -1).replace(/''/g, "'");
    if (/^N'.*'$/s.test(t)) return t.slice(2, -1).replace(/''/g, "'");
    if (/^[+-]?\d+$/.test(t)) return parseInt(t, 10);
    if (/^[+-]?\d*\.\d+$/.test(t)) return parseFloat(t);
    if (/^(true|false)$/i.test(t)) return /^true$/i.test(t);
    return t;
  }

  var INSERT_RE = /INSERT\s+INTO\s+([`"\[\]\w.]+)\s*(?:\(([^)]*)\))?\s*VALUES\s*/ig;

  function sqlDumpToDataset(text) {
    var src = stripBOM(text), m, tables = {}, order = [];
    INSERT_RE.lastIndex = 0;
    while ((m = INSERT_RE.exec(src)) !== null) {
      var table = m[1].replace(/[`"\[\]]/g, '');
      var cols = m[2] ? m[2].split(',').map(function (c) { return c.trim().replace(/[`"\[\]]/g, ''); }) : null;
      // read the tuple list that follows
      var i = INSERT_RE.lastIndex, depth = 0, inQ = false, buf = '';
      while (i < src.length) {
        var ch = src[i];
        if (inQ) { if (ch === "'" && src[i + 1] === "'") { buf += "''"; i += 2; continue; } if (ch === "'") inQ = false; buf += ch; i++; continue; }
        if (ch === "'") { inQ = true; buf += ch; i++; continue; }
        if (ch === '(') depth++;
        if (ch === ')') depth--;
        if (ch === ';' && depth === 0) break;
        buf += ch; i++;
      }
      INSERT_RE.lastIndex = i;
      var tuples = buf.match(/\(([\s\S]*?)\)(?=\s*(,|$))/g) || [];
      if (!tables[table]) { tables[table] = { columns: cols, rows: [] }; order.push(table); }
      if (cols && !tables[table].columns) tables[table].columns = cols;
      tuples.forEach(function (t) {
        var inner = t.replace(/^\(/, '').replace(/\)$/, '');
        tables[table].rows.push(splitSqlTuple(inner).map(sqlLiteral));
      });
    }
    if (!order.length) return null;
    var first = order[0], t0 = tables[first];
    var width = t0.rows.length ? Math.max.apply(null, t0.rows.map(function (r) { return r.length; })) : 0;
    var columns = t0.columns || Array.apply(null, Array(width)).map(function (_, i) { return 'column_' + (i + 1); });
    var warnings = [];
    if (order.length > 1) warnings.push({ code: 'MULTIPLE_TABLES', message: 'The dump contains ' + order.length + ' tables (' + order.join(', ') + '). The first one was loaded; pass { table: "name" } to pick another.', tables: order });
    if (!t0.columns) warnings.push({ code: 'NO_COLUMN_LIST', message: 'The INSERT statements had no column list, so columns were named positionally.' });
    return dataset('sql-insert', columns, t0.rows, { table: first, tables: order, allTables: tables }, warnings);
  }

  /* ------------------------------------------------------------------ *
   * INI / .properties / key=value
   * ------------------------------------------------------------------ */

  function iniToDataset(text) {
    var section = '', rows = [];
    nonEmptyLines(stripBOM(text)).forEach(function (l) {
      var t = l.trim();
      if (/^[#;]/.test(t)) return;
      var s = t.match(/^\[(.+)\]$/);
      if (s) { section = s[1]; return; }
      var m = t.match(/^([^=:]+)\s*[=:]\s*(.*)$/);
      if (m) rows.push([section, m[1].trim(), m[2].trim()]);
    });
    if (!rows.length) return null;
    return dataset('ini', ['section', 'key', 'value'], rows, {}, []);
  }

  /* ------------------------------------------------------------------ *
   * Excel (SheetJS, if present)
   * ------------------------------------------------------------------ */

  function getXLSX() {
    if (typeof XLSX !== 'undefined') return XLSX;
    if (typeof window !== 'undefined' && window.XLSX) return window.XLSX;
    return null;
  }

  function workbookToDataset(arrayBuffer, opts) {
    var X = getXLSX();
    if (!X) throw new Error('Excel support needs SheetJS. Add <script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script> to the page.');
    var wb = X.read(arrayBuffer, { type: 'array', cellDates: true });
    var sheetName = (opts && opts.sheet) || wb.SheetNames[0];
    var grid = X.utils.sheet_to_json(wb.Sheets[sheetName], { header: 1, defval: null, raw: true });
    grid = grid.filter(function (r) { return r.some(function (c) { return !isBlank(c); }); });
    if (!grid.length) throw new Error('Sheet "' + sheetName + '" is empty.');
    var hasHeader = looksLikeHeader(grid.map(function (r) { return r.map(function (c) { return c === null ? '' : String(c); }); }));
    var columns = hasHeader ? grid[0].map(function (h, i) { return isBlank(h) ? 'column_' + (i + 1) : String(h).trim(); })
                            : grid[0].map(function (_, i) { return 'column_' + (i + 1); });
    var body = hasHeader ? grid.slice(1) : grid;
    var width = columns.length;
    var rows = body.map(function (r) { var o = r.slice(0, width); while (o.length < width) o.push(null); return o; });
    var warnings = [];
    if (wb.SheetNames.length > 1) warnings.push({ code: 'MULTIPLE_SHEETS', message: 'Workbook has ' + wb.SheetNames.length + ' sheets (' + wb.SheetNames.join(', ') + '). "' + sheetName + '" was loaded; pass { sheet: "name" } to pick another.', sheets: wb.SheetNames });
    return dataset('xlsx', columns, rows, { sheet: sheetName, sheets: wb.SheetNames }, warnings);
  }

  /* ------------------------------------------------------------------ *
   * Format detection
   * ------------------------------------------------------------------ */

  var MAGIC = [
    { bytes: [0x50, 0x4B, 0x03, 0x04], format: 'xlsx', label: 'Office Open XML / ZIP container' },
    { bytes: [0xD0, 0xCF, 0x11, 0xE0], format: 'xls', label: 'Legacy Excel (OLE2 compound file)' },
    { bytes: [0x50, 0x41, 0x52, 0x31], format: 'parquet', label: 'Apache Parquet' },
    { bytes: [0x4F, 0x62, 0x6A, 0x01], format: 'avro', label: 'Apache Avro' },
    { bytes: [0x53, 0x51, 0x4C, 0x69], format: 'sqlite', label: 'SQLite database' },
    { bytes: [0x25, 0x50, 0x44, 0x46], format: 'pdf', label: 'PDF document' }
  ];

  function detectMagic(bytes) {
    for (var i = 0; i < MAGIC.length; i++) {
      var m = MAGIC[i], ok = true;
      for (var b = 0; b < m.bytes.length; b++) if (bytes[b] !== m.bytes[b]) { ok = false; break; }
      if (ok) return m;
    }
    return null;
  }

  function detect(input, opts) {
    opts = opts || {};
    var evidence = [], alternatives = [];

    // Binary sniff first
    if (input && (input instanceof ArrayBuffer || (typeof Uint8Array !== 'undefined' && input instanceof Uint8Array))) {
      var bytes = input instanceof ArrayBuffer ? new Uint8Array(input, 0, Math.min(16, input.byteLength)) : input.subarray(0, 16);
      var magic = detectMagic(bytes);
      if (magic) return { format: magic.format, confidence: 0.99, evidence: ['Magic bytes match ' + magic.label + '.'], alternatives: [], binary: true };
      // fall through: decode as text
      try { input = new TextDecoder('utf-8').decode(input); } catch (e) { return { format: 'binary', confidence: 0.5, evidence: ['Not decodable as UTF-8 text.'], alternatives: [], binary: true }; }
    }

    var text = stripBOM(String(input || ''));
    var trimmed = text.trim();
    if (trimmed === '') return { format: 'empty', confidence: 1, evidence: ['Input is empty.'], alternatives: [] };

    var head = trimmed.slice(0, 4096);
    var sample = nonEmptyLines(text, 40);

    function push(format, confidence, why) { alternatives.push({ format: format, confidence: confidence, reason: why }); }

    // JSON
    if (/^[\[{]/.test(trimmed)) {
      try {
        JSON.parse(trimmed);
        return { format: 'json', confidence: 0.99, evidence: ['Whole input parses as a single JSON value.'], alternatives: [] };
      } catch (e) { push('json', 0.3, 'Starts with ' + trimmed[0] + ' but does not parse as one JSON value: ' + e.message); }
    }

    // NDJSON / JSON Lines
    if (sample.length > 1) {
      var jsonLines = sample.filter(function (l) {
        var t = l.trim();
        if (!/^[\[{]/.test(t)) return false;
        try { JSON.parse(t); return true; } catch (e) { return false; }
      }).length;
      if (jsonLines / sample.length > 0.8) {
        return { format: 'ndjson', confidence: clamp(jsonLines / sample.length, 0, 0.98), evidence: [jsonLines + ' of the first ' + sample.length + ' lines are standalone JSON values.'], alternatives: [] };
      }
      if (jsonLines > 0) push('ndjson', jsonLines / sample.length, jsonLines + ' lines parse as JSON.');
    }

    // XML / HTML
    if (/^<\?xml/i.test(trimmed)) return { format: 'xml', confidence: 0.99, evidence: ['XML declaration on the first line.'], alternatives: [] };
    if (/^<!doctype html/i.test(trimmed) || /<html[\s>]/i.test(head)) {
      if (/<table[\s>]/i.test(text)) return { format: 'html-table', confidence: 0.9, evidence: ['HTML document containing a <table> element.'], alternatives: [{ format: 'html', confidence: 0.9, reason: 'HTML document' }] };
      return { format: 'html', confidence: 0.9, evidence: ['HTML document with no <table> to extract.'], alternatives: [] };
    }
    if (/^<[A-Za-z_][\w.:-]*[\s>]/.test(trimmed) && /<\/[A-Za-z_][\w.:-]*>\s*$/.test(trimmed)) {
      if (/<table[\s>]/i.test(text)) push('html-table', 0.6, 'Contains a <table> element.');
      return { format: 'xml', confidence: 0.85, evidence: ['Balanced markup: opens and closes with matching element tags.'], alternatives: alternatives };
    }

    // SQL dump
    if (/\bINSERT\s+INTO\b/i.test(head)) {
      var inserts = (text.match(/\bINSERT\s+INTO\b/ig) || []).length;
      return { format: 'sql-insert', confidence: clamp(0.7 + inserts * 0.02, 0, 0.97), evidence: [inserts + ' INSERT INTO statement(s) found.'], alternatives: [] };
    }
    if (/\bCREATE\s+TABLE\b/i.test(head)) push('sql-ddl', 0.7, 'CREATE TABLE statement found but no INSERT rows to load.');

    // Delimited
    var llines = logicalLines(text, '"', 60);
    var sniff = sniffDelimiter(text);
    // One line on its own is weak evidence — two commas in a broken JSON fragment
    // must not read as a CSV. Require a second record, or at least three fields.
    if (sniff && llines.length < 2 && sniff.fields < 3) {
      push(sniff.name, 0.35, 'Only one line, so the delimiter cannot be confirmed against a second record.');
      sniff = null;
    }
    if (sniff && sniff.consistency >= 0.9 && sniff.fields >= 2) {
      var label = { ',': 'commas', '\t': 'tabs', ';': 'semicolons', '|': 'pipes', '~': 'tildes', '': 'SOH characters' }[sniff.delimiter] || 'delimiters';
      return {
        format: sniff.name,
        confidence: clamp(0.55 + sniff.consistency * 0.42, 0, 0.98),
        evidence: [Math.round(sniff.consistency * 100) + '% of sampled lines have exactly ' + (sniff.fields - 1) + ' unquoted ' + label + ' (' + sniff.fields + ' fields).'],
        alternatives: alternatives,
        delimiter: sniff.delimiter
      };
    }
    if (sniff) push(sniff.name, sniff.consistency, 'Delimiter counts are inconsistent across lines (' + Math.round(sniff.consistency * 100) + '% agreement).');

    // YAML
    if (/^\s*-\s+\w+\s*:/m.test(head) || (/^[\w.-]+\s*:\s*(\S|$)/m.test(head) && !/^\s*[\w.-]+\s*:\s*\/\//m.test(head))) {
      var yamlLines = sample.filter(function (l) { return /^\s*(-\s+)?[\w."'-]+\s*:(\s|$)/.test(l) || /^\s*-\s+/.test(l); }).length;
      if (yamlLines / sample.length > 0.6) {
        return { format: 'yaml', confidence: clamp(0.5 + (yamlLines / sample.length) * 0.4, 0, 0.92), evidence: [yamlLines + ' of ' + sample.length + ' sampled lines are YAML key/value or list entries.'], alternatives: alternatives };
      }
    }

    // INI / properties
    if (/^\s*\[[^\]]+\]\s*$/m.test(head) && /^[^=\n]+=.*$/m.test(head)) {
      return { format: 'ini', confidence: 0.85, evidence: ['[section] headers with key=value pairs.'], alternatives: alternatives };
    }
    var kvLines = sample.filter(function (l) { return /^[^=\n#;]+=[^=]*$/.test(l); }).length;
    if (sample.length > 1 && kvLines / sample.length > 0.8) {
      return { format: 'ini', confidence: 0.7, evidence: [kvLines + ' of ' + sample.length + ' lines are key=value pairs.'], alternatives: alternatives };
    }

    // Fixed width
    if (sample.length >= 3) {
      var cuts = detectFixedWidthColumns(sample.slice(0, 50));
      var widths = sample.map(function (l) { return l.length; });
      var widthConsistency = widths.filter(function (w) { return Math.abs(w - median(widths)) <= 2; }).length / widths.length;
      if (cuts.length >= 3 && widthConsistency > 0.8) {
        return {
          format: 'fixed-width',
          confidence: clamp(0.4 + widthConsistency * 0.4, 0, 0.85),
          evidence: [(cuts.length - 1) + ' column boundaries found at consistent character positions; ' + Math.round(widthConsistency * 100) + '% of lines share the same length.'],
          alternatives: alternatives
        };
      }
      if (cuts.length >= 3) push('fixed-width', 0.4, (cuts.length - 1) + ' whitespace-aligned columns, but line lengths vary.');
    }

    // Single column: a plain list with a name at the top
    var scl = lines(text);
    while (scl.length && scl[scl.length - 1].trim() === '') scl.pop();
    if (singleColumnHeader(scl, opts.nullTokens)) {
      return {
        format: 'single-column',
        confidence: 0.7,
        evidence: ['One value per line with "' + String(scl[0]).trim() + '" as a header — the values below it are consistently typed.'],
        alternatives: alternatives.sort(function (a, b) { return b.confidence - a.confidence; })
      };
    }

    return {
      format: 'text',
      confidence: 0.3,
      evidence: ['No structured format matched with confidence. Treated as plain text (one column, one row per line).'],
      alternatives: alternatives.sort(function (a, b) { return b.confidence - a.confidence; })
    };
  }

  /* ------------------------------------------------------------------ *
   * Parse — detect then dispatch
   * ------------------------------------------------------------------ */

  var PARSERS = {
    'json': jsonToDataset,
    'ndjson': ndjsonToDataset,
    'xml': xmlToDataset,
    'html-table': htmlTableToDataset,
    'html': htmlTableToDataset,
    'yaml': yamlToDataset,
    'ini': iniToDataset,
    'sql-insert': sqlDumpToDataset,
    'fixed-width': fixedWidthToDataset,
    'csv': delimitedToDataset,
    'tsv': delimitedToDataset,
    'psv': delimitedToDataset,
    'csv-semicolon': delimitedToDataset,
    'soh-delimited': delimitedToDataset,
    'tilde-delimited': delimitedToDataset,
    'single-column': singleColumnToDataset,
    'text': singleColumnToDataset
  };

  function parse(input, opts) {
    opts = opts || {};
    var det = opts.format ? { format: opts.format, confidence: 1, evidence: ['Format supplied by the caller.'], alternatives: [] } : detect(input, opts);

    if (det.format === 'xlsx' || det.format === 'xls') {
      var buf = input instanceof ArrayBuffer ? input : (input && input.buffer) ? input.buffer : null;
      if (!buf) throw new Error('Excel files must be passed as an ArrayBuffer.');
      var dsx = workbookToDataset(buf, opts);
      dsx.detection = det;
      return dsx;
    }
    if (det.format === 'parquet' || det.format === 'avro' || det.format === 'sqlite' || det.format === 'pdf' || det.format === 'binary') {
      throw new Error(det.format.toUpperCase() + ' is recognised but cannot be read in the browser. Export it to CSV, JSON or Parquet-to-CSV server-side first.');
    }
    if (det.format === 'empty') throw new Error('Nothing to parse — the input is empty.');

    var text = (typeof input === 'string') ? input : new TextDecoder('utf-8').decode(input);
    var ds = null;

    // "text" is the catch-all and always parses, so when it wins only by default we
    // try the runner-up structured formats first rather than short-circuiting on it.
    var alts = (det.alternatives || []).slice().sort(function (a, b) { return b.confidence - a.confidence; });
    var order = (det.format === 'text' || det.confidence < 0.5)
      ? alts.map(function (a) { return a.format; }).concat([det.format])
      : [det.format].concat(alts.map(function (a) { return a.format; }));

    for (var i = 0; i < order.length && !ds; i++) {
      var fn = PARSERS[order[i]];
      if (!fn) continue;
      try {
        ds = fn(text, opts);
        if (ds && order[i] !== det.format) det.evidence.push('Parsed as ' + order[i] + ' — the strongest format that actually loaded.');
      } catch (e) {
        ds = null;
        det.evidence.push('Parsing as ' + order[i] + ' failed: ' + e.message);
      }
    }
    if (!ds) { try { ds = delimitedToDataset(text, opts); } catch (e3) { ds = null; } }
    if (!ds) { try { ds = singleColumnToDataset(text, opts); } catch (e4) { ds = null; } }
    if (!ds) {
      ds = dataset('text', ['line'], nonEmptyLines(text).map(function (l) { return [l]; }), {}, [
        { code: 'UNSTRUCTURED', message: 'No structure was recognised. Each line became one row.' }
      ]);
    }
    ds.detection = det;
    if (opts.limit && ds.rows.length > opts.limit) {
      ds.meta.truncatedFrom = ds.rows.length;
      ds.rows = ds.rows.slice(0, opts.limit);
    }
    return ds;
  }

  /* ------------------------------------------------------------------ *
   * Type inference / profiling
   * ------------------------------------------------------------------ */

  var PATTERNS = {
    uuid: /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
    email: /^[^\s@]+@[^\s@.]+\.[^\s@]{2,}$/,
    url: /^https?:\/\/[^\s]+$/i,
    integer: /^[+-]?\d{1,18}$/,
    decimal: /^[+-]?(\d+\.\d*|\.\d+|\d+)$/,
    float: /^[+-]?(\d+\.?\d*|\.\d+)[eE][+-]?\d+$/,
    isoDate: /^\d{4}-\d{2}-\d{2}$/,
    isoDateTime: /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?$/,
    slashDate: /^(\d{1,2})[\/.](\d{1,2})[\/.](\d{4}|\d{2})$/,
    time: /^\d{1,2}:\d{2}(:\d{2}(\.\d+)?)?$/,
    money: /^[£$€¥]\s?[+-]?[\d,]+(\.\d{1,4})?$|^[+-]?[\d,]+(\.\d{1,4})?\s?(GBP|USD|EUR)$/i,
    thousands: /^[+-]?\d{1,3}(,\d{3})+(\.\d+)?$/,
    boolWord: /^(true|false|yes|no|y|n|t|f)$/i,
    hex: /^0x[0-9a-f]+$/i
  };

  function profileColumn(name, values, opts) {
    opts = opts || {};
    var nullTokens = opts.nullTokens || NULL_TOKENS;
    var total = values.length, nulls = 0, nonNull = [];
    for (var i = 0; i < total; i++) {
      var v = values[i];
      if (v === null || v === undefined || isNullToken(v, nullTokens)) nulls++;
      else nonNull.push(v);
    }

    var counts = {}, maxLen = 0, minLen = Infinity, numMin = Infinity, numMax = -Infinity;
    var maxScale = 0, maxPrecision = 0;
    var distinct = {}, distinctCount = 0, distinctOverflow = false;
    var ambiguousDate = false, dayFirstEvidence = 0, monthFirstEvidence = 0;

    function bump(t) { counts[t] = (counts[t] || 0) + 1; }

    for (var j = 0; j < nonNull.length; j++) {
      var raw = nonNull[j];
      var s = (typeof raw === 'string') ? raw.trim() : raw;

      if (distinctCount < 5000) {
        var key = typeof s === 'object' ? JSON.stringify(s) : String(s);
        if (!distinct[key]) { distinct[key] = 1; distinctCount++; }
      } else distinctOverflow = true;

      // native (from JSON / XLSX) types short-circuit the regexes
      if (typeof raw === 'boolean') { bump('boolean'); continue; }
      if (raw instanceof Date) { bump('datetime'); continue; }
      if (typeof raw === 'number') {
        if (Number.isInteger(raw)) bump('integer'); else { bump('decimal'); maxScale = Math.max(maxScale, (String(raw).split('.')[1] || '').length); }
        numMin = Math.min(numMin, raw); numMax = Math.max(numMax, raw);
        maxPrecision = Math.max(maxPrecision, String(Math.trunc(Math.abs(raw))).length);
        continue;
      }
      if (typeof raw === 'object') { bump('json'); maxLen = Math.max(maxLen, JSON.stringify(raw).length); continue; }

      var str = String(s);
      maxLen = Math.max(maxLen, str.length);
      minLen = Math.min(minLen, str.length);

      if (PATTERNS.uuid.test(str)) { bump('uuid'); continue; }
      if (PATTERNS.boolWord.test(str)) { bump('boolean'); continue; }
      if (PATTERNS.isoDateTime.test(str)) { bump('datetime'); continue; }
      if (PATTERNS.isoDate.test(str)) { bump('date'); continue; }
      var sd = str.match(PATTERNS.slashDate);
      if (sd) {
        bump('date');
        var a = parseInt(sd[1], 10), b = parseInt(sd[2], 10);
        if (a > 12) dayFirstEvidence++;
        if (b > 12) monthFirstEvidence++;
        continue;
      }
      if (PATTERNS.time.test(str)) { bump('time'); continue; }
      if (PATTERNS.email.test(str)) { bump('email'); continue; }
      if (PATTERNS.url.test(str)) { bump('url'); continue; }
      if (PATTERNS.money.test(str)) {
        bump('money');
        var mnum = parseFloat(str.replace(/[^0-9.+-]/g, ''));
        if (!isNaN(mnum)) { numMin = Math.min(numMin, mnum); numMax = Math.max(numMax, mnum); }
        maxScale = Math.max(maxScale, (str.split('.')[1] || '').replace(/[^\d]/g, '').length);
        continue;
      }
      if (PATTERNS.thousands.test(str)) {
        bump('decimal');
        var tnum = parseFloat(str.replace(/,/g, ''));
        if (!isNaN(tnum)) { numMin = Math.min(numMin, tnum); numMax = Math.max(numMax, tnum); }
        maxScale = Math.max(maxScale, (str.split('.')[1] || '').length);
        maxPrecision = Math.max(maxPrecision, str.replace(/[^\d]/g, '').length);
        continue;
      }
      if (PATTERNS.integer.test(str)) {
        // leading zeros mean an identifier, not a number (postcodes, account refs, phone numbers)
        if (/^0\d/.test(str)) { bump('string'); continue; }
        bump('integer');
        var inum = parseInt(str, 10);
        numMin = Math.min(numMin, inum); numMax = Math.max(numMax, inum);
        maxPrecision = Math.max(maxPrecision, str.replace(/[^\d]/g, '').length);
        continue;
      }
      if (PATTERNS.float.test(str)) { bump('float'); continue; }
      if (PATTERNS.decimal.test(str)) {
        bump('decimal');
        var dnum = parseFloat(str);
        if (!isNaN(dnum)) { numMin = Math.min(numMin, dnum); numMax = Math.max(numMax, dnum); }
        maxScale = Math.max(maxScale, (str.split('.')[1] || '').length);
        maxPrecision = Math.max(maxPrecision, str.replace(/[^\d]/g, '').length);
        continue;
      }
      if ((str[0] === '{' || str[0] === '[')) {
        try { JSON.parse(str); bump('json'); continue; } catch (e) { /* fall through */ }
      }
      bump('string');
    }

    // Resolve the winning type, collapsing compatible numeric families.
    var order = ['uuid', 'boolean', 'datetime', 'date', 'time', 'email', 'url', 'money', 'json', 'float', 'decimal', 'integer', 'string'];
    var winner = 'string', winnerCount = 0;
    order.forEach(function (t) { if ((counts[t] || 0) > winnerCount) { winnerCount = counts[t]; winner = t; } });

    // integer + decimal in the same column -> decimal; date + datetime -> datetime
    if (counts.integer && counts.decimal) { winner = counts.float ? 'float' : 'decimal'; winnerCount = counts.integer + counts.decimal + (counts.float || 0); }
    if (counts.date && counts.datetime) { winner = 'datetime'; winnerCount = counts.date + counts.datetime; }
    if (counts.string && counts.string > 0 && winner !== 'string') {
      // any genuinely unparseable value forces a string column, but we keep the confidence honest
    }

    var confidence = nonNull.length ? winnerCount / nonNull.length : 0;
    if (!nonNull.length) { winner = 'unknown'; confidence = 0; }

    var mixed = Object.keys(counts).filter(function (k) { return counts[k] > 0; });
    if (dayFirstEvidence && monthFirstEvidence) ambiguousDate = true;
    else if ((counts.date || 0) > 0 && !dayFirstEvidence && !monthFirstEvidence && /[\/.]/.test(String(nonNull[0] || ''))) ambiguousDate = true;

    var col = {
      name: name,
      type: winner,
      confidence: Math.round(confidence * 1000) / 1000,
      count: total,
      nulls: nulls,
      nullRate: total ? Math.round((nulls / total) * 1000) / 1000 : 0,
      nullable: nulls > 0,
      distinct: distinctOverflow ? '5000+' : distinctCount,
      unique: !distinctOverflow && distinctCount === nonNull.length && nonNull.length > 0,
      maxLength: maxLen,
      minLength: minLen === Infinity ? 0 : minLen,
      min: numMin === Infinity ? null : numMin,
      max: numMax === -Infinity ? null : numMax,
      precision: maxPrecision,
      scale: maxScale,
      mixedTypes: mixed.length > 1 ? mixed.map(function (t) { return { type: t, count: counts[t] }; }).sort(function (a, b) { return b.count - a.count; }) : null,
      samples: nonNull.slice(0, 5).map(function (v) { return typeof v === 'object' ? JSON.stringify(v) : v; }),
      issues: []
    };

    // Uniqueness across a handful of rows proves nothing — every column in a
    // 3-row sample is "unique". Only claim a key when the sample can support it.
    var MIN_ROWS_FOR_KEY = (opts && opts.minRowsForKey) || 10;
    // A unique decimal amount is a coincidence, not a key. Restrict candidates to
    // the types real keys are actually built from.
    var KEYABLE = { integer: 1, string: 1, uuid: 1, email: 1 };
    var keyShaped = col.unique && !col.nullable && !!KEYABLE[winner];
    col.primaryKeyCandidate = keyShaped && col.count >= MIN_ROWS_FOR_KEY;
    col.keyEvidenceWeak = keyShaped && col.count < MIN_ROWS_FOR_KEY;
    if (ambiguousDate) col.issues.push({ code: 'AMBIGUOUS_DATE', severity: 'high', message: 'Dates are written as d/m/y or m/d/y and the order cannot be proven from the data. Confirm the source locale before loading — this silently corrupts up to 12 days in every month.' });
    if (col.mixedTypes && confidence < 1) col.issues.push({ code: 'MIXED_TYPES', severity: confidence < 0.9 ? 'high' : 'medium', message: Math.round((1 - confidence) * 100) + '% of values do not fit ' + winner + '. A strict target column will reject them.' });
    if (col.nullRate === 1 && col.count > 0) col.issues.push({ code: 'ALL_NULL', severity: 'medium', message: 'Every value is null — the type cannot be inferred and the column may be dead weight.' });
    if (counts.money) col.issues.push({ code: 'CURRENCY_SYMBOLS', severity: 'medium', message: 'Values carry currency symbols or separators. Strip them and store the currency alongside the amount.' });
    if (winner === 'integer' && numMax > 2147483647) col.issues.push({ code: 'INT_OVERFLOW', severity: 'high', message: 'Values exceed the 32-bit INT range — the target column must be BIGINT.' });
    if (winner === 'string' && maxLen > 4000) col.issues.push({ code: 'LONG_TEXT', severity: 'low', message: 'Longest value is ' + maxLen + ' characters — needs NVARCHAR(MAX) / TEXT.' });
    if (/^column_\d+$/.test(name)) col.issues.push({ code: 'POSITIONAL_NAME', severity: 'low', message: 'No name was available in the source; this column was named by position.' });

    return col;
  }

  function profile(ds, opts) {
    var cols = [];
    for (var c = 0; c < ds.columns.length; c++) {
      var vals = new Array(ds.rows.length);
      for (var r = 0; r < ds.rows.length; r++) vals[r] = ds.rows[r][c];
      cols.push(profileColumn(ds.columns[c], vals, opts));
    }
    return cols;
  }

  /* ------------------------------------------------------------------ *
   * Value coercion using an inferred profile
   * ------------------------------------------------------------------ */

  function coerceValue(v, type, nullTokens) {
    if (v === null || v === undefined || isNullToken(v, nullTokens || NULL_TOKENS)) return null;
    if (typeof v !== 'string') return v;
    var s = v.trim();
    switch (type) {
      case 'integer': return /^[+-]?\d+$/.test(s) && !/^0\d/.test(s) ? parseInt(s, 10) : v;
      case 'decimal':
      case 'float':
      case 'money': {
        var n = parseFloat(s.replace(/[£$€¥,\s]/g, '').replace(/(GBP|USD|EUR)$/i, ''));
        return isNaN(n) ? v : n;
      }
      case 'boolean': return /^(true|yes|y|t|1)$/i.test(s) ? true : /^(false|no|n|f|0)$/i.test(s) ? false : v;
      case 'json': try { return JSON.parse(s); } catch (e) { return v; }
      default: return v;
    }
  }

  function coerce(ds, cols, opts) {
    cols = cols || profile(ds, opts);
    var rows = ds.rows.map(function (r) {
      return r.map(function (v, i) { return coerceValue(v, cols[i] ? cols[i].type : 'string', opts && opts.nullTokens); });
    });
    var out = dataset(ds.format, ds.columns, rows, ds.meta, ds.warnings);
    out.detection = ds.detection;
    return out;
  }

  /* ------------------------------------------------------------------ *
   * SQL dialects & DDL
   * ------------------------------------------------------------------ */

  var DIALECTS = {
    sqlserver: {
      label: 'SQL Server / Azure SQL',
      quote: function (id) { return '[' + String(id).replace(/]/g, ']]') + ']'; },
      str: function (s) { return "N'" + String(s).replace(/'/g, "''") + "'"; },
      bool: function (b) { return b ? '1' : '0'; },
      type: function (c) {
        switch (c.type) {
          case 'boolean': return 'BIT';
          case 'integer': return (c.max !== null && (c.max > 2147483647 || c.min < -2147483648)) ? 'BIGINT' : 'INT';
          case 'decimal': case 'money': return 'DECIMAL(' + clamp(Math.max(c.precision, c.scale + 1) + 2, 5, 38) + ',' + clamp(Math.max(c.scale, 2), 0, 10) + ')';
          case 'float': return 'FLOAT';
          case 'date': return 'DATE';
          case 'datetime': return 'DATETIME2(3)';
          case 'time': return 'TIME(3)';
          case 'uuid': return 'UNIQUEIDENTIFIER';
          case 'json': return 'NVARCHAR(MAX)';
          case 'unknown': return 'NVARCHAR(255)';
          default: return c.maxLength > 4000 ? 'NVARCHAR(MAX)' : 'NVARCHAR(' + clamp(Math.ceil(c.maxLength * 1.3 / 10) * 10 || 50, 10, 4000) + ')';
        }
      }
    },
    postgres: {
      label: 'PostgreSQL',
      quote: function (id) { return '"' + String(id).replace(/"/g, '""') + '"'; },
      str: function (s) { return "'" + String(s).replace(/'/g, "''") + "'"; },
      bool: function (b) { return b ? 'TRUE' : 'FALSE'; },
      type: function (c) {
        switch (c.type) {
          case 'boolean': return 'BOOLEAN';
          case 'integer': return (c.max !== null && (c.max > 2147483647 || c.min < -2147483648)) ? 'BIGINT' : 'INTEGER';
          case 'decimal': case 'money': return 'NUMERIC(' + clamp(Math.max(c.precision, c.scale + 1) + 2, 5, 38) + ',' + clamp(Math.max(c.scale, 2), 0, 10) + ')';
          case 'float': return 'DOUBLE PRECISION';
          case 'date': return 'DATE';
          case 'datetime': return 'TIMESTAMP';
          case 'time': return 'TIME';
          case 'uuid': return 'UUID';
          case 'json': return 'JSONB';
          case 'unknown': return 'TEXT';
          default: return c.maxLength > 4000 ? 'TEXT' : 'VARCHAR(' + clamp(Math.ceil(c.maxLength * 1.3 / 10) * 10 || 50, 10, 4000) + ')';
        }
      }
    },
    mysql: {
      label: 'MySQL / MariaDB',
      quote: function (id) { return '`' + String(id).replace(/`/g, '``') + '`'; },
      str: function (s) { return "'" + String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'") + "'"; },
      bool: function (b) { return b ? '1' : '0'; },
      type: function (c) {
        switch (c.type) {
          case 'boolean': return 'TINYINT(1)';
          case 'integer': return (c.max !== null && (c.max > 2147483647 || c.min < -2147483648)) ? 'BIGINT' : 'INT';
          case 'decimal': case 'money': return 'DECIMAL(' + clamp(Math.max(c.precision, c.scale + 1) + 2, 5, 65) + ',' + clamp(Math.max(c.scale, 2), 0, 10) + ')';
          case 'float': return 'DOUBLE';
          case 'date': return 'DATE';
          case 'datetime': return 'DATETIME(3)';
          case 'time': return 'TIME(3)';
          case 'uuid': return 'CHAR(36)';
          case 'json': return 'JSON';
          case 'unknown': return 'VARCHAR(255)';
          default: return c.maxLength > 4000 ? 'LONGTEXT' : 'VARCHAR(' + clamp(Math.ceil(c.maxLength * 1.3 / 10) * 10 || 50, 10, 4000) + ')';
        }
      }
    },
    sqlite: {
      label: 'SQLite',
      quote: function (id) { return '"' + String(id).replace(/"/g, '""') + '"'; },
      str: function (s) { return "'" + String(s).replace(/'/g, "''") + "'"; },
      bool: function (b) { return b ? '1' : '0'; },
      type: function (c) {
        switch (c.type) {
          case 'boolean': case 'integer': return 'INTEGER';
          case 'decimal': case 'float': case 'money': return 'REAL';
          default: return 'TEXT';
        }
      }
    }
  };

  function safeIdent(name) {
    var s = String(name).trim().replace(/[^\w]+/g, '_').replace(/^_+|_+$/g, '');
    if (s === '') s = 'col';
    if (/^\d/.test(s)) s = 'c_' + s;
    return s;
  }

  function ddl(ds, opts) {
    opts = opts || {};
    var d = DIALECTS[opts.dialect || 'sqlserver'] || DIALECTS.sqlserver;
    var cols = opts.columns || profile(ds, opts);
    var table = opts.table || ds.meta.table || ds.meta.sheet || 'imported_data';
    var schema = opts.schema || (opts.dialect === 'sqlserver' ? 'dbo' : null);
    var qname = (schema ? d.quote(schema) + '.' : '') + d.quote(safeIdent(table));
    var out = [];
    out.push('-- Generated by cygenix-transform v' + VERSION + ' — target: ' + d.label);
    out.push('-- Source format: ' + ds.format + (ds.detection ? ' (detected at ' + Math.round(ds.detection.confidence * 100) + '% confidence)' : '') + '; ' + ds.rows.length + ' row(s) profiled.');
    if (cols.some(function (c) { return c.type === 'string' && c.maxLength <= 4000; })) {
      out.push('-- String lengths are sized from this sample plus 30% headroom. Widen them before loading a full extract.');
    }
    out.push('');
    out.push('CREATE TABLE ' + qname + ' (');
    var pk = cols.filter(function (c) { return c.primaryKeyCandidate; })[0];
    var defs = cols.map(function (c) {
      var notes = [];
      if (c.confidence < 1 && c.type !== 'string' && c.type !== 'unknown') notes.push(Math.round(c.confidence * 100) + '% of values matched');
      if (pk && c === pk) notes.push('unique + not null: primary key candidate');
      c.issues.forEach(function (i) { if (i.severity === 'high') notes.push('!! ' + i.code); });
      return {
        sql: '    ' + d.quote(safeIdent(c.name)) + ' ' + d.type(c) + (c.nullable ? ' NULL' : ' NOT NULL'),
        note: notes.length ? '  -- ' + notes.join('; ') : ''
      };
    });
    // The comma must precede the trailing comment, or the statement will not parse.
    out.push(defs.map(function (x, i) { return x.sql + (i < defs.length - 1 ? ',' : '') + x.note; }).join('\n'));
    out.push(');');
    if (pk) {
      out.push('');
      out.push('-- ' + pk.name + ' is unique and never null across all ' + ds.rows.length + ' rows profiled.');
      out.push('-- ALTER TABLE ' + qname + ' ADD CONSTRAINT ' + d.quote('PK_' + safeIdent(table)) + ' PRIMARY KEY (' + d.quote(safeIdent(pk.name)) + ');');
    } else if (cols.some(function (c) { return c.keyEvidenceWeak; })) {
      out.push('');
      out.push('-- No primary key is proposed: ' + ds.rows.length + ' row(s) is too small a sample for uniqueness to mean anything.');
    }
    var highIssues = [];
    cols.forEach(function (c) { c.issues.forEach(function (i) { if (i.severity === 'high') highIssues.push(c.name + ': ' + i.message); }); });
    if (highIssues.length) {
      out.push('');
      out.push('-- Resolve before loading:');
      highIssues.forEach(function (h) { out.push('--   * ' + h); });
    }
    return out.join('\n');
  }

  function sqlInserts(ds, opts) {
    opts = opts || {};
    var d = DIALECTS[opts.dialect || 'sqlserver'] || DIALECTS.sqlserver;
    var cols = opts.columns || profile(ds, opts);
    var table = opts.table || ds.meta.table || ds.meta.sheet || 'imported_data';
    var schema = opts.schema || (opts.dialect === 'sqlserver' ? 'dbo' : null);
    var qname = (schema ? d.quote(schema) + '.' : '') + d.quote(safeIdent(table));
    var colList = cols.map(function (c) { return d.quote(safeIdent(c.name)); }).join(', ');
    var batch = opts.batchSize || 500;
    var typed = coerce(ds, cols, opts);
    var out = [], i = 0;
    while (i < typed.rows.length) {
      var chunk = typed.rows.slice(i, i + batch);
      var tuples = chunk.map(function (r) {
        return '(' + r.map(function (v) {
          if (v === null || v === undefined) return 'NULL';
          if (typeof v === 'number') return String(v);
          if (typeof v === 'boolean') return d.bool(v);
          if (v instanceof Date) return d.str(v.toISOString());
          if (typeof v === 'object') return d.str(JSON.stringify(v));
          return d.str(v);
        }).join(', ') + ')';
      });
      out.push('INSERT INTO ' + qname + ' (' + colList + ') VALUES\n' + tuples.join(',\n') + ';');
      i += batch;
    }
    return out.join('\n\n');
  }

  /* ------------------------------------------------------------------ *
   * Emitters
   * ------------------------------------------------------------------ */

  function csvEscape(v, delim, quote, forceQuote) {
    if (v === null || v === undefined) return '';
    var s = (v instanceof Date) ? v.toISOString() : (typeof v === 'object' ? JSON.stringify(v) : String(v));
    var needs = forceQuote || s.indexOf(delim) >= 0 || s.indexOf(quote) >= 0 || /[\r\n]/.test(s) || s !== s.trim();
    if (!needs) return s;
    return quote + s.split(quote).join(quote + quote) + quote;
  }

  function toDelimited(ds, opts) {
    opts = opts || {};
    var delim = opts.delimiter || ',', quote = opts.quote || '"', eol = opts.eol || '\r\n';
    var out = [];
    if (opts.header !== false) out.push(ds.columns.map(function (c) { return csvEscape(c, delim, quote, opts.quoteAll); }).join(delim));
    for (var r = 0; r < ds.rows.length; r++) {
      out.push(ds.rows[r].map(function (v) { return csvEscape(v, delim, quote, opts.quoteAll); }).join(delim));
    }
    return out.join(eol);
  }

  function toJSON(ds, opts) {
    opts = opts || {};
    var data = (opts.typed === false ? ds : coerce(ds, opts.columns, opts)).toObjects();
    return JSON.stringify(data, null, opts.indent === undefined ? 2 : opts.indent);
  }

  function toNDJSON(ds, opts) {
    opts = opts || {};
    var data = (opts.typed === false ? ds : coerce(ds, opts.columns, opts)).toObjects();
    return data.map(function (o) { return JSON.stringify(o); }).join('\n');
  }

  function xmlEscape(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  function toXML(ds, opts) {
    opts = opts || {};
    var root = opts.root || 'rows', rec = opts.record || 'row';
    var out = ['<?xml version="1.0" encoding="UTF-8"?>', '<' + root + '>'];
    for (var r = 0; r < ds.rows.length; r++) {
      out.push('  <' + rec + '>');
      for (var c = 0; c < ds.columns.length; c++) {
        var tag = safeIdent(ds.columns[c]);
        var v = ds.rows[r][c];
        if (v === null || v === undefined) out.push('    <' + tag + ' xsi:nil="true"/>');
        else out.push('    <' + tag + '>' + xmlEscape(typeof v === 'object' ? JSON.stringify(v) : v) + '</' + tag + '>');
      }
      out.push('  </' + rec + '>');
    }
    out.push('</' + root + '>');
    return out.join('\n');
  }

  function toYAML(ds, opts) {
    opts = opts || {};
    var data = (opts.typed === false ? ds : coerce(ds, opts.columns, opts)).toObjects();
    function scal(v) {
      if (v === null || v === undefined) return 'null';
      if (typeof v === 'number' || typeof v === 'boolean') return String(v);
      if (v instanceof Date) return v.toISOString();
      var s = typeof v === 'object' ? JSON.stringify(v) : String(v);
      if (s === '' || /[:#\-{}\[\],&*?|>%@`"'\n]/.test(s) || /^\s|\s$/.test(s) || /^(true|false|null|yes|no|~)$/i.test(s) || /^[+-]?[\d.]+$/.test(s)) {
        return '"' + s.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n') + '"';
      }
      return s;
    }
    return data.map(function (o) {
      var keys = Object.keys(o);
      return keys.map(function (k, i) { return (i === 0 ? '- ' : '  ') + k + ': ' + scal(o[k]); }).join('\n');
    }).join('\n');
  }

  function toMarkdown(ds, opts) {
    opts = opts || {};
    var limit = opts.limit || ds.rows.length;
    var cells = ds.rows.slice(0, limit);
    var widths = ds.columns.map(function (c, i) {
      var w = String(c).length;
      cells.forEach(function (r) { var v = r[i]; var s = v === null || v === undefined ? '' : String(v); if (s.length > w) w = s.length; });
      return Math.min(w, 60);
    });
    function pad(s, w) { s = String(s === null || s === undefined ? '' : s); if (s.length > w) s = s.slice(0, w - 1) + '…'; return s + ' '.repeat(Math.max(0, w - s.length)); }
    var out = ['| ' + ds.columns.map(function (c, i) { return pad(c, widths[i]); }).join(' | ') + ' |'];
    out.push('| ' + widths.map(function (w) { return '-'.repeat(w); }).join(' | ') + ' |');
    cells.forEach(function (r) { out.push('| ' + r.map(function (v, i) { return pad(v, widths[i]); }).join(' | ') + ' |'); });
    return out.join('\n');
  }

  function toHTMLTable(ds, opts) {
    opts = opts || {};
    var out = ['<table>', '  <thead><tr>' + ds.columns.map(function (c) { return '<th>' + xmlEscape(c) + '</th>'; }).join('') + '</tr></thead>', '  <tbody>'];
    ds.rows.slice(0, opts.limit || ds.rows.length).forEach(function (r) {
      out.push('    <tr>' + r.map(function (v) { return '<td>' + (v === null || v === undefined ? '' : xmlEscape(typeof v === 'object' ? JSON.stringify(v) : v)) + '</td>'; }).join('') + '</tr>');
    });
    out.push('  </tbody>', '</table>');
    return out.join('\n');
  }

  /* ------------------------------------------------------------------ *
   * Column mapping — fuzzy match a source schema onto a target schema
   * ------------------------------------------------------------------ */

  // CustomerID -> "customer id"; acct_balance -> "acct balance".
  // Without the camelCase split, PascalCase target schemas never match snake_case sources.
  function splitCamel(s) {
    return String(s)
      .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
      .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
      .replace(/([A-Za-z])(\d)/g, '$1 $2');
  }

  function normaliseName(s) {
    return splitCamel(s).toLowerCase().replace(/[^a-z0-9]+/g, ' ').replace(/\s+/g, ' ').trim();
  }

  var ABBREVIATIONS = {
    'id': 'identifier', 'no': 'number', 'num': 'number', 'nbr': 'number', 'qty': 'quantity',
    'amt': 'amount', 'desc': 'description', 'dt': 'date', 'ts': 'timestamp', 'addr': 'address',
    'cust': 'customer', 'acct': 'account', 'ref': 'reference', 'nm': 'name', 'fname': 'first name',
    'lname': 'last name', 'dob': 'date of birth', 'tel': 'telephone', 'ph': 'phone', 'pc': 'postcode',
    'org': 'organisation', 'co': 'company', 'val': 'value', 'crt': 'created', 'upd': 'updated',
    'emp': 'employee', 'dept': 'department', 'mgr': 'manager', 'inv': 'invoice', 'ord': 'order',
    'prod': 'product', 'txn': 'transaction', 'pct': 'percent', 'yr': 'year', 'seq': 'sequence',
    // synonyms, not contractions — the same field under a different house style
    'surname': 'last name', 'familyname': 'last name', 'forename': 'first name',
    'givenname': 'first name', 'christianname': 'first name', 'zip': 'postcode',
    'email': 'email address', 'phone': 'telephone', 'mobile': 'telephone'
  };

  function expandTokens(name) {
    return normaliseName(name).split(' ').filter(Boolean).map(function (t) {
      return ABBREVIATIONS[t] || t;
    }).join(' ').split(' ').filter(Boolean);
  }

  // "emp" and "employee" are the same field; "dept" and "department" likewise.
  // Exact-match token overlap misses every contraction that is not in the table above.
  function tokenMatch(a, b) {
    if (a === b) return 1;
    if (a.length >= 3 && b.length >= 3) {
      if (b.indexOf(a) === 0 || a.indexOf(b) === 0) return 0.9;
    }
    return 0;
  }

  function softOverlap(ta, tb) {
    var used = {}, sum = 0;
    for (var i = 0; i < ta.length; i++) {
      var best = 0, bi = -1;
      for (var j = 0; j < tb.length; j++) {
        if (used[j]) continue;
        var s = tokenMatch(ta[i], tb[j]);
        if (s > best) { best = s; bi = j; }
      }
      if (bi >= 0) { used[bi] = 1; sum += best; }
    }
    var denom = ta.length + tb.length - sum;
    return denom > 0 ? sum / denom : 0;
  }

  function levenshtein(a, b) {
    if (a === b) return 0;
    var m = a.length, n = b.length;
    if (!m) return n; if (!n) return m;
    var prev = new Array(n + 1), cur = new Array(n + 1), i, j;
    for (j = 0; j <= n; j++) prev[j] = j;
    for (i = 1; i <= m; i++) {
      cur[0] = i;
      for (j = 1; j <= n; j++) {
        cur[j] = Math.min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1));
      }
      var t = prev; prev = cur; cur = t;
    }
    return prev[n];
  }

  function nameSimilarity(a, b) {
    var na = normaliseName(a), nb = normaliseName(b);
    if (na === nb) return 1;
    var ta = expandTokens(a), tb = expandTokens(b);
    var sa = ta.join(' '), sb = tb.join(' ');
    if (sa === sb) return 0.97;
    var jac = softOverlap(ta, tb);
    // edit distance on the compacted strings
    var ca = na.replace(/ /g, ''), cb = nb.replace(/ /g, '');
    var lev = 1 - levenshtein(ca, cb) / Math.max(ca.length, cb.length, 1);
    var contains = (ca.indexOf(cb) >= 0 || cb.indexOf(ca) >= 0) ? 0.15 : 0;
    return clamp(jac * 0.65 + lev * 0.35 + contains, 0, 0.96);
  }

  var TYPE_FAMILY = {
    integer: 'number', decimal: 'number', float: 'number', money: 'number',
    date: 'temporal', datetime: 'temporal', time: 'temporal',
    string: 'text', email: 'text', url: 'text', uuid: 'text', json: 'text',
    boolean: 'boolean', unknown: 'unknown'
  };

  function typeCompatibility(srcType, tgtType) {
    if (!tgtType) return 0.5;
    if (srcType === tgtType) return 1;
    var fa = TYPE_FAMILY[srcType] || 'text', fb = TYPE_FAMILY[tgtType] || 'text';
    if (fa === fb) return 0.85;
    if (fb === 'text') return 0.6;          // anything widens into text
    if (fa === 'unknown') return 0.5;
    return 0.15;
  }

  /**
   * map(sourceColumns, targetColumns)
   *   sourceColumns: profile() output, or an array of names
   *   targetColumns: [{name, type?, required?}] or an array of names
   * Returns { mappings: [...], unmappedSource: [...], unmappedTarget: [...] }
   */
  function map(sourceColumns, targetColumns, opts) {
    opts = opts || {};
    var threshold = opts.threshold === undefined ? 0.55 : opts.threshold;
    var src = sourceColumns.map(function (c) { return typeof c === 'string' ? { name: c, type: 'unknown' } : c; });
    var tgt = targetColumns.map(function (c) { return typeof c === 'string' ? { name: c } : c; });

    var pairs = [];
    src.forEach(function (s, si) {
      tgt.forEach(function (t, ti) {
        var nameScore = nameSimilarity(s.name, t.name);
        var typeScore = typeCompatibility(s.type, t.type);
        var score = nameScore * 0.75 + typeScore * 0.25;
        pairs.push({ si: si, ti: ti, score: score, nameScore: nameScore, typeScore: typeScore });
      });
    });
    pairs.sort(function (a, b) { return b.score - a.score; });

    var usedS = {}, usedT = {}, mappings = [];
    pairs.forEach(function (p) {
      if (usedS[p.si] || usedT[p.ti]) return;
      if (p.score < threshold) return;
      usedS[p.si] = 1; usedT[p.ti] = 1;
      var s = src[p.si], t = tgt[p.ti];
      var notes = [];
      if (p.nameScore < 0.9) notes.push('names differ (' + Math.round(p.nameScore * 100) + '% similar) — verify this pairing');
      if (t.type && p.typeScore < 0.85) notes.push('source is ' + s.type + ', target is ' + t.type + ' — a conversion is required');
      if (t.required && s.nullable) notes.push('target is NOT NULL but the source column contains nulls');
      mappings.push({
        source: s.name, target: t.name, confidence: Math.round(p.score * 1000) / 1000,
        nameScore: Math.round(p.nameScore * 1000) / 1000,
        typeScore: Math.round(p.typeScore * 1000) / 1000,
        sourceType: s.type, targetType: t.type || null,
        review: p.score < 0.85, notes: notes
      });
    });
    mappings.sort(function (a, b) { return b.confidence - a.confidence; });
    return {
      mappings: mappings,
      unmappedSource: src.filter(function (_, i) { return !usedS[i]; }).map(function (s) { return s.name; }),
      unmappedTarget: tgt.filter(function (_, i) { return !usedT[i]; }).map(function (t) { return t.name; })
    };
  }

  /* ------------------------------------------------------------------ *
   * One-call convenience
   * ------------------------------------------------------------------ */

  var TARGETS = {
    csv: function (ds, o) { return { text: toDelimited(ds, o), mime: 'text/csv', ext: 'csv' }; },
    tsv: function (ds, o) { return { text: toDelimited(ds, Object.assign({}, o, { delimiter: '\t' })), mime: 'text/tab-separated-values', ext: 'tsv' }; },
    psv: function (ds, o) { return { text: toDelimited(ds, Object.assign({}, o, { delimiter: '|' })), mime: 'text/plain', ext: 'txt' }; },
    json: function (ds, o) { return { text: toJSON(ds, o), mime: 'application/json', ext: 'json' }; },
    ndjson: function (ds, o) { return { text: toNDJSON(ds, o), mime: 'application/x-ndjson', ext: 'ndjson' }; },
    xml: function (ds, o) { return { text: toXML(ds, o), mime: 'application/xml', ext: 'xml' }; },
    yaml: function (ds, o) { return { text: toYAML(ds, o), mime: 'application/yaml', ext: 'yaml' }; },
    markdown: function (ds, o) { return { text: toMarkdown(ds, o), mime: 'text/markdown', ext: 'md' }; },
    html: function (ds, o) { return { text: toHTMLTable(ds, o), mime: 'text/html', ext: 'html' }; },
    sql: function (ds, o) { return { text: ddl(ds, o) + '\n\n' + sqlInserts(ds, o), mime: 'application/sql', ext: 'sql' }; },
    ddl: function (ds, o) { return { text: ddl(ds, o), mime: 'application/sql', ext: 'sql' }; }
  };

  /**
   * transform(input, target, opts) — detect, parse, profile and emit in one call.
   */
  function transform(input, target, opts) {
    opts = opts || {};
    var ds = parse(input, opts);
    var cols = profile(ds, opts);
    var emit = TARGETS[(target || 'json').toLowerCase()];
    if (!emit) throw new Error('Unknown target format "' + target + '". Supported: ' + Object.keys(TARGETS).join(', '));
    var o = Object.assign({}, opts, { columns: cols });
    var res = emit(ds, o);
    return {
      detection: ds.detection,
      format: ds.format,
      columns: cols,
      rowCount: ds.rows.length,
      warnings: ds.warnings,
      issues: cols.reduce(function (acc, c) { return acc.concat(c.issues.map(function (i) { return Object.assign({ column: c.name }, i); })); }, []),
      output: res.text,
      mime: res.mime,
      extension: res.ext,
      dataset: ds
    };
  }

  /* ------------------------------------------------------------------ *
   * Browser file helper
   * ------------------------------------------------------------------ */

  function readFile(file) {
    return new Promise(function (resolve, reject) {
      var binaryExt = /\.(xlsx|xlsm|xls|parquet|avro|db|sqlite|pdf)$/i.test(file.name || '');
      var fr = new FileReader();
      fr.onerror = function () { reject(new Error('Could not read ' + (file.name || 'the file') + '.')); };
      fr.onload = function () { resolve({ name: file.name, size: file.size, data: fr.result }); };
      if (binaryExt || file.size > 0 && /sheet|excel|octet-stream|zip/.test(file.type || '')) fr.readAsArrayBuffer(file);
      else fr.readAsText(file);
    });
  }

  function download(text, filename, mime) {
    var blob = new Blob([text], { type: (mime || 'text/plain') + ';charset=utf-8' });
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a');
    a.href = url; a.download = filename;
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    setTimeout(function () { URL.revokeObjectURL(url); }, 1000);
  }

  /* ------------------------------------------------------------------ *
   * Public API
   * ------------------------------------------------------------------ */

  return {
    version: VERSION,
    detect: detect,
    parse: parse,
    profile: profile,
    coerce: coerce,
    map: map,
    transform: transform,
    ddl: ddl,
    sqlInserts: sqlInserts,
    dialects: DIALECTS,
    to: {
      csv: toDelimited, delimited: toDelimited, json: toJSON, ndjson: toNDJSON,
      xml: toXML, yaml: toYAML, markdown: toMarkdown, html: toHTMLTable, sql: sqlInserts, ddl: ddl
    },
    targets: Object.keys(TARGETS),
    util: { readFile: readFile, download: download, flatten: flattenRecord, levenshtein: levenshtein, nameSimilarity: nameSimilarity, safeIdent: safeIdent }
  };
}));
