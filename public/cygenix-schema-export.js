/* ============================================================================
   cygenix-schema-export.js — take the Schema Explorer's diagram off the screen.
   ----------------------------------------------------------------------------
   THE PROBLEM

   Requested: a print-to-PDF button for the schema diagram, and "other useful
   ways to export this which include tables and columns (ie Excel, text)".

   The diagram is not a picture. It is HTML boxes in #sm-layer and SVG edges in
   #sm-edges, sharing one CSS transform, inside a viewport that is
   `height:calc(100vh - 132px); overflow:hidden`. So there is nothing to right-
   click and save, and printing the page as it stands prints whatever happens
   to be inside that viewport at the current pan and zoom — which is a crop of
   the thing you wanted, at whatever scale you left it.

   WHAT THIS PROVIDES

   Five exports off one model, so they can never disagree with each other:

     SVG          the diagram as a real vector file, whole, at any size
     PDF          the same SVG, laid on a page, through the browser's own
                  print dialog — "Save as PDF" is a destination there, and a
                  PDF writer that is already installed and already correct
                  beats one bundled from a CDN
     columns CSV  one row per column: the data dictionary, opens in Excel
     tables CSV   one row per table: an inventory with row counts
     rels CSV     one row per foreign key, declared and inferred kept apart

   ── Why the caller passes geometry rather than this file computing it ─────
   Every box position, every drawn row and every edge anchor is already decided
   by schema_explorer.html — smAnchor knows that an edge meets the row for its
   column when that row is drawn and the box's centre when it is not, and
   smShownCols knows which nine of forty columns are above the fold. Working
   any of that out a second time here is how the exported diagram quietly stops
   matching the one on screen. So the page hands over a POSITIONED model and
   this file only draws it.

   ── Why the scope is the diagram, not the database ───────────────────────
   The report that prompted this was taken on a database with 12,216 tables
   hidden by filters and 28 on the canvas. Exporting the database would mean
   thousands of schema-columns round trips for a file nobody asked for. The
   export is what you can see — which is also what "export this diagram" means
   — and every file says so in its own header rather than leaving the reader to
   assume it is complete.

   ── Why CSV values are neutralised ───────────────────────────────────────
   A CSV cell beginning = + - @ or a control character is executed as a formula
   when the file is opened in Excel, and these cells are database identifiers
   that this product did not choose. Quoting does not stop it; the value has to
   be prefixed. See csvCell.

   Node-requirable: everything here is a pure function of the model, so the
   formats are tested without a browser. The page owns the DOM, the downloads
   and the print call.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixSchemaExport) root.CygenixSchemaExport = api;
})(typeof window !== 'undefined' ? window : this, function () {
'use strict';

/* ── CSV ─────────────────────────────────────────────────────────────────── */

/* Excel treats a leading = + - @ as the start of a formula, and a leading tab
   or carriage return as one too. A table called "=cmd" is unlikely; a column
   carrying a user's own text in a comment field is not. Prefixing with a
   single quote is the conventional neutraliser: Excel shows the value and
   evaluates nothing, and every other CSV reader sees one extra character it
   can strip. Doing it inside the quoted form, so the apostrophe is data. */
var RISKY = /^[=+\-@\t\r]/;

function csvCell(v) {
  var s = String(v == null ? '' : v);
  if (RISKY.test(s)) s = "'" + s;
  return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}

function csvRow(cells) { return cells.map(csvCell).join(','); }

/* Excel on Windows reads a BOM-less UTF-8 CSV as the system code page, which
   turns any non-ASCII identifier into mojibake. The BOM costs three bytes and
   every other reader skips it. */
var BOM = '﻿';

/* Provenance, on every file. A schema export with no date and no database name
   is indistinguishable from a stale one somebody mailed round last quarter,
   and the scope line is what stops "28 tables" being read as the whole
   estate. Comment rows, so a spreadsheet still parses the file. */
function csvHeader(model, what) {
  var m = model || {};
  var lines = [
    '# Cygenix Schema Explorer — ' + what,
    '# database,' + csvCell(m.database || '(unknown)'),
    '# side,' + csvCell(m.side === 'src' ? 'source' : 'target'),
    '# generated,' + csvCell(isoStamp(m.generatedAt)),
    '# scope,' + csvCell(scopeSentence(m)),
  ];
  return lines.join('\n') + '\n';
}

/* The one sentence that keeps every export honest about what is in it. */
function scopeSentence(model) {
  var m = model || {};
  var n = (m.tables || []).length;
  var s = fmtInt(n) + (n === 1 ? ' table' : ' tables') + ' on the diagram';
  var hidden = Number(m.hiddenCount) || 0;
  if (hidden > 0) s += '; ' + fmtInt(hidden) + ' hidden by filters and not included';
  if (m.focusDepth) s += '; focused to ' + m.focusDepth + ' hop' + (m.focusDepth === 1 ? '' : 's');
  return s;
}

function isoStamp(ms) {
  var d = ms ? new Date(ms) : new Date();
  return isNaN(d.getTime()) ? '' : d.toISOString().replace(/\.\d+Z$/, 'Z');
}

/* ── Where a column's foreign key points ─────────────────────────────────── */
/* Built once per export rather than scanned per column: a 300-table diagram
   with 900 edges would otherwise be a quarter of a million comparisons. */
function fkIndex(edges) {
  var ix = {};
  for (var i = 0; i < (edges || []).length; i++) {
    var e = edges[i];
    var k = e.from + '|' + e.fromColumn;
    if (!ix[k]) ix[k] = [];
    ix[k].push((e.inferred ? '~' : '') + e.to + '(' + e.toColumn + ')');
  }
  return ix;
}

/* ── Columns: the data dictionary ────────────────────────────────────────── */
/* One row per column. A table whose columns never loaded still gets a row, so
   the reader can see it was on the diagram and why its columns are missing —
   an absent table reads as "there is nothing here", which would be a lie. */
function columnsCsv(model) {
  var m = model || {};
  var ix = fkIndex(m.edges);
  var out = [csvRow(['schema', 'table', 'kind', 'row_count', 'ordinal', 'column',
    'type', 'nullable', 'is_primary_key', 'is_foreign_key', 'references', 'default'])];
  var tables = (m.tables || []).slice().sort(byName);
  for (var i = 0; i < tables.length; i++) {
    var t = tables[i];
    var cols = t.columns;
    if (!cols || !cols.length) {
      out.push(csvRow([t.schema, t.name, t.kind || 'table', t.rowCount, '', '',
        '', '', '', '', '', t.columnsError ? 'columns unavailable: ' + t.columnsError
          : 'columns not loaded']));
      continue;
    }
    var pks = t.primaryKeys || [];
    for (var j = 0; j < cols.length; j++) {
      var c = cols[j];
      var refs = ix[t.key + '|' + c.name];
      out.push(csvRow([t.schema, t.name, t.kind || 'table', t.rowCount,
        c.ordinal != null ? c.ordinal : (j + 1), c.name, c.type || '',
        c.nullable ? 'YES' : 'NO',
        pks.indexOf(c.name) >= 0 ? 'YES' : 'NO',
        refs ? 'YES' : 'NO',
        refs ? refs.join(' ; ') : '',
        c.default == null ? '' : c.default]));
    }
  }
  return BOM + csvHeader(m, 'columns') + out.join('\n') + '\n';
}

/* ── Tables: the inventory ───────────────────────────────────────────────── */
function tablesCsv(model) {
  var m = model || {};
  var outDeg = {}, inDeg = {};
  for (var i = 0; i < (m.edges || []).length; i++) {
    var e = m.edges[i];
    outDeg[e.from] = (outDeg[e.from] || 0) + 1;
    inDeg[e.to] = (inDeg[e.to] || 0) + 1;
  }
  var out = [csvRow(['schema', 'table', 'kind', 'row_count', 'columns',
    'primary_key', 'references_out', 'referenced_by'])];
  var tables = (m.tables || []).slice().sort(byName);
  for (var j = 0; j < tables.length; j++) {
    var t = tables[j];
    out.push(csvRow([t.schema, t.name, t.kind || 'table', t.rowCount,
      t.columns ? t.columns.length : '',
      (t.primaryKeys || []).join(' + '),
      outDeg[t.key] || 0, inDeg[t.key] || 0]));
  }
  return BOM + csvHeader(m, 'tables') + out.join('\n') + '\n';
}

/* ── Relationships ──────────────────────────────────────────────────────── */
/* Declared and inferred in one file but never in one column: an inferred edge
   is a hypothesis with a confidence, and a reader filtering on `origin` must
   be able to throw them out in one move. Same rule the diagram follows — see
   smInferMerge in schema_explorer.html for why they are never merged. */
function relationshipsCsv(model) {
  var m = model || {};
  var out = [csvRow(['origin', 'constraint', 'from_schema', 'from_table', 'from_column',
    'to_schema', 'to_table', 'to_column', 'confidence', 'orphan_rate'])];
  var edges = (m.edges || []).slice().sort(function (a, b) {
    return (a.from + a.fromColumn).localeCompare(b.from + b.fromColumn);
  });
  for (var i = 0; i < edges.length; i++) {
    var e = edges[i];
    var f = splitKey(e.from), t = splitKey(e.to);
    out.push(csvRow([e.inferred ? 'inferred' : 'declared', e.name || '',
      f[0], f[1], e.fromColumn, t[0], t[1], e.toColumn,
      e.confidence != null ? Math.round(e.confidence * 100) + '%' : '',
      e.orphanRate != null ? (e.orphanRate * 100).toFixed(2) + '%' : '']));
  }
  return BOM + csvHeader(m, 'relationships') + out.join('\n') + '\n';
}

/* ── Markdown: the readable one ─────────────────────────────────────────── */
/* For a handover document or a pull request. Pipes inside an identifier would
   break the table, so they are escaped rather than dropped. */
function dictionaryMarkdown(model) {
  var m = model || {};
  var ix = fkIndex(m.edges);
  var md = ['# ' + (m.database || 'Schema') + ' — data dictionary', '',
    '- Side: ' + (m.side === 'src' ? 'source' : 'target'),
    '- Generated: ' + isoStamp(m.generatedAt),
    '- Scope: ' + scopeSentence(m), ''];
  var tables = (m.tables || []).slice().sort(byName);
  for (var i = 0; i < tables.length; i++) {
    var t = tables[i];
    md.push('## ' + t.schema + '.' + t.name
      + (t.kind && t.kind !== 'table' ? ' (' + t.kind + ')' : ''));
    md.push('');
    md.push(fmtInt(t.rowCount) + ' rows'
      + ((t.primaryKeys || []).length ? ' · PK: ' + t.primaryKeys.join(' + ') : ''));
    md.push('');
    if (!t.columns || !t.columns.length) {
      md.push('_' + (t.columnsError ? 'Columns unavailable: ' + t.columnsError
        : 'Columns not loaded.') + '_');
      md.push('');
      continue;
    }
    md.push('| Column | Type | Null | Key | References |');
    md.push('| --- | --- | --- | --- | --- |');
    for (var j = 0; j < t.columns.length; j++) {
      var c = t.columns[j];
      var refs = ix[t.key + '|' + c.name];
      var key = (t.primaryKeys || []).indexOf(c.name) >= 0 ? 'PK' : (refs ? 'FK' : '');
      md.push('| ' + pipe(c.name) + ' | ' + pipe(c.type || '') + ' | '
        + (c.nullable ? 'yes' : 'no') + ' | ' + key + ' | '
        + (refs ? pipe(refs.join(', ')) : '') + ' |');
    }
    md.push('');
  }
  return md.join('\n');
}

function pipe(s) { return String(s == null ? '' : s).replace(/\|/g, '\\|'); }
function splitKey(k) {
  var i = String(k || '').indexOf('.');
  return i < 0 ? ['', String(k || '')] : [k.slice(0, i), k.slice(i + 1)];
}
function byName(a, b) {
  return (a.schema + '.' + a.name).localeCompare(b.schema + '.' + b.name);
}
function fmtInt(n) {
  return String(Number(n) || 0).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}
function xml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
    return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&apos;' }[c];
  });
}

/* ── SVG ────────────────────────────────────────────────────────────────── */

var PAD = 48, TITLE_H = 64;

/* The bounds of everything drawn, so nothing is cropped. Edges are included
   as well as boxes: a bezier's control points push it outside the two boxes
   it joins, and a viewBox drawn to the boxes alone clips the curves. */
function bounds(model) {
  var m = model || {}, first = true;
  var x0 = 0, y0 = 0, x1 = 0, y1 = 0;
  function eat(x, y) {
    if (first) { x0 = x1 = x; y0 = y1 = y; first = false; return; }
    if (x < x0) x0 = x; if (x > x1) x1 = x;
    if (y < y0) y0 = y; if (y > y1) y1 = y;
  }
  var g = m.geom || {};
  var boxW = g.boxW || 210;
  for (var i = 0; i < (m.tables || []).length; i++) {
    var t = m.tables[i], p = (m.pos || {})[t.key];
    if (!p) continue;
    eat(p.x, p.y);
    eat(p.x + boxW, p.y + boxHeight(t, g));
  }
  for (var j = 0; j < (m.edges || []).length; j++) {
    var e = m.edges[j];
    if (!e.a || !e.b) continue;
    eat(e.a.x, e.a.y); eat(e.b.x, e.b.y);
  }
  if (first) return { x: 0, y: 0, w: 800, h: 600 };
  return { x: x0 - PAD, y: y0 - PAD - TITLE_H, w: (x1 - x0) + PAD * 2, h: (y1 - y0) + PAD * 2 + TITLE_H };
}

function boxHeight(t, g) {
  var headH = g.headH || 25, rowH = g.rowH || 15, maxRows = g.maxRows || 9;
  var shown = (t.shownColumns || []).length;
  return headH + Math.min(shown, maxRows) * rowH + (t.moreCount ? 14 : 4);
}

/* A standalone SVG: no external stylesheet, no web font, no CSS variable.
   Those are what make a file open correctly in one place and wrong in
   another — a var(--text) resolves to nothing in Illustrator, and a missing
   IBM Plex silently reflows every label. Colours are literal and the font
   stack ends in a generic family.

   Light, whichever theme the console is in. This file is going into a
   document or onto paper, and a dark-theme export prints a black rectangle
   and an inch of toner — the theme is a preference about a screen, not about
   the artefact. */
function diagramSvg(model) {
  var m = model || {};
  var g = m.geom || {};
  var boxW = g.boxW || 210, rowH = g.rowH || 15, headH = g.headH || 25;
  var b = bounds(m);
  var parts = [];

  parts.push('<svg xmlns="http://www.w3.org/2000/svg" version="1.1"'
    + ' viewBox="' + r1(b.x) + ' ' + r1(b.y) + ' ' + r1(b.w) + ' ' + r1(b.h) + '"'
    + ' width="' + r1(b.w) + '" height="' + r1(b.h) + '"'
    + ' font-family="IBM Plex Sans, Segoe UI, Helvetica, Arial, sans-serif">');
  parts.push('<title>' + xml((m.database || 'Schema') + ' — schema diagram') + '</title>');
  parts.push('<desc>' + xml(scopeSentence(m) + '. Generated ' + isoStamp(m.generatedAt)
    + ' by Cygenix Schema Explorer.') + '</desc>');
  parts.push('<rect x="' + r1(b.x) + '" y="' + r1(b.y) + '" width="' + r1(b.w)
    + '" height="' + r1(b.h) + '" fill="#ffffff"/>');

  // The caption travels with the picture. A diagram of 28 tables out of 12,244
  // is a different claim from a diagram of a database, and the reader of a
  // printout has nothing else to tell them which they are holding.
  parts.push('<text x="' + r1(b.x + PAD) + '" y="' + r1(b.y + 30)
    + '" font-size="17" font-weight="600" fill="#12141c">'
    + xml((m.database || 'Schema') + ' · ' + (m.side === 'src' ? 'source' : 'target')) + '</text>');
  parts.push('<text x="' + r1(b.x + PAD) + '" y="' + r1(b.y + 48)
    + '" font-size="11" fill="#5b6070">'
    + xml(scopeSentence(m) + ' · ' + isoStamp(m.generatedAt)) + '</text>');

  // Edges first, so a curve passes behind the boxes rather than over a label.
  for (var i = 0; i < (m.edges || []).length; i++) {
    var e = m.edges[i];
    if (!e.a || !e.b) continue;
    var dx = Math.max(30, Math.abs(e.b.x - e.a.x) * 0.45);
    // Inferred edges keep the diagram's dashed treatment: solid would present
    // a guess as a declared constraint, which is the one thing the exported
    // file must not do once it is out of the product and in a document.
    var stroke = e.inferred ? '#a888d0' : '#9aa0af';
    var dash = e.inferred ? ' stroke-dasharray="4 3"' : '';
    parts.push('<path d="M' + r1(e.a.x) + ',' + r1(e.a.y)
      + ' C' + r1(e.a.x + dx) + ',' + r1(e.a.y) + ' ' + r1(e.b.x - dx) + ',' + r1(e.b.y)
      + ' ' + r1(e.b.x) + ',' + r1(e.b.y) + '" fill="none" stroke="' + stroke
      + '" stroke-width="1"' + dash + '/>');
    parts.push('<circle cx="' + r1(e.b.x) + '" cy="' + r1(e.b.y) + '" r="2.2" fill="' + stroke + '"/>');
  }

  for (var j = 0; j < (m.tables || []).length; j++) {
    var t = m.tables[j], p = (m.pos || {})[t.key];
    if (!p) continue;
    var h = boxHeight(t, g);
    parts.push('<g>');
    parts.push('<rect x="' + r1(p.x) + '" y="' + r1(p.y) + '" width="' + boxW + '" height="' + r1(h)
      + '" rx="5" fill="#ffffff" stroke="#dfe2ea" stroke-width="1"/>');
    parts.push('<rect x="' + r1(p.x) + '" y="' + r1(p.y) + '" width="' + boxW + '" height="' + headH
      + '" rx="5" fill="#f4f5f8"/>');
    parts.push('<rect x="' + r1(p.x) + '" y="' + r1(p.y + headH - 5) + '" width="' + boxW
      + '" height="5" fill="#f4f5f8"/>');
    parts.push('<rect x="' + r1(p.x + 7) + '" y="' + r1(p.y + headH / 2 - 3) + '" width="6" height="6"'
      + ' rx="1.5" fill="' + xml(t.colour || '#4a5bd6') + '"/>');
    parts.push('<text x="' + r1(p.x + 19) + '" y="' + r1(p.y + headH / 2 + 4)
      + '" font-size="11" font-weight="600" fill="#12141c">' + xml(t.name) + '</text>');
    parts.push('<text x="' + r1(p.x + boxW - 7) + '" y="' + r1(p.y + headH / 2 + 4)
      + '" font-size="9" fill="#767c8c" text-anchor="end">' + xml(fmtInt(t.rowCount)) + '</text>');

    var rows = t.shownColumns || [];
    for (var k = 0; k < rows.length && k < (g.maxRows || 9); k++) {
      var c = rows[k];
      var y = p.y + headH + k * rowH + rowH - 4;
      var mark = c.pk ? '●' : (c.fk ? '↗' : '');
      if (mark) parts.push('<text x="' + r1(p.x + 8) + '" y="' + r1(y) + '" font-size="8" fill="#767c8c">'
        + mark + '</text>');
      parts.push('<text x="' + r1(p.x + 19) + '" y="' + r1(y) + '" font-size="9.5" fill="'
        + (c.fk ? '#4a5bd6' : '#191c24') + '">' + xml(c.name) + '</text>');
      if (c.type) parts.push('<text x="' + r1(p.x + boxW - 7) + '" y="' + r1(y)
        + '" font-size="8.5" fill="#9aa0af" text-anchor="end">' + xml(c.type) + '</text>');
    }
    if (t.moreCount) {
      parts.push('<text x="' + r1(p.x + 19) + '" y="' + r1(p.y + h - 4)
        + '" font-size="8.5" fill="#9aa0af">+' + t.moreCount + ' more</text>');
    }
    parts.push('</g>');
  }

  parts.push('</svg>');
  return parts.join('\n');
}

function r1(n) { return Math.round((Number(n) || 0) * 10) / 10; }

/* ── File names ─────────────────────────────────────────────────────────── */
/* Named for the database and dated, because these files end up in an email
   thread and "export.csv" is unidentifiable a week later. Anything that is
   not a safe filename character becomes a dash — a database can legally be
   called `a/b`, and a download named that silently fails on some browsers. */
function fileName(model, what, ext) {
  var m = model || {};
  var db = String(m.database || 'schema').replace(/[^A-Za-z0-9._-]+/g, '-').replace(/^-+|-+$/g, '');
  var d = m.generatedAt ? new Date(m.generatedAt) : new Date();
  var stamp = isNaN(d.getTime()) ? '' : d.toISOString().slice(0, 10);
  return ['cygenix', db || 'schema', what, stamp].filter(Boolean).join('_') + '.' + ext;
}

return {
  columnsCsv: columnsCsv,
  tablesCsv: tablesCsv,
  relationshipsCsv: relationshipsCsv,
  dictionaryMarkdown: dictionaryMarkdown,
  diagramSvg: diagramSvg,
  fileName: fileName,
  scopeSentence: scopeSentence,
  /* pure helpers, exported for the tests */
  __core: { csvCell: csvCell, bounds: bounds, fkIndex: fkIndex, isoStamp: isoStamp },
};
});
