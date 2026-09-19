/* ============================================================================
   cygenix-collation-rules.js — the collation rules, as pure functions
   ----------------------------------------------------------------------------
   THIS FILE SHIPS TWICE. The same bytes live at
       public/cygenix-collation-rules.js        (the browser)
       azure-function/src/collation-rules.js    (the Function App)
   and tests/collation-rules.test.js fails the build if they drift. Edit one,
   copy it over the other. Two copies rather than one shared path because the
   Function App is zipped and deployed from azure-function/ alone and cannot
   reach public/.

   WHY THE RULES LIVE APART FROM THE CARD

   The Collation card (public/cygenix-collation.js) is a screen. These are the
   decisions: which collation a column should be written with, where a piece
   of SQL puts two collations on opposite sides of a comparison, and whether a
   job may run at all. The Task Agent asks the last two in Azure, with no DOM
   and no localStorage, from the profile record in Cosmos. A rule that lived
   in the card would have to be reimplemented there, and two implementations
   of "is this safe to run" is one more than a person can trust.

   WHAT findClashes ACTUALLY SEES, AND WHAT IT DOES NOT

   It is a scanner, not a SQL parser, and it is written to be quiet rather
   than clever. It blanks comments and string literals first (preserving every
   offset, so line numbers stay true), collects table aliases from FROM, JOIN,
   UPDATE and INTO, then resolves each column reference it meets to a side and
   a collation. A reference it cannot resolve confidently — an unqualified
   name that exists on both sides with different collations, a variable, a
   function call — is DROPPED rather than guessed at. The brief's last
   verification line is "matching collations produce no false warnings
   anywhere", and a scanner that guesses fails that line on the first
   real-world query.

   So it finds, reliably:
     · a comparison or join predicate whose two sides resolve to different
       collations — the one that raises "Cannot resolve the collation
       conflict" at runtime;
     · an IN list or subquery compared against a column of another collation;
     · a UNION whose branches disagree column by column;
     · a CASE whose THEN and ELSE arms return different collations;
     · GROUP BY, DISTINCT and ORDER BY over a collation that is not the
       resolved one, but only in a statement that already crosses sides;
     · a temp table or table variable declaring text columns with no COLLATE,
       but only when tempdb actually disagrees with the resolved collation.

   It does not understand CTEs that rename columns, dynamic SQL built at run
   time, or a user-defined function's return collation. Those are reported by
   nothing, which is better than reported wrongly.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixCollationRules) root.CygenixCollationRules = api;
})(typeof window !== 'undefined' ? window : this, function () {
  'use strict';

  var VERSION = 1;

  /* ── Collation names ──────────────────────────────────────────────────── */

  var FLAG_TOKENS = { CS: 1, CI: 1, AS: 1, AI: 1, KS: 1, WS: 1, SC: 1, VSS: 1, BIN: 1, BIN2: 1, UTF8: 1 };

  /* SQL Server encodes the rules in the name:
       Latin1_General_CI_AS               case-insensitive, accent-sensitive
       Latin1_General_100_CS_AS_SC        case-sensitive, version 100
       Latin1_General_BIN2                binary, code-point order
       Latin1_General_100_CI_AS_SC_UTF8   UTF-8 encoded
       SQL_Latin1_General_CP1_CI_AS       the legacy SQL_ family
     BIN and BIN2 carry no _CS_/_AS_ tokens because they are both by
     definition, which is why they are tested first. */
  function parseCollation(name) {
    var n = String(name == null ? '' : name).trim();
    if (!n) return { name: '', known: false, cs: null, accent: null, bin: false, utf8: false,
                     legacy: false, lineage: '', language: '', version: '', family: '' };
    var upper = n.toUpperCase();
    var bin = /_BIN2?(_|$)/.test(upper);
    var legacy = /^SQL_/.test(upper);
    /* Token by token rather than one global replace: a global replace that
       rewrites `_CI_` to `_` moves past the `_AS` that followed it, so
       Latin1_General_CI_AS came out as LATIN1_GENERAL_AS and two collations
       that share a language looked like different languages. */
    var parts = upper.split('_').filter(Boolean);
    var language = [], version = '';
    parts.forEach(function (tok, i) {
      if (i === 0 && tok === 'SQL') return;                 // lineage, held separately
      if (FLAG_TOKENS[tok]) return;                         // a sensitivity flag
      if (/^\d+$/.test(tok)) { version = tok; return; }     // the collation version
      language.push(tok);
    });
    return {
      name: n, known: true, bin: bin,
      cs: bin ? true : (/_CS(_|$)/.test(upper) ? true : (/_CI(_|$)/.test(upper) ? false : null)),
      accent: bin ? true : (/_AS(_|$)/.test(upper) ? true : (/_AI(_|$)/.test(upper) ? false : null)),
      utf8: /_UTF8(_|$)/.test(upper),
      legacy: legacy,
      lineage: legacy ? 'SQL' : 'WINDOWS',
      language: language.join('_'),
      version: version,
      family: (legacy ? 'SQL_' : '') + language.join('_') + (version ? '_' + version : ''),
    };
  }

  /* How two collations differ, in the vocabulary the severity rules use.
     `accent` and `version` are the soft pair: same language, same lineage,
     same case rule, same encoding, and the values still compare equal for
     everything but ordering and diacritics. */
  var SOFT_DIFFS = { accent: 1, version: 1 };
  function collationDiff(a, b) {
    var pa = parseCollation(a), pb = parseCollation(b);
    var out = [];
    if (!pa.known || !pb.known) return out;
    if (pa.name === pb.name) return out;
    if (pa.cs !== pb.cs) out.push('case');
    if (pa.utf8 !== pb.utf8) out.push('encoding');
    if (pa.language !== pb.language) out.push('language');
    if (pa.lineage !== pb.lineage) out.push('lineage');
    if (pa.bin !== pb.bin) out.push('binary');
    if (pa.version !== pb.version) out.push('version');
    if (pa.accent !== pb.accent) out.push('accent');
    if (!out.length) out.push('version');   // the names differ for a reason we did not name
    return out;
  }
  function isSoftDiff(diff) {
    return diff.length > 0 && diff.every(function (d) { return !!SOFT_DIFFS[d]; });
  }
  /* Two collations clash when SQL Server would refuse to compare them, which
     is whenever the names differ at all. Both unknown is not a clash: we
     simply do not know, and saying so beats inventing a difference. */
  function collationsClash(a, b) {
    var pa = parseCollation(a), pb = parseCollation(b);
    if (!pa.known || !pb.known) return false;
    return pa.name !== pb.name;
  }

  function isNonUnicodeText(dataType) { return /^(char|varchar|text)$/i.test(String(dataType || '').trim()); }
  function isTextType(dataType) { return /^(n?char|n?varchar|n?text|sysname)$/i.test(String(dataType || '').trim()); }

  /* ── The saved settings ───────────────────────────────────────────────── */

  var ISSUE = { CODEPAGE: 'codepage', CASE_UNIQUE: 'case_unique', MISMATCH: 'mismatch',
                TEMPDB: 'tempdb', UTF8: 'utf8', ACCENT: 'accent' };

  function sideDefaults() {
    return { server: '', database: '', serverCollation: '', dbCollation: '', tempdbCollation: '',
             codePage: null, cs: null, as: null, utf8: false, productVersion: '', detectedAt: null };
  }
  function defaults() {
    return {
      version: VERSION,
      source: sideDefaults(), target: sideDefaults(),
      fingerprint: { source: '', target: '' },
      strategy: 'target', explicitCollation: null, resolvedCollation: '',
      tempTables: 'resolved', generatedSqlMode: 'apply', userSqlMode: 'offer_fix',
      caseRule: 'warn', codePageRule: 'warn',
      columnOverrides: {}, acknowledged: [], lastScan: null,
    };
  }
  /* A stored object from an older build, or a hand-edited one, is filled in
     rather than thrown on — this runs on page load and inside a job run. */
  function normalise(saved) {
    var d = defaults();
    if (!saved || typeof saved !== 'object') return d;
    var pickSide = function (s) {
      var out = sideDefaults();
      if (s && typeof s === 'object') {
        Object.keys(out).forEach(function (k) { if (s[k] !== undefined) out[k] = s[k]; });
      }
      return out;
    };
    var one = function (v, allowed, fallback) { return allowed.indexOf(v) >= 0 ? v : fallback; };
    return {
      version: VERSION,
      source: pickSide(saved.source), target: pickSide(saved.target),
      fingerprint: {
        source: String((saved.fingerprint && saved.fingerprint.source) || ''),
        target: String((saved.fingerprint && saved.fingerprint.target) || ''),
      },
      strategy: one(saved.strategy, ['target', 'source', 'explicit'], 'target'),
      explicitCollation: saved.explicitCollation || null,
      resolvedCollation: String(saved.resolvedCollation || ''),
      tempTables: one(saved.tempTables, ['resolved', 'database_default'], 'resolved'),
      generatedSqlMode: one(saved.generatedSqlMode, ['apply', 'warn'], 'apply'),
      userSqlMode: one(saved.userSqlMode, ['offer_fix', 'warn'], 'offer_fix'),
      caseRule: one(saved.caseRule, ['warn', 'block'], 'warn'),
      codePageRule: one(saved.codePageRule, ['warn', 'block'], 'warn'),
      columnOverrides: (saved.columnOverrides && typeof saved.columnOverrides === 'object'
        && !Array.isArray(saved.columnOverrides)) ? saved.columnOverrides : {},
      acknowledged: Array.isArray(saved.acknowledged) ? saved.acknowledged.slice() : [],
      lastScan: saved.lastScan || null,
    };
  }

  /* The collation the strategy chooses, before any per-column override. */
  function resolvedCollation(model) {
    if (!model) return '';
    if (model.strategy === 'explicit') return String(model.explicitCollation || '').trim();
    if (model.strategy === 'source') return (model.source && model.source.dbCollation) || '';
    return (model.target && model.target.dbCollation) || '';
  }

  /* resolve — the collation to WRITE for one column: override, then the
     strategy, then whatever was last resolved. Overrides are keyed
     "schema.table.column" and matched without regard to case, because a
     person typing an override into a box will not match the catalogue's
     capitalisation and should not have to. */
  function resolveWith(model, side, schema, table, column) {
    var m = model || defaults();
    var key = [schema, table, column].filter(function (x) { return x != null && x !== ''; }).join('.');
    var overrides = m.columnOverrides || {};
    var hit = overrides[key];
    if (!hit) {
      var lower = String(key).toLowerCase();
      var names = Object.keys(overrides);
      for (var i = 0; i < names.length; i++) {
        if (String(names[i]).toLowerCase() === lower) { hit = overrides[names[i]]; break; }
      }
    }
    if (hit && hit.collation) return String(hit.collation).trim();
    return resolvedCollation(m) || String(m.resolvedCollation || '');
  }

  /* gate — may a job run? Blocking is opt-in per rule, so the default answer
     is yes with warnings attached. A finding the operator has acknowledged
     is their decision and stops counting; that is what acknowledging means. */
  function gateWith(model) {
    var m = normalise(model);
    var ack = {};
    (m.acknowledged || []).forEach(function (k) { ack[k] = true; });
    var findings = (m.lastScan && Array.isArray(m.lastScan.findings)) ? m.lastScan.findings : [];
    var reasons = [], warnings = [];
    findings.forEach(function (f) {
      if (!f || f.severity !== 'high' || ack[f.id]) return;
      var rule = f.issueCode === ISSUE.CASE_UNIQUE ? m.caseRule
               : f.issueCode === ISSUE.CODEPAGE ? m.codePageRule : null;
      if (!rule) return;
      var line = (f.object || 'a mapped column') + ': ' + (f.issue || 'collation risk');
      if (rule === 'block') reasons.push(line); else warnings.push(line);
    });
    return { ok: reasons.length === 0, reasons: reasons, warnings: warnings,
             checked: !!m.lastScan, resolvedCollation: resolvedCollation(m) };
  }

  /* ════════════════════════════════════════════════════════════════════════
     findClashes
     ════════════════════════════════════════════════════════════════════════ */

  /* Blank comments and string literals, keeping every character position and
     every newline, so an offset in the cleaned text is the same offset in the
     original and line numbers stay true. Without this, an apostrophe in a
     comment swallows the rest of the file. */
  function blankNoise(sql) {
    var s = String(sql == null ? '' : sql);
    var out = s.split('');
    var i = 0, n = s.length;
    var blank = function (from, to) {
      for (var k = from; k < to && k < n; k++) if (out[k] !== '\n' && out[k] !== '\r') out[k] = ' ';
    };
    while (i < n) {
      var c = s[i], c2 = s[i + 1];
      if (c === '-' && c2 === '-') {
        var eol = s.indexOf('\n', i); if (eol === -1) eol = n;
        blank(i, eol); i = eol; continue;
      }
      if (c === '/' && c2 === '*') {
        var end = s.indexOf('*/', i + 2); end = end === -1 ? n : end + 2;
        blank(i, end); i = end; continue;
      }
      if (c === "'") {
        var j = i + 1;
        while (j < n) {
          if (s[j] === "'") { if (s[j + 1] === "'") { j += 2; continue; } j++; break; }
          j++;
        }
        blank(i, j); i = j; continue;
      }
      i++;
    }
    return out.join('');
  }

  function lineIndex(text) {
    var starts = [0];
    for (var i = 0; i < text.length; i++) if (text[i] === '\n') starts.push(i + 1);
    return starts;
  }
  function lineAt(starts, offset) {
    var lo = 0, hi = starts.length - 1;
    while (lo < hi) { var mid = (lo + hi + 1) >> 1; if (starts[mid] <= offset) lo = mid; else hi = mid - 1; }
    return lo + 1;
  }

  var IDENT = '(?:\\[[^\\]]+\\]|"[^"]+"|[A-Za-z_@#][\\w@#$]*)';
  var REF = '(' + IDENT + '(?:\\s*\\.\\s*' + IDENT + ')*)';
  /* Words that can follow a table reference and are NOT an alias. Without
     this list, "FROM dbo.Ledger WHERE" registers WHERE as the alias and every
     later reference to it resolves to the wrong table. */
  var NOT_ALIAS = ('on where inner left right full cross join group order union having set values output '
    + 'with option except intersect for pivot unpivot apply outer into select insert update delete as '
    + 'and or not exists between like in is null when then else end from by asc desc top distinct '
    + 'cross_apply go begin commit rollback declare').split(' ');
  function isAliasWord(w) { return NOT_ALIAS.indexOf(String(w || '').toLowerCase()) === -1; }

  function unquote(part) {
    var p = String(part || '').trim();
    if (/^\[.*\]$/.test(p)) return p.slice(1, -1);
    if (/^".*"$/.test(p)) return p.slice(1, -1);
    return p;
  }
  function refParts(ref) {
    return String(ref || '').split('.').map(function (x) { return unquote(x.trim()); }).filter(function (x) { return x !== ''; });
  }
  function isTempName(name) { return /^[#@]/.test(String(name || '')); }

  /* The column catalogue, indexed every way a reference might name it. */
  function buildIndex(context) {
    var cols = (context && context.columns) || [];
    var byQualified = {}, byTable = {}, byColumn = {}, tableSide = {}, tableKnown = {};
    cols.forEach(function (c) {
      if (!c || !c.column) return;
      var schema = String(c.schema || ''), table = String(c.table || ''), column = String(c.column);
      var entry = { side: c.side || '', schema: schema, table: table, column: column,
                    collation: c.collation || '', dataType: c.dataType || '',
                    inUniqueKey: !!c.inUniqueKey, maxLength: c.maxLength, codePage: c.codePage };
      if (schema && table) byQualified[(schema + '.' + table + '.' + column).toLowerCase()] = entry;
      if (table) {
        var tk = (table + '.' + column).toLowerCase();
        if (!byTable[tk]) byTable[tk] = [];
        byTable[tk].push(entry);
        tableSide[table.toLowerCase()] = entry.side;
        tableKnown[table.toLowerCase()] = 1;
        if (schema) {
          tableSide[(schema + '.' + table).toLowerCase()] = entry.side;
          tableKnown[(schema + '.' + table).toLowerCase()] = 1;
        }
      }
      var ck = column.toLowerCase();
      if (!byColumn[ck]) byColumn[ck] = [];
      byColumn[ck].push(entry);
    });
    (context && context.tables || []).forEach(function (t) {
      if (!t || !t.name) return;
      tableSide[String(t.name).toLowerCase()] = t.side || '';
      if (t.schema) tableSide[(t.schema + '.' + t.name).toLowerCase()] = t.side || '';
    });
    return { byQualified: byQualified, byTable: byTable, byColumn: byColumn,
             tableSide: tableSide, tableKnown: tableKnown };
  }

  /* Which side a table reference belongs to. A three- or four-part name that
     names a database is the strongest signal there is, because that is
     exactly the shape the cross-database rewriters produce. */
  function sideOfTableRef(ref, context, index) {
    var parts = refParts(ref);
    if (!parts.length) return '';
    var last = parts[parts.length - 1];
    if (isTempName(last) || isTempName(parts[0])) return 'temp';
    var m = (context && context.model) || defaults();
    if (parts.length >= 3) {
      var db = parts[parts.length - 3].toLowerCase();
      if (db && m.source && String(m.source.database || '').toLowerCase() === db) return 'src';
      if (db && m.target && String(m.target.database || '').toLowerCase() === db) return 'tgt';
    }
    var qualified = parts.length >= 2
      ? (parts[parts.length - 2] + '.' + last).toLowerCase() : '';
    if (qualified && index.tableSide[qualified]) return index.tableSide[qualified];
    if (index.tableSide[last.toLowerCase()]) return index.tableSide[last.toLowerCase()];
    return '';
  }

  /* Aliases, from every clause that can introduce one. */
  function collectAliases(clean, context, index) {
    var aliases = {};
    var re = new RegExp('\\b(from|join|into|update|apply)\\s+' + REF + '(?:\\s+(?:as\\s+)?(' + IDENT + '))?', 'gi');
    var m;
    while ((m = re.exec(clean))) {
      var ref = m[2], alias = m[3] ? unquote(m[3]) : '';
      var side = sideOfTableRef(ref, context, index);
      var parts = refParts(ref);
      var tableName = parts.length ? parts[parts.length - 1] : '';
      var rec = { ref: ref, table: tableName, schema: parts.length >= 2 ? parts[parts.length - 2] : '', side: side };
      if (alias && isAliasWord(alias)) aliases[alias.toLowerCase()] = rec;
      if (tableName) aliases[tableName.toLowerCase()] = aliases[tableName.toLowerCase()] || rec;
    }
    return aliases;
  }

  /* One column reference → what it is, or null when we are not sure.
     Not-sure is a deliberate answer: see the header. */
  function resolveRef(text, ctx) {
    var parts = refParts(text);
    if (!parts.length) return null;
    var column = parts[parts.length - 1];
    if (isTempName(column)) return null;                     // a variable, not a column
    var qualifier = parts.length >= 2 ? parts[parts.length - 2] : '';
    var index = ctx.index, aliases = ctx.aliases;
    var m = ctx.context.model || defaults();

    var finish = function (entry, side, label) {
      var coll = entry && entry.collation
        ? entry.collation
        : (side === 'src' ? (m.source && m.source.dbCollation) : side === 'tgt' ? (m.target && m.target.dbCollation) : '');
      if (!coll) return null;
      if (entry && entry.dataType && !isTextType(entry.dataType)) return null;   // not a text column
      return { side: side || (entry && entry.side) || '', collation: coll,
               dataType: entry ? entry.dataType : '', schema: entry ? entry.schema : '',
               table: entry ? entry.table : (label || ''), column: column,
               inUniqueKey: entry ? entry.inUniqueKey : false,
               label: (label ? label + '.' : '') + column };
    };

    if (qualifier) {
      var a = aliases[qualifier.toLowerCase()];
      var table = a ? a.table : qualifier;
      var schema = a ? a.schema : (parts.length >= 3 ? parts[parts.length - 3] : '');
      var side = a ? a.side : (index.tableSide[qualifier.toLowerCase()] || '');
      if (a && a.side === 'temp') return null;               // a temp table has no side of its own
      var q = schema ? index.byQualified[(schema + '.' + table + '.' + column).toLowerCase()] : null;
      if (q) return finish(q, q.side || side, qualifier);
      var t = index.byTable[(table + '.' + column).toLowerCase()];
      if (t && t.length === 1) return finish(t[0], t[0].side || side, qualifier);
      if (t && t.length > 1) {
        var narrowed = t.filter(function (e) { return e.side === side; });
        if (narrowed.length === 1) return finish(narrowed[0], side, qualifier);
        return null;                                          // genuinely ambiguous
      }
      /* The column is not in the catalogue. When we hold the catalogue for
         that table, that means the column is not a text column — or is not
         there at all — and either way there is nothing to say. Falling back
         to the database collation here is what made `a.Nope = b.AlsoNope`
         report a clash between two columns that do not exist. The fallback
         is kept only for a table we know nothing about, which is the case
         before anyone has run a scan. */
      var known = index.tableKnown[String(table).toLowerCase()]
        || (schema && index.tableKnown[(schema + '.' + table).toLowerCase()]);
      if (known) return null;
      if (side) return finish(null, side, qualifier);
      return null;
    }

    /* Unqualified. Safe only when every column of that name agrees. */
    var hits = index.byColumn[column.toLowerCase()] || [];
    if (!hits.length) return null;
    var collations = {};
    hits.forEach(function (h) { if (h.collation) collations[h.collation] = 1; });
    if (Object.keys(collations).length !== 1) return null;    // ambiguous: say nothing
    return finish(hits[0], hits[0].side, '');
  }

  function clash(kind, severity, line, expression, left, right, model, note) {
    var fixCollation = resolvedCollation(model) || '';
    /* Fix the side that is NOT already the resolved collation. When neither
       is, both get it, which is what a generator has to emit anyway. */
    var fixSide = '';
    if (left && right) {
      if (fixCollation && left.collation === fixCollation) fixSide = 'right';
      else if (fixCollation && right.collation === fixCollation) fixSide = 'left';
      else fixSide = 'both';
    }
    var target = fixSide === 'left' ? left : fixSide === 'right' ? right : null;
    return {
      kind: kind, severity: severity, line: line,
      expression: String(expression || '').replace(/\s+/g, ' ').trim(),
      left: left ? left.label : '', right: right ? right.label : '',
      leftCollation: left ? left.collation : '', rightCollation: right ? right.collation : '',
      fixCollation: fixCollation, fixSide: fixSide,
      fix: fixCollation
        ? ('COLLATE ' + fixCollation + (target ? ' on ' + target.label : fixSide === 'both' ? ' on both sides' : ''))
        : 'Detect the collations first — there is no resolved collation to apply.',
      note: note || '',
    };
  }

  /* Split on a keyword at parenthesis depth zero. Used for UNION branches and
     for select lists, where a comma inside a function call is not a
     separator. */
  function splitTopLevel(text, re) {
    var parts = [], depth = 0, last = 0, i = 0;
    while (i < text.length) {
      var ch = text[i];
      if (ch === '(') depth++;
      else if (ch === ')') depth--;
      else if (depth === 0) {
        re.lastIndex = i;
        var m = re.exec(text);
        if (m && m.index === i) {
          parts.push({ text: text.slice(last, i), start: last });
          i = m.index + m[0].length; last = i; continue;
        }
      }
      i++;
    }
    parts.push({ text: text.slice(last), start: last });
    return parts;
  }

  function findClashes(sql, context) {
    var ctxIn = context || {};
    var model = normalise(ctxIn.model);
    var raw = String(sql == null ? '' : sql);
    if (!raw.trim()) return [];
    var clean = blankNoise(raw);
    var starts = lineIndex(raw);
    var index = buildIndex(ctxIn);
    var aliases = collectAliases(clean, ctxIn, index);
    var ctx = { context: { model: model, columns: ctxIn.columns, tables: ctxIn.tables }, index: index, aliases: aliases };
    var out = [];
    var seen = {};
    var add = function (c) {
      var k = c.kind + '|' + c.line + '|' + c.expression;
      if (seen[k]) return;
      seen[k] = 1; out.push(c);
    };
    var lineOf = function (off) { return lineAt(starts, off); };

    /* 1. Comparisons and join predicates — the clash that actually errors. */
    var cmp = new RegExp(REF + '\\s*(=|<>|!=|<=|>=|<|>|\\bNOT\\s+LIKE\\b|\\bLIKE\\b)\\s*' + REF, 'gi');
    var m;
    while ((m = cmp.exec(clean))) {
      var l = resolveRef(m[1], ctx), r = resolveRef(m[3], ctx);
      if (!l || !r) continue;
      if (!collationsClash(l.collation, r.collation)) continue;
      var kind = /\bon\b[^()]*$/i.test(clean.slice(Math.max(0, m.index - 120), m.index)) ? 'join' : 'comparison';
      add(clash(kind, 'high', lineOf(m.index), raw.substr(m.index, m[0].length), l, r, model,
        'SQL Server raises "Cannot resolve the collation conflict" when this runs.'));
    }

    /* 2. IN lists and IN (SELECT …). */
    var inRe = new RegExp(REF + '\\s+(?:NOT\\s+)?IN\\s*\\(', 'gi');
    while ((m = inRe.exec(clean))) {
      var left = resolveRef(m[1], ctx);
      if (!left) continue;
      var open = m.index + m[0].length - 1, depth = 0, end = open;
      for (var k = open; k < clean.length; k++) {
        if (clean[k] === '(') depth++;
        else if (clean[k] === ')') { depth--; if (!depth) { end = k; break; } }
      }
      var inner = clean.slice(open + 1, end);
      var innerRefs = inner.match(new RegExp(REF, 'g')) || [];
      for (var j = 0; j < innerRefs.length; j++) {
        var rr = resolveRef(innerRefs[j], ctx);
        if (!rr) continue;
        if (!collationsClash(left.collation, rr.collation)) continue;
        add(clash('in', 'high', lineOf(m.index), raw.substr(m.index, Math.min(160, end - m.index + 1)), left, rr, model,
          'An IN list compares the two collations directly.'));
        break;
      }
    }

    /* 3. UNION — the branches have to agree column by column. */
    if (/\bUNION\b/i.test(clean)) {
      var branches = splitTopLevel(clean, /\bUNION(\s+ALL)?\b/gi);
      if (branches.length > 1) {
        var listOf = function (branch) {
          var sel = /\bSELECT\b(?:\s+DISTINCT\b|\s+ALL\b)?(?:\s+TOP\s*\([^)]*\)|\s+TOP\s+\d+)?/i.exec(branch.text);
          if (!sel) return [];
          var from = /\bFROM\b/i.exec(branch.text.slice(sel.index + sel[0].length));
          var listText = branch.text.slice(sel.index + sel[0].length,
            from ? sel.index + sel[0].length + from.index : branch.text.length);
          return splitTopLevel(listText, /,/g).map(function (item) {
            var refs = item.text.match(new RegExp(REF, 'g')) || [];
            for (var q = 0; q < refs.length; q++) { var rv = resolveRef(refs[q], ctx); if (rv) return rv; }
            return null;
          });
        };
        var first = listOf(branches[0]);
        for (var b = 1; b < branches.length; b++) {
          var other = listOf(branches[b]);
          for (var c2 = 0; c2 < Math.min(first.length, other.length); c2++) {
            if (!first[c2] || !other[c2]) continue;
            if (!collationsClash(first[c2].collation, other[c2].collation)) continue;
            add(clash('union', 'high', lineOf(branches[b].start),
              'UNION column ' + (c2 + 1) + ': ' + first[c2].label + ' / ' + other[c2].label,
              first[c2], other[c2], model,
              'The branches of a UNION must agree on collation column by column.'));
          }
        }
      }
    }

    /* 4. CASE — the arms have to agree on what they return. */
    var caseRe = /\bCASE\b/gi;
    while ((m = caseRe.exec(clean))) {
      var depth2 = 0, stop = clean.length;
      for (var z = m.index; z < clean.length; z++) {
        if (/\bCASE\b/i.test(clean.substr(z, 4)) && z !== m.index) depth2++;
        if (/\bEND\b/i.test(clean.substr(z, 3))) { if (!depth2) { stop = z + 3; break; } depth2--; }
      }
      var body = clean.slice(m.index, stop);
      var arms = [];
      var armRe = new RegExp('\\b(THEN|ELSE)\\b\\s*' + REF, 'gi');
      var am;
      while ((am = armRe.exec(body))) { var av = resolveRef(am[2], ctx); if (av) arms.push(av); }
      for (var x = 1; x < arms.length; x++) {
        if (!collationsClash(arms[0].collation, arms[x].collation)) continue;
        add(clash('case', 'medium', lineOf(m.index), raw.substr(m.index, Math.min(160, stop - m.index)),
          arms[0], arms[x], model, 'A CASE expression returns one collation; its arms disagree.'));
        break;
      }
    }

    /* 5. GROUP BY, DISTINCT and ORDER BY — only in a statement that already
          crosses sides, and only when the collation is not the resolved one.
          Grouping under a collation nobody chose changes which rows are
          considered the same row, quietly. */
    var sides = {};
    (function () {
      var all = clean.match(new RegExp(REF, 'g')) || [];
      all.forEach(function (t) { var rv = resolveRef(t, ctx); if (rv && rv.side) sides[rv.side] = 1; });
    })();
    var crossSide = sides.src && sides.tgt;
    if (crossSide) {
      var resolved = resolvedCollation(model);
      var clauseRe = /\b(GROUP\s+BY|ORDER\s+BY|SELECT\s+DISTINCT)\b/gi;
      while ((m = clauseRe.exec(clean))) {
        var tail = clean.slice(m.index + m[0].length);
        var stopAt = /\b(FROM|WHERE|HAVING|GROUP\s+BY|ORDER\s+BY|UNION|OPTION|FOR|INTO|\))/i.exec(tail);
        var listText2 = tail.slice(0, stopAt ? stopAt.index : Math.min(400, tail.length));
        var refs2 = listText2.match(new RegExp(REF, 'g')) || [];
        for (var y = 0; y < refs2.length; y++) {
          var g = resolveRef(refs2[y], ctx);
          if (!g || !resolved) continue;
          if (!collationsClash(g.collation, resolved)) continue;
          add(clash('ordering', 'low', lineOf(m.index), m[0].replace(/\s+/g, ' ') + ' ' + g.label, g,
            { label: 'the resolved collation', collation: resolved }, model,
            'Grouping or ordering under a collation other than the resolved one changes which values count as equal.'));
          break;
        }
      }
    }

    /* 6. Temp tables and table variables. Only worth saying when tempdb
          actually disagrees with the resolved collation — otherwise the
          declaration inherits exactly what was wanted. */
    var tempdb = (model.target && model.target.tempdbCollation) || '';
    var resolved2 = resolvedCollation(model);
    if (tempdb && resolved2 && collationsClash(tempdb, resolved2)) {
      var wanted = model.tempTables === 'database_default' ? 'DATABASE_DEFAULT' : resolved2;
      var ddlRe = new RegExp('\\b(?:CREATE\\s+TABLE\\s+(#{1,2}' + '[\\w@#$]+)|DECLARE\\s+(@[\\w@#$]+)\\s+TABLE)\\s*\\(', 'gi');
      while ((m = ddlRe.exec(clean))) {
        var open2 = m.index + m[0].length - 1, d3 = 0, end2 = clean.length;
        for (var w = open2; w < clean.length; w++) {
          if (clean[w] === '(') d3++;
          else if (clean[w] === ')') { d3--; if (!d3) { end2 = w; break; } }
        }
        var body2 = clean.slice(open2 + 1, end2);
        var cols2 = splitTopLevel(body2, /,/g);
        for (var v = 0; v < cols2.length; v++) {
          var def = cols2[v].text;
          if (!/\b(n?varchar|n?char|n?text|sysname)\b/i.test(def)) continue;
          if (/\bCOLLATE\b/i.test(def)) continue;
          var nameM = new RegExp('^\\s*(' + IDENT + ')').exec(def);
          var colName = nameM ? unquote(nameM[1]) : '(column)';
          add({
            kind: 'temptable', severity: 'medium', line: lineOf(open2 + 1 + cols2[v].start),
            expression: def.replace(/\s+/g, ' ').trim(),
            left: (m[1] || m[2] || '#temp') + '.' + colName, right: 'tempdb',
            leftCollation: tempdb, rightCollation: resolved2,
            fixCollation: wanted, fixSide: 'left',
            fix: 'COLLATE ' + wanted + ' on ' + (m[1] || m[2] || '#temp') + '.' + colName,
            note: 'A temp table inherits tempdb (' + tempdb + '), which is not the resolved collation.',
          });
        }
      }
    }

    out.sort(function (a, b) { return a.line - b.line; });
    return out;
  }

  /* A one-line summary for a banner. Kept here so the browser and the Function
     App word it the same way in a log and on a screen. */
  function summariseClashes(clashes) {
    var n = (clashes || []).length;
    if (!n) return 'No collation clashes found.';
    var high = clashes.filter(function (c) { return c.severity === 'high'; }).length;
    var lines = {};
    clashes.forEach(function (c) { lines[c.line] = 1; });
    return n + ' collation clash' + (n === 1 ? '' : 'es') + ' across ' + Object.keys(lines).length
      + ' line' + (Object.keys(lines).length === 1 ? '' : 's')
      + (high ? ' — ' + high + ' would fail at runtime' : '');
  }

  /* ── The shared cases. Both copies of this file are driven by the SAME
        fixtures, so "behaviourally identical" is a test rather than a
        promise. Each case is { name, sql, context, expect } where expect
        names the kinds in order. ─────────────────────────────────────────── */
  var CASE_MODEL = {
    source: { database: 'SRC', dbCollation: 'Latin1_General_CS_AS', tempdbCollation: 'Latin1_General_CS_AS', codePage: 1252 },
    target: { database: 'TGT', dbCollation: 'SQL_Latin1_General_CP1_CI_AS', tempdbCollation: 'SQL_Latin1_General_CP1_CI_AS', codePage: 1252 },
    strategy: 'target', tempTables: 'resolved',
  };
  var CASE_COLUMNS = [
    { side: 'src', schema: 'dbo', table: 'A', column: 'Code', collation: 'Latin1_General_CS_AS', dataType: 'varchar' },
    { side: 'src', schema: 'dbo', table: 'A', column: 'Name', collation: 'Latin1_General_CS_AS', dataType: 'varchar' },
    { side: 'tgt', schema: 'dbo', table: 'B', column: 'Code', collation: 'SQL_Latin1_General_CP1_CI_AS', dataType: 'varchar' },
    { side: 'tgt', schema: 'dbo', table: 'B', column: 'Name', collation: 'SQL_Latin1_General_CP1_CI_AS', dataType: 'varchar' },
    { side: 'tgt', schema: 'dbo', table: 'B', column: 'Qty', collation: '', dataType: 'int' },
  ];
  var MATCHED_MODEL = {
    source: { database: 'SRC', dbCollation: 'Latin1_General_CI_AS', tempdbCollation: 'Latin1_General_CI_AS' },
    target: { database: 'TGT', dbCollation: 'Latin1_General_CI_AS', tempdbCollation: 'Latin1_General_CI_AS' },
    strategy: 'target', tempTables: 'resolved',
  };
  var MATCHED_COLUMNS = CASE_COLUMNS.map(function (c) {
    return { side: c.side, schema: c.schema, table: c.table, column: c.column,
             collation: c.collation ? 'Latin1_General_CI_AS' : '', dataType: c.dataType };
  });
  var CTX = { model: CASE_MODEL, columns: CASE_COLUMNS };
  var MATCHED_CTX = { model: MATCHED_MODEL, columns: MATCHED_COLUMNS };

  var SHARED_CASES = [
    { name: 'a cross-collation join predicate',
      sql: 'SELECT a.Code FROM SRC.dbo.A a JOIN TGT.dbo.B b ON a.Code = b.Code',
      context: CTX, expect: ['join'] },
    { name: 'the same join with matching collations says nothing',
      sql: 'SELECT a.Code FROM SRC.dbo.A a JOIN TGT.dbo.B b ON a.Code = b.Code',
      context: MATCHED_CTX, expect: [] },
    { name: 'a WHERE comparison across sides',
      sql: 'SELECT 1 FROM SRC.dbo.A a, TGT.dbo.B b WHERE a.Name <> b.Name',
      context: CTX, expect: ['comparison'] },
    { name: 'a LIKE across sides',
      sql: 'SELECT 1 FROM SRC.dbo.A a, TGT.dbo.B b WHERE a.Name LIKE b.Name',
      context: CTX, expect: ['comparison'] },
    { name: 'a comparison against a literal is not a clash',
      sql: "SELECT 1 FROM SRC.dbo.A a WHERE a.Code = 'ABC'",
      context: CTX, expect: [] },
    { name: 'a comparison against a variable is not a clash',
      sql: 'SELECT 1 FROM SRC.dbo.A a WHERE a.Code = @code',
      context: CTX, expect: [] },
    { name: 'a non-text column is not a clash',
      sql: 'SELECT 1 FROM SRC.dbo.A a JOIN TGT.dbo.B b ON a.Code = b.Qty',
      context: CTX, expect: [] },
    { name: 'an IN subquery across sides',
      sql: 'SELECT 1 FROM SRC.dbo.A a WHERE a.Code IN (SELECT b.Code FROM TGT.dbo.B b)',
      context: CTX, expect: ['in'] },
    { name: 'a UNION whose branches disagree',
      sql: 'SELECT a.Code FROM SRC.dbo.A a UNION ALL SELECT b.Code FROM TGT.dbo.B b',
      context: CTX, expect: ['union'] },
    { name: 'a CASE whose arms disagree',
      sql: 'SELECT CASE WHEN 1=1 THEN a.Name ELSE b.Name END FROM SRC.dbo.A a, TGT.dbo.B b',
      context: CTX, expect: ['case'] },
    { name: 'GROUP BY under a collation that is not the resolved one, in a cross-side statement',
      sql: 'SELECT a.Code, COUNT(*) FROM SRC.dbo.A a JOIN TGT.dbo.B b ON a.Name = b.Name GROUP BY a.Code',
      context: CTX, expect: ['join', 'ordering'] },
    { name: 'a temp table declaring text columns with no COLLATE, when tempdb disagrees',
      sql: 'CREATE TABLE #stage (Code varchar(20), Qty int);',
      context: { model: { source: CASE_MODEL.source, target: { database: 'TGT', dbCollation: 'SQL_Latin1_General_CP1_CI_AS', tempdbCollation: 'Latin1_General_CS_AS' }, strategy: 'target', tempTables: 'resolved' }, columns: CASE_COLUMNS },
      expect: ['temptable'] },
    { name: 'the same temp table when tempdb already agrees',
      sql: 'CREATE TABLE #stage (Code varchar(20), Qty int);',
      context: CTX, expect: [] },
    { name: 'a temp table column that already carries COLLATE',
      sql: 'CREATE TABLE #stage (Code varchar(20) COLLATE DATABASE_DEFAULT, Qty int);',
      context: { model: { source: CASE_MODEL.source, target: { database: 'TGT', dbCollation: 'SQL_Latin1_General_CP1_CI_AS', tempdbCollation: 'Latin1_General_CS_AS' }, strategy: 'target', tempTables: 'resolved' }, columns: CASE_COLUMNS },
      expect: [] },
    { name: 'an operator inside a string literal is not a comparison',
      sql: "SELECT 'a.Code = b.Code' AS note FROM SRC.dbo.A a",
      context: CTX, expect: [] },
    { name: 'a clash inside a comment is not reported',
      sql: '-- a.Code = b.Code\nSELECT 1 FROM SRC.dbo.A a',
      context: CTX, expect: [] },
    { name: 'the line number counts from one and survives a preceding comment',
      sql: '-- header\n\nSELECT 1 FROM SRC.dbo.A a JOIN TGT.dbo.B b ON a.Code = b.Code',
      context: CTX, expect: ['join'], line: 3 },
    { name: 'an unqualified column that exists on both sides with different collations is not guessed at',
      sql: 'SELECT 1 FROM SRC.dbo.A, TGT.dbo.B WHERE Code = Name',
      context: CTX, expect: [] },
    { name: 'no settings at all yields no claims',
      sql: 'SELECT 1 FROM SRC.dbo.A a JOIN TGT.dbo.B b ON a.Code = b.Code',
      context: { model: null, columns: [] }, expect: [] },
    { name: 'empty SQL is not an error', sql: '', context: CTX, expect: [] },
  ];

  return {
    VERSION: VERSION, ISSUE: ISSUE,
    parseCollation: parseCollation, collationDiff: collationDiff, isSoftDiff: isSoftDiff,
    collationsClash: collationsClash, isNonUnicodeText: isNonUnicodeText, isTextType: isTextType,
    defaults: defaults, normalise: normalise, resolvedCollation: resolvedCollation,
    resolveWith: resolveWith, gateWith: gateWith,
    findClashes: findClashes, summariseClashes: summariseClashes,
    blankNoise: blankNoise, refParts: refParts,
    SHARED_CASES: SHARED_CASES,
  };
});
