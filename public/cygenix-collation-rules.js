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
      var c = clash(kind, 'high', lineOf(m.index), raw.substr(m.index, m[0].length), l, r, model,
        'SQL Server raises "Cannot resolve the collation conflict" when this runs.');
      /* Where each operand ENDS in the original text. applyFix splices the
         COLLATE clause in at one of these, so a clash carries the position
         as well as the diagnosis — without it, applying a fix would mean
         finding the expression again by searching, which goes wrong the
         moment the same comparison appears twice. */
      c.leftEnd = m.index + m[1].length;
      c.rightEnd = m.index + m[0].length;
      add(c);
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

  /* ════════════════════════════════════════════════════════════════════════
     APPLYING A FIX (Stage C)
     ────────────────────────────────────────────────────────────────────────
     Every helper below returns a SUFFIX — ' COLLATE X' or the empty string —
     so a generator can concatenate it unconditionally and read normally:

         out.push(quoteIdent(c.name) + ' ' + type + collateForTempColumn(m));

     That shape is deliberate. The brief's last verification line is that a
     migration on a profile whose collations already match must produce SQL
     byte-identical to before this feature existed. A generator that had to
     ask "do I need a COLLATE here?" would grow a branch at every site and
     get one of them wrong; a helper that returns '' when there is nothing to
     neutralise cannot.

     WHEN THERE IS SOMETHING TO NEUTRALISE

     needsWork() is the single gate: the two database collations differ, or
     tempdb differs from the resolved collation. If neither is true, adding
     COLLATE would change the text of every script to say exactly what the
     database already meant — noise in a diff, and a promise to maintain
     forever. So nothing is emitted.

     A column override is the one thing that overrides that: somebody who
     pinned a column asked for it explicitly, and gets it even when the
     databases agree.
     ════════════════════════════════════════════════════════════════════════ */

  var MARKER = '-- cyg:collation';

  function hasOverrides(model) {
    return !!(model && model.columnOverrides && Object.keys(model.columnOverrides).length);
  }
  function needsWork(model) {
    if (!model) return false;
    var src = (model.source && model.source.dbCollation) || '';
    var tgt = (model.target && model.target.dbCollation) || '';
    var resolved = resolvedCollation(model);
    var tempdb = (model.target && model.target.tempdbCollation) || '';
    if (src && tgt && collationsClash(src, tgt)) return true;
    if (tempdb && resolved && collationsClash(tempdb, resolved)) return true;
    return hasOverrides(model);
  }
  /* Is this model allowed to change SQL at all? Only in apply mode, only
     with a collation to apply, and only when there is something to fix. */
  function applies(model) {
    var m = model ? normalise(model) : null;
    if (!m) return false;
    if (m.generatedSqlMode !== 'apply') return false;
    if (!resolvedCollation(m)) return false;
    return needsWork(m);
  }

  function suffix(collation) { return collation ? (' COLLATE ' + collation) : ''; }

  /* The clause for ONE column reference in a comparison. `currentCollation`
     is what that column already carries; when it already matches what we
     would apply, nothing is emitted. */
  function collateForColumn(model, side, schema, table, column, currentCollation) {
    if (!applies(model)) return '';
    var want = resolveWith(model, side, schema, table, column);
    if (!want) return '';
    if (currentCollation && !collationsClash(currentCollation, want)) return '';
    return suffix(want);
  }

  /* A text column in a temp table or a staging table. DATABASE_DEFAULT is a
     pseudo-collation that evaluates to whatever the connection's database
     declares, which is how you neutralise a comparison without naming a
     collation; the named form pins it instead. Which one is the operator's
     choice, saved on the profile. */
  function collateForTempColumn(model, dataType) {
    if (!applies(model)) return '';
    if (dataType && !isTextType(dataType)) return '';
    var m = normalise(model);
    if (m.tempTables === 'database_default') return ' COLLATE DATABASE_DEFAULT';
    return suffix(resolvedCollation(m));
  }

  /* Both sides of a comparison at once. The side that already carries the
     resolved collation is left alone; when neither does, both are pinned,
     which is what makes the comparison legal whatever the two columns are. */
  function collateForComparison(model, left, right) {
    var none = { left: '', right: '' };
    if (!applies(model)) return none;
    var want = resolvedCollation(model);
    if (!want) return none;
    var lc = left && left.collation, rc = right && right.collation;
    var lWant = left ? resolveWith(model, left.side, left.schema, left.table, left.column) : want;
    var rWant = right ? resolveWith(model, right.side, right.schema, right.table, right.column) : want;
    var out = {
      left: (lc && !collationsClash(lc, lWant)) ? '' : suffix(lWant),
      right: (rc && !collationsClash(rc, rWant)) ? '' : suffix(rWant),
    };
    /* Two columns that already agree with each other need nothing, even if
       neither is the resolved collation — the comparison is already legal
       and rewriting it would change results for no reason. */
    if (lc && rc && !collationsClash(lc, rc) && lWant === rWant) return none;
    return out;
  }

  /* The header a generated script carries when anything was applied. */
  function headerComment(profileName, count, resolved) {
    return '-- Collation fixes applied from profile ' + (profileName || '(unnamed)')
      + ': ' + count + ' (resolved collation ' + (resolved || 'none') + ')';
  }

  /* A short, stable fingerprint of everything that can change generated SQL.
     A script stamped with a different one was built under different
     settings and should be regenerated. FNV-1a rather than a real hash: it
     runs in a browser with no crypto import, and this is a cache key, not a
     security claim. */
  function settingsStamp(model) {
    if (!model) return '';
    var m = normalise(model);
    if (!needsWork(m) && m.generatedSqlMode !== 'apply') return '';
    var keys = Object.keys(m.columnOverrides || {}).sort();
    var canonical = JSON.stringify({
      resolved: resolvedCollation(m),
      strategy: m.strategy,
      explicit: m.explicitCollation || '',
      temp: m.tempTables,
      mode: m.generatedSqlMode,
      src: (m.source && m.source.dbCollation) || '',
      tgt: (m.target && m.target.dbCollation) || '',
      tempdb: (m.target && m.target.tempdbCollation) || '',
      overrides: keys.map(function (k) { return k + '=' + (m.columnOverrides[k] || {}).collation; }),
    });
    var h = 0x811c9dc5;
    for (var i = 0; i < canonical.length; i++) {
      h ^= canonical.charCodeAt(i);
      h = (h + ((h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24))) >>> 0;
    }
    return ('0000000' + h.toString(16)).slice(-8);
  }

  /* ── Rewriting SQL somebody else wrote ────────────────────────────────────
     The one place this feature edits finished text rather than building it.
     A generator gets its COLLATE at construction; a query a person typed
     cannot, so this splices the clause in — and only ever behind an explicit
     confirmation, which is the caller's job to obtain.

     Only comparisons and join predicates are rewritten. They are the clashes
     that carry an operand position, they are the ones that actually fail at
     runtime, and they can be fixed by adding a suffix without changing what
     the query means. A UNION or a CASE would need the select list rewritten
     to stay correct, and guessing at that in somebody's query is worse than
     telling them where the problem is.

     Splices run back to front so an earlier offset is still valid after a
     later one has been applied. */
  function applyFix(sql, clashes, model) {
    var raw = String(sql == null ? '' : sql);
    var m = model ? normalise(model) : null;
    var edits = [];
    (clashes || []).forEach(function (c) {
      if (!c || (c.kind !== 'comparison' && c.kind !== 'join')) return;
      if (!c.fixCollation) return;
      var clause = ' COLLATE ' + c.fixCollation;
      if (c.fixSide === 'left' || c.fixSide === 'both') {
        if (typeof c.leftEnd === 'number') edits.push({ at: c.leftEnd, text: clause });
      }
      if (c.fixSide === 'right' || c.fixSide === 'both') {
        if (typeof c.rightEnd === 'number') edits.push({ at: c.rightEnd, text: clause });
      }
    });
    if (!edits.length) return { sql: raw, applied: 0, skipped: (clashes || []).length };
    edits.sort(function (a, b) { return b.at - a.at; });
    var out = raw;
    edits.forEach(function (e) { out = out.slice(0, e.at) + e.text + out.slice(e.at); });
    var handled = (clashes || []).filter(function (c) {
      return c && (c.kind === 'comparison' || c.kind === 'join') && c.fixCollation;
    }).length;
    return { sql: out, applied: edits.length, skipped: (clashes || []).length - handled,
             mode: m ? m.userSqlMode : null };
  }

  /* ════════════════════════════════════════════════════════════════════════
     MATCHING TWO VALUES IN JAVASCRIPT
     ────────────────────────────────────────────────────────────────────────
     Four places in Cygenix compare a source figure with a target figure
     WITHOUT a database in between: the Trial Balance and the GL balancing
     check read each side separately and merge them in a Map; a migration
     run's grouped reconciliation does the same with its group keys; and the
     evidence mapper measures how much of a source sample appears in a target
     sample. No COLLATE clause can reach any of them, because there is no
     SQL statement — the comparison is `===` in a browser.

     That matters because `===` is neither of the things a database does. It
     is always case-sensitive and always accent-sensitive, so under a
     case-insensitive profile — which is most of them — ACC001 and acc001 are
     reported as an account missing from one side AND an unexpected account
     on the other. Two false differences, on a screen whose entire job is to
     say whether the two sides agree. In the other direction the evidence
     mapper folded case unconditionally, so under a case-sensitive profile it
     counted values as overlapping that the database would keep apart.

     So: one folding function, driven by the same resolved collation
     everything else uses, applied at each of those four places.

       · case-insensitive collation  → fold case
       · accent-insensitive          → fold accents (NFD, drop the marks)
       · trailing spaces             → always folded, because SQL Server's =
                                       pads char and varchar operands to the
                                       same length before comparing, so
                                       'ACC1' and 'ACC1  ' ARE equal to it
       · anything else, or no settings at all → the value, unchanged

     The last line is the important one. With no collation configured these
     screens behave exactly as they did before this existed, which is what
     makes the change safe to make everywhere at once.

     What this is NOT: a reimplementation of SQL Server's collation
     algorithm. Width sensitivity, kana sensitivity, locale-specific
     casing and the sort ORDER of a collation are not modelled — folding
     changes which values are considered EQUAL, and nothing here claims to
     order them. `describeFold` exists so a screen can say which rule it
     applied rather than quietly applying one.                              */

  /* Unicode combining marks: é (e + U+0301) after NFD, and the rest of the
     block. Written as a range rather than \p{M} because this file has to
     load in whatever the Function App's Node gives it. */
  var COMBINING = /[̀-ͯ᪰-᫿᷀-᷿⃐-⃰︠-︯]/g;

  /* How the resolved collation treats two text values. `known` is false when
     nothing has been detected, which is the signal to fold nothing. */
  function matchRules(model) {
    var out = { known: false, caseInsensitive: false, accentInsensitive: false, collation: '' };
    if (!model) return out;
    var name = resolvedCollation(model);
    if (!name) return out;
    var p = parseCollation(name);
    if (!p.known) return out;
    out.known = true;
    out.collation = name;
    // A binary collation distinguishes everything, including case and
    // accent, so it folds neither — and says so rather than falling into
    // the unknown branch, which would read as "no settings".
    if (p.bin) return out;
    out.caseInsensitive = p.cs === false;
    out.accentInsensitive = p.accent === false;
    return out;
  }

  /* One value, as the resolved collation would see it for an equality test. */
  function foldKey(value, model) {
    if (value == null) return '';
    var v = String(value);
    var r = model && model.__matchRules ? model.__matchRules : matchRules(model);
    if (!r.known) return v;
    v = v.replace(/[ ]+$/, '');            // the = operator pads; so do we
    if (r.accentInsensitive && String.prototype.normalize) {
      v = v.normalize('NFD').replace(COMBINING, '').normalize('NFC');
    }
    if (r.caseInsensitive) v = v.toUpperCase().toLowerCase();
    return v;
  }
  function keysEqual(a, b, model) { return foldKey(a, model) === foldKey(b, model); }

  /* A ready-made folder, so a caller in a loop parses the collation once.
     `applied` is what a screen tests before saying anything to the user;
     `label` is the sentence to say. */
  function keyFolder(model) {
    var r = matchRules(model);
    var carrier = { __matchRules: r };
    return {
      applied: r.known && (r.caseInsensitive || r.accentInsensitive),
      known: r.known,
      caseInsensitive: r.caseInsensitive,
      accentInsensitive: r.accentInsensitive,
      collation: r.collation,
      label: describeFold(r),
      fold: function (v) { return foldKey(v, carrier); },
      equal: function (a, b) { return foldKey(a, carrier) === foldKey(b, carrier); },
    };
  }

  function describeFold(r) {
    if (!r || !r.known) return '';
    var parts = [];
    if (r.caseInsensitive) parts.push('ignoring case');
    if (r.accentInsensitive) parts.push('ignoring accents');
    if (!parts.length) return 'Matched exactly, as ' + r.collation + ' requires.';
    return 'Matched ' + parts.join(' and ') + ', to agree with ' + r.collation + '.';
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

  /* The apply-side fixtures, shared by both copies exactly as SHARED_CASES
     is. `want` is the suffix a generator should concatenate. */
  var APPLY_MODEL = {
    source: { database: 'SRC', dbCollation: 'Latin1_General_CS_AS', tempdbCollation: 'Latin1_General_CS_AS' },
    target: { database: 'TGT', dbCollation: 'SQL_Latin1_General_CP1_CI_AS', tempdbCollation: 'SQL_Latin1_General_CP1_CI_AS' },
    strategy: 'target', tempTables: 'resolved', generatedSqlMode: 'apply',
  };
  var MATCHED_APPLY = {
    source: { database: 'SRC', dbCollation: 'Latin1_General_CI_AS', tempdbCollation: 'Latin1_General_CI_AS' },
    target: { database: 'TGT', dbCollation: 'Latin1_General_CI_AS', tempdbCollation: 'Latin1_General_CI_AS' },
    strategy: 'target', tempTables: 'resolved', generatedSqlMode: 'apply',
  };
  var APPLY_CASES = [
    { name: 'a temp text column gets the resolved collation',
      fn: 'collateForTempColumn', args: [APPLY_MODEL, 'varchar'], want: ' COLLATE SQL_Latin1_General_CP1_CI_AS' },
    { name: 'and DATABASE_DEFAULT when that is what the profile says',
      fn: 'collateForTempColumn',
      args: [{ source: APPLY_MODEL.source, target: APPLY_MODEL.target, strategy: 'target', tempTables: 'database_default', generatedSqlMode: 'apply' }, 'varchar'],
      want: ' COLLATE DATABASE_DEFAULT' },
    { name: 'a non-text column never gets one',
      fn: 'collateForTempColumn', args: [APPLY_MODEL, 'int'], want: '' },
    { name: 'WARN MODE CHANGES NOTHING',
      fn: 'collateForTempColumn',
      args: [{ source: APPLY_MODEL.source, target: APPLY_MODEL.target, strategy: 'target', tempTables: 'resolved', generatedSqlMode: 'warn' }, 'varchar'],
      want: '' },
    { name: 'MATCHING COLLATIONS CHANGE NOTHING — the SQL stays byte-identical',
      fn: 'collateForTempColumn', args: [MATCHED_APPLY, 'varchar'], want: '' },
    { name: 'no settings at all change nothing',
      fn: 'collateForTempColumn', args: [null, 'varchar'], want: '' },
    { name: 'a column already carrying the resolved collation is left alone',
      fn: 'collateForColumn', args: [APPLY_MODEL, 'tgt', 'dbo', 'B', 'Code', 'SQL_Latin1_General_CP1_CI_AS'], want: '' },
    { name: 'and one that is not gets it',
      fn: 'collateForColumn', args: [APPLY_MODEL, 'src', 'dbo', 'A', 'Code', 'Latin1_General_CS_AS'],
      want: ' COLLATE SQL_Latin1_General_CP1_CI_AS' },
    { name: 'AN OVERRIDE WINS over the strategy',
      fn: 'collateForColumn',
      args: [{ source: APPLY_MODEL.source, target: APPLY_MODEL.target, strategy: 'target', generatedSqlMode: 'apply',
               columnOverrides: { 'dbo.A.Code': { collation: 'Latin1_General_BIN2' } } },
             'src', 'dbo', 'A', 'Code', 'Latin1_General_CS_AS'],
      want: ' COLLATE Latin1_General_BIN2' },
    { name: 'and an override applies even when the two databases already agree',
      fn: 'collateForColumn',
      args: [{ source: MATCHED_APPLY.source, target: MATCHED_APPLY.target, strategy: 'target', generatedSqlMode: 'apply',
               columnOverrides: { 'dbo.A.Code': { collation: 'Latin1_General_BIN2' } } },
             'src', 'dbo', 'A', 'Code', 'Latin1_General_CI_AS'],
      want: ' COLLATE Latin1_General_BIN2' },
  ];

  /* The matching cases, run against both copies for the same reason the
     clash cases are: a browser screen and a scheduled run must call the
     same two account codes the same thing. `model` is named rather than
     inlined so a reader can see which profile each answer belongs to. */
  var CI_MODEL = { source: { dbCollation: 'Latin1_General_CI_AS' },
                   target: { dbCollation: 'Latin1_General_CI_AS' }, strategy: 'target' };
  var CS_MODEL = { source: { dbCollation: 'Latin1_General_CS_AS' },
                   target: { dbCollation: 'Latin1_General_CS_AS' }, strategy: 'target' };
  var AI_MODEL = { source: { dbCollation: 'Latin1_General_CI_AI' },
                   target: { dbCollation: 'Latin1_General_CI_AI' }, strategy: 'target' };
  var BIN_MODEL = { source: { dbCollation: 'Latin1_General_BIN2' },
                    target: { dbCollation: 'Latin1_General_BIN2' }, strategy: 'target' };
  var NONE_MODEL = { source: { dbCollation: '' }, target: { dbCollation: '' }, strategy: 'target' };

  var MATCH_CASES = [
    { name: 'a case-insensitive profile treats two spellings of an account code as one',
      model: CI_MODEL, a: 'ACC001', b: 'acc001', equal: true },
    { name: 'and a case-sensitive one keeps them apart',
      model: CS_MODEL, a: 'ACC001', b: 'acc001', equal: false },
    { name: 'an accent-insensitive profile matches across an accent',
      model: AI_MODEL, a: 'Café', b: 'Cafe', equal: true },
    { name: 'a case-insensitive but accent-SENSITIVE profile does not',
      model: CI_MODEL, a: 'Café', b: 'Cafe', equal: false },
    { name: 'a binary collation distinguishes everything, so it folds nothing',
      model: BIN_MODEL, a: 'ACC001', b: 'acc001', equal: false },
    { name: 'trailing spaces never separate two values, because = pads them',
      model: CI_MODEL, a: 'ACC001', b: 'ACC001   ', equal: true },
    { name: 'and that holds under a case-sensitive profile too',
      model: CS_MODEL, a: 'ACC001', b: 'ACC001 ', equal: true },
    { name: 'a LEADING space is a different value, and stays one',
      model: CI_MODEL, a: 'ACC001', b: ' ACC001', equal: false },
    { name: 'WITH NO COLLATION DETECTED NOTHING IS FOLDED — today\'s behaviour, exactly',
      model: NONE_MODEL, a: 'ACC001', b: 'acc001', equal: false },
    { name: 'nor is a trailing space forgiven when nothing has been detected',
      model: NONE_MODEL, a: 'ACC001', b: 'ACC001 ', equal: false },
    { name: 'two genuinely different codes stay different, whatever the profile',
      model: CI_MODEL, a: 'ACC001', b: 'ACC002', equal: false },
    { name: 'null and empty are the same key, so neither becomes a phantom group',
      model: CI_MODEL, a: null, b: '', equal: true },
    { name: 'a Turkish dotted I folds the same way in both directions',
      model: CI_MODEL, a: 'İSTANBUL', b: 'i̇stanbul', equal: true },
  ];

  return {
    VERSION: VERSION, ISSUE: ISSUE,
    parseCollation: parseCollation, collationDiff: collationDiff, isSoftDiff: isSoftDiff,
    collationsClash: collationsClash, isNonUnicodeText: isNonUnicodeText, isTextType: isTextType,
    defaults: defaults, normalise: normalise, resolvedCollation: resolvedCollation,
    resolveWith: resolveWith, gateWith: gateWith,
    findClashes: findClashes, summariseClashes: summariseClashes,
    blankNoise: blankNoise, refParts: refParts,
    // Stage C: applying a fix at the point the SQL is built.
    MARKER: MARKER, needsWork: needsWork, applies: applies,
    collateForColumn: collateForColumn, collateForTempColumn: collateForTempColumn,
    collateForComparison: collateForComparison,
    headerComment: headerComment, settingsStamp: settingsStamp, applyFix: applyFix,
    // Matching two values where there is no SQL to put a COLLATE into.
    matchRules: matchRules, foldKey: foldKey, keysEqual: keysEqual,
    keyFolder: keyFolder, describeFold: describeFold,
    SHARED_CASES: SHARED_CASES, APPLY_CASES: APPLY_CASES, MATCH_CASES: MATCH_CASES,
  };
});
