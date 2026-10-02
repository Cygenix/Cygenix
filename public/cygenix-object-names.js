/* ============================================================================
   cygenix-object-names.js — find a saved table or column name in a live list
   the way the database would, not the way a string comparison would
   ----------------------------------------------------------------------------
   Oct-2026. A saved map named its source "dbo.STG_client". The database held
   "dbo.STG_Client". SQL Server compares object names case-insensitively under
   every default collation, so the SQL editor ran against it without a
   murmur — but Object Mapping looked the saved name up with ===, found
   nothing, and said "Source table not found — reconnect source DB". The
   connection was fine. Reconnecting changed nothing. The user was sent to fix
   the one thing that was not broken.

   Names reach a saved map from several places that do not agree on spelling:
   a Conversion Template builds "STG_" + the target's name in whatever case the
   template author typed; Claude, in a Dev Console staging session, creates the
   table in whatever case it chose; a person types one by hand; an older map
   was saved before a table was renamed only in case. Requiring them all to
   agree letter for letter is a demand nobody can meet, and the database
   itself never makes it.

   WHAT THIS DECIDES
   findObjectByName(list, wanted) looks a saved name up in the list a
   connection returned, and answers with one of four outcomes:

     exact      the text matches exactly. Always preferred, so a database
                with a case-SENSITIVE collation that really does hold both
                dbo.Client and dbo.client still opens the one that was saved.
     unique     no exact match, but exactly one object matches once case is
                ignored — and, when the saved name had no schema, in any
                schema. Use it. The saved map takes the live spelling the next
                time it is saved, because the page saves the live object's own
                name, not the text it was asked for.
     ambiguous  more than one object matches that loosely. Do not guess: a
                guess that picks the wrong one of stg.Client and dbo.Client
                maps the wrong data and looks perfectly healthy doing it. The
                caller shows the candidates and asks.
     missing    nothing matches. `close` carries up to five near names — the
                same name in another schema, or one a letter or two away — so
                the message can say "did you mean" instead of only "no".

   A saved name WITH a schema is not matched against another schema. "stg.X"
   and "dbo.X" are different tables, not different spellings of one; the other
   schema's table is offered as a close match instead. A saved name WITHOUT a
   schema ("STG_client") matches in any schema, because nothing says which.

   resolveColumnName / canonicaliseMapping do the same for column names: a
   saved mapping row whose srcCol or tgtCol differs from the live column only
   in case is rewritten to the live spelling, once, when the map is restored.
   Every exact comparison further down the page then works unchanged — the
   alternative was editing some twenty separate `c.name === m.tgtCol` sites
   and missing one. A joined column ("j1.Code") or an expression is left
   exactly as it is: it does not name a column of the base table.

   Pure: no DOM, no network, no storage. Loaded by object_mapping.html and by
   tests/object-names.test.js.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root) root.CygenixObjectNames = api;
})(typeof globalThis !== 'undefined' ? globalThis : (typeof window !== 'undefined' ? window : this), function () {
  'use strict';

  // Strip the quoting a name may arrive with — [dbo].[STG_Client],
  // "dbo"."STG_Client", `x` — and surrounding space. Quoting is how a name is
  // written, not part of it.
  function unquote(s) {
    s = String(s == null ? '' : s).trim();
    var m = /^\[(.*)\]$/.exec(s) || /^"(.*)"$/.exec(s) || /^`(.*)`$/.exec(s);
    return (m ? m[1] : s).trim();
  }

  // Split "schema.name" on the first dot that is outside brackets/quotes.
  // A name with no dot has no schema. Only two parts are understood; a
  // three-part name (db.schema.table) keeps its last two.
  function splitName(full) {
    var s = String(full == null ? '' : full).trim();
    var parts = [], cur = '', q = '';
    for (var i = 0; i < s.length; i++) {
      var ch = s[i];
      if (q) { cur += ch; if (ch === q) q = ''; continue; }
      if (ch === '[') { q = ']'; cur += ch; continue; }
      if (ch === '"' || ch === '`') { q = ch; cur += ch; continue; }
      if (ch === '.') { parts.push(cur); cur = ''; continue; }
      cur += ch;
    }
    parts.push(cur);
    parts = parts.map(unquote).filter(function (p, i, a) { return p !== '' || i === a.length - 1; });
    if (parts.length >= 2) return { schema: parts[parts.length - 2], name: parts[parts.length - 1] };
    return { schema: '', name: parts[0] || '' };
  }

  // The comparison key: schema and name, unquoted, lower-cased. Lower-casing
  // is what SQL Server's default (case-insensitive) collations do to an
  // identifier comparison, near enough for names people actually use.
  function normaliseObjectName(full) {
    var p = splitName(full);
    return {
      schema: p.schema, name: p.name,
      schemaKey: p.schema.toLowerCase(), nameKey: p.name.toLowerCase(),
      key: (p.schema ? p.schema.toLowerCase() + '.' : '') + p.name.toLowerCase(),
    };
  }

  // The live list's items come in the shape connectSrc/connectTgt build:
  // { value, label, schema, name, fullName }. Anything with at least a
  // fullName or value is understood.
  function partsOf(item) {
    if (item && (item.name != null)) {
      return { schema: String(item.schema || ''), name: String(item.name) };
    }
    return splitName(item && (item.fullName || item.value || item.label));
  }
  function fullOf(item) {
    if (!item) return '';
    if (item.fullName) return String(item.fullName);
    if (item.value) return String(item.value);
    var p = partsOf(item);
    return p.schema ? p.schema + '.' + p.name : p.name;
  }

  // Edit distance, bounded: anything past `max` is "far", which is all the
  // close-match list needs to know. Keeps a 2,000-table schema cheap.
  function distance(a, b, max) {
    if (a === b) return 0;
    if (Math.abs(a.length - b.length) > max) return max + 1;
    var prev = [], cur = [], i, j;
    for (j = 0; j <= b.length; j++) prev[j] = j;
    for (i = 1; i <= a.length; i++) {
      cur = [i];
      var rowMin = i;
      for (j = 1; j <= b.length; j++) {
        var c = a[i - 1] === b[j - 1] ? 0 : 1;
        cur[j] = Math.min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + c);
        if (cur[j] < rowMin) rowMin = cur[j];
      }
      if (rowMin > max) return max + 1;
      prev = cur;
    }
    return prev[b.length];
  }

  // Letters and digits only: "STG_Client", "stg-client" and "STGClient" are
  // the same word to a reader, and close enough to suggest.
  function squash(s) { return String(s).toLowerCase().replace(/[^a-z0-9]/g, ''); }

  function closeMatches(list, want, limit) {
    var scored = [];
    var wn = want.nameKey, ws = squash(want.name);
    var max = Math.max(1, Math.min(3, Math.floor(want.name.length / 4)));
    (list || []).forEach(function (item) {
      var p = partsOf(item), n = p.name.toLowerCase();
      var score;
      if (n === wn) score = 0;                                   // same name, other schema
      else if (squash(p.name) === ws) score = 1;                 // differs only in punctuation
      else {
        var d = distance(n, wn, max);
        if (d <= max) score = 1 + d;                             // a letter or two away
        else if (ws.length >= 4 && (squash(p.name).indexOf(ws) !== -1 || ws.indexOf(squash(p.name)) !== -1)
                 && squash(p.name).length >= 4) score = 6;       // one contains the other
      }
      if (score != null) scored.push({ item: item, score: score });
    });
    scored.sort(function (a, b) { return a.score - b.score || fullOf(a.item).localeCompare(fullOf(b.item)); });
    return scored.slice(0, limit || 5).map(function (x) { return x.item; });
  }

  /* findObjectByName(list, wanted) → { status, match, candidates, close }
       status      'exact' | 'unique' | 'ambiguous' | 'missing'
       match       the live item, for exact and unique; otherwise null
       candidates  every loose match, for ambiguous (and the one, for unique)
       close       near names, for missing                                   */
  function findObjectByName(list, wanted) {
    list = Array.isArray(list) ? list : [];
    var text = String(wanted == null ? '' : wanted).trim();
    var out = { status: 'missing', match: null, candidates: [], close: [] };
    if (!text) return out;

    // 1. Exactly as saved. The value and label are what the page stores.
    for (var i = 0; i < list.length; i++) {
      var it = list[i];
      if (it && (it.value === text || it.label === text || it.fullName === text)) {
        out.status = 'exact'; out.match = it; out.candidates = [it];
        return out;
      }
    }

    // 2. As the database would compare it.
    var want = normaliseObjectName(text);
    var hits = list.filter(function (item) {
      var p = partsOf(item);
      if (p.name.toLowerCase() !== want.nameKey) return false;
      return !want.schema || p.schema.toLowerCase() === want.schemaKey;
    });
    if (hits.length === 1) { out.status = 'unique'; out.match = hits[0]; out.candidates = hits; return out; }
    if (hits.length > 1) { out.status = 'ambiguous'; out.candidates = hits; return out; }

    out.close = closeMatches(list, want, 5);
    return out;
  }

  // A column list may hold strings or { name } objects; both appear.
  function colName(c) { return typeof c === 'string' ? c : (c && c.name != null ? String(c.name) : ''); }

  // The live spelling of a saved column name, or the saved text unchanged when
  // there is no single case-insensitive match (none, or two that differ only
  // in case on a case-sensitive database — leave those for the person).
  function resolveColumnName(columns, wanted) {
    var w = String(wanted == null ? '' : wanted);
    if (!w) return w;
    var names = (columns || []).map(colName);
    if (names.indexOf(w) !== -1) return w;
    var lw = w.toLowerCase();
    var hits = names.filter(function (n) { return n.toLowerCase() === lw; });
    return hits.length === 1 ? hits[0] : w;
  }

  // Rewrite a mapping's srcCol/tgtCol to the live spelling. Returns
  // { mapping, changed } — a new array; the rows are copies. srcCol is only
  // touched when it is a plain name: a joined column ("j1.Code") belongs to
  // another table and an expression is not a name at all.
  function canonicaliseMapping(mapping, srcColumns, tgtColumns) {
    var changed = 0;
    var rows = (Array.isArray(mapping) ? mapping : []).map(function (m) {
      if (!m || typeof m !== 'object') return m;
      var r = {}; for (var k in m) if (Object.prototype.hasOwnProperty.call(m, k)) r[k] = m[k];
      if (srcColumns && typeof r.srcCol === 'string' && r.srcCol && !/[.\s()+\-*/,'"\[\]]/.test(r.srcCol)) {
        var s = resolveColumnName(srcColumns, r.srcCol);
        if (s !== r.srcCol) { r.srcCol = s; changed++; }
      }
      if (tgtColumns && typeof r.tgtCol === 'string' && r.tgtCol) {
        var t = resolveColumnName(tgtColumns, r.tgtCol);
        if (t !== r.tgtCol) { r.tgtCol = t; changed++; }
      }
      return r;
    });
    return { mapping: rows, changed: changed };
  }

  return {
    normaliseObjectName: normaliseObjectName,
    findObjectByName: findObjectByName,
    resolveColumnName: resolveColumnName,
    canonicaliseMapping: canonicaliseMapping,
    _splitName: splitName,
    _distance: distance,
  };
});
