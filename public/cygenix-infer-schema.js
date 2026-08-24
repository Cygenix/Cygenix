/* cygenix-infer-schema.js — inferred relationships for a database that
 * declares no foreign keys.
 *
 * Legacy enterprise schemas frequently ship with zero referential integrity:
 * every join lives in application code and stored procedures, and the Schema
 * map draws a wall of disconnected boxes. This module rebuilds the edge list
 * from evidence, on the principle that heuristics PROPOSE and SQL DISPOSES:
 * nothing is published above `hypothesis` tier unless real data agreed.
 *
 * Evidence channels, run in order of trustworthiness:
 *   A  Code archaeology — join predicates parsed out of sys.sql_modules
 *      (views, procedures, functions, triggers). These are observed facts
 *      about how the business actually joins its data.
 *   B  Type compatibility — a gate, not a score. Removes implausible pairs
 *      before any data is read.
 *   C  Value containment — the exact inclusion-dependency test, run on the
 *      server for the top-ranked candidates only, TOP-sampled on big tables.
 *   (Name similarity contributes to the score after per-table prefix
 *   stripping, because legacy columns are prefixed per table — a naive
 *   name match finds nothing across `tc*` and `cl*` columns.)
 *
 * Scoring and tiers follow the inference brief:
 *   score = 0.35·code + 0.30·containment + 0.15·name + 0.10·cardinality
 *         + 0.10·semantic − penalties
 *   confirmed ≥0.90 (requires code evidence AND containment ≥0.98)
 *   probable 0.70–0.89 · possible 0.40–0.69 · hypothesis <0.40
 *
 * Read-only by construction: every statement this module builds is a SELECT
 * against catalog views or a WITH (NOLOCK) sample, and every scan is capped
 * with TOP. Caps are reported in the catalogue's stats — a silently skipped
 * table reads as covered, which is the one failure mode that discredits the
 * whole exercise.
 *
 * SQL Server only: the catalog views and T-SQL sampling have no portable
 * equivalent, and the run refuses with a reason rather than guessing.
 */
(function () {
  'use strict';

  const CAPS = {
    moduleCount: 400,        // modules parsed for join predicates
    moduleChars: 60000,      // definition truncation (Netlify response ceiling)
    candidateTables: 250,    // largest tables considered for name matching
    probeTop: 500000,        // TOP for uniqueness probes and containment child side
    keyProbes: 30,           // uniqueness COUNT probes per run
    confirmPairs: 40,        // containment tests per run
    parentsPerChildCol: 3,   // best parents kept per child column
  };

  const rows = (res) => (res && (res.recordset || res.rows)) || [];
  const q = (s) => '[' + String(s == null ? '' : s).replace(/]/g, ']]') + ']';
  const splitKey = (k) => {
    const i = String(k).indexOf('.');
    return i < 0 ? ['dbo', String(k)] : [String(k).slice(0, i), String(k).slice(i + 1)];
  };
  const qk = (k) => { const [s, n] = splitKey(k); return q(s) + '.' + q(n); };

  // ── Naming: prefixes and stems ────────────────────────────────────────────
  // Legacy columns carry a per-table mnemonic prefix (tcindex, tcdate, …), so
  // a join is child.<childprefix><stem> = parent.<parentprefix><stem> and raw
  // name equality finds nothing. Learn the dialect first.

  // The longest lowercase prefix of length 2–5 shared by at least 60% of the
  // table's columns. Longest wins so `tcb` beats `tc` when both qualify.
  function siPrefixOf(colNames) {
    const names = (colNames || []).map(n => String(n || '').toLowerCase()).filter(Boolean);
    if (names.length < 3) return '';
    let best = '';
    for (let len = 5; len >= 2; len--) {
      const counts = {};
      for (const n of names) {
        if (n.length <= len) continue;
        const p = n.slice(0, len);
        if (!/^[a-z]+$/.test(p)) continue;
        counts[p] = (counts[p] || 0) + 1;
      }
      for (const p of Object.keys(counts).sort()) {
        if (counts[p] >= names.length * 0.6) { best = p; break; }
      }
      if (best) break;
    }
    return best;
  }

  function siPrefixMap(tables) {
    const map = {};
    for (const t of tables || []) {
      let p = t.columns && t.columns.length ? siPrefixOf(t.columns.map(c => c.name)) : '';
      // A "prefix" that is the start of the table's own name is the entity
      // name, not a mnemonic — client_id/client_name on `client` must stem to
      // `client`, and stripping `clien` would destroy exactly the part the
      // matcher needs. Mnemonics like tc-on-timecard survive: they abbreviate
      // the name rather than beginning it.
      if (p && String(t.name || '').toLowerCase().startsWith(p)) p = '';
      map[t.key] = p;
    }
    return map;
  }

  // Normalized stem: strip the owning table's prefix, then the id/code/key
  // suffix noise, then underscores. `tcclientid` (prefix tc) and `client_id`
  // both stem to `client`.
  const STEM_SUFFIXES = ['_id', '_cd', '_nbr', 'id', 'no', 'num', 'code', 'key'];
  function siStem(colName, prefix) {
    let s = String(colName || '').toLowerCase();
    if (prefix && s.startsWith(prefix) && s.length - prefix.length >= 2) s = s.slice(prefix.length);
    for (const suf of STEM_SUFFIXES) {
      if (s.length - suf.length >= 2 && s.endsWith(suf)) { s = s.slice(0, -suf.length); break; }
    }
    return s.replace(/_/g, '');
  }

  // Stems that survive stripping but identify nothing on their own. A pair
  // whose only story is "both are called code" earns a penalty, not an edge.
  const GENERIC_STEMS = new Set(['', 'id', 'code', 'no', 'num', 'key', 'type', 'status', 'seq', 'flag', 'date', 'desc', 'name', 'row', 'rec', 'index']);

  // Backup and snapshot tables are out of migration scope; matching against
  // them would only manufacture edges into dead data. Still counted, so the
  // coverage report can show what was excluded.
  const siObsolete = (name) => /^_?baktab|^zz_|^_snr_|_bak(_|$)|_backup(_|$)|_\d{8}($|_)/i.test(String(name || ''));

  // ── Types: a gate, not a score ────────────────────────────────────────────
  function siParseType(fmt) {
    const s = String(fmt || '').trim().toUpperCase();
    const m = s.match(/^([A-Z ]+?)\s*(\((\s*MAX\s*|[\d,\s]+)\))?$/);
    if (!m) return { family: 'other', len: null };
    const base = m[1].trim();
    const arg = (m[3] || '').trim();
    const len = arg === '' ? null : (arg === 'MAX' ? Infinity : parseInt(arg, 10));
    const scale = arg.includes(',') ? parseInt(arg.split(',')[1], 10) : 0;
    if (/^(TINYINT|SMALLINT|INT|BIGINT)$/.test(base)) return { family: 'int', len: null };
    if (/^(DECIMAL|NUMERIC)$/.test(base)) return { family: scale > 0 ? 'decimal' : 'int', len: null };
    if (/^(MONEY|SMALLMONEY)$/.test(base)) return { family: 'money', len: null };
    if (/^(FLOAT|REAL)$/.test(base)) return { family: 'float', len: null };
    if (/^(CHAR|VARCHAR|NCHAR|NVARCHAR|TEXT|NTEXT)$/.test(base)) {
      return { family: 'text', len: /^N?TEXT$/.test(base) ? Infinity : len };
    }
    if (/^(DATE|DATETIME|DATETIME2|SMALLDATETIME|DATETIMEOFFSET|TIME)$/.test(base)) return { family: 'date', len: null };
    if (/^UNIQUEIDENTIFIER$/.test(base)) return { family: 'guid', len: null };
    if (/^(BINARY|VARBINARY|IMAGE|TIMESTAMP|ROWVERSION)$/.test(base)) return { family: 'binary', len: null };
    if (/^BIT$/.test(base)) return { family: 'bit', len: null };
    return { family: 'other', len: null };
  }

  // Join-compatible or not. Different families never join; a bounded string
  // against MAX is implausible as a key; wildly different bounded lengths too.
  function siTypeGate(fmtA, fmtB) {
    const a = siParseType(fmtA), b = siParseType(fmtB);
    if (a.family !== b.family) return false;
    if (a.family === 'text') {
      const la = a.len == null ? 0 : a.len, lb = b.len == null ? 0 : b.len;
      if ((la === Infinity) !== (lb === Infinity)) return false;
      if (la && lb && la !== Infinity && (Math.max(la, lb) / Math.min(la, lb) > 4) && Math.abs(la - lb) > 20) return false;
    }
    return true;
  }

  // Excluded from key candidacy entirely: dates, amounts and free text are
  // where containment lies — every busy table contains most calendar dates.
  function siKeyable(fmt) {
    const t = siParseType(fmt);
    if (['date', 'money', 'float', 'decimal', 'binary', 'other'].includes(t.family)) return false;
    if (t.family === 'text' && (t.len === Infinity || (t.len || 0) > 100)) return false;
    return true;
  }

  // A flag-shaped child column (bit, or a single character) can be contained
  // in almost anything; the tier logic caps such pairs at hypothesis.
  function siFlaggy(fmt) {
    const t = siParseType(fmt);
    return t.family === 'bit' || (t.family === 'text' && t.len === 1);
  }

  // ── Channel A: code archaeology ───────────────────────────────────────────
  // Parse join predicates out of module definitions. Comments and string
  // literals are stripped first so a predicate quoted inside dynamic SQL or a
  // commented-out join does not count as evidence.
  function siStripSql(text) {
    return String(text || '')
      .replace(/\/\*[\s\S]*?\*\//g, ' ')
      .replace(/--[^\n]*/g, ' ')
      .replace(/'(?:[^']|'')*'/g, "''");
  }

  const SQL_KEYWORDS = new Set(['on', 'where', 'inner', 'left', 'right', 'full', 'outer', 'cross', 'join', 'as', 'with', 'and', 'or', 'not', 'select', 'from', 'group', 'order', 'having', 'union', 'nolock', 'set', 'when', 'then', 'else', 'end', 'case']);

  // One module's predicates: [{a:{table,col}, b:{table,col}, ambiguous}].
  // resolveTable maps a lowercased qualifier (alias, bare name, schema.name)
  // to a canonical table key, or null.
  function siParseJoins(definition, resolveTable) {
    const text = siStripSql(definition);
    // Aliases. `FROM dbo.timecard tc` / `JOIN [dbo].[client] AS c`. An alias
    // reused for two different tables in the same module cannot be resolved
    // with confidence; edges through it are recorded but marked ambiguous.
    const aliases = new Map();   // alias → Set(tableKey)
    const aliasRe = /\b(?:from|join)\s+((?:\[[^\]]+\]|[\w#]+)(?:\s*\.\s*(?:\[[^\]]+\]|[\w#]+))?)\s*(?:\b(?:as)\s+)?(\w+)?/gi;
    let m;
    const addAlias = (rawTable, rawAlias) => {
      const table = resolveTable(String(rawTable).replace(/[\[\]\s]/g, '').toLowerCase());
      if (!table) return;
      const alias = String(rawAlias || '').toLowerCase();
      if (!alias || SQL_KEYWORDS.has(alias)) return;
      if (!aliases.has(alias)) aliases.set(alias, new Set());
      aliases.get(alias).add(table);
    };
    while ((m = aliasRe.exec(text))) addAlias(m[1], m[2]);
    // The pre-ANSI comma list: `FROM tab1 t1, tab2 t2 WHERE …`. Commas appear
    // in select lists too, so this only sticks when the captured name resolves
    // to a real table — garbage from a column list resolves to nothing.
    const commaRe = /,\s*((?:\[[^\]]+\]|[\w#]+)(?:\s*\.\s*(?:\[[^\]]+\]|[\w#]+))?)\s+(?:as\s+)?(\w+)/gi;
    while ((m = commaRe.exec(text))) addAlias(m[1], m[2]);
    const resolve = (qual) => {
      const a = aliases.get(qual);
      if (a && a.size === 1) return { table: [...a][0], ambiguous: false };
      if (a && a.size > 1) return { table: [...a].sort()[0], ambiguous: true };
      const t = resolveTable(qual);
      return t ? { table: t, ambiguous: false } : null;
    };
    const out = [];
    // Equality predicates, wherever they appear — ON clauses and the implicit
    // `FROM a, b WHERE a.x = b.y` both land here.
    const predRe = /(\w+)\s*\.\s*(?:\[([^\]]+)\]|(\w+))\s*=\s*(\w+)\s*\.\s*(?:\[([^\]]+)\]|(\w+))/g;
    while ((m = predRe.exec(text))) {
      const ra = resolve(m[1].toLowerCase()), rb = resolve(m[4].toLowerCase());
      if (!ra || !rb) continue;
      const colA = (m[2] || m[3] || '').toLowerCase(), colB = (m[5] || m[6] || '').toLowerCase();
      if (!colA || !colB) continue;
      if (ra.table === rb.table) continue;   // self joins order nothing here
      out.push({
        a: { table: ra.table, col: colA },
        b: { table: rb.table, col: colB },
        ambiguous: ra.ambiguous || rb.ambiguous,
      });
    }
    return out;
  }

  // All modules aggregated: occurrences counts DISTINCT modules, because one
  // procedure repeating a join five times is one observation, and a join that
  // forty separate reports agree on is near-certain.
  function siCodeJoins(modules, resolveTable) {
    const agg = new Map();
    for (const mod of modules || []) {
      const src = (mod.schemaName || 'dbo') + '.' + (mod.moduleName || '?');
      const seen = new Set();
      for (const p of siParseJoins(mod.definition, resolveTable)) {
        // Order-independent key: the same predicate written either way around
        // is the same evidence.
        const sides = [p.a.table + '|' + p.a.col, p.b.table + '|' + p.b.col].sort();
        const k = sides.join('||');
        if (!agg.has(k)) {
          const [x, y] = sides.map(s => s.split('|'));
          agg.set(k, { aTable: x[0], aCol: x[1], bTable: y[0], bCol: y[1], occurrences: 0, sources: [], ambiguous: false });
        }
        const e = agg.get(k);
        if (!seen.has(k)) { e.occurrences++; seen.add(k); if (e.sources.length < 8) e.sources.push(src); }
        if (p.ambiguous) e.ambiguous = true;
      }
    }
    return [...agg.values()].sort((a, b) =>
      (a.aTable + a.aCol + a.bTable + a.bCol).localeCompare(b.aTable + b.aCol + b.bTable + b.bCol));
  }

  // ── SQL builders — every one a capped, read-only SELECT ──────────────────
  function siModulesSql(caps) {
    const c = caps || CAPS;
    return 'SELECT TOP (' + (c.moduleCount | 0) + ') s.name AS schemaName, o.name AS moduleName,\n'
      + ' LEFT(m.definition, ' + (c.moduleChars | 0) + ') AS definition\n'
      + 'FROM sys.sql_modules m\n'
      + 'JOIN sys.objects o ON o.object_id = m.object_id\n'
      + 'JOIN sys.schemas s ON s.schema_id = o.schema_id\n'
      + "WHERE m.definition LIKE '%JOIN%' OR m.definition LIKE '%WHERE%'\n"
      + 'ORDER BY s.name, o.name;';
  }

  function siModuleCountSql() {
    return 'SELECT COUNT_BIG(*) AS n FROM sys.sql_modules;';
  }

  // In a database with no FKs, surviving unique indexes and identity columns
  // are the strongest remaining statement of intent about keys. One cheap
  // catalog pass; single-column unique indexes only — composite inference is
  // out of scope for this pass and is said so in the stats.
  function siUniqueColsSql() {
    return 'SELECT s.name AS schemaName, t.name AS tableName, c.name AS columnName, why FROM (\n'
      + "  SELECT i.object_id, ic.column_id, 'unique_index' AS why\n"
      + ' FROM sys.indexes i\n'
      + ' JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0\n'
      + ' WHERE i.is_unique = 1 AND 1 = (SELECT COUNT(*) FROM sys.index_columns k\n'
      + ' WHERE k.object_id = i.object_id AND k.index_id = i.index_id AND k.is_included_column = 0)\n'
      + ' UNION ALL\n'
      + "  SELECT c2.object_id, c2.column_id, 'identity' FROM sys.columns c2 WHERE c2.is_identity = 1\n"
      + ') u\n'
      + 'JOIN sys.columns c ON c.object_id = u.object_id AND c.column_id = u.column_id\n'
      + 'JOIN sys.tables t ON t.object_id = u.object_id\n'
      + 'JOIN sys.schemas s ON s.schema_id = t.schema_id\n'
      + 'ORDER BY s.name, t.name, c.name, why;';
  }

  function siKeyEvidence(catalogRows) {
    const map = new Map();   // 'schema.table|col(lower)' → 'unique_index' | 'identity'
    for (const r of catalogRows || []) {
      const k = (r.schemaName || 'dbo') + '.' + r.tableName + '|' + String(r.columnName || '').toLowerCase();
      // unique_index outranks identity: identity says generated, unique says unique.
      if (map.get(k) !== 'unique_index') map.set(k, r.why);
    }
    return map;
  }

  // Uniqueness probe. Row counts come from the catalog; this reads data, so it
  // is TOP-capped — above the cap the verdict is about the sample and says so.
  function siKeyProbeSql(tableKey, col, top) {
    return 'SELECT COUNT_BIG(*) AS n_rows,\n'
      + ' COUNT_BIG(DISTINCT ' + q(col) + ') AS n_distinct,\n'
      + ' SUM(CASE WHEN ' + q(col) + ' IS NULL THEN 1 ELSE 0 END) AS n_null\n'
      + 'FROM (SELECT TOP (' + (top | 0) + ') ' + q(col) + ' FROM ' + qk(tableKey) + ' WITH (NOLOCK)) x;';
  }

  function siKeyVerdict(row) {
    const n = Number(row && row.n_rows) || 0;
    const d = Number(row && row.n_distinct) || 0;
    const nul = Number(row && row.n_null) || 0;
    const denom = n - nul;
    const uniqueness = denom > 0 ? d / denom : 0;
    return { nRows: n, ndv: d, nulls: nul, uniqueness, unique: n > 0 && nul === 0 && d === n };
  }

  // The exact containment test, child side TOP-sampled. The orphans matter as
  // much as the edge: migration loads fail on the orphans.
  function siContainSql(childKey, childCol, parentKey, parentCol, childTop) {
    return 'SELECT COUNT_BIG(*) AS child_rows,\n'
      + ' COUNT_BIG(DISTINCT c.' + q(childCol) + ') AS child_ndv,\n'
      + ' COUNT_BIG(DISTINCT CASE WHEN p.' + q(parentCol) + ' IS NOT NULL THEN c.' + q(childCol) + ' END) AS matched_ndv,\n'
      + ' SUM(CASE WHEN p.' + q(parentCol) + ' IS NULL THEN 1 ELSE 0 END) AS orphan_rows\n'
      + 'FROM (SELECT TOP (' + (childTop | 0) + ') ' + q(childCol) + ' FROM ' + qk(childKey) + ' WITH (NOLOCK)\n'
      + ' WHERE ' + q(childCol) + ' IS NOT NULL) c\n'
      + 'LEFT JOIN ' + qk(parentKey) + ' p WITH (NOLOCK) ON p.' + q(parentCol) + ' = c.' + q(childCol) + ';';
  }

  function siContainVerdict(row) {
    const childRows = Number(row && row.child_rows) || 0;
    const ndv = Number(row && row.child_ndv) || 0;
    const matched = Number(row && row.matched_ndv) || 0;
    const orphans = Number(row && row.orphan_rows) || 0;
    return {
      childRows, childNdv: ndv,
      containment: ndv > 0 ? matched / ndv : null,
      orphanRows: orphans,
      orphanRate: childRows > 0 ? orphans / childRows : null,
    };
  }

  // ── Scoring ───────────────────────────────────────────────────────────────
  function siScore(c) {
    let code = c.codeOccurrences > 0 ? Math.min(1, Math.log1p(c.codeOccurrences) / Math.log1p(10)) : 0;
    if (c.ambiguous) code *= 0.5;   // unresolvable alias halves the channel
    let s = 0.35 * code
          + 0.30 * (c.containment == null ? 0 : c.containment)
          + 0.15 * (c.nameSim || 0)
          + 0.10 * (c.cardinalityOk ? 1 : 0)
          + 0.10 * (c.semantic ? 1 : 0);
    if (c.degenerateParent) s -= 0.20;      // parent NDV < 20: containment lies
    if (c.genericName) s -= 0.15;           // "code = code" identifies nothing
    if (c.orphanRate != null && c.orphanRate > 0.25) s -= 0.15;
    if (c.ambiguous) s -= 0.10;
    return Math.max(0, Math.round(s * 100) / 100);
  }

  function siTier(c, score) {
    let t = score >= 0.90 ? 'confirmed' : score >= 0.70 ? 'probable' : score >= 0.40 ? 'possible' : 'hypothesis';
    // `confirmed` is a promise: code said so AND the data agreed almost
    // perfectly. A high score without both is only probable.
    if (t === 'confirmed' && !(c.codeOccurrences > 0 && (c.containment || 0) >= 0.98)) t = 'probable';
    // Verification rule: a confirmed edge with >5% orphans is either wrong or
    // a genuine integrity problem — both need a human.
    if (t === 'confirmed' && c.orphanRate != null && c.orphanRate > 0.05) t = 'probable';
    // Degenerate parent domains make containment meaningless.
    if (c.degenerateParent && (t === 'confirmed' || t === 'probable')) t = 'possible';
    // Flags fit inside anything; identity ranges 1..n fit inside each other.
    // Both need code corroboration to rise above hypothesis.
    if (c.flaggyChild && !c.codeOccurrences) t = 'hypothesis';
    if (c.identityPair && !c.codeOccurrences) t = 'hypothesis';
    if (c.noUniqueSide && (t === 'confirmed' || t === 'probable' || t === 'possible')) t = 'hypothesis';
    return t;
  }

  function siMultiplicity(childUnique, parentUnique) {
    if (parentUnique && childUnique) return '1:1';
    if (parentUnique) return 'many_to_one';
    return 'unknown';
  }

  // ── Decisions: the human review loop ─────────────────────────────────────
  // Accept publishes at confirmed; reject removes the pair permanently.
  // Applied last and never overwritten by inference.
  const relMatch = (r, d) =>
    r.child === d.child && r.childCol === d.childCol && r.parent === d.parent && r.parentCol === d.parentCol;

  function siApplyDecisions(rels, decisions) {
    const d = decisions || {};
    const accepted = d.accept || [], rejected = d.reject || [];
    return (rels || [])
      .filter(r => !rejected.some(x => relMatch(r, x)))
      .map(r => accepted.some(x => relMatch(r, x))
        ? Object.assign({}, r, { tier: 'confirmed', status: 'accepted' })
        : r);
  }

  // ── Catalogue → graph edges ───────────────────────────────────────────────
  // Hypothesis edges are hidden unless asked for; join paths and the focus
  // control should only ever walk confirmed + probable, which the caller
  // enforces by reading the tier off the edge.
  function siToEdges(catalogue, opts) {
    const o = opts || {};
    return ((catalogue && catalogue.relationships) || [])
      .filter(r => r.tier !== 'hypothesis' || o.includeHypothesis)
      .map(r => ({
        from: r.child, to: r.parent,
        fromColumn: r.childCol, toColumn: r.parentCol,
        name: 'inferred:' + r.tier,
        self: r.child === r.parent,
        inferred: true, tier: r.tier, confidence: r.confidence,
        orphanRate: r.orphanRate, evidence: r.evidence,
      }));
  }

  // ── Orchestrator ──────────────────────────────────────────────────────────
  // graph: the CygenixSchemaGraph shape. io: { dbCall(body)→result,
  // columnsFor(keys)→Promise, onStatus(msg), caps }. Returns the catalogue.
  async function siRun(graph, io) {
    const caps = Object.assign({}, CAPS, (io && io.caps) || {});
    const say = (io && io.onStatus) || (() => {});
    const dbCall = io.dbCall;
    const notes = [];
    const stats = { probes: 0, confirms: 0, modulesParsed: 0, modulesTotal: null,
                    tablesConsidered: 0, tablesSkipped: 0, obsoleteExcluded: 0,
                    compositeKeysNote: 'single-column keys only in this pass' };

    if (!graph || !graph.ok || !graph.tables.length) return { ok: false, reason: 'No schema loaded' };

    // The catalog views and sampling syntax are T-SQL; refuse honestly on
    // anything else rather than half-running.
    try {
      await dbCall({ action: 'execute', sql: "SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS NVARCHAR(20)) AS v;" });
    } catch (e) {
      return { ok: false, reason: 'Relationship inference needs SQL Server catalog views ('
        + ((e && e.message) || 'probe failed') + ')' };
    }

    // Qualifier resolution for the code parser: schema.name, or a bare name
    // when it is unambiguous across schemas.
    const byLower = new Map(), byBare = new Map();
    for (const t of graph.tables) {
      byLower.set(t.key.toLowerCase(), t.key);
      const bare = t.name.toLowerCase();
      byBare.set(bare, byBare.has(bare) ? null : t.key);   // null = ambiguous
    }
    const resolveTable = (qual) => {
      if (!qual) return null;
      const withSchema = byLower.get(qual) || byLower.get(qual.includes('.') ? qual : 'dbo.' + qual);
      if (withSchema) return withSchema;
      return qual.includes('.') ? null : (byBare.get(qual) || null);
    };

    // Channel A.
    say('Reading code: views, procedures, functions…');
    let codeJoins = [];
    try {
      const [mods, count] = await Promise.all([
        dbCall({ action: 'execute', sql: siModulesSql(caps) }),
        dbCall({ action: 'execute', sql: siModuleCountSql() }).catch(() => null),
      ]);
      const list = rows(mods);
      stats.modulesParsed = list.length;
      stats.modulesTotal = count ? Number((rows(count)[0] || {}).n) || null : null;
      if (stats.modulesTotal && stats.modulesTotal > list.length) {
        notes.push('Parsed the ' + list.length + ' largest of ' + stats.modulesTotal + ' modules');
      }
      codeJoins = siCodeJoins(list, resolveTable);
    } catch (e) {
      notes.push('Code archaeology unavailable: ' + ((e && e.message) || 'query failed'));
    }
    say('Found ' + codeJoins.length + ' distinct join predicates in code');

    // Key evidence from the catalog.
    let keyEv = new Map();
    try {
      keyEv = siKeyEvidence(rows(await dbCall({ action: 'execute', sql: siUniqueColsSql() })));
    } catch (e) {
      notes.push('Index catalog unavailable: ' + ((e && e.message) || 'query failed'));
    }

    // Which tables take part. Code-join tables always; then the largest
    // non-empty, non-obsolete tables up to the cap. The skip count is
    // reported — never silent.
    const codeTables = new Set();
    for (const j of codeJoins) { codeTables.add(j.aTable); codeTables.add(j.bTable); }
    const eligible = graph.tables.filter(t => t.kind !== 'view' && !siObsolete(t.name));
    stats.obsoleteExcluded = graph.tables.filter(t => siObsolete(t.name)).length;
    const ranked = eligible.filter(t => t.rowCount > 0 && !codeTables.has(t.key))
      .sort((a, b) => b.rowCount - a.rowCount || a.key.localeCompare(b.key));
    const candSet = new Set([...codeTables].filter(k => graph.byKey[k]));
    for (const t of ranked) {
      if (candSet.size >= caps.candidateTables) break;
      candSet.add(t.key);
    }
    stats.tablesConsidered = candSet.size;
    stats.tablesSkipped = eligible.length - candSet.size;
    if (stats.tablesSkipped > 0) {
      notes.push(stats.tablesSkipped + ' smaller tables not considered (cap ' + caps.candidateTables + ')');
    }

    // Columns for the candidates — lazily cached by the graph layer.
    say('Loading columns for ' + candSet.size + ' tables…');
    if (io.columnsFor) {
      await io.columnsFor([...candSet].filter(k => !(graph.byKey[k] && graph.byKey[k].columns)));
    }

    // Naming dialect, then parent key columns: a column with unique/identity
    // evidence, or whose stem matches its own table's name (the classic
    // surrogate `tcindex` on `timecard` falls out of the prefix map instead).
    const candTables = [...candSet].map(k => graph.byKey[k]).filter(t => t && t.columns && t.columns.length);
    const prefixMap = siPrefixMap(candTables);
    const colType = new Map(), colIdent = new Map();
    for (const t of candTables) {
      for (const c of t.columns) {
        colType.set(t.key + '|' + c.name.toLowerCase(), c.type || '');
        colIdent.set(t.key + '|' + c.name.toLowerCase(), !!c.isIdentity);
      }
    }
    const tableStem = (t) => siStem(t.name.toLowerCase(), '');

    const parentCols = [];   // {table, col, stem, why}
    for (const t of candTables) {
      const tStem = tableStem(t);
      for (const c of t.columns) {
        if (!siKeyable(c.type)) continue;
        const lk = t.key + '|' + c.name.toLowerCase();
        const ev = keyEv.get(lk);
        const stem = siStem(c.name, prefixMap[t.key]);
        if (ev) parentCols.push({ table: t.key, col: c.name, stem, why: ev });
        else if (stem && !GENERIC_STEMS.has(stem) && (stem === tStem || t.name.toLowerCase().includes(stem)) )
          parentCols.push({ table: t.key, col: c.name, stem, why: 'name' });
      }
    }

    // Candidate pairs: name channel (same stem, type-gated) + every code join.
    say('Matching candidate pairs…');
    const cand = new Map();   // 'child|ccol|parent|pcol' (direction unknown yet: store unordered)
    const addCand = (aT, aC, bT, bC, from) => {
      const sides = [aT + '|' + aC.toLowerCase(), bT + '|' + bC.toLowerCase()].sort();
      const k = sides.join('||');
      if (!cand.has(k)) {
        const [x, y] = sides.map(s => { const i = s.indexOf('|'); return [s.slice(0, i), s.slice(i + 1)]; });
        cand.set(k, { aTable: x[0], aCol: x[1], bTable: y[0], bCol: y[1],
                      codeOccurrences: 0, sources: [], ambiguous: false, nameSim: 0, channels: new Set() });
      }
      const c = cand.get(k);
      c.channels.add(from);
      return c;
    };

    for (const j of codeJoins) {
      const c = addCand(j.aTable, j.aCol, j.bTable, j.bCol, 'code');
      c.codeOccurrences = j.occurrences;
      c.sources = j.sources;
      c.ambiguous = j.ambiguous;
    }
    for (const p of parentCols) {
      if (GENERIC_STEMS.has(p.stem)) continue;
      for (const t of candTables) {
        if (t.key === p.table) continue;
        for (const c of t.columns) {
          if (siStem(c.name, prefixMap[t.key]) !== p.stem) continue;
          const pType = colType.get(p.table + '|' + p.col.toLowerCase());
          if (!siTypeGate(c.type, pType)) continue;   // Channel B gates here
          addCand(t.key, c.name, p.table, p.col, 'name').nameSim = 1;
        }
      }
    }

    // Type-gate the code pairs too, where both types are known. Code evidence
    // outranks the gate only when the gate has nothing to say.
    for (const c of cand.values()) {
      const ta = colType.get(c.aTable + '|' + c.aCol), tb = colType.get(c.bTable + '|' + c.bCol);
      if (ta && tb && !siTypeGate(ta, tb)) c.typeClash = true;
      if (ta && tb && siStem(c.aCol, prefixMap[c.aTable]) === siStem(c.bCol, prefixMap[c.bTable])) {
        c.nameSim = Math.max(c.nameSim, 1);
      }
    }
    const candidates = [...cand.values()].filter(c => !c.typeClash);

    // Orientation: the parent is the unique side. Catalog evidence first;
    // probes for the rest, budgeted.
    say('Verifying candidate keys…');
    const uniqueness = new Map();   // 'table|col' → verdict
    // Identity counts as unique without a probe: SQL Server will not repeat
    // one unless someone forced it, and each probe is a real table scan.
    const isUnique = (t, col) => {
      const lk = t + '|' + String(col).toLowerCase();
      const ev = keyEv.get(lk);
      if (ev === 'unique_index' || ev === 'identity') return true;
      const v = uniqueness.get(lk);
      return v ? v.unique : null;   // null = unknown
    };
    const probeQueue = [];
    for (const c of candidates) {
      for (const side of [[c.aTable, c.aCol], [c.bTable, c.bCol]]) {
        const lk = side[0] + '|' + side[1];
        if (keyEv.get(lk) || uniqueness.has(lk)) continue;
        if (!probeQueue.some(p => p.lk === lk)) probeQueue.push({ lk, table: side[0], col: side[1] });
      }
    }
    // Code-backed pairs probe first — they are likeliest to publish.
    probeQueue.sort((a, b) => a.lk.localeCompare(b.lk));
    const codeCols = new Set();
    for (const c of candidates) if (c.codeOccurrences) { codeCols.add(c.aTable + '|' + c.aCol); codeCols.add(c.bTable + '|' + c.bCol); }
    probeQueue.sort((a, b) => (codeCols.has(b.lk) ? 1 : 0) - (codeCols.has(a.lk) ? 1 : 0) || a.lk.localeCompare(b.lk));
    for (const p of probeQueue.slice(0, caps.keyProbes)) {
      const node = graph.byKey[p.table];
      if (!node || node.rowCount === 0) { uniqueness.set(p.lk, { unique: false, empty: true }); continue; }
      try {
        const res = await dbCall({ action: 'execute', sql: siKeyProbeSql(p.table, p.col, caps.probeTop) });
        uniqueness.set(p.lk, siKeyVerdict(rows(res)[0]));
        stats.probes++;
      } catch (e) {
        uniqueness.set(p.lk, { unique: null, error: (e && e.message) || 'probe failed' });
      }
    }
    if (probeQueue.length > caps.keyProbes) {
      notes.push((probeQueue.length - caps.keyProbes) + ' key probes skipped (cap ' + caps.keyProbes + ')');
    }

    // Orient each candidate; drop nothing yet — an unoriented pair publishes
    // as hypothesis with the reason on it.
    const oriented = [];
    for (const c of candidates) {
      const uA = isUnique(c.aTable, c.aCol), uB = isUnique(c.bTable, c.bCol);
      let child, childCol, parent, parentCol, noUniqueSide = false, multiplicity;
      if (uB && !uA) { child = c.aTable; childCol = c.aCol; parent = c.bTable; parentCol = c.bCol; }
      else if (uA && !uB) { child = c.bTable; childCol = c.bCol; parent = c.aTable; parentCol = c.aCol; }
      else if (uA && uB) { child = c.aTable; childCol = c.aCol; parent = c.bTable; parentCol = c.bCol; }
      else {
        // Neither side is provably unique: junction table, shared code, or
        // just unprobed. Not an FK claim — a hypothesis with a reason.
        child = c.aTable; childCol = c.aCol; parent = c.bTable; parentCol = c.bCol;
        noUniqueSide = true;
      }
      multiplicity = noUniqueSide ? 'unknown' : siMultiplicity(uA && uB ? true : false, true);
      const parentNode = graph.byKey[parent];
      oriented.push(Object.assign({}, c, {
        child, childCol, parent, parentCol, noUniqueSide, multiplicity,
        degenerateParent: !!(parentNode && parentNode.rowCount > 0 && parentNode.rowCount < 20),
        flaggyChild: siFlaggy(colType.get(child + '|' + childCol) || ''),
        identityPair: !!(colIdent.get(child + '|' + childCol) && colIdent.get(parent + '|' + parentCol)),
        genericName: GENERIC_STEMS.has(siStem(childCol, prefixMap[child])) && !c.codeOccurrences,
        cardinalityOk: !noUniqueSide,
      }));
    }

    // Channel C: confirm the top-ranked pairs, best parents per child column.
    say('Confirming with containment tests…');
    const pre = (c) => siScore(c);
    oriented.sort((a, b) => pre(b) - pre(a)
      || (a.child + a.childCol + a.parent).localeCompare(b.child + b.childCol + b.parent));
    const perChild = new Map();
    let confirmed = 0;
    for (const c of oriented) {
      if (confirmed >= caps.confirmPairs) break;
      const ck = c.child + '|' + c.childCol;
      if ((perChild.get(ck) || 0) >= caps.parentsPerChildCol) continue;
      const childNode = graph.byKey[c.child];
      if (!childNode || childNode.rowCount === 0 || c.noUniqueSide) continue;
      try {
        const res = await dbCall({ action: 'execute',
          sql: siContainSql(c.child, c.childCol, c.parent, c.parentCol, caps.probeTop) });
        const v = siContainVerdict(rows(res)[0]);
        c.containment = v.containment;
        c.orphanRate = v.orphanRate;
        c.orphanRows = v.orphanRows;
        c.sampled = childNode.rowCount > caps.probeTop;
        confirmed++;
        stats.confirms++;
        say('Tested ' + confirmed + ' of up to ' + caps.confirmPairs + ' pairs…');
      } catch (e) {
        c.containError = (e && e.message) || 'test failed';
      }
      perChild.set(ck, (perChild.get(ck) || 0) + 1);
    }
    const untested = oriented.filter(c => c.containment == null && !c.noUniqueSide).length;
    if (untested > 0) notes.push(untested + ' candidate pairs not containment-tested (cap ' + caps.confirmPairs + ')');

    // Score, tier, publish. Deterministic order so run-to-run diffs mean
    // something.
    const relationships = oriented.map((c, i) => {
      const score = siScore(c);
      const tier = siTier(c, score);
      const evidence = [];
      if (c.codeOccurrences) evidence.push({ channel: 'code', detail: 'found in ' + c.codeOccurrences + ' module' + (c.codeOccurrences === 1 ? '' : 's'), sources: c.sources });
      if (c.containment != null) evidence.push({ channel: 'containment', detail: 'containment ' + c.containment.toFixed(4) + (c.sampled ? ' (sampled)' : '') });
      if (c.nameSim) evidence.push({ channel: 'name', detail: 'stem match after prefix normalization' });
      if (c.noUniqueSide) evidence.push({ channel: 'note', detail: 'no provably unique side — junction table or shared code?' });
      return {
        child: c.child, childCol: c.childCol, parent: c.parent, parentCol: c.parentCol,
        multiplicity: c.multiplicity, confidence: score, tier,
        orphanRate: c.orphanRate != null ? Math.round(c.orphanRate * 10000) / 10000 : null,
        orphanRows: c.orphanRows != null ? c.orphanRows : null,
        predicate: c.child + '.' + c.childCol + ' = ' + c.parent + '.' + c.parentCol,
        evidence, status: 'inferred',
      };
    }).sort((a, b) => (a.child + '|' + a.childCol + '|' + a.parent + '|' + a.parentCol)
      .localeCompare(b.child + '|' + b.childCol + '|' + b.parent + '|' + b.parentCol));

    // Duplicate parents for one child column: keep them all in the catalogue
    // (the review loop needs to see the runner-up), but only the best per
    // child column above hypothesis — two published parents for one column is
    // a contradiction, not information.
    const bestPerChild = new Map();
    for (const r of relationships) {
      if (r.tier === 'hypothesis') continue;
      const ck = r.child + '|' + r.childCol;
      const cur = bestPerChild.get(ck);
      if (!cur || r.confidence > cur.confidence) bestPerChild.set(ck, r);
    }
    for (const r of relationships) {
      const ck = r.child + '|' + r.childCol;
      if (r.tier !== 'hypothesis' && bestPerChild.get(ck) !== r) r.tier = 'hypothesis';
    }

    const tiers = { confirmed: 0, probable: 0, possible: 0, hypothesis: 0 };
    for (const r of relationships) tiers[r.tier] = (tiers[r.tier] || 0) + 1;

    return {
      ok: true, version: 1,
      generatedAt: new Date().toISOString(),
      database: graph.database || null,
      engine: 'mssql',
      stats, notes, tiers,
      relationships,
    };
  }

  // ── Persistence ───────────────────────────────────────────────────────────
  const CAT_KEY = 'cygenix_inferred_rels';
  const DEC_KEY = 'cygenix_inferred_decisions';
  const storeKey = (side, database) => side + '|' + (database || 'unknown');

  function siSaveCatalogue(side, database, cat) {
    try {
      const all = JSON.parse(localStorage.getItem(CAT_KEY) || '{}');
      all[storeKey(side, database)] = cat;
      localStorage.setItem(CAT_KEY, JSON.stringify(all));
    } catch {}
  }
  function siLoadCatalogue(side, database) {
    try {
      const all = JSON.parse(localStorage.getItem(CAT_KEY) || '{}');
      return all[storeKey(side, database)] || null;
    } catch { return null; }
  }
  function siClearCatalogue(side, database) {
    try {
      const all = JSON.parse(localStorage.getItem(CAT_KEY) || '{}');
      delete all[storeKey(side, database)];
      localStorage.setItem(CAT_KEY, JSON.stringify(all));
    } catch {}
  }
  function siLoadDecisions(side, database) {
    try {
      const all = JSON.parse(localStorage.getItem(DEC_KEY) || '{}');
      return all[storeKey(side, database)] || { accept: [], reject: [] };
    } catch { return { accept: [], reject: [] }; }
  }
  function siSaveDecisions(side, database, dec) {
    try {
      const all = JSON.parse(localStorage.getItem(DEC_KEY) || '{}');
      all[storeKey(side, database)] = dec;
      localStorage.setItem(DEC_KEY, JSON.stringify(all));
    } catch {}
  }

  const api = {
    CAPS, GENERIC_STEMS,
    siPrefixOf, siPrefixMap, siStem, siObsolete,
    siParseType, siTypeGate, siKeyable, siFlaggy,
    siStripSql, siParseJoins, siCodeJoins,
    siModulesSql, siModuleCountSql, siUniqueColsSql,
    siKeyProbeSql, siKeyVerdict, siContainSql, siContainVerdict,
    siScore, siTier, siMultiplicity,
    siApplyDecisions, siToEdges, siRun,
    siSaveCatalogue, siLoadCatalogue, siClearCatalogue,
    siLoadDecisions, siSaveDecisions,
  };

  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (typeof window !== 'undefined') window.CygenixInferSchema = api;
})();
