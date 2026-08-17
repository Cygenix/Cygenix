/* cygenix-nl-recon.js — plain-English reconciliation tests.
 *
 * "Check no customer lost their opening balance" becomes a generated
 * source/target query pair, a pass/fail, and a drillable list of exactly
 * which customers differ. The validation layer opens up to the people who
 * know what the data MEANS — business analysts — not only the engineers
 * who know where it lives.
 *
 * How a phrase becomes a check:
 *   1. The phrase is matched to a migration JOB (entity words vs table
 *      and job names) — jobs carry the src↔tgt table pair, the WHERE
 *      clause, and the column mapping.
 *   2. Noun words are matched to a MAPPED COLUMN PAIR through the job's
 *      columnMapping, so "opening balance" finds opn_bal → opening_balance
 *      without anyone typing SQL.
 *   3. Verb patterns choose the KIND: row counts, total preservation,
 *      per-key value preservation, duplicates, nulls, key containment.
 *
 * Source and target are different servers, so nothing ever joins across:
 * each side gets its own read-only SELECT and the comparison happens here,
 * client-side, which is also what makes the delta list drillable.
 *
 * Pure core (parse, SQL build, compare) + thin orchestrator over dbCall —
 * the same testable pattern as the preflight and evidence-map engines.
 */

(function (root) {
  'use strict';

  // ── Tokens & fuzzy naming (aligned with cygenix-evidence-map) ─────────────

  const ABBREV = {
    cust: 'customer', ref: 'reference', no: 'number', num: 'number', nbr: 'number',
    addr: 'address', qty: 'quantity', amt: 'amount', bal: 'balance', opn: 'opening',
    acct: 'account', dt: 'date', nm: 'name', tot: 'total', val: 'value',
  };
  const STOP = new Set(['check', 'that', 'the', 'a', 'an', 'no', 'not', 'their', 'his', 'her',
    'its', 'any', 'all', 'every', 'each', 'has', 'have', 'is', 'are', 'was', 'were', 'of',
    'in', 'on', 'to', 'from', 'with', 'and', 'or', 'should', 'must', 'be', 'still', 'same',
    'lost', 'missing', 'match', 'matches', 'equal', 'equals', 'target', 'source', 'table',
    'rows', 'row', 'count', 'counts', 'number', 'sum', 'total', 'totals', 'value', 'values',
    'duplicate', 'duplicates', 'null', 'nulls', 'empty', 'blank', 'exists', 'present',
    'arrived', 'migrated', 'made', 'it', 'per', 'by', 'for', 'against', 'between', 'within', 'percent']);

  function words(s) {
    return String(s || '').toLowerCase().replace(/([a-z])([A-Z])/g, '$1 $2')
      .split(/[^a-z0-9%]+/).filter(Boolean).map(w => ABBREV[w] || w).map(w => w.replace(/s$/, ''));
  }
  function nounWords(phrase) { return words(phrase).filter(w => !STOP.has(w) && !/^\d/.test(w)); }

  function nameHit(word, name) {
    const nw = words(name);
    if (!nw.length) return 0;
    if (nw.includes(word)) return 1;
    return nw.some(t => t.startsWith(word) || word.startsWith(t)) ? 0.6 : 0;
  }
  function phraseNameScore(phraseWords, name) {
    if (!phraseWords.length) return 0;
    let s = 0;
    for (const w of phraseWords) s += nameHit(w, name);
    return s / phraseWords.length;
  }

  // ── Job & column resolution ───────────────────────────────────────────────

  function nrResolveJob(phrase, jobs) {
    const pw = nounWords(phrase);
    let best = null, bestScore = 0;
    for (const j of jobs || []) {
      if (!j.sourceTable || !j.targetTable || !(j.columnMapping || []).length) continue;
      const names = [j.sourceTable, j.targetTable, j.name || ''];
      const score = Math.max(...names.map(n => phraseNameScore(pw, n)));
      if (score > bestScore) { bestScore = score; best = j; }
    }
    // A single-job project needs no entity words at all.
    if (!best && (jobs || []).filter(j => j.sourceTable && (j.columnMapping || []).length).length === 1) {
      best = jobs.find(j => j.sourceTable && (j.columnMapping || []).length);
      bestScore = 0.3;
    }
    return best ? { job: best, score: bestScore } : null;
  }

  function mappedPairs(job) {
    return (job.columnMapping || [])
      .filter(m => m && m.srcCol && m.tgtCol && !m._isIdentity)
      .map(m => ({ srcCol: m.srcCol, tgtCol: m.tgtCol }));
  }

  function nrResolveColumn(phrase, job) {
    const pw = Array.isArray(phrase) ? phrase : nounWords(phrase);
    if (!pw.length) return null;
    let best = null, bestScore = 0.34;              // floor: a hit needs substance
    for (const p of mappedPairs(job)) {
      const s = Math.max(phraseNameScore(pw, p.srcCol), phraseNameScore(pw, p.tgtCol));
      if (s > bestScore) { bestScore = s; best = p; }
    }
    return best ? { ...best, score: bestScore } : null;
  }

  // The entity key for per-key checks and containment: a mapped pair whose
  // name matches the entity word, preferring id/ref/number-shaped names,
  // falling back to the first id-like mapped column.
  function nrResolveKey(phrase, job) {
    const pw = nounWords(phrase);
    const idLike = (n) => /(^|_)(id|ref|reference|number|no|key|code)$/i.test(String(n).trim()) || /^id$/i.test(String(n).trim());
    const pairs = mappedPairs(job);
    let best = null, bestScore = 0;
    for (const p of pairs) {
      let s = Math.max(phraseNameScore(pw, p.srcCol), phraseNameScore(pw, p.tgtCol));
      if (idLike(p.srcCol) || idLike(p.tgtCol)) s += 0.5;
      if (s > bestScore) { bestScore = s; best = p; }
    }
    return best;
  }

  // ── Parsing ───────────────────────────────────────────────────────────────
  // Returns { ok:true, spec } or { ok:false, error, hint }.

  const KIND_LABEL = {
    rowcount: 'row counts match',
    total: 'total is preserved',
    perkey: 'value preserved per key',
    duplicates: 'no duplicates in target',
    nulls: 'no new NULLs in target',
    exists: 'every source key reaches the target',
  };

  function nrParse(phrase, jobs) {
    const p = String(phrase || '').trim();
    if (!p) return { ok: false, error: 'Empty phrase', hint: 'Describe a check, e.g. "no customer lost their opening balance".' };
    const lower = ' ' + p.toLowerCase() + ' ';

    const jr = nrResolveJob(p, jobs);
    if (!jr) return { ok: false, error: 'No migration job matches that phrase', hint: 'Mention the table or entity a job migrates — the checks run against a job\'s source→target pair.' };
    const job = jr.job;

    const tolM = /within\s+(\d+(?:\.\d+)?)\s*(%|percent)/i.exec(p);
    const tolPct = tolM ? parseFloat(tolM[1]) / 100 : 0;

    let kind = null;
    // The words that identified the JOB ("customers") must not double as
    // the measured column — otherwise "row counts match for customers"
    // reads cust_ref as a value column and stops being a rowcount check.
    // The key resolver still sees them: the entity word IS the key hint.
    const pw = nounWords(p);
    const entityHit = (w) => [job.sourceTable, job.targetTable, job.name || '']
      .some(n => nameHit(w, n) >= 0.6);
    const colWords = pw.filter(w => !entityHit(w));
    let col = nrResolveColumn(colWords, job);
    // "totals match" names no column — the intent is the job's amount-like
    // column (net_amt, balance, total_value…). Only a confident hit counts;
    // otherwise the phrase honestly degrades to a rowcount check.
    if (!col && /\b(total|sum)s?\b/i.test(p)) {
      const MONEY = ['amount', 'total', 'value', 'balance', 'net', 'gross', 'price', 'cost', 'quantity'];
      let best = null, bestScore = 0.5;
      for (const pr of mappedPairs(job)) {
        const s = Math.max(...MONEY.map(w => Math.max(nameHit(w, pr.srcCol), nameHit(w, pr.tgtCol))));
        if (s > bestScore) { bestScore = s; best = pr; }
      }
      col = best;
    }
    const perEntity = /\b(each|every|per|any|no)\s+\w+/i.test(p);

    if (/duplicat/i.test(lower)) kind = 'duplicates';
    else if (/(no|without|never)\s+(missing|null|empty|blank)/.test(lower) || /\bnulls?\b/.test(lower)) kind = 'nulls';
    else if (/(exist|present|found|reach|in the target|made it)/.test(lower) && !col) kind = 'exists';
    else if (col && perEntity) kind = 'perkey';
    else if (col) kind = 'total';
    else if (/(lost|missing|dropped|same (number|count)|counts? match|all .*(arrive|migrate)|row ?count)/.test(lower)) kind = 'rowcount';
    else if (/(lost|preserved|match|same|equal)/.test(lower)) kind = 'rowcount';

    if (!kind) return { ok: false, error: 'Could not work out what to compare', hint: 'Phrasings that work: "row counts match", "no customer lost their opening balance", "invoice totals match within 0.5%", "no duplicates on invoice number", "every order exists in the target".' };

    if ((kind === 'total' || kind === 'perkey' || kind === 'duplicates' || kind === 'nulls') && !col
        && kind !== 'duplicates' && kind !== 'nulls') {
      return { ok: false, error: 'Could not find that column on the job\'s mapping', hint: 'Use words from the mapped column names — the job maps: ' + mappedPairs(job).map(x => x.srcCol).join(', ') };
    }

    const key = (kind === 'perkey' || kind === 'exists' || kind === 'duplicates') ? nrResolveKey(p, job) : null;
    if (kind === 'perkey' && (!key || (col && key.srcCol === col.srcCol))) {
      // No usable grouping key distinct from the measured column — degrade
      // honestly to a total check rather than inventing a key.
      kind = 'total';
    }
    if ((kind === 'exists' || kind === 'duplicates') && !key && !col) {
      return { ok: false, error: 'Could not find a key column for that check', hint: 'Name the column, e.g. "no duplicates on invoice number".' };
    }

    const spec = {
      phrase: p, kind, label: KIND_LABEL[kind],
      jobId: job.id, jobName: job.name || '',
      srcTable: job.sourceTable, tgtTable: job.targetTable,
      srcWhere: (job.srcWhere || '').trim().replace(/^WHERE\s+/i, ''),
      col: col ? { srcCol: col.srcCol, tgtCol: col.tgtCol } : null,
      key: kind === 'duplicates' ? (col ? { srcCol: col.srcCol, tgtCol: col.tgtCol } : key && { srcCol: key.srcCol, tgtCol: key.tgtCol })
         : key ? { srcCol: key.srcCol, tgtCol: key.tgtCol } : null,
      tolPct,
    };
    return { ok: true, spec };
  }

  // ── SQL generation (read-only, one statement per side) ───────────────────

  function q(n) { return '[' + String(n).replace(/]/g, ']]') + ']'; }
  const wh = (spec, forSrc) => (forSrc && spec.srcWhere) ? ' WHERE ' + spec.srcWhere : '';

  const GROUP_CAP = 5000, DRILL_CAP = 500;

  function nrBuildSql(spec) {
    switch (spec.kind) {
      case 'rowcount':
        return { srcSql: 'SELECT COUNT_BIG(*) AS v FROM ' + spec.srcTable + wh(spec, true),
                 tgtSql: 'SELECT COUNT_BIG(*) AS v FROM ' + spec.tgtTable };
      case 'total':
        return { srcSql: 'SELECT SUM(CAST(' + q(spec.col.srcCol) + ' AS FLOAT)) AS v FROM ' + spec.srcTable + wh(spec, true),
                 tgtSql: 'SELECT SUM(CAST(' + q(spec.col.tgtCol) + ' AS FLOAT)) AS v FROM ' + spec.tgtTable };
      case 'perkey':
        return { srcSql: 'SELECT TOP (' + GROUP_CAP + ') ' + q(spec.key.srcCol) + ' AS k, SUM(CAST(' + q(spec.col.srcCol) + ' AS FLOAT)) AS v FROM '
                   + spec.srcTable + wh(spec, true) + ' GROUP BY ' + q(spec.key.srcCol) + ' ORDER BY k',
                 tgtSql: 'SELECT TOP (' + GROUP_CAP + ') ' + q(spec.key.tgtCol) + ' AS k, SUM(CAST(' + q(spec.col.tgtCol) + ' AS FLOAT)) AS v FROM '
                   + spec.tgtTable + ' GROUP BY ' + q(spec.key.tgtCol) + ' ORDER BY k' };
      case 'duplicates':
        return { srcSql: 'SELECT COUNT_BIG(*) AS v FROM (SELECT ' + q(spec.key.srcCol) + ' AS k FROM ' + spec.srcTable + wh(spec, true)
                   + ' GROUP BY ' + q(spec.key.srcCol) + ' HAVING COUNT(*) > 1) d',
                 tgtSql: 'SELECT TOP (' + DRILL_CAP + ') ' + q(spec.key.tgtCol) + ' AS k, COUNT(*) AS v FROM ' + spec.tgtTable
                   + ' GROUP BY ' + q(spec.key.tgtCol) + ' HAVING COUNT(*) > 1 ORDER BY v DESC' };
      case 'nulls': {
        const c = spec.col || spec.key;
        return { srcSql: 'SELECT COUNT_BIG(*) AS v FROM ' + spec.srcTable + ' WHERE ' + q(c.srcCol) + ' IS NULL' + (spec.srcWhere ? ' AND (' + spec.srcWhere + ')' : ''),
                 tgtSql: 'SELECT COUNT_BIG(*) AS v FROM ' + spec.tgtTable + ' WHERE ' + q(c.tgtCol) + ' IS NULL' };
      }
      case 'exists':
        // Distinct source keys come back here; the target probe is built
        // from them afterwards (different servers — no cross-join exists).
        return { srcSql: 'SELECT DISTINCT TOP (' + DRILL_CAP + ') ' + q(spec.key.srcCol) + ' AS k FROM ' + spec.srcTable
                   + wh(spec, true) + (spec.srcWhere ? ' AND ' : ' WHERE ') + q(spec.key.srcCol) + ' IS NOT NULL',
                 tgtSql: null };
      default:
        throw new Error('unknown check kind ' + spec.kind);
    }
  }

  function sqlLit(v) {
    if (typeof v === 'number' && isFinite(v)) return String(v);
    return "N'" + String(v).replace(/'/g, "''") + "'";
  }
  function nrExistsProbeSql(spec, keys) {
    const rows = keys.slice(0, DRILL_CAP).map(k => '(' + sqlLit(k) + ')').join(',');
    return 'SELECT v.val AS k FROM (VALUES ' + rows + ') v(val) '
      + 'WHERE NOT EXISTS (SELECT 1 FROM ' + spec.tgtTable + ' t WHERE t.' + q(spec.key.tgtCol) + ' = v.val)';
  }

  // ── Comparison ────────────────────────────────────────────────────────────

  function scalar(rs) { const r = (rs && rs[0]) || {}; const v = r.v !== undefined ? r.v : r.V; return v === null || v === undefined ? 0 : Number(v); }
  function close(a, b, tolPct) {
    const tol = Math.max(Math.abs(a), Math.abs(b)) * (tolPct || 0) + 0.005;   // float-noise allowance on sums
    return Math.abs(a - b) <= tol;
  }
  const fmt = (n) => Number(n).toLocaleString('en-GB', { maximumFractionDigits: 2 });

  function nrCompare(spec, srcRows, tgtRows) {
    switch (spec.kind) {
      case 'rowcount': case 'total': case 'nulls': {
        const s = scalar(srcRows), t = scalar(tgtRows);
        const pass = spec.kind === 'nulls' ? t <= s
          : spec.kind === 'rowcount' && !spec.tolPct ? s === t
          : close(s, t, spec.tolPct);
        return { pass, srcVal: s, tgtVal: t, delta: t - s,
          headline: (pass ? '✓ ' : '✗ ') + 'source ' + fmt(s) + ' vs target ' + fmt(t)
            + (pass ? '' : ' (Δ ' + fmt(t - s) + ')'), deltas: [] };
      }
      case 'perkey': {
        const sm = new Map(), tm = new Map();
        (srcRows || []).forEach(r => sm.set(String(r.k), Number(r.v) || 0));
        (tgtRows || []).forEach(r => tm.set(String(r.k), Number(r.v) || 0));
        const deltas = [];
        for (const [k, sv] of sm) {
          const tv = tm.has(k) ? tm.get(k) : null;
          if (tv === null) deltas.push({ key: k, src: sv, tgt: null, note: 'missing from target' });
          else if (!close(sv, tv, spec.tolPct)) deltas.push({ key: k, src: sv, tgt: tv, note: 'Δ ' + fmt(tv - sv) });
        }
        for (const [k, tv] of tm) if (!sm.has(k)) deltas.push({ key: k, src: null, tgt: tv, note: 'not in source' });
        const truncated = (srcRows || []).length >= GROUP_CAP || (tgtRows || []).length >= GROUP_CAP;
        return { pass: deltas.length === 0, deltas: deltas.slice(0, 200), deltaCount: deltas.length, truncated,
          headline: deltas.length === 0
            ? '✓ all ' + fmt(sm.size) + ' keys agree' + (truncated ? ' (first ' + GROUP_CAP + ' groups compared)' : '')
            : '✗ ' + fmt(deltas.length) + ' of ' + fmt(sm.size) + ' keys differ' };
      }
      case 'duplicates': {
        const dupes = (tgtRows || []).map(r => ({ key: String(r.k), tgt: Number(r.v), note: Number(r.v) + '×' }));
        const srcDupes = scalar(srcRows);
        return { pass: dupes.length === 0, deltas: dupes, deltaCount: dupes.length,
          headline: dupes.length === 0 ? '✓ no duplicate keys in the target'
            : '✗ ' + fmt(dupes.length) + ' duplicated key value(s) in the target'
              + (srcDupes ? ' (source already had ' + fmt(srcDupes) + ')' : '') };
      }
      case 'exists': {
        const missing = (tgtRows || []).map(r => ({ key: String(r.k), note: 'missing from target' }));
        const probed = (srcRows || []).length;
        return { pass: missing.length === 0, deltas: missing, deltaCount: missing.length, probed,
          headline: missing.length === 0
            ? '✓ all ' + fmt(probed) + ' probed source keys exist in the target'
            : '✗ ' + fmt(missing.length) + ' of ' + fmt(probed) + ' probed keys missing from the target' };
      }
      default:
        return { pass: false, headline: 'unknown kind', deltas: [] };
    }
  }

  // ── Orchestrator ──────────────────────────────────────────────────────────

  async function nrRun(spec, opts) {
    const { srcConn, tgtConn, dbCall } = opts;
    const sql = nrBuildSql(spec);
    const srcR = await dbCall(srcConn, { action: 'execute', sql: sql.srcSql });
    const srcRows = srcR.recordset || [];
    let tgtRows = [];
    if (spec.kind === 'exists') {
      const keys = srcRows.map(r => r.k).filter(k => k !== null && k !== undefined);
      if (keys.length) {
        const probeR = await dbCall(tgtConn, { action: 'execute', sql: nrExistsProbeSql(spec, keys) });
        tgtRows = probeR.recordset || [];
      }
    } else {
      const tgtR = await dbCall(tgtConn, { action: 'execute', sql: sql.tgtSql });
      tgtRows = tgtR.recordset || [];
    }
    const result = nrCompare(spec, srcRows, tgtRows);
    result.ranAt = new Date().toISOString();
    result.sql = sql;
    return result;
  }

  // Human line describing what the check will do — shown before it runs so
  // the analyst can see the interpretation matched their intent.
  function nrDescribe(spec) {
    const base = spec.srcTable + ' → ' + spec.tgtTable;
    switch (spec.kind) {
      case 'rowcount':  return 'Row counts must match: ' + base + (spec.srcWhere ? ' (source WHERE applied)' : '');
      case 'total':     return 'SUM(' + spec.col.srcCol + ') on source must equal SUM(' + spec.col.tgtCol + ') on target' + (spec.tolPct ? ' within ' + (spec.tolPct * 100) + '%' : '');
      case 'perkey':    return 'SUM(' + spec.col.srcCol + ') per ' + spec.key.srcCol + ' must match SUM(' + spec.col.tgtCol + ') per ' + spec.key.tgtCol + ' — differing keys are listed';
      case 'duplicates':return 'No duplicate ' + spec.key.tgtCol + ' values in ' + spec.tgtTable;
      case 'nulls':     return 'No more NULL ' + (spec.col || spec.key).tgtCol + ' in target than in source';
      case 'exists':    return 'Every sampled ' + spec.key.srcCol + ' from ' + spec.srcTable + ' must exist as ' + spec.key.tgtCol + ' in ' + spec.tgtTable;
      default: return spec.label;
    }
  }

  const api = { nrParse, nrResolveJob, nrResolveColumn, nrResolveKey, nrBuildSql,
                nrExistsProbeSql, nrCompare, nrRun, nrDescribe, KIND_LABEL };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  root.CygenixNLRecon = api;
})(typeof window !== 'undefined' ? window : globalThis);
