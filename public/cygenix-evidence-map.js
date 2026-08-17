/* cygenix-evidence-map.js — evidence-based auto-mapping.
 *
 * Name matching guesses; this measures. Both tables are profiled from a
 * sample of their real values — cardinality, format fingerprints, length
 * and type distributions, null rates — and, when the target already holds
 * data, the actual value overlap between candidate column pairs. Every
 * proposal carries its reasoning in plain words:
 *
 *   cust_ref → client_id   97% of sampled values appear in the target;
 *                          both 8-char alphanumeric; both effectively unique
 *
 * Name similarity is still a signal — just one of five, and the weakest.
 * That is what catches the mappings name-matching misses (the well-named
 * column holding the wrong data, the badly-named one holding the right
 * data), which is where migrations quietly go wrong.
 *
 * Read-only: the orchestrator issues two TOP-N SELECTs through the same
 * db-connect path everything else uses. Pure core first (no DOM, no
 * network) so the test suite loads this file directly.
 */

(function (root) {
  'use strict';

  // ── Format fingerprints ───────────────────────────────────────────────────
  // 'AB-1234' → 'A{2}-9{4}'. Letters collapse to A, digits to 9, whitespace
  // to _, punctuation stays itself. Case is not distinguished — it rarely
  // discriminates and doubles the pattern space.

  function emFingerprint(v) {
    let out = '', last = '', run = 0;
    const flush = () => { if (run) out += run > 1 ? last + '{' + run + '}' : last; run = 0; };
    for (const ch of String(v)) {
      const cls = /[a-zA-Z]/.test(ch) ? 'A' : /[0-9]/.test(ch) ? '9' : /\s/.test(ch) ? '_' : ch;
      if (cls === last) run++;
      else { flush(); last = cls; run = 1; }
    }
    flush();
    return out;
  }

  // A human phrase for a dominant pattern, for the reasoning strings.
  function emDescribePattern(pat, lenAvg) {
    if (!pat) return '';
    if (/^[A9{}\d]+$/.test(pat)) {
      const letters = /A/.test(pat), digits = /9/.test(pat);
      const len = Math.round(lenAvg || 0);
      if (letters && digits) return len + '-char alphanumeric';
      if (digits) return len + '-digit number';
      if (letters) return len + '-char text';
    }
    return 'pattern ' + pat;
  }

  // ── Value classification ──────────────────────────────────────────────────

  const GUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const ISO_RE  = /^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?([+-]\d{2}:?\d{2}|Z)?)?$/;
  const DMY_RE  = /^\d{1,2}[\/.-]\d{1,2}[\/.-]\d{4}/;

  function classify(v) {
    if (typeof v === 'number') return Number.isInteger(v) ? 'int' : 'decimal';
    if (typeof v === 'boolean') return 'bool';
    if (v instanceof Date) return 'date';
    const s = String(v).trim();
    if (!s) return 'empty';
    if (/^-?\d+$/.test(s)) return 'int';
    if (/^-?(\d+\.\d*|\.\d+)$/.test(s)) return 'decimal';
    if (ISO_RE.test(s) || DMY_RE.test(s)) return 'date';
    if (GUID_RE.test(s)) return 'guid';
    if (/^(true|false|0|1|y|n|yes|no)$/i.test(s) && s.length <= 5) return 'boolish';
    return 'text';
  }

  // ── Column profiling ──────────────────────────────────────────────────────

  const SET_CAP = 800;

  function emProfile(values) {
    const p = { n: values.length, nonNull: 0, nullRate: 0, distinct: 0, distinctRatio: 0,
                typeGuess: 'empty', lenMin: Infinity, lenMax: 0, lenAvg: 0,
                patterns: [], set: new Set() };
    const typeCounts = {}, patCounts = {};
    let lenSum = 0;
    for (const v of values) {
      if (v === null || v === undefined || v === '') continue;
      p.nonNull++;
      const s = String(v).trim();
      lenSum += s.length;
      if (s.length < p.lenMin) p.lenMin = s.length;
      if (s.length > p.lenMax) p.lenMax = s.length;
      const t = classify(v);
      typeCounts[t] = (typeCounts[t] || 0) + 1;
      const pat = emFingerprint(s);
      patCounts[pat] = (patCounts[pat] || 0) + 1;
      if (p.set.size < SET_CAP) p.set.add(s.toLowerCase());
    }
    if (!p.nonNull) { p.lenMin = 0; return p; }
    p.nullRate = (p.n - p.nonNull) / p.n;
    p.distinct = p.set.size;
    p.distinctRatio = Math.min(1, p.set.size / Math.min(p.nonNull, SET_CAP));
    p.lenAvg = lenSum / p.nonNull;
    // Dominant type: ints inside a decimal column stay decimal; boolish
    // only wins outright, otherwise it is just short text.
    const order = Object.entries(typeCounts).sort((a, b) => b[1] - a[1]);
    let guess = order[0][0];
    if (guess === 'int' && typeCounts.decimal) guess = 'decimal';
    if (guess === 'boolish' && order.length > 1) guess = 'text';
    p.typeGuess = guess;
    p.patterns = Object.entries(patCounts).sort((a, b) => b[1] - a[1]).slice(0, 3)
      .map(([pat, cnt]) => ({ pat, share: cnt / p.nonNull }));
    return p;
  }

  // ── Overlap ───────────────────────────────────────────────────────────────
  // Containment of the source sample in the target sample. Only meaningful
  // when the target actually holds data — an empty target proves nothing.

  function emOverlap(srcProfile, tgtProfile) {
    if (!srcProfile.set.size || !tgtProfile.set.size) return null;
    let hits = 0;
    for (const v of srcProfile.set) if (tgtProfile.set.has(v)) hits++;
    return hits / srcProfile.set.size;
  }

  // ── Name affinity ─────────────────────────────────────────────────────────
  // One signal among five, deliberately. Tokenised (snake and camel),
  // common abbreviations expanded, scored by token overlap with a bigram
  // fallback for typo-close names.

  const ABBREV = {
    cust: 'customer', ref: 'reference', no: 'number', num: 'number', nbr: 'number',
    addr: 'address', tel: 'telephone', qty: 'quantity', amt: 'amount', desc: 'description',
    dt: 'date', dob: 'birthdate', fname: 'firstname', lname: 'lastname', org: 'organisation',
    acct: 'account', bal: 'balance', pct: 'percent', cd: 'code', nm: 'name', usr: 'user',
  };

  function tokens(name) {
    return String(name || '')
      .replace(/([a-z])([A-Z])/g, '$1 $2')
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter(Boolean)
      .map(t => ABBREV[t] || t)
      .map(t => t.replace(/s$/, ''));            // crude singular
  }

  function bigrams(s) {
    const out = new Set();
    for (let i = 0; i < s.length - 1; i++) out.add(s.slice(i, i + 2));
    return out;
  }

  function emNameScore(a, b) {
    const ta = tokens(a), tb = tokens(b);
    if (!ta.length || !tb.length) return 0;
    const ja = ta.join(''), jb = tb.join('');
    if (ja === jb) return 1;
    const sa = new Set(ta), sb = new Set(tb);
    let inter = 0;
    for (const t of sa) if (sb.has(t)) inter++;
    const jaccard = inter / (sa.size + sb.size - inter);
    const ba = bigrams(ja), bb = bigrams(jb);
    let bi = 0;
    for (const g of ba) if (bb.has(g)) bi++;
    const dice = ba.size + bb.size ? (2 * bi) / (ba.size + bb.size) : 0;
    const contains = ja.includes(jb) || jb.includes(ja) ? 0.55 : 0;
    return Math.max(jaccard, dice * 0.9, contains);
  }

  // ── Type compatibility ────────────────────────────────────────────────────
  // The VALUES' type guess against the target column's declared type.
  // 1 aligned · 0.5 castable · 0 a clash the insert will fight.

  function tgtFamily(typeStr) {
    const base = String(typeStr || '').toUpperCase().replace(/\(.*$/, '').trim();
    if (/^(N?VAR)?CHAR$|^N?TEXT$/.test(base)) return 'text';
    if (/^(TINY|SMALL|BIG)?INT$/.test(base)) return 'int';
    if (/^(DECIMAL|NUMERIC|DEC|MONEY|SMALLMONEY|FLOAT|REAL)$/.test(base)) return 'decimal';
    if (/^(DATE|DATETIME2?|SMALLDATETIME|DATETIMEOFFSET|TIME)$/.test(base)) return 'date';
    if (base === 'BIT') return 'bool';
    if (base === 'UNIQUEIDENTIFIER') return 'guid';
    return 'other';
  }

  function emTypeCompat(guess, tgtType) {
    const t = tgtFamily(tgtType);
    if (t === 'other' || guess === 'empty') return 0.5;
    if (t === 'text') return 1;                               // strings take anything
    const g = guess === 'boolish' ? 'bool' : guess;
    if (g === t) return 1;
    if (g === 'int' && t === 'decimal') return 1;
    if (g === 'decimal' && t === 'int') return 0.5;           // loses fractions but casts
    if (g === 'int' && t === 'bool') return 0.5;
    return 0;                                                 // text→int, text→date, etc.
  }

  // ── Pair scoring ──────────────────────────────────────────────────────────

  function patternSimilarity(a, b) {
    if (!a.patterns.length || !b.patterns.length) return null;
    // Share-weighted agreement of the pattern distributions…
    let sim = 0;
    for (const pa of a.patterns) {
      const pb = b.patterns.find(x => x.pat === pa.pat);
      if (pb) sim += Math.min(pa.share, pb.share);
    }
    // …blended with average-length proximity, so '8-char' meets '8-char'.
    const lenSim = 1 - Math.min(1, Math.abs(a.lenAvg - b.lenAvg) / Math.max(a.lenAvg, b.lenAvg, 1));
    return sim * 0.7 + lenSim * 0.3;
  }

  function emScorePair(srcName, srcProfile, tgtCol, tgtProfile) {
    const reasons = [];
    const comp = {};

    comp.name = emNameScore(srcName, tgtCol.name);
    comp.type = emTypeCompat(srcProfile.typeGuess, tgtCol.type);
    comp.pattern = patternSimilarity(srcProfile, tgtProfile && tgtProfile.nonNull ? tgtProfile : srcProfile);
    if (!(tgtProfile && tgtProfile.nonNull)) comp.pattern = null;   // nothing to compare against
    comp.cardinality = (tgtProfile && tgtProfile.nonNull)
      ? 1 - Math.abs(srcProfile.distinctRatio - tgtProfile.distinctRatio) : null;
    comp.overlap = tgtProfile ? emOverlap(srcProfile, tgtProfile) : null;

    // Weights: measured evidence first; name last. Unavailable components
    // hand their weight to the ones that exist — and when the measured
    // overlap is strong, the name drops out entirely: a renamed column is
    // precisely the case this exists to catch, and a zero name score must
    // not hold down what the data has already proven.
    const W = { overlap: 45, pattern: 18, type: 12, cardinality: 8, name: 17 };
    if (comp.overlap !== null && comp.overlap >= 0.5) W.name = 0;
    let wsum = 0, acc = 0;
    for (const k of Object.keys(W)) {
      if (comp[k] === null || comp[k] === undefined || !W[k]) continue;
      wsum += W[k]; acc += comp[k] * W[k];
    }
    let score = wsum ? Math.round((acc / wsum) * 100) : 0;
    if (comp.type === 0) score = Math.min(score, 25);          // a clash caps everything

    // Reasoning — only what actually contributed.
    if (comp.overlap !== null && comp.overlap >= 0.3) {
      reasons.push(Math.round(comp.overlap * 100) + '% of sampled source values appear in the target data');
    }
    const sp = srcProfile.patterns[0], tp = tgtProfile && tgtProfile.patterns && tgtProfile.patterns[0];
    if (sp && tp && sp.pat === tp.pat && sp.share >= 0.6 && tp.share >= 0.6) {
      reasons.push('both ' + emDescribePattern(sp.pat, (srcProfile.lenAvg + tgtProfile.lenAvg) / 2));
    }
    if (comp.cardinality !== null && srcProfile.distinctRatio >= 0.95 && tgtProfile.distinctRatio >= 0.95) {
      reasons.push('both effectively unique');
    } else if (comp.cardinality !== null && srcProfile.distinctRatio <= 0.2 && tgtProfile.distinctRatio <= 0.2) {
      reasons.push('both low-cardinality code sets');
    }
    if (comp.type === 0) reasons.push('⚠ type clash: sampled ' + srcProfile.typeGuess + ' values against ' + tgtCol.type);
    else if (comp.type === 0.5) reasons.push('castable with care (' + srcProfile.typeGuess + ' → ' + tgtCol.type + ')');
    if (comp.name >= 0.85) reasons.push('names match');
    else if (comp.name >= 0.5) reasons.push('names similar');
    if (!reasons.length) reasons.push('weak evidence — review by hand');

    return { score, reasons, components: comp };
  }

  // ── Proposal assembly ─────────────────────────────────────────────────────
  // Greedy best-first over all candidate pairs: highest score claims its
  // pair, each source and target used once. Emits a row for EVERY target
  // column (editor contract), identity columns locked exactly as autoMap
  // does. minScore below which no candidate is credible: 35.

  function emGrade(score) { return score >= 80 ? 'HIGH' : score >= 55 ? 'MEDIUM' : score >= 35 ? 'LOW' : ''; }

  function emPropose(srcProfiles, tgtCols, tgtProfiles, opts) {
    opts = opts || {};
    const minScore = opts.minScore || 35;
    const pairs = [];
    const srcNames = Object.keys(srcProfiles);
    for (const tc of tgtCols) {
      if (tc.isIdentity) continue;
      for (const sn of srcNames) {
        const r = emScorePair(sn, srcProfiles[sn], tc, (tgtProfiles || {})[tc.name]);
        if (r.score >= minScore) pairs.push({ srcCol: sn, tgtCol: tc.name, ...r });
      }
    }
    pairs.sort((a, b) => b.score - a.score);
    const usedSrc = new Set(), usedTgt = new Set(), chosen = {};
    for (const p of pairs) {
      if (usedSrc.has(p.srcCol) || usedTgt.has(p.tgtCol)) continue;
      usedSrc.add(p.srcCol); usedTgt.add(p.tgtCol);
      chosen[p.tgtCol] = p;
    }
    return tgtCols.map(tc => {
      if (tc.isIdentity) {
        return { srcCol: '', tgtCol: tc.name, transform: 'NONE', match: '',
                 _isIdentity: true, _identityOverride: false };
      }
      const p = chosen[tc.name];
      if (!p) return { srcCol: '', tgtCol: tc.name, transform: 'NONE', match: '',
                       evidence: { score: 0, reasons: ['no credible candidate in the source sample'] } };
      return { srcCol: p.srcCol, tgtCol: tc.name, transform: 'NONE', match: emGrade(p.score),
               evidence: { score: p.score, reasons: p.reasons } };
    });
  }

  // ── SQL + orchestrator ────────────────────────────────────────────────────

  function q(name) { return '[' + String(name).replace(/]/g, ']]') + ']'; }

  function emSampleSql(table, cols, n) {
    return 'SELECT TOP (' + (n | 0) + ') ' + cols.map(q).join(', ') + ' FROM ' + table;
  }

  // srcTable/tgtTable: { fullName, columns:[{name,type,isIdentity,...}] }.
  // Profiles both sides from TOP-N samples and returns proposals + the
  // profiles, so the caller can show its working.
  async function emRun(srcTable, tgtTable, opts) {
    const { srcConn, tgtConn, dbCall } = opts;
    const n = opts.sampleSize || 400;
    const colName = c => (typeof c === 'string' ? c : c.name);
    const srcCols = (srcTable.columns || []).map(colName);
    const tgtCols = (tgtTable.columns || []).map(c => (typeof c === 'string' ? { name: c, type: '' } : c));

    const srcR = await dbCall(srcConn, { action: 'execute', sql: emSampleSql(srcTable.fullName || srcTable.name, srcCols, n) });
    const srcRows = srcR.recordset || [];
    let tgtRows = [];
    try {
      const tgtR = await dbCall(tgtConn, { action: 'execute', sql: emSampleSql(tgtTable.fullName || tgtTable.name, tgtCols.map(c => c.name), n) });
      tgtRows = tgtR.recordset || [];
    } catch (e) { /* unreadable target (permissions, view) — schema-only evidence */ }

    const srcProfiles = {}, tgtProfiles = {};
    for (const c of srcCols) srcProfiles[c] = emProfile(srcRows.map(r => r[c]));
    for (const c of tgtCols) tgtProfiles[c.name] = emProfile(tgtRows.map(r => r[c.name]));

    const proposals = emPropose(srcProfiles, tgtCols, tgtProfiles, opts);
    return {
      proposals, srcProfiles, tgtProfiles,
      sampledSrc: srcRows.length, sampledTgt: tgtRows.length,
      tgtHasData: tgtRows.length > 0,
      generatedAt: new Date().toISOString(),
    };
  }

  const api = {
    emFingerprint, emDescribePattern, emProfile, emOverlap,
    emNameScore, emTypeCompat, emScorePair, emPropose, emGrade,
    emSampleSql, emRun,
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  root.CygenixEvidenceMap = api;
})(typeof window !== 'undefined' ? window : globalThis);
