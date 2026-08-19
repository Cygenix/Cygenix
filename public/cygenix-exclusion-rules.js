/* cygenix-exclusion-rules.js — the pre-run exclusion picker's rule library.
 *
 * Measured on a live 2,545-table estate: 14% of the Migrate lane was
 * backups, test tables and dated warehouse dumps, and 53% of all candidate
 * targets held fewer than 10 rows. The picker lets an analyst scope the
 * estate down BEFORE a mapping run — in minutes, by rule, not by ticking
 * 1,751 boxes — with every exclusion reasoned, previewable, reversible and
 * auditable.
 *
 * Rules are data, not code: a table of patterns with ids, editable and
 * exportable, all OFF until ticked. Matching, counting and profile
 * application are pure functions so the whole layer is testable headlessly.
 *
 * The one rule that is NOT a table filter is the ratio guard (§6 of the
 * spec). An absolute "fewer than 10 rows" target filter looks spectacular
 * and is a trap: a third of known-correct mappings land on tiny reference
 * tables. The discriminating fact is implausible size MISMATCH — a
 * million-row source proposing a 3-row target — so the guard is relative,
 * candidate-level, and enforced inside the engine.
 */
(function () {
  'use strict';

  const XR_VERSION = 1;

  // ── Rule library (§5) — source side: name patterns ───────────────────────
  const XR_SOURCE_RULES = [
    { id: 'backup_snapshot', label: 'Backup / snapshot',
      re: '(^_?baktab)|_bak($|_)|_backup|_(19|20)\\d{6}($|_)|_[01]\\d[0-3]\\d\\d\\d$' },
    { id: 'test_temp', label: 'Test / temp / scratch',
      re: '_test($|_|\\d)|^t_|tmp|temp|scratch|_old($|_)' },
    { id: 'dated_dump', label: 'Dated warehouse dump', re: '^dw_.*\\d|^df_' },
    { id: 'masked_snr', label: 'Masked / SNR copy', re: '^mask_|^_?snr_' },
    { id: 'conversion_staging', label: 'Conversion / staging',
      re: 'conv|migration|go_?live|pre_conv' },
    { id: 'zero_row', label: 'Zero-row tables', meta: 'zeroRow' },
    { id: 'no_pk', label: 'No primary key detected', meta: 'noPk' },
  ];

  // ── Target side (§4.2): a vendor schema is clean by name ─────────────────
  // Different axes: emptiness, triage lane, licensed-module prefix.
  const XR_TARGET_RULES = [
    { id: 'tgt_empty', label: 'Empty (0 rows)', meta: 'zeroRow' },
    { id: 'tgt_low_row', label: 'Fewer than 10 rows', meta: 'lowRow', warn: true },
    { id: 'tgt_review', label: 'Triage lane: REVIEW', meta: 'reviewLane' },
    { id: 'tgt_prefix', label: 'Module / prefix', meta: 'prefix' },
  ];

  // §6: the recommended alternative to the low-row trap. Pre-ticked.
  const XR_RATIO_GUARD = {
    id: 'ratio_guard', side: 'target', recommended: true,
    params: { minSourceRows: 1000, maxRatio: 1000 },
  };

  const XR_LOW_ROW_WARNING = 'Small source tables legitimately map to small '
    + 'target tables — reference and lookup targets are supposed to be tiny, '
    + 'and nearly a third of known-correct mappings land on targets under 10 '
    + 'rows. Prefer the relative ratio guard, which removes only implausible '
    + 'size mismatches.';

  function xrRegex(src) { try { return new RegExp(src, 'i'); } catch (e) { return null; } }

  // Custom rule (§4.1): "regex or glob". A pattern that uses * or ? and no
  // regex-only metacharacters reads as a glob and is anchored; anything else
  // is treated as a raw regex, unanchored.
  function xrCompileCustom(text) {
    const t = String(text || '').trim();
    if (!t) return null;
    const globish = /[*?]/.test(t) && !/[\\()\[\]|^$+{}]/.test(t);
    const src = globish
      ? '^' + t.replace(/\./g, '\\.').replace(/\*/g, '.*').replace(/\?/g, '.') + '$'
      : t;
    const re = xrRegex(src);
    return re ? { source: src, re } : null;
  }

  // One table, one rule. `t` is the picker's minimal shape:
  // { key, name, rows, hasPk, lane } — lane is the current triage class.
  function xrRuleHits(rule, t, opts) {
    const o = opts || {};
    if (rule.custom != null) {
      const c = rule._c || (rule._c = xrCompileCustom(rule.custom));
      return c ? c.re.test(t.name) : false;
    }
    if (rule.re) {
      const re = rule._re || (rule._re = xrRegex(rule.re));
      return re ? re.test(t.name) : false;
    }
    if (rule.meta === 'zeroRow') return !(t.rows > 0);
    if (rule.meta === 'lowRow') return (t.rows || 0) < (o.lowRowUnder || 10);
    if (rule.meta === 'noPk') return !t.hasPk;
    if (rule.meta === 'reviewLane') return (t.lane || '') === 'REVIEW';
    if (rule.meta === 'prefix') {
      const pf = (rule.params && rule.params.prefixes) || [];
      const n = String(t.name).toLowerCase();
      return pf.some(p => p && n.startsWith(String(p).toLowerCase()));
    }
    return false;
  }

  // Live chip counts: tables and rows a rule would take.
  function xrCount(rule, tables, opts) {
    let n = 0, rows = 0;
    for (const t of tables) if (xrRuleHits(rule, t, opts)) { n++; rows += t.rows || 0; }
    return { n, rows };
  }

  // Module prefixes (§4.2): leading two-letter families large enough to be a
  // product module. A firm not licensing a module drops its whole family in
  // one action.
  function xrPrefixes(tables, minFamily) {
    const min = minFamily || 8;
    const fam = new Map();
    for (const t of tables) {
      const m = /^[A-Za-z]{2}/.exec(String(t.name));
      if (!m) continue;
      const kk = m[0].toLowerCase();
      const e = fam.get(kk) || { prefix: m[0], n: 0, rows: 0 };
      e.n++; e.rows += t.rows || 0;
      fam.set(kk, e);
    }
    return [...fam.values()].filter(e => e.n >= min)
      .sort((a, b) => b.n - a.n).slice(0, 12);
  }

  // ── Profile (§7) — versioned, per connection pair ────────────────────────
  function xrNewProfile(srcDb, tgtDb, by, engineVersion) {
    return {
      profileId: 'xp-' + Math.random().toString(36).slice(2, 10),
      connectionPair: { source: srcDb || '', target: tgtDb || '' },
      engineVersion: engineVersion || 0,
      version: 0, createdBy: by || '', createdAt: null, updatedAt: null,
      rules: [],                          // { id, side, enabled, params?, custom? }
      manual: { exclude: [], rescue: [] } // { key, side?, reason, by }
    };
  }

  // Apply a profile to both estates. Returns per-side Maps of
  // lowercased key → reason code ('RULE:<id>' or 'MANUAL'), plus the ratio
  // guard params if enabled. Rescue is first-class: a rescued table is out
  // of every rule's net AND out of the manual list, without editing either.
  function xrApply(profile, srcTables, tgtTables, opts) {
    const out = { src: new Map(), tgt: new Map(), ratioGuard: null };
    if (!profile) return out;
    const rescued = new Set(((profile.manual && profile.manual.rescue) || [])
      .map(r => String(r.key).toLowerCase()));
    const lib = { source: XR_SOURCE_RULES, target: XR_TARGET_RULES };
    for (const r of profile.rules || []) {
      if (!r.enabled) continue;
      if (r.id === 'ratio_guard') {
        out.ratioGuard = Object.assign({}, XR_RATIO_GUARD.params, r.params || {});
        continue;
      }
      const side = r.side === 'target' ? 'tgt' : 'src';
      const tables = side === 'src' ? srcTables : tgtTables;
      const def = (lib[r.side === 'target' ? 'target' : 'source'] || []).find(d => d.id === r.id);
      if (!def && r.custom == null) continue;
      const rule = r.custom != null ? { custom: r.custom }
        : Object.assign({}, def, { params: r.params || def.params });
      for (const t of tables) {
        const k = String(t.key).toLowerCase();
        if (rescued.has(k) || out[side].has(k)) continue;
        if (xrRuleHits(rule, t, opts)) out[side].set(k, 'RULE:' + r.id);
      }
    }
    for (const m of (profile.manual && profile.manual.exclude) || []) {
      const k = String(m.key).toLowerCase();
      if (rescued.has(k)) continue;
      out[m.side === 'target' ? 'tgt' : 'src'].set(k, 'MANUAL');
    }
    return out;
  }

  const api = {
    XR_VERSION, XR_SOURCE_RULES, XR_TARGET_RULES, XR_RATIO_GUARD, XR_LOW_ROW_WARNING,
    xrCompileCustom, xrRuleHits, xrCount, xrPrefixes, xrNewProfile, xrApply,
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (typeof window !== 'undefined') window.CygenixExclusionRules = api;
})();
