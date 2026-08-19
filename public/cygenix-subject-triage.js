/* cygenix-subject-triage.js — object triage and family collapse for the
 * mapping engine (v2 spec, stages 1–2).
 *
 * The single highest-leverage change in the spec, and it involves no AI at
 * all: classify every object MIGRATE / REVIEW / EXCLUDE from catalog
 * metadata, so a 2,545-row queue becomes a few hundred real decisions.
 * Junk is GATED here, not nudged — the v1 decoy penalty was a −0.10 inside
 * the ranking and could never remove an object from the queue.
 *
 * Every rule carries a reason code that surfaces in the UI. A reviewer must
 * always be able to ask "why was this hidden?" and get an answer, or they
 * will not trust the hiding.
 *
 * Everything here is pure and deterministic; weights are configuration, not
 * code. The inventory SQL is read-only catalog queries, paged below the
 * execute path's silent 5,000-row truncation.
 */
(function () {
  'use strict';

  // ── Inventory SQL — per side, paged ──────────────────────────────────────
  // The §3.1 inventory: row counts, keys, inbound references and FKs, usage
  // stats, modify dates, MS_Description. dm_db_index_usage_stats resets on
  // service restart, so the server start time ships with it and the UI shows
  // the observation window instead of implying eternity.
  function tgInventorySql(off, page) {
    const p = page || 2000;
    return 'WITH rowcounts AS (\n'
      + '  SELECT ps.object_id, SUM(ps.row_count) AS row_count\n'
      + '  FROM sys.dm_db_partition_stats ps WHERE ps.index_id IN (0,1) GROUP BY ps.object_id\n'
      + '), colcounts AS (\n'
      + '  SELECT object_id, COUNT(*) AS column_count FROM sys.columns GROUP BY object_id\n'
      + '), keyinfo AS (\n'
      + '  SELECT object_id,\n'
      + '         MAX(CASE WHEN is_primary_key = 1 THEN 1 ELSE 0 END) AS has_pk,\n'
      + '         MAX(CASE WHEN is_unique = 1 THEN 1 ELSE 0 END) AS has_unique,\n'
      + '         COUNT(*) AS index_count\n'
      + '  FROM sys.indexes WHERE index_id > 0 GROUP BY object_id\n'
      + '), inboundrefs AS (\n'
      + '  SELECT d.referenced_id AS object_id, COUNT(DISTINCT d.referencing_id) AS inbound_refs\n'
      + '  FROM sys.sql_expression_dependencies d WHERE d.referenced_id IS NOT NULL GROUP BY d.referenced_id\n'
      + '), inboundfks AS (\n'
      + '  SELECT referenced_object_id AS object_id, COUNT(*) AS inbound_fks\n'
      + '  FROM sys.foreign_keys GROUP BY referenced_object_id\n'
      + '), usagestats AS (\n'
      + '  SELECT object_id,\n'
      + '         MAX(last_user_seek) AS last_seek, MAX(last_user_scan) AS last_scan,\n'
      + '         MAX(last_user_update) AS last_update,\n'
      + '         SUM(user_seeks + user_scans + user_lookups) AS read_ops,\n'
      + '         SUM(user_updates) AS write_ops\n'
      + '  FROM sys.dm_db_index_usage_stats WHERE database_id = DB_ID() GROUP BY object_id\n'
      + '), descr AS (\n'
      + '  SELECT major_id AS object_id, CONVERT(nvarchar(400), value) AS ms_description\n'
      + "  FROM sys.extended_properties WHERE class = 1 AND minor_id = 0 AND name = 'MS_Description'\n"
      + ')\n'
      + 'SELECT SCHEMA_NAME(o.schema_id) AS s, o.name AS n, o.type_desc AS td,\n'
      + '       CONVERT(varchar(10), o.create_date, 120) AS cd,\n'
      + '       CONVERT(varchar(10), o.modify_date, 120) AS md,\n'
      + '       ISNULL(rc.row_count, 0) AS r, ISNULL(cc.column_count, 0) AS cols,\n'
      + '       ISNULL(k.has_pk, 0) AS haspk, ISNULL(k.has_unique, 0) AS hasuq,\n'
      + '       ISNULL(k.index_count, 0) AS ixn,\n'
      + '       ISNULL(ir.inbound_refs, 0) AS inref, ISNULL(ifk.inbound_fks, 0) AS infk,\n'
      + '       CONVERT(varchar(10), u.last_seek, 120) AS lseek,\n'
      + '       CONVERT(varchar(10), u.last_scan, 120) AS lscan,\n'
      + '       ISNULL(u.read_ops, 0) AS rops, ISNULL(u.write_ops, 0) AS wops,\n'
      + '       CASE WHEN u.object_id IS NULL THEN 0 ELSE 1 END AS hasusage,\n'
      + '       d.ms_description AS descr\n'
      + 'FROM sys.objects o\n'
      + 'LEFT JOIN rowcounts rc ON rc.object_id = o.object_id\n'
      + 'LEFT JOIN colcounts cc ON cc.object_id = o.object_id\n'
      + 'LEFT JOIN keyinfo k ON k.object_id = o.object_id\n'
      + 'LEFT JOIN inboundrefs ir ON ir.object_id = o.object_id\n'
      + 'LEFT JOIN inboundfks ifk ON ifk.object_id = o.object_id\n'
      + 'LEFT JOIN usagestats u ON u.object_id = o.object_id\n'
      + 'LEFT JOIN descr d ON d.object_id = o.object_id\n'
      + "WHERE o.type IN ('U','V') AND o.is_ms_shipped = 0\n"
      + 'ORDER BY SCHEMA_NAME(o.schema_id), o.name\n'
      + 'OFFSET ' + (off | 0) + ' ROWS FETCH NEXT ' + p + ' ROWS ONLY;';
  }

  // The usage observation window. Under ~14 days, usage weights are halved —
  // "nothing read it since Tuesday" is not evidence of disuse.
  function tgServerStartSql() {
    return 'SELECT CONVERT(varchar(19), sqlserver_start_time, 120) AS started FROM sys.dm_os_sys_info;';
  }

  // ── Configuration, not code ──────────────────────────────────────────────
  const TRIAGE = {
    // Weighted signals (§3.3)
    weights: {
      EMPTY: 0.30, NO_KEY: 0.15, ORPHANED: 0.20,
      COLD_READ: 0.20, COLD_WRITE: 0.10, STALE_MODIFY: 0.10,
      PFX_CONVERSION: 0.25, PFX_TEMP: 0.25, SFX_COPY: 0.20, PFX_REPORT: 0.20,
      NUMBERED_SIBLING: 0.15, CLONE: 0.20, WIDE_THIN: 0.10, NO_DESCRIPTION: 0.05,
    },
    // Rescue guards — so one unlucky name pattern cannot bury a real table.
    guards: {
      BIG: -0.25, REFERENCED: -0.30, HOT: -0.35, DOCUMENTED: -0.15, KEYED: -0.15,
    },
    exclude: 0.65,
    review: 0.35,
    usageWindowDays: 14,
    staleYears: 5,
  };

  const RE = {
    guidSuffix: /_[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}$/i,
    datedPrefix: /^(_baktab_|zz_|bak_)/i,
    datedSuffix: /(_(19|20)\d{6}$)|(_(19|20)\d{2}[01]\d[0-3]\d(_[a-z]+)?$)/i,
    pfxConversion: /^(cv_|conv_|stg_|staging_|load_|imp_|impt_)/i,
    pfxTemp: /^((t|tmp|temp|tst|test|x|old|dup|scrub|fix|miss)[_0-9])|^(temp|tmp|tester|test)/i,
    sfxCopy: /_(backup|bak|sav|save|old|orig|copy|pretax|new|[0-9]{1,2})$/i,
    pfxReport: /^(dw_|rpt_|erms_|hselt_|frx_|frl_)/i,
  };

  // Known sample databases (Northwind / pubs / AdventureWorks). The spec ships
  // column-signature fingerprints; without those exact hashes to hand this
  // matches on the table-name families instead, and only fires when at least
  // three family members co-occur in the estate — a lone genuine "Orders"
  // table is never condemned by its name alone.
  const SAMPLE_FAMILIES = {
    northwind: ['customers', 'orders', 'order details', 'orderdetails', 'shippers', 'suppliers', 'products',
      'categories', 'employees', 'employeeterritories', 'territories', 'region',
      'customercustomerdemo', 'customerdemographics'],
    pubs: ['authors', 'titles', 'titleauthor', 'publishers', 'roysched', 'pub_info', 'discounts', 'stores'],
    adventureworks: ['salesorderheader', 'salesorderdetail', 'productcategory', 'productsubcategory',
      'businessentity', 'personphone', 'emailaddress', 'salesterritory'],
  };

  function tgSampleHits(names) {
    const present = new Set(names.map(n => String(n).toLowerCase()));
    const hits = new Set();
    for (const family of Object.values(SAMPLE_FAMILIES)) {
      const found = family.filter(n => present.has(n));
      if (found.length >= 3) found.forEach(n => hits.add(n));
    }
    return hits;
  }

  // ── Family key (§4) — strip counters, date stamps, copy suffixes ─────────
  const FAMILY_STRIP = [
    /_(19|20)\d{6}(_[a-z]+)?$/i,
    /_(backup|bak|sav|save|old|orig|copy|new)$/i,
    /(_|)\d{1,3}$/,
    /^(zz_|_baktab_\d*_|x)/i,
  ];
  function tgFamilyKey(name) {
    let k = String(name || '').toLowerCase();
    for (const re of FAMILY_STRIP) k = k.replace(re, '');
    return k.replace(/_+$/, '');
  }

  // ── The ruleset (§3.3) ───────────────────────────────────────────────────
  // obj: { name, schema, kind ('table'|'view'), rows, cols, colsig, hasPk,
  //        hasUnique, indexCount, inboundRefs, inboundFks, lastRead (ISO|null),
  //        modified (ISO|null), readOps, writeOps, hasUsageRow, description }
  // est:  { sampleHits:Set, sigCount:Map(colsig→n), sigRowsCount:Map(sig|rows→n),
  //         familyCount:Map(familyKey|colsig→n), plainSigs:Set(colsig of
  //         un-affixed objects), descCoverage:0..1, usageWindowDays, now }
  function tgTriage(obj, est, cfg) {
    const C = cfg || TRIAGE;
    const name = String(obj.name || '');
    const lower = name.toLowerCase();
    const reasons = [];

    // Hard excludes — only where a false positive is essentially impossible.
    if (obj.kind === 'view') return { class: 'EXCLUDE', score: 1, reasons: ['HARD_VIEW'], hard: true };
    if (/^(sharedtempdb|tempdb)$/i.test(obj.schema || '') || /^#/.test(name)) {
      return { class: 'EXCLUDE', score: 1, reasons: ['HARD_TEMP_DB'], hard: true };
    }
    if (RE.guidSuffix.test(lower)) return { class: 'EXCLUDE', score: 1, reasons: ['HARD_GUID_SUFFIX'], hard: true };
    // Dated backup: the affix alone is not enough — an un-affixed sibling with
    // the SAME column signature must exist. The sibling test is what makes
    // this safe.
    if ((RE.datedPrefix.test(lower) || RE.datedSuffix.test(lower))
        && obj.colsig && est.plainSigs && est.plainSigs.has(obj.colsig)) {
      return { class: 'EXCLUDE', score: 1, reasons: ['HARD_DATED_BACKUP'], hard: true };
    }
    if (est.sampleHits && est.sampleHits.has(lower)) {
      return { class: 'EXCLUDE', score: 1, reasons: ['HARD_KNOWN_SAMPLE'], hard: true };
    }

    // Weighted signals.
    const W = C.weights;
    let score = 0;
    const add = (code, w) => { reasons.push(code); score += w; };

    if (!(obj.rows > 0)) add('EMPTY', W.EMPTY);
    if (!obj.hasPk && !obj.hasUnique) add('NO_KEY', W.NO_KEY);
    if (!(obj.inboundRefs > 0) && !(obj.inboundFks > 0)) add('ORPHANED', W.ORPHANED);

    // Usage: halved under a short observation window; skipped entirely for a
    // heap with no index, where absence of usage data is not evidence.
    const win = est.usageWindowDays == null ? 0 : est.usageWindowDays;
    const usageScale = win >= C.usageWindowDays ? 1 : (win > 0 ? 0.5 : 0);
    const usageKnown = obj.indexCount > 0 || obj.hasUsageRow;
    if (usageScale > 0 && usageKnown) {
      if (!obj.lastRead && !(obj.readOps > 0)) add('COLD_READ', W.COLD_READ * usageScale);
      if (!(obj.writeOps > 0)) add('COLD_WRITE', W.COLD_WRITE * usageScale);
    }
    if (obj.modified && est.now) {
      const years = (est.now - new Date(obj.modified).getTime()) / (365.25 * 24 * 3600 * 1000);
      if (years > C.staleYears) add('STALE_MODIFY', W.STALE_MODIFY);
    }

    if (RE.pfxConversion.test(lower)) add('PFX_CONVERSION', W.PFX_CONVERSION);
    if (RE.pfxTemp.test(lower)) add('PFX_TEMP', W.PFX_TEMP);
    if (RE.sfxCopy.test(lower)) add('SFX_COPY', W.SFX_COPY);
    if (RE.pfxReport.test(lower)) add('PFX_REPORT', W.PFX_REPORT);

    if (obj.colsig && est.familyCount
        && (est.familyCount.get(tgFamilyKey(name) + '::' + obj.colsig) || 0) >= 3) {
      add('NUMBERED_SIBLING', W.NUMBERED_SIBLING);
    }
    if (obj.colsig && est.sigRowsCount
        && (est.sigRowsCount.get(obj.colsig + '|' + (obj.rows || 0)) || 0) >= 2) {
      add('CLONE', W.CLONE);
    }
    if (obj.cols > 0 && obj.cols <= 3 && obj.rows > 0 && !obj.hasPk) add('WIDE_THIN', W.WIDE_THIN);
    if (!obj.description && (est.descCoverage || 0) > 0.3) add('NO_DESCRIPTION', W.NO_DESCRIPTION);

    // Rescue guards, applied last.
    const G = C.guards;
    const guard = (code, w) => { reasons.push(code); score += w; };
    if (obj.rows > 100000) guard('BIG', G.BIG);
    if (obj.inboundFks > 0) guard('REFERENCED', G.REFERENCED);
    if (obj.lastRead && est.now
        && (est.now - new Date(obj.lastRead).getTime()) < 90 * 24 * 3600 * 1000) guard('HOT', G.HOT);
    if (obj.description) guard('DOCUMENTED', G.DOCUMENTED);
    if (obj.hasPk) guard('KEYED', G.KEYED);

    score = Math.max(0, Math.min(1, score));
    let klass = score >= C.exclude ? 'EXCLUDE' : score >= C.review ? 'REVIEW' : 'MIGRATE';

    // Never auto-exclude, regardless of score: something in the schema
    // depends on it and it holds rows. A human decides.
    if (klass === 'EXCLUDE' && obj.inboundFks > 0 && obj.rows > 0) {
      klass = 'REVIEW';
      reasons.push('KEPT_REFERENCED');
    }
    return { class: klass, score: Math.round(score * 100) / 100, reasons, hard: false };
  }

  // Estate-level context computed once, then applied per object.
  function tgEstate(objects, opts) {
    const o = opts || {};
    const affixed = (n) => RE.datedPrefix.test(n) || RE.datedSuffix.test(n)
      || RE.sfxCopy.test(n) || RE.guidSuffix.test(n);
    const plainSigs = new Set();
    const sigRowsCount = new Map();
    const familyCount = new Map();
    let described = 0;
    for (const t of objects) {
      const lower = String(t.name || '').toLowerCase();
      if (t.colsig && !affixed(lower)) plainSigs.add(t.colsig);
      if (t.colsig) {
        const sr = t.colsig + '|' + (t.rows || 0);
        sigRowsCount.set(sr, (sigRowsCount.get(sr) || 0) + 1);
        const fk = tgFamilyKey(t.name) + '::' + t.colsig;
        familyCount.set(fk, (familyCount.get(fk) || 0) + 1);
      }
      if (t.description) described++;
    }
    return {
      plainSigs, sigRowsCount, familyCount,
      sampleHits: tgSampleHits(objects.map(t => t.name)),
      descCoverage: objects.length ? described / objects.length : 0,
      usageWindowDays: o.usageWindowDays != null ? o.usageWindowDays : 0,
      now: o.now != null ? o.now : 0,
    };
  }

  // ── Family collapse (§4) ─────────────────────────────────────────────────
  // Twenty whmfield* tables become one decision. Members collapse only when
  // they agree on structure — dw_credall1 and dw_credcoll1 rhyme but have
  // different shapes and get separate decisions.
  function tgCollapse(objects) {
    const families = new Map();
    for (const t of objects) {
      const key = tgFamilyKey(t.name) + '::' + (t.colsig || ('nosig:' + t.name.toLowerCase()));
      if (!families.has(key)) families.set(key, []);
      families.get(key).push(t);
    }
    return [...families.values()].map(members => {
      const rep = members.slice().sort((a, b) =>
        (b.rows || 0) - (a.rows || 0)
        || String(b.modified || '').localeCompare(String(a.modified || '')))[0];
      return {
        representative: rep,
        members,
        collapsed: members.length > 1,
        appliesTo: members.map(m => m.key),
      };
    });
  }

  // Criticality (spec UI #5): the estate supplies its own importance ranking
  // — inbound FKs and read traffic — so progress rewards deciding dbo.matter,
  // not deciding junk. Rows are the secondary figure, never the bar.
  function tgCriticality(obj) {
    return 1
      + 2 * Math.log1p(obj.inboundFks || 0)
      + 0.25 * Math.log1p(obj.readOps || 0)
      + 0.15 * Math.log1p(obj.rows || 0);
  }

  const api = {
    TRIAGE, RE, FAMILY_STRIP, SAMPLE_FAMILIES,
    tgInventorySql, tgServerStartSql,
    tgTriage, tgEstate, tgFamilyKey, tgCollapse, tgSampleHits, tgCriticality,
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (typeof window !== 'undefined') window.CygenixSubjectTriage = api;
})();
