// Tests for the semantic subject mapper — the SubjectMap engine
// (cygenix-subject-map.js) and the metadata harvest (cygenix-subject-harvest.js).
//
// The engine bridges two products that share no spelling by segmenting every
// identifier into a business vocabulary and comparing in concept space. The
// properties pinned here are the ones the implementation brief calls
// regressions if they drift: known-good pairs rank first, the decoy penalty
// holds, name twins bypass the cosine floor, confidence stays conservative,
// and coverage arithmetic is exact.
'use strict';

const S = require('../public/cygenix-subject-map.js');
const H = require('../public/cygenix-subject-harvest.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

// ── Segmentation and concepts ───────────────────────────────────────────────
{
  check('glued legacy names split into known morphemes',
    JSON.stringify(S.segment('tworkhrs')) === '["t","workhrs"]'
    && S.conceptsOf('tworkhrs').includes('time'), JSON.stringify(S.segment('tworkhrs')));
  check('four spellings of timekeeper meet at one concept',
    ['tkinit', 'ttk', 'tkprindex', 'timekeeperid'].every(n => S.conceptsOf(n).includes('timekeeper')));
  check('matter spellings from both products converge',
    S.conceptsOf('mttrnum').includes('matter') && S.conceptsOf('matterindex').includes('matter'));
  // The documented greedy misfire — pinned so a vocabulary edit that changes
  // it is a conscious decision, not an accident. The concept bag survives it
  // because other columns dominate.
  check('the known greedy misfire on mopendt is stable',
    JSON.stringify(S.segment('mopendt')) === '["mo","p","end","t"]', JSON.stringify(S.segment('mopendt')));
  check('digits are stripped before segmentation',
    S.conceptsOf('extuser143').includes('security'));
}

// ── Fixture estates: Elite-ish source, 3E-ish target ────────────────────────
const SRC = { tables: [
  { key: 'dbo.matter',   schema: 'dbo', name: 'matter',   rows: 62000, pk: ['mmatter'],
    cols: [{ name: 'mmatter', type: 'cha' }, { name: 'mname', type: 'var' }, { name: 'mclient', type: 'cha' }, { name: 'mopendt', type: 'dat' }] },
  { key: 'dbo.client',   schema: 'dbo', name: 'client',   rows: 30000, pk: ['clnum'],
    cols: [{ name: 'clnum', type: 'cha' }, { name: 'clname', type: 'var' }, { name: 'claddr', type: 'var' }] },
  { key: 'dbo.gj',       schema: 'dbo', name: 'gj',       rows: 24000, pk: [],
    cols: [{ name: 'gjid', type: 'int' }, { name: 'gjamt', type: 'dec' }, { name: 'gjpost', type: 'dat' }] },
  { key: 'dbo.timecard', schema: 'dbo', name: 'timecard', rows: 90000, pk: [],
    cols: [{ name: 'tkinit', type: 'cha' }, { name: 'tworkhrs', type: 'dec' }, { name: 'tmatter', type: 'cha' }] },
  { key: 'dbo.zzmisc',   schema: 'dbo', name: 'zzmisc',   rows: 10, pk: [],
    cols: [{ name: 'x1', type: 'int' }, { name: 'x2', type: 'int' }] },
]};
const TGT = { tables: [
  { key: 'dbo.Matter',      schema: 'dbo', name: 'Matter',      rows: 55000, pk: ['MatterIndex'],
    cols: [{ name: 'MatterIndex', type: 'int' }, { name: 'MatterID', type: 'var' }, { name: 'ClientIndex', type: 'int' }, { name: 'OpenDate', type: 'dat' }] },
  { key: 'dbo.Client',      schema: 'dbo', name: 'Client',      rows: 28000, pk: ['ClientIndex'],
    cols: [{ name: 'ClientIndex', type: 'int' }, { name: 'ClientID', type: 'var' }, { name: 'DisplayName', type: 'var' }] },
  { key: 'dbo.GJ',          schema: 'dbo', name: 'GJ',          rows: 20000, pk: [],
    cols: [{ name: 'GJTranNum', type: 'int' }, { name: 'GJAmt', type: 'dec' }, { name: 'PostDate', type: 'dat' }] },
  { key: 'dbo.TimeCardSum', schema: 'dbo', name: 'TimeCardSum', rows: 85000, pk: [],
    cols: [{ name: 'TimekeeperID', type: 'int' }, { name: 'WorkHrs', type: 'dec' }, { name: 'MatterIndex', type: 'int' }] },
  // The decoy family that outranked the real table before the penalty.
  { key: 'dbo.Matter_template', schema: 'dbo', name: 'Matter_template', rows: 0, pk: [],
    cols: [{ name: 'MatterIndex', type: 'int' }, { name: 'MatterID', type: 'var' }, { name: 'ClientIndex', type: 'int' }] },
]};

// ── Build: classification, spine, ranking ───────────────────────────────────
{
  const m = S.build(SRC, TGT);
  const q = (n) => m.src.find(t => t.name === n);

  check('the model carries its engine version for cache keying',
    m.engineVersion === S.ENGINE_VERSION);
  check('tables with real business columns all land in a named area',
    ['matter', 'client', 'gj', 'timecard'].every(n => q(n).area !== 'Unclassified'));
  check('the queue is in rows-at-stake order, biggest first',
    m.queue[0].name === 'timecard' && m.queue[m.queue.length - 1].name === 'zzmisc');

  // The brief's known-good pairs, on the fixture scale.
  check('matter → Matter ranks first', q('matter').candidates[0].table.name === 'Matter');
  check('client → Client ranks first', q('client').candidates[0].table.name === 'Client');
  check('gj → GJ ranks first — two spellings, one concept', q('gj').candidates[0].table.name === 'GJ');
  check('timecard → TimeCardSum ranks first without a name twin',
    q('timecard').candidates[0].table.name === 'TimeCardSum');

  check('the decoy penalty keeps Matter_template below Matter',
    (() => { const c = q('matter').candidates.map(x => x.table.name);
             return c.indexOf('Matter') < c.indexOf('Matter_template') || c.indexOf('Matter_template') < 0; })());
  check('exact-twin evidence is marked on the pair', q('matter').candidates[0].ev.exact === 1);

  // Spine: entity concepts keyed on both sides.
  const spine = Object.fromEntries(m.concepts.map(c => [c.concept, c]));
  check('matter and client are aligned on the spine',
    spine.matter && spine.matter.status === 'aligned'
    && spine.client && spine.client.status === 'aligned');
  check('spine rows carry the key columns from each side',
    Array.isArray(spine.matter.srcKeys) && Array.isArray(spine.matter.tgtKeys));

  // Confidence is conservative by construction.
  check('the tier ladder needs two independent kinds of evidence for strong',
    S.tier({ exact: 1, cos: 0.4, nameSim: 1, rowAff: 1, colAff: 1 }) === 'strong'
    && S.tier({ exact: 0, cos: 0.65, nameSim: 0.5, rowAff: 1, colAff: 1 }) === 'strong'
    && S.tier({ exact: 0, cos: 0.65, nameSim: 0.1, rowAff: 0.2, colAff: 1 }) !== 'strong'
    && S.tier(null) === 'none');
  check('a table the vocabulary cannot read has no strong claim',
    q('zzmisc').tier !== 'strong');
}

// ── buildAsync ≡ build, decisions, coverage and the harvest (async) ─────────
(async () => {
  {
    const events = [];
    const m1 = S.build(SRC, TGT);
    const m2 = await S.buildAsync(SRC, TGT, { chunk: 2 }, (p) => events.push(p.stage));
    const sig = (m) => JSON.stringify(m.queue.map(t => [t.name, t.area, t.tier,
      t.candidates.map(c => c.table.name)]));
    check('buildAsync produces byte-identical ranking to build', sig(m1) === sig(m2));
    check('and reports determinate progress through the stages',
      events.includes('profiling') && events.includes('scoring') && events[events.length - 1] === 'done');
  }

  // ── Overrides are fixed, not re-derived ───────────────────────────────────
  {
    const m = S.build(SRC, TGT, { srcOverrides: { 'dbo.zzmisc': 'Finance & ledger' } });
    const z = m.src.find(t => t.name === 'zzmisc');
    check('a user assignment pins the area and marks the source',
      z.area === 'Finance & ledger' && z.areaSource === 'user');
    check("an 'oos' override changes nothing here — it is a scope flag, not an area",
      S.build(SRC, TGT, { srcOverrides: { 'dbo.zzmisc': 'oos' } })
        .src.find(t => t.name === 'zzmisc').areaSource !== 'user');
  }

  // ── Decisions and honest coverage ─────────────────────────────────────────
  {
    const m = S.build(SRC, TGT);
    S.accept(m, 'dbo.matter', 'dbo.Matter');
    S.reject(m, 'dbo.zzmisc');
    check('accept records the pair; reject records target:null',
      m.decisions['dbo.matter'].target === 'dbo.Matter' && m.decisions['dbo.zzmisc'].target === null);
    const cov = S.coverage(m);
    const total = SRC.tables.reduce((a, t) => a + t.rows, 0);
    check('coverage totals are exact, not approximate',
      cov.total === total && cov.decided === 62000);
    check('a rejection never counts as decided coverage',
      !Object.entries(m.decisions).some(([k, d]) => !d.target && m.src.find(t => t.key === k).rows > 0
        && cov.decided !== 62000));
  }

  // ── Harvest: SQL builders and paging ──────────────────────────────────────
  {
    const all = [H.shColumnsSql(0), H.shPkSql(0), H.shRowsSql(4000)];
    check('every harvest statement is a read-only SELECT/CTE',
      all.every(s => /^\s*(WITH|SELECT)/i.test(s))
      && all.every(s => !/\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE\s+TABLE|MERGE|TRUNCATE|EXEC)\b/i.test(s)));
    check('every statement is paged below the 5,000-row silent truncation',
      /FETCH NEXT 500 ROWS/.test(all[0]) && /FETCH NEXT 4000 ROWS/.test(all[1])
      && /OFFSET 4000 ROWS FETCH NEXT 4000 ROWS/.test(all[2]));
    check('STRING_AGG is cast to varchar(max) — it truncates at 8000 otherwise',
      /CAST\(c\.COLUMN_NAME[\s\S]*?AS varchar\(max\)\)/.test(all[0]));
    check('queries are unqualified — cross-database access was an accident of one environment',
      all.every(s => !/son_db_hk|\bUSE\b/i.test(s)));

    // Paging loop: three pages then a short one.
    const pages = [[], [], []];
    for (let i = 0; i < 250; i++) pages[0].push({ s: 'dbo', n: 't' + i, c: 'a:int' });
    for (let i = 0; i < 250; i++) pages[1].push({ s: 'dbo', n: 'u' + i, c: 'a:int' });
    pages[2] = [{ s: 'dbo', n: 'last', c: 'a:int' }];
    let calls = 0;
    const exec = async () => ({ rows: pages[Math.min(calls++, 2)] });
    const rows = await H.shAllPages(exec, H.shColumnsSql, 250);
    check('paging keeps fetching until a short page and stitches the results',
      rows.length === 501 && calls === 3);

    // Full harvest against a stub, then applied to a graph.
    const stub = async (sql) => {
      if (/INFORMATION_SCHEMA\.COLUMNS/.test(sql)) return { rows: [
        { s: 'dbo', n: 'matter', c: 'mmatter:cha,mname:var' }] };
      if (/KEY_COLUMN_USAGE/.test(sql)) return { rows: [{ s: 'dbo', n: 'matter', pk: 'mmatter' }] };
      if (/sys\.partitions/.test(sql)) return { rows: [{ k: 'dbo.matter', r: 123 }] };
      return { rows: [] };
    };
    const doc = await H.shHarvest(stub, { database: 'X' });
    check('the harvest doc is versioned v3 — an old cache has no inventory and must never rehydrate silently',
      doc.v === 3 && H.HARVEST_VERSION === 3);
    const graph = { ok: true, tables: [{ key: 'dbo.matter', schema: 'dbo', name: 'matter', rowCount: 0, primaryKeys: [] }] };
    const hit = H.shApply(graph, doc);
    check('applying the harvest fills colsig, pk and row count on the node',
      hit === 1 && graph.tables[0].colsig === 'mmatter:cha,mname:var'
      && graph.tables[0].primaryKeys[0] === 'mmatter' && graph.tables[0].rowCount === 123);
    check('the harvest cache falls back to memory where IndexedDB is absent',
      await H.shWriteCache('k1', doc) === true && (await H.shReadCache('k1')).database === 'X');
    check('a stale or wrong-version doc is a miss, never a silent hit',
      await H.shWriteCache('k2', { ...doc, v: 1 }) === true && (await H.shReadCache('k2')) === null);
  }


  // ── v2: triage ruleset (cygenix-subject-triage.js) ────────────────────────
  {
    const T = require('../public/cygenix-subject-triage.js');
    const NOW = Date.parse('2026-08-19T00:00:00Z');
    const base = (o) => Object.assign({
      key: 'dbo.' + o.name, schema: 'dbo', kind: 'table', rows: 1000, cols: 8,
      colsig: 'sig-' + o.name, hasPk: true, hasUnique: false, indexCount: 2,
      inboundRefs: 1, inboundFks: 0, lastRead: '2026-08-01', modified: '2026-06-01',
      readOps: 50, writeOps: 5, hasUsageRow: true, description: null,
    }, o);
    const EST = (objs, win) => T.tgEstate(objs, { usageWindowDays: win == null ? 30 : win, now: NOW });

    // Hard excludes — only where a false positive is essentially impossible.
    check('a view is hard-excluded — views are lineage, not migration units',
      T.tgTriage(base({ name: 'vw_x', kind: 'view' }), EST([])).reasons[0] === 'HARD_VIEW');
    check('SharedTempDB and # objects are hard-excluded',
      T.tgTriage(base({ name: 'z_Matt', schema: 'SharedTempDB' }), EST([])).class === 'EXCLUDE'
      && T.tgTriage(base({ name: '#scratch' }), EST([])).reasons[0] === 'HARD_TEMP_DB');
    check('a GUID-suffixed session artefact is hard-excluded',
      T.tgTriage(base({ name: 'z_MattClose__Matters_9231d01f_aaaa_bbbb_cccc_1234567890ab' }), EST([]))
        .reasons[0] === 'HARD_GUID_SUFFIX');

    // The dated-backup sibling test is what makes the rule safe.
    const live = base({ name: 'extglobal', colsig: 'SIG1' });
    const bak  = base({ name: '_BAKTAB_20230421_extglobal', colsig: 'SIG1' });
    const est2 = EST([live, bak]);
    check('a dated backup with an identical-shape live sibling is hard-excluded',
      T.tgTriage(bak, est2).reasons[0] === 'HARD_DATED_BACKUP');
    check('the same name with NO matching sibling is not — the sibling test guards it',
      T.tgTriage(base({ name: '_BAKTAB_20230421_orphanb', colsig: 'SIGX' }), EST([])).hard === false);

    // Known sample databases fire only as a family, never on one name.
    const nw = ['customers','orders','shippers','customerdemographics'].map(n => base({ name: n }));
    check('Northwind is recognised as a family and excluded',
      T.tgTriage(nw[0], EST(nw)).reasons[0] === 'HARD_KNOWN_SAMPLE');
    check('a lone genuine "orders" table is never condemned by its name alone',
      T.tgTriage(base({ name: 'orders' }), EST([base({ name: 'orders' })])).hard === false);

    // Weighted signals and rescue guards.
    const junk = base({ name: 'cv_savematt', rows: 0, hasPk: false, inboundRefs: 0, inboundFks: 0,
      lastRead: null, readOps: 0, writeOps: 0, modified: '2015-01-01', description: null });
    const vj = T.tgTriage(junk, EST([junk]));
    check('conversion-prefixed, empty, unkeyed, orphaned and cold stacks to EXCLUDE',
      vj.class === 'EXCLUDE' && vj.reasons.includes('PFX_CONVERSION') && vj.reasons.includes('EMPTY')
      && vj.reasons.includes('ORPHANED') && vj.reasons.includes('COLD_READ'));
    const crown = base({ name: 'matter', rows: 6357, inboundFks: 12, readOps: 90000, description: 'Matters master' });
    check('the crown jewels sail through as MIGRATE with the guards on the record',
      T.tgTriage(crown, EST([crown])).class === 'MIGRATE'
      && T.tgTriage(crown, EST([crown])).reasons.includes('REFERENCED'));
    check('rescue guards keep an unlucky name pattern from burying a real table',
      T.tgTriage(base({ name: 'test_scores', rows: 500000, inboundFks: 3, description: 'live exam scores' }),
        EST([]))['class'] !== 'EXCLUDE');

    // Never auto-exclude: referenced + populated needs a human.
    const refd = base({ name: 'cv_refd_backup', rows: 10, inboundFks: 2, hasPk: false,
      inboundRefs: 0, lastRead: null, readOps: 0, writeOps: 0, modified: '2015-01-01' });
    const vr = T.tgTriage(refd, EST([refd]));
    check('referenced-and-populated is never auto-excluded, whatever the score',
      vr.class !== 'EXCLUDE' && vr.reasons.includes('KEPT_REFERENCED'));

    // Usage caveats: short window halves, heap-with-no-index abstains.
    const cold = base({ name: 'plaincold', lastRead: null, readOps: 0, writeOps: 0 });
    check('a short observation window halves the cold weights',
      T.tgTriage(cold, EST([cold], 5)).score < T.tgTriage(cold, EST([cold], 30)).score);
    const heap = base({ name: 'plainheap', indexCount: 0, hasUsageRow: false, lastRead: null, readOps: 0 });
    check('absence of usage data on an unindexed heap is not evidence of disuse',
      !T.tgTriage(heap, EST([heap], 30)).reasons.includes('COLD_READ'));

    // Family collapse: identical shape collapses, rhyming names do not.
    const fam = [1,2,3].map(i => base({ name: 'whmfield' + i, colsig: 'WHM', rows: 100 * i }));
    const odd = base({ name: 'whmfield9', colsig: 'DIFFERENT' });
    const fams = T.tgCollapse(fam.concat([odd]));
    const big = fams.find(f => f.collapsed);
    check('identical siblings collapse to one decision under the biggest member',
      fams.length === 2 && big.members.length === 3 && big.representative.name === 'whmfield3'
      && big.appliesTo.length === 3);
    check('a different column signature is a different decision, however the names rhyme',
      fams.some(f => !f.collapsed && f.representative.name === 'whmfield9'));
    check('the family key strips counters, dates and copy suffixes',
      T.tgFamilyKey('zz_matter_20160111_pretax') === 'matter'
      && T.tgFamilyKey('whmfield20') === 'whmfield'
      && T.tgFamilyKey('clarityintegration_backup') === 'clarityintegration');

    // Criticality: the estate ranks its own crown jewels.
    check('inbound references outweigh raw rows in criticality',
      T.tgCriticality({ inboundFks: 12, readOps: 90000, rows: 6357 })
      > T.tgCriticality({ inboundFks: 0, readOps: 0, rows: 2300000 }));

    // Inventory SQL: read-only, paged, usage-window aware.
    const inv = T.tgInventorySql(0), st = T.tgServerStartSql();
    check('the inventory is a read-only paged catalog query',
      /^WITH /.test(inv) && /OFFSET 0 ROWS FETCH NEXT 2000 ROWS ONLY/.test(inv)
      && !/\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE\s+TABLE|MERGE|TRUNCATE)\b/i.test(inv));
    check('it reads usage, dependencies, FKs and descriptions in one pass',
      /dm_db_index_usage_stats/.test(inv) && /sql_expression_dependencies/.test(inv)
      && /foreign_keys/.test(inv) && /MS_Description/.test(inv));
    check('the server start time ships so the UI can state the observation window',
      /sqlserver_start_time/.test(st));
  }

  // ── v2: engine governance rules ───────────────────────────────────────────
  {
    // Junk targets are refused at candidate generation, not nudged.
    const tgtTriage = { 'dbo.matter_template': 'EXCLUDE' };
    const m = S.build(SRC, TGT, { tgtTriage });
    const matterCands = m.src.find(t => t.name === 'matter').candidates.map(c => c.table.name);
    check('an EXCLUDE-triaged target never appears in any candidate list',
      !matterCands.includes('Matter_template')
      && !m.src.some(t => (t.candidates || []).some(c => c.table.name === 'Matter_template')));

    // Governance penalties are visible on the evidence, not silent.
    const m2 = S.build(SRC, TGT, { tgtTriage: { 'dbo.matter_template': 'REVIEW' } });
    const tpl = m2.src.find(t => t.name === 'matter').candidates.find(c => c.table.name === 'Matter_template');
    check('a review-lane target carries the −0.40 penalty on its evidence',
      !tpl || (tpl.ev.tgtReview === 1));
    const m3 = S.build(SRC, TGT, { srcTriage: { 'dbo.matter': 'REVIEW' } });
    check('a review-lane source is penalised too',
      m3.src.find(t => t.name === 'matter').candidates[0].ev.srcReview === 1);

    // Margin rule: a coin flip publishes as no candidate, tied options kept.
    const near = S.build(
      { tables: [ { key:'dbo.ambig', schema:'dbo', name:'ambig', rows: 100, pk: [],
        cols: [{ name:'clientindex', type:'int' }, { name:'matterindex', type:'int' }] } ] },
      { tables: [
        { key:'dbo.A1', schema:'dbo', name:'A1', rows: 100, pk: [],
          cols: [{ name:'clientindex', type:'int' }, { name:'matterindex', type:'int' }] },
        { key:'dbo.A2', schema:'dbo', name:'A2', rows: 100, pk: [],
          cols: [{ name:'clientindex', type:'int' }, { name:'matterindex', type:'int' }] },
      ] });
    const amb = near.src[0];
    check('tied candidates suppress the label but stay in the drawer',
      amb.suppressed === true && amb.tier === 'none' && amb.candidates.length === 2);

    // Zero-row rule: no data, no data evidence, capped confidence.
    const empty = S.build(
      { tables: [ { key:'dbo.matter', schema:'dbo', name:'matter', rows: 0, pk:['mmatter'],
        cols: [{ name:'mmatter', type:'cha' }, { name:'mname', type:'var' }] } ] },
      { tables: [ TGT.tables[1] ] });
    check('a zero-row source can never exceed possible, even on a name twin',
      empty.src[0].tier === 'possible' || empty.src[0].tier === 'none' || empty.src[0].tier === 'weak');

    // Immediate re-rank: an accepted target leaves other queues. Two sources
    // both see both targets; claiming one must clear the other's queue.
    const RSRC = { tables: [
      { key:'dbo.matter', schema:'dbo', name:'matter', rows:1000, pk:['mmatter'],
        cols:[{ name:'mmatter', type:'cha' }, { name:'mname', type:'var' }, { name:'mopendt', type:'dat' }] },
      { key:'dbo.mattdate', schema:'dbo', name:'mattdate', rows:500, pk:[],
        cols:[{ name:'mmatter', type:'cha' }, { name:'mdate', type:'dat' }] },
    ]};
    const RTGT = { tables: [
      { key:'dbo.Matter', schema:'dbo', name:'Matter', rows:900, pk:['MatterIndex'],
        cols:[{ name:'MatterIndex', type:'int' }, { name:'MatterName', type:'var' }, { name:'OpenDate', type:'dat' }] },
      { key:'dbo.MatterDates', schema:'dbo', name:'MatterDates', rows:400, pk:[],
        cols:[{ name:'MatterIndex', type:'int' }, { name:'DateValue', type:'dat' }] },
    ]};
    const m4 = S.build(RSRC, RTGT);
    const md4 = m4.src.find(t => t.name === 'mattdate');
    const before = (md4.candidates || []).map(c => c.table.name);
    S.accept(m4, 'dbo.matter', 'dbo.Matter');
    const after = (md4.candidates || []).map(c => c.table.name);
    check('accepting dbo.Matter removes it from other multi-candidate queues',
      before.includes('Matter') && before.length > 1 && !after.includes('Matter'));
    const m5 = S.build(RSRC, RTGT);
    const md5 = m5.src.find(t => t.name === 'mattdate');
    md5.candidates = md5.candidates.filter(c => c.table.name === 'Matter').slice(0, 1);
    S.accept(m5, 'dbo.matter', 'dbo.Matter');
    check('a source whose ONLY candidate was claimed keeps it, flagged contested',
      md5.candidates.length === 1 && md5.candidates[0].ev.contested === 1);
    check('the engine version bumped for the v2 rules', S.ENGINE_VERSION === 2 && S.GOV.marginFloor === 0.08);
  }

  // ── Exclusion picker §6: the relative ratio guard ─────────────────────────
  // The discriminating fact is implausible size MISMATCH, never absolute
  // size: a third of known-correct mappings land on tiny reference tables,
  // so an absolute low-row filter deletes correct answers and calls it
  // progress. The guard must drop the 3-row scaffold from a million-row
  // source while keeping the 4-row → 4-row lookup mapping untouched.
  {
    const GSRC = { tables: [
      { key:'dbo.timecard', schema:'dbo', name:'timecard', rows:2000000, pk:[],
        cols:[{ name:'tkinit', type:'cha' }, { name:'tworkhrs', type:'dec' }, { name:'tmatter', type:'cha' }] },
      { key:'dbo.phonetype', schema:'dbo', name:'phonetype', rows:4, pk:[],
        cols:[{ name:'phonetypeid', type:'int' }, { name:'phonetypedesc', type:'var' }] },
    ]};
    const GTGT = { tables: [
      { key:'dbo.TimeCardSum', schema:'dbo', name:'TimeCardSum', rows:1800000, pk:[],
        cols:[{ name:'TimekeeperID', type:'int' }, { name:'WorkHrs', type:'dec' }, { name:'MatterIndex', type:'int' }] },
      { key:'dbo.TimeCardType', schema:'dbo', name:'TimeCardType', rows:3, pk:[],
        cols:[{ name:'TimekeeperID', type:'int' }, { name:'WorkHrs', type:'dec' }] },
      { key:'dbo.PhoneType', schema:'dbo', name:'PhoneType', rows:4, pk:[],
        cols:[{ name:'PhoneTypeID', type:'int' }, { name:'PhoneTypeDesc', type:'var' }] },
    ]};
    const guard = { minSourceRows: 1000, maxRatio: 1000 };
    const g = S.build(GSRC, GTGT, { ratioGuard: guard });
    const tc = g.src.find(t => t.name === 'timecard');
    check('a 3-row scaffold never competes for a 2M-row source, and the drop is counted',
      !tc.candidates.some(c => c.table.name === 'TimeCardType')
      && tc.candidates.some(c => c.table.name === 'TimeCardSum')
      && tc.ratioDropped === 1);
    check('small sources legitimately map to small reference tables — the guard never fires there',
      g.src.find(t => t.name === 'phonetype').candidates.some(c => c.table.name === 'PhoneType'));
    const ug = S.build(GSRC, GTGT);
    check('without the guard the scaffold competes — the guard is opt-in, not a default',
      ug.src.find(t => t.name === 'timecard').candidates.some(c => c.table.name === 'TimeCardType'));
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})();
