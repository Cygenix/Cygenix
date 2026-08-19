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
    check('the harvest doc is versioned v2 — a v1 cache has no columns and must never rehydrate silently',
      doc.v === 2 && H.HARVEST_VERSION === 2);
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

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})();
