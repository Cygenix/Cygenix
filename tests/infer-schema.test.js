// Tests for cygenix-infer-schema.js — the inferred-relationship engine for
// databases that declare no foreign keys. The pure functions run directly;
// the orchestrator runs against a stubbed dbCall so the SQL routing and the
// evidence pipeline are exercised end to end without a server.
'use strict';

const I = require('../public/cygenix-infer-schema.js');

let pass = 0, fail = 0;
const check = (name, ok, detail) => {
  console.log((ok ? '  PASS  ' : '  FAIL  ') + name + (ok ? '' : (detail ? '  [' + detail + ']' : '')));
  ok ? pass++ : fail++;
};

// ── Naming: prefixes and stems ───────────────────────────────────────────
{
  check('a dominant per-table prefix is learned',
    I.siPrefixOf(['tcindex', 'tcdate', 'tcclient', 'tcmatter', 'rowver']) === 'tc');
  check('the longest qualifying prefix wins',
    I.siPrefixOf(['vbfcode', 'vbfdate', 'vbfamount', 'vbfstatus']) === 'vbf');
  check('no prefix is claimed below 60% share',
    I.siPrefixOf(['tcindex', 'client_id', 'matter_id', 'amount', 'status']) === '');
  check('two columns are not enough evidence for a prefix',
    I.siPrefixOf(['tcindex', 'tcdate']) === '');
  check('stems strip the owning prefix then the id suffix',
    I.siStem('tcclientid', 'tc') === 'client');
  check('an unprefixed global column stems the same way',
    I.siStem('client_id', '') === 'client');
  check('so prefixed child and global parent columns meet at one stem',
    I.siStem('tcclientid', 'tc') === I.siStem('client_id', ''));
  check('_cd and _nbr count as suffix noise', I.siStem('matter_cd', '') === 'matter');
  check('stripping never eats the whole name', I.siStem('tcid', 'tc') === 'id');
  check('underscores are removed after stripping', I.siStem('bill_addr_code', '') === 'billaddr');
  const pm = I.siPrefixMap([
    { key: 'dbo.client', name: 'client',
      columns: [{ name: 'client_id' }, { name: 'client_name' }, { name: 'client_addr' }] },
    { key: 'dbo.timecard', name: 'timecard',
      columns: [{ name: 'tcindex' }, { name: 'tcdate' }, { name: 'tcclient' }] },
  ]);
  check('a prefix that starts the table name is the entity, not a mnemonic',
    pm['dbo.client'] === '' && I.siStem('client_id', pm['dbo.client']) === 'client');
  check('a true mnemonic prefix still strips', pm['dbo.timecard'] === 'tc');
  check('backup and snapshot tables are flagged obsolete',
    I.siObsolete('_BAKTAB_20230421_client') && I.siObsolete('zz_client_20160111')
    && I.siObsolete('_snr_mtkper') && !I.siObsolete('client'));
}

// ── Types: the gate ──────────────────────────────────────────────────────
{
  check('integer widths are join-compatible', I.siTypeGate('INT', 'BIGINT'));
  check('different families never join', !I.siTypeGate('INT', 'NVARCHAR(20)'));
  check('close string lengths pass', I.siTypeGate('VARCHAR(15)', 'VARCHAR(20)'));
  check('bounded against MAX is implausible as a key', !I.siTypeGate('VARCHAR(15)', 'VARCHAR(MAX)'));
  check('scale-0 decimal is an integer in spirit', I.siTypeGate('DECIMAL(10,0)', 'INT'));
  check('scaled decimal is not', !I.siTypeGate('DECIMAL(10,2)', 'INT'));
  check('dates are not keyable — containment lies on them', !I.siKeyable('DATETIME'));
  check('money and float are not keyable', !I.siKeyable('MONEY') && !I.siKeyable('FLOAT'));
  check('free text is not keyable', !I.siKeyable('NVARCHAR(MAX)') && !I.siKeyable('NVARCHAR(500)'));
  check('ints and short strings are keyable', I.siKeyable('INT') && I.siKeyable('VARCHAR(30)'));
  check('bit and one-character columns are flag-shaped',
    I.siFlaggy('BIT') && I.siFlaggy('CHAR(1)') && !I.siFlaggy('INT'));
}

// ── Channel A: code archaeology ──────────────────────────────────────────
{
  const KNOWN = {
    'dbo.timecard': 'dbo.timecard', 'timecard': 'dbo.timecard',
    'dbo.client': 'dbo.client', 'client': 'dbo.client',
    'dbo.matter': 'dbo.matter', 'matter': 'dbo.matter',
  };
  const resolve = (q) => KNOWN[q] || KNOWN[q.replace(/^dbo\./, '')] || null;

  let p = I.siParseJoins(
    'SELECT * FROM dbo.timecard tc JOIN dbo.client c ON c.client_id = tc.client_id', resolve);
  check('an aliased ANSI join is parsed',
    p.length === 1 && p[0].a.table === 'dbo.client' && p[0].b.table === 'dbo.timecard'
    && p[0].a.col === 'client_id' && !p[0].ambiguous);

  p = I.siParseJoins(
    'SELECT 1 FROM [dbo].[timecard] AS t JOIN [dbo].[matter] AS m ON m.[matter_id] = t.[matter_id]', resolve);
  check('brackets and AS are handled', p.length === 1 && p[0].a.table === 'dbo.matter');

  p = I.siParseJoins(
    'SELECT 1 FROM timecard t1, client c1 WHERE t1.client_id = c1.client_id', resolve);
  check('the pre-ANSI comma join is found via WHERE', p.length === 1 && p[0].a.table === 'dbo.timecard');

  p = I.siParseJoins(
    'SELECT 1 FROM timecard, client WHERE timecard.client_id = client.client_id', resolve);
  check('bare table names qualify predicates without aliases', p.length === 1);

  p = I.siParseJoins(
    'SELECT 1 FROM timecard x JOIN client c ON c.client_id = x.client_id;'
    + 'SELECT 2 FROM matter x JOIN client c ON c.client_id = x.client_id', resolve);
  check('an alias reused for two tables is recorded but marked ambiguous',
    p.length === 2 && p.every(j => j.ambiguous));

  p = I.siParseJoins(
    '-- t.client_id = c.client_id\n/* t.client_id = c.client_id */'
    + "SELECT 'FROM a JOIN b ON x.y = z.w' FROM timecard t", resolve);
  check('predicates in comments and string literals are not evidence', p.length === 0);

  p = I.siParseJoins('SELECT 1 FROM timecard a JOIN timecard b ON a.parent = b.tcindex', resolve);
  check('a self-join produces no cross-table edge', p.length === 0);

  const mods = [
    { schemaName: 'dbo', moduleName: 'vw_wip', definition: 'SELECT 1 FROM timecard t JOIN client c ON c.client_id = t.client_id WHERE t.client_id = c.client_id' },
    { schemaName: 'dbo', moduleName: 'rpt_ar', definition: 'SELECT 1 FROM client c JOIN timecard t ON t.client_id = c.client_id' },
  ];
  const agg = I.siCodeJoins(mods, resolve);
  check('occurrences count distinct modules, not repetitions',
    agg.length === 1 && agg[0].occurrences === 2);
  check('the predicate written either way around is the same evidence',
    agg[0].sources.length === 2);
}

// ── SQL builders: read-only and capped, always ───────────────────────────
{
  const all = [
    I.siModulesSql(I.CAPS), I.siModuleCountSql(), I.siUniqueColsSql(),
    I.siKeyProbeSql('dbo.timecard', 'client_id', 500000),
    I.siContainSql('dbo.timecard', 'client_id', 'dbo.client', 'client_id', 500000),
  ];
  check('every statement the engine builds is a SELECT',
    all.every(s => /^\s*SELECT/i.test(s)));
  check('no statement contains a write verb',
    all.every(s => !/\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|MERGE|TRUNCATE|EXEC)\b/i.test(s)));
  check('data reads are TOP-capped and NOLOCK',
    /TOP \(500000\)/.test(all[3]) && /WITH \(NOLOCK\)/.test(all[3])
    && /TOP \(500000\)/.test(all[4]) && /WITH \(NOLOCK\)/.test(all[4]));
  check('module definitions are truncated to fit the response ceiling',
    /LEFT\(m\.definition, 60000\)/.test(all[0]));
  check('identifiers are bracket-quoted with ] doubled',
    /\[we\]\]ird\]/.test(I.siKeyProbeSql('dbo.we]ird', 'x', 10)));
  check('the containment test also counts the orphans',
    /orphan_rows/.test(all[4]) && /LEFT JOIN/.test(all[4]));
}

// ── Verdicts ─────────────────────────────────────────────────────────────
{
  let v = I.siKeyVerdict({ n_rows: 100, n_distinct: 100, n_null: 0 });
  check('all-distinct no-null is a verified key', v.unique === true && v.uniqueness === 1);
  v = I.siKeyVerdict({ n_rows: 100, n_distinct: 100, n_null: 1 });
  check('a single null disqualifies a key', v.unique === false);
  v = I.siKeyVerdict({ n_rows: 1000, n_distinct: 999, n_null: 0 });
  check('a near-miss reports its uniqueness rather than rounding up',
    v.unique === false && v.uniqueness === 0.999);

  const c = I.siContainVerdict({ child_rows: 1000, child_ndv: 40, matched_ndv: 39, orphan_rows: 30 });
  check('containment is matched over distinct, orphan rate over rows',
    c.containment === 0.975 && c.orphanRate === 0.03);
  check('an empty child yields no containment claim',
    I.siContainVerdict({ child_rows: 0, child_ndv: 0, matched_ndv: 0, orphan_rows: 0 }).containment === null);
}

// ── Scoring and tiers ────────────────────────────────────────────────────
{
  const strong = { codeOccurrences: 47, containment: 0.9988, nameSim: 1, cardinalityOk: true };
  const s1 = I.siScore(strong);
  check('code + containment + name + cardinality reaches confirmed territory', s1 >= 0.90);
  check('and the tier honours it', I.siTier(strong, s1) === 'confirmed');

  const noCode = { containment: 1, nameSim: 1, cardinalityOk: true, semantic: true };
  check('without code evidence the weights cannot even reach confirmed',
    I.siScore(noCode) < 0.90 && I.siTier(noCode, I.siScore(noCode)) === 'possible');
  const softContain = { codeOccurrences: 47, containment: 0.95, nameSim: 1, cardinalityOk: true, semantic: true };
  check('a 0.90+ score with containment under 0.98 is demoted to probable',
    I.siScore(softContain) >= 0.90 && I.siTier(softContain, I.siScore(softContain)) === 'probable');

  const orphaned = Object.assign({}, strong, { orphanRate: 0.06 });
  check('a confirmed edge with >5% orphans is downgraded for a human',
    I.siTier(orphaned, I.siScore(orphaned)) === 'probable');

  const degenerate = Object.assign({}, strong, { degenerateParent: true });
  check('a degenerate parent domain caps at possible',
    ['possible', 'hypothesis'].includes(I.siTier(degenerate, I.siScore(degenerate))));
  check('and costs 0.20 of score', I.siScore(degenerate) === Math.round((s1 - 0.20) * 100) / 100);

  const flag = { containment: 1, nameSim: 1, cardinalityOk: true, flaggyChild: true };
  check('a flag-shaped child without code corroboration is only a hypothesis',
    I.siTier(flag, I.siScore(flag)) === 'hypothesis');
  const ident = { containment: 1, nameSim: 1, cardinalityOk: true, identityPair: true };
  check('identity-to-identity without code is only a hypothesis',
    I.siTier(ident, I.siScore(ident)) === 'hypothesis');
  const nounique = { codeOccurrences: 40, containment: 1, nameSim: 1, noUniqueSide: true };
  check('no provably unique side is never published above hypothesis',
    I.siTier(nounique, I.siScore(nounique)) === 'hypothesis');

  const amb = { codeOccurrences: 47, containment: 0.9988, nameSim: 1, cardinalityOk: true, ambiguous: true };
  check('ambiguous alias resolution halves the code channel and takes a penalty',
    I.siScore(amb) < s1 - 0.20);

  check('multiplicity: parent unique is many-to-one, both unique is 1:1',
    I.siMultiplicity(false, true) === 'many_to_one' && I.siMultiplicity(true, true) === '1:1');
}

// ── Decisions and edges ──────────────────────────────────────────────────
{
  const rels = [
    { child: 'dbo.timecard', childCol: 'client_id', parent: 'dbo.client', parentCol: 'client_id', tier: 'possible', confidence: 0.6 },
    { child: 'dbo.ckrc', childCol: 'status', parent: 'dbo.actcode', parentCol: 'actcode', tier: 'possible', confidence: 0.5 },
    { child: 'dbo.x', childCol: 'y', parent: 'dbo.z', parentCol: 'y', tier: 'hypothesis', confidence: 0.2 },
  ];
  const out = I.siApplyDecisions(rels, {
    accept: [{ child: 'dbo.timecard', childCol: 'client_id', parent: 'dbo.client', parentCol: 'client_id' }],
    reject: [{ child: 'dbo.ckrc', childCol: 'status', parent: 'dbo.actcode', parentCol: 'actcode' }],
  });
  check('a rejected pair is excluded permanently', out.length === 2 && !out.some(r => r.child === 'dbo.ckrc'));
  check('an accepted pair publishes at confirmed',
    out.find(r => r.child === 'dbo.timecard').tier === 'confirmed'
    && out.find(r => r.child === 'dbo.timecard').status === 'accepted');
  check('inference input is not mutated by decisions', rels[0].tier === 'possible');

  const edges = I.siToEdges({ relationships: out });
  check('hypothesis edges are hidden by default', edges.length === 1);
  check('unless explicitly asked for',
    I.siToEdges({ relationships: out }, { includeHypothesis: true }).length === 2);
  check('an edge carries its tier, confidence and direction',
    edges[0].from === 'dbo.timecard' && edges[0].to === 'dbo.client'
    && edges[0].inferred === true && edges[0].tier === 'confirmed');
}

// ── The orchestrator, end to end against a stub ──────────────────────────
{
  const T = (key, rowCount, columns) => {
    const [schema, name] = key.split('.');
    return { key, schema, name, kind: 'table', rowCount, columns,
             fkColumns: [], refColumns: [], primaryKeys: [] };
  };
  const tables = [
    T('dbo.timecard', 1000, [
      { name: 'tcindex', type: 'INT', isIdentity: true },
      { name: 'tcdate', type: 'DATETIME' },
      { name: 'client_id', type: 'INT' },
    ]),
    T('dbo.client', 50, [
      { name: 'client_id', type: 'INT' },
      { name: 'client_name', type: 'NVARCHAR(80)' },
    ]),
    T('dbo.zz_client_20160111', 40, [{ name: 'client_id', type: 'INT' }]),
    T('dbo.emptytab', 0, [{ name: 'x', type: 'INT' }]),
  ];
  const graph = { ok: true, database: 'LegacyDb', tables, byKey: {}, edges: [] };
  for (const t of tables) graph.byKey[t.key] = t;

  const calls = [];
  const dbCall = async (req) => {
    const sql = req.sql;
    calls.push(sql);
    if (/SERVERPROPERTY/.test(sql)) return { rows: [{ v: '15' }] };
    if (/FROM sys\.sql_modules m/.test(sql)) return { rows: [
      { schemaName: 'dbo', moduleName: 'vw_wip', definition:
        'SELECT 1 FROM dbo.timecard tc JOIN dbo.client c ON c.client_id = tc.client_id' },
      { schemaName: 'dbo', moduleName: 'rpt_ar', definition:
        'SELECT 1 FROM timecard t, client cl WHERE t.client_id = cl.client_id' },
    ] };
    if (/AS n FROM sys\.sql_modules/.test(sql)) return { rows: [{ n: 2 }] };
    if (/sys\.indexes/.test(sql)) return { rows: [
      { schemaName: 'dbo', tableName: 'client', columnName: 'client_id', why: 'unique_index' },
      { schemaName: 'dbo', tableName: 'timecard', columnName: 'tcindex', why: 'identity' },
    ] };
    if (/LEFT JOIN/.test(sql)) return { rows: [
      { child_rows: 1000, child_ndv: 40, matched_ndv: 40, orphan_rows: 0 },
    ] };
    if (/COUNT_BIG\(DISTINCT \[/.test(sql)) return { rows: [
      { n_rows: 1000, n_distinct: 40, n_null: 0 },
    ] };
    throw new Error('unexpected sql: ' + sql.slice(0, 60));
  };

  (async () => {
    const cat = await I.siRun(graph, { dbCall, columnsFor: async () => {}, onStatus: () => {} });
    check('the run succeeds against a stubbed server', cat.ok === true, cat.reason);
    check('the code join and the name match resolve to one relationship',
      cat.relationships.length === 1);
    const r = cat.relationships[0] || {};
    check('oriented child → parent by uniqueness evidence',
      r.child === 'dbo.timecard' && r.childCol === 'client_id'
      && r.parent === 'dbo.client' && r.parentCol === 'client_id');
    check('two code modules plus perfect containment lands at probable',
      r.tier === 'probable', r.tier + ' @ ' + r.confidence);
    check('the evidence rides on the edge',
      (r.evidence || []).map(e => e.channel).sort().join() === 'code,containment,name');
    check('orphans are reported even when zero', r.orphanRate === 0 && r.orphanRows === 0);
    check('multiplicity is many-to-one', r.multiplicity === 'many_to_one');
    check('the child side was probed for uniqueness, once', cat.stats.probes === 1);
    check('exactly one containment test ran', cat.stats.confirms === 1);
    check('obsolete tables are excluded and counted', cat.stats.obsoleteExcluded === 1);
    check('skipped tables are reported, never silent',
      cat.stats.tablesSkipped >= 1 && cat.notes.some(n => /not considered/.test(n)));
    check('every data read the run issued was a capped SELECT',
      calls.every(s => /^\s*SELECT/i.test(s)));

    const dead = await I.siRun(graph, { dbCall: async () => { throw new Error('login failed'); } });
    check('an unreachable or non-SQL-Server target refuses with a reason',
      dead.ok === false && /SQL Server/.test(dead.reason));

    const emptyG = await I.siRun({ ok: false, tables: [] }, { dbCall });
    check('no schema means no run', emptyG.ok === false);

    console.log(`\n${pass} passed, ${fail} failed`);
    process.exit(fail ? 1 : 0);
  })();
}
