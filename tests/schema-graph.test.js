// Tests the Schema Explorer's data layer.
//
// Both tabs of schema_explorer.html read the schema and the saved maps through
// cygenix-schema-graph.js, so the rules that are easy to get wrong live in one
// place and are asserted here rather than in two views:
//
//   * literalValue overrides srcCol. A column carrying both is a fixed value,
//     not a mapped column — that is what the generated SQL emits, so coverage
//     counts and ribbon thickness must agree with it.
//   * A load order is a topological sort. A cycle has no valid order and must
//     be reported, never silently dropped, or the plan looks complete and
//     fails at run time on a constraint.
//   * The cache key must not contain the connection string. It carries a
//     password and localStorage keys are in plain sight.
const fs = require('fs');
const vm = require('vm');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

// The module is an IIFE that hangs itself off window. Give it just enough
// browser to load, with localStorage backed by a plain object so the test can
// see what it wrote.
function build(opts = {}) {
  const store = Object.assign({}, opts.store);
  const fetches = [];
  const sb = {
    console: { warn(){}, error(){}, log(){} },
    JSON, Math, Date, String, Number, Object, Array, Promise, Set, Error, isNaN, parseInt, parseFloat,
    setTimeout: (fn) => { if (opts.runTimers) fn(); return 0; },
    clearTimeout: () => {},
    AbortSignal: { timeout: () => null },
    localStorage: {
      getItem: (k) => (k in store ? store[k] : null),
      setItem: (k, v) => { store[k] = String(v); },
      removeItem: (k) => { delete store[k]; },
    },
    fetch: async (url, init) => {
      const body = JSON.parse(init.body);
      fetches.push({ url, body });
      const res = (opts.responses || {})[body.action];
      if (typeof res === 'function') return res(body);
      return { ok: true, json: async () => (res || { success: true }) };
    },
  };
  sb.window = sb;
  sb.window.CygenixConnections = opts.connections === null ? undefined : {
    get: () => opts.connections || { tgtConnString: 'mssql://sa:HUNTER2@db.example.com/App' },
  };
  vm.createContext(sb);
  vm.runInContext(fs.readFileSync(__dirname + '/../public/cygenix-schema-graph.js', 'utf8'), sb);
  return { G: sb.window.CygenixSchemaGraph, store, fetches, sb };
}

// A small schema with a chain, a diamond and a lookup, so the topological sort
// and the join finder have something with more than one answer to get right.
const FKS = [
  { fromSchema:'dbo', fromTable:'orders',     fromColumn:'customer_id', toSchema:'dbo', toTable:'customers', toColumn:'id', name:'FK_o_c' },
  { fromSchema:'dbo', fromTable:'order_items',fromColumn:'order_id',    toSchema:'dbo', toTable:'orders',    toColumn:'id', name:'FK_oi_o' },
  { fromSchema:'dbo', fromTable:'order_items',fromColumn:'product_id',  toSchema:'dbo', toTable:'products',  toColumn:'id', name:'FK_oi_p' },
  { fromSchema:'dbo', fromTable:'customers',  fromColumn:'address_id',  toSchema:'dbo', toTable:'addresses', toColumn:'id', name:'FK_c_a' },
  // An edge to a table that is not in the list — a different database, or a
  // schema the table query filtered out.
  { fromSchema:'dbo', fromTable:'orders',     fromColumn:'region_id',   toSchema:'ext', toTable:'regions',   toColumn:'id', name:'FK_o_r' },
  // A self-reference: real (an org chart), and it orders nothing.
  { fromSchema:'dbo', fromTable:'customers',  fromColumn:'parent_id',   toSchema:'dbo', toTable:'customers', toColumn:'id', name:'FK_c_c' },
];
const TABLES = ['customers','orders','order_items','products','addresses']
  .map(n => ({ schema:'dbo', name:n, kind:'table', rowCount: 10 }));

console.log('Schema Explorer — schema graph, load order and saved maps\n');

(async () => {
  const { G } = build();
  const graph = G._internals.buildGraph('AppDb', TABLES, FKS);

  // ── The graph ──────────────────────────────────────────────────────────
  check('every table becomes a node', graph.tables.length === 5);
  check('nodes are keyed schema.table', !!graph.byKey['dbo.orders']);
  check('an edge to a table outside the listed set is dropped, not left dangling',
    !graph.edges.some(e => /regions/.test(e.to)), JSON.stringify(graph.edges.map(e => e.to)));
  check('a self-reference is kept but flagged',
    graph.edges.filter(e => e.self).length === 1);
  // The ERD anchors an FK line to a column row, and can do it before the
  // column list has loaded because the edge names the column.
  check('FK columns are known from the edge list before columns load',
    graph.byKey['dbo.order_items'].fkColumns.sort().join(',') === 'order_id,product_id',
    graph.byKey['dbo.order_items'].fkColumns.join(','));
  check('and so are the referenced columns',
    graph.byKey['dbo.customers'].refColumns.join(',') === 'id');
  check('columns start empty rather than invented', graph.byKey['dbo.orders'].columns === null);
  check('adjacency is undirected, so a join can be walked either way',
    graph.adj['dbo.customers'].some(n => n.to === 'dbo.orders') &&
    graph.adj['dbo.orders'].some(n => n.to === 'dbo.customers'));
  check('a self-reference is not in the adjacency — it would be a loop to nowhere',
    !graph.adj['dbo.customers'].some(n => n.to === 'dbo.customers'));

  // ── Load order ─────────────────────────────────────────────────────────
  {
    const o = G.loadOrder(graph);
    check('the load order is valid', o.ok === true && o.cyclic.length === 0);
    const wave = {};
    o.waves.forEach((w, i) => w.forEach(k => { wave[k] = i; }));
    // The whole point: a parent is loaded before anything referencing it.
    check('a referenced table lands in an earlier wave than its child',
      wave['dbo.customers'] < wave['dbo.orders'] &&
      wave['dbo.orders']    < wave['dbo.order_items'] &&
      wave['dbo.addresses'] < wave['dbo.customers'],
      JSON.stringify(o.waves));
    check('independent tables share a wave, so they can run in parallel',
      o.waves[0].includes('dbo.addresses') && o.waves[0].includes('dbo.products'),
      JSON.stringify(o.waves[0]));
    check('a self-reference does not push a table into a later wave',
      wave['dbo.customers'] === 1, String(wave['dbo.customers']));
    check('every table is placed exactly once',
      o.waves.flat().length === 5 && new Set(o.waves.flat()).size === 5);
  }

  // A cycle has no valid order. Reporting it beats dropping the tables and
  // producing a plan that looks complete.
  {
    const cyc = G._internals.buildGraph('X', TABLES, FKS.concat([
      { fromSchema:'dbo', fromTable:'customers', fromColumn:'last_order_id', toSchema:'dbo', toTable:'orders', toColumn:'id', name:'FK_c_o' },
    ]));
    const o = G.loadOrder(cyc);
    check('a cycle is reported, not silently dropped', o.ok === false && o.cyclic.length > 0);
    check('the tables in the cycle are named',
      o.cyclic.includes('dbo.customers') && o.cyclic.includes('dbo.orders'), o.cyclic.join(','));
    check('the cycle path itself is reported so it can be broken',
      o.cycles.length > 0 && o.cycles[0].length >= 3, JSON.stringify(o.cycles[0]));
    check('tables outside the cycle still get an order',
      o.waves.flat().includes('dbo.products'));
  }

  // Restricting the sort to the tables actually being written — a migration
  // rarely touches the whole database.
  {
    const o = G.loadOrder(graph, ['dbo.orders', 'dbo.order_items']);
    check('a restricted order ignores tables outside the set',
      o.waves.flat().length === 2 && !o.waves.flat().includes('dbo.customers'));
    check('and still orders the ones inside it',
      o.waves[0].includes('dbo.orders') && o.waves[1].includes('dbo.order_items'));
  }

  // ── Join paths ─────────────────────────────────────────────────────────
  {
    // addresses and products are not related directly, or even closely.
    const p = G.joinPath(graph, 'dbo.addresses', 'dbo.products');
    check('a path is found between two unrelated tables', !!p);
    check('and it is the shortest one', p.tables.length === 5, p.tables.join(' → '));
    check('the path starts and ends where it was asked to',
      p.tables[0] === 'dbo.addresses' && p.tables[p.tables.length - 1] === 'dbo.products');

    const sql = G.joinSql(graph, p);
    check('the generated SQL joins every table in the path',
      (sql.match(/\bJOIN\b/g) || []).length === p.tables.length - 1, sql);
    check('every table is aliased, so a repeat cannot collide with itself',
      /FROM \[dbo\]\.\[addresses\] t1/.test(sql), sql.split('\n')[1]);
    // Direction matters: the FK column belongs to the child, the referenced
    // column to the parent, and the path may traverse either way.
    check('the join condition puts each column on the right side',
      /JOIN \[dbo\]\.\[customers\] t2 ON t2\.\[address_id\] = t1\.\[id\]/.test(sql), sql);
    check('and again when the path walks an FK the other way',
      /JOIN \[dbo\]\.\[orders\] t3 ON t3\.\[customer_id\] = t2\.\[id\]/.test(sql), sql);
    check('the statement is terminated', /;$/.test(sql.trim()));

    check('a table has a trivial path to itself',
      (G.joinPath(graph, 'dbo.orders', 'dbo.orders') || {}).hops.length === 0);
    check('an unknown table yields no path', G.joinPath(graph, 'dbo.orders', 'dbo.nope') === null);

    // Two components: no path exists and the finder must say so rather than
    // returning something that will not run.
    const split = G._internals.buildGraph('X',
      TABLES.concat([{ schema:'dbo', name:'audit', kind:'table', rowCount:0 }]), FKS);
    check('an unreachable table returns null, not an invented join',
      G.joinPath(split, 'dbo.orders', 'dbo.audit') === null);
  }

  // ── Focus by hops ──────────────────────────────────────────────────────
  {
    const one = G.neighbourhood(graph, ['dbo.orders'], 1);
    check('one hop reaches the direct relations',
      one.has('dbo.customers') && one.has('dbo.order_items') && !one.has('dbo.products'),
      [...one].join(','));
    const two = G.neighbourhood(graph, ['dbo.orders'], 2);
    check('two hops reach further', two.has('dbo.products') && two.has('dbo.addresses'));
    check('the starting table is always included', one.has('dbo.orders'));
  }

  // ── Saved maps ─────────────────────────────────────────────────────────
  // The rule the brief calls out as easy to get wrong, and it is: a column
  // with both a literal and a source column is a FIXED VALUE. The generated
  // SQL emits the literal and never reads srcCol.
  {
    const jobs = [
      { id:'j1', name:'Cases', jobType:'simple-map', projectId:'p1', sourceTable:'dbo.Case', targetTable:'dbo.Case_DM',
        columnMapping:[
          { srcCol:'CaseNo', tgtCol:'CaseNo', match:96 },
          { srcCol:'Name',   tgtCol:'CaseName', literalValue:'@@date', match:40 },
          { srcCol:'Ref',    tgtCol:'Ref',   match:71 },
          { tgtCol:'Loaded', literalValue:'GETDATE()' },
          { srcCol:'Orphan' },                              // no target: not mapped
        ] },
      { id:'j2', name:'Addresses', jobType:'simple-map', projectId:'p1',
        sourceTable:'dbo.Addr', targetTables:['dbo.addresses','dbo.Address_DM'],
        columnMapping:[{ srcCol:'Line1', tgtCol:'Line1', match:88 }] },
      { id:'j3', name:'Old', jobType:'simple-map', projectId:'p1', _deleted:true, columnMapping:[] },
      { id:'j4', name:'Other project', jobType:'simple-map', projectId:'p2', columnMapping:[] },
      { id:'j5', name:'Composite', jobType:'composite', projectId:'p1', columnMapping:[] },
      { id:'j6', name:'Legacy fixed', jobType:'simple-map', projectId:'p1', sourceTable:'dbo.A', targetTable:'dbo.B',
        columnMapping:[{ srcCol:'X', tgtCol:'X', fixedValue:'1' }, { srcCol:'Y', tgtCol:'Y', match:90 }] },
    ];
    const { G: G2 } = build({ store: {
      cygenix_jobs: JSON.stringify(jobs),
      cygenix_active_project_id: 'p1',
      cygenix_projects: JSON.stringify([{ id:'p1', name:'Demo' }]),
    } });

    const maps = G2.savedMaps();
    const ids = maps.map(m => m.id);
    check('only simple-map jobs are returned', !ids.includes('j5'));
    check('deleted maps are excluded', !ids.includes('j3'));
    check('another project\'s maps are excluded', !ids.includes('j4'));
    check('the current project\'s maps are returned', ids.includes('j1') && ids.includes('j2'));

    const j1 = maps.find(m => m.id === 'j1');
    check('a literal overrides its source column — it is not a mapped column',
      j1.mappedColumns === 2, 'mapped=' + j1.mappedColumns);
    check('and is counted as a fixed value instead', j1.fixedColumns === 2, 'fixed=' + j1.fixedColumns);
    check('a row with no target column is neither', j1.mappedColumns + j1.fixedColumns === 4);
    // Colour by worst confidence, not average: one bad guess should be
    // visible rather than diluted by good ones.
    check('the worst match confidence is carried, not the average',
      j1.worstMatch === 71, String(j1.worstMatch));
    check('the literal row does not drag the confidence down, being unmapped',
      j1.worstMatch !== 40);

    const j6 = maps.find(m => m.id === 'j6');
    check('the legacy fixedValue spelling counts as fixed too',
      j6.fixedColumns === 1 && j6.mappedColumns === 1);

    const j2 = maps.find(m => m.id === 'j2');
    check('a one-to-many map reports every target table',
      j2.targetTables.join(',') === 'dbo.addresses,dbo.Address_DM', j2.targetTables.join(','));

    check('the project name resolves for the header', G2.activeProjectName() === 'Demo');
    check('an untitled map still has a label', maps.every(m => !!m.name));
  }

  // A job saved before projects existed has no projectId. Stranding it
  // invisibly is worse than showing it — the job picker already treats it as
  // the active project's.
  {
    const { G: G3 } = build({ store: {
      cygenix_jobs: JSON.stringify([{ id:'old', jobType:'simple-map', columnMapping:[] }]),
      cygenix_active_project_id: 'p1',
    } });
    check('a map with no projectId is shown rather than stranded',
      G3.savedMaps().length === 1);
  }

  // The brief names cygenix_conv_project, which is the legacy single-project
  // key. The live one is cygenix_active_project_id — but a browser that has
  // not migrated still has only the old one.
  {
    const { G: G4 } = build({ store: {
      cygenix_conv_project: JSON.stringify({ id: 'legacy1', name: 'Legacy' }),
      cygenix_jobs: JSON.stringify([
        { id:'a', jobType:'simple-map', projectId:'legacy1', columnMapping:[] },
        { id:'b', jobType:'simple-map', projectId:'other',   columnMapping:[] },
      ]),
    } });
    check('the legacy project key is still honoured as a fallback',
      G4.activeProjectId() === 'legacy1');
    check('and filters correctly through it',
      G4.savedMaps().map(m => m.id).join(',') === 'a');
  }

  // Corrupt storage must not take the page down — both views render from this.
  {
    const { G: G5 } = build({ store: { cygenix_jobs: '{not json' } });
    check('corrupt job storage yields an empty list, not a throw', G5.savedMaps().length === 0);
  }

  // ── Secrets ────────────────────────────────────────────────────────────
  // The cache key is derived from the connection string, which carries a
  // password. It must not contain it.
  {
    const { G: G6 } = build();
    const CONN = 'mssql://sa:HUNTER2@db.example.com/App';
    const h = G6._internals.connHash(CONN);
    check('the cache key is a digest, not the connection string',
      !h.includes('HUNTER2') && !h.includes('db.example.com') && !h.includes('sa'), h);
    check('and it is stable', h === G6._internals.connHash(CONN));
    check('and distinguishes two connections',
      h !== G6._internals.connHash('mssql://sa:HUNTER2@db.example.com/Other'));
  }

  // ── Loading ────────────────────────────────────────────────────────────
  {
    const { G: G7, fetches, store } = build({ responses: {
      'schema-tables': { success:true, database:'AppDb', tables:TABLES },
      'schema-fks':    { success:true, foreignKeys:FKS },
    } });
    const g = await G7.load('tgt');
    check('a load returns a usable graph', g.ok === true && g.tables.length === 5);
    check('it names the database', g.database === 'AppDb');
    check('it uses the existing read-only schema actions, not raw SQL',
      fetches.map(f => f.body.action).sort().join(',') === 'schema-fks,schema-tables',
      fetches.map(f => f.body.action).join(','));
    check('nothing it sends could write', !fetches.some(f => /insert|update|delete|drop/i.test(JSON.stringify(f.body))));

    const cacheKeys = Object.keys(store).filter(k => k.startsWith('cygenix_schema_'));
    check('the result is cached under a per-connection key', cacheKeys.length === 1, cacheKeys.join(','));
    check('and the cache holds no credential',
      !store[cacheKeys[0]].includes('HUNTER2'), 'cache leaked the password');

    // A second load must not refetch.
    const before = fetches.length;
    await G7.load('tgt');
    check('a second load is served from memory', fetches.length === before);
  }

  // No connection is an empty state, not an exception — both views render
  // around it, and a first-time user has neither connection configured.
  {
    const { G: G8, fetches } = build({ connections: {} });
    const g = await G8.load('tgt');
    check('no connection yields an empty graph with a reason',
      g.ok === false && /connection/i.test(g.reason), g.reason);
    check('and no request is made', fetches.length === 0);
    check('an empty graph still has the shapes the views iterate',
      Array.isArray(g.tables) && Array.isArray(g.edges));
    check('the load order of an empty graph is empty, not a throw',
      G8.loadOrder(g).waves.length === 0);
    check('sample returns null with no connection, so the tab can hide',
      await G8.sample('tgt', 'dbo', 'x') === null);
  }

  // An unreachable database is reported the same way rather than thrown.
  {
    const { G: G9 } = build({ responses: {
      'schema-tables': () => { throw new Error('Login failed for user'); },
    } });
    const g = await G9.load('tgt');
    check('an unreachable database reports rather than throwing',
      g.ok === false && /Login failed/.test(g.reason), g.reason);
  }

  // A database with no FKs at all is normal on a staging schema. The ERD
  // still has to draw, and the load order is one wave.
  {
    const { G: GA } = build({ responses: {
      'schema-tables': { success:true, database:'Flat', tables:TABLES },
      'schema-fks':    () => { throw new Error('no permission on sys.foreign_keys'); },
    } });
    const g = await GA.load('tgt');
    check('a schema with no readable FKs still loads its tables',
      g.ok === true && g.tables.length === 5 && g.edges.length === 0);
    check('and everything lands in one parallel wave',
      GA.loadOrder(g).waves.length === 1);
  }

  // ── Sample rows ────────────────────────────────────────────────────────
  {
    const { G: GB, fetches } = build({ responses: {
      'execute': { success:true, recordset:[{ id:1 }] },
    } });
    await GB.sample('tgt', 'dbo', 'orders');
    const sql = fetches[0].body.sql;
    check('the sample query is a bounded read', /^SELECT TOP 12 \* FROM/.test(sql), sql);
    // A table name cannot be a parameter, so it is bracket-quoted with
    // internal ] doubled — the only escape T-SQL needs for an identifier.
    check('identifiers are bracket-quoted', /\[dbo\]\.\[orders\]/.test(sql), sql);
    await GB.sample('tgt', 'dbo', 'ev]il');
    check('a bracket in a table name is escaped, not passed through',
      /\[ev\]\]il\]/.test(fetches[1].body.sql), fetches[1].body.sql);
    await GB.sample('tgt', 'dbo', 'orders', 9999);
    check('the row limit is capped', /TOP 100 /.test(fetches[2].body.sql), fetches[2].body.sql);
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})();
