// Tests the Schema Explorer page itself — the coverage view's logic and the
// page's wiring.
//
// The data layer has its own suite (schema-graph.test.js). What this file
// pins is the part that produces something the rest of the app has to
// execute: the composite job.
//
// The runner contract, checked against azure-function/src/run-migration.js:
// execSqlStep is chosen by `step.type === 'sql'` (among others) and reads
// step.sql || step.insertSQL. Anything else goes to execMigrationStep, which
// needs srcTable and Project Builder's full step shape and fails with
// "srcTable not set on step". So a composite built here must emit sql steps —
// a 'map' step would be a job that looks fine in the list and fails on the
// first run.
const fs = require('fs');
const vm = require('vm');

const html = fs.readFileSync(__dirname + '/../public/schema_explorer.html', 'utf8');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

// The coverage module, extracted from the page and run against stubs so what
// is under test is what ships.
const start = html.indexOf('// ═══ VIEW 2 — COVERAGE MAP');
const end   = html.indexOf('// ── Boot ─');
if (start < 0 || end < 0 || end < start) {
  console.log('FAIL: could not locate the coverage module in schema_explorer.html');
  process.exit(1);
}

// CV is a top-level `const` in the page script, so it lives in the vm's
// global lexical scope rather than on the context object — it has to be
// reached through runInContext, not sb.CV.
function seed(sb, state) {
  vm.runInContext('Object.assign(CV, ' + JSON.stringify(state) + ')', sb);
}

function build(opts = {}) {
  const store = { cygenix_jobs: JSON.stringify(opts.jobs || []) };
  const sb = {
    console: { warn(){}, error(){}, log(){} },
    JSON, Math, Date, String, Number, Object, Array, Promise, Set, Error, parseInt, isNaN,
    alert: (m) => { sb._alert = m; },
    confirm: () => opts.confirm !== false,
    localStorage: {
      getItem: k => (k in store ? store[k] : null),
      setItem: (k, v) => { store[k] = String(v); },
    },
    document: { getElementById: () => ({ innerHTML:'', textContent:'', style:{}, classList:{ add(){}, remove(){} } }) },
    $: () => ({ innerHTML:'', textContent:'', style:{}, classList:{ add(){}, remove(){} } }),
    esc: s => String(s == null ? '' : s),
    num: n => String(n),
    CygenixSchemaGraph: {
      savedMaps: () => opts.maps || [],
      load: async () => opts.graph || { ok:false, reason:'no conn', tables:[], edges:[], byKey:{} },
      loadOrder: (g, keys) => opts.order || { waves: [], cyclic: [], cycles: [], ok: true },
      activeProjectId: () => 'p1',
      activeProjectName: () => 'Demo',
      _internals: {
        literalOf: (m) => {
          if (!m) return '';
          const v = (m.literalValue != null && m.literalValue !== '') ? m.literalValue
                  : (m.fixedValue != null && m.fixedValue !== '') ? m.fixedValue : '';
          return v == null ? '' : String(v);
        },
      },
    },
  };
  sb.window = sb;
  sb.store = store;
  vm.createContext(sb);
  vm.runInContext(html.slice(start, end), sb);
  return sb;
}

const MAPS = [
  { id:'m1', name:'Cust', sourceTable:'src.Cust', targetTables:['dbo.customers'], status:'complete',
    mappedColumns:2, fixedColumns:1, worstMatch:90, columnMapping:[], srcWhere:'' },
  { id:'m2', name:'Ord',  sourceTable:'src.Ord',  targetTables:['dbo.orders'],    status:'draft',
    mappedColumns:3, fixedColumns:0, worstMatch:70, columnMapping:[], srcWhere:'' },
  { id:'m3', name:'Items',sourceTable:'src.It',   targetTables:['dbo.order_items'],status:'complete',
    mappedColumns:1, fixedColumns:0, worstMatch:60, columnMapping:[], srcWhere:'' },
];
const JOBS = [
  { id:'m1', insertSQL:'INSERT INTO dbo.customers SELECT 1' },
  { id:'m2', insertSQL:'INSERT INTO dbo.orders SELECT 1' },
  { id:'m3', insertSQL:'INSERT INTO dbo.order_items SELECT 1' },
];
const ORDER = {
  waves: [['dbo.customers'], ['dbo.orders'], ['dbo.order_items']],
  cyclic: [], cycles: [], ok: true,
};

console.log('Schema Explorer — coverage view and page wiring\n');

// ── The composite job the runner has to execute ─────────────────────────────
{
  const sb = build({ maps: MAPS, jobs: JOBS, order: ORDER });
  seed(sb, { maps: MAPS, order: ORDER,
    graph: { ok:true, byKey:{ 'dbo.customers':1, 'dbo.orders':1, 'dbo.order_items':1 }, tables:[], edges:[] } });
  const job = vm.runInContext("cvBuildComposite('Demo – load order')", sb);

  check('a composite is produced', !!job);
  check('it is shaped as a composite job',
    job.jobType === 'composite' && job.type === 'composite' && job.source === '(multiple)' && job.target === '(multiple)');
  check('it carries the project so it lists under the right one',
    job.projectId === 'p1' && job.projectName === 'Demo');
  check('the step count and group count match the steps',
    job.childStepCount === job.childSteps.length && job.groupCount === 3);

  // The contract with run-migration.js.
  check('every step is a sql step, which is the only kind the runner executes',
    job.childSteps.every(s => s.type === 'sql'), JSON.stringify(job.childSteps.map(s => s.type)));
  check('and every step carries SQL, so none is a silent no-op',
    job.childSteps.every(s => !!s.insertSQL && !!s.sql));
  check('no step is a map step the runner would reject for having no srcTable',
    !job.childSteps.some(s => s.type === 'map' || s.jobType === 'map'));

  // Wave order is the whole point: a parent has to be inserted first.
  check('steps are ordered by wave',
    job.childSteps.map(s => s.groupName).join(',') === 'Wave 1,Wave 2,Wave 3',
    job.childSteps.map(s => s.groupName).join(','));
  check('order is a contiguous index the runner can sort on',
    job.childSteps.map(s => s.order).join(',') === '0,1,2');
  check('each step names its group', job.childSteps.every(s => !!s.groupId));
  check('a single-table wave is sequential', job.childSteps[0].groupMode === 'sequential');
  check('steps run against the target', job.childSteps.every(s => s.connOn === 'target'));
  check('each step remembers the map it came from', job.childSteps.every(s => !!s.sourceJobId));
}

// A wave holding more than one table has no internal dependency, so those
// steps may run together.
{
  const order = { waves: [['dbo.customers', 'dbo.orders']], cyclic: [], cycles: [], ok: true };
  const sb = build({ maps: MAPS, jobs: JOBS, order });
  seed(sb, { maps: MAPS, order });
  const job = vm.runInContext("cvBuildComposite('x')", sb);
  check('a multi-table wave is marked parallel',
    job.childSteps.every(s => s.groupMode === 'parallel'), job.childSteps[0].groupMode);
  check('and its steps share one group', new Set(job.childSteps.map(s => s.groupId)).size === 1);
}

// A map with no generated SQL would produce a step that runs nothing. Better
// to leave it out than to ship a composite that silently skips.
{
  const sb = build({ maps: MAPS, jobs: [{ id:'m1', insertSQL:'INSERT 1' }], order: ORDER });
  seed(sb, { maps: MAPS, order: ORDER });
  const job = vm.runInContext("cvBuildComposite('x')", sb);
  check('a map with no generated SQL is left out rather than emitted empty',
    job.childSteps.length === 1 && job.childSteps[0].sourceJobId === 'm1');
}

// One map writing two tables in the same wave must not be run twice.
{
  const maps = [{ id:'m9', name:'Split', sourceTable:'s', targetTables:['dbo.a','dbo.b'],
                  status:'complete', mappedColumns:2, fixedColumns:0, worstMatch:90, columnMapping:[] }];
  const order = { waves: [['dbo.a', 'dbo.b']], cyclic: [], cycles: [], ok: true };
  const sb = build({ maps, jobs: [{ id:'m9', insertSQL:'INSERT 1' }], order });
  seed(sb, { maps, order });
  const job = vm.runInContext("cvBuildComposite('x')", sb);
  check('a one-to-many map writing two tables in one wave runs once, not twice',
    job.childSteps.length === 1, String(job.childSteps.length));
}

// Nothing to build must explain itself rather than producing an empty job.
{
  const sb = build({ maps: [], jobs: [], order: { waves: [], cyclic: [], cycles: [], ok: true } });
  seed(sb, { maps: [], order: { waves: [], cyclic: [], cycles: [], ok: true } });
  check('an empty project builds no composite', vm.runInContext("cvBuildComposite('x')", sb) === null);
  check('and says why', /no saved maps/i.test(vm.runInContext('cvCompositeWhyNot()', sb)),
    vm.runInContext('cvCompositeWhyNot()', sb));
}

// ── Field trace ────────────────────────────────────────────────────────────
// "Who references this field?" — smComputeTrace collapses the diagram to the
// owning table plus every table whose foreign key points at the field.
// Pure function, executed from the shipped source.
{
  const tStart = html.indexOf('function smComputeTrace');
  const tEnd = html.indexOf('\n}', tStart);
  if (tStart < 0 || tEnd < 0) { check('smComputeTrace exists', false); }
  else {
    const sb2 = { String, Set, Map, Array };
    vm.createContext(sb2);
    vm.runInContext(html.slice(tStart, tEnd + 2), sb2);
    const E = (from, fc, to, tc) => ({ from, fromColumn: fc, to, toColumn: tc });
    const G = {
      ok: true,
      edges: [
        E('dbo.orders', 'CustomerId', 'dbo.customers', 'Id'),
        E('dbo.order_items', 'OrderId', 'dbo.orders', 'Id'),
        E('dbo.invoices', 'OrderId', 'dbo.orders', 'Id'),
        E('dbo.notes', 'NoteId', 'dbo.notes', 'NoteId', ),
      ],
    };
    G.edges[3].self = true;
    const trace = (q) => vm.runInContext('smComputeTrace', sb2)(G, q);

    let t = trace('CustomerId');
    check('a referencing column finds its owner',
      t && !t.empty && [...t.owners].join() === 'dbo.customers');
    check('and both sides of the reference are members',
      [...t.members].sort().join() === 'dbo.customers,dbo.orders');
    check('the traced columns are recorded on both tables',
      t.colHits.get('dbo.customers').has('Id') && t.colHits.get('dbo.orders').has('CustomerId'));

    t = trace('Id');
    check('a shared field name pulls in every owner that has it',
      [...t.owners].sort().join() === 'dbo.customers,dbo.orders');
    check('with every referencing table alongside', t.members.size === 4);

    t = trace('orders.Id');
    check('table.field disambiguates a shared name',
      [...t.owners].join() === 'dbo.orders'
      && [...t.members].sort().join() === 'dbo.invoices,dbo.order_items,dbo.orders');
    check('and only the traced edges survive',
      t.edgeKeys.size === 2 && ![...t.edgeKeys].some(k => k.includes('customers')));

    check('matching is case-insensitive', !trace('customerid').empty);
    check('a contains fallback catches a partial name',
      trace('ustomer') && !trace('ustomer').empty);
    check('an unknown field reports empty instead of blanking the map',
      trace('zzz').empty === true);
    check('an empty query means no trace', trace('') === null);
    check('a self-referencing edge is ignored', trace('NoteId').empty === true);
  }

  check('the trace input is in the toolbar', /id="sm-trace"/.test(html));
  check('the owner is visibly distinct — different border, declared after .sel so it wins',
    html.indexOf('.sm-box.sel{') < html.indexOf('.sm-box.owner{')
    && /\.sm-box\.owner\{border:2px solid var\(--amber\)/.test(html));
  check('referencing columns are highlighted', /sm-col-hit/.test(html));
  check('traced columns are promoted above the fold, with anchors following',
    /function smShownCols/.test(html)
    && /smShownCols\(node\)\.map\(c => c\.name\)/.test(html));
  check('the trace is computed over the whole schema, then drawn through the rules',
    /SM\.trace\.members\.has\(t\.key\)/.test(html)
    && /SM\.trace\.owners\.has\(t\.key\) \|\| !smHidden\(t, rules\)/.test(html));
  check('what the rules removed from a trace is counted in the headline, not silent',
    /function smTraceStatus/.test(html) && /hidden by your filters/.test(html));
  check('a filter change refreshes an active trace headline',
    /if \(SM\.trace\) smTraceStatus\(\);/.test(html));
  check('untraced edges between visible tables are not drawn',
    /SM\.trace && !SM\.trace\.edgeKeys\.has/.test(html));
  check('a schema reload drops the trace',
    /SM\.trace = null;\s*\{ const tr = \$\('sm-trace'\)/.test(html));
}

// ── Exclusion rules ────────────────────────────────────────────────────────
// The predicate that decides what the diagram draws. Extracted from the page
// and run against a stub SM, so the rules under test are the shipped ones.
{
  const fStart = html.indexOf('const RULE_MODES = {');
  const fEnd   = html.indexOf('const BOX_W =');
  if (fStart < 0 || fEnd < 0 || fEnd < fStart) {
    console.log('  FAIL  could not locate the filter block'); fail++;
  } else {
    const T = (name, extra) => Object.assign({
      key: 'dbo.' + name, schema: 'dbo', name, kind: 'table',
      rowCount: 10, fkColumns: [], refColumns: [],
    }, extra || {});
    const TABLES = [
      T('Customer'),
      T('Customer_draft',    { rowCount: 0 }),
      T('Customer_template', { rowCount: 0 }),
      T('CftSearchResultEntityOrg', { rowCount: 0 }),
      T('Orders',   { fkColumns: ['CustomerId'] }),
      T('Loner',    { rowCount: 5 }),
      // Views arrive from the server with rowCount 0 always — counting a view
      // means scanning it — so a realistic view fixture carries the zero.
      T('vw_Sales', { kind: 'view', rowCount: 0 }),
      T('NoCount',  { rowCount: undefined }),
    ];

    const mk = (filters) => {
      const sb = { JSON, Math, Object, Array, Set, String, RegExp, console: { warn(){}, error(){} } };
      sb.SM = { side: 'tgt', filters: Object.assign(
        { hideEmpty:false, hideViews:false, hideIsolated:false, rules: [] }, filters),
        graph: { ok: true, tables: TABLES } };
      vm.createContext(sb);
      vm.runInContext(html.slice(fStart, fEnd), sb);
      return sb;
    };
    const names = (filters) => vm.runInContext('smTables().map(t => t.name)', mk(filters));

    check('with no rules every table is drawn', names({}).length === TABLES.length);

    check('"ends with" hides the generated twins',
      JSON.stringify(names({ rules: [{ mode:'ends', text:'_draft', on:true }] }))
        === JSON.stringify(TABLES.filter(t => t.name !== 'Customer_draft').map(t => t.name)));

    check('rules are case-insensitive, because table names are not typed carefully',
      !names({ rules: [{ mode:'ends', text:'_DRAFT', on:true }] }).includes('Customer_draft'));

    check('"starts with" matches the front only',
      !names({ rules: [{ mode:'starts', text:'Cft', on:true }] }).includes('CftSearchResultEntityOrg')
      && names({ rules: [{ mode:'starts', text:'Cft', on:true }] }).includes('Customer'));

    check('"contains" matches anywhere',
      !names({ rules: [{ mode:'contains', text:'Search', on:true }] }).includes('CftSearchResultEntityOrg'));

    check('"is exactly" hides one table, not its siblings',
      (() => { const n = names({ rules: [{ mode:'equals', text:'Customer', on:true }] });
               return !n.includes('Customer') && n.includes('Customer_draft'); })());

    check('rules are OR-ed, so two rules hide both sets',
      (() => { const n = names({ rules: [{ mode:'ends', text:'_draft', on:true },
                                         { mode:'ends', text:'_template', on:true }] });
               return !n.includes('Customer_draft') && !n.includes('Customer_template')
                      && n.includes('Customer'); })());

    check('an unticked rule does nothing',
      names({ rules: [{ mode:'ends', text:'_draft', on:false }] }).includes('Customer_draft'));
    check('an empty rule does nothing, so a half-typed one hides nothing',
      names({ rules: [{ mode:'ends', text:'', on:true }] }).length === TABLES.length);
    check('whitespace-only text is treated as empty',
      names({ rules: [{ mode:'ends', text:'   ', on:true }] }).length === TABLES.length);

    // A regex box is typed one character at a time, so it is invalid most of
    // the time it is being used. It must never throw or hide everything.
    check('an invalid regex is ignored rather than thrown',
      names({ rules: [{ mode:'regex', text:'Cft([', on:true }] }).length === TABLES.length);
    check('and it is reported as bad so the box can be marked',
      vm.runInContext('smCompileRules()[0].bad === true',
        mk({ rules: [{ mode:'regex', text:'Cft([', on:true }] })));
    check('a valid regex filters',
      !names({ rules: [{ mode:'regex', text:'^Cft', on:true }] }).includes('CftSearchResultEntityOrg'));

    check('hide empty removes zero-row tables', !names({ hideEmpty:true }).includes('Customer_draft'));
    check('and keeps tables that have rows',   names({ hideEmpty:true }).includes('Customer'));
    // An absent row count is not evidence of emptiness.
    check('a table with no row count survives hide-empty',
      names({ hideEmpty:true }).includes('NoCount'));
    // A view's zero is a placeholder, not a fact — hide-empty must not become
    // a silent hide-all-views.
    check('a view survives hide-empty, because its count is never real',
      names({ hideEmpty:true }).includes('vw_Sales'));

    check('hide views removes views only',
      (() => { const n = names({ hideViews:true });
               return !n.includes('vw_Sales') && n.includes('Customer'); })());

    check('hide unrelated removes tables with no foreign key either way',
      (() => { const n = names({ hideIsolated:true });
               return !n.includes('Loner') && n.includes('Orders'); })());

    check('the hidden count is what the badge shows',
      vm.runInContext('smHiddenCount()', mk({ rules: [{ mode:'ends', text:'_draft', on:true }] })) === 1);
    check('no filter means nothing is reported as active',
      vm.runInContext('smFiltersActive()', mk({})) === false);
    check('an enabled rule with text is reported as active',
      vm.runInContext('smFiltersActive()', mk({ rules: [{ mode:'ends', text:'_x', on:true }] })) === true);
    check('an enabled rule with no text is not',
      vm.runInContext('smFiltersActive()', mk({ rules: [{ mode:'ends', text:'', on:true }] })) === false);

    // Dragging a box re-renders on every mousemove and each render asks for
    // the filtered list several times.
    check('the filtered list is memoised, not recomputed per call',
      vm.runInContext('smTables() === smTables()', mk({ rules: [{ mode:'ends', text:'_draft', on:true }] })));
    check('and the key set comes from the same cache',
      vm.runInContext('smVisibleKeys().has("dbo.Customer") && !smVisibleKeys().has("dbo.Customer_draft")',
        mk({ rules: [{ mode:'ends', text:'_draft', on:true }] })));
    check('the cache follows a filter change rather than going stale',
      vm.runInContext('const a = smTables().length; SM.filters.hideEmpty = true; a - smTables().length',
        mk({})) === 3);

    // The trace used to bypass the rules entirely — the bug report was a
    // "contains mx" rule that an acctindex trace ignored. Now the rules thin
    // the trace too, with one exemption: the owning table is the anchor of
    // the question and always stays.
    check('a trace is thinned by the exclusion rules',
      vm.runInContext(
        'SM.trace = { q:"cid", owners:new Set(["dbo.Customer"]),'
        + ' members:new Set(["dbo.Customer","dbo.CftSearchResultEntityOrg","dbo.Orders"]) };'
        + ' smTables().map(t => t.name).join()',
        mk({ rules: [{ mode:'starts', text:'Cft', on:true }] })) === 'Customer,Orders');
    check('but the owning table survives a rule that names it — the anchor stays',
      vm.runInContext(
        'SM.trace = { q:"cid", owners:new Set(["dbo.Customer"]),'
        + ' members:new Set(["dbo.Customer","dbo.Orders"]) };'
        + ' smTables().map(t => t.name).join()',
        mk({ rules: [{ mode:'equals', text:'customer', on:true }] })) === 'Customer,Orders');
  }
}

// ── Inferred relationships (FK-less databases) ─────────────────────────────
// The engine itself is tested in infer-schema.test.js; these pin the wiring.
{
  check('the inference engine is loaded', /cygenix-infer-schema\.js/.test(html));
  check('there is an Infer button in the toolbar', /id="sm-infer-btn"/.test(html));
  check('a FK-less database is detected automatically — tables but zero declared edges',
    /g\.tables\.length >= 8 && g\.edges\.length === 0/.test(html)
    && /id="sm-infer-banner"/.test(html));
  const merge = html.slice(html.indexOf('function smInferMerge'), html.indexOf('function smInferFkless'));
  check('inferred edges live beside the graph, never inside it — the graph is cached as fact',
    merge.length > 0 && !/g\.edges\.push/.test(merge) && /SM\.infEdges\.push/.test(merge));
  check('the renderer concats declared and inferred edges',
    /g\.edges\.concat\(SM\.infEdges/.test(html));
  check('join paths and hop focus walk confirmed + probable only',
    /e\.tier === 'confirmed' \|\| e\.tier === 'probable'/.test(html));
  check('tiers are styled: possible dashed, hypothesis dotted and off by default',
    /\.sm-edge\.inf-possible\{stroke-dasharray/.test(html)
    && /\.sm-edge\.inf-hypothesis\{stroke-dasharray/.test(html)
    && /showHypothesis: false/.test(html));
  check('the inspector offers accept and reject on inferred edges',
    /data-v="accept"/.test(html) && /data-v="reject"/.test(html)
    && /onclick="smInferDecide\(this\)"/.test(html));
  check('the trace answers over inferred edges too',
    /SM\.graph\.edges\.concat\(SM\.infEdges \|\| \[\]\) \}/.test(html)
    || /edges: SM\.graph\.edges\.concat\(SM\.infEdges/.test(html));
}

// ── Normalisation and thresholds ───────────────────────────────────────────
{
  const sb = build({});
  const norm = (s) => vm.runInContext('cvNormKey(' + JSON.stringify(s) + ')', sb);
  check('an unqualified table name is assumed to be dbo', norm('addresses') === 'dbo.addresses');
  check('a qualified name is left alone', norm('sales.orders') === 'sales.orders');
  check('brackets are stripped so [dbo].[x] matches dbo.x', norm('[dbo].[Case]') === 'dbo.Case');

  const cls = (n) => vm.runInContext('cvConfClass(' + n + ')', sb);
  check('a confident match reads green', cls(96) === 'hi');
  check('a middling one reads amber', cls(70) === 'mid');
  check('a poor one reads red', cls(40) === 'lo');
  check('the boundaries fall the safe way', cls(85) === 'hi' && cls(84) === 'mid' && cls(60) === 'mid' && cls(59) === 'lo');
}

// ── Page wiring ────────────────────────────────────────────────────────────
{
  check('the page mounts the sidebar under the new nav key',
    /data-active="schema-explorer"/.test(html));
  check('it loads the shared schema module rather than fetching its own',
    /src="\/cygenix-schema-graph\.js(\?v=[a-f0-9]+)?"/.test(html));
  check('it inherits the auth gate', /src="\/auth-gate\.js(\?v=[a-f0-9]+)?"/.test(html));
  check('and the token script', /src="\/cygenix-auth-token\.js(\?v=[a-f0-9]+)?"/.test(html));
  check('and connections, for the source and target strings', /src="\/connections\.js(\?v=[a-f0-9]+)?"/.test(html));
  check('and the project context', /src="\/cygenix-cosmos-sync\.js(\?v=[a-f0-9]+)?"/.test(html));
  check('and cookie consent', /src="\/cookie-consent\.js(\?v=[a-f0-9]+)?"/.test(html));

  check('all three tabs are present',
    /se-tab-schema/.test(html) && /se-tab-atlas/.test(html) && /se-tab-coverage/.test(html));
  // The atlas module must sit OUTSIDE the coverage slice this file extracts,
  // or the vm run above would have pulled canvas code into the sandbox.
  check('the atlas module precedes the coverage module',
    html.indexOf('VIEW 1.5 — DATA MAP') > 0
    && html.indexOf('// ═══ VIEW 1.5') < html.indexOf('// ═══ VIEW 2 — COVERAGE MAP'));
  check('atlas data colours are tokens ahead of the palette marker',
    html.indexOf('--da-s1:') < html.indexOf('--r:10px')
    && /v\('--da-s1'\)/.test(html) && /getPropertyValue\(n\)/.test(html));
  check('the atlas defaults to the source side, where the data lives',
    /side: 'src',\s*\/\/ the atlas is about where the data LIVES/.test(html));
  check('migration status comes from saved maps, not invention',
    /daMapsIndex/.test(html) && /CygenixSchemaGraph\.savedMaps\(\)/.test(html));
  check('the heuristic lenses declare themselves in the legend',
    /Name-pattern estimate/.test(html));
  check('volume is always labelled an estimate', /Est\. volume/.test(html));
  check('the prototype fields with no real source were dropped, not faked',
    !/null density/i.test(html) && !/Last written/.test(html.slice(html.indexOf('VIEW 1.5'), html.indexOf('// ═══ VIEW 2'))));
  check('the tab choice persists under its own key', /cygenix_schemaexp_tab/.test(html));
  check('the source/target toggle exists and defaults to target',
    /sm-side-tgt/.test(html) && /side:\s*'tgt'/.test(html));
  check('there is a visible reload for the cached schema', /Reload schema/.test(html));

  // Screen space.
  // Full screen moved to the shared helper — cygenix-fullscreen.js and
  // tests/fullscreen.test.js own the mechanism. What this file still pins is
  // that the two original controls survived the move with their behaviour.
  check('the diagram can go full screen',
    /id="sm-fs-btn"/.test(html) && /<script src="\/cygenix-fullscreen\.js/.test(html));
  check('and the canvas keeps its own background there, since the browser paints black behind it',
    /:fullscreen\{[^}]*background:var\(--bg\)/.test(html));
  check('leaving full screen re-measures rather than keeping the old zoom',
    /onChange: \(\) => \{ smRenderCanvas\(\); smFit\(\); \}/.test(html));
  // Each view supplies its own target and redraw, so a transition can never
  // refit the schema canvas while the atlas panel owns the screen — which
  // would measure a hidden 0×0 box.
  check('every view brings its own target and redraw to the shared helper',
    /target: \(\) => \$\('sm-canvas-wrap'\)/.test(html)
    && /target: \(\) => document\.querySelector\('\.da-panel'\)/.test(html));
  check('the atlas has its own full screen control',
    /id="da-fs-btn"/.test(html) && /if \(DA\.view === 'map'\) daResize\(\)/.test(html));
  check('and the atlas view flexes to the viewport instead of a fixed offset',
    /\.da-view\{display:flex;flex-direction:column;height:calc\(100vh/.test(html)
    && /\.da-body\{[^}]*flex:1;min-height:0\}/.test(html));
  // A mid-layout measurement can leave the canvas wider than its wrap; the
  // wrap clips so a stale canvas can never paint under the rail.
  check('the map wrap clips overdrawn canvas frames',
    /\.da-mapwrap\{[^}]*overflow:hidden/.test(html));
  // The CSS calc() offset guesses at the chrome height; daSizeView measures
  // the real top edge so the map bottom never falls below the fold.
  check('the atlas view is sized from a live measurement, not a magic offset',
    /function daSizeView\(\)/.test(html)
    && /window\.innerHeight - el\.getBoundingClientRect\(\)\.top/.test(html));
  // On a 12k-table estate a drilled group still has a sub-pixel tail. It
  // pools like the top level does, and — already drilled, nowhere deeper —
  // the pooled cell routes to the Table view, which honours the drill.
  const atlasJs = html.slice(html.indexOf('VIEW 1.5'), html.indexOf('// ═══ VIEW 2'));
  check('the drilled map pools its sub-pixel tail instead of dropping it',
    /keep\.length < 220/.test(atlasJs));
  // 2,600 near-empty tables next to million-row heads pool to a cell 0.7px
  // wide — an invisible way in is no way in. Layout v is floored; tv keeps
  // the honest total for the tooltip.
  check('the pooled remainder is floored so it stays visible and clickable',
    (atlasJs.match(/Math\.max\(rest, gv \* 0\.015\)/g) || []).length === 2
    && /tv !== undefined \? d\.tv : d\.v/.test(atlasJs));
  check('clicking the drilled remainder lists the tables it pooled',
    /d\.rest && DA\.drill\)[\s\S]{0,220}daSetView\('table'\)/.test(atlasJs));
  check('the table view honours the drill the crumb says it is in',
    /if \(DA\.drill\) list = list\.filter\(t => daCatOf\(t\) === DA\.drill\);/.test(atlasJs));
  check('the view switcher is one function, so buttons stay in step',
    /function daSetView\(v\)[\s\S]{0,400}aria-pressed/.test(atlasJs)
    && /daSetView\(b\.dataset\.daView\)/.test(atlasJs));
  // The filter bridge reuses the Schema map's own predicate, and only when
  // both views look at the same database.
  check('the atlas can apply the Schema map filters via the same predicate',
    /id="da-filters-btn"/.test(html) && /smCompileRules\(\)/.test(html.slice(html.indexOf('VIEW 1.5')))
    && /smHidden\(t, rules\)/.test(html.slice(html.indexOf('VIEW 1.5'), html.indexOf('// ═══ VIEW 2'))));
  check('and refuses when the sides differ',
    /DA\.side !== SM\.side \|\| !DA\.graphRef\) return null;/.test(html));
  check('either side panel folds away to give the diagram the width',
    /no-tree/.test(html) && /no-inspector/.test(html) && /panel-fold/.test(html));
  check('the fold survives a reload', /cygenix_schemaexp_panels/.test(html));
  // Compared inside the markup — the same class names appear earlier in the
  // stylesheet, where the order means nothing.
  const body = html.slice(html.indexOf('<body>'));
  check('the tab bar shares the header row rather than taking a line of its own',
    body.indexOf('mode-bar') > body.indexOf('se-title')
    && body.indexOf('mode-bar') < body.indexOf('se-head-actions'));

  // Exclusion rules.
  check('there is a filter control with the rule modes the brief asked for',
    /id="sm-filters-btn"/.test(html)
    && ['contains','starts','ends','equals','regex','schema'].every(m => new RegExp("\\b" + m + ":\\s*\\{").test(html)));
  check('rules survive a reload', /cygenix_schemaexp_filters/.test(html));
  check('there is always a way back to every table',
    /Show all tables/.test(html) && /onclick="smFiltersClear\(\)"[^>]*>Reset</.test(html));
  check('Apply sits next to Reset in the panel foot, Apply first',
    (() => {
      const foot = html.indexOf('sm-filters-foot');
      const a = html.indexOf('>Apply<', foot), r = html.indexOf('>Reset<', foot);
      return foot > -1 && a > -1 && r > -1 && a < r;
    })());
  check('Apply skips the keystroke debounce and closes the panel',
    /function smFiltersApplyNow/.test(html)
    && /smFiltersApplyNow\(\)\{[^]*?clearTimeout\(_filterTimer\)[^]*?smFiltersFlush\(\);[^]*?classList\.remove\('show'\)/.test(html));
  check('the panel is anchored under the toolbar, which wraps at narrow widths',
    /bar\.offsetHeight/.test(html));

  // The load order is a migration plan. Hiding tables from a plan makes it
  // wrong, not tidier — so the coverage view must not read the filter state.
  // Anchored on the JS banner, not the CSS one of the same name, which comes
  // first in the file and would drag the whole schema-map module into the slice.
  const coverage = html.slice(html.indexOf('// ═══ VIEW 2 — COVERAGE MAP'), html.indexOf('// ── Boot ─'));
  check('the coverage view ignores the diagram filters',
    !/smTables\(|SM\.filters|smHidden\(/.test(coverage));

  // Every fallback-prone glyph was replaced after one rendered as a box.
  check('no toolbar glyph falls back to a missing-character box',
    !/[\u26C3\u26F6\u25E7\u25E8]/.test(html));

  // The prototypes shipped their own dark palette. This page must use the
  // app's variables, so a theme change does not leave one page behind.
  check('no colour is hard-coded outside the palette block',
    (html.slice(html.indexOf('--r:10px')).match(/#[0-9a-fA-F]{6}\b/g) || []).length === 0,
    (html.slice(html.indexOf('--r:10px')).match(/#[0-9a-fA-F]{6}\b/g) || []).join(','));
  check('the page uses the app font, not system-ui', !/system-ui/.test(html));

  // getElementById returns the first match, so a duplicate id hands code the
  // wrong element.
  const ids = [...html.matchAll(/\sid="([^"]+)"/g)].map(m => m[1]);
  const seen = new Set(), dupes = new Set();
  ids.forEach(id => { if (seen.has(id)) dupes.add(id); else seen.add(id); });
  check('no element id is used twice', dupes.size === 0, [...dupes].join(', '));

  // Read-only by construction: this page must never write to either database.
  const script = html.slice(html.indexOf('// ═══════'), html.lastIndexOf('</script>'));
  const writes = (script.match(/action:\s*'(\w[\w-]*)'/g) || []);
  check('the page issues no write action', !/insert|update|delete|drop|batch/i.test(writes.join(',')),
    writes.join(','));

  // The canvas must have a route that is not a drag, per the a11y helper.
  check('the diagram is keyboard-reachable', /id="sm-viewport"[\s\S]{0,200}tabindex="0"/.test(html));
  check('and every canvas action has a button, not only a gesture',
    /onclick="smZoom/.test(html) && /onclick="smFit\(\)"/.test(html) && /onclick="smAutoLayout/.test(html));
}

// ── Migration map (semantic subject mapper) ────────────────────────────────
// The classifier is now SubjectMap (tested in subject-map.test.js). These
// tests run the PAGE's own logic — adapter, guard, honest arithmetic — from
// the shipped source, plus the wiring pins that keep the firewall intact.
{
  const amStart = html.indexOf('// ═══ VIEW 1.7 — MIGRATION MAP');
  const amEnd   = html.indexOf('// ── Ensure: harvest both sides');
  if (amStart < 0 || amEnd < 0){
    console.log('  FAIL  could not locate the migration-map block'); fail++;
  } else {
    const sb = { JSON, Math, String, Number, Array, Object, Map, Set, console,
      localStorage: { _d:{}, getItem(k){ return this._d[k] ?? null; }, setItem(k, v){ this._d[k] = String(v); } } };
    vm.createContext(sb);
    vm.runInContext(html.slice(amStart, amEnd), sb);
    const g = (name) => vm.runInContext(name, sb);

    // Adapter: cached-graph shape → engine shape.
    const side = g('amToSide')({ ok: true, tables: [
      { key: 'dbo.matter', schema: 'dbo', name: 'matter', rowCount: 500,
        primaryKeys: ['mmatter'], colsig: 'mmatter:cha,mname:var,mopendt:dat' },
      { key: 'dbo.x', schema: 'dbo', name: 'x', rowCount: 0,
        columns: [{ name: 'a', type: 'int' }] },
    ]});
    check('the adapter turns colsig strings into {name,type} columns',
      side.tables[0].cols.length === 3 && side.tables[0].cols[0].name === 'mmatter'
      && side.tables[0].cols[0].type === 'cha' && side.tables[0].pk[0] === 'mmatter'
      && side.tables[0].rows === 500);
    check('and falls back to lazily-loaded column arrays',
      side.tables[1].cols.length === 1 && side.tables[1].cols[0].name === 'a');

    // The loud guard: a columnless (stale) cache must fail with the cause,
    // never render as everything-Unclassified.
    const A = g('amAssertClassified');
    const mk = (n, unclassified) => ({ src: Array.from({ length: n }, (_, i) =>
      ({ area: i < unclassified ? 'Unclassified' : 'Finance & ledger' })) });
    check('a mostly-unclassified estate fails loudly, naming the harvest',
      (() => { try { A(mk(100, 60)); return false; }
               catch (e){ return /harvest/.test(e.message) && /Rebuild/.test(e.message); } })());
    check('a well-classified estate passes', A(mk(100, 5)) >= 0.8);
    check('a toy estate is exempt — small fixtures classify poorly by nature',
      A(mk(10, 10)) === 1);

    // Honest arithmetic: three buckets that sum to the total, exactly.
    const S = g('amRowsSummary');
    const tables = [
      { key: 'a', rows: 100, area: 'Finance & ledger' },   // accepted
      { key: 'b', rows: 50,  area: 'Finance & ledger' },   // saved map writes it
      { key: 'c', rows: 30,  area: 'Finance & ledger' },   // awaiting decision
      { key: 'd', rows: 20,  area: 'Nowhere' },            // area has no targets
      { key: 'e', rows: 7,   area: 'Finance & ledger' },   // human rejected
    ];
    const sum = S(tables,
      { a: { target: 'dw.A' }, e: { target: null } },
      (t) => t.key === 'b' ? 'dw.B' : null,
      new Set(['Finance & ledger']));
    check('decided + shared + no-home equals the total, exactly',
      sum.decided + sum.shared + sum.noHome === sum.total && sum.total === 207);
    check('an accepted decision and a saved map both count as decided',
      sum.decided === 150 && sum.decidedTables === 2);
    check('a rejection is a no-home decision, not "awaiting"',
      sum.noHome === 27 && sum.shared === 30);

    // Calibration guard from the brief: past ~400 strong the tool is bluffing.
    const C = g('amCalibrationNote');
    check('the reference calibration (112 strong of 2,545) raises no warning',
      C(112, 2545) === '');
    check('drifted thresholds raise the warning', /bluffing|drifted/i.test(C(500, 2545)));
    check('small estates never trigger it', C(30, 40) === '');

    // Saved-map links: brackets stripped, bare names resolve.
    const L = g('amMapLinks')([{ sourceTable: '[dbo].[Invoice_Lines]', targetTables: ['dw.InvoiceFact'] }]);
    check('a bracketed saved map still links the source table',
      L.bySrc.get('dbo.invoice_lines') === 'dw.InvoiceFact' && L.bySrc.get('invoice_lines') === 'dw.InvoiceFact');

    // Overrides persist per side and database, same store the Data map reads.
    g('amSetOverride')('src', 'SrcDb', 'dbo.T', 'Finance & ledger');
    check('area overrides persist per side and database',
      JSON.parse(sb.localStorage._d['cygenix_area_overrides'])['src|SrcDb']['dbo.t'] === 'Finance & ledger');
  }

  // Wiring pins.
  check('the engine and the harvest module are loaded',
    /cygenix-subject-map\.js/.test(html) && /cygenix-subject-harvest\.js/.test(html));
  check('the Migration map is still the registered fourth tab',
    /'schema', 'atlas', 'coverage', 'areas'/.test(html)
    && /id="se-tab-areas"/.test(html) && /⇄ Migration map/.test(html));
  check('four sub-views in the order the work happens',
    /id="am-sub-areas"/.test(html) && /id="am-sub-spine"/.test(html)
    && /id="am-sub-queue"/.test(html) && /id="am-sub-method"/.test(html));
  check('the build runs chunked with determinate progress, never blocking',
    /SubjectMap\.buildAsync/.test(html) && /Scoring candidates/.test(html));
  check('user area corrections are pinned into the build, not re-derived',
    /srcOverrides: amOverrides\('src', sg\.database\)/.test(html));
  check('decisions live in their own store, keyed by the estate pair',
    /cygenix_subjectmap_decisions/.test(html) && /amDecScope/.test(html));
  check('THE FIREWALL: suggestions are never persisted',
    /Suggestions are never persisted/.test(html)
    && !/localStorage\.setItem\([^)]*candidates/.test(html));
  check('accepting hands off to Object Mapping with the table pair ONLY — columns stay human',
    /srcFullName: srcKey, tgtFullName: d\.target/.test(html)
    && /columnMapping: \[\], targetTables: \[\]/.test(html));
  check('the queue is decided on the keyboard', /j\/k move · a accept · r reject/.test(html));
  check('progress is criticality-weighted, with rows as the secondary figure',
    /weighted criticality decided/.test(html) && /weighted by criticality/i.test(html)
    && /amFmt\(prog\.doneRows\)/.test(html));
  // v2 triage wiring.
  check('the triage module is loaded and drives the lanes',
    /cygenix-subject-triage\.js/.test(html) && /amTriageSide\(/.test(html)
    && /id="am-lane-MIGRATE"/.test(html) && /id="am-lane-REVIEW"/.test(html)
    && /id="am-lane-EXCLUDE"/.test(html));
  check('excluded objects keep their reason codes and a one-click restore',
    /amReasonChips/.test(html) && /Restore to the migrate lane/.test(html)
    && /cygenix_triage_overrides/.test(html));
  check('target-side triage feeds the engine so junk targets are refused',
    /tgtTriage: amTgtTriageMap\(\)/.test(html));
  check('family decisions propagate with the inheritance recorded',
    /amPropagateFamily/.test(html) && /via: 'family', propagatedFrom: srcKey/.test(html));
  check('a suppressed margin renders as no candidate with the tied options in the drawer',
    /no candidate — tied/.test(html) && /coin flip, not a recommendation/.test(html));
  check('the KPI reconciliation runs over the in-scope estate, with excluded gated and counted',
    /Excluded from scope/.test(html) && /the in-scope estate, exactly/.test(html));

  // ── The pre-run exclusion picker ─────────────────────────────────────────
  check('the exclusion-rules module is loaded and the toolbar offers the picker',
    /cygenix-exclusion-rules\.js/.test(html) && /⊘ Scope/.test(html) && /id="am-xp"/.test(html));
  check('profiles persist per connection pair and every save bumps the version',
    /cygenix_exclusion_profiles/.test(html) && /prof\.version = \(prof\.version \|\| 0\) \+ 1/.test(html));
  check('write-through: the picker folds into the triage lanes, one source of truth',
    /function amApplyScope/.test(html) && /picker: true/.test(html)
    && /preClass: v\.preClass \|\| v\.class/.test(html));
  check('an explicit human restore outranks a profile rule',
    /if \(v\.restored\) continue;/.test(html));
  check('the ratio guard rides into BOTH build paths',
    (html.match(/ratioGuard: AM\.scope && AM\.scope\.ratioGuard/g) || []).length === 2);
  check('commit is gated on the preview, and 1M-row tables demand acknowledgement',
    /id="am-xp-commit"/.test(html) && /confirm the tables over 1M rows are copies/.test(html)
    && /eff\.big\.length > 0 && !AM\.xp\.ack/.test(html));
  check('the §6 low-row trap carries its warning and is never pre-ticked',
    /candidate targets under 10 rows would be removed/.test(html)
    && /sel\.tgt_low_row = false/.test(html) && /sel\.ratio_guard = true/.test(html));
  check('restore on a picker exclusion rescues — the rule stays intact',
    /if \(v && v\.picker\)\{ xpRescue\('source', key\); return; \}/.test(html)
    && /manual\.rescue\.push/.test(html));
  check('the first run against a new pair offers the picker without forcing it',
    /id="am-xp-offer"/.test(html) && /function xpMaybeOffer/.test(html) && /xpDismissOffer/.test(html));
  check('exports carry the scope profile version for reproducibility',
    /scope_profile_v/.test(html));
  check('metadata freshness and the usage observation window are on the estate bar',
    /metadata ' \+ \(h < 1/.test(html) && /usage over/.test(html));
  check('the scoring weights are on show — a ranking nobody can inspect is not trusted',
    /SubjectMap\.W/.test(html) && /id="am-method-weights"/.test(html));
  check('the Data map still reads the same assignment store',
    /amOverrides\(DA\.side, graph\.database\)/.test(html) && /cygenix_area_overrides/.test(html));
  check('subject-area override names are translated for the atlas, unknown ones ignored',
    /DA_FROM_SUBJECT/.test(html) && /DA_ATLAS_IDS\.has\(ov\)/.test(html));
  check('an area correction still invalidates the built atlas',
    /DA\.loaded = null; DA\.tables = \[\];/.test(html));
  check('reload schema forces a full re-harvest on this tab',
    /seTab === 'areas'\)\{ AM\.ready = false; amEnsure\(true\); \}/.test(html));
  check('the fit report export ships every table with its tier and decision',
    /cygenix-fit-report\.csv/.test(html) && /top_candidate,score,margin,decision/.test(html));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
