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
  }
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
    /src="\/cygenix-schema-graph\.js"/.test(html));
  check('it inherits the auth gate', /src="\/auth-gate\.js"/.test(html));
  check('and the token script', /src="\/cygenix-auth-token\.js"/.test(html));
  check('and connections, for the source and target strings', /src="\/connections\.js"/.test(html));
  check('and the project context', /src="\/cygenix-cosmos-sync\.js"/.test(html));
  check('and cookie consent', /src="\/cookie-consent\.js"/.test(html));

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
  check('the diagram can go full screen', /id="sm-fs-btn"/.test(html) && /requestFullscreen/.test(html));
  check('and the canvas keeps its own background there, since the browser paints black behind it',
    /:fullscreen\{[^}]*background:var\(--bg\)/.test(html));
  check('leaving full screen re-measures rather than keeping the old zoom',
    /function smOnFullscreenChange\(\)[\s\S]{0,900}smFit\(\)/.test(html)
    && /addEventListener\('fullscreenchange', seOnFullscreenChange\)/.test(html));
  // The router keeps the two canvases apart: refitting the schema canvas
  // while the atlas panel owns the screen would measure a hidden 0×0 box.
  check('fullscreen changes route to whichever view owns the element',
    /contains\('da-panel'\)\) daOnFullscreenChange\(\);/.test(html)
    && /else smOnFullscreenChange\(\);/.test(html));
  check('the atlas has its own full screen control',
    /id="da-fs-btn"/.test(html) && /function daFullscreen\(\)/.test(html));
  check('and the atlas view flexes to the viewport instead of a fixed offset',
    /\.da-view\{display:flex;flex-direction:column;height:calc\(100vh/.test(html)
    && /\.da-body\{[^}]*flex:1;min-height:0\}/.test(html));
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
  check('there is always a way back to every table', /Show all/.test(html));
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

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
