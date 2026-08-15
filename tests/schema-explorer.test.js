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

  check('both tabs are present', /se-tab-schema/.test(html) && /se-tab-coverage/.test(html));
  check('the tab choice persists under its own key', /cygenix_schemaexp_tab/.test(html));
  check('the source/target toggle exists and defaults to target',
    /sm-side-tgt/.test(html) && /side:\s*'tgt'/.test(html));
  check('there is a visible reload for the cached schema', /Reload schema/.test(html));

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
