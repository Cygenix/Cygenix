// Structural tests for the Data Maps module — the four extra Schema Explorer
// map models (Estate, Relationship, Radial burst, Constellation).
//
// The drawing code is SVG and belongs to the browser smoke; what is testable
// here is the part that decides what gets drawn: which domain a table lands
// in, which tables collapse into one family, and — the piece that is easy to
// get quietly wrong — where a foreign key is taken to point. The console's
// fkColumns are the COLUMNS in a key, not the tables they reference, so the
// adapter has to prefer the graph's edge list whenever it is handed one.
const fs = require('fs');
const vm = require('vm');

const noopEl = () => ({ classList:{add(){},remove(){},toggle(){},contains:()=>false},
  addEventListener(){}, setAttribute(){}, appendChild(){}, style:{}, dataset:{}, textContent:'' });
const sandbox = {
  window: { addEventListener(){}, innerWidth: 1440, innerHeight: 900 },
  document: {
    // No .mode-bar in this DOM: install() must decline rather than throw.
    querySelector: () => null, getElementById: () => null, querySelectorAll: () => [],
    createElement: noopEl, head: noopEl(), body: noopEl(),
  },
  console, setInterval: () => 0, clearInterval(){}, setTimeout: () => 0,
};
sandbox.window.document = sandbox.document;
vm.createContext(sandbox);
vm.runInContext(fs.readFileSync(__dirname + '/../public/cygenix-data-maps.js', 'utf8'), sandbox);

const DM = sandbox.window.CygenixDataMaps;
const PAGE = fs.readFileSync(__dirname + '/../public/schema_explorer.html', 'utf8');
const SRC  = fs.readFileSync(__dirname + '/../public/cygenix-data-maps.js', 'utf8');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Data Maps — estate adapter and registration\n');

check('module loads under a DOM with no tab bar and exports its API',
  !!(DM && DM.fromSchema && DM.classify && DM.familyKey && DM.install && DM.refresh));
check('install() declines when there is no .mode-bar to register on', DM.install() === false);
check('four tabs, one per map model', DM.TABS.length === 4 &&
  DM.TABS.map(t => t.id).join(',') === 'estate,flow,burst,constellation');
check('eight domains, eight palette slots',
  DM.DOMAIN_NAMES.length === 8 && DM.PALETTE.length === 8);

// ── 1. Domain classification ────────────────────────────────────────────────
const DOMAIN_CASES = [
  ['ArInvoice', 0], ['GlPosting', 0], ['TrustBalance', 0],
  ['TimeEntry', 1], ['ProformaHeader', 1], ['Timekeeper', 1],
  ['ClientMatter', 2], ['Engagement', 2], ['ConflictCheck', 2],
  ['DocumentStore', 3], ['MailAttachment', 3],
  ['EmployeeRole', 4], ['StaffPermission', 4],
  ['CountryList', 5], ['RegionLookup', 5], ['LanguageCode', 5],
  ['cv_ClientBackup', 6], ['AuditTrail', 6], ['SyncQueue', 6],
  ['Widget', 7],
];
const misclassified = DOMAIN_CASES.filter(([n, d]) => DM.classify(n) !== d)
  .map(([n, d]) => n + ' → ' + DM.classify(n) + ' (want ' + d + ')');
check('every domain vocabulary lands in its own slot (' + DOMAIN_CASES.length + ')',
  misclassified.length === 0, misclassified.join(', '));
check('classification is case-insensitive', DM.classify('ARINVOICE') === DM.classify('arinvoice'));
check('a conversion prefix beats the business word inside it',
  DM.classify('cv_ClientMatter') === 6 && DM.classify('ClientMatter') === 2);
// Earlier rules win outright: "currency" is a finance word before it is a
// lookup one, and a migration table named after what it holds is filed under
// that. Pinned because the reading of a map depends on the order, not on any
// one pattern.
check('the first matching rule wins, in the order the classifier lists them',
  DM.classify('CurrencyCode') === 0 && DM.classify('MxMissingTime') === 1);

// ── 2. Family collapse ──────────────────────────────────────────────────────
check('tables differing only by a trailing number are one family',
  DM.familyKey('MxMissingTime9828') === DM.familyKey('MxMissingTime9843'));
check('the trailing number is dropped, not masked', DM.familyKey('MxMissingTime9828') === 'MxMissingTime');
check('an underscore before the number goes with it', DM.familyKey('Batch_17') === 'Batch');
check('digits inside the name are masked, not dropped', DM.familyKey('AR2Invoice3') === 'AR#Invoice');
check('a name with no digits is its own family', DM.familyKey('Client') === 'Client');

// ── 3. fromSchema totals ────────────────────────────────────────────────────
const t = (name, rowCount, extra) => Object.assign({
  name, key: 'dbo.' + name, rowCount, kind: 'table', columns: [], fkColumns: [],
}, extra || {});

const TABLES = [
  t('Client', 5000), t('Client1', 300), t('Client2', 200),   // one family, 3 tables
  t('ArInvoice', 1200),
  t('TimeEntry', 900),
  t('DocumentStore', 0),                                      // empty — counted, not drawn
  t('AuditTrail', 40),
  t('Widget', 0),                                             // empty
];
const D = DM.fromSchema(TABLES);

check('totals count every table handed over', D.totals.tables === 8);
check('totals sum every row', D.totals.rows === 5000 + 300 + 200 + 1200 + 900 + 40);
check('empty tables are counted', D.totals.empty === 2);
check('only row-bearing families are drawn', D.families.every(f => f.rows > 0));
check('an empty family is left out of the drawn set',
  !D.families.some(f => f.name === 'DocumentStore' || f.name === 'Widget'));
check('families are ordered heaviest first',
  D.families.every((f, i) => i === 0 || D.families[i - 1].rows >= f.rows));
check('the collapsed family carries its member count and their rows',
  D.families[0].name === 'Client' && D.families[0].n === 3 && D.families[0].rows === 5500);
check('every family knows its own index', D.families.every((f, i) => f.i === i));

const fin = D.domains[0], mat = D.domains[2], doc = D.domains[3];
check('domain row totals are per domain', fin.rows === 1200 && mat.rows === 5500);
check('domain table counts count tables, not families', mat.tables === 3);
check('an all-empty domain still shows its tables', doc.tables === 1 && doc.rows === 0 && doc.empty === 1);
const tailOk = D.domains.every(d => d.listedRows + d.tailRows === d.rows &&
                                    d.listedTables + d.tailTables === d.tables);
check('listed + tail reconciles to the domain total in every domain', tailOk);

// ── 4. Where a foreign key is taken to point ────────────────────────────────
// The graph's edges are table→table. The chord/link view has to use them when
// they exist, because a column called "ClientId" is not the Client table.
const FK_TABLES = [
  t('ArInvoice', 1000, { fkColumns: ['ClientId'] }),
  t('Client', 2000),
  t('Ghost', 10),
];
const EDGES = [{ from:'dbo.ArInvoice', to:'dbo.Client' }];

const withEdges = DM.fromSchema(FK_TABLES, EDGES);
const iInv = withEdges.families.findIndex(f => f.name === 'ArInvoice');
const iCli = withEdges.families.findIndex(f => f.name === 'Client');
check('an edge becomes a link between the two real families',
  withEdges.links.length === 1 && withEdges.links[0].s === iInv && withEdges.links[0].t === iCli);
check('an edge counts once in the domain matrix, finance → matters',
  withEdges.matrix[0][2] === 1 && withEdges.matrix[0][0] === 0);

const noEdges = DM.fromSchema(FK_TABLES);
check('with no edge list the column name is all there is to go on',
  noEdges.matrix[0][2] === 1 && noEdges.links.length === 0);
check('edges and fkColumns are never double counted',
  withEdges.matrix.flat().reduce((a, b) => a + b, 0) === 1);

const pairs = DM.refPairs(FK_TABLES, EDGES);
check('refPairs resolves edge keys to table names',
  pairs.length === 1 && pairs[0][0] === 'ArInvoice' && pairs[0][1] === 'Client');
check('an edge to a table that is filtered out is dropped, not guessed',
  DM.refPairs(FK_TABLES, [{ from:'dbo.ArInvoice', to:'dbo.Hidden' }]).length === 1 &&
  DM.refPairs([FK_TABLES[0]], [{ from:'dbo.ArInvoice', to:'dbo.Client' }])
    .every(p => p[1] === 'ClientId'));
check('an empty edge list falls back to fkColumns rather than showing no keys',
  DM.refPairs(FK_TABLES, []).length === 1);

const selfRef = DM.fromSchema([t('Client', 100), t('Client1', 50)],
  [{ from:'dbo.Client1', to:'dbo.Client' }]);
check('two tables in one family produce no link to themselves', selfRef.links.length === 0);

// A schema with nothing in it must adapt to an empty picture, not throw.
const empty = DM.fromSchema([]);
check('an empty schema adapts without throwing',
  empty.totals.tables === 0 && empty.families.length === 0 && empty.links.length === 0);
check('an empty schema still carries all eight domains', empty.domains.length === 8);

// ── 5. The family cap ───────────────────────────────────────────────────────
// Digit-free names, so each one is genuinely its own family — anything ending
// in a number would collapse into a handful of families and test nothing.
const letters = n => { let s = ''; do { s = String.fromCharCode(65 + n % 26) + s; n = Math.floor(n / 26) - 1; } while (n >= 0); return s; };
const MANY = [];
for (let i = 0; i < 400; i++) MANY.push(t('Fam' + letters(i), 400 - i));
const capped = DM.fromSchema(MANY);
check('the drawn set is capped', capped.families.length <= 260);
check('the cap keeps the heaviest families',
  capped.families[0].rows === 400 && capped.families.every(f => f.rows > 0));
check('totals still describe the whole estate, not the capped set',
  capped.totals.tables === 400 && capped.totals.listed === capped.families.length);
const cappedTail = capped.domains.every(d => d.listedRows + d.tailRows === d.rows);
check('what the cap left out is carried as the domain tail', cappedTail);

// ── 6. Purity — these maps read the estate, they never touch it ─────────────
check('no randomness: the same estate always draws the same map', !/Math\.random\s*\(/.test(SRC));
check('the module fetches nothing', !/\bfetch\s*\(/.test(SRC));
check('the module stores nothing', !/localStorage|sessionStorage/.test(SRC));
check('no baked-in table count survives from the prototype', !/12,245/.test(SRC));

// ── 7. Page wiring ──────────────────────────────────────────────────────────
check('the page loads the module', /<script src="\/cygenix-data-maps\.js[^"]*" defer><\/script>/.test(PAGE));
check('the module loads after the schema graph it reads through',
  PAGE.indexOf('cygenix-data-maps.js') > PAGE.indexOf('cygenix-schema-graph.js'));
check('the tab bar wraps — eight tabs no longer fit on one line',
  /\.mode-bar\{[^}]*flex-wrap:wrap/.test(PAGE));
check('the tab buttons size to their labels instead of splitting the width',
  /\.mode-btn\{flex:0 1 auto/.test(PAGE));
check('the page exposes the declared edges the maps draw from',
  /function smEdges\(\)/.test(PAGE));
check('smEdges reports only edges between visible tables',
  /keys\.has\(e\.from\) && keys\.has\(e\.to\)/.test(PAGE));
check('inferred edges stay out of the maps — a hypothesis is not a foreign key',
  !/smEdges[\s\S]{0,400}infEdges/.test(PAGE));
check('↻ Reload schema reloads the data maps when one of them is showing',
  /CygenixDataMaps\.isActive\(\)/.test(PAGE) &&
  /smEnsureLoaded\(true\)\.then\(\(\) => CygenixDataMaps\.refresh\(\)\)/.test(PAGE));
check('the four native tabs are untouched',
  ['se-tab-schema','se-tab-atlas','se-tab-coverage','se-tab-areas'].every(k => PAGE.includes(k)));

// The console paints selection with .active; aria-selected alone leaves the
// tab you left looking lit. Both have to move, in both directions.
check('registration moves .active with aria-selected',
  /classList\.toggle\('active', on\)/.test(SRC) && /setAttribute\('aria-selected', String\(on\)\)/.test(SRC));
check('our tabs do not inherit the native active state they were cloned from',
  /replace\(\/\\bactive\\b\/g, ''\)/.test(SRC));
check('surfaces follow the console theme rather than a baked white',
  /--cdm-surface:var\(--bg2/.test(SRC) && /getPropertyValue\('--cdm-surface'\)/.test(SRC));
// A domain with no keys has a zero-width arc; labelling it stacked every
// empty domain's label at the top of the ring and pushed the pile over the
// card header.
check('the chord does not label a domain that has no foreign keys',
  /if \(!sums\[k\]\) return;/.test(SRC));
check('a chord label is never nudged off the canvas',
  /ly = Math\.max\(14, Math\.min\(H - 20, ly\)\);/.test(SRC));
check('an unloaded schema is reported, not drawn as an empty estate',
  /if \(tables && tables\.length\)/.test(SRC) && /Loading schema…/.test(SRC));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
