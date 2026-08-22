// Tests for the Globe view: the module's own contract, the boundary assets it
// draws from, and the page wiring.
//
// The aggregation engine is tested next door in geo-index.test.js; the drawing
// is exercised by the browser smoke. What is worth pinning here is the part in
// between — that the module registers without a DOM to register into, that the
// committed geography is well formed, and that the scale on which every bar is
// drawn is the log one the legend claims.
const fs = require('fs');
const vm = require('vm');
const path = require('path');

const noopEl = () => ({ classList:{add(){},remove(){},toggle(){},contains:()=>false},
  addEventListener(){}, setAttribute(){}, getAttribute:()=>null, appendChild(){},
  insertBefore(){}, querySelector:()=>null, querySelectorAll:()=>[], style:{}, dataset:{},
  textContent:'', children:[] });
const sandbox = {
  window: { addEventListener(){}, innerWidth: 1440, innerHeight: 900,
            matchMedia: () => ({ matches: false }), devicePixelRatio: 1 },
  document: {
    // No tab bar: install() must decline rather than throw.
    querySelector: () => null, getElementById: () => null, querySelectorAll: () => [],
    createElement: noopEl, head: noopEl(), body: noopEl(), hidden: false,
    addEventListener(){},
  },
  console, setInterval: () => 0, clearInterval(){}, setTimeout: () => 0,
  requestAnimationFrame: () => 0, fetch: () => Promise.reject(new Error('no network in tests')),
};
sandbox.window.document = sandbox.document;
vm.createContext(sandbox);
vm.runInContext(fs.readFileSync(__dirname + '/../public/cygenix-geo-index.js', 'utf8'), sandbox);
vm.runInContext(fs.readFileSync(__dirname + '/../public/cygenix-globe.js', 'utf8'), sandbox);

const GL = sandbox.window.CygenixGlobe;
const PAGE = fs.readFileSync(__dirname + '/../public/schema_explorer.html', 'utf8');
const SRC  = fs.readFileSync(__dirname + '/../public/cygenix-globe.js', 'utf8');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Globe — module, assets and wiring\n');

// ── 1. The module ───────────────────────────────────────────────────────────
check('the module loads with no tab bar to register into',
  !!(GL && GL.install && GL.mount && GL.refresh && GL.isActive));
check('install() declines rather than throwing', GL.install() === false);
check('it is not active until its panel is on screen', GL.isActive() === false);
check('nothing is discovered on load', GL.index === null && GL.ledger === null);
check('it starts at world level with every business area on',
  GL.state.drill === null && GL.state.doms.size === 8 && GL.state.metric === 'rows');
check('and defaults to counting records, with suppression off',
  GL.state.metric === 'rows' && GL.state.minCell === 0 && GL.state.includeShadow === false);

// ── 2. The scale the legend claims ──────────────────────────────────────────
const h = (v, max) => GL.__barHeight(v, max);
check('nothing is drawn for nothing', h(0, 1000) === 0);
check('a bar grows with its value', h(10, 1000) < h(100, 1000) && h(100, 1000) < h(1000, 1000));
check('the largest value reaches the top of the scale',
  Math.abs(h(1000, 1000) - 0.46) < 1e-9, String(h(1000, 1000)));
// The point of a log scale: ten times the value is NOT ten times the bar, so a
// long tail stays visible instead of lying flat against the surface.
const tenth = h(1000, 1e6), full = h(1e6, 1e6);
check('a value a thousand times smaller still stands off the surface',
  tenth > full * 0.4, tenth + ' vs ' + full);
check('and a linear scale would have buried it', tenth > full * 0.001 * 10);

// ── 3. The committed geography ──────────────────────────────────────────────
const geoDir = path.join(__dirname, '..', 'public', 'geo');
check('the world boundary file is committed', fs.existsSync(path.join(geoDir, 'countries-110m.json')));
const world = JSON.parse(fs.readFileSync(path.join(geoDir, 'countries-110m.json'), 'utf8'));
check('it carries a country count worth drawing', world.features.length >= 170,
  String(world.features.length));
const ids = world.features.map(f => f.id);
check('one feature per ISO code — France and Clipperton Island share one in the source',
  new Set(ids).size === ids.length, 'dupes: ' + ids.filter((x, i) => ids.indexOf(x) !== i).join(','));
check('and no placeholder code survives', !ids.includes('-99') && ids.every(i => /^[A-Z]{3}$/.test(i)),
  ids.filter(i => !/^[A-Z]{3}$/.test(i)).join(','));
const badC = world.features.filter(f => !f.c || Math.abs(f.c[0]) > 180 || Math.abs(f.c[1]) > 90);
check('every country has a centroid on the planet', badC.length === 0,
  badC.map(f => f.id).join(','));
const fra = world.features.find(f => f.id === 'FRA');
check('the centroid is the mainland\'s, not an average over every island',
  fra && fra.c[0] > -5 && fra.c[0] < 10 && fra.c[1] > 40 && fra.c[1] < 52, JSON.stringify(fra && fra.c));
const usa = world.features.find(f => f.id === 'USA');
check('so the United States is not in the Pacific',
  usa && usa.c[0] < -80 && usa.c[0] > -110, JSON.stringify(usa && usa.c));
check('the world file stays small enough for a first paint',
  fs.statSync(path.join(geoDir, 'countries-110m.json')).size < 400 * 1024,
  Math.round(fs.statSync(path.join(geoDir, 'countries-110m.json')).size / 1024) + 'KB');
check('rings are closed lists of coordinate pairs',
  world.features.every(f => f.r.length && f.r.every(r => r.length >= 4 && r.every(p => p.length === 2))));

const a1dir = path.join(geoDir, 'admin1');
check('admin-1 boundaries are committed per country, fetched only on drill',
  fs.existsSync(a1dir) && fs.readdirSync(a1dir).length >= 150,
  String(fs.existsSync(a1dir) ? fs.readdirSync(a1dir).length : 0));
const gbr = JSON.parse(fs.readFileSync(path.join(a1dir, 'GBR.json'), 'utf8'));
const gbrNames = gbr.features.map(f => f.name).sort();
// Natural Earth's admin-1 for the UK is local authority districts, which is
// not what a postcode resolves to and not what anybody means by "region".
check('the UK divides into the four nations, matching what a postcode resolves to',
  gbrNames.join(',') === 'England,Northern Ireland,Scotland,Wales', gbrNames.join(','));
const G = require('../public/cygenix-geo-index.js');
const fromPostcodes = ['EH1 2AB', 'CF10 1AA', 'BT1 5GS', 'SW1A 1AA']
  .map(p => G.geoPostcodeAdmin1(p, 'GBR'));
check('and every one of those is a boundary the globe can actually draw',
  fromPostcodes.every(n => gbrNames.includes(n)), fromPostcodes.join(','));
const usStates = JSON.parse(fs.readFileSync(path.join(a1dir, 'USA.json'), 'utf8'));
check('US states are matchable by their postal code, which is what a ZIP gives',
  usStates.features.some(f => (f.k || []).includes('ny'))
  && usStates.features.some(f => (f.k || []).includes('ca')));
check('no single country file is heavy enough to stall a drill',
  fs.readdirSync(a1dir).every(f => fs.statSync(path.join(a1dir, f)).size < 1024 * 1024),
  fs.readdirSync(a1dir).filter(f => fs.statSync(path.join(a1dir, f)).size >= 1024 * 1024).join(','));
check('the generator that built them is committed alongside',
  fs.existsSync(path.join(__dirname, '..', 'scripts', 'build-geo.py')));

// ── 4. Privacy and purity ───────────────────────────────────────────────────
check('the globe reads the ledger and never issues a query of its own',
  !/SELECT /i.test(SRC.replace(/SELECT TOP 400 \* FROM/g, '')) || /geoRun/.test(SRC));
check('the only statement it composes is the reference-table read',
  (SRC.match(/SELECT /gi) || []).length === 1);
check('it stores nothing', !/localStorage|sessionStorage/.test(SRC));

// db-connect answers an execute with { success, rowsAffected, recordset }.
// Reading only .rows returns nothing from every query while every query
// reports success — a full set of answers and an empty map. Every other
// module in the console reads recordset; this one must too.
check('query results are read from the envelope db-connect actually sends',
  /res\.recordset \|\| res\.rows/.test(SRC), 'globe reads .rows only');
const readsRaw = (SRC.match(/res\.rows/g) || []).length;
check('and nowhere reaches past the one normaliser to do it by hand',
  readsRaw === 1 && (SRC.match(/resultRows/g) || []).length >= 3,
  readsRaw + ' raw .rows reads');
const OTHERS = ['cygenix-evidence-map.js', 'cygenix-nl-recon.js', 'cygenix-preflight.js',
                'cygenix-subject-harvest.js', 'cygenix-infer-schema.js'];
check('which is what the rest of the console does (' + OTHERS.length + ' modules)',
  OTHERS.every(f => /recordset/.test(fs.readFileSync(__dirname + '/../public/' + f, 'utf8'))));
check('and fetches nothing but its own boundary files',
  (SRC.match(/fetch\(/g) || []).length === 2 && /GEO_PATH \+ 'countries-110m\.json'/.test(SRC)
  && /GEO_PATH \+ 'admin1\/'/.test(SRC));

// ── 5. Lifecycle ────────────────────────────────────────────────────────────
check('the render loop stops when the view is off screen or the tab is hidden',
  /if \(!isActive\(\) \|\| document\.hidden\) return;/.test(SRC));
check('and is only ever scheduled once at a time', /if \(G\.raf \|\| !isActive\(\)/.test(SRC));
check('motion is dropped for anyone who asked for less of it',
  /prefers-reduced-motion/.test(SRC) && /reduced\(\) \? 0 : 0\.94/.test(SRC));
check('the canvas is reachable by keyboard', /canvas\.tabIndex = 0/.test(SRC)
  && /ArrowLeft/.test(SRC) && /aria-label/.test(SRC));
check('and every value it draws is also in a table',
  /\['globe','Globe'\],\['table','Table'\],\['sources','Sources'\],\['unresolved','Unresolved'\]/.test(SRC));

// ── 6. Page wiring ──────────────────────────────────────────────────────────
check('the page loads the engine and the view',
  /<script src="\/cygenix-geo-index\.js[^"]*" defer><\/script>/.test(PAGE)
  && /<script src="\/cygenix-globe\.js[^"]*" defer><\/script>/.test(PAGE));
check('after the data maps, so the Globe tab lands last in the row',
  PAGE.indexOf('cygenix-globe.js') > PAGE.indexOf('cygenix-data-maps.js'));
check('and after the full-screen helper it attaches to',
  PAGE.indexOf('cygenix-globe.js') > PAGE.indexOf('cygenix-fullscreen.js'));
check('the engine loads before the view that calls it',
  PAGE.indexOf('cygenix-geo-index.js') < PAGE.indexOf('cygenix-globe.js'));
check('the tab bar comment counts nine', /Nine buttons now/.test(PAGE));

// The globe borrows the data maps' chrome, so that stylesheet has to be there
// whichever tab the user opens first.
const DM = fs.readFileSync(__dirname + '/../public/cygenix-data-maps.js', 'utf8');
check('the shared chrome stylesheet is injected on install, not on first mount',
  /if \(!bar \|\| document\.getElementById\('se-tab-dm-estate'\)\) return false;\s*\n\s*\/\*[\s\S]{0,200}\*\/\s*\n\s*injectCSS\(\);/.test(DM));
check('and the globe asks for it too, so tab order cannot matter',
  /CygenixDataMaps\.injectStyles/.test(SRC) && /injectStyles: injectCSS/.test(DM));

check('the business areas are the app\'s, not a second vocabulary',
  /window\.CygenixDataMaps\.classify\(n\)/.test(SRC) && /CygenixDataMaps && window\.CygenixDataMaps\.PALETTE/.test(SRC));
check('full screen goes through the shared helper',
  /window\.CygenixFullscreen\.attach\(/.test(SRC) && /visible: function \(\) \{ return isActive\(\); \}/.test(SRC));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
