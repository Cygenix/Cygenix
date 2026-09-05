// tests/data-analyser.test.js — the Data Analyser is wired in, under a name
// that cannot collide, and keeps the promise its page makes.
//
// WHAT WENT WRONG BEFORE IT SHIPPED
//
// The engine arrived as cygenix-transform.js, defining a global called
// CygenixTransform. This repository already had both: public/cygenix-transform.js
// is the module that defines what a column transform does to a value inside
// the migration runner, and it defines the same global. The existing one
// guards its assignment; the new one overwrote unconditionally. Which module
// a page got would have depended on script order — and the Pipelines screen,
// which loads the runner's module, would have been the first thing to break,
// silently, on a page that has nothing to do with file analysis.
//
// So the engine and its widget were renamed on the way in. This suite pins
// the rename, pins that BOTH modules can now share a page, and pins the claim
// the tab makes to the user — "nothing leaves this page" — against the code
// rather than against the copy.
//
// WHERE IT LIVES
// It was a page of its own at /data-analyser. It is now the Data import tab
// of the dashboard's Connections view, alongside — not instead of — the
// importer that was already there: several files still auto-import as
// dbo.<filename>, one file still reaches the schema preview, and the old
// address redirects to the tab. Sections 2, 2b, 4 and 5 pin all of that.
'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const P = (...s) => path.join(__dirname, '..', ...s);
const read = (...s) => fs.readFileSync(P(...s), 'utf8');

console.log('Data Analyser — on the Data import tab, under a name that cannot collide\n');

/* ── 1. The rename, and the collision it avoids ─────────────────────────── */

const engine = read('public', 'cygenix-analyser.js');
const widget = read('public', 'cygenix-analyser-widget.js');
const oldTransform = read('public', 'cygenix-transform.js');

check('the engine exists under its new name', engine.length > 10000);
// The header is REQUIRED to mention the old global — that is how the next
// person learns why the rename exists — so this looks for an assignment, not
// a mention.
check('and exposes CygenixAnalyser, not the colliding global',
  /root\.CygenixAnalyser = factory\(\)/.test(engine) && !/root\.CygenixTransform\s*=/.test(engine));
check('the widget reads the engine from the new global and exposes itself under one',
  /root\.CygenixAnalyserWidget = factory\(root\.CygenixAnalyser\)/.test(widget)
  && !/CygenixTransform/.test(widget));
check('the widget\'s CommonJS path points at the renamed engine',
  /require\('\.\/cygenix-analyser\.js'\)/.test(widget));
check('the runner\'s transform module is untouched and still owns its global',
  /root\.CygenixTransform = api/.test(oldTransform),
  'the rename was to avoid this file, not to change it');
check('both headers say why the rename happened, so nobody undoes it',
  /already has a\s+public\/cygenix-transform\.js/.test(engine) && /collided/.test(widget));

// The actual test of the collision: load the runner's module AND the new
// engine into one global scope. Both must survive.
{
  const ctx = { console: { log() {}, warn() {}, error() {} } };
  ctx.self = ctx; ctx.window = ctx; ctx.globalThis = ctx;
  vm.createContext(ctx);
  let err = '';
  try {
    vm.runInContext(oldTransform, ctx, { filename: 'cygenix-transform.js' });
    vm.runInContext(engine, ctx, { filename: 'cygenix-analyser.js' });
    vm.runInContext(widget, ctx, { filename: 'cygenix-analyser-widget.js' });
  } catch (e) { err = e.message; }
  check('the runner\'s module, the engine and the widget load together without error', !err, err);
  check('and all three globals coexist',
    ctx.CygenixTransform && ctx.CygenixAnalyser && ctx.CygenixAnalyserWidget
    && typeof ctx.CygenixAnalyser.detect === 'function'
    && typeof ctx.CygenixAnalyserWidget.mount === 'function',
    Object.keys(ctx).filter((k) => /^Cygenix/.test(k)).join(', '));
  check('the runner\'s module is still the runner\'s module, not the analyser wearing its name',
    ctx.CygenixTransform && typeof ctx.CygenixTransform.detect !== 'function');
}

/* ── 2. Where it lives: the Data import tab of Connections ──────────────── */
//
// It had its own page for a short while. It now lives inside the dashboard's
// Connections view, on the Data import tab — the tab keeps its name and its
// place in the strip, and the analyser is mounted into it lazily. The old
// address redirects there (section 5).

const dash = read('public', 'dashboard.html');
const app = read('public', 'dashboard-app.js');
check('the standalone page is gone', !fs.existsSync(P('public', 'data-analyser.html')));
check('the tab is still called Data import, in its old place in the strip',
  (() => {
    const tabs = (dash.match(/data-tab="([a-z]+)"/g) || []).map((s) => s.replace(/data-tab="|"/g, ''));
    return tabs.join(',') === 'databases,import,restore,linked,blob'
      && /data-tab="import"[^>]*>\s*Data import\s*</.test(dash);
  })(), (dash.match(/data-tab="([a-z]+)"/g) || []).join(','));
check('the dashboard loads the engine before the widget, which requires it, and the hand-off',
  dash.indexOf('/cygenix-analyser.js') > 0
  && dash.indexOf('/cygenix-analyser.js') < dash.indexOf('/cygenix-analyser-widget.js')
  && dash.indexOf('/cygenix-analyser-widget.js') < dash.indexOf('/cygenix-analyser-handoff.js')
  && dash.indexOf('/cygenix-analyser-handoff.js') < dash.indexOf('/dashboard-app.js'));
check('the host element sits inside the import tab',
  (() => {
    const tab = dash.slice(dash.indexOf('id="conn-tab-import"'), dash.indexOf('id="conn-tab-linked"'));
    return /<div id="analyser"><\/div>/.test(tab) && /id="da-handoff"/.test(tab) && /id="imp-analyser-wrap"/.test(tab);
  })());
check('and is mounted lazily, on the first visit to the tab, not at boot',
  /if \(tab === 'import'\)\s*\{[^}]*impAnalyserInit\(\)/.test(app)
  && /function impAnalyserInit\(\)\{\s*if \(impAnalyser\) return impAnalyser;/.test(app)
  // Every call site is inside a function; none runs as the script loads.
  && (app.match(/^impAnalyserInit\(\);/gm) || []).length === 0);
check('the widget follows the console theme rather than fixing its own',
  /theme: impAnalyserTheme\(\)/.test(app) && /attributeFilter: \['data-theme'\]/.test(app));
check('the console\'s accent and type are imposed by id, since the widget injects later',
  /#analyser\.cygx\{[^}]*--accent:#4A5BD6/.test(dash) && /#analyser\.cygx\{[^}]*font-family:var\(--serif\)/.test(dash));
check('the widget\'s own single-file drop zone is hidden; the tab\'s multi-file one is the entry point',
  /#analyser\.cygx \.cygx-drop\{\s*display:none;?\s*\}/.test(dash)
  && /id="imp-file"[^>]*\bmultiple\b/.test(dash));
check('the optional libraries are pinned, from the one CDN the console already trusts',
  /cdnjs\.cloudflare\.com\/ajax\/libs\/xlsx\/0\.18\.5\//.test(dash)
  && /cdnjs\.cloudflare\.com\/ajax\/libs\/js-yaml\/4\.1\.0\//.test(dash)
  && !/unpkg/.test(dash));

/* ── 2b. The import half survived the move ──────────────────────────────── */
//
// The hard requirement of the consolidation: several files still auto-import
// to the source database as dbo.<filename>, one file still goes to the schema
// preview, and the limits are the same numbers they were.

check('one file goes to the analyser AND the import parser; several go to the batch',
  /function impHandleFiles\(fileList\)\{[\s\S]*?impAnalyserProfile\(fileList\[0\]\)[\s\S]*?impHandleFile\(fileList\[0\]\)[\s\S]*?impBatchPrepare\(Array\.from\(fileList\)\)/.test(app));
check('the batch names tables dbo.<filename> and defaults to the source database',
  /function impBatchTableName\(/.test(app)
  && /<select id="imp-batch-db"[\s\S]*?<option value="src" selected>Source database<\/option>/.test(dash));
check('the limits are unchanged', /IMP_XLSX_HARD_CAP\s*=\s*200000/.test(app) && /IMP_CSV_HARD_CAP\s*=\s*2000000/.test(app));
check('and so is the helper text that states them',
  /CSV: streams up to ~2M rows · Excel: up to 200k rows/.test(dash)
  && /Single file → schema preview · Multiple files → auto-import to <strong>source<\/strong> DB as <code[^>]*>dbo\.&lt;filename&gt;<\/code>/.test(dash));
check('a file the analyser reads but the importer cannot is a note, not an error',
  /impShowNote\('Profiled above\. Import to a database needs CSV or Excel/.test(app));
check('the Blob Source hand-in goes through the same router, so it is profiled too',
  /impHandleFiles\(\[file\]\)/.test(app) && !/\bimpHandleFile\(file\);\s*\}\s*catch/.test(app));

/* ── 3. The promise the tab makes ───────────────────────────────────────── */
//
// "Nothing leaves this page." That is the reason a prospect can try it with
// real data, and it is only true if the code has no way to send anything.

check('the tab says nothing leaves it', /Nothing leaves this page/.test(dash));
const NET = /\bfetch\s*\(|XMLHttpRequest|navigator\.sendBeacon|new WebSocket|EventSource\(/;
check('the engine has no network path at all', !NET.test(engine));
check('neither has the widget', !NET.test(widget));
check('and neither writes browser storage, so the storage inventory is untouched',
  !/localStorage|sessionStorage|indexedDB/.test(engine) && !/localStorage|sessionStorage|indexedDB/.test(widget));
check('the widget does not load anything at runtime either — the libraries are the page\'s decision',
  !/createElement\('script'\)|\.src\s*=/.test(widget));
check('the analyser reads a bounded slice, so a 1 GB CSV the importer streams does not go into memory whole',
  /IMP_ANALYSER_SLICE = 50 \* 1024 \* 1024/.test(app) && /file\.slice\(0, IMP_ANALYSER_SLICE\)/.test(app));

/* ── 4. The menu ────────────────────────────────────────────────────────── */

const sidebar = read('public', 'cygenix-sidebar.js');
const actions = read('public', 'cygenix-assistant-actions.js');
check('the sidebar no longer has a Data Analyser item',
  !/key:'data-analyser'/.test(sidebar) && !/label:'Data Analyser'/.test(sidebar));
check('and says why, so nobody adds one back',
  /Data import tab/.test(sidebar) && /\/data-analyser still/.test(sidebar));
check('the assistant\'s app map has no such page either',
  !/key: 'data-analyser'/.test(actions));
check('nothing in public/ still links to /data-analyser',
  (() => {
    const hits = [];
    for (const f of fs.readdirSync(P('public'))) {
      if (!/\.(html|js)$/.test(f)) continue;
      const src = read('public', f);
      if (/["'`]\/data-analyser["'`]/.test(src)) hits.push(f);
    }
    return hits.length === 0;
  })());

/* ── 5. Routing: the old address lands on the tab ───────────────────────── */

const redirects = read('public', '_redirects');
check('/data-analyser redirects to the dashboard with the import tab pre-selected',
  /^\/data-analyser\s+\/dashboard\?goto=connections\/import\s+301!/m.test(redirects),
  'run node scripts/build-routes.js');
check('and so does the old .html address, since no page answers it any more',
  /^\/data-analyser\.html\s+\/dashboard\?goto=connections\/import\s+301!/m.test(redirects));
check('nothing serves /data-analyser as a page', !/^\/data-analyser\s+\/data-analyser\.html/m.test(redirects));
check('the dashboard\'s goto reader takes a view AND a tab, from the query string as well as the hash',
  /goto=\(\[\^&\]\+\)\/\.exec\(location\.search/.test(app)
  && /var view = parts\[0\], tab = parts\[1\]/.test(app)
  && /switchConnTab\(tab\)/.test(app));

// The reader, run for real: a fake document with the Connections view and
// its import tab, the address /dashboard?goto=connections/import, and the
// two functions it should call.
{
  const start = app.indexOf('(function(){\n  function target(){');
  const end = app.indexOf('})();', start) + 5;
  const src = app.slice(start, end);
  const calls = [];
  const ids = new Set(['view-connections', 'conn-tab-import']);
  const ctx = {
    location: { hash: '', search: '?goto=connections%2Fimport&x=1', pathname: '/dashboard' },
    sessionStorage: { getItem() { return null; }, removeItem() {} },
    history: { replaceState(_s, _t, url) { calls.push('replace:' + url); } },
    document: {
      title: 'Dashboard', readyState: 'complete',
      getElementById(id) { return ids.has(id) ? {} : null; },
      addEventListener() {},
    },
    showView(v) { calls.push('view:' + v); },
    switchConnTab(t) { calls.push('tab:' + t); },
    decodeURIComponent,
  };
  vm.createContext(ctx);
  let err = '';
  try { vm.runInContext(src, ctx, { filename: 'goto.js' }); } catch (e) { err = e.message; }
  check('the reader extracted cleanly and ran', !err, err);
  check('it opens Connections and then its import tab',
    calls.includes('view:connections') && calls.includes('tab:import')
    && calls.indexOf('view:connections') < calls.indexOf('tab:import'), calls.join(' '));
  check('and takes goto out of the address, leaving the other parameters alone',
    calls.includes('replace:/dashboard?x=1'), calls.join(' '));
}

const docs = read('docs', 'data-analyser.md');
check('the doc records the rename and how to take an upstream update',
  /cygenix-transform\.js/.test(docs) && /CygenixAnalyser/.test(docs) && /upstream/i.test(docs));
check('and says where the analyser lives now',
  /Data import/.test(docs) && /goto=connections\/import/.test(docs));

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
