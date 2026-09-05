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
// the page makes to the user — "nothing leaves this page" — against the code
// rather than against the copy.
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

console.log('Data Analyser — a new menu item, under a name that cannot collide\n');

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

/* ── 2. The page ────────────────────────────────────────────────────────── */

const page = read('public', 'data-analyser.html');
check('the page exists and is titled for the menu item',
  /<title>Cygenix – Data Analyser<\/title>/.test(page));
check('it mounts the sidebar with its own key active',
  /<div id="cyg-sidebar-mount" data-active="data-analyser"><\/div>/.test(page));
check('auth-gate is the first script, as on every console page',
  /<script src="\/auth-gate\.js[^"]*"><\/script>/.test(page)
  && page.indexOf('/auth-gate.js') < page.indexOf('/cygenix-auth-token.js'));
check('the engine loads before the widget, which requires it',
  page.indexOf('/cygenix-analyser.js') < page.indexOf('/cygenix-analyser-widget.js'));
check('and the widget is mounted into the page\'s host element',
  /CygenixAnalyserWidget\.mount\('#analyser'/.test(page) && /<div id="analyser"><\/div>/.test(page));
check('the widget follows the console theme rather than fixing its own',
  /theme: consoleTheme\(\)/.test(page) && /attributeFilter: \['data-theme'\]/.test(page));
check('the console\'s accent and type are imposed by id, since the widget injects later',
  /#analyser\.cygx\{[^}]*--accent:#4A5BD6/.test(page) && /#analyser\.cygx\{[^}]*font-family:var\(--serif\)/.test(page));
check('it registers with the assistant like every other screen',
  /CygenixAssistant\.registerPage\('data-analyser'\)/.test(page));
check('the page loads the sync banner alongside the sync module, as the banner test requires',
  /cygenix-cosmos-sync\.js/.test(page) && /cygenix-sync-banner\.js/.test(page));
check('the optional libraries are pinned, from the one CDN the console already trusts',
  /cdnjs\.cloudflare\.com\/ajax\/libs\/xlsx\/0\.18\.5\//.test(page)
  && /cdnjs\.cloudflare\.com\/ajax\/libs\/js-yaml\/4\.1\.0\//.test(page)
  && !/unpkg|jsdelivr/.test(page));

/* ── 3. The promise the page makes ─────────────────────────────────────── */
//
// "Nothing leaves this page." That is the reason a prospect can try it with
// real data, and it is only true if the code has no way to send anything.

check('the page says nothing leaves it', /Nothing leaves this page/.test(page));
const NET = /\bfetch\s*\(|XMLHttpRequest|navigator\.sendBeacon|new WebSocket|EventSource\(/;
check('the engine has no network path at all', !NET.test(engine));
check('neither has the widget', !NET.test(widget));
check('and neither writes browser storage, so the storage inventory is untouched',
  !/localStorage|sessionStorage|indexedDB/.test(engine) && !/localStorage|sessionStorage|indexedDB/.test(widget));
check('the widget does not load anything at runtime either — the libraries are the page\'s decision',
  !/createElement\('script'\)|\.src\s*=/.test(widget));

/* ── 4. The menu ────────────────────────────────────────────────────────── */

const sidebar = read('public', 'cygenix-sidebar.js');
check('the sidebar has a Data Analyser item',
  /key:'data-analyser', label:'Data Analyser', href:'\/data-analyser'/.test(sidebar));
check('in the Connect section, after Profiles',
  (() => {
    const connect = sidebar.slice(sidebar.indexOf("section: 'Connect'"), sidebar.indexOf("section: 'Map & Build'"));
    return connect.indexOf("key:'profiles'") < connect.indexOf("key:'data-analyser'")
      && connect.indexOf("key:'data-analyser'") < connect.indexOf("key:'integrations'");
  })());
check('and the placement is explained',
  /a step before Connections/.test(sidebar));

/* ── 5. Routing and hygiene ─────────────────────────────────────────────── */

const redirects = read('public', '_redirects');
check('/data-analyser is routed to the page',
  /^\/data-analyser\s+\/data-analyser\.html\s+200/m.test(redirects),
  'run node scripts/build-routes.js');
check('and the .html address redirects to the clean one',
  /^\/data-analyser\.html\s+\/data-analyser\s+301/m.test(redirects));

const docs = read('docs', 'data-analyser.md');
check('the doc records the rename and how to take an upstream update',
  /cygenix-transform\.js/.test(docs) && /CygenixAnalyser/.test(docs) && /upstream/i.test(docs));

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
