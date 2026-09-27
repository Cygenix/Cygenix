// tests/menu-index.test.js — menu items and single settings are findable by name.
//
// WHY THIS FILE EXISTS
//
// The masthead search and the Search page searched saved work only, so
// "claude", "api key" or "anthropic" found nothing: the Anthropic key is one
// field on a screen called "General settings" in the account menu, and a
// person who does not already know that has nowhere to start.
// public/cygenix-menu-index.js is the answer — a local index of every menu
// destination (read from the sidebar) and a hand-kept list of settings — and
// this file holds it to what it promises, with no browser:
//
//   1. the ranking: exact > starts-with > word-start > keyword > substring,
//      case and punctuation ignored, every word of a multi-word query needed;
//   2. the real sidebar's navEntries(): every destination once, filtered by
//      the rail's own visibility test — the Audit log is absent for a person
//      the rail hides it from, and its tab-strip twin cannot sneak it back;
//   3. the queries the brief names, against the real sidebar and the real
//      SETTINGS table, return the right thing first;
//   4. every SETTINGS entry points at something that exists: its nav key is
//      one the sidebar resolves, every #id in its anchor is in the markup of
//      THAT view (or built by the module that owns it), its tab exists. A
//      setting whose target has gone fails here rather than producing a
//      result that lands nowhere;
//   5. go() and the landing: the focus key is set before navigating, a
//      Connections field off the dashboard goes straight to its tab, the key
//      is cleared BEFORE the scroll (the one-shot rule), and a target that
//      never appears is given up after 3s rather than left waiting;
//   6. the wiring: every page that loads the sidebar loads the index first,
//      showView looks for a pending field, and the Search page shows the new
//      group and says "Nothing matched" only when every group is empty.
//
// SETTINGS CANDIDATES LEFT OUT, and why (the brief asked for this list):
//   · Profiles, Users & roles, System parameters, Governance, Accessibility,
//     Help guide, Integrations, Diagnostics, Notifications — each is a whole
//     screen that is already a menu item, so it is found as one; a second
//     "Setting" row with the same name would only be a duplicate. Their extra
//     words ("rbac", "smtp", "a11y"…) are in MENU_KEYWORDS instead.
//   · Nothing in the seed list was dropped for a missing target: every field
//     named there exists. Help and Accessibility have no field to land on —
//     one opens a new tab, the other toggles a panel — so they carry no anchor.

'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

const ROOT = path.join(__dirname, '..');
const PUB = path.join(ROOT, 'public');
const read = (f) => fs.readFileSync(path.join(PUB, f), 'utf8');
const MI = require(path.join(PUB, 'cygenix-menu-index.js'));

// The real sidebar, evaluated with no DOM, as tests/sidebar-nav.test.js does.
// `roles` seeds the five-minute RBAC cache the rail reads for the Audit log.
function loadSidebar(roles) {
  const noopEl = () => ({ classList: { add() {}, remove() {}, toggle() {}, contains: () => false },
    addEventListener() {}, setAttribute() {}, querySelector: () => null, querySelectorAll: () => [],
    appendChild() {}, replaceWith() {}, style: {}, dataset: {}, textContent: '' });
  const ss = {};
  if (roles) ss.cygenix_rbac_me = JSON.stringify({ at: Date.now(), me: { roles } });
  const sandbox = {
    window: { addEventListener() {}, location: { pathname: '/x.html', origin: 'https://x' } },
    document: { readyState: 'loading', addEventListener() {}, createElement: noopEl,
      getElementById: () => null, querySelector: () => null, querySelectorAll: () => [],
      head: noopEl(), body: noopEl(), documentElement: noopEl() },
    localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
    sessionStorage: { getItem: (k) => (k in ss ? ss[k] : null), setItem() {}, removeItem() {} },
    console, setTimeout: () => 0, setInterval: () => 0, requestIdleCallback: undefined,
    fetch: undefined,
  };
  sandbox.window.document = sandbox.document;
  sandbox.window.localStorage = sandbox.localStorage;
  sandbox.window.sessionStorage = sandbox.sessionStorage;
  vm.createContext(sandbox);
  vm.runInContext(read('cygenix-sidebar.js'), sandbox);
  return sandbox.window.CygenixSidebar;
}

(async () => {
  console.log('Menu & settings search\n');

  /* ── 1. Ranking ─────────────────────────────────────────────────────────── */
  section('1. The ranking is exact > starts-with > word-start > keyword > substring');
  const E = (label, extra) => Object.assign({ id: label, label, path: '', keywords: [] }, extra || {});
  check('the exact label scores highest', MI.score(E('Theme'), 'theme') === 500);
  check('a label that starts with the query comes next', MI.score(E('Theme colours'), 'theme') === 400);
  check('then a word in the label that starts with it', MI.score(E('Default theme'), 'the') === 300);
  check('then a keyword', MI.score(E('Appearance', { keywords: ['dark mode'] }), 'dark') === 200);
  check('then a plain substring anywhere', MI.score(E('Appearance', { path: 'Settings › General' }), 'eneral') === 100);
  check('no match scores nothing', MI.score(E('Theme'), 'zebra') === 0);
  check('case is ignored', MI.score(E('Anthropic API key'), 'ANTHROPIC') === 400);
  check('&, › and punctuation are ignored', MI.score(E('Users & roles'), 'users roles') === 500
    && MI.score(E('Users & roles'), 'Users&Roles') === 500 && MI.score(E('Auto-snapshot on save'), 'auto snapshot') === 400);
  check('a multi-word query needs every word', MI.score(E('Primary model'), 'primary key') === 0
    && MI.score(E('Primary model'), 'model primary') === 300);
  const order = MI.rank([E('Default theme'), E('Theme'), E('Theme colours'), E('Nothing')], 'theme').map((e) => e.label);
  check('rank sorts by tier', order.join('|') === 'Theme|Theme colours|Default theme', order.join('|'));
  check('and honours a limit', MI.rank([E('a1'), E('a2'), E('a3')], 'a', 2).length === 2);
  check('an empty query finds nothing', MI.rank([E('Theme')], '   ').length === 0);

  /* ── 2. The real sidebar ────────────────────────────────────────────────── */
  section('2. navEntries(): every destination once, filtered as the rail filters');
  const SB = loadSidebar(null);
  const SBa = loadSidebar(['AU']);
  const nav = SB.navEntries();
  const navA = SBa.navEntries();
  const keys = nav.map((n) => n.key);
  check('the sidebar exposes navEntries and findItem', typeof SB.navEntries === 'function' && typeof SB.findItem === 'function');
  check('it returns rail, tab and account-menu destinations',
    ['dashboard', 'connections', 'integrations', 'project-settings', 'user-roles', 'diagnostics'].every((k) => keys.includes(k)), keys.join(','));
  check('each key appears once', new Set(keys).size === keys.length);
  check('every entry is a destination: a view, a page or an action',
    nav.every((n) => !!(n.view || n.href || n.action)));
  check('THE AUDIT LOG IS ABSENT FOR A PERSON THE RAIL HIDES IT FROM', !keys.includes('audit'));
  check('…including through its tab-strip twin, which carries no flag of its own',
    !nav.some((n) => /audit log/i.test(n.label)));
  check('and present for an Auditor', navA.some((n) => n.key === 'audit'));
  const jobs = nav.find((n) => n.key === 'jobs');
  check('a tab repeating a rail key under another label adds it as a keyword',
    jobs && jobs.label === 'Jobs & packages' && jobs.keywords.includes('Jobs'), JSON.stringify(jobs));
  const integ = nav.find((n) => n.key === 'integrations');
  check('a tab carries the rail item it sits under as its parent',
    integ && integ.parentLabel === 'Profiles & integrations' && integ.section === 'Connect', JSON.stringify(integ));
  const ps = nav.find((n) => n.key === 'project-settings');
  check('account-menu settings are filed under Settings', ps && ps.section === 'Settings');
  nav[0].label = 'mutated';
  check('the returned objects are copies; the nav cannot be changed through them', SB.navEntries()[0].label !== 'mutated');

  /* ── 3. The queries the brief names ─────────────────────────────────────── */
  section('3. The queries return the right thing first');
  const all = MI.buildFrom(SB.navEntries());
  const allA = MI.buildFrom(SBa.navEntries());
  const top = (q, set) => (MI.rank(set || all, q, 6)[0] || {}).id;
  check('"claude" → the Anthropic API key', top('claude') === 'set:anthropic-key', top('claude'));
  check('"api key" → the Anthropic API key', top('api key') === 'set:anthropic-key', top('api key'));
  check('"anthropic" → the Anthropic API key', top('anthropic') === 'set:anthropic-key', top('anthropic'));
  check('"sk-ant" → the Anthropic API key', top('sk-ant') === 'set:anthropic-key', top('sk-ant'));
  check('"theme" → Theme', top('theme') === 'set:theme', top('theme'));
  check('"collation" → Collation settings', top('collation') === 'set:collation', top('collation'));
  check('"users" → Users & roles', top('users') === 'menu:user-roles', top('users'));
  check('"object map" → Object mapping', top('object map') === 'menu:object-mapping', top('object map'));
  check('"audit" finds no Audit log for a person without access', !MI.rank(all, 'audit').some((e) => e.id === 'menu:audit'));
  check('"audit" → the Audit log for an Auditor', top('audit', allA) === 'menu:audit', top('audit', allA));
  const claude = MI.rank(all, 'claude')[0];
  check('the result carries its breadcrumb and kind', claude.path === 'Settings › General' && claude.kind === 'setting');
  const menuRow = MI.rank(all, 'object map')[0];
  check('a menu result is labelled with its group', menuRow.kind === 'menu' && menuRow.path === 'Model', menuRow.path);

  /* ── 4. Every setting points at something real ─────────────────────────── */
  section('4. Every SETTINGS entry points at a real screen and a real field');
  const dash = read('dashboard.html');
  const viewSlice = (view) => {
    const i = dash.indexOf('id="view-' + view + '"');
    if (i === -1) return '';
    const next = dash.indexOf('id="view-', i + 10);
    return dash.slice(i, next === -1 ? dash.length : next);
  };
  // Fields built by a module rather than shipped in the markup, and the file
  // that builds each. Checked against that file instead.
  const BUILT = { 'cyg-collation-card': 'cygenix-collation.js' };
  const ids = new Set(), labels = new Set();
  for (const s of MI.SETTINGS) {
    const tag = s.id + ': ';
    check(tag + 'has a label, a breadcrumb and keywords', !!s.label && !!s.path && Array.isArray(s.keywords) && s.keywords.length > 0);
    ids.add(s.id); labels.add(s.label.toLowerCase());
    if (s.billing) {
      check(tag + 'the plan picker it falls back to exists', fs.existsSync(path.join(PUB, s.href.replace(/^\//, '') + '.html')), s.href);
      continue;
    }
    const item = SB.findItem(s.navKey);
    check(tag + 'its nav key "' + s.navKey + '" is one the sidebar knows', !!item);
    if (!item) continue;
    check(tag + 'and is a dashboard view, where the field lives', !!item.view, JSON.stringify(item));
    const slice = viewSlice(item.view);
    check(tag + 'that view is in dashboard.html', !!slice, item.view);
    const anchorIds = String(s.anchor).split(',').map((x) => x.trim()).filter(Boolean);
    check(tag + 'its anchor is a list of #ids', anchorIds.length > 0 && anchorIds.every((a) => /^#[a-z0-9-]+$/i.test(a)), s.anchor);
    for (const a of anchorIds) {
      const id = a.slice(1);
      const built = BUILT[id];
      const found = built
        ? new RegExp('id="' + id + '"').test(read(built)) && (id !== 'cyg-collation-card' || /id="cyg-collation-mount"/.test(slice))
        : new RegExp('id="' + id + '"').test(slice);
      check(tag + a + ' exists ' + (built ? '(built by ' + built + ', mounted in this view)' : 'inside #view-' + item.view), found);
    }
    if (s.tab) check(tag + 'its tab conn-tab-' + s.tab + ' exists', new RegExp('id="conn-tab-' + s.tab + '"').test(slice));
  }
  check('setting ids are unique', ids.size === MI.SETTINGS.length);
  check('setting labels are unique', labels.size === MI.SETTINGS.length);
  const badKw = Object.keys(MI.MENU_KEYWORDS).filter((k) => !SB.findItem(k));
  check('every MENU_KEYWORDS key is a nav key the sidebar knows', badKw.length === 0, badKw.join(','));
  const settingsOnly = MI.buildFrom([]).filter((e) => e.kind === 'setting').map((e) => e.id);
  check('a setting is offered only when its screen is', settingsOnly.join(',') === 'set:subscription', settingsOnly.join(','));

  /* ── 5. go() and landing ───────────────────────────────────────────────── */
  section('5. go() sets the target first; landing clears it before it scrolls');
  function fakeWorld(pathname) {
    const store = {};
    const events = [];
    const timers = [];
    const win = {
      location: { pathname, href: pathname },
      sessionStorage: {
        getItem: (k) => (k in store ? store[k] : null),
        setItem: (k, v) => { store[k] = String(v); events.push('set ' + k); },
        removeItem: (k) => { delete store[k]; events.push('clear ' + k); },
      },
      navigated: [],
      getComputedStyle: () => ({ display: 'block', visibility: 'visible', position: 'static' }),
    };
    win.CygenixSidebar = {
      findItem: SB.findItem,
      navigate: (k) => { win.navigated.push(k); return true; },
    };
    return { win, store, events, timers };
  }
  // A private copy of the module bound to a fake window, so its timers and
  // storage are ours. It boots against a document that is already complete.
  function loadInto(win, doc, timers) {
    const src = read('cygenix-menu-index.js');
    const sandbox = { window: win, module: undefined, Date,
      setTimeout: (fn, ms) => { timers.push({ fn, ms }); return timers.length; } };
    win.document = doc;
    vm.createContext(sandbox);
    vm.runInContext(src, sandbox);
    return win.CygenixMenuIndex;
  }
  const emptyDoc = { readyState: 'complete', addEventListener() {}, querySelector: () => null,
    getElementById: () => null, createElement: () => ({}), head: { appendChild() {} } };

  {
    const w = fakeWorld('/object-mapping');
    const idx = loadInto(w.win, emptyDoc, w.timers);
    idx.go(idx.byId('set:anthropic-key'));
    check('from another page: the field is stashed', w.store.cyg_focus_target === '#settings-api-key', JSON.stringify(w.store));
    check('…and the screen opened through the sidebar, as the menu would', w.win.navigated.join() === 'project-settings');
    check('…and NO wait starts on the page being left, where it could time out and clear the key',
      w.timers.length === 0, w.timers.length + ' timers');
  }
  {
    const w = fakeWorld('/sql-editor');
    const idx = loadInto(w.win, emptyDoc, w.timers);
    idx.go(idx.byId('set:collation'));
    check('a Connections field off the dashboard goes straight to its tab',
      w.win.location.href === '/dashboard#goto=' + encodeURIComponent('connections/databases'), w.win.location.href);
    check('…through the same cyg_goto key the dashboard reads', w.store.cyg_goto === 'connections/databases');
  }
  {
    // Landing: a visible field. Record the order of clear vs scroll.
    const order = [];
    const el = {
      isConnected: true, offsetParent: {}, disabled: false,
      getBoundingClientRect: () => ({ width: 200, height: 30 }),
      matches: () => true,
      scrollIntoView: () => order.push('scroll'),
      focus: () => order.push('focus'),
      classList: { add: (c) => order.push('add ' + c), remove: (c) => order.push('remove ' + c) },
    };
    const doc = Object.assign({}, emptyDoc, { readyState: 'complete', querySelector: (s) => (s === '#settings-api-key' ? el : null) });
    const w = fakeWorld('/dashboard');
    w.store.cyg_focus_target = '#settings-api-key';
    w.win.sessionStorage.removeItem = (k) => { delete w.store[k]; order.push('clear ' + k); };
    loadInto(w.win, doc, w.timers);
    check('the page-load consumer does NOT land inside the call that asked — a caller may re-render right after',
      !order.includes('scroll') && w.timers.length === 1 && w.timers[0].ms === 0, order.join(' > ') + ' / ' + JSON.stringify(w.timers.map((t) => t.ms)));
    w.timers.shift().fn();
    check('it lands on the next tick', order.includes('scroll'), order.join(' > '));
    check('THE KEY IS CLEARED BEFORE THE SCROLL — the one-shot rule',
      order.indexOf('clear cyg_focus_target') !== -1 && order.indexOf('clear cyg_focus_target') < order.indexOf('scroll'), order.join(' > '));
    check('then it is focused and flashed', order.indexOf('focus') > order.indexOf('scroll') && order.includes('add cyg-search-flash'));
    check('the flash comes off after 1.5s, on a timer that only removes a class',
      w.timers.some((t) => t.ms === 1500));
    const flashTimer = w.timers.find((t) => t.ms === 1500);
    const before = order.length;
    flashTimer.fn();
    check('…and that timer touches nothing else', order.slice(before).join() === 'remove cyg-search-flash', order.slice(before).join());
  }
  {
    // A target that never appears: polls every 100ms, gives up after 3s.
    let now = 1000;
    const RealDate = Date;
    const w = fakeWorld('/dashboard');
    w.store.cyg_focus_target = '#nowhere';
    const src = read('cygenix-menu-index.js');
    const sandbox = { window: w.win, module: undefined,
      Date: { now: () => now },
      setTimeout: (fn, ms) => { w.timers.push({ fn, ms }); return w.timers.length; } };
    w.win.document = emptyDoc;
    vm.createContext(sandbox);
    vm.runInContext(src, sandbox);
    check('a missing target is first looked for on the next tick', w.timers.length === 1 && w.timers[0].ms === 0);
    w.timers.shift().fn();
    check('then polled, not busy-waited: every 100ms', w.timers.length === 1 && w.timers[0].ms === 100);
    let ticks = 0;
    while (w.timers.length && ticks < 100) { const t = w.timers.shift(); now += t.ms; t.fn(); ticks++; }
    check('and given up after 3s', ticks >= 29 && ticks <= 31, ticks + ' ticks');
    check('with the key cleared, so it does not wait on the next page', !('cyg_focus_target' in w.store));
    void RealDate;
  }

  /* ── 6. Wiring ─────────────────────────────────────────────────────────── */
  section('6. Loading and wiring');
  const pages = fs.readdirSync(PUB).filter((f) => f.endsWith('.html') && /<script src="\/cygenix-sidebar\.js/.test(read(f)));
  const badOrder = pages.filter((f) => {
    const h = read(f);
    const i = h.search(/<script src="\/cygenix-menu-index\.js\?v=[a-f0-9]{10}" defer><\/script>/);
    // The TAG, not the first mention: several pages name the sidebar's path
    // in a comment above their mount point.
    const j = h.search(/<script src="\/cygenix-sidebar\.js/);
    return i === -1 || j === -1 || i > j;
  });
  check('every page that loads the sidebar loads the index before it, deferred and stamped (' + pages.length + ' pages)',
    pages.length >= 28 && badOrder.length === 0, badOrder.join(', '));
  const idxSrc = read('cygenix-menu-index.js');
  check('the index makes no network request', !/\bfetch\s*\(|XMLHttpRequest|sendBeacon/.test(idxSrc));
  const sb = read('cygenix-sidebar.js');
  check('the masthead field is a combobox over a listbox with options',
    /setAttribute\('role', 'combobox'\)/.test(sb) && /setAttribute\('role', 'listbox'\)/.test(sb) && /role="option"/.test(sb));
  check('…which reports the highlighted row through aria-activedescendant', /aria-activedescendant/.test(sb));
  check('…debounced at 120ms, at most six rows', /SEARCH_DEBOUNCE_MS = 120/.test(sb) && /SEARCH_MAX = 6/.test(sb));
  check('…whose last row keeps the old behaviour: search saved work', /Search saved work for/.test(sb));
  check('…and which is simply absent if the index did not load', /const idx = index\(\);\s*\n\s*if \(!q \|\| !idx/.test(sb));
  check('the list sits above the rail and the hairline', /\.cx-mh-results\{position:fixed;z-index:1000/.test(sb));
  const app = read('dashboard-app.js');
  const showView = app.slice(app.indexOf('function showView(v)'), app.indexOf('function selectTarget('));
  check('showView looks for a pending field once the view is showing', /CygenixMenuIndex\.consumeFocusTarget\(\)/.test(showView));
  const run = app.slice(app.indexOf('function runGlobalSearch()'), app.indexOf('function _renderMenuSearchGroup('));
  check('the Search page asks the index for up to ten', /CygenixMenuIndex\.search\(qRaw, 10\)/.test(run));
  check('"Nothing matched" only when every group is empty', /if \(!rows\.length && !menuHits\.length\) \{[\s\S]{0,900}Nothing matched/.test(run));
  check('the Menu & settings group renders above saved work', /res\.innerHTML = menuHtml \+/.test(run));
  check('the empty state says menu items and settings are searched', /search menu items and settings/.test(app));
  check('the scope list has "Menu & settings only"', /<option value="menu">Menu &amp; settings only<\/option>/.test(dash));

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
