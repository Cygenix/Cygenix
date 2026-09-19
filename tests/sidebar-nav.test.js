// Structural tests for the console navigation — the five-group rail, the
// masthead, the tab strips and the aliases (design review, Sep-2026).
//
// Loads the real cygenix-sidebar.js under a minimal DOM stub and asserts the
// nav still covers every destination the pages reference. The failure mode of
// a nav restructure is a page whose data-active key no longer resolves, or a
// destination that silently fell out of the product. The redesign moved
// twenty-odd destinations out of the rail and into tab strips and the account
// menu; every one of them must still be reachable and must still light the
// right rail item when its page mounts.
const fs = require('fs');
const vm = require('vm');

const noopEl = () => ({ classList:{add(){},remove(){},toggle(){},contains:()=>false},
  addEventListener(){}, setAttribute(){}, querySelector:()=>null, querySelectorAll:()=>[],
  appendChild(){}, replaceWith(){}, style:{}, dataset:{}, textContent:'' });
const sandbox = {
  window: { addEventListener(){}, location:{ pathname:'/x.html', origin:'https://x' } },
  document: {
    readyState: 'loading', addEventListener(){}, createElement: noopEl,
    getElementById: () => null, querySelector: () => null, querySelectorAll: () => [],
    head: noopEl(), body: noopEl(), documentElement: noopEl(),
  },
  localStorage: { getItem: () => null, setItem(){}, removeItem(){} },
  sessionStorage: { getItem: () => null, setItem(){}, removeItem(){} },
  console, setTimeout: () => 0, setInterval: () => 0, requestIdleCallback: undefined,
};
sandbox.window.document = sandbox.document;
sandbox.window.localStorage = sandbox.localStorage;
vm.createContext(sandbox);
const src = fs.readFileSync(__dirname + '/../public/cygenix-sidebar.js', 'utf8');
vm.runInContext(src, sandbox);

const SB = sandbox.window.CygenixSidebar;
let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Sidebar navigation — structure\n');
check('sidebar module loads and exports the nav tree, the tabs and the aliases',
  !!(SB && SB.__nav && SB.__accountNav && SB.__tabs && SB.__aliases && SB.railKeyFor));

const NAV = SB.__nav, ACCT = SB.__accountNav, TABS = SB.__tabs, ALIASES = SB.__aliases;
const railKeys = [];
for (const sec of NAV) for (const it of sec.items) railKeys.push(it.key);

/* ── 1. Nothing fell out ──────────────────────────────────────────────────
   Every key that existed before the restructure must still resolve — pages
   mount with these in data-active, dashboard code targets them, the tour
   navigates to them. 'supported', 'project-plan', 'insights',
   'project-summary-document' and 'coworker' are deliberately absent, as
   before: removed on request, or replaced. */
const LEGACY_KEYS = ['dashboard','search','project-settings','connections','performance',
  'system-parameters','privacy-security','integrations','object-mapping',
  'sql-editor','agentive-migration','data-quality','data-cleansing',
  'validation','jobs','project-builder','server-migration','inventory','task-agent',
  'report-builder','reports','audit','diagnostics','help','accessibility',
  'profiles','schema-explorer','analytics','conversion-templates','data-enrichment',
  'data-stream','data-stream-store','data-stream-events','data-stream-monitor',
  'effort-estimator','project-plan-grid','user-roles','notifications','assurance'];
const missing = LEGACY_KEYS.filter(k => !SB.__findItem(k));
check('every pre-redesign key still resolves (' + LEGACY_KEYS.length + ')', missing.length === 0,
  'missing: ' + missing.join(', '));

/* ── 2. Six groups, in the order the work happens ────────────────────────── */
// Plan comes first: the Configurator decides what is in scope and sizes it,
// and the Project plan is built from that. The handoff filed both as tabs
// under Reports, and nobody found them there.
const sections = NAV.map(s => s.section).filter(Boolean);
check('the rail is six groups — Plan, Connect, Model, Run, Quality, Govern — after Home',
  JSON.stringify(sections) === JSON.stringify(['Plan','Connect','Model','Run','Quality','Govern'])
  && NAV[0].section === null && NAV[0].items.length === 1 && NAV[0].items[0].key === 'dashboard',
  'got: ' + sections.join(' → '));
check('and it is fifteen destinations, not thirty',
  railKeys.length === 15, railKeys.length + ': ' + railKeys.join(','));
check('the Configurator and the Project plan are the Plan group, directly below Home',
  NAV[1].section === 'Plan' && NAV[1].items.map(i => i.key).join(',') === 'effort-estimator,project-plan-grid'
  && NAV[1].items[0].label === 'Configurator' && NAV[1].items[0].href === '/configurator'
  && NAV[1].items[1].label === 'Project plan' && NAV[1].items[1].href === '/project-plan');

/* ── 3. No expanders, no decoration ───────────────────────────────────────
   Finding 04: the per-item colour was decoration using the status palette.
   There is no `color` on any item any more, and no `children` either —
   every former child is a tab inside its destination screen. */
const decorated = [];
for (const sec of NAV) for (const it of sec.items) { if ('color' in it) decorated.push(it.key); if (it.children) decorated.push(it.key + ' (children)'); }
check('no rail item carries a colour or a fold-out', decorated.length === 0, decorated.join(', '));
check('the source has no per-item colour left at all', !/color:\s*'var\(--/.test(src.slice(0, src.indexOf('function svg('))));

/* ── 4. Every leaf has exactly one destination ───────────────────────────── */
const badLeaves = [];
const leafCheck = (it, where) => {
  const n = ['href','view','action'].filter(k => it[k]).length;
  if (n !== 1) badLeaves.push(where + ':' + it.key + ' (' + n + ' destinations)');
};
for (const sec of NAV) for (const it of sec.items) leafCheck(it, 'rail');
for (const k in TABS) TABS[k].forEach(t => leafCheck(t, 'tab'));
ACCT.forEach(it => leafCheck(it, 'account'));
check('every rail item, tab and account item has exactly one destination', badLeaves.length === 0, badLeaves.join(', '));

/* ── 5. Tabs belong to rail items, and lead the strip with their owner ───── */
const badTabs = [];
for (const k in TABS) {
  if (!railKeys.includes(k)) badTabs.push(k + ' is not on the rail');
  if (!TABS[k].length || TABS[k][0].key !== k) badTabs.push(k + ' does not lead its own strip');
  const keys = TABS[k].map(t => t.key);
  keys.forEach((t, i) => { if (keys.indexOf(t) !== i) badTabs.push(k + ' repeats ' + t); });
}
check('every tab strip hangs off a rail item and starts with it', badTabs.length === 0, badTabs.join('; '));

// A key may be a rail item AND the first tab of its own strip; it may not
// appear anywhere else twice.
const seen = new Map();
const dupes = [];
railKeys.forEach(k => seen.set(k, 'rail'));
for (const k in TABS) TABS[k].forEach(t => {
  if (t.key === k) return;
  if (seen.has(t.key)) dupes.push(t.key + ' (' + seen.get(t.key) + ' + tab of ' + k + ')');
  seen.set(t.key, 'tab of ' + k);
});
ACCT.forEach(it => { if (seen.has(it.key)) dupes.push(it.key + ' (account + ' + seen.get(it.key) + ')'); seen.set(it.key, 'account'); });
check('no key lives in two places', dupes.length === 0, dupes.join(', '));

/* ── 6. Where everything went, per the handoff table ─────────────────────── */
const under = (rail, key) => (TABS[rail] || []).some(t => t.key === key);
check('Profiles & integrations: profiles + integrations', under('profiles', 'integrations'));
check('Object mapping: conversion templates and AI assist as tabs', under('object-mapping', 'conversion-templates') && under('object-mapping', 'agentive-migration'));
check('Jobs & packages: packages, server migration, analytics', under('jobs', 'project-builder') && under('jobs', 'server-migration') && under('jobs', 'analytics'));
check('Data stream: store, change events, monitor', under('data-stream', 'data-stream-store') && under('data-stream', 'data-stream-events') && under('data-stream', 'data-stream-monitor'));
check('Assurance: quality review and validation', under('assurance', 'data-quality') && under('assurance', 'validation'));
check('Cleansing & enrichment: enrichment', under('data-cleansing', 'data-enrichment'));
check('Reports: report builder, conversion report, artifacts — the plan is not a report',
  ['reports','inventory'].every(k => under('report-builder', k))
  && !under('report-builder', 'effort-estimator') && !under('report-builder', 'project-plan-grid'));
check('Audit log: performance and diagnostics', under('audit', 'performance') && under('audit', 'diagnostics'));
check('Settings and governance live in the account menu',
  ['project-settings','notifications','system-parameters','user-roles','privacy-security'].every(k => ACCT.some(i => i.key === k)));
check('Help and Accessibility are still in the account menu, and accessibility keeps its a11y-trigger class',
  ACCT.some(i => i.key === 'help') && (ACCT.find(i => i.key === 'accessibility') || {}).navClass === 'a11y-trigger');
check('Search is the masthead field, not a rail item — but the key still resolves',
  !railKeys.includes('search') && SB.__findItem('search') && SB.__findItem('search').view === 'search'
  && /id="cx-mh-search"/.test(src) && /cyg_search_q/.test(src));
check('AI assist keeps its feature flag on the way into the tab strip',
  (TABS['object-mapping'].find(t => t.key === 'agentive-migration') || {}).requiresAiEnabled === true);
check('the Audit log keeps its role gate', (SB.__findItem('audit') || {}).requiresAuditRead === true);

/* ── 7. Every former key lights the right rail item ─────────────────────── */
const expectRail = {
  'validation':'assurance', 'data-quality':'assurance', 'data-enrichment':'data-cleansing',
  'conversion-templates':'object-mapping', 'agentive-migration':'object-mapping',
  'project-builder':'jobs', 'server-migration':'jobs', 'analytics':'jobs',
  'data-stream-store':'data-stream', 'data-stream-events':'data-stream', 'data-stream-monitor':'data-stream',
  'integrations':'profiles', 'reports':'report-builder', 'inventory':'report-builder',
  'effort-estimator':'effort-estimator', 'project-plan-grid':'project-plan-grid',
  'performance':'audit', 'diagnostics':'audit',
  'search':'dashboard', 'project-summary-document':'dashboard', 'insights':'schema-explorer', 'data-analyser':'connections',
  'dashboard':'dashboard', 'jobs':'jobs', 'assurance':'assurance',
};
const wrong = Object.keys(expectRail).filter(k => SB.railKeyFor(k) !== expectRail[k]).map(k => k + '→' + SB.railKeyFor(k));
check('railKeyFor maps every moved key to its owner (' + Object.keys(expectRail).length + ')', wrong.length === 0, wrong.join(', '));
check('an account-menu key lights nothing rather than something wrong',
  SB.railKeyFor('project-settings') === '' && SB.railKeyFor('user-roles') === '' && SB.railKeyFor('nonsense') === '');
check('every alias points at a real rail item',
  Object.values(ALIASES).every(v => railKeys.includes(v)), JSON.stringify(ALIASES));
check('tabsFor returns the strip for a tab key and for its owner alike',
  (SB.tabsFor('validation') || []).map(t => t.key).join(',') === 'assurance,data-quality,validation'
  && (SB.tabsFor('assurance') || []).length === 3 && SB.tabsFor('sql-editor') === null);

/* ── 8. The masthead ──────────────────────────────────────────────────────── */
check('the masthead is built by the sidebar module: logo, wordmark, project switcher, search, Files, region, avatar',
  /function buildMasthead\(\)/.test(src) && /class="cx-logo"/.test(src) && /class="cx-wordmark"/.test(src)
  && /id="cyg-proj-btn"/.test(src) && /id="cx-mh-search"/.test(src) && /id="cyg-drive-btn"/.test(src)
  && /id="cx-mh-region"/.test(src) && /id="cyg-user-chip"/.test(src));
check('the logo keeps its own indigo gradient — it is the brand mark and does not take the theme',
  /#6d5df2/.test(src) && /#4a7cf3/.test(src) && /rx="7"/.test(src));
check('the project switcher is wired on the masthead, not the rail',
  /wireProjectSwitcher\(masthead\)/.test(src) && !/buildProjectSwitcher\(\)/.test(src));
check('the account menu opens from the avatar, below it',
  /wireUserChip\(masthead\)/.test(src) && /menu\.style\.top\s*=\s*\(r\.bottom \+ 6\)/.test(src));
check('the rail is 216 wide under a 60px masthead',
  /WIDTH_OPEN\s*=\s*216/.test(src) && /MASTHEAD_H\s*=\s*60/.test(src)
  && /top:calc\(var\(--cyg-hairline-h,0px\) \+ \$\{MASTHEAD_H\}px\)/.test(src));
check('the open rail is text; icons are for the collapsed rail only',
  /\.cyg-nav-icon\{[^}]*display:none\}/.test(src) && /\.cyg-sidebar\.collapsed \.cyg-nav-icon\{display:block\}/.test(src));
check('the active item is an accent-100 ground with a 2px accent bar, in ink — no white-on-dark rail',
  /\.cyg-nav-item\.active\{[^}]*var\(--color-accent-100/.test(src)
  && /border-left-color:var\(--color-accent/.test(src) && !/--cyg-ink:#14161f/.test(src));

/* ── 9. The tab strip ─────────────────────────────────────────────────────── */
check('the strip renders into #cyg-subnav-mount and nowhere else',
  /getElementById\('cyg-subnav-mount'\)/.test(src) && /class="cx-subnav"/.test(src)
  && !/insertBefore\(strip|prepend\(strip/.test(src));
check('setActive re-renders it, so a dashboard view switch moves the current tab',
  /function updateActive\(key\)\{[\s\S]*?renderSubnav\(key\)/.test(src));
check('a tab click goes through handleClick — the one place that knows views from pages',
  /a\.addEventListener\('click'[\s\S]{0,200}handleClick\(item\)/.test(src));

/* ── 10. Favourites still ride along ─────────────────────────────────────── */
check('the favourites block is appended and exports its surface',
  typeof SB.getPins === 'function' && typeof SB.togglePin === 'function'
  && typeof SB.refreshPins === 'function');
check('an empty store means no pins, never a throw', Array.isArray(SB.getPins()) && SB.getPins().length === 0);
check('pins store per user, capped at eight, with the P shortcut and drag reorder',
  /cygenix_sidebar_pinned_v1/.test(src) && /MAX_PINS\s*=\s*8/.test(src)
  && /cyg-fav-dragging/.test(src) && /e\.key !== 'p' && e\.key !== 'P'/.test(src));
check('pinned rows are clones that delegate to the real row — no duplicated nav logic',
  /data-favkey/.test(src) && /target\.click\(\)/.test(src));

/* ── 11. Every page that loads the rail must have somewhere to mount it ──── */
{
  const path2 = require('path');
  const dir = path2.join(__dirname, '..', 'public');
  const pages = fs.readdirSync(dir).filter(f => f.endsWith('.html'));
  const missing = pages.filter(f => {
    const html = fs.readFileSync(path2.join(dir, f), 'utf8');
    return /cygenix-sidebar\.js/.test(html) && !/id="cyg-sidebar-mount"/.test(html);
  });
  check('every page loading cygenix-sidebar.js declares a mount point',
    missing.length === 0, missing.join(', '));

  const projects = fs.readFileSync(path2.join(dir, 'projects.html'), 'utf8');
  check('the Projects page mounts the rail and leaves room for it',
    /id="cyg-sidebar-mount"/.test(projects) && /padding-left:216px/.test(projects)
    && /body\.cyg-collapsed\{padding-left:54px\}/.test(projects)
    && !/<div class="sidebar" id="sidebar">/.test(projects));

  check('a missing mount self-heals instead of dropping the navigation',
    /mounting at the top of <body>/.test(src) && /cyg-sidebar-autopad/.test(src));
}

/* ── 12. The nav has to know what page it is on ─────────────────────────── */
{
  const path3 = require('path');
  const dir3 = path3.join(__dirname, '..', 'public');
  const app = fs.readFileSync(path3.join(dir3, 'dashboard-app.js'), 'utf8');

  check('the nav decides what page it is on by normalising, not by matching .html',
    /location\.pathname\.replace\(\/\\\.html\$\/, ''\)/.test(src)
    && /onDashboard = here === '\/dashboard'/.test(src)
    && !/\/\\\/dashboard\\\.html\?\$\|/.test(src),
    'the stale /dashboard.html regex is back');
  check('and normalises it the same way auth-gate.js does, so the two agree',
    /\.replace\(\/\\\.html\$\/, ''\)\.replace\(\/\\\/\+\$\/, ''\) \|\| '\/'/.test(src)
    && /\.replace\(\/\\\.html\$\/, ''\)\.replace\(\/\\\/\+\$\/, ''\) \|\| '\/'/
        .test(fs.readFileSync(path3.join(dir3, 'auth-gate.js'), 'utf8')));
  check('the fallback forces a real load rather than a silent hash change',
    /window\.location\.reload\(\)/.test(src) && /differs only in the hash/.test(src));
  check('the dashboard reads a #goto= deep link', /goto=\(\[\^&\]\+\)/.test(app));
  check('and the cyg_goto key as the other transport', /getItem\('cyg_goto'\)/.test(app));
  check('it validates the view before switching to it', /getElementById\('view-' \+ view\)/.test(app));
  check('it clears both, so a refresh does not re-fire the deep link',
    /removeItem\('cyg_goto'\)/.test(app) && /history\.replaceState/.test(app));
  check('it only rewrites the address when goto= is in it, leaving #assistant alone',
    /var inHash = \/\(\?:\^\|\[#&\]\)goto=\/\.test\(location\.hash \|\| ''\)/.test(app)
    && /if \(!inHash && !inSearch\) return;/.test(app));
  check('the masthead search hands its query to the Search view, which consumes it',
    /sessionStorage\.getItem\('cyg_search_q'\)/.test(app) && /removeItem\('cyg_search_q'\)/.test(app));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
