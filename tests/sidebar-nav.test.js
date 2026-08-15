// Structural tests for the redesigned sidebar navigation.
// Loads the real cygenix-sidebar.js under a minimal DOM stub and asserts the
// NAV tree still covers every destination the pages reference — the failure
// mode of a nav restructure is a page whose data-active key no longer exists,
// or a destination that silently fell out of the rail.
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
vm.runInContext(fs.readFileSync(__dirname + '/../public/cygenix-sidebar.js', 'utf8'), sandbox);

const SB = sandbox.window.CygenixSidebar;
let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Sidebar navigation — structure\n');
check('sidebar module loads and exports the nav tree', !!(SB && SB.__nav && SB.__accountNav));

const NAV = SB.__nav, ACCT = SB.__accountNav;

// 1. Every key that existed before the restructure must still resolve —
//    pages mount with these in data-active, and dashboard code targets them.
//    'supported' is deliberately absent: the Supported Formats menu option
//    was removed on request.
const LEGACY_KEYS = ['dashboard','search','project-settings','connections','performance',
  'system-parameters','privacy-security','integrations','project-plan','object-mapping',
  'sql-editor','agentive-migration','coworker','data-quality','insights','data-cleansing',
  'validation','jobs','project-builder','server-migration','inventory','task-agent',
  'report-builder','reports','project-summary-document','audit','diagnostics',
  'help','accessibility'];
const missing = LEGACY_KEYS.filter(k => !SB.__findItem(k));
check('every pre-redesign key still resolves (' + LEGACY_KEYS.length + ')', missing.length === 0,
  'missing: ' + missing.join(', '));

// 2. No duplicate keys anywhere in the tree.
const allKeys = [];
for (const sec of NAV) for (const it of sec.items) {
  allKeys.push(it.key);
  if (it.children) for (const c of it.children) allKeys.push(c.key);
}
ACCT.forEach(it => allKeys.push(it.key));
const dupes = allKeys.filter((k, i) => allKeys.indexOf(k) !== i);
check('no duplicate keys', dupes.length === 0, 'dupes: ' + dupes.join(', '));

// 3. Parents are pure expanders: children but no destination of their own.
const badParents = [];
for (const sec of NAV) for (const it of sec.items) {
  if (it.children && (it.href || it.view || it.action)) badParents.push(it.key);
  if (it.children && it.children.length === 0) badParents.push(it.key + ' (empty)');
}
check('group parents are pure expanders with children', badParents.length === 0,
  badParents.join(', '));

// 4. Every leaf has exactly one destination (href, view, or action).
const badLeaves = [];
const leafCheck = (it) => {
  const n = ['href','view','action'].filter(k => it[k]).length;
  if (n !== 1) badLeaves.push(it.key + ' (' + n + ' destinations)');
};
for (const sec of NAV) for (const it of sec.items) {
  if (it.children) it.children.forEach(leafCheck); else leafCheck(it);
}
ACCT.forEach(leafCheck);
check('every leaf has exactly one destination', badLeaves.length === 0, badLeaves.join(', '));

// 5. Lifecycle order per the review: Connect → Map & Build → Run → Validate
//    → Report & Govern.
const sections = NAV.map(s => s.section).filter(Boolean);
check('sections follow the lifecycle order',
  JSON.stringify(sections) === JSON.stringify(['Connect','Map & Build','Run','Validate','Report & Govern']),
  'got: ' + sections.join(' → '));

// 6. The renames from the review landed, and the old branded labels are gone.
const labels = [];
for (const sec of NAV) for (const it of sec.items) {
  labels.push(it.label);
  if (it.children) it.children.forEach(c => labels.push(c.label));
}
check('"AI Assist" replaces "Agentive Migration"',
  labels.includes('AI Assist') && !labels.includes('Agentive Migration'));
check('cookie preferences moved out of the rail',
  !labels.includes('Cookie preferences') && !ACCT.some(i => i.key === 'cookie-prefs'));

// 6b. Supported Formats removed from the menu entirely (requested).
check('Supported Formats is gone from nav and account menu',
  !SB.__findItem('supported') && !labels.includes('Supported Formats'));

// 6c. Help + Accessibility live in the account menu, not the rail.
const acctKeys = ACCT.map(i => i.key);
check('Help and Accessibility are in the account menu',
  acctKeys.includes('help') && acctKeys.includes('accessibility'));
check('Help and Accessibility are NOT in the workflow rail',
  !allNavKeys().includes('help') && !allNavKeys().includes('accessibility'));
// The accessibility panel's outside-click handler skips `.a11y-trigger`;
// losing that class in the move would make the panel close as it opens.
check('accessibility item keeps its a11y-trigger class',
  (ACCT.find(i => i.key === 'accessibility') || {}).navClass === 'a11y-trigger');

function allNavKeys(){
  const out = [];
  for (const sec of NAV) for (const it of sec.items){
    out.push(it.key); (it.children || []).forEach(c => out.push(c.key));
  }
  return out;
}

// 7. Jobs promoted: the Run section must come before Validate and contain jobs.
const runIdx = NAV.findIndex(s => s.section === 'Run');
const runKeys = [];
NAV[runIdx].items.forEach(it => { runKeys.push(it.key); (it.children||[]).forEach(c => runKeys.push(c.key)); });
check('Run group holds jobs + execute + planner + schedules',
  ['jobs','project-builder','project-plan','task-agent'].every(k => runKeys.includes(k)));

// 8. Notifications sits under Settings and points at the dashboard view that
//    controls which run events fire.
const notif = SB.__findItem('notifications');
check('Notifications exists in the nav', !!notif);
check('Notifications opens a dashboard view, not a separate page',
  notif && notif.view === 'notifications' && !notif.href);
check('Notifications is labelled plainly', notif && notif.label === 'Notifications');
{
  const settings = SB.__findItem('settings-group');
  const kids = (settings && settings.children || []).map(c => c.key);
  check('Notifications is a child of Settings', kids.includes('notifications'), kids.join(','));
  check('the existing Settings children are still there',
    kids.includes('project-settings') && kids.includes('system-parameters'), kids.join(','));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
