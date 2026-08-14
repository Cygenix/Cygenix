// Tests the Object Mapping "Recent maps" grid.
// Extracts the real functions out of public/object_mapping.html and runs them
// against stubbed DOM/storage, so the logic under test is the shipped code —
// in particular the counts shown on each card, which must never overstate how
// much of a map is actually mapped.
const fs = require('fs');
const vm = require('vm');

const html = fs.readFileSync(__dirname + '/../public/object_mapping.html', 'utf8');
const start = html.indexOf('// ── Recent maps grid ─');
const end   = html.indexOf('function loadMapById(jobId){');
if (start < 0 || end < 0 || end < start) {
  console.log('FAIL: could not locate the recent-maps block in object_mapping.html');
  process.exit(1);
}
const code = html.slice(start, end);

// ── stubs ────────────────────────────────────────────────────────────────
let JOBS = [];
const els = {};
const mkEl = (id) => (els[id] = els[id] || {
  id, value: '', textContent: '', innerHTML: '', style: {},
  focus(){}, scrollIntoView(){},
});
const store = {};
const sandbox = {
  console,
  localStorage: {
    getItem: (k) => (k in store ? store[k] : null),
    setItem: (k, v) => { store[k] = String(v); },
  },
  $: mkEl,
  esc: (s) => String(s == null ? '' : s).replace(/[&<>"']/g,
        c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),
  _getAllSavedJobs: () => JOBS,
  editJobId: null,
  Date, JSON, Math, Number, String, Array, Set, isNaN, parseInt,
};
vm.createContext(sandbox);
vm.runInContext(code, sandbox);

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};
const gridHtml = () => mkEl('rm-grid').innerHTML;
const iso = (minsAgo) => new Date(Date.now() - minsAgo * 60000).toISOString();

console.log('Object Mapping — recent maps grid\n');

// 1. No saved maps → original guidance text, no grid.
JOBS = [];
sandbox.renderRecentMaps('');
check('with no saved maps, falls back to the guidance text',
  mkEl('recent-maps-wrap').style.display === 'none' &&
  mkEl('recent-maps-none').style.display === 'block');

// 2. A normal single map renders a card that opens it.
JOBS = [{
  id: 'job_1', name: 'Customer master', source: 'CUST_MASTER', target: 'crm_contacts',
  created: iso(120),
  columnMapping: [
    { srcCol: 'id',    tgtCol: 'contact_id' },
    { srcCol: 'name',  tgtCol: 'full_name' },
    { srcCol: 'email', tgtCol: 'email' },
  ],
}];
sandbox.renderRecentMaps('');
let g = gridHtml();
check('renders a card for a saved map', g.includes('Customer master'));
check('card opens the map on click', g.includes("loadMapById('job_1')"));
check('card shows the source → target path',
  g.includes('CUST_MASTER') && g.includes('crm_contacts'));
check('fully-mapped card shows the column count', g.includes('3 columns'));
check('fully-mapped card shows no warning chip', !g.includes('unmapped'));
check('thumbnail is an inline svg of the mapping', g.includes('<svg') && g.includes('<path d="M72'));
check('offers a "Start a new map" tile', g.includes('rm-new'));

// 3. Counts must be honest: target columns with no source and no literal are
//    NOT mapped, and must surface as a warning rather than inflate the count.
JOBS = [{
  id: 'job_2', name: 'Partly mapped', source: 'SRC', target: 'TGT', created: iso(10),
  columnMapping: [
    { srcCol: 'a', tgtCol: 'x' },
    { tgtCol: 'y' },                       // no source, no literal → unmapped
    { tgtCol: 'z' },                       // ditto
    { literalValue: '0', tgtCol: 'w' },    // fixed value counts as mapped
  ],
}];
sandbox.renderRecentMaps('');
g = gridHtml();
check('unmapped target columns are reported, not hidden', g.includes('2 unmapped'));
check('a fixed value counts as mapped (not flagged as 3)', !g.includes('3 unmapped'));

// 4. One-to-many maps summarise across their target tables.
JOBS = [{
  id: 'job_3', name: 'Split customer', source: 'CUST', created: iso(60 * 30),
  jobType: 'one-to-many',
  tables: [
    { mappings: [{ srcCol:'a', tgtCol:'x' }, { srcCol:'b', tgtCol:'y' }] },
    { mappings: [{ srcCol:'c', tgtCol:'z' }] },
  ],
}];
sandbox.renderRecentMaps('');
g = gridHtml();
check('one-to-many card counts columns across all targets', g.includes('3 columns'));
check('one-to-many card names the target-table count', g.includes('2 target tables'));

// 5. Deleted maps must never appear — the trash is the dashboard's job.
JOBS = [
  { id:'live', name:'Live map', source:'A', target:'B', created: iso(5), columnMapping:[{srcCol:'a',tgtCol:'b'}] },
  { id:'gone', name:'Deleted map', _deleted:true, source:'A', target:'B', created: iso(1), columnMapping:[] },
];
sandbox.renderRecentMaps('');
g = gridHtml();
check('soft-deleted maps are excluded', g.includes('Live map') && !g.includes('Deleted map'));

// 6. Filtering matches name, source and target.
JOBS = [
  { id:'m1', name:'Orders', source:'ORD_HEADER', target:'fact_orders', created: iso(5), columnMapping:[] },
  { id:'m2', name:'Ledger', source:'GL_POSTING', target:'fin_ledger',  created: iso(6), columnMapping:[] },
];
sandbox.renderRecentMaps('ledger');
g = gridHtml();
check('filter matches on name', g.includes('Ledger') && !g.includes('Orders'));
sandbox.renderRecentMaps('ORD_HEADER');
check('filter matches on source table', gridHtml().includes('Orders'));
sandbox.renderRecentMaps('zzz-no-such-map');
check('filter with no matches explains itself', gridHtml().includes('No recent maps match'));

// 7. Pinned maps sort to the front regardless of age.
store['cygenix_pinned_maps'] = JSON.stringify(['m2']);
JOBS = [
  { id:'m1', name:'Newer map', source:'A', target:'B', created: iso(1),   columnMapping:[] },
  { id:'m2', name:'Older pinned', source:'C', target:'D', created: iso(9999), columnMapping:[] },
];
sandbox.renderRecentMaps('');
g = gridHtml();
check('pinned map sorts above a newer unpinned one',
  g.indexOf('Older pinned') < g.indexOf('Newer map'));
check('pinned map is marked as pinned', g.includes('rm-card pinned') || g.includes('pinned'));

// 8. Relative time reads naturally.
check('rmWhen: minutes',   sandbox.rmWhen(iso(5)) === '5 min ago');
check('rmWhen: hours',     sandbox.rmWhen(iso(180)) === '3 hours ago');
check('rmWhen: yesterday', sandbox.rmWhen(iso(60 * 30)) === 'yesterday');
check('rmWhen: no date is blank', sandbox.rmWhen('') === '');

// 9. Names are escaped — map names are user input and land in innerHTML.
JOBS = [{ id:'x', name:'<img src=x onerror=alert(1)>', source:'A', target:'B',
          created: iso(1), columnMapping:[] }];
sandbox.renderRecentMaps('');
check('map names are HTML-escaped', !gridHtml().includes('<img src=x'));

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
