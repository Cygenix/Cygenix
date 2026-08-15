// Tests the Object Mapping "Recent maps" grid.
// Extracts the real functions out of public/object_mapping.html and runs them
// against stubbed DOM/storage, so the logic under test is the shipped code —
// in particular the counts shown on each card, which must never overstate how
// much of a map is actually mapped.
const fs = require('fs');
const vm = require('vm');

const html = (fs.readFileSync(__dirname + '/../public/object_mapping.html', 'utf8') + '\n' + fs.readFileSync(__dirname + '/../public/object-mapping-app.js', 'utf8'));
// Extract through deleteSavedJob so the card's ✕ path is covered too — that
// also proves RM_PIN_KEY and deleteSavedJob share a scope at runtime.
const start = html.indexOf('// ── Recent maps grid ─');
const end   = html.indexOf('// ── Wasis ─');
if (start < 0 || end < 0 || end < start) {
  console.log('FAIL: could not locate the recent-maps block in object_mapping.html');
  process.exit(1);
}
const code = html.slice(start, end);

// ── stubs ────────────────────────────────────────────────────────────────
let JOBS = [];
let CONFIRMED = true;              // what confirm() returns
let STATUS = [];                   // showStatus() calls
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
  _getProjectList: () => [{ id:'p-demo', name:'Demo' }, { id:'p-dentons', name:'Dentons ROW' }],
  editJobId: null,
  confirm: () => CONFIRMED,
  showStatus: (msg) => STATUS.push(msg),
  clearEditMode: () => { sandbox.editJobId = null; },
  renderLoadMapList: () => {},
  location: { href: 'https://x/object_mapping.html' },
  URL,
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

// 9. Delete from the card. Must go to Trash (recoverable), not vanish.
store['cygenix_jobs'] = '[]';
sandbox.localStorage.setItem = (k, v) => { store[k] = String(v); if (k === 'cygenix_jobs') JOBS = JSON.parse(v); };
const stopped = { called: false };
const fakeEv = { stopPropagation(){ stopped.called = true; } };

JOBS = [
  { id:'keep', name:'Keep me',   source:'A', target:'B', created: iso(1), columnMapping:[] },
  { id:'junk', name:'Junk map',  source:'C', target:'D', created: iso(2), columnMapping:[] },
];
sandbox.renderRecentMaps('');
check('card offers a delete control', gridHtml().includes('rm-del') &&
  gridHtml().includes("rmDelete(event,'junk')"));

CONFIRMED = false;
sandbox.rmDelete(fakeEv, 'junk');
check('delete stops the click from opening the map', stopped.called);
check('declining the confirm keeps the map', JOBS.find(j => j.id === 'junk')._deleted !== true);

CONFIRMED = true;
sandbox.rmDelete(fakeEv, 'junk');
const junk = JOBS.find(j => j.id === 'junk');
check('confirming soft-deletes to Trash (recoverable, not erased)',
  junk && junk._deleted === true && !!junk._deletedAt);
check('the other map is untouched', JOBS.find(j => j.id === 'keep')._deleted !== true);
sandbox.renderRecentMaps('');
check('deleted map disappears from the grid immediately',
  !gridHtml().includes('Junk map') && gridHtml().includes('Keep me'));

// Deleting a pinned map must not leave an orphan pin behind.
store['cygenix_pinned_maps'] = JSON.stringify(['keep']);
CONFIRMED = true;
sandbox.rmDelete(fakeEv, 'keep');
check('deleting a pinned map clears its pin',
  !JSON.parse(store['cygenix_pinned_maps']).includes('keep'));

// Deleting the map you are currently editing clears edit mode.
JOBS = [{ id:'open', name:'Open map', source:'A', target:'B', created: iso(1), columnMapping:[] }];
sandbox.editJobId = 'open';
sandbox.rmDelete(fakeEv, 'open');
check('deleting the currently-open map clears edit mode', sandbox.editJobId === null);

// 10. Delete without confirming — the toggle and the Shift shortcut.
//     Both skip the QUESTION only; the delete must still be soft.
let confirmCalls = 0;
sandbox.confirm = () => { confirmCalls++; return CONFIRMED; };

// (a) preference off → still prompts
delete store[ 'cygenix_rm_skip_delete_confirm' ];
JOBS = [{ id:'a1', name:'A', source:'S', target:'T', created: iso(1), columnMapping:[] }];
confirmCalls = 0; CONFIRMED = true;
sandbox.rmDelete({ stopPropagation(){} }, 'a1');
check('with the preference off, deleting still asks', confirmCalls === 1);

// (b) preference on → no prompt, still soft-deleted
sandbox.rmSetSkipConfirm(true);
check('preference persists to localStorage', store['cygenix_rm_skip_delete_confirm'] === '1');
JOBS = [{ id:'a2', name:'B', source:'S', target:'T', created: iso(1), columnMapping:[] }];
confirmCalls = 0;
sandbox.rmDelete({ stopPropagation(){} }, 'a2');
check('with the preference on, no confirmation is shown', confirmCalls === 0);
check('...and the map is still SOFT-deleted (recoverable)',
  JOBS.find(j=>j.id==='a2')._deleted === true && !!JOBS.find(j=>j.id==='a2')._deletedAt);
check('...and the status says how to get it back',
  STATUS.some(s => /Show deleted/.test(s)));

// (c) Shift+click skips the prompt even with the preference off
sandbox.rmSetSkipConfirm(false);
JOBS = [{ id:'a3', name:'C', source:'S', target:'T', created: iso(1), columnMapping:[] }];
confirmCalls = 0;
sandbox.rmDelete({ stopPropagation(){}, shiftKey: true }, 'a3');
check('Shift+click skips the prompt', confirmCalls === 0);
check('...and still soft-deletes', JOBS.find(j=>j.id==='a3')._deleted === true);

// (d) a plain click with the preference off is unaffected by the shortcut
JOBS = [{ id:'a4', name:'D', source:'S', target:'T', created: iso(1), columnMapping:[] }];
confirmCalls = 0; CONFIRMED = false;
sandbox.rmDelete({ stopPropagation(){}, shiftKey: false }, 'a4');
check('plain click still asks, and declining keeps the map',
  confirmCalls === 1 && JOBS.find(j=>j.id==='a4')._deleted !== true);
CONFIRMED = true;

// 11. Thumbnails are coloured, and the colour is tied to the map's identity.
JOBS = [
  { id:'colour-1', name:'One', source:'S', target:'T', created: iso(1), columnMapping:[{srcCol:'a',tgtCol:'b'}] },
  { id:'colour-2', name:'Two', source:'S', target:'T', created: iso(2), columnMapping:[{srcCol:'a',tgtCol:'b'}] },
];
sandbox.renderRecentMaps('');
g = gridHtml();
check('thumbnails use colour, not grey', g.includes('hsl(') && !g.includes('var(--text3)"'));
check('each card carries its own accent colour', g.includes('--rm-accent:hsl('));
check('links are drawn with a source→target gradient', g.includes('linearGradient') && g.includes('url(#rm'));
// Gradient ids must be unique per card or every thumbnail inherits the first.
const ids = (g.match(/<linearGradient id="([^"]+)"/g) || []).map(s => s.match(/id="([^"]+)"/)[1]);
check('gradient ids are unique across cards', new Set(ids).size === ids.length, ids.join(','));
// Colour derives from the id, so reordering (pin/filter) must not recolour.
const c1 = sandbox.rmAccent('colour-1');
sandbox.renderRecentMaps('two');           // different order/subset
check('a map keeps its colour when the grid reorders', sandbox.rmAccent('colour-1') === c1);
check('different maps get different colours', sandbox.rmAccent('colour-1') !== sandbox.rmAccent('colour-2'));

// 12. Scoping to the active project. Maps built under another project were
//     built against another schema — their tables usually don't exist here.
const PROJ_KEY = 'cygenix_active_project_id';
JOBS = [
  { id:'d1', name:'Cases',            projectId:'p-demo',    source:'dbo.Cases',    target:'dbo.cases',    created: iso(1), columnMapping:[{srcCol:'a',tgtCol:'b'}] },
  { id:'d2', name:'Addresses',        projectId:'p-demo',    source:'dbo.addresses',target:'dbo.addresses',created: iso(2), columnMapping:[{srcCol:'a',tgtCol:'b'}] },
  { id:'x1', name:'timekeeper_dm',    projectId:'p-dentons', source:'dbo.Timekeeper_DM', target:'dbo.Timekeeper', created: iso(3), columnMapping:[{srcCol:'a',tgtCol:'b'}] },
  { id:'x2', name:'tkprrate_to_adj',  projectId:'p-dentons', source:'dbo.TkprRate_DM',   target:'dbo.TkprRate',   created: iso(4), columnMapping:[{srcCol:'a',tgtCol:'b'}] },
  { id:'u1', name:'Legacy unassigned',                       source:'dbo.Old',      target:'dbo.New',      created: iso(5), columnMapping:[{srcCol:'a',tgtCol:'b'}] },
];

store[PROJ_KEY] = 'p-demo';
sandbox.renderRecentMaps('');
g = gridHtml();
check('maps from another project are hidden',
  !g.includes('timekeeper_dm') && !g.includes('tkprrate_to_adj'));
check('maps from the active project are shown',
  g.includes('Cases') && g.includes('Addresses'));
check('maps with no project recorded are kept (not stranded)',
  g.includes('Legacy unassigned'));
check('the header names the active project',
  /Demo/.test(mkEl('rm-scope').textContent));
check('the hidden maps are accounted for, not silently dropped',
  /2 maps from other projects are hidden/.test(mkEl('rm-sub').textContent));
check('"Browse all" still counts every map across projects',
  /Browse all 5 /.test(mkEl('rm-browse-all').textContent));

// Switching project switches the grid.
store[PROJ_KEY] = 'p-dentons';
sandbox.renderRecentMaps('');
g = gridHtml();
check('switching project shows that project\'s maps instead',
  g.includes('timekeeper_dm') && !g.includes('Cases'));

// No active project → nothing to scope by, so show everything.
delete store[PROJ_KEY];
sandbox.renderRecentMaps('');
g = gridHtml();
check('with no active project, all maps are shown',
  g.includes('Cases') && g.includes('timekeeper_dm'));
check('...and no project scope is claimed in the header',
  mkEl('rm-scope').textContent === '');

// An unassigned map is always shown, so a project is only truly empty when
// every other map belongs to a named, different project.
store[PROJ_KEY] = 'p-empty';
sandbox.renderRecentMaps('');
check('an unassigned map keeps a project from looking empty',
  gridHtml().includes('Legacy unassigned'));

JOBS = JOBS.filter(j => j.projectId);        // drop the unassigned one
sandbox.renderRecentMaps('');
check('a project with none of its own says so, not "you have no maps"',
  /No maps saved in this project/.test(mkEl('recent-maps-none').innerHTML) &&
  /exist in other projects/.test(mkEl('recent-maps-none').innerHTML));
check('...and does not fall back to the bare guidance text',
  !/^Select source and target tables to build/.test(mkEl('recent-maps-none').innerHTML));
delete store[PROJ_KEY];

// 13. Names are escaped — map names are user input and land in innerHTML.
JOBS = [{ id:'x', name:'<img src=x onerror=alert(1)>', source:'A', target:'B',
          created: iso(1), columnMapping:[] }];
sandbox.renderRecentMaps('');
check('map names are HTML-escaped', !gridHtml().includes('<img src=x'));

// ── Cancel edit returns to the picker ────────────────────────────────────
// Separate extraction: cancelEdit() lives beside clearEditMode, above the
// recent-maps block.
{
  const cstart = html.indexOf('// Cheap fingerprint of the current mapping');
  const cend   = html.indexOf('function clearEditMode(){');
  if (cstart < 0 || cend < 0 || cend < cstart) {
    check('could locate cancelEdit in the page', false);
  } else {
    let cleared = 0, confirms = 0, answer = true, statuses = [];
    const cs = {
      console, JSON, String, Array,
      srcTable: { fullName: 'dbo.CUST' }, tgtTable: { fullName: 'dbo.contacts' },
      columnMapping: [{ srcCol:'id', tgtCol:'contact_id' }],
      targetTables: [],
      // The Options panel's WHERE conditions and GROUP BY are part of the map,
      // so _mapSignature reads them. Without them in the sandbox it throws and
      // its try/catch returns null, which reads as "nothing changed" and every
      // assertion below passes for the wrong reason.
      whereConds: [{ connector:'AND', text:'' }],
      groupByCols: [],
      $: (id) => ({ id, value:'', textContent: id==='edit-job-name' ? 'Customer master' : '',
                    scrollIntoView(){} }),
      confirm: () => { confirms++; return answer; },
      clearMapState: () => { cleared++; },
      showStatus: (m) => statuses.push(m),
    };
    vm.createContext(cs);
    vm.runInContext(html.slice(cstart, cend), cs);

    // `let` bindings are lexical and are NOT exposed as properties of the vm
    // context, so the snapshot has to be set from inside it — otherwise the
    // code under test keeps reading its own (null) variable and every cancel
    // looks unchanged.
    const snapshot = () => vm.runInContext('_editLoadSig = _mapSignature();', cs);

    // Nothing changed since load → close straight away, no nagging.
    snapshot();
    cs.cancelEdit();
    check('cancel with no changes closes without prompting', confirms === 0 && cleared === 1);
    check('cancel returns to the picker (clears the canvas)', cleared === 1);
    check('cancel says which map was closed',
      statuses.some(s => /Closed "Customer master"/.test(s) && /pick another map/i.test(s)));

    // Edited since load → must warn before discarding.
    snapshot();
    cs.columnMapping.push({ srcCol:'email', tgtCol:'email' });   // an edit
    cleared = 0; confirms = 0; answer = false;
    cs.cancelEdit();
    check('cancel after an edit asks first', confirms === 1);
    check('declining that prompt keeps the map open', cleared === 0);

    answer = true;
    cs.cancelEdit();
    check('accepting the prompt closes the map', cleared === 1);

    // The signature must actually notice the kinds of edits people make.
    const sig0 = snapshot();
    check('an unchanged map produces an identical signature', cs._mapSignature() === sig0);
    cs.columnMapping[0].transform = 'UPPER';
    check('changing a transform is detected', cs._mapSignature() !== sig0);
    cs.columnMapping[0].transform = '';
    cs.$ = (id) => ({ id, value: id==='src-where' ? "active = 1" : '',
                      textContent: id==='edit-job-name' ? 'Customer master' : '', scrollIntoView(){} });
    check('changing the WHERE clause is detected', cs._mapSignature() !== sig0);

    // Adding, editing or removing a condition has to count as a change too —
    // otherwise Cancel would silently discard it.
    cs.$ = (id) => ({ id, value:'', textContent: id==='edit-job-name' ? 'Customer master' : '',
                      scrollIntoView(){} });
    const sig1 = snapshot();
    cs.whereConds.push({ connector:'AND', text:'status = 2' });
    check('adding a WHERE condition is detected', cs._mapSignature() !== sig1);

    const sig2 = snapshot();
    cs.whereConds[1].connector = 'OR';
    check('switching a condition from AND to OR is detected', cs._mapSignature() !== sig2);

    const sig3 = snapshot();
    cs.groupByCols.push('CaseNo');
    check('adding a GROUP BY column is detected', cs._mapSignature() !== sig3);
  }
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
