// tests/map-groups.test.js — Object Mapping: map groups and colours, Phase 1.
//
// The decisions this phase fixed, pinned as decisions:
//
//   * a map stores the group's ID and NEVER its colour, so recolouring a group
//     changes every map in it with nothing to update and nothing to go stale;
//   * a groupId naming a group that no longer exists displays as Ungrouped and
//     is NOT rewritten — deleting a group is a decision about the group, and
//     if it comes back from another device's sync its maps come back with it;
//   * names are required, trimmed, at most 40 characters and unique ignoring
//     case; colours are six hex digits;
//   * an account with no group list is seeded with four starter groups, and a
//     list that cannot be read is re-seeded rather than left empty;
//   * the group list is account-wide, synced and backed up;
//   * nothing in this feature posts: the list is one localStorage key and a
//     map's group is a field on a job already written to another.
'use strict';

const fs = require('fs');
const path = require('path');
const MG = require('../public/cygenix-map-groups.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

// A localStorage stand-in. The model never reaches for a global, so this is
// all the environment it needs.
function fakeStorage(seed) {
  const map = Object.assign({}, seed || {});
  return {
    map,
    getItem: (k) => (k in map ? map[k] : null),
    setItem: (k, v) => { map[k] = String(v); },
    removeItem: (k) => { delete map[k]; },
  };
}

console.log('Object Mapping — map groups and colours\n');

/* ════════════════════════════════════════════════════════════════════════
   1. Seeding and shape
   ════════════════════════════════════════════════════════════════════════ */
console.log('— the store —');

check('an account with no list gets the four starter groups, in order',
  (() => {
    const s = MG.mgLoad(fakeStorage());
    return s.version === MG.MG_VERSION && s.groups.length === 4
      && s.groups.map(g => g.name).join(',') === 'Master data,Transactional,Reference,Configuration'
      && s.groups.every((g, i) => g.order === i && /^#[0-9A-F]{6}$/.test(g.color));
  })());

check('the starter groups are target-agnostic — no product, module or table name anywhere',
  !/3E|elite|oracle|sql server|vchr|matter/i.test(JSON.stringify(MG.STARTER_GROUPS))
  && !/3E|elite/i.test(read('public', 'cygenix-map-groups.js')));

check('an unreadable list is re-seeded rather than left empty — a corrupt blob is not a choice',
  (() => {
    const a = MG.mgLoad(fakeStorage({ cygenix_map_groups: '{not json' }));
    const b = MG.mgNormalise({ version: 1, groups: 'nonsense' });
    const c = MG.mgNormalise({ version: 1, groups: [] });
    return a.groups.length === 4 && b.groups.length === 4 && c.groups.length === 4;
  })());

check('rows that are not groups are dropped, and what is left is kept as it is',
  (() => {
    const s = MG.mgNormalise({ version: 1, groups: [
      { id: 'a', name: 'Keep', color: '#3b7dd8', order: 5 },
      { id: '', name: 'No id', color: '#000000' },
      { id: 'b', name: '', color: '#000000' },
      { id: 'c', name: 'No colour', color: 'blue' },
      { id: 'a', name: 'Duplicate id', color: '#111111' },
      { id: 'd', name: 'KEEP', color: '#222222' },      // duplicate name, ignoring case
    ] });
    return s.groups.length === 1 && s.groups[0].name === 'Keep'
      && s.groups[0].color === '#3B7DD8' && s.groups[0].order === 0;
  })());

check('a write that cannot land is reported, not swallowed — the user was told it saved',
  (() => {
    const storage = fakeStorage();
    storage.setItem = () => { throw new Error('QuotaExceededError'); };
    const r = MG.mgSave(storage, MG.mgNewStore());
    return r.ok === false && /Quota/.test(r.reason);
  })());

check('every starter colour is in the palette, and the palette is twenty named colours',
  MG.PALETTE.length === 20
  && MG.PALETTE.every(c => c.name && /^#[0-9A-F]{6}$/.test(c.hex))
  && MG.STARTER_GROUPS.every(g => MG.PALETTE.some(c => c.hex === g.color)));

check('no two palette colours are the same, and no two names are',
  new Set(MG.PALETTE.map(c => c.hex)).size === 20
  && new Set(MG.PALETTE.map(c => c.name.toLowerCase())).size === 20);

/* The complaint that produced the wider palette: four dots that had to be
   compared with each other to be told apart, and a grey Configuration that
   read as the same dot as Ungrouped. Distance is measured in plain RGB, which
   is crude but is the thing being judged — two hex values a person is looking
   at side by side. */
const rgb = (h) => [1,3,5].map(i => parseInt(h.slice(i, i+2), 16));
const dist = (a, b) => Math.sqrt(rgb(a).reduce((n, v, i) => n + (v - rgb(b)[i]) ** 2, 0));
check('the four starter colours are clearly apart from each other',
  (() => {
    const cs = MG.STARTER_GROUPS.map(g => g.color);
    let min = Infinity;
    for (let i = 0; i < cs.length; i++) for (let j = i + 1; j < cs.length; j++) min = Math.min(min, dist(cs[i], cs[j]));
    return min > 120;
  })(), 'closest starter pair');

check('…and none of them is mistakable for the Ungrouped grey',
  MG.STARTER_GROUPS.every(g => dist(g.color, MG.UNGROUPED_COLOR) > 100));

check('every palette colour is distinguishable from the Ungrouped grey too',
  MG.PALETTE.every(c => dist(c.hex, MG.UNGROUPED_COLOR) > 60));

/* ── The version-1 → 2 migration ───────────────────────────────────
   The first four starter colours were too close together. Version 2 moves
   them apart — but only where nobody has chosen otherwise. */
const v1 = () => ({ version: 1, groups: [
  { id: 'grp_master', name: 'Master data',   color: '#E4579A', order: 0 },
  { id: 'grp_txn',    name: 'Transactional', color: '#D64545', order: 1 },
  { id: 'grp_ref',    name: 'Reference',     color: '#3B7DD8', order: 2 },
  { id: 'grp_config', name: 'Configuration', color: '#6B7785', order: 3 },
] });

check('an untouched version-1 list is moved to the new starter colours',
  (() => {
    const s = MG.mgNormalise(v1());
    return s.version === 2
      && s.groups.map(g => g.color).join(',') === MG.STARTER_GROUPS.map(g => g.color).join(',');
  })());

check('a starter colour somebody CHANGED is left exactly as they set it',
  (() => {
    const doc = v1();
    doc.groups[1].color = '#8E5BC7';          // chosen by hand
    const s = MG.mgNormalise(doc);
    return s.groups[1].color === '#8E5BC7' && s.groups[3].color === '#3B62D8';
  })());

check('a group the user created is never touched, whatever colour it holds',
  (() => {
    const doc = v1();
    doc.groups.push({ id: 'grp_mine', name: 'Balances', color: '#D64545', order: 4 });
    const s = MG.mgNormalise(doc);
    return s.groups[4].color === '#D64545' && s.groups[4].name === 'Balances';
  })());

check('running it twice changes nothing the second time',
  (() => {
    const once = MG.mgNormalise(v1());
    return JSON.stringify(MG.mgNormalise(once)) === JSON.stringify(once);
  })());

check('a version-2 list is left alone even if it holds an old colour',
  (() => {
    const doc = v1(); doc.version = 2;
    return MG.mgNormalise(doc).groups[1].color === '#D64545';
  })());

/* ════════════════════════════════════════════════════════════════════════
   2. Names and colours
   ════════════════════════════════════════════════════════════════════════ */
console.log('\n— what is allowed —');

check('a name is required, trimmed, and capped at 40 characters',
  (() => {
    const g = MG.mgNewStore().groups;
    return MG.mgValidateName('', g).ok === false
      && MG.mgValidateName('   ', g).ok === false
      && MG.mgValidateName('x'.repeat(41), g).ok === false
      && MG.mgValidateName('x'.repeat(40), g).ok === true
      && MG.mgValidateName('  Trimmed  ', g).name === 'Trimmed';
  })());

check('a duplicate name is refused whatever its case, and says which name clashes',
  (() => {
    const g = MG.mgNewStore().groups;
    const r = MG.mgValidateName('master data', g);
    return r.ok === false && /already a group called "master data"/.test(r.reason);
  })());

check('…but renaming a group may keep its own name',
  MG.mgValidateName('Master data', MG.mgNewStore().groups, 'grp_master').ok === true);

check('a colour is six hex digits, normalised, with or without the hash and in any case',
  MG.mgValidateColor('#3b7dd8') === '#3B7DD8'
  && MG.mgValidateColor('3B7DD8') === '#3B7DD8'
  && MG.mgValidateColor('  #f0a ') === '#FF00AA'        // shorthand is a colour, not an error
  && MG.mgValidateColor('blue') === null
  && MG.mgValidateColor('#12345') === null
  && MG.mgValidateColor('') === null);

check('the pill tint is computed from the colour, so a custom colour tints like a palette one',
  MG.mgTint('#3B7DD8', 0.14) === 'rgba(59,125,216,0.14)'
  && MG.mgTint('nonsense', 0.14) === 'rgba(154,163,174,0.14)');

/* ════════════════════════════════════════════════════════════════════════
   3. Adding, editing, deleting
   ════════════════════════════════════════════════════════════════════════ */
console.log('\n— changes —');

check('a new group is appended with a fresh id and the next order',
  (() => {
    const r = MG.mgAdd(MG.mgNewStore(), { name: 'Balances', color: '#3A9E5F' });
    return r.ok && r.group.id.indexOf('grp_') === 0 && r.group.order === 4
      && r.store.groups.length === 5;
  })());

check('a refused add returns the store untouched — a caller writing the result cannot half-apply it',
  (() => {
    const before = MG.mgNewStore();
    const dup = MG.mgAdd(before, { name: 'Reference', color: '#3A9E5F' });
    const bad = MG.mgAdd(before, { name: 'Fine', color: 'not a colour' });
    return dup.ok === false && bad.ok === false
      && dup.store.groups.length === 4 && bad.store.groups.length === 4
      && /Pick a colour/.test(bad.reason);
  })());

check('renaming and recolouring keep the id, which is what every map holds',
  (() => {
    const r = MG.mgUpdate(MG.mgNewStore(), 'grp_master', { name: 'Core data', color: '#8E5BC7' });
    return r.ok && r.group.id === 'grp_master' && r.group.name === 'Core data' && r.group.color === '#8E5BC7';
  })());

check('an edit to a group that has gone says so rather than creating one',
  MG.mgUpdate(MG.mgNewStore(), 'grp_nope', { name: 'x' }).ok === false);

check('deleting renumbers the rest and can empty the list without it re-seeding itself',
  (() => {
    let s = MG.mgNewStore();
    ['grp_master', 'grp_txn', 'grp_ref', 'grp_config'].forEach(id => { s = MG.mgRemove(s, id).store; });
    const one = MG.mgRemove(MG.mgNewStore(), 'grp_txn');
    return s.groups.length === 0
      && one.ok && one.store.groups.map(g => g.order).join(',') === '0,1,2';
  })());

/* ════════════════════════════════════════════════════════════════════════
   4. What a map holds — the point of the whole design
   ════════════════════════════════════════════════════════════════════════ */
console.log('\n— maps —');

check('a map with no group, and a map from before groups existed, are both Ungrouped',
  (() => {
    const s = MG.mgNewStore();
    return MG.mgGroupOf(s, { id: 'j1' }) === null
      && MG.mgGroupOf(s, { id: 'j2', groupId: null }) === null
      && MG.mgGroupOf(s, { id: 'j3', groupId: '' }) === null;
  })());

/* THE test this design exists to pass. */
check('recolouring a group changes every map in it, because no map ever held a colour',
  (() => {
    const maps = [{ id: 'a', groupId: 'grp_master' }, { id: 'b', groupId: 'grp_master' }];
    const before = MG.mgNewStore();
    const after = MG.mgUpdate(before, 'grp_master', { color: '#8E5BC7' }).store;
    const colours = (s) => maps.map(m => MG.mgGroupOf(s, m).color);
    return colours(before).join(',') === '#E4579A,#E4579A'
      && colours(after).join(',') === '#8E5BC7,#8E5BC7'
      // and nothing about the maps themselves changed
      && JSON.stringify(maps) === '[{"id":"a","groupId":"grp_master"},{"id":"b","groupId":"grp_master"}]';
  })());

check('a groupId naming a deleted group displays as Ungrouped, and the map is NOT rewritten',
  (() => {
    const map = { id: 'a', groupId: 'grp_txn' };
    const after = MG.mgRemove(MG.mgNewStore(), 'grp_txn').store;
    return MG.mgGroupOf(after, map) === null && map.groupId === 'grp_txn';
  })());

check('…so if the group comes back from another device, its maps come back with it',
  (() => {
    const map = { id: 'a', groupId: 'grp_txn' };
    const gone = MG.mgRemove(MG.mgNewStore(), 'grp_txn').store;
    const restored = MG.mgNormalise(MG.mgNewStore());
    return MG.mgGroupOf(gone, map) === null
      && MG.mgGroupOf(restored, map).name === 'Transactional';
  })());

check('usage counts only the groups actually on screen, and counts danglers as ungrouped',
  (() => {
    const s = MG.mgNewStore();
    const u = MG.mgUsage(s, [
      { groupId: 'grp_master' }, { groupId: 'grp_master' },
      { groupId: 'grp_ref' }, { groupId: 'grp_gone' }, {},
    ]);
    return u.used.length === 2 && u.used[0].name === 'Master data' && u.used[0].count === 2
      && u.used[1].name === 'Reference' && u.ungrouped === 2;
  })());

check('nothing at all on screen has a group, so there is nothing for a legend to say',
  MG.mgUsage(MG.mgNewStore(), [{}, { groupId: 'grp_gone' }]).used.length === 0);

/* ════════════════════════════════════════════════════════════════════════
   5. The page and the plumbing
   ════════════════════════════════════════════════════════════════════════ */
console.log('\n— the page —');

const html = read('public', 'object_mapping.html');
const app = read('public', 'object-mapping-app.js');

check('the model is loaded on the page, before the app that uses it',
  /cygenix-map-groups\.js\?v=/.test(html)
  && html.indexOf('cygenix-map-groups.js') < html.indexOf('object-mapping-app.js'));

check('the dropdown sits in the toolbar immediately before Save as job',
  /id="mg-btn"[\s\S]{0,900}id="save-job-btn"/.test(html)
  && !/id="save-job-btn"[\s\S]{0,400}id="mg-btn"/.test(html));

check('it starts disabled, and says why',
  /id="mg-btn"[^>]*disabled/.test(html)
  && /Choose source and target tables first/.test(html));

check('it is a real button with the menu wired to it, so Tab and Enter need no help',
  /<button type="button" class="mg-btn" id="mg-btn"[\s\S]{0,200}aria-haspopup="true"[\s\S]{0,80}aria-expanded="false"/.test(html)
  && /role="menu"/.test(html));

/* The bug this replaced: on the bubble phase the outside-click handler ran
   AFTER the inline onclick, which had already replaced the menu's innerHTML
   and detached the clicked button. An orphan has no ancestors, so closest()
   found nothing, the handler decided the click was outside, and the panel shut
   the instant it opened — "+ New group…" and "Edit groups…" appeared to do
   nothing at all. Capturing asks the question while the target is still in the
   tree. */
check('the outside-click handler CAPTURES, so opening a panel does not close the menu',
  /document\.addEventListener\('click', \(e\) => \{\s*\n\s*if \(!e\.target\.closest \|\| !e\.target\.closest\('#mg-wrap'\)\) mgCloseMenu\(\);\s*\n\s*\}, true\);/.test(app));

check('Esc closes it and the arrows move through it',
  /if \(e\.key === 'Escape'\)\{[\s\S]{0,120}mgCloseMenu\(\)/.test(app)
  && /e\.key !== 'ArrowDown' && e\.key !== 'ArrowUp'/.test(app));

check('the menu offers every group, Ungrouped, New group and Edit groups',
  /mgSetGroup\('\$\{escAttr\(g\.id\)\}'\)/.test(app) && /mgSetGroup\(''\)/.test(app)
  && /\+ New group/.test(app) && /Edit groups/.test(app));

check('deleting a group asks first, and says what happens to its maps',
  /Maps in this group will become Ungrouped/.test(app));

check('a card gets a 4px stripe in the group colour and a pill with the group NAME',
  /\.rm-card\.mg-has\{border-left:4px solid var\(--mg-color/.test(html)
  && /class="mg-pill"/.test(app) && /mg-pill-name/.test(app));

check('colour is never the only signal — every dot is beside the name it stands for',
  (app.match(/class="mg-dot"/g) || []).length >= 4
  && /<span class="mg-dot"[^>]*><\/span>\$\{esc\(g\.name\)\}/.test(app));

check('the legend is built from the maps ON SCREEN, and hidden when none has a group',
  /mgRenderLegend\(shown\)/.test(app)
  && /if \(!use\.used\.length\)\{ el\.style\.display = 'none'/.test(app));

check('Browse all has the filter chips, and the filter is not stored anywhere',
  /id="mg-chips"/.test(html) && /mgFilterMatches/.test(app)
  && /let _mgFilter = '__all__'/.test(app)
  && !/localStorage[^\n]*_mgFilter|setItem\([^)]*mg_filter/.test(app));

check('setting a group patches the stored job in place — not a full re-save of the map',
  /jobs\[idx\] = Object\.assign\(\{\}, jobs\[idx\], \{ groupId: id \}\)/.test(app)
  && /localStorage\.setItem\('cygenix_jobs', JSON\.stringify\(jobs\.slice\(0,100\)\)\)/.test(app));

check('saveAsJob carries the group forward, in both the single and one-to-many paths',
  (app.match(/typeof mgApplyOnSave === 'function'\) mgApplyOnSave\(job, jobs\)/g) || []).length === 2);

check('NOTHING in this feature posts — no fetch, no endpoint, no new action',
  !/fetch\(|\/\.netlify\/|\/api\//.test(read('public', 'cygenix-map-groups.js')));

check('the list syncs to Cosmos and is in the backup',
  /'cygenix_map_groups'/.test(read('public', 'cygenix-cosmos-sync.js'))
  && /map_groups: 'cygenix_map_groups'/.test(read('public', 'cygenix-cosmos-sync.js'))
  && /'cygenix_map_groups'/.test(read('public', 'dashboard-app.js')));

/* The inventory scanner reads public/ for a LITERAL key inside a
   localStorage call, so a key that only ever appears as a constant is one it
   never inventories and the classification test never guards. The page spells
   it out for exactly that reason; this keeps the two from drifting. */
check('the key the page writes is the key the model names',
  new RegExp("localStorage\\.getItem\\('" + MG.MG_KEY + "'\\)").test(app));
check('…so the key is inventoried and classified',
  /cygenix_map_groups/.test(read('docs', 'storage-inventory.md')));

// The brief forbids touching netlify.toml or adding a [[headers]] block for
// this. The file already had several, from long before, so a count is the
// wrong check — it would fail on somebody else's unrelated work. What this
// feature promises is that none of netlify.toml is about it, which is also
// true of the routes: the page is reached at its existing address and no
// redirect was added.
check('netlify.toml says nothing about this feature — no route, no headers block',
  (() => {
    const toml = read('netlify.toml');
    return !/map.?group/i.test(toml)
      && !/map.?group/i.test(read('public', '_redirects'));
  })());

console.log('\n' + pass + ' passed, ' + fail + ' failed');
if (fail) process.exit(1);
