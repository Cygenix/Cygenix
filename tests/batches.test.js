// Tests for saved Batches on the Batches screen (project-builder).
//
// A batch is a named arrangement of jobs: which jobs, in which groups, in what
// order. Before this there was exactly ONE arrangement per project, held on
// the project record — so wanting a second (a full load and a nightly delta; a
// three-job smoke set and the real forty-job run) meant destroying the first.
//
// The design decision worth pinning is that a batch stores job IDS, not copies
// of the jobs. That buys the right behaviour — editing a job's mapping updates
// every batch that includes it, because a batch is a running order and not a
// fork — and it costs one thing: a job deleted afterwards leaves a hole. So
// most of these tests are about that hole being reported rather than loaded.
'use strict';
const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 240) : '')); }
};
const P = (...p) => path.join(__dirname, '..', ...p);
const read = (...p) => fs.readFileSync(P(...p), 'utf8');

const store = {};
global.localStorage = {
  getItem: (k) => (Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null),
  setItem: (k, v) => { store[k] = String(v); },
  removeItem: (k) => { delete store[k]; },
};

const B = require('../public/cygenix-batches.js');

const step = (jobId, name, extra) => Object.assign({ jobId, name, jobType: 'migration' }, extra || {});
const groups = () => ([
  { id: 'g1', name: 'Staging', executionMode: 'sequential',
    steps: [step('j1', 'SQL script', { jobType: 'sql' }), step('j2', 'addresses')] },
  { id: 'g2', name: 'Load', executionMode: 'parallel',
    steps: [step('j3', 'Cases')] },
]);
const jobs = (...ids) => ids.map((id) => ({ id, name: 'job ' + id }));

console.log('Batches — a named running order, not a fork\n');

/* ── 1. Saving ──────────────────────────────────────────────────────────── */

const first = B.saveBatch('p1', 'Nightly delta', groups(), { user: 'me@x', now: '2026-08-24T10:00:00Z' });
check('a batch is saved against its project',
  first.record.name === 'Nightly delta' && first.replaced === false);
check('it counts the jobs and groups it holds',
  first.record.jobCount === 3 && first.record.groupCount === 2,
  first.record.jobCount + '/' + first.record.groupCount);
check('it records who saved it and when',
  first.record.savedBy === 'me@x' && first.record.savedAt === '2026-08-24T10:00:00Z');
check('it is listed for that project', B.batchesFor('p1').length === 1);
check('another project sees none of it', B.batchesFor('p2').length === 0);

// A list of six things called "Nightly" is a list nobody can read later.
const again = B.saveBatch('p1', 'nightly DELTA', groups(), { now: '2026-08-24T11:00:00Z' });
check('saving over the same name replaces rather than duplicating',
  again.replaced === true && B.batchesFor('p1').length === 1);
check('the replacement keeps the original id, so a rename does not orphan it',
  again.record.id === first.record.id);

B.saveBatch('p1', 'Smoke set', [{ id: 'g', name: 'G', steps: [step('j1', 'SQL script')] }],
  { now: '2026-08-24T12:00:00Z' });
check('two differently named batches coexist', B.batchesFor('p1').length === 2);
check('the newest is listed first', B.batchesFor('p1')[0].name === 'Smoke set');

check('a batch with no name is refused', (() => {
  try { B.saveBatch('p1', '   ', groups()); return false; } catch (e) { return /name/i.test(e.message); }
})());
check('a batch with no jobs is refused, saying what to do',
  (() => {
    try { B.saveBatch('p1', 'Empty', [{ id: 'g', steps: [] }]); return false; }
    catch (e) { return /no jobs to save/i.test(e.message) && /Add at least one job/i.test(e.message); }
  })());
check('a batch with no project is refused',
  (() => { try { B.saveBatch('', 'x', groups()); return false; } catch (e) { return true; } })());

/* ── 2. What is stored — ids, not copies ────────────────────────────────── */

const snap = B.snapshotGroups(groups());
check('a stored step keeps the job id', snap[0].steps[0].jobId === 'j1');
check('it keeps the name and type, so a missing job can still be named',
  snap[0].steps[0].name === 'SQL script' && snap[0].steps[0].jobType === 'sql');
check('it does NOT copy the job\'s mapping or SQL — a batch is an order, not a fork',
  JSON.stringify(B.snapshotGroups([{ id: 'g', steps: [step('j1', 'x', {
    columnMapping: [{ srcCol: 'a' }], insertSQL: 'INSERT …', totalRows: 999,
  })] }])).indexOf('insertSQL') === -1);
check('the group\'s execution mode is part of the arrangement',
  snap[0].executionMode === 'sequential' && snap[1].executionMode === 'parallel');
check('an unrecognised execution mode falls back to sequential, the safe one',
  B.snapshotGroups([{ id: 'g', executionMode: 'nonsense', steps: [step('j1')] }])[0].executionMode === 'sequential');
check('a step disabled in this batch stays disabled — that is part of the arrangement',
  B.snapshotGroups([{ id: 'g', steps: [step('j1', 'x', { enabled: false })] }])[0].steps[0].enabled === false);

/* ── 3. Loading — the hole must be reported, not loaded ─────────────────── */

const nightly = B.batchesFor('p1').find((b) => b.name === 'nightly DELTA');
const toStep = (job) => ({ jobId: job.id, name: job.name, status: 'pending' });

const whole = B.resolveBatch(nightly, jobs('j1', 'j2', 'j3'), toStep);
check('with every job present, everything is restored',
  whole.restored === 3 && whole.missing.length === 0 && whole.groups.length === 2);
check('the loaded steps are built by the page\'s own builder, not the snapshot',
  whole.groups[0].steps[0].status === 'pending');

const partial = B.resolveBatch(nightly, jobs('j1', 'j3'), toStep);
check('a deleted job is skipped rather than loaded as a dead step',
  partial.restored === 2 && partial.total === 3);
// "2 jobs are missing" is not actionable; "addresses is missing" is.
check('the missing job is named, not just counted',
  partial.missing.length === 1 && partial.missing[0].name === 'addresses',
  JSON.stringify(partial.missing));
check('the summary says how many of how many, and which are gone',
  /2 of 3 jobs/.test(B.loadSummary(partial, 'Nightly delta'))
  && /addresses/.test(B.loadSummary(partial, 'Nightly delta')),
  B.loadSummary(partial, 'Nightly delta'));
check('a clean load says so plainly, without a scary count',
  /loaded — 3 jobs in 2 groups/.test(B.loadSummary(whole, 'Nightly delta')),
  B.loadSummary(whole, 'Nightly delta'));

// A group that lost every job is an empty box with no explanation on screen.
const lostGroup = B.resolveBatch(nightly, jobs('j1', 'j2'), toStep);
check('a group that lost all its jobs is dropped rather than shown empty',
  lostGroup.groups.length === 1 && lostGroup.groups[0].name === 'Staging',
  JSON.stringify(lostGroup.groups.map((g) => g.name)));
const lostAll = B.resolveBatch(nightly, [], toStep);
check('losing everything still leaves one group — the screen needs somewhere to add jobs',
  lostAll.groups.length === 1 && lostAll.restored === 0 && lostAll.missing.length === 3);
check('and the summary makes clear nothing came back',
  /0 of 3 jobs/.test(B.loadSummary(lostAll, 'Nightly delta')), B.loadSummary(lostAll, 'Nightly delta'));
check('more than three missing jobs are summarised rather than listed forever',
  /and 2 more/.test(B.loadSummary({ restored: 0, total: 5, missing:
    [{name:'a'},{name:'b'},{name:'c'},{name:'d'},{name:'e'}] }, 'X')),
  B.loadSummary({ restored: 0, total: 5, missing: [{name:'a'},{name:'b'},{name:'c'},{name:'d'},{name:'e'}] }, 'X'));

/* ── 4. Rename and delete ───────────────────────────────────────────────── */

check('a batch can be renamed', B.renameBatch('p1', nightly.id, 'Delta run').name === 'Delta run');
check('renaming onto an existing name is refused, naming the clash', (() => {
  try { B.renameBatch('p1', nightly.id, 'Smoke set'); return false; }
  catch (e) { return /already called "Smoke set"/.test(e.message); }
})());
check('renaming a batch that is gone says so', (() => {
  try { B.renameBatch('p1', 'bat_nope', 'x'); return false; }
  catch (e) { return /no longer exists/.test(e.message); }
})());
check('a batch can be deleted', B.deleteBatch('p1', nightly.id) === true && B.batchesFor('p1').length === 1);
check('deleting one that is already gone reports false rather than throwing',
  B.deleteBatch('p1', nightly.id) === false);

B.saveBatch('p2', 'Other project', groups(), { now: '2026-08-24T13:00:00Z' });
B.deleteProjectBatches('p1');
check('deleting a project takes its batches with it', B.batchesFor('p1').length === 0);
check('and leaves other projects alone', B.batchesFor('p2').length === 1);

/* ── 5. Bounds and resilience ───────────────────────────────────────────── */

check('there is a cap per project, so a runaway save cannot fill storage',
  B.MAX_PER_PROJECT > 0 && B.MAX_PER_PROJECT <= 200, String(B.MAX_PER_PROJECT));
for (let i = 0; i < B.MAX_PER_PROJECT + 5; i++) {
  B.saveBatch('p3', 'batch ' + i, groups(), { now: '2026-08-' + String(10 + (i % 20)).padStart(2, '0') + 'T00:00:00Z' });
}
check('the cap holds, oldest dropped first', B.batchesFor('p3').length === B.MAX_PER_PROJECT);
check('a corrupt store reads as empty rather than throwing', (() => {
  store[B.STORE_KEY] = '{not json';
  const ok = B.batchesFor('p1').length === 0;
  delete store[B.STORE_KEY];
  return ok;
})());
check('a store that is an array rather than an object is ignored', (() => {
  store[B.STORE_KEY] = '[]';
  const ok = B.batchesFor('p1').length === 0;
  delete store[B.STORE_KEY];
  return ok;
})());

/* ── 6. Telling the user which one is on screen ─────────────────────────── */

check('an identical arrangement is recognised', B.sameArrangement(groups(), groups()) === true);
check('a reordered arrangement is not', (() => {
  const g = groups(); g[0].steps.reverse();
  return B.sameArrangement(groups(), g) === false;
})());
check('a changed execution mode is not', (() => {
  const g = groups(); g[1].executionMode = 'sequential';
  return B.sameArrangement(groups(), g) === false;
})());
// Comparison is on the arrangement, not on live job detail, or every batch
// would look "changed" the moment somebody edited a mapping.
check('a job\'s own detail changing does not make the arrangement look different', (() => {
  const g = groups(); g[0].steps[0].totalRows = 12345; g[0].steps[0].insertSQL = 'x';
  return B.sameArrangement(groups(), g) === true;
})());

/* ── 7. The page wiring ─────────────────────────────────────────────────── */

const page = read('public', 'project-builder.html');
const app  = read('public', 'project-builder-app.js');

check('the page has a Load batch button', /id="batch-load-btn"[\s\S]{0,120}openBatchPicker\(\)/.test(page));
check('and a Save as batch button', /openSaveBatchPrompt\(\)/.test(page));
check('the picker modal exists', page.includes('id="batch-modal"') && page.includes('id="batch-list"'));
check('the engine loads before the page script that calls it',
  page.indexOf('cygenix-batches.js') > -1
  && page.indexOf('cygenix-batches.js') < page.indexOf('project-builder-app.js'));

check('loading builds steps with the same builder as adding a job by hand',
  /function stepFromJob\(job\)/.test(app)
  && /const step = stepFromJob\(job\);/.test(app)
  && /B\.resolveBatch\(batch, liveJobs\(\), \(job\) => stepFromJob\(job\)\)/.test(app));
check('a load over unsaved work asks first',
  /unsaved changes[\s\S]{0,120}replaces them/.test(app));
check('a load over saved work still asks before replacing it',
  /Replace the ' \+ onScreen \+ ' job\(s\) on screen/.test(app));
check('a loaded arrangement is marked unsaved rather than pretending it was saved',
  /not on the project until the user saves it/.test(app) && /markDirty\(\);\n  hydrateProjectIntoUI\(\);/.test(app));
check('loading a batch whose jobs are all gone refuses instead of emptying the screen',
  /Nothing to load — every job in/.test(app));
check('the picker warns about missing jobs BEFORE you load, not after',
  /no longer exist\$\{gone === 1 \? 's' : ''\}/.test(app));
check('a batch with nothing loadable cannot be loaded at all',
  /disabled title="Every job in this batch has been deleted"/.test(app));
check('batch names are escaped before they reach markup',
  /function escB\(s\)/.test(app) && /escB\(b\.name\)/.test(app));
check('deleting a project cleans up its batches',
  /CygenixBatches\.deleteProjectBatches\(removedId\)/.test(app));
check('deleting a batch says the jobs themselves survive',
  /The jobs themselves are not deleted/.test(app));
check('every entry point degrades when the module is absent rather than throwing',
  /function batchApi\(\) \{ return window\.CygenixBatches; \}/.test(app)
  && (app.match(/if \(!B\)/g) || []).length >= 4);

/* ── 8. The screen is called Batches ────────────────────────────────────── */

check('the browser tab says Batches', /<title>Cygenix – Batches<\/title>/.test(page));
check('the topbar says Batches', />Batches<\/span><\/a>/.test(page));
check('it no longer says Execute Migration', !/Execute Migration/.test(page));
const sidebar = read('public', 'cygenix-sidebar.js');
check('the sidebar item says Batches', /label:'Batches'/.test(sidebar));
// The key is what pages set data-active on and what the dashboard routes from;
// renaming a label must not rename an identifier.
check('its key is unchanged, so nothing that routes on it breaks',
  /key:'project-builder', label:'Batches'/.test(sidebar));
check('the dashboard link says Batches too',
  />Batches<\/a>/.test(read('public', 'dashboard.html')));

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
