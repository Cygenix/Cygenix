// tests/pipeline.test.js — the Home screen's narrative, derived not decorated.
//
// The card this feeds makes a claim: that one line at the bottom is the thing
// standing between the user and a green cutover. A claim like that is only
// worth making if the derivation behind it is pinned, because the failure mode
// is not a crash — it is a confident sentence about the wrong stage, which
// nobody can tell is wrong by looking at it.
//
// So: fixtures in, model out, no DOM. Every case below is a project shape the
// console can really produce, including the one where the counters disagree.
'use strict';

const P = require('../public/cygenix-pipeline.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const PROJECT = { id: 'p1', name: 'Demo', end: '2099-01-01' };
const BOTH = { source: 'Server=a;Database=b', target: 'Server=c;Database=d' };
const NOW = Date.parse('2026-06-01T12:00:00Z');

/** A job at a given depth of the pipeline. */
function job(depth, over) {
  const j = { id: 'j' + Math.random().toString(36).slice(2, 7), status: 'pending' };
  if (depth >= 1) j.tables = [{ name: 'dbo.Customers', rows: 10 }];
  if (depth >= 2) j.columnMapping = [{ srcCol: 'id', tgtCol: 'id' }];
  if (depth >= 3) { j.insertSQL = 'INSERT INTO x SELECT * FROM y'; j.status = 'ready'; }
  if (depth >= 4) j.verifySQL = 'SELECT COUNT(*) FROM target WHERE 1=1 -- long enough to count';
  if (depth >= 5) j.status = 'complete';
  return Object.assign(j, over || {});
}
const model = (over) => P.toPipelineModel(Object.assign({
  project: PROJECT, jobs: [], connections: BOTH,
  quality: { rulesCount: 0, inspectedJobs: 0 }, now: NOW,
}, over || {}));
const stage = (m, k) => m.stages.find((s) => s.key === k);

console.log('Migration pipeline — six stages, one bottleneck, one sentence\n');

/* ── 1. Shape ────────────────────────────────────────────────────────────── */

const healthy = model({ jobs: [job(5), job(5)], quality: { rulesCount: 3, inspectedJobs: 2 } });

check('always six stages, always in lifecycle order',
  healthy.stages.length === 6
  && healthy.stages.map((s) => s.key).join(',') === 'connect,analyse,map,generate,validate,cutover',
  healthy.stages.map((s) => s.key).join(','));
check('every stage has a denominator, never a bare number',
  healthy.stages.every((s) => typeof s.total === 'number' && /\bof\b|no jobs/.test(s.headline)),
  healthy.stages.map((s) => s.headline).join(' | '));
check('every stage deep-links to the screen that acts on it',
  healthy.stages.every((s) => typeof s.href === 'string' && s.href.length > 1));
check('a finished project has no bottleneck at all, rather than a worst stage',
  healthy.bottleneck === null, healthy.bottleneck);
check('and says so plainly',
  /Nothing is blocking/.test(healthy.narrative), healthy.narrative);

/* ── 2. Every non-done state explains itself ─────────────────────────────── */

const empty = model({ project: PROJECT, jobs: [], connections: {} });
check('no connections at all is blocked, not merely empty',
  stage(empty, 'connect').state === 'blocked');
check('and the blockage says what is missing',
  /Neither a source nor a target/.test(stage(empty, 'connect').reason),
  stage(empty, 'connect').reason);
check('one side connected is attention, and names the missing side',
  (() => {
    const m = model({ connections: { source: 'x' } });
    const s = stage(m, 'connect');
    return s.state === 'attention' && /target is needed/.test(s.reason);
  })());
check('every blocked, attention or waiting stage carries a reason',
  [empty, model({ jobs: [job(1)] }), healthy].every((m) =>
    m.stages.filter((s) => s.state !== 'done' && s.state !== 'active')
      .every((s) => typeof s.reason === 'string' && s.reason.length > 10)),
  'a state with no reason is the em-dash problem in a different font');

/* A zero because nothing has reached here yet, and a zero because it was
   skipped, must not read the same. */
const noJobs = model({ jobs: [] });
check('with no jobs the downstream stages are waiting, not failing',
  ['analyse', 'map', 'generate', 'validate', 'cutover']
    .every((k) => stage(noJobs, k).state === 'waiting'));
check('and the reason says there is nothing to do rather than something wrong',
  /no jobs/i.test(stage(noJobs, 'analyse').reason), stage(noJobs, 'analyse').reason);
check('a stage nothing has reached yet is waiting; one that was skipped is attention',
  (() => {
    const fresh = model({ jobs: [job(0)] });            // nothing analysed
    const skipped = model({ jobs: [job(3)] });          // generated, never verified
    return stage(fresh, 'map').state === 'waiting'
      && stage(skipped, 'validate').state === 'attention';
  })(),
  JSON.stringify([stage(model({ jobs: [job(0)] }), 'map').state,
                  stage(model({ jobs: [job(3)] }), 'validate').state]));

/* ── 3. One bottleneck, chosen the same way every time ───────────────────── */

check('the earliest blocked stage wins over any attention stage',
  (() => {
    const m = model({ jobs: [job(3), job(0)], connections: {} });
    return m.bottleneck === 'connect';
  })());
check('with nothing blocked, the attention stage that has cleared least wins',
  (() => {
    // analyse 2/2 done, map 1/2, validate 0/2 → validate has cleared least.
    const m = model({ jobs: [job(3), job(1)] });
    return m.bottleneck === 'validate';
  })(), model({ jobs: [job(3), job(1)] }).bottleneck);
check('never more than one',
  [empty, noJobs, healthy, model({ jobs: [job(3), job(1)] })]
    .every((m) => m.bottleneck === null || typeof m.bottleneck === 'string'));
check('the bottleneck is always a stage that exists',
  [empty, model({ jobs: [job(1)] })].every((m) =>
    m.bottleneck === null || m.stages.some((s) => s.key === m.bottleneck)));

/* ── 4. The counters can disagree, and that is the story (§A.7) ──────────── */
//
// On the live Demo project Home showed ANALYSED 2 and SQL GENERATED 4 out of
// TOTAL JOBS 5 — more jobs with SQL than jobs analysed, which should be
// impossible if generate depends on analyse. It is possible, because the two
// tiles test independent facts: a mapped target column versus a status. A
// funnel that quietly widened would look like a rendering bug, so the model
// names it instead.

const inverted = model({
  jobs: [
    job(3),                                       // mapped and generated
    job(3),                                       // mapped and generated
    { id: 'x1', status: 'ready', insertSQL: 'INSERT …' },   // SQL, never mapped
    { id: 'x2', status: 'ready', insertSQL: 'INSERT …' },   // SQL, never mapped
    { id: 'x3', status: 'pending' },
  ],
});
check('a widening funnel is detected rather than drawn',
  inverted.inversions.length > 0
  && inverted.inversions[0].behind === 'map' && inverted.inversions[0].ahead === 'generate',
  JSON.stringify(inverted.inversions));
check('the stage that is behind is the one flagged',
  stage(inverted, 'map').state === 'attention');
check('and its reason names both counts, so the user can go and look',
  /4 jobs reached generate but only 2 cleared map/.test(stage(inverted, 'map').reason),
  stage(inverted, 'map').reason);
check('the narrative leads with the contradiction, not with a tidier stage',
  /should not be able to disagree/.test(inverted.narrative), inverted.narrative);

/* ── 5. The narrative ────────────────────────────────────────────────────── */

const cases = [
  ['no connections', model({ connections: {} }),
    /No source or target connected yet/],
  ['no jobs', noJobs, /no jobs yet/],
  ['schema analysis outstanding', model({ jobs: [job(1), job(0), job(0)] }),
    /2 of 3 jobs still need schema analysis/],
  ['data quality never run', model({ jobs: [job(4), job(4)], quality: { rulesCount: 0, inspectedJobs: 0 } }),
    /Data quality has never run\. That is the single thing/],
  ['data quality never run, with unverified jobs too',
    model({ jobs: [job(4), job(3)], quality: { rulesCount: 0, inspectedJobs: 0 } }),
    /Data quality has never run, and 1 of 2 jobs has no verify SQL/],
  ['validation short of complete',
    model({ jobs: [job(4), job(3)], quality: { rulesCount: 2, inspectedJobs: 2 } }),
    /1 of 2 jobs has no verify SQL, so 50% of the migration is unverified/],
  ['all green, planning phase',
    model({ jobs: [job(4), job(4)], quality: { rulesCount: 1, inspectedJobs: 2 } }),
    /Nothing is blocking — the next milestone is a full test run before 1 Jan 2099/],
  ['past target end, incomplete',
    P.toPipelineModel({ project: { id: 'p', name: 'Late', end: '2026-01-01' }, jobs: [job(4), job(5)],
      connections: BOTH, quality: { rulesCount: 1, inspectedJobs: 2 }, now: NOW }),
    /Target end date 1 Jan 2026 has passed with 1 job incomplete/],
  ['a failed job outranks everything downstream of it',
    model({ jobs: [job(4, { status: 'failed' }), job(4)] }),
    /1 job has failed/],
];
cases.forEach(([label, m, re]) => {
  check('narrative — ' + label, re.test(m.narrative), m.narrative);
});

check('the narrative is one to three sentences and stays under 200 characters',
  cases.every(([, m]) => m.narrative.length <= 200 && (m.narrative.match(/\./g) || []).length <= 3),
  cases.map(([l, m]) => l + ': ' + m.narrative.length).join(' | '));
// The rule is no em-dash standing IN FOR a value — "— CHECKING…" — not no
// em-dash in prose, where it is ordinary punctuation. So: never at the start,
// never at the end, and never the whole line.
check('and never uses an em-dash as a stand-in for a missing value',
  cases.every(([, m]) => !/^\s*—/.test(m.narrative) && !/—\s*$/.test(m.narrative)
    && m.narrative.replace(/[—\s]/g, '').length > 20),
  cases.map(([l, m]) => l + ': ' + m.narrative).find((s) => /—\s*$/.test(s)));
check('no project at all is stated, not left blank',
  /No project is active/.test(P.toPipelineModel({ project: null, jobs: [] }).narrative));

/* ── 6. Confidence, with arithmetic that adds up ─────────────────────────── */

const CONF = {
  score: 82, grade: 'amber',
  parts: [
    { key: 'mapping', label: 'Mapping confidence', weight: 30, hasData: true, score: 95, note: '12 mapped columns' },
    { key: 'preflight', label: 'Preflight risk', weight: 40, hasData: false, score: null, note: 'no preflight run yet' },
    { key: 'delivery', label: 'Jobs ready', weight: 30, hasData: true, score: 67, note: '2 of 3 jobs ready' },
  ],
};
const drivers = P.confidenceDrivers(CONF);
check('a driver per component, including the ones with no data',
  drivers.length === 3 && drivers.filter((d) => !d.hasData).length === 1);
check('the contributions add up to the score that is on screen',
  Math.abs(drivers.reduce((a, d) => a + d.points, 0) - 81) <= 1,
  drivers.map((d) => d.label + ': ' + d.points).join(' + '));
check('and what each component costs adds up to what is missing from 100',
  Math.abs(drivers.reduce((a, d) => a + d.lost, 0) - 19) <= 1,
  drivers.map((d) => d.lost).join(' + '));
check('a component with no data contributes nothing rather than a guess',
  drivers.find((d) => d.key === 'preflight').points === 0);
check('every driver links to where it is fixed',
  drivers.every((d) => /^\//.test(d.href)));
check('the band is words, not a colour',
  model({ confidence: CONF }).confidence.band === 'caution');
check('no confidence data at all yields no confidence block rather than a zero',
  model({}).confidence === null);

/* ── 7. The predicates match the dashboard's own ─────────────────────────── */
//
// Two screens disagreeing about what "analysed" means is the bug this card
// exists to end, so these are pinned against the same job shapes
// dashboard-app.js tests.
const pr = P.__predicates;
check('a job counts as mapped only with a target column, as the dashboard has it',
  pr.isMapped({ columnMapping: [{ srcCol: 'a', tgtCol: 'b' }] }) === true
  && pr.isMapped({ columnMapping: [{ srcCol: 'a' }] }) === false
  && pr.isMapped({}) === false);
check('SQL counts whether it is stored on the job or implied by its status',
  pr.hasSql({ insertSQL: 'x' }) && pr.hasSql({ status: 'ready' })
  && pr.hasSql({ status: 'complete' }) && !pr.hasSql({ status: 'pending' }));
check('a verify block has to be long enough to be a real check',
  pr.hasVerify({ verifySQL: 'SELECT COUNT(*) FROM a_table_somewhere' })
  && !pr.hasVerify({ verifySQL: 'SELECT 1' }));
check('the status spellings a project import can carry are all understood',
  ['complete', 'completed', 'success'].every((s) => pr.bucketOf({ status: s }) === 'complete')
  && ['failed', 'fail', 'error'].every((s) => pr.bucketOf({ status: s }) === 'failed')
  && ['ready', 'sql_ready', 'sql-ready'].every((s) => pr.bucketOf({ status: s }) === 'ready'));
check('executionStatus wins over the saved status, as it does on the dashboard',
  pr.bucketOf({ status: 'ready', executionStatus: 'complete' }) === 'complete');

/* ── 8. Determinism ──────────────────────────────────────────────────────── */

check('the same input gives the same model, so the sentence does not wander',
  (() => {
    const jobs = [job(3), job(1), job(4)];
    const a = P.toPipelineModel({ project: PROJECT, jobs, connections: BOTH, quality: {}, now: NOW });
    const b = P.toPipelineModel({ project: PROJECT, jobs, connections: BOTH, quality: {}, now: NOW });
    return JSON.stringify(a) === JSON.stringify(b);
  })());
check('and it never mutates the jobs it was given',
  (() => {
    const jobs = [job(3)];
    const before = JSON.stringify(jobs);
    P.toPipelineModel({ project: PROJECT, jobs, connections: BOTH, quality: {}, now: NOW });
    return JSON.stringify(jobs) === before;
  })());

/* ── 9. Wiring ───────────────────────────────────────────────────────────── */

const fs = require('fs');
const path = require('path');
const read = (...p) => fs.readFileSync(path.join(__dirname, '..', ...p), 'utf8');
const dash = read('public', 'dashboard.html');
const app = read('public', 'dashboard-app.js');

check('the dashboard loads the model', /<script src="\/cygenix-pipeline\.js/.test(dash));
check('the card sits above the KPI tiles, not below them',
  dash.indexOf('id="migration-pipeline"') !== -1
  && dash.indexOf('id="migration-pipeline"') < dash.indexOf('class="stats-row"'));
check('and renders on every dashboard paint, not on a click',
  /renderMigrationPipeline\(\);/.test(app) && /function renderDashboard\(\)[\s\S]{0,400}renderMigrationPipeline\(\)/.test(app));
check('the renderer consumes a model and derives nothing itself',
  /CygenixPipeline\.toPipelineModel\(pipelineInput\(\)\)/.test(app)
  && !/function renderMigrationPipeline[\s\S]{0,4000}columnMapping/.test(app),
  'a second copy of "what counts as mapped" is how two panels start disagreeing');

check('the opt-in AI summary button is gone',
  !/Generate summary/.test(app) && !/id="ps-ai-btn"/.test(app),
  'a narrative behind a button at the bottom of the page is a narrative nobody reads');
check('and no Anthropic call happens on a render',
  !/renderMigrationPipeline[\s\S]{0,3000}api\.anthropic\.com/.test(app),
  'the call bills the user\'s own key — spending it on a page load is not a thing to do quietly');
check('a cached AI narrative still replaces the deterministic line when there is one',
  /readAiNarrative\(model\.project\.id\)/.test(app) && /ai \? ai\.text : model\.narrative/.test(app));
check('and a regenerate control remains',
  /refreshPipelineNarrative/.test(app) && /↻/.test(app));

check('the confidence tile is taken apart with the pipeline\'s own drivers',
  /CygenixPipeline\.confidenceDrivers\(c\)/.test(app),
  'one scoring function, one source of truth — not a second copy for the UI');
check('the breakdown shows the arithmetic, not just the components',
  /drivers\.filter\(d => d\.hasData\)\.map\(d => '\+' \+ d\.points\)\.join\(' '\)/.test(app));
check('and each driver offers somewhere to go and fix it',
  /class="mp-conf-fix"/.test(app));

check('the stat tiles state which population they count',
  (dash.match(/class="stat-scope"/g) || []).length >= 3,
  'four tiles counting every project above a panel counting one, and neither saying so');
check('no headline value on the overview is left as a bare em-dash',
  !/valEl\.textContent = '—'/.test(app)
  && !/value: dqRun \? `\$\{dqPct\}%` : '—'/.test(app)
  && !/validatedJobs\.length === 0 \? '—'/.test(app));
check('every state in the strip is a word as well as a colour',
  /STATE_WORD = \{ done: '✓ done'/.test(app)
  && /mp-state mp-state-\$\{s\.state\}/.test(app));

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
