// tests/analytics.test.js — the Analytics page's arithmetic, pinned.
//
// WHY THIS FILE EXISTS
// The screen this feeds makes claims a reader cannot check: that 86% of
// conversions were automatic, that a project is at risk, that validation is
// passing 94% of the time. The failure mode is not a crash — it is a confident
// number that is wrong, on a page whose whole purpose is to be believed. So
// every model is exercised against fixtures the console can really produce,
// including the awkward ones: a paused stream, a project with no jobs, a
// completed job with no run date, an assurance store that has never run.
//
// Fixtures in, models out, no DOM. Same shape as tests/pipeline.test.js.
'use strict';

const A = require('../public/cygenix-analytics.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const NOW = Date.parse('2026-09-10T12:00:00Z');
const DAY = 86400000;
const ago = (d) => new Date(NOW - d * DAY).toISOString();

console.log('Analytics — the numbers behind the page\n');

/* ── 1. Job predicates ──────────────────────────────────────────────────── */

check('the trash never reaches a total',
  A.liveJobs([{ id: 'a' }, { id: 'b', _deleted: true }]).length === 1);
check('a run result outranks the status the job was saved with',
  A.jobBucket({ status: 'pending', executionStatus: 'complete' }) === 'complete');
check('imported spellings still bucket',
  A.jobBucket({ status: 'sql_ready' }) === 'ready'
  && A.jobBucket({ status: 'SUCCESS' }) === 'complete'
  && A.jobBucket({ status: 'error' }) === 'failed'
  && A.jobBucket({}) === 'pending');
check('a mapping with no target column is not a mapping',
  !A.jobIsMapped({ columnMapping: [{ srcCol: 'id' }] })
  && A.jobIsMapped({ columnMapping: [{ srcCol: 'id', tgtCol: 'id' }] }));

check('an explicit AI flag beats the heuristic',
  A.isAiMapped({ aiMapped: true, columnMapping: [] }));
check('a straight copy is not AI work',
  !A.isAiMapped({ columnMapping: [{ tgtCol: 'id', transform: 'NONE' }] }));
check('a real transform is',
  A.isAiMapped({ columnMapping: [{ tgtCol: 'id', transform: 'UPPER' }] }));

check('creation date is never treated as a run date',
  A.ranAt({ created: ago(3) }) === null
  && A.ranAt({ lastRun: ago(3) }) === Date.parse(ago(3)));

/* ── 2. Ring ────────────────────────────────────────────────────────────── */

const RING_JOBS = [
  { status: 'complete' }, { status: 'complete' },
  { status: 'ready' }, { status: 'failed' }, { status: 'pending' },
  { status: 'complete', _deleted: true },
];
{
  const m = A.ringModel(RING_JOBS);
  check('the ring counts live jobs only', m.total === 5, m.total);
  check('and buckets them the same way the tiles do',
    m.buckets.complete === 2 && m.buckets.ready === 1
    && m.buckets.failed === 1 && m.buckets.pending === 1, JSON.stringify(m.buckets));
  check('complete percentage is rounded from the live total', m.completePct === 40, m.completePct);
  check('segment percentages are carried per bucket',
    m.segments.find((s) => s.key === 'ready').pct === 20);
}
{
  const m = A.ringModel([]);
  check('with no jobs the percentage is null, not zero',
    m.total === 0 && m.completePct === null,
    'a dashboard that says "0% complete" when nothing exists is telling a different lie');
}

/* ── 3. Headline tiles ──────────────────────────────────────────────────── */
{
  const m = A.kpiModel([
    { status: 'ready', columnMapping: [{ tgtCol: 'a' }] },
    { status: 'complete' },
    { status: 'pending', columnMapping: [{ tgtCol: 'b' }] },
    { status: 'complete', _deleted: true },
  ], 4);
  check('Total counts live jobs', m.total === 3, m.total);
  check('Analysed counts mapped jobs, not all jobs', m.analysed === 2, m.analysed);
  check('SQL Generated counts ready OR complete', m.sqlGenerated === 2, m.sqlGenerated);
  check('Analysed and SQL Generated are genuinely different expressions',
    m.analysed !== m.total && m.sqlGenerated !== m.total,
    'these two tiles were once the same expression as Total');
  check('files falls back to zero rather than NaN', A.kpiModel([], undefined).files === 0);
}

/* ── 4. Delivery — the paused-is-not-failed rule ────────────────────────── */

const stream = (over) => Object.assign({
  id: 's', name: 's', status: 'running',
  metrics: { deliveredPerMin: 100, lagSeconds: 5, pendingInStore: 0, dlqDepth: 0, spark: [] },
  delivery: { lagThresholdSeconds: 30 },
}, over || {});

{
  const m = A.deliveryModel({ streams: [
    stream({ id: 'a', status: 'running' }),
    stream({ id: 'b', status: 'lagging', metrics: { deliveredPerMin: 50, lagSeconds: 90, pendingInStore: 12, dlqDepth: 0, spark: [] } }),
    stream({ id: 'c', status: 'paused', metrics: { deliveredPerMin: 0, lagSeconds: 9999, pendingInStore: 3, dlqDepth: 0, spark: [] } }),
    stream({ id: 'd', status: 'failed', metrics: { deliveredPerMin: 0, lagSeconds: 200, pendingInStore: 40, dlqDepth: 7, spark: [] } }),
    stream({ id: 'e', status: 'draft' }),
  ] });

  check('drafts are not counted as streams', m.total === 4, m.total);
  check('a paused stream is not a failed stream',
    m.pausedByUser === 1 && m.failed === 1, JSON.stringify({ p: m.pausedByUser, f: m.failed }));
  check('paused streams do not count as running', m.running === 2, m.running);
  check('a paused stream contributes nothing to the apply rate',
    m.applyRate === 150, m.applyRate);
  check('a paused stream reports no lag rather than a stale one',
    m.streams.find((s) => s.id === 'c').lagSeconds === null,
    'its last reading is hours old and would render as a live measurement');
  check('and is never marked over SLO',
    m.streams.find((s) => s.id === 'c').overSlo === false);
  check('a lagging stream is marked against its own threshold',
    m.streams.find((s) => s.id === 'b').overSlo === true);
  check('worst lag ignores paused streams',
    m.worstLagSeconds === 200, m.worstLagSeconds);
  check('queue depth includes paused streams — the backlog is still real',
    m.queueDepth === 55, m.queueDepth);
  check('dead-letter depth is summed', m.dlqDepth === 7, m.dlqDepth);
}
{
  const m = A.deliveryModel({ streams: [] });
  check('no streams is a reason, not a row of zeroes',
    m.hasData === false && /No streams configured/.test(m.reason), m.reason);
  const never = A.deliveryModel({ streams: [stream({ status: 'draft' })] });
  check('a stream that never started is the same empty case', never.hasData === false);
}

/* ── 5. Quality — built on assurance runs, honest when there are none ────── */
{
  const m = A.qualityModel({ health: null, now: NOW });
  check('no validation runs is a named empty state, not a zero pass rate',
    m.hasData === false && /Data Quality/.test(m.reason), m.reason);
  const zero = A.qualityModel({ health: { runs: 0 }, now: NOW });
  check('a store with rules but no runs is also empty', zero.hasData === false);
}
{
  const store = { runs: [
    { ruleId: 'r1', startedAt: NOW - 0.2 * DAY, status: 'fail' },
    { ruleId: 'r1', startedAt: NOW - 1.2 * DAY, status: 'pass' },
    { ruleId: 'r2', startedAt: NOW - 1.2 * DAY, status: 'warn' },
    { ruleId: 'r3', startedAt: NOW - 40 * DAY, status: 'fail' },   // outside the window
  ], breaches: [], rules: [] };
  const m = A.qualityModel({
    store, now: NOW, days: 14,
    health: { runs: 3, pass: 1, warn: 1, fail: 1, score: 50, activeRules: 3,
              openBreaches: 2, criticalOpen: 1, streakDays: 0, meanTimeToFixMs: 3600000,
              perCategory: { completeness: 100, referential: 25 } },
    series: [null, 80, 50],
  });
  check('the pass rate is assurance\'s own arithmetic, not a second copy',
    m.passRate === 50, m.passRate);
  check('categories are ordered worst first so the tab leads with the problem',
    m.categories[0].key === 'referential', JSON.stringify(m.categories));
  check('the heatmap has one row per rule that ran in the window',
    m.heatmap.length === 2, m.heatmap.map((r) => r.ruleId).join(','));
  const r1 = m.heatmap.find((r) => r.ruleId === 'r1');
  check('today is the last cell and carries the failure',
    r1.days[13] && r1.days[13].fail === 1 && r1.days[13].intensity === 1,
    JSON.stringify(r1.days[13]));
  check('a day a rule never ran is null, not a clean day',
    r1.days[5] === null,
    'zero failures and never checked must not paint the same colour');
  check('a warn counts half a failure, matching the pass-rate formula',
    m.heatmap.find((r) => r.ruleId === 'r2').days[12].intensity === 0.5);
}

/* ── 6. Weekly throughput ───────────────────────────────────────────────── */
{
  const m = A.weeklySeries([
    { status: 'complete', lastRun: ago(1) },
    { status: 'complete', lastRun: ago(2), columnMapping: [{ tgtCol: 'a', transform: 'CAST' }] },
    { status: 'complete', lastRun: ago(9) },
    { status: 'complete', lastRun: ago(400) },     // older than the window
    { status: 'complete', created: ago(3) },       // no run date
    { status: 'ready',    lastRun: ago(1) },       // not complete
  ], 8, NOW);

  check('the newest bucket is last', m.counts[7] === 2, JSON.stringify(m.counts));
  check('a job from last week lands in the previous bucket', m.counts[6] === 1);
  check('jobs outside the window are dropped, not clamped into it',
    m.total === 3, m.total);
  check('only completed jobs count as throughput',
    m.counts.reduce((a, b) => a + b, 0) === 3);
  check('the automatic band is a subset of the same buckets',
    m.automatic[7] === 1 && m.automatic[6] === 0, JSON.stringify(m.automatic));
  check('completions with no run date are reported, not silently dropped',
    m.undated === 1,
    'an imported bundle can be all-complete with no dates, and the chart must say so');
}

/* ── 7. Derived risk ────────────────────────────────────────────────────── */
{
  const base = { now: NOW, jobCount: 5, lastActivityAt: NOW - 1 * DAY };

  const clean = A.riskFor({ end: ago(-90) }, Object.assign({}, base, { readiness: 95 }));
  check('nothing firing is On track, and says so',
    clean.level === 'low' && clean.fired.length === 0 && /None of the four/.test(clean.why));

  const low = A.riskFor({ end: ago(-90) }, Object.assign({}, base, { readiness: 60 }));
  check('low readiness alone is Watch, not At risk',
    low.level === 'medium' && low.fired.join() === 'readiness', JSON.stringify(low.fired));

  const soon = A.riskFor({ end: new Date(NOW + 5 * DAY).toISOString() },
    Object.assign({}, base, { readiness: 95 }));
  check('a cutover window inside 14 days fires on its own',
    soon.fired.join() === 'window' && soon.daysToCutover === 5, soon.daysToCutover);

  const breached = A.riskFor({}, Object.assign({}, base, { readiness: 95, openBreaches: 3 }));
  check('open breaches fire', breached.fired.join() === 'breaches');

  const stalled = A.riskFor({}, Object.assign({}, base, { readiness: 95, lastActivityAt: NOW - 20 * DAY }));
  check('no movement in seven days fires', stalled.fired.join() === 'stalled');

  const fresh = A.riskFor({}, { now: NOW, readiness: 95, jobCount: 0, lastActivityAt: null });
  check('a project with no jobs is not stalled, it has not started',
    fresh.fired.length === 0 && fresh.level === 'low',
    'otherwise every draft project wears a risk pill');

  const two = A.riskFor({ end: new Date(NOW + 3 * DAY).toISOString() },
    Object.assign({}, base, { readiness: 40 }));
  check('two triggers is At risk', two.level === 'high' && two.fired.length === 2);
  check('and the tooltip names which ones fired, rather than asserting a colour',
    /readiness is below 70/.test(two.why) && /inside 14 days/.test(two.why), two.why);

  check('an unparseable cutover date does not fire the window trigger',
    A.riskFor({ end: 'whenever' }, Object.assign({}, base, { readiness: 95 })).fired.length === 0);
  check('an unknown readiness does not count as a low one',
    A.riskFor({}, { now: NOW, jobCount: 1, lastActivityAt: NOW }).fired.length === 0,
    'readiness is null before a mapping exists — that is not a risk, it is no signal');
}

/* ── 8. Portfolio ───────────────────────────────────────────────────────── */
{
  const projects = [{ id: 'p1', name: 'Alpha', client: 'ACME', end: ago(-60) },
                    { id: 'p2', name: 'Beta' }];
  const jobs = [
    { projectId: 'p1', status: 'complete', lastRun: ago(2), columnMapping: [{ tgtCol: 'a', transform: 'CAST' }] },
    { projectId: 'p1', status: 'complete', lastRun: ago(3), columnMapping: [{ tgtCol: 'a', transform: 'NONE' }] },
    { projectId: 'p1', status: 'pending' },
    { projectId: 'p2', status: 'pending' },
    { projectId: 'p1', status: 'complete', _deleted: true },
  ];
  const m = A.portfolioModel({
    projects, jobs, inventory: [{ projectId: 'p1' }],
    readinessById: { p1: 95, p2: 95 }, now: NOW,
  });

  check('one row per project', m.projects.length === 2);
  check('project cards do not count the trash',
    m.projects[0].objects === 3, m.projects[0].objects);
  check('percent complete is the ring percentage for that project',
    m.projects[0].percentComplete === 67, m.projects[0].percentComplete);
  check('a project with no completions reads 0%, because it does have jobs',
    m.projects[1].percentComplete === 0);
  check('objects converted counts completions across every project',
    m.objectsConverted === 2, m.objectsConverted);
  check('automation rate is measured over mapped jobs only',
    m.automationRate === 50 && m.automationBasis === 2,
    'over all four jobs it would read 25% and fall every time a job is created');
  check('the eight-week series is attached per project',
    m.projects[0].series.length === 8 && m.projects[0].series[7] === 2);
  check('at-risk counts only the high pills', m.atRisk === 0, m.atRisk);
}
{
  const m = A.portfolioModel({ projects: [], jobs: [], now: NOW });
  check('no projects yields no rows and a null automation rate',
    m.projects.length === 0 && m.automationRate === null && m.atRisk === 0,
    'zero percent automated is a claim; null is the absence of one');
}

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
