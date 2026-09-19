// tests/home-model.test.js — what Home says, worked out before it is drawn.
//
// cygenix-home.js is the pure model behind the redesigned Home (design
// review, Sep-2026, finding 02: "Home does not answer what now"). These pin
// the decisions:
//
//   * no project → three numbered steps, the first live, never "No projects yet";
//   * needs-you is one queue from four reads, sorted severity then age, and
//     only the single most urgent item gets the primary button;
//   * the plan is five rows read off the pipeline model, and cutover is a
//     date, not a percentage, because nothing having cut over is normal;
//   * numbers are short where they are sized and full where they are checked;
//   * a paused schedule's stale next-run is not a run that will happen;
//   * a duration is a dash unless both ends are stored — never a guess.
'use strict';

const H = require('../public/cygenix-home.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};

console.log('Home — the model\n');

const NOW = Date.UTC(2026, 8, 19, 10, 0, 0);       // Sat 19 Sep 2026 10:00 UTC
const h = 3600e3, d = 24 * h;
const iso = (t) => new Date(t).toISOString();

const project = { id: 'p1', name: 'Northbank Core', srcSystem: 'SQL Server 2016', tgtSystem: 'Azure SQL', end: iso(NOW + 6 * d) };
const job = (i, o) => Object.assign({ id: 'j' + i, name: 'Job ' + i, projectId: 'p1', columnMapping: [{ srcCol: 'a', tgtCol: 'a' }],
  insertSQL: 'INSERT', verifySQL: 'SELECT COUNT(*) FROM x WHERE 1=1', status: 'ready' }, o || {});
const stage = (key, count, total, state, pct) => ({ key, count, total, state, pct, label: key, href: '/' + key });
const pipeline = {
  stages: [stage('connect', 2, 2, 'done', 100), stage('analyse', 20, 20, 'done', 100), stage('map', 18, 20, 'active', 90),
           stage('generate', 18, 20, 'active', 90), stage('validate', 9, 20, 'active', 45), stage('cutover', 12, 20, 'active', 60)],
  narrative: 'Loading is under way: 12 of 20 jobs have completed a run.',
};

/* ── The empty state ─────────────────────────────────────────────────────── */
console.log('— no project —');
{
  const m = H.homeModel({ now: NOW, project: null, projects: [], jobs: [], connections: {}, region: 'UK South' });
  check('with no project the model is the empty state, never "No projects yet"',
    m.empty === true && Array.isArray(m.steps) && m.steps.length === 3 && !/No projects yet/.test(JSON.stringify(m)));
  check('three numbered steps: source, target, project',
    m.steps.map(s => s.n).join(',') === '1,2,3' && /source/i.test(m.steps[0].title) && /target/i.test(m.steps[1].title) && /project/i.test(m.steps[2].title));
  check('step 1 is live and steps 2–3 are dimmed until it is done',
    m.steps[0].live === true && m.steps[1].live === false && m.steps[2].live === false);
  const m2 = H.homeModel({ now: NOW, project: null, projects: [], jobs: [], connections: { source: true } });
  check('connecting the source lights step 2 and marks step 1 done',
    m2.steps[0].done === true && m2.steps[1].live === true && m2.steps[2].live === false);
  const m3 = H.homeModel({ now: NOW, project: null, projects: [], jobs: [], connections: { source: true, target: true } });
  check('both connected: step 3 is live and points at a new project',
    m3.steps[2].live === true && /projects\?new=1/.test(m3.steps[2].href));
  check('the footer still counts what exists', /0 projects · 2 connections/.test(m3.footer.line), m3.footer.line);
}

/* ── The header ──────────────────────────────────────────────────────────── */
console.log('\n— the header —');
{
  const m = H.homeModel({ now: NOW, project, projects: [project], jobs: [], connections: { source: true, target: true }, pipeline, region: 'UK South' });
  check('the kicker is migration · source → target · region',
    m.kicker === 'Migration · SQL Server 2016 → Azure SQL · UK South', m.kicker);
  check('the title is the project name, as stored — the stylesheet upper-cases it',
    m.title === 'Northbank Core');
  check('the sentence is the pipeline narrative plus the cutover window in days',
    /12 of 20 jobs/.test(m.sentence) && /Cutover window opens in 6 days\./.test(m.sentence), m.sentence);
  const withAi = H.homeModel({ now: NOW, project, projects: [project], jobs: [], pipeline,
    aiNarrative: { text: 'Delta 04 is loading cleanly; the ledger reconciliation is the one thing to watch before Friday.' } });
  check('a summary the user asked Claude to write outranks the deterministic line, as it does on Analytics',
    /Delta 04 is loading cleanly/.test(withAi.sentence) && !/12 of 20 jobs/.test(withAi.sentence)
    && /Cutover window opens in 6 days/.test(withAi.sentence), withAi.sentence);
  const past = H.homeModel({ now: NOW, project: Object.assign({}, project, { end: iso(NOW - 2 * d) }), projects: [project], jobs: [], pipeline });
  check('a passed cutover date is said plainly', /closed 2 days ago/.test(past.sentence), past.sentence);
}

/* ── The plan ────────────────────────────────────────────────────────────── */
console.log('\n— the plan —');
{
  const rows = H.planRows(pipeline, project, NOW);
  check('five rows, in order: Discover, Map, Load, Validate, Cutover',
    rows.map(r => r.label).join(',') === 'Discover,Map,Load,Validate,Cutover');
  check('a done stage is a full accent-700 bar that says Complete',
    rows[0].fill === 'done' && rows[0].pct === 100 && rows[0].word === 'Complete');
  check('a partial stage says its percentage', rows[1].fill === 'part' && rows[1].pct === 90 && rows[1].word === '90%');
  check('Load in progress says so in words, not a number', rows[2].word === 'In progress' && rows[2].tone === 'accent');
  check('Cutover is the date, not a percentage', rows[4].word === '25 Sep' && rows[4].pct === 0 && rows[4].fill === 'none', rows[4].word);
  const noConn = H.planRows({ stages: [stage('connect', 1, 2, 'attention', 50), stage('analyse', 0, 0, 'waiting', 0), stage('map', 0, 0, 'waiting', 0),
    stage('generate', 0, 0, 'waiting', 0), stage('validate', 0, 0, 'waiting', 0), stage('cutover', 0, 0, 'waiting', 0)] }, {}, NOW);
  check('with one connection, Discover says what to connect', noConn[0].word === 'Connect target' && noConn[0].tone === 'warn', noConn[0].word);
  check('and with no date Cutover says so rather than showing a percentage', noConn[4].word === 'No date');
  const blocked = H.planRows({ stages: pipeline.stages.map(s => s.key === 'validate' ? Object.assign({}, s, { state: 'blocked' }) : s) }, project, NOW);
  check('a blocked stage is the one word in a state colour', blocked[3].word === 'Blocked' && blocked[3].tone === 'fail');
}

/* ── Needs you ───────────────────────────────────────────────────────────── */
console.log('\n— needs you —');
{
  const needs = H.needsYou({
    approvals: [{ id: 'a1', requirement: 'destructive-change', act: 'schema.drop', requestedByEmail: 'j.okafor@example.test', requestedAt: iso(NOW - 72 * 60000) }],
    breaches: [
      { id: 'b1', ruleId: 'REF-001', proves: 'Every payment still points at an account the ledger knows', severity: 'critical', state: 'open', openedAt: NOW - 3 * h, currentRowsFailed: 1204, table: 'Payments' },
      { id: 'b2', ruleId: 'DQ-004', proves: 'No postcode is blank', severity: 'warning', state: 'open', openedAt: NOW - 30 * d, currentRowsFailed: 12 },
      { id: 'b3', ruleId: 'X', severity: 'critical', state: 'resolved', openedAt: NOW - 1 * h },
    ],
    failedJobs: [{ name: 'Customer → customer', lastRun: iso(NOW - 4 * h), lastError: 'String or binary data would be truncated in column memo.' }],
    blockedStreams: [{ name: 'crm-contacts', level: 'blocked', stoppedForSeconds: 38 * 60, consequence: '14,800 changes queued.' },
                     { name: 'orders', level: 'degraded', stoppedForSeconds: 120 }],
  }, NOW);
  check('a resolved breach is not in the queue', needs.length === 6 && !needs.some(n => /X/.test(n.text) && n.kind === 'breach'), needs.length);
  check('severity first: every fail item before every warn item',
    needs.map(n => n.severity).join(',') === 'fail,fail,fail,fail,warn,warn', needs.map(n => n.severity).join(','));
  check('then age, oldest first, within a severity',
    needs[0].kind === 'job' && needs[1].kind === 'breach' && needs[2].kind === 'approval' && needs[3].kind === 'stream',
    needs.map(n => n.kind + '@' + n.age).join(' | '));
  check('exactly one item gets the primary button', needs.filter(n => n.primary).length === 1 && needs[0].primary === true);
  const ap = needs.find(n => n.kind === 'approval');
  check('an approval names the act, the rule, who raised it and how long ago',
    /Approval — destructive change/.test(ap.title) && /Drop a schema object/.test(ap.text) && /j\.okafor/.test(ap.text) && /waiting 1 h 12 min/.test(ap.text), ap.text);
  const br = needs.find(n => n.kind === 'breach' && n.severity === 'fail');
  check('a breach says what the check proves and how many rows, not a rule id',
    /Every payment still points/.test(br.text) && /1,204 rows below threshold in Payments/.test(br.text) && !/REF-001/.test(br.text), br.text);
  const st = needs.find(n => n.kind === 'stream' && n.severity === 'fail');
  check('a blocked stream says how long and what it costs',
    /crm-contacts has not reached the destination for 38 minutes\./.test(st.text) && /14,800 changes queued/.test(st.text), st.text);
  const jb = needs.find(n => n.kind === 'job');
  check('a failed job carries the real error and opens the jobs screen',
    /String or binary data/.test(jb.text) && /goto=jobs/.test(jb.href));
  check('an empty queue is an empty array, not a throw', H.needsYou({}, NOW).length === 0);
}

/* ── Measures, runs, next scheduled ──────────────────────────────────────── */
console.log('\n— measures and runs —');
{
  const jobs = [
    job(1, { name: 'Nightly delta 03', executionStatus: 'complete', startedAt: iso(NOW - 4 * h), lastRun: iso(NOW - 4 * h + 38 * 60000), completedAt: iso(NOW - 4 * h + 38 * 60000), totalRows: 4102884 }),
    job(2, { name: 'Reference load', executionStatus: 'complete', lastRun: iso(NOW - 12 * h), totalRows: 84220 }),
    job(3, { name: 'Preflight — full estate', executionStatus: 'ready', lastRun: iso(NOW - 17 * h) }),
    job(4, { name: 'Nightly delta 02', executionStatus: 'failed', lastRun: iso(NOW - 2 * d), totalRows: 3881540 }),
    job(5, { name: 'Never ran' }),
    job(6, { executionStatus: 'complete', lastRun: iso(NOW - 3 * d) }),
    job(7, { executionStatus: 'complete', lastRun: iso(NOW - 4 * d) }),
  ];
  const ms = H.measures({ confidence: { score: 84, grade: 'amber' } }, jobs, NOW);
  check('readiness is pfConfidence\'s score with its grade, and a percent sign as the unit',
    ms.readiness.value === '84' && ms.readiness.unit === '%' && ms.readiness.grade === 'amber');
  check('no confidence at all is a dash and a pending flag, not a zero',
    H.measures({ confidence: null }, jobs, NOW).readiness.value === '—' && H.measures({ confidence: null }, jobs, NOW).readiness.pending === true);
  check('objects is complete over total', ms.objects.value === '4' && ms.objects.unit === '/7');
  check('rows today sums the runs since midnight and says how many runs',
    ms.rowsToday.value === '4.1' && ms.rowsToday.unit === 'm' && /from 1 run since midnight/.test(ms.rowsToday.note), JSON.stringify(ms.rowsToday));
  check('no runs today is a zero that says so', /no runs since midnight/.test(H.measures({}, [job(9)], NOW).rowsToday.note));

  const runs = H.recentRuns(jobs, NOW, 5);
  check('recent runs are the last five that ran, newest first, and a job that never ran is absent',
    runs.length === 5 && runs[0].name === 'Nightly delta 03' && !runs.some(r => r.name === 'Never ran'));
  check('the four result words, exactly',
    runs.map(r => r.result).slice(0, 4).join(',') === 'Complete,Complete,SQL ready,Failed', runs.map(r => r.result).join(','));
  check('a duration only when both ends are stored — otherwise a dash',
    runs[0].duration === '38 min' && runs[1].duration === '—');
  check('rows in a table are the full figure with separators', runs[0].rows === '4,102,884' && runs[2].rows === '—');
  check('when is Today / Yest. / a weekday / a date', runs[0].started.indexOf('Today ') === 0 && runs[1].started.indexOf('Yest. ') === 0
    && /^[A-Z][a-z]{2} \d\d:\d\d$/.test(runs[3].started), runs.map(r => r.started).join(' | '));

  const next = H.nextScheduled([
    { id: 's1', name: 'Nightly delta 05', enabled: true, nextRunAt: iso(NOW + 16 * h) },
    { id: 's2', name: 'Assurance sweep', enabled: true, nextRunAt: iso(NOW + 17.5 * h) },
    { id: 's3', name: 'Paused but stale', enabled: false, nextRunAt: iso(NOW + 1 * h) },
    { id: 's4', name: 'Retention purge', enabled: true, nextRunAt: iso(NOW + 2 * d) },
    { id: 's5', name: 'Fourth', enabled: true, nextRunAt: iso(NOW + 3 * d) },
  ], NOW, 3);
  check('next scheduled is three enabled runs, soonest first, and a paused schedule\'s stale next-run is not a run',
    next.map(n => n.name).join(',') === 'Nightly delta 05,Assurance sweep,Retention purge', next.map(n => n.name).join(','));
}

/* ── In flight ───────────────────────────────────────────────────────────── */
console.log('\n— in flight —');
{
  check('nothing running is null, not an invented bar', H.inFlight([job(1, { executionStatus: 'complete' })], NOW) === null);
  const f = H.inFlight([job(1, { name: 'Nightly delta 04', executionStatus: 'running', startedAt: iso(NOW - 41 * 60000), rowsDone: 700000, totalRows: 1000000 })], NOW);
  check('a running job carries its name, elapsed, progress and an estimate from its own rate',
    f && f.name === 'Nightly delta 04' && f.pct === 70 && f.elapsed === '41 min' && /~18 min/.test(f.eta), JSON.stringify(f));
}

/* ── Formatting ──────────────────────────────────────────────────────────── */
console.log('\n— formatting —');
check('compact numbers: 12.4m, 84k, 412, 1.2bn',
  H.fmtCompact(12400000).value + H.fmtCompact(12400000).unit === '12.4m' && H.fmtCompact(84220).value + H.fmtCompact(84220).unit === '84k'
  && H.fmtCompact(412).value === '412' && H.fmtCompact(1200000000).value + H.fmtCompact(1200000000).unit === '1.2bn');
check('durations: < 1 min, 38 min, 1 h 22 min, and a dash for nonsense',
  H.fmtDuration(20000) === '< 1 min' && H.fmtDuration(38 * 60000) === '38 min' && H.fmtDuration(82 * 60000) === '1 h 22 min' && H.fmtDuration(-1) === '—');
check('ages: just now, 12 min, 1 h 12 min, 3 days',
  H.fmtAgo(NOW - 10000, NOW) === 'just now' && H.fmtAgo(NOW - 12 * 60000, NOW) === '12 min'
  && H.fmtAgo(NOW - 72 * 60000, NOW) === '1 h 12 min' && H.fmtAgo(NOW - 3 * d, NOW) === '3 days');
check('the model is target-agnostic — nothing in it names a vendor or a product',
  !/Elite|3E|Northbank|Aderant|Thomson/.test(require('fs').readFileSync(__dirname + '/../public/cygenix-home.js', 'utf8')));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
if (fail) process.exit(1);
