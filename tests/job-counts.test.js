// Tests the Dashboard's job counting.
//
// Three panels on one screen used to count jobs three different ways: the
// donut read `executionStatus || status`, the stat cards read `status` alone,
// and the Project Status panel filtered by project but classified differently
// again. None of them excluded soft-deleted jobs, so trashing a job shrank the
// All Jobs table and left every other number untouched.
//
// The helpers below — liveJobs, jobBucket, jobIsMapped — are now the only way
// a panel counts jobs, so this file pins their behaviour. They are extracted
// from public/dashboard.html and run against stubs.
const fs = require('fs');
const vm = require('vm');

const html = (fs.readFileSync(__dirname + '/../public/dashboard.html', 'utf8') + '\n' + fs.readFileSync(__dirname + '/../public/dashboard-app.js', 'utf8'));
const start = html.indexOf('// ── Job counting — one definition, shared by every panel');
const end   = html.indexOf('function updateStats()');
if (start < 0 || end < 0 || end < start) {
  console.log('FAIL: could not locate the job-counting helpers in dashboard.html');
  process.exit(1);
}

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

// `store` stands in for localStorage's cygenix_jobs.
function build(store = []) {
  const sb = {
    console, JSON, String, Array, Number,
    safeArr: () => store,
  };
  vm.createContext(sb);
  vm.runInContext(html.slice(start, end), sb);
  return sb;
}

const S = build();
const bucket = j => S.jobBucket(j);

console.log('Dashboard — job counting\n');

// ── jobBucket ────────────────────────────────────────────────────────────
check('a complete job buckets as complete', bucket({ status: 'complete' }) === 'complete');
check('a ready job buckets as ready',       bucket({ status: 'ready' })    === 'ready');
check('a failed job buckets as failed',     bucket({ executionStatus: 'failed' }) === 'failed');
check('a job with no status at all buckets as pending', bucket({}) === 'pending');
check('an unrecognised status buckets as pending', bucket({ status: 'weird' }) === 'pending');

// The regression that made the donut and the stat card disagree: a project
// run records its outcome on executionStatus and leaves status at 'ready'.
check('executionStatus wins over the status a job was saved with',
  bucket({ status: 'ready', executionStatus: 'complete' }) === 'complete');
check('a run that failed is failed even if the job was saved complete',
  bucket({ status: 'complete', executionStatus: 'failed' }) === 'failed');

// Imported project bundles carry whatever spelling they were exported with.
check('the success spelling counts as complete',   bucket({ status: 'success' })   === 'complete');
check('the completed spelling counts as complete', bucket({ status: 'completed' }) === 'complete');
check('the error spelling counts as failed',       bucket({ status: 'error' })     === 'failed');
check('the fail spelling counts as failed',        bucket({ status: 'fail' })      === 'failed');
check('sql_ready counts as ready',                 bucket({ status: 'sql_ready' }) === 'ready');
check('status matching is case-insensitive',       bucket({ status: 'Complete' })  === 'complete');

// A null or undefined job must not throw — panels map over raw storage.
check('a null job buckets as pending without throwing', bucket(null) === 'pending');

// ── liveJobs ─────────────────────────────────────────────────────────────
const JOBS = [
  { id:'a', status:'complete' },
  { id:'b', status:'ready' },
  { id:'c', status:'complete', _deleted:true, _deletedAt:'2026-08-01' },
  { id:'d' },
  null,
];
let live = build(JOBS).liveJobs();
check('soft-deleted jobs are excluded', !live.some(j => j.id === 'c'));
check('live jobs are kept', live.map(j => j.id).join(',') === 'a,b,d', live.map(j => j && j.id).join(','));
check('null entries are dropped rather than throwing', live.length === 3);

// Reading from storage and being handed a list must agree.
check('liveJobs reads storage when given no list',
  build(JOBS).liveJobs().length === 3);
check('liveJobs filters a list it is handed',
  build([]).liveJobs(JOBS).length === 3);
check('an empty store yields no jobs', build([]).liveJobs().length === 0);

// A job restored from the trash comes back.
check('restoring a job brings it back into the count',
  build([{ id:'c', status:'complete', _deleted:false }]).liveJobs().length === 1);

// ── jobIsMapped ──────────────────────────────────────────────────────────
// "Analysed / Schema ready" was the same expression as Total Jobs, so the two
// cards could never differ. It now means something.
const mapped = j => S.jobIsMapped(j);
check('a job with a mapped target column is analysed',
  mapped({ columnMapping: [{ srcCol:'a', tgtCol:'b' }] }) === true);
check('a job with no column mapping is not analysed', mapped({}) === false);
check('a job with an empty mapping array is not analysed',
  mapped({ columnMapping: [] }) === false);
check('a mapping row with no target column does not count',
  mapped({ columnMapping: [{ srcCol:'a', tgtCol:'' }] }) === false);
check('a fixed-value row still counts once it has a target',
  mapped({ columnMapping: [{ srcCol:'', tgtCol:'CaseNo' }] }) === true);
check('a null job is not analysed and does not throw', mapped(null) === false);

// ── The panels agree ─────────────────────────────────────────────────────
// Reproduces the reported screen: 20 jobs, one of them trashed, and one run by
// project-builder so its status and executionStatus disagree.
const SCREEN = [
  ...Array.from({ length: 10 }, (_, i) => ({ id:'c'+i, status:'complete' })),
  { id:'pb', status:'ready', executionStatus:'complete' },   // project run
  { id:'r1', status:'ready' },
  { id:'r2', status:'ready' },
  { id:'f1', executionStatus:'failed' },
  ...Array.from({ length: 6 }, (_, i) => ({ id:'p'+i })),     // no status → pending
  { id:'trash', status:'complete', _deleted:true },
];
const sb = build(SCREEN);
const jobs = sb.liveJobs();
const count = b => jobs.filter(j => sb.jobBucket(j) === b).length;

check('the trashed job is out of the total', jobs.length === 20, String(jobs.length));
check('complete counts the project run too', count('complete') === 11, String(count('complete')));
check('ready is counted separately', count('ready') === 2, String(count('ready')));
check('failed is counted separately', count('failed') === 1, String(count('failed')));
check('everything else is pending', count('pending') === 6, String(count('pending')));
check('the buckets account for every live job',
  count('complete') + count('ready') + count('failed') + count('pending') === jobs.length);

// The old stat-card test would have disagreed with the donut on the same data.
const oldStatDone = jobs.filter(j => j.status === 'complete').length;
check('the old status-only test really did undercount project runs',
  oldStatDone === 10 && count('complete') === 11,
  'old=' + oldStatDone + ' new=' + count('complete'));


// ── Regression: the jobs-list TDZ crash ─────────────────────────────────────
// renderJobs() is called by boot code that executes above the parse-memo
// declaration (loadProject). Declared with `let`, the memo was in the
// temporal dead zone for those calls and every boot painted "Error reading
// jobs: Cannot access '_jobsParseMemo' before initialization" instead of the
// user's jobs. The memo (and the sibling debounce timers added in the same
// commit) must be hoisting-safe: `var` plus a guard that treats undefined as
// a cold cache.
{
  const pb = fs.readFileSync(__dirname + '/../public/project-builder-app.js', 'utf8');
  check('the jobs parse memo is hoisting-safe (var, not let)',
    /var _jobsParseMemo;/.test(pb) && !/let _jobsParseMemo/.test(pb));
  check('and readJobsCached guards the cold cache',
    /if \(_jobsParseMemo && _jobsParseMemo\.raw === raw\)/.test(pb));
  check('the debounce timer too', /var _renderJobsTimer;/.test(pb));
  const dash = fs.readFileSync(__dirname + '/../public/dashboard-app.js', 'utf8');
  check('dashboard shared-reports state is hoisting-safe', /var _reportsListShared;/.test(dash));
  const sqe = fs.readFileSync(__dirname + '/../public/sql-editor-app.js', 'utf8');
  check('sql-editor browser debounce timer too', /var _obTimer;/.test(sqe));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
