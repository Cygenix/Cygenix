// Runs ta_openScheduleModal end to end against DOM stubs.
//
// This exists because of a regression it would have caught. The job-picker
// change replaced the `const jobs = safeArr('cygenix_jobs')` line inside this
// function with a call to ta_jobOptions(), but three later lines still
// referenced `jobs` — so "+ New schedule" threw ReferenceError and the modal
// silently never opened. Extracting and testing the picker helper alone could
// not see it: the helper was correct, its caller was broken.
//
// So the assertion that matters here is the coarse one — the function runs to
// completion and opens the modal — in every branch it has: create, create with
// a preselected job, edit, and an empty job library.
const fs = require('fs');
const vm = require('vm');

const html = fs.readFileSync(__dirname + '/../public/dashboard.html', 'utf8');

function slice(from, to, what) {
  const a = html.indexOf(from), b = html.indexOf(to);
  if (a < 0 || b < 0 || b < a) {
    console.log('FAIL: could not locate ' + what + ' in dashboard.html');
    process.exit(1);
  }
  return html.slice(a, b);
}

const pickerSrc = slice('// ══════════════════════════════════════════════════════════════════════════\n// TASK AGENT — job picker',
                        '// ══════════════════════════════════════════════════════════════════════════\n// NOTIFICATIONS VIEW',
                        'the job-picker module');
const modalSrc  = slice('async function ta_openScheduleModal(existingId, preselectJobId) {',
                        'function ta_closeScheduleModal() {',
                        'ta_openScheduleModal');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

const job = o => ({ created: '2026-08-01T00:00:00Z', ...o });

function build({ jobs = [], schedules = [], projects = [], activeId = '' } = {}) {
  const els = {};
  const el = id => (els[id] = els[id] || {
    id, value: '', textContent: '', innerHTML: '', checked: false,
    _classes: new Set(),
    classList: { add: c => els[id]._classes.add(c), remove: c => els[id]._classes.delete(c),
                 contains: c => els[id]._classes.has(c) },
    dataset: {},
  });
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g,
    c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

  const called = [];
  const sb = {
    console, JSON, String, Number, Date, Map, Set, Array, isNaN, Promise,
    document: { getElementById: el },
    escapeAttr: esc, escapeHtml: esc,
    liveJobs: () => jobs.filter(j => j && !j._deleted),
    safeArr: () => projects,
    localStorage: { getItem: k => (k === 'cygenix_active_project_id' ? activeId : null) },
    TA: { schedules: [], editingScheduleId: null },
    ta_sched: async (action) => { called.push(action); return { schedules }; },
    ta_renderWeeklyDays: () => called.push('weekly'),
    ta_renderMonthlyDom: () => called.push('monthly'),
    ta_toggleTriggerMode: () => called.push('toggleMode'),
    ta_updateCronPreview: () => called.push('cronPreview'),
    ta_onJobPicked: async (v) => { called.push('jobPicked:' + (v === undefined ? '' : v)); },
    ta_parseCronIntoUI: () => 'daily',
  };
  vm.createContext(sb);
  vm.runInContext(pickerSrc + '\n' + modalSrc, sb);
  return { sb, els, called, el, open: (a, b) => vm.runInContext(
    'ta_openScheduleModal(' + JSON.stringify(a ?? null) + ',' + JSON.stringify(b ?? null) + ')', sb) };
}

const optionCount = h => (h.match(/<option/g) || []).length;

console.log('Task Agent — schedule modal\n');

const JOBS = [
  job({ id:'j1', name:'Demo', source:'dbo.Cases', target:'dbo.cases', created:'2026-08-15T00:00:00Z' }),
  job({ id:'j2', name:'Demo', source:'dbo.addresses', target:'dbo.addresses', created:'2026-08-14T00:00:00Z' }),
  job({ id:'j3', name:'Nightly', created:'2026-08-10T00:00:00Z' }),
];

// ── The regression ───────────────────────────────────────────────────────
// "+ New schedule" is this call with no arguments at all.
(async () => {
  let t = build({ jobs: JOBS });
  let threw = null;
  try { await t.open(); } catch (e) { threw = e; }
  check('"+ New schedule" opens without throwing', threw === null, threw && threw.message);
  check('the modal is actually shown',
    t.el('ta-schedule-modal')._classes.has('open'));
  check('it is titled for creating, not editing',
    t.el('ta-sched-modal-title').textContent === 'New schedule');
  check('the job picker is populated',
    optionCount(t.el('ta-sched-job').innerHTML) === 3,
    t.el('ta-sched-job').innerHTML);
  check('the duplicate names are qualified',
    /dbo\.Cases → dbo\.cases/.test(t.el('ta-sched-job').innerHTML));
  check('create mode starts with a blank name', t.el('ta-sched-name').value === '');
  check('create mode defaults to enabled', t.el('ta-sched-enabled').checked === true);
  check('create mode defaults to a daily trigger', t.el('ta-trigger-mode').value === 'daily');
  check('a job is picked by default when the library is not empty',
    t.called.some(c => c.startsWith('jobPicked')), t.called.join(','));

  // ── Preselected job ────────────────────────────────────────────────────
  t = build({ jobs: JOBS });
  threw = null;
  try { await t.open(null, 'j3'); } catch (e) { threw = e; }
  check('opening with a preselected job does not throw', threw === null, threw && threw.message);
  check('the preselected job is selected', t.el('ta-sched-job').value === 'j3');
  check('the schedule name is prefilled from that job',
    t.el('ta-sched-name').value === 'Nightly', t.el('ta-sched-name').value);

  // A preselect that no longer exists must fall through, not throw.
  t = build({ jobs: JOBS });
  threw = null;
  try { await t.open(null, 'deleted-job'); } catch (e) { threw = e; }
  check('a preselect for a missing job does not throw', threw === null, threw && threw.message);
  check('and leaves the name blank', t.el('ta-sched-name').value === '');

  // A trashed job must not be preselectable — it is not in the picker either.
  t = build({ jobs: [...JOBS, job({ id:'jx', name:'Trashed', _deleted:true })] });
  await t.open(null, 'jx');
  check('a trashed job cannot be preselected', t.el('ta-sched-name').value === '');
  check('and is absent from the picker',
    !/Trashed/.test(t.el('ta-sched-job').innerHTML));

  // ── Edit mode ──────────────────────────────────────────────────────────
  t = build({
    jobs: JOBS,
    schedules: [{ id:'s1', name:'Nightly conversion', jobId:'j3', enabled:true,
                  cron:'0 2 * * *', timezone:'Europe/London', jobVersionId:'v1' }],
  });
  threw = null;
  try { await t.open('s1'); } catch (e) { threw = e; }
  check('edit mode opens without throwing', threw === null, threw && threw.message);
  check('edit mode is titled for editing',
    t.el('ta-sched-modal-title').textContent === 'Edit schedule');
  check('the schedule name is loaded', t.el('ta-sched-name').value === 'Nightly conversion');
  check('its job is selected', t.el('ta-sched-job').value === 'j3');
  check('its enabled state is loaded', t.el('ta-sched-enabled').checked === true);
  check('its cron is loaded', t.el('ta-sched-cron').value === '0 2 * * *');
  check('the job version is passed through', t.called.includes('jobPicked:v1'), t.called.join(','));

  // Editing a schedule that has vanished must not throw.
  t = build({ jobs: JOBS, schedules: [] });
  threw = null;
  try { await t.open('gone'); } catch (e) { threw = e; }
  check('editing a missing schedule does not throw', threw === null, threw && threw.message);
  check('the modal still opens', t.el('ta-schedule-modal')._classes.has('open'));

  // ── Empty library ──────────────────────────────────────────────────────
  t = build({ jobs: [] });
  threw = null;
  try { await t.open(); } catch (e) { threw = e; }
  check('an empty job library does not throw', threw === null, threw && threw.message);
  check('it says there are no jobs',
    /No jobs saved on this browser/.test(t.el('ta-sched-job').innerHTML));
  check('and does not try to pick one',
    !t.called.some(c => c.startsWith('jobPicked')), t.called.join(','));
  check('the modal still opens so the user is not stuck',
    t.el('ta-schedule-modal')._classes.has('open'));

  // A library of nothing but trash behaves the same way.
  t = build({ jobs: [job({ id:'d', name:'Gone', _deleted:true })] });
  await t.open();
  check('a library of only trashed jobs reads as empty',
    /No jobs saved on this browser/.test(t.el('ta-sched-job').innerHTML));

  // ── The chain picker ───────────────────────────────────────────────────
  t = build({ jobs: JOBS, schedules: [{ id:'s1', name:'A' }, { id:'s2', name:'B' }] });
  await t.open('s1');
  check('the chain picker excludes the schedule being edited',
    !/value="s1"/.test(t.el('ta-sched-chain').innerHTML) &&
    /value="s2"/.test(t.el('ta-sched-chain').innerHTML),
    t.el('ta-sched-chain').innerHTML);

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})();
