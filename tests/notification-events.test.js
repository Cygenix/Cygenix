// Tests Settings → Notifications: which run events are allowed to notify.
//
// Delivery needs two independent switches — the event here, and the connector
// in Integrations. This file pins the event half: the defaults, persistence,
// and that notifyIntegrations / notifyRunStarted actually consult them rather
// than firing regardless.
//
// The module is extracted from public/dashboard.html and run against stubs, so
// what is under test is the shipped code.
const fs = require('fs');
const vm = require('vm');

const html = fs.readFileSync(__dirname + '/../public/dashboard.html', 'utf8');
const start = html.indexOf('// ── Which run events are allowed to notify ─');
const end   = html.indexOf('// ── Schedule modal (create + edit) ─');
if (start < 0 || end < 0 || end < start) {
  console.log('FAIL: could not locate the notification-event module in dashboard.html');
  process.exit(1);
}

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

function build(stored, opts = {}) {
  const store = {};
  if (stored !== undefined) store['cygenix_notify_events'] = JSON.stringify(stored);
  const emitted = [];
  const sb = {
    console: { warn(){}, error(){} }, JSON, String, Object, Promise, Array,
    localStorage: {
      getItem: k => (k in store ? store[k] : null),
      setItem: (k, v) => { store[k] = String(v); },
    },
    safeArr: () => opts.projects || [],
    TA: { schedules: opts.schedules || [] },
    window: {
      CygenixIntegrations: opts.noIntegrations ? undefined : {
        emit: async (event, payload) => { emitted.push({ event, payload }); return []; },
      },
    },
  };
  vm.createContext(sb);
  vm.runInContext(html.slice(start, end), sb);
  return { sb, store, emitted, get: e => vm.runInContext(e, sb) };
}

console.log('Settings → Notifications — run events\n');

// ── Defaults ─────────────────────────────────────────────────────────────
// These mirror what the app did before this page existed, so upgrading does
// not silently change anyone's notification volume.
let t = build();
let ev = t.get('notifyEvents()');
check('completed is on by default',  ev.completed === true);
check('failed is on by default',     ev.failed === true);
check('started is off by default',   ev.started === false);

// ── Persistence ──────────────────────────────────────────────────────────
t = build();
t.get("setNotifyEvent('started', true)");
check('turning an event on persists it',
  JSON.parse(t.store['cygenix_notify_events']).started === true, t.store['cygenix_notify_events']);
check('the change is visible immediately', t.get('notifyEvents()').started === true);

t.get("setNotifyEvent('failed', false)");
check('turning an event off persists it', t.get('notifyEvents()').failed === false);
check('other events are untouched by one change', t.get('notifyEvents()').started === true);

t = build({ started: true });
check('a saved value is restored', t.get('notifyEvents()').started === true);
check('keys absent from storage fall back to their default',
  t.get('notifyEvents()').completed === true && t.get('notifyEvents()').failed === true);

// A stored blob from an older release must not leave a new key undefined.
t = build({ completed: false });
ev = t.get('notifyEvents()');
check('a partial saved blob is merged over the defaults, not swapped in',
  ev.completed === false && ev.failed === true && ev.started === false, JSON.stringify(ev));

// Corrupt storage must not take the page down.
{
  const store = { 'cygenix_notify_events': '{not json' };
  const sb = {
    console:{warn(){},error(){}}, JSON, String, Object, Promise, Array,
    localStorage: { getItem: k => store[k] || null, setItem(){} },
    safeArr: () => [], TA: { schedules: [] }, window: {},
  };
  vm.createContext(sb);
  vm.runInContext(html.slice(start, end), sb);
  const e = vm.runInContext('notifyEvents()', sb);
  check('corrupt storage falls back to the defaults',
    e.completed === true && e.failed === true && e.started === false);
}

// ── Mapping a run to its event ───────────────────────────────────────────
t = build();
check('a successful run maps to completed', t.get("runEventKey('success')") === 'completed');
check('a failed run maps to failed',        t.get("runEventKey('failed')") === 'failed');
check('any other terminal state maps to failed — never silently to completed',
  t.get("runEventKey('cancelled')") === 'failed' && t.get("runEventKey('')") === 'failed');

// ── The gate actually gates ──────────────────────────────────────────────
const RUN_OK   = { status:'success', rowsAffected:1500, stepResults:[1,2], id:'run_1' };
const RUN_FAIL = { status:'failed', errorMessage:'timeout', id:'run_2' };

t = build();
t.get('notifyIntegrations(' + JSON.stringify(RUN_OK) + ", 's1')");
check('a completed run notifies when completed is on', t.emitted.length === 1);
check('it emits the success event',
  t.emitted[0].event === 'cygenix.migration.success', t.emitted[0] && t.emitted[0].event);

t = build({ completed: false });
t.get('notifyIntegrations(' + JSON.stringify(RUN_OK) + ", 's1')");
check('a completed run is silent when completed is off', t.emitted.length === 0);

t = build({ completed: false });
t.get('notifyIntegrations(' + JSON.stringify(RUN_FAIL) + ", 's1')");
check('turning completed off does not silence failures', t.emitted.length === 1);
check('a failure emits the failed event', t.emitted[0].event === 'cygenix.migration.failed');
check('the failure carries its error text', t.emitted[0].payload.message === 'timeout');

t = build({ failed: false });
t.get('notifyIntegrations(' + JSON.stringify(RUN_FAIL) + ", 's1')");
check('a failure is silent when failed is off', t.emitted.length === 0);

// ── Started ──────────────────────────────────────────────────────────────
t = build();
t.get("notifyRunStarted('s1','run_9')");
check('started does not fire by default', t.emitted.length === 0);

t = build({ started: true });
t.get("notifyRunStarted('s1','run_9')");
check('started fires once switched on', t.emitted.length === 1);
check('it emits the started event', t.emitted[0].event === 'cygenix.migration.started');
check('it carries the run id', t.emitted[0].payload.runId === 'run_9');
check('it is marked as started, not success', t.emitted[0].payload.status === 'started');

// Turning started on must not affect the terminal events.
t = build({ started: true });
t.get('notifyIntegrations(' + JSON.stringify(RUN_OK) + ", 's1')");
check('switching started on leaves completed working', t.emitted.length === 1);

// ── Context on every event ───────────────────────────────────────────────
// A recipient has to be able to tell which migration a message is about.
t = build({ started: true }, {
  projects: [{ id:'p1', name:'Demo' }],
  schedules: [{ id:'s1', name:'Nightly conversion' }],
});
t.sb.localStorage.setItem('cygenix_active_project_id', 'p1');
t.get('notifyIntegrations(' + JSON.stringify(RUN_OK) + ", 's1')");
check('the project name is included', t.emitted[0].payload.project === 'Demo', JSON.stringify(t.emitted[0].payload));
check('the schedule name is included', t.emitted[0].payload.job === 'Nightly conversion');
check('the row count is included', t.emitted[0].payload.rows === 1500);
check('the step count is included', t.emitted[0].payload.steps === 2);

t.emitted.length = 0;
t.get("notifyRunStarted('s1','run_9')");
check('started carries the same project and job context',
  t.emitted[0].payload.project === 'Demo' && t.emitted[0].payload.job === 'Nightly conversion');

// An unknown schedule must not throw — the run may outlive the schedule row.
t = build();
t.get('notifyIntegrations(' + JSON.stringify(RUN_OK) + ", 'gone')");
check('an unknown schedule still notifies, with a blank job name',
  t.emitted.length === 1 && t.emitted[0].payload.job === '');

// ── Nothing loaded ───────────────────────────────────────────────────────
t = build(undefined, { noIntegrations: true });
t.get('notifyIntegrations(' + JSON.stringify(RUN_OK) + ", 's1')");
t.get("notifyRunStarted('s1','r')");
check('no integrations module is a silent no-op, not a crash', true);

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
