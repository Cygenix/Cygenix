// tests/audit-client.test.js — the browser recorder, and the wiring that has
// to hold for it to ever be called.
//
// Two halves.
//
// The first exercises public/cygenix-audit.js against a fake fetch. The
// behaviour under test is what it does when the network is NOT fine, because
// that is the whole reason the module is shaped the way it is: a failed audit
// write must not break the user's export, but a failed PROD audit write must
// be loud, and neither of those is visible when everything works.
//
// The second is static, and checks the things that make the first half
// reachable at all: the recorder loaded before the code that calls it, the
// nine call sites naming actions the server will actually accept, and the
// legacy in-memory trail genuinely gone rather than merely unrendered.

'use strict';

const fs = require('fs');
const path = require('path');
const schema = require('../netlify/functions/lib/audit-schema');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

const P = (...p) => path.join(__dirname, '..', ...p);
const pub = (f) => fs.readFileSync(P('public', f), 'utf8');

console.log('Audit — the browser recorder and its wiring\n');

// ── Half one: the recorder ────────────────────────────────────────────────
section('1. Recording, and failing to');

// A minimal browser. The module is written for one, so it gets one rather
// than being restructured to suit a test runner.
const calls = [];
let respond = () => Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ recorded: true, id: 'X' }) });
const listeners = {};

const win = {
  getCygenixIdToken: () => 'test-token',
  navigator: { onLine: true },
  console: { warn: function () {} },
  addEventListener: (k, fn) => { listeners[k] = fn; },
  fetch: function (url, init) { calls.push({ url, init }); return respond(url, init); },
  setTimeout: setTimeout,
  clearTimeout: clearTimeout,
};
win.window = win;
global.fetch = win.fetch;

// The module reads `window` at load; give it this one.
const code = pub('cygenix-audit.js');
new Function('window', 'globalThis', 'module', 'fetch', 'setTimeout', 'clearTimeout',
  code)(win, win, { exports: {} }, win.fetch, setTimeout, clearTimeout);
const A = win.CygenixAudit;

check('the module defines the contract the brief asked for',
  typeof A.record === 'function' && typeof A.diff === 'function');

// diff
check('diff returns only changed fields',
  A.diff({ a: 1, b: 2 }, { a: 9, b: 2 }).length === 1);
check('and carries both sides',
  A.diff({ a: 1 }, { a: 9 })[0].before === 1 && A.diff({ a: 1 }, { a: 9 })[0].after === 9);
check('a restricted field list narrows the comparison',
  A.diff({ a: 1, b: 1 }, { a: 2, b: 2 }, ['a']).length === 1);
check('an added field reads as before: null, not undefined (JSON drops undefined)',
  A.diff({}, { a: 1 })[0].before === null);

// Poll rather than await: several of the paths below are fired by an event
// handler that does not hand back a promise.
const until = (fn, ms) => new Promise((resolve, reject) => {
  const t0 = Date.now();
  (function spin() {
    if (fn()) return resolve();
    if (Date.now() - t0 > (ms || 2000)) return reject(new Error('timed out waiting'));
    setTimeout(spin, 10);
  })();
});

// record — the happy path, mostly to establish what the payload looks like.
return (async function () {
  calls.length = 0;
  var r = await A.record({ action: 'jobs.reorder', summary: 'Reordered a job',
                           target: { type: 'job', id: 'j1', label: 'Job: x' } });
  check('a record posts once', calls.length === 1);
  check('to the audit function', /\/\.netlify\/functions\/audit$/.test(calls[0].url));
  check('with the bearer token attached',
    calls[0].init.headers.Authorization === 'Bearer test-token');
  var body = JSON.parse(calls[0].init.body);
  check('as op: record', body.op === 'record');
  check('carrying the action and the summary',
    body.action === 'jobs.reorder' && body.summary === 'Reordered a job');
  check('and it reports success', r.ok === true && r.recorded === true);

  // The server deciding NOT to store an event is a success, not a failure.
  // Retrying it would be the browser arguing with the capture state.
  respond = () => Promise.resolve({ ok: true, status: 200,
    json: () => Promise.resolve({ recorded: false, reason: 'capture paused' }) });
  r = await A.record({ action: 'jobs.reorder' });
  check('a server-side drop is a success with recorded:false, not an error',
    r.ok === true && r.recorded === false);
  check('and it says why, so a caller can tell the user the truth',
    /paused/.test(r.reason));
  check('a dropped event is not queued for retry', A.pending() === 0);

  // A refusal on the merits is final. Retrying gets refused identically.
  respond = () => Promise.resolve({ ok: false, status: 400,
    json: () => Promise.resolve({ error: 'Action not recordable from the browser' }) });
  r = await A.record({ action: 'role.assign' });
  check('a 400 is not retried — it will be refused identically', A.pending() === 0);
  check('and is reported as refused rather than queued', r.refused === true);

  respond = () => Promise.resolve({ ok: false, status: 403,
    json: () => Promise.resolve({ error: 'Not permitted' }) });
  r = await A.record({ action: 'jobs.reorder' });
  check('a 403 is not retried either', A.pending() === 0 && r.refused === true);

  // A transport failure IS retried, because it will probably work next time.
  respond = () => Promise.reject(new Error('network down'));
  r = await A.record({ action: 'jobs.reorder', summary: 'lost one' });
  check('a network failure queues the event', r.queued === true && A.pending() === 1);
  check('and never throws into the caller — an export must not fail because ' +
        'its audit row did not land', r.ok === false);

  // …and flushed when the network comes back, in the order things happened.
  await A.record({ action: 'jobs.validate', summary: 'lost two' });
  check('a second failure queues behind the first', A.pending() === 2);
  calls.length = 0;
  respond = () => Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ recorded: true }) });
  // The listener fires flush() without returning its promise — that is right
  // for an event handler and wrong for an await. Poll the SENDS rather than
  // the queue: flush() drains the queue up front and then sends, so pending()
  // reaches zero before the last request has actually gone out.
  listeners.online();
  await until(() => calls.length === 2);
  check('coming back online flushes the queue', A.pending() === 0);
  check('in the order the events happened, not the order the network recovered',
    JSON.parse(calls[0].init.body).summary === 'lost one' &&
    JSON.parse(calls[1].init.body).summary === 'lost two');

  // Offline is known in advance, so it does not even try.
  win.navigator.onLine = false;
  calls.length = 0;
  r = await A.record({ action: 'jobs.reorder' });
  check('offline queues without a pointless round trip', calls.length === 0 && r.queued === true);
  win.navigator.onLine = true;
  await A.flush();

  // PROD: loud. The page has already made the change by the time this posts,
  // so the module cannot undo it — what it can do is refuse to be quiet.
  respond = () => Promise.reject(new Error('network down'));
  r = await A.record({ action: 'sysparam.update', env: 'PROD' });
  check('a failed PROD audit write is flagged fatal, not swallowed', r.fatal === true);
  r = await A.record({ action: 'sysparam.update', env: 'DEV' });
  check('a failed DEV audit write is not', !r.fatal);
  await A.flush().catch(function () {});

  // Signed out: no identity to attribute it to, and the server would refuse.
  var saved = win.getCygenixIdToken;
  win.getCygenixIdToken = () => '';
  calls.length = 0;
  r = await A.record({ action: 'jobs.reorder' });
  check('signed out, nothing is posted and nothing is queued',
    calls.length === 0 && r.ok === false);
  win.getCygenixIdToken = saved;

  r = await A.record({});
  check('an event with no action is refused locally', r.ok === false);

  // The assistant entry point exists so a page cannot forget the flag.
  respond = () => Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ recorded: true }) });
  calls.length = 0;
  await A.recordAssistant({ action: 'mapping.ai-apply', summary: 'mapped 14 columns' });
  check('recordAssistant marks the actor type, so a page cannot forget to',
    JSON.parse(calls[0].init.body).actorType === 'assistant');

  // ── Half two: the wiring ────────────────────────────────────────────────
  section('2. Wiring');

  const dash = pub('dashboard.html');
  const app = pub('dashboard-app.js');

  const iRec = dash.indexOf('/cygenix-audit.js');
  const iView = dash.indexOf('/audit-app.js');
  const iApp = dash.indexOf('/dashboard-app.js');
  check('the dashboard loads the recorder', iRec > -1);
  check('and the audit screen', iView > -1);
  check('the recorder loads BEFORE dashboard-app.js, which calls it', iRec < iApp);

  check('addAudit records rather than pushing onto an in-memory array',
    /function addAudit\(summary, evt\)/.test(app) && /CygenixAudit\.record/.test(app));
  check('the in-memory legacy trail is gone from state, not merely unrendered',
    !/auditLog:\s*\[\]/.test(app));
  check('and nothing still reads it',
    !/state\.auditLog/.test(app.replace(/\/\/[^\n]*/g, '')));
  check('renderAuditLog hands the screen to audit-app.js',
    /CygenixAuditView\.render/.test(app));
  check('the old per-browser table is gone',
    !/No browser-local entries/.test(app) && !/This browser \(legacy\)/.test(app));
  check('and the footer sending people elsewhere for the full trail with it',
    !/full trail, verification and export/.test(app));

  // Every call site has to name an action the server will accept, or the
  // event is refused at the endpoint and the call site is decoration.
  // Scoped to the addAudit call sites by their action+category pair —
  // dashboard-app.js is 19,000 lines and full of unrelated `action:` keys for
  // its own API calls, which a looser pattern sweeps up.
  const actions = [...app.matchAll(/action:'([a-z.\-]+)', category:'/g)].map((m) => m[1]);
  const audited = actions.filter((a) => schema.isClientAction(a));
  check('every addAudit call site names an action', actions.length >= 8);
  check('and every one of them is on the server\'s client allowlist',
    audited.length === actions.length,
    actions.filter((a) => !schema.isClientAction(a)).join(', '));
  check('the call sites the brief listed are all covered',
    ['jobs.complete', 'jobs.reorder', 'jobs.validate', 'jobs.setup-check',
     'jobs.bulk-generate', 'data.export-scripts', 'data.export-package']
      .every((a) => actions.indexOf(a) !== -1));

  // The sidebar gate.
  const side = pub('cygenix-sidebar.js');
  check('the sidebar gates the Audit Log item on being able to read the trail',
    /key:'audit'[^}]*requiresAuditRead:true/.test(side));
  check('and the gate is evaluated in isItemVisible, with the other gates',
    /requiresAuditRead && !resolveAuditVisibility\(\)/.test(side));
  check('the roles that see it are the ones that read the organisation trail',
    /AUDIT_ROLES = \['OW', 'PA', 'AU'\]/.test(side));
  check('the default before roles are known is hidden, not shown',
    /return false;\s*\n\s*}\s*\n\s*function fetchAuditVisibility/.test(side));

  // The screen itself.
  const view = pub('audit-app.js');
  check('the screen holds no derived truth — the KPIs come from the server',
    /state\.status\.stats/.test(view) && !/prodChanges\s*=/.test(view));
  check('the tab widget is conformant, unlike the console\'s older ones',
    /role="tablist"/.test(view) && /aria-selected/.test(view) &&
    /role="tabpanel"/.test(view) && /ArrowRight/.test(view));
  check('a 403 renders the refusal panel rather than an error',
    /e\.denied/.test(view) && /cyg-a-denied/.test(view));
  check('the drawer shows the whole hash, not a friendly prefix',
    /e\.entryHash\)/.test(view) && !/entryHash\)\.slice\(0, ?10\)/.test(view.split('CHAIN')[1] || ''));
  check('the chain\'s known limit is stated on the page',
    /tamper-evident, not tamper-proof/.test(view));
  check('every colour is a theme token, so the screen follows the workspace theme',
    !/#[0-9a-f]{6}/i.test(view.replace(/rgba\([^)]*\)/g, '')
      .split('\n').filter((l) => !/^\s*(\/\/|\*|\/\*)/.test(l)).join('\n')
      .replace(/box-shadow:0 1px 2px rgba[^;']*/g, '')));

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
