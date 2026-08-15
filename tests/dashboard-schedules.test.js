// Tests the Schedules summary on the dashboard home screen.
//
// It mirrors the Task Agent read-only, so the state of the automation is
// visible where everyone lands. That means it must be honest about three
// things the Task Agent page handles implicitly: a paused schedule's stale
// nextRunAt, a failed last run, and the scheduler being unreachable — the
// home screen must not simply go blank when the network call fails.
//
// Extracted from public/dashboard.html and run against stubs, so what is
// under test is the shipped code.
const fs = require('fs');
const vm = require('vm');

const html = fs.readFileSync(__dirname + '/../public/dashboard.html', 'utf8');
const start = html.indexOf('// ══════════════════════════════════════════════════════════════════════════\n// HOME — schedules summary');
const end   = html.indexOf('// ══════════════════════════════════════════════════════════════════════════\n// RING CHART');
if (start < 0 || end < 0 || end < start) {
  console.log('FAIL: could not locate the home schedules module in dashboard.html');
  process.exit(1);
}

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

function build({ schedules = [], error = null, jobName = 'Nightly job' } = {}) {
  const els = {};
  const el = id => (els[id] = els[id] || { id, innerHTML: '', textContent: '' });
  ['dashboard-schedules-list', 'dashboard-schedules-sub'].forEach(el);
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g,
    c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const calls = [];
  const sb = {
    console, JSON, String, Number, Array, Promise, Date,
    document: { getElementById: id => els[id] || null },
    escapeHtml: esc,
    TA: { schedules: [] },
    ta_sched: async (action) => {
      calls.push(action);
      if (error) throw new Error(error);
      return { schedules };
    },
    ta_fmtDateTime: iso => 'AT:' + iso,
    ta_runStatusChip: s => '<span class="chip">' + esc(s || '?') + '</span>',
    ta_jobNameFor: () => jobName,
    showView: () => {},
  };
  vm.createContext(sb);
  vm.runInContext(html.slice(start, end), sb);
  return {
    sb, els, calls,
    list: () => els['dashboard-schedules-list'].innerHTML,
    sub:  () => els['dashboard-schedules-sub'].textContent,
    load: () => vm.runInContext('loadDashboardSchedules()', sb),
  };
}

const S = o => ({ id:'s1', name:'A schedule', enabled:true, cron:'0 2 * * *', ...o });
const rowNames = h => [...h.matchAll(/<strong style="color:var\(--text\)">([\s\S]*?)<\/strong>/g)].map(m => m[1]);

console.log('Dashboard home — schedules summary\n');

(async () => {
  // ── The counts ─────────────────────────────────────────────────────────
  let t = build({ schedules: [
    S({ id:'a', name:'Alpha', enabled:true }),
    S({ id:'b', name:'Bravo', enabled:false }),
    S({ id:'c', name:'Cee',   enabled:true }),
  ]});
  await t.load();
  check('the schedules are fetched once', t.calls.length === 1 && t.calls[0] === 'list-schedules');
  check('the total is stated', /3 schedules/.test(t.sub()), t.sub());
  check('active and paused are counted separately',
    /2 active/.test(t.sub()) && /1 paused/.test(t.sub()), t.sub());
  check('every schedule is listed', rowNames(t.list()).length === 3, rowNames(t.list()).join(' | '));

  // Active first, so what is running is at the top; then alphabetical.
  check('active schedules are listed before paused ones',
    rowNames(t.list()).join(',') === 'Alpha,Cee,Bravo', rowNames(t.list()).join(','));

  // ── The result is shared with the Task Agent ───────────────────────────
  check('the fetched schedules are shared with the Task Agent view',
    t.sb.TA.schedules.length === 3);

  // ── Failures are surfaced in the subtitle ──────────────────────────────
  t = build({ schedules: [
    S({ id:'a', name:'Alpha', lastRunStatus:'success', lastRunAt:'2026-08-15T02:00:00Z' }),
    S({ id:'b', name:'Bravo', lastRunStatus:'failed',  lastRunAt:'2026-08-14T02:00:00Z' }),
  ]});
  await t.load();
  check('a failed last run is called out in the subtitle',
    /1 last run failed/.test(t.sub()), t.sub());
  check('the last-run status chip is rendered', /class="chip"/.test(t.list()));

  t = build({ schedules: [S({ lastRunStatus:'success' })] });
  await t.load();
  check('no failures means no failure count', !/failed/.test(t.sub()), t.sub());

  // ── A paused schedule must not advertise a next run ────────────────────
  // The stored nextRunAt survives being disabled, so showing it would state
  // a run that is not going to happen.
  t = build({ schedules: [S({ enabled:false, nextRunAt:'2026-08-16T02:00:00Z' })] });
  await t.load();
  check('a paused schedule does not show a next run time',
    !/AT:2026-08-16/.test(t.list()), t.list());
  check('it says paused instead', /paused/.test(t.list()));

  t = build({ schedules: [S({ enabled:true, nextRunAt:'2026-08-16T02:00:00Z' })] });
  await t.load();
  check('an active schedule does show its next run',
    /AT:2026-08-16/.test(t.list()));

  t = build({ schedules: [S({ enabled:true })] });
  await t.load();
  check('an active schedule with no next run shows a dash, not "paused"',
    !/paused/.test(t.list()));

  // ── Never run ──────────────────────────────────────────────────────────
  t = build({ schedules: [S({})] });
  await t.load();
  check('a schedule that has never run says so', /Never run/.test(t.list()));

  // ── Chained triggers ───────────────────────────────────────────────────
  t = build({ schedules: [S({ chainAfter:'other', cron:null })] });
  await t.load();
  check('a chained schedule shows as chained rather than a blank cron',
    /Chained/.test(t.list()) && !/badge-blue/.test(t.list()), t.list());

  t = build({ schedules: [S({ humanReadable:'daily at 02:00' })] });
  await t.load();
  check('a cron schedule shows its readable form', /daily at 02:00/.test(t.list()));

  // ── Empty ──────────────────────────────────────────────────────────────
  t = build({ schedules: [] });
  await t.load();
  check('no schedules says so in the subtitle', /No schedules yet/.test(t.sub()), t.sub());
  check('and points at where to make one', /Task Agent/.test(t.list()), t.list());
  check('no empty table is rendered', !/<table/.test(t.list()));

  // ── The scheduler being unreachable ────────────────────────────────────
  // The home screen paints from localStorage; one failed network call must
  // not blank it or throw.
  t = build({ error: 'Unauthorized: token expired' });
  let threw = null;
  try { await t.load(); } catch (e) { threw = e; }
  check('a failed fetch does not throw', threw === null, threw && threw.message);
  check('the failure is explained, not silent',
    /Could not reach the scheduler/.test(t.list()), t.list());
  check('the underlying reason is shown', /token expired/.test(t.list()));
  check('a retry is offered', /retry/.test(t.list()));
  check('the subtitle says it could not load', /Could not load/.test(t.sub()), t.sub());

  // ── Escaping ───────────────────────────────────────────────────────────
  t = build({ schedules: [S({ name:'<img src=x onerror=alert(1)>' })] });
  await t.load();
  check('a schedule name containing markup is escaped', !/<img/.test(t.list()));

  t = build({ schedules: [S({ humanReadable:'<b>evil</b>' })] });
  await t.load();
  check('a trigger label containing markup is escaped', !/<b>evil/.test(t.list()));

  t = build({ error: '<script>alert(1)</script>' });
  await t.load();
  check('an error message containing markup is escaped', !/<script>alert/.test(t.list()));

  t = build({ schedules: [S({ name:'' })] });
  await t.load();
  check('a nameless schedule is labelled rather than blank',
    /\(unnamed\)/.test(t.list()));

  // ── Concurrent loads ───────────────────────────────────────────────────
  // The Refresh button is clickable while a load is in flight.
  t = build({ schedules: [S({})] });
  const both = Promise.all([t.load(), t.load()]);
  await both;
  check('a second load while one is in flight is ignored',
    t.calls.length === 1, String(t.calls.length));
  // …but a later load still works, so the guard cannot wedge it shut.
  await t.load();
  check('loading again afterwards still works', t.calls.length === 2, String(t.calls.length));

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})();
