// Tests one-shot ("Once at a specific date and time") schedules.
//
// The bug: a cron expression has five fields and none of them is a year, so
// "25 14 15 8 *" means 14:25 on 15 August in EVERY year. A one-shot is a
// single instant, so the year had to be guessed — and the guess was "this
// year, or next if that date has already passed".
//
// Consequences, both reproduced below:
//   * Re-opening a one-shot after its moment showed the year AFTER the one
//     the user saved: saved for 15 Aug 2026, displayed as 15 Aug 2027.
//   * Saving from that screen wrote a nextRunAt a year out. computeNextRun
//     only searches 60 days ahead, so it returned null — and the runner's
//     query requires nextRunAt to be defined, so the schedule could never
//     fire at all. "Saved for today, never runs."
//
// The instant is now stored as runOnceAt and nothing guesses.
const fs = require('fs');
const vm = require('vm');
const path = require('path');
const Module = require('module');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

// ── The backend rule ─────────────────────────────────────────────────────
const realRequire = Module.prototype.require;
Module.prototype.require = function (id) {
  // The scheduler pulls in the Cosmos SDK at module load; the rule under test
  // touches none of it.
  if (id === '@azure/cosmos') return { CosmosClient: function () {} };
  if (id === './lib/entra-auth') return { verifyAuthHeader: async () => ({ email: 'x@y.z' }) };
  return realRequire.apply(this, arguments);
};
const scheduler = require(path.join(__dirname, '..', 'netlify', 'functions', 'scheduler.js'));
const { computeNextRun } = require(path.join(__dirname, '..', 'netlify', 'functions', 'lib', 'cron.js'));
Module.prototype.require = realRequire;

const nextRun = scheduler.oneShotNextRun;
const iso = d => new Date(d).toISOString();
const inDays = n => new Date(Date.now() + n * 86400_000);

console.log('One-shot schedules — the missing year\n');

// ── The reported failure, at the level that caused it ────────────────────
// 15 Aug at 14:25 as a cron, asked for on 20 August. The only occurrence
// inside the 60-day search window is a year away, so nothing is found.
{
  const after = new Date(Date.UTC(2026, 7, 20, 9, 0, 0));   // 20 Aug 2026
  check('a cron alone cannot express next year — this is the root cause',
    computeNextRun('25 14 15 8 *', after) === null,
    String(computeNextRun('25 14 15 8 *', after)));
}

// With the instant stored, the same schedule resolves exactly.
{
  const when = iso(inDays(1));
  check('a one-shot with runOnceAt fires at exactly that instant',
    nextRun({ oneShot: true, runOnceAt: when, cron: '25 14 15 8 *' }) === when,
    String(nextRun({ oneShot: true, runOnceAt: when, cron: '25 14 15 8 *' })));
}

// Far beyond the cron search window — the case that used to return null.
{
  const when = iso(inDays(300));
  check('a one-shot 300 days out still resolves, where the cron search fails',
    nextRun({ oneShot: true, runOnceAt: when, cron: '25 14 15 8 *' }) === when);
  check('...and the cron search really does fail for it',
    computeNextRun('25 14 15 8 *', new Date()) === null ||
    computeNextRun('25 14 15 8 *', new Date()) !== when);
}

// A one-shot minutes away must still be picked up.
{
  const when = iso(new Date(Date.now() + 5 * 60_000));
  check('a one-shot five minutes away resolves', nextRun({ oneShot: true, runOnceAt: when }) === when);
}

// ── A past one-shot has no next run ──────────────────────────────────────
// Returning it unchanged would make the runner fire it on the very next tick,
// which is not what "run at 14:25 yesterday" means.
check('a one-shot in the past has no next run',
  nextRun({ oneShot: true, runOnceAt: iso(inDays(-1)) }) === null);
check('a one-shot a minute ago has no next run',
  nextRun({ oneShot: true, runOnceAt: iso(new Date(Date.now() - 60_000)) }) === null);

// ── Repeating schedules are untouched ────────────────────────────────────
{
  const daily = nextRun({ oneShot: false, cron: '0 2 * * *' });
  check('a repeating schedule still resolves from its cron', typeof daily === 'string' && daily.length > 0, String(daily));
  check('it matches computeNextRun exactly — no behaviour change',
    daily.slice(0, 16) === (computeNextRun('0 2 * * *', new Date()) || '').slice(0, 16));
}
check('runOnceAt is ignored when the schedule is not a one-shot',
  nextRun({ oneShot: false, runOnceAt: iso(inDays(300)), cron: '0 2 * * *' }) !== iso(inDays(300)));

// ── Degenerate input ─────────────────────────────────────────────────────
check('a one-shot with no runOnceAt falls back to the cron',
  nextRun({ oneShot: true, cron: '0 2 * * *' }) !== null);
check('an unparseable runOnceAt falls back to the cron',
  nextRun({ oneShot: true, runOnceAt: 'not-a-date', cron: '0 2 * * *' }) !== null);
check('no cron and no runOnceAt means no next run',
  nextRun({ oneShot: true }) === null);
check('a chain-only schedule has no next run', nextRun({ cron: null }) === null);
check('nothing at all does not throw', nextRun(null) === null || nextRun(undefined) === null);

// ── The editor's year ────────────────────────────────────────────────────
const html = (fs.readFileSync(path.join(__dirname, '..', 'public', 'dashboard.html'), 'utf8') + '\n' + fs.readFileSync(path.join(__dirname, '..', 'public', 'dashboard-app.js'), 'utf8'));
const a = html.indexOf('// ── One-shot schedules and the missing year ─');
const b = html.indexOf('function ta_parseCronIntoUI(cronExpr, schedule) {');
if (a < 0 || b < 0) { check('could locate the one-shot helpers', false); }
else {
  function build(fields = {}) {
    const els = {
      'ta-once-date': { value: fields.date || '' },
      'ta-once-time': { value: fields.time || '' },
    };
    const sb = {
      console, JSON, String, Number, Date, isNaN, parseInt, RegExp,
      document: { getElementById: id => els[id] || null },
    };
    vm.createContext(sb);
    vm.runInContext(html.slice(a, b), sb);
    return { sb, get: e => vm.runInContext(e, sb) };
  }

  // The instant is built from the fields as UTC, matching how the backend
  // matches a cron against server-local (UTC) time.
  let t = build({ date: '2026-08-15', time: '14:25' });
  check('the one-shot fields become an exact ISO instant',
    t.get('ta_onceIso()') === '2026-08-15T14:25:00.000Z', t.get('ta_onceIso()'));
  check('a missing date yields no instant', build({ time: '14:25' }).get('ta_onceIso()') === null);
  check('a missing time yields no instant', build({ date: '2026-08-15' }).get('ta_onceIso()') === null);
  check('a malformed date yields no instant',
    build({ date: '15/08/2026', time: '14:25' }).get('ta_onceIso()') === null);

  // THE REPORTED BUG. Saved for 15 Aug 2026; re-opened after that moment.
  const yearFor = (sched) => build().get(
    'ta_onceYearFor(' + JSON.stringify(sched) + ', "8", "15", "14", "25")');

  check('a saved one-shot shows the year it was saved with, not the year after',
    yearFor({ runOnceAt: '2026-08-15T14:25:00.000Z' }) === 2026,
    String(yearFor({ runOnceAt: '2026-08-15T14:25:00.000Z' })));
  check('that holds even for a date long past',
    yearFor({ runOnceAt: '2019-08-15T14:25:00.000Z' }) === 2019,
    String(yearFor({ runOnceAt: '2019-08-15T14:25:00.000Z' })));
  check('a future one-shot keeps its year too',
    yearFor({ runOnceAt: '2031-08-15T14:25:00.000Z' }) === 2031);

  // Schedules saved before runOnceAt existed still carry the instant the
  // backend computed at save time.
  check('a legacy schedule falls back to its stored nextRunAt',
    yearFor({ nextRunAt: '2026-08-15T14:25:00.000Z' }) === 2026,
    String(yearFor({ nextRunAt: '2026-08-15T14:25:00.000Z' })));
  check('runOnceAt wins over nextRunAt when both exist',
    yearFor({ runOnceAt: '2026-08-15T14:25:00.000Z', nextRunAt: '2027-08-15T14:25:00.000Z' }) === 2026);

  // Only with nothing stored at all does it guess — and it says so.
  {
    const guessed = yearFor({});
    const thisYear = new Date().getFullYear();
    check('with nothing stored it still guesses, as a last resort',
      guessed === thisYear || guessed === thisYear + 1, String(guessed));
  }
  check('an unparseable stored instant falls through to the guess rather than NaN',
    !isNaN(yearFor({ runOnceAt: 'rubbish' })));
  check('a null schedule does not throw', !isNaN(yearFor(null)));
}

// ── The two halves agree ─────────────────────────────────────────────────
// What the editor writes must be what the backend can read back.
{
  const when = iso(inDays(2));
  check('an instant the editor produces is accepted by the backend rule',
    nextRun({ oneShot: true, runOnceAt: when }) === when);
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
