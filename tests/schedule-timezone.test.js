// Tests that a schedule fires in the timezone the user picked.
//
// The Task Agent has always shown a Timezone dropdown, stored the choice, and
// then ignored it: the cron was matched against the server's own clock, which
// on Netlify is UTC. So "daily at 02:00, Europe/London" fired at 02:00 UTC —
// 03:00 in London for the seven months of the year London is on BST. Every
// schedule was an hour out through British Summer Time and the dropdown was
// decoration.
//
// The reported symptom was a one-shot set for 14:50 Europe/London that did not
// run at 14:50; it would have run at 15:50 local.
const fs = require('fs');
const vm = require('vm');
const path = require('path');
const { computeNextRun, zonedTimeToUtc, zoneOffsetMs, validZone } =
  require(path.join(__dirname, '..', 'netlify', 'functions', 'lib', 'cron.js'));

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

const at = s => new Date(s);
const HOUR = 3600_000;

console.log('Schedules — timezones\n');

// ── The reported case ────────────────────────────────────────────────────
// 15 August 2026: London is on BST, UTC+1.
{
  const from = at('2026-08-15T10:00:00Z');
  check('14:50 Europe/London in summer is 13:50Z, not 14:50Z',
    computeNextRun('50 14 * * *', from, 'Europe/London') === '2026-08-15T13:50:00.000Z',
    String(computeNextRun('50 14 * * *', from, 'Europe/London')));
  check('the old behaviour really was an hour out',
    computeNextRun('50 14 * * *', from) === '2026-08-15T14:50:00.000Z');
}

// In winter London is GMT, so the same cron resolves to the same clock time.
check('14:50 Europe/London in winter is 14:50Z',
  computeNextRun('50 14 * * *', at('2026-12-15T10:00:00Z'), 'Europe/London')
    === '2026-12-15T14:50:00.000Z');

// ── Other zones ──────────────────────────────────────────────────────────
check('02:00 America/New_York in summer is 06:00Z (EDT, UTC-4)',
  computeNextRun('0 2 * * *', at('2026-08-15T10:00:00Z'), 'America/New_York')
    === '2026-08-16T06:00:00.000Z',
  String(computeNextRun('0 2 * * *', at('2026-08-15T10:00:00Z'), 'America/New_York')));
check('02:00 America/New_York in winter is 07:00Z (EST, UTC-5)',
  computeNextRun('0 2 * * *', at('2026-12-15T10:00:00Z'), 'America/New_York')
    === '2026-12-16T07:00:00.000Z',
  String(computeNextRun('0 2 * * *', at('2026-12-15T10:00:00Z'), 'America/New_York')));
check('a zone ahead of UTC resolves backwards (Asia/Tokyo, UTC+9)',
  computeNextRun('0 9 * * *', at('2026-08-15T00:00:00Z'), 'Asia/Tokyo')
    === '2026-08-16T00:00:00.000Z',
  String(computeNextRun('0 9 * * *', at('2026-08-15T00:00:00Z'), 'Asia/Tokyo')));
// India is UTC+5:30 — proves the offset is not assumed to be whole hours.
check('a half-hour zone is handled (Asia/Kolkata, UTC+5:30)',
  computeNextRun('0 9 * * *', at('2026-08-15T00:00:00Z'), 'Asia/Kolkata')
    === '2026-08-15T03:30:00.000Z',
  String(computeNextRun('0 9 * * *', at('2026-08-15T00:00:00Z'), 'Asia/Kolkata')));

// ── Nothing changes for schedules with no zone ───────────────────────────
// Every existing row without a timezone must keep firing exactly as before.
{
  const from = at('2026-08-15T10:00:00Z');
  const legacy = computeNextRun('0 2 * * *', from);
  check('an absent timezone behaves as UTC, exactly as before',
    legacy === computeNextRun('0 2 * * *', from, 'UTC'), String(legacy));
  check('an explicitly null timezone is the same',
    computeNextRun('0 2 * * *', from, null) === legacy);
  check('an empty timezone string is the same',
    computeNextRun('0 2 * * *', from, '') === legacy);
}

// A bad zone name must not take the scheduler down over one bad row.
check('an unknown timezone falls back to UTC rather than throwing',
  computeNextRun('0 2 * * *', at('2026-08-15T10:00:00Z'), 'Mars/Olympus_Mons')
    === computeNextRun('0 2 * * *', at('2026-08-15T10:00:00Z'), 'UTC'));
check('validZone accepts a real zone and rejects nonsense',
  validZone('Europe/London') === true && validZone('Nowhere/Nothing') === false &&
  validZone('') === false && validZone(null) === false);

// ── DST boundaries ───────────────────────────────────────────────────────
// London springs forward 2026-03-29 at 01:00 → 02:00, so 01:30 never happens.
{
  const r = computeNextRun('30 1 29 3 *', at('2026-03-28T00:00:00Z'), 'Europe/London');
  check('a time inside the spring-forward gap still resolves to a real instant',
    typeof r === 'string' && !isNaN(new Date(r).getTime()), String(r));
}
// London falls back 2026-10-25 at 02:00 → 01:00, so 01:30 happens twice.
{
  const r = computeNextRun('30 1 25 10 *', at('2026-10-24T00:00:00Z'), 'Europe/London');
  check('an ambiguous time resolves to one of its two instants',
    r === '2026-10-25T00:30:00.000Z' || r === '2026-10-25T01:30:00.000Z', String(r));
}
// A daily cron either side of a transition keeps its LOCAL time, which is the
// whole point of storing a zone rather than a fixed offset.
// London's clocks go back at 02:00 on 2026-10-25, so 09:00 that day is already
// GMT. The run before it — 24 Oct — is still BST.
{
  const before = computeNextRun('0 9 * * *', at('2026-10-23T12:00:00Z'), 'Europe/London');
  const after  = computeNextRun('0 9 * * *', at('2026-10-25T12:00:00Z'), 'Europe/London');
  check('09:00 local on the last BST day is 08:00Z',
    before === '2026-10-24T08:00:00.000Z', String(before));
  check('09:00 local on the first GMT day is 09:00Z',
    after === '2026-10-26T09:00:00.000Z', String(after));
  // The whole point of storing a zone rather than a fixed offset: the local
  // hour holds still and the UTC hour is what moves.
  check('the local hour is preserved, so the UTC hour shifts across the change',
    new Date(before).getUTCHours() === 8 && new Date(after).getUTCHours() === 9);
}

// ── The conversion helper ────────────────────────────────────────────────
check('zonedTimeToUtc converts a summer London reading',
  zonedTimeToUtc({ year:2026, month:8, day:15, hour:14, minute:50 }, 'Europe/London').toISOString()
    === '2026-08-15T13:50:00.000Z');
check('zonedTimeToUtc converts a winter London reading',
  zonedTimeToUtc({ year:2026, month:12, day:15, hour:14, minute:50 }, 'Europe/London').toISOString()
    === '2026-12-15T14:50:00.000Z');
check('zonedTimeToUtc is identity for UTC',
  zonedTimeToUtc({ year:2026, month:8, day:15, hour:14, minute:50 }, 'UTC').toISOString()
    === '2026-08-15T14:50:00.000Z');
check('zoneOffsetMs reports BST as +1h', zoneOffsetMs(at('2026-08-15T12:00:00Z'), 'Europe/London') === HOUR);
check('zoneOffsetMs reports GMT as 0',   zoneOffsetMs(at('2026-12-15T12:00:00Z'), 'Europe/London') === 0);
// Midnight is the case where Intl's hour12:false can emit "24".
check('midnight in a zone does not become hour 24',
  zonedTimeToUtc({ year:2026, month:8, day:15, hour:0, minute:0 }, 'Europe/London').toISOString()
    === '2026-08-14T23:00:00.000Z',
  zonedTimeToUtc({ year:2026, month:8, day:15, hour:0, minute:0 }, 'Europe/London').toISOString());

// ── Performance ──────────────────────────────────────────────────────────
// The scan is 60 days of minutes. Calling Intl inside that loop would take
// seconds per schedule, and the runner ticks every minute across all users.
{
  const t0 = Date.now();
  computeNextRun('0 3 30 2 *', at('2026-08-15T10:00:00Z'), 'Europe/London');  // never matches
  const ms = Date.now() - t0;
  check('a full 60-day scan stays well under a second', ms < 800, ms + 'ms');
}

// ── The editor agrees with the backend ───────────────────────────────────
// ta_onceIso builds the instant a one-shot is stored with. It must produce the
// same answer as the backend's conversion, or a one-shot and the cron beside
// it would mean different moments.
{
  const html = (fs.readFileSync(path.join(__dirname, '..', 'public', 'dashboard.html'), 'utf8') + '\n' + fs.readFileSync(path.join(__dirname, '..', 'public', 'dashboard-app.js'), 'utf8'));
  const a = html.indexOf('// ── One-shot schedules and the missing year ─');
  const b = html.indexOf('function ta_parseCronIntoUI(cronExpr, schedule) {');
  if (a < 0 || b < 0) { check('could locate ta_onceIso', false); }
  else {
    const build = (date, time, tz) => {
      const els = {
        'ta-once-date': { value: date }, 'ta-once-time': { value: time },
        'ta-once-tz':   { value: tz },
      };
      const sb = { console, JSON, String, Number, Date, isNaN, parseInt, RegExp, Intl,
                   document: { getElementById: id => els[id] || null } };
      vm.createContext(sb);
      vm.runInContext(html.slice(a, b), sb);
      return vm.runInContext('ta_onceIso()', sb);
    };

    check('the editor stores 14:50 London as 13:50Z',
      build('2026-08-15', '14:50', 'Europe/London') === '2026-08-15T13:50:00.000Z',
      String(build('2026-08-15', '14:50', 'Europe/London')));
    check('the editor stores winter London unshifted',
      build('2026-12-15', '14:50', 'Europe/London') === '2026-12-15T14:50:00.000Z');
    check('the editor and the backend produce the same instant',
      build('2026-08-15', '14:50', 'Europe/London') ===
      zonedTimeToUtc({ year:2026, month:8, day:15, hour:14, minute:50 }, 'Europe/London').toISOString());
    check('a UTC choice is stored unshifted',
      build('2026-08-15', '14:50', 'UTC') === '2026-08-15T14:50:00.000Z');
    check('an unknown zone in the editor falls back to UTC rather than throwing',
      build('2026-08-15', '14:50', 'Mars/Olympus_Mons') === '2026-08-15T14:50:00.000Z');
    check('a missing date still yields nothing', build('', '14:50', 'Europe/London') === null);
  }
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
