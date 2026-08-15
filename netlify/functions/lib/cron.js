// netlify/functions/lib/cron.js
//
// Shared cron next-run calculator for scheduler.js and scheduled-runner.js
// (previously two hand-kept copies that had drifted).
//
// Supports standard 5-field expressions with numeric values, '*', '*/n'
// steps, ranges (1-5, with optional /step), comma lists (9,17) and
// day/month names (mon-fri, jan). Dependency-free.
//
// If the cron string uses syntax we can't parse, parseCronField throws and
// computeNextRun returns null — callers store a null nextRunAt rather than
// silently treating the field as a wildcard (which used to make
// "0 9 * * 1-5" fire every day and "0 9,17 * * *" fire every hour).

const CRON_NAMES = {
  sun:0, mon:1, tue:2, wed:3, thu:4, fri:5, sat:6,
  jan:1, feb:2, mar:3, apr:4, may:5, jun:6,
  jul:7, aug:8, sep:9, oct:10, nov:11, dec:12,
};

function parseCronField(s, min, max) {
  if (s === '*') return null;                                      // wildcard
  const values = new Set();
  for (const part of s.split(',')) {
    // [name|number][-name|number][/step]  or  */step
    const m = /^(\*|[a-z]{3}|\d+)(?:-([a-z]{3}|\d+))?(?:\/(\d+))?$/i.exec(part);
    if (!m) throw new Error(`Unsupported cron field: ${s}`);
    const resolve = (tok) => {
      if (/^\d+$/.test(tok)) return parseInt(tok, 10);
      const v = CRON_NAMES[tok.toLowerCase()];
      if (v === undefined) throw new Error(`Unknown name in cron field: ${tok}`);
      return v;
    };
    const step = m[3] ? parseInt(m[3], 10) : 1;
    if (!step) throw new Error(`Invalid step in cron field: ${s}`);
    let lo, hi;
    if (m[1] === '*')      { lo = min; hi = max; }
    else if (m[2] != null) { lo = resolve(m[1]); hi = resolve(m[2]); }
    else if (m[3])         { lo = resolve(m[1]); hi = max; }         // n/step
    else                   { lo = hi = resolve(m[1]); }
    if (lo > hi || lo < min || hi > max) {
      // Cron allows 7 for Sunday in day-of-week; normalise it.
      if (max === 6 && hi === 7) { values.add(0); hi = 6; }
      else throw new Error(`Out-of-range cron field: ${s}`);
    }
    for (let v = lo; v <= hi; v += step) values.add(v);
  }
  return [...values];
}

// ── Timezones ─────────────────────────────────────────────────────────────
// A schedule stores the IANA zone the user picked, but nothing used to read
// it: the cron was matched against the server's own clock, which on Netlify
// is UTC. So "daily at 02:00, Europe/London" fired at 02:00 UTC — 03:00 in
// London for the seven months of the year that London is on BST. Every
// schedule was an hour out through British Summer Time, and the timezone
// dropdown was decoration.
//
// Done without a dependency, using Intl. Two conversions are needed:
// UTC instant → wall clock in a zone (to test the cron against), and wall
// clock in a zone → UTC instant (to store as nextRunAt).

// The wall-clock fields `date` shows in `tz`.
function zonedParts(date, tz) {
  const f = new Intl.DateTimeFormat('en-US', {
    timeZone: tz, hour12: false,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
  });
  const p = {};
  for (const part of f.formatToParts(date)) p[part.type] = part.value;
  return {
    year: +p.year, month: +p.month, day: +p.day,
    // Intl can emit hour 24 for midnight under hour12:false.
    hour: (+p.hour) % 24, minute: +p.minute, second: +p.second,
  };
}

// How far ahead of UTC `tz` is at `date`, in milliseconds.
function zoneOffsetMs(date, tz) {
  const p = zonedParts(date, tz);
  return Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second) - date.getTime();
}

// A wall-clock reading in `tz` → the UTC instant it refers to.
//
// The offset has to be sampled at the answer, not the guess, or a time within
// an hour of a DST boundary lands an hour out. Sample at the guess, correct,
// then re-sample and correct again if the offset moved.
function zonedTimeToUtc({ year, month, day, hour, minute }, tz) {
  const guess = Date.UTC(year, month - 1, day, hour, minute, 0);
  const off1 = zoneOffsetMs(new Date(guess), tz);
  let ms = guess - off1;
  const off2 = zoneOffsetMs(new Date(ms), tz);
  if (off2 !== off1) ms = guess - off2;
  return new Date(ms);
}

// Searches forward minute-by-minute up to 60 days. Cheap, avoids DST and
// month-rollover edge cases. Returns an ISO string, or null if the cron is
// malformed or no occurrence exists within 60 days.
//
// `tz` is the IANA zone the cron's fields are expressed in. Omitted (or
// unrecognised) means UTC, which is what every schedule effectively used
// before this argument existed.
function computeNextRun(cronExpr, fromDate, tz) {
  try {
    const parts = String(cronExpr || '').trim().split(/\s+/);
    if (parts.length !== 5) return null;
    const [miStr, hStr, domStr, moStr, dowStr] = parts;
    const mins  = parseCronField(miStr, 0, 59);
    const hours = parseCronField(hStr, 0, 23);
    const doms  = parseCronField(domStr, 1, 31);
    const mos   = parseCronField(moStr, 1, 12);
    const dows  = parseCronField(dowStr, 0, 6);

    const zone = validZone(tz) ? tz : 'UTC';

    // Scan in the zone's wall clock rather than the server's. The cursor is a
    // Date whose UTC fields hold the zone's local reading, so the getUTC*
    // accessors below ARE the wall clock — no Intl call inside the loop,
    // which would turn 86,400 iterations into seconds of work.
    const from  = new Date(fromDate.getTime() + 60_000);   // start next minute
    const p     = zonedParts(from, zone);
    let cursor  = Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, 0);
    const limit = cursor + 60 * 24 * 60 * 60_000;

    for (; cursor < limit; cursor += 60_000) {
      const t = new Date(cursor);
      if (mins  && !mins.includes(t.getUTCMinutes())) continue;
      if (hours && !hours.includes(t.getUTCHours())) continue;
      if (doms  && !doms.includes(t.getUTCDate())) continue;
      if (mos   && !mos.includes(t.getUTCMonth() + 1)) continue;
      if (dows  && !dows.includes(t.getUTCDay())) continue;
      // Match found in wall-clock terms — turn it back into a real instant.
      return zonedTimeToUtc({
        year: t.getUTCFullYear(), month: t.getUTCMonth() + 1, day: t.getUTCDate(),
        hour: t.getUTCHours(), minute: t.getUTCMinutes(),
      }, zone).toISOString();
    }
    return null;
  } catch { return null; }
}

// An unknown zone name would make every Intl call throw. Fall back to UTC
// rather than taking the whole scheduler down over one bad row.
function validZone(tz) {
  if (!tz || typeof tz !== 'string') return false;
  try { new Intl.DateTimeFormat('en-US', { timeZone: tz }); return true; }
  catch { return false; }
}

module.exports = { computeNextRun, parseCronField, zonedTimeToUtc, zoneOffsetMs, validZone };
