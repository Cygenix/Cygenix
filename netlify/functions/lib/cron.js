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

// Searches forward minute-by-minute up to 60 days. Cheap, avoids DST and
// month-rollover edge cases. Returns an ISO string, or null if the cron is
// malformed or no occurrence exists within 60 days.
function computeNextRun(cronExpr, fromDate) {
  try {
    const parts = String(cronExpr || '').trim().split(/\s+/);
    if (parts.length !== 5) return null;
    const [miStr, hStr, domStr, moStr, dowStr] = parts;
    const mins  = parseCronField(miStr, 0, 59);
    const hours = parseCronField(hStr, 0, 23);
    const doms  = parseCronField(domStr, 1, 31);
    const mos   = parseCronField(moStr, 1, 12);
    const dows  = parseCronField(dowStr, 0, 6);

    const start = new Date(fromDate.getTime() + 60_000); // start from next minute
    start.setSeconds(0, 0);
    const limit = new Date(start.getTime() + 60 * 24 * 60 * 60_000);
    for (let t = start; t < limit; t = new Date(t.getTime() + 60_000)) {
      if (mins  && !mins.includes(t.getMinutes())) continue;
      if (hours && !hours.includes(t.getHours())) continue;
      if (doms  && !doms.includes(t.getDate())) continue;
      if (mos   && !mos.includes(t.getMonth() + 1)) continue;
      if (dows  && !dows.includes(t.getDay())) continue;
      return t.toISOString();
    }
    return null;
  } catch { return null; }
}

module.exports = { computeNextRun, parseCronField };
