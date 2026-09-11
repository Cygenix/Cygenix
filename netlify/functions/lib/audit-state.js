// netlify/functions/lib/audit-state.js
//
// Capture state — whether the audit log is Recording, Paused or Off — and
// the settings that sit under it. Pure: state in, decision out, with the
// clock passed in rather than read, so "the pause expires after four hours"
// is a test and not a wait.
//
// ── Why a pause exists at all ─────────────────────────────────────────────
//
// The honest argument against it is that an audit log you can switch off is
// not an audit log. That argument is right about the categories that matter
// and wrong about the rest: an operator running a ten-thousand-row bulk
// import does not want ten thousand mapping events, and if the product does
// not give them a pause they will ask for the category to be removed
// permanently, or stop reading the log, which is worse.
//
// So the pause is real but bounded, and the boundary is the point:
//
//   * Four categories cannot be paused, turned off, or disabled in settings
//     — security, access, prod and the log's own events. A pause taken to
//     quieten an import still records a sign-in, a role change, a
//     Production write and the fact of the pause itself. The window an
//     attacker would want is precisely the window that stays lit.
//   * A pause must carry a reason and must have an end. Off must carry a
//     reason and be typed out in full, because it has no end.
//   * Every state change is itself an event in the always-on `audit`
//     category, so the log records its own blindness. A gap in the timeline
//     is visible AS a gap, with who asked for it and why.
//
// ── Why expiry is lazy ────────────────────────────────────────────────────
//
// A pause resumes by itself. The obvious implementation is a scheduled
// function that wakes up and flips the flag, and it is the wrong one: it
// adds a moving part that can fail silently, and between the expiry and the
// next tick the state is a lie. Instead the stored state is a claim and
// resolveState() is the only thing allowed to interpret it — called at
// write time and at read time both, so there is no instant at which the log
// believes it is paused after the pause has run out. No scheduler, no
// window, nothing to monitor.

'use strict';

const schema = require('./audit-schema');

const STATES = ['recording', 'paused', 'off'];

// Presets the UI offers, and the ceilings the API enforces. The UI offering
// a value is not what makes it legal — validateTransition is.
const PAUSE_PRESETS_MIN = [30, 60, 120, 240];
const PAUSE_MAX_OPTIONS_MIN = [240, 1440];          // 4 hours or 24 hours
const RETENTION_OPTIONS_DAYS = [90, 365, 2555];     // 90 days, 1 year, 7 years

const DEFAULT_SETTINGS = {
  categories: schema.CATEGORY_KEYS.reduce((a, k) => { a[k] = true; return a; }, {}),
  retentionDays: 365,
  pauseMaxMinutes: 240,
  storeDiffs: true,        // keep before/after values on change events
  storeIp: true,           // keep the caller's IP in context
  recordAssistant: true,   // record acts Ask Cygenix takes on someone's behalf
};

const DEFAULT_STATE = {
  state: 'recording',
  reason: null,
  changedBy: null,
  changedAt: null,
  pausedUntil: null,
};

function defaultConfig() {
  return {
    ...DEFAULT_STATE,
    settings: { ...DEFAULT_SETTINGS, categories: { ...DEFAULT_SETTINGS.categories } },
  };
}

// ── Settings ──────────────────────────────────────────────────────────────
//
// Normalise never rejects; it coerces to something safe and, crucially,
// forces the always-on categories back to true. Stored state that says
// `security: false` — written by an older version, a bad migration, or a
// hand-edited blob — must not be able to switch security auditing off just
// by existing. The lock is re-applied on every read, not only on write.
function normaliseSettings(raw) {
  const r = raw || {};
  const cats = {};
  for (const k of schema.CATEGORY_KEYS) {
    cats[k] = schema.isAlwaysOn(k) ? true
      : (r.categories && r.categories[k] === false ? false : true);
  }
  const retention = RETENTION_OPTIONS_DAYS.indexOf(Number(r.retentionDays)) !== -1
    ? Number(r.retentionDays) : DEFAULT_SETTINGS.retentionDays;
  const pauseMax = PAUSE_MAX_OPTIONS_MIN.indexOf(Number(r.pauseMaxMinutes)) !== -1
    ? Number(r.pauseMaxMinutes) : DEFAULT_SETTINGS.pauseMaxMinutes;
  return {
    categories: cats,
    retentionDays: retention,
    pauseMaxMinutes: pauseMax,
    storeDiffs: r.storeDiffs !== false,
    storeIp: r.storeIp !== false,
    recordAssistant: r.recordAssistant !== false,
  };
}

// Rejects rather than coerces, because a request that tries to disable a
// locked category is not a typo to be tidied up — it is the one thing the
// acceptance criteria say must fail, and it must fail at the API and not
// only in the UI.
function validateSettings(patch, current) {
  const p = patch || {};
  if (p.categories) {
    if (typeof p.categories !== 'object' || Array.isArray(p.categories)) {
      return { ok: false, reason: 'categories must be an object' };
    }
    for (const k of Object.keys(p.categories)) {
      if (schema.CATEGORY_KEYS.indexOf(k) === -1) {
        return { ok: false, reason: 'unknown category: ' + k };
      }
      if (schema.isAlwaysOn(k) && p.categories[k] === false) {
        return { ok: false, reason: 'the ' + k + ' category cannot be disabled' };
      }
    }
  }
  if (p.retentionDays !== undefined && RETENTION_OPTIONS_DAYS.indexOf(Number(p.retentionDays)) === -1) {
    return { ok: false, reason: 'retentionDays must be one of ' + RETENTION_OPTIONS_DAYS.join(', ') };
  }
  if (p.pauseMaxMinutes !== undefined && PAUSE_MAX_OPTIONS_MIN.indexOf(Number(p.pauseMaxMinutes)) === -1) {
    return { ok: false, reason: 'pauseMaxMinutes must be one of ' + PAUSE_MAX_OPTIONS_MIN.join(', ') };
  }
  const base = normaliseSettings(current);
  const merged = normaliseSettings({
    ...base, ...p,
    categories: { ...base.categories, ...(p.categories || {}) },
  });
  return { ok: true, settings: merged, changes: schema.diff(base, merged) };
}

// ── Resolving the stored state against the clock ──────────────────────────
//
// The stored record is a claim. This is the only interpreter of it, and it
// is called at write time AND read time so the two can never disagree.
// `expired` is returned rather than acted on, because persisting the
// resolution and writing the `audit.resume` event are I/O and this module
// does none.
function resolveState(config, now) {
  const c = config || defaultConfig();
  const t = now === undefined ? Date.now() : Number(now);
  const stored = STATES.indexOf(c.state) !== -1 ? c.state : 'recording';

  if (stored === 'paused') {
    const until = c.pausedUntil ? Date.parse(c.pausedUntil) : NaN;
    // A paused state with no end time is a bug upstream, and the safe
    // reading of a bug in an audit log is "record everything".
    if (!until || Number.isNaN(until) || t >= until) {
      return {
        state: 'recording', expired: true, storedState: 'paused',
        pausedUntil: c.pausedUntil || null, reason: c.reason || null,
        changedBy: c.changedBy || null, changedAt: c.changedAt || null,
        settings: normaliseSettings(c.settings),
      };
    }
    return {
      state: 'paused', expired: false, storedState: 'paused',
      pausedUntil: c.pausedUntil, msRemaining: until - t,
      reason: c.reason || null, changedBy: c.changedBy || null, changedAt: c.changedAt || null,
      settings: normaliseSettings(c.settings),
    };
  }

  // Off has no timer by design — it stays off until a person turns it on.
  return {
    state: stored, expired: false, storedState: stored, pausedUntil: null,
    reason: c.reason || null, changedBy: c.changedBy || null, changedAt: c.changedAt || null,
    settings: normaliseSettings(c.settings),
  };
}

// ── State transitions ─────────────────────────────────────────────────────

const TRANSITION_ACTIONS = {
  recording: 'audit.resume',    // from paused
  paused:    'audit.pause',
  off:       'audit.disable',
};

function validateTransition(req, config, now) {
  const r = req || {};
  const cfg = config || defaultConfig();
  const t = now === undefined ? Date.now() : Number(now);
  const to = String(r.state || '').toLowerCase();
  if (STATES.indexOf(to) === -1) {
    return { ok: false, reason: 'state must be one of ' + STATES.join(', ') };
  }
  const settings = normaliseSettings(cfg.settings);
  const from = resolveState(cfg, t).state;

  if (to === 'recording') {
    if (from === 'recording') return { ok: false, reason: 'capture is already recording' };
    return {
      ok: true,
      action: from === 'off' ? 'audit.enable' : 'audit.resume',
      next: { state: 'recording', reason: r.reason ? String(r.reason).slice(0, 500) : null,
              pausedUntil: null },
    };
  }

  // A reason is not paperwork. The gap row in the timeline shows it, and a
  // gap nobody can explain six months later is the thing that turns an
  // audit finding into an incident.
  const reason = String(r.reason || '').trim();
  if (reason.length < 4) {
    return { ok: false, reason: 'a reason of at least 4 characters is required' };
  }

  if (to === 'paused') {
    const mins = Number(r.pauseMinutes);
    if (!mins || !Number.isFinite(mins) || mins <= 0) {
      return { ok: false, reason: 'pauseMinutes is required and must be positive' };
    }
    if (mins > settings.pauseMaxMinutes) {
      return { ok: false, reason: 'a pause may not exceed ' + settings.pauseMaxMinutes + ' minutes' };
    }
    return {
      ok: true, action: 'audit.pause',
      next: { state: 'paused', reason: reason.slice(0, 500),
              pausedUntil: new Date(t + mins * 60000).toISOString() },
      pauseMinutes: mins,
    };
  }

  // Off is the destructive one, so it is the one that has to be typed. A
  // segmented control is too easy to hit by accident for a change that has
  // no timer to undo it.
  if (String(r.confirm || '').trim().toUpperCase() !== 'OFF') {
    return { ok: false, reason: 'type OFF to confirm turning capture off' };
  }
  return {
    ok: true, action: 'audit.disable',
    next: { state: 'off', reason: reason.slice(0, 500), pausedUntil: null },
  };
}

// ── The drop decision ─────────────────────────────────────────────────────
//
// The server makes this call, never the browser. A client that is told
// "capture is paused" and skips the POST has made a security decision on
// the server's behalf, and a client that lies about its category has made
// it badly. Everything is posted; this function decides what survives.
function shouldRecord(category, resolved, settings) {
  const cat = schema.CATEGORY_KEYS.indexOf(category) !== -1 ? category : 'settings';
  if (schema.isAlwaysOn(cat)) return { record: true, reason: 'always-on category' };

  const state = (resolved && resolved.state) || 'recording';
  if (state === 'paused') return { record: false, reason: 'capture paused' };
  if (state === 'off')    return { record: false, reason: 'capture off' };

  const s = normaliseSettings(settings || (resolved && resolved.settings));
  if (s.categories[cat] === false) {
    return { record: false, reason: 'category ' + cat + ' is disabled' };
  }
  return { record: true, reason: 'recording' };
}

// ── Gap windows for the timeline ──────────────────────────────────────────
//
// The events are the source of truth for this, not the current state: the
// log has to be able to show a pause that happened in March when it is
// September and capture has been recording ever since. Entries may arrive
// newest-first (that is how readAudit returns them), so they are sorted
// here rather than assumed.
//
// An unclosed window is returned with open:true and no `to`. That covers
// both the honest case — capture is off right now — and the case worth
// noticing, a pause whose stored end time passed without an audit.resume
// ever being written, which would mean nothing has been recorded since and
// nobody has looked.
function gapWindows(entries, now) {
  const t = now === undefined ? Date.now() : Number(now);
  const rows = (entries || [])
    .filter(e => e && /^audit\.(pause|resume|disable|enable)$/.test(e.action))
    .slice()
    .sort((a, b) => Date.parse(a.occurredAt || 0) - Date.parse(b.occurredAt || 0));

  const out = [];
  let open = null;
  for (const e of rows) {
    const verb = e.action.split('.')[1];
    if (verb === 'pause' || verb === 'disable') {
      if (open) { open.to = e.occurredAt; open.open = false; out.push(open); }
      open = {
        kind: verb === 'pause' ? 'paused' : 'off',
        from: e.occurredAt,
        to: null,
        open: true,
        reason: e.reason || (e.detail && e.detail.reason) || null,
        by: e.actorEmail || null,
        expectedTo: (e.detail && e.detail.pausedUntil) || null,
      };
    } else if (open) {
      open.to = e.occurredAt;
      open.open = false;
      open.endedBy = e.actorType === 'system' ? 'system' : (e.actorEmail || null);
      out.push(open);
      open = null;
    }
  }
  if (open) {
    // Still open. If it was a pause whose end time has passed with no
    // resume written, say so — that is a log that stopped recording and
    // never started again.
    open.overdue = !!(open.expectedTo && Date.parse(open.expectedTo) < t);
    out.push(open);
  }
  return out;
}

module.exports = {
  STATES, PAUSE_PRESETS_MIN, PAUSE_MAX_OPTIONS_MIN, RETENTION_OPTIONS_DAYS,
  DEFAULT_SETTINGS, DEFAULT_STATE, defaultConfig,
  normaliseSettings, validateSettings,
  resolveState, validateTransition, TRANSITION_ACTIONS,
  shouldRecord, gapWindows,
};
