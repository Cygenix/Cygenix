// netlify/functions/scheduled-runner.js
//
// Phase 3 — Auto-trigger for Cygenix Task Agent schedules.
//
// Runs every minute on Netlify's cron infrastructure. Queries Cosmos for
// schedules that are (a) enabled and (b) due (nextRunAt <= now), and for
// each one, calls the same /api/scheduler?action=run-now path that the
// dashboard's ▶ Run button calls. The work itself happens in the existing
// scheduler-run-background.js — this function is just the trigger.
//
// ── How Netlify schedules this ──────────────────────────────────────────
// netlify.toml must contain:
//   [functions."scheduled-runner"]
//     schedule = "* * * * *"
//
// The `schedule` directive (cron, UTC) tells Netlify to invoke this
// function at the specified cadence. No HTTP route — the handler is
// invoked by the Netlify cron infrastructure directly.
//
// ── Why we don't do the work here ───────────────────────────────────────
// Netlify scheduled functions have the same ~26s timeout as standard
// functions. The actual migration runs in scheduler-run-background.js
// (15-minute background-function cap). This function only:
//   1. Decides which schedules are due
//   2. Bumps their nextRunAt forward (so a slow tick doesn't double-fire)
//   3. Creates a queued run record
//   4. Fires the background runner
// Total time per schedule: ~200ms. We can handle dozens of due schedules
// inside one tick before bumping against the function timeout.
//
// ── Why we bump nextRunAt BEFORE running ────────────────────────────────
// If this minute's tick takes 30 seconds and the next minute's tick fires
// while we're still working, we could double-trigger the same schedule.
// By writing the new nextRunAt the moment we decide to fire (before the
// background dispatch), the second tick reads nextRunAt > now and skips.
// Optimistic concurrency: even if two ticks try to fire the same schedule
// simultaneously, Cosmos's _etag preserves correctness — one update wins,
// the other 412s and the loser's nextRunAt re-read picks up the bump.
//
// ── Missed runs are not backfilled ──────────────────────────────────────
// If Netlify cron is down for 6 hours and a 02:00 trigger is missed, we
// fire the NEXT occurrence — not 6 backfilled runs. Catching up is a
// footgun: imagine waking up to 6 conversions having run back-to-back.
// A user wanting "did this miss" visibility can see lastRunAt vs nextRunAt
// in the dashboard.

const { CosmosClient } = require('@azure/cosmos');

// ── Cosmos client (module-scoped — reused across warm invocations) ──────
let _cosmosClient = null;
let _containers = null;
function getContainers() {
  if (_containers) return _containers;
  const conn = process.env.COSMOS_CONNECTION_STRING;
  if (!conn) throw new Error('COSMOS_CONNECTION_STRING env var not set');
  _cosmosClient = new CosmosClient(conn);
  const db = _cosmosClient.database(process.env.COSMOS_DATABASE || 'cygenix');
  _containers = {
    schedules:    db.container('schedules'),
    runs:         db.container('runs'),
    job_versions: db.container('job_versions'),
  };
  return _containers;
}

// ── Local copy of computeNextRun (kept in sync with scheduler.js) ───────
// Searches forward minute-by-minute up to 60 days. Cheap, avoids DST and
// month-rollover edge cases. If the cron is malformed or no occurrence
// found in 60 days, returns null and we skip the schedule.
function computeNextRun(cronExpr, fromDate) {
  try {
    const parts = String(cronExpr || '').trim().split(/\s+/);
    if (parts.length !== 5) return null;
    const [miStr, hStr, domStr, moStr, dowStr] = parts;
    const parseField = (s, min, max) => {
      if (s === '*') return null;
      const m = /^\*\/(\d+)$/.exec(s);
      if (m) {
        const step = parseInt(m[1], 10);
        if (!step) return null;
        const out = [];
        for (let v = min; v <= max; v += step) out.push(v);
        return out;
      }
      if (/^\d+$/.test(s)) {
        const v = parseInt(s, 10);
        if (v < min || v > max) return null;
        return [v];
      }
      return null;
    };
    const mins  = parseField(miStr, 0, 59);
    const hours = parseField(hStr, 0, 23);
    const doms  = parseField(domStr, 1, 31);
    const mos   = parseField(moStr, 1, 12);
    const dows  = parseField(dowStr, 0, 6);
    const start = new Date(fromDate.getTime() + 60_000);
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

// ── Helpers ─────────────────────────────────────────────────────────────
const isHttpUrl    = s => /^https?:\/\//i.test(s || '');
const isMssqlConn  = s => /^mssql:\/\//i.test(s || '');
function newRunId() {
  return 'run_' + Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
}
function nowIso() { return new Date().toISOString(); }

// ── Resolve runner URLs ─────────────────────────────────────────────────
// Two endpoints on the Azure Function App (cygenix-db-api, Flex Consumption,
// UK South, no enforced timeout):
//   migration jobs    → /api/run-migration with {runId, scheduleId, userId}
//   profile-build jobs → /api/profile-build with {role, conn, database,
//                        fingerprint, notifyEmail} from the pinned snapshot
// Routing is driven by schedule.jobType (set by createSchedule from the
// pinned version's snapshot.jobType).
const AZ_BASE = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api';
function getMigrationUrl()    { return AZ_BASE + '/run-migration'; }
function getProfileBuildUrl() { return AZ_BASE + '/profile-build'; }

// ── Fire one schedule ───────────────────────────────────────────────────
// Returns { fired: bool, reason?: string, runId?: string } for logging.
async function fireSchedule(containers, schedule, now) {
  // Defensive: re-check enabled at fire time (the cron loop already
  // filters, but a schedule could be toggled off mid-tick).
  if (!schedule.enabled) {
    return { fired: false, reason: 'schedule disabled' };
  }
  // Validate the schedule still has the pieces it needs.
  if (!schedule.jobVersionId) {
    return { fired: false, reason: 'no pinned jobVersionId' };
  }
  // Job type is denormalised onto the schedule by createSchedule. Older
  // schedules (created before profile-build routing existed) won't have
  // this field — treat absence as 'migration' for backwards compatibility.
  const jobType = schedule.jobType || 'migration';
  const isProfileBuild = jobType === 'profile-build';

  // Migration jobs need a target connection string; profile-build doesn't
  // (it carries its conn in the snapshot).
  if (!isProfileBuild) {
    if (!schedule.tgtConn || (!isMssqlConn(schedule.tgtConn) && !isHttpUrl(schedule.tgtConn))) {
      return { fired: false, reason: 'invalid target connection' };
    }
  }

  // 1. Bump nextRunAt FORWARD before doing anything else — this is our
  //    overlap protection. If another tick fires in parallel, it will
  //    see nextRunAt > now and skip. We use Cosmos's _etag-based
  //    optimistic concurrency: if our replace() fails with a 412
  //    PreconditionFailed, another instance already fired this one and
  //    we bail out without doing anything destructive.
  //
  //    One-shot schedules (created from the "Once at date/time" trigger
  //    mode) carry oneShot:true. We disable them after the first fire so
  //    the cron expression — which would otherwise re-match next year
  //    on the same day — doesn't auto-fire again. The user can re-enable
  //    manually if they want to repeat.
  const nextRunAt = schedule.cron ? computeNextRun(schedule.cron, now) : null;
  const bumped = {
    ...schedule,
    nextRunAt,
    updatedAt: nowIso(),
  };
  if (schedule.oneShot) {
    bumped.enabled   = false;
    bumped.nextRunAt = null;  // belt-and-braces: any cron tick must skip
  }
  try {
    await containers.schedules.item(schedule.id, schedule.userId).replace(bumped, {
      accessCondition: { type: 'IfMatch', condition: schedule._etag },
    });
  } catch (e) {
    if (e.code === 412 || e.statusCode === 412) {
      return { fired: false, reason: 'lost concurrent bump race' };
    }
    throw e;
  }

  // 2. Create the run record in 'queued' state.
  //    triggeredBy: 'cron' distinguishes auto-runs from manual ones in
  //    Run History. Useful for "did this run because I clicked it or
  //    because the schedule fired".
  const runId = newRunId();
  const runDoc = {
    id:           runId,
    scheduleId:   schedule.id,
    userId:       schedule.userId,
    jobId:        schedule.jobId,
    jobVersionId: schedule.jobVersionId,
    jobType,
    triggeredBy:  'cron',
    status:       'queued',
    startedAt:    nowIso(),
    finishedAt:   null,
    rowsAffected: 0,
    errorMessage: null,
    stepResults:  [],
  };
  await containers.runs.items.create(runDoc);

  // 3. Build the dispatch URL + body based on jobType. For profile-build
  //    we read the pinned version's snapshot to extract the conn/role/etc.
  //    For migration we keep the existing fast path — no extra reads.
  let bgUrl, dispatchBody;
  if (isProfileBuild) {
    bgUrl = getProfileBuildUrl();
    let version;
    try {
      const { resource } = await containers.job_versions
        .item(schedule.jobVersionId, schedule.jobId).read();
      version = resource;
    } catch {}
    const p = (version && version.snapshot && version.snapshot.profileBuildPayload) || null;
    if (!p || !p.role || !p.conn || !p.fingerprint || !p.database) {
      try {
        await containers.runs.item(runId, schedule.id).replace({
          ...runDoc,
          status:       'failed',
          finishedAt:   nowIso(),
          errorMessage: 'Pinned profile-build snapshot missing required fields — re-create the task from Insights.',
        });
      } catch {}
      return { fired: false, reason: 'profile-build snapshot incomplete', runId };
    }
    dispatchBody = {
      role:        p.role,
      conn:        p.conn,
      database:    p.database,
      fingerprint: p.fingerprint,
      notifyEmail: p.notifyEmail || schedule.userId,
    };
  } else {
    bgUrl = getMigrationUrl();
    dispatchBody = { runId, scheduleId: schedule.id, userId: schedule.userId };
  }

  // 4. Fire-and-forget. Short timeout because we only need the 202 ack,
  //    not the actual work to finish.
  try {
    const ctl = new AbortController();
    const to  = setTimeout(() => ctl.abort(), 8_000);
    await fetch(bgUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-user-id': schedule.userId },
      body: JSON.stringify(dispatchBody),
      signal: ctl.signal,
    });
    clearTimeout(to);
  } catch (e) {
    // Dispatch failed — mark the run record failed synchronously so
    // the dashboard's run history shows a stable state instead of a
    // permanently-queued ghost.
    try {
      await containers.runs.item(runId, schedule.id).replace({
        ...runDoc,
        status:       'failed',
        finishedAt:   nowIso(),
        errorMessage: 'Cron dispatch failed: ' + (e.message || e),
      });
    } catch {}
    return { fired: false, reason: 'dispatch failed: ' + (e.message || e), runId };
  }

  return { fired: true, runId };
}

// ── Cron entry point ────────────────────────────────────────────────────
// Netlify invokes this handler with no event body — we don't read it.
// Return value is ignored by Netlify's cron infrastructure but we still
// emit a JSON summary for visibility in the Functions log.
exports.handler = async () => {
  const startedAt = Date.now();
  let containers;
  try { containers = getContainers(); }
  catch (e) {
    console.error('[scheduled-runner] Cosmos init failed:', e.message);
    return { statusCode: 500, body: e.message };
  }

  // 1. Pick due schedules.
  //
  //    Filter:
  //      - enabled = true (user has not toggled off)
  //      - nextRunAt <= nowIso (we're past the firing time)
  //      - nextRunAt IS_DEFINED (skip schedules that have never been
  //        stamped — these are usually one-off / on-demand schedules
  //        with no cron expression at all)
  //
  //    Cross-partition (the schedules container is partitioned per
  //    /userId; we're scanning across all users). This is a small
  //    container in practice (one row per schedule per user) so cost
  //    is negligible. If you ever have thousands of users, switch to
  //    a per-user partition query — but at v1 scale cross-partition
  //    is fine and simpler.
  const now = new Date();
  let dueSchedules = [];
  try {
    const { resources } = await containers.schedules.items.query({
      query: 'SELECT * FROM c WHERE c.enabled = true AND IS_DEFINED(c.nextRunAt) AND c.nextRunAt <= @now',
      parameters: [{ name: '@now', value: now.toISOString() }],
    }).fetchAll();
    dueSchedules = resources || [];
  } catch (e) {
    console.error('[scheduled-runner] schedule query failed:', e.message);
    return { statusCode: 500, body: e.message };
  }

  if (!dueSchedules.length) {
    return { statusCode: 200, body: JSON.stringify({ ticked: true, fired: 0, durationMs: Date.now() - startedAt }) };
  }

  // 2. Fire each in sequence. We deliberately serialise rather than
  //    Promise.all so a slow dispatch doesn't crowd out the others, and
  //    so log output stays readable in Netlify's Functions log.
  const outcomes = [];
  for (const schedule of dueSchedules) {
    try {
      const out = await fireSchedule(containers, schedule, now);
      outcomes.push({ scheduleId: schedule.id, name: schedule.name, ...out });
    } catch (e) {
      console.error('[scheduled-runner] fireSchedule error for', schedule.id, e.message);
      outcomes.push({ scheduleId: schedule.id, name: schedule.name, fired: false, reason: 'exception: ' + e.message });
    }
  }

  const firedCount = outcomes.filter(o => o.fired).length;
  console.log('[scheduled-runner] tick complete:', JSON.stringify({
    fired:   firedCount,
    skipped: outcomes.length - firedCount,
    durationMs: Date.now() - startedAt,
    outcomes,
  }));

  return { statusCode: 200, body: JSON.stringify({ fired: firedCount, total: outcomes.length, outcomes }) };
};
