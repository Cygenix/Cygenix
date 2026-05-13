// netlify/functions/scheduler.js
//
// Unified scheduler backend for Cygenix Task Agent. Owns all reads/writes to
// the Cosmos containers that store schedule metadata, run history, and pinned
// SQL version snapshots. Replaces the (never-built) /api/schedules/* and
// /api/data/version-* endpoints that the Task Agent used to assume lived on
// the Azure Function — by routing through Netlify instead, the Task Agent
// works for both Azure-target and direct-SQL-target projects without
// requiring a Function URL to be configured.
//
// ── Endpoints ───────────────────────────────────────────────────────────────
// POST body shape: { action, ...payload }   plus header  x-user-id: <email>
//
//   action                 payload                            returns
//   --------------------   ---------------------------------  --------------------
//   list-schedules         {}                                 { schedules: [...] }
//   create-schedule        { name, jobId, jobVersionId,       { id, schedule }
//                            cron, timezone, chainAfter,
//                            enabled }
//   update-schedule        { id, patch: { ... } }             { schedule }
//   delete-schedule        { id }                             { deleted: true }
//   toggle-enabled         { id, enabled }                    { schedule }
//   list-runs              { scheduleId?, limit? }            { runs: [...] }
//   get-run                { id, scheduleId }                 { run }
//   version-list           { jobId }                          { versions: [...] }
//   version-create         { jobId, snapshot, label, note }   { id, version, created }
//
// ── Cosmos containers ──────────────────────────────────────────────────────
//   schedules     — partition key /userId   — TTL: none
//   runs          — partition key /scheduleId — TTL: 2,592,000 sec (30 days)
//   job_versions  — partition key /jobId    — TTL: none
//
// Containers must exist before first call. The function does NOT create them
// on demand because (a) container creation requires elevated RBAC, and (b)
// silent auto-create on a misconfigured COSMOS_DATABASE would write to the
// wrong place. If a container is missing, the function returns a 500 with
// a clear setup-required message.
//
// ── Auth ───────────────────────────────────────────────────────────────────
// The frontend sends x-user-id: <user email> in every request. Scheduler
// data is scoped to that user — list-schedules only returns the user's
// own rows, etc. This matches the per-user-document pattern used by the
// existing `projects` container.

const { CosmosClient } = require('@azure/cosmos');
const sql = require('mssql');

// ── Cosmos client (lazy, module-scoped so it's reused across invocations) ──
let _cosmosClient = null;
let _containers = null;

function getContainers() {
  if (_containers) return _containers;
  const conn = process.env.COSMOS_CONNECTION_STRING;
  if (!conn) {
    throw new Error('COSMOS_CONNECTION_STRING env var not set in Netlify');
  }
  _cosmosClient = new CosmosClient(conn);
  const dbName = process.env.COSMOS_DATABASE || 'cygenix';
  const db = _cosmosClient.database(dbName);
  _containers = {
    schedules:    db.container('schedules'),
    runs:         db.container('runs'),
    job_versions: db.container('job_versions'),
  };
  return _containers;
}

// ── Helpers ────────────────────────────────────────────────────────────────
const nowIso  = () => new Date().toISOString();
const newId   = () => 'sch_' + Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
const newRunId = () => 'run_' + Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
const newVerId = () => 'ver_' + Math.random().toString(36).slice(2, 10) + Date.now().toString(36);

function jsonResponse(statusCode, body) {
  return {
    statusCode,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Content-Type, x-user-id',
      'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
    },
    body: JSON.stringify(body),
  };
}

function errorResponse(statusCode, message, extras) {
  return jsonResponse(statusCode, { error: message, ...(extras || {}) });
}

// Cosmos container-missing detection. The SDK throws different shapes depending
// on whether the database or the container is the missing piece, so we check
// for both `NotFound` and the "Owner resource does not exist" message.
function isMissingResource(err) {
  if (!err) return false;
  if (err.code === 404) return true;
  if (typeof err.code === 'string' && err.code === 'NotFound') return true;
  const msg = String(err.message || '');
  return /Owner resource does not exist/i.test(msg)
      || /Resource Not Found/i.test(msg);
}

// Tiny cron next-run calculator. Supports standard 5-field expressions with
// numeric values, '*', and '*/n' increments. No day-of-week names, no ranges,
// no comma lists — keep this dependency-free. If the cron string uses
// unsupported syntax, returns null so the caller can store a null nextRunAt
// and let the trigger layer (phase 3) deal with it via a proper parser there.
function computeNextRun(cronExpr, fromDate) {
  try {
    const parts = String(cronExpr || '').trim().split(/\s+/);
    if (parts.length !== 5) return null;
    const [miStr, hStr, domStr, moStr, dowStr] = parts;
    const parseField = (s, min, max) => {
      if (s === '*') return null;                                   // wildcard
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

    // Search forward minute-by-minute up to 60 days. Cheap and avoids edge-case
    // bugs in clever DST/month-rollover math.
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

// Pretty cron summary for the schedules table.
function humaniseCron(cronExpr) {
  const parts = String(cronExpr || '').trim().split(/\s+/);
  if (parts.length !== 5) return cronExpr || '';
  const [mi, h, dom, mo, dow] = parts;
  if (mi === '*' && h === '*' && dom === '*' && mo === '*' && dow === '*') return 'every minute';
  if (h === '*' && dom === '*' && mo === '*' && dow === '*' && /^\d+$/.test(mi)) return `every hour at :${mi.padStart(2,'0')}`;
  if (dom === '*' && mo === '*' && dow === '*' && /^\d+$/.test(mi) && /^\d+$/.test(h)) return `daily at ${h.padStart(2,'0')}:${mi.padStart(2,'0')}`;
  if (dom === '*' && mo === '*' && /^\d+$/.test(mi) && /^\d+$/.test(h) && /^\d+$/.test(dow)) {
    const days = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
    return `${days[parseInt(dow,10)]} at ${h.padStart(2,'0')}:${mi.padStart(2,'0')}`;
  }
  return cronExpr;
}

// ── Action handlers ────────────────────────────────────────────────────────

async function listSchedules(userId, _body, containers) {
  const { resources } = await containers.schedules.items.query({
    query: 'SELECT * FROM c WHERE c.userId = @u ORDER BY c._ts DESC',
    parameters: [{ name: '@u', value: userId }],
  }).fetchAll();
  return { schedules: resources };
}

async function createSchedule(userId, body, containers) {
  const { name, jobId, jobVersionId, cron, timezone, chainAfter, enabled,
          srcConn, tgtConn } = body || {};
  if (!name)         return { __err: 'name required' };
  if (!jobId)        return { __err: 'jobId required' };
  if (!jobVersionId) return { __err: 'jobVersionId required' };
  if (!cron && !chainAfter) return { __err: 'cron or chainAfter required' };
  // tgtConn is required for run-now / auto-trigger to work without a browser.
  // We accept the create-schedule call even if it's missing, but the schedule
  // will fail at execution time with a clear error. Most users get here via
  // ta_saveSchedule which always sends both fields if Connections is set up.
  if (!tgtConn) return { __err: 'tgtConn required (configure Connections → Target before scheduling)' };

  // Look up the pinned version to denormalise its version number onto the
  // schedule row — the schedules table renders "v3" without doing a second
  // round trip, so we copy that small number at create time.
  let jobVersionNumber = null;
  try {
    const { resource: ver } = await containers.job_versions.item(jobVersionId, jobId).read();
    if (ver) jobVersionNumber = ver.version;
  } catch (e) { if (!isMissingResource(e)) throw e; }

  const id = newId();
  const doc = {
    id,
    userId,
    name,
    jobId,
    jobVersionId,
    jobVersionNumber,
    // Connection strings (or Azure Function URLs) needed for server-side
    // execution. Stored alongside the schedule because the auto-trigger has
    // no browser to fetch them from at run time. Cosmos is encrypted at
    // rest and access-controlled to the Cygenix Function App.
    srcConn: srcConn || null,
    tgtConn,
    cron:        cron || null,
    timezone:    timezone || null,
    chainAfter:  chainAfter || null,
    enabled:     enabled !== false,
    humanReadable: cron ? humaniseCron(cron) : null,
    nextRunAt:   cron ? computeNextRun(cron, new Date()) : null,
    lastRunAt:   null,
    lastRunStatus: null,
    createdAt:   nowIso(),
    updatedAt:   nowIso(),
  };
  const { resource } = await containers.schedules.items.create(doc);
  return { id, schedule: resource };
}

async function updateSchedule(userId, body, containers) {
  const { id, patch } = body || {};
  if (!id)    return { __err: 'id required' };
  if (!patch) return { __err: 'patch required' };

  const { resource: existing } = await containers.schedules.item(id, userId).read();
  if (!existing) return { __err: 'schedule not found', __status: 404 };
  if (existing.userId !== userId) return { __err: 'forbidden', __status: 403 };

  const merged = { ...existing, ...patch, updatedAt: nowIso() };
  // Recompute denormalised fields if their inputs changed.
  if (Object.prototype.hasOwnProperty.call(patch, 'cron')) {
    merged.humanReadable = patch.cron ? humaniseCron(patch.cron) : null;
    merged.nextRunAt     = patch.cron ? computeNextRun(patch.cron, new Date()) : null;
  }
  if (Object.prototype.hasOwnProperty.call(patch, 'jobVersionId') && patch.jobVersionId) {
    try {
      const { resource: ver } = await containers.job_versions.item(patch.jobVersionId, merged.jobId).read();
      if (ver) merged.jobVersionNumber = ver.version;
    } catch (e) { if (!isMissingResource(e)) throw e; }
  }
  const { resource } = await containers.schedules.item(id, userId).replace(merged);
  return { schedule: resource };
}

async function deleteSchedule(userId, body, containers) {
  const { id } = body || {};
  if (!id) return { __err: 'id required' };
  try {
    await containers.schedules.item(id, userId).delete();
    return { deleted: true };
  } catch (e) {
    if (isMissingResource(e)) return { deleted: false, __status: 404, __err: 'schedule not found' };
    throw e;
  }
}

async function toggleEnabled(userId, body, containers) {
  const { id, enabled } = body || {};
  if (!id) return { __err: 'id required' };
  const { resource: existing } = await containers.schedules.item(id, userId).read();
  if (!existing) return { __err: 'schedule not found', __status: 404 };
  const merged = { ...existing, enabled: !!enabled, updatedAt: nowIso() };
  const { resource } = await containers.schedules.item(id, userId).replace(merged);
  return { schedule: resource };
}

async function listRuns(userId, body, containers) {
  const { scheduleId, limit } = body || {};
  const lim = Math.min(parseInt(limit, 10) || 50, 200);
  let query, parameters;
  if (scheduleId) {
    query = 'SELECT TOP @l * FROM c WHERE c.scheduleId = @s AND c.userId = @u ORDER BY c.startedAt DESC';
    parameters = [
      { name: '@l', value: lim },
      { name: '@s', value: scheduleId },
      { name: '@u', value: userId },
    ];
  } else {
    // Cross-partition read of the user's most recent runs. OK at the runs
    // table's expected size — 30-day TTL keeps it bounded.
    query = 'SELECT TOP @l * FROM c WHERE c.userId = @u ORDER BY c.startedAt DESC';
    parameters = [
      { name: '@l', value: lim },
      { name: '@u', value: userId },
    ];
  }
  const { resources } = await containers.runs.items
    .query({ query, parameters }, { maxItemCount: lim })
    .fetchAll();
  return { runs: resources };
}

async function getRun(userId, body, containers) {
  const { id, scheduleId } = body || {};
  if (!id || !scheduleId) return { __err: 'id and scheduleId required' };
  try {
    const { resource } = await containers.runs.item(id, scheduleId).read();
    if (!resource)                       return { __err: 'run not found', __status: 404 };
    if (resource.userId !== userId)      return { __err: 'forbidden', __status: 403 };
    return { run: resource };
  } catch (e) {
    if (isMissingResource(e)) return { __err: 'run not found', __status: 404 };
    throw e;
  }
}

async function versionList(userId, body, containers) {
  const { jobId } = body || {};
  if (!jobId) return { __err: 'jobId required' };
  const { resources } = await containers.job_versions.items.query({
    query: 'SELECT * FROM c WHERE c.jobId = @j AND c.userId = @u ORDER BY c.version DESC',
    parameters: [
      { name: '@j', value: jobId },
      { name: '@u', value: userId },
    ],
  }).fetchAll();
  return { versions: resources };
}

async function versionCreate(userId, body, containers) {
  const { jobId, snapshot, label, note } = body || {};
  if (!jobId)    return { __err: 'jobId required' };
  if (!snapshot) return { __err: 'snapshot required' };

  // Dedupe: if the most recent version's snapshot SQL is identical to the
  // incoming one, return that existing version instead of creating a new
  // row. Matches the dashboard's expected "no change" path.
  const incomingSql = snapshot.sql || snapshot.SQL || '';
  const { resources: latest } = await containers.job_versions.items.query({
    query: 'SELECT TOP 1 * FROM c WHERE c.jobId = @j AND c.userId = @u ORDER BY c.version DESC',
    parameters: [
      { name: '@j', value: jobId },
      { name: '@u', value: userId },
    ],
  }).fetchAll();
  if (latest.length) {
    const prev = latest[0];
    const prevSql = (prev.snapshot && (prev.snapshot.sql || prev.snapshot.SQL)) || '';
    if (prevSql && prevSql === incomingSql) {
      return { id: prev.id, version: prev.version, created: false };
    }
  }
  const nextVersion = latest.length ? (latest[0].version + 1) : 1;
  const id = newVerId();
  const doc = {
    id,
    jobId,
    userId,
    version: nextVersion,
    label: label || 'auto',
    note:  note || '',
    snapshot,
    createdAt: nowIso(),
  };
  await containers.job_versions.items.create(doc);
  return { id, version: nextVersion, created: true };
}

// ── run-now (Phase 2) ──────────────────────────────────────────────────────
//
// Server-side execution of a scheduled task. Takes a scheduleId, reads the
// pinned version's snapshot, opens an mssql connection to the schedule's
// stored tgtConn, and executes each child step's `sql` in order. Writes a
// `runs` document with status + duration + error if any. Phase 3's auto-
// trigger will call the same handler when a cron fires — the run-now button
// and the timer both arrive here, which is the whole point of doing this
// server-side.
//
// What's executed in phase 2:
//   - Child steps with a non-empty `sql` field. These are SQL-script steps
//     (e.g. SQL Server INSERT/UPDATE/MERGE scripts) that are self-contained.
//
// What's NOT supported yet (returns a clear error in the run record):
//   - migration-type jobs that need paginated SELECT-from-source + INSERT-
//     to-target. Those use the dashboard's runMigrationStep path which has
//     ~hundreds of lines of pagination, type-coercion, and one-to-many
//     handling we haven't ported server-side. Run them from the dashboard
//     "Run selected" button for now.
//
// Direct-SQL target only in phase 2. If tgtConn is an Azure Function URL
// (https://...) we error rather than try to call it — the Function App
// route needs a different execution path that we'll add when phase 3 lands.

function isHttpUrl(s)    { return /^https?:\/\//i.test(s || ''); }
function isMssqlConn(s)  { return /^mssql:\/\//i.test(s || ''); }

// Parse a mssql:// URL into an mssql driver config. Mirrors the parsing in
// netlify/functions/db-connect.js so behaviour stays consistent across both
// execution paths.
function parseMssqlUrl(connStr) {
  // Format: mssql://user:pass@host:port/database?encrypt=true&trustServerCertificate=true
  const u = new URL(connStr);
  const params = u.searchParams;
  return {
    user:     decodeURIComponent(u.username || ''),
    password: decodeURIComponent(u.password || ''),
    server:   u.hostname,
    port:     u.port ? parseInt(u.port, 10) : 1433,
    database: u.pathname.replace(/^\//, ''),
    options: {
      encrypt:               params.get('encrypt') !== 'false',
      trustServerCertificate: params.get('trustServerCertificate') === 'true',
      enableArithAbort:      true,
    },
    requestTimeout: 120_000,
    connectionTimeout: 30_000,
  };
}

async function runNow(userId, body, containers) {
  const { id } = body || {};
  if (!id) return { __err: 'id required' };

  // Read the schedule.
  let schedule;
  try {
    const { resource } = await containers.schedules.item(id, userId).read();
    schedule = resource;
  } catch (e) {
    if (isMissingResource(e)) return { __err: 'schedule not found', __status: 404 };
    throw e;
  }
  if (!schedule)                       return { __err: 'schedule not found', __status: 404 };
  if (schedule.userId !== userId)      return { __err: 'forbidden', __status: 403 };

  // Read the pinned version's snapshot — that's where the SQL/childSteps live.
  let version;
  try {
    const { resource } = await containers.job_versions.item(schedule.jobVersionId, schedule.jobId).read();
    version = resource;
  } catch (e) {
    if (!isMissingResource(e)) throw e;
  }
  if (!version || !version.snapshot) {
    return { __err: 'Pinned version snapshot not found. Re-pin the version on the schedule.', __status: 400 };
  }

  // Resolve what to execute. Two supported shapes:
  //   (a) composite job → snapshot.childSteps[], execute each .sql in order
  //   (b) simple job    → snapshot.sql, execute as one statement
  const snap = version.snapshot;
  const stepsToRun = Array.isArray(snap.childSteps) && snap.childSteps.length
    ? snap.childSteps
    : (snap.sql ? [{ sql: snap.sql, jobType: snap.jobType || 'sql' }] : []);
  if (!stepsToRun.length) {
    return { __err: 'No SQL to execute on the pinned version', __status: 400 };
  }

  // Validate target connection.
  const tgtConn = schedule.tgtConn || '';
  if (!tgtConn) {
    return { __err: 'No target connection stored on this schedule. Edit and re-save.', __status: 400 };
  }
  if (isHttpUrl(tgtConn)) {
    return { __err: 'Azure Function targets are not supported by run-now yet — phase 3 will add this.', __status: 400 };
  }
  if (!isMssqlConn(tgtConn)) {
    return { __err: 'Only mssql:// connection strings are supported in phase 2.', __status: 400 };
  }

  // Create a "running" run record up front so the UI can see the row even
  // if the connection hangs / Netlify times out before completion.
  const runId    = newRunId();
  const startedAt = nowIso();
  const runDoc = {
    id: runId,
    scheduleId: schedule.id,
    userId,
    jobId:       schedule.jobId,
    jobVersionId: schedule.jobVersionId,
    triggeredBy: body.triggeredBy || 'manual',
    status:      'running',
    startedAt,
    finishedAt:  null,
    rowsAffected: 0,
    errorMessage: null,
    stepResults: [],
  };
  await containers.runs.items.create(runDoc);

  // Open the mssql connection, execute each step, accumulate results.
  let pool;
  let totalRows = 0;
  const stepResults = [];
  let failedStep = null;
  let failedError = null;
  try {
    pool = await sql.connect(parseMssqlUrl(tgtConn));

    for (let i = 0; i < stepsToRun.length; i++) {
      const step = stepsToRun[i];
      const stepLabel = step.tgtTable || step.name || ('step ' + (i + 1));
      const stepSql   = (step.sql || step.insertSQL || '').trim();
      if (!stepSql) {
        stepResults.push({ index: i, label: stepLabel, status: 'skipped', reason: 'no sql' });
        continue;
      }
      // Migration-style steps need paginated SELECT/INSERT which we haven't
      // ported server-side yet. Skip them with a clear marker rather than
      // running just the insertSQL (which would crash without bind data).
      if (step.jobType === 'migration' && !stepSql.toUpperCase().includes('SELECT')) {
        stepResults.push({
          index: i, label: stepLabel, status: 'skipped',
          reason: 'migration steps not supported in scheduled runs yet — use Project Builder → Run selected',
        });
        continue;
      }
      const stepStart = Date.now();
      try {
        const r = await pool.request().query(stepSql);
        const rows = Array.isArray(r.rowsAffected)
          ? r.rowsAffected.reduce((a,b) => a + b, 0)
          : (r.rowsAffected || 0);
        totalRows += rows;
        stepResults.push({
          index: i, label: stepLabel, status: 'success',
          rowsAffected: rows, durationMs: Date.now() - stepStart,
        });
      } catch (stepErr) {
        failedStep = i;
        failedError = stepErr;
        stepResults.push({
          index: i, label: stepLabel, status: 'failed',
          durationMs: Date.now() - stepStart,
          errorMessage: String(stepErr.message || stepErr),
        });
        break; // stop on first failure (matches "Run selected" behaviour)
      }
    }
  } catch (connErr) {
    failedError = connErr;
  } finally {
    if (pool) { try { await pool.close(); } catch {} }
  }

  // Finalise the run record.
  const finishedAt = nowIso();
  const status = failedError ? 'failed' : 'success';
  const errorMessage = failedError ? String(failedError.message || failedError) : null;

  const finalRun = {
    ...runDoc,
    status,
    finishedAt,
    rowsAffected: totalRows,
    errorMessage,
    stepResults,
  };
  await containers.runs.item(runId, schedule.id).replace(finalRun);

  // Update lastRunAt / lastRunStatus / nextRunAt on the schedule itself.
  const nextRunAt = schedule.cron ? computeNextRun(schedule.cron, new Date()) : null;
  const updatedSchedule = {
    ...schedule,
    lastRunAt: finishedAt,
    lastRunStatus: status,
    nextRunAt,
    updatedAt: nowIso(),
  };
  await containers.schedules.item(schedule.id, userId).replace(updatedSchedule);

  return {
    runId,
    status,
    rowsAffected: totalRows,
    errorMessage,
    stepResults,
    startedAt,
    finishedAt,
  };
}

// ── HTTP entry point ───────────────────────────────────────────────────────
exports.handler = async (event) => {
  // CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    return jsonResponse(200, { ok: true });
  }
  if (event.httpMethod !== 'POST' && event.httpMethod !== 'GET') {
    return errorResponse(405, 'Method not allowed');
  }

  // userId from header — frontend sets this on every Task Agent call.
  const userId = (event.headers && (event.headers['x-user-id'] || event.headers['X-User-Id'])) || '';
  if (!userId) return errorResponse(401, 'x-user-id header required (sign in first)');

  // Body
  let body = {};
  if (event.body) {
    try { body = JSON.parse(event.body); }
    catch { return errorResponse(400, 'Invalid JSON body'); }
  }

  const action = body.action || (event.queryStringParameters && event.queryStringParameters.action);
  if (!action) return errorResponse(400, 'action required');

  let containers;
  try { containers = getContainers(); }
  catch (e) { return errorResponse(500, e.message); }

  const handlers = {
    'list-schedules':  listSchedules,
    'create-schedule': createSchedule,
    'update-schedule': updateSchedule,
    'delete-schedule': deleteSchedule,
    'toggle-enabled':  toggleEnabled,
    'list-runs':       listRuns,
    'get-run':         getRun,
    'version-list':    versionList,
    'version-create':  versionCreate,
    'run-now':         runNow,
  };
  const fn = handlers[action];
  if (!fn) return errorResponse(400, `Unknown action: ${action}`);

  try {
    const result = await fn(userId, body, containers);
    if (result && result.__err) {
      const status = result.__status || 400;
      const { __err, __status, ...rest } = result;
      return errorResponse(status, __err, rest);
    }
    return jsonResponse(200, result);
  } catch (e) {
    if (isMissingResource(e)) {
      return errorResponse(500,
        'Cosmos container missing. Create containers schedules (/userId), runs (/scheduleId, 30-day TTL), and job_versions (/jobId) in the Cygenix Cosmos database.',
        { detail: e.message }
      );
    }
    return errorResponse(500, e.message, { stack: e.stack });
  }
};
