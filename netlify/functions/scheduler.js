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
//   get-notify-settings    {}                                 { settings }
//   save-notify-settings   { settings: { ... } }              { settings }
//   list-notifications     { limit? }                         { notifications: [...] }
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
const { verifyAuthHeader } = require('./lib/entra-auth');

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
    // Written by the Function App (notify-log.js); read here so Settings →
    // Notifications can show what was actually delivered.
    notifications: db.container('notifications'),
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
      'Access-Control-Allow-Headers': 'Content-Type, x-user-id, Authorization',
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

// Cron next-run calculation lives in lib/cron.js, shared with
// scheduled-runner.js so the two can't drift apart again. It supports
// ranges, comma lists, steps and day/month names, and returns null (rather
// than silently treating a field as a wildcard) for unsupported syntax.
const { computeNextRun } = require('./lib/cron');

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
    // The notification-settings document shares this container and this
    // partition key (see NOTIFY_SETTINGS_ID below), so it has to be filtered
    // out here or it shows up as a nameless schedule in the Task Manager.
    // Schedules written before the discriminator existed have no docType.
    query: 'SELECT * FROM c WHERE c.userId = @u '
         + "AND (NOT IS_DEFINED(c.docType) OR c.docType = 'schedule') "
         + 'ORDER BY c._ts DESC',
    parameters: [{ name: '@u', value: userId }],
  }, { partitionKey: userId }).fetchAll();
  return { schedules: resources };
}

// ── Notification settings ──────────────────────────────────────────────────
// Where the Webhook and Email (SMTP) connector settings live once a user asks
// for them to be delivered server-side.
//
// Until now those settings existed only in localStorage, which is why a
// scheduled run could never deliver to them: the cron tick runs in Azure with
// no browser to read. Moving them to Cosmos is what makes server-side
// delivery possible — and is also why it is opt-in, because it means an SMTP
// password leaves the browser, which the Integrations page otherwise promises
// it will not do.
//
// The document lives in the `schedules` container, which is already
// partitioned by /userId. scheduler.js does not create containers on demand
// (see the header), so a dedicated container would mean nothing worked until
// someone provisioned it by hand.
const NOTIFY_SETTINGS_ID   = 'notify-settings';
const NOTIFY_SETTINGS_TYPE = 'notify-settings';

const NOTIFY_EVENT_DEFAULTS = { started: false, completed: true, failed: true };

function emptyNotifySettings(userId) {
  return {
    id:       NOTIFY_SETTINGS_ID,
    docType:  NOTIFY_SETTINGS_TYPE,
    userId,
    serverDelivery: false,
    events:  { ...NOTIFY_EVENT_DEFAULTS },
    webhook: { enabled: false, url: '', secret: '' },
    smtp:    { enabled: false, host: '', port: '', user: '', pass: '', from: '', to: '' },
    updatedAt: null,
  };
}

// Secrets go up but never come back down. The browser has its own copy in
// localStorage; echoing the stored SMTP password to every page load would put
// it in memory, in logs, and in anything that inspects the response, for no
// benefit. The `*Set` flags let the UI show "a password is stored" without
// handling the password.
function maskNotifySettings(doc) {
  const d = doc || {};
  const webhook = { ...(d.webhook || {}) };
  const smtp    = { ...(d.smtp    || {}) };
  const secretSet = !!webhook.secret;
  const passSet   = !!smtp.pass;
  delete webhook.secret;
  delete smtp.pass;
  return {
    serverDelivery: !!d.serverDelivery,
    events:  { ...NOTIFY_EVENT_DEFAULTS, ...(d.events || {}) },
    webhook: { enabled: false, url: '', ...webhook, secretSet },
    smtp:    { enabled: false, host: '', port: '', user: '', from: '', to: '', ...smtp, passSet },
    updatedAt: d.updatedAt || null,
  };
}

async function getNotifySettings(userId, _body, containers) {
  try {
    const { resource } = await containers.schedules.item(NOTIFY_SETTINGS_ID, userId).read();
    if (!resource || resource.docType !== NOTIFY_SETTINGS_TYPE) {
      return { settings: maskNotifySettings(emptyNotifySettings(userId)) };
    }
    return { settings: maskNotifySettings(resource) };
  } catch (e) {
    // Never configured is the common case, not an error.
    if (isMissingResource(e)) return { settings: maskNotifySettings(emptyNotifySettings(userId)) };
    throw e;
  }
}

// A string field the client may omit. Omitting is "leave as it was"; sending
// an empty string is "clear it". Those are different intents and the mask
// above means the client legitimately cannot send the secrets back.
function pick(next, prev, key) {
  return (next && Object.prototype.hasOwnProperty.call(next, key))
    ? String(next[key] == null ? '' : next[key])
    : String(prev == null ? '' : prev);
}

async function saveNotifySettings(userId, body, containers) {
  const patch = (body && body.settings) || {};

  let existing = emptyNotifySettings(userId);
  try {
    const { resource } = await containers.schedules.item(NOTIFY_SETTINGS_ID, userId).read();
    if (resource && resource.docType === NOTIFY_SETTINGS_TYPE) existing = resource;
  } catch (e) {
    if (!isMissingResource(e)) throw e;
  }

  const w = patch.webhook || {};
  const s = patch.smtp    || {};

  const doc = {
    ...emptyNotifySettings(userId),
    serverDelivery: 'serverDelivery' in patch ? !!patch.serverDelivery : !!existing.serverDelivery,
    events: { ...NOTIFY_EVENT_DEFAULTS, ...(existing.events || {}), ...(patch.events || {}) },
    webhook: {
      enabled: 'enabled' in w ? !!w.enabled : !!(existing.webhook || {}).enabled,
      url:     pick(w, (existing.webhook || {}).url, 'url'),
      secret:  pick(w, (existing.webhook || {}).secret, 'secret'),
    },
    smtp: {
      enabled: 'enabled' in s ? !!s.enabled : !!(existing.smtp || {}).enabled,
      host: pick(s, (existing.smtp || {}).host, 'host'),
      port: pick(s, (existing.smtp || {}).port, 'port'),
      user: pick(s, (existing.smtp || {}).user, 'user'),
      pass: pick(s, (existing.smtp || {}).pass, 'pass'),
      from: pick(s, (existing.smtp || {}).from, 'from'),
      to:   pick(s, (existing.smtp || {}).to,   'to'),
    },
    updatedAt: nowIso(),
  };

  const { resource } = await containers.schedules.items.upsert(doc);
  return { settings: maskNotifySettings(resource || doc) };
}

// ── Notification log ───────────────────────────────────────────────────────
// Every send attempt — the built-in email and each connector delivery — is
// written to the `notifications` container by the Function App. Reading it
// back is what turns "I never got an email" from a support conversation into
// something the user can answer for themselves.
async function listNotifications(userId, body, containers) {
  const lim = Math.min(parseInt((body || {}).limit, 10) || 25, 100);
  try {
    const { resources } = await containers.notifications.items.query({
      query: 'SELECT TOP @l * FROM c WHERE c.userId = @u ORDER BY c.sentAt DESC',
      parameters: [{ name: '@l', value: lim }, { name: '@u', value: userId }],
    }, { maxItemCount: lim }).fetchAll();
    return {
      notifications: (resources || []).map(r => ({
        id:      r.id,
        type:    r.type,
        // Rows written before connectors existed were all Resend emails.
        channel: r.channel || 'resend',
        to:      r.to      || '',
        subject: r.subject || '',
        status:  r.status  || '',
        error:   r.error   || '',
        source:  r.source  || '',
        runId:   r.runId   || '',
        sentAt:  r.sentAt  || '',
      })),
    };
  } catch (e) {
    // The container is created by whoever provisioned the others. Saying so
    // beats a 500 that reads like the feature is broken.
    if (isMissingResource(e)) {
      return {
        notifications: [],
        unavailable: 'The notifications container does not exist in this Cosmos database yet. '
                   + 'Create a container named "notifications" partitioned by /userId; '
                   + 'until then nothing can be logged.',
      };
    }
    throw e;
  }
}

// When does this schedule next fire?
//
// For a repeating schedule the cron is the whole answer. For a one-shot it is
// not: a cron has five fields and none is a year, so "25 14 15 8 *" means
// 14:25 on 15 August in every year. computeNextRun only searches 60 days
// ahead, so once that date has passed it finds the occurrence a year later,
// is unable to reach it, and returns null — and a null nextRunAt is excluded
// by the runner's query, so the schedule silently stops being able to fire at
// all. Editing such a schedule re-saved that null, which is how a one-shot
// could be "saved for today" and never run.
//
// runOnceAt carries the instant, so a one-shot needs no searching. Schedules
// created before this field existed fall back to the cron.
function oneShotNextRun(s) {
  if (s && s.oneShot && s.runOnceAt) {
    const d = new Date(s.runOnceAt);
    // A one-shot in the past has no next run — same as a fired one. Returning
    // it unchanged would make the runner fire it immediately on the next tick,
    // which is not what "run at 14:25 yesterday" means.
    if (!isNaN(d.getTime())) return d.getTime() > Date.now() ? d.toISOString() : null;
  }
  return s && s.cron ? computeNextRun(s.cron, new Date(), s.timezone) : null;
}

async function createSchedule(userId, body, containers) {
  const { name, jobId, jobVersionId, cron, timezone, chainAfter, enabled,
          srcConn, tgtConn, oneShot, runOnceAt } = body || {};
  if (!name)         return { __err: 'name required' };
  if (!jobId)        return { __err: 'jobId required' };
  if (!jobVersionId) return { __err: 'jobVersionId required' };
  if (!cron && !chainAfter) return { __err: 'cron or chainAfter required' };

  // Look up the pinned version to denormalise its version number onto the
  // schedule row — the schedules table renders "v3" without doing a second
  // round trip, so we copy that small number at create time. We also use
  // the version's snapshot.jobType to decide whether tgtConn is required:
  // migration jobs need it (the runner connects to the target server),
  // profile-build jobs don't (they carry their own conn inside the snapshot).
  let jobVersionNumber = null;
  let snapJobType = '';
  try {
    const { resource: ver } = await containers.job_versions.item(jobVersionId, jobId).read();
    if (ver) {
      jobVersionNumber = ver.version;
      snapJobType = (ver.snapshot && ver.snapshot.jobType) || '';
    }
  } catch (e) { if (!isMissingResource(e)) throw e; }

  // tgtConn is required for migration runs (the runner has no browser to
  // fetch it from at execution time). Profile-build runs don't need it.
  if (snapJobType !== 'profile-build' && !tgtConn) {
    return { __err: 'tgtConn required (configure Connections → Target before scheduling)' };
  }

  const id = newId();
  const doc = {
    id,
    userId,
    name,
    jobId,
    jobVersionId,
    jobVersionNumber,
    jobType:     snapJobType || 'migration',  // remembered for UI filtering / lifecycle
    // Connection strings (or Azure Function URLs) needed for server-side
    // execution. Stored alongside the schedule because the auto-trigger has
    // no browser to fetch them from at run time. Cosmos is encrypted at
    // rest and access-controlled to the Cygenix Function App.
    srcConn: srcConn || null,
    tgtConn: tgtConn || null,
    cron:        cron || null,
    timezone:    timezone || null,
    chainAfter:  chainAfter || null,
    oneShot:     !!oneShot,  // one-off schedules auto-disable after firing
    // The instant a one-shot should fire. A cron has no year field, so for a
    // single-occurrence schedule the expression alone is ambiguous — see
    // oneShotNextRun below.
    runOnceAt:   runOnceAt || null,
    enabled:     enabled !== false,
    humanReadable: cron ? humaniseCron(cron) : null,
    nextRunAt:   oneShotNextRun({ oneShot, runOnceAt, cron }),
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
  // Recompute denormalised fields if their inputs changed. runOnceAt counts as
  // an input: editing only the date of a one-shot leaves the cron identical
  // (it carries no year), so keying solely off `cron` would keep the old
  // nextRunAt and the edit would appear to do nothing.
  if (Object.prototype.hasOwnProperty.call(patch, 'cron') ||
      Object.prototype.hasOwnProperty.call(patch, 'runOnceAt')) {
    merged.humanReadable = merged.cron ? humaniseCron(merged.cron) : null;
    merged.nextRunAt     = oneShotNextRun(merged);
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
  // Project the list fields and bound it: each version document carries a
  // full job snapshot (every step's insertSQL and column mapping), so
  // SELECT * on a job with hundreds of saved versions shipped megabytes to
  // render a list that shows five fields. versionGet fetches the snapshot
  // when one is actually opened.
  const { resources } = await containers.job_versions.items.query({
    query: 'SELECT TOP 100 c.id, c.jobId, c.userId, c.version, c.label, c.note, c.createdAt, c.hash '
         + 'FROM c WHERE c.jobId = @j AND c.userId = @u ORDER BY c.version DESC',
    parameters: [
      { name: '@j', value: jobId },
      { name: '@u', value: userId },
    ],
  }, { partitionKey: jobId }).fetchAll();
  return { versions: resources };
}

async function versionCreate(userId, body, containers) {
  const { jobId, snapshot, label, note } = body || {};
  if (!jobId)    return { __err: 'jobId required' };
  if (!snapshot) return { __err: 'snapshot required' };

  // Dedupe: if the most recent version's content is identical to the
  // incoming one, return that existing version instead of creating a new
  // row. Matches the dashboard's expected "no change" path.
  //
  // For composite tasks the top-level snapshot.sql is empty — the real SQL
  // lives in childSteps[*].sql / .insertSQL. So we hash a normalised
  // representation of the whole snapshot (top-level sql + each child step's
  // executable fields), which correctly distinguishes "the same composite"
  // from "the composite plus a new column mapping" or "the composite with
  // a different parameter substitution from phase 2a".
  const hashSnapshot = (snap) => {
    if (!snap || typeof snap !== 'object') return '';
    const parts = [];
    parts.push('top-sql:' + (snap.sql || snap.SQL || ''));
    parts.push('top-insertSQL:' + (snap.insertSQL || ''));
    if (Array.isArray(snap.childSteps)) {
      snap.childSteps.forEach((step, i) => {
        parts.push('child-' + i + '-sql:' + (step.sql || ''));
        parts.push('child-' + i + '-insertSQL:' + (step.insertSQL || ''));
        parts.push('child-' + i + '-srcWhere:' + (step.srcWhere || ''));
        parts.push('child-' + i + '-connOn:' + (step.connOn || ''));
      });
    }
    return parts.join('\n');
  };
  const incomingHash = hashSnapshot(snapshot);
  const { resources: latest } = await containers.job_versions.items.query({
    query: 'SELECT TOP 1 * FROM c WHERE c.jobId = @j AND c.userId = @u ORDER BY c.version DESC',
    parameters: [
      { name: '@j', value: jobId },
      { name: '@u', value: userId },
    ],
  }, { partitionKey: jobId }).fetchAll();
  if (latest.length) {
    const prev = latest[0];
    const prevHash = hashSnapshot(prev.snapshot);
    if (prevHash && prevHash === incomingHash) {
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

// version-delete — remove a single snapshot row. Used to clean up probe
// versions or accidental snapshots. Refuses if any schedule still pins
// to this version, to avoid leaving schedules with a dangling jobVersionId.
async function versionDelete(userId, body, containers) {
  const { id, jobId } = body || {};
  if (!id || !jobId) return { __err: 'id and jobId required' };

  // Verify the version belongs to this user before allowing delete.
  let existing;
  try {
    const { resource } = await containers.job_versions.item(id, jobId).read();
    existing = resource;
  } catch (e) {
    if (isMissingResource(e)) return { __err: 'version not found', __status: 404 };
    throw e;
  }
  if (!existing)                    return { __err: 'version not found', __status: 404 };
  if (existing.userId !== userId)   return { __err: 'forbidden', __status: 403 };

  // Block delete if any schedule pins to this version.
  const { resources: pinned } = await containers.schedules.items.query({
    query: 'SELECT c.id, c.name FROM c WHERE c.userId = @u AND c.jobVersionId = @v',
    parameters: [
      { name: '@u', value: userId },
      { name: '@v', value: id },
    ],
  }, { partitionKey: userId }).fetchAll();
  if (pinned.length) {
    return {
      __err: 'Cannot delete: this version is pinned by ' + pinned.length + ' schedule(s): '
        + pinned.map(s => s.name).join(', '),
      __status: 409,
    };
  }

  await containers.job_versions.item(id, jobId).delete();
  return { deleted: true };
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

async function runNow(userId, body, containers, event) {
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

  // Sanity-check the pinned version exists, so we fail fast at the queue
  // boundary rather than letting the background function discover it.
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

  // ── Job type routing ────────────────────────────────────────────────────
  // The Task Agent now supports two kinds of pinned jobs:
  //   * migration jobs (jobType missing, or 'composite', or 'migration') →
  //     dispatched to the Azure /api/run-migration endpoint which runs the
  //     SQL conversion against srcConn / tgtConn.
  //   * profile-build jobs (jobType === 'profile-build') → dispatched to
  //     /api/profile-build which runs an overnight Claude-assisted schema
  //     analysis. These don't have a tgtConn — they only need {role, conn,
  //     database, fingerprint, notifyEmail} from the snapshot payload.
  //
  // Routing is driven by the snapshot, NOT by the schedule row, so a job's
  // type is locked at the moment the version is created (which is what
  // version pinning is for).
  const snapJobType = (version.snapshot && version.snapshot.jobType) || '';
  const isProfileBuild = snapJobType === 'profile-build';

  // Quick connection-string sanity for migration jobs. Profile-build jobs
  // carry their connection inside the snapshot, not on the schedule row, so
  // these checks don't apply to them.
  let tgtConn = '';
  if (!isProfileBuild) {
    tgtConn = schedule.tgtConn || '';
    if (!tgtConn)            return { __err: 'No target connection stored on this schedule. Edit and re-save.', __status: 400 };
    if (isHttpUrl(tgtConn))  return { __err: 'Azure Function targets are not supported by run-now yet — phase 3 will add this.', __status: 400 };
    if (!isMssqlConn(tgtConn)) return { __err: 'Only mssql:// connection strings are supported.', __status: 400 };
  }

  // Create the run record in 'queued' state. The background function will
  // flip it to 'running' when it picks up, and then 'success' or 'failed'.
  // Dashboard polls `get-run` to detect the transition.
  const runId    = newRunId();
  const startedAt = nowIso();
  const runDoc = {
    id: runId,
    scheduleId:   schedule.id,
    userId,
    jobId:        schedule.jobId,
    jobVersionId: schedule.jobVersionId,
    jobType:      snapJobType || 'migration',  // remembered on the run for run-history filtering
    triggeredBy:  body.triggeredBy || 'manual',
    status:       'queued',
    startedAt,
    finishedAt:   null,
    rowsAffected: 0,
    errorMessage: null,
    stepResults:  [],
  };
  await containers.runs.items.create(runDoc);

  // Fire-and-forget the background runner. We hit the Azure Function App
  // (Flex Consumption, no enforced timeout) by absolute URL. Two endpoints:
  //   migration   → /api/run-migration with {runId, scheduleId, userId}
  //   profile-build → /api/profile-build with the snapshot payload directly,
  //                   because profile-builder.js doesn't read from the runs
  //                   container — it manages its own data_profile_tasks rows.
  // Either way we just need the 202 ack, not the work to finish.
  let bgUrl, dispatchBody;
  if (isProfileBuild) {
    bgUrl = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/profile-build';
    const p = version.snapshot.profileBuildPayload || {};
    if (!p.role || !p.conn || !p.fingerprint || !p.database) {
      // Mark the run failed synchronously and bail — the snapshot is malformed.
      const failedRun = {
        ...runDoc,
        status: 'failed',
        finishedAt: nowIso(),
        errorMessage: 'Profile-build snapshot is missing required fields (role/conn/database/fingerprint). Re-create the task from the Insights page.',
      };
      try { await containers.runs.item(runId, schedule.id).replace(failedRun); } catch {}
      return { __err: 'Profile-build snapshot incomplete — re-create the task from Insights.', __status: 400 };
    }
    dispatchBody = {
      role:         p.role,
      conn:         p.conn,
      database:     p.database,
      fingerprint:  p.fingerprint,
      notifyEmail:  p.notifyEmail || userId,
    };
  } else {
    bgUrl = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/run-migration';
    dispatchBody = { runId, scheduleId: schedule.id, userId };
  }

  try {
    // Short timeout: we just need the 202 ack, not the work to finish.
    const ctl = new AbortController();
    const to  = setTimeout(() => ctl.abort(), 8_000);
    const res = await fetch(bgUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-user-id': userId,
        // Server-to-server auth for the Azure runner. Set DISPATCH_KEY to
        // the same value in Netlify env and the Function App settings; the
        // runner enforces it once REQUIRE_TOKEN_AUTH is on over there.
        ...(process.env.DISPATCH_KEY ? { 'x-dispatch-key': process.env.DISPATCH_KEY } : {}),
      },
      body: JSON.stringify(dispatchBody),
      signal: ctl.signal,
    });
    clearTimeout(to);
    // A non-2xx ack (404/500/503 — deploy in progress, cold-start failure)
    // means the runner never started; without this check the run stayed
    // 'queued' forever with no failure surfaced in Run History.
    if (!res.ok) throw new Error(`Runner endpoint responded ${res.status}`);
  } catch (e) {
    // If the dispatch itself failed (DNS, network, abort), mark the run
    // failed synchronously so the dashboard's poll sees a stable state.
    const failedRun = {
      ...runDoc,
      status: 'failed',
      finishedAt: nowIso(),
      errorMessage: 'Could not dispatch background runner: ' + (e.message || e),
    };
    try { await containers.runs.item(runId, schedule.id).replace(failedRun); } catch {}
    return { __err: 'Could not dispatch background runner: ' + (e.message || e), __status: 502 };
  }

  return {
    runId,
    scheduleId: schedule.id,
    status: 'queued',
    startedAt,
    message: 'Run queued — background runner started. Poll get-run for completion.',
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

  // Identity from a VERIFIED Entra token only. The old x-user-id header was
  // client-supplied and let anyone read another user's schedules — which
  // include stored connection strings — by guessing their email.
  let userId;
  try {
    const authed = await verifyAuthHeader(event);
    userId = authed.email;
    if (!userId) throw new Error('Token contains no email claim');
  } catch (e) {
    return errorResponse(401, 'Auth error: ' + e.message);
  }

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
    'version-delete':  versionDelete,
    'run-now':         runNow,
    'get-notify-settings':  getNotifySettings,
    'save-notify-settings': saveNotifySettings,
    'list-notifications':   listNotifications,
  };
  const fn = handlers[action];
  if (!fn) return errorResponse(400, `Unknown action: ${action}`);

  try {
    const result = await fn(userId, body, containers, event);
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
    // Log the stack server-side only — returning it to clients leaks
    // internal paths and code structure.
    console.error('[scheduler]', e.stack || e.message);
    return errorResponse(500, e.message);
  }
};

// Exported for tests. Netlify only looks for `handler`, so this is inert in
// production — but the one-shot next-run rule is the thing that decides
// whether a schedule can fire at all, and it needs asserting directly.
module.exports.oneShotNextRun = oneShotNextRun;
// Likewise for the notification settings: they carry an SMTP password, and
// the rules about what is echoed back and what survives a partial save are
// the security surface of this feature.
module.exports.maskNotifySettings = maskNotifySettings;
module.exports.getNotifySettings  = getNotifySettings;
module.exports.saveNotifySettings = saveNotifySettings;
module.exports.listNotifications  = listNotifications;
module.exports.listSchedules      = listSchedules;
