// ═════════════════════════════════════════════════════════════════════════════
// Scheduler module — Task Agent
//
// Registers two things with the v4 programming model:
//   1. app.http('schedules', …) — /api/schedules/{action} multi-action dispatcher
//   2. app.timer('schedulerTick', …) — fires every minute, runs due schedules
//
// Reuses existing Cosmos containers where possible:
//   - `job_versions`   (existing — pinned SQL snapshots; already has version-create)
//   - `audit`          (existing — writes a row per run for auditability)
// And adds two new ones:
//   - `schedules`      partition /userId
//   - `schedule_runs`  partition /scheduleId
//
// SQL execution uses the same DefaultAzureCredential flow as /api/db/execute.
// No connection strings, no encryption — the Function App's managed identity
// already has access to the target SQL Server via env vars SQL_SERVER /
// SQL_DATABASE.
// ─────────────────────────────────────────────────────────────────────────────

const { app } = require('@azure/functions');

// ─── Cosmos (reuse the same lazy singleton pattern as index.js) ──────────────
let _cosmos = null;
function getCosmosContainer(containerName) {
  if (!_cosmos) {
    const { CosmosClient } = require('@azure/cosmos');
    _cosmos = new CosmosClient({
      endpoint: process.env.COSMOS_ENDPOINT,
      key:      process.env.COSMOS_KEY
    });
  }
  return _cosmos
    .database(process.env.COSMOS_DATABASE || 'cygenix')
    .container(containerName);
}

// ─── CORS / response helpers (match index.js) ────────────────────────────────
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, x-user-id',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
  'Content-Type': 'application/json'
};
const ok  = (body)      => ({ status: 200, headers: CORS, body: JSON.stringify(body) });
const err = (code, msg) => ({ status: code, headers: CORS, body: JSON.stringify({ error: msg }) });

function getUserId(req) {
  return req.headers.get('x-user-id') || req.query.get('userId') || null;
}
function nowIso() { return new Date().toISOString(); }
function newId(prefix) { return prefix + '_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2,10); }

// ═════════════════════════════════════════════════════════════════════════════
// Cron helper — 5-field parser + next-run calculator (UTC + timezone aware)
// ═════════════════════════════════════════════════════════════════════════════

function parseField(str, min, max) {
  if (str === '*') {
    const s = new Set();
    for (let i = min; i <= max; i++) s.add(i);
    return s;
  }
  const out = new Set();
  for (const part of String(str).split(',')) {
    const stepMatch = part.match(/^(.+)\/(\d+)$/);
    const step = stepMatch ? parseInt(stepMatch[2], 10) : 1;
    const rangePart = stepMatch ? stepMatch[1] : part;

    let lo, hi;
    if (rangePart === '*') { lo = min; hi = max; }
    else if (rangePart.includes('-')) {
      const [a, b] = rangePart.split('-').map(s => parseInt(s, 10));
      lo = a; hi = b;
    } else {
      const n = parseInt(rangePart, 10);
      if (Number.isNaN(n)) throw new Error('bad cron field: ' + str);
      lo = n; hi = n;
    }
    if (lo < min || hi > max || lo > hi) throw new Error('cron field out of range: ' + str);
    for (let v = lo; v <= hi; v += step) out.add(v);
  }
  return out;
}

function parseCron(expr) {
  const parts = String(expr).trim().split(/\s+/);
  if (parts.length !== 5) throw new Error('cron must have 5 fields');
  return {
    minute:     parseField(parts[0], 0, 59),
    hour:       parseField(parts[1], 0, 23),
    dayOfMonth: parseField(parts[2], 1, 31),
    month:      parseField(parts[3], 1, 12),
    dayOfWeek:  parseField(parts[4], 0, 6),
  };
}

function partsInTz(date, tz) {
  const fmt = new Intl.DateTimeFormat('en-GB', {
    timeZone: tz, hourCycle: 'h23',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', weekday: 'short',
  });
  const parts = {};
  for (const p of fmt.formatToParts(date)) parts[p.type] = p.value;
  const dowMap = { Sun:0, Mon:1, Tue:2, Wed:3, Thu:4, Fri:5, Sat:6 };
  return {
    year: +parts.year, month: +parts.month, day: +parts.day,
    hour: +parts.hour, minute: +parts.minute,
    dow: dowMap[parts.weekday],
  };
}

// Given a wall-clock in `tz`, find the UTC instant it represents.
// Handles DST gaps/overlaps by probing offsets.
function wallTimeToUtc(y, mo, d, h, mi, tz) {
  const guess = Date.UTC(y, mo - 1, d, h, mi);
  const probe = partsInTz(new Date(guess), tz);
  const wallMin  = y*525600 + (mo-1)*43800 + d*1440 + h*60 + mi;
  const probeMin = probe.year*525600 + (probe.month-1)*43800 + probe.day*1440 + probe.hour*60 + probe.minute;
  const deltaMs = (wallMin - probeMin) * 60000;
  const corrected = guess + deltaMs;
  const check = partsInTz(new Date(corrected), tz);
  if (check.year === y && check.month === mo && check.day === d && check.hour === h && check.minute === mi) {
    return corrected;
  }
  for (const shift of [-3600000, 3600000]) {
    const c2 = partsInTz(new Date(corrected + shift), tz);
    if (c2.year === y && c2.month === mo && c2.day === d && c2.hour === h && c2.minute === mi) {
      return corrected + shift;
    }
  }
  return corrected;
}

function nextRunAt(expr, tz, afterDate) {
  let c;
  try { c = parseCron(expr); } catch { return null; }
  tz = tz || 'UTC';
  const start = afterDate ? new Date(afterDate) : new Date();
  let probe = new Date(start.getTime() + 60000 - (start.getTime() % 60000));

  const MAX_MINUTES = 60 * 24 * 366 * 4;
  for (let i = 0; i < MAX_MINUTES; i++) {
    const p = partsInTz(probe, tz);
    if (c.minute.has(p.minute) &&
        c.hour.has(p.hour) &&
        c.dayOfMonth.has(p.day) &&
        c.month.has(p.month) &&
        c.dayOfWeek.has(p.dow)) {
      const utcMs = wallTimeToUtc(p.year, p.month, p.day, p.hour, p.minute, tz);
      return new Date(utcMs).toISOString();
    }
    probe = new Date(probe.getTime() + 60000);
  }
  return null;
}

function humanize(expr) {
  try {
    const parts = String(expr || '').trim().split(/\s+/);
    if (parts.length !== 5) return expr || '';
    const [mi, h, dom, mo, dow] = parts;
    if (mi === '*' && h === '*' && dom === '*' && mo === '*' && dow === '*') return 'Every minute';
    if (h === '*'  && dom === '*' && mo === '*' && dow === '*' && /^\d+$/.test(mi))   return `Every hour at :${mi.padStart(2,'0')}`;
    if (dom === '*' && mo === '*' && dow === '*' && /^\d+$/.test(mi) && /^\d+$/.test(h)) return `Daily at ${h.padStart(2,'0')}:${mi.padStart(2,'0')}`;
    if (dom === '*' && mo === '*' && /^\d+$/.test(mi) && /^\d+$/.test(h)) {
      const days = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
      const label = dow.split(',').map(n => days[+n] || n).join(',');
      return `${label} at ${h.padStart(2,'0')}:${mi.padStart(2,'0')}`;
    }
    return expr;
  } catch { return expr; }
}

// ═════════════════════════════════════════════════════════════════════════════
// Executor — load pinned version, run SQL via managed identity, record result
// ═════════════════════════════════════════════════════════════════════════════

async function runSqlWithManagedIdentity(sqlText, ctx) {
  const { DefaultAzureCredential } = require('@azure/identity');
  const sql = require('mssql');

  const credential = new DefaultAzureCredential();
  const tokenResp = await credential.getToken('https://database.windows.net/.default');

  const pool = await sql.connect({
    server: process.env.SQL_SERVER,
    database: process.env.SQL_DATABASE,
    options: { encrypt: true, trustServerCertificate: false, enableArithAbort: true },
    authentication: {
      type: 'azure-active-directory-access-token',
      options: { token: tokenResp.token }
    }
  });

  try {
    const r = await pool.request().query(sqlText);
    const total = Array.isArray(r.rowsAffected)
      ? r.rowsAffected.reduce((a, b) => a + (b || 0), 0)
      : (r.rowsAffected || 0);
    return { rowsAffected: total, recordsetCount: (r.recordsets || []).length };
  } finally {
    try { await pool.close(); } catch (e) { ctx && ctx.log && ctx.log('pool close error (non-fatal):', e.message); }
  }
}

// Execute one schedule. Writes the run doc, updates schedule.nextRunAt /
// lastRunAt, and if successful kicks any chained successors. Never throws
// past the top-level try so callers can keep processing other schedules.
async function executeSchedule(schedule, triggeredBy, parentRunId, ctx) {
  const runId = newId('run');
  const startedAt = nowIso();

  // 1) Create 'running' run doc up-front
  try {
    await getCosmosContainer('schedule_runs').items.create({
      id: runId,
      scheduleId: schedule.id,
      userId: schedule.userId,
      jobId: schedule.jobId,
      jobVersionId: schedule.jobVersionId,
      startedAt,
      finishedAt: null,
      status: 'running',
      rowsAffected: null,
      errorMessage: null,
      triggeredBy: triggeredBy || 'cron',
      parentRunId: parentRunId || null,
    });
  } catch (e) {
    ctx && ctx.log && ctx.log.error('could not create run doc:', e.message);
    return { runId, status: 'failed', errorMessage: 'Could not record run start: ' + e.message };
  }

  let status = 'failed';
  let errorMessage = null;
  let rowsAffected = null;

  try {
    // 2) Load pinned version from existing job_versions container
    const { resource: ver } = await getCosmosContainer('job_versions')
      .item(schedule.jobVersionId, schedule.jobId).read();
    if (!ver) throw new Error('pinned version not found: ' + schedule.jobVersionId);

    // The snapshot is whatever the version-create endpoint stored. Jobs in Cygenix
    // have `migrationSQL` (the insert/update pass) and `schemaSQL` (DDL). We run
    // migrationSQL for scheduled tasks — schemaSQL is a one-off and shouldn't
    // fire on a cron.
    const snap = ver.snapshot || {};
    const sqlText = snap.migrationSQL || snap.insertSQL || snap.sql || '';
    if (!String(sqlText).trim()) throw new Error('pinned version has no migrationSQL');

    // 3) Execute via managed identity
    const r = await runSqlWithManagedIdentity(sqlText, ctx);
    rowsAffected = r.rowsAffected;
    status = 'success';
  } catch (e) {
    errorMessage = (e && e.message) ? e.message : String(e);
    if (errorMessage.length > 4000) errorMessage = errorMessage.slice(0, 4000) + '…';
    ctx && ctx.log && ctx.log.error(`schedule ${schedule.id} failed:`, errorMessage);
  }

  const finishedAt = nowIso();

  // 4) Update run doc with final outcome
  try {
    await getCosmosContainer('schedule_runs').item(runId, schedule.id).replace({
      id: runId,
      scheduleId: schedule.id,
      userId: schedule.userId,
      jobId: schedule.jobId,
      jobVersionId: schedule.jobVersionId,
      startedAt,
      finishedAt,
      status,
      rowsAffected,
      errorMessage,
      triggeredBy: triggeredBy || 'cron',
      parentRunId: parentRunId || null,
    });
  } catch (e) {
    ctx && ctx.log && ctx.log('run doc update failed:', e.message);
  }

  // 5) Update schedule with lastRun + recompute nextRunAt (cron only)
  try {
    const { resource: fresh } = await getCosmosContainer('schedules').item(schedule.id, schedule.userId).read();
    if (fresh) {
      fresh.lastRunAt = finishedAt;
      fresh.lastRunStatus = status;
      if (fresh.cron && (!fresh.nextRunAt || new Date(fresh.nextRunAt) <= new Date(finishedAt))) {
        fresh.nextRunAt = nextRunAt(fresh.cron, fresh.timezone || 'UTC', new Date(finishedAt));
      }
      fresh.updatedAt = finishedAt;
      await getCosmosContainer('schedules').item(schedule.id, schedule.userId).replace(fresh);
    }
  } catch (e) {
    ctx && ctx.log && ctx.log('schedule update failed:', e.message);
  }

  // 6) Audit trail (best-effort, reuses existing `audit` container)
  try {
    await getCosmosContainer('audit').items.create({
      id:        `${schedule.userId}-run-${Date.now()}`,
      userId:    schedule.userId,
      action:    'schedule-run',
      scheduleId: schedule.id,
      jobId:     schedule.jobId,
      status,
      rowsAffected,
      triggeredBy: triggeredBy || 'cron',
      timestamp: finishedAt,
    });
  } catch {}

  // 7) On success, kick any chained successors
  if (status === 'success') {
    try {
      const q = {
        query: 'SELECT * FROM c WHERE c.userId = @uid AND c.chainAfter = @sid AND c.enabled = true',
        parameters: [
          { name: '@uid', value: schedule.userId },
          { name: '@sid', value: schedule.id },
        ],
      };
      const { resources } = await getCosmosContainer('schedules').items.query(q, { partitionKey: schedule.userId }).fetchAll();
      for (const child of resources) {
        child.nextRunAt = nowIso();
        child._chainParentRunId = runId;
        child.updatedAt = nowIso();
        await getCosmosContainer('schedules').item(child.id, child.userId).replace(child);
      }
    } catch (e) {
      ctx && ctx.log && ctx.log('chain advance failed:', e.message);
    }
  }

  return { runId, status, rowsAffected, errorMessage };
}

// ═════════════════════════════════════════════════════════════════════════════
// HTTP route: /api/schedules/{action}
// ═════════════════════════════════════════════════════════════════════════════

app.http('schedules', {
  methods: ['GET', 'POST', 'DELETE', 'OPTIONS'],
  authLevel: 'function',
  route: 'schedules/{action}',
  handler: async (req, ctx) => {

    if (req.method === 'OPTIONS') return { status: 200, headers: CORS, body: '' };

    if (!process.env.COSMOS_ENDPOINT || !process.env.COSMOS_KEY) {
      return err(500, 'Cosmos DB not configured');
    }

    const userId = getUserId(req);
    if (!userId) return err(401, 'x-user-id header is required');

    const action = req.params.action;
    ctx.log(`schedules/${action} for user: ${userId}`);

    let body = {};
    if (req.method === 'POST' || req.method === 'DELETE') {
      try { body = await req.json(); } catch {}
    }

    try {
      switch (action) {

        // ── LIST-SCHEDULES ─────────────────────────────────────────────────
        case 'list-schedules': {
          const q = {
            query: 'SELECT * FROM c WHERE c.userId = @uid ORDER BY c.createdAt DESC',
            parameters: [{ name: '@uid', value: userId }]
          };
          const { resources } = await getCosmosContainer('schedules').items
            .query(q, { partitionKey: userId }).fetchAll();

          // Enrich with the version number by looking up referenced job_versions.
          // One query per distinct jobId — volumes are small in practice.
          const jobIds = [...new Set(resources.map(r => r.jobId).filter(Boolean))];
          const verMap = {};
          for (const jid of jobIds) {
            try {
              const { resources: vers } = await getCosmosContainer('job_versions').items
                .query({
                  query: 'SELECT c.id, c.version FROM c WHERE c.jobId = @jid',
                  parameters: [{ name: '@jid', value: jid }]
                }, { partitionKey: jid }).fetchAll();
              for (const v of vers) verMap[v.id] = v.version;
            } catch {}
          }

          return ok({ schedules: resources.map(s => {
            const { _rid, _self, _etag, _attachments, _ts, claimedUntil, _chainParentRunId, ...rest } = s;
            return {
              ...rest,
              humanReadable: humanize(s.cron),
              jobVersionNumber: verMap[s.jobVersionId] || null,
            };
          })});
        }

        // ── CREATE-SCHEDULE ────────────────────────────────────────────────
        case 'create-schedule': {
          const { name, jobId, jobVersionId, cron, timezone, chainAfter, enabled } = body;
          if (!name || !jobId || !jobVersionId) return err(400, 'missing required fields');
          if (!chainAfter && !cron) return err(400, 'must provide cron or chainAfter');
          if (cron) {
            try { parseCron(cron); } catch (e) { return err(400, 'invalid cron: ' + e.message); }
          }

          const id = newId('sch');
          const doc = {
            id, userId, name, jobId, jobVersionId,
            cron:       cron || null,
            timezone:   timezone || 'UTC',
            chainAfter: chainAfter || null,
            enabled:    enabled !== false,
            nextRunAt:  cron ? nextRunAt(cron, timezone || 'UTC') : null,
            lastRunAt:  null,
            lastRunStatus: null,
            createdAt:  nowIso(),
            updatedAt:  nowIso(),
          };
          await getCosmosContainer('schedules').items.create(doc);
          return ok({ id, nextRunAt: doc.nextRunAt });
        }

        // ── UPDATE-SCHEDULE ────────────────────────────────────────────────
        case 'update-schedule': {
          const { id, patch } = body;
          if (!id || !patch) return err(400, 'missing id/patch');

          const { resource: existing } = await getCosmosContainer('schedules').item(id, userId).read();
          if (!existing) return err(404, 'schedule not found');

          const allowed = ['name','cron','timezone','chainAfter','enabled','jobVersionId'];
          for (const k of allowed) if (k in patch) existing[k] = patch[k];

          if ('cron' in patch || 'timezone' in patch) {
            if (existing.cron) {
              try { parseCron(existing.cron); } catch (e) { return err(400, 'invalid cron: ' + e.message); }
              existing.nextRunAt = nextRunAt(existing.cron, existing.timezone || 'UTC');
            } else {
              existing.nextRunAt = null;
            }
          }
          if (patch.enabled === true && existing.cron && !existing.nextRunAt) {
            existing.nextRunAt = nextRunAt(existing.cron, existing.timezone || 'UTC');
          }
          existing.updatedAt = nowIso();
          await getCosmosContainer('schedules').item(id, userId).replace(existing);
          return ok({ ok: true });
        }

        // ── DELETE-SCHEDULE ────────────────────────────────────────────────
        case 'delete-schedule': {
          const { id } = body;
          if (!id) return err(400, 'missing id');
          await getCosmosContainer('schedules').item(id, userId).delete();
          return ok({ ok: true });
        }

        // ── TOGGLE-ENABLED ─────────────────────────────────────────────────
        case 'toggle-enabled': {
          const { id, enabled } = body;
          if (!id) return err(400, 'missing id');
          const { resource: s } = await getCosmosContainer('schedules').item(id, userId).read();
          if (!s) return err(404, 'schedule not found');
          s.enabled = !!enabled;
          if (s.enabled && s.cron && !s.nextRunAt) {
            s.nextRunAt = nextRunAt(s.cron, s.timezone || 'UTC');
          }
          s.updatedAt = nowIso();
          await getCosmosContainer('schedules').item(id, userId).replace(s);
          return ok({ ok: true, nextRunAt: s.nextRunAt });
        }

        // ── RUN-NOW ────────────────────────────────────────────────────────
        // Synchronous execution. Azure Functions default timeout is 230s
        // (functionTimeout in host.json), so this is fine for most migrations.
        // For very long-running jobs, set nextRunAt = now and let the timer
        // pick it up (use action 'run-later' instead).
        case 'run-now': {
          const { id } = body;
          if (!id) return err(400, 'missing id');
          const { resource: s } = await getCosmosContainer('schedules').item(id, userId).read();
          if (!s) return err(404, 'schedule not found');
          const r = await executeSchedule(s, 'manual', null, ctx);
          return ok(r);
        }

        // ── RUN-LATER (async-style fire via timer) ─────────────────────────
        case 'run-later': {
          const { id } = body;
          if (!id) return err(400, 'missing id');
          const { resource: s } = await getCosmosContainer('schedules').item(id, userId).read();
          if (!s) return err(404, 'schedule not found');
          s.nextRunAt = nowIso();
          s.updatedAt = nowIso();
          await getCosmosContainer('schedules').item(id, userId).replace(s);
          return ok({ scheduled: true, pickupWithinSeconds: 60 });
        }

        // ── LIST-RUNS ──────────────────────────────────────────────────────
        case 'list-runs': {
          const { scheduleId, limit } = body;
          const lim = Math.min(parseInt(limit, 10) || 50, 200);
          let q;
          if (scheduleId) {
            q = {
              query: 'SELECT TOP @lim * FROM c WHERE c.userId = @uid AND c.scheduleId = @sid ORDER BY c.startedAt DESC',
              parameters: [
                { name: '@uid', value: userId },
                { name: '@sid', value: scheduleId },
                { name: '@lim', value: lim },
              ],
            };
            // Can use partition key since scheduleId is the partition
            const { resources } = await getCosmosContainer('schedule_runs').items
              .query(q, { partitionKey: scheduleId }).fetchAll();
            return ok({ runs: resources.map(stripMeta) });
          }
          // Cross-partition query when listing for all schedules
          q = {
            query: 'SELECT TOP @lim * FROM c WHERE c.userId = @uid ORDER BY c.startedAt DESC',
            parameters: [
              { name: '@uid', value: userId },
              { name: '@lim', value: lim },
            ],
          };
          const { resources } = await getCosmosContainer('schedule_runs').items.query(q).fetchAll();
          return ok({ runs: resources.map(stripMeta) });
        }

        // ── GET-RUN ────────────────────────────────────────────────────────
        case 'get-run': {
          const { id, scheduleId } = body;
          if (!id || !scheduleId) return err(400, 'missing id/scheduleId');
          const { resource } = await getCosmosContainer('schedule_runs').item(id, scheduleId).read();
          if (!resource || resource.userId !== userId) return err(404, 'run not found');
          return ok({ run: resource });
        }

        default:
          return err(404, `Unknown action: ${action}. Valid actions: list-schedules, create-schedule, update-schedule, delete-schedule, toggle-enabled, run-now, run-later, list-runs, get-run`);
      }
    } catch (e) {
      ctx.log.error('schedules error:', e.message, e.code, e.stack?.split('\n').slice(0,3).join(' | '));
      return err(500, `scheduler error: ${e.message}`);
    }
  }
});

function stripMeta(r) {
  const { _rid, _self, _etag, _attachments, _ts, ...rest } = r;
  return rest;
}

// ═════════════════════════════════════════════════════════════════════════════
// Timer trigger: schedulerTick — fires every minute
// ═════════════════════════════════════════════════════════════════════════════

// NCRONTAB format (Azure Functions): {second} {minute} {hour} {day} {month} {dayOfWeek}
// "0 */1 * * * *" = every minute at the :00 second mark
const TICK_SCHEDULE = '0 */1 * * * *';
const LEASE_MS = 90 * 1000;

app.timer('schedulerTick', {
  schedule: TICK_SCHEDULE,
  handler: async (myTimer, ctx) => {
    const now = new Date();
    const nowIsoStr = now.toISOString();
    const leaseUntil = new Date(now.getTime() + LEASE_MS).toISOString();

    if (myTimer && myTimer.isPastDue) ctx.log('schedulerTick: past due');

    let due = [];
    try {
      const q = {
        query: `SELECT * FROM c
                WHERE c.enabled = true
                  AND c.nextRunAt != null
                  AND c.nextRunAt <= @now
                  AND (NOT IS_DEFINED(c.claimedUntil) OR c.claimedUntil = null OR c.claimedUntil <= @now)`,
        parameters: [{ name: '@now', value: nowIsoStr }],
      };
      const { resources } = await getCosmosContainer('schedules').items.query(q).fetchAll();
      due = resources || [];
    } catch (e) {
      ctx.log.error('schedulerTick: query failed:', e.message);
      return;
    }

    if (!due.length) { ctx.log('schedulerTick: 0 schedule(s) due'); return; }
    ctx.log(`schedulerTick: ${due.length} schedule(s) due`);

    for (const s of due) {
      // Claim the lease (optimistic concurrency via etag). If another tick
      // already claimed it, skip.
      try {
        s.claimedUntil = leaseUntil;
        await getCosmosContainer('schedules').item(s.id, s.userId).replace(s, {
          accessCondition: { type: 'IfMatch', condition: s._etag },
        });
      } catch (e) {
        ctx.log(`schedulerTick: couldn't claim ${s.id} (etag mismatch) — skipping`);
        continue;
      }

      const triggeredBy = s._chainParentRunId ? 'chain' : 'cron';
      const parentRunId = s._chainParentRunId || null;

      try {
        const r = await executeSchedule(s, triggeredBy, parentRunId, ctx);
        ctx.log(`schedulerTick: ${s.id} -> ${r.status}`);
      } catch (e) {
        ctx.log.error(`schedulerTick: executor threw for ${s.id}:`, e.message);
      }

      // Clear lease + chain marker
      try {
        const { resource: fresh } = await getCosmosContainer('schedules').item(s.id, s.userId).read();
        if (fresh) {
          fresh._chainParentRunId = null;
          fresh.claimedUntil = null;
          await getCosmosContainer('schedules').item(s.id, s.userId).replace(fresh);
        }
      } catch {}
    }
  }
});
