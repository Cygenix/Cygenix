// azure-function/src/profile-builder.js
//
// /api/profile-build — server-side runner for Data Profiling overnight builds.
//
// ── Why this exists ───────────────────────────────────────────────────────
// The browser-side Data Profiling build in insights.html walks a 9k-table
// schema in 5–15 minutes for a real Elite-style database. That's fine if
// the user can sit and wait; for overnight builds where they want to close
// their laptop and pick up results in the morning we need a server runner.
//
// Same shape as run-migration.js: fire-and-forget POST returns 202 with a
// taskId, the work continues asynchronously on Flex Consumption (no
// execution time limit), task progress streams to the data_profile_tasks
// Cosmos container, the frontend polls profile-task-active to know what's
// happening, and on completion the user gets an email via notify.js.
//
// ── Pipeline (mirrors dpStartBuild in insights.html) ──────────────────────
//   STAGE 1: fingerprint    — SHA-256 of (database + sorted table list)
//   STAGE 2: graph          — declared FKs from /api/db schema-fks
//   STAGE 3: classify       — heuristic tiering (hub/spoke/lookup/audit/staging)
//   STAGE 4: deep            — per-hub Claude analysis (samples + stats + AI)
//   STAGE 5: light          — batched Claude analysis of lookups
//   STAGE 6: heuristic       — templated descriptions for audit/staging
//   STAGE 7: overview        — schema-wide synthesis (1 Claude call)
//   STAGE 8: save            — write profile docs to data_profiles
//
// ── Env vars required ─────────────────────────────────────────────────────
//   COSMOS_ENDPOINT, COSMOS_KEY                — already set
//   ANTHROPIC_API_KEY                          — already set, used by narrative
//   (No DB-connection env vars needed — the runner uses the connection
//    string the caller passes through, hitting /api/db like the browser does.)

const { app } = require('@azure/functions');

// Reuse the existing notify helper. Optional — if notify.js isn't loaded,
// we still complete successfully, just without the email.
let _notifier = null;
try { _notifier = require('./notify'); } catch (e) { /* notify.js not present */ }

// ── Cosmos lazy singleton (matches index.js pattern) ─────────────────────
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

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, x-user-id',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type': 'application/json'
};
const ok  = (body)      => ({ status: 200, headers: CORS, body: JSON.stringify(body) });
const err = (code, msg) => ({ status: code, headers: CORS, body: JSON.stringify({ error: msg }) });

// ─────────────────────────────────────────────────────────────────────────
// ROUTE: POST /api/profile-build
// ─────────────────────────────────────────────────────────────────────────
//
// Request body: {
//   role:        "source" | "target",
//   conn:        "<full connection string, mssql:// URL or Azure Function URL>",
//   database:    "<the database name from schema response>",
//   fingerprint: "<32-hex schema fingerprint, computed client-side>",
//   notifyEmail: "<email to send completion notice to>"
// }
//
// Behaviour: validates input, creates a task doc in data_profile_tasks,
// returns 202 with taskId immediately, then fires off the async work
// inline via setImmediate so the HTTP response can flush before the runner
// starts hammering the source DB.
//
// This works on Flex Consumption because the host doesn't kill the
// invocation when the HTTP response is sent — it waits for any pending
// async work to complete. Same trick run-migration.js uses.
app.http('profile-build', {
  methods: ['POST', 'OPTIONS'],
  authLevel: 'function',
  route: 'profile-build',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 200, headers: CORS, body: '' };

    const userId = req.headers.get('x-user-id') || req.query.get('userId');
    if (!userId) return err(401, 'x-user-id header is required');

    const body = await req.json().catch(() => null);
    if (!body) return err(400, 'JSON body required');
    const { role, conn, database, fingerprint, notifyEmail } = body;
    if (role !== 'source' && role !== 'target') return err(400, 'role must be source or target');
    if (!conn)        return err(400, 'conn is required');
    if (!fingerprint) return err(400, 'fingerprint is required');
    if (!database)    return err(400, 'database is required');

    // De-dup: if there's already an active task for (userId, role, fingerprint), return it
    try {
      const { resources } = await getCosmosContainer('data_profile_tasks').items
        .query({
          query: `SELECT TOP 1 * FROM c
                  WHERE c.userId = @uid AND c.role = @role AND c.fingerprint = @fp
                    AND (c.status = 'queued' OR c.status = 'running')
                  ORDER BY c.startedAt DESC`,
          parameters: [
            { name: '@uid',  value: userId      },
            { name: '@role', value: role        },
            { name: '@fp',   value: fingerprint }
          ]
        }, { partitionKey: userId })
        .fetchAll();
      if (resources.length) {
        ctx.log(`profile-build: returning existing active task ${resources[0].id}`);
        return ok({ taskId: resources[0].id, status: resources[0].status, existing: true });
      }
    } catch (e) {
      ctx.log('profile-build dedup check failed (non-fatal):', e.message);
    }

    // Create the task doc
    const now = new Date().toISOString();
    const taskId = 'task_' + userId.replace(/[^a-z0-9]/gi, '_') + '_' + role + '_' + Date.now();
    const task = {
      id:           taskId,
      userId,
      role,
      database,
      fingerprint,
      status:       'queued',
      stage:        'fingerprint',
      stageDetail:  'starting…',
      percent:      0,
      error:        null,
      startedAt:    now,
      updatedAt:    now,
      finishedAt:   null,
      notifyEmail:  notifyEmail || userId,
      tableCount:   0,
      hubsSaved:    0,
      lookupsSaved: 0
    };
    try {
      await getCosmosContainer('data_profile_tasks').items.create(task);
    } catch (e) {
      ctx.log.error('profile-build: failed to create task doc:', e.message);
      return err(500, 'Failed to queue task: ' + e.message);
    }

    // Fire async work
    setImmediate(() => {
      runProfileBuild(task, conn, ctx).catch(e => {
        ctx.log.error('profile-build runProfileBuild crashed:', e.message, e.stack?.split('\n').slice(0,3).join(' | '));
      });
    });

    return { status: 202, headers: CORS, body: JSON.stringify({ taskId, status: 'queued' }) };
  }
});

// ─────────────────────────────────────────────────────────────────────────
// Helper: hit /api/db via Netlify proxy with a body payload
// ─────────────────────────────────────────────────────────────────────────
// Mirrors the dbCall pattern from insights.html. The Function App can reach
// the Netlify endpoint at https://cygenix.co.uk/api/db — same as the
// browser would, but with no cookies and no MSAL session. /api/db doesn't
// require auth (it's a thin DB proxy), so this works.
//
// If the conn starts with https://, we hit it directly (it's an Azure
// Function URL with ?code=KEY) and proxy through to the underlying SQL.
// Otherwise we hit /api/db with connectionString in the body.
async function dbCall(conn, body, ctx) {
  const isFn = /^https?:\/\//i.test(conn);
  const url  = isFn ? conn : 'https://cygenix.co.uk/api/db';
  const payload = isFn ? body : Object.assign({}, body, { connectionString: conn });

  const res = await fetch(url, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify(payload),
    signal:  AbortSignal.timeout(120000)   // 2 min — schema-fks on huge DBs can take a while
  });
  const data = await res.json().catch(() => ({ error: 'Non-JSON response (' + res.status + ')' }));
  if (!res.ok) throw new Error(data.error || ('HTTP ' + res.status));
  if (data && data.success === false && data.error) throw new Error(data.error);
  return data;
}

// ─────────────────────────────────────────────────────────────────────────
// Helper: update task progress in Cosmos
// ─────────────────────────────────────────────────────────────────────────
// Throttled to once every 1.5s — sufficient for the frontend pill to feel
// live without flooding Cosmos with writes during the per-hub deep stage.
async function updateTask(task, patch, ctx, force = false) {
  Object.assign(task, patch);
  task.updatedAt = new Date().toISOString();
  const since = Date.now() - (task._lastWriteMs || 0);
  if (!force && since < 1500) return;
  task._lastWriteMs = Date.now();
  try {
    // strip private _lastWriteMs before writing
    const toSave = Object.assign({}, task);
    delete toSave._lastWriteMs;
    await getCosmosContainer('data_profile_tasks').items.upsert(toSave);
  } catch (e) {
    ctx.log('task update failed (non-fatal):', e.message);
  }
}

// Check if the user requested cancellation between stages. Returns true if
// the task is in 'cancelling' state. We refresh from Cosmos to pick up the
// flag the cancel endpoint set.
async function checkCancelled(task, ctx) {
  try {
    const { resource } = await getCosmosContainer('data_profile_tasks')
      .item(task.id, task.userId).read();
    if (resource && resource.status === 'cancelling') {
      return true;
    }
  } catch (e) { /* ignore */ }
  return false;
}

// ─────────────────────────────────────────────────────────────────────────
// MAIN BUILD PIPELINE
// ─────────────────────────────────────────────────────────────────────────
async function runProfileBuild(task, conn, ctx) {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    await finishTask(task, 'failed', { error: 'ANTHROPIC_API_KEY not configured on the Function App' }, ctx);
    return;
  }

  await updateTask(task, { status: 'running', stage: 'fingerprint', percent: 2, stageDetail: 'fetching schema…' }, ctx, true);
  ctx.log(`profile-build ${task.id}: starting for ${task.role}/${task.database}`);

  try {
    // ─── STAGE 1: fingerprint (already done client-side, just persist) ──
    // The client computed the fingerprint and passed it in. We trust it
    // because the runner doesn't have the schema yet at this point.
    await updateTask(task, { stage: 'fingerprint', percent: 5, stageDetail: task.fingerprint.slice(0,12) + '…' }, ctx, true);

    // ─── Fetch schema (paginated path always — server-side, large DBs) ─
    let schemaRes;
    try {
      schemaRes = await dbCall(conn, { action: 'schema-tables' }, ctx);
    } catch (e) {
      // Fallback to legacy schema action for small DBs
      if (/Unknown action/i.test(e.message)) {
        schemaRes = await dbCall(conn, { action: 'schema' }, ctx);
      } else {
        throw new Error('Failed to load schema-tables: ' + e.message);
      }
    }
    const rawTables = schemaRes.tables || [];

    if (await checkCancelled(task, ctx)) {
      await finishTask(task, 'cancelled', { stageDetail: 'cancelled before graph' }, ctx);
      return;
    }

    // ─── STAGE 2: build relationship graph ─────────────────────────────
    await updateTask(task, { stage: 'graph', percent: 8, stageDetail: 'fetching FK edges…' }, ctx, true);

    let inboundGraph = {};
    let outboundByTable = {};
    try {
      const fkRes = await dbCall(conn, { action: 'schema-fks' }, ctx);
      const edges = fkRes.foreignKeys || [];
      for (const e of edges) {
        const fromKey = e.fromSchema + '.' + e.fromTable;
        const toKey   = e.toSchema   + '.' + e.toTable;
        (inboundGraph[toKey] ||= []).push({
          fromSchema: e.fromSchema,
          fromTable:  e.fromTable,
          fromColumn: e.fromColumn,
          toColumn:   e.toColumn,
          kind:       'declared'
        });
        (outboundByTable[fromKey] ||= []).push({
          column:     e.fromColumn,
          refSchema:  e.toSchema,
          refTable:   e.toTable,
          refColumn:  e.toColumn,
          kind:       'declared',
          confidence: 'declared'
        });
      }
    } catch (e) {
      ctx.log('schema-fks unavailable, FK graph will be empty:', e.message);
    }
    const totalFKs = Object.values(inboundGraph).reduce((s,a)=>s+a.length, 0);
    await updateTask(task, { stage: 'graph', percent: 12, stageDetail: totalFKs + ' FKs across ' + Object.keys(inboundGraph).length + ' tables' }, ctx, true);

    if (await checkCancelled(task, ctx)) {
      await finishTask(task, 'cancelled', { stageDetail: 'cancelled before classify' }, ctx);
      return;
    }

    // ─── STAGE 3: classify ─────────────────────────────────────────────
    await updateTask(task, { stage: 'classify', percent: 15, stageDetail: 'tiering ' + rawTables.length + ' tables…', tableCount: rawTables.length }, ctx, true);

    const tables = rawTables.map(t => {
      const fullName = (t.schema || 'dbo') + '.' + t.name;
      const inboundCount = (inboundGraph[fullName] || []).length;
      const declaredFKs  = outboundByTable[fullName] || [];
      const tForClassify = {
        name:        t.name,
        rowCount:    t.rowCount || 0,
        columns:     t.columns || [],
        foreignKeys: declaredFKs.map(fk => ({ column: fk.column }))
      };
      return {
        fullName,
        schema:       t.schema || 'dbo',
        name:         t.name,
        rowCount:     t.rowCount || 0,
        columns:      t.columns || null,
        declaredFKs,
        inboundRefs:  inboundGraph[fullName] || [],
        tier:         classifyTier(tForClassify, inboundCount)
      };
    });
    const tierCounts = tables.reduce((acc,t)=>{ acc[t.tier]=(acc[t.tier]||0)+1; return acc; }, {});
    await updateTask(task, { percent: 18, stageDetail: Object.entries(tierCounts).map(([k,v])=>k+':'+v).join(' · ') }, ctx, true);

    // ─── STAGE 4: deep-analyse hubs ────────────────────────────────────
    const hubs = tables.filter(t => t.tier === 'hub');
    await updateTask(task, { stage: 'deep', percent: 20, stageDetail: '0 / ' + hubs.length + ' hubs' }, ctx, true);
    let hubsDone = 0;
    for (const hub of hubs) {
      if (hubsDone % 5 === 0 && await checkCancelled(task, ctx)) {
        await finishTask(task, 'cancelled', { stageDetail: 'cancelled at hub ' + hubsDone + '/' + hubs.length }, ctx);
        return;
      }
      try {
        await ensureColumns(hub, conn, ctx);
      } catch (e) { ctx.log('ensureColumns failed for', hub.fullName, e.message); hub.columns = hub.columns || []; }
      try {
        const desc = await deepAnalyseTable(hub, conn, apiKey, tables, ctx);
        Object.assign(hub, desc);
        hub.analysisDepth = 'deep';
      } catch (e) {
        ctx.log('deep analyse failed for', hub.fullName, e.message);
        hub.summary = 'Analysis failed: ' + e.message;
        hub.analysisDepth = 'failed';
      }
      hubsDone++;
      // hubs span 20% → 55%
      const pct = 20 + Math.round((hubsDone / Math.max(1, hubs.length)) * 35);
      await updateTask(task, { percent: pct, stageDetail: hubsDone + ' / ' + hubs.length + ' · ' + hub.name, hubsSaved: hubsDone }, ctx);
    }
    await updateTask(task, { percent: 55, stageDetail: hubsDone + ' hubs complete' }, ctx, true);

    // ─── STAGE 5: batch-analyse lookups ────────────────────────────────
    const lookups = tables.filter(t => t.tier === 'lookup');
    const BATCH = 30;
    const batches = [];
    for (let i = 0; i < lookups.length; i += BATCH) batches.push(lookups.slice(i, i + BATCH));
    await updateTask(task, { stage: 'light', percent: 56, stageDetail: '0 / ' + batches.length + ' batches' }, ctx, true);
    let batchesDone = 0;
    for (const batch of batches) {
      if (await checkCancelled(task, ctx)) {
        await finishTask(task, 'cancelled', { stageDetail: 'cancelled at lookup batch ' + batchesDone + '/' + batches.length }, ctx);
        return;
      }
      for (const b of batch) {
        if (!b.columns || !b.columns.length) {
          try { await ensureColumns(b, conn, ctx); } catch { b.columns = b.columns || []; }
        }
      }
      try {
        const results = await lightAnalyseBatch(batch, apiKey, ctx);
        for (const r of results) {
          const tgt = batch.find(b => b.fullName === r.fullName);
          if (tgt) {
            tgt.summary        = r.summary || ('Lookup table (' + tgt.rowCount + ' rows)');
            tgt.keyFields      = r.keyFields || [];
            tgt.patterns       = r.patterns || [];
            tgt.migrationNotes = r.migrationNotes || [];
            tgt.analysisDepth  = 'light';
          }
        }
      } catch (e) {
        ctx.log('light batch failed:', e.message);
        for (const b of batch) {
          if (!b.summary) {
            b.summary = 'Lookup/reference table — ' + b.rowCount + ' rows, ' + ((b.columns||[]).length) + ' columns.';
            b.analysisDepth = 'heuristic';
          }
        }
      }
      batchesDone++;
      const pct = 56 + Math.round((batchesDone / Math.max(1, batches.length)) * 19);
      await updateTask(task, { percent: pct, stageDetail: batchesDone + ' / ' + batches.length + ' batches', lookupsSaved: Math.min(lookups.length, batchesDone * BATCH) }, ctx);
    }
    await updateTask(task, { percent: 76, stageDetail: lookups.length + ' lookups complete' }, ctx, true);

    // ─── STAGE 6: heuristic-only for audit / staging / un-analysed ──────
    await updateTask(task, { stage: 'heuristic', percent: 78, stageDetail: 'documenting audit/staging…' }, ctx, true);
    let heuristicCount = 0;
    for (const t of tables) {
      if (t.summary) continue;
      t.summary = heuristicSummary(t);
      t.keyFields = [];
      t.patterns = [];
      t.migrationNotes = t.tier === 'audit'   ? ['Audit / log data — confirm retention policy before migrating.']
                       : t.tier === 'staging' ? ['Staging / temporary table — likely excluded from migration.']
                       : [];
      t.analysisDepth = 'heuristic';
      heuristicCount++;
    }
    await updateTask(task, { percent: 80, stageDetail: heuristicCount + ' tables' }, ctx, true);

    if (await checkCancelled(task, ctx)) {
      await finishTask(task, 'cancelled', { stageDetail: 'cancelled before overview' }, ctx);
      return;
    }

    // ─── STAGE 7: schema overview synthesis ─────────────────────────────
    await updateTask(task, { stage: 'overview', percent: 82, stageDetail: 'synthesising overview…' }, ctx, true);
    let overviewText, overviewExtras;
    try {
      const r = await synthesiseOverview(task.database, tables, tierCounts, apiKey, ctx);
      overviewText   = r.overview;
      overviewExtras = r.extras || {};
    } catch (e) {
      ctx.log('overview synthesis failed:', e.message);
      overviewText = task.database + ' database with ' + tables.length + ' tables (' +
                     Object.entries(tierCounts).map(([k,v])=>v+' '+k).join(', ') + '). Overview synthesis failed: ' + e.message;
      overviewExtras = {};
    }
    await updateTask(task, { percent: 90, stageDetail: 'overview complete' }, ctx, true);

    // ─── STAGE 8: save to data_profiles ─────────────────────────────────
    await updateTask(task, { stage: 'save', percent: 92, stageDetail: 'saving profile docs…' }, ctx, true);

    const now = new Date().toISOString();
    const overviewDoc = {
      id:           task.userId + '_' + task.fingerprint + '__overview',
      userId:       task.userId,
      fingerprint:  task.fingerprint,
      role:         task.role,
      database:     task.database,
      tableCount:   tables.length,
      tierCounts,
      overview:     overviewText,
      hubs:         tables.filter(t=>t.tier==='hub').map(t=>t.fullName),
      orphans:      tables.filter(t=>t.inboundRefs.length===0 && t.declaredFKs.length===0).map(t=>t.fullName),
      ...overviewExtras,
      createdAt:    now,
      updatedAt:    now,
      builtBy:      'server'   // distinguishes overnight builds from browser builds in audit
    };

    const tableDocs = tables.map(t => ({
      id:             task.userId + '_' + task.fingerprint + '_' + t.fullName,
      userId:         task.userId,
      fingerprint:    task.fingerprint,
      role:           task.role,
      database:       task.database,
      schema:         t.schema,
      table:          t.name,
      fullName:       t.fullName,
      tier:           t.tier,
      rowCount:       t.rowCount,
      columns:        t.columns || [],
      declaredFKs:    t.declaredFKs,
      inferredFKs:    t.inferredFKs || [],
      inboundRefs:    t.inboundRefs,
      summary:        t.summary,
      keyFields:      t.keyFields || [],
      patterns:       t.patterns || [],
      migrationNotes: t.migrationNotes || [],
      analysisDepth:  t.analysisDepth || 'heuristic',
      createdAt:      now,
      updatedAt:      now,
      builtBy:        'server'
    }));

    // Delete any prior fingerprint for this (userId, role, database)
    try {
      const { resources } = await getCosmosContainer('data_profiles').items
        .query({
          query: `SELECT c.id, c.fingerprint FROM c WHERE c.userId = @uid AND ENDSWITH(c.id, '__overview') AND c.role = @role AND c.database = @db AND c.fingerprint != @fp`,
          parameters: [
            { name: '@uid',  value: task.userId    },
            { name: '@role', value: task.role      },
            { name: '@db',   value: task.database  },
            { name: '@fp',   value: task.fingerprint }
          ]
        }, { partitionKey: task.userId }).fetchAll();
      // Each result's fingerprint identifies a stale set — wipe their docs
      const staleFps = [...new Set(resources.map(r => r.fingerprint))];
      for (const fp of staleFps) {
        const idPrefix = task.userId + '_' + fp;
        const { resources: staleDocs } = await getCosmosContainer('data_profiles').items
          .query({
            query: 'SELECT c.id FROM c WHERE c.userId = @uid AND STARTSWITH(c.id, @prefix)',
            parameters: [{ name: '@uid', value: task.userId }, { name: '@prefix', value: idPrefix }]
          }, { partitionKey: task.userId }).fetchAll();
        for (const d of staleDocs) {
          try { await getCosmosContainer('data_profiles').item(d.id, task.userId).delete(); } catch {}
        }
      }
    } catch (e) {
      ctx.log('stale cleanup skipped:', e.message);
    }

    // Save in batches of 50 (Cosmos throughput friendliness)
    const allDocs = [overviewDoc, ...tableDocs];
    let saved = 0;
    for (let i = 0; i < allDocs.length; i += 50) {
      const chunk = allDocs.slice(i, i + 50);
      for (const d of chunk) {
        try { await getCosmosContainer('data_profiles').items.upsert(d); saved++; }
        catch (e) { ctx.log('failed to save', d.id, ':', e.message); }
      }
      const pct = 92 + Math.round((saved / allDocs.length) * 7);
      await updateTask(task, { percent: pct, stageDetail: saved + ' / ' + allDocs.length }, ctx);
    }

    await finishTask(task, 'succeeded', {
      stageDetail: saved + ' docs saved',
      tableCount:  tables.length,
      tierCounts
    }, ctx);

  } catch (e) {
    ctx.log.error('profile-build pipeline failed:', e.message, e.stack?.split('\n').slice(0,3).join(' | '));
    await finishTask(task, 'failed', { error: e.message }, ctx);
  }
}

// ─────────────────────────────────────────────────────────────────────────
// Finalise + notify
// ─────────────────────────────────────────────────────────────────────────
async function finishTask(task, status, patch, ctx) {
  const now = new Date().toISOString();
  await updateTask(task, Object.assign({
    status,
    finishedAt: now,
    percent:    status === 'succeeded' ? 100 : task.percent || 0
  }, patch || {}), ctx, true);

  // Send completion email (best-effort, never throws)
  if (_notifier && _notifier.sendNotification && task.notifyEmail) {
    try {
      const type = status === 'succeeded' ? 'profile-success' : status === 'cancelled' ? null : 'profile-failed';
      if (type) {
        await _notifier.sendNotification(task.userId, type, {
          database:    task.database,
          role:        task.role,
          tableCount:  task.tableCount || 0,
          fingerprint: task.fingerprint,
          taskId:      task.id,
          error:       patch?.error || '',
          overrideTo:  task.notifyEmail
        }, ctx);
      }
    } catch (e) {
      ctx.log('[notify] profile-build completion email failed (non-fatal):', e.message);
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────
// Lazy column loader (mirrors browser-side ensureColumns)
// ─────────────────────────────────────────────────────────────────────────
async function ensureColumns(table, conn, ctx) {
  if (table.columns && table.columns.length) return table;
  try {
    const res = await dbCall(conn, {
      action:     'schema-columns',
      schemaName: table.schema,
      tableName:  table.name
    }, ctx);
    if (res && res.table) {
      table.columns     = res.table.columns     || [];
      table.primaryKeys = res.table.primaryKeys || [];
      // Don't overwrite declaredFKs — we built those from schema-fks (richer)
    } else {
      table.columns = table.columns || [];
    }
  } catch (e) {
    table.columns = table.columns || [];
    throw e;
  }
  return table;
}

// ─────────────────────────────────────────────────────────────────────────
// Tier classifier (identical to dpClassifyTier in insights.html)
// ─────────────────────────────────────────────────────────────────────────
function classifyTier(t, inboundCount) {
  const rows  = t.rowCount || 0;
  const cols  = t.columns || [];
  const fks   = t.foreignKeys || [];

  if (/^(tmp_|staging_|stg_|temp_|_temp)/i.test(t.name) || /(_tmp|_temp|_staging)$/i.test(t.name)) return 'staging';

  const auditNamePat = /(_log|_audit|_history|_hist|_archive|_arch|_journal)$|^audit_|^log_/i;
  const hasAuditCols = cols.some(c => /^(event_?date|log_?date|audit_?date|changed_?at|changed_?by|action_?type|operation)$/i.test(c.name||''));
  if (auditNamePat.test(t.name) || hasAuditCols) return 'audit';

  const lookupNamePat = /^(lk_|lkp_|ref_|cfg_|config_|setting_|type_|status_|code_|param_)/i;
  const lookupColShape = cols.length > 0 && cols.length <= 8 && cols.some(c => /(_?code|_?type|_?status|_?description|_?name)$/i.test(c.name||''));
  if (rows > 0 && rows < 500 && fks.length === 0 && (lookupNamePat.test(t.name) || lookupColShape)) return 'lookup';
  if (rows > 0 && rows < 200 && fks.length === 0 && inboundCount === 0 && cols.length > 0 && cols.length <= 12) return 'lookup';

  if (inboundCount >= 3 || rows >= 10000) return 'hub';
  return 'spoke';
}

function heuristicSummary(t) {
  const colCount = (t.columns || []).length;
  const colSuffix = colCount ? ', ' + colCount + ' columns' : '';
  switch (t.tier) {
    case 'audit':   return 'Audit / history table. Captures changes to other entities (' + t.rowCount.toLocaleString() + ' rows' + colSuffix + ').';
    case 'staging': return 'Staging / temporary table. Likely populated and emptied by ETL processes (' + t.rowCount.toLocaleString() + ' rows).';
    case 'spoke':   return 'Operational table with ' + t.rowCount.toLocaleString() + ' rows and ' + t.declaredFKs.length + ' outbound relationship(s).';
    case 'lookup':  return 'Reference / lookup table with ' + t.rowCount.toLocaleString() + ' rows' + colSuffix + '.';
    default:        return t.rowCount.toLocaleString() + ' rows' + colSuffix + '.';
  }
}

// ─────────────────────────────────────────────────────────────────────────
// Claude calls (mirror the browser-side prompts exactly, just server-side)
// ─────────────────────────────────────────────────────────────────────────
async function deepAnalyseTable(hub, conn, apiKey, allTables, ctx) {
  let rows = [];
  try {
    const r = await dbCall(conn, { action: 'execute', sql: `SELECT TOP 30 * FROM ${hub.fullName} ORDER BY (SELECT NULL)` }, ctx);
    rows = r.recordset || [];
  } catch (e) { ctx.log('sample failed for', hub.fullName, e.message); }

  let statsText = '';
  try {
    const statsCols = (hub.columns || []).slice(0,8).map(c =>
      `COUNT(DISTINCT [${c.name}]) AS [${c.name}_distinct], SUM(CASE WHEN [${c.name}] IS NULL THEN 1 ELSE 0 END) AS [${c.name}_nulls]`
    ).join(',\n');
    if (statsCols) {
      const r = await dbCall(conn, { action: 'execute', sql: `SELECT TOP 1 ${statsCols} FROM ${hub.fullName}` }, ctx);
      if (r.recordset?.[0]) statsText = JSON.stringify(r.recordset[0]);
    }
  } catch {}

  const candidates = [];
  for (const c of (hub.columns || [])) {
    const stripped = c.name.replace(/(index|_?id|_?key|_?code|num|number)$/i, '');
    if (stripped.length < 3) continue;
    const matches = allTables.filter(t =>
      t.fullName !== hub.fullName &&
      (t.name.toLowerCase().includes(stripped.toLowerCase()) ||
       stripped.toLowerCase().includes(t.name.toLowerCase().replace(/^(nx_|hbm_|gld_|tk_)/i,'')))
    ).slice(0, 3);
    for (const m of matches) {
      if (!candidates.find(cc => cc.col === c.name && cc.tgt === m.fullName)) {
        candidates.push({ col: c.name, tgt: m.fullName });
      }
    }
  }

  const sampleJson  = rows.length ? JSON.stringify(rows.slice(0,5), null, 2).slice(0, 4000) : 'No rows';
  const colsText    = (hub.columns || []).map(c => `  ${c.name} ${c.type||''}`).join('\n');
  const declaredFKs = (hub.declaredFKs || []).map(f => `${f.column} → ${f.refSchema}.${f.refTable}(${f.refColumn})`).join('; ') || 'none';
  const inboundRefs = (hub.inboundRefs || []).slice(0,10).map(r => `${r.fromSchema}.${r.fromTable}.${r.fromColumn}`).join('; ') || 'none';
  const candText    = candidates.slice(0,12).map(c => `  ${c.col} possibly → ${c.tgt}`).join('\n') || '  none';

  const prompt = `You are a senior data migration consultant documenting a database table. Be specific and reference actual column names. Be concise.

TABLE: ${hub.fullName}
ROWS: ${hub.rowCount.toLocaleString()}  ·  TIER: hub

COLUMNS:
${colsText}

DECLARED FOREIGN KEYS (outbound): ${declaredFKs}
INBOUND DECLARED REFS (other tables pointing here): ${inboundRefs}

CANDIDATE IMPLICIT REFERENCES (columns that look like they MIGHT reference another table):
${candText}

SAMPLE ROWS (first 5):
${sampleJson}

COLUMN STATS (distinct + null counts):
${statsText || 'unavailable'}

Respond with ONLY a JSON object — no markdown, no commentary outside the JSON:
{
  "summary": "2-3 sentence plain English description of what this table stores and its business role",
  "keyFields": [{"name":"column","role":"primary_key|foreign_key|code|description|date|amount|flag|other","insight":"brief note"}],
  "patterns": ["pattern 1", "pattern 2"],
  "migrationNotes": ["consideration 1"],
  "inferredFKs": [{"column":"MattIndex","refSchema":"dbo","refTable":"NX_Matter","refColumn":"MattIndex","confidence":"high|medium|low","reason":"why you think this"}]
}

For inferredFKs: only include relationships you're at least medium-confident about. Do NOT include relationships already in the DECLARED FKs above.`;

  const data = await claudeCall(apiKey, prompt, 2000, ctx);
  return {
    summary:        data.summary || hub.fullName + ' — analysis incomplete',
    keyFields:      data.keyFields || [],
    patterns:       data.patterns || [],
    migrationNotes: data.migrationNotes || [],
    inferredFKs:    data.inferredFKs || []
  };
}

async function lightAnalyseBatch(batch, apiKey, ctx) {
  const list = batch.map(t => {
    const cols = (t.columns || []).slice(0,8).map(c => c.name + ':' + (c.type||'')).join(', ');
    return `- ${t.fullName} [${t.rowCount} rows]: ${cols}`;
  }).join('\n');

  const prompt = `You are a data migration consultant documenting small reference/lookup tables. For each table below, write a one-sentence plain-English description of what it stores and what it's used for.

Respond with ONLY a JSON array — one object per table, in the same order. No markdown, no commentary:
[
  {"fullName":"dbo.LK_Currency","summary":"Currency reference list mapping ISO codes to display names","keyFields":[{"name":"CurrCode","role":"primary_key","insight":"3-letter ISO code"}],"migrationNotes":["Verify currency list matches target system before migration"]}
]

TABLES:
${list}`;

  return await claudeCallArray(apiKey, prompt, 4000, ctx);
}

async function synthesiseOverview(database, tables, tierCounts, apiKey, ctx) {
  const hubs = tables.filter(t => t.tier === 'hub').slice(0, 25);
  const hubSummaries = hubs.map(h => `- ${h.fullName} (${h.rowCount.toLocaleString()} rows): ${h.summary}`).join('\n');

  const prompt = `You are a senior data migration consultant. Below is a profile of a database. Write a high-level English overview of what this database is for and how it's organised.

DATABASE: ${database}
TOTAL TABLES: ${tables.length}
TIER BREAKDOWN: ${Object.entries(tierCounts).map(([k,v])=>v+' '+k).join(', ')}

HUB TABLES (the core entities, with their AI-generated descriptions):
${hubSummaries}

Respond with ONLY a JSON object — no markdown:
{
  "overview": "3-5 sentence English description: what kind of system this is, what it stores, how it's organised around its core entities, and anything notable about the data shape.",
  "domain": "e.g. legal practice management, healthcare claims, e-commerce orders",
  "coreEntities": ["the 3-6 most important hub tables, by business meaning"],
  "migrationComplexity": "low|medium|high|very_high",
  "complexityRationale": "1-2 sentence justification"
}`;

  const data = await claudeCall(apiKey, prompt, 1500, ctx);
  return {
    overview: data.overview || 'Schema overview unavailable.',
    extras: {
      domain:              data.domain || null,
      coreEntities:        data.coreEntities || [],
      migrationComplexity: data.migrationComplexity || 'medium',
      complexityRationale: data.complexityRationale || ''
    }
  };
}

// Anthropic Messages API caller — JSON object response
async function claudeCall(apiKey, prompt, maxTokens, ctx) {
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
    body:   JSON.stringify({ model: 'claude-sonnet-4-20250514', max_tokens: maxTokens, messages: [{ role: 'user', content: prompt }] }),
    signal: AbortSignal.timeout(90000)
  });
  if (!res.ok) {
    const t = await res.text().catch(() => '');
    throw new Error('Claude API ' + res.status + ': ' + t.slice(0, 200));
  }
  const json = await res.json();
  const raw  = json.content?.[0]?.text || '{}';
  const clean = raw.replace(/```json|```/g, '').trim();
  try { return JSON.parse(clean); }
  catch {
    const lastBrace = clean.lastIndexOf('}');
    return lastBrace > 0 ? JSON.parse(clean.slice(0, lastBrace + 1)) : { summary: raw.slice(0, 400) };
  }
}

// Array variant — for the batched lookup analyse
async function claudeCallArray(apiKey, prompt, maxTokens, ctx) {
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
    body:   JSON.stringify({ model: 'claude-sonnet-4-20250514', max_tokens: maxTokens, messages: [{ role: 'user', content: prompt }] }),
    signal: AbortSignal.timeout(90000)
  });
  if (!res.ok) {
    const t = await res.text().catch(() => '');
    throw new Error('Claude API ' + res.status + ': ' + t.slice(0, 200));
  }
  const json = await res.json();
  const raw  = json.content?.[0]?.text || '[]';
  const clean = raw.replace(/```json|```/g, '').trim();
  try { return JSON.parse(clean); }
  catch {
    const lastBracket = clean.lastIndexOf(']');
    if (lastBracket > 0) return JSON.parse(clean.slice(0, lastBracket + 1));
    throw new Error('Unparseable batch response');
  }
}
