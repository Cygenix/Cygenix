// ─────────────────────────────────────────────────────────────────────────────
// Cygenix Report Builder — Azure Function backend (Drop 5)
//
// Paste this code into your existing single-index.js Azure Function. It
// adds six new actions to your action-dispatch switch:
//
//   reports-list, reports-save, reports-delete
//   runs-list,    runs-save,    runs-delete
//
// All actions:
//   - Require ?code=<function-key> query string (handled by Azure already)
//   - Require x-user-id request header (set by the frontend; read via
//     CygenixSync.getUserId())
//   - Use the same Cosmos client your existing 'admin-users' / 'audit'
//     handlers use. If you've named the Cosmos client differently below,
//     adjust the variable names in the helpers section.
//
// Required Cosmos containers (create in Azure Portal before deploying):
//   Container: reports          Partition key: /userId
//   Container: report_runs      Partition key: /reportId
//
// Default indexing is fine for both. Throughput: shared (database-level)
// RU/s recommended — this is low-traffic.
// ─────────────────────────────────────────────────────────────────────────────


// ════════════════════════════════════════════════════════════════════════════
// SECTION 1: Cosmos container helpers
//
// Drop these helpers somewhere near the top of your index.js, with the
// other Cosmos initialisation. If your existing code already has a
// `getContainer(name)` or similar helper, you can skip this section and
// reuse it.
// ════════════════════════════════════════════════════════════════════════════

// Lazy-initialised container handles. Reuses the cosmosClient your
// existing code already creates. If your client variable is named
// something other than `cosmosClient`, update the line below.
let _reportsContainer = null;
let _runsContainer = null;

function getReportsContainer() {
  if (_reportsContainer) return _reportsContainer;
  // CHANGE THESE TWO if your DB / client naming differs:
  const dbName = process.env.COSMOS_DB_NAME || 'cygenix';
  _reportsContainer = cosmosClient.database(dbName).container('reports');
  return _reportsContainer;
}

function getRunsContainer() {
  if (_runsContainer) return _runsContainer;
  const dbName = process.env.COSMOS_DB_NAME || 'cygenix';
  _runsContainer = cosmosClient.database(dbName).container('report_runs');
  return _runsContainer;
}


// ════════════════════════════════════════════════════════════════════════════
// SECTION 2: Action handlers
//
// Add these `case` blocks to your action dispatch switch. The pattern
// mirrors how `admin-users` / `audit` are handled in your existing code.
//
// Helper functions (parseUserId, ok, fail) are at the bottom — adjust
// names to match what your existing code uses.
// ════════════════════════════════════════════════════════════════════════════

// ─────── reports-list ───────────────────────────────────────────────────────
// GET — returns all reports owned by the user.
// Response: { reports: [Report, ...] }
async function handleReportsList(context, req) {
  const userId = parseUserId(req);
  if (!userId) return fail(context, 401, 'Missing x-user-id header');

  const container = getReportsContainer();
  const { resources } = await container.items
    .query({
      query: 'SELECT * FROM c WHERE c.userId = @userId',
      parameters: [{ name: '@userId', value: userId }]
    })
    .fetchAll();

  // Strip Cosmos system fields that the frontend doesn't need.
  const reports = resources.map(stripSystemFields);
  return ok(context, { reports });
}

// ─────── reports-save ───────────────────────────────────────────────────────
// POST — upserts a single report. Body is the Report object.
// Body must include id and userId. We enforce that userId in the body
// matches the x-user-id header (no writing reports for other users).
async function handleReportsSave(context, req) {
  const userId = parseUserId(req);
  if (!userId) return fail(context, 401, 'Missing x-user-id header');

  const report = req.body;
  if (!report || typeof report !== 'object') return fail(context, 400, 'Body must be a report object');
  if (!report.id) return fail(context, 400, 'Report must have an id');
  if (report.userId && report.userId !== userId) {
    return fail(context, 403, 'Cannot save a report owned by another user');
  }
  // Stamp userId so the partition key is set correctly
  report.userId = userId;

  const container = getReportsContainer();
  const { resource } = await container.items.upsert(report);
  return ok(context, { report: stripSystemFields(resource) });
}

// ─────── reports-delete ─────────────────────────────────────────────────────
// POST — body: { id }. Deletes the report (and all associated runs).
async function handleReportsDelete(context, req) {
  const userId = parseUserId(req);
  if (!userId) return fail(context, 401, 'Missing x-user-id header');

  const { id } = req.body || {};
  if (!id) return fail(context, 400, 'Body must include id');

  // Delete the report doc (partitioned on userId)
  const reports = getReportsContainer();
  try {
    await reports.item(id, userId).delete();
  } catch (e) {
    // 404 = already gone, that's fine.
    if (e.code !== 404) throw e;
  }

  // Cascade: delete saved runs for this report. Runs are partitioned on
  // reportId, so a single partition query + bulk delete.
  const runs = getRunsContainer();
  try {
    const { resources: runDocs } = await runs.items
      .query({
        query: 'SELECT c.id FROM c WHERE c.reportId = @reportId',
        parameters: [{ name: '@reportId', value: id }],
        partitionKey: id
      })
      .fetchAll();
    for (const r of runDocs) {
      try { await runs.item(r.id, id).delete(); }
      catch (e) { if (e.code !== 404) throw e; }
    }
  } catch (e) {
    // Don't fail the whole delete just because the runs cleanup hit an
    // issue. The report is gone; orphan runs can be tidied later.
    context.log.warn('Failed to cascade-delete runs for report ' + id + ': ' + e.message);
  }

  return ok(context, { deleted: true, id });
}

// ─────── runs-list ──────────────────────────────────────────────────────────
// GET — query string: reportId. Returns all saved runs for that report.
// Response: { runs: [Run, ...] }, sorted newest first.
async function handleRunsList(context, req) {
  const userId = parseUserId(req);
  if (!userId) return fail(context, 401, 'Missing x-user-id header');

  const reportId = (req.query && req.query.reportId) || '';
  if (!reportId) return fail(context, 400, 'reportId query parameter is required');

  // Ownership check: confirm the report belongs to this user before
  // exposing its runs. Cheap point-read on the reports container.
  const reports = getReportsContainer();
  try {
    const { resource: report } = await reports.item(reportId, userId).read();
    if (!report) return fail(context, 403, 'Not your report');
  } catch (e) {
    if (e.code === 404) return fail(context, 403, 'Not your report');
    throw e;
  }

  const runs = getRunsContainer();
  const { resources } = await runs.items
    .query({
      query: 'SELECT * FROM c WHERE c.reportId = @reportId ORDER BY c.savedAt DESC',
      parameters: [{ name: '@reportId', value: reportId }],
      partitionKey: reportId
    })
    .fetchAll();
  return ok(context, { runs: resources.map(stripSystemFields) });
}

// ─────── runs-save ──────────────────────────────────────────────────────────
// POST — body is the Run object. Must include id, reportId.
async function handleRunsSave(context, req) {
  const userId = parseUserId(req);
  if (!userId) return fail(context, 401, 'Missing x-user-id header');

  const run = req.body;
  if (!run || typeof run !== 'object') return fail(context, 400, 'Body must be a run object');
  if (!run.id) return fail(context, 400, 'Run must have an id');
  if (!run.reportId) return fail(context, 400, 'Run must have a reportId');

  // Ownership check
  const reports = getReportsContainer();
  try {
    const { resource: report } = await reports.item(run.reportId, userId).read();
    if (!report) return fail(context, 403, 'Not your report');
  } catch (e) {
    if (e.code === 404) return fail(context, 403, 'Not your report');
    throw e;
  }

  // Stamp userId so we know who owns it (audit). Doesn't affect partitioning.
  run.userId = userId;

  // Cosmos has a 2 MB document limit. Reject upfront with a clear error
  // rather than letting Cosmos return its own less-friendly message.
  const sz = Buffer.byteLength(JSON.stringify(run), 'utf8');
  if (sz > 2000000) {
    return fail(context, 413, 'Run document is ' + sz + ' bytes, exceeds 2MB limit. Frontend should mark it truncated.');
  }

  const runs = getRunsContainer();
  const { resource } = await runs.items.upsert(run);
  return ok(context, { run: stripSystemFields(resource) });
}

// ─────── runs-delete ────────────────────────────────────────────────────────
// POST — body: { reportId, id }
async function handleRunsDelete(context, req) {
  const userId = parseUserId(req);
  if (!userId) return fail(context, 401, 'Missing x-user-id header');

  const { reportId, id } = req.body || {};
  if (!reportId || !id) return fail(context, 400, 'Body must include reportId and id');

  // Ownership check
  const reports = getReportsContainer();
  try {
    const { resource: report } = await reports.item(reportId, userId).read();
    if (!report) return fail(context, 403, 'Not your report');
  } catch (e) {
    if (e.code === 404) return fail(context, 403, 'Not your report');
    throw e;
  }

  const runs = getRunsContainer();
  try {
    await runs.item(id, reportId).delete();
  } catch (e) {
    if (e.code !== 404) throw e;
  }
  return ok(context, { deleted: true, id });
}


// ════════════════════════════════════════════════════════════════════════════
// SECTION 3: Wire into your dispatch switch
//
// Find the part of your index.js that looks something like:
//
//   switch (action) {
//     case 'admin-users':  return handleAdminUsers(context, req);
//     case 'audit':        return handleAudit(context, req);
//     ...
//   }
//
// Add these six cases:
// ════════════════════════════════════════════════════════════════════════════

/*
    case 'reports-list':    return handleReportsList(context, req);
    case 'reports-save':    return handleReportsSave(context, req);
    case 'reports-delete':  return handleReportsDelete(context, req);
    case 'runs-list':       return handleRunsList(context, req);
    case 'runs-save':       return handleRunsSave(context, req);
    case 'runs-delete':     return handleRunsDelete(context, req);
*/


// ════════════════════════════════════════════════════════════════════════════
// SECTION 4: Helpers (skip if your index.js already defines these)
// ════════════════════════════════════════════════════════════════════════════

// Strip Cosmos system fields the frontend doesn't need.
function stripSystemFields(doc) {
  if (!doc) return doc;
  const out = { ...doc };
  delete out._rid;
  delete out._self;
  delete out._etag;
  delete out._attachments;
  delete out._ts;
  return out;
}

// Read x-user-id header. Case-insensitive (Azure normalises but be safe).
function parseUserId(req) {
  if (!req || !req.headers) return '';
  const h = req.headers;
  return h['x-user-id'] || h['X-User-Id'] || h['X-USER-ID'] || '';
}

// Standard responses. If your existing code uses different helpers
// (e.g. `respond(...)`), use those instead.
function ok(context, body) {
  context.res = {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
    body
  };
}
function fail(context, status, msg) {
  context.res = {
    status,
    headers: { 'Content-Type': 'application/json' },
    body: { error: msg }
  };
}


// ════════════════════════════════════════════════════════════════════════════
// SECTION 5: Frontend configuration
//
// In reports.html (or any page that uses sync), the frontend looks for two
// window globals before talking to Cosmos:
//
//   window.CYGENIX_API       = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api'
//   window.CYGENIX_API_CODE  = '<your function key>'
//
// Set these in your existing config script (likely cygenix-params.js or
// similar — wherever your other pages already get their API URL/key).
//
// Without those globals, the Report Builder silently degrades to
// localStorage-only mode — everything works, just no cloud sync.
// ════════════════════════════════════════════════════════════════════════════
