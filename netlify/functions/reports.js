// netlify/functions/reports.js
//
// Conversion Reports storage — CRUD against Cosmos DB container `project_reports`.
//
// Actions (POST, JSON body):
//   list    { userEmail }                           → { reports: [...] }   (top 10, newest first)
//   get     { userEmail, id }                       → { report: {...} }
//   save    { userEmail, userName?, report }        → { id, savedCount, prunedCount }
//   delete  { userEmail, id }                       → { deleted: true|false }
//
// Per-user isolation:
//   Each stored document includes a `userId` field (lowercased email). Partition
//   key is `/userId`, so Cosmos queries are partition-scoped and cheap. Every
//   read/delete path re-verifies the document's userId matches the caller's
//   email — prevents a client from guessing ids across partitions.
//
// Authentication caveat:
//   For today's scope, the client tells us who they are (userEmail in body).
//   A future session should validate the Entra JWT server-side and extract
//   `oid` from the verified token. The only thing that needs to change is
//   `resolveUser(body, event)` below — replace its body with JWT verification
//   and every endpoint's isolation logic stays the same.
//
// Prune-to-10 behaviour:
//   On save, we count the user's existing reports. If count >= 10, we delete
//   the oldest ones first (ordered by savedAt ASC) to bring the count to 9,
//   then insert the new one. Silent — no warning returned.
//
// Environment variables required (set in Netlify site settings → Environment):
//   COSMOS_ENDPOINT   e.g. https://cygenix.documents.azure.com:443/
//   COSMOS_KEY        primary master key from Azure portal
//
// Config (hard-coded — change here if you rename the database/container):
//   Database:  cygenix
//   Container: project_reports
//   Partition: /userId

const { CosmosClient } = require('@azure/cosmos');

const DATABASE_NAME  = 'cygenix';
const CONTAINER_NAME = 'project_reports';
const MAX_PER_USER   = 10;

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

function ok(data)              { return { statusCode: 200, headers: CORS, body: JSON.stringify(data) }; }
function err(msg, code = 500)  { return { statusCode: code, headers: CORS, body: JSON.stringify({ error: msg }) }; }

// Module-level client — reused across warm invocations. Created lazily on first
// call so missing env vars surface as a clean 500 rather than a module-load
// crash during deploy.
let _client   = null;
let _container = null;
function getContainer() {
  if (_container) return _container;
  const endpoint = process.env.COSMOS_ENDPOINT;
  const key      = process.env.COSMOS_KEY;
  if (!endpoint || !key) {
    throw new Error('Cosmos not configured — set COSMOS_ENDPOINT and COSMOS_KEY in Netlify environment variables.');
  }
  _client    = new CosmosClient({ endpoint, key });
  _container = _client.database(DATABASE_NAME).container(CONTAINER_NAME);
  return _container;
}

// Resolve the acting user from the request. Returns the lowercased email or
// throws a 400 with a clear message if missing. Isolated into its own function
// so a future JWT-validation swap is a one-place change.
function resolveUser(body /*, event */) {
  const raw = (body && body.userEmail) || '';
  const email = String(raw || '').trim().toLowerCase();
  if (!email) {
    const e = new Error('userEmail is required');
    e.statusCode = 400;
    throw e;
  }
  return email;
}

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };
  if (event.httpMethod !== 'POST')    return err('Method not allowed', 405);

  let body;
  try { body = JSON.parse(event.body || '{}'); }
  catch (e) { return err('Invalid JSON: ' + e.message, 400); }

  const { action } = body;
  if (!action) return err('action is required', 400);

  try {
    const userId = resolveUser(body, event);
    const container = getContainer();

    switch (action) {
      case 'list':   return ok(await listReports(container, userId));
      case 'get':    return ok(await getReport(container, userId, body.id));
      case 'save':   return ok(await saveReport(container, userId, body));
      case 'delete': return ok(await deleteReport(container, userId, body.id));
      default:
        return err('Unknown action: ' + action + ' (valid: list | get | save | delete)', 400);
    }
  } catch (e) {
    const code = e.statusCode || 500;
    return err(e.message || String(e), code);
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// LIST — newest first, max MAX_PER_USER items. Cheap: single partition scan.
// ─────────────────────────────────────────────────────────────────────────────
async function listReports(container, userId) {
  const query = {
    query: 'SELECT c.id, c.projectName, c.savedAt, c.userName, c.userEmail, c.sourceSystem, c.targetSystem, c.totalRows, c.insertedRows, c.errors, ARRAY_LENGTH(c.tables) AS tableCount FROM c WHERE c.userId = @uid ORDER BY c.savedAt DESC',
    parameters: [{ name: '@uid', value: userId }],
  };
  const { resources } = await container.items.query(query, { partitionKey: userId }).fetchAll();
  return { reports: resources.slice(0, MAX_PER_USER) };
}

// ─────────────────────────────────────────────────────────────────────────────
// GET — fetch one by id, verify userId match.
// ─────────────────────────────────────────────────────────────────────────────
async function getReport(container, userId, id) {
  if (!id) { const e = new Error('id is required'); e.statusCode = 400; throw e; }
  try {
    const { resource } = await container.item(id, userId).read();
    if (!resource) { const e = new Error('Report not found'); e.statusCode = 404; throw e; }
    // Belt-and-braces — partition key already enforced isolation, but double-check.
    if (resource.userId !== userId) {
      const e = new Error('Report not found'); e.statusCode = 404; throw e;
    }
    return { report: resource };
  } catch (e) {
    if (e.code === 404 || e.statusCode === 404) {
      const nf = new Error('Report not found'); nf.statusCode = 404; throw nf;
    }
    throw e;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SAVE — prune to MAX_PER_USER-1 if needed, then insert.
// ─────────────────────────────────────────────────────────────────────────────
async function saveReport(container, userId, body) {
  const payload = body.report;
  if (!payload || typeof payload !== 'object') {
    const e = new Error('report object is required'); e.statusCode = 400; throw e;
  }

  // Count existing reports for this user. Scoped to the user's partition so
  // it's a single-partition count — very cheap.
  const countQuery = {
    query: 'SELECT VALUE COUNT(1) FROM c WHERE c.userId = @uid',
    parameters: [{ name: '@uid', value: userId }],
  };
  const { resources: countRes } = await container.items.query(countQuery, { partitionKey: userId }).fetchAll();
  const existingCount = Array.isArray(countRes) && countRes.length ? countRes[0] : 0;

  // Prune oldest if at or above cap. We want to end up with at most
  // MAX_PER_USER-1 existing, so the new one brings total to MAX_PER_USER.
  let prunedCount = 0;
  const surplus = existingCount - (MAX_PER_USER - 1);
  if (surplus > 0) {
    const oldestQuery = {
      query: 'SELECT TOP @n c.id, c.userId FROM c WHERE c.userId = @uid ORDER BY c.savedAt ASC',
      parameters: [
        { name: '@n',   value: surplus },
        { name: '@uid', value: userId },
      ],
    };
    const { resources: toDelete } = await container.items.query(oldestQuery, { partitionKey: userId }).fetchAll();
    for (const old of toDelete) {
      try { await container.item(old.id, userId).delete(); prunedCount++; }
      catch (delErr) { console.warn('[reports] prune delete failed for', old.id, delErr.message); }
    }
  }

  // Build the stored document. We never trust client-supplied userId — we set
  // it from the authenticated email. Accept the rest of the report payload
  // as-is (renderer validates shape).
  const now = new Date().toISOString();
  const doc = {
    ...payload,
    id:        payload.id || ('rpt_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8)),
    userId,                             // partition key — enforced
    userEmail: userId,                  // kept for backwards-compat in existing report shape
    userName:  body.userName || payload.userName || '',
    savedAt:   now,
    // Preserve any existing completedAt; default to savedAt if missing.
    completedAt: payload.completedAt || now,
  };

  const { resource } = await container.items.create(doc);
  return { id: resource.id, savedAt: resource.savedAt, prunedCount, totalAfter: existingCount - prunedCount + 1 };
}

// ─────────────────────────────────────────────────────────────────────────────
// DELETE — verify ownership via partition key.
// ─────────────────────────────────────────────────────────────────────────────
async function deleteReport(container, userId, id) {
  if (!id) { const e = new Error('id is required'); e.statusCode = 400; throw e; }
  try {
    await container.item(id, userId).delete();
    return { deleted: true };
  } catch (e) {
    if (e.code === 404 || e.statusCode === 404) return { deleted: false };
    throw e;
  }
}
