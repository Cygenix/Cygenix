// netlify/functions/reports.js
//
// Conversion Reports storage — CRUD against Cosmos DB container `project_reports`.
//
// Actions (POST, JSON body):
//   list    { }                                     → { reports: [...] }   (top 10, newest first)
//   get     { id }                                  → { report: {...} }
//   save    { report }                              → { id, savedCount, prunedCount }
//   delete  { id }                                  → { deleted: true|false }
//
// AUTHENTICATION (added):
//   Every request MUST include an `Authorization: Bearer <jwt>` header
//   containing a valid Entra ID token from the Cygenix tenant. The token
//   signature is verified against Entra's published JWKS. The user's email
//   is extracted from the verified token claims and used as the partition
//   key for all Cosmos operations.
//
//   The legacy `userEmail` field in the request body is now IGNORED for
//   authorisation purposes — a malicious client can no longer impersonate
//   another user by passing a different email. The field is still accepted
//   in `save` payloads (it gets stored alongside the report for display)
//   but is overwritten with the verified email before persistence.
//
// Per-user isolation:
//   Each stored document includes a `userId` field (lowercased email from
//   the verified token). Partition key is `/userId`, so Cosmos queries are
//   partition-scoped. Every read/delete path re-verifies the document's
//   userId matches the authenticated caller — defence in depth.
//
// Environment variables required:
//   COSMOS_ENDPOINT            e.g. https://cygenix.documents.azure.com:443/
//   COSMOS_KEY                 primary master key from Azure portal
//   ENTRA_TENANT_ID            fc8dfc7a-645f-4a5c-8f59-6762f97c803f
//   ENTRA_CLIENT_ID            f3478996-b2b5-4b21-9a23-a6b97a0e5b13
//   ENTRA_AUTHORITY_HOST       cygenix.ciamlogin.com   (no protocol, no path)
//
// Required npm dependency (add to netlify/functions/package.json):
//   "jsonwebtoken": "^9.0.2"
//   "jwks-rsa":     "^3.1.0"

const { CosmosClient } = require('@azure/cosmos');
const jwt              = require('jsonwebtoken');
const jwksClient       = require('jwks-rsa');

const DATABASE_NAME  = 'cygenix';
const CONTAINER_NAME = 'project_reports';
const MAX_PER_USER   = 10;

const TENANT_ID       = process.env.ENTRA_TENANT_ID       || 'fc8dfc7a-645f-4a5c-8f59-6762f97c803f';
const CLIENT_ID       = process.env.ENTRA_CLIENT_ID       || 'f3478996-b2b5-4b21-9a23-a6b97a0e5b13';
const AUTHORITY_HOST  = process.env.ENTRA_AUTHORITY_HOST  || 'cygenix.ciamlogin.com';

// JWKS endpoint — Entra publishes its public signing keys here. The library
// caches keys (default 10 min) so we hit this URL infrequently.
const JWKS_URI = `https://${AUTHORITY_HOST}/${TENANT_ID}/discovery/v2.0/keys`;

// Acceptable issuers — Entra External ID can issue tokens with several
// valid issuer values depending on the user flow and federation. We accept
// the standard CIAM patterns.
const VALID_ISSUERS = [
  `https://${AUTHORITY_HOST}/${TENANT_ID}/v2.0`,
  `https://${TENANT_ID}.ciamlogin.com/${TENANT_ID}/v2.0`,
  `https://login.microsoftonline.com/${TENANT_ID}/v2.0`,
];

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

function ok(data)              { return { statusCode: 200, headers: CORS, body: JSON.stringify(data) }; }
function err(msg, code = 500)  { return { statusCode: code, headers: CORS, body: JSON.stringify({ error: msg }) }; }

// ──────────────────────────────────────────────────────────────────────────
// JWT verification
// ──────────────────────────────────────────────────────────────────────────
//
// Module-level JWKS client — caches signing keys across warm invocations
// to avoid hammering Entra's discovery endpoint. Keys rotate roughly every
// 24h and the library auto-refreshes.
const _jwks = jwksClient({
  jwksUri: JWKS_URI,
  cache: true,
  cacheMaxEntries: 5,
  cacheMaxAge: 10 * 60 * 1000,    // 10 minutes
  rateLimit: true,
  jwksRequestsPerMinute: 10,
});

function getSigningKey(header, callback) {
  _jwks.getSigningKey(header.kid, (e, key) => {
    if (e) return callback(e);
    callback(null, key.getPublicKey());
  });
}

// Verify the Authorization header. Returns { email, name, oid } on success
// or throws an Error with a clear message on failure.
async function verifyAuthHeader(event) {
  const h = event.headers || {};
  // Header keys in Netlify are lowercased, but be defensive.
  const raw = h.authorization || h.Authorization || '';
  if (!raw) throw new Error('Missing Authorization header');

  const m = /^Bearer\s+(.+)$/i.exec(raw.trim());
  if (!m) throw new Error('Authorization header must be "Bearer <token>"');
  const token = m[1].trim();

  const decoded = await new Promise((resolve, reject) => {
    jwt.verify(token, getSigningKey, {
      audience: CLIENT_ID,
      issuer:   VALID_ISSUERS,
      algorithms: ['RS256'],
    }, (e, payload) => {
      if (e) return reject(e);
      resolve(payload);
    });
  });

  // Extract email — Entra puts it in different claims depending on the IdP.
  // For Google SSO logins, `email` claim is populated; for direct sign-ups
  // `preferred_username` is usually the email. Fall back through both.
  const email = String(
    decoded.email ||
    decoded.preferred_username ||
    decoded.upn ||
    ''
  ).trim().toLowerCase();

  if (!email) throw new Error('Token contains no email claim');

  return {
    email,
    name: decoded.name || '',
    oid:  decoded.oid  || decoded.sub || '',
  };
}

// ──────────────────────────────────────────────────────────────────────────
// Cosmos client (lazy)
// ──────────────────────────────────────────────────────────────────────────
let _container = null;
function getContainer() {
  if (_container) return _container;
  const endpoint = process.env.COSMOS_ENDPOINT;
  const key      = process.env.COSMOS_KEY;
  if (!endpoint || !key) throw new Error('COSMOS_ENDPOINT / COSMOS_KEY not set');
  const client = new CosmosClient({ endpoint, key });
  _container = client.database(DATABASE_NAME).container(CONTAINER_NAME);
  return _container;
}

// ──────────────────────────────────────────────────────────────────────────
// Handlers
// ──────────────────────────────────────────────────────────────────────────

async function handleList(authedEmail) {
  const container = getContainer();
  const { resources } = await container.items.query({
    query: 'SELECT * FROM c WHERE c.userId = @uid ORDER BY c.savedAt DESC OFFSET 0 LIMIT @max',
    parameters: [
      { name: '@uid', value: authedEmail },
      { name: '@max', value: MAX_PER_USER },
    ],
  }, { partitionKey: authedEmail }).fetchAll();
  return ok({ reports: resources });
}

async function handleGet(authedEmail, body) {
  const id = body.id;
  if (!id) return err('id required', 400);
  const container = getContainer();
  try {
    const { resource } = await container.item(id, authedEmail).read();
    if (!resource)                       return err('Report not found', 404);
    if (resource.userId !== authedEmail) return err('Report not found', 404);
    return ok({ report: resource });
  } catch (e) {
    if (e.code === 404) return err('Report not found', 404);
    throw e;
  }
}

async function handleSave(authed, body) {
  const report = body.report;
  if (!report || typeof report !== 'object') return err('report object required', 400);

  const container = getContainer();

  // Prune oldest if at cap
  let prunedCount = 0;
  const { resources: existing } = await container.items.query({
    query: 'SELECT c.id, c.savedAt FROM c WHERE c.userId = @uid ORDER BY c.savedAt ASC',
    parameters: [{ name: '@uid', value: authed.email }],
  }, { partitionKey: authed.email }).fetchAll();

  if (existing.length >= MAX_PER_USER) {
    const toDelete = existing.slice(0, existing.length - (MAX_PER_USER - 1));
    for (const old of toDelete) {
      try { await container.item(old.id, authed.email).delete(); prunedCount++; } catch {}
    }
  }

  const id = report.id || ('rpt_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8));
  const doc = Object.assign({}, report, {
    id,
    userId:    authed.email,        // partition key — verified, not client-supplied
    userEmail: authed.email,        // display field
    userName:  authed.name || report.userName || '',
    savedAt:   new Date().toISOString(),
  });

  await container.items.upsert(doc);

  return ok({ id, savedCount: 1, prunedCount });
}

async function handleDelete(authedEmail, body) {
  const id = body.id;
  if (!id) return err('id required', 400);
  const container = getContainer();
  try {
    // Re-verify ownership before deleting
    const { resource } = await container.item(id, authedEmail).read();
    if (!resource || resource.userId !== authedEmail) {
      return err('Report not found', 404);
    }
    await container.item(id, authedEmail).delete();
    return ok({ deleted: true });
  } catch (e) {
    if (e.code === 404) return err('Report not found', 404);
    throw e;
  }
}

// Patch presentation config / projectId on an existing report. The dashboard
// uses this for two things:
//   1. Saving the user's presentation config (charts, sort, pivots, etc.)
//   2. Backfilling projectId on older reports that pre-date that field
// Either or both fields can be supplied; missing fields are left untouched.
async function handleSavePresentation(authedEmail, body) {
  const id = body.id;
  if (!id) return err('id required', 400);
  const container = getContainer();

  // Load and re-verify ownership
  let reportDoc;
  try {
    const { resource } = await container.item(id, authedEmail).read();
    reportDoc = resource;
  } catch (e) {
    if (e.code === 404) return err('Report not found', 404);
    throw e;
  }
  if (!reportDoc || reportDoc.userId !== authedEmail) {
    return err('Report not found', 404);
  }

  // Apply only the fields the client asked to update
  if (body.presentationConfig !== undefined) {
    reportDoc.presentationConfig = body.presentationConfig;
  }
  if (body.projectId !== undefined) {
    reportDoc.projectId = body.projectId;
  }
  reportDoc.updatedAt = new Date().toISOString();

  await container.items.upsert(reportDoc);
  return ok({ ok: true, savedAt: reportDoc.updatedAt });
}

// ──────────────────────────────────────────────────────────────────────────
// Entry point
// ──────────────────────────────────────────────────────────────────────────
exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };
  if (event.httpMethod !== 'POST')    return err('POST only', 405);

  // 1. Authenticate
  let authed;
  try {
    authed = await verifyAuthHeader(event);
  } catch (e) {
    return err('Unauthorized: ' + (e.message || 'invalid token'), 401);
  }

  // 2. Parse body
  let body = {};
  try { body = JSON.parse(event.body || '{}'); } catch { return err('Invalid JSON body', 400); }

  const action = String(body.action || '').toLowerCase();

  // 3. Dispatch — note that handlers receive the AUTHENTICATED email, not
  //    anything from the request body. body.userEmail is ignored.
  try {
    if (action === 'list')              return await handleList(authed.email);
    if (action === 'get')               return await handleGet(authed.email, body);
    if (action === 'save')              return await handleSave(authed, body);
    if (action === 'delete')            return await handleDelete(authed.email, body);
    if (action === 'save-presentation') return await handleSavePresentation(authed.email, body);
    return err('Unknown action: ' + action, 400);
  } catch (e) {
    // In-band debugging — Application Insights not available on this plan.
    return {
      statusCode: 500,
      headers: CORS,
      body: JSON.stringify({
        error: e.message || 'Server error',
        stack: e.stack || '',
      }),
    };
  }
};
