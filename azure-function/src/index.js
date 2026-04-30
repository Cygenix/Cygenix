// Cosmos DB integration v2 - audit + admin-users + invite
// CM
const { app } = require('@azure/functions');

// Register the Task Agent scheduler module (HTTP route /api/schedules/{action}
// + every-minute timer trigger). Side-effect import — the module's app.http()
// and app.timer() calls run at load time.
require('./schedules');
require('./agent'); 

// ── Cosmos DB client (lazy singleton, key-based auth) ────────────────────────
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

// ── Shared CORS headers ───────────────────────────────────────────────────────
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, x-user-id',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
  'Content-Type': 'application/json'
};

const ok  = (body)       => ({ status: 200, headers: CORS, body: JSON.stringify(body) });
const err = (code, msg)  => ({ status: code, headers: CORS, body: JSON.stringify({ error: msg }) });

// HTML response helper for endpoints that return a rendered document
// (e.g. project-summary-document). Bypasses the JSON Content-Type that
// `ok` sets so the browser renders the body as a page rather than text.
const html = (body) => ({
  status: 200,
  headers: {
    ...CORS,
    'Content-Type': 'text/html; charset=utf-8',
    'Cache-Control': 'private, no-cache'
  },
  body
});

// ── Helper: get userId from request header ────────────────────────────────────
function getUserId(req) {
  return req.headers.get('x-user-id') || req.query.get('userId') || null;
}

// ── Helper: stable SHA-256 hash for version dedupe ───────────────────────────
function hashSnapshot(obj) {
  const crypto = require('crypto');
  const canonical = JSON.stringify(obj, Object.keys(obj || {}).sort());
  return crypto.createHash('sha256').update(canonical).digest('hex');
}

// ── Helper: confirm caller is an admin ───────────────────────────────────────
// Reads the caller's record from the Cosmos users container and checks
// `role === 'admin'`. Returns { ok: true } on success or { ok: false, code,
// msg } on failure so callers can return the appropriate HTTP error. Used to
// gate destructive admin-only endpoints (e.g. extend-membership) so a normal
// user cannot self-promote or self-extend by hitting the endpoint directly.
async function requireAdmin(callerUserId) {
  if (!callerUserId) return { ok: false, code: 401, msg: 'x-user-id header is required' };
  try {
    const { resource } = await getCosmosContainer('users')
      .item(callerUserId, callerUserId).read();
    if (!resource) return { ok: false, code: 403, msg: 'Caller has no user record' };
    if (resource.role !== 'admin') return { ok: false, code: 403, msg: 'Admin role required' };
    return { ok: true, caller: resource };
  } catch (e) {
    if (e.code === 404) return { ok: false, code: 403, msg: 'Caller has no user record' };
    return { ok: false, code: 500, msg: `Role check failed: ${e.message}` };
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// ROUTE 1: /api/db/{action} — existing SQL Server endpoint (unchanged)
// ─────────────────────────────────────────────────────────────────────────────
app.http('db', {
  methods: ['GET', 'POST', 'OPTIONS'],
  authLevel: 'function',
  route: 'db/{action?}',
  handler: async (req, ctx) => {
    const headers = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
      'Content-Type': 'application/json'
    };

    if (req.method === 'OPTIONS') return { status: 200, headers, body: '' };

    try {
      const { DefaultAzureCredential } = require('@azure/identity');
      const sql = require('mssql');

      let body = {};
      try { body = await req.json(); } catch {}
      const action = req.params.action || body.action || '';
      ctx.log('action:', action);

      ctx.log('Getting credential...');
      const credential = new DefaultAzureCredential();
      ctx.log('Getting token...');
      const tokenResp = await credential.getToken('https://database.windows.net/.default');
      ctx.log('Token acquired, connecting to SQL...');

      const pool = await sql.connect({
        server: process.env.SQL_SERVER,
        database: process.env.SQL_DATABASE,
        options: { encrypt: true, trustServerCertificate: false, enableArithAbort: true },
        authentication: {
          type: 'azure-active-directory-access-token',
          options: { token: tokenResp.token }
        }
      });
      ctx.log('Connected!');

      let result;
      switch (action) {
        case 'test': {
          const r = await pool.request().query('SELECT @@VERSION AS version, DB_NAME() AS dbname, SUSER_SNAME() AS sysuser');
          result = { success: true, version: r.recordset[0].version.split('\n')[0].trim(), database: r.recordset[0].dbname, user: r.recordset[0].sysuser };
          break;
        }
        case 'schema': {
          // Fetch tables, columns, primary keys, views, view-columns, procedures
          // and functions in parallel. The response keeps the existing `tables`
          // shape (for backward compatibility with AI prompts and other
          // consumers) and adds `views`, `procedures`, `functions` alongside.
          const [tablesR, colsR, pkR, viewsR, viewColsR, procsR, funcsR, paramsR] = await Promise.all([
            // BASE TABLEs with row counts
            pool.request().query(`SELECT t.TABLE_SCHEMA, t.TABLE_NAME, COALESCE(p.rows,0) AS row_count FROM INFORMATION_SCHEMA.TABLES t LEFT JOIN sys.tables st ON st.name=t.TABLE_NAME AND SCHEMA_NAME(st.schema_id)=t.TABLE_SCHEMA LEFT JOIN sys.partitions p ON p.object_id=st.object_id AND p.index_id IN(0,1) WHERE t.TABLE_TYPE='BASE TABLE' ORDER BY t.TABLE_SCHEMA,t.TABLE_NAME`),
            // Columns (covers tables AND views — INFORMATION_SCHEMA.COLUMNS includes both)
            pool.request().query(`SELECT c.TABLE_SCHEMA,c.TABLE_NAME,c.COLUMN_NAME,c.DATA_TYPE,c.CHARACTER_MAXIMUM_LENGTH,c.NUMERIC_PRECISION,c.NUMERIC_SCALE,c.IS_NULLABLE,c.ORDINAL_POSITION,COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA+'.'+c.TABLE_NAME),c.COLUMN_NAME,'IsIdentity') AS is_identity FROM INFORMATION_SCHEMA.COLUMNS c ORDER BY c.TABLE_SCHEMA,c.TABLE_NAME,c.ORDINAL_POSITION`),
            // Primary keys
            pool.request().query(`SELECT tc.TABLE_SCHEMA,tc.TABLE_NAME,kcu.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME=kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA=kcu.TABLE_SCHEMA WHERE tc.CONSTRAINT_TYPE='PRIMARY KEY'`),
            // VIEWs (without bodies — keep payload small)
            pool.request().query(`SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS ORDER BY TABLE_SCHEMA, TABLE_NAME`),
            // Columns scoped to views only — separates view cols from table cols
            pool.request().query(`SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.IS_NULLABLE, c.ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS c JOIN INFORMATION_SCHEMA.VIEWS v ON v.TABLE_SCHEMA = c.TABLE_SCHEMA AND v.TABLE_NAME = c.TABLE_NAME ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION`),
            // PROCEDUREs
            pool.request().query(`SELECT ROUTINE_SCHEMA, ROUTINE_NAME, CREATED, LAST_ALTERED FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE='PROCEDURE' ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME`),
            // FUNCTIONs (scalar + table-valued); DATA_TYPE is the return type
            pool.request().query(`SELECT ROUTINE_SCHEMA, ROUTINE_NAME, DATA_TYPE AS RETURN_TYPE, CREATED, LAST_ALTERED FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE='FUNCTION' ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME`),
            // PARAMETERS for both procedures and functions in one query
            pool.request().query(`SELECT SPECIFIC_SCHEMA, SPECIFIC_NAME, PARAMETER_NAME, PARAMETER_MODE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_RESULT, ORDINAL_POSITION FROM INFORMATION_SCHEMA.PARAMETERS WHERE PARAMETER_NAME IS NOT NULL ORDER BY SPECIFIC_SCHEMA, SPECIFIC_NAME, ORDINAL_POSITION`)
          ]);

          // ── Tables (existing shape, unchanged) ────────────────────────────
          const tables = {};
          for (const t of tablesR.recordset) {
            const k = `${t.TABLE_SCHEMA}.${t.TABLE_NAME}`;
            tables[k] = { schema: t.TABLE_SCHEMA, name: t.TABLE_NAME, rowCount: parseInt(t.row_count) || 0, columns: [], primaryKeys: [], foreignKeys: [] };
          }
          for (const c of colsR.recordset) {
            const k = `${c.TABLE_SCHEMA}.${c.TABLE_NAME}`;
            if (!tables[k]) continue; // skip view columns here — they're in viewColsR
            let type = c.DATA_TYPE.toUpperCase();
            if (c.CHARACTER_MAXIMUM_LENGTH) type += `(${c.CHARACTER_MAXIMUM_LENGTH===-1?'MAX':c.CHARACTER_MAXIMUM_LENGTH})`;
            else if (c.NUMERIC_PRECISION != null && c.NUMERIC_SCALE != null) type += `(${c.NUMERIC_PRECISION},${c.NUMERIC_SCALE})`;
            tables[k].columns.push({ name: c.COLUMN_NAME, type, nullable: c.IS_NULLABLE === 'YES', isIdentity: c.is_identity === 1, ordinal: c.ORDINAL_POSITION });
          }
          for (const pk of pkR.recordset) {
            const k = `${pk.TABLE_SCHEMA}.${pk.TABLE_NAME}`;
            if (tables[k]) tables[k].primaryKeys.push(pk.COLUMN_NAME);
          }

          // ── Views ────────────────────────────────────────────────────────
          const views = {};
          for (const v of viewsR.recordset) {
            const k = `${v.TABLE_SCHEMA}.${v.TABLE_NAME}`;
            views[k] = { schema: v.TABLE_SCHEMA, name: v.TABLE_NAME, columns: [] };
          }
          for (const c of viewColsR.recordset) {
            const k = `${c.TABLE_SCHEMA}.${c.TABLE_NAME}`;
            if (!views[k]) continue;
            let type = c.DATA_TYPE.toUpperCase();
            if (c.CHARACTER_MAXIMUM_LENGTH) type += `(${c.CHARACTER_MAXIMUM_LENGTH===-1?'MAX':c.CHARACTER_MAXIMUM_LENGTH})`;
            else if (c.NUMERIC_PRECISION != null && c.NUMERIC_SCALE != null) type += `(${c.NUMERIC_PRECISION},${c.NUMERIC_SCALE})`;
            views[k].columns.push({ name: c.COLUMN_NAME, type, nullable: c.IS_NULLABLE === 'YES', ordinal: c.ORDINAL_POSITION });
          }

          // ── Parameters: pre-bucket by routine for fast attachment ────────
          const paramsByRoutine = {};
          for (const p of paramsR.recordset) {
            const k = `${p.SPECIFIC_SCHEMA}.${p.SPECIFIC_NAME}`;
            if (!paramsByRoutine[k]) paramsByRoutine[k] = [];
            let type = (p.DATA_TYPE || '').toUpperCase();
            if (p.CHARACTER_MAXIMUM_LENGTH) type += `(${p.CHARACTER_MAXIMUM_LENGTH===-1?'MAX':p.CHARACTER_MAXIMUM_LENGTH})`;
            else if (p.NUMERIC_PRECISION != null && p.NUMERIC_SCALE != null) type += `(${p.NUMERIC_PRECISION},${p.NUMERIC_SCALE})`;
            paramsByRoutine[k].push({
              name:    p.PARAMETER_NAME,
              type,
              mode:    p.PARAMETER_MODE,        // 'IN' | 'OUT' | 'INOUT'
              isResult:p.IS_RESULT === 'YES',
              ordinal: p.ORDINAL_POSITION
            });
          }

          // ── Procedures ───────────────────────────────────────────────────
          const procedures = procsR.recordset.map(p => ({
            schema:      p.ROUTINE_SCHEMA,
            name:        p.ROUTINE_NAME,
            created:     p.CREATED,
            lastAltered: p.LAST_ALTERED,
            parameters:  paramsByRoutine[`${p.ROUTINE_SCHEMA}.${p.ROUTINE_NAME}`] || []
          }));

          // ── Functions ────────────────────────────────────────────────────
          const functions = funcsR.recordset.map(f => ({
            schema:      f.ROUTINE_SCHEMA,
            name:        f.ROUTINE_NAME,
            returnType:  (f.RETURN_TYPE || '').toUpperCase(),
            created:     f.CREATED,
            lastAltered: f.LAST_ALTERED,
            parameters:  paramsByRoutine[`${f.ROUTINE_SCHEMA}.${f.ROUTINE_NAME}`] || []
          }));

          result = {
            success:    true,
            tables:     Object.values(tables),
            views:      Object.values(views),
            procedures,
            functions
          };
          break;
        }
        case 'execute': {
          const r = await pool.request().query(body.sql);
          result = { success: true, rowsAffected: r.rowsAffected?.[0]||0, recordset: r.recordset||[] };
          break;
        }
        case 'batch': {
          let totalRows=0, errors=0; const results=[];
          for (let i=0;i<body.batchSql.length;i++) {
            try { const r=await pool.request().query(body.batchSql[i]); const rows=r.rowsAffected?.[0]||0; totalRows+=rows; results.push({index:i,success:true,rowsAffected:rows}); }
            catch(e) { errors++; results.push({index:i,success:false,error:e.message}); }
          }
          result = { success:errors===0, totalBatches:body.batchSql.length, totalRowsAffected:totalRows, errors, results };
          break;
        }
        case 'rowcounts': {
          const counts={};
          for (const t of body.tables) { try { const safe=t.replace(/[^a-zA-Z0-9_.\[\]]/g,''); const r=await pool.request().query(`SELECT COUNT(*) AS cnt FROM ${safe}`); counts[t]=r.recordset[0].cnt; } catch(e){counts[t]={error:e.message};} }
          result = { success:true, counts };
          break;
        }
        default:
          result = { success: false, error: `Unknown action: ${action}` };
      }

      await pool.close();
      return { status: 200, headers, body: JSON.stringify(result) };

    } catch (err) {
      ctx.log.error('CAUGHT ERROR:', err.message, err.stack);
      return {
        status: 500,
        headers,
        body: JSON.stringify({
          error: err.message,
          stack: err.stack ? err.stack.split('\n').slice(0,5).join(' | ') : null,
          env: {
            SQL_SERVER:       process.env.SQL_SERVER    || 'NOT SET',
            SQL_DATABASE:     process.env.SQL_DATABASE  || 'NOT SET',
            AZURE_CLIENT_ID:  process.env.AZURE_CLIENT_ID ? 'SET' : 'NOT SET'
          }
        })
      };
    }
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// ROUTE 2: /api/data/{action} — new Cosmos DB data persistence endpoint
// ─────────────────────────────────────────────────────────────────────────────
app.http('data', {
  methods: ['GET', 'POST', 'DELETE', 'OPTIONS'],
  authLevel: 'function',
  route: 'data/{action}',
  handler: async (req, ctx) => {

    if (req.method === 'OPTIONS') return { status: 200, headers: CORS, body: '' };

    // Validate environment
    if (!process.env.COSMOS_ENDPOINT || !process.env.COSMOS_KEY) {
      ctx.log.error('COSMOS_ENDPOINT or COSMOS_KEY not set');
      return err(500, 'Cosmos DB not configured — check COSMOS_ENDPOINT and COSMOS_KEY in Function environment variables');
    }

    const action = req.params.action;

    // Public actions don't require x-user-id (e.g. waitlist submissions from
    // anonymous visitors on /register.html). Every other action still enforces
    // it. Keep this list tight — anything added here is callable by anyone.
    const PUBLIC_ACTIONS = ['waitlist'];

    const userId = getUserId(req);
    if (!PUBLIC_ACTIONS.includes(action) && !userId) {
      return err(401, 'x-user-id header is required');
    }

    ctx.log(`data/${action}${userId ? ' for user: ' + userId : ' (public)'}`);

    try {
      switch (action) {

        // ── SAVE all project data ───────────────────────────────────────────
        // POST /api/data/save
        // Body: { jobs?, project_settings?, project_plan?, connections?, ... }
        //
        // IMPORTANT — merge, don't replace. The client's auto-save debounce
        // sends whatever is currently in localStorage, but localStorage may
        // only contain a subset of the fields at any given moment (pages load
        // different keys at different times). If we upsert with `?? []` / `?? {}`
        // defaults, a save that happens to not include `connections` will
        // silently WIPE the user's connections in Cosmos. That is a real bug
        // that has caused data loss — see: 21 Apr 2026 incident. The merge
        // strategy below preserves any field the client didn't send, which
        // means data loss requires an explicit intent to delete (handled via
        // delete-all or an empty explicit value, not a field omission).
        case 'save': {
          const body = await req.json().catch(() => null);
          if (!body || typeof body !== 'object') return err(400, 'Invalid JSON body');

          const container = getCosmosContainer('projects');

          // Read existing doc first (404 is fine — new user, empty base)
          let existing = {};
          try {
            const { resource } = await container.item(userId, userId).read();
            existing = resource || {};
          } catch (e) {
            if (e.code !== 404) throw e;
          }

          // Whitelist of syncable fields. Anything not in this list is ignored
          // from the body (prevents a malicious or buggy client from writing
          // arbitrary keys like `role: 'admin'` into the projects container).
          const SYNCABLE = [
            'jobs', 'project_settings', 'project_plan', 'connections',
            'saved_connections',
            'performance', 'validation_sources', 'wasis_rules',
            'sql_scripts', 'issues', 'inventory', 'sys_params',
            'projects'   // multi-project array (cygenix_projects)
          ];

          // Only overwrite fields explicitly present in the payload
          const merged = { ...existing };
          const touched = [];
          for (const key of SYNCABLE) {
            if (Object.prototype.hasOwnProperty.call(body, key)) {
              merged[key] = body[key];
              touched.push(key);
            }
          }

          // If the client didn't send ANY recognised field, don't churn Cosmos
          if (touched.length === 0) {
            ctx.log('Save request with no syncable fields — ignored');
            return ok({ saved: false, reason: 'no-syncable-fields', updatedAt: existing.updatedAt || null });
          }

          // Enforce ownership and metadata regardless of what the client sent
          merged.id        = userId;
          merged.userId    = userId;
          merged.updatedAt = new Date().toISOString();

          await container.items.upsert(merged);
          ctx.log(`Saved fields [${touched.join(', ')}] for ${userId}`);

          // Audit log — record exactly which fields were touched, not just keys from body
          await getCosmosContainer('audit').items.create({
            id:        `${userId}-${Date.now()}`,
            userId,
            action:    'save',
            timestamp: merged.updatedAt,
            keys:      touched
          }).catch(e => ctx.log('Audit write failed (non-fatal):', e.message));

          return ok({ saved: true, updatedAt: merged.updatedAt, fields: touched });
        }

        // ── LOAD project data ───────────────────────────────────────────────
        // GET /api/data/load
        case 'load': {
          try {
            const { resource } = await getCosmosContainer('projects')
              .item(userId, userId).read();
            return ok(resource ?? {});
          } catch (e) {
            if (e.code === 404) return ok({});  // new user — no data yet
            throw e;
          }
        }

        // ── GET user record ─────────────────────────────────────────────────
        // GET /api/data/user-get
        case 'user-get': {
          try {
            const { resource } = await getCosmosContainer('users')
              .item(userId, userId).read();
            return ok(resource ?? null);
          } catch (e) {
            if (e.code === 404) return ok(null);
            throw e;
          }
        }

        // ── CREATE user record (called on first login) ──────────────────────
        // POST /api/data/user-create
        // Body: { email, name }
        case 'user-create': {
          const body = await req.json().catch(() => ({}));
          const container = getCosmosContainer('users');

          // Return existing record if already there
          try {
            const { resource } = await container.item(userId, userId).read();
            if (resource) return ok(resource);
          } catch (e) {
            if (e.code !== 404) throw e;
          }

          const user = {
            id:          userId,
            userId,
            email:       body.email || userId,
            name:        body.name  || '',
            plan:        'trial',
            status:      'active',
            createdAt:   new Date().toISOString(),
            trialEndsAt: new Date(Date.now() + 14 * 86400000).toISOString(),
            stripeId:    null
          };
          await container.items.create(user);
          ctx.log('Created new user:', userId);
          return ok(user);
        }

        // ── UPDATE user record (plan, stripeId, status etc.) ────────────────
        // POST /api/data/user-update
        // Body: { plan, status, stripeId, ... }
        case 'user-update': {
          const body = await req.json().catch(() => ({}));
          const container = getCosmosContainer('users');
          let existing = {};
          try {
            const { resource } = await container.item(userId, userId).read();
            existing = resource || {};
          } catch (e) {
            if (e.code !== 404) throw e;
          }
          const updated = {
            ...existing,
            ...body,
            id:        userId,
            userId,
            updatedAt: new Date().toISOString()
          };
          await container.items.upsert(updated);
          return ok(updated);
        }

        // ── GET subscription status ─────────────────────────────────────────
        // GET /api/data/subscription
        case 'subscription': {
          try {
            const { resource } = await getCosmosContainer('subscriptions')
              .item(userId, userId).read();
            return ok(resource ?? { status: 'none' });
          } catch (e) {
            if (e.code === 404) return ok({ status: 'none' });
            throw e;
          }
        }

        // ── UPSERT subscription (called by Stripe webhook) ──────────────────
        // POST /api/data/subscription-update
        // Body: { status, plan, stripeSubscriptionId, currentPeriodEnd, ... }
        case 'subscription-update': {
          const body = await req.json().catch(() => ({}));
          const doc = {
            ...body,
            id:        userId,
            userId,
            updatedAt: new Date().toISOString()
          };
          await getCosmosContainer('subscriptions').items.upsert(doc);
          ctx.log('Subscription updated for:', userId, body.status);
          return ok({ updated: true });
        }

        // ── DELETE all user data (GDPR right to erasure) ────────────────────
        // DELETE /api/data/delete-all
        case 'delete-all': {
          const containers = ['projects', 'users', 'subscriptions'];
          const results = {};
          for (const name of containers) {
            try {
              await getCosmosContainer(name).item(userId, userId).delete();
              results[name] = 'deleted';
            } catch (e) {
              results[name] = e.code === 404 ? 'not found' : `error: ${e.message}`;
            }
          }
          await getCosmosContainer('audit').items.create({
            id:        `${userId}-delete-${Date.now()}`,
            userId,
            action:    'delete-all',
            timestamp: new Date().toISOString(),
            results
          }).catch(() => {});
          return ok({ deleted: true, results });
        }

        // ── PING — test Cosmos connectivity ─────────────────────────────────
        // GET /api/data/ping
        case 'ping': {
          const { resource: db } = await _cosmos
            ?.database(process.env.COSMOS_DATABASE || 'cygenix')
            .read()
            ?? getCosmosContainer('users')
              .database.read();
          return ok({
            ok:       true,
            database: process.env.COSMOS_DATABASE || 'cygenix',
            endpoint: process.env.COSMOS_ENDPOINT?.replace(/\/+$/, '')
          });
        }

        // ── WAITLIST: public endpoint for /register.html submissions ────────
        // POST /api/data/waitlist
        // Body: { name, email, company?, role?, teamSize?, useCase?, source?, website? }
        // `website` is a honeypot — if populated, silently accept but don't
        // store. Returning 200 either way means bots can't tell they've been
        // caught and won't adapt. NO x-user-id required (anonymous visitors).
        case 'waitlist': {
          const body = await req.json().catch(() => null);
          if (!body || typeof body !== 'object') {
            return err(400, 'Invalid JSON body');
          }

          // Honeypot check — pretend success, don't store
          if (body.website && String(body.website).trim().length > 0) {
            ctx.log('Waitlist honeypot triggered — silently dropping');
            return ok({ received: true });
          }

          // Required fields
          const name  = String(body.name  || '').trim();
          const email = String(body.email || '').trim().toLowerCase();

          if (!name || name.length < 2 || name.length > 100) {
            return err(400, 'Name is required (2-100 characters).');
          }
          if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) || email.length > 254) {
            return err(400, 'A valid email address is required.');
          }

          // Optional fields — trimmed, capped, all strings
          const cap = (v, max) => String(v || '').trim().slice(0, max);
          const company  = cap(body.company,  150);
          const role     = cap(body.role,     100);
          const teamSize = cap(body.teamSize,  20);
          const useCase  = cap(body.useCase, 1000);
          const source   = cap(body.source,    50);

          // Basic request metadata for abuse triage later
          const ip        = req.headers.get('x-forwarded-for')?.split(',')[0].trim() || null;
          const userAgent = cap(req.headers.get('user-agent'), 300);
          const referer   = cap(req.headers.get('referer'),    300);

          const now = new Date().toISOString();
          const id  = `wl_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

          const doc = {
            id,
            email,
            name,
            company,
            role,
            teamSize,
            useCase,
            source,
            status:    'new',    // new | contacted | invited | declined
            createdAt: now,
            ip,
            userAgent,
            referer
          };

          try {
            await getCosmosContainer('waitlist').items.create(doc);
            ctx.log('Waitlist submission stored:', email);
          } catch (e) {
            ctx.log.error('Waitlist write failed:', e.message);
            return err(500, 'Could not record your submission. Please email curtis.morris@cygenix.co.uk directly.');
          }

          // Audit log (non-fatal if it fails)
          await getCosmosContainer('audit').items.create({
            id:        `waitlist-${Date.now()}`,
            userId:    email,       // use email as pseudo-userId for audit
            action:    'waitlist-submit',
            timestamp: now,
            company,
            source
          }).catch(e => ctx.log('Audit write failed (non-fatal):', e.message));

          return ok({ received: true, id });
        }

        // ── LIST all waitlist submissions (admin-only UI, enforced later) ───
        // GET /api/data/waitlist-list
        // NOTE: currently behind the same x-user-id trust model as admin-users.
        // Will be role-gated properly in Stage C of the security rollout.
        case 'waitlist-list': {
          try {
            const { resources } = await getCosmosContainer('waitlist').items
              .query('SELECT * FROM c ORDER BY c.createdAt DESC')
              .fetchAll();

            const entries = (resources || []).map(e => ({
              id:        e.id,
              name:      e.name,
              email:     e.email,
              company:   e.company   || '',
              role:      e.role      || '',
              teamSize:  e.teamSize  || '',
              useCase:   e.useCase   || '',
              source:    e.source    || '',
              status:    e.status    || 'new',
              createdAt: e.createdAt || null
            }));

            return ok({ entries, total: entries.length });
          } catch(e) {
            ctx.log('waitlist-list error:', e.message);
            return err(500, `Failed to fetch waitlist: ${e.message}`);
          }
        }

        // ── INVITE user via Netlify Identity ────────────────────────────────────
        // POST /api/data/invite
        // Body: { email, name }
        case 'invite': {
          const body = await req.json().catch(() => ({}));
          const email = body.email;
          if (!email) return err(400, 'email is required');

          const netlifyToken = process.env.NETLIFY_TOKEN;
          const netlifySiteId = process.env.NETLIFY_SITE_ID || 'cygenix.netlify.app';
          if (!netlifyToken) return err(500, 'NETLIFY_TOKEN not configured');

          // GoTrue invite endpoint — runs on the site itself
          // Must use the site's own identity URL, not the Netlify API
          const identityUrl = `https://${netlifySiteId}/.netlify/identity`;

          const inviteRes = await fetch(`${identityUrl}/admin/users`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${netlifyToken}`
            },
            body: JSON.stringify({
              email,
              data: { full_name: body.name || email.split('@')[0] },
              send_email: true
            })
          });

          const inviteData = await inviteRes.json().catch(() => ({}));
          ctx.log('Netlify invite response:', inviteRes.status, JSON.stringify(inviteData));

          if (!inviteRes.ok) {
            return err(inviteRes.status, `Netlify invite failed: ${JSON.stringify(inviteData)}`);
          }

          // Pre-create user record in Cosmos DB
          try {
            const container = getCosmosContainer('users');
            const existCheck = await container.item(email, email).read().catch(() => ({ resource: null }));
            if (!existCheck.resource) {
              await container.items.create({
                id:          email,
                userId:      email,
                email,
                name:        body.name || email.split('@')[0],
                plan:        'trial',
                status:      'invited',
                createdAt:   new Date().toISOString(),
                trialEndsAt: new Date(Date.now() + 14 * 86400000).toISOString(),
                stripeId:    null
              });
            }
          } catch(e) {
            ctx.log('Cosmos pre-create warning (non-fatal):', e.message);
          }

          return ok({ invited: true, email, netlifyResponse: inviteData });
        }

        // ── LIST all users from Cosmos DB ────────────────────────────────────
        // GET /api/data/admin-users
        case 'admin-users': {
          try {
            const { resources } = await getCosmosContainer('users').items
              .query('SELECT * FROM c ORDER BY c.createdAt DESC')
              .fetchAll();

            const users = (resources || []).map(u => ({
              id:          u.id,
              email:       u.email,
              name:        u.name || u.email?.split('@')[0] || '',
              plan:        u.plan        || 'trial',
              status:      u.status      || 'active',
              createdAt:   u.createdAt   || null,
              trialEndsAt: u.trialEndsAt || null,
              updatedAt:   u.updatedAt   || null,
              stripeId:    u.stripeId    || null,
              provider:    u.provider    || 'email'
            }));

            return ok({ users, total: users.length });
          } catch(e) {
            ctx.log('admin-users error:', e.message);
            return err(500, `Failed to fetch users: ${e.message}`);
          }
        }

        // ── AUDIT log — recent activity from Cosmos DB ──────────────────────
        // GET /api/data/audit
        case 'audit': {
          try {
            const container = getCosmosContainer('audit');
            const { resources } = await container.items
              .query('SELECT TOP 50 * FROM c ORDER BY c._ts DESC')
              .fetchAll();
            return ok({ entries: resources || [] });
          } catch(e) {
            ctx.log('Audit query error:', e.message);
            return ok({ entries: [] });
          }
        }

        // ── VERSION-CREATE: snapshot a job ──────────────────────────────────
        // POST /api/data/version-create
        // Body: { jobId, snapshot, label?, note? }
        case 'version-create': {
          const body = await req.json().catch(() => null);
          if (!body || !body.jobId || !body.snapshot) {
            return err(400, 'jobId and snapshot are required');
          }

          const jobId    = String(body.jobId);
          const snapshot = body.snapshot;
          const label    = body.label || 'auto';
          const note     = body.note  || '';
          const hash     = hashSnapshot(snapshot);

          const container = getCosmosContainer('job_versions');

          // Dedupe against most recent version for this job
          const { resources: latest } = await container.items
            .query({
              query: 'SELECT TOP 1 c.hash, c.version FROM c WHERE c.jobId = @jobId ORDER BY c.version DESC',
              parameters: [{ name: '@jobId', value: jobId }]
            }, { partitionKey: jobId })
            .fetchAll();

          if (latest.length && latest[0].hash === hash) {
            return ok({ created: false, reason: 'duplicate', version: latest[0].version });
          }

          const nextVersion = (latest[0]?.version || 0) + 1;
          const now = new Date().toISOString();
          const id  = `ver_${jobId}_${Date.now()}`;

          const doc = {
            id,
            jobId,
            userId,
            version:   nextVersion,
            createdAt: now,
            label,
            note,
            hash,
            snapshot
          };

          await container.items.create(doc);
          ctx.log(`Created version ${nextVersion} for job ${jobId}`);

          await getCosmosContainer('audit').items.create({
            id:        `${userId}-ver-${Date.now()}`,
            userId,
            action:    'version-create',
            jobId,
            version:   nextVersion,
            label,
            timestamp: now
          }).catch(e => ctx.log('Audit write failed (non-fatal):', e.message));

          return ok({ created: true, version: nextVersion, id });
        }

        // ── VERSION-LIST: list all versions for a job ───────────────────────
        // GET /api/data/version-list?jobId=XYZ
        case 'version-list': {
          const jobId = req.query.get('jobId');
          if (!jobId) return err(400, 'jobId query param is required');

          const { resources } = await getCosmosContainer('job_versions').items
            .query({
              query: 'SELECT c.id, c.jobId, c.userId, c.version, c.createdAt, c.label, c.note, c.hash FROM c WHERE c.jobId = @jobId ORDER BY c.version DESC',
              parameters: [{ name: '@jobId', value: jobId }]
            }, { partitionKey: jobId })
            .fetchAll();
          return ok({ versions: resources || [] });
        }

        // ── VERSION-GET: fetch one version with full snapshot ───────────────
        // GET /api/data/version-get?id=ver_xxx&jobId=job_xxx
        case 'version-get': {
          const id    = req.query.get('id');
          const jobId = req.query.get('jobId');
          if (!id || !jobId) return err(400, 'id and jobId query params are required');

          try {
            const { resource } = await getCosmosContainer('job_versions')
              .item(id, jobId).read();
            if (!resource) return err(404, 'Version not found');
            return ok(resource);
          } catch (e) {
            if (e.code === 404) return err(404, 'Version not found');
            throw e;
          }
        }

        // ── EXTEND MEMBERSHIP: admin updates a user's trialEndsAt ───────────
        // POST /api/data/extend-membership
        // Body: { targetEmail, trialEndsAt? (ISO string), addDays? (number),
        //         neverExpires? (boolean) }
        //
        // Caller must be an admin (role === 'admin' in the users container).
        // The caller is identified by x-user-id; the target is named in the
        // body. Exactly one of trialEndsAt / addDays / neverExpires must be
        // supplied. Audit-logged with the caller's id and the resulting
        // trialEndsAt value so extensions are traceable.
        case 'extend-membership': {
          const gate = await requireAdmin(userId);
          if (!gate.ok) return err(gate.code, gate.msg);

          const body = await req.json().catch(() => ({}));
          const targetEmail = (body.targetEmail || '').trim();
          if (!targetEmail) return err(400, 'targetEmail is required');

          // Resolve which option was chosen — exactly one
          const opts = ['trialEndsAt', 'addDays', 'neverExpires']
            .filter(k => Object.prototype.hasOwnProperty.call(body, k));
          if (opts.length !== 1) {
            return err(400, 'Provide exactly one of: trialEndsAt, addDays, neverExpires');
          }

          const container = getCosmosContainer('users');

          // Look up target by email (id and userId are both the email in the
          // current schema, but allow either case for resilience)
          let target = null;
          try {
            const { resource } = await container.item(targetEmail, targetEmail).read();
            target = resource || null;
          } catch (e) {
            if (e.code !== 404) throw e;
          }
          if (!target) {
            // Fall back to a query in case id-as-email is mixed-case in storage
            const { resources } = await container.items
              .query({
                query: 'SELECT * FROM c WHERE LOWER(c.email) = @e',
                parameters: [{ name: '@e', value: targetEmail.toLowerCase() }]
              })
              .fetchAll();
            target = resources && resources[0] ? resources[0] : null;
          }
          if (!target) return err(404, `User not found: ${targetEmail}`);

          // Compute the new trialEndsAt
          let newEndsAt;
          if (body.neverExpires === true) {
            newEndsAt = null;  // null = no expiry
          } else if (typeof body.addDays === 'number' && Number.isFinite(body.addDays)) {
            // Add to whichever is later: now or current trialEndsAt. This
            // matches user expectation — clicking "+30d" on someone whose
            // trial ends next week extends to "next week + 30 days", not
            // "today + 30 days" (which would be a shortening).
            const base = target.trialEndsAt
              ? Math.max(Date.now(), new Date(target.trialEndsAt).getTime())
              : Date.now();
            newEndsAt = new Date(base + body.addDays * 86400000).toISOString();
          } else if (typeof body.trialEndsAt === 'string') {
            // Validate the ISO string before storing
            const parsed = new Date(body.trialEndsAt);
            if (Number.isNaN(parsed.getTime())) return err(400, 'Invalid trialEndsAt date');
            newEndsAt = parsed.toISOString();
          } else {
            return err(400, 'Invalid options for extend-membership');
          }

          // Apply update — only touch trialEndsAt + updatedAt, leave plan,
          // status, role, stripeId etc. untouched
          const updated = {
            ...target,
            trialEndsAt: newEndsAt,
            updatedAt:   new Date().toISOString()
          };
          await container.items.upsert(updated);

          // Audit log so we can see who extended whom and to when
          await getCosmosContainer('audit').items.create({
            id:        `${userId}-extend-${Date.now()}`,
            userId,             // partition key = caller's id
            action:    'extend-membership',
            timestamp: updated.updatedAt,
            target:    targetEmail,
            newTrialEndsAt: newEndsAt,
            mode:      body.neverExpires ? 'never' : (body.addDays ? `+${body.addDays}d` : 'date')
          }).catch(e => ctx.log('Audit write failed (non-fatal):', e.message));

          ctx.log(`Admin ${userId} extended ${targetEmail} → ${newEndsAt || 'never'}`);
          return ok({ updated: true, email: targetEmail, trialEndsAt: newEndsAt });
        }

        // ── PROJECT SUMMARY DOCUMENT — server-rendered HTML for client print ──
        // GET /api/data/project-summary-document?jobId=XYZ
        //
        // Returns a fully styled HTML document for the named job. The browser
        // module loads this and triggers print() to produce a PDF. We render
        // server-side so all data fetching happens once and the page is
        // self-contained (inlined CSS, no asset roundtrips during print).
        //
        // Data sourcing (matches the rest of this file):
        //   - Jobs live embedded in the user's `projects` doc (jobs[] array)
        //   - Audit entries are filtered by jobId across the audit container
        //
        // Auth: same x-user-id model as the other actions. Only the owner
        // can render their own jobs. No cross-user reads.
        case 'project-summary-document': {
          const jobId = req.query.get('jobId');
          if (!jobId) return err(400, 'jobId query param is required');

          // Read the user's project doc and find the job
          let projectsDoc = null;
          try {
            const { resource } = await getCosmosContainer('projects')
              .item(userId, userId).read();
            projectsDoc = resource || {};
          } catch (e) {
            if (e.code !== 404) throw e;
            projectsDoc = {};
          }

          const allJobs = Array.isArray(projectsDoc.jobs) ? projectsDoc.jobs : [];
          const job = allJobs.find(j => j && (j.id === jobId || j.jobId === jobId));
          if (!job) return err(404, `Job not found: ${jobId}`);

          // Audit entries for this job. Audit is partitioned by userId so we
          // scope to the caller; cross-user audit reads aren't possible by
          // design.
          let auditEntries = [];
          try {
            const { resources } = await getCosmosContainer('audit').items
              .query({
                query: 'SELECT * FROM c WHERE c.userId = @uid AND (c.jobId = @jid OR c.action = @act) ORDER BY c.timestamp ASC',
                parameters: [
                  { name: '@uid', value: userId },
                  { name: '@jid', value: jobId },
                  { name: '@act', value: 'job-run' }   // wide net; renderer filters again
                ]
              }, { partitionKey: userId })
              .fetchAll();
            auditEntries = (resources || []).filter(a =>
              a.jobId === jobId || (a.payload && a.payload.jobId === jobId)
            );
          } catch (e) {
            ctx.log('Audit query for project summary doc failed (non-fatal):', e.message);
          }

          // Project context (best-effort — projects[] is an array on the user doc)
          const projectsArr = Array.isArray(projectsDoc.projects) ? projectsDoc.projects : [];
          const project = projectsArr.find(p => p && p.id === job.projectId) || {
            id:   job.projectId || '',
            name: job.projectName || projectsDoc.project_settings?.name || 'Untitled project',
            client:       projectsDoc.project_settings?.client       || '',
            sourceSystem: projectsDoc.project_settings?.sourceSystem || job.source || '',
            targetSystem: projectsDoc.project_settings?.targetSystem || job.target || ''
          };

          ctx.log(`Rendering Project Summary Document for ${userId}, job ${jobId}`);
          const body = renderProjectSummaryDocument(project, job, auditEntries);
          return html(body);
        }

        default:
          return err(404, `Unknown action: ${action}. Valid actions: save, load, user-get, user-create, user-update, subscription, subscription-update, delete-all, ping, invite, admin-users, audit, version-create, version-list, version-get, waitlist, waitlist-list, extend-membership, project-summary-document`);
      }

    } catch (e) {
      ctx.log.error('Cosmos error:', e.message, e.code, e.stack?.split('\n').slice(0,3).join(' | '));
      return err(500, `Cosmos error: ${e.message}`);
    }
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// ROUTE 3: /api/narrative — AI-generated executive summary for migration reports
// Calls Anthropic's Messages API server-side so the API key never touches the
// browser. Requires app setting: ANTHROPIC_API_KEY.
//
// Request body: { report: <slim payload>, migrationId?: string }
// Response:     { narrative: string } on 200; { error: string } otherwise.
// ─────────────────────────────────────────────────────────────────────────────
const NARRATIVE_SYSTEM_PROMPT = `You are writing an executive summary paragraph for a database migration report.
You will receive a JSON object describing the migration outcome, optionally with an "artifacts" array containing
project documents (emails, briefs, notes, sign-offs) that may add context. Write 2-3 short paragraphs of plain prose
(no headings, no bullet points, no markdown beyond **bold** for emphasis of key numbers).

Structure:
1. Opening: state the outcome (complete success, partial success, or failure) and the scale (rows, tables, duration).
2. Middle: contextualize and quantify — compute throughput (rows/sec) if duration is given, mention Was/Is
   transformation rules applied if any, note reconciliation pass rate if present, flag warnings or errors.
   When an anomaly (warning, partial result, reconciliation breach) is plausibly explained by a project artifact,
   reference that artifact inline using its citation tag. Example: "Addresses came in 16 rows short — this matches
   the client's note that inactive entries would be pruned before migration [Note, 14 Apr]."
3. Closing: one line on data integrity / verification status.

Rules:
- Reference actual numbers from the payload. Round large numbers (e.g. "1.2 million rows" not "1,234,567").
- Be factual and neutral. Do not editorialize, recommend, or speculate.
- British English spelling. No emoji. No "the user" or "the client" — use passive voice or "this migration".
- If errors > 0, be honest about that. If reconciliation warned, mention it.
- Output only the prose. No preamble like "Here is the summary:".
- Maximum 180 words.

Artifact rules (only when "artifacts" is present and non-empty):
- Use citation tags EXACTLY as provided in each artifact's "citation" field. Never invent tags.
- Do not quote artifact content verbatim — paraphrase or refer.
- Do not list artifacts at the end; the report has a separate Sources section that does this.
- Prefer citations for explaining anomalies, not perfect outcomes. A successful migration rarely needs one.
- If an artifact contradicts the migration outcome (e.g. client expected something different), flag it honestly.
- If "artifactSummary.skippedDocs" > 0 AND the migration had warnings or partial results, you may add a single
  closing sentence: "A further N document(s) in the project folder were not readable in the browser and were
  not considered." Otherwise, say nothing about skipped artifacts.`;

// Build the user-message content for the narrative call. The artifacts array
// is rendered as a labelled section so the model treats it as evidence rather
// than additional migration data. When no artifacts are present, the message
// shape is identical to the pre-artifact version — backwards compatible with
// older clients that don't send artifacts/artifactSummary.
function buildNarrativeUserMessage(report) {
  const { artifacts, artifactSummary, ...migrationCore } = report;

  let msg = 'Migration report data:\n\n```json\n'
          + JSON.stringify(migrationCore, null, 2)
          + '\n```';

  if (Array.isArray(artifacts) && artifacts.length > 0) {
    msg += '\n\nProject artifacts (use citation tags inline when relevant; do not quote verbatim):\n\n';
    msg += artifacts.map(a => {
      const cite = String(a.citation || '[Doc]').slice(0, 64);
      const name = String(a.name || 'document').slice(0, 200);
      const date = a.addedAt ? new Date(a.addedAt).toISOString().slice(0, 10) : '';
      const text = String(a.text || '').slice(0, 8000);   // belt-and-braces server cap
      return `──── ${cite} · ${name}${date ? ' · ' + date : ''} ────\n${text}`;
    }).join('\n\n');
  }

  if (artifactSummary && artifactSummary.skippedDocs > 0) {
    msg += `\n\n(Note: ${artifactSummary.skippedDocs} document(s) in the project folder were not readable in the browser and are not included above.)`;
  }

  msg += '\n\nWrite the executive summary.';
  return msg;
}

app.http('narrative', {
  methods: ['POST', 'OPTIONS'],
  authLevel: 'function',
  route: 'narrative',
  handler: async (req, ctx) => {

    if (req.method === 'OPTIONS') return { status: 200, headers: CORS, body: '' };

    try {
      const apiKey = process.env.ANTHROPIC_API_KEY;
      if (!apiKey) {
        ctx.log.error('ANTHROPIC_API_KEY not configured');
        return err(500, 'Server not configured — ANTHROPIC_API_KEY missing');
      }

      const body = await req.json().catch(() => null);
      if (!body || typeof body !== 'object') return err(400, 'Invalid JSON body');

      const report = body.report;
      if (!report || typeof report !== 'object') {
        return err(400, 'Missing report payload');
      }

      ctx.log(`narrative for migration: ${body.migrationId || '(no id)'}`);

      const resp = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type':      'application/json',
          'x-api-key':         apiKey,
          'anthropic-version': '2023-06-01'
        },
        body: JSON.stringify({
          model:      'claude-sonnet-4-5',
          max_tokens: 600,
          system:     NARRATIVE_SYSTEM_PROMPT,
          messages: [{
            role: 'user',
            content: buildNarrativeUserMessage(report)
          }]
        })
      });

      if (!resp.ok) {
        const errText = await resp.text();
        ctx.log.error('Anthropic API error', resp.status, errText);
        return err(502, `Upstream error (${resp.status})`);
      }

      const data = await resp.json();
      const narrative = (data.content || [])
        .filter(b => b.type === 'text')
        .map(b => b.text)
        .join('\n')
        .trim();

      if (!narrative) {
        ctx.log.error('Empty narrative in Anthropic response');
        return err(502, 'Empty response from model');
      }

      return ok({ narrative });

    } catch (e) {
      ctx.log.error('Narrative generation failed:', e.message, e.stack?.split('\n').slice(0,3).join(' | '));
      return err(500, `Narrative error: ${e.message}`);
    }
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// PROJECT SUMMARY DOCUMENT — server-side renderer
// ─────────────────────────────────────────────────────────────────────────────
// Builds a fully styled HTML document for a completed migration job. Output is
// designed to be loaded in the browser and printed to PDF via window.print().
//
// v1 sections (live):
//   ✓ Cover + KPI summary
//   ✓ Scope & environment + Was/Is rules
//   ✓ Transformations applied
//   ✓ Reconciliation tables
//   ✓ Run timeline
//   ✓ Sign-off page
//
// v1 sections (stubbed — show "Available in next release" callout):
//   ⊘ Decisions with citations  (needs a `decisions[]` array on the job record)
//   ⊘ Rollback plan             (needs `job_versions` UI complete)
//
// All renderers are defensive — missing fields render as em-dashes or empty
// states, never crashes.

function renderProjectSummaryDocument(project, job, audit) {
  const generatedAt = new Date().toISOString();
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Project Summary Document — ${psd_esc(project.name)}</title>
${psd_stylesheet()}
</head>
<body>
${psd_section_cover(project, job, generatedAt)}
${psd_section_executiveSummary(job)}
${psd_section_scope(project, job)}
${psd_section_decisionsStub()}
${psd_section_transformations(job)}
${psd_section_reconciliation(job)}
${psd_section_timeline(audit)}
${psd_section_rollbackStub()}
${psd_section_signoff(job)}
${psd_printControls()}
</body>
</html>`;
}

function psd_section_cover(project, job, generatedAt) {
  const runDate = job.runCompletedAt ? psd_formatDate(job.runCompletedAt) : psd_formatDate(generatedAt);
  return `
<div class="cover">
  <div class="cover-logo">
    <span class="cover-logo-mark">C</span>
    <span>Cygenix</span>
  </div>
  <div>
    <div class="cover-eyebrow">Project Summary Document</div>
    <div class="cover-title">${psd_esc(project.name)}</div>
    <div class="cover-subtitle">${psd_esc(project.sourceSystem || 'Source')} → ${psd_esc(project.targetSystem || 'Target')}</div>
  </div>
  <div class="cover-meta">
    <div class="cover-meta-item"><div class="cover-meta-label">Project</div><div class="cover-meta-value">${psd_esc(project.name)}</div></div>
    <div class="cover-meta-item"><div class="cover-meta-label">Job ID</div><div class="cover-meta-value">${psd_esc(job.id || job.jobId || '')}</div></div>
    <div class="cover-meta-item"><div class="cover-meta-label">Run Date</div><div class="cover-meta-value">${psd_esc(runDate)}</div></div>
    <div class="cover-meta-item"><div class="cover-meta-label">Version</div><div class="cover-meta-value">${psd_esc(job.version || 'v1')}</div></div>
    <div class="cover-meta-item"><div class="cover-meta-label">Operator</div><div class="cover-meta-value">${psd_esc(job.operator || job.lastRunBy || '—')}</div></div>
    <div class="cover-meta-item"><div class="cover-meta-label">Client</div><div class="cover-meta-value">${psd_esc(project.client || '—')}</div></div>
  </div>
  <div class="cover-footer">
    <span>CONFIDENTIAL — ${psd_esc(project.client || project.name)} &amp; Cygenix</span>
    <span>cygenix.co.uk</span>
  </div>
</div>`;
}

function psd_section_executiveSummary(job) {
  const tables = Array.isArray(job.tables) ? job.tables : [];
  const totalRows = tables.reduce((s, t) => s + (Number(t.targetRows) || 0), 0);
  const recon = psd_computeReconStatus(job);
  const warnings = Array.isArray(job.warnings) ? job.warnings.length : 0;
  const runtime = psd_formatRuntime(job.runtimeSeconds);

  const tableRows = tables.map(t => `
    <tr>
      <td><code>${psd_esc(t.name)}</code></td>
      <td style="text-align:right">${psd_formatNumber(t.sourceRows)}</td>
      <td style="text-align:right">${psd_formatNumber(t.targetRows)}</td>
      <td>${t.sourceRows === t.targetRows
        ? '<span class="badge badge-success">Match</span>'
        : '<span class="badge badge-warn">Δ ' + psd_formatNumber(Math.abs((t.sourceRows||0)-(t.targetRows||0))) + '</span>'}</td>
    </tr>`).join('');

  return `
<div class="page">
  <div class="section-eyebrow">01 · Executive Summary</div>
  <h1 class="section">At a glance</h1>
  <div class="kpi-grid">
    <div class="kpi success">
      <div class="kpi-label">Rows migrated</div>
      <div class="kpi-value">${psd_formatCompact(totalRows)}</div>
      <div class="kpi-sub">Across ${tables.length} tables</div>
    </div>
    <div class="kpi ${recon.className}">
      <div class="kpi-label">Reconciliation</div>
      <div class="kpi-value">${recon.value}</div>
      <div class="kpi-sub">${recon.sub}</div>
    </div>
    <div class="kpi ${warnings === 0 ? 'success' : 'warn'}">
      <div class="kpi-label">Warnings</div>
      <div class="kpi-value">${warnings}</div>
      <div class="kpi-sub">${warnings === 0 ? 'Clean run' : 'See decisions'}</div>
    </div>
    <div class="kpi info">
      <div class="kpi-label">Runtime</div>
      <div class="kpi-value">${runtime}</div>
      <div class="kpi-sub">Job ${psd_esc(job.id || job.jobId || '')}</div>
    </div>
  </div>
  <h2>What was migrated</h2>
  <table>
    <thead><tr><th>Table</th><th style="text-align:right">Source rows</th><th style="text-align:right">Target rows</th><th>Status</th></tr></thead>
    <tbody>${tableRows || '<tr><td colspan="4" style="text-align:center;color:#94a3b8">No table data captured for this job.</td></tr>'}</tbody>
  </table>
</div>`;
}

function psd_section_scope(project, job) {
  const wasIs = (Array.isArray(job.wasIsRules) ? job.wasIsRules : []).map(r => `
    <tr>
      <td><code>${psd_esc(r.table || '*')}</code></td>
      <td><code>${psd_esc(r.field || '*')}</code></td>
      <td>${psd_esc(r.oldVal)}</td>
      <td>${psd_esc(r.newVal)}</td>
      <td style="text-align:right">${psd_formatNumber(r.rowsAffected || 0)}</td>
    </tr>`).join('');

  return `
<div class="page">
  <div class="section-eyebrow">02 · Scope &amp; Environment</div>
  <h1 class="section">Source &amp; target</h1>
  <div class="two-col">
    <div class="panel"><div class="panel-title">Source</div><p style="margin:0"><strong>${psd_esc(project.sourceSystem || '—')}</strong></p></div>
    <div class="panel"><div class="panel-title">Target</div><p style="margin:0"><strong>${psd_esc(project.targetSystem || '—')}</strong></p></div>
  </div>
  <h2>Was/Is rules applied</h2>
  ${wasIs
    ? `<table>
         <thead><tr><th>Table</th><th>Field</th><th>Old</th><th>New</th><th style="text-align:right">Rows rewritten</th></tr></thead>
         <tbody>${wasIs}</tbody>
       </table>`
    : '<p style="color:#64748b">No Was/Is rules active for this job.</p>'}
</div>`;
}

function psd_section_decisionsStub() {
  return `
<div class="page">
  <div class="section-eyebrow">03 · Decisions</div>
  <h1 class="section">Decisions made by Claude</h1>
  <div class="callout callout-info">
    <div class="callout-title">ⓘ Available in the next release</div>
    Structured decision tracking — every type promotion, stored proc translation,
    and schema choice Claude made during analysis, with confidence scores and
    citations back to the project's artifacts — will appear here in v1.1. The
    data is generated today during analysis but not yet persisted.
  </div>
</div>`;
}

function psd_section_transformations(job) {
  const rows = (Array.isArray(job.transformations) ? job.transformations : []).map(t => `
    <tr>
      <td><code>${psd_esc(t.target)}</code></td>
      <td>${psd_esc(t.fromType || '—')}</td>
      <td>${psd_esc(t.toType || '—')}</td>
      <td>${psd_esc(t.reason || '')}</td>
    </tr>`).join('');

  return `
<div class="page">
  <div class="section-eyebrow">04 · Transformations Applied</div>
  <h1 class="section">What changed in the data</h1>
  ${rows
    ? `<table>
         <thead><tr><th>Table.Column</th><th>Source type</th><th>Target type</th><th>Reason</th></tr></thead>
         <tbody>${rows}</tbody>
       </table>`
    : '<p style="color:#64748b">No type transformations recorded for this job.</p>'}
</div>`;
}

function psd_section_reconciliation(job) {
  const r = job.reconciliation || {};
  const sumRows = (Array.isArray(r.sums) ? r.sums : []).map(s => `
    <tr>
      <td><code>${psd_esc(s.table)}</code></td>
      <td>${psd_esc(s.expression)}</td>
      <td style="text-align:right">${psd_esc(s.sourceTotal)}</td>
      <td style="text-align:right">${psd_esc(s.targetTotal)}</td>
      <td>${s.delta == 0
        ? '<span class="badge badge-success">0.00</span>'
        : '<span class="badge badge-warn">' + psd_esc(String(s.delta)) + '</span>'}</td>
    </tr>`).join('');

  const fkRows = (Array.isArray(r.fks) ? r.fks : []).map(f => `
    <tr>
      <td><code>${psd_esc(f.name)}</code></td>
      <td>${psd_esc(f.from)} → ${psd_esc(f.to)}</td>
      <td>${f.orphans === 0
        ? '<span class="badge badge-success">Valid · 0 orphans</span>'
        : '<span class="badge badge-danger">' + Number(f.orphans) + ' orphans</span>'}</td>
    </tr>`).join('');

  return `
<div class="page">
  <div class="section-eyebrow">05 · Validation &amp; Reconciliation</div>
  <h1 class="section">Proof the data matches</h1>
  ${sumRows ? `<h2>Reconciliation by sum</h2>
    <table>
      <thead><tr><th>Table</th><th>Expression</th><th style="text-align:right">Source</th><th style="text-align:right">Target</th><th>Δ</th></tr></thead>
      <tbody>${sumRows}</tbody>
    </table>` : ''}
  ${fkRows ? `<h2>FK validation post-load</h2>
    <table>
      <thead><tr><th>Constraint</th><th>From → To</th><th>Status</th></tr></thead>
      <tbody>${fkRows}</tbody>
    </table>` : ''}
  ${!sumRows && !fkRows ? '<p style="color:#64748b">No reconciliation data captured for this job.</p>' : ''}
</div>`;
}

function psd_section_timeline(audit) {
  if (!Array.isArray(audit) || !audit.length) {
    return `
<div class="page">
  <div class="section-eyebrow">06 · Run Timeline</div>
  <h1 class="section">What happened, minute by minute</h1>
  <p style="color:#64748b">No audit entries recorded for this job.</p>
</div>`;
  }

  const start = new Date(audit[0].timestamp).getTime();
  const items = audit.map(a => {
    const t = new Date(a.timestamp);
    const elapsed = Math.round((t.getTime() - start) / 1000);
    const event = a.event || a.action || a.message || '—';
    const detail = a.detail || (a.label ? `Label: ${a.label}` : '');
    return `
      <div class="timeline-item">
        <div class="timeline-time">${t.toISOString().slice(11,19)}Z · T+${psd_formatElapsed(elapsed)}</div>
        <div class="timeline-event">${psd_esc(event)}</div>
        ${detail ? `<div class="timeline-detail">${psd_esc(detail)}</div>` : ''}
      </div>`;
  }).join('');

  return `
<div class="page">
  <div class="section-eyebrow">06 · Run Timeline</div>
  <h1 class="section">What happened, minute by minute</h1>
  <div class="timeline">${items}</div>
</div>`;
}

function psd_section_rollbackStub() {
  return `
<div class="page">
  <div class="section-eyebrow">07 · Rollback Plan</div>
  <h1 class="section">If something goes wrong post-cutover</h1>
  <div class="callout callout-info">
    <div class="callout-title">ⓘ Available once job versioning ships</div>
    Rollback plans require the per-job version history (currently in development).
    Once <code>job_versions</code> is live, this section will list snapshots
    available for revert and a decision tree for common failure modes.
  </div>
</div>`;
}

function psd_section_signoff(job) {
  return `
<div class="page">
  <div class="section-eyebrow">08 · Sign-off</div>
  <h1 class="section">Acceptance</h1>
  <p>The undersigned confirm that the migration described in this document
    (Job <code>${psd_esc(job.id || job.jobId || '')}</code>, version ${psd_esc(job.version || 'v1')})
    has been completed to the agreed scope and that reconciliation evidence has
    been reviewed.</p>
  <div class="signoff">
    <div class="signature-label" style="margin-bottom:1mm">Migration delivered by</div>
    <div class="signature-name" style="margin-bottom:6mm">Cygenix · cygenix.co.uk · ${psd_esc(job.id || job.jobId || '')} ${psd_esc(job.version || 'v1')}</div>
    <div class="signoff-row">
      <div><div class="signature-line"></div><div class="signature-label">Client — Technical lead</div></div>
      <div><div class="signature-line"></div><div class="signature-label">Client — Data owner</div></div>
    </div>
    <div class="signoff-row" style="margin-top: 12mm">
      <div><div class="signature-line"></div><div class="signature-label">Cygenix operator</div><div class="signature-name">${psd_esc(job.operator || job.lastRunBy || '')}</div></div>
      <div></div>
    </div>
  </div>
</div>`;
}

// ── PSD helpers ──────────────────────────────────────────────────────────────
function psd_esc(v) {
  if (v == null) return '';
  return String(v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function psd_formatNumber(n) {
  if (n == null) return '—';
  return Number(n).toLocaleString('en-GB');
}

function psd_formatCompact(n) {
  if (n == null) return '—';
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return String(n);
}

function psd_formatRuntime(seconds) {
  if (!seconds) return '—';
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return m + 'm ' + s + 's';
}

function psd_formatElapsed(seconds) {
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return m + ':' + String(s).padStart(2, '0');
}

function psd_formatDate(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'long', year: 'numeric' })
    + ', ' + d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
}

function psd_computeReconStatus(job) {
  const tables = Array.isArray(job.tables) ? job.tables : [];
  if (!tables.length) return { value: '—', sub: 'No table data', className: 'info' };
  const matches = tables.filter(t => t.sourceRows === t.targetRows).length;
  if (matches === tables.length) {
    return { value: '100%', sub: 'All totals match', className: 'success' };
  }
  return {
    value: Math.round((matches / tables.length) * 100) + '%',
    sub: (tables.length - matches) + ' mismatch' + (tables.length - matches > 1 ? 'es' : ''),
    className: 'warn'
  };
}

function psd_stylesheet() {
  return `<style>
@page { size: A4; margin: 18mm 16mm 20mm 16mm;
  @bottom-left { content: "Cygenix Project Summary Document"; font-family: Inter, sans-serif; font-size: 8pt; color: #6b7280; }
  @bottom-right { content: "Page " counter(page) " of " counter(pages); font-family: Inter, sans-serif; font-size: 8pt; color: #6b7280; }
}
* { box-sizing: border-box; }
html, body { margin: 0; padding: 0; font-family: Inter, -apple-system, BlinkMacSystemFont, sans-serif; color: #1a1d24; font-size: 10pt; line-height: 1.5; -webkit-print-color-adjust: exact; print-color-adjust: exact; }
.cover { height: 257mm; background: linear-gradient(160deg, #0a0e1a 0%, #1a2240 55%, #2d1b4e 100%); color: #fff; padding: 30mm 18mm 22mm 18mm; position: relative; page-break-after: always; display: flex; flex-direction: column; }
.cover::before { content: ""; position: absolute; top:0;left:0;right:0;bottom:0; background: radial-gradient(circle at 85% 15%, rgba(99, 102, 241, 0.25), transparent 40%), radial-gradient(circle at 15% 85%, rgba(168, 85, 247, 0.18), transparent 45%); pointer-events: none; }
.cover-logo { font-size: 14pt; font-weight: 800; letter-spacing: -0.02em; display: flex; align-items: center; gap: 8px; position: relative; z-index: 1; }
.cover-logo-mark { width: 28px; height: 28px; background: linear-gradient(135deg, #6366f1, #a855f7); border-radius: 7px; display: inline-flex; align-items: center; justify-content: center; color: white; font-size: 14pt; font-weight: 900; }
.cover-eyebrow { margin-top: auto; text-transform: uppercase; letter-spacing: 0.18em; font-size: 9pt; color: #a5b4fc; font-weight: 600; position: relative; z-index: 1; }
.cover-title { font-size: 38pt; font-weight: 800; line-height: 1.05; letter-spacing: -0.025em; margin: 8mm 0 4mm 0; position: relative; z-index: 1; }
.cover-subtitle { font-size: 14pt; font-weight: 400; color: #cbd5e1; max-width: 140mm; line-height: 1.4; position: relative; z-index: 1; }
.cover-meta { margin-top: 18mm; display: grid; grid-template-columns: repeat(2, 1fr); gap: 6mm 10mm; position: relative; z-index: 1; }
.cover-meta-item { border-left: 2px solid #6366f1; padding-left: 4mm; }
.cover-meta-label { text-transform: uppercase; font-size: 7.5pt; letter-spacing: 0.15em; color: #94a3b8; margin-bottom: 1mm; }
.cover-meta-value { font-size: 11pt; font-weight: 500; color: #f1f5f9; }
.cover-footer { margin-top: 14mm; padding-top: 6mm; border-top: 1px solid rgba(255,255,255,0.1); display: flex; justify-content: space-between; font-size: 8.5pt; color: #94a3b8; position: relative; z-index: 1; }
.page { page-break-after: always; padding: 6mm 0; }
.page:last-child { page-break-after: auto; }
h1.section { font-size: 22pt; font-weight: 800; letter-spacing: -0.02em; color: #0a0e1a; margin: 0 0 2mm 0; padding-bottom: 3mm; border-bottom: 3px solid #6366f1; }
.section-eyebrow { text-transform: uppercase; letter-spacing: 0.15em; font-size: 8pt; color: #6366f1; font-weight: 700; margin-bottom: 2mm; }
h2 { font-size: 13pt; font-weight: 700; margin: 8mm 0 3mm 0; letter-spacing: -0.01em; }
.kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 4mm; margin: 6mm 0; }
.kpi { background: linear-gradient(135deg, #f8fafc, #fff); border: 1px solid #e2e8f0; border-radius: 6px; padding: 5mm; position: relative; overflow: hidden; }
.kpi::before { content: ""; position: absolute; top: 0; left: 0; width: 3px; height: 100%; background: var(--accent, #6366f1); }
.kpi-label { font-size: 7.5pt; text-transform: uppercase; letter-spacing: 0.12em; color: #64748b; font-weight: 600; margin-bottom: 2mm; }
.kpi-value { font-size: 20pt; font-weight: 800; letter-spacing: -0.02em; color: #0a0e1a; line-height: 1; }
.kpi-sub { font-size: 8pt; color: #64748b; margin-top: 1.5mm; }
.kpi.success { --accent: #10b981; } .kpi.warn { --accent: #f59e0b; } .kpi.danger { --accent: #ef4444; } .kpi.info { --accent: #6366f1; }
.callout { border-radius: 6px; padding: 4mm 5mm; margin: 4mm 0; border-left: 3px solid; font-size: 9.5pt; }
.callout-success { background: #ecfdf5; border-color: #10b981; color: #064e3b; }
.callout-warn { background: #fffbeb; border-color: #f59e0b; color: #78350f; }
.callout-danger { background: #fef2f2; border-color: #ef4444; color: #7f1d1d; }
.callout-info { background: #eef2ff; border-color: #6366f1; color: #312e81; }
.callout-title { font-weight: 700; margin-bottom: 1mm; }
table { width: 100%; border-collapse: collapse; margin: 3mm 0; font-size: 9pt; }
thead th { background: #f1f5f9; color: #475569; text-align: left; padding: 2.5mm 3mm; font-weight: 600; font-size: 8pt; text-transform: uppercase; letter-spacing: 0.08em; border-bottom: 2px solid #cbd5e1; }
tbody td { padding: 2.5mm 3mm; border-bottom: 1px solid #e2e8f0; vertical-align: top; }
tbody tr:last-child td { border-bottom: none; }
tbody tr:nth-child(even) td { background: #fafbfc; }
.badge { display: inline-block; padding: 1mm 2.5mm; border-radius: 3px; font-size: 7.5pt; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; }
.badge-success { background: #d1fae5; color: #065f46; }
.badge-warn { background: #fef3c7; color: #92400e; }
.badge-danger { background: #fee2e2; color: #991b1b; }
.timeline { position: relative; padding-left: 8mm; }
.timeline::before { content: ""; position: absolute; left: 2mm; top: 2mm; bottom: 2mm; width: 2px; background: linear-gradient(180deg, #6366f1, #a855f7); }
.timeline-item { position: relative; padding-bottom: 5mm; }
.timeline-item::before { content: ""; position: absolute; left: -7.5mm; top: 1.5mm; width: 4mm; height: 4mm; border-radius: 50%; background: #6366f1; border: 2px solid white; box-shadow: 0 0 0 2px #6366f1; }
.timeline-time { font-family: 'JetBrains Mono', monospace; font-size: 8pt; color: #6366f1; font-weight: 600; }
.timeline-event { font-size: 9.5pt; }
.timeline-detail { font-size: 8.5pt; color: #64748b; margin-top: 0.5mm; }
code { font-family: 'JetBrains Mono', 'SF Mono', Menlo, monospace; font-size: 8.5pt; background: #f1f5f9; padding: 0.5mm 1.5mm; border-radius: 3px; color: #6366f1; }
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 5mm; margin: 4mm 0; }
.panel { background: #fafbfc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 4mm; }
.panel-title { font-size: 9pt; font-weight: 700; color: #475569; text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 2mm; }
.signoff { margin-top: 8mm; border: 2px solid #0a0e1a; border-radius: 8px; padding: 8mm; }
.signoff-row { display: grid; grid-template-columns: 1fr 1fr; gap: 8mm; margin-top: 8mm; }
.signature-line { border-bottom: 1.5px solid #1a1d24; height: 14mm; margin-bottom: 1.5mm; }
.signature-label { font-size: 8pt; color: #64748b; text-transform: uppercase; letter-spacing: 0.1em; font-weight: 600; }
.signature-name { font-size: 10pt; font-weight: 600; margin-top: 1mm; }
@media print { .no-print { display: none !important; } }
</style>`;
}

function psd_printControls() {
  return `
<div class="no-print" style="position:fixed;top:12px;right:12px;z-index:999;display:flex;gap:8px;font-family:Inter,sans-serif;">
  <button onclick="window.print()" style="background:#6366f1;color:#fff;border:0;padding:8px 14px;border-radius:6px;font-weight:600;cursor:pointer;font-size:13px;">⬇ Save as PDF</button>
</div>`;
}
