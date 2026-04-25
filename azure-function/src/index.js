// Cosmos DB integration v2 - audit + admin-users + invite
// CM
const { app } = require('@azure/functions');

// Register the Task Agent scheduler module (HTTP route /api/schedules/{action}
// + every-minute timer trigger). Side-effect import — the module's app.http()
// and app.timer() calls run at load time.
require('./schedules');
// ── require('./agent'); temp disabled.

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
            'sql_scripts', 'issues', 'inventory', 'sys_params'
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

        default:
          return err(404, `Unknown action: ${action}. Valid actions: save, load, user-get, user-create, user-update, subscription, subscription-update, delete-all, ping, invite, admin-users, audit, version-create, version-list, version-get, waitlist, waitlist-list`);
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
