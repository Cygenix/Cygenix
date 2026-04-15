const { app } = require('@azure/functions');

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
          const [tablesR, colsR, pkR] = await Promise.all([
            pool.request().query(`SELECT t.TABLE_SCHEMA, t.TABLE_NAME, COALESCE(p.rows,0) AS row_count FROM INFORMATION_SCHEMA.TABLES t LEFT JOIN sys.tables st ON st.name=t.TABLE_NAME AND SCHEMA_NAME(st.schema_id)=t.TABLE_SCHEMA LEFT JOIN sys.partitions p ON p.object_id=st.object_id AND p.index_id IN(0,1) WHERE t.TABLE_TYPE='BASE TABLE' ORDER BY t.TABLE_SCHEMA,t.TABLE_NAME`),
            pool.request().query(`SELECT c.TABLE_SCHEMA,c.TABLE_NAME,c.COLUMN_NAME,c.DATA_TYPE,c.CHARACTER_MAXIMUM_LENGTH,c.NUMERIC_PRECISION,c.NUMERIC_SCALE,c.IS_NULLABLE,c.ORDINAL_POSITION,COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA+'.'+c.TABLE_NAME),c.COLUMN_NAME,'IsIdentity') AS is_identity FROM INFORMATION_SCHEMA.COLUMNS c ORDER BY c.TABLE_SCHEMA,c.TABLE_NAME,c.ORDINAL_POSITION`),
            pool.request().query(`SELECT tc.TABLE_SCHEMA,tc.TABLE_NAME,kcu.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME=kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA=kcu.TABLE_SCHEMA WHERE tc.CONSTRAINT_TYPE='PRIMARY KEY'`)
          ]);
          const tables = {};
          for (const t of tablesR.recordset) { const k=`${t.TABLE_SCHEMA}.${t.TABLE_NAME}`; tables[k]={schema:t.TABLE_SCHEMA,name:t.TABLE_NAME,rowCount:parseInt(t.row_count)||0,columns:[],primaryKeys:[],foreignKeys:[]}; }
          for (const c of colsR.recordset) { const k=`${c.TABLE_SCHEMA}.${c.TABLE_NAME}`; if(!tables[k])continue; let type=c.DATA_TYPE.toUpperCase(); if(c.CHARACTER_MAXIMUM_LENGTH)type+=`(${c.CHARACTER_MAXIMUM_LENGTH===-1?'MAX':c.CHARACTER_MAXIMUM_LENGTH})`; else if(c.NUMERIC_PRECISION!=null&&c.NUMERIC_SCALE!=null)type+=`(${c.NUMERIC_PRECISION},${c.NUMERIC_SCALE})`; tables[k].columns.push({name:c.COLUMN_NAME,type,nullable:c.IS_NULLABLE==='YES',isIdentity:c.is_identity===1,ordinal:c.ORDINAL_POSITION}); }
          for (const pk of pkR.recordset) { const k=`${pk.TABLE_SCHEMA}.${pk.TABLE_NAME}`; if(tables[k])tables[k].primaryKeys.push(pk.COLUMN_NAME); }
          result = { success: true, tables: Object.values(tables) };
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
    const userId = getUserId(req);
    if (!userId) return err(401, 'x-user-id header is required');

    ctx.log(`data/${action} for user: ${userId}`);

    try {
      switch (action) {

        // ── SAVE all project data ───────────────────────────────────────────
        // POST /api/data/save
        // Body: { jobs, project_settings, project_plan, ... }
        case 'save': {
          const body = await req.json().catch(() => null);
          if (!body || typeof body !== 'object') return err(400, 'Invalid JSON body');

          const doc = {
            id:                userId,   // one document per user, upserted
            userId,
            updatedAt:         new Date().toISOString(),
            jobs:              body.jobs               ?? [],
            project_settings:  body.project_settings   ?? {},
            project_plan:      body.project_plan        ?? {},
            connections:       body.connections         ?? {},
            performance:       body.performance         ?? {},
            validation_sources:body.validation_sources  ?? [],
            wasis_rules:       body.wasis_rules          ?? [],
            sql_scripts:       body.sql_scripts          ?? [],
            issues:            body.issues               ?? [],
            inventory:         body.inventory            ?? [],
          };

          await getCosmosContainer('projects').items.upsert(doc);
          ctx.log('Saved project data to Cosmos');

          // Write audit log entry
          await getCosmosContainer('audit').items.create({
            id:        `${userId}-${Date.now()}`,
            userId,
            action:    'save',
            timestamp: new Date().toISOString(),
            keys:      Object.keys(body)
          }).catch(e => ctx.log('Audit write failed (non-fatal):', e.message));

          return ok({ saved: true, updatedAt: doc.updatedAt });
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

        default:
          return err(404, `Unknown action: ${action}. Valid actions: save, load, user-get, user-create, user-update, subscription, subscription-update, delete-all, ping`);
      }

    } catch (e) {
      ctx.log.error('Cosmos error:', e.message, e.code, e.stack?.split('\n').slice(0,3).join(' | '));
      return err(500, `Cosmos error: ${e.message}`);
    }
  }
});
