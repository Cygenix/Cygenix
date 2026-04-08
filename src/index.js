// src/index.js
// Cygenix Azure Function — SQL Server proxy
//
// Authentication: System-Assigned Managed Identity (no passwords needed)
// When deployed to Azure: uses ManagedIdentityCredential automatically
// When running locally: uses DefaultAzureCredential (Azure CLI login)
//
// Environment variables required in Azure Function App settings:
//   SQL_SERVER   — e.g. cygenix.database.windows.net
//   SQL_DATABASE — e.g. CygenixMigrations
//   ALLOWED_ORIGINS — comma-separated e.g. https://cygenix.netlify.app

const { app } = require('@azure/functions');
const { DefaultAzureCredential, ManagedIdentityCredential } = require('@azure/identity');
const sql = require('mssql');

// ── Config ────────────────────────────────────────────────────────────────────
const SQL_SERVER   = process.env.SQL_SERVER   || 'cygenix.database.windows.net';
const SQL_DATABASE = process.env.SQL_DATABASE || 'master';
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '*').split(',').map(s => s.trim());
const USE_MANAGED_IDENTITY = process.env.USE_MANAGED_IDENTITY !== 'false';

// Cache the token credential so we don't create it on every request
let _credential = null;
function getCredential() {
  if (!_credential) {
    // In Azure: ManagedIdentityCredential uses the Function App's system identity
    // Locally: DefaultAzureCredential falls back to Azure CLI / env vars
    _credential = process.env.AZURE_CLIENT_ID
      ? new ManagedIdentityCredential(process.env.AZURE_CLIENT_ID)
      : new DefaultAzureCredential();
  }
  return _credential;
}

// Cache the connection pool (reuse across warm invocations)
let _pool = null;
let _poolExpiry = 0;

async function getPool() {
  // Refresh pool every 45 minutes (tokens expire at 60 min)
  if (_pool && Date.now() < _poolExpiry) return _pool;

  if (_pool) {
    try { await _pool.close(); } catch {}
    _pool = null;
  }

  let config;

  if (USE_MANAGED_IDENTITY) {
    // Get an access token for Azure SQL
    const credential = getCredential();
    const tokenResponse = await credential.getToken(
      'https://database.windows.net/.default'
    );

    config = {
      server: SQL_SERVER,
      database: SQL_DATABASE,
      options: {
        encrypt: true,
        trustServerCertificate: false,
        enableArithAbort: true,
        connectTimeout: 15000,
        requestTimeout: 30000,
      },
      authentication: {
        type: 'azure-active-directory-access-token',
        options: {
          token: tokenResponse.token
        }
      }
    };
  } else {
    // Fallback: SQL auth via environment variables (for testing only)
    config = {
      server: SQL_SERVER,
      database: SQL_DATABASE,
      user: process.env.SQL_USER,
      password: process.env.SQL_PASSWORD,
      options: {
        encrypt: true,
        trustServerCertificate: false,
        enableArithAbort: true,
        connectTimeout: 15000,
        requestTimeout: 30000,
      }
    };
  }

  _pool = await sql.connect(config);
  _poolExpiry = Date.now() + 45 * 60 * 1000; // 45 minutes
  return _pool;
}

// ── CORS helper ───────────────────────────────────────────────────────────────
function getCorsHeaders(requestOrigin) {
  const origin = ALLOWED_ORIGINS.includes('*') || ALLOWED_ORIGINS.includes(requestOrigin)
    ? requestOrigin || '*'
    : ALLOWED_ORIGINS[0];
  return {
    'Access-Control-Allow-Origin': origin,
    'Access-Control-Allow-Headers': 'Content-Type, x-api-key',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Content-Type': 'application/json'
  };
}

// ── Main handler ──────────────────────────────────────────────────────────────
app.http('db', {
  methods: ['GET', 'POST', 'OPTIONS'],
  authLevel: 'function',  // Requires ?code=<function-key> — keeps endpoint private
  route: 'db/{action?}',
  handler: async (request, context) => {
    const origin = request.headers.get('origin') || '';
    const corsHeaders = getCorsHeaders(origin);

    // Handle preflight
    if (request.method === 'OPTIONS') {
      return { status: 200, headers: corsHeaders, body: '' };
    }

    const action = request.params.action || (await request.json().catch(() => ({}))).action;

    context.log(`Cygenix DB API: action=${action}`);

    let body = {};
    try {
      body = await request.json();
    } catch {}

    // Override action from route param if present
    if (request.params.action) body.action = request.params.action;

    try {
      const pool = await getPool();
      const result = await handleAction(pool, body, context);
      return {
        status: 200,
        headers: corsHeaders,
        body: JSON.stringify(result)
      };
    } catch (err) {
      context.log.error('DB error:', err.message);
      // Reset pool on connection errors so next request gets a fresh one
      if (err.code === 'ECONNRESET' || err.code === 'ESOCKET' || err.message.includes('connection')) {
        _pool = null;
        _poolExpiry = 0;
      }
      return {
        status: 500,
        headers: corsHeaders,
        body: JSON.stringify({
          error: err.message,
          hint: getHint(err.message),
          code: err.code
        })
      };
    }
  }
});

// ── Action handlers ───────────────────────────────────────────────────────────
async function handleAction(pool, body, context) {
  const { action } = body;

  switch (action) {

    // TEST: verify connectivity and return server info
    case 'test': {
      const r = await pool.request().query(
        'SELECT @@VERSION AS version, DB_NAME() AS dbname, SYSTEM_USER AS sysuser, SUSER_SNAME() AS loginname'
      );
      const row = r.recordset[0];
      return {
        success: true,
        server: SQL_SERVER,
        database: row.dbname,
        version: row.version.split('\n')[0].trim(),
        user: row.loginname || row.sysuser,
        authMode: USE_MANAGED_IDENTITY ? 'Managed Identity' : 'SQL Auth'
      };
    }

    // SCHEMA: read all tables, columns, PKs, FKs, row counts
    case 'schema': {
      const [tablesR, colsR, pkR, fkR] = await Promise.all([
        pool.request().query(`
          SELECT t.TABLE_SCHEMA, t.TABLE_NAME,
            COALESCE(p.rows, 0) AS row_count
          FROM INFORMATION_SCHEMA.TABLES t
          LEFT JOIN sys.tables st ON st.name = t.TABLE_NAME
            AND SCHEMA_NAME(st.schema_id) = t.TABLE_SCHEMA
          LEFT JOIN sys.partitions p ON p.object_id = st.object_id
            AND p.index_id IN (0, 1)
          WHERE t.TABLE_TYPE = 'BASE TABLE'
          ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME`),

        pool.request().query(`
          SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME,
            c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION, c.NUMERIC_SCALE,
            c.IS_NULLABLE, c.COLUMN_DEFAULT, c.ORDINAL_POSITION,
            COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA+'.'+c.TABLE_NAME),
              c.COLUMN_NAME, 'IsIdentity') AS is_identity
          FROM INFORMATION_SCHEMA.COLUMNS c
          ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION`),

        pool.request().query(`
          SELECT tc.TABLE_SCHEMA, tc.TABLE_NAME, kcu.COLUMN_NAME
          FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
          JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
          WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'`),

        pool.request().query(`
          SELECT
            OBJECT_SCHEMA_NAME(fk.parent_object_id) AS fk_schema,
            OBJECT_NAME(fk.parent_object_id) AS fk_table,
            COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS fk_column,
            OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS ref_schema,
            OBJECT_NAME(fk.referenced_object_id) AS ref_table,
            COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ref_column
          FROM sys.foreign_keys fk
          JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id`)
      ]);

      // Build structured schema object
      const tables = {};
      for (const t of tablesR.recordset) {
        const key = `${t.TABLE_SCHEMA}.${t.TABLE_NAME}`;
        tables[key] = {
          schema: t.TABLE_SCHEMA,
          name: t.TABLE_NAME,
          rowCount: parseInt(t.row_count) || 0,
          columns: [], primaryKeys: [], foreignKeys: []
        };
      }

      for (const c of colsR.recordset) {
        const key = `${c.TABLE_SCHEMA}.${c.TABLE_NAME}`;
        if (!tables[key]) continue;
        let type = c.DATA_TYPE.toUpperCase();
        if (c.CHARACTER_MAXIMUM_LENGTH) type += `(${c.CHARACTER_MAXIMUM_LENGTH === -1 ? 'MAX' : c.CHARACTER_MAXIMUM_LENGTH})`;
        else if (c.NUMERIC_PRECISION != null && c.NUMERIC_SCALE != null) type += `(${c.NUMERIC_PRECISION},${c.NUMERIC_SCALE})`;
        tables[key].columns.push({
          name: c.COLUMN_NAME,
          type,
          nullable: c.IS_NULLABLE === 'YES',
          default: c.COLUMN_DEFAULT,
          isIdentity: c.is_identity === 1,
          ordinal: c.ORDINAL_POSITION
        });
      }
      for (const pk of pkR.recordset) {
        const key = `${pk.TABLE_SCHEMA}.${pk.TABLE_NAME}`;
        if (tables[key]) tables[key].primaryKeys.push(pk.COLUMN_NAME);
      }
      for (const fk of fkR.recordset) {
        const key = `${fk.fk_schema}.${fk.fk_table}`;
        if (tables[key]) tables[key].foreignKeys.push({
          column: fk.fk_column,
          references: `${fk.ref_schema}.${fk.ref_table}(${fk.ref_column})`
        });
      }

      return { success: true, tables: Object.values(tables) };
    }

    // EXECUTE: run a single SQL statement (SELECT / DDL)
    case 'execute': {
      const { sql: sqlStr } = body;
      if (!sqlStr) throw new Error('sql is required');
      // Block accidental destructive statements
      if (/^\s*(DROP\s+DATABASE|TRUNCATE\s+TABLE|DELETE\s+FROM\s*\w+\s*$)/i.test(sqlStr)) {
        throw new Error('Destructive statement blocked. Use explicit WHERE clauses.');
      }
      const r = await pool.request().query(sqlStr);
      return {
        success: true,
        rowsAffected: r.rowsAffected?.[0] || 0,
        recordset: r.recordset || []
      };
    }

    // BATCH: run array of INSERT statements with per-statement error handling
    case 'batch': {
      const { batchSql } = body;
      if (!Array.isArray(batchSql) || batchSql.length === 0) throw new Error('batchSql array required');

      let totalRows = 0, errors = 0;
      const results = [];

      for (let i = 0; i < batchSql.length; i++) {
        try {
          const r = await pool.request().query(batchSql[i]);
          const rows = r.rowsAffected?.[0] || 0;
          totalRows += rows;
          results.push({ index: i, success: true, rowsAffected: rows });
        } catch (e) {
          errors++;
          results.push({ index: i, success: false, error: e.message, sql: batchSql[i].slice(0, 120) });
          context.log.warn(`Batch ${i} failed: ${e.message}`);
        }
      }

      return { success: errors === 0, totalBatches: batchSql.length, totalRowsAffected: totalRows, errors, results };
    }

    // ROWCOUNTS: verify row counts post-migration
    case 'rowcounts': {
      const { tables: tableNames } = body;
      if (!Array.isArray(tableNames)) throw new Error('tables array required');
      const counts = {};
      for (const t of tableNames) {
        try {
          const safe = t.replace(/[^a-zA-Z0-9_.\[\]]/g, '');
          const r = await pool.request().query(`SELECT COUNT(*) AS cnt FROM ${safe}`);
          counts[t] = r.recordset[0].cnt;
        } catch (e) {
          counts[t] = { error: e.message };
        }
      }
      return { success: true, counts };
    }

    default:
      throw new Error(`Unknown action: ${action}. Supported: test | schema | execute | batch | rowcounts`);
  }
}

function getHint(msg) {
  if (msg.includes('ECONNREFUSED') || msg.includes('ETIMEDOUT')) return 'Cannot reach the SQL server. Check SQL_SERVER setting and that the Azure SQL firewall allows Azure services.';
  if (msg.includes('Login failed') || msg.includes('token')) return 'Authentication failed. Ensure Managed Identity is enabled on the Function App and the identity has been added to the database with db_datareader + db_datawriter roles.';
  if (msg.includes('Cannot open database')) return 'Database not found. Check SQL_DATABASE environment variable.';
  if (msg.includes('permission') || msg.includes('EXECUTE')) return 'Permission denied. The Managed Identity may need additional database roles.';
  return null;
}
