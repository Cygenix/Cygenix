// netlify/functions/db-connect.js
// Connects to SQL Server (on-prem or cloud) using the mssql npm package.
// Handles: schema introspection, SQL execution, batch inserts with progress.
//
// IMPORTANT: This function requires the mssql package.
// Add to your repo root: package.json with "mssql": "^10.0.4"
// Netlify will auto-install it on deploy.

const sql = require('mssql');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };
  if (event.httpMethod !== 'POST') return { statusCode: 405, headers: CORS, body: JSON.stringify({ error: 'Method not allowed' }) };

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch (e) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Invalid JSON: ' + e.message }) };
  }

  const { action, connectionString, database, sql: sqlToRun, batchSql } = body;

  if (!connectionString) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'connectionString is required.' }) };
  }

  // Parse the connection string into mssql config
  let config;
  try {
    config = parseConnectionString(connectionString, database);
  } catch (e) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Invalid connection string: ' + e.message }) };
  }

  // Set a 20 second query timeout
  config.requestTimeout = 20000;
  config.connectionTimeout = 10000;

  let pool;
  try {
    pool = await sql.connect(config);
  } catch (e) {
    return {
      statusCode: 500, headers: CORS,
      body: JSON.stringify({
        error: 'Could not connect to database: ' + e.message,
        hint: getConnectionHint(e.message)
      })
    };
  }

  try {
    let result;

    switch (action) {

      // ── TEST: just verify connectivity ──────────────────────────────────────
      case 'test': {
        const r = await pool.request().query('SELECT @@VERSION AS version, DB_NAME() AS dbname, SYSTEM_USER AS sysuser');
        result = {
          success: true,
          version: r.recordset[0].version.split('\n')[0],
          database: r.recordset[0].dbname,
          user: r.recordset[0].sysuser
        };
        break;
      }

      // ── SCHEMA: read all tables, columns, types, PKs, FKs ──────────────────
      case 'schema': {
        // Get all user tables
        const tablesResult = await pool.request().query(`
          SELECT
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            p.rows AS row_count
          FROM INFORMATION_SCHEMA.TABLES t
          LEFT JOIN sys.tables st ON st.name = t.TABLE_NAME
          LEFT JOIN sys.partitions p ON p.object_id = st.object_id AND p.index_id IN (0,1)
          WHERE t.TABLE_TYPE = 'BASE TABLE'
            AND t.TABLE_SCHEMA NOT IN ('sys','INFORMATION_SCHEMA')
          ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
        `);

        // Get all columns with types
        const colsResult = await pool.request().query(`
          SELECT
            c.TABLE_SCHEMA,
            c.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            c.ORDINAL_POSITION,
            COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA+'.'+c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS is_identity
          FROM INFORMATION_SCHEMA.COLUMNS c
          WHERE c.TABLE_SCHEMA NOT IN ('sys','INFORMATION_SCHEMA')
          ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
        `);

        // Get primary keys
        const pkResult = await pool.request().query(`
          SELECT
            tc.TABLE_SCHEMA,
            tc.TABLE_NAME,
            kcu.COLUMN_NAME
          FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
          JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
          WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        `);

        // Get foreign keys
        const fkResult = await pool.request().query(`
          SELECT
            fk.name AS fk_name,
            OBJECT_SCHEMA_NAME(fk.parent_object_id) AS fk_schema,
            OBJECT_NAME(fk.parent_object_id) AS fk_table,
            COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS fk_column,
            OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS ref_schema,
            OBJECT_NAME(fk.referenced_object_id) AS ref_table,
            COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ref_column
          FROM sys.foreign_keys fk
          JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        `);

        // Build structured schema
        const tables = {};
        for (const t of tablesResult.recordset) {
          const key = `${t.TABLE_SCHEMA}.${t.TABLE_NAME}`;
          tables[key] = {
            schema: t.TABLE_SCHEMA,
            name: t.TABLE_NAME,
            rowCount: parseInt(t.row_count) || 0,
            columns: [],
            primaryKeys: [],
            foreignKeys: []
          };
        }

        for (const c of colsResult.recordset) {
          const key = `${c.TABLE_SCHEMA}.${c.TABLE_NAME}`;
          if (!tables[key]) continue;
          let fullType = c.DATA_TYPE.toUpperCase();
          if (c.CHARACTER_MAXIMUM_LENGTH) fullType += `(${c.CHARACTER_MAXIMUM_LENGTH === -1 ? 'MAX' : c.CHARACTER_MAXIMUM_LENGTH})`;
          else if (c.NUMERIC_PRECISION && c.NUMERIC_SCALE !== null) fullType += `(${c.NUMERIC_PRECISION},${c.NUMERIC_SCALE})`;
          tables[key].columns.push({
            name: c.COLUMN_NAME,
            type: fullType,
            nullable: c.IS_NULLABLE === 'YES',
            default: c.COLUMN_DEFAULT,
            isIdentity: c.is_identity === 1,
            ordinal: c.ORDINAL_POSITION
          });
        }

        for (const pk of pkResult.recordset) {
          const key = `${pk.TABLE_SCHEMA}.${pk.TABLE_NAME}`;
          if (tables[key]) tables[key].primaryKeys.push(pk.COLUMN_NAME);
        }

        for (const fk of fkResult.recordset) {
          const key = `${fk.fk_schema}.${fk.fk_table}`;
          if (tables[key]) tables[key].foreignKeys.push({
            column: fk.fk_column,
            references: `${fk.ref_schema}.${fk.ref_table}(${fk.ref_column})`
          });
        }

        result = { success: true, tables: Object.values(tables) };
        break;
      }

      // ── EXECUTE: run a single SQL statement ─────────────────────────────────
      case 'execute': {
        if (!sqlToRun) return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'sql is required for execute action' }) };

        // Safety: block destructive operations unless explicitly flagged
        const dangerous = /^\s*(DROP\s+DATABASE|DROP\s+TABLE|TRUNCATE\s+TABLE|DELETE\s+FROM\s+\w+\s*$)/i;
        if (dangerous.test(sqlToRun)) {
          return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Destructive statement blocked. Use explicit WHERE clauses or run in SSMS.' }) };
        }

        const r = await pool.request().query(sqlToRun);
        result = {
          success: true,
          rowsAffected: r.rowsAffected?.[0] || 0,
          recordset: r.recordset || []
        };
        break;
      }

      // ── BATCH: run multiple INSERT statements, return progress ──────────────
      case 'batch': {
        if (!batchSql || !Array.isArray(batchSql)) {
          return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'batchSql array is required for batch action' }) };
        }

        const results = [];
        let totalRowsAffected = 0;
        let errors = 0;

        for (let i = 0; i < batchSql.length; i++) {
          const stmt = batchSql[i];
          try {
            const r = await pool.request().query(stmt);
            const rows = r.rowsAffected?.[0] || 0;
            totalRowsAffected += rows;
            results.push({ index: i, success: true, rowsAffected: rows });
          } catch (e) {
            errors++;
            results.push({ index: i, success: false, error: e.message, sql: stmt.slice(0, 100) });
            // Continue with remaining batches — don't abort on single failure
          }
        }

        result = {
          success: errors === 0,
          totalBatches: batchSql.length,
          totalRowsAffected,
          errors,
          results
        };
        break;
      }

      // ── ROW COUNT: check actual row counts post-migration ───────────────────
      case 'rowcounts': {
        const { tables: tableNames } = body;
        if (!tableNames || !Array.isArray(tableNames)) {
          return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'tables array is required' }) };
        }
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
        result = { success: true, counts };
        break;
      }

      default:
        return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: `Unknown action: ${action}. Use test|schema|execute|batch|rowcounts` }) };
    }

    return { statusCode: 200, headers: CORS, body: JSON.stringify(result) };

  } catch (e) {
    return {
      statusCode: 500, headers: CORS,
      body: JSON.stringify({ error: 'Query error: ' + e.message, hint: getConnectionHint(e.message) })
    };
  } finally {
    try { await pool.close(); } catch {}
  }
};

// ── Connection string parser ──────────────────────────────────────────────────
// Supports:
//   ADO.NET: Server=myserver;Database=mydb;User Id=myuser;Password=mypass;
//   JDBC: jdbc:sqlserver://myserver:1433;databaseName=mydb;user=myuser;password=mypass
//   URL: mssql://user:pass@server:1433/database
function parseConnectionString(cs, dbOverride) {
  cs = cs.trim();

  // URL format: mssql://user:pass@host:port/database
  if (cs.startsWith('mssql://') || cs.startsWith('sqlserver://')) {
    const url = new URL(cs.replace(/^sqlserver/, 'mssql').replace(/^mssql/, 'http'));
    return {
      server: url.hostname,
      port: parseInt(url.port) || 1433,
      database: dbOverride || url.pathname.replace('/', '') || 'master',
      user: decodeURIComponent(url.username),
      password: decodeURIComponent(url.password),
      options: { encrypt: true, trustServerCertificate: true, enableArithAbort: true }
    };
  }

  // ADO.NET / JDBC key=value format
  const pairs = {};
  // Handle both ; and & as separators
  for (const part of cs.split(/[;&]+/)) {
    const eq = part.indexOf('=');
    if (eq < 0) continue;
    const key = part.slice(0, eq).trim().toLowerCase().replace(/\s+/g, '');
    const val = part.slice(eq + 1).trim();
    pairs[key] = val;
  }

  const server = pairs['server'] || pairs['datasource'] || pairs['data source'] || pairs['host'];
  if (!server) throw new Error('Could not find server/host in connection string');

  // Extract port from server if included (Server=host,1433 or host:1433)
  let host = server, port = 1433;
  if (server.includes(',')) { [host, port] = server.split(','); port = parseInt(port); }
  else if (server.includes(':')) { [host, port] = server.split(':'); port = parseInt(port); }

  const database = dbOverride ||
    pairs['database'] || pairs['initial catalog'] || pairs['initialcatalog'] ||
    pairs['databasename'] || 'master';

  const user = pairs['user id'] || pairs['userid'] || pairs['uid'] || pairs['user'];
  const password = pairs['password'] || pairs['pwd'] || pairs['pass'];

  // Azure AD / Windows Auth
  const useWinAuth = !user && !password;

  return {
    server: host.trim(),
    port,
    database,
    user: user?.trim(),
    password: password?.trim(),
    options: {
      encrypt: pairs['encrypt'] !== 'false',
      trustServerCertificate: pairs['trustservercertificate'] === 'true' || pairs['trust server certificate'] === 'true',
      enableArithAbort: true,
      integratedSecurity: useWinAuth
    }
  };
}

function getConnectionHint(msg) {
  if (msg.includes('ECONNREFUSED')) return 'Connection refused — check server address and port, and ensure SQL Server is accessible from the internet or Netlify\'s IP range.';
  if (msg.includes('ETIMEDOUT') || msg.includes('timeout')) return 'Connection timed out — the server may be behind a firewall. Whitelist Netlify\'s outbound IPs or use Azure SQL which accepts internet connections.';
  if (msg.includes('Login failed')) return 'Authentication failed — check username and password. For Azure SQL, ensure the user has db_datareader and db_datawriter roles.';
  if (msg.includes('Cannot open database')) return 'Database not found — check the database name in your connection string.';
  if (msg.includes('SSL') || msg.includes('TLS') || msg.includes('certificate')) return 'SSL/TLS error — try adding TrustServerCertificate=True to your connection string.';
  return null;
}
