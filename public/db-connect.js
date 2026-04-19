// netlify/functions/db-connect.js
//
// Dialect-aware database proxy. Routes requests to the right driver based on
// the connection string's scheme:
//   postgres://… or postgresql://…    → pg driver
//   anything else                      → mssql driver (existing behaviour)
//
// The response shape is identical across dialects so the client doesn't need
// to care which backend answered:
//   test:       { success, version, database, user }
//   schema:     { success, tables: [{ schema, name, rowCount, columns, primaryKeys, foreignKeys }, ...] }
//   execute:    { success, rowsAffected, recordset }
//   fetch-page: { success, rows: [...], hasMore, offset, pageSize }  — paginated SELECT for streaming
//   batch:      { success, totalBatches, totalRowsAffected, errors, results }
//   rowcounts:  { success, counts: { 'schema.table': n, ... } }
//
// All three dialects mirror the same destructive-statement guard pattern used
// today — it catches common accidents, not determined misuse. This is
// consistent with the existing function rather than an attempt to harden it.
//
// ── TIMEOUTS — IMPORTANT CAVEAT ─────────────────────────────────────────────
// Database driver timeouts below (requestTimeout/statement_timeout = 120s) are
// generous, BUT Netlify Functions have their own hard runtime ceiling:
//   • Free / Starter : 10 seconds
//   • Pro            : 26 seconds
//   • Background fns : 15 minutes  (file must be suffixed -background.js)
// If a schema introspection against a 9k-table DB takes ~40s, the mssql driver
// would happily finish, but Netlify terminates the lambda first and the client
// sees a generic 502/504 or "Failed to fetch".
// Mitigations applied here:
//   1. MSSQL column query no longer uses per-row COLUMNPROPERTY() — massive
//      reduction in query time on large schemas (was the primary bottleneck).
//   2. Driver-side timeouts raised so the driver isn't the cap.
// If large-schema introspection still times out after these changes, the
// right next step is to split schema introspection into its own background
// function (db-schema-background.js) and have the browser poll for result,
// rather than bumping timeouts further.

const mssql = require('mssql');
const { Client: PgClient } = require('pg');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

function ok(data)  { return { statusCode: 200, headers: CORS, body: JSON.stringify(data) }; }
function err(msg, hint, code=500) {
  return { statusCode: code, headers: CORS, body: JSON.stringify({ error: msg, hint: hint || null }) };
}

// Detect which dialect to use from the connection string alone.
// Returns 'postgres' | 'mssql'.
function detectDialect(cs) {
  if (!cs) return 'mssql';
  const trimmed = cs.trim();
  if (/^(postgres|postgresql):\/\//i.test(trimmed)) return 'postgres';
  // Postgres-style keyword params (e.g. "host=… user=…") can also be handed to
  // the pg driver directly. Look for a distinctive postgres keyword that isn't
  // valid in SQL Server connection strings.
  if (/(^|;|\s)driver\s*=\s*postgres/i.test(trimmed)) return 'postgres';
  return 'mssql';
}

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };
  if (event.httpMethod !== 'POST') return err('Method not allowed', null, 405);

  let body;
  try { body = JSON.parse(event.body || '{}'); }
  catch (e) { return err('Invalid JSON: ' + e.message, null, 400); }

  const { action, connectionString, database } = body;
  if (!connectionString) return err('connectionString is required', null, 400);

  const dialect = (body.dialect || detectDialect(connectionString)).toLowerCase();

  try {
    if (dialect === 'postgres') {
      return await handlePostgres(action, connectionString, database, body);
    }
    return await handleMssql(action, connectionString, database, body);
  } catch (e) {
    // Dialect handlers throw when connection or auth fails — catch here so
    // we always return a clean JSON error instead of a 500 stack trace.
    return err(e.message || String(e), e.hint || null, e.statusCode || 500);
  }
};

// ═════════════════════════════════════════════════════════════════════════════
// SQL SERVER (existing code path — untouched except for error routing)
// ═════════════════════════════════════════════════════════════════════════════

async function handleMssql(action, connectionString, database, body) {
  let config;
  try {
    config = parseMssqlConnectionString(connectionString, database);
  } catch (e) {
    return err('Invalid connection string: ' + e.message,
      'Supported formats:\n  ADO.NET: Server=host;Database=db;User Id=user;Password=pass;\n  URL: mssql://user:pass@host:1433/database?encrypt=true&trustServerCertificate=true',
      400);
  }

  let pool;
  try {
    pool = await mssql.connect(config);
  } catch (e) {
    return err('Could not connect: ' + e.message, getMssqlHint(e.message));
  }

  try {
    let result;
    switch (action) {
      case 'test': {
        const r = await pool.request().query(
          'SELECT @@VERSION AS version, DB_NAME() AS dbname, SUSER_SNAME() AS sysuser'
        );
        result = {
          success: true,
          version: r.recordset[0].version.split('\n')[0].trim(),
          database: r.recordset[0].dbname,
          user: r.recordset[0].sysuser,
        };
        break;
      }

      case 'schema': {
        const [tablesR, colsR, pkR, fkR] = await Promise.all([
          pool.request().query(`
            SELECT t.TABLE_SCHEMA, t.TABLE_NAME,
              COALESCE(p.rows,0) AS row_count
            FROM INFORMATION_SCHEMA.TABLES t
            LEFT JOIN sys.tables st ON st.name=t.TABLE_NAME AND SCHEMA_NAME(st.schema_id)=t.TABLE_SCHEMA
            LEFT JOIN sys.partitions p ON p.object_id=st.object_id AND p.index_id IN (0,1)
            WHERE t.TABLE_TYPE='BASE TABLE'
            ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME`),

          // NB: previously used COLUMNPROPERTY(..., 'IsIdentity') per row.
          // On 9k-table schemas that adds 100k+ scalar-function calls and pushes
          // the query past Netlify's function timeout. JOIN to sys.columns /
          // sys.objects directly for the is_identity flag — set-based,
          // cardinality-preserving, orders of magnitude faster.
          pool.request().query(`
            SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME,
              c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
              c.NUMERIC_PRECISION, c.NUMERIC_SCALE,
              c.IS_NULLABLE, c.COLUMN_DEFAULT, c.ORDINAL_POSITION,
              CAST(COALESCE(sc.is_identity, 0) AS INT) AS is_identity
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN sys.schemas ss ON ss.name = c.TABLE_SCHEMA
            LEFT JOIN sys.tables  so ON so.name = c.TABLE_NAME AND so.schema_id = ss.schema_id
            LEFT JOIN sys.columns sc ON sc.object_id = so.object_id AND sc.name = c.COLUMN_NAME
            ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION`),

          pool.request().query(`
            SELECT tc.TABLE_SCHEMA, tc.TABLE_NAME, kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
              ON tc.CONSTRAINT_NAME=kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA=kcu.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE='PRIMARY KEY'`),

          pool.request().query(`
            SELECT OBJECT_SCHEMA_NAME(fk.parent_object_id) AS fk_schema,
              OBJECT_NAME(fk.parent_object_id) AS fk_table,
              COL_NAME(fkc.parent_object_id,fkc.parent_column_id) AS fk_column,
              OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS ref_schema,
              OBJECT_NAME(fk.referenced_object_id) AS ref_table,
              COL_NAME(fkc.referenced_object_id,fkc.referenced_column_id) AS ref_column
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fk.object_id=fkc.constraint_object_id`)
        ]);

        const tables = {};
        for (const t of tablesR.recordset) {
          const key = `${t.TABLE_SCHEMA}.${t.TABLE_NAME}`;
          tables[key] = { schema: t.TABLE_SCHEMA, name: t.TABLE_NAME, rowCount: parseInt(t.row_count)||0, columns: [], primaryKeys: [], foreignKeys: [] };
        }
        for (const c of colsR.recordset) {
          const key = `${c.TABLE_SCHEMA}.${c.TABLE_NAME}`;
          if (!tables[key]) continue;
          let type = c.DATA_TYPE.toUpperCase();
          if (c.CHARACTER_MAXIMUM_LENGTH) type += `(${c.CHARACTER_MAXIMUM_LENGTH===-1?'MAX':c.CHARACTER_MAXIMUM_LENGTH})`;
          else if (c.NUMERIC_PRECISION!=null && c.NUMERIC_SCALE!=null) type += `(${c.NUMERIC_PRECISION},${c.NUMERIC_SCALE})`;
          tables[key].columns.push({ name: c.COLUMN_NAME, type, nullable: c.IS_NULLABLE==='YES', default: c.COLUMN_DEFAULT, isIdentity: c.is_identity===1, ordinal: c.ORDINAL_POSITION });
        }
        for (const pk of pkR.recordset) { const key=`${pk.TABLE_SCHEMA}.${pk.TABLE_NAME}`; if(tables[key]) tables[key].primaryKeys.push(pk.COLUMN_NAME); }
        for (const fk of fkR.recordset) { const key=`${fk.fk_schema}.${fk.fk_table}`; if(tables[key]) tables[key].foreignKeys.push({column:fk.fk_column,references:`${fk.ref_schema}.${fk.ref_table}(${fk.ref_column})`}); }
        result = { success: true, tables: Object.values(tables) };
        break;
      }

      case 'execute': {
        const sqlToRun = body.sql;
        if (!sqlToRun) return err('sql is required', null, 400);
        if (/^\s*(DROP\s+DATABASE|TRUNCATE\s+TABLE|DELETE\s+FROM\s*\w+\s*$)/i.test(sqlToRun))
          return err('Destructive statement blocked. Use explicit WHERE clauses.', null, 400);
        const r = await pool.request().query(sqlToRun);
        result = { success: true, rowsAffected: r.rowsAffected?.[0]||0, recordset: r.recordset||[] };
        break;
      }

      case 'fetch-page': {
        // Paginated SELECT for streaming large tables. Used by project-builder's
        // migration runner. Never exposes user input inside SQL — offset/pageSize
        // are integer-coerced, base SQL has trailing semicolons stripped.
        const baseSql = body.sql;
        if (!baseSql) return err('sql is required', null, 400);
        const offset   = Math.max(0, parseInt(body.offset)   || 0);
        const pageSize = Math.max(1, Math.min(50000, parseInt(body.pageSize) || 1000));
        // Strip trailing semicolons/whitespace so we can wrap the statement
        const cleaned = String(baseSql).trim().replace(/;+\s*$/, '');
        // SQL Server requires ORDER BY for OFFSET/FETCH. If the user's SQL already has
        // one, we just append OFFSET/FETCH. Otherwise we add a stable order by constant
        // so SQL Server accepts the pagination — callers who want deterministic order
        // should include ORDER BY in their SQL.
        const hasOrderBy = /\border\s+by\b/i.test(cleaned);
        const paged = hasOrderBy
          ? `${cleaned}\nOFFSET ${offset} ROWS FETCH NEXT ${pageSize + 1} ROWS ONLY`
          : `${cleaned}\nORDER BY (SELECT NULL) OFFSET ${offset} ROWS FETCH NEXT ${pageSize + 1} ROWS ONLY`;
        // Fetch pageSize+1 to learn whether another page exists without a second call.
        const r = await pool.request().query(paged);
        const rows = r.recordset || [];
        const hasMore = rows.length > pageSize;
        if (hasMore) rows.pop();  // drop the probe row before returning
        result = { success: true, rows, hasMore, offset, pageSize };
        break;
      }

      case 'batch': {
        const batchSql = body.batchSql;
        if (!Array.isArray(batchSql)||!batchSql.length) return err('batchSql array required', null, 400);
        let totalRows=0, errors=0;
        const results=[];
        for (let i=0;i<batchSql.length;i++) {
          try {
            const r = await pool.request().query(batchSql[i]);
            const rows = r.rowsAffected?.[0]||0;
            totalRows+=rows;
            results.push({index:i,success:true,rowsAffected:rows});
          } catch(e) {
            errors++;
            results.push({index:i,success:false,error:e.message,sql:batchSql[i].slice(0,120)});
          }
        }
        result = { success:errors===0, totalBatches:batchSql.length, totalRowsAffected:totalRows, errors, results };
        break;
      }

      case 'rowcounts': {
        const { tables: tableNames } = body;
        if (!Array.isArray(tableNames)) return err('tables array required', null, 400);
        const counts={};
        for (const t of tableNames) {
          try {
            const safe = t.replace(/[^a-zA-Z0-9_.\[\]]/g,'');
            const r = await pool.request().query(`SELECT COUNT(*) AS cnt FROM ${safe}`);
            counts[t] = r.recordset[0].cnt;
          } catch(e) { counts[t]={error:e.message}; }
        }
        result = { success:true, counts };
        break;
      }

      default:
        return err(`Unknown action: ${action}`, 'Valid actions: test | schema | execute | fetch-page | batch | rowcounts', 400);
    }

    return ok(result);

  } catch (e) {
    return err('Query error: ' + e.message, getMssqlHint(e.message));
  } finally {
    try { await pool.close(); } catch {}
  }
}

// ═════════════════════════════════════════════════════════════════════════════
// POSTGRES (new)
// ═════════════════════════════════════════════════════════════════════════════

async function handlePostgres(action, connectionString, database, body) {
  let config;
  try {
    config = parsePostgresConnectionString(connectionString, database);
  } catch (e) {
    return err('Invalid connection string: ' + e.message,
      'Supported formats:\n  URL: postgres://user:pass@host:5432/database?sslmode=require\n  Key-value: host=… port=… dbname=… user=… password=… sslmode=require',
      400);
  }

  let client;
  try {
    client = new PgClient(config);
    await client.connect();
  } catch (e) {
    return err('Could not connect: ' + e.message, getPostgresHint(e));
  }

  try {
    let result;
    switch (action) {
      case 'test': {
        const r = await client.query(`
          SELECT version() AS version,
                 current_database() AS dbname,
                 current_user AS sysuser
        `);
        const row = r.rows[0];
        result = {
          success: true,
          version: row.version.split(' on ')[0].trim(),
          database: row.dbname,
          user: row.sysuser,
        };
        break;
      }

      case 'schema': {
        const [tablesR, colsR, pkR, fkR] = await Promise.all([
          client.query(`
            SELECT n.nspname  AS table_schema,
                   c.relname  AS table_name,
                   COALESCE(c.reltuples, 0)::bigint AS row_count
            FROM   pg_class c
            JOIN   pg_namespace n ON n.oid = c.relnamespace
            WHERE  c.relkind = 'r'
              AND  n.nspname NOT IN ('pg_catalog','information_schema')
              AND  n.nspname NOT LIKE 'pg_toast%'
            ORDER  BY n.nspname, c.relname
          `),

          client.query(`
            SELECT c.table_schema, c.table_name, c.column_name,
                   c.data_type, c.character_maximum_length,
                   c.numeric_precision, c.numeric_scale,
                   c.is_nullable, c.column_default, c.ordinal_position,
                   (c.is_identity = 'YES' OR c.column_default LIKE 'nextval%') AS is_identity
            FROM   information_schema.columns c
            WHERE  c.table_schema NOT IN ('pg_catalog','information_schema')
            ORDER  BY c.table_schema, c.table_name, c.ordinal_position
          `),

          client.query(`
            SELECT tc.table_schema, tc.table_name, kcu.column_name
            FROM   information_schema.table_constraints tc
            JOIN   information_schema.key_column_usage kcu
                   ON tc.constraint_name = kcu.constraint_name
                   AND tc.table_schema   = kcu.table_schema
            WHERE  tc.constraint_type = 'PRIMARY KEY'
              AND  tc.table_schema NOT IN ('pg_catalog','information_schema')
          `),

          client.query(`
            SELECT kcu.table_schema AS fk_schema, kcu.table_name AS fk_table,
                   kcu.column_name  AS fk_column,
                   ccu.table_schema AS ref_schema, ccu.table_name AS ref_table,
                   ccu.column_name  AS ref_column
            FROM   information_schema.referential_constraints rc
            JOIN   information_schema.key_column_usage kcu
                   ON rc.constraint_name = kcu.constraint_name
            JOIN   information_schema.constraint_column_usage ccu
                   ON rc.unique_constraint_name = ccu.constraint_name
            WHERE  kcu.table_schema NOT IN ('pg_catalog','information_schema')
          `)
        ]);

        const tables = {};
        for (const t of tablesR.rows) {
          const key = `${t.table_schema}.${t.table_name}`;
          tables[key] = {
            schema: t.table_schema,
            name: t.table_name,
            rowCount: parseInt(t.row_count) || 0,
            columns: [],
            primaryKeys: [],
            foreignKeys: []
          };
        }
        for (const c of colsR.rows) {
          const key = `${c.table_schema}.${c.table_name}`;
          if (!tables[key]) continue;
          let type = (c.data_type || '').toUpperCase();
          if (c.character_maximum_length) type += `(${c.character_maximum_length})`;
          else if (c.numeric_precision != null && c.numeric_scale != null) type += `(${c.numeric_precision},${c.numeric_scale})`;
          tables[key].columns.push({
            name: c.column_name,
            type,
            nullable: c.is_nullable === 'YES',
            default: c.column_default,
            isIdentity: c.is_identity === true,
            ordinal: c.ordinal_position,
          });
        }
        for (const pk of pkR.rows) {
          const key = `${pk.table_schema}.${pk.table_name}`;
          if (tables[key]) tables[key].primaryKeys.push(pk.column_name);
        }
        for (const fk of fkR.rows) {
          const key = `${fk.fk_schema}.${fk.fk_table}`;
          if (tables[key]) tables[key].foreignKeys.push({
            column: fk.fk_column,
            references: `${fk.ref_schema}.${fk.ref_table}(${fk.ref_column})`,
          });
        }
        result = { success: true, tables: Object.values(tables) };
        break;
      }

      case 'execute': {
        const sqlToRun = body.sql;
        if (!sqlToRun) return err('sql is required', null, 400);
        if (/^\s*(DROP\s+DATABASE|TRUNCATE\s+TABLE|DELETE\s+FROM\s+\w+\s*$)/i.test(sqlToRun))
          return err('Destructive statement blocked. Use explicit WHERE clauses.', null, 400);
        const r = await client.query(sqlToRun);
        result = {
          success: true,
          rowsAffected: r.rowCount || 0,
          recordset: r.rows || [],
        };
        break;
      }

      case 'fetch-page': {
        // Paginated SELECT using Postgres LIMIT/OFFSET. Same fetch-pageSize+1 trick
        // as MSSQL so we can detect hasMore without a second query.
        const baseSql = body.sql;
        if (!baseSql) return err('sql is required', null, 400);
        const offset   = Math.max(0, parseInt(body.offset)   || 0);
        const pageSize = Math.max(1, Math.min(50000, parseInt(body.pageSize) || 1000));
        const cleaned = String(baseSql).trim().replace(/;+\s*$/, '');
        // Postgres doesn't require ORDER BY for LIMIT/OFFSET, but without one row
        // order is undefined and pagination isn't repeatable. We don't add a
        // synthetic ORDER BY because Postgres doesn't accept OFFSET-of-constant
        // ordering the way MSSQL does — caller should include ORDER BY if they
        // want deterministic pages.
        const paged = `${cleaned}\nLIMIT ${pageSize + 1} OFFSET ${offset}`;
        const r = await client.query(paged);
        const rows = r.rows || [];
        const hasMore = rows.length > pageSize;
        if (hasMore) rows.pop();
        result = { success: true, rows, hasMore, offset, pageSize };
        break;
      }

      case 'batch': {
        const batchSql = body.batchSql;
        if (!Array.isArray(batchSql) || !batchSql.length) return err('batchSql array required', null, 400);
        let totalRows = 0, errors = 0;
        const results = [];
        for (let i = 0; i < batchSql.length; i++) {
          try {
            const r = await client.query(batchSql[i]);
            const rows = r.rowCount || 0;
            totalRows += rows;
            results.push({ index: i, success: true, rowsAffected: rows });
          } catch (e) {
            errors++;
            results.push({ index: i, success: false, error: e.message, sql: batchSql[i].slice(0, 120) });
          }
        }
        result = { success: errors === 0, totalBatches: batchSql.length, totalRowsAffected: totalRows, errors, results };
        break;
      }

      case 'rowcounts': {
        const { tables: tableNames } = body;
        if (!Array.isArray(tableNames)) return err('tables array required', null, 400);
        const counts = {};
        for (const t of tableNames) {
          try {
            const safe = t.replace(/[^a-zA-Z0-9_."]/g, '');
            const r = await client.query(`SELECT COUNT(*)::bigint AS cnt FROM ${safe}`);
            counts[t] = parseInt(r.rows[0].cnt);
          } catch (e) { counts[t] = { error: e.message }; }
        }
        result = { success: true, counts };
        break;
      }

      default:
        return err(`Unknown action: ${action}`, 'Valid actions: test | schema | execute | fetch-page | batch | rowcounts', 400);
    }

    return ok(result);

  } catch (e) {
    return err('Query error: ' + e.message, getPostgresHint(e));
  } finally {
    try { await client.end(); } catch {}
  }
}

// ═════════════════════════════════════════════════════════════════════════════
// CONNECTION STRING PARSERS
// ═════════════════════════════════════════════════════════════════════════════

function parseMssqlConnectionString(cs, dbOverride) {
  cs = cs.trim();

  if (/^(mssql|sqlserver):\/\//i.test(cs)) {
    const withoutProto = cs.replace(/^(mssql|sqlserver):\/\//i, '');

    const qIdx = withoutProto.indexOf('?');
    const base  = qIdx >= 0 ? withoutProto.slice(0, qIdx) : withoutProto;
    const query = qIdx >= 0 ? withoutProto.slice(qIdx + 1) : '';

    const qParams = {};
    if (query) {
      for (const p of query.split('&')) {
        const [k, v] = p.split('=');
        if (k) qParams[decodeURIComponent(k).toLowerCase()] = v ? decodeURIComponent(v) : '';
      }
    }

    const atIdx = base.lastIndexOf('@');
    const credentials = atIdx >= 0 ? base.slice(0, atIdx) : '';
    const hostPart    = atIdx >= 0 ? base.slice(atIdx + 1) : base;

    const colonIdx = credentials.indexOf(':');
    const user     = colonIdx >= 0 ? decodeURIComponent(credentials.slice(0, colonIdx)) : credentials;
    const password = colonIdx >= 0 ? decodeURIComponent(credentials.slice(colonIdx + 1)) : '';

    const slashIdx = hostPart.indexOf('/');
    const hostPort = slashIdx >= 0 ? hostPart.slice(0, slashIdx) : hostPart;
    const dbPath   = slashIdx >= 0 ? hostPart.slice(slashIdx + 1) : '';

    const lastColon = hostPort.lastIndexOf(':');
    const host = lastColon >= 0 && !hostPort.includes('[') ? hostPort.slice(0, lastColon) : hostPort;
    const port = lastColon >= 0 ? parseInt(hostPort.slice(lastColon + 1)) || 1433 : 1433;

    const database = dbOverride || dbPath || qParams['database'] || qParams['initial catalog'] || 'master';
    const encrypt  = qParams['encrypt'] !== 'false';
    const trustCert = qParams['trustservercertificate'] === 'true' || !encrypt;

    if (!host) throw new Error('Could not parse host from URL connection string');

    return {
      server: host,
      port,
      database,
      user: user || undefined,
      password: password || undefined,
      options: {
        encrypt,
        trustServerCertificate: trustCert,
        enableArithAbort: true,
        connectTimeout: 30000,
        requestTimeout: 120000,
      }
    };
  }

  const pairs = {};
  for (const part of cs.split(/[;&]+/)) {
    const eq = part.indexOf('=');
    if (eq < 0) continue;
    const key = part.slice(0, eq).trim().toLowerCase().replace(/\s+/g, '');
    const val = part.slice(eq + 1).trim();
    pairs[key] = val;
  }

  const serverRaw = pairs['server'] || pairs['datasource'] || pairs['data source'] || pairs['host'];
  if (!serverRaw) throw new Error('Could not find server/host in connection string. Expected: Server=host;Database=db;User Id=user;Password=pass;');

  let host = serverRaw, port = 1433;
  if (serverRaw.includes(',')) { const parts = serverRaw.split(','); host = parts[0]; port = parseInt(parts[1])||1433; }
  else if (serverRaw.includes('\\')) { host = serverRaw; }
  else if (serverRaw.lastIndexOf(':') > 0) { const c = serverRaw.lastIndexOf(':'); host = serverRaw.slice(0,c); port = parseInt(serverRaw.slice(c+1))||1433; }

  const database = dbOverride || pairs['database'] || pairs['initialcatalog'] || pairs['initial catalog'] || pairs['databasename'] || 'master';
  const user     = pairs['user id'] || pairs['userid'] || pairs['uid'] || pairs['user'];
  const password = pairs['password'] || pairs['pwd'] || pairs['pass'];
  const encrypt  = pairs['encrypt'] !== 'false' && pairs['encrypt'] !== 'False';
  const trustCert = pairs['trustservercertificate'] === 'true' || pairs['trust server certificate'] === 'true' || pairs['trustservercertificate'] === 'True';

  return {
    server: host.trim(),
    port,
    database,
    user: user?.trim() || undefined,
    password: password?.trim() || undefined,
    options: {
      encrypt,
      trustServerCertificate: trustCert,
      enableArithAbort: true,
      connectTimeout: 30000,
      requestTimeout: 120000,
    }
  };
}

function parsePostgresConnectionString(cs, dbOverride) {
  cs = cs.trim();

  if (/^(postgres|postgresql):\/\//i.test(cs)) {
    const withoutProto = cs.replace(/^(postgres|postgresql):\/\//i, '');
    const qIdx = withoutProto.indexOf('?');
    const base  = qIdx >= 0 ? withoutProto.slice(0, qIdx) : withoutProto;
    const query = qIdx >= 0 ? withoutProto.slice(qIdx + 1) : '';

    const qParams = {};
    if (query) {
      for (const p of query.split('&')) {
        const [k, v] = p.split('=');
        if (k) qParams[decodeURIComponent(k).toLowerCase()] = v ? decodeURIComponent(v) : '';
      }
    }

    const atIdx = base.lastIndexOf('@');
    const credentials = atIdx >= 0 ? base.slice(0, atIdx) : '';
    const hostPart    = atIdx >= 0 ? base.slice(atIdx + 1) : base;

    const colonIdx = credentials.indexOf(':');
    const user     = colonIdx >= 0 ? decodeURIComponent(credentials.slice(0, colonIdx)) : credentials;
    const password = colonIdx >= 0 ? decodeURIComponent(credentials.slice(colonIdx + 1)) : '';

    const slashIdx = hostPart.indexOf('/');
    const hostPort = slashIdx >= 0 ? hostPart.slice(0, slashIdx) : hostPart;
    const dbPath   = slashIdx >= 0 ? hostPart.slice(slashIdx + 1) : '';

    const lastColon = hostPort.lastIndexOf(':');
    const host = lastColon >= 0 && !hostPort.includes('[') ? hostPort.slice(0, lastColon) : hostPort;
    const port = lastColon >= 0 ? parseInt(hostPort.slice(lastColon + 1)) || 5432 : 5432;

    const database = dbOverride || dbPath || qParams['database'] || 'postgres';

    if (!host) throw new Error('Could not parse host from URL connection string');

    return buildPgConfig({ host, port, database, user, password, qParams });
  }

  // libpq key=value format
  const pairs = {};
  const re = /(\w+)\s*=\s*(?:'([^']*)'|"([^"]*)"|(\S+))/g;
  let m;
  while ((m = re.exec(cs)) !== null) {
    pairs[m[1].toLowerCase()] = m[2] ?? m[3] ?? m[4];
  }

  const host = pairs['host'] || pairs['hostaddr'];
  if (!host) throw new Error('Could not find host in connection string. Expected: host=… port=5432 dbname=… user=… password=… sslmode=require');

  const port = parseInt(pairs['port']) || 5432;
  const database = dbOverride || pairs['dbname'] || pairs['database'] || 'postgres';
  const user     = pairs['user'];
  const password = pairs['password'];

  return buildPgConfig({ host, port, database, user, password, qParams: pairs });
}

function buildPgConfig({ host, port, database, user, password, qParams }) {
  // SSL handling — pg's default is no SSL. Cloud providers (Supabase, Neon,
  // Azure Flexible Server, RDS) all require SSL. Default to 'require' when the
  // host looks like a managed provider, otherwise no SSL (local dev).
  const sslmode = (qParams['sslmode'] || '').toLowerCase();
  let ssl;
  if (sslmode === 'disable') {
    ssl = false;
  } else if (sslmode === 'require') {
    ssl = { rejectUnauthorized: false };
  } else if (sslmode === 'verify-ca' || sslmode === 'verify-full') {
    ssl = { rejectUnauthorized: true };
  } else {
    const cloudHost = /\.(supabase\.co|neon\.tech|azure\.com|rds\.amazonaws\.com|database\.cloud\.google\.com|render\.com|railway\.app)$/i.test(host);
    ssl = cloudHost ? { rejectUnauthorized: false } : false;
  }

  return {
    host,
    port,
    database,
    user: user || undefined,
    password: password || undefined,
    ssl,
    connectionTimeoutMillis: 30000,
    // Bumped for large-schema introspection (9k+ tables). Was 30s, now 120s.
    query_timeout: 120000,
    statement_timeout: 120000,
  };
}

// ═════════════════════════════════════════════════════════════════════════════
// ERROR HINTS
// ═════════════════════════════════════════════════════════════════════════════

function getMssqlHint(msg) {
  if (!msg) return null;
  if (msg.includes('ECONNREFUSED')) return 'Connection refused — the server is not reachable. Check the IP/hostname and port, and ensure SQL Server allows connections from Netlify.';
  if (msg.includes('ETIMEDOUT') || msg.includes('timeout')) return 'Connection timed out — the server may be behind a firewall. Check that port 1433 is open to inbound connections.';
  if (msg.includes('Login failed')) return 'Authentication failed — check the username and password in your connection string.';
  if (msg.includes('Cannot open database')) return 'Database not found — check the database name in your connection string.';
  if (msg.includes('SSL') || msg.includes('TLS') || msg.includes('certificate')) return 'SSL error — add trustServerCertificate=true to your connection string.';
  if (msg.includes('password')) return 'Check your password. If it contains special characters like @, /, or ?, URL-encode them or use ADO.NET format instead.';
  return null;
}

function getPostgresHint(e) {
  // pg errors carry SQLSTATE codes — see https://www.postgresql.org/docs/current/errcodes-appendix.html
  const code = e?.code;
  const msg  = e?.message || '';
  if (code === 'ECONNREFUSED') return 'Connection refused — the server is not reachable. Check the host and port.';
  if (code === 'ETIMEDOUT' || msg.includes('timeout')) return 'Connection timed out — the server may be behind a firewall. Check that port 5432 is open to inbound connections from Netlify.';
  if (code === '28P01' || msg.includes('password authentication failed')) return 'Password authentication failed — check the username and password.';
  if (code === '28000') return 'Authentication failed — check your credentials and the auth method expected by this server (pg_hba.conf).';
  if (code === '3D000' || msg.includes('does not exist')) return 'Database not found — check the database name in your connection string.';
  if (msg.includes('SSL') || msg.includes('TLS') || msg.includes('certificate')) return 'SSL error — most cloud providers need sslmode=require. Add ?sslmode=require to your URL, or sslmode=require to your key-value string.';
  if (msg.includes('no pg_hba.conf entry')) return "Server rejected connection from Netlify. Add Netlify's egress IPs to the database's allow-list, or use a connection pooler like PgBouncer.";
  return null;
}
