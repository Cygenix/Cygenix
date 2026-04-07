// netlify/functions/db-connect.js
// Connects to SQL Server using the mssql npm package.
// Parses ADO.NET, JDBC, and mssql:// URL format connection strings.

const sql = require('mssql');

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

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };
  if (event.httpMethod !== 'POST') return err('Method not allowed', null, 405);

  let body;
  try { body = JSON.parse(event.body || '{}'); }
  catch (e) { return err('Invalid JSON: ' + e.message, null, 400); }

  const { action, connectionString, database, sql: sqlToRun, batchSql } = body;

  if (!connectionString) return err('connectionString is required', null, 400);

  let config;
  try {
    config = parseConnectionString(connectionString, database);
  } catch (e) {
    return err('Invalid connection string: ' + e.message,
      'Supported formats:\n  ADO.NET: Server=host;Database=db;User Id=user;Password=pass;\n  URL: mssql://user:pass@host:1433/database?encrypt=true&trustServerCertificate=true',
      400);
  }

  let pool;
  try {
    pool = await sql.connect(config);
  } catch (e) {
    return err('Could not connect: ' + e.message, getHint(e.message));
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

          pool.request().query(`
            SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME,
              c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
              c.NUMERIC_PRECISION, c.NUMERIC_SCALE,
              c.IS_NULLABLE, c.COLUMN_DEFAULT, c.ORDINAL_POSITION,
              COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA+'.'+c.TABLE_NAME),c.COLUMN_NAME,'IsIdentity') AS is_identity
            FROM INFORMATION_SCHEMA.COLUMNS c
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
        if (!sqlToRun) return err('sql is required', null, 400);
        if (/^\s*(DROP\s+DATABASE|TRUNCATE\s+TABLE|DELETE\s+FROM\s*\w+\s*$)/i.test(sqlToRun))
          return err('Destructive statement blocked. Use explicit WHERE clauses.', null, 400);
        const r = await pool.request().query(sqlToRun);
        result = { success: true, rowsAffected: r.rowsAffected?.[0]||0, recordset: r.recordset||[] };
        break;
      }

      case 'batch': {
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
        return err(`Unknown action: ${action}`, 'Valid actions: test | schema | execute | batch | rowcounts', 400);
    }

    return ok(result);

  } catch (e) {
    return err('Query error: ' + e.message, getHint(e.message));
  } finally {
    try { await pool.close(); } catch {}
  }
};

// ── Connection string parser ───────────────────────────────────────────────────
function parseConnectionString(cs, dbOverride) {
  cs = cs.trim();

  // ── URL format: mssql://user:pass@host:port/database?options ─────────────────
  if (/^(mssql|sqlserver):\/\//i.test(cs)) {
    // Replace protocol with http for URL API, but handle special chars in password
    // Split manually to avoid URL parsing issues with special chars
    const withoutProto = cs.replace(/^(mssql|sqlserver):\/\//i, '');

    // Extract query string options first
    const qIdx = withoutProto.indexOf('?');
    const base  = qIdx >= 0 ? withoutProto.slice(0, qIdx) : withoutProto;
    const query = qIdx >= 0 ? withoutProto.slice(qIdx + 1) : '';

    // Parse query params
    const qParams = {};
    if (query) {
      for (const p of query.split('&')) {
        const [k, v] = p.split('=');
        if (k) qParams[decodeURIComponent(k).toLowerCase()] = v ? decodeURIComponent(v) : '';
      }
    }

    // Split user:pass@host:port/db
    // Find last @ to handle passwords containing @
    const atIdx = base.lastIndexOf('@');
    const credentials = atIdx >= 0 ? base.slice(0, atIdx) : '';
    const hostPart    = atIdx >= 0 ? base.slice(atIdx + 1) : base;

    // Split user:pass (first colon only — password may contain colons)
    const colonIdx = credentials.indexOf(':');
    const user     = colonIdx >= 0 ? decodeURIComponent(credentials.slice(0, colonIdx)) : credentials;
    const password = colonIdx >= 0 ? decodeURIComponent(credentials.slice(colonIdx + 1)) : '';

    // Split host:port/database
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
        connectTimeout: 15000,
        requestTimeout: 30000,
      }
    };
  }

  // ── ADO.NET / JDBC key=value format ──────────────────────────────────────────
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
  else if (serverRaw.includes('\\')) { /* named instance — use default port */ host = serverRaw; }
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
      connectTimeout: 15000,
      requestTimeout: 30000,
    }
  };
}

function getHint(msg) {
  if (!msg) return null;
  if (msg.includes('ECONNREFUSED')) return 'Connection refused — the server is not reachable. Check the IP/hostname and port, and ensure SQL Server allows connections from Netlify.';
  if (msg.includes('ETIMEDOUT') || msg.includes('timeout')) return 'Connection timed out — the server may be behind a firewall. Check that port 1433 is open to inbound connections.';
  if (msg.includes('Login failed')) return 'Authentication failed — check the username and password in your connection string.';
  if (msg.includes('Cannot open database')) return 'Database not found — check the database name in your connection string.';
  if (msg.includes('SSL') || msg.includes('TLS') || msg.includes('certificate')) return 'SSL error — add trustServerCertificate=true to your connection string.';
  if (msg.includes('password')) return 'Check your password. If it contains special characters like @, /, or ?, URL-encode them or use ADO.NET format instead.';
  return null;
}
