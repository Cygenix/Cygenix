const { app } = require('@azure/functions');
const { DefaultAzureCredential } = require('@azure/identity');
const sql = require('mssql');

const SQL_SERVER   = process.env.SQL_SERVER   || '';
const SQL_DATABASE = process.env.SQL_DATABASE || '';
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '*').split(',').map(s => s.trim());

let _pool = null;

async function getPool() {
  if (_pool) return _pool;

  const credential = new DefaultAzureCredential();
  const token = await credential.getToken('https://database.windows.net/.default');

  _pool = await sql.connect({
    server: SQL_SERVER,
    database: SQL_DATABASE,
    options: { encrypt: true, trustServerCertificate: false, enableArithAbort: true },
    authentication: {
      type: 'azure-active-directory-access-token',
      options: { token: token.token }
    }
  });

  return _pool;
}

function corsHeaders(origin) {
  const allowed = ALLOWED_ORIGINS.includes('*') || ALLOWED_ORIGINS.includes(origin);
  return {
    'Access-Control-Allow-Origin': allowed ? origin || '*' : ALLOWED_ORIGINS[0] || '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Content-Type': 'application/json'
  };
}

app.http('db', {
  methods: ['GET', 'POST', 'OPTIONS'],
  authLevel: 'function',
  route: 'db/{action?}',
  handler: async (req, ctx) => {
    const origin = req.headers.get('origin') || '';
    const headers = corsHeaders(origin);

    if (req.method === 'OPTIONS') return { status: 200, headers, body: '' };

    let body = {};
    try { body = await req.json(); } catch {}

    const action = req.params.action || body.action || '';
    ctx.log('action:', action);

    try {
      const pool = await getPool();
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
            user: r.recordset[0].sysuser
          };
          break;
        }

        case 'schema': {
          const [tablesR, colsR, pkR] = await Promise.all([
            pool.request().query(`
              SELECT t.TABLE_SCHEMA, t.TABLE_NAME,
                COALESCE(p.rows, 0) AS row_count
              FROM INFORMATION_SCHEMA.TABLES t
              LEFT JOIN sys.tables st ON st.name = t.TABLE_NAME
                AND SCHEMA_NAME(st.schema_id) = t.TABLE_SCHEMA
              LEFT JOIN sys.partitions p ON p.object_id = st.object_id
                AND p.index_id IN (0,1)
              WHERE t.TABLE_TYPE = 'BASE TABLE'
              ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME`),
            pool.request().query(`
              SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME,
                c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION, c.NUMERIC_SCALE,
                c.IS_NULLABLE, c.COLUMN_DEFAULT, c.ORDINAL_POSITION,
                COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA+'.'+c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS is_identity
              FROM INFORMATION_SCHEMA.COLUMNS c
              ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION`),
            pool.request().query(`
              SELECT tc.TABLE_SCHEMA, tc.TABLE_NAME, kcu.COLUMN_NAME
              FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
              JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
              WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'`)
          ]);

          const tables = {};
          for (const t of tablesR.recordset) {
            const key = `${t.TABLE_SCHEMA}.${t.TABLE_NAME}`;
            tables[key] = { schema: t.TABLE_SCHEMA, name: t.TABLE_NAME, rowCount: parseInt(t.row_count) || 0, columns: [], primaryKeys: [], foreignKeys: [] };
          }
          for (const c of colsR.recordset) {
            const key = `${c.TABLE_SCHEMA}.${c.TABLE_NAME}`;
            if (!tables[key]) continue;
            let type = c.DATA_TYPE.toUpperCase();
            if (c.CHARACTER_MAXIMUM_LENGTH) type += `(${c.CHARACTER_MAXIMUM_LENGTH === -1 ? 'MAX' : c.CHARACTER_MAXIMUM_LENGTH})`;
            else if (c.NUMERIC_PRECISION != null && c.NUMERIC_SCALE != null) type += `(${c.NUMERIC_PRECISION},${c.NUMERIC_SCALE})`;
            tables[key].columns.push({ name: c.COLUMN_NAME, type, nullable: c.IS_NULLABLE === 'YES', isIdentity: c.is_identity === 1, ordinal: c.ORDINAL_POSITION });
          }
          for (const pk of pkR.recordset) {
            const key = `${pk.TABLE_SCHEMA}.${pk.TABLE_NAME}`;
            if (tables[key]) tables[key].primaryKeys.push(pk.COLUMN_NAME);
          }
          result = { success: true, tables: Object.values(tables) };
          break;
        }

        case 'execute': {
          const { sql: sqlStr } = body;
          if (!sqlStr) throw new Error('sql is required');
          const r = await pool.request().query(sqlStr);
          result = { success: true, rowsAffected: r.rowsAffected?.[0] || 0, recordset: r.recordset || [] };
          break;
        }

        case 'batch': {
          const { batchSql } = body;
          if (!Array.isArray(batchSql)) throw new Error('batchSql array required');
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
              results.push({ index: i, success: false, error: e.message });
            }
          }
          result = { success: errors === 0, totalBatches: batchSql.length, totalRowsAffected: totalRows, errors, results };
          break;
        }

        case 'rowcounts': {
          const { tables: tableNames } = body;
          if (!Array.isArray(tableNames)) throw new Error('tables array required');
          const counts = {};
          for (const t of tableNames) {
            try {
              const safe = t.replace(/[^a-zA-Z0-9_.\[\]]/g, '');
              const r = await pool.request().query(`SELECT COUNT(*) AS cnt FROM ${safe}`);
              counts[t] = r.recordset[0].cnt;
            } catch (e) { counts[t] = { error: e.message }; }
          }
          result = { success: true, counts };
          break;
        }

        default:
          throw new Error(`Unknown action: ${action}. Valid: test | schema | execute | batch | rowcounts`);
      }

      return { status: 200, headers, body: JSON.stringify(result) };

    } catch (err) {
      ctx.log.error('Error:', err.message);
      _pool = null; // reset pool on error
      return {
        status: 500,
        headers,
        body: JSON.stringify({ error: err.message, hint: getHint(err.message) })
      };
    }
  }
});

function getHint(msg) {
  if (!msg) return null;
  if (msg.includes('ECONNREFUSED') || msg.includes('ETIMEDOUT')) return 'Cannot reach SQL server — check SQL_SERVER setting.';
  if (msg.includes('Login failed') || msg.includes('token')) return 'Auth failed — check Managed Identity is enabled and has DB access.';
  if (msg.includes('Cannot open database')) return 'Database not found — check SQL_DATABASE setting.';
  return null;
}
