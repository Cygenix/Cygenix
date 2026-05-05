// agent-source-schema/index.js
//
// POST /api/agent/source-schema?code=<FUNC_KEY>
//
// Body:
//   { "srcConnString": "mssql://USER:PASS@HOST/DB?encrypt=true" }
//   OR
//   { "srcFnUrl": "https://your-source-fn.azurewebsites.net/api/db?code=..." }
//
// Returns a structured summary of the user's SOURCE database for the
// Agentive Migration page. The page renders this as a visual node graph
// (schemas + tables + foreign-key edges) plus a stats strip.
//
// Design note — why the connection comes from the body:
//   In Cygenix, connections are stored authoritatively in the user's
//   localStorage (via connections.js). The Cosmos copy via
//   cygenix-cosmos-sync.js is eventually-consistent and can lag.
//   So the browser passes its current connection details in the request,
//   matching the pattern used by db-call helpers in dashboard.html.
//
// Response:
//   {
//     "databaseLabel": "LegalProd on tcp:cygenix-source.database.windows.net",
//     "totals":   { "schemas": N, "tables": N, "columns": N, "piiColumns": N },
//     "schemas":  [{ "name", "tables": [{ "name", "rows", "hasPii", "columnCount" }] }],
//     "edges":    [{ "from": "schema.table", "to": "schema.table" }]
//   }

const sql = require('mssql');

// ── CORS (matches existing functions) ────────────────────────────────────────
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, x-user-id',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type':                 'application/json'
};

// ── PII heuristic — column names that almost certainly contain personal data
// Conservative: false positives just produce a flag (amber border in the UI),
// false negatives mean the agent might propose a mapping that includes a
// sensitive column without flagging it.
const PII_PATTERNS = [
  /^email$/i,        /email_?addr/i,                   /^e_?mail$/i,
  /^phone$/i,        /^mobile$/i,    /^tel(ephone)?$/i, /phone_?num/i,
  /^ssn$/i,          /national_?ins/i,                  /tax_?id$/i,
  /^dob$/i,          /date_?of_?birth/i,               /birth_?date/i,
  /first_?name/i,    /last_?name/i,  /full_?name/i,    /^surname$/i,
  /^password/i,      /pwd$/i,        /passwd$/i,
  /credit_?card/i,   /card_?num/i,   /cvv/i,           /^iban$/i,    /^bic$/i,
  /passport/i,       /^nino$/i,      /licen[cs]e_?(num|no)/i,
  /home_?addr/i,     /street_?addr/i,/postal_?code/i,  /^postcode$/i, /^zip$/i,
  /salary/i,         /^wage$/i,      /compensation/i,
  /medical/i,        /diagnosis/i,   /^health_/i,
];

function looksLikePII(columnName) {
  if (!columnName) return false;
  return PII_PATTERNS.some(rx => rx.test(columnName));
}

// ── Connection-string parser ─────────────────────────────────────────────────
// Accepts mssql://USER:PASS@HOST:PORT/DBNAME?encrypt=true&trustServerCertificate=false
// (matches parseMssqlUrl in the agent backend)
function parseMssqlUrl(connString) {
  let u;
  try {
    u = new URL(connString);
  } catch (e) {
    throw new Error(`Invalid connection string: ${e.message}`);
  }
  if (u.protocol !== 'mssql:') {
    throw new Error(`Unsupported protocol: ${u.protocol} (expected mssql:)`);
  }
  const params  = u.searchParams;
  const encrypt = params.get('encrypt') !== 'false';
  const trust   = params.get('trustServerCertificate') === 'true';

  return {
    server:   decodeURIComponent(u.hostname),
    port:     u.port ? Number(u.port) : 1433,
    database: decodeURIComponent((u.pathname || '/').slice(1)),
    user:     decodeURIComponent(u.username || ''),
    password: decodeURIComponent(u.password || ''),
    options: {
      encrypt,
      trustServerCertificate: trust,
      enableArithAbort: true
    },
    requestTimeout:    20000,
    connectionTimeout: 10000
  };
}

function describeConnString(s) {
  if (!s) return 'Source database';
  try {
    const cfg = parseMssqlUrl(s);
    if (cfg.database && cfg.server) return `${cfg.database} on ${cfg.server}`;
    if (cfg.server) return cfg.server;
    if (cfg.database) return cfg.database;
  } catch { /* fall through */ }
  return 'Source database';
}

// ── Azure-mode helper: forward to user's source Azure Function ───────────────
// In Azure mode the user's "source" is *another* Azure Function that wraps a
// SQL Server. The existing /api/db function returns a schema when called
// with {action:'schema'} — translate that response into our shape.
async function fetchSchemaViaSourceFn(srcFnUrl, log) {
  log(`[agent-source-schema] forwarding to source Azure Function: ${srcFnUrl.replace(/code=[^&]*/, 'code=***')}`);
  const r = await fetch(srcFnUrl, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ action: 'schema' }),
    // Cold-start tolerance — same 60s timeout as the agent backend.
    signal:  AbortSignal.timeout(60000)
  });
  if (!r.ok) {
    const text = await r.text().catch(() => '');
    throw new Error(`Source function returned ${r.status}: ${text.slice(0, 300)}`);
  }
  const data = await r.json();
  if (!data || !Array.isArray(data.tables)) {
    throw new Error('Source function returned unexpected shape (expected {tables: [...]})');
  }
  return convertFlatTables(data.tables);
}

// Converts /api/db's flat tables array:
//   [{ schema, name, rowCount, columns: [{name,...}], primaryKeys, foreignKeys: [{column, references}] }]
// into the Agentive page's grouped shape.
function convertFlatTables(tables) {
  const bySchema = new Map();
  let totalCols  = 0;
  let totalPii   = 0;
  const seenEdge = new Set();
  const edges    = [];

  for (const t of tables) {
    const schemaName = t.schema || 'dbo';
    if (!bySchema.has(schemaName)) bySchema.set(schemaName, []);
    const cols = Array.isArray(t.columns) ? t.columns : [];
    let hasPii = false;
    for (const c of cols) {
      totalCols++;
      if (looksLikePII(c.name)) { hasPii = true; totalPii++; }
    }
    bySchema.get(schemaName).push({
      name:        t.name,
      rows:        Number(t.rowCount) || 0,
      columnCount: cols.length,
      hasPii
    });
    // FKs → edges. references is "schema.table(col)" — strip the col,
    // skip self-edges, dedupe (multi-column FKs produce duplicates).
    for (const fk of (t.foreignKeys || [])) {
      const ref = String(fk.references || '').replace(/\s*\([^)]*\)\s*$/, '');
      if (!ref) continue;
      const from = `${schemaName}.${t.name}`;
      const key  = `${from}->${ref}`;
      if (from === ref || seenEdge.has(key)) continue;
      seenEdge.add(key);
      edges.push({ from, to: ref });
    }
  }

  const schemas = [...bySchema.entries()].map(([name, ts]) => ({
    name,
    tables: ts.sort((a, b) => (b.rows || 0) - (a.rows || 0))
  }));

  return {
    schemas,
    edges,
    totals: {
      schemas:    schemas.length,
      tables:     tables.length,
      columns:    totalCols,
      piiColumns: totalPii
    }
  };
}

// ── Direct-mode introspection: query the source DB ourselves ─────────────────
async function fetchSchemaDirect(connString, log) {
  const cfg = parseMssqlUrl(connString);
  log(`[agent-source-schema] direct connect: ${cfg.database}@${cfg.server}`);

  let pool;
  try {
    pool = await sql.connect(cfg);
  } catch (e) {
    throw new Error(`Could not connect to source database: ${e.message}`);
  }

  try {
    // Three queries in parallel — same pattern as /api/db's "schema" action.
    // sys.dm_db_partition_stats gives approximate row counts in O(1) per
    // table, avoiding COUNT(*) which would lock and take seconds on large
    // tables.
    const [tablesR, colsR, fksR] = await Promise.all([
      pool.request().query(`
        SELECT
          s.name  AS schema_name,
          t.name  AS table_name,
          SUM(CASE WHEN p.index_id IN (0,1) THEN p.rows ELSE 0 END) AS row_count
        FROM sys.tables t
        INNER JOIN sys.schemas s              ON s.schema_id = t.schema_id
        LEFT  JOIN sys.dm_db_partition_stats p ON p.object_id = t.object_id
        WHERE s.name NOT IN ('sys','INFORMATION_SCHEMA')
        GROUP BY s.name, t.name
        ORDER BY s.name, t.name
      `),
      pool.request().query(`
        SELECT
          c.TABLE_SCHEMA AS schema_name,
          c.TABLE_NAME   AS table_name,
          c.COLUMN_NAME  AS column_name
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA NOT IN ('sys','INFORMATION_SCHEMA')
      `),
      pool.request().query(`
        SELECT
          OBJECT_SCHEMA_NAME(fk.parent_object_id)     AS fk_schema,
          OBJECT_NAME(fk.parent_object_id)            AS fk_table,
          OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS ref_schema,
          OBJECT_NAME(fk.referenced_object_id)        AS ref_table
        FROM sys.foreign_keys fk
      `)
    ]);

    // Roll columns up by table — count + PII flag
    const tableMap = new Map();
    for (const row of colsR.recordset) {
      const key = `${row.schema_name}.${row.table_name}`;
      if (!tableMap.has(key)) tableMap.set(key, { count: 0, hasPii: false, piiCount: 0 });
      const entry = tableMap.get(key);
      entry.count++;
      if (looksLikePII(row.column_name)) {
        entry.hasPii = true;
        entry.piiCount++;
      }
    }

    // Per-schema groups
    const bySchema = new Map();
    let totalCols  = 0;
    let totalPii   = 0;
    for (const t of tablesR.recordset) {
      const key = `${t.schema_name}.${t.table_name}`;
      const ce  = tableMap.get(key) || { count: 0, hasPii: false, piiCount: 0 };
      totalCols += ce.count;
      totalPii  += ce.piiCount;
      if (!bySchema.has(t.schema_name)) bySchema.set(t.schema_name, []);
      bySchema.get(t.schema_name).push({
        name:        t.table_name,
        rows:        Number(t.row_count) || 0,
        columnCount: ce.count,
        hasPii:      ce.hasPii
      });
    }

    // Sort tables in each schema by row count (largest first) so the most
    // important tables are visible without scrolling in the UI's top-5 cap.
    const schemas = [...bySchema.entries()].map(([name, ts]) => ({
      name,
      tables: ts.sort((a, b) => (b.rows || 0) - (a.rows || 0))
    }));

    // Edge list: skip self-edges and dedupe multi-column FKs.
    const seen  = new Set();
    const edges = [];
    for (const fk of fksR.recordset) {
      if (!fk.fk_schema || !fk.ref_schema) continue;
      const from = `${fk.fk_schema}.${fk.fk_table}`;
      const to   = `${fk.ref_schema}.${fk.ref_table}`;
      if (from === to) continue;
      const key = `${from}->${to}`;
      if (seen.has(key)) continue;
      seen.add(key);
      edges.push({ from, to });
    }

    return {
      databaseLabel: describeConnString(connString),
      schemas,
      edges,
      totals: {
        schemas:    schemas.length,
        tables:     tablesR.recordset.length,
        columns:    totalCols,
        piiColumns: totalPii
      }
    };
  } finally {
    try { await pool.close(); } catch { /* ignore */ }
  }
}

// ── Main handler ─────────────────────────────────────────────────────────────
module.exports = async function (context, req) {
  const log = (...args) => context.log(...args);

  if (req.method === 'OPTIONS') {
    return { status: 204, headers: CORS };
  }

  // Parse the body (Azure already parses JSON when content-type is set;
  // be defensive in case it arrives as a string).
  let body = req.body;
  if (typeof body === 'string') {
    try { body = JSON.parse(body); }
    catch (e) {
      return {
        status: 400, headers: CORS,
        body: JSON.stringify({ error: 'Invalid JSON body' })
      };
    }
  }
  body = body || {};

  const srcFnUrl   = (body.srcFnUrl     || '').trim();
  const srcConnStr = (body.srcConnString || '').trim();

  if (!srcFnUrl && !srcConnStr) {
    return {
      status: 400, headers: CORS,
      body: JSON.stringify({
        error: 'Either srcConnString or srcFnUrl must be provided in the request body'
      })
    };
  }

  try {
    let payload;
    if (srcFnUrl) {
      const inner = await fetchSchemaViaSourceFn(srcFnUrl, log);
      payload = {
        databaseLabel: 'Source database (via Azure Function)',
        ...inner
      };
    } else {
      payload = await fetchSchemaDirect(srcConnStr, log);
    }

    // Belt and braces: cap the response size. A schema with 5000 tables would
    // produce a multi-MB response that the browser then has to render. The
    // UI only displays top 5 per schema; agent can request more via list_tables.
    for (const s of payload.schemas) {
      if (s.tables.length > 200) s.tables = s.tables.slice(0, 200);
    }

    return {
      status: 200, headers: CORS,
      body: JSON.stringify(payload)
    };
  } catch (e) {
    log(`[agent-source-schema] introspection failed: ${e.message}\n${e.stack || ''}`);
    // Surface error message + stack in the body. Curtis can't use App
    // Insights / Log Stream on the current Azure plan — this is how errors
    // become visible in the browser Network tab.
    return {
      status: 500, headers: CORS,
      body: JSON.stringify({
        error: e.message,
        stack: (e.stack || '').split('\n').slice(0, 8).join('\n')
      })
    };
  }
};
