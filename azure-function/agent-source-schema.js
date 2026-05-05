// agent-source-schema.js
//
// POST /api/agent/source-schema?code=<FUNC_KEY>
//
// Returns a structured snapshot of the SOURCE database for the Agentive
// Migration page (`agentive_migration.html`). The page renders this as a
// visual node graph: schemas → tables → foreign-key edges, with PII flags.
//
// The function supports the same two connection modes the page already
// understands:
//
//   1) DIRECT mode  — request body is { srcConnString: "mssql://USER:PASS@..." }
//      We connect to that server with `mssql` and run three INFORMATION_SCHEMA
//      / sys queries in parallel.
//
//   2) AZURE / managed-identity mode — request body is empty (or the page
//      passed { srcFnUrl: <this function's own URL> } which is the same thing).
//      We connect to the function's configured SQL_SERVER / SQL_DATABASE via
//      Managed Identity, exactly like the existing `db` function does.
//
// Response shape:
//   {
//     "databaseLabel": "LegalProd on tcp:cygenix-source.database.windows.net",
//     "totals":   { "schemas": N, "tables": N, "columns": N, "piiColumns": N },
//     "schemas":  [{ "name", "tables": [{ "name", "rows", "hasPii", "columnCount" }] }],
//     "edges":    [{ "from": "schema.table", "to": "schema.table" }]
//   }

const { app } = require('@azure/functions');

// ── CORS (matches existing functions in this index.js) ───────────────────────
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, x-user-id',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type':                 'application/json'
};

const ok  = (body)      => ({ status: 200, headers: CORS, body: JSON.stringify(body) });
const err = (code, msg, stack) => ({
  status: code,
  headers: CORS,
  body: JSON.stringify({ error: msg, ...(stack ? { stack } : {}) })
});

// ── PII heuristic — column names that almost certainly contain personal data
// Conservative: false positives just produce an amber border in the UI; false
// negatives mean the agent might propose a mapping that includes a sensitive
// column without flagging it.
const PII_PATTERNS = [
  /^email$/i,        /email_?addr/i,                    /^e_?mail$/i,
  /^phone$/i,        /^mobile$/i,    /^tel(ephone)?$/i,  /phone_?num/i,
  /^ssn$/i,          /national_?ins/i,                   /tax_?id$/i,
  /^dob$/i,          /date_?of_?birth/i,                /birth_?date/i,
  /first_?name/i,    /last_?name/i,  /full_?name/i,     /^surname$/i,
  /^password/i,      /pwd$/i,        /passwd$/i,
  /credit_?card/i,   /card_?num/i,   /cvv/i,            /^iban$/i,    /^bic$/i,
  /passport/i,       /^nino$/i,      /licen[cs]e_?(num|no)/i,
  /home_?addr/i,     /street_?addr/i,/postal_?code/i,   /^postcode$/i, /^zip$/i,
  /salary/i,         /^wage$/i,      /compensation/i,
  /medical/i,        /diagnosis/i,   /^health_/i,
];

function looksLikePII(columnName) {
  if (!columnName) return false;
  return PII_PATTERNS.some(rx => rx.test(columnName));
}

// ── Connection-string parser for direct mode ─────────────────────────────────
// Accepts mssql://USER:PASS@HOST:PORT/DBNAME?encrypt=true&trustServerCertificate=false
function parseMssqlUrl(connString) {
  let u;
  try { u = new URL(connString); }
  catch (e) { throw new Error(`Invalid connection string: ${e.message}`); }
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
    if (cfg.server)   return cfg.server;
    if (cfg.database) return cfg.database;
  } catch { /* fall through */ }
  return 'Source database';
}

// ── Schema introspection — common path used by both modes ───────────────────
// Takes a connected mssql pool, runs the three queries in parallel, returns
// the page's expected response shape.
async function introspect(pool, databaseLabel) {
  const [tablesR, colsR, fksR] = await Promise.all([
    // sys.dm_db_partition_stats gives approximate row counts in O(1) per
    // table — avoids COUNT(*) which would lock & take seconds on big tables.
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
    const e2 = tableMap.get(key);
    e2.count++;
    if (looksLikePII(row.column_name)) {
      e2.hasPii = true;
      e2.piiCount++;
    }
  }

  // Group by schema
  const bySchema = new Map();
  let totalCols = 0;
  let totalPii  = 0;
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
    const k = `${from}->${to}`;
    if (seen.has(k)) continue;
    seen.add(k);
    edges.push({ from, to });
  }

  // Belt & braces: cap at 200 tables per schema. UI only renders top 5;
  // the agent can request more via list_tables once a run starts.
  for (const s of schemas) {
    if (s.tables.length > 200) s.tables = s.tables.slice(0, 200);
  }

  return {
    databaseLabel,
    schemas,
    edges,
    totals: {
      schemas:    schemas.length,
      tables:     tablesR.recordset.length,
      columns:    totalCols,
      piiColumns: totalPii
    }
  };
}

// ── Connect via Managed Identity (Azure mode — same as the `db` function) ────
async function connectViaManagedIdentity(ctx) {
  const { DefaultAzureCredential } = require('@azure/identity');
  const sql = require('mssql');

  ctx.log('[agent-source-schema] connecting via Managed Identity to',
    process.env.SQL_SERVER + '/' + process.env.SQL_DATABASE);

  const credential = new DefaultAzureCredential();
  const tokenResp  = await credential.getToken('https://database.windows.net/.default');

  const pool = await sql.connect({
    server:   process.env.SQL_SERVER,
    database: process.env.SQL_DATABASE,
    options: { encrypt: true, trustServerCertificate: false, enableArithAbort: true },
    authentication: {
      type: 'azure-active-directory-access-token',
      options: { token: tokenResp.token }
    }
  });

  return {
    pool,
    label: `${process.env.SQL_DATABASE} on ${process.env.SQL_SERVER}`
  };
}

// ── Connect via mssql:// connection string (Direct mode) ─────────────────────
async function connectViaConnString(connString, ctx) {
  const sql = require('mssql');
  const cfg = parseMssqlUrl(connString);
  ctx.log(`[agent-source-schema] direct connect: ${cfg.database}@${cfg.server}`);
  const pool = await sql.connect(cfg);
  return { pool, label: describeConnString(connString) };
}

// ── Route registration (v4 programming model) ────────────────────────────────
app.http('agent-source-schema', {
  methods:   ['POST', 'OPTIONS'],
  authLevel: 'function',
  route:     'agent/source-schema',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 204, headers: CORS, body: '' };

    let body = {};
    try { body = await req.json(); } catch { /* empty body is fine — Azure-mode */ }

    const srcConnStr = (body && body.srcConnString || '').trim();
    // srcFnUrl is accepted for symmetry with the page, but we ignore its value
    // — if the page is calling THIS function as the source, we connect to our
    // own configured SQL_SERVER / SQL_DATABASE via Managed Identity. The URL
    // itself is just routing info that already got us here.

    let pool, label;
    try {
      if (srcConnStr) {
        ({ pool, label } = await connectViaConnString(srcConnStr, ctx));
      } else {
        if (!process.env.SQL_SERVER || !process.env.SQL_DATABASE) {
          return err(400,
            'No source connection provided. Either pass srcConnString in the body, ' +
            'or configure SQL_SERVER and SQL_DATABASE on this Function App.');
        }
        ({ pool, label } = await connectViaManagedIdentity(ctx));
      }
    } catch (e) {
      ctx.log(`[agent-source-schema] connection failed: ${e.message}`);
      return err(500, `Could not connect to source database: ${e.message}`,
        (e.stack || '').split('\n').slice(0, 6).join('\n'));
    }

    try {
      const payload = await introspect(pool, label);
      return ok(payload);
    } catch (e) {
      ctx.log(`[agent-source-schema] introspection failed: ${e.message}\n${e.stack || ''}`);
      // In-band debugging: surface stack in the response body since this
      // Azure plan has App Insights / Live Log Stream / Kudu disabled.
      return err(500, e.message,
        (e.stack || '').split('\n').slice(0, 6).join('\n'));
    } finally {
      try { await pool.close(); } catch { /* ignore */ }
    }
  }
});
