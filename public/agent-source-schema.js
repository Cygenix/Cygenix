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

// ── Theme classifier ─────────────────────────────────────────────────────
// Rule-based pass that groups tables by what they semantically contain.
// Each theme has a list of regex patterns — a table matches if ANY of its
// signals (table name, column names) match the theme. Themes are checked
// in priority order: a table only goes into ONE group (the first match).
//
// A table earns "score points" per theme based on how many distinct signals
// match. Highest scoring theme wins; ties broken by theme order. Tables
// with zero matches go to "Other".
const THEMES = [
  {
    id: 'matters',
    name: 'Matters & Cases',
    icon: '⚖',
    description: 'Legal matters, cases, files, and case-related work',
    // Priority: legal-domain table names are usually unambiguous
    patterns: {
      tableName: [/matter/i, /^case/i, /^cases/i, /^file/i, /^files/i, /docket/i, /lawsuit/i, /claim/i, /^rm[a-z]*folder/i],
      columnName: [/matter_?id/i, /matter_?num/i, /case_?(id|num)/i, /docket/i]
    }
  },
  {
    id: 'contacts',
    name: 'Contacts & People',
    icon: '👤',
    description: 'Clients, contacts, addresses, and people-related data',
    patterns: {
      tableName: [/^client/i, /contact/i, /address/i, /person/i, /^people$/i, /customer/i, /party/i, /attorney/i, /lawyer/i, /staff/i, /employee/i, /user(?!_log)/i],
      columnName: [/first_?name/i, /last_?name/i, /full_?name/i, /^name$/i, /email/i, /phone/i, /^mobile$/i, /addr/i, /^city$/i, /^postcode$/i, /^zip$/i]
    }
  },
  {
    id: 'financial',
    name: 'Financial Data',
    icon: '💰',
    description: 'Invoices, payments, ledger entries, billing, and money',
    patterns: {
      tableName: [/invoice/i, /payment/i, /^bill/i, /billing/i, /^gl[_-]?entr/i, /general_?ledger/i, /journal/i, /transaction/i, /receipt/i, /tax/i, /^cost/i, /^fee/i, /charge/i, /trust/i, /disburse/i, /collect/i, /^ar[_-]/i, /^ap[_-]/i],
      columnName: [/^amount$/i, /^total$/i, /^balance$/i, /currency/i, /^debit$/i, /^credit$/i, /invoice_?num/i, /tax_?rate/i, /^paid$/i]
    }
  },
  {
    id: 'time-activity',
    name: 'Time & Activity',
    icon: '⏱',
    description: 'Time entries, timesheets, activities, and work logs',
    patterns: {
      tableName: [/^time/i, /timesheet/i, /timekeep/i, /^activity/i, /activities/i, /worklog/i, /work_?entry/i, /^hours/i, /effort/i],
      columnName: [/^hours$/i, /minutes/i, /duration/i, /start_?time/i, /end_?time/i, /work_?date/i, /timer/i]
    }
  },
  {
    id: 'documents',
    name: 'Documents & Files',
    icon: '📄',
    description: 'Documents, files, attachments, folders, and content',
    patterns: {
      tableName: [/document/i, /^doc[_-]/i, /^docs$/i, /^file(?!_)/i, /folder/i, /attachment/i, /^image/i, /upload/i, /content/i, /template/i, /letter/i, /email_?msg/i],
      columnName: [/file_?name/i, /file_?path/i, /^path$/i, /mime_?type/i, /content_?type/i, /file_?size/i, /^size$/i]
    }
  },
  {
    id: 'audit',
    name: 'Audit & Logs',
    icon: '🛡',
    description: 'Audit trails, change logs, history, and access tracking',
    patterns: {
      tableName: [/^audit/i, /^log(?!in)/i, /^logs$/i, /history/i, /^hist[_-]/i, /change_?log/i, /event_?log/i, /trail/i, /tracking/i, /access_?log/i, /login_?hist/i, /session/i],
      columnName: [/created_?(at|by|date)/i, /modified_?(at|by|date)/i, /changed_?(at|by|date)/i, /event_?type/i, /audit_?id/i]
    }
  },
  {
    id: 'reference',
    name: 'Reference Data',
    icon: '📚',
    description: 'Lookup tables, codes, types, and reference values',
    patterns: {
      tableName: [/^lookup/i, /lookup$/i, /^lk_/i, /^ref_/i, /^code/i, /^type$/i, /^types$/i, /^status$/i, /^statuses$/i, /^categor/i, /classification/i, /^enum/i, /^group(?!s_)/i, /^role(?!_)/i, /currency_?ref/i, /^country/i, /^state(?:s)?$/i, /^region/i, /^industry/i],
      columnName: [/^code$/i, /^lookup_?val/i, /code_?val/i]
    }
  },
  {
    id: 'system',
    name: 'System & Tech',
    icon: '⚙',
    description: 'Configuration, settings, system tables, and migration scaffolding',
    patterns: {
      tableName: [/^config/i, /^settings?$/i, /^sys[_-]/i, /^tmp[_-]/i, /^temp[_-]/i, /_temp$/i, /_tmp$/i, /^stage/i, /staging/i, /_bak$/i, /_backup$/i, /_old$/i, /^test/i, /_test$/i, /migration/i, /^validation/i, /^script/i, /^cygenix/i, /post_?gre/i, /^pgsql/i],
      columnName: []
    }
  }
];

function classifyTable(t) {
  const tName = (t.name || '').toLowerCase();
  const cols  = (t.colNames || []).map(c => c.toLowerCase());

  // Hard rule: _bak / _backup / _old / _tmp / _temp suffixes always go to System,
  // regardless of what the rest of the name suggests. Backup of an "Address"
  // table is still a backup — the user typically doesn't want to migrate it.
  if (/_(bak|backup|old|tmp|temp|test|copy)$/i.test(tName)) {
    return 'system';
  }

  let best = null;
  let bestScore = 0;
  for (const theme of THEMES) {
    let score = 0;
    // Table name signals are weighted heavier than column signals
    for (const rx of (theme.patterns.tableName || [])) {
      if (rx.test(tName)) { score += 10; break; }
    }
    // Each unique column signal adds 1 (capped at 5 to avoid one big table dominating)
    let colHits = 0;
    for (const rx of (theme.patterns.columnName || [])) {
      if (cols.some(c => rx.test(c))) {
        colHits++;
        if (colHits >= 5) break;
      }
    }
    score += colHits;
    if (score > bestScore) { best = theme; bestScore = score; }
  }
  // Require minimum score of 2 to claim a theme — avoids a single weak column
  // signal placing a table in the wrong group.
  return bestScore >= 2 ? best.id : 'other';
}

function classifyIntoGroups(flatTables) {
  // Map of themeId -> table list
  const buckets = new Map();
  for (const theme of THEMES) buckets.set(theme.id, []);
  buckets.set('other', []);

  for (const t of flatTables) {
    const themeId = classifyTable(t);
    buckets.get(themeId).push({
      schema:      t.schema,
      name:        t.name,
      fullName:    t.fullName,
      rows:        t.rows,
      hasPii:      t.hasPii,
      piiCount:    t.piiCount,
      // _cols is included transiently for the response trimming step below
      _cols:       t.cols
    });
  }

  // Build the response, dropping empty groups except keeping "Other" only
  // if it has tables. Sort tables in each group by row count descending.
  // Include column metadata for the top 30 tables per group only — keeps
  // payload bounded for huge databases (8K+ tables) while giving the
  // suggest-criteria call enough representative data.
  const COL_SAMPLE_TABLES = 30;
  const COL_SAMPLE_COLS_PER_TABLE = 25;

  function trimTables(tables) {
    tables.sort((a, b) => (b.rows || 0) - (a.rows || 0));
    return tables.map((t, i) => {
      const out = {
        schema:   t.schema,
        name:     t.name,
        fullName: t.fullName,
        rows:     t.rows,
        hasPii:   t.hasPii,
        piiCount: t.piiCount
      };
      if (i < COL_SAMPLE_TABLES) {
        out.columns = (t._cols || []).slice(0, COL_SAMPLE_COLS_PER_TABLE).map(c => ({
          name: c.name, dataType: c.dataType
        }));
      }
      return out;
    });
  }

  const groups = [];
  for (const theme of THEMES) {
    const tables = buckets.get(theme.id);
    if (tables.length === 0) continue;
    const trimmed = trimTables(tables);
    groups.push({
      id:           theme.id,
      name:         theme.name,
      icon:         theme.icon,
      description:  theme.description,
      tableCount:   trimmed.length,
      totalRows:    trimmed.reduce((s, t) => s + (t.rows || 0), 0),
      piiCount:     trimmed.reduce((s, t) => s + (t.piiCount || 0), 0),
      tables:       trimmed
    });
  }
  // Add "Other" at the end if non-empty
  const other = buckets.get('other');
  if (other.length > 0) {
    const trimmed = trimTables(other);
    groups.push({
      id:           'other',
      name:         'Other',
      icon:         '❓',
      description:  'Tables that didn\'t match any specific theme',
      tableCount:   trimmed.length,
      totalRows:    trimmed.reduce((s, t) => s + (t.rows || 0), 0),
      piiCount:     trimmed.reduce((s, t) => s + (t.piiCount || 0), 0),
      tables:       trimmed
    });
  }
  return groups;
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
    // sys.partitions.rows gives row counts in O(1) per table — same pattern
    // the existing `db` function's "schema" action uses. Avoids COUNT(*)
    // which would lock and take seconds on big tables.
    pool.request().query(`
      SELECT
        s.name  AS schema_name,
        t.name  AS table_name,
        COALESCE(SUM(CASE WHEN p.index_id IN (0,1) THEN p.rows ELSE 0 END), 0) AS row_count
      FROM sys.tables t
      INNER JOIN sys.schemas s    ON s.schema_id = t.schema_id
      LEFT  JOIN sys.partitions p ON p.object_id = t.object_id
      WHERE s.name NOT IN ('sys','INFORMATION_SCHEMA')
      GROUP BY s.name, t.name
      ORDER BY s.name, t.name
    `),
    pool.request().query(`
      SELECT
        c.TABLE_SCHEMA AS schema_name,
        c.TABLE_NAME   AS table_name,
        c.COLUMN_NAME  AS column_name,
        c.DATA_TYPE    AS data_type
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

  // Roll columns up by table — count + PII flag + names + types (used by classifier and suggest-criteria)
  const tableMap = new Map();
  for (const row of colsR.recordset) {
    const key = `${row.schema_name}.${row.table_name}`;
    if (!tableMap.has(key)) tableMap.set(key, { count: 0, hasPii: false, piiCount: 0, colNames: [], cols: [] });
    const e2 = tableMap.get(key);
    e2.count++;
    e2.colNames.push(row.column_name);
    e2.cols.push({ name: row.column_name, dataType: row.data_type });
    if (looksLikePII(row.column_name)) {
      e2.hasPii = true;
      e2.piiCount++;
    }
  }

  // Group by schema
  const bySchema = new Map();
  let totalCols = 0;
  let totalPii  = 0;
  // Also build a flat list of all tables (with full schema-qualified names) for the classifier
  const flatTables = [];
  for (const t of tablesR.recordset) {
    const key = `${t.schema_name}.${t.table_name}`;
    const ce  = tableMap.get(key) || { count: 0, hasPii: false, piiCount: 0, colNames: [], cols: [] };
    totalCols += ce.count;
    totalPii  += ce.piiCount;
    const tableObj = {
      name:        t.table_name,
      rows:        Number(t.row_count) || 0,
      columnCount: ce.count,
      hasPii:      ce.hasPii
    };
    if (!bySchema.has(t.schema_name)) bySchema.set(t.schema_name, []);
    bySchema.get(t.schema_name).push(tableObj);
    flatTables.push({
      schema: t.schema_name,
      name:   t.table_name,
      fullName: key,
      rows:   tableObj.rows,
      hasPii: ce.hasPii,
      piiCount: ce.piiCount,
      colNames: ce.colNames,
      cols:    ce.cols
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

  // ── Classify tables into themed groups for the carousel UI ─────────────
  const groups = classifyIntoGroups(flatTables);

  return {
    databaseLabel,
    schemas,
    edges,
    groups,
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
