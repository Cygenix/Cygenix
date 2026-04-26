// ─────────────────────────────────────────────────────────────────────────────
// agent.js — Agentive Migration backend
// ─────────────────────────────────────────────────────────────────────────────
// Endpoints:
//   POST /api/agent/migrate              — Create a new agent run
//   GET  /api/agent/run/{runId}          — Read run state + messages
//   POST /api/agent/run/{runId}/respond  — User responds to an approval gate
//   POST /api/agent/run/{runId}/cancel   — Cancel a running agent
//
// Modes (controlled by AGENT_STUB_MODE env var):
//   AGENT_STUB_MODE=1  — Synthesize a fake proposal without calling Anthropic.
//                        Useful for UI testing without API cost.
//   AGENT_STUB_MODE=0  — Run the real agent loop (Stage 2b): Anthropic call
//                        with introspection tools, multi-turn until the model
//                        produces a final analysis.
//
// Cosmos containers required:
//   agent_runs      — partition key /userId
//   agent_messages  — partition key /runId
//
// SQL introspection tools provided to the model:
//   list_schemas(side)                          — schema names + counts
//   list_tables(side, schema?, namePattern?)    — tables in a schema
//   describe_tables(side, tables[])             — columns, keys, indexes
//   get_table_relationships(side, table)        — FK graph
//   sample_table(side, table, columns?, n?)     — N sample rows
//   search_tables(side, term)                   — fuzzy search by name
// ─────────────────────────────────────────────────────────────────────────────

const { app } = require('@azure/functions');
const crypto = require('crypto');

// ── Cosmos client (lazy singleton) ──────────────────────────────────────────
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

// ── Anthropic client (lazy singleton) ───────────────────────────────────────
let _anthropic = null;
function getAnthropic() {
  if (!_anthropic) {
    const Anthropic = require('@anthropic-ai/sdk');
    if (!process.env.ANTHROPIC_API_KEY) {
      throw new Error('ANTHROPIC_API_KEY is not configured in Function app settings');
    }
    _anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
  }
  return _anthropic;
}

// ── Shared CORS headers (match index.js) ────────────────────────────────────
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, x-user-id, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
  'Content-Type': 'application/json'
};
const ok  = (body)      => ({ status: 200, headers: CORS, body: JSON.stringify(body) });
const err = (code, msg) => ({ status: code, headers: CORS, body: JSON.stringify({ error: msg }) });

function getUserId(req) {
  return req.headers.get('x-user-id') || req.query.get('userId') || null;
}

function nowIso() { return new Date().toISOString(); }
function shortId(prefix) {
  return `${prefix}_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
}
function isStubMode() { return process.env.AGENT_STUB_MODE === '1'; }

// ── Agent loop limits ───────────────────────────────────────────────────────
const MAX_TURNS = 12;                     // hard ceiling on model round-trips
const MAX_TOOL_RESULT_BYTES = 15_000;     // truncate if a tool returns more
const MAX_TABLES_PER_LIST = 50;           // cap list_tables output
const MAX_DESCRIBE_TABLES = 20;           // describe_tables takes at most N
const SAMPLE_ROWS_DEFAULT = 5;
const SAMPLE_ROWS_MAX = 10;
const SAMPLE_COLS_MAX = 12;
const RUN_BUDGET_USD = 5.0;               // hard cost cap per run
const MODEL_MAX_TOKENS = 1024;            // bound output size per turn
const TOKEN_BUDGET_PER_CALL = 25_000;     // soft input cap (under 30K rate limit)

// ── Feature flag check ──────────────────────────────────────────────────────
async function isAgentiveEnabledForUser(userId, ctx) {
  try {
    const { resource } = await getCosmosContainer('projects')
      .item(userId, userId).read();
    if (resource && resource.aiAgentiveEnabled === false) return false;
  } catch (e) {
    if (e.code !== 404) ctx.log('isAgentiveEnabledForUser read error:', e.message);
  }
  return true;
}

// ── Project connection lookup ───────────────────────────────────────────────
async function getUserConnections(userId, ctx) {
  try {
    const { resource } = await getCosmosContainer('projects')
      .item(userId, userId).read();
    const conns = resource && resource.connections;
    if (!conns) return null;
    const srcConnString = conns.srcConnString || conns.source || '';
    const tgtConnString = conns.tgtConnString || conns.target || '';
    const tgtFnUrl      = conns.tgtFnUrl || '';
    const tgtFnKey      = conns.tgtFnKey || '';
    if (!srcConnString) return null;
    if (!tgtConnString && !tgtFnUrl) return null;
    return { srcConnString, tgtConnString, tgtFnUrl, tgtFnKey };
  } catch (e) {
    if (e.code === 404) return null;
    ctx.log('getUserConnections error:', e.message);
    return null;
  }
}

// ── Run document helpers ────────────────────────────────────────────────────
function newRunDoc({ userId, goal, conns }) {
  return {
    id: shortId('run'),
    userId,
    status: 'running',
    goal,
    direction: 'source_to_target',
    connectionsFingerprint: {
      sourceFingerprint: fingerprint(conns.srcConnString),
      targetFingerprint: fingerprint(conns.tgtConnString || conns.tgtFnUrl),
    },
    createdAt: nowIso(),
    updatedAt: nowIso(),
    pendingApproval: null,
    result: null,
    tokenUsage: { input: 0, output: 0, costUSD: 0 },
    budgetCap:  { maxTokens: 200_000, maxCostUSD: RUN_BUDGET_USD },
    mode: isStubMode() ? 'stub' : 'live',
  };
}

function fingerprint(s) {
  if (!s) return null;
  return crypto.createHash('sha256').update(String(s).slice(0, 80)).digest('hex').slice(0, 12);
}

async function readRun(runId, userId) {
  try {
    const { resource } = await getCosmosContainer('agent_runs')
      .item(runId, userId).read();
    return resource || null;
  } catch (e) {
    if (e.code === 404) return null;
    throw e;
  }
}

async function writeRun(run) {
  run.updatedAt = nowIso();
  await getCosmosContainer('agent_runs').items.upsert(run);
  return run;
}

async function appendMessage(runId, message) {
  const doc = {
    id: shortId('msg'),
    runId,
    seq: message.seq != null ? message.seq : Date.now(),
    role: message.role,
    content: message.content || null,
    toolName: message.toolName || null,
    toolInput: message.toolInput || null,
    toolResult: message.toolResult || null,
    createdAt: nowIso(),
  };
  await getCosmosContainer('agent_messages').items.create(doc);
  return doc;
}

async function loadMessages(runId, sinceSeq) {
  const container = getCosmosContainer('agent_messages');
  const query = sinceSeq && sinceSeq > 0
    ? {
        query: 'SELECT * FROM c WHERE c.runId = @runId AND c.seq > @seq ORDER BY c.seq ASC',
        parameters: [{ name: '@runId', value: runId }, { name: '@seq', value: Number(sinceSeq) }],
      }
    : {
        query: 'SELECT * FROM c WHERE c.runId = @runId ORDER BY c.seq ASC',
        parameters: [{ name: '@runId', value: runId }],
      };
  const { resources } = await container.items.query(query, { partitionKey: runId }).fetchAll();
  return resources;
}

// ── Token cost (claude-sonnet-4-5: $3/MTok in, $15/MTok out) ────────────────
function estimateCostUSD(usage) {
  const inputCost  = (usage.input_tokens  || 0) * (3.00 / 1_000_000);
  const outputCost = (usage.output_tokens || 0) * (15.00 / 1_000_000);
  return inputCost + outputCost;
}

// ─────────────────────────────────────────────────────────────────────────────
// SQL CONNECTION PARSING + POOLING
// ─────────────────────────────────────────────────────────────────────────────
//
// Stored connection strings look like:
//   mssql://user:pass@host:port/db?encrypt=true&trustServerCertificate=true
//
// The `mssql` package wants a config object, so we parse the URL into one.
// Pools are scoped per-side (source/target) per-run and closed at run end.
// ─────────────────────────────────────────────────────────────────────────────

function parseMssqlUrl(connString) {
  // URL parsing — use the WHATWG URL since the format is standard
  let u;
  try {
    u = new URL(connString);
  } catch (e) {
    throw new Error(`Invalid connection string: ${e.message}`);
  }
  if (u.protocol !== 'mssql:') {
    throw new Error(`Unsupported protocol: ${u.protocol} (expected mssql:)`);
  }
  const port = u.port ? Number(u.port) : 1433;
  const params = u.searchParams;
  const encrypt = params.get('encrypt') !== 'false';
  const trust   = params.get('trustServerCertificate') === 'true';

  return {
    server: decodeURIComponent(u.hostname),
    port,
    database: decodeURIComponent((u.pathname || '/').slice(1)),
    user: decodeURIComponent(u.username || ''),
    password: decodeURIComponent(u.password || ''),
    options: {
      encrypt,
      trustServerCertificate: trust,
      enableArithAbort: true,
    },
    requestTimeout: 30000,
    connectionTimeout: 15000,
  };
}

// Per-run pool cache. Key is `${runId}:${side}`. We don't want pools to leak
// across runs, so the cache is cleared via closeRunPools() at end of run.
const _pools = new Map();

async function getPool(side, conns, runId) {
  const sql = require('mssql');
  const cacheKey = `${runId}:${side}`;
  if (_pools.has(cacheKey)) return _pools.get(cacheKey);

  const connString = side === 'source' ? conns.srcConnString : conns.tgtConnString;
  if (!connString) {
    throw new Error(`No ${side} connection string configured`);
  }
  const config = parseMssqlUrl(connString);
  const pool = await new sql.ConnectionPool(config).connect();
  _pools.set(cacheKey, pool);
  return pool;
}

async function closeRunPools(runId) {
  const keys = [...(_pools.keys())].filter(k => k.startsWith(`${runId}:`));
  for (const k of keys) {
    const pool = _pools.get(k);
    try { await pool.close(); } catch (_e) { /* ignore */ }
    _pools.delete(k);
  }
}

// Run a parameterized query and return rows. Wraps mssql's request API.
async function runQuery(pool, sqlText, params) {
  const req = pool.request();
  if (params) {
    for (const [name, value] of Object.entries(params)) {
      req.input(name, value);
    }
  }
  const result = await req.query(sqlText);
  return result.recordset || [];
}

// ─────────────────────────────────────────────────────────────────────────────
// TOOL DEFINITIONS — what the model sees
// ─────────────────────────────────────────────────────────────────────────────
const TOOLS = [
  {
    name: 'list_schemas',
    description: 'List all schemas in either the source or target database, with the number of tables in each. Use this first to understand the high-level structure of a large database. Cheap and fast.',
    input_schema: {
      type: 'object',
      required: ['side'],
      properties: {
        side: { type: 'string', enum: ['source', 'target'], description: 'Which database to inspect.' },
      },
    },
  },
  {
    name: 'list_tables',
    description: 'List tables in a specific schema (or all schemas) of source or target. Returns just table names with row count estimates. CRITICAL: the database may have thousands of tables; this tool is capped at 50 results, so you MUST filter by schema or namePattern unless you know the database is small. Calling this without filters on a large database wastes a turn.',
    input_schema: {
      type: 'object',
      required: ['side'],
      properties: {
        side: { type: 'string', enum: ['source', 'target'] },
        schema: { type: 'string', description: 'Optional schema name. Omit only if you know the database has fewer than 50 tables total.' },
        namePattern: { type: 'string', description: 'Optional SQL LIKE pattern, e.g. "Customer%" or "%_log". Strongly recommended for unknown databases.' },
      },
    },
  },
  {
    name: 'search_tables',
    description: 'Find tables across all schemas whose name contains the given term (case-insensitive substring match). Useful when you do not know what schema a table lives in.',
    input_schema: {
      type: 'object',
      required: ['side', 'term'],
      properties: {
        side: { type: 'string', enum: ['source', 'target'] },
        term: { type: 'string', description: 'Substring to search for. Minimum 2 characters.' },
      },
    },
  },
  {
    name: 'describe_tables',
    description: 'Get detailed schema info for a specific list of tables: columns, types, nullability, primary keys, identity columns. Pass at most 20 tables per call. Use list_tables or search_tables first to identify candidates, then describe just the relevant ones.',
    input_schema: {
      type: 'object',
      required: ['side', 'tables'],
      properties: {
        side: { type: 'string', enum: ['source', 'target'] },
        tables: {
          type: 'array',
          description: 'Array of table identifiers as "schema.table" (e.g. "dbo.Customers"). Maximum 20.',
          items: { type: 'string' },
        },
      },
    },
  },
  {
    name: 'get_table_relationships',
    description: 'Get the foreign key relationships for a specific table — both incoming (other tables that reference this one) and outgoing (tables this one references). Useful for understanding migration order and dependencies.',
    input_schema: {
      type: 'object',
      required: ['side', 'table'],
      properties: {
        side: { type: 'string', enum: ['source', 'target'] },
        table: { type: 'string', description: 'Table identifier as "schema.table".' },
      },
    },
  },
  {
    name: 'sample_table',
    description: 'Read a small number of sample rows from a table. Use sparingly — sampling pulls actual data into context. Only use when type or content shape is genuinely ambiguous and would affect the migration mapping (e.g. is this column actually JSON? what does this status code look like?). Default 5 rows, max 10. You can specify which columns to read; default is all columns up to 12.',
    input_schema: {
      type: 'object',
      required: ['side', 'table'],
      properties: {
        side: { type: 'string', enum: ['source', 'target'] },
        table: { type: 'string', description: 'Table identifier as "schema.table".' },
        columns: {
          type: 'array',
          description: 'Optional list of column names to read. If omitted, reads up to the first 12 columns.',
          items: { type: 'string' },
        },
        n: { type: 'integer', description: 'Number of rows to sample. Default 5, max 10.' },
      },
    },
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// TOOL EXECUTORS — the actual SQL queries
// ─────────────────────────────────────────────────────────────────────────────

async function tool_list_schemas({ side }, conns, runId) {
  const pool = await getPool(side, conns, runId);
  const rows = await runQuery(pool, `
    SELECT TABLE_SCHEMA AS schemaName, COUNT(*) AS tableCount
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    GROUP BY TABLE_SCHEMA
    ORDER BY tableCount DESC
  `);
  return { schemas: rows, side };
}

async function tool_list_tables({ side, schema, namePattern }, conns, runId) {
  const pool = await getPool(side, conns, runId);
  const where = ["t.TABLE_TYPE = 'BASE TABLE'"];
  const params = {};
  if (schema)      { where.push('t.TABLE_SCHEMA = @schema'); params.schema = schema; }
  if (namePattern) { where.push('t.TABLE_NAME LIKE @namePattern'); params.namePattern = namePattern; }

  const rows = await runQuery(pool, `
    SELECT TOP ${MAX_TABLES_PER_LIST + 1}
      t.TABLE_SCHEMA AS schemaName,
      t.TABLE_NAME AS tableName,
      COALESCE(p.rows, 0) AS rowEstimate
    FROM INFORMATION_SCHEMA.TABLES t
    LEFT JOIN sys.objects o ON o.name = t.TABLE_NAME
                             AND SCHEMA_NAME(o.schema_id) = t.TABLE_SCHEMA
                             AND o.type = 'U'
    LEFT JOIN sys.partitions p ON p.object_id = o.object_id AND p.index_id IN (0, 1)
    WHERE ${where.join(' AND ')}
    ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
  `, params);

  const truncated = rows.length > MAX_TABLES_PER_LIST;
  const tables = rows.slice(0, MAX_TABLES_PER_LIST);
  return {
    tables,
    truncated,
    truncationNote: truncated ? `Output capped at ${MAX_TABLES_PER_LIST} tables. Use a more specific schema or namePattern to narrow.` : undefined,
    side,
  };
}

async function tool_search_tables({ side, term }, conns, runId) {
  if (!term || term.length < 2) throw new Error('search term must be at least 2 chars');
  const pool = await getPool(side, conns, runId);
  const rows = await runQuery(pool, `
    SELECT TOP ${MAX_TABLES_PER_LIST + 1}
      TABLE_SCHEMA AS schemaName, TABLE_NAME AS tableName
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
      AND TABLE_NAME LIKE @pattern
    ORDER BY TABLE_SCHEMA, TABLE_NAME
  `, { pattern: `%${term}%` });

  const truncated = rows.length > MAX_TABLES_PER_LIST;
  return {
    matches: rows.slice(0, MAX_TABLES_PER_LIST),
    truncated,
    side,
  };
}

async function tool_describe_tables({ side, tables }, conns, runId) {
  if (!Array.isArray(tables) || tables.length === 0) {
    throw new Error('tables must be a non-empty array');
  }
  if (tables.length > MAX_DESCRIBE_TABLES) {
    throw new Error(`Pass at most ${MAX_DESCRIBE_TABLES} tables per call (got ${tables.length})`);
  }
  const pool = await getPool(side, conns, runId);
  const out = [];
  for (const fqName of tables) {
    const [sch, tbl] = splitFqName(fqName);
    if (!sch || !tbl) {
      out.push({ table: fqName, error: 'Bad name; expected "schema.table"' });
      continue;
    }
    try {
      // Columns
      const cols = await runQuery(pool, `
        SELECT
          c.COLUMN_NAME AS columnName,
          c.DATA_TYPE AS dataType,
          c.CHARACTER_MAXIMUM_LENGTH AS maxLength,
          c.NUMERIC_PRECISION AS numericPrecision,
          c.NUMERIC_SCALE AS numericScale,
          c.IS_NULLABLE AS isNullable,
          c.COLUMN_DEFAULT AS columnDefault,
          c.ORDINAL_POSITION AS position,
          COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, 'IsIdentity') AS isIdentity
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = @sch AND c.TABLE_NAME = @tbl
        ORDER BY c.ORDINAL_POSITION
      `, { sch, tbl });

      // Primary key
      const pk = await runQuery(pool, `
        SELECT kcu.COLUMN_NAME AS columnName
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
          ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
          AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
          AND tc.TABLE_SCHEMA = @sch AND tc.TABLE_NAME = @tbl
        ORDER BY kcu.ORDINAL_POSITION
      `, { sch, tbl });

      out.push({
        table: `${sch}.${tbl}`,
        columns: cols,
        primaryKey: pk.map(r => r.columnName),
      });
    } catch (e) {
      out.push({ table: fqName, error: e.message });
    }
  }
  return { describedTables: out, side };
}

async function tool_get_table_relationships({ side, table }, conns, runId) {
  const [sch, tbl] = splitFqName(table);
  if (!sch || !tbl) throw new Error('table must be "schema.table"');
  const pool = await getPool(side, conns, runId);

  const outgoing = await runQuery(pool, `
    SELECT
      fk.name AS fkName,
      OBJECT_SCHEMA_NAME(fkc.parent_object_id) AS sourceSchema,
      OBJECT_NAME(fkc.parent_object_id) AS sourceTable,
      cs.name AS sourceColumn,
      OBJECT_SCHEMA_NAME(fkc.referenced_object_id) AS referencedSchema,
      OBJECT_NAME(fkc.referenced_object_id) AS referencedTable,
      cr.name AS referencedColumn
    FROM sys.foreign_key_columns fkc
    JOIN sys.foreign_keys fk ON fk.object_id = fkc.constraint_object_id
    JOIN sys.columns cs ON cs.object_id = fkc.parent_object_id AND cs.column_id = fkc.parent_column_id
    JOIN sys.columns cr ON cr.object_id = fkc.referenced_object_id AND cr.column_id = fkc.referenced_column_id
    WHERE OBJECT_SCHEMA_NAME(fkc.parent_object_id) = @sch
      AND OBJECT_NAME(fkc.parent_object_id) = @tbl
  `, { sch, tbl });

  const incoming = await runQuery(pool, `
    SELECT
      fk.name AS fkName,
      OBJECT_SCHEMA_NAME(fkc.parent_object_id) AS sourceSchema,
      OBJECT_NAME(fkc.parent_object_id) AS sourceTable,
      cs.name AS sourceColumn,
      OBJECT_SCHEMA_NAME(fkc.referenced_object_id) AS referencedSchema,
      OBJECT_NAME(fkc.referenced_object_id) AS referencedTable,
      cr.name AS referencedColumn
    FROM sys.foreign_key_columns fkc
    JOIN sys.foreign_keys fk ON fk.object_id = fkc.constraint_object_id
    JOIN sys.columns cs ON cs.object_id = fkc.parent_object_id AND cs.column_id = fkc.parent_column_id
    JOIN sys.columns cr ON cr.object_id = fkc.referenced_object_id AND cr.column_id = fkc.referenced_column_id
    WHERE OBJECT_SCHEMA_NAME(fkc.referenced_object_id) = @sch
      AND OBJECT_NAME(fkc.referenced_object_id) = @tbl
  `, { sch, tbl });

  return { table: `${sch}.${tbl}`, outgoing, incoming, side };
}

async function tool_sample_table({ side, table, columns, n }, conns, runId) {
  const [sch, tbl] = splitFqName(table);
  if (!sch || !tbl) throw new Error('table must be "schema.table"');
  const numRows = Math.max(1, Math.min(SAMPLE_ROWS_MAX, n || SAMPLE_ROWS_DEFAULT));
  const pool = await getPool(side, conns, runId);

  // Resolve columns: either user-provided, or first SAMPLE_COLS_MAX from INFORMATION_SCHEMA
  let colNames;
  if (Array.isArray(columns) && columns.length > 0) {
    colNames = columns.slice(0, SAMPLE_COLS_MAX);
  } else {
    const allCols = await runQuery(pool, `
      SELECT TOP ${SAMPLE_COLS_MAX} COLUMN_NAME AS columnName
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = @sch AND TABLE_NAME = @tbl
      ORDER BY ORDINAL_POSITION
    `, { sch, tbl });
    colNames = allCols.map(c => c.columnName);
  }
  if (colNames.length === 0) throw new Error(`no columns found for ${sch}.${tbl}`);

  // Quote identifiers safely — we built them from prior introspection or user input,
  // and we whitelist quoted identifiers. Reject any column name that contains ']'.
  for (const c of colNames) {
    if (c.includes(']')) throw new Error(`invalid column identifier: ${c}`);
  }
  const colList = colNames.map(c => `[${c}]`).join(', ');

  // Use TABLESAMPLE for large tables; for safety just use TOP since TABLESAMPLE
  // requires permission and isn't supported on views.
  const rows = await runQuery(pool, `
    SELECT TOP ${numRows} ${colList}
    FROM [${sch}].[${tbl}]
  `);

  // Truncate any cell that's huge (binary, big text)
  const truncatedRows = rows.map(r => {
    const out = {};
    for (const [k, v] of Object.entries(r)) {
      if (typeof v === 'string' && v.length > 200) {
        out[k] = v.slice(0, 200) + `…(truncated, original ${v.length} chars)`;
      } else if (Buffer.isBuffer(v)) {
        out[k] = `<binary ${v.length} bytes>`;
      } else {
        out[k] = v;
      }
    }
    return out;
  });

  return {
    table: `${sch}.${tbl}`,
    columns: colNames,
    rows: truncatedRows,
    rowCount: rows.length,
    side,
  };
}

function splitFqName(fq) {
  if (!fq || typeof fq !== 'string') return [null, null];
  const idx = fq.indexOf('.');
  if (idx < 0) return [null, null];
  return [fq.slice(0, idx).trim(), fq.slice(idx + 1).trim()];
}

// Dispatcher used by the agent loop
async function executeTool(name, input, conns, runId, ctx) {
  ctx.log(`[agent] tool: ${name}(${JSON.stringify(input).slice(0, 120)})`);
  const t0 = Date.now();
  try {
    let result;
    switch (name) {
      case 'list_schemas':            result = await tool_list_schemas(input, conns, runId); break;
      case 'list_tables':             result = await tool_list_tables(input, conns, runId); break;
      case 'search_tables':           result = await tool_search_tables(input, conns, runId); break;
      case 'describe_tables':         result = await tool_describe_tables(input, conns, runId); break;
      case 'get_table_relationships': result = await tool_get_table_relationships(input, conns, runId); break;
      case 'sample_table':            result = await tool_sample_table(input, conns, runId); break;
      default: throw new Error(`unknown tool: ${name}`);
    }
    ctx.log(`[agent] tool ${name} ok in ${Date.now() - t0}ms`);
    return enforceSize(result);
  } catch (e) {
    ctx.log(`[agent] tool ${name} failed: ${e.message}`);
    throw e;
  }
}

// Truncate large tool results so they don't blow the context window.
function enforceSize(obj) {
  const json = JSON.stringify(obj);
  if (json.length <= MAX_TOOL_RESULT_BYTES) return obj;
  return {
    truncatedResult: json.slice(0, MAX_TOOL_RESULT_BYTES) + '…',
    note: `Tool result exceeded ${MAX_TOOL_RESULT_BYTES} bytes and was truncated. Refine the query (smaller schema, more specific pattern, fewer tables) to get useful detail.`,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// SYSTEM PROMPT
// ─────────────────────────────────────────────────────────────────────────────
const SYSTEM_PROMPT = `You are the Cygenix Migration Agent, an AI assistant that analyses SQL Server databases to plan migrations.

You have a SOURCE database and a TARGET database. The user wants to migrate data from source to target. Your job in this stage is to explore both schemas using the introspection tools, understand what is relevant to the user's goal, and then write a clear analysis: what tables/columns are involved, what mappings are obvious, what decisions need user input, and what risks you see.

Important context:
- Databases are LARGE (the source may have 2,500 tables, the target 9,000+). Tool results are tightly capped to keep within rate limits, so you MUST filter aggressively.
- ALWAYS filter list_tables by schema or namePattern. Calling list_tables without filters on a large database wastes a turn — it returns 50 arbitrary results that are unlikely to be what you need.
- Start by calling list_schemas to see schema names, then use search_tables with a substring from the user's goal (e.g. if the goal mentions "customer", search for "customer"). Only call describe_tables on the small set of tables you've identified as relevant.
- Use sample_table sparingly — only when type or content is genuinely ambiguous and the answer would affect mapping decisions. Sampling reads real data into your context.
- You have a hard limit of ${MAX_TURNS} turns and a $${RUN_BUDGET_USD} cost cap. Be efficient. Each turn that explores wide rather than deep wastes tokens.
- If your context starts getting full (long history of large tool results), wrap up and produce your analysis with what you have rather than calling more tools.
- For each non-trivial decision, state your reasoning and your confidence (high / medium / low).
- Flag potential PII (email, ssn, dob, credit card, password) explicitly for user review.
- You cannot yet propose a structured machine-readable mapping (that comes in a future stage). For now, write a clear human-readable analysis as your final response.
- If the user's goal is unclear or you need a decision you cannot make confidently, say so explicitly in your final response.

Final output format:
When you are done exploring, write a final response (no tool calls) structured like:

  ## Summary
  (2-3 sentences: what you understood, what's in scope)

  ## Key tables identified
  - schema.table (purpose, row count)
  - ...

  ## Proposed mappings
  - source.foo → target.bar (notes on type changes, transformations needed)
  - ...

  ## Decisions needed
  - (anything you weren't confident about — phrase as questions)

  ## Risks
  - (PII, data loss, performance, etc.)

Be concise. Aim for a response under 800 words.`;

// Rough token estimation: ~4 chars per token for English/JSON. Used to decide
// whether the next Anthropic call would blow the per-minute rate limit.
function estimateTokens(messages, systemPrompt) {
  let chars = (systemPrompt || '').length;
  for (const msg of messages) {
    if (typeof msg.content === 'string') {
      chars += msg.content.length;
    } else if (Array.isArray(msg.content)) {
      for (const block of msg.content) {
        if (block.text) chars += block.text.length;
        if (block.content) chars += String(block.content).length;
        if (block.input) chars += JSON.stringify(block.input).length;
      }
    }
  }
  return Math.ceil(chars / 4);
}

// ─────────────────────────────────────────────────────────────────────────────
// THE AGENT LOOP
// ─────────────────────────────────────────────────────────────────────────────
async function runAgentLoop(run, conns, ctx) {
  const anthropic = getAnthropic();
  const model = process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-5';

  // Conversation builds up across turns. Start with the user's initial message.
  const messages = [{
    role: 'user',
    content: buildInitialPrompt(run.goal, run),
  }];

  let nextSeq = 2;  // seq=1 is the initial user message we wrote at run creation

  for (let turn = 0; turn < MAX_TURNS; turn++) {
    // Budget check before each model call (cost)
    if ((run.tokenUsage.costUSD || 0) >= RUN_BUDGET_USD) {
      ctx.log(`[agent] run ${run.id} hit cost cap at turn ${turn}`);
      messages.push({
        role: 'user',
        content: `You have hit the budget cap of $${RUN_BUDGET_USD}. Produce your final analysis now using the information you have already gathered. Do not call any more tools.`,
      });
    }

    // Token estimate check before each model call (rate limit)
    const estimatedTokens = estimateTokens(messages, SYSTEM_PROMPT);
    if (estimatedTokens > TOKEN_BUDGET_PER_CALL) {
      ctx.log(`[agent] run ${run.id} would exceed token budget (${estimatedTokens} > ${TOKEN_BUDGET_PER_CALL}), forcing final response`);
      // Replace the messages with a compacted version + final-response request
      messages.push({
        role: 'user',
        content: `Your context has grown large (${estimatedTokens} estimated tokens). Stop calling tools and produce your final analysis now using whatever you have gathered. Be concise.`,
      });
      // Force a no-tools call so the model has no choice but to write a response
      let finalResp;
      try {
        finalResp = await anthropic.messages.create({
          model,
          max_tokens: MODEL_MAX_TOKENS,
          system: SYSTEM_PROMPT,
          messages,
        });
      } catch (e) {
        const detail = e.status ? `${e.status} ${e.message}` : e.message;
        throw new Error(`Anthropic API call failed: ${detail}`);
      }
      const usage = finalResp.usage || {};
      run.tokenUsage = {
        input:   (run.tokenUsage.input  || 0) + (usage.input_tokens  || 0),
        output:  (run.tokenUsage.output || 0) + (usage.output_tokens || 0),
        costUSD: (run.tokenUsage.costUSD || 0) + estimateCostUSD(usage),
      };
      await appendMessage(run.id, {
        seq: nextSeq++,
        role: 'assistant',
        content: finalResp.content,
      });
      await writeRun(run);
      return (finalResp.content || []).filter(b => b.type === 'text').map(b => b.text).join('\n').trim()
        || '(no final response)';
    }

    ctx.log(`[agent] turn ${turn + 1}/${MAX_TURNS} (cost so far $${(run.tokenUsage.costUSD || 0).toFixed(4)}, est ${estimatedTokens} input tokens)`);

    let response;
    const t0 = Date.now();
    try {
      response = await anthropic.messages.create({
        model,
        max_tokens: MODEL_MAX_TOKENS,
        system: SYSTEM_PROMPT,
        tools: TOOLS,
        messages,
      });
    } catch (e) {
      const detail = e.status ? `${e.status} ${e.message}` : e.message;
      throw new Error(`Anthropic API call failed: ${detail}`);
    }
    const elapsed = Date.now() - t0;

    // Update token usage
    const usage = response.usage || {};
    const cost  = estimateCostUSD(usage);
    run.tokenUsage = {
      input:   (run.tokenUsage.input  || 0) + (usage.input_tokens  || 0),
      output:  (run.tokenUsage.output || 0) + (usage.output_tokens || 0),
      costUSD: (run.tokenUsage.costUSD || 0) + cost,
    };

    ctx.log(`[agent] turn ${turn + 1} response in ${elapsed}ms, ${usage.input_tokens || 0}+${usage.output_tokens || 0} tokens, $${cost.toFixed(4)} (stop=${response.stop_reason})`);

    // Persist the assistant turn so the frontend activity log updates
    await appendMessage(run.id, {
      seq: nextSeq++,
      role: 'assistant',
      content: response.content,
    });
    await writeRun(run);

    // If model is done (text response only, no tool_use), break.
    if (response.stop_reason === 'end_turn') {
      // Add the assistant's full content to messages array (for completeness)
      messages.push({ role: 'assistant', content: response.content });
      // Extract final text
      const finalText = (response.content || [])
        .filter(b => b.type === 'text')
        .map(b => b.text)
        .join('\n')
        .trim();
      return finalText || '(no response text)';
    }

    if (response.stop_reason !== 'tool_use') {
      // max_tokens or other unexpected stop — bail with what we have
      ctx.log(`[agent] unexpected stop_reason=${response.stop_reason}, ending loop`);
      const fallback = (response.content || [])
        .filter(b => b.type === 'text')
        .map(b => b.text)
        .join('\n')
        .trim();
      return fallback || `(model stopped with reason: ${response.stop_reason})`;
    }

    // Append assistant turn to messages so model sees its own tool calls next round
    messages.push({ role: 'assistant', content: response.content });

    // Run all tool calls in this turn and accumulate results
    const toolUseBlocks = (response.content || []).filter(b => b.type === 'tool_use');
    const toolResults = [];

    for (const tu of toolUseBlocks) {
      try {
        const result = await executeTool(tu.name, tu.input, conns, run.id, ctx);
        toolResults.push({
          type: 'tool_result',
          tool_use_id: tu.id,
          content: JSON.stringify(result),
        });
      } catch (e) {
        toolResults.push({
          type: 'tool_result',
          tool_use_id: tu.id,
          content: `Error: ${e.message}`,
          is_error: true,
        });
      }
    }

    // Persist the tool results as a "user" turn so the frontend log shows them
    await appendMessage(run.id, {
      seq: nextSeq++,
      role: 'user',
      content: toolResults,
    });
    await writeRun(run);

    messages.push({ role: 'user', content: toolResults });
  }

  // Hit max turns — ask for a final response with what we have
  ctx.log(`[agent] run ${run.id} hit MAX_TURNS=${MAX_TURNS}, requesting final response`);
  messages.push({
    role: 'user',
    content: `You have hit the maximum number of turns (${MAX_TURNS}). Produce your final analysis now using the information you have already gathered. Do not call any more tools.`,
  });

  const finalResp = await getAnthropic().messages.create({
    model,
    max_tokens: MODEL_MAX_TOKENS,
    system: SYSTEM_PROMPT,
    messages,  // no tools this time — force a text response
  });

  const usage = finalResp.usage || {};
  run.tokenUsage = {
    input:   (run.tokenUsage.input  || 0) + (usage.input_tokens  || 0),
    output:  (run.tokenUsage.output || 0) + (usage.output_tokens || 0),
    costUSD: (run.tokenUsage.costUSD || 0) + estimateCostUSD(usage),
  };
  await appendMessage(run.id, {
    seq: nextSeq++,
    role: 'assistant',
    content: finalResp.content,
  });
  await writeRun(run);

  return (finalResp.content || []).filter(b => b.type === 'text').map(b => b.text).join('\n').trim()
    || '(no final response)';
}

// ─────────────────────────────────────────────────────────────────────────────
// LIVE PROPOSAL — wraps the agent loop, sets pendingApproval at end
// ─────────────────────────────────────────────────────────────────────────────
async function liveProduceProposal(run, conns, ctx) {
  let finalText;
  try {
    finalText = await runAgentLoop(run, conns, ctx);
  } finally {
    // Always close pools, even if the loop threw
    await closeRunPools(run.id);
  }

  run.status = 'awaiting_approval';
  run.pendingApproval = {
    type: 'propose_mapping',
    requestedAt: nowIso(),
    payload: {
      summary: finalText,
      decisions: [
        {
          decision: 'Stage 2b — schema introspection complete',
          reasoning: 'The agent explored both databases and produced an analysis. Structured machine-readable mappings come in Stage 2c.',
          confidence: 'high',
        },
      ],
      mapping: { tables: [] },  // empty until 2c
    },
  };
  await writeRun(run);
}

// ─────────────────────────────────────────────────────────────────────────────
// STUB MODE (preserved as fallback for UI testing)
// ─────────────────────────────────────────────────────────────────────────────
async function stubProduceFakeProposal(run, ctx) {
  await appendMessage(run.id, {
    seq: 2,
    role: 'assistant',
    content: [
      { type: 'text', text: 'Reading source schema (stub mode).' },
      { type: 'tool_use', id: 'stub_t1', name: 'list_schemas', input: { side: 'source' } },
    ],
  });
  await appendMessage(run.id, {
    seq: 3,
    role: 'user',
    content: [{ type: 'tool_result', tool_use_id: 'stub_t1', content: '(stub) Found 4 tables: customers, orders, order_items, products' }],
  });

  run.status = 'awaiting_approval';
  run.pendingApproval = {
    type: 'propose_mapping',
    requestedAt: nowIso(),
    payload: {
      summary: 'Stub proposal — plumbing test. This is fake data; set AGENT_STUB_MODE=0 to run the real agent.',
      decisions: [
        { decision: 'Map customers.email as VARCHAR(255)', reasoning: 'Source uses VARCHAR(MAX); target is VARCHAR(255).', confidence: 'high' },
        { decision: 'Exclude customers.password_hash', reasoning: 'Target has its own auth.', confidence: 'high' },
      ],
      mapping: {
        tables: [
          { sourceTable: 'customers', targetTable: 'customers', columns: stubCols(8, 1) },
          { sourceTable: 'orders',    targetTable: 'orders',    columns: stubCols(6, 0) },
        ],
      },
    },
  };
  await writeRun(run);
  ctx.log(`[agent] run ${run.id} produced stub proposal`);
}

function stubCols(n, excluded) {
  const cols = [];
  for (let i = 0; i < n; i++) {
    cols.push({ sourceColumn: `col_${i}`, targetColumn: `col_${i}`, sourceType: 'VARCHAR', targetType: 'VARCHAR', excluded: i < excluded });
  }
  return cols;
}

function buildInitialPrompt(goal, run) {
  return `Migrate from source to target.

User goal: ${goal}

Run id: ${run.id}
Source fingerprint: ${run.connectionsFingerprint.sourceFingerprint}
Target fingerprint: ${run.connectionsFingerprint.targetFingerprint}

Use the introspection tools to explore both databases and produce your analysis. Start by listing schemas to understand the structure.`;
}

// ─────────────────────────────────────────────────────────────────────────────
// ROUTE: POST /api/agent/migrate
// ─────────────────────────────────────────────────────────────────────────────
app.http('agent_migrate', {
  methods: ['POST', 'OPTIONS'],
  authLevel: 'function',
  route: 'agent/migrate',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 200, headers: CORS, body: '' };

    const userId = getUserId(req);
    if (!userId) return err(401, 'x-user-id header is required');

    if (!(await isAgentiveEnabledForUser(userId, ctx))) {
      return err(403, 'Agentive Migration is not enabled for this user');
    }

    const body = await req.json().catch(() => null);
    if (!body || typeof body !== 'object') return err(400, 'Invalid JSON body');
    const goal = (body.goal || '').trim();
    if (goal.length < 10) return err(400, 'goal must be at least 10 characters');

    const conns = await getUserConnections(userId, ctx);
    if (!conns) {
      return err(400, 'Source and target connections must be configured before starting an agent run.');
    }

    const run = newRunDoc({ userId, goal, conns });
    await getCosmosContainer('agent_runs').items.create(run);
    await appendMessage(run.id, {
      seq: 1,
      role: 'user',
      content: [{ type: 'text', text: buildInitialPrompt(goal, run) }],
    });

    ctx.log(`[agent] run ${run.id} created (mode=${run.mode}) for user ${userId}`);

    // Note: synchronous execution. The frontend's POST hangs until we finish
    // (could be 30-90 seconds for a real run). The 5-min Function timeout
    // gives us plenty of headroom; if we ever need longer, this becomes a
    // queue trigger.
    try {
      if (isStubMode()) {
        await stubProduceFakeProposal(run, ctx);
      } else {
        await liveProduceProposal(run, conns, ctx);
      }
    } catch (e) {
      ctx.log(`[agent] run ${run.id} failed: ${e.message}`);
      await closeRunPools(run.id);
      run.status = 'failed';
      run.result = { error: e.message };
      await writeRun(run);
    }

    return ok({ runId: run.id });
  },
});

// ─────────────────────────────────────────────────────────────────────────────
// ROUTE: GET /api/agent/run/{runId}
// ─────────────────────────────────────────────────────────────────────────────
app.http('agent_run_read', {
  methods: ['GET', 'OPTIONS'],
  authLevel: 'function',
  route: 'agent/run/{runId}',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 200, headers: CORS, body: '' };

    const userId = getUserId(req);
    if (!userId) return err(401, 'x-user-id header is required');

    const runId = req.params.runId;
    if (!runId) return err(400, 'runId required');

    const run = await readRun(runId, userId);
    if (!run) return err(404, 'Run not found');

    const sinceSeq = parseInt(req.query.get('sinceSeq') || '0', 10) || 0;
    const messages = await loadMessages(runId, sinceSeq);
    const allMessages = sinceSeq > 0 ? await loadMessages(runId, 0) : messages;

    return ok({ run, messages, allMessages });
  },
});

// ─────────────────────────────────────────────────────────────────────────────
// ROUTE: POST /api/agent/run/{runId}/respond
// ─────────────────────────────────────────────────────────────────────────────
app.http('agent_run_respond', {
  methods: ['POST', 'OPTIONS'],
  authLevel: 'function',
  route: 'agent/run/{runId}/respond',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 200, headers: CORS, body: '' };

    const userId = getUserId(req);
    if (!userId) return err(401, 'x-user-id header is required');

    const runId = req.params.runId;
    const run = await readRun(runId, userId);
    if (!run) return err(404, 'Run not found');
    if (run.status !== 'awaiting_approval') {
      return err(409, `Run is not awaiting approval (status: ${run.status})`);
    }

    const body = await req.json().catch(() => null);
    if (!body || typeof body !== 'object') return err(400, 'Invalid JSON body');
    const action = body.action;

    switch (action) {
      case 'approve': {
        const mapping = run.pendingApproval && run.pendingApproval.payload && run.pendingApproval.payload.mapping;
        const mappingId = await saveProposedMapping(userId, mapping, run, ctx);
        run.status = 'completed';
        run.pendingApproval = null;
        run.result = {
          mappingId,
          summary: `Saved a mapping with ${mapping && mapping.tables ? mapping.tables.length : 0} tables.`,
        };
        await writeRun(run);
        return ok({ ok: true, status: 'completed', mappingId });
      }
      case 'edit': {
        const mapping = body.mapping || (run.pendingApproval && run.pendingApproval.payload && run.pendingApproval.payload.mapping);
        const mappingId = await saveProposedMapping(userId, mapping, run, ctx);
        run.status = 'completed';
        run.pendingApproval = null;
        run.result = { mappingId, summary: 'Mapping saved with your edits.' };
        await writeRun(run);
        return ok({ ok: true, status: 'completed', mappingId });
      }
      case 'reject': {
        run.status = 'failed';
        run.pendingApproval = null;
        run.result = { error: `Rejected: ${body.feedback || 'no feedback given'}.` };
        await writeRun(run);
        return ok({ ok: true, status: 'failed' });
      }
      case 'answer': {
        run.status = 'cancelled';
        run.pendingApproval = null;
        await writeRun(run);
        return ok({ ok: true, status: 'cancelled' });
      }
      default:
        return err(400, `Unknown action: ${action}`);
    }
  },
});

async function saveProposedMapping(userId, mapping, run, ctx) {
  const mappingId = shortId('map');
  if (!mapping) return mappingId;

  const container = getCosmosContainer('projects');
  let existing = {};
  try {
    const { resource } = await container.item(userId, userId).read();
    existing = resource || {};
  } catch (e) {
    if (e.code !== 404) throw e;
  }

  const list = Array.isArray(existing.agent_mappings) ? existing.agent_mappings : [];
  list.push({
    id: mappingId,
    runId: run.id,
    createdAt: nowIso(),
    summary: (run.pendingApproval && run.pendingApproval.payload && run.pendingApproval.payload.summary) || '',
    mapping,
  });
  existing.agent_mappings = list;
  existing.id = userId;
  existing.userId = userId;
  existing.updatedAt = nowIso();
  await container.items.upsert(existing);

  ctx.log(`[agent] saved mapping ${mappingId} for user ${userId} (run ${run.id})`);
  return mappingId;
}

// ─────────────────────────────────────────────────────────────────────────────
// ROUTE: POST /api/agent/run/{runId}/cancel
// ─────────────────────────────────────────────────────────────────────────────
app.http('agent_run_cancel', {
  methods: ['POST', 'OPTIONS'],
  authLevel: 'function',
  route: 'agent/run/{runId}/cancel',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 200, headers: CORS, body: '' };

    const userId = getUserId(req);
    if (!userId) return err(401, 'x-user-id header is required');

    const runId = req.params.runId;
    const run = await readRun(runId, userId);
    if (!run) return err(404, 'Run not found');

    if (run.status === 'completed' || run.status === 'failed' || run.status === 'cancelled') {
      return ok({ ok: true, status: run.status });
    }

    run.status = 'cancelled';
    run.pendingApproval = null;
    await writeRun(run);
    await closeRunPools(runId);
    ctx.log(`[agent] run ${runId} cancelled by user ${userId}`);
    return ok({ ok: true, status: 'cancelled' });
  },
});
