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
const MODEL_MAX_TOKENS = 4096;            // bound output size per turn (large enough for a full propose_table_mapping on wide tables)
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
    const srcConnMode   = conns.srcConnMode   === 'azure' ? 'azure' : 'direct';
    const srcFnUrl      = conns.srcFnUrl || '';
    const srcFnKey      = conns.srcFnKey || '';
    const tgtConnString = conns.tgtConnString || conns.target || '';
    const tgtConnMode   = conns.tgtConnMode   === 'azure' ? 'azure' : 'direct';
    const tgtFnUrl      = conns.tgtFnUrl || '';
    const tgtFnKey      = conns.tgtFnKey || '';
    // Source must resolve to *something* — direct conn string or azure Fn URL.
    if (!srcConnString && !srcFnUrl) return null;
    // Target likewise.
    if (!tgtConnString && !tgtFnUrl) return null;
    return {
      srcConnString, srcConnMode, srcFnUrl, srcFnKey,
      tgtConnString, tgtConnMode, tgtFnUrl, tgtFnKey,
    };
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
      sourceFingerprint: fingerprint(conns.srcConnString || conns.srcFnUrl),
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
    if (!resource) return null;

    // Staleness reaper: a run stuck in 'running' for >6 minutes (longer than
    // the Function timeout) means the worker died mid-loop. Flip to failed
    // so the frontend doesn't poll forever.
    if (resource.status === 'running') {
      const ageMs = Date.now() - new Date(resource.updatedAt || resource.createdAt).getTime();
      if (ageMs > 6 * 60 * 1000) {
        resource.status = 'failed';
        resource.result = { error: 'Run timed out (worker did not complete in time). The agent may have been killed by a restart or hit a deadlock.' };
        resource.pendingApproval = null;
        try { await writeRun(resource); } catch (_e) { /* best-effort */ }
      }
    }
    return resource;
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

// ─────────────────────────────────────────────────────────────────────────────
// EXECUTOR LAYER — supports BOTH direct mssql:// and azure-mode targets
// ─────────────────────────────────────────────────────────────────────────────
// Previously this layer was direct-mssql only: getPool() opened an mssql
// ConnectionPool and runQuery() ran SQL through it. That meant azure-mode
// targets (where the user's tgtFnUrl points at a Cygenix-managed Function
// instead of an mssql:// string) were not supported — the agent would
// throw "No target connection string configured" on the first target tool
// call, even though the request body had a valid tgtFnUrl.
//
// Now everything routes through agent-target-executor.js, which returns an
// executor object whose .query()/.close() methods work the same regardless
// of the underlying transport. The function names below (getPool /
// runQuery / closeRunPools) are kept as-is for back-compat with all the
// existing tool-handler call sites — they're now thin wrappers.
const { getExecutor, closeRunExecutors } = require('./agent-target-executor');

async function getPool(side, conns, runId) {
  // Returns an executor (not a raw mssql pool). The executor exposes the
  // same query interface for both modes via runQuery() below.
  return getExecutor(side, conns, runId);
}

async function closeRunPools(runId) {
  await closeRunExecutors(runId);
}

// Run a parameterized query and return rows. The first argument is now an
// executor (returned from getPool above), not a raw mssql pool — but the
// call signature is unchanged so existing tool handlers don't need
// modification.
async function runQuery(executor, sqlText, params) {
  return executor.query(sqlText, params);
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
  {
    name: 'propose_table_mapping',
    description: 'Propose a single source-table → target-table mapping with full column-level details. Call this once per table pair you want to migrate. You can call it multiple times in a single turn or across turns. Each call must include the complete column mapping for that pair.\n\nIMPORTANT: only use exact table and column names that introspection actually returned. Do not invent names. If you have not yet introspected a table, do not propose it.\n\nValidation rules (the call will fail if you violate these):\n- transform must be one of: NONE, TRIM, UPPER, LOWER, CAST\n- match must be one of: HIGH, MEDIUM, LOW\n- match guidance: HIGH = identical column name and clearly compatible type; MEDIUM = similar name or clear semantic match with type coercion needed; LOW = inferred match with significant uncertainty\n- sourceTable and targetTable must be in "schema.table" format',
    input_schema: {
      type: 'object',
      required: ['name', 'sourceTable', 'targetTable', 'columnMapping', 'reasoning'],
      properties: {
        name: {
          type: 'string',
          description: 'Short identifier for this mapping job, e.g. "customers_to_customers". Lowercase, underscores, no spaces.',
        },
        sourceTable: {
          type: 'string',
          description: 'Source table as "schema.table" (e.g. "dbo.Customers").',
        },
        targetTable: {
          type: 'string',
          description: 'Target table as "schema.table" (e.g. "dbo.Customer").',
        },
        columnMapping: {
          type: 'array',
          description: 'Array of column mappings. Include every target column that should be populated. For target columns that have no source equivalent, you may either omit them (target default/NULL applies) or include them with srcCol="" so the user sees the unmapped column.',
          items: {
            type: 'object',
            required: ['srcCol', 'tgtCol', 'transform', 'match'],
            properties: {
              srcCol: { type: 'string', description: 'Source column name. Empty string if no source mapping (uses target default/NULL).' },
              tgtCol: { type: 'string', description: 'Target column name.' },
              transform: { type: 'string', enum: ['NONE', 'TRIM', 'UPPER', 'LOWER', 'CAST'], description: 'Transformation. NONE = direct copy. CAST = type conversion (auto-handled by SQL generation).' },
              match: { type: 'string', enum: ['HIGH', 'MEDIUM', 'LOW'], description: 'Confidence in this mapping.' },
              note: { type: 'string', description: 'Optional explanation, especially for non-NONE transforms or LOW confidence matches.' },
            },
          },
        },
        reasoning: {
          type: 'string',
          description: 'Brief reasoning for this whole table-pair mapping (1-2 sentences). What is being migrated and why these columns map this way.',
        },
        confidence: {
          type: 'string',
          enum: ['HIGH', 'MEDIUM', 'LOW'],
          description: 'Overall confidence in this table-pair mapping. Optional; defaults to MEDIUM.',
        },
        warnings: {
          type: 'array',
          description: 'Optional warnings the user should review (PII columns, type narrowing risk, missing target columns, etc.).',
          items: { type: 'string' },
        },
      },
    },
  },
  {
    name: 'finalize_proposal',
    description: 'Call this exactly once at the end of your run to wrap up. After this is called, the run pauses for user approval. You should already have called propose_table_mapping for every table pair you want to migrate before calling this. If you have no proposals to make (e.g. user goal cannot be satisfied), still call this and explain why in summary.',
    input_schema: {
      type: 'object',
      required: ['summary'],
      properties: {
        summary: {
          type: 'string',
          description: '2-4 sentences explaining what was proposed and why. The user reads this first.',
        },
        decisionsForUser: {
          type: 'array',
          description: 'Optional list of decisions the user should weigh in on before approving (e.g. "Should historical orders before 2020 be migrated?").',
          items: {
            type: 'object',
            required: ['question', 'context'],
            properties: {
              question: { type: 'string' },
              context: { type: 'string', description: 'Why this question matters — what the user needs to know to decide.' },
            },
          },
        },
        risks: {
          type: 'array',
          description: 'Optional list of risks the user should be aware of (PII handling, data loss potential, migration order dependencies).',
          items: { type: 'string' },
        },
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

// Dispatcher used by the agent loop. The first six tools are side-effect-free
// SQL queries. The last two (propose_table_mapping, finalize_proposal) are
// state-affecting — they record a proposal in proposalState. The loop checks
// proposalState.finalized after each turn to know whether to exit.
async function executeTool(name, input, conns, runId, ctx, proposalState) {
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
      case 'propose_table_mapping':   result = tool_propose_table_mapping(input, proposalState); break;
      case 'finalize_proposal':       result = tool_finalize_proposal(input, proposalState); break;
      default: throw new Error(`unknown tool: ${name}`);
    }
    ctx.log(`[agent] tool ${name} ok in ${Date.now() - t0}ms`);
    return enforceSize(result);
  } catch (e) {
    ctx.log(`[agent] tool ${name} failed: ${e.message}`);
    throw e;
  }
}

// ── Validators for the two terminal tools ──────────────────────────────────
const ALLOWED_TRANSFORMS = ['NONE', 'TRIM', 'UPPER', 'LOWER', 'CAST'];
const ALLOWED_MATCHES = ['HIGH', 'MEDIUM', 'LOW'];

function tool_propose_table_mapping(input, proposalState) {
  if (proposalState.finalized) {
    throw new Error('Cannot propose more mappings after finalize_proposal has been called.');
  }
  const errs = [];
  const { name, sourceTable, targetTable, columnMapping, reasoning, confidence, warnings } = input || {};
  if (!name || typeof name !== 'string') errs.push('name is required');
  if (!sourceTable || !sourceTable.includes('.')) errs.push('sourceTable must be "schema.table"');
  if (!targetTable || !targetTable.includes('.')) errs.push('targetTable must be "schema.table"');
  if (!Array.isArray(columnMapping) || columnMapping.length === 0) errs.push('columnMapping must be a non-empty array');
  if (!reasoning || typeof reasoning !== 'string') errs.push('reasoning is required');

  if (Array.isArray(columnMapping)) {
    columnMapping.forEach((m, i) => {
      if (typeof m.srcCol !== 'string') errs.push(`columnMapping[${i}].srcCol must be a string`);
      if (!m.tgtCol || typeof m.tgtCol !== 'string') errs.push(`columnMapping[${i}].tgtCol must be a non-empty string`);
      if (!ALLOWED_TRANSFORMS.includes(m.transform)) {
        errs.push(`columnMapping[${i}].transform "${m.transform}" not allowed; must be one of ${ALLOWED_TRANSFORMS.join(', ')}`);
      }
      if (!ALLOWED_MATCHES.includes(m.match)) {
        errs.push(`columnMapping[${i}].match "${m.match}" not allowed; must be one of ${ALLOWED_MATCHES.join(', ')}`);
      }
    });
  }

  if (errs.length > 0) {
    throw new Error(`propose_table_mapping validation failed:\n - ${errs.join('\n - ')}`);
  }

  // Reject duplicate proposals (same source/target pair) — agent would have to revise via a fresh call
  const dup = proposalState.proposals.find(p => p.sourceTable === sourceTable && p.targetTable === targetTable);
  if (dup) {
    throw new Error(`A proposal for ${sourceTable} → ${targetTable} already exists. To revise, call finalize_proposal then start a new run.`);
  }

  const proposal = {
    name: name.replace(/[^a-zA-Z0-9_]/g, '_').slice(0, 80),
    sourceTable,
    targetTable,
    columnMapping: columnMapping.map(m => ({
      srcCol: m.srcCol || '',
      tgtCol: m.tgtCol,
      transform: m.transform,
      match: m.match,
      note: m.note || '',
    })),
    reasoning,
    confidence: confidence || 'MEDIUM',
    warnings: Array.isArray(warnings) ? warnings : [],
  };
  proposalState.proposals.push(proposal);
  return {
    accepted: true,
    proposalCount: proposalState.proposals.length,
    note: `Proposal recorded. ${proposalState.proposals.length} so far. Call finalize_proposal when done with all table pairs.`,
  };
}

function tool_finalize_proposal(input, proposalState) {
  if (proposalState.finalized) {
    throw new Error('finalize_proposal already called for this run.');
  }
  const { summary, decisionsForUser, risks } = input || {};
  if (!summary || typeof summary !== 'string') {
    throw new Error('summary is required');
  }
  proposalState.finalized = true;
  proposalState.summary = summary;
  proposalState.decisionsForUser = Array.isArray(decisionsForUser) ? decisionsForUser : [];
  proposalState.risks = Array.isArray(risks) ? risks : [];
  return {
    accepted: true,
    finalized: true,
    note: `Run finalized with ${proposalState.proposals.length} proposed mapping(s). Returning to user for approval.`,
  };
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
const SYSTEM_PROMPT = `You are the Cygenix Migration Agent, an AI assistant that analyses SQL Server databases and proposes structured migration mappings.

You have a SOURCE database and a TARGET database. The user wants to migrate data from source to target. You explore both schemas using introspection tools, then propose specific source-table → target-table mappings using propose_table_mapping. When done, you call finalize_proposal once to wrap up.

## Important context

- Databases are LARGE (the source may have 2,500 tables, the target 9,000+). Tool results are tightly capped to keep within rate limits, so you MUST filter aggressively.
- ALWAYS filter list_tables by schema or namePattern. Calling it without filters wastes a turn.
- Start by calling list_schemas to see schema names, then use search_tables with a substring from the user's goal. Only call describe_tables on the small set of tables you've identified as relevant.
- Use sample_table sparingly — only when type or content is genuinely ambiguous.
- You have a hard limit of ${MAX_TURNS} turns and a $${RUN_BUDGET_USD} cost cap.
- If your context is getting full, wrap up and finalize rather than calling more introspection tools.

## How to produce a proposal

Once you've explored enough to understand a source-table → target-table mapping:

1. Call describe_tables on BOTH the source table and the target table (you cannot propose without seeing both schemas).
2. Call propose_table_mapping with:
   - A short snake_case name like "customers_to_customer"
   - Full sourceTable and targetTable as "schema.table"
   - A complete columnMapping array — one entry per target column you intend to populate
   - Each entry has srcCol (or empty string for unmapped), tgtCol, transform (NONE/TRIM/UPPER/LOWER/CAST), match (HIGH/MEDIUM/LOW), and an optional note
   - Brief reasoning for the whole pair
   - Optional warnings (PII, type narrowing risk, etc.)
3. You can call propose_table_mapping multiple times — once per table pair.
4. When you are done with ALL table pairs, call finalize_proposal once with:
   - summary (2-4 sentences explaining the overall proposal)
   - decisionsForUser (optional list of {question, context} for things the user should weigh in on)
   - risks (optional list of strings)

After finalize_proposal is called, the run pauses and the user reviews. Do not call any more tools after finalize_proposal.

## Match value guidance

- HIGH: column names match exactly AND types are clearly compatible (e.g. id INT → id INT, email NVARCHAR(255) → email VARCHAR(255))
- MEDIUM: similar names or clear semantic match needing minor coercion (e.g. createdAt DATETIME → created_at DATETIME2, name NVARCHAR → name VARCHAR with no length narrowing risk)
- LOW: inferred match with significant uncertainty (e.g. status VARCHAR(50) → state INT — needs a transform you can't fully verify)

## Transform value guidance

- NONE: direct copy (most cases)
- TRIM: source has padding/whitespace that should be removed
- UPPER / LOWER: case normalization
- CAST: type conversion needed (the SQL generator will produce the appropriate CAST when Object Mapping renders this); use this when source and target types differ in a way that requires explicit casting

## What to flag as risks

- PII columns (email, ssn, dob, credit card, password, phone, address). Mention these explicitly.
- Type narrowing (e.g. NVARCHAR(MAX) → VARCHAR(255) — could truncate)
- Missing target columns (target has columns with no source — they need defaults)
- Migration order dependencies (foreign keys constraining the order you must run jobs)

## If the goal cannot be fulfilled

If the user's goal is unclear, infeasible, or the source/target don't have what's needed: still call finalize_proposal with zero proposed mappings. Use summary to explain what's wrong and decisionsForUser to ask for clarification. Do not invent mappings just to produce something.

Be concise. Each turn should advance the proposal — don't explore for the sake of exploring.`;

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
// Returns { finalText, proposalState }. proposalState contains the structured
// proposals if the model called finalize_proposal; otherwise it's empty (and
// finalText is the model's free-form fallback message).
async function runAgentLoop(run, conns, ctx) {
  const anthropic = getAnthropic();
  const model = process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-5';

  // State accumulated by propose_table_mapping / finalize_proposal calls
  const proposalState = {
    proposals: [],         // array of { name, sourceTable, targetTable, columnMapping, reasoning, confidence, warnings }
    finalized: false,
    summary: '',
    decisionsForUser: [],
    risks: [],
  };

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
        content: `You have hit the budget cap of $${RUN_BUDGET_USD}. Call finalize_proposal now with whatever proposals you have so far, even if you would have liked to do more. Do not call any other tools.`,
      });
    }

    // Token estimate check before each model call (rate limit)
    const estimatedTokens = estimateTokens(messages, SYSTEM_PROMPT);
    if (estimatedTokens > TOKEN_BUDGET_PER_CALL) {
      ctx.log(`[agent] run ${run.id} would exceed token budget (${estimatedTokens} > ${TOKEN_BUDGET_PER_CALL}), forcing wrap-up`);
      const wrapUpText = proposalState.proposals.length > 0
        ? `Your context has grown large (${estimatedTokens} estimated tokens). Stop introspecting. Call finalize_proposal now with the ${proposalState.proposals.length} proposal(s) you have already made. Do not call propose_table_mapping again unless you must, and do not call any other tools.`
        : `Your context has grown large (${estimatedTokens} estimated tokens). Stop introspecting. Based on what you have already seen, call propose_table_mapping for the table pair(s) you can map, then call finalize_proposal. Do not call any other tools.`;
      messages.push({ role: 'user', content: wrapUpText });
      // Allow propose_table_mapping AND finalize_proposal — the model needs
      // both to complete the run. Disallow introspection tools (those got us
      // into the budget overrun in the first place).
      const wrapUpTools = TOOLS.filter(t => t.name === 'propose_table_mapping' || t.name === 'finalize_proposal');
      return await wrapUpAndReturn(anthropic, model, messages, wrapUpTools, run, proposalState, nextSeq, ctx);
    }

    ctx.log(`[agent] turn ${turn + 1}/${MAX_TURNS} (cost so far $${(run.tokenUsage.costUSD || 0).toFixed(4)}, est ${estimatedTokens} input tokens, ${proposalState.proposals.length} proposals so far)`);

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
      messages.push({ role: 'assistant', content: response.content });
      const finalText = (response.content || [])
        .filter(b => b.type === 'text')
        .map(b => b.text)
        .join('\n')
        .trim();
      return { finalText: proposalState.summary || finalText || '(no response text)', proposalState };
    }

    if (response.stop_reason !== 'tool_use') {
      ctx.log(`[agent] unexpected stop_reason=${response.stop_reason}, ending loop`);
      const fallback = (response.content || [])
        .filter(b => b.type === 'text')
        .map(b => b.text)
        .join('\n')
        .trim();
      return { finalText: proposalState.summary || fallback || `(model stopped with reason: ${response.stop_reason})`, proposalState };
    }

    // Append assistant turn to messages so model sees its own tool calls next round
    messages.push({ role: 'assistant', content: response.content });

    // Run all tool calls in this turn and accumulate results
    const toolUseBlocks = (response.content || []).filter(b => b.type === 'tool_use');
    const toolResults = [];

    for (const tu of toolUseBlocks) {
      try {
        const result = await executeTool(tu.name, tu.input, conns, run.id, ctx, proposalState);
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

    // If the agent called finalize_proposal successfully this turn, exit the loop.
    if (proposalState.finalized) {
      ctx.log(`[agent] run ${run.id} finalized with ${proposalState.proposals.length} proposals`);
      return { finalText: proposalState.summary, proposalState };
    }
  }

  // Hit max turns — wrap up with whatever the agent has
  ctx.log(`[agent] run ${run.id} hit MAX_TURNS=${MAX_TURNS}, requesting wrap-up`);
  const wrapUpText = proposalState.proposals.length > 0
    ? `You have hit the maximum number of turns (${MAX_TURNS}). Call finalize_proposal now with the ${proposalState.proposals.length} proposal(s) you have already made. Do not call any other tools.`
    : `You have hit the maximum number of turns (${MAX_TURNS}). Based on what you have already seen, call propose_table_mapping for the table pair(s) you can map, then call finalize_proposal. Do not call any other tools.`;
  messages.push({ role: 'user', content: wrapUpText });
  const wrapUpTools = TOOLS.filter(t => t.name === 'propose_table_mapping' || t.name === 'finalize_proposal');
  return await wrapUpAndReturn(getAnthropic(), model, messages, wrapUpTools, run, proposalState, nextSeq, ctx);
}

// Forced wrap-up helper. Called when the loop must exit (token budget hit or
// max turns). Allows the model to make any pending propose_table_mapping
// calls plus a final finalize_proposal. If proposals come back but finalize
// doesn't, we make ONE more call asking only for finalize. This handles the
// common case where the model uses a turn to emit proposals, then a second
// turn to finalize.
async function wrapUpAndReturn(anthropic, model, messages, tools, run, proposalState, nextSeq, ctx) {
  let resp;
  try {
    resp = await anthropic.messages.create({
      model,
      max_tokens: MODEL_MAX_TOKENS,
      system: SYSTEM_PROMPT,
      tools,
      messages,
    });
  } catch (e) {
    const detail = e.status ? `${e.status} ${e.message}` : e.message;
    throw new Error(`Anthropic API call failed: ${detail}`);
  }

  // Update token usage
  let usage = resp.usage || {};
  run.tokenUsage = {
    input:   (run.tokenUsage.input  || 0) + (usage.input_tokens  || 0),
    output:  (run.tokenUsage.output || 0) + (usage.output_tokens || 0),
    costUSD: (run.tokenUsage.costUSD || 0) + estimateCostUSD(usage),
  };

  await appendMessage(run.id, {
    seq: nextSeq++,
    role: 'assistant',
    content: resp.content,
  });
  await writeRun(run);

  // Execute any tool calls in the response — propose_table_mapping (validated)
  // and finalize_proposal (sets proposalState.finalized).
  const toolUses = (resp.content || []).filter(b => b.type === 'tool_use');
  for (const tu of toolUses) {
    try {
      if (tu.name === 'propose_table_mapping') {
        tool_propose_table_mapping(tu.input, proposalState);
      } else if (tu.name === 'finalize_proposal') {
        tool_finalize_proposal(tu.input, proposalState);
      }
    } catch (e) {
      ctx.log(`[agent] wrap-up tool ${tu.name} failed: ${e.message}`);
    }
  }

  // If proposals came back but the agent didn't finalize, make ONE more
  // call asking for finalize only. Common case: wide tables consume the
  // turn's output budget on a single propose_table_mapping call.
  if (!proposalState.finalized && proposalState.proposals.length > 0) {
    ctx.log(`[agent] run ${run.id} got ${proposalState.proposals.length} proposals but no finalize, requesting one more turn`);
    messages.push({ role: 'assistant', content: resp.content });
    // Synthesize tool_results so the model's prior propose calls are acknowledged
    const fakeResults = toolUses.map(tu => ({
      type: 'tool_result',
      tool_use_id: tu.id,
      content: JSON.stringify({ accepted: true }),
    }));
    if (fakeResults.length > 0) {
      messages.push({ role: 'user', content: fakeResults });
    }
    messages.push({
      role: 'user',
      content: `Now call finalize_proposal with a brief summary of the ${proposalState.proposals.length} proposal(s) you just made. Do not call any other tools.`,
    });
    try {
      const finalizeOnly = await anthropic.messages.create({
        model,
        max_tokens: 1024,  // finalize is small
        system: SYSTEM_PROMPT,
        tools: TOOLS.filter(t => t.name === 'finalize_proposal'),
        messages,
      });
      usage = finalizeOnly.usage || {};
      run.tokenUsage = {
        input:   (run.tokenUsage.input  || 0) + (usage.input_tokens  || 0),
        output:  (run.tokenUsage.output || 0) + (usage.output_tokens || 0),
        costUSD: (run.tokenUsage.costUSD || 0) + estimateCostUSD(usage),
      };
      await appendMessage(run.id, {
        seq: nextSeq++,
        role: 'assistant',
        content: finalizeOnly.content,
      });
      await writeRun(run);
      const finalizeBlock = (finalizeOnly.content || []).find(b => b.type === 'tool_use' && b.name === 'finalize_proposal');
      if (finalizeBlock) {
        try { tool_finalize_proposal(finalizeBlock.input, proposalState); } catch (e) { ctx.log(`[agent] follow-up finalize failed: ${e.message}`); }
      }
    } catch (e) {
      ctx.log(`[agent] follow-up finalize call failed: ${e.message}`);
    }
  }

  const fallbackText = (resp.content || []).filter(b => b.type === 'text').map(b => b.text).join('\n').trim();
  return {
    finalText: proposalState.summary || fallbackText || (proposalState.proposals.length > 0 ? `${proposalState.proposals.length} proposal(s) recorded but the agent did not finalize.` : '(no proposals produced)'),
    proposalState,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// LIVE PROPOSAL — wraps the agent loop, sets pendingApproval at end
// ─────────────────────────────────────────────────────────────────────────────
async function liveProduceProposal(run, conns, ctx) {
  let result;
  try {
    result = await runAgentLoop(run, conns, ctx);
  } finally {
    // Always close pools, even if the loop threw
    await closeRunPools(run.id);
  }

  const { finalText, proposalState } = result;

  // Build the approval payload. If the agent finalized properly, we have
  // structured proposedJobs. Otherwise we have just a text summary and no
  // structured data — the user can still approve (saving zero jobs) or reject.
  run.status = 'awaiting_approval';
  run.pendingApproval = {
    type: 'propose_mapping',
    requestedAt: nowIso(),
    payload: {
      summary: finalText || 'No summary produced.',
      decisionsForUser: proposalState.decisionsForUser || [],
      risks: proposalState.risks || [],
      finalized: proposalState.finalized,
      proposedJobs: proposalState.proposals || [],
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

    // Resolve connections. Prefer body-supplied values (the browser is the
    // source of truth — Connections page persists locally, then passes the
    // current values in each request) and fall back to the Cosmos record
    // for backward compatibility. When body-supplied values are used, we
    // also write them back to Cosmos so the record stays current for any
    // other consumers (and so subsequent calls without a body still work).
    //
    // We accept BOTH transport modes per side:
    //   - direct mode: srcConnString / tgtConnString (mssql://...)
    //   - azure mode:  srcFnUrl / tgtFnUrl (HTTP endpoint at /api/db)
    // The explicit srcConnMode / tgtConnMode flags are honoured when the
    // browser sends them; otherwise the executor infers mode from which
    // field is populated.
    let conns = null;
    const bodySrc      = (body.srcConnString || '').trim();
    const bodySrcFn    = (body.srcFnUrl      || '').trim();
    const bodyTgtConn  = (body.tgtConnString || '').trim();
    const bodyTgtFn    = (body.tgtFnUrl      || '').trim();
    const haveSrc = !!(bodySrc || bodySrcFn);
    const haveTgt = !!(bodyTgtConn || bodyTgtFn);
    if (haveSrc && haveTgt) {
      conns = {
        srcConnString: bodySrc,
        srcConnMode:   body.srcConnMode === 'azure' ? 'azure' : 'direct',
        srcFnUrl:      bodySrcFn,
        srcFnKey:      (body.srcFnKey || '').trim(),
        tgtConnString: bodyTgtConn,
        tgtConnMode:   body.tgtConnMode === 'azure' ? 'azure' : 'direct',
        tgtFnUrl:      bodyTgtFn,
        tgtFnKey:      (body.tgtFnKey || '').trim(),
      };
      // Best-effort write-through to Cosmos. Failure here is not fatal —
      // the run still proceeds with the body-supplied values.
      try {
        await getCosmosContainer('projects').items.upsert({
          id: userId,
          userId,
          connections: conns,
          updatedAt: nowIso()
        });
        ctx.log(`[agent] connections refreshed in Cosmos for ${userId} (srcMode=${conns.srcConnMode}, tgtMode=${conns.tgtConnMode})`);
      } catch (e) {
        ctx.log(`[agent] cosmos connection upsert failed (non-fatal): ${e.message}`);
      }
    } else {
      conns = await getUserConnections(userId, ctx);
    }
    if (!conns) {
      return err(400, 'Source and target connections must be configured before starting an agent run. ' +
                      'Pass srcConnString (or srcFnUrl) and tgtConnString (or tgtFnUrl) in the request body, ' +
                      'or configure them via the Connections page.');
    }

    const run = newRunDoc({ userId, goal, conns });
    await getCosmosContainer('agent_runs').items.create(run);
    await appendMessage(run.id, {
      seq: 1,
      role: 'user',
      content: [{ type: 'text', text: buildInitialPrompt(goal, run) }],
    });

    ctx.log(`[agent] run ${run.id} created (mode=${run.mode}) for user ${userId}`);

    // Return the runId IMMEDIATELY so the frontend can start polling and show
    // progress as the agent works. The agent loop runs in the background.
    //
    // CAVEAT: Azure Functions v4 keeps the worker alive while there are
    // outstanding promises, but a Flex Consumption restart could still kill
    // the process mid-run. Runs left in `status: 'running'` for >6 minutes
    // are reaped by readRun's staleness check below.
    //
    // If you need rock-solid durability, refactor this to a queue trigger:
    //   1. POST migrate enqueues a message with run.id
    //   2. queue-triggered function runs the loop
    //   3. frontend polls run state same as today
    runInBackground(run, conns, ctx).catch(e => {
      ctx.log(`[agent] background run ${run.id} threw outside loop: ${e.message}`);
    });

    return ok({ runId: run.id });
  },
});

// Run the agent loop without blocking the HTTP response. Errors are caught
// and converted to a failed run state.
async function runInBackground(run, conns, ctx) {
  try {
    if (isStubMode()) {
      await stubProduceFakeProposal(run, ctx);
    } else {
      await liveProduceProposal(run, conns, ctx);
    }
  } catch (e) {
    ctx.log(`[agent] run ${run.id} failed: ${e.message}`);
    await closeRunPools(run.id);
    try {
      const fresh = await readRunRaw(run.id, run.userId);
      if (fresh) {
        fresh.status = 'failed';
        fresh.result = { error: e.message };
        await writeRun(fresh);
      }
    } catch (writeErr) {
      ctx.log(`[agent] could not record failure for ${run.id}: ${writeErr.message}`);
    }
  }
}

// Like readRun, but does NOT apply the staleness reaper. Used by the
// background runner where applying reaping would race the run itself.
async function readRunRaw(runId, userId) {
  try {
    const { resource } = await getCosmosContainer('agent_runs').item(runId, userId).read();
    return resource || null;
  } catch (e) {
    if (e.code === 404) return null;
    throw e;
  }
}

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
        // Frontend should pass the user's active projectId so the new jobs
        // appear in the right project view. Falls back to '' (the default
        // "no project" filter still shows them).
        const projectId = (body.projectId || '').trim();
        const proposedJobs = run.pendingApproval?.payload?.proposedJobs || [];
        const jobIds = await saveProposedJobs(userId, proposedJobs, run, projectId, ctx);
        run.status = 'completed';
        run.pendingApproval = null;
        run.result = {
          jobIds,
          firstJobId: jobIds[0] || null,
          jobCount: jobIds.length,
          summary: jobIds.length > 0
            ? `Saved ${jobIds.length} mapping job${jobIds.length === 1 ? '' : 's'}.`
            : 'No structured mappings to save (run did not produce any).',
        };
        await writeRun(run);
        return ok({ ok: true, status: 'completed', jobIds, firstJobId: jobIds[0] || null });
      }
      case 'edit': {
        // 'edit' currently behaves the same as 'approve' — saves the proposed
        // jobs as-is. The frontend's "Open in Object Mapping" link uses the
        // first jobId for further editing.
        const projectId = (body.projectId || '').trim();
        const proposedJobs = body.proposedJobs || run.pendingApproval?.payload?.proposedJobs || [];
        const jobIds = await saveProposedJobs(userId, proposedJobs, run, projectId, ctx);
        run.status = 'completed';
        run.pendingApproval = null;
        run.result = {
          jobIds,
          firstJobId: jobIds[0] || null,
          jobCount: jobIds.length,
          summary: 'Mapping saved with your edits.',
        };
        await writeRun(run);
        return ok({ ok: true, status: 'completed', jobIds, firstJobId: jobIds[0] || null });
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

// Save proposed table mappings as jobs in the user's projects doc.
// Each proposal becomes one job in jobs[] with the canonical shape Object
// Mapping reads. SQL fields are left empty — Object Mapping's tryAutoGenSQL
// will populate them when the user opens the job.
async function saveProposedJobs(userId, proposedJobs, run, projectId, ctx) {
  if (!Array.isArray(proposedJobs) || proposedJobs.length === 0) {
    ctx.log(`[agent] saveProposedJobs: no jobs to save for run ${run.id}`);
    return [];
  }

  const container = getCosmosContainer('projects');
  let existing = {};
  try {
    const { resource } = await container.item(userId, userId).read();
    existing = resource || {};
  } catch (e) {
    if (e.code !== 404) throw e;
  }

  const jobs = Array.isArray(existing.jobs) ? existing.jobs : [];
  const newIds = [];

  for (const proposal of proposedJobs) {
    const jobId = `job_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`;
    const job = {
      id: jobId,
      name: proposal.name || `agent_${run.id.slice(-6)}`,
      jobType: 'simple-map',
      type: 'migration',
      projectId: projectId || '',
      source: proposal.sourceTable,
      sourceTable: proposal.sourceTable,
      target: proposal.targetTable,
      targetTable: proposal.targetTable,
      // columnMapping shape mirrors what Object Mapping's auto-match produces.
      columnMapping: (proposal.columnMapping || []).map(m => ({
        srcCol: m.srcCol || '',
        tgtCol: m.tgtCol,
        transform: m.transform || 'NONE',
        match: m.match || '',
        note: m.note || '',
      })),
      joinState: [],
      // SQL fields intentionally empty — Object Mapping generates them on open
      insertSQL: '',
      schemaSQL: '',
      verifySQL: '',
      wasisRules: [],
      totalRows: 0,
      status: 'ready',
      created: nowIso(),
      warnings: Array.isArray(proposal.warnings) ? proposal.warnings : [],
      // Provenance — useful for audit and for the user to know this was AI-proposed
      origin: 'agentive_migration',
      runId: run.id,
      reasoning: proposal.reasoning || '',
      confidence: proposal.confidence || 'MEDIUM',
    };
    jobs.unshift(job);  // newest first, matches existing convention
    newIds.push(jobId);
  }

  // Cap at 100 jobs (mirrors the localStorage slice in object_mapping.html line 2721)
  existing.jobs = jobs.slice(0, 100);
  existing.id = userId;
  existing.userId = userId;
  existing.updatedAt = nowIso();
  await container.items.upsert(existing);

  ctx.log(`[agent] saved ${newIds.length} job(s) for user ${userId} (run ${run.id})`);
  return newIds;
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
