/* ============================================================================
   cc-mcp.js — the Dev Console's database bridge: an MCP server
   ----------------------------------------------------------------------------
   Oct-2026. The Dev Console's Claude works in a workspace hosted by
   Anthropic, and that workspace may only make web connections: SQL Server's
   and PostgreSQL's ports time out to every destination, a server built to
   answer on any port included. That was established on a real customer
   server after a day of firewall and router changes that made no
   difference. So Claude does not connect to the database at all. It calls
   this server — over HTTPS, from Anthropic's platform, which is allowed —
   and this server runs the query along the road the SQL editor already
   uses (db-connect's parsers, relay, Entra handling and RBAC gate), then
   hands the rows back.

   THE PROTOCOL
   MCP's streamable HTTP transport, stateless: every request is one POST of
   JSON-RPC, answered with application/json. initialize, ping, tools/list
   and tools/call; notifications are accepted and answered 202. No SSE
   stream is offered, so GET is 405, which the transport allows.

   WHO IS ASKING
   Nobody signs in here. Each Dev Console session has its own pass, held in
   an Anthropic vault and presented as a Bearer token; "Check the bridge"
   gets a two-minute one. The pass is redeemed at the Function App
   (agent/claude-code-bridge/redeem, host key, refused by data-proxy) for
   the ONE connection it was issued for, the session's owner and whether
   changes are allowed right now. Nothing the caller says is trusted beyond
   the pass. A revoked or expired pass is a 401 before any method runs.

   WHAT IS ENFORCED HERE, NOT ASKED OF CLAUDE
   - The organisation's Dev Console switch and role list, and the RBAC
     grant, checked again on every call: a role removed mid-session stops
     the bridge at the next query.
   - "Allow changes to data": off, anything that is not a read is refused;
     and reads are always run so that nothing they did could be kept — SQL
     Server inside a transaction that is rolled back, PostgreSQL inside a
     READ ONLY transaction that is rolled back.
   - Destructive statements — DROP, TRUNCATE, and DELETE or UPDATE without a
     WHERE — are refused in every mode. They belong to the SQL editor and
     its approvals.
   - One connection per session: USE is refused.
   - At most 1,000 rows, and about 90,000 characters, go back per call.
   - Every tools/call is recorded on the organisation's hash-chained audit
     trail as claudecode.query: who, which connection, read or write, the
     statement (redacted by the audit schema), rows, time — and refusals.

   STAGING SESSIONS (Oct-2026)
   A session may be opened with a staging schema, for building the staging
   tables a Conversion Template describes inside the connected database and
   loading them from the rest of it. The owner's rule: free inside that
   schema, read-only everywhere else. So in a staging session:
   - a read runs exactly as above, protected;
   - a write runs only if lib/staging-sql.js shows that every object it
     writes to is <staging>.<table> (or a #temp / @table that dies with the
     call). Inside the schema, DROP and TRUNCATE are allowed — rebuilding
     staging is the point — and the organisation's approval guardrail is
     not asked for (rbacGate's stagingSchema option; the role and
     environment check still decide, and the write is still audited);
   - the write runs in a transaction — SQL Server with XACT_ABORT ON, so a
     cancel rolls it back — and every statement, read or write, is
     cancelled ON THE SERVER after STATEMENT_MS, a little inside the time
     this function has, so a slow load is stopped and undone rather than
     left running behind a caller that gave up. Claude is told to load in
     slices when that happens;
   - "Allow changes to data" plays no part: the schema is the permission.
   A staging session needs a connection Cygenix logs in to itself: not a
   Function App connection and not the Entra relay, neither of which can be
   cancelled from here.

   THE TARGET, READ-ONLY (Oct-2026)
   A staging session on the source may carry the profile's target as a
   reference connection (redeemed with the pass, unsealed fresh like the
   session's own). target_list_tables, target_describe_table and
   target_query run against it, and only ever as reads: anything that is not
   a read is refused before it is sent, and every read runs inside a
   transaction that is rolled back, as all reads here do. "Allow changes",
   the staging schema and the session's own connection play no part. Each
   call is audited under the target's connection name.

   THE RULES (Oct-2026)
   get_translation_rules serves the Was/Is rules and global Parameters the
   session was opened with (agent/claude-code-bridge/rules, by the same
   pass): an overview first, then one table's rules at a time.

   THE TEMPLATE
   get_conversion_template reads the project's Conversion Template through
   the Function App (agent/claude-code-bridge/template, with the session's
   own pass, so it is always that session's project): the overview first,
   then a module or a table at a time with its columns and a CREATE TABLE
   for the staging schema built the way the template page builds its DDL.
   ========================================================================== */
'use strict';

const { can, detectDestructive, isReadOnlySql } = require('./lib/rbac');
const stagingSql = require('./lib/staging-sql');
const { orgStore, resolveActor, appendAudit, loadAll } = require('./lib/org-store');
const tenancy = require('./lib/tenancy');

const API_BASE = process.env.CYGENIX_DATA_API_BASE
  || 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/data';
const AGENT_ROOT = String(API_BASE).replace(/\/data\/?$/, '');
const PRODUCT_DB_API = String(process.env.AZURE_DB_API_URL
  || 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api').replace(/\/+$/, '');

const MAX_ROWS = 1000;
const DEFAULT_ROWS = 200;
const MAX_TEXT = 90000;            // under the 100,000 characters Anthropic inlines
const MAX_SQL = 100000;
const QUERY_BUDGET_MS = 21000;     // answer before Netlify's 26 seconds do
const STATEMENT_MS = 19000;        // and cancel the statement on the server before that
const TEMPLATE_TEXT = 60000;       // a template page of tables, under MAX_TEXT with room for the wrapper
const POLICY_TTL_MS = 30 * 1000;
const PROTOCOLS = ['2025-06-18', '2025-03-26', '2024-11-05'];

const HEADERS = { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' };
const httpReply = (statusCode, data, extra) => ({ statusCode, headers: Object.assign({}, HEADERS, extra || {}),
  body: data === undefined ? '' : JSON.stringify(data) });

// ── Dependencies (swapped in tests) ──────────────────────────────────────
const deps = {
  fetch: (...a) => fetch(...a),
  store: () => orgStore(),
  bridge: () => require('./db-connect').__bridge,
  resolveActor, appendAudit, loadAll,
  resolveTenant: (store, actor, users) => tenancy.resolveTenant(store, actor, users),
  now: () => Date.now(),
  env: (k) => process.env[k],
};

// ── The pass ─────────────────────────────────────────────────────────────
async function redeem(token) {
  const key = String(deps.env('CYGENIX_DATA_FN_KEY') || '').trim();
  if (!key) return { ok: false, status: 503, error: 'The bridge is not configured on this deployment (CYGENIX_DATA_FN_KEY).' };
  let res, text;
  try {
    res = await deps.fetch(AGENT_ROOT + '/agent/claude-code-bridge/redeem?code=' + encodeURIComponent(key), {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token }), signal: AbortSignal.timeout(8000),
    });
    text = await res.text();
  } catch (e) {
    return { ok: false, status: 503, error: 'Could not reach the Cygenix pass check: ' + ((e && e.message) || e) };
  }
  let data = {};
  try { data = JSON.parse(text || '{}'); } catch (e) { /* keep {} */ }
  if (res.status === 200 && data.ok) return { ok: true, ctx: data };
  if (res.status === 401) return { ok: false, status: 401, error: data.error || 'This bridge pass is not valid.' };
  return { ok: false, status: res.status === 409 ? 409 : 502, error: data.error || ('The pass check answered ' + res.status) };
}

// ── The organisation's say, every call (cached briefly per person) ───────
const policyCache = new Map();
async function consolePolicy(ctx, mutating) {
  const key = ctx.oid + '|' + (mutating ? 'w' : 'r');
  const hit = policyCache.get(key);
  if (hit && hit.until > deps.now()) return hit.value;
  const store = deps.store();
  const actor = await deps.resolveActor(store, { oid: ctx.oid, email: ctx.email }, {});
  let value;
  const decision = can(actor, 'claudecode.use', { mutating });
  if (!decision.allow) {
    value = { ok: false, actor, why: mutating ? 'Your role cannot let the Dev Console change data.' : 'The Dev Console is not enabled for your role.' };
  } else {
    const { tenant } = await deps.resolveTenant(store, actor, (await deps.loadAll(store)).users);
    const policy = tenancy.normaliseClaudeCode(tenant.claudeCode);
    if (!policy.enabled) value = { ok: false, actor, tenant, why: 'The Dev Console is switched off for this organisation.' };
    else if (!actor.roles.some(r => policy.roles.indexOf(r) !== -1)) value = { ok: false, actor, tenant, why: 'The Dev Console is not enabled for your role.' };
    else value = { ok: true, actor, tenant };
  }
  if (value.ok) policyCache.set(key, { until: deps.now() + POLICY_TTL_MS, value });
  else policyCache.delete(key);
  return value;
}

// ── Running SQL ──────────────────────────────────────────────────────────
// SQL Server reads run inside a transaction that is always rolled back, so
// a statement that slipped past the classifier still cannot keep anything.
// The text is wrapped rather than a Transaction object used, because the
// same text runs over a direct pool, the Entra relay and a Function App.
function wrapReadOnlyMssql(sql) {
  const body = String(sql).replace(/;\s*$/, '');
  return 'SET XACT_ABORT ON;\nBEGIN TRANSACTION;\n' + body + '\n;\nIF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;';
}
function isPostgres(ctx) { return ctx.dbType === 'postgres'; }

// A staging write: all of it or none of it. XACT_ABORT ON makes SQL Server
// roll the transaction back on an error AND on the cancel that STATEMENT_MS
// sends, which is what turns "stopped after 19 seconds" into "nothing kept".
// CREATE SCHEMA must be the first statement of its batch, so it is sent as
// it is; on its own it is atomic anyway.
function wrapStagingMssql(sql) {
  const body = String(sql).replace(/;\s*$/, '');
  if (/^\s*CREATE\s+SCHEMA\b/i.test(body)) return body;
  return 'SET XACT_ABORT ON;\nBEGIN TRANSACTION;\n' + body + '\n;\nIF @@TRANCOUNT > 0 COMMIT TRANSACTION;';
}

function productKeyFor(fnUrl) {
  try {
    const want = new URL(PRODUCT_DB_API).host.toLowerCase();
    if (new URL(fnUrl).host.toLowerCase() !== want) return '';
  } catch (e) { return ''; }
  return String(deps.env('AZURE_FUNCTION_KEY') || deps.env('CYGENIX_DATA_FN_KEY') || '').trim();
}
function fnExecuteUrl(fnUrl, fnKey) {
  const u = new URL(fnUrl);
  if (!u.searchParams.has('code')) {
    const k = fnKey || productKeyFor(fnUrl);
    if (k) u.searchParams.set('code', k);
  }
  return u.toString();
}

function withBudget(promise) {
  let timer;
  const late = new Promise((resolve) => {
    timer = setTimeout(() => resolve({ error: 'The query took longer than ' + Math.round(QUERY_BUDGET_MS / 1000)
      + ' seconds, so the bridge stopped waiting. Narrow it — filter, aggregate, or TOP/LIMIT — and try again.' }), QUERY_BUDGET_MS);
  });
  return Promise.race([promise, late]).finally(() => clearTimeout(timer));
}

// how: 'read' (protected, rolled back), 'write' (as sent), 'staging' (a
// write lib/staging-sql.js has already checked).
async function runDirect(ctx, sql, how) {
  const bridge = deps.bridge();
  const cs = ctx.connString;
  const dialect = bridge.detectDialect(cs);
  const staging = how === 'staging';
  if (staging && dialect !== 'postgres' && bridge.viaRelay(cs)) {
    return { error: 'This connection is reached through the Azure relay, which cannot stop a statement part way, so the Dev Console will not write staging tables over it. Use a connection with its own login.' };
  }
  // The SQL editor's own gate, as the session's owner: environment
  // classification, role grants, Production approvals, write auditing — the
  // approvals lifted only for a checked staging write.
  const gate = await bridge.rbacGate({ oid: ctx.oid, email: ctx.email }, 'execute', dialect, cs, null, { sql },
    staging ? { stagingSchema: ctx.stagingSchema } : undefined);
  if (gate.denied) {
    let d = {};
    try { d = JSON.parse(gate.denied.body || '{}'); } catch (e) { /* keep {} */ }
    return { error: (d.error || 'Not permitted.') + (d.hint ? ' ' + d.hint : '') + (gate.denied.statusCode === 428
      ? ' The Dev Console cannot wait for approvals; run this one in the SQL editor.' : '') };
  }
  const readOnly = how === 'read';
  const res = dialect === 'postgres'
    ? await bridge.handlePostgres('execute', cs, null, { sql, readOnly, transaction: staging, timeoutMs: STATEMENT_MS })
    : await bridge.handleMssql('execute', cs, null, { sql: readOnly ? wrapReadOnlyMssql(sql) : staging ? wrapStagingMssql(sql) : sql, timeoutMs: STATEMENT_MS });
  let data = {};
  try { data = JSON.parse(res.body || '{}'); } catch (e) { /* keep {} */ }
  if (res.statusCode !== 200 || data.success === false) {
    return { error: (data.error || ('The database answered ' + res.statusCode)) + (data.hint ? ' ' + data.hint : '') };
  }
  return { recordset: data.recordset || [], rowsAffected: data.rowsAffected || 0 };
}

async function runFunctionApp(ctx, sql, how) {
  const readOnly = how === 'read';
  if (how === 'staging') return { error: 'A staging session cannot write through a Function App connection.' };
  let res, text;
  try {
    res = await deps.fetch(fnExecuteUrl(ctx.fnUrl, ctx.fnKey), {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'execute', sql: readOnly ? wrapReadOnlyMssql(sql) : sql }),
      signal: AbortSignal.timeout(QUERY_BUDGET_MS),
    });
    text = await res.text();
  } catch (e) {
    return { error: 'Could not reach the Function App for "' + ctx.connectionName + '": ' + ((e && e.message) || e) };
  }
  let data = {};
  try { data = JSON.parse(text || '{}'); } catch (e) {
    return { error: 'The Function App answered HTTP ' + res.status + (res.status === 401 ? ' — its key was refused.' : '.') };
  }
  if (!res.ok || data.success === false) return { error: data.error || ('The Function App answered HTTP ' + res.status) };
  return { recordset: data.recordset || [], rowsAffected: data.rowsAffected || 0 };
}

async function runSql(ctx, sql, how) {
  try {
    return await withBudget(ctx.mode === 'azure' ? runFunctionApp(ctx, sql, how) : runDirect(ctx, sql, how));
  } catch (e) {
    return { error: ((e && e.message) || String(e)) + (e && e.hint ? ' ' + e.hint : '') };
  }
}

// ── Shaping rows for Claude ──────────────────────────────────────────────
function cell(v) {
  if (v === null || v === undefined) return null;
  if (v instanceof Date) return isNaN(v) ? null : v.toISOString();
  if (typeof v === 'bigint') return v.toString();
  if (Buffer.isBuffer(v)) return '0x' + v.toString('hex').slice(0, 64) + (v.length > 32 ? '…' : '');
  // Rows arrive here as JSON from db-connect, so binary comes as Node's
  // JSON form of a Buffer; show it as the short hex it is, not a byte list.
  if (typeof v === 'object' && v.type === 'Buffer' && Array.isArray(v.data)) return cell(Buffer.from(v.data));
  if (typeof v === 'object') return JSON.stringify(v);
  return v;
}
function shape(recordset, maxRows, extra) {
  const rows = Array.isArray(recordset) ? recordset : [];
  const columns = rows.length ? Object.keys(rows[0]) : [];
  let out = rows.slice(0, maxRows).map(r => columns.map(c => cell(r[c])));
  const result = Object.assign({ columns, rows: out, row_count_returned: out.length,
    truncated: rows.length > out.length, total_rows_read: rows.length }, extra || {});
  let text = JSON.stringify(result);
  while (text.length > MAX_TEXT && out.length > 1) {
    out = out.slice(0, Math.floor(out.length / 2));
    Object.assign(result, { rows: out, row_count_returned: out.length, truncated: true,
      note: 'Cut to fit the size limit; aggregate or select fewer columns for more.' });
    text = JSON.stringify(result);
  }
  return text;
}

// ── The tools ────────────────────────────────────────────────────────────
const TOOLS = [
  {
    name: 'list_tables',
    title: 'List tables',
    description: 'List the tables and views in this session\'s database, with schema, type and approximate row count. Optionally only one schema.',
    inputSchema: { type: 'object', properties: { schema: { type: 'string', description: 'Only this schema.' } }, additionalProperties: false },
    annotations: { readOnlyHint: true },
  },
  {
    name: 'describe_table',
    title: 'Describe a table',
    description: 'The columns of one table or view: name, data type, length, precision, nullable, default, and whether each is part of the primary key.',
    inputSchema: { type: 'object', properties: {
      table: { type: 'string', description: 'Table name; "schema.table" also works.' },
      schema: { type: 'string', description: 'Schema, if not given in table.' },
    }, required: ['table'], additionalProperties: false },
    annotations: { readOnlyHint: true },
  },
  {
    name: 'run_query',
    title: 'Run a query',
    description: 'Run one SQL statement against this session\'s database and return the first result set, up to max_rows rows '
      + '(default ' + DEFAULT_ROWS + ', at most ' + MAX_ROWS + '). Reads always work. Changes work only while the user has allowed '
      + 'changes to data in this session; DROP, TRUNCATE, and DELETE or UPDATE without WHERE are always refused. Aggregate in SQL '
      + 'rather than fetching large tables.',
    inputSchema: { type: 'object', properties: {
      sql: { type: 'string', description: 'One SQL statement.' },
      max_rows: { type: 'integer', minimum: 1, maximum: MAX_ROWS, description: 'Rows to return (default ' + DEFAULT_ROWS + ').' },
    }, required: ['sql'], additionalProperties: false },
  },
];

// run_query says what this session may do; a staging session says it differently.
function runQueryTool(ctx) {
  if (!ctx.stagingSchema) return TOOLS[2];
  const sch = ctx.stagingSchema;
  return Object.assign({}, TOOLS[2], {
    description: 'Run SQL against this session\'s database and return the first result set, up to max_rows rows (default '
      + DEFAULT_ROWS + ', at most ' + MAX_ROWS + '). This is a STAGING session: anything may be created, loaded, emptied or dropped '
      + 'inside the schema "' + sch + '" — always write names in full as ' + sch + '.<table> — and everything else in the database is '
      + 'read-only. A statement that changes "' + sch + '" runs in a transaction and must not contain comments; one that writes '
      + 'anywhere else, or that cannot be checked (EXEC, MERGE, an UPDATE or DELETE through an alias), is refused with the reason. '
      + 'Every statement is stopped after ' + Math.round(STATEMENT_MS / 1000) + ' seconds and anything it changed is undone, so load '
      + 'large tables in slices (by key range or date).',
  });
}
const TEMPLATE_TOOL = {
  name: 'get_conversion_template',
  title: 'Read the Conversion Template',
  description: 'Read this project\'s Conversion Template: which staging tables to build and the target tables they mirror. Call it '
    + 'first with no arguments for the overview (modules, tables, load order). Then call it with a module, or a table, for the '
    + 'columns — name, type, required in the target, identity — and a CREATE TABLE statement for the staging schema. The staging '
    + 'tables take the target\'s column names and types exactly, every column nullable; the data comes from this database.',
  inputSchema: { type: 'object', properties: {
    module: { type: 'string', description: 'One module, by name.' },
    table: { type: 'string', description: 'One table, by staging or target name.' },
    template_id: { type: 'string', description: 'A different template from the list the overview gives.' },
  }, additionalProperties: false },
  annotations: { readOnlyHint: true },
};
const TARGET_TOOLS = [
  {
    name: 'target_list_tables',
    title: 'List the target\'s tables',
    description: 'List the tables and views in the TARGET database (read-only), with schema, type and approximate row count. Optionally only one schema.',
    inputSchema: { type: 'object', properties: { schema: { type: 'string', description: 'Only this schema.' } }, additionalProperties: false },
    annotations: { readOnlyHint: true },
  },
  {
    name: 'target_describe_table',
    title: 'Describe a target table',
    description: 'The columns of one table or view in the TARGET database (read-only): name, type, length, nullable, default, primary key.',
    inputSchema: { type: 'object', properties: {
      table: { type: 'string', description: 'Table name; "schema.table" also works.' },
      schema: { type: 'string', description: 'Schema, if not given in table.' },
    }, required: ['table'], additionalProperties: false },
    annotations: { readOnlyHint: true },
  },
  {
    name: 'target_query',
    title: 'Read from the target',
    description: 'Run one READ-ONLY SQL statement against the TARGET database and return the first result set, up to max_rows rows '
      + '(default ' + DEFAULT_ROWS + ', at most ' + MAX_ROWS + '). Use it to read lookup and reference tables, check codes and see what is '
      + 'already there. Anything that is not a read is refused; nothing can be changed in the target from here.',
    inputSchema: { type: 'object', properties: {
      sql: { type: 'string', description: 'One read-only SQL statement.' },
      max_rows: { type: 'integer', minimum: 1, maximum: MAX_ROWS, description: 'Rows to return (default ' + DEFAULT_ROWS + ').' },
    }, required: ['sql'], additionalProperties: false },
    annotations: { readOnlyHint: true },
  },
];
const RULES_TOOL = {
  name: 'get_translation_rules',
  title: 'Read the Was/Is rules and Parameters',
  description: 'The user\'s Was/Is translation rules (in a source table\'s field, this old value becomes this new value) and global '
    + 'Parameters (named values written @@Name, such as cut-off dates or default codes). Call it with no arguments for the overview — '
    + 'every Parameter and how many rules each source table and field has — then with a table (and optionally a field) for the rules.',
  inputSchema: { type: 'object', properties: {
    table: { type: 'string', description: 'A source table name.' },
    field: { type: 'string', description: 'A field of that table.' },
  }, additionalProperties: false },
  annotations: { readOnlyHint: true },
};
function hasTarget(ctx) { return !!(ctx.reference && !ctx.reference.error && (ctx.reference.connString || ctx.reference.fnUrl)); }
function toolsFor(ctx) {
  const list = [TOOLS[0], TOOLS[1], runQueryTool(ctx)];
  if (ctx.projectId) list.push(TEMPLATE_TOOL);
  if (ctx.reference) list.push(...TARGET_TOOLS);
  if (ctx.rules) list.push(RULES_TOOL);
  return list;
}
// The session as seen through its reference connection: the target's road,
// the target's name, and read-only whatever the session itself may do.
function targetCtx(ctx) {
  const r = ctx.reference || {};
  const t = Object.assign({}, ctx, {
    connectionId: r.connectionId, connectionName: r.connectionName, side: r.side || 'tgt', dbType: r.dbType || 'sqlserver',
    mode: r.mode === 'azure' ? 'azure' : 'direct', connString: r.connString || null, fnUrl: r.fnUrl || null, fnKey: r.fnKey || null,
    readOnly: true, stagingSchema: '', reference: null,
  });
  if (ctx._token) Object.defineProperty(t, '_token', { value: ctx._token, enumerable: false });
  return t;
}

function lit(ctx, v) {
  const s = String(v).replace(/'/g, "''");
  return isPostgres(ctx) ? "'" + s + "'" : "N'" + s + "'";
}
function cleanName(v) {
  return String(v == null ? '' : v).trim().replace(/^[\["`]|[\]"`]$/g, '').slice(0, 256);
}
function splitTable(args) {
  let table = String(args.table || '').trim(), schema = args.schema ? cleanName(args.schema) : '';
  if (!schema) {
    const m = /^(\[[^\]]+\]|"[^"]+"|[^.]+)\.(.+)$/.exec(table);
    if (m) { schema = cleanName(m[1]); table = m[2]; }
  }
  return { schema, table: cleanName(table) };
}
function listTablesSql(ctx, schema) {
  if (isPostgres(ctx)) {
    return "SELECT t.table_schema AS schema, t.table_name AS name, CASE t.table_type WHEN 'BASE TABLE' THEN 'table' ELSE 'view' END AS type, "
      + 'c.reltuples::bigint AS approx_rows FROM information_schema.tables t '
      + 'LEFT JOIN pg_namespace n ON n.nspname = t.table_schema LEFT JOIN pg_class c ON c.relname = t.table_name AND c.relnamespace = n.oid '
      + "WHERE t.table_schema NOT IN ('pg_catalog','information_schema')" + (schema ? ' AND t.table_schema = ' + lit(ctx, schema) : '')
      + ' ORDER BY 1, 2';
  }
  return "SELECT s.name AS [schema], o.name AS [name], CASE o.type WHEN 'U' THEN 'table' ELSE 'view' END AS [type], "
    + '(SELECT SUM(p.rows) FROM sys.partitions p WHERE p.object_id = o.object_id AND p.index_id IN (0,1)) AS approx_rows '
    + "FROM sys.objects o JOIN sys.schemas s ON s.schema_id = o.schema_id WHERE o.type IN ('U','V') AND o.is_ms_shipped = 0"
    + (schema ? ' AND s.name = ' + lit(ctx, schema) : '') + ' ORDER BY s.name, o.name';
}
function describeSql(ctx, schema, table) {
  const pk = 'SELECT ku.table_schema AS ks, ku.table_name AS kt, ku.column_name AS kc FROM information_schema.table_constraints tc '
    + 'JOIN information_schema.key_column_usage ku ON ku.constraint_name = tc.constraint_name AND ku.table_schema = tc.table_schema '
    + "WHERE tc.constraint_type = 'PRIMARY KEY'";
  return 'SELECT c.table_schema AS ' + (isPostgres(ctx) ? 'schema' : '[schema]') + ', c.column_name AS ' + (isPostgres(ctx) ? '"column"' : '[column]')
    + ', c.data_type AS type, c.character_maximum_length AS max_length, c.numeric_precision AS precision, c.numeric_scale AS scale, '
    + 'c.is_nullable AS nullable, c.column_default AS ' + (isPostgres(ctx) ? '"default"' : '[default]') + ', '
    + 'CASE WHEN k.kc IS NULL THEN 0 ELSE 1 END AS primary_key FROM information_schema.columns c '
    + 'LEFT JOIN (' + pk + ') k ON k.ks = c.table_schema AND k.kt = c.table_name AND k.kc = c.column_name '
    + 'WHERE c.table_name = ' + lit(ctx, table) + (schema ? ' AND c.table_schema = ' + lit(ctx, schema) : '')
    + ' ORDER BY c.table_schema, c.ordinal_position';
}

// What may never run through the bridge, whatever the mode.
function refusalFor(sql) {
  const bad = detectDestructive(sql).filter(d => /^DROP|TRUNCATE/.test(d.type) || !d.bounded);
  if (bad.length) {
    const d = bad[0];
    return 'Refused: ' + d.type + (d.table ? ' ' + d.table : '') + (/^DROP|TRUNCATE/.test(d.type) ? '' : ' without a WHERE clause')
      + ' is a destructive statement, which the Dev Console never runs. Use the SQL editor, where it goes through your organisation\'s approvals.';
  }
  if (/(^|;)\s*USE\s+/i.test(String(sql).replace(/--[^\n]*|\/\*[\s\S]*?\*\//g, ''))) {
    return 'Refused: USE would leave this session\'s database. The Dev Console works on one connection per session; start a session on the other one.';
  }
  return '';
}

function textResult(text, isError) { return { content: [{ type: 'text', text }], isError: !!isError }; }

/* ── The Conversion Template, for Claude ─────────────────────────────────
   sqlType, populateVerdict and moduleActive are COPIES of the functions of
   the same names in public/cygenix-template-spec.js, which builds the
   template page's workbook and DDL. They are copied rather than required
   because that file lives outside this function's folder; tests/cc-mcp.test.js
   holds the copies to the originals on every column shape the template
   stores. If you change one, change the other. */
const LENGTH_TYPES = ['char', 'varchar', 'nchar', 'nvarchar', 'binary', 'varbinary'];
const PRECISION_SCALE_TYPES = ['decimal', 'numeric', 'dec'];
const PRECISION_ONLY_TYPES = ['datetime2', 'datetimeoffset', 'time'];
function sqlType(col) {
  const c = col || {};
  const t = String(c.dataType || c.type || '').trim().toLowerCase();
  if (!t) return '';
  if (t.indexOf('(') > 0) return t;
  if (LENGTH_TYPES.indexOf(t) !== -1 && c.maxLength != null) {
    let len = Number(c.maxLength);
    if (len === -1) return t + '(max)';
    if (t === 'nvarchar' || t === 'nchar') len = Math.floor(len / 2) || len;
    return t + '(' + len + ')';
  }
  if (PRECISION_SCALE_TYPES.indexOf(t) !== -1 && c.precision != null) return t + '(' + c.precision + ',' + (c.scale == null ? 0 : c.scale) + ')';
  if (PRECISION_ONLY_TYPES.indexOf(t) !== -1 && c.scale != null) return t + '(' + c.scale + ')';
  return t;
}
function populateVerdict(col) {
  const c = col || {};
  if (c.isIdentity) return 'No — identity';
  if (c.isComputed) return 'No — computed';
  return c.isNullable === false ? 'Required' : 'Optional';
}
function moduleActive(m) { return !!m && m.inScope !== false && !!m.included; }

const qb = (n) => '[' + String(n).replace(/]/g, ']]') + ']';
const qd = (n) => '"' + String(n).replace(/"/g, '""') + '"';
// The staging table's CREATE, the way the template page writes its DDL:
// the target's columns and types, every one NULL, computed columns left out,
// no keys, no defaults, no identity — and no comments, which a staging write
// may not carry.
function stagingCreate(ctx, table) {
  const cols = (table.columns || []).filter(c => c && !c.isComputed && c.name);
  if (!cols.length || !ctx.stagingSchema) return null;
  const sch = ctx.stagingSchema;
  if (isPostgres(ctx)) {
    return 'CREATE TABLE IF NOT EXISTS ' + qd(sch) + '.' + qd(table.stagingTable) + ' (' + cols.map(c => qd(c.name) + ' ' + sqlType(c) + ' NULL').join(', ') + ')';
  }
  return "IF OBJECT_ID(N'" + (sch + '.' + table.stagingTable).replace(/'/g, "''") + "', N'U') IS NULL CREATE TABLE "
    + qb(sch) + '.' + qb(table.stagingTable) + ' (' + cols.map(c => qb(c.name) + ' ' + sqlType(c) + ' NULL').join(', ') + ')';
}

function templateTables(tpl) {
  const mods = Array.isArray(tpl && tpl.modules) ? tpl.modules : [];
  let active = mods.filter(moduleActive);
  let note = '';
  if (!active.length) {
    active = mods.filter(m => m && m.inScope !== false);
    if (active.length) note = 'No module is ticked "Include" on this template yet, so every module in scope is shown.';
  }
  return { note, modules: active.map(m => ({
    module: String(m.module || ''),
    tables: (Array.isArray(m.tables) ? m.tables : []).slice().sort((a, b) =>
      (Number(a.loadOrder) || 0) - (Number(b.loadOrder) || 0) || String(a.targetTable).localeCompare(String(b.targetTable))),
  })) };
}
const lc = (v) => String(v == null ? '' : v).trim().toLowerCase();

function templateView(ctx, data, args) {
  const tpl = data.template || {};
  const head = {
    template: { id: data.chosen && data.chosen.id, name: tpl.name, version: tpl.version, status: tpl.status,
      target_type: tpl.targetType || undefined, profile: tpl.profileId || undefined },
    other_templates: (data.templates || []).filter(t => !data.chosen || t.id !== data.chosen.id)
      .slice(0, 20).map(t => ({ template_id: t.id, name: t.name, version: t.version, kind: t.kind, profile: t.profileId || undefined })),
    staging_schema: ctx.stagingSchema || null,
  };
  const { note, modules } = templateTables(tpl);
  if (note) head.note = note;
  const wantModule = lc(args.module), wantTable = lc(args.table);

  if (!wantModule && !wantTable) {
    head.modules = modules.map(m => ({ module: m.module, tables: m.tables.map(t => ({
      staging_table: t.stagingTable, target_table: t.targetTable, load_order: Number(t.loadOrder) || 0,
      required: t.required !== false, columns: (t.columns || []).length, notes: t.notes || undefined })) }));
    head.next = 'Call get_conversion_template with a module or a table for its columns and CREATE TABLE.';
    return JSON.stringify(head);
  }
  let picked = [];
  modules.forEach(m => m.tables.forEach(t => {
    if ((wantModule && lc(m.module) === wantModule) || (wantTable && (lc(t.stagingTable) === wantTable || lc(t.targetTable) === wantTable))) {
      picked.push({ module: m.module, t });
    }
  }));
  if (!picked.length) {
    return JSON.stringify(Object.assign(head, { error: 'No ' + (wantTable ? 'table "' + args.table + '"' : 'module "' + args.module + '"')
      + ' in this template. Call get_conversion_template with no arguments for the list.' }));
  }
  const out = [], left = [];
  let size = JSON.stringify(head).length;
  picked.forEach(({ module, t }) => {
    const entry = {
      module, staging_table: t.stagingTable, target_table: t.targetTable, load_order: Number(t.loadOrder) || 0,
      required: t.required !== false, notes: t.notes || undefined,
      columns: (t.columns || []).map(c => ({ name: c.name, type: sqlType(c), required_in_target: c.isNullable === false,
        identity: !!c.isIdentity || undefined, computed: !!c.isComputed || undefined, primary_key: !!c.isPrimaryKey || undefined,
        populate: populateVerdict(c) })),
      create_table: stagingCreate(ctx, t) || undefined,
    };
    if (!entry.columns.length) entry.note = 'The template has no column detail for this table yet: read the target\'s columns on the template page first.';
    const n = JSON.stringify(entry).length;
    if (out.length && size + n > TEMPLATE_TEXT) { left.push(t.stagingTable); return; }
    out.push(entry); size += n;
  });
  head.tables = out;
  if (left.length) head.not_shown = { tables: left, why: 'Too much for one answer: ask for these one table at a time.' };
  if (isPostgres(ctx)) head.types_note = 'Column types are the target\'s as recorded in the template; adjust any PostgreSQL does not have.';
  return JSON.stringify(head);
}

async function fetchTemplate(ctx, templateId) {
  const key = String(deps.env('CYGENIX_DATA_FN_KEY') || '').trim();
  if (!key) return { error: 'The bridge is not configured on this deployment (CYGENIX_DATA_FN_KEY).' };
  let res, text;
  try {
    res = await deps.fetch(AGENT_ROOT + '/agent/claude-code-bridge/template?code=' + encodeURIComponent(key), {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token: ctx._token, templateId: templateId || '' }), signal: AbortSignal.timeout(10000),
    });
    text = await res.text();
  } catch (e) {
    return { error: 'Could not reach the Conversion Templates store: ' + ((e && e.message) || e) };
  }
  let data = {};
  try { data = JSON.parse(text || '{}'); } catch (e) { /* keep {} */ }
  if (res.status !== 200 || !data.ok) return { error: data.error || ('The template store answered ' + res.status) };
  return { data };
}

async function templateTool(ctx, args) {
  const audit = { tool: 'get_conversion_template', sql: '', write: false };
  if (!ctx.projectId) return { result: textResult('This session was not opened from a project, so it has no Conversion Template.', true), audit: null };
  const pol = await consolePolicy(ctx, false);
  audit.actor = pol.actor; audit.tenant = pol.tenant;
  if (!pol.ok) return { result: textResult('Refused: ' + pol.why, true), audit: Object.assign(audit, { outcome: 'denied', reason: pol.why }) };
  const started = deps.now();
  const got = await fetchTemplate(ctx, args.template_id ? String(args.template_id).slice(0, 200) : '');
  const ms = deps.now() - started;
  if (got.error) return { result: textResult(got.error, true), audit: Object.assign(audit, { outcome: 'failed', reason: got.error.slice(0, 300), ms }) };
  return { result: textResult(templateView(ctx, got.data, args), false),
    audit: Object.assign(audit, { outcome: 'allowed', ms, reason: (got.data.chosen && got.data.chosen.id) || undefined }) };
}

/* ── The Was/Is rules and Parameters ──────────────────────────────────────
   paramLiteral is the SQL a Parameter stands for, as public/cygenix-params.js
   formatValue writes it: number unquoted (when it is one), raw as written,
   text and date quoted with quotes doubled. A COPY; tests/cc-mcp.test.js
   holds the two to the same answers. */
function paramLiteral(p) {
  const raw = p && p.value != null ? String(p.value) : '';
  let t = p && p.type ? String(p.type).toLowerCase() : '';
  if (['number', 'text', 'date', 'raw'].indexOf(t) === -1) t = (raw.trim() !== '' && /^-?\d+(\.\d+)?$/.test(raw.trim())) ? 'number' : 'text';
  if (t === 'number' && /^-?\d+(\.\d+)?$/.test(raw.trim())) return raw.trim();
  if (t === 'raw') return raw;
  return "'" + raw.replace(/'/g, "''") + "'";
}
async function fetchRules(ctx) {
  const key = String(deps.env('CYGENIX_DATA_FN_KEY') || '').trim();
  if (!key) return { error: 'The bridge is not configured on this deployment (CYGENIX_DATA_FN_KEY).' };
  let res, text;
  try {
    res = await deps.fetch(AGENT_ROOT + '/agent/claude-code-bridge/rules?code=' + encodeURIComponent(key), {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token: ctx._token }), signal: AbortSignal.timeout(10000),
    });
    text = await res.text();
  } catch (e) {
    return { error: 'Could not reach the rules store: ' + ((e && e.message) || e) };
  }
  let data = {};
  try { data = JSON.parse(text || '{}'); } catch (e) { /* keep {} */ }
  if (res.status !== 200 || !data.ok) return { error: data.error || ('The rules store answered ' + res.status) };
  return { data };
}
function rulesView(data, args) {
  const wasis = Array.isArray(data.wasis) ? data.wasis : [];
  const params = (Array.isArray(data.params) ? data.params : []).map(p => ({ name: p.name || undefined, token: p.code || undefined,
    type: p.type || undefined, value: p.value, sql: paramLiteral(p), note: p.note || undefined }));
  const wantT = lc(args.table).replace(/^[\["`]|[\]"`]$/g, ''), wantF = lc(args.field);
  const bare = (t) => t.split('.').pop();
  if (!wantT) {
    const groups = {};
    wasis.forEach(w => { const k = w.table + '|' + w.field; groups[k] = (groups[k] || 0) + 1; });
    return JSON.stringify({ parameters: params,
      was_is: Object.keys(groups).sort().map(k => ({ table: k.split('|')[0], field: k.split('|')[1], rules: groups[k] })),
      truncated: data.truncated || undefined,
      next: wasis.length ? 'Call get_translation_rules with a table (and a field) for its rules.' : undefined });
  }
  const hit = wasis.filter(w => (w.table === wantT || bare(w.table) === bare(wantT)) && (!wantF || w.field === wantF));
  let out = hit.map(w => ({ field: w.field, from: w.from, to: w.to, note: w.note || undefined }));
  let text = JSON.stringify({ table: args.table, field: args.field || undefined, rules: out });
  while (text.length > MAX_TEXT && out.length > 1) {
    out = out.slice(0, Math.floor(out.length / 2));
    text = JSON.stringify({ table: args.table, field: args.field || undefined, rules: out, truncated: true,
      note: 'Too many to show at once: ask for one field at a time.' });
  }
  if (!hit.length) text = JSON.stringify({ table: args.table, field: args.field || undefined, rules: [],
    note: 'No Was/Is rules for this ' + (wantF ? 'field' : 'table') + '. Call get_translation_rules with no arguments for the tables that have them.' });
  return text;
}
async function rulesTool(ctx, args) {
  const audit = { tool: 'get_translation_rules', sql: '', write: false };
  if (!ctx.rules) return { result: textResult('This session was opened without Was/Is rules or Parameters.', true), audit: null };
  const pol = await consolePolicy(ctx, false);
  audit.actor = pol.actor; audit.tenant = pol.tenant;
  if (!pol.ok) return { result: textResult('Refused: ' + pol.why, true), audit: Object.assign(audit, { outcome: 'denied', reason: pol.why }) };
  const started = deps.now();
  const got = await fetchRules(ctx);
  const ms = deps.now() - started;
  if (got.error) return { result: textResult(got.error, true), audit: Object.assign(audit, { outcome: 'failed', reason: got.error.slice(0, 300), ms }) };
  return { result: textResult(rulesView(got.data, args), false), audit: Object.assign(audit, { outcome: 'allowed', ms }) };
}

// The target tools: the same three as the session's own, on the reference
// connection, and reads only.
async function targetTool(ctx, name, args) {
  if (!ctx.reference) return { rpcError: { code: -32602, message: 'Unknown tool: ' + name } };
  if (!hasTarget(ctx)) return { result: textResult('The target cannot be read in this session: ' + (ctx.reference.error || 'no target connection.'), true), audit: null };
  const tctx = targetCtx(ctx);
  const own = name.replace(/^target_/, '').replace(/^query$/, 'run_query');
  if (own === 'run_query') {
    const sql = String(args.sql == null ? '' : args.sql);
    if (sql.trim() && !isReadOnlySql(sql)) {
      return { result: textResult('Refused: the target is read-only in the Dev Console. target_query runs reads only — load the staging '
        + 'tables in this session\'s own database with run_query.', true),
        audit: { tool: name, sql, write: true, outcome: 'denied', reason: 'target is read-only' }, auditCtx: tctx };
    }
  }
  const out = await callTool(tctx, own, args);
  if (out.audit) out.audit.tool = name;
  out.auditCtx = tctx;
  return out;
}

async function callTool(ctx, name, args) {
  args = args && typeof args === 'object' ? args : {};
  if (/^target_(list_tables|describe_table|query)$/.test(name)) return targetTool(ctx, name, args);
  if (name === 'get_translation_rules') return rulesTool(ctx, args);
  const started = deps.now();
  let sql, internal = true;
  if (name === 'list_tables') sql = listTablesSql(ctx, args.schema ? cleanName(args.schema) : '');
  else if (name === 'describe_table') {
    const t = splitTable(args);
    if (!t.table) return { result: textResult('describe_table needs a table name.', true), audit: null };
    sql = describeSql(ctx, t.schema, t.table);
  } else if (name === 'get_conversion_template') {
    return templateTool(ctx, args);
  } else if (name === 'run_query') {
    internal = false;
    sql = String(args.sql == null ? '' : args.sql);
    if (!sql.trim()) return { result: textResult('run_query needs some SQL.', true), audit: null };
    if (sql.length > MAX_SQL) return { result: textResult('That statement is over ' + MAX_SQL + ' characters.', true), audit: null };
  } else {
    return { rpcError: { code: -32602, message: 'Unknown tool: ' + name } };
  }

  const reads = internal || isReadOnlySql(sql);
  const audit = { tool: name, sql, write: !reads };
  // A staging session's writes answer to the staging rule and nothing else;
  // its reads, and every other session's statements, to the rules above.
  const staging = !reads && !!ctx.stagingSchema;
  if (staging) {
    const chk = stagingSql.checkStagingWrite(sql, { schema: ctx.stagingSchema, dialect: isPostgres(ctx) ? 'postgres' : 'sqlserver' });
    if (!chk.ok) {
      return { result: textResult('Refused: ' + chk.why, true), audit: Object.assign(audit, { outcome: 'denied', reason: 'staging: ' + chk.why.slice(0, 200), staging: true }) };
    }
    audit.staging = true;
  } else {
    const refusal = internal ? '' : refusalFor(sql);
    if (refusal) return { result: textResult(refusal, true), audit: Object.assign(audit, { outcome: 'denied', reason: 'destructive or USE' }) };
    if (!reads && ctx.readOnly) {
      return { result: textResult('Refused: this changes data, and the "Allow changes" switch is off for this session. Ask the user to '
        + 'switch it on at the top of the Dev Console if they want this to run.', true),
        audit: Object.assign(audit, { outcome: 'denied', reason: 'session is read-only' }) };
    }
  }
  const pol = await consolePolicy(ctx, !reads);
  audit.actor = pol.actor; audit.tenant = pol.tenant;
  if (!pol.ok) return { result: textResult('Refused: ' + pol.why, true), audit: Object.assign(audit, { outcome: 'denied', reason: pol.why }) };

  const maxRows = Math.max(1, Math.min(MAX_ROWS, parseInt(args.max_rows, 10) || (internal ? MAX_ROWS : DEFAULT_ROWS)));
  const r = await runSql(ctx, sql, reads ? 'read' : staging ? 'staging' : 'write');
  const ms = deps.now() - started;
  if (r.error) {
    let msg = 'The query failed: ' + r.error;
    if (/^Cancelled after/.test(r.error)) {
      msg += staging
        ? ' Nothing it changed was kept — it ran in a transaction that was rolled back. Load this in slices (by key range or date) so each statement finishes in time.'
        : ' Narrow it — filter, aggregate, or TOP/LIMIT — and try again.';
    }
    return { result: textResult(msg, true), audit: Object.assign(audit, { outcome: 'failed', reason: r.error.slice(0, 300), ms }) };
  }
  const text = shape(r.recordset, maxRows, reads ? { ms } : { rows_affected: r.rowsAffected, ms });
  return { result: textResult(text, false), audit: Object.assign(audit, { outcome: 'allowed', rows: (r.recordset || []).length, rowsAffected: r.rowsAffected, ms }) };
}

async function recordQuery(ctx, a) {
  if (!a) return;
  try {
    const store = deps.store();
    await deps.appendAudit(store, {
      actorOid: ctx.oid, actorEmail: ctx.email, effectiveRoles: (a.actor && a.actor.roles) || [],
      action: 'claudecode.query', outcome: a.outcome,
      severity: a.outcome !== 'allowed' ? 'notice' : (a.write ? (a.staging ? 'notice' : 'high') : 'info'),
      resourceType: ctx.sessionId ? 'claudecode_session' : 'claudecode_check',
      resourceId: ctx.sessionId || ('check:' + ctx.connectionId),
      summary: (a.write ? (a.staging ? 'Dev Console staging change' : 'Dev Console change') : a.tool === 'get_conversion_template' ? 'Dev Console template read'
        : a.tool === 'get_translation_rules' ? 'Dev Console rules read' : /^target_/.test(a.tool) ? 'Dev Console target read' : 'Dev Console query') + ' on ' + (ctx.connectionName || ctx.connectionId)
        + (a.outcome === 'allowed' ? '' : ' — ' + a.outcome),
      detail: {
        tenantId: (a.tenant && a.tenant.id) || ctx.tenantId || undefined, route: 'cc-mcp',
        tool: a.tool, connection: ctx.connectionName || ctx.connectionId, profile: ctx.profileName || undefined,
        via: ctx.mode, sessionReadOnly: !!ctx.readOnly, write: !!a.write, stagingSchema: ctx.stagingSchema || undefined,
        sql: a.tool === 'run_query' || a.tool === 'target_query' ? String(a.sql).slice(0, 2000) : undefined,
        rows: a.rows, rowsAffected: a.write ? a.rowsAffected : undefined, ms: a.ms, reason: a.reason,
      },
    });
  } catch (e) {
    // The trail being unreachable does not take the answer away; it is
    // logged, without the statement, for whoever reads the function logs.
    console.error('[cc-mcp] audit append failed:', e && e.message);
  }
}

// ── JSON-RPC ─────────────────────────────────────────────────────────────
function rpcResult(id, result) { return { jsonrpc: '2.0', id, result }; }
function rpcError(id, code, message) { return { jsonrpc: '2.0', id: id === undefined ? null : id, error: { code, message } }; }

async function handleMessage(ctx, msg) {
  if (!msg || msg.jsonrpc !== '2.0' || typeof msg.method !== 'string') return rpcError(msg && msg.id, -32600, 'Invalid request');
  const isNote = msg.id === undefined || msg.id === null;
  const p = msg.params || {};
  switch (msg.method) {
    case 'initialize':
      return rpcResult(msg.id, {
        protocolVersion: PROTOCOLS.indexOf(p.protocolVersion) !== -1 ? p.protocolVersion : PROTOCOLS[0],
        capabilities: { tools: { listChanged: false } },
        serverInfo: { name: 'cygenix', title: 'Cygenix database bridge', version: '1.0.0' },
        instructions: 'Tools for this Dev Console session\'s one database, "' + (ctx.connectionName || ctx.connectionId) + '" ('
          + (isPostgres(ctx) ? 'PostgreSQL' : 'SQL Server') + '). ' + (ctx.stagingSchema
            ? 'Staging session: changes are allowed inside the schema "' + ctx.stagingSchema + '" only; everything else is read-only.'
            : 'Changes to data are currently ' + (ctx.readOnly ? 'NOT allowed.' : 'allowed.'))
          + (ctx.reference ? ' The target_* tools read the target, "' + (ctx.reference.connectionName || 'target') + '", read-only.' : ''),
      });
    case 'ping': return rpcResult(msg.id, {});
    case 'tools/list': return rpcResult(msg.id, { tools: toolsFor(ctx) });
    case 'tools/call': {
      const out = await callTool(ctx, String(p.name || ''), p.arguments);
      await recordQuery(out.auditCtx || ctx, out.audit);
      if (out.rpcError) return rpcError(msg.id, out.rpcError.code, out.rpcError.message);
      return rpcResult(msg.id, out.result);
    }
    default:
      if (isNote || msg.method.indexOf('notifications/') === 0) return null;
      return rpcError(msg.id, -32601, 'Method not found: ' + msg.method);
  }
}

exports.handler = async function (event) {
  const method = event.httpMethod;
  if (method === 'OPTIONS') {
    return httpReply(204, undefined, { 'Access-Control-Allow-Methods': 'POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization, Mcp-Session-Id, Mcp-Protocol-Version' });
  }
  if (method !== 'POST') return httpReply(405, { error: 'POST only — this MCP server offers no event stream.' }, { Allow: 'POST' });

  const h = event.headers || {};
  const auth = String(h.authorization || h.Authorization || '').trim();
  const m = /^Bearer\s+(\S+)$/i.exec(auth);
  if (!m) return httpReply(401, { error: 'A bridge pass is required.' }, { 'WWW-Authenticate': 'Bearer' });

  let body;
  try { body = JSON.parse(event.body || ''); }
  catch (e) { return httpReply(400, rpcError(null, -32700, 'Parse error')); }

  const pass = await redeem(m[1]);
  if (!pass.ok) {
    return httpReply(pass.status, { error: pass.error }, pass.status === 401 ? { 'WWW-Authenticate': 'Bearer error="invalid_token"' } : undefined);
  }
  const ctx = pass.ctx;
  // The pass itself, for the template read, which the Function App scopes to
  // this session's project by the same pass. Never logged or audited.
  Object.defineProperty(ctx, '_token', { value: m[1], enumerable: false });

  const batch = Array.isArray(body);
  const replies = [];
  for (const msg of (batch ? body : [body])) {
    try {
      const r = await handleMessage(ctx, msg);
      if (r) replies.push(r);
    } catch (e) {
      replies.push(rpcError(msg && msg.id, -32603, 'Internal error: ' + ((e && e.message) || e)));
    }
  }
  if (!replies.length) return httpReply(202, undefined);
  return httpReply(200, batch ? replies : replies[0]);
};

exports._internals = { deps, redeem, consolePolicy, callTool, handleMessage, refusalFor, wrapReadOnlyMssql, wrapStagingMssql, shape, cell,
  TARGET_TOOLS, RULES_TOOL, targetCtx, hasTarget, paramLiteral, rulesView, fetchRules,
  toolsFor, runQueryTool, TEMPLATE_TOOL, templateView, templateTables, stagingCreate, sqlType, populateVerdict, moduleActive, STATEMENT_MS, TEMPLATE_TEXT,
  listTablesSql, describeSql, splitTable, fnExecuteUrl, productKeyFor, TOOLS, MAX_ROWS, DEFAULT_ROWS, MAX_TEXT,
  _reset: () => policyCache.clear() };
