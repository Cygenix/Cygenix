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
   ========================================================================== */
'use strict';

const { can, detectDestructive, isReadOnlySql } = require('./lib/rbac');
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

async function runDirect(ctx, sql, readOnly) {
  const bridge = deps.bridge();
  const cs = ctx.connString;
  const dialect = bridge.detectDialect(cs);
  // The SQL editor's own gate, as the session's owner: environment
  // classification, role grants, Production approvals, write auditing.
  const gate = await bridge.rbacGate({ oid: ctx.oid, email: ctx.email }, 'execute', dialect, cs, null, { sql });
  if (gate.denied) {
    let d = {};
    try { d = JSON.parse(gate.denied.body || '{}'); } catch (e) { /* keep {} */ }
    return { error: (d.error || 'Not permitted.') + (d.hint ? ' ' + d.hint : '') + (gate.denied.statusCode === 428
      ? ' The Dev Console cannot wait for approvals; run this one in the SQL editor.' : '') };
  }
  const res = dialect === 'postgres'
    ? await bridge.handlePostgres('execute', cs, null, { sql, readOnly })
    : await bridge.handleMssql('execute', cs, null, { sql: readOnly ? wrapReadOnlyMssql(sql) : sql });
  let data = {};
  try { data = JSON.parse(res.body || '{}'); } catch (e) { /* keep {} */ }
  if (res.statusCode !== 200 || data.success === false) {
    return { error: (data.error || ('The database answered ' + res.statusCode)) + (data.hint ? ' ' + data.hint : '') };
  }
  return { recordset: data.recordset || [], rowsAffected: data.rowsAffected || 0 };
}

async function runFunctionApp(ctx, sql, readOnly) {
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

async function runSql(ctx, sql, readOnly) {
  try {
    return await withBudget(ctx.mode === 'azure' ? runFunctionApp(ctx, sql, readOnly) : runDirect(ctx, sql, readOnly));
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

async function callTool(ctx, name, args) {
  args = args && typeof args === 'object' ? args : {};
  const started = deps.now();
  let sql, internal = true;
  if (name === 'list_tables') sql = listTablesSql(ctx, args.schema ? cleanName(args.schema) : '');
  else if (name === 'describe_table') {
    const t = splitTable(args);
    if (!t.table) return { result: textResult('describe_table needs a table name.', true), audit: null };
    sql = describeSql(ctx, t.schema, t.table);
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
  const refusal = internal ? '' : refusalFor(sql);
  if (refusal) return { result: textResult(refusal, true), audit: Object.assign(audit, { outcome: 'denied', reason: 'destructive or USE' }) };
  if (!reads && ctx.readOnly) {
    return { result: textResult('Refused: this changes data, and "Allow changes to data this session" is off. Ask the user to '
      + 'switch it on at the top of the Dev Console if they want this to run.', true),
      audit: Object.assign(audit, { outcome: 'denied', reason: 'session is read-only' }) };
  }
  const pol = await consolePolicy(ctx, !reads);
  audit.actor = pol.actor; audit.tenant = pol.tenant;
  if (!pol.ok) return { result: textResult('Refused: ' + pol.why, true), audit: Object.assign(audit, { outcome: 'denied', reason: pol.why }) };

  const maxRows = Math.max(1, Math.min(MAX_ROWS, parseInt(args.max_rows, 10) || (internal ? MAX_ROWS : DEFAULT_ROWS)));
  const r = await runSql(ctx, sql, reads);
  const ms = deps.now() - started;
  if (r.error) return { result: textResult('The query failed: ' + r.error, true), audit: Object.assign(audit, { outcome: 'failed', reason: r.error.slice(0, 300), ms }) };
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
      severity: a.outcome !== 'allowed' ? 'notice' : (a.write ? 'high' : 'info'),
      resourceType: ctx.sessionId ? 'claudecode_session' : 'claudecode_check',
      resourceId: ctx.sessionId || ('check:' + ctx.connectionId),
      summary: (a.write ? 'Dev Console change' : 'Dev Console query') + ' on ' + (ctx.connectionName || ctx.connectionId)
        + (a.outcome === 'allowed' ? '' : ' — ' + a.outcome),
      detail: {
        tenantId: (a.tenant && a.tenant.id) || ctx.tenantId || undefined, route: 'cc-mcp',
        tool: a.tool, connection: ctx.connectionName || ctx.connectionId, profile: ctx.profileName || undefined,
        via: ctx.mode, sessionReadOnly: !!ctx.readOnly, write: !!a.write,
        sql: a.tool === 'run_query' ? String(a.sql).slice(0, 2000) : undefined,
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
          + (isPostgres(ctx) ? 'PostgreSQL' : 'SQL Server') + '). Changes to data are currently '
          + (ctx.readOnly ? 'NOT allowed.' : 'allowed.'),
      });
    case 'ping': return rpcResult(msg.id, {});
    case 'tools/list': return rpcResult(msg.id, { tools: TOOLS });
    case 'tools/call': {
      const out = await callTool(ctx, String(p.name || ''), p.arguments);
      await recordQuery(ctx, out.audit);
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

exports._internals = { deps, redeem, consolePolicy, callTool, handleMessage, refusalFor, wrapReadOnlyMssql, shape, cell,
  listTablesSql, describeSql, splitTable, fnExecuteUrl, productKeyFor, TOOLS, MAX_ROWS, DEFAULT_ROWS, MAX_TEXT,
  _reset: () => policyCache.clear() };
