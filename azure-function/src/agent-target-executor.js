// agent-target-executor.js
//
// Polymorphic database executor for the Agentive agent.
//
// Background:
//   The agent supports two modes of target connection — direct mssql:// and
//   azure-mode (HTTP POSTs to a Cygenix-managed Function whose env vars
//   point at the actual target database). Until now, only the direct mode
//   was wired through; azure-mode was accepted at the request boundary but
//   silently ignored at runtime, causing "target connection not configured"
//   errors when the agent tried to introspect target schemas.
//
//   This module provides a single uniform interface — `getExecutor(side, conns)`
//   returns an object with `.query(sql, params)` and `.close()`, regardless
//   of which mode is in play. The agent's tool handlers can call it the
//   same way for both sides without caring which transport is used.
//
// Usage:
//   const exec = await getExecutor('target', conns, runId);
//   const rows = await exec.query('SELECT * FROM x WHERE id = @id', { id: 1 });
//   // ... later, batched per run:
//   await closeRunExecutors(runId);
//
// Important constraints documented inline:
//   - Azure-mode does not bind parameters at the Function (the /api/db
//     `execute` action takes raw SQL only). This module substitutes
//     parameters into the SQL string with strict whitelisting & escaping
//     before sending. Only string/number/boolean/null/Date params are
//     accepted; anything else throws.
//   - Azure-mode targets are determined entirely by the Function's
//     SQL_SERVER/SQL_DATABASE app settings, not by anything the user can
//     set. The user can only choose WHICH Function to call (via tgtFnUrl).
//   - The Function uses Managed Identity for auth to the SQL backend, so
//     no SQL credentials flow through the request.

const sql = require('mssql');

// Per-run executor cache. Keyed by `${runId}:${side}` so executors are
// scoped to a run and cleaned up via closeRunExecutors() when the run
// finishes. Mirrors the existing _pools cache in agent.js so the lifecycle
// shape doesn't change.
const _executors = new Map();

// ─────────────────────────────────────────────────────────────────────────────
// PARAMETER SUBSTITUTION (azure-mode only)
// ─────────────────────────────────────────────────────────────────────────────
// The Function's `execute` action takes raw SQL. We don't have a parameter-
// binding API on the HTTP side, so we substitute @param placeholders into
// the SQL ourselves. This is internal-only — the SQL templates are written
// by the agent's tool handlers, never user input — but we still escape
// rigorously and reject unexpected types so Cosmos-stored values (project
// names, table names) can't introduce injection.
function escapeSqlLiteral(value) {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new Error(`Refusing to substitute non-finite number: ${value}`);
    }
    return String(value);
  }
  if (typeof value === 'boolean') return value ? '1' : '0';
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) throw new Error('Refusing to substitute Invalid Date');
    return `'${value.toISOString().replace(/'/g, "''")}'`;
  }
  if (typeof value === 'string') {
    // Standard SQL string-literal escaping: single quote → two single quotes.
    return `'${value.replace(/'/g, "''")}'`;
  }
  throw new Error(`Unsupported parameter type for SQL substitution: ${typeof value}`);
}

// Replace @paramName with its escaped literal form. Only matches @-prefixed
// identifiers to avoid replacing email addresses or other @ usage in SQL.
function substituteParams(sqlText, params) {
  if (!params || Object.keys(params).length === 0) return sqlText;
  // Sort by length descending so @namePattern is replaced before @name.
  const keys = Object.keys(params).sort((a, b) => b.length - a.length);
  let out = sqlText;
  for (const key of keys) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
      throw new Error(`Invalid parameter name: ${key}`);
    }
    const literal = escapeSqlLiteral(params[key]);
    // Replace all @key occurrences not preceded by another word char (so
    // @nameRest doesn't match @name). Word-boundary on the right too.
    const re = new RegExp(`(?<![A-Za-z0-9_])@${key}(?![A-Za-z0-9_])`, 'g');
    out = out.replace(re, literal);
  }
  return out;
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP EXECUTOR — for azure-mode targets
// ─────────────────────────────────────────────────────────────────────────────
function buildAzureExecutor(fnUrl, fnKey) {
  if (!fnUrl) throw new Error('azure-mode executor requires fnUrl');
  // Append code= if a key was provided AND the URL doesn't already carry
  // one. The Connections page sometimes stores the key separately for
  // display purposes; the agent expects to see it in the URL when
  // calling.
  const baseUrl = (() => {
    if (!fnKey) return fnUrl;
    if (/[?&]code=/.test(fnUrl)) return fnUrl;
    return fnUrl + (fnUrl.includes('?') ? '&' : '?') + 'code=' + encodeURIComponent(fnKey);
  })();

  // The /api/db endpoint mounts actions under /api/db/{action}. The fnUrl
  // we receive may be either /api/db (no action) or /api/db/{action}.
  // Normalise to a base WITHOUT a trailing action, then we append per call.
  const apiBase = (() => {
    const u = new URL(baseUrl);
    // Strip any trailing /something off /api/db/something
    u.pathname = u.pathname.replace(/\/api\/db(\/[^/]+)?\/?$/, '/api/db');
    return u.toString();
  })();

  function urlFor(action) {
    const u = new URL(apiBase);
    u.pathname = u.pathname.replace(/\/$/, '') + '/' + action;
    return u.toString();
  }

  async function postAction(action, body) {
    const r = await fetch(urlFor(action), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body || {}),
    });
    const text = await r.text();
    let parsed;
    try { parsed = JSON.parse(text); }
    catch { throw new Error(`azure executor: non-JSON response from ${action} (${r.status}): ${text.slice(0, 300)}`); }
    if (!r.ok || parsed.error) {
      const msg = parsed.error || `HTTP ${r.status}`;
      const stack = parsed.stack ? ` :: ${parsed.stack}` : '';
      throw new Error(`azure executor (${action}): ${msg}${stack}`);
    }
    return parsed;
  }

  return {
    mode: 'azure',
    async query(sqlText, params) {
      const expanded = substituteParams(sqlText, params);
      const result = await postAction('execute', { sql: expanded });
      // Match the shape runQuery() in agent.js returns: just the recordset.
      return Array.isArray(result.recordset) ? result.recordset : [];
    },
    // Schema-shape helper. The Function's `schema` action returns a rich
    // structure; we expose it raw for handlers that want it, but most
    // handlers will just use .query() with INFORMATION_SCHEMA SQL, which
    // also works fine via .query().
    async fetchSchema() {
      return postAction('schema', {});
    },
    async close() { /* HTTP — nothing to close */ },
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// DIRECT EXECUTOR — for mssql:// connection strings
// ─────────────────────────────────────────────────────────────────────────────
// Wraps an mssql ConnectionPool in the same interface as the azure executor.
// Equivalent to the previous getPool() + runQuery() pair, just with an
// .query() method on the wrapper.
function parseMssqlUrl(connString) {
  const u = new URL(connString);
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
    options: { encrypt, trustServerCertificate: trust, enableArithAbort: true },
    requestTimeout:    30000,
    connectionTimeout: 15000,
  };
}

async function buildDirectExecutor(connString) {
  if (!connString) throw new Error('direct-mode executor requires connString');
  const config = parseMssqlUrl(connString);
  const pool = await new sql.ConnectionPool(config).connect();
  return {
    mode: 'direct',
    async query(sqlText, params) {
      const req = pool.request();
      if (params) {
        for (const [name, value] of Object.entries(params)) {
          req.input(name, value);
        }
      }
      const result = await req.query(sqlText);
      return result.recordset || [];
    },
    async close() {
      try { await pool.close(); } catch { /* ignore */ }
    },
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// PUBLIC API
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Get a per-run cached executor for source or target.
 *
 * For target side:
 *   - If conns.tgtConnMode === 'azure' OR (no tgtConnString AND tgtFnUrl set):
 *     returns an azure executor.
 *   - Else: returns a direct executor against tgtConnString.
 *
 * For source side: same logic but with src* fields.
 */
async function getExecutor(side, conns, runId) {
  if (!conns) throw new Error('getExecutor: conns is required');
  if (side !== 'source' && side !== 'target') {
    throw new Error(`getExecutor: invalid side: ${side}`);
  }
  const cacheKey = `${runId}:${side}`;
  if (_executors.has(cacheKey)) return _executors.get(cacheKey);

  // Determine mode. Honour explicit ConnMode flag first; fall back to
  // field-presence detection for older requests that don't carry the flag.
  const explicitMode = side === 'source' ? conns.srcConnMode : conns.tgtConnMode;
  const fnUrl        = side === 'source' ? conns.srcFnUrl    : conns.tgtFnUrl;
  const fnKey        = side === 'source' ? conns.srcFnKey    : conns.tgtFnKey;
  const connString   = side === 'source' ? conns.srcConnString : conns.tgtConnString;

  const isAzure = explicitMode === 'azure'
    ? !!fnUrl
    : (!connString && !!fnUrl);

  let executor;
  if (isAzure) {
    executor = buildAzureExecutor(fnUrl, fnKey);
  } else if (connString) {
    executor = await buildDirectExecutor(connString);
  } else {
    throw new Error(`No ${side} connection configured (mode=${explicitMode || 'inferred'})`);
  }

  _executors.set(cacheKey, executor);
  return executor;
}

/**
 * Close all executors for a run. Mirrors closeRunPools() in the original
 * agent.js — call this in the run's finally block.
 */
async function closeRunExecutors(runId) {
  const keys = [..._executors.keys()].filter(k => k.startsWith(`${runId}:`));
  for (const k of keys) {
    const exec = _executors.get(k);
    try { await exec.close(); } catch { /* ignore */ }
    _executors.delete(k);
  }
}

module.exports = {
  getExecutor,
  closeRunExecutors,
  // exported for tests
  _internal: { substituteParams, escapeSqlLiteral, parseMssqlUrl },
};
