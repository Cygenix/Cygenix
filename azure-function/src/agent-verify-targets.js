// agent-verify-targets.js
//
// POST /api/agent/verify-targets?code=<FUNC_KEY>
//
// Pre-flight check called when the user clicks Start on the Agentive
// Migration page. Verifies that the tables and columns the page is about
// to ask the agent to operate on actually exist in the source database —
// catching prompt/schema drift BEFORE the agent burns tokens trying to
// figure out what's wrong.
//
// What goes wrong without this:
//   The agent receives a prompt naming tables (e.g. arcs_ext_matter) and
//   columns (e.g. mstatus) that don't exist. It introspects, finds the
//   real names (Matterload, mtstatus), realises the prompt is wrong, and
//   returns a long error message — after spending real money on tokens.
//   With this pre-flight, mismatches surface in the page in <500ms with
//   zero AI cost.
//
// Body:
//   {
//     "srcConnString": "mssql://...",  // direct mode (preferred)
//     // OR
//     "srcFnUrl":      "...",          // azure mode (this function's own DB)
//     "expected": {
//       "tables":  [
//         { "schema": "dbo", "name": "Matterload" },
//         { "schema": "dbo", "name": "Addressload" }
//       ],
//       "columns": [
//         { "schema": "dbo", "name": "Matterload", "column": "mtstatus" },
//         { "schema": "dbo", "name": "Matterload", "column": "mtopendt" }
//       ]
//     }
//   }
//
// Response:
//   {
//     "ok":            true | false,    // true if NO mismatches
//     "tablesOk":      [...],
//     "tablesMissing": [...],           // tables that don't exist
//     "columnsOk":     [...],
//     "columnsMissing":[...],           // columns that don't exist
//     "suggestions":   [                // close matches for missing items
//       { "kind": "column", "asked": "mstatus", "table": "dbo.Matterload", "didYouMean": "mtstatus" }
//     ]
//   }

const { app } = require('@azure/functions');

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, x-user-id',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type':                 'application/json'
};

const ok  = (body)             => ({ status: 200, headers: CORS, body: JSON.stringify(body) });
const err = (code, msg, stack) => ({
  status: code, headers: CORS,
  body: JSON.stringify({ error: msg, ...(stack ? { stack } : {}) })
});

// ── Connection helpers (mirror agent-source-schema) ──────────────────────
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
    options: { encrypt, trustServerCertificate: trust, enableArithAbort: true },
    requestTimeout:    15000,
    connectionTimeout: 10000
  };
}

async function connectDirect(connString, ctx) {
  const sql = require('mssql');
  const cfg = parseMssqlUrl(connString);
  ctx.log(`[verify-targets] direct connect: ${cfg.database}@${cfg.server}`);
  return sql.connect(cfg);
}

async function connectViaManagedIdentity(ctx) {
  const { DefaultAzureCredential } = require('@azure/identity');
  const sql = require('mssql');
  ctx.log('[verify-targets] connecting via Managed Identity');
  const credential = new DefaultAzureCredential();
  const tokenResp  = await credential.getToken('https://database.windows.net/.default');
  return sql.connect({
    server:   process.env.SQL_SERVER,
    database: process.env.SQL_DATABASE,
    options: { encrypt: true, trustServerCertificate: false, enableArithAbort: true },
    authentication: {
      type: 'azure-active-directory-access-token',
      options: { token: tokenResp.token }
    }
  });
}

// ── "Did you mean" — find close matches for a name ───────────────────────
// Simple Levenshtein-distance-flavoured helper. We're not optimising — the
// candidate sets are tiny (columns within one table, max ~200 entries).
function levenshtein(a, b) {
  const m = a.length, n = b.length;
  if (m === 0) return n;
  if (n === 0) return m;
  const dp = Array.from({ length: m + 1 }, () => new Array(n + 1).fill(0));
  for (let i = 0; i <= m; i++) dp[i][0] = i;
  for (let j = 0; j <= n; j++) dp[0][j] = j;
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      const cost = a[i - 1].toLowerCase() === b[j - 1].toLowerCase() ? 0 : 1;
      dp[i][j] = Math.min(
        dp[i - 1][j] + 1,
        dp[i][j - 1] + 1,
        dp[i - 1][j - 1] + cost
      );
    }
  }
  return dp[m][n];
}

function bestMatch(needle, haystack) {
  if (!needle || !Array.isArray(haystack) || haystack.length === 0) return null;
  let best = null;
  let bestDist = Infinity;
  const max = Math.max(2, Math.floor(needle.length / 3)); // tolerate up to ~33% diff
  for (const candidate of haystack) {
    const d = levenshtein(needle, candidate);
    if (d < bestDist) { bestDist = d; best = candidate; }
  }
  return bestDist <= max ? best : null;
}

// ── Route registration ──────────────────────────────────────────────────
app.http('agent-verify-targets', {
  methods:   ['POST', 'OPTIONS'],
  authLevel: 'function',
  route:     'agent/verify-targets',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 204, headers: CORS, body: '' };

    let body;
    try { body = await req.json(); }
    catch (e) { return err(400, 'Invalid JSON body'); }
    if (!body || typeof body !== 'object') return err(400, 'Body must be a JSON object');

    const expected = body.expected || {};
    const expTables  = Array.isArray(expected.tables)  ? expected.tables  : [];
    const expColumns = Array.isArray(expected.columns) ? expected.columns : [];

    if (expTables.length === 0 && expColumns.length === 0) {
      return ok({ ok: true, tablesOk: [], tablesMissing: [], columnsOk: [], columnsMissing: [], suggestions: [] });
    }

    const srcConnStr = (body.srcConnString || '').trim();
    const srcFnUrl   = (body.srcFnUrl     || '').trim();
    const srcConnMode = body.srcConnMode === 'azure' ? 'azure' : 'direct';

    // Azure-mode source: verify-targets is a pre-flight optimisation that
    // avoids burning AI tokens on a hopeless run. It's not a correctness
    // check — the agent's own introspection will catch any name mismatches.
    // Rather than introduce a parallel azure-mode path here (which would
    // mean wiring the executor through this endpoint too), we cleanly
    // skip and return ok=true with a marker. The page treats this as
    // "verify skipped, proceed".
    if (srcConnMode === 'azure' || (!srcConnStr && srcFnUrl)) {
      ctx.log('[verify-targets] azure-mode source — skipping pre-flight verification');
      return ok({
        ok: true,
        skipped: true,
        skippedReason: 'azure-mode source — verification will happen during agent introspection',
        tablesOk: [], tablesMissing: [],
        columnsOk: [], columnsMissing: [],
        suggestions: []
      });
    }

    let pool;
    try {
      if (srcConnStr) pool = await connectDirect(srcConnStr, ctx);
      else return err(400, 'No source connection provided.');
    } catch (e) {
      ctx.log(`[verify-targets] connect failed: ${e.message}`);
      return err(500, `Could not connect to source: ${e.message}`,
        (e.stack || '').split('\n').slice(0, 6).join('\n'));
    }

    try {
      // Fetch all tables with their columns in one query — much faster than
      // hitting INFORMATION_SCHEMA per table.
      const r = await pool.request().query(`
        SELECT
          c.TABLE_SCHEMA AS schema_name,
          c.TABLE_NAME   AS table_name,
          c.COLUMN_NAME  AS column_name
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA NOT IN ('sys','INFORMATION_SCHEMA')
      `);

      // Build an index: schemaName.tableName -> Set of column names
      const tableCols = new Map();
      for (const row of r.recordset) {
        const key = `${row.schema_name}.${row.table_name}`.toLowerCase();
        if (!tableCols.has(key)) tableCols.set(key, []);
        tableCols.get(key).push(row.column_name);
      }
      // Also a simple set of table keys for existence checks
      const tableSet = new Set(tableCols.keys());

      // Verify tables
      const tablesOk = [];
      const tablesMissing = [];
      for (const t of expTables) {
        const key = `${(t.schema || 'dbo')}.${t.name}`.toLowerCase();
        if (tableSet.has(key)) tablesOk.push(`${t.schema || 'dbo'}.${t.name}`);
        else tablesMissing.push(`${t.schema || 'dbo'}.${t.name}`);
      }

      // Verify columns
      const columnsOk = [];
      const columnsMissing = [];
      const suggestions = [];
      for (const c of expColumns) {
        const tableKey = `${(c.schema || 'dbo')}.${c.name}`.toLowerCase();
        const cols = tableCols.get(tableKey);
        if (!cols) {
          // Table itself doesn't exist; skip column check (table is already in tablesMissing)
          columnsMissing.push(`${c.schema || 'dbo'}.${c.name}.${c.column}`);
          continue;
        }
        const colLower = c.column.toLowerCase();
        const found = cols.find(cc => cc.toLowerCase() === colLower);
        if (found) {
          columnsOk.push(`${c.schema || 'dbo'}.${c.name}.${found}`);
        } else {
          columnsMissing.push(`${c.schema || 'dbo'}.${c.name}.${c.column}`);
          // Did-you-mean suggestion
          const guess = bestMatch(c.column, cols);
          if (guess) {
            suggestions.push({
              kind:       'column',
              asked:      c.column,
              table:      `${c.schema || 'dbo'}.${c.name}`,
              didYouMean: guess
            });
          }
        }
      }

      // Did-you-mean for missing tables: search across all tables
      const allTableNames = [...tableCols.keys()].map(k => k.split('.')[1]); // unqualified names
      for (const m of tablesMissing) {
        const unqual = m.split('.').slice(1).join('.');
        const guess = bestMatch(unqual, allTableNames);
        if (guess) {
          // Find its full schema-qualified form
          const fullKey = [...tableCols.keys()].find(k => k.endsWith('.' + guess.toLowerCase()));
          if (fullKey) {
            const full = fullKey.split('.').map((p, i, a) => i === a.length - 1 ? guess : p).join('.');
            suggestions.push({
              kind:       'table',
              asked:      m,
              didYouMean: full
            });
          }
        }
      }

      return ok({
        ok: tablesMissing.length === 0 && columnsMissing.length === 0,
        tablesOk,
        tablesMissing,
        columnsOk,
        columnsMissing,
        suggestions
      });
    } catch (e) {
      ctx.log(`[verify-targets] query failed: ${e.message}`);
      return err(500, e.message,
        (e.stack || '').split('\n').slice(0, 6).join('\n'));
    } finally {
      try { await pool.close(); } catch { /* ignore */ }
    }
  }
});
