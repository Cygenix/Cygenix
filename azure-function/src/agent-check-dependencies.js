// agent-check-dependencies.js
//
// POST /api/agent/check-dependencies?code=<FUNC_KEY>
//
// Called eagerly when the user picks a group card on the Agentive Migration
// page, alongside the suggest-criteria call. Tells the user which OTHER
// themed groups the picked group depends on (via foreign keys), and how
// populated those dependency groups already are in the target.
//
// The page already has the source schema (groups, edges, classifications)
// from a prior /api/agent/source-schema call. So this function doesn't need
// to introspect source — it just receives a list of parent tables and runs
// fast COUNT(*) queries against the target.
//
// Body:
//   {
//     "tgtConnString": "mssql://...",   // direct-mode target
//     // OR
//     "tgtFnUrl":      "https://...",   // azure-mode target (this function's own URL)
//     "dependencyGroups": [
//       {
//         "groupId":   "contacts",
//         "groupName": "Contacts & People",
//         "icon":      "👤",
//         "parentTables": [
//           { "schema": "dbo", "name": "clientload", "expectedRows": 11830 },
//           { "schema": "dbo", "name": "Addressload", "expectedRows": 27370 }
//         ]
//       },
//       ...
//     ]
//   }
//
// Response:
//   {
//     "dependencies": [
//       {
//         "groupId": "contacts",
//         "groupName": "Contacts & People",
//         "icon": "👤",
//         "parentTableCount": 2,
//         "expectedRows": 39200,
//         "targetRows":   0,           // total across the parent tables we counted
//         "tablesMissing": 0,           // tables not present at all in target (table not found)
//         "tablesEmpty":   2,           // tables present but with 0 rows
//         "tablesPopulated": 0,         // tables with > 0 rows
//         "fillRatio":    0.0,          // targetRows / expectedRows, 0..1
//         "recommendation": "bring-along"
//       }
//     ],
//     "overallRecommendation": "bring-along"
//   }

const { app } = require('@azure/functions');

// ── CORS ─────────────────────────────────────────────────────────────────
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

// ── Connection-string parser (matches agent-source-schema.js) ────────────
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
    requestTimeout:    15000,
    connectionTimeout: 10000
  };
}

// ── Connection helpers (mirror source-schema) ────────────────────────────
async function connectDirect(connString, ctx) {
  const sql = require('mssql');
  const cfg = parseMssqlUrl(connString);
  ctx.log(`[check-dependencies] direct connect: ${cfg.database}@${cfg.server}`);
  return sql.connect(cfg);
}

async function connectViaManagedIdentity(ctx) {
  const { DefaultAzureCredential } = require('@azure/identity');
  const sql = require('mssql');
  ctx.log('[check-dependencies] connecting via Managed Identity to',
    process.env.SQL_SERVER + '/' + process.env.SQL_DATABASE);
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

// ── Recommendation logic ─────────────────────────────────────────────────
// Three states based on how full the target dependency group is:
//  - bring-along:    target has < 5% of expected → user almost certainly
//                    needs the parents migrated as part of this run
//  - assume-present: target has >= 95% → parents already there, skip
//  - let-agent-decide: anywhere in between, mixed state
function recommendForGroup(targetRows, expectedRows) {
  if (expectedRows === 0) return 'let-agent-decide'; // can't reason without source data
  const ratio = targetRows / expectedRows;
  if (ratio < 0.05) return 'bring-along';
  if (ratio >= 0.95) return 'assume-present';
  return 'let-agent-decide';
}

// Aggregate the per-group recommendations into a single top-level pick.
// Conservative: if ANY group looks empty, default to bring-along to avoid
// FK violations during the run.
function recommendOverall(groupRecs) {
  if (groupRecs.length === 0) return 'no-deps';
  if (groupRecs.some(r => r === 'bring-along')) return 'bring-along';
  if (groupRecs.every(r => r === 'assume-present')) return 'assume-present';
  return 'let-agent-decide';
}

// ── Route registration (v4 programming model) ───────────────────────────
app.http('agent-check-dependencies', {
  methods:   ['POST', 'OPTIONS'],
  authLevel: 'function',
  route:     'agent/check-dependencies',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 204, headers: CORS, body: '' };

    let body;
    try { body = await req.json(); }
    catch (e) { return err(400, 'Invalid JSON body'); }
    if (!body || typeof body !== 'object') return err(400, 'Body must be a JSON object');

    const dependencyGroups = Array.isArray(body.dependencyGroups) ? body.dependencyGroups : [];
    if (dependencyGroups.length === 0) {
      // No FK dependencies at all — return an empty success; the UI will
      // show "no dependencies" and hide the panel.
      return ok({ dependencies: [], overallRecommendation: 'no-deps' });
    }

    const tgtConnStr = (body.tgtConnString || '').trim();
    const tgtFnUrl   = (body.tgtFnUrl     || '').trim();

    let pool;
    try {
      if (tgtConnStr) {
        pool = await connectDirect(tgtConnStr, ctx);
      } else if (tgtFnUrl) {
        // Azure-mode target = this function. Use its own SQL via Managed Identity.
        pool = await connectViaManagedIdentity(ctx);
      } else {
        return err(400, 'No target connection provided. Pass tgtConnString or tgtFnUrl.');
      }
    } catch (e) {
      ctx.log(`[check-dependencies] target connect failed: ${e.message}`);
      // Don't fail outright — return an empty dependency report so the UI
      // can degrade gracefully ("target not reachable, defaulting to bring-along").
      return ok({
        dependencies: [],
        overallRecommendation: 'target-unreachable',
        targetError: e.message
      });
    }

    const dependencies = [];
    try {
      for (const grp of dependencyGroups) {
        const parentTables = Array.isArray(grp.parentTables) ? grp.parentTables : [];
        if (parentTables.length === 0) continue;

        // Run COUNT(*) on each parent table in target. We aggregate at the
        // group level for the UI but still need per-table counts internally
        // to compute tablesMissing / tablesEmpty / tablesPopulated.
        let targetRows = 0;
        let expectedRows = 0;
        let tablesMissing = 0;
        let tablesEmpty = 0;
        let tablesPopulated = 0;

        // Parallel COUNTs — bounded to 30 concurrent so we don't fan out
        // beyond the SQL pool's connection limit.
        const CHUNK = 30;
        for (let i = 0; i < parentTables.length; i += CHUNK) {
          const slice = parentTables.slice(i, i + CHUNK);
          const results = await Promise.all(slice.map(async (t) => {
            expectedRows += Number(t.expectedRows || 0);
            // Use sys.partitions for a fast approximate count — same
            // pattern source-schema uses. This avoids COUNT(*) which
            // would lock and be slow on large tables.
            try {
              const r = await pool.request()
                .input('schema', t.schema || 'dbo')
                .input('name',   t.name)
                .query(`
                  SELECT COALESCE(SUM(CASE WHEN p.index_id IN (0,1) THEN p.rows ELSE 0 END), 0) AS rows_count,
                         CASE WHEN OBJECT_ID(QUOTENAME(@schema) + '.' + QUOTENAME(@name), 'U') IS NULL THEN 0 ELSE 1 END AS exists_flag
                  FROM sys.tables t
                  INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                  LEFT JOIN sys.partitions p ON p.object_id = t.object_id
                  WHERE s.name = @schema AND t.name = @name
                `);
              const row = r.recordset[0] || {};
              const exists = row.exists_flag === 1;
              const rows = Number(row.rows_count || 0);
              return { exists, rows };
            } catch (e) {
              ctx.log(`[check-dependencies] count failed for ${t.schema}.${t.name}: ${e.message}`);
              return { exists: false, rows: 0, error: e.message };
            }
          }));
          for (const r of results) {
            if (!r.exists) tablesMissing++;
            else if (r.rows === 0) tablesEmpty++;
            else tablesPopulated++;
            targetRows += r.rows;
          }
        }

        const recommendation = recommendForGroup(targetRows, expectedRows);
        dependencies.push({
          groupId:          grp.groupId,
          groupName:        grp.groupName,
          icon:             grp.icon || '◆',
          parentTableCount: parentTables.length,
          expectedRows,
          targetRows,
          tablesMissing,
          tablesEmpty,
          tablesPopulated,
          fillRatio:        expectedRows > 0 ? targetRows / expectedRows : 0,
          recommendation
        });
      }
    } catch (e) {
      ctx.log(`[check-dependencies] aggregate failed: ${e.message}\n${e.stack || ''}`);
      return err(500, e.message,
        (e.stack || '').split('\n').slice(0, 6).join('\n'));
    } finally {
      try { await pool.close(); } catch { /* ignore */ }
    }

    const overallRecommendation = recommendOverall(dependencies.map(d => d.recommendation));
    return ok({ dependencies, overallRecommendation });
  }
});
