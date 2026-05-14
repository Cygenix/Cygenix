// netlify/functions/scheduler-run-background.js
//
// Background runner for scheduled migration jobs. Invoked fire-and-forget
// by scheduler.js's run-now handler. Reads a queued `runs` document from
// Cosmos, executes the migration server-side using the same logic as the
// dashboard's manual runner (runMigrationStep in project-builder.html),
// and writes final state back to the runs document.
//
// ── Netlify Background Function semantics ──────────────────────────────────
// - Filename ends with `-background.js` → Netlify spawns this asynchronously
//   on POST, returns 202 to the caller immediately, and gives the function
//   up to 15 minutes to complete.
// - Caller cannot read the return value. All results must be persisted to
//   Cosmos (`runs` container) before the function exits.
// - There is no Netlify-side status endpoint; the dashboard polls
//   scheduler?action=get-run to learn what happened.
//
// ── What this runner DOES ──────────────────────────────────────────────────
// - SQL-script steps (jobType 'sql' or step has only top-level .sql with no
//   srcTable): execute as one statement on the target.
// - Migration steps (simple-map, mapping-based migration): paginated SELECT
//   from source, per-row transforms (TRIM/UPPER/LOWER/CAST), per-row Was/Is
//   substitution, fixed/literal value handling, length-truncate, identity-
//   column detection on target, batched INSERTs to target, accurate
//   rows-affected via target before/after delta.
// - connOn dispatch: each step uses srcConn or tgtConn per its connOn field.
//
// ── What this runner DOES NOT do (deliberately) ────────────────────────────
// - dm_staging schema / rollback support — manual Project Builder runs have
//   it; scheduled unattended runs don't need it (per Curtis 2026-05-14).
// - Reconciliation — runs skipped with explicit "scheduled-run mode" marker.
// - One-to-many cursor SQL — skipped with explicit marker. These are rare
//   and use a different execution shape (target-side cursor with parameter
//   substitution, already done at snapshot time by phase 2a).
// - Progress reporting back to the dashboard — step log captures the same
//   detail, surfaced via the run-detail modal after completion.

const { CosmosClient } = require('@azure/cosmos');
const sql = require('mssql');

// ── Cosmos client (module-scoped so subsequent invocations reuse it) ───────
let _cosmosClient = null;
let _containers = null;
function getContainers() {
  if (_containers) return _containers;
  const conn = process.env.COSMOS_CONNECTION_STRING;
  if (!conn) throw new Error('COSMOS_CONNECTION_STRING env var not set');
  _cosmosClient = new CosmosClient(conn);
  const db = _cosmosClient.database(process.env.COSMOS_DATABASE || 'cygenix');
  _containers = {
    schedules: db.container('schedules'),
    runs:      db.container('runs'),
  };
  return _containers;
}

// ── mssql connection helpers ───────────────────────────────────────────────
function parseMssqlUrl(connStr) {
  const u = new URL(connStr);
  const params = u.searchParams;
  return {
    user:     decodeURIComponent(u.username || ''),
    password: decodeURIComponent(u.password || ''),
    server:   u.hostname,
    port:     u.port ? parseInt(u.port, 10) : 1433,
    database: u.pathname.replace(/^\//, ''),
    options: {
      encrypt:                params.get('encrypt') !== 'false',
      trustServerCertificate: params.get('trustServerCertificate') === 'true',
      enableArithAbort:       true,
    },
    requestTimeout: 600_000,   // 10 min per query — well within 15 min lambda
    connectionTimeout: 30_000,
  };
}
const isHttpUrl    = s => /^https?:\/\//i.test(s || '');
const isMssqlConn  = s => /^mssql:\/\//i.test(s || '');

// ── Per-row transform helpers, ported from project-builder.html ────────────
// Mirrors fmtVal / applyMappingTransform / applyStepWasis / isSqlExpression
// so scheduled runs produce identical output to manual "Run selected" on
// the same job. Any divergence here will silently produce different rows on
// 2am scheduled runs vs daytime manual runs — which is exactly the failure
// mode we promised not to introduce. If you change these, change them in
// project-builder.html the same way.

const SQL_EXPRESSION_ALLOWLIST = [
  'newid', 'newsequentialid',
  'getdate', 'getutcdate', 'sysdatetime', 'sysutcdatetime',
  'sysdatetimeoffset', 'current_timestamp',
  'host_name', 'suser_sname', 'user_name', 'app_name',
  'session_user', 'current_user', 'system_user',
  '@@spid', '@@servername', '@@version',
];
function isSqlExpression(s) {
  if (typeof s !== 'string') return false;
  const norm = s.toLowerCase().replace(/\s+/g, '');
  if (norm.startsWith('@@')) return SQL_EXPRESSION_ALLOWLIST.includes(norm);
  const m = /^([a-z_][a-z_0-9]*)\(\)$/.exec(norm);
  return !!(m && SQL_EXPRESSION_ALLOWLIST.includes(m[1]));
}
function fmtVal(v) {
  if (v === null || v === undefined) return 'NULL';
  if (typeof v === 'boolean') return v ? '1' : '0';
  if (typeof v === 'number')  return String(v);
  if (v instanceof Date)      return "'" + v.toISOString() + "'"; // ISO is parseable by SQL Server
  const s = String(v);
  if (/^-?\d+(\.\d+)?$/.test(s)) return s;
  if (isSqlExpression(s))        return s;
  return "N'" + s.replace(/'/g, "''") + "'";
}
function applyMappingTransform(m, v) {
  if (!m) return v;
  // Fixed/literal overrides source entirely.
  if (m.literalValue != null && m.literalValue !== '') return m.literalValue;
  if (m.fixedValue   != null && m.fixedValue   !== '') return m.fixedValue;
  let out = v;
  // Was/Is rules on this column mapping
  if (Array.isArray(m.wasisRules) && m.wasisRules.length && out != null) {
    const sv = String(out);
    for (const rule of m.wasisRules) {
      if (!rule) continue;
      const oldVal = rule.oldVal ?? rule.was ?? rule.from;
      const newVal = rule.newVal ?? rule.is  ?? rule.to;
      if (oldVal == null) continue;
      if (sv === String(oldVal)) { out = newVal; break; }
    }
  }
  // String transforms
  if (out != null) {
    const t = String(m.transform || 'NONE').toUpperCase();
    if (t === 'TRIM' || t === 'UPPER' || t === 'LOWER') {
      const s = String(out);
      if (t === 'TRIM')  out = s.trim();
      if (t === 'UPPER') out = s.toUpperCase();
      if (t === 'LOWER') out = s.toLowerCase();
    }
  }
  // Auto-truncate char widths
  const srcType = String(m.srcType || '').toUpperCase();
  const tgtType = String(m.tgtType || '').toUpperCase();
  const isCharish = t => /CHAR|TEXT/.test(t);
  if (out != null && isCharish(srcType) && isCharish(tgtType)) {
    const lenMatch = tgtType.match(/\((\d+|MAX)\)/);
    const tgtLen = lenMatch && lenMatch[1] !== 'MAX' ? parseInt(lenMatch[1], 10) : null;
    if (tgtLen != null) {
      const s = String(out);
      if (s.length > tgtLen) out = s.slice(0, tgtLen);
    }
  }
  return out;
}
function applyStepWasis(step, srcCol, v) {
  if (v == null) return v;
  const rules = step.wasisRules || step.wasIsRules;
  if (!Array.isArray(rules) || !rules.length) return v;
  const sv = String(v);
  for (const r of rules) {
    if (!r) continue;
    const ruleCol = r.srcField || r.field || r.column;
    if (ruleCol && String(ruleCol).toLowerCase() !== String(srcCol).toLowerCase()) continue;
    const oldVal = r.oldVal ?? r.was ?? r.from;
    const newVal = r.newVal ?? r.is  ?? r.to;
    if (oldVal == null) continue;
    if (sv === String(oldVal)) return newVal;
  }
  return v;
}

// ── Identity column detection ──────────────────────────────────────────────
// SQL Server rejects INSERTs into IDENTITY columns unless IDENTITY_INSERT is
// ON for that table. We detect the target's IDENTITY columns and exclude
// them from the INSERT column list. Mirrors what StagingArea.ensureTable
// does in the manual runner (just without the staging table itself).
async function getIdentityColumns(pool, fullTableName) {
  // fullTableName may be "dbo.Addressload" or "[dbo].[Addressload]" — normalise.
  const m = /^\s*\[?([^.\[\]]+)\]?\.\[?([^\[\]]+)\]?\s*$/.exec(fullTableName);
  const schema = m ? m[1] : 'dbo';
  const table  = m ? m[2] : fullTableName.replace(/[\[\]]/g, '');
  const r = await pool.request()
    .input('sch', sql.NVarChar, schema)
    .input('tbl', sql.NVarChar, table)
    .query(`
      SELECT c.name AS col
      FROM sys.columns c
      JOIN sys.tables  t ON c.object_id = t.object_id
      JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = @sch AND t.name = @tbl AND c.is_identity = 1
    `);
  const set = new Set();
  (r.recordset || []).forEach(row => set.add(String(row.col).toLowerCase()));
  return set;
}

// ── Page-fetch (same SQL as db-connect.js fetch-page) ──────────────────────
async function fetchPage(pool, baseSql, offset, pageSize) {
  const cleaned = String(baseSql).trim().replace(/;+\s*$/, '');
  const hasOrderBy = /\border\s+by\b/i.test(cleaned);
  const paged = hasOrderBy
    ? `${cleaned}\nOFFSET ${offset} ROWS FETCH NEXT ${pageSize + 1} ROWS ONLY`
    : `${cleaned}\nORDER BY (SELECT NULL) OFFSET ${offset} ROWS FETCH NEXT ${pageSize + 1} ROWS ONLY`;
  const r = await pool.request().query(paged);
  const rows = r.recordset || [];
  const hasMore = rows.length > pageSize;
  if (hasMore) rows.pop();
  return { rows, hasMore };
}

// ── Build INSERT for a batch of rows ───────────────────────────────────────
// Returns SQL like:
//   INSERT INTO [dbo].[Addressload] ([col1], [col2], ...)
//   VALUES (...), (...), ...
// or null if the mapping produced zero usable columns or the batch was empty.
function buildBatchInsert(tgtTable, mapping, batch, step, identityCols) {
  if (!batch.length) return null;
  // Filter mapping: drop columns whose tgtCol is in the identity set.
  const useMapping = mapping.filter(m =>
    m && m.tgtCol && !identityCols.has(String(m.tgtCol).toLowerCase())
  );
  if (!useMapping.length) return null;

  const colList = useMapping.map(m => '[' + m.tgtCol + ']').join(', ');
  const valueRows = batch.map(row => {
    const vals = useMapping.map(m => {
      // Look up the raw source value by srcCol, then transform.
      let v = (m.srcCol && Object.prototype.hasOwnProperty.call(row, m.srcCol))
        ? row[m.srcCol]
        : (m.srcCol ? null : undefined);
      // Per-mapping transform (incl. fixed/literal override)
      v = applyMappingTransform(m, v);
      // Step-level Was/Is — applied AFTER mapping-level
      v = applyStepWasis(step, m.srcCol, v);
      return fmtVal(v);
    });
    return '(' + vals.join(', ') + ')';
  });

  return 'INSERT INTO ' + tgtTable +
         ' (' + colList + ')\nVALUES\n' + valueRows.join(',\n');
}

// ── Step executors ─────────────────────────────────────────────────────────

async function execSqlStep(step, ensureSrcPool, ensureTgtPool, log) {
  // Raw-SQL step: either jobType=sql or step.sql exists with no srcTable.
  const stepSql = (step.sql || step.insertSQL || '').trim();
  if (!stepSql) return { status: 'skipped', reason: 'no sql', rowsAffected: 0, log: [] };
  const connOn = step.connOn === 'source' ? 'source' : 'target';
  const pool = connOn === 'source' ? await ensureSrcPool() : await ensureTgtPool();
  log.push('Executing raw SQL on ' + connOn + ' (' + stepSql.length.toLocaleString() + ' chars)');
  const r = await pool.request().query(stepSql);
  const rows = Array.isArray(r.rowsAffected) ? r.rowsAffected.reduce((a, b) => a + (b || 0), 0) : (r.rowsAffected || 0);
  log.push('Done. rowsAffected=' + rows.toLocaleString());
  return { status: 'success', rowsAffected: rows, connOn, log };
}

async function execMigrationStep(step, ensureSrcPool, ensureTgtPool, log) {
  // Reject upfront the things we don't support.
  if (step.jobType === 'one-to-many' || step.oneToManyConfig) {
    return { status: 'skipped', reason: 'one-to-many migrations not supported in scheduled runs yet — run from Project Builder', log: [] };
  }
  if (step.reconciliation) {
    log.push('Note: reconciliation config present; skipped in scheduled-run mode (no rollback context).');
  }
  if (!step.srcTable) {
    return { status: 'failed', errorMessage: 'srcTable not set on step', log };
  }

  // Same-connection / small-SQL single-shot path. Mirrors the dashboard's
  // useSingleShot heuristic so we don't paginate when one-shot would do.
  const SAVED_SQL_INLINE_LIMIT = 2000;
  let srcPool, tgtPool;
  try { srcPool = await ensureSrcPool(); }
  catch (e) {
    return { status: 'failed', errorMessage: 'Could not open source connection: ' + e.message, log };
  }
  try { tgtPool = await ensureTgtPool(); }
  catch (e) {
    return { status: 'failed', errorMessage: 'Could not open target connection: ' + e.message, log };
  }

  const sameConn = sameConnDirective(srcPool, tgtPool);
  const useSingleShot = sameConn
    && step.insertSQL
    && typeof step.insertSQL === 'string'
    && step.insertSQL.trim()
    && step.insertSQL.length <= SAVED_SQL_INLINE_LIMIT;

  if (useSingleShot) {
    log.push('Single-shot path (same connection, ' + step.insertSQL.length + ' chars).');
    return await execMigrationSingleShot(step, tgtPool, log);
  }

  log.push('Paginated path: source → target with per-row transforms.');
  return await execMigrationPaginated(step, srcPool, tgtPool, log);
}

// Helper to tell whether two mssql pools point at the same instance+db.
// We compare configured server/port/database — same triple = same.
function sameConnDirective(a, b) {
  if (!a || !b || !a.config || !b.config) return false;
  return a.config.server === b.config.server
      && a.config.port   === b.config.port
      && a.config.database === b.config.database;
}

async function execMigrationSingleShot(step, tgtPool, log) {
  const insertSQL = step.insertSQL;
  // Capture target row count before/after for accurate delta (SET NOCOUNT ON
  // in the generated script suppresses rowsAffected on the driver).
  let rowsBefore = null, rowsAfter = null;
  try {
    const cr = await tgtPool.request().query('SELECT COUNT(*) AS cnt FROM ' + step.tgtTable);
    rowsBefore = cr.recordset?.[0]?.cnt ?? null;
  } catch (e) { log.push('rowsBefore unavailable: ' + e.message); }

  const res = await tgtPool.request().query(insertSQL);

  try {
    const cr2 = await tgtPool.request().query('SELECT COUNT(*) AS cnt FROM ' + step.tgtTable);
    rowsAfter = cr2.recordset?.[0]?.cnt ?? null;
  } catch (e) { log.push('rowsAfter unavailable: ' + e.message); }

  const scriptCount = res.recordset?.[0]?.migrated_rows ?? null;
  const rowsAffSum = Array.isArray(res.rowsAffected) ? res.rowsAffected.reduce((a,b)=>a+(b||0),0) : (res.rowsAffected || 0);

  let affected;
  if (rowsBefore != null && rowsAfter != null) {
    affected = Math.max(0, rowsAfter - rowsBefore);
    log.push('Target row count: ' + rowsBefore.toLocaleString() + ' → ' + rowsAfter.toLocaleString());
  } else if (typeof scriptCount === 'number') {
    affected = scriptCount;
    log.push('migrated_rows from script: ' + scriptCount);
  } else {
    affected = rowsAffSum;
  }

  return {
    status: 'success', connOn: 'target',
    rowsAffected: affected, rowsBefore, rowsAfter,
    log,
  };
}

async function execMigrationPaginated(step, srcPool, tgtPool, log) {
  const PAGE_SIZE    = 500;
  const INSERT_BATCH = 100;

  // Build source SELECT honouring srcWhere
  let srcSelectSQL = 'SELECT * FROM ' + step.srcTable;
  const wh = (step.srcWhere || '').trim().replace(/^WHERE\s+/i, '');
  if (wh) { srcSelectSQL += ' WHERE ' + wh; log.push('WHERE: ' + wh); }

  // Source row count (subquery-wrapped to permit ORDER BY in source SQL)
  let totalRows = 0;
  try {
    const innerSql = srcSelectSQL.replace(/;\s*$/, '');
    const countSql = 'SELECT COUNT(*) AS cnt FROM (' + innerSql + ') AS _src_count_wrap';
    const cr = await srcPool.request().query(countSql);
    totalRows = Number(cr.recordset?.[0]?.cnt || 0);
    log.push('Source rows: ' + totalRows.toLocaleString());
  } catch (e) {
    log.push('Could not count source rows: ' + e.message);
  }
  if (!totalRows && step.totalRows) {
    totalRows = step.totalRows;
    log.push('Using saved estimate: ' + totalRows.toLocaleString());
  }

  // Target rowsBefore for delta-based rowsAffected
  let rowsBefore = null;
  try {
    const cr = await tgtPool.request().query('SELECT COUNT(*) AS cnt FROM ' + step.tgtTable);
    rowsBefore = cr.recordset?.[0]?.cnt ?? null;
  } catch (e) { log.push('rowsBefore unavailable: ' + e.message); }

  // Identity columns on target — exclude from INSERT
  let identityCols = new Set();
  try {
    identityCols = await getIdentityColumns(tgtPool, step.tgtTable);
    if (identityCols.size) log.push('Identity columns excluded: ' + Array.from(identityCols).join(', '));
  } catch (e) { log.push('Identity detection failed: ' + e.message); }

  // Mapping: use saved, or auto-build by name from first page
  let mapping = (step.columnMapping || []).filter(m => m && m.tgtCol);
  let offset = 0, pageNum = 0, totalInserted = 0;
  let batchErrors = 0, firstBatchError = null;
  let hasMore = true;

  while (hasMore) {
    pageNum++;
    const page = await fetchPage(srcPool, srcSelectSQL, offset, PAGE_SIZE);
    if (!page.rows.length) break;
    hasMore = page.hasMore;

    // Build mapping on first page if not provided
    if (offset === 0 && !mapping.length) {
      mapping = Object.keys(page.rows[0]).map(c => ({ srcCol: c, tgtCol: c, transform: 'NONE' }));
      log.push('Auto-mapping ' + mapping.length + ' columns by name');
    }

    // Slice page into INSERT batches
    for (let i = 0; i < page.rows.length; i += INSERT_BATCH) {
      const slice = page.rows.slice(i, i + INSERT_BATCH);
      const insertSql = buildBatchInsert(step.tgtTable, mapping, slice, step, identityCols);
      if (!insertSql) continue;
      try {
        const r = await tgtPool.request().query(insertSql);
        const inserted = Array.isArray(r.rowsAffected) ? r.rowsAffected.reduce((a,b)=>a+(b||0),0) : (r.rowsAffected || 0);
        totalInserted += inserted;
      } catch (e) {
        batchErrors++;
        if (!firstBatchError) firstBatchError = String(e.message || e);
      }
    }

    const pct = totalRows > 0 ? ' (' + Math.round(totalInserted / totalRows * 100) + '%)' : '';
    log.push('Page ' + pageNum + ': fetched ' + page.rows.length + ' rows, inserted ' + totalInserted.toLocaleString() + pct);
    offset += page.rows.length;
  }

  // rowsAfter for delta
  let rowsAfter = null;
  try {
    const cr = await tgtPool.request().query('SELECT COUNT(*) AS cnt FROM ' + step.tgtTable);
    rowsAfter = cr.recordset?.[0]?.cnt ?? null;
  } catch {}

  const status = batchErrors ? 'failed' : 'success';
  return {
    status, connOn: 'target',  // composite: SELECT on src, INSERT on tgt; reported as target for the batch errors
    rowsAffected: totalInserted,
    rowsBefore, rowsAfter,
    batchErrorCount: batchErrors,
    errorMessage: firstBatchError || null,
    pagesProcessed: pageNum,
    log,
  };
}

// ── Top-level entry ────────────────────────────────────────────────────────
exports.handler = async (event) => {
  // Background functions only need to return a quick acknowledgement to
  // Netlify; the actual work continues until exit. Errors here MUST be
  // persisted to Cosmos because the caller can't see them.
  const body = JSON.parse(event.body || '{}');
  const { runId, scheduleId, userId } = body;
  if (!runId || !scheduleId || !userId) {
    console.error('Background runner: missing runId/scheduleId/userId', body);
    return { statusCode: 400, body: 'missing parameters' };
  }

  let containers;
  try { containers = getContainers(); }
  catch (e) {
    console.error('Cosmos init failed:', e.message);
    return { statusCode: 500, body: e.message };
  }

  // Load the existing run record so we can update it on completion.
  let run, schedule;
  try {
    const { resource } = await containers.runs.item(runId, scheduleId).read();
    run = resource;
  } catch (e) {
    console.error('Could not read run record:', e.message);
    return { statusCode: 500, body: 'run not found' };
  }
  try {
    const { resource } = await containers.schedules.item(scheduleId, userId).read();
    schedule = resource;
  } catch (e) {
    await markRunFailed(containers, run, 'Could not load schedule: ' + e.message);
    return { statusCode: 500, body: 'schedule missing' };
  }

  // Resolve what to execute (same logic as the sync runNow did).
  let version;
  try {
    const { resource } = await containers.runs.database.client
      .database(process.env.COSMOS_DATABASE || 'cygenix')
      .container('job_versions')
      .item(schedule.jobVersionId, schedule.jobId).read();
    version = resource;
  } catch (e) {
    await markRunFailed(containers, run, 'Could not load pinned version: ' + e.message);
    return { statusCode: 500, body: 'version missing' };
  }
  if (!version || !version.snapshot) {
    await markRunFailed(containers, run, 'Pinned version snapshot missing');
    return { statusCode: 500, body: 'snapshot missing' };
  }

  const snap = version.snapshot;
  const stepsToRun = Array.isArray(snap.childSteps) && snap.childSteps.length
    ? snap.childSteps
    : (snap.sql ? [{ sql: snap.sql, jobType: snap.jobType || 'sql' }] : []);
  if (!stepsToRun.length) {
    await markRunFailed(containers, run, 'No SQL to execute on the pinned version');
    return { statusCode: 400, body: 'no steps' };
  }

  // Connection validation
  const srcConn = schedule.srcConn || '';
  const tgtConn = schedule.tgtConn || '';
  if (!tgtConn || !isMssqlConn(tgtConn)) {
    await markRunFailed(containers, run, 'Target must be a mssql:// connection');
    return { statusCode: 400, body: 'bad target' };
  }
  // Source is only required if any step uses connOn=source OR is a migration step
  const needsSrc = stepsToRun.some(s =>
    s.connOn === 'source' ||
    s.srcTable ||
    (s.jobType && s.jobType !== 'sql' && s.jobType !== 'sql-script')
  );
  if (needsSrc && (!srcConn || !isMssqlConn(srcConn))) {
    await markRunFailed(containers, run, 'Source mssql:// connection required for migration steps');
    return { statusCode: 400, body: 'bad source' };
  }

  // Open pools lazily
  let srcPool = null, tgtPool = null;
  const ensureSrc = async () => srcPool || (srcPool = await openPool(srcConn));
  const ensureTgt = async () => tgtPool || (tgtPool = await openPool(tgtConn));

  // Mark run as 'running' (it was 'queued' from the sync handler)
  run.status = 'running';
  run.runnerStartedAt = new Date().toISOString();
  try { await containers.runs.item(run.id, run.scheduleId).replace(run); } catch {}

  // Execute steps
  const stepResults = [];
  let totalRows = 0;
  let firstFailure = null;
  try {
    for (let i = 0; i < stepsToRun.length; i++) {
      const step = stepsToRun[i];
      const stepLabel = step.tgtTable || step.name || ('step ' + (i + 1));
      const stepLog = [];
      const stepStart = Date.now();
      try {
        // Dispatch: SQL-script step vs migration step
        const isSqlStep =
          step.jobType === 'sql' || step.jobType === 'sql-script' || step.type === 'sql' ||
          (!step.srcTable && (step.sql || step.insertSQL));
        const r = isSqlStep
          ? await execSqlStep(step, ensureSrc, ensureTgt, stepLog)
          : await execMigrationStep(step, ensureSrc, ensureTgt, stepLog);

        stepResults.push({
          index: i, label: stepLabel,
          status: r.status,
          connOn: r.connOn || (step.connOn === 'source' ? 'source' : 'target'),
          rowsAffected: r.rowsAffected || 0,
          rowsBefore:   r.rowsBefore ?? null,
          rowsAfter:    r.rowsAfter  ?? null,
          batchErrorCount: r.batchErrorCount || 0,
          pagesProcessed: r.pagesProcessed || null,
          durationMs: Date.now() - stepStart,
          reason: r.reason || null,
          errorMessage: r.errorMessage || null,
          log: r.log || stepLog,
        });
        totalRows += (r.rowsAffected || 0);
        if (r.status === 'failed') {
          firstFailure = r.errorMessage || ('Step ' + i + ' failed');
          break;
        }
      } catch (stepErr) {
        stepResults.push({
          index: i, label: stepLabel, status: 'failed',
          connOn: step.connOn === 'source' ? 'source' : 'target',
          durationMs: Date.now() - stepStart,
          errorMessage: String(stepErr.message || stepErr),
          log: stepLog,
        });
        firstFailure = String(stepErr.message || stepErr);
        break;
      }
    }
  } finally {
    if (srcPool) { try { await srcPool.close(); } catch {} }
    if (tgtPool) { try { await tgtPool.close(); } catch {} }
  }

  // Finalise run record
  const finishedAt = new Date().toISOString();
  const finalStatus = firstFailure ? 'failed' : 'success';
  const finalRun = {
    ...run,
    status:       finalStatus,
    finishedAt,
    rowsAffected: totalRows,
    errorMessage: firstFailure,
    stepResults,
  };
  await containers.runs.item(run.id, run.scheduleId).replace(finalRun);

  // Update the schedule's lastRunAt / lastRunStatus / nextRunAt
  try {
    const { resource: sched } = await containers.schedules.item(scheduleId, userId).read();
    if (sched) {
      const nextRunAt = sched.cron ? computeNextRun(sched.cron, new Date()) : null;
      const updated = {
        ...sched,
        lastRunAt: finishedAt,
        lastRunStatus: finalStatus,
        nextRunAt,
        updatedAt: new Date().toISOString(),
      };
      await containers.schedules.item(scheduleId, userId).replace(updated);
    }
  } catch (e) {
    console.error('Schedule update failed:', e.message);
  }

  return { statusCode: 200, body: 'done' };
};

async function openPool(connStr) {
  // Create a discrete pool (not the global default) so src and tgt can
  // coexist in the same lambda invocation.
  const pool = new sql.ConnectionPool(parseMssqlUrl(connStr));
  await pool.connect();
  return pool;
}

async function markRunFailed(containers, run, errorMessage) {
  try {
    const updated = {
      ...run,
      status: 'failed',
      finishedAt: new Date().toISOString(),
      errorMessage,
    };
    await containers.runs.item(run.id, run.scheduleId).replace(updated);
  } catch (e) {
    console.error('Could not mark run failed:', e.message);
  }
}

// Local copy of cron next-run computation (duplicated from scheduler.js so
// this function has no dependency on the sync function).
function computeNextRun(cronExpr, fromDate) {
  try {
    const parts = String(cronExpr || '').trim().split(/\s+/);
    if (parts.length !== 5) return null;
    const [miStr, hStr, domStr, moStr, dowStr] = parts;
    const parseField = (s, min, max) => {
      if (s === '*') return null;
      const m = /^\*\/(\d+)$/.exec(s);
      if (m) {
        const step = parseInt(m[1], 10);
        if (!step) return null;
        const out = [];
        for (let v = min; v <= max; v += step) out.push(v);
        return out;
      }
      if (/^\d+$/.test(s)) {
        const v = parseInt(s, 10);
        if (v < min || v > max) return null;
        return [v];
      }
      return null;
    };
    const mins  = parseField(miStr, 0, 59);
    const hours = parseField(hStr, 0, 23);
    const doms  = parseField(domStr, 1, 31);
    const mos   = parseField(moStr, 1, 12);
    const dows  = parseField(dowStr, 0, 6);
    const start = new Date(fromDate.getTime() + 60_000);
    start.setSeconds(0, 0);
    const limit = new Date(start.getTime() + 60 * 24 * 60 * 60_000);
    for (let t = start; t < limit; t = new Date(t.getTime() + 60_000)) {
      if (mins  && !mins.includes(t.getMinutes())) continue;
      if (hours && !hours.includes(t.getHours())) continue;
      if (doms  && !doms.includes(t.getDate())) continue;
      if (mos   && !mos.includes(t.getMonth() + 1)) continue;
      if (dows  && !dows.includes(t.getDay())) continue;
      return t.toISOString();
    }
    return null;
  } catch { return null; }
}
