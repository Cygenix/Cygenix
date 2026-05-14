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
    schedules:       db.container('schedules'),
    runs:            db.container('runs'),
    project_reports: db.container('project_reports'),
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

  // ── Conversion report ─────────────────────────────────────────────────
  // Build and persist a conversion report into project_reports — the same
  // Cosmos container the manual "💾 Save Report" button writes to. After
  // this, the Reports view shows the scheduled run alongside manual runs
  // without distinction. Wrapped in try/catch because a report-save
  // failure must not retroactively fail a run that did its actual work
  // correctly.
  try {
    const reportResult = await saveScheduledReport(containers, schedule, finalRun, snap);
    try {
      if (reportResult.ok) {
        finalRun.reportId = reportResult.reportId;
        finalRun.reportSaveError = null;
      } else {
        // Don't stamp a fake reportId if the save genuinely failed —
        // that's what bit us before. Record the error instead so the
        // dashboard can surface it ("Run succeeded but report save failed").
        finalRun.reportId = null;
        finalRun.reportSaveError = reportResult.error || 'unknown';
      }
      await containers.runs.item(finalRun.id, finalRun.scheduleId).replace(finalRun);
    } catch (e) { console.error('Run record reportId stamp failed:', e.message); }
  } catch (e) {
    console.error('Report save flow failed:', e.message);
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

// ── Server-side conversion report ─────────────────────────────────────────
// Port of project-builder.html's buildProjectReport, adapted for the
// server-side context: no `project` global, no localStorage, no
// CygenixParams. Reads everything it needs from (a) the pinned snapshot
// (which already contains the column mappings, generated SQL, projectName,
// connections) and (b) the run record we just finalised (which has the
// per-step status, rowsAffected, durations, error messages).
//
// Same Cosmos container as the manual save (project_reports, partition
// key /userId) and the same document shape, so /reports renders scheduled
// reports identically to manual ones. The only fields that come out
// emptier than a manual save are:
//   - sourceFriendlyName / targetFriendlyName — these are looked up from
//     localStorage.cygenix_saved_connections client-side; the server has
//     no equivalent. The report viewer falls back to server/database
//     identifiers gracefully when these are empty.
//   - Was/Is "Path 3" cross-reference to localStorage.cygenix_wasis_rules
//     — this is browser-only. Path 1 (per-column attribution) and Path 2
//     (CASE WHEN regex parsing of insertSQL) still run and cover the
//     common case where rules are recorded on the columnMapping itself.
//
// IMPORTANT: if you change the shape of buildProjectReport in
// project-builder.html, mirror the change here. Both writers feed the
// same Cosmos container and the report viewer expects one consistent
// shape across both.

function parseDbConnFromMssqlUrl(connStr) {
  try {
    if (!connStr || !/^mssql:\/\//i.test(connStr)) return null;
    const u = new URL(connStr);
    return {
      server:   u.hostname || '',
      database: (u.pathname || '').replace(/^\//, ''),
    };
  } catch { return null; }
}

function buildScheduledReport({ schedule, runRecord, snapshot, userEmail, userName }) {
  const now = new Date().toISOString();
  const childSteps = Array.isArray(snapshot.childSteps) ? snapshot.childSteps : [];

  // Marry each snapshot step with its corresponding runRecord stepResult,
  // by index. The runRecord may have fewer entries than the snapshot if
  // execution stopped early — that's fine; missing entries become
  // status:'skipped'.
  const stepResultsByIndex = new Map();
  (runRecord.stepResults || []).forEach(sr => stepResultsByIndex.set(sr.index, sr));

  // ── Merge snapshot + runRecord into the report's "steps" array ────────
  // Translate scheduled-run status names to manual-run status names so the
  // report viewer treats both identically:
  //   success → passed, failed → failed, skipped → skipped
  const stepStatusMap = { success: 'passed', failed: 'failed', skipped: 'skipped' };
  const mergedSteps = childSteps.map((step, i) => {
    const sr = stepResultsByIndex.get(i) || { status: 'skipped' };
    const isMig = (step.jobType === 'migration' || step.type === 'migration' || step.srcTable);
    return {
      jobId:        step.jobId || '',
      name:         step.name || step.tgtTable || ('step ' + (i + 1)),
      type:         isMig ? 'migration' : 'sql',
      status:       stepStatusMap[sr.status] || sr.status || 'skipped',
      // Per-step log is what the runner accumulated for this step.
      log:          Array.isArray(sr.log) ? sr.log.join('\n') : (sr.log || ''),
      srcTable:     step.srcTable || '',
      tgtTable:     step.tgtTable || '',
      rowsInserted: sr.rowsAffected || 0,
      rowsBefore:   sr.rowsBefore  ?? undefined,
      rowsAfter:    sr.rowsAfter   ?? undefined,
      rowsStaged:   undefined,    // not produced by scheduled runs
      rowsExcluded: undefined,    // not produced (no staging on scheduled runs)
      excludedCapture: undefined,
      stagingTable: '',
      runId:        '',
      srcWhere:     step.srcWhere || '',
      // Step timing
      startedAt:    sr.startedAt   || null,
      finishedAt:   sr.finishedAt  || null,
      durationMs:   typeof sr.durationMs === 'number' ? sr.durationMs : null,
      connOn:       sr.connOn || step.connOn || 'target',
      reconResult:  null,         // reconciliation skipped in scheduled-run mode
    };
  });

  const migrationSteps = mergedSteps.filter(s => s.type === 'migration');
  const totalRows      = mergedSteps.reduce((sum, s) => sum + (s.rowsInserted || 0), 0);
  const failed         = mergedSteps.filter(s => s.status === 'failed').length;
  const passed         = mergedSteps.filter(s => s.status === 'passed').length;

  // ── Expanded per-target-table breakdown ──────────────────────────────
  const expandedTables = childSteps.filter(s => s.srcTable).flatMap(s => {
    if ((s.jobType === 'one-to-many' || s.oneToManyConfig) && s.tables && s.tables.length) {
      return s.tables.map(t => ({
        name:     t.name || t.fullName || '',
        rows:     t.rows || 0,
        cols:     t.cols || 0,
        srcTable: s.srcTable,
      }));
    }
    return [{
      name:     s.tgtTable || '',
      rows:     (stepResultsByIndex.get(childSteps.indexOf(s)) || {}).rowsAffected || 0,
      cols:     (s.columnMapping || []).filter(m => m.tgtCol).length,
      srcTable: s.srcTable,
    }];
  });

  // ── Column mapping summary ───────────────────────────────────────────
  const allMappings = [];
  childSteps.forEach(s => {
    if (!s.srcTable) return;   // SQL-script steps don't have a mapping
    (s.columnMapping || []).filter(m => m.tgtCol).forEach(m => {
      allMappings.push({
        srcCol:        m.srcCol || '',
        srcTable:      s.srcTable || '',
        tgtCol:        m.tgtCol || '',
        tgtTable:      s.tgtTable || '',
        tgtType:       m.tgtType || '',
        transform:     m.transform || 'NONE',
        transformExpr: m.transformExpr || null,
        wasisRules:    m.wasisRules || null,
        wasisCount:    m.wasisCount || (Array.isArray(m.wasisRules) ? m.wasisRules.length : 0),
      });
    });
  });

  // ── Was/Is aggregation (Path 1 + Path 2 from project-builder) ────────
  // Path 3 (cross-ref against localStorage.cygenix_wasis_rules) is omitted
  // — that store is browser-only. The viewer handles missing srcField on
  // a rule gracefully.
  const wasisAggRules = [];
  const wasisSeen     = new Set();
  const wasisColSet   = new Set();
  const addRule = (srcTable, srcField, oldVal, newVal) => {
    const key = (srcTable || '') + '|' + (srcField || '') + '|' + (oldVal || '') + '|' + (newVal || '');
    if (wasisSeen.has(key)) return;
    wasisSeen.add(key);
    wasisColSet.add((srcTable || '') + '|' + (srcField || ''));
    wasisAggRules.push({
      srcField: srcField || '',
      srcTable: srcTable || '',
      oldVal:   oldVal   || '',
      newVal:   newVal   || '',
    });
  };
  childSteps.forEach(s => {
    if (!s.srcTable) return;
    // Path 1 — per-column attribution
    (s.columnMapping || []).forEach(m => {
      if (!m || !Array.isArray(m.wasisRules) || !m.wasisRules.length) return;
      m.wasisRules.forEach(r => {
        let oldVal = '', newVal = '';
        if (typeof r === 'string') {
          const parts = r.split(/\s*(?:→|->|=>)\s*/);
          oldVal = (parts[0] || '').trim();
          newVal = (parts[1] || '').trim();
        } else if (r && typeof r === 'object') {
          oldVal = r.oldVal != null ? String(r.oldVal) : '';
          newVal = r.newVal != null ? String(r.newVal) : '';
        }
        addRule(s.srcTable, m.srcCol, oldVal, newVal);
      });
    });
    // Path 2 — parse CASE WHEN blocks out of insertSQL. Matches the
    // generator's output format: `CASE WHEN [col] = N'old' THEN N'new'
    // ... END AS [tgtCol]`.
    if (s.insertSQL && typeof s.insertSQL === 'string') {
      const blockRe = /CASE\s+(?:WHEN[\s\S]+?THEN[\s\S]+?)+(?:ELSE[\s\S]+?)?END(?:\s+AS\s+\[([^\]]+)\])?/gi;
      let blockMatch;
      while ((blockMatch = blockRe.exec(s.insertSQL)) !== null) {
        const block = blockMatch[0];
        const whenRe = /WHEN\s+\[([^\]]+)\]\s*=\s*N?'((?:''|[^'])*)'\s+THEN\s+N?'((?:''|[^'])*)'/gi;
        let m2;
        while ((m2 = whenRe.exec(block)) !== null) {
          const col    = m2[1];
          const oldVal = m2[2].replace(/''/g, "'");
          const newVal = m2[3].replace(/''/g, "'");
          addRule(s.srcTable, col, oldVal, newVal);
        }
      }
    }
  });
  const wasisRuleCount = wasisAggRules.length;
  const wasisColCount  = wasisColSet.size;

  // ── Connection identifiers (server/database only — never credentials) ─
  const srcDesc = parseDbConnFromMssqlUrl(schedule.srcConn || '');
  const tgtDesc = parseDbConnFromMssqlUrl(schedule.tgtConn || '');

  // First migration step's table names (mirror of manual report)
  const firstMig = childSteps.find(s => s.srcTable);
  const srcTableName = firstMig ? (firstMig.srcTable || '') : '';
  const tgtTableName = firstMig ? (firstMig.tgtTable || '') : '';

  return {
    id:           'rpt_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8),
    projectName:  snapshot.projectName || snapshot.name || 'Scheduled Conversion',
    userName:     userName || '',
    userEmail:    userEmail || '',
    organisation: 'Cygenix',
    sourceTable:  srcTableName,
    sourceSystem: 'Microsoft SQL Server / Azure SQL',
    sourceFriendlyName: '',   // not resolvable server-side
    sourceServer:       srcDesc ? srcDesc.server   : '',
    sourceDatabase:     srcDesc ? srcDesc.database : '',
    targetTable:  tgtTableName,
    targetSystem: 'Microsoft SQL Server / Azure SQL',
    targetFriendlyName: '',
    targetServer:   tgtDesc ? tgtDesc.server   : '',
    targetDatabase: tgtDesc ? tgtDesc.database : '',
    authMethod:   'SQL Authentication',
    totalRows,
    insertedRows: totalRows,
    errors:       failed,
    rowsBefore:   0,
    rowsAfter:    totalRows,
    columnMapping: allMappings,
    columnsMapped: allMappings.length,
    wasisRules:    wasisAggRules,
    wasisRuleCount,
    wasisColCount,
    // Parameter usage: pre-resolved at phase 2a snapshot time. We surface
    // the list (with resolved values stamped on the snapshot) so the
    // report still shows which params drove the run.
    paramUsage:           Array.isArray(snapshot._substitutedTokens) ? snapshot._substitutedTokens.map(t => ({
      paramName:  t.token.replace(/^@@/, '').replace(/[{}]/g, ''),
      paramToken: t.token,
      resolved:   t.resolved,
      jobId:      '',
      jobName:    snapshot.name || '',
      srcTable:   '',
      tgtTable:   '',
      srcCol:     '',
      tgtCol:     '',
      source:     'snapshot resolution',
      expression: t.token,
    })) : [],
    paramUsageCount:      Array.isArray(snapshot._substitutedTokens) ? snapshot._substitutedTokens.length : 0,
    paramUsageParamCount: Array.isArray(snapshot._substitutedTokens)
      ? new Set(snapshot._substitutedTokens.map(t => t.token)).size
      : 0,
    warnings: [
      ...mergedSteps.filter(s => s.status === 'failed').map(s => 'Job failed: ' + (s.name || s.type)),
    ],
    startedAt:    runRecord.startedAt || new Date(Date.now() - mergedSteps.length * 1000).toISOString(),
    completedAt:  runRecord.finishedAt || now,
    isProjectReport: true,
    steps: mergedSteps,
    reconciliation: [],   // reconciliation skipped in scheduled-run mode
    // Scheduled-run provenance — extra fields not in the manual report so
    // the viewer (and you, when poking at Cosmos) can tell where this
    // report came from. The viewer ignores unknown fields.
    scheduledRunMeta: {
      runId:       runRecord.id,
      scheduleId:  schedule.id,
      scheduleName: schedule.name,
      triggeredBy: runRecord.triggeredBy || 'manual',
      mode:        'scheduled',
    },
  };
}

async function saveScheduledReport(containers, schedule, runRecord, snapshot) {
  // Build the report payload exactly as the dashboard's buildProjectReport
  // would, then write it directly to Cosmos. We bypass /api/reports because
  // that endpoint requires an Entra JWT Bearer token — which the background
  // function doesn't have. Cosmos write goes through the same container
  // (project_reports) and same partition key (/userId) as the manual save,
  // so the report viewer renders both identically.
  //
  // Document shape MUST match what reports.js's handleSave produces:
  //   Object.assign({}, report, { id, userId, userEmail, userName, savedAt })
  // i.e. report fields are SPREAD AT TOP LEVEL (not nested under .report)
  // and the timestamp field is `savedAt` (the list query orders by this).
  // Getting either wrong means the report won't render or won't appear in
  // the list — both ways have bitten us already (2026-05-14).
  try {
    const userEmail = schedule.userId;   // userId IS the email per current convention
    const userName  = '';                // server has no display-name source
    const report = buildScheduledReport({
      schedule, runRecord, snapshot, userEmail, userName,
    });
    const doc = Object.assign({}, report, {
      id:        report.id,
      userId:    schedule.userId,
      userEmail,
      userName,
      savedAt:   new Date().toISOString(),
      // Provenance — these are EXTRA fields beyond what the manual save
      // writes. The viewer ignores unknown fields, so they're safe to add.
      // Lets you tell scheduled reports from manual ones at a glance in
      // Cosmos and in the dashboard if you ever surface this in the UI.
      mode:      'scheduled',
      scheduledRunMeta: {
        runId:        runRecord.id,
        scheduleId:   schedule.id,
        scheduleName: schedule.name,
        triggeredBy:  runRecord.triggeredBy || 'manual',
      },
    });
    // Use upsert (matching handleSave). If a report with this id already
    // exists, replace it — safer than create() which throws on duplicate.
    await containers.project_reports.items.upsert(doc);
    return { ok: true, reportId: report.id };
  } catch (e) {
    // Don't fail the whole run because the report-save flopped — the run
    // record itself is the source of truth. But DO log loud enough that
    // a Netlify log inspection will catch it, and return the error so the
    // caller can stamp it on the run record for visibility.
    console.error('Scheduled report save failed:', e.message, e.code, e.stack);
    return { ok: false, error: (e.message || 'unknown') + (e.code ? ' (code: ' + e.code + ')' : '') };
  }
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
