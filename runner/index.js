// runner/index.js
//
// Cygenix migration runner — standalone Node script for Azure Container Instances.
//
// Replaces netlify/functions/scheduler-run-background.js for actually executing
// the migration work. Same logic, same Cosmos containers, same report shape —
// the only difference is the container has no 15-minute cap, so multi-hour
// conversions complete in one execution instead of needing checkpoint/resume.
//
// ── Triggered by ──────────────────────────────────────────────────────────
// scheduler.js's runNow (manual ▶ Run) and scheduled-runner.js (cron) call
// the Azure Container Instances REST API to start a container with these
// environment variables:
//
//   RUN_ID         — the queued run record's id in the `runs` Cosmos container
//   SCHEDULE_ID    — partition key of the runs container; identifies the schedule
//   USER_ID        — partition key of the schedules container; the owner email
//
// Plus Cosmos credentials:
//
//   COSMOS_CONNECTION_STRING
//   COSMOS_DATABASE       (defaults to "cygenix")
//
// The container reads the queued run, executes, writes results back to
// Cosmos (run record, schedule record, conversion report), exits.
// The Netlify dashboard polls get-run for status — invisible to it whether
// the runner was Netlify or ACI.
//
// ── What this runner DOES ─────────────────────────────────────────────────
// Same as the Netlify background function it replaces:
//   - SQL-script steps (jobType 'sql' or step with only top-level .sql):
//     execute as one statement on source or target per step.connOn.
//   - Migration steps (simple-map / mapping-based): paginated SELECT from
//     source, per-row transforms (TRIM/UPPER/LOWER/CAST), per-row Was/Is,
//     fixed/literal values, char truncation, identity-column detection,
//     batched INSERTs to target.
//   - Conversion report autosaves to project_reports container — same
//     shape as manual saves, so the Reports view renders them identically.
//
// ── What this runner DOES NOT do (deliberately) ───────────────────────────
// Same exclusions as the Netlify background function:
//   - dm_staging schema / rollback support (manual ProjectBuilder has it;
//     unattended runs don't need it).
//   - Reconciliation
//   - One-to-many cursor SQL
//
// ── Why a separate file from scheduler-run-background.js ──────────────────
// Different runtime context. The Netlify background function lives inside
// netlify/functions/ and uses the Netlify handler signature. This runner
// is a standalone Node script with a main() function, runs in a container,
// terminates on completion. We could share code via a /lib directory
// imported by both, but the duplication is small (~900 lines) and keeping
// each runtime self-contained means a change to one doesn't risk breaking
// the other unexpectedly.
//
// IMPORTANT: if you change migration logic in netlify/functions/
// scheduler-run-background.js, mirror the change here. Both runtimes
// share the same Cosmos containers and produce reports the same UI
// renders.

const { CosmosClient } = require('@azure/cosmos');
const sql = require('mssql');

// ── Cosmos client (process-scoped) ─────────────────────────────────────────
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
    // ACI has no 15-min cap so we can be generous on per-query timeout.
    // Single statements that legitimately take more than an hour are rare
    // even for big migrations — but we set it to 4h to handle the worst
    // honest cases.
    requestTimeout:    14_400_000,
    connectionTimeout: 30_000,
  };
}
const isHttpUrl   = s => /^https?:\/\//i.test(s || '');
const isMssqlConn = s => /^mssql:\/\//i.test(s || '');

// ── Per-row transform helpers (ported verbatim from scheduler-run-background) ──
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
  if (v instanceof Date)      return "'" + v.toISOString() + "'";
  const s = String(v);
  if (/^-?\d+(\.\d+)?$/.test(s)) return s;
  if (isSqlExpression(s))        return s;
  return "N'" + s.replace(/'/g, "''") + "'";
}
function applyMappingTransform(m, v) {
  if (!m) return v;
  if (m.literalValue != null && m.literalValue !== '') return m.literalValue;
  if (m.fixedValue   != null && m.fixedValue   !== '') return m.fixedValue;
  let out = v;
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
  if (out != null) {
    const t = String(m.transform || 'NONE').toUpperCase();
    if (t === 'TRIM' || t === 'UPPER' || t === 'LOWER') {
      const s = String(out);
      if (t === 'TRIM')  out = s.trim();
      if (t === 'UPPER') out = s.toUpperCase();
      if (t === 'LOWER') out = s.toLowerCase();
    }
  }
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
async function getIdentityColumns(pool, fullTableName) {
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

// ── Page-fetch ─────────────────────────────────────────────────────────────
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
function buildBatchInsert(tgtTable, mapping, batch, step, identityCols) {
  if (!batch.length) return null;
  const useMapping = mapping.filter(m =>
    m && m.tgtCol && !identityCols.has(String(m.tgtCol).toLowerCase())
  );
  if (!useMapping.length) return null;

  const colList = useMapping.map(m => '[' + m.tgtCol + ']').join(', ');
  const valueRows = batch.map(row => {
    const vals = useMapping.map(m => {
      let v = (m.srcCol && Object.prototype.hasOwnProperty.call(row, m.srcCol))
        ? row[m.srcCol]
        : (m.srcCol ? null : undefined);
      v = applyMappingTransform(m, v);
      v = applyStepWasis(step, m.srcCol, v);
      return fmtVal(v);
    });
    return '(' + vals.join(', ') + ')';
  });

  return 'INSERT INTO ' + tgtTable +
         ' (' + colList + ')\nVALUES\n' + valueRows.join(',\n');
}

// ── Same-connection check ──────────────────────────────────────────────────
function sameConnDirective(a, b) {
  if (!a || !b || !a.config || !b.config) return false;
  return a.config.server === b.config.server
      && a.config.port   === b.config.port
      && a.config.database === b.config.database;
}

// ── Step executors ─────────────────────────────────────────────────────────
async function execSqlStep(step, ensureSrcPool, ensureTgtPool, log) {
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
  if (step.jobType === 'one-to-many' || step.oneToManyConfig) {
    return { status: 'skipped', reason: 'one-to-many migrations not supported in scheduled runs yet — run from Project Builder', log: [] };
  }
  if (step.reconciliation) {
    log.push('Note: reconciliation config present; skipped in scheduled-run mode (no rollback context).');
  }
  if (!step.srcTable) {
    return { status: 'failed', errorMessage: 'srcTable not set on step', log };
  }

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

async function execMigrationSingleShot(step, tgtPool, log) {
  const insertSQL = step.insertSQL;
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

  let srcSelectSQL = 'SELECT * FROM ' + step.srcTable;
  const wh = (step.srcWhere || '').trim().replace(/^WHERE\s+/i, '');
  if (wh) { srcSelectSQL += ' WHERE ' + wh; log.push('WHERE: ' + wh); }

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

  let rowsBefore = null;
  try {
    const cr = await tgtPool.request().query('SELECT COUNT(*) AS cnt FROM ' + step.tgtTable);
    rowsBefore = cr.recordset?.[0]?.cnt ?? null;
  } catch (e) { log.push('rowsBefore unavailable: ' + e.message); }

  let identityCols = new Set();
  try {
    identityCols = await getIdentityColumns(tgtPool, step.tgtTable);
    if (identityCols.size) log.push('Identity columns excluded: ' + Array.from(identityCols).join(', '));
  } catch (e) { log.push('Identity detection failed: ' + e.message); }

  let mapping = (step.columnMapping || []).filter(m => m && m.tgtCol);
  let offset = 0, pageNum = 0, totalInserted = 0;
  let batchErrors = 0, firstBatchError = null;
  let hasMore = true;

  while (hasMore) {
    pageNum++;
    const page = await fetchPage(srcPool, srcSelectSQL, offset, PAGE_SIZE);
    if (!page.rows.length) break;
    hasMore = page.hasMore;

    if (offset === 0 && !mapping.length) {
      mapping = Object.keys(page.rows[0]).map(c => ({ srcCol: c, tgtCol: c, transform: 'NONE' }));
      log.push('Auto-mapping ' + mapping.length + ' columns by name');
    }

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

  let rowsAfter = null;
  try {
    const cr = await tgtPool.request().query('SELECT COUNT(*) AS cnt FROM ' + step.tgtTable);
    rowsAfter = cr.recordset?.[0]?.cnt ?? null;
  } catch {}

  const status = batchErrors ? 'failed' : 'success';
  return {
    status, connOn: 'target',
    rowsAffected: totalInserted,
    rowsBefore, rowsAfter,
    batchErrorCount: batchErrors,
    errorMessage: firstBatchError || null,
    pagesProcessed: pageNum,
    log,
  };
}

// ── Open mssql pool ────────────────────────────────────────────────────────
async function openPool(connStr) {
  const pool = new sql.ConnectionPool(parseMssqlUrl(connStr));
  await pool.connect();
  return pool;
}

// ── Server-side conversion report builder ──────────────────────────────────
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

function extractAtAtTokens(s) {
  if (typeof s !== 'string') return [];
  const out = [];
  const re = /@@([A-Za-z_][A-Za-z_0-9]*)/g;
  let m;
  while ((m = re.exec(s)) !== null) {
    if (!out.includes(m[1])) out.push(m[1]);
  }
  return out;
}

function buildScheduledReport({ schedule, runRecord, snapshot, userEmail, userName }) {
  const now = new Date().toISOString();
  const childSteps = Array.isArray(snapshot.childSteps) ? snapshot.childSteps : [];

  const stepResultsByIndex = new Map();
  (runRecord.stepResults || []).forEach(sr => stepResultsByIndex.set(sr.index, sr));

  const stepStatusMap = { success: 'passed', failed: 'failed', skipped: 'skipped' };
  const mergedSteps = childSteps.map((step, i) => {
    const sr = stepResultsByIndex.get(i) || { status: 'skipped' };
    const isMig = (step.jobType === 'migration' || step.type === 'migration' || step.srcTable);
    return {
      jobId:        step.jobId || '',
      name:         step.name || step.tgtTable || ('step ' + (i + 1)),
      type:         isMig ? 'migration' : 'sql',
      status:       stepStatusMap[sr.status] || sr.status || 'skipped',
      log:          Array.isArray(sr.log) ? sr.log.join('\n') : (sr.log || ''),
      srcTable:     step.srcTable || '',
      tgtTable:     step.tgtTable || '',
      rowsInserted: sr.rowsAffected || 0,
      rowsBefore:   sr.rowsBefore  ?? undefined,
      rowsAfter:    sr.rowsAfter   ?? undefined,
      rowsStaged:   undefined,
      rowsExcluded: undefined,
      excludedCapture: undefined,
      stagingTable: '',
      runId:        '',
      srcWhere:     step.srcWhere || '',
      startedAt:    sr.startedAt   || null,
      finishedAt:   sr.finishedAt  || null,
      durationMs:   typeof sr.durationMs === 'number' ? sr.durationMs : null,
      connOn:       sr.connOn || step.connOn || 'target',
      reconResult:  null,
    };
  });

  const migrationSteps = mergedSteps.filter(s => s.type === 'migration');
  const totalRows      = mergedSteps.reduce((sum, s) => sum + (s.rowsInserted || 0), 0);
  const failed         = mergedSteps.filter(s => s.status === 'failed').length;
  const passed         = mergedSteps.filter(s => s.status === 'passed').length;

  // Per-target-table breakdown (matches manual report's `tables` field)
  const tables = childSteps.filter(s => s.srcTable).flatMap((s) => {
    const i  = childSteps.indexOf(s);
    const sr = stepResultsByIndex.get(i) || {};
    const loaded = sr.rowsAffected || 0;
    if ((s.jobType === 'one-to-many' || s.oneToManyConfig) && Array.isArray(s.tables) && s.tables.length) {
      return s.tables.map(t => ({
        name:         t.name || t.fullName || '',
        sourceRows:   t.rows || 0,
        insertedRows: t.rows || 0,
        errors:       sr.status === 'failed' ? 1 : 0,
        status:       sr.status === 'success' ? 'success' : (sr.status || 'unknown'),
      }));
    }
    return [{
      name:         s.tgtTable || s.name || '',
      sourceRows:   loaded,
      insertedRows: loaded,
      errors:       sr.batchErrorCount || (sr.status === 'failed' ? 1 : 0),
      excludedRows: 0,
      excludedKnown: false,
      status:       sr.status === 'success' ? 'success'
                  : sr.status === 'skipped' ? 'skipped'
                  : sr.status === 'failed'  ? 'failed'
                  : 'unknown',
    }];
  });

  const truncationWarnings = childSteps.flatMap(s =>
    (Array.isArray(s.warnings) ? s.warnings : []).filter(w =>
      typeof w === 'string' && w.startsWith('TRUNCATION:')
    )
  );

  const allMappings = [];
  childSteps.forEach(s => {
    if (!s.srcTable) return;
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

  const paramUsageRecords = [];
  const paramUsageSeen    = new Set();
  const paramUsageNames   = new Set();
  const addParamUsage = (jobId, jobName, srcTable, tgtTable, srcCol, tgtCol, source, expression, token) => {
    const key = (jobId || '') + '|' + (tgtCol || '') + '|' + (source || '') + '|' + token + '|' + (expression || '');
    if (paramUsageSeen.has(key)) return;
    paramUsageSeen.add(key);
    paramUsageNames.add(token);
    let resolved = null;
    if (Array.isArray(snapshot._substitutedTokens)) {
      const hit = snapshot._substitutedTokens.find(t => {
        const cleanToken = String(t.token || '').replace(/^@@/, '').replace(/[{}]/g, '');
        return cleanToken === token;
      });
      if (hit) resolved = hit.resolved;
    }
    paramUsageRecords.push({
      paramName:  token,
      paramToken: '@@' + token,
      resolved,
      jobId, jobName,
      srcTable: srcTable || '',
      tgtTable: tgtTable || '',
      srcCol:   srcCol   || '',
      tgtCol:   tgtCol   || '',
      source, expression,
    });
  };
  childSteps.forEach(s => {
    if (!s.srcTable) return;
    const jobId    = s.jobId || '';
    const jobName  = s.name || s.jobId || '(unnamed)';
    const srcTable = s.srcTable || '';
    const tgtTable = s.tgtTable || '';
    (Array.isArray(s.columnMapping) ? s.columnMapping : []).forEach(m => {
      if (!m) return;
      const srcCol = m.srcCol || '';
      const tgtCol = m.tgtCol || '';
      ['literalValue', 'fixedValue'].forEach(fieldName => {
        const v = m[fieldName];
        if (typeof v !== 'string' || !v.includes('@@')) return;
        const tokens = extractAtAtTokens(v);
        tokens.forEach(tok => {
          addParamUsage(jobId, jobName, srcTable, tgtTable, srcCol, tgtCol,
            fieldName === 'literalValue' ? 'literal value' : 'fixed value',
            v, tok);
        });
      });
      (Array.isArray(m.wasisRules) ? m.wasisRules : []).forEach(r => {
        if (!r) return;
        const newVal = r.newVal ?? r.is ?? r.to;
        if (typeof newVal !== 'string' || !newVal.includes('@@')) return;
        const oldVal = r.oldVal ?? r.was ?? r.from ?? '';
        const tokens = extractAtAtTokens(newVal);
        tokens.forEach(tok => {
          addParamUsage(jobId, jobName, srcTable, tgtTable, srcCol, tgtCol,
            'Was/Is rule', String(oldVal) + ' → ' + newVal, tok);
        });
      });
    });
  });
  const paramUsageCount      = paramUsageRecords.length;
  const paramUsageParamCount = paramUsageNames.size;

  const srcDesc = parseDbConnFromMssqlUrl(schedule.srcConn || '');
  const tgtDesc = parseDbConnFromMssqlUrl(schedule.tgtConn || '');

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
    sourceFriendlyName: '',
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
    paramUsage:    paramUsageRecords,
    paramUsageCount,
    paramUsageParamCount,
    warnings: [
      ...mergedSteps.filter(s => s.status === 'failed').map(s => 'Job failed: ' + (s.name || s.type)),
    ],
    startedAt:    runRecord.startedAt || new Date(Date.now() - mergedSteps.length * 1000).toISOString(),
    completedAt:  runRecord.finishedAt || now,
    isProjectReport: true,
    steps: mergedSteps,
    tables,
    truncationWarnings,
    reconciliation: [],
    scheduledRunMeta: {
      runId:        runRecord.id,
      scheduleId:   schedule.id,
      scheduleName: schedule.name,
      triggeredBy:  runRecord.triggeredBy || 'manual',
      mode:         'scheduled',
    },
  };
}

async function saveScheduledReport(containers, schedule, runRecord, snapshot) {
  try {
    const userEmail = schedule.userId;
    const userName  = '';
    const report = buildScheduledReport({
      schedule, runRecord, snapshot, userEmail, userName,
    });
    const doc = Object.assign({}, report, {
      id:        report.id,
      userId:    schedule.userId,
      userEmail,
      userName,
      savedAt:   new Date().toISOString(),
      mode:      'scheduled',
      scheduledRunMeta: {
        runId:        runRecord.id,
        scheduleId:   schedule.id,
        scheduleName: schedule.name,
        triggeredBy:  runRecord.triggeredBy || 'manual',
      },
    });
    await containers.project_reports.items.upsert(doc);
    return { ok: true, reportId: report.id };
  } catch (e) {
    console.error('Scheduled report save failed:', e.message, e.code, e.stack);
    return { ok: false, error: (e.message || 'unknown') + (e.code ? ' (code: ' + e.code + ')' : '') };
  }
}

// ── Mark run failed helper ─────────────────────────────────────────────────
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

// ── Local copy of cron next-run computation ────────────────────────────────
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

// ── Main entry point ───────────────────────────────────────────────────────
async function main() {
  const runId      = process.env.RUN_ID;
  const scheduleId = process.env.SCHEDULE_ID;
  const userId     = process.env.USER_ID;

  if (!runId || !scheduleId || !userId) {
    console.error('[runner] missing required env vars: RUN_ID / SCHEDULE_ID / USER_ID');
    process.exit(1);
  }

  console.log('[runner] starting', { runId, scheduleId, userId });
  const startedAt = Date.now();

  let containers;
  try { containers = getContainers(); }
  catch (e) {
    console.error('[runner] Cosmos init failed:', e.message);
    process.exit(1);
  }

  // Load the queued run record
  let run;
  try {
    const { resource } = await containers.runs.item(runId, scheduleId).read();
    run = resource;
  } catch (e) {
    console.error('[runner] could not read run record:', e.message);
    process.exit(1);
  }
  if (!run) {
    console.error('[runner] run record not found:', runId);
    process.exit(1);
  }

  // Load the schedule
  let schedule;
  try {
    const { resource } = await containers.schedules.item(scheduleId, userId).read();
    schedule = resource;
  } catch (e) {
    await markRunFailed(containers, run, 'Could not load schedule: ' + e.message);
    process.exit(1);
  }
  if (!schedule) {
    await markRunFailed(containers, run, 'Schedule not found');
    process.exit(1);
  }

  // Load the pinned version snapshot
  let version;
  try {
    const { resource } = await containers.runs.database.client
      .database(process.env.COSMOS_DATABASE || 'cygenix')
      .container('job_versions')
      .item(schedule.jobVersionId, schedule.jobId).read();
    version = resource;
  } catch (e) {
    await markRunFailed(containers, run, 'Could not load pinned version: ' + e.message);
    process.exit(1);
  }
  if (!version || !version.snapshot) {
    await markRunFailed(containers, run, 'Pinned version snapshot missing');
    process.exit(1);
  }

  const snap = version.snapshot;
  const stepsToRun = Array.isArray(snap.childSteps) && snap.childSteps.length
    ? snap.childSteps
    : (snap.sql ? [{ sql: snap.sql, jobType: snap.jobType || 'sql' }] : []);
  if (!stepsToRun.length) {
    await markRunFailed(containers, run, 'No SQL to execute on the pinned version');
    process.exit(1);
  }

  // Validate connections
  const srcConn = schedule.srcConn || '';
  const tgtConn = schedule.tgtConn || '';
  if (!tgtConn || !isMssqlConn(tgtConn)) {
    await markRunFailed(containers, run, 'Target must be a mssql:// connection');
    process.exit(1);
  }
  const needsSrc = stepsToRun.some(s =>
    s.connOn === 'source' ||
    s.srcTable ||
    (s.jobType && s.jobType !== 'sql' && s.jobType !== 'sql-script')
  );
  if (needsSrc && (!srcConn || !isMssqlConn(srcConn))) {
    await markRunFailed(containers, run, 'Source mssql:// connection required for migration steps');
    process.exit(1);
  }

  // Open pools lazily
  let srcPool = null, tgtPool = null;
  const ensureSrc = async () => srcPool || (srcPool = await openPool(srcConn));
  const ensureTgt = async () => tgtPool || (tgtPool = await openPool(tgtConn));

  // Mark run as 'running'
  run.status = 'running';
  run.runnerStartedAt = new Date().toISOString();
  run.runnerHost = 'aci';   // distinguishes ACI vs Netlify background runs
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
      const stepStart  = Date.now();
      const startedAtIso = new Date(stepStart).toISOString();
      try {
        const isSqlStep =
          step.jobType === 'sql' || step.jobType === 'sql-script' || step.type === 'sql' ||
          (!step.srcTable && (step.sql || step.insertSQL));
        const r = isSqlStep
          ? await execSqlStep(step, ensureSrc, ensureTgt, stepLog)
          : await execMigrationStep(step, ensureSrc, ensureTgt, stepLog);
        const stepEnd = Date.now();
        const finishedAtIso = new Date(stepEnd).toISOString();

        stepResults.push({
          index: i, label: stepLabel,
          status: r.status,
          connOn: r.connOn || (step.connOn === 'source' ? 'source' : 'target'),
          rowsAffected: r.rowsAffected || 0,
          rowsBefore:   r.rowsBefore ?? null,
          rowsAfter:    r.rowsAfter  ?? null,
          batchErrorCount: r.batchErrorCount || 0,
          pagesProcessed: r.pagesProcessed || null,
          startedAt: startedAtIso,
          finishedAt: finishedAtIso,
          durationMs: stepEnd - stepStart,
          reason: r.reason || null,
          errorMessage: r.errorMessage || null,
          log: r.log || stepLog,
        });
        totalRows += (r.rowsAffected || 0);

        // Checkpoint after each step so the dashboard sees progress as it
        // happens. The poll loop on ta_runNow updates its label from this.
        try {
          run.stepResults = stepResults;
          run.rowsAffected = totalRows;
          await containers.runs.item(run.id, run.scheduleId).replace(run);
        } catch (e) { console.warn('[runner] checkpoint write failed:', e.message); }

        if (r.status === 'failed') {
          firstFailure = r.errorMessage || ('Step ' + i + ' failed');
          break;
        }
      } catch (stepErr) {
        const stepEnd = Date.now();
        stepResults.push({
          index: i, label: stepLabel, status: 'failed',
          connOn: step.connOn === 'source' ? 'source' : 'target',
          startedAt: startedAtIso,
          finishedAt: new Date(stepEnd).toISOString(),
          durationMs: stepEnd - stepStart,
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

  // Finalise the run record
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
    console.error('[runner] schedule update failed:', e.message);
  }

  // Save the conversion report
  try {
    const reportResult = await saveScheduledReport(containers, schedule, finalRun, snap);
    try {
      if (reportResult.ok) {
        finalRun.reportId = reportResult.reportId;
        finalRun.reportSaveError = null;
      } else {
        finalRun.reportId = null;
        finalRun.reportSaveError = reportResult.error || 'unknown';
      }
      await containers.runs.item(finalRun.id, finalRun.scheduleId).replace(finalRun);
    } catch (e) { console.error('[runner] run record reportId stamp failed:', e.message); }
  } catch (e) {
    console.error('[runner] report save flow failed:', e.message);
  }

  const elapsed = Math.round((Date.now() - startedAt) / 1000);
  console.log('[runner] done', { runId, status: finalStatus, rowsAffected: totalRows, elapsedSec: elapsed });
  process.exit(finalStatus === 'success' ? 0 : 1);
}

main().catch(async (e) => {
  console.error('[runner] uncaught error:', e.message, e.stack);
  // Best-effort: try to mark the run failed so the dashboard sees a stable state
  try {
    const containers = getContainers();
    const runId = process.env.RUN_ID;
    const scheduleId = process.env.SCHEDULE_ID;
    if (runId && scheduleId) {
      const { resource: run } = await containers.runs.item(runId, scheduleId).read();
      if (run) await markRunFailed(containers, run, 'Runner crashed: ' + e.message);
    }
  } catch {}
  process.exit(1);
});
