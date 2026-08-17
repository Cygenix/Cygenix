/* cygenix-restore.js — the Restore Database module's engine.
 *
 * Puts a database BACK onto the configured target: from a .bak/.trn on
 * the target's disk, or from an Azure Blob URL. The single most important
 * design constraint (spec §3): "Azure" is not one target. SQL Server,
 * Azure SQL Managed Instance and Azure SQL Database support different
 * restore operations — Azure SQL Database cannot run RESTORE DATABASE at
 * all — so everything the UI shows is driven by a capability matrix
 * derived from the probed EngineEdition, and unsupported modes are
 * disabled WITH the reason, never hidden.
 *
 * Pure core (classification, parsers, plan validation, SQL generation,
 * error translation) — no DOM, no network — with the SQL text builders
 * used by the Connections tab UI in dashboard-app.js. Server-side, the
 * db-connect RBAC gate classifies RESTORE/BACKUP statements as
 * restore.inspect / restore.execute so the permission is enforced where
 * it counts, not by hiding buttons.
 *
 * Deliberately NOT in this version (spec §12 later steps): snapshots and
 * rollback, differential/log chain building with STOPAT, ARM point-in-
 * time restore for Azure SQL Database, and .bacpac import — the UI names
 * these when it disables them.
 */

(function (root) {
  'use strict';

  // ── Identifier / literal quoting ──────────────────────────────────────────
  // QUOTENAME semantics. A database called  x];--  must arrive as
  // [x]];--] — never allow user text to close the bracket.
  function rdQuoteName(name) { return '[' + String(name).replace(/]/g, ']]') + ']'; }
  function rdQuoteStr(s) { return "N'" + String(s).replace(/'/g, "''") + "'"; }

  // ── Target classification (spec §3.1) ─────────────────────────────────────
  // SELECT SERVERPROPERTY('EngineEdition'): 5 = Azure SQL Database,
  // 8 = Managed Instance, 2/3/4 = boxed SQL Server.

  const rdProbeSql = "SELECT CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) AS edition, "
    + "CAST(SERVERPROPERTY('EngineEdition') AS INT) AS engine_edition, "
    + "CAST(SERVERPROPERTY('ProductMajorVersion') AS INT) AS major_version, "
    + "CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS NVARCHAR(512)) AS data_path, "
    + "CAST(SERVERPROPERTY('InstanceDefaultLogPath') AS NVARCHAR(512)) AS log_path";

  function rdClassifyEngine(engineEdition) {
    const e = Number(engineEdition);
    if (e === 8) return { family: 'mi', label: 'Azure SQL Managed Instance' };
    if (e === 5) return { family: 'azuresqldb', label: 'Azure SQL Database' };
    if ([1, 2, 3, 4].includes(e)) return { family: 'sqlserver', label: 'SQL Server' };
    return { family: 'unknown', label: 'Unknown engine (' + engineEdition + ')' };
  }

  // The §3 matrix. Each capability is true, or a string explaining WHY not —
  // the string becomes the disabled control's tooltip.
  function rdCapabilities(family) {
    switch (family) {
      case 'sqlserver': return {
        family, fromDisk: true, fromUrl: true, replace: true, withMove: true,
        logChain: true, stopAt: true, adhocBackup: true,
        platformPitr: 'Platform point-in-time restore is an Azure feature — use the log chain instead',
        bacpac: true,
      };
      case 'mi': return {
        family,
        fromDisk: 'Managed Instance has no accessible disk — restore FROM URL (Azure Blob) instead',
        fromUrl: true, replace: true,
        withMove: 'Managed Instance storage is managed — WITH MOVE is not applicable',
        logChain: true, stopAt: true, adhocBackup: true,
        platformPitr: true, bacpac: true,
      };
      case 'azuresqldb': return {
        family,
        fromDisk: 'Azure SQL Database cannot restore .bak files at all — use .bacpac import or point-in-time restore',
        fromUrl: 'Azure SQL Database cannot execute RESTORE DATABASE — use .bacpac import or point-in-time restore',
        replace: 'Azure SQL Database always restores to a NEW database name',
        withMove: 'Not applicable on Azure SQL Database',
        logChain: 'No transaction-log restores on Azure SQL Database — its platform PITR replaces them',
        stopAt: 'Use the platform point-in-time restore instead',
        adhocBackup: 'No BACKUP DATABASE on Azure SQL Database — snapshots use database copy or PITR markers',
        platformPitr: true, bacpac: true,
      };
      default: return {
        family, fromDisk: 'Unknown target engine — run Connect & test first',
        fromUrl: 'Unknown target engine — run Connect & test first',
        replace: false, withMove: false, logChain: false, stopAt: false,
        adhocBackup: false, platformPitr: false, bacpac: false,
      };
    }
  }

  // ── Inspect (spec §5.1 step 1 — never restore a file blind) ──────────────

  function fromClause(source) {
    return (source.kind === 'url' ? 'URL' : 'DISK') + ' = ' + rdQuoteStr(source.path);
  }
  function rdInspectSql(source) {
    return {
      headerSql:   'RESTORE HEADERONLY FROM '   + fromClause(source),
      fileListSql: 'RESTORE FILELISTONLY FROM ' + fromClause(source),
      verifySql:   'RESTORE VERIFYONLY FROM '   + fromClause(source),
    };
  }

  const BACKUP_TYPE = { 1: 'Full', 2: 'Log', 4: 'File', 5: 'Differential', 6: 'Partial' };

  function rdParseHeaderOnly(recordset) {
    return (recordset || []).map(r => ({
      position: Number(r.Position || r.position || 1),
      databaseName: r.DatabaseName || r.databasename || '',
      backupName: r.BackupName || '',
      type: BACKUP_TYPE[Number(r.BackupType ?? r.backuptype)] || ('Type ' + r.BackupType),
      typeCode: Number(r.BackupType ?? r.backuptype),
      startedAt: r.BackupStartDate || null,
      finishedAt: r.BackupFinishDate || null,
      firstLsn: String(r.FirstLSN ?? ''),
      lastLsn: String(r.LastLSN ?? ''),
      sizeBytes: Number(r.BackupSize || 0),
      recoveryModel: r.RecoveryModel || '',
      compatibilityLevel: Number(r.CompatibilityLevel || 0),
      softwareVersionMajor: Number(r.SoftwareVersionMajor || 0),
      serverName: r.ServerName || '',
    }));
  }

  function rdParseFileList(recordset) {
    return (recordset || []).map(r => ({
      logicalName: r.LogicalName || '',
      physicalName: r.PhysicalName || '',
      type: r.Type || 'D',                               // D data · L log
      sizeBytes: Number(r.Size || 0),
    }));
  }

  // The single most common failure users hit: SQL Server cannot restore a
  // backup taken on a NEWER major version. Hard block, not a warning.
  function rdVersionBlock(backupMajor, targetMajor) {
    if (!backupMajor || !targetMajor) return { blocked: false };
    if (Number(backupMajor) > Number(targetMajor)) {
      return { blocked: true,
        message: 'This backup was taken on a newer version of SQL Server (v' + backupMajor
          + ') than the target (v' + targetMajor + '). SQL Server cannot restore backups from a newer version — restore it onto a v'
          + backupMajor + '+ instance instead.' };
    }
    return { blocked: false };
  }

  // ── WITH MOVE auto-population (spec §5.1 step 2) ─────────────────────────
  // Retarget every file into the instance's default data/log paths, renamed
  // for the destination database so a restore-to-new-name never collides
  // with the original's files (SQL error 1834).

  function rdBuildMoves(fileList, dataPath, logPath, newDbName) {
    const safe = String(newDbName || 'restored').replace(/[^A-Za-z0-9_-]/g, '_');
    const dir = (p) => String(p || '').replace(/[\\/]+$/, '');
    let dataN = 0, logN = 0;
    return (fileList || []).map(f => {
      const isLog = f.type === 'L';
      const ext = isLog ? '.ldf' : (dataN === 0 ? '.mdf' : '.ndf');
      const n = isLog ? (logN++ ? '_log' + logN : '_log') : (dataN++ ? '_' + dataN : '');
      const base = dir(isLog ? (logPath || dataPath) : dataPath);
      return { logicalName: f.logicalName,
               physicalPath: base + '\\' + safe + n + ext };
    });
  }

  // ── Plan validation (spec §8 — the guardrails) ────────────────────────────
  //
  // plan: { targetDb, mode:'disk'|'url', source:{kind,path}, restoreAs:
  //   {kind:'overwrite'|'newDatabase', name}, recovery:'RECOVERY'|'NORECOVERY',
  //   replace, confirmName, forceSourceOverwrite, moves[], fileIndex, checksum }

  function rdValidatePlan(plan, ctx) {
    ctx = ctx || {};
    const errors = [], warnings = [];
    const caps = ctx.capabilities || rdCapabilities('unknown');
    const asNew = plan.restoreAs && plan.restoreAs.kind === 'newDatabase';
    const destName = asNew ? (plan.restoreAs.name || '') : (plan.targetDb || '');

    if (!destName.trim()) errors.push('Give the restored database a name.');
    if (!plan.source || !String(plan.source.path || '').trim()) errors.push('Choose a backup to restore from.');

    if (plan.mode === 'disk' && caps.fromDisk !== true) errors.push(String(caps.fromDisk));
    if (plan.mode === 'url' && caps.fromUrl !== true) errors.push(String(caps.fromUrl));
    if (plan.mode === 'url' && !/^https:\/\//i.test(String(plan.source && plan.source.path || ''))) {
      errors.push('Blob restore needs an https:// URL (http and other schemes are rejected by SQL Server).');
    }

    // Overwriting is the destructive branch: capability, type-to-confirm,
    // and never over the configured SOURCE database without force.
    if (!asNew || plan.replace) {
      if (caps.replace !== true) errors.push(String(caps.replace));
      if ((plan.confirmName || '') !== destName) {
        errors.push('Type the database name (' + destName + ') to confirm overwriting it.');
      }
    }
    const srcDb = String(ctx.sourceDbName || '').trim().toLowerCase();
    if (srcDb && destName.trim().toLowerCase() === srcDb && !plan.forceSourceOverwrite) {
      errors.push('“' + destName + '” is the configured SOURCE database. Restoring over your own source mid-migration is catastrophic — pick another name, or force-confirm if you truly mean it.');
    }

    // Version downgrade — hard block (spec §8.5).
    if (ctx.backupMajor && ctx.targetMajor) {
      const v = rdVersionBlock(ctx.backupMajor, ctx.targetMajor);
      if (v.blocked) errors.push(v.message);
    }

    if (plan.recovery === 'NORECOVERY') {
      warnings.push('NORECOVERY leaves the database in RESTORING until further files are applied — use “Bring online” afterwards if you change your mind.');
    }
    if (ctx.headerType && ctx.headerType !== 1 && !plan.replace && !asNew) {
      warnings.push('The selected backup set is a ' + (BACKUP_TYPE[ctx.headerType] || 'non-full') + ' backup — it needs a matching full restore beneath it.');
    }
    return { ok: errors.length === 0, errors, warnings };
  }

  // ── SQL generation (spec §6 — QUOTENAME everything) ──────────────────────

  function rdBuildRestoreSql(plan) {
    const asNew = plan.restoreAs && plan.restoreAs.kind === 'newDatabase';
    const dest = asNew ? plan.restoreAs.name : plan.targetDb;
    const opts = ['FILE = ' + (Number(plan.fileIndex) || 1)];
    for (const m of (plan.moves || [])) {
      opts.push('MOVE ' + rdQuoteStr(m.logicalName) + ' TO ' + rdQuoteStr(m.physicalPath));
    }
    if (plan.replace) opts.push('REPLACE');
    if (plan.checksum !== false) opts.push('CHECKSUM');
    opts.push(plan.recovery === 'NORECOVERY' ? 'NORECOVERY' : 'RECOVERY');
    opts.push('STATS = 5');
    return 'RESTORE DATABASE ' + rdQuoteName(dest)
      + ' FROM ' + fromClause(plan.source)
      + ' WITH ' + opts.join(', ');
  }

  // Recovery action for a database stuck in RESTORING after a NORECOVERY
  // or failed chain (spec §5.3a).
  function rdBringOnlineSql(dbName) {
    return 'RESTORE DATABASE ' + rdQuoteName(dbName) + ' WITH RECOVERY';
  }

  // Poll, don't block (spec §5.1 step 4).
  const rdProgressSql = "SELECT r.percent_complete AS pct, r.estimated_completion_time AS eta_ms, r.command "
    + "FROM sys.dm_exec_requests r WHERE r.command LIKE 'RESTORE%' OR r.command LIKE 'BACKUP%'";

  // The SAS credential prerequisite for FROM URL (spec §5.2) — surfaced as
  // copyable SQL with the gotchas baked into the comment.
  function rdCredentialSql(containerUrl) {
    const url = String(containerUrl || '').replace(/\/+$/, '');
    return '-- IDENTITY must be the literal string SHARED ACCESS SIGNATURE.\n'
      + '-- The SECRET is the SAS token WITHOUT the leading "?", needing Read + List.\n'
      + '-- The credential name must exactly match the container URL (https, no trailing slash).\n'
      + 'IF NOT EXISTS (SELECT 1 FROM sys.credentials WHERE name = ' + rdQuoteStr(url) + ')\n'
      + 'CREATE CREDENTIAL ' + rdQuoteName(url) + '\n'
      + "  WITH IDENTITY = 'SHARED ACCESS SIGNATURE',\n"
      + "       SECRET   = '<sas-token-without-leading-question-mark>'";
  }

  // ── Error translation (spec §9) ───────────────────────────────────────────

  const ERROR_MAP = [
    { re: /Msg 3154|already exists.*different|database .* already exists/i,
      msg: 'A different database already exists with this name. Tick “Overwrite existing” to replace it, or choose a new name.' },
    { re: /Msg 3201|Msg 3013|Cannot open backup device|BackupDiskFile::OpenMedia/i,
      msg: 'Cannot read the backup file. Check the path — or for blob sources, that the SAS credential exists on the server and has not expired.' },
    { re: /Msg 4326|Msg 4305|too recent to apply|too early to apply|log in this backup set/i,
      msg: 'This backup is out of sequence for the chain — log files are missing or in the wrong order. Restore the full backup first, then each log in LSN order.' },
    { re: /Msg 1834|cannot be overwritten\. It is being used by database/i,
      msg: 'The target files are in use by another database. Use File relocation (WITH MOVE) to choose different paths.' },
    { re: /Msg 3169|was created on a server running version|newer version of SQL Server/i,
      msg: 'This backup was taken on a newer version of SQL Server than the target. SQL Server cannot restore backups from a newer version.' },
    { re: /timeout|ETIMEDOUT|socket hang up|connection.*closed/i,
      msg: 'The connection timed out — on Managed Instance a restore keeps running on the server after the client disconnects. Treat this as “still running” and watch the progress panel.' ,
      stillRunning: true },
  ];

  function rdTranslateError(message) {
    const msg = String(message || '');
    for (const e of ERROR_MAP) if (e.re.test(msg)) return { friendly: e.msg, stillRunning: !!e.stillRunning, raw: msg };
    return { friendly: null, stillRunning: false, raw: msg };
  }

  const api = {
    rdQuoteName, rdQuoteStr,
    rdProbeSql, rdClassifyEngine, rdCapabilities,
    rdInspectSql, rdParseHeaderOnly, rdParseFileList, BACKUP_TYPE,
    rdVersionBlock, rdBuildMoves,
    rdValidatePlan, rdBuildRestoreSql, rdBringOnlineSql, rdProgressSql, rdCredentialSql,
    rdTranslateError,
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  root.CygenixRestore = api;
})(typeof window !== 'undefined' ? window : globalThis);
