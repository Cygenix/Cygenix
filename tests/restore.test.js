// tests/restore.test.js — the Restore Database engine.
//
// The spec's own warning drives the tests: "Azure" is not one target.
// The capability matrix must classify SQL Server / Managed Instance /
// Azure SQL Database correctly and give REASONS for what each cannot do;
// the guardrails (version downgrade, type-to-confirm, source-overwrite
// refusal, injection-proof names) are all mandatory; and every generated
// statement must be exactly the T-SQL the spec prescribes.

'use strict';

const R = require('../public/cygenix-restore.js');
const RB = require('../netlify/functions/lib/rbac.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Restore Database — engine\n');

// ── Classification & capability matrix (spec §3) ──────────────────────────
check('EngineEdition 8 is Managed Instance, 5 is Azure SQL Database, 3 is SQL Server',
  R.rdClassifyEngine(8).family === 'mi' && R.rdClassifyEngine(5).family === 'azuresqldb'
  && R.rdClassifyEngine(3).family === 'sqlserver');
check('SQL Server restores from disk and URL, with MOVE and REPLACE',
  (() => { const c = R.rdCapabilities('sqlserver');
           return c.fromDisk === true && c.fromUrl === true && c.withMove === true && c.replace === true; })());
check('Managed Instance: URL is the ONLY file path, and the disk reason says so',
  (() => { const c = R.rdCapabilities('mi');
           return c.fromUrl === true && typeof c.fromDisk === 'string' && /FROM URL|Blob/i.test(c.fromDisk)
             && typeof c.withMove === 'string'; })());
check('Azure SQL Database can restore NO .bak at all, and the reason offers bacpac/PITR',
  (() => { const c = R.rdCapabilities('azuresqldb');
           return typeof c.fromDisk === 'string' && /bacpac|point-in-time/i.test(c.fromDisk)
             && typeof c.fromUrl === 'string' && typeof c.replace === 'string'
             && c.platformPitr === true; })());
check('an unprobed target disables everything with a probe hint',
  /Connect & test/.test(String(R.rdCapabilities('unknown').fromDisk)));
check('the probe query reads Edition, EngineEdition and default paths',
  /SERVERPROPERTY\('EngineEdition'\)/.test(R.rdProbeSql) && /InstanceDefaultDataPath/.test(R.rdProbeSql));

// ── Inspect (spec §5.1 — never restore blind) ─────────────────────────────
const insp = R.rdInspectSql({ kind: 'disk', path: "D:\\backups\\Conversion_DM.bak" });
check('inspect emits HEADERONLY, FILELISTONLY and VERIFYONLY',
  /^RESTORE HEADERONLY FROM DISK = N'/.test(insp.headerSql)
  && /^RESTORE FILELISTONLY FROM DISK/.test(insp.fileListSql)
  && /^RESTORE VERIFYONLY FROM DISK/.test(insp.verifySql));
check('a blob source inspects FROM URL',
  /FROM URL = N'https:/.test(R.rdInspectSql({ kind: 'url', path: 'https://x.blob.core.windows.net/b/f.bak' }).headerSql));
check("a path containing a quote can't break out of the literal",
  R.rdInspectSql({ kind: 'disk', path: "D:\\o'brien.bak" }).headerSql.includes("N'D:\\o''brien.bak'"));

const header = R.rdParseHeaderOnly([{
  Position: 1, DatabaseName: 'Conversion_DM', BackupType: 1, BackupStartDate: '2026-08-17T09:00:00Z',
  FirstLSN: '37000000005600001', LastLSN: '37000000007200001', BackupSize: 4402341478,
  RecoveryModel: 'FULL', CompatibilityLevel: 150, SoftwareVersionMajor: 15, ServerName: 'OLDPROD',
}, { Position: 2, DatabaseName: 'Conversion_DM', BackupType: 2, BackupSize: 1048576 }]);
check('HEADERONLY parses into typed backup sets',
  header[0].type === 'Full' && header[0].recoveryModel === 'FULL'
  && header[0].softwareVersionMajor === 15 && header[1].type === 'Log');

const files = R.rdParseFileList([
  { LogicalName: 'Conversion_DM', PhysicalName: 'E:\\data\\Conversion_DM.mdf', Type: 'D', Size: 4e9 },
  { LogicalName: 'Conversion_DM_log', PhysicalName: 'F:\\log\\Conversion_DM_log.ldf', Type: 'L', Size: 1e9 },
]);
check('FILELISTONLY parses logical/physical names and file types',
  files[0].type === 'D' && files[1].type === 'L' && files[0].logicalName === 'Conversion_DM');

// ── Version downgrade — hard block (spec §8.5) ────────────────────────────
check('a v16 backup cannot restore onto a v15 target, and the message explains',
  (() => { const v = R.rdVersionBlock(16, 15);
           return v.blocked && /newer version/.test(v.message); })());
check('same or older version passes',
  !R.rdVersionBlock(15, 15).blocked && !R.rdVersionBlock(13, 15).blocked);

// ── WITH MOVE auto-population (spec §5.1 step 2) ─────────────────────────
const moves = R.rdBuildMoves(files, 'E:\\SQLData\\', 'F:\\SQLLogs', 'Conversion_DM_r1');
check('moves retarget every file into the default paths, renamed for the new database',
  moves[0].physicalPath === 'E:\\SQLData\\Conversion_DM_r1.mdf'
  && moves[1].physicalPath === 'F:\\SQLLogs\\Conversion_DM_r1_log.ldf'
  && moves[0].logicalName === 'Conversion_DM', JSON.stringify(moves));

// ── SQL generation (spec §6 — QUOTENAME everything) ──────────────────────
const plan = {
  targetDb: 'Conversion_DM', mode: 'disk',
  source: { kind: 'disk', path: 'D:\\backups\\Conversion_DM.bak' },
  restoreAs: { kind: 'newDatabase', name: 'Conversion_DM_r1' },
  recovery: 'RECOVERY', replace: false, fileIndex: 1, moves,
};
const sql = R.rdBuildRestoreSql(plan);
check('the generated RESTORE names the destination, moves the files and recovers',
  /^RESTORE DATABASE \[Conversion_DM_r1\] FROM DISK = N'D:\\backups\\Conversion_DM\.bak' WITH FILE = 1, MOVE N'Conversion_DM' TO N'E:\\SQLData\\Conversion_DM_r1\.mdf', MOVE N'Conversion_DM_log' TO N'F:\\SQLLogs\\Conversion_DM_r1_log\.ldf', CHECKSUM, RECOVERY, STATS = 5$/.test(sql), sql);
check('overwrite adds REPLACE; NORECOVERY replaces RECOVERY',
  (() => { const s = R.rdBuildRestoreSql({ ...plan, restoreAs: { kind: 'overwrite' }, replace: true, recovery: 'NORECOVERY' });
           return /\[Conversion_DM\]/.test(s) && /REPLACE/.test(s) && /NORECOVERY/.test(s) && !/, RECOVERY/.test(s); })());
check('a database named  x];--  cannot inject (acceptance §10)',
  (() => { const s = R.rdBuildRestoreSql({ ...plan, restoreAs: { kind: 'newDatabase', name: 'x];--' } });
           return s.includes('RESTORE DATABASE [x]];--]') && !/\]\s*;\s*--/.test(s.replace('[x]];--]', '')); })());
check('URL restores emit FROM URL',
  /FROM URL = N'https:/.test(R.rdBuildRestoreSql({ ...plan, mode: 'url', source: { kind: 'url', path: 'https://acc.blob.core.windows.net/b/f.bak' } })));
check('the stuck-in-RESTORING recovery action is one statement',
  R.rdBringOnlineSql('Conversion_DM') === 'RESTORE DATABASE [Conversion_DM] WITH RECOVERY');
check('the SAS credential helper bakes in the gotchas',
  (() => { const s = R.rdCredentialSql('https://acc.blob.core.windows.net/backups/');
           return /SHARED ACCESS SIGNATURE/.test(s) && /without the leading/i.test(s)
             && s.includes("N'https://acc.blob.core.windows.net/backups'"); })());

// ── Plan validation — the guardrails (spec §8) ────────────────────────────
const capsSql = R.rdCapabilities('sqlserver');
check('a clean new-name plan validates',
  R.rdValidatePlan(plan, { capabilities: capsSql }).ok);
check('overwrite without type-to-confirm is refused',
  (() => { const v = R.rdValidatePlan({ ...plan, restoreAs: { kind: 'overwrite' }, replace: true }, { capabilities: capsSql });
           return !v.ok && v.errors.some(e => /Type the database name/.test(e)); })());
check('type-to-confirm with the exact name passes',
  R.rdValidatePlan({ ...plan, restoreAs: { kind: 'overwrite' }, replace: true, confirmName: 'Conversion_DM' },
    { capabilities: capsSql }).ok);
check('restoring over the configured SOURCE database is refused without force',
  (() => { const v = R.rdValidatePlan({ ...plan, restoreAs: { kind: 'newDatabase', name: 'LegacyLive' } },
             { capabilities: capsSql, sourceDbName: 'LegacyLive' });
           return !v.ok && /catastrophic/.test(v.errors.join(' ')); })());
check('force-confirm overrides the source-overwrite refusal',
  R.rdValidatePlan({ ...plan, restoreAs: { kind: 'newDatabase', name: 'LegacyLive' }, forceSourceOverwrite: true },
    { capabilities: capsSql, sourceDbName: 'LegacyLive' }).ok);
check('a .bak plan against Azure SQL Database is blocked BEFORE execution, pointing at bacpac',
  (() => { const v = R.rdValidatePlan(plan, { capabilities: R.rdCapabilities('azuresqldb') });
           return !v.ok && v.errors.some(e => /bacpac|point-in-time/i.test(e)); })());
check('a disk plan against Managed Instance is blocked with the URL alternative',
  (() => { const v = R.rdValidatePlan(plan, { capabilities: R.rdCapabilities('mi') });
           return !v.ok && v.errors.some(e => /FROM URL|Blob/i.test(e)); })());
check('the version downgrade block reaches validation',
  (() => { const v = R.rdValidatePlan(plan, { capabilities: capsSql, backupMajor: 16, targetMajor: 15 });
           return !v.ok && v.errors.some(e => /newer version/.test(e)); })());
check('a non-https blob URL is rejected',
  !R.rdValidatePlan({ ...plan, mode: 'url', source: { kind: 'url', path: 'http://x/y.bak' } },
    { capabilities: capsSql }).ok);
check('NORECOVERY warns about the RESTORING state',
  R.rdValidatePlan({ ...plan, recovery: 'NORECOVERY' }, { capabilities: capsSql })
    .warnings.some(w => /RESTORING/.test(w)));

// ── Error translation (spec §9) ───────────────────────────────────────────
check('3154 translates to the overwrite hint',
  /Overwrite existing/.test(R.rdTranslateError('Msg 3154, Level 16 ...').friendly));
check('3201 points at path or SAS credential',
  /SAS credential/.test(R.rdTranslateError('Msg 3201: Cannot open backup device').friendly));
check('1834 points at file relocation',
  /File relocation/.test(R.rdTranslateError("Msg 1834: The file ... cannot be overwritten. It is being used by database 'x'").friendly));
check('3169 explains the version rule',
  /newer version/.test(R.rdTranslateError('Msg 3169 ...').friendly));
check('a timeout reads as still-running, not failed (MI async rule)',
  (() => { const t = R.rdTranslateError('ETIMEDOUT while awaiting response');
           return t.stillRunning && /still running|keeps running/i.test(t.friendly); })());
check('an unknown error passes through raw with no fake translation',
  R.rdTranslateError('some new failure').friendly === null);

// ── RBAC wiring (spec §8.8 — enforced server-side) ────────────────────────
check('restore.execute is Lead-only in the matrix',
  RB.can({ roles: ['ML'] }, 'restore.execute', { environment: 'DEV', mutating: true }).allow
  && !RB.can({ roles: ['EN'] }, 'restore.execute', { environment: 'DEV', mutating: true }).allow
  && !RB.can({ roles: ['PA'] }, 'restore.execute', { environment: 'DEV', mutating: true }).allow);
check('restore.inspect is open to Engineers too',
  RB.can({ roles: ['EN'] }, 'restore.inspect', { environment: 'DEV' }).allow);
check('a Production restore is allowed to the Lead at high severity',
  (() => { const d = RB.can({ roles: ['ML'] }, 'restore.execute', { environment: 'PROD', mutating: true });
           return d.allow && d.severity === 'high'; })());

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
