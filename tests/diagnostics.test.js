// tests/diagnostics.test.js — the Diagnostics tab: the rules, from Node.
//
// Sep-2026. The Diagnostics tab had one button whose onclick named a function
// that no longer existed. It is now seventeen selectable checks in
// public/cygenix-diagnostics.js, backed by two new db-connect actions.
//
// This file pins what a browser cannot usefully pin and what a regression
// would silently break:
//
//   · the catalogue — seventeen tests, four groups, numbered in the order
//     the spec lists them, each with a name and a plain-English description;
//   · the thresholds, as constants a reader can find at the top of the file;
//   · the pure functions the statuses come from: how a driver error is
//     classified into "where it stopped", the ping bands, the version
//     minimum, the type-compatibility table, the free-space concern;
//   · the scrubber — a password, a function key, a code= and a user:pass@
//     never survive it — and the host masking that shows a host and nothing
//     else;
//   · the backend: both actions gate as connection.test, the probe SQL is
//     read-only and interpolates no caller value, the temp-table probe rolls
//     back and reports whether anything remained;
//   · the wiring: the view mounts the module, the module loads after
//     dashboard-app.js, the old button is gone, the storage key is
//     classified.
//
// The browser half — a ticked box runs and an unticked one does not, the
// friendly Login failed, Skipped on PostgreSQL, the downloaded report — is
// tests/browser/diagnostics.smoke.js.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

const D = require(path.join(ROOT, 'public', 'cygenix-diagnostics.js'));
const SRC = read('public', 'cygenix-diagnostics.js');
const DASH = read('public', 'dashboard.html');
const APP = read('public', 'dashboard-app.js');
const FN = read('netlify', 'functions', 'db-connect.js');
const INV = read('scripts', 'storage-inventory.js');

console.log('Diagnostics — the rules\n');

/* ── 1. The catalogue ───────────────────────────────────────────────────── */
check('seventeen tests in four groups', D.TESTS.length === 17 && D.GROUPS.length === 4);
check('numbered 1 to 17 in the order the spec lists them',
  D.TESTS.every((t, i) => t.n === i + 1 && t.id === 't' + String(i + 1).padStart(2, '0')));
check('grouped platform (1-3), connections (4-8), permissions (9-12), readiness (13-17)',
  D.TESTS.slice(0, 3).every((t) => t.group === 'platform') && D.TESTS.slice(3, 8).every((t) => t.group === 'connections')
  && D.TESTS.slice(8, 12).every((t) => t.group === 'permissions') && D.TESTS.slice(12).every((t) => t.group === 'readiness'));
check('every test has a name and a one-line description', D.TESTS.every((t) => t.name && t.desc && t.desc.length > 30 && t.desc.indexOf('\n') === -1));
check('the connection tests honour the Source / Target / Both toggle; 9 is source-only; 10-12 and 15 are target-only',
  ['t04', 't05', 't06', 't07', 't08'].every((id) => D.TESTS.find((t) => t.id === id).side === true)
  && D.TESTS.find((t) => t.id === 't09').side === 'src'
  && ['t10', 't11', 't12', 't15'].every((id) => D.TESTS.find((t) => t.id === id).side === 'tgt'));
check('nothing in the catalogue names a customer table, and nothing is 3E-specific',
  !/3E|Elite|Aderant|NxUnit|HBM_/i.test(SRC));

/* ── 2. Thresholds, as constants at the top ─────────────────────────────── */
check('a 20s client timeout per call and a 3s minimum run interval', D.TEST_TIMEOUT_MS === 20000 && D.MIN_RUN_INTERVAL_MS === 3000);
check('the ping bands are 100 and 500 ms', D.PING_MS.good === 100 && D.PING_MS.slow === 500);
check('minimum versions are declared for both engines, with a label a reader recognises',
  D.MIN_VERSIONS.mssql.major === 13 && /2016/.test(D.MIN_VERSIONS.mssql.label) && D.MIN_VERSIONS.postgres.major === 12);
check('the constants sit at the top of the file, before the first function',
  SRC.indexOf('var MIN_VERSIONS') < SRC.indexOf('function esc(') && SRC.indexOf('var TEST_TIMEOUT_MS') < SRC.indexOf('function esc('));

/* ── 3. Classification ──────────────────────────────────────────────────── */
const C = D.classifyConnError;
check('DNS, timeout and refused are told apart', C('getaddrinfo ENOTFOUND db.example') === 'dns'
  && C('Could not connect: Failed to connect to x:1433 in 15000ms (ETIMEDOUT)') === 'timeout'
  && C('connect ECONNREFUSED 10.0.0.4:1433') === 'refused');
check('a login failure and a missing database are later stages than the network', C("Login failed for user 'sa'.") === 'login'
  && C('password authentication failed for user "pg"') === 'login'
  && C("Cannot open database \"X\" requested by the login") === 'database'
  && C('database "nope" does not exist') === 'database');
check('a TLS failure and an expired Cygenix session are named', C('self signed certificate in certificate chain') === 'tls'
  && C('Auth error: jwt expired') === 'session');
check('advice exists for every stage and mentions Connections for a bad password',
  ['dns', 'timeout', 'refused', 'network', 'tls', 'config', 'session', 'rbac', 'login', 'database'].every((s) => D.connAdvice(s).length > 30)
  && /Login failed: check the username and password/.test(D.connAdvice('login')));

check('ping bands: 42 pass, 260 warn, 900 fail, unknown fail',
  D.pingStatus(42) === 'pass' && D.pingStatus(100) === 'warn' && D.pingStatus(260) === 'warn' && D.pingStatus(900) === 'fail' && D.pingStatus(null) === 'fail');
check('version: SQL Server 12 warns, 15 passes; PostgreSQL 11 warns, 15 passes; unreadable warns rather than fails',
  D.versionStatus('mssql', { version: '12.0.6024.0' }).status === 'warn' && D.versionStatus('mssql', { version: '15.0.4261.1' }).status === 'pass'
  && D.versionStatus('postgres', { serverVersionNum: 110022 }).status === 'warn' && D.versionStatus('postgres', { serverVersion: '15.3' }).status === 'pass'
  && D.versionStatus('mssql', {}).status === 'warn' && D.versionStatus('mssql', {}).known === false);

/* ── 4. Type compatibility — warns, and says why ────────────────────────── */
const T = D.typeCompat;
check('a longer string into a shorter one may truncate',
  /length 255 to 120 may truncate/.test(T({ baseType: 'varchar', maxLength: 255 }, { baseType: 'varchar', maxLength: 120 }).join()));
check('MAX into a bounded string is called out, bounded into MAX is not',
  /MAX/.test(T({ baseType: 'nvarchar', maxLength: -1 }, { baseType: 'varchar', maxLength: 50 }).join())
  && T({ baseType: 'varchar', maxLength: 50 }, { baseType: 'nvarchar', maxLength: -1 }).length === 0);
check('decimal precision and scale', /may overflow/.test(T({ baseType: 'decimal', precision: 19, scale: 4 }, { baseType: 'decimal', precision: 10, scale: 2 }).join())
  && /will round/.test(T({ baseType: 'decimal', precision: 10, scale: 4 }, { baseType: 'decimal', precision: 10, scale: 2 }).join()));
check('int into bigint is fine; bigint into int may overflow',
  T({ baseType: 'int', type: 'INT' }, { baseType: 'bigint', type: 'BIGINT' }).length === 0
  && /overflow/.test(T({ baseType: 'bigint', type: 'BIGINT' }, { baseType: 'int', type: 'INT' }).join()));
check('a string into a number may not convert; anything into a string is allowed',
  /may not convert/.test(T({ baseType: 'varchar', type: 'VARCHAR(20)' }, { baseType: 'int', type: 'INT' }).join())
  && T({ baseType: 'datetime2' }, { baseType: 'nvarchar', maxLength: 100 }).length === 0);
check('nullable into NOT NULL without a default is flagged; with a default it is not',
  /allows NULL but target does not/.test(T({ baseType: 'varchar', maxLength: 5, nullable: true }, { baseType: 'varchar', maxLength: 5, nullable: false }).join())
  && T({ baseType: 'varchar', maxLength: 5, nullable: true }, { baseType: 'varchar', maxLength: 5, nullable: false, default: "('')" }).length === 0);
check('the families cover both engines\u2019 names', D.typeFamily('character varying') === 'string' && D.typeFamily('timestamp without time zone') === 'datetime'
  && D.typeFamily('uniqueidentifier') === 'uuid' && D.typeFamily('uuid') === 'uuid' && D.typeFamily('bytea') === 'binary' && D.typeFamily('bit') === 'boolean');

check('free space: a file under the threshold that cannot grow warns; one that can grow does not',
  D.spaceStatus({ files: [{ name: 'D', kind: 'ROWS', freeMb: 200, autogrow: false }] }).status === 'warn'
  && D.spaceStatus({ files: [{ name: 'D', kind: 'ROWS', freeMb: 200, autogrow: true }] }).status === 'pass'
  && D.spaceStatus({ files: [], volumes: [{ mount: 'D:\\', freeMb: 512, totalMb: 4096 }] }).concerns.length === 1);

/* ── 5. The scrubber and the mask ───────────────────────────────────────── */
const S = D.scrub;
check('a password= fragment is masked', S('Server=x;Password=Hunter2!;', []).indexOf('Hunter2') === -1 && /Password=\*\*\*/.test(S('Server=x;Password=Hunter2!;', [])));
check('a pwd=, a code= and a user:pass@ are masked', S('pwd=abc123;', []).indexOf('abc123') === -1
  && S('https://f.net/api/db?code=KEY123456', []).indexOf('KEY123456') === -1
  && S('mssql://sa:Sup3r@host/db', []).indexOf('Sup3r') === -1 && /sa:\*\*\*@host/.test(S('mssql://sa:Sup3r@host/db', [])));
check('a whole connection value handed in as a secret is removed wherever it appears',
  S('failed for Server=a;Password=b; again Server=a;Password=b;', ['Server=a;Password=b;']).indexOf('Server=a') === -1);
check('a bare password echoed by a driver is removed too, because the run scrubs the passwords out of every connection value',
  /function passwordsOf/.test(SRC) && /s\.push\.apply\(s, passwordsOf\(c\.value\)\)/.test(SRC)
  && SRC.indexOf('(?:password|pwd)\\s*=') !== -1 && SRC.indexOf('[?&]code=([^&\\s]+)') !== -1);
check('the host mask shows a host and nothing else, for every string form',
  D.hostOf('mssql://sa:pw@src.example.internal:1433/SRC') === 'src.example.internal'
  && D.hostOf('Server=tcp:tgt.example.internal,1433;Database=TGT;User Id=u;Password=p;') === 'tgt.example.internal'
  && D.hostOf('postgres://pg:pw@pg.example.internal:5432/db') === 'pg.example.internal'
  && D.hostOf('host=pg.internal port=5432 dbname=x user=u password=p') === 'pg.internal'
  && D.hostOf('https://cygenix-db-api-x.uksouth-01.azurewebsites.net/api/db?code=K') === 'cygenix-db-api-x.uksouth-01.azurewebsites.net');
check('the engine is told from the value: https is Azure-direct, postgres:// and driver=postgres are PostgreSQL, the rest SQL Server',
  D.engineOf('https://x.azurewebsites.net/api/db') === 'azure' && D.engineOf('postgresql://a:b@c/d') === 'postgres'
  && D.engineOf('host=h user=u dbname=d') === 'postgres' && D.engineOf('Server=x;Database=y;') === 'mssql');
check('object names split on schema, with brackets and quotes stripped',
  JSON.stringify(D.splitObject('[dbo].[Ledger]')) === '{"schema":"dbo","name":"Ledger"}'
  && JSON.stringify(D.splitObject('"fin"."ledger_entry"')) === '{"schema":"fin","name":"ledger_entry"}'
  && D.splitObject('Ledger', 'public').schema === 'public');

check('the module never logs — no console.log, and the one console.warn would be scrubbed',
  !/console\.log\(/.test(SRC) && (SRC.match(/console\.(warn|error|info|debug)\(/g) || []).length === 0);
check('localStorage reads and writes are wrapped in try/catch',
  /function loadPrefs\(\) \{[\s\S]*?try \{ all = JSON\.parse\(localStorage/.test(SRC) && /function savePrefs\(\) \{\s*try \{/.test(SRC));
check('the storage key is declared and classified in the inventory',
  D.STORE_KEY === 'cygenix_diag_selection_v1' && /'cygenix_diag_selection_v1':\s*\['C'/.test(INV));

/* ── 6. The report ──────────────────────────────────────────────────────── */
const fakeState = { lastRunAt: Date.parse('2026-09-19T10:00:00Z'), user: 'me@example.test', side: 'both',
  ctx: { profile: { id: 'P1', name: 'Conv to Azure', envClass: 'DEV' }, conns: { src: { host: 'src.internal', engine: 'mssql' }, tgt: { host: 'tgt.internal', engine: 'postgres' } } },
  results: { t01: { status: 'pass', summary: 'Web service: 90 ms · Data service: 210 ms' },
             t05: { status: 'fail', parts: [{ label: 'Source', status: 'fail', summary: 'Login failed', action: 'Login failed: check the username and password saved for this connection under Connections.', details: "Login failed for user 'sa'." }, { label: 'Target', status: 'pass', summary: 'Signed in as pg' }] },
             t11: { status: 'skip', summary: 'Not supported on PostgreSQL' } } };
const txt = D.reportText(fakeState);
check('the text report carries the header, the hosts, and one line per test with the status word',
  /CYGENIX DIAGNOSTICS REPORT/.test(txt) && /Source:\s+src\.internal \(SQL Server\)/.test(txt) && /Target:\s+tgt\.internal \(PostgreSQL\)/.test(txt)
  && /1\. Cygenix services reachable\s+PASS/.test(txt) && /5\. Login works\s+FAIL/.test(txt) && /11\. Bulk load permission\s+SKIPPED/.test(txt));
check('per-side results appear under the test with their own What to do',
  /Source: FAIL — Login failed/.test(txt) && /What to do: Login failed: check the username/.test(txt) && /Target: PASS — Signed in as pg/.test(txt));
check('and the report says what it is not', /No credentials are included/.test(txt) && /read-only/.test(txt));
const js = D.reportJson(fakeState);
check('the JSON report has the same facts as data', js.results.length === 3 && js.source.host === 'src.internal' && js.results[1].parts[0].side === 'Source' && js.results[1].parts[0].status === 'fail');

/* ── 7. The backend ─────────────────────────────────────────────────────── */
check('both actions are gated as a connection test, not a write',
  /'diag-probe': 'connection\.test'/.test(FN) && /'diag-temp-table': 'connection\.test'/.test(FN));
check('both dialect handlers answer both actions',
  (FN.match(/case 'diag-probe':/g) || []).length === 2 && (FN.match(/case 'diag-temp-table':/g) || []).length === 2);
const between = (a, b) => FN.slice(FN.indexOf(a), FN.indexOf(b));
const probeMs = between('async function diagProbeMssql', 'async function diagTempTableMssql');
const probePg = between('async function diagProbePostgres', 'async function diagTempTablePostgres');
const sqlOf = (body) => (body.match(/`[^`]*`/g) || []).join('\n').replace(/'[^']*'/g, "''");
for (const [name, body] of [['SQL Server', probeMs], ['PostgreSQL', probePg]]) {
  check('the ' + name + ' probes are read-only', !/\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|MERGE|EXEC|CREATE)\b/i.test(sqlOf(body)), sqlOf(body).match(/\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|MERGE|EXEC|CREATE)\b/i));
  check('and interpolate no caller value into the statement text',
    !/\$\{[^}]*(body|\.schema|\.name|tables\[)/.test(sqlOf(body)));
}
check('SQL Server names are bound and quoted by QUOTENAME, never concatenated in JavaScript',
  /QUOTENAME\(@s\) \+ '\.' \+ QUOTENAME\(@t\)/.test(probeMs) && /r\.input\(k, mssql\.NVarChar, v\)/.test(probeMs));
check('PostgreSQL names are bound and quoted by format(\'%I\')',
  /format\('%I\.%I', \$1, \$2\)/.test(probePg) && /\[t\.schema, t\.name\]/.test(probePg));
check('the probe menu is a fixed set and anything else is a 400',
  /DIAG_PROBES = new Set\(\['version', 'ping', 'read-access', 'write-access', 'bulk', 'space', 'collation'\]\)/.test(FN) && /Unknown probe:/.test(FN));
check('the table list is capped and each name length-checked',
  /DIAG_MAX_TABLES = 200/.test(FN) && /name\.length > 128 \|\| schema\.length > 128/.test(FN));
const tmpMs = between('async function diagTempTableMssql', 'async function diagProbePostgres');
const tmpPg = FN.slice(FN.indexOf('async function diagTempTablePostgres'));
check('the SQL Server temp table is created inside a transaction that is rolled back in the same batch, with XACT_ABORT on',
  /SET XACT_ABORT ON/.test(tmpMs) && /BEGIN TRAN/.test(tmpMs) && /ROLLBACK TRAN/.test(tmpMs) && /CREATE TABLE #cygenix_diag_probe/.test(tmpMs));
check('and reports whether anything remained, after the rollback and after the cleanup',
  /remainsAfterRollback/.test(tmpMs) && /remainsAfterCleanup/.test(tmpMs) && /IF @afterRollback = 1 DROP TABLE #cygenix_diag_probe/.test(tmpMs));
check('the PostgreSQL temp table is ON COMMIT DROP inside BEGIN / ROLLBACK, with the rollback in a finally',
  /CREATE TEMP TABLE cygenix_diag_probe[^']*ON COMMIT DROP/.test(tmpPg) && /finally \{\s*await client\.query\('ROLLBACK'\)/.test(tmpPg) && /to_regclass\('pg_temp\.cygenix_diag_probe'\)/.test(tmpPg));
check('a 4xx thrown by the probe helpers keeps its status code instead of becoming a 500',
  (FN.match(/if \(e\.statusCode\) return err\(e\.message, e\.hint \|\| null, e\.statusCode\);/g) || []).length === 2);
check('the unknown-action hint lists the new actions on both dialects',
  (FN.match(/rowcounts \| diag-probe \| diag-temp-table/g) || []).length === 2);
check('the timing for test 7 is measured on the server, three samples',
  /async function diagPing/.test(FN) && /for \(let i = 0; i < 3; i\+\+\)/.test(FN) && /process\.hrtime\.bigint\(\)/.test(FN));

/* ── 8. The wiring ──────────────────────────────────────────────────────── */
check('the view is a mount the module fills, and the old button is gone',
  /<div id="view-diagnostics" class="view">\s*<div id="cyg-diag-mount"><\/div>\s*<\/div>/.test(DASH) && !/runDiagnostics/.test(DASH) && !/Netlify function setup/.test(DASH));
check('the module loads after dashboard-app.js, deferred',
  DASH.indexOf('<script src="/cygenix-diagnostics.js') > DASH.indexOf('<script src="/dashboard-app.js') && /cygenix-diagnostics\.js\?v=[0-9a-f]+" defer/.test(DASH));
check('showView mounts it', /if \(v === 'diagnostics' && window\.CygenixDiagnostics\) window\.CygenixDiagnostics\.init\('cyg-diag-mount'\);/.test(APP));
check('nothing outside the signed-in app was touched', !/cygenix-diagnostics/.test(read('public', 'index.html')));
check('the Azure-direct permission statement is a constant with the names bound as parameters',
  /var AZ_PERMS_SQL = "SELECT CASE WHEN OBJECT_ID\(QUOTENAME\(@s\)/.test(SRC) && /params: \[\{ name: 's', value: t\.schema \}, \{ name: 't', value: t\.name \}\]/.test(SRC));
check('the browser sends probe names, never SQL, to db-connect for a diagnostic',
  !/action: 'diag-probe'[^}]*sql:/.test(SRC) && (SRC.match(/action: 'diag-probe', probe: '/g) || []).length >= 6);
check('the running flag is cleared in the finally of run(), and nowhere else',
  /\} finally \{[\s\S]{0,300}state\.running = false;/.test(SRC) && (SRC.match(/state\.running = false/g) || []).length === 1);
check('tests run one at a time — a for loop with an await, not Promise.all',
  /for \(var i = 0; i < ids\.length; i\+\+\) \{[\s\S]*?await runOne/.test(SRC) && !/Promise\.all\(ids/.test(SRC));
check('every fetch carries a 20s AbortSignal', (SRC.match(/AbortSignal\.timeout\(TEST_TIMEOUT_MS\)/g) || []).length >= 4);
check('no emoji, no icon glyphs beyond the console\u2019s set', !/[\u{1F300}-\u{1FAFF}\u{2600}-\u{26FF}\u{2700}-\u{27BF}]/u.test(SRC));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
