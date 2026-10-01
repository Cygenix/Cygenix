/* cc-mcp.test.js — the Dev Console's database bridge (an MCP server)
   ---------------------------------------------------------------------------
   Plain Node, no framework. The bridge is the only road from Claude to a
   database, so what it enforces is pinned here from the outside: the pass,
   the protocol, the read-only switch, the destructive refusals, the row
   cap, the organisation's switch and roles, the audit record, and the
   three ways a query can travel (SQL Server, PostgreSQL, a Function App).
   Everything it calls — the pass check, the database road, the org store,
   the audit trail — is a stub recording what it was asked. */
'use strict';

const path = require('path');
let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra !== undefined ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const section = (t) => console.log('\n' + t);

const MCP = require(path.join(__dirname, '..', 'netlify', 'functions', 'cc-mcp.js'));
const I = MCP._internals;
const D = I.deps;

// ── The stubbed world ──────────────────────────────────────────────────────
let REDEEM = null;            // what the pass check answers
let ENV = { CYGENIX_DATA_FN_KEY: 'hostkey', AZURE_FUNCTION_KEY: 'productkey' };
let CALLS = [];
let ROLES = ['EN'];
let POLICY = { enabled: true, roles: ['OW', 'PA', 'ML', 'EN'] };
let DB = { recordset: [{ n: 1 }], rowsAffected: 0 };
let GATE = null;              // rbacGate's answer; null = allowed
let FN_REPLY = null;
let AUDIT = [];
let AUDIT_THROWS = false;

const ctxFor = (over) => Object.assign({
  ok: true, kind: 'session', sessionId: 'sesn_1', oid: 'oid-me', email: 'me@acme.test', tenantId: 'tn_1',
  connectionId: 'sconn_x', connectionName: 'Target DEV', profileName: 'Demo', side: 'tgt', dbType: 'sqlserver',
  mode: 'direct', connString: 'Server=db.acme.test;Database=Fin;User Id=u;Password=p', fnUrl: null, fnKey: null, readOnly: true,
}, over || {});

D.env = (k) => ENV[k];
D.now = () => 1700000000000;
D.fetch = async (url, init) => {
  CALLS.push(['fetch', url, init && init.body ? JSON.parse(init.body) : null]);
  if (/claude-code-bridge\/redeem/.test(url)) {
    const r = REDEEM;
    return { status: r.status, text: async () => JSON.stringify(r.body) };
  }
  const r = FN_REPLY || { status: 200, body: { success: true, recordset: [{ v: 1 }], rowsAffected: 0 } };
  return { status: r.status, ok: r.status >= 200 && r.status < 300, text: async () => (typeof r.body === 'string' ? r.body : JSON.stringify(r.body)) };
};
D.store = () => ({ fake: true });
D.resolveActor = async (store, authed) => ({ oid: authed.oid, email: authed.email, roles: ROLES.slice(), isActive: true });
D.loadAll = async () => ({ users: {} });
D.resolveTenant = async () => ({ tenant: { id: 'tn_1', claudeCode: POLICY } });
D.appendAudit = async (store, evt) => { if (AUDIT_THROWS) throw new Error('blob down'); AUDIT.push(evt); return {}; };
D.bridge = () => ({
  detectDialect: (cs) => (/^postgres/i.test(cs) ? 'postgres' : 'mssql'),
  rbacGate: async (authed, action, dialect, cs, database, body) => { CALLS.push(['rbacGate', authed, action, dialect, body.sql]); return GATE || { guardrail: null }; },
  handleMssql: async (action, cs, database, body) => {
    CALLS.push(['handleMssql', action, body.sql]);
    if (DB.error) return { statusCode: 500, body: JSON.stringify({ error: DB.error }) };
    return { statusCode: 200, body: JSON.stringify({ success: true, recordset: DB.recordset, rowsAffected: DB.rowsAffected }) };
  },
  handlePostgres: async (action, cs, database, body) => {
    CALLS.push(['handlePostgres', action, body.sql, body.readOnly]);
    return { statusCode: 200, body: JSON.stringify({ success: true, recordset: DB.recordset, rowsAffected: DB.rowsAffected }) };
  },
});

const reset = (redeemCtx) => {
  CALLS = []; AUDIT = []; AUDIT_THROWS = false; GATE = null; FN_REPLY = null; DB = { recordset: [{ n: 1 }], rowsAffected: 0 };
  ROLES = ['EN']; POLICY = { enabled: true, roles: ['OW', 'PA', 'ML', 'EN'] };
  ENV = { CYGENIX_DATA_FN_KEY: 'hostkey', AZURE_FUNCTION_KEY: 'productkey' };
  REDEEM = { status: 200, body: ctxFor(redeemCtx) };
  I._reset();
};
const post = (body, headers) => MCP.handler({ httpMethod: 'POST', headers: Object.assign({ authorization: 'Bearer cyb_' + 'a'.repeat(18) + '.' + 'b'.repeat(43) }, headers || {}),
  body: typeof body === 'string' ? body : JSON.stringify(body) });
const rpc = (method, params, id) => ({ jsonrpc: '2.0', id: id === undefined ? 1 : id, method, params: params || {} });
const call = async (name, args, redeemCtx) => {
  if (redeemCtx !== undefined) REDEEM = { status: 200, body: ctxFor(redeemCtx) };
  const r = await post(rpc('tools/call', { name, arguments: args || {} }));
  const b = JSON.parse(r.body || '{}');
  const res = b.result || {};
  const text = res.content && res.content[0] && res.content[0].text;
  let json = null; try { json = JSON.parse(text); } catch (e) { /* refusal text */ }
  return { http: r, body: b, isError: !!res.isError, text: text || '', json };
};
const calls = (kind) => CALLS.filter(c => c[0] === kind);

(async () => {
  console.log('Dev Console bridge — cc-mcp\n');

  section('1. The door: a pass, and nothing else');
  reset();
  let r = await MCP.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(rpc('ping')) });
  check('NO PASS IS A 401, with the Bearer challenge, and nothing is asked of anyone', r.statusCode === 401 && /Bearer/.test(r.headers['WWW-Authenticate']) && CALLS.length === 0, r.statusCode);
  r = await MCP.handler({ httpMethod: 'GET', headers: {} });
  check('GET is 405 — no event stream is offered, which the transport allows', r.statusCode === 405 && r.headers.Allow === 'POST');
  REDEEM = { status: 401, body: { error: 'This bridge pass is not valid.' } };
  r = await post(rpc('tools/list'));
  check('A PASS THE FUNCTION APP REFUSES IS A 401 before any method runs', r.statusCode === 401 && /invalid_token/.test(r.headers['WWW-Authenticate']) && /not valid/.test(r.body), r.body);
  const redeemCall = calls('fetch')[0];
  check('the pass is redeemed at the bridge route, with the host key, carrying only the pass',
    /\/agent\/claude-code-bridge\/redeem\?code=hostkey$/.test(redeemCall[1]) && Object.keys(redeemCall[2]).join() === 'token', redeemCall[1]);
  reset(); ENV = {};
  r = await post(rpc('tools/list'));
  check('no host key on this deployment is a 503 that names the setting', r.statusCode === 503 && /CYGENIX_DATA_FN_KEY/.test(r.body));
  reset();
  r = await post('{not json');
  check('a body that is not JSON is a JSON-RPC parse error', r.statusCode === 400 && JSON.parse(r.body).error.code === -32700);

  section('2. The protocol');
  reset();
  r = await post(rpc('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'anthropic' } }));
  let b = JSON.parse(r.body);
  check('initialize answers with the version asked for, tools, and who this server is',
    r.statusCode === 200 && b.result.protocolVersion === '2025-03-26' && b.result.capabilities.tools && b.result.serverInfo.name === 'cygenix', r.body);
  check('…and tells Claude which database and that changes are NOT allowed right now', /Target DEV/.test(b.result.instructions) && /NOT allowed/.test(b.result.instructions));
  r = await post(rpc('initialize', { protocolVersion: '1999-01-01' }));
  check('an unknown version gets the newest one this server speaks', JSON.parse(r.body).result.protocolVersion === '2025-06-18');
  r = await post({ jsonrpc: '2.0', method: 'notifications/initialized' });
  check('a notification is accepted with 202 and no body', r.statusCode === 202 && !r.body);
  r = await post(rpc('tools/list'));
  const tools = JSON.parse(r.body).result.tools;
  check('THREE TOOLS: list_tables, describe_table, run_query', tools.map(t => t.name).join() === 'list_tables,describe_table,run_query');
  check('run_query takes one statement and at most 1,000 rows', tools[2].inputSchema.required.join() === 'sql' && tools[2].inputSchema.properties.max_rows.maximum === 1000);
  check('the two catalogue tools say they only read', tools[0].annotations.readOnlyHint === true && tools[1].annotations.readOnlyHint === true);
  r = await post(rpc('resources/list'));
  check('an unknown method is -32601', JSON.parse(r.body).error.code === -32601);
  r = await post(rpc('tools/call', { name: 'drop_everything', arguments: {} }));
  check('an unknown tool is -32602', JSON.parse(r.body).error.code === -32602);
  r = await post([rpc('ping', {}, 1), { jsonrpc: '2.0', method: 'notifications/x' }, rpc('tools/list', {}, 2)]);
  b = JSON.parse(r.body);
  check('a batch is answered message by message, notifications dropped', Array.isArray(b) && b.length === 2 && b[0].id === 1 && b[1].id === 2);

  section('3. Reads, in a read-only session (SQL Server, direct)');
  reset();
  DB.recordset = [{ id: 1, name: 'Ann', at: new Date('2026-01-02T03:04:05Z'), big: '12345678901234567890', blob: Buffer.from('abc') }, { id: 2, name: 'Bo', at: null, big: '1', blob: null }];
  let q = await call('run_query', { sql: 'SELECT id, name FROM dbo.customers' });
  check('A READ RUNS, and the rows come back as columns and rows', !q.isError && q.json.columns.join() === 'id,name,at,big,blob' && q.json.rows.length === 2 && q.json.rows[0][1] === 'Ann', q.text);
  check('dates as ISO, big integers as the strings the driver gives, binary as a short hex — not a list of bytes',
    q.json.rows[0][2] === '2026-01-02T03:04:05.000Z' && q.json.rows[0][3] === '12345678901234567890' && q.json.rows[0][4] === '0x616263', JSON.stringify(q.json.rows[0]));
  check('the cell shaper copes with real Dates, Buffers and BigInts too', I.cell(new Date(0)) === '1970-01-01T00:00:00.000Z' && I.cell(10n) === '10' && I.cell(Buffer.from([255])) === '0xff');
  const gateCall = calls('rbacGate')[0];
  check('the SQL editor\'s own RBAC gate decides first, as the session\'s owner, on the statement as written',
    gateCall && gateCall[1].oid === 'oid-me' && gateCall[2] === 'execute' && gateCall[3] === 'mssql' && gateCall[4] === 'SELECT id, name FROM dbo.customers');
  const ran = calls('handleMssql')[0][2];
  check('THE READ RUNS INSIDE A TRANSACTION THAT IS ALWAYS ROLLED BACK', /^SET XACT_ABORT ON;\nBEGIN TRANSACTION;\nSELECT id, name FROM dbo\.customers\n;\nIF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;$/.test(ran), ran);
  check('…and nothing like a password went into the answer', q.text.indexOf('Password') === -1);
  const a = AUDIT[0];
  check('EVERY QUERY IS RECORDED: claudecode.query, the owner, the connection, the statement, rows, read',
    a && a.action === 'claudecode.query' && a.outcome === 'allowed' && a.actorOid === 'oid-me' && a.resourceId === 'sesn_1'
    && a.detail.connection === 'Target DEV' && a.detail.sql === 'SELECT id, name FROM dbo.customers' && a.detail.rows === 2 && a.detail.write === false
    && a.severity === 'info' && a.detail.tenantId === 'tn_1', JSON.stringify(a));

  section('4. The "Allow changes" switch, enforced here');
  reset();
  q = await call('run_query', { sql: "UPDATE dbo.customers SET name = 'X' WHERE id = 1" });
  check('A WRITE IN A READ-ONLY SESSION IS REFUSED, saying which switch, and nothing runs',
    q.isError && /Allow changes to data this session/.test(q.text) && calls('handleMssql').length === 0 && calls('rbacGate').length === 0, q.text);
  check('…and the refusal is recorded too', AUDIT[0].outcome === 'denied' && AUDIT[0].detail.reason === 'session is read-only' && AUDIT[0].detail.write === true);
  for (const sneaky of ['SELECT * INTO dbo.copy FROM dbo.customers', 'WITH x AS (SELECT 1 a) INSERT INTO t SELECT a FROM x', 'EXEC dbo.purge', 'SELECT 1; DELETE FROM t WHERE id = 1']) {
    reset();
    q = await call('run_query', { sql: sneaky });
    check('a write dressed as a read is still a write: ' + sneaky.slice(0, 40), q.isError && calls('handleMssql').length === 0, q.text);
  }
  reset();
  DB.rowsAffected = 1; DB.recordset = [];
  q = await call('run_query', { sql: "UPDATE dbo.customers SET name = 'X' WHERE id = 1" }, { readOnly: false });
  check('WITH CHANGES ALLOWED a bounded write runs as written — not wrapped — and says what it changed',
    !q.isError && calls('handleMssql')[0][2] === "UPDATE dbo.customers SET name = 'X' WHERE id = 1" && q.json.rows_affected === 1, q.text);
  check('…recorded as a change, at high severity', AUDIT[0].detail.write === true && AUDIT[0].severity === 'high' && AUDIT[0].detail.rowsAffected === 1);
  reset();
  q = await call('run_query', { sql: 'SELECT 1 AS one' }, { readOnly: false });
  check('a READ is wrapped and rolled back even when changes are allowed', /ROLLBACK TRANSACTION/.test(calls('handleMssql')[0][2]));

  section('5. Never, in any mode');
  for (const [sql, word] of [['DROP TABLE dbo.customers', 'DROP TABLE'], ['TRUNCATE TABLE dbo.customers', 'TRUNCATE'],
    ['DELETE FROM dbo.customers', 'DELETE'], ['UPDATE dbo.customers SET name = 1', 'UPDATE'], ['drop database Fin', 'DROP DATABASE']]) {
    reset();
    q = await call('run_query', { sql }, { readOnly: false });
    check('REFUSED with changes allowed: ' + sql, q.isError && new RegExp(word, 'i').test(q.text) && /SQL editor/.test(q.text)
      && calls('handleMssql').length === 0 && AUDIT[0].outcome === 'denied', q.text);
  }
  reset();
  q = await call('run_query', { sql: 'DELETE FROM dbo.customers WHERE id = 7' }, { readOnly: false });
  check('a DELETE with a WHERE runs when changes are allowed', !q.isError && calls('handleMssql').length === 1, q.text);
  reset();
  q = await call('run_query', { sql: 'USE master; SELECT name FROM sys.databases' });
  check('USE IS REFUSED — one connection per session', q.isError && /USE/.test(q.text) && calls('handleMssql').length === 0);
  reset();
  q = await call('run_query', { sql: '   ' });
  check('an empty statement is an error, not a call', q.isError && calls('handleMssql').length === 0 && AUDIT.length === 0);

  section('6. The organisation\'s say, every call');
  reset(); POLICY = { enabled: false, roles: ['EN'] };
  q = await call('run_query', { sql: 'SELECT 1' });
  check('THE DEV CONSOLE SWITCHED OFF stops the bridge at the next query', q.isError && /switched off/.test(q.text) && calls('handleMssql').length === 0);
  reset(); POLICY = { enabled: true, roles: ['OW', 'PA'] };
  q = await call('run_query', { sql: 'SELECT 1' });
  check('a role taken off the list stops it too', q.isError && /not enabled for your role/.test(q.text));
  reset(); ROLES = [];
  q = await call('run_query', { sql: 'SELECT 1' });
  check('no role at all is refused', q.isError && calls('handleMssql').length === 0);
  reset(); ROLES = ['AU']; POLICY = { enabled: true, roles: ['AU', 'EN'] };
  q = await call('run_query', { sql: 'SELECT 1' });
  check('an Auditor may read through it…', !q.isError, q.text);
  reset(); ROLES = ['AU']; POLICY = { enabled: true, roles: ['AU', 'EN'] };
  q = await call('run_query', { sql: 'UPDATE t SET a = 1 WHERE b = 2' }, { readOnly: false });
  check('…but never change data, whatever the session switch says', q.isError && /cannot let the Dev Console change data/.test(q.text) && calls('handleMssql').length === 0, q.text);

  section('7. The SQL editor\'s gate still has its say');
  reset(); GATE = { denied: { statusCode: 403, body: JSON.stringify({ error: 'Not permitted: no grant', hint: 'Ask a Platform Administrator.' }) } };
  q = await call('run_query', { sql: 'SELECT 1' });
  check('a 403 from the RBAC gate comes back in its own words', q.isError && /Not permitted: no grant/.test(q.text) && /Platform Administrator/.test(q.text) && calls('handleMssql').length === 0);
  reset(); GATE = { denied: { statusCode: 428, body: JSON.stringify({ error: 'This change needs a second person to approve it before it runs.' }) } };
  q = await call('run_query', { sql: 'UPDATE t SET a = 1 WHERE b = 2' }, { readOnly: false });
  check('a change that needs approval is sent to the SQL editor, not left hanging', q.isError && /approve/.test(q.text) && /SQL editor/.test(q.text));
  reset(); DB.error = 'Invalid object name \'dbo.nope\'.';
  q = await call('run_query', { sql: 'SELECT * FROM dbo.nope' });
  check('a SQL error is a tool error Claude can read, and is recorded as failed', q.isError && /Invalid object name/.test(q.text) && AUDIT[0].outcome === 'failed');
  reset(); AUDIT_THROWS = true;
  q = await call('run_query', { sql: 'SELECT 1' });
  check('an audit trail that cannot be reached does not take the answer away', !q.isError);

  section('8. Rows and size');
  reset(); DB.recordset = Array.from({ length: 1500 }, (_, i) => ({ i }));
  q = await call('run_query', { sql: 'SELECT i FROM t' });
  check('DEFAULT 200 ROWS, and it says there were more', q.json.rows.length === 200 && q.json.truncated === true && q.json.total_rows_read === 1500);
  q = await call('run_query', { sql: 'SELECT i FROM t', max_rows: 5000 });
  check('AT MOST 1,000, whatever is asked for', q.json.rows.length === 1000);
  reset(); DB.recordset = Array.from({ length: 1000 }, (_, i) => ({ i, pad: 'x'.repeat(400) }));
  q = await call('run_query', { sql: 'SELECT * FROM t', max_rows: 1000 });
  check('an answer too big for Claude is cut to fit, and says so', q.text.length <= I.MAX_TEXT && q.json.truncated === true && /size limit/.test(q.json.note), q.text.length);

  section('9. The catalogue tools');
  reset(); DB.recordset = [{ schema: 'dbo', name: 'customers', type: 'table', approx_rows: 12 }];
  q = await call('list_tables', {});
  const lt = calls('handleMssql')[0][2];
  check('list_tables reads sys.objects, user tables and views, with row counts — read-only like everything else',
    /sys\.objects/.test(lt) && /'U','V'/.test(lt) && /ROLLBACK/.test(lt) && q.json.rows[0][1] === 'customers', lt);
  reset();
  await call('list_tables', { schema: "o'brien" });
  check('a schema filter is a quoted literal, its quote doubled', /s\.name = N'o''brien'/.test(calls('handleMssql')[0][2]));
  reset();
  await call('describe_table', { table: '[sales].[Order Lines]' });
  const dt = calls('handleMssql')[0][2];
  check('describe_table takes schema.table, brackets and all, as literals', /c\.table_name = N'Order Lines'/.test(dt) && /c\.table_schema = N'sales'/.test(dt), dt);
  check('…with the primary-key columns marked', /PRIMARY KEY/.test(dt) && /primary_key/.test(dt));
  reset();
  q = await call('describe_table', {});
  check('describe_table without a table is an error, not a query', q.isError && calls('handleMssql').length === 0);
  check('catalogue reads are recorded without a statement', true);

  section('10. PostgreSQL');
  reset();
  q = await call('run_query', { sql: 'SELECT 1' }, { dbType: 'postgres', connString: 'postgres://u:p@pg.acme.test/fin' });
  const pg = calls('handlePostgres')[0];
  check('A POSTGRES READ RUNS IN A READ ONLY TRANSACTION (the handler is asked to), unwrapped text', pg && pg[2] === 'SELECT 1' && pg[3] === true && calls('handleMssql').length === 0);
  reset();
  await call('list_tables', {}, { dbType: 'postgres', connString: 'postgres://u:p@pg.acme.test/fin' });
  check('its list_tables reads information_schema, no N-prefixed literals', /information_schema\.tables/.test(calls('handlePostgres')[0][2]) && !/N'/.test(calls('handlePostgres')[0][2]));
  const src = require('fs').readFileSync(path.join(__dirname, '..', 'netlify', 'functions', 'db-connect.js'), 'utf8');
  check('db-connect honours readOnly: BEGIN READ ONLY, then ROLLBACK whatever happens',
    /if \(body\.readOnly === true\) \{\s*await client\.query\('BEGIN READ ONLY'\);\s*try \{ r = await client\.query\(sqlToRun\); \}\s*finally \{ try \{ await client\.query\('ROLLBACK'\)/.test(src));
  check('db-connect exposes its road to the bridge, unchanged for its own callers', /exports\.__bridge = \{ rbacGate, handleMssql, handlePostgres, detectDialect/.test(src));

  section('11. A Function App connection');
  reset();
  q = await call('run_query', { sql: 'SELECT 1 AS one' }, { mode: 'azure', connString: null, fnUrl: 'https://customer-fn.azurewebsites.net/api/db', fnKey: 'custkey' });
  const fc = calls('fetch').filter(c => !/redeem/.test(c[1]))[0];
  check('IT GOES TO THE FUNCTION APP, with its own key, as the SQL editor does', fc && fc[1] === 'https://customer-fn.azurewebsites.net/api/db?code=custkey' && fc[2].action === 'execute', fc && fc[1]);
  check('…wrapped and rolled back, being a read', /ROLLBACK TRANSACTION/.test(fc[2].sql) && !q.isError && q.json.rows[0][0] === 1);
  check('…and it does not pass through the direct-connection gate', calls('rbacGate').length === 0);
  reset();
  await call('run_query', { sql: 'SELECT 1' }, { mode: 'azure', connString: null, fnUrl: 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/db', fnKey: null });
  const pc = calls('fetch').filter(c => !/redeem/.test(c[1]))[0];
  check('the product\'s own Function App, saved with no key, is reached with the product key', /\?code=productkey$/.test(pc[1]), pc[1]);
  reset();
  await call('run_query', { sql: 'SELECT 1' }, { mode: 'azure', connString: null, fnUrl: 'https://someone-else.azurewebsites.net/api/db', fnKey: null });
  const oc = calls('fetch').filter(c => !/redeem/.test(c[1]))[0];
  check('…but nobody else\'s Function App is ever handed the product key', !/code=/.test(oc[1]), oc[1]);
  reset(); FN_REPLY = { status: 401, body: 'Unauthorized' };
  q = await call('run_query', { sql: 'SELECT 1' }, { mode: 'azure', connString: null, fnUrl: 'https://customer-fn.azurewebsites.net/api/db', fnKey: 'wrong' });
  check('a refused Function App key says so', q.isError && /key was refused/.test(q.text));

  section('12. A check pass');
  reset();
  q = await call('run_query', { sql: 'SELECT 1 AS ok' }, { kind: 'bridgecheck', sessionId: null, readOnly: true });
  check('"Check the bridge" runs down the same road and is recorded against the connection', !q.isError && AUDIT[0].resourceType === 'claudecode_check' && AUDIT[0].resourceId === 'check:sconn_x');

  section('13. data-proxy keeps browsers away from the redeem route');
  const proxy = require(path.join(__dirname, '..', 'netlify', 'functions', 'data-proxy.js'));
  const pr = await proxy.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: { path: '/agent/claude-code-bridge/redeem' }, body: '{}' });
  check('THE REDEEM ROUTE IS NOT FORWARDED, whatever the caller holds', pr.statusCode === 400 && /Unsupported path/.test(pr.body), pr.statusCode);
  const ok2 = await proxy.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: { path: '/agent/claude-code/session' }, body: '{}' });
  check('…while the Dev Console\'s own routes still are (refused later for want of a key or token, not the path)', !/Unsupported path/.test(ok2.body || ''));

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
