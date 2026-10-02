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
let RELAY = false;            // the connection goes through the Entra relay
let TEMPLATE_REPLY = null;    // what the Function App's template read answers

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
  if (/claude-code-bridge\/template/.test(url)) {
    const r = TEMPLATE_REPLY || { status: 404, body: { error: 'This project has no Conversion Template yet.' } };
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
  viaRelay: () => RELAY,
  rbacGate: async (authed, action, dialect, cs, database, body, opts) => { CALLS.push(['rbacGate', authed, action, dialect, body.sql, opts]); return GATE || { guardrail: null }; },
  handleMssql: async (action, cs, database, body) => {
    CALLS.push(['handleMssql', action, body.sql, body.timeoutMs]);
    if (DB.error) return { statusCode: 500, body: JSON.stringify({ error: DB.error }) };
    if (DB.error) return { statusCode: 500, body: JSON.stringify({ error: DB.error }) };
    return { statusCode: 200, body: JSON.stringify({ success: true, recordset: DB.recordset, rowsAffected: DB.rowsAffected }) };
  },
  handlePostgres: async (action, cs, database, body) => {
    CALLS.push(['handlePostgres', action, body.sql, body.readOnly, body.transaction, body.timeoutMs]);
    return { statusCode: 200, body: JSON.stringify({ success: true, recordset: DB.recordset, rowsAffected: DB.rowsAffected }) };
  },
});

const reset = (redeemCtx) => {
  CALLS = []; AUDIT = []; AUDIT_THROWS = false; GATE = null; FN_REPLY = null; DB = { recordset: [{ n: 1 }], rowsAffected: 0 };
  RELAY = false; TEMPLATE_REPLY = null;
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
    q.isError && /"Allow changes" switch is off/.test(q.text) && calls('handleMssql').length === 0 && calls('rbacGate').length === 0, q.text);
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

  section('14. Every statement is cancelled on the server before the function gives up');
  reset();
  await call('run_query', { sql: 'SELECT 1' });
  check('a read carries the server-side time limit, inside the bridge\'s own budget',
    calls('handleMssql')[0][3] === I.STATEMENT_MS && I.STATEMENT_MS < 21000 && I.STATEMENT_MS >= 15000, calls('handleMssql')[0][3]);
  reset(); DB.error = 'Cancelled after 19 seconds: the statement took too long and was stopped on the server.';
  q = await call('run_query', { sql: 'SELECT COUNT(*) FROM dbo.big' });
  check('a cancelled read says to narrow it', q.isError && /Narrow it/.test(q.text), q.text);

  section('15. A staging session');
  const STG = { stagingSchema: 'staging', projectId: 'proj_1', readOnly: true };
  reset(STG);
  r = await post(rpc('tools/list'));
  let tl = JSON.parse(r.body).result.tools;
  const rq = tl.find(t => t.name === 'run_query');
  check('tools/list offers the template tool, and run_query describes the staging rule',
    tl.map(t => t.name).join() === 'list_tables,describe_table,run_query,get_conversion_template'
    && /inside the schema "staging"/.test(rq.description) && /everything else in the database is read-only/.test(rq.description), tl.map(t => t.name).join());
  r = await post(rpc('initialize', { protocolVersion: '2025-06-18' }));
  check('initialize says it is a staging session', /Staging session: changes are allowed inside the schema "staging" only/.test(JSON.parse(r.body).result.instructions));
  reset({ projectId: '' });
  r = await post(rpc('tools/list'));
  check('a session with no project has no template tool', JSON.parse(r.body).result.tools.length === 3);

  reset(STG);
  q = await call('run_query', { sql: 'INSERT INTO staging.Matter (Number) SELECT m.Num FROM dbo.Matters m' });
  const sg = calls('rbacGate')[0], sm = calls('handleMssql')[0];
  check('A STAGING WRITE RUNS although "Allow changes" is off', !q.isError && calls('handleMssql').length === 1, q.text);
  check('…through the SQL editor\'s gate, asking it to lift the approvals for THIS schema only',
    sg && sg[5] && sg[5].stagingSchema === 'staging' && sg[4] === 'INSERT INTO staging.Matter (Number) SELECT m.Num FROM dbo.Matters m', JSON.stringify(sg && sg[5]));
  check('…inside a transaction with XACT_ABORT, so a cancel undoes it, with the time limit',
    /^SET XACT_ABORT ON;\nBEGIN TRANSACTION;\nINSERT INTO staging\.Matter/.test(sm[2]) && /IF @@TRANCOUNT > 0 COMMIT TRANSACTION;$/.test(sm[2]) && sm[3] === I.STATEMENT_MS, sm[2]);
  check('…and is on the trail as a staging change, at notice, naming the schema',
    AUDIT[0].outcome === 'allowed' && AUDIT[0].severity === 'notice' && AUDIT[0].detail.stagingSchema === 'staging' && AUDIT[0].detail.write === true
    && /staging change/.test(AUDIT[0].summary), JSON.stringify(AUDIT[0]));
  reset(STG);
  q = await call('run_query', { sql: 'CREATE SCHEMA staging' });
  check('CREATE SCHEMA is sent on its own (it must be first in its batch)', !q.isError && calls('handleMssql')[0][2] === 'CREATE SCHEMA staging', calls('handleMssql')[0] && calls('handleMssql')[0][2]);
  reset(STG);
  q = await call('run_query', { sql: 'TRUNCATE TABLE staging.Matter' });
  check('TRUNCATE inside the staging schema runs — rebuilding staging is the point', !q.isError && calls('handleMssql').length === 1, q.text);
  reset(STG);
  q = await call('run_query', { sql: 'DROP TABLE staging.Matter' });
  check('…and so does DROP TABLE', !q.isError && calls('handleMssql').length === 1, q.text);
  reset(STG);
  q = await call('run_query', { sql: 'UPDATE dbo.Matters SET Open = 0 WHERE Id = 1' });
  check('A WRITE ANYWHERE ELSE IS REFUSED with the reason, never reaching the gate or the database',
    q.isError && /outside the staging schema "staging"/.test(q.text) && calls('rbacGate').length === 0 && calls('handleMssql').length === 0, q.text);
  check('…and the refusal is on the trail', AUDIT[0].outcome === 'denied' && /^staging: /.test(AUDIT[0].detail.reason), JSON.stringify(AUDIT[0]));
  reset(STG);
  q = await call('run_query', { sql: 'INSERT INTO staging.t SELECT 1 DELETE FROM dbo.x WHERE 1 = 1' });
  check('a second write hidden after the staging one is refused', q.isError && calls('handleMssql').length === 0, q.text);
  reset(STG);
  q = await call('run_query', { sql: 'SELECT TOP 5 * FROM dbo.Matters' });
  check('a READ in a staging session is still wrapped and rolled back, and asks for no exemption',
    !q.isError && /ROLLBACK TRANSACTION/.test(calls('handleMssql')[0][2]) && !calls('rbacGate')[0][5], q.text);
  reset(STG);
  q = await call('run_query', { sql: 'USE master' });
  check('USE is still refused', q.isError && /USE/.test(q.text) && calls('handleMssql').length === 0);
  reset(STG); DB.error = 'Cancelled after 19 seconds: the statement took too long and was stopped on the server.';
  q = await call('run_query', { sql: 'INSERT INTO staging.big SELECT * FROM dbo.big' });
  check('A CANCELLED STAGING LOAD SAYS NOTHING WAS KEPT, and to load in slices', q.isError && /Nothing it changed was kept/.test(q.text) && /slices/.test(q.text), q.text);
  reset(STG); RELAY = true;
  q = await call('run_query', { sql: 'INSERT INTO staging.t SELECT 1' });
  check('a staging write over the Entra relay (which cannot be cancelled) is refused before the gate', q.isError && /relay/.test(q.text) && calls('rbacGate').length === 0, q.text);
  reset(Object.assign({}, STG, { mode: 'azure', connString: null, fnUrl: 'https://customer-fn.azurewebsites.net/api/db', fnKey: 'k' }));
  q = await call('run_query', { sql: 'INSERT INTO staging.t SELECT 1' });
  check('…and so is one through a Function App', q.isError && /Function App/.test(q.text) && calls('fetch').filter(c => !/redeem/.test(c[1])).length === 0, q.text);
  reset(Object.assign({}, STG, { dbType: 'postgres', connString: 'postgres://u:p@h/db', stagingSchema: 'staging' }));
  q = await call('run_query', { sql: 'INSERT INTO staging.t (a) SELECT a FROM public.x' });
  const pgc = calls('handlePostgres')[0];
  check('PostgreSQL: a staging write runs in a transaction with the time limit, unwrapped text',
    !q.isError && pgc[2] === 'INSERT INTO staging.t (a) SELECT a FROM public.x' && pgc[3] === false && pgc[4] === true && pgc[5] === I.STATEMENT_MS, JSON.stringify(pgc));
  check('the wrapper is what it says', I.wrapStagingMssql('INSERT INTO staging.t SELECT 1;') === 'SET XACT_ABORT ON;\nBEGIN TRANSACTION;\nINSERT INTO staging.t SELECT 1\n;\nIF @@TRANCOUNT > 0 COMMIT TRANSACTION;'
    && I.wrapStagingMssql('  create schema staging') === '  create schema staging');

  section('16. The Conversion Template');
  const spec = require(path.join(__dirname, '..', 'public', 'cygenix-template-spec.js'));
  const shapes = [
    { dataType: 'nvarchar', maxLength: 128 }, { dataType: 'nvarchar', maxLength: -1 }, { dataType: 'nchar', maxLength: 2 },
    { dataType: 'varchar', maxLength: 50 }, { dataType: 'varbinary', maxLength: -1 }, { dataType: 'decimal', precision: 18, scale: 2 },
    { dataType: 'numeric', precision: 9 }, { dataType: 'datetime2', scale: 7 }, { dataType: 'time', scale: 0 }, { dataType: 'int' },
    { dataType: 'INT' }, { type: 'bit' }, { dataType: 'decimal(10,4)' }, {}, { dataType: 'uniqueidentifier' },
  ];
  check('THE BRIDGE\'S sqlType IS THE TEMPLATE PAGE\'S, on every column shape', shapes.every(c => I.sqlType(c) === spec.sqlType(c)),
    shapes.map(c => I.sqlType(c) + '/' + spec.sqlType(c)).join(' '));
  const verdicts = [{ isIdentity: true }, { isComputed: true }, { isNullable: false }, { isNullable: true }, {}];
  check('…and so are populateVerdict and moduleActive', verdicts.every(c => I.populateVerdict(c) === spec.populateVerdict(c))
    && [{ inScope: true, included: true }, { inScope: false, included: true }, { included: false }, {}, null].every(m => I.moduleActive(m) === spec.moduleActive(m)));

  const TPL = { id: 'tpl_1', name: 'Finance', version: 2, status: 'published', profileId: 'DEMO', modules: [
    { module: 'Billing', inScope: true, included: true, tables: [
      { stagingTable: 'Invoice', targetTable: 'Invoice', loadOrder: 2, required: true, notes: 'one row per bill', columns: [
        { name: 'InvIndex', dataType: 'int', isNullable: false, isIdentity: true, isPrimaryKey: true },
        { name: 'InvNumber', dataType: 'nvarchar', maxLength: 64, isNullable: false },
        { name: 'Total', dataType: 'decimal', precision: 18, scale: 2, isNullable: true },
        { name: 'Calc', dataType: 'int', isComputed: true } ] },
      { stagingTable: 'Client', targetTable: 'Client', loadOrder: 1, columns: [{ name: 'Name', dataType: 'nvarchar', maxLength: 200 }] } ] },
    { module: 'Time', inScope: true, included: false, tables: [{ stagingTable: 'Timecard', targetTable: 'Timecard', columns: [] }] },
  ] };
  const TEMPLATES = [{ id: 'pub_tpl_1_v2', templateId: 'tpl_1', kind: 'published', name: 'Finance', version: 2, profileId: 'DEMO' },
    { id: 'tpl_1', templateId: 'tpl_1', kind: 'draft', name: 'Finance', version: 3, profileId: 'DEMO' }];
  reset(STG); TEMPLATE_REPLY = { status: 200, body: { ok: true, templates: TEMPLATES, chosen: TEMPLATES[0], template: TPL } };
  q = await call('get_conversion_template', {});
  const tf = calls('fetch').find(c => /claude-code-bridge\/template/.test(c[1]));
  check('THE OVERVIEW: the template, the others, the modules ticked for publish, tables in load order',
    !q.isError && q.json.template.id === 'pub_tpl_1_v2' && q.json.template.version === 2 && q.json.other_templates.length === 1
    && q.json.modules.length === 1 && q.json.modules[0].module === 'Billing'
    && q.json.modules[0].tables.map(t => t.staging_table).join() === 'Client,Invoice' && q.json.modules[0].tables[1].columns === 4
    && q.json.staging_schema === 'staging' && /module or a table/.test(q.json.next), q.text.slice(0, 400));
  check('…read from the Function App with the host key and THE SESSION\'S OWN PASS, nothing else',
    tf && /\/agent\/claude-code-bridge\/template\?code=hostkey$/.test(tf[1]) && tf[2].token === 'cyb_' + 'a'.repeat(18) + '.' + 'b'.repeat(43) && tf[2].templateId === '', tf && JSON.stringify(tf[2]));
  check('…and recorded as a template read, without the pass', AUDIT[0].action === 'claudecode.query' && AUDIT[0].detail.tool === 'get_conversion_template'
    && /template read/.test(AUDIT[0].summary) && JSON.stringify(AUDIT).indexOf('b'.repeat(43)) === -1, JSON.stringify(AUDIT[0]));
  q = await call('get_conversion_template', { table: 'invoice' });
  const inv = q.json.tables && q.json.tables[0];
  check('A TABLE: its columns, typed as the template page types them, required and identity marked',
    inv && inv.staging_table === 'Invoice' && inv.columns.map(c => c.name + ':' + c.type).join() === 'InvIndex:int,InvNumber:nvarchar(32),Total:decimal(18,2),Calc:int'
    && inv.columns[0].identity === true && inv.columns[0].populate === 'No — identity' && inv.columns[1].required_in_target === true
    && inv.columns[1].populate === 'Required' && inv.notes === 'one row per bill', q.text.slice(0, 500));
  check('…with a CREATE TABLE for the staging schema: every column NULL, the computed one left out',
    inv.create_table === "IF OBJECT_ID(N'staging.Invoice', N'U') IS NULL CREATE TABLE [staging].[Invoice] ([InvIndex] int NULL, [InvNumber] nvarchar(32) NULL, [Total] decimal(18,2) NULL)", inv.create_table);
  const stagingSql = require(path.join(__dirname, '..', 'netlify', 'functions', 'lib', 'staging-sql.js'));
  check('AND THAT CREATE TABLE PASSES THE STAGING CHECK it will meet', stagingSql.checkStagingWrite(inv.create_table, { schema: 'staging', dialect: 'sqlserver' }).ok === true);
  q = await call('get_conversion_template', { module: 'Billing' });
  check('a module gives all its tables', q.json.tables.length === 2 && q.json.tables[0].staging_table === 'Client');
  q = await call('get_conversion_template', { module: 'Nope' });
  check('an unknown module says so and how to find the list', /No module "Nope"/.test(q.json.error));
  await call('get_conversion_template', { template_id: 'tpl_1' });
  check('another template is asked for by id', calls('fetch').filter(c => /template/.test(c[1])).pop()[2].templateId === 'tpl_1');

  reset(Object.assign({}, STG, { dbType: 'postgres', connString: 'postgres://u:p@h/db' }));
  TEMPLATE_REPLY = { status: 200, body: { ok: true, templates: TEMPLATES, chosen: TEMPLATES[0], template: TPL } };
  q = await call('get_conversion_template', { table: 'Client' });
  check('PostgreSQL: a CREATE TABLE IF NOT EXISTS in its quoting, that passes its check, and a note about types',
    q.json.tables[0].create_table === 'CREATE TABLE IF NOT EXISTS "staging"."Client" ("Name" nvarchar(100) NULL)'
    && stagingSql.checkStagingWrite(q.json.tables[0].create_table, { schema: 'staging', dialect: 'postgres' }).ok === true && /adjust/.test(q.json.types_note), q.text);

  const NONE = JSON.parse(JSON.stringify(TPL)); NONE.modules.forEach(m => { m.included = false; });
  reset(STG); TEMPLATE_REPLY = { status: 200, body: { ok: true, templates: TEMPLATES, chosen: TEMPLATES[1], template: NONE } };
  q = await call('get_conversion_template', {});
  check('a draft with nothing ticked "Include" shows every module in scope, and says so',
    q.json.modules.length === 2 && /No module is ticked/.test(q.json.note), q.text.slice(0, 300));
  const BIG = { modules: [{ module: 'M', inScope: true, included: true, tables: [] }] };
  for (let i = 0; i < 60; i++) BIG.modules[0].tables.push({ stagingTable: 'T' + i, targetTable: 'T' + i, columns: Array.from({ length: 40 }, (x, k) => ({ name: 'Column_' + k + '_with_a_long_name', dataType: 'nvarchar', maxLength: 400 })) });
  reset(STG); TEMPLATE_REPLY = { status: 200, body: { ok: true, templates: [], chosen: { id: 'x' }, template: BIG } };
  q = await call('get_conversion_template', { module: 'M' });
  check('A MODULE TOO BIG FOR ONE ANSWER is cut, naming the tables left to ask for one at a time',
    q.text.length < I.MAX_TEXT && q.json.tables.length < 60 && q.json.not_shown.tables.length === 60 - q.json.tables.length && /one table at a time/.test(q.json.not_shown.why), q.text.length);
  reset(STG);
  q = await call('get_conversion_template', {});
  check('a project with no template says so', q.isError && /no Conversion Template yet/.test(q.text));
  reset({ projectId: '' });
  q = await call('get_conversion_template', {});
  check('a session with no project says so, and nothing is fetched', q.isError && /not opened from a project/.test(q.text) && !calls('fetch').some(c => /template/.test(c[1])));
  reset(STG); POLICY = { enabled: false, roles: [] };
  q = await call('get_conversion_template', {});
  check('the organisation\'s switch applies to the template read too', q.isError && /switched off/.test(q.text));


  /* ── 17. The target, read-only, and the rules (Oct-2026) ───────────────── */
  section('17. The target, read-only; the Was/Is rules and Parameters');
  const REF = { connectionId: 'sconn_tgt', connectionName: 'Target PROD', side: 'tgt', dbType: 'sqlserver', mode: 'direct',
    connString: 'Server=tgt.acme.test;Database=Fin;User Id=u;Password=tp', fnUrl: null, fnKey: null };
  const SRCSTG = { side: 'src', connectionName: 'Legacy', connString: 'Server=src.acme.test;Database=Old;User Id=u;Password=p', stagingSchema: 'stg',
    projectId: 'proj_1', readOnly: true, reference: REF, rules: { wasis: 2, params: 2 } };
  reset(SRCSTG);
  let lr = JSON.parse((await post(rpc('tools/list'))).body);
  const names = lr.result.tools.map(t => t.name);
  check('A SESSION WITH A REFERENCE AND RULES LISTS the three target tools and the rules tool, beside its own',
    ['list_tables', 'describe_table', 'run_query', 'get_conversion_template', 'target_list_tables', 'target_describe_table', 'target_query', 'get_translation_rules']
      .every(n => names.indexOf(n) !== -1), names.join());
  check('…and the target tools say read-only', I.TARGET_TOOLS.every(t => t.annotations.readOnlyHint === true) && /READ-ONLY/.test(I.TARGET_TOOLS[2].description));
  reset({});
  lr = JSON.parse((await post(rpc('tools/list'))).body);
  check('…a session without them lists none of them', !lr.result.tools.some(t => /^target_|get_translation_rules/.test(t.name)));

  reset(SRCSTG);
  DB = { recordset: [{ code: 'A' }, { code: 'I' }], rowsAffected: 0 };
  q = await call('target_query', { sql: 'SELECT code FROM dbo.StatusCode' });
  const mq = calls('handleMssql')[0], gq = calls('rbacGate')[0];
  check('TARGET_QUERY RUNS A READ ON THE TARGET: through the SQL editor\'s own gate, inside a transaction that is rolled back',
    !q.isError && q.json.rows.length === 2 && /ROLLBACK TRANSACTION/.test(mq[2]) && /SELECT code FROM dbo\.StatusCode/.test(mq[2]) && gq[4] === 'SELECT code FROM dbo.StatusCode' && !gq[5], q.text);
  check('…on the TARGET\'s connection, and audited under its name, as a target read', AUDIT.length === 1 && AUDIT[0].detail.connection === 'Target PROD'
    && AUDIT[0].detail.tool === 'target_query' && /target read on Target PROD/.test(AUDIT[0].summary) && AUDIT[0].detail.write === false && AUDIT[0].detail.sql === 'SELECT code FROM dbo.StatusCode', JSON.stringify(AUDIT[0]));
  reset(SRCSTG);
  q = await call('target_query', { sql: 'INSERT INTO dbo.StatusCode (code) VALUES (\'X\')' });
  check('ANYTHING BUT A READ ON THE TARGET IS REFUSED before it is sent — even in a staging session', q.isError && /target is read-only/.test(q.text)
    && !calls('handleMssql').length && !calls('rbacGate').length && AUDIT[0].outcome === 'denied' && AUDIT[0].detail.connection === 'Target PROD');
  reset(SRCSTG);
  q = await call('target_query', { sql: 'INSERT INTO stg.T (a) SELECT 1' });
  check('…including a write naming the staging schema: the staging rule is the session\'s own database only', q.isError && /target is read-only/.test(q.text) && !calls('handleMssql').length);
  reset(SRCSTG);
  q = await call('target_query', { sql: 'DROP TABLE dbo.Client' });
  check('…and DROP is refused there as everywhere', q.isError && !calls('handleMssql').length);
  reset(SRCSTG);
  q = await call('target_list_tables', {});
  check('target_list_tables lists the target\'s tables, read the same protected way', !q.isError && /sys\.objects/.test(calls('handleMssql')[0][2]) && AUDIT[0].detail.tool === 'target_list_tables');
  reset(SRCSTG);
  q = await call('target_describe_table', { table: 'dbo.Client' });
  check('target_describe_table describes one', !q.isError && /c\.table_name = N'Client'/.test(calls('handleMssql')[0][2]) && /c\.table_schema = N'dbo'/.test(calls('handleMssql')[0][2]));
  reset(Object.assign({}, SRCSTG, { reference: { connectionName: 'Target PROD', error: 'The credential for "Target PROD" is no longer saved on the server.' } }));
  q = await call('target_query', { sql: 'SELECT 1' });
  check('a target whose credential has gone: the tool says why, and nothing runs', q.isError && /no longer saved/.test(q.text) && !calls('handleMssql').length);
  reset(SRCSTG); POLICY = { enabled: false, roles: [] };
  q = await call('target_query', { sql: 'SELECT 1' });
  check('the organisation\'s switch applies to the target too', q.isError && /switched off/.test(q.text) && !calls('handleMssql').length);
  reset(Object.assign({}, SRCSTG, { reference: Object.assign({}, REF, { dbType: 'postgres', connString: 'postgres://u:p@tgt/fin' }) }));
  q = await call('target_query', { sql: 'SELECT code FROM status_code' });
  const pq = calls('handlePostgres')[0];
  check('a PostgreSQL target: read-only, no transaction write', !q.isError && pq[3] === true && pq[4] === false, JSON.stringify(pq));
  reset({});
  q = await call('target_query', { sql: 'SELECT 1' });
  check('a session with no reference has no target tools at all', !!q.body.error && /Unknown tool/.test(q.body.error.message));
  // The session's own run_query is unchanged by having a reference.
  reset(SRCSTG);
  q = await call('run_query', { sql: 'SELECT COUNT(*) AS n FROM dbo.Client' });
  check('the session\'s own run_query still runs on the session\'s own database', !q.isError && AUDIT[0].detail.connection === 'Legacy');

  // The rules.
  const RULES = { ok: true, wasis: [
      { table: 'dbo.client', field: 'status', from: 'A', to: 'Active', note: 'status codes' },
      { table: 'dbo.client', field: 'status', from: 'I', to: 'Inactive' },
      { table: 'dbo.matter', field: 'type', from: 'L', to: 'Litigation' }],
    params: [{ name: 'Cut off', code: '@@Cutoff', type: 'date', value: '2018-01-01' }, { name: 'Company', code: '@@Co', type: 'number', value: '7' },
      { name: 'Who', code: '@@Who', type: 'raw', value: 'SUSER_SNAME()' }, { name: 'Odd', code: '@@Odd', type: 'text', value: "O'Brien" }],
    totals: { wasis: 3, params: 4 } };
  let RULES_REPLY = { status: 200, body: RULES };
  const realFetch = D.fetch;
  D.fetch = async (url, init) => {
    if (/claude-code-bridge\/rules/.test(url)) { CALLS.push(['fetch', url, JSON.parse(init.body)]); return { status: RULES_REPLY.status, text: async () => JSON.stringify(RULES_REPLY.body) }; }
    return realFetch(url, init);
  };
  reset(SRCSTG);
  q = await call('get_translation_rules', {});
  check('THE OVERVIEW: every Parameter, with the SQL it stands for, and how many rules each source table and field has',
    !q.isError && q.json.parameters.length === 4 && q.json.parameters[0].token === '@@Cutoff' && q.json.parameters[0].sql === "'2018-01-01'"
    && q.json.parameters[1].sql === '7' && q.json.parameters[2].sql === 'SUSER_SNAME()' && q.json.parameters[3].sql === "'O''Brien'"
    && q.json.was_is.length === 2 && q.json.was_is[0].table === 'dbo.client' && q.json.was_is[0].rules === 2, q.text);
  const rf = calls('fetch').find(c => /claude-code-bridge\/rules/.test(c[1]));
  check('…read from the Function App with the session\'s own pass and the host key', !!rf && /code=hostkey/.test(rf[1]) && /^cyb_/.test(rf[2].token));
  check('…audited as a rules read, with no statement', AUDIT[0].detail.tool === 'get_translation_rules' && /rules read/.test(AUDIT[0].summary) && AUDIT[0].detail.sql === undefined);
  q = await call('get_translation_rules', { table: 'Client' });
  check('ONE TABLE\'S RULES, found by its bare name too', q.json.rules.length === 2 && q.json.rules[0].from === 'A' && q.json.rules[0].to === 'Active');
  q = await call('get_translation_rules', { table: 'dbo.Client', field: 'Status' });
  check('…and one field\'s', q.json.rules.length === 2);
  q = await call('get_translation_rules', { table: 'dbo.Nope' });
  check('…a table with none says so', q.json.rules.length === 0 && /No Was\/Is rules/.test(q.json.note));
  require(path.join(__dirname, '..', 'public', 'cygenix-params.js'));        // attaches itself to the global
  const P = globalThis.CygenixParams;
  const ps = [{ type: 'number', value: '7' }, { type: 'number', value: 'abc' }, { type: 'text', value: "a'b" }, { type: 'date', value: '2020-01-01' },
    { type: 'raw', value: 'GETDATE()' }, { value: '12' }, { value: 'x' }, { type: 'weird', value: '3' }, {}];
  check('THE BRIDGE\'S paramLiteral IS THE PARAMETERS MODULE\'S formatValue, on every shape', ps.every(p => I.paramLiteral(p) === P.formatValue(p)),
    ps.map(p => I.paramLiteral(p) + '/' + P.formatValue(p)).join(' '));
  reset(Object.assign({}, SRCSTG, { rules: null }));
  q = await call('get_translation_rules', {});
  check('a session opened without rules says so', q.isError && /without Was\/Is rules/.test(q.text));
  reset(SRCSTG); RULES_REPLY = { status: 404, body: { error: 'This session was opened without Was/Is rules or Parameters.' } };
  q = await call('get_translation_rules', {});
  check('the store\'s refusal is passed on in words', q.isError && /opened without/.test(q.text));
  D.fetch = realFetch;
  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
