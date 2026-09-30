/* tests/claude-code.test.js — the Claude Code connection test, end to end
 * ---------------------------------------------------------------------------
 * The console is built on Anthropic Managed Agents, and before any of it is
 * switched on an administrator checks that Anthropic's workspace can reach
 * the database at all. This file pins every layer of that check:
 *
 *   1. the Netlify gate — who may, the organisation policy, what is audited;
 *   2. the policy shape, the permission rows and the audit category;
 *   3. the Azure routes — registration, strict identity, the gate call and
 *      its cache, the caller's key and only theirs, the session it creates
 *      (spend cap, allow-list, web tools off, owner stamp), reading the
 *      result back, somebody else's session, cleanup, Anthropic's errors;
 *   4. the Python probe itself, run for real against local fake servers —
 *      a SQL Server that answers PRELOGIN, a PostgreSQL that answers
 *      SSLRequest, a port that accepts and says nothing, a closed port;
 *   5. the browser module — reading host and port out of every connection
 *      string shape, and nothing else;
 *   6. the page and the house rules around it.
 *
 * Run it:  node tests/claude-code.test.js
 */
'use strict';

const fs = require('fs');
const path = require('path');
const net = require('net');
const Module = require('module');
const { execFile } = require('child_process');

const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra !== undefined ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

// ── Loading the Azure module with its runtime stubbed ────────────────────
const ROUTES = {};
const realRequire = Module.prototype.require;
Module.prototype.require = function (id) {
  if (id === '@azure/functions') return { app: { http: (name, spec) => { ROUTES[name] = spec; } } };
  if (id === './entra-auth') return { verifyJwt: async () => ({}), enforceAuth: async () => ({ ok: true }) };
  return realRequire.apply(this, arguments);
};
const CC = require(path.join(ROOT, 'azure-function', 'src', 'claude-code.js'));
Module.prototype.require = realRequire;

// ── Loading the Netlify gate with authz stubbed ──────────────────────────
const tenancy = require(path.join(ROOT, 'netlify', 'functions', 'lib', 'tenancy.js'));
const rbac = require(path.join(ROOT, 'netlify', 'functions', 'lib', 'rbac.js'));
const schema = require(path.join(ROOT, 'netlify', 'functions', 'lib', 'audit-schema.js'));
let ACTOR = { oid: 'o1', email: 'a@x', roles: [] };
let TENANT = { id: 'tn_1' };
const AUDITED = [];
Module.prototype.require = function (id) {
  if (id === './lib/authz') return {
    authorize: async (event) => {
      const h = event.headers || {};
      if (!h.authorization) { const e = new Error('no token'); e.statusCode = 401; throw e; }
      return { actor: ACTOR, tenant: TENANT, audit: async (e) => { AUDITED.push(e); } };
    },
    errorResponse: (e, headers) => ({ statusCode: e.statusCode || 500, headers, body: JSON.stringify({ error: e.message }) }),
  };
  return realRequire.apply(this, arguments);
};
const GATE = require(path.join(ROOT, 'netlify', 'functions', 'claude-code-gate.js'));
Module.prototype.require = realRequire;

const P = require(path.join(ROOT, 'public', 'cygenix-cc-probe.js'));

const gateCall = async (act, extra) => {
  AUDITED.length = 0;
  const r = await GATE.handler(Object.assign({ httpMethod: 'POST', headers: { authorization: 'Bearer t' },
    body: JSON.stringify(Object.assign({ act }, extra || {})) }));
  let body = {}; try { body = JSON.parse(r.body); } catch (e) { /* */ }
  return { status: r.statusCode, body };
};

// ── A fake Anthropic client, recording what it is asked ──────────────────
function fakeClient(opts) {
  const o = Object.assign({ agents: [], envs: [], session: null, events: [], envCreate409: false }, opts || {});
  const calls = [];
  const pages = (items) => ({ [Symbol.asyncIterator]: async function* () { for (const i of items) yield i; } });
  const client = {
    calls,
    beta: {
      agents: {
        list: () => { calls.push(['agents.list']); return pages(o.agents); },
        create: async (p) => { calls.push(['agents.create', p]); return { id: 'agent_new' }; },
        update: async (id, p) => { calls.push(['agents.update', id, p]); return { id }; },
      },
      environments: {
        list: () => { calls.push(['environments.list']); return pages(o.envs); },
        create: async (p) => {
          calls.push(['environments.create', p]);
          if (o.envCreate409) { o.envs.push({ id: 'env_raced', name: p.name }); const e = new Error('exists'); e.status = 409; throw e; }
          return { id: 'env_new', name: p.name };
        },
      },
      sessions: {
        create: async (p) => { calls.push(['sessions.create', p]); return { id: 'sesn_011probe', status: 'running' }; },
        retrieve: async (id) => { calls.push(['sessions.retrieve', id]); if (!o.session) { const e = new Error('nf'); e.status = 404; throw e; } return o.session; },
        archive: async (id) => { calls.push(['sessions.archive', id]); return {}; },
        events: { list: (id, params) => { calls.push(['events.list', id, params]); return pages(o.events); } },
      },
    },
  };
  return client;
}

const KEY = 'sk-ant-test-key-000000000000';
const who = { ok: true, oid: 'oid-me', bearer: 'Bearer tok' };
let FETCHED = [];
let GATE_REPLY = { status: 200, body: { allowed: true, roles: ['OW'] } };
CC.deps.fetch = async (url, init) => {
  FETCHED.push({ url, init });
  if (GATE_REPLY.throws) throw new Error('getaddrinfo ENOTFOUND');
  return { status: GATE_REPLY.status, text: async () => JSON.stringify(GATE_REPLY.body) };
};
let CLIENT = fakeClient();
const KEYS_SEEN = [];
CC.deps.makeClient = (k) => { KEYS_SEEN.push(k); return CLIENT; };

// A request, as the Functions runtime hands it over.
function req(method, opts) {
  const o = opts || {};
  const headers = Object.assign({}, o.headers || {});
  return {
    method,
    headers: { get: (k) => headers[k.toLowerCase()] || null },
    query: { get: (k) => (o.query || {})[k] || null },
    json: async () => { if (o.body === undefined) throw new Error('no body'); return o.body; },
  };
}
const logs = [];
const ctx = { log: (...a) => logs.push(a.join(' ')) };

function runPython(script, env) {
  return new Promise((resolve) => {
    execFile('python3', ['-c', script], { timeout: 30000, env }, (err, stdout, stderr) => {
      const m = /CYGPROBE_RESULT (\{.*\})/.exec(stdout || '');
      resolve({ err, stdout, stderr, result: m ? JSON.parse(m[1]) : null });
    });
  });
}
function listen(onConn) {
  return new Promise((resolve) => {
    const s = net.createServer(onConn);
    s.listen(0, '127.0.0.1', () => resolve(s));
  });
}

(async () => {
  console.log('Claude Code — the connection test\n');

  /* ── 1. The gate ────────────────────────────────────────────────────── */
  section('1. The Netlify gate');
  {
    ACTOR = { oid: 'o1', roles: [] };
    let r = await gateCall('probe');
    check('a caller with no role is refused the connection test (403)', r.status === 403, r.status);
    check('…and the refusal is audited', AUDITED.length === 1 && AUDITED[0].outcome === 'denied' && AUDITED[0].action === 'claudecode.configure');
    for (const role of ['ML', 'EN', 'AU', 'MB']) {
      ACTOR = { oid: 'o1', roles: [role] };
      r = await gateCall('probe');
      check('the connection test refuses ' + role, r.status === 403, r.status);
    }
    ACTOR = { oid: 'o1', roles: ['PA'] };
    r = await gateCall('probe', { detail: { host: 'db.acme.io', port: 1433, secret: 'nope' } });
    check('a Platform Administrator may run it', r.status === 200 && r.body.allowed === true, JSON.stringify(r.body));
    check('a result check is NOT audited (only the start is)', AUDITED.length === 0, JSON.stringify(AUDITED));
    r = await gateCall('probe', { record: true, detail: { host: 'db.acme.io', port: 1433, secret: 'nope', password: 'x' } });
    const rec = AUDITED[0] || {};
    check('the start IS audited as claudecode.probe, with host and port', rec.action === 'claudecode.probe' && rec.outcome === 'allowed'
      && rec.detail.host === 'db.acme.io' && rec.detail.port === '1433', JSON.stringify(rec));
    check('…and nothing the gate does not know about reaches the trail', !('secret' in rec.detail) && !('password' in rec.detail));
    ACTOR = { oid: 'o1', roles: ['OW'] };
    check('an Organisation Owner may run it', (await gateCall('probe')).status === 200);

    TENANT = { id: 'tn_1' };
    ACTOR = { oid: 'o1', roles: ['OW'] };
    r = await gateCall('use');
    check('THE CONSOLE IS OFF BY DEFAULT — even for an Owner', r.status === 403 && /switched off/.test(r.body.error), JSON.stringify(r.body));
    TENANT = { id: 'tn_1', claudeCode: { enabled: true } };
    check('switched on, the default allow-list admits OW…', (await gateCall('use')).status === 200);
    ACTOR = { oid: 'o1', roles: ['PA'] };
    check('…and PA', (await gateCall('use')).status === 200);
    ACTOR = { oid: 'o1', roles: ['EN'] };
    r = await gateCall('use');
    check('…and not an Engineer, with the sentence the brief asked for',
      r.status === 403 && r.body.error === 'Not enabled for your role — ask an Owner to enable it in Governance.', JSON.stringify(r.body));
    TENANT = { id: 'tn_1', claudeCode: { enabled: true, roles: ['OW', 'PA', 'EN', 'AU'] } };
    check('an Engineer on the allow-list may use it', (await gateCall('use')).status === 200);
    check('…and let it change data', (await gateCall('changes')).status === 200);
    ACTOR = { oid: 'o1', roles: ['AU'] };
    check('an Auditor on the allow-list may use it read-only', (await gateCall('use')).status === 200);
    r = await gateCall('changes');
    check('BUT NOT LET IT CHANGE DATA (A-22: no mutating grant for the Auditor)', r.status === 403, JSON.stringify(r.body));
    ACTOR = { oid: 'o1', roles: ['MB'] };
    check('a Member is never admitted, whatever the list says',
      (TENANT = { id: 't', claudeCode: { enabled: true, roles: ['OW', 'PA', 'ML', 'EN', 'AU', 'MB'] } }, (await gateCall('use')).status === 403));
    check('an unknown act is a 400', (await gateCall('nonsense')).status === 400);
    const anon = await GATE.handler({ httpMethod: 'POST', headers: {}, body: '{"act":"probe"}' });
    check('no token is a 401', anon.statusCode === 401);
    check('GET is refused', (await GATE.handler({ httpMethod: 'GET', headers: { authorization: 'Bearer t' } })).statusCode === 405);
  }

  /* ── 2. Policy, permissions, audit category ─────────────────────────── */
  section('2. Policy shape, permission rows, audit category');
  {
    const d = tenancy.normaliseClaudeCode(undefined);
    check('the policy defaults to OFF with OW and PA', d.enabled === false && d.roles.join() === 'OW,PA', JSON.stringify(d));
    check('only enabled:true switches it on', tenancy.normaliseClaudeCode({ enabled: 'yes' }).enabled === false
      && tenancy.normaliseClaudeCode({ enabled: true }).enabled === true);
    check('roles outside the five are dropped, not stored',
      tenancy.normaliseClaudeCode({ roles: ['EN', 'MB', 'SP', 'OW', 'x'] }).roles.join() === 'OW,EN');
    check('an explicit empty list means nobody', tenancy.normaliseClaudeCode({ enabled: true, roles: [] }).roles.length === 0);
    check('configure is OW and PA; the Auditor reads it',
      rbac.can({ roles: ['OW'] }, 'claudecode.configure', { mutating: true }).allow
      && rbac.can({ roles: ['PA'] }, 'claudecode.configure', { mutating: true }).allow
      && !rbac.can({ roles: ['AU'] }, 'claudecode.configure', { mutating: true }).allow
      && !rbac.can({ roles: ['ML'] }, 'claudecode.configure', { mutating: true }).allow);
    check('use is capped at the five roles the brief names',
      ['OW', 'PA', 'ML', 'EN', 'AU'].every(r => rbac.can({ roles: [r] }, 'claudecode.use', {}).allow)
      && ['AP', 'DO', 'VA', 'MB', 'SP'].every(r => !rbac.can({ roles: [r] }, 'claudecode.use', {}).allow));
    check('Claude Code events file under security, which cannot be switched off',
      schema.categoryFor('claudecode.probe') === 'security' && schema.categoryFor('claudecode.session.start') === 'security');
  }

  /* ── 3. The Azure routes ────────────────────────────────────────────── */
  section('3. The Azure routes');
  {
    const spec = ROUTES['claude-code-probe'] || {};
    check('the route is registered with app.http under agent/, function-key level',
      spec.route === 'agent/claude-code/probe' && spec.authLevel === 'function' && spec.methods.join() === 'GET,POST,OPTIONS');
    const proxy = read('netlify', 'functions', 'data-proxy.js');
    check('data-proxy already allows the path, so it needed no change',
      /\^\\\/agent\(\\\/\[A-Za-z0-9_\.-\]\+\)\*\$/.test(proxy) && /^\/agent(\/[A-Za-z0-9_.-]+)*$/.test('/agent/claude-code/probe'));
    check('index.js imports the module', /require\('\.\/claude-code'\);/.test(read('azure-function', 'src', 'index.js')));
    check('no function.json folder was created for it', !fs.existsSync(path.join(ROOT, 'azure-function', 'claude-code-probe')));
    check('host.json carries no functionTimeout', !/functionTimeout/.test(read('azure-function', 'host.json')));
    check('the SDK is a version with Managed Agents in it',
      /"@anthropic-ai\/sdk": "\^0\.(1[2-9]\d|[2-9]\d\d)\./.test(read('azure-function', 'package.json')));

    const H = spec.handler;
    check('OPTIONS is a 204', (await H(req('OPTIONS'), ctx)).status === 204);
    let r = await H(req('POST', { headers: { 'x-anthropic-key': KEY }, body: {} }), ctx);
    check('NO TOKEN, NO TEST — even with REQUIRE_TOKEN_AUTH off (401)', r.status === 401, r.status);
    CC.deps.verify = async () => { throw new Error('bad signature'); };
    r = await H(req('POST', { headers: { authorization: 'Bearer x', 'x-anthropic-key': KEY }, body: {} }), ctx);
    check('a token that does not verify is a 401 with the reason', r.status === 401 && /bad signature/.test(r.body), r.body);
    CC.deps.verify = async () => ({ oid: 'oid-me' });
    r = await H(req('POST', { headers: { authorization: 'Bearer x' }, body: { host: 'db.acme.io', port: 1433 } }), ctx);
    check('no Anthropic key is the house 400 (USER_KEY_REQUIRED), and nothing is called',
      r.status === 400 && /USER_KEY_REQUIRED/.test(r.body) && FETCHED.length === 0 && KEYS_SEEN.length === 0, r.body);
    r = await H(req('POST', { headers: { authorization: 'Bearer x', 'x-anthropic-key': KEY }, body: { host: 'https://db.acme.io', port: 1433 } }), ctx);
    check('a host with a scheme is refused before anything is asked', r.status === 400 && FETCHED.length === 0);
    r = await H(req('POST', { headers: { authorization: 'Bearer x', 'x-anthropic-key': KEY }, body: { host: 'db.acme.io', port: 70000 } }), ctx);
    check('so is a port out of range', r.status === 400);

    // The gate.
    CC._reset(); FETCHED = [];
    GATE_REPLY = { status: 403, body: { error: 'Only an Organisation Owner or Platform Administrator can run the Claude Code connection test.' } };
    r = await H(req('POST', { headers: { authorization: 'Bearer x', 'x-anthropic-key': KEY }, body: { host: 'db.acme.io', port: 1433 } }), ctx);
    check('the gate is asked, with the CALLER\'S token, and its no is passed on word for word',
      r.status === 403 && /Owner or Platform Administrator/.test(r.body) && FETCHED[0].init.headers.Authorization === 'Bearer x'
      && FETCHED[0].url === 'https://cygenix.co.uk/.netlify/functions/claude-code-gate', FETCHED[0] && FETCHED[0].url);
    check('…and nothing reaches Anthropic', KEYS_SEEN.length === 0);
    process.env.CYGENIX_SITE_URL = 'https://staging.example.test/';
    check('CYGENIX_SITE_URL is honoured (trailing slash trimmed)', CC.siteUrl() === 'https://staging.example.test');
    delete process.env.CYGENIX_SITE_URL;
    GATE_REPLY = { throws: true };
    r = await CC.gate(who, 'use');
    check('an unreachable gate is a 503 naming the setting to check', !r.ok && r.response.status === 503 && /CYGENIX_SITE_URL/.test(r.response.body));
    GATE_REPLY = { status: 200, body: { allowed: true } };
    FETCHED = [];
    await CC.gate(who, 'use'); await CC.gate(who, 'use'); await CC.gate(who, 'use');
    check('a yes is cached: three checks inside 30s cost one gate call', FETCHED.length === 1, FETCHED.length);
    await CC.gate(who, 'probe', { record: true }); await CC.gate(who, 'probe', { record: true });
    check('an act to be recorded is never served from the cache', FETCHED.length === 3, FETCHED.length);
    GATE_REPLY = { status: 403, body: { error: 'no' } };
    const t0 = Date.now(); CC.deps.now = () => t0 + 31000;
    r = await CC.gate(who, 'use');
    check('after 30s the gate is asked again, and a no is not cached', !r.ok && FETCHED.length === 4);
    CC.deps.now = () => Date.now();

    // Starting a test.
    CC._reset(); FETCHED = []; KEYS_SEEN.length = 0; logs.length = 0;
    GATE_REPLY = { status: 200, body: { allowed: true } };
    CLIENT = fakeClient();
    r = await H(req('POST', { headers: { authorization: 'Bearer x', 'x-anthropic-key': KEY },
      body: { host: 'DB.Acme.io', port: 1433, kind: 'sqlserver', network: 'limited' } }), ctx);
    const out = JSON.parse(r.body);
    check('a test starts and returns the session id', r.status === 200 && out.sessionId === 'sesn_011probe' && out.host === 'db.acme.io', r.body);
    check('the Anthropic client is built from the caller\'s key and nothing else', KEYS_SEEN.length === 1 && KEYS_SEEN[0] === KEY);
    const gateBody = JSON.parse(FETCHED[0].init.body);
    check('the gate is told to record it, with host, port and network', gateBody.act === 'probe' && gateBody.record === true
      && gateBody.detail.host === 'db.acme.io' && gateBody.detail.network === 'limited');
    const created = CLIENT.calls.find(c => c[0] === 'agents.create');
    check('with no agent in the account, ONE is created', !!created && CLIENT.calls.filter(c => c[0] === 'agents.create').length === 1);
    const tools = created[1].tools[0];
    check('the agent has the toolset with web search and fetch switched OFF',
      tools.type === 'agent_toolset_20260401'
      && tools.configs.some(c => c.name === 'web_search' && c.enabled === false)
      && tools.configs.some(c => c.name === 'web_fetch' && c.enabled === false));
    check('and carries metadata to find it by next time', created[1].metadata.cygenix === 'claude-code' && created[1].metadata.spec === CC.AGENT_SPEC);
    const env = (CLIENT.calls.find(c => c[0] === 'environments.create') || [])[1] || {};
    check('the environment is LIMITED to the host and the address echo, nothing else',
      env.config.type === 'cloud' && env.config.networking.type === 'limited'
      && env.config.networking.allowed_hosts.join() === 'db.acme.io,api.ipify.org'
      && env.config.networking.allow_package_managers === false && env.config.networking.allow_mcp_servers === false, JSON.stringify(env));
    check('its name carries a hash, not the database host', /^cygenix-cc-limited-[0-9a-f]{16}$/.test(env.name) && !/acme/.test(env.name));
    const s = (CLIENT.calls.find(c => c[0] === 'sessions.create') || [])[1] || {};
    check('THE SESSION CARRIES THE SPEND CAP: 600 cents USD (about £5)',
      s.budget && s.budget.type === 'limit' && s.budget.max_list_cost.amount === '600' && s.budget.max_list_cost.currency === 'USD', JSON.stringify(s.budget));
    check('it is stamped with the caller, so nobody else can read it', s.metadata.cyg_oid === 'oid-me' && s.metadata.cygenix === 'probe');
    check('it overrides the agent for this one run: low effort, the probe prompt',
      s.agent.type === 'agent_with_overrides' && s.agent.model.effort === 'low' && s.agent.system === CC.PROBE_SYSTEM);
    check('it starts working in the same call (initial_events)', s.initial_events.length === 1 && /CYGPROBE_RESULT/.test(s.initial_events[0].content[0].text));
    check('THE KEY APPEARS NOWHERE IN WHAT WAS SENT TO ANTHROPIC OR THE GATE',
      !JSON.stringify(CLIENT.calls).includes(KEY) && !JSON.stringify(FETCHED).includes(KEY));
    check('the log line carries method, status and time — no host, no key',
      logs.length === 1 && /probe POST status=200 ms=\d+/.test(logs[0]) && !/acme|sk-ant/.test(logs[0]), logs[0]);

    // A second test reuses both.
    CLIENT = fakeClient({ agents: [{ id: 'agent_old', metadata: { cygenix: 'claude-code', spec: '0' } }], envs: [] });
    CC._reset(); GATE_REPLY = { status: 200, body: { allowed: true } };
    await CC.probeStart(null, who, KEY, { host: 'db.acme.io', port: 1433, kind: 'sqlserver', network: 'open' });
    check('an existing agent is found, not duplicated — and updated in place when its spec is older',
      !CLIENT.calls.some(c => c[0] === 'agents.create') && CLIENT.calls.some(c => c[0] === 'agents.update' && c[1] === 'agent_old'));
    const openEnv = (CLIENT.calls.find(c => c[0] === 'environments.create') || [])[1];
    check('the open-networking test gets an unrestricted environment', openEnv.config.networking.type === 'unrestricted');
    const n0 = CLIENT.calls.length;
    await CC.probeStart(null, who, KEY, { host: 'db.acme.io', port: 1433, kind: 'sqlserver', network: 'open' });
    check('a repeat inside ten minutes asks Anthropic for nothing but the session',
      CLIENT.calls.slice(n0).map(c => c[0]).join() === 'sessions.create', CLIENT.calls.slice(n0).map(c => c[0]).join());
    CC._reset();
    CLIENT = fakeClient({ agents: [{ id: 'agent_cur', metadata: { cygenix: 'claude-code', spec: CC.AGENT_SPEC } }], envCreate409: true });
    await CC.probeStart(null, who, KEY, { host: 'db2.acme.io', port: 5432, kind: 'postgres', network: 'limited' });
    const sc = (CLIENT.calls.find(c => c[0] === 'sessions.create') || [])[1];
    check('a race on the environment name (409) uses the winner\'s environment', sc && sc.environment_id === 'env_raced');
    check('a current agent is left alone', !CLIENT.calls.some(c => c[0] === 'agents.update'));

    // Reading the result.
    const line = 'CYGPROBE_RESULT {"host":"db.acme.io","port":1433,"kind":"sqlserver","dns":["10.0.0.4"],"tcp":"open","handshake":"sqlserver-replied","tcp_ms":41,"egress_ip":"203.0.113.9"}';
    CLIENT = fakeClient({
      session: { id: 'sesn_011probe', status: 'idle', metadata: { cygenix: 'probe', cyg_oid: 'oid-me' }, usage: { list_cost: { amount: '3', currency: 'USD' } } },
      events: [
        { type: 'user.message', content: [{ type: 'text', text: 'run it' }] },
        { type: 'agent.tool_use', name: 'bash' },
        { type: 'agent.tool_result', content: [{ type: 'text', text: 'x\n' + line + '\n' }] },
        { type: 'agent.message', content: [{ type: 'text', text: line }] },
        { type: 'session.status_idle', stop_reason: { type: 'end_turn' } },
      ],
    });
    r = await H(req('GET', { headers: { authorization: 'Bearer x', 'x-anthropic-key': KEY }, query: { sessionId: 'sesn_011probe' } }), ctx);
    const res = JSON.parse(r.body);
    check('the result is read back from the script\'s own output', r.status === 200 && res.done === true
      && res.result.handshake === 'sqlserver-replied' && res.result.egress_ip === '203.0.113.9', r.body);
    check('with a plain-language verdict and the cost', res.verdict.ok === true && /answered/.test(res.verdict.text) && res.costCents === 3);
    check('a finished test session is archived (routine cleanup)', CLIENT.calls.some(c => c[0] === 'sessions.archive'));
    check('events are read oldest first', CLIENT.calls.find(c => c[0] === 'events.list')[2].order === 'asc');

    CLIENT = fakeClient({ session: { id: 'sesn_x', status: 'idle', metadata: { cygenix: 'probe', cyg_oid: 'somebody-else' } } });
    r = await H(req('GET', { headers: { authorization: 'Bearer x', 'x-anthropic-key': KEY }, query: { sessionId: 'sesn_xxxxxx' } }), ctx);
    check('SOMEBODY ELSE\'S TEST IN A SHARED ANTHROPIC ACCOUNT IS NOT FOUND', r.status === 404 && !CLIENT.calls.some(c => c[0] === 'events.list'), r.body);
    CLIENT = fakeClient({ session: null });
    r = await H(req('GET', { headers: { authorization: 'Bearer x', 'x-anthropic-key': KEY }, query: { sessionId: 'sesn_gone00' } }), ctx);
    check('an unknown session is a 404', r.status === 404);
    r = await H(req('GET', { headers: { authorization: 'Bearer x', 'x-anthropic-key': KEY }, query: { sessionId: '../../x' } }), ctx);
    check('a malformed session id is a 400', r.status === 400);
    CLIENT = fakeClient({ session: { id: 's', status: 'running', metadata: { cygenix: 'probe', cyg_oid: 'oid-me' } }, events: [{ type: 'session.status_running' }] });
    r = await H(req('GET', { headers: { authorization: 'Bearer x', 'x-anthropic-key': KEY }, query: { sessionId: 'sesn_run00' } }), ctx);
    check('a running test says not done, and is not archived', JSON.parse(r.body).done === false && !CLIENT.calls.some(c => c[0] === 'sessions.archive'));

    // Anthropic's refusals.
    CLIENT = fakeClient();
    CLIENT.beta.agents.list = () => { const e = new Error('invalid x-api-key'); e.status = 401; throw e; };
    CC._reset();
    r = await H(req('POST', { headers: { authorization: 'Bearer x', 'x-anthropic-key': KEY }, body: { host: 'db.acme.io', port: 1433 } }), ctx);
    check('Anthropic refusing the key is a 401 in words', r.status === 401 && /did not accept your API key/.test(r.body), r.body);
    CLIENT.beta.agents.list = () => { throw new TypeError('boom here'); };
    CC._reset();
    r = await H(req('POST', { headers: { authorization: 'Bearer x', 'x-anthropic-key': KEY }, body: { host: 'db.acme.io', port: 1433 } }), ctx);
    const b = JSON.parse(r.body);
    check('anything else is a 500 carrying the message AND the stack (house rule)', r.status === 500 && b.error === 'boom here' && /claude-code/.test(b.stack));

    check('the budget setting is honoured and a bad one falls back to 600',
      (process.env.CLAUDE_CODE_BUDGET_CENTS = '250', CC.budget().max_list_cost.amount === '250')
      && (process.env.CLAUDE_CODE_BUDGET_CENTS = '6.00', CC.budget().max_list_cost.amount === '600'));
    delete process.env.CLAUDE_CODE_BUDGET_CENTS;

    // The verdicts.
    check('verdict: DNS failure', /could not resolve/.test(CC.verdict({ host: 'h', dns_error: 'x' }).text));
    check('verdict: connection failed', CC.verdict({ host: 'h', port: 1, tcp: 'failed' }).ok === false);
    check('verdict: a connection nobody answered on is NOT a pass',
      CC.verdict({ host: 'h', port: 1, kind: 'sqlserver', tcp: 'open', handshake: 'no-reply' }).ok === false);
    check('verdict: the database answered', CC.verdict({ host: 'h', port: 1, kind: 'postgres', tcp: 'open', handshake: 'postgres-replied' }).ok === true);
    check('the agent\'s reply is the fallback when the tool result lacks the line',
      CC.parseProbeEvents([{ type: 'agent.message', content: [{ type: 'text', text: 'CYGPROBE_RESULT {"tcp":"open"}' }] }]).result.tcp === 'open');
    check('session errors are collected', CC.parseProbeEvents([{ type: 'session.error', error: { type: 'billing_error', message: 'credit balance too low' } }]).errors[0] === 'credit balance too low');
  }

  /* ── 4. The probe, for real ─────────────────────────────────────────── */
  section('4. The Python probe, against local fake servers');
  {
    const env = Object.assign({}, process.env);
    ['HTTPS_PROXY', 'https_proxy', 'HTTP_PROXY', 'http_proxy'].forEach(k => delete env[k]);
    // The address echo would reach the internet; point it at a closed port
    // so the test is quick and offline. Everything else runs as shipped.
    const script = (p) => CC.probeScript(p).replace('https://' + CC.IP_ECHO_HOST, 'https://127.0.0.1:9');

    const mssql = await listen((sock) => sock.once('data', (d) => {
      // A PRELOGIN request must arrive; answer with a type-0x04 packet header.
      sock.end(d[0] === 0x12 ? Buffer.from([0x04, 0x01, 0x00, 0x08, 0x00, 0x00, 0x01, 0x00]) : Buffer.alloc(0));
    }));
    let r = await runPython(script({ host: '127.0.0.1', port: mssql.address().port, kind: 'sqlserver' }), env);
    check('SQL Server: a server that answers PRELOGIN is reported as answering',
      r.result && r.result.tcp === 'open' && r.result.handshake === 'sqlserver-replied', r.stderr || r.stdout);
    check('the address-echo failure is reported, not fatal', r.result && !!r.result.egress_ip_error && !r.result.egress_ip);
    mssql.close();

    const pg = await listen((sock) => sock.once('data', (d) => sock.end(d.length === 8 && d.readUInt32BE(4) === 80877103 ? 'N' : '?')));
    r = await runPython(script({ host: '127.0.0.1', port: pg.address().port, kind: 'postgres' }), env);
    check('PostgreSQL: a server that answers SSLRequest is reported as answering',
      r.result && r.result.handshake === 'postgres-replied', r.stderr || r.stdout);
    pg.close();

    const mute = await listen((sock) => sock.end());
    r = await runPython(script({ host: '127.0.0.1', port: mute.address().port, kind: 'sqlserver' }), env);
    check('A PORT THAT ACCEPTS AND SAYS NOTHING IS NOT MISTAKEN FOR A DATABASE',
      r.result && r.result.tcp === 'open' && r.result.handshake === 'no-reply' && CC.verdict(r.result).ok === false, JSON.stringify(r.result));
    mute.close();

    const closed = await listen(() => {}); const port = closed.address().port; closed.close();
    await new Promise(res => setTimeout(res, 50));
    r = await runPython(script({ host: '127.0.0.1', port, kind: 'postgres' }), env);
    check('a closed port is a failed connection, with the error', r.result && r.result.tcp === 'failed' && /Refused|refused/.test(r.result.tcp_error), JSON.stringify(r.result));
    check('the shipped script asks the real address-echo service', /https:\/\/api\.ipify\.org/.test(CC.probeScript({ host: 'h', port: 1, kind: 'other' })));
    check('the instruction tells the agent to run it unchanged and run nothing else',
      /Do not change it and do not run anything else/.test(CC.probeInstruction({ host: 'h', port: 1, kind: 'other' })));
  }

  /* ── 5. The browser module ──────────────────────────────────────────── */
  section('5. Reading host and port from a connection string');
  {
    const e = (s, m) => P.endpointOf(s, m);
    let x = e('Server=tcp:acme.database.windows.net,1433;Database=x;User ID=u;Password=hunter2');
    check('Azure SQL ADO string', x.ok && x.host === 'acme.database.windows.net' && x.port === 1433 && x.kind === 'sqlserver');
    check('AND THE PASSWORD IS NOWHERE IN WHAT IS RETURNED', !JSON.stringify(x).includes('hunter2'));
    x = e('Data Source=SQL01\\PROD;Initial Catalog=x');
    check('a named instance is read, with a note about its port', x.ok && x.host === 'sql01' && x.port === 1433 && /Named instance/.test(x.note));
    x = e('mssql://u:p%40ss@db.acme.local:1444/x');
    check('an mssql:// URL, with an @ inside the password', x.ok && x.host === 'db.acme.local' && x.port === 1444);
    x = e('postgresql://u:secret@pg.acme.io/db');
    check('a postgres URL defaults to 5432', x.ok && x.kind === 'postgres' && x.port === 5432 && !JSON.stringify(x).includes('secret'));
    x = e("host=pg2.acme.io port=6432 dbname=x user=u password='s'");
    check('libpq keywords', x.ok && x.host === 'pg2.acme.io' && x.port === 6432 && x.kind === 'postgres');
    check('localhost is refused in words', !e('Server=.;Database=x').ok && /this computer/.test(e('Server=localhost;Database=x').why));
    check('a Function App connection says to enter the server by hand', !e('x', 'azure').ok && /Function App/.test(e('x', 'azure').why));
    check('an empty string says so', !e('').ok);
    check('validate: scheme, port and path are refused', !!P.validate('https://x.io', 1) && !!P.validate('x.io', 0) && !!P.validate('x.io/db', 1) && P.validate('x.io', 1433) === '');
    check('the firewall sentence is the brief\'s, with the address when known',
      /Your database may only accept known IP addresses — the Anthropic workspace may need allowing\./.test(P.firewallHelp({}))
      && /203\.0\.113\.9/.test(P.firewallHelp({ egress_ip: '203.0.113.9' })));
    check('cost is shown in dollars', P.costText(3) === 'US$0.03' && P.costText(null) === '');
  }

  /* ── 6. The page and the house rules ────────────────────────────────── */
  section('6. The page and the house rules');
  {
    const page = read('public', 'claude-code.html');
    check('the page loads the probe module and the sidebar', /src="\/cygenix-cc-probe\.js\?v=/.test(page) && /cygenix-sidebar\.js/.test(page));
    check('it is not in the menu yet — the console proper adds it with Develop',
      !/key:'claude-code'/.test(read('public', 'cygenix-sidebar.js')));
    check('one start at a time, 3 seconds apart, reset only in finally',
      /if \(CC\.starting\) return;/.test(page) && /CC\.lastStart \+ 3000/.test(page)
      && /finally \{\s*CC\.starting = false; renderButtons\(\);/.test(page) && (page.match(/CC\.starting = false/g) || []).length === 1);
    check('polling: one request in flight, every 3s, stops on done, error or 3 minutes',
      /if \(!p \|\| p\.inflight\) return;/.test(page) && /POLL_MS = 3000/.test(page) && /GIVE_UP_MS = 3 \* 60 \* 1000/.test(page)
      && (page.match(/stopPoll\(run\)/g) || []).length >= 3);
    check('a failed read ends the polling rather than retrying', /run\.status = 'error'; run\.error = e\.message; stopPoll\(run\);/.test(page));
    check('with no API key the buttons are off and the note links to Settings',
      /var block = CC\.starting \|\| !hasKey\(\);/.test(page) && /href="\/dashboard#project-settings"/.test(page));
    check('the page sends host, port, kind and network — and no connection string',
      /\{ host: host, port: port, kind: kind, network: network \}/.test(page) && !/connString\s*[,}]/.test(page.split('ccRun')[1] || ''));
    const src = read('azure-function', 'src', 'claude-code.js');
    check('the server reads the key only through userAnthropicKey', /userAnthropicKey\(req\)/.test(src) && !/process\.env\.ANTHROPIC_API_KEY/.test(src));
    check('no key or secret literal in any new file',
      [src, page, read('public', 'cygenix-cc-probe.js'), read('netlify', 'functions', 'claude-code-gate.js')].every(t => !/sk-ant-[A-Za-z0-9]/.test(t)));
    check('no 3E names in the new files (target-agnostic)',
      [src, page, read('public', 'cygenix-cc-probe.js')].every(t => !/\b3E\b|Elite|Timekeeper|Matter\b/.test(t)));
    check('the Assistant knows the page', /key: 'claude-code'/.test(read('public', 'cygenix-assistant-actions.js')));
  }

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
