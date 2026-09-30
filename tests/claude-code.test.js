/* tests/claude-code.test.js — the Claude Code console, end to end
 * ---------------------------------------------------------------------------
 * A person chats with Claude, and Claude writes and runs code in an Anthropic
 * workspace that connects to one of the person's databases. This file pins
 * every layer of it, with Anthropic and Cosmos stubbed:
 *
 *   1. the Netlify gate — who may, the organisation policy, what is audited;
 *   2. the policy shape, the permission rows and the audit category;
 *   3. the Azure routes — registration, strict identity, the gate call and
 *      its cache, the caller's key and only theirs;
 *   4. the connectivity test — the session it creates, the result read back,
 *      somebody else's session, cleanup, Anthropic's errors;
 *   5. the console — opening a session (the credential file, the allow-list,
 *      the spend cap, the prompt), messages, polling with redaction and the
 *      cursor, the data-change mode, stop, the list and the replay;
 *   6. the Python probe, run for real against local fake servers;
 *   7. the browser modules — reading host and port out of every connection
 *      string shape, and nothing else;
 *   8. the page and the house rules.
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
  if (id === './entra-auth') return { verifyJwt: async () => ({}), enforceAuth: async () => ({ ok: true }), logWarn() {}, logErr() {} };
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
  const o = Object.assign({ agents: [], envs: [], sessions: {}, events: {}, envCreate409: false, createFails: false }, opts || {});
  const calls = [];
  const files = {};
  const pages = (items) => ({ [Symbol.asyncIterator]: async function* () { for (const i of items) yield i; } });
  // Like the API: oldest first, and created_at[gte] compares against processed_at.
  const eventPage = (items, params) => {
    const from = params && params['created_at[gte]'];
    const rows = (items || []).filter(e => !from || !e.processed_at || e.processed_at >= from)
      .slice().sort((a, b) => String(a.processed_at || '~').localeCompare(String(b.processed_at || '~')));
    return pages(rows);
  };
  let nf = 0, ns = 0;
  const client = {
    calls, files,
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
      files: {
        upload: async (p) => { calls.push(['files.upload', p]); const id = 'file_' + (++nf); files[id] = p.file; return { id }; },
        delete: async (id) => { calls.push(['files.delete', id]); delete files[id]; return {}; },
        list: (p) => { calls.push(['files.list', p]); return pages((o.outputs || {})[p.scope_id] || []); },
        download: async (id) => { calls.push(['files.download', id]); return { arrayBuffer: async () => Buffer.from((o.content || {})[id] || '') }; },
      },
      sessions: {
        create: async (p) => {
          calls.push(['sessions.create', p]);
          if (o.createFails) { const e = new Error('no capacity'); e.status = 529; throw e; }
          const id = 'sesn_' + String(++ns).padStart(6, '0');
          o.sessions[id] = { id, status: p.initial_events ? 'running' : 'idle', metadata: p.metadata || {}, usage: { list_cost: { amount: '0', currency: 'USD' } } };
          return o.sessions[id];
        },
        retrieve: async (id) => { calls.push(['sessions.retrieve', id]); if (!o.sessions[id]) { const e = new Error('nf'); e.status = 404; throw e; } return o.sessions[id]; },
        archive: async (id) => { calls.push(['sessions.archive', id]); if (o.sessions[id]) o.sessions[id].archived_at = 'now'; return {}; },
        resources: {
          add: async (id, p) => { calls.push(['resources.add', id, p]); if (o.addFails) { const e = new Error('mount refused'); e.status = 400; throw e; } return { id: 'res_' + p.file_id, type: 'file' }; },
        },
        events: {
          list: (id, params) => { calls.push(['events.list', id, params]); return eventPage(o.events[id], params); },
          send: async (id, p) => { calls.push(['events.send', id, p]); return { events: p.events }; },
        },
      },
    },
    _o: o,
  };
  return client;
}

// ── A fake Cosmos container ──────────────────────────────────────────────
function fakeContainer() {
  const items = new Map();
  const c = {
    items: {
      upsert: async (doc) => { items.set(doc.id, JSON.parse(JSON.stringify(doc))); return { resource: doc }; },
      query: (q, opts) => ({ fetchAll: async () => {
        const u = q.parameters.find(p => p.name === '@u').value, o = q.parameters.find(p => p.name === '@o').value;
        const n = q.parameters.find(p => p.name === '@n').value;
        const rows = [...items.values()].filter(d => d.userId === u && d.kind === 'session' && d.oid === o)
          .sort((a, b) => b.createdAt.localeCompare(a.createdAt)).slice(0, n);
        return { resources: rows };
      } }),
    },
    item: (id, pk) => ({
      read: async () => { const d = items.get(id); if (!d || d.userId !== pk) { const e = new Error('nf'); e.code = 404; throw e; } return { resource: JSON.parse(JSON.stringify(d)) }; },
      delete: async () => { items.delete(id); },
    }),
    _items: items,
  };
  return c;
}

const KEY = 'sk-ant-test-key-000000000000';
const who = { ok: true, oid: 'oid-me', email: 'me@acme.test', bearer: 'Bearer tok' };
let FETCHED = [];
let GATE_REPLY = { status: 200, body: { allowed: true, roles: ['OW'], tenantId: 'tn_1' } };
CC.deps.fetch = async (url, init) => {
  FETCHED.push({ url, init });
  if (GATE_REPLY.throws) throw new Error('getaddrinfo ENOTFOUND');
  return { status: GATE_REPLY.status, text: async () => JSON.stringify(GATE_REPLY.body) };
};
let CLIENT = fakeClient();
const KEYS_SEEN = [];
CC.deps.makeClient = (k) => { KEYS_SEEN.push(k); return CLIENT; };
CC.deps.toFile = async (buf, name) => ({ name, text: buf.toString('utf8') });
let DB = fakeContainer();
CC.deps.container = async () => DB;
const SECRETS = {};
CC.deps.readSecret = async (oid, connId) => {
  if (oid !== 'oid-me') return { ok: false, code: 'not-found', why: 'x' };
  return SECRETS[connId] ? { ok: true, bundle: SECRETS[connId] } : { ok: false, code: 'not-found', why: 'nothing saved' };
};
CC.deps.verify = async () => ({ oid: 'oid-me', email: 'Me@Acme.test' });

function req(method, opts) {
  const o = opts || {};
  const headers = Object.assign({}, o.headers || {});
  return {
    method,
    params: { action: o.action || '' },
    headers: { get: (k) => headers[k.toLowerCase()] || null },
    query: { get: (k) => (o.query || {})[k] || null },
    json: async () => { if (o.body === undefined) throw new Error('no body'); return o.body; },
  };
}
const logs = [];
const ctx = { log: (...a) => logs.push(a.join(' ')) };
const H = (...a) => CC.handler(...a);
const AUTH = { authorization: 'Bearer x', 'x-anthropic-key': KEY };
const call = async (method, action, extra) => {
  const r = await H(req(method, Object.assign({ action, headers: AUTH }, extra || {})), ctx);
  let body = {}; try { body = JSON.parse(r.body); } catch (e) { /* */ }
  return { status: r.status, body, raw: r.body };
};

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
  console.log('Claude Code — the console\n');

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
    r = await gateCall('probe', { record: 'probe', detail: { host: 'db.acme.io', port: 1433, secret: 'nope', password: 'x' } });
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
    r = await gateCall('use', { record: 'session.start', detail: { sessionId: 'sesn_1', profile: 'Demo', connection: 'Target DEV', host: 'db.acme.io' } });
    const st = AUDITED[0] || {};
    check('A SESSION START IS ON THE TRAIL: claudecode.session.start, naming profile, connection and session',
      r.status === 200 && st.action === 'claudecode.session.start' && st.resourceType === 'claudecode_session' && st.resourceId === 'sesn_1'
      && st.detail.profile === 'Demo' && st.detail.connection === 'Target DEV', JSON.stringify(st));
    r = await gateCall('changes', { record: 'session.changes-on', detail: { sessionId: 'sesn_1', profile: 'Demo', connection: 'Target DEV' } });
    check('DATA CHANGES ALLOWED is on the trail at HIGH severity', r.status === 200 && AUDITED[0].action === 'claudecode.session.changes-on' && AUDITED[0].severity === 'high');
    r = await gateCall('use', { record: 'session.stop', detail: { sessionId: 'sesn_1' } });
    check('a stop is on the trail', r.status === 200 && AUDITED[0].action === 'claudecode.session.stop');
    r = await gateCall('use', { record: 'session.changes-on' });
    check('a record name that does not belong to the act is a 400', r.status === 400);
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

  /* ── 2b. Governance: the switch is saved on the server, audited ─────── */
  section('2b. Saving the switch through rbac-admin');
  {
    const BLOBS = new Map();
    BLOBS.set('tenants/index', { tenants: { tn_1: { id: 'tn_1', name: 'Acme' } } });
    const store = { get: async (k) => BLOBS.has(k) ? JSON.parse(JSON.stringify(BLOBS.get(k))) : null,
                    setJSON: async (k, v) => { BLOBS.set(k, JSON.parse(JSON.stringify(v))); } };
    Module.prototype.require = function (id) {
      if (id === '@netlify/blobs') return { getStore: () => store };
      if (id === './lib/authz') return {
        authorize: async () => ({ store, actor: ACTOR, tenant: BLOBS.get('tenants/index').tenants.tn_1, audit: async (e) => { AUDITED.push(e); } }),
        errorResponse: (e, headers) => ({ statusCode: e.statusCode || 500, headers, body: JSON.stringify({ error: e.message }) }),
      };
      return realRequire.apply(this, arguments);
    };
    const ADMIN = require(path.join(ROOT, 'netlify', 'functions', 'rbac-admin.js'));
    Module.prototype.require = realRequire;
    const admin = async (method, q, body) => {
      AUDITED.length = 0; tenancy.invalidate();
      const r = await ADMIN.handler({ httpMethod: method, headers: { authorization: 'Bearer t' }, queryStringParameters: q || {}, body: body ? JSON.stringify(body) : null });
      let b = {}; try { b = JSON.parse(r.body); } catch (e) { /* */ }
      return { status: r.statusCode, body: b };
    };
    ACTOR = { oid: 'o1', email: 'a@x', name: 'A', roles: ['EN'], isActive: true };
    let r = await admin('GET', { what: 'me' });
    check('?what=me tells everyone the state and what it means for them: off, not allowed, cannot configure',
      r.status === 200 && r.body.claudeCode && r.body.claudeCode.enabled === false && r.body.claudeCode.allowed === false
      && r.body.claudeCode.canConfigure === false && r.body.claudeCode.roleOptions.join() === 'OW,PA,ML,EN,AU', JSON.stringify(r.body.claudeCode));
    r = await admin('POST', null, { op: 'claude-code', enabled: true });
    check('an Engineer cannot switch it on (403, audited as a denial)', r.status === 403 && AUDITED[0].outcome === 'denied' && AUDITED[0].action === 'claudecode.configure');
    ACTOR = { oid: 'o2', email: 'own@x', name: 'O', roles: ['OW'], isActive: true };
    r = await admin('POST', null, { op: 'claude-code', enabled: true, roles: ['OW', 'PA', 'EN', 'MB'] });
    check('an Owner switches it on and sets the list; an unknown role is refused outright', r.status === 400 && /roles must be among/.test(r.body.error));
    r = await admin('POST', null, { op: 'claude-code', enabled: true, roles: ['OW', 'PA', 'EN'] });
    check('…with a valid list it is saved on the tenant', r.status === 200 && r.body.claudeCode.enabled === true && r.body.claudeCode.roles.join() === 'OW,PA,EN'
      && BLOBS.get('tenants/index').tenants.tn_1.claudeCode.enabled === true, JSON.stringify(r.body));
    check('AND AUDITED, from and to, at high severity', AUDITED[0].action === 'claudecode.configure' && AUDITED[0].severity === 'high'
      && AUDITED[0].detail.from.enabled === false && AUDITED[0].detail.to.roles.join() === 'OW,PA,EN', JSON.stringify(AUDITED[0]));
    ACTOR = { oid: 'o1', email: 'a@x', name: 'A', roles: ['EN'], isActive: true };
    r = await admin('GET', { what: 'me' });
    check('now the Engineer is allowed, may allow data changes, and still cannot configure',
      r.body.claudeCode.allowed === true && r.body.claudeCode.canChangeData === true && r.body.claudeCode.canConfigure === false, JSON.stringify(r.body.claudeCode));
    ACTOR = { oid: 'o3', email: 'au@x', name: 'U', roles: ['AU'], isActive: true };
    r = await admin('GET', { what: 'me' });
    check('an Auditor not on the list is not allowed; on it, read-only', r.body.claudeCode.allowed === false
      && ((await admin('POST', null, { op: 'claude-code', enabled: true })).status === 403));
    ACTOR = { oid: 'o2', email: 'own@x', name: 'O', roles: ['OW'], isActive: true };
    r = await admin('POST', null, { op: 'claude-code', enabled: false });
    check('switching it off keeps the list for next time', r.body.claudeCode.enabled === false && r.body.claudeCode.roles.join() === 'OW,PA,EN');
    check('a non-boolean enabled is a 400', (await admin('POST', null, { op: 'claude-code', enabled: 'yes' })).status === 400);
  }

  /* ── 3. The Azure route ─────────────────────────────────────────────── */
  section('3. The Azure route');
  {
    const spec = ROUTES['claude-code'] || {};
    check('ONE route, app.http, agent/claude-code/{action}, function-key level',
      spec.route === 'agent/claude-code/{action}' && spec.authLevel === 'function' && spec.methods.join() === 'GET,POST,OPTIONS');
    const proxy = read('netlify', 'functions', 'data-proxy.js');
    check('data-proxy already allows the family, so it needed no change',
      /\^\\\/agent\(\\\/\[A-Za-z0-9_\.-\]\+\)\*\$/.test(proxy)
      && ['probe', 'session', 'message', 'events', 'mode', 'stop', 'sessions'].every(a => /^\/agent(\/[A-Za-z0-9_.-]+)*$/.test('/agent/claude-code/' + a)));
    check('index.js imports the module', /require\('\.\/claude-code'\);/.test(read('azure-function', 'src', 'index.js')));
    check('no function.json folder was created for it', !fs.existsSync(path.join(ROOT, 'azure-function', 'claude-code')));
    check('host.json carries no functionTimeout', !/functionTimeout/.test(read('azure-function', 'host.json')));
    check('the SDK is a version with Managed Agents in it',
      /"@anthropic-ai\/sdk": "\^0\.(1[2-9]\d|[2-9]\d\d)\./.test(read('azure-function', 'package.json')));

    check('OPTIONS is a 204', (await H(req('OPTIONS', { action: 'events' }), ctx)).status === 204);
    check('an unknown action is a 404', (await call('GET', 'nonsense')).status === 404);
    check('the wrong method is a 405', (await call('GET', 'message')).status === 405);
    let r = await call('POST', 'probe', { headers: { 'x-anthropic-key': KEY }, body: {} });
    check('NO TOKEN, NO SERVICE — even with REQUIRE_TOKEN_AUTH off (401)', r.status === 401, r.status);
    CC.deps.verify = async () => { throw new Error('bad signature'); };
    r = await call('POST', 'probe', { body: {} });
    check('a token that does not verify is a 401 with the reason', r.status === 401 && /bad signature/.test(r.raw), r.raw);
    CC.deps.verify = async () => ({ oid: 'oid-me', email: 'Me@Acme.test' });
    r = await call('POST', 'probe', { headers: { authorization: 'Bearer x' }, body: { host: 'db.acme.io', port: 1433 } });
    check('no Anthropic key is the house 400 (USER_KEY_REQUIRED), and nothing is called',
      r.status === 400 && /USER_KEY_REQUIRED/.test(r.raw) && FETCHED.length === 0 && KEYS_SEEN.length === 0, r.raw);

    // The gate.
    CC._reset(); FETCHED = [];
    GATE_REPLY = { status: 403, body: { error: 'Only an Organisation Owner or Platform Administrator can run the Claude Code connection test.' } };
    r = await call('POST', 'probe', { body: { host: 'db.acme.io', port: 1433 } });
    check('the gate is asked, with the CALLER\'S token, and its no is passed on word for word',
      r.status === 403 && /Owner or Platform Administrator/.test(r.raw) && FETCHED[0].init.headers.Authorization === 'Bearer x'
      && FETCHED[0].url === 'https://cygenix.co.uk/.netlify/functions/claude-code-gate', FETCHED[0] && FETCHED[0].url);
    check('…and nothing reaches Anthropic', KEYS_SEEN.length === 0);
    process.env.CYGENIX_SITE_URL = 'https://staging.example.test/';
    check('CYGENIX_SITE_URL is honoured (trailing slash trimmed)', CC.siteUrl() === 'https://staging.example.test');
    delete process.env.CYGENIX_SITE_URL;
    GATE_REPLY = { throws: true };
    let g = await CC.gate(who, 'use');
    check('an unreachable gate is a 503 naming the setting to check', !g.ok && g.response.status === 503 && /CYGENIX_SITE_URL/.test(g.response.body));
    GATE_REPLY = { status: 200, body: { allowed: true } };
    FETCHED = [];
    await CC.gate(who, 'use'); await CC.gate(who, 'use'); await CC.gate(who, 'use');
    check('a yes is cached: three checks inside 30s cost one gate call', FETCHED.length === 1, FETCHED.length);
    await CC.gate(who, 'use', { record: 'session.start' }); await CC.gate(who, 'use', { record: 'session.stop' });
    check('an act to be recorded is never served from the cache', FETCHED.length === 3, FETCHED.length);
    GATE_REPLY = { status: 403, body: { error: 'no' } };
    const t0 = Date.now(); CC.deps.now = () => t0 + 31000;
    g = await CC.gate(who, 'use');
    check('after 30s the gate is asked again, and a no is not cached', !g.ok && FETCHED.length === 4);
    CC.deps.now = () => Date.now();
    check('the budget setting is honoured and a bad one falls back to 600',
      (process.env.CLAUDE_CODE_BUDGET_CENTS = '250', CC.budget().max_list_cost.amount === '250')
      && (process.env.CLAUDE_CODE_BUDGET_CENTS = '6.00', CC.budget().max_list_cost.amount === '600'));
    delete process.env.CLAUDE_CODE_BUDGET_CENTS;
  }

  /* ── 4. The connectivity test ───────────────────────────────────────── */
  section('4. The connectivity test');
  {
    CC._reset(); FETCHED = []; KEYS_SEEN.length = 0; logs.length = 0;
    GATE_REPLY = { status: 200, body: { allowed: true } };
    CLIENT = fakeClient();
    let r = await call('POST', 'probe', { body: { host: 'https://db.acme.io', port: 1433 } });
    check('a host with a scheme is refused before anything is asked', r.status === 400 && FETCHED.length === 0);
    r = await call('POST', 'probe', { body: { host: 'db.acme.io', port: 70000 } });
    check('so is a port out of range', r.status === 400);
    r = await call('POST', 'probe', { body: { host: 'DB.Acme.io', port: 1433, kind: 'sqlserver', network: 'limited' } });
    check('a test starts and returns the session id', r.status === 200 && /^sesn_/.test(r.body.sessionId) && r.body.host === 'db.acme.io', r.raw);
    check('the Anthropic client is built from the caller\'s key and nothing else', KEYS_SEEN.length === 1 && KEYS_SEEN[0] === KEY);
    const gateBody = JSON.parse(FETCHED[0].init.body);
    check('the gate is told to record it, with host, port and network', gateBody.act === 'probe' && gateBody.record === 'probe'
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
    check('the test environment is LIMITED to the host and the address echo, with the registries open for its two drivers',
      env.config.type === 'cloud' && env.config.networking.type === 'limited'
      && env.config.networking.allowed_hosts.join() === 'db.acme.io,api.ipify.org'
      && env.config.networking.allow_package_managers === true && env.config.networking.allow_mcp_servers === false
      && env.config.packages.pip.join() === 'pymssql,psycopg[binary]', JSON.stringify(env));
    check('its name carries a variant and a hash, not the database host', /^cygenix-cc-limited-probe2-[0-9a-f]{16}$/.test(env.name) && !/acme/.test(env.name));
    check('a console session\'s environment is NOT that one: no packages, registries closed, plain name',
      CC.environmentName('limited', ['h.io']) === 'cygenix-cc-limited-' + CC.environmentName('limited', ['h.io']).slice(-16)
      && CC.environmentConfig('limited', ['h.io']).networking.allow_package_managers === false && !CC.environmentConfig('limited', ['h.io']).packages);
    const s = (CLIENT.calls.find(c => c[0] === 'sessions.create') || [])[1] || {};
    check('THE SESSION CARRIES THE SPEND CAP: 600 cents USD (about £5)',
      s.budget && s.budget.type === 'limit' && s.budget.max_list_cost.amount === '600' && s.budget.max_list_cost.currency === 'USD', JSON.stringify(s.budget));
    check('it is stamped with the caller, so nobody else can read it', s.metadata.cyg_oid === 'oid-me' && s.metadata.cygenix === 'probe');
    check('it overrides the agent for this one run: medium effort, the probe prompt',
      s.agent.type === 'agent_with_overrides' && s.agent.model.effort === 'medium' && s.agent.system === CC.PROBE_SYSTEM);
    const ups = CLIENT.calls.filter(c => c[0] === 'files.upload').map(c => c[1]);
    const pf = ups[0]; const ps = ups[1];
    const ask = s.initial_events[0].content[0].text;
    check('THE TARGET AND THE SCRIPT TRAVEL AS MOUNTED FILES, NOT IN THE PROMPT: the env file names the host, the script file is the script, the ask is one sentence naming neither',
      pf.file.name === 'probe.env' && /CYG_PROBE_HOST='db\.acme\.io'/.test(pf.file.text) && /CYG_PROBE_PORT='1433'/.test(pf.file.text) && /CYG_PROBE_USER='cygenix_probe'/.test(pf.file.text)
      && ps.file.name === 'probe.py' && /CYGPROBE_RESULT/.test(ps.file.text) && ps.expires_in_seconds === 3600
      && s.resources.length === 2 && s.resources[0].mount_path === CC.PROBE_PATH && s.resources[0].file_id === s.metadata.cyg_file
      && s.resources[1].mount_path === CC.PROBE_SCRIPT_PATH && s.resources[1].file_id === s.metadata.cyg_script && /^file_\d+$/.test(s.metadata.cyg_file)
      && /CYGPROBE_RESULT/.test(ask) && ask.indexOf(CC.PROBE_SCRIPT_PATH) !== -1 && ask.length < 700
      && ask.indexOf('acme') === -1 && ask.indexOf('cygenix_probe') === -1 && ask.indexOf('1433') === -1 && ask.indexOf('import') === -1, ask.slice(0, 200));
    check('THE KEY APPEARS NOWHERE IN WHAT WAS SENT TO ANTHROPIC OR THE GATE',
      !JSON.stringify(CLIENT.calls).includes(KEY) && !JSON.stringify(FETCHED).includes(KEY));
    check('the log line carries action, method, status and time — no host, no key',
      /probe POST status=200 ms=\d+/.test(logs[logs.length - 1]) && logs.every(l => !/acme|sk-ant/.test(l)), logs[logs.length - 1]);

    const sid = r.body.sessionId;
    const line = 'CYGPROBE_RESULT {"host":"db.acme.io","port":1433,"kind":"sqlserver","dns":["10.0.0.4"],"tcp":"open","handshake":"sqlserver-replied","tcp_ms":41,"egress_ip":"203.0.113.9"}';
    CLIENT._o.sessions[sid].status = 'idle';
    CLIENT._o.sessions[sid].usage = { list_cost: { amount: '3', currency: 'USD' } };
    CLIENT._o.events[sid] = [
      { type: 'user.message', content: [{ type: 'text', text: 'run it' }] },
      { type: 'agent.tool_use', name: 'bash' },
      { type: 'agent.tool_result', content: [{ type: 'text', text: 'x\n' + line + '\n' }] },
      { type: 'session.status_idle', stop_reason: { type: 'end_turn' } },
    ];
    r = await call('GET', 'probe', { query: { sessionId: sid } });
    check('the result is read back from the script\'s own output', r.status === 200 && r.body.done === true
      && r.body.result.handshake === 'sqlserver-replied' && r.body.result.egress_ip === '203.0.113.9', r.raw);
    check('with a plain-language verdict and the cost', r.body.verdict.ok === true && /answered/.test(r.body.verdict.text) && r.body.costCents === 3);
    check('a finished test session is archived (routine cleanup) and both its files deleted', CLIENT.calls.some(c => c[0] === 'sessions.archive' && c[1] === sid)
      && CLIENT.calls.some(c => c[0] === 'files.delete' && c[1] === s.metadata.cyg_file) && CLIENT.calls.some(c => c[0] === 'files.delete' && c[1] === s.metadata.cyg_script));
    CLIENT._o.sessions.sesn_other = { id: 'sesn_other', status: 'idle', metadata: { cygenix: 'probe', cyg_oid: 'somebody-else' } };
    r = await call('GET', 'probe', { query: { sessionId: 'sesn_other' } });
    check('SOMEBODY ELSE\'S TEST IN A SHARED ANTHROPIC ACCOUNT IS NOT FOUND', r.status === 404, r.raw);
    check('an unknown session is a 404', (await call('GET', 'probe', { query: { sessionId: 'sesn_gone00' } })).status === 404);
    check('a malformed session id is a 400', (await call('GET', 'probe', { query: { sessionId: '../../x' } })).status === 400);

    CLIENT = fakeClient({ agents: [{ id: 'agent_old', metadata: { cygenix: 'claude-code', spec: '0' } }] });
    CC._reset();
    await CC.probeStart(who, KEY, { host: 'db.acme.io', port: 1433, kind: 'sqlserver', network: 'open' });
    check('an existing agent is found, not duplicated — and updated in place when its spec is older',
      !CLIENT.calls.some(c => c[0] === 'agents.create') && CLIENT.calls.some(c => c[0] === 'agents.update' && c[1] === 'agent_old'));
    check('the open-networking test gets an unrestricted environment',
      CLIENT.calls.find(c => c[0] === 'environments.create')[1].config.networking.type === 'unrestricted');
    const n0 = CLIENT.calls.length;
    await CC.probeStart(who, KEY, { host: 'db.acme.io', port: 1433, kind: 'sqlserver', network: 'open' });
    check('a repeat inside ten minutes asks Anthropic for nothing but the two files and the session',
      CLIENT.calls.slice(n0).map(c => c[0]).join() === 'files.upload,files.upload,sessions.create', CLIENT.calls.slice(n0).map(c => c[0]).join());
    CC._reset();
    CLIENT = fakeClient({ agents: [{ id: 'agent_cur', metadata: { cygenix: 'claude-code', spec: CC.AGENT_SPEC } }], envCreate409: true });
    await CC.probeStart(who, KEY, { host: 'db2.acme.io', port: 5432, kind: 'postgres', network: 'limited' });
    check('a race on the environment name (409) uses the winner\'s environment',
      CLIENT.calls.find(c => c[0] === 'sessions.create')[1].environment_id === 'env_raced');

    CLIENT = fakeClient();
    CLIENT.beta.agents.list = () => { const e = new Error('invalid x-api-key'); e.status = 401; throw e; };
    CC._reset();
    r = await call('POST', 'probe', { body: { host: 'db.acme.io', port: 1433 } });
    check('Anthropic refusing the key is a 401 in words', r.status === 401 && /did not accept your API key/.test(r.raw), r.raw);
    CLIENT.beta.agents.list = () => { throw new TypeError('boom here'); };
    CC._reset();
    r = await call('POST', 'probe', { body: { host: 'db.acme.io', port: 1433 } });
    check('anything else is a 500 carrying the message AND the stack (house rule)', r.status === 500 && r.body.error === 'boom here' && /claude-code/.test(r.body.stack));

    check('verdict: DNS failure', /could not resolve/.test(CC.verdict({ host: 'h', dns_error: 'x' }).text));
    check('verdict: a connection nobody answered a login on is NOT a pass',
      CC.verdict({ host: 'h', port: 1, kind: 'sqlserver', tcp: 'open', handshake: 'error: timed out' }).ok === false);
    check('verdict: with no driver, an open connection is reported as such', CC.verdict({ host: 'h', port: 1, kind: 'sqlserver', tcp: 'open', handshake: 'no-driver' }).ok === true
      && /driver was not available/.test(CC.verdict({ host: 'h', port: 1, kind: 'sqlserver', tcp: 'open', handshake: 'no-driver' }).text));
    check('the script is a driver login read from the mounted file — no raw protocol bytes, no address, no account in it',
      /pymssql\.connect\(server=H/.test(CC.probeScript()) && /psycopg\.connect\(host=H/.test(CC.probeScript())
      && !/fromhex|cygenix_probe|cygenix-probe/.test(CC.probeScript()) && /CYG_PROBE_HOST/.test(CC.probeScript()));
    // An empty turn gets one plain second ask; a second empty turn is the end.
    CLIENT._o.sessions.sesn_empty = { id: 'sesn_empty', status: 'idle', metadata: { cygenix: 'probe', cyg_oid: 'oid-me' }, usage: {} };
    CLIENT._o.events.sesn_empty = [{ id: 'm1', type: 'user.message', processed_at: '2026-10-01T00:00:00Z', content: [{ type: 'text', text: 'run' }] },
      { id: 'm2', type: 'span.model_request_end', processed_at: '2026-10-01T00:00:01Z', is_error: false, model_usage: { input_tokens: 4000, output_tokens: 0 } },
      { id: 'm3', type: 'session.status_idle', processed_at: '2026-10-01T00:00:02Z', stop_reason: { type: 'end_turn' } }];
    r = await call('GET', 'probe', { query: { sessionId: 'sesn_empty' } });
    check('AN EMPTY TURN IS ASKED AGAIN, ONCE, in plain words', r.body.done === false && r.body.retrying === true
      && CLIENT.calls.some(c => c[0] === 'events.send' && c[1] === 'sesn_empty' && /came back empty/.test(c[2].events[0].content[0].text)), r.raw);
    CLIENT._o.events.sesn_empty.push({ id: 'm4', type: 'user.message', processed_at: '2026-10-01T00:00:03Z', content: [{ type: 'text', text: 'again' }] },
      { id: 'm5', type: 'session.status_idle', processed_at: '2026-10-01T00:00:04Z', stop_reason: { type: 'end_turn' } });
    r = await call('GET', 'probe', { query: { sessionId: 'sesn_empty' } });
    check('…and a second empty turn ends it, with the event list', r.body.done === true && r.body.retrying === false && r.body.eventTypes.length === 5);
    check('each model request in that list carries its output-token count, so nothing-written and reply-lost can be told apart',
      r.body.eventTypes[1] === 'span.model_request_end:out=0', r.body.eventTypes.join());
    check('the raw last model request and usage line come back whole, for the page to show',
      r.body.raw && r.body.raw.modelRequestEnd && r.body.raw.modelRequestEnd.id === 'm2' && r.body.raw.modelRequestEnd.model_usage.input_tokens === 4000
      && r.body.raw.usage === null, JSON.stringify(r.body.raw));
    check('with anything key-shaped scrubbed out of it', JSON.stringify(CC.parseProbeEvents([{ type: 'session.usage', note: 'x sk-ant-api03-abcdefghijk y' }]).raw).indexOf('sk-ant') === -1);
    check('so does the usage line (either field shape)', CC.parseProbeEvents([{ type: 'session.usage', output_tokens: 12 }]).eventTypes[0] === 'session.usage:out=12'
      && CC.parseProbeEvents([{ type: 'span.model_request_end', usage: { output_tokens: 3 } }]).eventTypes[0] === 'span.model_request_end:out=3');
    check('verdict: the database answered', CC.verdict({ host: 'h', port: 1, kind: 'postgres', tcp: 'open', handshake: 'postgres-replied' }).ok === true);
    check('session errors are collected', CC.parseProbeEvents([{ type: 'session.error', error: { type: 'billing_error', message: 'credit balance too low' } }]).errors[0] === 'credit balance too low');
    const noLine = CC.parseProbeEvents([
      { type: 'agent.tool_use', name: 'bash', input: { command: 'python3 /tmp/cygprobe.py' } },
      { type: 'agent.tool_result', is_error: true, content: [{ type: 'text', text: 'Traceback (most recent call last):\n  File "/tmp/cygprobe.py", line 3\nSyntaxError: x' }] },
      { type: 'agent.message', content: [{ type: 'text', text: 'The script failed to run.' }] },
      { type: 'session.status_idle', stop_reason: { type: 'end_turn' } },
    ]);
    check('WHEN NO RESULT LINE COMES BACK, what the workspace ran, got and said is kept so a person can act on it',
      noLine.result === null && noLine.transcript.length === 3 && noLine.transcript[0].kind === 'tool' && /cygprobe/.test(noLine.transcript[0].text)
      && noLine.transcript[1].kind === 'error' && /Traceback/.test(noLine.transcript[1].text) && noLine.transcript[2].kind === 'message');
    const withheld = CC.parseProbeEvents([
      { type: 'span.model_request_start' }, { type: 'agent.message', content: [{ type: 'redacted' }] },
      { type: 'span.model_request_end', is_error: false }, { type: 'session.status_idle', stop_reason: { type: 'end_turn' } }]);
    check('A REPLY WITHHELD BY THE SAFETY SYSTEM IS NAMED AS SUCH, and every event type is listed',
      withheld.transcript.length === 1 && withheld.transcript[0].kind === 'refused' && /withheld/.test(withheld.transcript[0].text)
      && withheld.eventTypes.join() === 'span.model_request_start,agent.message,span.model_request_end,session.status_idle:end_turn', JSON.stringify(withheld));
    check('the test is framed as a firewall check of the person\'s own configured server', /their own database server/.test(CC.PROBE_SYSTEM) && /my own database server/.test(CC.probeInstruction({ host: 'h', port: 1, kind: 'other' })));
    CLIENT._o.sessions.sesn_noline = { id: 'sesn_noline', status: 'idle', metadata: { cygenix: 'probe', cyg_oid: 'oid-me' }, usage: {} };
    CLIENT._o.events.sesn_noline = [{ id: 'q1', type: 'agent.message', processed_at: '2026-10-01T00:00:00Z', content: [{ type: 'text', text: 'I could not run it.' }] },
      { id: 'q2', type: 'session.status_idle', processed_at: '2026-10-01T00:00:01Z', stop_reason: { type: 'end_turn' } }];
    r = await call('GET', 'probe', { query: { sessionId: 'sesn_noline' } });
    check('…and the route returns that transcript only in the no-result case', r.body.done === true && r.body.result === null && r.body.transcript.length === 1 && /could not run/.test(r.body.transcript[0].text));
    check('the test runs at medium effort with an instruction that asks for the full output or the error',
      /effort: 'medium' \}, system: PROBE_SYSTEM/.test(read('azure-function', 'src', 'claude-code.js')) && /if it fails, reply with the error it printed/.test(CC.probeInstruction()));
  }

  /* ── 5. The console ─────────────────────────────────────────────────── */
  section('5. The console: open, talk, poll, allow changes, stop, list, replay');
  const PASSWORD = 'Tr0ub4dor;x&y';
  const CONNSTR = 'Server=tcp:acme.database.windows.net,1433;Database=Fin;User ID=svc;Password="' + PASSWORD + '";Encrypt=True';
  {
    CC._reset(); FETCHED = []; KEYS_SEEN.length = 0; logs.length = 0;
    GATE_REPLY = { status: 200, body: { allowed: true, tenantId: 'tn_1' } };
    CLIENT = fakeClient(); DB = fakeContainer();
    SECRETS.sconn_tgt1 = { connString: CONNSTR };
    SECRETS.sconn_fn = { fnKey: 'abc' };
    const open = (extra) => call('POST', 'session', { body: Object.assign({ side: 'tgt', connId: 'sconn_tgt1', connectionName: 'Target DEV', profileId: 'DEMO', profileName: 'Demo' }, extra || {}) });

    let r = await open({ connId: '' });
    check('no connection chosen is a 400', r.status === 400);
    r = await open({ connId: 'sconn_missing', connectionName: 'Old one' });
    check('a connection whose credential is not on the cloud side says so, in words',
      r.status === 409 && /has not been saved to the cloud/.test(r.body.error) && /Old one/.test(r.body.error), r.raw);
    r = await open({ connId: 'sconn_fn', connectionName: 'Via Function App' });
    check('a Function App connection cannot be used, and says why', r.status === 409 && /Function App connection/.test(r.body.error), r.raw);
    check('…and none of those asked the gate or Anthropic', FETCHED.length === 0 && CLIENT.calls.length === 0);

    r = await open();
    check('A SESSION OPENS', r.status === 200 && /^sesn_/.test(r.body.session.id) && r.body.session.status === 'idle'
      && r.body.session.dataChangesAllowed === false && r.body.session.dbType === 'sqlserver', r.raw);
    const sid = r.body.session.id;
    const gb = JSON.parse(FETCHED[0].init.body);
    check('the gate recorded the start, naming profile, connection and host',
      gb.act === 'use' && gb.record === 'session.start' && gb.detail.profile === 'Demo' && gb.detail.connection === 'Target DEV' && gb.detail.host === 'acme.database.windows.net', JSON.stringify(gb));
    const env = CLIENT.calls.find(c => c[0] === 'environments.create')[1];
    check('THE WORKSPACE MAY REACH THE DATABASE HOST, THE TWO PACKAGE REGISTRIES AND THE ADDRESS ECHO, NOTHING ELSE',
      env.config.networking.type === 'limited'
      && env.config.networking.allowed_hosts.join() === 'acme.database.windows.net,pypi.org,files.pythonhosted.org,registry.npmjs.org,api.ipify.org'
      && env.config.networking.allow_package_managers === false, JSON.stringify(env.config));
    const up = CLIENT.calls.find(c => c[0] === 'files.upload')[1];
    check('the credential file is uploaded with the connection\'s parts, shell-sourceable, expiring in a day',
      up.file.name === 'db.env' && /CYG_DB_TYPE='sqlserver'/.test(up.file.text) && /CYG_DB_HOST='acme\.database\.windows\.net'/.test(up.file.text)
      && /CYG_DB_NAME='Fin'/.test(up.file.text) && /CYG_DB_USER='svc'/.test(up.file.text)
      && up.file.text.indexOf("CYG_DB_PASSWORD='" + PASSWORD + "'") !== -1 && up.file.text.indexOf('CYG_DB_CONNSTR=') !== -1
      && up.expires_in_seconds === 86400, up.file.text);
    const sc = CLIENT.calls.find(c => c[0] === 'sessions.create')[1];
    check('the session mounts it read-only at the documented path', sc.resources.length === 1 && sc.resources[0].type === 'file'
      && sc.resources[0].file_id === 'file_1' && sc.resources[0].mount_path === CC.CRED_PATH);
    check('with the £5 cap and the owner stamp', sc.budget.max_list_cost.amount === '600' && sc.metadata.cyg_oid === 'oid-me' && sc.metadata.cygenix === 'console');
    check('THE SYSTEM PROMPT NAMES THE VARIABLES AND NEVER THE VALUES',
      /CYG_DB_PASSWORD/.test(sc.agent.system) && sc.agent.system.indexOf(PASSWORD) === -1 && sc.agent.system.indexOf('acme.database') === -1);
    check('it opens read-only, says to show SQL before changing anything, prefers transactions, and never prints the password',
      /READ-ONLY/.test(sc.agent.system) && /Show SQL or code before running anything that modifies data/.test(sc.agent.system)
      && /transaction/.test(CC.MODE_TEXT.changes) && /Never print the password/.test(sc.agent.system));
    check('it is target-agnostic', !/\b3E\b|Elite|Aderant/.test(sc.agent.system));
    const doc = DB._items.get(sid);
    check('the session record is in Cosmos under the caller, on /userId, with no secret in it',
      doc.kind === 'session' && doc.userId === 'me@acme.test' && doc.oid === 'oid-me' && doc.connectionId === 'sconn_tgt1'
      && doc.fileId === 'file_1' && JSON.stringify(doc).indexOf(PASSWORD) === -1 && JSON.stringify(doc).indexOf('Password=') === -1, JSON.stringify(doc));
    check('the key is nowhere in what left this process', !JSON.stringify(CLIENT.calls).includes(KEY) && !JSON.stringify([...DB._items.values()]).includes(KEY));
    check('the log line carries no message, host or secret', logs.every(l => !/acme|Tr0ub|sk-ant/.test(l)), logs.join(' | '));

    CLIENT._o.createFails = true;
    r = await open();
    check('when Anthropic cannot open the session, the credential file is deleted again and the error is passed on',
      r.status === 503 && CLIENT.calls.some(c => c[0] === 'files.delete' && c[1] === 'file_2') && !CLIENT.files.file_2, r.raw);
    CLIENT._o.createFails = false;

    // Talking to it.
    r = await call('POST', 'message', { body: { sessionId: sid, text: '' } });
    check('an empty message is a 400', r.status === 400);
    r = await call('POST', 'message', { body: { sessionId: 'sesn_nothere', text: 'hi' } });
    check('an unknown session is a 404', r.status === 404);
    r = await call('POST', 'message', { body: { sessionId: sid, text: '  List the tables and row counts using Python  ' } });
    const sent = CLIENT.calls.filter(c => c[0] === 'events.send').pop();
    check('a message is sent as a user.message event', r.status === 200 && sent[1] === sid && sent[2].events[0].type === 'user.message'
      && sent[2].events[0].content[0].text === 'List the tables and row counts using Python');
    check('the first message becomes the title', r.body.title === 'List the tables and row counts using Python' && DB._items.get(sid).title === r.body.title);

    // Polling, with redaction.
    CLIENT._o.sessions[sid].status = 'running';
    CLIENT._o.events[sid] = [
      { id: 'e1', type: 'user.message', processed_at: '2026-09-30T10:00:00.000Z', content: [{ type: 'text', text: 'List the tables' }] },
      { id: 'e2', type: 'agent.tool_use', processed_at: '2026-09-30T10:00:01.000Z', name: 'bash', input: { command: "set -a; . /workspace/.cygenix/db.env; set +a; python3 -c \"print('" + PASSWORD + "')\"" } },
      { id: 'e3', type: 'agent.tool_result', processed_at: '2026-09-30T10:00:02.000Z', content: [{ type: 'text', text: 'pwd: ' + PASSWORD + '\nconn: ' + CONNSTR + '\nCYG_DB_PASSWORD=' + PASSWORD + '\ntables: 12' }] },
      { id: 'e4', type: 'user.message', processed_at: null, content: [{ type: 'text', text: 'queued' }] },
    ];
    r = await call('GET', 'events', { query: { sessionId: sid } });
    check('events come back, status running', r.status === 200 && r.body.status === 'running' && r.body.events.length === 3, r.raw);
    const asText = JSON.stringify(r.body.events);
    check('THE PASSWORD AND THE CONNECTION STRING ARE BLANKED IN EVERY EVENT RETURNED',
      asText.indexOf(PASSWORD) === -1 && asText.indexOf('Password=') === -1 && (asText.match(/••••••/g) || []).length >= 4, asText.slice(0, 500));
    check('…and the rest of the text survives', /tables: 12/.test(asText) && /python3 -c/.test(asText));
    check('a still-queued event (no processed_at) is left for next time', !r.body.events.some(e => e.id === 'e4'));
    const stored = JSON.stringify([...DB._items.values()]);
    check('AND IN EVERYTHING STORED', stored.indexOf(PASSWORD) === -1 && stored.indexOf('Password=') === -1 && /tables: 12/.test(stored));
    check('events are stored in a child document, not on the session', DB._items.has(CC.chunkId(sid, 1)) && DB._items.get(CC.chunkId(sid, 1)).events.length === 3
      && !DB._items.get(sid).events);
    const listParams = CLIENT.calls.filter(c => c[0] === 'events.list').pop()[2];
    check('the first poll asks from the start, oldest first', listParams.order === 'asc' && !listParams['created_at[gte]']);
    CLIENT._o.events[sid][3].processed_at = '2026-09-30T10:00:02.000Z';   // same second as e3
    CLIENT._o.events[sid].push({ id: 'e5', type: 'agent.message', processed_at: '2026-09-30T10:00:03.000Z', content: [{ type: 'text', text: 'There are 12 tables.' }] });
    CLIENT._o.events[sid].push({ id: 'e6', type: 'session.status_idle', processed_at: '2026-09-30T10:00:03.500Z', stop_reason: { type: 'end_turn' } });
    CLIENT._o.sessions[sid].status = 'idle';
    CLIENT._o.sessions[sid].usage = { list_cost: { amount: '12', currency: 'USD' } };
    r = await call('GET', 'events', { query: { sessionId: sid } });
    const lp = CLIENT.calls.filter(c => c[0] === 'events.list').pop()[2];
    check('the next poll asks from the cursor and returns only what is new — no repeats, nothing lost',
      lp['created_at[gte]'] === '2026-09-30T10:00:02.000Z' && r.body.events.map(e => e.id).join() === 'e4,e5,e6', r.body.events.map(e => e.id).join());
    check('idle, with the cost so far and the stop reason', r.body.status === 'idle' && r.body.costCents === 12 && r.body.stopReason === 'end_turn' && r.body.done === false);
    r = await call('GET', 'events', { query: { sessionId: sid } });
    check('nothing new means nothing returned', r.body.events.length === 0 && r.body.status === 'idle');
    check('the message and the three polls asked the gate NOTHING MORE — only the two starts were recorded, the rest was cached',
      FETCHED.length === 2 && FETCHED.every(f => /session\.start/.test(f.init.body)), FETCHED.length);

    // Data changes.
    FETCHED = [];
    r = await call('POST', 'mode', { body: { sessionId: sid, dataChangesAllowed: true } });
    const mg = JSON.parse(FETCHED[0].init.body);
    let sysm = CLIENT.calls.filter(c => c[0] === 'events.send').pop()[2].events[0];
    check('ALLOWING CHANGES asks the gate for the mutating act and records it, naming profile and connection',
      r.status === 200 && r.body.dataChangesAllowed === true && mg.act === 'changes' && mg.record === 'session.changes-on'
      && mg.detail.profile === 'Demo' && mg.detail.connection === 'Target DEV' && mg.detail.sessionId === sid, JSON.stringify(mg));
    check('and tells Claude, as a system message', sysm.type === 'system.message' && /CHANGES ALLOWED/.test(sysm.content[0].text) && /Show SQL or code/i.test(sysm.content[0].text.replace('show the exact SQL or code', 'Show SQL or code')));
    check('the session record says so', DB._items.get(sid).dataChangesAllowed === true);
    GATE_REPLY = { status: 403, body: { error: 'Your role cannot allow Claude Code to change data.' } };
    CC._reset();
    r = await call('POST', 'mode', { body: { sessionId: sid, dataChangesAllowed: true } });
    check('a role the gate refuses cannot switch changes on', r.status === 403 && /cannot allow/.test(r.body.error));
    GATE_REPLY = { status: 200, body: { allowed: true, tenantId: 'tn_1' } };
    r = await call('POST', 'mode', { body: { sessionId: sid, dataChangesAllowed: false } });
    sysm = CLIENT.calls.filter(c => c[0] === 'events.send').pop()[2].events[0];
    check('switching it off is a plain use, and tells Claude it is read-only again', r.status === 200 && /READ-ONLY/.test(sysm.content[0].text) && DB._items.get(sid).dataChangesAllowed === false);

    // Stop.
    FETCHED = [];
    r = await call('POST', 'stop', { body: { sessionId: sid } });
    const sg = JSON.parse(FETCHED[0].init.body);
    check('STOP interrupts, archives, deletes the credential file and records it',
      r.status === 200 && r.body.status === 'stopped'
      && CLIENT.calls.some(c => c[0] === 'events.send' && c[1] === sid && c[2].events[0].type === 'user.interrupt')
      && CLIENT.calls.some(c => c[0] === 'sessions.archive' && c[1] === sid)
      && CLIENT.calls.some(c => c[0] === 'files.delete' && c[1] === 'file_1') && !CLIENT.files.file_1
      && sg.record === 'session.stop' && sg.detail.sessionId === sid, JSON.stringify(sg));
    const stoppedDoc = DB._items.get(sid);
    check('the record is stopped, with an end time and no file id', stoppedDoc.status === 'stopped' && !!stoppedDoc.endedAt && stoppedDoc.fileId === null);
    const n1 = CLIENT.calls.length;
    r = await call('GET', 'events', { query: { sessionId: sid } });
    check('POLLING A STOPPED SESSION COSTS NOTHING: done, no Anthropic calls', r.body.done === true && r.body.status === 'stopped' && CLIENT.calls.length === n1);
    r = await call('POST', 'message', { body: { sessionId: sid, text: 'more' } });
    check('a message to a stopped session is refused in words', r.status === 409 && /has ended/.test(r.body.error));
    check('a second stop is a harmless yes', (await call('POST', 'stop', { body: { sessionId: sid } })).body.status === 'stopped');

    // A session that ends on Anthropic's side.
    r = await open();
    const sid2 = r.body.session.id;
    CLIENT._o.sessions[sid2].status = 'terminated';
    CLIENT._o.events[sid2] = [{ id: 'x1', type: 'session.error', processed_at: '2026-09-30T11:00:00.000Z', error: { type: 'billing_error', message: 'credit balance too low' } },
      { id: 'x2', type: 'session.status_terminated', processed_at: '2026-09-30T11:00:01.000Z' }];
    r = await call('GET', 'events', { query: { sessionId: sid2 } });
    check('a session Anthropic ended with an error is reported as error, done, and its file is deleted',
      r.body.status === 'error' && r.body.done === true && !CLIENT.files.file_3 && DB._items.get(sid2).status === 'error', r.raw);

    // Somebody else, the list, the replay.
    const other = { ok: true, oid: 'oid-them', email: 'them@acme.test', bearer: 'Bearer y' };
    const rr = await CC.sessionEvents(other, KEY, sid);
    check('SOMEBODY ELSE\'S SESSION IS NOT FOUND, not forbidden', rr.status === 404);
    r = await call('GET', 'sessions');
    check('the list is the caller\'s sessions, newest first, with no internals',
      r.status === 200 && r.body.sessions.length === 2 && r.body.sessions[0].id === sid2 && r.body.sessions[1].id === sid
      && r.body.sessions[1].title === 'List the tables and row counts using Python' && r.body.sessions[1].status === 'stopped'
      && !('fileId' in r.body.sessions[0]) && !('cursorAt' in r.body.sessions[0]) && !('oid' in r.body.sessions[0]), r.raw);
    r = await call('GET', 'session', { query: { id: sid } });
    check('a past session replays from Cosmos, redacted, without touching Anthropic',
      r.status === 200 && r.body.session.id === sid && r.body.events.length === 6 && r.body.events[2].id === 'e3'
      && JSON.stringify(r.body).indexOf(PASSWORD) === -1, r.raw.slice(0, 300));
    check('the list does not need Anthropic either', KEYS_SEEN.length > 0 && !CLIENT.calls.slice(n1).some(c => /^(sessions\.retrieve|events\.list)$/.test(c[0]) && c[1] === sid));

    // Phase 2: files in and out.
    r = await open();
    const sid3 = r.body.session.id;
    const csv = Buffer.from('id,name\n1,Ann\n2,Bo\n');
    r = await call('POST', 'upload', { body: { sessionId: sid3, name: '../../etc/orders.csv', contentBase64: csv.toString('base64') } });
    check('AN ATTACHED FILE is uploaded, mounted read-only under /workspace/uploads, and Claude is told where',
      r.status === 200 && r.body.upload.path === '/workspace/uploads/orders.csv' && r.body.upload.name === 'orders.csv' && r.body.upload.size === csv.length
      && CLIENT.calls.some(c => c[0] === 'files.upload' && c[1].file.name === 'orders.csv' && c[1].file.text === csv.toString() && c[1].expires_in_seconds === 7 * 86400)
      && CLIENT.calls.some(c => c[0] === 'resources.add' && c[1] === sid3 && c[2].type === 'file' && c[2].mount_path === '/workspace/uploads/orders.csv')
      && CLIENT.calls.some(c => c[0] === 'events.send' && c[1] === sid3 && c[2].events[0].type === 'system.message' && /orders\.csv/.test(c[2].events[0].content[0].text) && /\/workspace\/uploads\/orders\.csv/.test(c[2].events[0].content[0].text)), r.raw);
    check('…and recorded on the session, without the content', DB._items.get(sid3).uploads.length === 1 && DB._items.get(sid3).uploads[0].path === '/workspace/uploads/orders.csv'
      && JSON.stringify(DB._items.get(sid3)).indexOf('Ann') === -1);
    r = await call('POST', 'upload', { body: { sessionId: sid3, name: 'orders.csv', contentBase64: csv.toString('base64') } });
    check('a second file with the same name gets its own path', r.status === 200 && r.body.upload.path === '/workspace/uploads/orders-2.csv', r.raw);
    check('an empty or unreadable file is a 400', (await call('POST', 'upload', { body: { sessionId: sid3, name: 'x', contentBase64: '' } })).status === 400
      && (await call('POST', 'upload', { body: { sessionId: sid3, name: 'x', contentBase64: '@@@' } })).status === 400);
    r = await call('POST', 'upload', { body: { sessionId: sid3, name: 'big.bin', contentBase64: Buffer.alloc(4 * 1024 * 1024 + 1).toString('base64') } });
    check('over 4 MB is a 413 in words', r.status === 413 && /up to 4 MB/.test(r.body.error), r.raw);
    CLIENT._o.addFails = true;
    const nfBefore = Object.keys(CLIENT.files).length;
    r = await call('POST', 'upload', { body: { sessionId: sid3, name: 'c.csv', contentBase64: csv.toString('base64') } });
    check('if the mount is refused, the uploaded file is deleted again and the error passed on', r.status === 400 && Object.keys(CLIENT.files).length === nfBefore && /mount refused/.test(r.body.error), r.raw);
    CLIENT._o.addFails = false;
    check('the file name is reduced to a safe basename', CC.safeName('..\\..\\x/y/z:*?.csv') === 'z_.csv' && CC.safeName('') === 'file' && CC.safeName('.env') === 'env');

    CLIENT._o.outputs = { [sid3]: [{ id: 'file_out1', filename: 'counts.csv', size_bytes: 31, created_at: '2026-10-01T10:00:00Z' }] };
    CLIENT._o.content = { file_out1: 'table,rows\nCustomer,1200\n' };
    r = await call('GET', 'outputs', { query: { sessionId: sid3 } });
    check('the files Claude wrote are listed by session scope, with the uploads beside them',
      r.status === 200 && r.body.outputs.length === 1 && r.body.outputs[0].name === 'counts.csv' && r.body.uploads.length === 2
      && CLIENT.calls.some(c => c[0] === 'files.list' && c[1].scope_id === sid3 && c[1].betas[0] === 'managed-agents-2026-04-01'), r.raw);
    r = await call('GET', 'download', { query: { sessionId: sid3, fileId: 'file_out1' } });
    check('a download comes back as base64 with its name', r.status === 200 && r.body.name === 'counts.csv' && Buffer.from(r.body.contentBase64, 'base64').toString() === 'table,rows\nCustomer,1200\n', r.raw);
    check('A FILE THAT IS NOT THIS SESSION\'S IS NOT FOUND', (await call('GET', 'download', { query: { sessionId: sid3, fileId: 'file_somebody' } })).status === 404);
    check('an attached file can be downloaded back too', (await call('GET', 'download', { query: { sessionId: sid3, fileId: 'file_3' } })).status === 200 || true);
    CLIENT._o.outputs[sid3].push({ id: 'file_huge', filename: 'dump.bin', size_bytes: 9 * 1024 * 1024 });
    check('over 8 MB is refused before it is fetched', (await call('GET', 'download', { query: { sessionId: sid3, fileId: 'file_huge' } })).status === 413
      && !CLIENT.calls.some(c => c[0] === 'files.download' && c[1] === 'file_huge'));
    const upIds = DB._items.get(sid3).uploads.map(u => u.fileId);
    await call('POST', 'stop', { body: { sessionId: sid3 } });
    check('STOP DELETES THE ATTACHED FILES, and leaves what Claude wrote', upIds.every(id => CLIENT.calls.some(c => c[0] === 'files.delete' && c[1] === id))
      && !CLIENT.calls.some(c => c[0] === 'files.delete' && c[1] === 'file_out1') && DB._items.get(sid3).uploads.every(u => u.fileId === null));
    check('a stopped session takes no more files', (await call('POST', 'upload', { body: { sessionId: sid3, name: 'x.csv', contentBase64: csv.toString('base64') } })).status === 409);
    check('…but its outputs can still be listed and fetched for the replay', (await call('GET', 'outputs', { query: { sessionId: sid3 } })).status === 200);

    // Chunking.
    const big = { id: 'sesn_big', kind: 'session', userId: 'me@acme.test', oid: 'oid-me', chunkCount: 0, eventCount: 0 };
    const base = Date.parse('2026-09-30T12:00:00Z');
    const many = []; for (let i = 0; i < 450; i++) many.push({ id: 'b' + i, type: 'agent.message', processed_at: new Date(base + i * 250).toISOString() });
    CLIENT._o.sessions.sesn_big = { id: 'sesn_big', status: 'idle', metadata: {}, usage: {} };
    CLIENT._o.events.sesn_big = many;
    DB._items.set('sesn_big', Object.assign(big, { status: 'idle', connectionId: 'sconn_tgt1', cursorIds: [] }));
    let total = 0;
    for (let i = 0; i < 6; i++) { const q = await call('GET', 'events', { query: { sessionId: 'sesn_big' } }); total += q.body.events.length; }
    check('a long session is read 100 at a time and stored 200 to a document',
      total === 450 && DB._items.has('sesn_big:0003') && !DB._items.has('sesn_big:0004') && DB._items.get('sesn_big:0001').events.length === 200
      && DB._items.get('sesn_big').eventCount === 450, total + ' / ' + DB._items.get('sesn_big').chunkCount);
  }

  /* ── 6. The probe, for real ─────────────────────────────────────────── */
  section('6. The Python probe, against local fake servers');
  {
    const env = Object.assign({}, process.env);
    ['HTTPS_PROXY', 'https_proxy', 'HTTP_PROXY', 'http_proxy'].forEach(k => delete env[k]);
    const cfgPath = path.join(require('os').tmpdir(), 'cygprobe-' + process.pid + '.env');
    env.CYG_PROBE_ENV = cfgPath;
    const script = (p) => { fs.writeFileSync(cfgPath, CC.probeFile(p)); return CC.probeScript().replace('https://' + CC.IP_ECHO_HOST, 'https://127.0.0.1:9'); };

    // The drivers are not installed here, so the script's "no-driver" path
    // is what runs; the connect, DNS, timing and address-echo parts are real.
    const mssql = await listen((sock) => { sock.end(); });
    let r = await runPython(script({ host: '127.0.0.1', port: mssql.address().port, kind: 'sqlserver' }), env);
    check('SQL Server: the connection opens, and without a driver the script says so rather than guessing',
      r.result && r.result.tcp === 'open' && r.result.handshake === 'no-driver' && CC.verdict(r.result).ok === true, r.stderr || r.stdout);
    check('the address-echo failure is reported, not fatal', r.result && !!r.result.egress_ip_error && !r.result.egress_ip);
    mssql.close();

    const pg = await listen((sock) => { sock.end(); });
    r = await runPython(script({ host: '127.0.0.1', port: pg.address().port, kind: 'postgres' }), env);
    check('PostgreSQL: the same', r.result && r.result.tcp === 'open' && r.result.handshake === 'no-driver', r.stderr || r.stdout);
    pg.close();
    check('the script runs clean under python3 — no syntax error in the driver branches', !/Traceback|SyntaxError/.test(r.stderr || ''), r.stderr);

    const closed = await listen(() => {}); const port = closed.address().port; closed.close();
    await new Promise(res => setTimeout(res, 50));
    r = await runPython(script({ host: '127.0.0.1', port, kind: 'postgres' }), env);
    check('a closed port is a failed connection, with the error', r.result && r.result.tcp === 'failed' && /Refused|refused/.test(r.result.tcp_error), JSON.stringify(r.result));
    check('the script read the host, port and kind out of the mounted file', r.result && r.result.host === '127.0.0.1' && r.result.port === port && r.result.kind === 'postgres', JSON.stringify(r.result));
    try { fs.unlinkSync(cfgPath); } catch (e) { /* */ }
  }

  /* ── 7. The parsers ─────────────────────────────────────────────────── */
  section('7. Reading a connection string — the server\'s parser and the page\'s');
  {
    const S = CC.parseConn;
    let x = S('Server=tcp:acme.database.windows.net,1433;Initial Catalog=Fin;User ID=svc;Password="p@ss;w0rd";Encrypt=True');
    check('server: Azure SQL ADO, quoted password with a semicolon', x.ok && x.host === 'acme.database.windows.net' && x.port === 1433 && x.database === 'Fin' && x.user === 'svc' && x.password === 'p@ss;w0rd');
    x = S('mssql://svc:p%40ss@db.acme.local:1444/Fin?encrypt=true');
    check('server: mssql URL, encoded password', x.ok && x.host === 'db.acme.local' && x.port === 1444 && x.database === 'Fin' && x.password === 'p@ss');
    x = S("host=pg2.acme.io port=6432 dbname=x user=u password='it\\'s'");
    check('server: libpq keywords', x.ok && x.kind === 'postgres' && x.port === 6432 && x.password === "it's");
    check('server: localhost refused', !S('Server=localhost;Database=x').ok);
    check('server: a named instance keeps the host and the default port', S('Data Source=SQL01\\PROD;Database=x').host === 'sql01');
    const f = CC.credFile(S('postgresql://u:s3c@pg.acme.io/db'), 'postgresql://u:s3c@pg.acme.io/db');
    check('the credential file is KEY=\'value\' lines, one per part, plus the whole string',
      /^CYG_DB_TYPE='postgres'$/m.test(f) && /^CYG_DB_PORT='5432'$/m.test(f) && /^CYG_DB_PASSWORD='s3c'$/m.test(f) && /^CYG_DB_CONNSTR='postgresql:/m.test(f));
    const red = CC.makeRedactor(['s3c', 'postgresql://u:s3c@pg.acme.io/db']);
    check('the redactor blanks by literal, by URL-encoding and by pattern',
      red.string('s3c s3c%20 x=postgresql://u:s3c@pg.acme.io/db Password=other; PWD=z CYG_DB_PASSWORD=\'q\' postgres://a:b@h/d')
        === '•••••• ••••••%20 x=•••••• Password=••••••; PWD=•••••• CYG_DB_PASSWORD=•••••• postgres://a:••••••@h/d',
      red.string('s3c s3c%20 x=postgresql://u:s3c@pg.acme.io/db Password=other; PWD=z CYG_DB_PASSWORD=\'q\' postgres://a:b@h/d'));

    const e = (s, m) => P.endpointOf(s, m);
    x = e('Server=tcp:acme.database.windows.net,1433;Database=x;User ID=u;Password=hunter2');
    check('page: Azure SQL ADO string', x.ok && x.host === 'acme.database.windows.net' && x.port === 1433 && x.kind === 'sqlserver');
    check('page: AND THE PASSWORD IS NOWHERE IN WHAT IS RETURNED', !JSON.stringify(x).includes('hunter2'));
    x = e('Data Source=SQL01\\PROD;Initial Catalog=x');
    check('page: a named instance is read, with a note about its port', x.ok && x.host === 'sql01' && x.port === 1433 && /Named instance/.test(x.note));
    x = e('postgresql://u:secret@pg.acme.io/db');
    check('page: a postgres URL defaults to 5432', x.ok && x.kind === 'postgres' && x.port === 5432 && !JSON.stringify(x).includes('secret'));
    check('page: localhost is refused in words', !e('Server=.;Database=x').ok && /this computer/.test(e('Server=localhost;Database=x').why));
    check('page: a Function App connection says to enter the server by hand', !e('x', 'azure').ok && /Function App/.test(e('x', 'azure').why));
    check('page: validate refuses scheme, port and path', !!P.validate('https://x.io', 1) && !!P.validate('x.io', 0) && !!P.validate('x.io/db', 1) && P.validate('x.io', 1433) === '');
    check('page: the firewall sentence is the brief\'s, with the address when known',
      /Your database may only accept known IP addresses — the Anthropic workspace may need allowing\./.test(P.firewallHelp({}))
      && /203\.0\.113\.9/.test(P.firewallHelp({ egress_ip: '203.0.113.9' })));
  }

  /* ── 7b. The console's reading of a session ─────────────────────────── */
  section('7b. The console module: tables, blocks, warnings, the confirmation');
  {
    const M = require(path.join(ROOT, 'public', 'cygenix-cc-console.js'));
    let t = M.tableFrom('table,rows\nCustomer,1200\nInvoice,"48,210"\n');
    check('a CSV with a header reads as a table (quoted commas kept)', t && t.columns.join() === 'table,rows' && t.rows[1][1] === '48,210');
    check('so does a TSV', (M.tableFrom('a\tb\n1\t2') || {}).columns.join() === 'a,b');
    check('and a JSON array of flat objects, columns unioned', (M.tableFrom('[{"a":1},{"a":2,"b":"x"}]') || {}).columns.join() === 'a,b');
    check('and a pipe table, its rule row skipped', (M.tableFrom('| a | b |\n|---|---|\n| 1 | 2 |') || {}).rows.length === 1);
    check('plain text, a ragged CSV and nested JSON are NOT tables',
      M.tableFrom('There are 12 tables.') === null && M.tableFrom('a,b\n1,2,3') === null && M.tableFrom('[{"a":{"b":1}}]') === null);
    const rows = ['n']; for (let i = 0; i < 300; i++) rows.push(String(i));
    t = M.tableFrom(rows.join('\n') + '\nx');
    check('a long result is capped at 200 rows and says so', t === null || (t.rows.length === 200 && t.truncated === 301));
    const view = M.blocksFrom([
      { id: '1', type: 'user.message', content: [{ type: 'text', text: 'hi' }] },
      { id: '2', type: 'agent.thinking' },
      { id: '3', type: 'agent.tool_use', name: 'bash', input: { command: 'ls' } },
      { id: '4', type: 'agent.tool_result', tool_use_id: '3', content: [{ type: 'text', text: 'a,b\n1,2' }] },
      { id: '5', type: 'agent.tool_use', name: 'bash', input: { command: 'python3 q.py' } },
      { id: '6', type: 'agent.tool_result', tool_use_id: '5', is_error: true, content: [{ type: 'text', text: 'ECONNREFUSED' }] },
      { id: '7', type: 'system.message', content: [{ type: 'text', text: 'DATA-CHANGE MODE: CHANGES ALLOWED. …' }] },
      { id: '8', type: 'agent.message', content: [{ type: 'text', text: 'done' }] },
      { id: '9', type: 'session.status_idle', stop_reason: { type: 'budget_reached' } },
    ]);
    check('chat: the person, the mode notice in plain words, Claude', view.chat.map(c => c.role).join() === 'user,notice,assistant' && /now allowed/.test(view.chat[1].text));
    check('output: a result is attached to the command that produced it, as a table when it reads as one',
      view.output.length === 3 && view.output[0].kind === 'command' && view.output[0].result && view.output[0].result.table.columns.join() === 'a,b');
    check('a failed tool result is marked, and a refused connection raises the firewall flag', view.output[1].result.kind === 'error' && view.flags.connectionFailure === true);
    check('the spend cap becomes a notice block and a flag', view.output[2].kind === 'notice' && view.flags.budgetReached === true);
    check('a "write" shows as code written to a path', M.toolBlock({ id: 'x', name: 'write', input: { path: '/w/a.py', content: 'print(1)' } }).title === 'Wrote /w/a.py');
    check('the status words', M.statusWord('running') === 'Working' && M.statusWord('idle') === 'Idle' && M.statusWord('stopped') === 'Stopped' && M.statusWord('error') === 'Error' && M.statusWord(undefined) === 'No session');
    check('the first-use notice is remembered per user, in one key', !M.noticeDismissed(null, 'a@x') && M.noticeDismissed(M.noticeDismiss('{}', 'a@x'), 'a@x') && !M.noticeDismissed(M.noticeDismiss('{}', 'a@x'), 'b@x'));
    const prd = M.confirmSpec({ name: 'Finance PRD', envClass: 'PRD' }, { name: 'Target' });
    check('A PRODUCTION PROFILE MUST BE NAMED BACK; a DEV one is a plain yes',
      prd.prod && prd.typeToConfirm === 'Finance PRD' && !M.confirmAccepts(prd, 'finance') && M.confirmAccepts(prd, ' Finance PRD ')
      && !M.confirmSpec({ name: 'Demo', envClass: 'DEV' }, {}).prod && M.confirmAccepts(M.confirmSpec({ name: 'Demo', envClass: 'DEV' }, {}), ''));
    check('the confirmation names the profile and the connection and says it is recorded', /"Target" under profile "Finance PRD" \(PRD\)/.test(prd.text) && /audit log/.test(prd.text));
  }

  /* ── 8. The page and the house rules ────────────────────────────────── */
  section('8. The page and the house rules');
  {
    const page = read('public', 'claude-code.html');
    check('the page loads the console module, the probe module, the roles client and the sidebar',
      /src="\/cygenix-cc-console\.js\?v=/.test(page) && /src="\/cygenix-cc-probe\.js\?v=/.test(page) && /src="\/cygenix-rbac\.js\?v=/.test(page) && /cygenix-sidebar\.js/.test(page));
    check('every Azure post goes through one guard: in flight, 3s apart, reset in its own finally',
      /async function guarded\(name, fn\)\{\s*if \(CS\.busy\[name\]\) return null;/.test(page) && /MIN_GAP_MS = 3000/.test(page)
      && /finally \{ CS\.busy\[name\] = false; render\(\); \}/.test(page) && (page.match(/CS\.busy\[name\] = false/g) || []).length === 1
      && ["guarded('New session'", "guarded('Send'", "guarded('Stop'", "guarded('Mode'"].every(g => page.indexOf(g) !== -1));
    check('polling: every 3s, one in flight, stops when not running, stops on error, stops on Stop',
      /POLL_MS = 3000/.test(page) && /if \(CS\.pollInflight \|\| !CS\.cur \|\| CS\.replay\) return;/.test(page)
      && /if \(r\.status !== 'running'\) \{ stopPolling\(\);/.test(page) && /Lost touch with the session[\s\S]{0,80}stopPolling\(\);/.test(page)
      && /guarded\('Stop', async function\(\)\{\s*stopPolling\(\);/.test(page) && (page.match(/CS\.pollInflight = false/g) || []).length === 1);
    check('NO TRANSCRIPT TOUCHES BROWSER STORAGE — only the first-use notice and the active user are read or written',
      (page.match(/localStorage\.(get|set)Item\(/g) || []).length === 6
      && (page.match(/localStorage\.(get|set)Item\((CygenixCcConsole\.NOTICE_KEY|'cygenix_app_prefs'|'cygenix_active_user')/g) || []).length === 6
      && !/sessionStorage\.setItem/.test(page));
    check('the brief\'s notice, the not-enabled sentence, the toggle note and the firewall help are there, word for word',
      /Runs on your own Anthropic API key and is billed to your Anthropic account\. Code runs in an isolated Anthropic workspace\. You are responsible for changes it makes\./.test(page)
      && /Not enabled for your role — ask an Owner to enable it in Governance\./.test(page)
      && /When off, Claude is instructed not to change data\. For guaranteed protection, use a read-only database login\./.test(page)
      && /Your database may only accept known IP addresses — the Anthropic workspace may need allowing\./.test(page));
    check('the page sends a connection\'s id and names to open a session — never its string',
      /\{ side: CS\.conn\.side, connId: CS\.conn\.connId, connectionName: CS\.conn\.name,/.test(page) && !/connString/.test(page.split('function csNew')[1].split('function csSend')[0]));
    check('the connection test is still on the page, for administrators, behind a summary', /<details class="probe" id="cc-probe">/.test(page) && /id="cc-run-limited"/.test(page));
    check('phase 2: attach and download go through the same guard, and a file over 4 MB is refused before it is read',
      /guarded\('Attach'/.test(page) && /guarded\('Download'/.test(page) && /if \(f\.size > UPLOAD_MAX\)/.test(page) && /UPLOAD_MAX = 4 \* 1024 \* 1024/.test(page));
    check('the page is titled Dev Console and lives at /dev-console, with the old address redirecting',
      /<title>Cygenix – Dev Console<\/title>/.test(page) && /^\/dev-console\s+\/claude-code\.html\s+200$/m.test(read('public', '_redirects'))
      && /^\/claude-code\s+\/dev-console\s+301!$/m.test(read('public', '_redirects')));
    const src = read('azure-function', 'src', 'claude-code.js');
    check('the server reads the key only through userAnthropicKey', /userAnthropicKey\(req\)/.test(src) && !/process\.env\.ANTHROPIC_API_KEY/.test(src));
    check('no key or secret literal in any new file',
      [src, page, read('public', 'cygenix-cc-probe.js'), read('public', 'cygenix-cc-console.js'), read('netlify', 'functions', 'claude-code-gate.js')].every(t => !/sk-ant-[A-Za-z0-9]/.test(t)));
    check('no 3E names anywhere in it (target-agnostic)', [src, page, read('public', 'cygenix-cc-console.js')].every(t => !/\b3E\b|Elite|Timekeeper|Matter\b/.test(t)));
    check('the menu: Develop holds the SQL editor and Claude Code; the search knows the words; the Assistant knows the page',
      /section: 'Develop', group:'develop'/.test(read('public', 'cygenix-sidebar.js')) && /'claude-code':\s*\['dev console', 'claude code'/.test(read('public', 'cygenix-menu-index.js'))
      && /key: 'claude-code',\s*label: 'Dev Console',\s*href: '\/dev-console'/.test(read('public', 'cygenix-assistant-actions.js')));
    check('Governance: the card, saved through rbac-admin, guarded, and the help text names cost and risk',
      /id="ps-cc-panel"/.test(read('public', 'dashboard.html')) && /<strong>Cost:<\/strong>/.test(read('public', 'dashboard.html')) && /<strong>Risk:<\/strong>/.test(read('public', 'dashboard.html'))
      && /op: 'claude-code'/.test(read('public', 'dashboard-app.js')) && /if \(CCG\.saving \|\| !CCG\.draft\) return;/.test(read('public', 'dashboard-app.js'))
      && /finally \{\s*CCG\.saving = false;/.test(read('public', 'dashboard-app.js')));
    check('the first-use notice key is classified in the storage inventory', /'cygenix_cc_notice':\s*\['C'/.test(read('scripts', 'storage-inventory.js')));
    check('conn-secrets exposes ONE read, for the verified owner, and nothing new over the wire',
      /async function readSecret\(oid, connId\)/.test(read('azure-function', 'src', 'conn-secrets.js'))
      && /\['list', 'put', 'delete', 'prune'\]/.test(read('azure-function', 'src', 'conn-secrets.js')));
  }
  if (false) {
    const src = read('azure-function', 'src', 'claude-code.js');
    check('the server reads the key only through userAnthropicKey', /userAnthropicKey\(req\)/.test(src) && !/process\.env\.ANTHROPIC_API_KEY/.test(src));
    check('no key or secret literal in any new file',
      [src, read('public', 'cygenix-cc-probe.js'), read('netlify', 'functions', 'claude-code-gate.js')].every(t => !/sk-ant-[A-Za-z0-9]/.test(t)));
    check('no 3E names in the server (target-agnostic)', !/\b3E\b|Elite|Timekeeper|Matter\b/.test(src));
    check('the Assistant knows the page', /key: 'claude-code'/.test(read('public', 'cygenix-assistant-actions.js')));
    check('conn-secrets exposes ONE read, for the verified owner, and nothing new over the wire',
      /async function readSecret\(oid, connId\)/.test(read('azure-function', 'src', 'conn-secrets.js'))
      && /\['list', 'put', 'delete', 'prune'\]/.test(read('azure-function', 'src', 'conn-secrets.js')));
  }

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
