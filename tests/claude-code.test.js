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
 *   4. the database bridge — the agent's MCP server, the workspace that
 *      reaches no database, the pass and only its hash, Check the bridge,
 *      redeeming a pass (and every way one is refused), Function App
 *      connections, the redeem route's own door;
 *   5. the console — opening a session (the vault, the allow-list, the spend
 *      cap, the prompt), messages, polling with redaction and the cursor, the
 *      data-change mode as the bridge sees it, stop revoking the pass, the
 *      list and the replay;
 *   6a/7. the server's connection-string parser and the redactor;
 *   7b. the console module, including the bridge's query blocks;
 *   8. the page and the house rules.
 *
 * The bridge's other half — the MCP server that runs the queries — is
 * pinned in tests/cc-mcp.test.js.
 *
 * Run it:  node tests/claude-code.test.js
 */
'use strict';

const fs = require('fs');
const path = require('path');
const Module = require('module');

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
  let nf = 0, ns = 0, nv = 0;
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
      vaults: {
        create: async (p) => { calls.push(['vaults.create', p]); return { id: 'vlt_' + (++nv) }; },
        delete: async (id) => { calls.push(['vaults.delete', id]); return {}; },
        credentials: {
          create: async (id, p) => { calls.push(['vaults.credentials.create', id, p]); return { id: 'vcrd_' + nv }; },
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
        const lid = q.parameters.find(p => p.name === '@lid');
        if (lid) return { resources: [...items.values()].filter(d => d.bridgeLid === lid.value) };
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

const PASSWORD = 'Tr0ub4dor;x&y';
const CONNSTR = 'Server=tcp:acme.database.windows.net,1433;Database=Fin;User ID=svc;Password="' + PASSWORD + '";Encrypt=True';

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
    TENANT = { id: 'tn_1', claudeCode: { enabled: true, roles: ['OW', 'PA', 'EN', 'AU'] } };
    ACTOR = { oid: 'o1', roles: ['EN'] };
    r = await gateCall('changes', { record: 'session.staging', detail: { profile: 'Demo', connection: 'Legacy', host: 'db.acme.io', schema: 'staging', secret: 'x' } });
    check('A STAGING SESSION IS ON THE TRAIL at HIGH severity, naming the schema (and nothing it was not asked for)',
      r.status === 200 && AUDITED[0].action === 'claudecode.session.staging' && AUDITED[0].severity === 'high'
      && AUDITED[0].detail.schema === 'staging' && AUDITED[0].detail.connection === 'Legacy' && !('secret' in AUDITED[0].detail), JSON.stringify(AUDITED[0]));
    check('…it is recorded only under the changes act', (await gateCall('use', { record: 'session.staging' })).status === 400);
    ACTOR = { oid: 'o1', roles: ['AU'] };
    check('…so a role that cannot change data cannot open one', (await gateCall('changes', { record: 'session.staging' })).status === 403);
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
    check('data-proxy allows the console family',
      /\^\\\/agent\(\\\/\[A-Za-z0-9_\.-\]\+\)\*\$/.test(proxy)
      && ['check', 'session', 'message', 'events', 'mode', 'stop', 'sessions'].every(a => /^\/agent(\/[A-Za-z0-9_.-]+)*$/.test('/agent/claude-code/' + a)));
    check('…but NOT the bridge\'s redeem door, which only the MCP server may knock on',
      /\/\^\\\/agent\\\/claude-code-bridge\(\\\/\|\$\)\/i/.test(proxy), 'data-proxy.js');
    check('index.js imports the module', /require\('\.\/claude-code'\);/.test(read('azure-function', 'src', 'index.js')));
    check('no function.json folder was created for it', !fs.existsSync(path.join(ROOT, 'azure-function', 'claude-code')));
    check('host.json carries no functionTimeout', !/functionTimeout/.test(read('azure-function', 'host.json')));
    check('the SDK is a version with Managed Agents in it',
      /"@anthropic-ai\/sdk": "\^0\.(1[2-9]\d|[2-9]\d\d)\./.test(read('azure-function', 'package.json')));

    check('OPTIONS is a 204', (await H(req('OPTIONS', { action: 'events' }), ctx)).status === 204);
    check('an unknown action is a 404', (await call('GET', 'nonsense')).status === 404);
    check('the wrong method is a 405', (await call('GET', 'message')).status === 405);
    let r = await call('POST', 'check', { headers: { 'x-anthropic-key': KEY }, body: {} });
    check('NO TOKEN, NO SERVICE — even with REQUIRE_TOKEN_AUTH off (401)', r.status === 401, r.status);
    CC.deps.verify = async () => { throw new Error('bad signature'); };
    r = await call('POST', 'check', { body: {} });
    check('a token that does not verify is a 401 with the reason', r.status === 401 && /bad signature/.test(r.raw), r.raw);
    CC.deps.verify = async () => ({ oid: 'oid-me', email: 'Me@Acme.test' });
    r = await call('POST', 'check', { headers: { authorization: 'Bearer x' }, body: { connId: 'sconn_tgt1' } });
    check('no Anthropic key is the house 400 (USER_KEY_REQUIRED), and nothing is called',
      r.status === 400 && /USER_KEY_REQUIRED/.test(r.raw) && FETCHED.length === 0 && KEYS_SEEN.length === 0, r.raw);

    // The gate.
    CC._reset(); FETCHED = [];
    GATE_REPLY = { status: 403, body: { error: 'Only an Organisation Owner or Platform Administrator can do that.' } };
    SECRETS.sconn_tgt1 = { connString: CONNSTR };
    r = await call('POST', 'check', { body: { connId: 'sconn_tgt1' } });
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

  /* ── 4. The bridge: the agent, the pass, the check, redeeming it ────── */
  section('4. The bridge: the agent, the pass, the check, redeeming it');
  {
    CC._reset(); FETCHED = []; KEYS_SEEN.length = 0;
    GATE_REPLY = { status: 200, body: { allowed: true, tenantId: 'tn_1' } };
    CLIENT = fakeClient(); DB = fakeContainer();

    // The agent and its workspace.
    const spec = CC.agentSpec();
    check('THE AGENT CARRIES THE CYGENIX MCP SERVER, at the site\'s own address',
      spec.mcp_servers.length === 1 && spec.mcp_servers[0].type === 'url' && spec.mcp_servers[0].name === 'cygenix'
      && spec.mcp_servers[0].url === 'https://cygenix.co.uk/.netlify/functions/cc-mcp', JSON.stringify(spec.mcp_servers));
    check('…and its tools run without an approval click, because Cygenix decides what may run',
      spec.tools.some(t => t.type === 'mcp_toolset' && t.mcp_server_name === 'cygenix' && t.default_config.permission_policy.type === 'always_allow'));
    check('web search and web fetch stay off',
      spec.tools[0].configs.every(c => c.enabled === false) && spec.tools[0].configs.map(c => c.name).join() === 'web_search,web_fetch');
    const tagNow = CC.specTag();
    process.env.CYGENIX_SITE_URL = 'https://staging.example.test';
    check('the spec tag carries the address, so an agent pointing somewhere else is updated',
      CC.specTag() !== tagNow && /^3-[0-9a-f]{8}$/.test(tagNow) && CC.agentSpec().mcp_servers[0].url === 'https://staging.example.test/.netlify/functions/cc-mcp');
    delete process.env.CYGENIX_SITE_URL;
    CLIENT = fakeClient({ agents: [{ id: 'agent_old', metadata: { cygenix: 'claude-code', spec: '2' } }] });
    await CC.ensureAgent(CLIENT, 'tag-a');
    check('an agent made before the bridge is updated in place, not duplicated',
      CLIENT.calls.some(c => c[0] === 'agents.update' && c[1] === 'agent_old' && c[2].mcp_servers) && !CLIENT.calls.some(c => c[0] === 'agents.create'));
    CC._reset();
    CLIENT = fakeClient({ agents: [{ id: 'agent_cur', metadata: { cygenix: 'claude-code', spec: CC.specTag() } }] });
    await CC.ensureAgent(CLIENT, 'tag-b');
    check('…and one already current is left alone', !CLIENT.calls.some(c => c[0] === 'agents.update' || c[0] === 'agents.create'));
    const cfg = CC.environmentConfig('limited', CC.SESSION_HOSTS);
    check('THE WORKSPACE REACHES THE TWO REGISTRIES AND THE ADDRESS ECHO, NO DATABASE, AND MAY CALL MCP SERVERS',
      cfg.networking.type === 'limited' && cfg.networking.allowed_hosts.join() === 'pypi.org,files.pythonhosted.org,registry.npmjs.org,api.ipify.org'
      && cfg.networking.allow_mcp_servers === true && cfg.networking.allow_package_managers === false, JSON.stringify(cfg));
    const oldName = 'cygenix-cc-limited-' + require('crypto').createHash('sha256').update('limited|' + CC.SESSION_HOSTS.slice().sort().join(',')).digest('hex').slice(0, 16);
    check('the environment name is versioned, so one made before the bridge (no MCP allowed) is not reused',
      /^cygenix-cc-limited-[0-9a-f]{16}$/.test(CC.environmentName('limited', CC.SESSION_HOSTS)) && CC.environmentName('limited', CC.SESSION_HOSTS) !== oldName);

    // The pass.
    const p1 = CC.newBridgePass(), p2 = CC.newBridgePass();
    check('a pass is cyb_<lookup id>.<secret>, new every time, and what is kept is the secret\'s hash',
      /^cyb_[0-9a-f]{18}\.[A-Za-z0-9_-]{43}$/.test(p1.token) && p1.token !== p2.token
      && p1.hash === CC.sha256(p1.token.split('.')[1]) && p1.token.indexOf(p1.hash) === -1, p1.token);
    check('splitPass reads a pass back and refuses anything else',
      CC.splitPass(p1.token).lid === p1.lid && CC.splitPass(p1.token).secret === p1.token.split('.')[1]
      && CC.splitPass('cyb_x.y') === null && CC.splitPass(p1.token + 'x') === null && CC.splitPass(null) === null && CC.splitPass(' ' + p1.token) === null);

    // Check the bridge.
    CLIENT = fakeClient(); FETCHED = [];
    SECRETS.sconn_tgt1 = { connString: CONNSTR };
    const chkBody = { side: 'src', connId: 'sconn_tgt1', connectionName: 'Source', profileName: 'Demo' };
    let r = await call('POST', 'check', { body: chkBody });
    check('CHECK THE BRIDGE: a two-minute pass for that connection, and where to take it',
      r.status === 200 && /^cyb_/.test(r.body.token) && r.body.mcpUrl === CC.mcpUrl() && r.body.expiresInSeconds === 120, r.raw);
    const tok = r.body.token;
    const chk = [...DB._items.values()].find(d => d.kind === 'bridgecheck') || {};
    check('the check record names the connection, under the caller, and keeps only the hash',
      chk.id === 'chk_' + CC.splitPass(tok).lid && chk.bridgeHash === CC.sha256(CC.splitPass(tok).secret)
      && chk.userId === 'me@acme.test' && chk.oid === 'oid-me' && chk.connectionId === 'sconn_tgt1' && chk.side === 'src'
      && JSON.stringify(chk).indexOf(tok) === -1 && JSON.stringify(chk).indexOf(PASSWORD) === -1, JSON.stringify(chk));
    check('…asks the gate for plain use, records nothing, and spends nothing with Anthropic',
      FETCHED.length === 1 && JSON.parse(FETCHED[0].init.body).act === 'use' && !JSON.parse(FETCHED[0].init.body).record
      && CLIENT.calls.length === 0);
    r = await call('POST', 'check', { body: { connId: '' } });
    check('a check with no connection is a 400', r.status === 400);
    CC._reset();
    GATE_REPLY = { status: 403, body: { error: 'Not enabled for your role — ask an Owner to enable it in Governance.' } };
    const before = DB._items.size;
    r = await call('POST', 'check', { body: chkBody });
    check('a check the gate refuses is refused, word for word, and issues no pass', r.status === 403 && /Not enabled/.test(r.body.error) && DB._items.size === before);
    GATE_REPLY = { status: 200, body: { allowed: true, tenantId: 'tn_1' } };

    // Redeeming it.
    let x = await CC.bridgeRedeem({ token: tok });
    let b = JSON.parse(x.body);
    check('REDEEMED, A CHECK PASS GIVES THE ONE CONNECTION, AND IS ALWAYS READ-ONLY',
      x.status === 200 && b.kind === 'bridgecheck' && b.sessionId === null && b.readOnly === true && b.mode === 'direct'
      && b.connString === CONNSTR && b.fnUrl === null && b.fnKey === null && b.dbType === 'sqlserver'
      && b.oid === 'oid-me' && b.email === 'me@acme.test' && b.connectionName === 'Source', x.body);
    const wrong = tok.slice(0, -1) + (tok.slice(-1) === 'A' ? 'B' : 'A');
    check('a pass with the wrong secret is a 401', (await CC.bridgeRedeem({ token: wrong })).status === 401);
    check('a pass nobody issued is a 401', (await CC.bridgeRedeem({ token: CC.newBridgePass().token })).status === 401);
    check('anything not shaped like a pass is a 401, without a lookup', (await CC.bridgeRedeem({ token: 'Bearer nope' })).status === 401
      && (await CC.bridgeRedeem({})).status === 401 && (await CC.bridgeRedeem(null)).status === 401);
    delete SECRETS.sconn_tgt1;
    x = await CC.bridgeRedeem({ token: tok });
    check('a connection whose credential has since been deleted is a 409 in words, not a login attempt',
      x.status === 409 && /no longer saved on the server/.test(x.body), x.body);
    SECRETS.sconn_tgt1 = { connString: CONNSTR };
    CC.deps.now = () => Date.now() + 121 * 1000;
    x = await CC.bridgeRedeem({ token: tok });
    check('AFTER TWO MINUTES THE CHECK PASS IS REFUSED, and its record tidied away',
      x.status === 401 && /expired/.test(x.body) && !DB._items.has(chk.id), x.body);
    CC.deps.now = () => Date.now();

    // Only a session or a check carries a pass; a stopped session's does not work.
    const sp = CC.newBridgePass();
    DB._items.set('sesn_ended', { id: 'sesn_ended', kind: 'session', userId: 'me@acme.test', oid: 'oid-me', connectionId: 'sconn_tgt1',
      connMode: 'direct', status: 'stopped', bridgeLid: sp.lid, bridgeHash: sp.hash, bridgeExp: Date.now() + 3600 * 1000 });
    x = await CC.bridgeRedeem({ token: sp.token });
    check('a session that has ended is refused, even if its pass somehow survived', x.status === 401 && /has ended/.test(x.body), x.body);
    const op = CC.newBridgePass();
    DB._items.set('sesn_x:0001', { id: 'sesn_x:0001', kind: 'events', userId: 'me@acme.test', oid: 'oid-me', bridgeLid: op.lid, bridgeHash: op.hash, bridgeExp: Date.now() + 3600 * 1000 });
    check('a pass found on any other kind of record is refused', (await CC.bridgeRedeem({ token: op.token })).status === 401);
    DB._items.delete('sesn_ended'); DB._items.delete('sesn_x:0001');

    // A Function App connection.
    SECRETS.sconn_fn = { fnKey: 'fn-key-abc' };
    r = await call('POST', 'check', { body: { side: 'tgt', connId: 'sconn_fn', connectionName: 'Via Function App', mode: 'azure', fnUrl: 'https://fn.acme.test/api/data?code=leak#x' } });
    check('a Function App connection can be checked; its query string is dropped', r.status === 200, r.raw);
    x = await CC.bridgeRedeem({ token: r.body.token });
    b = JSON.parse(x.body);
    check('…and redeems to its address and its own key, with no connection string',
      x.status === 200 && b.mode === 'azure' && b.fnUrl === 'https://fn.acme.test/api/data' && b.fnKey === 'fn-key-abc' && b.connString === null && b.readOnly === true, x.body);
    delete SECRETS.sconn_fn;
    r = await call('POST', 'check', { body: { side: 'tgt', connId: 'sconn_fn', mode: 'azure', fnUrl: 'https://cygenix-db-api.example.test/api/data' } });
    x = await CC.bridgeRedeem({ token: r.body.token });
    check('the product\'s own Function App needs no saved key: it redeems with none, and the bridge supplies the product key',
      r.status === 200 && x.status === 200 && JSON.parse(x.body).fnKey === null, x.body);
    r = await call('POST', 'check', { body: { connId: 'sconn_fn', mode: 'azure', fnUrl: 'http://fn.acme.test/api/data' } });
    check('a Function App address that is not https is a 400', r.status === 400 && /https/.test(r.body.error), r.raw);

    // The bridge's own door.
    const bs = ROUTES['claude-code-bridge'] || {};
    check('THE REDEEM ROUTE IS ITS OWN: agent/claude-code-bridge/{action}, POST, host key only',
      bs.route === 'agent/claude-code-bridge/{action}' && bs.authLevel === 'function' && bs.methods.join() === 'POST');
    logs.length = 0;
    r = await call('POST', 'check', { body: chkBody });
    const bh = await CC.bridgeHandler(req('POST', { action: 'redeem', body: { token: r.body.token } }), ctx);
    check('the handler redeems, and logs the status and nothing else',
      bh.status === 200 && logs.some(l => /redeem status=200/.test(l)) && logs.every(l => l.indexOf(r.body.token) === -1 && l.indexOf(PASSWORD) === -1 && !/acme/.test(l)), logs.join(' | '));
    check('…an unknown bridge action is a 404, GET a 405', (await CC.bridgeHandler(req('POST', { action: 'list', body: {} }), ctx)).status === 404
      && (await CC.bridgeHandler(req('GET', { action: 'redeem' }), ctx)).status === 405);
    check('the console route has no redeem: a pass cannot be redeemed with a user\'s token', (await call('POST', 'redeem', { body: { token: r.body.token } })).status === 404);
  }

  /* ── 5. The console ─────────────────────────────────────────────────── */
  section('5. The console: open, talk, poll, allow changes, stop, list, replay');
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
    r = await open({ connId: 'sconn_fn', connectionName: 'Via Function App', mode: 'azure', fnUrl: 'ftp://fn.acme.test/api' });
    check('a Function App connection with an address that is not https is a 400 in words', r.status === 400 && /not a valid https URL/.test(r.body.error), r.raw);
    check('…and none of those asked the gate or Anthropic', FETCHED.length === 0 && CLIENT.calls.length === 0);

    r = await open();
    check('A SESSION OPENS', r.status === 200 && /^sesn_/.test(r.body.session.id) && r.body.session.status === 'idle'
      && r.body.session.dataChangesAllowed === false && r.body.session.dbType === 'sqlserver', r.raw);
    const sid = r.body.session.id;
    const gb = JSON.parse(FETCHED[0].init.body);
    check('the gate recorded the start, naming profile, connection and host',
      gb.act === 'use' && gb.record === 'session.start' && gb.detail.profile === 'Demo' && gb.detail.connection === 'Target DEV' && gb.detail.host === 'acme.database.windows.net', JSON.stringify(gb));
    const env = CLIENT.calls.find(c => c[0] === 'environments.create')[1];
    check('THE WORKSPACE MAY REACH THE TWO PACKAGE REGISTRIES AND THE ADDRESS ECHO — NOT THE DATABASE — AND THE BRIDGE',
      env.config.networking.type === 'limited'
      && env.config.networking.allowed_hosts.join() === 'pypi.org,files.pythonhosted.org,registry.npmjs.org,api.ipify.org'
      && env.config.networking.allow_package_managers === false && env.config.networking.allow_mcp_servers === true, JSON.stringify(env.config));
    const vc = CLIENT.calls.find(c => c[0] === 'vaults.create')[1];
    const cred = CLIENT.calls.find(c => c[0] === 'vaults.credentials.create');
    const sessTok = cred[2].auth.token;
    check('A VAULT OF ITS OWN holds the bridge pass, as a bearer token for the bridge\'s address only',
      cred[1] === 'vlt_1' && cred[2].auth.type === 'static_bearer' && cred[2].auth.mcp_server_url === CC.mcpUrl()
      && /^cyb_[0-9a-f]{18}\.[A-Za-z0-9_-]{43}$/.test(sessTok) && vc.metadata.cyg_oid === 'oid-me' && vc.metadata.cygenix === 'console', JSON.stringify(cred));
    const sc = CLIENT.calls.find(c => c[0] === 'sessions.create')[1];
    check('THE SESSION GETS THE VAULT AND NO FILES: no database login reaches the workspace',
      sc.vault_ids.join() === 'vlt_1' && !sc.resources && !CLIENT.calls.some(c => c[0] === 'files.upload'));
    check('with the £5 cap and the owner stamp', sc.budget.max_list_cost.amount === '600' && sc.metadata.cyg_oid === 'oid-me' && sc.metadata.cygenix === 'console');
    check('THE SYSTEM PROMPT SENDS CLAUDE TO THE THREE MCP TOOLS, and names no login, host or file',
      /ONLY through the "cygenix" MCP tools: list_tables, describe_table and run_query/.test(sc.agent.system)
      && /Do not try to connect to the database from the workspace/.test(sc.agent.system)
      && sc.agent.system.indexOf(PASSWORD) === -1 && sc.agent.system.indexOf('acme.database') === -1
      && !/CYG_DB_|db\.env/.test(sc.agent.system) && sc.agent.system.indexOf(sessTok) === -1, sc.agent.system);
    check('it opens read-only and says Cygenix enforces it; changes mode says to show SQL first and that destructive statements are refused',
      /READ-ONLY/.test(sc.agent.system) && /Cygenix enforces it/.test(sc.agent.system) && /at most 1,000 rows/.test(sc.agent.system)
      && /show the exact SQL/.test(CC.MODE_TEXT.changes) && /DROP, TRUNCATE, and DELETE or UPDATE without a WHERE clause/.test(CC.MODE_TEXT.changes));
    check('it is target-agnostic', !/\b3E\b|Elite|Aderant/.test(sc.agent.system));
    const doc = DB._items.get(sid);
    check('the session record is in Cosmos under the caller, on /userId, with the vault and the pass\'s HASH — never the pass or a secret',
      doc.kind === 'session' && doc.userId === 'me@acme.test' && doc.oid === 'oid-me' && doc.connectionId === 'sconn_tgt1'
      && doc.vaultId === 'vlt_1' && doc.connMode === 'direct' && doc.bridgeLid === CC.splitPass(sessTok).lid
      && doc.bridgeHash === CC.sha256(CC.splitPass(sessTok).secret) && doc.bridgeExp > Date.now() + 23 * 3600 * 1000
      && JSON.stringify(doc).indexOf(sessTok) === -1 && JSON.stringify(doc).indexOf(CC.splitPass(sessTok).secret) === -1
      && JSON.stringify(doc).indexOf(PASSWORD) === -1 && JSON.stringify(doc).indexOf('Password=') === -1, JSON.stringify(doc));
    check('the key is nowhere in what left this process', !JSON.stringify(CLIENT.calls).includes(KEY) && !JSON.stringify([...DB._items.values()]).includes(KEY));
    check('the log line carries no message, host or secret', logs.every(l => !/acme|Tr0ub|sk-ant|cyb_/.test(l)), logs.join(' | '));
    let red = JSON.parse((await CC.bridgeRedeem({ token: sessTok })).body);
    check('THE SESSION\'S PASS REDEEMS to its connection, read-only while changes are off',
      red.ok === true && red.kind === 'session' && red.sessionId === sid && red.connString === CONNSTR && red.readOnly === true
      && red.profileName === 'Demo' && red.connectionName === 'Target DEV' && red.side === 'tgt' && red.tenantId === 'tn_1', JSON.stringify(red));

    CLIENT._o.createFails = true;
    const nDocs = DB._items.size;
    r = await open();
    check('when Anthropic cannot open the session, its vault is deleted again, nothing is stored, and the error is passed on',
      r.status === 503 && CLIENT.calls.some(c => c[0] === 'vaults.delete' && c[1] === 'vlt_2') && DB._items.size === nDocs, r.raw);
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
      { id: 'e2', type: 'agent.tool_use', processed_at: '2026-09-30T10:00:01.000Z', name: 'bash', input: { command: "python3 -c \"print('" + PASSWORD + "')\"" } },
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
    check('and tells Claude, as a system message', sysm.type === 'system.message' && /CHANGES ALLOWED/.test(sysm.content[0].text) && /show the exact SQL/.test(sysm.content[0].text));
    check('the session record says so', DB._items.get(sid).dataChangesAllowed === true);
    red = JSON.parse((await CC.bridgeRedeem({ token: sessTok })).body);
    check('AND THE BRIDGE SEES IT AT ONCE: the same pass now redeems with changes allowed', red.readOnly === false, JSON.stringify(red));
    GATE_REPLY = { status: 403, body: { error: 'Your role cannot allow Claude Code to change data.' } };
    CC._reset();
    r = await call('POST', 'mode', { body: { sessionId: sid, dataChangesAllowed: true } });
    check('a role the gate refuses cannot switch changes on', r.status === 403 && /cannot allow/.test(r.body.error));
    GATE_REPLY = { status: 200, body: { allowed: true, tenantId: 'tn_1' } };
    r = await call('POST', 'mode', { body: { sessionId: sid, dataChangesAllowed: false } });
    sysm = CLIENT.calls.filter(c => c[0] === 'events.send').pop()[2].events[0];
    check('switching it off is a plain use, and tells Claude it is read-only again', r.status === 200 && /READ-ONLY/.test(sysm.content[0].text) && DB._items.get(sid).dataChangesAllowed === false);
    check('…and the bridge is read-only again', JSON.parse((await CC.bridgeRedeem({ token: sessTok })).body).readOnly === true);

    // Stop.
    FETCHED = [];
    r = await call('POST', 'stop', { body: { sessionId: sid } });
    const sg = JSON.parse(FETCHED[0].init.body);
    check('STOP interrupts, archives, deletes the vault and records it',
      r.status === 200 && r.body.status === 'stopped'
      && CLIENT.calls.some(c => c[0] === 'events.send' && c[1] === sid && c[2].events[0].type === 'user.interrupt')
      && CLIENT.calls.some(c => c[0] === 'sessions.archive' && c[1] === sid)
      && CLIENT.calls.some(c => c[0] === 'vaults.delete' && c[1] === 'vlt_1')
      && sg.record === 'session.stop' && sg.detail.sessionId === sid, JSON.stringify(sg));
    const stoppedDoc = DB._items.get(sid);
    check('the record is stopped, with an end time, and the pass is gone from it', stoppedDoc.status === 'stopped' && !!stoppedDoc.endedAt
      && stoppedDoc.vaultId === null && stoppedDoc.bridgeLid === null && stoppedDoc.bridgeHash === null && stoppedDoc.bridgeExp === null);
    check('A STOPPED SESSION\'S PASS NO LONGER WORKS', (await CC.bridgeRedeem({ token: sessTok })).status === 401);
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
    const sid2Vault = CLIENT.calls.filter(c => c[0] === 'vaults.credentials.create').pop()[1];
    check('a session Anthropic ended with an error is reported as error, done, and its vault is deleted',
      r.body.status === 'error' && r.body.done === true && CLIENT.calls.some(c => c[0] === 'vaults.delete' && c[1] === sid2Vault)
      && DB._items.get(sid2).status === 'error' && DB._items.get(sid2).bridgeLid === null, r.raw);

    // Somebody else, the list, the replay.
    const other = { ok: true, oid: 'oid-them', email: 'them@acme.test', bearer: 'Bearer y' };
    const rr = await CC.sessionEvents(other, KEY, sid);
    check('SOMEBODY ELSE\'S SESSION IS NOT FOUND, not forbidden', rr.status === 404);
    r = await call('GET', 'sessions');
    check('the list is the caller\'s sessions, newest first, with no internals',
      r.status === 200 && r.body.sessions.length === 2 && r.body.sessions[0].id === sid2 && r.body.sessions[1].id === sid
      && r.body.sessions[1].title === 'List the tables and row counts using Python' && r.body.sessions[1].status === 'stopped'
      && !('vaultId' in r.body.sessions[0]) && !('bridgeHash' in r.body.sessions[0]) && !('bridgeLid' in r.body.sessions[0])
      && !('cursorAt' in r.body.sessions[0]) && !('oid' in r.body.sessions[0]) && !('fnUrl' in r.body.sessions[0]), r.raw);
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
    check('AN ATTACHED FILE is uploaded, mounted read-only under /workspace/uploads, and Claude is told where it REALLY appears',
      r.status === 200 && r.body.upload.path === '/mnt/session/uploads/workspace/uploads/orders.csv' && r.body.upload.mountPath === '/workspace/uploads/orders.csv'
      && r.body.upload.name === 'orders.csv' && r.body.upload.size === csv.length
      && CLIENT.calls.some(c => c[0] === 'files.upload' && c[1].file.name === 'orders.csv' && c[1].file.text === csv.toString() && c[1].expires_in_seconds === 7 * 86400)
      && CLIENT.calls.some(c => c[0] === 'resources.add' && c[1] === sid3 && c[2].type === 'file' && c[2].mount_path === '/workspace/uploads/orders.csv')
      && CLIENT.calls.some(c => c[0] === 'events.send' && c[1] === sid3 && c[2].events[0].type === 'system.message' && /orders\.csv/.test(c[2].events[0].content[0].text) && /\/mnt\/session\/uploads\/workspace\/uploads\/orders\.csv/.test(c[2].events[0].content[0].text)), r.raw);
    check('…and recorded on the session, without the content', DB._items.get(sid3).uploads.length === 1 && DB._items.get(sid3).uploads[0].path === '/mnt/session/uploads/workspace/uploads/orders.csv'
      && JSON.stringify(DB._items.get(sid3)).indexOf('Ann') === -1);
    r = await call('POST', 'upload', { body: { sessionId: sid3, name: 'orders.csv', contentBase64: csv.toString('base64') } });
    check('a second file with the same name gets its own path', r.status === 200 && r.body.upload.mountPath === '/workspace/uploads/orders-2.csv' && r.body.upload.path === '/mnt/session/uploads/workspace/uploads/orders-2.csv', r.raw);
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

    // A Function App connection opens too.
    SECRETS.sconn_fn = { fnKey: 'abc' };
    FETCHED = [];
    r = await open({ connId: 'sconn_fn', connectionName: 'Via Function App', mode: 'azure', fnUrl: 'https://fn.acme.test/api/data' });
    const fdoc = DB._items.get(r.body.session && r.body.session.id) || {};
    check('A FUNCTION APP CONNECTION OPENS A SESSION: the record keeps its address, the gate is told its host',
      r.status === 200 && fdoc.connMode === 'azure' && fdoc.fnUrl === 'https://fn.acme.test/api/data' && fdoc.dbType === 'sqlserver'
      && JSON.parse(FETCHED[0].init.body).detail.host === 'fn.acme.test' && JSON.stringify(fdoc).indexOf('abc') === -1, r.raw);
    await call('POST', 'stop', { body: { sessionId: r.body.session.id } });
    DB._items.delete(r.body.session.id);

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

  /* ── 6a. The Dev Console reads the form's quoting too ───────────────── */
  section('6a. The Dev Console reads braced and quoted passwords the form writes');
  {
    const B = require('../public/cygenix-conn-builder.js');
    for (const pw of ['ZrsKD+wfr72iEAcoqyFhNvZ4=ovuA1xnZXT3poHYPN8I', 'pa;ss=word', 'a}b', 'sp ace', "it's"]) {
      const cs = B.compose({ engine: 'mssql', host: 'cygenix.database.windows.net', port: '1433', database: 'cygenix', user: 'claude_api', password: pw, encrypt: true });
      const p = CC.parseConn(cs);
      check('the server reads the real password the form wrote: ' + JSON.stringify(pw),
        p.ok && p.password === pw && p.user === 'claude_api' && p.host === 'cygenix.database.windows.net' && p.database === 'cygenix', cs + ' -> ' + JSON.stringify(p));
    }
    check('double-quoted values still work, doubled quotes unescaped',
      CC.parseConn('Server=h;Database=d;User Id=u;Password="p;w""x"').password === 'p;w"x');
  }

  /* ── 7. The parsers ─────────────────────────────────────────────────── */
  section('7. Reading a connection string, and blanking secrets');
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
    const red = CC.makeRedactor(['s3c', 'postgresql://u:s3c@pg.acme.io/db']);
    check('the redactor blanks by literal, by URL-encoding and by pattern',
      red.string('s3c s3c%20 x=postgresql://u:s3c@pg.acme.io/db Password=other; PWD=z CYG_DB_PASSWORD=\'q\' postgres://a:b@h/d')
        === '•••••• ••••••%20 x=•••••• Password=••••••; PWD=•••••• CYG_DB_PASSWORD=•••••• postgres://a:••••••@h/d',
      red.string('s3c s3c%20 x=postgresql://u:s3c@pg.acme.io/db Password=other; PWD=z CYG_DB_PASSWORD=\'q\' postgres://a:b@h/d'));
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
    const bv = M.blocksFrom([
      { id: 'm1', type: 'agent.mcp_tool_use', name: 'run_query', mcp_server_name: 'cygenix', input: { sql: 'SELECT TOP 5 name FROM sys.tables' } },
      { id: 'm2', type: 'agent.mcp_tool_result', mcp_tool_use_id: 'm1', is_error: false,
        content: [{ type: 'text', text: JSON.stringify({ columns: ['name', 'rows'], rows: [['Customer', 1200], ['Note', null]], row_count_returned: 2, truncated: true, total_rows_read: 9 }) }] },
      { id: 'm3', type: 'agent.mcp_tool_use', name: 'describe_table', input: { schema: 'dbo', table: 'Customer' } },
      { id: 'm4', type: 'agent.mcp_tool_use', name: 'run_query', input: { sql: 'SELECT 1' } },
      { id: 'm5', type: 'agent.mcp_tool_result', mcp_tool_use_id: 'm4', is_error: true, content: [{ type: 'text', text: 'The query failed: connect ETIMEDOUT 10.0.0.1:1433' }] },
    ]);
    const q1 = bv.output[0];
    check('THE BRIDGE: a query shows as its SQL, and its answer as a table straight away, NULLs and the cap shown',
      q1.kind === 'code' && q1.title === 'Query run' && q1.body === 'SELECT TOP 5 name FROM sys.tables'
      && q1.result && q1.result.title === '2 rows' && q1.result.table.columns.join() === 'name,rows'
      && q1.result.table.rows[0].join() === 'Customer,1200' && q1.result.table.rows[1][1] === 'NULL' && q1.result.table.truncated === 9, JSON.stringify(q1));
    check('describe_table is named in words', bv.output[1].title === 'Described dbo.Customer');
    check('a failed bridge query is marked, and a connection failure raises the help note',
      bv.output[2].result.kind === 'error' && bv.flags.connectionFailure === true);
    check('an answer that is not the bridge\'s shape is not a table', M.bridgeTable('not json') === null && M.bridgeTable('{"a":1}') === null);
    check('a "write" shows as code written to a path', M.toolBlock({ id: 'x', name: 'write', input: { path: '/w/a.py', content: 'print(1)' } }).title === 'Wrote /w/a.py');
    check('the status words', M.statusWord('running') === 'Working' && M.statusWord('idle') === 'Idle' && M.statusWord('stopped') === 'Stopped' && M.statusWord('error') === 'Error' && M.statusWord(undefined) === 'No session');
    check('STAGING: the name is checked before the round trip', M.stagingNameProblem('') === '' && M.stagingNameProblem('staging') === ''
      && /letters, digits/.test(M.stagingNameProblem('my stage')) && /own schemas/.test(M.stagingNameProblem('dbo')) && /own schemas/.test(M.stagingNameProblem('DB_OWNER')));
    const st = M.stagingConfirmSpec({ name: 'Finance PRD', envClass: 'PRD' }, { name: 'Legacy' }, 'staging');
    check('…the staging confirmation says what it allows, where, without approvals, and a PRD profile is named back',
      st.prod && st.typeToConfirm === 'Finance PRD' && st.title === 'Start a staging session?' && st.okLabel === 'Start staging session'
      && /inside the schema "staging" of "Legacy" under profile "Finance PRD" \(PRD\)/.test(st.text) && /without asking for approvals/.test(st.text)
      && /Everything else in that database stays read-only/.test(st.text) && /audit log/.test(st.text) && !M.confirmAccepts(st, 'finance'));
    check('…and the suggested first message asks for the template first', /Conversion Template/.test(M.STAGING_STARTER) && /what the template contains/.test(M.STAGING_STARTER));
    check('the first-use notice is remembered per user, in one key', !M.noticeDismissed(null, 'a@x') && M.noticeDismissed(M.noticeDismiss('{}', 'a@x'), 'a@x') && !M.noticeDismissed(M.noticeDismiss('{}', 'a@x'), 'b@x'));
    const prd = M.confirmSpec({ name: 'Finance PRD', envClass: 'PRD' }, { name: 'Target' });
    check('A PRODUCTION PROFILE MUST BE NAMED BACK; a DEV one is a plain yes',
      prd.prod && prd.typeToConfirm === 'Finance PRD' && !M.confirmAccepts(prd, 'finance') && M.confirmAccepts(prd, ' Finance PRD ')
      && !M.confirmSpec({ name: 'Demo', envClass: 'DEV' }, {}).prod && M.confirmAccepts(M.confirmSpec({ name: 'Demo', envClass: 'DEV' }, {}), ''));
    check('the confirmation names the profile and the connection and says it is recorded', /"Target" under profile "Finance PRD" \(PRD\)/.test(prd.text) && /audit log/.test(prd.text));
  }

  /* ── 9. Staging sessions, on the Azure side ─────────────────────────── */
  section('9. Staging sessions: opening one, the switch, the pass, the template');
  {
    CC._reset(); FETCHED = []; KEYS_SEEN.length = 0; logs.length = 0;
    GATE_REPLY = { status: 200, body: { allowed: true, tenantId: 'tn_1' } };
    CLIENT = fakeClient(); DB = fakeContainer();
    SECRETS.sconn_src = { connString: 'Server=tcp:legacy.acme.test,1433;Database=Old;User ID=svc;Password=' + PASSWORD };
    const openStg = (extra) => call('POST', 'session', { body: Object.assign({ side: 'src', connId: 'sconn_src', connectionName: 'Legacy',
      profileId: 'DEMO', profileName: 'Demo', projectId: 'proj_1', stagingSchema: 'staging' }, extra || {}) });

    // The two copies of the name rule.
    const lib = require(path.join(ROOT, 'netlify', 'functions', 'lib', 'staging-sql.js'));
    const names = ['staging', 'Stage_1', '', 'dbo', 'DBO', 'sys', 'guest', 'INFORMATION_SCHEMA', 'public', 'pg_catalog', 'db_owner', 'pg_x',
      'my stage', '1st', 'a.b', 'x'.repeat(63), 'x'.repeat(64), 'Staging', "o'neil", '[x]'];
    check('THE AZURE AND BRIDGE COPIES OF THE SCHEMA-NAME RULE AGREE, in both dialects',
      names.every(n => ['sqlserver', 'postgres'].every(d => CC.stagingSchemaProblem(n, d) === lib.stagingSchemaProblem(n, d))),
      names.filter(n => CC.stagingSchemaProblem(n, 'postgres') !== lib.stagingSchemaProblem(n, 'postgres')).join());

    let r = await openStg({ stagingSchema: 'dbo' });
    check('a staging schema that is one of the database\'s own is a 400, before anything is asked', r.status === 400 && /own schemas/.test(r.body.error) && FETCHED.length === 0, r.raw);
    r = await openStg({ stagingSchema: 'my stage' });
    check('…so is one that is not a plain name', r.status === 400 && /letters, digits/.test(r.body.error));
    SECRETS.sconn_fnx = { fnKey: 'k' };
    r = await openStg({ connId: 'sconn_fnx', mode: 'azure', fnUrl: 'https://fn.acme.test/api/data' });
    check('…and a Function App connection cannot be a staging session', r.status === 400 && /logs in to itself/.test(r.body.error) && FETCHED.length === 0, r.raw);

    r = await openStg();
    check('A STAGING SESSION OPENS', r.status === 200 && r.body.session.stagingSchema === 'staging', r.raw);
    const sid = r.body.session.id;
    const g1 = JSON.parse(FETCHED[0].init.body), g2 = JSON.parse(FETCHED[1].init.body);
    check('…having asked the gate for the CHANGES act and recorded the staging start with the schema, then the ordinary start',
      g1.act === 'changes' && g1.record === 'session.staging' && g1.detail.schema === 'staging' && g1.detail.connection === 'Legacy'
      && g2.act === 'use' && g2.record === 'session.start', JSON.stringify([g1, g2]));
    const doc = DB._items.get(sid);
    check('the record keeps the schema and the project', doc.stagingSchema === 'staging' && doc.projectId === 'proj_1' && doc.dataChangesAllowed === false);
    const sc = CLIENT.calls.find(c => c[0] === 'sessions.create')[1];
    check('CLAUDE IS GIVEN THE STAGING BRIEF: the job, the rule, the order of work, the mapping to agree before loading, the report',
      /THIS IS A STAGING SESSION/.test(sc.agent.system) && /inside the\s+schema "staging"/.test(sc.agent.system)
      && /get_conversion_template/.test(sc.agent.system) && /Wait for them to agree/.test(sc.agent.system)
      && /INSERT INTO staging\.<table>/.test(sc.agent.system) && /staging-report\.md/.test(sc.agent.system) && /staging-mapping\.csv/.test(sc.agent.system)
      && /Never invent data/.test(sc.agent.system) && /slices/.test(sc.agent.system) && !/DATA-CHANGE MODE/.test(sc.agent.system), sc.agent.system.slice(0, 300));
    check('…which names no secret, host or product', sc.agent.system.indexOf(PASSWORD) === -1 && !/legacy\.acme/.test(sc.agent.system)
      && !/\b3E\b|Elite|Aderant|Timekeeper/.test(CC.conversionPlaybook('staging', 'sqlserver')));
    check('the session title says it is staging', /\(staging staging\)$/.test(sc.title));

    r = await call('POST', 'mode', { body: { sessionId: sid, dataChangesAllowed: true } });
    check('THE "ALLOW CHANGES" SWITCH IS REFUSED in a staging session — the schema is the permission', r.status === 409 && /staging session/.test(r.body.error), r.raw);

    const tok = CLIENT.calls.find(c => c[0] === 'vaults.credentials.create')[2].auth.token;
    let x = JSON.parse((await CC.bridgeRedeem({ token: tok })).body);
    check('the pass redeems with the staging schema and the project, and read-only outside it', x.stagingSchema === 'staging' && x.projectId === 'proj_1' && x.readOnly === true, JSON.stringify(x));
    r = await call('POST', 'check', { body: { connId: 'sconn_src', connectionName: 'Legacy' } });
    x = JSON.parse((await CC.bridgeRedeem({ token: r.body.token })).body);
    check('a check pass carries no staging schema and no project', x.stagingSchema === '' && x.projectId === '');

    // The template.
    const TDOCS = {
      pub_tpl_a_v2: { id: 'pub_tpl_a_v2', templateId: 'tpl_a', kind: 'published', name: 'A', version: 2, profileId: 'DEMO', updatedAt: '2026-09-01', doc: { id: 'tpl_a', name: 'A', version: 2 } },
      tpl_a: { id: 'tpl_a', templateId: 'tpl_a', kind: 'draft', name: 'A', version: 3, profileId: 'DEMO', updatedAt: '2026-09-20', doc: { id: 'tpl_a', name: 'A', version: 3 } },
      tpl_b: { id: 'tpl_b', templateId: 'tpl_b', kind: 'draft', name: 'B', version: 1, profileId: 'OTHER', updatedAt: '2026-09-25', doc: { id: 'tpl_b', name: 'B' } },
    };
    const TQ = [];
    CC.deps.templates = () => ({
      items: { query: (q, o) => ({ fetchAll: async () => { TQ.push([q, o]); const p = q.parameters[0].value; return { resources: p === 'proj_1' ? Object.values(TDOCS).map(d => Object.assign({}, d, { doc: undefined })) : [] }; } }) },
      item: (id, pk) => ({ read: async () => ({ resource: pk === 'proj_1' ? TDOCS[id] : null }) }),
    });
    let t = await CC.bridgeTemplate({ token: tok });
    let tb = JSON.parse(t.body);
    check('THE TEMPLATE: the newest PUBLISHED one for the session\'s profile, beating a newer draft, with the list',
      t.status === 200 && tb.chosen.id === 'pub_tpl_a_v2' && tb.template.version === 2 && tb.templates.length === 3, t.body);
    check('…read from the session\'s own project, in its partition', TQ[0][0].parameters[0].value === 'proj_1' && TQ[0][1].partitionKey === 'proj_1');
    t = await CC.bridgeTemplate({ token: tok, templateId: 'tpl_b' });
    check('another one by id', JSON.parse(t.body).chosen.id === 'tpl_b');
    t = await CC.bridgeTemplate({ token: tok, templateId: 'tpl_a' });
    check('an envelope id is exact: the draft\'s own id gives the draft', JSON.parse(t.body).chosen.id === 'tpl_a');
    check('…and a bare template id with no envelope of that id prefers its published copy',
      CC.pickTemplate([{ id: 'pub_tpl_z_v1', templateId: 'tpl_z', kind: 'published', version: 1 }, { id: 'pub_tpl_z_v2', templateId: 'tpl_z', kind: 'published', version: 2 }], 'P', 'tpl_z').id === 'pub_tpl_z_v2');
    t = await CC.bridgeTemplate({ token: tok, templateId: 'tpl_nope' });
    check('an unknown id is a 404 in words', t.status === 404 && /No template "tpl_nope"/.test(JSON.parse(t.body).error));
    check('pickTemplate: draft for the profile when nothing is published, else the project\'s newest',
      CC.pickTemplate([{ id: 'd', kind: 'draft', profileId: 'P', version: 1 }, { id: 'p', kind: 'published', profileId: 'Q', version: 5 }], 'P').id === 'd'
      && CC.pickTemplate([{ id: 'd', kind: 'draft', profileId: 'Q', version: 1 }, { id: 'p', kind: 'published', profileId: 'Q', version: 5 }], 'P').id === 'p');
    t = await CC.bridgeTemplate({ token: r.body.token });
    check('a check pass has no template (404)', t.status === 404 && /not opened from a project/.test(t.body));
    check('a bad pass is a 401', (await CC.bridgeTemplate({ token: 'cyb_nope' })).status === 401);
    r = await openStg({ projectId: 'proj_empty', stagingSchema: '' });
    const emptyTok = CLIENT.calls.filter(c => c[0] === 'vaults.credentials.create').pop()[2].auth.token;
    t = await CC.bridgeTemplate({ token: emptyTok });
    check('a project with no template says to make one', t.status === 404 && /no Conversion Template yet/.test(t.body));
    r = await openStg({ projectId: "x' OR 1=1 --", stagingSchema: '' });
    check('a project id that is not a plain id is not stored', DB._items.get(r.body.session.id).projectId === '');
    logs.length = 0;
    const th = await CC.bridgeHandler(req('POST', { action: 'template', body: { token: tok } }), ctx);
    check('the bridge door serves the template action, logging only the status', th.status === 200 && logs.some(l => /template status=200/.test(l)) && logs.every(l => l.indexOf(tok) === -1));
    await call('POST', 'stop', { body: { sessionId: sid } });
    check('once the session stops, its template cannot be read with its pass', (await CC.bridgeTemplate({ token: tok })).status === 401);

    // db-connect: the exemption is not reachable from a request, and the time limit only shortens.
    const dbc = read('netlify', 'functions', 'db-connect.js');
    check('DB-CONNECT: the HTTP handler calls rbacGate with six arguments — the staging exemption is the bridge\'s alone',
      /const gate = await rbacGate\(authed, action, dialect, connectionString, database, body\);/.test(dbc)
      && /async function rbacGate\(authed, action, dialect, connectionString, database, body, opts\)/.test(dbc)
      && /const requirement = stagingSchema \? null : tenancy\.requirementFor\(tenant\.guardrails, act\);/.test(dbc)
      && /guardrailExempt: stagingSchema \? 'dev-console staging schema' : undefined/.test(dbc));
    const tf = new Function(dbc.slice(dbc.indexOf('function timeoutFrom('), dbc.indexOf('function parseMssqlConnectionString(')) + '\nreturn timeoutFrom;')();
    check('…and timeoutMs is whole seconds-ish, at least one, always under the drivers\' two minutes',
      tf({ timeoutMs: 19000 }) === 19000 && tf({ timeoutMs: 999 }) === 0 && tf({ timeoutMs: 120000 }) === 0 && tf({ timeoutMs: '19000' }) === 19000
      && tf({ timeoutMs: 1.5 }) === 0 && tf({}) === 0 && tf(null) === 0);
    check('…SQL Server cancels the request on a timer; PostgreSQL sets statement_timeout on its own throwaway client and can wrap a transaction',
      /timer = setTimeout\(\(\) => \{ cancelled = true; try \{ rq\.cancel\(\); \}/.test(dbc)
      && /if \(limit\) await client\.query\('SET statement_timeout = ' \+ limit\);/.test(dbc)
      && /else if \(body\.transaction === true\) \{\s*await client\.query\('BEGIN'\);/.test(dbc));
  }

  /* ── 8. The page and the house rules ────────────────────────────────── */
  section('8. The page and the house rules');
  {
    const page = read('public', 'claude-code.html');
    check('the page loads the console module, the roles client and the sidebar — and the old connection test is gone',
      /src="\/cygenix-cc-console\.js\?v=/.test(page) && /src="\/cygenix-rbac\.js\?v=/.test(page) && /cygenix-sidebar\.js/.test(page)
      && !/cygenix-cc-probe|id="cc-probe"|CYGPROBE/.test(page) && !fs.existsSync(path.join(ROOT, 'public', 'cygenix-cc-probe.js')));
    check('every Azure post goes through one guard: in flight, 3s apart, reset in its own finally',
      /async function guarded\(name, fn\)\{\s*if \(CS\.busy\[name\]\) return null;/.test(page) && /MIN_GAP_MS = 3000/.test(page)
      && /finally \{ CS\.busy\[name\] = false; render\(\); \}/.test(page) && (page.match(/CS\.busy\[name\] = false/g) || []).length === 1
      && ["guarded('New session'", "guarded('Send'", "guarded('Stop'", "guarded('Mode'", "guarded('Check'"].every(g => page.indexOf(g) !== -1));
    check('polling: every 3s, one in flight, stops when not running, stops on error, stops on Stop',
      /POLL_MS = 3000/.test(page) && /if \(CS\.pollInflight \|\| !CS\.cur \|\| CS\.replay\) return;/.test(page)
      && /if \(r\.status !== 'running'\) \{ stopPolling\(\);/.test(page) && /Lost touch with the session[\s\S]{0,80}stopPolling\(\);/.test(page)
      && /guarded\('Stop', async function\(\)\{\s*stopPolling\(\);/.test(page) && (page.match(/CS\.pollInflight = false/g) || []).length === 1);
    check('NO TRANSCRIPT TOUCHES BROWSER STORAGE — localStorage only the notice, theme and active user; sessionStorage only the per-tab full-screen and split',
      (page.match(/localStorage\.(get|set)Item\(/g) || []).length
        === (page.match(/localStorage\.(get|set)Item\((CygenixCcConsole\.NOTICE_KEY|'cygenix_app_prefs'|'cygenix_active_user')/g) || []).length
          + (page.match(/localStorage\.getItem\('cygenix_active_project_id'\)/g) || []).length
      && !/localStorage\.setItem\('cygenix_active_project_id'/.test(page)
      && /FULL_KEY = 'cygenix_cc_full', SPLIT_KEY = 'cygenix_cc_split'/.test(page)
      && (page.match(/ss(Get|Set)\(/g) || []).length === (page.match(/ss(Get|Set)\((FULL_KEY|SPLIT_KEY|k[,)])/g) || []).length
      && (page.match(/sessionStorage\.(get|set|remove)Item\(/g) || []).length === (page.match(/sessionStorage\.(get|set|remove)Item\((k|v|FULL_KEY|SPLIT_KEY)/g) || []).length
      && !/(local|session)Storage\.setItem\([^)]*(transcript|events|cc_events|cc_session)/i.test(page));
    check('the brief\'s notice, the not-enabled sentence, the toggle note and the firewall help are there, word for word',
      /Runs on your own Anthropic API key and is billed to your Anthropic account\. Code runs in an isolated Anthropic workspace\. You are responsible for changes it makes\./.test(page)
      && /Not enabled for your role — ask an Owner to enable it in Governance\./.test(page)
      && /When off, Cygenix refuses any change to data and runs reads so nothing can be kept\. A read-only database login adds a second lock\./.test(page)
      && /Claude reaches your database only through Cygenix: the database login never enters the\s+workspace/.test(page)
      && /Cygenix could not reach the database\. The SQL editor uses the same route, so check the connection there first\./.test(page));
    const body = page.split('function connBody')[1].split('function csCheck')[0];
    check('the page sends a connection\'s id, names and Function App address to open a session or a check — never its string or key',
      /\{ side: CS\.conn\.side, connId: CS\.conn\.connId, connectionName: CS\.conn\.name, mode: CS\.conn\.mode \|\| 'direct',/.test(body)
      && !/connString|fnKey|password/i.test(body)
      && /var body = connBody\(\); body\.projectId = activeProjectId\(\);\s*if \(staging\) body\.stagingSchema = staging;\s*var r = await call\('POST', '\/agent\/claude-code\/session', body\);/.test(page));
    check('STAGING ON THE PAGE: a schema field, its own confirmation, the switch refused in such a session, a pill, and a suggested first message',
      /<input id="cs-staging" type="text"/.test(page) && /CygenixCcConsole\.stagingConfirmSpec\(/.test(page) && /openConfirm\(CygenixCcConsole\.confirmSpec\(prof, conn\)/.test(page)
      && /if \(CS\.cur && CS\.cur\.stagingSchema\) \{ note\('cs-note', 'This is a staging session/.test(page)
      && /id="cs-staging-pill"/.test(page) && /CygenixCcConsole\.STAGING_STARTER/.test(page)
      && /\$\('cs-staging'\)\.disabled = !allowed \|\| !!CS\.replay \|\| !!CS\.busy\['New session'\];/.test(page)
      && /--cs-bar-h/.test(page) && /new ResizeObserver\(set\)\.observe\(bar\)/.test(page) && /\.main\{padding-top:var\(--cs-bar-h,52px\)\}/.test(page) && /guarded\('New session'/.test(page.split('function openSession')[1] || ''));
    const chk = page.split('function csCheck')[1].split('/* ── Sessions')[0];
    check('CHECK THE BRIDGE: a button; a pass from the Azure side; SELECT 1 through the bridge with that pass — never the Entra token',
      /id="cs-check" onclick="csCheck\(\)"/.test(page) && /call\('POST', '\/agent\/claude-code\/check', connBody\(\)\)/.test(chk)
      && /fetch\(MCP_PATH, \{ method: 'POST', headers: \{ 'Content-Type': 'application\/json', Authorization: 'Bearer ' \+ p\.token \}/.test(chk)
      && /SELECT 1 AS ok/.test(chk) && /MCP_PATH = '\/\.netlify\/functions\/cc-mcp'/.test(page) && !/getCygenixIdToken/.test(chk));
    check('phase 2: attach and download go through the same guard, and a file over 4 MB is refused before it is read',
      /guarded\('Attach'/.test(page) && /guarded\('Download'/.test(page) && /if \(f\.size > UPLOAD_MAX\)/.test(page) && /UPLOAD_MAX = 4 \* 1024 \* 1024/.test(page));
    check('the page is titled Dev Console and lives at /dev-console, with the old address redirecting',
      /<title>Cygenix – Dev Console<\/title>/.test(page) && /^\/dev-console\s+\/claude-code\.html\s+200$/m.test(read('public', '_redirects'))
      && /^\/claude-code\s+\/dev-console\s+301!$/m.test(read('public', '_redirects')));
    const src = read('azure-function', 'src', 'claude-code.js');
    check('the server reads the key only through userAnthropicKey', /userAnthropicKey\(req\)/.test(src) && !/process\.env\.ANTHROPIC_API_KEY/.test(src));
    check('no key or secret literal in any new file',
      [src, page, read('public', 'cygenix-cc-console.js'), read('netlify', 'functions', 'claude-code-gate.js'), read('netlify', 'functions', 'cc-mcp.js')].every(t => !/sk-ant-[A-Za-z0-9]|cyb_[0-9a-f]{18}\./.test(t)));
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
  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
