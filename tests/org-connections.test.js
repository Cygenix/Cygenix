// tests/org-connections.test.js — the organisation's connection register
// (Phase A of docs/design/server-held-connections.md).
//
// Three things are proved here, and the order matters:
//
//   1. The Netlify function DECIDES where the roles are. It is loaded for
//      real — its authorisation, its audit writes, its relay of the Function
//      App's answer — with the token verifier, the blob store and the
//      Function App swapped out. The fake Function App runs the REAL
//      validator lifted from azure-function/src/index.js, so a body carrying
//      a password is refused by the code that will refuse it in production.
//
//   2. The Function App's validators are pure and tested without a database:
//      what a record must carry, what it must not, what may change in place.
//
//   3. The browser module posts with the guards this codebase insists on —
//      one write in flight, three seconds between writes, one list request
//      shared among concurrent callers — and never touches a secret.
//
// Plus the pins that stop the trust model rotting: the data proxy does not
// forward the register actions and never sets the tenant header; the
// Function App refuses the actions without it; the RBAC rows are PA-only.

'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');
const Module = require('module');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));
const read = (...p) => fs.readFileSync(path.join(__dirname, '..', ...p), 'utf8');

const FN_DIR = path.join(__dirname, '..', 'netlify', 'functions');
const INDEX = read('azure-function', 'src', 'index.js');
const NETLIFY_FN = read('netlify', 'functions', 'org-connections.js');
const PROXY = read('netlify', 'functions', 'data-proxy.js');
const CLIENT = read('public', 'cygenix-org-connections.js');
const APP = read('public', 'dashboard-app.js');
const HTML = read('public', 'dashboard.html');
const SUMMARY = read('public', 'cygenix-project-summary.js');

// ── Lift the Function App's validators ────────────────────────────────────
// The block from the constants to orgConnId is pure: no Cosmos, no request.
function lift(src, startRe, endRe) {
  const s = src.search(startRe);
  const e = src.slice(s).search(endRe);
  if (s < 0 || e < 0) throw new Error('lift failed: ' + startRe);
  return src.slice(s, s + e);
}
const V = (() => {
  const code = lift(INDEX, /const ORG_CONN_CONTAINER = /, /\n\/\/ The eligibility rule for deleting a connection profile/);
  const sandbox = { Math, Number, Array, Object, String, RegExp, console };
  vm.runInNewContext(code + '\nthis.validateOrgConnection = validateOrgConnection; this.validateOrgConnectionPatch = validateOrgConnectionPatch; this.orgConnId = orgConnId; this.ORG_CONN_FORBIDDEN = ORG_CONN_FORBIDDEN; this.ORG_CONN_PATCHABLE = ORG_CONN_PATCHABLE;', sandbox);
  return sandbox;
})();

// ── The faked world under the Netlify function ────────────────────────────
let ACTOR = { oid: 'oid-pa', email: 'pa@acme.test', name: 'A PA', roles: ['PA'] };
const USERS = {};
for (const r of ['none', 'MB', 'EN', 'ML', 'PA']) USERS['oid-' + r] = { email: r.toLowerCase() + '@acme.test', isActive: true };
let TOKEN_OK = true;
const MEM = new Map();
const BLOBS = {
  get: async (k) => (MEM.has(k) ? JSON.parse(MEM.get(k)) : null),
  setJSON: async (k, v) => { MEM.set(k, JSON.stringify(v)); },
  set: async (k, v) => { MEM.set(k, JSON.stringify(String(v))); },
  delete: async (k) => { MEM.delete(k); },
  list: async () => ({ blobs: [...MEM.keys()].map(key => ({ key })) }),
};
const realRequire = Module.prototype.require;
Module.prototype.require = function (id) {
  if (id === '@netlify/blobs') return { getStore: () => BLOBS };
  if (id === './lib/entra-auth' || id === './entra-auth') return {
    verifyAuthHeader: async (event) => {
      const h = (event && event.headers) || {};
      if (!(h.authorization || h.Authorization) || !TOKEN_OK) throw new Error('invalid token');
      return { oid: ACTOR.oid, sub: ACTOR.oid, email: ACTOR.email, name: ACTOR.name };
    },
  };
  if (id === './lib/org-store' || id === './org-store') {
    const real = realRequire.call(this, path.join(FN_DIR, 'lib', 'org-store.js'));
    return Object.assign({}, real, {
      orgStore: () => BLOBS,
      resolveActor: async () => ({ oid: ACTOR.oid, email: ACTOR.email, name: ACTOR.name, roles: ACTOR.roles.slice(), isActive: true, user: { isActive: true } }),
      // Every actor this file signs in as is already in the directory, so
      // the first tenant to be created enrols them all — the organisation
      // shares ONE register, which is the whole point of Phase A. Without
      // this each role would land in a tenant of its own and the PA's
      // record would be invisible to the Member's list.
      loadAll: async () => ({
        users: USERS,
        assignments: ACTOR.roles.map(r => ({ id: 'ra_' + r, oid: ACTOR.oid, role: r, revokedAt: null })),
        classifications: {},
      }),
      invalidate: () => {},
    });
  }
  return realRequire.apply(this, arguments);
};
process.env.NETLIFY_SITE_ID = 'site-test';
process.env.NETLIFY_API_TOKEN = 'token-test';

// A Function App in miniature: one tenant's register, the real validator,
// and a `holders` switch for the lock rule. Records every request.
const upstream = { calls: [], docs: new Map(), holders: 0, mode: 'ok' };
global.fetch = async (url, init) => {
  const u = new URL(url);
  const action = u.pathname.split('/').pop();
  const body = init && init.body ? JSON.parse(init.body) : null;
  upstream.calls.push({ action, method: init.method, headers: init.headers, body, code: u.searchParams.get('code'), qs: u.search });
  const res = (status, json) => ({ status, ok: status < 300, text: async () => JSON.stringify(json) });
  if (upstream.mode === 'down') return res(500, { error: 'boom' });
  const tenant = init.headers['x-cygenix-tenant'];
  if (!tenant) return res(403, { error: 'The organisation register is reached through the Cygenix site, not directly' });
  const user = init.headers['x-user-id'];
  if (action === 'org-connection-list') return res(200, { connections: [...upstream.docs.values()].filter(d => u.searchParams.get('includeRetired') === '1' || !d.retiredAt) });
  if (action === 'org-connection-get') { const d = upstream.docs.get(body.id); return d ? res(200, { connection: d }) : res(404, { error: 'No such connection' }); }
  if (action === 'org-connection-create') {
    const v = V.validateOrgConnection(body.connection);
    if (!v.ok) return res(400, { error: v.why });
    const doc = Object.assign({}, v.value, { id: V.orgConnId(Date.now()), tenantId: tenant, secretRef: null, createdBy: user, retiredAt: null });
    upstream.docs.set(doc.id, doc);
    return res(200, { connection: doc });
  }
  const existing = upstream.docs.get(body.id);
  if (!existing) return res(404, { error: 'No such connection' });
  if (action === 'org-connection-update') {
    if (existing.retiredAt) return res(409, { error: 'A retired connection cannot be changed' });
    const v = V.validateOrgConnectionPatch(body.patch);
    if (!v.ok) return res(400, { error: v.why });
    const next = Object.assign({}, existing, v.value);
    upstream.docs.set(body.id, next);
    return res(200, { connection: next, changed: Object.keys(v.value) });
  }
  if (action === 'org-connection-retire') {
    if (existing.retiredAt) return res(200, { connection: existing, alreadyRetired: true });
    if (upstream.holders) return res(409, { error: `Locked: ${upstream.holders} member(s) still have a non-retired profile bound to this connection. Retire those profiles first.` });
    const r = Object.assign({}, existing, { retiredAt: new Date().toISOString(), retiredBy: user });
    upstream.docs.set(body.id, r);
    return res(200, { connection: r });
  }
  return res(400, { error: 'unknown ' + action });
};

const HANDLER_PATH = path.join(FN_DIR, 'org-connections.js');
function loadHandler() { delete require.cache[HANDLER_PATH]; return require(HANDLER_PATH).handler; }

const org = require(path.join(FN_DIR, 'lib', 'org-store.js'));
const tenancy = require(path.join(FN_DIR, 'lib', 'tenancy.js'));
const rbac = require(path.join(FN_DIR, 'lib', 'rbac.js'));

function as(roles, email) {
  ACTOR = { oid: 'oid-' + (email || roles.join('') || 'none'), email: email || ((roles.join('') || 'nobody').toLowerCase() + '@acme.test'), name: roles.join('+'), roles: roles.slice() };
  tenancy.invalidate();
}
let handler;
async function call(ev) {
  const res = await handler(Object.assign(
    { httpMethod: 'GET', headers: { authorization: 'Bearer valid', 'user-agent': 'Test/1.0' }, queryStringParameters: {} }, ev));
  let json = null;
  try { json = JSON.parse(res.body); } catch {}
  return { status: res.statusCode, json, raw: res.body };
}
const get = (qs) => call({ httpMethod: 'GET', queryStringParameters: qs || {} });
const post = (body) => call({ httpMethod: 'POST', body: JSON.stringify(body) });
const lastCall = () => upstream.calls[upstream.calls.length - 1];
async function auditEntries() {
  const out = [];
  for (const [k, v] of MEM) if (k.startsWith('audit/e/')) out.push(JSON.parse(v));
  return out.sort((a, b) => a.seq - b.seq);
}
const SQL = { name: 'Finance prod source', side: 'src', kind: 'sqlserver', envClass: 'PRD', server: 'sqlprod01.internal', database: 'Conversion_DM', authType: 'sql', userName: 'svc_cyg_fin' };

(async () => {
  console.log('Organisation connection register — Phase A\n');

  /* ── 1. The validators ─────────────────────────────────────────────────── */
  section('1. What the Function App accepts (real validator, no database)');
  let v = V.validateOrgConnection(SQL);
  check('a complete SQL Server record is accepted', v.ok, v.why);
  check('and normalised: side/kind lower, env upper, port defaulted to 1433',
    v.ok && v.value.port === 1433 && v.value.side === 'src' && v.value.envClass === 'PRD' && v.value.endpoint === null);
  v = V.validateOrgConnection(Object.assign({}, SQL, { kind: 'postgres', port: '' }));
  check('PostgreSQL defaults the port to 5432', v.ok && v.value.port === 5432, v.why);
  v = V.validateOrgConnection(Object.assign({}, SQL, { envClass: '', authType: '' }));
  check('an unclassified record is UNKNOWN (treated as PROD downstream) and SQL auth by default',
    v.ok && v.value.envClass === 'UNKNOWN' && v.value.authType === 'sql', v.why);
  for (const k of V.ORG_CONN_FORBIDDEN) {
    v = V.validateOrgConnection(Object.assign({}, SQL, { [k]: 'hunter2' }));
    check('a body carrying "' + k + '" is refused, and the refusal names the field but not the value',
      !v.ok && v.why.indexOf(k) !== -1 && v.why.indexOf('hunter2') === -1, v.why);
  }
  check('the forbidden list covers the seven spellings a secret arrives under',
    ['connString', 'connectionString', 'password', 'fnKey', 'key', 'secret', 'pwd'].every(k => V.ORG_CONN_FORBIDDEN.includes(k)));
  check('no name, no record', !V.validateOrgConnection(Object.assign({}, SQL, { name: '  ' })).ok);
  check('a 121-character name is refused', !V.validateOrgConnection(Object.assign({}, SQL, { name: 'x'.repeat(121) })).ok);
  check('side must be src or tgt', !V.validateOrgConnection(Object.assign({}, SQL, { side: 'both' })).ok);
  check('kind must be one of the three', !V.validateOrgConnection(Object.assign({}, SQL, { kind: 'oracle' })).ok);
  check('a server with a semicolon (a smuggled connection-string fragment) is refused',
    !V.validateOrgConnection(Object.assign({}, SQL, { server: 'h;Password=x' })).ok);
  check('no database, no record', !V.validateOrgConnection(Object.assign({}, SQL, { database: '' })).ok);
  check('port 70000 is refused', !V.validateOrgConnection(Object.assign({}, SQL, { port: 70000 })).ok);
  const FN = { name: 'Product Function App', side: 'tgt', kind: 'azurefn', envClass: 'DEV', endpoint: 'https://cygenix-db-api-x.uksouth-01.azurewebsites.net/api/db' };
  v = V.validateOrgConnection(FN);
  check('an Azure Function record needs only an https endpoint; auth defaults to key and the SQL fields are null',
    v.ok && v.value.authType === 'key' && v.value.server === null && v.value.endpoint === FN.endpoint, v.why);
  check('http:// is refused', !V.validateOrgConnection(Object.assign({}, FN, { endpoint: 'http://x.net/api/db' })).ok);
  check('AN ENDPOINT CARRYING ?code= IS REFUSED — the key is a secret, not part of the address',
    !V.validateOrgConnection(Object.assign({}, FN, { endpoint: FN.endpoint + '?code=abc' })).ok);
  check('aliases are trimmed, emptied and capped at ten',
    V.validateOrgConnection(Object.assign({}, SQL, { aliases: [' a ', '', 'b', 1, 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'] })).value.aliases.length === 10);

  section('   …and what may change in place');
  check('name, aliases and envClass are patchable', V.ORG_CONN_PATCHABLE.join() === 'name,aliases,envClass');
  check('a name patch is accepted and trimmed', V.validateOrgConnectionPatch({ name: ' New ' }).value.name === 'New');
  check('envClass is upper-cased and checked', V.validateOrgConnectionPatch({ envClass: 'uat' }).value.envClass === 'UAT' && !V.validateOrgConnectionPatch({ envClass: 'LIVE' }).ok);
  check('THE SERVER FIELD CANNOT BE PATCHED — a changed endpoint is a new connection',
    !V.validateOrgConnectionPatch({ server: 'other' }).ok && /new connection/.test(V.validateOrgConnectionPatch({ server: 'other' }).why));
  check('nor the endpoint, database, port, kind or side',
    ['endpoint', 'database', 'port', 'kind', 'side', 'userName'].every(k => !V.validateOrgConnectionPatch({ [k]: 'x' }).ok));
  check('a patch carrying a password is refused', !V.validateOrgConnectionPatch({ name: 'n', password: 'x' }).ok);
  check('an empty patch is refused', !V.validateOrgConnectionPatch({}).ok);
  check('ids are conn_<time36>_<6 chars>', /^conn_[0-9a-z]+_[0-9a-z]{6}$/.test(V.orgConnId(Date.now())));

  /* ── 2. The trust model, pinned in source ─────────────────────────────── */
  section('2. Who can reach the register, pinned in the source');
  const ALLOWED = (() => {
    const m = PROXY.match(/const ALLOWED = new Set\(\[([\s\S]*?)\]\);/);
    return (m[1].match(/'([^']+)'/g) || []).map(s => s.slice(1, -1));
  })();
  check('THE DATA PROXY DOES NOT FORWARD ANY org-connection-* ACTION',
    ALLOWED.length > 20 && !ALLOWED.some(a => a.startsWith('org-connection')), ALLOWED.filter(a => a.startsWith('org-connection')).join());
  check('and never sets the tenant header', !/x-cygenix-tenant/.test(PROXY));
  check('the Function App reads the tenant only from x-cygenix-tenant',
    /String\(req\.headers\.get\('x-cygenix-tenant'\)/.test(INDEX) && !/req\.query\.get\('tenant/.test(INDEX));
  check('and refuses the five actions without it, 403',
    /if \(!tenantId\) return err\(403, 'The organisation register is reached through the Cygenix site, not directly'\)/.test(INDEX));
  check('the container is partitioned on /tenantId',
    /createIfNotExists\(\{ id: ORG_CONN_CONTAINER, partitionKey: \{ paths: \['\/tenantId'\] \} \}\)/.test(INDEX));
  check('the lock rule is a cross-member EXISTS query over connection_profiles, not a browser-side count',
    /EXISTS\(SELECT VALUE p FROM p IN c\.connection_profiles\.profiles/.test(INDEX) && /p\.status != 'retired' AND \(p\.srcConnId = @id OR p\.tgtConnId = @id\)/.test(INDEX));
  check('a retired record cannot be changed (409)', /if \(existing\.retiredAt\) return err\(409, 'A retired connection cannot be changed'\)/.test(INDEX));
  check('the Function App never logs the body — only id, kind, side, env and tenant',
    /ctx\.log\(`org-connection-create: \$\{doc\.id\} \(\$\{doc\.kind\}, \$\{doc\.side\}, \$\{doc\.envClass\}\)/.test(INDEX));
  check('the Netlify function sets both x-user-id and x-cygenix-tenant from the VERIFIED context, not from the request',
    /'x-user-id': ctx\.actor\.email/.test(NETLIFY_FN) && /'x-cygenix-tenant': ctx\.tenant\.id/.test(NETLIFY_FN)
    && !/headers\['x-user-id'\]|headers\['x-cygenix-tenant'\]/.test(NETLIFY_FN));
  check('and uses authz.authorize — the token is the user\'s, never the function key alone',
    /authz\.authorize\(event, \{ route: 'org-connections'/.test(NETLIFY_FN));
  check('its console lines carry no body', !/console\.(log|error|warn)\([^)]*\b(body|connection|patch)\b/.test(NETLIFY_FN));

  section('   …and the RBAC rows');
  const every = (roles, action, res) => rbac.can({ roles, isActive: true }, action, res || {});
  check('connection.create is a Platform Administrator act and nobody else\'s',
    every(['PA'], 'connection.create', { mutating: true, environment: 'PROD' }).allow
    && ['OW', 'ML', 'EN', 'AP', 'DO', 'VA', 'AU', 'MB', 'AT'].every(r => !every([r], 'connection.create', { mutating: true, environment: 'DEV' }).allow));
  check('so is connection.retire',
    every(['PA'], 'connection.retire', { mutating: true, environment: 'PROD' }).allow
    && ['OW', 'ML', 'EN', 'AP', 'DO', 'VA', 'AU', 'MB', 'AT'].every(r => !every([r], 'connection.retire', { mutating: true, environment: 'DEV' }).allow));
  check('a Member may view the register (connection.view R)', every(['MB'], 'connection.view').allow);
  check('an Engineer may edit a DEV record but not a PROD one (existing connection.edit rule reused)',
    every(['EN'], 'connection.edit', { mutating: true, environment: 'DEV' }).allow && !every(['EN'], 'connection.edit', { mutating: true, environment: 'PROD' }).allow);

  /* ── 3. The Netlify function, driven for real ─────────────────────────── */
  section('3. Configuration');
  delete process.env.CYGENIX_DATA_FN_KEY;
  handler = loadHandler();
  as(['PA']);
  let r = await get();
  check('WITHOUT CYGENIX_DATA_FN_KEY THE FUNCTION SAYS SO (503 no-fn-key) rather than calling with an empty key',
    r.status === 503 && r.json.code === 'no-fn-key' && upstream.calls.length === 0, r.raw);
  process.env.CYGENIX_DATA_FN_KEY = 'HOSTKEY-TEST';
  handler = loadHandler();

  section('4. Reading');
  TOKEN_OK = false;
  check('no token, no register (401)', (await get()).status === 401);
  TOKEN_OK = true;
  as([]);
  r = await get();
  check('a signed-in caller with no role is refused (403), and the Function App is never called',
    r.status === 403 && upstream.calls.length === 0, r.raw);
  as(['MB']);
  r = await get();
  check('a Member lists the register', r.status === 200 && Array.isArray(r.json.connections), r.raw);
  let c = lastCall();
  check('the hop carries the host key in the query, the verified actor and the tenant in headers, and the caller\'s bearer',
    c.code === 'HOSTKEY-TEST' && c.headers['x-user-id'] === 'mb@acme.test' && /^ten_|^tenant|./.test(c.headers['x-cygenix-tenant']) && c.headers['x-cygenix-tenant'].length > 3 && c.headers.Authorization === 'Bearer valid', JSON.stringify(c.headers));
  const TENANT = c.headers['x-cygenix-tenant'];
  await get({ includeRetired: '1' });
  check('includeRetired=1 passes through', /includeRetired=1/.test(lastCall().qs));

  section('5. Creating');
  as(['EN']);
  r = await post({ op: 'create', connection: SQL });
  check('an Engineer may not create (403), and nothing reaches the Function App',
    r.status === 403 && r.json.code === 'denied' && !upstream.calls.some(x => x.action === 'org-connection-create'), r.raw);
  let entries = await auditEntries();
  check('the refusal is in the audit chain as connection.create denied',
    entries.some(e => e.action === 'connection.create' && e.outcome === 'denied' && e.actorEmail === 'en@acme.test'));
  as(['ML']);
  check('a Migration Lead may not create either, even in DEV', (await post({ op: 'create', connection: Object.assign({}, SQL, { envClass: 'DEV' }) })).status === 403);
  as(['PA']);
  r = await post({ op: 'create', connection: SQL });
  check('a Platform Administrator creates one', r.status === 200 && r.json.connection && r.json.connection.id, r.raw);
  const ID = r.json.connection.id;
  c = lastCall();
  check('the create went to org-connection-create under the same tenant', c.action === 'org-connection-create' && c.headers['x-cygenix-tenant'] === TENANT);
  entries = await auditEntries();
  const created = entries.find(e => e.action === 'connection.create' && e.outcome === 'allowed');
  // The schema files a PROD-environment act under the locked `prod`
  // category regardless of what the caller named, so this entry cannot be
  // switched off by anyone — which is right for a production endpoint.
  check('and is audited: connection.create allowed, filed under the locked prod category, environment PROD, resource id, no secret fields',
    created && created.category === 'prod' && created.environment === 'PROD' && created.resourceId === ID
    && created.detail.server === 'sqlprod01.internal' && !('password' in created.detail) && created.detail.tenantId === TENANT, JSON.stringify(created));
  r = await post({ op: 'create', connection: Object.assign({}, SQL, { name: 'Leaky', password: 'hunter2' }) });
  check('A CREATE CARRYING A PASSWORD IS REFUSED BY THE FUNCTION APP (400) and relayed verbatim',
    r.status === 400 && /remove "password"/.test(r.json.error) && r.json.code === 'upstream-refused', r.raw);
  const everything = [...MEM.values()].join('\n') + r.raw;
  check('and the value appears nowhere — not in the response, not in any blob',
    everything.indexOf('hunter2') === -1);
  check('a refused create is not audited as allowed', !(await auditEntries()).some(e => e.action === 'connection.create' && e.outcome === 'allowed' && e.detail.name === 'Leaky'));
  const fnRec = await post({ op: 'create', connection: FN });
  check('an Azure Function record (DEV) is created too', fnRec.status === 200, fnRec.raw);
  const FN_ID = fnRec.json.connection.id;

  section('6. Changing');
  as(['EN']);
  r = await post({ op: 'update', id: ID, patch: { name: 'Renamed by EN' } });
  check('an Engineer may not rename a PROD record (403)', r.status === 403, r.raw);
  r = await post({ op: 'update', id: FN_ID, patch: { name: 'Renamed by EN' } });
  check('but may rename a DEV one — the existing connection.edit rule, with the environment read from the RECORD not the request',
    r.status === 200 && r.json.connection.name === 'Renamed by EN', r.raw);
  r = await post({ op: 'update', id: FN_ID, patch: { envClass: 'PRD' } });
  check('an Engineer may not reclassify (connection.classify is PA-only)', r.status === 403, r.raw);
  as(['ML']);
  check('nor may a Migration Lead', (await post({ op: 'update', id: FN_ID, patch: { envClass: 'PRD' } })).status === 403);
  as(['PA']);
  r = await post({ op: 'update', id: FN_ID, patch: { envClass: 'UAT' } });
  check('a PA reclassifies', r.status === 200 && r.json.connection.envClass === 'UAT', r.raw);
  entries = await auditEntries();
  const cls = entries.find(e => e.action === 'connection.classify' && e.outcome === 'allowed');
  check('audited as connection.classify with before and after',
    cls && cls.detail.before.envClass === 'DEV' && cls.detail.after.envClass === 'UAT' && cls.resourceId === FN_ID, JSON.stringify(cls));
  r = await post({ op: 'update', id: ID, patch: { server: 'elsewhere' } });
  check('an endpoint change is refused by the Function App (400) and relayed', r.status === 400 && /new connection/.test(r.json.error), r.raw);
  check('a missing id is 400', (await post({ op: 'update', patch: { name: 'x' } })).status === 400);
  check('an unknown id is 404, relayed', (await post({ op: 'update', id: 'conn_nope', patch: { name: 'x' } })).status === 404);
  check('an unknown op is 400', (await post({ op: 'explode', id: ID })).status === 400);
  check('bad JSON is 400', (await call({ httpMethod: 'POST', body: '{nope' })).status === 400);
  check('PUT is 405', (await call({ httpMethod: 'PUT', body: '{}' })).status === 405);

  section('7. Retiring');
  as(['ML']);
  check('a Migration Lead may not retire (403)', (await post({ op: 'retire', id: ID })).status === 403);
  as(['PA']);
  upstream.holders = 2;
  r = await post({ op: 'retire', id: ID });
  check('RETIRE IS REFUSED (409) WHILE A MEMBER\'S NON-RETIRED PROFILE BINDS IT, and the count comes through',
    r.status === 409 && /2 member/.test(r.json.error) && r.json.code === 'upstream-refused', r.raw);
  entries = await auditEntries();
  check('the refused retirement is audited as connection.retire denied, reason locked',
    entries.some(e => e.action === 'connection.retire' && e.outcome === 'denied' && e.detail.reason === 'locked' && e.resourceId === ID));
  check('the record is not retired', !upstream.docs.get(ID).retiredAt);
  upstream.holders = 0;
  r = await post({ op: 'retire', id: ID });
  check('with no holders it retires', r.status === 200 && r.json.connection.retiredAt, r.raw);
  entries = await auditEntries();
  check('audited as connection.retire allowed', entries.some(e => e.action === 'connection.retire' && e.outcome === 'allowed' && e.resourceId === ID));
  const n = (await auditEntries()).length;
  r = await post({ op: 'retire', id: ID });
  check('retiring again is idempotent (200, alreadyRetired) and writes no second audit line',
    r.status === 200 && r.json.alreadyRetired === true && (await auditEntries()).length === n, r.raw);
  check('a retired record refuses an update (409)', (await post({ op: 'update', id: ID, patch: { name: 'x' } })).status === 409);
  r = await get();
  check('the default list hides it', !r.json.connections.some(x => x.id === ID));
  r = await get({ includeRetired: '1' });
  check('includeRetired shows it', r.json.connections.some(x => x.id === ID));

  section('8. When the Function App is down');
  upstream.mode = 'down';
  r = await get();
  check('a 5xx upstream is 502 with the detail in band, not a bare failure', r.status === 502 && r.json.code === 'upstream' && /boom/.test(r.json.error), r.raw);
  upstream.mode = 'ok';
  check('the chain still verifies after all of the above', (await org.verifyChain(BLOBS)).ok === true);

  /* ── 4. The browser module ─────────────────────────────────────────────── */
  section('9. The browser module: guards and events');
  const events = [];
  const fetches = [];
  let TOKEN = 'tok';
  let respond = (method, body) => ({ status: 200, json: { connections: [] } });
  const win = {
    getCygenixIdToken: () => TOKEN,
    dispatchEvent: (e) => events.push(e),
    CustomEvent: class { constructor(type, init) { this.type = type; this.detail = init && init.detail; } },
    fetch: (url, init) => {
      fetches.push({ url, init });
      const body = init.body ? JSON.parse(init.body) : null;
      return Promise.resolve().then(() => {
        const r = respond(init.method, body);
        return { ok: r.status < 300, status: r.status, json: () => Promise.resolve(r.json) };
      });
    },
    Date, JSON, Array, Object, Promise, Math, String, Number, Error, console,
  };
  win.window = win;
  vm.runInNewContext(CLIENT.replace(/\bfetch\(/g, 'window.fetch(').replace(/new CustomEvent\(/g, 'new window.CustomEvent('), win);
  const O = win.CygenixOrgConnections;
  const tick = () => new Promise(res => setImmediate(res));
  check('the module exposes list, create, update, retire, cached, status', ['list', 'create', 'update', 'retire', 'cached', 'status'].every(k => typeof O[k] === 'function'));
  check('the source has no localStorage, no sessionStorage: nothing about the register is kept in the browser',
    !/localStorage|sessionStorage/.test(CLIENT));
  check('and its code never names a secret field (the header comment may say the word)',
    !/password|connString|fnKey/.test(CLIENT.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '')));

  TOKEN = '';
  let rr = await O.list();
  check('signed out: no request, code no-token', !rr.ok && rr.code === 'no-token' && fetches.length === 0);
  TOKEN = 'tok';
  respond = () => ({ status: 200, json: { connections: [{ id: 'conn_1', name: 'A', kind: 'sqlserver', side: 'src', envClass: 'DEV', server: 'h', port: 1433, database: 'd' }] } });
  const [a, b] = await Promise.all([O.list(), O.list()]);
  check('TWO CONCURRENT LISTS SHARE ONE REQUEST', fetches.length === 1 && a.ok && b.ok && a.connections.length === 1, fetches.length);
  check('the request carries the bearer and hits the Netlify function', /\/\.netlify\/functions\/org-connections$/.test(fetches[0].url) && fetches[0].init.headers.Authorization === 'Bearer tok');
  check('a changed list announces cygenix:org-connections-changed', events.some(e => e.type === 'cygenix:org-connections-changed' && e.detail.source === 'list'));
  rr = await O.list();
  check('a third list inside 30 s is answered from cache with no request', fetches.length === 1 && rr.cached === true);
  await O.list({ force: true });
  check('force bypasses the cache', fetches.length === 2);
  check('cached() is a copy', O.cached() !== O.cached() && O.cached()[0].id === 'conn_1');
  check('endpointOf renders host/db and hides a default port', O.endpointOf(O.cached()[0]) === 'h/d' && O.endpointOf({ kind: 'postgres', server: 'h', port: 5433, database: 'd' }) === 'h:5433/d');

  respond = (m, body) => ({ status: 200, json: { connection: { id: 'conn_2', name: body.connection.name, retiredAt: null } } });
  const w1 = O.create({ name: 'B', side: 'tgt', kind: 'postgres' });
  const w2 = await O.create({ name: 'C', side: 'tgt', kind: 'postgres' });
  check('A SECOND WRITE WHILE ONE IS IN FLIGHT IS REFUSED AS busy, not queued', !w2.ok && w2.code === 'busy');
  rr = await w1;
  check('the first completes and folds into the cache', rr.ok && O.cached().some(x => x.id === 'conn_2'));
  check('POST body is { op:"create", connection } with no secret fields', (() => { const p = JSON.parse(fetches[fetches.length - 1].init.body); return p.op === 'create' && p.connection.name === 'B' && !('password' in p.connection); })());
  rr = await O.retire('conn_2');
  check('a write inside three seconds of the last is refused as throttled', !rr.ok && rr.code === 'throttled');
  O._reset();
  respond = () => ({ status: 403, json: { error: 'Not permitted: no grant', code: 'denied' } });
  rr = await O.create({ name: 'D' });
  check('a 403 is classified denied and the status says so', !rr.ok && rr.code === 'denied' && O.status().state === 'denied');
  O._reset();
  respond = () => ({ status: 409, json: { error: 'Locked: 1 member still has…', code: 'upstream-refused' } });
  rr = await O.retire('conn_x');
  check('a 409 comes back with the server\'s own words for the page', !rr.ok && /Locked/.test(rr.message));
  O._reset();
  win.fetch = () => Promise.reject(new Error('offline'));
  rr = await O.list();
  check('a network failure is classified, not thrown', !rr.ok && rr.code === 'network' && O.status().state === 'error');

  /* ── 5. The page ───────────────────────────────────────────────────────── */
  section('10. The Connections view');
  check('the register panel is in dashboard.html with counter, PA-only New button, status line, form and list',
    ['id="orgconn-panel"', 'id="orgconn-count"', 'id="orgconn-new-btn"', 'id="orgconn-status"', 'id="orgconn-form"', 'id="orgconn-list"'].every(s => HTML.indexOf(s) !== -1));
  check('the form has no password or key field — the register is metadata',
    (() => { const m = HTML.match(/<form id="orgconn-form"[\s\S]*?<\/form>/); const inputs = m ? (m[0].match(/<(input|select)[^>]*>/g) || []) : [];
             return inputs.length >= 10 && !inputs.some(t => /type="password"|id="[^"]*(key|password|secret)[^"]*"/i.test(t)); })());
  check('the form carries the fields the validator needs',
    ['orgconn-f-name', 'orgconn-f-side', 'orgconn-f-kind', 'orgconn-f-env', 'orgconn-f-server', 'orgconn-f-port', 'orgconn-f-database', 'orgconn-f-auth', 'orgconn-f-user', 'orgconn-f-endpoint'].every(id => HTML.indexOf('id="' + id + '"') !== -1));
  check('the RBAC mirror and the register module are loaded, in that order, deferred',
    (() => { const i = HTML.indexOf('/cygenix-rbac.js?v='); const j = HTML.indexOf('/cygenix-org-connections.js?v='); return i > 0 && j > i; })());
  check('the page asks CygenixRBAC (the real global) for PA, never a misspelt one',
    /window\.CygenixRBAC/.test(APP) && !/CygenixRbac\b/.test(APP));
  check('roles are asked for once, and the answer redraws (no loop: the flag is set before the request, never by its callback)',
    /let _orgConnRolesAsked = false;/.test(APP) && /_orgConnRolesAsked = true;\s*try \{ R\.me\(\)\.then\(\(\) => orgConnRender\(\)\)/.test(APP) && !/_orgConnRolesAsked = false;\s*\}/.test(APP.slice(APP.indexOf('function orgConnEnsureRoles'))));
  check('opening the view renders, asks for roles, and fetches the list once', /connCloudStatus\(\);\s*orgConnRender\(\);\s*orgConnEnsureRoles\(\);/.test(APP) && /CygenixOrgConnections\.list\(\)\.then\(orgConnRender\)/.test(APP));
  check('the page redraws on cygenix:org-connections-changed', /addEventListener\('cygenix:org-connections-changed'/.test(APP));
  check('Retire confirms first and says the lock rule', /confirm\('Retire ' \+ \(c \? c\.name : id\)/.test(APP) && /refused while any non-retired profile binds it/.test(APP));
  check('Retire and New are drawn only for a PA (the server refuses everyone else regardless)',
    /pa \? '<button class="btn btn-ghost btn-sm" onclick="orgConnRetire/.test(APP) && /newBtn\.style\.display = pa \? '' : 'none'/.test(APP));
  check('every value drawn into the table is escaped', (APP.match(/orgConnEsc\(/g) || []).length >= 10);
  check('the submit sends the record through the module (never fetch directly) and shows the server\'s refusal in the form',
    /const r = await O\.create\(rec\)/.test(APP) && /orgConnFormError\(r\.message/.test(APP) && !/fetch\(['"]\/\.netlify\/functions\/org-connections/.test(APP));

  section('11. The dead key read is gone');
  check('cygenix-project-summary.js no longer reads CygenixSync.funcCode — getFunctionKey() returns "" and the proxy supplies the key',
    !/funcCode/.test(SUMMARY.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, ''))
    && /function getFunctionKey\(\)\s*\{[\s\S]{0,500}return '';\s*\}/.test(SUMMARY));

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
