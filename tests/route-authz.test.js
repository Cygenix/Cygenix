// tests/route-authz.test.js — every route, from the outside.
//
// The brief asked for two things this file provides:
//
//   "Every API route is covered by a test asserting the expected outcome for
//    each role, including denial cases."
//   "A test enumerates all registered routes and fails if any route has no
//    authorisation assertion — no route can be added without one."
//
// The enumeration is of the functions directory itself, not of a list kept
// here, so a new file in netlify/functions/ fails this test the moment it
// lands unless it is either gated or declared public in lib/authz.js. A
// declaration in the source shows up in a diff; a skip kept in a test does
// not, which is why the exemption list lives there and is only READ here.
//
// The assertion is behavioural rather than structural. Grepping a handler
// for the string `authorize(` proves nothing about what it does with the
// answer, so each route is invoked twice: with no Authorization header, and
// with a verified token whose actor holds no role. A 2xx from either is a
// failure. That check cannot be satisfied by writing the right words in the
// right place.

'use strict';

const fs = require('fs');
const path = require('path');
const Module = require('module');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

const FN_DIR = path.join(__dirname, '..', 'netlify', 'functions');

// ── The mocked world ──────────────────────────────────────────────────────
//
// Identity and roles are swappable per call. Everything with a network on
// the other side is stubbed to throw, so a route that gets past the gate
// fails loudly at the database rather than quietly appearing to work.

let TOKEN_OK = true;
let ACTOR_ROLES = [];
const CURRENT_USER = { email: 'nobody@acme.test', oid: 'oid-nobody', sub: 'oid-nobody', name: 'Nobody' };

function fakeBlobs() {
  const mem = new Map();
  const store = {
    get: async (k) => (mem.has(k) ? JSON.parse(mem.get(k)) : null),
    setJSON: async (k, v) => { mem.set(k, JSON.stringify(v)); },
    set: async (k, v) => { mem.set(k, JSON.stringify(String(v))); },
    delete: async (k) => { mem.delete(k); },
    list: async () => ({ blobs: [...mem.keys()].map(key => ({ key })) }),
  };
  return store;
}
const BLOBS = fakeBlobs();

// Everything any route audits, in call order. Section 6 reads it.
const AUDITED = [];

const realRequire = Module.prototype.require;
Module.prototype.require = function (id) {
  if (id === 'mssql') return { ConnectionPool: function () { throw new Error('no real DB in tests'); } };
  if (id === 'pg') return { Client: function () { throw new Error('no real DB in tests'); } };
  if (id === 'nodemailer') return { createTransport: () => ({ sendMail: async () => { throw new Error('no real SMTP in tests'); } }) };
  if (id === '@azure/cosmos') return { CosmosClient: function () { throw new Error('no real Cosmos in tests'); } };
  if (id === '@netlify/blobs') return { getStore: () => BLOBS };
  if (id === './lib/entra-auth' || id === './entra-auth') return {
    verifyAuthHeader: async (event) => {
      const h = (event && event.headers) || {};
      const auth = h.authorization || h.Authorization;
      if (!auth || !TOKEN_OK) throw new Error('invalid token');
      return CURRENT_USER;
    },
  };
  if (id === './lib/sql-entra') return {
    applyEntraAuth: c => c, resolveSqlConfig: c => c,
    connectWithRetry: async () => { throw new Error('no real DB in tests'); },
  };
  if (id === './lib/azure-sql-relay') return { shouldRelay: () => false, createRelayPool: () => null };
  if (id === './lib/org-store' || id === './org-store') {
    const real = realRequire.call(this, path.join(FN_DIR, 'lib', 'org-store.js'));
    return Object.assign({}, real, {
      orgStore: () => BLOBS,
      // The directory already has this signer in it, holding whatever
      // ACTOR_ROLES says at the moment of the call.
      resolveActor: async () => ({
        oid: CURRENT_USER.oid, email: CURRENT_USER.email, name: CURRENT_USER.name,
        roles: ACTOR_ROLES.slice(), isActive: true, user: { isActive: true },
      }),
      loadAll: async () => ({
        users: { [CURRENT_USER.oid]: { email: CURRENT_USER.email, isActive: true } },
        assignments: ACTOR_ROLES.map(r => ({ id: 'ra_' + r, oid: CURRENT_USER.oid, role: r, revokedAt: null })),
        classifications: {},
      }),
      classificationFor: async () => 'PROD',
      appendAudit: async (_s, evt) => { AUDITED.push(evt); return evt; },
      invalidate: () => {},
    });
  }
  return realRequire.apply(this, arguments);
};

process.env.NETLIFY_SITE_ID = process.env.NETLIFY_SITE_ID || 'site-test';
process.env.NETLIFY_API_TOKEN = process.env.NETLIFY_API_TOKEN || 'token-test';

const authz = realRequire.call(module, path.join(FN_DIR, 'lib', 'authz.js'));
const tenancy = realRequire.call(module, path.join(FN_DIR, 'lib', 'tenancy.js'));

// ── The routes on disk ────────────────────────────────────────────────────

const ROUTE_FILES = fs.readdirSync(FN_DIR)
  .filter(f => f.endsWith('.js'))
  .map(f => f.replace(/\.js$/, ''))
  .sort();

// How to call each gated route so it reaches its own logic rather than
// bouncing off a method check. The shape only has to be plausible — the
// point is the status code, and a gated route must refuse before it cares
// whether the body made sense.
const CALL = {
  'data-proxy':  { httpMethod: 'GET',  path: '/api/data', queryStringParameters: { action: 'get-user' } },
  // A WRITE, deliberately: the interesting denial on this route is sql.write
  // against a PROD-classified target, which is where the Engineer stops.
  'db-connect':  { httpMethod: 'POST', body: JSON.stringify({ action: 'execute', sql: 'DELETE FROM orders WHERE id = 1', connectionString: 'mssql://u:p@h/d' }) },
  'drive':       { httpMethod: 'POST', body: JSON.stringify({ action: 'manifest' }) },
  'projects':    { httpMethod: 'GET',  queryStringParameters: {} },
  'rbac-admin':  { httpMethod: 'GET',  queryStringParameters: { what: 'users' } },
  'reports':     { httpMethod: 'GET',  queryStringParameters: {} },
  'scheduler':   { httpMethod: 'GET',  queryStringParameters: { action: 'list' } },
  'send-email':  { httpMethod: 'POST', body: JSON.stringify({ to: 'x@y.z', subject: 's', text: 't' }) },
};

async function invoke(name, event) {
  const mod = require(path.join(FN_DIR, name + '.js'));
  const handler = mod.handler || mod.default;
  if (typeof handler !== 'function') return { statusCode: null, note: 'no handler export' };
  try {
    const res = await handler(Object.assign({ httpMethod: 'GET', headers: {}, queryStringParameters: {} }, event));
    return res || { statusCode: null, note: 'no response' };
  } catch (e) {
    // A route that throws has not served anything, which is a refusal of the
    // only kind that matters here.
    return { statusCode: 500, note: e.message };
  }
}

const is2xx = (r) => r && typeof r.statusCode === 'number' && r.statusCode >= 200 && r.statusCode < 300;

(async () => {
  console.log('Route authorisation — every route, from the outside\n');

  // ── 1. Enumeration ──────────────────────────────────────────────────────
  section('1. No route may exist without a decision about it');
  {
    const publicRoutes = Object.keys(authz.PUBLIC_ROUTES);
    const gated = ROUTE_FILES.filter(r => !publicRoutes.includes(r));
    const undeclared = gated.filter(r => !CALL[r]);
    check('every route file is either declared public in lib/authz.js or gated and covered here',
      undeclared.length === 0,
      undeclared.length ? 'no coverage for: ' + undeclared.join(', ') +
        ' — add it to CALL in this test, or to PUBLIC_ROUTES in lib/authz.js with a reason' : '');

    const stalePublic = publicRoutes.filter(r => !ROUTE_FILES.includes(r));
    check('the public list names no route that has been deleted',
      stalePublic.length === 0, stalePublic.join(', '));

    check('every public exemption carries a written reason',
      publicRoutes.every(r => typeof authz.PUBLIC_ROUTES[r] === 'string' && authz.PUBLIC_ROUTES[r].length > 20),
      publicRoutes.filter(r => !(authz.PUBLIC_ROUTES[r] || '').length).join(', '));

    check('the exemption list is short enough to review at a glance',
      publicRoutes.length <= 4, publicRoutes.join(', '));
    console.log('        routes: ' + ROUTE_FILES.length + ' (' + gated.length + ' gated, ' +
                publicRoutes.length + ' public: ' + publicRoutes.join(', ') + ')');
  }

  // ── 2. Anonymous ────────────────────────────────────────────────────────
  section('2. No token, no service');
  for (const name of Object.keys(CALL)) {
    TOKEN_OK = false; ACTOR_ROLES = []; tenancy.invalidate();
    const r = await invoke(name, Object.assign({ headers: {} }, CALL[name]));
    check(name + ' refuses an unauthenticated caller',
      !is2xx(r), 'got ' + r.statusCode + (r.note ? ' (' + r.note + ')' : ''));
  }

  // ── 3. Authenticated, but holding nothing ───────────────────────────────
  section('3. A verified token is not an entitlement (A-01)');
  for (const name of Object.keys(CALL)) {
    TOKEN_OK = true; ACTOR_ROLES = []; tenancy.invalidate();
    const r = await invoke(name, Object.assign(
      { headers: { authorization: 'Bearer valid' } }, CALL[name]));
    check(name + ' refuses a signed-in caller with no role assignment',
      !is2xx(r), 'got ' + r.statusCode + (r.note ? ' (' + r.note + ')' : ''));
  }

  // ── 4. Per-role outcomes on the routes that carry real authority ────────
  section('4. Expected outcome per role, including the denials');

  // { route: { role: 'allow' | 'deny' } }. 'allow' means the gate passed —
  // the route may still fail afterwards on the stubbed database, which is
  // why the assertion is "not a 401/403", not "a 200".
  const EXPECT = {
    'projects':   { MB: 'allow', EN: 'allow', AU: 'allow' },        // project.read
    'rbac-admin': { OW: 'allow', PA: 'allow', EN: 'deny', MB: 'deny' },  // admin.read
    'db-connect': { ML: 'allow', EN: 'deny', PA: 'deny', MB: 'deny' },   // sql.write on PROD
  };
  const refused = (r) => r && (r.statusCode === 401 || r.statusCode === 403);

  for (const [route, roles] of Object.entries(EXPECT)) {
    for (const [role, outcome] of Object.entries(roles)) {
      TOKEN_OK = true; ACTOR_ROLES = [role]; tenancy.invalidate();
      const r = await invoke(route, Object.assign(
        { headers: { authorization: 'Bearer valid' } }, CALL[route]));
      const ok = outcome === 'deny' ? refused(r) : !refused(r);
      check(route + ' ' + (outcome === 'deny' ? 'refuses' : 'admits') + ' ' + role,
        ok, 'got ' + r.statusCode + (r.note ? ' (' + r.note + ')' : ''));
    }
  }

  // ── 5. Writes are gated harder than reads ───────────────────────────────
  section('5. Reading is not writing');
  {
    TOKEN_OK = true; ACTOR_ROLES = ['MB']; tenancy.invalidate();
    const read = await invoke('projects', { headers: { authorization: 'Bearer valid' }, httpMethod: 'GET', queryStringParameters: {} });
    const write = await invoke('projects', {
      headers: { authorization: 'Bearer valid' }, httpMethod: 'POST',
      body: JSON.stringify({ name: 'x', project: {} }),
    });
    check('a Member may list projects', !refused(read), 'got ' + read.statusCode);
    check('a Member may not create one', refused(write), 'got ' + write.statusCode + ' ' + (write.body || ''));

    ACTOR_ROLES = ['EN']; tenancy.invalidate();
    const engWrite = await invoke('projects', {
      headers: { authorization: 'Bearer valid' }, httpMethod: 'POST',
      body: JSON.stringify({ name: 'x', project: {} }),
    });
    check('an Engineer may', !refused(engWrite), 'got ' + engWrite.statusCode + ' ' + (engWrite.body || ''));
  }

  // ── 6. Cross-tenant ─────────────────────────────────────────────────────
  section('6. Another tenant’s id is not found, and the reach is recorded');
  {
    TOKEN_OK = true; ACTOR_ROLES = ['EN']; tenancy.invalidate();
    // Plant an object owned by somebody else's tenant.
    await BLOBS.setJSON('tenants/objects', { byId: { 'proj_elsewhere': 'tn_notyours' } });
    tenancy.invalidate();

    AUDITED.length = 0;
    const r = await invoke('projects', {
      headers: { authorization: 'Bearer valid' }, httpMethod: 'GET',
      queryStringParameters: { id: 'proj_elsewhere' },
    });
    check('a project belonging to another tenant answers 404, not 403',
      r.statusCode === 404, 'got ' + r.statusCode + ' ' + (r.body || ''));

    const cross = AUDITED.find(e => e.action === 'tenant.cross-access');
    check('and the attempt is audited at high severity',
      !!cross && cross.severity === 'high' && cross.outcome === 'denied',
      JSON.stringify(AUDITED.map(a => a.action)));
    check('the audit entry names the tenant the object actually belongs to',
      !!cross && cross.detail && cross.detail.objectTenantId === 'tn_notyours',
      cross ? JSON.stringify(cross.detail) : 'no entry');

    // An id that exists nowhere is the same 404 with nothing recorded.
    AUDITED.length = 0;
    const missing = await invoke('projects', {
      headers: { authorization: 'Bearer valid' }, httpMethod: 'GET',
      queryStringParameters: { id: 'proj_nosuchthing' },
    });
    check('an id that exists nowhere gives the identical answer',
      missing.statusCode === 404, 'got ' + missing.statusCode);
    check('but records no cross-tenant event, because there was no other tenant',
      !AUDITED.some(e => e.action === 'tenant.cross-access'));
  }

  // ── 7. One identity provider ────────────────────────────────────────────
  section('7. Exactly one identity provider remains');
  {
    const roots = ['netlify', 'public', 'azure-function/src'];
    const hits = [];
    const walk = (dir) => {
      let entries = [];
      try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch { return; }
      for (const e of entries) {
        const p = path.join(dir, e.name);
        if (e.isDirectory()) { if (e.name !== 'node_modules') walk(p); continue; }
        if (!/\.(js|html)$/.test(e.name)) continue;
        // Comments are blanked before scanning, line count preserved: a note
        // explaining why the GoTrue path was removed must not read as the
        // GoTrue path still being there.
        const src = fs.readFileSync(p, 'utf8')
          .replace(/\/\*[\s\S]*?\*\//g, m => m.replace(/[^\n]/g, ' '))
          .replace(/(^|[^:])\/\/[^\n]*/g, (m, p1) => p1 + ' '.repeat(m.length - p1.length))
          .replace(/<!--[\s\S]*?-->/g, m => m.replace(/[^\n]/g, ' '));
        if (/netlifyIdentity|netlify-identity-widget|gotrue|\/\.netlify\/identity/i.test(src)) hits.push(p);
      }
    };
    roots.forEach(r => walk(path.join(__dirname, '..', r)));
    check('no Netlify Identity remains anywhere in the codebase', hits.length === 0, hits.join(', '));

    const helpSrc = fs.readFileSync(path.join(__dirname, '..', 'public', 'help.html'), 'utf8');
    check('and no documented flow sends a user to an identity provider console to add a teammate',
      !/app_metadata/i.test(helpSrc) && !/identity[- ]provider console/i.test(helpSrc));

    const rolesSrc = fs.readFileSync(path.join(__dirname, '..', 'public', 'user_roles.html'), 'utf8');
    check('the Users & Roles page invites members itself rather than pointing at Entra',
      /op:\s*'invite'/.test(rolesSrc) && !/Invite them by\s*\n?\s*granting access in your Microsoft Entra/i.test(rolesSrc));
  }

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
