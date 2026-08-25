// tests/rbac-enforcement.test.js — the RBAC gate as wired into the
// db-connect endpoint: the server-side enforcement point of spec §11,
// exercised at handler level with the identity and the org store mocked.
//
// This is acceptance A-02 in executable form: "a crafted API call to the
// execute endpoint by an Engineer against a PROD connection returns 403,
// with the interface bypassed entirely."

'use strict';

const path = require('path');
const Module = require('module');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

// ── Mocks ────────────────────────────────────────────────────────────────
// Identity is swappable per call; the org store is in-memory. The real
// rbac policy module is NOT mocked — the point is to prove the wiring
// reaches it.
let CURRENT_USER = { email: 'en@x.y', oid: 'oid-en', sub: 'oid-en', name: 'Eng', roles: [] };
let ACTOR_ROLES = ['EN'];
const CLASSIFICATIONS = { 'prodhost|livedb': 'PROD', 'devhost|scratch': 'DEV' };
const AUDITED = [];

// In-memory stand-in for Netlify Blobs, shared by the org store and the
// tenancy module. Tenancy is NOT mocked: the point of these cases is that
// the real guardrail policy sits on this path, and a tenant created here
// starts with enforcement off (the migrated default), so the pre-existing
// expectations below are the right ones.
const FAKE_BLOBS = (() => {
  const mem = new Map();
  return {
    get: async (k) => (mem.has(k) ? JSON.parse(mem.get(k)) : null),
    setJSON: async (k, v) => { mem.set(k, JSON.stringify(v)); },
    delete: async (k) => { mem.delete(k); },
    list: async () => ({ blobs: [...mem.keys()].map(key => ({ key })) }),
    _mem: mem,
  };
})();

const realRequire = Module.prototype.require;
Module.prototype.require = function (id) {
  if (id === 'mssql') return { ConnectionPool: function () {} };
  if (id === 'pg') return { Client: function () {} };
  if (id === './lib/entra-auth') return { verifyAuthHeader: async () => CURRENT_USER };
  if (id === './lib/sql-entra') return { applyEntraAuth: c => c, resolveSqlConfig: c => c, connectWithRetry: async () => { throw new Error('no real DB in tests'); } };
  if (id === './lib/azure-sql-relay') return { shouldRelay: () => false, createRelayPool: () => null };
  if (id === './lib/org-store') return {
    // A real-enough blob store, so lib/tenancy runs its own logic here
    // rather than being mocked into agreement with the code under test.
    orgStore: () => FAKE_BLOBS,
    loadAll: async () => ({ users: { [CURRENT_USER.oid]: { email: CURRENT_USER.email, isActive: true } },
                            assignments: [], classifications: {} }),
    resolveActor: async () => ({ oid: CURRENT_USER.oid, email: CURRENT_USER.email, roles: ACTOR_ROLES, isActive: true }),
    classificationFor: async (_s, server, db) =>
      CLASSIFICATIONS[String(server).toLowerCase() + '|' + String(db).toLowerCase()] || 'PROD',
    appendAudit: async (_s, evt) => { AUDITED.push(evt); return evt; },
  };
  return realRequire.apply(this, arguments);
};
const dbc = require(path.join(__dirname, '..', 'netlify', 'functions', 'db-connect.js'));
Module.prototype.require = realRequire;

const call = async (action, opts) => {
  const body = Object.assign({
    action,
    connectionString: `mssql://user:pw@${opts.host || 'prodhost'}/${opts.db || 'livedb'}`,
  }, opts.body || {});
  const res = await dbc.handler({ httpMethod: 'POST', headers: { authorization: 'Bearer x' }, body: JSON.stringify(body) });
  return { status: res.statusCode, data: JSON.parse(res.body) };
};

(async () => {
  console.log('RBAC enforcement — the db-connect gate\n');

  // A-02: Engineer, PROD target, write SQL — refused at the API.
  ACTOR_ROLES = ['EN']; AUDITED.length = 0;
  let r = await call('execute', { body: { sql: 'DELETE FROM addresses' } });
  check('an Engineer writing to a PROD-classified target gets 403 (A-02)',
    r.status === 403 && /Migration Lead/.test(r.data.error), JSON.stringify(r.data));
  check('the refusal is audited as denied at high severity (A-10)',
    AUDITED.length === 1 && AUDITED[0].outcome === 'denied' && AUDITED[0].severity === 'high'
    && AUDITED[0].environment === 'PROD', JSON.stringify(AUDITED));

  // The same Engineer runs free on a DEV-classified target — the gate
  // passes and the request proceeds to the (mocked, failing) database.
  AUDITED.length = 0;
  r = await call('execute', { host: 'devhost', db: 'scratch', body: { sql: 'DELETE FROM addresses WHERE id=1' } });
  check('the same write on a DEV target passes the gate (fails only at the mock DB)',
    r.status !== 403, JSON.stringify(r.data));
  check('and the allowed write is audited with its destructive analysis',
    AUDITED.length === 1 && AUDITED[0].outcome === 'allowed'
    && AUDITED[0].detail.destructive && AUDITED[0].detail.destructive[0].bounded === true,
    JSON.stringify(AUDITED));

  // Unclassified target: PROD by default (A-13 direction).
  ACTOR_ROLES = ['EN'];
  r = await call('execute', { host: 'unknownhost', db: 'newdb', body: { sql: 'UPDATE t SET x=1' } });
  check('an unclassified target defaults to PROD and blocks the Engineer (A-13/R-15)',
    r.status === 403);

  // A Migration Lead passes the PROD gate (Phase 2 adds the approval).
  ACTOR_ROLES = ['ML']; AUDITED.length = 0;
  r = await call('execute', { body: { sql: 'DELETE FROM addresses' } });
  check('a Migration Lead passes the PROD gate',
    r.status !== 403, JSON.stringify(r.data));
  check('at high severity, with the unbounded DELETE parsed out (R-17)',
    AUDITED.length === 1 && AUDITED[0].severity === 'high'
    && AUDITED[0].detail.destructive[0].type === 'DELETE'
    && AUDITED[0].detail.destructive[0].bounded === false);

  // Reads on PROD are permitted for the Engineer — logged, not blocked.
  ACTOR_ROLES = ['EN'];
  r = await call('execute', { body: { sql: 'SELECT * FROM addresses' } });
  check('an Engineer reading from PROD is permitted (6.2: logged, read path only)',
    r.status !== 403, JSON.stringify(r.data));

  // A write smuggled through the read-shaped fetch-page action still gates.
  r = await call('fetch-page', { body: { sql: 'DELETE FROM t' } });
  check('a write smuggled through fetch-page gates as a write',
    r.status === 403);

  // Control plane: the Platform Administrator holds no data-plane grant.
  ACTOR_ROLES = ['PA'];
  r = await call('execute', { host: 'devhost', db: 'scratch', body: { sql: 'SELECT 1' } });
  check('a Platform Administrator cannot run SQL anywhere (SoD-3/A-05)',
    r.status === 403);
  r = await call('schema-tables', { host: 'devhost', db: 'scratch' });
  check('nor browse schema — the control plane reads no customer metadata',
    r.status === 403);

  // No roles at all: authenticated but unauthorised (A-01).
  ACTOR_ROLES = [];
  r = await call('schema-tables', { host: 'devhost', db: 'scratch' });
  check('an actor with no role assignment is denied everything (A-01)',
    r.status === 403 && /no active role/.test(r.data.error));

  // The batch path gates on the union of its statements.
  ACTOR_ROLES = ['EN'];
  r = await call('batch', { body: { batchSql: ['SELECT 1', 'TRUNCATE TABLE t'] } });
  check('a batch containing one write gates the whole batch as a write',
    r.status === 403);

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
