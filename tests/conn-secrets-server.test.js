// tests/conn-secrets-server.test.js — the encrypted store, on the Function App.
//
// azure-function/src/conn-secrets.js seals saved-connection credentials
// before they reach Cosmos and unseals them only for the signed-in owner.
// The claims that matter, each pinned below by driving the real handler
// with a stubbed verifier and an in-memory container:
//
//   · no key configured → 503 on every action, and the module still LOADS,
//     so a bad app setting cannot take the rest of /api/* down;
//   · no valid token → 401, and a function key in the query does not help;
//   · what lands in Cosmos carries iv/tag/ct and no readable credential;
//   · a record copied to another user, or another connection, will not
//     open — the binding is in the AAD, not in a lookup;
//   · a stale write is skipped, never allowed to roll a newer one back;
//   · one user's list never contains another user's records;
//   · validation refuses what the client store would never send;
//   · nothing in the file puts a credential into a log line or an error.
'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const Module = require('module');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

console.log('Encrypted connection secrets — the Function App side\n');

/* ── Stub @azure/functions so the module can be required and its route
      captured, without a Functions host. ──────────────────────────────── */
// @azure/functions is a dependency of azure-function/, not of the repo root,
// so it cannot be resolved from here at all — the loader is intercepted by
// name instead. jsonwebtoken and jwks-rsa (pulled in by entra-auth.js) are
// stubbed the same way when absent; the verifier is replaced below anyway.
const registered = [];
const STUBS = {
  '@azure/functions': { app: { http: (name, cfg) => registered.push({ name, cfg }) } },
  'jsonwebtoken': { verify: () => { throw new Error('stubbed'); } },
  'jwks-rsa': () => ({ getSigningKey: () => {} }),
};
const _load = Module._load;
Module._load = function (request, parent, isMain) {
  if (Object.prototype.hasOwnProperty.call(STUBS, request)) {
    try { return _load.call(this, request, parent, isMain); }
    catch (e) { if (e.code !== 'MODULE_NOT_FOUND') throw e; return STUBS[request]; }
  }
  return _load.call(this, request, parent, isMain);
};

const MOD_PATH = path.join(ROOT, 'azure-function', 'src', 'conn-secrets.js');
const SRC = read('azure-function', 'src', 'conn-secrets.js');
// The header explains what the code avoids, by name. Claims about what the
// CODE does are made against the code alone.
const CODE = SRC.split('\n').filter((l) => !/^\s*(\/\/|\*|\/\*)/.test(l)).join('\n');

// The key the tests seal with. A test fixture, not a secret: 32 zero-ish
// bytes made from a fixed phrase.
const KEY_B64 = crypto.createHash('sha256').update('cygenix test key, not a real one').digest('base64');

/* ── In-memory Cosmos container, shaped like the SDK calls the module makes ── */
function fakeContainer() {
  const docs = new Map();   // id → doc
  const notFound = () => Object.assign(new Error('not found'), { code: 404 });
  return {
    _docs: docs,
    items: {
      query: (q, opts) => ({
        fetchAll: async () => {
          const uid = (q.parameters || []).find((p) => p.name === '@uid');
          const pk = (opts && opts.partitionKey) || (uid && uid.value);
          return { resources: [...docs.values()].filter((d) => d.userId === pk) };
        },
      }),
      upsert: async (doc) => { docs.set(doc.id, Object.assign({}, doc)); return { resource: doc }; },
    },
    item: (id, pk) => ({
      read: async () => { const d = docs.get(id); if (!d || d.userId !== pk) throw notFound(); return { resource: d }; },
      delete: async () => { const d = docs.get(id); if (!d || d.userId !== pk) throw notFound(); docs.delete(id); return {}; },
    }),
  };
}

/* ── A request, the way the v4 model hands one over ───────────────────── */
function req(o) {
  const h = new Headers(o.headers || {});
  return {
    method: o.method || 'GET',
    headers: { get: (k) => h.get(k) },
    params: { action: o.action },
    query: { get: (k) => (o.query || {})[k] || null },
    json: async () => (o.body === undefined ? (() => { throw new Error('no body'); })() : o.body),
  };
}
const logged = [];
const ctx = { log: (...a) => logged.push(a.join(' ')), warn: (...a) => logged.push(a.join(' ')), error: (...a) => logged.push(a.join(' ')) };

const M = require(MOD_PATH);
let env = {};
let container = fakeContainer();
M._deps.env = () => env;
M._deps.container = async () => container;
// The verifier: 'good:<oid>' is a valid token for that oid; anything else fails.
M._deps.verify = async (tok) => {
  const m = /^good:([A-Za-z0-9-]+)$/.exec(tok);
  if (!m) throw new Error('jwt malformed');
  return { oid: m[1], email: m[1] + '@example.test' };
};
const call = (o) => M.handler(req(o), ctx);
const body = (r) => { try { return JSON.parse(r.body); } catch { return {}; } };

(async () => {
  /* ── 1. Registration ─────────────────────────────────────────────────── */
  section('1. Registered the v4 way');
  check('the module registers one HTTP route with app.http and nothing else',
    registered.length === 1 && registered[0].name === 'conn-secrets');
  check('at /api/secrets/{action}, anonymous at the host — the token check is in code',
    registered[0].cfg.route === 'secrets/{action}' && registered[0].cfg.authLevel === 'anonymous');
  check('index.js side-effect requires it beside the other route modules',
    /require\('\.\/conn-secrets'\);/.test(read('azure-function', 'src', 'index.js')));
  check('no function.json folder, and host.json still has no functionTimeout',
    !fs.existsSync(path.join(ROOT, 'azure-function', 'conn-secrets'))
    && !/functionTimeout/.test(read('azure-function', 'host.json')));
  check('the verifier is the shared one from entra-auth.js, not a second implementation',
    /require\('\.\/entra-auth'\)/.test(SRC) && /verifyJwt/.test(SRC) && !/jwks-rsa|jsonwebtoken/.test(SRC)
    && /verifyJwt/.test(read('azure-function', 'src', 'entra-auth.js').split('module.exports')[1]));

  /* ── 2. Not configured ───────────────────────────────────────────────── */
  section('2. No key configured');
  env = {};
  for (const a of ['list', 'put', 'delete', 'prune']) {
    const r = await call({ action: a, method: a === 'list' ? 'GET' : 'POST', headers: { authorization: 'Bearer good:alice' }, body: {} });
    check(a + ' answers 503 "secrets store not configured"', r.status === 503 && /not configured/.test(body(r).error), r.body);
  }
  env = { CONN_SECRETS_KEY: Buffer.from('short').toString('base64') };
  let r = await call({ action: 'list', headers: { authorization: 'Bearer good:alice' } });
  check('a key of the wrong length is "not configured" too, not a crash', r.status === 503, r.body);
  check('the key is read inside the handler, never at module load — a bad setting cannot take /api/* down',
    !/^const\s+\w+\s*=\s*.*CONN_SECRETS_KEY/m.test(SRC) && /function loadKey\(env\)/.test(SRC));
  check('and it is a plain module: registering it never touches process.env.CONN_SECRETS_KEY',
    CODE.indexOf('CONN_SECRETS_KEY') > CODE.indexOf('function loadKey'));

  /* ── 3. Identity ─────────────────────────────────────────────────────── */
  section('3. No token, no answer');
  env = { CONN_SECRETS_KEY: KEY_B64 };
  r = await call({ action: 'list' });
  check('no Authorization header → 401', r.status === 401, r.body);
  r = await call({ action: 'list', query: { code: 'A-FUNCTION-KEY' } });
  check('A FUNCTION KEY ALONE IS STILL 401 — the key was published once and guards nothing here', r.status === 401, r.body);
  r = await call({ action: 'list', headers: { authorization: 'Bearer forged' } });
  check('a token that does not verify → 401', r.status === 401, r.body);
  r = await call({ action: 'list', headers: { authorization: 'Token good:alice' } });
  check('a non-Bearer scheme → 401', r.status === 401, r.body);
  check('the strict verifier is used, not the log-only enforceAuth rollout helper',
    !/enforceAuth/.test(CODE), 'enforceAuth lets a request through without a token until REQUIRE_TOKEN_AUTH is on');
  r = await call({ action: 'list', method: 'OPTIONS' });
  check('CORS preflight is answered before any of that', r.status === 200);

  /* ── 4. Round trip ───────────────────────────────────────────────────── */
  section('4. Seal, store, unseal');
  container = fakeContainer();
  M.resetRateLimit();
  const alice = { authorization: 'Bearer good:alice' };
  const CS = 'mssql://svc:Sup3r-Secret-Pw@fin.example.internal:1433/FIN';
  r = await call({ action: 'put', method: 'POST', headers: alice, body: { connId: 'sconn_1', bundle: { connString: CS }, updatedAt: 1000 } });
  check('put succeeds', r.status === 200 && body(r).ok === true && body(r).updatedAt === 1000, r.body);

  const stored = container._docs.get('alice:sconn_1');
  check('THE STORED DOCUMENT HOLDS iv, tag AND ct — AND NO READABLE CREDENTIAL',
    !!stored && stored.iv && stored.tag && stored.ct
    && JSON.stringify(stored).indexOf('Sup3r-Secret-Pw') === -1
    && JSON.stringify(stored).indexOf('fin.example.internal') === -1, JSON.stringify(stored));
  check('it is keyed on the oid, partitioned on the oid, and remembers the email for a human',
    stored.id === 'alice:sconn_1' && stored.userId === 'alice' && stored.email === 'alice@example.test');
  check('and records the key version, so a rotation has something to go on',
    stored.keyVersion === '1');
  check('the IV is 12 bytes and the tag 16, as GCM wants',
    Buffer.from(stored.iv, 'base64').length === 12 && Buffer.from(stored.tag, 'base64').length === 16);

  r = await call({ action: 'list', headers: alice });
  check('list returns it, decrypted, to the owner',
    r.status === 200 && body(r).secrets.sconn_1 && body(r).secrets.sconn_1.bundle.connString === CS
    && body(r).secrets.sconn_1.updatedAt === 1000, r.body);
  check('with an empty undecryptable list', Array.isArray(body(r).undecryptable) && body(r).undecryptable.length === 0);

  r = await call({ action: 'put', method: 'POST', headers: alice, body: { connId: 'sconn_2', bundle: { fnKey: 'fk-' + 'x'.repeat(30) }, updatedAt: 5 } });
  const r2 = await call({ action: 'put', method: 'POST', headers: alice, body: { connId: 'sconn_3', bundle: { secret: 'sas=token' }, updatedAt: 5 } });
  check('an Azure Function key and a stream destination secret seal by the same path',
    r.status === 200 && r2.status === 200);
  r = await call({ action: 'put', method: 'POST', headers: alice, body: { connId: 'sconn_4', connString: 'Server=h;Database=d;', updatedAt: 5 } });
  check('the contract as first written — a bare connString — is accepted and wrapped', r.status === 200);
  r = await call({ action: 'list', headers: alice });
  check('all four come back to the owner', Object.keys(body(r).secrets).length === 4
    && body(r).secrets.sconn_4.bundle.connString === 'Server=h;Database=d;');

  /* ── 5. Two IVs, never the same ciphertext ───────────────────────────── */
  await call({ action: 'put', method: 'POST', headers: alice, body: { connId: 'sconn_1', bundle: { connString: CS }, updatedAt: 1001 } });
  const again = container._docs.get('alice:sconn_1');
  check('the same credential written twice produces different iv and ct — a fresh IV per write',
    again.iv !== stored.iv && again.ct !== stored.ct);

  /* ── 6. The binding ──────────────────────────────────────────────────── */
  section('5. The record is bound to its owner and its connection');
  const k = Buffer.from(KEY_B64, 'base64');
  check('the pure round trip agrees with itself',
    M.decryptBundle(k, 'alice', 'sconn_1', M.encryptBundle(k, 'alice', 'sconn_1', { connString: 'a' })).connString === 'a');
  check('A RECORD COPIED TO ANOTHER USER WILL NOT OPEN',
    M.decryptBundle(k, 'mallory', 'sconn_1', again) === null);
  check('nor one copied onto another connection',
    M.decryptBundle(k, 'alice', 'sconn_9', again) === null);
  check('nor with a different key — and none of those throws',
    M.decryptBundle(crypto.randomBytes(32), 'alice', 'sconn_1', again) === null);
  // A record sealed under a key this deployment no longer has: listed as
  // undecryptable, and the rest of the list still comes back.
  container._docs.set('alice:sconn_old', Object.assign({}, M.encryptBundle(crypto.randomBytes(32), 'alice', 'sconn_old', { connString: 'gone' }),
    { id: 'alice:sconn_old', userId: 'alice', connId: 'sconn_old', keyVersion: '0', updatedAt: 1 }));
  r = await call({ action: 'list', headers: alice });
  check('an undecryptable record is named and left out, and the others are unaffected',
    body(r).undecryptable.length === 1 && body(r).undecryptable[0] === 'sconn_old'
    && Object.keys(body(r).secrets).length === 4 && !('sconn_old' in body(r).secrets), r.body);

  /* ── 7. Isolation ────────────────────────────────────────────────────── */
  section('6. One user cannot see another');
  const bob = { authorization: 'Bearer good:bob' };
  r = await call({ action: 'list', headers: bob });
  check('a different user\'s list is empty', r.status === 200 && Object.keys(body(r).secrets).length === 0, r.body);
  r = await call({ action: 'delete', method: 'POST', headers: bob, body: { connId: 'sconn_1' } });
  check('and their delete of the same connId touches nothing of alice\'s',
    r.status === 200 && container._docs.has('alice:sconn_1'));
  r = await call({ action: 'prune', method: 'POST', headers: bob, body: { keepConnIds: [] } });
  check('nor does their prune', r.status === 200 && body(r).removed === 0 && container._docs.has('alice:sconn_1'));

  /* ── 8. Last write wins ──────────────────────────────────────────────── */
  section('7. Last write wins');
  r = await call({ action: 'put', method: 'POST', headers: alice, body: { connId: 'sconn_1', bundle: { connString: 'OLD' }, updatedAt: 500 } });
  check('AN OLDER WRITE IS SKIPPED, and says so', r.status === 200 && body(r).skipped === true && body(r).updatedAt === 1001, r.body);
  r = await call({ action: 'list', headers: alice });
  check('and the newer credential is still the one stored', body(r).secrets.sconn_1.bundle.connString === CS);
  r = await call({ action: 'put', method: 'POST', headers: alice, body: { connId: 'sconn_1', bundle: { connString: 'NEW' }, updatedAt: 2000 } });
  check('a newer write lands', r.status === 200 && !body(r).skipped);

  /* ── 9. Delete and prune ─────────────────────────────────────────────── */
  section('8. Delete and prune');
  r = await call({ action: 'delete', method: 'POST', headers: alice, body: { connId: 'sconn_2' } });
  check('delete removes the record', r.status === 200 && !container._docs.has('alice:sconn_2'));
  r = await call({ action: 'delete', method: 'POST', headers: alice, body: { connId: 'sconn_2' } });
  check('deleting it again is fine — idempotent', r.status === 200);
  r = await call({ action: 'prune', method: 'POST', headers: alice, body: { keepConnIds: ['sconn_1'] } });
  check('prune removes everything not in the keep list, and counts it',
    r.status === 200 && body(r).removed === 3 && container._docs.has('alice:sconn_1')
    && !container._docs.has('alice:sconn_3') && !container._docs.has('alice:sconn_old'), r.body);

  /* ── 10. Validation ──────────────────────────────────────────────────── */
  section('9. Validation');
  const bad = async (o, label) => {
    const rr = await call(Object.assign({ method: 'POST', headers: alice }, o));
    check(label, rr.status === 400, rr.status + ' ' + rr.body);
  };
  await bad({ action: 'put', body: { connId: 'job_1', bundle: { connString: 'x' }, updatedAt: 1 } }, 'a connId that is not sconn_… is refused');
  await bad({ action: 'put', body: { connId: 'sconn_' + 'a'.repeat(81), bundle: { connString: 'x' }, updatedAt: 1 } }, 'an over-long connId is refused');
  await bad({ action: 'put', body: { connId: 'sconn_1', bundle: { connString: '' }, updatedAt: 1 } }, 'an empty credential is refused');
  await bad({ action: 'put', body: { connId: 'sconn_1', bundle: { connString: 'x'.repeat(4001) }, updatedAt: 1 } }, 'a credential over 4000 characters is refused');
  await bad({ action: 'put', body: { connId: 'sconn_1', bundle: { password: 'x' }, updatedAt: 1 } }, 'a field the client store does not have is refused');
  await bad({ action: 'put', body: { connId: 'sconn_1', bundle: {}, updatedAt: 1 } }, 'an empty bundle is refused');
  await bad({ action: 'put', body: { connId: 'sconn_1', bundle: { connString: 'x' }, updatedAt: 'now' } }, 'a non-numeric updatedAt is refused');
  await bad({ action: 'put', body: { connId: 'sconn_1', bundle: { connString: 'x' }, updatedAt: -1 } }, 'a negative updatedAt is refused');
  await bad({ action: 'prune', body: { keepConnIds: 'sconn_1' } }, 'a keep list that is not an array is refused');
  await bad({ action: 'prune', body: { keepConnIds: ['sconn_1', '../x'] } }, 'a keep list with a bad id is refused');
  r = await call({ action: 'put', method: 'GET', headers: alice });
  check('put by GET is refused', r.status === 405);
  r = await call({ action: 'explode', method: 'POST', headers: alice, body: {} });
  check('an unknown action is 404', r.status === 404);

  /* ── 11. Rate limit ──────────────────────────────────────────────────── */
  section('10. Rate limit');
  M.resetRateLimit();
  let limited = 0, okCount = 0;
  for (let i = 0; i < M.WRITES_PER_MIN + 5; i++) {
    const rr = await call({ action: 'put', method: 'POST', headers: alice, body: { connId: 'sconn_rl', bundle: { connString: 'x' }, updatedAt: i } });
    if (rr.status === 429) limited++; else if (rr.status === 200) okCount++;
  }
  check(M.WRITES_PER_MIN + ' writes a minute per user, then 429', okCount === M.WRITES_PER_MIN && limited === 5, okCount + ' ok, ' + limited + ' limited');
  check('reads are not rate-limited', (await call({ action: 'list', headers: alice })).status === 200);
  M.resetRateLimit();

  /* ── 12. Failure ─────────────────────────────────────────────────────── */
  section('11. When Cosmos fails');
  M._deps.container = async () => { throw new Error('cosmos is down'); };
  r = await call({ action: 'list', headers: alice });
  check('a Cosmos failure is a 500 with the message and stack in the body — there is no App Insights to look in',
    r.status === 500 && /cosmos is down/.test(body(r).error) && typeof body(r).stack === 'string', r.body);
  M._deps.container = async () => container;

  /* ── 13. Nothing leaks ───────────────────────────────────────────────── */
  section('12. Nothing that should stay secret can leave');
  const loud = SRC.split('\n').filter((l) => /console\.|logWarn|logErr|ctx\.log|ctx\.warn|ctx\.error/.test(l));
  check('no log line in the module mentions a bundle, a credential field, or a ciphertext',
    loud.every((l) => !/bundle|connString|fnKey|\.secret\b|\bct\b|\biv\b|\btag\b|plain/.test(l)), loud.join(' | '));
  check('no error body is built from a bundle or a document',
    !/fail\([^)]*\b(bundle|doc|sealed|plain)\b/.test(SRC));
  check('everything this test logged is free of the credential',
    logged.every((l) => l.indexOf('Sup3r-Secret-Pw') === -1 && l.indexOf(CS) === -1));
  check('the client store\'s key is still out of SYNC_KEYS',
    !/cygenix_saved_conn_secrets/.test(read('public', 'cygenix-cosmos-sync.js').split('const FIELD_MAP')[0].split('const SYNC_KEYS')[1]));
  check('and out of the JSON backup',
    !/cygenix_saved_conn_secrets/.test(read('public', 'dashboard-app.js').split('const BACKUP_KEYS')[1].split('];')[0]));

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
