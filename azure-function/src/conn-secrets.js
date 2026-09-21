// ═════════════════════════════════════════════════════════════════════════════
// conn-secrets.js — encrypted cloud sync for saved-connection secrets
//
// WHY THIS EXISTS
//
// A saved connection is two halves. The name, side, mode and endpoint sync
// to Cosmos with the rest of the user's data (cygenix_saved_connections is in
// SYNC_KEYS). The credential — the connection string with the password in
// it, the Azure Function key, a stream destination's signing secret — was
// kept in a separate localStorage store on purpose, so it never reached the
// cloud in plain text. The price was paid on the next machine: the
// connection appears by name and has no credential behind it, and the person
// has to type it again on every device they use.
//
// This module is the cloud half of that store, and it is encrypted so the
// price of keeping the credential in Cosmos is not "Cosmos holds the
// password". Every credential is sealed by THIS Function App with a key that
// only it holds, before it is written, and unsealed only for the signed-in
// owner, only when their request carries a valid Entra token.
//
// THE RULES THIS FILE KEEPS
//
//   · The caller's identity is the VERIFIED token and nothing else. These
//     routes are authLevel 'anonymous' and check the Bearer JWT in code:
//     a request with no valid token gets 401 even if it carries a function
//     key. The host key was published in the client once; it must not be
//     what stands between a stranger and somebody's passwords.
//
//   · Records are keyed on `oid`, the stable object id from the token, not
//     on the email, which can change. The email is stored beside it so a
//     record is readable by a person.
//
//   · AES-256-GCM, a fresh 12-byte IV per write, and the AAD is
//     `${oid}:${connId}`. A ciphertext copied onto another user's record,
//     or onto another connection, fails to decrypt — the binding is
//     cryptographic, not a lookup that could be got wrong.
//
//   · The key is CONN_SECRETS_KEY (32 bytes, base64). If it is missing or
//     the wrong length every route here answers 503 and NOTHING ELSE
//     happens: the key is read inside the handler, never at module load,
//     because a throw at load time takes down every /api/* route in the app.
//
//   · No log line and no error body ever carries a credential, plaintext or
//     ciphertext. There is no Application Insights on Flex Consumption, so
//     failures come back in the 500 body as message + stack — and a stack
//     is fine, a connection string is not. Nothing in this file interpolates
//     a bundle, a ct, an iv or a tag into a string that leaves the process.
//
//   · Last write wins by the client's `updatedAt`. Two devices editing the
//     same connection converge on whichever saved last, and an old device
//     coming back online cannot roll a newer credential back.
//
// v4 programming model: app.http() here, side-effect required from index.js.
// No function.json, no functionTimeout in host.json.
// ─────────────────────────────────────────────────────────────────────────────
'use strict';

const crypto = require('crypto');
const { app } = require('@azure/functions');
const { verifyJwt, logWarn, logErr } = require('./entra-auth');

// ─── Constants ───────────────────────────────────────────────────────────────
const CONTAINER      = 'conn_secrets';
const PARTITION_KEY  = '/userId';
const CONN_ID_RE     = /^sconn_[A-Za-z0-9_]{1,80}$/;
const SECRET_FIELDS  = ['connString', 'fnKey', 'secret'];   // the three the client store knows
const MAX_FIELD_LEN  = 4000;
const WRITES_PER_MIN = 60;

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type': 'application/json',
};
const reply = (status, body) => ({ status, headers: CORS, body: JSON.stringify(body) });
const ok    = (body) => reply(200, body);
const fail  = (status, error, extra) => reply(status, Object.assign({ error }, extra || {}));

// ─── The key ─────────────────────────────────────────────────────────────────
// Read per request. Cheap, and it means a key added or fixed in the Portal is
// picked up on the next call rather than the next restart — and a key that
// is WRONG cannot crash the host, only answer 503.
function loadKey(env) {
  const e = env || process.env;
  const raw = String(e.CONN_SECRETS_KEY || '').trim();
  if (!raw) return { ok: false, why: 'CONN_SECRETS_KEY is not set' };
  let buf;
  try { buf = Buffer.from(raw, 'base64'); } catch { return { ok: false, why: 'CONN_SECRETS_KEY is not base64' }; }
  if (buf.length !== 32) return { ok: false, why: 'CONN_SECRETS_KEY must decode to 32 bytes (got ' + buf.length + ')' };
  const version = String(e.CONN_SECRETS_KEY_VERSION || '1').trim() || '1';
  return { ok: true, key: buf, version };
}

// ─── Sealing ─────────────────────────────────────────────────────────────────
// The bundle is the same object the browser store holds — { connString } or
// { fnKey } or { secret } — serialised, then sealed. One shape for all three
// kinds of credential, so a stream destination's SAS token syncs by the same
// path as a SQL password.
function aadFor(oid, connId) { return Buffer.from(oid + ':' + connId, 'utf8'); }

function encryptBundle(key, oid, connId, bundle) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  cipher.setAAD(aadFor(oid, connId));
  const plain = Buffer.from(JSON.stringify(bundle), 'utf8');
  const ct = Buffer.concat([cipher.update(plain), cipher.final()]);
  const tag = cipher.getAuthTag();
  return { iv: iv.toString('base64'), ct: ct.toString('base64'), tag: tag.toString('base64') };
}

// Returns the bundle, or null when the record cannot be opened with this key
// for this owner and connection — a wrong key, a rotated key, a copied
// record. Never throws: a caller listing twenty records must not lose the
// other nineteen to one bad one.
function decryptBundle(key, oid, connId, doc) {
  try {
    const iv = Buffer.from(String(doc.iv || ''), 'base64');
    const ct = Buffer.from(String(doc.ct || ''), 'base64');
    const tag = Buffer.from(String(doc.tag || ''), 'base64');
    if (iv.length !== 12 || tag.length !== 16) return null;
    const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
    decipher.setAAD(aadFor(oid, connId));
    decipher.setAuthTag(tag);
    const plain = Buffer.concat([decipher.update(ct), decipher.final()]).toString('utf8');
    const bundle = JSON.parse(plain);
    return (bundle && typeof bundle === 'object' && !Array.isArray(bundle)) ? bundle : null;
  } catch {
    return null;
  }
}

// ─── Validation ──────────────────────────────────────────────────────────────
function validateConnId(v) {
  return typeof v === 'string' && CONN_ID_RE.test(v);
}

// Accepts the bundle object the client store holds, or — for the contract as
// first written — a bare `connString` string, which is wrapped. Returns the
// clean bundle or a reason.
function validateBundle(body) {
  let bundle = null;
  if (body && typeof body.bundle === 'object' && body.bundle && !Array.isArray(body.bundle)) {
    bundle = body.bundle;
  } else if (body && typeof body.connString === 'string') {
    bundle = { connString: body.connString };
  } else {
    return { ok: false, why: 'bundle must be an object holding connString, fnKey or secret' };
  }
  const clean = {};
  for (const k of Object.keys(bundle)) {
    if (SECRET_FIELDS.indexOf(k) === -1) return { ok: false, why: 'unknown bundle field: ' + k };
    const v = bundle[k];
    if (typeof v !== 'string') return { ok: false, why: k + ' must be a string' };
    if (v.length < 1 || v.length > MAX_FIELD_LEN) return { ok: false, why: k + ' must be 1-' + MAX_FIELD_LEN + ' characters' };
    clean[k] = v;
  }
  if (!Object.keys(clean).length) return { ok: false, why: 'bundle is empty' };
  return { ok: true, bundle: clean };
}

function validateUpdatedAt(v) {
  const n = Number(v);
  return (Number.isFinite(n) && n >= 0) ? Math.floor(n) : null;
}

// ─── Rate limit ──────────────────────────────────────────────────────────────
// In memory, per instance. Flex Consumption may run several instances, so
// this is a ceiling per instance rather than a global one — enough to stop a
// loop or a script from hammering the store, which is what it is for.
const _writes = new Map();   // oid → [timestamps within the last minute]
function rateLimited(oid, now) {
  const t = now || Date.now();
  const list = (_writes.get(oid) || []).filter((ts) => t - ts < 60000);
  if (list.length >= WRITES_PER_MIN) { _writes.set(oid, list); return true; }
  list.push(t);
  _writes.set(oid, list);
  return false;
}
function resetRateLimit() { _writes.clear(); }

// ─── Cosmos ──────────────────────────────────────────────────────────────────
// Same lazy singleton as index.js and schedules.js. The container is created
// on first use because nothing else in this app provisions containers in
// code and the owner should not have to make one by hand before the feature
// works. createIfNotExists is idempotent; the promise is memoised so a burst
// of first requests does not race it.
let _cosmos = null;
let _ensured = null;
function realContainer() {
  if (!_cosmos) {
    const { CosmosClient } = require('@azure/cosmos');
    _cosmos = new CosmosClient({ endpoint: process.env.COSMOS_ENDPOINT, key: process.env.COSMOS_KEY });
  }
  const db = _cosmos.database(process.env.COSMOS_DATABASE || 'cygenix');
  if (!_ensured) {
    _ensured = db.containers.createIfNotExists({ id: CONTAINER, partitionKey: { paths: [PARTITION_KEY] } })
      .catch((e) => { _ensured = null; throw e; });
  }
  return _ensured.then(() => db.container(CONTAINER));
}

// The seams a test replaces. Production never touches these.
const deps = {
  container: realContainer,
  verify: verifyJwt,
  env: () => process.env,
  now: () => Date.now(),
};

// ─── Identity ────────────────────────────────────────────────────────────────
// Strict, deliberately not enforceAuth(): that helper is log-only until
// REQUIRE_TOKEN_AUTH is switched on, which is the right rollout for the
// routes that used to trust x-user-id and the wrong one for a route that
// hands out passwords. No token, no answer.
async function identify(req) {
  const raw = req.headers.get('authorization') || '';
  const m = /^Bearer\s+(.+)$/i.exec(raw.trim());
  if (!m) return { ok: false, why: 'Authorization Bearer token required' };
  let claims;
  try { claims = await deps.verify(m[1].trim()); }
  catch (e) { return { ok: false, why: 'Invalid token: ' + (e && e.message || String(e)) }; }
  const oid = String(claims.oid || claims.sub || '').trim();
  if (!oid) return { ok: false, why: 'token carries no oid or sub claim' };
  const email = String(claims.email || claims.preferred_username || claims.upn || '').trim().toLowerCase();
  return { ok: true, oid, email };
}

// ─── The handler ─────────────────────────────────────────────────────────────
async function handler(req, ctx) {
  if (req.method === 'OPTIONS') return { status: 200, headers: CORS, body: '' };

  const action = String((req.params && req.params.action) || '').toLowerCase();
  if (['list', 'put', 'delete', 'prune'].indexOf(action) === -1) return fail(404, 'unknown action: ' + action);

  // 503 before 401: an operator probing a misconfigured deployment gets the
  // answer that tells them what to fix, and no token is verified or logged
  // for a route that cannot serve anyway.
  const k = loadKey(deps.env());
  if (!k.ok) {
    logWarn(ctx, '[conn-secrets] ' + k.why);
    return fail(503, 'secrets store not configured', { code: 'no-secrets-key' });
  }

  const who = await identify(req);
  if (!who.ok) return fail(401, who.why);
  const { oid, email } = who;

  let body = {};
  if (req.method === 'POST') {
    try { body = await req.json(); } catch { body = {}; }
    if (!body || typeof body !== 'object') body = {};
  }

  try {
    const container = await deps.container();
    const now = deps.now();

    switch (action) {

      case 'list': {
        if (req.method !== 'GET') return fail(405, 'list is GET');
        const { resources } = await container.items.query({
          query: 'SELECT c.id, c.connId, c.iv, c.ct, c.tag, c.keyVersion, c.updatedAt FROM c WHERE c.userId = @uid',
          parameters: [{ name: '@uid', value: oid }],
        }, { partitionKey: oid }).fetchAll();
        const secrets = {};
        const undecryptable = [];
        for (const doc of resources || []) {
          if (!validateConnId(doc.connId)) continue;
          const bundle = decryptBundle(k.key, oid, doc.connId, doc);
          if (!bundle) { undecryptable.push(doc.connId); continue; }
          secrets[doc.connId] = { bundle, updatedAt: Number(doc.updatedAt) || 0 };
        }
        ctx && typeof ctx.log === 'function' && ctx.log('[conn-secrets] list for ' + oid + ': ' + Object.keys(secrets).length + ' ok, ' + undecryptable.length + ' undecryptable');
        return ok({ secrets, undecryptable, keyVersion: k.version });
      }

      case 'put': {
        if (req.method !== 'POST') return fail(405, 'put is POST');
        if (!validateConnId(body.connId)) return fail(400, 'connId must match ' + String(CONN_ID_RE));
        const v = validateBundle(body);
        if (!v.ok) return fail(400, v.why);
        const updatedAt = validateUpdatedAt(body.updatedAt);
        if (updatedAt === null) return fail(400, 'updatedAt must be a non-negative number');
        if (rateLimited(oid, now)) return fail(429, 'too many writes; try again in a minute');

        const id = oid + ':' + body.connId;
        // Last write wins. A stale device replaying an older credential is
        // told it was skipped rather than allowed to roll the newer one back.
        try {
          const { resource: existing } = await container.item(id, oid).read();
          if (existing && Number(existing.updatedAt) > updatedAt) {
            return ok({ ok: true, skipped: true, updatedAt: Number(existing.updatedAt) });
          }
        } catch (e) {
          if (!e || e.code !== 404) throw e;
        }
        const sealed = encryptBundle(k.key, oid, body.connId, v.bundle);
        await container.items.upsert({
          id, userId: oid, email, connId: body.connId,
          iv: sealed.iv, tag: sealed.tag, ct: sealed.ct,
          keyVersion: k.version, updatedAt,
        });
        return ok({ ok: true, updatedAt });
      }

      case 'delete': {
        if (req.method !== 'POST') return fail(405, 'delete is POST');
        if (!validateConnId(body.connId)) return fail(400, 'connId must match ' + String(CONN_ID_RE));
        if (rateLimited(oid, now)) return fail(429, 'too many writes; try again in a minute');
        try { await container.item(oid + ':' + body.connId, oid).delete(); }
        catch (e) { if (!e || e.code !== 404) throw e; }
        return ok({ ok: true });
      }

      case 'prune': {
        if (req.method !== 'POST') return fail(405, 'prune is POST');
        const keep = Array.isArray(body.keepConnIds) ? body.keepConnIds : null;
        if (!keep) return fail(400, 'keepConnIds must be an array');
        if (!keep.every(validateConnId)) return fail(400, 'keepConnIds contains an invalid connId');
        if (rateLimited(oid, now)) return fail(429, 'too many writes; try again in a minute');
        const keepSet = new Set(keep);
        const { resources } = await container.items.query({
          query: 'SELECT c.id, c.connId FROM c WHERE c.userId = @uid',
          parameters: [{ name: '@uid', value: oid }],
        }, { partitionKey: oid }).fetchAll();
        let removed = 0;
        for (const doc of resources || []) {
          if (keepSet.has(doc.connId)) continue;
          try { await container.item(doc.id, oid).delete(); removed++; }
          catch (e) { if (!e || e.code !== 404) throw e; }
        }
        return ok({ ok: true, removed });
      }
    }
    return fail(404, 'unknown action');
  } catch (e) {
    // In-band diagnostics, because there is no Application Insights here.
    // The message and stack of a Cosmos or crypto failure never contain a
    // credential — nothing in this file puts one into an Error.
    logErr(ctx, '[conn-secrets] ' + action + ' failed: ' + (e && e.message));
    return fail(500, (e && e.message) || String(e), { stack: e && e.stack ? String(e.stack) : undefined });
  }
}

app.http('conn-secrets', {
  methods: ['GET', 'POST', 'OPTIONS'],
  authLevel: 'anonymous',
  route: 'secrets/{action}',
  handler,
});

// For the tests: the pure pieces, the handler, and the seams.
module.exports = {
  handler,
  loadKey, encryptBundle, decryptBundle, validateBundle, validateConnId, validateUpdatedAt,
  rateLimited, resetRateLimit,
  CONN_ID_RE, SECRET_FIELDS, MAX_FIELD_LEN, WRITES_PER_MIN, CONTAINER,
  _deps: deps,
};
