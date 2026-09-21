// cygenix-saved-conn-secrets.js
//
// The secret half of every saved connection: local store, and — since
// Sep-2026 — an encrypted copy in the cloud so it follows the user to their
// next machine.
//
// Why this exists:
//   The main saved-connections blob (`cygenix_saved_connections`) is in
//   the cosmos sync layer's SYNC_KEYS list, which means anything stored
//   under that key gets uploaded to Cosmos DB as it is. Connection strings
//   of the form `mssql://user:password@host:port/db` and Azure Function
//   keys therefore ended up in cloud storage in plain text — not what we
//   want.
//
//   This helper owns the secret half of each saved connection (the
//   password-bearing connection string, the function key, or a stream
//   destination's credential) and stores it under a SEPARATE localStorage
//   key — `cygenix_saved_conn_secrets` — that is deliberately NOT in
//   SYNC_KEYS and not in any backup/export list. The synced blob holds
//   names + sanitised metadata only.
//
// Storage shape:
//   localStorage['cygenix_saved_conn_secrets'] = JSON object keyed by
//   the saved-connection's `id` (e.g. 'sconn_1777375369157_08hoy'):
//
//     {
//       "sconn_1777...": { connString: "mssql://user:pwd@host/db", updatedAt: 1758400000000 },
//       "sconn_1888...": { fnKey: "abc123...", updatedAt: 1758400000000 },
//       ...
//     }
//
//   Direct-mode entries get { connString }. Azure-mode entries get
//   { fnKey } (the fnUrl itself is NOT a secret on its own — it's
//   just an HTTPS endpoint URL — so it stays in the synced blob).
//   Stream destinations (side 'dest', cygenix-stream-destinations.js)
//   use a third field, `secret`. Three field names, no more, so this file
//   cannot fall behind a new kind of credential.
//
//   `updatedAt` is new. An entry written before it existed has none and is
//   read as 0 — older than anything — so the first cloud copy wins over it
//   and a device that never synced cannot roll a newer credential back.
//   get() strips it, because every existing caller destructures the bundle
//   and one of them stores the object it gets back.
//
// The cloud half:
//   The Function App has /api/secrets/{list,put,delete,prune}
//   (azure-function/src/conn-secrets.js). It seals each bundle with
//   AES-256-GCM under a key only it holds, keyed on the caller's Entra oid,
//   and unseals only for that caller with a valid token. This file reaches
//   it the only way the browser reaches the Function App — through
//   /.netlify/functions/data-proxy, via CygenixDataApi, which attaches the
//   Entra token and refuses to call at all when there is none. The browser
//   holds no key and asserts no identity.
//
//   Contract, in order:
//     sync()      after sign-in, once per page load: pull the cloud list,
//                 merge per id (newer updatedAt wins), then — once per
//                 device per user — upload what the cloud lacks.
//     set()       local first, then a debounced, coalesced put.
//     set(null)   local delete, then a delete.
//     pruneTo()   local prune; a cloud prune only when something went.
//
//   Request-storm rules, all kept here: one flush in flight at a time; at
//   least three seconds between flushes; the one-time upload flag is set
//   only after every upload in the batch succeeds and is never reset by
//   the code that runs after an upload. A failed write is remembered in a
//   small dirty list and retried on the next page load — not on a timer,
//   because a timer against a dead endpoint is a loop.
//
// Cross-browser behaviour, now:
//   On a new browser the synced blob arrives from Cosmos, this file pulls
//   the sealed secrets, and the connection works without re-entry. When a
//   secret is genuinely absent (never synced, or deleted from the store) or
//   cannot be opened (the server's key changed), status(id) says which, and
//   the Connections page shows "Password needed on this device" or
//   "Couldn't decrypt — re-enter" with a way to type it again.

(function () {
  'use strict';

  const KEY = 'cygenix_saved_conn_secrets';
  const SECRET_FIELDS = ['connString', 'fnKey', 'secret'];
  // The live source/target pair's credentials live here too, under two
  // reserved ids (connections.js mirrors them in). They are not saved
  // connections, so a prune to the saved list must never take them.
  const LIVE_IDS = ['sconn_live_src', 'sconn_live_tgt'];
  const MIN_FLUSH_INTERVAL_MS = 3000;   // never more than one flush per 3s
  const DEBOUNCE_MS = 800;              // coalesce a burst of edits into one flush
  const MIN_SYNC_INTERVAL_MS = 3000;

  // ── Local store ──────────────────────────────────────────────────────────

  function readAll() {
    try {
      const raw = localStorage.getItem(KEY);
      if (!raw) return {};
      const obj = JSON.parse(raw);
      return (obj && typeof obj === 'object' && !Array.isArray(obj)) ? obj : {};
    } catch { return {}; }
  }

  function writeAll(obj) {
    try {
      localStorage.setItem(KEY, JSON.stringify(obj || {}));
    } catch (e) {
      console.error('[saved-conn-secrets] localStorage write failed:', e && e.message);
    }
  }

  // Only the three secret fields, only non-empty strings. Anything else a
  // caller hands us is dropped rather than stored or uploaded.
  function cleanBundle(secrets) {
    const out = {};
    if (!secrets || typeof secrets !== 'object') return out;
    for (const k of SECRET_FIELDS) {
      const v = secrets[k];
      if (typeof v === 'string' && v) out[k] = v;
    }
    return out;
  }
  function sameBundle(a, b) {
    for (const k of SECRET_FIELDS) if ((a && a[k]) !== (b && b[k])) return false;
    return true;
  }
  function stampOf(entry) {
    const n = Number(entry && entry.updatedAt);
    return Number.isFinite(n) && n > 0 ? n : 0;
  }
  function hasAny(entry) {
    return !!(entry && SECRET_FIELDS.some((k) => typeof entry[k] === 'string' && entry[k]));
  }

  // Get the secret bundle for a given saved-connection id.
  // Returns {} (not null) so callers can `const { connString } = get(id)`
  // safely without an existence check. The stamp is not part of the bundle.
  function getSecret(id) {
    if (!id) return {};
    const all = readAll();
    return all[id] ? cleanBundle(all[id]) : {};
  }

  // Replace the secret bundle for a given id. Pass null/undefined to delete.
  //
  // sconnSetAll calls this for EVERY entry on every save of the list, so an
  // unchanged bundle must be a no-op: no new stamp, nothing queued.
  // Otherwise "Save as…" on one connection would re-upload all of them.
  function setSecret(id, secrets) {
    if (!id) return;
    const all = readAll();
    const next = cleanBundle(secrets);
    if (secrets == null || !Object.keys(next).length) {
      if (!(id in all)) return;
      delete all[id];
      writeAll(all);
      queue(id, 'delete');
      return;
    }
    const prev = all[id];
    if (prev && sameBundle(prev, next)) return;
    next.updatedAt = Date.now();
    all[id] = next;
    writeAll(all);
    queue(id, 'put');
  }

  // Bulk update — used when sconnSetAll rewrites the whole list.
  // Removes any entries whose ids are no longer in `keepIds`, so
  // deleting a saved connection also removes its orphaned secret — here
  // and, when something was actually removed, in the cloud.
  function pruneTo(keepIds) {
    const keep = new Set((keepIds || []).concat(LIVE_IDS));
    const all = readAll();
    let changed = false;
    for (const id of Object.keys(all)) {
      if (!keep.has(id)) {
        delete all[id];
        changed = true;
      }
    }
    if (changed) {
      writeAll(all);
      queuePrune([...keep]);
    }
  }

  // Merge stored secrets back into a sanitised entries array. Mutates
  // the passed array in place — sconnGetAll calls this so that
  // downstream UI code (which expects connString / fnKey to be
  // populated) keeps working without any other changes. Synchronous and
  // local: the cloud pull is sync(), which runs once per page load and
  // announces itself, so a page re-renders and this sees the result.
  function rehydrate(entries) {
    if (!Array.isArray(entries)) return entries;
    const all = readAll();
    for (const e of entries) {
      if (!e || !e.id) continue;
      const sec = all[e.id];
      if (!sec) continue;
      if (sec.connString && !e.connString) e.connString = sec.connString;
      if (sec.fnKey      && !e.fnKey)      e.fnKey      = sec.fnKey;
      if (sec.secret     && !e.secret)     e.secret     = sec.secret;
    }
    return entries;
  }

  // Strip secrets out of an entry and return both halves separately.
  // Used by sconnSetAll: the sanitised entry goes to the synced blob,
  // the secret bundle goes to this store.
  function split(entry) {
    if (!entry || typeof entry !== 'object') return { sanitised: entry, secrets: null };
    const secrets = {};
    const sanitised = Object.assign({}, entry);
    if ('connString' in sanitised) {
      if (sanitised.connString) secrets.connString = sanitised.connString;
      delete sanitised.connString;
    }
    if ('fnKey' in sanitised) {
      if (sanitised.fnKey) secrets.fnKey = sanitised.fnKey;
      delete sanitised.fnKey;
    }
    if ('secret' in sanitised) {
      if (sanitised.secret) secrets.secret = sanitised.secret;
      delete sanitised.secret;
    }
    return {
      sanitised,
      secrets: Object.keys(secrets).length ? secrets : null,
    };
  }

  // Has-secret check — used by the UI to show a "secret present" tick or a
  // "needs re-entry on this browser" hint.
  function hasSecret(id) {
    if (!id) return false;
    return hasAny(readAll()[id]);
  }

  // 'ok'            the credential is on this device
  // 'undecryptable' the cloud holds one this server could not open — the
  //                 sealing key changed; only re-entering it helps
  // 'missing'       nothing here and nothing usable in the cloud
  const _undecryptable = new Set();
  function status(id) {
    if (!id) return 'missing';
    if (hasAny(readAll()[id])) return 'ok';
    if (_undecryptable.has(id)) return 'undecryptable';
    return 'missing';
  }

  // ── Identity, for the per-user upload flag ───────────────────────────────
  // The oid is read from the ID token the app already holds; the flag is
  // per user per device so two people sharing a browser do not share a
  // "done" marker. It is only ever SET here. Nothing in this file removes it.
  function currentOid() {
    try {
      const tok = (typeof window.getCygenixIdToken === 'function') ? window.getCygenixIdToken() : '';
      if (!tok) return '';
      const b64 = tok.split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
      const claims = JSON.parse(atob(b64 + '='.repeat((4 - (b64.length % 4)) % 4)));
      return String(claims.oid || claims.sub || '').trim();
    } catch { return ''; }
  }
  function uploadFlagKey(oid) { return `cygenix_conn_secrets_uploaded_v1::${oid}`; }
  function uploadedBefore(oid) {
    try { return localStorage.getItem(uploadFlagKey(oid)) === '1'; } catch { return false; }
  }
  function markUploaded(oid) {
    try { localStorage.setItem(uploadFlagKey(oid), '1'); } catch { /* the next load tries again */ }
  }

  // Ids whose cloud write failed. Retried by sync() on the next page load,
  // not by a timer: a timer against a dead endpoint is a request loop.
  const DIRTY_KEY = 'cygenix_conn_secrets_dirty_v1';
  function readDirty() {
    try { const a = JSON.parse(localStorage.getItem(DIRTY_KEY) || '[]'); return Array.isArray(a) ? a : []; }
    catch { return []; }
  }
  function writeDirty(ids) {
    try {
      if (ids.length) localStorage.setItem(DIRTY_KEY, JSON.stringify(ids));
      else localStorage.removeItem(DIRTY_KEY);
    } catch { /* best effort */ }
  }
  function markDirty(id) { const d = readDirty(); if (d.indexOf(id) === -1) { d.push(id); writeDirty(d); } }
  function clearDirty(id) { writeDirty(readDirty().filter((x) => x !== id)); }

  // ── Transport ────────────────────────────────────────────────────────────
  // CygenixDataApi is the browser's only route to the Function App. It
  // attaches the Entra token and answers { ok:false, code:'no-token' }
  // without making a request when there is none — which is exactly the
  // signed-out behaviour this file wants.
  function api() { return (typeof window !== 'undefined') && window.CygenixDataApi; }
  function signedIn() {
    const a = api();
    return !!(a && typeof a.isSignedIn === 'function' && a.isSignedIn());
  }
  async function callCloud(action, body) {
    const a = api();
    if (!a || typeof a.callResult !== 'function') {
      return { ok: false, error: { code: 'no-api', message: 'CygenixDataApi is not loaded on this page' } };
    }
    return a.callResult(action, body ? { method: 'POST', body } : { method: 'GET' });
  }

  // ── The write queue ──────────────────────────────────────────────────────
  // Coalesced per id: ten edits to one connection in a second are one put.
  // One flush at a time, three seconds apart at least. A failed put is
  // marked dirty for the next page load and NOT rescheduled.
  const _pending = new Map();        // id → 'put' | 'delete'
  let _prunePending = null;          // keep list, or null
  let _debounce = null;
  let _flushing = false;
  let _lastFlushAt = 0;
  let _batchIds = null;              // the one-time upload batch, or null
  let _batchFailed = false;

  function queue(id, op) {
    _pending.set(id, op);
    schedule();
  }
  function queuePrune(keepIds) {
    _prunePending = keepIds;
    schedule();
  }
  function schedule(delayMs) {
    if (!signedIn()) return;                       // nothing leaves this device while signed out
    if (_debounce) clearTimeout(_debounce);
    _debounce = setTimeout(() => { _debounce = null; flush(); }, delayMs == null ? DEBOUNCE_MS : delayMs);
  }

  async function flush() {
    if (_flushing) return false;                   // in-flight guard
    if (!signedIn()) return false;                 // signed out: nothing is even attempted; the queue keeps for later
    const wait = MIN_FLUSH_INTERVAL_MS - (Date.now() - _lastFlushAt);
    if (wait > 0) { schedule(wait); return false; } // minimum interval
    if (!_pending.size && !_prunePending) return false;
    _flushing = true;
    _lastFlushAt = Date.now();
    try {
      const ops = [..._pending.entries()];
      _pending.clear();
      const prune = _prunePending;
      _prunePending = null;
      const all = readAll();
      for (const [id, op] of ops) {
        const entry = all[id];
        // Deleted since it was queued: send the delete instead of a stale put.
        const effective = (op === 'put' && !hasAny(entry)) ? 'delete' : op;
        let r;
        if (effective === 'put') {
          r = await callCloud('secrets-put', { connId: id, bundle: cleanBundle(entry), updatedAt: stampOf(entry) });
        } else {
          r = await callCloud('secrets-delete', { connId: id });
        }
        if (r && r.ok) {
          clearDirty(id);
        } else {
          if (effective === 'put') markDirty(id);
          if (_batchIds && _batchIds.has(id)) _batchFailed = true;
          if (r && r.error && r.error.code !== 'no-token' && r.error.code !== 'auth') {
            console.warn('[saved-conn-secrets] cloud ' + effective + ' failed:', r.error.code || r.error.message);
          }
        }
      }
      if (prune) {
        const r = await callCloud('secrets-prune', { keepConnIds: prune });
        if (!(r && r.ok)) _prunePending = _prunePending || prune;   // retry next flush, not by timer
      }
      // The one-time upload flag: set only when every upload in the batch
      // landed. The batch is closed here regardless; the FLAG is what
      // decides whether the next load tries again, and nothing here ever
      // clears it.
      if (_batchIds && !_batchIds.size) { /* nothing was queued */ }
      if (_batchIds) {
        const oid = currentOid();
        if (!_batchFailed && oid && !_pending.size) markUploaded(oid);
        _batchIds = null;
        _batchFailed = false;
      }
    } finally {
      _flushing = false;
    }
    return true;
  }

  // ── Pull, merge, and the one-time upload ─────────────────────────────────
  let _syncPromise = null;
  let _lastSyncAt = 0;
  let _syncedOnce = false;

  function sync(opts) {
    const o = opts || {};
    if (_syncPromise) return _syncPromise;                       // in-flight guard
    if (!o.force && Date.now() - _lastSyncAt < MIN_SYNC_INTERVAL_MS) {
      return Promise.resolve({ ok: false, code: 'throttled' });
    }
    _lastSyncAt = Date.now();
    // Cleared the moment the work is done, not a tick later: callers that
    // arrive during the flight share the promise; a caller that arrives
    // after it must meet the interval check, not a stale resolved promise.
    _syncPromise = (async () => {
      try { return await _sync(); }
      finally { _syncPromise = null; }
    })();
    return _syncPromise;
  }

  async function _sync() {
    if (!signedIn()) return { ok: false, code: 'no-token' };
    const r = await callCloud('secrets-list');
    if (!r.ok) {
      if (r.error && r.error.code !== 'no-token' && r.error.code !== 'auth') {
        console.warn('[saved-conn-secrets] cloud list failed:', r.error.code || r.error.message);
      }
      return { ok: false, code: (r.error && r.error.code) || 'unknown' };
    }
    const cloud = (r.data && r.data.secrets && typeof r.data.secrets === 'object') ? r.data.secrets : {};
    const undecryptable = Array.isArray(r.data && r.data.undecryptable) ? r.data.undecryptable : [];

    const all = readAll();
    let pulled = 0, changed = false;
    const localNewer = [];
    for (const id of Object.keys(cloud)) {
      const c = cloud[id] || {};
      const bundle = cleanBundle(c.bundle);
      if (!Object.keys(bundle).length) continue;
      const cStamp = Number(c.updatedAt) || 0;
      const local = all[id];
      if (!hasAny(local) || cStamp > stampOf(local)) {
        bundle.updatedAt = cStamp;
        all[id] = bundle;
        pulled++; changed = true;
      } else if (stampOf(local) > cStamp) {
        localNewer.push(id);
      }
    }
    if (changed) writeAll(all);

    _undecryptable.clear();
    for (const id of undecryptable) if (!hasAny(all[id])) _undecryptable.add(id);

    // What the cloud should now be given. Three sources, one queue:
    //   · the one-time upload — everything here the cloud lacks, once per
    //     device per user, and only until the flag says it has been done;
    //   · anything local that is newer than the cloud's copy;
    //   · anything whose last write failed (the dirty list).
    const oid = currentOid();
    const toUpload = new Set(localNewer.concat(readDirty().filter((id) => hasAny(all[id]))));
    let firstUpload = false;
    if (oid && !uploadedBefore(oid)) {
      firstUpload = true;
      for (const id of Object.keys(all)) if (hasAny(all[id]) && !cloud[id]) toUpload.add(id);
    }
    if (firstUpload) { _batchIds = new Set(toUpload); _batchFailed = false; }
    // A first upload with nothing to send is done as soon as it is judged.
    if (firstUpload && !toUpload.size) { markUploaded(oid); _batchIds = null; }
    for (const id of toUpload) _pending.set(id, 'put');
    if (_pending.size) schedule(0);

    _syncedOnce = true;
    try {
      window.dispatchEvent(new CustomEvent('cygenix:conn-secrets-synced', {
        detail: { pulled, undecryptable: [..._undecryptable], queued: toUpload.size },
      }));
    } catch { /* no event API in this environment */ }
    return { ok: true, pulled, undecryptable: [..._undecryptable], queued: toUpload.size };
  }

  // Once per page load, after the other deferred scripts have run and
  // connections.js has resolved the signed-in user. Wrapped: a page must
  // render whether or not the cloud answers. Pages that show credentials
  // listen for 'cygenix:conn-secrets-synced' and re-render.
  function autoStart() {
    try {
      if (typeof document === 'undefined' || typeof window === 'undefined') return;
      const go = () => setTimeout(() => { try { sync(); } catch { /* next load */ } }, 600);
      if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', go, { once: true });
      else go();
    } catch { /* never block the page */ }
  }

  window.CygenixSavedConnSecrets = {
    get: getSecret,
    set: setSecret,
    pruneTo,
    rehydrate,
    split,
    hasSecret,
    // Sep-2026: the cloud half.
    status,
    sync,
    synced: () => _syncedOnce,
    KEY,
    LIVE_IDS,
    // For the tests — not for pages.
    _flushNow: flush,
    _pendingSize: () => _pending.size,
    _reset: () => { _pending.clear(); _prunePending = null; _lastFlushAt = 0; _lastSyncAt = 0; _syncPromise = null; _batchIds = null; _batchFailed = false; _undecryptable.clear(); _syncedOnce = false; if (_debounce) { clearTimeout(_debounce); _debounce = null; } },
  };

  autoStart();
})();
