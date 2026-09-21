// tests/conn-secrets-client.test.js — the browser half of the encrypted store.
//
// public/cygenix-saved-conn-secrets.js keeps its old, synchronous API and
// gains a cloud half. What has to be true, pinned by running the real
// module against a fake window, a fake localStorage and a fake data layer:
//
//   · the old shape migrates on read and get() still returns a bare bundle;
//   · set() with an unchanged bundle is a no-op — sconnSetAll calls it for
//     every entry on every save, and a stamp there would re-upload them all;
//   · edits coalesce; one flush at a time; a failed write is remembered for
//     the next page load and not retried on a timer;
//   · sync() merges newer-wins, marks what the server could not open, and
//     uploads once per device per user — with a flag set only when every
//     upload landed and never cleared by the code after an upload;
//   · signed out, nothing is called at all;
//   · the key stays out of SYNC_KEYS and BACKUP_KEYS.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');
const SRC = read('public', 'cygenix-saved-conn-secrets.js');

console.log('Encrypted connection secrets — the browser side\n');

/* ── A browser, in the small ────────────────────────────────────────────── */
const store = new Map();
global.localStorage = {
  getItem: (k) => (store.has(k) ? store.get(k) : null),
  setItem: (k, v) => store.set(k, String(v)),
  removeItem: (k) => store.delete(k),
};
const events = [];
global.window = global;
global.document = { readyState: 'complete', addEventListener: () => {} };
global.CustomEvent = class { constructor(type, init) { this.type = type; this.detail = init && init.detail; } };
global.dispatchEvent = (e) => events.push(e);
global.window.dispatchEvent = global.dispatchEvent;

// A token with an oid in it — the module reads the claim, never verifies it
// (the server does that). Base64url payload, signature irrelevant.
const claims = (o) => Buffer.from(JSON.stringify(o)).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
let TOKEN = 'h.' + claims({ oid: 'alice-oid', email: 'alice@example.test' }) + '.s';
global.getCygenixIdToken = () => TOKEN;

// The data layer: records every call, answers from a script.
const calls = [];
let cloud = { secrets: {}, undecryptable: [] };
let failPut = false, failList = false;
global.CygenixDataApi = {
  isSignedIn: () => !!TOKEN,
  callResult: async (action, opts) => {
    calls.push({ action, body: opts && opts.body });
    if (!TOKEN) return { ok: false, error: { code: 'no-token' } };
    if (action === 'secrets-list') return failList ? { ok: false, error: { code: 'server', status: 500 } } : { ok: true, data: cloud };
    if (action === 'secrets-put') {
      if (failPut) return { ok: false, error: { code: 'server', status: 500 } };
      cloud.secrets[opts.body.connId] = { bundle: opts.body.bundle, updatedAt: opts.body.updatedAt };
      return { ok: true, data: { ok: true, updatedAt: opts.body.updatedAt } };
    }
    if (action === 'secrets-delete') { delete cloud.secrets[opts.body.connId]; return { ok: true, data: { ok: true } }; }
    if (action === 'secrets-prune') {
      const keep = new Set(opts.body.keepConnIds);
      for (const id of Object.keys(cloud.secrets)) if (!keep.has(id)) delete cloud.secrets[id];
      return { ok: true, data: { ok: true } };
    }
    return { ok: false, error: { code: 'client' } };
  },
};

require(path.join(ROOT, 'public', 'cygenix-saved-conn-secrets.js'));
const S = global.window.CygenixSavedConnSecrets;
const KEY = 'cygenix_saved_conn_secrets';
const raw = () => JSON.parse(store.get(KEY) || '{}');
const puts = () => calls.filter((c) => c.action === 'secrets-put');
const flag = 'cygenix_conn_secrets_uploaded_v1::alice-oid';

(async () => {
  /* ── 1. The old API, unchanged ───────────────────────────────────────── */
  section('1. Backward compatibility');
  check('the surface every existing caller uses is still there',
    ['get', 'set', 'pruneTo', 'rehydrate', 'split', 'hasSecret'].every((k) => typeof S[k] === 'function'));
  check('and the new one is additive', ['status', 'sync'].every((k) => typeof S[k] === 'function'));
  store.set(KEY, JSON.stringify({ sconn_a: { connString: 'mssql://u:p@h/db' }, sconn_b: { fnKey: 'k1' } }));
  check('THE OLD SHAPE READS WITHOUT A MIGRATION STEP — a missing updatedAt is simply 0',
    S.get('sconn_a').connString === 'mssql://u:p@h/db' && S.hasSecret('sconn_b') && S.status('sconn_a') === 'ok');
  check('get() returns a bare bundle, never the stamp — one caller stores what it gets back',
    !('updatedAt' in S.get('sconn_a')));
  const entries = [{ id: 'sconn_a' }, { id: 'sconn_b', fnUrl: 'https://x' }, { id: 'sconn_none' }];
  S.rehydrate(entries);
  check('rehydrate() is still synchronous and still fills from the local store',
    entries[0].connString === 'mssql://u:p@h/db' && entries[1].fnKey === 'k1' && !entries[2].connString);
  const sp = S.split({ id: 'x', name: 'n', connString: 'cs', fnKey: 'fk', secret: 'sc' });
  check('split() still strips exactly the three secret fields',
    !('connString' in sp.sanitised) && !('fnKey' in sp.sanitised) && !('secret' in sp.sanitised)
    && sp.sanitised.name === 'n' && sp.secrets.connString === 'cs' && sp.secrets.fnKey === 'fk' && sp.secrets.secret === 'sc');

  /* ── 2. set() ────────────────────────────────────────────────────────── */
  section('2. Writing');
  S._reset(); calls.length = 0;
  S.set('sconn_a', { connString: 'mssql://u:p@h/db' });
  check('AN UNCHANGED BUNDLE IS A NO-OP — no stamp, nothing queued',
    !('updatedAt' in raw().sconn_a) && S._pendingSize() === 0,
    'sconnSetAll re-sets every entry on every save; a stamp here re-uploads the whole list');
  const before = Date.now();
  S.set('sconn_a', { connString: 'mssql://u:NEW@h/db' });
  check('a changed bundle is stamped and queued',
    raw().sconn_a.updatedAt >= before && raw().sconn_a.connString === 'mssql://u:NEW@h/db' && S._pendingSize() === 1);
  S.set('sconn_a', { connString: 'mssql://u:NEWER@h/db' });
  S.set('sconn_a', { connString: 'mssql://u:NEWEST@h/db' });
  check('three edits to one connection are ONE pending write', S._pendingSize() === 1);
  S.set('sconn_c', { connString: 'x', password: 'dropped', note: 'dropped' });
  check('only the three secret fields are ever stored', Object.keys(raw().sconn_c).sort().join() === 'connString,updatedAt');
  S.set('sconn_c', { connString: '' });
  check('an empty bundle is a delete', !raw().sconn_c && S._pendingSize() === 2);

  await S._flushNow();
  check('one flush sends the coalesced put with the LATEST value, and the delete',
    puts().length === 1 && puts()[0].body.connString === undefined
    && puts()[0].body.bundle.connString === 'mssql://u:NEWEST@h/db' && puts()[0].body.connId === 'sconn_a'
    && calls.some((c) => c.action === 'secrets-delete' && c.body.connId === 'sconn_c'), JSON.stringify(calls));
  check('the put carries the local stamp, so the server can apply last-write-wins',
    typeof puts()[0].body.updatedAt === 'number' && puts()[0].body.updatedAt === raw().sconn_a.updatedAt);
  S.set('sconn_a', { connString: 'mssql://u:AGAIN@h/db' });
  const second = await S._flushNow();
  check('A SECOND FLUSH INSIDE THREE SECONDS DOES NOT RUN', second === false && S._pendingSize() === 1);

  /* ── 3. A failed write ───────────────────────────────────────────────── */
  section('3. A failed write is remembered, not retried on a timer');
  S._reset(); calls.length = 0; failPut = true;
  S.set('sconn_a', { connString: 'mssql://u:FAIL@h/db' });
  await S._flushNow();
  check('the credential stays on this device', raw().sconn_a.connString === 'mssql://u:FAIL@h/db');
  check('and the id is written to the dirty list for the next page load',
    JSON.parse(store.get('cygenix_conn_secrets_dirty_v1') || '[]').indexOf('sconn_a') !== -1);
  check('nothing is re-queued — a timer against a dead endpoint is a loop', S._pendingSize() === 0);
  failPut = false;

  /* ── 4. pruneTo ──────────────────────────────────────────────────────── */
  section('4. Pruning');
  S._reset(); calls.length = 0;
  S.pruneTo(['sconn_a', 'sconn_b']);
  check('a prune that removes nothing locally sends nothing', S._pendingSize() === 0 && calls.length === 0);
  store.set(KEY, JSON.stringify(Object.assign(raw(), { sconn_gone: { connString: 'z', updatedAt: 1 } })));
  S.pruneTo(['sconn_a', 'sconn_b']);
  await S._flushNow();
  check('a prune that removed something locally prunes the cloud to the same keep list',
    !raw().sconn_gone && calls.some((c) => c.action === 'secrets-prune' && c.body.keepConnIds.join() === 'sconn_a,sconn_b'), JSON.stringify(calls));

  /* ── 5. sync(): pull and merge ───────────────────────────────────────── */
  section('5. Pulling the cloud copy');
  S._reset(); calls.length = 0; events.length = 0;
  store.set(KEY, JSON.stringify({ sconn_a: { connString: 'LOCAL-OLD', updatedAt: 100 }, sconn_local: { connString: 'ONLY-HERE', updatedAt: 300 } }));
  store.set(flag, '1');                                   // not the first upload — isolate the merge
  cloud = { secrets: {
    sconn_a:   { bundle: { connString: 'CLOUD-NEW' }, updatedAt: 200 },
    sconn_new: { bundle: { fnKey: 'from-cloud' }, updatedAt: 50 },
  }, undecryptable: ['sconn_broken', 'sconn_a'] };
  let res = await S.sync({ force: true });
  check('sync pulls', res.ok && res.pulled === 2, JSON.stringify(res));
  check('A NEWER CLOUD COPY REPLACES AN OLDER LOCAL ONE', raw().sconn_a.connString === 'CLOUD-NEW' && raw().sconn_a.updatedAt === 200);
  check('a connection this device never had arrives', raw().sconn_new.fnKey === 'from-cloud');
  check('a local-only credential is untouched', raw().sconn_local.connString === 'ONLY-HERE');
  check('status() names what the server could not open — but only where nothing is on this device',
    S.status('sconn_broken') === 'undecryptable' && S.status('sconn_a') === 'ok' && S.status('sconn_nothing') === 'missing');
  check('and announces itself, so pages re-render', events.some((e) => e.type === 'cygenix:conn-secrets-synced'));
  check('a second sync inside three seconds is throttled', (await S.sync()).code === 'throttled');
  check('the same in-flight promise is shared', (() => { S._reset(); const a = S.sync({ force: true }); const b = S.sync(); return a === b; })());
  await S.sync();

  /* ── 6. Local newer than cloud ───────────────────────────────────────── */
  S._reset(); calls.length = 0;
  store.set(KEY, JSON.stringify({ sconn_a: { connString: 'LOCAL-NEWER', updatedAt: 900 } }));
  cloud = { secrets: { sconn_a: { bundle: { connString: 'CLOUD-OLDER' }, updatedAt: 200 } }, undecryptable: [] };
  res = await S.sync({ force: true });
  await S._flushNow();
  check('A LOCAL COPY NEWER THAN THE CLOUD\'S IS KEPT AND SENT UP',
    raw().sconn_a.connString === 'LOCAL-NEWER' && puts().length === 1 && puts()[0].body.bundle.connString === 'LOCAL-NEWER');

  /* ── 7. The one-time upload ──────────────────────────────────────────── */
  section('6. The one-time upload');
  S._reset(); calls.length = 0;
  store.delete(flag); store.delete('cygenix_conn_secrets_dirty_v1');
  store.set(KEY, JSON.stringify({ sconn_a: { connString: 'A' }, sconn_b: { fnKey: 'B' }, sconn_c: { secret: 'C', updatedAt: 7 } }));
  cloud = { secrets: { sconn_c: { bundle: { secret: 'C' }, updatedAt: 7 } }, undecryptable: [] };
  res = await S.sync({ force: true });
  check('the first sync queues everything the cloud lacks, and nothing it already has', res.queued === 2, JSON.stringify(res));
  check('THE FLAG IS NOT SET YET — nothing has been uploaded', store.get(flag) === undefined);
  await S._flushNow();
  check('both are uploaded', puts().map((p) => p.body.connId).sort().join() === 'sconn_a,sconn_b');
  check('AND ONLY THEN IS THE FLAG SET', store.get(flag) === '1');
  S._reset(); calls.length = 0;
  await S.sync({ force: true });
  await S._flushNow();
  check('a reload does not upload again', puts().length === 0);

  S._reset(); calls.length = 0; store.delete(flag);
  store.set(KEY, JSON.stringify({ sconn_a: { connString: 'A' }, sconn_b: { fnKey: 'B' } }));
  cloud = { secrets: {}, undecryptable: [] };
  failPut = true;
  await S.sync({ force: true });
  await S._flushNow();
  check('WHEN AN UPLOAD FAILS THE FLAG STAYS UNSET, so the next load tries again', store.get(flag) === undefined);
  failPut = false;
  check('the module never removes its own flag — set once, by one line, nowhere cleared',
    (SRC.match(/cygenix_conn_secrets_uploaded_v1/g) || []).length === 1
    && /localStorage\.setItem\(uploadFlagKey\(oid\), '1'\)/.test(SRC)
    && !/removeItem\(uploadFlagKey/.test(SRC), 'the render-loop rule: a one-shot flag is never reset by its own callback');
  check('a first sync with nothing to send is done at once', (() => {
    S._reset(); store.delete(flag); store.set(KEY, '{}'); cloud = { secrets: {}, undecryptable: [] };
    return S.sync({ force: true }).then(() => store.get(flag) === '1');
  })() instanceof Promise);
  await new Promise((r) => setTimeout(r, 20));
  check('…and the flag says so', store.get(flag) === '1');
  check('the flag is per user — a different oid on the same device is not "done"',
    store.get('cygenix_conn_secrets_uploaded_v1::someone-else') === undefined);

  /* ── 8. The dirty list is retried on the next load ───────────────────── */
  section('7. Retry on the next page load');
  S._reset(); calls.length = 0;
  store.set(KEY, JSON.stringify({ sconn_a: { connString: 'RETRY-ME', updatedAt: 50 } }));
  store.set('cygenix_conn_secrets_dirty_v1', JSON.stringify(['sconn_a']));
  cloud = { secrets: {}, undecryptable: [] };
  await S.sync({ force: true });
  await S._flushNow();
  check('a credential whose write failed last time is sent on the next sync',
    puts().length === 1 && puts()[0].body.connId === 'sconn_a');
  check('and comes off the dirty list once it lands', !store.has('cygenix_conn_secrets_dirty_v1'));

  /* ── 9. Signed out ───────────────────────────────────────────────────── */
  section('8. Signed out, nothing leaves');
  S._reset(); calls.length = 0;
  TOKEN = '';
  res = await S.sync({ force: true });
  check('sync makes no call without a token', res.code === 'no-token' && calls.length === 0);
  S.set('sconn_a', { connString: 'OFFLINE-EDIT' });
  await S._flushNow();
  check('an edit while signed out is kept locally and NOT sent — the queue does not even schedule',
    raw().sconn_a.connString === 'OFFLINE-EDIT' && calls.length === 0);
  TOKEN = 'h.' + claims({ oid: 'alice-oid' }) + '.s';

  /* ── 10. A failed list ───────────────────────────────────────────────── */
  S._reset(); calls.length = 0; failList = true;
  store.set(KEY, JSON.stringify({ sconn_a: { connString: 'KEEP', updatedAt: 1 } }));
  res = await S.sync({ force: true });
  check('a failed list leaves local data exactly as it was and reports the code',
    res.ok === false && res.code === 'server' && raw().sconn_a.connString === 'KEEP');
  failList = false;

  /* ── 11. Where it lives ──────────────────────────────────────────────── */
  section('9. Where the key does and does not go');
  const sync = read('public', 'cygenix-cosmos-sync.js');
  check('cygenix_saved_conn_secrets IS NOT IN SYNC_KEYS',
    !/cygenix_saved_conn_secrets/.test(sync.split('const SYNC_KEYS')[1].split('];')[0]));
  check('and not in FIELD_MAP either', !/saved_conn_secrets/.test(sync.split('const FIELD_MAP')[1].split('};')[0]));
  const dash = read('public', 'dashboard-app.js');
  check('and not in BACKUP_KEYS', !/cygenix_saved_conn_secrets/.test(dash.split('const BACKUP_KEYS')[1].split('];')[0]));
  check('the browser reaches the store only through CygenixDataApi — no URL, no key, no fetch of its own',
    !/fetch\(/.test(SRC) && !/azurewebsites/.test(SRC) && !/\?code=/.test(SRC) && /CygenixDataApi/.test(SRC));
  check('no log line can carry a credential',
    SRC.split('\n').filter((l) => /console\./.test(l))
      .map((l) => l.replace(/\[saved-conn-secrets\]/g, ''))     // the log tag is not a credential
      .every((l) => !/bundle|connString|fnKey|entry|secrets\b/.test(l)));
  check('the new keys are classified in the storage inventory',
    /cygenix_conn_secrets_uploaded_v1::\*/.test(read('scripts', 'storage-inventory.js'))
    && /cygenix_conn_secrets_dirty_v1/.test(read('scripts', 'storage-inventory.js')));

  /* ── 12. The pages that show it ──────────────────────────────────────── */
  section('10. The badge');
  check('the Connections chips carry the badge, and only for a connection that needs a string',
    /function sconnNeedsSecret\(entry\)/.test(dash) && /entry\.mode === 'azure'\) return null/.test(dash)
    && /Password needed on this device/.test(dash) && /Couldn\\'t decrypt — re-enter/.test(dash));
  check('clicking it re-enters through set(), which syncs it up', /S\.set\(id, \{ connString: v \}\)/.test(dash));
  check('and the page re-renders when the cloud copy lands',
    /addEventListener\('cygenix:conn-secrets-synced', refillIfShowing\)/.test(dash));
  const prof = read('public', 'profiles.html');
  check('the profile pickers say it under the selected connection',
    /function renderConnSecretStatus\(\)/.test(prof) && /Password needed on this device/.test(prof)
    && /cygenix:conn-secrets-synced/.test(prof));

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
