// tests/live-connection-secrets.test.js — the live pair's credentials never
// reach Cosmos in the clear, and a new device can still recover them.
//
// The active connection pair (cygenix_project_connections) is in SYNC_KEYS,
// and for a long time it went to Cosmos with srcConnString, tgtConnString,
// srcFnKey and tgtFnKey in it — plaintext passwords in a document store keyed
// by email. Phase 4 of the encrypted-sync brief closes that on four fronts,
// and this file pins each one by running the real modules:
//
//   1. OUT — cygenix-cosmos-sync.js strips the four fields (and the legacy
//      flat-key names) from what save()/saveDetailed() send. The local blob
//      is untouched, so every reader on this device keeps working.
//   2. IN  — applyCloud() carries this device's credentials across when the
//      cloud copy (which has none) arrives, so "cloud wins on load" cannot
//      blank a password that was only ever local. A slice the cloud no
//      longer has is dropped, credentials and all.
//   3. ACROSS devices — connections.js mirrors the live pair into the
//      encrypted store under sconn_live_src / sconn_live_tgt on every
//      setActive(), fills get() from it when the blob lacks a credential the
//      mode needs, clears both on clear(), and pruneTo() never removes them.
//   4. SERVER — azure-function/src/index.js strips the same fields on every
//      save (an old client cannot put them back), and an admin-only
//      scrub-connection-secrets action cleans documents already stored. The
//      log carries a count, never a value.
//
// Plus the two things that make 3 work at all: the secrets module is loaded
// on every page that syncs the pair, and the proxy allow-list knows the
// scrub action.
'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));
const ROOT = path.join(__dirname, '..');
const PUB = path.join(ROOT, 'public');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');
const settle = () => new Promise((r) => setImmediate(r));

const SECRET_FIELDS = ['srcConnString', 'tgtConnString', 'srcFnKey', 'tgtFnKey'];
const hasSecret = (o) => !!o && SECRET_FIELDS.some((f) => f in o);

console.log('Live connection credentials — stripped going out, kept locally, recoverable elsewhere\n');

/* ── A browser for cosmos-sync, near enough ─────────────────────────────── */
function jwt(claims) {
  const b64 = (o) => Buffer.from(JSON.stringify(o)).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  return b64({ alg: 'none' }) + '.' + b64(claims) + '.sig';
}
const TOKEN = jwt({ preferred_username: 'someone@example.com' });
const UID = 'someone@example.com';

function makeStorage(seed) {
  const data = Object.assign({}, seed || {});
  return {
    getItem: (k) => (Object.prototype.hasOwnProperty.call(data, k) ? data[k] : null),
    setItem: (k, v) => { data[k] = String(v); },
    removeItem: (k) => { delete data[k]; },
    clear: () => { Object.keys(data).forEach((k) => delete data[k]); },
    _data: data,
  };
}
const quiet = { log() {}, warn() {}, error() {}, info() {} };

function bootSync(opts) {
  const o = opts || {};
  const localStorage = makeStorage(Object.assign({ cygenix_token: TOKEN }, o.local || {}));
  const calls = [];
  const window = {
    addEventListener() {},
    dispatchEvent() { return true; },
    CygenixDataApi: {
      async callResult(action, opts2) {
        const method = (opts2 && opts2.method) || 'GET';
        calls.push({ action, method, body: opts2 && opts2.body });
        const dflt = (a) => ({ ok: true, data: a === 'save' ? { saved: true } : {} });
        return (o.responder || dflt)(action, method, opts2 && opts2.body);
      },
    },
  };
  const ctx = {
    window, localStorage, sessionStorage: makeStorage({}), console: quiet,
    CustomEvent: function (type, init) { this.type = type; this.detail = init && init.detail; },
    setTimeout: () => 0, clearTimeout: () => {},
    document: { createElement: () => ({ style: {}, setAttribute() {}, appendChild() {} }), body: { appendChild() {}, removeChild() {} } },
    atob: (s) => Buffer.from(s, 'base64').toString('binary'),
  };
  ctx.globalThis = ctx;
  vm.createContext(ctx);
  vm.runInContext(read('public', 'cygenix-cosmos-sync.js'), ctx, { filename: 'cygenix-cosmos-sync.js' });
  return { sync: window.CygenixSync, localStorage, calls };
}

const LIVE_BLOB = {
  [UID]: {
    srcConnString: 'Server=tcp:src.example,1433;Password=hunter2;', srcConnMode: 'direct', srcFnUrl: '', srcFnKey: '',
    tgtConnString: '', tgtConnMode: 'azure', tgtFnUrl: 'https://fn.example/api/sql', tgtFnKey: 'fnkey-abc',
  },
  'other@example.com': { srcConnString: 'mssql://u:p@h/db', srcConnMode: 'direct' },
};

(async () => {
  /* ── 1. Outbound: what save() sends ──────────────────────────────────── */
  section('1. Out — cosmos-sync strips the credentials from what it sends');
  {
    const t = bootSync({ local: { cygenix_project_connections: JSON.stringify(LIVE_BLOB) } });
    await settle(); await settle();
    t.calls.length = 0;
    const d = await t.sync.saveNow();
    const save = t.calls.find((c) => c.action === 'save');
    check('saveNow() posts the connections field', d.ok === true && !!save && !!save.body.connections, JSON.stringify(d));
    const sent = save ? save.body.connections : {};
    check('NO connection string or function key leaves the device',
      !hasSecret(sent[UID]) && !hasSecret(sent['other@example.com']), JSON.stringify(sent));
    check('mode and URL still go — the other device learns the shape of the pair',
      sent[UID] && sent[UID].srcConnMode === 'direct' && sent[UID].tgtConnMode === 'azure' && sent[UID].tgtFnUrl === 'https://fn.example/api/sql',
      JSON.stringify(sent));
    check('the whole payload is free of the four field names',
      !SECRET_FIELDS.some((f) => JSON.stringify(save.body).includes('"' + f + '"')));
    check('the LOCAL blob still holds them — every reader on this device is unaffected',
      JSON.parse(t.localStorage.getItem('cygenix_project_connections'))[UID].srcConnString === LIVE_BLOB[UID].srcConnString
      && JSON.parse(t.localStorage.getItem('cygenix_project_connections'))[UID].tgtFnKey === 'fnkey-abc');

    t.calls.length = 0;
    t.localStorage.setItem('cygenix_project_connections', JSON.stringify(LIVE_BLOB));
    await t.sync.save();
    const save2 = t.calls.find((c) => c.action === 'save');
    check('save() — the other upload path — strips too',
      !!save2 && !hasSecret(save2.body.connections[UID]), JSON.stringify(save2 && save2.body.connections));
  }
  {
    const flat = { cygenix_src_conn_string: 'Server=a;Password=b;', cygenix_conn_string: 'x', cygenix_fn_key: 'k', cygenix_fn_url: 'https://f' };
    const t = bootSync({ local: { cygenix_project_connections: JSON.stringify(flat) } });
    await settle(); await settle(); t.calls.length = 0;
    await t.sync.saveNow();
    const save = t.calls.find((c) => c.action === 'save');
    const sent = save ? save.body.connections : null;
    check('the legacy flat shape (pre per-user blob) is stripped as well',
      !!sent && !('cygenix_src_conn_string' in sent) && !('cygenix_conn_string' in sent) && !('cygenix_fn_key' in sent) && sent.cygenix_fn_url === 'https://f',
      JSON.stringify(sent));
  }

  /* ── 2. Inbound: cloud wins, except for what it never had ────────────── */
  section('2. In — a credential-free cloud copy does not blank the local one');
  {
    const cloud = {
      connections: {
        [UID]: { srcConnMode: 'direct', srcFnUrl: '', tgtConnMode: 'azure', tgtFnUrl: 'https://fn.example/api/v2' },
        // other@example.com is gone from the cloud: it was cleared elsewhere.
      },
    };
    const t = bootSync({
      local: { cygenix_project_connections: JSON.stringify(LIVE_BLOB) },
      responder: (action) => (action === 'load' ? { ok: true, data: cloud } : { ok: true, data: {} }),
    });
    await settle(); await settle();
    const d = await t.sync.loadDetailed({ force: true });
    const after = JSON.parse(t.localStorage.getItem('cygenix_project_connections'));
    check('the load applied', d.ok === true && d.applied >= 1, JSON.stringify(d));
    check('THE PASSWORD SURVIVES — this device\'s connection string is carried into the cloud slice',
      after[UID].srcConnString === LIVE_BLOB[UID].srcConnString, JSON.stringify(after));
    check('and so does the function key', after[UID].tgtFnKey === 'fnkey-abc', JSON.stringify(after));
    check('while everything else is the cloud\'s — the URL changed on another device and that wins',
      after[UID].tgtFnUrl === 'https://fn.example/api/v2', JSON.stringify(after));
    check('a user slice the cloud no longer has is dropped, credential and all',
      !('other@example.com' in after), JSON.stringify(after));
  }
  {
    const cloud = { connections: { [UID]: { srcConnMode: 'direct' } } };
    const t = bootSync({ local: {}, responder: (action) => (action === 'load' ? { ok: true, data: cloud } : { ok: true, data: {} }) });
    await settle(); await settle();
    await t.sync.loadDetailed({ force: true });
    const after = JSON.parse(t.localStorage.getItem('cygenix_project_connections'));
    check('on a device with nothing local, the slice arrives as-is and holds no credential',
      after && after[UID] && after[UID].srcConnMode === 'direct' && !hasSecret(after[UID]), JSON.stringify(after));
  }

  /* ── 3. Across devices: the encrypted store ──────────────────────────── */
  section('3. Across — connections.js mirrors the live pair into the encrypted store and fills from it');
  {
    const mk = () => {
      const mem = new Map();
      return { getItem: (k) => (mem.has(k) ? mem.get(k) : null), setItem: (k, v) => { mem.set(k, String(v)); }, removeItem: (k) => { mem.delete(k); }, _mem: mem };
    };
    global.localStorage = mk();
    global.sessionStorage = mk();
    global.window = global;
    global.atob = (s) => Buffer.from(s, 'base64').toString('binary');
    global.dispatchEvent = () => true;
    global.addEventListener = () => {};
    global.CustomEvent = class { constructor(type, init) { this.type = type; this.detail = init && init.detail; } };
    global.console = Object.assign({}, console, { error() {}, warn() {} });
    localStorage.setItem('cygenix_user', JSON.stringify({ email: 'you@example.test' }));
    // Signed out as far as the data layer is concerned: the secrets module
    // must not try to talk to the cloud from a test.
    global.CygenixDataApi = { isSignedIn: () => false, callResult: async () => ({ ok: false, error: { code: 'no-token' } }) };
    global.getCygenixIdToken = () => null;

    require(path.join(PUB, 'connections.js'));
    require(path.join(PUB, 'cygenix-saved-conn-secrets.js'));
    const C = global.CygenixConnections;
    const S = global.CygenixSavedConnSecrets;
    const KEY = 'cygenix_project_connections';
    const blob = () => JSON.parse(localStorage.getItem(KEY) || '{}');

    check('the reserved ids are published by the store', Array.isArray(S.LIVE_IDS) && S.LIVE_IDS.includes('sconn_live_src') && S.LIVE_IDS.includes('sconn_live_tgt'));

    C.setActive({ srcConnMode: 'direct', srcConnString: 'Server=tcp:src;Password=pw1;', tgtConnMode: 'azure', tgtFnUrl: 'https://fn.example/api/sql', tgtFnKey: 'key-9' });
    check('setActive() mirrors the source string into sconn_live_src',
      S.get('sconn_live_src') && S.get('sconn_live_src').connString === 'Server=tcp:src;Password=pw1;', JSON.stringify(S.get('sconn_live_src')));
    check('and the target function key into sconn_live_tgt',
      S.get('sconn_live_tgt') && S.get('sconn_live_tgt').fnKey === 'key-9' && !S.get('sconn_live_tgt').connString, JSON.stringify(S.get('sconn_live_tgt')));

    // The new device: the blob arrived from the cloud with modes and URLs but
    // no credentials (section 1 is why), and the encrypted store has synced.
    const b = blob();
    const stripped = Object.assign({}, b['you@example.test']);
    SECRET_FIELDS.forEach((f) => delete stripped[f]);
    localStorage.setItem(KEY, JSON.stringify({ 'you@example.test': stripped }));
    check('(setup) the blob now lacks every credential', !hasSecret(blob()['you@example.test']));

    const got = C.get();
    check('get() FILLS the source string from the store when the mode needs one',
      got.srcConnString === 'Server=tcp:src;Password=pw1;' && got.srcConnMode === 'direct', JSON.stringify(got));
    check('and the target key, keeping the URL the cloud sent',
      got.tgtFnKey === 'key-9' && got.tgtFnUrl === 'https://fn.example/api/sql' && got.tgtConnMode === 'azure', JSON.stringify(got));
    check('what it found is written back into the blob, so readers that never learned about the store see it too',
      blob()['you@example.test'].srcConnString === 'Server=tcp:src;Password=pw1;' && blob()['you@example.test'].tgtFnKey === 'key-9', JSON.stringify(blob()));

    // A blob that already has its credentials is not touched — the store is
    // not consulted, and the local value wins even if the store differs.
    S.set('sconn_live_src', { connString: 'Server=tcp:stale;Password=old;' });
    check('a blob that already holds a credential is left alone — local wins', C.get().srcConnString === 'Server=tcp:src;Password=pw1;');

    // A side with no mode requirement is not filled: an empty pair on a fresh
    // device must stay empty, not inherit a string from an old session.
    localStorage.setItem(KEY, JSON.stringify({ 'you@example.test': { srcConnMode: 'azure', srcFnUrl: 'https://fn.example/api/a', tgtConnMode: 'direct' } }));
    const g2 = C.get();
    check('an azure side is not given a connection string, only a key', g2.srcConnString === '' && g2.srcFnUrl === 'https://fn.example/api/a', JSON.stringify(g2));

    // pruneTo, which sconnSetAll calls with the SAVED list's ids, must not
    // sweep the live pair away.
    S.set('sconn_live_src', { connString: 'Server=tcp:src;Password=pw1;' });
    S.set('sconn_zzz', { connString: 'gone' });
    S.pruneTo(['sconn_keep']);
    check('pruneTo() keeps the two live ids however short the saved list is',
      S.hasSecret('sconn_live_src') && S.hasSecret('sconn_live_tgt') && !S.hasSecret('sconn_zzz'));

    C.clear();
    check('clear() removes both live secrets from the store, not only the blob',
      !S.hasSecret('sconn_live_src') && !S.hasSecret('sconn_live_tgt') && !blob()['you@example.test']);

    const CS = read('public', 'connections.js');
    const code = CS.split('\n').filter((l) => !/^\s*(\/\/|\*|\/\*)/.test(l)).join('\n');
    check('the mirror happens inside setActive(), the one writer — not in a caller',
      /writeBlob\(LS_ACTIVE, blob\);\s*mirrorLiveSecrets\(s, t\);/.test(code));
    check('the store is optional: a page without the module still reads and writes the blob',
      /if \(!S \|\| typeof S\.get !== 'function'\) return mine;/.test(code) && /if \(!S \|\| typeof S\.set !== 'function'\) return;/.test(code));
  }

  /* ── 4. The server ───────────────────────────────────────────────────── */
  section('4. Server — the Function App strips on save and can scrub what is already there');
  {
    const IDX = read('azure-function', 'src', 'index.js');
    const m = IDX.match(/const LIVE_SECRET_FIELDS = new Set\(\[[\s\S]*?\n\]\);\nfunction stripConnectionSecrets\(conns\) \{[\s\S]*?\n\}\n/);
    check('index.js defines LIVE_SECRET_FIELDS and stripConnectionSecrets at module scope', !!m);
    const ctx = {}; vm.createContext(ctx);
    vm.runInContext((m ? m[0] : '') + '\nthis.strip = stripConnectionSecrets;', ctx);
    const strip = ctx.strip;
    const r = strip ? strip(LIVE_BLOB) : null;
    check('it removes the four fields from every user slice and counts what it removed',
      !!r && !hasSecret(r.value[UID]) && !hasSecret(r.value['other@example.com']) && r.stripped === 3, JSON.stringify(r));
    check('empty credential fields are dropped but not counted — the log says how many secrets, not how many keys',
      strip && strip({ [UID]: { srcConnString: '', srcFnKey: '', srcConnMode: 'direct' } }).stripped === 0);
    check('mode and URL are kept', !!r && r.value[UID].tgtFnUrl === 'https://fn.example/api/sql' && r.value[UID].srcConnMode === 'direct');
    check('the legacy flat keys are removed too',
      strip && !('cygenix_src_conn_string' in strip({ cygenix_src_conn_string: 'x', cygenix_fn_url: 'u' }).value));
    check('a non-object passes through unchanged', strip && strip(null).value === null && strip('s').value === 's');

    const code = IDX.split('\n').filter((l) => !/^\s*(\/\/|\*|\/\*)/.test(l)).join('\n');
    check('the save action strips whatever the client sent, before the document is written',
      /case 'save'[\s\S]*?merged\.connections = s\.value;[\s\S]*?\.upsert\(/.test(code));
    check('the save log line carries a count, never a value',
      /ctx\.log\(`save: stripped \$\{s\.stripped\} live connection credential field\(s\)/.test(code)
      && !/ctx\.log\([^\n]*(srcConnString|connString|s\.value)[^\n]*\)/.test(code.match(/case 'save'[\s\S]*?case '/)[0]));
    const scrub = (code.match(/case 'scrub-connection-secrets': \{[\s\S]*?\n {8}\}/) || [''])[0];
    check('scrub-connection-secrets exists', scrub.length > 0);
    check('it is admin-only and POST-only', /requireAdmin\(userId\)/.test(scrub) && /req\.method !== 'POST'/.test(scrub));
    check('it rewrites only documents that held a credential', /if \(!s\.stripped\) continue;/.test(scrub) && /\.replace\(doc\)/.test(scrub));
    check('it reports counts and the ids that failed, never a value',
      /return ok\(\{ scanned, scrubbed, fieldsRemoved, failed \}\)/.test(scrub) && !/connString|srcConnString/.test(scrub));
    check('the scrub log line is counts only', /ctx\.log\(`scrub-connection-secrets: \$\{scanned\} scanned/.test(scrub) && !/ctx\.log\([^\n]*(doc\.connections|s\.value)/.test(scrub));

    const PROXY = read('netlify', 'functions', 'data-proxy.js');
    check('the proxy allow-list knows the scrub action — it cannot be reached any other way',
      /'scrub-connection-secrets'/.test(PROXY.split('\n').filter((l) => !/^\s*(\/\/|\*|\/\*)/.test(l)).join('\n')));

    const CSYNC = read('public', 'cygenix-cosmos-sync.js');
    const clientList = (CSYNC.match(/const LIVE_SECRET_FIELDS = new Set\(\[([\s\S]*?)\]\);/) || [])[1];
    const serverList = (IDX.match(/const LIVE_SECRET_FIELDS = new Set\(\[([\s\S]*?)\]\);/) || [])[1];
    const names = (s) => (s || '').match(/'[^']+'/g).sort().join(',');
    check('the client and server strip the SAME list of field names', !!clientList && names(clientList) === names(serverList), names(clientList) + ' vs ' + names(serverList));
  }

  /* ── 5. Every syncing page can resolve the live pair ─────────────────── */
  section('5. Pages — the secrets module is loaded wherever the pair syncs');
  {
    const missing = [];
    for (const f of fs.readdirSync(PUB).filter((n) => n.endsWith('.html'))) {
      const html = fs.readFileSync(path.join(PUB, f), 'utf8');
      const syncs = /cygenix-cosmos-sync\.js/.test(html);
      const conns = /\/connections\.js/.test(html);
      const secrets = /cygenix-saved-conn-secrets\.js/.test(html);
      if (syncs && conns && !secrets) missing.push(f);
    }
    check('every page that loads both cosmos-sync and connections.js also loads the secrets module',
      missing.length === 0, 'missing on: ' + missing.join(', '));
    const inv = read('scripts', 'storage-inventory.js');
    const CSYNC = read('public', 'cygenix-cosmos-sync.js');
    const sk = (CSYNC.match(/const SYNC_KEYS = \[([\s\S]*?)\];/) || [])[1] || '';
    check('cygenix_saved_conn_secrets is still not a synced key', !/cygenix_saved_conn_secrets/.test(sk));
    check('and is classified in the inventory as before', /cygenix_saved_conn_secrets/.test(inv));
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
