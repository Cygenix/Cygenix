// tests/connection-recovery.test.js — why a cookie clear lost every credential,
// and the four things that now say so or fix it.
//
// WHAT WAS SEEN (Sep-2026, demo@cygenix.onmicrosoft.com, after clearing site data)
//   · data-proxy?action=secrets-list answered 503 (a 64-byte body) — every time.
//   · the live target came back as a Function App URL with no ?code= key, and
//     POST /api/db answered 401; Object Mapping said "Target: Non-JSON (401)".
//   · the red sync banner said "the server refused the change" while every
//     save in the network tab was 200 OK.
//
// WHAT WAS WRONG, in order
//   1. CONN_SECRETS_KEY has never been set on the Function App, so every
//      secrets route answers 503 and NO credential has ever been persisted.
//      The browser swallowed that in a console.warn. Now the store keeps a
//      cloudStatus() and the Connections and Profiles pages show it.
//   2. The product's own Function App key had to be typed and lived only in
//      the (never-synced) credential store. connections.js now fetches it
//      from the proxy's blob-credential hand-off when a side points at the
//      product's host and has no key — guarded, and never for another host.
//   3. The browser's FIELD_MAP had four fields the server's SYNCABLE did not.
//      A save carrying only one of them came back 200 saved:false
//      reason:'no-syncable-fields', which the client called "refused". The
//      lists are now pinned equal here, and the client names the field.
//   4. The connections.js legacy save() path was already fixed at the one
//      writer (setActive → normaliseSide); pinned here so it stays fixed.
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
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');
const code = (src) => src.split('\n').filter((l) => !/^\s*(\/\/|\*|\/\*)/.test(l)).join('\n');
const settle = () => new Promise((r) => setImmediate(r));

console.log('Connection recovery — credentials that fail loudly, keys that are fetched, saves that are named\n');

/* ── 1. The two field lists can never drift again ────────────────────────── */
section('1. The browser and the Function App agree on what syncs');
{
  const SYNC = read('public', 'cygenix-cosmos-sync.js');
  const IDX = read('azure-function', 'src', 'index.js');
  const fm = (code(SYNC).match(/const FIELD_MAP = \{([\s\S]*?)\n {2}\};/) || [])[1] || '';
  // Several fields share a line in FIELD_MAP, so match every `name:'cygenix_…'`
  // pair rather than the first per line.
  const clientFields = [...fm.matchAll(/([a-z_]+):\s*'cygenix_/g)].map((m) => m[1]);
  const sy = (code(IDX).match(/const SYNCABLE = \[([\s\S]*?)\];/) || [])[1] || '';
  const serverFields = [...sy.matchAll(/'([a-z_]+)'/g)].map((m) => m[1]);
  check('both lists were found, and the client list is the full twenty', clientFields.length === 20 && serverFields.length >= 20, clientFields.length + ' / ' + serverFields.length);
  const missing = clientFields.filter((f) => !serverFields.includes(f));
  check('EVERY FIELD THE BROWSER SENDS IS ONE THE SERVER SYNCS — this is the pin that would have caught the phantom banner',
    missing.length === 0, 'server lacks: ' + missing.join(', '));
  check('the four that were missing are now listed',
    ['map_groups', 'datagen_selection', 'datagen_runs', 'collation_ui'].every((f) => serverFields.includes(f)));
  check('the save answer names any field it dropped, on success and on the empty case',
    /return ok\(\{ saved: false, reason: 'no-syncable-fields', ignored,/.test(code(IDX))
    && /return ok\(\{ saved: true, updatedAt: merged\.updatedAt, fields: touched, ignored \}\)/.test(code(IDX)));
}

/* ── 2. The sync layer says which field, not "refused" ───────────────────── */
section('2. A 200 that did not save is named, not called a refusal');
{
  const SYNC_SRC = read('public', 'cygenix-cosmos-sync.js');
  const b64 = (o) => Buffer.from(JSON.stringify(o)).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  const TOKEN = b64({ alg: 'none' }) + '.' + b64({ preferred_username: 'someone@example.com' }) + '.sig';
  function boot(responder) {
    const data = { cygenix_token: TOKEN, cygenix_collation_ui_v1: '{"collapsed":true}' };
    const localStorage = {
      getItem: (k) => (k in data ? data[k] : null), setItem: (k, v) => { data[k] = String(v); },
      removeItem: (k) => { delete data[k]; }, clear: () => {}, _data: data,
    };
    const health = [];
    const window = {
      addEventListener() {}, dispatchEvent(e) { if (e.type === 'cygenix-sync-health') health.push(e.detail); return true; },
      CygenixDataApi: { async callResult(action, o) { return responder(action, o); } },
    };
    const ctx = { window, localStorage, sessionStorage: localStorage, console: { log() {}, warn() {}, error() {}, info() {} },
      CustomEvent: function (t, i) { this.type = t; this.detail = i && i.detail; },
      setTimeout: () => 0, clearTimeout: () => {}, document: { createElement: () => ({ style: {}, setAttribute() {}, appendChild() {} }), body: { appendChild() {}, removeChild() {} } },
      atob: (s) => Buffer.from(s, 'base64').toString('binary') };
    ctx.globalThis = ctx; vm.createContext(ctx);
    vm.runInContext(SYNC_SRC, ctx, { filename: 'cygenix-cosmos-sync.js' });
    return { sync: window.CygenixSync, health };
  }
  const skew = boot((a) => a === 'save'
    ? { ok: true, data: { saved: false, reason: 'no-syncable-fields', ignored: ['collation_ui'] } }
    : { ok: true, data: {} });
  (async () => {
    await settle(); await settle();
    const r = await skew.sync.saveNow();
    check('saveNow() reports the skew by code, not as a generic rejection', r.ok === false && r.code === 'unknown-fields', JSON.stringify(r));
    check('and names the field the server dropped', /collation_ui/.test(r.error) && Array.isArray(r.ignored) && r.ignored[0] === 'collation_ui', r.error);
    const h = skew.sync.getHealth();
    check('the health state carries the same code and the sentence with the field in it',
      h.degraded === true && h.reason === 'unknown-fields' && /does not sync collation_ui yet/.test(h.lastError), JSON.stringify(h));
    check('it says the change is KEPT, because it is — nothing was refused', /kept on this device/.test(h.lastError));

    const refused = boot((a) => a === 'save' ? { ok: true, data: { saved: false } } : { ok: true, data: {} });
    await settle(); await settle();
    const r2 = await refused.sync.saveNow();
    check('a genuine saved:false with no reason is still a rejection', r2.code === 'rejected' && refused.sync.getHealth().reason === 'rejected', JSON.stringify(r2));

    const BANNER = code(read('public', 'cygenix-sync-banner.js'));
    check('the banner knows the new code', /'unknown-fields':/.test(BANNER));
    check('and for that code shows the sentence with the field name instead of the generic reason',
      /h\.reason === 'unknown-fields' && h\.lastError \? h\.lastError : 'Because '/.test(BANNER));
    runSection3();
  })();
}

/* ── 3. The credential store says when its cloud half is broken ──────────── */
function runSection3() {
  section('3. A 503 from the secrets store is a sentence on the page, not a console line');
  const store = new Map();
  global.localStorage = { getItem: (k) => (store.has(k) ? store.get(k) : null), setItem: (k, v) => store.set(k, String(v)), removeItem: (k) => store.delete(k) };
  global.sessionStorage = global.localStorage;
  const events = [];
  global.window = global;
  global.document = { readyState: 'complete', addEventListener: () => {} };
  global.CustomEvent = class { constructor(type, init) { this.type = type; this.detail = init && init.detail; } };
  global.dispatchEvent = (e) => events.push(e);
  global.addEventListener = () => {};
  global.atob = (s) => Buffer.from(s, 'base64').toString('binary');
  const b64 = (o) => Buffer.from(JSON.stringify(o)).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  let TOKEN = 'h.' + b64({ oid: 'oid-1', email: 'a@b.test' }) + '.s';
  global.getCygenixIdToken = () => TOKEN;
  let mode = '503';
  const calls = [];
  global.CygenixDataApi = {
    isSignedIn: () => !!TOKEN,
    callResult: async (action, opts) => {
      calls.push(action + (opts && opts.query ? '?' + JSON.stringify(opts.query) : ''));
      if (!TOKEN) return { ok: false, error: { code: 'no-token' } };
      if (action === 'blob-credential') {
        if (mode === 'no-fn-key') return { ok: false, error: { code: 'config', status: 503, serverCode: 'no-fn-key' } };
        return { ok: true, data: { base: 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/data', code: 'HOSTKEY123' } };
      }
      if (action === 'secrets-list') {
        if (mode === '503') return { ok: false, error: { code: 'server', status: 503, serverCode: 'no-secrets-key',
          detail: '{"error":"secrets store not configured","code":"no-secrets-key"}', message: 'data-proxy secrets-list returned 503' } };
        if (mode === 'net') return { ok: false, error: { code: 'network', status: 0 } };
        return { ok: true, data: { secrets: {}, undecryptable: [] } };
      }
      if (action === 'secrets-put') return mode === '503'
        ? { ok: false, error: { code: 'server', status: 503, serverCode: 'no-secrets-key' } }
        : { ok: true, data: { ok: true } };
      return { ok: true, data: {} };
    },
  };
  require(path.join(ROOT, 'public', 'cygenix-saved-conn-secrets.js'));
  const S = global.window.CygenixSavedConnSecrets;
  require(path.join(ROOT, 'public', 'connections.js'));
  const C = global.CygenixConnections;
  localStorage.setItem('cygenix_user', JSON.stringify({ email: 'you@example.test' }));

  (async () => {
    check('cloudStatus() is exported and starts unknown', typeof S.cloudStatus === 'function' && S.cloudStatus().state === 'unknown');
    events.length = 0;
    const r = await S.sync({ force: true });
    const cs = S.cloudStatus();
    check('a 503 no-secrets-key makes the store UNAVAILABLE, with the setting NAMED in the message',
      cs.state === 'unavailable' && cs.code === 'no-secrets-key' && /CONN_SECRETS_KEY/.test(cs.message) && /Function App/.test(cs.message), JSON.stringify(cs));
    check('and the sentence says what it means for the person — kept on this device only', /this device/.test(cs.message));
    check('sync() reports the same', r.ok === false && r.serverCode === 'no-secrets-key' && /CONN_SECRETS_KEY/.test(r.message), JSON.stringify(r));
    const ev = events.find((e) => e.type === 'cygenix:conn-secrets-error');
    check('THE FAILURE IS ANNOUNCED on its own event, carrying the code and the sentence',
      !!ev && ev.detail.ok === false && ev.detail.code === 'no-secrets-key' && /CONN_SECRETS_KEY/.test(ev.detail.message), JSON.stringify(ev && ev.detail));
    check('and NOT on the synced event — nothing arrived, so no page refills a form somebody may be typing in',
      !events.some((e) => e.type === 'cygenix:conn-secrets-synced'));
    const APP2 = code(read('public', 'dashboard-app.js'));
    check('the Connections view listens to the error event and only re-renders the status box, never the form',
      /addEventListener\('cygenix:conn-secrets-error', function \(\) \{[\s\S]{0,300}connCloudStatus\(\);/.test(APP2)
      && !/addEventListener\('cygenix:conn-secrets-error', refillIfShowing/.test(APP2));
    check('no credential was touched by the failure', store.get('cygenix_saved_conn_secrets') == null || store.get('cygenix_saved_conn_secrets') === '{}');

    S._reset(); events.length = 0; mode = 'net';
    await S.sync({ force: true });
    check('a transport failure is "error" with a plain sentence', S.cloudStatus().state === 'error' && /Could not reach/.test(S.cloudStatus().message), JSON.stringify(S.cloudStatus()));

    S._reset(); events.length = 0; mode = 'ok';
    await S.sync({ force: true });
    check('a successful list is "ok" and the synced event says so', S.cloudStatus().state === 'ok' && events.some((e) => e.type === 'cygenix:conn-secrets-synced' && e.detail.ok === true));

    S._reset(); events.length = 0; TOKEN = '';
    await S.sync({ force: true });
    check('signed out is "signed-out" — not a fault, and no event', S.cloudStatus().state === 'signed-out' && events.length === 0);
    TOKEN = 'h.' + b64({ oid: 'oid-1', email: 'a@b.test' }) + '.s';

    // A page that only ever writes still learns.
    S._reset(); mode = '503';
    S.set('sconn_x', { connString: 'Server=a;Password=b;' });
    await S._flushNow();
    check('a put refused for configuration records the same fact', S.cloudStatus().state === 'unavailable' && S.cloudStatus().code === 'no-secrets-key');
    mode = 'ok';
    runSection4(C, S, calls, events, () => mode, (m) => { mode = m; });
  })();
}

/* ── 4. The product's own Function App key is fetched, not typed ─────────── */
function runSection4(C, S, calls, events, getMode, setMode) {
  section('4. The Function App key — fetched for the product\'s host, guarded, never for another');
  const KEY = 'cygenix_project_connections';
  const PRODUCT = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/db';
  const blob = () => JSON.parse(localStorage.getItem(KEY) || '{}')['you@example.test'] || {};
  (async () => {
    check('ensureFnKeys and fnKeyStatus are on the API', typeof C.ensureFnKeys === 'function' && typeof C.fnKeyStatus === 'function');

    // (a) nothing needs a key: no request.
    C.setActive({ srcConnMode: 'direct', srcConnString: 'Server=a;Database=x;', tgtConnMode: 'direct', tgtConnString: 'Server=b;' });
    calls.length = 0;
    let r = await C.ensureFnKeys({ force: true });
    check('when no side needs a key, NO request is made', r.code === 'not-needed' && !calls.some((c) => /blob-credential/.test(c)), JSON.stringify(calls));

    // (b) target on the product's host, no key.
    C.setActive({ srcConnMode: 'direct', srcConnString: 'Server=a;Database=x;', tgtConnMode: 'azure', tgtFnUrl: PRODUCT, tgtFnKey: '' });
    check('(setup) the target is azure-mode on the product host with no key', blob().tgtFnUrl === PRODUCT && !blob().tgtFnKey);
    check('(setup) tgtConn therefore has no ?code= — the 401 case', !/code=/.test(C.tgtConn));
    calls.length = 0; events.length = 0;
    r = await C.ensureFnKeys({ force: true });
    check('ONE blob-credential request is made', calls.filter((c) => /blob-credential/.test(c)).length === 1, JSON.stringify(calls));
    check('the key is written into the live pair', r.ok === true && r.filled.join() === 'tgt' && blob().tgtFnKey === 'HOSTKEY123', JSON.stringify(blob()));
    check('and tgtConn now carries ?code= — the direct call to /api/db will authenticate', /\?code=HOSTKEY123$/.test(C.tgtConn), C.tgtConn);
    check('the source, which needed nothing, is untouched', blob().srcConnString === 'Server=a;Database=x;' && !blob().srcFnKey);
    check('the key is mirrored into the credential store under the live id, so it survives like a typed one',
      S.get('sconn_live_tgt') && S.get('sconn_live_tgt').fnKey === 'HOSTKEY123', JSON.stringify(S.get('sconn_live_tgt')));
    check('the Connections view is told to refill (connections-applied), and the fill is attributed',
      events.some((e) => e.type === 'cygenix:connections-applied' && e.detail && e.detail.source === 'fn-credential'));
    check('fnKeyStatus() reads ok', C.fnKeyStatus().state === 'ok' && C.fnKeyStatus().filled.join() === 'tgt');
    check('a second call finds nothing to do and makes no request',
      (await C.ensureFnKeys({ force: true })).code === 'not-needed' && calls.filter((c) => /blob-credential/.test(c)).length === 1);

    // (c) a customer's own Function App: not ours to fill.
    C.setActive({ srcConnMode: 'azure', srcFnUrl: 'https://customer-fn.azurewebsites.net/api/db', srcFnKey: '', tgtConnMode: 'direct', tgtConnString: 'Server=b;' });
    calls.length = 0;
    r = await C.ensureFnKeys({ force: true });
    check('ANOTHER HOST IS LEFT ALONE — no request, no key', r.code === 'not-needed' && !blob().srcFnKey && calls.length === 0, JSON.stringify({ r, blob: blob(), calls }));

    // (d) guards: in-flight and the 3-second interval.
    C.setActive({ srcConnMode: 'direct', srcConnString: 'Server=a;', tgtConnMode: 'azure', tgtFnUrl: PRODUCT, tgtFnKey: '' });
    calls.length = 0;
    const p1 = C.ensureFnKeys({ force: true }), p2 = C.ensureFnKeys({ force: true }), p3 = C.ensureFnKeys();
    const [a, b, c] = await Promise.all([p1, p2, p3]);
    check('three overlapping calls share ONE request', calls.filter((x) => /blob-credential/.test(x)).length === 1 && a.ok && b.ok && c.ok, JSON.stringify(calls));
    // strip the key again and ask without force inside the window
    C.setActive({ srcConnMode: 'direct', srcConnString: 'Server=a;', tgtConnMode: 'azure', tgtFnUrl: PRODUCT, tgtFnKey: '' });
    calls.length = 0;
    r = await C.ensureFnKeys();
    check('a call inside the 3-second window without force is throttled, not sent', r.code === 'throttled' && calls.length === 0, JSON.stringify(r));

    // (e) failure is a sentence that names the setting.
    setMode('no-fn-key');
    r = await C.ensureFnKeys({ force: true });
    const fs2 = C.fnKeyStatus();
    check('when the proxy has no key, the status names CYGENIX_DATA_FN_KEY and the blob is untouched',
      r.ok === false && fs2.state === 'error' && /CYGENIX_DATA_FN_KEY/.test(fs2.message) && !blob().tgtFnKey, JSON.stringify(fs2));
    setMode('ok');

    const CS = code(read('public', 'connections.js'));
    check('the interval stamp is taken BEFORE the request, and the status is written by the outcome only — nothing in the callback gates a retry',
      /_fnCredLastAt = Date\.now\(\);\s*\n\s*_fnCredInflight = \(async/.test(CS) && !/_fnCredLastAt = 0/.test(CS.split('function ensureFnKeys')[1] || ''));
    check('the in-flight promise is cleared in finally, by the function that set it', /finally \{\s*\n\s*_fnCredInflight = null;/.test(CS));
    check('the triggers are the two "something arrived" events plus page load, and none of them is an event ensureFnKeys dispatches',
      /addEventListener\('cygenix-sync-loaded'/.test(CS) && /addEventListener\('cygenix:conn-secrets-synced'/.test(CS)
      && !/addEventListener\('cygenix:connections-applied'/.test(CS));

    // (f) the legacy save() misfiling, pinned at the writer.
    sessionStorage.setItem('cygenix_fn_url', 'mssql://u:p@h:1433/db');
    sessionStorage.setItem('cygenix_conn_mode', 'azure');
    C.save();
    const after = C.get();
    check('a connection string filed under cygenix_fn_url with mode azure lands in tgtConnString, mode direct — the writer fixes the shape',
      after.tgtConnString === 'mssql://u:p@h:1433/db' && after.tgtConnMode === 'direct' && after.tgtFnUrl === '', JSON.stringify(after));
    check('and the flat keys are cleared afterwards', sessionStorage.getItem('cygenix_fn_url') === null);

    runSection5();
  })();
}

/* ── 5. The pages say it, and the deploy note exists ─────────────────────── */
function runSection5() {
  section('5. Where it is said');
  const DASH = read('public', 'dashboard.html');
  const APP = code(read('public', 'dashboard-app.js'));
  const PROF = read('public', 'profiles.html');
  const OM = code(read('public', 'object-mapping-app.js'));
  check('the Connections view has a red box for credential plumbing, an alert, hidden while fine',
    /id="conn-cloud-status" class="cx-attn cx-attn-fail[^"]*" style="display:none" role="alert"/.test(DASH));
  check('it is rendered on every view init from the two modules\' state', /connRenderEnv\(\);\s*\n\s*connCloudStatus\(\);/.test(APP) && /function connCloudStatus\(\)\{/.test(APP));
  check('the box names the store failure and the key failure separately', /cloudStatus\(\)/.test(APP.split('function connCloudStatus')[1]) && /fnKeyStatus\(\)/.test(APP.split('function connCloudStatus')[1]));
  check('Test on an azure side with no key fetches the key first, with force — the one user action that may retry',
    /C\.ensureFnKeys\(\{ force: true \}\)/.test(APP.split('async function testProjConn')[1]));
  check('and when no key can be had, Test says so in red and does not send a request that can only 401',
    /No function key for this Function App URL\./.test(APP));
  check('the Profiles page has the same line', /id="cp-cloud-status"/.test(PROF) && /function renderCloudStatus\(\)/.test(code(PROF)) && /renderCloudStatus\(\);/.test(code(PROF)));
  check('Object Mapping turns a 401 from a Function App URL into instructions, not "Non-JSON (401)"',
    /res\.status===401 && isFn\(conn\)/.test(OM) && /this connection has no function key/.test(OM));
  const DOC = read('docs', 'DEPLOY-conn-secrets.md');
  check('the deploy note exists, names the variable, the 503 body and the generate command',
    /CONN_SECRETS_KEY/.test(DOC) && /no-secrets-key/.test(DOC) && /randomBytes\(32\)/.test(DOC));
  check('nothing hands a Function App key to a page that never had one — the fetch only fills a side that already points at the product host',
    /return !!url && !key && hostOf\(url\) === \(host \|\| productHost\(\)\)/.test(code(read('public', 'connections.js'))));
  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
}
