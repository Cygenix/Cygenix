// tests/cloud-sync-resilience.test.js
//
// WHAT WENT WRONG
//
// CYGENIX_DATA_FN_KEY was missing from the Netlify deployment for about three
// weeks. Every call to /.netlify/functions/data-proxy returned 503. The data
// layer collapsed that into `null`, which was indistinguishable from "this
// account has nothing stored", and the only report anywhere was a
// console.warn. Saves silently did nothing; a user could work all day and
// keep none of it.
//
// The handover that described this said the null was read as "Cosmos is
// empty" and overwrote local state. That is not what init() did — a null
// response skipped the apply block entirely and local survived. The real
// failure was quieter and worse: saves no-opped with no retry, no queue and
// no surface, and any second device showed an empty account.
//
// There WAS a live null-shaped data-loss path, in a different place: init()
// deleted the local copy of every FIELD_MAP key the cloud response did not
// carry, guarded only against a response with no fields at all. A partial
// response deleted local data for every field it omitted — and FIELD_MAP's
// own comment says two fields may not round-trip until the backend schema
// catches up.
//
// These tests run the real modules against a scripted data layer, because
// every one of these bugs is about what the code DOES with a failure, and a
// grep cannot see that.
'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const PUB = path.join(__dirname, '..', 'public');
const SYNC_SRC = fs.readFileSync(path.join(PUB, 'cygenix-cosmos-sync.js'), 'utf8');
const API_SRC = fs.readFileSync(path.join(PUB, 'cygenix-data-api.js'), 'utf8');

/* ── A browser, near enough ───────────────────────────────────────────────── */

// A token whose claims resolve to an identity, so getUserId() succeeds and
// the module does more than retry.
function jwt(claims) {
  const b64 = (o) => Buffer.from(JSON.stringify(o)).toString('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  return b64({ alg: 'none' }) + '.' + b64(claims) + '.sig';
}
const TOKEN = jwt({ preferred_username: 'someone@example.com' });

function makeStorage(seed) {
  const data = Object.assign({}, seed || {});
  const store = {};
  Object.defineProperties(store, {
    getItem: { value: (k) => (Object.prototype.hasOwnProperty.call(data, k) ? data[k] : null) },
    setItem: { value: (k, v) => { data[k] = String(v); }, writable: true },
    removeItem: { value: (k) => { delete data[k]; } },
    clear: { value: () => { Object.keys(data).forEach((k) => delete data[k]); } },
    _data: { value: data },
  });
  return store;
}

/**
 * Boot cygenix-cosmos-sync.js against a scripted data layer.
 *
 * @param opts.local     seed localStorage
 * @param opts.responder (action, method, body) => result, where result is
 *                       { ok:true, data } or { ok:false, error:{code,…} }
 */
function boot(opts) {
  const o = opts || {};
  const localStorage = makeStorage(Object.assign({ cygenix_token: TOKEN }, o.local || {}));
  const sessionStorage = makeStorage({});
  const calls = [];
  const events = [];
  const timers = [];

  const window = {
    addEventListener() {},
    dispatchEvent(e) { events.push({ type: e.type, detail: e.detail }); return true; },
    CygenixDataApi: {
      async callResult(action, opts2) {
        calls.push({ action, method: (opts2 && opts2.method) || 'GET', body: opts2 && opts2.body });
        const r = (o.responder || (() => ({ ok: true, data: {} })))(action, (opts2 && opts2.method) || 'GET', opts2 && opts2.body);
        return r;
      },
    },
  };

  const ctx = {
    window, localStorage, sessionStorage, console: quietConsole(),
    CustomEvent: function (type, init) { this.type = type; this.detail = init && init.detail; },
    // Timers are captured rather than run: the retry schedule is asserted on
    // directly, and a real setTimeout would leak across tests.
    setTimeout: (fn, ms) => { timers.push({ fn, ms }); return timers.length; },
    clearTimeout: () => {},
    document: { createElement: () => ({ style: {}, setAttribute() {}, appendChild() {} }), body: { appendChild() {}, removeChild() {} } },
    atob: (s) => Buffer.from(s, 'base64').toString('binary'),
  };
  ctx.globalThis = ctx;
  vm.createContext(ctx);
  vm.runInContext(SYNC_SRC, ctx, { filename: 'cygenix-cosmos-sync.js' });
  return { sync: window.CygenixSync, localStorage, calls, events, timers, window };
}

function quietConsole() {
  const noop = () => {};
  return { log: noop, warn: noop, error: noop, info: noop };
}

const settle = () => new Promise((r) => setImmediate(r));

const FAIL_503 = { ok: false, error: { code: 'config', retryable: false, message: 'data-proxy load returned 503' } };

console.log('Cloud sync resilience — a broken data layer must be loud, contained and non-destructive\n');

(async function run() {

  /* ── 1. An unverified response never touches local data ─────────────────── */

  {
    const seeded = { cygenix_jobs: '[{"id":"j1"}]', cygenix_sys_params: '{"a":1}' };
    const t = boot({ local: seeded, responder: () => FAIL_503 });
    await settle(); await settle(); await settle();

    check('a 503 on load leaves every local key exactly as it was',
      t.localStorage.getItem('cygenix_jobs') === seeded.cygenix_jobs
      && t.localStorage.getItem('cygenix_sys_params') === seeded.cygenix_sys_params,
      JSON.stringify(t.localStorage._data));

    const h = t.sync.getHealth();
    check('and the health state says it is degraded, with the reason', h.degraded === true && h.reason === 'config', JSON.stringify(h));
    check('and does NOT claim the cloud was verified', h.verified === false, JSON.stringify(h));
    check('the reason is machine-readable, so a banner can explain it in the user\'s terms',
      ['config', 'auth', 'network', 'server', 'rejected', 'suspect-empty', 'no-data-api'].includes(h.reason));
  }

  /* ── 2. An omitted field is not a deletion ──────────────────────────────── */
  //
  // This is the live data-loss path the handover missed. init() used to call
  // removeItem() for every FIELD_MAP key absent from the response.

  {
    const t = boot({
      local: {
        cygenix_jobs: '[{"id":"j1"}]',
        cygenix_conv_project: '{"id":"p1","name":"Acme"}',
        cygenix_last_snapshots: '{"s":1}',
      },
      // A partial response: jobs come back, the two fields FIELD_MAP warns
      // may not round-trip do not.
      responder: (a) => (a === 'load' ? { ok: true, data: { jobs: [{ id: 'j1' }, { id: 'j2' }] } } : { ok: true, data: { saved: true } }),
    });
    await settle(); await settle(); await settle();

    check('a field the cloud omits keeps its local value',
      t.localStorage.getItem('cygenix_conv_project') === '{"id":"p1","name":"Acme"}',
      'got ' + t.localStorage.getItem('cygenix_conv_project'));
    check('including the second field FIELD_MAP warns may not round-trip',
      t.localStorage.getItem('cygenix_last_snapshots') === '{"s":1}',
      'got ' + t.localStorage.getItem('cygenix_last_snapshots'));
    check('while a field the cloud DOES carry is applied',
      t.localStorage.getItem('cygenix_jobs') === JSON.stringify([{ id: 'j1' }, { id: 'j2' }]),
      'got ' + t.localStorage.getItem('cygenix_jobs'));
  }

  /* ── 3. A deletion still propagates ─────────────────────────────────────── */
  //
  // The fix above must not break the thing the delete existed for. save()
  // serialises what is in localStorage, so emptying a list sends [] — a
  // PRESENT empty value, which is how a real deletion crosses machines.

  {
    const t = boot({
      local: { cygenix_jobs: '[{"id":"j1"}]', cygenix_sys_params: '{"a":1}' },
      responder: (a) => (a === 'load' ? { ok: true, data: { jobs: [], sys_params: { a: 1 } } } : { ok: true, data: { saved: true } }),
    });
    await settle(); await settle(); await settle();

    check('an explicitly empty value from the cloud DOES clear the local key',
      t.localStorage.getItem('cygenix_jobs') === '[]',
      'a deletion made on another machine must still arrive; got ' + t.localStorage.getItem('cygenix_jobs'));
  }

  /* ── 4. Nothing empties everything at once ──────────────────────────────── */

  {
    const t = boot({
      local: { cygenix_jobs: '[{"id":"j1"}]', cygenix_sys_params: '{"a":1}', cygenix_projects: '[{"id":"p"}]' },
      responder: (a) => (a === 'load'
        ? { ok: true, data: { jobs: [], sys_params: {}, projects: [] } }
        : { ok: true, data: { saved: true } }),
    });
    await settle(); await settle(); await settle();

    check('a response that would empty every populated key at once is refused',
      t.localStorage.getItem('cygenix_jobs') === '[{"id":"j1"}]'
      && t.localStorage.getItem('cygenix_projects') === '[{"id":"p"}]',
      'fifteen simultaneous deletions is a backend fault, not a user action');
    check('and that refusal is reported rather than passed off as success',
      t.sync.getHealth().reason === 'suspect-empty', JSON.stringify(t.sync.getHealth()));
  }

  /* ── 5. A verified-empty account keeps the only copy there is ───────────── */

  {
    const t = boot({
      local: { cygenix_jobs: '[{"id":"local-only"}]' },
      responder: (a) => (a === 'load' ? { ok: true, data: {} } : { ok: true, data: { saved: true } }),
    });
    await settle(); await settle(); await settle();

    check('a verified-but-empty cloud does not clear a first sign-in\'s local data',
      t.localStorage.getItem('cygenix_jobs') === '[{"id":"local-only"}]');
    check('but it IS recorded as verified — it is a real answer, not a failure',
      t.sync.getHealth().verified === true && t.sync.getHealth().degraded === false,
      JSON.stringify(t.sync.getHealth()));
  }

  /* ── 6. resetToCloud verifies before it wipes ───────────────────────────── */
  //
  // It used to remove every sync key and THEN call the API, returning
  // {local_now_empty: true} when the load failed: a recovery helper that
  // destroyed the last copy of the data whenever the thing being recovered
  // from was still broken.

  {
    const t = boot({ local: { cygenix_jobs: '[{"id":"j1"}]' }, responder: () => FAIL_503 });
    await settle(); await settle();
    const r = await t.sync.resetToCloud();

    check('resetToCloud on a failing load leaves local intact',
      t.localStorage.getItem('cygenix_jobs') === '[{"id":"j1"}]',
      JSON.stringify(t.localStorage._data));
    check('and reports that it did not wipe, rather than that it did',
      r.ok === false && r.local_untouched === true && r.local_now_empty === undefined,
      JSON.stringify(r));
  }

  {
    const t = boot({
      local: { cygenix_jobs: '[{"id":"stale"}]' },
      responder: (a) => (a === 'load' ? { ok: true, data: { jobs: [{ id: 'fresh' }] } } : { ok: true, data: { saved: true } }),
    });
    await settle(); await settle();
    const r = await t.sync.resetToCloud();
    check('and on a good load it still does its job',
      r.ok === true && t.localStorage.getItem('cygenix_jobs') === JSON.stringify([{ id: 'fresh' }]),
      JSON.stringify(r));
  }

  {
    const t = boot({
      local: { cygenix_jobs: '[{"id":"j1"}]' },
      responder: (a) => (a === 'load' ? { ok: true, data: {} } : { ok: true, data: { saved: true } }),
    });
    await settle(); await settle();
    const r = await t.sync.resetToCloud();
    check('resetToCloud against a verified-EMPTY cloud also refuses to wipe',
      r.ok === false && r.error === 'cloud-empty'
      && t.localStorage.getItem('cygenix_jobs') === '[{"id":"j1"}]',
      'wiping local to match an empty cloud leaves no copy anywhere: ' + JSON.stringify(r));
  }

  /* ── 7. nuke reads before it overwrites ─────────────────────────────────── */

  {
    const t = boot({
      local: { cygenix_conv_project: '{"id":"p1","name":"Acme"}' },
      responder: (a) => (a === 'load' ? FAIL_503 : { ok: true, data: { saved: true } }),
    });
    await settle(); await settle();
    const r = await t.sync.nuke({ confirm: 'YES' });

    check('nuke refuses when it cannot read the cloud it is about to overwrite',
      r.ok === false && r.error === 'cloud-unreadable', JSON.stringify(r));
    check('and sends no save while refusing',
      !t.calls.some((c) => c.action === 'save'),
      t.calls.map((c) => c.action).join(','));
  }

  /* ── 8. A failed save is queued, counted and surfaced ───────────────────── */
  //
  // The actual three-week bug. save() returned null and nothing else
  // happened: no retry, no record, no surface.

  {
    let allow = false;
    const t = boot({
      local: {},
      responder: (a) => {
        if (a === 'load') return { ok: true, data: { jobs: [] } };
        if (a === 'save') return allow ? { ok: true, data: { saved: true, updatedAt: 'now' } } : FAIL_503;
        return { ok: true, data: {} };
      },
    });
    await settle(); await settle();

    t.localStorage.setItem('cygenix_jobs', '[{"id":"new"}]');   // a user edit
    check('an edit is counted as pending the moment it is made',
      t.sync.getHealth().pendingSaves === 1, JSON.stringify(t.sync.getHealth()));

    await t.sync.save();
    const h = t.sync.getHealth();
    check('a failed save keeps the change queued rather than dropping it',
      h.pendingSaves === 1, JSON.stringify(h));
    check('and marks the data layer degraded so something can say so', h.degraded === true, JSON.stringify(h));
    check('and schedules a retry rather than waiting for the user to type again',
      t.timers.some((x) => x.ms >= 1000), JSON.stringify(t.timers.map((x) => x.ms)));

    allow = true;
    await t.sync.retryPending();
    const h2 = t.sync.getHealth();
    check('when the deployment is fixed, the queued work goes up and the count clears',
      h2.pendingSaves === 0 && h2.degraded === false, JSON.stringify(h2));
  }

  /* ── 9. The public contract is unchanged ────────────────────────────────── */
  //
  // Roughly forty scripts call this module. Structured results were ADDED,
  // not swapped in for what was there.

  {
    const t = boot({
      local: {},
      responder: (a) => (a === 'load' ? { ok: true, data: { jobs: [{ id: 'j' }] } } : { ok: true, data: { saved: true } }),
    });
    await settle(); await settle();

    const l = await t.sync.load();
    const f = await t.sync.forceLoad();
    check('load() still returns a boolean', typeof l === 'boolean', typeof l);
    check('forceLoad() still returns a boolean', typeof f === 'boolean', typeof f);
    ['init', 'save', 'saveNow', 'load', 'forceLoad', 'ensureKey', 'ensureUser', 'ping',
      'getSubscription', 'getUserId', 'exportBackup', 'importBackup', 'resetToCloud', 'nuke']
      .forEach((fn) => check('CygenixSync.' + fn + ' is still exported', typeof t.sync[fn] === 'function'));
    ['loadDetailed', 'getHealth', 'retryPending']
      .forEach((fn) => check('CygenixSync.' + fn + ' was added alongside', typeof t.sync[fn] === 'function'));

    const d = await t.sync.loadDetailed({ force: true });
    check('loadDetailed() gives the structured answer load() cannot',
      d.ok === true && d.verified === true && typeof d.applied === 'number', JSON.stringify(d));
  }

  {
    const t = boot({ local: {}, responder: () => FAIL_503 });
    await settle(); await settle();
    const d = await t.sync.loadDetailed({ force: true });
    check('and on failure carries the code rather than a bare false',
      d.ok === false && d.code === 'config', JSON.stringify(d));
  }

  /* ── 10. The data layer tells failures apart ────────────────────────────── */

  // The module attaches itself to `window`, so give it one rather than
  // unpicking the IIFE — the thing under test should be the shipped file.
  function bootApi(opts) {
    const o = opts || {};
    const responses = (o.responses || []).slice();
    const win = { getCygenixIdToken: () => (o.token === undefined ? 'tok' : o.token), console: quietConsole() };
    const ctx = {
      window: win, console: quietConsole(),
      fetch: async () => {
        const next = responses.shift();
        if (next instanceof Error) throw next;
        return next;
      },
    };
    ctx.globalThis = ctx;
    vm.createContext(ctx);
    vm.runInContext(API_SRC, ctx, { filename: 'cygenix-data-api.js' });
    return win.CygenixDataApi;
  }

  {
    const API = bootApi({
      responses: [
        { ok: false, status: 503, text: async () => JSON.stringify({ error: 'x', code: 'no-fn-key' }) },
        { ok: true, status: 200, json: async () => null },
        new Error('network down'),
      ],
    });

    check('classify: a 503 is a configuration fault, not an empty account',
      API.classify(503, '').code === 'config' && API.classify(503, '').retryable === false);
    check('classify: the proxy\'s own no-fn-key code wins over guessing from status',
      API.classify(200, 'no-fn-key').code === 'config');
    check('classify: 401 and 403 are auth, and retrying will not help',
      API.classify(401, '').code === 'auth' && API.classify(403, '').retryable === false);
    check('classify: a request that never completed is network, and retryable',
      API.classify(0, '').code === 'network' && API.classify(0, '').retryable === true);
    check('classify: a 500 is the server\'s problem, and worth retrying',
      API.classify(500, '').code === 'server' && API.classify(500, '').retryable === true);
    check('classify: a 400 is our bug, and is not retryable',
      API.classify(400, '').code === 'client' && API.classify(400, '').retryable === false);

    const r1 = await API.callResult('load');
    check('callResult reports a 503 as not-ok with the config code',
      r1.ok === false && r1.error.code === 'config' && r1.error.serverCode === 'no-fn-key', JSON.stringify(r1));

    const r2 = await API.callResult('load');
    check('and reports a verified empty answer as OK with null data — a fact, not a failure',
      r2.ok === true && r2.data === null, JSON.stringify(r2));

    const r3 = await API.callResult('load');
    check('a fetch that never completed is network, and never throws out of callResult',
      r3.ok === false && r3.error.code === 'network' && r3.error.retryable === true, JSON.stringify(r3));

    const noTok = bootApi({ token: '' });
    const r4 = await noTok.callResult('load');
    check('a signed-out page is no-token, which is not a fault to report',
      r4.ok === false && r4.error.code === 'no-token', JSON.stringify(r4));

    // callOrNull is deliberately still fail-open: three call sites (the tier
    // gate, the user pill, the billing portal) must not lock a paying user
    // out when a billing lookup 500s. It was left alone and callResult added
    // beside it, rather than made to throw into those.
    const openApi = bootApi({ responses: [{ ok: false, status: 500, text: async () => '{}' }] });
    const opened = await openApi.callOrNull('subscription');
    check('callOrNull stays fail-open, so a 500 does not lock anyone out',
      opened === null, String(opened));
  }

  /* ── 11. The deployment cannot ship broken quietly ──────────────────────── */

  {
    const proxy = fs.readFileSync(path.join(__dirname, '..', 'netlify', 'functions', 'data-proxy.js'), 'utf8');
    check('every proxy error carries a machine-readable code',
      !/return fail\([^;]*?,\s*\d+\s*\)/.test(proxy),
      'a client cannot branch on prose');
    check('the missing-key case has the code the client classifies as config',
      /'no-fn-key'/.test(proxy) && /503, 'no-fn-key'/.test(proxy));
    check('a 503 is logged at error level, not swallowed',
      /console\.error\('\[data-proxy\] CYGENIX_DATA_FN_KEY is not set/.test(proxy),
      'three weeks of 503s left no log entry anybody saw');
    check('there is an unauthenticated health action',
      /action === 'health'/.test(proxy));
    check('and it answers BEFORE the key check and the token check',
      // The CALL site, not the require at the top of the file.
      proxy.indexOf("action === 'health'") < proxy.indexOf('if (!FN_KEY)')
      && proxy.indexOf("action === 'health'") < proxy.indexOf('await verifyAuthHeader('),
      'a health probe that needs the thing it is probing is not a probe');
    check('health reports the presence of configuration and never its content',
      /apiBaseConfigured: !!process\.env\.CYGENIX_DATA_API_BASE/.test(proxy)
      && !/FN_KEY\.(slice|substring|length)/.test(proxy),
      'not a fragment, not a length');

    const env = fs.readFileSync(path.join(__dirname, '..', 'scripts', 'check-env.js'), 'utf8');
    check('CYGENIX_DATA_FN_KEY is the one required variable',
      /name: 'CYGENIX_DATA_FN_KEY'/.test(env) && /REQUIRED/.test(env));
    // Behaviour, not text: run it the way Netlify does. It must WARN and let
    // the deploy through by default — the first version refused, and blocked
    // production for two days when the variable was set in a scope the build
    // could not see — and refuse only when the hard gate is asked for.
    const { spawnSync } = require('child_process');
    const runEnv = (extra) => spawnSync(process.execPath, [path.join(__dirname, '..', 'scripts', 'check-env.js')],
      { env: Object.assign({}, process.env, { CYGENIX_DATA_FN_KEY: '', CYGENIX_ENFORCE_ENV: '' }, extra), encoding: 'utf8' });
    const asNetlify = runEnv({ NETLIFY: 'true' });
    check('in a Netlify build with the key missing it warns and still exits 0',
      asNetlify.status === 0 && /WARNING — deploying anyway/.test(asNetlify.stderr), 'exit ' + asNetlify.status);
    check('and the warning says where and in which SCOPE to set it',
      /SCOPE MATTERS/.test(asNetlify.stderr) && /Builds/.test(asNetlify.stderr));
    const enforced = runEnv({ NETLIFY: 'true', CYGENIX_ENFORCE_ENV: 'true' });
    check('with CYGENIX_ENFORCE_ENV=true it refuses, for operators who want the hard gate',
      enforced.status === 1 && /REFUSING TO BUILD/.test(enforced.stderr), 'exit ' + enforced.status);
    // The marker is deliberately not a real word: the script's own success
    // line says "variables are present", and a marker of 'present' matched it.
    const MARKER = 'zq9-secret-marker-7f3';
    const withKey = runEnv({ NETLIFY: 'true', CYGENIX_DATA_FN_KEY: MARKER });
    check('and with the key present it is quiet and exits 0',
      withKey.status === 0 && !/WARNING|REFUSING/.test(withKey.stderr)
      && (withKey.stdout + withKey.stderr).indexOf(MARKER) === -1,
      'the value must never be echoed');
    // Read the REQUIRED array literal itself. The prose above it discusses
    // both variables by name, so searching the whole file finds the
    // explanation and calls it a declaration.
    const requiredBlock = env.slice(env.indexOf('const REQUIRED = ['), env.indexOf('const OPTIONAL = ['));
    check('but does NOT require CYGENIX_DATA_API_BASE, which has a working fallback',
      !/CYGENIX_DATA_API_BASE/.test(requiredBlock)
      && /CYGENIX_DATA_API_BASE/.test(env.slice(env.indexOf('const OPTIONAL = ['))),
      'requiring it would have failed the very deploy carrying the fix');
    check('and it prints no value, no fragment and no length',
      !/process\.env\[[^\]]+\]\.(slice|substring|length)/.test(env)
      && !/console\.log\([^)]*process\.env\[[^\]]*\]\s*\)/.test(env),
      'a build log is not a private place');

    const toml = fs.readFileSync(path.join(__dirname, '..', 'netlify.toml'), 'utf8');
    check('the check runs first in the build, before anything else can succeed',
      /check-env\.js/.test(toml)
      && toml.indexOf('check-env.js') < toml.indexOf('stamp-assets.js'));
  }

  /* ── 12. The failure has somewhere to appear ────────────────────────────── */

  {
    const banner = fs.readFileSync(path.join(PUB, 'cygenix-sync-banner.js'), 'utf8');
    check('a banner listens for the health event',
      /addEventListener\('cygenix-sync-health'/.test(banner));
    check('it offers the download first — this device may hold the only copy',
      banner.indexOf('Download my data') < banner.indexOf('Retry now'));
    check('it explains a config fault as something an administrator must fix',
      /administrator needs to set it/.test(banner),
      'telling a user to retry something only an operator can fix wastes their time');
    check('it does not block the page',
      !/position:fixed;[^"]*top:0[^"]*bottom:0/.test(banner) && !/preventDefault/.test(banner),
      'a data layer that is down does not stop work; it stops work being KEPT');

    // A page that LOADS the sync module, not one that merely mentions it in a
    // comment — insights.html talks about cygenix-cosmos-sync.js in prose but
    // never loads it, so it has no CygenixSync to report health from and the
    // banner would be dead weight there.
    const pages = fs.readdirSync(PUB).filter((f) => f.endsWith('.html'));
    const loadsSync = (f) => /<script[^>]*src="\/cygenix-cosmos-sync\.js/.test(fs.readFileSync(path.join(PUB, f), 'utf8'));
    const withSync = pages.filter(loadsSync);
    const withoutBanner = withSync.filter((f) => !/cygenix-sync-banner/.test(fs.readFileSync(path.join(PUB, f), 'utf8')));
    check('every page that syncs can show the banner (' + withSync.length + ' pages)',
      withSync.length > 0 && withoutBanner.length === 0, withoutBanner.join(', '));
  }

  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})();
