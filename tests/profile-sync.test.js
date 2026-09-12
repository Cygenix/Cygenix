// tests/profile-sync.test.js — connection profiles follow the account.
//
// WHAT WAS WRONG
// cygenix_profiles_v1 lived in localStorage and nowhere else. A profile made
// on one machine did not exist on the next, cleared site data erased the
// register, and nothing else in the product could rely on "the active
// profile" because there was no account-level fact to rely on.
//
// WHAT THIS PINS
// The key is synced — but it is the one synced field that is MERGED, not
// replaced, on both ends of the wire. The sync layer's contract for every
// other field ("cloud wins on load, local wins on save") would lose a
// profile created on the laptop while the desktop tab was open, and would
// wipe a pre-sync browser's register the first time the page loaded. The
// brief named both. So:
//
//   1. the merge itself is pure, commutative, idempotent, and never throws;
//   2. the same bytes run in the browser and in the Function App;
//   3. on init the browser applies the UNION and uploads whatever the cloud
//      lacked; on save the server unions again, so two machines cannot undo
//      each other;
//   4. every record that can be written on two machines carries the time it
//      was written, because that is what the merge decides by.
//
// The sync scenarios run the real cygenix-cosmos-sync.js against a scripted
// data layer, the way tests/cloud-sync-resilience.test.js does, because a
// grep cannot see what init() does with a response.
'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

const ROOT = path.join(__dirname, '..');
const PUB = path.join(ROOT, 'public');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

const MERGE_SRC = read('public', 'cygenix-profile-merge.js');
const SYNC_SRC = read('public', 'cygenix-cosmos-sync.js');
const M = require('../public/cygenix-profile-merge.js');
const P = require('../public/cygenix-profiles.js');

console.log('Connection profiles — persistence against the account\n');

/* ── 1. One implementation, shipped twice ─────────────────────────────────── */
section('1. The browser and the Function App run the same merge');

check('azure-function/src/profile-merge.js is byte-for-byte public/cygenix-profile-merge.js',
  read('azure-function', 'src', 'profile-merge.js') === MERGE_SRC,
  'edit one and copy it over the other — the deploy zips azure-function/ alone');
check('the server copy loads under Node and exposes the merge',
  typeof require('../azure-function/src/profile-merge.js').mergeProfileStores === 'function');

/* ── 2. The merge ─────────────────────────────────────────────────────────── */
section('2. Merge semantics — nothing is ever lost, the newer record wins');

const T0 = 1700000000000;
function store(over) {
  return Object.assign({
    v: 1, createdAt: T0, connMeta: {}, profiles: [], bindings: [], runRecords: [], events: [],
    settings: { envClasses: ['DEV', 'TEST', 'UAT', 'PRD', 'SANDBOX'], activeProfileId: null, selectedAt: 0 },
  }, over || {});
}
const prof = (id, status, t, extra) => Object.assign({ id, name: id, status, envClass: 'DEV',
  srcConnId: 's', tgtConnId: 't', createdAt: t, updatedAt: t }, extra || {});

// THE case from the brief: A on the laptop, B on the desktop.
{
  const laptop = store({ profiles: [prof('A', 'active', T0 + 1)] });
  const desktop = store({ profiles: [prof('B', 'active', T0 + 2)] });
  const m = M.mergeProfileStores(laptop, desktop);
  check('a profile created on each of two machines survives as two profiles',
    m.profiles.map((p) => p.id).sort().join(',') === 'A,B', JSON.stringify(m.profiles));
  check('the merge is commutative when the records differ',
    M.storesEqual(m, M.mergeProfileStores(desktop, laptop)));
  check('and idempotent — merging the result with either input changes nothing',
    M.storesEqual(m, M.mergeProfileStores(m, laptop)) && M.storesEqual(m, M.mergeProfileStores(desktop, m)));
}

// Same profile on both sides.
{
  const older = store({ profiles: [prof('A', 'active', T0 + 5, { name: 'old name' })] });
  const newer = store({ profiles: [prof('A', 'active', T0 + 9, { name: 'new name' })] });
  check('the same profile edited on both: the newer edit wins',
    M.mergeProfileStores(older, newer).profiles[0].name === 'new name'
    && M.mergeProfileStores(newer, older).profiles[0].name === 'new name');

  const retiredEarly = store({ profiles: [prof('A', 'retired', T0 + 1)] });
  const activeLater = store({ profiles: [prof('A', 'active', T0 + 50)] });
  check('a retirement is never undone by a machine that had not seen it — status only moves forward',
    M.mergeProfileStores(retiredEarly, activeLater).profiles[0].status === 'retired'
    && M.mergeProfileStores(activeLater, retiredEarly).profiles[0].status === 'retired');

  const draft = store({ profiles: [prof('A', 'draft', T0 + 99)] });
  const active = store({ profiles: [prof('A', 'active', T0 + 1)] });
  check('an activation is not undone by a newer draft either',
    M.mergeProfileStores(draft, active).profiles[0].status === 'active');

  const noStamp = store({ profiles: [Object.assign(prof('A', 'active', T0), { updatedAt: undefined, name: 'pre-sync build' })] });
  const stamped = store({ profiles: [prof('A', 'active', T0 + 1, { name: 'stamped' })] });
  check('a record from before timestamps existed counts as oldest',
    M.mergeProfileStores(noStamp, stamped).profiles[0].name === 'stamped'
    && M.mergeProfileStores(stamped, noStamp).profiles[0].name === 'stamped');
}

// Bindings, connection meta.
{
  const a = store({ bindings: [{ artifactType: 'job', artifactId: 'j1', profileId: 'A', boundAt: T0 + 1 }],
    connMeta: { c1: { envClass: 'DEV', updatedAt: T0 + 1 }, c2: { envClass: 'UAT' } } });
  const b = store({ bindings: [{ artifactType: 'job', artifactId: 'j1', profileId: 'B', boundAt: T0 + 2 },
                               { artifactType: 'script', artifactId: 's1', profileId: 'B', boundAt: T0 }],
    connMeta: { c1: { envClass: 'PRD', updatedAt: T0 + 2 }, c2: { envClass: 'PRD', updatedAt: T0 + 1 }, c3: { envClass: 'TEST' } } });
  const m = M.mergeProfileStores(a, b);
  check('a rebind made later wins, and a binding only one side has is kept',
    m.bindings.length === 2 && m.bindings.find((x) => x.artifactId === 'j1').profileId === 'B');
  check('connection classifications: the newer wins per connection, an unstamped one loses, a lone one is kept',
    m.connMeta.c1.envClass === 'PRD' && m.connMeta.c2.envClass === 'PRD' && m.connMeta.c3.envClass === 'TEST',
    JSON.stringify(m.connMeta));
}

// Run records and events: append-only, capped.
{
  const runs = (from, n) => Array.from({ length: n }, (_, i) => ({ runId: 'r' + (from + i), startedAt: T0 + from + i }));
  const a = store({ runRecords: runs(0, 250), events: [{ at: T0, type: 'x' }, { at: T0 + 1, type: 'y' }] });
  const b = store({ runRecords: runs(200, 250), events: [{ at: T0 + 1, type: 'y' }, { at: T0 + 2, type: 'z' }] });
  const m = M.mergeProfileStores(a, b);
  check('run records are unioned by id (450 distinct) then capped at ' + M.RUN_RECORD_CAP,
    m.runRecords.length === M.RUN_RECORD_CAP, m.runRecords.length);
  check('and the cap keeps the NEWEST, in time order',
    m.runRecords[0].runId === 'r150' && m.runRecords[m.runRecords.length - 1].runId === 'r449');
  check('events are unioned by content — a shared event appears once',
    m.events.length === 3 && m.events.map((e) => e.type).join('') === 'xyz', JSON.stringify(m.events));
  const big = store({ events: Array.from({ length: 700 }, (_, i) => ({ at: T0 + i, type: 'e' + i })) });
  check('events are capped at ' + M.EVENT_CAP, M.mergeProfileStores(big, store()).events.length === M.EVENT_CAP);
}

// Settings — the selection follows the person who chose most recently.
{
  const a = store({ profiles: [prof('A', 'active', T0), prof('B', 'active', T0)],
    settings: { envClasses: ['DEV'], activeProfileId: 'A', selectedAt: T0 + 10 } });
  const b = store({ profiles: [prof('A', 'active', T0), prof('B', 'active', T0)],
    settings: { envClasses: ['PRD', 'DEV'], activeProfileId: 'B', selectedAt: T0 + 20 } });
  check('the more recent selection wins, whichever side made it',
    M.mergeProfileStores(a, b).settings.activeProfileId === 'B'
    && M.mergeProfileStores(b, a).settings.activeProfileId === 'B');
  check('environment classes are a union in order of first appearance',
    M.mergeProfileStores(a, b).settings.envClasses.join(',') === 'DEV,PRD');
  const cloudChose = store({ profiles: [prof('A', 'active', T0)], settings: { envClasses: [], activeProfileId: 'A', selectedAt: 0 } });
  const deviceNone = store({ profiles: [prof('A', 'active', T0)], settings: { envClasses: [], activeProfileId: null, selectedAt: 0 } });
  check('with no timestamps at all, a made choice beats no choice',
    M.mergeProfileStores(cloudChose, deviceNone).settings.activeProfileId === 'A'
    && M.mergeProfileStores(deviceNone, cloudChose).settings.activeProfileId === 'A');
  const gone = store({ profiles: [prof('A', 'active', T0)], settings: { envClasses: [], activeProfileId: 'ZZZ', selectedAt: T0 + 99 } });
  check('a selection naming a profile that no longer exists anywhere is dropped, not carried',
    M.mergeProfileStores(gone, store()).settings.activeProfileId === null);
}

// Robustness: this runs inside init() and inside the save endpoint.
{
  let threw = false;
  try {
    M.mergeProfileStores(null, undefined);
    M.mergeProfileStores('garbage', 42);
    M.mergeProfileStores({ profiles: 'not a list', settings: null }, { connMeta: [] });
    M.mergeProfileStores({ profiles: [null, {}, { id: 'ok' }] }, {});
  } catch (e) { threw = e.message; }
  check('malformed input never throws', threw === false, threw);
  check('a record with no id is dropped rather than merged under "undefined"',
    M.mergeProfileStores({ profiles: [null, {}, { id: 'ok' }] }, {}).profiles.length === 1);
  check('isEmptyStore recognises a store with nothing in it', M.isEmptyStore(store()) && M.isEmptyStore(null)
    && !M.isEmptyStore(store({ profiles: [prof('A', 'draft', T0)] })));
  check('storesEqual ignores key order and array order',
    M.storesEqual(store({ profiles: [prof('A', 'active', T0), prof('B', 'active', T0)] }),
                  store({ profiles: [prof('B', 'active', T0), prof('A', 'active', T0)] })));
}

/* ── 3. The sync layer ────────────────────────────────────────────────────── */
section('3. On page load the union is applied and the cloud is brought up to it');

function jwt(claims) {
  const b64 = (o) => Buffer.from(JSON.stringify(o)).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  return b64({ alg: 'none' }) + '.' + b64(claims) + '.sig';
}
const TOKEN = jwt({ preferred_username: 'someone@example.com' });

function makeStorage(seed) {
  const data = Object.assign({}, seed || {});
  const s = {};
  Object.defineProperties(s, {
    getItem: { value: (k) => (Object.prototype.hasOwnProperty.call(data, k) ? data[k] : null) },
    setItem: { value: (k, v) => { data[k] = String(v); }, writable: true },
    removeItem: { value: (k) => { delete data[k]; } },
    clear: { value: () => { Object.keys(data).forEach((k) => delete data[k]); } },
    _data: { value: data },
  });
  return s;
}

function boot(opts) {
  const o = opts || {};
  const localStorage = makeStorage(Object.assign({ cygenix_token: TOKEN }, o.local || {}));
  const calls = [], timers = [];
  const window = {
    addEventListener() {}, dispatchEvent() { return true; },
    CygenixDataApi: {
      async callResult(action, o2) {
        const body = o2 && o2.body;
        calls.push({ action, method: (o2 && o2.method) || 'GET', body });
        return (o.responder || (() => ({ ok: true, data: {} })))(action, (o2 && o2.method) || 'GET', body);
      },
    },
  };
  const noop = () => {};
  const ctx = {
    window, localStorage, sessionStorage: makeStorage({}),
    console: { log: noop, warn: noop, error: noop, info: noop },
    CustomEvent: function (type, init) { this.type = type; this.detail = init && init.detail; },
    setTimeout: (fn, ms) => { timers.push({ fn, ms }); return timers.length; },
    clearTimeout: noop,
    document: { createElement: () => ({ style: {} }), body: { appendChild: noop, removeChild: noop } },
    atob: (s) => Buffer.from(s, 'base64').toString('binary'),
  };
  ctx.globalThis = ctx;
  vm.createContext(ctx);
  if (o.withMerge !== false) vm.runInContext(MERGE_SRC, ctx, { filename: 'cygenix-profile-merge.js' });
  vm.runInContext(SYNC_SRC, ctx, { filename: 'cygenix-cosmos-sync.js' });
  return { sync: window.CygenixSync, localStorage, calls, timers, window };
}
const settle = async () => { for (let i = 0; i < 4; i++) await new Promise((r) => setImmediate(r)); };
// The scenarios below are async; they are queued here and run in order at
// the end, so the synchronous sections keep their place in the output.
const queue = [];
function await_(fn) { queue.push(fn); }
const savedProfiles = (t) => JSON.parse(t.localStorage.getItem('cygenix_profiles_v1') || 'null');
const pendingSave = (t) => t.timers.find((x) => x.ms === 3000);
const okSave = { ok: true, data: { saved: true, updatedAt: 'now' } };

const A = prof('A', 'active', T0 + 1), B = prof('B', 'active', T0 + 2);

// a. Laptop has A locally; the cloud (from the desktop) has B.
{
  const t = boot({
    local: { cygenix_profiles_v1: JSON.stringify(store({ profiles: [A] })) },
    responder: (a) => (a === 'load' ? { ok: true, data: { jobs: [], connection_profiles: store({ profiles: [B] }) } } : okSave),
  });
  await_(async () => {
    await settle();
    const local = savedProfiles(t);
    check('local ends up with BOTH profiles, not the cloud\'s copy alone',
      local && local.profiles.map((p) => p.id).sort().join(',') === 'A,B', JSON.stringify(local && local.profiles));
    check('and an upload is queued, because the cloud is missing one', !!pendingSave(t));
    await pendingSave(t).fn(); await settle();
    const save = t.calls.find((c) => c.action === 'save');
    check('the upload carries the union under the connection_profiles field',
      save && save.body && save.body.connection_profiles
      && save.body.connection_profiles.profiles.map((p) => p.id).sort().join(',') === 'A,B',
      JSON.stringify(save && Object.keys(save.body || {})));
    check('and nothing else — one changed key, one field on the wire',
      save && Object.keys(save.body).join(',') === 'connection_profiles', JSON.stringify(save && Object.keys(save.body)));
  });
}

// b. First sign-in from a pre-sync browser: the cloud has nothing at all.
{
  const t = boot({
    local: { cygenix_profiles_v1: JSON.stringify(store({ profiles: [A] })) },
    responder: (a) => (a === 'load' ? { ok: true, data: {} } : okSave),
  });
  await_(async () => {
    await settle();
    check('a verified-empty cloud does not clear the local register',
      savedProfiles(t) && savedProfiles(t).profiles.length === 1);
    check('and the local register is adopted — an upload is queued', !!pendingSave(t));
    await pendingSave(t).fn(); await settle();
    const save = t.calls.find((c) => c.action === 'save');
    check('carrying the browser\'s profiles up to the account',
      save && save.body.connection_profiles && save.body.connection_profiles.profiles[0].id === 'A');
  });
}

// c. Cloud already has everything: apply, and do NOT churn a save.
{
  const t = boot({
    local: { cygenix_profiles_v1: JSON.stringify(store({ profiles: [A] })) },
    responder: (a) => (a === 'load' ? { ok: true, data: { connection_profiles: store({ profiles: [A, B] }) } } : okSave),
  });
  await_(async () => {
    await settle();
    check('when the cloud already holds everything local has, the cloud copy is applied',
      savedProfiles(t).profiles.length === 2);
    check('and no save is queued — a page load must not write to Cosmos for nothing', !pendingSave(t));
  });
}

// d. Nothing local: plain cloud-wins, no save.
{
  const t = boot({
    responder: (a) => (a === 'load' ? { ok: true, data: { connection_profiles: store({ profiles: [B] }) } } : okSave),
  });
  await_(async () => {
    await settle();
    check('a fresh browser receives the account\'s profiles', savedProfiles(t).profiles[0].id === 'B');
    check('without queueing a save', !pendingSave(t));
  });
}

// e. The merge module missing from a page: the plain contract, not a crash.
{
  const t = boot({
    withMerge: false,
    local: { cygenix_profiles_v1: JSON.stringify(store({ profiles: [A] })) },
    responder: (a) => (a === 'load' ? { ok: true, data: { connection_profiles: store({ profiles: [B] }) } } : okSave),
  });
  await_(async () => {
    await settle();
    check('a page without cygenix-profile-merge.js still boots and applies the cloud copy',
      savedProfiles(t) && savedProfiles(t).profiles[0].id === 'B');
  });
}

// f. An ordinary edit on the Profiles page is synced like any other key.
{
  const t = boot({ responder: (a) => (a === 'load' ? { ok: true, data: {} } : okSave) });
  await_(async () => {
    await settle();
    t.localStorage.setItem('cygenix_profiles_v1', JSON.stringify(store({ profiles: [A] })));
    check('writing the profiles key schedules the debounced save', !!pendingSave(t));
    await pendingSave(t).fn(); await settle();
    const save = t.calls.find((c) => c.action === 'save');
    check('and the save carries it as connection_profiles',
      save && save.body.connection_profiles && save.body.connection_profiles.profiles[0].id === 'A');
  });
}

// g. The user-switch wipe covers the key.
check('the key is in SYNC_KEYS, so a different user signing in on this browser does not inherit the register',
  /const SYNC_KEYS = \[[\s\S]*?'cygenix_profiles_v1'[\s\S]*?\];/.test(SYNC_SRC));

/* ── 4. The server ────────────────────────────────────────────────────────── */
section('4. The Function App merges on save');
const INDEX = read('azure-function', 'src', 'index.js');

check('connection_profiles is in the SYNCABLE whitelist — the server drops unknown fields silently',
  /const SYNCABLE = \[[\s\S]*?'connection_profiles'[\s\S]*?\];/.test(INDEX));
check('the save handler merges that field with the existing document instead of replacing it',
  /key === 'connection_profiles' && body\[key\] && existing\[key\][\s\S]{0,200}mergeProfileStores\(existing\[key\], body\[key\]\)/.test(INDEX));
check('a merge failure falls back to the sender\'s copy rather than failing the whole save',
  /merge failed[\s\S]{0,120}merged\[key\] = body\[key\]/.test(INDEX));
check('a null body value bypasses the merge — the one way to wipe (what nuke sends)',
  /body\[key\] && existing\[key\]/.test(INDEX) && /connection_profiles: null/.test(SYNC_SRC));
check('index.js requires ./profile-merge, which ships in azure-function/src',
  /require\('\.\/profile-merge'\)/.test(INDEX) && fs.existsSync(path.join(ROOT, 'azure-function', 'src', 'profile-merge.js')));
check('no function.json folder was added — the app stays on the v4 programming model',
  !fs.existsSync(path.join(ROOT, 'azure-function', 'profile-merge')) && !fs.existsSync(path.join(ROOT, 'azure-function', 'src', 'function.json')));
check('host.json still has no functionTimeout',
  !/functionTimeout/.test(read('azure-function', 'host.json')));

/* ── 5. The engine stamps what the merge needs ────────────────────────────── */
section('5. Every record that can be written on two machines says when');

{
  const CONNS = [
    { id: 's', side: 'src', mode: 'direct', connString: 'mssql://u:p@h/db' },
    { id: 't', side: 'tgt', mode: 'direct', connString: 'mssql://u:p@h/db2' },
  ];
  const st = P.cpNewStore(T0);
  P.cpSetConnMeta(st, 's', { envClass: 'DEV' }, 'me', T0 + 1);
  P.cpSetConnMeta(st, 't', { envClass: 'DEV' }, 'me', T0 + 1);
  check('connection meta carries updatedAt', st.connMeta.s.updatedAt === T0 + 1);
  const p = P.cpSaveProfile(st, { id: 'P1', envClass: 'DEV', srcConnId: 's', tgtConnId: 't' }, CONNS, 'me', T0 + 2);
  check('a new profile carries updatedAt', p.updatedAt === T0 + 2);
  P.cpSaveProfile(st, { id: 'P1', name: 'renamed' }, CONNS, 'me', T0 + 3);
  check('an edit refreshes it', p.updatedAt === T0 + 3);
  P.cpActivateProfile(st, 'P1', CONNS, 'me', T0 + 4);
  check('activation refreshes it', p.updatedAt === T0 + 4);

  check('cpSelectProfile is the one way to choose the working profile', typeof P.cpSelectProfile === 'function');
  P.cpSelectProfile(st, 'P1', 'me', T0 + 5);
  check('it records the choice and when it was made',
    st.settings.activeProfileId === 'P1' && st.settings.selectedAt === T0 + 5);
  let refused = false;
  P.cpSaveProfile(st, { id: 'P2', envClass: 'DEV', srcConnId: 's', tgtConnId: 't' }, CONNS, 'me', T0 + 6);
  try { P.cpSelectProfile(st, 'P2', 'me', T0 + 7); } catch (e) { refused = /only an active profile/.test(e.message); }
  check('a draft cannot be selected — only an active profile can be worked in', refused);
  P.cpRetireProfile(st, 'P1', 'me', T0 + 8);
  check('retiring the selected profile deselects it, with a fresh timestamp',
    st.settings.activeProfileId === null && st.settings.selectedAt === T0 + 8 && p.updatedAt === T0 + 8);
  check('the Profiles page selects through the engine, not by poking the field',
    /cpSelectProfile\(store, id/.test(read('public', 'profiles.html'))
    && !/store\.settings\.activeProfileId = id/.test(read('public', 'profiles.html')));
}

/* ── 6. Page wiring ───────────────────────────────────────────────────────── */
section('6. Every page that syncs loads the merge first');

{
  const pages = fs.readdirSync(PUB).filter((f) => f.endsWith('.html'));
  const syncing = pages.filter((f) => /<script src="\/cygenix-cosmos-sync\.js/.test(read('public', f)));
  const bad = syncing.filter((f) => {
    const src = read('public', f);
    const m = src.indexOf('/cygenix-profile-merge.js'), s = src.indexOf('<script src="/cygenix-cosmos-sync.js');
    return m === -1 || m > s;
  });
  check('every page that loads cygenix-cosmos-sync.js loads cygenix-profile-merge.js before it (' + syncing.length + ' pages)',
    syncing.length >= 15 && bad.length === 0, bad.join(', '));
  check('the storage inventory now calls the key synced',
    /'cygenix_profiles_v1':\s*\['A', 'Connection profiles', 'synced \(SYNC_KEYS\)'\]/.test(read('scripts', 'storage-inventory.js')));
}

/* ── run the async scenarios, then report ─────────────────────────────────── */
(async () => {
  for (const fn of queue) await fn();
  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
