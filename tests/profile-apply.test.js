// tests/profile-apply.test.js — selecting a profile loads its connections.
//
// The rules, pinned as decisions:
//   1. the plan decides by the SHAPE of a value, not the stored mode — a
//      connection string filed under fnUrl is still a connection string;
//   2. a plan never blanks a field it does not have, and never adopts a live
//      connection string it cannot tie to the entry;
//   3. every write goes through CygenixConnections.setActive, which is what
//      the sync layer watches — no second writer, no second sync;
//   4. a running job blocks the switch; a live stream or an agentive marker
//      asks; PRD asks for the id typed; draft/retired are refused;
//   5. the load-time check runs once per selection per session, after the
//      cloud load, and re-applies only when nothing is missing.
//
// The real connections.js is loaded, because the shape fix lives in its
// setActive() and this is the one place that proves a mssql:// string can
// no longer land in the function-URL field.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

console.log('Profiles — selecting one loads its connections\n');

/* ── A browser-shaped world ─────────────────────────────────────────────── */
const mk = () => {
  const mem = new Map();
  return {
    getItem: (k) => (mem.has(k) ? mem.get(k) : null),
    setItem: (k, v) => { mem.set(k, String(v)); },
    removeItem: (k) => { mem.delete(k); },
    key: (i) => Array.from(mem.keys())[i] || null,
    get length() { return mem.size; },
    _mem: mem,
  };
};
global.localStorage = mk();
global.sessionStorage = mk();
global.window = global;
global.atob = (s) => Buffer.from(s, 'base64').toString('binary');
const events = [];
global.dispatchEvent = (e) => { events.push(e.type); return true; };
global.addEventListener = () => {};
global.CustomEvent = class { constructor(type, init) { this.type = type; this.detail = init && init.detail; } };
localStorage.setItem('cygenix_user', JSON.stringify({ email: 'you@example.test' }));

require('../public/connections.js');
require('../public/cygenix-saved-conn-secrets.js');
const P = require('../public/cygenix-profiles.js');
const A = require('../public/cygenix-profile-apply.js');
const C = global.CygenixConnections;
const SEC = global.CygenixSavedConnSecrets;

/* ── 1. The setActive shape fix (the latent bug) ────────────────────────── */

{
  C.setActive({ tgtConnMode: 'azure', tgtFnUrl: 'mssql://u:p@h:1433/db', tgtFnKey: 'k',
    srcConnMode: 'direct', srcConnString: 'https://fn.example/api/x', srcFnKey: 'sk' });
  const c = C.get();
  check('a connection string in the function-URL box lands in connString, mode direct',
    c.tgtConnString === 'mssql://u:p@h:1433/db' && c.tgtConnMode === 'direct' && c.tgtFnUrl === '',
    JSON.stringify(c));
  check('a function URL in the string box lands in fnUrl, mode azure, keeping the key',
    c.srcFnUrl === 'https://fn.example/api/x' && c.srcConnMode === 'azure' && c.srcConnString === '' && c.srcFnKey === 'sk',
    JSON.stringify(c));
  C.setActive({ tgtConnMode: 'azure', tgtFnUrl: 'https://fn.example/api/y', tgtFnKey: 'k2' });
  check('a well-formed azure side is stored as given', C.get().tgtConnMode === 'azure' && C.get().tgtFnUrl === 'https://fn.example/api/y');
  check('the fix is in the ONE writer, with the reason written on it',
    /function normaliseSide\(cs, mode, url, key\)/.test(read('public', 'connections.js'))
    && /Shape, not mode/.test(read('public', 'connections.js')));
}

/* ── 2. The plan ────────────────────────────────────────────────────────── */

const store = () => ({
  v: 1, connMeta: {}, bindings: [], runRecords: [], events: [],
  profiles: [
    { id: 'FIN-DEV-01', name: 'Dev', envClass: 'DEV', status: 'active', srcConnId: 'c_afs', tgtConnId: 'c_cloud' },
    { id: 'FIN_3E_UAT', name: 'Finance UAT', envClass: 'UAT', status: 'active', srcConnId: 'c_conv', tgtConnId: 'c_h3' },
    { id: 'FIN-PRD-01', name: 'Production', envClass: 'PRD', status: 'active', srcConnId: 'c_conv', tgtConnId: 'c_h3' },
    { id: 'DRAFTY', name: 'Draft', envClass: 'DEV', status: 'draft', srcConnId: 'c_afs', tgtConnId: 'c_cloud' },
    { id: 'OLD', name: 'Retired', envClass: 'DEV', status: 'retired', srcConnId: 'c_afs', tgtConnId: 'c_cloud' },
  ],
  settings: { envClasses: ['DEV', 'TEST', 'UAT', 'PRD', 'SANDBOX'], activeProfileId: null, selectedAt: 0 },
});
const conns = () => [
  { id: 'c_afs',   side: 'src', mode: 'azure',  name: 'Azure Function Source', fnUrl: 'https://src.azurewebsites.net/api/db', fnKey: 'afs-key' },
  { id: 'c_cloud', side: 'tgt', mode: 'azure',  name: 'cygenix-cloud_new', fnUrl: 'https://cloud.azurewebsites.net/api/db', fnKey: 'cloud-key' },
  { id: 'c_conv',  side: 'src', mode: 'direct', name: 'Conversion', connString: 'mssql://u:p@conv:1433/Conversion' },
  { id: 'c_h3',    side: 'tgt', mode: 'direct', name: 'H Database 3', connString: 'mssql://u:p@h3:1433/HDB3' },
];

{
  const pl = A.plan(store(), 'FIN-DEV-01', conns());
  check('FIN-DEV-01 plans both azure sides by name, URL and key',
    pl.ok && pl.src.name === 'Azure Function Source' && pl.tgt.name === 'cygenix-cloud_new'
    && pl.fields.srcConnMode === 'azure' && pl.fields.srcFnUrl === 'https://src.azurewebsites.net/api/db'
    && pl.fields.srcFnKey === 'afs-key' && pl.fields.tgtFnKey === 'cloud-key' && pl.missing.length === 0,
    JSON.stringify(pl));
  const pl2 = A.plan(store(), 'FIN_3E_UAT', conns());
  check('FIN_3E_UAT plans two direct sides', pl2.ok && pl2.fields.srcConnMode === 'direct'
    && pl2.fields.srcConnString === 'mssql://u:p@conv:1433/Conversion' && pl2.fields.tgtConnString === 'mssql://u:p@h3:1433/HDB3');
  check('the summary names the connections and never a value',
    A.summary(pl2) === 'Loaded FIN_3E_UAT: Source = Conversion, Target = H Database 3' && A.summary(pl2).indexOf('mssql') === -1);
  check('a draft profile is refused by the plan', !A.plan(store(), 'DRAFTY', conns()).ok && /draft/.test(A.plan(store(), 'DRAFTY', conns()).reasons[0]));
  check('a retired one too', /retired/.test(A.plan(store(), 'OLD', conns()).reasons[0]));
  check('a profile whose connection is gone from the list is refused, naming it',
    /c_h3/.test(A.plan(store(), 'FIN_3E_UAT', conns().slice(0, 3)).reasons[0]));

  // Shape, not mode.
  const misfiled = [{ id: 'c_x', side: 'tgt', mode: 'azure', name: 'Misfiled', fnUrl: 'mssql://u:p@h/db' }];
  const n = A.normaliseEntry(misfiled[0]);
  check('an entry with a connection string under fnUrl is planned as direct', n.mode === 'direct' && n.connString === 'mssql://u:p@h/db' && n.fnUrl === '');
  const n2 = A.normaliseEntry({ mode: 'direct', connString: 'https://fn.example/api', fnKey: 'k' });
  check('and a URL under connString as azure', n2.mode === 'azure' && n2.fnUrl === 'https://fn.example/api' && n2.fnKey === 'k');

  // Missing.
  const stripped = conns().map((c) => { const x = Object.assign({}, c); delete x.connString; delete x.fnKey; return x; });
  const pm = A.plan(store(), 'FIN_3E_UAT', stripped);
  check('on another browser a direct entry has no string: HARD missing, named by side and connection',
    pm.ok && pm.missing.length === 2 && pm.missing.every((m) => m.hard && m.field === 'connString')
    && pm.missing[0].name === 'Conversion' && pm.missing[1].name === 'H Database 3', JSON.stringify(pm.missing));
  check('the missing sentence says exactly which field, and what to do',
    /Source "Conversion": no connection string on this browser — enter it once on Connections/.test(A.missingSentence(pm)));
  const pa = A.plan(store(), 'FIN-DEV-01', stripped);
  check('an azure entry without its key is SOFT missing — a function may need none',
    pa.ok && pa.missing.length === 2 && pa.missing.every((m) => !m.hard && m.field === 'fnKey') && pa.fields.srcFnUrl);
}

/* ── 3. Merging with the live values ────────────────────────────────────── */

{
  const stripped = conns().map((c) => { const x = Object.assign({}, c); delete x.fnKey; return x; });
  const pl = A.plan(store(), 'FIN-DEV-01', stripped);
  const live = { srcFnUrl: 'https://src.azurewebsites.net/api/db', srcFnKey: 'live-key', tgtFnUrl: 'https://other/api', tgtFnKey: 'other-key' };
  const m = A.mergeWithLive(pl.fields, live);
  check('a missing key is taken from the live values when the live URL is the SAME URL', m.srcFnKey === 'live-key');
  check('…and not when the live URL is a different endpoint', m.tgtFnKey === '');
  const pl2 = A.plan(store(), 'FIN_3E_UAT', conns().map((c) => { const x = Object.assign({}, c); delete x.connString; return x; }));
  const m2 = A.mergeWithLive(pl2.fields, { srcConnString: 'mssql://something/else', srcConnMode: 'direct' });
  check('a missing connection string is NEVER adopted from the live values', m2.srcConnString === '');
  check('sameFields compares the eight fields and nothing else',
    A.sameFields({ srcConnString: 'a', extra: 1 }, { srcConnString: 'a' }) && !A.sameFields({ srcConnString: 'a' }, { srcConnString: 'b' }));
}

/* ── 4. What is running ─────────────────────────────────────────────────── */

{
  check('nothing running: clear', A.busy({}).clear === true);
  const b = A.busy({ jobs: [{ id: 'j1', name: 'Nightly', executionStatus: 'running' }] });
  check('a running job BLOCKS, naming it', b.block.length === 1 && /Nightly/.test(b.block[0]) && !b.clear);
  const b2 = A.busy({ streams: [{ streams: [{ name: 'Orders CDC', status: 'lagging' }] }], agentiveRun: true });
  check('a live stream and an agentive marker ASK rather than block',
    b2.block.length === 0 && b2.confirm.length === 2 && /Orders CDC/.test(b2.confirm[0]));
  check('a paused stream and a complete job are not running',
    A.busy({ streams: [{ streams: [{ name: 'x', status: 'paused' }] }], jobs: [{ status: 'complete' }] }).clear);
}

/* ── 5. select(): the whole act, through the real stores ────────────────── */

const seedWorld = (st, list, secretsFor) => {
  localStorage.setItem(P.STORE_KEY, JSON.stringify(st));
  const blob = {}; blob['you@example.test'] = list.map((c) => { const x = Object.assign({}, c); delete x.connString; delete x.fnKey; return x; });
  localStorage.setItem('cygenix_saved_connections', JSON.stringify(blob));
  const sec = {};
  list.forEach((c) => {
    if (secretsFor && secretsFor.indexOf(c.id) === -1) return;
    const b = {}; if (c.connString) b.connString = c.connString; if (c.fnKey) b.fnKey = c.fnKey;
    if (Object.keys(b).length) sec[c.id] = b;
  });
  localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify(sec));
  sessionStorage.removeItem(A.FINISH_KEY);
  sessionStorage.removeItem(A.SEEN_KEY);
};

{
  seedWorld(store(), conns());
  C.setActive({ srcConnString: 'mssql://old/src', tgtConnString: 'mssql://old/tgt' });
  events.length = 0;
  const r = A.select('FIN-DEV-01');
  const live = C.get();
  const st = P.cpLoad();
  check('selecting FIN-DEV-01 selects it in the store and loads both sides into the live settings',
    r.ok && r.applied && st.settings.activeProfileId === 'FIN-DEV-01'
    && live.srcFnUrl === 'https://src.azurewebsites.net/api/db' && live.srcFnKey === 'afs-key' && live.srcConnMode === 'azure'
    && live.tgtFnUrl === 'https://cloud.azurewebsites.net/api/db' && live.tgtConnMode === 'azure' && live.srcConnString === '',
    JSON.stringify({ r: r.message, live }));
  check('the confirmation names the profile and both connections',
    r.message === 'Loaded FIN-DEV-01: Source = Azure Function Source, Target = cygenix-cloud_new');
  check('the applied event and the profiles-changed event both fire, once each',
    events.filter((e) => e === 'cygenix:connections-applied').length === 1 && events.filter((e) => e === 'cygenix:profiles-changed').length === 1,
    events.join(','));
  check('the store records the load with names only',
    st.events.some((e) => e.type === 'profile.applied' && e.src === 'Azure Function Source') && JSON.stringify(st).indexOf('afs-key') === -1);
  check('the saved entry itself is untouched — a copy, never an edit',
    JSON.parse(localStorage.getItem('cygenix_saved_connections'))['you@example.test'].every((c) => !c.connString && !c.fnKey));

  const r2 = A.select('FIN_3E_UAT');
  const live2 = C.get();
  check('switching to FIN_3E_UAT replaces both with Conversion / H Database 3, no typing',
    r2.ok && live2.srcConnString === 'mssql://u:p@conv:1433/Conversion' && live2.srcConnMode === 'direct'
    && live2.tgtConnString === 'mssql://u:p@h3:1433/HDB3' && live2.srcFnUrl === '' && live2.tgtFnKey === '');

  // Draft / retired.
  check('a draft profile cannot be selected', !A.select('DRAFTY').ok && /draft/.test(A.select('DRAFTY').reason));
  check('nor a retired one', /retired/.test(A.select('OLD').reason));
  check('…and the live settings did not move', C.get().tgtConnString === 'mssql://u:p@h3:1433/HDB3');

  // PRD.
  const asked = [];
  const r3 = A.select('FIN-PRD-01', { prompt: (q) => { asked.push(q); return 'WRONG'; } });
  check('a PRD profile asks for its id typed, in the dashboard\'s words; a wrong answer changes nothing',
    !r3.ok && r3.refused && /PRD profile \(FIN-PRD-01\)/.test(asked[0]) && /Type the profile id/.test(asked[0])
    && P.cpLoad().settings.activeProfileId === 'FIN_3E_UAT', JSON.stringify(r3));
  check('the refusal is on the profile record',
    P.cpLoad().events.some((e) => e.type === 'profile.select_refused' && e.profileId === 'FIN-PRD-01'));
  const r4 = A.select('FIN-PRD-01', { prompt: () => ' FIN-PRD-01 ' });
  check('the right id (whitespace forgiven) selects and loads it', r4.ok && P.cpLoad().settings.activeProfileId === 'FIN-PRD-01');

  // Busy.
  localStorage.setItem('cygenix_jobs', JSON.stringify([{ id: 'j1', name: 'Nightly load', executionStatus: 'running' }]));
  const r5 = A.select('FIN-DEV-01');
  check('a running job blocks the switch outright, naming the job',
    !r5.ok && /Nightly load/.test(r5.reason) && P.cpLoad().settings.activeProfileId === 'FIN-PRD-01');
  localStorage.setItem('cygenix_jobs', '[]');
  localStorage.setItem('cygenix_datastream_v1::p1', JSON.stringify({ streams: [{ name: 'Orders CDC', status: 'running' }] }));
  const r6 = A.select('FIN-DEV-01', { confirm: () => false });
  check('a live stream asks; declining leaves everything as it was', !r6.ok && /Orders CDC/.test(r6.reason) && P.cpLoad().settings.activeProfileId === 'FIN-PRD-01');
  const r7 = A.select('FIN-DEV-01', { confirm: () => true });
  check('…and accepting goes ahead', r7.ok && P.cpLoad().settings.activeProfileId === 'FIN-DEV-01');
  localStorage.removeItem('cygenix_datastream_v1::p1');

  // Rate guard.
  const before = JSON.stringify(C.get());
  C.setActive({ srcConnString: 'mssql://hand/typed' });
  const pl = A.plan(P.cpLoad(), 'FIN-DEV-01', A.loadSavedConns());
  check('a second apply of the same profile inside 3 seconds is dropped', A.apply(pl) === null && C.get().srcConnString === 'mssql://hand/typed');
  check('…unless forced, which is what a deliberate re-select does', !!A.apply(pl, { force: true }) && JSON.stringify(C.get()) === before);
}

/* ── 6. Missing credentials: load what exists, mark the side, finish once ── */

{
  seedWorld(store(), conns(), ['c_conv']);       // H Database 3's string is NOT on this browser
  C.setActive({});
  const r = A.select('FIN_3E_UAT');
  const live = C.get();
  check('what exists is loaded; the missing side is left empty rather than guessed',
    r.ok && live.srcConnString === 'mssql://u:p@conv:1433/Conversion' && live.tgtConnString === '');
  check('the message names the field and the connection',
    /Target "H Database 3": no connection string on this browser/.test(r.missing));
  const pend = A.pendingFinish();
  check('the Connections page is told which side to open',
    pend && pend.profileId === 'FIN_3E_UAT' && pend.items.some((m) => m.side === 'tgt' && m.field === 'connString'));

  // The person types it on Connections and saves.
  const saved = Object.assign({}, live, { tgtConnString: 'mssql://u:p@h3:1433/HDB3', tgtConnMode: 'direct' });
  C.setActive(saved);
  const kept = A.finishOnce(saved);
  check('finishOnce keeps it in the entry\'s LOCAL secret store, by name',
    kept.length === 1 && kept[0].side === 'tgt' && kept[0].name === 'H Database 3' && SEC.get('c_h3').connString === 'mssql://u:p@h3:1433/HDB3');
  check('the synced entry is still untouched', JSON.parse(localStorage.getItem('cygenix_saved_connections'))['you@example.test'].every((c) => !c.connString));
  check('and the pending marker is cleared, so it is not asked for again', A.pendingFinish() === null);
  check('a second finish has nothing to do', A.finishOnce(saved).length === 0);
  check('a string typed with no pending mark is NOT adopted onto an entry',
    (() => { seedWorld(store(), conns(), ['c_conv']); C.setActive({}); A.select('FIN_3E_UAT'); A.clearFinish();
      return A.finishOnce({ tgtConnString: 'mssql://someone/else' }).length === 0 && !SEC.hasSecret('c_h3'); })());
}

/* ── 7. The load-time check ─────────────────────────────────────────────── */

{
  seedWorld(store(), conns());
  const st = store(); st.settings.activeProfileId = 'FIN-DEV-01'; st.settings.selectedAt = 5;
  localStorage.setItem(P.STORE_KEY, JSON.stringify(st));
  C.setActive({ srcConnString: 'mssql://drifted/away' });
  const d = A.drift(P.cpLoad(), A.loadSavedConns(), C.get());
  check('drift() sees the live settings no longer match the selected profile', d.drifted && d.profileId === 'FIN-DEV-01' && d.hard.length === 0);
  const r = A.checkOnLoad();
  check('checkOnLoad re-applies silently when nothing is missing',
    r && r.applied && C.get().srcFnUrl === 'https://src.azurewebsites.net/api/db' && C.get().srcConnString === '');
  check('and stamps the selection so it runs once per session', sessionStorage.getItem(A.SEEN_KEY) === 'FIN-DEV-01::5');
  C.setActive({ srcConnString: 'mssql://drifted/again' });
  check('a second check in the same session for the same selection does nothing', A.checkOnLoad() === null && C.get().srcConnString === 'mssql://drifted/again');
  const st2 = store(); st2.settings.activeProfileId = 'FIN-DEV-01'; st2.settings.selectedAt = 6;
  localStorage.setItem(P.STORE_KEY, JSON.stringify(st2));
  check('a NEW selection is checked again', !!A.checkOnLoad() && C.get().srcFnUrl === 'https://src.azurewebsites.net/api/db');
  check('with the live values matching, drift() is quiet', !A.drift(P.cpLoad(), A.loadSavedConns(), C.get()).drifted);

  // Missing on this browser: warn, never write.
  seedWorld(store(), conns(), ['c_conv']);
  const st3 = store(); st3.settings.activeProfileId = 'FIN_3E_UAT'; st3.settings.selectedAt = 7;
  localStorage.setItem(P.STORE_KEY, JSON.stringify(st3));
  C.setActive({ tgtConnString: 'mssql://from/the/cloud' });
  const r3 = A.checkOnLoad();
  check('a hard-missing credential means a warning and NO write over the live values',
    r3 && r3.warned && !r3.applied && C.get().tgtConnString === 'mssql://from/the/cloud'
    && A.pendingFinish() && A.pendingFinish().profileId === 'FIN_3E_UAT');
}

/* ── 8. Wiring ──────────────────────────────────────────────────────────── */

{
  const mod = read('public', 'cygenix-profile-apply.js');
  check('the module writes through setActive and nowhere else',
    (mod.match(/\.setActive\(/g) || []).length === 1 && !/localStorage\.setItem\('cygenix_project_connections'/.test(mod));
  check('it makes no network call of its own', !/fetch\(|XMLHttpRequest|callResult|callApi/.test(mod));
  check('no value reaches a log or the notice', !/console\.(log|warn|info)/.test(mod));
  check('the load-time check waits for the cloud load and cannot run twice',
    /cygenix-sync-loaded/.test(mod) && /if \(ran\) return; ran = true;/.test(mod));
  check('the applied event is dispatched by apply() and listened to by views, never by this file',
    /dispatchEvent\(new CustomEvent\('cygenix:connections-applied'/.test(mod) && !/addEventListener\('cygenix:connections-applied'/.test(mod));

  const prof = read('public', 'profiles.html');
  check('the Profiles page selects through the module and re-reads the store rather than saving twice',
    /A\.select\(id\)/.test(prof) && /store = P\.cpLoad\(\);\s*\n\s*renderAll\(\);/.test(prof) && /id="cp-apply-msg"/.test(prof));
  check('…and still works without the module', /if \(!A\) \{[\s\S]{0,200}P\.cpSelectProfile/.test(prof));

  const dash = read('public', 'dashboard-app.js');
  check('the Connections view names the profile the values came from, and opens a side to finish',
    /function connProfileLines\(\)/.test(dash) && /From profile ' \+ id/.test(dash) && /connReveal\(side\)/.test(dash));
  check('a Save keeps a finished credential through finishOnce', /A\.finishOnce\(fields\)/.test(dash));
  check('the view refills on the applied event and on another tab\'s write, only while showing',
    /addEventListener\('cygenix:connections-applied', refillIfShowing\)/.test(dash)
    && /e\.key === 'cygenix_project_connections'\) refillIfShowing/.test(dash)
    && /offsetParent === null\) return;/.test(dash));

  const pages = ['dashboard.html', 'profiles.html', 'data-enrichment.html', 'data-generator.html', 'data-quality.html'];
  const bad = pages.filter((f) => {
    const s = read('public', f);
    return !/cygenix-profile-apply\.js/.test(s) || s.indexOf('cygenix-profile-apply.js') < s.indexOf('cygenix-profiles.js')
      || s.indexOf('cygenix-profile-apply.js') < s.indexOf('connections.js');
  });
  check('every page that carries profiles loads the module, after connections.js and the engine', bad.length === 0, bad.join(', '));
  const others = fs.readdirSync(path.join(ROOT, 'public')).filter((f) => f.endsWith('.html') && !pages.includes(f)
    && /cygenix-profile-apply\.js/.test(read('public', f)));
  check('and no page without the engine loads it', others.length === 0, others.join(', '));

  const audit = read('netlify', 'functions', 'lib', 'audit-schema.js');
  check('profile.applied is on the client allowlist', /'profile\.applied':\s*'connections'/.test(audit));
  const inv = read('scripts', 'storage-inventory.js');
  check('both session keys are classified', /'cygenix_profile_apply_seen'/.test(inv) && /'cygenix_profile_finish'/.test(inv));
}

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
