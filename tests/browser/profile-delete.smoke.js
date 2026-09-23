/* tests/browser/profile-delete.smoke.js
 * ---------------------------------------------------------------------------
 * Retired profiles fold away; an unreferenced one can be deleted — in a
 * real browser.
 *
 * tests/profile-delete.test.js proves the rules. What it cannot show is that
 * /profiles actually hides the retired row, that the toggle survives a
 * reload, that the pickers stop listing it, that the Delete slot is where
 * it should be and off where it should be, that the typed-id prompt refuses
 * a wrong answer, that a delete makes ONE delete request and ONE audit post,
 * that the saved connections are the same bytes afterwards, that it stays
 * gone after a reload AND after a "second browser" uploads an old copy, and
 * that nothing loops.
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/profile-delete.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8412);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.json': 'application/json' };
const ROUTES = (() => {
  const map = {};
  fs.readFileSync(path.join(PUB, '_redirects'), 'utf8').split('\n').forEach((line) => {
    const m = line.trim().match(/^(\/\S*)\s+(\/\S+)\s+200$/);
    if (m) map[m[1]] = m[2];
  });
  return map;
})();
const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/') p = '/index.html';
  if (ROUTES[p]) p = ROUTES[p];
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end('no'); }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

const U = 'you@example.test';
const CONNS = [
  { id: 'c_dsrc', side: 'src', mode: 'direct', name: 'Dev source', connString: 'mssql://u:p@dev:1433/FinDev' },
  { id: 'c_dtgt', side: 'tgt', mode: 'direct', name: 'Dev target', connString: 'mssql://u:p@dev:1433/FinDevTgt' },
  { id: 'c_usrc', side: 'src', mode: 'direct', name: 'UAT source', connString: 'mssql://u:p@uat:1433/FinUat' },
  { id: 'c_utgt', side: 'tgt', mode: 'direct', name: 'UAT target', connString: 'mssql://u:p@uat:1433/FinUatTgt' },
];
const T = 1700000000000;
const STORE = {
  v: 1, createdAt: T,
  connMeta: { c_dsrc: { envClass: 'DEV', systemDomain: 'FIN', updatedAt: T }, c_dtgt: { envClass: 'DEV', systemDomain: 'FIN', updatedAt: T },
    c_usrc: { envClass: 'UAT', systemDomain: 'FIN', updatedAt: T }, c_utgt: { envClass: 'UAT', systemDomain: 'FIN', updatedAt: T } },
  profiles: [
    { id: 'FIN-DEV-01', name: 'Dev (old)', envClass: 'DEV', status: 'retired', srcConnId: 'c_dsrc', tgtConnId: 'c_dtgt', createdAt: T, updatedAt: T + 5, retiredAt: T + 5 },
    { id: 'FIN-DEV-02', name: 'Dev', envClass: 'DEV', status: 'active', srcConnId: 'c_dsrc', tgtConnId: 'c_dtgt', createdAt: T + 1, updatedAt: T + 1 },
    { id: 'FIN-UAT-01', name: 'UAT', envClass: 'UAT', status: 'active', srcConnId: 'c_usrc', tgtConnId: 'c_utgt', createdAt: T + 2, updatedAt: T + 2 },
    { id: 'FIN-UAT-02', name: 'UAT 2', envClass: 'UAT', status: 'active', srcConnId: 'c_usrc', tgtConnId: 'c_utgt', createdAt: T + 3, updatedAt: T + 3 },
  ],
  bindings: [], runRecords: [], events: [], deleted: [],
  settings: { envClasses: ['DEV', 'TEST', 'UAT', 'PRD', 'SANDBOX'], activeProfileId: null, selectedAt: 0 },
};

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  const token = 'x.' + Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: U })).toString('base64url') + '.y';

  const reqs = [];          // every data-layer / audit request: { action, body }
  let cloudLoad = {};       // what action=load answers
  let deleteAnswer = { status: 200, body: { deleted: true, tombstoned: true } };
  await ctx.route('**', (route) => {
    const u = route.request().url();
    const json = (status, body) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
    const post = () => { try { return JSON.parse(route.request().postData() || 'null'); } catch (e) { return null; } };
    if (/action=whoami/.test(u)) return json(200, { tier: 'pro', tier_status: 'active', role: 'user' });
    if (/action=load/.test(u)) { reqs.push({ action: 'load' }); return json(200, cloudLoad); }
    if (/action=save/.test(u)) { reqs.push({ action: 'save', body: post() }); return json(200, { saved: true, updatedAt: new Date().toISOString() }); }
    if (/action=connection-profile-delete/.test(u)) { reqs.push({ action: 'delete', body: post() }); return json(deleteAnswer.status, deleteAnswer.body); }
    if (/functions\/audit/.test(u)) { reqs.push({ action: 'audit', body: post() }); return json(200, { ok: true, seq: reqs.length }); }
    if (/data-proxy|netlify\/functions|\/api\//.test(u)) { reqs.push({ action: 'other:' + u.replace(/^.*action=/, '').slice(0, 40) }); return json(200, {}); }
    if (u.startsWith('http://localhost:' + PORT)) return route.continue();
    return route.abort();
  });
  await ctx.addInitScript((arg) => {
    if (localStorage.getItem('cygenix_profiles_v1')) return;    // seed once; later loads keep the state
    const acct = { homeAccountId: 'h.t', environment: 'cygenix.ciamlogin.com', tenantId: 't', username: arg.U, localAccountId: 'l', authorityType: 'MSSTS', name: 'You' };
    [localStorage, sessionStorage].forEach((s) => { s.setItem('cygenix_token', arg.token); s.setItem('cygenix_expires', String(Date.now() + 3600e3)); });
    localStorage.setItem('cygenix_onboarded', 'true');
    localStorage.setItem('cygenix_user', JSON.stringify({ email: arg.U }));
    localStorage.setItem('cygenix_active_user', arg.U);
    localStorage.setItem('cygenix_tier', 'pro');
    localStorage.setItem('cygenix_cookie_consent', JSON.stringify({ version: '2', essential: true, functional: true, analytics: false, timestamp: new Date().toISOString() }));
    localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify(acct));
    localStorage.setItem('h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-',
      JSON.stringify({ credentialType: 'IdToken', secret: arg.token, expiresOn: String(Math.floor(Date.now() / 1000) + 3600) }));
    const blob = {}; blob[arg.U] = arg.conns.map((c) => { const x = Object.assign({}, c); delete x.connString; return x; });
    localStorage.setItem('cygenix_saved_connections', JSON.stringify(blob));
    const sec = {}; arg.conns.forEach((c) => { sec[c.id] = { connString: c.connString }; });
    localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify(sec));
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify(arg.store));
  }, { U, token, conns: CONNS, store: STORE });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  // "Failed to load resource" is the browser noting an aborted or refused
  // fetch: the fonts this harness blocks, and the 409 this test asks for.
  // Neither is a page error. Everything else the console calls an error is.
  page.on('console', (m) => { if (m.type() === 'error' && !/Failed to load resource/.test(m.text())) errors.push('[console] ' + m.text()); });
  const answers = [];
  const dialogs = [];
  page.on('dialog', async (d) => {
    dialogs.push({ type: d.type(), message: d.message() });
    if (d.type() === 'prompt') return d.accept(answers.length ? answers.shift() : '');
    if (d.type() === 'confirm') return d.accept();
    return d.dismiss();
  });

  const open = async () => {
    await page.goto('http://localhost:' + PORT + '/profiles', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => !!window.CygenixSync && !!window.CygenixProfiles && document.querySelectorAll('#cp-profiles tbody tr').length > 0, null, { timeout: 20000 });
    await page.waitForTimeout(900);     // the page's own 600ms re-render
  };
  const rows = () => page.evaluate(() => Array.from(document.querySelectorAll('#cp-profiles tbody tr[data-profile-id]')).map((r) => r.dataset.profileId));
  const summary = () => page.evaluate(() => document.getElementById('cp-prof-sub').textContent);
  const toggle = () => page.evaluate(() => { const n = document.getElementById('cp-retired-note'); const b = document.getElementById('cp-retired-toggle'); return { shown: n.style.display !== 'none', text: b ? b.textContent : '' }; });
  const options = (id) => page.evaluate((i) => Array.from(document.getElementById(i).options).map((o) => o.value), id);
  const store = () => page.evaluate(() => JSON.parse(localStorage.getItem('cygenix_profiles_v1')));
  const conns = () => page.evaluate(() => localStorage.getItem('cygenix_saved_connections') + '|' + localStorage.getItem('cygenix_saved_conn_secrets'));
  const deleteSlots = () => page.evaluate(() => Array.from(document.querySelectorAll('#cp-profiles tbody tr[data-profile-id]')).map((r) => {
    const b = Array.from(r.querySelectorAll('button')).find((x) => x.textContent === 'Delete');
    return { id: r.dataset.profileId, status: r.dataset.status, has: !!b, disabled: !!(b && b.disabled), title: b ? (b.closest('.cp-hold') || {}).title || '' : '' };
  }));

  console.log('Profiles — retired rows hidden, unreferenced ones deletable\n');

  /* ── a. Hidden by default; the toggle; it survives a reload ───────────── */
  await open();
  const connsBefore = await conns();
  check('/profiles loads with the retired row hidden: three rows, none of them FIN-DEV-01',
    (await rows()).join() === 'FIN-DEV-02,FIN-UAT-01,FIN-UAT-02', (await rows()).join());
  check('the summary line still counts it: "3 active · 0 draft · 1 retired"', (await summary()) === '3 active · 0 draft · 1 retired', await summary());
  let t = await toggle();
  check('the toggle reads "Show 1 retired"', t.shown && t.text === 'Show 1 retired', JSON.stringify(t));
  await page.click('#cp-retired-toggle');
  check('clicking it reveals FIN-DEV-01', (await rows()).indexOf('FIN-DEV-01') === 0, (await rows()).join());
  t = await toggle();
  check('and now reads "Hide retired"', t.text === 'Hide retired', JSON.stringify(t));
  await open();
  check('the toggle survives a reload', (await rows()).indexOf('FIN-DEV-01') !== -1 && (await toggle()).text === 'Hide retired');

  /* ── b. The pickers ───────────────────────────────────────────────────── */
  check('the Sentinel picker no longer lists FIN-DEV-01, and defaults to the first active profile',
    (await options('cp-s-profile')).join() === 'FIN-DEV-02,FIN-UAT-01,FIN-UAT-02'
    && (await page.evaluate(() => document.getElementById('cp-s-profile').value)) === 'FIN-DEV-02', (await options('cp-s-profile')).join());
  check('the Bind-artifact picker no longer lists FIN-DEV-01', (await options('cp-b-profile')).join() === 'FIN-DEV-02,FIN-UAT-01,FIN-UAT-02');
  check('the sentinel SQL is generated for the fallback, not for the retired profile',
    await page.evaluate(() => /FIN-DEV-02/.test(document.getElementById('cp-s-assert').textContent) && !/FIN-DEV-01/.test(document.getElementById('cp-s-assert').textContent)));

  /* ── c. Where Delete is ───────────────────────────────────────────────── */
  let slots = await deleteSlots();
  check('active rows show no Delete', slots.filter((s) => s.status === 'active').every((s) => !s.has), JSON.stringify(slots));
  check('the retired row with no history shows an ENABLED Delete', slots.some((s) => s.id === 'FIN-DEV-01' && s.has && !s.disabled), JSON.stringify(slots));

  /* ── e (first half). A retired profile WITH history ───────────────────── */
  await page.evaluate(() => {
    const s = JSON.parse(localStorage.getItem('cygenix_profiles_v1'));
    s.profiles.push({ id: 'FIN-DEV-09', name: 'Dev 9', envClass: 'DEV', status: 'retired', srcConnId: 'c_dsrc', tgtConnId: 'c_dtgt', createdAt: 1, updatedAt: 2, retiredAt: 2 });
    s.bindings.push({ artifactType: 'job', artifactId: 'j1', profileId: 'FIN-DEV-09', boundAt: 3, boundBy: 'me' });
    s.runRecords.push({ runId: 'run_1', artifactType: 'job', artifactId: 'j1', profileId: 'FIN-DEV-09', startedAt: 4, outcome: 'ok' });
    s.runRecords.push({ runId: 'run_2', artifactType: 'job', artifactId: 'j1', profileId: 'FIN-DEV-09', startedAt: 5, outcome: 'ok' });
    s.runRecords.push({ runId: 'run_3', artifactType: 'job', artifactId: 'j1', profileId: 'FIN-DEV-09', startedAt: 6, outcome: 'ok' });
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify(s));
    store = CygenixProfiles.cpLoad(); renderAll();
  });
  slots = await deleteSlots();
  const nine = slots.find((s) => s.id === 'FIN-DEV-09');
  check('a retired profile with history shows Delete DISABLED with the reason as tooltip',
    nine && nine.has && nine.disabled && nine.title === 'Kept for audit: 3 run records, 1 binding', JSON.stringify(nine));
  check('its binding and run records still render normally',
    await page.evaluate(() => /FIN-DEV-09/.test(document.querySelector('#cp-bindings tbody').textContent) && /FIN-DEV-09/.test(document.querySelector('#cp-runs tbody').textContent)));
  reqs.length = 0; dialogs.length = 0;
  await page.evaluate(() => cpDeleteProfile('FIN-DEV-09'));
  await page.waitForTimeout(300);
  check('forcing the call anyway is refused by the eligibility check before any request is made',
    dialogs.some((d) => d.type === 'alert' && /Kept for audit: 3 run records, 1 binding/.test(d.message)) && !reqs.some((r) => r.action === 'delete'),
    JSON.stringify({ dialogs, reqs }));
  check('and it is still there', (await store()).profiles.some((p) => p.id === 'FIN-DEV-09'));

  /* ── d. The delete ────────────────────────────────────────────────────── */
  for (let i = 0; i < 6; i++) { const n = reqs.length; await page.waitForTimeout(3500); if (reqs.length === n) break; }   // let the boot saves settle
  reqs.length = 0; dialogs.length = 0; answers.push('nope');
  await page.click('#cp-profiles tbody tr[data-profile-id="FIN-DEV-01"] button.btn-danger:not([disabled])');
  await page.waitForTimeout(400);
  check('Delete asks for the id typed, says what it removes, and a wrong answer changes nothing',
    dialogs.some((d) => d.type === 'prompt' && /Delete profile FIN-DEV-01/.test(d.message) && /Saved connections are not touched/.test(d.message) && /Type the profile id to confirm/.test(d.message))
    && dialogs.some((d) => d.type === 'alert' && /did not match FIN-DEV-01/.test(d.message))
    && (await store()).profiles.some((p) => p.id === 'FIN-DEV-01') && !reqs.some((r) => r.action === 'delete'),
    JSON.stringify(dialogs));

  await page.waitForTimeout(3100);      // the 3-second minimum interval
  dialogs.length = 0; reqs.length = 0; answers.push('FIN-DEV-01');
  await page.click('#cp-profiles tbody tr[data-profile-id="FIN-DEV-01"] button.btn-danger:not([disabled])');
  await page.waitForFunction(() => /Deleted FIN-DEV-01/.test((document.getElementById('cp-apply-msg') || {}).textContent || ''), null, { timeout: 10000 });
  const s1 = await store();
  check('the profile disappears from the store, leaving a tombstone',
    !s1.profiles.some((p) => p.id === 'FIN-DEV-01') && s1.deleted.some((d) => d.id === 'FIN-DEV-01' && d.by === U), JSON.stringify(s1.deleted));
  check('the row is gone and the counts update', (await rows()).indexOf('FIN-DEV-01') === -1 && (await summary()) === '3 active · 0 draft · 1 retired', await summary());
  const del = reqs.filter((r) => r.action === 'delete');
  check('ONE delete request went to the cloud, naming the profile, before the local removal',
    del.length === 1 && del[0].body && del[0].body.profileId === 'FIN-DEV-01', JSON.stringify(del));
  const aud = reqs.filter((r) => r.action === 'audit');
  check('ONE audit post: profile.delete with id, name, env and both standard names',
    aud.length === 1 && aud[0].body.action === 'profile.delete' && aud[0].body.category === 'connections'
    && aud[0].body.target.id === 'FIN-DEV-01' && aud[0].body.detail.name === 'Dev (old)' && aud[0].body.detail.envClass === 'DEV'
    && aud[0].body.detail.srcStandardName === 'DEV_SRC_FIN_FinDev' && aud[0].body.detail.tgtStandardName === 'DEV_TGT_FIN_FinDevTgt'
    && /^\d{4}-\d\d-\d\dT.*Z$/.test(aud[0].body.detail.deletedAtUtc), JSON.stringify(aud.map((a) => a.body)));
  check('no connection value reaches the audit body', !/mssql:\/\//.test(JSON.stringify(aud)));
  check('the message says what happened, including that connections were not changed',
    await page.evaluate(() => /Removed from this browser and from the cloud copy\. Saved connections were not changed/.test(document.getElementById('cp-apply-msg').textContent)));
  check('the saved connections are the same bytes as before', (await conns()) === connsBefore);
  await page.waitForTimeout(4000);
  const saves = reqs.filter((r) => r.action === 'save');
  check('the normal debounced save carried the tombstone up — once',
    saves.length >= 1 && saves.length <= 2 && saves.some((s) => s.body && s.body.connection_profiles && (s.body.connection_profiles.deleted || []).some((d) => d.id === 'FIN-DEV-01')),
    'saves=' + saves.length + ' ' + JSON.stringify(saves.map((s) => Object.keys(s.body || {}))));
  check('no second delete request, no request loop',
    reqs.filter((r) => r.action === 'delete').length === 1 && reqs.length < 8, JSON.stringify(reqs.map((r) => r.action)));

  await open();
  check('after a reload it is still gone', !(await store()).profiles.some((p) => p.id === 'FIN-DEV-01') && (await rows()).indexOf('FIN-DEV-01') === -1);

  /* the "second browser": the cloud answers with a copy uploaded by a
     machine that still holds FIN-DEV-01 as retired, and no tombstone */
  const old = JSON.parse(JSON.stringify(STORE));
  cloudLoad = { connection_profiles: old };
  reqs.length = 0;
  await open();
  await page.waitForTimeout(4000);
  const s2 = await store();
  check('a sync from a second browser that still holds it does NOT bring it back',
    !s2.profiles.some((p) => p.id === 'FIN-DEV-01') && (await rows()).indexOf('FIN-DEV-01') === -1, JSON.stringify(s2.profiles.map((p) => p.id)));
  check('and the tombstone is what this browser uploads to settle it',
    reqs.some((r) => r.action === 'save' && r.body && r.body.connection_profiles && (r.body.connection_profiles.deleted || []).some((d) => d.id === 'FIN-DEV-01')),
    JSON.stringify(reqs.map((r) => r.action)));
  cloudLoad = {};

  /* ── e (second half). The cloud refuses with 409 ──────────────────────── */
  await page.evaluate(() => {
    const s = JSON.parse(localStorage.getItem('cygenix_profiles_v1'));
    s.profiles.push({ id: 'FIN-DEV-08', name: 'Dev 8', envClass: 'DEV', status: 'retired', srcConnId: 'c_dsrc', tgtConnId: 'c_dtgt', createdAt: 1, updatedAt: 2, retiredAt: 2 });
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify(s));
    store = CygenixProfiles.cpLoad(); renderAll();
  });
  for (let i = 0; i < 6; i++) { const n = reqs.length; await page.waitForTimeout(3500); if (reqs.length === n) break; }
  deleteAnswer = { status: 409, body: { error: 'Kept for audit: 1 run record' } };
  reqs.length = 0; dialogs.length = 0; answers.push('FIN-DEV-08');
  await page.click('#cp-profiles tbody tr[data-profile-id="FIN-DEV-08"] button.btn-danger:not([disabled])');
  await page.waitForFunction(() => /Not deleted/.test((document.getElementById('cp-apply-msg') || {}).textContent || ''), null, { timeout: 10000 });
  check('when the cloud copy refuses with 409 (a run record on another machine), the page shows its reason and deletes nothing',
    await page.evaluate(() => /Not deleted\. Kept for audit: 1 run record/.test(document.getElementById('cp-apply-msg').textContent))
    && (await store()).profiles.some((p) => p.id === 'FIN-DEV-08') && !(await store()).deleted.some((d) => d.id === 'FIN-DEV-08')
    && !reqs.some((r) => r.action === 'audit'), JSON.stringify(reqs.map((r) => r.action)));
  deleteAnswer = { status: 200, body: { deleted: true, tombstoned: true } };

  /* ── the guards ───────────────────────────────────────────────────────── */
  dialogs.length = 0;
  await page.evaluate(() => { cpDeleteProfile('FIN-DEV-08'); cpDeleteProfile('FIN-DEV-08'); });
  await page.waitForTimeout(300);
  check('two clicks inside three seconds open ONE prompt', dialogs.filter((d) => d.type === 'prompt').length <= 1, JSON.stringify(dialogs));

  /* ── g. Quiet ─────────────────────────────────────────────────────────── */
  check('no page errors and no console errors', errors.length === 0, JSON.stringify(errors));

  await browser.close();
  server.close();
  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
