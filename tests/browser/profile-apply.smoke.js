/* tests/browser/profile-apply.smoke.js
 * ---------------------------------------------------------------------------
 * Selecting a profile loads its connections — in a real browser.
 *
 * tests/profile-apply.test.js proves the rules. What it cannot show is that
 * the radio on /profiles actually drives them, that the sentence appears
 * where a person is looking, that the top bar follows, that the dashboard's
 * Connections view shows the loaded values and opens the right side when a
 * credential is missing, that a Save there keeps it — and, the assertion the
 * brief singles out, that a switch makes ONE save to the data layer and not
 * a burst.
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/profile-apply.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8410);
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
  { id: 'c_afs',   side: 'src', mode: 'azure',  name: 'Azure Function Source', fnUrl: 'https://src.azurewebsites.net/api/db', fnKey: 'afs-key' },
  { id: 'c_cloud', side: 'tgt', mode: 'azure',  name: 'cygenix-cloud_new', fnUrl: 'https://cloud.azurewebsites.net/api/db', fnKey: 'cloud-key' },
  { id: 'c_conv',  side: 'src', mode: 'direct', name: 'Conversion', connString: 'mssql://u:p@conv:1433/Conversion' },
  { id: 'c_h3',    side: 'tgt', mode: 'direct', name: 'H Database 3', connString: 'mssql://u:p@h3:1433/HDB3' },
];

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  const token = 'x.' + Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: U })).toString('base64url') + '.y';
  const saves = [];
  await ctx.route('**', (route) => {
    const u = route.request().url();
    if (/action=whoami/.test(u)) return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ tier: 'pro', tier_status: 'active', role: 'user' }) });
    if (/action=save/.test(u)) { saves.push(u.replace(/^.*\/functions\//, '') + ' ' + Object.keys(JSON.parse(route.request().postData() || '{}')).join('+')); return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ saved: true, updatedAt: new Date().toISOString() }) }); }
    if (/data-proxy|netlify\/functions|\/api\//.test(u)) return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
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
    localStorage.setItem('cygenix_cookie_consent', JSON.stringify({ version: '1', essential: true, functional: true, timestamp: new Date().toISOString() }));
    localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify(acct));
    localStorage.setItem('h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-',
      JSON.stringify({ credentialType: 'IdToken', secret: arg.token, expiresOn: String(Math.floor(Date.now() / 1000) + 3600) }));
    const blob = {}; blob[arg.U] = arg.conns.map((c) => { const x = Object.assign({}, c); delete x.connString; delete x.fnKey; return x; });
    localStorage.setItem('cygenix_saved_connections', JSON.stringify(blob));
    const sec = {};
    arg.conns.forEach((c) => { const b = {}; if (c.connString) b.connString = c.connString; if (c.fnKey) b.fnKey = c.fnKey; sec[c.id] = b; });
    localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify(sec));
    const meta = { c_afs: { envClass: 'DEV' }, c_cloud: { envClass: 'DEV' }, c_conv: { envClass: 'UAT' }, c_h3: { envClass: 'UAT' } };
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify({ v: 1, connMeta: meta, bindings: [], runRecords: [], events: [],
      profiles: [
        { id: 'FIN-DEV-01', name: 'Dev', envClass: 'DEV', status: 'active', srcConnId: 'c_afs', tgtConnId: 'c_cloud', createdAt: 1, updatedAt: 1 },
        { id: 'FIN_3E_UAT', name: 'Finance UAT', envClass: 'UAT', status: 'active', srcConnId: 'c_conv', tgtConnId: 'c_h3', createdAt: 1, updatedAt: 1 },
        { id: 'FIN-PRD-01', name: 'Production', envClass: 'PRD', status: 'active', srcConnId: 'c_conv', tgtConnId: 'c_h3', createdAt: 1, updatedAt: 1 },
      ],
      settings: { envClasses: ['DEV', 'TEST', 'UAT', 'PRD', 'SANDBOX'], activeProfileId: null, selectedAt: 0 } }));
  }, { U, token, conns: CONNS });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  const answers = [];
  const dialogs = [];
  page.on('dialog', async (d) => {
    dialogs.push({ type: d.type(), message: d.message() });
    if (d.type() === 'prompt') return d.accept(answers.length ? answers.shift() : '');
    if (d.type() === 'confirm') return d.accept();
    return d.dismiss();
  });
  const live = () => page.evaluate(() => window.CygenixConnections.get());
  const msg = () => page.evaluate(() => (document.getElementById('cp-apply-msg') || {}).textContent || '');
  const select = async (id) => { await page.evaluate((i) => cpSelectActive(i), id); await page.waitForTimeout(400); };
  const openProfiles = async () => {
    await page.goto('http://localhost:' + PORT + '/profiles', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => !!window.CygenixProfileApply && !!window.CygenixConnections && document.querySelectorAll('input[name="cp-active"]').length > 0, null, { timeout: 20000 });
    // The seeded profile store is newer than the (empty) cloud, so the sync
    // layer's own boot merge queues a save, and its retry timer follows with
    // a full push once it finds nothing left dirty. Both are the sync layer's
    // boot behaviour, not the switch's. Wait until saves have been quiet for
    // four seconds, so the count below is the SWITCH's saves and nothing else's.
    for (let i = 0; i < 8; i++) { const n = saves.length; await page.waitForTimeout(4000); if (saves.length === n) break; }
  };

  console.log('Profiles — selecting one loads its connections\n');

  /* ── 1. Select FIN-DEV-01 ─────────────────────────────────────────────── */
  await openProfiles();
  const before = await live();
  check('the live settings start empty', !before.srcFnUrl && !before.srcConnString && !before.tgtFnUrl && !before.tgtConnString);
  saves.length = 0;
  await page.click('input[name="cp-active"][onchange*="FIN-DEV-01"]');
  await page.waitForTimeout(500);
  const l1 = await live();
  check('the radio loads both sides of FIN-DEV-01 into the live settings',
    l1.srcFnUrl === 'https://src.azurewebsites.net/api/db' && l1.srcFnKey === 'afs-key' && l1.srcConnMode === 'azure'
    && l1.tgtFnUrl === 'https://cloud.azurewebsites.net/api/db' && l1.tgtConnMode === 'azure', JSON.stringify(l1));
  check('the confirmation names the profile and both connections, where the person is looking',
    /Loaded FIN-DEV-01: Source = Azure Function Source, Target = cygenix-cloud_new/.test(await msg()), await msg());
  check('the top bar shows the selected profile',
    await page.evaluate(() => /FIN-DEV-01/.test((window.CygenixStatusHairline.current() || {}).label || '')));
  check('the radio is the selected one', await page.evaluate(() => document.querySelector('input[name="cp-active"]:checked').getAttribute('onchange').indexOf('FIN-DEV-01') !== -1));
  await page.waitForTimeout(4200);
  check('the switch made ONE save to the data layer, not a burst', saves.length === 1, 'saves=' + JSON.stringify(saves));
  check('…and that one save carried both the profile selection and the connections',
    saves.length === 1 && /(^|\+)connections(\+|$)/.test(saves[0].split(' ')[1] || '') && /connection_profiles/.test(saves[0]), (saves[0] || '').slice(0, 120));

  /* ── 2. Switch to FIN_3E_UAT ──────────────────────────────────────────── */
  saves.length = 0;
  await select('FIN_3E_UAT');
  const l2 = await live();
  check('FIN_3E_UAT replaces them with Conversion / H Database 3, no typing',
    l2.srcConnString === 'mssql://u:p@conv:1433/Conversion' && l2.srcConnMode === 'direct' && l2.srcFnUrl === ''
    && l2.tgtConnString === 'mssql://u:p@h3:1433/HDB3' && l2.tgtFnKey === '', JSON.stringify(l2));
  check('and says so', /Loaded FIN_3E_UAT: Source = Conversion, Target = H Database 3/.test(await msg()));
  check('no connection value reaches the page text',
    await page.evaluate(() => document.body.innerText.indexOf('mssql://') === -1 && document.body.innerText.indexOf('afs-key') === -1));

  /* ── 3. PRD asks for the id typed ─────────────────────────────────────── */
  dialogs.length = 0; answers.push('nope');
  await select('FIN-PRD-01');
  check('a PRD profile prompts for its id; a wrong answer leaves everything as it was',
    dialogs.some((d) => d.type === 'prompt' && /PRD profile \(FIN-PRD-01\)/.test(d.message))
    && dialogs.some((d) => d.type === 'alert' && /did not match/.test(d.message))
    && /Not switched/.test(await msg())
    && (await page.evaluate(() => JSON.parse(localStorage.getItem('cygenix_profiles_v1')).settings.activeProfileId)) === 'FIN_3E_UAT',
    JSON.stringify(dialogs));
  dialogs.length = 0; answers.push('FIN-PRD-01');
  await select('FIN-PRD-01');
  check('the right id selects it', (await page.evaluate(() => JSON.parse(localStorage.getItem('cygenix_profiles_v1')).settings.activeProfileId)) === 'FIN-PRD-01');
  check('the top bar locks red for it', await page.evaluate(() => (window.CygenixStatusHairline.current() || {}).level === 'red'));

  /* ── 4. A running job blocks ──────────────────────────────────────────── */
  await page.evaluate(() => localStorage.setItem('cygenix_jobs', JSON.stringify([{ id: 'j1', name: 'Nightly load', executionStatus: 'running' }])));
  await select('FIN-DEV-01');
  check('a running job blocks the switch and names the job',
    /Not switched/.test(await msg()) && /Nightly load/.test(await msg())
    && (await page.evaluate(() => JSON.parse(localStorage.getItem('cygenix_profiles_v1')).settings.activeProfileId)) === 'FIN-PRD-01');
  await page.evaluate(() => localStorage.setItem('cygenix_jobs', '[]'));

  /* ── 5. Hard refresh: the selection and the connections persist ──────── */
  await openProfiles();
  const l5 = await live();
  check('after a reload the live settings are still the selected profile\'s',
    l5.srcConnString === 'mssql://u:p@conv:1433/Conversion' && l5.tgtConnString === 'mssql://u:p@h3:1433/HDB3');
  check('and the radio is still on it', await page.evaluate(() => document.querySelector('input[name="cp-active"]:checked').getAttribute('onchange').indexOf('FIN-PRD-01') !== -1));
  check('the load-time check ran once and stamped the selection',
    await page.evaluate(() => /^FIN-PRD-01::\d+$/.test(sessionStorage.getItem('cygenix_profile_apply_seen') || '')));

  /* ── 6. The dashboard's Connections view shows where the values came from ─ */
  await page.goto('http://localhost:' + PORT + '/dashboard.html#goto=connections', { waitUntil: 'domcontentloaded' });
  await page.waitForFunction(() => !!window.CygenixSync && typeof initConnectionsView === 'function', null, { timeout: 20000 });
  await page.waitForTimeout(1500);
  await page.evaluate(() => { showView('connections'); initConnectionsView(); });
  const lines6 = await page.evaluate(() => ({ src: document.getElementById('src-conn-result').textContent, tgt: document.getElementById('tgt-conn-result').textContent,
    srcCs: document.getElementById('proj-src-cs').value, tgtCs: document.getElementById('proj-tgt-cs').value }));
  check('Connections shows the profile\'s values and names the profile they came from',
    /From profile FIN-PRD-01 · Conversion/.test(lines6.src) && /From profile FIN-PRD-01 · H Database 3/.test(lines6.tgt)
    && lines6.srcCs === 'mssql://u:p@conv:1433/Conversion' && lines6.tgtCs === 'mssql://u:p@h3:1433/HDB3', JSON.stringify(lines6));

  /* ── 7. A credential missing on this browser: finish it once ─────────── */
  await page.evaluate(() => {
    const s = JSON.parse(localStorage.getItem('cygenix_saved_conn_secrets')); delete s.c_h3;
    localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify(s));
  });
  await openProfiles();
  await select('FIN_3E_UAT');
  const l7 = await live();
  check('what exists is loaded; the missing side is left empty and named',
    l7.srcConnString === 'mssql://u:p@conv:1433/Conversion' && l7.tgtConnString === ''
    && /Target "H Database 3": no connection string on this browser/.test(await msg()), await msg());
  await page.goto('http://localhost:' + PORT + '/dashboard.html#goto=connections', { waitUntil: 'domcontentloaded' });
  await page.waitForFunction(() => !!window.CygenixSync && typeof initConnectionsView === 'function', null, { timeout: 20000 });
  await page.waitForTimeout(1500);
  await page.evaluate(() => { showView('connections'); initConnectionsView(); });
  const seven = await page.evaluate(() => ({ tgt: document.getElementById('tgt-conn-result').textContent,
    revealed: document.getElementById('proj-tgt-cs').offsetParent !== null || document.getElementById('tgt-b-host').offsetParent !== null }));
  check('Connections opens the target side and says which field to finish',
    /Finish "H Database 3" for profile FIN_3E_UAT: enter the connection string/.test(seven.tgt) && seven.revealed, JSON.stringify(seven));
  await page.evaluate(() => { setTgtMode('direct'); setConnEntry('tgt', 'paste'); document.getElementById('proj-tgt-cs').value = 'mssql://u:p@h3:1433/HDB3'; saveProjectConnections(); });
  await page.waitForTimeout(300);
  const kept = await page.evaluate(() => ({ line: document.getElementById('tgt-conn-result').textContent,
    secret: (JSON.parse(localStorage.getItem('cygenix_saved_conn_secrets')).c_h3 || {}).connString,
    synced: JSON.parse(localStorage.getItem('cygenix_saved_connections'))['you@example.test'].find((c) => c.id === 'c_h3'),
    live: window.CygenixConnections.get().tgtConnString }));
  check('a Save keeps it for that connection on this browser, says so, and leaves the synced entry untouched',
    /Kept for "H Database 3"/.test(kept.line) && kept.secret === 'mssql://u:p@h3:1433/HDB3' && !kept.synced.connString && kept.live === 'mssql://u:p@h3:1433/HDB3', JSON.stringify(kept));
  await openProfiles();
  await select('FIN-DEV-01');
  await select('FIN_3E_UAT');
  check('and it is never asked for again', !/no connection string/.test(await msg()) && (await live()).tgtConnString === 'mssql://u:p@h3:1433/HDB3');

  check('no page errors', errors.length === 0, errors.join(' | '));

  await browser.close();
  server.close();
  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
