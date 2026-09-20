/* tests/browser/conn-builder.smoke.js
 * ---------------------------------------------------------------------------
 * The Connections form, in a browser, doing the thing that lost a connection.
 *
 * tests/conn-builder.test.js pins the parser and the composer against the
 * real server parser. What it cannot show is the journey that actually
 * happened to somebody: a saved connection written as an mssql:// URL, the
 * Reveal & edit button, a Settings form that came up completely blank
 * because the parser did not know that spelling, and one keystroke that
 * composed the blank form back over the string. The connection was gone,
 * nothing said so, and the next Save persisted the wreckage.
 *
 * Everything below is that sequence, clicked rather than described, plus the
 * refusal that now stops a value which is not a connection from being saved
 * at all.
 *
 * Not part of `npm test`: it needs a browser.
 *   node tests/browser/conn-builder.smoke.js
 * Screenshots (optional): SHOTS=/path/to/dir
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8419);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const SHOTS = process.env.SHOTS || '';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.json': 'application/json', '.woff2': 'font/woff2' };

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
  if (p === '/auth-gate.js') {
    res.writeHead(200, { 'Content-Type': TYPES['.js'] });
    return res.end('/* stubbed: the connection form is under test, not the auth gate */');
  }
  if (ROUTES[p]) p = ROUTES[p];
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) {
    res.writeHead(404); return res.end('no');
  }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

const U = 'you@example.test';
// The password is the tripwire: if it is drawn on screen, or dropped on the
// way through the form, the checks below say so.
const PW = 'S3cret-pw';
const MS_URL = 'mssql://svc_fin:' + PW + '@fin-dm.database.windows.net:1433/FIN_DM';

const SEED = (a) => {
  const tok = 'x.' + btoa(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: a.U })).replace(/=+$/, '') + '.y';
  [localStorage, sessionStorage].forEach((s) => { s.setItem('cygenix_token', tok); s.setItem('cygenix_expires', String(Date.now() + 36e5)); });
  localStorage.setItem('cygenix_user', JSON.stringify({ email: a.U, name: 'A Tester' }));
  localStorage.setItem('cygenix_active_user', a.U);
  localStorage.setItem('cygenix_entra_account', JSON.stringify({ email: a.U, userId: a.U }));
  localStorage.setItem('cygenix_cookie_consent', 'all');
  localStorage.setItem('cygenix_onboarded', '1');
  localStorage.setItem('cygenix_tier', 'pro');
  const acct = { homeAccountId: 'h.t', environment: 'cygenix.ciamlogin.com', tenantId: 't', username: a.U, localAccountId: 'l', authorityType: 'MSSTS', name: 'A Tester' };
  localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify(acct));
  localStorage.setItem('h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-', JSON.stringify({ credentialType: 'IdToken', secret: tok, expiresOn: String(Math.floor(Date.now() / 1000) + 3600) }));
  sessionStorage.setItem('cygenix_rbac_me', JSON.stringify({ at: Date.now(), me: { oid: 'x', email: a.U, roles: ['OW', 'PA'] } }));
  localStorage.setItem('cygenix_projects', JSON.stringify([{ id: 'p1', name: 'Northbank Core' }]));
  localStorage.setItem('cygenix_active_project_id', 'p1');

  // The live source connection: a driver URL, which is what a great many of
  // them are, and the shape the builder could not read.
  const active = {};
  active[a.U] = { srcConnString: a.URL, srcConnMode: 'direct',
                  tgtConnString: '', tgtConnMode: 'direct' };
  localStorage.setItem('cygenix_project_connections', JSON.stringify(active));

  // One saved entry that is already wrong, of the kind that reached a real
  // profile: a label where the connection string should be.
  const saved = {};
  saved[a.U] = [{ id: 'c_bad', name: 'Finance source', side: 'src', mode: 'direct' }];
  localStorage.setItem('cygenix_saved_connections', JSON.stringify(saved));
  localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify({ c_bad: { connString: 'API' } }));
};

const PAGE_HOSTS = (() => {
  const html = fs.readFileSync(path.join(PUB, 'dashboard.html'), 'utf8');
  const hosts = new Set(['fonts.gstatic.com']);
  for (const m of html.matchAll(/https:\/\/([a-z0-9.-]+)\//gi)) hosts.add(m[1]);
  return [...hosts];
})();

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 1000 } });

  const offSite = [];
  await ctx.route('**', (route) => {
    const u = route.request().url();
    const json = (status, body) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
    if (/\/api\/health$/.test(u) || /functions\/health/.test(u)) return json(200, { status: 'ok' });
    // Refused rather than answered empty: "cloud wins on page load" would
    // take an empty document as the truth and wipe the seed.
    if (/functions\/data-proxy/.test(u)) {
      if (/action=whoami/.test(u)) return json(200, { email: U, role: 'user', tier: 'pro', tier_status: 'active', exists: true });
      return route.abort();
    }
    if (/functions\/(audit|rbac-admin|projects|scheduler|db-connect)/.test(u)) return json(200, {});
    if (u.startsWith('http://localhost:' + PORT)) return route.continue();
    try { if (PAGE_HOSTS.indexOf(new URL(u).host) === -1) offSite.push(u); } catch (e) { offSite.push(u); }
    return route.abort();
  });

  const page = await ctx.newPage();
  const errors = [], consoleText = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('console', (m) => {
    consoleText.push(m.text());
    if (m.type() === 'error' && !/ERR_|Failed to load resource|CygenixSync/.test(m.text())) errors.push(m.text());
  });

  // sconnSaveAs asks for a name, then an environment class, both with
  // prompt(). Anything it refuses arrives as an alert() instead, so the
  // dialog log is how the refusals are read.
  const dialogs = [];
  let promptAnswers = [];
  page.on('dialog', async (d) => {
    dialogs.push({ type: d.type(), message: d.message() });
    if (d.type() === 'prompt') return d.accept(promptAnswers.shift() || '');
    if (d.type() === 'confirm') return d.accept();
    return d.dismiss();
  });

  await page.addInitScript(SEED, { U, URL: MS_URL });

  const shot = async (name) => {
    if (!SHOTS) return;
    await page.setViewportSize({ width: 1440, height: 1600 });
    await page.waitForTimeout(150);
    await page.screenshot({ path: path.join(SHOTS, name) });
    await page.setViewportSize({ width: 1440, height: 1000 });
  };
  const stored = () => page.$eval('#proj-src-cs', (e) => e.value);
  const field = (n) => page.$eval('#src-b-' + n, (e) => e.value);
  const savedEntries = () => page.evaluate((u) => {
    const all = JSON.parse(localStorage.getItem('cygenix_saved_connections') || '{}');
    return (all[u] || []).map((e) => ({ id: e.id, name: e.name, mode: e.mode }));
  }, U);
  const secretOf = (id) => page.evaluate((i) => {
    const s = JSON.parse(localStorage.getItem('cygenix_saved_conn_secrets') || '{}');
    return (s[i] && s[i].connString) || '';
  }, id);

  console.log('Connections form — the round trip that lost a connection\n');

  await page.goto('http://localhost:' + PORT + '/dashboard', { waitUntil: 'domcontentloaded' });
  await page.waitForFunction(() => typeof window.showView === 'function', null, { timeout: 20000 });
  await page.evaluate(() => {
    window.getCygenixIdToken = () => 'smoke-token';
    window.showView('connections');
  });
  await page.waitForSelector('#src-conn-locked', { state: 'attached', timeout: 10000 });

  // ── 1. A stored driver URL, opened for editing ──────────────────────────
  console.log('1. Reveal and edit');
  check('the source arrives masked, with only the database named',
    await page.isVisible('#src-conn-locked')
    && (await page.textContent('#src-conn-locked-value')).trim() === 'FIN_DM');
  check('and the connection string itself is what was stored',
    await stored() === MS_URL);

  await page.click('#src-conn-locked button');           // Reveal & edit
  await page.waitForSelector('#src-build-wrap', { state: 'visible', timeout: 5000 });
  check('THE FORM COMES UP FILLED IN, not blank — the parser knows this spelling',
    await field('host') === 'fin-dm.database.windows.net'
    && await field('db') === 'FIN_DM' && await field('user') === 'svc_fin'
    && await field('port') === '1433',
    JSON.stringify({ host: await field('host'), db: await field('db'), user: await field('user') }));
  check('opening it changed nothing, because opening is not editing',
    await stored() === MS_URL);
  check('the preview masks the password rather than drawing it',
    !(await page.textContent('#src-b-preview')).includes(PW)
    && (await page.textContent('#src-b-preview')).includes('fin-dm.database.windows.net'));
  await shot('conn-1-revealed.png');

  // ── 2. One keystroke ────────────────────────────────────────────────────
  console.log('\n2. Editing a field');
  await page.fill('#src-b-db', 'FIN_DM_2');
  await page.waitForTimeout(150);
  const afterEdit = await stored();
  check('THE EDIT LANDS AND THE CONNECTION SURVIVES IT',
    afterEdit.indexOf('mssql://') === 0 && afterEdit.indexOf('/FIN_DM_2') !== -1, afterEdit);
  check('the credential is still in it — the form did not quietly drop what it read',
    afterEdit.indexOf('svc_fin') !== -1 && afterEdit.indexOf(encodeURIComponent(PW)) !== -1
      || afterEdit.indexOf(PW) !== -1);
  check('and it is still a URL, not silently rewritten into keyword form',
    afterEdit.indexOf('Server=') === -1);

  // ── 3. An incomplete form is not an instruction to erase ────────────────
  console.log('\n3. Clearing a field');
  await page.fill('#src-b-host', '');
  await page.waitForTimeout(150);
  check('EMPTYING THE HOST DOES NOT DELETE THE STORED CONNECTION',
    await stored() === afterEdit, await stored());
  check('and the preview still shows what is stored rather than claiming nothing is',
    (await page.textContent('#src-b-preview')).includes('fin-dm.database.windows.net'));
  await page.fill('#src-b-host', 'fin-dm.database.windows.net');
  await page.waitForTimeout(150);
  check('typing the host back resumes writing through',
    (await stored()).indexOf('fin-dm.database.windows.net') !== -1);

  // ── 4. What may be saved ────────────────────────────────────────────────
  console.log('\n4. Save as…');
  const before = (await savedEntries()).length;
  await page.click('#src-entry-paste');
  await page.fill('#proj-src-cs', 'API');
  dialogs.length = 0;
  promptAnswers = ['Finance API', 'DEV'];
  await page.click('button[onclick="sconnSaveAs(\'src\')"]');
  await page.waitForTimeout(250);
  check('SAVING A LABEL IS REFUSED, before it is ever written',
    dialogs.length === 1 && dialogs[0].type === 'alert'
    && /That is not a connection string/.test(dialogs[0].message), JSON.stringify(dialogs));
  check('and the refusal names the shapes that would work',
    /mssql:\/\/user:pass@host:1433\/database/.test(dialogs[0].message || ''));
  check('nothing was added to the store',
    (await savedEntries()).length === before);
  check('it never even asked for a name — the refusal comes first',
    dialogs.every((d) => d.type !== 'prompt'));

  await page.fill('#proj-src-cs', MS_URL);
  dialogs.length = 0;
  promptAnswers = ['Finance source URL', 'DEV'];
  await page.click('button[onclick="sconnSaveAs(\'src\')"]');
  await page.waitForTimeout(300);
  const after = await savedEntries();
  check('a real connection saves, and is asked about rather than refused',
    after.length === before + 1 && dialogs.some((d) => d.type === 'prompt'),
    JSON.stringify(dialogs.map((d) => d.type)));
  const fresh = after.filter((e) => e.name === 'Finance source URL')[0];
  check('and what was stored is the connection, whole',
    !!fresh && await secretOf(fresh.id) === MS_URL);
  check('the credential stayed out of the synced half',
    !JSON.stringify(after).includes(PW));

  // ── 5. An entry that is already wrong says so ───────────────────────────
  console.log('\n5. The one that is already broken');
  const chips = await page.$$eval('#sconn-src-list .sconn-chip, #sconn-src-list [data-sconn-id], #sconn-src-list > *',
    (els) => els.map((e) => e.textContent.replace(/\s+/g, ' ').trim()));
  const badChip = chips.filter((t) => /Finance source(?!\s*URL)/.test(t))[0] || chips.join(' | ');
  check('the entry holding a label is marked for checking, not vouched for as MSSQL',
    /CHECK/.test(badChip), badChip);
  check('and its tooltip says what to do about it',
    await page.evaluate(() => window.sconnPreview
      ? window.sconnPreview({ mode: 'direct', connString: 'API' })
      : document.querySelector('#sconn-src-list [title]')?.getAttribute('title') || '')
      === 'Not a connection — re-save it');
  await shot('conn-2-chips.png');

  // ── 6. Hygiene ──────────────────────────────────────────────────────────
  console.log('\n6. Hygiene');
  const pageText = await page.textContent('#view-connections');
  check('NO PASSWORD IS ANYWHERE ON THE PAGE', !pageText.includes(PW));
  check('and none was written to the console', consoleText.every((t) => !t.includes(PW)));
  check('no uncaught errors on any of that', errors.length === 0, errors.slice(0, 3).join(' | '));
  check('nothing reached off-site', offSite.length === 0, offSite.slice(0, 3).join(' | '));

  await browser.close();
  server.close();
  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
