/* tests/browser/org-connections.smoke.js
 * ---------------------------------------------------------------------------
 * The organisation's connection register (Phase A) on the Connections view,
 * in a real browser. tests/org-connections.test.js proves the server rules
 * and the module's guards; this proves the page: that a Member sees the
 * register and no New button, that a Platform Administrator sees the button,
 * fills the form, and the record lands in the table after ONE request with
 * no secret field in the body; that a refused retirement is said on the
 * page in the server's words; and that nothing loops.
 *
 * Not part of `npm test`: it needs a browser.
 *   node tests/browser/org-connections.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8424);
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

const U = 'demo@cygenix.onmicrosoft.com';

// The world behind the Netlify functions: one register, a role switch, a
// holders switch for the lock rule, and a count of every request.
const world = { roles: ['MB'], holders: 0, counts: {}, bodies: [] };
const bump = (k) => { world.counts[k] = (world.counts[k] || 0) + 1; };
const REGISTER = [
  { id: 'conn_a', name: 'Finance prod source', side: 'src', kind: 'sqlserver', envClass: 'PRD', server: 'sqlprod01.internal', port: 1433, database: 'Conversion_DM', authType: 'sql', userName: 'svc_cyg_fin', aliases: ['FIN-PRD'], secretRef: null, retiredAt: null },
  { id: 'conn_b', name: 'Product Function App', side: 'tgt', kind: 'azurefn', envClass: 'DEV', endpoint: 'https://cygenix-db-api-x.uksouth-01.azurewebsites.net/api/db', authType: 'key', aliases: [], secretRef: 'sec_1', secretUpdatedAt: '2026-09-20T10:00:00Z', secretUpdatedBy: 'admin@cygenix.onmicrosoft.com', retiredAt: null },
];

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 960 } });
  const token = 'x.' + Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: U, oid: 'oid-demo' })).toString('base64url') + '.y';

  await ctx.route('**', (route) => {
    const u = route.request().url();
    const json = (status, body) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
    const q = (() => { try { return new URL(u).searchParams; } catch (e) { return new URLSearchParams(); } })();
    if (u.indexOf('data-proxy') !== -1) {
      const action = q.get('action') || '';
      bump('proxy:' + action);
      if (action === 'whoami') return json(200, { tier: 'pro', tier_status: 'active', role: 'user' });
      if (action === 'load') return json(200, {});
      if (action === 'save') return json(200, { saved: true, updatedAt: new Date().toISOString(), fields: [], ignored: [] });
      if (action === 'secrets-list') return json(200, { secrets: {}, undecryptable: [], keyVersion: '1' });
      if (action === 'blob-credential') return json(200, { base: 'https://x/api/data', code: 'K' });
      return json(200, {});
    }
    if (/functions\/rbac-admin\?what=me/.test(u)) {
      bump('rbac:me');
      return json(200, { oid: 'oid-demo', email: U, roles: world.roles.slice(), roleNames: [], collapsed: [] });
    }
    if (/functions\/org-connections/.test(u)) {
      const m = route.request().method();
      bump('register:' + m);
      const auth = route.request().headers()['authorization'] || '';
      if (!/^Bearer /.test(auth)) return json(401, { error: 'no token' });
      if (m === 'GET') return json(200, { connections: REGISTER.filter((c) => !c.retiredAt) });
      const body = (() => { try { return JSON.parse(route.request().postData() || '{}'); } catch (e) { return {}; } })();
      world.bodies.push(body);
      if (!world.roles.includes('PA')) return json(403, { error: 'Not permitted: no grant for connection.' + body.op, code: 'denied' });
      if (body.op === 'create') {
        const c = body.connection || {};
        if (!c.name || !c.server) return json(400, { error: 'name is required (1-120 characters)', code: 'upstream-refused' });
        const rec = Object.assign({ id: 'conn_new', aliases: [], secretRef: null, retiredAt: null, port: Number(c.port) || 1433 }, c);
        REGISTER.push(rec);
        return json(200, { connection: rec });
      }
      if (body.op === 'retire') {
        const rec = REGISTER.find((c) => c.id === body.id);
        if (!rec) return json(404, { error: 'No such connection', code: 'upstream-refused' });
        if (world.holders) return json(409, { error: 'Locked: ' + world.holders + ' member still has a non-retired profile bound to this connection. Retire those profiles first.', code: 'upstream-refused' });
        rec.retiredAt = new Date().toISOString();
        return json(200, { connection: rec });
      }
      return json(400, { error: 'Unknown op' });
    }
    if (/netlify\/functions|\/api\//.test(u)) return json(200, {});
    if (u.startsWith('http://localhost:' + PORT)) return route.continue();
    return route.abort();
  });

  await ctx.addInitScript((arg) => {
    if (localStorage.getItem('cygenix_onboarded')) return;
    [localStorage, sessionStorage].forEach((s) => { s.setItem('cygenix_token', arg.token); s.setItem('cygenix_expires', String(Date.now() + 3600e3)); });
    localStorage.setItem('cygenix_onboarded', 'true');
    localStorage.setItem('cygenix_user', JSON.stringify({ email: arg.U }));
    localStorage.setItem('cygenix_active_user', arg.U);
    localStorage.setItem('cygenix_tier', 'pro');
    localStorage.setItem('cygenix_cookie_consent', JSON.stringify({ version: '2', essential: true, functional: true, analytics: false, timestamp: new Date().toISOString() }));
    localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify({ homeAccountId: 'h.t', environment: 'cygenix.ciamlogin.com', tenantId: 't', username: arg.U, localAccountId: 'l', authorityType: 'MSSTS', name: 'Demo' }));
    localStorage.setItem('h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-',
      JSON.stringify({ credentialType: 'IdToken', secret: arg.token, expiresOn: String(Math.floor(Date.now() / 1000) + 3600) }));
  }, { U, token });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('console', (m) => {
    if (m.type() !== 'error') return;
    if (/Failed to load resource|ERR_/.test(m.text())) return;
    errors.push('[console] ' + m.text());
  });
  page.on('dialog', (d) => d.accept());

  // A goto to a URL that differs only in its fragment is a same-document
  // navigation and reloads nothing, so each open carries its own query.
  let opens = 0;
  const openConnections = async () => {
    await page.goto('http://localhost:' + PORT + '/dashboard.html?open=' + (++opens) + '#goto=connections', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => !!window.CygenixSync && !!window.CygenixOrgConnections && !!window.CygenixRBAC && typeof initConnectionsView === 'function', null, { timeout: 20000 });
    await page.waitForTimeout(1500);
    await page.evaluate(() => { showView('connections'); initConnectionsView(); });
    await page.waitForFunction(() => /orgconn-table|No organisation connections|could not be read/.test(document.getElementById('orgconn-list').innerHTML), null, { timeout: 10000 });
    await page.waitForTimeout(400);
  };
  const rows = () => page.evaluate(() => [...document.querySelectorAll('#orgconn-list tr[data-orgconn-id]')].map((tr) => ({ id: tr.dataset.orgconnId, text: tr.textContent.replace(/\s+/g, ' ').trim(), retire: !!tr.querySelector('button') })));
  const visible = (sel) => page.evaluate((s) => { const el = document.querySelector(s); return !!el && el.style.display !== 'none' && el.offsetParent !== null; }, sel);

  console.log('Organisation connection register — in a browser\n');

  /* ── 1. A Member ─────────────────────────────────────────────────────── */
  console.log('1. A Member sees the register, read-only');
  await openConnections();
  let r = await rows();
  check('the register panel lists the two organisation connections', r.length === 2 && r[0].id === 'conn_a' && r[1].id === 'conn_b', JSON.stringify(r));
  check('the counter says 2', (await page.textContent('#orgconn-count')).trim() === '2');
  check('a SQL Server row shows kind, host/db, PRD tag, SQL login, the user name and the alias',
    /SQL Server/.test(r[0].text) && /sqlprod01\.internal\/Conversion_DM/.test(r[0].text) && /PRD/.test(r[0].text) && /SQL login/.test(r[0].text) && /svc_cyg_fin/.test(r[0].text) && /FIN-PRD/.test(r[0].text), r[0].text);
  check('an Azure Function row shows the endpoint and that a secret is saved — by whom, never what',
    /Azure Function/.test(r[1].text) && /azurewebsites\.net\/api\/db/.test(r[1].text) && /Saved/.test(r[1].text) && /admin@cygenix/.test(r[1].text) && !/sec_1/.test(r[1].text), r[1].text);
  check('NO RETIRE BUTTON AND NO NEW BUTTON for a Member', !r.some((x) => x.retire) && !(await visible('#orgconn-new-btn')));
  // The sidebar asks rbac-admin?what=me too (for the Audit item) and shares
  // the sessionStorage cache; on a slow load both can fire before either
  // answer lands. Two is the ceiling; the register module never adds a
  // third.
  check('the list was fetched once and the roles asked at most twice (sidebar + register share one cache)',
    world.counts['register:GET'] === 1 && world.counts['rbac:me'] >= 1 && world.counts['rbac:me'] <= 2, JSON.stringify(world.counts));
  const rolesAsked = world.counts['rbac:me'];

  /* ── 2. Not a loop ───────────────────────────────────────────────────── */
  console.log('\n2. Opening the view again is answered from cache');
  await page.evaluate(() => { showView('jobs'); showView('connections'); initConnectionsView(); });
  await page.waitForTimeout(1500);
  check('no second list request inside thirty seconds, no further roles request', world.counts['register:GET'] === 1 && world.counts['rbac:me'] === rolesAsked, JSON.stringify(world.counts));

  /* ── 3. A Platform Administrator ─────────────────────────────────────── */
  console.log('\n3. A Platform Administrator adds one');
  world.roles = ['PA'];
  await page.evaluate(() => { sessionStorage.removeItem('cygenix_rbac_me'); });
  await openConnections();
  check('the New button is shown', await visible('#orgconn-new-btn'));
  r = await rows();
  check('and every row has Retire', r.length === 2 && r.every((x) => x.retire));
  await page.click('#orgconn-new-btn');
  check('the form opens with the SQL fields and without the Function URL', await visible('#orgconn-form') && await visible('#orgconn-f-server') && !(await visible('#orgconn-f-endpoint')));
  await page.selectOption('#orgconn-f-kind', 'azurefn');
  check('choosing Azure Function swaps the fields', await visible('#orgconn-f-endpoint') && !(await visible('#orgconn-f-server')));
  await page.selectOption('#orgconn-f-kind', 'sqlserver');
  check('the form has no password field', await page.evaluate(() => !document.querySelector('#orgconn-form input[type="password"]')));
  await page.fill('#orgconn-f-name', 'HR test target');
  await page.selectOption('#orgconn-f-side', 'tgt');
  await page.selectOption('#orgconn-f-env', 'TEST');
  await page.fill('#orgconn-f-server', 'sqltest02.internal');
  await page.fill('#orgconn-f-database', 'HR_Conv');
  await page.fill('#orgconn-f-user', 'svc_hr');
  await page.click('#orgconn-save-btn');
  await page.waitForFunction(() => document.querySelectorAll('#orgconn-list tr[data-orgconn-id]').length === 3, null, { timeout: 10000 });
  r = await rows();
  check('THE RECORD LANDS IN THE TABLE after one POST, and the form closes',
    r.some((x) => /HR test target/.test(x.text) && /sqltest02\.internal\/HR_Conv/.test(x.text) && /TEST/.test(x.text)) && world.counts['register:POST'] === 1 && !(await visible('#orgconn-form')), JSON.stringify(r));
  const sent = world.bodies[0];
  check('the body is { op:"create", connection } carrying name, side, kind, env, server, database, auth, user — and nothing secret',
    sent && sent.op === 'create' && sent.connection.name === 'HR test target' && sent.connection.side === 'tgt' && sent.connection.envClass === 'TEST'
    && sent.connection.authType === 'sql' && sent.connection.userName === 'svc_hr' && !('password' in sent.connection) && !('connString' in sent.connection), JSON.stringify(sent));
  check('the counter says 3', (await page.textContent('#orgconn-count')).trim() === '3');

  console.log('\n4. A refused save is shown in the form, and the second click is not a second request');
  await page.click('#orgconn-new-btn');
  await page.fill('#orgconn-f-name', 'Missing server');
  await page.fill('#orgconn-f-database', 'x');
  await page.click('#orgconn-save-btn');
  await page.waitForTimeout(300);
  const err1 = await page.textContent('#orgconn-form-err');
  check('inside three seconds of the last write the module says wait, not a request', /Wait a moment/.test(err1) && world.counts['register:POST'] === 1, err1);
  await page.waitForTimeout(3200);
  await page.click('#orgconn-save-btn');
  await page.waitForFunction(() => /required/.test(document.getElementById('orgconn-form-err').textContent), null, { timeout: 5000 });
  check('after the interval the server\'s refusal is shown verbatim in the form', /name is required/.test(await page.textContent('#orgconn-form-err')) && world.counts['register:POST'] === 2);
  await page.click('#orgconn-new-btn');   // close

  /* ── 5. Retire, locked and then free ─────────────────────────────────── */
  console.log('\n5. Retire');
  world.holders = 1;
  await page.waitForTimeout(3200);
  await page.click('#orgconn-list tr[data-orgconn-id="conn_a"] button');
  await page.waitForFunction(() => /Not retired/.test(document.getElementById('orgconn-status').textContent), null, { timeout: 5000 });
  const st = await page.textContent('#orgconn-status');
  check('A LOCKED RETIREMENT IS SAID ON THE PAGE in the server\'s words', /Not retired/.test(st) && /1 member still has a non-retired profile/.test(st), st);
  check('and the row is still there', (await rows()).some((x) => x.id === 'conn_a'));
  world.holders = 0;
  await page.waitForTimeout(3200);
  await page.click('#orgconn-list tr[data-orgconn-id="conn_a"] button');
  await page.waitForFunction(() => !document.querySelector('#orgconn-list tr[data-orgconn-id="conn_a"]'), null, { timeout: 5000 });
  check('with no holders it retires and leaves the list', !(await rows()).some((x) => x.id === 'conn_a') && (await page.textContent('#orgconn-count')).trim() === '2');

  /* ── 6. Signed-in but refused ────────────────────────────────────────── */
  console.log('\n6. Nothing is kept in the browser');
  check('no localStorage key holds the register', await page.evaluate(() => !Object.keys(localStorage).some((k) => /orgconn|org_conn|org-conn/i.test(k) || /conn_a|sqlprod01/.test(localStorage.getItem(k) || ''))));

  console.log('\n7. Quiet');
  check('no page errors and no console errors', errors.length === 0, JSON.stringify(errors));

  await browser.close();
  server.close();
  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
