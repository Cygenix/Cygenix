/* tests/browser/connection-recovery.smoke.js
 * ---------------------------------------------------------------------------
 * After a cookie clear, in a real browser: the credential store is broken on
 * the server, the target URL has no key, and a save carries a field the
 * server does not list. Each of those must be SAID on the page, and the one
 * that can be fixed from the browser — the product's own Function App key —
 * must be fixed without anyone typing it.
 *
 * tests/connection-recovery.test.js proves the rules. This proves the
 * Connections view shows the red box, that the key lands in the form and the
 * blob after ONE request, that Test then reaches the (mocked) Function App
 * and reports Connected, that the sync banner names the field rather than
 * saying "refused", and that none of it loops.
 *
 * Not part of `npm test`: it needs a browser.
 *   node tests/browser/connection-recovery.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8423);
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
const PRODUCT_HOST = 'cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net';
const PRODUCT_DB = 'https://' + PRODUCT_HOST + '/api/db';
const HOSTKEY = 'HOSTKEY-FROM-PROXY';

// What the cloud holds after a cookie clear: the pair came back with a URL
// and no key (the strip on upload removed it), the credential store has
// nothing, and the server's own secrets key is missing.
const CLOUD = {
  connections: { [U]: { srcConnString: '', srcConnMode: 'direct', srcFnUrl: '', tgtConnMode: 'azure', tgtFnUrl: PRODUCT_DB } },
  saved_connections: { [U]: [] },
};

const world = { secretsMode: '503', saveMode: 'skew', counts: {} };
const bump = (k) => { world.counts[k] = (world.counts[k] || 0) + 1; };

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 960 } });
  const token = 'x.' + Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: U, oid: 'oid-demo' })).toString('base64url') + '.y';

  await ctx.route('**', (route) => {
    const u = route.request().url();
    const json = (status, body) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
    const q = (() => { try { return new URL(u).searchParams; } catch (e) { return new URLSearchParams(); } })();
    const action = q.get('action') || '';
    if (u.indexOf('data-proxy') !== -1) {
      bump(action);
      if (action === 'whoami') return json(200, { tier: 'pro', tier_status: 'active', role: 'user' });
      if (action === 'load') return json(200, CLOUD);
      if (action === 'save') {
        const body = (() => { try { return JSON.parse(route.request().postData() || '{}'); } catch (e) { return {}; } })();
        const KNOWN = ['jobs', 'project_settings', 'project_plan', 'connections', 'saved_connections', 'performance', 'validation_sources',
          'wasis_rules', 'sql_scripts', 'issues', 'inventory', 'sys_params', 'projects', 'conv_project', 'last_snapshots', 'connection_profiles'];
        const touched = Object.keys(body).filter((k) => KNOWN.includes(k) || world.saveMode === 'fixed');
        const ignored = Object.keys(body).filter((k) => !touched.includes(k));
        if (!touched.length) return json(200, { saved: false, reason: 'no-syncable-fields', ignored });
        return json(200, { saved: true, updatedAt: new Date().toISOString(), fields: touched, ignored });
      }
      if (action === 'secrets-list') {
        if (world.secretsMode === '503') return json(503, { error: 'secrets store not configured', code: 'no-secrets-key' });
        return json(200, { secrets: {}, undecryptable: [], keyVersion: '1' });
      }
      if (action === 'secrets-put' || action === 'secrets-prune' || action === 'secrets-delete') {
        if (world.secretsMode === '503') return json(503, { error: 'secrets store not configured', code: 'no-secrets-key' });
        return json(200, { ok: true });
      }
      if (action === 'blob-credential') return json(200, { base: 'https://' + PRODUCT_HOST + '/api/data', code: HOSTKEY });
      return json(200, {});
    }
    // The Function App's db route, called directly by the browser.
    if (u.startsWith(PRODUCT_DB)) {
      bump('api/db' + (u.indexOf('code=') !== -1 ? ':keyed' : ':bare'));
      if (u.indexOf('code=' + HOSTKEY) === -1) return route.fulfill({ status: 401, contentType: 'text/plain', body: 'Unauthorized' });
      return json(200, { success: true, version: 'SQL Server 2022', database: 'FinConv', user: 'svc_cyg@x' });
    }
    if (/netlify\/functions|\/api\//.test(u)) return json(200, {});
    if (u.startsWith('http://localhost:' + PORT)) return route.continue();
    return route.abort();
  });

  await ctx.addInitScript((arg) => {
    // A cookie clear: NOTHING seeded but the sign-in itself.
    if (localStorage.getItem('cygenix_onboarded')) return;
    [localStorage, sessionStorage].forEach((s) => { s.setItem('cygenix_token', arg.token); s.setItem('cygenix_expires', String(Date.now() + 3600e3)); });
    localStorage.setItem('cygenix_onboarded', 'true');
    localStorage.setItem('cygenix_user', JSON.stringify({ email: arg.U }));
    localStorage.setItem('cygenix_active_user', arg.U);
    localStorage.setItem('cygenix_tier', 'pro');
    localStorage.setItem('cygenix_cookie_consent', JSON.stringify({ version: '1', essential: true, functional: true, timestamp: new Date().toISOString() }));
    localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify({ homeAccountId: 'h.t', environment: 'cygenix.ciamlogin.com', tenantId: 't', username: arg.U, localAccountId: 'l', authorityType: 'MSSTS', name: 'Demo' }));
    localStorage.setItem('h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-',
      JSON.stringify({ credentialType: 'IdToken', secret: arg.token, expiresOn: String(Math.floor(Date.now() / 1000) + 3600) }));
  }, { U, token });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  // Section 5 deliberately provokes a save the server does not list, and the
  // sync layer logs that at error level ON PURPOSE — a deployment fault must
  // be loud in the console as well as on the page. That one line is the
  // behaviour under test, not a defect; everything else the console calls an
  // error still is.
  page.on('console', (m) => {
    if (m.type() !== 'error') return;
    if (/Failed to load resource|ERR_/.test(m.text())) return;
    if (/\[CygenixSync\] save rejected by the server: .*no-syncable-fields/.test(m.text())) return;
    errors.push('[console] ' + m.text());
  });

  const openConnections = async () => {
    await page.goto('http://localhost:' + PORT + '/dashboard.html#goto=connections', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => !!window.CygenixSync && !!window.CygenixConnections && typeof initConnectionsView === 'function', null, { timeout: 20000 });
    await page.waitForTimeout(2500);        // boot load, secrets sync (600ms), key fill (900ms)
    await page.evaluate(() => { showView('connections'); initConnectionsView(); });
    await page.waitForTimeout(300);
  };
  const live = () => page.evaluate(() => window.CygenixConnections.get());
  const box = () => page.evaluate(() => { const b = document.getElementById('conn-cloud-status'); return { shown: b && b.style.display !== 'none', text: b ? b.textContent.replace(/\s+/g, ' ').trim() : '' }; });

  console.log('Connection recovery — after a cookie clear, in a browser\n');

  /* ── 1. The store failure is on the page ─────────────────────────────── */
  console.log('1. The broken credential store is said');
  await openConnections();
  const b1 = await box();
  check('THE RED BOX IS SHOWN and names CONN_SECRETS_KEY and the Function App',
    b1.shown && /CONN_SECRETS_KEY/.test(b1.text) && /Function App/.test(b1.text), JSON.stringify(b1));
  check('it says what it means — kept on this device', /this device/.test(b1.text));
  check('the module agrees', await page.evaluate(() => window.CygenixSavedConnSecrets.cloudStatus().state) === 'unavailable');

  /* ── 2. The key was fetched, once, and only for the product host ─────── */
  console.log('\n2. The Function App key arrives without being typed');
  const l2 = await live();
  check('the live target came from the cloud as a URL with no key (the 401 case) and now HAS the key',
    l2.tgtFnUrl === PRODUCT_DB && l2.tgtFnKey === HOSTKEY, JSON.stringify(l2));
  check('tgtConn carries ?code=', await page.evaluate(() => /code=HOSTKEY-FROM-PROXY/.test(window.CygenixConnections.tgtConn)));
  check('the form shows it too', (await page.inputValue('#proj-tgt-fn-key')) === HOSTKEY);
  check('EXACTLY ONE blob-credential request, across the boot load, the secrets sync and the view open',
    world.counts['blob-credential'] === 1, JSON.stringify(world.counts));
  check('and it is mirrored into the credential store under the live id',
    await page.evaluate(() => (window.CygenixSavedConnSecrets.get('sconn_live_tgt') || {}).fnKey) === HOSTKEY);

  /* ── 3. Test now reaches the Function App ────────────────────────────── */
  console.log('\n3. Test');
  await page.evaluate(() => testProjConn('tgt'));
  await page.waitForFunction(() => /Connected|refused|No function key|Unauthorized/.test(document.getElementById('tgt-conn-result').textContent), null, { timeout: 15000 });
  const t3 = await page.textContent('#tgt-conn-result');
  check('Test on the target reports Connected — the request carried the key', /Connected · FinConv/.test(t3), t3);
  check('no bare (unkeyed) request ever reached /api/db', !world.counts['api/db:bare'], JSON.stringify(world.counts));

  /* ── 4. Nothing loops ─────────────────────────────────────────────────── */
  console.log('\n4. Not a loop');
  const before = Object.assign({}, world.counts);
  await page.evaluate(() => { showView('jobs'); showView('connections'); initConnectionsView(); });
  await page.waitForTimeout(4500);
  check('switching views and waiting fetches no more keys and no more secrets lists',
    world.counts['blob-credential'] === before['blob-credential'] && (world.counts['secrets-list'] || 0) <= (before['secrets-list'] || 0) + 1,
    JSON.stringify({ before, after: world.counts }));

  /* ── 5. The banner names the field ───────────────────────────────────── */
  console.log('\n5. A save the server does not understand');
  await page.evaluate(() => { localStorage.setItem('cygenix_collation_ui_v1', JSON.stringify({ collapsed: true, t: Date.now() })); });
  await page.waitForFunction(() => { const b = document.getElementById('cygenix-sync-bar'); return b && /collation_ui/.test(b.textContent); }, null, { timeout: 15000 });
  const bar = await page.textContent('#cygenix-sync-bar');
  check('THE BANNER NAMES THE FIELD the server dropped, instead of "the server refused the change"',
    /does not sync collation_ui yet/.test(bar) && !/refused the change/.test(bar), bar.slice(0, 200));
  check('and says the change is kept', /kept on this device/.test(bar));
  world.saveMode = 'fixed';
  await page.click('#cygenix-sync-bar button:nth-of-type(2)');    // Retry now
  await page.waitForFunction(() => !document.getElementById('cygenix-sync-bar'), null, { timeout: 15000 });
  check('once the server lists the field, Retry clears the banner', await page.evaluate(() => !document.getElementById('cygenix-sync-bar')));

  /* ── 6. When the store is fixed, the box goes ─────────────────────────── */
  console.log('\n6. After the key is set on the server');
  world.secretsMode = 'ok';
  await page.evaluate(() => window.CygenixSavedConnSecrets.sync({ force: true }));
  await page.waitForTimeout(500);
  await page.evaluate(() => initConnectionsView());
  const b6 = await box();
  check('the red box is gone', !b6.shown, JSON.stringify(b6));

  console.log('\n7. Quiet');
  check('no page errors and no console errors', errors.length === 0, JSON.stringify(errors));

  await browser.close();
  server.close();
  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
