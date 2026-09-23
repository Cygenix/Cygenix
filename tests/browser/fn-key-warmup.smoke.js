/* tests/browser/fn-key-warmup.smoke.js
 * ---------------------------------------------------------------------------
 * The product's OWN Azure Function target, in a real browser.
 *
 * Two reported problems, both about a key nobody types.
 *
 *   1. FALSE 401 IN THE STATUS BAR. The saved target for the product's own
 *      Function App has an empty fnKey by design: the key is fetched from the
 *      data proxy, not stored on the connection. connections.js runs that step
 *      for every real call. The status probe did not — it built its own
 *      request and sent it bare — so Azure answered 401 and the bar said the
 *      target was unreachable while every query against it worked.
 *
 *   2. TWO ATTEMPTS TO CONNECT. The Function App is Flex Consumption, so the
 *      first request after idle is a cold start. When that request also raced
 *      the key fetch it failed outright, and the three-second guards held the
 *      retry back — which is what made it feel like "try twice and wait".
 *
 * The mock here behaves like the real thing: the product host answers 401 to
 * any request without the right key, and the key is only obtainable from the
 * data proxy. So a test that passes here could not have passed before.
 *
 * Not part of `npm test`: it needs a browser.
 *   node tests/browser/fn-key-warmup.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8428);
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

// world.cold: how many of the first /api/db calls answer like a sleeping
// Function App before it wakes.
const world = { db: [], cred: 0, cold: 0 };

const now = Date.now();
// The live shape: the saved target is the product's own function, with NO key
// on it, because the key is fetched.
const SAVED = [
  { id: 'c_src', name: 'Source DB', connMode: 'direct' },
  { id: 'c_tgt', name: 'Product Function', connMode: 'azure', fnUrl: PRODUCT_DB },
];
const SECRETS = { c_src: { connString: 'Server=src;Database=S;User Id=u;Password=p;' } };
const STORE = {
  version: 1, connMeta: {}, bindings: [], runs: [], events: [],
  profiles: [{ id: 'prof_1', name: 'Demo', envClass: 'DEV', srcConnId: 'c_src', tgtConnId: 'c_tgt',
    status: 'active', effectiveFrom: now, retiredAt: null, supersedesProfileId: null, createdAt: now, updatedAt: now }],
  settings: { envClasses: ['DEV', 'TEST', 'UAT', 'PRD', 'SANDBOX'], activeProfileId: 'prof_1', selectedAt: now },
};

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 960 } });
  const token = 'x.' + Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: U, oid: 'oid-demo' })).toString('base64url') + '.y';

  await ctx.route('**', (route) => {
    const u = route.request().url();
    const json = (s, b) => route.fulfill({ status: s, contentType: 'application/json', body: JSON.stringify(b) });

    if (u.indexOf(PRODUCT_DB) === 0) {
      const codes = new URL(u).searchParams.getAll('code');
      // The warm-up's probe is `{"action":"test"}`. A page doing its own work
      // — the Schema Explorer reading schemas, for instance — also comes
      // through here, and counting those as warm-ups would make this test
      // measure the wrong thing.
      const body = route.request().postData() || '';
      const probe = /"action"\s*:\s*"test"/.test(body);
      world.db.push({ codes, at: Date.now(), probe });
      if (world.cold > 0) { world.cold--; return route.abort('connectionfailed'); }   // asleep
      // Azure's real behaviour: no key, or the wrong one, is 401.
      if (codes.length !== 1 || codes[0] !== HOSTKEY) {
        return route.fulfill({ status: 401, contentType: 'text/plain', body: 'Unauthorized' });
      }
      return json(200, { success: true, version: 'SQL Server 2022', database: 'Target' });
    }
    if (u.indexOf('data-proxy') !== -1) {
      const q = (() => { try { return new URL(u).searchParams; } catch (e) { return new URLSearchParams(); } })();
      const a = q.get('action') || '';
      if (a === 'blob-credential') { world.cred++; return json(200, { base: 'https://' + PRODUCT_HOST + '/api/data', code: HOSTKEY }); }
      if (a === 'whoami') return json(200, { tier: 'pro', tier_status: 'active', role: 'user' });
      if (a === 'load') return json(200, {});
      if (a === 'save') return json(200, { saved: true, updatedAt: new Date().toISOString(), fields: [], ignored: [] });
      if (a === 'secrets-list') return json(200, { secrets: {}, undecryptable: [], keyVersion: '1' });
      return json(200, {});
    }
    if (/functions\/db-connect/.test(u)) return json(200, { success: true, version: 'SQL Server 2022' });
    if (/netlify\/functions|\/api\//.test(u)) return json(200, {});
    if (u.startsWith('http://localhost:' + PORT)) return route.continue();
    if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return route.fulfill({ status: 200, contentType: 'text/css', body: '' });
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
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify(arg.STORE));
    localStorage.setItem('cygenix_saved_connections', JSON.stringify({ [arg.U]: arg.SAVED }));
    localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify(arg.SECRETS));
    // The live pair as the site really has it: the product's function, NO key.
    localStorage.setItem('cygenix_project_connections', JSON.stringify({ [arg.U]: {
      srcConnString: 'Server=src;Database=S;User Id=u;Password=p;', srcConnMode: 'direct', srcFnUrl: '', srcFnKey: '',
      tgtConnString: '', tgtConnMode: 'azure', tgtFnUrl: arg.PRODUCT_DB, tgtFnKey: '' } }));
  }, { U, token, STORE, SAVED, SECRETS, PRODUCT_DB });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('console', (m) => {
    if (m.type() !== 'error') return;
    if (/Failed to load resource|ERR_|401/.test(m.text())) return;
    errors.push('[console] ' + m.text());
  });

  let opens = 0;
  const open = async (p) => {
    await page.goto('http://localhost:' + PORT + '/' + p + '?o=' + (++opens), { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => !!window.CygenixActiveConn && !!window.CygenixFnKeys, null, { timeout: 20000 });
  };
  const settle = (ms) => page.waitForTimeout(ms === undefined ? 4500 : ms);

  console.log('The product Function App — key, warm-up and cold start\n');

  /* ── 1. The shared key step exists ───────────────────────────────────── */
  console.log('1. One key step, shared');
  await open('sql-editor.html');
  check('CygenixFnKeys is on the page, beside the connections helper',
    await page.evaluate(() => typeof window.CygenixFnKeys === 'object'
      && typeof window.CygenixFnKeys.ensure === 'function'
      && typeof window.CygenixFnKeys.needsProductKey === 'function'));
  check('it loads AFTER connections.js, which owns the implementation',
    await page.evaluate(() => typeof window.CygenixConnections === 'object'));
  check('the saved target genuinely has no key — the live shape this is about',
    await page.evaluate(() => {
      const list = JSON.parse(localStorage.getItem('cygenix_saved_connections'))[Object.keys(JSON.parse(localStorage.getItem('cygenix_saved_connections')))[0]];
      const t = list.find((c) => c.id === 'c_tgt');
      return !!t.fnUrl && !t.fnKey;
    }));

  /* ── 2. The false 401 ────────────────────────────────────────────────── */
  console.log('\n2. The status probe now carries the key');
  await settle();
  const probes = world.db.slice();
  check('A PROBE WAS MADE to the product Function App', probes.length >= 1, probes.length);
  check('EVERY PROBE CARRIED EXACTLY ONE KEY — none went out bare, which is what caused the 401',
    probes.every((p) => p.codes.length === 1 && p.codes[0] === HOSTKEY), JSON.stringify(probes.map((p) => p.codes)));
  check('the key was fetched from the data proxy, not typed', world.cred >= 1, world.cred);
  const st = await page.evaluate(() => window.CygenixActiveConn.lastTest('tgt'));
  check('THE TARGET READS OK, not "HTTP 401"', st && st.ok === true, JSON.stringify(st));
  check('and the status bar carries no target complaint',
    await page.evaluate(() => {
      const s = window.CygenixStatusHairline && window.CygenixStatusHairline.current && window.CygenixStatusHairline.current();
      return !s || !/401/.test(s.text || '');
    }));
  check('the resolved target URL has one key and one ?', await page.evaluate(() => {
    const u = window.CygenixActiveConn.connUrl('tgt');
    return (u.match(/code=/g) || []).length === 1 && (u.match(/\?/g) || []).length === 1;
  }), await page.evaluate(() => window.CygenixActiveConn.connUrl('tgt')));

  /* ── 3. Warmed once per session ──────────────────────────────────────── */
  console.log('\n3. Warmed once, not once per page');
  const probesOf = () => world.db.filter((d) => d.probe).length;
  const afterFirst = probesOf();
  const credAfterFirst = world.cred;
  await open('schema_explorer.html');
  await settle();
  check('A SECOND PAGE DOES NOT WARM AGAIN — the session flag holds', probesOf() === afterFirst,
    'warm-up probes went ' + afterFirst + ' -> ' + probesOf());
  check('(the page\'s own database work still happens, and still carries the key)',
    world.db.filter((d) => !d.probe).every((d) => d.codes.length === 1 && d.codes[0] === HOSTKEY));
  check('and fetches no second key', world.cred === credAfterFirst, 'cred ' + credAfterFirst + ' -> ' + world.cred);
  check('the session flag names the profile and its connection',
    /prof_1::c_tgt/.test(await page.evaluate(() => sessionStorage.getItem('cygenix_conn_warm_v1') || '')),
    await page.evaluate(() => sessionStorage.getItem('cygenix_conn_warm_v1')));
  check('a reload in the same session warms nothing', await (async () => {
    const n = probesOf();
    await open('sql-editor.html'); await settle();
    return probesOf() === n;
  })(), 'probes=' + probesOf());

  /* ── 4. A real call right after sign-in ──────────────────────────────── */
  console.log('\n4. A real call made straight away');
  const before4 = world.db.length;
  const status4 = await page.evaluate(() => {
    const u = window.CygenixActiveConn.connUrl('tgt');
    return fetch(u, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{"action":"test"}' }).then((r) => r.status);
  });
  check('THE FIRST REAL TARGET CALL SUCCEEDS, first time', status4 === 200, 'status ' + status4);
  check('it carried the key', world.db[world.db.length - 1].codes[0] === HOSTKEY);
  check('and it needed no extra key fetch', world.cred === credAfterFirst, world.cred);
  check('exactly one more request went to the database', world.db.length === before4 + 1);

  /* ── 5. Cold start ───────────────────────────────────────────────────── */
  console.log('\n5. A sleeping Function App');
  await page.evaluate(() => {
    try { sessionStorage.removeItem('cygenix_conn_warm_v1'); sessionStorage.removeItem('cygenix_conn_test_v1'); } catch (e) {}
  });
  world.cold = 1;                               // the next call finds it asleep
  const before5 = world.db.length;
  await page.evaluate(() => window.CygenixActiveConn.warmUp({ force: true }));
  await page.waitForTimeout(9000);              // the retry waits ~4s
  const made = world.db.length - before5;
  check('THE COLD REQUEST IS RETRIED, exactly once', made === 2, 'requests made: ' + made);
  check('and the retry succeeded', await page.evaluate(() => {
    const r = window.CygenixActiveConn.lastTest('tgt'); return !!r && r.ok === true;
  }), JSON.stringify(await page.evaluate(() => window.CygenixActiveConn.lastTest('tgt'))));
  check('the gap between the two was about four seconds',
    (world.db[world.db.length - 1].at - world.db[world.db.length - 2].at) >= 3500,
    (world.db[world.db.length - 1].at - world.db[world.db.length - 2].at) + 'ms');

  /* ── 6. No storm ─────────────────────────────────────────────────────── */
  console.log('\n6. Idle');
  const before6 = world.db.length, cred6 = world.cred;
  await page.waitForTimeout(12000);
  check('nothing is requested while the page sits idle', world.db.length === before6,
    'db ' + before6 + ' -> ' + world.db.length);
  check('and no repeated key fetches', world.cred === cred6, 'cred ' + cred6 + ' -> ' + world.cred);

  /* ── 7. The key is never shown ───────────────────────────────────────── */
  console.log('\n7. The key stays out of sight');
  const shown = await page.evaluate(() => document.body.innerText || '');
  check('THE KEY APPEARS NOWHERE ON THE PAGE', shown.indexOf('HOSTKEY-FROM-PROXY') === -1);
  check('nor in the status bar text', await page.evaluate((k) => {
    const s = window.CygenixStatusHairline && window.CygenixStatusHairline.current && window.CygenixStatusHairline.current();
    return !s || String(s.text || '').indexOf(k) === -1;
  }, HOSTKEY));
  check('and it was never written to the console', !errors.some((e) => e.indexOf('HOSTKEY-FROM-PROXY') !== -1));

  console.log('\n8. Quiet');
  check('no page errors and no unexpected console errors', errors.length === 0, JSON.stringify(errors));

  await browser.close();
  server.close();
  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
