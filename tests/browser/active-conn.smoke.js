/* tests/browser/active-conn.smoke.js
 * ---------------------------------------------------------------------------
 * Steps 1 and 3 of the profile-reliability brief, in a real browser, against
 * a mocked backend.
 *
 * The three claims that matter for the demo:
 *
 *   · a fresh sign-in with a profile defined and NOTHING selected ends up
 *     connected, because the profile is auto-selected and applied;
 *   · a request to the Function App carries EXACTLY ONE code parameter, even
 *     when the stored URL already had one — the 401;
 *   · switching profile changes both connections and throws away the schema
 *     cached from the old one.
 *
 * Plus the thing that must not happen: no request storm. Every call to the
 * mocked backend is counted, and the count is checked after sixty seconds of
 * idling (compressed to a shorter wait; the guards are interval-based).
 *
 * Not part of `npm test`: it needs a browser.
 *   node tests/browser/active-conn.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8427);
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
const FN_HOST = 'https://fn.azurewebsites.net/api/db';
const GOOD_KEY = 'RIGHTKEY';

// Every request to the "database" is recorded, with its code parameters.
const hits = { db: [], netlify: 0, proxy: 0 };

const now = Date.now();
const profile = (id, envClass, src, tgt, updatedAt) => ({
  id, name: id, projectId: null, serviceAccount: null, envClass,
  srcConnId: src, tgtConnId: tgt, status: 'active',
  effectiveFrom: now, retiredAt: null, supersedesProfileId: null,
  createdAt: now, updatedAt: updatedAt || now,
});

// ELITE's target URL ALREADY carries a code — the exact shape that produced
// the doubled parameter and the 401.
const SAVED = [
  { id: 'c_e_src', name: 'Elite Source', connMode: 'direct' },
  { id: 'c_e_tgt', name: 'Elite Target', connMode: 'azure', fnUrl: FN_HOST + '?code=' + GOOD_KEY },
  { id: 'c_s_src', name: 'Second Source', connMode: 'direct' },
  { id: 'c_s_tgt', name: 'Second Target', connMode: 'azure', fnUrl: FN_HOST },
];
const SECRETS = {
  c_e_src: { connString: 'Server=elite-src;Database=EliteSrc;User Id=u;Password=p1;' },
  c_e_tgt: { fnKey: GOOD_KEY },
  c_s_src: { connString: 'Server=second-src;Database=SecondSrc;User Id=u;Password=p3;' },
  c_s_tgt: { fnKey: GOOD_KEY },
};
const STORE = {
  version: 1, connMeta: {}, bindings: [], runs: [], events: [],
  profiles: [
    profile('prof_elite', 'DEV', 'c_e_src', 'c_e_tgt', now),
    profile('prof_second', 'TEST', 'c_s_src', 'c_s_tgt', now - 90000),
    profile('prof_prod', 'PRD', 'c_e_src', 'c_e_tgt', now + 60000),  // newest, but PRD
  ],
  // DELIBERATELY UNSELECTED — the state that used to brick the product.
  settings: { envClasses: ['DEV', 'TEST', 'UAT', 'PRD', 'SANDBOX'], activeProfileId: null, selectedAt: 0 },
};

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 960 } });
  const token = 'x.' + Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: U, oid: 'oid-demo' })).toString('base64url') + '.y';

  await ctx.route('**', (route) => {
    const u = route.request().url();
    const json = (s, b) => route.fulfill({ status: s, contentType: 'application/json', body: JSON.stringify(b) });
    if (u.indexOf(FN_HOST) === 0) {
      const codes = new URL(u).searchParams.getAll('code');
      hits.db.push({ url: u, codes });
      // The Function App's real behaviour: the LAST code wins, and a wrong
      // one is 401. A doubled parameter therefore fails exactly as reported.
      if (codes.length !== 1 || codes[0] !== GOOD_KEY) {
        return route.fulfill({ status: 401, contentType: 'text/plain', body: 'Unauthorized' });
      }
      return json(200, { success: true, version: 'SQL Server 2022', database: 'EliteTgt' });
    }
    if (/functions\/db-connect/.test(u)) { hits.netlify++; return json(200, { success: true, version: 'SQL Server 2022' }); }
    if (u.indexOf('data-proxy') !== -1) {
      hits.proxy++;
      const q = (() => { try { return new URL(u).searchParams; } catch (e) { return new URLSearchParams(); } })();
      const a = q.get('action') || '';
      if (a === 'whoami') return json(200, { tier: 'pro', tier_status: 'active', role: 'user' });
      if (a === 'load') return json(200, {});
      if (a === 'save') return json(200, { saved: true, updatedAt: new Date().toISOString(), fields: [], ignored: [] });
      if (a === 'secrets-list') return json(200, { secrets: {}, undecryptable: [], keyVersion: '1' });
      return json(200, {});
    }
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
    // Stale schema from a previous session, and an EMPTY live pair.
    localStorage.setItem('cygenix_schema_elite_src', JSON.stringify({ tables: ['OLD_TABLE'] }));
    localStorage.setItem('cygenix_project_connections', JSON.stringify({ [arg.U]: {
      srcConnString: '', srcConnMode: 'direct', srcFnUrl: '', srcFnKey: '',
      tgtConnString: '', tgtConnMode: 'direct', tgtFnUrl: '', tgtFnKey: '' } }));
  }, { U, token, STORE, SAVED, SECRETS });

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
    await page.waitForFunction(() => !!window.CygenixActiveConn && !!window.CygenixProfileApply && !!window.CygenixProfiles, null, { timeout: 20000 });
    await page.waitForTimeout(1800);
  };
  const active = () => page.evaluate(() => ({
    src: window.CygenixActiveConn.get('src'),
    tgt: window.CygenixActiveConn.get('tgt'),
    srcUrl: window.CygenixActiveConn.connUrl('src'),
    tgtUrl: window.CygenixActiveConn.connUrl('tgt'),
    selected: JSON.parse(localStorage.getItem('cygenix_profiles_v1')).settings.activeProfileId,
  }));

  console.log('Active connection and auto-apply — in a browser\n');

  /* ── 1. Sign in with nothing selected ────────────────────────────────── */
  console.log('1. A fresh sign-in, profiles defined, none selected');
  await open('sql-editor.html');
  let a = await active();
  check('A PROFILE IS AUTO-SELECTED rather than leaving the product blocked', !!a.selected, JSON.stringify(a.selected));
  check('and it is NOT the PRD one, even though PRD is the most recently updated',
    a.selected !== 'prof_prod', a.selected);
  check('it is the newest non-production profile', a.selected === 'prof_elite', a.selected);
  check('BOTH SIDES RESOLVE, from the profile', a.src.ok && a.tgt.ok && a.src.source === 'profile' && a.tgt.source === 'profile',
    JSON.stringify({ s: a.src.source, t: a.tgt.source }));
  check('the source carries its password', /Password=p1;/.test(a.srcUrl), a.srcUrl);
  check('the live pair was written back, so legacy readers agree',
    await page.evaluate(() => {
      const c = window.CygenixConnections.get();
      return !!(c.srcConnString || c.srcFnUrl) && !!(c.tgtConnString || c.tgtFnUrl);
    }));

  /* ── 2. Exactly one code ─────────────────────────────────────────────── */
  console.log('\n2. The doubled key');
  check('the stored target URL already carried a code (the shape that broke it)',
    /code=/.test(SAVED[1].fnUrl));
  check('EXACTLY ONE code SURVIVES composition', (a.tgtUrl.match(/code=/g) || []).length === 1, a.tgtUrl);
  check('and it is the right one', a.tgtUrl.indexOf('code=' + GOOD_KEY) !== -1, a.tgtUrl);
  check('there is no double ?', (a.tgtUrl.match(/\?/g) || []).length === 1, a.tgtUrl);

  const before = hits.db.length;
  const probe = await page.evaluate((u) => fetch(u, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{"action":"test"}' })
    .then((r) => r.status), a.tgtUrl);
  check('A REAL CALL TO THAT URL RETURNS 200, not 401', probe === 200, 'status ' + probe);
  const lastHit = hits.db[hits.db.length - 1];
  check('the server saw exactly one code parameter', lastHit && lastHit.codes.length === 1, JSON.stringify(lastHit && lastHit.codes));
  check('(and the old shape would have failed) — a doubled code is rejected by the mock',
    await page.evaluate((u) => fetch(u + '&code=WRONG', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' }).then((r) => r.status), a.tgtUrl) === 401);

  /* ── 3. Another page agrees ──────────────────────────────────────────── */
  console.log('\n3. Every page gives the same answer');
  await open('schema_explorer.html');
  const b = await active();
  check('the Schema Explorer resolves the same pair', b.srcUrl === a.srcUrl && b.tgtUrl === a.tgtUrl);
  check('and it did not re-select or re-apply anything', b.selected === a.selected);

  /* ── 4. Switching ────────────────────────────────────────────────────── */
  console.log('\n4. Switching profile');
  check('the stale schema cache is present before the switch',
    await page.evaluate(() => !!localStorage.getItem('cygenix_schema_elite_src')));
  const sw = await page.evaluate(() => window.CygenixProfileApply.select('prof_second'));
  check('the switch is accepted', sw && sw.ok !== false, JSON.stringify(sw && sw.reason));
  await page.waitForTimeout(500);
  const c = await active();
  check('BOTH CONNECTIONS CHANGED without a reload', /second-src/.test(c.srcUrl) && c.srcUrl !== a.srcUrl, c.srcUrl);
  check('the new target still composes exactly one code', (c.tgtUrl.match(/code=/g) || []).length === 1, c.tgtUrl);
  check('THE STALE SCHEMA CACHE IS GONE',
    await page.evaluate(() => !localStorage.getItem('cygenix_schema_elite_src')));
  const swBack = await page.evaluate(() => window.CygenixProfileApply.select('prof_elite'));
  await page.waitForTimeout(500);
  const d = await active();
  check('switching back works too', swBack && swBack.ok !== false && /elite-src/.test(d.srcUrl), d.srcUrl);

  /* ── 5. No storm ─────────────────────────────────────────────────────── */
  console.log('\n5. Idle, and quiet');
  const dbBefore = hits.db.length, netBefore = hits.netlify, proxyBefore = hits.proxy;
  await page.waitForTimeout(12000);
  check('no database calls at all while the page sits idle', hits.db.length === dbBefore,
    'db went ' + dbBefore + ' -> ' + hits.db.length);
  check('and no repeated netlify database calls either', hits.netlify - netBefore <= 1,
    'netlify went ' + netBefore + ' -> ' + hits.netlify);
  check('the proxy is not being polled', hits.proxy - proxyBefore <= 3, 'proxy went ' + proxyBefore + ' -> ' + hits.proxy);

  console.log('\n6. Quiet');
  check('no page errors and no unexpected console errors', errors.length === 0, JSON.stringify(errors));

  await browser.close();
  server.close();
  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
