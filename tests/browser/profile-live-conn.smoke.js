/* tests/browser/profile-live-conn.smoke.js
 * ---------------------------------------------------------------------------
 * The reported bug, in a real browser: the SQL Editor and the Schema Explorer
 * saw no connection even though the selected profile named both sides and the
 * credentials were on the device.
 *
 * The cause was not missing logic. cygenix-profile-apply.js already copies the
 * selected profile's two connections into the live settings, on selection and
 * again on load. It was loaded on six pages. The SQL Editor and the Schema
 * Explorer were not among them, so they read whatever the Connections page
 * last held — for somebody who had only ever used profiles, nothing.
 *
 * tests/profile-apply.test.js pins that every page loading connections.js now
 * loads the engine too. This proves the consequence the report asked for:
 * open the SQL Editor with a profile selected and BOTH live connection
 * strings are populated, switch profile and both change without a reload, and
 * a side whose credential is not on this device is left empty and SAID rather
 * than silently blank.
 *
 * Not part of `npm test`: it needs a browser.
 *   node tests/browser/profile-live-conn.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8426);
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

// Two profiles, four saved connections. ELITE has both credentials on this
// device; SECOND has both too, so a switch is expected to change every value.
// THIRD names a connection whose secret is NOT here, which is the warned case.
const SAVED = [
  { id: 'sconn_e_src', name: 'Elite Source',  connMode: 'direct' },
  { id: 'sconn_e_tgt', name: 'Elite Target',  connMode: 'direct' },
  { id: 'sconn_s_src', name: 'Second Source', connMode: 'direct' },
  { id: 'sconn_s_tgt', name: 'Second Target', connMode: 'direct' },
  { id: 'sconn_n_src', name: 'No Secret Src', connMode: 'direct' },
];
const SECRETS = {
  sconn_e_src: { connString: 'Server=elite-src;Database=EliteSrc;User Id=u;Password=p1;' },
  sconn_e_tgt: { connString: 'Server=elite-tgt;Database=EliteTgt;User Id=u;Password=p2;' },
  sconn_s_src: { connString: 'Server=second-src;Database=SecondSrc;User Id=u;Password=p3;' },
  sconn_s_tgt: { connString: 'Server=second-tgt;Database=SecondTgt;User Id=u;Password=p4;' },
  // sconn_n_src deliberately absent.
};
const now = Date.now();
const profile = (id, name, src, tgt) => ({
  id, name, projectId: null, serviceAccount: null, envClass: 'DEV',
  srcConnId: src, tgtConnId: tgt, status: 'active',
  effectiveFrom: now, retiredAt: null, supersedesProfileId: null,
  createdAt: now, updatedAt: now,
});
const STORE = {
  version: 1,
  connMeta: {}, bindings: [], runs: [], events: [],
  profiles: [
    profile('prof_elite', 'Elite Sample', 'sconn_e_src', 'sconn_e_tgt'),
    profile('prof_second', 'Second Sample', 'sconn_s_src', 'sconn_s_tgt'),
    profile('prof_nosec', 'Missing Credential', 'sconn_n_src', 'sconn_e_tgt'),
  ],
  settings: { envClasses: ['DEV', 'TEST', 'UAT', 'PRD', 'SANDBOX'], activeProfileId: 'prof_elite', selectedAt: now },
};

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 960 } });
  const token = 'x.' + Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: U, oid: 'oid-demo' })).toString('base64url') + '.y';

  await ctx.route('**', (route) => {
    const u = route.request().url();
    const json = (s, b) => route.fulfill({ status: s, contentType: 'application/json', body: JSON.stringify(b) });
    if (u.indexOf('data-proxy') !== -1) {
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
    // The profile store, the saved connections (no secrets in them) and the
    // local secret store — exactly the shape the live site has.
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify(arg.STORE));
    localStorage.setItem('cygenix_saved_connections', JSON.stringify({ [arg.U]: arg.SAVED }));
    localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify(arg.SECRETS));
    // The live pair starts EMPTY — the state the bug was reported in.
    localStorage.setItem('cygenix_project_connections', JSON.stringify({ [arg.U]: {
      srcConnString: '', srcConnMode: 'direct', srcFnUrl: '', srcFnKey: '',
      tgtConnString: '', tgtConnMode: 'direct', tgtFnUrl: '', tgtFnKey: '' } }));
  }, { U, token, STORE, SAVED, SECRETS });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('console', (m) => {
    if (m.type() !== 'error') return;
    if (/Failed to load resource|ERR_/.test(m.text())) return;
    errors.push('[console] ' + m.text());
  });

  const open = async (p) => {
    await page.goto('http://localhost:' + PORT + '/' + p, { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => !!window.CygenixConnections && !!window.CygenixProfiles && !!window.CygenixProfileApply, null, { timeout: 20000 });
    await page.waitForTimeout(1500);
  };
  const live = () => page.evaluate(() => {
    const c = window.CygenixConnections.get();
    return { src: c.srcConnString || '', tgt: c.tgtConnString || '' };
  });

  console.log('Profile to live connection — the SQL Editor and the Schema Explorer\n');

  /* ── 1. The reported page ────────────────────────────────────────────── */
  console.log('1. SQL Editor, with Elite Sample selected and the live pair empty');
  await open('sql-editor.html');
  check('the three modules are loaded on this page at all (they were not, which was the bug)',
    await page.evaluate(() => !!window.CygenixProfiles && !!window.CygenixProfileApply && !!window.CygenixSavedConnSecrets));
  let l = await live();
  check('BOTH LIVE CONNECTION STRINGS ARE NOW POPULATED', !!l.src && !!l.tgt, JSON.stringify(l));
  check('and they are the selected profile\'s two connections, not some other pair',
    /elite-src/.test(l.src) && /elite-tgt/.test(l.tgt), JSON.stringify(l));
  check('the credential came with them — the string carries its password',
    /Password=p1;/.test(l.src) && /Password=p2;/.test(l.tgt));
  check('srcConn and tgtConn, which the pages actually read, are non-empty too',
    await page.evaluate(() => !!window.CygenixConnections.srcConn && !!window.CygenixConnections.tgtConn));

  /* ── 2. Switching, without a reload ──────────────────────────────────── */
  console.log('\n2. Switching profile on the same page');
  const res = await page.evaluate(() => window.CygenixProfileApply.select('prof_second'));
  check('the switch is accepted', res && res.ok !== false, JSON.stringify(res));
  await page.waitForTimeout(400);
  const l2 = await live();
  check('BOTH VALUES CHANGED WITHOUT A PAGE REFRESH',
    /second-src/.test(l2.src) && /second-tgt/.test(l2.tgt) && l2.src !== l.src && l2.tgt !== l.tgt, JSON.stringify(l2));
  check('with the new credentials', /Password=p3;/.test(l2.src) && /Password=p4;/.test(l2.tgt));
  check('and the store records the new selection',
    await page.evaluate(() => JSON.parse(localStorage.getItem('cygenix_profiles_v1')).settings.activeProfileId) === 'prof_second');

  /* ── 3. The Schema Explorer sees the same thing ──────────────────────── */
  console.log('\n3. Schema Explorer, fresh load, no re-selection');
  await open('schema_explorer.html');
  const l3 = await live();
  check('it reads the pair the selected profile named', /second-src/.test(l3.src) && /second-tgt/.test(l3.tgt), JSON.stringify(l3));
  check('both sides, with credentials', /Password=p3;/.test(l3.src) && /Password=p4;/.test(l3.tgt));

  /* ── 4. A credential this device does not have ───────────────────────── */
  console.log('\n4. A profile whose credential is not on this device');
  await page.evaluate(() => {
    // Clear the once-per-selection stamp so the load check runs again.
    try { sessionStorage.removeItem('cygenix_profile_apply_seen'); } catch (e) {}
  });
  const res4 = await page.evaluate(() => window.CygenixProfileApply.select('prof_nosec'));
  check('the switch is accepted', res4 && res4.ok !== false, JSON.stringify(res4));
  await page.waitForTimeout(500);
  const l4 = await live();
  check('THE SIDE WITH NO CREDENTIAL IS NOT SILENTLY FILLED WITH THE WRONG ONE',
    !/second-src/.test(l4.src) && !/elite-src/.test(l4.src), JSON.stringify(l4));
  // select() hands the sentence back to whoever called it — the Profiles
  // page renders it inline, next to the switch that caused it. The floating
  // notice belongs to the LOAD path, because a page opened later has no
  // caller to tell.
  check('select() names what is missing, for its caller to show',
    !!res4.missing && /No Secret Src/.test(res4.missing), JSON.stringify(res4 && res4.missing));
  check('and the side that DOES have its credential is still loaded',
    /elite-tgt/.test(l4.tgt), JSON.stringify(l4));

  console.log('\n4b. …and on a later page load, where no caller is listening');
  await open('sql-editor.html');
  const l4b = await live();
  check('the unusable side is still not guessed at', !/second-src|elite-src/.test(l4b.src), JSON.stringify(l4b));
  check('THE PAGE SAYS SO rather than looking merely empty',
    await page.evaluate(() => {
      const n = document.getElementById('cyg-profile-notice');
      return !!n && /No Secret Src|password|missing|finish/i.test(n.textContent);
    }), await page.evaluate(() => { const n = document.getElementById('cyg-profile-notice'); return n ? n.textContent : '(no notice)'; }));
  check('with a way to fix it', await page.evaluate(() => {
    const n = document.getElementById('cyg-profile-notice');
    return !!n && !!n.querySelector('a[href*="connections"]');
  }));

  /* ── 5. No loop ──────────────────────────────────────────────────────── */
  console.log('\n5. Not a loop');
  const writes = await page.evaluate(() => {
    let n = 0;
    const orig = localStorage.setItem.bind(localStorage);
    localStorage.setItem = function (k, v) { if (k === 'cygenix_project_connections') n++; return orig(k, v); };
    return new Promise((res) => setTimeout(() => res(n), 4000));
  });
  check('the live pair is not rewritten on a timer while the page sits idle', writes === 0, 'writes=' + writes);

  console.log('\n6. Quiet');
  check('no page errors and no console errors', errors.length === 0, JSON.stringify(errors));

  await browser.close();
  server.close();
  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
