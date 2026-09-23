/* tests/browser/analytics-consent.smoke.js
 * ---------------------------------------------------------------------------
 * The compliance claim, in a real browser: NOTHING is requested from Google
 * until somebody presses Accept.
 *
 * tests/ga4-analytics.test.js proves this against a fake window, which is
 * enough to pin the logic but not enough to prove the page. Here a real
 * Chromium loads the real home page, every request to googletagmanager.com
 * is counted at the network layer, and the count is checked before and
 * after each click. It also proves the negative that matters most: a signed
 * -in console page loads no tag even when the visitor HAS accepted.
 *
 * Not part of `npm test`: it needs a browser.
 *   node tests/browser/analytics-consent.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8425);
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
const KEY = 'cygenix_cookie_consent';
let google = 0;

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  const token = 'x.' + Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: U, oid: 'oid-demo' })).toString('base64url') + '.y';

  await ctx.route('**', (route) => {
    const u = route.request().url();
    // The measurement itself. Counted, then answered with an inert stub so
    // the page behaves as it would with Google reachable.
    if (/googletagmanager\.com|google-analytics\.com/.test(u)) {
      google++;
      return route.fulfill({ status: 200, contentType: 'text/javascript', body: '/* stub gtag.js */' });
    }
    if (u.indexOf('data-proxy') !== -1) return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    if (/netlify\/functions|\/api\//.test(u)) return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    if (u.startsWith('http://localhost:' + PORT)) return route.continue();
    if (/fonts\.googleapis\.com|fonts\.gstatic\.com/.test(u)) return route.fulfill({ status: 200, contentType: 'text/css', body: '' });
    return route.abort();
  });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('console', (m) => {
    if (m.type() !== 'error') return;
    if (/Failed to load resource|ERR_/.test(m.text())) return;
    errors.push('[console] ' + m.text());
  });

  const home = 'http://localhost:' + PORT + '/index.html';
  const open = async (url) => {
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(900);          // the banner's 200ms init, plus slack
  };
  const banner = () => page.evaluate(() => !!document.getElementById('cc-banner'));
  const record = () => page.evaluate((k) => { try { return JSON.parse(localStorage.getItem(k) || 'null'); } catch (e) { return 'unparseable'; } }, KEY);
  const btn = (label) => page.locator('#cc-banner button', { hasText: new RegExp('^' + label + '$') });

  console.log('Analytics consent — in a browser\n');

  /* ── 1. A first visit ────────────────────────────────────────────────── */
  console.log('1. A first visit');
  await page.context().clearCookies();
  await open(home);
  await page.evaluate(() => { try { localStorage.clear(); } catch (e) {} });
  await open(home);
  check('THE BANNER IS SHOWN', await banner());
  check('AND NOTHING HAS BEEN REQUESTED FROM GOOGLE', google === 0, 'google=' + google);
  check('the module agrees it has not loaded', await page.evaluate(() => window.CygenixGA4.isLoaded()) === false);
  check('the denied default was set before any of that',
    await page.evaluate(() => {
      const d = (window.dataLayer || []).map(a => Array.prototype.slice.call(a));
      const f = d.find(c => c[0] === 'consent' && c[1] === 'default');
      return !!f && f[2].analytics_storage === 'denied' && f[2].ad_storage === 'denied'
          && f[2].ad_user_data === 'denied' && f[2].ad_personalization === 'denied';
    }));

  /* ── 2. Equal prominence ─────────────────────────────────────────────── */
  console.log('\n2. Reject is exactly as prominent as Accept');
  const styleOf = (label) => page.evaluate((l) => {
    const b = [...document.querySelectorAll('#cc-banner button')].find(x => x.textContent.trim() === l);
    if (!b) return null;
    const s = getComputedStyle(b);
    return { bg: s.backgroundColor, color: s.color, border: s.borderTopWidth + ' ' + s.borderTopColor,
             pad: s.padding, font: s.fontSize + ' ' + s.fontWeight, w: Math.round(b.getBoundingClientRect().width) };
  }, label);
  const rej = await styleOf('Reject');
  const acc = await styleOf('Accept');
  check('both buttons exist', !!rej && !!acc);
  check('SAME BACKGROUND, COLOUR, BORDER, PADDING AND FONT',
    JSON.stringify({ ...rej, w: 0 }) === JSON.stringify({ ...acc, w: 0 }), JSON.stringify({ rej, acc }));
  check('and the same width to within a few pixels of the label',
    Math.abs(rej.w - acc.w) <= 8, JSON.stringify({ rejectWidth: rej.w, acceptWidth: acc.w }));

  /* ── 3. Reject ───────────────────────────────────────────────────────── */
  console.log('\n3. Reject');
  await btn('Reject').click();
  await page.waitForTimeout(400);
  check('the banner goes', !(await banner()));
  let rec = await record();
  check('the choice is remembered, at the current version, with analytics off',
    rec && rec.version === '2' && rec.analytics === false && rec.essential === true, JSON.stringify(rec));
  check('STILL NOTHING FROM GOOGLE', google === 0, 'google=' + google);
  await open(home);
  check('and on a fresh page load the banner does not come back', !(await banner()));
  check('and still nothing from Google', google === 0, 'google=' + google);

  /* ── 4. Accept ───────────────────────────────────────────────────────── */
  console.log('\n4. Accept');
  await page.evaluate(() => { try { localStorage.clear(); } catch (e) {} });
  await open(home);
  check('the banner is back once the record is cleared', await banner());
  check('nothing requested yet', google === 0, 'google=' + google);
  await btn('Accept').click();
  await page.waitForTimeout(900);
  rec = await record();
  check('the choice is remembered with analytics on', rec && rec.analytics === true, JSON.stringify(rec));
  check('GOOGLE IS REQUESTED, EXACTLY ONCE, AND ONLY NOW', google === 1, 'google=' + google);
  check('the module says it loaded', await page.evaluate(() => window.CygenixGA4.isLoaded()) === true);
  check('and analytics_storage was updated to granted',
    await page.evaluate(() => {
      const d = (window.dataLayer || []).map(a => Array.prototype.slice.call(a));
      const u = d.filter(c => c[0] === 'consent' && c[1] === 'update').pop();
      return !!u && u[2].analytics_storage === 'granted';
    }));
  check('no advertising signal was ever granted',
    await page.evaluate(() => (window.dataLayer || []).map(a => Array.prototype.slice.call(a))
      .filter(c => c[0] === 'consent')
      .every(c => !c[2] || (c[2].ad_storage !== 'granted' && c[2].ad_user_data !== 'granted' && c[2].ad_personalization !== 'granted'))));

  /* ── 5. Every marketing page, and no app page ────────────────────────── */
  console.log('\n5. Where the tag runs, now that consent is given');
  for (const p of ['pricing', 'help', 'about', 'register', 'privacy', 'terms']) {
    const before = google;
    await open('http://localhost:' + PORT + '/' + p + '.html');
    check(p + ' loads the tag (consent is stored)', google === before + 1 && !(await banner()), 'google went ' + before + ' -> ' + google);
  }

  console.log('\n6. The console is never measured');
  // Sign the visitor in, keeping the accepted analytics record.
  await page.evaluate((arg) => {
    localStorage.setItem('cygenix_onboarded', 'true');
    localStorage.setItem('cygenix_user', JSON.stringify({ email: arg.U }));
    localStorage.setItem('cygenix_active_user', arg.U);
    localStorage.setItem('cygenix_tier', 'pro');
    localStorage.setItem('cygenix_token', arg.token);
    localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify({ homeAccountId: 'h.t', environment: 'cygenix.ciamlogin.com', tenantId: 't', username: arg.U, localAccountId: 'l', authorityType: 'MSSTS', name: 'Demo' }));
    localStorage.setItem('h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-',
      JSON.stringify({ credentialType: 'IdToken', secret: arg.token, expiresOn: String(Math.floor(Date.now() / 1000) + 3600) }));
  }, { U, token });
  for (const p of ['dashboard', 'login', 'profiles']) {
    const before = google;
    await open('http://localhost:' + PORT + '/' + p + '.html');
    check(p + ' REQUESTS NOTHING FROM GOOGLE, even though analytics is accepted',
      google === before, 'google went ' + before + ' -> ' + google);
    check('  and defines no gtag on ' + p, await page.evaluate(() => typeof window.CygenixGA4 === 'undefined'));
  }

  console.log('\n7. Quiet');
  check('no page errors and no console errors', errors.length === 0, JSON.stringify(errors));

  await browser.close();
  server.close();
  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
