/* tests/browser/sidebar-nav.smoke.js
 * ---------------------------------------------------------------------------
 * The dashboard sidebar actually switches views.
 *
 * THE BUG THIS EXISTS FOR
 * The nav's "am I on the dashboard" test was a regex written against
 * /dashboard.html. The addresses went extensionless and nobody updated it, so
 * on the real URL it was always false: all fifteen `view:` items fell through
 * to a navigate branch that assigned a URL differing only in the hash — which
 * does not reload a page — while the twenty `href:` items carried on working.
 * Clicking most of the menu did nothing at all, silently, with no console
 * error, because nothing had failed. Something had merely not happened.
 *
 * A source-level test would not have caught it. The regex was valid, the
 * branch was reachable, the fallback existed. What was wrong was the
 * relationship between a string and a deployed URL — which only a browser
 * sitting on that URL can tell you.
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/sidebar-nav.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8399);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

// charset matters: without it the browser reads UTF-8 source as latin-1.
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.json': 'application/json' };

const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/') p = '/index.html';
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';   // the extensionless rewrite
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) {
    res.writeHead(404); return res.end('no');
  }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.route('**/*', (r) =>
    r.request().url().startsWith('http://localhost:' + PORT) ? r.continue() : r.abort());
  await page.addInitScript(() => {
    if (sessionStorage.getItem('cygenix_token')) return;
    const exp = String(Date.now() + 3600e3);
    for (const s of [localStorage, sessionStorage]) {
      s.setItem('cygenix_token', 'smoke'); s.setItem('cygenix_expires', exp);
    }
    localStorage.setItem('cygenix_onboarded', '1');
    localStorage.setItem('cygenix_user', JSON.stringify({ email: 'you@example.test', name: 'You' }));
    localStorage.setItem('cygenix_tier', 'pro');
    localStorage.setItem('cygenix_cookie_consent', 'all');
    localStorage.setItem('acct-cygenix.ciamlogin.com-x', JSON.stringify({
      homeAccountId: 'x', environment: 'cygenix.ciamlogin.com', authorityType: 'MSSTS',
      username: 'you@example.test', localAccountId: 'x', tenantId: 'x' }));
  });

  const url = (p) => 'http://localhost:' + PORT + p;
  const open = async (p) => {
    await page.goto(url(p), { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => !!document.querySelector('.cyg-nav-item'), null, { timeout: 15000 });
    await page.waitForTimeout(400);
  };
  const activeView = () => page.evaluate(() => {
    const el = document.querySelector('.view.active');
    return el ? el.id.replace(/^view-/, '') : null;
  });
  // Click through the DOM the way a person does, not by calling the handler.
  const clickNav = async (key) => {
    const ok = await page.evaluate((k) => {
      const el = document.querySelector('.cyg-nav-item[data-key="' + k + '"]');
      if (!el) return false;
      el.click();
      return true;
    }, key);
    await page.waitForTimeout(220);
    return ok;
  };

  console.log('Dashboard sidebar — the nav actually changes the view\n');

  await open('/dashboard');
  check('the dashboard is served at the extensionless address and the nav renders',
    (await page.evaluate(() => document.querySelectorAll('.cyg-nav-item').length)) > 20);
  check('and the address really is /dashboard, with no extension',
    (await page.evaluate(() => location.pathname)) === '/dashboard');

  /* ── 1. Every view: item switches the view ──────────────────────────────── */
  const VIEWS = await page.evaluate(() =>
    Array.from(document.querySelectorAll('.cyg-nav-item[data-key]'))
      .map((el) => el.dataset.key));

  // The fifteen the bug killed, by their view name.
  const EXPECTED = ['dashboard', 'search', 'connections', 'integrations', 'project-settings',
    'notifications', 'system-parameters', 'jobs', 'task-agent', 'server-migration',
    'reports', 'inventory', 'privacy-security', 'audit', 'diagnostics'];

  const keyForView = await page.evaluate(() => {
    const map = {};
    (window.__navPairs || []).forEach((p) => { map[p.view] = p.key; });
    return map;
  });

  const broken = [];
  for (const view of EXPECTED) {
    // Nav keys and view names differ for some items; find the item whose click
    // lands on this view by trying its own key first, then the view name.
    const key = keyForView[view] || view;
    await open('/dashboard');
    const clicked = await clickNav(key);
    const now = await activeView();
    if (!clicked || now !== view) broken.push(view + ' (key=' + key + ', landed on ' + now + ')');
  }
  check('all fifteen view: items switch the view when clicked',
    broken.length === 0, broken.join(' | '));

  /* ── 1b. …in place, without reloading the whole application ─────────────── */
  //
  // This is the check that isolates the path test from the fallback. With the
  // stale regex still in place, the repaired fallback rescues every one of the
  // fifteen — by reloading the entire dashboard for each click. The user sees
  // a working menu that flashes white and re-fetches everything. That is not
  // this bug fixed, it is this bug made survivable, so the assertion is that
  // the document instance survives the click.
  await open('/dashboard');
  await page.evaluate(() => { window.__sameDocument = true; });
  await clickNav('connections');
  check('a view: item switches IN PLACE — no page reload',
    (await page.evaluate(() => window.__sameDocument === true)) === true
    && (await activeView()) === 'connections',
    'the fallback would also land on the view, but only by reloading the app');

  /* ── 2. The same item twice ─────────────────────────────────────────────── */
  await open('/dashboard');
  await clickNav('integrations');
  const first = await activeView();
  await clickNav('integrations');
  const second = await activeView();
  check('clicking the same item twice stays on that view',
    first === 'integrations' && second === 'integrations', first + ' then ' + second);
  check('and does not leave a stale goto in the address',
    !/goto=/.test(await page.evaluate(() => location.hash)));

  /* ── 3. A cold load of a deep link ──────────────────────────────────────── */
  //
  // Navigating from /dashboard to /dashboard#goto=x is a SAME-DOCUMENT hash
  // change: the page does not reload and the reader never runs. That is the
  // very trap this bug was made of, and the first draft of this test fell into
  // it — the deep-link checks passed on a view left over from the previous
  // one. Every cold load below goes somewhere else first.
  const coldLoad = async (target) => {
    await page.goto(url('/sql-editor'), { waitUntil: 'domcontentloaded' });
    await page.goto(url(target), { waitUntil: 'domcontentloaded' });
  };

  await coldLoad('/dashboard#goto=integrations');
  await page.waitForFunction(() => !!document.querySelector('.view.active'), null, { timeout: 15000 });
  await page.waitForTimeout(700);
  check('a cold load of /dashboard#goto=integrations lands on Integrations',
    (await activeView()) === 'integrations', await activeView());
  check('and the goto is cleared from the address, so a refresh does not re-fire it',
    !/goto=/.test(await page.evaluate(() => location.hash)),
    await page.evaluate(() => location.hash));

  // The view cygenix-project-summary.js INJECTS rather than ships — the reader
  // has to run late enough to see it.
  await coldLoad('/dashboard#goto=project-summary-document');
  await page.waitForTimeout(1200);
  check('a deep link to a view injected by a later script also lands',
    (await activeView()) === 'project-summary-document', await activeView());

  // An unknown view must not blank the screen.
  await coldLoad('/dashboard#goto=not-a-real-view');
  await page.waitForTimeout(700);
  const unknown = await activeView();
  check('an unknown view name leaves the dashboard on a real view, not blank',
    unknown !== null && unknown !== 'not-a-real-view', String(unknown));

  // The other transport.
  await page.goto(url('/dashboard'), { waitUntil: 'domcontentloaded' });
  await page.evaluate(() => sessionStorage.setItem('cyg_goto', 'audit'));
  await page.goto(url('/dashboard'), { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(700);
  check('the sessionStorage transport still works for links from other pages',
    (await activeView()) === 'audit', await activeView());
  check('and the key is consumed, not left for the next load',
    (await page.evaluate(() => sessionStorage.getItem('cyg_goto'))) === null);

  /* ── 4. href: items still navigate ──────────────────────────────────────── */
  await open('/dashboard');
  const hrefKeys = await page.evaluate(() =>
    Array.from(document.querySelectorAll('.cyg-nav-item[data-key]'))
      .map((el) => el.dataset.key));
  await clickNav('sql-editor');
  await page.waitForTimeout(600);
  check('an href: item still performs a real navigation',
    (await page.evaluate(() => location.pathname)) === '/sql-editor',
    await page.evaluate(() => location.pathname));

  await open('/dashboard');
  await clickNav('object-mapping');
  await page.waitForTimeout(600);
  check('and so does a second one',
    (await page.evaluate(() => location.pathname)) === '/object-mapping',
    await page.evaluate(() => location.pathname));

  check('nothing threw along the way', errors.length === 0, errors.slice(0, 3).join(' | '));
  console.log('    (' + VIEWS.length + ' nav items on the rail, ' + hrefKeys.length + ' addressable)');

  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
