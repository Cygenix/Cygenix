// tests/browser/data-analyser.smoke.js
//
// The static suite proves the files are wired up. This proves the page works:
// the menu item is lit, the widget mounts inside the console, a pasted CSV is
// identified and profiled, the DDL tab holds a CREATE TABLE, the widget
// follows a theme switch — and, the one claim that matters commercially,
// nothing about the data leaves the browser.
//
// cdnjs and Google Fonts are aborted deliberately. The optional libraries
// must degrade to a message rather than a broken page, and a sandbox with no
// egress is the cheapest way to prove that.
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = 8398;
const EXE = '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const TYPES = {
  '.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8', '.svg': 'image/svg+xml',
};

function serve() {
  return http.createServer((req, res) => {
    let p = decodeURIComponent(req.url.split('?')[0]);
    if (p === '/') p = '/index.html';
    let file = path.join(PUB, p);
    if (!fs.existsSync(file) && fs.existsSync(file + '.html')) file += '.html';
    if (!fs.existsSync(file) || fs.statSync(file).isDirectory()) {
      res.writeHead(404, { 'Content-Type': TYPES['.html'] }); return res.end('not found');
    }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream' });
    fs.createReadStream(file).pipe(res);
  }).listen(PORT);
}

function seed(arg) {
  const token = arg.token, expires = arg.expires;
  [localStorage, sessionStorage].forEach((s) => {
    s.setItem('cygenix_token', token);
    s.setItem('cygenix_expires', String(expires));
  });
  localStorage.setItem('cygenix_onboarded', 'true');
  localStorage.setItem('cygenix_user', JSON.stringify({ email: 'smoke@example.com' }));
  localStorage.setItem('cygenix_tier', 'pro');
  localStorage.setItem('cygenix_cookie_consent', 'all');
  localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify({
    homeAccountId: 'h.t', environment: 'cygenix.ciamlogin.com', tenantId: 't',
    username: 'smoke@example.com', localAccountId: 'l', authorityType: 'MSSTS', name: 'Smoke Test',
  }));
  localStorage.setItem(
    'h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-',
    JSON.stringify({ credentialType: 'IdToken', secret: token, expiresOn: String(Math.floor(expires / 1000)) })
  );
}

// Something recognisable, with the traps the profiler exists to catch.
const SAMPLE = [
  'order_id,customer,order_date,amount,postcode,active',
  '1001,"Smith, John",01/02/2024,£1042.50,LS1 4AP,true',
  '1002,"O\'Brien, Ann",03/04/2024,£3.00,00417,false',
  '1003,Bob Jones,05/06/2024,£12000.00,HU1 1AA,true',
].join('\n');

(async () => {
  const server = serve();
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext();
    const b64 = (o) => Buffer.from(JSON.stringify(o)).toString('base64url');
    const token = b64({ alg: 'none' }) + '.' + b64({ preferred_username: 'smoke@example.com', exp: Math.floor(Date.now() / 1000) + 3600 }) + '.sig';
    await ctx.addInitScript(seed, { token, expires: Date.now() + 3600e3 });

    const requests = [];
    await ctx.route('**/*', (route) => {
      const url = route.request().url();
      requests.push({ url, method: route.request().method(), post: route.request().postData() || '' });
      if (/cdnjs\.cloudflare\.com|fonts\.g(oogleapis|static)\.com/.test(url)) return route.abort();
      // auth-gate asks whoami and sends a verified answer with no tier to
      // /pick-plan — correctly. It fails OPEN on a failure and CLOSED on an
      // empty record, so the stub has to answer like a subscribed account.
      if (/data-proxy.*action=whoami/.test(url)) return route.fulfill({ status: 200, contentType: 'application/json',
        body: JSON.stringify({ email: 'smoke@example.com', tier: 'pro', tier_status: 'trialing' }) });
      if (/data-proxy/.test(url)) return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
      return route.continue();
    });

    const page = await ctx.newPage();
    const errors = [];
    page.on('pageerror', (e) => errors.push(e.message));

    await page.goto('http://localhost:' + PORT + '/data-analyser.html', { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('#analyser.cygx', { timeout: 15000 });

    // ── The menu and the mount ─────────────────────────────────────────
    check('the widget mounted inside the console page', await page.locator('#analyser.cygx').count() === 1);
    check('it is titled for the menu item, not the vendor\'s name',
      (await page.locator('#analyser .cygx-title').innerText()) === 'Data Analyser');
    // The sidebar REPLACES its mount element with its own markup, so nothing
    // is ever found under #cyg-sidebar-mount after it renders. Its items are
    // .cyg-nav-item[data-key], which is what tests/browser/sidebar-nav.smoke.js
    // waits on too.
    await page.waitForFunction(() => !!document.querySelector('.cyg-nav-item'), null, { timeout: 15000 });
    const active = await page.evaluate(() => {
      const el = document.querySelector('.cyg-nav-item[data-key="data-analyser"]');
      return el ? { text: el.textContent.trim(), cls: el.className, aria: el.getAttribute('aria-current') } : null;
    });
    check('the sidebar has the Data Analyser item and it is the active one',
      !!active && /Data Analyser/.test(active.text) && (/active|current/i.test(active.cls) || active.aria === 'page'),
      JSON.stringify(active));

    const themeAttr = await page.getAttribute('#analyser', 'data-theme');
    const consoleTheme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    check('the widget wears the console\'s theme', themeAttr === consoleTheme, themeAttr + ' vs ' + consoleTheme);

    // ── A file goes in ─────────────────────────────────────────────────
    await page.evaluate((text) => window.CygenixAnalyserPage.load(text, 'orders.csv'), SAMPLE);
    await page.waitForSelector('#analyser .cygx-result:not([hidden])', { timeout: 10000 });
    const verdict = (await page.locator('#analyser .cygx-verdict').innerText()).replace(/\s+/g, ' ');
    check('the file is identified as CSV', /\bcsv\b/i.test(verdict), verdict);
    check('and the verdict shows row and column counts', /\b3\b/.test(verdict) && /\b6\b/.test(verdict), verdict);

    const flags = (await page.locator('#analyser .cygx-flags').innerText().catch(() => '')).replace(/\s+/g, ' ');
    check('the ambiguous d/m/y dates are flagged', /AMBIGUOUS_DATE/.test(flags), flags);
    check('and the currency symbols', /CURRENCY_SYMBOLS/.test(flags), flags);

    const result = await page.evaluate(() => {
      const r = window.CygenixAnalyserPage.result;
      return r ? { cols: r.columns.map((c) => c.name + ':' + c.type) } : null;
    });
    check('the postcode column with a leading-zero value stays string, not number',
      !!result && result.cols.some((c) => /^postcode:string$/.test(c)), JSON.stringify(result));

    const sql = await page.evaluate(() => {
      const pre = document.querySelector('#analyser pre, #analyser textarea.cygx-out, #analyser .cygx-out');
      return pre ? (pre.value || pre.textContent) : '';
    });
    check('the SQL tab holds a CREATE TABLE for SQL Server', /CREATE TABLE \[dbo\]\.\[imported_data\]/.test(sql), sql.slice(0, 200));

    // ── The theme follows the console ──────────────────────────────────
    const next = consoleTheme === 'dark' ? 'light' : 'dark';
    await page.evaluate((t) => document.documentElement.setAttribute('data-theme', t), next);
    await page.waitForFunction((t) => document.getElementById('analyser').getAttribute('data-theme') === t, next, { timeout: 3000 });
    check('switching the console theme re-themes the widget without a reload', true);

    // ── Nothing left the page ──────────────────────────────────────────
    const external = requests.filter((r) => !/^http:\/\/localhost:8398\//.test(r.url)
      && !/cdnjs\.cloudflare\.com|fonts\.g(oogleapis|static)\.com/.test(r.url));
    check('no request went anywhere but this origin (plus the aborted font and CDN loads)',
      external.length === 0, external.map((r) => r.url).join(', '));
    const carried = requests.filter((r) => r.post && /Smith, John|LS1 4AP/.test(r.post));
    check('and no request carried the data', carried.length === 0, carried.map((r) => r.url).join(', '));

    const unexpected = errors.filter((e) => !/xlsx|jsyaml|XLSX/i.test(e));
    check('no page errors (the aborted optional libraries excepted)', unexpected.length === 0, unexpected.join(' | '));
  } finally {
    await browser.close();
    server.close();
  }
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})();
