// tests/browser/sync-degraded.smoke.js
//
// The static tests assert that the banner's source does the right things. This
// asserts that a real page, with a real 503 from the data layer, actually
// shows it — which is the only claim that matters, because the whole failure
// being fixed here is one that was technically reported and practically
// invisible for three weeks.
//
// Runs the real dashboard against a stubbed /.netlify/functions/data-proxy:
// first refusing every call the way the broken deployment did, then answering,
// so the recovery path is exercised too.
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = 8397;
const EXE = '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
};

function serve() {
  return http.createServer((req, res) => {
    // Strip the ?v= asset stamp; the files on disk carry no hash in their name.
    let p = decodeURIComponent(req.url.split('?')[0]);
    if (p === '/') p = '/index.html';
    let file = path.join(PUB, p);
    if (!fs.existsSync(file) && fs.existsSync(file + '.html')) file += '.html';
    if (!fs.existsSync(file) || fs.statSync(file).isDirectory()) {
      res.writeHead(404, { 'Content-Type': TYPES['.html'] });
      return res.end('not found');
    }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream' });
    fs.createReadStream(file).pipe(res);
  }).listen(PORT);
}

// The auth state a signed-in browser carries; without it auth-gate.js
// redirects to the login page before anything under test runs.
function seed(arg) {
  const token = arg.token, expires = arg.expires;
  const acct = {
    homeAccountId: 'h.t', environment: 'cygenix.ciamlogin.com', tenantId: 't',
    username: 'smoke@example.com', localAccountId: 'l', authorityType: 'MSSTS',
    name: 'Smoke Test',
  };
  [localStorage, sessionStorage].forEach((s) => {
    s.setItem('cygenix_token', token);
    s.setItem('cygenix_expires', String(expires));
  });
  localStorage.setItem('cygenix_onboarded', 'true');
  localStorage.setItem('cygenix_user', JSON.stringify({ email: 'smoke@example.com' }));
  localStorage.setItem('cygenix_tier', 'pro');
  localStorage.setItem('cygenix_cookie_consent', 'all');
  localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify(acct));
  // cygenix-auth-token.js finds the bearer token by scanning localStorage for
  // an MSAL IdToken CREDENTIAL record under a key carrying the tenant marker.
  // An account record alone is not enough — without this every call
  // short-circuits at 'no-token' and never reaches the network at all.
  localStorage.setItem(
    'h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-',
    JSON.stringify({ credentialType: 'IdToken', secret: token, expiresOn: String(Math.floor(expires / 1000)) })
  );
  // Something worth losing, so the pending count has a reason to be non-zero.
  localStorage.setItem('cygenix_jobs', JSON.stringify([{ id: 'j1', name: 'Existing job' }]));
}

(async () => {
  const server = serve();
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  let mode = 'broken';   // flipped to 'healthy' partway through

  try {
    const ctx = await browser.newContext();
    const b64 = (o) => Buffer.from(JSON.stringify(o)).toString('base64url');
    const token = b64({ alg: 'none' }) + '.'
      + b64({ preferred_username: 'smoke@example.com', exp: Math.floor(Date.now() / 1000) + 3600 })
      + '.sig';
    await ctx.addInitScript(seed, { token: token, expires: Date.now() + 3600e3 });

    // The broken deployment, exactly: 503 with the code the proxy now sends.
    await ctx.route('**/data-proxy*', (route) => {
      if (mode === 'broken') {
        return route.fulfill({
          status: 503,
          contentType: 'application/json',
          body: JSON.stringify({
            error: 'CYGENIX_DATA_FN_KEY is not configured for this deployment.',
            code: 'no-fn-key',
          }),
        });
      }
      const isSave = /action=save/.test(route.request().url());
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(isSave ? { saved: true, updatedAt: new Date().toISOString() } : {}),
      });
    });

    const page = await ctx.newPage();
    const errors = [];
    page.on('pageerror', (e) => errors.push(e.message));

    await page.goto('http://localhost:' + PORT + '/dashboard.html', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => !!window.CygenixSync, null, { timeout: 15000 });

    // ── The outage, as a user would meet it ──────────────────────────────
    const bar = page.locator('#cygenix-sync-bar');
    await bar.waitFor({ state: 'visible', timeout: 15000 });
    check('a 503 from the data layer puts a visible bar on the page', await bar.isVisible());

    const text = (await bar.innerText()).replace(/\s+/g, ' ');
    check('it leads with the consequence, not the error code',
      /not being saved/i.test(text), text);
    check('it names a cause the user can act on',
      /administrator needs to set it/i.test(text), text);
    check('it offers to get the data off this device',
      /Download my data/i.test(text), text);
    check('and it does not leak the variable value or a fragment of one',
      !/sk-|[A-Za-z0-9_-]{30,}/.test(text), text);

    const health = await page.evaluate(() => window.CygenixSync.getHealth());
    check('the sync layer reports the failure as a configuration fault',
      health.degraded === true && health.reason === 'config', JSON.stringify(health));
    check('and does not claim to have verified anything',
      health.verified === false, JSON.stringify(health));

    // ── Local data survived it ───────────────────────────────────────────
    const jobs = await page.evaluate(() => localStorage.getItem('cygenix_jobs'));
    check('nothing local was cleared on the strength of a failed load',
      /Existing job/.test(jobs || ''), String(jobs));

    // ── An edit made during the outage is queued, not lost ───────────────
    await page.evaluate(() => {
      localStorage.setItem('cygenix_sys_params', JSON.stringify({ madeDuringOutage: true }));
    });
    await page.waitForFunction(() => window.CygenixSync.getHealth().pendingSaves > 0,
      null, { timeout: 5000 });
    const pending = await page.evaluate(() => window.CygenixSync.getHealth().pendingSaves);
    check('an edit made while the cloud is down is counted as unsaved', pending > 0, String(pending));
    await page.waitForFunction(() => /held on this device/i.test(
      (document.getElementById('cygenix-sync-bar') || {}).innerText || ''), null, { timeout: 5000 });
    check('and the bar says so, in the user\'s terms',
      /held on this device/i.test(await bar.innerText()));

    // ── The deployment gets fixed ────────────────────────────────────────
    mode = 'healthy';
    await page.getByRole('button', { name: 'Retry now' }).click();
    await page.waitForFunction(() => {
      const h = window.CygenixSync.getHealth();
      return h.degraded === false && h.pendingSaves === 0;
    }, null, { timeout: 15000 });
    check('setting the variable lets the queued work go up on its own', true);
    await bar.waitFor({ state: 'detached', timeout: 10000 }).catch(() => {});
    check('and the bar goes away rather than lingering',
      await page.locator('#cygenix-sync-bar').count() === 0);

    check('no page errors throughout', errors.length === 0, errors.join(' | '));
  } finally {
    await browser.close();
    server.close();
  }

  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})();
