// tests/browser/login-busy.smoke.js
//
// tests/login-busy.test.js reads the markup. This asks the browser the one
// question the report was actually about: with the sign-in page open and auth
// still resolving, IS THERE ANYTHING ON THE SCREEN?
//
// That cannot be answered from source. The old page set #loading to
// display:block and set its message — both true in the markup — while the
// element's parent was display:none, so it rendered nothing. A source test
// would have passed. innerText of the form panel would have been ''.
//
// So every check below measures rendered geometry or rendered text, and the
// central one is deliberately blunt: the panel must not be empty.
//
// MSAL is stubbed rather than loaded. The real library redirects to
// cygenix.ciamlogin.com, which is not reachable from a test and would not be
// deterministic if it were. What matters here is the SHAPE of the wait — a
// promise that takes a while to settle — so the stub takes a configurable
// number of milliseconds and the page cannot tell the difference.
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = 8401;
const EXE = '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.txt': 'text/plain; charset=utf-8' };

function serve() {
  return http.createServer((req, res) => {
    let p = decodeURIComponent(req.url.split('?')[0].split('#')[0]);
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

/* The stub. Installed before any page script runs, so the page's
   waitForMsal() finds it immediately and the only delay is the one asked for.
   handleRedirectPromise is the slow call on a cold visit. */
const stubMsal = (delayMs) => {
  window.__msalDelay = delayMs;
  window.msal = {
    PublicClientApplication: function () {
      return {
        initialize: () => Promise.resolve(),
        handleRedirectPromise: () => new Promise((r) => setTimeout(() => r(null), window.__msalDelay)),
        getAllAccounts: () => [],
        acquireTokenSilent: () => Promise.reject(new Error('no account')),
        loginRedirect: () => new Promise(() => {}),   // never settles, like a real redirect
        clearCacheForAccount: () => {},
      };
    },
  };
};

/* What is actually on the screen. Geometry, not CSS declarations.
   Every lookup tolerates a missing element on purpose: run this against the
   page as it was and #signin-ui does not exist, and a TypeError here would
   report "Cannot read properties of null" instead of the finding, which is
   that the panel is blank. A test should fail with the answer. */
const snap = () => {
  const el = (id) => document.getElementById(id);
  const vis = (n) => { if (!n) return false; const r = n.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
  const text = (n) => (n ? n.textContent : '');
  const panel = document.querySelector('.form-panel');
  const loading = el('loading');
  const signin = el('signin-ui');
  const spin = document.querySelector('.spinner');
  const bar = el('cygBusyBar');
  return {
    panelText: ((panel && panel.innerText) || '').trim(),
    loadingVisible: vis(loading),
    spinnerVisible: vis(spin),
    spinnerTop: spin ? getComputedStyle(spin).borderTopColor : null,
    loadingBg: loading ? getComputedStyle(loading).backgroundColor : null,
    signinVisible: vis(signin),
    googleVisible: vis(el('google-btn')),
    msg: text(el('loading-msg')),
    hint: text(el('loading-hint')),
    busyClass: el('main-card') ? el('main-card').className : '',
    topBarOn: bar ? bar.classList.contains('on') : false,
    loadingInSignin: !!(signin && loading && signin.contains(loading)),
  };
};

(async () => {
  const server = serve();
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1280, height: 860 } });
    // Fonts and the MSAL CDN are cut off: the stub replaces the library, and a
    // test that depends on Google's uptime is not a test.
    await ctx.route('**/*', (r) => (r.request().url().startsWith('http://localhost:' + PORT)
      ? r.continue() : r.abort()));
    const page = await ctx.newPage();
    const problems = [];
    page.on('pageerror', (e) => problems.push(e.message));

    /* ── A slow session check — the reported case ───────────────────────── */
    await page.addInitScript(stubMsal, 9000);
    await page.goto('http://localhost:' + PORT + '/login.html', { waitUntil: 'domcontentloaded' });

    // Immediately. This is the regression: the panel used to be empty here.
    await page.waitForTimeout(120);
    let s = await page.evaluate(snap);
    check('the panel is not blank while auth resolves — the reported fault',
      s.panelText.length > 20, JSON.stringify(s.panelText));
    check('the spinner is on screen from the start, and the sign-in controls are not',
      s.spinnerVisible && s.loadingVisible && !s.signinVisible && !s.googleVisible, JSON.stringify(s));
    check('and the loader is not inside the element that gets hidden',
      s.loadingInSignin === false);
    check('it says what is happening, not just that something is',
      /Checking your session/.test(s.msg) && /Microsoft Entra/.test(s.hint),
      s.msg + ' / ' + s.hint);
    // White on white was the second fault. Compare the two rendered colours.
    check('the spinner is a different colour from the surface it is drawn on',
      s.spinnerTop !== s.loadingBg && s.spinnerTop !== 'rgb(255, 255, 255)',
      s.spinnerTop + ' on ' + s.loadingBg);

    // The shared top bar joins once the deferred module has loaded.
    await page.waitForFunction(() => {
      const b = document.getElementById('cygBusyBar');
      return b && b.classList.contains('on');
    }, null, { timeout: 8000 }).catch(() => {});
    s = await page.evaluate(snap);
    check('the console\'s shared busy bar is running too', s.topBarOn, JSON.stringify(s.busyClass));

    // The counter — "is it stuck?" — appears once the wait is worth timing.
    await page.waitForFunction(() => / · \d+s$/.test(document.getElementById('loading-msg').textContent),
      null, { timeout: 8000 });
    s = await page.evaluate(snap);
    check('an elapsed counter appears, so a long wait reads as running rather than hung', / · \d+s$/.test(s.msg), s.msg);

    // And the sentence escalates rather than repeating itself for ever.
    await page.waitForFunction(() => /Still checking/.test(document.getElementById('loading-msg').textContent),
      null, { timeout: 12000 });
    s = await page.evaluate(snap);
    check('and the sentence escalates once it runs long', /Still checking with Microsoft Entra/.test(s.msg), s.msg);
    check('the controls are still withheld while it works', !s.googleVisible);

    /* ── It ends ────────────────────────────────────────────────────────── */
    // A busy state that never clears is worse than none. The stub settles at
    // 9s with no account, which is the "show them the login form" path.
    await page.waitForFunction(() => {
      const g = document.getElementById('google-btn');
      const r = g.getBoundingClientRect();
      return r.width > 0 && r.height > 0;
    }, null, { timeout: 15000 });
    s = await page.evaluate(snap);
    check('when the check finishes the sign-in controls appear',
      s.googleVisible && s.signinVisible && /Welcome back/.test(s.panelText), JSON.stringify(s.panelText.slice(0, 60)));
    check('and the spinner is gone, not left turning behind the form',
      !s.loadingVisible && !/is-busy/.test(s.busyClass), s.busyClass);
    check('the shared top bar is released as well', !s.topBarOn);

    /* ── A fast session check must not flash ───────────────────────────── */
    // Most visits resolve quickly. A spinner that appears and vanishes in
    // 90ms reads as a glitch; the sign-in form should simply be there.
    const fast = await ctx.newPage();
    await fast.addInitScript(stubMsal, 40);
    await fast.goto('http://localhost:' + PORT + '/login.html', { waitUntil: 'domcontentloaded' });
    await fast.waitForTimeout(600);
    const f = await fast.evaluate(snap);
    check('a fast check lands on the form with no counter and no top bar',
      f.googleVisible && !f.loadingVisible && !f.topBarOn && !/ · \d+s/.test(f.msg),
      JSON.stringify({ g: f.googleVisible, l: f.loadingVisible, b: f.topBarOn }));

    /* ── Pressing a button says something too ───────────────────────────── */
    await fast.click('#email-btn');
    await fast.waitForTimeout(250);
    const g = await fast.evaluate(snap);
    check('pressing sign in replaces the form with a spinner and a reason',
      g.spinnerVisible && !g.googleVisible && /Redirecting to sign in/.test(g.msg)
      && /Microsoft sign-in page/.test(g.hint), g.msg + ' / ' + g.hint);

    check('no console errors anywhere', problems.length === 0, problems.join(' | '));
    await ctx.close();
  } finally {
    await browser.close();
    server.close();
  }
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})();
