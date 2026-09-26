/* tests/browser/data-stream-pages.smoke.js
 * ---------------------------------------------------------------------------
 * The five Data Stream screens still work with their code in deferred files.
 *
 * THE CHANGE THIS EXISTS FOR
 * Each screen used to carry its code as a 60KB inline <script> a tenth of the
 * way through the page, right after the engine modules it reads. Inline
 * scripts cannot be deferred, so the modules could not be either, and the
 * parser stopped there to fetch and run 182–272KB of JavaScript before it had
 * seen the markup that is the screen. The code moved to data-stream-*-app.js,
 * loaded with `defer` after the modules, which are deferred too.
 *
 * WHAT COULD HAVE BROKEN, AND WHAT THIS CHECKS
 * A classic script's top-level `function dsFoo(){}` is a global whether it is
 * inline or external — but only if it really is loaded as a classic script,
 * really runs, and really runs AFTER the modules it reads from at top level
 * (`const DS = window.CygenixDataStream` on line 2 of each). Get any of that
 * wrong and every onclick= on the page throws ReferenceError the first time
 * somebody clicks, and nothing says so until they do.
 *
 * So for each page this reads every onclick= handler out of the markup and
 * asks the live page whether that function exists — the exact call the
 * click would make, without making it. It also checks the page threw nothing
 * while loading, that the DOMContentLoaded boot ran, and that the modules
 * the app reads on its first line were there when it ran.
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/data-stream-pages.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8417);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const USER = 'you@example.test';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.json': 'application/json',
  '.woff2': 'font/woff2' };

const ROUTES = (() => {
  const m = {};
  fs.readFileSync(path.join(PUB, '_redirects'), 'utf8').split('\n').forEach((l) => {
    const x = l.trim().match(/^(\/\S*)\s+(\/\S+)\s+200$/);
    if (x) m[x[1]] = x[2];
  });
  return m;
})();

const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/') p = '/index.html';
  if (ROUTES[p]) p = ROUTES[p];
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) {
    res.writeHead(404); return res.end('no');
  }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

const PAGES = [
  ['data_stream.html',          'data-stream-app.js'],
  ['data_stream_designer.html', 'data-stream-designer-app.js'],
  ['data_stream_events.html',   'data-stream-events-app.js'],
  ['data_stream_monitor.html',  'data-stream-monitor-app.js'],
  ['data_stream_store.html',    'data-stream-store-app.js'],
];

// Every function an onclick= on the page would call. `event.stopPropagation();
// dsOpen(id)` yields dsOpen; the first identifier that is not the event.
function handlersIn(html) {
  const names = new Set();
  for (const m of html.matchAll(/on(?:click|change|input|submit|keydown)="([^"]*)"/g)) {
    for (const id of m[1].matchAll(/(?:^|[;\s(!])([A-Za-z_$][\w$]*)\s*\(/g)) {
      const n = id[1];
      if (!['event', 'this', 'window', 'document', 'if', 'return', 'confirm', 'alert', 'setTimeout'].includes(n)) names.add(n);
    }
  }
  return [...names].sort();
}

const tokenFor = (email) => 'x.' + Buffer.from(JSON.stringify({
  exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: email, oid: 'oid-smoke',
})).toString('base64url') + '.y';

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  const tok = tokenFor(USER);

  await ctx.route('**', (route) => {
    const url = route.request().url();
    if (/\/\.netlify\/functions\/data-proxy\b/.test(url)) {
      if (/\baction=whoami\b/.test(url)) {
        return route.fulfill({ status: 200, contentType: 'application/json',
          body: JSON.stringify({ email: USER, role: 'demo', tier: 'pro', tier_status: 'active' }) });
      }
      return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    }
    if (url.startsWith('http://localhost:' + PORT)) return route.continue();
    return route.fulfill({ status: 200, contentType: 'text/javascript', body: '/* stub */' });
  });

  await ctx.addInitScript((a) => {
    for (const s of [localStorage, sessionStorage]) {
      s.setItem('cygenix_token', a.tok);
      s.setItem('cygenix_expires', String(Date.now() + 3600e3));
    }
    localStorage.setItem('cygenix_onboarded', '1');
    localStorage.setItem('cygenix_user', JSON.stringify({ email: a.USER }));
    localStorage.setItem('cygenix_active_user', a.USER);
    localStorage.setItem('cygenix_tier', 'pro');
    localStorage.setItem('cygenix_cookie_consent', 'all');
    localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify({
      homeAccountId: 'h.t', environment: 'cygenix.ciamlogin.com', tenantId: 't',
      username: a.USER, localAccountId: 'l', authorityType: 'MSSTS' }));
    localStorage.setItem('h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-',
      JSON.stringify({ credentialType: 'IdToken', secret: a.tok,
        expiresOn: String(Math.floor(Date.now() / 1000) + 3600) }));
    localStorage.setItem('cygenix_conv_project', JSON.stringify({ id: 'p1', name: 'Legal Sample Migration' }));
    localStorage.setItem('cygenix_active_project_id', 'p1');
    // Record the order things ran in, from inside the page.
    window.__order = [];
    document.addEventListener('DOMContentLoaded', () => window.__order.push('DCL'));
  }, { tok, USER });

  console.log('Data Stream screens — do they still work with deferred code?\n');

  for (const [page, app] of PAGES) {
    section(page);
    const html = fs.readFileSync(path.join(PUB, page), 'utf8');
    const handlers = handlersIn(html);
    const tab = await ctx.newPage();
    const errors = [];
    tab.on('pageerror', (e) => errors.push(e.message));

    await tab.goto('http://localhost:' + PORT + '/' + page, { waitUntil: 'load' });
    await tab.waitForTimeout(700);

    check('loads without a single uncaught error', errors.length === 0, errors.slice(0, 3).join(' | '));

    const state = await tab.evaluate((names) => ({
      missing: names.filter((n) => typeof window[n] !== 'function'),
      engine: typeof window.CygenixDataStream === 'object' && typeof window.CygenixDataStreamUI === 'object'
        && typeof window.CygenixDataStreamPage === 'object',
      rail: !!document.querySelector('.cyg-sidebar'),
      appTag: (() => { const s = [...document.scripts].find((x) => /data-stream(?:-[a-z]+)?-app\.js/.test(x.src)); return s ? { defer: s.defer, type: s.type || 'classic' } : null; })(),
      dcl: window.__order.includes('DCL'),
      // Something the screen's boot paints. Each page's DOMContentLoaded
      // handler calls P.boot(), which mounts the shared toggle and header.
      painted: !!document.querySelector('[data-ds-scope], .ds-scope, #ds-scope, .ds-toolbar, .ds-kpis, .ds-table, .ds-step, .ds-designer, main, .page'),
    }), handlers);

    check('EVERY onclick= ON THE PAGE RESOLVES TO A FUNCTION (' + handlers.length + ' handlers)',
      state.missing.length === 0, 'missing: ' + state.missing.join(', '));
    check('the app is loaded as a deferred classic script, so its functions are globals',
      state.appTag && state.appTag.defer && state.appTag.type === 'classic', JSON.stringify(state.appTag));
    check('the three engine modules it reads on its first lines were there', state.engine);
    check('the page reached DOMContentLoaded, where the screen boots', state.dcl);
    check('and the rail rendered — the deferred sidebar ran before the screen', state.rail);
    check('and there is a screen', state.painted);

    // The order that matters: engine before app. Deferred scripts run in
    // document order, and the test proves it rather than trusting it.
    const order = await tab.evaluate(() => [...document.scripts]
      .filter((s) => /cygenix-datastream(?:-ui|-page)?\.js|data-stream(?:-[a-z]+)?-app\.js/.test(s.src))
      .map((s) => s.src.replace(/^.*\//, '').replace(/\?.*$/, '')));
    check('engine, ui, page glue, then the screen — in that order in the document',
      /cygenix-datastream\.js.*cygenix-datastream-ui\.js.*cygenix-datastream-page\.js.*-app\.js/.test(order.join(' ')), order.join(' → '));

    await tab.close();
  }

  await ctx.close();
  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
