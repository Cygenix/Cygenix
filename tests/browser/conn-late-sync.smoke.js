/* tests/browser/conn-late-sync.smoke.js
 * ---------------------------------------------------------------------------
 * Project Connections fills itself when the cloud copy lands late.
 *
 * THE BUG THIS EXISTS FOR
 * Open /dashboard#goto=connections on a cold page and both cards read "Not
 * configured", even for an account with connections saved. Go to Profiles &
 * integrations, come back, and there they are. The data was never missing.
 *
 * initConnectionsView() reads CygenixConnections.get() once, when the view
 * opens. cygenix-cosmos-sync.js fetches the Cosmos copy over the network and
 * writes it into localStorage when it arrives — after that read. The refill
 * listener beside the view was watching four events, and the closest one,
 * 'storage', does not fire for a write made by the SAME tab. So the view had
 * already read an empty store, the store had since filled, and nothing
 * connected the two. Visiting Profiles worked only because coming back re-ran
 * initConnectionsView from scratch.
 *
 * There are two ways the load finishes and only one of them announced itself:
 *   · cloud HAS data  → applyCloud() → 'cygenix-sync-loaded'
 *   · cloud is EMPTY  → health only  → 'cygenix-sync-health', no load event
 * The second is a first sign-in, where the local copy is the only copy and is
 * exactly what should be on screen. Both are exercised below.
 *
 * WHY THIS HAS TO BE A BROWSER TEST
 * The defect is an ordering relationship between a network response, a
 * same-tab localStorage write and a DOM read. Every piece was correct on its
 * own; what was missing was an edge between two of them. Only something that
 * runs the real page against a real (here: controlled) response ordering can
 * tell you the edge is there — and, just as important, that adding it did not
 * turn one render into a loop.
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/conn-late-sync.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8413);
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

// What the cloud is holding for this account: a source and a target that the
// LOCAL store does not have. If either lands on screen, it came from the
// cloud copy and therefore from a refill after the first read.
// No password in these: the live pair is uploaded with its credentials
// stripped and they are re-merged from whatever this device holds, so a
// string with a password in it would not survive the round trip unchanged
// and the test would be measuring that instead of the refill.
const CLOUD_SRC = 'Server=cloud-src.example.net;Database=SalesLive;User Id=u;';
const CLOUD_TGT = 'Server=cloud-tgt.example.net;Database=SalesTarget;User Id=u;';

const tokenFor = (email) => 'x.' + Buffer.from(JSON.stringify({
  exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: email, oid: 'oid-smoke',
})).toString('base64url') + '.y';

/* One page, wired so the data-proxy 'load' response can be held back until
   after the Connections view has opened and read an empty store. `cloud`
   false serves the verified-empty response instead, which is the first
   sign-in path that never dispatches a load event. */
async function openConsole(browser, { cloud }) {
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  const tok = tokenFor(USER);
  let release = null;
  const held = new Promise((r) => { release = r; });
  let loadCalls = 0;

  // cygenix-data-api.js puts the action in the QUERY STRING and sends the
  // load as a GET with no body, so match on the URL rather than postData.
  // The body it returns is the stored blob itself — callResult hands back the
  // parsed JSON and applyCloud reads its keys directly.
  await ctx.route('**', async (route) => {
    const url = route.request().url();
    // The proxy is same-origin (/.netlify/functions/data-proxy), so this has
    // to be matched BEFORE the static passthrough or the local server answers
    // it with a 404 and the sync module correctly reports the cloud as
    // unreachable — which is a different story from the one under test.
    if (/\/\.netlify\/functions\/data-proxy\b/.test(url)) {
      // auth-gate.js reads whoami and sends anyone without a tier in good
      // standing to /pick-plan. An empty 200 is "no tier", so the dashboard
      // would never render and this file would time out looking for a field
      // on a page that had navigated away. 'demo' bypasses the tier check.
      if (/\baction=whoami\b/.test(url)) {
        return route.fulfill({ status: 200, contentType: 'application/json',
          body: JSON.stringify({ email: USER, role: 'demo', tier: 'pro', tier_status: 'active' }) });
      }
      if (!/\baction=load\b/.test(url)) {
        return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
      }
      loadCalls++;
      await held;                                   // the late cloud response
      return route.fulfill({
        status: 200, contentType: 'application/json',
        body: JSON.stringify(cloud
          // The cloud field is `connections`; FIELD_MAP in
          // cygenix-cosmos-sync.js maps it to cygenix_project_connections.
          ? { connections: { [USER]: {
                srcConnString: CLOUD_SRC, srcConnMode: 'direct',
                tgtConnString: CLOUD_TGT, tgtConnMode: 'direct',
                srcFnUrl: '', srcFnKey: '', tgtFnUrl: '', tgtFnKey: '' } } }
          : {}),                                     // verified, and empty
      });
    }
    if (url.startsWith('http://localhost:' + PORT)) return route.continue();
    return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
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
    // The LOCAL copy is deliberately bare on the cloud run: whatever appears
    // has to have arrived from the response held above.
    if (a.local) localStorage.setItem('cygenix_project_connections', JSON.stringify(a.local));

    // Count the renders from inside the page. A listener that re-triggers its
    // own source shows up here as a number that keeps climbing.
    window.__initCount = 0;
    const tick = () => { window.__initCount++; };
    const iv = setInterval(() => {
      if (typeof window.initConnectionsView !== 'function') return;
      clearInterval(iv);
      const real = window.initConnectionsView;
      window.initConnectionsView = function () { tick(); return real.apply(this, arguments); };
    }, 10);
  }, { tok, USER, local: cloud ? null : { [USER]: {
        srcConnString: CLOUD_SRC, srcConnMode: 'direct',
        tgtConnString: CLOUD_TGT, tgtConnMode: 'direct' } } });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  // SMOKE_DEBUG=1 prints the page's own log and every proxy request. Left in
  // because three separate harness faults hid behind a silent page here — an
  // unanswered whoami that redirected to /pick-plan, a same-origin proxy call
  // falling through to the static server, and the wrong cloud field name —
  // and each one looked exactly like "the fix does not work".
  if (process.env.SMOKE_DEBUG) {
    page.on('console', (m) => console.log('    [page]', m.text().slice(0, 180)));
    page.on('request', (r) => {
      if (/data-proxy/.test(r.url())) console.log('    [req]', r.method(), r.url().slice(0, 120));
    });
  }
  return { ctx, page, release, errors, loads: () => loadCalls };
}

const srcValue = (page) => page.evaluate(() => {
  const el = document.getElementById('proj-src-cs');
  return el ? el.value : null;
});
const tgtValue = (page) => page.evaluate(() => {
  const el = document.getElementById('proj-tgt-cs');
  return el ? el.value : null;
});

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });

  console.log('Project Connections — does it fill when the cloud lands late?\n');

  /* ── 1. The reported case: cloud has data, arrives after the view ──────── */
  section('1. The cloud copy arrives after the view has opened');

  {
    const t = await openConsole(browser, { cloud: true });
    await t.page.goto('http://localhost:' + PORT + '/dashboard#goto=connections',
      { waitUntil: 'domcontentloaded' });
    // The field of record is hidden while the builder is the entry mode;
    // a hidden input still carries its value, which is what is read here.
    await t.page.waitForSelector('#proj-src-cs', { state: 'attached', timeout: 15000 });
    await t.page.waitForSelector('#view-connections', { state: 'visible', timeout: 15000 });
    await t.page.waitForTimeout(900);

    const before = await srcValue(t.page);
    check('the view opens on an empty store, exactly as reported',
      !before, JSON.stringify(before));
    check('and the load it is waiting on has genuinely been issued',
      t.loads() > 0, t.loads() + ' load call(s)');

    const rendersBefore = await t.page.evaluate(() => window.__initCount);

    t.release();                                     // the cloud answers, late
    // The fix has one second to land; the bug never lands at all.
    await t.page.waitForFunction(
      (want) => (document.getElementById('proj-src-cs') || {}).value === want,
      CLOUD_SRC, { timeout: 4000 },
    ).catch(() => {});
    await t.page.waitForTimeout(200);

    const after = await srcValue(t.page);
    const afterT = await tgtValue(t.page);
    check('THE SOURCE CARD FILLS ITSELF, WITH NO TRIP TO PROFILES',
      after === CLOUD_SRC, JSON.stringify(after));
    check('and so does the target', afterT === CLOUD_TGT, JSON.stringify(afterT));

    const rendersAfter = await t.page.evaluate(() => window.__initCount);
    check('it cost ONE more render, not a stream of them',
      rendersAfter - rendersBefore >= 1 && rendersAfter - rendersBefore <= 2,
      rendersBefore + ' → ' + rendersAfter);

    // The loop this could have been: a refill that wrote would land in the
    // sync module's write-behind and come back as another sync event.
    await t.page.waitForTimeout(1500);
    const settled = await t.page.evaluate(() => window.__initCount);
    check('AND IT SETTLES — the count stops climbing once the data is in',
      settled === rendersAfter, rendersAfter + ' → ' + settled);
    check('nothing threw', t.errors.length === 0, t.errors.slice(0, 2).join(' | '));
    await t.ctx.close();
  }

  /* ── 2. First sign-in: the cloud is verified empty ─────────────────────── */
  section('2. The cloud is verified empty — the local copy is the only copy');

  /* WHAT THIS SECTION FOUND, and it is worth stating because it changes what
     the second listener is for.

     applyCloud() is the ONLY path in cygenix-cosmos-sync.js that writes to
     localStorage, and it always dispatches 'cygenix-sync-loaded'. The two
     verified-empty paths (loadDetailed and init) write nothing; they only
     move health. And setHealth() compares degraded|reason|pendingSaves and
     returns early when they are unchanged — so on a clean first sign-in,
     where nothing was ever degraded, NO health event is dispatched either.

     Which means: the first listener is what fixes the reported bug, because
     every path that puts new data in the store announces itself. The health
     listener is belt and braces for a load that was degraded and has since
     recovered. On this path there is nothing new to show, and the right
     behaviour is for the screen not to move. */

  {
    const t = await openConsole(browser, { cloud: false });
    await t.page.goto('http://localhost:' + PORT + '/dashboard#goto=connections',
      { waitUntil: 'domcontentloaded' });
    await t.page.waitForSelector('#proj-src-cs', { state: 'attached', timeout: 15000 });
    await t.page.waitForSelector('#view-connections', { state: 'visible', timeout: 15000 });
    await t.page.waitForTimeout(700);
    const rendersBefore = await t.page.evaluate(() => window.__initCount);

    t.release();                                     // "verified, and empty"
    await t.page.waitForTimeout(1200);

    check('the local connection is still on screen — an empty cloud clears nothing',
      (await srcValue(t.page)) === CLOUD_SRC, JSON.stringify(await srcValue(t.page)));
    const rendersAfter = await t.page.evaluate(() => window.__initCount);
    check('and the view does not redraw, because nothing was written to redraw from',
      rendersAfter === rendersBefore, rendersBefore + ' → ' + rendersAfter);

    // Now the case the health listener IS for: a load that reports itself
    // through health. The first lastLoadAt transition may refill; every later
    // health event must not, or a builder open mid-edit would be reset under
    // the person typing into it.
    await t.page.evaluate(() => {
      window.dispatchEvent(new CustomEvent('cygenix-sync-health', {
        detail: { lastLoadAt: new Date().toISOString(), degraded: false },
      }));
    });
    await t.page.waitForTimeout(400);
    const afterFirst = await t.page.evaluate(() => window.__initCount);
    check('a health event carrying the first lastLoadAt refills once',
      afterFirst === rendersAfter + 1, rendersAfter + ' → ' + afterFirst);

    await t.page.evaluate(() => {
      for (let i = 0; i < 5; i++) {
        window.dispatchEvent(new CustomEvent('cygenix-sync-health', {
          detail: { lastLoadAt: new Date().toISOString(), degraded: i % 2 === 0 },
        }));
      }
    });
    await t.page.waitForTimeout(400);
    const afterNoise = await t.page.evaluate(() => window.__initCount);
    check('FIVE MORE CHANGE NOTHING — it is the transition that refills, not the event',
      afterNoise === afterFirst, afterFirst + ' → ' + afterNoise);
    check('nothing threw', t.errors.length === 0, t.errors.slice(0, 2).join(' | '));
    await t.ctx.close();
  }

  /* ── 3. The listener is read-only ──────────────────────────────────────── */
  section('3. The refill reads and paints, and writes nothing');

  {
    const t = await openConsole(browser, { cloud: true });
    await t.page.goto('http://localhost:' + PORT + '/dashboard#goto=connections',
      { waitUntil: 'domcontentloaded' });
    // The field of record is hidden while the builder is the entry mode;
    // a hidden input still carries its value, which is what is read here.
    await t.page.waitForSelector('#proj-src-cs', { state: 'attached', timeout: 15000 });
    await t.page.waitForSelector('#view-connections', { state: 'visible', timeout: 15000 });
    await t.page.evaluate(() => {
      window.__writes = [];
      const real = localStorage.setItem.bind(localStorage);
      localStorage.setItem = function (k, v) { window.__writes.push(k); return real(k, v); };
    });
    await t.page.waitForTimeout(400);
    t.release();
    await t.page.waitForFunction(
      (want) => (document.getElementById('proj-src-cs') || {}).value === want,
      CLOUD_SRC, { timeout: 4000 },
    ).catch(() => {});
    await t.page.waitForTimeout(400);

    // The sync module writes the keys it just fetched — that is its job. What
    // must not appear is a write made BY the refill, so the connections key
    // should be written by sync and not written again afterwards.
    const writes = await t.page.evaluate(() => window.__writes.slice());
    const connWrites = writes.filter((k) => k === 'cygenix_project_connections').length;
    check('the refill adds no write of its own to the connections key',
      connWrites <= 1, connWrites + ' write(s): ' + writes.join(', ').slice(0, 160));
    check('nothing threw', t.errors.length === 0, t.errors.slice(0, 2).join(' | '));
    await t.ctx.close();
  }

  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
