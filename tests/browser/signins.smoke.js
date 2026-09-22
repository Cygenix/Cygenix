/* tests/browser/signins.smoke.js
 * ---------------------------------------------------------------------------
 * The Sign-ins tab on the Audit Log screen, in a browser.
 *
 * tests/login-audit.test.js proves the rules: what the edge function records,
 * what the read action will answer, and what the label helpers return. What
 * it cannot show is whether the tab is reachable, whether it loads once
 * rather than on every click, whether the scope toggle works, what happens
 * when the server refuses the wider scope, and whether any of it throws.
 *
 * The two backends this screen talks to are both stubbed at the network: the
 * audit function for the rest of the screen, and the data proxy for the
 * sign-ins themselves. Every request to either is counted, because "does it
 * loop" is the assertion that matters most here and cannot be made any
 * other way.
 *
 * Not part of `npm test`: it needs a browser.
 *   node tests/browser/signins.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8421);
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

// auth-gate.js redirects to /login unless localStorage holds an MSAL account
// record and an unexpired credential, so those are seeded too. Standing up a
// real MSAL cache would be testing cygenix-auth-token.js rather than this
// screen; these are the two records the gate actually reads.
const SEED = () => {
  localStorage.setItem('cygenix_user', JSON.stringify({ email: 'owner@example.test', name: 'An Owner' }));
  localStorage.setItem('cygenix_active_user', 'owner@example.test');
  localStorage.setItem('cygenix_cookie_consent', 'all');
  localStorage.setItem('cygenix_token', 'smoke-token');
  localStorage.setItem('cygenix_expires', String(Date.now() + 3600e3));
  localStorage.setItem('cygenix_tier', 'pro');
  localStorage.setItem('cygenix_projects', JSON.stringify([{ id: 'p1', name: 'Demo migration' }]));
  localStorage.setItem('cygenix_active_project_id', 'p1');
  localStorage.setItem('cygenix_onboarded', '1');
  localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify({
    homeAccountId: 'h.t', environment: 'cygenix.ciamlogin.com', tenantId: 't',
    username: 'owner@example.test', localAccountId: 'l', authorityType: 'MSSTS', name: 'An Owner' }));
  localStorage.setItem('h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-',
    JSON.stringify({ credentialType: 'IdToken', secret: 'smoke-token',
      expiresOn: String(Math.floor(Date.now() / 1000) + 3600) }));
};

const iso = (minsAgo) => new Date(Date.now() - minsAgo * 60000).toISOString();
const UA_EDGE = 'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36 Edg/120';
const UA_SAFARI = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1 Version/17 Safari/605.1';

const MINE = [
  { id: 'a', userId: 'owner@example.test', email: 'owner@example.test', type: 'signin', timestamp: iso(5),
    idp: 'local', ip: '203.0.113.9', city: 'Leeds', region: 'England', country: 'United Kingdom',
    countryCode: 'GB', userAgent: UA_EDGE },
  { id: 'b', userId: 'owner@example.test', email: 'owner@example.test', type: 'signin', timestamp: iso(2000),
    idp: 'google.com', ip: '198.51.100.4', city: null, region: null, country: null,
    countryCode: null, userAgent: UA_SAFARI },
];
const EVERYONE = MINE.concat([
  { id: 'c', userId: 'other@example.test', email: 'other@example.test', type: 'signin', timestamp: iso(60),
    idp: 'local', ip: '192.0.2.7', city: 'Dublin', region: null, country: 'Ireland',
    countryCode: 'IE', userAgent: UA_EDGE },
]);

const world = { adminAllowed: true, calls: [], failNext: false };

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 960 } });

  await ctx.route('**', (route) => {
    const u = route.request().url();
    const json = (status, body) => route.fulfill({
      status, contentType: 'application/json', body: JSON.stringify(body) });

    // The sign-ins read, through the data proxy.
    if (u.indexOf('action=audit-signins') !== -1) {
      const q = new URL(u).searchParams;
      const scope = q.get('scope') || 'mine';
      world.calls.push({ scope, days: q.get('days') });
      if (world.failNext) { world.failNext = false; return json(500, { error: 'Cosmos unavailable' }); }
      if (scope === 'all' && !world.adminAllowed) return json(403, { error: 'Admin role required' });
      return json(200, {
        signins: scope === 'all' ? EVERYONE : MINE,
        scope, days: Number(q.get('days') || 30),
      });
    }
    // The rest of the audit screen: enough for render() to get past its gates.
    if (u.indexOf('/.netlify/functions/audit') !== -1) {
      return json(200, {
        state: 'recording', settings: { categories: {}, storeIp: true, storeDiffs: true, retentionDays: 365 },
        categories: [], kpis: { events24h: 42, actors: 3, prodChanges: 0, deniedOrFailed: 0 },
        headSeq: 42, lastVerify: null, gaps: [], entries: [], total: 0, nextCursor: null, indexed: true,
      });
    }
    if (u.indexOf('action=whoami') !== -1) return json(200, { tier: 'pro', tier_status: 'active', role: 'admin' });
    if (/data-proxy|netlify\/functions|\/api\//.test(u)) return json(200, {});
    if (u.startsWith('http://localhost:' + PORT)) return route.continue();
    return route.abort();
  });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('console', (m) => {
    if (m.type() === 'error' && !/ERR_|Failed to load resource/.test(m.text())) errors.push(m.text());
  });
  await page.addInitScript(SEED);

  const openAudit = async () => {
    await page.waitForFunction(() => !!window.CygenixAuditView, null, { timeout: 20000 });
    await page.evaluate(() => {
      window.getCygenixIdToken = () => 'smoke-token';
      document.querySelectorAll('.view').forEach((v) => v.classList.remove('active'));
      const v = document.getElementById('view-audit');
      v.classList.add('active');
      v.style.display = 'block';
      window.CygenixAuditView.render(document.getElementById('audit-log-wrap'));
    });
    await page.waitForSelector('.cyg-a-tabs', { timeout: 10000 });
  };
  const rows = () => page.$$eval('#cyg-a-panel-signins tbody tr', (rs) => rs.map((r) => r.innerText.replace(/\s+/g, ' ').trim()));
  const panelText = () => page.textContent('#cyg-a-panel-signins');

  console.log('Sign-ins — the tab, in a browser\n');

  await page.goto('http://localhost:' + PORT + '/dashboard', { waitUntil: 'domcontentloaded' });
  await openAudit();

  /* ── 1. The tab exists and does not load until asked ──────────────────── */
  console.log('1. Reaching it');
  check('the audit screen shows five tabs, with Sign-ins second',
    (await page.$$eval('.cyg-a-tab', (ts) => ts.map((t) => t.textContent.trim()))).join('|') === 'Events|Sign-ins|Capture settings|Integrity|Retention',
    (await page.$$eval('.cyg-a-tab', (ts) => ts.map((t) => t.textContent.trim()))).join('|'));
  check('NOTHING IS FETCHED BEFORE THE TAB IS OPENED — the audit screen does not pay for a tab nobody looked at',
    world.calls.length === 0, JSON.stringify(world.calls));
  check('the panel says so rather than looking broken',
    (await panelText()).indexOf('Open this tab to load') !== -1);

  /* ── 2. Opening it loads once ─────────────────────────────────────────── */
  console.log('\n2. Opening it');
  await page.click('#cyg-a-tab-signins');
  await page.waitForSelector('#cyg-a-panel-signins tbody tr', { timeout: 10000 });
  check('one request went out, for the caller\'s own history over 30 days',
    world.calls.length === 1 && world.calls[0].scope === 'mine' && world.calls[0].days === '30',
    JSON.stringify(world.calls));
  check('the panel is visible and the tab is selected',
    await page.isVisible('#cyg-a-panel-signins')
    && (await page.getAttribute('#cyg-a-tab-signins', 'aria-selected')) === 'true');

  const r1 = await rows();
  check('both sign-ins are listed, newest first', r1.length === 2 && /Leeds/.test(r1[0]), JSON.stringify(r1));
  check('the place reads as a person would write it, with the country code beside it',
    /Leeds, England, United Kingdom/.test(r1[0]) && /GB/.test(r1[0]), r1[0]);
  check('EDGE IS NOT REPORTED AS CHROME', /Edge on Windows/.test(r1[0]) && !/Chrome/.test(r1[0]), r1[0]);
  check('a password sign-in reads "Password", not "local"', /Password/.test(r1[0]) && !/local/.test(r1[0]), r1[0]);
  check('a Google sign-in is named as Google', /Google/.test(r1[1]), r1[1]);
  check('a sign-in with NO location still appears, and says so plainly',
    /Unknown location/.test(r1[1]) && /198\.51\.100\.4/.test(r1[1]), r1[1]);
  check('Safari on macOS reads as itself', /Safari on macOS/.test(r1[1]), r1[1]);
  check('the summary counts the sign-ins and the distinct places',
    /2 sign-ins in the last 30 days/.test(await panelText()), (await panelText()).slice(0, 200));
  check('the standing note explains a missing location and names the 30-day window',
    /could not be placed/.test(await panelText()) && /kept for 30 days/.test(await panelText()));
  check('no Who column while looking at your own',
    (await page.$$eval('#cyg-a-panel-signins thead th', (h) => h.map((x) => x.textContent))).indexOf('Who') === -1);

  /* ── 3. Switching tabs away and back does not refetch ─────────────────── */
  console.log('\n3. Not a loop');
  await page.click('#cyg-a-tab-events');
  await page.click('#cyg-a-tab-signins');
  await page.click('#cyg-a-tab-integrity');
  await page.click('#cyg-a-tab-signins');
  await page.waitForTimeout(600);
  check('SWITCHING TABS FOUR TIMES FETCHED NOTHING MORE — loaded once means once',
    world.calls.length === 1, JSON.stringify(world.calls));
  check('and the rows are still there', (await rows()).length === 2);

  /* ── 4. The scope toggle ──────────────────────────────────────────────── */
  console.log('\n4. Everyone');
  await page.click('[data-si-scope="all"]');
  await page.waitForFunction(() => document.querySelectorAll('#cyg-a-panel-signins tbody tr').length === 3, null, { timeout: 10000 });
  check('asking for everyone fetches once more, with scope=all',
    world.calls.length === 2 && world.calls[1].scope === 'all', JSON.stringify(world.calls));
  check('three sign-ins across two people are listed', (await rows()).length === 3);
  check('the Who column appears only now',
    (await page.$$eval('#cyg-a-panel-signins thead th', (h) => h.map((x) => x.textContent))).indexOf('Who') !== -1);
  check('and names the other person', /other@example\.test/.test(await panelText()));
  check('the summary counts the people too', /across 2 people/.test(await panelText()), (await panelText()).slice(0, 200));

  check('pressing the scope it is already on fetches nothing',
    await (async () => { const n = world.calls.length; await page.click('[data-si-scope="all"]'); await page.waitForTimeout(400); return world.calls.length === n; })());

  /* ── 5. The period ────────────────────────────────────────────────────── */
  console.log('\n5. The period');
  await page.selectOption('#cyg-a-si-days', '7');
  await page.waitForFunction(() => /last 7 days/.test(document.getElementById('cyg-a-panel-signins').textContent), null, { timeout: 10000 });
  check('changing the period asks again with the new one',
    world.calls.length === 3 && world.calls[2].days === '7', JSON.stringify(world.calls));

  /* ── 6. A refused wider scope ─────────────────────────────────────────── */
  console.log('\n6. When the server says no');
  world.adminAllowed = false;
  await page.click('[data-si-scope="mine"]');
  await page.waitForTimeout(500);
  const before = world.calls.length;
  await page.click('[data-si-scope="all"]');
  await page.waitForFunction(() => /Only an administrator/.test(document.getElementById('cyg-a-panel-signins').textContent), null, { timeout: 10000 });
  check('a 403 is explained in words, not shown as an error',
    /Only an administrator can see everyone/.test(await panelText()));
  await page.waitForTimeout(1200);
  check('IT FALLS BACK TO YOUR OWN HISTORY ONCE, AND DOES NOT KEEP ASKING',
    world.calls.length === before + 2, JSON.stringify(world.calls.slice(before)));
  check('and shows your own rows rather than an empty table', (await rows()).length === 2);
  check('the toggle is back on Mine',
    (await page.getAttribute('[data-si-scope="mine"]', 'aria-pressed')) === 'true');

  /* ── 7. A real failure ────────────────────────────────────────────────── */
  console.log('\n7. When it genuinely breaks');
  world.adminAllowed = true;
  world.failNext = true;
  await page.click('#cyg-a-si-refresh');
  await page.waitForFunction(() => /cyg-a-note err/.test(document.getElementById('cyg-a-panel-signins').innerHTML), null, { timeout: 10000 });
  check('a 500 is surfaced to the reader rather than swallowed',
    (await page.$$('#cyg-a-panel-signins .cyg-a-note.err')).length === 1);
  check('and the rows already on screen are kept, not blanked', (await rows()).length === 2);
  await page.click('#cyg-a-si-refresh');
  await page.waitForTimeout(700);
  check('a refresh after the failure works', (await page.$$('#cyg-a-panel-signins .cyg-a-note.err')).length === 0);

  /* ── 8. Quiet ─────────────────────────────────────────────────────────── */
  console.log('\n8. Quiet');
  check('no page errors and no console errors', errors.length === 0, JSON.stringify(errors));

  await browser.close();
  server.close();
  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
