/* tests/browser/collation-match.smoke.js
 * ---------------------------------------------------------------------------
 * The Trial Balance, run twice over the same two databases, under two
 * different collation profiles.
 *
 * tests/collation-match.test.js pins the folding rule and reads the wiring
 * out of the source. What it cannot show is the thing that was actually
 * wrong: a real Trial Balance, against a real pair of result sets, turning
 * one account into two rows because the source spells it ACC001 and the
 * target spells it acc001. Under a case-insensitive profile the database
 * calls those the same account. The screen did not, and reported a missing
 * account AND an unexpected one, with the whole balance shown as a variance.
 *
 * The stub answers the two generated queries with exactly that: four
 * accounts, agreeing on every figure, differing only in how two of them are
 * spelled and one trailing space.
 *
 * Not part of `npm test`: it needs a browser.
 *   node tests/browser/collation-match.smoke.js
 * Screenshots (optional): SHOTS=/path/to/dir
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8421);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const SHOTS = process.env.SHOTS || '';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.json': 'application/json', '.woff2': 'font/woff2' };

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
  if (p === '/auth-gate.js') {
    res.writeHead(200, { 'Content-Type': TYPES['.js'] });
    return res.end('/* stubbed: the balance merge is under test, not the auth gate */');
  }
  if (ROUTES[p]) p = ROUTES[p];
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) {
    res.writeHead(404); return res.end('no');
  }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

const U = 'you@example.test';

/* The two sides agree on every figure. They disagree only about spelling:
   two accounts differ in case, one carries a trailing space, and one
   genuinely differs — that last one must still be reported however the
   profile is set, or the fix would have traded false differences for
   hidden ones. */
const SRC_ROWS = [
  { period: '2026-01', account: 'ACC001', balance: 1000 },
  { period: '2026-01', account: 'ACC002', balance: 2000 },
  { period: '2026-01', account: 'ACC003', balance: 3000 },
  { period: '2026-01', account: 'ACC004', balance: 4000 },
];
const TGT_ROWS = [
  { period: '2026-01', account: 'acc001', balance: 1000 },   // case only
  { period: '2026-01', account: 'ACC002  ', balance: 2000 }, // trailing space only
  { period: '2026-01', account: 'ACC003', balance: 3000 },   // identical
  { period: '2026-01', account: 'ACC004', balance: 3500 },   // a REAL difference
];

const SEED = (a) => {
  const tok = 'x.' + btoa(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: a.U })).replace(/=+$/, '') + '.y';
  [localStorage, sessionStorage].forEach((s) => { s.setItem('cygenix_token', tok); s.setItem('cygenix_expires', String(Date.now() + 36e5)); });
  localStorage.setItem('cygenix_user', JSON.stringify({ email: a.U, name: 'A Tester' }));
  localStorage.setItem('cygenix_active_user', a.U);
  localStorage.setItem('cygenix_entra_account', JSON.stringify({ email: a.U, userId: a.U }));
  localStorage.setItem('cygenix_cookie_consent', 'all');
  localStorage.setItem('cygenix_onboarded', '1');
  localStorage.setItem('cygenix_tier', 'pro');
  const acct = { homeAccountId: 'h.t', environment: 'cygenix.ciamlogin.com', tenantId: 't', username: a.U, localAccountId: 'l', authorityType: 'MSSTS', name: 'A Tester' };
  localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify(acct));
  localStorage.setItem('h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-', JSON.stringify({ credentialType: 'IdToken', secret: tok, expiresOn: String(Math.floor(Date.now() / 1000) + 3600) }));
  sessionStorage.setItem('cygenix_rbac_me', JSON.stringify({ at: Date.now(), me: { oid: 'x', email: a.U, roles: ['OW', 'PA'] } }));
  // Everything above is re-established on every load; everything below is
  // seeded once, because the test writes the collation onto this profile
  // between runs and a reload must not put the pristine copy back.
  if (localStorage.getItem('cygenix_profiles_v1')) return;
  localStorage.setItem('cygenix_projects', JSON.stringify([{ id: 'p1', name: 'Northbank Core' }]));
  localStorage.setItem('cygenix_active_project_id', 'p1');

  const saved = {};
  saved[a.U] = [
    { id: 'c_src', name: 'Ledger source', side: 'src', mode: 'direct' },
    { id: 'c_tgt', name: 'Finance target', side: 'tgt', mode: 'direct' },
  ];
  localStorage.setItem('cygenix_saved_connections', JSON.stringify(saved));
  localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify({
    c_src: { connString: 'mssql://sa:pw@src.example.internal:1433/SRC' },
    c_tgt: { connString: 'mssql://sa:pw@tgt.example.internal:1433/TGT' },
  }));
  localStorage.setItem('cygenix_profiles_v1', JSON.stringify({
    v: 1, createdAt: Date.now(), connMeta: {}, bindings: [], runRecords: [], events: [],
    profiles: [{ id: 'FIN-01', name: 'Ledger to Finance', envClass: 'DEV', status: 'active',
                 srcConnId: 'c_src', tgtConnId: 'c_tgt', updatedAt: 1 }],
    settings: { envClasses: [], activeProfileId: 'FIN-01', selectedAt: 1 },
  }));

  localStorage.setItem('cygenix_balancing_tb_reports', JSON.stringify([{
    id: 'tb_smoke', name: 'January trial balance',
    src: { mode: 'view', view: 'dbo.tb_src', table: '', periodCol: 'period', accountCol: 'account', balanceCol: 'balance', balanceMethod: 'balance', where: '' },
    tgt: { mode: 'view', view: 'dbo.tb_tgt', table: '', periodCol: 'period', accountCol: 'account', balanceCol: 'balance', balanceMethod: 'balance', where: '' },
    period: { mode: 'all', single: '', from: '', to: '' },
  }]));
};

const PAGE_HOSTS = (() => {
  const html = fs.readFileSync(path.join(PUB, 'balancing.html'), 'utf8');
  const hosts = new Set(['fonts.gstatic.com']);
  for (const m of html.matchAll(/https:\/\/([a-z0-9.-]+)\//gi)) hosts.add(m[1]);
  return [...hosts];
})();

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 1000 } });

  const offSite = [];
  await ctx.route('**', (route) => {
    const u = route.request().url();
    const json = (status, body) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
    if (/\/api\/health$/.test(u) || /functions\/health/.test(u)) return json(200, { status: 'ok' });
    if (/functions\/data-proxy/.test(u)) {
      if (/action=whoami/.test(u)) return json(200, { email: U, role: 'user', tier: 'pro', tier_status: 'active', exists: true });
      return route.abort();
    }
    if (/functions\/(audit|rbac-admin|projects|scheduler|db-connect)/.test(u)) return json(200, {});
    if (u.startsWith('http://localhost:' + PORT)) return route.continue();
    try { if (PAGE_HOSTS.indexOf(new URL(u).host) === -1) offSite.push(u); } catch (e) { offSite.push(u); }
    return route.abort();
  });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('console', (m) => {
    if (m.type() === 'error' && !/ERR_|Failed to load resource|CygenixSync/.test(m.text())) errors.push(m.text());
  });
  await page.addInitScript(SEED, { U });

  const shot = async (name) => {
    if (!SHOTS) return;
    await page.setViewportSize({ width: 1440, height: 1600 });
    await page.waitForTimeout(150);
    await page.screenshot({ path: path.join(SHOTS, name) });
    await page.setViewportSize({ width: 1440, height: 1000 });
  };

  /* Run the report with a given collation saved on the profile, and read
     the merged rows back out of the run store. balExec is replaced rather
     than the network stubbed, because what is under test is the merge, not
     the SQL that feeds it — and the two sides have to be told apart. */
  const runWith = async (collation) => {
    await page.evaluate((args) => {
      const st = JSON.parse(localStorage.getItem('cygenix_profiles_v1'));
      st.profiles[0].collation = args.collation ? {
        v: 1, strategy: 'target',
        source: { dbCollation: args.collation }, target: { dbCollation: args.collation },
        fingerprint: {}, columnOverrides: {}, acknowledged: [],
      } : undefined;
      if (!args.collation) delete st.profiles[0].collation;
      localStorage.setItem('cygenix_profiles_v1', JSON.stringify(st));
      localStorage.setItem('cygenix_balancing_tb_runs', '{}');
    }, { collation });
    await page.reload({ waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => typeof window.tbRun === 'function'
      && typeof window.balKeyFolder === 'function', null, { timeout: 20000 });
    await page.evaluate((rows) => {
      // Both sides answered from the same stub, told apart by the view name
      // the report's own SQL builder puts in the statement.
      window.balExec = async (side, sql) => (String(sql).indexOf('tb_src') !== -1 ? rows.src : rows.tgt);
    }, { src: SRC_ROWS, tgt: TGT_ROWS });
    await page.evaluate(() => window.tbRun(0));
    await page.waitForFunction(() => {
      try { return !!JSON.parse(localStorage.getItem('cygenix_balancing_tb_runs') || '{}').tb_smoke; }
      catch (e) { return false; }
    }, null, { timeout: 15000 });
    return page.evaluate(() => JSON.parse(localStorage.getItem('cygenix_balancing_tb_runs')).tb_smoke);
  };

  console.log('Trial Balance — one account, two spellings\n');

  // The first load, so the origin exists and runWith's localStorage writes
  // have somewhere to go.
  await page.goto('http://localhost:' + PORT + '/balancing', { waitUntil: 'domcontentloaded' });
  await page.waitForFunction(() => typeof window.tbRun === 'function', null, { timeout: 20000 });

  // ── 1. No collation detected: today's behaviour, unchanged ──────────────
  console.log('1. With no collation on the profile');
  const plain = await runWith('');
  check('the report runs and produces rows', Array.isArray(plain.rows) && plain.rows.length > 0,
    JSON.stringify(plain).slice(0, 200));
  check('NOTHING IS FOLDED — six rows for four accounts, exactly as before',
    plain.rows.length === 6, plain.rows.length + ' rows');
  check('and it says nothing about a matching rule, because it applied none',
    !plain.matchNote);
  check('the three spelling differences read as missing-and-unexpected pairs',
    plain.rows.filter((r) => r.source == null || r.target == null).length === 4,
    JSON.stringify(plain.rows));

  // ── 2. A case-insensitive profile: what the database would say ──────────
  console.log('\n2. With a case-insensitive collation');
  const ci = await runWith('Latin1_General_CI_AS');
  check('FOUR ACCOUNTS COME BACK AS FOUR ROWS',
    ci.rows.length === 4, JSON.stringify(ci.rows.map((r) => r.account)));
  check('the account spelled differently on each side is ONE row, and it balances',
    (() => {
      const r = ci.rows.filter((x) => String(x.account).trim().toUpperCase() === 'ACC001')[0];
      return !!r && r.source === 1000 && r.target === 1000 && r.variance === 0;
    })(), JSON.stringify(ci.rows));
  check('so is the one that differed only by a trailing space',
    (() => {
      const r = ci.rows.filter((x) => String(x.account).trim().toUpperCase() === 'ACC002')[0];
      return !!r && r.source === 2000 && r.target === 2000 && r.variance === 0;
    })());
  check('THE ACCOUNT THAT REALLY DIFFERS IS STILL REPORTED — no difference was hidden',
    (() => {
      const r = ci.rows.filter((x) => String(x.account).trim().toUpperCase() === 'ACC004')[0];
      return !!r && r.source === 4000 && r.target === 3500 && r.variance === 500;
    })(), JSON.stringify(ci.rows));
  check('the variance total is the one real difference, not the whole ledger',
    ci.totals.varianceTotal === 500 && ci.totals.mismatches === 1,
    JSON.stringify(ci.totals) + ' vs unfolded ' + JSON.stringify(plain.totals));
  check('the account codes shown are the ones the database holds, not folded ones',
    ci.rows.every((r) => !/^[a-z]/.test(String(r.account))),
    JSON.stringify(ci.rows.map((r) => r.account)));
  check('and the report states the rule it matched by',
    /ignoring case/.test(ci.matchNote || '') && /Latin1_General_CI_AS/.test(ci.matchNote || ''),
    ci.matchNote);

  // ── 3. A case-sensitive profile: the spellings matter again ─────────────
  console.log('\n3. With a case-sensitive collation');
  const cs = await runWith('Latin1_General_CS_AS');
  check('the case difference is a real difference again, as that database says',
    cs.rows.filter((r) => r.source == null || r.target == null).length === 2,
    JSON.stringify(cs.rows.map((r) => r.account)));
  check('but the trailing space is still not one, because = pads either way',
    (() => {
      const r = cs.rows.filter((x) => String(x.account).trim() === 'ACC002')[0];
      return !!r && r.source === 2000 && r.target === 2000;
    })(), JSON.stringify(cs.rows.map((r) => r.account)));
  check('and it does not claim to have ignored case',
    !/ignoring case/.test(cs.matchNote || ''), cs.matchNote);

  // ── 4. On screen ────────────────────────────────────────────────────────
  console.log('\n4. On screen');
  await runWith('Latin1_General_CI_AS');
  await page.evaluate(() => { if (typeof window.balSwitchTab === 'function') window.balSwitchTab('trial-balance'); });
  await page.waitForTimeout(400);
  const text = (await page.textContent('body')).replace(/\s+/g, ' ');
  check('the matching rule is printed under the figures, not only kept in the run',
    /ignoring case/.test(text), text.slice(0, 200));
  check('no uncaught errors on any of that', errors.length === 0, errors.slice(0, 3).join(' | '));
  check('nothing reached off-site', offSite.length === 0, offSite.slice(0, 3).join(' | '));
  await shot('collation-match-trial-balance.png');

  await browser.close();
  server.close();
  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
