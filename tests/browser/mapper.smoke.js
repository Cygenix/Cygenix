/* tests/browser/mapper.smoke.js
 * ---------------------------------------------------------------------------
 * The older Object Mapping screen, in a browser.
 *
 * public/mapper.html is 2,000 lines of inline script that nothing links to
 * any more and that tests/mapper.test.js can only read. It is still routed
 * and still does the whole job — connect, map, generate a cross-database
 * INSERT…SELECT, save it as a job — so "it still loads and still maps" is
 * worth executing rather than asserting from the source.
 *
 * It is also the page the collation wiring never reached. The profile
 * seeded below has a case-sensitive source and a case-insensitive target,
 * which is the brief's own example: every mapped text pair should carry a
 * badge, the summary should say so, and the generated SQL's JOIN should
 * raise a clash in the banner under it.
 *
 * Not part of `npm test`: it needs a browser.
 *   node tests/browser/mapper.smoke.js
 * Screenshots (optional): SHOTS=/path/to/dir
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8423);
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
    return res.end('/* stubbed: the mapper is under test, not the auth gate */');
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
const PW = 'Sup3rSecret';

/* The two schemas the stubbed dbCall answers with. Code and Name are text
   on both sides; LedgerID and entry_id are not, and must get no badge. */
const SRC_SCHEMA = { database: 'SRC', tables: [{ schema: 'dbo', name: 'Ledger', rowCount: 4200, columns: [
  { name: 'LedgerID', type: 'INT', nullable: false, isIdentity: true },
  { name: 'Code', type: 'VARCHAR(20)', nullable: false },
  { name: 'Name', type: 'VARCHAR(120)', nullable: true },
  { name: 'Narrative', type: 'VARCHAR(MAX)', nullable: true },
]}]};
const TGT_SCHEMA = { database: 'TGT', tables: [{ schema: 'fin', name: 'ledger_entry', rowCount: 0, columns: [
  { name: 'entry_id', type: 'INT', nullable: false, isIdentity: true },
  { name: 'Code', type: 'VARCHAR(20)', nullable: false },
  { name: 'Name', type: 'VARCHAR(60)', nullable: true },
  { name: 'Narrative', type: 'NVARCHAR(-1)', nullable: true },
]}]};

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
  localStorage.setItem('cygenix_projects', JSON.stringify([{ id: 'p1', name: 'Northbank Core' }]));
  localStorage.setItem('cygenix_active_project_id', 'p1');
  localStorage.removeItem('cygenix_api_key');
  sessionStorage.removeItem('cygenix_api_key');
  localStorage.setItem('cygenix_jobs', '[]');

  const active = {};
  active[a.U] = { srcConnString: 'mssql://sa:' + a.PW + '@src.example.internal:1433/SRC', srcConnMode: 'direct',
                  tgtConnString: 'mssql://sa:' + a.PW + '@tgt.example.internal:1433/TGT', tgtConnMode: 'direct' };
  localStorage.setItem('cygenix_project_connections', JSON.stringify(active));

  // The brief's own example: a case-sensitive source into a case-insensitive
  // target. Every mapped text pair differs, so every one should be badged.
  localStorage.setItem('cygenix_profiles_v1', JSON.stringify({
    v: 1, createdAt: Date.now(), connMeta: {}, bindings: [], runRecords: [], events: [],
    profiles: [{ id: 'FIN-01', name: 'Ledger to Finance', envClass: 'DEV', status: 'active',
                 srcConnId: 'c_src', tgtConnId: 'c_tgt', updatedAt: 1,
                 collation: { v: 1, strategy: 'target',
                   source: { database: 'SRC', dbCollation: 'Latin1_General_CS_AS', tempdbCollation: 'Latin1_General_CS_AS', codePage: 1252 },
                   target: { database: 'TGT', dbCollation: 'SQL_Latin1_General_CP1_CI_AS', tempdbCollation: 'SQL_Latin1_General_CP1_CI_AS', codePage: 1252 },
                   fingerprint: {}, columnOverrides: {}, acknowledged: [] } }],
    settings: { envClasses: [], activeProfileId: 'FIN-01', selectedAt: 1 },
  }));
};

const PAGE_HOSTS = (() => {
  const html = fs.readFileSync(path.join(PUB, 'mapper.html'), 'utf8');
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
  const errors = [], consoleText = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('console', (m) => {
    consoleText.push(m.text());
    if (m.type() === 'error' && !/ERR_|Failed to load resource|CygenixSync/.test(m.text())) errors.push(m.text());
  });
  page.on('dialog', (d) => d.dismiss());
  await page.addInitScript(SEED, { U, PW });

  const shot = async (name) => {
    if (!SHOTS) return;
    await page.setViewportSize({ width: 1440, height: 1800 });
    await page.waitForTimeout(200);
    await page.screenshot({ path: path.join(SHOTS, name) });
    await page.setViewportSize({ width: 1440, height: 1000 });
  };

  console.log('Mapper — the older Object Mapping screen, in a browser\n');

  // ── 1. It loads ─────────────────────────────────────────────────────────
  console.log('1. The page');
  await page.goto('http://localhost:' + PORT + '/mapper', { waitUntil: 'domcontentloaded' });
  await page.waitForFunction(() => typeof window.selectTable === 'function'
    && typeof window.connectSrc === 'function', null, { timeout: 20000 });
  check('the page loads and its script is live', true);
  check('NO UNCAUGHT ERROR ON LOAD — 2,000 lines of inline script still parse',
    errors.length === 0, errors.slice(0, 3).join(' | '));
  check('the collation module loaded here too, with its rules',
    await page.evaluate(() => !!(window.CygenixCollationRules && window.cygCollation
      && typeof window.cygCollation.badgeFor === 'function')));
  check('and it found the profile this page is bound to',
    await page.evaluate(() => { const s = window.cygCollation.settings(); return !!s && s.target.dbCollation; })
      === 'SQL_Latin1_General_CP1_CI_AS');

  // ── 2. Connect and map ──────────────────────────────────────────────────
  console.log('\n2. Connect, pick two tables, map them');
  await page.evaluate((s) => {
    // dbCall is a function DECLARATION, so it is on window and can be
    // replaced; srcAllTables and the rest are `let`, so they are populated
    // by calling the page's own connect functions rather than assigned.
    window.dbCall = async (conn) => (String(conn).indexOf('/SRC') !== -1 ? s.src : s.tgt);
  }, { src: SRC_SCHEMA, tgt: TGT_SCHEMA });
  await page.evaluate(() => window.connectSrc());
  await page.evaluate(() => window.connectTgt());
  await page.waitForFunction(() => /4 tables|1 tables/.test(document.getElementById('src-label').textContent), null, { timeout: 10000 })
    .catch(() => {});
  check('both sides connect and report their database',
    /SRC/.test(await page.textContent('#src-label')) && /TGT/.test(await page.textContent('#tgt-label')),
    (await page.textContent('#src-label')) + ' | ' + (await page.textContent('#tgt-label')));
  check('and no credential is on screen with it',
    !(await page.textContent('body')).includes(PW));

  await page.evaluate(() => window.selectTable('src', 'dbo.Ledger'));
  await page.evaluate(() => window.selectTable('tgt', 'fin.ledger_entry'));
  await page.waitForSelector('#mapping-tbody tr', { timeout: 10000 });
  check('the mapping table builds without an API key, from the local matcher',
    (await page.$$('#mapping-tbody tr')).length >= 3);

  // ── 3. The collation surface that was missing ───────────────────────────
  console.log('\n3. What the page never used to say');
  const badges = await page.$$eval('#mapping-tbody .cyg-coll-badge',
    (els) => els.map((e) => ({ cls: e.className, title: e.getAttribute('title') })));
  check('EVERY MAPPED TEXT PAIR CARRIES A COLLATION BADGE',
    badges.length >= 3, JSON.stringify(badges));
  check('and they are red, because a case-sensitive source into a case-insensitive target is not a soft difference',
    badges.every((b) => / fail\b/.test(b.cls)), JSON.stringify(badges.map((b) => b.cls)));
  check('each one says which collations, and what the fix is',
    badges.every((b) => /Latin1_General_CS_AS/.test(b.title) && /COLLATE/.test(b.title)),
    badges[0] && badges[0].title);
  check('the badge is styled rather than unstyled text',
    await page.$eval('#mapping-tbody .cyg-coll-badge',
      (e) => getComputedStyle(e).display === 'inline-flex'));
  check('the summary above the table says it once, with a way in to the settings',
    await page.isVisible('#map-collation-summary')
    && /mapped column/.test(await page.textContent('#map-collation-summary'))
    && await page.$eval('#map-collation-summary a', (a) => /connections\/databases/.test(a.getAttribute('href'))),
    await page.textContent('#map-collation-summary'));
  await shot('mapper-1-mapping.png');

  // ── 4. The generated SQL, and the clash in it ───────────────────────────
  console.log('\n4. The generated SQL');
  await page.evaluate(() => window.generateSingleSQL(true));
  await page.waitForSelector('#sql-panel', { state: 'visible', timeout: 10000 });
  const sql = await page.textContent('#sql-output');
  check('SQL is generated', /INSERT INTO/.test(sql) && /SELECT/.test(sql), sql.slice(0, 120));
  check('AND IT CARRIES NO COLLATE — this page reports, it does not rewrite',
    !/COLLATE/i.test(sql),
    'an INSERT converts to the target column on its own; a COLLATE here would change nothing');
  check('the truncation guard still fires on the narrower target column',
    /LEFT\(\[Name\], 60\)/.test(sql), sql.slice(0, 400));
  check('AND NVARCHAR(-1) IS NOT TREATED AS A LENGTH OF MINUS ONE',
    !/LEFT\([^)]*,\s*-1\)/.test(sql),
    'the duplicate parseTypeLen read sys.columns\' -1 as a real length, which emits SQL Server rejects');

  // ── 5. A saved job carries the stamp ────────────────────────────────────
  console.log('\n5. Save as job');
  await page.evaluate(() => window.saveAsJob());
  await page.waitForTimeout(300);
  const job = await page.evaluate(() => JSON.parse(localStorage.getItem('cygenix_jobs') || '[]')[0] || null);
  check('the job saves', !!job && /Ledger/.test(job.name || ''), JSON.stringify(job && job.name));
  check('AND IT CARRIES THE COLLATION STAMP, so it can be marked for regeneration later',
    !!job && typeof job.collationStamp === 'string' && job.collationStamp.length > 0,
    job && JSON.stringify(job.collationStamp));
  check('and the stamp is the one the shared module would produce now',
    job && job.collationStamp === await page.evaluate(() => window.cygCollation.stamp()));
  check('no credential reached the saved job',
    !JSON.stringify(job).includes(PW));

  // ── 6. Hygiene ──────────────────────────────────────────────────────────
  console.log('\n6. Hygiene');
  check('nothing was asked of Anthropic — no key, no call',
    offSite.filter((u) => /anthropic/.test(u)).length === 0);
  check('NO PASSWORD ANYWHERE ON THE PAGE', !(await page.textContent('body')).includes(PW));
  check('and none in the console', consoleText.every((t) => !t.includes(PW)));
  check('no uncaught errors through any of that', errors.length === 0, errors.slice(0, 3).join(' | '));
  check('nothing reached off-site', offSite.length === 0, offSite.slice(0, 3).join(' | '));
  await shot('mapper-2-sql.png');

  await browser.close();
  server.close();
  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
