/* tests/browser/collation.smoke.js
 * ---------------------------------------------------------------------------
 * The Collation card, in a browser, against a stubbed db-connect.
 *
 * tests/collation.test.js pins the rules — the parser, the severity grades,
 * the parameterised SQL, the saved shape. What it cannot show is that the
 * card appears in the right place on the right tab, that Detect fills both
 * sides, that Scan grades the columns the brief's worked example describes,
 * that Save survives a reload and a profile switch, and that repointing a
 * connection raises the amber banner. Those only happen in a browser.
 *
 * The stub answers the two detection queries with the brief's own example
 * collations: source Latin1_General_CI_AS, target
 * SQL_Latin1_General_CP1_CI_AS, plus one source column in a case-sensitive
 * collation whose target column is case-insensitive and in a unique index.
 * That is verification steps 2 and 3 of Stage A, executed rather than
 * described.
 *
 * Not part of `npm test`: it needs a browser.
 *   node tests/browser/collation.smoke.js
 * Screenshots (optional): SHOTS=/path/to/dir
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8415);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const SHOTS = process.env.SHOTS || '';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
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
    return res.end('/* stubbed: the collation card is under test, not the auth gate */');
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
// If any of these reaches the screen, the card or the saved profile, the
// test fails. That is the point of them.
const SECRETS = ['Sup3rSecret', 'Hunter2!', 'PgPass9'];

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
  if (localStorage.getItem('cygenix_profiles_v1')) return;   // seed once; a reload keeps what the card saved
  localStorage.setItem('cygenix_projects', JSON.stringify([{ id: 'p1', name: 'Northbank Core' }]));
  localStorage.setItem('cygenix_active_project_id', 'p1');
  localStorage.setItem('cygenix_jobs', JSON.stringify([
    { id: 'job_ledger', name: 'Ledger to ledger_entry', projectId: 'p1', profileId: 'FIN-DEV-01',
      source: 'dbo.Ledger', sourceTable: 'dbo.Ledger', target: 'fin.ledger_entry',
      columnMapping: [
        { srcCol: 'LedgerID', tgtCol: 'entry_id' },      // not text: no collation, skipped
        { srcCol: 'AcctNo', tgtCol: 'account_no' },      // CS source into CI target, not unique → Medium
        { srcCol: 'Narrative', tgtCol: 'memo' },         // CI both, different lineage → Medium
        { srcCol: 'Code', tgtCol: 'code' },              // CS source into CI target IN A UNIQUE KEY → High
      ] },
  ]));
  const saved = {};
  saved[a.U] = [
    { id: 'c_src', name: 'Ledger source', side: 'src', mode: 'direct' },
    { id: 'c_tgt', name: 'Finance target', side: 'tgt', mode: 'direct' },
    { id: 'c_pg', name: 'Postgres target', side: 'tgt', mode: 'direct' },
  ];
  localStorage.setItem('cygenix_saved_connections', JSON.stringify(saved));
  localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify({
    c_src: { connString: 'mssql://sa:' + a.S[0] + '@src.example.internal:1433/SRC' },
    c_tgt: { connString: 'Server=tcp:tgt.example.internal,1433;Database=TGT;User Id=loader;Password=' + a.S[1] + ';' },
    c_pg: { connString: 'postgres://pg:' + a.S[2] + '@pg.example.internal:5432/tgtdb' },
  }));
  localStorage.setItem('cygenix_profiles_v1', JSON.stringify({
    v: 1, createdAt: Date.now(), connMeta: {}, bindings: [], runRecords: [], events: [],
    profiles: [
      { id: 'FIN-DEV-01', name: 'Conv_DM to Azure', envClass: 'DEV', status: 'active', srcConnId: 'c_src', tgtConnId: 'c_tgt', updatedAt: 1 },
      { id: 'PG-DEV-02', name: 'Ledger to Postgres', envClass: 'DEV', status: 'active', srcConnId: 'c_src', tgtConnId: 'c_pg', updatedAt: 1 },
    ],
    settings: { envClasses: [], activeProfileId: 'FIN-DEV-01', selectedAt: 1 },
  }));
};

const PAGE_HOSTS = (() => {
  const html = fs.readFileSync(path.join(PUB, 'dashboard.html'), 'utf8');
  const hosts = new Set(['fonts.gstatic.com']);
  for (const m of html.matchAll(/https:\/\/([a-z0-9.-]+)\//gi)) hosts.add(m[1]);
  return [...hosts];
})();

/* ── The stub database ───────────────────────────────────────────────────
   The brief's worked example: a Windows collation on the source and the
   legacy SQL_ one on the target, plus one case-sensitive source column
   whose target is case-insensitive and in a unique index.               */
const SRC_DB = { server_collation: 'Latin1_General_CI_AS', db_collation: 'Latin1_General_CI_AS',
  tempdb_collation: 'Latin1_General_CI_AS', code_page: 1252, database_name: 'SRC', product_version: '15.0.4261.1' };
const TGT_DB = { server_collation: 'SQL_Latin1_General_CP1_CI_AS', db_collation: 'SQL_Latin1_General_CP1_CI_AS',
  tempdb_collation: 'SQL_Latin1_General_CP1_CI_AS', code_page: 1252, database_name: 'TGT', product_version: '15.0.4261.1' };

const SRC_COLS = [
  { schema_name: 'dbo', table_name: 'Ledger', column_name: 'AcctNo', data_type: 'varchar', max_length: 12, collation_name: 'Latin1_General_CS_AS', code_page: 1252, in_unique_key: 0 },
  { schema_name: 'dbo', table_name: 'Ledger', column_name: 'Narrative', data_type: 'varchar', max_length: 255, collation_name: 'Latin1_General_CI_AS', code_page: 1252, in_unique_key: 0 },
  { schema_name: 'dbo', table_name: 'Ledger', column_name: 'Code', data_type: 'varchar', max_length: 20, collation_name: 'Latin1_General_CS_AS', code_page: 1252, in_unique_key: 0 },
];
const TGT_COLS = [
  { schema_name: 'fin', table_name: 'ledger_entry', column_name: 'account_no', data_type: 'varchar', max_length: 12, collation_name: 'SQL_Latin1_General_CP1_CI_AS', code_page: 1252, in_unique_key: 0 },
  { schema_name: 'fin', table_name: 'ledger_entry', column_name: 'memo', data_type: 'varchar', max_length: 120, collation_name: 'SQL_Latin1_General_CP1_CI_AS', code_page: 1252, in_unique_key: 0 },
  { schema_name: 'fin', table_name: 'ledger_entry', column_name: 'code', data_type: 'varchar', max_length: 20, collation_name: 'SQL_Latin1_General_CP1_CI_AS', code_page: 1252, in_unique_key: 1 },
];

const world = { calls: [], srcDb: 'SRC', tgtDb: 'TGT' };

function whichSide(cs) {
  if (String(cs).indexOf(SECRETS[0]) !== -1) return 'src';
  if (String(cs).indexOf(SECRETS[1]) !== -1) return 'tgt';
  if (String(cs).indexOf(SECRETS[2]) !== -1) return 'pg';
  return '';
}
function dbAnswer(body) {
  const side = whichSide(body.connectionString || '');
  if (!side) return { status: 400, body: { error: 'connectionString is required' } };
  if (side === 'pg') return { status: 500, body: { error: 'Could not connect: this stub has no PostgreSQL' } };
  const sql = String(body.sql || '');
  if (/SERVERPROPERTY\('Collation'\)/.test(sql)) {
    const row = side === 'src'
      ? Object.assign({}, SRC_DB, { database_name: world.srcDb })
      : Object.assign({}, TGT_DB, { database_name: world.tgtDb });
    return { status: 200, body: { success: true, recordset: [row] } };
  }
  if (/fn_helpcollations/.test(sql)) {
    return { status: 200, body: { success: true, recordset: [{ name: 'Latin1_General_CI_AS' }, { name: 'SQL_Latin1_General_CP1_CI_AS' }] } };
  }
  if (/sys\.columns/.test(sql)) {
    // The names arrive as bound parameters, never in the statement text —
    // so the stub reads them from `params`, exactly as the driver would.
    const wanted = {};
    (body.params || []).forEach((p, i, all) => {
      if (/^t\d+$/.test(p.name)) {
        const s = all.filter((x) => x.name === 's' + p.name.slice(1))[0];
        wanted[((s ? s.value : '') + '.' + p.value).toLowerCase()] = true;
      }
    });
    const pool = side === 'src' ? SRC_COLS : TGT_COLS;
    return { status: 200, body: { success: true,
      recordset: pool.filter((c) => wanted[(c.schema_name + '.' + c.table_name).toLowerCase()]) } };
  }
  return { status: 200, body: { success: true, recordset: [] } };
}

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 1000 }, acceptDownloads: true });

  const offSite = [];
  await ctx.route('**', (route) => {
    const u = route.request().url();
    const json = (status, body) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
    if (/\/api\/health$/.test(u) || /functions\/health/.test(u)) return json(200, { status: 'ok' });
    if (/functions\/data-proxy/.test(u)) {
      if (/action=whoami/.test(u)) return json(200, { email: U, role: 'user', tier: 'pro', tier_status: 'active', exists: true });
      // Everything else is refused rather than answered empty: "cloud wins on
      // page load" would take an empty document as the truth and wipe the seed.
      return route.abort();
    }
    if (/functions\/db-connect/.test(u)) {
      let body = {};
      try { body = JSON.parse(route.request().postData() || '{}'); } catch (e) { body = {}; }
      world.calls.push(body);
      const a = dbAnswer(body);
      return json(a.status, a.body);
    }
    if (/functions\/(audit|rbac-admin|projects|scheduler)/.test(u)) return json(200, {});
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
  await page.addInitScript(SEED, { U, S: SECRETS });

  const openTab = async () => {
    await page.waitForFunction(() => !!window.cygCollation && typeof window.showView === 'function', null, { timeout: 20000 });
    await page.evaluate(() => {
      window.getCygenixIdToken = () => 'smoke-token';
      window.getCygenixIdTokenAsync = async () => localStorage.getItem('cygenix_token');
      // showView ONLY — exactly what clicking Connections in the rail does.
      // Calling switchConnTab here as well is what hid a real bug: Database
      // connections is the default tab, so a person arriving at this page
      // never fires it, and the card mounted only from there was invisible
      // to everybody who did not first click away to another tab and back.
      window.showView('connections');
    });
    await page.waitForSelector('#cyg-collation-card', { timeout: 10000 });
  };
  const chip = () => page.textContent('#col-chip').then((s) => s.trim());
  const cardText = () => page.textContent('#cyg-collation-card').then((s) => s.replace(/\s+/g, ' '));
  const settle = (fn, ms) => page.waitForFunction(fn, null, { timeout: ms || 15000 });
  const shot = async (name) => {
    if (!SHOTS) return;
    await page.setViewportSize({ width: 1440, height: 2200 });
    await page.waitForTimeout(150);
    await page.screenshot({ path: path.join(SHOTS, name) });
    await page.setViewportSize({ width: 1440, height: 1000 });
  };

  console.log('Collation card — Stage A, in a browser\n');

  // ── 1. It renders, in the right place ───────────────────────────────────
  console.log('1. The card');
  await page.goto('http://localhost:' + PORT + '/dashboard', { waitUntil: 'domcontentloaded' });
  await openTab();

  check('the card is on the Database connections tab', await page.isVisible('#cyg-collation-card'));
  check('BELOW the source and target panels, not beside them',
    await page.evaluate(() => {
      const tgt = document.getElementById('tgt-conn-panel').getBoundingClientRect();
      const col = document.getElementById('cyg-collation-card').getBoundingClientRect();
      return col.top >= tgt.bottom - 2;
    }));
  check('and inside the databases tab, so it hides with it',
    await page.evaluate(() => !!document.getElementById('conn-tab-databases').querySelector('#cyg-collation-card')));
  check('no new tab was added to the tab bar', (await page.$$('#view-connections .cx-tab[data-tab]')).length === 5);
  check('the header names the active profile', (await page.textContent('#cyg-collation-card .col-profile')).indexOf('Conv_DM to Azure') !== -1);
  check('with nothing saved the chip reads Not checked', await chip() === 'Not checked');
  check('the four buttons the brief names are in the header',
    await page.isVisible('#col-detect') && await page.isVisible('#col-scan')
    && await page.isVisible('#col-export') && await page.isVisible('#col-save'));
  check('Export is disabled until there is something to export', await page.$eval('#col-export', (b) => b.disabled));
  await shot('collation-1-fresh.png');

  // ── 2. Detect ───────────────────────────────────────────────────────────
  console.log('\n2. Detect collations');
  world.calls = [];
  await page.click('#col-detect');
  await settle(() => /Latin1_General_CI_AS/.test(document.getElementById('cyg-collation-card').textContent));
  const t2 = await cardText();
  check('both sides populate', /Latin1_General_CI_AS/.test(t2) && /SQL_Latin1_General_CP1_CI_AS/.test(t2));
  check('with the server, database and tempdb collations, the code page and the version',
    /tempdb collation/i.test(t2) && /Code page\s*1252/.test(t2) && /15\.0\.4261\.1/.test(t2), t2.slice(0, 900));
  check('the case and accent flags are words, not letters left for the reader to decode',
    /Case-insensitive \(CI\)/.test(t2) && /Accent-sensitive \(AS\)/.test(t2));
  check('the resolved collation follows the default strategy, the target database',
    (await page.textContent('#cyg-collation-card .col-resolved')).trim() === 'SQL_Latin1_General_CP1_CI_AS');
  check('the detection query went to both sides and took no parameters',
    world.calls.filter((c) => /SERVERPROPERTY/.test(c.sql || '')).length === 2
    && world.calls.filter((c) => /SERVERPROPERTY/.test(c.sql || '')).every((c) => !c.params));

  // ── 3. Scan ─────────────────────────────────────────────────────────────
  console.log('\n3. Scan');
  world.calls = [];
  await page.waitForTimeout(3100);                     // past the three-second minimum interval
  await page.click('#col-scan');
  await settle(() => document.querySelectorAll('#cyg-collation-card table.col-tbl tbody tr').length > 0);
  const rows = await page.$$('#cyg-collation-card table.col-tbl tbody tr');
  check('the findings table fills', rows.length === 3, rows.length + ' rows');
  const t3 = await cardText();
  check('THE HIGH FINDING IS THE CASE-SENSITIVE SOURCE INTO A UNIQUE CASE-INSENSITIVE TARGET',
    (await page.$$('#cyg-collation-card .col-sev-high')).length === 1 && /fin\.ledger_entry\.code/.test(t3)
    && /collide/.test(t3));
  check('and the two Medium findings are the differing collations',
    (await page.$$('#cyg-collation-card .col-sev-medium')).length === 2
    && /Cannot resolve the collation conflict/.test(t3));
  check('each finding names both collations and a recommendation',
    /Latin1_General_CS_AS/.test(t3) && /Apply COLLATE SQL_Latin1_General_CP1_CI_AS/.test(t3));
  check('and the "Where used" column names the modules the inventory found',
    /Object Mapping/.test(t3) && /Task Agent runs/.test(t3));
  check('the chip reads High risk', await chip() === 'High risk (1)');
  check('the non-text column pair was skipped rather than guessed at', !/entry_id/.test(t3));
  check('THE SCAN SENT TABLE NAMES AS BOUND PARAMETERS, NEVER IN THE SQL',
    world.calls.filter((c) => /sys\.columns/.test(c.sql || '')).length === 2
    && world.calls.filter((c) => /sys\.columns/.test(c.sql || ''))
        .every((c) => Array.isArray(c.params) && c.params.length > 0
          && c.sql.indexOf('Ledger') === -1 && c.sql.indexOf('ledger_entry') === -1));
  check('and it asked only for the mapped tables',
    world.calls.filter((c) => /sys\.columns/.test(c.sql || '')).every((c) => c.params.length === 2));
  await shot('collation-2-findings.png');

  // ── 4. Filters and acknowledgement ──────────────────────────────────────
  console.log('\n4. Filters and acknowledgement');
  await page.click('#cyg-collation-card .col-filters button[data-filter="high"]');
  await page.waitForTimeout(150);
  check('filtering by severity narrows the table', (await page.$$('#cyg-collation-card table.col-tbl tbody tr')).length === 1);
  await page.click('#cyg-collation-card .col-filters button[data-filter="all"]');
  await page.waitForTimeout(150);
  await page.check('#cyg-collation-card input[data-ack]');
  await settle(() => document.querySelectorAll('#cyg-collation-card tr.ackd').length > 0);
  check('an acknowledged finding is greyed but still listed',
    (await page.$$('#cyg-collation-card tr.ackd')).length === 1
    && (await page.$$('#cyg-collation-card table.col-tbl tbody tr')).length === 3);
  check('AND IT STOPS COUNTING TOWARDS THE CHIP', await chip() === 'Warnings (2)', await chip());

  // ── 5. Settings ─────────────────────────────────────────────────────────
  console.log('\n5. Settings');
  check('every setting in the brief is on the card',
    await page.isVisible('input[name="col-strategy"]') && await page.isVisible('input[name="col-temp"]')
    && await page.isVisible('input[name="col-gen"]') && await page.isVisible('input[name="col-user"]')
    && await page.isVisible('input[name="col-case"]') && await page.isVisible('input[name="col-cp"]'));
  check('and the defaults are the ones it specifies',
    await page.$eval('input[name="col-strategy"][value="target"]', (e) => e.checked)
    && await page.$eval('input[name="col-temp"][value="resolved"]', (e) => e.checked)
    && await page.$eval('input[name="col-gen"][value="apply"]', (e) => e.checked)
    && await page.$eval('input[name="col-user"][value="offer_fix"]', (e) => e.checked)
    && await page.$eval('input[name="col-case"][value="warn"]', (e) => e.checked)
    && await page.$eval('input[name="col-cp"][value="warn"]', (e) => e.checked));
  check('the explicit-collation box is disabled until Explicit is chosen',
    await page.$eval('#col-explicit', (e) => e.disabled));
  await page.check('input[name="col-strategy"][value="source"]');
  await settle(() => document.querySelector('#cyg-collation-card .col-resolved').textContent.trim() === 'Latin1_General_CI_AS');
  check('choosing Preserve source changes the resolved collation on the spot',
    (await page.textContent('#cyg-collation-card .col-resolved')).trim() === 'Latin1_General_CI_AS');
  await page.check('input[name="col-strategy"][value="target"]');
  await page.waitForTimeout(150);

  await page.fill('#col-ovr-key', 'fin.ledger_entry.memo');
  await page.fill('#col-ovr-coll', 'Latin1_General_BIN2');
  await page.fill('#col-ovr-note', 'sorted as bytes downstream');
  await page.click('#col-ovr-add');
  await settle(() => /sorted as bytes downstream/.test(document.getElementById('cyg-collation-card').textContent));
  check('a column override can be added, with its note', /fin\.ledger_entry\.memo/.test(await cardText()));
  await page.fill('#col-ovr-key', 'nonsense');
  await page.fill('#col-ovr-coll', 'X');
  await page.click('#col-ovr-add');
  await settle(() => /schema\.table\.column/.test(document.getElementById('cyg-collation-card').textContent));
  check('and a malformed one is refused with a sentence rather than accepted',
    /Write the column as schema\.table\.column/.test(await cardText()));
  await page.check('input[name="col-case"][value="block"]');
  await page.waitForTimeout(100);

  // ── 6. Save, reload, switch profile and back ────────────────────────────
  console.log('\n6. Saved on the profile');
  await page.click('#col-save');
  await settle(() => /Saved to profile/.test(document.getElementById('cyg-collation-card').textContent));
  const saved = await page.evaluate(() => {
    const st = JSON.parse(localStorage.getItem('cygenix_profiles_v1'));
    const p = st.profiles.filter((x) => x.id === 'FIN-DEV-01')[0];
    return { collation: p.collation, keys: Object.keys(p), updatedAt: p.updatedAt,
             other: st.profiles.filter((x) => x.id === 'PG-DEV-02')[0] };
  });
  check('the profile now carries a collation object', !!saved.collation && saved.collation.version === 1);
  check('with the settings, the overrides, the acknowledgement and the last scan',
    saved.collation.caseRule === 'block'
    && saved.collation.columnOverrides['fin.ledger_entry.memo'].collation === 'Latin1_General_BIN2'
    && saved.collation.acknowledged.length === 1
    && saved.collation.lastScan && saved.collation.lastScan.findings.length === 3);
  check('EVERY OTHER FIELD ON THE PROFILE IS UNCHANGED, and the other profile is untouched',
    ['id', 'name', 'envClass', 'status', 'srcConnId', 'tgtConnId'].every((k) => saved.keys.indexOf(k) >= 0)
    && saved.other.id === 'PG-DEV-02' && saved.other.collation === undefined);
  check('and updatedAt was bumped so the sync merge keeps this version', saved.updatedAt > 1);
  check('NO CREDENTIAL REACHED THE SAVED PROFILE',
    SECRETS.every((s) => JSON.stringify(saved.collation).indexOf(s) === -1)
    && saved.collation.fingerprint.source === 'src.example.internal|SRC'
    && saved.collation.fingerprint.target === 'tgt.example.internal|TGT');

  await page.reload({ waitUntil: 'domcontentloaded' });
  await openTab();
  check('after a reload every setting is restored',
    await page.$eval('input[name="col-case"][value="block"]', (e) => e.checked)
    && /fin\.ledger_entry\.memo/.test(await cardText()));
  check('the last scan is restored with it, acknowledgement and all',
    (await page.$$('#cyg-collation-card table.col-tbl tbody tr')).length === 3
    && (await page.$$('#cyg-collation-card tr.ackd')).length === 1 && await chip() === 'Warnings (2)');

  await page.evaluate(() => {
    const st = JSON.parse(localStorage.getItem('cygenix_profiles_v1'));
    st.settings.activeProfileId = 'PG-DEV-02';
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify(st));
  });
  await openTab();
  check('switching to a PostgreSQL profile says SQL Server only and shows nothing else',
    /Collation matching supports SQL Server connections only/.test(await cardText())
    && (await page.$$('#cyg-collation-card table.col-tbl')).length === 0);
  check('and nothing broke', errors.length === 0, errors.slice(0, 2).join(' | '));
  await shot('collation-3-postgres.png');

  await page.evaluate(() => {
    const st = JSON.parse(localStorage.getItem('cygenix_profiles_v1'));
    st.settings.activeProfileId = 'FIN-DEV-01';
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify(st));
  });
  await openTab();
  check('switching back restores the SQL Server profile in full',
    await chip() === 'Warnings (2)' && /SQL_Latin1_General_CP1_CI_AS/.test(await cardText()));

  // ── 7. The connection moved ─────────────────────────────────────────────
  console.log('\n7. A repointed connection');
  await page.evaluate((s) => {
    const sec = JSON.parse(localStorage.getItem('cygenix_saved_conn_secrets'));
    sec.c_tgt.connString = 'Server=tcp:tgt.example.internal,1433;Database=TGT_COPY;User Id=loader;Password=' + s + ';';
    localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify(sec));
  }, SECRETS[1]);
  await openTab();
  check('THE AMBER BANNER APPEARS when the target points somewhere else',
    await page.isVisible('#col-drift') && /Connection changed since collations were detected/.test(await cardText()));
  check('and the chip drops back to Not checked rather than vouching for a stale read',
    await chip() === 'Not checked');
  check('the card opens itself to show it', await page.$eval('#cyg-collation-card', (e) => !e.classList.contains('closed')));
  await shot('collation-4-drift.png');

  await page.evaluate((s) => {
    const sec = JSON.parse(localStorage.getItem('cygenix_saved_conn_secrets'));
    sec.c_tgt.connString = 'Server=tcp:tgt.example.internal,1433;Database=TGT;User Id=loader;Password=' + s + ';';
    localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify(sec));
  }, SECRETS[1]);
  await openTab();
  check('and it clears when the connection goes back', (await page.$$('#col-drift')).length === 0);

  // ── 8. Collapse, and the guards ─────────────────────────────────────────
  console.log('\n8. Collapse and the guards');
  // Acknowledge the remaining findings so nothing forces the card open.
  await page.evaluate(() => {
    document.querySelectorAll('#cyg-collation-card input[data-ack]').forEach((c) => { if (!c.checked) c.click(); });
  });
  await page.waitForTimeout(250);
  await page.click('#col-toggle');
  await page.waitForTimeout(150);
  check('the card collapses', await page.$eval('#cyg-collation-card', (e) => e.classList.contains('closed')));
  check('and the collapsed head still shows the title, the chip and the resolved collation',
    await page.isVisible('#col-chip') && await page.isVisible('#cyg-collation-card .col-resolved')
    && await page.isVisible('#cyg-collation-card .col-title'));
  await page.reload({ waitUntil: 'domcontentloaded' });
  await openTab();
  check('IT STAYS COLLAPSED ACROSS A RELOAD when nothing needs attention',
    await page.$eval('#cyg-collation-card', (e) => e.classList.contains('closed')));

  const guard = await page.evaluate(async () => {
    const C = window.cygCollation;
    const first = C.detect();
    const second = await C.detect();
    await first;
    return second;
  });
  check('a second Detect while one is in flight is refused', guard === false);
  const again = await page.evaluate(() => window.cygCollation.detect());
  check('and a Detect inside the three-second interval is refused too', again === false);

  // openCard() is the deep link Object Mapping and the job-run block use.
  await page.evaluate(() => window.cygCollation.open());
  await page.waitForTimeout(250);
  check('the deep link opens the card expanded',
    await page.$eval('#cyg-collation-card', (e) => !e.classList.contains('closed')));

  // ── 9. Export ───────────────────────────────────────────────────────────
  console.log('\n9. Export');
  const dl = page.waitForEvent('download', { timeout: 8000 }).catch(() => null);
  await page.click('#col-export');
  const d = await dl;
  check('Export produces a file named for the feature and the day',
    !!d && /^cygenix_collation_\d{4}-\d{2}-\d{2}\.(xlsx|csv)$/.test(d.suggestedFilename()),
    d && d.suggestedFilename());
  if (d && /\.csv$/.test(d.suggestedFilename())) {
    const body = fs.readFileSync(await d.path(), 'utf8');
    check('the export carries the findings and no credential',
      /Severity,Object/.test(body) && /fin\.ledger_entry\.code/.test(body)
      && SECRETS.every((s) => body.indexOf(s) === -1));
  } else {
    check('the export is a workbook, so the credential check is on the card instead', !!d);
  }

  // ── 10. Hygiene ─────────────────────────────────────────────────────────
  console.log('\n10. Hygiene');
  const finalText = await cardText();
  check('NO CREDENTIAL IS ANYWHERE ON THE CARD', SECRETS.every((s) => finalText.indexOf(s) === -1));
  check('and none was written to the console', consoleText.every((t) => SECRETS.every((s) => t.indexOf(s) === -1)));
  check('the card shows the host, never the port, the user or the connection string',
    /src\.example\.internal/.test(finalText) && !/User Id|1433|sa@/.test(finalText));
  check('no uncaught errors on any of that', errors.length === 0, errors.slice(0, 3).join(' | '));
  check('nothing reached off-site', offSite.length === 0, offSite.slice(0, 3).join(' | '));
  check('every db-connect call was a read', world.calls.every((c) => c.action === 'execute' && /^\s*SELECT/i.test(String(c.sql || ''))));

  // The console ships one light theme plus the named alternative; the card
  // uses only shared tokens, so it follows whichever is active.
  await page.evaluate(() => document.documentElement.setAttribute('data-theme', 'financial'));
  await page.waitForTimeout(150);
  check('the card takes its colours from the theme tokens',
    await page.$eval('#cyg-collation-card .col-title', (e) => getComputedStyle(e).color !== 'rgba(0, 0, 0, 0)'));
  check('and every severity is a word, not only a colour',
    await page.$$eval('#cyg-collation-card .col-sev', (els) => els.every((e) => /^(high|medium|low)$/.test(e.textContent.trim()))));
  await shot('collation-5-financial.png');
  await page.evaluate(() => document.documentElement.removeAttribute('data-theme'));

  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  check('no horizontal overflow at 1440', overflow <= 0, 'overflow ' + overflow);
  for (const w of [1024, 768]) {
    await page.setViewportSize({ width: w, height: 1000 });
    await page.waitForTimeout(150);
    const o = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
    check('no horizontal overflow at ' + w, o <= 2, 'overflow ' + o);
  }

  await browser.close();
  server.close();
  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
