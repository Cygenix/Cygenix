/* tests/browser/diagnostics.smoke.js
 * ---------------------------------------------------------------------------
 * The Diagnostics tab, in a browser, against a stub of db-connect.
 *
 * tests/diagnostics.test.js pins the rules — the thresholds, the error
 * classification, the type-compatibility table, that the report scrubber
 * removes a password. What that cannot show is that a ticked box runs a test
 * and an unticked one does not, that a wrong password reaches the screen as
 * "Login failed" and nothing more, that the Skipped word appears where a
 * PostgreSQL profile has no such thing to check, and that the downloaded
 * report is a file with hosts in it and no credentials. Those only happen in
 * a browser, so db-connect, the data proxy, the scheduler and /api/health
 * are intercepted at the network and the real cygenix-diagnostics.js is put
 * through them.
 *
 * The sharpest assertion here is on the network log rather than the DOM:
 * with test 7 unticked, NO request carrying probe:'ping' is made. A panel
 * that ran every test and merely hid the unticked rows would pass a DOM
 * check and fail this one.
 *
 * Not part of `npm test`: it needs a browser.
 *   node tests/browser/diagnostics.smoke.js
 * Screenshots (optional): SHOTS=/path/to/dir
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8413);
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
    return res.end('/* stubbed: the diagnostics tab is under test, not the auth gate */');
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
// The three secrets. If any of these strings reaches the screen, the
// report, or the console, the test fails — that is the point of them.
const SECRETS = ['Sup3rSecret', 'Hunter2!', 'PgPass9'];

const SEED = (a) => {
  const tok = 'x.' + btoa(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: a.U })).replace(/=+$/, '') + '.y';
  [localStorage, sessionStorage].forEach((s) => { s.setItem('cygenix_token', tok); s.setItem('cygenix_expires', String(Date.now() + 36e5)); });
  localStorage.setItem('cygenix_user', JSON.stringify({ email: a.U, name: 'A Tester' }));
  // The Task Agent reads the Entra account record for the caller's email
  // (test 17 goes through it), and login.html writes exactly this shape.
  localStorage.setItem('cygenix_entra_account', JSON.stringify({ email: a.U, userId: a.U }));
  localStorage.setItem('cygenix_active_user', a.U);
  localStorage.setItem('cygenix_cookie_consent', 'all');
  localStorage.setItem('cygenix_onboarded', '1');
  localStorage.setItem('cygenix_tier', 'pro');
  const acct = { homeAccountId: 'h.t', environment: 'cygenix.ciamlogin.com', tenantId: 't', username: a.U, localAccountId: 'l', authorityType: 'MSSTS', name: 'A Tester' };
  localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify(acct));
  localStorage.setItem('h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-', JSON.stringify({ credentialType: 'IdToken', secret: tok, expiresOn: String(Math.floor(Date.now() / 1000) + 3600) }));
  sessionStorage.setItem('cygenix_rbac_me', JSON.stringify({ at: Date.now(), me: { oid: 'x', email: a.U, roles: ['OW', 'PA'] } }));
  if (localStorage.getItem('cygenix_profiles_v1')) return;   // seed once; a reload keeps what the panel saved
  localStorage.setItem('cygenix_projects', JSON.stringify([{ id: 'p1', name: 'Northbank Core' }]));
  localStorage.setItem('cygenix_active_project_id', 'p1');
  localStorage.setItem('cygenix_jobs', JSON.stringify([
    { id: 'job_ledger', name: 'Ledger to ledger_entry', projectId: 'p1', profileId: 'FIN-DEV-01', source: 'dbo.Ledger', sourceTable: 'dbo.Ledger', target: 'fin.ledger_entry',
      columnMapping: [{ srcCol: 'LedgerID', tgtCol: 'entry_id' }, { srcCol: 'AcctNo', tgtCol: 'account_no' }, { srcCol: 'Narrative', tgtCol: 'memo' }, { srcCol: 'Region', tgtCol: 'region' }] },
    { id: 'job_other', name: 'Unbound job', projectId: 'p1', source: 'dbo.Other', target: 'dbo.Other', columnMapping: [{ srcCol: 'a', tgtCol: 'a' }] },
  ]));
  const saved = {};
  saved[a.U] = [
    { id: 'c_src', name: 'Ledger source', side: 'src', mode: 'direct' },
    { id: 'c_tgt', name: 'Finance target', side: 'tgt', mode: 'direct' },
    { id: 'c_pg',  name: 'Postgres target', side: 'tgt', mode: 'direct' },
  ];
  localStorage.setItem('cygenix_saved_connections', JSON.stringify(saved));
  localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify({
    c_src: { connString: 'mssql://sa:' + a.S[0] + '@src.example.internal:1433/SRC' },
    c_tgt: { connString: 'Server=tcp:tgt.example.internal,1433;Database=TGT;User Id=loader;Password=' + a.S[1] + ';' },
    c_pg:  { connString: 'postgres://pg:' + a.S[2] + '@pg.example.internal:5432/tgtdb' },
  }));
  localStorage.setItem('cygenix_profiles_v1', JSON.stringify({ v: 1,
    profiles: [
      { id: 'FIN-DEV-01', name: 'Conv_DM to Azure', envClass: 'DEV', status: 'active', srcConnId: 'c_src', tgtConnId: 'c_tgt' },
      { id: 'PG-DEV-02', name: 'Ledger to Postgres', envClass: 'DEV', status: 'active', srcConnId: 'c_src', tgtConnId: 'c_pg' },
    ],
    bindings: [], connMeta: {}, runRecords: [], events: [],
    settings: { envClasses: [], activeProfileId: 'FIN-DEV-01', selectedAt: 1 } }));
};

// Hosts dashboard.html itself references. Derived, not listed.
const PAGE_HOSTS = (() => {
  const html = fs.readFileSync(path.join(PUB, 'dashboard.html'), 'utf8');
  const hosts = new Set(['fonts.gstatic.com']);
  for (const m of html.matchAll(/https:\/\/([a-z0-9.-]+)\//gi)) hosts.add(m[1]);
  return [...hosts];
})();

/* ── The stub database, keyed by which secret the connection string carries */
const SRC_COLS = [
  { name: 'LedgerID', type: 'INT', baseType: 'int', nullable: false },
  { name: 'AcctNo', type: 'VARCHAR(12)', baseType: 'varchar', maxLength: 12, nullable: true },
  { name: 'Narrative', type: 'VARCHAR(255)', baseType: 'varchar', maxLength: 255, nullable: true },
  { name: 'Region', type: 'CHAR(2)', baseType: 'char', maxLength: 2, nullable: true },
];
const TGT_COLS = [
  { name: 'entry_id', type: 'BIGINT', baseType: 'bigint', nullable: false, isIdentity: false },
  { name: 'account_no', type: 'VARCHAR(12)', baseType: 'varchar', maxLength: 12, nullable: false, default: null },
  { name: 'memo', type: 'VARCHAR(120)', baseType: 'varchar', maxLength: 120, nullable: true },
];

const world = { badPassword: false, calls: [] };

function whichDb(cs) {
  if (cs.indexOf(SECRETS[0]) !== -1) return 'src';
  if (cs.indexOf(SECRETS[1]) !== -1) return 'tgt';
  if (cs.indexOf(SECRETS[2]) !== -1) return 'pg';
  return 'unknown';
}

function dbAnswer(body) {
  const db = whichDb(body.connectionString || '');
  const pg = db === 'pg';
  const ok = (o) => ({ status: 200, body: Object.assign({ success: true }, o) });
  if (db === 'unknown') return { status: 400, body: { error: 'connectionString is required' } };
  if (db === 'src' && world.badPassword) {
    return { status: 500, body: { error: "Could not connect: Login failed for user 'sa'. The password is " + SECRETS[0],
      hint: 'Authentication failed — check the username and password in your connection string.' } };
  }
  switch (body.action) {
    case 'test':
      return pg ? ok({ version: 'PostgreSQL 15.3', database: 'tgtdb', user: 'pg' })
                : ok({ version: 'Microsoft SQL Server 2019 (RTM-CU18) (KB5017593) - 15.0.4261.1 (X64)', database: db === 'src' ? 'SRC' : 'TGT', user: db === 'src' ? 'sa' : 'loader' });
    case 'schema-columns': {
      const key = (body.schemaName + '.' + body.tableName).toLowerCase();
      const cols = key === 'dbo.ledger' ? SRC_COLS : key === 'fin.ledger_entry' ? TGT_COLS : [];
      return ok({ table: { schema: body.schemaName, name: body.tableName, columns: cols, primaryKeys: [], foreignKeys: [] } });
    }
    case 'diag-temp-table':
      return ok({ engine: pg ? 'postgres' : 'mssql', rows: 1, remainsAfterRollback: false, remainsAfterCleanup: false });
    case 'diag-probe':
      switch (body.probe) {
        case 'version':
          return pg ? ok({ engine: 'postgres', version: 'PostgreSQL 15.3', serverVersion: '15.3', serverVersionNum: 150003 })
                    : ok({ engine: 'mssql', version: '15.0.4261.1', level: 'RTM', edition: 'Standard Edition (64-bit)', compatibilityLevel: 150 });
        case 'ping':
          return ok({ engine: pg ? 'postgres' : 'mssql', samplesMs: db === 'tgt' ? [250, 270, 260] : [40, 45, 41], avgMs: db === 'tgt' ? 260 : 42 });
        case 'read-access':
          return ok({ engine: 'mssql', tables: (body.tables || []).map((t) => ({ schema: t.schema, name: t.name, exists: true, select: true })) });
        case 'write-access':
          return ok({ engine: pg ? 'postgres' : 'mssql', tables: (body.tables || []).map((t) => ({ schema: t.schema, name: t.name, exists: true, insert: true, update: true, 'delete': true })),
            createTable: true, schemas: (body.tables || []).map((t) => ({ schema: t.schema, alter: true, create: true })) });
        case 'bulk':
          return pg ? ok({ engine: 'postgres', supported: false, reason: 'Not supported on PostgreSQL' })
                    : ok({ engine: 'mssql', supported: true, serverPermission: false, bulkadmin: false, databasePermission: false });
        case 'space':
          return pg ? ok({ engine: 'postgres', databaseBytes: 1288490188, freeSupported: false })
                    : ok({ engine: 'mssql', files: [
                        { name: 'TGT', kind: 'ROWS', sizeMb: 20480, usedMb: 15000, freeMb: 5480, maxSizeMb: null, autogrow: true },
                        { name: 'TGT_log', kind: 'LOG', sizeMb: 4096, usedMb: 100, freeMb: 3996, maxSizeMb: null, autogrow: true }],
                      volumes: null, volumesUnavailable: 'VIEW SERVER STATE permission was denied' });
        case 'collation':
          return db === 'src'
            ? ok({ engine: 'mssql', collation: 'Latin1_General_CI_AS', columns: [{ schema: 'dbo', name: 'Ledger', column: 'Narrative', collation: 'Latin1_General_CI_AS' }] })
            : ok({ engine: pg ? 'postgres' : 'mssql', collation: pg ? 'en_GB.UTF-8' : 'SQL_Latin1_General_CP1_CI_AS',
                   columns: [{ schema: 'fin', name: 'ledger_entry', column: 'memo', collation: 'SQL_Latin1_General_CP1_CI_AS' }] });
        default:
          return { status: 400, body: { error: 'Unknown probe: ' + body.probe } };
      }
    default:
      return { status: 400, body: { error: 'Unknown action: ' + body.action } };
  }
}

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 960 }, acceptDownloads: true });

  const offSite = [];
  await ctx.route('**', (route) => {
    const u = route.request().url();
    const json = (status, body) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
    if (/\/api\/health$/.test(u) || /functions\/health/.test(u)) return json(200, { status: 'ok', timestamp: new Date().toISOString() });
    if (/functions\/data-proxy/.test(u)) {
      if (/action=ping/.test(u)) return json(200, { ok: true });
      if (/action=whoami/.test(u)) return json(200, { email: U, role: 'user', tier: 'pro', tier_status: 'active', exists: true });
      // Everything else — load, save — is refused rather than answered with
      // an empty document: "cloud wins on page load" would take an empty
      // answer as the truth and clear the seeded profiles and jobs.
      return route.abort();
    }
    if (/functions\/scheduler/.test(u)) {
      return json(200, { schedules: [
        { id: 's1', name: 'Nightly ledger', jobId: 'job_ledger', enabled: true },
        { id: 's2', name: 'Orphan schedule', jobId: 'job_gone', enabled: true },
        { id: 's3', name: 'Someone else’s', jobId: 'job_other', enabled: true },
      ] });
    }
    if (/functions\/db-connect/.test(u)) {
      let body = {};
      try { body = JSON.parse(route.request().postData() || '{}'); } catch (e) { body = {}; }
      world.calls.push(body);
      const a = dbAnswer(body);
      return json(a.status, a.body);
    }
    if (/functions\/(audit|rbac-admin|projects)/.test(u)) return json(200, {});
    if (u.startsWith('http://localhost:' + PORT)) return route.continue();
    try {
      if (PAGE_HOSTS.indexOf(new URL(u).host) === -1) offSite.push(u);
    } catch (e) { offSite.push(u); }
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

  const openDiag = async () => {
    await page.waitForFunction(() => !!window.CygenixDiagnostics && typeof window.showView === 'function', null, { timeout: 20000 });
    await page.evaluate(() => {
      window.getCygenixIdToken = () => 'smoke-token';
      window.getCygenixIdTokenAsync = async () => localStorage.getItem('cygenix_token');
      window.showView('diagnostics');
    });
    await page.waitForSelector('#cyg-diag .dg-row', { timeout: 10000 });
  };
  const runDone = () => page.waitForFunction(() => {
    const b = document.getElementById('dg-run');
    return !!b && /^Run selected/.test(b.textContent);
  }, null, { timeout: 60000 });
  const status = (id) => page.textContent('#dg-st-' + id).then((s) => s.trim());
  const out = (id) => page.textContent('#dg-out-' + id).then((s) => s.replace(/\s+/g, ' ').trim());
  const setOnly = async (ids) => {
    await page.click('#dg-none');
    for (const id of ids) await page.check('input[data-test-sel="' + id + '"]');
  };
  // The console's .main pane scrolls on its own, so fullPage would show
  // only the first screen; a tall viewport shows the whole panel.
  const shot = async (name) => {
    if (!SHOTS) return;
    await page.setViewportSize({ width: 1440, height: 2400 });
    await page.waitForTimeout(150);
    await page.screenshot({ path: path.join(SHOTS, name) });
    await page.setViewportSize({ width: 1440, height: 960 });
  };

  console.log('Diagnostics — the tab, in a browser\n');

  // ── 1. It renders ───────────────────────────────────────────────────────
  console.log('1. The panel');
  await page.goto('http://localhost:' + PORT + '/dashboard', { waitUntil: 'domcontentloaded' });
  await openDiag();

  check('the kicker and title are in the console vocabulary',
    (await page.textContent('#cyg-diag .cx-kicker')).indexOf('Diagnostics') !== -1
    && (await page.textContent('#cyg-diag .cx-title')) === 'Connection diagnostics');
  check('the old developer-facing button is gone', (await page.$$('#view-diagnostics [onclick*="runDiagnostics"]')).length === 0
    && (await page.textContent('#view-diagnostics')).indexOf('Netlify function setup') === -1);
  check('four groups, seventeen tests', (await page.$$('#cyg-diag .dg-group')).length === 4 && (await page.$$('#cyg-diag .dg-row')).length === 17);
  check('every test has a checkbox, a name and a one-line description',
    await page.$$eval('#cyg-diag .dg-row', (rows) => rows.every((r) => r.querySelector('input[type=checkbox]') && r.querySelector('.dg-name').textContent.trim() && r.querySelector('.dg-desc').textContent.trim())));
  check('every row starts as Not run', await page.$$eval('#cyg-diag .dg-st-none', (els) => els.length) === 17);
  check('all tests are ticked on a first visit and the button counts them',
    (await page.textContent('#dg-run')).trim() === 'Run selected (17)');
  check('the profile selector defaults to the active profile',
    await page.$eval('#dg-profile', (s) => s.value) === 'FIN-DEV-01'
    && (await page.$$('#dg-profile option')).length === 2);
  const bar = await page.textContent('#dg-bar');
  check('the bar shows the source and target as host only', /src\.example\.internal/.test(bar) && /tgt\.example\.internal/.test(bar));
  check('and never the user, port, database or password',
    !/sa@|loader|1433|SRC\b|TGT\b/.test(bar) && SECRETS.every((s) => bar.indexOf(s) === -1));
  check('the environment tag is drawn from the profile', (await page.textContent('#dg-env')).trim() === 'DEV');
  check('Source / Target / Both defaults to Both', await page.$eval('#dg-side button.on', (b) => b.getAttribute('data-side')) === 'both');

  // Group toggle collapses and the per-group checkbox selects the group.
  await page.click('#cyg-diag .dg-group[data-group="permissions"] .dg-gtoggle');
  check('a group heading collapses its rows',
    await page.$eval('#cyg-diag .dg-group[data-group="permissions"]', (s) => s.classList.contains('closed') && getComputedStyle(s.querySelector('.dg-rows')).display === 'none'));
  await page.click('#cyg-diag .dg-group[data-group="permissions"] .dg-gtoggle');
  await page.uncheck('input[data-group-sel="platform"]');
  check('the per-group toggle clears the whole group and the count follows',
    (await page.textContent('#dg-run')).trim() === 'Run selected (14)'
    && (await page.textContent('#dg-gcount-platform')).trim() === '0 of 3 selected');
  await page.click('#dg-all');
  check('Select all restores them', (await page.textContent('#dg-run')).trim() === 'Run selected (17)');
  await shot('diag-1-before.png');

  // ── 2. An unticked test does not run ────────────────────────────────────
  console.log('\n2. Run selected, with test 7 unticked');
  await page.uncheck('input[data-test-sel="t07"]');
  world.calls = [];
  await page.click('#dg-run');
  await page.waitForFunction(() => /Running/.test(document.getElementById('dg-run').textContent), null, { timeout: 5000 });
  check('the button says Running and is disabled while the run is in flight',
    await page.$eval('#dg-run', (b) => b.disabled && /Running/.test(b.textContent)));
  await runDone();
  check('test 7 stayed Not run', await status('t07') === 'Not run');
  check('AND NO PING PROBE WAS SENT', !world.calls.some((c) => c.action === 'diag-probe' && c.probe === 'ping'),
    world.calls.filter((c) => c.probe === 'ping').length + ' ping probe(s) went out');
  check('the last-run stamp is set', /Last run/.test(await page.textContent('#dg-lastrun')));
  check('Copy and Download are enabled once there are results',
    await page.$eval('#dg-copy', (b) => !b.disabled) && await page.$eval('#dg-download', (b) => !b.disabled));

  check('1 services: Pass with two response times', await status('t01') === 'Pass' && /Web service: \d+ ms · Data service: \d+ ms/.test(await out('t01')));
  check('2 licence: Pass naming the plan', await status('t02') === 'Pass' && /Pro plan active/.test(await out('t02')));
  check('3 region: Pass, UK South', await status('t03') === 'Pass' && /UK South/.test(await out('t03')));
  for (const [id, word] of [['t04', 'Reached'], ['t05', 'Signed in as'], ['t06', 'Opened']]) {
    check(id.replace('t0', '') + ' connection: Pass on both sides', await status(id) === 'Pass'
      && (await page.$$('#dg-out-' + id + ' .dg-part')).length === 2 && new RegExp(word).test(await out(id)));
  }
  check('4 shows the host, never the connection string', /src\.example\.internal/.test(await out('t04')) && SECRETS.every((s) => true));
  check('8 version: Pass with the compatibility level', await status('t08') === 'Pass' && /SQL Server 15\.0\.4261\.1 · compatibility level 150/.test(await out('t08')));
  check('9 source read: Pass over the mapped tables only',
    await status('t09') === 'Pass' && /1 mapped source table/.test(await out('t09'))
    && world.calls.some((c) => c.probe === 'read-access' && c.tables.length === 1 && c.tables[0].name === 'Ledger'));
  check('and the unbound job’s table was not sent', !world.calls.some((c) => (c.tables || []).some((t) => t.name === 'Other')));
  check('10 target write: Pass', await status('t10') === 'Pass' && /Can write all 1 mapped target table/.test(await out('t10')));
  check('11 bulk: Warn with a what-to-do sentence', await status('t11') === 'Warn' && /What to do:.*ADMINISTER BULK OPERATIONS/.test(await out('t11')));
  check('12 temp table: Pass and says nothing was left behind', await status('t12') === 'Pass' && /nothing left behind/.test(await out('t12')));
  check('13 mapped objects: Fail listing the missing target column',
    await status('t13') === 'Fail' && /target column fin\.ledger_entry\.region/.test(await out('t13')) && /re-map/.test(await out('t13')));
  check('14 types: Warn, never Fail, naming the truncation and the nullability',
    await status('t14') === 'Warn' && /length 255 to 120 may truncate/.test(await out('t14')) && /allows NULL but target does not/.test(await out('t14')));
  check('15 space: Pass with data and log free', await status('t15') === 'Pass' && /Data 5\.4 GB free · log 3\.9 GB free/.test(await out('t15')));
  check('16 collation: Warn naming both collations', await status('t16') === 'Warn' && /Latin1_General_CI_AS vs SQL_Latin1_General_CP1_CI_AS/.test(await out('t16')));
  check('17 schedules: Fail on the orphan, ignoring the other profile’s schedule',
    await status('t17') === 'Fail' && /Orphan schedule/.test(await out('t17')) && !/Someone else/.test(await out('t17')) && /1 schedule will not run/.test(await out('t17')),
    (await status('t17')) + ' / ' + (await out('t17')));
  check('details are behind a disclosure, not on the face of the row',
    (await page.$$('#cyg-diag details.dg-details')).length > 0
    && await page.$$eval('#cyg-diag details.dg-details', (els) => els.every((d) => !d.open)));
  await shot('diag-2-results.png');

  // ── 3. The in-flight guard ──────────────────────────────────────────────
  console.log('\n3. Render-loop rule');
  const guard = await page.evaluate(async () => {
    const D = window.CygenixDiagnostics;
    const first = D.run();                 // starts (or is refused by the 3s interval)
    const second = await D.run();          // must be refused while the first is in flight
    const r1 = await first;
    return { second, r1 };
  });
  check('a second Run while one is in flight is refused', guard.second === false);
  await runDone();
  const t0 = Date.now();
  const again = await page.evaluate(() => window.CygenixDiagnostics.run());
  check('and a Run inside the 3-second interval is refused too', again === false, 'elapsed ' + (Date.now() - t0));

  // ── 4. A wrong password ─────────────────────────────────────────────────
  console.log('\n4. A wrong password');
  world.badPassword = true;
  await page.waitForTimeout(3100);        // past the minimum interval
  await setOnly(['t04', 't05', 't06']);
  check('clearing then ticking three gives Run selected (3)', (await page.textContent('#dg-run')).trim() === 'Run selected (3)');
  await page.click('#dg-run');
  await runDone();
  const o5 = await out('t05');
  check('5 login: Fail on the source with the friendly sentence',
    await status('t05') === 'Fail' && /Source.*Login failed/.test(o5) && /What to do: Login failed: check the username and password/.test(o5));
  check('4 still passes for the source — the server was reached', /Source.*Reached src\.example\.internal/.test(await out('t04')));
  check('6 is Skipped on the source with the reason, Pass on the target', /Source.*Login failed/.test(await out('t06')) && /Target.*Opened TGT/.test(await out('t06')));
  const o5open = await page.$eval('#dg-out-t05 details pre', (e) => e.textContent);
  check('the technical detail is there, under Details', /Login failed for user/.test(o5open));
  check('AND THE PASSWORD THE DRIVER ECHOED IS NOT', o5open.indexOf(SECRETS[0]) === -1 && o5open.indexOf('***') !== -1, o5open);
  world.badPassword = false;
  await shot('diag-4-login-failed.png');

  // ── 5. A PostgreSQL target ──────────────────────────────────────────────
  console.log('\n5. A PostgreSQL profile');
  await page.selectOption('#dg-profile', 'PG-DEV-02');
  check('changing profile clears the results', (await page.$$('#cyg-diag .dg-st-none')).length === 17);
  check('and shows the new target host', /pg\.example\.internal/.test(await page.textContent('#dg-tgt')) && /PostgreSQL/.test(await page.textContent('#dg-tgt')));
  await page.waitForTimeout(3100);
  await setOnly(['t08', 't11', 't12', 't15', 't16']);
  await page.click('#dg-run');
  await runDone();
  check('8 reads the PostgreSQL version', /PostgreSQL 15\.3/.test(await out('t08')));
  check('11 bulk: Skipped, "Not supported on PostgreSQL"', await status('t11') === 'Skipped' && /Not supported on PostgreSQL/.test(await out('t11')));
  check('12 temp table still runs on PostgreSQL', await status('t12') === 'Pass');
  check('15 space: Skipped, with the database size', await status('t15') === 'Skipped' && /Database size 1\.2 GB/.test(await out('t15')));
  check('16 collation: Skipped across engines', await status('t16') === 'Skipped' && /not comparable across SQL Server and PostgreSQL/.test(await out('t16')));
  await shot('diag-5-postgres.png');

  // ── 6. The report ───────────────────────────────────────────────────────
  console.log('\n6. The report');
  const dl1 = page.waitForEvent('download', { timeout: 8000 }).catch(() => null);
  await page.click('#dg-download');
  const d1 = await dl1;
  check('Download produces a .txt by default', !!d1 && /^cygenix_diagnostics_\d{4}-\d{2}-\d{2}\.txt$/.test(d1.suggestedFilename()));
  const txt = d1 ? fs.readFileSync(await d1.path(), 'utf8') : '';
  check('the text report names the profile, the hosts and every result',
    /CYGENIX DIAGNOSTICS REPORT/.test(txt) && /Ledger to Postgres/.test(txt) && /pg\.example\.internal/.test(txt) && /11\. Bulk load permission\s+SKIPPED/.test(txt));
  check('AND CONTAINS NO CREDENTIAL', SECRETS.every((s) => txt.indexOf(s) === -1) && !/password=|pwd=|code=|:\/\/\w+:\w+@/i.test(txt));
  await page.selectOption('#dg-fmt', 'json');
  const dl2 = page.waitForEvent('download', { timeout: 8000 }).catch(() => null);
  await page.click('#dg-download');
  const d2 = await dl2;
  check('and a .json when asked', !!d2 && /\.json$/.test(d2.suggestedFilename()));
  let js = null;
  try { js = JSON.parse(fs.readFileSync(await d2.path(), 'utf8')); } catch (e) { js = null; }
  check('the JSON is well-formed, carries the results and no credential',
    !!js && Array.isArray(js.results) && js.results.length === 5 && js.target.host === 'pg.example.internal'
    && SECRETS.every((s) => JSON.stringify(js).indexOf(s) === -1));
  await page.click('#dg-copy');
  await page.waitForFunction(() => document.getElementById('dg-copy').textContent === 'Copied', null, { timeout: 4000 }).catch(() => {});
  check('Copy report acknowledges', (await page.textContent('#dg-copy')) === 'Copied');

  // ── 7. Remembered per user ──────────────────────────────────────────────
  console.log('\n7. Remembered');
  await page.reload({ waitUntil: 'domcontentloaded' });
  await openDiag();
  check('the selection survives a reload', (await page.textContent('#dg-run')).trim() === 'Run selected (5)'
    && await page.$eval('input[data-test-sel="t07"]', (c) => !c.checked) && await page.$eval('input[data-test-sel="t11"]', (c) => c.checked));
  check('and so does the profile and the format', await page.$eval('#dg-profile', (s) => s.value) === 'PG-DEV-02' && await page.$eval('#dg-fmt', (s) => s.value) === 'json');
  check('kept under the user’s own key', await page.evaluate((u) => {
    const all = JSON.parse(localStorage.getItem('cygenix_diag_selection_v1') || '{}');
    return !!all[u] && Array.isArray(all[u].selected) && all[u].selected.length === 5;
  }, U));

  // ── 8. Themes ───────────────────────────────────────────────────────────
  console.log('\n8. Themes');
  const chipColour = () => page.$eval('#cyg-diag .dg-st-none', (e) => getComputedStyle(e).color);
  const lightChip = await chipColour();
  await page.evaluate(() => document.documentElement.setAttribute('data-theme', 'financial'));
  await page.waitForTimeout(100);
  check('the panel takes its colours from the theme tokens (the alternative theme changes them)',
    await page.$eval('#cyg-diag .dg-name', (e) => getComputedStyle(e).color !== 'rgba(0, 0, 0, 0)') && !!lightChip);
  check('and stays readable: every status is a word, not only a colour',
    await page.$$eval('#cyg-diag .dg-st', (els) => els.every((e) => /^(Not run|Pass|Warn|Fail|Skipped|Running)$/.test(e.textContent.trim()))));
  await shot('diag-8-financial.png');
  await page.evaluate(() => document.documentElement.removeAttribute('data-theme'));

  // ── 9. Hygiene ──────────────────────────────────────────────────────────
  console.log('\n9. Hygiene');
  check('no uncaught errors on any of that', errors.length === 0, errors.slice(0, 3).join(' | '));
  check('nothing reached off-site', offSite.length === 0, offSite.slice(0, 3).join(' | '));
  check('NO SECRET WAS WRITTEN TO THE CONSOLE', consoleText.every((t) => SECRETS.every((s) => t.indexOf(s) === -1)));
  check('every db-connect call carried a connection string and never a bare SQL statement for a probe',
    world.calls.every((c) => typeof c.connectionString === 'string') && !world.calls.some((c) => c.action === 'diag-probe' && c.sql));
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  check('no horizontal overflow at 1440', overflow <= 0, 'overflow ' + overflow);
  for (const w of [1024, 768]) {
    await page.setViewportSize({ width: w, height: 900 });
    await page.waitForTimeout(150);
    const o = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
    check('no horizontal overflow at ' + w, o <= 2, 'overflow ' + o);
  }

  await browser.close();
  server.close();
  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
