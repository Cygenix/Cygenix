/* tests/browser/collation-wiring.smoke.js
 * ---------------------------------------------------------------------------
 * Stage B, in a browser: the badges on Object Mapping and the lint in the
 * SQL editor.
 *
 * tests/collation-rules.test.js proves the rules are right and that every
 * module calls them. What it cannot show is that a badge reaches a table row
 * that is rebuilt on every keystroke, that its tooltip carries both
 * collations, that the SQL panel's banner survives a tab switch, and that a
 * query typed into the editor is linted before it runs. Those only happen in
 * a browser.
 *
 * The sharpest assertion here is the last one in section 2: with the two
 * databases on the SAME collation, the page draws no warning anywhere. The
 * brief's final verification line is exactly that, and a scanner that
 * guesses would pass every other check in this file and fail that one.
 *
 * Not part of `npm test`: it needs a browser.
 *   node tests/browser/collation-wiring.smoke.js
 * Screenshots (optional): SHOTS=/path/to/dir
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8419);
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
    return res.end('/* stubbed: the collation wiring is under test, not the auth gate */');
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
const SECRET = 'Sup3rSecret';

/* The saved state: a profile whose two databases disagree, a scan that
   recorded the column collations, and a job mapping four columns. */
const SRC_COLS = [
  { side: 'src', schema: 'dbo', table: 'Ledger', column: 'AcctNo', collation: 'Latin1_General_CS_AS', dataType: 'varchar', codePage: 1252, inUniqueKey: false, maxLength: 12 },
  { side: 'src', schema: 'dbo', table: 'Ledger', column: 'Narrative', collation: 'Latin1_General_CI_AS', dataType: 'varchar', codePage: 1252, inUniqueKey: false, maxLength: 255 },
  { side: 'src', schema: 'dbo', table: 'Ledger', column: 'Code', collation: 'Latin1_General_CS_AS', dataType: 'varchar', codePage: 1252, inUniqueKey: false, maxLength: 20 },
];
const TGT_COLS = [
  { side: 'tgt', schema: 'fin', table: 'ledger_entry', column: 'account_no', collation: 'SQL_Latin1_General_CP1_CI_AS', dataType: 'varchar', codePage: 1252, inUniqueKey: false, maxLength: 12 },
  { side: 'tgt', schema: 'fin', table: 'ledger_entry', column: 'memo', collation: 'SQL_Latin1_General_CP1_CI_AS', dataType: 'varchar', codePage: 1252, inUniqueKey: false, maxLength: 120 },
  { side: 'tgt', schema: 'fin', table: 'ledger_entry', column: 'code', collation: 'SQL_Latin1_General_CP1_CI_AS', dataType: 'varchar', codePage: 1252, inUniqueKey: true, maxLength: 20 },
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

  localStorage.setItem('cygenix_projects', JSON.stringify([{ id: 'p1', name: 'Northbank Core' }]));
  localStorage.setItem('cygenix_active_project_id', 'p1');
  localStorage.setItem('cygenix_jobs', JSON.stringify([
    { id: 'job_ledger', name: 'Ledger to ledger_entry', projectId: 'p1', profileId: 'FIN-DEV-01',
      source: 'dbo.Ledger', sourceTable: 'dbo.Ledger', target: 'fin.ledger_entry',
      columnMapping: [
        { srcCol: 'AcctNo', tgtCol: 'account_no' },
        { srcCol: 'Narrative', tgtCol: 'memo' },
        { srcCol: 'Code', tgtCol: 'code' },
      ] },
  ]));
  const saved = {};
  saved[a.U] = [
    { id: 'c_src', name: 'Ledger source', side: 'src', mode: 'direct' },
    { id: 'c_tgt', name: 'Finance target', side: 'tgt', mode: 'direct' },
  ];
  localStorage.setItem('cygenix_saved_connections', JSON.stringify(saved));
  localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify({
    c_src: { connString: 'mssql://sa:' + a.S + '@src.example.internal:1433/SRC' },
    c_tgt: { connString: 'Server=tcp:tgt.example.internal,1433;Database=TGT;User Id=loader;Password=' + a.S + ';' },
  }));
  // Both connections are also the ambient project pair, so pages that do not
  // adopt profiles still resolve a source and target.
  const pc = {}; pc[a.U] = {
    srcConnString: 'mssql://sa:' + a.S + '@src.example.internal:1433/SRC',
    tgtConnString: 'Server=tcp:tgt.example.internal,1433;Database=TGT;User Id=loader;Password=' + a.S + ';',
  };
  localStorage.setItem('cygenix_project_connections', JSON.stringify(pc));

  const collation = {
    version: 1,
    source: { server: 'src.example.internal', database: 'SRC', serverCollation: 'Latin1_General_CI_AS',
      dbCollation: 'Latin1_General_CI_AS', tempdbCollation: 'Latin1_General_CI_AS', codePage: 1252,
      cs: false, as: true, utf8: false, productVersion: '15.0.4261.1', detectedAt: new Date().toISOString() },
    target: { server: 'tgt.example.internal', database: 'TGT', serverCollation: 'SQL_Latin1_General_CP1_CI_AS',
      dbCollation: 'SQL_Latin1_General_CP1_CI_AS', tempdbCollation: 'SQL_Latin1_General_CP1_CI_AS', codePage: 1252,
      cs: false, as: true, utf8: false, productVersion: '15.0.4261.1', detectedAt: new Date().toISOString() },
    fingerprint: { source: 'src.example.internal|SRC', target: 'tgt.example.internal|TGT' },
    strategy: 'target', explicitCollation: null, resolvedCollation: 'SQL_Latin1_General_CP1_CI_AS',
    tempTables: 'resolved', generatedSqlMode: 'apply', userSqlMode: 'offer_fix',
    caseRule: 'warn', codePageRule: 'warn', columnOverrides: {}, acknowledged: [],
    lastScan: { at: new Date().toISOString(), summary: { high: 1, medium: 2, low: 0 },
      columns: a.COLS,
      findings: [{ id: 'fin.ledger_entry.code|case_unique', severity: 'high', issueCode: 'case_unique',
        object: 'fin.ledger_entry.code', sourceCollation: 'Latin1_General_CS_AS',
        targetCollation: 'SQL_Latin1_General_CP1_CI_AS', issue: 'case risk on a unique key',
        recommendation: 'Give the target column a case-sensitive collation.', usedBy: ['object-mapping'], job: 'J' }] },
  };
  localStorage.setItem('cygenix_profiles_v1', JSON.stringify({
    v: 1, createdAt: Date.now(), connMeta: {}, bindings: [], runRecords: [], events: [],
    profiles: [{ id: 'FIN-DEV-01', name: 'Conv_DM to Azure', envClass: 'DEV', status: 'active',
      srcConnId: 'c_src', tgtConnId: 'c_tgt', updatedAt: 2, collation: a.MATCHED ? a.MATCHED_COLLATION : collation }],
    settings: { envClasses: [], activeProfileId: 'FIN-DEV-01', selectedAt: 1 },
  }));
};

const PAGE_HOSTS = (() => {
  const hosts = new Set(['fonts.gstatic.com']);
  // The app scripts as well as the markup: the SQL editor loads Monaco from
  // a CDN named in sql-editor-app.js, not in the page, and that is the
  // page's own dependency rather than anything this feature reaches for.
  ['object_mapping.html', 'sql-editor.html', 'object-mapping-app.js', 'sql-editor-app.js'].forEach((f) => {
    const src = fs.readFileSync(path.join(PUB, f), 'utf8');
    for (const m of src.matchAll(/https:\/\/([a-z0-9.-]+)\//gi)) hosts.add(m[1]);
  });
  return [...hosts];
})();

/* A matched-collation variant of the same saved state, for the final
   no-false-warnings check. */
function matchedCollation(base) {
  const m = JSON.parse(JSON.stringify(base));
  m.source.dbCollation = 'Latin1_General_CI_AS';
  m.target.dbCollation = 'Latin1_General_CI_AS';
  m.target.tempdbCollation = 'Latin1_General_CI_AS';
  m.resolvedCollation = 'Latin1_General_CI_AS';
  m.lastScan.findings = [];
  m.lastScan.summary = { high: 0, medium: 0, low: 0 };
  m.lastScan.columns = m.lastScan.columns.map((c) => Object.assign({}, c, { collation: 'Latin1_General_CI_AS' }));
  return m;
}

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 1000 } });

  const offSite = [];
  const calls = [];
  await ctx.route('**', (route) => {
    const u = route.request().url();
    const json = (status, body) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
    if (/functions\/data-proxy/.test(u)) {
      if (/action=whoami/.test(u)) return json(200, { email: U, tier: 'pro', tier_status: 'active', exists: true });
      return route.abort();     // never answer load with {} — it would wipe the seed
    }
    if (/functions\/db-connect/.test(u)) {
      let body = {}; try { body = JSON.parse(route.request().postData() || '{}'); } catch (e) { body = {}; }
      calls.push(body);
      // Enough of a schema for Object Mapping to render its table.
      if (body.action === 'schema-tables') {
        return json(200, { success: true, database: 'SRC', tables: [
          { schema: 'dbo', name: 'Ledger', kind: 'table', rowCount: 100 },
          { schema: 'fin', name: 'ledger_entry', kind: 'table', rowCount: 0 }] });
      }
      if (body.action === 'schema-columns') {
        const src = String(body.tableName).toLowerCase() === 'ledger';
        const cols = (src ? SRC_COLS : TGT_COLS).map((c) => ({
          name: c.column, type: c.dataType.toUpperCase() + '(' + c.maxLength + ')',
          baseType: c.dataType, maxLength: c.maxLength, nullable: true, ordinal: 1 }));
        return json(200, { success: true, table: { schema: body.schemaName, name: body.tableName,
          columns: cols, primaryKeys: [], foreignKeys: [], uniques: [] } });
      }
      return json(200, { success: true, recordset: [], rowsAffected: 0 });
    }
    if (/functions\//.test(u)) return json(200, {});
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

  const shot = async (name) => {
    if (!SHOTS) return;
    await page.screenshot({ path: path.join(SHOTS, name), fullPage: true });
  };

  console.log('Collation — Stage B wiring, in a browser\n');

  // ── 1. Object Mapping: badges and the summary ───────────────────────────
  console.log('1. Object Mapping');
  await page.addInitScript(SEED, { U, S: SECRET, COLS: SRC_COLS.concat(TGT_COLS), MATCHED: false });
  await page.goto('http://localhost:' + PORT + '/object_mapping', { waitUntil: 'domcontentloaded' });
  await page.waitForFunction(() => !!window.cygCollation && typeof window.renderMappingTable === 'function',
    null, { timeout: 20000 });

  check('the rules module loaded before the card, and the card defined itself',
    await page.evaluate(() => !!window.CygenixCollationRules && !!window.cygCollation));
  check('and the four public calls are there',
    await page.evaluate(() => ['settings', 'resolve', 'findClashes', 'gate']
      .every((k) => typeof window.cygCollation[k] === 'function')));
  check('settings come from the active profile',
    await page.evaluate(() => (window.cygCollation.settings() || {}).resolvedCollation) === 'SQL_Latin1_General_CP1_CI_AS');

  // Drive the page to a rendered mapping table the way the app does.
  await page.evaluate((cols) => {
    srcTable = { schema: 'dbo', name: 'Ledger', fullName: 'dbo.Ledger',
      columns: cols.src.map((c) => ({ name: c.column, type: c.dataType.toUpperCase() + '(' + c.maxLength + ')' })) };
    tgtTable = { schema: 'fin', name: 'ledger_entry', fullName: 'fin.ledger_entry',
      columns: cols.tgt.map((c) => ({ name: c.column, type: c.dataType.toUpperCase() + '(' + c.maxLength + ')' })) };
    columnMapping = [
      { tgtCol: 'account_no', srcCol: 'AcctNo', match: 'HIGH' },
      { tgtCol: 'memo', srcCol: 'Narrative', match: 'HIGH' },
      { tgtCol: 'code', srcCol: 'Code', match: 'HIGH' },
    ];
    window.renderMappingTable();
  }, { src: SRC_COLS, tgt: TGT_COLS });
  await page.waitForSelector('#mapping-tbody tr', { timeout: 8000, state: 'attached' });

  const badges = await page.$$('#mapping-tbody .cyg-coll-badge');
  check('EVERY MAPPED TEXT COLUMN PAIR CARRIES A BADGE', badges.length === 3, badges.length + ' badges');
  // The stylesheet used to be injected only when a BANNER was drawn, which
  // happens when SQL is generated — so a mapping table looked at before
  // anything had been generated showed these as bare unstyled words.
  check('and it is drawn as a badge, without waiting for a banner to bring the stylesheet',
    await page.$eval('#mapping-tbody .cyg-coll-badge', (e) => getComputedStyle(e).display) === 'inline-flex');
  check('all three pairs differ in a way that errors, so all three are red',
    (await page.$$('#mapping-tbody .cyg-coll-badge.fail')).length === 3,
    (await page.$$('#mapping-tbody .cyg-coll-badge.fail')).length + ' red');
  const codeTip = await page.getAttribute('#mapping-tbody tr:nth-child(3) .cyg-coll-badge', 'title');
  check('and the case-sensitive source into a unique case-insensitive target names the case difference',
    /case/.test(codeTip), codeTip);
  check('the other two differing pairs are drawn as differences, not as matches',
    (await page.$$('#mapping-tbody .cyg-coll-badge.ok')).length === 0);
  const tip = await page.getAttribute('#mapping-tbody tr:nth-child(1) .cyg-coll-badge', 'title');
  check('THE TOOLTIP NAMES BOTH COLLATIONS',
    /Latin1_General_CS_AS/.test(tip) && /SQL_Latin1_General_CP1_CI_AS/.test(tip), tip);
  check('and the fix', /Fix: COLLATE SQL_Latin1_General_CP1_CI_AS/.test(tip), tip);

  const summary = await page.textContent('#om-collation-summary');
  check('the summary line is above the table and counts the columns',
    await page.isVisible('#om-collation-summary') && /mapped column/.test(summary), summary);
  check('and links to the Collation card',
    await page.$eval('#om-collation-summary a', (a) => a.getAttribute('href')) === '/dashboard#goto=connections/databases');

  // The tbody is rebuilt on every edit; the badge has to come back with it.
  await page.evaluate(() => { columnMapping[1].srcCol = 'AcctNo'; window.renderMappingTable(); });
  await page.waitForTimeout(150);
  check('THE BADGES SURVIVE THE TABLE BEING REBUILT, which happens on every edit',
    (await page.$$('#mapping-tbody .cyg-coll-badge')).length === 3);
  await shot('collation-b1-mapping.png');

  // ── 2. Object Mapping: the SQL panel ────────────────────────────────────
  console.log('\n2. The generated SQL');
  await page.evaluate(() => {
    window.showSQLOutput('INSERT INTO fin.ledger_entry (code)\n'
      + 'SELECT s.Code FROM dbo.Ledger s\n'
      + 'JOIN fin.ledger_entry t ON t.code = s.Code;');
  });
  await page.waitForTimeout(200);
  check('a cross-collation join in the generated SQL is reported under the SQL tabs',
    await page.isVisible('#om-collation-banner')
    && /collation clash/i.test(await page.textContent('#om-collation-banner')));
  const banner = await page.textContent('#om-collation-banner');
  check('the banner names the line, both collations and the fix',
    /line 3/.test(banner) && /Latin1_General_CS_AS/.test(banner)
    && /COLLATE SQL_Latin1_General_CP1_CI_AS/.test(banner), banner);
  check('AND SAYS NOTHING WAS CHANGED — Stage B reports only',
    /Nothing has been changed/.test(banner));
  check('the SQL itself is untouched',
    (await page.textContent('#sql-output')).indexOf('COLLATE') === -1);

  // The banner is a sibling of #sql-output, so a tab switch must not wipe it.
  await page.evaluate(() => { generatedSQL = { insert: 'SELECT 1', schema: 'SELECT 2', verify: 'SELECT 3' }; window.showSQLTab('schema'); });
  await page.waitForTimeout(150);
  check('switching tabs re-lints rather than leaving a stale banner',
    !(await page.isVisible('#om-collation-banner')));
  await page.evaluate(() => {
    window.showSQLOutput('SELECT 1 FROM dbo.Ledger s JOIN fin.ledger_entry t ON t.code = s.Code');
  });
  await page.waitForTimeout(150);
  check('and it comes back on SQL that clashes', await page.isVisible('#om-collation-banner'));

  // ── 3. The SQL editor ───────────────────────────────────────────────────
  console.log('\n3. The SQL editor');
  await page.goto('http://localhost:' + PORT + '/sql-editor', { waitUntil: 'domcontentloaded' });
  await page.waitForFunction(() => !!window.cygCollation && typeof window.sqlEdLintCollation === 'function',
    null, { timeout: 20000 });
  check('the banner sits above the results grid, not inside it',
    await page.evaluate(() => {
      const b = document.getElementById('sqled-collation-banner');
      const r = document.getElementById('results-container');
      return !!b && !!r && !r.contains(b) && (b.compareDocumentPosition(r) & Node.DOCUMENT_POSITION_FOLLOWING) !== 0;
    }));
  const edLint = await page.evaluate(() => window.sqlEdLintCollation(
    'SELECT * FROM SRC.dbo.Ledger a\nJOIN TGT.fin.ledger_entry b ON a.Code = b.code'));
  check('THE BRIEF\'S OWN QUERY IS FLAGGED: a join across two mismatched databases',
    edLint.length === 1 && edLint[0].kind === 'join', JSON.stringify(edLint.map((c) => c.kind)));
  check('the warning appears above the results',
    await page.isVisible('#sqled-collation-banner')
    && /Latin1_General_CS_AS/.test(await page.textContent('#sqled-collation-banner')));
  check('and the editor text is never rewritten',
    await page.evaluate(() => document.getElementById('sql-editor').value.indexOf('COLLATE') === -1));
  const clean = await page.evaluate(() => window.sqlEdLintCollation('SELECT 1'));
  check('a query with nothing to say clears the banner rather than leaving the last one',
    clean.length === 0 && !(await page.isVisible('#sqled-collation-banner')));
  await shot('collation-b2-sqleditor.png');

  // ── 3b. Stage C: the offer, the preview, and Cancel ─────────────────────
  console.log('\n3b. Apply collation fix');
  const CROSS = 'SELECT * FROM SRC.dbo.Ledger a\nJOIN TGT.fin.ledger_entry b ON a.Code = b.code';
  await page.evaluate((sql) => {
    // The results pane starts collapsed to 36px, which leaves the banner in
    // the DOM but not clickable. Expanding it is what a real run does.
    if (typeof window.autoExpandResultsPane === 'function') window.autoExpandResultsPane();
    document.getElementById('sql-editor').value = sql;
    window.sqlEdLintCollation(sql);
  }, CROSS);
  await page.waitForTimeout(200);
  check('the banner offers to apply the fix',
    await page.isVisible('#sqled-collation-fix')
    && /not changed until you confirm/.test(await page.textContent('#sqled-collation-banner')));

  await page.click('#sqled-collation-fix');
  await page.waitForSelector('#sqled-collation-modal', { timeout: 5000 });
  const dlg = await page.textContent('#sqled-collation-modal');
  check('THE DIALOG SHOWS A BEFORE AND AN AFTER',
    /Before/.test(dlg) && /After/.test(dlg)
    && dlg.indexOf('a.Code = b.code') !== -1
    && dlg.indexOf('a.Code COLLATE SQL_Latin1_General_CP1_CI_AS = b.code') !== -1);
  check('and says nothing is written until Apply', /Nothing is written until you choose Apply/.test(dlg));

  await page.click('#sqled-collation-cancel');
  await page.waitForTimeout(150);
  check('CANCEL LEAVES THE SQL EXACTLY AS IT WAS',
    await page.evaluate(() => document.getElementById('sql-editor').value) === CROSS
    && (await page.$$('#sqled-collation-modal')).length === 0);

  await page.click('#sqled-collation-fix');
  await page.waitForSelector('#sqled-collation-modal', { timeout: 5000 });
  await page.click('#sqled-collation-apply');
  await page.waitForTimeout(250);
  const applied = await page.evaluate(() => document.getElementById('sql-editor').value);
  check('Apply rewrites the query, and only then',
    applied === 'SELECT * FROM SRC.dbo.Ledger a\nJOIN TGT.fin.ledger_entry b ON a.Code COLLATE SQL_Latin1_General_CP1_CI_AS = b.code',
    applied);
  check('the dialog closes', (await page.$$('#sqled-collation-modal')).length === 0);
  check('and the re-lint finds nothing left to warn about',
    !(await page.isVisible('#sqled-collation-banner')));

  // ── 4. No false warnings ────────────────────────────────────────────────
  console.log('\n4. Matching collations say nothing');
  const matched = matchedCollation(JSON.parse(JSON.stringify({
    version: 1,
    source: { dbCollation: 'Latin1_General_CS_AS', database: 'SRC', tempdbCollation: 'Latin1_General_CS_AS' },
    target: { dbCollation: 'SQL_Latin1_General_CP1_CI_AS', database: 'TGT', tempdbCollation: 'SQL_Latin1_General_CP1_CI_AS' },
    fingerprint: { source: 'src.example.internal|SRC', target: 'tgt.example.internal|TGT' },
    strategy: 'target', resolvedCollation: 'SQL_Latin1_General_CP1_CI_AS', tempTables: 'resolved',
    generatedSqlMode: 'apply', userSqlMode: 'offer_fix', caseRule: 'warn', codePageRule: 'warn',
    columnOverrides: {}, acknowledged: [],
    lastScan: { at: new Date().toISOString(), summary: { high: 0, medium: 0, low: 0 },
      columns: SRC_COLS.concat(TGT_COLS), findings: [] },
  })));
  await page.evaluate((m) => {
    const st = JSON.parse(localStorage.getItem('cygenix_profiles_v1'));
    st.profiles[0].collation = m;
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify(st));
  }, matched);

  const quiet = await page.evaluate(() => [
    window.sqlEdLintCollation('SELECT * FROM SRC.dbo.Ledger a JOIN TGT.fin.ledger_entry b ON a.Code = b.code').length,
    window.cygCollation.findClashes('SELECT a.Code FROM SRC.dbo.Ledger a UNION ALL SELECT b.code FROM TGT.fin.ledger_entry b').length,
    window.cygCollation.findClashes('CREATE TABLE #s (Code varchar(20))').length,
  ]);
  check('THE SAME JOIN, WITH BOTH DATABASES ON ONE COLLATION, IS NOT FLAGGED', quiet[0] === 0, JSON.stringify(quiet));
  check('nor a UNION across them', quiet[1] === 0);
  check('nor a temp table, because tempdb now agrees too', quiet[2] === 0);
  check('and the banner is hidden', !(await page.isVisible('#sqled-collation-banner')));

  await page.goto('http://localhost:' + PORT + '/object_mapping', { waitUntil: 'domcontentloaded' });
  await page.waitForFunction(() => !!window.cygCollation && typeof window.renderMappingTable === 'function', null, { timeout: 20000 });
  // addInitScript runs again on every navigation, so the seed has just put
  // the MISMATCHED profile back. Re-apply the matched one here, after the
  // page has loaded, or this section would test the same thing as section 1.
  await page.evaluate((m) => {
    const st = JSON.parse(localStorage.getItem('cygenix_profiles_v1'));
    st.profiles[0].collation = m;
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify(st));
  }, matched);
  await page.evaluate((cols) => {
    srcTable = { schema: 'dbo', name: 'Ledger', fullName: 'dbo.Ledger',
      columns: cols.src.map((c) => ({ name: c.column, type: 'VARCHAR' })) };
    tgtTable = { schema: 'fin', name: 'ledger_entry', fullName: 'fin.ledger_entry',
      columns: cols.tgt.map((c) => ({ name: c.column, type: 'VARCHAR' })) };
    columnMapping = [
      { tgtCol: 'account_no', srcCol: 'AcctNo', match: 'HIGH' },
      { tgtCol: 'memo', srcCol: 'Narrative', match: 'HIGH' },
      { tgtCol: 'code', srcCol: 'Code', match: 'HIGH' },
    ];
    window.renderMappingTable();
  }, { src: SRC_COLS, tgt: TGT_COLS });
  await page.waitForSelector('#mapping-tbody tr', { timeout: 8000, state: 'attached' });
  check('every badge on the mapping table is green',
    (await page.$$('#mapping-tbody .cyg-coll-badge.ok')).length === 3
    && (await page.$$('#mapping-tbody .cyg-coll-badge.fail')).length === 0);
  check('and the summary says so in a sentence rather than a count of problems',
    /share a collation/.test(await page.textContent('#om-collation-summary')));
  await shot('collation-b3-matched.png');

  // ── 5. Hygiene ──────────────────────────────────────────────────────────
  console.log('\n5. Hygiene');
  check('no credential reached either screen',
    (await page.content()).indexOf(SECRET) === -1);
  check('no uncaught errors on any of that', errors.length === 0, errors.slice(0, 3).join(' | '));
  check('nothing reached off-site', offSite.length === 0, offSite.slice(0, 3).join(' | '));
  check('LINTING SENT NO QUERY OF ITS OWN — it reads what the page already knows',
    calls.every((c) => c.action !== 'execute' || !/SERVERPROPERTY|sys\.columns/.test(String(c.sql || ''))));
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  check('no horizontal overflow at 1440', overflow <= 0, 'overflow ' + overflow);

  await browser.close();
  server.close();
  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
