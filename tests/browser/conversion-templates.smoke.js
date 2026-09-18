/* tests/browser/conversion-templates.smoke.js
 * ---------------------------------------------------------------------------
 * Conversion Templates, Phase 1, in a real browser.
 *
 * tests/conversion-templates.test.js proves the engine and the wiring. This
 * file walks the acceptance criteria the brief lists: the sidebar item and
 * clean address, the modules from the Configurator in its order, a table
 * added from the live picker and one typed by hand, STG_ defaults, publish
 * blocked until every module has a table, save-and-reload round-trip,
 * untick / re-tick keeping tables, and publish freezing a copy while the
 * draft moves to the next version. The data layer is stubbed at the network,
 * so what is under test is the page and the model, not Cosmos.
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/conversion-templates.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8411);
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

const U = 'you@example.test';

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  const token = 'x.' + Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: U })).toString('base64url') + '.y';

  // The stub Cosmos: one envelope per id, exactly what the Function App does.
  const store = new Map();
  const calls = [];
  let schemaCalls = 0;
  const colCalls = [];
  const execSql = [];          // every statement Create staging tables sends
  const made = new Set();      // the stub database's tables
  const json = (route, body, status) => route.fulfill({ status: status || 200, contentType: 'application/json', body: JSON.stringify(body) });
  await ctx.route('**', (route) => {
    const u = route.request().url();
    const q = new URL(u, 'http://x').searchParams;
    const action = q.get('action') || '';
    if (/action=whoami/.test(u)) return json(route, { tier: 'pro', tier_status: 'active', role: 'user' });
    if (/data-proxy/.test(u) && /^template-/.test(action)) {
      calls.push(action);
      const body = (() => { try { return JSON.parse(route.request().postData() || 'null'); } catch (e) { return null; } })();
      if (action === 'template-list') {
        return json(route, { templates: [...store.values()].filter((e) => e.projectId === q.get('projectId')).map((e) => Object.assign({}, e, { doc: undefined })) });
      }
      if (action === 'template-get') {
        const e = store.get(q.get('id'));
        return e ? json(route, { template: e.doc, kind: e.kind }) : json(route, { error: 'Template not found' }, 404);
      }
      if (action === 'template-save') {
        const d = body.template;
        store.set(d.id, { id: d.id, projectId: d.projectId, kind: 'draft', name: d.name, version: d.version, status: d.status, profileId: d.profileId, updatedAt: d.updatedAt, doc: d });
        return json(route, { saved: true, id: d.id, version: d.version });
      }
      if (action === 'template-publish') {
        const d = body.template;
        const id = 'pub_' + d.id + '_v' + d.version;
        store.set(id, { id, projectId: d.projectId, kind: 'published', name: d.name, version: d.version, status: 'published', profileId: d.profileId, updatedAt: d.updatedAt, publishedAt: d.publishedAt, doc: d });
        return json(route, { published: true, id, templateId: d.id, version: d.version });
      }
      if (action === 'template-delete') { const had = store.delete(body.id); return json(route, { deleted: had }); }
    }
    // The target's schema, as db-connect answers it: three tables, no keys.
    if (/db-connect/.test(u)) {
      const body = (() => { try { return JSON.parse(route.request().postData() || '{}'); } catch (e) { return {}; } })();
      if (body.action === 'schema-tables') schemaCalls++;
      if (body.action === 'schema-tables') return json(route, { database: 'ELITE3E', tables: [
        { schema: 'dbo', name: 'VchrDetail', kind: 'table' }, { schema: 'dbo', name: 'Vchr', kind: 'table' },
        { schema: 'dbo', name: 'Matter', kind: 'table' }, { schema: 'dbo', name: 'MattDate', kind: 'table' }] });
      if (body.action === 'schema-fks') return json(route, { foreignKeys: [] });
      /* The staging side. `execute` is the only route Create staging tables
         uses — the catalog read and then one CREATE per table. The stub
         behaves like a database: a table it has already created is there the
         next time it is asked, which is what makes the "run it twice" check
         mean something. */
      if (body.action === 'execute') {
        const sql = String(body.sql || '');
        execSql.push(sql);
        if (/sys\.tables/.test(sql)) return json(route, { recordset: [...made].map((n) => ({ name: n })) });
        const m = sql.match(/CREATE TABLE \[dbo\]\.\[([^\]]+)\]/);
        if (m) {
          if (made.has(m[1])) return json(route, { error: "There is already an object named '" + m[1] + "' in the database." }, 400);
          made.add(m[1]);
          return json(route, { success: true, rowsAffected: 0, recordset: [] });
        }
        return json(route, { success: true, recordset: [] });
      }
      if (body.action === 'schema-columns') {
        colCalls.push(body.tableName);
        return json(route, { table: { schema: 'dbo', name: body.tableName, primaryKeys: ['Id'], columns: [
          { name: 'Id', ordinal: 1, baseType: 'int', type: 'INT', nullable: false, isIdentity: true },
          { name: 'Descr', ordinal: 2, baseType: 'nvarchar', maxLength: 128, nullable: true },
          { name: 'Body', ordinal: 3, baseType: 'nvarchar', maxLength: -1, nullable: true },
          { name: 'Amount', ordinal: 4, baseType: 'decimal', precision: 18, scale: 2, nullable: false },
          { name: 'Calc', ordinal: 5, baseType: 'decimal', precision: 18, scale: 2, nullable: true, isComputed: true } ] } });
      }
      return json(route, {});
    }
    if (/data-proxy|netlify\/functions|\/api\//.test(u)) return json(route, {});
    if (u.startsWith('http://localhost:' + PORT)) return route.continue();
    return route.abort();
  });

  await ctx.addInitScript((arg) => {
    if (localStorage.getItem('cygenix_profiles_v1')) return;
    const acct = { homeAccountId: 'h.t', environment: 'cygenix.ciamlogin.com', tenantId: 't', username: arg.U, localAccountId: 'l', authorityType: 'MSSTS', name: 'You' };
    [localStorage, sessionStorage].forEach((s) => { s.setItem('cygenix_token', arg.token); s.setItem('cygenix_expires', String(Date.now() + 3600e3)); });
    localStorage.setItem('cygenix_onboarded', 'true');
    localStorage.setItem('cygenix_user', JSON.stringify({ email: arg.U }));
    localStorage.setItem('cygenix_active_user', arg.U);
    localStorage.setItem('cygenix_tier', 'pro');
    localStorage.setItem('cygenix_cookie_consent', JSON.stringify({ version: '1', essential: true, functional: true, timestamp: new Date().toISOString() }));
    localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify(acct));
    localStorage.setItem('h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-',
      JSON.stringify({ credentialType: 'IdToken', secret: arg.token, expiresOn: String(Math.floor(Date.now() / 1000) + 3600) }));
    localStorage.setItem('cygenix_projects', JSON.stringify([{ id: 'p1', name: 'Acme conversion' }]));
    localStorage.setItem('cygenix_active_project_id', 'p1');
    // A live target (direct), so the schema reader goes through db-connect.
    const live = {}; live[arg.U] = { srcConnString: 'mssql://u:p@src/legacy', srcConnMode: 'direct', tgtConnString: 'mssql://u:p@tgt/ELITE3E', tgtConnMode: 'direct' };
    localStorage.setItem('cygenix_project_connections', JSON.stringify(live));
    const blob = {}; blob[arg.U] = [{ id: 'c_src', side: 'src', mode: 'direct', name: 'Legacy' }, { id: 'c_tgt', side: 'tgt', mode: 'direct', name: 'Elite 3E UAT' }];
    localStorage.setItem('cygenix_saved_connections', JSON.stringify(blob));
    localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify({ c_src: { connString: 'mssql://u:p@src/legacy' }, c_tgt: { connString: 'mssql://u:p@tgt/ELITE3E' } }));
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify({ v: 1, connMeta: { c_src: { envClass: 'UAT' }, c_tgt: { envClass: 'UAT' } }, bindings: [], runRecords: [], events: [],
      profiles: [{ id: 'FIN_3E_UAT', name: 'Finance UAT', envClass: 'UAT', status: 'active', srcConnId: 'c_src', tgtConnId: 'c_tgt', createdAt: 1, updatedAt: 1 }],
      settings: { envClasses: ['DEV', 'TEST', 'UAT', 'PRD', 'SANDBOX'], activeProfileId: 'FIN_3E_UAT', selectedAt: 1 } }));
    // The Configurator's estimate: AP, Matters and WIP ticked, in that order.
    localStorage.setItem('cygenix_effort_estimates_v1', JSON.stringify({ active: 'Acme 3E', estimates: { 'Acme 3E': {
      v: 1, name: 'Acme 3E', modules: ['Addresses', 'AP', 'AP Master', 'Matters', 'WIP', 'Timekeepers'],
      ticks: { 'analysis|AP': 1, 'scripts|Matters': 1, 'uat|WIP': 1 }, meta: {}, variables: [], rates: {}, tc: {}, data: {} } } }));
  }, { U, token });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  const answers = [];
  page.on('dialog', async (d) => {
    if (d.type() === 'prompt') return d.accept(answers.length ? answers.shift() : 'Finance modules');
    return d.accept();
  });
  const open = async () => {
    await page.goto('http://localhost:' + PORT + '/conversion-templates', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => !!window.CygenixTemplateModel && typeof CT !== 'undefined' && !!CT.tpl, null, { timeout: 20000 });
    await page.waitForTimeout(600);
  };
  const mods = () => page.evaluate(() => Array.from(document.querySelectorAll('#ct-mods li')).map((li) => ({ text: li.textContent.trim(), out: li.classList.contains('out'), sep: li.classList.contains('ct-sep') })));
  const rows = () => page.evaluate(() => Array.from(document.querySelectorAll('#ct-tables tbody tr[data-id]')).map((tr) => ({ target: tr.querySelectorAll('input')[0].value, staging: tr.querySelectorAll('input')[1].value })));
  const noteText = () => page.evaluate(() => document.getElementById('ct-note').textContent);

  console.log('Conversion Templates — Phase 1\n');

  /* ── 1. Sidebar, address, shell ─────────────────────────────────────────── */
  await open();
  const shell = await page.evaluate(() => {
    const items = Array.from(document.querySelectorAll('.cyg-nav-item[data-key]')).map((n) => n.getAttribute('data-key'));
    return { items, active: (document.querySelector('.cyg-nav-item.active') || {}).getAttribute ? document.querySelector('.cyg-nav-item.active').getAttribute('data-key') : null,
      hairline: !!document.getElementById('cyg-envbar'), assistant: !!document.getElementById('cygaLaunch'), title: document.title };
  });
  check('the sidebar shows Conversion Templates directly above Object Mapping',
    shell.items.indexOf('conversion-templates') >= 0 && shell.items.indexOf('object-mapping') === shell.items.indexOf('conversion-templates') + 1, shell.items.join(','));
  check('the page opens at its clean address with the sidebar, the status hairline and the Ask Cygenix launcher',
    /Conversion Templates/.test(shell.title) && shell.hairline && shell.assistant && page.url().endsWith('/conversion-templates'));

  /* ── 2. Scope from the Configurator, in its order ───────────────────────── */
  let m = await mods();
  check('the modules are exactly the ticked ones, in the estimate\'s order',
    JSON.stringify(m.filter((x) => !x.sep).map((x) => x.text.replace(/\d+$/, '').trim())) === JSON.stringify(['AP', 'Matters', 'WIP']), JSON.stringify(m));
  const band = await page.evaluate(() => ({ profile: document.getElementById('ct-profile').textContent, estimate: document.getElementById('ct-estimate').textContent,
    scopeMode: document.getElementById('ct-scopemode').value, prefix: document.getElementById('ct-prefix').value }));
  check('the header names the profile and the estimate, defaults the scope rule to "any" and the prefix to STG_',
    /FIN_3E_UAT/.test(band.profile) && /Acme 3E/.test(band.estimate) && band.scopeMode === 'any' && band.prefix === 'STG_', JSON.stringify(band));

  /* ── 3. Add a table from the live picker, and one typed ────────────────── */
  await page.evaluate(() => ctSelect('AP'));
  await page.click('#ct-add');
  await page.waitForFunction(() => /tables/.test(document.getElementById('ct-pick-status').textContent), null, { timeout: 15000 });
  await page.fill('#ct-pick-q', 'vchr');
  await page.waitForTimeout(150);
  const pick = await page.evaluate(() => Array.from(document.querySelectorAll('#ct-pick-list li')).map((li) => li.textContent.trim()));
  check('the picker reads the target database and filters as you type',
    pick.length === 2 && pick.some((t) => /VchrDetail/.test(t)) && pick.some((t) => /^dbo\.Vchrtable$/.test(t)), pick.join(' | '));
  await page.click('#ct-pick-list li:has-text("VchrDetail")');
  await page.fill('#ct-pick-manual', 'MyCustomTable');
  await page.click('button:has-text("Add typed name")');
  await page.click('button:has-text("Done")');
  let r = await rows();
  check('the picked table and the typed one are both rows, with STG_ staging names derived',
    r.length === 2 && r[0].target === 'VchrDetail' && r[0].staging === 'STG_VchrDetail' && r[1].target === 'MyCustomTable' && r[1].staging === 'STG_MyCustomTable', JSON.stringify(r));
  await page.evaluate(() => { const tr = document.querySelector('#ct-tables tbody tr[data-id]'); const inp = tr.querySelectorAll('input')[1]; inp.value = 'STG_VOUCHER_LINES'; inp.dispatchEvent(new Event('change')); });
  r = await rows();
  check('a staging name can be overridden', r[0].staging === 'STG_VOUCHER_LINES');

  /* ── 4. Publish is blocked until every module has a table ──────────────── */
  const gate1 = await page.evaluate(() => ({ disabled: document.getElementById('ct-publish').disabled, issues: document.getElementById('ct-issues').textContent }));
  check('Publish is disabled while Matters and WIP have no tables, and the reason is in plain language',
    gate1.disabled && /Matters/.test(gate1.issues) && /No target tables chosen/.test(gate1.issues), gate1.issues.slice(0, 200));

  /* ── 5. Save, reload, identical ────────────────────────────────────────── */
  await page.click('#ct-save');
  await page.waitForFunction(() => /Saved/.test(document.getElementById('ct-note').textContent), null, { timeout: 10000 });
  const before = await page.evaluate(() => JSON.stringify(CT.tpl));
  check('Save posts the whole document once and says what it stored', calls.filter((c) => c === 'template-save').length === 1 && /2 tables/.test(await noteText()));
  await open();
  const after = await page.evaluate(() => JSON.stringify(CT.tpl));
  // The first in-scope module is selected on load, so AP's two rows show.
  check('after a reload the template comes back identical, timestamp included', before === after && (await rows()).length === 2 && (await mods()).length === 3,
    before === after ? '' : 'differs');

  /* ── 6. Untick / re-tick in the Configurator, tables kept ──────────────── */
  await page.evaluate(() => { const s = JSON.parse(localStorage.getItem('cygenix_effort_estimates_v1')); delete s.estimates['Acme 3E'].ticks['analysis|AP']; localStorage.setItem('cygenix_effort_estimates_v1', JSON.stringify(s)); });
  await page.click('#ct-refresh');
  await page.waitForTimeout(200);
  m = await mods();
  check('unticking AP moves it under "no longer in scope", greyed, with its tables kept, and says so',
    m.some((x) => x.sep && /No longer in scope/i.test(x.text)) && m.some((x) => x.out && /^AP/.test(x.text) && /2$/.test(x.text))
    && /no longer in scope: AP \(tables kept\)/.test(await noteText()), JSON.stringify(m));
  await page.evaluate(() => { const s = JSON.parse(localStorage.getItem('cygenix_effort_estimates_v1')); s.estimates['Acme 3E'].ticks['analysis|AP'] = 1; localStorage.setItem('cygenix_effort_estimates_v1', JSON.stringify(s)); });
  await page.click('#ct-refresh');
  await page.waitForTimeout(200);
  m = await mods();
  check('re-ticking brings AP back, in the estimate\'s order, tables intact',
    !m.some((x) => x.out) && JSON.stringify(m.map((x) => x.text.replace(/\d+$/, '').trim())) === JSON.stringify(['AP', 'Matters', 'WIP']) && /back in scope: AP/.test(await noteText()), JSON.stringify(m));

  /* ── 7. Publish freezes a copy; the draft becomes v2 ───────────────────── */
  await page.evaluate(() => { ctSelect('Matters'); ctAddTable('Matter'); ctSelect('WIP'); ctAddTable('TimeCard'); });
  check('with every module covered, Publish is enabled', !(await page.evaluate(() => document.getElementById('ct-publish').disabled)));
  calls.length = 0;
  await page.click('#ct-publish');
  await page.waitForFunction(() => /Published version 1/.test(document.getElementById('ct-note').textContent), null, { timeout: 20000 });
  const pub = await page.evaluate(() => ({ version: CT.tpl.version, status: CT.tpl.status, badges: document.getElementById('ct-badges').textContent }));
  const stored = [...store.values()];
  check('publish stored the frozen v1 and the draft is now v2, draft',
    pub.version === 2 && pub.status === 'draft' && /v2/.test(pub.badges) && /Draft/.test(pub.badges)
    && stored.some((e) => e.kind === 'published' && e.version === 1 && e.doc.status === 'published' && e.doc.publishedAt)
    && stored.some((e) => e.kind === 'draft' && e.version === 2) && !stored.some((e) => e.kind === 'draft' && e.version === 1), JSON.stringify(pub) + ' ' + JSON.stringify(stored.map((e) => e.kind + e.version)));
  check('the sequence was save, publish, save, delete — single documents, no burst',
    JSON.stringify(calls.filter((c) => c !== 'template-list' && c !== 'template-get')) === JSON.stringify(['template-save', 'template-publish', 'template-save', 'template-delete']), calls.join(','));

  /* ── 8. Load shows both versions; a published one is read-only ─────────── */
  await page.click('#ct-load');
  await page.waitForSelector('#ct-load-modal.open');
  const listed = await page.evaluate(() => Array.from(document.querySelectorAll('#ct-load-list li')).map((li) => li.textContent.replace(/\s+/g, ' ').trim()));
  check('Load lists the v1 published copy and the v2 draft', listed.some((t) => /published/.test(t) && /v1/.test(t)) && listed.some((t) => /draft/.test(t) && /v2/.test(t)), listed.join(' | '));
  await page.click('#ct-load-list li:has-text("published")');
  await page.waitForFunction(() => CT.tpl && CT.tpl.status === 'published', null, { timeout: 10000 });
  const ro = await page.evaluate(() => ({ nameDisabled: document.getElementById('ct-name').disabled, saveDisabled: document.getElementById('ct-save').disabled, addDisabled: document.getElementById('ct-add').disabled }));
  check('a published version opens frozen: no editing, no save, no add', ro.nameDisabled && ro.saveDisabled && ro.addDisabled, JSON.stringify(ro));


  /* ── 9. Import / export of the mapping ─────────────────────────────────── */
  const os = require('os');
  const tmpFile = (name, text) => { const f = path.join(os.tmpdir(), 'cyg-' + Date.now() + '-' + name); fs.writeFileSync(f, text); return f; };
  await page.click('#ct-load');
  await page.waitForSelector('#ct-load-modal.open');
  await page.click('#ct-load-list li:has-text("draft")');
  await page.waitForFunction(() => CT.tpl && CT.tpl.status === 'draft', null, { timeout: 10000 });
  await page.waitForTimeout(300);
  const exp = await page.evaluate(() => ({ rows: exportRowsNow(), csv: CygenixTemplateIO.toCsv(exportRowsNow()), name: exportFileName('csv') }));
  const tableTotal = await page.evaluate(() => CT.tpl.modules.filter((m) => m.inScope !== false).reduce((n, m) => n + m.tables.length, 0));
  const emptyMods = await page.evaluate(() => CT.tpl.modules.filter((m) => m.inScope !== false && !m.tables.length).length);
  check('export: header plus one row per table plus one blank row per empty module',
    exp.rows[0].join('|') === 'Module|Target Table|Staging Table|Required|Load Order|Notes' && exp.rows.length - 1 === tableTotal + emptyMods, exp.rows.length + ' rows for ' + tableTotal + '+' + emptyMods);
  check('the CSV carries a BOM and the file name follows the pattern', exp.csv.charCodeAt(0) === 0xFEFF && /^cygenix-template-Conversion-Template-FIN-3E-UAT-\d{8}\.csv$/.test(exp.name), exp.name);

  // Re-import the exact export: nothing to add, nothing changed.
  const beforeImport = await page.evaluate(() => JSON.stringify(CT.tpl));
  await page.evaluate(() => { for (let i = localStorage.length - 1; i >= 0; i--) { const k = localStorage.key(i); if (k && k.indexOf('cygenix_schema_') === 0) localStorage.removeItem(k); } });
  schemaCalls = 0;
  await page.setInputFiles('#ct-import-file', tmpFile('export.csv', exp.csv));
  await page.waitForSelector('#ct-import-modal.open', { timeout: 15000 });
  const tiles = () => page.evaluate(() => Array.from(document.querySelectorAll('#ct-import-counts .ct-count')).reduce((o, el) => { o[el.querySelector('.k').textContent] = Number(el.querySelector('.v').textContent); return o; }, {}));
  let t9 = await tiles();
  check('re-importing the export previews 0 rows to add, the rest already present or blank',
    t9['rows to add'] === 0 && t9['already present'] === tableTotal && t9['blank rows skipped'] === emptyMods && (await page.evaluate(() => document.getElementById('ct-import-go').disabled)), JSON.stringify(t9));
  check('the import read the target schema exactly once', schemaCalls === 1, 'calls=' + schemaCalls);
  await page.click('#ct-import-modal button:has-text("Cancel")');
  check('Cancel leaves the template byte-for-byte unchanged', (await page.evaluate(() => JSON.stringify(CT.tpl))) === beforeImport && /nothing was changed/.test(await noteText()));

  // A two-column SSMS-style file with a NULL, an unknown table and an out-of-scope module.
  await page.setInputFiles('#ct-import-file', tmpFile('two.csv', 'entity,production_table\r\nAP,MattDate\r\nAP,NULL\r\nWIP,Ghost\r\nTrust,Matter\r\n'));
  await page.waitForSelector('#ct-import-modal.open', { timeout: 15000 });
  t9 = await tiles();
  check('a two-column file previews: 3 to add, 1 blank (NULL) skipped, 1 unknown table, 1 out of scope',
    t9['rows to add'] === 3 && t9['blank rows skipped'] === 1 && t9['flagged — unknown target table'] === 1 && t9['flagged — module out of scope'] === 1, JSON.stringify(t9));
  check('and no second schema read for the second import', schemaCalls === 1, 'calls=' + schemaCalls);
  await page.click('#ct-import-go');
  await page.waitForTimeout(300);
  const after9 = await page.evaluate(() => {
    ctSelect('WIP');
    const row = Array.from(document.querySelectorAll('#ct-tables tbody tr')).find((tr) => /Ghost/.test(tr.querySelector('input').value));
    return { note: document.getElementById('ct-note').textContent, unsaved: /unsaved/.test(document.getElementById('ct-badges').textContent),
      flaggedRow: !!row && row.classList.contains('flagged') && /Unknown target table/.test(row.textContent),
      mods: Array.from(document.querySelectorAll('#ct-mods li')).map((li) => li.textContent.replace(/\s+/g, ' ').trim()),
      issues: document.getElementById('ct-issues').textContent, publishDisabled: document.getElementById('ct-publish').disabled };
  });
  check('after Import the template is unsaved and the summary says what happened', after9.unsaved && /3 rows added/.test(after9.note) && /Press Save/.test(after9.note), after9.note);
  check('the unknown table is flagged in the grid', after9.flaggedRow);
  check('the out-of-scope module shows its badge', after9.mods.some((t) => /^Trust/.test(t) && /out of scope/.test(t)), after9.mods.join(' | '));
  check('both flags are errors in Ready to publish, and Publish is disabled',
    /Unknown target table/.test(after9.issues) && /Module out of scope/.test(after9.issues) && after9.publishDisabled, after9.issues.slice(0, 200));
  check('nothing was saved by the import', !calls.slice(calls.lastIndexOf('template-get') + 1).includes('template-save'));


  // The file people actually have: two columns, no header, straight from a
  // database tool. Read as Module, Target Table; the preview says so.
  await page.setInputFiles('#ct-import-file', tmpFile('raw.csv', 'AP,VchrDetail\r\nAR,NULL\r\nAR,ChrgCard\r\n'));
  await page.waitForSelector('#ct-import-modal.open', { timeout: 15000 });
  t9 = await tiles();
  const rawWarn = await page.evaluate(() => document.getElementById('ct-import-warn').textContent);
  check('a headerless two-column file is read as Module, Target Table and the preview says so',
    /No header row found/.test(rawWarn) && t9['rows to add'] === 1 && t9['already present'] === 1 && t9['blank rows skipped'] === 1, JSON.stringify(t9) + ' ' + rawWarn);
  await page.click('#ct-import-modal button:has-text("Cancel")');

  // Excel export with the CDN blocked: a clear error, CSV offered.
  await page.evaluate(() => ctExport('xlsx'));
  await page.waitForFunction(() => /CSV/.test(document.getElementById('ct-note').textContent), null, { timeout: 10000 });
  check('when the sheet library cannot load, the error says so and offers CSV', (await page.evaluate(() => CT.xlsx)) === 'failed' && /could not be loaded/.test(await noteText()));


  /* ── 10. Phase 2: columns, the specification workbook and the DDL ──────── */
  await page.click('#ct-load');
  await page.waitForSelector('#ct-load-modal.open');
  await page.click('#ct-load-list li:has-text("draft")');
  await page.waitForFunction(() => CT.tpl && CT.tpl.status === 'draft', null, { timeout: 10000 });
  await page.waitForTimeout(300);

  // A schema-1 draft, exactly as the live one is, must open unchanged.
  const v1ok = await page.evaluate(() => {
    const before = JSON.parse(JSON.stringify(CT.tpl));
    before.schema = 1;
    before.modules.forEach((m) => m.tables.forEach((t) => { delete t.columns; }));
    const after = CygenixTemplateModel.tmMigrate(JSON.parse(JSON.stringify(before)));
    return after.schema === CygenixTemplateModel.TM_SCHEMA_VERSION
      && after.modules.every((m) => m.tables.every((t) => Array.isArray(t.columns)))
      && after.modules.every((m) => m.excluded === false && m.mapped === false)
      && CygenixTemplateModel.tmSummary(after).tableCount === CygenixTemplateModel.tmSummary(CT.tpl).tableCount;
  });
  check('a schema-1 draft forward-migrates and still summarises the same', v1ok);

  colCalls.length = 0;
  await page.click('#ct-refresh-cols');
  await page.waitForFunction(() => /Columns read for/.test(document.getElementById('ct-note').textContent), null, { timeout: 20000 });
  const cols = await page.evaluate(() => {
    const cov = CygenixTemplateModel.tmColumnCoverage(CT.tpl);
    const stored = JSON.parse(localStorage.getItem('cygenix_template_draft_v1::p1') || 'null');
    const oos = CT.tpl.modules.filter((m) => m.inScope === false);
    return { note: document.getElementById('ct-note').textContent, cov,
      storedHasColumns: !!stored && stored.modules.some((m) => m.tables.some((t) => (t.columns || []).length)),
      stamped: CT.tpl.modules.some((m) => m.tables.some((t) => !!t.columnsFetchedAt)),
      oosUntouched: oos.every((m) => m.tables.every((t) => !(t.columns || []).length)),
      header: document.getElementById('ct-badges').textContent };
  });
  check('Refresh columns reads the in-scope tables and says how many',
    /Columns read for \d+ of \d+/.test(cols.note) && cols.cov.tablesWithColumns > 0, cols.note);
  check('the snapshot is written to the template and stamped, and the local mirror has it',
    cols.stamped && cols.storedHasColumns);
  check('out-of-scope modules are untouched', cols.oosUntouched);
  check('the header line shows column coverage', /columns read for/.test(cols.header), cols.header);
  check('one request per in-scope table, no repeats', colCalls.length === new Set(colCalls).size, colCalls.join(','));

  // The guards: a second click inside three seconds does nothing.
  const before2 = colCalls.length;
  await page.click('#ct-refresh-cols');
  await page.waitForTimeout(600);
  check('a second Refresh columns inside three seconds is refused, with no extra requests',
    colCalls.length === before2 && /wait a moment/i.test(await noteText()), await noteText());

  // The specification workbook, built in the page from the real snapshot.
  const spec = await page.evaluate(() => {
    const S = window.CygenixTemplateSpec;
    const rows = S.buildSpecRows(CT.tpl, {});
    const fake = { utils: { aoa_to_sheet: (a) => ({ aoa: a }), book_new: () => ({ SheetNames: [], Sheets: {} }),
      book_append_sheet: (wb, ws, n) => { wb.SheetNames.push(n); wb.Sheets[n] = ws; } } };
    const wb = S.buildSpecWorkbook(fake, CT.tpl, {});
    const colSheet = wb.Sheets['Columns'].aoa;
    const firstPop = wb.SheetNames[4];
    return { sheets: wb.SheetNames.length, fixed: wb.SheetNames.slice(0, 4),
      allShort: wb.SheetNames.every((n) => n.length <= 31),
      unique: new Set(wb.SheetNames.map((n) => n.toLowerCase())).size === wb.SheetNames.length,
      columnRows: colSheet.length - 1, tablesSum: rows.tables.reduce((n, t) => n + t.columnCount, 0),
      hasMax: colSheet.some((r) => r[5] === 'nvarchar(max)'), hasDec: colSheet.some((r) => r[5] === 'decimal(18,2)'),
      identityVerdict: (colSheet.find((r) => r[4] === 'Id') || [])[14],
      computedVerdict: (colSheet.find((r) => r[4] === 'Calc') || [])[14],
      popHead: wb.Sheets[firstPop].aoa[0], popHint: wb.Sheets[firstPop].aoa[1],
      fileName: S.specFileName(CT.tpl, 'xlsx') };
  });
  check('the workbook has the four fixed sheets plus one per table, all names legal and unique',
    spec.fixed.join('|') === 'Read Me|Tables|Columns|Load Order' && spec.allShort && spec.unique && spec.sheets > 4, JSON.stringify(spec.fixed));
  check('the Columns row count equals the sum of the per-table counts in Tables',
    spec.columnRows === spec.tablesSum && spec.columnRows > 0, spec.columnRows + ' vs ' + spec.tablesSum);
  check('nvarchar(max) and decimal(18,2) render correctly', spec.hasMax && spec.hasDec);
  check('the identity column says do-not-populate and the computed one too',
    spec.identityVerdict === 'No — identity' && spec.computedVerdict === 'No — computed', spec.identityVerdict + ' / ' + spec.computedVerdict);
  check('the populate sheet omits the computed column and marks the identity one',
    !spec.popHead.some((h) => /Calc/.test(h)) && /Id \(do not populate\)/.test(spec.popHead[0]) && /identity/.test(spec.popHint[0]), spec.popHead.join(' | '));
  check('the file name carries the profile, the template and the version',
    /^FIN_3E_UAT-.*-v\d+-spec\.xlsx$/.test(spec.fileName), spec.fileName);

  // The DDL.
  const ddl = await page.evaluate(() => window.CygenixTemplateSpec.buildStagingDdl(CT.tpl, {}));
  const stmts = ddl.replace(/\/\*[\s\S]*?\*\//g, '');
  check('the DDL creates every table idempotently, every column nullable, no identity, no computed',
    /IF OBJECT_ID\(N'dbo\./.test(stmts) && !/NOT NULL/.test(stmts) && !/IDENTITY/i.test(stmts) && !/Calc/.test(stmts)
    && /\[Body\] nvarchar\(max\) NULL/.test(stmts) && /\[Amount\] decimal\(18,2\) NULL/.test(stmts), stmts.slice(0, 200));

  // Published versions are frozen: change the draft, the published spec is unmoved.
  const pubBefore = await page.evaluate(() => {
    const S = window.CygenixTemplateSpec;
    const r = S.buildSpecRows(CT.tpl, {});
    return r.tables.map((t) => t.stagingTable).join(',');
  });
  await page.evaluate(() => { const m = CT.tpl.modules.find((x) => x.inScope !== false && x.tables.length);
    CygenixTemplateModel.tmUpdateTable(CT.tpl, m.module, m.tables[0].id, { stagingTable: 'STG_RENAMED' }, 'me'); });
  const pubAfter = await page.evaluate(() => window.CygenixTemplateSpec.buildSpecRows(CT.tpl, {}).tables.map((t) => t.stagingTable).join(','));
  check('renaming a staging table changes the draft\'s spec — proving the spec follows the document it is given',
    pubAfter !== pubBefore && /STG_RENAMED/.test(pubAfter));

  /* ── 9. Include in publishing (Sep-2026) ──────────────────────────────── */
  // Put the renamed table back first, so what follows is about the Include
  // tick and nothing else.
  await page.evaluate(() => { const m = CT.tpl.modules.find((x) => x.tables.some((t) => t.stagingTable === 'STG_RENAMED'));
    const t = m.tables.find((x) => x.stagingTable === 'STG_RENAMED');
    CygenixTemplateModel.tmUpdateTable(CT.tpl, m.module, t.id, { stagingTable: 'STG_' + t.targetTable }, 'me'); render(); });

  const exModule = await page.evaluate(() => CT.tpl.modules.find((m) => m.inScope !== false && m.tables.length).module);
  const specBefore = await page.evaluate(() => window.CygenixTemplateSpec.buildSpecRows(CT.tpl, {}).tables.length);
  const readyBefore = await page.evaluate(() => canPublishNow());
  // Both columns read the same way round, but they START differently and must:
  // Map is off until somebody asks for it, Include is on until somebody says
  // otherwise. A template that opened with nothing included would be one that
  // publishes nothing until every box is ticked.
  const startTicked = await page.evaluate(() => Array.from(document.querySelectorAll('#ct-mods li:not(.out):not(.ct-sep)'))
    .filter((li) => li.querySelectorAll('.tk input').length === 2)
    .every((li) => li.querySelectorAll('.tk input')[1].checked && !li.querySelectorAll('.tk input')[0].checked));
  await page.evaluate((mod) => {
    const li = Array.from(document.querySelectorAll('#ct-mods li')).find((x) => x.textContent.trim().indexOf(mod) === 0);
    li.querySelectorAll('.tk input')[1].click();
  }, exModule);
  await page.waitForTimeout(250);
  const ex = await page.evaluate((mod) => {
    const li = Array.from(document.querySelectorAll('#ct-mods li')).find((x) => x.textContent.trim().indexOf(mod) === 0);
    return {
      greyed: li.classList.contains('ex'),
      label: /not included/.test(li.textContent),
      ticked: li.querySelectorAll('.tk input')[1].checked,
      sub: document.getElementById('ct-mods-sub').textContent,
      badges: document.getElementById('ct-badges').textContent,
      specTables: window.CygenixTemplateSpec.buildSpecRows(CT.tpl, {}).tables.length,
      specHasModule: window.CygenixTemplateSpec.buildSpecRows(CT.tpl, {}).tables.some((t) => t.module === mod),
      ddlHasModule: new RegExp('\\* ' + mod + ' ·').test(window.CygenixTemplateSpec.buildStagingDdl(CT.tpl, {})),
      stagingHasModule: window.CygenixTemplateStaging.stagingPlan(CT.tpl, {}).tables.some((t) => t.module === mod),
      ready: canPublishNow(),
      issues: document.getElementById('ct-issues').textContent,
      tablesKept: CygenixTemplateModel.tmFindModule(CT.tpl, mod).tables.length,
    };
  }, exModule);
  check('Include starts TICKED on every module in scope — nothing is left out by default',
    startTicked === true, String(startTicked));
  check('un-ticking Include greys the module and labels it, and the box stays clear',
    ex.greyed && ex.label && ex.ticked === false, JSON.stringify({ g: ex.greyed, l: ex.label, t: ex.ticked }));
  check('the summary lines count it — "N in scope · 1 not included" and "… · 1 not included in the publish"',
    /1 not included/.test(ex.sub) && /1 not included in the publish/.test(ex.badges), ex.sub + ' || ' + ex.badges);
  check('it keeps its tables', ex.tablesKept > 0);
  check('the workbook, the staging DDL and Create staging tables all skip it',
    ex.specTables < specBefore && !ex.specHasModule && !ex.ddlHasModule && !ex.stagingHasModule,
    JSON.stringify({ before: specBefore, after: ex.specTables }));
  check('the readiness check ignores it, and says so as a warning rather than a problem',
    readyBefore === true && ex.ready === true && /not included in the publish/.test(ex.issues));
  // Back in.
  await page.evaluate((mod) => {
    const li = Array.from(document.querySelectorAll('#ct-mods li')).find((x) => x.textContent.trim().indexOf(mod) === 0);
    li.querySelectorAll('.tk input')[1].click();
  }, exModule);
  await page.waitForTimeout(200);
  check('ticking Include again puts it straight back into the publish',
    await page.evaluate(() => window.CygenixTemplateSpec.buildSpecRows(CT.tpl, {}).tables.length) === specBefore);

  /* ── 10. Object Mapping ────────────────────────────────────────────────── */
  await page.evaluate(() => localStorage.setItem('cygenix_jobs', JSON.stringify([
    { id: 'hand', name: 'my careful mapping', jobType: 'simple-map', projectId: 'p1',
      sourceTable: 'dbo.STG_Vchr', targetTable: 'dbo.Vchr', columnMapping: [{ srcCol: 'a', tgtCol: 'b' }], status: 'ready' },
  ])));
  await page.evaluate((mod) => {
    const li = Array.from(document.querySelectorAll('#ct-mods li')).find((x) => x.textContent.trim().indexOf(mod) === 0);
    li.querySelectorAll('.tk input')[0].click();
  }, exModule);
  await page.waitForTimeout(400);
  const sent = await page.evaluate((mod) => {
    const jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]');
    const mine = jobs.filter((j) => j.fromTemplate && j.fromTemplate.templateId === CT.tpl.id);
    return { total: jobs.length, mine: mine.length,
      drafts: mine.every((j) => j.status === 'draft' && !j.columnMapping.length),
      tagged: mine.every((j) => j.fromTemplate.module === mod),
      pairs: mine.map((j) => j.sourceTable + '→' + j.targetTable).sort(),
      handKept: jobs.some((j) => j.id === 'hand' && j.columnMapping.length === 1),
      profiled: mine.every((j) => !!j.profileId || !!j.connectionProfileId),
      heading: document.getElementById('ct-mods-sent').textContent,
      ticked: CygenixTemplateModel.tmFindModule(CT.tpl, mod).mapped,
      note: document.getElementById('ct-note').textContent };
  }, exModule);
  check('ticking Map creates one draft mapping per table, tagged with the template and module',
    sent.mine > 0 && sent.drafts && sent.tagged, JSON.stringify(sent.pairs));
  check('the source is the staging table and the target is the target table',
    sent.pairs.every((p) => /^dbo\.STG_.*→dbo\./.test(p)), sent.pairs.join(' | '));
  check('a pair already mapped by hand is not duplicated',
    sent.pairs.every((p) => p !== 'dbo.STG_Vchr→dbo.Vchr') || sent.mine === 0, sent.pairs.join(' | '));
  check('the heading counts what is actually in Object Mapping',
    new RegExp(sent.mine + ' sent to Object Mapping').test(sent.heading), sent.heading);
  check('each created job is stamped with the active connection profile', sent.profiled);

  // Ticking again must add nothing.
  const twice = await page.evaluate(async (mod) => { await ctToggleMap(mod, true); return JSON.parse(localStorage.getItem('cygenix_jobs') || '[]').length; }, exModule);
  check('sending the same module again adds nothing', twice === sent.total);

  await page.evaluate((mod) => {
    const li = Array.from(document.querySelectorAll('#ct-mods li')).find((x) => x.textContent.trim().indexOf(mod) === 0);
    li.querySelectorAll('.tk input')[0].click();
  }, exModule);
  await page.waitForTimeout(400);
  const back = await page.evaluate(() => {
    const jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]');
    return { total: jobs.length, mine: jobs.filter((j) => j.fromTemplate && j.fromTemplate.templateId === CT.tpl.id).length,
      hand: jobs.find((j) => j.id === 'hand') };
  });
  check('unticking removes only what this template created',
    back.mine === 0 && back.total === 1, JSON.stringify({ total: back.total, mine: back.mine }));
  check('THE mapping built by hand is untouched, column mapping and all',
    !!back.hand && back.hand.columnMapping.length === 1 && back.hand.status === 'ready');

  /* ── 11. Create staging tables ─────────────────────────────────────────── */
  execSql.length = 0;
  await page.click('#ct-stage');
  await page.waitForSelector('#ct-stage-modal.open');
  await page.waitForFunction(() => /already exist/.test(document.getElementById('ct-note').textContent)
    || document.getElementById('ct-stage-go').textContent !== 'Creating…', null, { timeout: 15000 });
  await page.waitForTimeout(400);
  const dlg = await page.evaluate(() => ({
    conns: Array.from(document.querySelectorAll('#ct-stage-conn option')).map((o) => o.value + ':' + o.textContent),
    schema: document.getElementById('ct-stage-schema').value,
    sql: document.getElementById('ct-stage-sql').value,
    rows: Array.from(document.querySelectorAll('#ct-stage-rows tbody tr')).map((tr) => tr.children[4] ? tr.children[4].textContent : ''),
    toCreate: CT.stagePlan.toCreate, scanned: CT.stagePlan.scanned,
  }));
  check('the picker offers only the active profile\'s connections, source first',
    dlg.conns.length === 2 && dlg.conns[0].indexOf('src:') === 0 && /FIN_3E_UAT/.test(dlg.conns[0]), dlg.conns.join(' | '));
  check('the dialog shows the SQL it will send, create-only and guarded',
    /IF OBJECT_ID\(N'dbo\./.test(dlg.sql) && /Create only\./.test(dlg.sql)
    && !/\bDROP\b|\bTRUNCATE\b|\bALTER\b/i.test(dlg.sql.replace(/\/\*[\s\S]*?\*\//g, '')), dlg.sql.slice(0, 160));
  check('the connection was checked first, and nothing exists yet',
    dlg.scanned === true && dlg.toCreate > 0 && dlg.rows.every((r) => /will create|no columns/.test(r)), dlg.rows.join(','));

  await page.click('#ct-stage-go');
  await page.waitForFunction(() => /created/.test(document.getElementById('ct-stage-progress').textContent), null, { timeout: 20000 });
  await page.waitForTimeout(300);
  const run1 = await page.evaluate(() => ({ prog: document.getElementById('ct-stage-progress').textContent,
    note: document.getElementById('ct-note').textContent }));
  const creates = execSql.filter((s) => /CREATE TABLE/.test(s));
  check('every table was created, one CREATE per call — never one big batch',
    creates.length === dlg.toCreate && creates.every((s) => (s.match(/CREATE TABLE/g) || []).length === 1),
    creates.length + ' calls for ' + dlg.toCreate + ' tables');
  check('the run reports what happened', new RegExp(dlg.toCreate + ' created').test(run1.prog), run1.prog);
  check('nothing sent could drop, truncate or alter anything',
    !execSql.some((s) => /\b(drop|truncate|alter|delete|insert|update|merge)\b/i.test(s)));

  // Run it again: everything is there now, so everything is skipped.
  execSql.length = 0;
  await page.waitForTimeout(3200);                       // the three-second gap is real
  await page.click('#ct-stage-scan');
  await page.waitForFunction(() => /already exist/.test(document.getElementById('ct-note').textContent), null, { timeout: 15000 });
  const run2 = await page.evaluate(() => ({
    toCreate: CT.stagePlan.toCreate, toSkip: CT.stagePlan.toSkip,
    goDisabled: document.getElementById('ct-stage-go').disabled,
    rows: Array.from(document.querySelectorAll('#ct-stage-rows tbody tr')).map((tr) => tr.children[4].textContent),
  }));
  check('a second run finds every table already there and offers to create nothing',
    run2.toCreate === 0 && run2.toSkip === creates.length && run2.goDisabled
    && run2.rows.filter((r) => /exists/.test(r)).length === creates.length, JSON.stringify(run2));
  check('and the second pass sent no CREATE at all — one catalog read, nothing else',
    !execSql.some((s) => /CREATE TABLE/.test(s)) && execSql.length === 1, execSql.length + ' calls');
  await page.click('#ct-stage-modal .ct-modal-foot .btn:has-text("Close")');

  /* ── 12. The ticks survive a reload ────────────────────────────────────── */
  await page.evaluate((mod) => {
    CygenixTemplateModel.tmSetModuleExcluded(CT.tpl, mod, true, 'me');
    CygenixTemplateModel.tmSetModuleMapped(CT.tpl, mod, true, 'me');
    touchDirty(); render();
  }, exModule);
  await page.click('#ct-save');
  await page.waitForFunction(() => /^Saved /.test(document.getElementById('ct-note').textContent), null, { timeout: 15000 });
  await open();
  const afterReload = await page.evaluate((mod) => {
    const m = CygenixTemplateModel.tmFindModule(CT.tpl, mod);
    const li = Array.from(document.querySelectorAll('#ct-mods li')).find((x) => x.textContent.trim().indexOf(mod) === 0);
    return { excluded: m.excluded, mapped: m.mapped,
      mapBox: li.querySelectorAll('.tk input')[0].checked, incBox: li.querySelectorAll('.tk input')[1].checked,
      greyed: li.classList.contains('ex') };
  }, exModule);
  check('both ticks are stored with the template and are exactly as they were after a reload',
    afterReload.excluded === true && afterReload.mapped === true
    && afterReload.mapBox === true && afterReload.incBox === false && afterReload.greyed, JSON.stringify(afterReload));

  check('no page errors', errors.length === 0, errors.join(' | '));

  await browser.close();
  server.close();
  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
