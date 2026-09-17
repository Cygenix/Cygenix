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
      if (body.action === 'schema-tables') return json(route, { database: 'ELITE3E', tables: [
        { schema: 'dbo', name: 'VchrDetail', kind: 'table' }, { schema: 'dbo', name: 'Vchr', kind: 'table' },
        { schema: 'dbo', name: 'Matter', kind: 'table' }, { schema: 'dbo', name: 'MattDate', kind: 'table' }] });
      if (body.action === 'schema-fks') return json(route, { foreignKeys: [] });
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

  check('no page errors', errors.length === 0, errors.join(' | '));

  await browser.close();
  server.close();
  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
