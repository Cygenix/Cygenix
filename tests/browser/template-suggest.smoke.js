/* tests/browser/template-suggest.smoke.js
 * ---------------------------------------------------------------------------
 * AI "Suggest tables" on the real Conversion Templates page, with the target
 * database and the two AI routes stubbed (neutral table names — the feature
 * is target-agnostic, and so is its test).
 *
 * Walks the brief's checks that a browser can make:
 *   3. two tables added by hand, then per-module Suggest: both show as
 *      "already added", and nothing is removed by Apply;
 *   4. a table in two modules carries the Shared flag, in the preview and in
 *      the grid;
 *   5. load order filled parents-before-children on new rows; a typed number
 *      is changed neither by Apply nor by Recalculate;
 *   6. editing an AI row clears its badge; the fields survive save + reload;
 *   7. a quick double-click starts one run; Cancel mid-run adds nothing;
 *   8. a failing call shows a readable error, once — no retry storm;
 * plus the buttons' disabled states, the headers the calls carry (the
 * caller's own key and token), and that nothing throws.
 * (Check 2 — a live run against the Demo profile's target — needs a real
 * database and a real key, and is for a person to do.)
 *
 * Run it by hand:  node tests/browser/template-suggest.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8440);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

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
const TABLES = ['Site', 'SiteAddress', 'AddressType', 'Region', 'Person', 'PersonPhone', 'Invoice', 'InvoiceLine'];
const FKS = [['SiteAddress', 'Site'], ['SiteAddress', 'AddressType'], ['Site', 'Region'], ['Person', 'Region'], ['PersonPhone', 'Person'], ['InvoiceLine', 'Invoice']];
// What the stubbed model says, per module.
const RANK = {
  Addresses: [['Site', 'high'], ['SiteAddress', 'high'], ['AddressType', 'high'], ['Region', 'medium']],
  Contacts: [['Person', 'high'], ['PersonPhone', 'high'], ['Region', 'high']],
};

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  const token = 'x.' + Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: U })).toString('base64url') + '.y';

  const store = new Map();
  const ai = { shortlist: 0, rank: 0, headers: [], mode: 'ok', rankDelay: 0 };
  const json = (route, body, status) => route.fulfill({ status: status || 200, contentType: 'application/json', body: JSON.stringify(body) });
  await ctx.route('**', async (route) => {
    const u = route.request().url();
    const q = new URL(u, 'http://x').searchParams;
    const action = q.get('action') || '';
    const body = (() => { try { return JSON.parse(route.request().postData() || 'null'); } catch (e) { return null; } })();
    if (/action=whoami/.test(u)) return json(route, { tier: 'pro', tier_status: 'active', role: 'user' });
    if (/data-proxy/.test(u) && /^\/agent\/template-suggest\//.test(q.get('path') || '')) {
      const h = route.request().headers();
      ai.headers.push({ key: h['x-anthropic-key'] || '', auth: h.authorization || '' });
      if (/shortlist$/.test(q.get('path'))) {
        ai.shortlist++;
        if (ai.mode === 'fail') return json(route, { error: 'Claude API error (401) — check the API key in Settings' }, 400);
        const out = {};
        body.modules.forEach((m) => { out[m.key] = (RANK[m.key] || []).map((x) => x[0]); });
        if (body.modules.some((m) => m.key === 'Billing')) out.Billing = ['Invoice'];
        return json(route, { shortlist: out, tableCount: body.tables.length });
      }
      ai.rank++;
      if (ai.rankDelay) await new Promise((r) => setTimeout(r, ai.rankDelay));
      if (body.module.key === 'Billing') return json(route, { error: 'Upstream error (529)' }, 502);
      return json(route, { ranked: (RANK[body.module.key] || []).map(([t, c]) => ({ table: t, confidence: c, reason: 'columns fit ' + body.module.key })), fks: [] });
    }
    if (/data-proxy/.test(u) && /^template-/.test(action)) {
      if (action === 'template-list') return json(route, { templates: [...store.values()].map((e) => Object.assign({}, e, { doc: undefined })) });
      if (action === 'template-get') { const e = store.get(q.get('id')); return e ? json(route, { template: e.doc, kind: 'draft' }) : json(route, { error: 'nf' }, 404); }
      if (action === 'template-save') { const d = body.template; store.set(d.id, { id: d.id, projectId: d.projectId, kind: 'draft', name: d.name, version: d.version, status: d.status, updatedAt: d.updatedAt, doc: d }); return json(route, { saved: true, id: d.id, version: d.version }); }
    }
    if (/db-connect/.test(u)) {
      const b = body || {};
      if (b.action === 'schema-tables') return json(route, { database: 'TARGETDB', tables: TABLES.map((n, i) => ({ schema: 'dbo', name: n, kind: 'table', rowCount: 100 * (i + 1) })) });
      if (b.action === 'schema-fks') return json(route, { foreignKeys: FKS.map(([c, p]) => ({ fromSchema: 'dbo', fromTable: c, fromColumn: p + 'Id', toSchema: 'dbo', toTable: p, toColumn: 'Id' })) });
      if (b.action === 'schema-columns') return json(route, { table: { schema: 'dbo', name: b.tableName, primaryKeys: ['Id'], columns: [{ name: 'Id', dataType: 'int' }, { name: b.tableName + 'Name', dataType: 'nvarchar' }] } });
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
    localStorage.setItem('cygenix_api_key', 'sk-ant-test-key');
    localStorage.setItem('cygenix_cookie_consent', JSON.stringify({ version: '2', essential: true, functional: true, analytics: false, timestamp: new Date().toISOString() }));
    localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify(acct));
    localStorage.setItem('h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-',
      JSON.stringify({ credentialType: 'IdToken', secret: arg.token, expiresOn: String(Math.floor(Date.now() / 1000) + 3600) }));
    localStorage.setItem('cygenix_projects', JSON.stringify([{ id: 'p1', name: 'Demo conversion' }]));
    localStorage.setItem('cygenix_active_project_id', 'p1');
    const live = {}; live[arg.U] = { srcConnString: 'mssql://u:p@src/legacy', srcConnMode: 'direct', tgtConnString: 'mssql://u:p@tgt/TARGETDB', tgtConnMode: 'direct' };
    localStorage.setItem('cygenix_project_connections', JSON.stringify(live));
    const blob = {}; blob[arg.U] = [{ id: 'c_src', side: 'src', mode: 'direct', name: 'Legacy' }, { id: 'c_tgt', side: 'tgt', mode: 'direct', name: 'Target DEV' }];
    localStorage.setItem('cygenix_saved_connections', JSON.stringify(blob));
    localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify({ c_src: { connString: 'mssql://u:p@src/legacy' }, c_tgt: { connString: 'mssql://u:p@tgt/TARGETDB' } }));
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify({ v: 1, connMeta: { c_src: { envClass: 'DEV' }, c_tgt: { envClass: 'DEV' } }, bindings: [], runRecords: [], events: [],
      profiles: [{ id: 'DEMO_DEV', name: 'Demo', envClass: 'DEV', status: 'active', srcConnId: 'c_src', tgtConnId: 'c_tgt', createdAt: 1, updatedAt: 1 }],
      settings: { envClasses: ['DEV', 'TEST', 'UAT', 'PRD', 'SANDBOX'], activeProfileId: 'DEMO_DEV', selectedAt: 1 } }));
    localStorage.setItem('cygenix_effort_estimates_v1', JSON.stringify({ active: 'Demo', estimates: { Demo: {
      v: 1, name: 'Demo', modules: ['Addresses', 'Contacts', 'Billing'],
      ticks: { 'analysis|Addresses': 1, 'analysis|Contacts': 1, 'analysis|Billing': 1 }, meta: {}, variables: [], rates: {}, tc: {}, data: {} } } }));
  }, { U, token });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('dialog', (d) => d.accept());
  const open = async () => {
    await page.goto('http://localhost:' + PORT + '/conversion-templates', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => typeof CT !== 'undefined' && !!CT.tpl && !!window.CygenixTemplateSuggest, null, { timeout: 20000 });
    await page.waitForTimeout(500);
  };
  const grid = () => page.evaluate(() => Array.from(document.querySelectorAll('#ct-tables tbody tr[data-id]')).map((tr) => {
    const inp = tr.querySelectorAll('input');
    return { target: inp[0].value, staging: inp[1].value, order: Number(inp[3].value),
      ai: !!tr.querySelector('.ct-ai'), shared: (tr.querySelector('.ct-shared') || {}).title || '' };
  }));
  const rowOf = async (name) => (await grid()).find((r) => r.target === name);
  const noteText = () => page.evaluate(() => document.getElementById('ct-note').textContent);
  const preview = () => page.evaluate(() => Array.from(document.querySelectorAll('#ct-sg-body .ct-sg-mod')).map((m) => ({
    module: m.querySelector('.ct-sg-mh').firstChild.textContent.trim(),
    error: (m.querySelector('.ct-sg-err') || {}).textContent || '',
    rows: Array.from(m.querySelectorAll('.ct-sg-row')).map((r) => ({ table: r.querySelector('.mono').textContent,
      already: r.classList.contains('already'), checked: !!(r.querySelector('input') || {}).checked,
      conf: r.querySelector('.chip').textContent, shared: (r.querySelector('.ct-shared') || {}).title || '' })),
  })));
  const waitPreview = () => page.waitForSelector('#ct-sg-modal.open', { timeout: 15000 });

  console.log('Conversion Templates — Suggest tables\n');
  await open();
  await page.evaluate(() => { const i = (CT.tpl.modules || []).findIndex(() => true); CT.tpl.modules.forEach((m) => { m.included = true; }); ctSelect('Addresses'); });
  await page.waitForTimeout(200);

  /* ── Buttons ───────────────────────────────────────────────────────────── */
  section('Buttons');
  let btn = await page.evaluate(() => ({ all: document.getElementById('ct-suggest-all'), one: document.getElementById('ct-suggest-mod') })
    && ({ all: !document.getElementById('ct-suggest-all').disabled, one: !document.getElementById('ct-suggest-mod').disabled,
      recalc: getComputedStyle(document.getElementById('ct-recalc')).display !== 'none' }));
  check('"Suggest tables" on the toolbar and "Suggest" on the module panel are there and live', btn.all && btn.one && btn.recalc, JSON.stringify(btn));
  const noKey = await page.evaluate(() => { const k = localStorage.getItem('cygenix_api_key'); localStorage.removeItem('cygenix_api_key'); renderSuggestButtons();
    const b = document.getElementById('ct-suggest-all'); const r = { disabled: b.disabled, title: b.title }; localStorage.setItem('cygenix_api_key', k); renderSuggestButtons(); return r; });
  check('without an Anthropic key they are disabled, and the tooltip says why', noKey.disabled && /Anthropic API key/.test(noKey.title), JSON.stringify(noKey));

  /* ── 3. Already added; add-only ─────────────────────────────────────────── */
  section('3. Two tables added by hand, then Suggest for the module');
  await page.evaluate(() => { ctAddTable('Site'); ctAddTable('SiteAddress'); });
  await page.evaluate(() => { const t = CT.tpl.modules.find((m) => m.module === 'Addresses').tables.find((x) => x.targetTable === 'SiteAddress'); ctPatchTable(t.id, 'loadOrder', '5'); });
  await page.click('#ct-suggest-mod');
  await waitPreview();
  let pv = await preview();
  const ad = pv.find((g) => g.module === 'Addresses') || { rows: [] };
  const pr = (t) => ad.rows.find((r) => r.table === t) || {};
  check('the preview is grouped by module (one, for the per-module button)', pv.length === 1 && pv[0].module === 'Addresses', JSON.stringify(pv.map((g) => g.module)));
  check('THE TWO HAND-ADDED TABLES SHOW AS "already added", greyed, with no checkbox', pr('Site').already && pr('SiteAddress').already && !pr('Site').checked);
  check('High is ticked, Medium is not', pr('AddressType').checked && pr('AddressType').conf === 'High' && !pr('Region').checked && pr('Region').conf === 'Medium');
  await page.evaluate(() => ctSuggestTick(0, true));
  await page.click('#ct-sg-apply');
  await page.waitForTimeout(200);
  let g = await grid();
  check('APPLY REMOVED NOTHING and added the ticked ones', ['Site', 'SiteAddress', 'AddressType', 'Region'].every((n) => g.some((r) => r.target === n)) && g.length === 4, g.map((r) => r.target).join());
  const at = await rowOf('AddressType');
  check('an added row\'s staging name is derived exactly as a manual add\'s (prefix)', at.staging === 'STG_AddressType', at.staging);
  check('added rows carry the AI badge; hand-added ones do not', at.ai && (await rowOf('Region')).ai && !(await rowOf('Site')).ai);
  const t3 = await noteText();
  check('the summary says what happened', /^Added 2 tables across 1 module\. \d+ shared\. Load order set on \d+ rows?\./.test(t3), t3);
  check('the template is marked unsaved, not saved', await page.evaluate(() => CT.dirty === true) && store.size === 0);

  /* ── 5. Load order ─────────────────────────────────────────────────────── */
  section('5. Load order');
  const site = await rowOf('Site'), region = await rowOf('Region');
  check('PARENTS BEFORE CHILDREN on the rows it filled: Region before Site', region.order < site.order && region.order % 10 === 0 && site.order % 10 === 0, region.order + ' / ' + site.order);
  check('A TYPED LOAD ORDER IS NOT CHANGED BY APPLY', (await rowOf('SiteAddress')).order === 5);
  await page.click('#ct-recalc');
  await page.waitForTimeout(250);
  check('…NOR BY RECALCULATE', (await rowOf('SiteAddress')).order === 5);

  /* ── 4. Shared ─────────────────────────────────────────────────────────── */
  section('4. Suggest across every in-scope module; Shared');
  await page.waitForTimeout(3100);                     // the 3-second minimum between runs
  const before = { sl: ai.shortlist, rk: ai.rank };
  await page.click('#ct-suggest-all');
  await waitPreview();
  pv = await preview();
  check('grouped by every in-scope module', pv.map((x) => x.module).join() === 'Addresses,Contacts,Billing', pv.map((x) => x.module).join());
  const reg = pv.find((x) => x.module === 'Contacts').rows.find((r) => r.table === 'Region');
  check('REGION IS FLAGGED SHARED IN THE PREVIEW, naming Addresses', !!reg && /Addresses/.test(reg.shared), JSON.stringify(reg));
  check('a module that failed shows its error, and the others are still there', /Could not rank this module: Upstream error \(529\)/.test(pv.find((x) => x.module === 'Billing').error)
    && pv.find((x) => x.module === 'Contacts').rows.length === 3);
  check('rank ran once per module, shortlist once', ai.rank - before.rk === 3 && ai.shortlist - before.sl === 1, (ai.rank - before.rk) + ' / ' + (ai.shortlist - before.sl));
  check('every AI call carried the caller\'s own key and their token', ai.headers.every((h) => h.key === 'sk-ant-test-key' && /^Bearer /.test(h.auth)));
  await page.click('#ct-sg-apply');
  await page.waitForTimeout(200);
  await page.evaluate(() => ctSelect('Contacts'));
  await page.waitForTimeout(150);
  check('REGION SHOWS SHARED IN THE GRID, in Contacts…', /Also in: Addresses/.test((await rowOf('Region')).shared), (await rowOf('Region')).shared);
  await page.evaluate(() => ctSelect('Addresses'));
  await page.waitForTimeout(150);
  check('…and in Addresses', /Also in: Contacts/.test((await rowOf('Region')).shared));

  /* ── 6. Badge, save, reload ───────────────────────────────────────────── */
  section('6. Editing clears the badge; the fields persist');
  await page.evaluate(() => { const t = CT.tpl.modules.find((m) => m.module === 'Addresses').tables.find((x) => x.targetTable === 'AddressType'); ctPatchTable(t.id, 'notes', 'checked by me'); });
  await page.waitForTimeout(100);
  check('EDITING AN AI ROW CLEARS ITS BADGE', !(await rowOf('AddressType')).ai && (await rowOf('Region')).ai);
  await page.evaluate(() => ctSave());
  await page.waitForTimeout(400);
  const saved = [...store.values()][0];
  const savedRegion = saved && saved.doc.modules.find((m) => m.module === 'Addresses').tables.find((t) => t.targetTable === 'Region');
  check('the saved template carries source, confidence, reason and loadOrderSource',
    !!savedRegion && savedRegion.source === 'ai' && savedRegion.aiConfidence === 'medium' && /columns fit/.test(savedRegion.aiReason) && savedRegion.loadOrderSource === 'ai', JSON.stringify(savedRegion));
  await page.evaluate(() => { try { localStorage.removeItem('cygenix_template_draft_v1::p1'); } catch (e) {} });
  await open();
  await page.evaluate(() => ctSelect('Addresses'));
  await page.waitForTimeout(150);
  check('AFTER A RELOAD the AI badge and the typed load order are still there', (await rowOf('Region')).ai && (await rowOf('SiteAddress')).order === 5 && !(await rowOf('AddressType')).ai);

  /* ── 7. Double-click, cancel ───────────────────────────────────────────── */
  section('7. One run at a time; Cancel keeps nothing');
  await page.waitForTimeout(3100);
  ai.rankDelay = 600;
  const sl0 = ai.shortlist;
  await page.evaluate(() => { ctSuggest(false); ctSuggest(false); });
  await page.waitForSelector('#ct-sg-progress.open');
  await page.waitForTimeout(300);
  check('A QUICK DOUBLE-CLICK STARTS ONE RUN', ai.shortlist - sl0 === 1, ai.shortlist - sl0);
  const rowsBefore = await page.evaluate(() => CT.tpl.modules.reduce((n, m) => n + m.tables.length, 0));
  await page.click('#ct-sg-cancel');
  await page.waitForTimeout(1200);
  const afterCancel = await page.evaluate(() => ({ rows: CT.tpl.modules.reduce((n, m) => n + m.tables.length, 0), preview: document.getElementById('ct-sg-modal').classList.contains('open'), running: SG.running }));
  check('CANCEL MID-RUN: no preview, nothing added, not stuck running', afterCancel.rows === rowsBefore && !afterCancel.preview && !afterCancel.running, JSON.stringify(afterCancel));
  check('…and it says so', /cancelled — nothing was added/.test(await noteText()));
  ai.rankDelay = 0;

  /* ── 8. A failing call ─────────────────────────────────────────────────── */
  section('8. A failing call');
  await page.waitForTimeout(3100);
  ai.mode = 'fail';
  const f0 = ai.shortlist, r0 = ai.rank;
  await page.click('#ct-suggest-all');
  await page.waitForTimeout(800);
  const t8 = await noteText();
  check('A READABLE ERROR appears', /Suggest tables failed: Claude API error \(401\) — check the API key in Settings/.test(t8), t8);
  check('…after exactly one call — no retries, no flood', ai.shortlist - f0 === 1 && ai.rank === r0, (ai.shortlist - f0) + ' / ' + (ai.rank - r0));
  check('…and the buttons are usable again', await page.evaluate(() => !SG.running));

  check('nothing threw', errors.length === 0, errors.slice(0, 3).join(' | '));
  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
