/* tests/browser/template-suggest.smoke.js
 * ---------------------------------------------------------------------------
 * "Suggest all" (and the module panel's "Suggest") on the real Conversion
 * Templates page, with the target database and the four batch routes of
 * agent/table-classify stubbed (neutral table names — the feature is
 * target-agnostic, and so is its test).
 *
 * The brief's final test, as far as a browser can make it:
 *   - three modules ticked (one with two tables added by hand, two empty),
 *     one in scope but NOT ticked; Suggest all; the review dialog — summary,
 *     already added, defaults (high and medium ticked, low not), Shared,
 *     Req/Opt, unassigned; Apply; then: the unticked module untouched, the
 *     hand-added tables still there, the typed load order kept, parents
 *     before children, "Also in" on the shared table, template unsaved;
 * plus: every table goes in with its columns and every chunk with every
 * ticked module; a failed chunk is listed and retried on its own; closing
 * the review keeps the run and Suggest all picks it up again without a new
 * batch; a double-click starts one run; Cancel cancels the batch and adds
 * nothing; a refused key shows one readable error after one call; the
 * per-module button sends one module; nothing throws.
 * (A live run against a real target with a real key is for a person to do.)
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
const TABLES = ['Site', 'SiteAddress', 'AddressType', 'Region', 'Person', 'PersonPhone', 'Invoice', 'InvoiceLine', 'AuditLog', 'Misc'];
const FKS = [['SiteAddress', 'Site'], ['SiteAddress', 'AddressType'], ['Site', 'Region'], ['Person', 'Region'], ['PersonPhone', 'Person'], ['InvoiceLine', 'Invoice']];
// What the stubbed classifier says, per table: [modules, confidence, required].
const ANSWER = {
  Site: [['Addresses'], 'high', true], SiteAddress: [['Addresses'], 'high', true], AddressType: [['Addresses'], 'medium', false],
  Region: [['Addresses', 'Contacts'], 'high', true], Person: [['Contacts'], 'high', true], PersonPhone: [['Contacts'], 'low', false],
  Invoice: [['Billing'], 'high', true], InvoiceLine: [['Billing'], 'high', true], AuditLog: [[], 'high', false], Misc: [[], 'low', false],
};

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  const token = 'x.' + Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: U })).toString('base64url') + '.y';

  const store = new Map();
  // ai.mode: 'ok' | 'failOnce' (results fail the first time) | 'slow' (never ends) | 'badkey' (start refused)
  const ai = { start: [], status: 0, results: 0, cancel: [], headers: [], mode: 'ok', batches: {}, n: 0 };
  const json = (route, body, status) => route.fulfill({ status: status || 200, contentType: 'application/json', body: JSON.stringify(body) });
  await ctx.route('**', async (route) => {
    const u = route.request().url();
    const q = new URL(u, 'http://x').searchParams;
    const action = q.get('action') || '';
    const body = (() => { try { return JSON.parse(route.request().postData() || 'null'); } catch (e) { return null; } })();
    if (/action=whoami/.test(u)) return json(route, { tier: 'pro', tier_status: 'active', role: 'user' });
    const agent = q.get('path') || '';
    if (/data-proxy/.test(u) && /^\/agent\/table-classify\//.test(agent)) {
      const h = route.request().headers();
      ai.headers.push({ key: h['x-anthropic-key'] || '', auth: h.authorization || '' });
      if (/start$/.test(agent)) {
        if (ai.mode === 'badkey') { ai.start.push(body); return json(route, { error: 'Claude API error (401) — check the API key in Settings' }, 400); }
        ai.start.push(body);
        const id = 'msgbatch_' + (++ai.n);
        ai.batches[id] = body.chunks;
        return json(route, { batchId: id, status: 'in_progress', chunkIds: body.chunks.map((c) => c.id) });
      }
      if (/status$/.test(agent)) {
        ai.status++;
        return json(route, { batches: body.batchIds.map((id) => ({ id, status: ai.mode === 'slow' ? 'in_progress' : 'ended',
          counts: { processing: 0, succeeded: (ai.batches[id] || []).length, errored: 0, canceled: 0, expired: 0 } })) });
      }
      if (/results$/.test(agent)) {
        ai.results++;
        const failNow = ai.mode === 'failOnce' && !ai.failedOnce;
        if (failNow) ai.failedOnce = true;
        const mods = body.modules;
        return json(route, { chunks: body.chunks.map((c) => failNow
          ? { id: c.id, ok: false, error: 'Claude could not process this batch of tables (overloaded_error)' }
          : { id: c.id, ok: true, unanswered: [], rows: c.tables.map((t) => { const a = ANSWER[t];
              return { table: t, modules: a[0].filter((m) => mods.indexOf(m) >= 0), confidence: a[1], required: a[2], reason: 'columns fit ' + t }; }) }) });
      }
      if (/cancel$/.test(agent)) { ai.cancel.push(body.batchIds); return json(route, { cancelled: body.batchIds }); }
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
      v: 1, name: 'Demo', modules: ['Addresses', 'Contacts', 'Billing', 'Other'],
      ticks: { 'analysis|Addresses': 1, 'analysis|Contacts': 1, 'analysis|Billing': 1, 'analysis|Other': 1 }, meta: {}, variables: [], rates: {}, tc: {}, data: {} } } }));
  }, { U, token });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  let dialogAnswer = true;
  const dialogs = [];
  page.on('dialog', (d) => { dialogs.push(d.message()); return dialogAnswer ? d.accept() : d.dismiss(); });
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
  const tablesIn = (mod) => page.evaluate((m) => (CT.tpl.modules.find((x) => x.module === m) || { tables: [] }).tables.map((t) => t.targetTable), mod);
  const tableRow = (mod, name) => page.evaluate(([m, n]) => (CT.tpl.modules.find((x) => x.module === m) || { tables: [] }).tables.find((t) => t.targetTable === n) || null, [mod, name]);
  const noteText = () => page.evaluate(() => document.getElementById('ct-note').textContent);
  const preview = () => page.evaluate(() => ({
    summary: (document.querySelector('#ct-sg-body .ct-sg-sum') || {}).textContent || '',
    failed: (document.querySelector('#ct-sg-body .ct-sg-fail') || {}).textContent || '',
    unassigned: (document.querySelector('#ct-sg-body .ct-sg-un') || {}).textContent || '',
    groups: Array.from(document.querySelectorAll('#ct-sg-body .ct-sg-mod')).map((m) => ({
      module: m.querySelector('.ct-sg-mh').firstChild.textContent.trim(),
      rows: Array.from(m.querySelectorAll('.ct-sg-row')).map((r) => ({ table: r.querySelector('.mono').textContent,
        already: r.classList.contains('already'), checked: !!(r.querySelector('input') || {}).checked,
        conf: r.querySelectorAll('.chip')[0].textContent, req: r.querySelectorAll('.chip')[1].textContent,
        shared: (r.querySelector('.ct-shared') || {}).title || '' })) })),
  }));
  const waitPreview = () => page.waitForSelector('#ct-sg-modal.open', { timeout: 20000 });
  const gap = () => page.waitForTimeout(3100);            // the 3-second minimum between runs

  console.log('Conversion Templates — Suggest all\n');
  await open();
  // Three modules ticked; "Other" stays in scope but unticked.
  await page.evaluate(() => { CT.tpl.modules.forEach((m) => { m.included = m.module !== 'Other'; }); ctSelect('Addresses'); });
  await page.waitForTimeout(200);

  /* ── Buttons ───────────────────────────────────────────────────────────── */
  section('Buttons');
  const btn = await page.evaluate(() => ({ label: document.getElementById('ct-suggest-all').textContent.trim(), all: !document.getElementById('ct-suggest-all').disabled,
    one: !document.getElementById('ct-suggest-mod').disabled, title: document.getElementById('ct-suggest-all').title }));
  check('the toolbar button now reads "Suggest all", and it and the module panel\'s "Suggest" are live', btn.label === 'Suggest all' && btn.all && btn.one, JSON.stringify(btn));
  check('…its tooltip counts the ticked modules', /3 ticked modules/.test(btn.title), btn.title);
  const noKey = await page.evaluate(() => { const k = localStorage.getItem('cygenix_api_key'); localStorage.removeItem('cygenix_api_key'); renderSuggestButtons();
    const b = document.getElementById('ct-suggest-all'); const r = { disabled: b.disabled, title: b.title }; localStorage.setItem('cygenix_api_key', k); renderSuggestButtons(); return r; });
  check('without an Anthropic key it is disabled, and the tooltip says why', noKey.disabled && /Anthropic API key/.test(noKey.title), JSON.stringify(noKey));
  const noTick = await page.evaluate(() => { const was = CT.tpl.modules.map((m) => m.included); CT.tpl.modules.forEach((m) => { m.included = false; }); renderSuggestButtons();
    const b = document.getElementById('ct-suggest-all'); const r = { disabled: b.disabled, title: b.title }; CT.tpl.modules.forEach((m, i) => { m.included = was[i]; }); renderSuggestButtons(); return r; });
  check('with no module ticked it is disabled and says to tick Include', noTick.disabled && /Tick Include/.test(noTick.title), JSON.stringify(noTick));

  /* ── The final test ────────────────────────────────────────────────────── */
  section('Three ticked modules (two of them empty), one unticked; Suggest all; review; apply');
  await page.evaluate(() => { ctAddTable('Site'); ctAddTable('SiteAddress'); });
  await page.evaluate(() => { const t = CT.tpl.modules.find((m) => m.module === 'Addresses').tables.find((x) => x.targetTable === 'SiteAddress'); ctPatchTable(t.id, 'loadOrder', '5'); });
  await page.click('#ct-suggest-all');
  await waitPreview();
  const sent = ai.start[0] || { modules: [], chunks: [] };
  check('ONE batch submitted, with the three TICKED modules only', ai.start.length === 1 && sent.modules.map((m) => m.name).join() === 'Addresses,Contacts,Billing', JSON.stringify(sent.modules));
  check('EVERY target table went in, with its columns, key and FK neighbours',
    sent.chunks.reduce((n, c) => n + c.tables.length, 0) === TABLES.length
    && sent.chunks[0].tables.every((t) => t.columns.length === 2 && t.pk.join() === 'Id')
    && sent.chunks[0].tables.find((t) => t.name === 'Site').refBy.join() === 'SiteAddress', JSON.stringify(sent.chunks[0].tables[0]));
  check('every call carried the caller\'s own key and their token', ai.headers.every((h) => h.key === 'sk-ant-test-key' && /^Bearer /.test(h.auth)));
  let pv = await preview();
  const grp = (m) => pv.groups.find((g) => g.module === m) || { rows: [] };
  const pr = (m, t) => grp(m).rows.find((r) => r.table === t) || {};
  check('SUMMARY: "6 tables across 3 modules, 1 shared, 2 unassigned"', /6 tables across 3 modules, 1 shared, 2 unassigned/.test(pv.summary), pv.summary);
  check('grouped by the ticked modules only — "Other" is not in the dialog', pv.groups.map((g) => g.module).join() === 'Addresses,Contacts,Billing', pv.groups.map((g) => g.module).join());
  check('THE TWO HAND-ADDED TABLES SHOW AS "already added", with no tick box', pr('Addresses', 'Site').already && pr('Addresses', 'SiteAddress').already && !pr('Addresses', 'Site').checked);
  check('High and Medium ticked, Low not', pr('Addresses', 'Region').checked && pr('Addresses', 'AddressType').checked && pr('Addresses', 'AddressType').conf === 'Medium'
    && !pr('Contacts', 'PersonPhone').checked && pr('Contacts', 'PersonPhone').conf === 'Low');
  check('required shows as Req / Opt', pr('Addresses', 'Region').req === 'Req' && pr('Addresses', 'AddressType').req === 'Opt');
  check('REGION IS SHARED in both modules, each naming the other', /Contacts/.test(pr('Addresses', 'Region').shared) && /Addresses/.test(pr('Contacts', 'Region').shared));
  check('the unassigned tables are listed and offered nowhere', /Unassigned — 2 tables/.test(pv.unassigned) && /AuditLog/.test(pv.unassigned) && !pv.groups.some((g) => g.rows.some((r) => r.table === 'AuditLog')));
  const otherBefore = JSON.stringify(await tablesIn('Other'));
  await page.click('#ct-sg-apply');
  await page.waitForTimeout(250);
  check('APPLY added the ticked tables and removed nothing',
    (await tablesIn('Addresses')).join() === 'Site,SiteAddress,Region,AddressType' && (await tablesIn('Contacts')).join() === 'Person,Region'
    && (await tablesIn('Billing')).join() === 'Invoice,InvoiceLine', JSON.stringify([await tablesIn('Addresses'), await tablesIn('Contacts'), await tablesIn('Billing')]));
  check('THE UNTICKED MODULE IS UNTOUCHED', JSON.stringify(await tablesIn('Other')) === otherBefore && otherBefore === '[]');
  const at = await rowOf('AddressType');
  check('staging names derived as for a manual add (prefix)', at.staging === 'STG_AddressType', at.staging);
  check('required set from the AI; AI badge on added rows, not on hand-added ones',
    (await tableRow('Addresses', 'AddressType')).required === false && at.ai && !(await rowOf('Site')).ai);
  check('THE SHARED TABLE SAYS WHERE ELSE IT IS, in its notes', (await tableRow('Addresses', 'Region')).notes === 'Also in: Contacts' && (await tableRow('Contacts', 'Region')).notes === 'Also in: Addresses');
  const site = await rowOf('Site'), region = await rowOf('Region');
  check('LOAD ORDER: parents before children', region.order < site.order && (await tableRow('Billing', 'Invoice')).loadOrder < (await tableRow('Billing', 'InvoiceLine')).loadOrder, region.order + ' / ' + site.order);
  check('A TYPED LOAD ORDER IS KEPT', (await rowOf('SiteAddress')).order === 5);
  const t1 = await noteText();
  check('the summary says what happened, and that it is not saved', /^Added 6 tables across 3 modules\. 1 shared\. Load order set on \d+ rows?\..* Not saved yet\.$/.test(t1), t1);
  check('the template is marked unsaved, not saved', await page.evaluate(() => CT.dirty === true) && store.size === 0);
  check('the run record is cleared once applied', await page.evaluate(() => localStorage.getItem('cygenix_ct_suggest_run_v1') === null));

  /* ── Failed chunk, retry, close, resume ────────────────────────────────── */
  section('A failed chunk is retried on its own; closing keeps the run; Suggest all picks it up');
  await gap();
  ai.mode = 'failOnce';
  const s0 = ai.start.length;
  await page.click('#ct-suggest-all');
  await waitPreview();
  pv = await preview();
  check('the failure is listed with its reason, and a Retry button', /Some tables could not be classified/.test(pv.failed) && /overloaded_error/.test(pv.failed)
    && await page.evaluate(() => !!document.getElementById('ct-sg-retry')), pv.failed);
  check('…and the summary says how many were not classified', /10 not classified/.test(pv.summary), pv.summary);
  await gap();
  await page.click('#ct-sg-retry');
  await waitPreview();
  await page.waitForFunction(() => !document.getElementById('ct-sg-retry'), null, { timeout: 15000 }).catch(() => {});
  pv = await preview();
  const retryBody = ai.start[ai.start.length - 1];
  check('RETRY sent only the failed tables, as a new batch', ai.start.length === s0 + 2 && retryBody.chunks.every((c) => /^r1c\d+$/.test(c.id)) && retryBody.chunks[0].tables.length === 10);
  check('…and the review now has them, with nothing failed', !pv.failed && /2 unassigned/.test(pv.summary), pv.summary);
  await page.evaluate(() => ctSuggestClose());
  check('closing the review keeps the run record', await page.evaluate(() => !!localStorage.getItem('cygenix_ct_suggest_run_v1')));
  check('…and the button says a run is waiting', /is waiting/.test(await page.evaluate(() => document.getElementById('ct-suggest-all').title)));
  await gap();
  const s1 = ai.start.length, r1 = ai.results;
  dialogAnswer = true;
  await page.click('#ct-suggest-all');
  await waitPreview();
  check('SUGGEST ALL ASKS, then PICKS IT UP without submitting anything new', /has not been applied yet/.test(dialogs[dialogs.length - 1] || '') && ai.start.length === s1 && ai.results > r1);
  await page.evaluate(() => ctSuggestClose());
  await gap();
  dialogAnswer = false;                                  // discard it this time
  ai.mode = 'ok';
  await page.click('#ct-suggest-all');
  await waitPreview();
  dialogAnswer = true;
  check('…or discards it and starts a new run', ai.start.length === s1 + 1);
  await page.evaluate(() => ctSuggestClose());

  /* ── Per-module ────────────────────────────────────────────────────────── */
  section('The module panel\'s Suggest: the same thing with one module');
  await gap();
  dialogAnswer = false;                                  // do not resume the closed run
  await page.evaluate(() => ctSelect('Billing'));
  await page.click('#ct-suggest-mod');
  await waitPreview();
  dialogAnswer = true;
  const last = ai.start[ai.start.length - 1];
  pv = await preview();
  check('one module goes with every table', last.modules.map((m) => m.name).join() === 'Billing' && last.chunks.reduce((n, c) => n + c.tables.length, 0) === TABLES.length);
  check('the review has that module only, its tables already added', pv.groups.length === 1 && pv.groups[0].rows.every((r) => r.already));
  await page.evaluate(() => ctSuggestClose());

  /* ── Double-click, cancel ──────────────────────────────────────────────── */
  section('One run at a time; Cancel cancels the batch and keeps nothing');
  await page.evaluate(() => { try { localStorage.removeItem('cygenix_ct_suggest_run_v1'); } catch (e) {} });
  await gap();
  ai.mode = 'slow';
  const s2 = ai.start.length;
  await page.evaluate(() => { ctSuggest(false); ctSuggest(false); });
  await page.waitForSelector('#ct-sg-progress.open');
  await page.waitForFunction(() => /Classifying/.test(document.getElementById('ct-sg-step').textContent), null, { timeout: 15000 });
  check('A QUICK DOUBLE-CLICK STARTS ONE RUN', ai.start.length - s2 === 1, ai.start.length - s2);
  check('progress reads "Classifying: x of y chunks done…"', /^Classifying: \d+ of \d+ chunks? done…$/.test(await page.evaluate(() => document.getElementById('ct-sg-step').textContent)));
  const rowsBefore = await page.evaluate(() => CT.tpl.modules.reduce((n, m) => n + m.tables.length, 0));
  await page.click('#ct-sg-cancel');
  await page.waitForTimeout(800);
  const afterCancel = await page.evaluate(() => ({ rows: CT.tpl.modules.reduce((n, m) => n + m.tables.length, 0), preview: document.getElementById('ct-sg-modal').classList.contains('open'),
    running: SG.running, run: localStorage.getItem('cygenix_ct_suggest_run_v1') }));
  check('CANCEL: the batch is cancelled at Anthropic', ai.cancel.length === 1 && ai.cancel[0].join() === 'msgbatch_' + ai.n);
  check('…no preview, nothing added, not stuck running, no run kept', afterCancel.rows === rowsBefore && !afterCancel.preview && !afterCancel.running && !afterCancel.run, JSON.stringify(afterCancel));
  check('…and it says so', /cancelled — nothing was added/.test(await noteText()));

  /* ── A refused key ─────────────────────────────────────────────────────── */
  section('A refused key');
  await gap();
  ai.mode = 'badkey';
  const s3 = ai.start.length, st3 = ai.status;
  await page.click('#ct-suggest-all');
  await page.waitForTimeout(1200);
  const t8 = await noteText();
  check('A READABLE ERROR appears', /Suggest failed: Claude API error \(401\) — check the API key in Settings/.test(t8), t8);
  check('…after exactly one call — no retries, no polling, no flood', ai.start.length - s3 === 1 && ai.status === st3);
  check('…and the buttons are usable again', await page.evaluate(() => !SG.running));
  ai.mode = 'ok';

  /* ── Save, reload ──────────────────────────────────────────────────────── */
  section('The fields persist');
  await page.evaluate(() => ctSave());
  await page.waitForTimeout(400);
  const saved = [...store.values()][0];
  const savedRegion = saved && saved.doc.modules.find((m) => m.module === 'Addresses').tables.find((t) => t.targetTable === 'Region');
  check('the saved template carries source, confidence, reason, required, notes and loadOrderSource',
    !!savedRegion && savedRegion.source === 'ai' && savedRegion.aiConfidence === 'high' && /columns fit/.test(savedRegion.aiReason)
    && savedRegion.required === true && savedRegion.notes === 'Also in: Contacts' && savedRegion.loadOrderSource === 'ai', JSON.stringify(savedRegion));
  await page.evaluate(() => { try { localStorage.removeItem('cygenix_template_draft_v1::p1'); } catch (e) {} });
  await open();
  await page.evaluate(() => ctSelect('Addresses'));
  await page.waitForTimeout(150);
  check('AFTER A RELOAD the AI badge and the typed load order are still there', (await rowOf('Region')).ai && (await rowOf('SiteAddress')).order === 5);

  check('nothing threw', errors.length === 0, errors.slice(0, 3).join(' | '));
  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
