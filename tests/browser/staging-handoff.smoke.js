/* tests/browser/staging-handoff.smoke.js
 * ---------------------------------------------------------------------------
 * The Dev Console's staging session, end to end in a real browser, for the
 * four gaps closed in Oct-2026:
 *   C. the template VERSION is chosen beside the Staging box (default: the
 *      newest published), a warning shows when its draft has unpublished
 *      edits, the choice is named in the confirmation, sent with the session
 *      and shown on the pill;
 *   A. a session on the SOURCE is given the profile's target as a read-only
 *      reference — its connection id and name, never its secret;
 *   B. the person's Was/Is rules and Parameters go with the session, and the
 *      confirmation says how many;
 *   D. "Load into target": the plan is confirmed, then the maps are written
 *      and Object Mapping generates each one's SQL in a hidden frame
 *      (?autosave=1) — a real Object Mapping page, not a stub — so the jobs
 *      come out ready to run, a worked-on map is left alone, and a table the
 *      session did not build gets nothing.
 *
 * Azure and db-connect are stubbed at the network; everything in the browser
 * is the real code. Table names are invented: the tool is target-agnostic.
 *
 * Run it by hand:  node tests/browser/staging-handoff.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8487);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

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
  if (ROUTES[p]) p = ROUTES[p];
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end('no'); }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

const U = 'you@example.test';
const SRC_CS = 'Server=src.example.test;Database=legacy;User ID=u;Password=SrcPa55word';
const TGT_CS = 'Server=tgt.example.test;Database=newdb;User ID=u;Password=TgtPa55word';

/* The source database after a staging build: its own tables, and the
   staging schema "stg" holding tables in the target's shape. */
const col = (name, type, extra) => Object.assign({ name, type, nullable: true }, extra || {});
const SRC_TABLES = [
  { schema: 'dbo', name: 'Customer', columns: [col('CustNo', 'INT'), col('CustName', 'NVARCHAR(100)')] },
  { schema: 'stg', name: 'STG_Client', columns: [col('ClientID', 'INT', { nullable: false }), col('Name', 'NVARCHAR(100)'), col('TypeCode', 'NVARCHAR(10)')] },
  { schema: 'stg', name: 'STG_Invoice', columns: [col('InvoiceID', 'INT', { nullable: false }), col('ClientID', 'INT'), col('Amount', 'DECIMAL(18,2)')] },
  { schema: 'stg', name: 'STG_ClientType', columns: [col('TypeCode', 'NVARCHAR(10)'), col('Label', 'NVARCHAR(50)')] },
];
/* The target: Invoice lives in its own schema, so the new map must say so. */
const TGT_TABLES = [
  { schema: 'dbo', name: 'Client', columns: [col('ClientID', 'INT', { nullable: false }), col('Name', 'NVARCHAR(100)'), col('TypeCode', 'NVARCHAR(10)'), col('Created', 'DATETIME')] },
  { schema: 'fin', name: 'Invoice', columns: [col('InvoiceID', 'INT', { nullable: false }), col('ClientID', 'INT'), col('Amount', 'DECIMAL(18,2)')] },
  { schema: 'dbo', name: 'ClientType', columns: [col('TypeCode', 'NVARCHAR(10)'), col('Label', 'NVARCHAR(50)')] },
  { schema: 'dbo', name: 'ClientNote', columns: [col('NoteID', 'INT')] },
];

/* The template: four tables ticked Include, one module not ticked. */
const T = (id, target, order) => ({ id, targetTable: target, stagingTable: 'STG_' + target, loadOrder: order, columns: [] });
const TEMPLATE = { id: 'tpl_a', name: 'Finance', version: 2, projectId: 'p1', profileId: 'DEMO', modules: [
  { module: 'Clients', inScope: true, included: true, tables: [T('t1', 'Client', 20), T('t2', 'ClientType', 10), T('t3', 'ClientNote', 30)] },
  { module: 'Billing', inScope: true, included: true, tables: [T('t4', 'Invoice', 40)] },
  { module: 'Ledger', inScope: true, included: false, tables: [T('t5', 'Ledger', 50)] },
] };
const OLD = new Date(Date.now() - 86400e3).toISOString();
const NOW = new Date().toISOString();
const TEMPLATES = [
  { id: 'pub_tpl_a_v2', templateId: 'tpl_a', projectId: 'p1', profileId: 'DEMO', kind: 'published', name: 'Finance', version: 2, publishedAt: OLD, updatedAt: OLD },
  { id: 'tpl_a', templateId: 'tpl_a', projectId: 'p1', profileId: 'DEMO', kind: 'draft', name: 'Finance', version: 3, updatedAt: NOW },
];

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const token = 'x.' + Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: U })).toString('base64url') + '.y';

  const world = { calls: [], tplCalls: [], db: [], sessions: {} };
  const json = (route, body, status) => route.fulfill({ status: status || 200, contentType: 'application/json', body: JSON.stringify(body) });
  const me = { oid: 'x', email: U, roles: ['ML'], claudeCode: { enabled: true, roles: ['OW', 'PA', 'ML'], allowed: true, canChangeData: true, canConfigure: false } };
  await ctx.route('**', async (route) => {
    const u = route.request().url();
    const q = new URL(u, 'http://x').searchParams;
    const p = q.get('path') || '';
    const action = q.get('action') || '';
    if (/rbac-admin\?what=me/.test(u)) return json(route, me);
    if (/action=whoami/.test(u)) return json(route, { tier: 'pro', tier_status: 'active', role: 'user' });
    if (/api\.anthropic\.com/.test(u)) { world.calls.push({ action: 'ANTHROPIC' }); return json(route, {}); }
    if (/data-proxy/.test(u) && /^template-/.test(action)) {
      world.tplCalls.push({ action, id: q.get('id'), projectId: q.get('projectId') });
      if (action === 'template-list') return json(route, { templates: TEMPLATES });
      if (action === 'template-get') return q.get('id') === 'pub_tpl_a_v2' || q.get('id') === 'tpl_a'
        ? json(route, { template: Object.assign({}, TEMPLATE, { version: q.get('id') === 'tpl_a' ? 3 : 2 }), kind: q.get('id') === 'tpl_a' ? 'draft' : 'published' })
        : json(route, { error: 'Template not found' }, 404);
    }
    if (/data-proxy/.test(u) && /^\/agent\/claude-code\//.test(p)) {
      const act = p.replace(/^\/agent\/claude-code\//, '').split('?')[0];
      const body = route.request().postData() ? JSON.parse(route.request().postData()) : {};
      world.calls.push({ action: act, method: route.request().method(), body });
      if (act === 'session' && route.request().method() === 'POST') {
        const id = 'sesn_' + String(Object.keys(world.sessions).length + 1).padStart(6, '0');
        const t = TEMPLATES.find((x) => x.id === body.templateId) || TEMPLATES[0];
        const s = { id, title: '', status: 'idle', dataChangesAllowed: false, profileId: body.profileId, profileName: body.profileName,
          connectionId: body.connId, connectionName: body.connectionName, side: body.side, dbType: 'sqlserver', dbHost: 'src.example.test', dbName: 'legacy',
          createdAt: NOW, costCents: 0, stagingSchema: body.stagingSchema || '', projectId: body.projectId || '',
          reference: body.reference ? { connectionName: body.reference.connectionName, side: 'tgt', dbName: 'newdb', dbHost: 'tgt.example.test' } : null,
          templateRef: body.stagingSchema ? { id: t.id, templateId: t.templateId, name: t.name, version: t.version, kind: t.kind } : null,
          rulesCount: body.rules ? { wasis: (body.rules.wasis || []).length, params: (body.rules.params || []).length, truncated: false } : null, uploads: [] };
        world.sessions[id] = s;
        return json(route, { session: s });
      }
      if (act === 'sessions') return json(route, { sessions: Object.values(world.sessions).reverse() });
      if (act === 'events') return json(route, { status: 'idle', events: [], costCents: 0, dataChangesAllowed: false, done: false });
      if (act === 'outputs') return json(route, { outputs: [], uploads: [] });
      return json(route, { error: 'unexpected ' + act }, 500);
    }
    if (/db-connect/.test(u)) {
      let body = {}; try { body = JSON.parse(route.request().postData() || '{}'); } catch (e) {}
      const isSrc = /src\.example\.test/.test(body.connectionString || '');
      const tables = isSrc ? SRC_TABLES : TGT_TABLES;
      world.db.push({ side: isSrc ? 'src' : 'tgt', action: body.action, sql: String(body.sql || '') });
      if (body.action === 'schema-tables') return json(route, { success: true, database: isSrc ? 'legacy' : 'newdb',
        tables: tables.map((t) => ({ schema: t.schema, name: t.name, fullName: t.schema + '.' + t.name, type: 'BASE TABLE', rowCount: 10 })) });
      if (body.action === 'schema-fks') return json(route, { success: true, foreignKeys: [] });
      if (body.action === 'schema-columns') {
        const t = tables.find((x) => x.schema.toLowerCase() === String(body.schemaName).toLowerCase() && x.name.toLowerCase() === String(body.tableName).toLowerCase());
        return json(route, { success: true, table: t ? { schema: t.schema, name: t.name, columns: t.columns, primaryKeys: [], foreignKeys: [] } : { schema: body.schemaName, name: body.tableName, columns: [], primaryKeys: [], foreignKeys: [] } });
      }
      return json(route, { success: true });
    }
    // A save the stub does not confirm reads as a refusal, and the banner it
    // raises would sit over the page.
    if (/data-proxy/.test(u) && action === 'save') return json(route, { saved: true, updatedAt: NOW, fields: [], ignored: [] });
    if (/data-proxy|netlify\/functions|\/api\//.test(u)) return json(route, {});
    if (u.startsWith('http://localhost:' + PORT)) return route.continue();
    return route.abort();
  });

  await ctx.addInitScript((a) => {
    if (localStorage.getItem('cygenix_profiles_v1')) return;      // runs again in every frame and navigation
    const acct = { homeAccountId: 'h.t', environment: 'cygenix.ciamlogin.com', tenantId: 't', username: a.U, localAccountId: 'l', authorityType: 'MSSTS', name: 'You' };
    [localStorage, sessionStorage].forEach((s) => { s.setItem('cygenix_token', a.token); s.setItem('cygenix_expires', String(Date.now() + 3600e3)); });
    localStorage.setItem('cygenix_onboarded', 'true');
    localStorage.setItem('cygenix_user', JSON.stringify({ email: a.U }));
    localStorage.setItem('cygenix_active_user', a.U);
    localStorage.setItem('cygenix_tier', 'pro');
    localStorage.setItem('cygenix_api_key', 'sk-ant-smoke-key');
    localStorage.setItem('cygenix_cookie_consent', JSON.stringify({ version: '2', essential: true, functional: true, analytics: false, timestamp: new Date().toISOString() }));
    localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify(acct));
    localStorage.setItem('h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-',
      JSON.stringify({ credentialType: 'IdToken', secret: a.token, expiresOn: String(Math.floor(Date.now() / 1000) + 3600) }));
    localStorage.setItem('cygenix_projects', JSON.stringify([{ id: 'p1', name: 'Demo' }]));
    localStorage.setItem('cygenix_active_project_id', 'p1');
    const conns = { srcConnString: a.SRC_CS, srcConnMode: 'direct', tgtConnString: a.TGT_CS, tgtConnMode: 'direct' };
    localStorage.setItem('cygenix_connections', JSON.stringify(conns));
    const live = {}; live[a.U] = conns;
    localStorage.setItem('cygenix_project_connections', JSON.stringify(live));
    const blob = {}; blob[a.U] = [{ id: 'c_src', side: 'src', mode: 'direct', name: 'Legacy' }, { id: 'c_tgt', side: 'tgt', mode: 'direct', name: 'New system' }];
    localStorage.setItem('cygenix_saved_connections', JSON.stringify(blob));
    localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify({ c_src: { connString: a.SRC_CS }, c_tgt: { connString: a.TGT_CS } }));
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify({ v: 1, connMeta: { c_src: { envClass: 'DEV' }, c_tgt: { envClass: 'DEV' } }, bindings: [], runRecords: [], events: [],
      profiles: [{ id: 'DEMO', name: 'Demo', envClass: 'DEV', status: 'active', srcConnId: 'c_src', tgtConnId: 'c_tgt', createdAt: 1, updatedAt: 1 }],
      settings: { envClasses: ['DEV', 'TEST', 'UAT', 'PRD', 'SANDBOX'], activeProfileId: 'DEMO', selectedAt: 1 } }));
    // The person's translation rules.
    localStorage.setItem('cygenix_wasis_rules', JSON.stringify([
      { id: 'w1', srcTable: 'Customer', srcField: 'status', oldVal: 'A', newVal: 'ACTIVE', desc: 'status codes' },
      { id: 'w2', srcTable: 'Customer', srcField: 'status', oldVal: 'C', newVal: 'CLOSED', desc: '' }]));
    localStorage.setItem('cygenix_sys_params', JSON.stringify([{ name: 'Cut-over date', code: '@@CUTOVER', type: 'date', value: '2026-10-31' }]));
    // Maps already in the project: the template's untouched draft for Client,
    // reading from the old place, and a map somebody built by hand for
    // ClientType, with columns.
    localStorage.setItem('cygenix_jobs', JSON.stringify([
      { id: 'job_tpl_client', name: 'STG_Client → Client', jobType: 'simple-map', projectId: 'p1', source: 'dbo.STG_Client', sourceTable: 'dbo.STG_Client',
        target: 'dbo.Client', targetTable: 'dbo.Client', columnMapping: [], status: 'draft', created: new Date().toISOString(),
        fromTemplate: { templateId: 'tpl_a', module: 'Clients', tableId: 't1', version: 1 } },
      { id: 'job_hand_type', name: 'My client types', jobType: 'simple-map', projectId: 'p1', source: 'dbo.Customer', sourceTable: 'dbo.Customer',
        target: 'dbo.ClientType', targetTable: 'dbo.ClientType', columnMapping: [{ srcCol: 'CustNo', tgtCol: 'TypeCode' }], insertSQL: 'INSERT INTO dbo.ClientType (TypeCode) SELECT CustNo FROM dbo.Customer;',
        created: new Date().toISOString() }]));
  }, { U, token, SRC_CS, TGT_CS });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('dialog', (d) => d.accept());
  await page.goto('http://localhost:' + PORT + '/dev-console', { waitUntil: 'domcontentloaded' });
  await page.waitForFunction(() => !!window.CygenixCcConsole && typeof csNew === 'function' && !document.getElementById('cc-console').hidden, null, { timeout: 20000 });
  await page.waitForTimeout(500);
  if (await page.evaluate(() => document.getElementById('cs-notice').classList.contains('open'))) await page.click('#cs-notice-ok');
  const text = (id) => page.evaluate((i) => document.getElementById(i).textContent, id);
  const sessionPosts = () => world.calls.filter((c) => c.action === 'session' && c.method === 'POST');

  console.log('Dev Console — what a staging session is given, and Load into target\n');

  /* Choose the SOURCE: a staging build happens beside the legacy data. */
  await page.evaluate(() => {
    const sel = document.getElementById('cs-conn');
    const o = Array.from(sel.options).find((x) => /^Source/.test(x.textContent));
    sel.value = o.value; csPickConn();
  });
  check('the source connection is chosen', await page.evaluate(() => CS.conn && CS.conn.side === 'src' && CS.conn.connId === 'c_src'));
  check('NO TEMPLATE PICKER until a staging schema is typed', await page.evaluate(() => document.getElementById('cs-template').hidden));
  check('…and Load into target is off without a staging session', await page.evaluate(() => document.getElementById('cs-load-btn').disabled));

  /* C. The template version. */
  await page.fill('#cs-staging', 'stg');
  await page.waitForFunction(() => !document.getElementById('cs-template').hidden && document.getElementById('cs-template').options.length === 2, null, { timeout: 8000 });
  const pick = await page.evaluate(() => { const s = document.getElementById('cs-template');
    return { value: s.value, labels: Array.from(s.options).map((o) => o.textContent), warn: s.classList.contains('warn'), title: s.title }; });
  check('TEMPLATE PICKER: both versions offered, the newest PUBLISHED chosen by default',
    pick.value === 'pub_tpl_a_v2' && pick.labels.includes('Finance — v2 published (default)') && pick.labels.includes('Finance — v3 draft'), JSON.stringify(pick));
  check('…and it WARNS that the draft has unpublished edits Claude will not see', pick.warn && /draft of "Finance" \(v3\) has changes that are not published\. Claude will read v2/.test(pick.title), pick.title);
  check('the templates were read once, for the active project', world.tplCalls.filter((c) => c.action === 'template-list').length === 1 && world.tplCalls[0].projectId === 'p1', JSON.stringify(world.tplCalls));

  /* The confirmation names the template, the target and the rules. */
  await page.click('#cs-new');
  await page.waitForFunction(() => document.getElementById('cs-confirm').classList.contains('open'), null, { timeout: 8000 });
  const conf = await page.evaluate(() => document.getElementById('cs-confirm-text').textContent);
  check('THE CONFIRMATION NAMES THE TEMPLATE VERSION', /Finance/.test(conf) && /v2/.test(conf), conf);
  check('…SAYS CLAUDE CAN READ THE TARGET, and never change it', /Claude can READ the target, database NEWDB \(connection "New system"\), to check lookup values — never change it\./i.test(conf), conf);
  check('…SAYS THE RULES GO WITH IT', /Your 2 Was\/Is rule\(s\) and 1 Parameter\(s\) go with it\./.test(conf), conf);
  check('…and repeats the unpublished-edits warning', /not published/.test(conf), conf);
  await page.click('#cs-confirm-ok');
  await page.waitForFunction(() => !document.getElementById('cs-staging-pill').hidden, null, { timeout: 8000 });
  const sb = sessionPosts().pop().body;
  check('THE SESSION IS SENT the template chosen, the schema and the project', sb.templateId === 'pub_tpl_a_v2' && sb.stagingSchema === 'stg' && sb.projectId === 'p1' && sb.connId === 'c_src', JSON.stringify(sb));
  check('A. …the target as a REFERENCE: its id and name, read-only, NO SECRET',
    sb.reference && sb.reference.side === 'tgt' && sb.reference.connId === 'c_tgt' && sb.reference.connectionName === 'New system'
    && !/Pa55word|connString|fnKey|Password/i.test(JSON.stringify(sb)), JSON.stringify(sb.reference));
  check('B. …the Was/Is rules and the Parameters, as they are stored',
    sb.rules && sb.rules.wasis.length === 2 && sb.rules.wasis[0].newVal === 'ACTIVE' && sb.rules.params.length === 1 && sb.rules.params[0].code === '@@CUTOVER', JSON.stringify(sb.rules));
  const pill = await page.evaluate(() => ({ t: document.getElementById('cs-staging-pill').textContent, title: document.getElementById('cs-staging-pill').title }));
  check('THE PILL SAYS WHICH TEMPLATE VERSION THIS SESSION BUILDS FROM', pill.t === 'Staging: stg · Finance v2', pill.t);
  check('…and its tooltip names the target and the rule counts', /Reads the target "New system" \(read-only\)/.test(pill.title) && /2 Was\/Is rule\(s\), 1 Parameter\(s\)/.test(pill.title), pill.title);
  check('Load into target is ON for a staging session on the source', !(await page.evaluate(() => document.getElementById('cs-load-btn').disabled)));

  /* D. Load into target: the plan, confirmed. */
  await page.waitForTimeout(3100);
  await page.click('#cs-load-btn');
  await page.waitForFunction(() => document.getElementById('cs-confirm').classList.contains('open'), null, { timeout: 15000 }).catch(() => {});
  const lc = await page.evaluate(() => ({ open: document.getElementById('cs-confirm').classList.contains('open'), title: document.getElementById('cs-confirm-title').textContent,
    ok: document.getElementById('cs-confirm-ok').textContent, text: document.getElementById('cs-confirm-text').textContent }));
  check('LOAD INTO TARGET ASKS FIRST, with the template version and what it will do',
    lc.open && lc.title === 'Make the maps that load "stg" into the target?' && lc.ok === 'Make 2 maps'
    && /From template "Finance" v2: 1 new map, 1 template draft pointed at "stg", 1 left as they are, 1 not built in the schema\./.test(lc.text), JSON.stringify(lc) + ' note: ' + (await text('cs-note')));
  check('…it says nothing reaches the target until the jobs are run', /Nothing is loaded into the target until you run the jobs in Task Agent\./.test(lc.text));
  check('…and names the map it leaves alone, with the reason', /ClientType \(Your map "My client types" loads this table from dbo\.Customer — left as it is\.\)/.test(lc.text), lc.text);
  check('the template read was the PINNED version', world.tplCalls.some((c) => c.action === 'template-get' && c.id === 'pub_tpl_a_v2'), JSON.stringify(world.tplCalls));
  check('nothing was written before the confirmation', await page.evaluate(() => JSON.parse(localStorage.getItem('cygenix_jobs')).length === 2));

  /* The run: real Object Mapping pages in hidden frames. */
  await page.click('#cs-confirm-ok');
  const done = await page.waitForFunction(() => document.getElementById('cs-load').classList.contains('open'), null, { timeout: 120000 }).then(() => true).catch(() => false);
  check('THE MAPS ARE MADE AND THEIR SQL GENERATED — the results open', done, await text('cs-note'));
  const sum = await text('cs-load-sum');
  check('…2 of 2 ready to run', /^2 of 2 maps have SQL and are ready to run in Task Agent\./.test(sum), sum + ' | ' + (await text('cs-load-list')));
  const jobs = await page.evaluate(() => JSON.parse(localStorage.getItem('cygenix_jobs')));
  const byTarget = (t) => jobs.filter((j) => (j.targetTable || '').toLowerCase() === t.toLowerCase());
  const inv = byTarget('fin.Invoice')[0] || {};
  check('A NEW MAP for Invoice, from the staging schema, into the target\'s own schema, stamped with the template',
    inv.source === 'stg.STG_Invoice' && inv.fromTemplate && inv.fromTemplate.templateId === 'tpl_a' && inv.fromTemplate.stagingSchema === 'stg' && inv.projectId === 'p1', JSON.stringify(inv).slice(0, 400));
  check('…its columns matched by name and its SQL GENERATED',
    Array.isArray(inv.columnMapping) && inv.columnMapping.filter((m) => m.srcCol && m.tgtCol).length === 3 && /INSERT INTO/i.test(inv.insertSQL || '') && /fin.*Invoice/.test(inv.insertSQL || '') && /STG_Invoice/.test(inv.insertSQL || ''),
    (inv.insertSQL || '').slice(0, 300));
  const cli = jobs.find((j) => j.id === 'job_tpl_client') || {};
  check('THE TEMPLATE\'S UNTOUCHED DRAFT now reads from "stg", with its SQL', cli.sourceTable === 'stg.STG_Client' && /INSERT INTO/i.test(cli.insertSQL || '') && /STG_Client/.test(cli.insertSQL || ''), JSON.stringify(cli).slice(0, 400));
  // Object Mapping's save rebuilds the record from the form; it used to drop
  // the template stamp on every save, so the template lost track of its maps.
  check('…AND BOTH MAPS STILL CARRY THE TEMPLATE STAMP after Object Mapping saved them',
    cli.fromTemplate && cli.fromTemplate.templateId === 'tpl_a' && cli.fromTemplate.tableId === 't1' && cli.fromTemplate.stagingSchema === 'stg'
    && inv.fromTemplate && inv.fromTemplate.tableId === 't4', JSON.stringify([cli.fromTemplate, inv.fromTemplate]));
  const hand = jobs.find((j) => j.id === 'job_hand_type') || {};
  check('THE MAP SOMEBODY BUILT IS UNTOUCHED', hand.sourceTable === 'dbo.Customer' && hand.columnMapping.length === 1 && hand.insertSQL === 'INSERT INTO dbo.ClientType (TypeCode) SELECT CustNo FROM dbo.Customer;', JSON.stringify(hand));
  check('a table the session did not build gets no map', byTarget('dbo.ClientNote').length === 0 && byTarget('ClientNote').length === 0);
  check('the unticked module gets no map', !jobs.some((j) => /Ledger/.test(j.targetTable || '')));
  check('three maps in all — one made, none doubled', jobs.length === 3, jobs.map((j) => j.id + ':' + j.targetTable).join());
  const list = await text('cs-load-list');
  check('THE RESULTS LIST each map, the one left alone and the one not built', /Ready/.test(list) && /Left alone/.test(list) && /Not built/.test(list) && /STG_ClientNote → ClientNote/.test(list), list);
  check('…with the way to Jobs and to Schedules', await page.evaluate(() => !!document.querySelector('#cs-load a[href="/dashboard#goto=jobs"]') && !!document.querySelector('#cs-load a[href="/dashboard#goto=task-agent"]')));
  check('the hidden frames are gone', await page.evaluate(() => document.querySelectorAll('#cs-gen-host iframe').length === 0));
  check('NOTHING CALLED CLAUDE: the maps were matched by name', !world.calls.some((c) => c.action === 'ANTHROPIC'));
  // Object Mapping tests each connection with SELECT 1; anything else sent
  // to either database would be a write that no one confirmed.
  const notRead = world.db.filter((d) => !/^schema-/.test(d.action || '') && !(d.action === 'execute' && /^\s*SELECT\b/i.test(d.sql)));
  check('NOTHING WAS WRITTEN TO EITHER DATABASE: schema reads and SELECTs only', notRead.length === 0, JSON.stringify(notRead));
  check('…AND NO FRAME\'S REPORT WAS SENT TO CLAUDE AS A CHAT MESSAGE', !world.calls.some((c) => c.action === 'message'), JSON.stringify(world.calls.filter((c) => c.action === 'message')));

  /* Pressing it again only regenerates — no doubles. */
  await page.evaluate(() => document.getElementById('cs-load').classList.remove('open'));
  await page.waitForTimeout(3100);
  await page.click('#cs-load-btn');
  await page.waitForFunction(() => document.getElementById('cs-confirm').classList.contains('open'), null, { timeout: 15000 }).catch(() => {});
  const again = await page.evaluate(() => document.getElementById('cs-confirm-text').textContent);
  check('PRESSED AGAIN, it regenerates the two it made — it does not make them twice', /2 maps regenerated, 1 left as they are, 1 not built in the schema/.test(again), again);
  await page.click('#cs-confirm-cancel').catch(() => page.evaluate(() => document.getElementById('cs-confirm').classList.remove('open')));

  check('nothing threw', errors.length === 0, errors.slice(0, 3).join(' | '));
  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
