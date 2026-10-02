/* tests/browser/claude-code-console.smoke.js
 * ---------------------------------------------------------------------------
 * The Claude Code console in a real browser, with the Azure routes and the
 * roles record stubbed at the data-proxy. Walks the brief's browser checks:
 *   1. off for the organisation → the plain "not enabled" message, no calls;
 *   2. on but no API key → the message and the Settings link, no calls;
 *   3. allowed → first-use notice, dismissed once and remembered;
 *   4. Check the bridge → a pass from the Azure side, then SELECT 1 taken
 *      straight to the MCP bridge with that pass (not the Entra token);
 *   5. New session → Idle; a message → Working → tool blocks, a table, the
 *      redacted output → Idle, and the polling stops;
 *   6/7. the toggle: off → the confirmation; a production profile needs its
 *      name typed; on → the mode call and the notice in the chat;
 *   7. a quick double-click on New session starts one;
 *   8. Stop → Stopped, and no further /events calls;
 *   8. a query through the bridge shows its SQL and a table; a bridge query
 *      that cannot reach the database raises the connection note;
 *   11. the sessions drawer lists the session; a past one replays read-only;
 *   a staging session: the schema field, its confirmation, the project and
 *      schema sent, the pill, the suggested first message, the switch off;
 *   a failing route shows its error once; nothing throws.
 *
 * Run it by hand:  node tests/browser/claude-code-console.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8442);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

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
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const token = 'x.' + Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: U })).toString('base64url') + '.y';

  // The stubbed world: a roles record the page asks for, and a fake Azure.
  const world = { outputs: [], me: { oid: 'x', email: U, roles: ['ML'], claudeCode: { enabled: false, roles: ['OW', 'PA'], allowed: false, canChangeData: false, canConfigure: false } },
    calls: [], sessions: {}, nextEvents: [], failEvents: false, failStop: false, mcp: [], mcpFails: false, saved: [], reportJson: null };
  const json = (route, body, status) => route.fulfill({ status: status || 200, contentType: 'application/json', body: JSON.stringify(body) });
  await ctx.route('**', async (route) => {
    const u = route.request().url();
    const q = new URL(u, 'http://x').searchParams;
    const p = q.get('path') || '';
    if (/rbac-admin\?what=me/.test(u)) return json(route, world.me);
    if (/data-proxy/.test(u) && /^\/agent\/claude-code\//.test(p)) {
      const action = p.replace(/^\/agent\/claude-code\//, '').split('?')[0];
      const body = route.request().postData() ? JSON.parse(route.request().postData()) : {};
      world.calls.push({ action, method: route.request().method(), body, path: p, headers: route.request().headers() });
      if (action === 'session' && route.request().method() === 'POST') {
        const id = 'sesn_' + String(Object.keys(world.sessions).length + 1).padStart(6, '0');
        const s = { id, title: '', status: 'idle', dataChangesAllowed: false, connectionName: body.connectionName, profileName: body.profileName, createdAt: new Date().toISOString(), costCents: 0, stagingSchema: body.stagingSchema || '',
          side: body.side, dbName: 'tgt', dbHost: 'acme.database.windows.net', dbType: 'sqlserver' };
        world.sessions[id] = { session: s, events: [] };
        return json(route, { session: s });
      }
      if (action === 'message') { const s = world.sessions[body.sessionId]; s.session.status = 'running'; if (!s.session.title) s.session.title = body.text.slice(0, 80);
        s.events.push({ id: 'u' + s.events.length, type: 'user.message', content: [{ type: 'text', text: body.text }] }); return json(route, { ok: true, title: s.session.title, status: 'running' }); }
      if (action === 'events') {
        if (world.failEvents) return json(route, { error: 'Anthropic is rate-limiting this account. Wait a minute and try again.' }, 429);
        const s = world.sessions[q.get('sessionId') || new URL('http://x' + p).searchParams.get('sessionId')];
        const fresh = world.nextEvents.splice(0); fresh.forEach((e) => s.events.push(e));
        if (!world.nextEvents.length && fresh.some((e) => e.type === 'session.status_idle')) s.session.status = 'idle';
        return json(route, { status: s.session.status, events: fresh, costCents: 7, dataChangesAllowed: s.session.dataChangesAllowed, done: false });
      }
      if (action === 'mode') { world.sessions[body.sessionId].session.dataChangesAllowed = body.dataChangesAllowed; return json(route, { ok: true, dataChangesAllowed: body.dataChangesAllowed }); }
      if (action === 'stop') { if (world.failStop) return json(route, { error: 'boom' }, 500); world.sessions[body.sessionId].session.status = 'stopped'; return json(route, { ok: true, status: 'stopped' }); }
      if (action === 'check') return json(route, { token: 'cyb_0123456789abcdef01.' + 'A'.repeat(43), mcpUrl: 'https://cygenix.co.uk/.netlify/functions/cc-mcp', expiresInSeconds: 120 });
      if (action === 'sessions') return json(route, { sessions: Object.values(world.sessions).map((x) => x.session).reverse() });
      if (action === 'upload') { const s = world.sessions[body.sessionId]; const up = { fileId: 'file_u' + (s.session.uploads || []).length, name: body.name, path: '/workspace/uploads/' + body.name, size: Buffer.from(body.contentBase64, 'base64').length, at: new Date().toISOString() };
        s.session.uploads = (s.session.uploads || []).concat([up]); return json(route, { upload: up }); }
      if (action === 'outputs') { const s = world.sessions[new URL('http://x' + p).searchParams.get('sessionId')]; return json(route, { outputs: world.outputs, uploads: (s && s.session.uploads) || [] }); }
      if (action === 'download' && /fileId=file_rep/.test(p)) return json(route, { name: 'conversion-report.json', size: 10, contentBase64: Buffer.from(JSON.stringify(world.reportJson)).toString('base64') });
      if (action === 'download') return json(route, { name: 'counts.csv', size: 24, contentBase64: Buffer.from('table,rows\nCustomer,1200\n').toString('base64') });
      if (action === 'session') { const s = world.sessions[new URL('http://x' + p).searchParams.get('id')]; return s ? json(route, { session: s.session, events: s.events }) : json(route, { error: 'No such session.' }, 404); }
      return json(route, { error: 'unexpected ' + action }, 500);
    }
    if (/\/\.netlify\/functions\/reports/.test(u)) {
      world.saved.push({ headers: route.request().headers(), body: JSON.parse(route.request().postData() || '{}') });
      return json(route, { id: 'rpt_1', savedCount: 1, prunedCount: 0 });
    }
    if (/\/\.netlify\/functions\/cc-mcp/.test(u)) {
      world.mcp.push({ headers: route.request().headers(), body: JSON.parse(route.request().postData() || '{}') });
      const result = world.mcpFails
        ? { content: [{ type: 'text', text: 'The query failed: Login failed for user \'claude_api\'.' }], isError: true }
        : { content: [{ type: 'text', text: JSON.stringify({ columns: ['db'], rows: [['tgt']], row_count_returned: 1, truncated: false, ms: 4 }) }], isError: false };
      return json(route, { jsonrpc: '2.0', id: 1, result });
    }
    if (/action=whoami/.test(u)) return json(route, { tier: 'pro', tier_status: 'active', role: 'user' });
    // A save the stub does not confirm reads as a refusal, and the sync
    // banner it raises would sit over the composer.
    if (/data-proxy/.test(u) && q.get('action') === 'save') return json(route, { saved: true, updatedAt: new Date().toISOString(), fields: [], ignored: [] });
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
    localStorage.setItem('cygenix_cookie_consent', JSON.stringify({ version: '2', essential: true, functional: true, analytics: false, timestamp: new Date().toISOString() }));
    localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify(acct));
    localStorage.setItem('h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-',
      JSON.stringify({ credentialType: 'IdToken', secret: arg.token, expiresOn: String(Math.floor(Date.now() / 1000) + 3600) }));
    localStorage.setItem('cygenix_projects', JSON.stringify([{ id: 'p1', name: 'Demo' }]));
    localStorage.setItem('cygenix_active_project_id', 'p1');
    const cs = 'Server=tcp:acme.database.windows.net,1433;Database=tgt;User ID=u;Password=Tr0ub4dor';
    const live = {}; live[arg.U] = { srcConnString: 'Server=src.example.test;Database=s', srcConnMode: 'direct', tgtConnString: cs, tgtConnMode: 'direct' };
    localStorage.setItem('cygenix_project_connections', JSON.stringify(live));
    const blob = {}; blob[arg.U] = [{ id: 'c_src', side: 'src', mode: 'direct', name: 'Legacy' }, { id: 'c_tgt', side: 'tgt', mode: 'direct', name: 'Target DEV' }];
    localStorage.setItem('cygenix_saved_connections', JSON.stringify(blob));
    localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify({ c_src: { connString: 'Server=src.example.test;Database=s' }, c_tgt: { connString: cs } }));
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify({ v: 1, connMeta: { c_src: { envClass: arg.env }, c_tgt: { envClass: arg.env } }, bindings: [], runRecords: [], events: [],
      profiles: [{ id: 'DEMO', name: 'Demo', envClass: arg.env, status: 'active', srcConnId: 'c_src', tgtConnId: 'c_tgt', createdAt: 1, updatedAt: 1 }],
      settings: { envClasses: ['DEV', 'TEST', 'UAT', 'PRD', 'SANDBOX'], activeProfileId: 'DEMO', selectedAt: 1 } }));
  }, { U, token, env: 'DEV' });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  const open = async () => {
    await page.goto('http://localhost:' + PORT + '/dev-console', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => !!window.CygenixCcConsole && typeof csNew === 'function' && !!document.getElementById('cs-status'), null, { timeout: 20000 });
    await page.waitForTimeout(500);
  };
  const calls = (a) => world.calls.filter((c) => c.action === a);
  const text = (id) => page.evaluate((i) => document.getElementById(i).textContent, id);

  console.log('Claude Code — the console\n');

  /* 1. Off for the organisation. */
  await open();
  check('switched off for the organisation: the plain message, the console hidden, nothing called',
    /Not enabled for your role — ask an Owner to enable it in Governance\./.test(await text('cc-gate-text'))
    && (await page.evaluate(() => document.getElementById('cc-console').hidden)) && world.calls.length === 0, await text('cc-gate-text'));
  check('…and New session is off', await page.evaluate(() => document.getElementById('cs-new').disabled));

  /* 2. On, but no key. */
  world.me.claudeCode = { enabled: true, roles: ['OW', 'PA', 'ML'], allowed: true, canChangeData: true, canConfigure: false };
  await page.evaluate(() => sessionStorage.removeItem('cygenix_rbac_me'));
  await open();
  check('allowed but no API key: the Settings link, the console hidden, nothing called',
    !(await page.evaluate(() => document.getElementById('cc-key-note').hidden))
    && !!(await page.$('#cc-key-note a[href="/dashboard#project-settings"]')) && world.calls.length === 0);

  /* 3. Allowed. */
  await page.evaluate(() => { localStorage.setItem('cygenix_api_key', 'sk-ant-smoke-key'); sessionStorage.removeItem('cygenix_rbac_me'); });
  await open();
  check('the console shows, and the first-use notice with the brief\'s words',
    !(await page.evaluate(() => document.getElementById('cc-console').hidden))
    && (await page.evaluate(() => document.getElementById('cs-notice').classList.contains('open')))
    && /Runs on your own Anthropic API key and is billed to your Anthropic account\. Code runs in an isolated Anthropic workspace\. You are responsible for changes it makes\./.test(await text('cs-notice')));
  await page.click('#cs-notice-ok');
  await page.evaluate(() => sessionStorage.removeItem('cygenix_rbac_me'));
  await open();
  check('dismissed once, the notice is remembered for this user', !(await page.evaluate(() => document.getElementById('cs-notice').classList.contains('open'))));
  const opts = await page.evaluate(() => Array.from(document.getElementById('cs-conn').options).map((o) => o.textContent + (o.disabled ? ' (off)' : '')));
  check('the picker lists the active profile\'s connections, target first, each with its database', opts[0] === 'Target — Target DEV · tgt' && opts[1] === 'Source — Legacy · s', opts.join(' | '));
  const fits = () => page.evaluate(() => {
    const w = document.documentElement.clientWidth;
    return Array.from(document.querySelectorAll('.topbar > *')).filter((el) => el.offsetParent !== null)
      .filter((el) => { const r = el.getBoundingClientRect(); return r.right > w + 1 || r.left < 0; }).map((el) => el.id || el.className);
  });
  const widths = () => page.evaluate(() => Array.from(document.querySelectorAll('.topbar > *')).filter((el) => el.offsetParent !== null)
    .map((el) => (el.id || el.className) + ':' + Math.round(el.getBoundingClientRect().width)).join(' '));
  let off = await fits();
  check('EVERY TOOLBAR CONTROL IS ON SCREEN at 1440px, staging field included', off.length === 0, off.join() + ' — ' + (await widths()));
  const oneRow = await page.evaluate(() => Math.round(document.querySelector('.topbar').getBoundingClientRect().height));
  check('…on one row, before a session', oneRow <= 53, oneRow + 'px');
  check('status: No session; Stop and Send off', (await text('cs-status')) === 'No session'
    && (await page.evaluate(() => document.getElementById('cs-stop').disabled && document.getElementById('cs-send').disabled)));

  /* Which database, before a session: from the chosen connection. */
  const banner = () => page.evaluate(() => ({ side: document.getElementById('cs-db-side').textContent, name: document.getElementById('cs-db-name').textContent,
    detail: document.getElementById('cs-db-detail').textContent, bg: getComputedStyle(document.getElementById('cs-dbbar')).backgroundColor,
    fg: getComputedStyle(document.getElementById('cs-db-name')).color, size: parseFloat(getComputedStyle(document.getElementById('cs-db-name')).fontSize) }));
  let bb = await banner();
  check('THE DATABASE IS NAMED IN RED ON YELLOW, large: side, database, host and connection',
    bb.side === 'TARGET' && bb.name === 'TGT' && /on acme\.database\.windows\.net/.test(bb.detail) && /connection "Target DEV"/.test(bb.detail)
    && bb.bg === 'rgb(255, 241, 118)' && bb.fg === 'rgb(176, 0, 0)' && bb.size >= 20, JSON.stringify(bb));
  check('…and the picker names the database too', opts[0] === 'Target — Target DEV · tgt' || (await page.evaluate(() => document.getElementById('cs-conn').options[0].textContent)) === 'Target — Target DEV · tgt',
    await page.evaluate(() => document.getElementById('cs-conn').options[0].textContent));
  await page.selectOption('#cs-conn', '1');
  bb = await banner();
  check('…and changes the moment another connection is picked', bb.side === 'SOURCE' && bb.name === 'S' && /src\.example\.test/.test(bb.detail), JSON.stringify(bb));
  await page.selectOption('#cs-conn', '0');

  /* 4. Check the bridge. */
  await page.click('#cs-check');
  await page.waitForFunction(() => /The bridge works/.test(document.getElementById('cs-note').textContent), null, { timeout: 8000 });
  const chkCall = calls('check')[0] || { body: {} };
  check('CHECK THE BRIDGE asks for a pass for the chosen connection, by id — no string, no password',
    calls('check').length === 1 && chkCall.method === 'POST' && chkCall.body.connId === 'c_tgt' && chkCall.body.mode === 'direct'
    && !JSON.stringify(chkCall.body).includes('Tr0ub4dor'), JSON.stringify(chkCall.body));
  const m0 = world.mcp[0] || { headers: {}, body: {} };
  check('…then runs SELECT 1 through the bridge, carrying THE PASS, not the Entra token',
    world.mcp.length === 1 && m0.headers.authorization === 'Bearer cyb_0123456789abcdef01.' + 'A'.repeat(43)
    && m0.body.method === 'tools/call' && m0.body.params.name === 'run_query' && m0.body.params.arguments.sql === 'SELECT DB_NAME() AS db', JSON.stringify(m0));
  check('…and says WHICH DATABASE answered', /The bridge works: "Target DEV" is database TGT, answered in \d+ ms/.test(await text('cs-note')), await text('cs-note'));
  await page.waitForTimeout(3100);
  world.mcpFails = true;
  await page.click('#cs-check');
  await page.waitForFunction(() => /could not query/.test(document.getElementById('cs-note').textContent), null, { timeout: 8000 });
  check('a bridge that cannot log in says why, without the "The query failed" prefix',
    /The bridge could not query "Target DEV": Login failed for user 'claude_api'\./.test(await text('cs-note')), await text('cs-note'));
  world.mcpFails = false;

  /* 5 + 7. New session, double-clicked. */
  await page.evaluate(() => { csNew(); csNew(); });
  await page.waitForFunction(() => document.getElementById('cs-status').textContent === 'Idle', null, { timeout: 10000 });
  check('A QUICK DOUBLE-CLICK ON NEW SESSION OPENS ONE', calls('session').filter((c) => c.method === 'POST').length === 1);
  const sb = calls('session')[0].body;
  check('it sends the connection id and names — never a connection string or password',
    sb.connId === 'c_tgt' && sb.connectionName === 'Target DEV' && sb.profileName === 'Demo' && sb.side === 'tgt' && sb.mode === 'direct' && sb.fnUrl === ''
    && !JSON.stringify(world.calls).includes('Tr0ub4dor') && !JSON.stringify(world.calls).includes('acme.database'), JSON.stringify(sb));
  check('the caller\'s key and token ride in the headers', /sk-ant-smoke-key/.test(calls('session')[0].headers['x-anthropic-key']) && /^Bearer /.test(calls('session')[0].headers.authorization));
  check('Idle: Send is on, Stop is OFF (nothing to stop)', (await text('cs-status')) === 'Idle' && (await page.evaluate(() => !document.getElementById('cs-send').disabled && document.getElementById('cs-stop').disabled)));

  /* A message, then the events. */
  world.nextEvents = [
    { id: 'e1', type: 'agent.tool_use', name: 'write', input: { path: '/workspace/count.py', content: 'import os, pymssql\nprint("hi")' } },
    { id: 'e2', type: 'agent.tool_result', tool_use_id: 'e1', content: [{ type: 'text', text: 'ok' }] },
    { id: 'e3', type: 'agent.tool_use', name: 'bash', input: { command: 'python3 count.py' } },
    { id: 'e4', type: 'agent.tool_result', tool_use_id: 'e3', content: [{ type: 'text', text: 'table,rows\nCustomer,1200\nInvoice,48210\n' }] },
  ];
  await page.fill('#cs-input', 'List the tables and row counts using Python');
  await page.keyboard.press('Control+Enter');
  await page.waitForFunction(() => document.getElementById('cs-status').textContent === 'Working', null, { timeout: 5000 });
  check('Ctrl+Enter sends, the message shows at once, status Working',
    calls('message').length === 1 && calls('message')[0].body.text === 'List the tables and row counts using Python'
    && /List the tables and row counts/.test(await text('cs-chat')) && /Working…/.test(await text('cs-chat')));
  await page.waitForFunction(() => document.querySelectorAll('#cs-output details.block').length >= 2, null, { timeout: 8000 });
  const out = await page.evaluate(() => ({ titles: Array.from(document.querySelectorAll('#cs-output details.block > summary')).map((s) => s.textContent.trim()),
    table: !!document.querySelector('#cs-output table.res'), cells: Array.from(document.querySelectorAll('#cs-output table.res td')).map((td) => td.textContent) }));
  check('the output panel shows the code written and the command run, as collapsible blocks',
    out.titles[0] === 'Wrote /workspace/count.py' && out.titles[1] === 'Command run', out.titles.join(' | '));
  check('AND A QUERY RESULT IS SHOWN AS A TABLE', out.table && out.cells.join() === 'Customer,1200,Invoice,48210', out.cells.join());
  const pollsMid = calls('events').length;
  world.nextEvents = [
    { id: 'e5', type: 'agent.message', content: [{ type: 'text', text: 'There are 2 tables: Customer (1,200 rows) and Invoice (48,210 rows).' }] },
    { id: 'e6', type: 'session.status_idle', stop_reason: { type: 'end_turn' } },
  ];
  await page.waitForFunction(() => document.getElementById('cs-status').textContent === 'Idle', null, { timeout: 8000 });
  check('Claude\'s reply lands in the chat, the cost shows, status Idle', /There are 2 tables/.test(await text('cs-chat')) && (await text('cs-cost')) === 'US$0.07');
  const pollsAtIdle = calls('events').length;
  await page.waitForTimeout(7000);
  check('POLLING STOPS WHEN THE SESSION IS IDLE — no further /events calls', calls('events').length === pollsAtIdle && pollsAtIdle > pollsMid, calls('events').length + ' vs ' + pollsAtIdle);

  /* Phase 2: attach a file, and get one back. */
  await page.setInputFiles('#cs-file', { name: 'orders.csv', mimeType: 'text/csv', buffer: Buffer.from('id,name\n1,Ann\n') });
  await page.waitForFunction(() => /Attached orders\.csv/.test(document.getElementById('cs-chat').textContent), null, { timeout: 8000 });
  const up = calls('upload')[0];
  check('ATTACHING A FILE sends its name and content, and the chat shows where it landed',
    !!up && up.body.name === 'orders.csv' && Buffer.from(up.body.contentBase64, 'base64').toString() === 'id,name\n1,Ann\n'
    && /\/workspace\/uploads\/orders\.csv/.test(await text('cs-chat')), JSON.stringify(up && up.body).slice(0, 200));
  check('the Files panel opens and lists it', !(await page.evaluate(() => document.getElementById('cs-files').hidden)) && /orders\.csv/.test(await text('cs-uploads')));
  world.outputs = [{ id: 'file_o1', name: 'counts.csv', size: 24, at: new Date().toISOString() }];
  await page.waitForTimeout(3100);
  await page.click('#cs-files .ct-link');
  await page.waitForFunction(() => /counts\.csv/.test(document.getElementById('cs-outputs').textContent), null, { timeout: 8000 });
  const [dl] = await Promise.all([page.waitForEvent('download', { timeout: 8000 }), page.click('#cs-outputs button')]);
  check('A FILE CLAUDE WROTE downloads with its name', dl.suggestedFilename() === 'counts.csv' && calls('download').length === 1 && /fileId=file_o1/.test(calls('download')[0].path));
  await page.waitForTimeout(3100);

  /* 6. The toggle, on a DEV profile. */
  await page.click('#cs-toggle');
  check('the toggle asks first, naming the profile and the connection; no production name needed on DEV',
    (await page.evaluate(() => document.getElementById('cs-confirm').classList.contains('open')))
    && /"Target DEV" under profile "Demo" \(DEV\)/.test(await text('cs-confirm-text'))
    && (await page.evaluate(() => document.getElementById('cs-confirm-input').hidden && !document.getElementById('cs-confirm-ok').disabled)), await text('cs-confirm-text'));
  await page.click('#cs-confirm-ok');
  await page.waitForFunction(() => document.getElementById('cs-toggle').classList.contains('on'), null, { timeout: 5000 });
  check('confirmed: the mode call goes out and the chat says changes are allowed',
    calls('mode').length === 1 && calls('mode')[0].body.dataChangesAllowed === true && /Changes to data are now allowed/.test(await text('cs-chat')));
  await page.waitForTimeout(3100);
  await page.click('#cs-toggle');
  await page.waitForFunction(() => !document.getElementById('cs-toggle').classList.contains('on'), null, { timeout: 5000 });
  check('switching it off needs no confirmation and tells Claude', calls('mode').length === 2 && calls('mode')[1].body.dataChangesAllowed === false && /read-only again/.test(await text('cs-chat')));

  /* 8. Queries through the bridge; one that cannot reach the database raises the note. */
  world.nextEvents = [
    { id: 'm1', type: 'agent.mcp_tool_use', name: 'run_query', mcp_server_name: 'cygenix', input: { sql: 'SELECT name FROM sys.tables' } },
    { id: 'm2', type: 'agent.mcp_tool_result', mcp_tool_use_id: 'm1', is_error: false, content: [{ type: 'text', text: JSON.stringify({ columns: ['name'], rows: [['Ledger'], [null]], row_count_returned: 2, truncated: false }) }] },
    { id: 'e7', type: 'agent.mcp_tool_use', name: 'run_query', mcp_server_name: 'cygenix', input: { sql: 'SELECT 1' } },
    { id: 'e8', type: 'agent.mcp_tool_result', mcp_tool_use_id: 'e7', is_error: true, content: [{ type: 'text', text: 'The query failed: connect ETIMEDOUT 10.0.0.1:1433' }] },
    { id: 'e9', type: 'session.status_idle', stop_reason: { type: 'end_turn' } },
  ];
  await page.waitForTimeout(3100);
  await page.fill('#cs-input', 'Run the query');
  await page.click('#cs-send');
  await page.waitForFunction(() => !document.getElementById('cs-firewall').hidden, null, { timeout: 8000 });
  check('A BRIDGE QUERY THAT CANNOT REACH THE DATABASE RAISES THE CONNECTION NOTE',
    /Cygenix could not reach the database\. The SQL editor uses the same route, so check the connection there first\./.test(await text('cs-firewall')), await text('cs-firewall'));
  const bq = await page.evaluate(() => {
    const blocks = Array.from(document.querySelectorAll('#cs-output details.block'));
    const b = blocks.find((d) => /SELECT name FROM sys\.tables/.test(d.textContent));
    return b ? { title: b.querySelector('summary').textContent.trim(), cells: Array.from(b.querySelectorAll('table.res td')).map((td) => td.textContent) } : null;
  });
  check('A QUERY THROUGH THE BRIDGE shows as "Query run" with its SQL, and its answer as a table (NULL shown)',
    !!bq && /Query run/.test(bq.title) && bq.cells.join() === 'Ledger,NULL', JSON.stringify(bq));

  /* 8. Stop. */
  await page.waitForTimeout(3100);
  world.nextEvents = [{ id: 'e10', type: 'agent.thinking' }];
  await page.fill('#cs-input', 'Try again');
  await page.click('#cs-send');
  await page.waitForFunction(() => document.getElementById('cs-status').textContent === 'Working', null, { timeout: 5000 });
  await page.click('#cs-stop');
  await page.waitForFunction(() => document.getElementById('cs-status').textContent === 'Stopped', null, { timeout: 5000 });
  const pollsAtStop = calls('events').length;
  await page.waitForTimeout(7000);
  check('STOP: status Stopped, the stop call made, and no further /events calls',
    calls('stop').length === 1 && calls('events').length === pollsAtStop, calls('events').length + ' vs ' + pollsAtStop);
  check('after Stop, Send and the toggle are off', await page.evaluate(() => document.getElementById('cs-send').disabled && document.getElementById('cs-toggle').classList.contains('disabled')));

  /* 11. The drawer and a replay. */
  await page.click('#cs-sessions-btn');
  await page.waitForFunction(() => document.querySelectorAll('#cs-drawer-list .sess').length >= 1, null, { timeout: 5000 });
  const row = await page.evaluate(() => document.querySelector('#cs-drawer-list .sess').textContent);
  check('the drawer lists the session with its title, connection and status', /List the tables and row counts using Python/.test(row) && /Target DEV/.test(row) && /Stopped/.test(row), row);
  await page.evaluate(() => { CS.cur = null; CS.events = []; CS.seen = {}; render(); });
  await page.click('#cs-drawer-list .sess');
  await page.waitForFunction(() => !document.getElementById('cs-replay-bar').hidden, null, { timeout: 5000 });
  check('a past session replays read-only: the bar, the transcript, Send off',
    /Replaying/.test(await text('cs-replay-text')) && /List the tables and row counts/.test(await text('cs-chat'))
    && (await page.evaluate(() => document.getElementById('cs-send').disabled && document.getElementById('cs-input').disabled))
    && calls('session').some((c) => c.method === 'GET'));
  check('and the replay came from the server, not this browser',
    await page.evaluate(() => !Object.keys(localStorage).some((k) => /transcript|cc_events|cc_session/.test(k))));

  /* A failing route. */
  await page.evaluate(() => csReplayBack());
  await page.waitForTimeout(3100);
  await page.evaluate(() => csNew());
  await page.waitForFunction(() => document.getElementById('cs-status').textContent === 'Idle', null, { timeout: 8000 });
  world.failEvents = true;
  await page.waitForTimeout(3100);
  await page.fill('#cs-input', 'hello');
  await page.click('#cs-send');
  await page.waitForFunction(() => /Lost touch with the session/.test(document.getElementById('cs-note').textContent), null, { timeout: 8000 });
  const pollsFail = calls('events').length;
  await page.waitForTimeout(7000);
  check('a failing result read shows its error once and stops — no retry storm',
    /rate-limiting/.test(await text('cs-note')) && calls('events').length === pollsFail, calls('events').length - pollsFail);

  /* A staging session. */
  world.failEvents = false;
  page.on('dialog', (d) => d.accept());              // "still working — start a new one?"
  await page.waitForTimeout(3100);
  await page.fill('#cs-staging', 'dbo');
  const postsBefore = calls('session').filter((c) => c.method === 'POST').length;
  await page.click('#cs-new');
  await page.waitForTimeout(300);
  check('a staging schema that is the database\'s own is refused on the page, before any call',
    /own schemas/.test(await text('cs-note')) && calls('session').filter((c) => c.method === 'POST').length === postsBefore, await text('cs-note'));
  await page.fill('#cs-staging', 'staging');
  await page.click('#cs-new');
  await page.waitForFunction(() => document.getElementById('cs-confirm').classList.contains('open'), null, { timeout: 5000 });
  const sc = await page.evaluate(() => ({ title: document.getElementById('cs-confirm-title').textContent, ok: document.getElementById('cs-confirm-ok').textContent,
    text: document.getElementById('cs-confirm-text').textContent }));
  check('STARTING A STAGING SESSION ASKS FIRST, saying what it allows and where',
    sc.title === 'Start a staging session?' && sc.ok === 'Start staging session' && /inside the schema "staging" of database TGT \(connection "Target DEV"\) under profile "Demo" \(DEV\)/.test(sc.text)
    && /stays read-only/.test(sc.text), JSON.stringify(sc));
  await page.click('#cs-confirm-ok');
  await page.waitForFunction(() => !document.getElementById('cs-staging-pill').hidden, null, { timeout: 8000 });
  const sb2 = calls('session').filter((c) => c.method === 'POST').pop().body;
  check('…it sends the schema and the active project with the connection — still no secret',
    sb2.stagingSchema === 'staging' && sb2.projectId === 'p1' && sb2.connId === 'c_tgt' && !JSON.stringify(sb2).includes('Tr0ub4dor'), JSON.stringify(sb2));
  check('…the pill says it is a staging session, the field stays free for the NEXT session, and the first message is suggested, not sent',
    (await text('cs-staging-pill')) === 'Staging: staging' && !(await page.evaluate(() => document.getElementById('cs-staging').disabled))
    && /Conversion Template/.test(await page.evaluate(() => document.getElementById('cs-input').value))
    && (await page.evaluate(() => document.getElementById('cs-toggle').classList.contains('disabled'))), await text('cs-staging-pill'));
  off = await fits();
  await page.evaluate(() => { window.scrollTo(0, 0); document.scrollingElement.scrollTop = 0; });
  await page.waitForTimeout(100);
  const under = await page.evaluate(() => Math.round(document.getElementById('cc-console').getBoundingClientRect().top - document.querySelector('.topbar').getBoundingClientRect().bottom));
  check('…and with the staging pill showing everything is still on screen, the console starting right under the toolbar however tall it is',
    off.length === 0 && Math.abs(under) <= 2, off.join() + ' gap ' + under + ' — ' + (await widths()));
  const modes = calls('mode').length;
  await page.evaluate(() => csToggle());
  check('…and the "Allow changes" switch explains it does not apply, calling nothing', /This is a staging session/.test(await text('cs-note')) && calls('mode').length === modes);
  bb = await banner();
  check('IN THE SESSION, THE BANNER IS THE SERVER\'S READING, with the staging schema', bb.name === 'TGT' && /staging schema "staging"/.test(bb.detail), JSON.stringify(bb));

  /* Save report: Claude has not written it, so it is asked; when the turn ends it is saved by itself. */
  world.outputs = [];
  world.reportJson = { template: { name: 'Demo Conversion Template', version: 2 }, summary: 'Two tables loaded.', warnings: [],
    tables: [{ staging_table: 'STG_addresses', target_table: 'addresses', source_tables: ['dbo.Address'], rows_loaded: 2600, rows_expected: 2600, status: 'loaded',
      columns: [{ column: 'Street', source: 'a.Line1', transform: '', notes: '' }] },
      { staging_table: 'STG_client', target_table: 'client', source_tables: ['dbo.Client'], rows_loaded: 121, rows_expected: 121, status: 'loaded', columns: [] }] };
  await page.waitForTimeout(3100);
  const msgsBefore = calls('message').length;
  world.nextEvents = [{ id: 'r1', type: 'agent.message', content: [{ type: 'text', text: 'The report is written.' }] }, { id: 'r2', type: 'session.status_idle', stop_reason: { type: 'end_turn' } }];
  await page.click('#cs-report-btn');
  await page.waitForFunction(() => /Claude is writing the report/.test(document.getElementById('cs-note').textContent), null, { timeout: 8000 });
  const ask = calls('message')[msgsBefore];
  check('SAVE REPORT, NOTHING WRITTEN YET: Claude is asked once, for conversion-report.json',
    calls('message').length === msgsBefore + 1 && /conversion-report\.json/.test(ask.body.text) && /"rows_loaded"/.test(ask.body.text), ask && ask.body.text.slice(0, 120));
  world.outputs = [{ id: 'file_rep', name: 'conversion-report.json', size: 900, at: new Date().toISOString() }];
  await page.waitForFunction(() => /Saved to Reports → Conversion Report/.test(document.getElementById('cs-note').textContent), null, { timeout: 15000 });
  const sv = world.saved[0] || { body: {}, headers: {} };
  check('…AND WHEN CLAUDE FINISHES IT IS SAVED to the reports function, with the person\'s token, in the Conversion Report shape',
    world.saved.length === 1 && sv.body.action === 'save' && /^Bearer /.test(sv.headers.authorization || '') && sv.body.report.isProjectReport === true
    && sv.body.report.totalRows === 2721 && sv.body.report.steps.length === 2 && sv.body.report.tables[0].name === 'staging.STG_addresses'
    && sv.body.report.sourceDatabase === 'tgt' && sv.body.report.projectId === 'p1' && sv.body.report.devConsole.stagingSchema === 'staging', JSON.stringify(sv.body).slice(0, 400));
  check('…and says so with the numbers', /2 tables, 2,721 rows/.test(await text('cs-note')), await text('cs-note'));
  await page.waitForTimeout(3100);
  await page.click('#cs-report-btn');
  await page.waitForFunction(() => document.getElementById('cs-note').textContent && !document.getElementById('cs-report-btn').disabled, null, { timeout: 8000 });
  await page.waitForTimeout(800);
  check('pressed again with the file already there, it saves straight away without asking Claude', world.saved.length === 2 && calls('message').length === msgsBefore + 1, world.saved.length);

  /* PRD needs the name typed. */
  await ctx.addInitScript(() => { try { const s = JSON.parse(localStorage.getItem('cygenix_profiles_v1')); s.profiles[0].envClass = 'PRD'; localStorage.setItem('cygenix_profiles_v1', JSON.stringify(s)); } catch (e) {} });
  const page2 = await ctx.newPage();
  page2.on('pageerror', (e) => errors.push(e.message));
  await page2.goto('http://localhost:' + PORT + '/dev-console', { waitUntil: 'domcontentloaded' });
  await page2.waitForFunction(() => typeof csToggle === 'function' && !document.getElementById('cc-console').hidden, null, { timeout: 20000 });
  await page2.waitForTimeout(400);
  await page2.click('#cs-toggle');
  const prod = await page2.evaluate(() => ({ open: document.getElementById('cs-confirm').classList.contains('open'), input: !document.getElementById('cs-confirm-input').hidden,
    okOff: document.getElementById('cs-confirm-ok').disabled, prod: !document.getElementById('cs-confirm-prod').hidden }));
  check('A PRODUCTION PROFILE ASKS FOR ITS NAME, with Allow off until it is typed', prod.open && prod.input && prod.okOff && prod.prod, JSON.stringify(prod));
  await page2.fill('#cs-confirm-input', 'Dem');
  check('a wrong name keeps Allow off', await page2.evaluate(() => document.getElementById('cs-confirm-ok').disabled));
  await page2.fill('#cs-confirm-input', 'Demo');
  check('the right name turns it on', !(await page2.evaluate(() => document.getElementById('cs-confirm-ok').disabled)));
  await page2.close();

  /* No empty strip: the console starts right under its toolbar. */
  const gap = () => page.evaluate(() => { window.scrollTo(0, 0); document.scrollingElement.scrollTop = 0; return null; }).then(() => page.waitForTimeout(100)).then(() => page.evaluate(() => Math.round(document.getElementById('cc-console').getBoundingClientRect().top - document.querySelector('.topbar').getBoundingClientRect().bottom)));
  const g1 = await gap();
  check('NO EMPTY STRIP under the toolbar', Math.abs(g1) <= 2, g1 + 'px');

  /* Full screen and the draggable split, on the first page. */
  await page.evaluate(() => csFull());
  const g2 = await gap();
  const top2 = await page.evaluate(() => Math.round(document.querySelector('.topbar').getBoundingClientRect().top));
  check('…nor in full screen, where the toolbar sits at the very top', Math.abs(g2) <= 2 && top2 === 0, g2 + 'px gap, toolbar at ' + top2);
  check('FULL SCREEN hides the chrome and relabels the button, remembered for the tab',
    await page.evaluate(() => document.body.classList.contains('cs-full') && document.getElementById('cs-full-label').textContent === 'Exit full screen'
      && sessionStorage.getItem('cygenix_cc_full') === '1'));
  await page.keyboard.press('Escape');
  check('Esc exits full screen', await page.evaluate(() => !document.body.classList.contains('cs-full') && document.getElementById('cs-full-label').textContent === 'Full screen'));
  await page.evaluate(() => applySplit(520));
  check('the divider sets the chat column width and it can be reset',
    await page.evaluate(() => {
      const cols = document.querySelector('.cols');
      const set = getComputedStyle(cols).getPropertyValue('--cs-chat').trim();
      csDragReset();
      const cleared = cols.style.getPropertyValue('--cs-chat');
      return /px/.test(set) && !cleared;
    }));
  check('Enter sends and Shift+Enter does not', await page.evaluate(() => {
    return typeof csInputKey === 'function'
      && (function(){ let sent = false; const orig = window.csSend; window.csSend = function(){ sent = true; }; csInputKey({ key: 'Enter', shiftKey: false, preventDefault(){} }); const a = sent; sent = false; csInputKey({ key: 'Enter', shiftKey: true, preventDefault(){} }); const b = sent; window.csSend = orig; return a && !b; })();
  }));

  check('nothing threw', errors.length === 0, errors.slice(0, 3).join(' | '));
  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
