/* tests/browser/claude-code-console.smoke.js
 * ---------------------------------------------------------------------------
 * The Claude Code console in a real browser, with the Azure routes and the
 * roles record stubbed at the data-proxy. Walks the brief's browser checks:
 *   1. off for the organisation → the plain "not enabled" message, no calls;
 *   2. on but no API key → the message and the Settings link, no calls;
 *   3. allowed → first-use notice, dismissed once and remembered;
 *   5. New session → Idle; a message → Working → tool blocks, a table, the
 *      redacted output → Idle, and the polling stops;
 *   6/7. the toggle: off → the confirmation; a production profile needs its
 *      name typed; on → the mode call and the notice in the chat;
 *   7. a quick double-click on New session starts one;
 *   8. Stop → Stopped, and no further /events calls;
 *   8. a connection failure in the output raises the firewall note;
 *   11. the sessions drawer lists the session; a past one replays read-only;
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
    calls: [], sessions: {}, nextEvents: [], failEvents: false, failStop: false };
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
        const s = { id, title: '', status: 'idle', dataChangesAllowed: false, connectionName: body.connectionName, profileName: body.profileName, createdAt: new Date().toISOString(), costCents: 0 };
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
      if (action === 'sessions') return json(route, { sessions: Object.values(world.sessions).map((x) => x.session).reverse() });
      if (action === 'upload') { const s = world.sessions[body.sessionId]; const up = { fileId: 'file_u' + (s.session.uploads || []).length, name: body.name, path: '/workspace/uploads/' + body.name, size: Buffer.from(body.contentBase64, 'base64').length, at: new Date().toISOString() };
        s.session.uploads = (s.session.uploads || []).concat([up]); return json(route, { upload: up }); }
      if (action === 'outputs') { const s = world.sessions[new URL('http://x' + p).searchParams.get('sessionId')]; return json(route, { outputs: world.outputs, uploads: (s && s.session.uploads) || [] }); }
      if (action === 'download') return json(route, { name: 'counts.csv', size: 24, contentBase64: Buffer.from('table,rows\nCustomer,1200\n').toString('base64') });
      if (action === 'session') { const s = world.sessions[new URL('http://x' + p).searchParams.get('id')]; return s ? json(route, { session: s.session, events: s.events }) : json(route, { error: 'No such session.' }, 404); }
      return json(route, { error: 'unexpected ' + action }, 500);
    }
    if (/action=whoami/.test(u)) return json(route, { tier: 'pro', tier_status: 'active', role: 'user' });
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
  check('the picker lists the active profile\'s connections, target first', opts[0] === 'Target — Target DEV' && opts[1] === 'Source — Legacy', opts.join(' | '));
  check('status: No session; Stop and Send off', (await text('cs-status')) === 'No session'
    && (await page.evaluate(() => document.getElementById('cs-stop').disabled && document.getElementById('cs-send').disabled)));

  /* 5 + 7. New session, double-clicked. */
  await page.evaluate(() => { csNew(); csNew(); });
  await page.waitForFunction(() => document.getElementById('cs-status').textContent === 'Idle', null, { timeout: 10000 });
  check('A QUICK DOUBLE-CLICK ON NEW SESSION OPENS ONE', calls('session').filter((c) => c.method === 'POST').length === 1);
  const sb = calls('session')[0].body;
  check('it sends the connection id and names — never a connection string or password',
    sb.connId === 'c_tgt' && sb.connectionName === 'Target DEV' && sb.profileName === 'Demo' && sb.side === 'tgt'
    && !JSON.stringify(world.calls).includes('Tr0ub4dor') && !JSON.stringify(world.calls).includes('acme.database'), JSON.stringify(sb));
  check('the caller\'s key and token ride in the headers', /sk-ant-smoke-key/.test(calls('session')[0].headers['x-anthropic-key']) && /^Bearer /.test(calls('session')[0].headers.authorization));
  check('Idle: Send is on, Stop is on', (await text('cs-status')) === 'Idle' && (await page.evaluate(() => !document.getElementById('cs-send').disabled && !document.getElementById('cs-stop').disabled)));

  /* A message, then the events. */
  world.nextEvents = [
    { id: 'e1', type: 'agent.tool_use', name: 'write', input: { path: '/workspace/count.py', content: 'import os, pymssql\nprint("hi")' } },
    { id: 'e2', type: 'agent.tool_result', tool_use_id: 'e1', content: [{ type: 'text', text: 'ok' }] },
    { id: 'e3', type: 'agent.tool_use', name: 'bash', input: { command: 'set -a; . /workspace/.cygenix/db.env; set +a; python3 count.py' } },
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

  /* 8. A failing connection in the output raises the firewall note. */
  world.nextEvents = [
    { id: 'e7', type: 'agent.tool_use', name: 'bash', input: { command: 'python3 q.py' } },
    { id: 'e8', type: 'agent.tool_result', tool_use_id: 'e7', is_error: true, content: [{ type: 'text', text: 'pymssql.OperationalError: (20009, b\'DB-Lib error: Unable to connect: Adaptive Server is unavailable or does not exist\')' }] },
    { id: 'e9', type: 'session.status_idle', stop_reason: { type: 'end_turn' } },
  ];
  await page.waitForTimeout(3100);
  await page.fill('#cs-input', 'Run the query');
  await page.click('#cs-send');
  await page.waitForFunction(() => !document.getElementById('cs-firewall').hidden, null, { timeout: 8000 });
  check('A CONNECTION FAILURE RAISES THE FIREWALL HELP', /Your database may only accept known IP addresses — the Anthropic workspace may need allowing\./.test(await text('cs-firewall')));

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

  check('nothing threw', errors.length === 0, errors.slice(0, 3).join(' | '));
  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
