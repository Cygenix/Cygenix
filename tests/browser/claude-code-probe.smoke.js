/* tests/browser/claude-code-probe.smoke.js
 * ---------------------------------------------------------------------------
 * The Claude Code connection test page, in a real browser, with the Azure
 * route stubbed at the data-proxy:
 *   - the host, port and type fill themselves from the active profile's
 *     target connection, and the password never leaves the page;
 *   - with no Anthropic key the buttons are off and Settings is linked;
 *   - a quick double-click starts one test;
 *   - results are polled until done, then polling stops — no more calls;
 *   - a failing result read shows its error once and stops;
 *   - it follows the console theme through the shared tokens, and nothing throws.
 *
 * Run it by hand:  node tests/browser/claude-code-probe.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8441);
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
const PASSWORD = 'Tr0ub4dor-secret';

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
  const token = 'x.' + Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, preferred_username: U })).toString('base64url') + '.y';

  const seen = { starts: [], reads: 0, bodies: [], failReads: false, doneAfter: 2, emptyTurn: false };
  const json = (route, body, status) => route.fulfill({ status: status || 200, contentType: 'application/json', body: JSON.stringify(body) });
  await ctx.route('**', async (route) => {
    const u = route.request().url();
    const q = new URL(u, 'http://x').searchParams;
    if (/data-proxy/.test(u) && /^\/agent\/claude-code\/probe/.test(q.get('path') || '')) {
      const body = route.request().postData() || '';
      seen.bodies.push(body + ' ' + JSON.stringify(route.request().headers()));
      if (route.request().method() === 'POST') {
        const b = JSON.parse(body);
        seen.starts.push(b);
        return json(route, { sessionId: 'sesn_' + seen.starts.length + 'aaaaaa', status: 'running', host: b.host, port: b.port, network: b.network });
      }
      seen.reads++;
      if (seen.failReads) return json(route, { error: 'Anthropic is rate-limiting this account. Wait a minute and try again.' }, 429);
      const done = seen.reads >= seen.doneAfter;
      const steps = (a, b, c) => [{ name: 'hello', label: 'Workspace answers', status: a }, { name: 'connect', label: 'Connection to the server', status: b }, { name: 'login', label: 'Database reply', status: c }];
      if (done && seen.emptyTurn) return json(route, { done: true, status: 'idle', costCents: 7, result: null, verdict: null, errors: [], transcript: [], emptyTurn: true, stopReason: 'end_turn',
        steps: steps('passed', 'empty', 'pending'), step: 2, failedStep: { index: 2, name: 'connect', label: 'Connection to the server', status: 'empty' },
        eventTypes: ['user.message', 'span.model_request_end:out=0', 'session.status_idle:end_turn'],
        raw: { modelRequestEnd: { type: 'span.model_request_end', is_error: false, model_usage: { cache_creation_input_tokens: 4833, input_tokens: 4, output_tokens: 0 } }, usage: { type: 'session.usage', usage: { output_tokens: 0 } } } });
      return json(route, done ? {
        done: true, status: 'idle', costCents: 4, steps: steps('passed', 'passed', 'passed'), step: 3, failedStep: null,
        result: { host: 'acme.database.windows.net', port: 1433, kind: 'sqlserver', dns: ['10.1.2.3'], tcp: 'open', tcp_ms: 38, handshake: 'no-reply', egress_ip: '203.0.113.9' },
        verdict: { ok: false, text: 'A connection opened, but no database answered on it (no-reply).' }, errors: [],
      } : { done: false, status: 'running', result: null, verdict: null, errors: [], steps: steps('passed', 'running', 'pending'), step: 2, failedStep: null });
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
    const cs = 'Server=tcp:acme.database.windows.net,1433;Database=tgt;User ID=u;Password=' + arg.PASSWORD;
    const live = {}; live[arg.U] = { srcConnString: 'Server=src.example.test;Database=s', srcConnMode: 'direct', tgtConnString: cs, tgtConnMode: 'direct' };
    localStorage.setItem('cygenix_project_connections', JSON.stringify(live));
    const blob = {}; blob[arg.U] = [{ id: 'c_src', side: 'src', mode: 'direct', name: 'Legacy' }, { id: 'c_tgt', side: 'tgt', mode: 'direct', name: 'Target DEV' }];
    localStorage.setItem('cygenix_saved_connections', JSON.stringify(blob));
    localStorage.setItem('cygenix_saved_conn_secrets', JSON.stringify({ c_src: { connString: 'Server=src.example.test;Database=s' }, c_tgt: { connString: cs } }));
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify({ v: 1, connMeta: { c_src: { envClass: 'DEV' }, c_tgt: { envClass: 'DEV' } }, bindings: [], runRecords: [], events: [],
      profiles: [{ id: 'DEMO', name: 'Demo', envClass: 'DEV', status: 'active', srcConnId: 'c_src', tgtConnId: 'c_tgt', createdAt: 1, updatedAt: 1 }],
      settings: { envClasses: ['DEV', 'TEST', 'UAT', 'PRD', 'SANDBOX'], activeProfileId: 'DEMO', selectedAt: 1 } }));
  }, { U, token, PASSWORD });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  // Leave the page before opening it again: a goto to an address that
  // differs only by its #fragment is a same-document jump, not a reload.
  const open = async () => {
    await page.goto('http://localhost:' + PORT + '/favicon.svg');
    await page.goto('http://localhost:' + PORT + '/dev-console#test', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => !!window.CygenixCcProbe && typeof ccRun === 'function', null, { timeout: 20000 });
    await page.waitForTimeout(400);
  };

  console.log('Claude Code — connection test page\n');
  await open();
  let st = await page.evaluate(() => ({ off: document.getElementById('cc-run-limited').disabled, note: !document.getElementById('cc-key-note').hidden,
    link: !!document.querySelector('#cc-key-note a[href="/dashboard#project-settings"]') }));
  check('with no Anthropic key: buttons off, a note linking to Settings', st.off && st.note && st.link, JSON.stringify(st));
  await page.click('#cc-run-limited', { force: true });
  await page.waitForTimeout(300);
  check('…and clicking anyway calls nothing', seen.starts.length === 0);

  // The key is seeded before the page loads, as it is for a real user who
  // saved it in Settings.
  await ctx.addInitScript(() => { try { localStorage.setItem('cygenix_api_key', 'sk-ant-smoke-key'); } catch (e) {} });
  await open();
  st = await page.evaluate(() => ({ host: document.getElementById('cc-host').value, port: document.getElementById('cc-port').value,
    kind: document.getElementById('cc-kind').value, off: document.getElementById('cc-run-limited').disabled }));
  check('the server fills itself from the active profile\'s target', st.host === 'acme.database.windows.net' && st.port === '1433' && st.kind === 'sqlserver' && !st.off, JSON.stringify(st));

  await page.evaluate(() => { ccRun('limited'); ccRun('limited'); });
  await page.waitForTimeout(600);
  check('A QUICK DOUBLE-CLICK STARTS ONE TEST', seen.starts.length === 1, seen.starts.length);
  check('it sends host, port, type and network — nothing else',
    JSON.stringify(Object.keys(seen.starts[0]).sort()) === '["host","kind","network","port"]' && seen.starts[0].network === 'limited', JSON.stringify(seen.starts[0]));
  check('THE PASSWORD NEVER LEAVES THE PAGE', seen.bodies.every((b) => b.indexOf(PASSWORD) === -1));
  check('the caller\'s key and token ride in the headers', /x-anthropic-key":"sk-ant-smoke-key/.test(seen.bodies[0]) && /authorization":"Bearer /.test(seen.bodies[0]));

  await page.waitForFunction(() => /Not reached/.test(document.getElementById('cc-runs').textContent), null, { timeout: 15000 });
  const readsAtDone = seen.reads;
  const out = await page.evaluate(() => document.getElementById('cc-runs').textContent);
  check('the result shows the verdict, the facts and the workspace IP', /no database answered/.test(out) && /203\.0\.113\.9/.test(out) && /10\.1\.2\.3/.test(out), out.slice(0, 300));
  check('a failure shows the firewall sentence with the address', /Your database may only accept known IP addresses — the Anthropic workspace may need allowing\. This test arrived from 203\.0\.113\.9/.test(out));
  check('and the cost', /US\$0\.04/.test(out));
  check('the three rungs are listed with their outcome', /1 Workspace answers: passed · 2 Connection to the server: passed · 3 Database reply: passed/.test(out), out.slice(0, 400));
  await page.waitForTimeout(7000);
  check('POLLING STOPS WHEN THE TEST IS DONE — no further result calls', seen.reads === readsAtDone, seen.reads + ' vs ' + readsAtDone);

  seen.failReads = true;
  await page.waitForTimeout(3100);
  await page.evaluate(() => ccRun('open'));
  await page.waitForFunction(() => /rate-limiting/.test(document.getElementById('cc-runs').textContent), null, { timeout: 15000 });
  const r0 = seen.reads;
  await page.waitForTimeout(7000);
  check('a failing read shows its error once and stops — no retry storm', seen.reads === r0, seen.reads - r0);
  check('the open-networking test was asked for as such', seen.starts[1] && seen.starts[1].network === 'open');

  // An empty turn is named as one, with the numbers from the raw request.
  seen.failReads = false; seen.emptyTurn = true; seen.reads = 0;
  await page.waitForTimeout(3100);
  await page.evaluate(() => ccRun('limited'));
  await page.waitForFunction(() => /No result/.test(document.getElementById('cc-runs').textContent), null, { timeout: 15000 });
  const empty = await page.evaluate(() => document.getElementById('cc-runs').textContent);
  check('AN EMPTY TURN SAYS SO, NAMING THE RUNG, WITH THE STOP REASON AND THE TOKEN COUNTS — not "no reply"',
    /Claude's turn ended with nothing written at step 2 \(Connection to the server\): 0 output tokens, stop reason end_turn, no error reported by Anthropic/.test(empty) && /4837 input tokens/.test(empty)
    && /stopped at step 2 \(Connection to the server\) without reporting a result/.test(empty) && /2 Connection to the server: empty turn · 3 Database reply: not reached/.test(empty)
    && !/sent back no reply/.test(empty), empty.slice(0, 600));
  check('and the raw events are there to open', /Raw events/.test(empty) && /cache_creation_input_tokens/.test(empty)
    && await page.evaluate(() => !document.querySelector('#cc-runs details:not([open]) summary') === false));

  // The console has one palette plus named themes (cygenix-theme.css); a
  // saved 'dark' is migrated to light by every page's pre-paint script. What
  // must hold is that this page follows a theme through the shared tokens.
  const look = () => page.evaluate(() => getComputedStyle(document.querySelector('.card')).backgroundColor
    + '|' + getComputedStyle(document.querySelector('.btn.primary')).backgroundColor);
  const light = await look();
  await page.evaluate(() => document.documentElement.setAttribute('data-theme', 'financial'));
  const named = await look();
  await page.evaluate(() => document.documentElement.setAttribute('data-theme', 'light'));
  check('the page takes its colours from the shared tokens, so a named theme restyles it', named !== light && !/rgba\(0, 0, 0, 0\)/.test(light), named + ' / ' + light);
  check('a saved dark theme is migrated to light, as on every console page',
    /if \(t === 'dark'\) \{ t = 'light';/.test(fs.readFileSync(path.join(PUB, 'claude-code.html'), 'utf8')));
  check('nothing threw', errors.length === 0, errors.slice(0, 3).join(' | '));

  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
