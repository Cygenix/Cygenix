/* tests/browser/stream-prod-guard.smoke.js
 * ---------------------------------------------------------------------------
 * Phase 2 of the Data Stream profile work, in a real browser.
 *
 * tests/data-stream-destinations.test.js proves the rules in the engine and
 * the store. What it cannot show is that the page actually ASKS: that a Start
 * on a PRD profile puts up the typed prompt, that a wrong answer leaves the
 * stream where it was and an alert says why, that the red PRD marker is on
 * the row, and that "Convert to saved connection" in the Designer writes the
 * endpoint to the synced blob and the credential to the local-only key.
 *
 * The sharpest assertion is the storage one: it reads localStorage after the
 * conversion, because "the credential never reaches the synced blob" is a
 * claim about bytes, not about intent.
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/stream-prod-guard.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8406);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.json': 'application/json' };

const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/') p = '/index.html';
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';
  if (!fs.existsSync(f) && fs.existsSync(f.replace(/-/g, '_') + '.html')) f = f.replace(/-/g, '_') + '.html';
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) {
    res.writeHead(404); return res.end('no');
  }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const page = await browser.newPage({ viewport: { width: 1500, height: 1100 } });
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));

  // One dialog handler for the whole file. Confirms are accepted; prompts
  // are answered from a queue (so a test can decide what gets TYPED); alerts
  // are recorded so a refusal can be read back.
  const answers = [];
  const dialogs = [];
  page.on('dialog', async (d) => {
    dialogs.push({ type: d.type(), message: d.message() });
    if (d.type() === 'prompt') return d.accept(answers.length ? answers.shift() : '');
    if (d.type() === 'confirm') return d.accept();
    return d.dismiss();
  });
  await page.route('**/*', (r) => {
    const u = r.request().url();
    if (u.startsWith('http://localhost:' + PORT)) return r.continue();
    return r.abort();
  });
  await page.addInitScript(() => {
    if (sessionStorage.getItem('cygenix_token')) return;
    const exp = String(Date.now() + 3600e3);
    for (const s of [localStorage, sessionStorage]) {
      s.setItem('cygenix_token', 'smoke'); s.setItem('cygenix_expires', exp);
    }
    localStorage.setItem('cygenix_onboarded', '1');
    localStorage.setItem('cygenix_user', JSON.stringify({ email: 'you@example.test', name: 'You' }));
    localStorage.setItem('cygenix_tier', 'pro');
    localStorage.setItem('cygenix_cookie_consent', 'all');
    localStorage.setItem('acct-cygenix.ciamlogin.com-x', JSON.stringify({
      homeAccountId: 'x', environment: 'cygenix.ciamlogin.com', authorityType: 'MSSTS',
      username: 'you@example.test', localAccountId: 'x', tenantId: 'x' }));
    // A PRODUCTION profile, active, with two saved connections.
    const U = 'you@example.test';
    const blob = {}; blob[U] = [
      { id: 'c_src', side: 'src', mode: 'direct', name: 'PROD-SQL01' },
      { id: 'c_tgt', side: 'tgt', mode: 'direct', name: 'PROD-AZSQL' }];
    localStorage.setItem('cygenix_saved_connections', JSON.stringify(blob));
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify({ v: 1,
      profiles: [{ id: 'SMOKE_PRD', name: 'Smoke production', envClass: 'PRD', status: 'active', srcConnId: 'c_src', tgtConnId: 'c_tgt' }],
      bindings: [], connMeta: {}, runRecords: [], events: [],
      settings: { envClasses: ['DEV', 'TEST', 'UAT', 'PRD'], activeProfileId: 'SMOKE_PRD', selectedAt: 1 } }));
  });

  const openList = async () => {
    await page.goto('http://localhost:' + PORT + '/data-stream', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => document.querySelectorAll('.ds-kpi').length > 0, null, { timeout: 20000 });
    await page.evaluate(() => {
      const P = window.CygenixDataStreamPage, DS = window.CygenixDataStream;
      const prof = P.activeProfile();
      let changed = false;
      P.state.streams.forEach((s) => {
        if (DS.isUnassigned(s)) { DS.assignProfile(P.state, s.id, prof, s.capture.side, { savedConns: P.savedConns() }); changed = true; }
      });
      if (changed) { P.persist(); if (typeof paint === 'function') paint(); }
    });
    await page.waitForTimeout(700);
  };

  console.log('Data Stream — the production guard, and saved destinations\n');

  /* ── 1. The red marker, and Start asks for the id ───────────────────── */

  await openList();
  const marks = await page.evaluate(() => ({
    rows: document.querySelectorAll('#ds-main tbody tr').length,
    marked: document.querySelectorAll('#ds-main tbody tr .ds-prd').length,
    text: (document.querySelector('.ds-prd') || {}).textContent || '',
    red: getComputedStyle(document.querySelector('.ds-prd')).backgroundColor,
  }));
  check('every row under the PRD profile carries the marker, in red, saying PRD',
    marks.rows > 0 && marks.marked === marks.rows && marks.text === 'PRD' && /rgb\(192, 57, 43\)/.test(marks.red),
    JSON.stringify(marks));

  const target = await page.evaluate(() => {
    const P = window.CygenixDataStreamPage;
    const s = P.state.streams.filter((x) => x.status === 'draft' || x.status === 'stopped')[0];
    return { id: s.id, status: s.status };
  });
  dialogs.length = 0; answers.push('WRONG_ID');
  await page.evaluate((id) => dsStart(id), target.id);
  await page.waitForTimeout(400);
  const afterWrong = await page.evaluate((id) => {
    const P = window.CygenixDataStreamPage;
    const s = P.state.streams.filter((x) => x.id === id)[0];
    return { status: s.status, guard: P.state.audit.filter((a) => a.action === 'stream.prod_guard').map((a) => a.detail.outcome) };
  }, target.id);
  check('Start: the ordinary confirm says production is coming, then a prompt asks for the id',
    dialogs.some((d) => d.type === 'confirm' && /PRODUCTION/.test(d.message))
    && dialogs.some((d) => d.type === 'prompt' && /SMOKE_PRD/.test(d.message) && /Type the profile id/.test(d.message)),
    JSON.stringify(dialogs.map((d) => d.type)));
  check('a wrong answer: an alert says why, the stream stays where it was, the refusal is on the record',
    dialogs.some((d) => d.type === 'alert' && /production \(PRD\)/.test(d.message))
    && afterWrong.status === target.status && afterWrong.guard.some((o) => /refused/.test(o)),
    JSON.stringify(afterWrong));

  dialogs.length = 0; answers.push('SMOKE_PRD');
  await page.evaluate((id) => dsStart(id), target.id);
  await page.waitForTimeout(400);
  const afterRight = await page.evaluate((id) => {
    const P = window.CygenixDataStreamPage, DS = window.CygenixDataStream;
    const s = P.state.streams.filter((x) => x.id === id)[0];
    return { live: DS.isLive(s.status), confirmed: P.state.audit.filter((a) => a.action === 'stream.prod_guard' && a.detail.outcome === 'confirmed').length };
  }, target.id);
  check('the right id starts it, and that is audited as confirmed', afterRight.live && afterRight.confirmed === 1, JSON.stringify(afterRight));

  /* ── 2. The Designer: an inline destination, converted ──────────────── */

  const wh = await page.evaluate(() => {
    const P = window.CygenixDataStreamPage;
    const s = P.state.streams.filter((x) => x.destination.kind === 'webhook' && !x.destination.savedId)[0];
    return s ? { id: s.id, label: s.destination.label } : null;
  });
  check('the demo world has an inline webhook destination to convert', !!wh);

  await page.goto('http://localhost:' + PORT + '/data-stream-designer?stream=' + wh.id + '&step=3', { waitUntil: 'domcontentloaded' });
  await page.waitForFunction(() => !!document.querySelector('#ds-dest'), null, { timeout: 20000 });
  const step3 = await page.evaluate(() => ({
    inlineTag: !!document.querySelector('#ds-step .ds-tag.inline'),
    convert: !!Array.from(document.querySelectorAll('#ds-step button')).find((b) => /Convert to saved connection/.test(b.textContent)),
    typed: (document.querySelector('#ds-dest-inline') || {}).value || '',
    options: document.querySelectorAll('#ds-dest option').length,
  }));
  check('step 3 shows the endpoint as inline, with the typed value and a Convert button',
    step3.inlineTag && step3.convert && step3.typed === wh.label && step3.options === 1, JSON.stringify(step3));

  await page.evaluate(() => dsConvertDestination());
  await page.waitForSelector('.sdst-modal', { timeout: 5000 });
  const form = await page.evaluate(() => ({
    kind: document.querySelector('#sdst-kind').value, kindLocked: document.querySelector('#sdst-kind').disabled,
    name: document.querySelector('#sdst-name').value, url: document.querySelector('#sdst-url').value,
    secretType: document.querySelector('#sdst-secret').type,
  }));
  check('the editor opens locked to Webhook, prefilled from the inline label, with a password field for the secret',
    form.kind === 'webhook' && form.kindLocked && form.name === wh.label && form.url === wh.label && form.secretType === 'password',
    JSON.stringify(form));
  await page.fill('#sdst-name', 'Finance hook');
  await page.fill('#sdst-secret', 'smoke-signing-secret');
  await page.click('#sdst-save');
  await page.waitForFunction(() => !document.querySelector('.sdst-modal'), null, { timeout: 5000 });
  await page.waitForTimeout(300);

  const after = await page.evaluate((id) => {
    const blob = localStorage.getItem('cygenix_saved_connections') || '';
    const sec = JSON.parse(localStorage.getItem('cygenix_saved_conn_secrets') || '{}');
    const d = draft.destination;
    const stored = window.CygenixDataStreamPage.state.streams.filter((x) => x.id === id)[0].destination;
    const entry = window.CygenixStreamDestinations.byId(d.savedId);
    return {
      savedId: d.savedId, label: d.label, storedSavedId: stored.savedId,
      blobHasName: /Finance hook/.test(blob), blobHasSecret: /smoke-signing-secret/.test(blob),
      secretStored: !!(entry && sec[entry.id] && sec[entry.id].secret === 'smoke-signing-secret'),
      savedTag: !!document.querySelector('#ds-step .ds-tag.saved'),
      selected: document.querySelector('#ds-dest').value,
      markupHasSecret: /smoke-signing-secret/.test(document.body.innerHTML),
      converted: window.CygenixDataStreamPage.state.audit.some((a) => a.action === 'stream.destination_converted'),
    };
  }, wh.id);
  check('the draft and the stored stream now point at the saved destination by id',
    !!after.savedId && after.storedSavedId === after.savedId && after.label === 'Finance hook' && after.selected === after.savedId,
    JSON.stringify(after));
  check('the name went to the synced blob; the credential went ONLY to the local-only key, and never into the page',
    after.blobHasName && !after.blobHasSecret && after.secretStored && !after.markupHasSecret, JSON.stringify(after));
  check('step 3 now says saved, and the conversion is on the record', after.savedTag && after.converted);

  /* ── 3. The Designer: turning on log capture asks too ───────────────── */

  await page.evaluate(() => { draft.capture.method = 'poll'; goStep(1); });
  await page.waitForTimeout(200);
  dialogs.length = 0; answers.push('nope');
  await page.evaluate(() => setMethod('log'));
  await page.waitForTimeout(300);
  const logWrong = await page.evaluate(() => ({ method: draft.capture.method,
    checked: (document.querySelector('input[name="ds-method"]:checked') || {}).value }));
  check('switching to log-based capture under PRD asks for the id; a wrong answer leaves the method as it was',
    dialogs.some((d) => d.type === 'prompt' && /transaction log/.test(d.message))
    && dialogs.some((d) => d.type === 'alert' && /did not match/.test(d.message))
    && logWrong.method === 'poll' && logWrong.checked === 'poll', JSON.stringify({ logWrong, dialogs }));
  dialogs.length = 0; answers.push('SMOKE_PRD');
  await page.evaluate(() => setMethod('log'));
  await page.waitForTimeout(300);
  const logRight = await page.evaluate(() => ({ method: draft.capture.method, carried: prodConfirmedId }));
  check('the right id switches it, and is carried for the save', logRight.method === 'log' && logRight.carried === 'SMOKE_PRD');

  check('no page errors on either screen', errors.length === 0, errors.join(' | '));

  await browser.close();
  server.close();
  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
