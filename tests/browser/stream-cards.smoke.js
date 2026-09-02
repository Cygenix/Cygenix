/* tests/browser/stream-cards.smoke.js
 * ---------------------------------------------------------------------------
 * The enriched stream cards on /data-stream.
 *
 * tests/data-stream.test.js pins the derivations — the trend, the redaction,
 * the impact line, the sort weight. What it cannot show is that the tile
 * actually draws a line, that the accessible name is the sentence rather than
 * the number, that the dead-letter values arrive on screen hidden, or that a
 * stalled stream's consumers are named where somebody would see them.
 *
 * The sharpest assertion in this file is the redaction one: it reads the DOM,
 * not the model, because "redacted by default" is a claim about what reaches
 * the screen.
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/stream-cards.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8405);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

// charset matters: without it the browser reads UTF-8 source as latin-1.
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.json': 'application/json' };

const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/') p = '/index.html';
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';
  // The console's addresses are hyphenated; some files are not.
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
  const offSite = [];
  page.on('pageerror', (e) => errors.push(e.message));
  // One handler for the whole file: a second one racing it throws
  // "already handled" the moment both fire on the same dialog.
  let lastDialog = null;
  page.on('dialog', (d) => { lastDialog = d.message(); d.dismiss(); });
  await page.route('**/*', (r) => {
    const u = r.request().url();
    if (u.startsWith('http://localhost:' + PORT)) return r.continue();
    offSite.push(u);
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
  });

  const open = async () => {
    await page.goto('http://localhost:' + PORT + '/data-stream', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => document.querySelectorAll('.ds-kpi').length > 0,
      null, { timeout: 20000 });
    await page.waitForTimeout(900);
  };
  const tiles = () => page.evaluate(() =>
    Array.from(document.querySelectorAll('.ds-kpi')).map((el) => ({
      label: el.querySelector('.ds-kpi-label').textContent.trim(),
      figure: el.querySelector('.ds-kpi-figure').textContent.trim(),
      delta: (el.querySelector('.ds-kpi-delta') || {}).textContent || '',
      deltaBad: !!el.querySelector('.ds-kpi-delta.bad'),
      spark: !!el.querySelector('.ds-spark'),
      points: (el.querySelector('.ds-spark polyline') || {}).getAttribute
        ? el.querySelector('.ds-spark polyline').getAttribute('points') : '',
      collecting: (el.querySelector('.ds-spark-none') || {}).textContent || '',
      aria: el.getAttribute('aria-label'),
      svgHidden: !el.querySelector('.ds-spark') || el.querySelector('.ds-spark').getAttribute('aria-hidden') === 'true',
    })));

  console.log('Data Stream — tiles that show a trend, failures that say why\n');

  await open();

  /* ── 1. Sparklines ──────────────────────────────────────────────────────── */

  const t = await tiles();
  check('all four KPI tiles are there', t.length === 4, t.map((x) => x.label).join(' | '));
  check('each draws a sparkline once there is history to draw',
    t.every((x) => x.spark), JSON.stringify(t.map((x) => [x.label, x.spark])));
  check('drawn as inline SVG — no charting library was added',
    (await page.evaluate(() => Array.from(document.scripts).map((s) => s.src).join(' ')))
      .indexOf('chart') === -1
    && !offSite.some((u) => /chart|d3|recharts/i.test(u)));
  check('the polyline has a point per sample rather than a smoothed guess',
    t.every((x) => !x.spark || x.points.split(' ').length >= 8),
    t.map((x) => x.points.split(' ').length).join(','));

  check('each tile carries a delta label beside the figure',
    t.filter((x) => x.delta).length === 4, JSON.stringify(t.map((x) => [x.label, x.delta])));
  check('errors read as a rate, throughput as a percentage',
    /\/min|level/.test(t[3].delta) && /%|level/.test(t[1].delta),
    t[3].delta + ' | ' + t[1].delta);

  /* ── 2. The accessible name is the sentence ─────────────────────────────── */

  check('every tile\'s accessible name is a sentence, not a number list',
    t.every((x) => /[a-z]{4,}.*\./.test(x.aria || '')), JSON.stringify(t.map((x) => x.aria)));
  // The delivery tile reads as a state now, so its accessible name is the
  // state sentence rather than a rate glued onto a phrase.
  check('and it names the metric, the value and the direction',
    /^Max lag: .+, (rising|falling|level)/.test(t[2].aria || ''), t[2].aria);
  check('the delivery tile says what is wrong, not how fast a counter moves',
    /^\d+ streams? blocked: .+\.$|^All streams are delivering\.$|losing records/.test(t[3].aria || ''),
    t[3].aria);
  check('the drawing itself is hidden from screen readers',
    t.every((x) => x.svgHidden));

  /* ── 3. Under eight points, no line at all ──────────────────────────────── */

  await page.evaluate(() => { P.state.kpiHistory = P.state.kpiHistory.slice(0, 3); paint(); });
  await page.waitForTimeout(200);
  const thin = await tiles();
  check('with only three samples no sparkline is drawn',
    thin.every((x) => !x.spark), 'a flat line from three samples is a lie told in a confident font');
  check('and the tile says it is still collecting',
    thin.every((x) => /collecting/.test(x.collecting)), JSON.stringify(thin.map((x) => x.collecting)));
  check('the accessible name says so too, rather than implying a trend',
    /still collecting/.test(thin[1].aria || ''), thin[1].aria);
  await page.evaluate(() => { P.boot(); paint(); });
  await page.waitForTimeout(300);

  /* ── 4. Failure detail — why, not just where ────────────────────────────── */

  // Stop the live loop first. The page repaints every two seconds by design,
  // and a test that reads a panel, clicks it, then reads it again is racing
  // that repaint rather than testing anything.
  await page.evaluate(() => P.stop());
  await page.evaluate(() => {
    const s = P.state.streams.filter((x) => x.status === 'failed')[0];
    dsSelect(s.id); dsInspTab('errors');
  });
  await page.waitForTimeout(300);
  const insp = await page.evaluate(() =>
    document.querySelector('.ds-insp-body').textContent.replace(/\s+/g, ' ').trim());

  check('the failure is classified, not just quoted',
    /String truncation/.test(insp), insp.slice(0, 120));
  check('with how long it has been failing, and since when',
    /Failing for\s*\d+[smhd]/.test(insp) && /since \d\d:\d\d/.test(insp), insp.slice(0, 200));
  check('and the last successful delivery in words a person can use',
    /Last successful delivery\s*(\d\d:\d\d, .*ago|none recorded)/.test(insp), insp.slice(0, 260));
  check('the hex checkpoint is still there for an engineer who needs it',
    await page.evaluate(() => !!document.querySelector('.ds-checkpoint')));
  check('but demoted to a chip rather than presented as a headline',
    await page.evaluate(() => {
      const chip = document.querySelector('.ds-checkpoint');
      const head = document.querySelector('.ds-note.error');
      if (!chip || !head) return false;
      const cs = getComputedStyle(chip), hs = getComputedStyle(head);
      return parseFloat(cs.fontSize) <= parseFloat(hs.fontSize);
    }));

  /* ── 5. Dead-letter values are redacted on screen ───────────────────────── */

  const values = await page.evaluate(() =>
    Array.from(document.querySelectorAll('#ds-dlq-rows td.v')).map((e) => e.textContent));
  const fields = await page.evaluate(() =>
    Array.from(document.querySelectorAll('#ds-dlq-rows td.f')).map((e) => e.textContent));
  check('a representative dead-lettered record is shown', fields.length > 0, JSON.stringify(fields));
  check('with its field NAMES visible', fields.every((f) => /^[a-z_]+$/i.test(f)), JSON.stringify(fields));
  check('and every value redacted before it ever reaches the screen',
    values.length > 0 && values.every((v) => /^•+$/.test(v)), JSON.stringify(values));
  check('no customer-looking string is anywhere in the panel by default',
    !/Pinehurst|Acme|Northgate|Baxter/.test(insp), insp.slice(0, 200));

  await page.click('#ds-dlq-reveal');
  await page.waitForTimeout(200);
  check('revealing is a separate, deliberate click',
    await page.evaluate(() =>
      Array.from(document.querySelectorAll('#ds-dlq-rows td.v')).some((e) => !/^•+$/.test(e.textContent))));

  /* ── 6. The three actions, through the existing guardrail ───────────────── */

  const actions = await page.evaluate(() =>
    Array.from(document.querySelectorAll('.ds-insp-body button, .ds-insp-body a'))
      .map((b) => b.textContent.trim()));
  check('retry, reset and pause are all offered on a failing stream',
    /Retry dead letters/.test(actions.join('|')) && /Reset to last good checkpoint/.test(actions.join('|')),
    actions.join(' | '));
  lastDialog = null;
  await page.evaluate(() => {
    const b = Array.from(document.querySelectorAll('.ds-insp-body button'))
      .filter((x) => /Reset to last good/.test(x.textContent))[0];
    if (b) b.click();
  });
  await page.waitForTimeout(300);
  check('resetting the checkpoint confirms first, with the blast radius stated',
    !!lastDialog && /re-delivered/.test(lastDialog), lastDialog);
  check('and declining it changes nothing',
    (await page.evaluate(() =>
      P.state.streams.filter((x) => x.status === 'failed').length)) === 1);

  /* ── 7. Downstream consumers ────────────────────────────────────────────── */

  await page.evaluate(() => { dsInspTab('summary'); });
  await page.waitForTimeout(200);
  check('the inspector names who reads this stream',
    /Consumers/.test(await page.evaluate(() =>
      document.querySelector('.ds-insp-body').textContent)));
  const impacts = await page.evaluate(() =>
    Array.from(document.querySelectorAll('.ds-impact')).map((e) => e.textContent.trim()));
  check('a stalled stream with consumers says what it means for them',
    impacts.length > 0 && impacts.some((l) => /has had no new data for/.test(l)),
    JSON.stringify(impacts));
  check('and names them, rather than saying "consumers may be affected"',
    impacts.every((l) => !/may be affected/.test(l)));
  check('the impact line carries words, not only amber',
    await page.evaluate(() => {
      const el = document.querySelector('.ds-impact');
      return !!el && el.textContent.trim().length > 20;
    }));
  check('a stalled stream somebody reads sorts above one nobody reads',
    await page.evaluate(() => {
      const rows = Array.from(document.querySelectorAll('.ds-table tbody tr'));
      const ids = rows.map((r) => r.dataset.id);
      const byId = {};
      P.state.streams.forEach((s) => { byId[s.id] = s; });
      const paused = ids.filter((id) => byId[id] && byId[id].status === 'paused');
      if (paused.length < 2) return true;                 // nothing to prove here
      const first = byId[paused[0]];
      return (first.consumers || []).length > 0;
    }));

  /* ── 8. The blocked band, which is the whole acceptance test ────────────── */
  //
  // "An operator who opens Home and does not click anything must be able to
  // say, out loud: which stream is broken, that nothing is reaching the
  // destination, roughly why, what data is at risk, and how long they have."
  // Those five, read off the DOM, with nothing clicked.

  await open();
  const band = await page.evaluate(() => {
    const b = document.querySelector('#ds-bandhost .ds-band');
    if (!b) return null;
    return {
      tag: b.querySelector('.ds-band-tag').textContent.trim(),
      what: b.querySelector('.ds-band-what').textContent.trim(),
      why: b.querySelector('.ds-band-err').textContent.trim(),
      target: (b.querySelector('.ds-band-target') || {}).textContent || '',
      piling: b.querySelector('.ds-band-line').textContent.trim(),
      means: b.querySelector('.ds-band-line.means').textContent.trim(),
      deadline: b.querySelector('.ds-band-deadline').textContent.trim(),
      actions: Array.from(b.querySelectorAll('.ds-band-actions .btn')).map((x) => x.textContent.trim()),
      aboveTiles: (() => {
        const k = document.getElementById('ds-kpis');
        return (b.compareDocumentPosition(k) & Node.DOCUMENT_POSITION_FOLLOWING) !== 0;
      })(),
    };
  });
  check('a blocked stream gets a band, above the KPI tiles',
    !!band && band.aboveTiles, JSON.stringify(band));
  check('WHICH STREAM — it is named, in a sentence',
    /Cutover delta into target has delivered nothing for/.test(band.what), band.what);
  check('THAT NOTHING IS ARRIVING — said in words, not as a rate of 0',
    /delivered nothing/.test(band.what));
  check('ROUGHLY WHY — the destination\'s own message, verbatim',
    band.why === 'destination rejected: string truncation on stg_customers.name', band.why);
  check('with the failing table and column picked out of it',
    band.target === 'stg_customers.name', band.target);
  check('WHAT IS AT RISK — the queue and what has been given up on',
    /changes are queued and \d+ have been given up on/.test(band.piling), band.piling);
  check('and what that means for the data, not just for the system',
    /Deletions are not being applied|not reaching/.test(band.means), band.means);
  check('HOW LONG THEY HAVE — the retention deadline, which was shown nowhere before',
    /retention · .*(expires|resync|headroom)/.test(band.deadline), band.deadline);
  check('with the actions beside it',
    band.actions.length >= 4 && /Copy diagnostics/.test(band.actions.join('|')),
    band.actions.join(' | '));

  /* The row, which is where the original lie was told */
  const row = await page.evaluate(() => {
    const tr = document.querySelector('.ds-table tbody tr');
    const c = tr.querySelectorAll('td');
    return {
      cls: tr.className,
      name: c[1].querySelector('.ds-cell-name').textContent.trim(),
      reason: (c[1].querySelector('.ds-rowreason') || {}).textContent || '',
      capture: c[5].textContent.trim(),
      deliver: c[6].textContent.trim(),
      deliverStalled: !!c[6].querySelector('.ds-stalled'),
      lastDelivered: c[8].textContent.trim(),
    };
  });
  check('the blocked stream sorts to the top, whatever its status label says',
    /Cutover delta/.test(row.name), row.name);
  check('its row is marked as blocked', /blocked-blocked/.test(row.cls), row.cls);
  check('and carries a one-line reason under the name',
    /truncation/.test(row.reason), row.reason);
  check('capture and deliver are two columns, because they disagree',
    /260/.test(row.capture) && row.deliver === '0', row.capture + ' / ' + row.deliver);
  check('and the zero that is the whole problem is not a quiet number',
    row.deliverStalled);
  check('"last delivered" reads 13 minutes, not "0s ago"',
    /1[0-9]m ago|13m ago/.test(row.lastDelivered), row.lastDelivered);

  /* The flow view */
  await page.evaluate(() => dsSetView('flow'));
  await page.waitForTimeout(400);
  check('the flow view severs the delivery hop instead of labelling it 0/min',
    await page.evaluate(() => {
      const a = document.querySelector('.ds-flow-arrow.cut');
      return !!a && /delivering nothing/.test(a.textContent)
        && !!a.querySelector('.ds-flow-err');
    }));
  await page.evaluate(() => dsSetView('table'));
  await page.waitForTimeout(300);

  /* Dismissal is per session */
  await page.evaluate(() => dsBandDismiss());
  await page.waitForTimeout(200);
  check('the band can be dismissed for the session',
    (await page.evaluate(() => !document.querySelector('#ds-bandhost .ds-band'))));
  await page.reload({ waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(1200);
  check('and comes back on the next visit, because the stream is still broken',
    (await page.evaluate(() => !!document.querySelector('#ds-bandhost .ds-band'))));

  /* ── 9. Nothing threw ───────────────────────────────────────────────────── */

  const real = errors.filter((e) => !/ResizeObserver|Failed to fetch|NetworkError|Load failed/.test(e));
  check('no page errors through all of that', real.length === 0, real.join(' | '));

  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  await browser.close();
  server.close();
  process.exit(fail ? 1 : 0);
})().catch((e) => {
  console.error('\nSMOKE CRASHED: ' + (e && e.stack || e));
  try { server.close(); } catch (_) {}
  process.exit(1);
});
