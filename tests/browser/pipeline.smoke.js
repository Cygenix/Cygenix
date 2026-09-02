/* tests/browser/pipeline.smoke.js
 * ---------------------------------------------------------------------------
 * The Migration Pipeline card, on the real dashboard.
 *
 * tests/pipeline.test.js proves the derivation: which stage is the bottleneck,
 * what the sentence says, that the confidence arithmetic adds up. What it
 * cannot prove is that any of it reaches a screen — that the card renders
 * above the tiles, that the narrative is there at FIRST PAINT with no API call
 * and no button, that the stage chips are really links, and that none of it
 * depends on colour to be read. Those are the acceptance criteria, and they
 * are browser facts.
 *
 * The network is cut off at the browser: every request other than the local
 * static server is aborted. So if the narrative needed an Anthropic call to
 * appear, this file would fail — which is the point.
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/pipeline.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8404);
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
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) {
    res.writeHead(404); return res.end('no');
  }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

/* The §A.7 shape, on purpose: four jobs carry SQL and only two are mapped,
   because two were imported with their SQL already written. The funnel
   widens, and the card has to say so rather than draw it. */
const JOBS = [
  { id: 'j1', projectId: 'p1', name: 'Customers', status: 'ready', insertSQL: 'INSERT …',
    tables: [{ name: 'dbo.Customers', rows: 1200 }], columnMapping: [{ srcCol: 'id', tgtCol: 'id' }],
    totalRows: 1200 },
  { id: 'j2', projectId: 'p1', name: 'Orders', status: 'ready', insertSQL: 'INSERT …',
    tables: [{ name: 'dbo.Orders', rows: 9000 }], columnMapping: [{ srcCol: 'id', tgtCol: 'id' }],
    verifySQL: 'SELECT COUNT(*) FROM stg.Orders WHERE 1=1 -- verify', totalRows: 9000 },
  { id: 'j3', projectId: 'p1', name: 'Invoices (imported)', status: 'ready', insertSQL: 'INSERT …' },
  { id: 'j4', projectId: 'p1', name: 'Payments (imported)', status: 'ready', migrationSQL: 'INSERT …' },
  { id: 'j5', projectId: 'p1', name: 'Ledger', status: 'pending' },
];

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const page = await browser.newPage({ viewport: { width: 1500, height: 1000 } });
  const errors = [];
  const offSite = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.route('**/*', (r) => {
    const u = r.request().url();
    if (u.startsWith('http://localhost:' + PORT)) return r.continue();
    offSite.push(u);
    return r.abort();
  });
  await page.addInitScript((jobs) => {
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
    localStorage.setItem('cygenix_project_connections', JSON.stringify({
      'you@example.test': { srcConnString: 'Server=a;Database=Legacy', srcConnMode: 'direct',
                            tgtConnString: 'Server=b;Database=New', tgtConnMode: 'direct' } }));
    localStorage.setItem('cygenix_projects', JSON.stringify([{
      id: 'p1', name: 'Demo migration', client: 'Acme', phase: 'Build',
      start: '2026-01-05', end: '2026-12-01', description: 'Legacy SQL Server to Azure SQL' }]));
    localStorage.setItem('cygenix_active_project_id', 'p1');
    localStorage.setItem('cygenix_jobs', JSON.stringify(jobs));
  }, JOBS);

  const open = async () => {
    await page.goto('http://localhost:' + PORT + '/dashboard', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => document.querySelectorAll('.mp-stage').length > 0,
      null, { timeout: 20000 });
    await page.waitForTimeout(1500);       // the delayed re-render settles
  };
  const stages = () => page.evaluate(() =>
    Array.from(document.querySelectorAll('.mp-stage')).map((el) => ({
      key: el.querySelector('.mp-stage-key').textContent.trim(),
      count: el.querySelector('.mp-stage-count').textContent.trim(),
      state: el.querySelector('.mp-state').textContent.trim(),
      href: el.getAttribute('href'),
      bottleneck: !!el.querySelector('.mp-bottleneck'),
    })));

  console.log('Migration pipeline — the narrative reaches the screen\n');

  await open();

  /* ── 1. It is there, above the tiles ────────────────────────────────────── */

  const st = await stages();
  check('all six stages render, in lifecycle order',
    st.map((s) => s.key).join(',') === 'Connect,Analyse,Map,Generate,Validate,Cutover',
    st.map((s) => s.key).join(','));
  check('above the KPI tiles, where the overview starts',
    await page.evaluate(() => {
      const mp = document.getElementById('migration-pipeline');
      const tiles = document.querySelector('#view-dashboard .stats-row');
      return !!mp && !!tiles
        && (mp.compareDocumentPosition(tiles) & Node.DOCUMENT_POSITION_FOLLOWING) !== 0;
    }));
  check('the KPI tiles are still there — this replaces nothing',
    (await page.evaluate(() => document.querySelectorAll('#view-dashboard .stats-row .stat-card').length)) >= 5);
  check('every stage carries a denominator rather than a bare number',
    st.every((s) => /\bof\b/.test(s.count)), st.map((s) => s.count).join(' | '));
  check('every stage is a link to the screen that acts on it',
    st.every((s) => s.href && s.href.length > 1), JSON.stringify(st.map((s) => s.href)));
  // Against the real routing table, not this file's toy server: the console's
  // addresses are extensionless and only public/_redirects knows which file
  // each one serves. A chip linking to a 404 is a diagnosis with no treatment.
  const REDIRECTS = fs.readFileSync(path.join(PUB, '_redirects'), 'utf8');
  const served = (href) => {
    const p2 = href.split('#')[0];
    return new RegExp('^' + p2.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      + '\\s+\\S+\\s+200', 'm').test(REDIRECTS);
  };
  check('and those links go somewhere this site serves',
    st.every((s) => served(s.href)),
    st.map((s) => s.href).filter((h) => !served(h)).join(' | '));

  /* ── 2. One bottleneck, and it is the one the sentence is about ─────────── */

  const hot = st.filter((s) => s.bottleneck);
  check('at most one stage is labelled BOTTLENECK', hot.length <= 1, hot.length);
  check('and with this data it is Map, which is the stage that is behind',
    hot.length === 1 && hot[0].key === 'Map', JSON.stringify(hot));
  check('the label is a word, not only a colour',
    await page.evaluate(() => {
      const el = document.querySelector('.mp-bottleneck');
      return !!el && /bottleneck/i.test(el.textContent);
    }));
  check('the call to action goes to that same stage',
    /object-mapping/.test(await page.evaluate(() =>
      document.querySelector('.mp-cta').getAttribute('href'))),
    await page.evaluate(() => document.querySelector('.mp-cta').getAttribute('href')));

  /* ── 3. The narrative is on screen at first paint ───────────────────────── */

  const narrative = await page.evaluate(() =>
    document.getElementById('mp-narrative-text').textContent.trim());
  check('a narrative sentence is present without anyone clicking anything',
    narrative.length > 40, narrative);
  check('and it names the contradiction in the data rather than a tidier stage',
    /should not be able to disagree/.test(narrative), narrative);
  check('no Anthropic call was made to produce it',
    !offSite.some((u) => /anthropic/i.test(u)), offSite.filter((u) => /anthropic/i.test(u)).join(' '));
  check('the "Generate summary" button is gone, along with the panel it sat in',
    await page.evaluate(() => !document.getElementById('ps-ai-btn')
      && !/Generate summary/.test(document.body.textContent)));
  check('a regenerate control is still offered, for when a rewrite is wanted',
    await page.evaluate(() => !!document.querySelector('.mp-asof button')));

  /* ── 4. Confidence, taken apart ─────────────────────────────────────────── */

  await page.evaluate(() => toggleConfidenceDetail());
  await page.waitForTimeout(200);
  const conf = await page.evaluate(() => ({
    score: document.getElementById('stat-confidence').textContent.trim(),
    rows: Array.from(document.querySelectorAll('.mp-conf-row')).map((r) => r.textContent.replace(/\s+/g, ' ').trim()),
    sum: document.querySelector('.mp-conf-sum').textContent.replace(/\s+/g, ' ').trim(),
  }));
  check('clicking the tile opens a driver breakdown', conf.rows.length >= 3, JSON.stringify(conf.rows));
  check('the contributions shown add up to the score displayed',
    (() => {
      const parts = (conf.sum.match(/\+\d+/g) || []).map(Number);
      const shown = Number(conf.score);
      return parts.length > 0 && Math.abs(parts.reduce((a, b) => a + b, 0) - shown) <= 1;
    })(), conf.sum + '  vs  ' + conf.score);
  check('a component with no data says so instead of scoring zero',
    conf.rows.some((r) => /not counted/.test(r)), JSON.stringify(conf.rows));
  check('and every component that is costing points offers a way to fix it',
    await page.evaluate(() => !!document.querySelector('.mp-conf-fix')));

  /* ── 5. Empty states say why, not "—" ───────────────────────────────────── */

  const dashes = await page.evaluate(() =>
    Array.from(document.querySelectorAll('#view-dashboard .stat-val, #view-dashboard .ps-card-value'))
      .map((e) => e.textContent.trim()).filter((t) => t === '—' || t === ''));
  check('no headline value on the overview is a bare em-dash',
    dashes.length === 0, JSON.stringify(dashes));
  check('the Files Processed zero says which kind of zero it is',
    /none uploaded yet/.test(await page.evaluate(() =>
      document.getElementById('stat-files-sub').textContent)));
  check('the two job tiles state the population they count',
    (await page.evaluate(() => document.querySelectorAll('#view-dashboard .stat-scope').length)) >= 3,
    'four tiles count every project while the panel below counts one');

  /* ── 6. Readable without colour, and in the dark ────────────────────────── */

  const legible = () => page.evaluate(() => {
    const bad = [];
    document.querySelectorAll('#migration-pipeline .mp-state, #migration-pipeline .mp-stage-count')
      .forEach((el) => {
        const s = getComputedStyle(el);
        if (!el.textContent.trim()) bad.push('empty: ' + el.className);
        if (s.visibility === 'hidden' || s.display === 'none') bad.push('hidden: ' + el.className);
      });
    return bad;
  });

  check('every state carries a word, so colour is never the only signal',
    (await page.evaluate(() =>
      Array.from(document.querySelectorAll('.mp-state')).map((e) => e.textContent.trim()))
    ).every((t) => t.length > 2));
  check('nothing in the card is empty or hidden in the default theme',
    (await legible()).length === 0, (await legible()).join(' | '));

  await page.evaluate(() => document.documentElement.setAttribute('data-theme', 'dark'));
  await page.waitForTimeout(150);
  check('the card survives dark mode', (await legible()).length === 0, (await legible()).join(' | '));
  check('and the stage text keeps a real colour against the dark ground',
    await page.evaluate(() => {
      const el = document.querySelector('.mp-stage-count');
      const c = getComputedStyle(el).color;
      return !!c && c !== 'rgba(0, 0, 0, 0)';
    }));

  await page.evaluate(() => {
    document.documentElement.setAttribute('data-theme', 'light');
    document.documentElement.classList.add('a11y-hc');
  });
  await page.waitForTimeout(150);
  check('and high contrast', (await legible()).length === 0, (await legible()).join(' | '));
  check('with the bottleneck still named in words under high contrast',
    await page.evaluate(() => {
      const el = document.querySelector('.mp-bottleneck');
      return !!el && el.textContent.trim().length > 3;
    }));
  await page.evaluate(() => document.documentElement.classList.remove('a11y-hc'));

  /* ── 7. Narrow ──────────────────────────────────────────────────────────── */

  await page.setViewportSize({ width: 620, height: 900 });
  await page.waitForTimeout(300);
  check('under 720px the row stacks instead of scrolling off the side',
    await page.evaluate(() => {
      const row = document.querySelector('.mp-row');
      return getComputedStyle(row).flexDirection === 'column';
    }));
  check('and the page itself does not scroll sideways',
    await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 2),
    await page.evaluate(() => document.documentElement.scrollWidth + ' vs ' + window.innerWidth));
  await page.setViewportSize({ width: 1500, height: 1000 });

  /* ── 8. Nothing threw ───────────────────────────────────────────────────── */

  const real = errors.filter((e) => !/ResizeObserver|Failed to fetch|NetworkError|Load failed/.test(e));
  check('no page errors while rendering all of that', real.length === 0, real.join(' | '));

  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  await browser.close();
  server.close();
  process.exit(fail ? 1 : 0);
})().catch((e) => {
  console.error('\nSMOKE CRASHED: ' + (e && e.stack || e));
  try { server.close(); } catch (_) {}
  process.exit(1);
});
