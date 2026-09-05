// tests/browser/data-analyser.smoke.js
//
// The static suite proves the files are wired up. This proves the tab works:
// the old address lands on the Data import tab of Connections, the widget
// mounts inside it, ONE file is profiled by the analyser and parsed for the
// import's schema preview, SEVERAL files go to the batch as dbo.<filename>
// with the analyser out of the way, the widget follows a theme switch, the
// hand-off opens Object Mapping — and, the one claim that matters
// commercially, nothing about the data leaves the browser.
//
// cdnjs and Google Fonts are aborted deliberately, with one exception: the
// import path parses CSV with PapaParse, and without a parser "one file goes
// to the schema preview" could not be exercised at all. So the PapaParse URL
// is answered with a small stand-in that speaks the four options the import
// code uses (header, transformHeader, step, complete). The analyser has its
// own parser and needs nothing. xlsx and js-yaml stay blocked: the optional
// libraries must degrade to a message rather than a broken tab, and a
// sandbox with no egress is the cheapest way to prove that.
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = 8398;
const EXE = '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const TYPES = {
  '.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8', '.svg': 'image/svg+xml',
};

// Clean addresses are hyphenated (/object-mapping); some files are not
// (object_mapping.html). Production bridges that in public/_redirects, so the
// toy server reads the same table rather than guessing — appending .html to
// the address served a 404 for Object Mapping and the smoke could not tell,
// because the URL still matched. The 301 rules are honoured too, so the old
// /data-analyser address is exercised the way Netlify would serve it.
const RULES = (() => {
  const serve = {}, redirect = {};
  try {
    fs.readFileSync(path.join(PUB, '_redirects'), 'utf8').split('\n').forEach((ln) => {
      const m = ln.trim().match(/^(\/\S*)\s+(\S+)\s+(200|301!?)$/);
      if (!m) return;
      if (m[3] === '200') serve[m[1]] = m[2]; else redirect[m[1]] = m[2];
    });
  } catch (e) { /* no table — fall back to the .html guess below */ }
  return { serve, redirect };
})();

function serve() {
  return http.createServer((req, res) => {
    let p = decodeURIComponent(req.url.split('?')[0]);
    if (RULES.redirect[p]) { res.writeHead(301, { Location: RULES.redirect[p] }); return res.end(); }
    if (p === '/') p = '/index.html';
    if (RULES.serve[p]) p = RULES.serve[p];
    let file = path.join(PUB, p);
    if (!fs.existsSync(file) && fs.existsSync(file + '.html')) file += '.html';
    if (!fs.existsSync(file) || fs.statSync(file).isDirectory()) {
      res.writeHead(404, { 'Content-Type': TYPES['.html'] }); return res.end('not found');
    }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream' });
    fs.createReadStream(file).pipe(res);
  }).listen(PORT);
}

function seed(arg) {
  const token = arg.token, expires = arg.expires;
  [localStorage, sessionStorage].forEach((s) => {
    s.setItem('cygenix_token', token);
    s.setItem('cygenix_expires', String(expires));
  });
  localStorage.setItem('cygenix_onboarded', 'true');
  localStorage.setItem('cygenix_user', JSON.stringify({ email: 'smoke@example.com' }));
  localStorage.setItem('cygenix_tier', 'pro');
  localStorage.setItem('cygenix_cookie_consent', 'all');
  localStorage.setItem('acct-cygenix.ciamlogin.com-h.t', JSON.stringify({
    homeAccountId: 'h.t', environment: 'cygenix.ciamlogin.com', tenantId: 't',
    username: 'smoke@example.com', localAccountId: 'l', authorityType: 'MSSTS', name: 'Smoke Test',
  }));
  localStorage.setItem(
    'h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-',
    JSON.stringify({ credentialType: 'IdToken', secret: token, expiresOn: String(Math.floor(expires / 1000)) })
  );
}

// A stand-in for PapaParse, enough for the import code's calls: header row,
// quoted fields with commas inside, `step` per row with an abortable parser,
// `complete` with {data, errors}. Not a CSV library; a smoke's crutch.
const PAPA_STUB = `
window.Papa = { parse: function (file, o) {
  var fr = new FileReader();
  fr.onload = function () {
    var text = String(fr.result || '');
    var lines = text.split(/\\r?\\n/).filter(function (l) { return !o.skipEmptyLines || l.trim() !== ''; });
    var split = function (line) {
      var out = [], cur = '', q = false;
      for (var i = 0; i < line.length; i++) {
        var c = line[i];
        if (c === '"') { q = !q; continue; }
        if (c === ',' && !q) { out.push(cur); cur = ''; continue; }
        cur += c;
      }
      out.push(cur); return out;
    };
    var head = split(lines.shift() || '').map(function (h) { return o.transformHeader ? o.transformHeader(h) : h; });
    var data = [], aborted = false;
    var parser = { abort: function () { aborted = true; } };
    lines.forEach(function (line) {
      if (aborted) return;
      var cells = split(line), row = {};
      head.forEach(function (h, i) { row[h] = cells[i] === undefined ? '' : cells[i]; });
      if (o.step) o.step({ data: row }, parser); else data.push(row);
    });
    if (o.complete) o.complete({ data: data, errors: [] });
  };
  fr.readAsText(file);
} };`;

// Something recognisable, with the traps the profiler exists to catch.
const SAMPLE = [
  'order_id,customer,order_date,amount,postcode,active',
  '1001,"Smith, John",01/02/2024,£1042.50,LS1 4AP,true',
  '1002,"O\'Brien, Ann",03/04/2024,£3.00,00417,false',
  '1003,Bob Jones,05/06/2024,£12000.00,HU1 1AA,true',
].join('\n');
const SECOND = 'customer_id,name,joined\n7,Whitfield & Sons,2024-01-09\n8,Halcyon Media,2024-02-11\n';

(async () => {
  const server = serve();
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext();
    const b64 = (o) => Buffer.from(JSON.stringify(o)).toString('base64url');
    const token = b64({ alg: 'none' }) + '.' + b64({ preferred_username: 'smoke@example.com', exp: Math.floor(Date.now() / 1000) + 3600 }) + '.sig';
    await ctx.addInitScript(seed, { token, expires: Date.now() + 3600e3 });

    const requests = [];
    await ctx.route('**/*', (route) => {
      const url = route.request().url();
      requests.push({ url, method: route.request().method(), post: route.request().postData() || '' });
      if (/cdnjs\.cloudflare\.com\/ajax\/libs\/PapaParse\//.test(url)) {
        return route.fulfill({ status: 200, contentType: 'application/javascript', body: PAPA_STUB });
      }
      if (/cdnjs\.cloudflare\.com|cdn\.jsdelivr\.net|fonts\.g(oogleapis|static)\.com/.test(url)) return route.abort();
      // auth-gate asks whoami and sends a verified answer with no tier to
      // /pick-plan — correctly. It fails OPEN on a failure and CLOSED on an
      // empty record, so the stub has to answer like a subscribed account.
      if (/data-proxy.*action=whoami/.test(url)) return route.fulfill({ status: 200, contentType: 'application/json',
        body: JSON.stringify({ email: 'smoke@example.com', tier: 'pro', tier_status: 'trialing' }) });
      if (/data-proxy.*action=save/.test(url)) return route.fulfill({ status: 200, contentType: 'application/json',
        body: JSON.stringify({ saved: true, updatedAt: new Date().toISOString() }) });
      if (/data-proxy|\.netlify\/functions\//.test(url)) return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
      return route.continue();
    });

    const page = await ctx.newPage();
    const errors = [];
    page.on('pageerror', (e) => errors.push(e.message));

    // ── The old address lands on the tab ───────────────────────────────
    await page.goto('http://localhost:' + PORT + '/data-analyser', { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('#analyser.cygx', { timeout: 20000 });
    check('/data-analyser redirects to the dashboard', /\/dashboard/.test(page.url()), page.url());
    check('and the goto parameter has been consumed out of the address', !/goto=/.test(page.url()), page.url());
    check('the Connections view is showing', await page.locator('#view-connections').isVisible());
    const activeTab = await page.evaluate(() => {
      const t = document.querySelector('#view-connections .cx-tab.cx-tab-active');
      return t ? { tab: t.dataset.tab, text: t.textContent.trim() } : null;
    });
    check('with the Data import tab selected, under its old name',
      !!activeTab && activeTab.tab === 'import' && activeTab.text === 'Data import', JSON.stringify(activeTab));
    check('and the tab panel visible', await page.locator('#conn-tab-import').isVisible());
    // The sixth entry in the strip is the Data Generator, a link to its own
    // page rather than a tab with a panel, so it carries no data-tab.
    const strip = await page.evaluate(() => Array.from(document.querySelectorAll('#view-connections .cx-tabs .cx-tab, #view-connections .cx-tab'))
      .filter((t, i, a) => a.indexOf(t) === i).map((t) => t.dataset.tab || t.textContent.trim()).join(','));
    check('the tab strip keeps its order', strip === 'databases,import,restore,linked,blob,Data Generator', strip);

    // ── The menu and the mount ─────────────────────────────────────────
    check('the widget mounted inside the tab', await page.locator('#conn-tab-import #analyser.cygx').count() === 1);
    check('it is titled for the feature, not the vendor\'s name',
      (await page.locator('#analyser .cygx-title').innerText()) === 'Data Analyser');
    check('the widget\'s own drop zone is hidden and the tab\'s is the entry point',
      !(await page.locator('#analyser .cygx-drop').isVisible()) && (await page.locator('#imp-drop').isVisible()));
    await page.waitForFunction(() => !!document.querySelector('.cyg-nav-item'), null, { timeout: 15000 });
    const navKeys = await page.evaluate(() => Array.from(document.querySelectorAll('.cyg-nav-item[data-key]')).map((e) => e.dataset.key));
    check('the sidebar has no Data Analyser item any more', !navKeys.includes('data-analyser') && navKeys.includes('connections'), navKeys.join(','));
    check('and nothing on the page links to the old address',
      (await page.locator('a[href="/data-analyser"]').count()) === 0);

    const themeAttr = await page.getAttribute('#analyser', 'data-theme');
    const consoleTheme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    check('the widget wears the console\'s theme', themeAttr === consoleTheme, themeAttr + ' vs ' + consoleTheme);

    // ── ONE file: profiled by the analyser AND parsed for import ───────
    await page.setInputFiles('#imp-file', { name: 'orders.csv', mimeType: 'text/csv', buffer: Buffer.from(SAMPLE) });
    await page.waitForSelector('#analyser .cygx-result:not([hidden])', { timeout: 10000 });
    const verdict = (await page.locator('#analyser .cygx-verdict').innerText()).replace(/\s+/g, ' ');
    check('the file is identified as CSV', /\bcsv\b/i.test(verdict), verdict);
    check('and the verdict shows row and column counts', /\b3\b/.test(verdict) && /\b6\b/.test(verdict), verdict);

    const flags = (await page.locator('#analyser .cygx-flags').innerText().catch(() => '')).replace(/\s+/g, ' ');
    check('the ambiguous d/m/y dates are flagged', /AMBIGUOUS_DATE/.test(flags), flags);
    check('and the currency symbols', /CURRENCY_SYMBOLS/.test(flags), flags);

    const result = await page.evaluate(() => {
      const r = window.CygenixAnalyserPage.result;
      return r ? { cols: r.columns.map((c) => c.name + ':' + c.type) } : null;
    });
    check('the postcode column with a leading-zero value stays string, not number',
      !!result && result.cols.some((c) => /^postcode:string$/.test(c)), JSON.stringify(result));

    const sql = await page.evaluate(() => {
      const pre = document.querySelector('#analyser pre, #analyser textarea.cygx-out, #analyser .cygx-out');
      return pre ? (pre.value || pre.textContent) : '';
    });
    check('the SQL tab holds a CREATE TABLE for SQL Server', /CREATE TABLE \[dbo\]\.\[imported_data\]/.test(sql), sql.slice(0, 200));

    // The import half of the same drop: schema preview and target, as before.
    await page.waitForFunction(() => {
      const w = document.getElementById('imp-schema-wrap');
      return w && w.style.display === 'block';
    }, null, { timeout: 10000 });
    check('the same file went to the import\'s schema preview', await page.locator('#imp-schema-wrap').isVisible());
    check('with a row per column', (await page.locator('#imp-schema-table tbody tr').count()) === 6);
    check('and the target step, defaulting the table to dbo.<filename>',
      (await page.locator('#imp-target-wrap').isVisible()) && (await page.inputValue('#imp-target-table')) === 'dbo.orders',
      await page.inputValue('#imp-target-table'));
    check('the batch panel stayed hidden for one file', !(await page.locator('#imp-batch-wrap').isVisible()));
    check('and the hand-off appears once there is a result', await page.locator('#da-handoff').isVisible());

    // ── SEVERAL files: the batch, with the analyser out of the way ─────
    await page.setInputFiles('#imp-file', [
      { name: 'orders.csv', mimeType: 'text/csv', buffer: Buffer.from(SAMPLE) },
      { name: 'customers 2024.csv', mimeType: 'text/csv', buffer: Buffer.from(SECOND) },
    ]);
    await page.waitForSelector('#imp-batch-wrap', { state: 'visible', timeout: 10000 });
    check('two files open the batch panel', await page.locator('#imp-batch-wrap').isVisible());
    check('and hide the analyser, which profiles one file', !(await page.locator('#imp-analyser-wrap').isVisible()));
    check('and the single-file steps', !(await page.locator('#imp-schema-wrap').isVisible()) && !(await page.locator('#imp-target-wrap').isVisible()));
    const batch = await page.evaluate(() => Array.from(document.querySelectorAll('#imp-batch-table tbody tr'))
      .map((tr) => Array.from(tr.querySelectorAll('td')).slice(0, 2).map((td) => td.textContent.trim()).join(' → ')));
    check('each file is bound for dbo.<filename>',
      batch.length === 2 && /orders\.csv → dbo\.orders$/.test(batch[0]) && /customers 2024\.csv → dbo\.customers_2024$/.test(batch[1]),
      batch.join(' | '));
    check('the destination defaults to the source database', (await page.inputValue('#imp-batch-db')) === 'src');
    await page.waitForFunction(() => /Parsed|parsed/.test(document.querySelector('#imp-batch-table tbody').textContent), null, { timeout: 10000 }).catch(() => {});
    const batchText = (await page.locator('#imp-batch-table tbody').innerText()).replace(/\s+/g, ' ');
    check('both files parsed and are waiting for the import to run', (batchText.match(/waiting to import/g) || []).length === 2, batchText);
    check('the run button is offered', await page.locator('#imp-batch-run-btn').isVisible());

    await page.click('button:has-text("Clear batch")');
    check('clearing the batch brings the analyser back', await page.locator('#imp-analyser-wrap').isVisible());

    // ── The theme follows the console ──────────────────────────────────
    const next = consoleTheme === 'dark' ? 'light' : 'dark';
    await page.evaluate((t) => document.documentElement.setAttribute('data-theme', t), next);
    await page.waitForFunction((t) => document.getElementById('analyser').getAttribute('data-theme') === t, next, { timeout: 3000 });
    check('switching the console theme re-themes the widget without a reload', true);

    // ── Nothing left the page ──────────────────────────────────────────
    const external = requests.filter((r) => !/^http:\/\/localhost:8398\//.test(r.url)
      && !/cdnjs\.cloudflare\.com|cdn\.jsdelivr\.net|fonts\.g(oogleapis|static)\.com/.test(r.url));
    check('no request went anywhere but this origin (plus the aborted font and CDN loads)',
      external.length === 0, external.map((r) => r.url).join(', '));
    const carried = requests.filter((r) => r.post && /Smith, John|LS1 4AP|Halcyon Media/.test(r.post));
    check('and no request carried the data', carried.length === 0, carried.map((r) => r.url).join(', '));

    const unexpected = errors.filter((e) => !/xlsx|jsyaml|XLSX|msal/i.test(e));
    check('no page errors (the aborted optional libraries excepted)', unexpected.length === 0, unexpected.join(' | '));

    // ── Hand it to Object Mapping ──────────────────────────────────────
    await page.evaluate((text) => window.CygenixAnalyserPage.load(text, 'orders.csv'), SAMPLE);
    await page.waitForSelector('#da-handoff', { state: 'visible', timeout: 10000 });
    const before = requests.length;
    await page.click('#da-handoff-btn');
    await page.waitForURL(/\/object-mapping\?edit=job_\d+/, { timeout: 20000 });
    check('clicking the hand-off opens Object Mapping on the new job', /\/object-mapping\?edit=job_/.test(page.url()), page.url());

    const saves = requests.slice(before).filter((r) => /action=save/.test(r.url) && r.method === 'POST');
    check('the profile was flushed to the account before leaving the page', saves.length >= 1);
    const saved = saves.map((r) => r.post).join('\n');
    check('and that save carried the column names', /postcode/.test(saved) && /order_date/.test(saved));
    check('but not one cell value', !/Smith, John|LS1 4AP|00417|1042\.50/.test(saved),
      'the profile travels; the data does not');

    // Object Mapping, with no source connection at all.
    await page.waitForFunction(() => /profiled by the Data Analyser/.test(document.body.innerText), null, { timeout: 20000 });
    const omText = (await page.locator('body').innerText()).replace(/\s+/g, ' ');
    check('Object Mapping shows the file as the source, in its own banner',
      /Source: orders\.csv · CSV · 6 columns · 3 rows · profiled by the Data Analyser/.test(omText), omText.slice(0, 300));
    await page.waitForFunction(() => /\b6 cols\b/.test((document.getElementById('src-col-count') || {}).textContent || ''), null, { timeout: 15000 });
    check('and the six columns are loaded as the source side', true);
    // srcTable is a top-level `let` in a classic script — reachable by bare
    // name from the page's global scope, but NOT a window property.
    const srcTypes = await page.evaluate(() => {
      let t = null;
      try { t = srcTable; } catch (e) { t = null; }
      return t ? t.columns.map((c) => c.name + ':' + c.type) : null;
    });
    check('with SQL types Object Mapping can reason about',
      !!srcTypes && srcTypes.some((c) => /^order_id:INT$/.test(c)) && srcTypes.some((c) => /^postcode:NVARCHAR\(/.test(c)),
      JSON.stringify(srcTypes));
    check('it did not fall back to "Source: not configured"', !/Source: not configured/.test(omText));
  } finally {
    await browser.close();
    server.close();
  }
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})();
