// tests/browser/schema-export.smoke.js
//
// tests/schema-export.test.js pins the file formats against a hand-built
// model. This asks the browser the questions that model cannot answer:
//
//   * does the page's own diagram state turn into that model correctly —
//     positions, shown columns, edge anchors, the filtered table set;
//   * does the SVG that comes out actually PARSE and lay out, rather than
//     merely being a plausible-looking string;
//   * does the print path put the diagram in the document and take it out
//     again, leaving the page as it found it;
//   * does the export honour the filters, which is the difference between
//     "28 tables" and "12,244".
//
// The schema comes from a stubbed CygenixSchemaGraph rather than a database.
// What is being tested is the export, and a test that needs SQL Server is a
// test that does not run.
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = 8402;
const EXE = '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.txt': 'text/plain; charset=utf-8' };

function serve() {
  return http.createServer((req, res) => {
    let p = decodeURIComponent(req.url.split('?')[0].split('#')[0]);
    if (p === '/') p = '/index.html';
    let file = path.join(PUB, p);
    if (!fs.existsSync(file) && fs.existsSync(file + '.html')) file += '.html';
    if (!fs.existsSync(file)) {
      const alt = path.join(PUB, p.replace(/-/g, '_') + '.html');
      if (fs.existsSync(alt)) file = alt;
    }
    if (!fs.existsSync(file) || fs.statSync(file).isDirectory()) {
      res.writeHead(404, { 'Content-Type': TYPES['.html'] }); return res.end('not found');
    }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream' });
    fs.createReadStream(file).pipe(res);
  }).listen(PORT);
}

/* The schema, served where a database would be. db-connect is intercepted at
   the network rather than CygenixSchemaGraph being replaced in the page: the
   module attaches itself to window unconditionally, so a stub installed before
   it loads is simply overwritten — and stubbing it would have skipped the real
   fetch, cache and buildGraph path that the export then reads from. Three
   tables and one foreign key is enough; one table is filtered out later, which
   is the scope test. */
const SCHEMA = {
  database: 'Conv_DM',
  tables: [
    { schema: 'dbo', name: 'TimeType', kind: 'table', rowCount: 15 },
    { schema: 'dbo', name: 'Timecard', kind: 'table', rowCount: 573106 },
    { schema: 'dbo', name: 'ZZ_Scratch', kind: 'table', rowCount: 0 },
  ],
  foreignKeys: [{ fromSchema: 'dbo', fromTable: 'Timecard', fromColumn: 'TimeType',
    toSchema: 'dbo', toTable: 'TimeType', toColumn: 'Code', name: 'FK_Timecard_TimeType' }],
  columns: {
    'dbo.TimeType': { columns: [{ name: 'Code', type: 'NVARCHAR(10)', nullable: false, ordinal: 1 },
                                { name: 'Descr', type: 'NVARCHAR(200)', nullable: true, ordinal: 2 }],
                      primaryKeys: ['Code'] },
    'dbo.Timecard': { columns: [{ name: 'Id', type: 'INT', nullable: false, ordinal: 1 },
                                { name: 'TimeType', type: 'NVARCHAR(10)', nullable: true, ordinal: 2 }],
                      primaryKeys: ['Id'] },
    'dbo.ZZ_Scratch': { columns: [{ name: 'Id', type: 'INT', nullable: false, ordinal: 1 }],
                        primaryKeys: ['Id'] },
  },
};

function dbRoute(route) {
  let body = {};
  try { body = JSON.parse(route.request().postData() || '{}'); } catch { /* handled below */ }
  let payload;
  if (body.action === 'schema-tables') payload = { database: SCHEMA.database, tables: SCHEMA.tables };
  else if (body.action === 'schema-fks') payload = { foreignKeys: SCHEMA.foreignKeys };
  else if (body.action === 'schema-columns') {
    const k = body.schemaName + '.' + body.tableName;
    payload = { table: SCHEMA.columns[k] || { columns: [], primaryKeys: [] } };
  } else payload = { success: false, error: 'unexpected action: ' + body.action };
  return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(payload) });
}

const seed = () => {
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
  sessionStorage.setItem('cygenix_rbac_me', JSON.stringify({
    at: Date.now(), me: { oid: 'x', email: 'you@example.test', roles: ['OW', 'PA'] } }));
  // No saved filters or cached schema from a previous run.
  localStorage.removeItem('cygenix_schemaexp_filters');
  for (const k of Object.keys(localStorage)) {
    if (k.indexOf('cygenix_schema_') === 0) localStorage.removeItem(k);
  }
  // A target connection, so CygenixSchemaGraph has somewhere to ask. The
  // requests go to the intercepted db-connect, never to a database.
  localStorage.setItem('cygenix_project_connections', JSON.stringify({
    'you@example.test': { tgtConnString: 'mssql://u:p@h:1433/Conv_DM', tgtConnMode: 'direct' },
  }));

  // An MSAL IdToken record, because cygenix-auth-token.js wraps window.fetch
  // for every /.netlify/functions/* call and, finding no usable token, awaits
  // getCygenixIdTokenAsync() — a renewal against cygenix.ciamlogin.com, which
  // this test has cut off. Without this the first request resolves and the
  // NEXT one hangs for ever, which cost an afternoon: the diagram drew, the
  // background column prefetch silently never returned, and every export said
  // "columns not loaded". Unsigned, obviously fake, and never sent anywhere
  // real — it exists only so the synchronous cache path is taken.
  const b64 = (o) => btoa(JSON.stringify(o)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  const jwt = b64({ alg: 'none', typ: 'JWT' }) + '.'
    + b64({ sub: 'x', email: 'you@example.test', exp: Math.floor(Date.now() / 1000) + 3600 })
    + '.smoke';
  localStorage.setItem('x-cygenix.ciamlogin.com-idtoken-x', JSON.stringify({
    credentialType: 'IdToken', secret: jwt, homeAccountId: 'x',
    environment: 'cygenix.ciamlogin.com', clientId: 'x', realm: 'x' }));
};

(async () => {
  const server = serve();
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
    // Order matters: Playwright uses the LAST matching route, so the catch-all
    // goes first and the db-connect interceptor after it.
    await ctx.route('**/*', (r) => (r.request().url().startsWith('http://localhost:' + PORT)
      ? r.continue() : r.abort()));
    await ctx.route('**/functions/db-connect', dbRoute);
    const page = await ctx.newPage();
    page.setDefaultTimeout(20000);
    const problems = [];
    page.on('pageerror', (e) => problems.push(e.message));
    await page.addInitScript(seed);

    await page.goto('http://localhost:' + PORT + '/schema_explorer.html', { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.sm-box', { timeout: 20000 });
    await page.waitForTimeout(800);

    check('the diagram drew the seeded tables',
      (await page.locator('.sm-box').count()) === 3);

    /* ── The dialog ─────────────────────────────────────────────────────── */
    await page.click('#sm-export-btn');
    await page.waitForTimeout(150);
    check('the Export panel opens', await page.locator('#sm-export.show').isVisible());
    const scope = await page.locator('#sm-export-scope').innerText();
    check('and leads with the scope rather than a list of file types',
      /3\s+tables on the diagram/.test(scope), scope);

    // Opening it must close Filters — all three panels are absolutely
    // positioned at the same corner, so two open at once is one invisible.
    await page.click('#sm-export-btn');          // close it again first
    await page.click('#sm-filters-btn');
    await page.waitForTimeout(150);
    await page.click('#sm-export-btn');
    await page.waitForTimeout(150);
    check('opening it closes the Filters panel it would otherwise sit on top of',
      await page.locator('#sm-export.show').isVisible()
      && !(await page.locator('#sm-filters.show').count()));

    /* ── The model the page builds ──────────────────────────────────────── */
    const model = await page.evaluate(() => smExportModel());
    check('the model carries a position for every table',
      model.tables.every((t) => model.pos[t.key] && isFinite(model.pos[t.key].x)),
      JSON.stringify(model.pos));
    check('and the geometry the diagram is actually drawn with',
      model.geom.boxW === 210 && model.geom.maxRows === 9, JSON.stringify(model.geom));
    // The whole reason the page passes anchors instead of the export
    // recomputing them: they have to be the same numbers smRenderCanvas used.
    check('every edge carries both anchors, as real coordinates',
      model.edges.length === 1 && ['a', 'b'].every((k) =>
        isFinite(model.edges[0][k].x) && isFinite(model.edges[0][k].y)),
      JSON.stringify(model.edges[0]));
    const anchor = await page.evaluate(() => {
      const a = smAnchor('dbo.Timecard', 'TimeType');
      return { x: a.x, y: a.y };
    });
    check('and they match what the drawn diagram used, not a second calculation',
      model.edges[0].a.x === anchor.x && model.edges[0].a.y === anchor.y,
      JSON.stringify([model.edges[0].a, anchor]));
    check('shown columns come from smShownCols, with the key flags set',
      model.tables.find((t) => t.name === 'Timecard').shownColumns
        .some((c) => c.name === 'TimeType' && c.fk === true),
      JSON.stringify(model.tables.find((t) => t.name === 'Timecard').shownColumns));

    /* ── The SVG actually renders ───────────────────────────────────────── */
    // A string that looks like SVG and an SVG that lays out are different
    // things: one bad attribute and the browser renders nothing at all.
    const svg = await page.evaluate(() => {
      const s = CygenixSchemaExport.diagramSvg(smExportModel());
      const doc = new DOMParser().parseFromString(s, 'image/svg+xml');
      const err = doc.querySelector('parsererror');
      const host = document.createElement('div');
      host.style.cssText = 'position:fixed;left:-9999px;top:0';
      host.innerHTML = s;
      document.body.appendChild(host);
      const el = host.querySelector('svg');
      const box = el ? el.getBoundingClientRect() : null;
      const texts = [...host.querySelectorAll('text')].map((t) => t.textContent);
      const paths = host.querySelectorAll('path').length;
      host.remove();
      return { parseError: err ? err.textContent.slice(0, 120) : null,
        w: box ? Math.round(box.width) : 0, h: box ? Math.round(box.height) : 0,
        texts, paths };
    });
    check('the exported SVG parses as XML', svg.parseError === null, svg.parseError);
    check('and lays out with a real size', svg.w > 100 && svg.h > 100, svg.w + '×' + svg.h);
    check('every table name is in it', ['TimeType', 'Timecard', 'ZZ_Scratch']
      .every((n) => svg.texts.includes(n)), svg.texts.join(','));
    check('the relationship is drawn as a curve', svg.paths >= 1, String(svg.paths));
    check('and the caption travels with the picture',
      svg.texts.some((t) => /Conv_DM · target/.test(t))
      && svg.texts.some((t) => /3 tables on the diagram/.test(t)), svg.texts.join(' | '));

    /* ── Scope follows the filters ──────────────────────────────────────── */
    // This is the finding the whole export hangs on: on the reported database
    // 12,216 tables were hidden. An export that ignored the filters would be
    // a different document from the one on screen.
    await page.evaluate(() => {
      SM.filters.rules = [{ mode: 'starts', text: 'ZZ_', on: true }];
      smFiltersRead ? null : null;
      smRenderCanvas(); smRenderTree();
    });
    await page.waitForTimeout(200);
    // Through the real button, not by calling the formatter: smExportRun is
    // where the lazy columns are fetched, and a test that skipped it would
    // have passed with "columns not loaded" in every row — which is exactly
    // what it did before this was written this way.
    const filtered = await page.evaluate(async () => {
      const got = {};
      const real = window.smDownload;
      window.smDownload = (text, name) => { got.text = text; got.name = name; };
      await smExportRun('columns');
      window.smDownload = real;
      const m = smExportModel();
      return Object.assign(got, { n: m.tables.length, names: m.tables.map((t) => t.name) });
    });
    check('a filtered-out table leaves the export as well as the diagram',
      filtered.n === 2 && !filtered.names.includes('ZZ_Scratch'), filtered.names.join(','));
    check('and the file says how many were left out',
      /1 hidden by filters and not included/.test(filtered.text),
      (filtered.text || '').split('\n').find((l) => /scope/.test(l)));
    check('pressing the export fetches the lazy columns first, so no row says "not loaded"',
      !/columns not loaded/.test(filtered.text),
      (filtered.text || '').split('\n').filter((l) => /not loaded/.test(l)).join(' | '));
    check('the columns CSV holds a row per column of what remains',
      (filtered.text.match(/^dbo,/gm) || []).length === 4,
      (filtered.text.match(/^dbo,.*$/gm) || []).join(' | '));
    check('and the download is named for the database and dated',
      /^cygenix_Conv_DM_columns_\d{4}-\d{2}-\d{2}\.csv$/.test(filtered.name), filtered.name);

    /* ── Print ──────────────────────────────────────────────────────────── */
    // window.print() blocks in a headless browser, so it is stubbed; what is
    // being tested is that the page prepares itself and puts itself back.
    const printed = await page.evaluate(() => {
      const seen = {};
      const real = window.print;
      window.print = () => {
        seen.busy = document.body.classList.contains('se-printing');
        const host = document.getElementById('se-print');
        seen.svgInDoc = !!host.querySelector('svg');
        seen.tables = host.querySelectorAll('rect').length;
      };
      smPrintDiagram(smExportModel());
      window.dispatchEvent(new Event('afterprint'));
      window.print = real;
      return Object.assign(seen, {
        afterBusy: document.body.classList.contains('se-printing'),
        afterEmpty: document.getElementById('se-print').innerHTML === '',
      });
    });
    check('printing puts the diagram in the document first',
      printed.busy === true && printed.svgInDoc === true && printed.tables > 3,
      JSON.stringify(printed));
    check('and afterprint puts the page back, so the app is not left in print mode',
      printed.afterBusy === false && printed.afterEmpty === true, JSON.stringify(printed));

    /* ── The print stylesheet hides the app ─────────────────────────────── */
    // A fixed element that is merely off-screen repeats on every printed page,
    // and this page has four of them.
    const printCss = await page.evaluate(() => {
      const out = [];
      for (const sheet of document.styleSheets) {
        let rules; try { rules = sheet.cssRules; } catch { continue; }
        for (const r of rules) {
          if (r.media && /print/.test(r.conditionText || r.media.mediaText)) out.push(r.cssText);
        }
      }
      return out.join('\n');
    });
    check('the print rules hide the app and reveal only the print container',
      /body > \*/.test(printCss) && /se-printing/.test(printCss) && /display:\s*none/.test(printCss),
      printCss.slice(0, 200));
    check('and ask for landscape', /landscape/.test(printCss));

    check('no console errors', problems.length === 0, problems.join(' | '));
    await ctx.close();
  } finally {
    await browser.close();
    server.close();
  }
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})();
