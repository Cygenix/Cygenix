/* tests/browser/sql-windows.smoke.js
 * ---------------------------------------------------------------------------
 * The SQL Editor's query windows, driven in a real browser.
 *
 * WHY THIS CANNOT BE A SOURCE TEST
 * tests/sql-windows.test.js proves the list is right: the order, the cap, the
 * close-and-reopen stack, what reaches storage. None of that is the part
 * people actually feel. What they feel is switching to another tab and
 * finding the cursor where they left it, an undo that still knows what they
 * typed, and yesterday's rows NOT reappearing under today's query. Those are
 * properties of Monaco models, view state and a results panel — they exist
 * only when there is a browser, and a grep against the source would pass
 * whether or not any of it worked.
 *
 * MONACO
 * The page loads Monaco from cdnjs. Rather than reach the network mid-test
 * (and rather than commit 13MB of editor to the repository) the CDN requests
 * are fulfilled from a copy fetched once with `npm pack monaco-editor` and
 * cached in the system temp directory. Point MONACO_VS at an existing
 * min/vs directory to skip the fetch.
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/sql-windows.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFileSync } = require('child_process');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8401);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const MONACO_VERSION = '0.45.0';
const CDN = 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/' + MONACO_VERSION + '/min/vs/';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

/* ── Monaco, fetched once and cached ─────────────────────────────────────── */

function monacoDir() {
  if (process.env.MONACO_VS) return process.env.MONACO_VS;
  const cache = path.join(os.tmpdir(), 'cygenix-monaco-' + MONACO_VERSION);
  const vs = path.join(cache, 'min', 'vs');
  if (fs.existsSync(path.join(vs, 'loader.js'))) return vs;
  fs.mkdirSync(cache, { recursive: true });
  console.log('  …fetching monaco-editor@' + MONACO_VERSION + ' (once; cached in ' + cache + ')');
  try {
    execFileSync('npm', ['pack', 'monaco-editor@' + MONACO_VERSION], { cwd: cache, stdio: 'ignore' });
    execFileSync('tar', ['-xzf', 'monaco-editor-' + MONACO_VERSION + '.tgz',
      '-C', cache, '--strip-components=1', 'package/min'], { cwd: cache, stdio: 'ignore' });
  } catch (e) {
    console.error('\nCould not fetch Monaco. Point MONACO_VS at a monaco-editor min/vs\n' +
                  'directory, or run: npm pack monaco-editor@' + MONACO_VERSION + '\n');
    throw e;
  }
  return vs;
}

// charset matters: without it the browser reads UTF-8 source as latin-1.
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.json': 'application/json',
  '.ttf': 'font/ttf', '.map': 'application/json' };

const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/') p = '/index.html';
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';   // the extensionless rewrite
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) {
    res.writeHead(404); return res.end('no');
  }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  const VS = monacoDir();
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('dialog', (d) => d.accept());          // close-a-dirty-window confirms

  /* Everything the page asks for: local files, Monaco from the cache, the
     database call from the stub below. Nothing leaves the machine. */
  let dbCalls = 0;
  await page.route('**/*', async (route) => {
    const url = route.request().url();

    if (url.startsWith(CDN)) {
      let rel = url.slice(CDN.length).split('?')[0];
      if (rel === 'loader.min.js') rel = 'loader.js';           // the CDN's name for it
      const f = path.join(VS, rel);
      if (!f.startsWith(VS) || !fs.existsSync(f)) return route.fulfill({ status: 404, body: '' });
      return route.fulfill({
        status: 200,
        contentType: TYPES[path.extname(f)] || 'application/octet-stream',
        body: fs.readFileSync(f),
      });
    }

    // The database. Row count, delay and failure are all chosen by the SQL, so
    // a test can say what it wants without a second channel.
    if (url.indexOf('/fake-db') !== -1) {
      dbCalls++;
      const sql = JSON.parse(route.request().postData() || '{}').sql || '';
      if (/BOOM/.test(sql)) {
        return route.fulfill({ status: 500, contentType: 'application/json',
          body: JSON.stringify({ error: 'Invalid object name BOOM.' }) });
      }
      const n = Number((sql.match(/ROWS\((\d+)\)/) || [, 2])[1]);
      if (/SLOW/.test(sql)) await sleep(1500);
      const rows = Array.from({ length: n }, (_, i) => ({ id: i + 1, note: sql.slice(0, 24) }));
      return route.fulfill({ status: 200, contentType: 'application/json',
        body: JSON.stringify({ recordset: rows }) });
    }

    if (url.startsWith('http://localhost:' + PORT)) return route.continue();
    return route.abort();
  });

  await page.addInitScript((port) => {
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
    // Both sides point at the stub, so switching connection is a real switch.
    localStorage.setItem('cygenix_project_connections', JSON.stringify({
      'you@example.test': {
        srcConnString: 'http://localhost:' + port + '/fake-db?side=source',
        tgtConnString: 'http://localhost:' + port + '/fake-db?side=target',
      },
    }));
  }, PORT);

  const url = (p) => 'http://localhost:' + PORT + p;

  /* ── Page helpers ──────────────────────────────────────────────────────── */

  const open = async () => {
    await page.goto(url('/sql-editor'), { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => window.__cygMonaco && window.__cygMonaco.ready,
      null, { timeout: 30000 });
    await page.waitForFunction(() => document.querySelectorAll('.sqlw-tab').length > 0,
      null, { timeout: 15000 });
    await page.waitForTimeout(200);
  };
  const tabs = () => page.evaluate(() =>
    Array.from(document.querySelectorAll('.sqlw-tab')).map((el) => ({
      id: el.dataset.id,
      name: el.querySelector('.sqlw-name').textContent,
      active: el.classList.contains('active'),
      dirty: (el.querySelector('.sqlw-dot') || {}).textContent === '•',
      running: !!el.querySelector('.sqlw-spin'),
      hidden: el.style.display === 'none',
    })));
  const names = async () => (await tabs()).map((t) => t.name);
  const activeName = async () => ((await tabs()).find((t) => t.active) || {}).name;
  const editorText = () => page.evaluate(() => window.__cygMonaco.editor.getValue());
  const clickTab = async (name) => {
    await page.evaluate((n) => {
      const el = Array.from(document.querySelectorAll('.sqlw-tab'))
        .find((t) => t.querySelector('.sqlw-name').textContent === n);
      if (el) el.click();
    }, name);
    await page.waitForTimeout(150);
  };
  // Type the way a person does, so Monaco builds real undo stops.
  const typeInEditor = async (text) => {
    await page.click('#monaco-host .monaco-editor .view-lines');
    await page.keyboard.type(text, { delay: 6 });
    await page.waitForTimeout(120);
  };
  const runNow = async () => {
    await page.click('#run-btn');
  };
  const resultRows = () => page.evaluate(() =>
    document.querySelectorAll('#results-container table.results-table tbody tr').length);
  const resultsText = () => page.evaluate(() =>
    document.getElementById('results-container').textContent.trim());
  const historyCount = () => page.evaluate(() =>
    document.querySelectorAll('#history-list [onclick^="loadFromHistory"]').length);
  const stored = () => page.evaluate(() => {
    const k = Object.keys(localStorage).find((x) => x.indexOf('cygenix_sql_windows::') === 0);
    return k ? { key: k, raw: localStorage.getItem(k), doc: JSON.parse(localStorage.getItem(k)) } : null;
  });

  console.log('SQL windows — several queries open at once, like browser tabs\n');

  // A clean slate: the persistence checks near the end depend on knowing what
  // was there before them.
  await page.goto(url('/sql-editor'), { waitUntil: 'domcontentloaded' });
  await page.evaluate(() => Object.keys(localStorage)
    .filter((k) => k.indexOf('cygenix_sql_windows::') === 0)
    .forEach((k) => localStorage.removeItem(k)));
  await open();

  /* ── 1. The strip exists and starts with one window ─────────────────────── */

  check('a fresh editor opens with exactly one window', (await tabs()).length === 1);
  check('and it is the active one', (await tabs())[0].active);
  check('the strip sits above the toolbar, where a tab strip belongs',
    await page.evaluate(() => {
      const s = document.getElementById('sqlw-strip');
      const t = document.querySelector('.editor-toolbar');
      return !!s && !!t && (s.compareDocumentPosition(t) & Node.DOCUMENT_POSITION_FOLLOWING) !== 0;
    }));

  /* ── 2. Text is per window ──────────────────────────────────────────────── */

  await typeInEditor('SELECT one');
  check('typing marks the window as having unsaved changes', (await tabs())[0].dirty);

  await page.click('#sqlw-new');
  await page.waitForTimeout(200);
  check('the + button opens a second window', (await tabs()).length === 2);
  check('the new window is active', (await activeName()) === (await names())[1]);
  check('and it opens empty rather than inheriting the first window\'s SQL',
    (await editorText()) === '', await editorText());

  await typeInEditor('SELECT two');
  const [n1, n2] = await names();
  await clickTab(n1);
  check('switching back brings the first window\'s SQL with it',
    (await editorText()) === 'SELECT one', await editorText());
  await clickTab(n2);
  check('and forward again brings the second\'s',
    (await editorText()) === 'SELECT two', await editorText());

  /* ── 3. Undo history survives the switch ────────────────────────────────── */
  //
  // This is the one that would quietly not work if all the windows shared a
  // single Monaco model: undo would walk back through the OTHER window's
  // edits. Each window owns a model, and a model owns its undo stack.

  await clickTab(n1);
  await page.keyboard.press('End');
  await page.keyboard.press('Enter');            // an undo stop
  await typeInEditor('FROM t');
  const full1 = await editorText();
  check('the first window now has two lines', /SELECT one\nFROM t/.test(full1), full1);

  await clickTab(n2);
  await typeInEditor(' -- edited elsewhere');
  await clickTab(n1);
  await page.click('#monaco-host .monaco-editor .view-lines');
  await page.keyboard.press('Control+z');
  await page.waitForTimeout(150);
  const undone = await editorText();
  check('undo after a round trip through another window steps back through THIS window',
    undone !== full1 && /SELECT one/.test(undone), undone);
  check('and it did not reach into the other window\'s edits',
    undone.indexOf('edited elsewhere') === -1, undone);

  await page.keyboard.press('Control+Shift+z');
  await page.waitForTimeout(150);
  if ((await editorText()) !== full1) { await page.keyboard.press('Control+y'); await page.waitForTimeout(150); }
  check('redo puts it back', (await editorText()) === full1, await editorText());
  check('and the other window still says what it said',
    await (async () => { await clickTab(n2); return /edited elsewhere/.test(await editorText()); })());

  /* ── 4. Cursor, selection and scroll ────────────────────────────────────── */

  await clickTab(n1);
  await page.evaluate(() => {
    const e = window.__cygMonaco.editor;
    e.setValue(Array.from({ length: 200 }, (_, i) => 'SELECT ' + (i + 1)).join('\n'));
    e.setSelection({ startLineNumber: 120, startColumn: 1, endLineNumber: 122, endColumn: 5 });
    e.revealLineInCenter(120);
  });
  await page.waitForTimeout(200);
  const before = await page.evaluate(() => ({
    sel: window.__cygMonaco.editor.getSelection(),
    top: Math.round(window.__cygMonaco.editor.getScrollTop()),
  }));
  await clickTab(n2);
  await clickTab(n1);
  await page.waitForTimeout(200);
  const after = await page.evaluate(() => ({
    sel: window.__cygMonaco.editor.getSelection(),
    top: Math.round(window.__cygMonaco.editor.getScrollTop()),
  }));
  check('the selection comes back exactly where it was',
    after.sel.startLineNumber === before.sel.startLineNumber &&
    after.sel.endLineNumber === before.sel.endLineNumber &&
    after.sel.endColumn === before.sel.endColumn,
    JSON.stringify(after.sel));
  check('and so does the scroll position, not the top of the file',
    before.top > 100 && Math.abs(after.top - before.top) <= 2, before.top + ' → ' + after.top);

  /* ── 5. The connection belongs to the window ────────────────────────────── */

  await clickTab(n2);
  await page.selectOption('#conn-select', 'target');
  await page.waitForTimeout(120);
  await clickTab(n1);
  check('a window pointed at the source stays pointed at the source',
    (await page.inputValue('#conn-select')) === 'source');
  await clickTab(n2);
  check('and the one pointed at the target stays pointed at the target',
    (await page.inputValue('#conn-select')) === 'target',
    'two windows on two databases is the whole reason for this feature');

  /* ── 6. Results are cached per window, and never re-run ─────────────────── */

  await page.selectOption('#conn-select', 'source');
  await clickTab(n1);
  await page.evaluate(() => window.__cygMonaco.editor.setValue('SELECT ROWS(2) FROM a'));
  await runNow();
  await page.waitForFunction(() =>
    document.querySelectorAll('#results-container table tbody tr').length > 0, null, { timeout: 15000 });
  check('running a query in the first window shows its rows', (await resultRows()) === 2);
  const afterFirstRun = dbCalls;

  await clickTab(n2);
  await page.evaluate(() => window.__cygMonaco.editor.setValue('SELECT ROWS(5) FROM b'));
  check('a window that has not been run shows its own empty state, not the last window\'s rows',
    /Run a query/.test(await resultsText()), await resultsText());
  await runNow();
  await page.waitForFunction(() =>
    document.querySelectorAll('#results-container table tbody tr').length === 5, null, { timeout: 15000 });
  check('and running there shows five rows', (await resultRows()) === 5);

  const beforeSwitch = dbCalls;
  await clickTab(n1);
  await page.waitForTimeout(300);
  check('switching back shows the first window\'s cached rows again', (await resultRows()) === 2);
  check('without going back to the database for them',
    dbCalls === beforeSwitch, dbCalls + ' calls, expected ' + beforeSwitch);
  check('the row count and timing in the footer are that window\'s, too',
    /^2 rows · 2 cols · \d+ms/.test(await page.evaluate(() =>
      document.getElementById('results-footer').textContent)),
    await page.evaluate(() => document.getElementById('results-footer').textContent));
  check('and the first run really did reach the database once',
    afterFirstRun >= 1);

  /* ── 7. History is per window ───────────────────────────────────────────── */

  await page.click('[onclick*="history"]').catch(() => {});
  const hist1 = await page.evaluate(() => {
    const el = document.querySelector('.bottom-tab, [onclick*="switchBottomTab(\'history\')"]');
    if (el) el.click();
    return document.querySelectorAll('#history-list [onclick^="loadFromHistory"]').length;
  });
  check('the first window\'s History has its one run in it', hist1 === 1, hist1);
  await clickTab(n2);
  await page.waitForTimeout(150);
  check('and the second window\'s History has its own, not both',
    (await historyCount()) === 1, await historyCount());
  check('the entry in view is the query that window ran',
    /ROWS\(5\)/.test(await page.evaluate(() =>
      document.getElementById('history-list').textContent)),
    await page.evaluate(() => document.getElementById('history-list').textContent.slice(0, 120)));

  /* ── 8. An error is cached like a result ────────────────────────────────── */

  await page.click('#sqlw-new');
  await page.waitForTimeout(200);
  const n3 = (await names())[2];
  await page.evaluate(() => window.__cygMonaco.editor.setValue('SELECT * FROM BOOM'));
  await runNow();
  await page.waitForFunction(() =>
    /Invalid object name/.test(document.getElementById('results-container').textContent),
    null, { timeout: 15000 });
  check('a failed query shows its error', /Invalid object name/.test(await resultsText()));
  await clickTab(n1);
  await clickTab(n3);
  await page.waitForTimeout(200);
  check('and the error is still there after a round trip, not silently cleared',
    /Invalid object name/.test(await resultsText()), await resultsText());

  /* ── 9. A run in a background window keeps running, and lands in ITS tab ── */
  //
  // The failure this rules out is not cosmetic: two windows can be pointed at
  // two different databases, so a result painted into the wrong window is a
  // result attributed to the wrong database.

  await clickTab(n3);
  await page.evaluate(() => window.__cygMonaco.editor.setValue('SELECT SLOW ROWS(7) FROM c'));
  await runNow();
  await page.waitForTimeout(200);
  check('the running window is marked as running in the strip',
    (await tabs()).find((t) => t.name === n3).running, JSON.stringify(await tabs()));

  await clickTab(n1);
  await page.waitForTimeout(150);
  check('switching away from a running query does not disable the other window\'s Run button',
    (await page.evaluate(() => document.getElementById('run-btn').disabled)) === false);
  check('and the other window still shows its own two rows while that one runs',
    (await resultRows()) === 2);

  await page.waitForFunction(() =>
    !document.querySelector('.sqlw-spin'), null, { timeout: 20000 });
  check('when the background query finishes its tab stops spinning',
    !(await tabs()).some((t) => t.running));
  check('and the window on screen was not overwritten by an answer to another window\'s question',
    (await resultRows()) === 2, await resultsText());
  await clickTab(n3);
  await page.waitForTimeout(200);
  check('the seven rows are waiting in the window that asked for them',
    (await resultRows()) === 7, await resultsText());

  /* ── 10. Closing, reopening, and the middle button ──────────────────────── */

  const before10 = await names();
  await page.evaluate((n) => {
    const el = Array.from(document.querySelectorAll('.sqlw-tab'))
      .find((t) => t.querySelector('.sqlw-name').textContent === n);
    el.querySelector('.sqlw-close').click();
  }, n3);
  await page.waitForTimeout(200);
  check('the ✕ closes that window', !(await names()).includes(n3));
  check('and the window to its left takes over, rather than nothing being active',
    !!(await activeName()));

  await page.keyboard.press('Alt+Shift+t');
  await page.waitForTimeout(250);
  check('Alt+Shift+T brings the closed window back', (await names()).includes(n3));
  check('at the position it was closed from',
    (await names()).join('|') === before10.join('|'), (await names()).join('|'));
  check('with its results intact — reopening is not a new window wearing its name',
    (await resultRows()) === 7);

  await page.evaluate((n) => {
    const el = Array.from(document.querySelectorAll('.sqlw-tab'))
      .find((t) => t.querySelector('.sqlw-name').textContent === n);
    el.dispatchEvent(new MouseEvent('auxclick', { button: 1, bubbles: true, cancelable: true }));
  }, n3);
  await page.waitForTimeout(250);
  check('middle-clicking a tab closes it, as it does in a browser',
    !(await names()).includes(n3));

  /* ── 11. Renaming ──────────────────────────────────────────────────────── */

  await clickTab(n2);
  await page.evaluate((n) => {
    const el = Array.from(document.querySelectorAll('.sqlw-tab'))
      .find((t) => t.querySelector('.sqlw-name').textContent === n);
    el.dispatchEvent(new MouseEvent('dblclick', { bubbles: true }));
  }, n2);
  await page.waitForTimeout(150);
  check('double-clicking a tab opens an input on the tab itself',
    await page.evaluate(() => !!document.querySelector('.sqlw-tab input')));
  await page.keyboard.press('Control+a');
  await page.keyboard.type('Row counts');
  await page.keyboard.press('Enter');
  await page.waitForTimeout(200);
  check('typing a name and pressing Enter renames it',
    (await names()).includes('Row counts'), (await names()).join('|'));

  /* ── 12. Keyboard ──────────────────────────────────────────────────────── */

  await page.keyboard.press('Alt+t');
  await page.waitForTimeout(200);
  check('Alt+T opens a window', (await tabs()).length === 3);
  await page.keyboard.press('Alt+1');
  await page.waitForTimeout(200);
  check('Alt+1 goes to the first window', (await activeName()) === (await names())[0]);
  await page.keyboard.press('Alt+9');
  await page.waitForTimeout(200);
  const all12 = await names();
  check('Alt+9 goes to the last one, however many there are',
    (await activeName()) === all12[all12.length - 1]);
  await page.keyboard.press('Control+Alt+ArrowLeft');
  await page.waitForTimeout(200);
  check('Ctrl+Alt+Left cycles backwards', (await activeName()) === all12[all12.length - 2]);
  await page.keyboard.press('Control+Alt+ArrowRight');
  await page.waitForTimeout(200);
  check('and Ctrl+Alt+Right forwards again', (await activeName()) === all12[all12.length - 1]);
  await page.keyboard.press('Alt+w');
  await page.waitForTimeout(250);
  check('Alt+W closes the one in front of you', (await tabs()).length === 2,
    (await names()).join('|'));

  /* ── 13. Drag to reorder ───────────────────────────────────────────────── */

  const order13 = await names();
  await page.evaluate(() => {
    const els = Array.from(document.querySelectorAll('.sqlw-tab'));
    const from = els[0], to = els[els.length - 1];
    const dt = new DataTransfer();
    from.dispatchEvent(new DragEvent('dragstart', { bubbles: true, dataTransfer: dt }));
    const r = to.getBoundingClientRect();
    const at = { bubbles: true, cancelable: true, dataTransfer: dt,
                 clientX: Math.round(r.right - 2), clientY: Math.round(r.top + r.height / 2) };
    to.dispatchEvent(new DragEvent('dragover', at));
    to.dispatchEvent(new DragEvent('drop', at));
    from.dispatchEvent(new DragEvent('dragend', { bubbles: true, dataTransfer: dt }));
  });
  await page.waitForTimeout(250);
  check('dragging a tab past another reorders the strip',
    (await names()).join('|') === order13.slice(1).concat(order13[0]).join('|'),
    (await names()).join('|') + '  was  ' + order13.join('|'));

  /* ── 14. The window menu ───────────────────────────────────────────────── */

  await page.click('#sqlw-menu-btn');
  await page.waitForTimeout(150);
  check('the Windows button opens a menu', await page.evaluate(() => !!document.querySelector('.sqlw-menu')));
  check('listing every open window',
    (await page.evaluate(() => document.querySelectorAll('.sqlw-menu-row').length)) === (await tabs()).length);
  check('with the active one marked for a screen reader, not just in colour',
    await page.evaluate(() => !!document.querySelector('.sqlw-menu-row[aria-checked="true"]')));
  await page.keyboard.press('Escape');
  await page.waitForTimeout(150);
  check('Escape closes it', await page.evaluate(() => !document.querySelector('.sqlw-menu')));

  await page.click('#sqlw-menu-btn');
  await page.waitForTimeout(150);
  await page.click('.sqlw-menu-action[data-act="new"]');
  await page.waitForTimeout(250);
  check('New window from the menu opens one', (await tabs()).length === 3, (await names()).join('|'));

  await page.click('#sqlw-menu-btn');
  await page.waitForTimeout(150);
  await page.click('.sqlw-menu-action[data-act="others"]');
  await page.waitForTimeout(250);
  check('Close others leaves exactly the one you were on', (await tabs()).length === 1);

  /* ── 15. Overflow ──────────────────────────────────────────────────────── */

  for (let i = 0; i < 11; i++) { await page.keyboard.press('Alt+t'); await page.waitForTimeout(60); }
  await page.waitForTimeout(300);
  check('a dozen windows open', (await tabs()).length === 12, (await tabs()).length);
  // On a wide screen a dozen short names still fit; overflow is a property of
  // the strip's width, so narrow it the way a smaller laptop would.
  await page.setViewportSize({ width: 720, height: 900 });
  await page.waitForTimeout(400);
  const t15 = await tabs();
  check('the ones that do not fit are hidden rather than pushed off the edge',
    t15.some((t) => t.hidden), JSON.stringify(t15.map((t) => t.hidden)));
  check('and the Windows button says how many are out of sight',
    /^\d+$/.test(await page.evaluate(() =>
      document.getElementById('sqlw-overflow-count').textContent)),
    await page.evaluate(() => document.getElementById('sqlw-overflow-count').textContent));
  check('the active window is never one of the hidden ones',
    !t15.some((t) => t.active && t.hidden));
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.waitForTimeout(300);
  check('widening the window brings them back rather than leaving them hidden',
    !(await tabs()).some((t) => t.hidden));

  await page.click('#sqlw-menu-btn');
  await page.waitForTimeout(150);
  await page.click('.sqlw-menu-action[data-act="others"]');
  await page.waitForTimeout(250);

  /* ── 16. Persistence ───────────────────────────────────────────────────── */

  await page.evaluate(() => window.__cygMonaco.editor.setValue('SELECT ROWS(3) FROM survives'));
  await page.waitForTimeout(150);
  await runNow();
  await page.waitForFunction(() =>
    document.querySelectorAll('#results-container table tbody tr').length === 3, null, { timeout: 15000 });
  await page.keyboard.press('Alt+t');
  await page.waitForTimeout(200);
  await typeInEditor('SELECT second window');
  await page.evaluate(() => {
    const el = document.querySelectorAll('.sqlw-tab')[1];
    el.dispatchEvent(new MouseEvent('dblclick', { bubbles: true }));
  });
  await page.waitForTimeout(150);
  await page.keyboard.press('Control+a');
  await page.keyboard.type('Second');
  await page.keyboard.press('Enter');
  await page.waitForTimeout(250);

  const beforeReload = await names();
  const raw = await stored();
  check('what is written to storage carries the text and the names',
    raw && /survives/.test(raw.raw) && /Second/.test(raw.raw));
  check('and carries no rows — a result is a photograph, not a fact',
    raw && raw.raw.indexOf('"note"') === -1 && raw.raw.indexOf('results') === -1, raw && raw.raw.slice(0, 200));
  check('and no connection string, only which side was chosen',
    raw && raw.raw.indexOf('fake-db') === -1 && /"conn":"(source|target)"/.test(raw.raw));

  await open();
  check('after a reload the windows are still there', (await names()).join('|') === beforeReload.join('|'),
    (await names()).join('|'));
  check('with the window that was in front still in front',
    (await activeName()) === 'Second', await activeName());
  check('and its text', (await editorText()) === 'SELECT second window', await editorText());
  await clickTab(beforeReload[0]);
  check('the other window\'s text came back too', /survives/.test(await editorText()), await editorText());
  check('but its results did not — a reload cannot vouch for yesterday\'s rows',
    /Run a query/.test(await resultsText()), await resultsText());

  /* ── 17. Saving clears the dirty mark ──────────────────────────────────── */

  await typeInEditor(' -- one more line');
  check('an edit since the last save shows the unsaved dot',
    (await tabs()).find((t) => t.active).dirty);
  await page.fill('#script-name', 'Saved thing');
  await page.evaluate(() => saveScript());
  await page.waitForTimeout(250);
  check('saving clears it', !(await tabs()).find((t) => t.active).dirty, JSON.stringify(await tabs()));
  check('and the tab takes the script\'s name',
    (await activeName()) === 'Saved thing', await activeName());
  await clickTab('Second');
  await clickTab('Saved thing');
  await page.waitForTimeout(200);
  check('the saved-script association follows the window, so Save still means that script',
    await page.evaluate(() => currentScript !== null && editorCtx.type === 'script'),
    await page.evaluate(() => JSON.stringify(editorCtx)));
  check('while the draft beside it is still a draft',
    await (async () => { await clickTab('Second'); return page.evaluate(() => editorCtx.type === 'draft'); })());

  /* ── 18. Nothing threw along the way ───────────────────────────────────── */

  const real = errors.filter((e) => !/ResizeObserver|Failed to fetch|NetworkError/.test(e));
  check('no page errors while doing all of that', real.length === 0, real.join(' | '));

  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  await browser.close();
  server.close();
  process.exit(fail ? 1 : 0);
})().catch(async (e) => {
  console.error('\nSMOKE CRASHED: ' + (e && e.stack || e));
  try { server.close(); } catch (_) {}
  process.exit(1);
});
