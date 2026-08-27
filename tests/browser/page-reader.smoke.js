/* tests/browser/page-reader.smoke.js
 * ---------------------------------------------------------------------------
 * The DOM half of cygenix-page-reader.js, against a real browser.
 *
 * tests/page-reader.test.js pins the rules — what an id is made of, what
 * confirms, what is refused — against pure functions and against the source.
 * This proves they hold against an actual document: that a fixed toolbar is
 * not mistaken for a hidden control, that a recycled list row is caught, that
 * a password is written under confirmation and still never read back.
 *
 * Not part of `npm test`: it needs a browser, and the deploy build has none.
 * Run it by hand:  node tests/browser/page-reader.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8396);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

// charset matters: without it the browser reads UTF-8 source as latin-1 and
// every em dash in a label arrives as mojibake. Netlify sets it; this must too.
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.json': 'application/json', '.svg': 'image/svg+xml' };

const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/') p = '/index.html';
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';
  if (!fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); res.end('nope'); return; }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};

const FIXTURE = `<!doctype html><html><head><title>Fixture screen</title></head><body>
  <h1>Connections</h1>
  <section>
    <h2>Source</h2>
    <label for="srv">Server name</label><input id="srv" name="server" value="prod01.database.windows.net">
    <label for="pw">Password</label><input id="pw" type="password" value="hunter2">
    <input name="fnKey" placeholder="Function key" value="abcdef1234567890">
    <textarea name="notes" placeholder="Notes">Server=prod01;User Id=sa;Password=SuperSecret1;</textarea>
    <select name="mode"><option value="d" selected>Direct</option><option>Relay</option></select>
    <button>Test connection</button>
    <button disabled>Save</button>
  </section>
  <section>
    <h2>Target</h2>
    <button>Test connection</button>
    <a href="/schema-explorer">Open Schema Explorer</a>
    <a href="https://example.com/docs">Read the docs</a>
    <div role="button" aria-label="Remove target">x</div>
    <button>Search tables</button>
    <button>Delete mapping</button>
  </section>
  <div style="display:none"><button>Invisible action</button></div>
  <div hidden><button>Hidden action</button></div>
  <input type="hidden" name="csrf" value="tok">
  <div id="cygAssistant"><button>Send</button></div>
  <div style="position:fixed;top:0"><button>Fixed toolbar action</button></div>
</body></html>`;

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const page = await browser.newPage();
  page.on('console', (m) => { if (m.type() === 'error') console.log('    [console] ' + m.text()); });

  // The console pages are gated; seed a signed-in browser, and refuse every
  // off-origin request so a CDN that cannot be reached here does not hang.
  await page.route('**/*', (route) => {
    route.request().url().startsWith('http://localhost:' + PORT) ? route.continue() : route.abort();
  });
  await page.addInitScript(() => {
    if (sessionStorage.getItem('cygenix_token')) return;
    const exp = String(Date.now() + 3600e3);
    for (const s of [localStorage, sessionStorage]) {
      s.setItem('cygenix_token', 'smoke-token');
      s.setItem('cygenix_expires', exp);
    }
    localStorage.setItem('cygenix_onboarded', '1');
    localStorage.setItem('cygenix_user', JSON.stringify({ email: 'smoke@example.test', name: 'Smoke' }));
    localStorage.setItem('cygenix_tier', 'pro');
    localStorage.setItem('cygenix_cookie_consent', 'all');
    localStorage.setItem('acct-cygenix.ciamlogin.com-smoke', JSON.stringify({
      homeAccountId: 'smoke', environment: 'cygenix.ciamlogin.com', authorityType: 'MSSTS',
      username: 'smoke@example.test', localAccountId: 'smoke', tenantId: 'smoke',
    }));
  });

  await page.goto('http://localhost:' + PORT + '/sql-editor', { waitUntil: 'domcontentloaded' });
  await page.setContent(FIXTURE);
  await page.addScriptTag({ path: PUB + '/cygenix-page-reader.js' });

  const snap = await page.evaluate(() => window.CygenixPageReader.readPage());
  console.log(JSON.stringify(snap, null, 2).slice(0, 3000));
  const labels = snap.interactive_elements.map((e) => e.label);
  const blob = JSON.stringify(snap);

  check('the page title comes through', snap.page_title === 'Fixture screen', snap.page_title);
  check('headings become landmarks',
    snap.landmarks.map((l) => l.heading).join('|') === 'Connections|Source|Target',
    snap.landmarks.map((l) => l.heading).join('|'));
  check('every landmark carries an id', snap.landmarks.every((l) => /^el_[0-9a-f]{8}$/.test(l.element_id)));
  check('every element carries an id', snap.interactive_elements.every((e) => /^el_[0-9a-f]{8}(_\d+)?$/.test(e.id)));
  check('ids are unique', new Set(snap.interactive_elements.map((e) => e.id)).size === snap.interactive_elements.length);

  check('a field is named by its <label>', labels.includes('Server name'), labels.join(' | '));
  check('a field with no label falls back to its placeholder', labels.includes('Function key'));
  check('an aria-label names a role=button', labels.includes('Remove target'));
  check('a link is a link', snap.interactive_elements.some((e) => e.kind === 'link' && e.label === 'Open Schema Explorer'));
  check('a select is a select and reports its selection',
    snap.interactive_elements.some((e) => e.kind === 'select' && e.value === 'Direct'));

  check('display:none is skipped', !labels.includes('Invisible action'));
  check('[hidden] is skipped', !labels.includes('Hidden action'));
  check('input[type=hidden] is skipped', !labels.includes('csrf'));
  check('the assistant panel is skipped', !labels.includes('Send'));
  check('a fixed-position control IS listed', labels.includes('Fixed toolbar action'));
  check('a disabled button is listed and flagged',
    snap.interactive_elements.some((e) => e.label === 'Save' && e.disabled === true));

  check('the password value never appears', blob.indexOf('hunter2') === -1);
  check('the function key value never appears', blob.indexOf('abcdef1234567890') === -1);
  check('a connection string in a notes box never appears', blob.indexOf('SuperSecret1') === -1);
  check('an ordinary value does appear', blob.indexOf('prod01.database.windows.net') !== -1);

  const twins = snap.interactive_elements.filter((e) => e.label === 'Test connection');
  check('two identical buttons under different headings get different ids',
    twins.length === 2 && twins[0].id !== twins[1].id, JSON.stringify(twins));
  check('each is attributed to its own section',
    twins[0].section === 'Source' && twins[1].section === 'Target', JSON.stringify(twins.map((t) => t.section)));

  // Stability: scroll, resize, re-render the DOM around the controls, read again.
  await page.setViewportSize({ width: 640, height: 480 });
  await page.evaluate(() => { window.scrollTo(0, 200); document.body.appendChild(document.createElement('div')); });
  const again = await page.evaluate(() => window.CygenixPageReader.readPage());
  check('the ids survive a scroll, a resize and a DOM change',
    JSON.stringify(again.interactive_elements.map((e) => e.id)) ===
    JSON.stringify(snap.interactive_elements.map((e) => e.id)),
    JSON.stringify(again.interactive_elements.map((e) => e.id)));

  // ── click ────────────────────────────────────────────────────────────────
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.setContent(FIXTURE + `<script>
    window.__pressed = [];
    document.querySelectorAll('button, a, [role=button]').forEach(function (b) {
      b.addEventListener('click', function (e) { e.preventDefault(); window.__pressed.push(b.textContent.trim() || b.getAttribute('aria-label')); });
    });
  </script>`);
  await page.addScriptTag({ path: PUB + '/cygenix-page-reader.js' });

  const idOf = async (label) => page.evaluate((l) => {
    const s = window.CygenixPageReader.readPage();
    const e = s.interactive_elements.find((x) => x.label === l);
    return e ? e.id : null;
  }, label);

  const decide = async (label) => {
    const id = await idOf(label);
    return page.evaluate((i) => window.CygenixPageReader.clickConfirmation(i), id);
  };
  const press = async (label) => {
    const id = await idOf(label);
    return page.evaluate((i) => {
      try { return { ok: true, out: window.CygenixPageReader.click(i) }; }
      catch (e) { return { ok: false, error: e.message }; }
    }, id);
  };

  check('a search button read just now presses without asking',
    (await decide('Search tables')).confirm === false, JSON.stringify(await decide('Search tables')));
  check('an ordinary button asks', (await decide('Test connection')).confirm === true);
  const del = await decide('Delete mapping');
  check('an irreversible label always asks, and says which word', del.confirm === true && /"delete"/.test(del.reason), JSON.stringify(del));
  check('a link within the app presses without asking',
    (await decide('Open Schema Explorer')).confirm === false);
  check('a link leaving the app asks', (await decide('Read the docs')).confirm === true);

  const hit = await press('Test connection');
  check('a click actually fires the page\'s own handler', hit.ok === true
    && (await page.evaluate(() => window.__pressed)).includes('Test connection'), JSON.stringify(hit));
  check('and reports back that the screen may have moved', /read_page again/.test(hit.out.note || ''));

  const link = await press('Open Schema Explorer');
  check('an in-app link is handed back as a navigation rather than followed',
    link.ok && link.out.__navigate === '/schema-explorer', JSON.stringify(link));

  const dis = await press('Save');
  check('a disabled control is refused', !dis.ok && /disabled/.test(dis.error), dis.error);

  const bogus = await page.evaluate(() => {
    try { window.CygenixPageReader.click('el_00000000'); return { ok: true }; }
    catch (e) { return { ok: false, error: e.message }; }
  });
  check('an id that was never read is refused', !bogus.ok && /was not in the last read_page/.test(bogus.error), bogus.error);

  // A recycled row: same node, different meaning.
  const recycled = await page.evaluate(() => {
    const s = window.CygenixPageReader.readPage();
    const e = s.interactive_elements.find((x) => x.label === 'Delete mapping');
    document.querySelectorAll('button').forEach((b) => {
      if (b.textContent.trim() === 'Delete mapping') b.textContent = 'Delete everything';
    });
    try { window.CygenixPageReader.click(e.id); return { ok: true }; }
    catch (err) { return { ok: false, error: err.message }; }
  });
  check('a node whose label changed under it counts as gone',
    !recycled.ok && /no longer exists; call read_page again/.test(recycled.error), recycled.error);

  const removed = await page.evaluate(() => {
    const s = window.CygenixPageReader.readPage();
    const e = s.interactive_elements.find((x) => x.label === 'Test connection');
    document.querySelectorAll('button').forEach((b) => { if (b.textContent.trim() === 'Test connection') b.remove(); });
    try { window.CygenixPageReader.click(e.id); return { ok: true }; }
    catch (err) { return { ok: false, error: err.message }; }
  });
  check('a removed element gets exactly the specified message',
    !removed.ok && /^Element el_[0-9a-f]{8} no longer exists; call read_page again$/.test(removed.error),
    removed.error);

  const staleRead = await page.evaluate(() => {
    const s = window.CygenixPageReader.readPage();
    const e = s.interactive_elements[0];
    window.CygenixPageReader.lastRead().at -= 61000;
    try { window.CygenixPageReader.click(e.id); return { ok: true }; }
    catch (err) { return { ok: false, error: err.message }; }
  });
  check('a read over a minute old is refused outright',
    !staleRead.ok && /seconds old.*Call read_page again/.test(staleRead.error), staleRead.error);

  const staleSkip = await page.evaluate(() => {
    const s = window.CygenixPageReader.readPage();
    const e = s.interactive_elements.find((x) => x.label === 'Search tables');
    window.CygenixPageReader.lastRead().at -= 45000;
    return window.CygenixPageReader.clickConfirmation(e.id);
  });
  check('a safe control read 45 seconds ago asks after all',
    staleSkip.confirm === true && /45 seconds ago/.test(staleSkip.reason), JSON.stringify(staleSkip));

  check('nothing was pressed by any of the refusals',
    (await page.evaluate(() => window.__pressed)).filter((p) => /Delete|Save|Docs/.test(p)).length === 0,
    JSON.stringify(await page.evaluate(() => window.__pressed)));

  // ── type ─────────────────────────────────────────────────────────────────
  const TYPE_FIXTURE = `<!doctype html><html><head><title>Type fixture</title></head><body>
    <h1>Connection</h1>
    <input id="tbl" name="table" placeholder="Table name" value="old value">
    <input id="pw" type="password" name="dbPassword" placeholder="Password">
    <input id="fk" name="fnKey" placeholder="Function key">
    <textarea id="notes" placeholder="Notes">before</textarea>
    <div id="rich" contenteditable="" aria-label="Rich notes">edit me</div>
    <input id="ro" name="readonly_field" placeholder="Computed" value="x" readonly>
    <input id="chk" type="checkbox" name="enabled">
    <input id="file" type="file" name="upload">
    <select id="mode" name="mode"><option>Direct</option></select>
    <button id="go">Save</button>
    <script>
      window.__events = [];
      ['input','change'].forEach(function (n) {
        document.addEventListener(n, function (e) {
          window.__events.push(n + ':' + (e.target.id || e.target.tagName));
        }, true);
      });
    <\/script>
  </body></html>`;
  await page.setContent(TYPE_FIXTURE);
  await page.addScriptTag({ path: PUB + '/cygenix-page-reader.js' });

  const tId = async (label) => page.evaluate((l) => {
    const s = window.CygenixPageReader.readPage();
    const e = s.interactive_elements.find((x) => x.label === l);
    return e ? e.id : null;
  }, label);
  const typeInto = async (label, text) => {
    const id = await tId(label);
    return page.evaluate(([i, t]) => {
      try { return { ok: true, out: window.CygenixPageReader.type(i, t) }; }
      catch (e) { return { ok: false, error: e.message }; }
    }, [id, text]);
  };
  const typeDecide = async (label) => {
    const id = await tId(label);
    return page.evaluate((i) => window.CygenixPageReader.typeConfirmation(i), id);
  };

  const t1 = await typeInto('Table name', 'dbo.customers');
  check('typing replaces what was in the field', t1.ok
    && (await page.evaluate(() => document.getElementById('tbl').value)) === 'dbo.customers',
    JSON.stringify(t1));
  check('and fires input and change so a framework listener sees it',
    (await page.evaluate(() => window.__events)).join(',').includes('input:tbl')
    && (await page.evaluate(() => window.__events)).join(',').includes('change:tbl'));
  check('the result reports a length, never the text',
    t1.out.characters === 13 && JSON.stringify(t1.out).indexOf('dbo.customers') === -1,
    JSON.stringify(t1.out));
  check('and says plainly that nothing has been saved', /Nothing has been saved or submitted/.test(t1.out.note));

  const t2 = await typeInto('Notes', 'after');
  check('a text area works the same way', t2.ok
    && (await page.evaluate(() => document.getElementById('notes').value)) === 'after');

  const t3 = await typeInto('Rich notes', 'replaced');
  check('an editable region works too — including contenteditable="" with no value',
    t3.ok && (await page.evaluate(() => document.getElementById('rich').textContent)) === 'replaced',
    JSON.stringify(t3));

  check('an ordinary field does not ask', (await typeDecide('Table name')).confirm === false);
  const pw = await typeDecide('Password');
  check('a password field asks first', pw.confirm === true && /credential/.test(pw.reason), JSON.stringify(pw));
  const fk = await typeDecide('Function key');
  check('so does a field named for a key, whatever its input type', fk.confirm === true, JSON.stringify(fk));

  const pwWrite = await typeInto('Password', 'set-by-assistant');
  check('the confirmation is advisory to the runtime — the reader itself still types',
    pwWrite.ok && (await page.evaluate(() => document.getElementById('pw').value)) === 'set-by-assistant',
    'the panel is what holds the action until the user approves it');
  check('and even then the value is not echoed back',
    JSON.stringify(pwWrite.out).indexOf('set-by-assistant') === -1);
  check('a password field is still never READ back',
    (await page.evaluate(() => JSON.stringify(window.CygenixPageReader.readPage())))
      .indexOf('set-by-assistant') === -1);

  const ro = await typeInto('Computed', 'nope');
  check('a read-only field is refused', !ro.ok && /read-only or disabled/.test(ro.error), ro.error);
  for (const [label, what] of [['enabled', 'a checkbox'], ['upload', 'a file picker'],
                               ['mode', 'a dropdown'], ['Save', 'a button']]) {
    const r = await typeInto(label, 'x');
    check(what + ' is refused and pointed at click',
      !r.ok && /holds no text|use click/.test(r.error || ''), what + ': ' + (r.error || 'TYPED'));
  }
  const big = await typeInto('Table name', 'x'.repeat(10001));
  check('an absurdly long value is refused rather than pasted',
    !big.ok && /character limit/.test(big.error), big.error);
  check('the file picker was not touched by any of that',
    (await page.evaluate(() => document.getElementById('file').value)) === '');

  // ── wait_for_change ──────────────────────────────────────────────────────
  const WAIT_FIXTURE = `<!doctype html><html><head><title>Wait fixture</title></head><body>
    <h1>Jobs</h1>
    <button id="run">Run</button>
    <button id="doomed">Cancel run</button>
    <div id="slot"></div>
  </body></html>`;

  const waitRun = (script, desc, ms) => page.evaluate(([s, d, t]) => {
    const p = window.CygenixPageReader.waitForChange(d, t);
    // eslint-disable-next-line no-new-func
    setTimeout(new Function(s), 300);
    return p;
  }, [script, desc, ms]);

  const reset = async () => {
    await page.setContent(WAIT_FIXTURE);
    await page.addScriptTag({ path: PUB + '/cygenix-page-reader.js' });
  };

  await reset();
  const w1 = await waitRun("history.pushState({}, '', '/jobs/42')", 'anything', 4000);
  check('a pushState with no DOM change at all is still detected',
    w1.changed && /address changed to \/jobs\/42/.test(w1.what), JSON.stringify(w1));

  await reset();
  const w2 = await waitRun("document.getElementById('doomed').remove()", 'anything', 4000);
  check('a control that disappears is detected, and named',
    w2.changed && /"Cancel run" is no longer on the screen/.test(w2.what), JSON.stringify(w2));

  await reset();
  const w3 = await waitRun(
    "document.getElementById('doomed').style.display='none'", 'anything', 4000);
  check('and so is one merely hidden — a removed node is not the only way to vanish',
    w3.changed && /no longer on the screen/.test(w3.what), JSON.stringify(w3));

  await reset();
  const w4 = await waitRun(
    "document.getElementById('slot').innerHTML='<h2>Results</h2>'", 'anything', 4000);
  check('a new heading is detected as a new section',
    w4.changed && /new section appeared: "Results"/.test(w4.what), JSON.stringify(w4));

  await reset();
  const w5 = await waitRun(
    "document.getElementById('slot').innerHTML='<p>Connection succeeded</p>'",
    'the connection test result', 4000);
  check('text loosely matching the description is detected, and says which word',
    w5.changed && /text mentioning "connection" appeared/.test(w5.what), JSON.stringify(w5));

  await reset();
  const w6 = await waitRun(
    "document.getElementById('slot').innerHTML='<p>Loading projects</p>'",
    'the query results appear', 1200);
  check('unrelated text is not mistaken for the thing being waited for',
    w6.changed === false, JSON.stringify(w6));

  await reset();
  const t0 = Date.now();
  const w7 = await page.evaluate(() => window.CygenixPageReader.waitForChange('nothing at all', 900));
  const took = Date.now() - t0;
  check('a page that does not change times out rather than throwing',
    w7.changed === false && w7.what === null, JSON.stringify(w7));
  check('and says so as a fact, telling the model not to wait again',
    /That is a result, not a failure/.test(w7.note) && /rather than waiting again/.test(w7.note));
  check('the timeout is actually honoured', took >= 850 && took < 2500, took + 'ms');
  check('the wait reports how long it waited', w7.waited_ms >= 850, w7.waited_ms);

  await reset();
  const w8 = await page.evaluate(() => window.CygenixPageReader.waitForChange('x', 1));
  check('a timeout below the floor is raised to it, not honoured as given',
    w8.changed === false && w8.waited_ms >= 250, JSON.stringify(w8));

  await reset();
  const quiet = await page.evaluate(() => window.CygenixPageReader.waitForChange('x', 1500));
  check('an untouched page reports no change — nothing is mistaken for movement',
    quiet.changed === false && quiet.what === null, JSON.stringify(quiet));

  await reset();
  const leaks = await page.evaluate(async () => {
    const before = document.body.innerHTML;
    await window.CygenixPageReader.waitForChange('nothing', 400);
    return before === document.body.innerHTML;
  });
  check('waiting leaves the page exactly as it found it', leaks === true);

  // ── choose ───────────────────────────────────────────────────────────────
  const CHOOSE_FIXTURE = `<!doctype html><html><head><title>Choose fixture</title></head><body>
    <h1>Connection</h1>
    <label for="env">Environment</label>
    <select id="env" name="env">
      <option value="dev">Development</option>
      <option value="prod">Production</option>
      <option value="prod-eu">Production (EU)</option>
    </select>
    <label for="act">Bulk action</label>
    <select id="act" name="act"><option>Export</option><option>Delete selected</option></select>
    <label for="many">Table</label>
    <select id="many" name="many"></select>
    <label for="multi">Columns</label>
    <select id="multi" name="multi" multiple><option>id</option><option>name</option><option>email</option></select>
    <select id="empty" name="empty"></select>
    <input id="txt" name="table" placeholder="Table name">
    <script>
      var many = document.getElementById('many');
      for (var i = 0; i < 60; i++) many.add(new Option('dbo.customer_order_line_' + i, 't' + i));
      window.__changed = [];
      document.addEventListener('change', function (e) { window.__changed.push(e.target.id); }, true);
    <\/script>
  </body></html>`;
  await page.setContent(CHOOSE_FIXTURE);
  await page.addScriptTag({ path: PUB + '/cygenix-page-reader.js' });

  const cId = async (label) => page.evaluate((l) => {
    const s = window.CygenixPageReader.readPage();
    const e = s.interactive_elements.find((x) => x.label === l);
    return e ? e.id : null;
  }, label);
  const pick = async (label, option) => {
    const id = await cId(label);
    return page.evaluate(([i, o]) => {
      try { return { ok: true, out: window.CygenixPageReader.choose(i, o) }; }
      catch (e) { return { ok: false, error: e.message }; }
    }, [id, option]);
  };

  const snapC = await page.evaluate(() => window.CygenixPageReader.readPage());
  const envEntry = snapC.interactive_elements.find((e) => e.label === 'Environment');
  check('read_page lists a dropdown\'s options',
    envEntry && envEntry.options.join(',') === 'Development,Production,Production (EU)',
    JSON.stringify(envEntry));
  const manyEntry = snapC.interactive_elements.find((e) => e.label === 'Table');
  check('a dropdown too big to list reports its size instead of going silent',
    manyEntry && manyEntry.option_count === 60 && !manyEntry.options, JSON.stringify(manyEntry));
  check('and the whole snapshot still fits the cap with options in it',
    JSON.stringify(snapC).length <= 6000, JSON.stringify(snapC).length);

  const c1 = await pick('Environment', 'Production');
  check('an exact option is chosen, even when a longer one contains it',
    c1.ok && (await page.evaluate(() => document.getElementById('env').value)) === 'prod',
    JSON.stringify(c1));
  check('and change fires so the page reacts',
    (await page.evaluate(() => window.__changed)).includes('env'));
  check('the result names the dropdown and the option',
    c1.out.dropdown === 'Environment' && c1.out.chose === 'Production');

  const c2 = await pick('Environment', 'prod-eu');
  check('the underlying value works as well as the text',
    c2.ok && (await page.evaluate(() => document.getElementById('env').selectedIndex)) === 2);

  const c3 = await pick('Environment', 'Produc');
  check('a partial matching two options is refused rather than guessed at',
    !c3.ok && /matches more than one option/.test(c3.error), c3.error);
  const c4 = await pick('Environment', 'Staging');
  check('an option that does not exist comes back with the ones that do',
    !c4.ok && /The options are: Development, Production, Production \(EU\)/.test(c4.error), c4.error);
  const c5 = await pick('Table', 'dbo.customer_order_line_57');
  check('a dropdown too big to list is still choosable by name',
    c5.ok && (await page.evaluate(() => document.getElementById('many').value)) === 't57',
    JSON.stringify(c5));
  const c6 = await pick('empty', 'anything');
  check('a dropdown with no options yet says to wait rather than blaming the model',
    !c6.ok && /has no options yet/.test(c6.error), c6.error);
  const c7 = await pick('Table name', 'x');
  check('choose refuses a text field and points at type',
    !c7.ok && /not a dropdown/.test(c7.error), c7.error);

  const c8 = await page.evaluate(async () => {
    const s = window.CygenixPageReader.readPage();
    const e = s.interactive_elements.find((x) => x.label === 'Bulk action');
    return window.CygenixPageReader.chooseConfirmation(e.id, 'Delete selected');
  });
  check('choosing a destructive option asks first', c8.confirm === true && /"delete"/.test(c8.reason),
    JSON.stringify(c8));
  const c9 = await page.evaluate(async () => {
    const s = window.CygenixPageReader.readPage();
    const e = s.interactive_elements.find((x) => x.label === 'Bulk action');
    return window.CygenixPageReader.chooseConfirmation(e.id, 'Export');
  });
  check('choosing an ordinary one does not', c9.confirm === false, JSON.stringify(c9));

  await page.evaluate(() => {
    const m = document.getElementById('multi');
    m.options[0].selected = true;
  });
  const c10 = await pick('Columns', 'email');
  check('a multi-select is added to, not replaced',
    c10.ok && (await page.evaluate(() => Array.from(document.getElementById('multi').selectedOptions)
      .map((o) => o.text).join(','))) === 'id,email', JSON.stringify(c10));

  // ── scroll ───────────────────────────────────────────────────────────────
  const SCROLL_FIXTURE = `<!doctype html><html><head><title>Scroll fixture</title>
    <style>body{margin:0}.tall{height:4000px}</style></head><body>
    <h1>Top</h1><div class="tall"></div><h2>Bottom</h2></body></html>`;
  await page.setContent(SCROLL_FIXTURE);
  await page.addScriptTag({ path: PUB + '/cygenix-page-reader.js' });
  const doScroll = (d) => page.evaluate((dir) => window.CygenixPageReader.scroll(dir), d);

  const s1 = await doScroll('down');
  check('scrolling down moves the window', s1.scrolled === true && s1.at_top === false,
    JSON.stringify(s1));
  check('and says the viewport moved but the page did not change',
    /nothing on the page was changed/.test(s1.note));
  const s2 = await doScroll('bottom');
  check('bottom goes to the bottom and says so', s2.at_bottom === true, JSON.stringify(s2));
  const s3 = await doScroll('down');
  check('scrolling down at the bottom reports it instead of silently doing nothing',
    s3.scrolled === false && /Already at the bottom/.test(s3.note), JSON.stringify(s3));
  const s4 = await doScroll('top');
  check('top comes back to the top', s4.at_top === true && s4.scrolled === true, JSON.stringify(s4));
  const s5 = await page.evaluate(() => {
    try { window.CygenixPageReader.scroll('sideways'); return { ok: true }; }
    catch (e) { return { ok: false, error: e.message }; }
  });
  check('an unknown direction is refused, not treated as down',
    !s5.ok && /is not a direction/.test(s5.error), s5.error);

  // A pane inside a fixed shell — the shape half this console uses.
  await page.setContent(`<!doctype html><html><head><title>Pane</title>
    <style>html,body{height:100%;margin:0;overflow:hidden}
    #pane{height:100%;overflow-y:auto}.tall{height:3000px}</style></head>
    <body><div id="pane"><h1>Head</h1><div class="tall"></div><h2>Foot</h2></div></body></html>`);
  await page.addScriptTag({ path: PUB + '/cygenix-page-reader.js' });
  const s6 = await doScroll('down');
  check('an inner scrolling pane is found when the window itself cannot scroll',
    s6.scrolled === true && (await page.evaluate(() => document.getElementById('pane').scrollTop)) > 0,
    JSON.stringify(s6));

  await page.setContent('<!doctype html><html><head><title>Short</title></head><body><p>hi</p></body></html>');
  await page.addScriptTag({ path: PUB + '/cygenix-page-reader.js' });
  const s7 = await doScroll('down');
  check('a screen that does not scroll says so calmly',
    s7.scrolled === false && /does not scroll/.test(s7.note), JSON.stringify(s7));

  // A real console page, at size.
  await page.goto('http://localhost:' + PORT + '/sql-editor', { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(600);
  const real = await page.evaluate(() => window.CygenixPageReader.readPage());
  const size = JSON.stringify(real).length;
  check('a real console page fits the cap', size <= 6000, size + ' chars');
  check('and still says something useful',
    real.interactive_elements.length > 5 && !!real.page_title, JSON.stringify(real).slice(0, 300));
  check('the route is the clean address', real.current_route === '/sql-editor', real.current_route);
  console.log('    (' + real.interactive_elements.length + ' elements, ' + size + ' chars, ' +
    (real.truncated || 0) + ' dropped)');
  console.log('    first: ' + real.interactive_elements.slice(0, 6).map((e) => e.kind + ':' + e.label).join(' | '));

  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
