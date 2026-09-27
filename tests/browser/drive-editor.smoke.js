/* tests/browser/drive-editor.smoke.js
 * ---------------------------------------------------------------------------
 * The Drive can open a text file, edit it and save it — and rules.md is one
 * click from the Assistant.
 *
 * WHY THIS EXISTS
 * The Assistant's workspace put a file in the Drive that the user is meant
 * to edit by hand — Claude/<project>/rules.md — and the Drive could only
 * download files. This walks the editor that fixed that, against a real
 * IndexedDB in a real browser, because every promise it makes is about
 * state the browser holds: what is in the store after Save, what is on
 * screen after a refusal, which dialog appears, whether a download fired.
 *
 *   1. a text file's row opens it; its path, text and line count show;
 *   2. typing marks it unsaved; Ctrl+S writes it back in place — same id,
 *      same folder, new content, size and mtime — and asks the sync engine;
 *   3. Esc and closing the Drive ask before discarding unsaved text;
 *   4. a file changed behind the editor's back is not overwritten without
 *      asking; one deleted while open is not saved and the text stays;
 *   5. a non-text file, or a text file over 1 MB, downloads as before;
 *   6. spellcheck is off on the editor, so no browser posts file text away;
 *   7. the Assistant's Rules chip opens rules.md in the editor, and saving it
 *      updates the chip's line count at once.
 *
 * Run it by hand:  node tests/browser/drive-editor.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8435);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const BASE = 'http://localhost:' + PORT;

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.json': 'application/json', '.woff2': 'font/woff2' };
const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/') p = '/index.html';
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end('no'); }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

const RULES = '# Rules for the Assistant\n\n## How to write scripts\n\n## Where things may run\n\n## Things never to do\n';

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const page = await browser.newPage({ viewport: { width: 1400, height: 900 }, acceptDownloads: true });
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  // Dialogs are answered by whatever the current step sets here, and logged.
  let answer = 'dismiss';
  const dialogs = [];
  page.on('dialog', async (d) => { dialogs.push({ type: d.type(), msg: d.message() }); if (answer === 'accept') await d.accept(); else await d.dismiss(); });
  await page.route('**/*', (r) => (r.request().url().startsWith(BASE) ? r.continue() : r.abort()));
  await page.addInitScript(() => {
    const exp = String(Date.now() + 3600e3);
    for (const s of [localStorage, sessionStorage]) { s.setItem('cygenix_token', 'smoke'); s.setItem('cygenix_expires', exp); }
    localStorage.setItem('cygenix_onboarded', '1');
    localStorage.setItem('cygenix_user', JSON.stringify({ email: 'you@example.test', name: 'You' }));
    localStorage.setItem('cygenix_tier', 'pro');
    // cookie-consent.js JSON-parses this; a bare string is "no answer yet" and the banner shows.
    localStorage.setItem('cygenix_cookie_consent', JSON.stringify({ version: '2', essential: true, functional: true, analytics: false, timestamp: new Date().toISOString() }));
    localStorage.setItem('acct-cygenix.ciamlogin.com-x', JSON.stringify({ homeAccountId: 'x', environment: 'cygenix.ciamlogin.com',
      authorityType: 'MSSTS', username: 'you@example.test', localAccountId: 'x', tenantId: 'x' }));
    localStorage.setItem('cygenix_projects', JSON.stringify([{ id: 'p-demo', name: 'Demo' }]));
    localStorage.setItem('cygenix_active_project_id', 'p-demo');
  });

  await page.goto(BASE + '/dashboard', { waitUntil: 'domcontentloaded' });
  await page.waitForFunction(() => !!window.CygenixDriveStore && !!window.CygenixAssistant, null, { timeout: 15000 });
  // The Drive overlay is idle-loaded by the sidebar; make sure it is here.
  await page.evaluate(() => new Promise((res) => {
    if (window.CygenixDriveModal) return res();
    const s = document.getElementById('cygenix-drive-modal-js');
    if (s) s.addEventListener('load', res, { once: true });
    else { const t = document.createElement('script'); t.src = '/cygenix-drive-modal.js'; t.onload = res; document.head.appendChild(t); }
  }));

  // Seed the Drive: the Assistant's workspace, a user folder, a picture and a
  // text file just over the 1 MB editing limit.
  await page.evaluate(async (RULES) => {
    const db = await new Promise((res, rej) => {
      const r = indexedDB.open('cygenix_coworker_drive', 1);
      r.onupgradeneeded = () => { const s = r.result.createObjectStore('nodes', { keyPath: 'id' }); s.createIndex('parentId', 'parentId', { unique: false }); };
      r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error);
    });
    const put = (n) => new Promise((res, rej) => { const tx = db.transaction('nodes', 'readwrite'); tx.objectStore('nodes').put(n); tx.oncomplete = res; tx.onerror = () => rej(tx.error); });
    const t = Date.now() - 60000;
    const file = (id, parentId, name, text, mime) => { const b = new Blob([text], { type: mime || 'text/plain' }); return { id, parentId, name, kind: 'file', size: b.size, mime: mime || 'text/plain', mtime: t, content: b }; };
    await put({ id: 'claude', parentId: '', name: 'Claude', kind: 'folder', mtime: t, meta: { reserved: 'claude' } });
    await put({ id: 'demo', parentId: 'claude', name: 'Demo', kind: 'folder', mtime: t, meta: { claudeProject: 'p-demo' } });
    await put({ id: 'scripts', parentId: 'demo', name: 'scripts', kind: 'folder', mtime: t });
    await put({ id: 'results', parentId: 'demo', name: 'results', kind: 'folder', mtime: t });
    await put(file('rules', 'demo', 'rules.md', RULES));
    await put(file('notes', 'demo', 'notes.md', "# Assistant's notes\n"));
    await put({ id: 'mine', parentId: '', name: 'Mine', kind: 'folder', mtime: t });
    await put(file('q', 'mine', 'query.sql', 'select 1;\n'));
    await put(file('pic', 'mine', 'diagram.png', 'not really a png', 'image/png'));
    await put(file('big', 'mine', 'big.txt', 'x'.repeat(1048577)));
  }, RULES);

  // Count sync requests from here on, without reaching the network.
  await page.evaluate(() => {
    window.__syncs = 0;
    window.CygenixDriveSync = Object.assign({}, window.CygenixDriveSync || {}, {
      sync: () => { window.__syncs++; return Promise.resolve({ skipped: 'smoke' }); },
      status: () => ({ state: 'idle' }), onChange: () => {},
    });
  });
  const readNode = (id) => page.evaluate((id) => new Promise((res) => {
    const r = indexedDB.open('cygenix_coworker_drive', 1);
    r.onsuccess = () => { const g = r.result.transaction('nodes').objectStore('nodes').get(id);
      g.onsuccess = async () => { const n = g.result; if (!n) return res(null); res({ id: n.id, parentId: n.parentId, name: n.name, size: n.size, mtime: n.mtime, text: await n.content.text() }); }; };
  }), id);
  const ui = () => page.evaluate(() => {
    const m = document.querySelector('.cygdm-modal');
    return {
      open: !!document.querySelector('.cygdm-bg.open'),
      editing: !!(m && m.classList.contains('editing')),
      path: (document.getElementById('cygdm-ed-path') || {}).textContent || '',
      text: (document.getElementById('cygdm-ed-text') || {}).value || '',
      state: (document.getElementById('cygdm-ed-state') || {}).textContent || '',
      saveDisabled: !!(document.getElementById('cygdm-ed-save') || {}).disabled,
      info: (document.getElementById('cygdm-ed-info') || {}).textContent || '',
      spell: (document.getElementById('cygdm-ed-text') || { getAttribute() { return null; } }).getAttribute('spellcheck'),
      focused: document.activeElement && document.activeElement.id,
    };
  });
  const row = (name) => page.locator('.cygdm-row', { hasText: name }).first();

  console.log('Drive text editor\n');

  /* ── 1. Opening ─────────────────────────────────────────────────────────── */
  section('1. A text file opens in the editor');
  await page.evaluate(() => window.CygenixDriveModal.open({ folderId: 'demo' }));
  await page.waitForSelector('.cygdm-row', { timeout: 5000 });
  await row('rules.md').click();
  await page.waitForFunction(() => document.querySelector('.cygdm-modal.editing'));
  let u = await ui();
  check('CLICKING rules.md OPENS IT IN THE EDITOR', u.editing);
  check('its path is shown', u.path === 'Claude / Demo / rules.md', u.path);
  check('its text is loaded exactly', u.text === RULES);
  check('nothing to save yet', u.saveDisabled && u.state === 'Saved', u.state);
  check('the line count is shown', u.info === RULES.split('\n').length + ' lines', u.info);
  check('the editor has focus', u.focused === 'cygdm-ed-text', u.focused);
  check('SPELLCHECK IS OFF, so no browser posts the text anywhere', u.spell === 'false', u.spell);

  /* ── 2. Editing and saving ──────────────────────────────────────────────── */
  section('2. Editing and saving');
  const before = await readNode('rules');
  await page.focus('#cygdm-ed-text');
  await page.keyboard.press('Control+End');
  await page.keyboard.type('- Never add comments to SQL scripts\n');
  u = await ui();
  check('typing marks it unsaved and enables Save', u.state === 'Unsaved changes' && !u.saveDisabled, u.state);
  await page.keyboard.press('Control+s');
  await page.waitForFunction(() => document.getElementById('cygdm-ed-state').textContent === 'Saved');
  const after = await readNode('rules');
  check('CTRL+S WRITES IT BACK: the new text is in the store', after.text === RULES + '- Never add comments to SQL scripts\n', JSON.stringify(after.text.slice(-60)));
  check('…in place: same id, same folder, same name', after.id === 'rules' && after.parentId === 'demo' && after.name === 'rules.md');
  check('…with its size and modified time updated', after.size === Buffer.byteLength(after.text) && after.mtime > before.mtime, after.size + ' / ' + after.mtime);
  check('…and the sync engine was asked to push it', await page.evaluate(() => window.__syncs) >= 1);
  u = await ui();
  check('Save is disabled again once saved', u.saveDisabled);

  /* ── 3. Not losing work ─────────────────────────────────────────────────── */
  section('3. Unsaved text is never thrown away without asking');
  await page.keyboard.type('unsaved line');
  dialogs.length = 0; answer = 'dismiss';
  await page.keyboard.press('Escape');
  await page.waitForTimeout(150);
  u = await ui();
  check('ESC WITH UNSAVED TEXT ASKS FIRST', dialogs.length === 1 && dialogs[0].type === 'confirm' && /Discard unsaved changes/.test(dialogs[0].msg), JSON.stringify(dialogs));
  check('…and "no" keeps the editor and the text', u.editing && /unsaved line$/.test(u.text));
  dialogs.length = 0;
  await page.click('#cygdm-close');
  await page.waitForTimeout(150);
  u = await ui();
  check('CLOSING THE DRIVE WITH UNSAVED TEXT ASKS TOO, and "no" keeps it open', dialogs.length === 1 && u.open && u.editing, JSON.stringify(dialogs) + ' ' + JSON.stringify(u.open));
  answer = 'accept'; dialogs.length = 0;
  await page.keyboard.press('Escape');
  await page.waitForTimeout(200);
  u = await ui();
  check('"yes" goes back to the folder', !u.editing && u.open);
  check('…and the unsaved line was not written', !/unsaved line/.test((await readNode('rules')).text));
  dialogs.length = 0;
  await page.keyboard.press('Escape');
  await page.waitForTimeout(150);
  check('Esc on the folder list closes the Drive as before, without asking', !(await ui()).open && dialogs.length === 0);

  /* ── 4. Someone else's save, a deleted file ─────────────────────────────── */
  section('4. A file that changed or vanished while open');
  await page.evaluate(() => window.CygenixDriveModal.open({ fileId: 'notes' }));
  await page.waitForFunction(() => document.querySelector('.cygdm-modal.editing'));
  u = await ui();
  check('open({ fileId }) OPENS THE FILE STRAIGHT IN THE EDITOR', u.path === 'Claude / Demo / notes.md', u.path);
  // Behind the editor's back: another save lands on the same file.
  await page.evaluate(() => new Promise((res) => {
    const r = indexedDB.open('cygenix_coworker_drive', 1);
    r.onsuccess = () => { const tx = r.result.transaction('nodes', 'readwrite'); const s = tx.objectStore('nodes');
      const g = s.get('notes'); g.onsuccess = () => { const n = g.result; n.content = new Blob(['# Assistant\'s notes\nwritten elsewhere\n']); n.size = n.content.size; n.mtime = Date.now(); s.put(n); };
      tx.oncomplete = res; };
  }));
  await page.focus('#cygdm-ed-text');
  await page.keyboard.press('Control+End');
  await page.keyboard.type('my line\n');
  answer = 'dismiss'; dialogs.length = 0;
  await page.click('#cygdm-ed-save');
  await page.waitForTimeout(250);
  check('SAVING OVER A NEWER VERSION ASKS FIRST', dialogs.length === 1 && /changed after you opened it/.test(dialogs[0].msg), JSON.stringify(dialogs));
  check('…and "no" leaves the other version untouched', /written elsewhere/.test((await readNode('notes')).text) && (await ui()).state === 'Unsaved changes');
  answer = 'accept'; dialogs.length = 0;
  await page.click('#cygdm-ed-save');
  await page.waitForFunction(() => document.getElementById('cygdm-ed-state').textContent === 'Saved');
  check('…and "yes" saves this version', /my line/.test((await readNode('notes')).text) && !/written elsewhere/.test((await readNode('notes')).text));

  await page.evaluate(() => new Promise((res) => {
    const r = indexedDB.open('cygenix_coworker_drive', 1);
    r.onsuccess = () => { const tx = r.result.transaction('nodes', 'readwrite'); tx.objectStore('nodes').delete('notes'); tx.oncomplete = res; };
  }));
  await page.focus('#cygdm-ed-text');
  await page.keyboard.press('Control+End');
  await page.keyboard.type('after delete');
  answer = 'accept'; dialogs.length = 0;
  await page.click('#cygdm-ed-save');
  await page.waitForTimeout(250);
  u = await ui();
  check('A FILE DELETED WHILE OPEN IS NOT RECREATED BY SAVE, and says so', dialogs.length === 1 && dialogs[0].type === 'alert' && /deleted or moved/.test(dialogs[0].msg) && !(await readNode('notes')), JSON.stringify(dialogs));
  check('…and the text is still in the editor to copy', u.editing && /after delete$/.test(u.text));
  answer = 'accept';
  await page.click('#cygdm-ed-back');
  await page.waitForTimeout(150);

  /* ── 5. Everything else still downloads ─────────────────────────────────── */
  section('5. Non-text and oversized files download as before');
  await page.evaluate(() => window.CygenixDriveModal.open({ folderId: 'mine' }));
  await page.waitForSelector('.cygdm-row', { timeout: 5000 });
  let dl = page.waitForEvent('download', { timeout: 3000 }).catch(() => null);
  await row('diagram.png').click();
  let d = await dl;
  check('A PICTURE DOWNLOADS rather than opening', !!d && d.suggestedFilename() === 'diagram.png' && !(await ui()).editing, d && d.suggestedFilename());
  dl = page.waitForEvent('download', { timeout: 3000 }).catch(() => null);
  await row('big.txt').click();
  d = await dl;
  check('A TEXT FILE OVER 1 MB DOWNLOADS rather than opening', !!d && d.suggestedFilename() === 'big.txt' && !(await ui()).editing);
  await row('query.sql').click();
  await page.waitForFunction(() => document.querySelector('.cygdm-modal.editing'));
  check('a .sql file opens in the editor', (await ui()).path === 'Mine / query.sql');
  dl = page.waitForEvent('download', { timeout: 3000 }).catch(() => null);
  await page.click('#cygdm-ed-dl');
  d = await dl;
  check('and the editor keeps a Download button', !!d && d.suggestedFilename() === 'query.sql');
  await page.click('#cygdm-ed-back');
  await page.keyboard.press('Escape');

  /* ── 7. The Assistant's Rules chip ─────────────────────────────────────── */
  section('7. The Rules chip opens rules.md, and a save updates it');
  await page.evaluate(() => window.CygenixAssistant.open());
  await page.evaluate(() => window.CygenixAssistant.reloadWorkspace());
  await page.waitForFunction(() => { const c = document.getElementById('cygaRules'); return c && !c.hidden; }, null, { timeout: 5000 });
  const chip0 = await page.evaluate(() => document.getElementById('cygaRules').textContent);
  const lines0 = (await readNode('rules')).text.split('\n').length;
  check('the chip shows the real line count of rules.md', chip0 === 'Rules: rules.md (' + lines0 + ' lines)', chip0);
  await page.click('#cygaRules');
  await page.waitForFunction(() => document.querySelector('.cygdm-bg.open .cygdm-modal.editing'), null, { timeout: 5000 });
  u = await ui();
  check('CLICKING THE CHIP OPENS rules.md IN THE EDITOR', u.path === 'Claude / Demo / rules.md', u.path);
  await page.focus('#cygdm-ed-text');
  await page.keyboard.press('Control+End');
  await page.keyboard.type('- Only run against TEST\n');
  await page.keyboard.press('Control+s');
  await page.waitForFunction(() => document.getElementById('cygdm-ed-state').textContent === 'Saved');
  await page.waitForFunction((n) => document.getElementById('cygaRules').textContent === 'Rules: rules.md (' + n + ' lines)', lines0 + 1, { timeout: 3000 }).catch(() => {});
  const chip1 = await page.evaluate(() => document.getElementById('cygaRules').textContent);
  check('SAVING rules.md UPDATES THE CHIP AT ONCE — the Assistant has re-read it', chip1 === 'Rules: rules.md (' + (lines0 + 1) + ' lines)', chip1);
  const ws = await page.evaluate(() => window.CygenixAssistant.workspace().rules);
  check('…and the next reply will carry the new rule', /Only run against TEST/.test(ws));

  check('nothing threw', errors.length === 0, errors.slice(0, 3).join(' | '));

  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
