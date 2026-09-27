/* tests/browser/menu-search.smoke.js
 * ---------------------------------------------------------------------------
 * Typing "claude" in the masthead finds the Anthropic API key and lands on it.
 *
 * tests/menu-index.test.js holds the ranking, the SETTINGS table and the
 * one-shot rule to account without a DOM. What only a browser can say is
 * whether the whole journey works on the real pages: the dropdown appears
 * under the field and above the rail, the keyboard drives it, Enter opens
 * the setting, the dashboard switches to General settings, and the key field
 * ends up scrolled into view, focused and flashing — from the dashboard AND
 * from another page, which is a full navigation with the target carried
 * across in sessionStorage. Then that it stops: no view re-rendering after
 * landing, no requests while typing, and nothing thrown.
 *
 * This walks the brief's verification list, 1 to 8.
 *
 * Run it by hand:  node tests/browser/menu-search.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8432);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const BASE = 'http://localhost:' + PORT;

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.json': 'application/json',
  '.woff2': 'font/woff2' };
// The two extensionless addresses this walks that do not match their file
// name; the real site gets these from public/_redirects.
const ALIAS = { '/object-mapping': '/object_mapping.html' };

const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/') p = '/index.html';
  if (ALIAS[p]) p = ALIAS[p];
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) {
    res.writeHead(404); return res.end('no');
  }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

// WCAG contrast, for the dropdown in each theme.
const rgba = (s) => { const n = (s.match(/[\d.]+/g) || []).map(Number); return { r: n[0] || 0, g: n[1] || 0, b: n[2] || 0, a: n.length > 3 ? n[3] : 1 }; };
const over = (fg, bg) => ({ r: fg.a * fg.r + (1 - fg.a) * bg.r, g: fg.a * fg.g + (1 - fg.a) * bg.g, b: fg.a * fg.b + (1 - fg.a) * bg.b, a: 1 });
const lum = (c) => { const ch = [c.r, c.g, c.b].map((v) => { const s = v / 255; return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4); }); return 0.2126 * ch[0] + 0.7152 * ch[1] + 0.0722 * ch[2]; };
const ratio = (a, b) => { const [hi, lo] = [lum(a), lum(b)].sort((x, y) => y - x); return (hi + 0.05) / (lo + 0.05); };

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });

  async function newPage(roles) {
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    const errors = [];
    page.on('pageerror', (e) => errors.push(e.message));
    await page.route('**/*', (r) => (r.request().url().startsWith(BASE) ? r.continue() : r.abort()));
    await page.addInitScript((roles) => {
      const exp = String(Date.now() + 3600e3);
      for (const s of [localStorage, sessionStorage]) { s.setItem('cygenix_token', 'smoke'); s.setItem('cygenix_expires', exp); }
      localStorage.setItem('cygenix_onboarded', '1');
      localStorage.setItem('cygenix_user', JSON.stringify({ email: 'you@example.test', name: 'You' }));
      localStorage.setItem('cygenix_tier', 'pro');
      // cookie-consent.js JSON-parses this; a bare string is "no answer yet" and the banner shows.
      localStorage.setItem('cygenix_cookie_consent', JSON.stringify({ version: '2', essential: true, functional: true, analytics: false, timestamp: new Date().toISOString() }));
      localStorage.setItem('acct-cygenix.ciamlogin.com-x', JSON.stringify({
        homeAccountId: 'x', environment: 'cygenix.ciamlogin.com', authorityType: 'MSSTS',
        username: 'you@example.test', localAccountId: 'x', tenantId: 'x' }));
      if (roles) sessionStorage.setItem('cygenix_rbac_me', JSON.stringify({ at: Date.now(), me: { roles } }));
    }, roles || null);
    return { page, errors };
  }
  async function open(page, where) {
    await page.goto(BASE + where, { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('#cx-mh-search', { timeout: 15000 });
    await page.waitForFunction(() => !!window.CygenixMenuIndex && !!(window.CygenixSidebar && window.CygenixSidebar.navEntries));
    await page.waitForTimeout(400);
  }
  async function typeQuery(page, q) {
    await page.fill('#cx-mh-search', '');
    await page.click('#cx-mh-search');
    await page.type('#cx-mh-search', q, { delay: 15 });
    // Wait for the list to answer THIS query. The previous one's list stays
    // up through the 120ms debounce, so "the list is visible" is not enough.
    await page.waitForFunction((q) => {
      const l = document.getElementById('cx-mh-results');
      const last = l && !l.hidden && l.querySelector('.cx-mh-opt-saved');
      return !!last && last.textContent.indexOf('“' + q + '”') !== -1;
    }, q, { timeout: 3000 });
  }
  const readRows = (page) => page.evaluate(() => Array.from(document.querySelectorAll('#cx-mh-results [role=option]')).map((o) => ({
    label: (o.querySelector('.cx-mh-opt-lbl') || {}).textContent || '',
    path: (o.querySelector('.cx-mh-opt-path') || {}).textContent || '',
    tag: (o.querySelector('.cx-mh-opt-tag') || {}).textContent || '',
    selected: o.getAttribute('aria-selected'),
  })));
  // Where focus ended up, and what the page looks like around it.
  const landed = (page) => page.evaluate(() => {
    const a = document.activeElement;
    const view = document.querySelector('.view.active');
    const r = a ? a.getBoundingClientRect() : null;
    let key = null;
    try { key = sessionStorage.getItem('cyg_focus_target'); } catch (e) {}
    return {
      path: location.pathname, view: view ? view.id : '', focused: a ? a.id : '',
      flashing: !!(a && (a.classList.contains('cyg-search-flash') || (a.closest && a.closest('.cyg-search-flash')))),
      inView: !!(r && r.top >= 0 && r.bottom <= window.innerHeight),
      key,
    };
  });

  console.log('Menu & settings search — the masthead and the Search page\n');

  /* ── 1. "claude" on the dashboard ───────────────────────────────────────── */
  section('1. "claude" offers the Anthropic key first and lands on the field');
  {
    const { page, errors } = await newPage();
    await open(page, '/dashboard');
    await typeQuery(page, 'claude');
    const rows = await readRows(page);
    check('THE FIRST ROW IS "Anthropic API key (Claude)"', rows[0] && rows[0].label === 'Anthropic API key (Claude)', JSON.stringify(rows[0]));
    check('…with its breadcrumb and a Setting tag', rows[0] && rows[0].path === 'Settings › General' && rows[0].tag === 'Setting', JSON.stringify(rows[0]));
    check('the last row is always "Search saved work for …"', /^Search saved work for .claude. /.test(rows[rows.length - 1].label), rows[rows.length - 1].label);
    check('at most six matches above it', rows.length <= 7, rows.length);
    const a11y = await page.evaluate(() => {
      const i = document.getElementById('cx-mh-search');
      return { role: i.getAttribute('role'), exp: i.getAttribute('aria-expanded'), ctl: i.getAttribute('aria-controls'),
        listRole: document.getElementById('cx-mh-results').getAttribute('role') };
    });
    check('the field is a combobox that says it is expanded, over a listbox',
      a11y.role === 'combobox' && a11y.exp === 'true' && a11y.ctl === 'cx-mh-results' && a11y.listRole === 'listbox', JSON.stringify(a11y));

    // z-order: the dropdown is on top of whatever it covers, rail included.
    const onTop = await page.evaluate(() => {
      const l = document.getElementById('cx-mh-results').getBoundingClientRect();
      const hit = document.elementFromPoint(l.left + 20, l.top + 12);
      return !!(hit && hit.closest('#cx-mh-results'));
    });
    check('the dropdown is painted above what it covers', onTop);

    await page.keyboard.press('ArrowDown');
    const ad = await page.evaluate(() => document.getElementById('cx-mh-search').getAttribute('aria-activedescendant'));
    check('↓ highlights the first row and names it through aria-activedescendant', ad === 'cx-mh-results-0', ad);
    // Count showView calls from here on: landing must not start a loop.
    await page.evaluate(() => {
      window.__views = 0;
      const orig = window.showView;
      window.showView = function () { window.__views++; return orig.apply(this, arguments); };
    });
    await page.keyboard.press('Enter');
    await page.waitForTimeout(600);
    const l = await landed(page);
    check('ENTER OPENS GENERAL SETTINGS', l.view === 'view-project-settings', l.view);
    check('…WITH THE KEY FIELD FOCUSED', l.focused === 'settings-api-key', l.focused);
    check('…scrolled into view', l.inView);
    check('…and flashing', l.flashing);
    check('the one-shot key is gone', l.key === null, l.key);
    const views0 = await page.evaluate(() => window.__views);
    await page.waitForTimeout(1600);
    const after = await page.evaluate(() => ({ views: window.__views,
      flash: document.querySelectorAll('.cyg-search-flash').length }));
    check('THE PAGE DOES NOT KEEP RE-RENDERING after landing', after.views === views0, views0 + ' → ' + after.views);
    check('the flash is gone after 1.5s', after.flash === 0, after.flash);
    check('the flash outline uses the theme accent', await page.evaluate(() => {
      const el = document.createElement('div'); el.className = 'cyg-search-flash'; document.body.appendChild(el);
      const c = getComputedStyle(el).outlineColor; el.remove();
      const t = document.createElement('div'); t.style.color = 'var(--color-accent)'; document.body.appendChild(t);
      const a = getComputedStyle(t).color; t.remove();
      return c === a;
    }));
    check('nothing threw', errors.length === 0, errors.slice(0, 3).join(' | '));
    await page.close();
  }

  /* ── 2. From another page ──────────────────────────────────────────────── */
  section('2. The same from /object-mapping: a full navigation, then the field');
  {
    const { page, errors } = await newPage();
    await open(page, '/object-mapping');
    await typeQuery(page, 'claude');
    await page.keyboard.press('ArrowDown');
    await Promise.all([page.waitForURL(/\/dashboard/, { timeout: 15000 }), page.keyboard.press('Enter')]);
    await page.waitForFunction(() => document.activeElement && document.activeElement.id === 'settings-api-key', null, { timeout: 5000 }).catch(() => {});
    const l = await landed(page);
    check('it lands on the dashboard', l.path === '/dashboard', l.path);
    check('ON GENERAL SETTINGS WITH THE KEY FIELD FOCUSED', l.view === 'view-project-settings' && l.focused === 'settings-api-key', JSON.stringify(l));
    check('and the key did not outlive the landing', l.key === null);
    check('nothing threw', errors.length === 0, errors.slice(0, 3).join(' | '));
    await page.close();
  }

  /* ── 3. The other queries ──────────────────────────────────────────────── */
  section('3. api key, anthropic, theme, collation, users, object map');
  {
    const { page } = await newPage();
    await open(page, '/dashboard');
    const want = {
      'api key': 'Anthropic API key (Claude)', 'anthropic': 'Anthropic API key (Claude)', 'theme': 'Theme',
      'collation': 'Collation settings', 'users': 'Users & roles', 'object map': 'Object mapping',
    };
    for (const [q, label] of Object.entries(want)) {
      await typeQuery(page, q);
      const rows = await readRows(page);
      check('"' + q + '" → ' + label, rows[0] && rows[0].label === label, rows.slice(0, 3).map((r) => r.label).join(' | '));
    }
    // A Connections field: the right tab, then the card.
    await typeQuery(page, 'collation');
    await page.keyboard.press('ArrowDown');
    await page.keyboard.press('Enter');
    await page.waitForTimeout(500);
    const c = await page.evaluate(() => {
      const card = document.getElementById('cyg-collation-card');
      return {
        view: (document.querySelector('.view.active') || {}).id,
        tab: getComputedStyle(document.getElementById('conn-tab-databases')).display,
        flashing: !!(card && card.classList.contains('cyg-search-flash')),
        focusInCard: !!(card && card.contains(document.activeElement)),
        key: sessionStorage.getItem('cyg_focus_target'),
      };
    });
    check('"collation" opens Connections on the Database connections tab', c.view === 'view-connections' && c.tab !== 'none', JSON.stringify(c));
    // The Collation module rebuilds its card with innerHTML on every visit to
    // that tab. A landing done inside showView flashed the card that was
    // about to be thrown away; this is the check that caught it.
    check('THE COLLATION CARD ITSELF IS FLASHED AND HOLDS FOCUS — the rebuilt card, not the discarded one',
      c.flashing && c.focusInCard, JSON.stringify(c));
    check('…and the key is spent', c.key === null, JSON.stringify(c));
    await page.close();
  }

  /* ── 4. RBAC ───────────────────────────────────────────────────────────── */
  section('4. "audit" respects the rail\'s visibility');
  {
    const { page } = await newPage(null);
    await open(page, '/dashboard');
    await typeQuery(page, 'audit');
    const rows = await readRows(page);
    check('WITHOUT AUDIT ACCESS THERE IS NO AUDIT LOG ROW', !rows.some((r) => r.label === 'Audit log'), rows.map((r) => r.label).join(' | '));
    await page.close();
  }
  {
    const { page } = await newPage(['AU']);
    await open(page, '/dashboard');
    await typeQuery(page, 'audit');
    const rows = await readRows(page);
    check('an Auditor gets it first', rows[0] && rows[0].label === 'Audit log', rows.map((r) => r.label).join(' | '));
    await page.close();
  }

  /* ── 5. Enter with nothing highlighted, and the Search page ─────────────── */
  section('5. Enter with nothing highlighted still opens the Search page, now with the new group');
  {
    const { page, errors } = await newPage();
    await page.addInitScript(() => {
      localStorage.setItem('cygenix_jobs', JSON.stringify([{ id: 'j1', name: 'Claude import job', jobType: 'sql', sql: 'select 1' }]));
    });
    await open(page, '/dashboard');
    await typeQuery(page, 'claude');
    await page.keyboard.press('Enter');
    await page.waitForTimeout(500);
    const s = await page.evaluate(() => ({
      view: (document.querySelector('.view.active') || {}).id,
      q: document.getElementById('search-input').value,
      html: document.getElementById('search-results').innerHTML,
      text: document.getElementById('search-results').textContent,
      list: (document.getElementById('cx-mh-results') || {}).hidden,
    }));
    check('THE SEARCH PAGE OPENS, WITH THE QUERY RUN', s.view === 'view-search' && s.q === 'claude', s.view + ' / ' + s.q);
    check('the dropdown closed on the way', s.list === true);
    check('"Menu & settings" is the first group', s.text.indexOf('Menu & settings') === 0 || s.text.trim().indexOf('Menu & settings') === 0, s.text.slice(0, 80));
    check('…and holds the Anthropic key', /Anthropic API key/.test(s.text));
    check('saved work still follows it', /Saved work/.test(s.text) && /Claude import job/.test(s.text));
    // Open from the Search page lands on the field too.
    await page.click('#search-results button:has-text("Open")');
    await page.waitForTimeout(600);
    const l = await landed(page);
    check('"Open" on a setting lands on the field', l.view === 'view-project-settings' && l.focused === 'settings-api-key', JSON.stringify(l));

    await page.evaluate(() => { showView('search'); document.getElementById('search-input').value = 'claude';
      document.getElementById('search-scope').value = 'menu'; runGlobalSearch(); });
    const only = await page.evaluate(() => document.getElementById('search-results').textContent);
    check('"Menu & settings only" shows that group and no saved work', /Anthropic API key/.test(only) && !/Claude import job/.test(only), only.slice(0, 120));
    await page.evaluate(() => { document.getElementById('search-scope').value = 'job'; runGlobalSearch(); });
    const jobsOnly = await page.evaluate(() => document.getElementById('search-results').textContent);
    check('"Jobs / Maps only" leaves the group out', !/Anthropic API key/.test(jobsOnly) && /Claude import job/.test(jobsOnly));
    await page.evaluate(() => { document.getElementById('search-scope').value = 'all';
      document.getElementById('search-input').value = 'theme'; runGlobalSearch(); });
    const themeOnly = await page.evaluate(() => document.getElementById('search-results').textContent);
    check('a query only settings match is NOT "Nothing matched"', /Theme/.test(themeOnly) && !/Nothing matched/.test(themeOnly), themeOnly.slice(0, 120));
    await page.evaluate(() => { document.getElementById('search-input').value = 'zqxjv'; runGlobalSearch(); });
    const none = await page.evaluate(() => document.getElementById('search-results').textContent);
    check('"Nothing matched" when every group is empty', /Nothing matched/.test(none));
    await page.evaluate(() => { document.getElementById('search-input').value = ''; runGlobalSearch(); });
    const empty = await page.evaluate(() => document.getElementById('search-results').textContent);
    check('the empty state says menu items and settings are searched', /menu items and settings/.test(empty), empty);
    check('nothing threw', errors.length === 0, errors.slice(0, 3).join(' | '));
    await page.close();
  }

  /* ── Keyboard, closing ─────────────────────────────────────────────────── */
  section('Keyboard and closing');
  {
    const { page } = await newPage();
    await open(page, '/dashboard');
    await typeQuery(page, 'theme');
    await page.keyboard.press('ArrowUp');
    let rows = await readRows(page);
    check('↑ from nothing highlights the last row', rows[rows.length - 1].selected === 'true');
    await page.keyboard.press('ArrowDown');
    rows = await readRows(page);
    check('↓ from the last wraps to the first', rows[0].selected === 'true');
    await page.keyboard.press('Escape');
    check('Esc closes it', await page.evaluate(() => document.getElementById('cx-mh-results').hidden));
    check('…and says so', await page.evaluate(() => document.getElementById('cx-mh-search').getAttribute('aria-expanded')) === 'false');
    await typeQuery(page, 'theme');
    await page.mouse.click(700, 500);
    check('a click outside closes it', await page.evaluate(() => document.getElementById('cx-mh-results').hidden));
    await page.close();
  }

  /* ── 6. Network ────────────────────────────────────────────────────────── */
  section('6. Typing sends no requests');
  {
    const { page } = await newPage();
    await open(page, '/dashboard');
    await page.waitForTimeout(1500);
    const seen = [];
    const on = (r) => seen.push(r.url());
    page.on('request', on);
    await typeQuery(page, 'anthropic api key');
    await page.waitForTimeout(300);
    page.off('request', on);
    check('NO REQUEST WHILE TYPING — the index is local', seen.length === 0, seen.join(' | '));
    await page.close();
  }

  /* ── 7. Themes ─────────────────────────────────────────────────────────── */
  section('7. The dropdown reads in every theme');
  {
    const { page } = await newPage();
    await open(page, '/dashboard');
    for (const theme of ['light', 'dark', 'financial']) {
      await page.evaluate((t) => document.documentElement.setAttribute('data-theme', t), theme);
      await typeQuery(page, 'claude');
      const c = await page.evaluate(() => {
        const l = document.getElementById('cx-mh-results');
        const pick = (sel) => getComputedStyle(l.querySelector(sel)).color;
        return { bg: getComputedStyle(l).backgroundColor, lbl: pick('.cx-mh-opt-lbl'), path: pick('.cx-mh-opt-path'),
          tag: pick('.cx-mh-opt-tag'), saved: pick('.cx-mh-opt-saved .cx-mh-opt-lbl'), size: parseFloat(getComputedStyle(l.querySelector('.cx-mh-opt-path')).fontSize) };
      });
      const bg = rgba(c.bg);
      const r = (x) => Math.round(ratio(over(rgba(x), bg), bg) * 10) / 10;
      check(theme + ': label, breadcrumb, tag and last row all clear 4.5:1',
        r(c.lbl) >= 4.5 && r(c.path) >= 4.5 && r(c.tag) >= 4.5 && r(c.saved) >= 4.5,
        'label ' + r(c.lbl) + ' · path ' + r(c.path) + ' · tag ' + r(c.tag) + ' · last ' + r(c.saved) + ' on ' + c.bg);
      check(theme + ': nothing below 12px', c.size >= 12, c.size);
      await page.keyboard.press('Escape');
    }
    await page.close();
  }

  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
