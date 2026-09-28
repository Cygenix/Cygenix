/* tests/browser/plan-actuals.smoke.js
 * ---------------------------------------------------------------------------
 * Project Plan actuals (phase 1) on the real page, against a v1 plan shaped
 * exactly like the live one: "Plan — New estimate", whose first task baked
 * twelve objects into its title and wrote ": +11 more" for the rest.
 *
 * Walks the brief's checks:
 *   1. the plan migrates: all 23 objects show (12, then "+11 more" expands),
 *      recovered from the Configurator; a plan that cannot be recovered is
 *      flagged "objects incomplete";
 *   2. set AP Done: its dot turns green, the roll-up updates, the bar darkens;
 *   3. an untouched object in a phase whose weeks are all past turns red and
 *      the summary's late count goes up (in red);
 *   4. Done with a date after the planned end: green with a red ring, and the
 *      tooltip says done late;
 *   5. print shows the legend; the CSV has the new columns, one row per object;
 *   6. a reload keeps every status.
 * And the rules around them: nothing red before tracking starts, the popover
 * closes on Esc and outside click, one save per click (no render loop), the
 * dots clear 3:1 against the task column in both palettes, and the darker bar
 * is visibly darker on every phase tint.
 *
 * Run it by hand:  node tests/browser/plan-actuals.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8438);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const BASE = 'http://localhost:' + PORT;
const TODAY = '2026-09-28';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.json': 'application/json', '.woff2': 'font/woff2' };
const ALIAS = { '/project-plan': '/project_plan.html' };
const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (ALIAS[p]) p = ALIAS[p];
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end('no'); }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

const rgb = (s) => (s.match(/[\d.]+/g) || []).slice(0, 3).map(Number);
const lum = (c) => { const ch = c.map((v) => { v /= 255; return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); }); return 0.2126 * ch[0] + 0.7152 * ch[1] + 0.0722 * ch[2]; };
const ratio = (a, b) => { const [x, y] = [lum(a), lum(b)].sort((p, q) => q - p); return (x + 0.05) / (y + 0.05); };
const r1 = (n) => Math.round(n * 10) / 10;

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const EM = require(path.join(PUB, 'cygenix-effort-model.js'));

  // The Configurator estimate and the v1 plan built from it.
  // The Configurator's own 23 default modules: its normaliser keeps ticks
  // only for modules in the estimate's list, exactly as the import did.
  const est = EM.emNewDoc('New estimate');
  const mods = est.modules.slice();
  const BANK = mods[15];              // any untouched object will do for the popover checks
  for (const m of mods) est.ticks['analysis|' + m] = 1;
  for (const m of ['AP', 'AR']) est.ticks['design|' + m] = 1;
  const r = EM.emCompute(est);
  const ucA = r.perUseCase.find((u) => u.id === 'analysis').name;
  const ucD = r.perUseCase.find((u) => u.id === 'design').name;
  const work = (tid, ym, ws) => Object.fromEntries(ws.map((w) => [tid + '|' + ym + '|' + w, { t: 'work' }]));
  const v1 = {
    v: 1, name: 'Plan — New estimate', client: '', timeline: { start: '2026-06', months: 6 },
    phases: [{ id: 'pa', name: ucA, color: 0 }, { id: 'pd', name: ucD, color: 1 }],
    tasks: [
      { id: 'ta', phaseId: 'pa', title: [ucA].concat(mods.slice(0, 12).map((m) => ': ' + m), [': +11 more']).join('\n'), resource: 'Curtis', comment: '31 FP' },
      { id: 'td', phaseId: 'pd', title: [ucD, ': AP', ': AR'].join('\n'), resource: 'Curtis', comment: '3 FP' },
    ],
    // Analysis: all of June (past). Design: late October (future).
    cells: Object.assign(work('ta', '2026-06', [1, 2, 3, 4]), work('td', '2026-10', [3, 4])),
  };
  // A second v1 plan whose estimate no longer exists — cannot be recovered.
  const orphan = JSON.parse(JSON.stringify(v1));
  orphan.name = 'Plan — Deleted estimate';

  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 }, acceptDownloads: true });
  await ctx.addInitScript(({ TODAY }) => {
    // Pin "today" so the late rules are tested against a known week.
    const T = new Date(TODAY + 'T10:00:00').getTime();
    const RealDate = Date;
    class FakeDate extends RealDate { constructor(...a) { if (a.length) super(...a); else super(T); } static now() { return T; } }
    window.Date = FakeDate;
  }, { TODAY });
  await ctx.addInitScript(({ est, v1, orphan }) => {
    if (sessionStorage.getItem('__seeded')) return;
    sessionStorage.setItem('__seeded', '1');
    const exp = String(Date.now() + 3600e3);
    for (const s of [localStorage, sessionStorage]) { s.setItem('cygenix_token', 'smoke'); s.setItem('cygenix_expires', exp); }
    localStorage.setItem('cygenix_onboarded', '1');
    localStorage.setItem('cygenix_user', JSON.stringify({ email: 'you@example.test', name: 'You' }));
    localStorage.setItem('cygenix_tier', 'pro');
    // cookie-consent.js JSON-parses this; a bare string is "no answer yet" and the banner shows.
    localStorage.setItem('cygenix_cookie_consent', JSON.stringify({ version: '2', essential: true, functional: true, analytics: false, timestamp: new Date().toISOString() }));
    localStorage.setItem('acct-cygenix.ciamlogin.com-x', JSON.stringify({ homeAccountId: 'x', environment: 'cygenix.ciamlogin.com',
      authorityType: 'MSSTS', username: 'you@example.test', localAccountId: 'x', tenantId: 'x' }));
    localStorage.setItem('cygenix_effort_estimates_v1', JSON.stringify({ active: 'New estimate', estimates: { 'New estimate': est } }));
    localStorage.setItem('cygenix_project_plans_v1', JSON.stringify({ active: v1.name, plans: { [v1.name]: v1, [orphan.name]: orphan } }));
  }, { est, v1, orphan });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.route('**/*', (rq) => (rq.request().url().startsWith(BASE) ? rq.continue() : rq.abort()));

  const load = async () => {
    await page.goto(BASE + '/project-plan', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => document.querySelectorAll('#pp-grid .pp-dot').length > 0, null, { timeout: 15000 });
    await page.waitForTimeout(200);
  };
  const stored = () => page.evaluate(() => JSON.parse(localStorage.getItem('cygenix_project_plans_v1')));
  const taskRow = (tid) => `#pp-grid tr:has(.pp-dot[data-task="${tid}"])`;
  const dot = (tid, key) => `#pp-grid .pp-dot[data-task="${tid}"][data-key="${key}"]`;
  const dotInfo = (tid, key) => page.evaluate((sel) => {
    const d = document.querySelector(sel);
    if (!d) return null;
    const cs = getComputedStyle(d, '::before');
    return { cls: d.className, title: d.title, bg: cs.backgroundColor, ring: cs.boxShadow };
  }, dot(tid, key));
  const setStatus = async (tid, key, status) => {
    await page.click(dot(tid, key));
    await page.waitForSelector('#pp-pop:not([hidden])');
    await page.click(`#pp-pop [data-status="${status}"]`);
    await page.waitForTimeout(80);
  };

  console.log('Project Plan — actuals\n');
  await load();

  /* ── 1. Migration ─────────────────────────────────────────────────────── */
  section('1. The v1 plan migrates; the lost objects come back');
  let st = await stored();
  const plan = st.plans['Plan — New estimate'];
  check('the stored plan is now v2', plan.v === 2);
  check('ALL 23 OBJECTS ARE STORED — the eleven "+11 more" names recovered', plan.tasks[0].objects.length === 23 && plan.tasks[0].objects.join() === mods.join(), plan.tasks[0].objects.length);
  check('the title is the task name alone', plan.tasks[0].title === ucA);
  check('the recovered task is not flagged', !plan.tasks[0].objectsIncomplete);
  let shown = await page.$$eval('#pp-grid .pp-dot[data-task="ta"]', (a) => a.length);
  check('the grid shows twelve, then "+11 more" (display only)', shown === 12 && await page.isVisible('#pp-grid .pp-more[data-more="ta"]'), shown);
  await page.click('#pp-grid .pp-more[data-more="ta"]');
  shown = await page.$$eval('#pp-grid .pp-dot[data-task="ta"]', (a) => a.length);
  check('…and expands to all 23', shown === 23, shown);
  check('BEFORE ANY STATUS IS SET NOTHING IS RED: no red dots, no hatching, no summary',
    await page.$$eval('#pp-grid .pp-dot.is-late', (a) => a.length) === 0 && await page.$$eval('#pp-grid .pp-overdue', (a) => a.length) === 0
    && !/% done/.test(await page.textContent('#pp-stats')));
  const orphanPlan = st.plans['Plan — Deleted estimate'];
  check('a plan whose estimate is gone keeps its 12 names and is FLAGGED', orphanPlan.tasks[0].objects.length === 12 && orphanPlan.tasks[0].objectsIncomplete === true);
  await page.selectOption('#pp-plan-sel', 'Plan — Deleted estimate');
  await page.waitForTimeout(150);
  check('…and the grid says "objects incomplete — re-import from Configurator"', /objects incomplete — re-import from Configurator/.test(await page.textContent('#pp-grid')));
  await page.selectOption('#pp-plan-sel', 'Plan — New estimate');
  await page.waitForTimeout(150);

  /* ── 2. Done ──────────────────────────────────────────────────────────── */
  section('2. AP in "Initial analysis" set to Done');
  await page.evaluate(() => { window.__saves = 0; const o = window.ppSave; window.ppSave = function () { window.__saves++; return o.apply(this, arguments); }; });
  const plainBg = await page.evaluate(() => { const c = document.querySelector('#pp-grid tr:has(.pp-dot[data-task="ta"]) td.pp-cell[title^="June · week 1"]'); return c && getComputedStyle(c).backgroundColor; });
  await setStatus('ta', 'AP', 'done');
  check('ONE SAVE PER CLICK — the popover does not re-fire its own save', await page.evaluate(() => window.__saves) === 1, await page.evaluate(() => window.__saves));
  // Put the done date inside the planned weeks, so this one is on time.
  await page.fill('#pp-pop-done', '2026-06-20');
  await page.dispatchEvent('#pp-pop-done', 'change');
  await page.waitForTimeout(80);
  let d = await dotInfo('ta', 'AP');
  check('AP\'S DOT IS GREEN', /\bis-done\b/.test(d.cls) && !/is-donelate/.test(d.cls), d.cls);
  const roll = await page.textContent(taskRow('ta') + ' .pp-rollup');
  check('the roll-up appears: 1/23 done', /^1\/23 done/.test(roll.trim()), roll);
  // AP was marked Done (which stamps today as its start) and its done date
  // moved back to 20 June — so the start is pulled back to 20 June too, and
  // the work ran June W3 onward. W3 and W4 darken; W1 and W2 keep the plan tint.
  const cellBg = (w) => page.evaluate((w) => { const c = document.querySelector('#pp-grid tr:has(.pp-dot[data-task="ta"]) td.pp-cell[title^="June · week ' + w + '"]');
    return c && getComputedStyle(c).backgroundColor; }, w);
  const w2 = await cellBg(2), w3 = await cellBg(3);
  check('THE BAR DARKENS where the work happened', w3 && plainBg && w3 !== plainBg && lum(rgb(w3)) < lum(rgb(plainBg)), plainBg + ' → ' + w3);
  check('…and only there: the weeks before it started keep the plan tint', w2 === plainBg, w2);
  check('the done date moved back pulled the stamped start back with it',
    (await stored()).plans['Plan — New estimate'].actuals.ta.AP.startedAt === '2026-06-20');
  check('the design task, untouched, shows no roll-up', !(await page.$(taskRow('td') + ' .pp-rollup')));

  /* ── 3. Late ──────────────────────────────────────────────────────────── */
  section('3. Objects left Not started in a phase that is all past');
  d = await dotInfo('ta', 'Addresses');
  check('ITS DOT TURNS RED', /is-late/.test(d.cls) && /late/.test(d.title), d.cls);
  const stats = await page.innerHTML('#pp-stats');
  const lateN = Number((/(\d+) late/.exec(await page.textContent('#pp-stats')) || [])[1]);
  check('THE SUMMARY COUNTS THEM: % done and the late count, in red', /\d+% done/.test(stats) && lateN === 22 && /pp-late-txt">22 late/.test(stats), await page.textContent('#pp-stats'));
  check('the roll-up counts them too', /1\/23 done · 22 late/.test(await page.textContent(taskRow('ta') + ' .pp-rollup')));
  const hatched = await page.$$eval(taskRow('ta') + ' td.pp-overdue', (a) => a.length);
  check('weeks past the planned end, up to now, are hatched red (July W1–September W4: 12)', hatched === 12, hatched);
  check('the future design phase is not late', !/is-late/.test((await dotInfo('td', 'AP')).cls));

  /* ── 4. Done late ─────────────────────────────────────────────────────── */
  section('4. Done after the planned end');
  await setStatus('ta', 'AR', 'done');           // stamped today: 28 Sep, well after June
  d = await dotInfo('ta', 'AR');
  check('GREEN WITH A RED RING', /is-done/.test(d.cls) && /is-donelate/.test(d.cls) && /rgb\(156, 63, 56\)|var\(--red\)/.test(d.ring) === true || (/is-donelate/.test(d.cls) && d.ring !== 'none'), d.cls + ' ' + d.ring);
  check('the tooltip says done late, with the dates and the planned weeks',
    /done late/.test(d.title) && /Done 2026-09-28/.test(d.title) && /Planned June W1 – June W4/.test(d.title) && /Source: manual/.test(d.title), d.title);
  check('the roll-up counts it as done, and says it was late', /2\/23 done · 21 late · 1 done late/.test(await page.textContent(taskRow('ta') + ' .pp-rollup')));

  /* ── popover behaviour ───────────────────────────────────────────────── */
  section('The popover');
  await page.click(dot('ta', BANK));
  await page.waitForSelector('#pp-pop:not([hidden])');
  const cover = await page.evaluate(() => {
    const pop = document.getElementById('pp-pop').getBoundingClientRect();
    return [...document.querySelectorAll('#pp-grid .pp-dot')].filter((d) => {
      const r = d.getBoundingClientRect();
      return r.right > pop.left && r.left < pop.right && r.bottom > pop.top && r.top < pop.bottom;
    }).length;
  });
  check('the popover sits beside the dot and covers no other dot', cover === 0, cover);
  check('it shows three states and the dates', await page.$$eval('#pp-pop [data-status]', (a) => a.map((b) => b.textContent).join('|')) === 'Not started|Active|Done'
    && !!(await page.$('#pp-pop-start')) && !!(await page.$('#pp-pop-done')));
  await page.keyboard.press('Escape');
  check('Esc closes it', await page.isHidden('#pp-pop'));
  await page.click(dot('ta', BANK));
  await page.mouse.click(1300, 120);
  check('a click outside closes it', await page.isHidden('#pp-pop'));
  await setStatus('ta', BANK, 'active');
  const bank = (await stored()).plans['Plan — New estimate'].actuals.ta[BANK];
  check('Active stamps today as the start', bank.status === 'active' && bank.startedAt === TODAY && bank.doneAt === '');
  await page.click(`#pp-pop [data-status="not_started"]`);
  await page.waitForTimeout(80);
  check('Not started removes the record', !(BANK in (await stored()).plans['Plan — New estimate'].actuals.ta));
  await page.keyboard.press('Escape');

  /* ── 5. Print and CSV ────────────────────────────────────────────────── */
  section('5. Print and CSV');
  await page.emulateMedia({ media: 'print' });
  check('PRINT SHOWS THE LEGEND', await page.evaluate(() => getComputedStyle(document.getElementById('pp-legend')).display) === 'flex');
  check('…and keeps colour for dots and hatching', await page.evaluate(() => getComputedStyle(document.querySelector('.pp')).printColorAdjust === 'exact'));
  await page.emulateMedia({ media: 'screen' });
  check('the legend is not on screen', await page.evaluate(() => getComputedStyle(document.getElementById('pp-legend')).display) === 'none');
  const dl = page.waitForEvent('download');
  await page.click('button:has-text("CSV")');
  const csv = fs.readFileSync(await (await dl).path(), 'utf8').trim().split('\n');
  check('THE CSV HAS THE NEW COLUMNS after the original ones', csv[0].startsWith('phase,task,resource,comment,June W1') && csv[0].endsWith(',object,status,started,done,late'), csv[0].slice(-60));
  check('…one row per object', csv.length - 1 === 23 + 2, csv.length - 1);
  check('…with status, dates and lateness', csv.some((l) => /,AP,done,2026-09-28,2026-06-20,$/.test(l) || /,AP,done,[\d-]+,2026-06-20,$/.test(l))
    && csv.some((l) => /,AR,done,2026-09-28,2026-09-28,done late$/.test(l)) && csv.some((l) => /,Addresses,not started,,,late$/.test(l)));

  /* ── Portfolio ───────────────────────────────────────────────────────── */
  await page.click('#pf-open-btn');
  check('the portfolio shows % done and late for the plan', /\d+% done · \d+ late/.test(await page.textContent('#pf-inner')));
  await page.click('button:has-text("Back to plan")');

  /* ── 6. Reload ───────────────────────────────────────────────────────── */
  section('6. A reload keeps every status');
  await load();
  check('AP IS STILL GREEN, AR STILL DONE LATE, ADDRESSES STILL RED',
    /is-done/.test((await dotInfo('ta', 'AP')).cls) && /is-donelate/.test((await dotInfo('ta', 'AR')).cls) && /is-late/.test((await dotInfo('ta', 'Addresses')).cls));
  st = await stored();
  check('the migrated plan was not migrated again: still 23 objects, still v2', st.plans['Plan — New estimate'].tasks[0].objects.length === 23 && st.plans['Plan — New estimate'].v === 2);

  /* ── Contrast ────────────────────────────────────────────────────────── */
  section('Contrast');
  for (const theme of ['light', 'financial']) {
    await page.evaluate((t) => document.documentElement.setAttribute('data-theme', t), theme);
    await page.waitForTimeout(50);
    const c = await page.evaluate(() => {
      const cell = document.querySelector('#pp-grid td.pp-c-task');
      const get = (cls) => { const b = document.createElement('button'); b.className = 'pp-dot ' + cls; cell.appendChild(b);
        const v = getComputedStyle(b, '::before').backgroundColor; b.remove(); return v; };
      return { bg: getComputedStyle(cell).backgroundColor, none: get('is-none'), active: get('is-active'), done: get('is-done'), late: get('is-late') };
    });
    const rs = ['none', 'active', 'done', 'late'].map((k) => [k, r1(ratio(rgb(c[k]), rgb(c.bg)))]);
    check(theme + ': every dot clears 3:1 against the task column', rs.every(([, v]) => v >= 3), rs.map((x) => x.join(' ')).join(' · ') + ' on ' + c.bg);
  }
  await page.evaluate(() => document.documentElement.removeAttribute('data-theme'));
  const shades = await page.evaluate(() => { const PP = window.CygenixProjectPlan; return PP.PP_PALETTE.map((p) => [p.bg, PP.ppShade(p.bg, p.fg)]); });
  const hex = (h) => [1, 3, 5].map((i) => parseInt(h.slice(i, i + 2), 16));
  const worst = Math.min(...shades.map(([a, b]) => ratio(hex(a), hex(b))));
  check('the darker bar is distinguishable from the plan tint on every phase colour (≥1.3:1)', worst >= 1.3, r1(worst));

  check('nothing threw', errors.length === 0, errors.slice(0, 3).join(' | '));
  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
