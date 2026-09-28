/* tests/browser/active-project.smoke.js
 * ---------------------------------------------------------------------------
 * Home says "Choose your active project" — not "Create project" — when
 * projects exist and none is active, and creating one then activates it.
 *
 * THE BUG
 * A user created a project and Home kept showing "Start a migration" with
 * step 3 "Create project", above a footer that said "2 projects". Two causes:
 * saveProject() in projects.html activated a new project only when it was
 * the first ever, so with older projects and none active the new one was
 * saved and never activated; and Home's empty state did not look at whether
 * any projects existed. Both are fixed; this walks the brief's six checks on
 * the real pages.
 *
 *   1. projects exist, none active → "Choose your active project", the
 *      right count, CTA to /projects;
 *   2. creating a project from there activates it, and Home shows it;
 *   3. with A active, creating B leaves A active;
 *   4. a brand-new account still gets "Create a project" → /projects?new=1;
 *   5. deactivating the active project by a manual status → "Choose…";
 *   6. the footer count matches, and nothing throws on either page.
 *
 * Run it by hand:  node tests/browser/active-project.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8437);
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

const A = { id: 'proj_a', name: 'Alpha migration', created: '2026-09-01T00:00:00Z', modified: '2026-09-01T00:00:00Z' };
const B = { id: 'proj_b', name: 'Bravo migration', created: '2026-09-02T00:00:00Z', modified: '2026-09-02T00:00:00Z' };

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });

  // One fresh browser per scenario; `seed` is what localStorage holds on
  // first load. The init script only seeds once, so the pages' own writes
  // survive navigation within a scenario.
  async function scenario(seed) {
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
    const page = await ctx.newPage();
    const errors = [];
    page.on('pageerror', (e) => errors.push(page.url().replace(BASE, '') + ': ' + e.message));
    page.on('dialog', (d) => d.accept());
    await page.route('**/*', (r) => (r.request().url().startsWith(BASE) ? r.continue() : r.abort()));
    await page.addInitScript((seed) => {
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
      // No legacy single-project settings, so neither page's one-time
      // migration invents a project behind the test's back.
      localStorage.setItem('cygenix_projects_migrated', '1');
      localStorage.setItem('cygenix_projects', JSON.stringify(seed.projects));
      if (seed.active) localStorage.setItem('cygenix_active_project_id', seed.active);
      else localStorage.removeItem('cygenix_active_project_id');
    }, seed);
    return { ctx, page, errors };
  }
  async function home(page) {
    await page.goto(BASE + '/dashboard', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => {
      const t = document.querySelector('#view-dashboard .cx-title');
      return t && t.textContent.trim().length > 0;
    }, null, { timeout: 15000 });
    await page.waitForTimeout(300);
    return page.evaluate(() => {
      const root = document.querySelector('#view-dashboard .hm-grid');
      const steps = Array.from(document.querySelectorAll('#view-dashboard .cx-step')).map((s) => {
        const a = s.querySelector('a.cx-btn');
        return { title: (s.querySelector('.cx-step-t') || {}).textContent || '', text: (s.querySelector('.cx-step-p') || {}).textContent || '',
          cta: a ? a.textContent.trim() : '', href: a ? a.getAttribute('href') : '', disabled: !!(a && a.getAttribute('aria-disabled') === 'true') };
      });
      return { empty: !!(root && root.classList.contains('hm-empty')),
        title: (document.querySelector('#view-dashboard .cx-title') || {}).textContent || '',
        foot: (document.querySelector('#view-dashboard .hm-foot') || {}).textContent || '', steps };
    });
  }
  async function createProject(page, name) {
    await page.goto(BASE + '/projects', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => typeof window.openProjectModal === 'function' && typeof window.saveProject === 'function');
    await page.evaluate(() => openProjectModal());
    await page.fill('#pm-name', name);
    await page.evaluate(() => saveProject());
    await page.waitForTimeout(200);
    return page.evaluate((name) => {
      const list = JSON.parse(localStorage.getItem('cygenix_projects') || '[]');
      const made = list.find((p) => p.name === name) || {};
      let sess = null; try { sess = JSON.parse(sessionStorage.getItem('cygenix_active_project') || 'null'); } catch (e) {}
      const card = Array.from(document.querySelectorAll('.proj-card')).find((c) => c.textContent.indexOf(name) !== -1);
      return { id: made.id, status: made.status, active: localStorage.getItem('cygenix_active_project_id'),
        sessName: sess && sess.name, cardActive: !!(card && card.classList.contains('active-project')), count: list.length };
    }, name);
  }

  console.log('Active project — Home and the Projects page\n');

  /* ── 1 ─────────────────────────────────────────────────────────────────── */
  section('1. Projects exist, none is active');
  {
    const { ctx, page, errors } = await scenario({ projects: [A, B], active: null });
    const h = await home(page);
    const s3 = h.steps[2] || {};
    check('Home is the empty state (it does not pick a project itself)', h.empty, JSON.stringify(h.title));
    check('STEP 3 SAYS "Choose your active project"', s3.title === 'Choose your active project', s3.title);
    check('…with the right count', /You have 2 projects, but none is active\./.test(s3.text), s3.text);
    check('…and its button goes to /projects', s3.cta === 'Choose project' && s3.href === '/projects', s3.cta + ' ' + s3.href);
    check('…and can be clicked with no connections saved', !s3.disabled);
    check('the heading is not "Start a migration"', h.title === 'Pick up where you left off', h.title);
    check('the footer counts 2 projects', /^2 projects/.test(h.foot.trim()), h.foot);

    /* ── 2 ── */
    section('2. Creating a project from that state activates it');
    const made = await createProject(page, 'Charlie migration');
    check('THE NEW PROJECT IS THE ACTIVE ONE', made.active === made.id && !!made.id, JSON.stringify(made));
    check('its record says active', made.status === 'active');
    check('its card shows Active', made.cardActive);
    check('…and setActive refreshed the session copy too', made.sessName === 'Charlie migration', made.sessName);
    const h2 = await home(page);
    check('HOME SHOWS THE PROJECT, NOT THE EMPTY STATE', !h2.empty && h2.title === 'Charlie migration', h2.title);
    check('nothing threw on /dashboard or /projects', errors.length === 0, errors.slice(0, 3).join(' | '));
    await ctx.close();
  }

  /* ── 3 ─────────────────────────────────────────────────────────────────── */
  section('3. With A active, creating B leaves A active');
  {
    const { ctx, page, errors } = await scenario({ projects: [A], active: A.id });
    const made = await createProject(page, 'Delta migration');
    check('A IS STILL ACTIVE', made.active === A.id, made.active);
    check('the new project is not marked active', made.status !== 'active' && !made.cardActive, JSON.stringify(made));
    const h = await home(page);
    check('Home still shows A', !h.empty && h.title === A.name, h.title);
    check('nothing threw', errors.length === 0, errors.slice(0, 3).join(' | '));
    await ctx.close();
  }
  {
    // An active id that points at a deleted project counts as none active.
    const { ctx, page } = await scenario({ projects: [A], active: 'proj_deleted' });
    const made = await createProject(page, 'Echo migration');
    check('an active id pointing at a deleted project is replaced by the new one', made.active === made.id, made.active);
    await ctx.close();
  }

  /* ── 4 ─────────────────────────────────────────────────────────────────── */
  section('4. A brand-new account');
  {
    const { ctx, page, errors } = await scenario({ projects: [], active: null });
    const h = await home(page);
    const s3 = h.steps[2] || {};
    check('STEP 3 IS STILL "Create a project"', s3.title === 'Create a project' && s3.cta === 'Create project', s3.title + ' / ' + s3.cta);
    check('…to /projects?new=1', s3.href === '/projects?new=1', s3.href);
    check('…dimmed until both connections exist', s3.disabled);
    check('the heading is "Start a migration"', h.title === 'Start a migration', h.title);
    check('the footer counts 0 projects', /^0 projects/.test(h.foot.trim()), h.foot);
    const made = await createProject(page, 'First migration');
    check('the first project ever is still activated', made.active === made.id, JSON.stringify(made));
    check('nothing threw', errors.length === 0, errors.slice(0, 3).join(' | '));
    await ctx.close();
  }

  /* ── 5 ─────────────────────────────────────────────────────────────────── */
  section('5. Deactivating the active project by a manual status');
  {
    const { ctx, page, errors } = await scenario({ projects: [A, B], active: A.id });
    await page.goto(BASE + '/projects', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => typeof window.editProject === 'function');
    await page.evaluate((id) => editProject(id), A.id);
    await page.selectOption('#pm-status', 'inactive');
    await page.evaluate(() => saveProject());
    await page.waitForTimeout(200);
    const active = await page.evaluate(() => localStorage.getItem('cygenix_active_project_id'));
    check('the active id is cleared, as intended', active === null, active);
    const h = await home(page);
    const s3 = h.steps[2] || {};
    check('HOME SAYS "Choose your active project", NOT "Create project"', s3.title === 'Choose your active project' && s3.href === '/projects', s3.title);
    check('the footer still counts 2 projects', /^2 projects/.test(h.foot.trim()), h.foot);
    check('nothing threw', errors.length === 0, errors.slice(0, 3).join(' | '));
    await ctx.close();
  }

  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
