/* tests/browser/sidebar-nav.smoke.js
 * ---------------------------------------------------------------------------
 * The dashboard sidebar actually switches views.
 *
 * THE BUG THIS EXISTS FOR
 * The nav's "am I on the dashboard" test was a regex written against
 * /dashboard.html. The addresses went extensionless and nobody updated it, so
 * on the real URL it was always false: all fifteen `view:` items fell through
 * to a navigate branch that assigned a URL differing only in the hash — which
 * does not reload a page — while the twenty `href:` items carried on working.
 * Clicking most of the menu did nothing at all, silently, with no console
 * error, because nothing had failed. Something had merely not happened.
 *
 * A source-level test would not have caught it. The regex was valid, the
 * branch was reachable, the fallback existed. What was wrong was the
 * relationship between a string and a deployed URL — which only a browser
 * sitting on that URL can tell you.
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/sidebar-nav.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8399);
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
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';   // the extensionless rewrite
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) {
    res.writeHead(404); return res.end('no');
  }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.route('**/*', (r) =>
    r.request().url().startsWith('http://localhost:' + PORT) ? r.continue() : r.abort());
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
    // The Audit Log item is gated on holding a role that can read the
    // organisation trail, and the sidebar reads that from the same
    // five-minute session cache cygenix-rbac.js writes. With the network cut
    // off there is nobody to ask, so the roles are seeded here — otherwise
    // this file would be testing the role gate rather than the fifteen-item
    // navigation contract it exists for.
    sessionStorage.setItem('cygenix_rbac_me', JSON.stringify({
      at: Date.now(), me: { oid: 'x', email: 'you@example.test', roles: ['OW', 'PA'] } }));
  });

  const url = (p) => 'http://localhost:' + PORT + p;
  const open = async (p) => {
    await page.goto(url(p), { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => !!document.querySelector('.cyg-nav-item'), null, { timeout: 15000 });
    await page.waitForTimeout(400);
  };
  const activeView = () => page.evaluate(() => {
    const el = document.querySelector('.view.active');
    return el ? el.id.replace(/^view-/, '') : null;
  });
  // Click through the DOM the way a person does, not by calling the handler.
  const clickNav = async (key) => {
    const ok = await page.evaluate((k) => {
      const el = document.querySelector('.cyg-nav-item[data-key="' + k + '"]');
      if (!el) return false;
      el.click();
      return true;
    }, key);
    await page.waitForTimeout(220);
    return ok;
  };

  console.log('Dashboard sidebar — the nav actually changes the view\n');

  await open('/dashboard');
  check('the dashboard is served at the extensionless address and the six-group rail renders',
    (await page.evaluate(() => document.querySelectorAll('.cyg-nav-item[data-key]').length)) === 15
    && (await page.evaluate(() => Array.from(document.querySelectorAll('.cyg-nav-label')).map(l => l.textContent.trim())
          .filter(l => l !== 'Pinned').join(',')))
       === 'Plan,Connect,Model,Run,Quality,Govern');
  check('the masthead is on the page: logo, project, search, region, avatar',
    await page.evaluate(() => !!document.querySelector('#cx-masthead .cx-logo') && !!document.getElementById('cyg-proj-btn')
      && !!document.getElementById('cx-mh-search') && !!document.getElementById('cx-mh-region') && !!document.getElementById('cyg-user-chip')));
  check('and the address really is /dashboard, with no extension',
    (await page.evaluate(() => location.pathname)) === '/dashboard');

  /* ── 1. Every view: destination switches the view ───────────────────────── */
  const VIEWS = await page.evaluate(() =>
    Array.from(document.querySelectorAll('.cyg-nav-item[data-key]'))
      .map((el) => el.dataset.key));

  // The fifteen the bug killed, by their view name — now spread across the
  // rail, the tab strips and the account menu. Each is reached the way a
  // person reaches it: a rail click, a tab click on the owning screen, or
  // the account menu. Search is the masthead field.
  const RAIL   = ['dashboard', 'connections', 'jobs', 'task-agent', 'audit'];
  const TABBED = { 'integrations': 'profiles', 'server-migration': 'jobs', 'reports': 'report-builder',
                   'inventory': 'report-builder', 'diagnostics': 'audit' };
  const ACCOUNT = ['project-settings', 'notifications', 'system-parameters', 'privacy-security'];

  const broken = [];
  for (const view of RAIL) {
    await open('/dashboard');
    const clicked = await clickNav(view);
    const now = await activeView();
    if (!clicked || now !== view) broken.push(view + ' (rail, landed on ' + now + ')');
  }
  check('every rail view: item switches the view when clicked', broken.length === 0, broken.join(' | '));

  const brokenTabs = [];
  for (const view of Object.keys(TABBED)) {
    await open('/dashboard');
    // The owning rail item first — a tab is inside its destination screen.
    const owner = TABBED[view];
    const ownerItem = await page.evaluate((k) => !!document.querySelector('.cyg-nav-item[data-key="' + k + '"]'), owner);
    if (ownerItem) { await clickNav(owner); await page.waitForTimeout(400); }
    else { await page.goto(url('/dashboard#goto=' + owner), { waitUntil: 'domcontentloaded' }); await page.waitForTimeout(700); }
    const clicked = await page.evaluate((k) => {
      const a = document.querySelector('#cyg-subnav-mount a[data-key="' + k + '"]');
      if (!a) return false; a.click(); return true;
    }, view);
    // A tab whose owner is its own page (Reports lives at /reports) reaches a
    // dashboard view by navigating there, which is a real load and a deep
    // link, not an in-place switch — so wait for the view rather than a timer.
    await page.waitForFunction((k) => {
      const el = document.querySelector('.view.active');
      return !!el && el.id === 'view-' + k;
    }, view, { timeout: 8000 }).catch(() => {});
    const now = await activeView();
    if (!clicked || now !== view) brokenTabs.push(view + ' (tab of ' + owner + ', landed on ' + now + ')');
  }
  check('every view: item that became a tab still switches the view from its strip', brokenTabs.length === 0, brokenTabs.join(' | '));

  const brokenAcct = [];
  for (const view of ACCOUNT) {
    await open('/dashboard');
    await page.click('#cyg-user-chip');
    await page.waitForTimeout(150);
    const clicked = await page.evaluate((k) => {
      const b = document.querySelector('#cyg-user-menu [data-acct-key="' + k + '"]');
      if (!b) return false; b.click(); return true;
    }, view);
    await page.waitForTimeout(250);
    const now = await activeView();
    if (!clicked || now !== view) brokenAcct.push(view + ' (account menu, landed on ' + now + ')');
  }
  check('every view: item that moved to the account menu still switches the view', brokenAcct.length === 0, brokenAcct.join(' | '));

  await open('/dashboard');
  await page.fill('#cx-mh-search', 'ledger');
  await page.press('#cx-mh-search', 'Enter');
  await page.waitForTimeout(400);
  check('the masthead search opens the Search view with the query in it',
    (await activeView()) === 'search' && (await page.evaluate(() => document.getElementById('search-input').value)) === 'ledger');

  // The tab strip lights the current tab, and the rail lights the owner.
  // (A same-document hash change would not run the deep-link reader — see
  // the note on coldLoad further down — so this goes somewhere else first.)
  const coldLoad = async (target) => {
    await page.goto(url('/sql-editor'), { waitUntil: 'domcontentloaded' });
    await page.goto(url(target), { waitUntil: 'domcontentloaded' });
  };
  await coldLoad('/dashboard#goto=server-migration');
  await page.waitForFunction(() => !!document.querySelector('#cyg-subnav-mount a.on'), null, { timeout: 8000 }).catch(() => {});
  check('a tabbed view lights its own tab and its owner on the rail',
    await page.evaluate(() => {
      const on = document.querySelector('#cyg-subnav-mount a.on');
      const rail = document.querySelector('.cyg-nav-item.active');
      return !!on && on.dataset.key === 'server-migration' && !!rail && rail.dataset.key === 'jobs';
    }));

  /* ── 1b. …in place, without reloading the whole application ─────────────── */
  //
  // This is the check that isolates the path test from the fallback. With the
  // stale regex still in place, the repaired fallback rescues every one of the
  // fifteen — by reloading the entire dashboard for each click. The user sees
  // a working menu that flashes white and re-fetches everything. That is not
  // this bug fixed, it is this bug made survivable, so the assertion is that
  // the document instance survives the click.
  await open('/dashboard');
  await page.evaluate(() => { window.__sameDocument = true; });
  await clickNav('connections');
  check('a view: item switches IN PLACE — no page reload',
    (await page.evaluate(() => window.__sameDocument === true)) === true
    && (await activeView()) === 'connections',
    'the fallback would also land on the view, but only by reloading the app');

  /* ── 2. The same item twice ─────────────────────────────────────────────── */
  await open('/dashboard');
  await clickNav('connections');
  const first = await activeView();
  await clickNav('connections');
  const second = await activeView();
  check('clicking the same item twice stays on that view',
    first === 'connections' && second === 'connections', first + ' then ' + second);
  check('and does not leave a stale goto in the address',
    !/goto=/.test(await page.evaluate(() => location.hash)));

  /* ── 3. A cold load of a deep link ──────────────────────────────────────── */
  //
  // Navigating from /dashboard to /dashboard#goto=x is a SAME-DOCUMENT hash
  // change: the page does not reload and the reader never runs. That is the
  // very trap this bug was made of, and the first draft of this test fell into
  // it — the deep-link checks passed on a view left over from the previous
  // one. Every cold load below goes somewhere else first.
  await coldLoad('/dashboard#goto=integrations');
  await page.waitForFunction(() => !!document.querySelector('.view.active'), null, { timeout: 15000 });
  await page.waitForTimeout(700);
  check('a cold load of /dashboard#goto=integrations lands on Integrations',
    (await activeView()) === 'integrations', await activeView());
  check('and the goto is cleared from the address, so a refresh does not re-fire it',
    !/goto=/.test(await page.evaluate(() => location.hash)),
    await page.evaluate(() => location.hash));

  // The view cygenix-project-summary.js INJECTS rather than ships — the reader
  // has to run late enough to see it.
  await coldLoad('/dashboard#goto=project-summary-document');
  await page.waitForTimeout(1200);
  check('a deep link to a view injected by a later script also lands',
    (await activeView()) === 'project-summary-document', await activeView());

  // An unknown view must not blank the screen.
  await coldLoad('/dashboard#goto=not-a-real-view');
  await page.waitForTimeout(700);
  const unknown = await activeView();
  check('an unknown view name leaves the dashboard on a real view, not blank',
    unknown !== null && unknown !== 'not-a-real-view', String(unknown));

  // The other transport.
  await page.goto(url('/dashboard'), { waitUntil: 'domcontentloaded' });
  await page.evaluate(() => sessionStorage.setItem('cyg_goto', 'audit'));
  await page.goto(url('/dashboard'), { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(700);
  check('the sessionStorage transport still works for links from other pages',
    (await activeView()) === 'audit', await activeView());
  check('and the key is consumed, not left for the next load',
    (await page.evaluate(() => sessionStorage.getItem('cyg_goto'))) === null);

  /* ── 4. href: items still navigate ──────────────────────────────────────── */
  await open('/dashboard');
  const hrefKeys = await page.evaluate(() =>
    Array.from(document.querySelectorAll('.cyg-nav-item[data-key]'))
      .map((el) => el.dataset.key));
  await clickNav('sql-editor');
  await page.waitForTimeout(600);
  check('an href: item still performs a real navigation',
    (await page.evaluate(() => location.pathname)) === '/sql-editor',
    await page.evaluate(() => location.pathname));

  await open('/dashboard');
  await clickNav('object-mapping');
  await page.waitForTimeout(600);
  check('and so does a second one',
    (await page.evaluate(() => location.pathname)) === '/object-mapping',
    await page.evaluate(() => location.pathname));

  /* ── The Audit Log's role gate ─────────────────────────────────────────── */
  //
  // Everything above runs with Owner + Platform Administrator seeded, so the
  // item is on the rail. This checks the other half: that it is NOT, for
  // somebody whose roles do not reach the organisation trail. Hiding is only
  // a courtesy — netlify/functions/audit.js refuses on the server regardless,
  // and tests/audit-api.test.js is what proves that — but a menu entry
  // leading to a refusal is a bad menu entry.
  await page.evaluate(() => sessionStorage.setItem('cygenix_rbac_me', JSON.stringify({
    at: Date.now(), me: { oid: 'x', email: 'you@example.test', roles: ['EN'] } })));
  await open('/dashboard');
  check('an Engineer does not see the Audit Log item',
    !(await page.evaluate(() => !!document.querySelector('.cyg-nav-item[data-key="audit"]'))));
  check('and the rest of the rail is untouched by that',
    (await page.evaluate(() =>
      document.querySelectorAll('.cyg-nav-item[data-key]').length)) === VIEWS.length - 1);
  // The audit tab strip goes with it: Performance and Diagnostics belong to
  // the Audit log screen, and an Engineer is not offered a strip for a
  // screen they cannot open.

  await page.evaluate(() => sessionStorage.setItem('cygenix_rbac_me', JSON.stringify({
    at: Date.now(), me: { oid: 'x', email: 'you@example.test', roles: ['AU'] } })));
  await open('/dashboard');
  check('an Auditor does — reading this screen is the whole role',
    await page.evaluate(() => !!document.querySelector('.cyg-nav-item[data-key="audit"]')));

  /* ── The status hairline ────────────────────────────────────────────────
   *
   * THE BUG IT REPLACED
   * #cyg-envbar was a fixed 22px bar at z-index 2000. The Ask Cygenix panel
   * is also fixed at top 0, at z-index 290, so the bar painted over its
   * header: the New and ✕ buttons start at y=12, which put the top 10px of
   * both under the bar. A click there navigated to /profiles instead of
   * pressing the button.
   *
   * That is not something a source test can see. What matters is what is at
   * a point on the screen, so every check below asks the browser what it
   * would hit — elementFromPoint — rather than what the CSS says.
   */
  const seedProfiles = (envClass, activeId) => page.evaluate(([env, id]) => {
    localStorage.setItem('cygenix_profiles_v1', JSON.stringify({
      v: 1, profiles: [{ id: 'FIN-DEV-01', name: 'Conv_DM to Azure', envClass: env, status: 'active' }],
      bindings: [], connMeta: {}, runRecords: [], events: [],
      settings: { envClasses: [], activeProfileId: id, selectedAt: 1 },
    }));
  }, [envClass, activeId]);

  const barBox = () => page.evaluate(() => {
    const b = document.getElementById('cyg-envbar');
    return b ? b.getBoundingClientRect().height : null;
  });
  const hitAt = (x, y) => page.evaluate(([px, py]) => {
    const n = document.elementFromPoint(px, py);
    return n ? (n.id || n.className || n.tagName) : null;
  }, [x, y]);

  await seedProfiles('DEV', 'FIN-DEV-01');
  await open('/dashboard');
  await page.waitForTimeout(400);

  check('the hairline renders once a profile exists',
    (await page.evaluate(() => !!document.getElementById('cyg-envbar'))));
  check('and rests at 2px, not the old 22', (await barBox()) === 2, await barBox());
  check('reserving those 2px of the page above the 60px masthead',
    (await page.evaluate(() => getComputedStyle(document.body).paddingTop)) === '62px',
    await page.evaluate(() => getComputedStyle(document.body).paddingTop));

  // 1. The assistant panel's buttons are clickable to their topmost pixel.
  await page.evaluate(() => window.CygenixAssistant && window.CygenixAssistant.open());
  await page.waitForSelector('#cygaClose', { state: 'visible', timeout: 5000 });
  // The panel slides in over .22s; a rect read mid-flight is off-screen and
  // elementFromPoint answers null, which would read as a pass-shaped failure.
  await page.waitForTimeout(500);
  const btnTops = await page.evaluate(() => ['cygaClose', 'cygaClear'].map((id) => {
    const r = document.getElementById(id).getBoundingClientRect();
    return { id, x: Math.round(r.left + r.width / 2), y: Math.round(r.top) + 1 };
  }));
  check('the assistant panel is actually on screen to be tested',
    btnTops.every((b) => b.x > 0 && b.x < 1440 && b.y > 0), JSON.stringify(btnTops));
  for (const b of btnTops) {
    const who = await hitAt(b.x, b.y);
    check('the assistant\'s ' + b.id + ' button is hittable at its very top pixel', who === b.id,
      'got ' + who + ' — this is the bug the hairline exists to fix');
  }

  /* The rail's profile chip. A 2px green line and a line that failed to
   * render look identical, so the fact lives in words as well, permanently
   * — and it must never be inside something a user can fold away. */
  const chip = () => page.evaluate(() => {
    const a = document.getElementById('cyg-prof-area');
    const c = document.getElementById('cyg-prof-chip');
    if (!a || !c) return null;
    return {
      hidden: a.hidden, cls: c.className,
      id: (document.getElementById('cyg-prof-id') || {}).textContent,
      env: (document.getElementById('cyg-prof-env') || {}).textContent,
      visible: c.getBoundingClientRect().height > 0,
      dot: getComputedStyle(c.querySelector('.cyg-prof-dot')).backgroundColor,
    };
  });
  {
    const c = await chip();
    check('the rail names the profile in words', c && c.id === 'FIN-DEV-01', JSON.stringify(c));
    check('and its environment class', c && c.env === 'DEV', JSON.stringify(c));
    check('and it is actually on screen, not merely in the DOM', c && c.visible && !c.hidden);
    check('carrying the same level as the hairline', c && /lv-green/.test(c.cls), c && c.cls);
  }

  // 3. Crossing the top edge quickly must not open it.
  await page.mouse.move(700, 1);
  await page.waitForTimeout(60);
  await page.mouse.move(700, 400);
  await page.waitForTimeout(500);
  check('sweeping across the top edge does NOT open the line — the 140ms delay holds',
    (await barBox()) === 2, await barBox());

  // 2. Hovering and leaving moves nothing on the page.
  const yBefore = await page.evaluate(() => {
    const n = document.querySelector('.page, main, #cyg-sidebar-mount + *') || document.body.children[1];
    return n ? n.getBoundingClientRect().top : 0;
  });
  await page.mouse.move(700, 3);
  await page.waitForTimeout(350);
  check('holding the pointer at the top edge opens it to 22px', (await barBox()) === 22, await barBox());
  check('and the text becomes readable',
    /FIN-DEV-01/.test(await page.textContent('#cyg-envbar')));
  const yDuring = await page.evaluate(() => {
    const n = document.querySelector('.page, main, #cyg-sidebar-mount + *') || document.body.children[1];
    return n ? n.getBoundingClientRect().top : 0;
  });
  check('expanding overlays the page rather than pushing it — NO vertical movement',
    Math.abs(yDuring - yBefore) < 0.5, yBefore + ' -> ' + yDuring);

  // Reported: "I can't see the collapse button on the green status bar."
  // There was none — moving the pointer away was the only way out, which is
  // not a gesture that exists on a touch device.
  check('the expanded bar offers a way to put it away',
    await page.evaluate(() => {
      const x = document.querySelector('#cyg-envbar .cyg-envbar-x');
      return !!x && x.getBoundingClientRect().width > 0;
    }));
  // With the assistant panel still open, which is the case that caught the
  // first version of this: pinned to the right edge, the button sat under
  // 420px of panel at z-index 290 and could not be pressed at all.
  check('and it is the button that is actually under the pointer there',
    await page.evaluate(() => {
      const x = document.querySelector('#cyg-envbar .cyg-envbar-x');
      const r = x.getBoundingClientRect();
      return document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2) === x;
    }),
    'pinned right it lands under the assistant panel; pinned left, under the rail');
  await page.click('#cyg-envbar .cyg-envbar-x');
  await page.waitForTimeout(400);
  check('and pressing it collapses the bar', (await barBox()) === 2, await barBox());
  check('without navigating to /profiles — the button sits inside that link',
    /\/dashboard$/.test(page.url()), page.url());

  await page.mouse.move(700, 500);
  await page.waitForTimeout(600);
  check('and it settles back to a hairline when the pointer leaves', (await barBox()) === 2, await barBox());

  /* The status colour must mean the same thing on every screen. It did not:
     the bar read var(--green), and 26 of the 27 pages carrying it redefine
     that in their own inline :root, disagreeing with each other. The same DEV
     profile was bright mint on the dashboard and forest green on Reports. */
  const barColour = () => page.evaluate(() => {
    const b = document.getElementById('cyg-envbar');
    return b ? getComputedStyle(b).backgroundColor : null;
  });
  const onDash = await barColour();
  check('the dashboard paints the green level from the fixed status palette',
    onDash === 'rgb(63, 107, 82)', onDash);
  await open('/profiles');
  await page.waitForTimeout(400);
  const onProf = await barColour();
  check('and Profiles — whose own --green is a completely different green — paints it identically',
    onProf === onDash, onProf + ' vs ' + onDash);
  check('the rail chip agrees with the bar, on both pages',
    await page.evaluate(() => {
      const d = document.querySelector('.cyg-prof-dot');
      const b = document.getElementById('cyg-envbar');
      return !!d && !!b && getComputedStyle(d).backgroundColor === getComputedStyle(b).backgroundColor;
    }));
  await open('/dashboard');
  await page.waitForTimeout(300);

  // The collapsed line must not intercept a click meant for the page.
  check('a click at the very top edge does not land on the profile link while collapsed',
    (await hitAt(700, 1)) === 'cyg-envbar-hit');
  check('and the collapsed line itself takes no pointer events',
    (await page.evaluate(() => getComputedStyle(document.getElementById('cyg-envbar')).pointerEvents)) === 'none');

  // 6. The busy bar still wins the strip.
  check('the busy bar still sits above the hairline',
    await page.evaluate(() => {
      const b = document.querySelector('.cygbusy'), h = document.getElementById('cyg-envbar');
      if (!b || !h) return false;
      return Number(getComputedStyle(b).zIndex) > Number(getComputedStyle(h).zIndex);
    }));

  // 5. Keyboard.
  await page.evaluate(() => document.getElementById('cyg-envbar').focus());
  await page.waitForTimeout(400);          // the 180ms height transition, with room
  check('focusing the profile link expands it, so tabbing does not land on an invisible link',
    (await barBox()) === 22, await barBox());
  await page.evaluate(() => document.getElementById('cyg-envbar').blur());
  await page.waitForTimeout(120);

  // 4. Red locks open, pushes the page down, and moves the assistant below it.
  await seedProfiles('PRD', 'FIN-DEV-01');
  await open('/dashboard');
  await page.waitForTimeout(400);
  check('a PRD profile locks the bar open at 22px', (await barBox()) === 22, await barBox());
  check('the page concedes the space — production owns it',
    (await page.evaluate(() => getComputedStyle(document.body).paddingTop)) === '82px',
    await page.evaluate(() => getComputedStyle(document.body).paddingTop));
  check('and the assistant panel starts BELOW the bar rather than under it',
    (await page.evaluate(() => getComputedStyle(document.querySelector('.cyga')).top)) === '22px');
  {
    const c = await chip();
    check('the rail chip turns red with it', c && /lv-red/.test(c.cls), c && c.cls);
    check('and still names the profile', c && c.id === 'FIN-DEV-01' && c.env === 'PRD', JSON.stringify(c));
  }
  // Collapsing the rail must not lose the one bit that matters.
  await page.evaluate(() => document.getElementById('cyg-sidebar-toggle').click());
  await page.waitForTimeout(400);
  check('collapsing the rail keeps the dot — 54px has no room for a name, but PRD must still show',
    await page.evaluate(() => {
      const d = document.querySelector('.cyg-prof-dot');
      return !!d && d.getBoundingClientRect().width > 0;
    }));
  await page.evaluate(() => document.getElementById('cyg-sidebar-toggle').click());
  await page.waitForTimeout(400);
  await page.mouse.move(700, 3); await page.waitForTimeout(350);
  check('hover does nothing to a locked bar', (await barBox()) === 22);
  await page.mouse.move(700, 500);

  // Red also covers a session with nothing selected — every write is blocked.
  await seedProfiles('DEV', null);
  await open('/dashboard');
  await page.waitForTimeout(400);
  check('no profile selected is red and locked too', (await barBox()) === 22, await barBox());
  check('and says why', /writes are blocked/i.test(await page.textContent('#cyg-envbar')));

  // Before the first profile exists, nothing changes anywhere.
  await page.evaluate(() => localStorage.removeItem('cygenix_profiles_v1'));
  await open('/dashboard');
  await page.waitForTimeout(400);
  check('with no profiles at all there is no bar and only the masthead is reserved',
    !(await page.evaluate(() => !!document.getElementById('cyg-envbar')))
    && (await page.evaluate(() => getComputedStyle(document.body).paddingTop)) === '60px');
  check('and no chip either — before adoption the console looks exactly as it did',
    (await chip()).hidden === true, JSON.stringify(await chip()));

  // 8. Touch: no hover, so tap must open it.
  {
    const tctx = await browser.newContext({ viewport: { width: 900, height: 800 }, hasTouch: true });
    // Both init scripts go on the CONTEXT, before any page exists, or the
    // first navigation runs without them and the auth gate bounces the page.
    await tctx.addInitScript(() => {
      const exp = String(Date.now() + 3600e3);
      for (const s of [localStorage, sessionStorage]) {
        s.setItem('cygenix_token', 'smoke'); s.setItem('cygenix_expires', exp);
      }
      localStorage.setItem('cygenix_onboarded', '1');
      localStorage.setItem('cygenix_user', JSON.stringify({ email: 'you@example.test', name: 'You' }));
      localStorage.setItem('cygenix_tier', 'pro');
      localStorage.setItem('cygenix_cookie_consent', 'all');
      // The MSAL account record: without it auth-gate.js bounces the page to
      // /login?reason=protected and the hairline never gets to render.
      localStorage.setItem('acct-cygenix.ciamlogin.com-x', JSON.stringify({
        homeAccountId: 'x', environment: 'cygenix.ciamlogin.com', authorityType: 'MSSTS',
        username: 'you@example.test', localAccountId: 'x', tenantId: 'x' }));
      localStorage.setItem('cygenix_profiles_v1', JSON.stringify({
        v: 1, profiles: [{ id: 'FIN-DEV-01', name: 'Conv_DM', envClass: 'DEV', status: 'active' }],
        bindings: [], connMeta: {}, runRecords: [], events: [],
        settings: { envClasses: [], activeProfileId: 'FIN-DEV-01', selectedAt: 1 } }));
      // A touch context still reports hover:hover in headless Chromium, so
      // the query is forced. What is under test is the tap path, not
      // Chromium's idea of the device.
      const real = window.matchMedia.bind(window);
      window.matchMedia = (q) => (q === '(hover: none)'
        ? { matches: true, media: q, addEventListener() {}, addListener() {}, removeEventListener() {} }
        : real(q));
    });
    const tpage = await tctx.newPage();
    await tpage.route('**/*', (r) =>
      r.request().url().startsWith('http://localhost:' + PORT) ? r.continue() : r.abort());
    await tpage.goto('http://localhost:' + PORT + '/dashboard', { waitUntil: 'domcontentloaded' });
    await tpage.waitForSelector('#cyg-envbar-hit', { timeout: 15000 });
    await tpage.waitForTimeout(400);
    const th = () => tpage.evaluate(() => {
      const b = document.getElementById('cyg-envbar');
      return b ? b.getBoundingClientRect().height : null;
    });
    check('on a touch device the line still rests as a hairline', (await th()) === 2, await th());
    await tpage.tap('#cyg-envbar-hit');
    await tpage.waitForTimeout(200);
    check('and a tap opens it, because there is no hover to wait for', (await th()) === 22, await th());
    await tpage.tap('body', { position: { x: 400, y: 500 } });
    await tpage.waitForTimeout(200);
    check('a tap elsewhere closes it again', (await th()) === 2, await th());
    await tctx.close();
  }

  check('nothing threw along the way', errors.length === 0, errors.slice(0, 3).join(' | '));
  console.log('    (' + VIEWS.length + ' nav items on the rail, ' + hrefKeys.length + ' addressable)');

  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
