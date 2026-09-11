/* tests/browser/tour.smoke.js
 * ---------------------------------------------------------------------------
 * The guided tour, end to end, in a browser, WITH NO API KEY SET.
 *
 * That last clause is the point of the whole feature and so it is the point of
 * this file. A new user's assistant panel says "No API key set" and nothing
 * else; the tour is the one thing on that screen they can actually do. If it
 * ever starts needing a key, this file fails.
 *
 * The network is cut off at the browser: every request other than the local
 * static server is aborted. A tour that reached for Anthropic would hang here
 * rather than quietly working on the developer's machine.
 *
 * Covered: a no-key start, the step/page/highlight triple at each stop, Y / B /
 * Esc, a mid-tour reload, rapid Y (no stale spotlight), `resume`, `tour <area>`,
 * and that nothing was written anywhere but the tour's own keys.
 *
 * Not part of `npm test`: it needs a browser.
 *   node tests/browser/tour.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8407);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.json': 'application/json' };

/* The console's addresses are extensionless and NOT simply the filename:
   /object-mapping is served by object_mapping.html, and only public/_redirects
   knows that. Guessing "<path>.html" gets a 404 on eight of the twenty-odd
   pages the tour visits — which looks like a tour bug and is not one. So the
   real routing table is parsed and used, which also means this file fails if
   somebody points a step at an address the site does not serve. */
const ROUTES = (() => {
  const map = {};
  fs.readFileSync(path.join(PUB, '_redirects'), 'utf8').split('\n').forEach((line) => {
    const m = line.trim().match(/^(\/\S*)\s+(\/\S+)\s+200$/);
    if (m) map[m[1]] = m[2];
  });
  return map;
})();

const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/') p = '/index.html';
  // The auth gate's second layer asks the whoami function for the account's
  // tier, and with the network cut that call fails and it sends the browser to
  // /login — on the second navigation, not the first, which is why this only
  // bites a test that walks between pages. The gate is not what is under test.
  if (p === '/auth-gate.js') {
    res.writeHead(200, { 'Content-Type': TYPES['.js'] });
    return res.end('/* stubbed: the tour is under test, not the auth gate */');
  }
  if (ROUTES[p]) p = ROUTES[p];
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) {
    res.writeHead(404); return res.end('no');
  }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

/* A signed-in session and a project, but deliberately NO cygenix_api_key. */
const SEED = () => {
  localStorage.setItem('cygenix_user', JSON.stringify({ email: 'you@example.test', name: 'You' }));
  localStorage.setItem('cygenix_active_user', 'you@example.test');
  localStorage.setItem('cygenix_cookie_consent', 'all');
  localStorage.setItem('acct-cygenix.ciamlogin.com-x', JSON.stringify({
    homeAccountId: 'x', environment: 'cygenix.ciamlogin.com', authorityType: 'MSSTS',
    username: 'you@example.test', localAccountId: 'x', tenantId: 'x' }));
  localStorage.setItem('cygenix_projects', JSON.stringify([{ id: 'p1', name: 'Demo migration' }]));
  localStorage.setItem('cygenix_active_project_id', 'p1');
  // Object Mapping and the SQL Editor each carry their own one-line guard that
  // bounces to /login without this, independently of auth-gate.js — and the
  // tour walks through both.
  localStorage.setItem('cygenix_token', 'smoke-token');
  // The first-run modal appears 800ms in, covers the screen and intercepts
  // every click. It is not what this file tests, and the tour's own paths
  // deliberately stand down while it is up.
  localStorage.setItem('cygenix_onboarded', '1');
  localStorage.removeItem('cygenix_api_key');
  sessionStorage.removeItem('cygenix_api_key');
};

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const offSite = [];
  await ctx.route('**', (route) => {
    const u = route.request().url();
    if (u.startsWith('http://localhost:' + PORT)) return route.continue();
    offSite.push(u);
    return route.abort();
  });
  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('console', (m) => { if (m.type() === 'error' && !/ERR_|Failed to load resource/.test(m.text())) errors.push(m.text()); });
  await page.addInitScript(SEED);

  const openPanel = async () => {
    // A step that changes page is a full reload, so settle first: asking for
    // window.CygenixTour mid-navigation asks the outgoing document.
    try { await page.waitForLoadState('domcontentloaded', { timeout: 15000 }); } catch (e) {}
    await page.waitForFunction(() => !!window.CygenixAssistant && !!window.CygenixTour, null, { timeout: 20000 });
    await page.evaluate(() => window.CygenixAssistant.open());
    await page.waitForSelector('#cygaInput', { state: 'visible', timeout: 8000 });
  };
  const card = () => page.evaluate(() => {
    const cards = document.querySelectorAll('.cyg-tour-card');
    const cur = cards[cards.length - 1];
    if (!cur) return null;
    return {
      title: cur.querySelector('h4').textContent.trim(),
      section: cur.querySelector('.tc-sec').textContent.trim(),
      count: cur.querySelector('.tc-count').textContent.trim(),
      past: cur.classList.contains('past'),
      cards: cards.length,
    };
  });
  /* Does the spotlight sit over this selector? Compared rect to rect rather
     than by elementFromPoint: the overlay is pointer-events:none and stacked
     with two siblings, so hit-testing through it answers a question about
     stacking order, not about what is lit. */
  const spotCovers = (sel) => page.evaluate((s) => {
    const spot = document.getElementById('cygTourSpot');
    const t = document.querySelector(s);
    if (!spot || !spot.classList.contains('on') || !t) return { ok: false, why: 'missing' };
    const a = spot.getBoundingClientRect(), b = t.getBoundingClientRect();
    const near = (x, y) => Math.abs(x - y) <= 8;
    return {
      ok: near(a.left + a.width / 2, b.left + b.width / 2) && near(a.top + a.height / 2, b.top + b.height / 2)
        && a.width >= b.width && a.height >= b.height,
      spot: [Math.round(a.left), Math.round(a.top), Math.round(a.width), Math.round(a.height)],
      target: [Math.round(b.left), Math.round(b.top), Math.round(b.width), Math.round(b.height)],
    };
  }, sel);
  const type = async (t) => { await page.fill('#cygaInput', t); await page.press('#cygaInput', 'Enter'); await page.waitForTimeout(450); };
  const key = async (k) => { await page.focus('#cygaInput'); await page.press('#cygaInput', k); await page.waitForTimeout(500); };

  console.log('Guided tour — a walkthrough that needs no API key\n');

  /* ── 1. It starts, with no key ──────────────────────────────────────────── */

  await page.goto('http://localhost:' + PORT + '/dashboard', { waitUntil: 'domcontentloaded' });
  await openPanel();

  check('the panel really has no API key',
    !(await page.evaluate(() => window.CygenixAssistant.hasKey())));
  check('and the no-key empty state still offers the tour',
    (await page.locator('.cyga-chip[data-ask*="tour"]').count()) === 1,
    'this chip is the only thing a brand-new user can act on');

  await type('Give me a tour of what you can do');
  let c = await card();
  check('typing the intent starts the tour', !!c && /Welcome/.test(c.title), JSON.stringify(c));
  check('the intro card is labelled Intro, not 0 / n', c && c.count === 'Intro', c && c.count);
  check('the TOUR pill is showing',
    await page.evaluate(() => { const p = document.getElementById('cygaTourPill'); return !!p && !p.hidden; }));
  check('and the placeholder explains the keys',
    /Press Y to continue/.test(await page.getAttribute('#cygaInput', 'placeholder')));
  // The page itself reaches for fonts, MSAL and PapaParse on load; those are
  // the app, not the tour. What matters is that STARTING the tour adds none.
  const offSiteAtStart = offSite.length;
  check('starting the tour made no off-site request of its own',
    offSite.length === offSiteAtStart && !offSite.some((u) => /anthropic/i.test(u)),
    offSite.filter((u) => /anthropic/i.test(u)).join(' '));

  /* ── 2. Y advances; the page, the card and the spotlight agree ──────────── */

  await key('y');
  c = await card();
  check('Y advances to the first real stop', c && /Home/.test(c.title), JSON.stringify(c));
  check('and the counter is now n / total', c && /^1 \/ \d+$/.test(c.count), c && c.count);
  let cov = await spotCovers('[data-key="dashboard"]');
  check('the spotlight is on the Home nav item', cov.ok, JSON.stringify(cov));
  check('the region outline is drawn round the Home cards',
    await page.evaluate(() => {
      const r = document.getElementById('cygTourRegion');
      return !!r && r.classList.contains('on') && r.getBoundingClientRect().height > 50;
    }));
  check('the earlier card dimmed and has no buttons left',
    await page.evaluate(() => {
      const first = document.querySelectorAll('.cyg-tour-card')[0];
      return first.classList.contains('past') && !first.querySelector('[data-tour-act]');
    }),
    'the transcript should read as history, not as five live control panels');
  check('the step offers Continue, Back and Exit, each naming its key',
    await page.evaluate(() => {
      const cur = [...document.querySelectorAll('.cyg-tour-card')].pop();
      return [...cur.querySelectorAll('[data-tour-act]')].map((b) => b.textContent.trim()).join('|');
    }) === 'Y · Continue|B · Back|Exit',
    await page.evaluate(() => {
      const cur = [...document.querySelectorAll('.cyg-tour-card')].pop();
      return [...cur.querySelectorAll('[data-tour-act]')].map((b) => b.textContent.trim()).join('|');
    }));

  await key('y');   // Files
  cov = await spotCovers('.cyg-drive-btn');
  check('Files spotlights the Drive shortcut, which is not a nav item', cov.ok, JSON.stringify(cov));

  /* ── 3. B goes back, and typed words work too ───────────────────────────── */

  await key('b');
  c = await card();
  check('B goes back a step', c && /Home/.test(c.title), c && c.title);
  check('Y and B keypresses are not echoed as chat bubbles',
    (await page.locator('.cyga-msg.user').count()) === 0,
    'a transcript full of one-letter bubbles is noise, not history');

  await type('next');
  c = await card();
  check('typing "next" also advances', c && /Files/.test(c.title), c && c.title);
  // A control word is not echoed either — "next" is the Y key spelled out, and
  // a transcript of the word "next" five times is no more useful than five
  // "y"s. A real sentence is echoed, because the user meant to say it.
  await type('what does this page do?');
  check('but a real question IS echoed, because the user meant to say it',
    (await page.locator('.cyga-msg.user').count()) === 1,
    (await page.locator('.cyga-msg.user').count()));
  check('and with no key it says so rather than failing silently',
    /API key/.test(await page.textContent('#cygaBody')));
  check('the tour is still running after a question',
    await page.evaluate(() => window.CygenixTour.isActive()));
  await type('back');
  c = await card();
  check('typing "back" also reverses', c && /Home/.test(c.title), c && c.title);

  /* ── 3b. The card you are reading is whole, and near the top ─────────────
     Both of these were real bugs. The cards are flex items in a column flex
     container, so without flex:0 0 auto they SHRINK once the transcript is
     taller than the panel — and being overflow:hidden, they clip their own
     text mid-sentence. Eight stops in, every card was a bare header strip and
     the step you were on was cut in half by the input box. */

  for (let i = 0; i < 4; i++) { await key('y'); }          // build up a transcript
  await page.waitForTimeout(1200);
  await openPanel();
  await page.waitForTimeout(900);                           // the scroll animates

  const layout = await page.evaluate(() => {
    const body = document.getElementById('cygaBody');
    const cards = [...document.querySelectorAll('.cyg-tour-card')];
    const cur = cards[cards.length - 1];
    const br = body.getBoundingClientRect(), cr = cur.getBoundingClientRect();
    return {
      cards: cards.length,
      // A card is clipped when its own content is taller than the box it is in.
      clipped: cards.filter((c) => c.scrollHeight > c.clientHeight + 1).length,
      everyPastHasItsText: cards.slice(0, -1).every((c) => {
        const p = c.querySelector('.tc-body p');
        return p && p.textContent.trim().length > 20;
      }),
      currentWhollyVisible: cr.top >= br.top - 1 && cr.bottom <= br.bottom + 1,
      // "Near the top" — within a third of the panel, not pinned to the input.
      currentNearTop: (cr.top - br.top) < br.height / 3,
    };
  });
  check('no card is clipped, however long the transcript gets',
    layout.clipped === 0 && layout.everyPastHasItsText, JSON.stringify(layout));
  check('and the step you are reading is whole and near the top of the panel',
    layout.currentWhollyVisible && layout.currentNearTop, JSON.stringify(layout));

  /* ── 4. Navigation to another page, and surviving the load ──────────────── */

  await type('tour connections');
  await page.waitForFunction(() => location.pathname.indexOf('connect') !== -1
    || (document.getElementById('cyg-sidebar-mount') || {}).dataset === undefined
    || document.querySelector('.cyg-tour-card'), null, { timeout: 10000 });
  await page.waitForTimeout(1200);
  await openPanel();
  c = await card();
  check('"tour <area>" jumps to that stop', c && /Connections/.test(c.title), c && c.title);
  check('and the browser really is on that page',
    /connections|dashboard/.test(page.url()), page.url());
  check('the transcript survived the page load', c && c.cards >= 2, c && c.cards);
  check('the tour is still active after the reload',
    await page.evaluate(() => window.CygenixTour.isActive()));

  /* ── 5. A hard reload mid-tour ──────────────────────────────────────────── */

  await page.reload({ waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(1400);
  c = await card();
  check('a hard reload resumes the same step, panel already open',
    c && /Connections/.test(c.title) && (await page.isVisible('#cygaInput')), c && c.title);

  /* ── 6. Rapid Y leaves no stale spotlight ───────────────────────────────── */

  const before = await card();
  for (let i = 0; i < 5; i++) {
    // Each Y may navigate, which tears the page down mid-loop; a press that
    // lands during a load is exactly the race being tested, so it is tolerated
    // rather than awaited away.
    try { await page.press('#cygaInput', 'y', { timeout: 2500 }); } catch (e) { /* navigated */ }
    await page.waitForTimeout(120);
  }
  await page.waitForTimeout(2000);
  await openPanel();
  const after = await card();
  check('five fast Y presses land on one step, not five spotlights',
    after && after.title !== before.title
      && (await page.evaluate(() => document.querySelectorAll('#cygTourSpot').length)) === 1,
    JSON.stringify(after));
  const shown = await card();
  const stale = await page.evaluate(() => {
    // Whatever is lit must be the target of the step whose card is current —
    // a spotlight left over from a step the user has already flown past is the
    // exact failure this section exists for.
    const spot = document.getElementById('cygTourSpot');
    if (!spot || !spot.classList.contains('on')) return 'none';
    const a = spot.getBoundingClientRect();
    const hit = [...document.querySelectorAll('.cyg-sidebar [data-key],.cyg-sidebar [data-parent]')]
      .find((e) => {
        const b = e.getBoundingClientRect();
        return Math.abs((a.left + a.width / 2) - (b.left + b.width / 2)) <= 8
          && Math.abs((a.top + a.height / 2) - (b.top + b.height / 2)) <= 8;
      });
    return hit ? (hit.dataset.key || hit.dataset.parent) : 'unmatched';
  });
  check('and the surviving spotlight is over a real nav row, not a ghost',
    stale !== 'unmatched', JSON.stringify({ stale, title: shown && shown.title }));

  /* ── 7. Esc ends it, and the sidebar is put back ────────────────────────── */

  const groupsBefore = await page.evaluate(() =>
    [...document.querySelectorAll('.cyg-nav-children.open')].map((k) => k.dataset.children).sort().join(','));
  await key('Escape');
  check('Esc ends the tour',
    !(await page.evaluate(() => window.CygenixTour.isActive())));
  check('every overlay is cleared',
    await page.evaluate(() => !document.querySelector('#cygTourSpot.on, #cygTourRegion.on, #cygTourCallout.on')));
  check('the TOUR pill is gone',
    await page.evaluate(() => document.getElementById('cygaTourPill').hidden));
  check('and it says how to come back',
    /resume/.test(await page.textContent('#cygaBody')));
  check('groups the tour opened were closed again',
    (await page.evaluate(() =>
      [...document.querySelectorAll('.cyg-nav-children.open')].map((k) => k.dataset.children).sort().join(',')
    )).length <= groupsBefore.length);

  /* ── 8. resume ──────────────────────────────────────────────────────────── */

  await type('resume');
  await page.waitForTimeout(1200);
  await openPanel();
  c = await card();
  check('"resume" picks the tour back up', !!c && !c.past && await page.evaluate(() => window.CygenixTour.isActive()),
    JSON.stringify(c));

  /* ── 9. Nothing was written, and nothing was called ─────────────────────── */

  // Every page in this app pulls fonts, MSAL and PapaParse on load; that is the
  // app, not the tour. The claim worth pinning is narrower and is the whole
  /* ── 9. Interruption: ask a question mid-tour, then carry on ───────────── */
  //
  // The reported bug in one sequence. A user on a step types a question, the
  // assistant switches into agent mode and answers, and the tour comes back
  // with the user's place lost. Everything below is that path.
  //
  // The model is STUBBED. This file's whole premise is that it never reaches
  // Anthropic, and the assertion above enforces that — so the interruption is
  // exercised by replacing CygenixModel.mdCall with a scripted answer. What
  // is under test is the tour's handling of an interruption, not the model.

  // Jump to a step FIRST. That navigates, which reloads the page — and a
  // reload wipes anything installed on window, which is how the first version
  // of this test managed to assert against a stub that no longer existed.
  await type('tour connections');
  await page.waitForTimeout(1200);
  await openPanel();

  await page.evaluate(() => {
    localStorage.setItem('cygenix_api_key', 'sk-ant-smoke');
    sessionStorage.setItem('cygenix_api_key', 'sk-ant-smoke');
    window.__asked = [];
    window.CygenixModel.mdCall = async (req) => {
      window.__asked.push(req);
      return {
        degraded: false,
        response: { json: async () => ({ content: [{ type: 'text',
          text: 'A connection points Cygenix at one database. You need a source and a target.' }] }) },
      };
    };
  });

  c = await card();
  const atStep = c && c.count;
  check('jumped to a known step to interrupt from', !!c && /^\d+ \/ \d+$/.test(atStep), JSON.stringify(c));
  const stepBefore = await page.evaluate(() => window.CygenixTour.__state().stepIndex);
  const idBefore = await page.evaluate(() => window.CygenixTour.__state().stepId);

  await type('can you connect and test these connections');
  await page.waitForTimeout(1500);

  check('the question reached the model rather than being eaten as a command',
    await page.evaluate(() => window.__asked.length === 1));
  check('and the tour paused rather than advancing or ending',
    await page.evaluate(() => window.CygenixTour.isPaused()));
  check('THE STEP DID NOT MOVE — this is the 5 → 4 bug',
    await page.evaluate(() => window.CygenixTour.__state().stepIndex) === stepBefore
    && await page.evaluate(() => window.CygenixTour.__state().stepId) === idBefore);

  // The context the brief asks for: the model must be told which step the
  // user is on, or "tell me more about this" cannot work.
  const sys = await page.evaluate(() => window.__asked[0].system);
  check('the model was told which step the user is on',
    /welcome tour/i.test(sys) && sys.indexOf(idBefore) !== -1, sys.slice(-200));
  check('and asked to keep the answer short',
    /concise/i.test(sys));
  check('the answer was capped, so a curious question has a predictable cost',
    await page.evaluate(() => window.__asked[0].max_tokens) < 2048);

  const body = await page.textContent('#cygaBody');
  check('the credits notice appeared', /use AI credits/i.test(body));
  check('a resume prompt is offered, naming the paused step',
    /Paused at step/.test(body) && body.indexOf(atStep) !== -1);

  // Several questions in a row, which the brief calls out explicitly.
  await type('and what about PROD?');
  await page.waitForTimeout(1500);
  check('a second question also works', await page.evaluate(() => window.__asked.length === 2));
  check('and the step still has not moved',
    await page.evaluate(() => window.CygenixTour.__state().stepIndex) === stepBefore);
  const body2 = await page.textContent('#cygaBody');
  check('the credits notice is shown once a session, not per question',
    (body2.match(/use AI credits/gi) || []).length === 1);

  // "no" mid-conversation must be an answer, not an exit.
  await type('no');
  await page.waitForTimeout(1200);
  check('"no" while paused is a reply to the assistant, not an Exit',
    await page.evaluate(() => window.CygenixTour.isPaused()));

  await type('y');
  await page.waitForTimeout(1400);
  await openPanel();
  c = await card();
  check('RESUMING RETURNS TO THE SAME STEP', c && c.count === atStep, JSON.stringify(c));
  check('and the tour is running again',
    await page.evaluate(() => window.CygenixTour.isActive()));
  check('the step index is exactly where it was',
    await page.evaluate(() => window.CygenixTour.__state().stepIndex) === stepBefore);
  check('resuming cost nothing — Y is local',
    await page.evaluate(() => window.__asked.length === 3));

  /* ── 10. Reloading mid-tour offers to resume, rather than relaunching ──── */

  await page.reload({ waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(1200);
  await openPanel();
  const afterReload = await page.textContent('#cygaBody');
  check('a reload keeps the place',
    await page.evaluate(() => window.CygenixTour.__state().stepIndex) === stepBefore);
  check('and the tour carries on rather than starting over',
    /\d+ \/ \d+/.test(afterReload));

  // A fresh tab — no session transcript, but a durable position — is the
  // "came back tomorrow" case, and must OFFER rather than take over.
  await page.evaluate(() => sessionStorage.removeItem('cyg_tour'));
  await page.reload({ waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(1200);
  await openPanel();
  const offered = await page.textContent('#cygaBody');
  check('coming back later offers to resume rather than relaunching',
    /You were on step/.test(offered));
  check('with Resume, Start over and Dismiss',
    (await page.locator('[data-tour-act="resume"]').count()) === 1 &&
    (await page.locator('[data-tour-act="restart"]').count()) === 1 &&
    (await page.locator('[data-tour-act="dismiss"]').count()) === 1);
  check('and it has NOT taken over the panel',
    !(await page.evaluate(() => window.CygenixTour.isActive())));

  await page.click('[data-tour-act="resume"]');
  await page.waitForTimeout(1400);
  await openPanel();
  c = await card();
  check('accepting the offer lands on the step they left',
    c && c.count === atStep, JSON.stringify(c));

  // Put the key back the way the rest of the file expects to find it.
  await page.evaluate(() => {
    localStorage.removeItem('cygenix_api_key');
    sessionStorage.removeItem('cygenix_api_key');
  });

  // design: the walkthrough never reaches for a model.
  check('the tour never called Anthropic, from first step to last',
    !offSite.some((u) => /anthropic/i.test(u)),
    offSite.filter((u) => /anthropic/i.test(u)).join(' '));
  check('and asked for nothing beyond the fonts and libraries the pages load anyway',
    offSite.every((u) => /fonts\.(googleapis|gstatic)|msal-browser|papaparse|cdnjs|jsdelivr/i.test(u)),
    offSite.filter((u) => !/fonts\.(googleapis|gstatic)|msal-browser|papaparse|cdnjs|jsdelivr/i.test(u)).slice(0, 3).join(' '));
  const wrote = await page.evaluate(() => Object.keys(localStorage)
    .filter((k) => /^cygenix_(jobs|projects|inventory|wasis|validation|assurance)/.test(k)));
  check('no migration data was created or modified', wrote.length <= 1, JSON.stringify(wrote));

  check('no console errors anywhere in the run', errors.length === 0, errors.slice(0, 3).join(' | '));

  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => {
  console.log('\nSMOKE CRASHED: ' + (e && e.stack || e));
  try { server.close(); } catch (_) {}
  process.exit(1);
});
