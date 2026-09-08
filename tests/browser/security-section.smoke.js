// tests/browser/security-section.smoke.js
//
// tests/security-section.test.js reads the markup. This checks the four things
// only a browser can answer, all of them on the brief's acceptance list:
//
//   * the section does not widen the page — it holds two tables wider than a
//     phone, and a table that pushes the document sideways is the classic way
//     a section like this ships broken and nobody notices on a desktop;
//   * the anchor resolves when arriving from another route, not just from the
//     top of the homepage — the nav mixes anchors and paths, and /#security
//     from /pricing is the journey a reviewer actually takes;
//   * the scrollable table box can be reached and driven by keyboard;
//   * the status pills are legible rather than colour-only, which the static
//     test can assert about markup but not about what renders.
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = 8400;
const EXE = '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.txt': 'text/plain; charset=utf-8' };

function serve() {
  return http.createServer((req, res) => {
    let p = decodeURIComponent(req.url.split('?')[0].split('#')[0]);
    if (p === '/') p = '/index.html';
    let file = path.join(PUB, p);
    if (!fs.existsSync(file) && fs.existsSync(file + '.html')) file += '.html';
    if (!fs.existsSync(file) || fs.statSync(file).isDirectory()) {
      res.writeHead(404, { 'Content-Type': TYPES['.html'] }); return res.end('not found');
    }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream' });
    fs.createReadStream(file).pipe(res);
  }).listen(PORT);
}

(async () => {
  const server = serve();
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await ctx.route('**/*', (r) => (/fonts\.g(oogleapis|static)\.com/.test(r.request().url()) ? r.abort() : r.continue()));
    const page = await ctx.newPage();
    const problems = [];
    page.on('pageerror', (e) => problems.push(e.message));

    await page.goto('http://localhost:' + PORT + '/index.html', { waitUntil: 'load' });
    await page.waitForSelector('#security', { timeout: 15000 });

    check('the section renders', await page.locator('#security').isVisible());
    const heads = await page.evaluate(() => Array.from(document.querySelectorAll('#security h2, #security h3'))
      .map((e) => e.tagName + ':' + e.textContent.trim()));
    check('one h2 and six h3s, in order',
      heads.length === 7 && heads[0].startsWith('H2') && heads.slice(1).every((h) => h.startsWith('H3')),
      heads.join(' | '));

    // Text that renders as the same colour as its background would pass a
    // markup test and be invisible here.
    const pills = await page.evaluate(() => Array.from(document.querySelectorAll('#security .pill'))
      .map((p) => ({ t: p.textContent.trim(), c: getComputedStyle(p).color, bg: getComputedStyle(p).backgroundColor })));
    check('every status pill renders a word in a colour distinct from its own background',
      pills.length >= 10 && pills.every((p) => p.t.length > 2 && p.c !== p.bg), JSON.stringify(pills.slice(0, 2)));

    // No horizontal overflow, desktop and phone. body{overflow-x:hidden} would
    // mask a scrollbar, so the honest measure is whether the section itself is
    // wider than the viewport.
    for (const [w, hgt] of [[1440, 900], [375, 667]]) {
      await page.setViewportSize({ width: w, height: hgt });
      await page.waitForTimeout(120);
      const m = await page.evaluate(() => {
        const s = document.getElementById('security');
        const r = s.getBoundingClientRect();
        const t = s.querySelector('.sec-scroll');
        return { right: Math.round(r.right), width: Math.round(r.width), inner: window.innerWidth,
          scrollerFits: t ? t.getBoundingClientRect().width <= window.innerWidth : false,
          tableWider: t ? t.querySelector('table').scrollWidth > t.clientWidth : null };
      });
      check('at ' + w + 'px the section stays inside the viewport',
        m.right <= m.inner + 1 && m.width <= m.inner, JSON.stringify(m));
      check('at ' + w + 'px the wide table is contained by its own scroller', m.scrollerFits, JSON.stringify(m));
    }
    // At 375 the table is genuinely wider than the box, which is what the
    // scroller is for — and it must be operable, not just present.
    const scrollable = await page.evaluate(() => {
      const t = document.querySelector('#security .sec-scroll');
      const before = t.scrollLeft;
      t.scrollLeft = 120;
      const after = t.scrollLeft;
      t.scrollLeft = before;
      return { moved: after > before, tabbable: t.tabIndex >= 0 };
    });
    check('the table scroller actually scrolls on a phone, and is keyboard-reachable',
      scrollable.moved && scrollable.tabbable, JSON.stringify(scrollable));

    await page.setViewportSize({ width: 1440, height: 900 });

    // The journey a reviewer takes: they are on /pricing and click Security.
    await page.goto('http://localhost:' + PORT + '/pricing.html', { waitUntil: 'load' });
    await page.goto('http://localhost:' + PORT + '/index.html#security', { waitUntil: 'load' });
    // The page sets html{scroll-behavior:smooth}, and #security is ~7000px
    // down, so the jump is an animation rather than an instant seek. Wait for
    // the position to stop moving instead of guessing a duration — a fixed
    // wait here measured the scroll mid-flight and read as a failure.
    await page.waitForFunction(() => {
      const y = Math.round(window.scrollY);
      const settled = window.__lastY === y;
      window.__lastY = y;
      return settled && y > 0;
    }, null, { timeout: 15000, polling: 250 });
    const landed = await page.evaluate(() => {
      const r = document.getElementById('security').getBoundingClientRect();
      return { top: Math.round(r.top), scrolled: Math.round(window.scrollY) };
    });
    check('arriving at /#security from another route lands on the section',
      landed.scrolled > 200 && Math.abs(landed.top) < 200, JSON.stringify(landed));

    const anchorHref = await page.evaluate(() => {
      const a = document.querySelector('.nav-links a[href="#security"]');
      return a ? { text: a.textContent.trim(), href: a.getAttribute('href') } : null;
    });
    check('the nav item is there and points at the section',
      anchorHref && anchorHref.text === 'Security' && anchorHref.href === '#security', JSON.stringify(anchorHref));

    check('security.txt is served at its well-known address',
      (await (await page.request.get('http://localhost:' + PORT + '/.well-known/security.txt')).text()).includes('Contact: mailto:security@cygenix.co.uk'));

    check('no console errors', problems.length === 0, problems.join(' | '));
    await ctx.close();
  } finally {
    await browser.close();
    server.close();
  }
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})();
