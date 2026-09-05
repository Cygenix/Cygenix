// tests/browser/brand-stream.smoke.js
//
// tests/brand-stream.test.js runs the module against a fake DOM and counts
// calls. This runs it in the real landing page in a real browser, because the
// things that matter about a background layer are things only a browser can
// tell you: whether it paints under the headline, whether a click on the call
// to action reaches the button, whether the loop really stops when the reader
// scrolls away, and whether a phone-width viewport grows a horizontal
// scrollbar. These are the acceptance checks the layer was delivered with.
//
// requestAnimationFrame is wrapped before any script runs, so the frame count
// is the page's own and "never called" is a number, not an inference.
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = 8399;
const EXE = '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const TYPES = {
  '.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml',
};

function serve() {
  return http.createServer((req, res) => {
    let p = decodeURIComponent(req.url.split('?')[0]);
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

function countFrames() {
  window.__raf = 0;
  const orig = window.requestAnimationFrame.bind(window);
  window.requestAnimationFrame = function (f) { window.__raf++; return orig(f); };
}

const wait = (ms) => new Promise((r) => setTimeout(r, ms));

async function open(browser, opts) {
  const ctx = await browser.newContext(Object.assign({ viewport: { width: 1440, height: 900 } }, opts || {}));
  await ctx.addInitScript(countFrames);
  await ctx.route('**/*', (route) => {
    if (/fonts\.g(oogleapis|static)\.com/.test(route.request().url())) return route.abort();
    return route.continue();
  });
  const page = await ctx.newPage();
  const problems = [];
  page.on('pageerror', (e) => problems.push('pageerror: ' + e.message));
  page.on('console', (m) => {
    if ((m.type() === 'error' || m.type() === 'warning') && !/fonts\.g|net::ERR_FAILED|Failed to load resource/.test(m.text())) {
      problems.push(m.type() + ': ' + m.text());
    }
  });
  await page.goto('http://localhost:' + PORT + '/index.html', { waitUntil: 'load' });
  await page.waitForSelector('canvas.brand-stream', { state: 'attached', timeout: 15000 });
  return { ctx, page, problems };
}

(async () => {
  const server = serve();
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  try {
    // ── The ordinary visit ─────────────────────────────────────────────
    {
      const { ctx, page, problems } = await open(browser);

      const order = await page.evaluate(() => Array.from(document.body.children).slice(0, 3).map((e) => e.className));
      check('the canvas sits between .brand-glow and .brand-grid, under the grid and under everything else',
        order.join(',') === 'brand-glow,brand-stream,brand-grid', order.join(','));

      const cs = await page.evaluate(() => {
        const s = getComputedStyle(document.querySelector('canvas.brand-stream'));
        const g = getComputedStyle(document.querySelector('.brand-grid'));
        return { pos: s.position, z: s.zIndex, pe: s.pointerEvents, op: s.opacity, gridZ: g.zIndex, gridOp: g.opacity,
          mask: s.maskImage || s.webkitMaskImage };
      });
      check('it is fixed, at z-index 0, inert, at the balanced opacity',
        cs.pos === 'fixed' && cs.z === '0' && cs.pe === 'none' && cs.op === '0.62', JSON.stringify(cs));
      check('the grid is still visible and paints on top (same layer, later in the DOM)',
        cs.gridZ === '0' && cs.gridOp === '0.65', JSON.stringify(cs));
      check('its edges are masked', /radial-gradient/.test(cs.mask || ''), cs.mask);

      // 1. Contrast: the colours of the headline and the buttons are what they
      // were, and the thing under the cursor at each of them is the text or
      // the button — not the canvas.
      const hero = await page.evaluate(() => {
        const at = (sel) => { const r = document.querySelector(sel).getBoundingClientRect(); const e = document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2); return e ? (e.tagName + '.' + e.className) : null; };
        return {
          h1: getComputedStyle(document.querySelector('.hero h1 .line1')).color,
          btn: getComputedStyle(document.querySelector('.hero-btns .btn-primary')).backgroundColor,
          btnText: getComputedStyle(document.querySelector('.hero-btns .btn-primary')).color,
          underH1: at('.hero h1 .line1'), underBtn: at('.hero-btns .btn-primary'), underBtn2: at('.hero-btns .btn-secondary'),
        };
      });
      check('the headline is still #f2f4f8 on black and the primary button still the accent',
        hero.h1 === 'rgb(242, 244, 248)' && hero.btn === 'rgb(74, 91, 214)' && hero.btnText === 'rgb(255, 255, 255)', JSON.stringify(hero));
      check('the beams pass BEHIND the headline and both buttons — the canvas is not what is under them',
        /SPAN\.line1/.test(hero.underH1) && /btn-primary/.test(hero.underBtn) && /btn-secondary/.test(hero.underBtn2), JSON.stringify(hero));

      // 3. Clicks. Playwright's actionability check refuses to click an element
      // another element would intercept, so a trial click IS the test.
      let intercepted = [];
      for (const sel of ['.hero-btns .btn-primary', '.hero-btns .btn-secondary', '.nav-links a[href="#platform"]', '.nav-links a[href="/pricing"]', '.nav-cta']) {
        try { await page.click(sel, { trial: true, timeout: 3000 }); } catch (e) { intercepted.push(sel + ': ' + e.message.split('\n')[0]); }
      }
      check('"Start free trial", the secondary button and the nav links are all clickable — the canvas swallows nothing',
        intercepted.length === 0, intercepted.join(' | '));

      // 5. The loop runs at the top and stops past the hero.
      const r0 = await page.evaluate(() => window.__raf);
      await wait(400);
      const r1 = await page.evaluate(() => window.__raf);
      check('the animation loop is running behind the hero', r1 > r0 && r0 > 0, r0 + ' → ' + r1);

      // The page scrolls smoothly (html{scroll-behavior:smooth}); a smooth
      // trip down a page this long takes longer than the waits below, so the
      // jumps are made instant. What is under test is the layer, not the scroll.
      await page.evaluate(() => window.scrollTo({ top: document.documentElement.scrollHeight, behavior: 'instant' }));
      await wait(1200);   // the 600ms fade plus the 700ms stop timer, with slack
      const bottom = await page.evaluate(() => ({ op: getComputedStyle(document.querySelector('canvas.brand-stream')).opacity, raf: window.__raf, y: window.scrollY }));
      await wait(400);
      const bottom2 = await page.evaluate(() => window.__raf);
      check('scrolled to the bottom, the layer is at opacity 0', bottom.op === '0', JSON.stringify(bottom));
      check('and the rAF loop has stopped — no new frames in 400ms', bottom2 === bottom.raf, bottom.raf + ' → ' + bottom2);

      await page.evaluate(() => window.scrollTo({ top: 0, behavior: 'instant' }));
      await wait(900);   // the 600ms fade back in, with slack
      const top = await page.evaluate(() => {
        const c = document.querySelector('canvas.brand-stream');
        return { inline: c.style.opacity, op: getComputedStyle(c).opacity, raf: window.__raf };
      });
      check('scrolling back up brings it back and restarts the loop',
        top.inline === '0.62' && Number(top.op) >= 0.6 && top.raf > bottom2, JSON.stringify(top));

      // 6. Resize 1440 → 375: rebuilt, not stretched, no horizontal scrollbar.
      await page.setViewportSize({ width: 375, height: 667 });
      await wait(400);   // the 150ms debounce, with slack
      const small = await page.evaluate(() => {
        const c = document.querySelector('canvas.brand-stream');
        const dpr = Math.min(window.devicePixelRatio || 1, 1.5);
        const r = c.getBoundingClientRect();
        // body{overflow-x:hidden} propagates to the viewport, so the page can
        // never show a horizontal scrollbar; the honest question is whether
        // THIS layer widens the document. Measure with and without it.
        const withCanvas = document.documentElement.scrollWidth;
        const parent = c.parentNode, next = c.nextSibling;
        parent.removeChild(c);
        const without = document.documentElement.scrollWidth;
        parent.insertBefore(c, next);
        return { w: c.width, h: c.height, cssW: c.style.width, cssH: c.style.height, dpr,
          right: r.right, inner: window.innerWidth, withCanvas, without,
          viewportOverflowX: getComputedStyle(document.body).overflowX,
          scrollbarHeight: window.innerHeight - document.documentElement.clientHeight };
      });
      check('at 375px the canvas is rebuilt at the new size, so nothing is stretched or blurred',
        small.w === Math.round(375 * small.dpr) && small.h === Math.round(667 * small.dpr) && small.cssW === '375px' && small.cssH === '667px',
        JSON.stringify(small));
      check('and no horizontal scrollbar appears: the layer stays inside the viewport and adds no width',
        small.right <= small.inner && small.withCanvas === small.without && small.viewportOverflowX === 'hidden' && small.scrollbarHeight === 0,
        JSON.stringify(small));

      // 8. Nothing in the console.
      check('no new console errors or warnings', problems.length === 0, problems.join(' | '));
      await ctx.close();
    }

    // ── prefers-reduced-motion: a static field, no loop ────────────────
    {
      const { ctx, page, problems } = await open(browser, { reducedMotion: 'reduce' });
      await wait(500);
      const rm = await page.evaluate(() => {
        const c = document.querySelector('canvas.brand-stream');
        // The module's own context; a second getContext('2d') returns the same one.
        const d = c.getContext('2d').getImageData(0, 0, c.width, c.height).data;
        let lit = 0;
        for (let i = 3; i < d.length; i += 4 * 97) if (d[i] > 0) lit++;
        return { raf: window.__raf, op: getComputedStyle(c).opacity, lit, transition: getComputedStyle(c).transitionProperty };
      });
      check('with reduced motion the page shows a static field of beams', rm.lit > 0, JSON.stringify(rm));
      check('at the dimmer opacity, with no fade transition', rm.op === '0.32' && /none/.test(rm.transition), JSON.stringify(rm));
      check('and requestAnimationFrame is never called', rm.raf === 0, rm.raf);
      check('no console errors there either', problems.length === 0, problems.join(' | '));
      await ctx.close();
    }
  } finally {
    await browser.close();
    server.close();
  }
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})();
