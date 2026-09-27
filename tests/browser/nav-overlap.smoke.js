/* tests/browser/nav-overlap.smoke.js
 * ---------------------------------------------------------------------------
 * Nothing shows through the transparent menu bar on the homepage or pricing.
 *
 * THE BUG THIS EXISTS FOR
 * Both pages keep the fixed menu bar transparent over the hero and make it
 * solid "once the hero has scrolled away". The rule was
 *     scrollY > hero.offsetHeight - 80
 * — about 600px. But the hero's first line (the badge) sits only ~80px
 * below the bar, so for the 500px in between, the badge, the headline and,
 * on pricing, the Monthly / Annual toggle slid under a transparent bar and
 * showed through the menu links. Noticed on pricing; the homepage had the
 * identical rule and the identical overlap.
 *
 * The rule is now "solid the moment the hero's first child meets the bar's
 * bottom edge". This file does not check the rule. It checks the promise:
 * at every scroll position through the top of each page, at desktop and
 * phone width, EITHER the bar is solid OR nothing from the page is behind
 * it. And at the very top it is still transparent, which is the design.
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/nav-overlap.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8431);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.woff2': 'font/woff2' };

const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/') p = '/index.html';
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end('no'); }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

const wait = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });

  console.log('Menu bar — does anything show through it?\n');

  for (const [page, heroSel] of [['index.html', '.hero'], ['pricing.html', '.pricing-hero']]) {
    for (const vp of [{ width: 1440, height: 900 }, { width: 375, height: 667 }]) {
      const label = page.replace('.html', '') + ' @' + vp.width;
      const ctx = await browser.newContext({ viewport: vp });
      await ctx.route('**/*', (r) => (r.request().url().startsWith('http://localhost:' + PORT) ? r.continue() : r.abort()));
      await ctx.addInitScript(() => {
        try { localStorage.setItem('cygenix_cookie_consent', JSON.stringify({ version: '2', essential: true, functional: true, analytics: false })); } catch (e) { /* none */ }
      });
      const tab = await ctx.newPage();
      const errors = [];
      tab.on('pageerror', (e) => errors.push(e.message));
      await tab.goto('http://localhost:' + PORT + '/' + page, { waitUntil: 'load' });
      await wait(400);

      const top = await tab.evaluate(() => ({ solid: document.querySelector('nav').classList.contains('solid'),
        bg: getComputedStyle(document.querySelector('nav')).backgroundColor }));
      check(label + ': at the very top the bar is still transparent — the design is unchanged',
        !top.solid && top.bg === 'rgba(0, 0, 0, 0)', JSON.stringify(top));

      // Walk the first 900px in 10px steps. At each: is anything in the
      // page (other than the fixed chrome) intersecting the bar while it is
      // still transparent?
      const bad = [];
      let flippedAt = null;
      for (let y = 0; y <= 900; y += 10) {
        // The browser delivers the scroll event on the next frame, and a
        // real scroll updates the bar before that frame is painted. Reading
        // in the same instant as scrollTo() would measure the bar one frame
        // before the page has been told — so wait for two frames first.
        await tab.evaluate((v) => new Promise((done) => {
          window.scrollTo({ top: v, behavior: 'instant' });
          requestAnimationFrame(() => requestAnimationFrame(done));
        }), y);
        const r = await tab.evaluate((sel) => {
          const nav = document.querySelector('nav');
          const nb = nav.getBoundingClientRect().bottom;
          const solid = nav.classList.contains('solid');
          if (solid) return { solid };
          const hero = document.querySelector(sel);
          // Visible things only: text, controls, anything with a box of its
          // own. Sampled along the bar's full width at its lower edge.
          const hits = [];
          for (let x = 8; x < innerWidth; x += 24) {
            for (const e of document.elementsFromPoint(x, nb - 2)) {
              if (e.closest('nav') || /^(HTML|BODY)$/.test(e.tagName)) continue;
              if (e.closest('.cx-mesh-layer,.brand-glow,.brand-grid')) continue;
              if (e === hero || e.matches('section, .section, .container, .pricing-grid')) continue;
              hits.push(e.tagName + '.' + (e.className || '').toString().split(' ')[0]);
              break;
            }
            if (hits.length) break;
          }
          return { solid, hit: hits[0] || null };
        }, heroSel);
        if (r.solid && flippedAt === null) flippedAt = y;
        if (!r.solid && r.hit) bad.push(y + 'px:' + r.hit);
      }
      check(label + ': NOTHING SHOWS THROUGH THE TRANSPARENT BAR at any scroll position',
        bad.length === 0, bad.slice(0, 4).join(', '));
      check(label + ': and it turns solid early — within the first screen, not after the whole hero',
        flippedAt !== null && flippedAt <= 200, 'solid from ' + flippedAt + 'px');

      // Back to the top: transparent again.
      await tab.evaluate(() => window.scrollTo({ top: 0, behavior: 'instant' }));
      await wait(100);
      check(label + ': scrolling back to the top makes it transparent again',
        !(await tab.evaluate(() => document.querySelector('nav').classList.contains('solid'))));
      check(label + ': no page errors', errors.length === 0, errors.join(' | '));
      await ctx.close();
    }
  }

  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
