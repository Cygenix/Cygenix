// tests/browser/hero-mesh.smoke.js
//
// tests/hero-mesh.test.js runs the engine against a fake DOM and counts
// calls. This runs it in the real landing page in a real browser, because
// what matters about a background layer only a browser can tell you: whether
// the hero is exactly where and how big it was before the layer existed,
// whether the copy is in front of the canvas, whether a click on the call to
// action reaches the button, whether the loop really stops once the hero is
// scrolled away, and whether a phone-width viewport grows a scrollbar.
// These are the brief's acceptance checks.
//
// requestAnimationFrame is wrapped before any script runs, so the frame
// count is the page's own and "never scheduled" is a number.
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
  window.__raf = 0; window.__frameMs = [];
  const orig = window.requestAnimationFrame.bind(window);
  window.requestAnimationFrame = function (f) {
    window.__raf++;
    return orig(function (t) { const a = performance.now(); f(t); window.__frameMs.push(performance.now() - a); });
  };
}

const wait = (ms) => new Promise((r) => setTimeout(r, ms));

async function open(browser, opts) {
  const ctx = await browser.newContext(Object.assign({ viewport: { width: 1440, height: 900 } }, opts || {}));
  await ctx.addInitScript(countFrames);
  const requests = [];
  await ctx.route('**/*', (route) => {
    const url = route.request().url();
    requests.push(url);
    if (/fonts\.g(oogleapis|static)\.com/.test(url)) return route.abort();
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
  await page.waitForFunction(() => !!window.cygenixHeroMesh, null, { timeout: 15000 });
  return { ctx, page, problems, requests };
}

(async () => {
  const server = serve();
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  try {
    // ── The ordinary visit ─────────────────────────────────────────────
    {
      const { ctx, page, problems, requests } = await open(browser);

      const tree = await page.evaluate(() => {
        const stage = document.querySelector('.hero-stage');
        return stage ? Array.from(stage.children).map((e) => e.tagName + '.' + e.className) : null;
      });
      check('the stage holds the mesh layer first and the hero after it',
        !!tree && tree.length === 2 && /DIV\.cx-mesh-layer/.test(tree[0]) && /SECTION\.hero/.test(tree[1]), JSON.stringify(tree));

      // 1. No layout shift: the hero and its headline are exactly where and
      // how big they are with the layer removed from layout.
      const geom = await page.evaluate(() => {
        const r = (sel) => { const b = document.querySelector(sel).getBoundingClientRect(); return [b.left, b.top, b.width, b.height].map((v) => Math.round(v * 10) / 10).join(','); };
        const layer = document.querySelector('.cx-mesh-layer');
        const shown = { hero: r('.hero'), h1: r('.hero h1'), btn: r('.hero-btns .btn-primary'), stage: r('.hero-stage') };
        layer.style.display = 'none';
        const hidden = { hero: r('.hero'), h1: r('.hero h1'), btn: r('.hero-btns .btn-primary'), stage: r('.hero-stage') };
        layer.style.display = '';
        return { shown, hidden };
      });
      check('the hero, its headline and its button sit exactly where they do without the layer — no layout shift',
        JSON.stringify(geom.shown) === JSON.stringify(geom.hidden), JSON.stringify(geom));
      const stageVsHero = await page.evaluate(() => {
        const s = document.querySelector('.hero-stage').getBoundingClientRect();
        const h = document.querySelector('.hero').getBoundingClientRect();
        return { stageH: s.height, heroH: h.height, stageW: s.width, inner: window.innerWidth, heroW: h.width };
      });
      check('the stage adds no height of its own and spans the viewport while the hero keeps its 1000px box',
        Math.abs(stageVsHero.stageH - stageVsHero.heroH) < 0.5 && stageVsHero.stageW === stageVsHero.inner && stageVsHero.heroW === 1000,
        JSON.stringify(stageVsHero));

      const cs = await page.evaluate(() => {
        const l = getComputedStyle(document.querySelector('.cx-mesh-layer'));
        const c = document.getElementById('cx-mesh');
        const lr = document.querySelector('.cx-mesh-layer').getBoundingClientRect();
        const sr = document.querySelector('.hero-stage').getBoundingClientRect();
        return { pos: l.position, z: l.zIndex, pe: l.pointerEvents, mask: l.maskImage || l.webkitMaskImage,
          w: c.width, h: c.height, cssW: c.style.width, cssH: c.style.height, dpr: Math.min(2, window.devicePixelRatio || 1),
          layerW: lr.width, layerH: lr.height, stageW: sr.width, stageH: sr.height,
          gridOp: getComputedStyle(document.querySelector('.brand-grid')).opacity };
      });
      check('the layer is absolute, at z-index 0, inert, and masked toward the section below',
        cs.pos === 'absolute' && cs.z === '0' && cs.pe === 'none' && /linear-gradient/.test(cs.mask || ''), JSON.stringify(cs));
      check('the canvas fills the stage at a backing scale of at most 2',
        cs.layerW === cs.stageW && cs.layerH === cs.stageH && cs.w === Math.round(cs.layerW * cs.dpr) && cs.h === Math.round(cs.layerH * cs.dpr)
        && cs.cssW === Math.round(cs.layerW) + 'px', JSON.stringify(cs));
      check('the fixed grid underneath is still there and still visible', cs.gridOp === '0.65', cs.gridOp);

      // The colours resolved from the stylesheet's tokens, not the engine's
      // darker defaults: connectors are --accent-ink, nodes --accent-ink2.
      const colours = await page.evaluate(() => {
        const c = window.cygenixHeroMesh.config;
        return { node: c.node, line: c.line, glow: c.glowColor, speed: c.speed, lineAlpha: c.lineAlpha };
      });
      check('the mount resolved the page\'s own tokens for its colours',
        colours.node === '#a9b6ff' && colours.line === '#8ea0ff' && colours.glow.toLowerCase() === '#4a5bd6', JSON.stringify(colours));
      check('with the live-tuned speed and line opacity', colours.speed === 1.0 && colours.lineAlpha === 0.5, JSON.stringify(colours));

      // 2. Contrast: the copy's colours are what they were, and the thing at
      // each of them is the text or the button, never the canvas.
      const hero = await page.evaluate(() => {
        const at = (sel) => { const r = document.querySelector(sel).getBoundingClientRect(); const e = document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2); return e ? (e.tagName + '.' + e.className) : null; };
        return {
          h1: getComputedStyle(document.querySelector('.hero h1 .line1')).color,
          sub: getComputedStyle(document.querySelector('.hero-sub')).color,
          btn: getComputedStyle(document.querySelector('.hero-btns .btn-primary')).backgroundColor,
          underH1: at('.hero h1 .line1'), underSub: at('.hero-sub'), underBtn: at('.hero-btns .btn-primary'), underBtn2: at('.hero-btns .btn-secondary'),
        };
      });
      check('headline and sub-copy colours are unchanged',
        hero.h1 === 'rgb(242, 244, 248)' && hero.sub === 'rgba(255, 255, 255, 0.62)' && hero.btn === 'rgb(74, 91, 214)', JSON.stringify(hero));
      check('the mesh never crosses in front of the text or the buttons',
        /SPAN\.line1/.test(hero.underH1) && /P\.hero-sub/.test(hero.underSub) && /btn-primary/.test(hero.underBtn) && /btn-secondary/.test(hero.underBtn2),
        JSON.stringify(hero));

      const intercepted = [];
      for (const sel of ['.hero-btns .btn-primary', '.hero-btns .btn-secondary', '.nav-links a[href="#platform"]', '.nav-cta']) {
        try { await page.click(sel, { trial: true, timeout: 3000 }); } catch (e) { intercepted.push(sel + ': ' + e.message.split('\n')[0]); }
      }
      check('the calls to action and the nav are clickable — the canvas swallows nothing', intercepted.length === 0, intercepted.join(' | '));

      // 4. The loop runs at the top and stops once the hero is out of view.
      const r0 = await page.evaluate(() => window.__raf);
      await wait(500);
      const r1 = await page.evaluate(() => window.__raf);
      check('the animation loop is running behind the hero', r1 > r0 && r0 > 0, r0 + ' → ' + r1);
      const ms = await page.evaluate(() => window.__frameMs.slice(-30));
      const mean = ms.length ? ms.reduce((a, b) => a + b, 0) / ms.length : 0;
      console.log('  info  mean draw time per frame in this headless, software-rendered browser: ' + mean.toFixed(2) + ' ms over ' + ms.length + ' frames');

      await page.evaluate(() => window.scrollTo({ top: document.documentElement.scrollHeight, behavior: 'instant' }));
      await wait(600);
      const a = await page.evaluate(() => window.__raf);
      await wait(500);
      const b = await page.evaluate(() => window.__raf);
      check('scrolled past the hero, the loop has stopped — no new frames in 500ms', a === b, a + ' → ' + b);
      await page.evaluate(() => window.scrollTo({ top: 0, behavior: 'instant' }));
      await wait(500);
      const c = await page.evaluate(() => window.__raf);
      check('scrolling back to the hero restarts it', c > b, b + ' → ' + c);

      // 5. Resize 1440 → 375: rebuilt at the new size, no horizontal scrollbar.
      await page.setViewportSize({ width: 375, height: 667 });
      await wait(400);
      const small = await page.evaluate(() => {
        const c = document.getElementById('cx-mesh');
        const lr = document.querySelector('.cx-mesh-layer').getBoundingClientRect();
        const dpr = Math.min(2, window.devicePixelRatio || 1);
        const layer = document.querySelector('.cx-mesh-layer');
        const withLayer = document.documentElement.scrollWidth;
        layer.style.display = 'none';
        const without = document.documentElement.scrollWidth;
        layer.style.display = '';
        return { w: c.width, cssW: c.style.width, layerW: lr.width, dpr, inner: window.innerWidth, withLayer, without,
          scrollbarHeight: window.innerHeight - document.documentElement.clientHeight };
      });
      check('at 375px the canvas is rebuilt at the layer\'s new width, not stretched',
        small.layerW === 375 && small.w === Math.round(375 * small.dpr) && small.cssW === '375px', JSON.stringify(small));
      check('and it adds no width to the page — no horizontal scrollbar',
        small.withLayer === small.without && small.scrollbarHeight === 0, JSON.stringify(small));

      // 6. Nothing in the console; nothing fetched from anywhere else.
      check('no console errors or warnings', problems.length === 0, problems.join(' | '));
      const external = requests.filter((u) => !/^http:\/\/localhost:8399\//.test(u) && !/fonts\.g(oogleapis|static)\.com/.test(u));
      check('no new network requests beyond this origin (the aborted font loads excepted)', external.length === 0, external.join(', '));
      check('the engine is the only script the page added', requests.filter((u) => /cygenix-mesh\.js/.test(u)).length === 1);
      await ctx.close();
    }

    // ── prefers-reduced-motion: degraded, not frozen ───────────────────
    // The engine as supplied drew one still frame here. That looked broken
    // on any machine with Reduce Motion on, so it now drifts at 15% speed
    // with parallax (pointer response and camera sweep) zeroed.
    {
      const { ctx, page, problems } = await open(browser, { reducedMotion: 'reduce' });
      await wait(500);
      const rm = await page.evaluate(() => {
        const c = document.getElementById('cx-mesh');
        const d = c.getContext('2d').getImageData(0, 0, c.width, c.height).data;
        let lit = 0;
        for (let i = 3; i < d.length; i += 4 * 97) if (d[i] > 0) lit++;
        const cfg = window.cygenixHeroMesh.config;
        return { raf: window.__raf, lit, speed: cfg.speed, parallax: cfg.parallax };
      });
      check('with reduced motion the mesh is visible', rm.lit > 0, JSON.stringify(rm));
      check('and still animating, slowly: the loop runs', rm.raf > 1, rm.raf);
      check('at 15% of the configured speed with parallax zeroed',
        Math.abs(rm.speed - 1.0 * 0.15) < 1e-9 && rm.parallax === 0, JSON.stringify(rm));
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
