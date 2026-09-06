// tests/hero-mesh.test.js — the polygon mesh behind the landing hero.
//
// Decoration on the first page anyone sees is held to a stricter standard
// than a feature: it must cost nothing it can avoid and never get in the
// way. The brief listed what the engine does — one canvas, no dependency,
// the page's own colours, one static frame and no requestAnimationFrame
// under prefers-reduced-motion, paused out of view and on a hidden tab,
// devicePixelRatio capped at 2, the hero's size and spacing untouched. A
// grep can see some of that. The rest is about what the code DOES, so the
// second half runs the real engine against a fake DOM and counts the calls.
'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const P = (...s) => path.join(__dirname, '..', ...s);
const read = (...s) => fs.readFileSync(P(...s), 'utf8');

console.log('Hero mesh — a network behind the headline, and nothing in front of it\n');

const js = read('public', 'cygenix-mesh.js');
const index = read('public', 'index.html');

/* ── 1. Wiring ──────────────────────────────────────────────────────────── */

check('the engine exists and exposes CygenixMesh.mount and its presets',
  /window\.CygenixMesh = \{ mount: mount, presets: PRESETS \}/.test(js)
  && /subtle:\s*\{ density: 0\.55/.test(js) && /balanced:\s*\{ density: 1\.00/.test(js) && /cinematic:\s*\{ density: 1\.60/.test(js));
check('index.html loads it deferred and mounts it after DOMContentLoaded, guarded',
  /<script src="\/cygenix-mesh\.js(\?v=[a-f0-9]+)?" defer><\/script>/.test(index)
  && /document\.addEventListener\('DOMContentLoaded', function \(\) \{\s*var el = document\.getElementById\('cx-mesh'\);\s*if \(!el \|\| !window\.CygenixMesh\) return;/.test(index));
check('and no other page loads it — the hero only exists on the landing page',
  fs.readdirSync(P('public')).filter((f) => f.endsWith('.html') && f !== 'index.html')
    .every((f) => !/cygenix-mesh\.js/.test(read('public', f))));
check('the mount uses the balanced values with the page\'s own colours',
  /density:\s*1\.00/.test(index) && /speed:\s*0\.75/.test(index) && /reach:\s*165/.test(index)
  && /glow:\s*0\.65/.test(index) && /parallax:\s*0\.55/.test(index) && /lineAlpha:\s*0\.30/.test(index)
  && /node:\s*'#a9b6ff'/.test(index) && /line:\s*'#4a5bd6'/.test(index) && /glowColor:\s*'#4a5bd6'/.test(index)
  && /--accent-ink2:#a9b6ff/.test(index) && /--accent:\s*#4A5BD6/i.test(read('public', 'cygenix-brand.css')));

// The layout: a full-width stage wrapping the hero, the layer first in it.
check('the hero is wrapped in a stage whose first child is the mesh layer, then the hero itself',
  /<div class="hero-stage">\s*<div class="cx-mesh-layer" aria-hidden="true"><canvas id="cx-mesh"><\/canvas><\/div>\s*<section class="hero">/.test(index)
  && /<\/section>\s*<\/div>\s*\n/.test(index.slice(index.indexOf('<section class="hero">'), index.indexOf('<section class="hero">') + 2000)));
check('the stage is a positioned, isolated, clipping box that adds no space',
  /\.hero-stage\{position:relative;z-index:1;isolation:isolate;overflow:hidden\}/.test(index)
  && !/\.hero-stage\{[^}]*(padding|margin|height|min-height)/.test(index));
check('the hero keeps its own rule exactly — same padding, same 1000px box',
  /\.hero\{position:relative;z-index:1;text-align:center;padding:9rem clamp\(1rem,5vw,2rem\) 3\.5rem;\s*max-width:1000px;margin:0 auto\}/.test(index));
check('the copy is above the layer', /\.hero-stage>\.hero\{position:relative;z-index:1\}/.test(index));
check('the layer is absolute, at z-index 0, inert, and fades out before the next section',
  /\.cx-mesh-layer\{position:absolute;inset:0;z-index:0;pointer-events:none;/.test(index)
  && /\.cx-mesh-layer\{[^}]*mask-image:linear-gradient\(to bottom,#000 0%,#000 70%,transparent 100%\)/.test(index)
  && /\.cx-mesh-layer\{[^}]*-webkit-mask-image:linear-gradient\(to bottom,#000 0%,#000 70%,transparent 100%\)/.test(index));
check('the canvas fills the layer as a block, so the engine reads a real size from its parent',
  /\.cx-mesh-layer canvas\{display:block;width:100%;height:100%\}/.test(index));
check('the fixed glow and grid underneath are exactly as they were',
  /radial-gradient\(760px 420px at 20% -10%/.test(index) && /background-size:52px 52px/.test(index)
  && /<div class="brand-glow" aria-hidden="true"><\/div>\s*<div class="brand-grid" aria-hidden="true"><\/div>/.test(index));
check('the data-stream layer it replaced is gone, everywhere',
  !fs.existsSync(P('public', 'brand-stream.js')) && !/brand-stream/.test(index)
  && !/brand-stream/.test(read('package.json')));

/* ── 2. The budget ──────────────────────────────────────────────────────── */

check('no dependency: the engine requires, imports and fetches nothing',
  !/require\(|import |\bfetch\s*\(|XMLHttpRequest|createElement\('script'\)/.test(js));
check('and touches no storage', !/localStorage|sessionStorage|indexedDB|document\.cookie/.test(js));
check('devicePixelRatio is capped at 2', /maxDPR: 2/.test(js) && /Math\.min\(cfg\.maxDPR, window\.devicePixelRatio \|\| 1\)/.test(js));
check('the far layer is drawn at half resolution and blurred once, not per node',
  /far\.width = Math\.max\(1, Math\.round\(W \* dpr \* 0\.5\)\)/.test(js) && /ctx\.filter = 'blur\(1\.6px\)'/.test(js));
check('links are found on a spatial grid, not by comparing every pair',
  /var cell = reach, cols = Math\.ceil/.test(js) && /grid\[idx\] \|\| \(grid\[idx\] = \[\]\)/.test(js));
check('the node count is capped', /Math\.min\(420, Math\.max\(24,/.test(js));
check('it stops out of view and on a hidden tab',
  /new IntersectionObserver\(/.test(js) && /inView = es\[0\]\.isIntersecting/.test(js)
  && /document\.addEventListener\('visibilitychange', onVis\)/.test(js));
check('the pointer listener is passive', /addEventListener\('pointermove', onPointer, \{ passive: true \}\)/.test(js));
check('the engine below the header is the one supplied, verbatim',
  js.indexOf('/* Cygenix ambient mesh — animated polygon network background.') > 0
  && /^\(function \(\) \{\n  var PRESETS = \{/m.test(js) && /^\}\)\(\);\n$/m.test(js));

/* ── 3. What it does, run for real ──────────────────────────────────────── */
//
// A fake DOM with what the engine needs: a canvas whose parent has a size,
// 2d contexts that record what was drawn, and a requestAnimationFrame that
// never fires on its own — a frame runs only when the test asks, which is
// how "never called" becomes a number.

function world(opts) {
  opts = opts || {};
  const calls = { raf: 0, cancel: 0, draws: 0, clears: 0, removed: [] };
  const listeners = { window: {}, document: {}, host: {} };
  let ioCb = null, roCb = null;
  const ctx2d = () => ({
    setTransform() {}, clearRect() { calls.clears++; }, fillRect() {}, drawImage() { calls.draws++; },
    beginPath() {}, moveTo() {}, lineTo() {}, stroke() {}, arc() {}, fill() { calls.draws++; },
    createRadialGradient() { return { addColorStop() {} }; },
    filter: 'none', lineWidth: 1, strokeStyle: null, fillStyle: null, globalCompositeOperation: 'source-over',
  });
  const canvas = { style: {}, width: 0, height: 0, getContext: () => ctx2d() };
  const host = {
    getBoundingClientRect: () => ({ width: opts.w || 1440, height: opts.h || 600, left: 0, top: 0 }),
    addEventListener: (t, f) => { listeners.host[t] = f; },
    removeEventListener: (t) => { calls.removed.push('host:' + t); },
  };
  canvas.parentElement = host;
  const doc = {
    hidden: false,
    createElement: () => ({ style: {}, getContext: () => ctx2d() }),
    addEventListener: (t, f) => { listeners.document[t] = f; },
    removeEventListener: (t) => { calls.removed.push('document:' + t); },
  };
  const win = {
    document: doc, devicePixelRatio: opts.dpr || 3,
    matchMedia: (q) => ({ matches: !!opts.reduced && /reduced-motion/.test(q) }),
    requestAnimationFrame: (f) => { calls.raf++; calls.next = f; return calls.raf; },
    cancelAnimationFrame: () => { calls.cancel++; calls.next = null; },
    addEventListener: (t, f) => { listeners.window[t] = f; },
    removeEventListener: (t) => { calls.removed.push('window:' + t); },
    performance: { now: () => 1000 },
    IntersectionObserver: function (cb) { ioCb = cb; this.observe = () => {}; this.disconnect = () => { calls.removed.push('io'); }; },
    ResizeObserver: function (cb) { roCb = cb; this.observe = () => {}; this.disconnect = () => { calls.removed.push('ro'); }; },
    Math, Object, console,
  };
  win.window = win; win.self = win;
  vm.createContext(win);
  vm.runInContext(js, win, { filename: 'cygenix-mesh.js' });
  const api = win.CygenixMesh.mount(canvas, { density: 1.0, speed: 0.75, reach: 165, glow: 0.65, parallax: 0.55, lineAlpha: 0.30, node: '#a9b6ff', line: '#4a5bd6', glowColor: '#4a5bd6' });
  return { win, canvas, host, calls, listeners, api, io: () => ioCb, ro: () => roCb };
}

// The ordinary case.
{
  const t = world({});
  check('it sizes the canvas from its parent, at a backing scale of at most 2 even on a 3x screen',
    t.canvas.width === 1440 * 2 && t.canvas.height === 600 * 2 && t.canvas.style.width === '1440px' && t.canvas.style.height === '600px',
    t.canvas.width + 'x' + t.canvas.height);
  check('it draws a first frame at once, so there is no blank canvas before the loop', t.calls.draws > 0);
  check('and starts the loop', t.calls.raf === 1, t.calls.raf);
  const d0 = t.calls.draws;
  t.calls.next(1016); t.calls.next(1032); t.calls.next(1048);
  check('a frame moves the field, draws it and asks for the next', t.calls.draws > d0 && t.calls.raf === 4, t.calls.raf);

  t.win.document.hidden = true; t.listeners.document.visibilitychange();
  check('hiding the tab cancels the loop', t.calls.cancel === 1 && t.calls.next === null);
  let before = t.calls.raf;
  t.win.document.hidden = false; t.listeners.document.visibilitychange();
  check('showing it again restarts it', t.calls.raf === before + 1);

  t.io()([{ isIntersecting: false }]);
  check('scrolling the hero out of view stops the loop', t.calls.cancel === 2 && t.calls.next === null);
  before = t.calls.raf;
  t.io()([{ isIntersecting: true }]);
  check('and scrolling it back in restarts it', t.calls.raf === before + 1);

  t.host.getBoundingClientRect = () => ({ width: 375, height: 520, left: 0, top: 0 });
  t.ro()();
  check('a resize rebuilds the canvas at the parent\'s new size, not stretched',
    t.canvas.width === 375 * 2 && t.canvas.style.width === '375px' && t.canvas.style.height === '520px', t.canvas.style.width);

  t.api.update({ preset: 'subtle' });
  check('update() can switch preset', t.api.config.density === 0.55 && t.api.config.lineAlpha === 0.20, JSON.stringify(t.api.config));

  t.api.destroy();
  check('destroy() stops the loop and removes every listener and observer',
    t.calls.next === null && ['io', 'ro', 'document:visibilitychange', 'window:pointermove', 'host:pointerleave'].every((k) => t.calls.removed.includes(k)),
    t.calls.removed.join(','));
}

// prefers-reduced-motion: one still frame, no loop, ever.
{
  const t = world({ reduced: true });
  check('under prefers-reduced-motion it draws one still frame', t.calls.draws > 0 && t.calls.clears >= 1);
  check('and never calls requestAnimationFrame', t.calls.raf === 0, t.calls.raf);
  t.win.document.hidden = false; t.listeners.document.visibilitychange();
  t.io()([{ isIntersecting: true }]);
  check('not even after a visibility or intersection event', t.calls.raf === 0, t.calls.raf);
}

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
