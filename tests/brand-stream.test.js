// tests/brand-stream.test.js — the data-stream layer behind the landing hero.
//
// The layer is decoration, and decoration on the first page anyone sees is
// held to a stricter standard than a feature: it must cost nothing it can
// avoid, and it must never get in the way. The prompt that brought it listed
// seven things it already does — no dependency, one static frame under
// prefers-reduced-motion with no requestAnimationFrame at all, paused on a
// hidden tab, faded out and stopped past the hero, inert to pointers and to
// assistive tech, devicePixelRatio capped, resize debounced. A grep can see
// some of those. The rest are about what the code DOES, so the second half of
// this file runs the real module against a fake DOM and counts the calls.
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

console.log('Brand stream — packets of light behind the hero, and nothing else\n');

const js = read('public', 'brand-stream.js');
const index = read('public', 'index.html');

/* ── 1. Wiring ──────────────────────────────────────────────────────────── */

check('the module exists and exposes BrandStream with CFG and apply()',
  /window\.BrandStream = \(function \(\) \{/.test(js) && /return \{\s*CFG: CFG,\s*apply:/.test(js));
check('index.html loads it deferred, after the decorative divs it anchors to',
  /<script src="\/brand-stream\.js(\?v=[a-f0-9]+)?" defer><\/script>/.test(index)
  && index.indexOf('class="brand-glow"') < index.search(/<script src="\/brand-stream\.js/));
check('and no other page loads it — it is the landing page\'s alone',
  fs.readdirSync(P('public')).filter((f) => f.endsWith('.html') && f !== 'index.html')
    .every((f) => !/brand-stream\.js/.test(read('public', f))));
check('the canvas goes in right after .brand-glow, so it renders under .brand-grid',
  /document\.querySelector\('\.brand-glow'\)/.test(js)
  && /anchor\.parentNode\.insertBefore\(cv, anchor\.nextSibling\)/.test(js));
check('the CSS sits with its two siblings and follows their rules: fixed, z-index 0, inert',
  /\.brand-stream\{[^}]*position:fixed/.test(index)
  && /\.brand-stream\{[^}]*z-index:0/.test(index)
  && /\.brand-stream\{[^}]*pointer-events:none/.test(index)
  && index.indexOf('.brand-stream{') > index.indexOf('.brand-grid{'));
check('its edges are masked so trails enter and leave rather than being cut off',
  /\.brand-stream\{[^}]*mask-image:radial-gradient\(125% 95% at 50% 26%/.test(index)
  && /\.brand-stream\{[^}]*-webkit-mask-image:radial-gradient\(125% 95% at 50% 26%/.test(index));
check('under prefers-reduced-motion it is dimmer and does not fade',
  /@media \(prefers-reduced-motion:reduce\)\{\.brand-stream\{opacity:0\.32;transition:none\}\}/.test(index));
check('the canvas is hidden from assistive tech', /cv\.setAttribute\('aria-hidden', 'true'\)/.test(js));
check('the existing atmosphere is untouched: both glows and the grid are as they were',
  /radial-gradient\(760px 420px at 20% -10%/.test(index)
  && /radial-gradient\(620px 360px at 110% 110%/.test(index)
  && /background-size:52px 52px/.test(index)
  && /<div class="brand-glow" aria-hidden="true"><\/div>\s*<div class="brand-grid" aria-hidden="true"><\/div>/.test(index));

/* ── 2. The budget ──────────────────────────────────────────────────────── */

check('no dependency: the file requires, imports and fetches nothing',
  !/require\(|import |\bfetch\s*\(|XMLHttpRequest|createElement\('script'\)/.test(js));
check('and touches no storage', !/localStorage|sessionStorage|indexedDB|document\.cookie/.test(js));
check('devicePixelRatio is capped at 1.5', /Math\.min\(window\.devicePixelRatio \|\| 1, 1\.5\)/.test(js));
check('beams are pre-rendered sprites drawn with drawImage, not gradients per frame',
  /function makeSprites\(\)/.test(js) && /ctx\.drawImage\(beams\[p\.ci\]/.test(js)
  && !/function draw\(\) \{[\s\S]*?createLinearGradient[\s\S]*?\n  \}/.test(js));
check('resize is debounced', /clearTimeout\(rt\); rt = setTimeout\(resize, 150\)/.test(js));
check('the scroll and resize listeners are passive',
  /addEventListener\('scroll', onScroll, \{ passive: true \}\)/.test(js)
  && /addEventListener\('resize', [\s\S]*?\{ passive: true \}\)/.test(js));
check('the level is "balanced", and the other two levels are written down beside it',
  /laneGap:\s*52,[\s\S]*maxLanes:\s*22,[\s\S]*speedMin:\s*40,[\s\S]*speedMax:\s*110,[\s\S]*trailMin:\s*100,[\s\S]*trailMax:\s*340,[\s\S]*twoPacket:\s*0\.45,[\s\S]*layer:\s*0\.62/.test(js)
  && /\/\/ subtle:\s+laneGap 68/.test(js) && /\/\/ bold:\s+laneGap 42/.test(js)
  && /\/\/ balanced:\s+laneGap 52[^\n]*<- current/.test(js));
check('its colours are the page\'s own: accent ink, accent, teal',
  /'142,160,255'/.test(js) && /'74,91,214'/.test(js) && /'63,181,176'/.test(js)
  && /--accent-ink:#8ea0ff/.test(index) && /--teal:#3fb5b0/.test(index)
  && /--accent:\s*#4A5BD6/i.test(read('public', 'cygenix-brand.css')));

/* ── 3. What it does, run for real ──────────────────────────────────────── */
//
// A fake DOM with the two things the module needs: a body holding .brand-glow
// and .brand-grid, and canvases whose 2d contexts record what was drawn. A
// scripted requestAnimationFrame that never fires on its own, so a frame runs
// only when the test asks — which is how "never called" becomes provable.

function world(opts) {
  opts = opts || {};
  const calls = { raf: 0, cancel: 0, draws: 0, clears: 0 };
  const listeners = { window: {}, document: {} };
  const ctx2d = () => ({
    setTransform() {}, clearRect() { calls.clears++; }, fillRect() {}, drawImage() { calls.draws++; },
    createLinearGradient() { return { addColorStop() {} }; },
    createRadialGradient() { return { addColorStop() {} }; },
    fillStyle: null, globalAlpha: 1, globalCompositeOperation: 'source-over',
  });
  const el = (cls) => {
    const node = { className: cls || '', style: {}, attrs: {}, children: [], parentNode: null,
      setAttribute(k, v) { this.attrs[k] = v; }, getContext() { return ctx2d(); } };
    return node;
  };
  const body = el('');
  const glow = el('brand-glow'), grid = el('brand-grid');
  body.children.push(glow, grid); glow.parentNode = body; grid.parentNode = body;
  body.insertBefore = (node, before) => {
    const i = body.children.indexOf(before);
    body.children.splice(i === -1 ? body.children.length : i, 0, node);
    node.parentNode = body;
  };
  Object.defineProperty(glow, 'nextSibling', { get() { return body.children[body.children.indexOf(glow) + 1] || null; } });
  const doc = {
    body, hidden: false,
    createElement: (t) => el(''),
    querySelector: (s) => (s === '.brand-glow' ? glow : null),
    addEventListener: (t, f) => { listeners.document[t] = f; },
  };
  const win = {
    document: doc, innerWidth: opts.w || 1440, innerHeight: opts.h || 900, scrollY: 0,
    devicePixelRatio: opts.dpr || 2,
    matchMedia: (q) => ({ matches: !!opts.reduced && /reduced-motion/.test(q) }),
    requestAnimationFrame: (f) => { calls.raf++; calls.next = f; return calls.raf; },
    cancelAnimationFrame: () => { calls.cancel++; calls.next = null; },
    addEventListener: (t, f) => { listeners.window[t] = f; },
    setTimeout: (f, ms) => { calls.timer = f; return 1; }, clearTimeout: () => {},
    Math, console,
  };
  win.window = win; win.self = win;
  vm.createContext(win);
  vm.runInContext(js, win, { filename: 'brand-stream.js' });
  return { win, body, glow, grid, calls, listeners, canvas: body.children.find((c) => c.className === 'brand-stream') };
}

// The ordinary case.
{
  const t = world({});
  check('it inserts a canvas between .brand-glow and .brand-grid',
    t.body.children.map((c) => c.className).join(',') === 'brand-glow,brand-stream,brand-grid',
    t.body.children.map((c) => c.className).join(','));
  check('marked aria-hidden', t.canvas && t.canvas.attrs['aria-hidden'] === 'true');
  check('it starts the loop on load', t.calls.raf === 1);
  check('the backing store is scaled by at most 1.5 even on a 2x screen',
    t.canvas.width === 1440 * 1.5 && t.canvas.height === 900 * 1.5
    && t.canvas.style.width === '1440px' && t.canvas.style.height === '900px',
    t.canvas.width + 'x' + t.canvas.height);
  check('the layer opacity is the balanced 0.62', t.canvas.style.opacity === '0.62');
  // Run three frames by hand.
  t.calls.next(16); t.calls.next(32); t.calls.next(48);
  check('a frame draws packets and requests the next frame', t.calls.draws > 0 && t.calls.raf === 4, t.calls.raf);

  // Hidden tab: paused; back: resumed.
  t.win.document.hidden = true; t.listeners.document.visibilitychange();
  check('hiding the tab cancels the loop', t.calls.cancel === 1 && t.calls.next === null);
  const before = t.calls.raf;
  t.win.document.hidden = false; t.listeners.document.visibilitychange();
  check('showing it again restarts the loop', t.calls.raf === before + 1);

  // Past the hero: faded, then stopped.
  t.win.scrollY = 900 * 1.15 + 1; t.listeners.window.scroll();
  check('scrolling past 1.15 viewport heights fades the layer to 0', t.canvas.style.opacity === '0');
  const cancels = t.calls.cancel;
  t.calls.timer();   // the 700ms fade timer
  check('and stops the loop once the fade has had time to finish', t.calls.cancel === cancels + 1 && t.calls.next === null);
  const rafs = t.calls.raf;
  t.win.scrollY = 0; t.listeners.window.scroll();
  check('scrolling back up restores the opacity and restarts the loop',
    t.canvas.style.opacity === '0.62' && t.calls.raf === rafs + 1);

  // Resize rebuilds at the new size.
  t.win.innerWidth = 375; t.win.innerHeight = 667; t.listeners.window.resize(); t.calls.timer();
  check('a resize rebuilds the canvas at the new viewport, not stretched',
    t.canvas.style.width === '375px' && t.canvas.width === Math.round(375 * 1.5), t.canvas.style.width);

  // apply() changes the level.
  t.win.BrandStream.apply({ layer: 0.42 });
  check('apply() retunes the layer without a reload', t.canvas.style.opacity === '0.42');
}

// prefers-reduced-motion: one static frame, no loop, ever.
{
  const t = world({ reduced: true });
  check('under prefers-reduced-motion it draws one static frame',
    t.calls.draws > 0 && t.calls.clears === 1, 'draws=' + t.calls.draws + ' clears=' + t.calls.clears);
  check('and never calls requestAnimationFrame', t.calls.raf === 0, t.calls.raf);
  t.win.document.hidden = false; t.listeners.document.visibilitychange();
  t.win.scrollY = 0; t.listeners.window.scroll();
  check('not even after a visibility or scroll event', t.calls.raf === 0, t.calls.raf);
  // As supplied, the routine wrote CFG.layer inline and the stylesheet's
  // dimmer 0.32 could never apply. Now it leaves the inline value empty.
  check('it leaves the opacity to the stylesheet, so the reduced-motion 0.32 applies',
    t.canvas.style.opacity === '', JSON.stringify(t.canvas.style.opacity));
  t.win.scrollY = 900 * 1.15 + 1; t.listeners.window.scroll();
  t.win.scrollY = 0; t.listeners.window.scroll();
  check('and still does after fading out past the hero and coming back',
    t.canvas.style.opacity === '', JSON.stringify(t.canvas.style.opacity));
}

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
