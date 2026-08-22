// Tests for the shared full-screen helper and the eight views wired onto it.
//
// The helper is driven here against a small DOM stub rather than a browser:
// what matters at this level is the state machine — that button state is read
// from the browser's own fullscreenElement and never from a local flag, that
// overlays are moved in and put back exactly where they came from, and that a
// refusal falls back to pseudo-full-screen with identical public behaviour.
// The real Fullscreen API is exercised in the browser smoke.
const fs = require('fs');
const vm = require('vm');

// ── A DOM stub with just enough of the shape the helper touches ─────────────
function makeEl(tag){
  const el = {
    tagName: (tag || 'div').toUpperCase(), children: [], parentNode: null, style: {},
    _class: new Set(), _attr: {}, _listeners: {}, clientHeight: 800, offsetHeight: 40,
    nextSibling: null, isContentEditable: false,
  };
  el.classList = {
    add: (...c) => c.forEach(x => el._class.add(x)),
    remove: (...c) => c.forEach(x => el._class.delete(x)),
    toggle: (c, on) => { on ? el._class.add(c) : el._class.delete(c); },
    contains: c => el._class.has(c),
  };
  Object.defineProperty(el, 'className', {
    get: () => [...el._class].join(' '),
    set: v => { el._class = new Set(String(v).split(/\s+/).filter(Boolean)); },
  });
  el.setAttribute = (k, v) => { el._attr[k] = String(v); };
  el.getAttribute = k => (k in el._attr ? el._attr[k] : null);
  el.addEventListener = (t, fn) => { (el._listeners[t] = el._listeners[t] || []).push(fn); };
  el.removeEventListener = (t, fn) => {
    el._listeners[t] = (el._listeners[t] || []).filter(f => f !== fn);
  };
  el.dispatch = (t, ev) => (el._listeners[t] || []).forEach(fn => fn(Object.assign({ preventDefault(){} }, ev)));
  el.appendChild = (c) => {
    if (c.parentNode) c.parentNode.removeChild(c);
    el.children.push(c); c.parentNode = el; reindex(el); return c;
  };
  el.insertBefore = (c, ref) => {
    if (c.parentNode) c.parentNode.removeChild(c);
    const i = ref ? el.children.indexOf(ref) : -1;
    i < 0 ? el.children.push(c) : el.children.splice(i, 0, c);
    c.parentNode = el; reindex(el); return c;
  };
  el.removeChild = (c) => {
    el.children = el.children.filter(x => x !== c); c.parentNode = null; reindex(el); return c;
  };
  el.contains = (n) => { for (let p = n; p; p = p.parentNode) if (p === el) return true; return false; };
  el.closest = (sel) => { for (let p = el; p; p = p.parentNode) if (matches(p, sel)) return p; return null; };
  return el;
}
function reindex(parent){
  parent.children.forEach((c, i) => { c.nextSibling = parent.children[i + 1] || null; });
}
function matches(el, sel){
  if (!el || !el.tagName) return false;
  if (sel.startsWith('#')) return el._attr.id === sel.slice(1);
  if (sel.startsWith('.')) return el._class.has(sel.slice(1));
  return el.tagName === sel.toUpperCase();
}
function walk(root, out){ (root.children || []).forEach(c => { out.push(c); walk(c, out); }); return out; }

const doc = makeEl('body');
doc.head = makeEl('head');
doc.body = makeEl('body');
doc.documentElement = makeEl('html');
doc.documentElement.requestFullscreen = function () { return { catch(){} }; };
doc.createElement = makeEl;
doc.getElementById = id => walk(doc.body, []).concat(walk(doc.head, [])).find(e => e._attr.id === id) || null;
doc.querySelector = sel => walk(doc.body, []).find(e => matches(e, sel)) || null;
doc.querySelectorAll = sel => walk(doc.body, []).filter(e => matches(e, sel));
doc.fullscreenElement = null;
doc._listeners = {};
doc.addEventListener = (t, fn) => { (doc._listeners[t] = doc._listeners[t] || []).push(fn); };
doc.removeEventListener = (t, fn) => { doc._listeners[t] = (doc._listeners[t] || []).filter(f => f !== fn); };
doc.dispatch = (t, ev) => (doc._listeners[t] || []).forEach(fn => fn(Object.assign({ preventDefault(){} }, ev)));

const frames = [];
const sandbox = {
  window: { addEventListener(){} },
  document: doc,
  console,
  requestAnimationFrame: fn => { frames.push(fn); return frames.length; },
  ResizeObserver: function (cb) { this.cb = cb; this.observe = () => {}; this.disconnect = () => {}; },
};
sandbox.window.document = doc;
vm.createContext(sandbox);
vm.runInContext(fs.readFileSync(__dirname + '/../public/cygenix-fullscreen.js', 'utf8'), sandbox);
const F = sandbox.window.CygenixFullscreen;
const flush = () => { const f = frames.splice(0); f.forEach(fn => fn()); };

// Drive the browser's side of the contract.
function goFullscreen(el){ doc.fullscreenElement = el; doc.dispatch('fullscreenchange'); }
function leaveFullscreen(){ doc.fullscreenElement = null; doc.dispatch('fullscreenchange'); }
doc.exitFullscreen = () => { leaveFullscreen(); };

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Full screen — shared helper\n');

check('the helper loads and exposes attach', typeof F.attach === 'function');

// Wrapped, because one case has to let a rejected promise settle.
(async function main(){

// ── 1. A plain view: enter, leave, button state ─────────────────────────────
const view = doc.body.appendChild(makeEl('div'));
view._attr.id = 'view-a';
view.requestFullscreen = function () { goFullscreen(view); return { catch(){} }; };
const btn = doc.body.appendChild(makeEl('button'));
let changes = [];
const A = F.attach({
  button: btn, target: view,
  onChange: (on, t) => changes.push([on, t === view]),
});

check('the button is a real button with a resting label',
  btn.getAttribute('type') === 'button' && btn.textContent === '⤢ Full screen');
check('and says it is not pressed', btn.getAttribute('aria-pressed') === 'false');

btn.dispatch('click');
flush();
check('clicking it enters full screen', A.isFull() === true);
check('the label offers the way out', btn.textContent === '⤡ Exit full screen');
check('and reports itself pressed', btn.getAttribute('aria-pressed') === 'true'
  && btn.classList.contains('on'));
check('onChange fired once, with the target', changes.length === 1 && changes[0][0] === true && changes[0][1]);
check('the target carries the class its :fullscreen rules key off',
  view.classList.contains('cyg-fs-target'));
check('the document is marked so fixed page furniture can stand down',
  doc.documentElement.classList.contains('cyg-fs-on'));

// Esc never goes through the click handler — state must come from the browser.
leaveFullscreen();
flush();
check('Esc alone restores the button', A.isFull() === false && btn.textContent === '⤢ Full screen');
check('and fires onChange with false', changes.length === 2 && changes[1][0] === false);
check('the document mark is cleared', !doc.documentElement.classList.contains('cyg-fs-on'));

// ── 2. Overlays parented to <body> ──────────────────────────────────────────
const tip = doc.body.appendChild(makeEl('div'));
tip._class.add('tip-x');
const after = doc.body.appendChild(makeEl('div'));   // tip's original next sibling
const B = F.attach({ button: doc.body.appendChild(makeEl('button')), target: view, adopt: ['.tip-x'] });
B.enter(); flush();
check('a body-level overlay is carried into the full-screen element',
  tip.parentNode === view && view.contains(tip));
check('and marked, so CSS can place it', tip.classList.contains('cyg-fs-adopted'));
B.exit(); flush();
check('and put back in the same place on the way out',
  tip.parentNode === doc.body && tip.nextSibling === after);
check('with the marker removed', !tip.classList.contains('cyg-fs-adopted'));

// ── 3. Refusal → pseudo-full-screen, same public behaviour ──────────────────
const shy = doc.body.appendChild(makeEl('div'));
shy.requestFullscreen = function () { return Promise.reject(new Error('not allowed')); };
const shyBtn = doc.body.appendChild(makeEl('button'));
let shyChanges = 0;
const C = F.attach({ button: shyBtn, target: shy, onChange: () => shyChanges++ });
C.enter();
await new Promise(r => setTimeout(r, 0));
flush();
check('a rejected request falls back to pseudo-full-screen',
  C.isFull() === true && shy.classList.contains('cyg-pseudo-fs'));
check('the button reads exactly as it would natively', shyBtn.textContent === '⤡ Exit full screen');
check('onChange still fired', shyChanges === 1);
check('the page behind it is scroll-locked', doc.body.style.overflow === 'hidden');
doc.dispatch('keydown', { key: 'Escape', target: doc.body });
flush();
check('Esc leaves pseudo-full-screen', C.isFull() === false && !shy.classList.contains('cyg-pseudo-fs'));
check('and the scroll lock is released', doc.body.style.overflow === '');

const old = doc.body.appendChild(makeEl('div'));    // no requestFullscreen at all
const D = F.attach({ button: doc.body.appendChild(makeEl('button')), target: old });
D.enter(); flush();
check('a browser with no Fullscreen API at all gets the same fallback',
  D.isFull() === true && old.classList.contains('cyg-pseudo-fs'));
D.exit(); flush();

// ── 4. The F key ────────────────────────────────────────────────────────────
let visible = false;
const keyTarget = doc.body.appendChild(makeEl('div'));
keyTarget.requestFullscreen = function () { goFullscreen(keyTarget); return { catch(){} }; };
const E = F.attach({ button: doc.body.appendChild(makeEl('button')), target: keyTarget,
                     visible: () => visible });
doc.dispatch('keydown', { key: 'f', target: doc.body });
flush();
check('F does nothing while the view is off screen', E.isFull() === false);
visible = true;
doc.dispatch('keydown', { key: 'f', target: doc.body });
flush();
check('F takes the view on screen full screen', E.isFull() === true);
doc.dispatch('keydown', { key: 'f', target: doc.body });
flush();
check('and F again brings it back', E.isFull() === false);

const input = makeEl('input');
doc.dispatch('keydown', { key: 'f', target: input });
check('F typed into a filter box is left alone', E.isFull() === false);
doc.dispatch('keydown', { key: 'f', target: doc.body, metaKey: true });
check('and ⌘F still belongs to the browser', E.isFull() === false);

// ── 5. A view whose target switches ─────────────────────────────────────────
const one = doc.body.appendChild(makeEl('div')), two = doc.body.appendChild(makeEl('div'));
[one, two].forEach(el => { el.requestFullscreen = function () { goFullscreen(el); return { catch(){} }; }; });
let which = one;
const G = F.attach({ button: doc.body.appendChild(makeEl('button')), target: () => which });
G.enter(); flush();
check('a switching view goes full screen on the sub-view showing', G.target === one);
G.exit(); flush();
which = two;
G.enter(); flush();
check('and follows when the sub-view changes', G.target === two);
G.exit(); flush();

// ── 6. Every view on the page is wired to it ────────────────────────────────
const PAGE = fs.readFileSync(__dirname + '/../public/schema_explorer.html', 'utf8');
const DM = fs.readFileSync(__dirname + '/../public/cygenix-data-maps.js', 'utf8');

check('the page loads the shared helper', /<script src="\/cygenix-fullscreen\.js[^"]*" defer><\/script>/.test(PAGE));
check('before the data maps module, which attaches to it',
  PAGE.indexOf('cygenix-fullscreen.js') < PAGE.indexOf('cygenix-data-maps.js'));

// One control per view. The four data maps share a card, so they share a
// button — the brief's "one button, not four".
const BUTTONS = ['sm-fs-btn', 'da-fs-btn', 'cv-fs-ribbons', 'cv-fs-waves', 'cv-fs-checks', 'am-fs-btn'];
const missingBtns = BUTTONS.filter(id => !new RegExp('id="' + id + '"').test(PAGE));
check('every view carries a full-screen button (' + BUTTONS.length + ' in the page)',
  missingBtns.length === 0, missingBtns.join(', '));
check('and the four data maps share one, built with their card',
  /wireFullscreen\(card, hd, seg\)/.test(DM) && /'⤢ Full screen'/.test(DM));
check('all of them say the same thing',
  (PAGE.match(/⤢ Full screen<\/button>/g) || []).length === BUTTONS.length);
check('and offer the same shortcut',
  (PAGE.match(/title="Full screen \(F, or Esc to leave\)"/g) || []).length === BUTTONS.length);

check('the two hand-rolled implementations are gone',
  !/function smFullscreen\(/.test(PAGE) && !/function daFullscreen\(/.test(PAGE)
  && !/function smOnFullscreenChange\(/.test(PAGE) && !/function daOnFullscreenChange\(/.test(PAGE));
check('and so is the page\'s own fullscreenchange plumbing',
  !/addEventListener\('fullscreenchange'/.test(PAGE) && !/seOnFullscreenChange/.test(PAGE));
check('nothing calls the removed functions any more',
  !/onclick="smFullscreen\(\)"/.test(PAGE) && !/onclick="daFullscreen\(\)"/.test(PAGE));

check('the Schema map still re-measures and refits after a transition',
  /onChange: \(\) => \{ smRenderCanvas\(\); smFit\(\); \}/.test(PAGE));
check('the Data map still resizes its canvas', /if \(DA\.view === 'map'\) daResize\(\)/.test(PAGE));
check('Coverage re-lays out each panel', /onChange: \(\) => cvRender\(\)/.test(PAGE));
check('the Migration map follows whichever sub-view is showing',
  /target: \(\) => \$\('am-v-' \+ \(AM\.sub \|\| 'areas'\)\)/.test(PAGE));
check('and re-flows it', /onChange: \(\) => amRender\(\)/.test(PAGE));
check('the data maps re-draw, which is what lifts their height cap',
  /fsHeight = on \? Math\.max\(0, card\.clientHeight/.test(DM) && /render\(\);/.test(DM));

check('F is routed by which view is on screen, not bound globally per view',
  /visible: \(\) => seTab === 'schema'/.test(PAGE) && /visible: \(\) => seTab === 'atlas'/.test(PAGE)
  && /visible: \(\) => seTab === 'areas'/.test(PAGE));
check('the page no longer keeps its own F handler', !/smFullscreen\(\); \}/.test(PAGE));

check('the Data map tooltip is carried into full screen', /adopt: \['#da-tip'\]/.test(PAGE));
check('so are the data maps\' tooltip and legend rail',
  /adopt: \['\.cdm-tip', '\.cdm-rail'\]/.test(DM));
check('the consent banner is suppressed rather than left painting underneath',
  /#cc-banner/.test(fs.readFileSync(__dirname + '/../public/cygenix-fullscreen.js', 'utf8')));

check('each new target has :fullscreen CSS of its own, webkit included',
  /\.panel\.cyg-fs-target:fullscreen/.test(PAGE) && /\.panel\.cyg-fs-target:-webkit-full-screen/.test(PAGE)
  && /\.cdm-card\.cyg-fs-target:fullscreen/.test(PAGE));
check('and the two original rules are untouched',
  /\.sm-canvas-wrap:fullscreen\{height:100vh/.test(PAGE) && /\.da-panel:fullscreen\{background:var\(--bg2\)/.test(PAGE));

check('the helper is attached once the page is up', /seAttachFullscreen\(\);/.test(PAGE));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);

})();
