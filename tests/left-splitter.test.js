// Tests the Object Mapping editor's resizable / collapsible source panel.
//
// The left column was a fixed 300px: long schema-qualified table names got
// clipped in the picker, the WHERE and GROUP BY rows had little room to type
// in, and once the source table was picked there was no way to hand that space
// back to the mapping grid. It now drags to resize and collapses, with both
// remembered across visits.
//
// The module is extracted from public/object_mapping.html and run against DOM
// and localStorage stubs, so what is under test is the shipped code.
const fs = require('fs');
const vm = require('vm');

const html = fs.readFileSync(__dirname + '/../public/object_mapping.html', 'utf8');
const start = html.indexOf('// ══════════════════════════════════════════════════════════════════════════\n// LEFT COLUMN SPLITTER');
const end   = html.indexOf('// ══════════════════════════════════════════════════════════════════════════\n// WHERE CONDITIONS + GROUP BY');
if (start < 0 || end < 0 || end < start) {
  console.log('FAIL: could not locate the splitter module in object_mapping.html');
  process.exit(1);
}

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

function build(stored = {}) {
  const store = { ...stored };
  const classes = new Set();
  const grid = {
    id: 'om-grid',
    _props: {},
    style: { setProperty: (k, v) => { grid._props[k] = v; } },
    classList: {
      toggle: (c, on) => { on ? classes.add(c) : classes.delete(c); },
      contains: c => classes.has(c),
    },
    getBoundingClientRect: () => ({ left: 100 }),
  };
  const btn   = { id:'om-split-btn', textContent:'‹', title:'' };
  const split = { id:'om-split', _classes:new Set(),
                  classList:{ add(c){ split._classes.add(c); }, remove(c){ split._classes.delete(c); } } };
  const els = { 'om-grid': grid, 'om-split-btn': btn, 'om-split': split };

  const listeners = {};
  const sb = {
    console, JSON, String, Number, Math, isFinite, parseInt,
    $: id => els[id] || null,
    localStorage: {
      getItem: k => (k in store ? store[k] : null),
      setItem: (k, v) => { store[k] = String(v); },
    },
    getComputedStyle: () => ({ getPropertyValue: k => grid._props[k] || '' }),
    document: {
      body: { style: {} },
      addEventListener: (t, fn) => { (listeners[t] = listeners[t] || []).push(fn); },
      removeEventListener: (t, fn) => {
        if (listeners[t]) listeners[t] = listeners[t].filter(f => f !== fn);
      },
    },
  };
  vm.createContext(sb);
  vm.runInContext(html.slice(start, end), sb);
  const fire = (t, ev) => (listeners[t] || []).slice().forEach(fn => fn(ev));
  return {
    sb, grid, btn, split, store, fire, listeners,
    get: expr => vm.runInContext(expr, sb),
    width: () => parseInt(grid._props['--om-left-w'], 10),
    collapsed: () => classes.has('left-collapsed'),
  };
}

console.log('Object Mapping — source panel splitter\n');

// ── Clamping ─────────────────────────────────────────────────────────────
let t = build();
t.get('omApplyLeft(420, false)');
check('a width in range is applied as-is', t.width() === 420, String(t.width()));
t.get('omApplyLeft(50, false)');
check('a width below the minimum clamps up', t.width() === 220, String(t.width()));
t.get('omApplyLeft(9999, false)');
check('a width above the maximum clamps down', t.width() === 620, String(t.width()));
t.get('omApplyLeft(300.6, false)');
check('a fractional width is rounded', t.width() === 301, String(t.width()));
t.get('omApplyLeft("nonsense", false)');
check('an unparseable width falls back to the default', t.width() === 300, String(t.width()));

// ── Persistence ──────────────────────────────────────────────────────────
t = build();
t.get('omApplyLeft(380, false)');
check('applying without persist does not write', t.store['cygenix_om_left_width'] === undefined);
t.get('omApplyLeft(380, true)');
check('applying with persist writes the width', t.store['cygenix_om_left_width'] === '380');
check('the clamped value is what gets stored — not the raw input',
  build().get('omApplyLeft(50, true)') === undefined);

t = build({ 'cygenix_om_left_width': '450' });
t.get('omInitSplitter()');
check('a stored width is restored on load', t.width() === 450, String(t.width()));
check('restoring does not re-collapse', t.collapsed() === false);

t = build({ 'cygenix_om_left_width': '99999' });
t.get('omInitSplitter()');
check('a corrupt stored width is clamped, not applied blindly', t.width() === 620);

t = build();
t.get('omInitSplitter()');
check('with nothing stored the width is left at the CSS default',
  t.grid._props['--om-left-w'] === undefined);

// ── Collapse ─────────────────────────────────────────────────────────────
t = build();
t.get('omToggleLeft()');
check('toggling collapses the panel', t.collapsed() === true);
check('the button flips to the expand arrow', t.btn.textContent === '›');
check('the button title tells you what it will do',
  /Show the source panel/.test(t.btn.title), t.btn.title);
check('collapsing is remembered', t.store['cygenix_om_left_collapsed'] === '1');

t.get('omToggleLeft()');
check('toggling again expands', t.collapsed() === false);
check('the button flips back', t.btn.textContent === '‹');
check('expanding is remembered', t.store['cygenix_om_left_collapsed'] === '0');

t = build({ 'cygenix_om_left_collapsed': '1' });
t.get('omInitSplitter()');
check('a collapsed panel stays collapsed across visits', t.collapsed() === true);
check('the restored button shows the expand arrow', t.btn.textContent === '›');

// ── Reset ────────────────────────────────────────────────────────────────
t = build({ 'cygenix_om_left_width':'600', 'cygenix_om_left_collapsed':'1' });
t.get('omInitSplitter()');
t.get('omResetLeft()');
check('reset returns the width to the default', t.width() === 300);
check('reset also expands a collapsed panel', t.collapsed() === false);
check('reset persists both', t.store['cygenix_om_left_width'] === '300' &&
                             t.store['cygenix_om_left_collapsed'] === '0');

// ── Dragging ─────────────────────────────────────────────────────────────
// Width is measured from the grid's left edge (stubbed at x=100), so the page
// being scrolled sideways cannot skew it.
t = build();
t.get('omSplitStart({ preventDefault(){}, clientX: 400 })');
check('starting a drag marks the handle', t.split._classes.has('dragging'));
t.fire('mousemove', { clientX: 500 });
check('dragging right widens the panel', t.width() === 400, String(t.width()));
t.fire('mousemove', { clientX: 250 });
check('dragging left narrows it', t.width() === 150 || t.width() === 220, String(t.width()));
check('dragging past the minimum clamps rather than collapsing', t.width() === 220);
check('mid-drag positions are not persisted', t.store['cygenix_om_left_width'] === undefined);

t.fire('mouseup', {});
check('releasing persists the final width', t.store['cygenix_om_left_width'] === '220');
check('releasing clears the dragging state', !t.split._classes.has('dragging'));
check('releasing removes its move listener', (t.listeners['mousemove'] || []).length === 0);
check('releasing removes its up listener', (t.listeners['mouseup'] || []).length === 0);

// A drag that starts while collapsed should bring the panel back rather than
// resizing something invisible.
t = build({ 'cygenix_om_left_collapsed': '1' });
t.get('omInitSplitter()');
t.get('omSplitStart({ preventDefault(){}, clientX: 400 })');
check('starting a drag on a collapsed panel expands it', t.collapsed() === false);

// Touch drags go through the same path.
t = build();
t.get('omSplitStart({ preventDefault(){}, touches:[{ clientX: 400 }] })');
t.fire('touchmove', { touches:[{ clientX: 460 }] });
check('a touch drag resizes', t.width() === 360, String(t.width()));
t.fire('touchend', {});
check('a touch drag persists on release', t.store['cygenix_om_left_width'] === '360');

// ── Element ids are unique ───────────────────────────────────────────────
// getElementById returns the FIRST match, so a duplicate id silently hands
// code the wrong element. This caught the GROUP BY datalist reusing
// #src-col-list, the id of the source column list — rendering the GROUP BY
// rows replaced the user's source columns with <option> tags.
{
  const ids = [...html.matchAll(/\sid="([^"]+)"/g)].map(m => m[1]);
  const seen = new Set(), dupes = new Set();
  ids.forEach(id => { if (seen.has(id)) dupes.add(id); else seen.add(id); });
  check('no element id is used twice in the page',
    dupes.size === 0, [...dupes].join(', '));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
