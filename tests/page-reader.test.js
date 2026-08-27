// Tests for the assistant's generic page reader — read_page.
//
// The assistant's other actions are typed: they know what a job is, what a
// stream is, what the SQL editor holds. read_page is the opposite: it knows
// nothing about the product and describes whatever is drawn. That generality
// is the point, and it is also what makes it worth pinning down, because three
// properties have to hold for it to be safe to hand a model:
//
//   1. The ids are STABLE. An id derived from what a control says, what kind
//      of thing it is and which section it sits in survives a re-render, a
//      sort, a scroll and a resize. An id derived from a DOM `id` attribute
//      (most controls here have none) or from screen position (which moves the
//      moment the user scrolls) does not. These tests pin the inputs to the
//      hash, and pin what is deliberately absent from it.
//
//   2. Secrets do not leave the screen. Everything read_page returns is sent
//      to the model. A password box, a connection string and a function key
//      are all just <input> to the DOM, so the refusal has to happen here.
//
//   3. It cannot flood the conversation. A page here can carry thousands of
//      nodes; the snapshot is capped, and what was dropped is reported rather
//      than silently lost.
//
// The pure half — the hash, the id, the redaction, the cap, the classifier —
// is tested directly. The DOM half is checked at the source level, and against
// a real browser in tests/smoke (run by hand).
'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 220) : '')); }
};
const P = (...p) => path.join(__dirname, '..', ...p);
const read = (...p) => fs.readFileSync(P(...p), 'utf8');

const R = require('../public/cygenix-page-reader.js');
const C = R.__core;

console.log('Page reader — a generic, honest, bounded view of the screen\n');

/* ── 1. The hash ────────────────────────────────────────────────────────────
   It is implemented in the page rather than through crypto.subtle, which is
   async and absent over plain http. That is only acceptable if it is actually
   SHA-256 — so it is compared against Node's own. */

const cases = ['', 'a', 'Save', 'Save changes|button||Connections',
  'x'.repeat(55), 'x'.repeat(56), 'x'.repeat(57), 'x'.repeat(64), 'x'.repeat(120),
  'Ünïcøde — em dash and £', '🙂 astral plane'];
const wrong = cases.filter((s) => C.sha256Hex(s) !== crypto.createHash('sha256').update(s, 'utf8').digest('hex'));
check('the in-page sha256 agrees with Node\'s, including at every padding boundary',
  wrong.length === 0, wrong.map((s) => s.slice(0, 24)).join(' | '));
check('and agrees on multibyte and astral characters',
  C.sha256Hex('🙂 astral plane') === crypto.createHash('sha256').update('🙂 astral plane').digest('hex'));

/* ── 2. The element id ──────────────────────────────────────────────────── */

check('an id is el_ plus eight hex digits', /^el_[0-9a-f]{8}$/.test(C.idFor('Save', 'button', '', 'Connections')));
check('the same control gets the same id every time',
  C.idFor('Save', 'button', '', 'Connections') === C.idFor('Save', 'button', '', 'Connections'));
check('a different label is a different id',
  C.idFor('Save', 'button', '', 'Connections') !== C.idFor('Delete', 'button', '', 'Connections'));
check('the same label in a different section is a different id',
  C.idFor('Save', 'button', '', 'Connections') !== C.idFor('Save', 'button', '', 'Profiles'));
check('the same label on a different kind of control is a different id',
  C.idFor('Search', 'button', '', 'Jobs') !== C.idFor('Search', 'input', 'text', 'Jobs'));
check('case and surrounding whitespace do not change the id',
  C.idFor(' SAVE ', 'BUTTON', '', 'Connections') === C.idFor('save', 'button', '', 'connections'),
  'a heading re-cased in a redesign must not renumber every control under it');

// The two things the id deliberately does NOT depend on. Both were rejected on
// purpose: most controls in this console carry no DOM id at all, and a
// position changes the moment the user scrolls or the window is resized — an
// id that moves points at the wrong button a second later.
const src = read('public', 'cygenix-page-reader.js');
check('the id ignores the DOM id attribute',
  !/idFor\([^)]*getAttribute\(['"]id['"]\)/.test(src) && /idFor\(label, tag, type, section\)/.test(src));
check('the id ignores screen position',
  !/getBoundingClientRect|offsetTop|offsetLeft|scrollY|pageYOffset/.test(src),
  'nothing positional may reach the hash');
check('duplicates under one heading are separated in document order, not by position',
  /counts\[base\] === 1 \? base : base \+ '_' \+ counts\[base\]/.test(src));

/* ── 3. Secrets stay on the screen ──────────────────────────────────────── */

check('a password field never reports its value',
  C.redactValue('password', 'p', 'Password', 'hunter2').indexOf('hunter2') === -1
  && /hidden/.test(C.redactValue('password', 'p', 'Password', 'hunter2')));
const named = ['dbPassword', 'apiKey', 'api_key', 'fnKey', 'connString', 'connectionString',
  'secret', 'accessToken', 'sasUrl', 'credential', 'Authorization'];
const leaked = named.filter((n) => (C.redactValue('text', n, '', 'VALUE-abc-123') || '').indexOf('VALUE-abc-123') !== -1);
check('a field named for a credential never reports its value either', leaked.length === 0, leaked.join(', '));
check('a field labelled for a credential is caught even when its name is not',
  (C.redactValue('text', 'f1', 'Function key', 'abc123') || '').indexOf('abc123') === -1);
check('a value shaped like a connection string is withheld whatever the field is called',
  (C.redactValue('text', 'notes', 'Notes', 'Server=prod01;User Id=sa;Password=SuperSecret1;') || '')
    .indexOf('SuperSecret1') === -1);
check('a value shaped like a bearer token is withheld too',
  (C.redactValue('text', 'notes', 'Notes', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9abcdef') || '')
    .indexOf('eyJhbGciOi') === -1);
check('an ordinary field still reports its value — this is a reader, not a blindfold',
  C.redactValue('text', 'tableName', 'Table name', 'dbo.customers') === 'dbo.customers');
check('an empty field reports nothing at all', C.redactValue('text', 'x', 'x', '') === null);
check('a long value is clamped rather than sent whole',
  C.redactValue('text', 'x', 'x', 'y'.repeat(500)).length <= C.LIMITS.MAX_VALUE);
check('the field itself is still listed when its value is withheld — the assistant ' +
  'needs to know the box exists', /hidden — this field holds a credential/.test(src));

/* ── 4. Classification ──────────────────────────────────────────────────── */

const kinds = [
  ['button', '', '', 'button'],
  ['a', '', '', 'link'],
  ['input', 'text', '', 'input'],
  ['input', 'checkbox', '', 'input'],
  ['input', 'submit', '', 'button'],
  ['input', 'button', '', 'button'],
  ['select', '', '', 'select'],
  ['textarea', '', '', 'textarea'],
  ['div', '', 'button', 'button'],
  ['span', '', 'link', 'link'],
  ['li', '', 'tab', 'button'],
];
const misread = kinds.filter(([tag, type, role, want]) => C.kindFor(tag, type, role) !== want);
check('every operable thing lands in one of the five kinds the model reasons about',
  misread.length === 0, misread.map((k) => k.join('/')).join(' | '));
check('an ARIA role beats the tag it is written on', C.kindFor('div', '', 'button') === 'button');

/* ── 5. The route matches the rest of the console ───────────────────────── */

check('the route drops the extension', C.routeOf('/sql-editor.html') === '/sql-editor');
check('the route drops a trailing slash', C.routeOf('/projects/') === '/projects');
check('the root stays the root', C.routeOf('/') === '/' && C.routeOf('') === '/');

/* ── 6. The size cap ────────────────────────────────────────────────────── */

const big = {
  page_title: 'Object Mapping',
  current_route: '/object-mapping',
  landmarks: Array.from({ length: 60 }, (_, i) => ({ heading: 'Section ' + i, element_id: 'el_0000000' + (i % 10) })),
  interactive_elements: Array.from({ length: 900 }, (_, i) => ({
    id: 'el_abcdef' + String(i).padStart(2, '0'), kind: 'button',
    label: 'Edit mapping for customers column ' + i, section: 'Mappings'
  }))
};
const fitted = C.fit(big);
check('a huge screen is cut down to fit the conversation',
  JSON.stringify(fitted).length <= C.LIMITS.MAX_CHARS, JSON.stringify(fitted).length);
check('the cap is between 4 and 6 KB, as specified',
  C.LIMITS.MAX_CHARS >= 4000 && C.LIMITS.MAX_CHARS <= 6000, C.LIMITS.MAX_CHARS);
check('what was dropped is reported, never silently lost',
  fitted.truncated > 0 && /were not listed/.test(fitted.note || ''));
check('the top of the screen survives the cut — the user is looking at it',
  fitted.interactive_elements[0].id === big.interactive_elements[0].id);
check('the headings survive too, so the model still knows the shape of the page',
  fitted.landmarks.length > 0 && fitted.landmarks.length <= 25);
const small = C.fit({ page_title: 'x', current_route: '/x', landmarks: [], interactive_elements: [
  { id: 'el_00000001', kind: 'button', label: 'Save' }] });
check('a small screen is returned whole, with no truncation noise',
  small.interactive_elements.length === 1 && small.truncated === undefined && small.note === undefined);

/* ── 7. What the DOM half refuses to look at ────────────────────────────── */

check('hidden elements are skipped', /display === 'none'/.test(src) && /visibility === 'hidden'/.test(src));
check('a fixed-position control is NOT mistaken for a hidden one',
  /cs\.position !== 'fixed'/.test(src),
  'offsetParent is null for position:fixed, which is how every toolbar here is anchored');
check('input[type=hidden] never appears', /String\(node\.type\)\.toLowerCase\(\) === 'hidden'/.test(src));
check('aria-hidden subtrees are skipped', /aria-hidden/.test(src));
check('the assistant does not read its own panel',
  /#cygAssistant/.test(src) && /inOwnPanel/.test(src),
  'listing its own Send button is how an agent ends up talking to itself');
const nameFn = src.slice(src.indexOf('function accessibleName'), src.indexOf('function sectionOf'));
check('the label prefers what a person reads over the markup',
  nameFn.indexOf('aria-label') < nameFn.indexOf('placeholder')
  && nameFn.indexOf('placeholder') < nameFn.indexOf("attr(node, 'name')")
  && /label\[for=/.test(nameFn),
  'aria-label, then the <label>, then the placeholder, and the raw field name last');
check('a disabled control is reported as disabled rather than offered as clickable',
  /if \(node\.disabled\) entry\.disabled = true/.test(src));
check('the reader records what it showed, for whatever operates on it later',
  /function lastRead/.test(src) && /last = \{/.test(src));
check('nothing in the reader clicks, types or submits',
  !/\.click\(\s*\)|\.submit\(\s*\)|dispatchEvent|\.focus\(\s*\)/.test(src),
  'read_page reads; acting is a separate step that has not shipped');

/* ── 8. Registration in the assistant ───────────────────────────────────── */

const store = {};
global.localStorage = {
  getItem: (k) => (Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null),
  setItem: (k, v) => { store[k] = String(v); },
  removeItem: (k) => { delete store[k]; },
  key: (i) => Object.keys(store)[i] || null,
  get length() { return Object.keys(store).length; },
};
global.sessionStorage = { getItem: () => null, setItem: () => {}, removeItem: () => {} };

const A = require('../public/cygenix-assistant.js');
const env = {};
require('../public/cygenix-assistant-actions.js').register(A, env);
const acts = A.getActions();

check('read_page is registered as an action', !!acts['read_page']);
check('read_page is a read, so no guardrail policy pauses it', acts['read_page'].effect === 'read');
check('read_page takes no arguments — it describes the screen as it is',
  Object.keys(acts['read_page'].input_schema.properties || {}).length === 0);
check('its name is a legal Anthropic tool name', /^[a-zA-Z0-9_-]{1,64}$/.test('read_page'));
check('the trail marks a screen read with its own symbol',
  acts['read_page'].icon === '◉' && acts['read_page'].title === 'Reading page');
check('the description tells the model it cannot click or type with it',
  /cannot click, type or submit/.test(acts['read_page'].description));
check('the description sends the model to a typed action first where one exists',
  /Prefer a typed action/.test(acts['read_page'].description));
check('the description warns that the ids expire with the screen',
  /valid only for the screen/.test(acts['read_page'].description));

let unloaded = null;
acts['read_page'].handler({}).then(() => { unloaded = 'resolved'; }, (e) => { unloaded = e.message; });

/* ── 9. The runtime: the prompt, the brakes, the wiring ─────────────────── */

const runtime = read('public', 'cygenix-assistant.js');
const prompt = A.__core.buildSystemPrompt({ page: 'dashboard' }, A.appMap);

check('the prompt tells the model what read_page is for', /SEEING THE SCREEN/.test(prompt));
check('the prompt tells it to say what it is about to do, in plain English, first',
  /plain English, BEFORE you do it/.test(prompt) && /open the Connections page/.test(prompt));
check('the prompt keeps typed actions ahead of the generic reader',
  /Prefer the typed action, every time/.test(prompt));
check('the prompt says destructive work stays with the user',
  /Anything destructive[\s\S]{0,120}stays\s*\n?with the user/.test(prompt.replace(/\n/g, '\n')));
check('the prompt tells it to re-read after the screen changes', /read again rather than/.test(prompt));
check('the prompt tells it to stop after two fruitless reads', /stop and ask the user/.test(prompt));

check('a run is capped at fifteen tool calls per turn',
  /MAX_TOOL_CALLS = 15;/.test(runtime)
  && /'Task exceeded ' \+ MAX_TOOL_CALLS \+ ' tool calls/.test(runtime)
  && /state\.calls >= MAX_TOOL_CALLS/.test(runtime));
check('the budget resets when the user asks something new',
  /state\.calls = 0/.test(runtime) && /per user turn/.test(runtime));
check('the same tool with the same arguments twice in a row breaks the run',
  /callSignature/.test(runtime) && /sig === state\.lastCall/.test(runtime)
  && /Detected a loop/.test(runtime));
check('breaking out still answers every outstanding tool call, so the transcript stays valid',
  /function breakOut/.test(runtime) && /results\.push\(toolResult\(tu\.id, reason, true\)\)/.test(runtime));
check('breaking out does not send the results back for another turn',
  /another turn is the thing being prevented/.test(runtime)
  && !/breakOut[\s\S]{0,400}sendResults/.test(runtime));

/* Every page that carries the assistant must carry the reader, or read_page
   fails on some screens and not others — which is worse than not having it. */
const appPages = fs.readdirSync(P('public')).filter((f) =>
  f.endsWith('.html') && /cygenix-assistant-actions\.js/.test(read('public', f)));
const noReader = appPages.filter((f) => !/<script src="\/cygenix-page-reader\.js/.test(read('public', f)));
check('every page with the assistant also loads the reader',
  appPages.length >= 20 && noReader.length === 0, noReader.join(', '));
const badOrder = appPages.filter((f) => {
  const s = read('public', f);
  return s.indexOf('cygenix-page-reader.js') > s.indexOf('cygenix-assistant.js');
});
check('the reader loads before the runtime that uses it', badOrder.length === 0, badOrder.join(', '));
for (const f of ['login.html', 'index.html', 'register.html', 'pricing.html']) {
  check(f + ' does not load the reader', !/cygenix-page-reader\.js/.test(read('public', f)));
}

setTimeout(() => {
  check('without the reader loaded, read_page says so instead of inventing a screen',
    typeof unloaded === 'string' && /not loaded on this page/.test(unloaded), unloaded);
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
}, 0);
