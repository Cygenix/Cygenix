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
const readFn = src.slice(src.indexOf('function readPage'), src.indexOf('click — the one thing'));
check('reading the page presses nothing',
  !/\.click\(|\.submit\(|dispatchEvent|\.focus\(/.test(readFn),
  'read_page describes; only click() may act, and only under its own rules');
check('nothing submits a form directly — a submit button is a click, with a click\'s rules',
  !/\.submit\(/.test(src));
check('writing to a field happens only in the typing half of the module',
  src.slice(0, src.indexOf('type — putting text into a field')).indexOf('node.value =') === -1
  && /function setValue/.test(src),
  'reading a screen must never change it');

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
  /Destructive work is theirs, not yours/.test(prompt)
  && /let them press it themselves/.test(prompt));
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

/* ── 10. click: the rules, decided in the open ──────────────────────────────
   The consequence of a click is a property of the thing clicked, not of the
   action, so every one of these decisions is made per element and every one of
   them is pure enough to state here. */

const SPEC_WORDS = ['Delete', 'Remove', 'Drop', 'Destroy', 'Confirm', 'Send',
                    'Submit', 'Publish', 'Pay', 'Purchase', 'Archive'];
const notListed = SPEC_WORDS.filter((w) => !C.irreversible('Please ' + w.toUpperCase() + ' it'));
check('every word on the irreversible list is matched, case-insensitively, anywhere in the label',
  notListed.length === 0, notListed.join(', '));
check('an ordinary label is not on the list',
  !C.irreversible('Save mapping') && !C.irreversible('Run preflight') && !C.irreversible('Open'));
check('the list is exactly the eleven words specified',
  C.IRREVERSIBLE.length === 11 && C.IRREVERSIBLE.join(',') === SPEC_WORDS.join(',').toLowerCase(),
  C.IRREVERSIBLE.join(','));

check('a field is safe to focus without asking', C.safeToClick({ kind: 'input', label: 'Filter tables' }));
check('a dropdown and a text area are too',
  C.safeToClick({ kind: 'select', label: 'Mode' }) && C.safeToClick({ kind: 'textarea', label: 'Notes' }));
check('a link within the app is safe', C.safeToClick({ kind: 'link', label: 'Schema Explorer' }, true));
check('a link leaving the app is NOT', !C.safeToClick({ kind: 'link', label: 'Docs' }, false));
check('a search or filter button is safe',
  ['Search', 'Find a table', 'Filter', 'Refresh', 'Sort by name', 'Clear filters']
    .every((l) => C.safeToClick({ kind: 'button', label: l })), 'the specified safe set');
check('every other button is not',
  ['Save', 'Run job', 'Create connection', 'Start migration', 'Cancel', 'Close']
    .every((l) => !C.safeToClick({ kind: 'button', label: l })),
  'Cancel and Close are deliberately outside the safe set — cancelling can throw work away');
check('an irreversible label is never safe, whatever kind of control it is on',
  !C.safeToClick({ kind: 'input', label: 'Delete reason' })
  && !C.safeToClick({ kind: 'link', label: 'Remove this project' }, true)
  && !C.safeToClick({ kind: 'button', label: 'Search and destroy' }));

const fresh = 5000, stale = 45000;
check('an irreversible element always confirms, even fresh and even as a link',
  C.decideConfirm({ kind: 'link', label: 'Delete project' }, fresh, true).confirm === true);
check('and the reason names the word that put it there',
  /"delete"/.test(C.decideConfirm({ kind: 'button', label: 'Delete row' }, fresh, false).reason)
  && /irreversible-action list/.test(C.decideConfirm({ kind: 'button', label: 'Delete row' }, fresh, false).reason));
check('an ordinary button confirms', C.decideConfirm({ kind: 'button', label: 'Save' }, fresh, false).confirm === true);
check('a safe element read within thirty seconds does not',
  C.decideConfirm({ kind: 'button', label: 'Search' }, fresh, false).confirm === false);
check('the same element confirms once the read is over thirty seconds old',
  C.decideConfirm({ kind: 'button', label: 'Search' }, stale, false).confirm === true);
check('and says how old the read was', /45 seconds ago/.test(
  C.decideConfirm({ kind: 'button', label: 'Search' }, stale, false).reason));
check('the skip window is thirty seconds and the refusal is sixty',
  C.LIMITS.CLICK_SKIP_MS === 30000 && C.LIMITS.CLICK_MAX_AGE_MS === 60000);

/* The refusals, in the reader's source: each one has to tell the model what to
   do next, because a refusal it cannot act on is just a dead end. */
check('an id from no read at all is refused', /no read_page result to work from/.test(src));
check('a read over the age limit is refused with the age', /over the ' \+ \(CLICK_MAX_AGE_MS \/ 1000\)/.test(src));
check('an id that was not in the last read is refused', /was not in the last read_page result/.test(src));
check('a read taken on another screen is refused', /The screen has changed since that read_page/.test(src));
check('a vanished element gets exactly the message the specification asks for',
  /'Element ' \+ id \+ ' no longer exists; call read_page again'/.test(src));
check('a recycled list row counts as vanished — node identity is not enough',
  /liveLabel !== entry\.label \|\| sectionOf\(node\) !== \(entry\.section/.test(src),
  'a virtualised list can turn "Delete row 1" into "Delete row 500" behind the same node');
check('a disabled control is refused rather than clicked at',
  /is disabled and cannot be/.test(src) && /aria-disabled/.test(src));
check('an in-app link hands the navigation to the runtime rather than following it',
  /entry\.kind === 'link' && r\.inApp && r\.url/.test(src) && /__navigate: r\.url/.test(src),
  'the runtime persists a run across a navigation; the browser following a link would drop it');
check('a link that leaves the app or opens a tab is not treated as in-app navigation',
  /u\.origin !== win\.location\.origin/.test(src) && /target.*!== '_self'/.test(src));
check('the confirmation question can be answered without touching anything',
  /function clickConfirmation/.test(src) && !/clickConfirmation[\s\S]{0,400}node\.click/.test(src));
check('an unresolvable id declines to confirm and lets the click throw the real reason',
  /catch \(e\) \{ return \{ confirm: false, reason: e\.message/.test(src));

const clickAct = acts['click'];
check('click is registered', !!clickAct);
check('click takes an element id and requires it',
  clickAct.input_schema.required.join(',') === 'element_id'
  && !!clickAct.input_schema.properties.element_id);
check('click asks for a plain-English reason to show the user',
  !!clickAct.input_schema.properties.reason);
check('click is a write, so the default policy holds it', clickAct.effect === 'write');
check('click asks per element, not per action', typeof clickAct.confirms === 'function');
check('with no reader loaded, the confirmation hook fails closed',
  clickAct.confirms({ element_id: 'el_deadbeef' }) === true,
  'anything it cannot resolve must confirm');
check('the dialog asks the question in the words the specification gives',
  clickAct.confirmTitle({ element_id: 'el_deadbeef' }) ===
    'Assistant wants to click "el_deadbeef". Proceed?');
check('the trail row names what was pressed',
  /^Clicked: /.test(clickAct.trailTitle({ element_id: 'el_deadbeef' })));
check('two clicks of the same element with no read between them is a loop',
  clickAct.repeatGuard({ element_id: 'el_1' }) === 'click:el_1'
  && clickAct.repeatGuard({ element_id: 'el_1' }) === clickAct.repeatGuard({ element_id: 'el_1' })
  && clickAct.repeatGuard({ element_id: 'el_2' }) !== clickAct.repeatGuard({ element_id: 'el_1' }));
check('reading the screen clears that guard', acts['read_page'].clearsRepeatGuard === true);
check('click tells the model to read again afterwards',
  /read_page again rather than trusting/.test(clickAct.description));
check('click tells the model not to retry a declined press',
  /do not try again/.test(clickAct.description));

let unloadedClick = null;
clickAct.handler({ element_id: 'el_1' }).then(() => { unloadedClick = 'resolved'; },
  (e) => { unloadedClick = e.message; });

/* The runtime side: an action may raise a confirmation, and may only clear one
   the policy was not holding for its own reasons. */
const nc = A.__core.needsConfirmation;
check('an action can raise a read to a confirmation',
  nc({ effect: 'read', confirms: () => true }, 'confirm_destructive') === true);
check('an action can clear a confirmation the policy would have made',
  nc({ effect: 'write', confirms: () => false }, 'confirm_all') === false);
check('but it cannot clear one on a destructive action — the policy is the floor',
  nc({ effect: 'destructive', confirms: () => false }, 'confirm_all') === true);
check('an action that answers neither way leaves the policy to decide',
  nc({ effect: 'write', confirms: () => null }, 'confirm_all') === true
  && nc({ effect: 'write', confirms: () => null }, 'confirm_destructive') === false);
check('a hook that throws confirms', nc({ effect: 'read', confirms: () => { throw new Error('x'); } }, 'confirm_all') === true);
check('an action with no hook behaves exactly as before',
  nc({ effect: 'write' }, 'confirm_all') === true && nc({ effect: 'write' }, 'confirm_destructive') === false);

check('the panel puts the action\'s own question at the top of the dialog',
  /a\.confirmTitle \? a\.confirmTitle\(state\.pending\.input\)/.test(runtime));
check('the audit records whether a confirmation actually happened, not what it would be now',
  /confirmed: !!confirmed/.test(runtime) && /execute\(tu, action, false\)/.test(runtime)
  && /actions\[p\.name\], true\)/.test(runtime),
  'asking after the fact would re-ask a question whose element has since gone');
check('an unanswered tool call left by a page reload is answered on the next load',
  /function healTranscript/.test(runtime) && /reloaded before this finished/.test(runtime)
  && /Do not assume it ran/.test(runtime));
check('a run parked on a confirmation is not "healed" out from under itself',
  /if \(state\.pending \|\| state\.resume\) return;/.test(runtime));

check('the prompt gives the read-say-click-read shape', /PRESSING SOMETHING/.test(prompt)
  && /read_page again, because the screen has probably changed/.test(prompt));
check('the prompt says a declined click is an answer, not a retry',
  /do not press it again/.test(prompt));
check('the prompt says typing saves nothing on its own',
  /It saves nothing/.test(prompt) && /press once at the end/.test(prompt));
check('the prompt forbids the assistant inventing a credential to type',
  /NEVER invent a password/.test(prompt)
  && /let them enter it themselves/.test(prompt));

/* ── 11. type: the same fences, a narrower target, one exception ─────────── */

check('a text input, a text area and an editable region are text fields', (() => {
  const F = (tag, type, ce) => ({ tagName: tag, type: type,
    getAttribute: (k) => (k === 'contenteditable' ? ce : null) });
  return C.isTextField(F('INPUT', 'text')) && C.isTextField(F('TEXTAREA'))
    && C.isTextField(F('DIV', '', 'true')) && C.isTextField(F('INPUT', 'password'))
    && C.isTextField(F('INPUT', 'email')) && C.isTextField(F('INPUT', 'search'));
})());
check('a checkbox, a radio, a file picker and a button are not', (() => {
  const F = (type) => ({ tagName: 'INPUT', type: type, getAttribute: () => null });
  return ['checkbox', 'radio', 'file', 'button', 'submit', 'reset', 'range', 'color']
    .every((t) => !C.isTextField(F(t)));
})());
check('a file picker especially — its value cannot be set from script by design',
  C.NON_TEXT_INPUT.test('file'));
check('a select and a plain div are not text fields', (() => {
  const F = (tag) => ({ tagName: tag, type: '', getAttribute: () => null });
  return !C.isTextField(F('SELECT')) && !C.isTextField(F('DIV')) && !C.isTextField(F('BUTTON'));
})());

check('typing into a credential field asks first',
  C.decideTypeConfirm({ kind: 'input', label: 'Password' }, true).confirm === true);
check('and says why, without lecturing',
  /holds a credential/.test(C.decideTypeConfirm({ kind: 'input', label: 'Password' }, true).reason)
  && /better typed by you/.test(C.decideTypeConfirm({ kind: 'input', label: 'Password' }, true).reason));
check('typing into an ordinary field does not ask — that is what makes it useful',
  C.decideTypeConfirm({ kind: 'input', label: 'Table name' }, false).confirm === false);
check('a field whose label is on the irreversible list asks too, as click does',
  C.decideTypeConfirm({ kind: 'input', label: 'Type DELETE to confirm' }, false).confirm === true);
check('the credential test is the same one that refuses to READ those fields',
  /function isCredentialField[\s\S]{0,240}isSensitive\(/.test(src),
  'one rule, so a field cannot be unreadable and freely writable at the same time');

check('type replaces rather than appends — it clears first, as a person would',
  /setValue\(node, '', win\);/.test(src) && /setValue\(node, value, win\)/.test(src));
check('it dispatches input and change so framework listeners fire',
  /fire\(node, win, 'input'\)/.test(src) && /fire\(node, win, 'change'\)/.test(src));
check('and goes through the prototype value setter, which a value tracker cannot swallow',
  /Object\.getOwnPropertyDescriptor\(proto, 'value'\)/.test(src));
check('a read-only or disabled field is refused', /is read-only or disabled/.test(src));
check('a non-text element is refused and pointed at click',
  /holds no text\./.test(src) && /use click\./.test(src));
check('an absurdly long value is refused rather than pasted',
  C.LIMITS.MAX_TYPE_CHARS === 10000 && /-character limit for one field/.test(src));
check('the text is never echoed back in the result',
  /Deliberately no echo of the text/.test(src)
  && /characters: value\.length/.test(src)
  && !/typed:\s*value|text: value/.test(src),
  'the model supplied it; repeating it gains nothing and can lose something');

const typeAct = acts['type'];
check('type is registered', !!typeAct);
check('type needs both an element and the text',
  typeAct.input_schema.required.join(',') === 'element_id,text');
check('with no reader loaded, the confirmation hook fails closed',
  typeAct.confirms({ element_id: 'el_deadbeef' }) === true);
check('the dialog names the field it is about to fill in',
  typeAct.confirmTitle({ element_id: 'el_deadbeef' }) ===
    'Assistant wants to type into "el_deadbeef". Proceed?');
check('the trail row names the field',
  /^Typed into: /.test(typeAct.trailTitle({ element_id: 'el_deadbeef' })));
check('the dialog shows the operator exactly what is about to go in',
  typeAct.preview({ element_id: 'el_1', text: 'dbo.customers' }).indexOf('dbo.customers') !== -1,
  'the point of asking is that they can judge it');
check('type tells the model it has saved nothing',
  /does not save or submit anything/.test(typeAct.description));
check('type forbids inventing a credential', /Never invent a password/.test(typeAct.description));

let unloadedType = null;
typeAct.handler({ element_id: 'el_1', text: 'x' }).then(() => { unloadedType = 'resolved'; },
  (e) => { unloadedType = e.message; });

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
  check('and click says so too, rather than pressing something it cannot see',
    typeof unloadedClick === 'string' && /not loaded on this page/.test(unloadedClick), unloadedClick);
  check('and type says so rather than filling in a field it cannot see',
    typeof unloadedType === 'string' && /not loaded on this page/.test(unloadedType), unloadedType);
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
}, 0);
