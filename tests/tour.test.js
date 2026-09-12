// tests/tour.test.js — the guided tour's content and its wiring, without a browser.
//
// tests/browser/tour.smoke.js drives the thing end to end. What it cannot do
// cheaply is check every step against the real nav tree, so that is here: a
// step pointing at a data-key the sidebar does not have would spotlight
// nothing, and the person who broke it would be whoever renamed a nav item
// three months later with no idea this file existed.
//
// Plain Node, no DOM. The steps file and the sidebar's NAV are both
// node-requirable, which is the whole reason they are shaped that way.
'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const P = (...p) => path.join(__dirname, '..', ...p);
const read = (...p) => fs.readFileSync(P(...p), 'utf8');

console.log('Guided tour — every stop points at something that exists\n');

const STEPS = require('../public/cygenix-tour-steps.js');

/* The sidebar under a DOM stub, the same way tests/sidebar-nav.test.js does it. */
const noopEl = () => ({ classList: { add(){}, remove(){}, toggle(){}, contains: () => false },
  addEventListener(){}, setAttribute(){}, querySelector: () => null, querySelectorAll: () => [],
  appendChild(){}, replaceWith(){}, style: {}, dataset: {}, textContent: '' });
const sandbox = {
  window: { addEventListener(){}, location: { pathname: '/x.html', origin: 'https://x' } },
  document: { readyState: 'loading', addEventListener(){}, createElement: noopEl,
    getElementById: () => null, querySelector: () => null, querySelectorAll: () => [],
    head: noopEl(), body: noopEl(), documentElement: noopEl() },
  localStorage: { getItem: () => null, setItem(){}, removeItem(){} },
  sessionStorage: { getItem: () => null, setItem(){}, removeItem(){} },
  console, setTimeout: () => 0, setInterval: () => 0,
};
sandbox.window.document = sandbox.document;
sandbox.window.localStorage = sandbox.localStorage;
vm.createContext(sandbox);
vm.runInContext(read('public', 'cygenix-sidebar.js'), sandbox);
const SB = sandbox.window.CygenixSidebar;

const NAV = SB.__nav;
const navKeys = new Set();
const parentKeys = new Set();
NAV.forEach((sec) => sec.items.forEach((it) => {
  if (it.children) { parentKeys.add(it.key); it.children.forEach((c) => navKeys.add(c.key)); }
  else navKeys.add(it.key);
}));

/* ── 1. The steps are well formed ───────────────────────────────────────── */

check('there are steps, and one intro', STEPS.length > 10 && STEPS[0].section === 'Welcome', STEPS.length);
check('every step has the whole schema',
  STEPS.every((s) => s.id && s.section && s.title && s.body && s.page),
  STEPS.filter((s) => !(s.id && s.section && s.title && s.body && s.page)).map((s) => s.id).join(', '));
check('ids are unique',
  new Set(STEPS.map((s) => s.id)).size === STEPS.length);
check('exactly one step is final, and it is the last',
  STEPS.filter((s) => s.final).length === 1 && !!STEPS[STEPS.length - 1].final);

// The panel is narrow and the card is read, not skimmed. Three sentences was
// the brief; four is where it stops being a caption.
const longOnes = STEPS.filter((s) => (s.body.match(/\.\s|\.$/g) || []).length > 3);
check('no step body runs past three sentences', longOnes.length === 0,
  longOnes.map((s) => s.id).join(', '));
const tagged = STEPS.filter((s) => /<(?!\/?(b|kbd)\b)[a-z]/i.test(s.body));
check('bodies use only <b> and <kbd>', tagged.length === 0, tagged.map((s) => s.id).join(', '));

/* ── 2. Every page and target resolves against the real nav tree ─────────── */

const badPages = STEPS.filter((s) => !navKeys.has(s.page));
check('every step navigates to a key the sidebar actually has',
  badPages.length === 0,
  badPages.map((s) => s.id + ' → ' + s.page).join(', '));

const badTargets = [];
STEPS.forEach((s) => {
  if (!s.target) return;
  let m = s.target.match(/^\[data-key="([^"]+)"\]$/);
  if (m && !navKeys.has(m[1])) return badTargets.push(s.id + ' → ' + s.target);
  m = s.target.match(/^\[data-parent="([^"]+)"\]$/);
  if (m && !parentKeys.has(m[1])) return badTargets.push(s.id + ' → ' + s.target);
});
check('every sidebar target is a key or a group the sidebar emits',
  badTargets.length === 0, badTargets.join(', '));

// A parent-group step has to land on one of that group's own children, or the
// spotlight points at a group the page has nothing to do with.
const mismatched = [];
STEPS.forEach((s) => {
  const m = s.target && s.target.match(/^\[data-parent="([^"]+)"\]$/);
  if (!m) return;
  const group = NAV.reduce((f, sec) => f || sec.items.find((i) => i.key === m[1]), null);
  if (!group) return;
  if (!group.children.some((c) => c.key === s.page)) mismatched.push(s.id + ': ' + s.page + ' is not in ' + m[1]);
});
check('a group step navigates to one of that group\'s own children',
  mismatched.length === 0, mismatched.join(', '));

/* Non-sidebar targets are real selectors in real files, not hopeful guesses. */
const assistantJs = read('public', 'cygenix-assistant.js');
const sidebarJs = read('public', 'cygenix-sidebar.js');
const dashboard = read('public', 'dashboard.html');
const OTHER = { '.cyg-drive-btn': sidebarJs, '.cyga-foot': assistantJs };
const missingSel = STEPS.filter((s) => s.target && OTHER[s.target] !== undefined)
  .filter((s) => OTHER[s.target].indexOf(s.target.replace(/^\./, '')) === -1);
check('the non-sidebar targets exist in the files that render them',
  missingSel.length === 0, missingSel.map((s) => s.target).join(', '));
const regions = STEPS.filter((s) => s.region);
check('every region hook is present in the markup',
  regions.every((s) => dashboard.indexOf(s.region.replace(/^\[|\]$/g, '')) !== -1),
  regions.map((s) => s.region).join(', '));

/* ── 3. The wiring that makes it work without a key ─────────────────────── */

const tourJs = read('public', 'cygenix-tour.js');

check('the tour is intercepted BEFORE the API-key gate',
  /tourHooks\.onInput[\s\S]{0,500}ask\(text,/.test(assistantJs)
  && assistantJs.indexOf('tourHooks.onInput') < assistantJs.indexOf('function ask('),
  'a new user has no key — intercepting after the gate would make the tour unreachable');
check('and the no-key empty state offers it',
  /No API key set[\s\S]{0,600}tourChip\(\)/.test(assistantJs));

check('the engine navigates through the sidebar rather than faking clicks',
  /SB\.navigate\(/.test(tourJs) && !/\.click\(\)/.test(tourJs),
  'handleClick is the one place that knows a view key from a page key');
check('and the sidebar exposes that navigation',
  /navigate:\s*\(key\)/.test(sidebarJs));

// The claim the whole design rests on: this thing cannot change anything.
check('the engine calls no assistant action and writes no app data',
  !/CygenixAssistantAdapters|runAction|registerAction/.test(tourJs)
  && !/localStorage\.setItem\('cygenix_(jobs|projects|inventory)/.test(tourJs),
  'read-only is the reason it is scripted rather than model-driven');
// Comments are allowed to say the word "Anthropic"; code is not allowed to
// call it. Strip the comments before asking.
const tourCode = tourJs.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
check('it makes no network call at all',
  !/fetch\(|XMLHttpRequest|anthropic|api\.anthropic/i.test(tourCode),
  'the whole point is that it runs with no key and no connection');

// The transcript is per-tab; the POSITION outlives the tab and the day. This
// assertion used to pin a bare index in localStorage — that design is what
// the 5 → 4 bug grew out of, so it now pins the replacement instead.
check('the transcript is per-tab and the position is per-user and durable',
  /SESSION_KEY\s*=\s*'cyg_tour'/.test(tourJs)
  && /sessionStorage\.(get|set)Item\(SESSION_KEY/.test(tourJs)
  && /STATE_KEY\s*=\s*'cygenix_tour_state'/.test(tourJs)
  && /localStorage\.setItem\(stateKey\(\)/.test(tourJs)
  && /function userTag\(\)/.test(tourJs),
  'a half-finished tour should not follow you into a new tab, but the step should survive a reload');
check('every transition persists immediately',
  (tourJs.match(/\bpersist\(\);/g) || []).length >= 6,
  'a reload between a transition and its save lands on the wrong step');
check('and the cross-machine gap is written down rather than left as a surprise',
  /TODO \(cross-machine\)/.test(tourJs) && /SYNC_KEYS/.test(tourJs));

// ── The 5 → 4 bug, asserted structurally ─────────────────────────────────
// The behaviour is covered in tests/tour-state.test.js against the pure
// module. What is checked HERE is that the engine actually uses it, because
// the bug was never in the rules — it was in resolving a position against a
// list recomputed from the DOM.
check('the itinerary is frozen once, at start',
  /st: T\.create\(liveSteps\(\)/.test(tourJs),
  'if liveSteps() is consulted again for numbering, the tour can renumber underneath the user');
// Against the comment-stripped source: the comment above the fixed line
// quotes the broken one on purpose, so that a reader knows what changed and
// why. Asking the raw file would fail on the explanation of the fix.
check('the renderer resolves a card by ID, never by index into a live list',
  /var step = stepById\(row\.id\);/.test(tourCode)
  && !/live\[row\.i\]/.test(tourCode),
  'live[row.i] is the exact line that showed step 4 to someone on step 5');
check('each card carries the counter it was drawn with',
  /kind: 'step', id: step\.id, i: index, tot: total\(\)/.test(tourJs),
  'recomputing the denominator lets a card already on screen change what it says');
check('show() no longer clamps the step against a DOM-derived length',
  !/tour\.i >= live\.length/.test(tourJs) && !/tour\.i = live\.length - 1/.test(tourJs),
  'that clamp let a re-render rewind the user');
check('position is only ever written through the state module',
  !/tour\.st\.stepIndex\s*=/.test(tourJs) && !/tour\.st\.stepId\s*=/.test(tourJs),
  'a direct write is a transition nobody can find later');

// ── Interruption ─────────────────────────────────────────────────────────
check('a free-text question pauses the tour before handing over to the model',
  /pauseFor\('question'\);[\s\S]{0,160}return false;/.test(tourJs),
  'pausing first is what makes the position survive an agent action that navigates');
check('and the resume prompt comes back on the assistant\'s turn ending',
  /onTurnEnd:\s*onTurnEnd/.test(tourJs) && /resumePromptHtml\(\)/.test(tourJs));
check('a failed answer still hands the tour back rather than killing it',
  /could not answer that[\s\S]{0,700}pushNote\(resumePromptHtml/.test(tourJs));

/* The answer has to land in THIS transcript, under the question. The panel
   draws the conversation above the tour, so an answer left there sits above
   every card shown so far — ten steps in, off the top of a panel that scrolls
   to the bottom. Three questions, three answers, and a user who reported that
   nothing was responding. */
check('the answer is shown in the tour transcript, not left in the conversation',
  /function onTurnEnd\(status, error, answer\)/.test(tourJs)
  && /pushNote\(esc\(text\)\)/.test(tourJs),
  'onTurnEnd must take the answer and push it into the transcript');
check('and it tells the assistant so, to stop a second copy being drawn above',
  /shown = true;[\s\S]{0,600}return shown;/.test(tourJs));
check('a turn that wrote nothing still says something rather than going silent',
  /finished without a written answer/.test(tourJs));
check('the assistant only hides a message the tour actually claimed',
  /=== true && last\)[\s\S]{0,120}shownByTour = true/.test(assistantJs)
  && /m\.shownByTour && tourHtml/.test(assistantJs),
  'hiding it with no tour transcript on screen would lose the answer entirely');
check('the live rows — thinking, error, the approval card — are drawn below the tour',
  /html \+= tourHtml;[\s\S]{0,400}html \+= live;/.test(assistantJs),
  '"Thinking…" rendered where nobody can see it is the same bug in a hat');
check('resuming redraws the paused step and cannot advance it',
  /function resumeTour\(\)[\s\S]{0,420}T\.resume\(tour\.st\)[\s\S]{0,420}show\(\)/.test(tourJs)
  && !/function resumeTour\(\)[\s\S]{0,420}T\.next/.test(tourJs));
check('mid-tour answers are capped, so a curious question has a predictable cost',
  /TOUR_ANSWER_TOKENS\s*=\s*\d+/.test(tourJs) && /maxTokens:/.test(tourJs));
check('the credits notice is shown once a session, not on every question',
  /CREDIT_KEY/.test(tourJs) && /sessionStorage\.getItem\(CREDIT_KEY\) === '1'/.test(tourJs));
check('with no API key the tour keeps working rather than failing silently',
  /keep going with the tour/.test(tourJs));

// The brief asks for these calls to be wired into "whatever credit metering
// the assistant already uses". There is none — Cygenix holds no Anthropic key
// and every call is billed to the operator's own, which
// tests/anthropic-billing.test.js enforces. So the honest wiring is the
// consequence, not an invented balance: a quota failure arrives through the
// model's own error mapping and the tour survives it.
check('the absence of a credit ledger is stated, not quietly worked around',
  /credit metering/i.test(tourJs) && /anthropic-billing\.test\.js/.test(tourJs));
check('and a failed or quota-exhausted call leaves the tour usable',
  /could not answer that[\s\S]{0,300}keep going with the tour/.test(tourJs));

// ── Coming back ──────────────────────────────────────────────────────────
check('a saved tour is OFFERED, never relaunched over what the user is doing',
  /function offerRestore\(/.test(tourJs)
  && /data-tour-act="resume"[\s\S]{0,200}data-tour-act="restart"[\s\S]{0,200}data-tour-act="dismiss"/.test(tourJs));
check('closing the panel pauses rather than exiting',
  /onClose:[\s\S]{0,320}pauseFor\('closed'\)/.test(tourJs),
  'shutting a drawer is not the same as saying you are finished');
check('an exited tour does not nag — only a runnable one is offered',
  /T\.isRunnable\(durable\)/.test(tourJs));
check('the model is told which step the user is on, so "tell me more" works',
  /tourGuidance/.test(tourJs) && /registerContext/.test(tourJs));
check('a stale highlight from an earlier step cannot land',
  /hiToken/.test(tourJs) && /tok !== hiToken/.test(tourJs));
check('reduced motion is honoured',
  /prefers-reduced-motion/.test(tourJs));
check('the sidebar is put back the way it was found',
  /restore[\s\S]{0,200}collapsed/.test(tourJs) && /collapseTourGroups/.test(tourJs));

/* ── 4. Loaded everywhere the assistant is ──────────────────────────────── */

const pages = fs.readdirSync(P('public')).filter((f) => f.endsWith('.html'));
const appPages = pages.filter((f) => /cygenix-assistant-actions\.js/.test(read('public', f)));
const noTour = appPages.filter((f) => !/cygenix-tour\.js/.test(read('public', f)));
check('every page with the assistant also loads the tour (' + appPages.length + ' pages)',
  noTour.length === 0, noTour.join(', '));
const wrongOrder = appPages.filter((f) => {
  const s = read('public', f);
  return s.indexOf('cygenix-tour-steps.js') > s.indexOf('cygenix-tour.js');
});
check('and the steps load before the engine that reads them',
  wrongOrder.length === 0, wrongOrder.join(', '));
const marketing = pages.filter((f) => !appPages.includes(f) && /cygenix-tour\.js/.test(read('public', f)));
check('and it is not dragged onto the marketing pages',
  marketing.length === 0, marketing.join(', '));

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
