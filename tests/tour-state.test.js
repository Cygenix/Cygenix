// tests/tour-state.test.js — tour position, and the bug that lost it.
//
// ── The bug, in one paragraph ─────────────────────────────────────────────
//
// A user on step 5 (Connections) typed "can you connect and test these
// connections". The assistant answered; the tour card came back showing step
// 4 (Project). The interruption was not the cause. The cause was that a
// step's identity was its INDEX into a list recomputed from the live DOM:
//
//     liveSteps() = STEPS.filter(stepAvailable)    // queries the document
//     stepAt(i)   = liveSteps()[i]
//
// stepAvailable() asks whether a step's target exists and has a non-zero box.
// That answer changes for reasons unrelated to the tour — a sidebar group
// collapsing, the assistant's own targets coming and going, a nav item that
// appears once roles resolve — and every one of them renumbers the itinerary
// underneath a stored integer. show() then made it worse by clamping the
// stored index to the new length, letting a re-render REWIND the user.
//
// The fix is that the itinerary is frozen at start and position is a stepId.
// The tests below are mostly about proving that nothing except a transition
// can move it.

'use strict';

const T = require('../public/cygenix-tour-state.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

console.log('Tour state — position, transitions and input routing\n');

// A plan shaped like the real one: intro, then the sections the brief names.
const PLAN = ['intro', 'home', 'files', 'search', 'project', 'connections',
  'schema', 'mapping', 'validate', 'run', 'reports', 'finish'];

section('1. Creation');

const fresh = T.create(PLAN, 0);
check('a new tour starts at the intro', fresh.stepIndex === 0 && fresh.stepId === 'intro');
check('it is active', fresh.status === 'active');
check('it carries the tour id and version, so a later change can be detected',
  fresh.tourId === 'welcome-v1' && fresh.tourVersion === 1);
check('and the frozen itinerary', fresh.plan.length === PLAN.length);
check('the plan is a copy — a caller mutating theirs cannot renumber a running tour',
  (() => { const p = PLAN.slice(); const st = T.create(p, 0); p.length = 2; return st.plan.length === PLAN.length; })());
check('an out-of-range start is clamped rather than producing a null step',
  T.create(PLAN, 999).stepIndex === PLAN.length - 1);
check('a negative start is clamped too', T.create(PLAN, -5).stepIndex === 0);
check('an empty plan does not throw', T.create([], 0).stepIndex === 0);
check('updatedAt is an ISO timestamp', !isNaN(Date.parse(fresh.updatedAt)));

section('2. Transitions move the step; nothing else may');

let st = T.create(PLAN, 5);
check('the fixture is on step 5 — Connections', st.stepId === 'connections');

check('next advances one', T.next(st).stepIndex === 6);
check('and updates the id with it', T.next(st).stepId === 'schema');
check('back goes one the other way', T.back(st).stepIndex === 4);
check('back at the intro does nothing rather than going negative',
  T.back(T.create(PLAN, 0)).stepIndex === 0);
check('next on the last step completes the tour',
  T.next(T.create(PLAN, PLAN.length - 1)).status === 'completed');
check('jump moves to a named position', T.jump(st, 2).stepId === 'files');

// THE regression. Every one of these is something that happened around the
// user in the reported session. None of them is a transition, so none of them
// may move the step.
section('3. THE 5 → 4 REGRESSION');

const at5 = T.create(PLAN, 5);
check('baseline: the state is on connections, step 5',
  at5.stepIndex === 5 && at5.stepId === 'connections');

const paused = T.pause(at5, 'question');
check('pausing for a question does not move the step',
  paused.stepIndex === 5 && paused.stepId === 'connections');
check('it only changes status', paused.status === 'paused');
check('and records why', paused.pausedReason === 'question');

const pausedForAction = T.pause(at5, 'action');
check('pausing for an agent action does not move the step either',
  pausedForAction.stepIndex === 5 && pausedForAction.stepId === 'connections');

const pausedByNav = T.pause(at5, 'navigation');
check('nor does a navigation away from the page',
  pausedByNav.stepIndex === 5 && pausedByNav.stepId === 'connections');

const pausedByClose = T.pause(at5, 'closed');
check('nor closing the panel',
  pausedByClose.stepIndex === 5 && pausedByClose.stepId === 'connections');

const resumed = T.resume(paused);
check('RESUMING RETURNS TO 5 — not 4, and not 6',
  resumed.stepIndex === 5 && resumed.stepId === 'connections');
check('and it is active again', resumed.status === 'active');

// Several questions in a row, which is the case the brief calls out.
let multi = T.pause(at5, 'question');
for (let i = 0; i < 5; i++) multi = T.resume(T.pause(T.resume(multi), 'question'));
check('five questions in a row still resume at 5',
  T.resume(multi).stepIndex === 5 && T.resume(multi).stepId === 'connections');

// Pause is idempotent and resume is not a way to advance.
check('pausing twice does not double-move', T.pause(T.pause(at5, 'question'), 'question').stepIndex === 5);
check('resuming an active tour is a no-op, not a step forward',
  T.resume(at5).stepIndex === 5 && T.resume(at5) === at5);
check('resuming twice does not advance',
  T.resume(T.resume(T.pause(at5, 'question'))).stepIndex === 5);

// The old clamp: the shipped code did `if (i >= live.length) i = live.length-1`
// against a DOM-derived length. Nothing here consults a live list at all, so
// a shrinking itinerary cannot rewind anybody — the plan is frozen.
check('the plan does not shrink when the DOM does — it is frozen at start',
  T.pause(at5, 'question').plan.length === PLAN.length);

section('4. Routing: commands are local, everything else is a question');

const R = (t, s) => T.route(t, s).kind;

check('Y advances', R('Y') === 'next');
check('y advances', R('y') === 'next');
check('yes advances', R('yes') === 'next');
check('continue advances', R('continue') === 'next');
check('next advances', R('next') === 'next');
check('whitespace around a command is ignored', R('  Y  ') === 'next');
check('case is ignored', R('YES') === 'next');
check('an empty submit advances, as it always did', R('') === 'next');

check('B goes back', R('B') === 'back');
check('back goes back', R('back') === 'back');
check('previous goes back', R('previous') === 'back');

check('Exit ends it', R('Exit') === 'exit');
check('stop ends it', R('stop') === 'exit');
check('quit ends it', R('quit') === 'exit');

check('go to connections is a jump', R('go to connections') === 'jump');
check('and carries the term', T.route('go to connections').term === 'connections');

// The heart of Change 1: only EXACT matches are commands.
check('"yes, but what does this page do?" is a QUESTION, not a Y',
  R('yes, but what does this page do?') === 'ask');
check('"can you connect and test these connections" is a question',
  R('can you connect and test these connections') === 'ask');
check('"tell me more about this" is a question', R('tell me more about this') === 'ask');
check('"what is a 1:N relationship?" is a question', R("what's a 1:N relationship?") === 'ask');
check('"backup the database" is not a Back',
  R('backup the database') === 'ask');
check('"next steps?" is not a Next', R('next steps?') === 'ask');
check('"no thanks, carry on" is not an Exit', R('no thanks, carry on') === 'ask');
check('a question keeps its original casing for the model',
  T.route('Tell Me More').text === 'Tell Me More');

section('5. Routing while PAUSED is deliberately narrower');
//
// The same word means different things in the two states. While the tour is
// running, "no" stops it. While it is paused mid-conversation, "no" is the
// user answering the assistant — and routing that to exit would end the tour
// for saying no to a question.

check('while active, "no" exits', R('no') === 'exit');
check('while PAUSED, "no" is a reply to the assistant, not an exit',
  R('no', 'paused') === 'ask');
check('while paused, Y resumes rather than advancing', R('y', 'paused') === 'resume');
check('resume resumes', R('resume', 'paused') === 'resume');
check('exit still exits while paused', R('exit', 'paused') === 'exit');
check('a stray Enter while reading an answer does nothing at all',
  R('', 'paused') === 'noop');
check('and a follow-up question is still a question', R('and what about PROD?', 'paused') === 'ask');

check('isCommand agrees with route',
  T.isCommand('y') && T.isCommand('exit') && !T.isCommand('tell me more'));

section('6. Versioning: a saved position survives the steps changing');

const byId = {};
PLAN.forEach((id) => { byId[id] = { id }; });

let res = T.resolve(T.create(PLAN, 5), byId);
check('a saved step that still exists resolves to itself',
  res.ok && res.index === 5 && res.stepId === 'connections');

// The index drifted but the id is good — the id wins, which is the whole
// reason stepId is stored alongside it.
const drifted = Object.assign(T.create(PLAN, 5), { stepIndex: 2 });
res = T.resolve(drifted, byId);
check('when the index and the id disagree, the ID wins',
  res.ok && res.index === 5 && res.moved === true);

// A step removed in a later version of the tour.
const shrunk = Object.assign({}, byId);
delete shrunk.connections;
res = T.resolve(T.create(PLAN, 5), shrunk);
check('a saved step that no longer exists falls back rather than crashing', res.ok);
check('to a neighbour, not to the beginning', res.index === 6 && res.fellBack === true);

const onlyIntro = { intro: { id: 'intro' } };
res = T.resolve(T.create(PLAN, 5), onlyIntro);
check('when almost everything is gone it finds what is left',
  res.ok && res.stepId === 'intro');

res = T.resolve(T.create(PLAN, 5), {});
check('when nothing in the plan survives it says so instead of guessing',
  !res.ok && /no step/.test(res.reason));
check('an empty plan resolves to a clean failure', !T.resolve(T.create([], 0), byId).ok);

section('7. Status');

check('exit marks the tour exited', T.exit(at5).status === 'exited');
check('exiting does not pretend the step moved', T.exit(at5).stepIndex === 5);
check('completing lands on the last step',
  T.complete(at5).status === 'completed' && T.complete(at5).stepId === 'finish');
check('an exited tour is not runnable', !T.isRunnable(T.exit(at5)));
check('a completed tour is not runnable', !T.isRunnable(T.complete(at5)));
check('an active one is', T.isRunnable(at5));
check('a paused one is too — that is the point of the resume prompt',
  T.isRunnable(T.pause(at5, 'question')));
check('next on an exited tour does nothing', T.next(T.exit(at5)).stepIndex === 5);
check('back on a completed tour does nothing',
  T.back(T.complete(at5)).stepId === T.complete(at5).stepId);

check('the denominator comes from the frozen plan, so it cannot wobble',
  T.total(at5) === PLAN.length - 1 && T.total(T.pause(at5, 'question')) === PLAN.length - 1);

section('8. State is never mutated in place');
//
// Every transition returns a new object. A stale closure holding an old state
// — one of the likely causes named in the brief — then cannot corrupt the
// current one; it can only be out of date, which the caller notices.

const original = T.create(PLAN, 5);
const snapshot = JSON.stringify(original);
T.next(original); T.back(original); T.pause(original, 'question');
T.exit(original); T.complete(original); T.jump(original, 1);
check('calling every transition leaves the input untouched',
  JSON.stringify(original) === snapshot);

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
