// tests/login-busy.test.js — the sign-in page must never look hung.
//
// Reported: "/login takes a while to ask for credentials or show the welcome
// back message… it currently looks like it is broken with no activity."
//
// It was. Two faults stacked, and each on its own was enough:
//
//   1. #loading lived INSIDE #main-card, and startup set #main-card to
//      display:none. Setting #loading to display:block after that does
//      nothing — a hidden ancestor wins. So the whole form panel rendered
//      empty for the entire wait.
//   2. .spinner was `border: 2px solid rgba(255,255,255,.3)` with a #fff top
//      edge, drawn on #loading's var(--panel) background, which is #ffffff.
//      White on white. Invisible even if fault 1 had not existed.
//
// The wait is real and not short: MSAL from a CDN, then
// handleRedirectPromise, then possibly acquireTokenSilent against
// cygenix.ciamlogin.com. Seconds, on a cold visit.
//
// These checks are structural on purpose. A screenshot test would pass with
// the spinner one pixel outside the viewport; what actually broke here was
// the containment relationship and a colour, so those are what get pinned.
// The behaviour over time is in tests/browser/login-busy.smoke.js.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

console.log('Sign-in busy state — working must not look like broken\n');

const PAGE = read('public', 'login.html');
const BUSY = read('public', 'cygenix-busy.js');

/* ── 1. The containment fault ───────────────────────────────────────────── */
//
// The rule that was broken: the element that says "this is working" must not
// sit inside the element the busy state hides. #signin-ui is what gets
// hidden now; #loading is its sibling.

const SIGNIN = PAGE.slice(PAGE.indexOf('<div id="signin-ui">'), PAGE.indexOf('<!-- /#signin-ui -->'));
check('the sign-in controls are wrapped in one element the busy state can hide',
  SIGNIN.length > 200 && /id="google-btn"/.test(SIGNIN) && /id="email-btn"/.test(SIGNIN),
  'signin-ui: ' + SIGNIN.length + ' chars');
check('and #loading is NOT inside it — the exact fault that left the panel empty',
  !/id="loading"/.test(SIGNIN),
  'if the loader is inside the thing being hidden, it can never render');
check('the busy state hides the controls and shows the loader, rather than hiding the card',
  /\.js #main-card\.is-busy #signin-ui\{display:none\}/.test(PAGE)
  && /\.js #main-card\.is-busy #loading\{display:block\}/.test(PAGE)
  && !/getElementById\('main-card'\)\.style\.display = 'none'/.test(PAGE));

/* ── 2. The colour fault ────────────────────────────────────────────────── */
//
// #loading's background is var(--panel) === #ffffff. A spinner painted in
// white on it is not a spinner.

const SPIN = (/\.spinner\{([^}]*)\}/.exec(PAGE) || ['', ''])[1];
check('the spinner is drawn in tokens, not in the white it used to be',
  /var\(--accent\)/.test(SPIN) && !/#fff|255,\s*255,\s*255/.test(SPIN),
  SPIN);
check('and #loading still paints on the panel colour, so the two are testable together',
  /#loading\{[^}]*background:var\(--panel\)/.test(PAGE));

/* ── 3. Three signals, because a spinner alone stops reassuring ─────────── */
//
// A ring that has been turning for twenty seconds reads as hung. The counter
// is what says otherwise, and the changed sentence is what says "slower than
// usual, still going".

check('there is a sentence saying what is happening, not just a spinner',
  /id="loading-msg"/.test(PAGE) && /id="loading-hint"/.test(PAGE)
  && /Checking your session/.test(PAGE));
check('an elapsed counter appears once the wait is worth timing',
  /CygenixBusy\.busyLabel\(base, ms, true\)/.test(PAGE));
check('and the sentence escalates if it runs long, so a slow wait is not a silent one',
  /const SLOW_AFTER_MS = \d+/.test(PAGE) && /Still checking with Microsoft Entra/.test(PAGE)
  && /ms >= SLOW_AFTER_MS/.test(PAGE));
check('the escalation only applies to the session check, not to a redirect that is imminent',
  /busyBase === BUSY_MSG/.test(PAGE));

/* ── 4. One way of saying it, shared with the rest of the console ──────── */
//
// cygenix-busy.js already exists for exactly this — see tests/busy.test.js,
// whose header is the same sentence. The failure mode being avoided is a
// second elapsed formatter that drifts into "7.4 seconds" or "00:07".

check('the page loads the shared busy module',
  /<script src="\/cygenix-busy\.js\?v=[a-f0-9]{10}" defer><\/script>/.test(PAGE));
check('formatElapsed and busyLabel are a real export now, not reached through the test seam',
  /^\s*formatElapsed: formatElapsed,$/m.test(BUSY) && /^\s*busyLabel: busyLabel,$/m.test(BUSY)
  && !/CygenixBusy\.__core/.test(PAGE),
  '__core is the test seam; a page reading through it is depending on a private');
check('the module still exports them under __core too, so tests/busy.test.js is unaffected',
  /__core: \{[\s\S]*?formatElapsed: formatElapsed[\s\S]*?busyLabel: busyLabel/.test(BUSY));
// The module is deferred and the page's own indicator is not, so every use of
// it has to survive its absence — otherwise a failure to load the busy module
// would take out the very indicator that exists to report failures.
// Every call site is spelled window.CygenixBusy.x so the guard is visible AT
// the call rather than somewhere above it — which is also what makes this
// check a one-liner instead of a scope analysis. Comment lines are dropped
// first; prose about the module is not a call into it.
const CODE = PAGE.replace(/\/\*[\s\S]*?\*\//g, ' ').replace(/^\s*\/\/.*$/gm, ' ')
  .replace(/<!--[\s\S]*?-->/g, ' ');
const BARE = (CODE.match(/(?<!window\.)\bCygenixBusy\./g) || []);
check('every use of it is guarded, so the card still says what is happening if it never loads',
  /window\.CygenixBusy\.start\(/.test(CODE) && /window\.CygenixBusy\.busyLabel\(/.test(CODE)
  && BARE.length === 0,
  BARE.length + ' unguarded: ' + BARE.join(', '));

/* ── 5. First paint, and the no-JS case ────────────────────────────────── */
//
// The busy state is in the MARKUP, so there is no frame where the panel is
// blank and no flash of the sign-in card before script runs. That makes the
// no-script case the thing to get right: nothing here can ever resolve
// without JavaScript, so a spinner would turn for ever.

check('the card starts busy in the markup, so the first frame already shows the spinner',
  /<div class="card is-busy" id="main-card">/.test(PAGE));
check('the busy rules are gated on .js, set before first paint',
  /document\.documentElement\.className \+= ' js'/.test(PAGE)
  && /^#loading\{display:none\}$/m.test(PAGE));
check('and without scripting the visitor gets the card and a reason, not a spinner',
  /<noscript>/.test(PAGE) && /Sign-in needs JavaScript/.test(PAGE));

/* ── 6. Reached without sight, and without motion ──────────────────────── */

check('the loader is announced rather than only drawn',
  /id="loading" role="status" aria-live="polite"/.test(PAGE));
// Stopping the spinner under reduced motion would restore the exact
// impression this page is being fixed for. It slows instead, and the counter
// keeps ticking either way — that is the part that really proves life.
check('reduced motion slows the spinner rather than freezing it',
  /@media \(prefers-reduced-motion:reduce\)\{\.spinner\{animation-duration:2\.6s\}\}/.test(PAGE));

/* ── 7. Every exit path puts the page back ─────────────────────────────── */
//
// A busy state with a start and no stop is worse than none: it is a spinner
// that never ends. showCard() is the single exit, and every path reaches it.

check('stopping is one function, and it clears both the timer and the shared token',
  /function stopBusy\(\)[\s\S]{0,240}clearInterval\(busyTimer\)[\s\S]{0,160}busyTok\.done\(\)/.test(PAGE));
check('showCard is the only exit, and it stops the busy state',
  /function showCard\(\) \{\s*\n\s*stopBusy\(\);/.test(PAGE));
check('the error paths all go through it',
  /function showError\(msg\)[\s\S]{0,260}showCard\(\);/.test(PAGE)
  && (PAGE.match(/showCard\(\);/g) || []).length >= 4);

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
