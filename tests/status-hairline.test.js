// tests/status-hairline.test.js — the environment bar, and the panel it used
// to sit on top of.
//
// WHAT WAS WRONG
//
// #cyg-envbar was a fixed 22px bar at z-index 2000, on all 27 console pages.
// Two faults, one cosmetic and one a real bug:
//
//   1. It cost 22 permanent pixels of every screen for a fact that matters
//      intensely twice a day and not at all the rest of the time.
//   2. The Ask Cygenix panel is also position:fixed at top 0, at z-index 290.
//      2000 > 290, so the bar painted over the panel's header. Its New and ✕
//      buttons begin at y=12, which put the top 10px of both under the bar:
//      clicking there navigated to /profiles instead of pressing the button.
//
// WHAT REPLACES IT
//
// A 2px hairline that swells to 22px on hover and settles back, at z-index 55
// — below the assistant, below the busy bar. Red (production, or nothing
// selected and therefore every write blocked) locks it open at 22px and the
// page concedes the space.
//
// WHAT THESE TESTS PIN
//
// The state rules, because they decide whether somebody is warned they are
// pointed at production. The geometry, because "expanding must not reflow" is
// the difference between a hover affordance and the page jumping every time
// the pointer crosses the top edge. And the stacking, because the whole bug
// was two fixed elements at top 0 in the wrong order.
//
// The behaviour — hover timing, click-through, the locked state — is asserted
// in a real browser in tests/browser/sidebar-nav.smoke.js. A grep cannot see
// whether a button is clickable.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 240) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

const ROOT = path.join(__dirname, '..');
const PUB = path.join(ROOT, 'public');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

const H = require('../public/cygenix-status-hairline.js');
const SRC = read('public', 'cygenix-status-hairline.js');

console.log('Status hairline — which database is this session pointed at\n');

/* ── 1. The state rules ───────────────────────────────────────────────────── */
section('1. One function decides the level, and nothing else has to');

const store = (profiles, activeId) => ({
  v: 1, profiles: profiles || [], bindings: [], connMeta: {}, runRecords: [], events: [],
  settings: { envClasses: [], activeProfileId: activeId || null, selectedAt: 0 },
});
const prof = (id, env, name) => ({ id, name: name || id, envClass: env, status: 'active' });
const at = (input) => H.resolveStatus(input);

// Before the first profile exists nothing changes, anywhere. That is the same
// migration posture cygenix-profiles.js takes: the product behaves exactly as
// it always has until somebody defines a profile.
check('with no profiles at all the hairline does not render',
  at({ store: null }).level === 'off' && at({ store: store([]) }).level === 'off');
check('and reserves no space', at({ store: null }).restHeight === 0);

check('a DEV profile is green', at({ store: store([prof('FIN-DEV-01', 'DEV')], 'FIN-DEV-01') }).level === 'green');
check('a TEST profile is green', at({ store: store([prof('P', 'TEST')], 'P') }).level === 'green');
check('a UAT profile is green — warn is for things that are wrong, not for a tier',
  at({ store: store([prof('P', 'UAT')], 'P') }).level === 'green');

// RED 1 — production.
{
  const s = at({ store: store([prof('FIN-PRD-01', 'PRD', 'Cutover')], 'FIN-PRD-01') });
  check('a PRD profile is red', s.level === 'red');
  check('and locks the bar open', s.locked === true && s.restHeight === H.HEIGHTS.H_OPEN);
  check('the label names the profile, its environment and its name',
    s.text.indexOf('FIN-PRD-01') > -1 && s.text.indexOf('PRD') > -1 && s.text.indexOf('Cutover') > -1, s.text);
  check('and says nothing redundant — the label already reads PRD',
    s.detail === '', s.detail);
}

// RED 2 — profiles in force, none selected. Every write is blocked in this
// state (cpGuardWrite refuses outright), so it is a failure, not a warning.
{
  const s = at({ store: store([prof('A', 'DEV')], null) });
  check('profiles defined but none selected is red, not amber', s.level === 'red');
  check('it locks open too — writes are blocked and the user needs to know why',
    s.locked === true && s.restHeight === H.HEIGHTS.H_OPEN);
  check('and it says what to do about it',
    /writes are blocked/i.test(s.text), s.text);
}

// AMBER — the three the client can actually determine.
{
  const unknown = at({ store: store([prof('P', 'UNKNOWN')], 'P') });
  check('an unclassified environment is amber', unknown.level === 'amber');
  check('amber rests a pixel taller than green, and does not lock',
    unknown.restHeight === H.HEIGHTS.H_AMBER && unknown.locked === false
    && H.HEIGHTS.H_AMBER > H.HEIGHTS.H_GREEN);
  check('and it says writes are blocked, because they are',
    /writes blocked/i.test(unknown.detail), unknown.detail);

  const green = store([prof('P', 'DEV')], 'P');
  check('a degraded cloud sync is amber',
    at({ store: green, health: { degraded: true, reason: 'config' } }).level === 'amber');
  check('and names the reason the sync layer gave, rather than inventing one',
    /config/.test(at({ store: green, health: { degraded: true, reason: 'config' } }).detail));
  check('unsaved edits are amber',
    at({ store: green, health: { degraded: false, pendingSaves: 3 } }).level === 'amber');
  check('and are counted',
    /3 unsaved changes/.test(at({ store: green, health: { degraded: false, pendingSaves: 3 } }).detail));
  check('one unsaved edit is not "1 unsaved changes"',
    /1 unsaved change\b/.test(at({ store: green, health: { pendingSaves: 1 } }).detail));
  check('a healthy sync is not amber',
    at({ store: green, health: { degraded: false, pendingSaves: 0, verified: true } }).level === 'green');

  const jobs = [{ status: 'complete' }, { status: 'failed' }, { executionStatus: 'error' }];
  check('failed jobs are amber', at({ store: green, jobs: jobs }).level === 'amber');
  check('and are counted across both spellings of the status field',
    /2 jobs failed/.test(at({ store: green, jobs: jobs }).detail));
  check('jobs that merely have not run are not a warning',
    at({ store: green, jobs: [{ status: 'pending' }, { status: 'ready' }] }).level === 'green');
}

// Severity does not go backwards.
{
  const prd = store([prof('P', 'PRD')], 'P');
  check('an amber condition cannot demote a red one',
    at({ store: prd, jobs: [{ status: 'failed' }], health: { degraded: true } }).level === 'red');
  check('and the bar stays locked', at({ store: prd, jobs: [{ status: 'failed' }] }).locked === true);
}

// The seam for what this file cannot determine on its own.
{
  const green = store([prof('P', 'DEV')], 'P');
  check('a reported failure escalates the line',
    at({ store: green, overrides: { db: { level: 'red', label: 'target unreachable' } } }).level === 'red');
  check('carrying the reporter\'s own words',
    /target unreachable/.test(at({ store: green, overrides: { db: { level: 'red', label: 'target unreachable' } } }).text));
  check('two reporters do not overwrite each other — the override map is keyed',
    at({ store: green, overrides: { db: { level: 'amber', label: 'slow' }, val: { level: 'red', label: 'no preflight' } } }).level === 'red');
  check('report() is published for them to call', typeof H.report === 'function');
}

// It runs on every page load; it must not throw on anything.
{
  let threw = false;
  try {
    at(); at({}); at({ store: 'garbage' }); at({ store: { profiles: [null, {}] } });
    at({ store: store([prof('P', 'DEV')], 'P'), jobs: 'not a list', health: 'nope', overrides: { x: null } });
  } catch (e) { threw = e.message; }
  check('malformed input never throws', threw === false, threw);
}

/* ── 2. Geometry: expanding must not move the page ────────────────────────── */
section('2. The hover must not reflow the document');

check('the resting heights are the hairline, not the bar',
  H.HEIGHTS.H_GREEN === 2 && H.HEIGHTS.H_AMBER === 3 && H.HEIGHTS.H_OPEN === 22);
check('the hover catcher is wider than the line it protects — 2px is not hittable',
  H.HEIGHTS.HIT_H >= 10 && H.HEIGHTS.HIT_H > H.HEIGHTS.H_GREEN);

// body padding follows the RESTING height. If it followed the open height the
// whole page would jump every time the pointer crossed the top edge.
// The masthead (console redesign, Sep-2026) sits under the hairline, so the
// reserved strip is the hairline's resting height PLUS the masthead's — one
// variable each, summed in one place, never the open height.
check('body padding is driven by one variable, set from restHeight, plus the masthead',
  /padding-top:calc\(var\(--cyg-hairline-h\) \+ var\(--cx-masthead-h,0px\)\)/.test(SRC)
  && /--cyg-hairline-h['"]?,\s*s\.restHeight/.test(SRC.replace(/\s+/g, ' ')),
  'the reserved strip must be the resting height, never the open one');
check('opening changes only the element\'s own height, via a class',
  /#cyg-envbar\.is-open,#cyg-envbar\.is-locked\{height:'\+H_OPEN\+'px/.test(SRC.replace(/\s+/g, '')),
  'nothing outside the element may change size when it opens');
check('and the sidebar follows the same variables instead of hard-coding 22px',
  /\.cyg-sidebar\{top:calc\(var\(--cyg-hairline-h\)\+var\(--cx-masthead-h,0px\)\)/.test(SRC.replace(/\s+/g, '')));

check('the collapsed line does not intercept clicks at all',
  /#cyg-envbar\{[^}]*pointer-events:none/.test(SRC.replace(/\s+/g, '')),
  'a stray click near the top edge must never navigate away');
check('it becomes clickable only once open or locked',
  /#cyg-envbar\.is-open,#cyg-envbar\.is-locked\{[^}]*pointer-events:auto/.test(SRC.replace(/\s+/g, '')));

check('only the locked state moves the assistant panel down',
  /body\.cyg-envbar-locked\.cyga\{top:var\(--cyg-hairline-h\)\}/.test(SRC.replace(/\s+/g, '')),
  'at 2px there is nothing to clear; at 22px production owns the space');

/* ── 3. Stacking — the actual bug ─────────────────────────────────────────── */
section('3. The panel is never covered again');

const ASSISTANT = read('public', 'cygenix-assistant.js');
const BUSY = read('public', 'cygenix-busy.js');
const zOf = (src, re) => { const m = src.match(re); return m ? Number(m[1]) : NaN; };

const zAssistant = zOf(ASSISTANT, /\.cyga\{[\s\S]{0,400}?z-index:(\d+)/);
const zBusy = zOf(BUSY, /\.cygbusy\{[\s\S]{0,200}?z-index:(\d+)/);

check('the assistant panel is still at z-index 290', zAssistant === 290, zAssistant);
check('the hairline sits BELOW it — this is the whole fix',
  H.Z.BAR < zAssistant, H.Z.BAR + ' vs ' + zAssistant);
check('and the hover catcher below that, so it cannot steal the panel\'s clicks either',
  H.Z.HIT < H.Z.BAR && H.Z.HIT < zAssistant);
check('the busy bar stays above the hairline — "something is running" outranks "which database"',
  zBusy > H.Z.BAR, zBusy + ' vs ' + H.Z.BAR);
check('nothing here raises itself to the old 2000',
  !/z-index:\s*2000/.test(SRC) && H.Z.BAR < 100);

check('the old bar is gone from the sidebar, not merely unused',
  !/#cyg-envbar\{position:fixed/.test(read('public', 'cygenix-sidebar.js')),
  'two implementations of the same bar is how one of them gets fixed and the other does not');
check('and the sidebar no longer hard-codes the bar height',
  !/cyg-envbar-pad \.cyg-sidebar\{top:22px/.test(read('public', 'cygenix-sidebar.js')));

/* ── 4. The things that are easy to leave out ─────────────────────────────── */
section('4. Touch, keyboard, reduced motion, announcement');

check('the open and close delays are named constants, not magic numbers',
  H.TIMING.OPEN_DELAY_MS === 140 && H.TIMING.CLOSE_DELAY_MS === 320
  && /var OPEN_DELAY_MS = 140;/.test(SRC) && /var CLOSE_DELAY_MS = 320;/.test(SRC));
check('opening is delayed, so sweeping up to the address bar does not open it',
  /_openTimer = setTimeout\([\s\S]{0,120}OPEN_DELAY_MS\)/.test(SRC));
check('and leaving is delayed too, so the pointer can travel into the bar it just opened',
  /_closeTimer = setTimeout\([\s\S]{0,120}CLOSE_DELAY_MS\)/.test(SRC));
check('a fresh amber shows itself for four seconds and then settles',
  H.TIMING.AMBER_PEEK_MS === 4000 && /function peek\(\)/.test(SRC));
check('and that one-shot clears its own handle BEFORE acting, never re-arming from inside itself',
  /_peekTimer = setTimeout\(function \(\) \{\s*_peekTimer = null;/.test(SRC),
  'a one-shot that re-arms from its own callback caused an infinite rerender here before');
check('the amber peek is remembered per session, so every page does not re-flash it',
  /sessionStorage/.test(SRC) && /SEEN_KEY/.test(SRC));

check('touch devices get tap-to-open, because there is no hover to wait for',
  /matchMedia\('\(hover: none\)'\)/.test(SRC) && /_tapMode/.test(SRC));
check('and a tap anywhere else closes it',
  /document\.addEventListener\('click'[\s\S]{0,300}setOpen\(false\)/.test(SRC));
check('the profile link stays keyboard-reachable and focusing it opens the line',
  /addEventListener\('focusin', function \(\) \{ clearTimers\(\); setOpen\(true\)/.test(SRC));
check('reduced motion snaps instead of animating',
  /@media \(prefers-reduced-motion: reduce\)/.test(SRC));
check('escalation is announced through an aria-live region',
  /aria-live['"]?, ?['"]polite/.test(SRC) && /live\.textContent = 'Warning: '/.test(SRC));
check('and only on the transition, not on every page load',
  /if \(s\.level !== prev\)/.test(SRC));

/* ── 4c. The status colours are the same on every screen ──────────────────── */
section('4c. A level must look identical wherever you see it');
//
// This shipped wrong. The bar used var(--green)/var(--amber)/var(--red) — the
// console's design tokens — and 26 of the 27 pages that carry it redefine
// those three in their own inline :root, disagreeing with each other: three
// greens, three ambers, three reds. The same DEV profile rendered bright mint
// on the dashboard and forest green on Reports. For a colour whose entire job
// is to be recognised at a glance on any screen, that is a defect.
{
  const SIDE = read('public', 'cygenix-sidebar.js');
  const pages = fs.readdirSync(PUB).filter((f) => f.endsWith('.html'))
    .filter((f) => /cygenix-status-hairline\.js/.test(read('public', f)));
  const greens = new Set(), ambers = new Set(), reds = new Set();
  pages.forEach((f) => {
    const src = read('public', f);
    const g = src.match(/--green:\s*(#[0-9a-fA-F]+)/); if (g) greens.add(g[1].toLowerCase());
    const a = src.match(/--amber:\s*(#[0-9a-fA-F]+)/); if (a) ambers.add(a[1].toLowerCase());
    const r = src.match(/--red:\s*(#[0-9a-fA-F]+)/);   if (r) reds.add(r[1].toLowerCase());
  });
  // They used to: three different greens across 26 pages. The console
  // redesign stripped every page's copy of the palette — the pages declare
  // no --green at all now, and cygenix-console.css is the one place it is
  // set. The hairline still paints from its own inline values, because a
  // page could grow a palette back and the line must not follow it.
  check('no page carries its own --green any more — the palette has one home',
    greens.size === 0, [...greens].join(', ') + ' across ' + pages.length + ' pages');

  // Only the declarations matter; the comment above PALETTE names the old
  // tokens on purpose, to say what was wrong.
  const DECLS = SRC.replace(/\/\*[\s\S]*?\*\//g, '');
  check('the bar does NOT read the page\'s status tokens',
    !/background:\s*var\(--green/.test(DECLS) && !/background:\s*var\(--amber/.test(DECLS)
    && !/background:\s*var\(--red/.test(DECLS),
    'a page style block must not be able to change what a level looks like');
  check('it carries its own palette instead — the console\'s status trio, as literals',
    /var PALETTE = \{ green: '#3f6b52', amber: '#9a6b1f', red: '#9c3f38' \};/.test(SRC));
  check('applied INLINE on the root element, which outranks any stylesheet :root',
    /root\.style\.setProperty\('--cyg-status-green', PALETTE\.green\)/.test(SRC));
  check('and the bar paints from those, not from the page\'s',
    /#cyg-envbar\.lv-red\{background:var\(--cyg-status-red/.test(SRC.replace(/'\s*\+\s*PALETTE\.\w+\s*\+\s*'/g, 'X').replace(/\s+/g, '')));
  check('the rail chip reads the same three, so the dot and the line cannot disagree',
    /--cyg-status-green/.test(SIDE) && /--cyg-status-amber/.test(SIDE) && /--cyg-status-red/.test(SIDE)
    && !/\.cyg-prof-dot\{[^}]*var\(--green/.test(SIDE));
  check('and the old hard-coded bar colours are gone for good',
    !/#1f7a4d/i.test(SRC) && !/#c23636/i.test(SRC),
    'the old bar hard-coded #1f7a4d, which no theme could reach');
}

/* ── 4d. Putting it away ──────────────────────────────────────────────────── */
section('4d. An expanded bar can be dismissed');
//
// Reported: "I can't see the collapse button on the green status bar." There
// was none — moving the pointer away was the only way, which is no way at all
// on a touch device, and is not discoverable anywhere.
check('the expanded bar carries a dismiss button',
  /x\.className = 'cyg-envbar-x';/.test(SRC)
  && /setAttribute\('aria-label', 'Collapse the status bar'\)/.test(SRC));
check('which does not exist while collapsed — there is nothing to dismiss',
  /#cyg-envbar \.cyg-envbar-x\{display:none/.test(SRC.replace(/\s+/g, ' ')));
check('and is withheld while locked — production owns that space',
  /#cyg-envbar\.is-locked \.cyg-envbar-x\{display:none\}/.test(SRC.replace(/',\s*'/g, '').replace(/\s+/g, ' ')));
check('clicking it does not also follow the link it sits inside',
  /e\.preventDefault\(\); e\.stopPropagation\(\);\s*clearTimers\(\);\s*setOpen\(false\)/.test(SRC),
  'the button is inside the anchor; without both it would navigate to /profiles');
check('and tabbing between the link and the button does not collapse the bar mid-tab',
  /focusout/.test(SRC) && /el\.bar\.contains\(e\.relatedTarget\)/.test(SRC),
  'a plain blur handler took the button out of the document while focus was moving to it');

/* ── 4b. The rail's profile chip ──────────────────────────────────────────── */
section('4b. Something legible always says which database this is');

// A 2px green line and a line that failed to render look identical. The chip
// is the readable half of the same fact — and it must be ONE fact: two
// implementations of "what environment am I in" is how one of them ends up
// wrong, on the screen that matters.
{
  const SIDE = read('public', 'cygenix-sidebar.js');
  check('the rail carries a profile chip', /id="cyg-prof-chip"/.test(SIDE));
  check('it is pinned above the scroll area, not inside anything that can be folded away',
    /return head \+ buildProfilePill\(\) \+ `<div class="cyg-sidebar-scroll">/.test(SIDE),
    'the database a run will touch must never be hidden');
  check('it links to the Profiles page', /class="cyg-prof-chip" id="cyg-prof-chip"\s*href="\/profiles"/.test(SIDE.replace(/\s+/g, ' ')));
  check('it renders from the hairline\'s state function, not from a second read of the store',
    /CygenixStatusHairline/.test(SIDE) && !/cygenix_profiles_v1/.test(SIDE),
    'the sidebar must not parse the profile store itself');
  check('the hairline publishes that state as an event',
    /cygenix:profile-status/.test(SRC) && typeof H.current === 'function');
  check('and the rail both takes the current value and subscribes — script order is not guaranteed',
    /H\.current\(\)\)/.test(SIDE) && /addEventListener\('cygenix:profile-status'/.test(SIDE));
  // The environment badge is a bordered tag now (console redesign): the level
  // is its text and its border, not a fill — a filled amber block on the
  // light rail read as a button.
  check('the level colours the dot and the environment badge',
    /\.cyg-prof-chip\.lv-red\s+\.cyg-prof-dot\{background:var\(--cyg-status-red/.test(SIDE.replace(/\{\s+/g, '{'))
    && /\.cyg-prof-chip\.lv-amber\s+\.cyg-prof-env\{color:var\(--cyg-status-amber[^}]*border-color:var\(--cyg-status-amber/.test(SIDE.replace(/\{\s+/g, '{')));
  check('the collapsed rail keeps the dot — 54px has no room for a name, but PRD must still show',
    /\.cyg-sidebar\.collapsed \.cyg-prof-id,\s*\.cyg-sidebar\.collapsed \.cyg-prof-env\{\s*display:none/.test(SIDE)
    && !/\.cyg-sidebar\.collapsed \.cyg-prof-dot\{\s*display:none/.test(SIDE));
  check('and before any profile exists the chip is hidden, like everything else about profiles',
    /if \(!s \|\| s\.level === 'off'\)\{ area\.hidden = true; return; \}/.test(SIDE));
}

/* ── 5. Coverage ──────────────────────────────────────────────────────────── */
section('5. Every page that had the old bar has the new one');

{
  const pages = fs.readdirSync(PUB).filter((f) => f.endsWith('.html'));
  const withSidebar = pages.filter((f) => /<script src="\/cygenix-sidebar\.js/.test(read('public', f)));
  const missing = withSidebar.filter((f) => !/<script src="\/cygenix-status-hairline\.js/.test(read('public', f)));
  check('every page that loads the sidebar loads the hairline (' + withSidebar.length + ' pages)',
    withSidebar.length >= 25 && missing.length === 0, missing.join(', '));

  const wrongOrder = withSidebar.filter((f) => {
    const src = read('public', f);
    return src.indexOf('/cygenix-status-hairline.js') > src.indexOf('<script src="/cygenix-sidebar.js');
  });
  check('and loads it first, so the rail offset is in place before the rail draws',
    wrongOrder.length === 0, wrongOrder.join(', '));

  const marketing = ['index.html', 'login.html', 'register.html', 'pricing.html'];
  const leaked = marketing.filter((f) => fs.existsSync(path.join(PUB, f))
    && /cygenix-status-hairline\.js/.test(read('public', f)));
  check('and it is not dragged onto the signed-out pages', leaked.length === 0, leaked.join(', '));
}

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
