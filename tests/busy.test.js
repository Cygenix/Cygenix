// Tests for the shared busy indicator.
//
// The slowest things in this console had the least feedback. An AI mapping
// call takes ten to thirty seconds; several of the buttons that started one
// showed nothing at all, because the icon sweep replaced their glyph with an
// empty string and left a blank disabled button for the whole call. A user
// watching a blank button cannot tell working from hung.
//
// Two kinds of test here. The first pins the ENGINE: ref-counting (so three
// overlapping operations share one bar and it clears when the last finishes),
// the determinate/indeterminate rule, and the elapsed formatting. The second
// pins the WIRING: every slow path starts a token, every start has a matching
// release on every exit path, and no button anywhere is left blank.
'use strict';
const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 240) : '')); }
};
const P = (...p) => path.join(__dirname, '..', ...p);
const read = (...p) => fs.readFileSync(P(...p), 'utf8');

const B = require('../public/cygenix-busy.js');
const core = B.__core;

console.log('Busy indicator — working must never look like hung\n');

/* ── 1. Elapsed formatting ──────────────────────────────────────────────── */

check('under a minute reads in seconds', core.formatElapsed(8000) === '8s', core.formatElapsed(8000));
check('a minute and over reads in minutes, zero-padded',
  core.formatElapsed(64000) === '1m 04s' && core.formatElapsed(600000) === '10m 00s',
  core.formatElapsed(64000) + ' / ' + core.formatElapsed(600000));
// A counter that twitches ten times a second is noise, not information.
check('there are no decimals', !/\./.test(core.formatElapsed(8432)));

/* ── 2. The button label ────────────────────────────────────────────────── */

// "Working…" on its own loses which button you pressed.
check('a busy button keeps its own name', core.busyLabel('AI map', 500, true) === 'AI map');
check('the elapsed time appears only once the wait is worth timing',
  core.busyLabel('AI map', 500, true) === 'AI map'
  && core.busyLabel('AI map', 9000, true) === 'AI map · 9s');
check('the threshold is a couple of seconds, so a quick call never flashes a number',
  core.ELAPSED_AFTER_MS >= 1500 && core.ELAPSED_AFTER_MS <= 5000, core.ELAPSED_AFTER_MS);
// A 90ms flicker reads as a glitch, not as progress.
check('nothing is shown at all until the work outlives a flicker',
  core.SHOW_AFTER_MS >= 100 && core.SHOW_AFTER_MS <= 400, core.SHOW_AFTER_MS);
check('a nameless operation still gets a word rather than an empty spinner',
  core.busyLabel('', 0, true) === 'Working');

/* ── 3. Ref-counting ────────────────────────────────────────────────────── */

check('nothing is busy to begin with', B.isBusy() === false && B.count() === 0);
const a = B.start('one');
const b = B.start('two');
const c = B.start('three');
check('three overlapping operations count as three', B.count() === 3);
check('the bar is active while any of them runs', core.aggregate(Date.now()).active === true);
b.done();
check('finishing one of three leaves the bar up', B.count() === 2 && core.aggregate(Date.now()).active);
a.done(); c.done();
check('the bar clears only when the last finishes',
  B.count() === 0 && core.aggregate(Date.now()).active === false);
check('releasing the same token twice is harmless', (() => {
  const t = B.start('x'); t.done(); t.done();
  return B.count() === 0;
})());

/* ── 4. Determinate vs indeterminate ────────────────────────────────────── */

const d1 = B.start('tables', { total: 10 });
d1.progress(4);
let agg = core.aggregate(Date.now());
check('known work reports a real fraction',
  agg.determinate === true && Math.abs(agg.fraction - 0.4) < 1e-9, JSON.stringify(agg));
// A bar claiming 60% while something unmeasured runs is a lie.
const d2 = B.start('unknown');
agg = core.aggregate(Date.now());
check('one unmeasured operation makes the whole bar indeterminate',
  agg.determinate === false, JSON.stringify(agg));
d2.done();
check('it becomes determinate again once the unknown finishes',
  core.aggregate(Date.now()).determinate === true);
d1.progress(20);
check('progress past the total is clamped rather than overflowing',
  core.aggregate(Date.now()).fraction === 1, String(core.aggregate(Date.now()).fraction));
d1.done();
check('everything is clear afterwards', B.count() === 0);

/* ── 5. around() always releases ────────────────────────────────────────── */

// The synchronous case first: the async ones below hold live tokens until
// their promises settle, so a count check between them would read those.
check('around clears when the function throws synchronously', (() => {
  try { B.around('sync', () => { throw new Error('x'); }); } catch (e) { /* expected */ }
  return B.count() === 0;
})());
check('around returns a plain value and clears', B.around('plain', () => 7) === 7 && B.count() === 0);

const results = [];
results.push(B.around('ok', () => Promise.resolve(42)).then((v) => {
  check('around returns the resolved value and clears', v === 42);
}));
results.push(B.around('boom', () => Promise.reject(new Error('nope'))).then(
  () => check('around clears on rejection', false, 'should have rejected'),
  (e) => check('around clears on rejection', e.message === 'nope')));

/* ── 6. The module itself ───────────────────────────────────────────────── */

const busy = read('public', 'cygenix-busy.js');
check('the bar outranks modals and the assistant panel, or it is invisible when they wait',
  /\.cygbusy\{[^}]*z-index:500/.test(busy));
check('reduced motion keeps the signal and drops the movement',
  /prefers-reduced-motion:reduce/.test(busy) && /animation:none/.test(busy));
check('the spinner inherits the button colour rather than needing a variant each',
  /border:1\.5px solid currentColor/.test(busy));
check('a busy button reads as busy, not as merely disabled',
  /cursor:progress/.test(busy));
check('the bar is announced to assistive tech',
  /role', 'progressbar'/.test(busy) && /aria-valuenow/.test(busy) && /aria-busy/.test(busy));
check('a navigation abandons every in-flight token',
  /pagehide.*clearAll|addEventListener\('pagehide', clearAll\)/.test(busy));
check('the label falls back when a button was left blank',
  /blank label is what the icon sweep left behind/.test(busy));
// Caught only by the browser smoke: paint() hides the bar until the work has
// outlived SHOW_AFTER_MS, and with a 500ms tick the first repaint after that
// threshold could be half a second late — so a 700ms operation showed the bar
// as it finished, or never. A one-shot lands on the threshold itself.
check('the bar is revealed on the threshold, not on the next half-second tick',
  /_revealTimer = setTimeout\(function \(\) \{ _revealTimer = null; paint\(\); \}, SHOW_AFTER_MS \+ 10\)/.test(busy));
check('the reveal timer is cleared with the last operation, not left pending',
  /if \(_revealTimer\) \{ clearTimeout\(_revealTimer\); _revealTimer = null; \}/.test(busy));
check('the reveal one-shot is set even when the tick is already running',
  /if \(!_tickTimer\) _tickTimer = setInterval\(paint, TICK_MS\);/.test(busy));

/* ── 7. Wiring: every start has a release on every path ─────────────────── */

const WIRED = ['object-mapping-app.js', 'mapper.html', 'one-to-many.html',
               'project-builder-app.js', 'cygenix-assistant.js'];
for (const f of WIRED) {
  const src = read('public', f);
  const starts = (src.match(/CygenixBusy\.start\(/g) || []).length;
  const dones  = (src.match(/\w*[Bb]usy\.done\(\)|_busy\.done\(\)|_busyToken\.done\(\)/g) || []).length;
  check(f + ' releases every busy token it starts',
    starts > 0 && dones >= starts, starts + ' start(s), ' + dones + ' release(s)');
}
// The releases that matter are the ones on the paths people actually hit.
const om = read('public', 'object-mapping-app.js');
check('the mapping page releases in finally blocks, not just on the happy path',
  (om.match(/finally \{[\s\S]{0,140}Busy\.done\(\)/g) || []).length >= 5,
  String((om.match(/finally \{[\s\S]{0,140}Busy\.done\(\)/g) || []).length));
check('the DDL run releases on its success path too — it did not before',
  /Every path, including the success one/.test(om));

/* ── 8. Wiring: the slow paths are covered ──────────────────────────────── */

for (const [what, needle] of [
  ['the OTM AI map', /CygenixBusy\.start\('AI map'/],
  ['the evidence map', /CygenixBusy\.start\('Evidence map'/],
  ['the single-map build', /CygenixBusy\.start\(apiKey \? 'Mapping columns with Claude'/],
  ['re-mapping with Claude', /CygenixBusy\.start\('Re-mapping with Claude'/],
  ['reading the source schema', /CygenixBusy\.start\('Reading the source schema'\)/],
  ['reading the target schema', /CygenixBusy\.start\('Reading the target schema'\)/],
  ['loading an object\'s columns', /CygenixBusy\.start\('Loading columns for '/],
  ['counting rows on demand', /CygenixBusy\.start\('Counting rows in '/],
  ['creating a table', /CygenixBusy\.start\('Creating the table'/],
]) {
  check(what + ' shows a busy indicator', needle.test(om));
}

const asst = read('public', 'cygenix-assistant.js');
check('the assistant raises the page bar for a whole turn',
  /_busyToken = root\.CygenixBusy\.start\('Assistant'\)/.test(asst));
check('the bar keeps running when the panel is closed mid-turn',
  /panel can be closed while a turn runs/.test(asst));
check('the assistant busy row counts elapsed time',
  /formatElapsed\(since\)/.test(asst));
check('it names the running action rather than saying Working for thirty seconds',
  /beginBusy\(action\.title \|\| tu\.name\)/.test(asst));
check('the counter advances on a timer, not only when a turn ends',
  /setInterval\(function \(\) \{[\s\S]{0,200}render\(\);/.test(asst));
check('every terminal state releases the token',
  (asst.match(/endBusy\(\)/g) || []).length >= 6,
  String((asst.match(/endBusy\(\)/g) || []).length));

/* ── 9. No blank buttons anywhere ───────────────────────────────────────── */

// The regression that started this: a sweep set a button's text to '' and the
// control became invisible. Nothing should create a button that way.
const suspects = [];
for (const f of fs.readdirSync(P('public'))) {
  if (!/\.(js|html)$/.test(f)) continue;
  const src = read('public', f);
  const lines = src.split('\n');
  lines.forEach((ln, i) => {
    // a button element being given empty visible content
    if (/createElement\('button'\)/.test(ln) && /\.textContent\s*=\s*''/.test(ln)) {
      suspects.push(f + ':' + (i + 1));
    }
    // a busy handler blanking the button it just disabled
    if (/\.textContent\s*=\s*''\s*;\s*\w+\.disabled\s*=\s*true/.test(ln)
     || /\.disabled\s*=\s*true\s*;\s*\w+\.textContent\s*=\s*''/.test(ln)) {
      suspects.push(f + ':' + (i + 1));
    }
  });
}
check('no button is created or left with empty visible content',
  suspects.length === 0, suspects.join(', '));

const drive = read('public', 'cygenix-drive-modal.js');
for (const [label, icon] of [['Download', 'ic-download'], ['Move to folder…', 'ic-folder-open'],
                             ['Rename', 'ic-edit'], ['Delete', 'ic-trash']]) {
  check('the Drive\'s ' + label + ' button draws an icon again',
    new RegExp("title = '" + label.replace(/…/, '…') + "'; \\w+\\.innerHTML = '<i class=\"ic " + icon).test(drive));
}

/* ── 10. Loaded where it is needed, and only there ──────────────────────── */

const appPages = fs.readdirSync(P('public')).filter((f) =>
  f.endsWith('.html') && /cygenix-sidebar\.js/.test(read('public', f)));
const missing = appPages.filter((f) => !/<script src="\/cygenix-busy\.js/.test(read('public', f)));
check('every app page loads the busy module', missing.length === 0, missing.join(', '));
// The assistant calls into it, so it has to be defined first.
const outOfOrder = appPages.filter((f) => {
  const src = read('public', f);
  if (!/cygenix-assistant\.js/.test(src)) return false;
  return src.indexOf('cygenix-busy.js') > src.indexOf('cygenix-assistant.js');
});
check('the busy module loads before the assistant that uses it',
  outOfOrder.length === 0, outOfOrder.join(', '));
for (const f of ['login.html', 'index.html', 'pricing.html']) {
  check(f + ' does not carry it', !/cygenix-busy\.js/.test(read('public', f)));
}
// Every call site guards on the global, so a page that forgets the script
// degrades to its old behaviour instead of throwing.
const unguarded = [];
for (const f of WIRED) {
  const src = read('public', f);
  const calls = (src.match(/(?:^|[^.\w])CygenixBusy\.start\(/g) || []).length;
  const guarded = (src.match(/(?:window\.CygenixBusy|root\.CygenixBusy) && \w*\.?CygenixBusy\.start\(|root && root\.CygenixBusy/g) || []).length;
  if (guarded < 1 && calls > 0) unguarded.push(f);
}
check('every call site guards on the global being present', unguarded.length === 0, unguarded.join(', '));

Promise.all(results).then(() => {
  check('nothing is left running once every around() has settled', B.count() === 0, String(B.count()));
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
});
