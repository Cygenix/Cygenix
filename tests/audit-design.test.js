// tests/audit-design.test.js — the Audit log in the console's design language.
//
// Design review, Sep-2026, Phase 5. The screen keeps everything it did —
// the capture state and its two modals, the per-column filters, the drawer
// with the full hash, retention that says what it has done — and is drawn to
// the handoff's §6:
//
//   · kicker GOVERN · HASH-CHAINED, APPEND ONLY, title AUDIT LOG, and the two
//     actions the screen exists for: Export evidence pack and Verify chain;
//   · four tabs — Events · Capture settings · Integrity · Retention;
//   · an integrity band, the one blueprint frame on the screen, that states
//     the verification as a sentence in the status colour, with the known
//     limit beside it — and, on a break, what to do about it;
//   · a filter row with the count pushed right, and a table that runs Seq ·
//     When · Actor · Category · Action · Object · Env · Outcome · Source, the
//     category drawn as the environment tag and PROD in state-fail;
//   · the four always-on categories rendered as locked, never as disabled
//     switches.
//
// The server side of the band is pinned too: the status call carries the
// last verification from the trail and the head sequence, so "verified to
// entry N" is a statement about a recorded fact.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const PUB = path.join(__dirname, '..', 'public');
const VIEW = fs.readFileSync(path.join(PUB, 'audit-app.js'), 'utf8');
const DASH = fs.readFileSync(path.join(PUB, 'dashboard.html'), 'utf8');
const FN = fs.readFileSync(path.join(__dirname, '..', 'netlify', 'functions', 'audit.js'), 'utf8');
const CSS = VIEW.slice(VIEW.indexOf('function injectStyles()'), VIEW.indexOf('// ── Loading'));

console.log('Audit log — the console design language\n');

/* ── 1. Header ──────────────────────────────────────────────────────────── */
check('the screen renders its own header: kicker GOVERN · HASH-CHAINED, APPEND ONLY, title AUDIT LOG',
  /function headerHtml\(\)/.test(VIEW)
  && /<div class="cx-kicker">Govern · Hash-chained, append only<\/div>/.test(VIEW)
  && /<h1 class="cx-title">Audit log<\/h1>/.test(VIEW)
  && /<div id="view-audit" class="view">\s*<!--[^>]*-->\s*<div id="audit-log-wrap"><\/div>/.test(DASH)
  && !/<div class="panel-title">Audit Log<\/div>/.test(DASH));
check('Export evidence pack is secondary and Verify chain is the primary, both in the header',
  /<button class="cyg-a-btn" id="cyg-a-csv" type="button"[^>]*>Export evidence pack<\/button>/.test(VIEW)
  && /<button class="cyg-a-btn primary" id="cyg-a-verify" type="button"/.test(VIEW)
  && /'Verify chain'/.test(VIEW)
  && /function wireHeader\(\)/.test(VIEW) && /vb\.addEventListener\('click', runVerify\)/.test(VIEW));
check('the header is rendered first, then the tabs, then the band — and a reader who may not export is told why',
  /headerHtml\(\) \+ tabsHtml\(\) \+ bandHtml\(\) \+ bannerHtml\(\) \+ statusHtml\(\) \+ kpiHtml\(\)/.test(VIEW)
  && /Export needs the Platform Administrator or Auditor role/.test(VIEW));

/* ── 2. Tabs ────────────────────────────────────────────────────────────── */
// Five since Sep-2026: Sign-ins sits second, beside Events, because both
// answer "what happened" and the three after it are configuration.
check('the tabs, in the handoff\'s order with Sign-ins added, drawn as the console strip',
  /\{ key: 'events', label: 'Events' \},\s*\{ key: 'signins', label: 'Sign-ins' \},\s*\{ key: 'settings', label: 'Capture settings' \},\s*\{ key: 'integrity', label: 'Integrity' \},\s*\{ key: 'retention', label: 'Retention' \},/.test(VIEW)
  && /'\.cyg-a-tab\{[^']*font-family:var\(--font-heading\)[^']*font-size:16px[^']*text-transform:uppercase/.test(CSS)
  && /'\.cyg-a-tab\[aria-selected="true"\]\{color:var\(--color-text\);border-color:var\(--color-accent\)\}'/.test(CSS)
  && /id="cyg-a-panel-retention" role="tabpanel"/.test(VIEW)
  && /function renderRetentionPanel\(\)/.test(VIEW));
check('the tab widget stays conformant — tablist, aria-selected, roving tabindex, arrow keys',
  /role="tablist"/.test(VIEW) && /aria-selected/.test(VIEW) && /ArrowRight/.test(VIEW) && /tabindex="' \+ \(on \? '0' : '-1'\)/.test(VIEW));

/* ── 3. The integrity band ──────────────────────────────────────────────── */
check('the band is the one blueprint frame — 16px 20px, four items with a 28px gap',
  /<div class="cx-blueprint cyg-a-band" id="cyg-a-band">/.test(VIEW)
  && (VIEW.match(/cx-blueprint/g) || []).length === 1
  && /'\.cyg-a-band\{padding:16px 20px;display:flex;align-items:flex-start;gap:28px/.test(CSS));
check('Chain is stated as a sentence at 22px Condensed uppercase, state-ok when verified, state-fail on a break, neutral when never',
  /'\.cyg-a-band \.v\{font-family:var\(--font-heading\);font-weight:600;font-size:22px[^']*text-transform:uppercase/.test(CSS)
  && /'\.cyg-a-band \.v\.ok\{color:var\(--state-ok\)\}\.cyg-a-band \.v\.fail\{color:var\(--state-fail\)\}/.test(CSS)
  && /'<div class="v ok">Verified to entry ' \+ esc\(Number\(f\.to \|\| 0\)\.toLocaleString\('en-GB'\)\)/.test(VIEW)
  && /'<div class="v fail">Break at entry '/.test(VIEW)
  && /'<div class="v dim">Not yet verified<\/div>'/.test(VIEW));
check('Last verified and Capture ("N categories · M locked") follow; the caveat is a 42ch sentence pushed right',
  /<div class="k">Last verified<\/div>/.test(VIEW) && /<div class="k">Capture<\/div>/.test(VIEW)
  && /' categor' \+ \(cats === 1 \? 'y' : 'ies'\) \+\s*' · ' \+ locked \+ ' locked<\/div><\/div>'/.test(VIEW)
  && /'\.cyg-a-band \.caveat\{margin-left:auto;max-width:42ch;font-size:14px/.test(CSS)
  && /A simultaneous append can race the head\. A break is reported, never hidden — state that in any evidence pack\./.test(VIEW));
check('on a break the caveat is replaced by what to do about it',
  /'<div class="caveat fail"><b>Entry ' \+ esc\(f\.brokenAt\) \+ ' does not line up'/.test(VIEW)
  && /Export the trail before anything else is written/.test(VIEW)
  && /record the finding as an incident/.test(VIEW));
check('a verification run in this session outranks the recorded one, and neither is invented',
  /var v = state\.verifyResult \|\| state\.status\.lastVerify \|\| null;/.test(VIEW)
  && /if \(!v\) return \{ kind: 'none' \};/.test(VIEW)
  && /refreshBand\(\);/.test(VIEW.slice(VIEW.indexOf('function runVerify'), VIEW.indexOf('function refreshBand'))));
check('the server reports the last verification from the trail and the head sequence',
  /const lastVerifyEntry = \(recent\.entries \|\| \[\]\)\.filter\(e => e\.action === 'audit\.verify'\)\[0\] \|\| null;/.test(FN)
  && /headSeq: recent\.chainTotal \|\| 0,/.test(FN) && /lastVerify,/.test(FN)
  && /ok: lastVerifyEntry\.outcome === 'allowed',/.test(FN));

/* ── 4. Events: the filter row and the table ────────────────────────────── */
check('the filter row: free text "Filter by actor, object or id", Clear while set, the count pushed right',
  /placeholder="Filter by actor, object or id"/.test(VIEW)
  && /<span class="cyg-a-count" id="cyg-a-count">/.test(VIEW)
  && /' event' \+ \(state\.total === 1 \? '' : 's'\) \+ ' · ' \+ windowWord/.test(VIEW)
  && /'\.cyg-a-count\{margin-left:auto;font-size:14px;color:var\(--color-neutral-700\)/.test(CSS));
check('the table runs Seq · When · Actor · Category · Action · Object · Env · Outcome · Source (right)',
  /<th>Seq<\/th><th>When<\/th><th>Actor<\/th><th>Category<\/th><th>Action<\/th><th>Object<\/th>' \+\s*'<th>Env<\/th><th>Outcome<\/th><th class="r">Source<\/th>/.test(VIEW)
  && /'<td class="cyg-a-seq">' \+ esc\(e\.seq\) \+ '<\/td>'/.test(VIEW)
  && /'<td class="r cyg-a-srcw">' \+ \(e\.source === 'client' \? 'client' : 'server'\) \+ '<\/td><\/tr>'/.test(VIEW)
  && /'\.cyg-a-seq\{font-variant-numeric:tabular-nums/.test(CSS)
  && /'\.cyg-a-srcw\{font-size:13px;color:var\(--color-neutral-600\)/.test(CSS));
check('the per-column filters survive, one per column it governs, still sticky, with empty cells under the columns that have none',
  (VIEW.match(/data-filter="(days|actor|action|target|env|outcome)"/g) || []).length === 6
  && (VIEW.slice(VIEW.indexOf('function filterRow'), VIEW.indexOf('// Gap rows')).match(/cell\(''\)/g) || []).length === 3
  && /position:sticky;top:0;z-index:1/.test(CSS)
  && /colspan="9"/.test(VIEW) && !/colspan="6"/.test(VIEW));
check('category renders as the environment tag; a PROD-scoped event takes the state-fail border and text',
  /function categoryTag\(e\)/.test(VIEW)
  && /var prod = e\.category === 'prod' \|\| String\(e\.environment \|\| ''\)\.toUpperCase\(\) === 'PROD';/.test(VIEW)
  && /'\.cyg-a-cat\{font-family:var\(--font-heading\);font-weight:600;font-size:12px;letter-spacing:\.1em;text-transform:uppercase;padding:2px 8px[^']*border:1px solid var\(--color-divider\)/.test(CSS)
  && /'\.cyg-a-cat\.prod\{color:var\(--state-fail\);border-color:var\(--state-fail\)\}'/.test(CSS));
check('the outcome is a word in the status colour and the avatar blocks are gone — hue is for state',
  /'\.cyg-a-out\{font-size:13px\}\.cyg-a-out\.allowed\{color:var\(--state-ok\)\}\.cyg-a-out\.denied\{color:var\(--state-fail\)\}\.cyg-a-out\.failed\{color:var\(--state-warn\)\}'/.test(CSS)
  && !/AVATAR_COLOURS/.test(VIEW) && !/cyg-a-av/.test(VIEW)
  && !/var\(--teal\)|var\(--purple\)|var\(--green\)|var\(--amber\)|var\(--red\)/.test(VIEW));
check('a gap row is a 3px state-warn rule, state-fail when capture was off',
  /'\.cyg-a-table tr\.gap td\{[^']*border-left:3px solid var\(--state-warn\)\}'/.test(CSS)
  && /'\.cyg-a-table tr\.gap\.off td\{border-left-color:var\(--state-fail\)\}'/.test(CSS));

/* ── 5. Capture settings ────────────────────────────────────────────────── */
check('the four always-on categories render as locked with a note, never as disabled switches',
  /if \(c\.alwaysOn\) \{/.test(VIEW)
  && /<span class="cyg-a-locked" title="Locked: recorded whatever the capture state">Locked<\/span>/.test(VIEW)
  && /Always recorded — a pause cannot hide it and no role can switch it off\./.test(VIEW)
  && !/\(\(c\.alwaysOn \|\| !can\) \? ' disabled' : ''\)/.test(VIEW)
  && /A locked category is a fact, not a control/.test(VIEW));
check('the capture state control and its modals are untouched',
  /id="cyg-a-state-seg"/.test(VIEW) && /function askPause\(\)/.test(VIEW) && /function askOff\(\)/.test(VIEW)
  && /body\.confirm\.toUpperCase\(\) !== 'OFF'/.test(VIEW));

/* ── 6. Integrity and retention ─────────────────────────────────────────── */
check('the Integrity tab keeps the chain blocks and the known limit; Verify lives in the header',
  /'<h3>Hash chain<\/h3>'/.test(VIEW) && /tamper-evident, not tamper-proof/.test(VIEW)
  && !/Verify the full chain<\/button>/.test(VIEW)
  && /Verify chain is in the header\./.test(VIEW));
check('Retention has its own tab with the run control; the policy stays under Capture settings',
  /function renderRetentionPanel\(\)[\s\S]{0,300}el\.innerHTML = retentionHtml\(\);/.test(VIEW)
  && /if \(pb\) pb\.addEventListener\('click', runPurge\);/.test(VIEW)
  && /<div class="cyg-a-radio" id="cyg-a-ret">/.test(VIEW) && /Nothing has been purged yet/.test(VIEW));

/* ── 7. Type and colour discipline ──────────────────────────────────────── */
const small = (CSS.match(/font-size:\s*(\d+(?:\.\d+)?)px/g) || []).map(m => Number(m.match(/[\d.]+/)[0])).filter(n => n < 11);
check('nothing in the screen\'s stylesheet is below 11px, and body text is 13px or more',
  small.length === 0 && /'\.cyg-a-table\{[^']*font-size:13px/.test(CSS), small.join(','));
check('no border-radius, no rgba(), no hex — every colour is a token and every corner is square',
  !/border-radius/.test(CSS) && !/rgba\(/.test(CSS) && !/#[0-9a-f]{6}/i.test(CSS.replace(/#fff\b/g, '')));
check('the drawer, the modals and the toast take the tokens too',
  /'\.cyg-a-dh h2\{[^']*font-family:var\(--font-heading\)[^']*text-transform:uppercase/.test(CSS)
  && /'\.cyg-a-mbox\{background:var\(--color-bg\);border:1px solid var\(--color-divider\)/.test(CSS)
  && /'\.cyg-a-toast\{[^']*background:var\(--color-accent-900\);color:var\(--color-bg\)/.test(CSS));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
if (fail) process.exit(1);
