// tests/collation-match.test.js — the comparisons that happen in JavaScript.
//
// Stage B taught Cygenix to find a collation clash in SQL and Stage C to fix
// it at the point the SQL is built. Neither can reach a comparison that never
// becomes SQL, and there are four of those: the Trial Balance and the GL
// balancing check each read their own side and merge the two in a Map; a
// migration run's grouped reconciliation does the same with its group keys;
// and the evidence mapper intersects two sampled value sets. A fifth turned
// up while wiring them — Assurance compared a recon pair with Number() on
// both sides, which made a MIN or MAX over a text column pass every time.
//
// `===` is neither of the things a database does: always case-sensitive,
// always accent-sensitive. Under a case-insensitive profile — which is most
// of them — ACC001 and acc001 came out of the Trial Balance as an account
// missing from one side AND an unexpected account on the other. Two invented
// differences on the screen whose entire job is to say whether the two sides
// agree.
//
// This file pins the rule, the wiring at each of the five places, and the
// property that makes the change safe to make everywhere at once: with no
// collation detected, every one of them behaves exactly as it did before.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

const R = require(path.join(ROOT, 'public', 'cygenix-collation-rules.js'));
const E = require(path.join(ROOT, 'public', 'cygenix-evidence-map.js'));
const A = require(path.join(ROOT, 'public', 'cygenix-assurance.js'));
const BAL = read('public', 'balancing.html');
const PB = read('public', 'project-builder-app.js');
const ASSUR = read('public', 'assurance.html');
const EM = read('public', 'cygenix-evidence-map.js');
const CARD = read('public', 'cygenix-collation.js');

const model = (c) => ({ source: { dbCollation: c }, target: { dbCollation: c }, strategy: 'target' });
const CI = R.keyFolder(model('Latin1_General_CI_AS'));
const CS = R.keyFolder(model('Latin1_General_CS_AS'));
const AI = R.keyFolder(model('Latin1_General_CI_AI'));
const NONE = R.keyFolder(model(''));

console.log('Collation matching — the comparisons JavaScript makes\n');

/* ── 1. The rule itself ─────────────────────────────────────────────────── */
section('1. What counts as the same value');

check('under a case-insensitive profile two spellings of a code are one key',
  CI.fold('ACC001') === CI.fold('acc001'));
check('under a case-sensitive one they stay two',
  CS.fold('ACC001') !== CS.fold('acc001'));
check('an accent-insensitive profile folds the accent as well',
  AI.fold('Café') === AI.fold('Cafe') && CI.fold('Café') !== CI.fold('Cafe'));
check('a trailing space never separates two values, because = pads them',
  CI.fold('ACC001  ') === CI.fold('ACC001') && CS.fold('ACC001 ') === CS.fold('ACC001'));
check('a leading space is a different value and stays one',
  CI.fold(' ACC001') !== CI.fold('ACC001'));
check('WITH NOTHING DETECTED THE FOLD IS THE IDENTITY — the behaviour before this existed',
  NONE.fold('ACC001') === 'ACC001' && NONE.fold('acc001') === 'acc001'
  && NONE.fold('ACC001 ') === 'ACC001 ' && NONE.applied === false);
check('and each folder says which rule it applied, for the screen to repeat',
  /ignoring case/.test(CI.label) && /Latin1_General_CI_AS/.test(CI.label)
  && NONE.label === '', CI.label + ' | ' + NONE.label);

/* ── 2. Trial Balance and the GL check ──────────────────────────────────── */
section('2. balancing.html — the two merges');

check('the page asks the collation module for a folder rather than folding its own way',
  /function balKeyFolder\(\)\{/.test(BAL)
  && /window\.cygCollation\.matcher\(\)/.test(BAL));
check('and falls back to an identity fold, so Balancing runs without that module',
  /return \{ applied: false, label: '', fold: \(v\) => \(v == null \? '' : String\(v\)\) \};/.test(BAL));
check('THE TRIAL BALANCE KEY IS FOLDED on both parts of (period, account)',
  /const key = \(row\) => fold\.fold\(row\.period == null \? '' : row\.period\)\s*\n\s*\+ '\|' \+ fold\.fold\(row\.account == null \? '' : row\.account\);/.test(BAL));
check('AND THE GL CHECK IS FOLDED THE SAME WAY',
  /const key = r => fold\.fold\(r\.period\) \+ '\|' \+ fold\.fold\(r\.account\);/.test(BAL));
check('the target row travels with its figure, so the key is never taken apart again',
  !/const \[period, account\] = k\.split\('\|'\)/.test(BAL)
  && /for \(const hit of tMap\.values\(\)\)/.test(BAL),
  'splitting the folded key on | would print a lower-cased code, and lost any code containing a |');
check('what is DISPLAYED is the original value, not the folded one',
  /period: hit\.period,\s*\n\s*account: hit\.account,/.test(BAL));
check('and the report says which matching rule produced its rows',
  /matchNote: fold\.applied \? fold\.label : '',/.test(BAL)
  && /run\.matchNote/.test(BAL));
check('the GL check says it too, in its detail text',
  /\+ \(fold\.applied \? '\\n' \+ fold\.label : ''\)/.test(BAL));
check('balancing.html loads the collation module at all',
  /<script src="\/cygenix-collation\.js\?v=[0-9a-f]+" defer><\/script>/.test(BAL));

/* ── 3. The migration run's grouped reconciliation ──────────────────────── */
section('3. project-builder-app.js — grouped reconciliation');

check('the run asks for the same folder, guarded the same way',
  /function reconKeyFolder\(\)\{/.test(PB) && /window\.cygCollation\.matcher\(\)/.test(PB));
check('BOTH SIDES ARE KEYED BY THE FOLDED GROUP KEY',
  /new Map\(srcGroups\.map\(g => \[fold\.fold\(g\.key\), \{ value: g\.value, label: g\.key \}\]\)\)/.test(PB)
  && /new Map\(tgtGroups\.map\(g => \[fold\.fold\(g\.key\), \{ value: g\.value, label: g\.key \}\]\)\)/.test(PB));
check('and the ORIGINAL key is what comes back in the result',
  /key: \(sHit \? sHit\.label : tHit\.label\)/.test(PB),
  'a lower-cased account code in a reconciliation report is its own small lie');
check('the run log states the rule, at every one of the three places it reports',
  (PB.match(/matchNote\) add\(/g) || []).length === 3);
check('and the result carries it for the report',
  /matchNote: fold\.applied \? fold\.label : '',/.test(PB));

/* ── 4. The evidence mapper ─────────────────────────────────────────────── */
section('4. cygenix-evidence-map.js — sampled value overlap');

const srcVals = ['CUST01', 'CUST02', 'CUST03', 'CUST04'];
const tgtSame = ['cust01', 'cust02', 'cust03', 'cust04'];
const overlap = (folder) => E.emOverlap(E.emProfile(srcVals, folder), E.emProfile(tgtSame, folder));

check('A CASE-SENSITIVE PROFILE NO LONGER CLAIMS A PERFECT OVERLAP',
  overlap(CS) === 0,
  'the old code lower-cased unconditionally and scored these 100%, proposing a mapping on a match the database will not make');
check('a case-insensitive profile still sees them as the same values',
  overlap(CI) === 1);
check('WITH NO PROFILE THE OLD LOWERCASE DEFAULT STANDS — an overlap score is a heuristic',
  overlap(NONE) === 1 && overlap(null) === 1 && overlap(undefined) === 1);
check('an accent-insensitive profile matches across an accent',
  E.emOverlap(E.emProfile(['Café', 'Zoë'], AI), E.emProfile(['Cafe', 'Zoe'], AI)) === 1
  && E.emOverlap(E.emProfile(['Café', 'Zoë'], CI), E.emProfile(['Cafe', 'Zoe'], CI)) === 0);
check('genuinely different values do not overlap under any profile',
  [CI, CS, AI, NONE, null].every((f) =>
    E.emOverlap(E.emProfile(['A1', 'A2'], f), E.emProfile(['B1', 'B2'], f)) === 0));
check('the distinct count follows the same rule, because it counts the same set',
  E.emProfile(['A', 'a'], CI).distinct === 1 && E.emProfile(['A', 'a'], CS).distinct === 2);
check('the pure core still takes the folder as an argument rather than reaching for a global',
  /function emProfile\(values, folder\)/.test(EM)
  && EM.indexOf('function emProfile') < EM.indexOf('function emCollationFolder'),
  'the test suite loads this file directly, with no window');
check('only the orchestrator reaches for one, and it is guarded',
  /function emCollationFolder\(\)/.test(EM) && /typeof window !== 'undefined'/.test(EM)
  && /const folder = opts\.folder \|\| emCollationFolder\(\);/.test(EM));
check('both sides are profiled with the SAME folder',
  /emProfile\(srcRows\.map\(r => r\[c\]\), folder\)/.test(EM)
  && /emProfile\(tgtRows\.map\(r => r\[c\.name\]\), folder\)/.test(EM),
  'two sets folded by different rules cannot be intersected and mean anything');

/* ── 5. Assurance — the recon pair that could not fail ──────────────────── */
section('5. cygenix-assurance.js — a text aggregate is not a number');

const rule0 = { params: { tolerance: 0 } };
check('numbers behave exactly as they did: equal passes, different fails, tolerance holds',
  A.asEvaluatePair(rule0, 100, 100).status === 'pass'
  && A.asEvaluatePair(rule0, 100, 99).status === 'fail'
  && A.asEvaluatePair({ params: { tolerance: 2 } }, 100, 99).status === 'pass');
check('and the measure is still the numeric difference',
  A.asEvaluatePair(rule0, 100, 99).measure === 1
  && A.asEvaluatePair(rule0, 100, 100).measure === 0);
check('A TEXT MIN/MAX THAT DIFFERS NOW FAILS — it used to pass, every time',
  A.asEvaluatePair(rule0, 'ACC001', 'ACC999').status === 'fail',
  'Number("ACC001") is NaN, NaN || 0 is 0, and 0 vs 0 is a difference of nothing');
check('a text pair that agrees passes',
  A.asEvaluatePair(rule0, 'ACC001', 'ACC001').status === 'pass');
check('and the text case goes through the folder, because this comparison has no SQL either',
  A.asEvaluatePair(rule0, 'ACC001', 'acc001', CI).status === 'pass'
  && A.asEvaluatePair(rule0, 'ACC001', 'acc001', CS).status === 'fail'
  && A.asEvaluatePair(rule0, 'ACC001', 'acc001').status === 'fail');
check('the failure says it was a text comparison, so nobody reads the tolerance as ignored',
  /text comparison/.test(A.asEvaluatePair(rule0, 'ACC001', 'ACC999').why));
check('a numeric string is still a number — a COUNT(*) coming back as text is not text',
  A.asIsNumeric('100') === true && A.asIsNumeric(100) === true
  && A.asIsNumeric('ACC001') === false && A.asIsNumeric(null) === true);
check('two empty sides stay the pass they were, rather than becoming a text comparison',
  A.asEvaluatePair(rule0, null, null).status === 'pass');
check('the page passes the RAW value now, not Number(...) of it',
  /const va = ra\[0\] \? \(ra\[0\]\.n \?\? ra\[0\]\.N \?\? 0\) : 0;/.test(ASSUR)
  && /A\.asEvaluatePair\(rule, va, vb, asCollationFolder\(\)\)/.test(ASSUR));
check('and takes the measure from the evaluation rather than recomputing it on text',
  /measure = v\.measure;/.test(ASSUR) && !/measure = Math\.abs\(va - vb\)/.test(ASSUR));
check('with the folder guarded, so Assurance runs without the collation module',
  /function asCollationFolder\(\)\{/.test(ASSUR) && /catch \(e\) \{ \/\* the rule still runs, comparing exactly \*\//.test(ASSUR));

/* ── 6. One rule, reached one way ───────────────────────────────────────── */
section('6. The wiring');

check('the card exposes a matcher, and it is the rules module\'s folder',
  /function matcher\(profileId\) \{/.test(CARD)
  && /return Rules\.keyFolder\(settings\(profileId\)\);/.test(CARD)
  && /matcher: matcher,/.test(CARD));
check('it never throws, whatever the profile store holds',
  /catch \(e\) \{ return Rules\.keyFolder\(null\); \}/.test(CARD));
/* Scoped to the merges themselves. Elsewhere in these files .toLowerCase()
   compares IDENTIFIERS — table names, job names, column names — which is
   correct and has nothing to do with what the data means. The rule here is
   only about comparing VALUES: a second definition of "the same value" is
   how the first one stops being true. */
const slice = (src, from, to) => src.slice(src.indexOf(from), src.indexOf(to));
const MERGES = [
  ['the Trial Balance merge', slice(BAL, 'async function tbRun(idx){', 'async function tbRunAll(')],
  ['the grouped reconciliation', slice(PB, '// Grouped case: outer-join', 'function reconKeyFolder(){')],
];
MERGES.forEach(([name, body]) => {
  check(name + ' folds through the shared rule and nowhere else',
    /fold\.fold\(/.test(body) && !/toLowerCase\(\)|toUpperCase\(\)|normalize\(/.test(body), name);
});
check('the rules module ships the matching helpers to both copies',
  /matchRules: matchRules, foldKey: foldKey, keysEqual: keysEqual,/.test(read('public', 'cygenix-collation-rules.js'))
  && read('azure-function', 'src', 'collation-rules.js') === read('public', 'cygenix-collation-rules.js'));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
