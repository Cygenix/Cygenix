// tests/mapper.test.js — the older Object Mapping screen.
//
// public/mapper.html is the Object Mapping journey as it was before
// object-mapping-app.js replaced it. Nothing links to it any more, but it is
// still routed, still deployed, and still does the whole job: connect to two
// databases, map columns with Claude's help, generate a cross-database
// INSERT…SELECT, save it as a job the Task Agent will run. A page that can
// still move somebody's data is a page that has to be right.
//
// It had been left behind twice over:
//
//   · the collation wiring went into every other screen that maps columns or
//     generates SQL and never reached this one, so the part of the product
//     that exists to say "these two columns will not compare cleanly" was
//     silent exactly where columns are matched up;
//   · jobs saved here carried no collation stamp, which made them the one
//     kind of job the Jobs screen could never mark for regeneration;
//   · parseTypeLen and isCharType were each declared TWICE, and in a classic
//     script the last declaration wins for the whole file. The two copies of
//     parseTypeLen had drifted apart in both directions without anything
//     failing, because the dead one was never called;
//   · the Anthropic key was read from localStorage alone, so an operator who
//     had set it on the Connect page — which stores it in sessionStorage —
//     was asked for a key they had already given.
//
// The collation wiring itself is pinned in tests/collation-rules.test.js,
// with the rest of the clash-point inventory. This file holds what is
// specific to this page.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');
const SRC = read('public', 'mapper.html');

console.log('Mapper — the older Object Mapping screen\n');

/* ── 1. It still parses, and each name is declared once ─────────────────── */

const blocks = [...SRC.matchAll(/<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/g)].map((m) => m[1]);
check('every inline script block parses', blocks.length >= 2 && blocks.every((b) => {
  try { new Function(b); return true; } catch (e) { return false; }
}), blocks.length + ' blocks');

/* The general rule, not just the two that were wrong. A second declaration
   of the same name silently replaces the first for the entire file, and it
   fails in the quietest possible way: the version a reader is looking at is
   not the version that runs. */
const declared = {};
for (const m of SRC.matchAll(/^\s*(?:async\s+)?function\s+([A-Za-z_$][\w$]*)\s*\(/gm)) {
  declared[m[1]] = (declared[m[1]] || 0) + 1;
}
const dupes = Object.keys(declared).filter((k) => declared[k] > 1);
check('NO FUNCTION IS DECLARED TWICE — the last one would silently win',
  dupes.length === 0, dupes.join(', '));

/* ── 2. parseTypeLen, which is what the duplication broke ───────────────── */

const body = SRC.slice(SRC.indexOf('function parseTypeLen(t){'), SRC.indexOf('function isCharType'));
const parseTypeLen = new Function(body + '\nreturn parseTypeLen;')();

check('a declared length is read', parseTypeLen('VARCHAR(50)') === 50
  && parseTypeLen('NVARCHAR(255)') === 255 && parseTypeLen('NVARCHAR( 40 )') === 40);
check('MAX is no limit', parseTypeLen('VARCHAR(MAX)') === Infinity
  && parseTypeLen('NVARCHAR(max)') === Infinity);
check('AND SO IS -1, which is how sys.columns reports MAX',
  parseTypeLen('NVARCHAR(-1)') === Infinity,
  'a negative length reaching the truncation check emits LEFT(expr, -1), which SQL Server rejects');
check('a precision reads as a precision rather than as no limit',
  parseTypeLen('DECIMAL(18,2)') === 18);
check('a type with no length at all is unlimited, not zero',
  parseTypeLen('INT') === Infinity && parseTypeLen('') === Infinity
  && parseTypeLen(null) === Infinity && parseTypeLen(undefined) === Infinity);
check('the truncation check only ever runs on char types, so a precision never reaches it',
  /isCharType\(srcT\)&&isCharType\(tgtT\)&&parseTypeLen\(srcT\)>parseTypeLen\(tgtT\)/.test(SRC));

/* ── 3. A saved job carries what the Jobs screen needs ──────────────────── */

check('BOTH save paths stamp the collation settings they generated under',
  (SRC.match(/collationStamp: mapCollationStamp\(\),/g) || []).length === 2,
  'single-table and one-to-many are two separate job objects in this file');
check('and the stamp comes from the shared module, guarded',
  /function mapCollationStamp\(\)\{/.test(SRC)
  && /window\.cygCollation \? window\.cygCollation\.stamp\(\) : ''/.test(SRC)
  && /catch\(e\)\{ return ''; \}/.test(SRC));
check('a job from here is still profile-stamped as it always was',
  (SRC.match(/CygenixJobProfile\.attach\(job, jobs\)/g) || []).length === 2);

/* ── 4. The collation surface, on the page as well as in the script ─────── */

check('the rules load before the card that refuses to start without them',
  SRC.indexOf('cygenix-collation-rules.js') > 0
  && SRC.indexOf('cygenix-collation-rules.js') < SRC.indexOf('cygenix-collation.js?'));
check('the summary sits above the mapping it describes',
  SRC.indexOf('id="map-collation-summary"') < SRC.indexOf('id="mapping-table"'));
check('and the clash banner sits under the SQL it is about',
  SRC.indexOf('id="sql-output"') < SRC.indexOf('id="map-collation-banner"'));
check('the summary is drawn in THIS page\'s idiom, not with classes it has no stylesheet for',
  !/class="cx-attn/.test(SRC) && /border-left:2px solid '\+colour\+'/.test(SRC),
  'this page predates the console stylesheet; cx-attn markup renders here as bare text');
check('the summary is rebuilt with the table, so it cannot describe a stale mapping',
  /mapRenderCollationSummary\(\);\s*\n\s*\/\/ Re-apply any active filter/.test(SRC));
check('THE BANNER FOLLOWS THE TAB — clashes are not attributed to the wrong script',
  /function showSQLTab\(tab\)\{[\s\S]{0,700}mapLintSQL\(\$\('sql-output'\)\.textContent\);/.test(SRC));
check('a fixed literal gets no badge, because there is no source column to compare',
  /const collBadge = hasFixed \? '' : mapCollationBadge\(m\);/.test(SRC));
check('every collation call is wrapped, so a linter cannot break the mapping screen',
  (SRC.match(/if\(!window\.cygCollation/g) || []).length >= 3
  && (SRC.slice(SRC.indexOf('function mapCollationRefs'), SRC.indexOf('function renderMappingTable'))
       .match(/catch\(e\)\{/g) || []).length >= 4);

/* Stage B reports; it does not rewrite. This page's generator must be
   byte-for-byte what it was. */
const gen = SRC.slice(SRC.indexOf('function generateSingleSQL'), SRC.indexOf('function updateTransform'));
check('THE GENERATED SQL IS UNCHANGED — no COLLATE was introduced into it',
  !/COLLATE/i.test(gen),
  'an INSERT converts to the target column\'s collation on its own; a COLLATE on the SELECT would change nothing and claim something');
const otm = SRC.slice(SRC.indexOf('function generateOTMSQL'), SRC.indexOf('function showSQLOutput'));
check('and the one-to-many generator is unchanged too', !/COLLATE/i.test(otm));

/* ── 5. The API key ─────────────────────────────────────────────────────── */

check('the key is read through the shared helper, which tries sessionStorage first',
  /CygenixModel\.userKey\(\)/.test(SRC),
  'the Connect page stores it in sessionStorage; reading localStorage alone asked for it twice');
check('with the direct read kept as the fallback, so the page works without that module',
  /localStorage\.getItem\('cygenix_api_key'\) \|\| ''/.test(SRC));
check('NO KEY IS HARD-CODED, here or anywhere near it',
  !/sk-ant-[A-Za-z0-9]/.test(SRC));

/* ── 6. Nothing else about the page moved ───────────────────────────────── */

check('it is still routed, and still by the extensionless form',
  /^\/mapper\s+\/mapper\.html\s+200$/m.test(read('public', '_redirects')));
// The no-emoji rule and its KEEP set belong to tests/icons.test.js, which
// already reads every page in public/. Restating the range here, less well,
// would only be a second rule to disagree with the first.
check('the page still loads its own audit recorder and sync, as it did',
  /cygenix-audit\.js/.test(SRC) && /cygenix-cosmos-sync\.js/.test(SRC));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
