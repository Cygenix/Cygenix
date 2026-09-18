// tests/object-map-open.test.js — opening a saved map, and reading a reply
// that is not the reply we expected.
//
// Reported Sep-2026: a map created by Conversion Templates would not open.
// Both tables loaded, the target's name sat in the search box, and the grid
// never appeared. The message on screen was "Remap error: Unexpected end of
// JSON input", which came from the Re-map the user pressed afterwards trying
// to get SOMETHING to happen — not from the open at all.
//
// Two separate faults, and the second one hides behind that one sentence
// twice over:
//
//   1. A saved map with an empty columnMapping was treated as a broken map.
//      It is a DRAFT — which is exactly what Conversion Templates creates
//      when it sends a module across, because a template knows the
//      staging→target pair and nothing about the columns. The restore said
//      "No column mapping found in this job" and stopped, leaving nothing on
//      screen to edit.
//
//   2. `await res.json()` on an empty or non-JSON body throws "Unexpected end
//      of JSON input" BEFORE the res.ok check, so the HTTP status that would
//      have explained it is never read. And a reply cut off at the output
//      limit throws the SAME message from JSON.parse — discarding the sixty
//      columns it had matched because of the one it did not finish.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

const app = read('public', 'object-mapping-app.js');
const html = read('public', 'object_mapping.html');

console.log('Object Mapping — opening a saved map\n');

/* Lift a function out of the page script by name. The page has no module
   boundary and never will — it is a <script src> in a static file — so the
   only way to exercise its logic in Node is to take the source of the one
   function under test. Doing that is also a check in itself: if the function
   is renamed or reshaped, this stops finding it. */
function lift(name, src) {
  const m = src.match(new RegExp('(?:async )?function ' + name + '\\([\\s\\S]*?\\n\\}', 'm'));
  if (!m) throw new Error('could not find ' + name);
  return new Function('return (' + m[0] + ')')();
}

const parseMappingArray = lift('parseMappingArray', app);
const readApiJson = lift('readApiJson', app);

/* ── A reply that stopped mid-sentence ─────────────────────────────────── */
console.log('— a truncated reply —');

const WHOLE = '[{"srcCol":"A","tgtCol":"A"},{"srcCol":"B","tgtCol":"B"}]';
const CUT = '[{"srcCol":"A","tgtCol":"A"},{"srcCol":"B","tgtCol":"B"},{"srcCol":"C","tgtCo';

check('a complete reply parses, and is not reported as truncated',
  (() => {
    const r = parseMappingArray(WHOLE, 'end_turn');
    return r.rows.length === 2 && r.truncated === false;
  })());

check('a reply cut off mid-object keeps every column it DID finish',
  (() => {
    const r = parseMappingArray(CUT, 'max_tokens');
    return r.truncated === true && r.rows.length === 2
      && r.rows[0].tgtCol === 'A' && r.rows[1].tgtCol === 'B';
  })(), JSON.stringify(parseMappingArray(CUT, 'max_tokens')));

check('a half-written object is dropped whole — never half-read',
  parseMappingArray(CUT, 'max_tokens').rows.every(r => r.srcCol && r.tgtCol));

check('cut off before the first object closes is not salvageable, and says why',
  (() => {
    try { parseMappingArray('[{"srcCol":"A","tgtCo', 'max_tokens'); return false; }
    catch (e) { return /cut off by its length limit/.test(e.message); }
  })());

check('…and when it was not a length problem the error says THAT instead',
  (() => {
    try { parseMappingArray('I could not map these columns.', 'end_turn'); return false; }
    catch (e) { return /not a JSON array/.test(e.message)
      && /It began: I could not map/.test(e.message) && !/length limit/.test(e.message); }
  })());

check('the salvage only ever trims — it never invents a row',
  (() => {
    const r = parseMappingArray(CUT, 'max_tokens');
    return JSON.stringify(r.rows) === '[{"srcCol":"A","tgtCol":"A"},{"srcCol":"B","tgtCol":"B"}]';
  })());

check('an empty reply says it was empty, rather than describing a parser',
  (() => {
    try { parseMappingArray('', 'end_turn'); return false; }
    catch (e) { return /the reply was empty/.test(e.message); }
  })());

check('"Unexpected end of JSON input" never reaches the user from here',
  (() => {
    const msgs = [];
    [['[{"a":1', 'max_tokens'], ['not json', 'end_turn'], ['', 'end_turn']].forEach(([raw, sr]) => {
      try { parseMappingArray(raw, sr); } catch (e) { msgs.push(e.message); }
    });
    return msgs.length === 3 && msgs.every(m => !/Unexpected end of JSON input/.test(m));
  })());

/* ── A reply that is not JSON at all ───────────────────────────────────── */
/* These four are awaited inside main() below. A promise handed straight to
   check() is truthy whether it resolves true or false, which would make every
   one of them pass for ever without testing a thing. */
async function main(){
console.log('\n— a reply that is not a reply —');

const fakeRes = (status, statusText, body) => ({
  status, statusText,
  text: async () => { if (body instanceof Error) throw body; return body; },
});

check('an empty body reports the HTTP status, which is what actually happened',
  await (async () => {
    try { await readApiJson(fakeRes(502, 'Bad Gateway', '')); return false; }
    catch (e) { return /empty reply/.test(e.message) && /502/.test(e.message); }
  })());

check('an HTML error page reports the status and a snippet, not a parser message',
  await (async () => {
    try { await readApiJson(fakeRes(403, 'Forbidden', '<html><body>Blocked</body></html>')); return false; }
    catch (e) { return /not JSON/.test(e.message) && /403/.test(e.message) && /Blocked/.test(e.message); }
  })());

check('a cut connection says the connection was cut',
  await (async () => {
    try { await readApiJson(fakeRes(200, 'OK', new Error('network error'))); return false; }
    catch (e) { return /connection to Claude was cut off/.test(e.message); }
  })());

check('a real reply is returned unchanged',
  await (async () => {
    const d = await readApiJson(fakeRes(200, 'OK', '{"content":[{"text":"[]"}]}'));
    return d.content[0].text === '[]';
  })());

check('the status is read BEFORE the body is trusted — res.ok no longer comes second',
  /const data = await readApiJson\(res\);/.test(app)
  && /Claude returned HTTP '\+res\.status/.test(app));

/* ── The draft map ─────────────────────────────────────────────────────── */
console.log('\n— a map with nothing mapped yet —');

check('a saved map with no columns no longer dead-ends on an error',
  !/showStatus\('No column mapping found in this job','err'\)/.test(app),
  (app.match(/No column mapping found in this job/) || [''])[0]);

check('it opens on a name-matched grid instead, built from the target',
  (() => {
    const m = app.match(/\} else if\(tgtTable\)\{[\s\S]*?\n    \} else \{/);
    return !!m && /ensureAllTargetCols\(autoMap\(srcTable, tgtTable\), tgtTable\)/.test(m[0])
      && /renderMappingTable\(\)/.test(m[0])
      && /\$\('mapping-wrap'\)\.style\.display='block'/.test(m[0]);
  })());

check('opening a map never calls Claude — that costs money nobody asked to spend',
  (() => {
    const m = app.match(/\} else if\(tgtTable\)\{[\s\S]*?\n    \} else \{/);
    return !!m && !/askClaudeForMapping|buildMappingIfReady|apiKey/.test(m[0]);
  })());

check('it says what it did, and that nothing is saved until Save as job',
  (() => {
    const m = app.match(/\} else if\(tgtTable\)\{[\s\S]*?\n    \} else \{/);
    return !!m && /by name\./.test(m[0]) && /Save as job/.test(m[0]);
  })());

check('and a map with no target at all still says so rather than showing an empty grid',
  /No column mapping in this job, and no target table to build one from/.test(app));

check('the note survives the generic "Editing:" line that follows it',
  /showStatus\(_restoreNote \|\| \('Editing: "'\+job\.name/.test(app)
  && /_restoreNote \? 'warn' : 'info'/.test(app));

check('a warning stays on screen like an error does, rather than fading in four seconds',
  /if\(type!=='err' && type!=='warn'\) setTimeout/.test(app));

check('…and it has a style of its own, so it does not render as bare text',
  /\.status-warn\{/.test(html));

/* ── The thing that made this reachable at all ─────────────────────────── */
check('a template-created map really does carry an empty mapping — this is its normal shape',
  /columnMapping: \[\]/.test(read('public', 'cygenix-template-mapping.js'))
  && /status: 'draft'/.test(read('public', 'cygenix-template-mapping.js')));

check('the output limit is raised, because one entry per target column outgrew 4096',
  (() => {
    const m = app.match(/async function askClaudeForMapping[\s\S]*?max_tokens:(\d+)/);
    return !!m && Number(m[1]) >= 8192;
  })());

console.log('\n' + pass + ' passed, ' + fail + ' failed');
if (fail) process.exit(1);
}

main().catch(e => { console.error(e); process.exit(1); });
