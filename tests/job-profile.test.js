// tests/job-profile.test.js — which connection profile a job belongs to.
//
// Requested: show the profile on every row of the Migration Jobs list, and
// link every NEW job to one when it is created. A job had no profile field at
// all, and nineteen places in twelve files mint a job.
//
// Three things make this worth pinning rather than reading once:
//
//   * NINETEEN CALL SITES. The value of a shared helper is entirely in
//     everybody using it. A creation point that forgets produces a job with
//     no profile, which looks exactly like a job made before the feature
//     existed — so the gap is invisible for as long as you care to look.
//     The wiring section below counts them.
//   * EDITS MUST NOT RE-STAMP. Most save handlers rebuild the job object from
//     the form, so an edited job arrives with no profile even though the
//     stored copy has one. stamp() alone would quietly re-stamp it with
//     today's profile — a change to an existing job, which the brief
//     explicitly rules out.
//   * TWO SOURCES, ONE COLUMN. A binding (deliberate, on the Profiles page)
//     and a stamp (provenance, at creation) can disagree. The precedence is a
//     decision, not an accident, so it is asserted rather than described.
'use strict';

const fs = require('fs');
const path = require('path');
const JP = require('../public/cygenix-job-profile.js');
const CP = require('../public/cygenix-profiles.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

console.log('Job profile — which profile a job belongs to\n');

const store = () => ({
  v: 1,
  profiles: [
    { id: 'FIN_3E_UAT', name: 'Finance UAT', envClass: 'TEST', status: 'active' },
    { id: 'FIN_3E_PRD', name: 'Finance production', envClass: 'PRD', status: 'active' },
    { id: 'FIN_OLD', name: 'Retired one', envClass: 'TEST', status: 'retired' },
  ],
  bindings: [],
  settings: { activeProfileId: 'FIN_3E_UAT' },
});

/* ── 1. Stamping a new job ──────────────────────────────────────────────── */

check('a new job gets the active profile — the id AND the name',
  (() => { const j = JP.stamp({ id: 'job_1' }, store());
    return j.profileId === 'FIN_3E_UAT' && j.profileName === 'Finance UAT'; })());
// The id is immutable (cpSaveProfile refuses to change it) and the name is
// free text. Storing both means a rename keeps the link and a delete keeps
// the label.
check('the id is the link and the name is only a snapshot',
  (() => { const s = store(); const j = JP.stamp({ id: 'job_1' }, s);
    s.profiles[0].name = 'Renamed';
    const r = JP.of(j, s);
    return j.profileId === 'FIN_3E_UAT' && r.name === 'Renamed' && r.source === 'job'; })(),
  'a renamed profile must still resolve through the id');
check('a job that already carries a profile is never re-stamped',
  (() => { const j = JP.stamp({ id: 'job_1', profileId: 'FIN_3E_PRD', profileName: 'x' }, store());
    return j.profileId === 'FIN_3E_PRD'; })());

// No profile must never block a job being created — the brief is explicit.
check('no active profile still returns the job, with the fields absent rather than empty',
  (() => { const s = store(); s.settings.activeProfileId = null;
    const j = JP.stamp({ id: 'job_1' }, s);
    return j.id === 'job_1' && !('profileId' in j) && !('profileName' in j); })());
// An empty string would read as "we tried and failed"; absence reads as
// "made before profiles". Only one of those is true, and the jobs list shows
// the same dash either way.
check('a RETIRED selected profile is not stamped — a link that could never run',
  (() => { const s = store(); s.settings.activeProfileId = 'FIN_OLD';
    return !('profileId' in JP.stamp({ id: 'job_1' }, s)); })());
check('a missing store does not throw and does not block',
  JP.stamp({ id: 'job_1' }, null).id === 'job_1'
  && JP.stamp(null) === null);

/* ── 2. attach() — the call every save handler makes ────────────────────── */

check('a job with no previous copy is stamped',
  JP.attach({ id: 'new' }, [], store()).profileId === 'FIN_3E_UAT');
// The fault this exists to prevent: a save handler rebuilds the job from the
// form, so an EDIT arrives with no profile. Stamping it would change an
// existing job to today's profile — which the brief rules out.
check('an EDIT carries the stored profile forward instead of taking today\'s',
  (() => { const s = store(); s.settings.activeProfileId = 'FIN_3E_PRD';
    const prev = [{ id: 'job_9', profileId: 'FIN_3E_UAT', profileName: 'Finance UAT' }];
    const j = JP.attach({ id: 'job_9' }, prev, s);
    return j.profileId === 'FIN_3E_UAT' && j.profileName === 'Finance UAT'; })(),
  'editing a job must not move it to whichever profile is selected now');
check('an edit of a job that never had one is stamped, not left blank',
  JP.attach({ id: 'job_9' }, [{ id: 'job_9' }], store()).profileId === 'FIN_3E_UAT');
check('a previous array that is missing or not an array is survivable',
  JP.attach({ id: 'a' }, null, store()).profileId === 'FIN_3E_UAT'
  && JP.attach({ id: 'a' }, 'nonsense', store()).profileId === 'FIN_3E_UAT');

/* ── 3. Resolution — the precedence the column shows ────────────────────── */

check('an old job with nothing resolves to nothing, so the row shows a dash',
  JP.of({ id: 'old' }, store()) === null);
check('a stamped job resolves through its id to the live profile',
  (() => { const r = JP.of({ id: 'j', profileId: 'FIN_3E_PRD' }, store());
    return r.name === 'Finance production' && r.envClass === 'PRD' && r.source === 'job'; })());
// The decision: binding beats stamp. Somebody binding a job is a deliberate
// act about where it RUNS; a stamp is a record of what was selected the day
// it was made.
check('a BINDING outranks the creation stamp — a deliberate act beats a default',
  (() => { const s = store();
    s.bindings = [{ artifactType: 'job', artifactId: 'j', profileId: 'FIN_3E_PRD' }];
    const r = JP.of({ id: 'j', profileId: 'FIN_3E_UAT' }, s);
    return r.id === 'FIN_3E_PRD' && r.source === 'binding'; })());
check('a binding for a DIFFERENT artifact type is not mistaken for this job\'s',
  (() => { const s = store();
    s.bindings = [{ artifactType: 'report', artifactId: 'j', profileId: 'FIN_3E_PRD' }];
    return JP.of({ id: 'j', profileId: 'FIN_3E_UAT' }, s).source === 'job'; })());
// A deleted profile must not blank the cell: the job still ran against
// something, and the stamped name is the last honest record of what.
check('a profile that has been deleted still shows the name, marked stale',
  (() => { const r = JP.of({ id: 'j', profileId: 'GONE', profileName: 'Old UAT' }, store());
    return r.name === 'Old UAT' && r.source === 'stale' && r.id === 'GONE'; })());
check('and a job carrying only a name — an import from elsewhere — still shows it',
  (() => { const r = JP.of({ id: 'j', profileName: 'Somebody else\'s' }, store());
    return r.name === "Somebody else's" && r.source === 'stale' && r.id === null; })());
check('label() is the one-line form the table uses',
  JP.label({ id: 'j', profileId: 'FIN_3E_UAT' }, store()) === 'Finance UAT'
  && JP.label({ id: 'j' }, store()) === '');

/* ── 4. One reader, not a second engine ─────────────────────────────────── */
//
// This file reads the profile store rather than importing the governance
// engine, because eight of the ten pages that create jobs do not load it. A
// second reader is exactly the thing that drifts, so the binding lookup is
// checked against the engine's own rather than assumed to match.
{
  const s = CP.cpNewStore(1000);
  // Both connections have to be classified into the profile's own environment
  // or cpActivateProfile refuses — a profile must not span environments.
  const CONNS = [{ id: 'a', side: 'src' }, { id: 'b', side: 'tgt' }];
  CP.cpSetConnMeta(s, 'a', { envClass: 'DEV' }, 'u', 1000);
  CP.cpSetConnMeta(s, 'b', { envClass: 'DEV' }, 'u', 1000);
  CP.cpSaveProfile(s, { id: 'P1', envClass: 'DEV', srcConnId: 'a', tgtConnId: 'b' }, CONNS, 'u', 1000);
  CP.cpActivateProfile(s, 'P1', CONNS, 'u', 1000);
  CP.cpBind(s, 'job', 'job_x', 'P1', 'u', 1000);
  CP.cpBind(s, 'report', 'job_x', 'P1', 'u', 1000);
  const mine = JP.bindingOf(s, 'job_x');
  const theirs = CP.cpBindingOf(s, 'job', 'job_x');
  check('the binding lookup agrees with cpBindingOf on the same store, object for object',
    mine === theirs, JSON.stringify({ mine, theirs }));
  check('and on a job that has none', JP.bindingOf(s, 'nope') === CP.cpBindingOf(s, 'job', 'nope'));
  CP.cpSelectProfile(s, 'P1', 'u', 1000);
  check('and it reads the same selected profile the engine does',
    JP.activeProfile(s).id === CP.cpBannerInfo(s).profileId);
}

/* ── 5. Wiring — nineteen creation points ───────────────────────────────── */
//
// A helper nobody calls is not a feature. These are every place a job id is
// minted, from the audit in the report that preceded this work.

const CREATORS = [
  ['dashboard-app.js', 1],          // + New Migration
  ['object-mapping-app.js', 2],     // simple map, one-to-many
  ['mapper.html', 2],
  ['one-to-many.html', 1],
  ['sql-editor-app.js', 1],
  ['project-builder-app.js', 2],    // file import, composite task
  ['connect.html', 2],
  ['insights.html', 2],
  ['data-enrichment.html', 2],      // apply + rollback, one call site
  ['schema_explorer.html', 1],
  ['cygenix-analyser-handoff.js', 1],
  ['cygenix-integrations.js', 2],   // import into active project, import as new
];
const missing = [], counts = [];
for (const [file, want] of CREATORS) {
  const src = read('public', file);
  const n = (src.match(/CygenixJobProfile\.(attach|stamp)\(/g) || []).length;
  counts.push(file + ':' + n);
  if (n < want) missing.push(file + ' has ' + n + ', expected ' + want);
}
check('every browser creation point stamps the job (' + CREATORS.length + ' files)',
  missing.length === 0, missing.join(' | '));
check('and that is nineteen calls in total, not a subset that happens to pass',
  counts.reduce((t, c) => t + Number(c.split(':')[1]), 0) === 19, counts.join(' '));
// Every call is guarded on the global: these pages load the module with defer
// and the creation code is inline, so a page that somehow renders without it
// must still create the job.
const unguarded = [];
for (const [file] of CREATORS) {
  const src = read('public', file);
  const calls = (src.match(/CygenixJobProfile\.(attach|stamp)\(/g) || []).length;
  const guards = (src.match(/window\.CygenixJobProfile/g) || []).length;
  if (calls && !guards) unguarded.push(file);
}
check('every call is guarded, so a missing module costs a stamp and not a job',
  unguarded.length === 0, unguarded.join(', '));
const noScript = CREATORS.map(([f]) => f).filter(f => f.endsWith('.html'))
  .concat(['dashboard.html', 'object_mapping.html', 'sql-editor.html', 'project-builder.html', 'assurance.html'])
  .filter(f => !/<script src="\/cygenix-job-profile\.js\?v=[a-f0-9]{10}" defer><\/script>/.test(read('public', f)));
check('and every page that creates or displays one loads the module',
  noScript.length === 0, noScript.join(', '));

/* ── 6. The column ──────────────────────────────────────────────────────── */

const DASH = read('public', 'dashboard-app.js');
check('the jobs table has a Profile column, immediately after Job Name',
  /<th>Job Name<\/th><th>Profile<\/th><th>Source<\/th>/.test(DASH));
check('and a cell in every row, in the same position',
  /\$\{jobProfilePill\(j\)\}<\/td>\s*\n\s*<td><span class="job-source"/.test(DASH));
check('an old job renders a dash rather than an empty cell',
  /if \(!p\) return '<span style="color:var\(--text3\)[^']*">—<\/span>'/.test(DASH));
// Once per render, not once per row: this redraws on every filter change and
// every drag, and a hundred rows would be a hundred JSON.parses otherwise.
check('the store is read once per render, not once per row',
  /const _profStore = \(window\.CygenixJobProfile && CygenixJobProfile\.load\(\)\) \|\| null;/.test(DASH)
  && /CygenixJobProfile\.of\(j, _profStore\)/.test(DASH));
check('a stale profile is visually distinct rather than silently identical',
  /stale \? 'background:var\(--bg3\)/.test(DASH));
check('the cell is escaped — a profile name is user-typed text',
  /escapeHtml\(p\.name\)/.test(DASH) && /escapeAttr\(title\)/.test(DASH));
// The column sits between existing ones, so the things that count or address
// columns have to still work.
check('nothing in the jobs table addresses columns by index, so the insert is safe',
  !/cells\[\d+\]/.test(DASH) && !/jobs-table[\s\S]{0,400}colspan/.test(DASH));

/* ── 7. The server half ─────────────────────────────────────────────────── */
//
// The Agentive Migration flow writes jobs from Azure, where there is no
// browser and no localStorage. It reads the profile from the same document it
// is already holding — the field the browser syncs to.

const AGENT = read('azure-function', 'src', 'agent.js');
check('the server reads the selected profile from the document it already loaded',
  /const store = existing\.connection_profiles;/.test(AGENT)
  && /store\.settings \? store\.settings\.activeProfileId : null/.test(AGENT));
check('and applies the same active-only rule as the browser',
  /p\.status === 'active'/.test(AGENT));
// The bug this is written against: a wholesale upsert of a field somebody
// else owns. connection_profiles is the one MERGED field in the sync
// endpoint, and writing it from here would undo another machine's profile.
check('it only READS the profile store — never writes it back',
  !/connection_profiles\s*=/.test(AGENT)
  && !/existing\.connection_profiles\s*=/.test(AGENT));
check('the two fields are ADDED to the new job, not merged over the jobs array',
  /if \(activeProfile\) \{\s*\n\s*job\.profileId = activeProfile\.id;/.test(AGENT));
check('and a run with no active profile leaves the fields off rather than blank',
  /jobs saved without a profile|saved without one/.test(AGENT));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
