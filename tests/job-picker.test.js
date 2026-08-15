// Tests the Task Agent's job picker.
//
// It rendered every entry in cygenix_jobs as its bare name, which produced a
// dropdown of a dozen entries all reading "Demo" — the Connect flow names a
// saved job after the PROJECT, so every conversion run under one project adds
// another job with the same name. They are distinct jobs, not duplicates, but
// nothing on screen said which was which. Trashed jobs were listed too, and a
// job with no name rendered the string "undefined".
//
// The module is extracted from public/dashboard.html and run against stubs, so
// what is under test is the shipped code.
const fs = require('fs');
const vm = require('vm');

const html = (fs.readFileSync(__dirname + '/../public/dashboard.html', 'utf8') + '\n' + fs.readFileSync(__dirname + '/../public/dashboard-app.js', 'utf8'));
const start = html.indexOf('// ══════════════════════════════════════════════════════════════════════════\n// TASK AGENT — job picker');
const end   = html.indexOf('// ══════════════════════════════════════════════════════════════════════════\n// NOTIFICATIONS VIEW');
if (start < 0 || end < 0 || end < start) {
  console.log('FAIL: could not locate the job-picker module in dashboard.html');
  process.exit(1);
}

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

function build(jobs, { projects = [], activeId = '' } = {}) {
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g,
    c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const sb = {
    console, JSON, String, Number, Date, Map, Set, Array, isNaN,
    escapeAttr: esc, escapeHtml: esc,
    liveJobs: () => jobs.filter(j => j && !j._deleted),
    safeArr: () => projects,
    localStorage: { getItem: k => (k === 'cygenix_active_project_id' ? activeId : null) },
  };
  vm.createContext(sb);
  vm.runInContext(html.slice(start, end), sb);
  return { sb, get: e => vm.runInContext(e, sb) };
}

// Parse the rendered <option> list back into labels, in order.
const labels = h => [...h.matchAll(/<option value="[^"]*">([\s\S]*?)<\/option>/g)].map(m => m[1]);
const groups = h => [...h.matchAll(/<optgroup label="([^"]*)"/g)].map(m => m[1]);

const job = (o) => ({ id: o.id, name: o.name, created: o.created || '2026-08-01T00:00:00Z', ...o });

console.log('Task Agent — job picker\n');

// ── The reported bug ─────────────────────────────────────────────────────
// Several jobs genuinely share the name "Demo". They must be told apart.
const DEMOS = [
  job({ id:'j1', name:'Demo', source:'dbo.Cases',     target:'dbo.cases',     created:'2026-08-15T13:28:00Z' }),
  job({ id:'j2', name:'Demo', source:'dbo.addresses', target:'dbo.addresses', created:'2026-08-14T09:00:00Z' }),
  job({ id:'j3', name:'Cases → cases with param', source:'dbo.Cases', target:'dbo.cases', created:'2026-08-13T09:00:00Z' }),
];
let t = build(DEMOS);
let L = labels(t.get('ta_jobOptions()'));
check('every job is still listed', L.length === 3, L.join(' | '));
check('duplicated names are qualified so they can be told apart',
  L[0] !== L[1], L.join(' | '));
check('the qualifier names the tables',
  /dbo\.Cases → dbo\.cases/.test(L[0]) && /dbo\.addresses/.test(L[1]), L.join(' | '));
check('the qualifier includes the date',
  /15 Aug/.test(L[0]) && /14 Aug/.test(L[1]), L.join(' | '));
check('a name that is already unique is left clean',
  L[2] === 'Cases → cases with param', L[2]);

// ── Trashed jobs ─────────────────────────────────────────────────────────
t = build([
  job({ id:'live', name:'Nightly' }),
  job({ id:'gone', name:'Deleted job', _deleted:true }),
]);
L = labels(t.get('ta_jobOptions()'));
check('a trashed job is not offered for scheduling',
  L.length === 1 && L[0] === 'Nightly', L.join(' | '));

// ── Missing names ────────────────────────────────────────────────────────
t = build([job({ id:'x', name:undefined, sourceTable:'dbo.Src', targetTable:'dbo.Tgt' })]);
check('a job with no name shows its tables, not "undefined"',
  labels(t.get('ta_jobOptions()'))[0] === 'dbo.Src → dbo.Tgt',
  labels(t.get('ta_jobOptions()'))[0]);

t = build([job({ id:'x' })]);
check('a job with nothing to identify it is labelled, not blank',
  labels(t.get('ta_jobOptions()'))[0] === 'Untitled job');

t = build([job({ id:'x', name:'   ' })]);
check('a whitespace-only name is treated as absent',
  labels(t.get('ta_jobOptions()'))[0] === 'Untitled job');

// ── Ordering ─────────────────────────────────────────────────────────────
t = build([
  job({ id:'old', name:'Older', created:'2026-01-01T00:00:00Z' }),
  job({ id:'new', name:'Newer', created:'2026-08-15T00:00:00Z' }),
]);
check('the newest job is offered first',
  labels(t.get('ta_jobOptions()'))[0] === 'Newer');

// A job with no created date must not throw or jump the queue.
t = build([job({ id:'a', name:'Dated', created:'2026-08-15T00:00:00Z' }), { id:'b', name:'Undated' }]);
L = labels(t.get('ta_jobOptions()'));
check('a job with no date still appears', L.length === 2 && L.includes('Undated'), L.join(' | '));

// An unparseable date must not produce "Invalid Date" in the label.
t = build([
  job({ id:'a', name:'Same', created:'not-a-date' }),
  job({ id:'b', name:'Same', created:'2026-08-15T00:00:00Z' }),
]);
L = labels(t.get('ta_jobOptions()'));
check('an unparseable date is omitted rather than shown as Invalid Date',
  !L.some(l => /Invalid/.test(l)), L.join(' | '));

// ── Project grouping ─────────────────────────────────────────────────────
// Nothing is hidden — scheduling another project's job is legitimate — but
// the active project's jobs come first, under a heading.
const MIXED = [
  job({ id:'a', name:'Mine',       projectId:'p1' }),
  job({ id:'b', name:'Theirs',     projectId:'p2' }),
  job({ id:'c', name:'Unassigned' }),
];
t = build(MIXED, { projects:[{ id:'p1', name:'Demo' }, { id:'p2', name:'Other' }], activeId:'p1' });
let h = t.get('ta_jobOptions()');
check('the active project heads its own group', groups(h)[0] === 'Demo', groups(h).join(' | '));
check('other projects are grouped separately, not hidden',
  groups(h)[1] === 'Other projects' && labels(h).includes('Theirs'), groups(h).join(' | '));
check('an unassigned job sits with the active project',
  h.indexOf('Unassigned') < h.indexOf('Other projects'), groups(h).join(' | '));
check('every job is still reachable', labels(h).length === 3, labels(h).join(' | '));

// With no other projects there is nothing to separate, so no headings.
t = build([job({ id:'a', name:'Mine', projectId:'p1' })],
          { projects:[{ id:'p1', name:'Demo' }], activeId:'p1' });
h = t.get('ta_jobOptions()');
check('no grouping when every job belongs to the active project', groups(h).length === 0);

// With no active project, everything is one flat list.
t = build(MIXED, { projects:[{ id:'p1', name:'Demo' }] });
h = t.get('ta_jobOptions()');
check('no active project means one flat list', groups(h).length === 0 && labels(h).length === 3);

// A project whose name is missing must not render an empty heading.
t = build(MIXED, { projects:[{ id:'p1' }, { id:'p2', name:'Other' }], activeId:'p1' });
check('a nameless active project falls back to a readable heading',
  groups(t.get('ta_jobOptions()'))[0] === 'This project',
  groups(t.get('ta_jobOptions()'))[0]);

// ── Empty ────────────────────────────────────────────────────────────────
t = build([]);
check('an empty library says so', /No jobs saved on this browser/.test(t.get('ta_jobOptions()')));
t = build([job({ id:'x', name:'Gone', _deleted:true })]);
check('a library of nothing but trash also says so',
  /No jobs saved on this browser/.test(t.get('ta_jobOptions()')));

// ── Escaping ─────────────────────────────────────────────────────────────
t = build([job({ id:'x', name:'<img src=x onerror=alert(1)>' })]);
h = t.get('ta_jobOptions()');
check('a job name containing markup is escaped', !/<img/.test(h), h);

t = build(MIXED, { projects:[{ id:'p1', name:'"><script>' }, { id:'p2', name:'O' }], activeId:'p1' });
check('a project name containing markup is escaped in the heading',
  !/<script>/.test(t.get('ta_jobOptions()')));

// ── The root cause is fixed at source ────────────────────────────────────
// Existing jobs keep the names they were saved with, but new ones from the
// Connect flow must no longer be named after the project alone.
{
  const connect = fs.readFileSync(__dirname + '/../public/connect.html', 'utf8');
  check('the Connect flow no longer names a job after the project alone',
    !/name:\s+projectName \|\| safeTgt \+ ' migration'/.test(connect));
  check('it now qualifies the name with the target table',
    /name:\s+\[projectName, safeTgt\]\.filter\(Boolean\)\.join/.test(connect));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
