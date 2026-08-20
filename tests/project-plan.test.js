// Tests for the Project Plan grid — cygenix-project-plan.js (the pure
// engine: week math, document shape, normalization, painting, CSV) plus
// structural pins on project_plan.html.
'use strict';

const fs = require('fs');
const path = require('path');
const PP = require('../public/cygenix-project-plan.js');

let pass = 0, fail = 0;
const check = (name, ok, detail) => {
  console.log((ok ? '  PASS  ' : '  FAIL  ') + name + (ok || !detail ? '' : '  [' + detail + ']'));
  ok ? pass++ : fail++;
};

// ── Timeline math ───────────────────────────────────────────────────────────
{
  const m = PP.ppMonths('2026-01', 8);
  check('eight months from January read January through August',
    m.length === 8 && m[0].label === 'January' && m[7].label === 'August'
    && m[0].ym === '2026-01' && m[7].ym === '2026-08');
  const roll = PP.ppMonths('2026-10', 5);
  check('the year rolls over and only the new year carries its number',
    roll[2].ym === '2026-12' && roll[3].ym === '2027-01'
    && roll[2].label === 'December' && roll[3].label === 'January 2027');
  check('every month exposes the same fixed week slots — a planning grid, not a calendar',
    PP.PP_WEEKS === 4);
  check('a garbage start or length never breaks the grid',
    PP.ppMonths('nope', 999).length === PP.PP_MAX_MONTHS
    && PP.ppMonths(null, 0)[0].label === 'January');
}

// ── Document shape and normalization ────────────────────────────────────────
{
  const doc = PP.ppNewDoc('Test plan');
  check('a new plan is immediately renderable — one phase, one task, empty grid',
    doc.v === PP.PP_VERSION && doc.phases.length === 1 && doc.tasks.length === 1
    && doc.tasks[0].phaseId === doc.phases[0].id && Object.keys(doc.cells).length === 0);

  const norm = PP.ppNormalize(null);
  check('normalizing nothing yields a working default, never a throw',
    norm.phases.length === 1 && norm.timeline.months === 8);

  const messy = PP.ppNormalize({
    name: 'X', timeline: { start: '2026-03', months: 2 },
    phases: [{ id: 'p1', name: 'Kick-off', color: 99 }],
    tasks: [
      { id: 't1', phaseId: 'p1', title: 'ok' },
      { id: 't2', phaseId: 'GONE', title: 'orphan' },
    ],
    cells: {
      't1|2026-03|2': { t: 'work' },
      't1|2026-03|9': { t: 'work' },            // week out of range
      't1|2030-01|1': { t: 'work' },            // month outside the timeline
      'zz|2026-03|1': { t: 'work' },            // task does not exist
      't1|2026-04|1': { t: 'mile', label: 'Meeting 2' },
      'bad-key': { t: 'work' },
    },
  });
  check('a phase-less task is reassigned, never dropped silently',
    messy.tasks.length === 2 && messy.tasks[1].phaseId === 'p1');
  check('cells outside the timeline, the week range or the task list are dropped',
    Object.keys(messy.cells).sort().join(';') === 't1|2026-03|2;t1|2026-04|1');
  check('a wild color index wraps into the palette',
    messy.phases[0].color >= 0 && messy.phases[0].color < PP.PP_PALETTE.length);
  check('milestone labels survive normalization', messy.cells['t1|2026-04|1'].label === 'Meeting 2');
}

// ── Painting ────────────────────────────────────────────────────────────────
{
  const doc = PP.ppNewDoc();
  const t = doc.tasks[0].id;
  const k = PP.ppCellKey(t, '2026-02', 3);
  check('painting work fills the cell', PP.ppPaint(doc, k, 'work').t === 'work');
  check('painting the same state again erases it — a misclick undoes itself',
    PP.ppPaint(doc, k, 'work') === null && !doc.cells[k]);
  PP.ppPaint(doc, k, 'mile', 'Session 5');
  check('a milestone carries its label', doc.cells[k].label === 'Session 5');
  PP.ppPaint(doc, k, 'mile', 'Session 6');
  check('a different label replaces, the same label toggles off',
    doc.cells[k].label === 'Session 6'
    && PP.ppPaint(doc, k, 'mile', 'Session 6') === null && !doc.cells[k]);
  PP.ppPaint(doc, k, 'work');
  PP.ppPaint(doc, k, 'erase');
  check('erase erases whatever is there', !doc.cells[k]);
  check('milestones are always the one green family, whatever the phase tint',
    /^#/.test(PP.PP_MILESTONE.bg) && PP.PP_PALETTE.length >= 6);
}

// ── Rows and CSV ────────────────────────────────────────────────────────────
{
  const doc = PP.ppNewDoc();
  const p2 = { id: 'p2', name: 'Design', color: 1 };
  doc.phases.push(p2);
  doc.tasks.push({ id: 'ta', phaseId: 'p2', title: 'Contracts\n: Companies\n: Deals', resource: 'Anjay/Epix', comment: 'Film, Log' });
  doc.tasks.push({ id: 'tb', phaseId: 'p2', title: 'Content', resource: '', comment: '' });
  const rows = PP.ppRows(doc);
  check('rows come out in phase order with the rowspan on the first task',
    rows.length === 3 && rows[1].first === true && rows[1].span === 2 && rows[2].first === false);

  doc.cells[PP.ppCellKey('ta', '2026-02', 1)] = { t: 'mile', label: 'Meeting 2' };
  doc.cells[PP.ppCellKey('ta', '2026-01', 3)] = { t: 'work' };
  const csv = PP.ppCsv(doc);
  const lines = csv.trim().split('\n');
  check('the CSV header names every month-week column',
    lines[0].startsWith('phase,task,resource,comment,January W1')
    && lines[0].includes('August W4'));
  check('milestones export by name, work weeks as a mark',
    /Meeting 2/.test(csv) && /(^|,)#(,|$)/m.test(csv));
  check('multiline titles and commas are quoted correctly',
    /"Contracts\n: Companies\n: Deals"/.test(csv) && /"Film, Log"/.test(csv));
  const s = PP.ppStats(doc);
  check('the stats count phases, tasks, work weeks and milestones',
    s.phases === 2 && s.tasks === 3 && s.work === 1 && s.miles === 1);
}

// ── Page pins ───────────────────────────────────────────────────────────────
{
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'project_plan.html'), 'utf8');
  check('the page ships the standard chrome — auth gate, sidebar, mobile layer',
    /auth-gate\.js/.test(html) && /cygenix-mobile\.css/.test(html)
    && /data-active="project-plan-grid"/.test(html) && /cygenix-sidebar\.js/.test(html));
  check('the engine module is loaded and the page store is versioned',
    /cygenix-project-plan\.js/.test(html) && /cygenix_project_plans_v1/.test(html));
  check('the three paint tools and the milestone label are on the toolbar',
    /id="pp-tool-work"/.test(html) && /id="pp-tool-mile"/.test(html)
    && /id="pp-tool-erase"/.test(html) && /id="pp-mile-label"/.test(html));
  check('user text is escaped everywhere it renders',
    /esc\(lines\[0\]/.test(html) && /esc\(c\.label\)/.test(html) && /esc\(r\.phase\.name\)/.test(html));
  check('drag extends and never toggles — sweeping back cannot erase the bar',
    /Drag never toggles: it extends/.test(html));
  check('printing hides the chrome and prints the grid as the report',
    /@media print/.test(html) && /#cyg-sidebar-mount,.*display:none/.test(html));
  check('the timeline is user-set: start month and month count',
    /id="pp-start" type="month"/.test(html) && /id="pp-months" type="number"/.test(html));
  check('the phase label stands vertical, exactly like the sheet',
    /writing-mode:vertical-rl/.test(html));

  const sidebar = fs.readFileSync(path.join(__dirname, '..', 'public', 'cygenix-sidebar.js'), 'utf8');
  check('Project Plan lives under Reports in the rail, on its own key',
    /key:'project-plan-grid',\s*label:'Project Plan',\s*href:'\/project_plan\.html'/.test(sidebar));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
