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
  check('red is the last tint — the emphasis colour for key deliverables',
    PP.PP_PALETTE[PP.PP_PALETTE.length - 1].name === 'red'
    && /^#[0-9A-F]{6}$/i.test(PP.PP_PALETTE[PP.PP_PALETTE.length - 1].bg)
    && PP.PP_PALETTE.filter(c => c.name === 'red').length === 1);
  check('auto-assignment stops before it — red is only ever chosen by hand',
    PP.PP_AUTO_TINTS === PP.PP_PALETTE.length - 1);
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

// ── Import from the Effort Estimator ────────────────────────────────────────
// One source of truth: the estimate's use cases, employee, dates and
// durations become a plan without maintaining the same data twice.
{
  const EM = require('../public/cygenix-effort-model.js');
  const est = EM.emNewDoc('Corus 3E');
  for (const m of ['Addresses', 'AP', 'AP Master', 'AR', 'Card Summary', 'Chart of Accounts (GL)'])
    est.ticks['analysis|' + m] = 1;
  est.ticks['design|Addresses'] = 1;
  est.ticks['scripts|Addresses'] = 1;
  est.meta.employee = 'Curtis';
  est.meta.startDate = '2026-08-21';
  est.meta.dueDate = '2027-06-03';
  const r = EM.emCompute(est);
  const plan = PP.ppFromEstimate(est, r);

  check('only COSTED use cases become tasks — untouched work is not planned',
    plan.tasks.length === 3
    && plan.tasks.map(t => t.title.split('\n')[0]).join(';')
      === 'Initial analysis, business review, documentation;Design and documentation;Script development');
  check('a seven-use-case import never lands on red by accident',
    (() => { const e2 = EM.emNewDoc('all');
      for (const uc of EM.EM_USE_CASES) e2.ticks[uc.id + '|AP'] = 1;
      const p7 = PP.ppFromEstimate(e2, EM.emCompute(e2));
      return p7.phases.every(ph => PP.PP_PALETTE[ph.color].name !== 'red'); })());
  check('each use case is a PHASE of its own, named after it, in its own tint',
    plan.phases.length === 3
    && plan.phases.map(p => p.name).join(';')
      === 'Initial analysis, business review, documentation;Design and documentation;Script development'
    && plan.phases.map(p => p.color).join(',') === '0,1,2'
    && plan.phases.every((p, i) => plan.tasks[i].phaseId === p.id));
  check('the employee becomes every task\'s resource and the FP rides in the comment',
    plan.tasks.every(t => t.resource === 'Curtis')
    && plan.tasks[0].comment === '13.5 FP' && plan.tasks[2].comment === '26.5 FP');
  // v2: the modules are DATA on the task, in full — no longer baked into the
  // title, where v1 kept twelve and wrote "+N more" for the rest.
  check('ticked modules become the task\'s objects, as data, and the title is the name alone',
    plan.tasks[0].objects.join(';') === 'Addresses;AP;AP Master;AR;Card Summary;Chart of Accounts (GL)'
    && plan.tasks[0].title === 'Initial analysis, business review, documentation'
    && plan.tasks[1].objects.join(';') === 'Addresses' && plan.v === 2);
  check('the timeline starts on the estimate\'s start month',
    plan.timeline.start === '2026-08');

  // Sequential bars at the model's own rates: 18.5wd→4 slots, 12.3→3, 36.3→8.
  const work = Object.entries(plan.cells).filter(([, v]) => v.t === 'work');
  const byTask = (id) => work.filter(([k]) => k.startsWith(id + '|')).length;
  check('work bars follow the model: 4 + 3 + 8 sequential week slots',
    byTask(plan.tasks[0].id) === 4 && byTask(plan.tasks[1].id) === 3
    && byTask(plan.tasks[2].id) === 8 && work.length === 15);
  check('bars are sequential, never overlapping — the estimate as a schedule',
    (() => {
      const slot = (k) => { const [, ym, w] = k.split('|');
        const m = PP.ppMonths(plan.timeline.start, plan.timeline.months).findIndex(x => x.ym === ym);
        return m * PP.PP_WEEKS + Number(w) - 1; };
      const s0 = work.filter(([k]) => k.startsWith(plan.tasks[0].id)).map(([k]) => slot(k));
      const s2 = work.filter(([k]) => k.startsWith(plan.tasks[2].id)).map(([k]) => slot(k));
      return Math.max(...s0) < Math.min(...s2);
    })());

  const miles = Object.values(plan.cells).filter(v => v.t === 'mile').map(v => v.label).sort();
  check('the projected delivery and the due date land as milestones',
    miles.join(',') === 'Due date,Est. delivery');
  check('the timeline stretches to show the due date',
    plan.timeline.months === 11
    && plan.cells[Object.keys(plan.cells).find(k => /2027-06/.test(k))].label === 'Due date');
  check('the import is a valid plan document — it survives its own normalization',
    JSON.stringify(PP.ppNormalize(plan)) === JSON.stringify(plan));
  check('the client rides in from the estimate, name and code together',
    (() => { const e2 = EM.emNewDoc('X'); e2.ticks['uat|AP'] = 1;
      e2.meta.clientName = 'Corus'; e2.meta.clientCode = 'MDG';
      return PP.ppFromEstimate(e2, EM.emCompute(e2)).client === 'Corus (MDG)'; })());
  check('the client survives normalization and defaults empty',
    PP.ppNormalize({ client: 'Corus' }).client === 'Corus' && PP.ppNewDoc().client === '');

  // The Excel workbook: grid, colours and rotated labels intact.
  {
    const xls = PP.ppExcelHtml(Object.assign(plan, { client: 'Corus (MDG)' }));
    check('the Excel export is a workbook with the plan and the client on top',
      /urn:schemas-microsoft-com:office:excel/.test(xls)
      && /<x:Name>Project Plan<\/x:Name>/.test(xls)
      && /Client<\/td><td colspan="3">Corus \(MDG\)/.test(xls));
    check('phases keep their tint and stand rotated; milestones stay green with their names',
      /mso-rotate:90/.test(xls) && xls.includes(PP.PP_PALETTE[0].bg)
      && xls.includes(PP.PP_MILESTONE.bg) && /Est\. delivery/.test(xls) && /Due date/.test(xls));
    check('work bars land as filled cells and each task lists its objects',
      (xls.match(new RegExp('background:' + PP.PP_PALETTE[2].bg, 'g')) || []).length >= 8
      && /<br>AP<br>/.test(xls));
    check('user text is escaped in the workbook',
      PP.ppExcelHtml(Object.assign(PP.ppNewDoc(), { client: '<img src=x>' }))
        .includes('&lt;img src=x&gt;'));
  }
  check('an estimate with nothing ticked imports as null, never an empty husk',
    PP.ppFromEstimate(EM.emNewDoc('empty'), EM.emCompute(EM.emNewDoc('empty'))) === null);
  check('date → slot math: whole months plus the day mapped onto the week slots',
    JSON.stringify(PP.ppSlotForDate('2026-08', '2026-11-25')) === '{"monthIndex":3,"week":4}'
    && PP.ppSlotForDate('2026-08', '2026-05-01') === null
    && PP.ppSlotForDate('2026-08', 'garbage') === null);
  // "this week" on the grid: the column a date occupies, or nothing at all
  check('today maps to its timeline column — the 24th of the start month is week 4',
    PP.ppNowIndex('2026-08', 4, '2026-08-24') === 3
    && PP.ppNowIndex('2026-08', 4, '2026-08-01') === 0
    && PP.ppNowIndex('2026-08', 4, '2026-09-02') === 4);
  check('a date outside the plan window highlights nothing rather than the nearest edge',
    PP.ppNowIndex('2026-08', 4, '2026-07-31') === null
    && PP.ppNowIndex('2026-08', 4, '2026-12-01') === null
    && PP.ppNowIndex('2026-08', 4, '2026-11-30') === 15
    && PP.ppNowIndex('2026-08', 4, 'garbage') === null);
}

// ── Portfolio: every plan on one shared timeline ────────────────────────────
{
  const EM = require('../public/cygenix-effort-model.js');
  const e1 = EM.emNewDoc('Corus 3E');
  Object.assign(e1.meta, { clientName: 'Corus', clientCode: 'MDG',
    employee: 'Curtis', startDate: '2026-08-21', dueDate: '2027-06-03' });
  e1.ticks['analysis|AP'] = 1; e1.ticks['analysis|AR'] = 1; e1.ticks['scripts|AP'] = 1;
  const p1 = PP.ppFromEstimate(e1, EM.emCompute(e1));

  const p2 = PP.ppNewDoc('Hand plan');
  p2.client = 'Kestrel';
  p2.timeline = { start: '2026-06', months: 6 };
  p2.tasks[0].resource = 'Curtis';
  p2.cells[PP.ppCellKey(p2.tasks[0].id, '2026-08', 1)] = { t: 'work' };
  p2.cells[PP.ppCellKey(p2.tasks[0].id, '2026-09', 2)] = { t: 'mile', label: 'Go live' };

  const m = PP.ppPortfolio({ a: p1, b: p2 });
  check('the global axis is the union of every plan\'s timeline',
    m.start === '2026-06' && m.months === 13 && m.weeks === 52 && m.projects.length === 2);
  check('each plan re-bases onto global weeks — August W1 lands at week 8 from a June start',
    m.projects[1].byWeek[8].length === 1
    && m.projects[0].byWeek.findIndex(a => a.length) === 8);
  check('milestones re-base too, labels intact',
    m.projects[1].milestones[0].w === 13 && m.projects[1].milestones[0].label === 'Go live'
    && m.projects[0].milestones.map(x => x.label).join(',') === 'Est. delivery,Due date');
  check('per-project rollups: client, worked weeks, FP parsed from the comments',
    m.projects[0].client === 'Corus (MDG)' && m.projects[0].fp === 32
    && m.projects[0].weeks > 0 && m.projects[1].fp === 0);
  check('phase summaries carry tint, resources and their global weeks',
    (() => { const ph = m.projects[0].phases.find(x => x.weeks.length && /Script/.test(x.name));
      return ph && ph.resources.join(',') === 'Curtis' && ph.weeks[0] >= 8; })());
  check('the resource ledger finds the SAME person on two projects in the same week',
    (() => { const c = m.resources.find(r => r.name === 'Curtis');
      return c && c.peak === 2 && c.load[8].length === 2; })());
  check('a plan with no month overlap never clashes',
    (() => { const q = PP.ppNormalize(JSON.parse(JSON.stringify(p2)));
      q.name = 'Late plan'; q.timeline.start = '2027-01'; q.cells = {};
      q.cells[PP.ppCellKey(q.tasks[0].id, '2027-01', 1)] = { t: 'work' };
      const mm = PP.ppPortfolio({ a: p1, b: q });
      const c = mm.resources.find(r => r.name === 'Curtis');
      return c.peak === 1; })());
  check('an empty store or plans without tasks yield null, never a husk',
    PP.ppPortfolio({}) === null && PP.ppPortfolio(null) === null);
  check('month arithmetic and FP parsing are plain and safe',
    PP.ppMonthDiff('2026-06', '2027-01') === 7 && PP.ppMonthDiff('x', 'y') === null
    && PP.ppFpOf('13.5 FP · TC 2') === 13.5 && PP.ppFpOf('no figure') === 0);
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
    /esc\(r\.task\.title \|\| '\(untitled\)'\)/.test(html) && /esc\(o\) \+ '<\/span><\/div>'/.test(html)
    && /esc\(c\.label\)/.test(html) && /esc\(r\.phase\.name\)/.test(html));
  check('drag extends and never toggles — sweeping back cannot erase the bar',
    /Drag never toggles: it extends/.test(html));
  check('printing hides the chrome and prints the grid as the report',
    /@media print/.test(html) && /#cyg-sidebar-mount,.*display:none/.test(html));
  check('the timeline is user-set: start month and month count',
    /id="pp-start" type="month"/.test(html) && /id="pp-months" type="number"/.test(html));
  check('the phase label stands vertical, exactly like the sheet',
    /writing-mode:vertical-rl/.test(html));
  check('the current week is marked on the grid, header and column alike',
    /ppNowIndex\(doc\.timeline\.start, months\.length\)/.test(html)
    && /isNow \? ' pp-now' : ''/.test(html)
    && /now \? 'pp-now' : ''/.test(html)
    && /pp-month-now/.test(html));
  check('the marker layers over work bars instead of replacing their tint',
    /\.pp td\.pp-now\{box-shadow:inset/.test(html)
    && !/\.pp td\.pp-now\{background/.test(html));
  check('"this week" is the reader\'s week: the local date, never the UTC one',
    /function ppTodayIso/.test(html) && /getFullYear\(\) \+ '-'/.test(html)
    && !/new Date\(\)\.toISOString\(\)\.slice\(0, 10\)/.test(html));
  check('a plan whose window has not started says so rather than marking nothing silently',
    /today is outside this plan/.test(html) && /pp-nowchip/.test(html));
  check('the grid scrolls to this week once, not on every repaint',
    /ppScrolledToNow/.test(html) && /ppScrolledToNow = true/.test(html));
  check('the toolbar offers the From-estimator import, one-way and explicit about it',
    /⇪ From Configurator/.test(html) && /ppImportEstimate/.test(html)
    && /cygenix-effort-model\.js/.test(html) && /ONE-WAY copy/.test(html)
    && /cygenix_effort_estimates_v1/.test(html));
  check('the client from the estimate shows in the header area, click-to-edit',
    /id="pp-client"/.test(html) && /ppEditClient/.test(html));
  check('a plan without its own client falls back LIVE to the estimator\'s',
    /function ppEstimatorClient/.test(html)
    && /own \|\| ppEstimatorClient\(\)/.test(html)
    && /Carried from the Configurator/.test(html));
  check('the tint picker names red as the key-deliverable colour and rotates past it',
    /key deliverables \(Go Live, cutover\)/.test(html)
    && /Red is the emphasis tint/.test(html)
    && /doc\.phases\.length % PP\.PP_AUTO_TINTS/.test(html));
  check('the grid exports to Excel as well as CSV',
    /⤓ Excel/.test(html) && /ppExportExcel/.test(html)
    && /application\/vnd\.ms-excel/.test(html) && /cygenix-project-plan\.xls/.test(html));
  check('the Portfolio button opens the multi-project view in place',
    /id="pf-open-btn"/.test(html) && /▤ Portfolio/.test(html)
    && /id="pf-view"/.test(html) && /Back to plan/.test(html)
    && /ppPortfolio\(PPState\.store\.plans, \{ todayIso: ppTodayIso\(\) \}\)/.test(html));
  check('portfolio modes, zoom, client filter and expand/collapse are wired',
    /pfSetMode\('resource'\)/.test(html) && /pfSetZoom\('wk'\)/.test(html)
    && /id="pf-client"/.test(html) && /pfExpandAll/.test(html));
  check('overlapping phases split the tint and double-bookings hatch red',
    /phases overlap/.test(html) && /pf-c\.clash/.test(html)
    && /booked on ' \+ a\.length \+ ' projects this week/.test(html));
  check('the today line and milestone flags ride on every track',
    /pf-today/.test(html) && /pf-flag/.test(html) && /pfTodayWeek/.test(html));

  // ── Actuals on the page ──
  check('v1 plans migrate on load with the Configurator as the recovery source, and are saved once',
    /function ppRecoverSource/.test(html) && /PP\.ppIsLegacy\(s\.plans\[k\]\)/.test(html)
    && /PP\.ppNormalize\(s\.plans\[k\], \{ recover \}\)/.test(html) && /if \(migrated\) ppSave\(\);/.test(html));
  check('lateness uses the page\'s own "now", unclamped — ppWeekIndex, not a second calculation',
    /function ppNowWeek\(doc\)\{\s*return window\.CygenixProjectPlan\.ppWeekIndex\(doc\.timeline\.start, ppTodayIso\(\)\);/.test(html));
  check('no red until the first status is set in the plan', /const lateNow = tracking \? nowWeek : null;/.test(html));
  check('"+N more" is display only: the stored list is shown in full on request',
    /const shown = open \? objs : objs\.slice\(0, PP_OBJ_SHOWN\)/.test(html) && /data-more=/.test(html));
  check('the popover only READS the plan when it draws — a save cannot set off another save',
    /function ppPopRender\(\)\{[\s\S]{0,2400}?\n\}/.test(html)
    && !/ppSave\(/.test((/function ppPopRender\(\)\{([\s\S]*?)\n\}/.exec(html) || [])[1] || 'ppSave('));
  check('the popover closes on Esc and on a click outside it',
    /e\.key === 'Escape' && !pop\.hidden/.test(html) && /pop\.contains\(e\.target\)/.test(html));
  check('print keeps the dots, the darker bars and the hatching, with a legend at the foot',
    /print-color-adjust:exact/.test(html) && /id="pp-legend"/.test(html) && /\.pp-legend:not\(\[hidden\]\)\{display:flex\}/.test(html));
  check('exports carry today so the late column can be worked out',
    /PP\.ppCsv\(doc, \{ todayIso: ppTodayIso\(\) \}\)/.test(html) && /PP\.ppExcelHtml\(ppDoc\(\), \{ todayIso: ppTodayIso\(\) \}\)/.test(html));

  const sidebar = fs.readFileSync(path.join(__dirname, '..', 'public', 'cygenix-sidebar.js'), 'utf8');
  // The Configurator and the Project plan used to sit in a Project expander
  // under Home. The console redesign (Sep-2026) dissolved the expanders and
  // filed both as tabs on Reports, where nobody found them: a plan is not a
  // report. They are the PLAN group now, directly below Home — Configurator
  // first, then the plan — and their keys are unchanged so this page's
  // data-active still resolves.
  check('the Configurator and the Project plan are the Plan group at the top of the rail, Configurator first',
    (() => {
      const grp = /section: 'Plan', group:'plan', items: \[[\s\S]*?\]\}/.exec(sidebar);
      if (!grp) return false;
      const est = grp[0].indexOf("key:'effort-estimator'");
      const pln = grp[0].indexOf("key:'project-plan-grid'");
      return est > -1 && pln > -1 && est < pln && !/key:'project-group'/.test(sidebar);
    })());
  check('and neither module is listed under Reports any more',
    !/reports-group[\s\S]{0,900}key:'project-plan-grid'/.test(sidebar)
    && !/reports-group[\s\S]{0,900}key:'effort-estimator'/.test(sidebar));
}

// ── Actuals v2: migration ───────────────────────────────────────────────────
// Found in live data: the objects under each task were text in the title, and
// the import kept twelve and wrote "+N more" — eleven of "Initial analysis"'s
// objects existed nowhere. Migration splits v1 once, recovers the lost names
// from the Configurator when it can prove they are the right ones, and flags
// the task when it cannot.
{
  const EM = require('../public/cygenix-effort-model.js');
  const mods = Array.from({ length: 23 }, (_, i) => 'Mod ' + String(i + 1).padStart(2, '0'));
  const est = EM.emNewDoc('New estimate');
  for (const m of mods) est.ticks['analysis|' + m] = 1;
  const r = EM.emCompute(est);
  const ucName = r.perUseCase.find(u => u.id === 'analysis').name;
  // Exactly what v1 stored.
  const v1 = { v: 1, name: 'Plan — New estimate', timeline: { start: '2026-01', months: 6 },
    phases: [{ id: 'p1', name: ucName, color: 0 }, { id: 'p2', name: 'Manual', color: 1 }],
    tasks: [
      { id: 't1', phaseId: 'p1', title: [ucName].concat(mods.slice(0, 12).map(m => ': ' + m), [': +11 more']).join('\n'), resource: 'Curtis', comment: '13 FP' },
      { id: 't2', phaseId: 'p2', title: 'Workshop\nBring the org chart\n: Contracts', resource: '', comment: '' },
      { id: 't3', phaseId: 'p2', title: 'Kick-off', resource: '', comment: '' },
    ],
    cells: { 't1|2026-01|1': { t: 'work' } } };

  check('a v1 plan is recognised as needing migration, a v2 one is not',
    PP.ppIsLegacy(v1) && !PP.ppIsLegacy(PP.ppNewDoc()));
  const split = PP.ppSplitTitle(v1.tasks[0].title);
  check('the split: first line is the title, ": " lines objects, "+N more" the count lost',
    split.title === ucName && split.objects.length === 12 && split.more === 11);

  const recover = PP.ppRecoverFromEstimates({ 'New estimate': est }, (e) => EM.emCompute(EM.emNormalize(e)));
  const m1 = PP.ppNormalize(JSON.parse(JSON.stringify(v1)), { recover });
  check('THE ELEVEN LOST OBJECTS ARE RECOVERED FROM THE CONFIGURATOR',
    m1.tasks[0].objects.length === 23 && m1.tasks[0].objects.join() === mods.join() && !m1.tasks[0].objectsIncomplete,
    m1.tasks[0].objects.length);
  check('…and the title is the name alone', m1.tasks[0].title === ucName);
  check('a line that is not ": X" is kept as detail, not lost and not an object',
    m1.tasks[1].title === 'Workshop' && m1.tasks[1].detail === 'Bring the org chart' && m1.tasks[1].objects.join() === 'Contracts');
  check('a task with no objects has none', m1.tasks[2].objects.length === 0);
  check('the migrated plan is v2 with an empty actuals map', m1.v === 2 && JSON.stringify(m1.actuals) === '{}');
  check('MIGRATION RUNS ONCE: normalising the result again changes nothing',
    JSON.stringify(PP.ppNormalize(JSON.parse(JSON.stringify(m1)), { recover })) === JSON.stringify(m1));

  const noRecover = PP.ppNormalize(JSON.parse(JSON.stringify(v1)));
  check('with no Configurator to recover from, the 12 names are kept and the task is FLAGGED',
    noRecover.tasks[0].objects.length === 12 && noRecover.tasks[0].objectsIncomplete === true);
  const edited = EM.emNewDoc('New estimate');
  for (const m of mods.slice(0, 20)) edited.ticks['analysis|' + m] = 1;
  const wrong = PP.ppNormalize(JSON.parse(JSON.stringify(v1)),
    { recover: PP.ppRecoverFromEstimates({ x: edited }, (e) => EM.emCompute(EM.emNormalize(e))) });
  check('an estimate edited since the import does NOT supply objects — the count must match exactly',
    wrong.tasks[0].objects.length === 12 && wrong.tasks[0].objectsIncomplete === true);
  check('recovery refuses a list whose first names differ',
    !PP.ppAcceptRecovered(['A', 'B'], 1, ['A', 'X', 'C']) && PP.ppAcceptRecovered(['A', 'B'], 1, ['A', 'B', 'C']));
  const cut = PP.ppNormalize({ v: 1, tasks: [{ id: 'c', title: 'T\n' + ': x'.repeat(200) }] });
  check('a v1 title cut at the old 600-character cap is flagged too', cut.tasks[0].objectsIncomplete === true);
  check('a flag survives later loads', PP.ppNormalize(noRecover).tasks[0].objectsIncomplete === true);
  const dup = PP.ppNormalize({ v: 2, tasks: [{ id: 'd', title: 'T', objects: ['AP', 'AP', ' ', '_task', 'AR'] }] });
  check('objects are cleaned: no blanks, no duplicates, never the reserved "_task"', dup.tasks[0].objects.join() === 'AP,AR');
}

// ── Actuals v2: status and lateness ─────────────────────────────────────────
{
  const doc = PP.ppNormalize({ v: 2, name: 'L', timeline: { start: '2026-01', months: 6 },
    phases: [{ id: 'p', name: 'Analysis', color: 0 }],
    tasks: [{ id: 't', phaseId: 'p', title: 'Analysis', objects: ['AP', 'AR', 'GL'] },
            { id: 'u', phaseId: 'p', title: 'Unplanned', objects: ['X'] },
            { id: 'n', phaseId: 'p', title: 'No objects', objects: [] }],
    // planned: Feb W1–W4 → columns 4..7
    cells: { 't|2026-02|1': { t: 'work' }, 't|2026-02|2': { t: 'work' }, 't|2026-02|3': { t: 'work' }, 't|2026-02|4': { t: 'work' },
             'n|2026-01|1': { t: 'work' } } });
  const T = doc.tasks[0];
  check('planned weeks are the task\'s work cells as columns', PP.ppTaskWeeks(doc, 't').join() === '4,5,6,7');
  check('the week index is ppNowIndex without its window clamp',
    PP.ppWeekIndex('2026-01', '2026-02-10') === 5 && PP.ppWeekIndex('2026-01', '2027-01-01') === 48
    && PP.ppNowIndex('2026-01', 6, '2027-01-01') === null && PP.ppWeekIndex('2026-01', '2025-12-31') < 0);

  check('before the first planned week, not started is simply not started',
    PP.ppObjectState(doc, T, 'AP', 3).late === false && PP.ppObjectState(doc, T, 'AP', 3).status === 'not_started');
  check('in the first planned week it is not yet late', PP.ppObjectState(doc, T, 'AP', 4).late === false);
  const ls = PP.ppObjectState(doc, T, 'AP', 5);
  check('LATE START: not started, first planned week behind us', ls.late && ls.lateKind === 'late_start');
  const lt = PP.ppObjectState(doc, T, 'AP', 8);
  check('LATE: not done, last planned week behind us', lt.late && lt.lateKind === 'late');
  check('a plan whose window has ended can still be late', PP.ppObjectState(doc, T, 'AP', 60).lateKind === 'late');
  check('a task with no planned weeks gets no late logic', PP.ppObjectState(doc, doc.tasks[1], 'X', 60).late === false);

  PP.ppSetStatus(doc, 't', 'AP', 'active', '2026-02-02');
  check('Active stamps the start date, and says manual',
    doc.actuals.t.AP.status === 'active' && doc.actuals.t.AP.startedAt === '2026-02-02' && doc.actuals.t.AP.doneAt === '' && doc.actuals.t.AP.source === 'manual');
  check('an active object past its planned end is late', PP.ppObjectState(doc, T, 'AP', 8).lateKind === 'late');
  check('an active object is never a late START', PP.ppObjectState(doc, T, 'AP', 5).late === false);
  PP.ppSetStatus(doc, 't', 'AP', 'done', '2026-02-20');
  check('Done stamps the finish and keeps the start',
    doc.actuals.t.AP.doneAt === '2026-02-20' && doc.actuals.t.AP.startedAt === '2026-02-02');
  check('done on time is not late', PP.ppObjectState(doc, T, 'AP', 20).late === false);
  PP.ppSetStatus(doc, 't', 'AR', 'done', '2026-03-10');
  const dl = PP.ppObjectState(doc, T, 'AR', 20);
  check('DONE LATE: done after the last planned week — green, with the flag', dl.status === 'done' && dl.late && dl.lateKind === 'done_late');
  check('Done with no start stamps both', doc.actuals.t.AR.startedAt === '2026-03-10');
  PP.ppSetDates(doc, 't', 'AR', { doneAt: '2026-02-25' });
  check('editing the done date back inside the plan clears done-late', PP.ppObjectState(doc, T, 'AR', 20).late === false);
  check('…and pulls the stamped start back with it: work cannot start after it finished',
    doc.actuals.t.AR.startedAt === '2026-02-25');
  PP.ppSetDates(doc, 't', 'AR', { doneAt: 'garbage' });
  check('a malformed date is ignored, not stored', doc.actuals.t.AR.doneAt === '2026-02-25');
  PP.ppSetStatus(doc, 't', 'AR', 'active', '2026-03-01');
  check('moving Done back to Active clears the finish and keeps the start', doc.actuals.t.AR.doneAt === '' && doc.actuals.t.AR.startedAt === '2026-02-25');
  PP.ppSetStatus(doc, 't', 'AR', 'not_started', '2026-03-01');
  check('Not started removes the record', !('AR' in doc.actuals.t));
  PP.ppSetStatus(doc, 'n', '', 'done', '2026-01-05');
  check('a task with no objects keeps its status under "_task"', doc.actuals.n._task.status === 'done');

  const ts = PP.ppTaskSummary(doc, T, 8);
  check('the task roll-up: 1/3 done, 2 late', ts.total === 3 && ts.done === 1 && ts.late === 2 && ts.any && !ts.complete);
  const ps = PP.ppPlanSummary(doc, 8);
  check('the plan summary: done %, late count, tracking on',
    ps.tracking && ps.total === 5 && ps.done === 2 && ps.pct === 40 && ps.late === 2, JSON.stringify(ps));
  check('tracking is off until the first status is set', !PP.ppTracking(PP.ppNewDoc()));

  const span = PP.ppTaskActualSpan(doc, T, 9);
  check('the actual span runs from the first start to now while the task is open', span.from === 4 && span.to === 9, JSON.stringify(span));
  check('a task with nothing started has no actual span', PP.ppTaskActualSpan(doc, doc.tasks[1], 9) === null);

  const re = PP.ppNormalize(JSON.parse(JSON.stringify(doc)));
  check('actuals survive normalisation — including the timeline re-crop', JSON.stringify(re.actuals) === JSON.stringify(doc.actuals));
  const junk = PP.ppNormalize({ v: 2, tasks: [{ id: 'a', title: 'A' }],
    actuals: { a: { X: { status: 'weird' }, Y: { status: 'done', doneAt: 'no', startedAt: '2026-01-01', source: 'hack' } }, gone: { Z: { status: 'done' } } } });
  check('normalise drops bad statuses, bad dates, unknown sources and deleted tasks',
    JSON.stringify(junk.actuals) === JSON.stringify({ a: { Y: { status: 'done', startedAt: '2026-01-01', doneAt: '', source: 'manual' } } }));
  check('the stronger shade keeps the hue: it moves the tint toward its own ink',
    PP.ppShade('#F8E0C8', '#7A4A12', 0.3) !== '#F8E0C8' && /^#[0-9a-f]{6}$/.test(PP.ppShade('#F8E0C8', '#7A4A12')));

  // Exports
  const csv = PP.ppCsv(doc, { todayIso: '2026-03-02' }).trim().split('\n');
  check('CSV: the original columns come first, unchanged, then object/status/started/done/late',
    csv[0].startsWith('phase,task,resource,comment,January W1') && csv[0].endsWith(',object,status,started,done,late'));
  const apRow = csv.find(l => /,AP,done,2026-02-02,2026-02-20,$/.test(l));
  check('CSV: ONE ROW PER OBJECT, with its status and dates', !!apRow && csv.filter(l => /^Analysis,Analysis,/.test(l)).length === 3, csv.slice(1, 4).join(' | '));
  check('CSV: a late object says so', csv.some(l => /,AR,not started,,,late$/.test(l)));
  check('CSV: a task with no objects is one row, keyed by nothing', csv.some(l => /^Analysis,No objects,.*,,done,2026-01-05,2026-01-05,$/.test(l)));
  const xls = PP.ppExcelHtml(doc, { todayIso: '2026-03-02' });
  check('Excel: the grid keeps its rows and gains an actuals table, one row per object',
    />Object<\/td>/.test(xls) && />Late<\/td>/.test(xls) && (xls.match(/>GL<\/td>/g) || []).length === 1);
  check('Excel: an old plan with no actuals exports exactly as before — no actuals table',
    !/>Object<\/td>/.test(PP.ppExcelHtml(PP.ppNewDoc(), { todayIso: '2026-03-02' })));
  const pf = PP.ppPortfolio({ a: doc }, { todayIso: '2026-03-02' });
  check('Portfolio: each plan carries % done and late', pf.projects[0].actuals.pct === 40 && pf.projects[0].actuals.late === 2 && pf.projects[0].actuals.tracking);
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
