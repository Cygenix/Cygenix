/* cygenix-project-plan.js — the Project Plan task-planning grid's engine.
 *
 * An Excel-style delivery plan: phases down the side, tasks in rows, a
 * month → week timeline across the top, work bars shaded in the phase's
 * tint and named milestone markers (Meeting 2, Session 5) standing
 * vertically in their week. This module is the pure part — week math,
 * document shape, normalization, painting and CSV export — so the whole
 * planning model is testable headlessly; project_plan.html renders it.
 *
 * The timeline is a PLANNING grid, not a calendar: every month exposes the
 * same four week-slots, exactly like the spreadsheet it replaces, so
 * column arithmetic never shifts under a plan mid-project.
 */
(function () {
  'use strict';

  const PP_VERSION = 1;
  const PP_WEEKS = 4;                       // week-slots per month, fixed
  const PP_MAX_MONTHS = 24;

  const PP_MONTH_NAMES = ['January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'];

  // Phase tints — pastel fills for work bars and the rotated phase label.
  const PP_PALETTE = [
    { name: 'peach',  bg: '#F8E0C8', fg: '#7A4A12' },
    { name: 'blue',   bg: '#D6E4F0', fg: '#1F4E79' },
    { name: 'green',  bg: '#D9E8D0', fg: '#375623' },
    { name: 'lilac',  bg: '#E4DCEF', fg: '#4B3869' },
    { name: 'sand',   bg: '#EFE7CE', fg: '#6A5A1E' },
    { name: 'rose',   bg: '#F3DBDB', fg: '#7A2E2E' },
    // Red is the emphasis tint — key deliverables, Go Lives, cutover
    // weekends. Deliberately stronger than the pastel 'rose' beside it so it
    // reads as a signal, not another phase colour.
    { name: 'red',    bg: '#F0A9A2', fg: '#7A1710' },
  ];
  // Auto-assignment (imports, "+ Phase") rotates through the tints BEFORE
  // red: an emphasis colour nobody chose is an emphasis colour nobody
  // believes. Red is only ever picked by hand.
  const PP_AUTO_TINTS = PP_PALETTE.length - 1;
  // Milestones are always the green of the source spreadsheet — they must
  // read as one family across every phase.
  const PP_MILESTONE = { bg: '#C6D9A8', fg: '#3B4A22' };

  const ppId = () => 'pp' + Math.random().toString(36).slice(2, 9);

  // ── Timeline math ────────────────────────────────────────────────────────
  // start: 'YYYY-MM'. Returns one entry per month with a label that carries
  // the year only when the range crosses into a new one.
  function ppMonths(start, months) {
    const m = /^(\d{4})-(\d{2})$/.exec(String(start || ''));
    let y = m ? Number(m[1]) : 2026, mo = m ? Number(m[2]) - 1 : 0;
    if (mo < 0 || mo > 11) mo = 0;
    const n = Math.max(1, Math.min(PP_MAX_MONTHS, Number(months) || 1));
    const out = [];
    const firstYear = y;
    for (let i = 0; i < n; i++) {
      out.push({
        ym: y + '-' + String(mo + 1).padStart(2, '0'),
        label: PP_MONTH_NAMES[mo] + (y !== firstYear ? ' ' + y : ''),
        year: y,
      });
      mo++; if (mo > 11) { mo = 0; y++; }
    }
    return out;
  }

  const ppCellKey = (taskId, ym, w) => taskId + '|' + ym + '|' + w;

  // ── Document shape ───────────────────────────────────────────────────────
  function ppNewDoc(name) {
    const phase = { id: ppId(), name: 'Kick-off', color: 0 };
    return {
      v: PP_VERSION,
      name: name || 'Migration plan',
      client: '',                 // carried from the Effort Estimator on import
      timeline: { start: '2026-01', months: 8 },
      phases: [phase],
      tasks: [{ id: ppId(), phaseId: phase.id,
        title: 'Kick-off meeting', resource: '', comment: '' }],
      cells: {},
    };
  }

  // Tolerant normalization: whatever a stale store holds, the page gets a
  // renderable document. Cells that reference a missing task or a week
  // outside the timeline are dropped, never rendered half-broken.
  function ppNormalize(doc) {
    const d = (doc && typeof doc === 'object') ? doc : {};
    const out = ppNewDoc(d.name);
    out.name = String(d.name || out.name).slice(0, 80);
    out.client = String(d.client || '').slice(0, 120);
    const t = d.timeline || {};
    out.timeline = {
      start: /^\d{4}-\d{2}$/.test(String(t.start)) ? t.start : '2026-01',
      months: Math.max(1, Math.min(PP_MAX_MONTHS, Number(t.months) || 8)),
    };
    out.phases = (Array.isArray(d.phases) ? d.phases : []).map((p, i) => ({
      id: String(p && p.id || ppId()),
      name: String(p && p.name || 'Phase ' + (i + 1)).slice(0, 60),
      color: Math.abs(Number(p && p.color) || 0) % PP_PALETTE.length,
    }));
    if (!out.phases.length) out.phases = [{ id: ppId(), name: 'Kick-off', color: 0 }];
    const phaseIds = new Set(out.phases.map(p => p.id));
    out.tasks = (Array.isArray(d.tasks) ? d.tasks : []).map(x => ({
      id: String(x && x.id || ppId()),
      phaseId: phaseIds.has(String(x && x.phaseId)) ? String(x.phaseId) : out.phases[0].id,
      title: String(x && x.title || '').slice(0, 600),
      resource: String(x && x.resource || '').slice(0, 60),
      comment: String(x && x.comment || '').slice(0, 200),
    }));
    const taskIds = new Set(out.tasks.map(x => x.id));
    const months = new Set(ppMonths(out.timeline.start, out.timeline.months).map(m => m.ym));
    out.cells = {};
    for (const [k, v] of Object.entries(d.cells || {})) {
      const parts = String(k).split('|');
      if (parts.length !== 3) continue;
      const [taskId, ym, w] = parts;
      if (!taskIds.has(taskId) || !months.has(ym)) continue;
      const wn = Number(w);
      if (!(wn >= 1 && wn <= PP_WEEKS)) continue;
      if (v && v.t === 'work') out.cells[k] = { t: 'work' };
      else if (v && v.t === 'mile') out.cells[k] = { t: 'mile', label: String(v.label || 'Milestone').slice(0, 40) };
    }
    return out;
  }

  // ── Painting ─────────────────────────────────────────────────────────────
  // One tool, one cell. Painting the same state again erases it, so a
  // misclick undoes itself without switching tools.
  function ppPaint(doc, key, tool, label) {
    const cur = doc.cells[key];
    if (tool === 'erase') { delete doc.cells[key]; return null; }
    if (tool === 'work') {
      if (cur && cur.t === 'work') { delete doc.cells[key]; return null; }
      doc.cells[key] = { t: 'work' };
      return doc.cells[key];
    }
    if (tool === 'mile') {
      const lab = String(label || 'Milestone').slice(0, 40);
      if (cur && cur.t === 'mile' && cur.label === lab) { delete doc.cells[key]; return null; }
      doc.cells[key] = { t: 'mile', label: lab };
      return doc.cells[key];
    }
    return cur || null;
  }

  function ppStats(doc) {
    let work = 0, miles = 0;
    for (const v of Object.values(doc.cells || {})) v.t === 'mile' ? miles++ : work++;
    return { phases: doc.phases.length, tasks: doc.tasks.length, work, miles };
  }

  // Tasks in phase order — the grid's row order.
  function ppRows(doc) {
    const rows = [];
    for (const p of doc.phases) {
      const tasks = doc.tasks.filter(t => t.phaseId === p.id);
      for (let i = 0; i < tasks.length; i++)
        rows.push({ phase: p, task: tasks[i], first: i === 0, span: tasks.length });
    }
    return rows;
  }

  // ── CSV export ───────────────────────────────────────────────────────────
  function ppCsvCell(v) {
    const s = String(v == null ? '' : v);
    return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  }
  function ppCsv(doc) {
    const months = ppMonths(doc.timeline.start, doc.timeline.months);
    const head = ['phase', 'task', 'resource', 'comment'];
    for (const m of months) for (let w = 1; w <= PP_WEEKS; w++) head.push(m.label + ' W' + w);
    const lines = [head.map(ppCsvCell).join(',')];
    for (const r of ppRows(doc)) {
      const row = [r.phase.name, r.task.title, r.task.resource, r.task.comment];
      for (const m of months) for (let w = 1; w <= PP_WEEKS; w++) {
        const c = doc.cells[ppCellKey(r.task.id, m.ym, w)];
        row.push(!c ? '' : (c.t === 'mile' ? c.label : '#'));
      }
      lines.push(row.map(ppCsvCell).join(','));
    }
    return lines.join('\n') + '\n';
  }

  // ── Import from the Effort Estimator ─────────────────────────────────────
  // One source of truth: the estimate already knows the use cases, who runs
  // them, the dates and how long each piece takes. This converts it into a
  // plan — a ONE-WAY copy, freely editable afterwards, never a live link.
  //
  // est: the estimator document (CygenixEffortModel shape);
  // r:   its computed result (emCompute output).
  // Costed use cases become tasks under one phase, ticked modules become the
  // detail lines, the employee becomes the resource, and work bars paint
  // sequentially at the model's own working-day rates. The estimated
  // delivery and the due date land as milestones on the top row.

  // Which grid slot a calendar date falls in: whole months from the start
  // month, then the day mapped onto the four fixed week slots.
  function ppSlotForDate(startYm, iso) {
    const s = /^(\d{4})-(\d{2})$/.exec(String(startYm || ''));
    const d = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(iso || ''));
    if (!s || !d) return null;
    const monthIndex = (Number(d[1]) - Number(s[1])) * 12 + (Number(d[2]) - Number(s[2]));
    if (monthIndex < 0) return null;
    const week = Math.min(PP_WEEKS, Math.max(1, Math.ceil(Number(d[3]) / (31 / PP_WEEKS))));
    return { monthIndex, week };
  }

  // Which timeline column a date lands in, or null when it falls outside the
  // plan's window — before the start, or past the last month. Callers pass a
  // LOCAL date string: "this week" means the reader's week, not UTC's.
  function ppNowIndex(startYm, months, iso) {
    const s = ppSlotForDate(startYm, iso);
    if (!s) return null;
    const n = Math.max(1, Math.min(PP_MAX_MONTHS, Number(months) || 1));
    const i = s.monthIndex * PP_WEEKS + (s.week - 1);
    return i < n * PP_WEEKS ? i : null;
  }

  function ppFromEstimate(est, r, opts) {
    const o = opts || {};
    const costed = (r && r.perUseCase || []).filter(u => u.fp > 0);
    if (!costed.length) return null;

    const start = (est.meta && /^\d{4}-\d{2}/.test(est.meta.startDate))
      ? est.meta.startDate.slice(0, 7)
      : (o.fallbackStart || '2026-01');
    const rates = est.rates || {};
    const daysPerFP = rates.daysPerFP || 1.37;
    const wdPerSlot = (rates.workingDaysPerMonth || 19.5) / PP_WEEKS;
    const resource = (est.meta && est.meta.employee) || '';

    const doc = {
      v: PP_VERSION,
      name: (o.name || ('Plan — ' + (est.name || 'estimate'))).slice(0, 80),
      // The client, exactly as the estimate has it — one source of truth.
      client: [est.meta && est.meta.clientName,
        est.meta && est.meta.clientCode ? '(' + est.meta.clientCode + ')' : '']
        .filter(Boolean).join(' ').slice(0, 120),
      timeline: { start, months: 1 },
      phases: [], tasks: [], cells: {},
    };

    // Each costed USE CASE is a PHASE — its name on the rotated label, each
    // in its own tint — with its module list as the task beneath it.
    // Sequential bars: each use case takes its share of the working days,
    // starting where the previous one ends — the estimate as a schedule.
    let cursor = 0;
    const spans = [];
    for (let i = 0; i < costed.length; i++) {
      const u = costed[i];
      const phase = { id: ppId(), name: EstUC(u.id, u.name).slice(0, 60),
        color: i % PP_AUTO_TINTS };
      doc.phases.push(phase);
      const mods = Object.keys(est.ticks || {})
        .filter(k => k.startsWith(u.id + '|')).map(k => k.split('|')[1]);
      const detail = mods.slice(0, 12).map(m => ': ' + m)
        .concat(mods.length > 12 ? [': +' + (mods.length - 12) + ' more'] : []);
      const task = { id: ppId(), phaseId: phase.id,
        title: [EstUC(u.id, u.name)].concat(detail).join('\n').slice(0, 600),
        resource: String(resource).slice(0, 60),
        comment: (u.fp.toFixed(1) + ' FP' + (u.tc !== 1 ? ' · TC ' + u.tc : '')).slice(0, 200) };
      doc.tasks.push(task);
      const slots = Math.max(1, Math.ceil((u.fp * daysPerFP) / wdPerSlot));
      spans.push({ task, from: cursor, to: cursor + slots - 1 });
      cursor += slots;
    }
    let months = Math.max(1, Math.ceil(cursor / PP_WEEKS));

    // Milestones on the top row: the model's projected delivery, and the
    // client's due date. The timeline stretches to show them (capped).
    const miles = [];
    const del = r.delivery && ppSlotForDate(start, r.delivery);
    if (del) miles.push({ slot: del, label: 'Est. delivery' });
    const due = est.meta && est.meta.dueDate && ppSlotForDate(start, est.meta.dueDate);
    if (due) miles.push({ slot: due, label: 'Due date' });
    for (const m of miles) months = Math.max(months, m.slot.monthIndex + 1);
    months = Math.min(PP_MAX_MONTHS, months);
    doc.timeline.months = months;

    const monthList = ppMonths(start, months);
    for (const s of spans) {
      for (let w = s.from; w <= s.to; w++) {
        const m = monthList[Math.floor(w / PP_WEEKS)];
        if (!m) break;                      // ran past the capped timeline
        doc.cells[ppCellKey(s.task.id, m.ym, (w % PP_WEEKS) + 1)] = { t: 'work' };
      }
    }
    const topTask = doc.tasks[0];
    for (const m of miles) {
      const mm = monthList[m.slot.monthIndex];
      if (!mm) continue;
      doc.cells[ppCellKey(topTask.id, mm.ym, m.slot.week)] = { t: 'mile', label: m.label };
    }
    return ppNormalize(doc);
  }
  // The estimator's use-case names, resolved locally so this module never
  // requires the effort model: the computed result already carries them.
  function EstUC(id, name) { return String(name || id); }

  // ── Portfolio: every plan on one shared timeline ─────────────────────────
  // The multi-project view's model. Each plan becomes one project lane on a
  // GLOBAL week axis (the union of every plan's timeline): per-week phase
  // hits for the striped lane, milestones re-based to global weeks, phase
  // summaries for the expanded rows, and per-resource loads so the page can
  // hatch any week a person is booked on two projects at once. Pure — the
  // page only draws it.
  const ppMonthDiff = (aYm, bYm) => {
    const a = /^(\d{4})-(\d{2})$/.exec(String(aYm)), b = /^(\d{4})-(\d{2})$/.exec(String(bYm));
    if (!a || !b) return null;
    return (Number(b[1]) - Number(a[1])) * 12 + (Number(b[2]) - Number(a[2]));
  };
  const PP_PORTFOLIO_MAX_MONTHS = 36;
  const ppFpOf = (comment) => {
    const m = /(\d+(?:\.\d+)?) FP/.exec(String(comment || ''));
    return m ? Number(m[1]) : 0;
  };

  function ppPortfolio(plansByName) {
    const list = Object.values(plansByName || {}).map(ppNormalize)
      .filter(p => p.tasks.length);
    if (!list.length) return null;

    const startYm = list.map(p => p.timeline.start).sort()[0];
    let months = 1;
    for (const p of list)
      months = Math.max(months, (ppMonthDiff(startYm, p.timeline.start) || 0) + p.timeline.months);
    months = Math.min(PP_PORTFOLIO_MAX_MONTHS, months);
    const weeks = months * PP_WEEKS;

    const projects = list.map((p, idx) => {
      const phaseIdx = new Map(p.phases.map((ph, i) => [ph.id, i]));
      const taskById = new Map(p.tasks.map(t => [t.id, t]));
      const byWeek = Array.from({ length: weeks }, () => []);
      const milestones = [];
      const phaseAgg = p.phases.map(ph => ({
        name: ph.name, color: ph.color, weeks: new Set(), resources: new Set(), fp: 0 }));
      for (const [k, v] of Object.entries(p.cells)) {
        const [taskId, ym, w] = k.split('|');
        const task = taskById.get(taskId);
        if (!task) continue;
        const md = ppMonthDiff(startYm, ym);
        if (md == null || md < 0) continue;
        const gw = md * PP_WEEKS + (Number(w) - 1);
        if (gw < 0 || gw >= weeks) continue;
        if (v.t === 'mile') { milestones.push({ w: gw, label: v.label }); continue; }
        const pi = phaseIdx.get(task.phaseId);
        if (pi == null) continue;
        byWeek[gw].push({ phase: pi, resource: task.resource || '', task: task.title.split('\n')[0] });
        phaseAgg[pi].weeks.add(gw);
        if (task.resource) phaseAgg[pi].resources.add(task.resource);
      }
      for (const t of p.tasks) {
        const pi = phaseIdx.get(t.phaseId);
        if (pi != null) phaseAgg[pi].fp += ppFpOf(t.comment);
      }
      const worked = byWeek.map((a, w) => a.length ? w : -1).filter(w => w >= 0);
      return {
        id: idx, name: p.name, client: p.client,
        byWeek, milestones: milestones.sort((a, b) => a.w - b.w),
        phases: phaseAgg.map(a => ({ name: a.name, color: a.color,
          weeks: [...a.weeks].sort((x, y) => x - y), resources: [...a.resources],
          fp: Math.round(a.fp * 10) / 10 })),
        weeks: worked.length,
        start: worked.length ? worked[0] : null,
        fp: Math.round(phaseAgg.reduce((s, a) => s + a.fp, 0) * 10) / 10,
      };
    });

    // Resources: who is where, week by week, across every project in view.
    const byName = new Map();
    projects.forEach(pr => pr.byWeek.forEach((hits, w) => {
      for (const h of hits) {
        if (!h.resource) continue;
        let r = byName.get(h.resource);
        if (!r) byName.set(h.resource, r = Array.from({ length: weeks }, () => []));
        if (!r[w].some(x => x.project === pr.id)) r[w].push({ project: pr.id, phase: h.phase });
      }
    }));
    const resources = [...byName.entries()].map(([name, load]) => ({
      name, load, peak: Math.max(0, ...load.map(a => a.length)),
    })).sort((a, b) => a.name.localeCompare(b.name));

    return { start: startYm, months, weeks, projects, resources };
  }

  // ── Excel export ─────────────────────────────────────────────────────────
  // An Excel-compatible HTML workbook (.xls): Excel opens it with the grid
  // intact — phase tints, rotated phase labels (mso-rotate), work bars in
  // the phase colour and green milestone markers. HTML because a real .xlsx
  // is a zip archive, which a dependency-free static page cannot author;
  // this format round-trips into Excel and prints identically.
  const xEsc = (s) => String(s == null ? '' : s).replace(/[&<>"]/g,
    c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

  function ppExcelHtml(doc) {
    const months = ppMonths(doc.timeline.start, doc.timeline.months);
    const rows = ppRows(doc);
    const th = 'background:#ECEEF0;border:0.5pt solid #999;font-weight:bold;text-align:center';
    const td = 'border:0.5pt solid #BBB';
    let h = '<html xmlns:x="urn:schemas-microsoft-com:office:excel"><head><meta charset="UTF-8">'
      + '<!--[if gte mso 9]><xml><x:ExcelWorkbook><x:ExcelWorksheets><x:ExcelWorksheet>'
      + '<x:Name>Project Plan</x:Name><x:WorksheetOptions><x:Print><x:ValidPrinterInfo/></x:Print>'
      + '</x:WorksheetOptions></x:ExcelWorksheet></x:ExcelWorksheets></x:ExcelWorkbook></xml><![endif]-->'
      + '</head><body><table style="border-collapse:collapse;font-family:Arial;font-size:9pt">';
    h += '<tr><td colspan="4" style="font-size:12pt;font-weight:bold">' + xEsc(doc.name) + '</td></tr>';
    if (doc.client)
      h += '<tr><td style="font-weight:bold">Client</td><td colspan="3">' + xEsc(doc.client) + '</td></tr>';
    h += '<tr><td colspan="4"></td></tr>';
    h += '<tr><td style="' + th + '"></td><td style="' + th + ';text-align:left">Task</td>'
      + '<td style="' + th + '">Resource</td><td style="' + th + '">Comment</td>'
      + months.map(m => '<td colspan="' + PP_WEEKS + '" style="' + th + '">' + xEsc(m.label) + '</td>').join('')
      + '</tr>';
    h += '<tr><td style="' + th + '"></td><td style="' + th + '"></td><td style="' + th + '"></td><td style="' + th + '"></td>'
      + months.map(() => Array.from({ length: PP_WEEKS }, (_, i) =>
          '<td style="' + th + ';font-weight:normal">' + (i + 1) + '</td>').join('')).join('')
      + '</tr>';
    for (const r of rows) {
      const pal = PP_PALETTE[r.phase.color] || PP_PALETTE[0];
      h += '<tr>';
      if (r.first)
        h += '<td rowspan="' + r.span + '" style="' + td + ';background:' + pal.bg + ';color:' + pal.fg
          + ';mso-rotate:90;font-weight:bold;text-align:center;vertical-align:middle;width:24px">'
          + xEsc(r.phase.name) + '</td>';
      h += '<td style="' + td + ';vertical-align:top;white-space:normal;width:280px">'
        + xEsc(r.task.title).replace(/\n/g, '<br>') + '</td>'
        + '<td style="' + td + ';vertical-align:top">' + xEsc(r.task.resource) + '</td>'
        + '<td style="' + td + ';vertical-align:top">' + xEsc(r.task.comment) + '</td>';
      for (const m of months) for (let w = 1; w <= PP_WEEKS; w++) {
        const c = doc.cells[ppCellKey(r.task.id, m.ym, w)];
        if (c && c.t === 'mile')
          h += '<td style="' + td + ';background:' + PP_MILESTONE.bg + ';color:' + PP_MILESTONE.fg
            + ';mso-rotate:90;font-size:7pt;text-align:center">' + xEsc(c.label) + '</td>';
        else if (c && c.t === 'work')
          h += '<td style="' + td + ';background:' + pal.bg + '"></td>';
        else h += '<td style="' + td + '"></td>';
      }
      h += '</tr>';
    }
    h += '</table></body></html>';
    return h;
  }

  const api = {
    PP_VERSION, PP_WEEKS, PP_MAX_MONTHS, PP_PALETTE, PP_AUTO_TINTS, PP_MILESTONE, PP_MONTH_NAMES,
    ppId, ppMonths, ppCellKey, ppNewDoc, ppNormalize, ppPaint, ppStats, ppRows, ppCsv,
    ppSlotForDate, ppNowIndex, ppFromEstimate, ppExcelHtml,
    ppMonthDiff, ppFpOf, ppPortfolio,
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (typeof window !== 'undefined') window.CygenixProjectPlan = api;
})();
