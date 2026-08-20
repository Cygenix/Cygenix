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
  ];
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

  const api = {
    PP_VERSION, PP_WEEKS, PP_MAX_MONTHS, PP_PALETTE, PP_MILESTONE, PP_MONTH_NAMES,
    ppId, ppMonths, ppCellKey, ppNewDoc, ppNormalize, ppPaint, ppStats, ppRows, ppCsv,
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (typeof window !== 'undefined') window.CygenixProjectPlan = api;
})();
