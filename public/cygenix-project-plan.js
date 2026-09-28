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
 *
 * ACTUALS (Sep-2026, plan v2)
 * The grid showed only the plan. v2 adds what has actually happened, per
 * OBJECT — the Configurator modules under each task ("AP", "Addresses") —
 * without adding rows or columns:
 *
 *   task.title     the task name only (it used to carry the objects too)
 *   task.objects   the full list of object names — never truncated in
 *                  storage; "+N more" is a display concern only
 *   task.detail    any other lines a person typed under the title, kept
 *   plan.actuals   { [taskId]: { [object]: { status, startedAt, doneAt,
 *                  source } } } — an object with no record is not started;
 *                  a task with no objects keeps its status under "_task"
 *
 * WHY THE MIGRATION EXISTS. v1 stored the objects as text baked into the
 * title — "Initial analysis\n: Addresses\n: AP … \n: +11 more" — and the
 * import cut the list at twelve, so eleven objects of that task were simply
 * gone. Per-object status cannot work on names that are not stored. On load
 * a v1 plan is split (first line → title, ": " lines → objects, anything
 * else → detail); where it says "+N more", the caller may pass a recover()
 * that rebuilds the full list from the Configurator, and it is accepted only
 * if it agrees with what was parsed (same first names, exactly N more).
 * Otherwise the parsed names are kept and the task is flagged
 * objectsIncomplete, which the page says out loud. Migration runs once: the
 * result is v2, and a v2 plan is never split again.
 *
 * LATENESS is one pure function, ppObjectState(), against the plan's own
 * week grid and the page's own "now" (ppWeekIndex — the same calculation
 * ppNowIndex uses, without its window clamp, so a plan whose end has passed
 * can still be late). Everything that shows or counts lateness calls it.
 */
(function () {
  'use strict';

  const PP_VERSION = 2;
  const PP_STATUSES = ['not_started', 'active', 'done'];
  const PP_TASK_KEY = '_task';              // status of a task with no objects
  const PP_MAX_OBJECTS = 500;
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
        title: 'Kick-off meeting', resource: '', comment: '', objects: [], detail: '' }],
      cells: {},
      actuals: {},
    };
  }

  // ── v1 → v2 ──────────────────────────────────────────────────────────────
  // Split a v1 title: first line is the name, ": X" lines are objects, a
  // ": +N more" line is the count the import threw away, and any other line
  // is detail a person typed — kept, never dropped.
  function ppSplitTitle(title) {
    const lines = String(title == null ? '' : title).split('\n');
    const out = { title: (lines[0] || '').trim(), objects: [], detail: [], more: 0 };
    for (const raw of lines.slice(1)) {
      const line = raw.replace(/\s+$/, '');
      const m = /^\s*:\s?(.*)$/.exec(line);
      if (m) {
        const name = m[1].trim();
        const more = /^\+(\d+) more$/.exec(name);
        if (more) { out.more += Number(more[1]); continue; }
        if (name) out.objects.push(name);
      } else if (line.trim()) out.detail.push(line.trim());
    }
    out.detail = out.detail.join('\n');
    return out;
  }
  // A recovered list is trusted only when it agrees with what survived: the
  // same names first, in the same order, and exactly as many more as the
  // "+N more" line said. An estimate edited since the import fails this and
  // the task stays flagged rather than gaining someone else's objects.
  function ppAcceptRecovered(parsed, more, recovered) {
    if (!Array.isArray(recovered) || !more) return false;
    if (recovered.length !== parsed.length + more) return false;
    for (let i = 0; i < parsed.length; i++) if (String(recovered[i]) !== parsed[i]) return false;
    return true;
  }
  function ppCleanObjects(list) {
    const seen = new Set(), out = [];
    for (const x of (Array.isArray(list) ? list : [])) {
      const n = String(x == null ? '' : x).trim().slice(0, 120);
      if (!n || n === PP_TASK_KEY || seen.has(n)) continue;
      seen.add(n); out.push(n);
      if (out.length >= PP_MAX_OBJECTS) break;
    }
    return out;
  }
  const ppIsIso = (v) => /^\d{4}-\d{2}-\d{2}$/.test(String(v || ''));
  function ppCleanActuals(a, taskIds) {
    const out = {};
    if (!a || typeof a !== 'object') return out;
    for (const [tid, recs] of Object.entries(a)) {
      if (!taskIds.has(tid) || !recs || typeof recs !== 'object') continue;
      const t = {};
      for (const [k, r] of Object.entries(recs)) {
        if (!r || PP_STATUSES.indexOf(r.status) < 1) continue;     // not_started is "no record"
        t[String(k).slice(0, 120)] = {
          status: r.status,
          startedAt: ppIsIso(r.startedAt) ? r.startedAt : '',
          doneAt: r.status === 'done' && ppIsIso(r.doneAt) ? r.doneAt : '',
          source: r.source === 'auto' ? 'auto' : 'manual',
        };
      }
      if (Object.keys(t).length) out[tid] = t;
    }
    return out;
  }
  // Does this stored plan still need the v1 → v2 split?
  const ppIsLegacy = (doc) => !!doc && typeof doc === 'object' && !(Number(doc.v) >= 2);

  // Tolerant normalization: whatever a stale store holds, the page gets a
  // renderable document. Cells that reference a missing task or a week
  // outside the timeline are dropped, never rendered half-broken.
  // opts.recover(planName, taskTitle, parsedObjects, more) → full list or
  // null, consulted only while migrating a v1 task that lost objects.
  function ppNormalize(doc, opts) {
    const d = (doc && typeof doc === 'object') ? doc : {};
    const legacy = ppIsLegacy(d) && Array.isArray(d.tasks);
    const recover = opts && typeof opts.recover === 'function' ? opts.recover : null;
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
    out.tasks = (Array.isArray(d.tasks) ? d.tasks : []).map(x => {
      const t = {
        id: String(x && x.id || ppId()),
        phaseId: phaseIds.has(String(x && x.phaseId)) ? String(x.phaseId) : out.phases[0].id,
        title: '', resource: String(x && x.resource || '').slice(0, 60),
        comment: String(x && x.comment || '').slice(0, 200),
        objects: [], detail: '',
      };
      if (legacy) {
        const raw = String(x && x.title || '');
        const sp = ppSplitTitle(raw);
        t.title = sp.title.slice(0, 200);
        t.objects = ppCleanObjects(sp.objects);
        t.detail = sp.detail.slice(0, 2000);
        // v1 capped the whole title at 600 characters, so a long list may
        // have been cut mid-name with no "+N more" left to say so.
        const cut = raw.length >= 600;
        if (sp.more && recover) {
          let full = null;
          try { full = recover(out.name, t.title, t.objects.slice(), sp.more); } catch (e) { full = null; }
          if (ppAcceptRecovered(t.objects, sp.more, full)) t.objects = ppCleanObjects(full);
          else t.objectsIncomplete = true;
        } else if (sp.more || cut) t.objectsIncomplete = true;
      } else {
        t.title = String(x && x.title || '').split('\n')[0].slice(0, 200);
        t.objects = ppCleanObjects(x && x.objects);
        t.detail = String(x && x.detail || '').slice(0, 2000);
        if (x && x.objectsIncomplete) t.objectsIncomplete = true;
      }
      return t;
    });
    const taskIds = new Set(out.tasks.map(x => x.id));
    out.actuals = legacy ? {} : ppCleanActuals(d.actuals, taskIds);
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

  // ── Actuals: status and lateness ─────────────────────────────────────────
  // The task's planned work weeks, as column indexes on the plan's grid.
  function ppTaskWeeks(doc, taskId) {
    const out = [];
    for (const [k, v] of Object.entries(doc.cells || {})) {
      if (!v || v.t !== 'work') continue;
      const [tid, ym, w] = k.split('|');
      if (tid !== taskId) continue;
      const md = ppMonthDiff(doc.timeline.start, ym);
      if (md == null) continue;
      out.push(md * PP_WEEKS + (Number(w) - 1));
    }
    return out.sort((a, b) => a - b);
  }
  // What the page shows a dot for: every object, or the task itself.
  const ppTaskKeys = (task) => (task.objects && task.objects.length) ? task.objects : [PP_TASK_KEY];
  const ppRecord = (doc, taskId, key) => ((doc.actuals || {})[taskId] || {})[key] || null;

  // THE one status function. nowWeek is ppWeekIndex(start, today) — null
  // when unknown, negative before the plan starts.
  //   late_start  not started, first planned week already behind us
  //   late        not done, last planned week already behind us
  //   done_late   done, but in a week after the last planned one (stays
  //               green; carries the flag)
  // A task with no planned work weeks gets no late logic at all.
  function ppObjectState(doc, task, objectName, nowWeek) {
    const key = objectName == null || objectName === '' ? PP_TASK_KEY : objectName;
    const rec = ppRecord(doc, task.id, key);
    const status = rec ? rec.status : 'not_started';
    const weeks = ppTaskWeeks(doc, task.id);
    const out = { status, late: false, lateKind: null, record: rec,
      first: weeks.length ? weeks[0] : null, last: weeks.length ? weeks[weeks.length - 1] : null };
    if (!weeks.length) return out;
    if (status === 'done') {
      const dw = rec && rec.doneAt ? ppWeekIndex(doc.timeline.start, rec.doneAt) : null;
      if (dw != null && dw > out.last) { out.late = true; out.lateKind = 'done_late'; }
      return out;
    }
    if (nowWeek == null) return out;
    if (out.last < nowWeek) { out.late = true; out.lateKind = 'late'; }
    else if (status === 'not_started' && out.first < nowWeek) { out.late = true; out.lateKind = 'late_start'; }
    return out;
  }

  // One task's roll-up. `open` counts lateness still to be dealt with (late,
  // late start); done_late is history and is counted apart.
  function ppTaskSummary(doc, task, nowWeek) {
    const keys = ppTaskKeys(task);
    const s = { total: keys.length, done: 0, active: 0, late: 0, doneLate: 0,
      any: !!(doc.actuals && doc.actuals[task.id] && Object.keys(doc.actuals[task.id]).length) };
    for (const k of keys) {
      const st = ppObjectState(doc, task, k === PP_TASK_KEY ? '' : k, nowWeek);
      if (st.status === 'done') s.done++;
      if (st.status === 'active') s.active++;
      if (st.lateKind === 'done_late') s.doneLate++;
      else if (st.late) s.late++;
    }
    s.complete = s.done === s.total;
    return s;
  }
  // Tracking starts with the first status set anywhere in the plan. Until
  // then the page draws no red at all: a plan nobody has started recording
  // against is not "late", it is simply not being tracked yet.
  const ppTracking = (doc) => !!doc && !!doc.actuals && Object.keys(doc.actuals).length > 0;
  function ppPlanSummary(doc, nowWeek) {
    const out = { total: 0, done: 0, late: 0, doneLate: 0, pct: 0, tracking: ppTracking(doc) };
    for (const t of doc.tasks) {
      const s = ppTaskSummary(doc, t, nowWeek);
      out.total += s.total; out.done += s.done; out.late += s.late; out.doneLate += s.doneLate;
    }
    out.pct = out.total ? Math.round(100 * out.done / out.total) : 0;
    return out;
  }
  // Where the work has actually happened, as grid columns: from the earliest
  // start (or done date) to the latest done date — or to now while anything
  // in the task is still open. null when nothing has started.
  function ppTaskActualSpan(doc, task, nowWeek) {
    const recs = Object.values((doc.actuals || {})[task.id] || {});
    let from = null, to = null;
    for (const r of recs) {
      // The earlier of the two dates, whatever order they were entered in.
      const first = [r.startedAt, r.doneAt].filter(ppIsIso).sort()[0];
      const a = first ? ppWeekIndex(doc.timeline.start, first) : null;
      if (a != null && (from == null || a < from)) from = a;
      const b = r.doneAt ? ppWeekIndex(doc.timeline.start, r.doneAt) : null;
      if (b != null && (to == null || b > to)) to = b;
    }
    if (from == null) return null;
    if (!ppTaskSummary(doc, task, nowWeek).complete && nowWeek != null) to = Math.max(to == null ? nowWeek : to, nowWeek);
    if (to == null || to < from) to = from;
    return { from, to };
  }

  // Set a status, stamping dates the way a person would: Active stamps the
  // start, Done stamps the finish (and the start if there was none), Not
  // started removes the record. Dates already set are kept. Always manual:
  // a later automatic source must never overrule a person.
  function ppSetStatus(doc, taskId, key, status, todayIso) {
    if (PP_STATUSES.indexOf(status) < 0) return null;
    const k = key == null || key === '' ? PP_TASK_KEY : String(key);
    doc.actuals = doc.actuals || {};
    const bucket = doc.actuals[taskId] || {};
    const prev = bucket[k] || {};
    if (status === 'not_started') {
      delete bucket[k];
      if (Object.keys(bucket).length) doc.actuals[taskId] = bucket; else delete doc.actuals[taskId];
      return null;
    }
    const today = ppIsIso(todayIso) ? todayIso : '';
    const rec = { status,
      startedAt: prev.startedAt || today,
      doneAt: status === 'done' ? (prev.status === 'done' && prev.doneAt ? prev.doneAt : today) : '',
      source: 'manual' };
    bucket[k] = rec;
    doc.actuals[taskId] = bucket;
    return rec;
  }
  // Edit the dates on an existing record. Blank clears; anything else must
  // be YYYY-MM-DD or it is ignored.
  function ppSetDates(doc, taskId, key, dates) {
    const k = key == null || key === '' ? PP_TASK_KEY : String(key);
    const rec = ppRecord(doc, taskId, k);
    if (!rec) return null;
    for (const f of ['startedAt', 'doneAt']) {
      if (!dates || !(f in dates)) continue;
      const v = String(dates[f] || '');
      if (v === '' ) rec[f] = '';
      else if (ppIsIso(v)) rec[f] = v;
    }
    if (rec.status !== 'done') rec.doneAt = '';
    // Work cannot start after it finished. Marking something Done stamps
    // today as its start if it had none; moving the done date back to when
    // the work really finished must pull that start back with it.
    if (rec.doneAt && rec.startedAt && rec.startedAt > rec.doneAt) rec.startedAt = rec.doneAt;
    rec.source = 'manual';
    return rec;
  }
  const PP_LATE_LABEL = { late: 'late', late_start: 'late start', done_late: 'done late' };

  // A stronger shade of a phase tint: the tint mixed toward its own ink.
  // Used where planned work has actually happened, so the bar keeps its hue.
  function ppShade(bg, fg, t) {
    const hex = (h) => { const n = parseInt(String(h).replace('#', ''), 16); return [n >> 16 & 255, n >> 8 & 255, n & 255]; };
    const a = hex(bg), b = hex(fg), k = t == null ? 0.3 : t;
    return '#' + a.map((v, i) => Math.round(v + (b[i] - v) * k).toString(16).padStart(2, '0')).join('');
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
  // One row per OBJECT (a task with none is one row). The original columns
  // come first, unchanged; object, status, started, done and late follow.
  // opts.todayIso lets the late column be worked out; without it, or before
  // tracking starts, it stays blank — as the grid shows no red then either.
  function ppCsv(doc, opts) {
    const months = ppMonths(doc.timeline.start, doc.timeline.months);
    const head = ['phase', 'task', 'resource', 'comment'];
    for (const m of months) for (let w = 1; w <= PP_WEEKS; w++) head.push(m.label + ' W' + w);
    head.push('object', 'status', 'started', 'done', 'late');
    const lines = [head.map(ppCsvCell).join(',')];
    const now = opts && opts.todayIso && ppTracking(doc) ? ppWeekIndex(doc.timeline.start, opts.todayIso) : null;
    for (const r of ppRows(doc)) {
      const base = [r.phase.name, r.task.title + (r.task.detail ? '\n' + r.task.detail : ''), r.task.resource, r.task.comment];
      for (const m of months) for (let w = 1; w <= PP_WEEKS; w++) {
        const c = doc.cells[ppCellKey(r.task.id, m.ym, w)];
        base.push(!c ? '' : (c.t === 'mile' ? c.label : '#'));
      }
      for (const k of ppTaskKeys(r.task)) {
        const obj = k === PP_TASK_KEY ? '' : k;
        const st = ppObjectState(doc, r.task, obj, now);
        const rec = st.record || {};
        lines.push(base.concat([obj, st.status.replace('_', ' '), rec.startedAt || '', rec.doneAt || '',
          (now != null || st.lateKind === 'done_late') && ppTracking(doc) ? (PP_LATE_LABEL[st.lateKind] || '') : ''])
          .map(ppCsvCell).join(','));
      }
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
  // The column a date falls in, NOT clamped to the plan's window: negative
  // before the start, past the last column after the end. ppNowIndex is this
  // plus the window check. Lateness needs the unclamped form — a plan whose
  // end has passed is exactly the plan most likely to be late.
  function ppWeekIndex(startYm, iso) {
    const s = /^(\d{4})-(\d{2})$/.exec(String(startYm || ''));
    const d = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(iso || ''));
    if (!s || !d) return null;
    const monthIndex = (Number(d[1]) - Number(s[1])) * 12 + (Number(d[2]) - Number(s[2]));
    const week = Math.min(PP_WEEKS, Math.max(1, Math.ceil(Number(d[3]) / (31 / PP_WEEKS))));
    return monthIndex * PP_WEEKS + (week - 1);
  }
  function ppNowIndex(startYm, months, iso) {
    const i = ppWeekIndex(startYm, iso);
    if (i == null || i < 0) return null;
    const n = Math.max(1, Math.min(PP_MAX_MONTHS, Number(months) || 1));
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
      v: PP_VERSION, actuals: {},
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
      // Every ticked module, as data. v1 baked the first twelve into the
      // title and wrote "+N more" for the rest — which lost them for good.
      const mods = ppModulesFor(est, u.id);
      const task = { id: ppId(), phaseId: phase.id,
        title: EstUC(u.id, u.name).slice(0, 200), objects: mods, detail: '',
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
  // The modules ticked for one use case, in the estimate's own order — the
  // one list both the import and the v1 recovery read, so they cannot differ.
  function ppModulesFor(est, ucId) {
    return Object.keys((est && est.ticks) || {})
      .filter(k => k.startsWith(ucId + '|')).map(k => k.split('|')[1]);
  }
  // The v1 recovery source: the estimate a plan was built from, found by the
  // name the import gave the plan ("Plan — <estimate>", maybe "(2)"), and the
  // use case whose name is the task's title. Returns a recover() for
  // ppNormalize, or null when the pieces are missing.
  function ppRecoverFromEstimates(estimates, compute) {
    const list = Object.values(estimates || {}).filter(e => e && typeof e === 'object');
    if (!list.length || typeof compute !== 'function') return null;
    return function (planName, taskTitle) {
      const base = String(planName || '').replace(/^Plan — /, '').replace(/ \(\d+\)$/, '');
      const est = list.find(e => String(e.name || '') === base);
      if (!est) return null;
      let r = null;
      try { r = compute(est); } catch (e) { return null; }
      const u = (r && r.perUseCase || []).find(x => EstUC(x.id, x.name) === taskTitle);
      return u ? ppModulesFor(est, u.id) : null;
    };
  }

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

  function ppPortfolio(plansByName, opts) {
    const list = Object.values(plansByName || {}).map(p => ppNormalize(p))
      .filter(p => p.tasks.length);
    const todayIso = opts && opts.todayIso;
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
      // Each plan against its own grid and its own "now".
      const sum = ppPlanSummary(p, todayIso ? ppWeekIndex(p.timeline.start, todayIso) : null);
      return {
        id: idx, name: p.name, client: p.client,
        actuals: { tracking: sum.tracking, pct: sum.pct, done: sum.done, total: sum.total, late: sum.late },
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

  function ppExcelHtml(doc, opts) {
    const months = ppMonths(doc.timeline.start, doc.timeline.months);
    const rows = ppRows(doc);
    const tracking = ppTracking(doc);
    const now = opts && opts.todayIso ? ppWeekIndex(doc.timeline.start, opts.todayIso) : null;
    const lateNow = tracking ? now : null;
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
      const objs = (r.task.objects || []).map(o => {
        const st = ppObjectState(doc, r.task, o, lateNow);
        return xEsc(o) + (tracking ? ' — ' + st.status.replace('_', ' ')
          + (st.late && tracking ? ' (' + PP_LATE_LABEL[st.lateKind] + ')' : '') : '');
      });
      h += '<td style="' + td + ';vertical-align:top;white-space:normal;width:280px">'
        + '<b>' + xEsc(r.task.title) + '</b>'
        + (r.task.detail ? '<br>' + xEsc(r.task.detail).replace(/\n/g, '<br>') : '')
        + (objs.length ? '<br>' + objs.join('<br>') : '') + '</td>'
        + '<td style="' + td + ';vertical-align:top">' + xEsc(r.task.resource) + '</td>'
        + '<td style="' + td + ';vertical-align:top">' + xEsc(r.task.comment) + '</td>';
      const span = tracking ? ppTaskActualSpan(doc, r.task, now) : null;
      const sum = tracking ? ppTaskSummary(doc, r.task, lateNow) : null;
      const weeks = ppTaskWeeks(doc, r.task.id);
      const last = weeks.length ? weeks[weeks.length - 1] : null;
      let col = 0;
      for (const m of months) for (let w = 1; w <= PP_WEEKS; w++, col++) {
        const c = doc.cells[ppCellKey(r.task.id, m.ym, w)];
        const done = span && col >= span.from && col <= span.to;
        const overdue = sum && !sum.complete && last != null && lateNow != null && col > last && col <= lateNow;
        if (c && c.t === 'mile')
          h += '<td style="' + td + ';background:' + PP_MILESTONE.bg + ';color:' + PP_MILESTONE.fg
            + ';mso-rotate:90;font-size:7pt;text-align:center">' + xEsc(c.label) + '</td>';
        else if (c && c.t === 'work')
          h += '<td style="' + td + ';background:' + (done ? ppShade(pal.bg, pal.fg) : pal.bg) + '"></td>';
        else if (overdue) h += '<td style="' + td + ';background:#F2C4BF"></td>';
        else h += '<td style="' + td + '"></td>';
      }
      h += '</tr>';
    }
    h += '</table>';
    // Actuals: one row per object, below the grid, so the grid itself keeps
    // the shape of the sheet it replaces.
    if (tracking) {
      h += '<br><table style="border-collapse:collapse;font-family:Arial;font-size:9pt">'
        + '<tr>' + ['Phase', 'Task', 'Object', 'Status', 'Started', 'Done', 'Late']
          .map(x => '<td style="' + th + '">' + x + '</td>').join('') + '</tr>';
      for (const r of rows) for (const k of ppTaskKeys(r.task)) {
        const obj = k === PP_TASK_KEY ? '' : k;
        const st = ppObjectState(doc, r.task, obj, lateNow);
        const rec = st.record || {};
        h += '<tr>' + [r.phase.name, r.task.title, obj, st.status.replace('_', ' '), rec.startedAt || '',
          rec.doneAt || '', PP_LATE_LABEL[st.lateKind] || '']
          .map((x, i) => '<td style="' + td + (i === 6 && x ? ';color:#9C3F38;font-weight:bold' : '') + '">' + xEsc(x) + '</td>').join('') + '</tr>';
      }
      h += '</table>';
    }
    h += '</body></html>';
    return h;
  }

  const api = {
    PP_VERSION, PP_WEEKS, PP_MAX_MONTHS, PP_PALETTE, PP_AUTO_TINTS, PP_MILESTONE, PP_MONTH_NAMES,
    PP_STATUSES, PP_TASK_KEY, PP_LATE_LABEL,
    ppId, ppMonths, ppCellKey, ppNewDoc, ppNormalize, ppPaint, ppStats, ppRows, ppCsv,
    ppSlotForDate, ppNowIndex, ppWeekIndex, ppFromEstimate, ppExcelHtml,
    ppMonthDiff, ppFpOf, ppPortfolio,
    ppSplitTitle, ppAcceptRecovered, ppIsLegacy, ppModulesFor, ppRecoverFromEstimates, ppCleanObjects,
    ppTaskWeeks, ppTaskKeys, ppObjectState, ppTaskSummary, ppPlanSummary, ppTracking,
    ppTaskActualSpan, ppSetStatus, ppSetDates, ppShade,
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (typeof window !== 'undefined') window.CygenixProjectPlan = api;
})();
