/* ============================================================================
   cygenix-home.js — what Home says, worked out before anything is drawn.
   ----------------------------------------------------------------------------
   THE FINDING THIS ANSWERS (design review, Sep-2026, finding 02)
   Home rendered five stacked panels of identical weight: a readiness strip
   hidden until data arrived, an active-project summary, Projects, Schedules,
   Project Status. Three of them opened on "Loading…". An operator opening
   the console to learn whether anything was broken had to go to Analytics
   for readiness, Jobs for failures, Assurance for breaches, Data Stream for
   blocked streams and the Task Manager for what runs next. Home had
   containers but no state: it told you what existed, not what was happening
   or what to do.

   WHAT THIS MODULE IS
   One pure function, homeModel(), that takes the shapes the console already
   stores and returns exactly what the screen shows, in reading order: the
   project header, the plan, three measures, the recent runs, the needs-you
   queue, the run in flight, the next scheduled runs and the footer facts —
   or the three-step empty state when there is no project. No DOM, no
   storage, no fetch, no clock it is not handed. The renderer in
   dashboard-app.js consumes this and knows nothing about jobs.

   NO NEW STORES
   Every block reads something that already exists:
     readiness      — pfConfidence(), the same call Analytics makes
     plan           — the pipeline model cygenix-pipeline.js already derives
     needs you      — pending approvals (rbac-admin), open breaches (the
                      assurance store), failed jobs (cygenix_jobs), blocked
                      streams (cygenix-datastream.js)
     in flight      — the running job record, when one exists
     next scheduled — the Task Agent's list, lazily; it must not block paint
     footer         — project and connection counts already in local state
   Four existing reads, not a new endpoint. The needs-you queue is built here
   so that Home and the screens it points at cannot describe one incident
   two ways.

   ORDER IS THE POINT
   Needs-you sorts by severity, then by age, oldest first. The single most
   urgent item gets the primary button; the rest get secondary. A queue that
   ordered by kind, or by arrival, would make the reader do the triage the
   screen exists to do.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixHome) root.CygenixHome = api;
})(typeof globalThis !== 'undefined' ? globalThis : (typeof window !== 'undefined' ? window : this), function () {
'use strict';

function str(v) { return v == null ? '' : String(v); }
function num(v, d) { var n = Number(v); return isFinite(n) ? n : (d || 0); }
function plural(n, one, many) { return n === 1 ? one : (many || one + 's'); }

/* ── Formatting ──────────────────────────────────────────────────────────
   Numbers on Home are read at a glance, so they are short: 12.4m, not
   12,400,000. Table cells keep the full figure with thousands separators,
   because a table is where somebody checks a number rather than sizes it. */
function fmtInt(n) {
  n = Math.round(num(n));
  return String(n).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}
function fmtCompact(n) {
  n = num(n);
  var abs = Math.abs(n);
  if (abs >= 1e9) return { value: (n / 1e9).toFixed(1).replace(/\.0$/, ''), unit: 'bn' };
  if (abs >= 1e6) return { value: (n / 1e6).toFixed(1).replace(/\.0$/, ''), unit: 'm' };
  if (abs >= 1e4) return { value: (n / 1e3).toFixed(0), unit: 'k' };
  return { value: fmtInt(n), unit: '' };
}

/* "Today 06:02", "Yest. 22:15", "Thu 02:00", "12 Aug" — the mock's own
   vocabulary, which is what an operator reads in a run list. */
function fmtWhen(ts, now) {
  var t = typeof ts === 'number' ? ts : Date.parse(ts);
  if (!isFinite(t)) return '—';
  var d = new Date(t), n = new Date(now);
  var hm = pad(d.getHours()) + ':' + pad(d.getMinutes());
  var dayMs = 86400000;
  var startToday = new Date(n.getFullYear(), n.getMonth(), n.getDate()).getTime();
  if (t >= startToday && t < startToday + dayMs) return 'Today ' + hm;
  if (t >= startToday - dayMs && t < startToday) return 'Yest. ' + hm;
  if (t >= startToday - 6 * dayMs && t < startToday) {
    return ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][d.getDay()] + ' ' + hm;
  }
  if (t >= startToday + dayMs && t < startToday + 2 * dayMs) return 'Tomorrow ' + hm;
  if (t >= startToday + dayMs && t < startToday + 7 * dayMs) {
    return ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][d.getDay()] + ' ' + hm;
  }
  return d.getDate() + ' ' + ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][d.getMonth()];
}
function pad(n) { return (n < 10 ? '0' : '') + n; }

function fmtDuration(ms) {
  if (!isFinite(ms) || ms == null || ms < 0) return '—';
  var m = Math.round(ms / 60000);
  if (m < 1) return '< 1 min';
  if (m < 60) return m + ' min';
  var h = Math.floor(m / 60), r = m % 60;
  return h + ' h' + (r ? ' ' + r + ' min' : '');
}

function fmtAgo(ts, now) {
  var t = typeof ts === 'number' ? ts : Date.parse(ts);
  if (!isFinite(t)) return '';
  var m = Math.max(0, Math.round((now - t) / 60000));
  if (m < 1) return 'just now';
  if (m < 60) return m + ' min';
  var h = Math.floor(m / 60);
  if (h < 24) return h + ' h ' + (m % 60) + ' min';
  var d = Math.floor(h / 24);
  return d + ' ' + plural(d, 'day');
}

function daysUntil(ts, now) {
  var t = typeof ts === 'number' ? ts : Date.parse(ts);
  if (!isFinite(t)) return null;
  return Math.ceil((t - now) / 86400000);
}

/* ── Job vocabulary ──────────────────────────────────────────────────────
   Four words, exactly, from the handoff: Complete · SQL ready · Failed ·
   Pending. The bucketing is the same test cygenix-pipeline.js and the
   dashboard use, spelled out here so this file stays requirable on its own
   — two screens disagreeing about what "failed" means is the bug the whole
   review exists to end. */
function bucketOf(j) {
  var s = str(j && (j.executionStatus || j.status)).toLowerCase();
  if (s === 'complete' || s === 'completed' || s === 'success') return 'complete';
  if (s === 'ready' || s === 'sql_ready' || s === 'sql-ready') return 'ready';
  if (s === 'failed' || s === 'fail' || s === 'error') return 'failed';
  if (s === 'running') return 'running';
  return 'pending';
}
var RESULT_WORD = { complete: 'Complete', ready: 'SQL ready', failed: 'Failed', pending: 'Pending', running: 'Running' };
var RESULT_TONE = { complete: 'ok', ready: 'accent', failed: 'fail', pending: 'muted', running: 'accent' };

function jobRunTs(j) {
  var t = Date.parse(j && (j.lastRun || j.completedAt || j.executedAt) || '');
  return isFinite(t) ? t : null;
}

/* ── The plan ────────────────────────────────────────────────────────────
   Five rows — Discover · Map · Load · Validate · Cutover — read off the
   pipeline model's six stages. Discover is the schema read, which needs both
   connections; Load is the jobs that have completed a run; Cutover is a
   date, because nothing having cut over is the normal state of a migration
   in build and only the calendar makes it a problem. Each row carries a
   percentage, how to fill the bar, and a status word — "Complete", "88%",
   "In progress", "Partial", a date, or "Not started". */
function planRows(pipeline, project, now) {
  var by = {};
  ((pipeline && pipeline.stages) || []).forEach(function (s) { by[s.key] = s; });
  var connect = by.connect || {}, analyse = by.analyse || {}, map = by.map || {};
  var cutover = by.cutover || {}, validate = by.validate || {};

  function row(label, stage, opts) {
    var o = opts || {};
    var pct = stage.pct || 0, state = stage.state || 'waiting';
    var fill, word, tone = 'muted';
    if (state === 'done') { fill = 'done'; pct = 100; word = 'Complete'; tone = 'text'; }
    else if (state === 'blocked') { fill = 'part'; word = 'Blocked'; tone = 'fail'; }
    else if (state === 'active') { fill = 'part'; word = o.activeWord || (pct + '%'); tone = o.activeWord ? 'accent' : 'text'; }
    else if (state === 'attention') { fill = 'part'; word = pct > 0 ? 'Partial' : 'Needs attention'; tone = 'warn'; }
    else { fill = 'none'; word = 'Not started'; }
    return { key: o.key || stage.key, label: label, pct: pct, fill: fill, word: word, tone: tone,
             href: stage.href || o.href || '', reason: stage.reason || '' };
  }

  var discover;
  if (connect.state && connect.state !== 'done') {
    discover = { key: 'discover', label: 'Discover', pct: Math.round((connect.pct || 0) / 2), fill: connect.count ? 'part' : 'none',
      word: connect.count ? 'Connect target' : 'Connect', tone: 'warn', href: connect.href || '/dashboard#goto=connections',
      reason: connect.reason || '' };
  } else {
    discover = row('Discover', analyse, { key: 'discover' });
  }
  var load = row('Load', cutover, { key: 'load', activeWord: 'In progress' });
  if (load.fill === 'none' && cutover.state === 'waiting' && (map.count || 0) > 0) load.word = 'Not started';

  var cut;
  var endDays = project && project.end ? daysUntil(project.end, now) : null;
  if (endDays != null) {
    var d = new Date(Date.parse(project.end));
    var when = d.getDate() + ' ' + ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][d.getMonth()];
    cut = { key: 'cutover', label: 'Cutover', pct: cutover.state === 'done' ? 100 : 0,
      fill: cutover.state === 'done' ? 'done' : 'none',
      word: cutover.state === 'done' ? 'Complete' : when,
      tone: endDays < 0 && cutover.state !== 'done' ? 'fail' : 'text', href: '/projects', reason: '' };
  } else {
    cut = { key: 'cutover', label: 'Cutover', pct: 0, fill: 'none', word: 'No date', tone: 'muted', href: '/projects',
      reason: 'The project has no target end date.' };
  }

  return [discover, row('Map', map), load, row('Validate', validate), cut];
}

/* ── The state sentence ──────────────────────────────────────────────────
   One line under the project name saying what is happening now. The
   pipeline model already writes an honest narrative; the cutover window is
   appended when the project has a date, because "6 days" is the number an
   operator is holding in their head. */
function stateSentence(input, pipeline, inFlight, now) {
  var parts = [];
  var ai = input.aiNarrative && str(input.aiNarrative.text).trim();
  if (inFlight) {
    parts.push('Loading ' + inFlight.name + ' — ' + inFlight.elapsed + ' in' + (inFlight.eta ? ', ' + inFlight.eta + ' to go' : '') + '.');
  } else if (ai) {
    // The summary the user asked Claude to write, on their own key, from the
    // Rewrite summary control. Analytics shows the same text beside the
    // pipeline card; two screens must not tell the project two ways.
    parts.push(ai);
  } else if (pipeline && pipeline.narrative) {
    parts.push(pipeline.narrative);
  }
  var project = input.project;
  if (project && project.end) {
    var d = daysUntil(project.end, now);
    if (d != null) {
      if (d > 1) parts.push('Cutover window opens in ' + d + ' days.');
      else if (d === 1) parts.push('Cutover window opens tomorrow.');
      else if (d === 0) parts.push('Cutover window opens today.');
      else parts.push('Cutover window closed ' + (-d) + ' ' + plural(-d, 'day') + ' ago.');
    }
  }
  return parts.join(' ');
}

/* ── Needs you ───────────────────────────────────────────────────────────
   Four sources, one queue. Severity: an approval waiting on a person is
   `fail` (nothing moves until they act), a critical breach or a hard-blocked
   stream is `fail`, a failed job is `fail`, a warning breach or a degraded
   stream is `warn`. Then age, oldest first. */
var SEV_RANK = { fail: 0, warn: 1 };

function needsYou(input, now) {
  var out = [];
  (input.approvals || []).forEach(function (a) {
    if (!a) return;
    var who = a.requestedByEmail || a.requestedBy || 'somebody';
    out.push({
      kind: 'approval', severity: 'fail', at: Date.parse(a.requestedAt || '') || now,
      title: 'Approval' + (a.requirement ? ' — ' + humanRequirement(a.requirement) : ''),
      text: (a.act ? humanAct(a.act) + '. ' : '') + 'Two-person rule, raised by ' + who
        + (a.requestedAt ? ', waiting ' + fmtAgo(a.requestedAt, now) : '') + '.',
      cta: 'Review', href: '/user-roles#approvals',
    });
  });
  (input.breaches || []).forEach(function (b) {
    if (!b || b.state === 'resolved' || b.state === 'suppressed') return;
    var rows = num(b.currentRowsFailed != null ? b.currentRowsFailed : b.peakRowsFailed);
    out.push({
      kind: 'breach', severity: b.severity === 'critical' ? 'fail' : 'warn', at: num(b.openedAt) || now,
      title: 'Assurance breach',
      text: (b.proves || b.ruleName || b.ruleId || 'A check') + ' — '
        + (rows ? fmtInt(rows) + ' ' + plural(rows, 'row') + ' below threshold' : 'open')
        + (b.table ? ' in ' + b.table : '') + '.',
      cta: 'Open', href: '/assurance#breach=' + encodeURIComponent(str(b.id)),
    });
  });
  (input.failedJobs || []).forEach(function (j) {
    if (!j) return;
    var err = str(j.lastError || j.error || j.executionError || j.errorMessage).trim();
    out.push({
      kind: 'job', severity: 'fail', at: jobRunTs(j) || now,
      title: 'Failed job',
      text: (j.name || 'A job') + (err ? ': ' + err : ' — the last run failed.'),
      cta: 'Open', href: '/dashboard#goto=jobs',
    });
  });
  (input.blockedStreams || []).forEach(function (e) {
    if (!e) return;
    var hard = e.level === 'blocked';
    var mins = e.stoppedForSeconds != null ? Math.round(e.stoppedForSeconds / 60) : null;
    out.push({
      kind: 'stream', severity: hard ? 'fail' : 'warn', at: now - (e.stoppedForSeconds || 0) * 1000,
      title: hard ? 'Stream blocked' : 'Stream degraded',
      text: (e.name || 'A stream') + (hard
        ? ' has not reached the destination' + (mins != null ? ' for ' + mins + ' ' + plural(mins, 'minute') : '')
        : ' is falling behind') + '.' + (e.consequence ? ' ' + e.consequence : ''),
      cta: 'Open', href: '/data-stream',
    });
  });
  out.sort(function (a, b) {
    var s = (SEV_RANK[a.severity] || 0) - (SEV_RANK[b.severity] || 0);
    return s !== 0 ? s : a.at - b.at;
  });
  out.forEach(function (it, i) { it.primary = i === 0; it.age = fmtAgo(it.at, now); });
  return out;
}

function humanRequirement(r) {
  var s = str(r).replace(/[-_]/g, ' ');
  if (/destructive/i.test(s)) return 'destructive change';
  return s;
}
function humanAct(act) {
  var s = str(act);
  var map = { 'schema.drop': 'Drop a schema object', 'schema.write': 'Write to the schema', 'sql.write': 'Run a write statement',
              'job.run.prod': 'Run a job against production', 'stream.start.prod': 'Start a stream against production' };
  return map[s] || s.replace(/[._]/g, ' ');
}

/* ── In flight ───────────────────────────────────────────────────────────
   A job whose execution status is `running`. Nothing on the console holds a
   running state across a reload today — runs are driven from the page that
   started them — so this is usually null, and Home says "Nothing in flight"
   rather than inventing a bar. When a runner does persist one, this is where
   it appears. */
function inFlight(jobs, now) {
  var j = (jobs || []).filter(function (x) { return bucketOf(x) === 'running'; })[0];
  if (!j) return null;
  var started = Date.parse(j.startedAt || j.lastRun || '') || null;
  var done = num(j.rowsDone), total = num(j.totalRows);
  var pct = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : null;
  var elapsedMs = started ? now - started : null;
  var eta = (pct && pct > 0 && elapsedMs) ? fmtDuration(elapsedMs * (100 - pct) / pct) : '';
  return {
    name: j.name || 'Job', meta: [j.packageName ? 'Package' : 'Job', j.jobCount ? j.jobCount + ' jobs' : null,
      started ? 'started ' + fmtWhen(started, now).replace(/^Today /, '') : null].filter(Boolean).join(' · '),
    pct: pct, elapsed: elapsedMs != null ? fmtDuration(elapsedMs) : '', eta: eta ? '~' + eta : '',
  };
}

/* ── Measures ────────────────────────────────────────────────────────────
   Readiness is pfConfidence's score, or null (a project with no gradeable
   signal is not a zero). Objects is jobs complete over jobs. Rows today is
   the sum of totalRows on jobs whose last run started since midnight — the
   nearest thing the stored shape has to rows applied, and labelled as
   such rather than dressed up as a live counter. */
function measures(input, jobs, now) {
  var conf = input.confidence || null;
  var complete = jobs.filter(function (j) { return bucketOf(j) === 'complete'; }).length;
  var n = new Date(now);
  var midnight = new Date(n.getFullYear(), n.getMonth(), n.getDate()).getTime();
  var rows = 0, ran = 0;
  jobs.forEach(function (j) {
    var t = jobRunTs(j);
    if (t != null && t >= midnight) { rows += num(j.totalRows); ran++; }
  });
  var compact = fmtCompact(rows);
  return {
    readiness: conf && conf.score != null
      ? { value: String(conf.score), unit: '%', grade: conf.grade || null, pending: false }
      : { value: '—', unit: '', grade: null, pending: !conf },
    objects: { value: String(complete), unit: '/' + jobs.length, done: complete, total: jobs.length },
    rowsToday: { value: ran ? compact.value : '0', unit: ran ? compact.unit : '', note: ran
      ? 'from ' + ran + ' ' + plural(ran, 'run') + ' since midnight' : 'no runs since midnight' },
  };
}

/* ── Recent runs ─────────────────────────────────────────────────────────
   The last five jobs that ran, newest first. Duration only when the stored
   record has both ends; a guessed duration is worse than a dash. */
function recentRuns(jobs, now, limit) {
  return jobs.map(function (j) {
    var t = jobRunTs(j);
    if (t == null) return null;
    var started = Date.parse(j.startedAt || '') || null;
    var finished = Date.parse(j.completedAt || j.lastRun || '') || null;
    var b = bucketOf(j);
    return {
      id: j.id, name: j.name || (j.source && j.target ? j.source + ' → ' + j.target : 'Job'),
      at: t, started: fmtWhen(t, now),
      duration: (started && finished && finished >= started) ? fmtDuration(finished - started) : '—',
      rows: j.totalRows != null ? fmtInt(j.totalRows) : '—',
      result: RESULT_WORD[b] || 'Pending', tone: RESULT_TONE[b] || 'muted',
    };
  }).filter(Boolean).sort(function (a, b) { return b.at - a.at; }).slice(0, limit || 5);
}

/* ── Next scheduled ──────────────────────────────────────────────────────
   Enabled schedules with a next run, soonest first, three of them. A paused
   schedule's stale nextRunAt is not a run that is going to happen. */
function nextScheduled(schedules, now, limit) {
  return (schedules || []).filter(function (s) { return s && s.enabled && s.nextRunAt && isFinite(Date.parse(s.nextRunAt)); })
    .sort(function (a, b) { return Date.parse(a.nextRunAt) - Date.parse(b.nextRunAt); })
    .slice(0, limit || 3)
    .map(function (s) { return { id: s.id, name: s.name || '(unnamed)', when: fmtWhen(s.nextRunAt, now) }; });
}

/* ── The empty state ─────────────────────────────────────────────────────
   Three numbered steps, the first live and the rest dimmed until it is
   done. A new account used to land on "No projects yet" in 12px grey mono
   (finding 06). The console's whole promise is a safe migration; the empty
   state is the pitch. */
function emptySteps(input) {
  var c = input.connections || {};
  var src = !!c.source, tgt = !!c.target;
  return [
    { n: 1, title: 'Connect a source', text: 'Point Cygenix at the database you are moving from. Read-only until you say otherwise.',
      cta: src ? 'Source connected' : 'Connect source', href: '/dashboard#goto=connections', live: true, done: src },
    { n: 2, title: 'Connect a target', text: 'Add the database you are moving to. Every page — mapping, jobs, assurance — inherits both.',
      cta: tgt ? 'Target connected' : 'Connect target', href: '/dashboard#goto=connections', live: src, done: tgt },
    { n: 3, title: 'Create a project', text: 'A project scopes the jobs, the plan and the evidence. Readiness is scored from the moment it exists.',
      cta: 'Create project', href: '/projects?new=1', live: src && tgt, done: false },
  ];
}

/* ── The model ───────────────────────────────────────────────────────────
   input: { now, project, projects, jobs, connections:{source,target},
            pipeline, confidence, aiNarrative, approvals, breaches, failedJobs,
            blockedStreams, schedules, region, prodCount, approvalsPending }
   `jobs` are already scoped to the project and already exclude the trash —
   scoping is the caller's business, so which scope produced a number is
   never hidden in here. */
function homeModel(input) {
  input = input || {};
  var now = input.now || Date.now();
  var project = input.project || null;
  var jobs = Array.isArray(input.jobs) ? input.jobs.filter(Boolean) : [];
  var projects = Array.isArray(input.projects) ? input.projects : [];

  if (!project) {
    return {
      empty: true, steps: emptySteps(input),
      title: 'Start a migration', kicker: 'Cygenix' + (input.region ? ' · ' + input.region : ''),
      sentence: 'Connect a source and a target, then create a project. Home fills in from there.',
      footer: footer(input, projects),
    };
  }

  var flight = inFlight(jobs, now);
  var failed = input.failedJobs || jobs.filter(function (j) { return bucketOf(j) === 'failed'; });
  var needs = needsYou({ approvals: input.approvals, breaches: input.breaches, failedJobs: failed,
                         blockedStreams: input.blockedStreams }, now);
  var kicker = ['Migration', [project.srcSystem, project.tgtSystem].filter(Boolean).join(' → ') || null, input.region || null]
    .filter(Boolean).join(' · ');

  return {
    empty: false,
    kicker: kicker,
    title: project.name || 'Untitled project',
    sentence: stateSentence(input, input.pipeline, flight, now),
    plan: planRows(input.pipeline, project, now),
    measures: measures(input, jobs, now),
    runs: recentRuns(jobs, now, 5),
    needs: needs,
    needsPending: !!input.approvalsPending,
    inFlight: flight,
    next: nextScheduled(input.schedules, now, 3),
    nextPending: input.schedules == null,
    footer: footer(input, projects),
  };
}

function footer(input, projects) {
  var c = input.connections || {};
  var conns = (c.source ? 1 : 0) + (c.target ? 1 : 0);
  var prod = num(input.prodCount);
  return {
    projects: projects.length, connections: conns, prod: prod,
    audit: input.auditVerifiedAt ? 'Audit chain verified ' + fmtWhen(input.auditVerifiedAt, input.now || Date.now()).replace(/^Today /, '')
         : 'Audit chain not verified this session',
    line: projects.length + ' ' + plural(projects.length, 'project') + ' · ' + conns + ' ' + plural(conns, 'connection')
      + (prod ? ' (' + prod + ' PROD)' : ''),
  };
}

return {
  homeModel: homeModel,
  planRows: planRows, needsYou: needsYou, measures: measures, recentRuns: recentRuns,
  nextScheduled: nextScheduled, inFlight: inFlight, emptySteps: emptySteps, stateSentence: stateSentence,
  fmtInt: fmtInt, fmtCompact: fmtCompact, fmtWhen: fmtWhen, fmtDuration: fmtDuration, fmtAgo: fmtAgo,
  bucketOf: bucketOf, RESULT_WORD: RESULT_WORD,
};
});
