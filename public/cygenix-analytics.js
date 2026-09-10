/* ============================================================================
   cygenix-analytics.js — every number the Analytics page shows, derived once.
   ----------------------------------------------------------------------------
   WHAT WENT WRONG
   Home grew into a screen that was half operations and half analysis, and the
   two halves disagreed. `TOTAL JOBS` and the job-status ring were the same
   expression — `liveJobs().length` — rendered 400px apart, so they could never
   differ but cost the reader two glances to confirm that. Worse, the Analysed
   and SQL Generated tiles counted jobs across ALL projects while the pipeline
   strip counted the same jobs scoped to the ACTIVE project. Those two really
   can differ, they were labelled as though they could not, and the failure
   mode was not a crash: it was two true numbers that looked like one fact.

   So the analytical material moved to its own screen, and the arithmetic
   behind it moved here — one module, one definition of each figure, consumed
   by both /analytics and what is left on Home.

   WHY IT OWNS NO DOM, NO FETCH AND NO STORAGE
   Same reason as cygenix-pipeline.js, which this sits beside: the judgement is
   in the derivation — what counts as converted, when a project is at risk,
   whether a stream is failing or merely paused — and that is exactly the part
   that has to be provable against fixtures without a browser. The page reads
   localStorage and hands the shapes in; the renderers in analytics-app.js
   consume view models and know nothing about jobs.

   PAUSED IS NOT FAILED
   The one rule that runs through every model here. A stream a person paused on
   purpose is not an incident, and must never reach a failure count, a health
   score or a risk pill. The Data Stream page has warned about this for a while
   ("individually paused streams will not resume globally"); the arithmetic now
   agrees with the warning instead of quietly contradicting it.

   NO DATA IS A VALUE
   Nothing here invents a figure to fill a slot. A model with nothing to say
   returns `null` for the number and a reason string saying which input was
   missing, and the renderer prints the reason. Zero and unknown are different
   answers to different questions, and a dashboard that confuses them teaches
   people to distrust all of it.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object') root.CygenixAnalytics = api;
})(typeof window !== 'undefined' ? window : this, function () {
'use strict';

var DAY = 86400000;
var WEEK = 7 * DAY;

/* ══ Job predicates ════════════════════════════════════════════════════════
   Moved verbatim from dashboard-app.js so the ring, the tiles, the pipeline
   and the portfolio table cannot classify one job three ways. */

/* Soft-deleted jobs are in the trash and restorable, so they must not appear
   in any total — the number on screen has to match the All Jobs table. */
function liveJobs(list) {
  return (Array.isArray(list) ? list : []).filter(function (j) { return j && !j._deleted; });
}

/* A job's lifecycle bucket. executionStatus is written by a project run and
   reflects what actually happened, so it wins over the status the job was
   saved with. The extra spellings are accepted because project bundles
   imported through cygenix-integrations carry whatever they were exported
   with. */
function jobBucket(j) {
  var s = String((j && (j.executionStatus || j.status)) || '').toLowerCase();
  if (s === 'complete' || s === 'completed' || s === 'success') return 'complete';
  if (s === 'ready' || s === 'sql_ready' || s === 'sql-ready') return 'ready';
  if (s === 'failed' || s === 'fail' || s === 'error') return 'failed';
  return 'pending';
}

/* A job counts as analysed once it has at least one mapped target column. */
function jobIsMapped(j) {
  return Array.isArray(j && j.columnMapping) && j.columnMapping.some(function (m) {
    return m && m.tgtCol;
  });
}

/* Automation rate asks how much conversion happened without a person editing
   it. The system generator only ever produces direct copies, so any non-NONE
   transform is evidence of the AI path. An explicit flag wins when present —
   the heuristic is a fallback for jobs written before the flag existed. */
function isAiMapped(j) {
  if (!j) return false;
  if (j.aiMapped === true) return true;
  if (j.mappingSource && /ai|claude|agent/i.test(String(j.mappingSource))) return true;
  var cm = Array.isArray(j.columnMapping) ? j.columnMapping : [];
  if (!cm.length) return false;
  return cm.some(function (m) {
    return m && m.transform && String(m.transform).toUpperCase() !== 'NONE';
  });
}

/* When a job last actually ran. Three spellings because three code paths write
   it; `created` is deliberately NOT a fallback — a job that was created and
   never run has no completion date, and treating creation as completion is how
   a weekly throughput chart invents work nobody did. */
function ranAt(j) {
  var t = j && (j.lastRun || j.completedAt || j.executedAt);
  var ms = t ? Date.parse(t) : NaN;
  return isNaN(ms) ? null : ms;
}

/* ══ Job status ring ═══════════════════════════════════════════════════════ */

var RING_SEGMENTS = [
  { key: 'complete', label: 'Complete',  color: 'var(--green)' },
  { key: 'ready',    label: 'SQL Ready', color: 'var(--accent)' },
  { key: 'failed',   label: 'Failed',    color: 'var(--red)' },
  { key: 'pending',  label: 'Pending',   color: 'var(--purple)' },
];

function ringModel(jobs) {
  var live = liveJobs(jobs);
  var buckets = { complete: 0, ready: 0, failed: 0, pending: 0 };
  live.forEach(function (j) { buckets[jobBucket(j)]++; });
  var total = live.length;
  return {
    total: total,
    buckets: buckets,
    /* completePct is null, not 0, when there is nothing to divide by. "0%
       complete" and "no jobs yet" are different sentences. */
    completePct: total ? Math.round(buckets.complete / total * 100) : null,
    segments: RING_SEGMENTS.map(function (s) {
      return {
        key: s.key, label: s.label, color: s.color,
        count: buckets[s.key],
        pct: total ? Math.round(buckets[s.key] / total * 100) : 0,
      };
    }),
  };
}

/* ══ Headline tiles ════════════════════════════════════════════════════════ */

function kpiModel(jobs, filesProcessed) {
  var live = liveJobs(jobs);
  return {
    total: live.length,
    analysed: live.filter(jobIsMapped).length,
    sqlGenerated: live.filter(function (j) {
      var b = jobBucket(j);
      return b === 'ready' || b === 'complete';
    }).length,
    files: Number(filesProcessed) || 0,
  };
}

/* ══ Delivery ══════════════════════════════════════════════════════════════
   Reads a CygenixDataStream state object. The split that matters is between
   streams that are trying and failing and streams somebody stopped: `failed`
   counts the first, `pausedByUser` the second, and nothing adds them up. */

var LIVE_STATUS = { running: 1, lagging: 1 };

function deliveryModel(state) {
  var streams = (state && state.streams) || [];
  var eligible = streams.filter(function (s) { return s.status !== 'draft'; });
  if (!eligible.length) {
    return { hasData: false, reason: 'No streams configured for this project.',
             streams: [], running: 0, total: 0, pausedByUser: 0, failed: 0,
             applyRate: null, worstLagSeconds: null, queueDepth: 0, dlqDepth: 0 };
  }

  var live = eligible.filter(function (s) { return LIVE_STATUS[s.status]; });
  var paused = eligible.filter(function (s) { return s.status === 'paused'; });
  var failed = eligible.filter(function (s) { return s.status === 'failed'; });

  var worst = null;
  live.concat(failed).forEach(function (s) {
    var lag = (s.metrics && s.metrics.lagSeconds) || 0;
    if (!worst || lag > worst.lag) worst = { lag: lag, name: s.name };
  });

  var rows = eligible.map(function (s) {
    var m = s.metrics || {};
    var thr = (s.delivery && s.delivery.lagThresholdSeconds) || 30;
    var isPaused = s.status === 'paused';
    return {
      id: s.id,
      name: s.name,
      status: s.status,
      /* A paused stream reports no lag rather than a stale one: the number it
         last held is minutes or days old and reads as a live measurement. */
      lagSeconds: isPaused || s.status === 'draft' ? null : (m.lagSeconds || 0),
      overSlo: !isPaused && (m.lagSeconds || 0) > thr,
      sloSeconds: thr,
      rowsPerMin: isPaused ? 0 : (m.deliveredPerMin || 0),
      queued: m.pendingInStore || 0,
      dlq: m.dlqDepth || 0,
      spark: Array.isArray(m.spark) ? m.spark.slice() : [],
      pausedByUser: isPaused,
      failing: s.status === 'failed',
    };
  });

  return {
    hasData: true,
    streams: rows,
    running: live.length,
    total: eligible.length,
    pausedByUser: paused.length,
    failed: failed.length,
    applyRate: live.reduce(function (a, s) { return a + ((s.metrics || {}).deliveredPerMin || 0); }, 0),
    worstLagSeconds: worst ? worst.lag : null,
    worstStream: worst ? worst.name : null,
    queueDepth: eligible.reduce(function (a, s) { return a + ((s.metrics || {}).pendingInStore || 0); }, 0),
    dlqDepth: eligible.reduce(function (a, s) { return a + ((s.metrics || {}).dlqDepth || 0); }, 0),
  };
}

/* ══ Quality ═══════════════════════════════════════════════════════════════
   This tab was drawn against source-vs-target row parity, which nothing in
   this system measures — no code anywhere compares a source row count to a
   target row count or a checksum. Rather than six tiles reading "—", it is
   built on what Assurance really records: rule runs, their verdicts and the
   breaches they opened. That answers "are my validation rules passing", which
   is a real question, and the page says plainly that it is not the same
   question as "does the target match the source".

   `health` and `series` are computed by cygenix-assurance.js and passed in, so
   the pass-rate arithmetic has exactly one home. */

function qualityModel(input) {
  input = input || {};
  var health = input.health;
  var series = input.series || [];
  var store = input.store;

  if (!health || !health.runs) {
    return { hasData: false,
             reason: 'No validation runs yet. Create a rule in Data Quality and run it to populate this.',
             heatmap: [], categories: [] };
  }

  var categories = Object.keys(health.perCategory || {}).map(function (k) {
    return { key: k, score: health.perCategory[k] };
  }).sort(function (a, b) { return (a.score === null) - (b.score === null) || a.score - b.score; });

  return {
    hasData: true,
    passRate: health.score,
    runs: health.runs,
    pass: health.pass,
    warn: health.warn,
    fail: health.fail,
    activeRules: health.activeRules,
    openBreaches: health.openBreaches,
    criticalOpen: health.criticalOpen,
    streakDays: health.streakDays,
    meanTimeToFixMs: health.meanTimeToFixMs,
    series: series,
    categories: categories,
    heatmap: heatmapCells(store, input.days || 14, input.now || Date.now()),
  };
}

/* One cell per (rule, day): how many of that rule's runs failed that day.
   Derived from the run log rather than stored, so it costs nothing to keep and
   cannot drift from the runs it describes. */
function heatmapCells(store, days, now) {
  var runs = (store && store.runs) || [];
  if (!runs.length) return [];
  var byRule = {};
  runs.forEach(function (r) {
    var age = now - r.startedAt;
    if (age < 0 || age > days * DAY) return;
    var day = Math.floor(age / DAY);            // 0 = today
    var row = byRule[r.ruleId] || (byRule[r.ruleId] = {});
    var cell = row[day] || (row[day] = { runs: 0, fail: 0, warn: 0 });
    cell.runs++;
    if (r.status === 'fail') cell.fail++;
    else if (r.status === 'warn') cell.warn++;
  });
  return Object.keys(byRule).map(function (ruleId) {
    var row = byRule[ruleId];
    return {
      ruleId: ruleId,
      days: Array.from({ length: days }, function (_, i) {
        var d = row[days - 1 - i];
        return d ? { runs: d.runs, fail: d.fail, warn: d.warn,
                     intensity: d.runs ? (d.fail + 0.5 * d.warn) / d.runs : 0 }
                 : null;                        // null = never ran, not "clean"
      }),
    };
  });
}

/* ══ Weekly throughput ═════════════════════════════════════════════════════
   Eight buckets of completed jobs, oldest first, derived from run timestamps.
   There is no stored history to read, and this is the honest substitute: it is
   exact for jobs that carry a run date and silently omits ones that do not,
   which is why `dated` is reported alongside so the page can say so. */

function weeklySeries(jobs, weeks, now) {
  weeks = weeks || 8;
  now = now || Date.now();
  var live = liveJobs(jobs);
  var counts = new Array(weeks).fill(0);
  var auto = new Array(weeks).fill(0);
  var dated = 0;

  live.forEach(function (j) {
    if (jobBucket(j) !== 'complete') return;
    var t = ranAt(j);
    if (t === null) return;
    dated++;
    var idx = weeks - 1 - Math.floor((now - t) / WEEK);
    if (idx < 0 || idx >= weeks) return;
    counts[idx]++;
    if (isAiMapped(j)) auto[idx]++;
  });

  return {
    weeks: weeks, counts: counts, automatic: auto, dated: dated,
    total: counts.reduce(function (a, b) { return a + b; }, 0),
    /* Undated completions are a caveat the chart has to carry, not a rounding
       error to hide: a project imported from a bundle can have every job
       complete and no run dates at all. */
    undated: live.filter(function (j) {
      return jobBucket(j) === 'complete' && ranAt(j) === null;
    }).length,
  };
}

/* ══ Portfolio ═════════════════════════════════════════════════════════════ */

/* Risk is computed, never typed. Four triggers, and the pill reports which
   ones fired so the tooltip can say why rather than asserting a colour. */
var RISK_REASON = {
  readiness:  'Cutover readiness is below 70',
  window:     'Cutover window is inside 14 days',
  breaches:   'Unresolved validation breaches are open',
  stalled:    'No progress in the last 7 days',
};

function riskFor(project, ctx) {
  ctx = ctx || {};
  var now = ctx.now || Date.now();
  var fired = [];

  if (typeof ctx.readiness === 'number' && ctx.readiness < 70) fired.push('readiness');

  var end = project && (project.end || project.cutoverAt);
  var days = null;
  if (end) {
    var ms = Date.parse(end);
    if (!isNaN(ms)) {
      days = Math.ceil((ms - now) / DAY);
      if (days <= 14) fired.push('window');
    }
  }

  if (Number(ctx.openBreaches) > 0) fired.push('breaches');

  /* "Stalled" only means something once there is work to stall. A project with
     no jobs at all is not stalled, it has not started, and calling that a risk
     would put a red pill on every draft. */
  if (ctx.jobCount > 0 && ctx.lastActivityAt && (now - ctx.lastActivityAt) > WEEK) {
    fired.push('stalled');
  }

  var level = fired.length >= 2 ? 'high' : fired.length === 1 ? 'medium' : 'low';
  return {
    level: level,
    label: level === 'high' ? 'At risk' : level === 'medium' ? 'Watch' : 'On track',
    fired: fired,
    daysToCutover: days,
    /* Never a bare colour: the pill has a word and this sentence behind it. */
    why: fired.length
      ? fired.map(function (k) { return RISK_REASON[k]; }).join(' · ')
      : 'None of the four risk triggers are firing.',
  };
}

function portfolioModel(input) {
  input = input || {};
  var now = input.now || Date.now();
  var projects = input.projects || [];
  var all = liveJobs(input.jobs);
  var inv = input.inventory || [];

  var rows = projects.map(function (p) {
    var jobs = all.filter(function (j) { return j.projectId === p.id; });
    var ring = ringModel(jobs);
    var lastActivity = jobs.reduce(function (m, j) {
      var t = ranAt(j) || Date.parse(j.lastModified || j.created || 0) || 0;
      return t > m ? t : m;
    }, 0);
    return {
      id: p.id,
      name: p.name || 'Untitled project',
      client: p.client || '',
      objects: jobs.length,
      artifacts: inv.filter(function (a) { return a.projectId === p.id; }).length,
      percentComplete: ring.completePct,
      series: weeklySeries(jobs, 8, now).counts,
      risk: riskFor(p, {
        now: now,
        readiness: (input.readinessById || {})[p.id],
        openBreaches: (input.breachesById || {})[p.id] || 0,
        jobCount: jobs.length,
        lastActivityAt: lastActivity || null,
      }),
    };
  });

  var mapped = all.filter(jobIsMapped);
  return {
    projects: rows,
    projectCount: projects.length,
    objectsConverted: all.filter(function (j) { return jobBucket(j) === 'complete'; }).length,
    /* Automation rate over MAPPED jobs, not all jobs: a job with no mapping
       was not converted by anything, and counting it as "not automatic" would
       make the rate fall every time somebody creates a job. */
    automationRate: mapped.length ? Math.round(mapped.filter(isAiMapped).length / mapped.length * 100) : null,
    automationBasis: mapped.length,
    atRisk: rows.filter(function (r) { return r.risk.level === 'high'; }).length,
  };
}

return {
  liveJobs: liveJobs,
  jobBucket: jobBucket,
  jobIsMapped: jobIsMapped,
  isAiMapped: isAiMapped,
  ranAt: ranAt,
  ringModel: ringModel,
  RING_SEGMENTS: RING_SEGMENTS,
  kpiModel: kpiModel,
  deliveryModel: deliveryModel,
  qualityModel: qualityModel,
  heatmapCells: heatmapCells,
  weeklySeries: weeklySeries,
  riskFor: riskFor,
  RISK_REASON: RISK_REASON,
  portfolioModel: portfolioModel,
};
});
