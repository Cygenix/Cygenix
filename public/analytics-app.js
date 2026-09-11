/* ============================================================================
   analytics-app.js — the Analytics page, rendered.
   ----------------------------------------------------------------------------
   This file reads storage, turns it into the shapes cygenix-analytics.js
   expects, and builds HTML strings. It does no arithmetic of its own. If you
   find yourself computing a percentage in here, it belongs in the module —
   that separation is what lets the numbers be tested without a browser, and it
   is why Home and this page cannot end up disagreeing about readiness.

   WHAT THIS PAGE WILL NOT DO
   Invent a figure to fill a slot. Several widgets in the original design need
   a continuous metrics store, and this system has none: stream telemetry lives
   in memory for the life of the tab, and nothing anywhere compares a source
   row count to a target row count. Those panels render a sentence saying what
   is missing and what would populate it. A dashboard that fabricates history
   teaches people to distrust the parts that are real.

   ONE READ PER PAINT
   Every tab is rendered from a single snapshot taken at the top of paint(), so
   two panels on the same screen cannot be showing storage from two different
   moments. The 30s poll re-takes the snapshot; it does not refresh panels
   independently.
   ========================================================================== */
(function () {
'use strict';

var A  = window.CygenixAnalytics;
var DS = window.CygenixDataStream;
var U  = window.CygenixDataStreamUI;
var PF = window.CygenixPreflight;
var PL = window.CygenixPipeline;
var AS = window.CygenixAssurance;

var TABS = ['delivery', 'quality', 'portfolio'];
var POLL_MS = 30000;
var AS_KEY = 'cygenix_assurance_v1';

var activeTab = 'delivery';
var rangeDays = 14;
var scope = 'active';
var timer = null;
var snapshot = null;

/* ── Small helpers ──────────────────────────────────────────────────────── */

function esc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}
function el(id) { return document.getElementById(id); }
function num(n) { return Number(n || 0).toLocaleString('en-GB'); }
function readLS(key, fallback) {
  try {
    var raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : fallback;
  } catch (_) { return fallback; }
}

/* Duration in the shortest form that is still exact enough to act on. */
function dur(sec) {
  if (sec === null || sec === undefined) return '—';
  if (sec < 60) return Math.round(sec) + 's';
  var m = Math.floor(sec / 60), s = Math.round(sec % 60);
  if (m < 60) return m + 'm ' + (s < 10 ? '0' : '') + s + 's';
  return Math.floor(m / 60) + 'h ' + (m % 60) + 'm';
}

/* A sparkline is a shape, and a shape is not readable. Every one gets a text
   alternative naming the range and direction beside it, so the figure survives
   with the picture turned off. */
function spark(values, label) {
  var vals = (values || []).filter(function (v) { return typeof v === 'number'; });
  if (vals.length < 2) {
    return '<span class="an-kpi-sub">' + esc(label || 'Not enough history to plot yet') + '</span>';
  }
  var first = vals[0], last = vals[vals.length - 1];
  var dir = last > first ? 'rising' : last < first ? 'falling' : 'flat';
  var text = (label || 'Trend') + ': ' + dir + ', from ' + num(first) + ' to ' + num(last)
           + ' over ' + vals.length + ' points';
  return '<span class="an-spark" role="img" aria-label="' + esc(text) + '">'
       + U.sparkline(vals, { width: 74, height: 18 }) + '</span>';
}

function kpi(o) {
  var cls = 'an-kpi' + (o.tone === 'crit' ? ' an-kpi-alert' : o.tone === 'warn' ? ' an-kpi-warn' : '');
  return '<div class="' + cls + '">'
    + '<div class="an-kpi-label">' + esc(o.label) + '</div>'
    + '<div class="an-kpi-val">' + (o.value === null || o.value === undefined ? '—' : esc(o.value))
    + (o.unit ? '<em>' + esc(o.unit) + '</em>' : '') + '</div>'
    + '<div class="an-kpi-sub">' + (o.subHtml || esc(o.sub || '')) + '</div>'
    + '</div>';
}

function panel(o) {
  return '<div class="panel">'
    + '<div class="panel-head"><div><div class="panel-title">' + esc(o.title) + '</div>'
    + (o.sub ? '<div class="panel-sub">' + esc(o.sub) + '</div>' : '') + '</div>'
    + (o.aside || '') + '</div>'
    + '<div class="panel-body">' + o.body + '</div>'
    + (o.foot ? '<div class="panel-foot">' + o.foot + '</div>' : '')
    + '</div>';
}

/* The empty state this page uses everywhere. `why` says what is missing, `how`
   links to the screen that would fill it. Both are required — "no data" on its
   own is the message that makes a dashboard look broken. */
function empty(title, why, how) {
  return '<div class="empty-state"><h3>' + esc(title) + '</h3><p>' + esc(why)
    + (how ? ' ' + how : '') + '</p></div>';
}

function pill(tone, text) {
  return '<span class="an-pill an-pill-' + tone + '">' + esc(text) + '</span>';
}

/* ── The snapshot ───────────────────────────────────────────────────────────
   One read of storage per paint. Scope is honoured the way Home honours it:
   'active' means the project in cygenix_active_project_id, and when there is
   none the page says so rather than silently showing everything. */

function takeSnapshot() {
  var now = Date.now();
  var activeId = localStorage.getItem('cygenix_active_project_id') || '';
  var projects = readLS('cygenix_projects', []) || [];
  var project = projects.find(function (p) { return p.id === activeId; }) || null;
  var allJobs = A.liveJobs(readLS('cygenix_jobs', []) || []);
  var jobs = (scope === 'active' && project)
    ? allJobs.filter(function (j) { return j.projectId === project.id; })
    : allJobs;

  var streams = null;
  try { streams = DS.load(activeId || 'default'); } catch (_) {}

  var preflight = null;
  try { preflight = PF.pfLoad(activeId || 'default'); } catch (_) {}
  var confidence = null;
  try { confidence = PF.pfConfidence({ jobs: jobs, preflight: preflight, now: now }); } catch (_) {}

  var asStore = readLS(AS_KEY, null);
  var health = null, series = [];
  if (asStore && Array.isArray(asStore.runs)) {
    try {
      health = AS.asHealth(asStore, rangeDays * 86400000, now);
      series = AS.asHealthSeries(asStore, Math.min(rangeDays, 30), now);
    } catch (_) {}
  }

  var conns = {};
  try {
    var c = window.CygenixConnections ? window.CygenixConnections.get() : {};
    conns = { source: c.srcConnString || c.srcFnUrl, target: c.tgtConnString || c.tgtFnUrl };
  } catch (_) {}

  return {
    now: now, activeId: activeId, project: project, projects: projects,
    jobs: jobs, allJobs: allJobs,
    inventory: readLS('cygenix_inventory', []) || [],
    confidence: confidence,
    delivery: A.deliveryModel(streams),
    ring: A.ringModel(jobs),
    kpis: A.kpiModel(jobs, 0),
    quality: A.qualityModel({ health: health, series: series, store: asStore,
                              days: Math.min(rangeDays, 30), now: now }),
    weekly: A.weeklySeries(jobs, 8, now),
    portfolio: A.portfolioModel({
      projects: projects, jobs: allJobs, inventory: readLS('cygenix_inventory', []) || [],
      readinessById: readinessByProject(projects, allJobs, now), now: now,
    }),
    pipeline: safePipeline(project, jobs, conns, preflight, confidence, now),
    streamState: streams,
  };
}

/* Readiness per project, using the SAME calculation the tile shows. Computed
   here rather than in the pure module because pfConfidence lives in preflight
   and the module must stay free of cross-module imports. */
function readinessByProject(projects, allJobs, now) {
  var out = {};
  projects.forEach(function (p) {
    try {
      var jobs = allJobs.filter(function (j) { return j.projectId === p.id; });
      var c = PF.pfConfidence({ jobs: jobs, preflight: PF.pfLoad(p.id), now: now });
      if (c && c.score !== null) out[p.id] = c.score;
    } catch (_) {}
  });
  return out;
}

function safePipeline(project, jobs, conns, preflight, confidence, now) {
  if (!project) return null;
  try {
    var rules = (readLS('cygenix_wasis_rules', []) || [])
      .filter(function (r) { return !r.projectId || r.projectId === project.id; });
    var inspected = jobs.filter(function (j) {
      return Array.isArray(j.tables) && j.tables.some(function (t) { return Number(t.rows) > 0; });
    }).length;
    return PL.toPipelineModel({
      project: project, jobs: jobs, connections: conns,
      quality: { rulesCount: rules.length, inspectedJobs: inspected },
      confidence: confidence, now: now,
    });
  } catch (_) { return null; }
}

/* ── Delivery tab ───────────────────────────────────────────────────────── */

function renderDelivery(s) {
  var d = s.delivery;
  var c = s.confidence;
  var out = '';

  /* The blocked band, from the same builder the Data Stream page and Home use,
     so one incident cannot be described three ways. */
  if (s.streamState && U && U.blockedBand) {
    try { out += U.blockedBand(DS, s.streamState, { actions: false }); } catch (_) {}
  }

  out += '<div class="an-kpis">'
    + kpi({ label: 'Cutover readiness',
            value: c && c.score !== null ? c.score : null, unit: c && c.score !== null ? '/100' : '',
            tone: c && c.grade === 'red' ? 'crit' : c && c.grade === 'amber' ? 'warn' : '',
            subHtml: c && c.score !== null
              ? pill(c.grade === 'green' ? 'good' : c.grade === 'amber' ? 'warn' : 'crit',
                     c.grade === 'green' ? 'Ready' : c.grade === 'amber' ? 'Caution' : 'Not ready')
              : 'No signals yet — needs a mapping, a preflight or a job' })
    + kpi({ label: 'Apply rate', value: d.hasData ? num(d.applyRate) : null, unit: 'rows/min',
            tone: d.hasData && d.applyRate === 0 && d.queueDepth > 0 ? 'crit' : '',
            sub: d.hasData ? d.running + ' of ' + d.total + ' streams delivering' : 'No streams configured' })
    + kpi({ label: 'Queued changes', value: d.hasData ? num(d.queueDepth) : null,
            tone: d.hasData && d.queueDepth > 0 ? 'warn' : '',
            sub: d.dlqDepth ? num(d.dlqDepth) + ' in the dead-letter queue' : 'Nothing dead-lettered' })
    + kpi({ label: 'Replication lag', value: d.hasData ? dur(d.worstLagSeconds) : null,
            sub: d.worstStream ? 'Worst: ' + d.worstStream : 'No live stream to measure' })
    + kpi({ label: 'Streams running', value: d.hasData ? d.running : null,
            unit: d.hasData ? 'of ' + d.total : '',
            /* Paused and failed are reported as separate sentences on purpose:
               a stream somebody stopped is not an incident. */
            sub: d.hasData
              ? [d.pausedByUser ? d.pausedByUser + ' paused by a person' : '',
                 d.failed ? d.failed + ' failing' : ''].filter(Boolean).join(' · ')
                || 'Every stream that should run is running'
              : 'None configured' })
    + kpi({ label: 'Projected parity', value: null,
            sub: 'Needs apply-rate history, which is not retained yet' })
    + '</div>';

  /* Readiness breakdown — confidenceDrivers is the same function the Home tile
     used, so the two can never fall out of step. */
  var drivers = [];
  try { drivers = c ? PL.confidenceDrivers(c) : []; } catch (_) {}
  /* The breakdown, moved from Home's confidence tile with its arithmetic
     intact. pfConfidence is a WEIGHTED AVERAGE, not a hundred points with
     deductions, so what is shown is each component's weighted contribution —
     those really do sum to the score — beside what it is costing against a
     perfect one. The sum line is the point: a composite number nobody can take
     apart is a number nobody can act on. */
  var readinessBody = drivers.length
    ? drivers.map(function (dr) {
        var of = dr.points + dr.lost;
        var pct = of ? Math.round(dr.points / of * 100) : 0;
        var col = pct >= 90 ? 'var(--green)' : pct >= 60 ? 'var(--amber)' : 'var(--red)';
        return '<div class="mp-conf-row">'
          + '<span>'
          /* The component's own health as a dot, so a row that is costing
             points is findable at a glance without reading every number. It is
             never the only signal: the points are right there beside it. */
          + '<span aria-hidden="true" style="display:inline-block;width:7px;height:7px;border-radius:50%;'
          + 'margin-right:7px;background:' + (dr.hasData ? col : 'var(--bg4)') + '"></span>'
          + esc(dr.label)
          + ' <span style="color:var(--text3)">\u00b7 ' + esc(dr.note) + '</span>'
          + (dr.hasData && dr.lost > 0
              ? ' <a class="mp-conf-fix" href="' + esc(dr.href) + '">Fix \u2192</a>' : '')
          + '</span>'
          + '<span class="mp-conf-pts">'
          + (dr.hasData
              ? '+' + dr.points + ' of ' + of + ' <span style="color:var(--text3)">('
                + dr.weightPct + '% weight)</span>'
              : '<span style="color:var(--text3)">not counted</span>')
          + '</span></div>';
      }).join('')
      + '<div class="mp-conf-sum">'
      + drivers.filter(function (d) { return d.hasData; }).map(function (d) { return '+' + d.points; }).join(' ')
      + ' = ' + (c && c.score !== null ? c.score : '\u2014') + '<br>'
      + 'Weighted over the components that have data \u2014 a component with none is excluded, '
      + 'never guessed at.</div>'
    : empty('No readiness signals yet',
            'The score appears once there is a mapping to grade, a preflight to read or a job to count.',
            '<a href="/object-mapping">Start mapping</a>.');

  var rejBody = empty('Rejections are not classified by reason yet',
    'Streams record the destination\'s error text but nothing buckets it into classes, so there is no '
    + 'reason breakdown to chart. The individual errors are on each stream.',
    '<a href="/data-stream">Open Data Stream</a>.');

  out += '<div class="an-cols-2">'
    + panel({ title: 'What is holding readiness' + (c && c.score !== null ? ' at ' + c.score : ''),
              sub: 'Each component is scored and weighted; the score is the weighted average.',
              body: readinessBody })
    + panel({ title: 'Rejections by reason', sub: 'Across all streams in scope', body: rejBody })
    + '</div>';

  /* Apply-rate history: real for the life of the tab, and labelled as such
     rather than presented as 24 hours it does not have. */
  var withSpark = d.hasData ? d.streams.filter(function (r) { return r.spark.length > 1; }) : [];
  var rateBody = withSpark.length
    ? withSpark.map(function (r) {
        return '<div class="an-bar-row"><div class="an-obj">' + esc(r.name) + '</div>'
          + '<div>' + spark(r.spark, r.name + ' apply rate') + '</div>'
          + '<div class="an-bar-val">' + num(r.rowsPerMin) + '</div></div>';
      }).join('')
      + '<div class="an-caveat">These are the samples taken since this tab was opened. Cygenix keeps '
      + 'no continuous metrics store, so there is no 24-hour history to compare against and no median '
      + 'to measure today against.</div>'
    : empty('No apply-rate samples yet',
            'Stream telemetry is collected while this page is open and is not retained between visits. '
            + 'Leave the tab open, or open the stream itself for its live chart.',
            '<a href="/data-stream">Open Data Stream</a>.');

  out += panel({ title: 'Apply rate', sub: 'Rows written to target per minute, sampled live', body: rateBody });

  /* Pipeline stages — moved from Home. */
  out += panel({
    title: 'Pipeline stages',
    sub: 'Objects that have cleared each stage, and the drop-off between them',
    body: s.pipeline ? pipelineHtml(s.pipeline)
      : empty('No active project',
              'The pipeline is scoped to one migration at a time.',
              '<a href="/projects">Choose a project</a>.'),
  });

  /* Streams table. Paused is its own state, never folded into failed. */
  out += panel({
    title: 'Streams', sub: 'Per-stream lag, throughput and backlog',
    aside: '<a class="btn btn-sm" href="/data-stream">Open Data Stream</a>',
    body: d.hasData ? streamsTable(d) : empty('No streams configured',
      'A stream reads from a connection this project already holds.',
      '<a href="/data-stream">Set one up</a>.'),
    foot: d.hasData ? '<span>Sampled live while this page is open</span>'
      + '<span>' + d.total + ' stream' + (d.total === 1 ? '' : 's') + ' in scope</span>' : '',
  });

  /* Job status breakdown — moved from Home. */
  out += panel({
    title: 'Job status breakdown', sub: 'Where every job in scope currently sits',
    body: s.ring.total ? ringHtml(s.ring)
      : empty('No jobs yet',
              'Once a migration job exists, this tracks how jobs move through pending, ready and complete.',
              '<a href="/dashboard#goto=new-job">Create one</a>.'),
  });

  return out;
}

/* The pipeline card, moved here from Home unchanged in substance: the same
   stage chips, the same single named bottleneck, the same narrative line. What
   is added is the drop-off between adjacent stages — the gap between two
   stages IS the queue you have to clear before cutover, and on a page about
   analysis it deserves to be a number rather than left to subtraction.

   The AI narrative is READ here and never generated: the call bills the user's
   own Anthropic key, and Analytics is read-only. Regeneration stays on the
   Project Status panel that authors it. */
var STATE_WORD = { done: '\u2713 done', active: 'active', attention: 'attention',
                   blocked: 'blocked', waiting: 'not started' };

function readAiNarrative(projectId) {
  try { return JSON.parse(localStorage.getItem('cygenix_ps_ai_' + projectId) || 'null'); }
  catch (_) { return null; }
}

function pipelineHtml(model) {
  var stageHtml = model.stages.map(function (st, i) {
    var hot = model.bottleneck === st.key;
    var chip = '<a class="mp-stage is-' + esc(st.state) + '" href="' + esc(st.href) + '"'
      + ' title="' + esc(st.reason || st.headline) + '">'
      + '<div class="mp-stage-key">' + esc(st.label) + '</div>'
      + '<div class="mp-stage-count">' + esc(st.headline) + '</div>'
      + '<div class="mp-stage-detail">' + esc(st.detail || st.reason || '') + '</div>'
      + '<span class="mp-state mp-state-' + esc(st.state) + '">' + STATE_WORD[st.state] + '</span>'
      + (hot ? '<span class="mp-bottleneck ' + (st.state === 'blocked' ? 'red' : 'amber') + '">bottleneck</span>' : '')
      + '</a>';
    if (i === model.stages.length - 1) return chip;

    var next = model.stages[i + 1];
    var hotArrow = model.bottleneck === next.key;
    /* The connector label is a transition count, not a rate — this is a
       lifecycle, not a throughput — with the drop-off named beneath it. */
    var drop = (typeof st.count === 'number' && typeof next.count === 'number')
      ? st.count - next.count : null;
    return chip
      + '<div class="mp-arrow' + (hotArrow ? ' hot' : '') + '"'
      + ' title="' + esc(drop && drop > 0 ? drop + ' did not carry through to ' + next.label
                                          : 'Nothing dropped off here') + '">'
      + '<div class="mp-arrow-label">' + (next.total ? next.count + '/' + next.total : '') + '</div>'
      + '<div class="mp-arrow-line"></div>'
      + '<div class="mp-arrow-label">' + (drop && drop > 0 ? '\u2212' + drop : '') + '</div>'
      + '</div>';
  }).join('');

  var bs = model.stages.find(function (st) { return st.key === model.bottleneck; });
  var cta = bs ? { href: bs.href, label: 'Go to ' + bs.label + ' \u2192' }
               : { href: '/dashboard#goto=all-jobs', label: 'Open all jobs \u2192' };
  var ai = model.project ? readAiNarrative(model.project.id) : null;

  return '<div class="mp-row" role="list">' + stageHtml + '</div>'
    + '<div class="mp-narrative"><div>'
    + '<div class="mp-narrative-text" id="mp-narrative-text">' + esc(ai ? ai.text : model.narrative) + '</div>'
    + (ai ? '<div class="mp-narrative-src">Written by ' + esc(ai.model || 'Claude') + ' \u00b7 '
            + esc(new Date(ai.ts).toLocaleString('en-GB')) + '</div>' : '')
    + '</div><a class="mp-cta" href="' + esc(cta.href) + '">' + esc(cta.label) + '</a></div>';
}

function streamsTable(d) {
  return '<div class="an-tblwrap"><table class="an-tbl"><thead><tr>'
    + '<th>Stream</th><th class="num">Lag</th><th class="num">Rows/min</th>'
    + '<th class="num">Queued</th><th class="num">Dead-letter</th><th>State</th>'
    + '</tr></thead><tbody>'
    + d.streams.map(function (r) {
        var tone = r.failing ? 'crit' : r.pausedByUser ? 'mute' : r.overSlo ? 'warn' : 'good';
        var word = r.failing ? 'Blocked' : r.pausedByUser ? 'Paused by user'
          : r.overSlo ? 'Over SLO' : 'Running';
        return '<tr><td class="an-obj"><a href="/data-stream">' + esc(r.name) + '</a></td>'
          + '<td class="num">' + (r.lagSeconds === null ? '—' : dur(r.lagSeconds)) + '</td>'
          + '<td class="num">' + num(r.rowsPerMin) + '</td>'
          + '<td class="num">' + num(r.queued) + '</td>'
          + '<td class="num">' + num(r.dlq) + '</td>'
          + '<td>' + pill(tone, word) + '</td></tr>';
      }).join('')
    + '</tbody></table>'
    + '<div class="an-caveat">A stream someone paused is listed as paused, never as failed, and its '
    + 'lag reads as unknown rather than showing the stale figure it stopped on.</div></div>';
}

function ringHtml(ring) {
  return '<div class="an-bar-row" style="grid-template-columns:1fr;padding-bottom:0.4rem">'
    + '<div class="an-kpi-sub">' + num(ring.total) + ' job' + (ring.total === 1 ? '' : 's') + ' in scope'
    + (ring.completePct !== null ? ' · ' + ring.completePct + '% complete' : '') + '</div></div>'
    + ring.segments.map(function (sg) {
        return '<div class="an-bar-row"><div>' + esc(sg.label) + '</div>'
          + '<div class="an-bar-track"><div class="an-bar-fill" style="width:' + sg.pct + '%;background:' + sg.color + '"></div></div>'
          + '<div class="an-bar-val">' + sg.count + ' · ' + sg.pct + '%</div></div>';
      }).join('');
}

/* ── Quality tab ────────────────────────────────────────────────────────────
   Built on Assurance rule runs, not on source-vs-target parity. The panel at
   the bottom says so in as many words: two tables can hold the same number of
   rows and different data, and this system does not yet compare them, so it
   must not imply that it does. */

function renderQuality(s) {
  var q = s.quality;
  if (!q.hasData) {
    return '<div class="an-kpis">'
      + kpi({ label: 'Rule pass rate', value: null, sub: 'No runs in this window' })
      + kpi({ label: 'Active rules', value: null, sub: 'No rules yet' })
      + kpi({ label: 'Open breaches', value: null, sub: '—' })
      + '</div>'
      + panel({ title: 'Validation quality', sub: 'Assurance rule outcomes over the selected window',
                body: empty('No validation runs yet', q.reason,
                            '<a href="/assurance">Open Assurance</a>.') })
      + parityCaveatPanel();
  }

  var out = '<div class="an-kpis">'
    + kpi({ label: 'Rule pass rate', value: q.passRate, unit: '%',
            tone: q.passRate < 90 ? 'warn' : '',
            subHtml: esc(q.runs + ' runs · ' + q.pass + ' pass, ' + q.warn + ' warn, ' + q.fail + ' fail') })
    + kpi({ label: 'Active rules', value: q.activeRules, sub: 'Enabled and scheduled' })
    + kpi({ label: 'Open breaches', value: q.openBreaches,
            tone: q.criticalOpen ? 'crit' : q.openBreaches ? 'warn' : '',
            sub: q.criticalOpen ? q.criticalOpen + ' critical' : 'None critical' })
    + kpi({ label: 'Clean streak', value: q.streakDays === null ? null : q.streakDays, unit: 'days',
            sub: q.streakDays === null ? 'No failure recorded yet' : 'Since the last failing run' })
    + kpi({ label: 'Mean time to fix', value: q.meanTimeToFixMs === null ? null : dur(q.meanTimeToFixMs / 1000),
            sub: q.meanTimeToFixMs === null ? 'No breach resolved in this window' : 'Across resolved breaches' })
    + kpi({ label: 'Row-level parity', value: null,
            subHtml: 'Not measured yet — <a href="/data-quality">see below</a>' })
    + '</div>';

  var trendBody = q.series.filter(function (v) { return v !== null; }).length > 1
    ? '<div style="padding:0.4rem 0">' + spark(q.series, 'Rule pass rate') + '</div>'
      + '<div class="an-kpi-sub">Daily pass rate over the selected window. A day with no runs is '
      + 'skipped rather than plotted as zero.</div>'
    : empty('Not enough history to plot',
            'A trend needs runs on more than one day. Schedule the rules and this fills in.',
            '<a href="/assurance">Open Assurance</a>.');

  var taxBody = q.categories.length
    ? q.categories.map(function (cat) {
        var score = cat.score === null ? 0 : cat.score;
        var col = score >= 90 ? 'var(--green)' : score >= 60 ? 'var(--amber)' : 'var(--red)';
        return '<div class="an-bar-row"><div>' + esc(cat.key) + '</div>'
          + '<div class="an-bar-track"><div class="an-bar-fill" style="width:' + score + '%;background:' + col + '"></div></div>'
          + '<div class="an-bar-val">' + (cat.score === null ? '—' : score + '%') + '</div></div>';
      }).join('')
      + '<div class="an-caveat">Worst category first: that is the one a single fix buys the most from.</div>'
    : empty('No categorised runs yet', 'Rules report a category once they have run.',
            '<a href="/assurance">Open Assurance</a>.');

  out += '<div class="an-cols-2">'
    + panel({ title: 'Pass rate over time', sub: 'Daily, across every rule in scope', body: trendBody })
    + panel({ title: 'Where the failures are', sub: 'Pass rate by rule category', body: taxBody })
    + '</div>';

  out += panel({
    title: 'Rule health by day',
    sub: 'Darker means a larger share of that rule\'s runs failed that day',
    body: q.heatmap.length ? heatmapHtml(q.heatmap)
      : empty('No runs in this window', 'Widen the time range, or run the rules.',
              '<a href="/assurance">Open Assurance</a>.'),
  });

  out += parityCaveatPanel();
  return out;
}

function heatmapHtml(rows) {
  return '<div class="an-tblwrap"><table class="an-heat"><tbody>'
    + rows.map(function (r) {
        return '<tr><th scope="row">' + esc(r.ruleId) + '</th>'
          + r.days.map(function (d) {
              if (!d) return '<td class="miss" title="Not run"></td>';
              /* Intensity is share-of-runs-failed, so a rule that ran once and
                 failed reads as strongly as one that failed ten of ten — which
                 is right: both are entirely failing. */
              var a = 0.12 + d.intensity * 0.78;
              return '<td style="background:rgba(192,57,43,' + a.toFixed(2) + ')" title="'
                + esc(d.runs + ' run(s), ' + d.fail + ' failed, ' + d.warn + ' warned') + '"></td>';
            }).join('')
          + '</tr>';
      }).join('')
    + '</tbody></table>'
    + '<div class="an-caveat">Oldest day on the left, today on the right. A dashed cell means the rule '
    + 'did not run that day — which is not the same as passing.</div></div>';
}

/* The honest panel. This is the thing the tab was originally drawn around, and
   it does not exist, so it is described rather than faked. */
function parityCaveatPanel() {
  return panel({
    title: 'Row-level parity is not measured yet',
    sub: 'What this tab does and does not tell you',
    body: '<p class="an-kpi-sub" style="line-height:1.7">Everything above reports whether your '
      + '<strong>validation rules</strong> are passing. That is not the same question as whether the '
      + 'target matches the source. Cygenix does not currently compare source and target row counts '
      + 'or checksums, so tables-at-parity, rows-diverged and drift-by-table cannot be shown without '
      + 'inventing them. Two tables can hold the same number of rows and different data, and a count '
      + 'that agreed would prove nothing on its own.</p>'
      + '<p class="an-kpi-sub" style="margin-top:0.6rem">The nearest thing available today is a '
      + '<a href="/data-quality">Quality Review</a> run, and rules promoted into '
      + '<a href="/assurance">Assurance</a> keep checking continuously.</p>',
  });
}

/* ── Portfolio tab ──────────────────────────────────────────────────────── */

function renderPortfolio(s) {
  var p = s.portfolio;
  var w = s.weekly;

  var out = '<div class="an-kpis">'
    + kpi({ label: 'Projects', value: p.projectCount,
            sub: p.projectCount ? p.atRisk + ' at risk' : 'None yet' })
    + kpi({ label: 'Objects converted', value: num(p.objectsConverted), sub: 'Jobs complete, all projects' })
    + kpi({ label: 'Automation rate', value: p.automationRate, unit: p.automationRate === null ? '' : '%',
            sub: p.automationRate === null ? 'No mapped jobs to measure'
               : 'of ' + p.automationBasis + ' mapped jobs, converted without a hand edit' })
    + kpi({ label: 'Converted this week', value: num(w.counts[w.counts.length - 1]),
            subHtml: spark(w.counts, 'Weekly completions') })
    + kpi({ label: 'AI Assist spend', value: null,
            subHtml: 'Not tracked per project — <a href="/dashboard#goto=system-parameters">see usage</a>' })
    + kpi({ label: 'At risk of slipping', value: p.atRisk, tone: p.atRisk ? 'warn' : '',
            sub: p.atRisk ? 'Hover a risk pill for the reason' : 'No project is triggering a risk' })
    + '</div>';

  var weeklyBody = w.total
    ? '<div class="an-bar-row" style="grid-template-columns:1fr">' + spark(w.counts, 'Objects converted per week') + '</div>'
      + w.counts.map(function (n, i) {
          var max = Math.max.apply(null, w.counts) || 1;
          var back = w.counts.length - 1 - i;
          var label = back === 0 ? 'This week' : back === 1 ? 'Last week' : back + ' weeks ago';
          return '<div class="an-bar-row"><div>' + label + '</div>'
            + '<div class="an-bar-track"><div class="an-bar-fill" style="width:' + (n / max * 100) + '%"></div></div>'
            + '<div class="an-bar-val">' + n + (w.automatic[i] ? ' · ' + w.automatic[i] + ' auto' : '') + '</div></div>';
        }).join('')
      + (w.undated
          ? '<div class="an-caveat">' + w.undated + ' completed job'
            + (w.undated === 1 ? ' carries' : 's carry') + ' no run date and cannot be placed in a week. '
            + 'Imported project bundles often arrive that way.</div>'
          : '')
    : empty('Nothing completed in the last eight weeks',
            'This chart is built from job run dates. It fills in as jobs finish.',
            '<a href="/dashboard#goto=all-jobs">See jobs</a>.');

  var effortBody = window.CygenixEffortModel
    ? '<p class="an-kpi-sub" style="line-height:1.7">Hours saved is calculated from the estimator\'s '
      + 'own baseline, which is a setting rather than a constant so the assumption travels with the '
      + 'figure. Edit the baseline and the saving recalculates.</p>'
      + '<p style="margin-top:0.7rem"><a class="btn btn-sm" href="/effort-estimator">Open the Configurator</a></p>'
    : empty('Effort model unavailable', 'The estimator module did not load on this page.', '');

  out += '<div class="an-cols-2">'
    + panel({ title: 'Objects converted per week', sub: 'Last eight weeks, with the automatic share', body: weeklyBody })
    + panel({ title: 'Effort model', sub: 'How hours saved is calculated', body: effortBody })
    + '</div>';

  out += panel({
    title: 'All projects', sub: 'Every migration, with eight-week progress and a derived risk',
    aside: '<a class="btn btn-sm" href="/projects">Manage projects</a>',
    body: p.projects.length ? projectsTable(p) : empty('No projects yet',
      'Create a project and its progress appears here.', '<a href="/projects?new=1">New project</a>.'),
    foot: '<span>Derived from job, validation and stream state as it stands now</span>'
      + '<span>' + p.projectCount + ' project' + (p.projectCount === 1 ? '' : 's') + '</span>',
  });

  return out;
}

function projectsTable(p) {
  return '<div class="an-tblwrap"><table class="an-tbl"><thead><tr>'
    + '<th>Project</th><th>Client</th><th class="num">Jobs</th><th style="width:150px">Progress</th>'
    + '<th style="width:110px">8-week trend</th><th class="num">To cutover</th><th>Risk</th>'
    + '</tr></thead><tbody>'
    + p.projects.map(function (r) {
        var tone = r.risk.level === 'high' ? 'crit' : r.risk.level === 'medium' ? 'warn' : 'good';
        var pct = r.percentComplete === null ? 0 : r.percentComplete;
        return '<tr>'
          + '<td><a href="/projects">' + esc(r.name) + '</a></td>'
          + '<td>' + esc(r.client || '—') + '</td>'
          + '<td class="num">' + r.objects + '</td>'
          + '<td><div class="an-prog"><div class="an-prog-track"><div class="an-prog-fill" style="width:' + pct + '%"></div></div>'
          + '<span class="an-prog-pct">' + (r.percentComplete === null ? '—' : pct + '%') + '</span></div></td>'
          + '<td>' + spark(r.series, r.name + ' weekly progress') + '</td>'
          + '<td class="num">' + (r.risk.daysToCutover === null ? '—' : r.risk.daysToCutover + 'd') + '</td>'
          /* The pill carries a word AND the reason, so it is readable without
             colour and answerable without opening anything. */
          + '<td><span class="an-pill an-pill-' + tone + '" title="' + esc(r.risk.why) + '">'
          + esc(r.risk.label) + '</span></td>'
          + '</tr>';
      }).join('')
    + '</tbody></table>'
    + '<div class="an-caveat">Risk is derived, never typed in: it combines readiness, days to the '
    + 'cutover window, open validation breaches and whether anything moved in the last seven days. '
    + 'Hover a pill to see which of the four fired.</div></div>';
}

/* ── Paint ──────────────────────────────────────────────────────────────── */

function paint() {
  snapshot = takeSnapshot();

  if (scope === 'active' && !snapshot.project) {
    /* The same guidance Home gives, rather than silently widening to every
       project and quietly answering a different question. */
    var none = empty('No active project',
      'Analytics is scoped to the project you are working on. Choose one, or switch the scope '
      + 'control above to All projects.',
      '<a href="/projects">Go to Projects</a>.');
    el('an-panel-delivery').innerHTML = none;
    el('an-panel-quality').innerHTML = none;
    el('an-panel-portfolio').innerHTML = renderPortfolio(snapshot);
    return;
  }

  el('an-panel-delivery').innerHTML  = renderDelivery(snapshot);
  el('an-panel-quality').innerHTML   = renderQuality(snapshot);
  el('an-panel-portfolio').innerHTML = renderPortfolio(snapshot);
}

/* ── Tabs ───────────────────────────────────────────────────────────────────
   State goes in the query string so a view can be linked to — the alert band
   on Home points at ?tab=delivery — but with replaceState, because changing
   tab is a filter change and the back button should still mean "the previous
   screen". Same policy as the Data Stream page. */

function setTab(tab, focus) {
  if (TABS.indexOf(tab) === -1) tab = 'delivery';
  activeTab = tab;
  TABS.forEach(function (t) {
    var btn = el('an-tab-' + t), pane = el('an-panel-' + t);
    var on = t === tab;
    btn.classList.toggle('cx-tab-active', on);
    btn.setAttribute('aria-selected', on ? 'true' : 'false');
    btn.tabIndex = on ? 0 : -1;          // roving tabindex: one stop for the set
    pane.hidden = !on;
  });
  if (focus) el('an-tab-' + tab).focus();
  try {
    var q = new URLSearchParams(location.search);
    q.set('tab', tab);
    history.replaceState(null, '', location.pathname + '?' + q.toString());
  } catch (_) {}
}

function wireTabs() {
  var bar = document.querySelector('.cx-tabs');
  bar.addEventListener('click', function (e) {
    var btn = e.target.closest('[data-an-tab]');
    if (btn) setTab(btn.dataset.anTab);
  });
  bar.addEventListener('keydown', function (e) {
    var i = TABS.indexOf(activeTab);
    if (e.key === 'ArrowRight') { e.preventDefault(); setTab(TABS[(i + 1) % TABS.length], true); }
    else if (e.key === 'ArrowLeft') { e.preventDefault(); setTab(TABS[(i - 1 + TABS.length) % TABS.length], true); }
    else if (e.key === 'Home') { e.preventDefault(); setTab(TABS[0], true); }
    else if (e.key === 'End') { e.preventDefault(); setTab(TABS[TABS.length - 1], true); }
  });
}

/* ── Polling ────────────────────────────────────────────────────────────────
   Every 30s, and never while the tab is hidden: a backgrounded analytics page
   re-reading storage all day is battery nobody agreed to spend. */

function start() { if (!timer) timer = setInterval(paint, POLL_MS); }
function stop() { if (timer) { clearInterval(timer); timer = null; } }

/* ── Export ─────────────────────────────────────────────────────────────── */

/* RFC 4180-ish escaping, the same rules validation.html uses: wrap any cell
   containing a comma, quote or newline, and double up internal quotes. */
function csvEscape(v) {
  var s = v == null ? '' : String(v);
  return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}

function exportCsv() {
  if (!snapshot) return;
  var rows = [];
  if (activeTab === 'portfolio') {
    rows.push(['Project', 'Client', 'Jobs', 'Percent complete', 'Days to cutover', 'Risk', 'Why']);
    snapshot.portfolio.projects.forEach(function (r) {
      rows.push([r.name, r.client, r.objects, r.percentComplete === null ? '' : r.percentComplete,
                 r.risk.daysToCutover === null ? '' : r.risk.daysToCutover, r.risk.label, r.risk.why]);
    });
  } else if (activeTab === 'quality') {
    var q = snapshot.quality;
    rows.push(['Category', 'Pass rate %']);
    (q.categories || []).forEach(function (c) { rows.push([c.key, c.score === null ? '' : c.score]); });
  } else {
    rows.push(['Stream', 'Lag seconds', 'Rows per minute', 'Queued', 'Dead-letter', 'State']);
    (snapshot.delivery.streams || []).forEach(function (r) {
      rows.push([r.name, r.lagSeconds === null ? '' : r.lagSeconds, r.rowsPerMin, r.queued, r.dlq,
                 r.failing ? 'Blocked' : r.pausedByUser ? 'Paused by user' : r.overSlo ? 'Over SLO' : 'Running']);
    });
  }
  if (rows.length < 2) { alert('There is nothing on this tab to export yet.'); return; }

  var body = rows.map(function (r) { return r.map(csvEscape).join(','); }).join('\r\n');
  // BOM so Excel detects UTF-8 rather than rendering names as mojibake.
  var blob = new Blob(['﻿' + body], { type: 'text/csv;charset=utf-8' });
  var a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'cygenix_analytics_' + activeTab + '_' + new Date().toISOString().slice(0, 10) + '.csv';
  document.body.appendChild(a);
  a.click();
  setTimeout(function () { URL.revokeObjectURL(a.href); a.remove(); }, 0);
  // Data leaving the product is worth a row in the trail. The row count and
  // the tab say what left; nothing about the contents is sent, because the
  // figures are already in the log's own events and copying them here would
  // duplicate the data without adding a fact.
  if (window.CygenixAudit) {
    window.CygenixAudit.record({
      action: 'data.export-csv', category: 'data',
      target: { type: 'export', id: 'analytics-' + activeTab,
                label: 'Analytics > ' + activeTab },
      summary: 'Exported ' + (rows.length - 1) + ' row' + (rows.length === 2 ? '' : 's') +
               ' of ' + activeTab + ' analytics as CSV',
      projectId: (snapshot && snapshot.projectId) || null,
    });
  }
}

/* The client pack. jsPDF and html2canvas are lazy-loaded from CDN on click,
   exactly as reports-app.js does it — the page must not pay for a PDF library
   nobody has asked for yet. */
var PDF_CDN = [
  'https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js',
  'https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js',
];
function loadScript(src) {
  return new Promise(function (res, rej) {
    var s = document.createElement('script');
    s.src = src; s.onload = res; s.onerror = function () { rej(new Error('Could not load ' + src)); };
    document.head.appendChild(s);
  });
}

async function exportPdf() {
  var btn = el('an-export-pdf');
  var label = btn.innerHTML;
  btn.disabled = true;
  btn.textContent = 'Building…';
  try {
    if (!window.jspdf) await loadScript(PDF_CDN[0]);
    if (!window.html2canvas) await loadScript(PDF_CDN[1]);
    var pane = el('an-panel-' + activeTab);
    var canvas = await window.html2canvas(pane, { scale: 2, backgroundColor: '#ffffff' });
    var pdf = new window.jspdf.jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });
    var pw = pdf.internal.pageSize.getWidth() - 16;
    var ph = canvas.height * pw / canvas.width;
    pdf.setFontSize(13);
    pdf.text('Cygenix Analytics — ' + activeTab, 8, 12);
    pdf.setFontSize(8);
    pdf.text(new Date().toLocaleString('en-GB'), 8, 17);
    pdf.addImage(canvas.toDataURL('image/png'), 'PNG', 8, 21, pw, ph);
    pdf.save('cygenix_analytics_' + activeTab + '_' + new Date().toISOString().slice(0, 10) + '.pdf');
    if (window.CygenixAudit) {
      window.CygenixAudit.record({
        action: 'data.export-pdf', category: 'data',
        target: { type: 'export', id: 'analytics-' + activeTab,
                  label: 'Analytics > ' + activeTab },
        summary: 'Exported the ' + activeTab + ' analytics tab as PDF',
        projectId: (snapshot && snapshot.projectId) || null,
      });
    }
  } catch (e) {
    // The CDN is the one thing here that can fail for a reason the user can
    // act on, so say which step failed rather than "export failed".
    alert('The client pack needs to fetch its PDF library and that request did not complete. '
      + 'Check your connection and try again, or use CSV.\n\n' + (e && e.message ? e.message : ''));
  } finally {
    btn.disabled = false;
    btn.innerHTML = label;
  }
}

/* ── Boot ───────────────────────────────────────────────────────────────── */

function boot() {
  if (!A || !DS || !U || !PF || !PL) {
    document.querySelector('.page').insertAdjacentHTML('beforeend',
      '<div class="panel"><div class="panel-body">' + empty('Analytics could not start',
        'One of the modules this page reads did not load. Reload the page; if it persists, the '
        + 'browser console names the file.', '') + '</div></div>');
    return;
  }

  var q = new URLSearchParams(location.search);
  var t = q.get('tab');
  rangeDays = Number(q.get('days')) || 14;
  scope = q.get('scope') === 'all' ? 'all' : 'active';
  el('an-range').value = String(rangeDays);
  el('an-scope').value = scope;

  wireTabs();
  setTab(TABS.indexOf(t) !== -1 ? t : 'delivery');
  paint();
  start();

  el('an-range').addEventListener('change', function (e) { rangeDays = Number(e.target.value) || 14; paint(); });
  el('an-scope').addEventListener('change', function (e) { scope = e.target.value; paint(); });
  el('an-refresh').addEventListener('click', paint);
  el('an-export-csv').addEventListener('click', exportCsv);
  el('an-export-pdf').addEventListener('click', exportPdf);

  document.addEventListener('visibilitychange', function () {
    if (document.hidden) stop(); else { paint(); start(); }
  });

  /* Another tab changing the active project or writing a job should not leave
     this screen showing yesterday's answer. */
  window.addEventListener('storage', function (e) {
    if (!e.key) return;
    if (/^cygenix_(jobs|projects|active_project_id|inventory|assurance)/.test(e.key)) paint();
  });

  /* Give the assistant something to answer with beyond the DOM. */
  try {
    if (window.CygenixAssistant && window.CygenixAssistant.registerContext) {
      window.CygenixAssistant.registerContext(function () {
        return snapshot ? {
          analyticsTab: activeTab,
          analyticsScope: scope,
          readiness: snapshot.confidence ? snapshot.confidence.score : null,
          streamsRunning: snapshot.delivery.running,
          streamsPausedByUser: snapshot.delivery.pausedByUser,
          jobsInScope: snapshot.ring.total,
        } : {};
      });
    }
  } catch (_) {}
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
else boot();
})();
