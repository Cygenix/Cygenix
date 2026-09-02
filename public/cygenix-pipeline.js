/* ============================================================================
   cygenix-pipeline.js — the migration lifecycle, as one honest sentence.
   ----------------------------------------------------------------------------
   The Home screen reported state and told no story. Eight cards of equal
   weight, each true, none of them saying what today's problem is: a person
   had to read all eight and do the joining up themselves, every morning.

   The Data Stream page already solved this. Its Flow view puts capture →
   store → delivery in a row with the live rate on each hop and labels the one
   that cannot keep up BOTTLENECK — which is what makes sense of a stream
   showing "Failed" and "260/min" at the same time. This module ports that
   pattern to the migration lifecycle: connect → analyse → map → generate →
   validate → cutover, with what is sitting at each stage and at most ONE
   thing named as the blockage.

   WHY IT OWNS NO DOM
   Everything here is a pure function over the shapes the console already
   stores. The derivation is where the judgement lives — what counts as
   analysed, which stage is the bottleneck, what the sentence says — and that
   is exactly the part that must be testable without a browser and provable
   against fixtures. The renderer in dashboard-app.js consumes a PipelineModel
   and knows nothing about jobs.

   ONE BOTTLENECK OR NONE
   The value of the Flow view is that it points at one thing. When a project
   is healthy this module returns `bottleneck: null` and the card says so,
   rather than inventing a worst stage to fill the slot.

   THE COUNTERS DISAGREE, AND THAT IS DATA
   Nothing in the stored shape forces the funnel to narrow: a job can carry
   generated SQL and no column mapping, because SQL can arrive from an import
   or the mapping can be cleared afterwards. A funnel that quietly widened
   would look like a rendering bug. So the inversion is detected, named on the
   stage that is behind, and can be the narrative — see `inversions`.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object') root.CygenixPipeline = api;
})(typeof window !== 'undefined' ? window : this, function () {
'use strict';

var STAGE_ORDER = ['connect', 'analyse', 'map', 'generate', 'validate', 'cutover'];

var STAGE_LABEL = {
  connect: 'Connect', analyse: 'Analyse', map: 'Map',
  generate: 'Generate', validate: 'Validate', cutover: 'Cutover',
};

/* Where each stage is acted on. A stage chip that is not a link is a
   diagnosis with no treatment. */
var STAGE_HREF = {
  connect:  '/dashboard#goto=connections',
  analyse:  '/schema-explorer',
  map:      '/object-mapping',
  generate: '/sql-editor',
  validate: '/validation',
  cutover:  '/dashboard#goto=all-jobs',
};

/* ── Job predicates ───────────────────────────────────────────────────────────
 * These mirror the dashboard's own helpers deliberately. Where one already
 * exists in dashboard-app.js (jobIsMapped, jobBucket) the definition here is
 * character-for-character the same test, because two screens disagreeing
 * about what "analysed" means is the bug this whole card exists to end.
 */

function bucketOf(j) {
  var s = String((j && (j.executionStatus || j.status)) || '').toLowerCase();
  if (s === 'complete' || s === 'completed' || s === 'success') return 'complete';
  if (s === 'ready' || s === 'sql_ready' || s === 'sql-ready') return 'ready';
  if (s === 'failed' || s === 'fail' || s === 'error') return 'failed';
  return 'pending';
}

/** Structure known: the job has learned something about its own columns. */
function hasStructure(j) {
  if (!j) return false;
  if (Array.isArray(j.tables) && j.tables.length) return true;
  if (Array.isArray(j.columns) && j.columns.length) return true;
  if (Array.isArray(j.columnMapping) && j.columnMapping.length) return true;
  return false;
}

/** Mapped: at least one source column points at a target column. */
function isMapped(j) {
  return !!(j && Array.isArray(j.columnMapping) && j.columnMapping.some(function (m) {
    return m && m.tgtCol;
  }));
}

/** SQL exists — written by the generator, or the job has run with it. */
function hasSql(j) {
  if (!j) return false;
  if (j.insertSQL || j.migrationSQL || j.sql) return true;
  var b = bucketOf(j);
  return b === 'ready' || b === 'complete';
}

/** A verify block long enough to be a check rather than a placeholder. */
function hasVerify(j) {
  return !!(j && j.verifySQL && String(j.verifySQL).trim().length > 20);
}

function isComplete(j) { return bucketOf(j) === 'complete'; }
function isFailed(j) { return bucketOf(j) === 'failed'; }

function pct(n, d) { return d > 0 ? Math.round((n / d) * 100) : 0; }
function plural(n, one, many) { return n === 1 ? one : (many || one + 's'); }

/* ── The model ────────────────────────────────────────────────────────────── */

/**
 * Build the pipeline from what the console already stores.
 *
 * @param {object} input
 *   project      the active project (or null)
 *   jobs         jobs ALREADY scoped to that project and already excluding
 *                soft-deleted ones — scoping is the caller's business, and
 *                doing it here would hide which scope produced a number
 *   connections  { source: truthy, target: truthy }
 *   quality      { rulesCount, inspectedJobs }
 *   reports      { count: number|null }   null = not known yet
 *   confidence   the result of CygenixPreflight.pfConfidence, or null
 *   now          ms, for testability
 */
function toPipelineModel(input) {
  input = input || {};
  var jobs = Array.isArray(input.jobs) ? input.jobs.filter(Boolean) : [];
  var project = input.project || null;
  var conns = input.connections || {};
  var quality = input.quality || {};
  var reports = input.reports || {};
  var now = input.now || Date.now();
  var total = jobs.length;

  var srcOn = !!conns.source, tgtOn = !!conns.target;
  var connCount = (srcOn ? 1 : 0) + (tgtOn ? 1 : 0);

  var analysed = jobs.filter(hasStructure).length;
  var mapped = jobs.filter(isMapped).length;
  var generated = jobs.filter(hasSql).length;
  var validated = jobs.filter(hasVerify).length;
  var complete = jobs.filter(isComplete).length;
  var failed = jobs.filter(isFailed).length;

  var stages = [];

  /* Connect. Two sides, and neither is optional: without a target there is
     nothing to generate SQL against. */
  stages.push(finish({
    key: 'connect', count: connCount, total: 2,
    headline: connCount + ' of 2 connected',
    state: connCount === 2 ? 'done' : connCount === 0 ? 'blocked' : 'attention',
    reason: connCount === 2 ? null
      : connCount === 0 ? 'Neither a source nor a target database is connected.'
      : 'Only the ' + (srcOn ? 'source' : 'target') + ' is connected — the '
        + (srcOn ? 'target' : 'source') + ' is needed before SQL can be generated.',
    detail: connCount === 2 ? 'source and target' : null,
  }));

  /* Everything downstream is per job, so with no jobs the whole tail is
     waiting rather than failing. A zero that means "nothing asked of it yet"
     must not look like a zero that means "you forgot". */
  var noJobs = total === 0;

  stages.push(finish(perJob('analyse', analysed, total, {
    upstream: connCount,
    detailDone: 'schema read',
    reasonNone: connCount === 0
      ? 'Nothing can be analysed until a database is connected.'
      : 'No job has had its schema read yet.',
    reasonSome: function (rest) {
      return rest + ' ' + plural(rest, 'job') + ' still ' + (rest === 1 ? 'needs' : 'need')
        + ' schema analysis before mapping can complete.';
    },
  }, noJobs)));

  stages.push(finish(perJob('map', mapped, total, {
    upstream: analysed,
    detailDone: 'columns mapped',
    reasonNone: 'No columns are mapped to a target yet.',
    reasonSome: function (rest) {
      return rest + ' ' + plural(rest, 'job') + ' ' + (rest === 1 ? 'has' : 'have')
        + ' no column mapping.';
    },
  }, noJobs)));

  stages.push(finish(perJob('generate', generated, total, {
    upstream: mapped,
    detailDone: 'SQL written',
    reasonNone: 'No migration SQL has been generated yet.',
    reasonSome: function (rest) {
      return rest + ' ' + plural(rest, 'job') + ' ' + (rest === 1 ? 'has' : 'have')
        + ' no generated SQL.';
    },
  }, noJobs)));

  stages.push(finish(perJob('validate', validated, total, {
    upstream: generated,
    detailDone: 'verify SQL set',
    reasonNone: 'No job has a verify query, so nothing would be checked after a run.',
    reasonSome: function (rest) {
      return rest + ' of ' + total + ' ' + plural(total, 'job') + ' ' + (rest === 1 ? 'has' : 'have')
        + ' no verify SQL, so ' + (100 - pct(validated, total)) + '% of the migration is unverified.';
    },
  }, noJobs)));

  /* Cutover is the only stage where a date can make an otherwise quiet
     "not started" into a problem. */
  var endTs = project && project.end ? Date.parse(project.end) : NaN;
  var pastEnd = !isNaN(endTs) && now > endTs;
  var cut = perJob('cutover', complete, total, {
    upstream: validated,
    detailDone: 'run and complete',
    reasonNone: 'No job has completed a run yet.',
    reasonSome: function (rest) {
      return rest + ' ' + plural(rest, 'job') + ' ' + (rest === 1 ? 'has' : 'have')
        + ' not completed a run.';
    },
  }, noJobs);
  /* Cutover is the terminal stage, and nothing having cut over yet is the
     normal state of a migration in build — not a gap to point at. Left as
     'attention' it would win the bottleneck every time on percentage alone
     and the card would spend the whole project pointing at the end of it.
     Only the calendar turns this into a problem. */
  if (cut.count === 0 && cut.state === 'attention') {
    cut.state = 'waiting';
    cut.reason = 'No job has completed a run yet, which is expected until the first full run.';
  }
  if (pastEnd && complete < total && total > 0) {
    cut.state = 'blocked';
    cut.reason = 'Target end date ' + fmtDate(endTs) + ' has passed with '
      + (total - complete) + ' ' + plural(total - complete, 'job') + ' incomplete.';
  }
  stages.push(finish(cut));

  /* Inversions. A later stage cannot legitimately be ahead of an earlier one,
     so where it is, the earlier stage is the one carrying the surprise. */
  var inversions = [];
  for (var i = 2; i < stages.length; i++) {          // from 'map' onwards
    var prev = stages[i - 1], cur = stages[i];
    if (cur.count > prev.count) {
      inversions.push({ behind: prev.key, ahead: cur.key, delta: cur.count - prev.count });
      prev.state = prev.state === 'blocked' ? 'blocked' : 'attention';
      prev.reason = cur.count + ' ' + plural(cur.count, 'job') + ' reached '
        + STAGE_LABEL[cur.key].toLowerCase() + ' but only ' + prev.count + ' cleared '
        + STAGE_LABEL[prev.key].toLowerCase() + '. Those jobs skipped this step or had it undone.';
    }
  }

  var bottleneck = selectBottleneck(stages);

  var model = {
    stages: stages,
    bottleneck: bottleneck,
    inversions: inversions,
    counts: {
      jobs: total, analysed: analysed, mapped: mapped, generated: generated,
      validated: validated, complete: complete, failed: failed,
      connections: connCount,
      qualityRules: Number(quality.rulesCount) || 0,
      qualityInspected: Number(quality.inspectedJobs) || 0,
      reports: reports.count === undefined ? null : reports.count,
    },
    project: project ? { id: project.id, name: project.name, end: project.end || null } : null,
    pastEnd: pastEnd,
    confidence: input.confidence
      ? { score: input.confidence.score, band: bandOf(input.confidence.grade),
          drivers: confidenceDrivers(input.confidence) }
      : null,
    asOf: new Date(now).toISOString(),
    narrative: '',
  };
  model.narrative = buildNarrative(model);
  return model;
}

/* Shared per-job stage construction. `upstream` is how many jobs cleared the
   previous stage: it is what separates "not started because nothing has
   reached here" from "started upstream and stalled here". */
function perJob(key, count, total, opts, noJobs) {
  var rest = total - count;
  if (noJobs) {
    return {
      key: key, count: 0, total: 0, state: 'waiting',
      headline: 'no jobs yet',
      reason: 'This project has no jobs, so there is nothing to ' + STAGE_LABEL[key].toLowerCase() + ' yet.',
    };
  }
  var state;
  if (count === total) state = 'done';
  else if (count === 0) state = opts.upstream > 0 ? 'attention' : 'waiting';
  else state = 'active';

  return {
    key: key, count: count, total: total, state: state,
    headline: count + ' of ' + total + ' ' + plural(total, 'job'),
    detail: count === total ? opts.detailDone : (rest + ' to go'),
    reason: state === 'done' ? null : (count === 0 ? opts.reasonNone : opts.reasonSome(rest)),
  };
}

function finish(s) {
  s.label = STAGE_LABEL[s.key];
  s.href = STAGE_HREF[s.key];
  s.pct = s.total > 0 ? pct(s.count, s.total) : 0;
  if (!s.reason && (s.state === 'blocked' || s.state === 'attention' || s.state === 'waiting')) {
    s.reason = 'Not started yet.';
  }
  return s;
}

/**
 * At most one bottleneck, chosen in a fixed order so the same project always
 * names the same stage:
 *   1. the earliest blocked stage
 *   2. otherwise the attention stage that has cleared the least of its total,
 *      earliest wins a tie
 *   3. otherwise none — a healthy project gets no label rather than an
 *      invented worst.
 */
function selectBottleneck(stages) {
  for (var i = 0; i < stages.length; i++) {
    if (stages[i].state === 'blocked') return stages[i].key;
  }
  var worst = null;
  stages.forEach(function (s) {
    if (s.state !== 'attention') return;
    if (!worst || s.pct < worst.pct) worst = s;
  });
  return worst ? worst.key : null;
}

function bandOf(grade) {
  return grade === 'green' ? 'good' : grade === 'amber' ? 'caution'
    : grade === 'red' ? 'risk' : 'unknown';
}

/* ── The narrative ────────────────────────────────────────────────────────────
 * What is blocking → why it matters → what to do. Deterministic, no network,
 * no latency: it is on screen at first paint or it is not doing its job.
 *
 * Order matters more than coverage. The first matching case wins, so the
 * cases are arranged worst-first: a project can be past its end date AND
 * unvalidated AND unmapped, and saying all three would say nothing.
 */
var DRIVER_HREF = {
  mapping: '/object-mapping',
  preflight: '/project-builder',
  delivery: '/dashboard#goto=all-jobs',
};

function buildNarrative(model) {
  var c = model.counts;
  var stage = function (k) {
    for (var i = 0; i < model.stages.length; i++) if (model.stages[i].key === k) return model.stages[i];
    return null;
  };

  if (!model.project) {
    return 'No project is active, so there is nothing to report. Activate one and this fills in.';
  }
  if (c.connections === 0) {
    return 'No source or target connected yet — nothing can be analysed until both are set.';
  }
  if (c.jobs === 0) {
    return 'This project has no jobs yet. Create one and the pipeline starts filling in from Analyse.';
  }
  if (model.pastEnd && c.complete < c.jobs) {
    return 'Target end date ' + fmtDate(Date.parse(model.project.end)) + ' has passed with '
      + (c.jobs - c.complete) + ' ' + plural(c.jobs - c.complete, 'job') + ' incomplete.';
  }
  if (c.failed > 0) {
    return c.failed + ' ' + plural(c.failed, 'job') + ' ' + (c.failed === 1 ? 'has' : 'have')
      + ' failed. Nothing downstream of ' + (c.failed === 1 ? 'it' : 'them')
      + ' can be trusted until the ' + plural(c.failed, 'failure') + ' ' + (c.failed === 1 ? 'is' : 'are')
      + ' understood.';
  }
  if (model.inversions.length) {
    var inv = model.inversions[0];
    return c[countKeyFor(inv.ahead)] + ' ' + plural(c[countKeyFor(inv.ahead)], 'job') + ' reached '
      + STAGE_LABEL[inv.ahead].toLowerCase() + ' with only ' + c[countKeyFor(inv.behind)] + ' through '
      + STAGE_LABEL[inv.behind].toLowerCase() + '. Check those jobs before trusting the SQL — '
      + 'the two counts should not be able to disagree.';
  }
  if (c.connections === 1) {
    return 'Only one side is connected. Analysis can run, but nothing can be generated against a '
      + 'target that is not there yet.';
  }

  var an = stage('analyse');
  if (an && an.count < an.total) {
    return (an.total - an.count) + ' of ' + an.total + ' jobs still need schema analysis before '
      + 'mapping can complete.';
  }

  var qualityNeverRun = c.qualityRules === 0 && c.qualityInspected === 0;
  var unverified = c.jobs - c.validated;
  if (qualityNeverRun) {
    return 'Data quality has never run'
      + (unverified > 0 ? ', and ' + unverified + ' of ' + c.jobs + ' ' + plural(c.jobs, 'job')
          + ' ' + (unverified === 1 ? 'has' : 'have') + ' no verify SQL' : '')
      + '. That is the single thing between you and a green cutover score.';
  }
  if (unverified > 0) {
    return unverified + ' of ' + c.jobs + ' ' + plural(c.jobs, 'job') + ' ' + (unverified === 1 ? 'has' : 'have')
      + ' no verify SQL, so ' + (100 - pct(c.validated, c.jobs)) + '% of the migration is unverified.';
  }

  var mp = stage('map');
  if (mp && mp.count < mp.total) {
    return (mp.total - mp.count) + ' of ' + mp.total + ' jobs have no column mapping yet — '
      + 'nothing can be generated for them until they do.';
  }
  var gen = stage('generate');
  if (gen && gen.count < gen.total) {
    return (gen.total - gen.count) + ' of ' + gen.total + ' jobs have no generated SQL yet.';
  }

  if (c.complete === c.jobs) {
    return 'Every job is analysed, mapped, generated, verified and complete. Nothing is blocking.';
  }
  return 'Everything analysed and mapped, and every job has a verify query. Nothing is blocking'
    + (model.project.end ? ' — the next milestone is a full test run before '
        + fmtDate(Date.parse(model.project.end)) : '') + '.';
}

function countKeyFor(stageKey) {
  return { analyse: 'analysed', map: 'mapped', generate: 'generated',
           validate: 'validated', cutover: 'complete' }[stageKey] || 'jobs';
}

function fmtDate(ts) {
  if (!ts || isNaN(ts)) return 'the target date';
  return new Date(ts).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
}

/* ── Confidence, explained ────────────────────────────────────────────────────
 * pfConfidence in cygenix-preflight.js is a WEIGHTED AVERAGE over the
 * components that have data, not a hundred points with deductions — so
 * "100 − 9 − 6" would be a fiction, however tidy it reads. What is true, and
 * still adds up on screen, is each component's weighted contribution: those
 * sum to the score. `lost` is the same arithmetic the other way round — what
 * that component costs against a perfect one — which is the number a person
 * is actually asking for when they click.
 *
 * One function, one source of truth: this reads pfConfidence's own parts and
 * does not re-derive the score.
 */
function confidenceDrivers(conf) {
  if (!conf || !Array.isArray(conf.parts)) return [];
  var live = conf.parts.filter(function (p) { return p.hasData; });
  var wsum = live.reduce(function (a, p) { return a + p.weight; }, 0) || 1;
  return conf.parts.map(function (p) {
    var share = p.hasData ? p.weight / wsum : 0;
    return {
      key: p.key,
      label: p.label,
      note: p.note,
      hasData: !!p.hasData,
      weightPct: Math.round(share * 100),
      points: p.hasData ? Math.round(p.score * share) : 0,      // sums to the score
      lost: p.hasData ? Math.round((100 - p.score) * share) : 0, // sums to 100 − score
      href: DRIVER_HREF[p.key] || '/dashboard',
    };
  });
}

return {
  STAGE_ORDER: STAGE_ORDER,
  STAGE_LABEL: STAGE_LABEL,
  STAGE_HREF: STAGE_HREF,
  toPipelineModel: toPipelineModel,
  selectBottleneck: selectBottleneck,
  buildNarrative: buildNarrative,
  confidenceDrivers: confidenceDrivers,
  __predicates: { bucketOf: bucketOf, hasStructure: hasStructure, isMapped: isMapped,
                  hasSql: hasSql, hasVerify: hasVerify },
};
});
