/* ============================================================================
   cygenix-template-mapping.js — sending a Conversion Template's staging→target
   table pairs to Object Mapping, and taking them back again
   ----------------------------------------------------------------------------
   Sep-2026. A Conversion Template already says, for one module, which staging
   table feeds which target table. Object Mapping stores exactly that as a
   job. Re-typing thirty of them by hand is work nobody should be doing twice,
   so a tick box on the module does it.

   THE ONE RULE THAT MATTERS
   Unticking must never touch a mapping somebody built by hand. Object Mapping
   is where the real work happens — column mappings, WHERE clauses, joins,
   was/is rules — and a tick box that could delete that would be a tick box
   nobody would dare use. So every job this file creates carries

       fromTemplate: { templateId, module, version, tableId }

   and NOTHING is ever removed unless it carries that stamp, for this exact
   template and this exact module. A job without the stamp is somebody's work.
   A job with a different templateId is another template's. Both are left
   alone, always, including when they happen to describe the same pair.

   WHAT IT CREATES: DRAFTS
   A created job is `status: 'draft'`, with no column mapping and no SQL. It
   is the pair — "this staging table feeds this target table" — and a starting
   point for someone to open in Object Mapping and finish. It is deliberately
   not 'ready': a job with no column mapping that claims to be ready is a job
   that will be run by a scheduler one night and move no data.

   THE HUNDRED-JOB CAP
   Every writer of cygenix_jobs in this product ends with
   `jobs.slice(0, 100)`, so the store silently loses the oldest job past a
   hundred. This file will not participate in that. If sending a module would
   push the total past the cap it REFUSES the whole module, says how many
   jobs there are, how many the module needs and how many would be lost, and
   changes nothing. Losing a hundredth of somebody's mapping work to a tick
   box is not a trade this feature gets to make on their behalf.

   No DOM, no storage, no network: the caller passes the jobs array in and
   writes the result back, which is what makes this testable in Node and what
   keeps the storage write in the one place on the page that knows about
   CygenixJobProfile and the sync layer.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixTemplateMapping) root.CygenixTemplateMapping = api;
})(typeof globalThis !== 'undefined' ? globalThis : (typeof window !== 'undefined' ? window : this), function () {
'use strict';

/* The cap every writer of cygenix_jobs applies. Named here so the refusal
   message and the check cannot drift apart. */
var JOB_CAP = 100;

function str(v) { return v == null ? '' : String(v); }
function trim(v) { return str(v).trim(); }
function lower(v) { return trim(v).toLowerCase(); }

/* Qualify a bare table name with a schema, and leave an already-qualified
   name alone. Object Mapping stores and looks up tables as schema.name, so a
   bare name here would make a job it cannot resolve when opened. */
function qualify(name, schema) {
  var n = trim(name);
  if (!n) return '';
  if (n.indexOf('.') !== -1) return n;
  var s = trim(schema);
  return s ? s + '.' + n : n;
}

/* ── The stamp ────────────────────────────────────────────────────────────
   Read through these two, never by poking at the object, so "is this ours"
   is decided in one place. */
function isFromTemplate(job, templateId, moduleName) {
  var ft = job && job.fromTemplate;
  if (!ft || !trim(ft.templateId)) return false;
  if (templateId != null && trim(ft.templateId) !== trim(templateId)) return false;
  if (moduleName != null && lower(ft.module) !== lower(moduleName)) return false;
  return true;
}
function templateJobs(jobs, templateId, moduleName) {
  return (jobs || []).filter(function (j) { return isFromTemplate(j, templateId, moduleName); });
}

/* Two jobs describe the same pair when their source and target tables match,
   ignoring case. Schema included: dbo.X and stg.X are different tables. */
function pairKey(source, target) { return lower(source) + ' -> ' + lower(target); }
function jobPairKey(job) { return pairKey(job && (job.sourceTable || job.source), job && (job.targetTable || job.target)); }

/* ── The pairs a module would send ───────────────────────────────────────── */
function modulePairs(tpl, moduleName, opts) {
  var o = opts || {};
  var mods = (tpl && tpl.modules) || [];
  var m = null;
  for (var i = 0; i < mods.length; i++) {
    if (lower(mods[i].module) === lower(moduleName)) { m = mods[i]; break; }
  }
  if (!m) return [];
  var stagingSchema = trim(o.stagingSchema);
  var targetSchemaOf = typeof o.targetSchemaOf === 'function' ? o.targetSchemaOf : function () { return trim(o.targetSchema); };
  return (m.tables || []).slice().sort(function (a, b) {
    return (Number(a.loadOrder) || 0) - (Number(b.loadOrder) || 0)
      || String(a.targetTable).localeCompare(String(b.targetTable));
  }).map(function (t) {
    return {
      tableId: str(t.id),
      module: m.module,
      stagingTable: str(t.stagingTable),
      targetTable: str(t.targetTable),
      source: qualify(t.stagingTable, stagingSchema),
      target: qualify(t.targetTable, targetSchemaOf(t.targetTable)),
      loadOrder: Number(t.loadOrder) || 0,
    };
  }).filter(function (p) { return p.source && p.target; });
}

/* ── The draft job ───────────────────────────────────────────────────────── */
function buildJob(tpl, pair, opts) {
  var o = opts || {};
  var now = o.now ? new Date(o.now) : new Date();
  return {
    id: str(o.id) || ('job_tpl_' + now.getTime() + '_' + Math.random().toString(36).slice(2, 8)),
    name: pair.stagingTable + ' → ' + pair.targetTable,
    jobType: 'simple-map',
    type: 'migration',
    projectId: str(o.projectId),
    source: pair.source,
    sourceTable: pair.source,
    sourceObjectType: 'TABLE',
    sourceBaseObjects: [],
    target: pair.target,
    targetTable: pair.target,
    // Empty on purpose: the pair is what the template knows. The columns are
    // what somebody decides in Object Mapping, and an empty list there reads
    // as "nothing mapped yet", which is true.
    columnMapping: [],
    insertSQL: '', schemaSQL: '', verifySQL: '',
    totalRows: 0,
    // Not 'ready'. See the header: a job with no column mapping that says it
    // is ready is a job a scheduler will run to no effect.
    status: 'draft',
    created: now.toISOString(),
    warnings: [],
    fromTemplate: {
      templateId: str(tpl && tpl.id),
      templateName: str(tpl && tpl.name),
      version: Number(tpl && tpl.version) || 1,
      module: pair.module,
      tableId: pair.tableId,
      at: now.toISOString(),
      by: str(o.by),
    },
  };
}

/* ── Plan a send ──────────────────────────────────────────────────────────
   Pure. Says what WOULD happen, including the refusal, so the page can show
   it before anything is written. */
function planSend(tpl, moduleName, jobs, opts) {
  var o = opts || {};
  var cap = o.cap || JOB_CAP;
  var list = jobs || [];
  var pairs = modulePairs(tpl, moduleName, o);
  var have = {};
  list.forEach(function (j) { have[jobPairKey(j)] = j; });

  var create = [], duplicates = [];
  pairs.forEach(function (p) {
    var existing = have[pairKey(p.source, p.target)];
    if (existing) {
      // Already mapped — by hand or by an earlier send. Either way the pair
      // is covered and a second job for it would just be noise.
      duplicates.push({ pair: p, jobId: existing.id, jobName: str(existing.name),
        byHand: !isFromTemplate(existing, null, null) });
      return;
    }
    create.push(p);
  });

  var would = list.length + create.length;
  var overBy = would - cap;
  return {
    module: trim(moduleName),
    pairs: pairs,
    create: create,
    duplicates: duplicates,
    total: list.length,
    cap: cap,
    would: would,
    overCap: overBy > 0,
    overBy: overBy > 0 ? overBy : 0,
  };
}

/* Apply a plan. Returns a NEW array — the caller decides whether to store it
   — or a refusal with a reason worth reading. Nothing is trimmed here: if the
   result would not fit, nothing is added at all. */
function applySend(tpl, moduleName, jobs, opts) {
  var o = opts || {};
  var plan = planSend(tpl, moduleName, jobs, o);
  if (plan.overCap) {
    return {
      ok: false, plan: plan,
      reason: 'Object Mapping holds ' + plan.total + ' of its ' + plan.cap + ' saved jobs and "' + plan.module
        + '" needs ' + plan.create.length + ' more, which would push ' + plan.overBy + ' of the oldest out of the store. '
        + 'Nothing was sent. Delete jobs you have finished with in Object Mapping, then tick this again.',
    };
  }
  if (!plan.create.length) {
    return { ok: true, added: 0, jobs: (jobs || []).slice(), plan: plan,
      reason: plan.duplicates.length
        ? 'Every table in "' + plan.module + '" is already mapped in Object Mapping — nothing to add.'
        : 'Module "' + plan.module + '" has no tables to send.' };
  }
  var next = (jobs || []).slice();
  var made = plan.create.map(function (p, i) {
    return buildJob(tpl, p, { projectId: o.projectId, by: o.by, now: o.now ? (Number(o.now) + i) : (Date.now() + i) });
  });
  // Newest first, the same order every other writer of this store uses.
  next = made.concat(next);
  return { ok: true, added: made.length, created: made, jobs: next, plan: plan };
}

/* ── Taking them back ─────────────────────────────────────────────────────
   Only jobs carrying THIS template's stamp for THIS module. Everything else
   survives, including a hand-made job for the same pair and including a job
   that started here and was then edited — an edit does not remove the stamp,
   and that is deliberate: the tick box is the record of where the job came
   from, and a person who has done work on one should untick it knowingly. */
function planRemove(tpl, moduleName, jobs, opts) {
  var o = opts || {};
  var id = str(tpl && tpl.id);
  var list = jobs || [];
  var remove = [], kept = [];
  list.forEach(function (j) {
    if (isFromTemplate(j, id, moduleName)) remove.push(j); else kept.push(j);
  });
  return {
    module: trim(moduleName),
    remove: remove,
    jobs: kept,
    // Worth naming in the confirmation: these are the ones somebody has since
    // put work into, and they are about to go.
    edited: remove.filter(function (j) {
      return (j.columnMapping && j.columnMapping.length) || trim(j.insertSQL) || (j.status && j.status !== 'draft');
    }),
    unaffected: o.countUnaffected ? list.length - remove.length : undefined,
  };
}

function applyRemove(tpl, moduleName, jobs, opts) {
  var plan = planRemove(tpl, moduleName, jobs, opts);
  return { ok: true, removed: plan.remove.length, jobs: plan.jobs, plan: plan };
}

/* How many of this template's jobs are in the store right now — the number
   beside the section heading. Counted from the JOBS, not from the ticks:
   a tick is what the operator asked for, this is what is actually there. */
function countSent(tpl, jobs) {
  return templateJobs(jobs, str(tpl && tpl.id), null).length;
}
function countSentForModule(tpl, moduleName, jobs) {
  return templateJobs(jobs, str(tpl && tpl.id), moduleName).length;
}

return {
  JOB_CAP: JOB_CAP,
  qualify: qualify,
  isFromTemplate: isFromTemplate,
  templateJobs: templateJobs,
  pairKey: pairKey,
  jobPairKey: jobPairKey,
  modulePairs: modulePairs,
  buildJob: buildJob,
  planSend: planSend,
  applySend: applySend,
  planRemove: planRemove,
  applyRemove: applyRemove,
  countSent: countSent,
  countSentForModule: countSentForModule,
};
});
