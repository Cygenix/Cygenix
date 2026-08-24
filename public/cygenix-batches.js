/* ============================================================================
   cygenix-batches.js — named, reusable arrangements of migration jobs.
   ----------------------------------------------------------------------------
   WHAT A BATCH IS

   The Batches screen runs a set of jobs, arranged into ordered groups, each
   group sequential or parallel. That arrangement — which jobs, in which
   groups, in which order — is the batch.

   Until now there was exactly ONE arrangement per project, held on the project
   record itself. Wanting a second one (a full load and a nightly delta; a
   three-job smoke set and the real forty-job run) meant destroying the first
   or inventing a duplicate project. Save/Load fixes that: arrangements are
   named, saved per project, and loaded back over the current one.

   WHAT A BATCH IS NOT

   It is not a copy of the jobs. A batch stores each step's jobId and enough
   identity to report on it — the job records themselves stay where they are,
   in cygenix_jobs. That matters two ways:

     - editing a job's mapping updates every batch that includes it, which is
       what you want: a batch is a running order, not a fork;
     - a job deleted after the batch was saved leaves a hole, so loading has
       to say so rather than silently loading a step that will fail at run
       time. resolveBatch() is where that reconciliation happens.

   Node-requirable so the merge, the reconciliation and the pruning rules are
   tested without a browser.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixBatches) root.CygenixBatches = api;
})(typeof window !== 'undefined' ? window : this, function () {
'use strict';

var STORE_KEY = 'cygenix_batches_v1';
/* Per project. Enough for any real workflow, and a bound stops a runaway
   save loop filling the storage quota and taking the page down with it. */
var MAX_PER_PROJECT = 50;

function read() {
  try {
    if (typeof localStorage === 'undefined') return {};
    var raw = localStorage.getItem(STORE_KEY);
    var doc = raw ? JSON.parse(raw) : {};
    return (doc && typeof doc === 'object' && !Array.isArray(doc)) ? doc : {};
  } catch (e) { return {}; }
}
function write(doc) {
  try {
    if (typeof localStorage !== 'undefined') localStorage.setItem(STORE_KEY, JSON.stringify(doc));
    return true;
  } catch (e) { return false; }   // quota — the caller reports it
}

function newId() {
  return 'bat_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 7);
}

/* ── reading ─────────────────────────────────────────────────────────────── */

/** Every batch saved against this project, newest first. */
function batchesFor(projectId) {
  var list = read()[String(projectId || '')] || [];
  if (!Array.isArray(list)) return [];
  return list.slice().sort(function (a, b) {
    return String(b.savedAt || '').localeCompare(String(a.savedAt || ''));
  });
}

function getBatch(projectId, batchId) {
  return batchesFor(projectId).filter(function (b) { return b.id === batchId; })[0] || null;
}

/* ── writing ─────────────────────────────────────────────────────────────── */

/* Only the arrangement is stored, not the jobs. A step keeps its jobId, plus
   the name and type as they were — so a batch whose job has since been
   deleted can still tell you WHICH job is missing rather than showing a bare
   id. Everything else about a step is re-read from the live job on load. */
function snapshotGroups(groups) {
  return (groups || []).map(function (g) {
    return {
      id: g.id,
      name: g.name || 'Group',
      executionMode: g.executionMode === 'parallel' ? 'parallel' : 'sequential',
      steps: (g.steps || []).map(function (s) {
        return {
          jobId: s.jobId || '',
          name: s.name || '',
          jobType: s.jobType || 'migration',
          // Per-step run flags that belong to the arrangement rather than to
          // the job: skipping a step is a property of this batch.
          enabled: s.enabled === false ? false : true,
        };
      }),
    };
  });
}

function countSteps(groups) {
  return (groups || []).reduce(function (n, g) { return n + ((g.steps || []).length); }, 0);
}

/**
 * Save the current arrangement under a name. Saving over an existing name
 * replaces it — the alternative is a list of six things called "Nightly", and
 * nobody can tell those apart later.
 */
function saveBatch(projectId, name, groups, opts) {
  var o = opts || {};
  var pid = String(projectId || '');
  var clean = String(name || '').trim();
  if (!pid) throw new Error('A batch belongs to a project — no project id given.');
  if (!clean) throw new Error('Give the batch a name.');
  if (!countSteps(groups)) throw new Error('There are no jobs to save. Add at least one job first.');

  var doc = read();
  var list = Array.isArray(doc[pid]) ? doc[pid].slice() : [];
  var existing = list.filter(function (b) {
    return String(b.name || '').toLowerCase() === clean.toLowerCase();
  })[0];

  var record = {
    id: existing ? existing.id : newId(),
    name: clean,
    savedAt: o.now || new Date().toISOString(),
    savedBy: o.user || null,
    note: String(o.note || '').slice(0, 300) || null,
    groups: snapshotGroups(groups),
  };
  record.jobCount = countSteps(record.groups);
  record.groupCount = record.groups.length;

  if (existing) list = list.map(function (b) { return b.id === existing.id ? record : b; });
  else list.unshift(record);

  // Oldest first out of the door.
  if (list.length > MAX_PER_PROJECT) {
    list = list.slice().sort(function (a, b) {
      return String(b.savedAt || '').localeCompare(String(a.savedAt || ''));
    }).slice(0, MAX_PER_PROJECT);
  }
  doc[pid] = list;
  if (!write(doc)) throw new Error('Could not save — this browser\'s storage is full.');
  return { record: record, replaced: !!existing };
}

function renameBatch(projectId, batchId, name) {
  var pid = String(projectId || '');
  var clean = String(name || '').trim();
  if (!clean) throw new Error('Give the batch a name.');
  var doc = read();
  var list = Array.isArray(doc[pid]) ? doc[pid] : [];
  var clash = list.filter(function (b) {
    return b.id !== batchId && String(b.name || '').toLowerCase() === clean.toLowerCase();
  })[0];
  if (clash) throw new Error('Another batch is already called "' + clean + '".');
  var hit = null;
  doc[pid] = list.map(function (b) {
    if (b.id !== batchId) return b;
    hit = Object.assign({}, b, { name: clean });
    return hit;
  });
  if (!hit) throw new Error('That batch no longer exists.');
  write(doc);
  return hit;
}

function deleteBatch(projectId, batchId) {
  var pid = String(projectId || '');
  var doc = read();
  var list = Array.isArray(doc[pid]) ? doc[pid] : [];
  var before = list.length;
  doc[pid] = list.filter(function (b) { return b.id !== batchId; });
  write(doc);
  return doc[pid].length < before;
}

/* Deleting a project should not leave its batches behind forever. */
function deleteProjectBatches(projectId) {
  var doc = read();
  delete doc[String(projectId || '')];
  write(doc);
}

/* ── loading ──────────────────────────────────────────────────────────────
   The interesting half. A batch holds jobIds; the jobs live elsewhere and may
   have changed or gone. resolveBatch rebuilds real groups from the CURRENT
   jobs and reports exactly what it could not find, so the caller can tell the
   user instead of loading steps that will fail at run time. ─────────────── */

/**
 * @param batch   a record from batchesFor()
 * @param jobs    the live job list (cygenix_jobs)
 * @param toStep  (job, savedStep) => step  — the page's own step builder, so
 *                a loaded step is identical to one added by hand
 * @returns { groups, missing:[{name, jobId}], restored, total }
 */
function resolveBatch(batch, jobs, toStep) {
  var byId = {};
  (jobs || []).forEach(function (j) { if (j && j.id) byId[j.id] = j; });

  var missing = [];
  var restored = 0;
  var groups = (batch && batch.groups || []).map(function (g) {
    var steps = [];
    (g.steps || []).forEach(function (s) {
      var job = byId[s.jobId];
      if (!job) {
        // Named, not just counted: "2 jobs are missing" is not actionable,
        // "Load addresses is missing" is.
        missing.push({ name: s.name || s.jobId || 'unnamed job', jobId: s.jobId || '' });
        return;
      }
      var step = toStep ? toStep(job, s) : Object.assign({}, s);
      if (step) {
        if (s.enabled === false) step.enabled = false;
        steps.push(step);
        restored++;
      }
    });
    return {
      id: g.id,
      name: g.name || 'Group',
      executionMode: g.executionMode === 'parallel' ? 'parallel' : 'sequential',
      collapsed: false,
      steps: steps,
    };
  });

  // A group that lost every one of its jobs is an empty box on screen with no
  // way to tell why. Drop it, unless dropping would leave nothing at all —
  // the screen needs one group to add jobs into.
  var kept = groups.filter(function (g) { return g.steps.length > 0; });
  if (!kept.length) kept = groups.slice(0, 1);

  return {
    groups: kept,
    missing: missing,
    restored: restored,
    total: restored + missing.length,
  };
}

/** One line describing what a load actually did. */
function loadSummary(res, batchName) {
  var name = batchName ? '"' + batchName + '"' : 'Batch';
  if (!res.total) return name + ' has no jobs in it.';
  if (!res.missing.length) {
    return name + ' loaded — ' + res.restored + ' job' + (res.restored === 1 ? '' : 's')
      + ' in ' + res.groups.length + ' group' + (res.groups.length === 1 ? '' : 's') + '.';
  }
  var names = res.missing.map(function (m) { return m.name; });
  var shown = names.slice(0, 3).join(', ');
  var more = names.length > 3 ? ' and ' + (names.length - 3) + ' more' : '';
  return name + ' loaded with ' + res.restored + ' of ' + res.total + ' jobs. '
    + 'Skipped ' + names.length + ' that no longer exist: ' + shown + more + '.';
}

/* Does this batch match what is on screen right now? Used to mark the batch
   the user is currently looking at, so Load does not present the arrangement
   they already have as something to switch to. */
function sameArrangement(batchGroups, groups) {
  var a = JSON.stringify(snapshotGroups(batchGroups || []));
  var b = JSON.stringify(snapshotGroups(groups || []));
  return a === b;
}

return {
  STORE_KEY: STORE_KEY,
  MAX_PER_PROJECT: MAX_PER_PROJECT,
  batchesFor: batchesFor,
  getBatch: getBatch,
  saveBatch: saveBatch,
  renameBatch: renameBatch,
  deleteBatch: deleteBatch,
  deleteProjectBatches: deleteProjectBatches,
  resolveBatch: resolveBatch,
  loadSummary: loadSummary,
  sameArrangement: sameArrangement,
  snapshotGroups: snapshotGroups,
  countSteps: countSteps,
};
});
