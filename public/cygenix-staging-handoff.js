/* ============================================================================
   cygenix-staging-handoff.js — after a Dev Console staging build: the maps
   that load each staging table into its target table, ready to run
   ----------------------------------------------------------------------------
   Oct-2026. A staging session leaves a schema full of tables that already
   have the target's shape — the target's column names and types — loaded
   from the source. The last step, moving them into the target, was by hand:
   open each Conversion Template draft in Object Mapping, point it at the new
   schema, map the columns (one-to-one, every time), generate the SQL, save.
   For thirty tables that is an afternoon of clicking the same buttons.

   This file decides, without touching anything, what one "Load into target"
   would do for a template and a staging schema. The Dev Console page then
   writes the jobs and has Object Mapping generate each one's SQL in a hidden
   frame (?autosave=1, the same road the Jobs page's Bulk Generate SQL takes),
   so the result is ordinary jobs, ready to run in Task Agent.

   WHAT EACH TEMPLATE TABLE BECOMES
   For every table of every module ticked Include on the template:
   - not built      the session did not build it in the schema: nothing.
   - create         no map loads this table yet: a new draft, stamped with the
                    template (as Conversion Templates' own send does), whose
                    source is <schema>.<staging table>.
   - repoint        the template's own draft for this table exists and nobody
                    has worked on it (no columns, no SQL): its source moves to
                    the new schema.
   - regenerate     the template's draft already reads from this schema: its
                    SQL is generated again.
   - keep           somebody has worked on the map, or made one by hand for
                    this target, and it reads from somewhere else: it is NOT
                    changed, and the reason is said. Work a person did is never
                    overwritten by a button.
   The hundred-job cap every writer of cygenix_jobs keeps is respected the
   way Conversion Templates respects it: if the new jobs would push live maps
   off the end of the list, nothing is written, and the numbers are given.

   No DOM, no storage, no network. Node-requirable; tested in
   tests/staging-handoff.test.js.
   ========================================================================== */
(function (root, factory) {
  var api = factory(root);
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixStagingHandoff) root.CygenixStagingHandoff = api;
})(typeof globalThis !== 'undefined' ? globalThis : (typeof window !== 'undefined' ? window : this), function (root) {
'use strict';

function str(v) { return v == null ? '' : String(v); }
function trim(v) { return str(v).trim(); }
function lower(v) { return trim(v).toLowerCase(); }
function TMap() {
  if (root && root.CygenixTemplateMapping) return root.CygenixTemplateMapping;
  if (typeof require === 'function') return require('./cygenix-template-mapping.js');
  throw new Error('cygenix-template-mapping.js is not loaded');
}

function moduleActive(m) { return !!m && m.inScope !== false && !!m.included; }
function isLive(j) { return !!j && !j._deleted; }
function inProject(j, projectId) { return trim(j && j.projectId) === trim(projectId); }
// A map nobody has worked on: no columns mapped, no SQL.
function untouched(j) {
  return !(Array.isArray(j.columnMapping) && j.columnMapping.length) && !trim(j.insertSQL);
}
function sourceOf(j) { return trim(j && (j.sourceTable || j.source)); }
function targetOf(j) { return trim(j && (j.targetTable || j.target)); }
function bare(name) { var s = trim(name); var i = s.lastIndexOf('.'); return lower(i >= 0 ? s.slice(i + 1) : s); }

/* plan(tpl, jobs, opts) → { rows, counts, refused?, why? }
   opts: { stagingSchema, projectId, built: [staging table names in the
           schema] | null (unknown: assume all built), targetSchemaOf(name),
           cap } */
function plan(tpl, jobs, opts) {
  var o = opts || {};
  var TM = TMap();
  var schema = trim(o.stagingSchema);
  var list = jobs || [];
  var live = list.filter(isLive);
  var mine = live.filter(function (j) { return inProject(j, o.projectId); });
  var built = null;
  if (Array.isArray(o.built)) { built = {}; o.built.forEach(function (n) { built[bare(n)] = true; }); }
  var rows = [];
  ((tpl && tpl.modules) || []).filter(moduleActive).forEach(function (m) {
    TM.modulePairs(tpl, m.module, { stagingSchema: schema, targetSchemaOf: o.targetSchemaOf }).forEach(function (p) {
      var row = { module: p.module, tableId: p.tableId, stagingTable: p.stagingTable, targetTable: p.targetTable,
        source: p.source, target: p.target, loadOrder: p.loadOrder, pair: p };
      if (built && !built[lower(p.stagingTable)]) { row.action = 'not-built'; row.why = 'Not built in "' + schema + '".'; rows.push(row); return; }
      // The template's own map for this table, if there is one.
      var own = mine.filter(function (j) {
        return TM.isFromTemplate(j, tpl.id) && trim(j.fromTemplate.tableId) === trim(p.tableId);
      })[0] || null;
      if (own) {
        row.jobId = own.id; row.jobName = own.name;
        if (lower(sourceOf(own)) === lower(p.source)) { row.action = 'regenerate'; row.why = 'Already reads from "' + schema + '"; its SQL is generated again.'; }
        else if (untouched(own)) { row.action = 'repoint'; row.why = 'Moves from ' + (sourceOf(own) || 'no source') + ' to ' + p.source + '.'; row.from = sourceOf(own); }
        else { row.action = 'keep'; row.why = 'Mapped by hand from ' + sourceOf(own) + ' — left as it is.'; }
        rows.push(row); return;
      }
      // A map somebody made for this target, by hand or from another template.
      var other = mine.filter(function (j) { return bare(targetOf(j)) === lower(p.targetTable); })[0] || null;
      if (other) {
        row.jobId = other.id; row.jobName = other.name;
        if (lower(sourceOf(other)) === lower(p.source)) { row.action = 'regenerate'; row.why = 'Your map "' + other.name + '" already reads from "' + schema + '"; its SQL is generated again.'; }
        else { row.action = 'keep'; row.why = 'Your map "' + other.name + '" loads this table from ' + (sourceOf(other) || 'elsewhere') + ' — left as it is.'; }
        rows.push(row); return;
      }
      row.action = 'create'; row.why = 'A new map from ' + p.source + '.';
      rows.push(row);
    });
  });
  var counts = { create: 0, repoint: 0, regenerate: 0, keep: 0, 'not-built': 0 };
  rows.forEach(function (r) { counts[r.action]++; });
  var out = { rows: rows, counts: counts, schema: schema };
  // The cap, as Conversion Templates' send checks it: live maps everywhere,
  // and nothing live may fall off the end of the stored list.
  var cap = o.cap || TM.JOB_CAP || 100;
  var adding = counts.create;
  if (adding && live.length + adding > cap) {
    out.refused = true;
    out.why = 'There are ' + live.length + ' maps and this would add ' + adding + '; the job list holds ' + cap + ', so '
      + (live.length + adding - cap) + ' of your oldest maps would be lost. Delete maps you no longer need, or build fewer modules, then try again.';
  } else if (adding && list.length + adding > cap) {
    var fallsOff = list.slice(Math.max(0, cap - adding));
    if (fallsOff.some(isLive)) {
      out.refused = true;
      out.why = 'Adding ' + adding + ' maps would push ' + fallsOff.filter(isLive).length + ' of your older maps off the end of the job list. Empty the bin of deleted maps, or build fewer modules, then try again.';
    }
  }
  return out;
}

/* apply(tpl, jobs, planned, opts) → { jobs, toGenerate: [job ids], created, repointed }
   Pure: returns the new list; the caller writes it. New maps go first, as
   every other writer of cygenix_jobs puts them. */
function apply(tpl, jobs, planned, opts) {
  var o = opts || {};
  if (!planned || planned.refused) return { jobs: (jobs || []).slice(), toGenerate: [], created: 0, repointed: 0 };
  var TM = TMap();
  var now = o.now || Date.now();
  var list = (jobs || []).map(function (j) { return j; });
  var byId = {};
  list.forEach(function (j, i) { if (j && j.id) byId[j.id] = i; });
  var created = [], toGenerate = [], repointed = 0, n = 0;
  planned.rows.forEach(function (r) {
    if (r.action === 'create') {
      var job = TM.buildJob(tpl, r.pair, { projectId: o.projectId, by: o.by, now: now + (n++),
        id: 'job_stg_' + (now + n) + '_' + Math.random().toString(36).slice(2, 8) });
      job.fromTemplate.stagingSchema = planned.schema;
      created.push(job); toGenerate.push(job.id);
    } else if (r.action === 'repoint' && r.jobId in byId) {
      var i = byId[r.jobId];
      var j = Object.assign({}, list[i]);
      j.source = r.source; j.sourceTable = r.source;
      j.fromTemplate = Object.assign({}, j.fromTemplate, { stagingSchema: planned.schema, repointedAt: new Date(now).toISOString() });
      list[i] = j; repointed++; toGenerate.push(j.id);
    } else if (r.action === 'regenerate' && r.jobId) {
      toGenerate.push(r.jobId);
    }
  });
  // In load order, so the jobs are generated — and listed — parents first.
  var order = {};
  planned.rows.forEach(function (r) { if (r.jobId) order[r.jobId] = r.loadOrder; });
  created.forEach(function (j, k) { order[j.id] = planned.rows.filter(function (r) { return r.action === 'create'; })[k].loadOrder; });
  toGenerate.sort(function (a, b) { return (order[a] || 0) - (order[b] || 0); });
  return { jobs: created.concat(list), toGenerate: toGenerate, created: created.length, repointed: repointed };
}

// A one-line summary of a plan, for the confirmation.
function describe(planned) {
  var c = planned.counts;
  var bits = [];
  if (c.create) bits.push(c.create + ' new map' + (c.create === 1 ? '' : 's'));
  if (c.repoint) bits.push(c.repoint + ' template draft' + (c.repoint === 1 ? '' : 's') + ' pointed at "' + planned.schema + '"');
  if (c.regenerate) bits.push(c.regenerate + ' map' + (c.regenerate === 1 ? '' : 's') + ' regenerated');
  if (c.keep) bits.push(c.keep + ' left as they are');
  if (c['not-built']) bits.push(c['not-built'] + ' not built in the schema');
  return bits.join(', ');
}

return { plan: plan, apply: apply, describe: describe, untouched: untouched, moduleActive: moduleActive };
});
