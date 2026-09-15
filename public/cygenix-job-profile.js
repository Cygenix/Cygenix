/* ============================================================================
   cygenix-job-profile.js — which connection profile a job belongs to.
   ----------------------------------------------------------------------------
   THE PROBLEM

   Requested: show the connection profile on every row of the Migration Jobs
   list, and make sure a NEW job is linked to one when it is created.

   A job had no profile field at all. Nineteen places in twelve files mint a
   job — the dashboard's + New Migration, Object Mapping, Mapper, one-to-many,
   the SQL Editor, Project Builder, Connect, Insights, Data Enrichment, the
   Schema Explorer, the Data Analyser hand-off, job import and duplication,
   and the Agentive Migration flow on the server. Nineteen copies of "read the
   active profile and copy two fields off it" is nineteen chances to read it
   differently, and the one that drifts is the one nobody notices, because a
   wrong profile on a job looks exactly like a right one.

   So: one place that answers the question, used by all of them.

   ── The two questions, and why the answer is not one field ───────────────
   There are two facts about a job and a profile, and they are not the same:

     PROVENANCE   which profile was active when the job was made. Stamped
                  once, at creation, never rewritten. This is what
                  stamp() writes, as job.profileId + job.profileName.
     GOVERNANCE   which profile the job is PERMITTED to run against. That
                  already existed before this file: the profile store's
                  `bindings` list, written deliberately on the Profiles page,
                  audited, rebindable, and enforced at run time by
                  cpResolve() in cygenix-profiles.js.

   The jobs list has one column, so of() has to choose, and it prefers the
   BINDING: somebody binding a job is a deliberate act about where it runs,
   and it should outrank a stamp left behind by whichever profile happened to
   be selected on the day. The stamp is the fallback, and profileName the
   fallback after that — a profile can be deleted, and a job that then showed
   a blank would lose the only record of what it was built against.

   ── Why the id, and why the name as well ─────────────────────────────────
   A profile's `id` (FIN_3E_UAT) is immutable: cpSaveProfile refuses to change
   it. Its `name` is free text and renameable. So the id is the link and the
   name is a snapshot for display — rename a profile and the link survives;
   delete one and the row still says what it used to be.

   ── One reader, not a second engine ──────────────────────────────────────
   This file READS the profile store. It does not reimplement any of its
   rules: no validation, no status transitions, no resolution logic. The
   binding lookup matches cpBindingOf() exactly, and the test asserts the two
   agree on the same store rather than trusting that they do. Anything that
   needs to DECIDE something about a profile still goes to
   cygenix-profiles.js.

   Node-requirable, so the stamping and precedence rules are tested without a
   browser — which is also how the server half (azure-function/src/agent.js)
   can follow the same rules without importing a browser file.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixJobProfile) root.CygenixJobProfile = api;
})(typeof window !== 'undefined' ? window : this, function () {
'use strict';

var STORE_KEY = 'cygenix_profiles_v1';

/* The store, or null. Never throws: every caller here is on a creation path,
   and a job must be created whether or not the profile store can be read. */
function load() {
  try {
    if (typeof localStorage === 'undefined') return null;
    var raw = localStorage.getItem(STORE_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch (e) { return null; }
}

function profilesOf(store) {
  return (store && Array.isArray(store.profiles)) ? store.profiles : [];
}

/* The profile the session is pointed at — the one the sidebar pill shows.
   Status is checked rather than assumed: cpSelectProfile refuses to select a
   non-active profile, but a selected profile can be retired afterwards, and
   stamping a new job with a retired profile would record a link that could
   never run. */
function activeProfile(store) {
  var s = store || load();
  var id = s && s.settings ? s.settings.activeProfileId : null;
  if (!id) return null;
  var list = profilesOf(s);
  for (var i = 0; i < list.length; i++) {
    if (list[i] && list[i].id === id) return list[i].status === 'active' ? list[i] : null;
  }
  return null;
}

function byId(store, id) {
  if (!id) return null;
  var list = profilesOf(store);
  for (var i = 0; i < list.length; i++) if (list[i] && list[i].id === id) return list[i];
  return null;
}

/* Same filter as cpBindingOf() in cygenix-profiles.js. Duplicated as a read
   rather than imported because eight of the ten pages that create jobs do not
   load the governance engine and should not have to; tests/job-profile.test.js
   runs both against one store and fails if they ever disagree. */
function bindingOf(store, jobId) {
  var list = (store && Array.isArray(store.bindings)) ? store.bindings : [];
  for (var i = 0; i < list.length; i++) {
    var b = list[i];
    if (b && b.artifactType === 'job' && b.artifactId === jobId) return b;
  }
  return null;
}

/* ── Creation ────────────────────────────────────────────────────────────
   Stamp a NEW job with the profile that is active now.

   Never overwrites: a caller that already knows the profile (a duplicate
   carrying its original's, an import bringing its own) keeps it. That is also
   what makes this safe to call on a path that might run twice.

   Never blocks: no active profile means the job is still created, with the
   fields left off entirely rather than set to empty strings — an absent field
   reads as "before profiles" and an empty one reads as "we tried and failed",
   and the jobs list shows the same dash for both anyway. A console warning
   says which job it was, because the alternative is a silent gap that turns
   up weeks later as a row with no profile and no explanation. */
function stamp(job, store) {
  if (!job || typeof job !== 'object') return job;
  if (job.profileId) return job;

  var p = activeProfile(store);
  if (!p) {
    try {
      if (typeof console !== 'undefined' && console.warn) {
        console.warn('[job-profile] No active connection profile — job '
          + (job.id || '(unsaved)') + ' created without one. '
          + 'Select a profile on /profiles and new jobs will carry it.');
      }
    } catch (e) { /* a logger that throws must not stop a job being created */ }
    return job;
  }
  job.profileId = p.id;
  /* name defaults to the id when a profile was created without one, so this
     is never blank while profileId is set. */
  job.profileName = p.name || p.id;
  return job;
}

/* ── Creating, where the same call also SAVES an edit ────────────────────
   Most of the save handlers that mint a job also handle editing one, and
   they rebuild the whole object from the form rather than patching it — so
   an edited job arrives here with no profile even though the stored copy has
   one. stamp() alone would then quietly re-stamp it with whatever profile
   happens to be selected today, which is a change to an existing job and the
   opposite of a provenance record.

   attach() is what the save handlers call. It carries the stored profile
   forward when the job already exists, and stamps only when it is genuinely
   new. `prev` is the jobs array the handler already has in hand. */
function attach(job, prev, store) {
  if (!job || typeof job !== 'object') return job;
  if (job.profileId) return job;

  var list = Array.isArray(prev) ? prev : [];
  for (var i = 0; i < list.length; i++) {
    var old = list[i];
    if (old && old.id === job.id && old.profileId) {
      job.profileId = old.profileId;
      if (old.profileName) job.profileName = old.profileName;
      return job;
    }
  }
  return stamp(job, store);
}

/* ── Display ─────────────────────────────────────────────────────────────
   What the jobs list shows. Returns null when there is nothing to show, so
   the caller renders its own dash rather than this file inventing one.

     source: 'binding'  bound on the Profiles page — a deliberate act
             'job'      stamped at creation, profile still exists
             'stale'    stamped at creation, profile no longer exists
   `stale` is reported rather than hidden: a job whose profile has been
   deleted still ran against something, and the name it ran against is the
   last honest thing left to show. */
function of(job, store) {
  if (!job) return null;
  var s = store || load();

  var b = bindingOf(s, job.id);
  if (b && b.profileId) {
    var bp = byId(s, b.profileId);
    return { id: b.profileId, name: (bp && (bp.name || bp.id)) || b.profileId,
      envClass: bp ? bp.envClass : null, status: bp ? bp.status : null, source: 'binding' };
  }

  if (job.profileId) {
    var jp = byId(s, job.profileId);
    if (jp) {
      return { id: jp.id, name: jp.name || jp.id, envClass: jp.envClass,
        status: jp.status, source: 'job' };
    }
    return { id: job.profileId, name: job.profileName || job.profileId,
      envClass: null, status: null, source: 'stale' };
  }

  /* A job carrying only a name — an import from a machine whose profile ids
     mean nothing here. Better than a dash, and honestly labelled. */
  if (job.profileName) {
    return { id: null, name: job.profileName, envClass: null, status: null, source: 'stale' };
  }
  return null;
}

/* The label for a jobs-list cell, or '' for none. Kept here so nineteen
   creation points and one table agree on what a profile looks like. */
function label(job, store) {
  var r = of(job, store);
  return r ? r.name : '';
}

return {
  STORE_KEY: STORE_KEY,
  load: load,
  activeProfile: activeProfile,
  bindingOf: bindingOf,
  stamp: stamp,
  attach: attach,
  of: of,
  label: label,
};
});
