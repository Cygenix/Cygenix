/* ============================================================================
   cygenix-template-model.js
   Conversion Template — data model and pure logic (Phase 1)

   Repo path: public/cygenix-template-model.js

   WHAT THIS IS
   A Conversion Template is a cut-down copy of the target (Elite 3E) schema.
   Each Configurator module (AP, Matters, WIP ...) maps to a small set of real
   target tables. The client builds a staging database to that shape and hands
   it over as a source connection, so Cygenix can run the same standard import
   scripts for every client with no per-conversion rewriting.

   This file holds ONLY the shape of a template and the rules about it.
   No DOM. No network. No storage. That keeps it testable and keeps the page,
   the API and the future script generator all agreeing on one structure.

   Loaded as a classic script (same pattern as cygenix-effort-model.js):
       <script src="/cygenix-template-model.js?v=..."></script>
   Exposes: window.CygenixTemplateModel
   ========================================================================== */

(function (global) {
  'use strict';

  /* --------------------------------------------------------------------
     Constants
     ------------------------------------------------------------------ */

  // Bump only when the stored document shape changes in a way that needs
  // migrating. This is NOT the user-facing template version number.
  var TM_SCHEMA_VERSION = 1;

  var TM_TARGET_TYPES = ['Elite 3E'];

  // How we decide a Configurator module is in scope for the template.
  //   'any'     - module counts if ANY use case is ticked for it
  //   'scripts' - module counts only if the Script development row is ticked
  var TM_SCOPE_MODES = { ANY: 'any', SCRIPTS: 'scripts' };
  var TM_DEFAULT_SCOPE_MODE = TM_SCOPE_MODES.ANY;

  // The use case id used by TM_SCOPE_MODES.SCRIPTS. Matches the id in
  // CygenixEffortModel.EM_USE_CASES.
  var TM_SCRIPTS_USE_CASE_ID = 'scripts';

  var TM_STATUS = { DRAFT: 'draft', PUBLISHED: 'published' };

  // Default prefix for staging table names. Client-facing, so keep it short.
  var TM_STAGING_PREFIX = 'STG_';

  /* --------------------------------------------------------------------
     Small helpers
     ------------------------------------------------------------------ */

  function tmNow() { return new Date().toISOString(); }

  function tmId(prefix) {
    var rnd = Math.random().toString(36).slice(2, 10);
    return (prefix || 'tm') + '_' + Date.now().toString(36) + '_' + rnd;
  }

  function tmTrim(v) { return (v == null ? '' : String(v)).trim(); }

  function tmSameName(a, b) {
    return tmTrim(a).toLowerCase() === tmTrim(b).toLowerCase();
  }

  /* --------------------------------------------------------------------
     Scope — read straight from a saved Configurator estimate

     An estimate document (CygenixEffortModel.emNewDoc()) carries:
       doc.modules  ['Addresses','AP','AP Master', ...]
       doc.ticks    { 'analysis|Addresses': true, 'scripts|Matters': true, ... }
     Tick keys are `${useCaseId}|${moduleName}`.
     ------------------------------------------------------------------ */

  /**
   * Return the module names that are in scope in a Configurator estimate.
   * @param {object} estimateDoc effort-model document
   * @param {string} [mode] TM_SCOPE_MODES value
   * @returns {string[]} module names, in the estimate's own order
   */
  function tmScopeFromEstimate(estimateDoc, mode) {
    var doc = estimateDoc || {};
    var modules = Array.isArray(doc.modules) ? doc.modules : [];
    var ticks = doc.ticks || {};
    var useScripts = (mode || TM_DEFAULT_SCOPE_MODE) === TM_SCOPE_MODES.SCRIPTS;

    return modules.filter(function (m) {
      if (useScripts) return !!ticks[TM_SCRIPTS_USE_CASE_ID + '|' + m];
      // 'any' mode: one tick anywhere in that module's column is enough
      for (var k in ticks) {
        if (!Object.prototype.hasOwnProperty.call(ticks, k)) continue;
        if (!ticks[k]) continue;
        var bar = k.indexOf('|');
        if (bar > -1 && k.slice(bar + 1) === m) return true;
      }
      return false;
    });
  }

  /* --------------------------------------------------------------------
     Document shape
     ------------------------------------------------------------------ */

  /**
   * A table entry inside a module.
   * columns is left empty in Phase 1 — column detail is read live from the
   * target database when the specification is generated (Phase 2), so a
   * template stays correct across 3E versions and sites with custom fields.
   */
  function tmNewTable(opts) {
    var o = opts || {};
    var target = tmTrim(o.targetTable);
    return {
      id: tmId('tbl'),
      targetTable: target,                       // real 3E table, e.g. 'VchrDetail'
      stagingTable: tmTrim(o.stagingTable) || tmStagingTableName(target),
      required: o.required !== false,            // default true
      loadOrder: typeof o.loadOrder === 'number' ? o.loadOrder : 0,
      notes: tmTrim(o.notes),
      columns: Array.isArray(o.columns) ? o.columns : []
    };
  }

  function tmNewModule(name) {
    return {
      module: tmTrim(name),
      inScope: true,          // set false when the module leaves the estimate
      tables: [],
      notes: ''
    };
  }

  /**
   * Create an empty template document.
   * @param {object} opts {name, projectId, profileId, estimateId, targetType, scopeMode, createdBy}
   */
  function tmNewTemplate(opts) {
    var o = opts || {};
    return {
      id: tmId('tpl'),
      schema: TM_SCHEMA_VERSION,
      name: tmTrim(o.name) || 'Conversion Template',
      version: 1,                                 // user-facing version
      status: TM_STATUS.DRAFT,
      targetType: tmTrim(o.targetType) || TM_TARGET_TYPES[0],

      projectId: tmTrim(o.projectId),
      profileId: tmTrim(o.profileId),             // template belongs to a profile
      estimateId: tmTrim(o.estimateId),           // Configurator estimate driving scope
      scopeMode: o.scopeMode || TM_DEFAULT_SCOPE_MODE,
      stagingPrefix: tmTrim(o.stagingPrefix) || TM_STAGING_PREFIX,

      modules: [],                                // tmNewModule entries

      createdAt: tmNow(),
      createdBy: tmTrim(o.createdBy),
      updatedAt: tmNow(),
      updatedBy: tmTrim(o.createdBy),
      publishedAt: '',
      publishedBy: ''
    };
  }

  function tmStagingTableName(targetTable, prefix) {
    var t = tmTrim(targetTable).replace(/[^A-Za-z0-9_]/g, '_');
    return (prefix || TM_STAGING_PREFIX) + t;
  }

  /* --------------------------------------------------------------------
     Mutators — always return the template, never mutate a frozen one
     ------------------------------------------------------------------ */

  function tmFindModule(tpl, moduleName) {
    var mods = (tpl && tpl.modules) || [];
    for (var i = 0; i < mods.length; i++) {
      if (tmSameName(mods[i].module, moduleName)) return mods[i];
    }
    return null;
  }

  function tmTouch(tpl, who) {
    tpl.updatedAt = tmNow();
    if (who) tpl.updatedBy = tmTrim(who);
    return tpl;
  }

  /**
   * Bring a template's module list into line with the Configurator scope.
   * Modules new to the estimate are added. Modules dropped from the estimate
   * are marked inScope:false and KEPT, so no table work is ever lost.
   * @returns {object} {added:string[], removed:string[], restored:string[]}
   */
  function tmSyncScope(tpl, scopeModules, who) {
    var scope = Array.isArray(scopeModules) ? scopeModules : [];
    var result = { added: [], removed: [], restored: [] };
    if (!tpl) return result;
    tpl.modules = Array.isArray(tpl.modules) ? tpl.modules : [];

    scope.forEach(function (name) {
      var existing = tmFindModule(tpl, name);
      if (!existing) {
        tpl.modules.push(tmNewModule(name));
        result.added.push(name);
      } else if (existing.inScope === false) {
        existing.inScope = true;
        result.restored.push(name);
      }
    });

    tpl.modules.forEach(function (m) {
      var stillIn = scope.some(function (s) { return tmSameName(s, m.module); });
      if (!stillIn && m.inScope !== false) {
        m.inScope = false;
        result.removed.push(m.module);
      }
    });

    // Keep the estimate's order for in-scope modules, dropped ones last.
    tpl.modules.sort(function (a, b) {
      if (a.inScope !== b.inScope) return a.inScope === false ? 1 : -1;
      var ia = scope.findIndex(function (s) { return tmSameName(s, a.module); });
      var ib = scope.findIndex(function (s) { return tmSameName(s, b.module); });
      if (ia === -1) ia = 9999;
      if (ib === -1) ib = 9999;
      return ia - ib;
    });

    return tmTouch(tpl, who), result;
  }

  function tmAddTable(tpl, moduleName, tableOpts, who) {
    var mod = tmFindModule(tpl, moduleName);
    if (!mod) return null;
    var t = tmNewTable(tableOpts);
    if (!t.targetTable) return null;
    var dupe = mod.tables.some(function (x) { return tmSameName(x.targetTable, t.targetTable); });
    if (dupe) return null;
    if (!t.loadOrder) t.loadOrder = mod.tables.length + 1;
    if (tpl.stagingPrefix) t.stagingTable = tmStagingTableName(t.targetTable, tpl.stagingPrefix);
    mod.tables.push(t);
    tmTouch(tpl, who);
    return t;
  }

  function tmUpdateTable(tpl, moduleName, tableId, patch, who) {
    var mod = tmFindModule(tpl, moduleName);
    if (!mod) return null;
    for (var i = 0; i < mod.tables.length; i++) {
      if (mod.tables[i].id !== tableId) continue;
      var t = mod.tables[i];
      if (patch && typeof patch === 'object') {
        if ('targetTable' in patch) {
          t.targetTable = tmTrim(patch.targetTable);
          t.stagingTable = tmStagingTableName(t.targetTable, tpl.stagingPrefix);
        }
        if ('stagingTable' in patch) t.stagingTable = tmTrim(patch.stagingTable);
        if ('required' in patch) t.required = !!patch.required;
        if ('loadOrder' in patch) t.loadOrder = Number(patch.loadOrder) || 0;
        if ('notes' in patch) t.notes = tmTrim(patch.notes);
        if ('columns' in patch && Array.isArray(patch.columns)) t.columns = patch.columns;
      }
      tmTouch(tpl, who);
      return t;
    }
    return null;
  }

  function tmRemoveTable(tpl, moduleName, tableId, who) {
    var mod = tmFindModule(tpl, moduleName);
    if (!mod) return false;
    var before = mod.tables.length;
    mod.tables = mod.tables.filter(function (t) { return t.id !== tableId; });
    if (mod.tables.length === before) return false;
    tmTouch(tpl, who);
    return true;
  }

  function tmSetModuleNotes(tpl, moduleName, notes, who) {
    var mod = tmFindModule(tpl, moduleName);
    if (!mod) return false;
    mod.notes = tmTrim(notes);
    tmTouch(tpl, who);
    return true;
  }

  /* --------------------------------------------------------------------
     Validation and publishing
     ------------------------------------------------------------------ */

  /**
   * Check a template is fit to publish.
   * @returns {Array} [{level:'error'|'warning', module, message}]
   */
  function tmValidate(tpl) {
    var issues = [];
    if (!tpl) return [{ level: 'error', module: '', message: 'No template loaded.' }];

    if (!tmTrim(tpl.name)) {
      issues.push({ level: 'error', module: '', message: 'Template needs a name.' });
    }
    if (!tmTrim(tpl.profileId)) {
      issues.push({ level: 'error', module: '', message: 'Template is not linked to a connection profile.' });
    }

    var inScope = (tpl.modules || []).filter(function (m) { return m.inScope !== false; });
    if (!inScope.length) {
      issues.push({
        level: 'error', module: '',
        message: 'No modules in scope. Tick modules in Project › Configurator first.'
      });
    }

    inScope.forEach(function (m) {
      if (!m.tables || !m.tables.length) {
        issues.push({ level: 'error', module: m.module, message: 'No target tables chosen for this module.' });
        return;
      }
      var seen = {};
      m.tables.forEach(function (t) {
        var key = tmTrim(t.targetTable).toLowerCase();
        if (!key) {
          issues.push({ level: 'error', module: m.module, message: 'A table entry has no target table name.' });
          return;
        }
        if (seen[key]) {
          issues.push({ level: 'error', module: m.module, message: 'Target table "' + t.targetTable + '" is listed twice.' });
        }
        seen[key] = true;
        if (!tmTrim(t.stagingTable)) {
          issues.push({ level: 'error', module: m.module, message: 'Table "' + t.targetTable + '" has no staging table name.' });
        }
      });
    });

    var dropped = (tpl.modules || []).filter(function (m) { return m.inScope === false && m.tables && m.tables.length; });
    dropped.forEach(function (m) {
      issues.push({
        level: 'warning', module: m.module,
        message: 'Module is no longer in the Configurator scope but still has tables defined. It will not be published.'
      });
    });

    return issues;
  }

  function tmCanPublish(tpl) {
    return !tmValidate(tpl).some(function (i) { return i.level === 'error'; });
  }

  /**
   * Publish: freeze the current content as a numbered version.
   * Returns a NEW published document; the caller stores it alongside history.
   */
  function tmPublish(tpl, who) {
    if (!tmCanPublish(tpl)) return null;
    var copy = JSON.parse(JSON.stringify(tpl));
    copy.status = TM_STATUS.PUBLISHED;
    copy.publishedAt = tmNow();
    copy.publishedBy = tmTrim(who);
    copy.updatedAt = copy.publishedAt;
    copy.updatedBy = copy.publishedBy;
    return copy;
  }

  /** Start a new draft from a published template (version + 1). */
  function tmNewDraftFrom(tpl, who) {
    var copy = JSON.parse(JSON.stringify(tpl || {}));
    copy.id = tmId('tpl');
    copy.version = (Number(tpl && tpl.version) || 1) + 1;
    copy.status = TM_STATUS.DRAFT;
    copy.publishedAt = '';
    copy.publishedBy = '';
    copy.createdAt = tmNow();
    copy.createdBy = tmTrim(who);
    return tmTouch(copy, who);
  }

  /* --------------------------------------------------------------------
     Summary — for the page header and for the Assistant
     ------------------------------------------------------------------ */

  function tmSummary(tpl) {
    var mods = (tpl && tpl.modules) || [];
    var inScope = mods.filter(function (m) { return m.inScope !== false; });
    var tables = 0;
    inScope.forEach(function (m) { tables += (m.tables || []).length; });
    var empty = inScope.filter(function (m) { return !(m.tables || []).length; }).length;
    return {
      name: (tpl && tpl.name) || '',
      version: (tpl && tpl.version) || 0,
      status: (tpl && tpl.status) || TM_STATUS.DRAFT,
      moduleCount: inScope.length,
      tableCount: tables,
      modulesWithoutTables: empty,
      outOfScopeCount: mods.length - inScope.length
    };
  }

  /* --------------------------------------------------------------------
     Export
     ------------------------------------------------------------------ */

  var api = {
    TM_SCHEMA_VERSION: TM_SCHEMA_VERSION,
    TM_TARGET_TYPES: TM_TARGET_TYPES,
    TM_SCOPE_MODES: TM_SCOPE_MODES,
    TM_DEFAULT_SCOPE_MODE: TM_DEFAULT_SCOPE_MODE,
    TM_SCRIPTS_USE_CASE_ID: TM_SCRIPTS_USE_CASE_ID,
    TM_STATUS: TM_STATUS,
    TM_STAGING_PREFIX: TM_STAGING_PREFIX,

    tmId: tmId,
    tmScopeFromEstimate: tmScopeFromEstimate,
    tmNewTemplate: tmNewTemplate,
    tmNewModule: tmNewModule,
    tmNewTable: tmNewTable,
    tmStagingTableName: tmStagingTableName,
    tmFindModule: tmFindModule,
    tmSyncScope: tmSyncScope,
    tmAddTable: tmAddTable,
    tmUpdateTable: tmUpdateTable,
    tmRemoveTable: tmRemoveTable,
    tmSetModuleNotes: tmSetModuleNotes,
    tmValidate: tmValidate,
    tmCanPublish: tmCanPublish,
    tmPublish: tmPublish,
    tmNewDraftFrom: tmNewDraftFrom,
    tmSummary: tmSummary
  };

  global.CygenixTemplateModel = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;

})(typeof window !== 'undefined' ? window : globalThis);