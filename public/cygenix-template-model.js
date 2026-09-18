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
  //
  // 2 (Sep-2026): every table row may carry `columns` (the target's column
  // detail, as read from the target database) and `columnsFetchedAt`. A v1
  // document is forward-migrated by tmMigrate: `columns` was already on the
  // shape and already `[]`, so the migration adds nothing to a table — it
  // only stamps the schema number. Nothing is required, so a v1 draft that
  // has never been near this code loads and renders exactly as before.
  //
  // 3 (Sep-2026): every module may carry two operator flags, `excluded` and
  // `mapped`, written explicitly rather than left undefined so that no reader
  // has to guess what an absent flag meant.
  //
  // 4 (Sep-2026): `excluded` becomes `included`, and the decision becomes
  // OPT-IN. A module is in the publish only because somebody ticked it. Both
  // module ticks now behave the same way: off until asked for.
  //
  // The migration does NOT carry `!excluded` across. Under schema 3 every
  // module was included unless somebody said otherwise, so translating it
  // faithfully would tick every module on every existing template and leave
  // the operator unticking twenty-four boxes to get back to the handful they
  // wanted. Instead every module starts NOT included, and the operator ticks
  // the ones this version ships. That also fails the safe way: the worst case
  // is a publish that is blocked until somebody chooses, rather than a
  // workbook that quietly carries modules nobody asked for to a client.
  var TM_SCHEMA_VERSION = 4;

  // The persisted column record. Named here so the page, the specification
  // builder and the tests agree on one shape, and so a reader can see what a
  // stored snapshot contains without opening the spec builder.
  var TM_COLUMN_FIELDS = ['name', 'ordinal', 'dataType', 'maxLength', 'precision', 'scale',
    'isNullable', 'isIdentity', 'isComputed', 'isPrimaryKey', 'defaultDefinition'];

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
      notes: '',
      /* Two operator decisions, added Sep-2026. Both are opt-in: they start
         off and only a person turns them on, which is why both read the same
         way round on screen and why neither can happen by accident.

           included  this module is in the publish: the specification
                     workbook, the staging DDL, Create staging tables and the
                     readiness check. A module that is in scope but not
                     included keeps its tables and is simply left out. "Not
                     this time", not "not at all".
           mapped    this module's staging→target pairs have been sent to
                     Object Mapping as draft jobs. The flag is the record of
                     the decision; the jobs themselves live in cygenix_jobs
                     and carry a tag pointing back here.

         Being in scope is not the same as being in the publish. Scope comes
         from the Configurator and says what the conversion covers; this says
         what THIS version of the template ships. Silence means not included,
         which is the safe way round: a module nobody ticked is left out of a
         client's workbook rather than put into it unasked. */
      included: false,
      mapped: false
    };
  }

  /* ── The two module flags ───────────────────────────────────────────────
     Read through helpers rather than touched directly, because "is this
     module in play" is asked by five callers and they must not each decide
     it for themselves. A module that has dropped out of scope is never in
     play whatever its flags say — that is the older rule and it wins. */
  function tmModuleIncluded(m) { return !!(m && m.included); }
  function tmModuleActive(m) { return !!m && m.inScope !== false && !!m.included; }
  /* Every module that publishing, the workbook, the DDL and the readiness
     check should consider. One definition, one place. */
  function tmActiveModules(tpl) {
    return ((tpl && tpl.modules) || []).filter(tmModuleActive);
  }
  function tmSetModuleIncluded(tpl, moduleName, included, who) {
    var m = tmFindModule(tpl, moduleName);
    if (!m || m.inScope === false) return null;   // out of scope has no ticks
    m.included = !!included;
    tmTouch(tpl, who);
    return m;
  }
  function tmSetModuleMapped(tpl, moduleName, mapped, who) {
    var m = tmFindModule(tpl, moduleName);
    if (!m || m.inScope === false) return null;
    m.mapped = !!mapped;
    tmTouch(tpl, who);
    return m;
  }
  /* The modules in scope that this version leaves out. Named for the fact,
     not for the flag: what a reader of a published template wants to know is
     what is missing from it, and that question has the same answer whichever
     way round the tick is stored. */
  function tmExcludedModules(tpl) {
    return ((tpl && tpl.modules) || [])
      .filter(function (m) { return m.inScope !== false && !m.included; })
      .map(function (m) { return m.module; });
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

  /* --------------------------------------------------------------------
     Column detail (schema 2)

     A table's `columns` is a SNAPSHOT of the target's shape, taken when
     somebody pressed Refresh columns or published. It is stored rather than
     re-read on demand for one reason: regenerating the specification for a
     published version next year has to produce THAT version's schema, not
     whatever the target looks like by then. A published document is the
     record of what the client was asked to build.

     Everything here is normalisation and counting. Reading the target is the
     page's job (cygenix-template-spec.js); nothing in this file touches a
     network or a DOM.
     ------------------------------------------------------------------ */

  /* One column, from whatever the schema reader handed back, in the shape
     TM_COLUMN_FIELDS describes. The reader's own field names differ between
     backends and have grown over time, so every one of them is accepted and
     the record written here is the single shape everything downstream reads. */
  function tmNormaliseColumn(raw, index, primaryKeys) {
    var c = raw || {};
    var pk = Array.isArray(primaryKeys) ? primaryKeys : [];
    var name = tmTrim(c.name || c.COLUMN_NAME || c.column_name);
    var base = tmTrim(c.baseType || c.dataType || c.type || c.DATA_TYPE).toLowerCase();
    // `type` arrives assembled — NVARCHAR(64) — and baseType is the bare
    // word. When only the assembled form is present, the bare word is
    // whatever precedes the bracket; the parts stay null rather than being
    // guessed out of the string.
    var bare = base.indexOf('(') > 0 ? base.slice(0, base.indexOf('(')) : base;
    var num = function (v) { return (v === null || v === undefined || v === '') ? null : Number(v); };
    return {
      name: name,
      ordinal: typeof c.ordinal === 'number' ? c.ordinal : (typeof c.ORDINAL_POSITION === 'number' ? c.ORDINAL_POSITION : index + 1),
      dataType: bare,
      maxLength: num(c.maxLength !== undefined ? c.maxLength : c.CHARACTER_MAXIMUM_LENGTH),
      precision: num(c.precision !== undefined ? c.precision : c.NUMERIC_PRECISION),
      scale: num(c.scale !== undefined ? c.scale : c.NUMERIC_SCALE),
      // nullable is the reader's word; isNullable is ours. Absent means
      // nullable, which is the permissive answer and the safe one for a
      // specification: it never tells a client a column is optional when the
      // target says otherwise, because the target said nothing.
      isNullable: c.isNullable !== undefined ? !!c.isNullable : (c.nullable !== undefined ? !!c.nullable : true),
      isIdentity: !!(c.isIdentity || c.is_identity),
      isComputed: !!(c.isComputed || c.is_computed),
      isPrimaryKey: c.isPrimaryKey !== undefined ? !!c.isPrimaryKey
        : pk.some(function (k) { return tmSameName(k, name); }),
      defaultDefinition: tmTrim(c.defaultDefinition !== undefined ? c.defaultDefinition : c['default']),
    };
  }

  /* Replace a table's column snapshot. Sorted by ordinal, stamped, touched.
     An empty array is a legitimate answer — a table that exists with no
     readable columns — and is stored as such; `columnsFetchedAt` is what
     says whether anybody has looked. */
  function tmSetTableColumns(tpl, moduleName, tableId, columns, opts) {
    var o = opts || {};
    var mod = tmFindModule(tpl, moduleName);
    if (!mod) return null;
    for (var i = 0; i < mod.tables.length; i++) {
      if (mod.tables[i].id !== tableId) continue;
      var t = mod.tables[i];
      var list = (Array.isArray(columns) ? columns : [])
        .map(function (c, ix) { return tmNormaliseColumn(c, ix, o.primaryKeys); })
        .filter(function (c) { return !!c.name; });
      list.sort(function (a, b) { return (a.ordinal || 0) - (b.ordinal || 0); });
      t.columns = list;
      t.columnsFetchedAt = o.at || tmNow();
      tmTouch(tpl, o.by);
      return t;
    }
    return null;
  }

  function tmTableHasColumns(t) {
    return !!(t && Array.isArray(t.columns) && t.columns.length);
  }

  /* How much of the in-scope template has column detail. The header line and
     the publish warning both read this, so they cannot disagree. */
  /* Counts INCLUDED modules only. A module that is in scope but not ticked
     keeps its tables on the document, but nothing is going to be built from
     them this time round, so counting their missing columns would raise a
     publish warning about work nobody is doing. */
  function tmColumnCoverage(tpl) {
    var inScope = tmActiveModules(tpl);
    var tablesInScope = 0, tablesWithColumns = 0, totalColumns = 0;
    var missing = [];
    inScope.forEach(function (m) {
      (m.tables || []).forEach(function (t) {
        tablesInScope++;
        if (tmTableHasColumns(t)) { tablesWithColumns++; totalColumns += t.columns.length; }
        else missing.push({ module: m.module, targetTable: t.targetTable });
      });
    });
    return { tablesInScope: tablesInScope, tablesWithColumns: tablesWithColumns,
      tablesMissing: missing, totalColumns: totalColumns };
  }

  /* Non-blocking warnings for a publish. Deliberately NOT folded into
     tmCanPublish: that returns a boolean, tmPublish branches on it, and the
     page and three tests read it — an array there would be truthy and would
     silently turn "cannot publish" into "publish". The blocking rule is
     unchanged; this is the second question, asked separately. */
  function tmPublishWarnings(tpl) {
    var out = [];
    var cov = tmColumnCoverage(tpl);
    if (cov.tablesMissing.length) {
      out.push({ level: 'warning', module: '', code: 'no-columns',
        message: cov.tablesMissing.length + ' table' + (cov.tablesMissing.length === 1 ? ' has' : 's have')
          + ' no column detail; the specification workbook will list ' + (cov.tablesMissing.length === 1 ? 'it' : 'them')
          + ' as unresolved. Read the columns first if the client needs them.' });
    }
    return out;
  }

  /* Forward migration. Called by anything that loads a stored document, so a
     v1 draft written before column snapshots existed opens unchanged. */
  function tmMigrate(tpl) {
    if (!tpl || typeof tpl !== 'object') return tpl;
    (tpl.modules || []).forEach(function (m) {
      /* Schema 4. Both flags are written as an explicit false rather than
         left undefined, so no later reader has to guess what silence meant.
         A document that already carries `included` keeps whatever the
         operator set; a schema-3 document's `excluded` is DROPPED rather than
         inverted — see the note on TM_SCHEMA_VERSION for why. */
      if (typeof m.included !== 'boolean') m.included = false;
      if (typeof m.mapped !== 'boolean') m.mapped = false;
      if ('excluded' in m) delete m.excluded;
      (m.tables || []).forEach(function (t) {
        if (!Array.isArray(t.columns)) t.columns = [];
      });
    });
    tpl.schema = TM_SCHEMA_VERSION;
    return tpl;
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

    /* "Ready to publish?" asks about the modules that are actually going to be
       published. A module that is in scope but not included is skipped
       entirely: no tables is not a problem for a module nobody is building
       this time. The one thing we do still check is that SOMETHING is
       included — a publish with nothing ticked produces an empty workbook and
       an empty DDL, and the operator would find that out after sending it to
       the client rather than before. */
    var active = tmActiveModules(tpl);
    if (inScope.length && !active.length) {
      issues.push({
        level: 'error', module: '',
        message: 'No module is included in the publish yet. Tick Include beside the modules this version covers.'
      });
    }

    active.forEach(function (m) {
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

    /* Not an error — the operator chose this — but it must be visible on the
       same panel they read before pressing Publish, so that "where did FIN go"
       is answered before the workbook reaches the client rather than after.
       Silent when nothing is included at all: the error above already says so,
       and listing every module in scope underneath it would bury it. The names
       are capped, because a warning nobody can read is a warning nobody
       reads. */
    var excluded = active.length ? tmExcludedModules(tpl) : [];
    if (excluded.length) {
      var named = excluded.slice(0, 8).join(', ') + (excluded.length > 8 ? ', …' : '');
      issues.push({
        level: 'warning', module: '',
        message: excluded.length + ' module' + (excluded.length === 1 ? ' is' : 's are')
          + ' not included in the publish and will be left out of the workbook, the staging DDL and this publish: '
          + named + '.'
      });
    }

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
    /* Freeze WHAT WAS LEFT OUT alongside what went in. The module flags travel
       with the copy anyway, but a reader six months later asking "was AP in
       version 4?" should not have to reconstruct the answer from a boolean on
       each of twenty-four modules. This is the audit record of the decision. */
    copy.excludedModules = tmExcludedModules(tpl);
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
    var active = tmActiveModules(tpl);
    /* moduleCount and tableCount stay counted over everything IN SCOPE, not
       over the included set. The header reads "24 modules · 6 included in the
       publish": the 24 is the size of the scope and the 6 is a subset of it.
       Subtracting here would print "18 modules · 6 included", which reads as
       24 and is wrong. */
    var tables = 0;
    inScope.forEach(function (m) { tables += (m.tables || []).length; });
    var activeTables = 0;
    active.forEach(function (m) { activeTables += (m.tables || []).length; });
    var empty = active.filter(function (m) { return !(m.tables || []).length; }).length;
    var cov = tmColumnCoverage(tpl);
    return {
      name: (tpl && tpl.name) || '',
      version: (tpl && tpl.version) || 0,
      status: (tpl && tpl.status) || TM_STATUS.DRAFT,
      moduleCount: inScope.length,
      tableCount: tables,
      modulesWithoutTables: empty,
      outOfScopeCount: mods.length - inScope.length,
      // Added with schema 2; a v1 document reports 0 and 0, which is true.
      totalColumns: cov.totalColumns,
      tablesWithColumns: cov.tablesWithColumns,
      tablesMissingColumns: cov.tablesMissing.length,
      // Added with schema 3, opt-in since 4. A document with no flags reports
      // nothing included and nothing mapped, which is exactly what it says.
      excludedCount: inScope.length - active.length,
      mappedCount: inScope.filter(function (m) { return !!m.mapped; }).length,
      publishModuleCount: active.length,
      publishTableCount: activeTables
    };
  }

  /* --------------------------------------------------------------------
     Export
     ------------------------------------------------------------------ */

  var api = {
    TM_SCHEMA_VERSION: TM_SCHEMA_VERSION,
    TM_COLUMN_FIELDS: TM_COLUMN_FIELDS,
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
    tmModuleIncluded: tmModuleIncluded,
    tmModuleActive: tmModuleActive,
    tmActiveModules: tmActiveModules,
    tmSetModuleIncluded: tmSetModuleIncluded,
    tmSetModuleMapped: tmSetModuleMapped,
    tmExcludedModules: tmExcludedModules,
    tmNormaliseColumn: tmNormaliseColumn,
    tmSetTableColumns: tmSetTableColumns,
    tmTableHasColumns: tmTableHasColumns,
    tmColumnCoverage: tmColumnCoverage,
    tmPublishWarnings: tmPublishWarnings,
    tmMigrate: tmMigrate,
    tmValidate: tmValidate,
    tmCanPublish: tmCanPublish,
    tmPublish: tmPublish,
    tmNewDraftFrom: tmNewDraftFrom,
    tmSummary: tmSummary
  };

  global.CygenixTemplateModel = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;

})(typeof window !== 'undefined' ? window : globalThis);