/* ============================================================================
   cygenix-map-context.js — the migration scope, read from the map
   ----------------------------------------------------------------------------
   Assurance currently takes its scope from whichever connection is selected,
   via buildCatalog() -> CygenixSchemaGraph.load('src'|'tgt'). That enumerates
   a database. This module enumerates the *migration*: the set of target tables
   the active project's Object Mapping and jobs actually write to, plus the
   predicate, key, column mapping and hand-written verify SQL for each one.

   Everything here is read-only. No network, no writes, no side effects.

   Sources, in the order they are trusted:
     1. project.groups[].steps[]  (type === 'migration')  — executed steps,
        carry run results (rowsInserted, rowsExcluded, status, finishedAt)
     2. cygenix_jobs composite jobs -> childSteps[]        — the saved plan
     3. cygenix_jobs simple-map jobs                       — standalone jobs,
        the only place verifySQL and schemaSQL survive
     4. CygenixSchemaGraph.savedMaps()                     — the console's own
        accessor; used only to cross-check, because it is lossy (see notes)

   NOT a source: cygenix_objmap_wip_<projectId>. That key is the Mapping
   screen's scratch buffer — one source/target pair mid-edit. It is per-editor,
   often stale, and absent entirely for projects mapped through Jobs.
   ========================================================================== */

(function (global) {
  'use strict';

  var LS = {
    projects:    'cygenix_projects',
    jobs:        'cygenix_jobs',
    currentProj: 'cygenix_current_project',
    projConns:   'cygenix_project_connections',
    inferredRel: 'cygenix_inferred_rels'
  };

  function readJSON(key) {
    try { return JSON.parse(global.localStorage.getItem(key)); }
    catch (e) { return null; }
  }

  /* --------------------------------------------------------------------------
     Project identity.

     Do NOT trust cygenix_current_project on its own. It is written by several
     screens and drifts: on the console today it holds a project id that is not
     in cygenix_projects at all. CygenixSchemaGraph.activeProjectId() resolves
     correctly, so prefer it and fall back only if the graph is not loaded.
     -------------------------------------------------------------------------- */
  function activeProjectId() {
    var G = global.CygenixSchemaGraph;
    if (G && typeof G.activeProjectId === 'function') {
      var id = G.activeProjectId();
      if (id) return id;
    }
    return global.localStorage.getItem(LS.currentProj) || null;
  }

  function activeProject() {
    var id = activeProjectId();
    var all = readJSON(LS.projects) || [];
    return all.filter(function (p) { return p && p.id === id; })[0] || null;
  }

  /* --------------------------------------------------------------------------
     Normalise one migration step into a mapped-table record.
     -------------------------------------------------------------------------- */
  function toStep(s, origin) {
    if (!s || s.type !== 'migration') return null;

    var cols = (s.columnMapping || []).map(function (m) {
      return {
        src:       m.srcCol || '',
        tgt:       m.tgtCol || '',
        transform: m.transform || 'NONE',
        literal:   m.literalValue || '',
        match:     m.match || null,
        note:      m.note || ''
      };
    });

    return {
      origin:       origin,
      jobId:        s.jobId || s.id || null,
      name:         s.name || '',
      srcTable:     s.srcTable || s.sourceTable || '',
      tgtTable:     s.tgtTable || s.targetTable || '',
      /* the single highest-value field on this object: reconciliation that
         ignores it reports a deliberately filtered migration as data loss */
      predicate:    s.srcWhere || '',
      groupBy:      s.srcGroupBy || '',
      key:          s.targetPkCol || null,
      keyKind:      s.targetPkKind || null,
      stagingTable: s.stagingTable || null,
      columns:      cols,
      /* columns the map writes with no source column behind them */
      unsourced:    cols.filter(function (c) { return !c.src && !c.literal; })
                        .map(function (c) { return c.tgt; }),
      /* columns written from a literal or parameter, not from the source —
         these must be excluded from any source-vs-target column comparison */
      literals:     cols.filter(function (c) { return !!c.literal; })
                        .map(function (c) { return { column: c.tgt, value: c.literal }; }),
      /* one source column feeding two or more target columns */
      fanOut:       fanOut(cols),
      run: {
        status:       s.status || null,
        srcRows:      num(s.totalRows),
        staged:       num(s.rowsStaged),
        inserted:     num(s.rowsInserted),
        excluded:     num(s.rowsExcluded),
        startedAt:    s.startedAt || null,
        finishedAt:   s.finishedAt || s.lastRun || null,
        runId:        s.runId || null,
        hasRun:       !!(s.finishedAt || s.lastRun)
      }
    };
  }

  function num(v) { return (typeof v === 'number') ? v : null; }

  function fanOut(cols) {
    var seen = {}, out = [];
    cols.forEach(function (c) {
      if (!c.src) return;
      (seen[c.src] = seen[c.src] || []).push(c.tgt);
    });
    Object.keys(seen).forEach(function (src) {
      if (seen[src].length > 1) out.push({ source: src, targets: seen[src] });
    });
    return out;
  }

  /* --------------------------------------------------------------------------
     Hand-written verify SQL lives only on the top-level job record. It is
     dropped by savedMaps() and never copied into childSteps, so collect it
     separately and attach by jobId.
     -------------------------------------------------------------------------- */
  function verifyByJob(jobs) {
    var out = {};
    jobs.forEach(function (j) {
      if (j && j.verifySQL) out[j.id] = { sql: j.verifySQL, jobName: j.name || '' };
    });
    return out;
  }

  /* --------------------------------------------------------------------------
     The connection pair the map was built against.

     project.dbHistory is ordered most-recent-first and records every src/tgt
     the project has been pointed at. The pair in force when the jobs ran is
     not necessarily the pair selected now — compare and say so rather than
     silently profiling a database the migration never touched.
     -------------------------------------------------------------------------- */
  function connectionContext(project, steps) {
    var hist = (project && project.dbHistory) || [];
    var lastRun = steps.map(function (s) { return s.run.finishedAt; })
                       .filter(Boolean).sort().pop() || null;

    function atTime(role) {
      var candidates = hist.filter(function (d) { return d.role === role; });
      if (!lastRun) return candidates[0] || null;
      var inForce = candidates.filter(function (d) {
        return !d.firstSeen || d.firstSeen <= lastRun;
      });
      return inForce.sort(function (a, b) {
        return String(b.lastSeen || '').localeCompare(String(a.lastSeen || ''));
      })[0] || candidates[0] || null;
    }

    return {
      lastRunAt:  lastRun,
      mappedSrc:  atTime('src'),
      mappedTgt:  atTime('tgt'),
      selectedSrc: hist.filter(function (d) { return d.role === 'src'; })
                       .sort(function (a, b) {
                         return String(b.lastSeen || '').localeCompare(String(a.lastSeen || ''));
                       })[0] || null,
      selectedTgt: hist.filter(function (d) { return d.role === 'tgt'; })
                       .sort(function (a, b) {
                         return String(b.lastSeen || '').localeCompare(String(a.lastSeen || ''));
                       })[0] || null
    };
  }

  /* --------------------------------------------------------------------------
     Does the currently connected schema graph actually contain the mapped
     tables? If not, every rule bound by the catalogue is bound to a different
     database than the one the migration wrote to.
     -------------------------------------------------------------------------- */
  function tablesPresentInGraph(side, steps) {
    var G = global.CygenixSchemaGraph;
    if (!G || typeof G.load !== 'function') return null;
    if (typeof G.hasConnection === 'function' && !G.hasConnection(side)) return null;

    var g;
    try { g = G.load(side); } catch (e) { return null; }
    if (!g || typeof g.then === 'function' || !g.tables) return null;   // async: caller re-checks

    var names = {};
    g.tables.forEach(function (t) {
      names[String((t.schema || 'dbo') + '.' + (t.name || t.table)).toLowerCase()] = true;
    });

    var want = steps.map(function (s) {
      return side === 'src' ? s.srcTable : s.tgtTable;
    }).filter(Boolean);

    return {
      database: g.database || null,
      tableCount: g.tables.length,
      missing: want.filter(function (n) { return !names[String(n).toLowerCase()]; })
    };
  }

  /* --------------------------------------------------------------------------
     resolveMigrationScope() — the one function Assurance needs.

     A source/target pair can appear several times, from records of different
     ages: the saved job definition, the composite job's plan, and one entry
     per executed run. They are NOT interchangeable. dbo.Cases on this console
     carries a run from 14 Aug that moved 100 rows with no filter, and a run
     from 16 Aug that moved 24 with `caseno like '%z%'` — pairing the newer
     predicate with the older run count invents a 76-row discrepancy that
     never happened.

     So the two halves are resolved separately:
       definition (predicate, key, columns) — from the newest saved definition
       state     (rows, status, timestamps) — from the newest completed run
     and when the definition is newer than the run, the derived checks are
     Armed rather than Proven, because nothing has yet proved the current map.
     -------------------------------------------------------------------------- */
  function resolveMigrationScope() {
    var project = activeProject();
    var pid     = project ? project.id : activeProjectId();
    var jobs    = (readJSON(LS.jobs) || []).filter(function (j) { return j && !j._deleted; });
    var verify  = verifyByJob(jobs);

    var definitions = [];   /* what the map says should happen */
    var runs        = [];   /* what happened */

    /* when a step was last defined is the *step's own* job record, never the
       composite wrapper's — saving the wrapper after a run would otherwise
       read as "the map changed since this ran" when nothing changed */
    var definedAtOf = {};
    jobs.forEach(function (j) {
      if (j.jobType !== 'composite') definedAtOf[j.id] = j.lastModified || j.created || null;
    });

    /* definitions, newest-authority first: saved job -> composite plan */
    jobs.forEach(function (j) {
      if (j.projectId && pid && j.projectId !== pid) return;
      if (j.jobType === 'composite') {
        (j.childSteps || []).forEach(function (s) {
          var st = toStep(s, 'job:' + j.id);
          /* null when the step has no standalone job record: unknown, not stale */
          if (st) { st.definedAt = definedAtOf[st.jobId] || null; definitions.push(st); }
        });
      } else if (j.type === 'migration' || j.jobType === 'simple-map') {
        var one = toStep(
          { type: 'migration', id: j.id, name: j.name, srcTable: j.sourceTable || j.source,
            tgtTable: j.targetTable || j.target, srcWhere: j.srcWhere, srcGroupBy: j.srcGroupBy,
            columnMapping: j.columnMapping, totalRows: j.totalRows, status: j.status },
          'job:' + j.id
        );
        if (one) { one.definedAt = j.lastModified || j.created || null; definitions.push(one); }
      }
    });

    /* runs — executed steps recorded on the project */
    ((project && project.groups) || []).forEach(function (g) {
      (g.steps || []).forEach(function (s) {
        var st = toStep(s, 'project.groups/' + (g.name || g.id));
        if (st && st.run.hasRun) runs.push(st);
      });
    });
    /* composite childSteps also carry run results */
    definitions.forEach(function (d) { if (d.run.hasRun) runs.push(d); });

    var pairKey = function (s) { return s.srcTable + '→' + s.tgtTable; };

    var defByPair = {};
    definitions.forEach(function (d) {
      var k = pairKey(d), held = defByPair[k];
      if (!held || String(d.definedAt || '') > String(held.definedAt || '')) defByPair[k] = d;
    });

    var runByPair = {};
    runs.forEach(function (r) {
      var k = pairKey(r), held = runByPair[k];
      if (!held || String(r.run.finishedAt || '') > String(held.run.finishedAt || '')) runByPair[k] = r;
    });

    var mapped = Object.keys(defByPair).map(function (k) {
      var def = defByPair[k];
      var run = runByPair[k];
      def.run = run ? run.run : def.run;
      def.runOrigin = run ? run.origin : null;
      def.definitionNewerThanRun =
        !!(def.definedAt && def.run.finishedAt && def.definedAt > def.run.finishedAt);
      var v = def.jobId && verify[def.jobId];
      def.verifySQL = v ? v.sql : null;
      return def;
    });

    var conns = connectionContext(project, mapped);

    return {
      project: project ? {
        id: project.id, name: project.name, phase: project.phase || null,
        srcSystem: project.srcSystem || null, tgtSystem: project.tgtSystem || null
      } : { id: pid },
      mappedTableCount: mapped.length,
      mappedRowsAtSource: mapped.reduce(function (n, s) { return n + (s.run.srcRows || 0); }, 0),
      mappedRowsLanded:   mapped.reduce(function (n, s) { return n + (s.run.inserted || 0); }, 0),
      jobsWithVerifySQL:  mapped.filter(function (s) { return !!s.verifySQL; }).length,
      tables: mapped,
      connections: conns,
      graphCheck: { src: tablesPresentInGraph('src', mapped),
                    tgt: tablesPresentInGraph('tgt', mapped) }
    };
  }

  /* --------------------------------------------------------------------------
     deriveChecks() — the fixed set a mapped table implies.

     Every type used here already exists in the rule engine's vocabulary:
       recon.rowcount, recon.column_aggregate, uniqueness.key, schema.table,
       completeness.not_null, drift.null_rate, drift.freshness, domain.enum,
       referential.orphan, custom.sql
     Nothing new is introduced.
     -------------------------------------------------------------------------- */
  function deriveChecks(scope) {
    var out = [];
    var preCutover = !scope.project || scope.project.phase !== 'cutover';

    scope.tables.forEach(function (t, i) {
      var ref = (t.jobId || 'JOB-' + (i + 1));
      /* a run only proves the definition it ran against */
      var proven = t.run.hasRun && !t.definitionNewerThanRun ? 'proven' : 'armed';
      var n = 0;
      function id() { return ref + '/S' + (++n); }

      out.push({
        id: id(), check: 'schema.table', state: 'armed',
        scope: 'tgt.' + t.tgtTable,
        proves: t.tgtTable + ' exists in the target with the ' + t.columns.length +
                ' columns the map writes.',
        derivedFrom: 'target table + column list in the mapping'
      });

      out.push({
        id: id(), check: 'recon.rowcount',
        state: proven,
        scope: 'src.' + t.srcTable + ' → tgt.' + t.tgtTable,
        predicate: t.predicate || null,
        proves: t.predicate
          ? 'The same number of rows landed in ' + t.tgtTable +
            ' as left the source under the job\'s own filter (' + t.predicate + ').'
          : 'The same number of rows landed in ' + t.tgtTable + ' as left the source.',
        derivedFrom: t.predicate ? 'WHERE clause lifted from the job' : 'source and target tables in the mapping',
        sql: rowcountSQL(t)
      });

      if (t.run.excluded) {
        out.push({
          id: id(), check: 'recon.rowcount', state: proven,
          scope: 'src.' + t.srcTable,
          proves: t.run.excluded + ' source rows fall outside the filter and were ' +
                  'excluded on purpose — they are accounted for, not lost.',
          derivedFrom: 'rowsExcluded recorded on the run'
        });
      }

      if (t.key) {
        out.push({
          id: id(), check: 'uniqueness.key',
          state: proven,
          scope: 'tgt.' + t.tgtTable + '.' + t.key,
          proves: 'Every row in ' + t.tgtTable + ' has a distinct ' + t.key +
                  ' — the load did not run twice.',
          derivedFrom: 'targetPkCol on the mapping'
        });
      }

      t.fanOut.forEach(function (f) {
        out.push({
          id: id(), check: 'custom.sql', state: 'armed',
          scope: 'tgt.' + t.tgtTable,
          proves: f.targets.join(' and ') + ' hold the same value on every row — ' +
                  'the map fills both from ' + f.source + '.',
          derivedFrom: 'one source column mapped to ' + f.targets.length + ' target columns'
        });
      });

      t.literals.forEach(function (l) {
        out.push({
          id: id(), check: 'domain.enum', state: 'armed',
          scope: 'tgt.' + t.tgtTable + '.' + l.column,
          proves: l.column + ' holds the resolved value of ' + l.value +
                  ' on every migrated row, not the source column of the same name.',
          derivedFrom: 'literalValue ' + l.value + ' on the mapping',
          note: 'Excludes ' + l.column + ' from any source-vs-target column comparison.'
        });
      });

      if (t.unsourced.length) {
        out.push({
          id: id(), check: 'drift.null_rate', state: 'armed',
          scope: 'tgt.' + t.tgtTable + ' (' + t.unsourced.join(', ') + ')',
          proves: 'The ' + t.unsourced.length + ' target columns the map leaves unfilled stay ' +
                  'empty — anything writing to them is not this migration.',
          derivedFrom: 'target columns with no source column in the mapping'
        });
      }

      t.columns.filter(function (c) { return MONEY.test(c.tgt); }).forEach(function (c) {
        out.push({
          id: id(), check: 'recon.column_aggregate', state: 'armed',
          scope: 'tgt.' + t.tgtTable + '.' + c.tgt,
          proves: c.tgt + ' sums to the same total on both sides.',
          derivedFrom: 'numeric column carried across by the mapping'
        });
      });

      out.push({
        id: ref + '/C1', check: 'drift.freshness',
        state: preCutover ? 'dormant' : 'armed',
        scope: 'tgt.' + t.tgtTable,
        proves: t.tgtTable + ' keeps receiving data once the application is live.',
        derivedFrom: 'continuous rule, activates at cutover',
        note: preCutover ? 'Project phase is "' + (scope.project.phase || 'unset') +
                           '" — this rule does not fire and does not open a breach.' : null
      });

      if (t.verifySQL) {
        out.push({
          id: ref + '/V1', check: 'custom.sql',
          state: proven,
          scope: 'src.' + t.srcTable + ' → tgt.' + t.tgtTable,
          proves: 'Hand-written verification for this job, imported from Validation.',
          derivedFrom: 'verifySQL on the job record',
          supersedes: ref + '/S2',
          sql: t.verifySQL
        });
      }
    });

    return out;
  }

  var MONEY = /(amount|total|value|balance|price|cost|fee|sum|qty|quantity)/i;

  function rowcountSQL(t) {
    var src = bracket(t.srcTable), tgt = bracket(t.tgtTable);
    return 'SELECT\n' +
      '  (SELECT COUNT(*) FROM ' + src +
      (t.predicate ? ' WHERE ' + t.predicate : '') + ') AS src_n,\n' +
      '  (SELECT COUNT(*) FROM ' + tgt + ') AS tgt_n;';
  }

  function bracket(name) {
    return String(name || '').split('.').map(function (p) { return '[' + p + ']'; }).join('.');
  }

  global.CygenixMapContext = {
    activeProjectId: activeProjectId,
    activeProject: activeProject,
    resolveMigrationScope: resolveMigrationScope,
    deriveChecks: deriveChecks,
    /* convenience: the shape asCoverage() already expects, but scoped to the
       migration instead of the connection catalogue */
    catalogFromMap: function () {
      var scope = resolveMigrationScope();
      return {
        tables: scope.tables.map(function (t) {
          var parts = String(t.tgtTable).split('.');
          return {
            db: 'tgt',
            schema: parts.length > 1 ? parts[0] : 'dbo',
            table: parts[parts.length - 1],
            columns: t.columns.map(function (c) { return c.tgt; }),
            rowCount: t.run.inserted || t.run.srcRows || 0,
            referencedBy: [],
            mapped: true,
            jobId: t.jobId,
            predicate: t.predicate || null
          };
        }),
        outsideMigration: null   /* fill from the connection catalogue for the
                                    secondary "not in this migration" line */
      };
    }
  };
})(typeof window !== 'undefined' ? window : this);
