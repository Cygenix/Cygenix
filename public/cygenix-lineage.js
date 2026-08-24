/* cygenix-lineage.js — live lineage and impact analysis.
 *
 * Click any target column and see the full path back to source — the
 * transform at each hop, every Was/Is rule attached, the join it rode in
 * on, the WHERE clause that filters it — and then the other direction:
 * "change this mapping and these N objects are affected", by name.
 *
 * Lineage is assembled from metadata the product already keeps, so it is
 * live by construction — nothing to sync, nothing to annotate:
 *   - jobs and their column mappings (transforms, literals, joins)
 *   - Was/Is value-mapping rules        (bound to source table + field)
 *   - composite tasks                   (childSteps embed job snapshots)
 *   - plain-English reconciliation checks (bound to job + column pair)
 *   - validation sources                (bound to tables)
 *   - the last preflight report         (findings name column pairs)
 *   - foreign keys, when a caller supplies them
 *
 * Pure core over an explicit ctx object; buildContext() gathers the ctx
 * from localStorage in the browser. Same testable pattern as the other
 * engines.
 */

(function (root) {
  'use strict';

  const norm = (s) => String(s || '').trim().toLowerCase();
  const bareTable = (t) => norm(t).replace(/[[\]]/g, '');
  const sameTable = (a, b) => {
    const na = bareTable(a), nb = bareTable(b);
    if (na === nb) return true;
    // schema-less vs schema-qualified: dbo.clients ~ clients
    return na.split('.').pop() === nb.split('.').pop() && !!na && !!nb;
  };

  // ── Lineage chain (target ← … ← source) ──────────────────────────────────

  function lnColumnLineage(job, tgtCol, ctx) {
    ctx = ctx || {};
    const m = (job.columnMapping || []).find(x => x && norm(x.tgtCol) === norm(tgtCol));
    const hops = [{ kind: 'target', table: job.targetTable, column: tgtCol }];
    // No row, or a row with neither a source column nor a value: unmapped
    // either way — say so rather than inventing a path.
    if (!m || (!m.srcCol && !m.literalValue && !m.fixedValue)) {
      hops.push({ kind: 'unmapped' });
      return hops;
    }

    const literal = m.literalValue || m.fixedValue;
    if (m.transform && m.transform !== 'NONE') hops.push({ kind: 'transform', transform: m.transform });

    if (literal) {
      hops.push({ kind: 'literal', value: literal,
        note: /@@/.test(literal) ? 'parameter token — resolved at run time' : 'fixed value — no source column' });
      if (!m.srcCol || !/\bMAX|MIN|SUM|COUNT|AVG\b/i.test(literal)) return hops;
    }

    // Was/Is rules bind to (source table, source field) — they rewrite the
    // value between the source read and the insert.
    const rules = (ctx.wasisRules || []).filter(r =>
      norm(r.srcField) === norm(m.srcCol)
      && (!r.srcTable || sameTable(r.srcTable, job.sourceTable)));
    if (rules.length) hops.push({ kind: 'wasis', rules });

    if (m.srcCol) {
      const joined = (ctx.joinColumnsByJob && ctx.joinColumnsByJob[job.id] || [])
        .find(c => norm(c.name) === norm(m.srcCol));
      hops.push({ kind: 'source', table: joined ? (joined.table || 'joined table') : job.sourceTable,
        column: m.srcCol, fromJoin: !!joined });
    }
    if (job.srcWhere || job.srcGroupBy) {
      hops.push({ kind: 'filter', where: job.srcWhere || '', groupBy: job.srcGroupBy || '' });
    }
    return hops;
  }

  // ── Impact analysis (what breaks if this mapping changes) ────────────────

  function lnImpact(job, tgtCol, ctx) {
    ctx = ctx || {};
    const m = (job.columnMapping || []).find(x => x && norm(x.tgtCol) === norm(tgtCol)) || {};
    const srcCol = m.srcCol || '';
    const out = [];

    // Composite tasks embed a SNAPSHOT of the job's steps — the change does
    // not reach them until the task is re-created, and a schedule pinned to
    // a task version keeps the old mapping until re-pinned. Both facts are
    // exactly what someone about to edit needs to hear.
    for (const t of (ctx.jobs || []).filter(j => j.jobType === 'composite')) {
      const embeds = (t.childSteps || []).some(s =>
        s.jobId === job.id ||
        (sameTable(s.tgtTable, job.targetTable) && sameTable(s.srcTable, job.sourceTable)));
      if (embeds) out.push({ type: 'task', name: t.name || t.id,
        detail: 'embeds a snapshot of this job — re-create the task (and re-pin any schedule) or it runs the OLD mapping' });
    }

    // Plain-English reconciliation checks bound to this job and column pair.
    for (const c of (ctx.reconChecks || [])) {
      const s = c.spec || {};
      if (s.jobId !== job.id && !(sameTable(s.tgtTable, job.targetTable))) continue;
      const touches = ['col', 'key'].some(k => s[k] &&
        (norm(s[k].tgtCol) === norm(tgtCol) || (srcCol && norm(s[k].srcCol) === norm(srcCol))));
      if (touches) out.push({ type: 'recon', name: '“' + (c.phrase || s.label) + '”',
        detail: 'reconciliation check reads this column pair — it will compare the wrong thing or fail to parse' });
    }

    // Was/Is rules keyed to the CURRENT source column stop firing if the
    // source changes.
    for (const r of (ctx.wasisRules || [])) {
      if (srcCol && norm(r.srcField) === norm(srcCol)
          && (!r.srcTable || sameTable(r.srcTable, job.sourceTable))) {
        out.push({ type: 'wasis', name: 'Was/Is: ' + r.oldVal + ' → ' + r.newVal,
          detail: 'bound to ' + (r.srcTable || 'any table') + '.' + r.srcField + ' — a different source column silently stops this substitution' });
      }
    }

    // Chained jobs: another job reads THIS job's target table as its source,
    // and maps this very column onward.
    //
    // A job whose source is a VIEW counts too, when that view reads this
    // table. Otherwise a view sitting between two jobs makes the dependency
    // invisible: "what breaks if I change this column" would answer "nothing",
    // while a downstream load through vw_x quietly stops working. The base
    // objects are recorded on the job at save time (sourceBaseObjects); a job
    // saved before that existed simply has none, and behaves as it always did.
    const readsTable = (j, table) => {
      if (sameTable(j.sourceTable, table)) return true;
      return (j.sourceBaseObjects || []).some(o => {
        const full = o && (o.schema ? o.schema + '.' + o.name : o.name);
        return full && sameTable(full, table);
      });
    };
    for (const j of (ctx.jobs || [])) {
      if (j.id === job.id || j.jobType === 'composite') continue;
      if (!readsTable(j, job.targetTable)) continue;
      const usesCol = (j.columnMapping || []).some(x => x && norm(x.srcCol) === norm(tgtCol));
      const viaView = !sameTable(j.sourceTable, job.targetTable);
      if (usesCol) out.push({ type: 'job', name: j.name || j.id,
        detail: 'reads ' + job.targetTable + '.' + tgtCol + ' as its source'
          + (viaView ? ' through the view ' + j.sourceTable : '')
          + ' — changes here flow straight into it' });
    }

    // Validation sources registered on the target table.
    for (const v of (ctx.validationSources || [])) {
      const vt = v.table ? (v.schema ? v.schema + '.' + v.table : v.table) : (v.tableName || '');
      if (vt && sameTable(vt, job.targetTable)) {
        out.push({ type: 'validation', name: v.label || v.name || vt,
          detail: 'validation scripts run against ' + job.targetTable });
      }
    }

    // The last preflight report's findings for this column pair — evidence
    // the mapping already needs care, worth surfacing at the point of edit.
    const pf = ctx.preflight;
    if (pf && pf.jobs) {
      for (const pj of pf.jobs) {
        if (!sameTable(pj.tgtTable, job.targetTable)) continue;
        for (const f of (pj.findings || [])) {
          if (String(f.column || '').toLowerCase().includes(norm(tgtCol))) {
            out.push({ type: 'preflight', name: 'Preflight: ' + f.code,
              detail: (f.projected ? '~' + Number(f.projected).toLocaleString('en-GB') + ' rows projected — ' : '') + (f.column || '') });
          }
        }
      }
    }

    // Foreign keys, when the caller has graph data: children pointing at
    // this column break if its values change shape.
    for (const fk of (ctx.fks || [])) {
      if (sameTable(fk.toTable, job.targetTable) && norm(fk.toColumn) === norm(tgtCol)) {
        out.push({ type: 'fk', name: fk.fromTable + '.' + fk.fromColumn,
          detail: 'foreign key references ' + job.targetTable + '.' + tgtCol });
      }
    }

    return out;
  }

  function lnHeadline(impact) {
    if (!impact.length) return 'Nothing else depends on this mapping — safe to change.';
    const n = impact.length;
    return 'Change this mapping and ' + n + ' downstream object' + (n === 1 ? ' is' : 's are') + ' affected.';
  }

  // ── Browser context assembly ──────────────────────────────────────────────
  // Everything from localStorage and the shared helpers already on the page.
  function buildContext() {
    const get = (k, fb) => { try { return JSON.parse(localStorage.getItem(k) || 'null') || fb; } catch { return fb; } };
    const pid = (() => { try { return localStorage.getItem('cygenix_active_project_id') || 'default'; } catch { return 'default'; } })();
    return {
      jobs: get('cygenix_jobs', []),
      wasisRules: (typeof root.CygenixWasis !== 'undefined' && root.CygenixWasis.getRules)
        ? root.CygenixWasis.getRules() : get('cygenix_wasis_rules', []),
      reconChecks: (get('cygenix_recon_tests', {})[pid]) || [],
      validationSources: get('cygenix_validation_sources', []),
      preflight: (get('cygenix_preflight_results', {})[pid]) || null,
      fks: null,
    };
  }

  /* icon is markup: it renders into the impact drawer, so it carries the
     shared icon class rather than a glyph that would differ per platform */
  const TYPE_META = {
    task:       { icon: '<i class="ic ic-package"></i>',    label: 'Task' },
    recon:      { icon: '<i class="ic ic-chat"></i>',       label: 'Reconciliation check' },
    wasis:      { icon: '<i class="ic ic-sync"></i>',       label: 'Was/Is rule' },
    job:        { icon: '<i class="ic ic-chain"></i>',      label: 'Downstream job' },
    validation: { icon: '<i class="ic ic-search"></i>',     label: 'Validation' },
    preflight:  { icon: '<i class="ic ic-takeoff"></i>',    label: 'Preflight finding' },
    fk:         { icon: '<i class="ic ic-link"></i>',       label: 'Foreign key' },
  };

  const api = { lnColumnLineage, lnImpact, lnHeadline, buildContext, TYPE_META, sameTable };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  root.CygenixLineage = api;
})(typeof window !== 'undefined' ? window : globalThis);
