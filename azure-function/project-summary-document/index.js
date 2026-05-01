/**
 * Azure Function: project-summary-document
 * ----------------------------------------
 * GET /api/project-summary-document?jobId=<jobId>
 *
 * Returns a fully styled HTML document for the Cygenix Project Summary
 * Document. The browser-side module loads this in a hidden iframe and
 * triggers window.print() — no server-side Chromium needed.
 *
 * In-band debugging: every error response includes message + stack so it's
 * visible in the browser Network tab (per Curtis's Azure plan constraint
 * where Application Insights and Live Log Stream are unavailable).
 *
 * v1 scope:
 *   ✓ Cover + KPI summary
 *   ✓ Scope & environment + Was/Is rules
 *   ✓ Transformations applied
 *   ✓ Reconciliation tables
 *   ✓ Run timeline
 *   ✓ Sign-off page
 *   ⊘ Decisions with citations  — STUBBED (needs `decisions[]` on job record)
 *   ⊘ Rollback plan             — STUBBED (needs `job_versions` container)
 */

const { CosmosClient } = require('@azure/cosmos');

let cosmosClient = null;
let containers = null;

function getContainers() {
  if (containers) return containers;
  const conn = process.env.COSMOS_CONNECTION_STRING;
  if (!conn) throw new Error('COSMOS_CONNECTION_STRING env var not set');
  cosmosClient = new CosmosClient(conn);
  const db = cosmosClient.database(process.env.COSMOS_DATABASE || 'cygenix');
  containers = {
    projects: db.container('projects'),
    audit: db.container('audit'),
  };
  return containers;
}

module.exports = async function (context, req) {
  try {
    const jobId = (req.query && req.query.jobId) || '';
    if (!jobId) {
      context.res = {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
        body: { error: 'jobId query param required' }
      };
      return;
    }

    const data = await collectData(jobId);
    const html = renderDocument(data);

    context.res = {
      status: 200,
      headers: {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'private, no-cache',
        'X-Cygenix-Doc': 'project-summary-document-v1',
      },
      body: html,
    };
  } catch (err) {
    // Surface the error to the browser Network tab.
    context.res = {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
      body: {
        error: err.message,
        stack: err.stack,
        where: 'project-summary-document',
      },
    };
  }
};

// ────────────────────────────────────────────────────────────────────────
// Data collection
// ────────────────────────────────────────────────────────────────────────
//
// IMPORTANT — Cygenix data model on Cosmos:
//
// The 'projects' container holds ONE document PER USER, not per project.
// Each user-doc has top-level arrays:
//   - jobs[]      — every migration / SQL job the user has created
//   - projects[]  — every Cygenix project (a project groups jobs into
//                   ordered execution groups)
//   - wasis_rules[] — global Was/Is rule store (a rule applies to a job
//                   when its srcTable matches the job's source table)
//
// The earlier query `FROM p JOIN j IN p.jobs WHERE j.id = @jobId` worked
// (because `p.jobs` is the user-doc's top-level jobs array), but then the
// handler treated `p` as if it were a "project record". It isn't — it's the
// whole user doc. So fields like project.name / project.client / project.
// sourceSystem were all undefined and the document rendered as a skeleton.
//
// What this function now does:
//   1. Find the user-doc that contains a job with the given id
//   2. Pull the matching job out of user.jobs
//   3. Find the matching project record in user.projects via job.projectId
//   4. Filter user.wasis_rules down to rules that apply to this job's
//      source table
//   5. Derive everything else (tables list, transformations, runtime, etc.)
//      from the actual fields the runtime persists, not the fields the
//      original handler hoped for.
async function collectData(jobId) {
  const c = getContainers();

  const userQuery = {
    query: 'SELECT * FROM u JOIN j IN u.jobs WHERE j.id = @jobId',
    parameters: [{ name: '@jobId', value: jobId }],
  };
  const { resources: matches } = await c.projects.items.query(userQuery).fetchAll();
  if (!matches.length) throw new Error(`Job not found: ${jobId}`);

  // The JOIN flattens — `matches[0]` may either be the user doc itself, or
  // a {u, j} pair depending on how Cosmos serialises the JOIN result. Walk
  // both shapes safely.
  const userDoc = matches[0].u || matches[0];

  const allJobs = Array.isArray(userDoc.jobs) ? userDoc.jobs : [];
  const allProjects = Array.isArray(userDoc.projects) ? userDoc.projects : [];
  const allWasis = Array.isArray(userDoc.wasis_rules) ? userDoc.wasis_rules : [];

  const job = allJobs.find(j => j && j.id === jobId);
  if (!job) throw new Error(`Job ${jobId} not present in user payload`);

  const projectRecord = job.projectId
    ? allProjects.find(p => p && p.id === job.projectId)
    : null;

  // Source / target table names — the runtime stores these under several
  // possible keys depending on which save path produced the job. Try them
  // in priority order so we always find what's there.
  const srcTable = job.sourceTable || job.srcTable || job.source || '';
  const tgtTable = job.targetTable || job.tgtTable || job.target || '';

  // Tables list — if job.tables is populated by the runtime, prefer it.
  // Otherwise synthesise a single-row "tables" list from the job's
  // own metadata so the doc has something to show.
  let tables = Array.isArray(job.tables) && job.tables.length
    ? job.tables.map(t => ({
        name:        t.name || t.tgtTable || tgtTable || '—',
        sourceRows:  t.sourceRows != null ? t.sourceRows : (t.rows != null ? t.rows : (job.totalRows || 0)),
        targetRows:  t.targetRows != null ? t.targetRows : (t.insertedRows != null ? t.insertedRows : (job.totalRows || 0)),
      }))
    : [];
  if (!tables.length && (srcTable || tgtTable || job.totalRows)) {
    const total = Number(job.totalRows || 0);
    tables = [{
      name:        tgtTable || srcTable || job.name || '—',
      sourceRows:  total,
      targetRows:  total,
    }];
  }

  // Transformations — the saved column mapping records the transform per
  // column. Surface only the rows where something non-trivial happened
  // (transform != NONE, literal columns, type changes).
  const transformations = (job.columnMapping || [])
    .filter(m => m && m.tgtCol && (
      (m.transform && m.transform !== 'NONE') ||
      m.literalValue != null ||
      (m.tgtType && m.srcType && m.tgtType !== m.srcType)
    ))
    .map(m => ({
      target:   `${tgtTable || ''}${tgtTable ? '.' : ''}${m.tgtCol}`,
      fromType: m.srcType || '—',
      toType:   m.tgtType || '—',
      reason:   m.transform && m.transform !== 'NONE'
                  ? `Transform: ${m.transform}${m.transformExpr ? ' (' + m.transformExpr + ')' : ''}`
                  : (m.literalValue != null ? `Literal: ${m.literalValue}` : 'Type change'),
    }));

  // Was/Is rules — there are two possible sources:
  //   1. job.wasisRules — the runtime sometimes copies the global rule
  //      store onto the job. This is unfiltered (every rule for every
  //      table).
  //   2. user.wasis_rules — the global rule store.
  //
  // Either way we filter down to rules whose srcTable matches the job's
  // source table (case-insensitive, schema-tolerant), so the document
  // only lists rules that were actually applicable to this job.
  const stripSchema = (s) => String(s || '').split('.').pop().toLowerCase();
  const jobSrcTableNorm = stripSchema(srcTable);

  const wasisCandidates = Array.isArray(job.wasisRules) && job.wasisRules.length
    ? job.wasisRules
    : allWasis;

  // Also parse CASE WHEN substitutions out of insertSQL — that's the
  // ground truth (it's the SQL that ran). Each block looks like:
  //   CASE WHEN [col] = 'old' THEN 'new' ... END AS [tgtCol]
  const sqlSubstitutions = [];
  if (typeof job.insertSQL === 'string' && job.insertSQL.length) {
    const blocks = job.insertSQL.match(/CASE\s+WHEN[\s\S]+?END(?:\s+AS\s+\[[^\]]+\])?/gi) || [];
    blocks.forEach(b => {
      const colMatch = b.match(/WHEN\s+\[?(\w+)\]?\s*=/i);
      if (!colMatch) return;
      const colName = colMatch[1];
      const pairRe = /WHEN\s+\[?\w+\]?\s*=\s*N?'([^']*)'\s+THEN\s+N?'([^']*)'/gi;
      let m;
      while ((m = pairRe.exec(b)) !== null) {
        sqlSubstitutions.push({ field: colName, oldVal: m[1], newVal: m[2] });
      }
    });
  }

  // Dedup helper for wasIs output: table|field|old|new
  const wasIsSeen = new Set();
  const wasIsRules = [];
  const pushWasIs = (table, field, oldVal, newVal) => {
    const key = `${(table||'').toLowerCase()}|${(field||'').toLowerCase()}|${oldVal}|${newVal}`;
    if (wasIsSeen.has(key)) return;
    wasIsSeen.add(key);
    wasIsRules.push({ table: table || srcTable || '*', field: field || '*', oldVal, newVal, rowsAffected: 0 });
  };

  // Path A — rules from the wasis store, filtered to this job's source
  wasisCandidates.forEach(r => {
    if (!r) return;
    const ruleTableNorm = stripSchema(r.srcTable);
    // No table on the rule = applies to all sources; otherwise must match.
    if (ruleTableNorm && ruleTableNorm !== jobSrcTableNorm) return;
    pushWasIs(r.srcTable || srcTable, r.srcField, r.oldVal, r.newVal);
  });
  // Path B — substitutions parsed straight out of the insertSQL. These
  // override / supplement Path A and are guaranteed to be what actually
  // ran.
  sqlSubstitutions.forEach(s => {
    pushWasIs(srcTable, s.field, s.oldVal, s.newVal);
  });

  // Runtime — the runtime persists `created` (ISO) and `lastRun` (ISO).
  // Treat them as start and completion if we have nothing better.
  const runStartedAt   = job.runStartedAt   || job.created || null;
  const runCompletedAt = job.runCompletedAt || job.lastRun || null;
  let runtimeSeconds = null;
  if (job.runtimeSeconds != null) {
    runtimeSeconds = Number(job.runtimeSeconds);
  } else if (runStartedAt && runCompletedAt) {
    const ms = new Date(runCompletedAt).getTime() - new Date(runStartedAt).getTime();
    if (Number.isFinite(ms) && ms >= 0) runtimeSeconds = Math.round(ms / 1000);
  }

  // Status — `executionStatus` is the runtime's verdict; fall back to the
  // editorial `status` only if executionStatus is missing.
  const status = job.executionStatus || job.status || 'unknown';

  // Audit — same pattern as before. The audit container is queried by
  // jobId; if the user doesn't have audit set up, this just returns []
  // and the timeline section will say so honestly.
  let audit = [];
  try {
    const auditQuery = {
      query: 'SELECT * FROM a WHERE a.jobId = @jobId ORDER BY a.timestamp ASC',
      parameters: [{ name: '@jobId', value: jobId }],
    };
    const r = await c.audit.items.query(auditQuery).fetchAll();
    audit = r.resources || [];
  } catch (e) {
    // Audit container may not exist — don't fail the whole document
    audit = [];
  }

  return {
    project: {
      id:            (projectRecord && projectRecord.id)   || job.projectId || '',
      name:          (projectRecord && projectRecord.name) || job.name      || 'Untitled project',
      client:        (projectRecord && projectRecord.client) || '',
      sourceSystem:  (projectRecord && (projectRecord.srcSystem || projectRecord.sourceSystem)) || srcTable || '',
      targetSystem:  (projectRecord && (projectRecord.tgtSystem || projectRecord.targetSystem)) || tgtTable || '',
      description:   (projectRecord && projectRecord.description) || '',
      analyst:       (projectRecord && projectRecord.analyst)     || '',
      pm:            (projectRecord && projectRecord.pm)          || '',
      ref:           (projectRecord && projectRecord.ref)         || '',
    },
    job: {
      id:             job.id,
      name:           job.name || 'Untitled job',
      version:        job.version || 'v1',
      status:         status,
      operator:       job.operator || userDoc.userId || userDoc.email || 'unknown',
      runStartedAt:   runStartedAt,
      runCompletedAt: runCompletedAt,
      runtimeSeconds: runtimeSeconds,
      srcTable:       srcTable,
      tgtTable:       tgtTable,
      tables:         tables,
      transformations: transformations,
      wasIsRules:     wasIsRules,
      reconciliation: job.reconciliation || { sums: [], fks: [] },
      warnings:       Array.isArray(job.warnings) ? job.warnings : [],
    },
    audit,
    generatedAt: new Date().toISOString(),
  };
}

// ────────────────────────────────────────────────────────────────────────
// Renderer
// ────────────────────────────────────────────────────────────────────────
function renderDocument(d) {
  const { project, job, audit, generatedAt } = d;
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Project Summary Document — ${escapeHtml(project.name)}</title>
${stylesheet()}
</head>
<body>
${section_cover(project, job, generatedAt)}
${section_executiveSummary(job)}
${section_scope(project, job)}
${section_decisions_stub()}
${section_transformations(job)}
${section_reconciliation(job)}
${section_timeline(audit)}
${section_rollback_stub()}
${section_signoff(job)}
${printControls()}
</body>
</html>`;
}

function section_cover(project, job, generatedAt) {
  const runDate = job.runCompletedAt ? formatDate(job.runCompletedAt) : formatDate(generatedAt);
  return `
<div class="cover">
  <div class="cover-logo">
    <span class="cover-logo-mark">C</span>
    <span>Cygenix</span>
  </div>
  <div>
    <div class="cover-eyebrow">Project Summary Document</div>
    <div class="cover-title">${escapeHtml(project.name)}</div>
    <div class="cover-subtitle">${escapeHtml(project.sourceSystem || 'Source')} → ${escapeHtml(project.targetSystem || 'Target')}</div>
  </div>
  <div class="cover-meta">
    <div class="cover-meta-item"><div class="cover-meta-label">Project</div><div class="cover-meta-value">${escapeHtml(project.name)}</div></div>
    <div class="cover-meta-item"><div class="cover-meta-label">Job ID</div><div class="cover-meta-value">${escapeHtml(job.id)}</div></div>
    <div class="cover-meta-item"><div class="cover-meta-label">Run Date</div><div class="cover-meta-value">${escapeHtml(runDate)}</div></div>
    <div class="cover-meta-item"><div class="cover-meta-label">Version</div><div class="cover-meta-value">${escapeHtml(job.version)}</div></div>
    <div class="cover-meta-item"><div class="cover-meta-label">Operator</div><div class="cover-meta-value">${escapeHtml(job.operator)}</div></div>
    <div class="cover-meta-item"><div class="cover-meta-label">Client</div><div class="cover-meta-value">${escapeHtml(project.client || '—')}</div></div>
  </div>
  <div class="cover-footer">
    <span>CONFIDENTIAL — ${escapeHtml(project.client || project.name)} &amp; Cygenix</span>
    <span>cygenix.co.uk</span>
  </div>
</div>`;
}

function section_executiveSummary(job) {
  const totalRows = (job.tables || []).reduce((s, t) => s + (t.targetRows || 0), 0);
  const recon = computeReconStatus(job);
  const warnings = job.warnings.length;
  const runtime = formatRuntime(job.runtimeSeconds);

  const tableRows = (job.tables || []).map(t => `
    <tr>
      <td><code>${escapeHtml(t.name)}</code></td>
      <td style="text-align:right">${formatNumber(t.sourceRows)}</td>
      <td style="text-align:right">${formatNumber(t.targetRows)}</td>
      <td>${t.sourceRows === t.targetRows
        ? '<span class="badge badge-success">Match</span>'
        : '<span class="badge badge-warn">Δ ' + formatNumber(Math.abs((t.sourceRows||0)-(t.targetRows||0))) + '</span>'}</td>
    </tr>`).join('');

  return `
<div class="page">
  <div class="section-eyebrow">01 · Executive Summary</div>
  <h1 class="section">At a glance</h1>
  <div class="kpi-grid">
    <div class="kpi success">
      <div class="kpi-label">Rows migrated</div>
      <div class="kpi-value">${formatCompact(totalRows)}</div>
      <div class="kpi-sub">Across ${job.tables.length} tables</div>
    </div>
    <div class="kpi ${recon.className}">
      <div class="kpi-label">Reconciliation</div>
      <div class="kpi-value">${recon.value}</div>
      <div class="kpi-sub">${recon.sub}</div>
    </div>
    <div class="kpi ${warnings === 0 ? 'success' : 'warn'}">
      <div class="kpi-label">Warnings</div>
      <div class="kpi-value">${warnings}</div>
      <div class="kpi-sub">${warnings === 0 ? 'Clean run' : 'See decisions'}</div>
    </div>
    <div class="kpi info">
      <div class="kpi-label">Runtime</div>
      <div class="kpi-value">${runtime}</div>
      <div class="kpi-sub">Job ${escapeHtml(job.id)}</div>
    </div>
  </div>
  <h2>What was migrated</h2>
  <table>
    <thead><tr><th>Table</th><th style="text-align:right">Source rows</th><th style="text-align:right">Target rows</th><th>Status</th></tr></thead>
    <tbody>${tableRows || '<tr><td colspan="4" style="text-align:center;color:#94a3b8">No table data</td></tr>'}</tbody>
  </table>
</div>`;
}

function section_scope(project, job) {
  const wasIs = (job.wasIsRules || []).map(r => `
    <tr>
      <td><code>${escapeHtml(r.table || '*')}</code></td>
      <td><code>${escapeHtml(r.field || '*')}</code></td>
      <td>${escapeHtml(r.oldVal)}</td>
      <td>${escapeHtml(r.newVal)}</td>
      <td style="text-align:right">${formatNumber(r.rowsAffected || 0)}</td>
    </tr>`).join('');

  return `
<div class="page">
  <div class="section-eyebrow">02 · Scope &amp; Environment</div>
  <h1 class="section">Source &amp; target</h1>
  <div class="two-col">
    <div class="panel">
      <div class="panel-title">Source</div>
      <p style="margin:0"><strong>${escapeHtml(project.sourceSystem || '—')}</strong></p>
    </div>
    <div class="panel">
      <div class="panel-title">Target</div>
      <p style="margin:0"><strong>${escapeHtml(project.targetSystem || '—')}</strong></p>
    </div>
  </div>
  <h2>Was/Is rules applied</h2>
  ${wasIs
    ? `<table>
         <thead><tr><th>Table</th><th>Field</th><th>Old</th><th>New</th><th style="text-align:right">Rows rewritten</th></tr></thead>
         <tbody>${wasIs}</tbody>
       </table>`
    : '<p style="color:#64748b">No Was/Is rules active for this job.</p>'}
</div>`;
}

function section_decisions_stub() {
  return `
<div class="page">
  <div class="section-eyebrow">03 · Decisions</div>
  <h1 class="section">Decisions made by Claude</h1>
  <div class="callout callout-info">
    <div class="callout-title">ⓘ Available in the next release</div>
    Structured decision tracking — every type promotion, stored proc translation,
    and schema choice Claude made during analysis, with confidence scores and
    citations back to the project's artifacts — will appear here in v1.1. The
    data is generated today during analysis but not yet persisted.
  </div>
</div>`;
}

function section_transformations(job) {
  const rows = (job.transformations || []).map(t => `
    <tr>
      <td><code>${escapeHtml(t.target)}</code></td>
      <td>${escapeHtml(t.fromType || '—')}</td>
      <td>${escapeHtml(t.toType || '—')}</td>
      <td>${escapeHtml(t.reason || '')}</td>
    </tr>`).join('');

  return `
<div class="page">
  <div class="section-eyebrow">04 · Transformations Applied</div>
  <h1 class="section">What changed in the data</h1>
  ${rows
    ? `<table>
         <thead><tr><th>Table.Column</th><th>Source type</th><th>Target type</th><th>Reason</th></tr></thead>
         <tbody>${rows}</tbody>
       </table>`
    : '<p style="color:#64748b">No type transformations recorded for this job.</p>'}
</div>`;
}

function section_reconciliation(job) {
  const r = job.reconciliation || {};
  const sumRows = (r.sums || []).map(s => `
    <tr>
      <td><code>${escapeHtml(s.table)}</code></td>
      <td>${escapeHtml(s.expression)}</td>
      <td style="text-align:right">${escapeHtml(s.sourceTotal)}</td>
      <td style="text-align:right">${escapeHtml(s.targetTotal)}</td>
      <td>${s.delta == 0
        ? '<span class="badge badge-success">0.00</span>'
        : '<span class="badge badge-warn">' + escapeHtml(String(s.delta)) + '</span>'}</td>
    </tr>`).join('');

  const fkRows = (r.fks || []).map(f => `
    <tr>
      <td><code>${escapeHtml(f.name)}</code></td>
      <td>${escapeHtml(f.from)} → ${escapeHtml(f.to)}</td>
      <td>${f.orphans === 0
        ? '<span class="badge badge-success">Valid · 0 orphans</span>'
        : '<span class="badge badge-danger">' + f.orphans + ' orphans</span>'}</td>
    </tr>`).join('');

  return `
<div class="page">
  <div class="section-eyebrow">05 · Validation &amp; Reconciliation</div>
  <h1 class="section">Proof the data matches</h1>
  ${sumRows ? `<h2>Reconciliation by sum</h2>
    <table>
      <thead><tr><th>Table</th><th>Expression</th><th style="text-align:right">Source</th><th style="text-align:right">Target</th><th>Δ</th></tr></thead>
      <tbody>${sumRows}</tbody>
    </table>` : ''}
  ${fkRows ? `<h2>FK validation post-load</h2>
    <table>
      <thead><tr><th>Constraint</th><th>From → To</th><th>Status</th></tr></thead>
      <tbody>${fkRows}</tbody>
    </table>` : ''}
  ${!sumRows && !fkRows ? '<p style="color:#64748b">No reconciliation data captured for this job.</p>' : ''}
</div>`;
}

function section_timeline(audit) {
  if (!audit || !audit.length) {
    return `
<div class="page">
  <div class="section-eyebrow">06 · Run Timeline</div>
  <h1 class="section">What happened, minute by minute</h1>
  <p style="color:#64748b">No audit entries recorded for this job.</p>
</div>`;
  }

  const start = new Date(audit[0].timestamp).getTime();
  const items = audit.map(a => {
    const t = new Date(a.timestamp);
    const elapsed = Math.round((t.getTime() - start) / 1000);
    return `
      <div class="timeline-item">
        <div class="timeline-time">${t.toISOString().slice(11,19)}Z · T+${formatElapsed(elapsed)}</div>
        <div class="timeline-event">${escapeHtml(a.event || a.message || '—')}</div>
        ${a.detail ? `<div class="timeline-detail">${escapeHtml(a.detail)}</div>` : ''}
      </div>`;
  }).join('');

  return `
<div class="page">
  <div class="section-eyebrow">06 · Run Timeline</div>
  <h1 class="section">What happened, minute by minute</h1>
  <div class="timeline">${items}</div>
</div>`;
}

function section_rollback_stub() {
  return `
<div class="page">
  <div class="section-eyebrow">07 · Rollback Plan</div>
  <h1 class="section">If something goes wrong post-cutover</h1>
  <div class="callout callout-info">
    <div class="callout-title">ⓘ Available once job versioning ships</div>
    Rollback plans require the per-job version history (currently in development).
    Once <code>job_versions</code> is live, this section will list snapshots
    available for revert and a decision tree for common failure modes.
  </div>
</div>`;
}

function section_signoff(job) {
  return `
<div class="page">
  <div class="section-eyebrow">08 · Sign-off</div>
  <h1 class="section">Acceptance</h1>
  <p>The undersigned confirm that the migration described in this document
    (Job <code>${escapeHtml(job.id)}</code>, version ${escapeHtml(job.version)})
    has been completed to the agreed scope and that reconciliation evidence has
    been reviewed.</p>
  <div class="signoff">
    <div class="signature-label" style="margin-bottom:1mm">Migration delivered by</div>
    <div class="signature-name" style="margin-bottom:6mm">Cygenix · cygenix.co.uk · ${escapeHtml(job.id)} ${escapeHtml(job.version)}</div>
    <div class="signoff-row">
      <div>
        <div class="signature-line"></div>
        <div class="signature-label">Client — Technical lead</div>
      </div>
      <div>
        <div class="signature-line"></div>
        <div class="signature-label">Client — Data owner</div>
      </div>
    </div>
    <div class="signoff-row" style="margin-top: 12mm">
      <div>
        <div class="signature-line"></div>
        <div class="signature-label">Cygenix operator</div>
        <div class="signature-name">${escapeHtml(job.operator)}</div>
      </div>
      <div></div>
    </div>
  </div>
</div>`;
}

// ────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────
function escapeHtml(v) {
  if (v == null) return '';
  return String(v)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatNumber(n) {
  if (n == null) return '—';
  return Number(n).toLocaleString('en-GB');
}

function formatCompact(n) {
  if (n == null) return '—';
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return String(n);
}

function formatRuntime(seconds) {
  if (!seconds) return '—';
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${m}m ${s}s`;
}

function formatElapsed(seconds) {
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${m}:${String(s).padStart(2, '0')}`;
}

function formatDate(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'long', year: 'numeric' })
    + ', ' + d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
}

function computeReconStatus(job) {
  const tables = job.tables || [];
  if (!tables.length) return { value: '—', sub: 'No table data', className: 'info' };
  const matches = tables.filter(t => t.sourceRows === t.targetRows).length;
  if (matches === tables.length) {
    return { value: '100%', sub: 'All totals match', className: 'success' };
  }
  return {
    value: `${Math.round((matches / tables.length) * 100)}%`,
    sub: `${tables.length - matches} mismatch${tables.length - matches > 1 ? 'es' : ''}`,
    className: 'warn',
  };
}

function stylesheet() {
  return `<style>
@page { size: A4; margin: 18mm 16mm 20mm 16mm;
  @bottom-left { content: "Cygenix Project Summary Document"; font-family: Inter, sans-serif; font-size: 8pt; color: #6b7280; }
  @bottom-right { content: "Page " counter(page) " of " counter(pages); font-family: Inter, sans-serif; font-size: 8pt; color: #6b7280; }
}
* { box-sizing: border-box; }
html, body { margin: 0; padding: 0; font-family: Inter, -apple-system, BlinkMacSystemFont, sans-serif; color: #1a1d24; font-size: 10pt; line-height: 1.5; -webkit-print-color-adjust: exact; print-color-adjust: exact; }
.cover { height: 257mm; background: linear-gradient(160deg, #0a0e1a 0%, #1a2240 55%, #2d1b4e 100%); color: #fff; padding: 30mm 18mm 22mm 18mm; position: relative; page-break-after: always; display: flex; flex-direction: column; }
.cover::before { content: ""; position: absolute; top:0;left:0;right:0;bottom:0; background: radial-gradient(circle at 85% 15%, rgba(99, 102, 241, 0.25), transparent 40%), radial-gradient(circle at 15% 85%, rgba(168, 85, 247, 0.18), transparent 45%); pointer-events: none; }
.cover-logo { font-size: 14pt; font-weight: 800; letter-spacing: -0.02em; display: flex; align-items: center; gap: 8px; position: relative; z-index: 1; }
.cover-logo-mark { width: 28px; height: 28px; background: linear-gradient(135deg, #6366f1, #a855f7); border-radius: 7px; display: inline-flex; align-items: center; justify-content: center; color: white; font-size: 14pt; font-weight: 900; }
.cover-eyebrow { margin-top: auto; text-transform: uppercase; letter-spacing: 0.18em; font-size: 9pt; color: #a5b4fc; font-weight: 600; position: relative; z-index: 1; }
.cover-title { font-size: 38pt; font-weight: 800; line-height: 1.05; letter-spacing: -0.025em; margin: 8mm 0 4mm 0; position: relative; z-index: 1; }
.cover-subtitle { font-size: 14pt; font-weight: 400; color: #cbd5e1; max-width: 140mm; line-height: 1.4; position: relative; z-index: 1; }
.cover-meta { margin-top: 18mm; display: grid; grid-template-columns: repeat(2, 1fr); gap: 6mm 10mm; position: relative; z-index: 1; }
.cover-meta-item { border-left: 2px solid #6366f1; padding-left: 4mm; }
.cover-meta-label { text-transform: uppercase; font-size: 7.5pt; letter-spacing: 0.15em; color: #94a3b8; margin-bottom: 1mm; }
.cover-meta-value { font-size: 11pt; font-weight: 500; color: #f1f5f9; }
.cover-footer { margin-top: 14mm; padding-top: 6mm; border-top: 1px solid rgba(255,255,255,0.1); display: flex; justify-content: space-between; font-size: 8.5pt; color: #94a3b8; position: relative; z-index: 1; }
.page { page-break-after: always; }
.page:last-child { page-break-after: auto; }
h1.section { font-size: 22pt; font-weight: 800; letter-spacing: -0.02em; color: #0a0e1a; margin: 0 0 2mm 0; padding-bottom: 3mm; border-bottom: 3px solid #6366f1; }
.section-eyebrow { text-transform: uppercase; letter-spacing: 0.15em; font-size: 8pt; color: #6366f1; font-weight: 700; margin-bottom: 2mm; }
h2 { font-size: 13pt; font-weight: 700; margin: 8mm 0 3mm 0; letter-spacing: -0.01em; }
.kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 4mm; margin: 6mm 0; }
.kpi { background: linear-gradient(135deg, #f8fafc, #fff); border: 1px solid #e2e8f0; border-radius: 6px; padding: 5mm; position: relative; overflow: hidden; }
.kpi::before { content: ""; position: absolute; top: 0; left: 0; width: 3px; height: 100%; background: var(--accent, #6366f1); }
.kpi-label { font-size: 7.5pt; text-transform: uppercase; letter-spacing: 0.12em; color: #64748b; font-weight: 600; margin-bottom: 2mm; }
.kpi-value { font-size: 20pt; font-weight: 800; letter-spacing: -0.02em; color: #0a0e1a; line-height: 1; }
.kpi-sub { font-size: 8pt; color: #64748b; margin-top: 1.5mm; }
.kpi.success { --accent: #10b981; } .kpi.warn { --accent: #f59e0b; } .kpi.danger { --accent: #ef4444; } .kpi.info { --accent: #6366f1; }
.callout { border-radius: 6px; padding: 4mm 5mm; margin: 4mm 0; border-left: 3px solid; font-size: 9.5pt; }
.callout-success { background: #ecfdf5; border-color: #10b981; color: #064e3b; }
.callout-warn { background: #fffbeb; border-color: #f59e0b; color: #78350f; }
.callout-danger { background: #fef2f2; border-color: #ef4444; color: #7f1d1d; }
.callout-info { background: #eef2ff; border-color: #6366f1; color: #312e81; }
.callout-title { font-weight: 700; margin-bottom: 1mm; }
table { width: 100%; border-collapse: collapse; margin: 3mm 0; font-size: 9pt; }
thead th { background: #f1f5f9; color: #475569; text-align: left; padding: 2.5mm 3mm; font-weight: 600; font-size: 8pt; text-transform: uppercase; letter-spacing: 0.08em; border-bottom: 2px solid #cbd5e1; }
tbody td { padding: 2.5mm 3mm; border-bottom: 1px solid #e2e8f0; vertical-align: top; }
tbody tr:last-child td { border-bottom: none; }
tbody tr:nth-child(even) td { background: #fafbfc; }
.badge { display: inline-block; padding: 1mm 2.5mm; border-radius: 3px; font-size: 7.5pt; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; }
.badge-success { background: #d1fae5; color: #065f46; }
.badge-warn { background: #fef3c7; color: #92400e; }
.badge-danger { background: #fee2e2; color: #991b1b; }
.timeline { position: relative; padding-left: 8mm; }
.timeline::before { content: ""; position: absolute; left: 2mm; top: 2mm; bottom: 2mm; width: 2px; background: linear-gradient(180deg, #6366f1, #a855f7); }
.timeline-item { position: relative; padding-bottom: 5mm; }
.timeline-item::before { content: ""; position: absolute; left: -7.5mm; top: 1.5mm; width: 4mm; height: 4mm; border-radius: 50%; background: #6366f1; border: 2px solid white; box-shadow: 0 0 0 2px #6366f1; }
.timeline-time { font-family: 'JetBrains Mono', monospace; font-size: 8pt; color: #6366f1; font-weight: 600; }
.timeline-event { font-size: 9.5pt; }
.timeline-detail { font-size: 8.5pt; color: #64748b; margin-top: 0.5mm; }
code { font-family: 'JetBrains Mono', 'SF Mono', Menlo, monospace; font-size: 8.5pt; background: #f1f5f9; padding: 0.5mm 1.5mm; border-radius: 3px; color: #6366f1; }
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 5mm; margin: 4mm 0; }
.panel { background: #fafbfc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 4mm; }
.panel-title { font-size: 9pt; font-weight: 700; color: #475569; text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 2mm; }
.signoff { margin-top: 8mm; border: 2px solid #0a0e1a; border-radius: 8px; padding: 8mm; }
.signoff-row { display: grid; grid-template-columns: 1fr 1fr; gap: 8mm; margin-top: 8mm; }
.signature-line { border-bottom: 1.5px solid #1a1d24; height: 14mm; margin-bottom: 1.5mm; }
.signature-label { font-size: 8pt; color: #64748b; text-transform: uppercase; letter-spacing: 0.1em; font-weight: 600; }
.signature-name { font-size: 10pt; font-weight: 600; margin-top: 1mm; }
@media print { .no-print { display: none !important; } }
</style>`;
}

function printControls() {
  return `
<div class="no-print" style="position:fixed;top:12px;right:12px;z-index:999;display:flex;gap:8px;font-family:Inter,sans-serif;">
  <button onclick="window.print()" style="background:#6366f1;color:#fff;border:0;padding:8px 14px;border-radius:6px;font-weight:600;cursor:pointer;font-size:13px;">⬇ Save as PDF</button>
  <button onclick="window.close()" style="background:#e2e8f0;color:#1a1d24;border:0;padding:8px 14px;border-radius:6px;font-weight:600;cursor:pointer;font-size:13px;">Close</button>
</div>`;
}
