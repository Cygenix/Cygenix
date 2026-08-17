/* cygenix-resume.js — self-healing resume.
 *
 * A load dies at row 4,000,000. Instead of "run it again and lose six
 * hours", the failure is diagnosed at the moment it happens — the runner
 * still holds the failing rows in memory — a mapping patch is proposed
 * (the editor's own transforms: SAFE_TRUNC, DATE_CAST, SAFE_NUMERIC), and
 * the run resumes from the exact failure offset under the same run id, so
 * staging and the final load see one continuous run.
 *
 * Three cooperating pieces, pure and testable:
 *   rsClassifyError  — what SQL Server said, decoded (column extracted
 *                      from the message when it names one; transient
 *                      deadlock/timeout flagged as retry-without-patch)
 *   rsScanRows       — the failing rows themselves, checked value-by-value
 *                      against the target types (via the preflight engine's
 *                      checkers, injected) to name the culprit column when
 *                      the error message doesn't
 *   rsResumePlan     — where it is safe to restart from, given what the
 *                      backend reports. Honest about the unsafe cases:
 *                      a plan is only offered when staging is provably
 *                      contiguous or can be made so by deleting the
 *                      failing page's rows by source key.
 *
 * The runner (project-builder-app.js) wires these into its failure paths
 * and paints the resume card on the step.
 */

(function (root) {
  'use strict';

  // ── Error classification ──────────────────────────────────────────────────
  // The classic SQL Server failure messages, with the column pulled out of
  // the text when the engine names it (2017+ truncation messages do; NULL
  // violations always have; conversions usually don't).

  const PATTERNS = [
    { code: 'truncation', re: /string or binary data would be truncated.*?column '([^']+)'/i, colGroup: 1 },
    { code: 'truncation', re: /string or binary data would be truncated/i },
    { code: 'null-violation', re: /cannot insert the value null into column '([^']+)'/i, colGroup: 1 },
    { code: 'bad-date', re: /conversion failed when converting date and\/or time/i },
    { code: 'bad-date', re: /conversion failed when converting .* to (?:a )?date/i },
    { code: 'bad-date', re: /out-of-range value|out of range.*datetime/i },
    { code: 'overflow', re: /arithmetic overflow/i },
    { code: 'not-numeric', re: /error converting data type \w+ to (?:numeric|decimal|int|bigint|float|money)/i },
    { code: 'not-numeric', re: /conversion failed when converting the n?varchar value '([^']*)'.*to data type (?:int|numeric|decimal|bigint)/i },
    { code: 'fk-violation', re: /conflicted with the foreign key constraint "([^"]+)"/i, nameGroup: 1 },
    { code: 'pk-violation', re: /violation of (?:primary key|unique key|unique index) constraint '?"?([^'".]+)/i, nameGroup: 1 },
    { code: 'deadlock', re: /deadlocked on lock resources|error 1205/i, transient: true },
    { code: 'timeout', re: /timeout expired|etimedout|econnreset|socket hang up|connection (?:was )?closed|network error/i, transient: true },
    { code: 'permission', re: /permission was denied|login failed/i, fatal: true },
  ];

  function rsClassifyError(message) {
    const msg = String(message || '');
    for (const p of PATTERNS) {
      const m = p.re.exec(msg);
      if (!m) continue;
      return {
        code: p.code,
        column: p.colGroup && m[p.colGroup] ? m[p.colGroup] : null,
        constraint: p.nameGroup && m[p.nameGroup] ? m[p.nameGroup] : null,
        transient: !!p.transient,
        fatal: !!p.fatal,
      };
    }
    return { code: 'unknown', column: null, transient: false, fatal: false };
  }

  // ── Value-level scan of the failing rows ──────────────────────────────────
  // checkers = { pfParseType, pfCheckValue } from cygenix-preflight — the
  // exact same maths the preflight forecast uses, now pointed at the ~100
  // rows that actually failed. Returns per-column findings so the culprit
  // is named even when the error message is vague, with example values.

  function rsScanRows(rows, mapping, targetColsByName, checkers) {
    const out = [];
    if (!rows || !rows.length || !checkers) return out;
    for (const m of (mapping || [])) {
      if (!m || !m.srcCol || !m.tgtCol) continue;
      const tc = targetColsByName[String(m.tgtCol).toLowerCase()];
      if (!tc || tc.isIdentity) continue;
      const spec = checkers.pfParseType(tc.type);
      spec.nullable = tc.nullable; spec.isIdentity = tc.isIdentity; spec.hasDefault = !!tc.default;
      const counts = {};
      for (const r of rows) {
        const v = r[m.srcCol];
        if (v === null || v === undefined) {
          if (!tc.nullable && !tc.default) (counts['null-violation'] = counts['null-violation'] || { n: 0, ex: [] }).n++;
          continue;
        }
        const f = checkers.pfCheckValue(v, spec);
        if (f) {
          const c = (counts[f.code] = counts[f.code] || { n: 0, ex: [] });
          c.n++;
          if (c.ex.length < 3) c.ex.push(String(v).slice(0, 60));
        }
      }
      for (const [code, c] of Object.entries(counts)) {
        out.push({ code, srcCol: m.srcCol, tgtCol: m.tgtCol, tgtType: tc.type, count: c.n, examples: c.ex });
      }
    }
    out.sort((a, b) => b.count - a.count);
    return out;
  }

  // ── Patch proposal ────────────────────────────────────────────────────────
  // The editor already owns the safe transforms; a patch is just choosing
  // one for the culprit column. Codes with no safe transform get advice
  // instead of a button.

  const PATCH_FOR = {
    'truncation':   { transform: 'SAFE_TRUNC',   why: 'truncate to the target length with LEFT() so the row loads' },
    'bad-date':     { transform: 'DATE_CAST',    why: 'TRY_CONVERT with range clamping — unparseable dates load as NULL instead of killing the batch' },
    'date-range':   { transform: 'DATE_CAST',    why: 'TRY_CONVERT with range clamping — out-of-range dates load as NULL instead of killing the batch' },
    'not-numeric':  { transform: 'SAFE_NUMERIC', why: 'TRY_CONVERT — non-numeric values load as NULL instead of killing the batch' },
    'overflow':     { transform: 'SAFE_NUMERIC', why: 'TRY_CONVERT — overflowing values load as NULL instead of killing the batch' },
    'fraction-loss':{ transform: 'SAFE_NUMERIC', why: 'TRY_CONVERT handles the cast explicitly' },
  };

  function rsProposePatch(classified, scan) {
    if (classified.transient) {
      return { kind: 'retry', why: 'the failure was transient (' + classified.code + ') — resume without changes and it will likely go through' };
    }
    if (classified.fatal) {
      return { kind: 'none', why: 'this is not a data problem (' + classified.code + ') — fix access/permissions, then resume' };
    }
    // Pick the column: from the error message when named, else the scan's
    // top finding whose code family agrees with (or refines) the message.
    let code = classified.code, srcCol = null, tgtCol = classified.column, examples = [];
    const hit = (scan || []).find(f =>
      (tgtCol && f.tgtCol.toLowerCase() === tgtCol.toLowerCase())
      || (!tgtCol && (code === 'unknown' || f.code === code
           || (code === 'not-numeric' && ['overflow', 'scale-round'].includes(f.code)))));
    if (hit) { code = hit.code === 'scale-round' ? 'not-numeric' : hit.code; srcCol = hit.srcCol; tgtCol = hit.tgtCol; examples = hit.examples; }
    if (code === 'null-violation') {
      return { kind: 'advise', code, srcCol, tgtCol, examples,
        why: 'NULLs are arriving in a NOT NULL column — set a Fixed value on the mapping row (or fix the source), then resume' };
    }
    if (code === 'pk-violation') {
      return { kind: 'advise', code, tgtCol,
        why: 'duplicate keys — the rows may already be loaded from an earlier attempt, or the source has duplicates. Check the target, then resume or roll back first' };
    }
    if (code === 'fk-violation') {
      return { kind: 'advise', code, constraint: classified.constraint,
        why: 'a parent row is missing — load the parent table first (check the group order), then resume' };
    }
    const p = PATCH_FOR[code];
    if (p && tgtCol) {
      return { kind: 'patch', code, srcCol, tgtCol, transform: p.transform, examples,
        why: p.why + (examples.length ? ' — failing values: ' + examples.join(', ') : '') };
    }
    return { kind: 'none', code, why: 'no safe automatic patch for this failure — inspect the captured SQL, fix the mapping, then resume' };
  }

  // ── Resume plan ───────────────────────────────────────────────────────────
  // Where can the run restart WITHOUT duplicating or dropping rows?
  //
  //   pageStartOffset  source offset of the page being processed on failure
  //   stagedBefore     rows staged in earlier pages (contiguous by construction)
  //   results          the backend's per-statement results for the failing page
  //   stoppedEarly     backend honoured stopOnError
  //   thrown           the page died on a thrown call (staging state unknown)
  //   keyed            rows carry a real source key in dm_source_row_id
  //
  // Returns { resumable, resumeOffset, cleanupKeysFrom, reason }.
  //   cleanupKeysFrom: row index within the page from which staged rows must
  //   be deleted by key before resuming (null = no cleanup needed).

  function rsResumePlan(input) {
    const { pageStartOffset, results, stoppedEarly, thrown, keyed, batchSize } = input;
    if (thrown) {
      // The batch call itself died — how much of the page staged is unknown.
      if (keyed) {
        return { resumable: true, resumeOffset: pageStartOffset, cleanupKeysFrom: 0,
          reason: 'the failing page\'s staged rows are removed by source key, then the run restarts at the page boundary' };
      }
      return { resumable: false,
        reason: 'the failure left staging in an unknown state and the source rows have no key column to clean up by — restart the step (or add an id-like column to the mapping first)' };
    }
    const failIdx = (results || []).findIndex(r => r && r.success === false);
    if (failIdx < 0) {
      // No in-page failure — the run failed before/after this page; resume
      // at the contiguous staged count.
      const staged = (results || []).reduce((a, r) => a + ((r && r.rowsAffected) || 0), 0);
      return { resumable: true, resumeOffset: pageStartOffset + staged, cleanupKeysFrom: null, reason: 'staging is contiguous' };
    }
    const stagedThisPage = results.slice(0, failIdx).reduce((a, r) => a + ((r && r.rowsAffected) || 0), 0);
    const laterSuccess = results.slice(failIdx + 1).some(r => r && r.success);
    if (!laterSuccess || stoppedEarly) {
      return { resumable: true, resumeOffset: pageStartOffset + stagedThisPage, cleanupKeysFrom: null,
        reason: 'the backend stopped at the failing statement, so staging is contiguous to the failure point' };
    }
    // Statements after the failure also ran (backend without stopOnError):
    // there is a hole. Only a keyed cleanup restores contiguity.
    if (keyed) {
      return { resumable: true, resumeOffset: pageStartOffset + stagedThisPage, cleanupKeysFrom: failIdx * batchSize,
        reason: 'rows staged after the failure are removed by source key to close the hole, then the run resumes at the failure point' };
    }
    return { resumable: false,
      reason: 'the backend continued past the failing statement and the rows have no key column — staging has a hole that cannot be closed safely. Restart the step' };
  }

  // Delete-staged-rows-by-key SQL for the cleanup step. Keys are the
  // dm_source_row_id values of the page rows from cleanupKeysFrom onward.
  function rsCleanupSql(stagingFull, runId, keys) {
    const lit = (v) => "N'" + String(v).replace(/'/g, "''") + "'";
    const chunks = [];
    for (let i = 0; i < keys.length; i += 500) {
      chunks.push('DELETE FROM ' + stagingFull + " WHERE [dm_run_id] = " + lit(runId)
        + ' AND [dm_source_row_id] IN (' + keys.slice(i, i + 500).map(lit).join(',') + ')');
    }
    return chunks;
  }

  // ── Persistence (browser) ────────────────────────────────────────────────
  const STORE = 'cygenix_resume_points';
  const stepKey = (projectId, step) => projectId + '|' + (step.jobId || '') + '|' + (step.tgtTable || '');

  function rsSave(projectId, step, point) {
    try {
      const all = JSON.parse(localStorage.getItem(STORE) || '{}');
      all[stepKey(projectId, step)] = point;
      localStorage.setItem(STORE, JSON.stringify(all));
    } catch {}
  }
  function rsLoad(projectId, step) {
    try { return JSON.parse(localStorage.getItem(STORE) || '{}')[stepKey(projectId, step)] || null; }
    catch { return null; }
  }
  function rsClear(projectId, step) {
    try {
      const all = JSON.parse(localStorage.getItem(STORE) || '{}');
      delete all[stepKey(projectId, step)];
      localStorage.setItem(STORE, JSON.stringify(all));
    } catch {}
  }

  const api = { rsClassifyError, rsScanRows, rsProposePatch, rsResumePlan, rsCleanupSql,
                rsSave, rsLoad, rsClear, PATCH_FOR };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  root.CygenixResume = api;
})(typeof window !== 'undefined' ? window : globalThis);
