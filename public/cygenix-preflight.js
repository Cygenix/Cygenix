/* cygenix-preflight.js — the Preflight Simulator.
 *
 * Before anyone hits Run, simulate the migration on a sample of the real
 * source data against the real target schema and forecast what will break:
 * projected failure counts per job, the specific values that will reject
 * (truncation, numeric overflow, unparseable dates, NOT NULL violations,
 * unicode narrowing, FK orphans) and an estimated runtime — as a
 * green / amber / red report. The six-hour load that dies at 94% on a
 * varchar(50) becomes a ninety-second forecast.
 *
 * Everything here is READ-ONLY: the engine issues SELECTs through the same
 * db-connect path the runner uses, so it inherits authentication and the
 * RBAC gate (reads are permitted and logged even where writes are not).
 * Nothing is written to either database.
 *
 * Layout: a pure core (type parsing, per-value checks, projection, verdicts,
 * the Cutover Confidence roll-up) with no DOM or network so the test suite
 * loads it directly, then a browser orchestrator (pfRunPreflight) that takes
 * a dbCall function so tests can stub the wire.
 *
 * Severity model — red rejects the row, amber loads it changed:
 *   red   truncation, overflow, not-numeric, bad-date, date-range,
 *         null-violation, unmapped-required, fk-orphan, bad-guid
 *   amber scale-round (SQL rounds silently), fraction-loss (int cast),
 *         unicode-loss (varchar '?' substitution)
 */

(function (root) {
  'use strict';

  // ── Type parsing ──────────────────────────────────────────────────────────
  // schema-columns returns ready-to-use type strings: VARCHAR(50),
  // DECIMAL(10,2), INT, NVARCHAR(MAX), DATETIME2(7)…

  const INT_RANGES = {
    TINYINT:  [0, 255],
    SMALLINT: [-32768, 32767],
    INT:      [-2147483648, 2147483647],
    BIGINT:   [-9223372036854775808, 9223372036854775807], // approx beyond 2^53; good enough for a forecast
  };

  function pfParseType(typeStr) {
    const m = /^([A-Z_ ]+?)\s*(?:\(\s*([^)]+)\s*\))?$/i.exec(String(typeStr || '').trim());
    const base = m ? m[1].toUpperCase().trim() : 'UNKNOWN';
    const args = m && m[2] ? m[2].split(',').map(s => s.trim().toUpperCase()) : [];
    const spec = { base, raw: typeStr };
    if (base === 'CHAR' || base === 'VARCHAR' || base === 'TEXT') {
      spec.family = 'string'; spec.unicode = false;
      spec.length = args[0] === 'MAX' ? Infinity : (parseInt(args[0], 10) || (base === 'TEXT' ? Infinity : 1));
    } else if (base === 'NCHAR' || base === 'NVARCHAR' || base === 'NTEXT') {
      spec.family = 'string'; spec.unicode = true;
      spec.length = args[0] === 'MAX' ? Infinity : (parseInt(args[0], 10) || (base === 'NTEXT' ? Infinity : 1));
    } else if (INT_RANGES[base]) {
      spec.family = 'int'; spec.min = INT_RANGES[base][0]; spec.max = INT_RANGES[base][1];
    } else if (base === 'DECIMAL' || base === 'NUMERIC' || base === 'DEC') {
      spec.family = 'decimal';
      spec.precision = parseInt(args[0], 10) || 18;
      spec.scale = parseInt(args[1], 10) || 0;
    } else if (base === 'MONEY')      { spec.family = 'decimal'; spec.precision = 19; spec.scale = 4; }
    else if (base === 'SMALLMONEY')   { spec.family = 'decimal'; spec.precision = 10; spec.scale = 4; }
    else if (base === 'FLOAT' || base === 'REAL') { spec.family = 'float'; }
    else if (base === 'DATE')         { spec.family = 'date'; }
    else if (base === 'DATETIME')     { spec.family = 'datetime'; spec.minYear = 1753; }
    else if (base === 'SMALLDATETIME'){ spec.family = 'datetime'; spec.minYear = 1900; spec.maxYear = 2079; }
    else if (base === 'DATETIME2' || base === 'DATETIMEOFFSET') { spec.family = 'datetime'; spec.minYear = 1; }
    else if (base === 'TIME')         { spec.family = 'time'; }
    else if (base === 'BIT')          { spec.family = 'bool'; }
    else if (base === 'UNIQUEIDENTIFIER') { spec.family = 'guid'; }
    else if (base === 'BINARY' || base === 'VARBINARY' || base === 'IMAGE' || base === 'TIMESTAMP' || base === 'ROWVERSION') { spec.family = 'binary'; }
    else spec.family = 'other';
    return spec;
  }

  // ── Value parsing helpers ─────────────────────────────────────────────────

  const GUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const ISO_RE  = /^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?([+-]\d{2}:?\d{2}|Z)?)?$/;
  const DMY_RE  = /^(\d{1,2})[\/.-](\d{1,2})[\/.-](\d{4})( .*)?$/;

  // A value SQL Server could land in a date column. Returns the year, or
  // null when unparseable. dd/mm/yyyy and mm/dd/yyyy are both accepted when
  // either reading is a real date — preflight flags impossibilities, not
  // regional settings.
  function pfDateYear(v) {
    if (v instanceof Date) return isNaN(v) ? null : v.getUTCFullYear();
    if (typeof v === 'number') return null;             // numbers don't cast to dates meaningfully here
    const s = String(v).trim();
    if (!s) return null;
    if (ISO_RE.test(s)) {
      const d = new Date(s.slice(0, 10) + 'T00:00:00Z');
      return isNaN(d) ? null : d.getUTCFullYear();
    }
    const m = DMY_RE.exec(s);
    if (m) {
      const a = +m[1], b = +m[2], y = +m[3];
      const dmyOk = a >= 1 && a <= 31 && b >= 1 && b <= 12;
      const mdyOk = a >= 1 && a <= 12 && b >= 1 && b <= 31;
      return (dmyOk || mdyOk) ? y : null;
    }
    const d = new Date(s);
    return isNaN(d) ? null : d.getUTCFullYear();
  }

  function pfNumeric(v) {
    if (typeof v === 'number') return isFinite(v) ? v : null;
    if (typeof v === 'boolean') return v ? 1 : 0;
    const s = String(v).trim();
    if (!s || !/^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/.test(s)) return null;
    const n = Number(s);
    return isFinite(n) ? n : null;
  }

  function decimalsOf(v) {
    const s = typeof v === 'number' ? String(v) : String(v).trim();
    const i = s.indexOf('.');
    if (i < 0) return 0;
    return s.slice(i + 1).replace(/[eE].*$/, '').replace(/0+$/, '').length;
  }

  // ── Per-value check ───────────────────────────────────────────────────────
  // Returns null (fits) or { code } naming why this value fails or changes.

  function pfCheckValue(v, spec) {
    if (v === null || v === undefined) return null;      // null handling is a column-level check
    switch (spec.family) {
      case 'string': {
        const s = String(v);
        if (s.length > spec.length) return { code: 'truncation' };
        if (!spec.unicode && /[^\x00-\xFF]/.test(s)) return { code: 'unicode-loss' };
        return null;
      }
      case 'int': {
        const n = pfNumeric(v);
        if (n === null) return { code: 'not-numeric' };
        if (n < spec.min || n > spec.max) return { code: 'overflow' };
        if (n % 1 !== 0) return { code: 'fraction-loss' };
        return null;
      }
      case 'decimal': {
        const n = pfNumeric(v);
        if (n === null) return { code: 'not-numeric' };
        if (Math.abs(n) >= Math.pow(10, spec.precision - spec.scale)) return { code: 'overflow' };
        if (decimalsOf(v) > spec.scale) return { code: 'scale-round' };
        return null;
      }
      case 'float': {
        return pfNumeric(v) === null ? { code: 'not-numeric' } : null;
      }
      case 'date': case 'datetime': {
        const y = pfDateYear(v);
        if (y === null) return { code: 'bad-date' };
        if (spec.minYear && y < spec.minYear) return { code: 'date-range' };
        if (spec.maxYear && y > spec.maxYear) return { code: 'date-range' };
        return null;
      }
      case 'bool': {
        const s = String(v).trim().toLowerCase();
        return ['0','1','true','false',''].includes(s) ? null : { code: 'not-numeric' };
      }
      case 'guid': {
        return GUID_RE.test(String(v).trim()) ? null : { code: 'bad-guid' };
      }
      default:
        return null;                                     // binary / other — nothing to forecast
    }
  }

  const CHECK_META = {
    'truncation':       { sev: 'red',   label: 'String longer than target column',   why: 'the insert rejects the row ("string or binary data would be truncated")' },
    'overflow':         { sev: 'red',   label: 'Number too large for target type',   why: 'arithmetic overflow rejects the row' },
    'not-numeric':      { sev: 'red',   label: 'Value is not numeric',               why: 'the cast fails and rejects the row' },
    'bad-date':         { sev: 'red',   label: 'Value does not parse as a date',     why: 'the date conversion fails and rejects the row' },
    'date-range':       { sev: 'red',   label: 'Date outside the target type range', why: 'DATETIME accepts 1753+ (SMALLDATETIME 1900–2079); out-of-range rejects' },
    'null-violation':   { sev: 'red',   label: 'NULL into a NOT NULL column',        why: 'the constraint rejects the row' },
    'unmapped-required':{ sev: 'red',   label: 'Required column has no mapping',     why: 'NOT NULL, no default, not identity, nothing mapped — every row rejects' },
    'fk-orphan':        { sev: 'red',   label: 'Foreign-key value has no parent',    why: 'the FK constraint rejects the row' },
    'bad-guid':         { sev: 'red',   label: 'Value is not a UNIQUEIDENTIFIER',    why: 'the cast fails and rejects the row' },
    'scale-round':      { sev: 'amber', label: 'More decimals than target scale',    why: 'SQL Server rounds silently — values change' },
    'fraction-loss':    { sev: 'amber', label: 'Fraction into an integer column',    why: 'the fraction is lost silently' },
    'unicode-loss':     { sev: 'amber', label: 'Unicode into a non-unicode column',  why: "characters outside the code page become '?'" },
    'preflight-error':  { sev: 'unknown', label: 'Preflight could not analyse',      why: 'see the detail — the job itself may still be fine' },
  };

  // ── Column-level aggregation ──────────────────────────────────────────────
  // samples: array of raw values for one source column. Returns findings
  // keyed by code with counts and up to three example values.

  function pfCheckColumn(samples, spec, opts) {
    opts = opts || {};
    const out = {};
    const hit = (code, v) => {
      const f = out[code] || (out[code] = { code, count: 0, examples: [] });
      f.count++;
      if (f.examples.length < 3) {
        const s = v === null || v === undefined ? 'NULL' : String(v);
        f.examples.push(s.length > 60 ? s.slice(0, 57) + '…' : s);
      }
    };
    const needsValue = !spec.nullable && !spec.isIdentity && !spec.hasDefault && !opts.hasLiteral;
    for (const v of samples) {
      if (v === null || v === undefined) {
        if (needsValue) hit('null-violation', v);
        continue;
      }
      const r = pfCheckValue(v, spec);
      if (r) hit(r.code, v);
    }
    return Object.values(out);
  }

  // Target NOT NULL columns nothing feeds: no mapping, no literal, no
  // default, not identity — every inserted row fails.
  function pfUnmappedRequired(targetCols, mapping) {
    const fed = new Set((mapping || [])
      .filter(m => m && m.tgtCol && (m.srcCol || m.literalValue || m.fixedValue))
      .map(m => m.tgtCol.toLowerCase()));
    return (targetCols || [])
      .filter(c => !c.nullable && !c.isIdentity && !c.default && !fed.has(c.name.toLowerCase()))
      .map(c => ({ code: 'unmapped-required', column: c.name, count: null, examples: [] }));
  }

  // ── Projection ────────────────────────────────────────────────────────────
  // Sample failure rate → projected rows over the full load. Never projects
  // below the observed count, never above the total.

  function pfProject(sampleFails, sampleSize, totalRows) {
    if (!sampleSize || !sampleFails) return { projected: 0, pct: 0 };
    const rate = sampleFails / sampleSize;
    const projected = Math.min(totalRows || sampleFails,
      Math.max(sampleFails, Math.round(rate * (totalRows || sampleSize))));
    return { projected, pct: rate * 100 };
  }

  // ── Verdicts ──────────────────────────────────────────────────────────────

  function pfSeverity(code) { return (CHECK_META[code] || {}).sev || 'red'; }

  function pfJobVerdict(findings) {
    let amber = false, unknown = false;
    for (const f of findings || []) {
      const sev = pfSeverity(f.code);
      if (sev === 'red') return 'red';
      if (sev === 'amber') amber = true;
      if (sev === 'unknown') unknown = true;
    }
    return amber ? 'amber' : (unknown ? 'unknown' : 'green');
  }

  function pfOverallVerdict(jobs) {
    const vs = (jobs || []).map(j => j.verdict);
    if (vs.includes('red')) return 'red';
    if (vs.includes('amber')) return 'amber';
    if (vs.length && vs.every(v => v === 'unknown')) return 'unknown';
    return vs.length ? 'green' : 'unknown';
  }

  // ── Runtime estimate ──────────────────────────────────────────────────────
  // Anchored on the measured read throughput of the sample fetch, derated
  // for insert + transform overhead. A forecast, labelled as one.

  function pfRuntimeEstimate(totalRows, sampleRows, sampleMs) {
    if (!totalRows) return { seconds: 0, basis: 'no rows' };
    if (!sampleRows || !sampleMs) return { seconds: null, basis: 'no sample timing' };
    const readRps = sampleRows / (sampleMs / 1000);
    const effRps = Math.max(50, readRps * 0.35);         // write path ≈ ⅓ of read throughput
    return { seconds: Math.max(1, Math.round(totalRows / effRps)), basis: Math.round(effRps) + ' rows/s est.' };
  }

  function pfFmtDuration(s) {
    if (s === null || s === undefined) return '—';
    if (s < 60) return s + 's';
    if (s < 3600) return Math.floor(s / 60) + 'm ' + (s % 60) + 's';
    return Math.floor(s / 3600) + 'h ' + Math.floor((s % 3600) / 60) + 'm';
  }

  // ── Cutover Confidence ────────────────────────────────────────────────────
  // One number a programme director can look at, rolled up honestly:
  // components without data are excluded, not faked, and the breakdown says
  // which ones counted.

  const PF_JOB_SCORE = { green: 100, amber: 72, red: 22, unknown: 60 };

  function pfConfidence(input) {
    input = input || {};
    const parts = [];

    // Mapping health: average match confidence over mapped columns. The
    // editor writes grades (HIGH/MEDIUM/LOW, or an evidence score); older
    // AI-produced jobs carry numbers. Both count.
    const GRADE_SCORE = { HIGH: 95, MEDIUM: 70, LOW: 40, auto: 85 };
    const jobs = input.jobs || [];
    const matches = [];
    jobs.forEach(j => (j.columnMapping || []).forEach(m => {
      if (!m || !m.tgtCol) return;
      if (m.evidence && typeof m.evidence.score === 'number') matches.push(Math.max(0, Math.min(100, m.evidence.score)));
      else if (typeof m.match === 'number') matches.push(Math.max(0, Math.min(100, m.match)));
      else if (GRADE_SCORE[m.match]) matches.push(GRADE_SCORE[m.match]);
    }));
    parts.push({
      key: 'mapping', label: 'Mapping confidence', weight: 30,
      hasData: matches.length > 0,
      score: matches.length ? Math.round(matches.reduce((a, b) => a + b, 0) / matches.length) : null,
      note: matches.length ? matches.length + ' mapped columns' : 'no scored mappings yet',
    });

    // Preflight risk: the latest report's per-job verdicts, decayed if stale.
    const pf = input.preflight;
    if (pf && pf.jobs && pf.jobs.length) {
      let score = Math.round(pf.jobs.reduce((a, j) => a + (PF_JOB_SCORE[j.verdict] || 60), 0) / pf.jobs.length);
      const now = input.now || Date.now();
      const ageDays = pf.generatedAt ? (now - new Date(pf.generatedAt).getTime()) / 86400000 : 0;
      let note = pf.jobs.length + ' jobs preflighted';
      if (ageDays > 7) { score = Math.max(0, score - 15); note += ' · stale (' + Math.floor(ageDays) + 'd old)'; }
      parts.push({ key: 'preflight', label: 'Preflight risk', weight: 40, hasData: true, score, note });
    } else {
      parts.push({ key: 'preflight', label: 'Preflight risk', weight: 40, hasData: false, score: null, note: 'no preflight run yet' });
    }

    // Delivery state: how much of the plan has reached SQL-ready/complete.
    const buckets = jobs.map(j => j.status || '').filter(Boolean);
    const doneish = jobs.filter(j => ['complete', 'ready', 'sql-generated'].includes(String(j.status || '').toLowerCase())).length;
    parts.push({
      key: 'delivery', label: 'Jobs ready', weight: 30,
      hasData: buckets.length > 0,
      score: buckets.length ? Math.round(doneish / jobs.length * 100) : null,
      note: buckets.length ? doneish + ' of ' + jobs.length + ' jobs ready or complete' : 'no jobs yet',
    });

    const live = parts.filter(p => p.hasData);
    if (!live.length) return { score: null, grade: 'no-data', parts };
    const wsum = live.reduce((a, p) => a + p.weight, 0);
    let score = Math.round(live.reduce((a, p) => a + p.score * p.weight, 0) / wsum);
    // A red preflight is a forecast of rejected rows — good mapping scores
    // must not average it away. Any red job caps the number at caution;
    // an entirely red preflight caps it at not-ready.
    if (pf && pf.jobs && pf.jobs.length) {
      const reds = pf.jobs.filter(j => j.verdict === 'red').length;
      if (reds === pf.jobs.length && reds) score = Math.min(score, 45);
      else if (reds) score = Math.min(score, 65);
    }
    const grade = score >= 85 ? 'green' : score >= 60 ? 'amber' : 'red';
    return { score, grade, parts };
  }

  // ── SQL building helpers (pure, tested) ───────────────────────────────────

  function pfQuoteIdent(name) { return '[' + String(name).replace(/]/g, ']]') + ']'; }

  function pfSampleSql(srcTable, srcCols, srcWhere, n) {
    const cols = srcCols.length ? srcCols.map(pfQuoteIdent).join(', ') : '*';
    const wh = String(srcWhere || '').trim().replace(/^WHERE\s+/i, '');
    return 'SELECT TOP (' + (n | 0) + ') ' + cols + ' FROM ' + srcTable + (wh ? ' WHERE ' + wh : '');
  }

  function pfCountSql(srcTable, srcWhere) {
    const wh = String(srcWhere || '').trim().replace(/^WHERE\s+/i, '');
    return 'SELECT COUNT_BIG(*) AS n FROM ' + srcTable + (wh ? ' WHERE ' + wh : '');
  }

  function pfSqlLit(v) {
    if (typeof v === 'number' && isFinite(v)) return String(v);
    return "N'" + String(v).replace(/'/g, "''") + "'";
  }

  // Existence probe for FK orphans: how many of these sampled child values
  // have a parent in the authority table. VALUES constructor caps at 1000;
  // we stay well under.
  function pfFkProbeSql(values, authorityTable, authorityCol, authorityWhere) {
    const rows = values.slice(0, 200).map(v => '(' + pfSqlLit(v) + ')').join(',');
    const wh = String(authorityWhere || '').trim().replace(/^WHERE\s+/i, '');
    return 'SELECT COUNT(*) AS hits FROM (VALUES ' + rows + ') v(val) '
      + 'WHERE EXISTS (SELECT 1 FROM ' + authorityTable + ' a WHERE a.' + pfQuoteIdent(authorityCol) + ' = v.val'
      + (wh ? ' AND ' + wh.replace(/\b(\w+)\./g, 'a.') : '') + ')';
  }

  function splitTable(full) {
    const parts = String(full || '').replace(/[[\]]/g, '').split('.');
    return parts.length >= 2 ? { schema: parts[parts.length - 2], table: parts[parts.length - 1] }
                             : { schema: 'dbo', table: parts[0] || '' };
  }

  // ── Orchestrator ──────────────────────────────────────────────────────────
  // steps: migration steps ({srcTable, tgtTable, srcWhere, columnMapping,
  // name, jobType, type}); dbCall(conn, body) → db-connect response.

  async function pfRunPreflight(steps, opts) {
    const { srcConn, tgtConn, dbCall } = opts;
    const sampleSize = opts.sampleSize || 1000;
    const onProgress = opts.onProgress || function () {};
    const report = { generatedAt: new Date().toISOString(), sampleSize, jobs: [] };

    // Target tables loaded by this run — FK authority resolution below.
    const loadedBy = {};
    for (const s of steps) {
      if (s.tgtTable) loadedBy[String(s.tgtTable).replace(/[[\]]/g, '').toLowerCase()] = s;
    }

    const tgtSchemaCache = {};
    async function targetSchema(tgtTable) {
      const key = tgtTable.toLowerCase();
      if (tgtSchemaCache[key]) return tgtSchemaCache[key];
      const { schema, table } = splitTable(tgtTable);
      const r = await dbCall(tgtConn, { action: 'schema-columns', schemaName: schema, tableName: table });
      if (!r || !r.table || !Array.isArray(r.table.columns) || !r.table.columns.length) {
        throw new Error('target table ' + tgtTable + ' not found or has no columns');
      }
      return (tgtSchemaCache[key] = r.table);
    }

    for (let i = 0; i < steps.length; i++) {
      const step = steps[i];
      const jr = {
        name: step.name || step.tgtTable || ('job ' + (i + 1)),
        srcTable: step.srcTable || '', tgtTable: step.tgtTable || '',
        totalRows: 0, sampled: 0, findings: [], verdict: 'unknown', est: null,
      };
      report.jobs.push(jr);

      if (step.type === 'sql' || step.jobType === 'sql' || step.jobType === 'sql-script') {
        jr.skipped = 'SQL script job — free-form SQL is not simulated';
        jr.verdict = 'unknown';
        onProgress(i + 1, steps.length, jr);
        continue;
      }
      if (!step.srcTable || !step.tgtTable) {
        jr.skipped = 'no source/target table on this job';
        onProgress(i + 1, steps.length, jr);
        continue;
      }

      try {
        const tgt = await targetSchema(step.tgtTable);
        const tgtByName = {};
        tgt.columns.forEach(c => { tgtByName[c.name.toLowerCase()] = c; });

        // Row count, timed alongside the sample for the runtime estimate.
        const t0 = Date.now();
        const countR = await dbCall(srcConn, { action: 'execute', sql: pfCountSql(step.srcTable, step.srcWhere) });
        jr.totalRows = Number((countR.recordset && countR.recordset[0] && (countR.recordset[0].n ?? countR.recordset[0].N)) || 0);

        const mapping = (step.columnMapping || []).filter(m => m && m.tgtCol);
        const srcCols = [...new Set(mapping.filter(m => m.srcCol).map(m => m.srcCol))];
        const t1 = Date.now();
        const sampleR = srcCols.length
          ? await dbCall(srcConn, { action: 'execute', sql: pfSampleSql(step.srcTable, srcCols, step.srcWhere, sampleSize) })
          : { recordset: [] };
        const sampleMs = Date.now() - t1 || 1;
        const rows = sampleR.recordset || [];
        jr.sampled = rows.length;

        // Column-by-column forecast.
        for (const m of mapping) {
          const tc = tgtByName[m.tgtCol.toLowerCase()];
          if (!tc || tc.isIdentity) continue;
          const spec = pfParseType(tc.type);
          spec.nullable = tc.nullable; spec.isIdentity = tc.isIdentity; spec.hasDefault = !!tc.default;
          const hasLiteral = !!(m.literalValue || m.fixedValue);
          if (hasLiteral || !m.srcCol) continue;         // literals resolve at run time; nothing to sample
          const samples = rows.map(r => r[m.srcCol]);
          for (const f of pfCheckColumn(samples, spec, {})) {
            const proj = pfProject(f.count, jr.sampled, jr.totalRows);
            jr.findings.push({
              code: f.code, column: m.srcCol + ' → ' + m.tgtCol, tgtType: tc.type,
              sampleFails: f.count, projected: proj.projected, pct: proj.pct, examples: f.examples,
            });
          }
        }

        // Required target columns nothing feeds.
        for (const f of pfUnmappedRequired(tgt.columns, mapping)) {
          jr.findings.push({
            code: f.code, column: f.column, tgtType: (tgtByName[f.column.toLowerCase()] || {}).type || '',
            sampleFails: jr.sampled, projected: jr.totalRows, pct: 100, examples: [],
          });
        }

        // FK orphans: sampled child values probed against the parent — the
        // parent being loaded by another job in this run, its SOURCE is the
        // authority; otherwise the target's current contents are.
        for (const fk of (tgt.foreignKeys || [])) {
          const m = mapping.find(x => x.tgtCol.toLowerCase() === String(fk.column).toLowerCase() && x.srcCol);
          if (!m) continue;
          const ref = /^(.+)\(([^)]+)\)$/.exec(fk.references || '');
          if (!ref) continue;
          const refTable = ref[1], refCol = ref[2];
          const vals = [...new Set(rows.map(r => r[m.srcCol]).filter(v => v !== null && v !== undefined))];
          if (!vals.length) continue;
          const parentStep = loadedBy[refTable.replace(/[[\]]/g, '').toLowerCase()];
          let probeConn = tgtConn, probeTable = refTable, probeCol = refCol, probeWhere = '';
          if (parentStep) {
            const pm = (parentStep.columnMapping || []).find(x => x.tgtCol && x.tgtCol.toLowerCase() === refCol.toLowerCase() && x.srcCol);
            if (pm) { probeConn = srcConn; probeTable = parentStep.srcTable; probeCol = pm.srcCol; probeWhere = parentStep.srcWhere || ''; }
          }
          try {
            const probeR = await dbCall(probeConn, { action: 'execute', sql: pfFkProbeSql(vals, probeTable, probeCol, probeWhere) });
            const hits = Number((probeR.recordset && probeR.recordset[0] && probeR.recordset[0].hits) || 0);
            const orphans = Math.max(0, Math.min(vals.length, 200) - hits);
            if (orphans > 0) {
              const proj = pfProject(orphans, Math.min(vals.length, 200), jr.totalRows);
              jr.findings.push({
                code: 'fk-orphan', column: m.srcCol + ' → ' + fk.column, tgtType: 'FK → ' + fk.references,
                sampleFails: orphans, projected: proj.projected, pct: proj.pct,
                examples: [], authority: parentStep ? probeTable + ' (loads in this run)' : fk.references,
              });
            }
          } catch (e) {
            jr.findings.push({ code: 'preflight-error', column: fk.column, detail: 'FK probe failed: ' + e.message, sampleFails: 0, projected: 0, examples: [] });
          }
        }

        jr.est = pfRuntimeEstimate(jr.totalRows, jr.sampled, sampleMs + (t1 - t0));
        jr.verdict = pfJobVerdict(jr.findings);
      } catch (e) {
        jr.findings.push({ code: 'preflight-error', column: '', detail: e.message, sampleFails: 0, projected: 0, examples: [] });
        jr.verdict = 'unknown';
      }
      onProgress(i + 1, steps.length, jr);
    }

    report.verdict = pfOverallVerdict(report.jobs);
    // One row can fail several checks, so summing findings overcounts —
    // a job can never reject more rows than it has.
    report.projectedFailures = report.jobs.reduce((a, j) => {
      const red = j.findings.filter(f => pfSeverity(f.code) === 'red')
        .reduce((b, f) => b + (f.projected || 0), 0);
      return a + Math.min(j.totalRows || red, red);
    }, 0);
    return report;
  }

  // ── Persistence (browser only) ────────────────────────────────────────────
  const STORE_KEY = 'cygenix_preflight_results';

  function pfSave(projectId, report) {
    try {
      const all = JSON.parse(localStorage.getItem(STORE_KEY) || '{}');
      all[projectId || 'default'] = report;
      localStorage.setItem(STORE_KEY, JSON.stringify(all));
    } catch {}
  }
  function pfLoad(projectId) {
    try { return JSON.parse(localStorage.getItem(STORE_KEY) || '{}')[projectId || 'default'] || null; }
    catch { return null; }
  }

  const api = {
    pfParseType, pfCheckValue, pfCheckColumn, pfUnmappedRequired, pfProject,
    pfJobVerdict, pfOverallVerdict, pfSeverity, CHECK_META,
    pfRuntimeEstimate, pfFmtDuration, pfConfidence,
    pfQuoteIdent, pfSampleSql, pfCountSql, pfSqlLit, pfFkProbeSql, pfDateYear, pfNumeric,
    pfRunPreflight, pfSave, pfLoad,
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  root.CygenixPreflight = api;
})(typeof window !== 'undefined' ? window : globalThis);
