// server-migration.js
// ─────────────────────────────────────────────────────────────────────────────
// Server Migration — Phase 1: Logins, Phase 2: SQL Agent Jobs
//
// Replicates server-level objects from a source SQL Server instance to a
// target instance. Phase 1 covers logins (sys.server_principals); Phase 2
// adds SQL Server Agent jobs and their dependencies (categories, operators,
// schedules, job steps, job-schedule attachments).
//
// The flow is a five-step shell shared between categories:
//   1. Verify   — sysadmin check on both sides.
//   2. Discover — pulls inventory for whichever categories the user enabled
//                 (logins ☑ jobs ☑ — checkboxes drive what runs).
//   3. Preview  — generates a single combined T-SQL script. Order matters:
//                 logins first (jobs may reference them as owners), then
//                 categories+operators, then jobs+steps+schedules.
//   4. Execute  — runs the per-object blocks one at a time against target,
//                 with live pass/skip/fail rows in the UI.
//   5. Audit    — summary panel + downloadable JSON. Capped at 50 runs in
//                 localStorage. Each result row carries a 'category' field
//                 ('login' | 'category' | 'operator' | 'job') so the audit
//                 summary can split totals.
//
// Phase 2 design decisions confirmed in chat:
//   - Owner remapping: best-effort. Try original owner; if it doesn't exist
//     on target, fall back to 'sa' and flag in audit.
//   - Disabled on creation: checkbox at step 2, default ON. Bringing a job
//     over and having it fire immediately is how you accidentally email
//     customers from staging.
//   - Proxies / credentials: detect at discovery, warn in preview, do NOT
//     try to replicate. Credential secrets can't be read.
//   - Cross-DB references: warn at discovery if a step's database_name
//     doesn't exist on target. Don't block.
//   - Operators: auto-included with jobs (FK dependency). No separate
//     selection.
//   - Alerts: out of scope for Phase 2.
//
// What we explicitly DON'T do:
//   - Migrate the `sa` login, `##MS_*` system logins, or NT-prefixed
//     built-ins.
//   - Migrate database users (those re-map automatically when login SIDs
//     are preserved).
//   - Migrate job history (msdb.dbo.sysjobhistory). New runs accumulate
//     on the target after migration; history stays on the source.
//   - Migrate SQL Agent settings (`sp_configure`, agent service account,
//     mail profile). These are server-config, separate from the jobs
//     themselves.
//   - Migrate proxies, credentials, alerts. Out of Phase 2 scope.
// ─────────────────────────────────────────────────────────────────────────────

(function () {
  'use strict';

  // ── State ─────────────────────────────────────────────────────────────────
  // Held in module scope, intentionally not persisted between page loads.
  // A migration session is short-lived: discover, review, execute, audit,
  // close. If the user reloads, they re-verify and re-discover. The audit
  // log is what persists.
  const SM = {
    verified: false,             // both sides confirmed sysadmin
    sourceDesc: '',              // human-readable label for source server
    targetDesc: '',              // human-readable label for target server

    // Category selection — populated from the checkboxes at step 2.
    // Drives both Discover (which queries to run) and Preview/Execute
    // (which sections to emit).
    categories: { logins: true, jobs: true },

    // ── Logins (Phase 1) ──
    logins: [],                  // login inventory rows
    selectedLoginIds: new Set(), // principal_id values chosen for migration

    // ── Jobs (Phase 2) ──
    jobs: [],                    // job inventory rows (each carries its own steps[], schedules[])
    selectedJobIds: new Set(),   // job_id (uniqueidentifier string) values chosen
    operators: [],               // sysoperators rows — auto-included with jobs
    jobOptions: {                // per-discovery options that affect preview generation
      createDisabled: true,      // override source's enabled state to disabled on target
    },
    targetLogins: new Set(),     // names of logins that exist on target — used for owner remapping
    targetDatabases: new Set(),  // names of DBs on target — used for cross-DB warnings
    targetProxies: new Set(),    // names of proxies on target — used for proxy warnings

    // ── Run state ──
    previewSql: '',              // generated T-SQL before execution
    runId: '',                   // unique id for this run, used in audit storage
    runResults: [],              // per-object pass/skip/fail records during execute
  };

  // localStorage key for the audit history. Capped at 50 most recent runs.
  const AUDIT_KEY = 'cygenix_server_migration_audit';

  // ── Helpers ───────────────────────────────────────────────────────────────

  // Short-hand DOM accessor — same pattern dashboard.html uses elsewhere.
  function $(id) { return document.getElementById(id); }

  // HTML-escape user-controlled values before injecting into innerHTML.
  function escHtml(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#039;');
  }

  // T-SQL string-literal escape: doubles single quotes. Used for any value
  // that ends up inside N'...'.
  function tsqlEsc(s) {
    return String(s == null ? '' : s).replace(/'/g, "''");
  }

  // T-SQL identifier escape: wraps in [] and doubles any embedded ].
  function tsqlIdent(s) {
    return '[' + String(s == null ? '' : s).replace(/]/g, ']]') + ']';
  }

  // Convert a varbinary hex string returned by SQL Server into a clean
  // "0x..." literal we can paste into T-SQL. The shape of `v` depends on
  // how impDbCall's downstream proxy serialises varbinary; we've seen
  // node-mssql Buffers, "0x..." strings, and base64 in the wild.
  function toHexLiteral(v) {
    if (v == null) return null;
    if (typeof v === 'string') {
      if (/^0x[0-9a-fA-F]*$/.test(v)) return v;
      try {
        const bin = atob(v);
        let hex = '0x';
        for (let i = 0; i < bin.length; i++) hex += bin.charCodeAt(i).toString(16).padStart(2, '0');
        return hex.length > 2 ? hex : null;
      } catch { return null; }
    }
    if (v && Array.isArray(v.data)) {
      let hex = '0x';
      for (const b of v.data) hex += (b & 0xff).toString(16).padStart(2, '0');
      return hex.length > 2 ? hex : null;
    }
    if (Array.isArray(v)) {
      let hex = '0x';
      for (const b of v) hex += (b & 0xff).toString(16).padStart(2, '0');
      return hex.length > 2 ? hex : null;
    }
    return null;
  }

  // System logins we never migrate.
  function isSystemLogin(name) {
    if (!name) return true;
    if (name === 'sa') return true;
    if (name.startsWith('##MS_')) return true;
    if (name.startsWith('NT SERVICE\\')) return true;
    if (name.startsWith('NT AUTHORITY\\')) return true;
    if (name === 'public') return true;
    return false;
  }

  function loginKind(typeDesc) {
    if (typeDesc === 'SQL_LOGIN')      return 'sql';
    if (typeDesc === 'WINDOWS_LOGIN')  return 'win';
    if (typeDesc === 'WINDOWS_GROUP')  return 'wingroup';
    return 'other';
  }

  // ── Connection accessors ──────────────────────────────────────────────────
  // Both impGetConn() and impDbCall() are defined in dashboard.html and are
  // the same primitives the linked-servers tab uses.
  function getSrcConn() {
    if (typeof window.impGetConn !== 'function') {
      throw new Error('impGetConn not available — server-migration.js loaded outside dashboard.html?');
    }
    return window.impGetConn('src');
  }
  function getTgtConn() {
    if (typeof window.impGetConn !== 'function') {
      throw new Error('impGetConn not available — server-migration.js loaded outside dashboard.html?');
    }
    return window.impGetConn('tgt');
  }
  async function dbCall(conn, body) {
    if (typeof window.impDbCall !== 'function') {
      throw new Error('impDbCall not available — server-migration.js loaded outside dashboard.html?');
    }
    return window.impDbCall(conn, body);
  }

  function describeConn(conn) {
    if (!conn) return '—';
    if (/^https?:\/\//i.test(conn)) {
      const host = (conn.match(/^https?:\/\/([^\/?]+)/) || [])[1] || conn;
      return 'Azure Function → ' + host;
    }
    const host = (conn.match(/Server\s*=\s*([^;,]+)/i) || [])[1] || '';
    const db   = (conn.match(/(?:Database|Initial Catalog)\s*=\s*([^;]+)/i) || [])[1] || '';
    if (host && db) return host + ' · ' + db;
    if (host)       return host;
    return 'connection configured';
  }

  // ── Step 1: Verify both connections are sysadmin ──────────────────────────
  async function verifyConnections() {
    SM.verified = false;
    const banner = $('sm-verify-banner');
    const srcDot = $('sm-src-dot');
    const tgtDot = $('sm-tgt-dot');
    const srcMsg = $('sm-src-msg');
    const tgtMsg = $('sm-tgt-msg');
    const proceedBtn = $('sm-proceed-btn');

    if (srcDot) srcDot.style.background = 'var(--text3)';
    if (tgtDot) tgtDot.style.background = 'var(--text3)';
    if (srcMsg) srcMsg.textContent = 'Checking…';
    if (tgtMsg) tgtMsg.textContent = 'Checking…';
    if (proceedBtn) proceedBtn.disabled = true;

    const srcConn = getSrcConn();
    const tgtConn = getTgtConn();
    SM.sourceDesc = describeConn(srcConn);
    SM.targetDesc = describeConn(tgtConn);

    if (!srcConn) {
      if (srcMsg) srcMsg.textContent = 'No source connection configured. Set it in the Connections tab.';
      if (srcDot) srcDot.style.background = 'var(--red)';
      return;
    }
    if (!tgtConn) {
      if (tgtMsg) tgtMsg.textContent = 'No target connection configured. Set it in the Connections tab.';
      if (tgtDot) tgtDot.style.background = 'var(--red)';
      return;
    }

    const [srcOk, tgtOk] = await Promise.all([
      checkSysadmin(srcConn).then(r => updateDot('src', r)),
      checkSysadmin(tgtConn).then(r => updateDot('tgt', r)),
    ]);

    if (srcOk && tgtOk) {
      SM.verified = true;
      if (banner) {
        banner.style.borderColor = 'rgba(34,197,94,0.4)';
        banner.style.background  = 'rgba(34,197,94,0.06)';
      }
      if (proceedBtn) proceedBtn.disabled = false;
    }
  }

  function updateDot(side, result) {
    const dot = $('sm-' + side + '-dot');
    const msg = $('sm-' + side + '-msg');
    const label = side === 'src' ? SM.sourceDesc : SM.targetDesc;
    if (result.ok && result.sysadmin) {
      if (dot) dot.style.background = 'var(--green)';
      if (msg) msg.innerHTML = escHtml(label) + ' &nbsp;·&nbsp; <span style="color:var(--green)">sysadmin ✓</span>';
      return true;
    }
    if (result.ok && !result.sysadmin) {
      if (dot) dot.style.background = 'var(--red)';
      if (msg) msg.innerHTML = escHtml(label) + ' &nbsp;·&nbsp; <span style="color:var(--red)">connected, but not a sysadmin on this server</span>';
      return false;
    }
    if (dot) dot.style.background = 'var(--red)';
    if (msg) msg.innerHTML = escHtml(label) + ' &nbsp;·&nbsp; <span style="color:var(--red)">' + escHtml(result.error || 'connection failed') + '</span>';
    return false;
  }

  async function checkSysadmin(conn) {
    try {
      const res = await dbCall(conn, { action: 'execute', sql: 'SELECT IS_SRVROLEMEMBER(\'sysadmin\') AS isSysadmin;' });
      const row = (res?.recordset || [])[0];
      const flag = row ? Number(row.isSysadmin) : 0;
      return { ok: true, sysadmin: flag === 1 };
    } catch (e) {
      return { ok: false, error: e.message || String(e) };
    }
  }

  // ── Step 2: Category-aware discovery ──────────────────────────────────────
  // Reads the "migrate logins" / "migrate jobs" checkboxes, then runs only
  // the discovery queries the user asked for. This is the entry point bound
  // to the Discover button.
  async function discoverAll() {
    if (!SM.verified) { alert('Verify both connections first.'); return; }

    SM.categories.logins = !!$('sm-cat-logins')?.checked;
    SM.categories.jobs   = !!$('sm-cat-jobs')?.checked;
    SM.jobOptions.createDisabled = !!$('sm-jobs-disabled-on-create')?.checked;

    if (!SM.categories.logins && !SM.categories.jobs) {
      alert('Select at least one category to discover.');
      return;
    }

    // Pull target-side reference data we need for warnings/remapping.
    // Done once up front so we have it when scoring the source inventory.
    await loadTargetReferenceData();

    if (SM.categories.logins) await discoverLogins();
    if (SM.categories.jobs)   await discoverJobs();
  }

  // Populate sets of logins / databases / proxies that exist on the target,
  // so we can flag missing references at preview time. Cached in SM —
  // re-discover refreshes them.
  async function loadTargetReferenceData() {
    SM.targetLogins    = new Set();
    SM.targetDatabases = new Set();
    SM.targetProxies   = new Set();
    try {
      const tgt = getTgtConn();
      // Three independent queries; can happen in parallel, all small.
      const [logins, dbs, proxies] = await Promise.all([
        dbCall(tgt, { action: 'execute', sql: 'SELECT name FROM sys.server_principals WHERE type IN (\'S\',\'U\',\'G\');' }),
        dbCall(tgt, { action: 'execute', sql: 'SELECT name FROM sys.databases;' }),
        // Proxies live in msdb. Wrap in catch in case the user has an unusual
        // setup where msdb isn't queryable; warning UX gracefully degrades.
        dbCall(tgt, { action: 'execute', sql: 'SELECT name FROM msdb.dbo.sysproxies;' }).catch(() => ({ recordset: [] })),
      ]);
      for (const r of (logins?.recordset  || [])) SM.targetLogins.add(r.name);
      for (const r of (dbs?.recordset     || [])) SM.targetDatabases.add(r.name);
      for (const r of (proxies?.recordset || [])) SM.targetProxies.add(r.name);
    } catch (e) {
      console.warn('[server-migration] Could not load target reference data:', e);
    }
  }

  // ── Step 2a: Discover logins ──────────────────────────────────────────────
  async function discoverLogins() {
    const tableEl = $('sm-logins-table');
    const summaryEl = $('sm-logins-summary');
    if (tableEl) tableEl.innerHTML = '<div style="font-size:11px;color:var(--text3);padding:1rem">Discovering logins on source…</div>';
    if (summaryEl) summaryEl.textContent = '';

    // STRING_AGG requires SQL Server 2017+. Switch to FOR XML PATH if a
    // client needs 2016 support.
    // is_policy_checked / is_expiration_checked live on sys.sql_logins,
    // NOT sys.server_principals. They only apply to SQL logins; Windows
    // logins inherit their policy from AD. We LEFT JOIN so Windows
    // logins still come back, just with NULLs for those flags.
    const sql = `
      SELECT
        sp.principal_id,
        sp.name,
        sp.type_desc,
        sp.is_disabled,
        sp.default_database_name,
        sp.default_language_name,
        sp.sid,
        CONVERT(varbinary(256), LOGINPROPERTY(sp.name, 'PasswordHash'))  AS password_hash,
        sl.is_policy_checked,
        sl.is_expiration_checked,
        ISNULL((
          SELECT STRING_AGG(rp.name, ',') WITHIN GROUP (ORDER BY rp.name)
          FROM   sys.server_role_members rm
          JOIN   sys.server_principals    rp ON rp.principal_id = rm.role_principal_id
          WHERE  rm.member_principal_id = sp.principal_id
        ), '') AS server_roles
      FROM      sys.server_principals sp
      LEFT JOIN sys.sql_logins        sl ON sl.principal_id = sp.principal_id
      WHERE sp.type IN ('S','U','G')
        AND sp.principal_id > 0
      ORDER BY sp.name;
    `;

    let res;
    try {
      res = await dbCall(getSrcConn(), { action: 'execute', sql });
    } catch (e) {
      if (tableEl) tableEl.innerHTML = '<div style="color:var(--red);padding:1rem;font-size:12px">🔴 Login discovery failed: ' + escHtml(e.message || String(e)) + '</div>';
      return;
    }

    const rows = res?.recordset || [];
    SM.logins = rows.map(r => ({
      principal_id          : r.principal_id,
      name                  : r.name,
      type_desc             : r.type_desc,
      kind                  : loginKind(r.type_desc),
      is_disabled           : !!r.is_disabled,
      default_database_name : r.default_database_name || 'master',
      default_language_name : r.default_language_name || 'us_english',
      sid                   : toHexLiteral(r.sid),
      password_hash         : toHexLiteral(r.password_hash),
      check_policy          : !!r.is_policy_checked,
      check_expiration      : !!r.is_expiration_checked,
      server_roles          : (r.server_roles || '').split(',').map(s => s.trim()).filter(Boolean),
      isSystem              : isSystemLogin(r.name),
    }));

    SM.selectedLoginIds = new Set(SM.logins.filter(l => !l.isSystem).map(l => l.principal_id));
    renderLoginsTable();
  }

  function renderLoginsTable() {
    const el = $('sm-logins-table');
    const summaryEl = $('sm-logins-summary');
    if (!el) return;

    const filterText = ($('sm-login-filter')?.value || '').trim().toLowerCase();
    const showSystem = !!$('sm-show-system')?.checked;

    const filtered = SM.logins.filter(l => {
      if (!showSystem && l.isSystem) return false;
      if (!filterText) return true;
      return l.name.toLowerCase().includes(filterText);
    });

    if (summaryEl) {
      const counts = SM.logins.reduce((a, l) => { a[l.kind] = (a[l.kind] || 0) + 1; return a; }, {});
      const selected = SM.selectedLoginIds.size;
      summaryEl.innerHTML =
        '<strong>' + SM.logins.length + '</strong> logins discovered &nbsp;·&nbsp; ' +
        '<span style="color:var(--text2)">SQL: ' + (counts.sql || 0) + '</span> &nbsp;· ' +
        '<span style="color:var(--text2)">Windows: ' + (counts.win || 0) + '</span> &nbsp;· ' +
        '<span style="color:var(--text2)">Win group: ' + (counts.wingroup || 0) + '</span> &nbsp;· ' +
        '<strong style="color:var(--accent)">' + selected + ' selected</strong>';
    }

    if (filtered.length === 0) {
      el.innerHTML = '<div style="font-size:11px;color:var(--text3);font-style:italic;padding:1rem">No logins match the filter.</div>';
      return;
    }

    let html = `
      <table style="width:100%;border-collapse:collapse;font-size:12px">
        <thead>
          <tr style="background:var(--bg3);text-align:left">
            <th style="padding:8px 10px;width:40px;border-bottom:0.5px solid var(--border)">
              <input type="checkbox" id="sm-select-all-logins" onchange="ServerMigration._toggleAllLogins(this.checked)" style="cursor:pointer">
            </th>
            <th style="padding:8px 10px;border-bottom:0.5px solid var(--border)">Name</th>
            <th style="padding:8px 10px;border-bottom:0.5px solid var(--border);width:120px">Type</th>
            <th style="padding:8px 10px;border-bottom:0.5px solid var(--border);width:160px">Default DB</th>
            <th style="padding:8px 10px;border-bottom:0.5px solid var(--border)">Server roles</th>
            <th style="padding:8px 10px;border-bottom:0.5px solid var(--border);width:80px">State</th>
          </tr>
        </thead>
        <tbody>`;
    for (const l of filtered) {
      const checked = SM.selectedLoginIds.has(l.principal_id) ? 'checked' : '';
      const typeBadge =
        l.kind === 'sql'      ? '<span style="background:rgba(79,142,255,0.15);color:#6ea4ff;padding:1px 6px;border-radius:3px;font-family:var(--mono);font-size:10px">SQL</span>' :
        l.kind === 'win'      ? '<span style="background:rgba(45,212,191,0.15);color:#2dd4bf;padding:1px 6px;border-radius:3px;font-family:var(--mono);font-size:10px">WIN</span>' :
        l.kind === 'wingroup' ? '<span style="background:rgba(167,139,250,0.15);color:#a78bfa;padding:1px 6px;border-radius:3px;font-family:var(--mono);font-size:10px">WIN GRP</span>' :
                                '<span style="color:var(--text3);font-size:11px">' + escHtml(l.type_desc) + '</span>';
      const stateBits = [];
      if (l.is_disabled) stateBits.push('<span style="color:var(--amber)">disabled</span>');
      if (l.isSystem)    stateBits.push('<span style="color:var(--text3)">system</span>');
      if (l.kind === 'sql' && !l.password_hash) stateBits.push('<span style="color:var(--red)" title="No hash readable — likely permissions">no hash</span>');
      const rolesHtml = l.server_roles.length
        ? l.server_roles.map(r => '<span style="background:var(--bg4);color:var(--text2);padding:1px 5px;border-radius:3px;font-family:var(--mono);font-size:10px;margin-right:3px">' + escHtml(r) + '</span>').join('')
        : '<span style="color:var(--text3);font-style:italic">none</span>';
      const rowBg = l.isSystem ? 'background:rgba(255,255,255,0.015)' : '';
      html += `
        <tr style="${rowBg}">
          <td style="padding:8px 10px;border-bottom:0.5px solid var(--border)">
            <input type="checkbox" ${checked} onchange="ServerMigration._toggleLogin(${l.principal_id}, this.checked)" style="cursor:pointer">
          </td>
          <td style="padding:8px 10px;border-bottom:0.5px solid var(--border);font-family:var(--mono)">${escHtml(l.name)}</td>
          <td style="padding:8px 10px;border-bottom:0.5px solid var(--border)">${typeBadge}</td>
          <td style="padding:8px 10px;border-bottom:0.5px solid var(--border);font-family:var(--mono);font-size:11px;color:var(--text2)">${escHtml(l.default_database_name)}</td>
          <td style="padding:8px 10px;border-bottom:0.5px solid var(--border)">${rolesHtml}</td>
          <td style="padding:8px 10px;border-bottom:0.5px solid var(--border);font-size:11px">${stateBits.join(' &nbsp;') || '<span style="color:var(--green)">enabled</span>'}</td>
        </tr>`;
    }
    html += '</tbody></table>';
    el.innerHTML = html;

    const allBox = $('sm-select-all-logins');
    if (allBox) {
      const visibleIds = filtered.map(l => l.principal_id);
      allBox.checked = visibleIds.length > 0 && visibleIds.every(id => SM.selectedLoginIds.has(id));
    }
  }

  function _toggleLogin(principalId, checked) {
    if (checked) SM.selectedLoginIds.add(principalId);
    else         SM.selectedLoginIds.delete(principalId);
    renderLoginsTable();
  }

  function _toggleAllLogins(checked) {
    const filterText = ($('sm-login-filter')?.value || '').trim().toLowerCase();
    const showSystem = !!$('sm-show-system')?.checked;
    const filtered = SM.logins.filter(l => {
      if (!showSystem && l.isSystem) return false;
      if (!filterText) return true;
      return l.name.toLowerCase().includes(filterText);
    });
    for (const l of filtered) {
      if (checked) SM.selectedLoginIds.add(l.principal_id);
      else         SM.selectedLoginIds.delete(l.principal_id);
    }
    renderLoginsTable();
  }

  function _filterLoginsChanged() { renderLoginsTable(); }

  // ── Step 2b: Discover jobs (Phase 2) ──────────────────────────────────────
  // Pulls jobs + steps + schedules + categories + operators in parallel:
  //   1. job header rows from sysjobs
  //   2. all steps for those jobs from sysjobsteps
  //   3. schedules attached via sysjobschedules → sysschedules
  //   4. operators referenced by jobs (auto-included with any selected job)
  async function discoverJobs() {
    const tableEl = $('sm-jobs-table');
    const summaryEl = $('sm-jobs-summary');
    if (tableEl) tableEl.innerHTML = '<div style="font-size:11px;color:var(--text3);padding:1rem">Discovering Agent jobs on source…</div>';
    if (summaryEl) summaryEl.textContent = '';

    const sqlJobs = `
      SELECT
        j.job_id,
        j.name,
        j.enabled,
        j.description,
        j.owner_sid,
        SUSER_SNAME(j.owner_sid)                       AS owner_login_name,
        j.notify_level_eventlog,
        j.notify_level_email,
        j.notify_email_operator_id,
        op.name                                         AS notify_email_operator_name,
        c.name                                          AS category_name,
        c.category_class,
        c.category_type,
        j.delete_level,
        j.start_step_id,
        j.date_created
      FROM   msdb.dbo.sysjobs                  j
      LEFT JOIN msdb.dbo.syscategories          c  ON c.category_id = j.category_id
      LEFT JOIN msdb.dbo.sysoperators           op ON op.id          = j.notify_email_operator_id
      ORDER BY j.name;
    `;
    const sqlSteps = `
      SELECT
        js.job_id,
        js.step_id,
        js.step_name,
        js.subsystem,
        js.command,
        js.database_name,
        js.database_user_name,
        js.on_success_action,
        js.on_success_step_id,
        js.on_fail_action,
        js.on_fail_step_id,
        js.retry_attempts,
        js.retry_interval,
        js.output_file_name,
        js.flags,
        js.proxy_id,
        p.name AS proxy_name
      FROM   msdb.dbo.sysjobsteps js
      LEFT JOIN msdb.dbo.sysproxies  p ON p.proxy_id = js.proxy_id
      ORDER BY js.job_id, js.step_id;
    `;
    // sysjobschedules is the join table; sysschedules holds the schedule
    // definition. A schedule may be reused across jobs, but for migration
    // we copy it per-job (sp_add_jobschedule). If two jobs share a schedule
    // on source, we'll create two identical schedules on target — slight
    // redundancy, but it keeps per-job execution self-contained.
    const sqlSchedules = `
      SELECT
        jsch.job_id,
        s.schedule_id,
        s.name                  AS schedule_name,
        s.enabled               AS schedule_enabled,
        s.freq_type,
        s.freq_interval,
        s.freq_subday_type,
        s.freq_subday_interval,
        s.freq_relative_interval,
        s.freq_recurrence_factor,
        s.active_start_date,
        s.active_end_date,
        s.active_start_time,
        s.active_end_time
      FROM   msdb.dbo.sysjobschedules  jsch
      JOIN   msdb.dbo.sysschedules     s     ON s.schedule_id = jsch.schedule_id
      ORDER BY jsch.job_id, s.schedule_id;
    `;
    const sqlOperators = `
      SELECT
        id, name, enabled, email_address, weekday_pager_start_time,
        weekday_pager_end_time, saturday_pager_start_time,
        saturday_pager_end_time, sunday_pager_start_time,
        sunday_pager_end_time, pager_days, pager_address,
        netsend_address, category_id
      FROM msdb.dbo.sysoperators
      ORDER BY name;
    `;

    let jobsRes, stepsRes, schedRes, opsRes;
    try {
      [jobsRes, stepsRes, schedRes, opsRes] = await Promise.all([
        dbCall(getSrcConn(), { action: 'execute', sql: sqlJobs }),
        dbCall(getSrcConn(), { action: 'execute', sql: sqlSteps }),
        dbCall(getSrcConn(), { action: 'execute', sql: sqlSchedules }),
        dbCall(getSrcConn(), { action: 'execute', sql: sqlOperators }),
      ]);
    } catch (e) {
      if (tableEl) tableEl.innerHTML = '<div style="color:var(--red);padding:1rem;font-size:12px">🔴 Job discovery failed: ' + escHtml(e.message || String(e)) + '</div>';
      return;
    }

    // Index steps and schedules by job_id so we can attach them
    const stepsByJob = {};
    for (const s of (stepsRes?.recordset || [])) {
      const k = String(s.job_id);
      if (!stepsByJob[k]) stepsByJob[k] = [];
      stepsByJob[k].push(s);
    }
    const schedsByJob = {};
    for (const sc of (schedRes?.recordset || [])) {
      const k = String(sc.job_id);
      if (!schedsByJob[k]) schedsByJob[k] = [];
      schedsByJob[k].push(sc);
    }
    SM.operators = (opsRes?.recordset || []);

    // Build the unified job records, computing warnings as we go so the
    // table can show them immediately.
    SM.jobs = (jobsRes?.recordset || []).map(j => {
      const k = String(j.job_id);
      const steps = stepsByJob[k] || [];
      const schedules = schedsByJob[k] || [];

      const warnings = [];
      // Owner doesn't exist on target → will fall back to sa
      if (j.owner_login_name && !SM.targetLogins.has(j.owner_login_name)) {
        warnings.push({ kind: 'owner-missing', detail: `Owner '${j.owner_login_name}' not on target — will fall back to sa.` });
      }
      // Step-level warnings
      const proxyMissing = new Set();
      const dbMissing    = new Set();
      for (const s of steps) {
        if (s.proxy_name && !SM.targetProxies.has(s.proxy_name)) proxyMissing.add(s.proxy_name);
        if (s.database_name && !['master','msdb','tempdb','model'].includes(String(s.database_name).toLowerCase())) {
          if (!SM.targetDatabases.has(s.database_name)) dbMissing.add(s.database_name);
        }
      }
      for (const p of proxyMissing) warnings.push({ kind: 'proxy-missing', detail: `Step uses proxy '${p}' which doesn't exist on target — affected step will fail until proxy is created.` });
      for (const d of dbMissing)    warnings.push({ kind: 'db-missing',    detail: `Step references database '${d}' which doesn't exist on target — step will fail at runtime.` });

      return {
        job_id     : k,                    // string form, used as Set key
        name       : j.name,
        enabled    : !!j.enabled,
        description: j.description || '',
        owner_login_name: j.owner_login_name || 'sa',
        notify_email_operator_name: j.notify_email_operator_name || null,
        category_name : j.category_name || '[Uncategorized (Local)]',
        category_class: j.category_class || 1,
        category_type : j.category_type  || 1,
        steps,
        schedules,
        warnings,
      };
    });

    SM.selectedJobIds = new Set(SM.jobs.map(j => j.job_id));
    renderJobsTable();
  }

  function renderJobsTable() {
    const el = $('sm-jobs-table');
    const summaryEl = $('sm-jobs-summary');
    if (!el) return;
    const filterText = ($('sm-job-filter')?.value || '').trim().toLowerCase();
    const filtered = SM.jobs.filter(j => !filterText || j.name.toLowerCase().includes(filterText));

    if (summaryEl) {
      const totalSteps = SM.jobs.reduce((a, j) => a + j.steps.length, 0);
      const withWarnings = SM.jobs.filter(j => j.warnings.length).length;
      summaryEl.innerHTML =
        '<strong>' + SM.jobs.length + '</strong> jobs &nbsp;·&nbsp; ' +
        '<span style="color:var(--text2)">' + totalSteps + ' steps total</span> &nbsp;· ' +
        '<span style="color:var(--text2)">' + (SM.operators?.length || 0) + ' operator(s) (auto-included)</span> &nbsp;· ' +
        (withWarnings ? '<span style="color:var(--amber)">' + withWarnings + ' with warnings</span> &nbsp;· ' : '') +
        '<strong style="color:var(--accent)">' + SM.selectedJobIds.size + ' selected</strong>';
    }

    if (filtered.length === 0) {
      el.innerHTML = '<div style="font-size:11px;color:var(--text3);font-style:italic;padding:1rem">No jobs match the filter.</div>';
      return;
    }

    let html = `
      <table style="width:100%;border-collapse:collapse;font-size:12px">
        <thead>
          <tr style="background:var(--bg3);text-align:left">
            <th style="padding:8px 10px;width:40px;border-bottom:0.5px solid var(--border)">
              <input type="checkbox" id="sm-select-all-jobs" onchange="ServerMigration._toggleAllJobs(this.checked)" style="cursor:pointer">
            </th>
            <th style="padding:8px 10px;border-bottom:0.5px solid var(--border)">Job name</th>
            <th style="padding:8px 10px;border-bottom:0.5px solid var(--border);width:60px;text-align:center">Steps</th>
            <th style="padding:8px 10px;border-bottom:0.5px solid var(--border);width:80px;text-align:center">Schedules</th>
            <th style="padding:8px 10px;border-bottom:0.5px solid var(--border);width:160px">Owner</th>
            <th style="padding:8px 10px;border-bottom:0.5px solid var(--border);width:80px">State</th>
            <th style="padding:8px 10px;border-bottom:0.5px solid var(--border)">Warnings</th>
          </tr>
        </thead>
        <tbody>`;
    for (const j of filtered) {
      const checked = SM.selectedJobIds.has(j.job_id) ? 'checked' : '';
      const stateBadge = j.enabled
        ? '<span style="color:var(--green)">enabled</span>'
        : '<span style="color:var(--amber)">disabled</span>';
      const warnHtml = j.warnings.length
        ? j.warnings.map(w => '<span style="color:var(--amber);font-size:10.5px;display:block;line-height:1.4" title="' + escHtml(w.detail) + '">⚠ ' + escHtml(w.detail) + '</span>').join('')
        : '<span style="color:var(--text3);font-style:italic;font-size:10.5px">none</span>';
      html += `
        <tr>
          <td style="padding:8px 10px;border-bottom:0.5px solid var(--border)">
            <input type="checkbox" ${checked} onchange="ServerMigration._toggleJob('${escHtml(j.job_id)}', this.checked)" style="cursor:pointer">
          </td>
          <td style="padding:8px 10px;border-bottom:0.5px solid var(--border);font-family:var(--mono)">${escHtml(j.name)}</td>
          <td style="padding:8px 10px;border-bottom:0.5px solid var(--border);text-align:center;font-family:var(--mono);color:var(--text2)">${j.steps.length}</td>
          <td style="padding:8px 10px;border-bottom:0.5px solid var(--border);text-align:center;font-family:var(--mono);color:var(--text2)">${j.schedules.length}</td>
          <td style="padding:8px 10px;border-bottom:0.5px solid var(--border);font-family:var(--mono);font-size:11px;color:var(--text2)">${escHtml(j.owner_login_name)}</td>
          <td style="padding:8px 10px;border-bottom:0.5px solid var(--border);font-size:11px">${stateBadge}</td>
          <td style="padding:8px 10px;border-bottom:0.5px solid var(--border)">${warnHtml}</td>
        </tr>`;
    }
    html += '</tbody></table>';
    el.innerHTML = html;

    const allBox = $('sm-select-all-jobs');
    if (allBox) {
      const visibleIds = filtered.map(j => j.job_id);
      allBox.checked = visibleIds.length > 0 && visibleIds.every(id => SM.selectedJobIds.has(id));
    }
  }

  function _toggleJob(jobId, checked) {
    if (checked) SM.selectedJobIds.add(jobId);
    else         SM.selectedJobIds.delete(jobId);
    renderJobsTable();
  }

  function _toggleAllJobs(checked) {
    const filterText = ($('sm-job-filter')?.value || '').trim().toLowerCase();
    const filtered = SM.jobs.filter(j => !filterText || j.name.toLowerCase().includes(filterText));
    for (const j of filtered) {
      if (checked) SM.selectedJobIds.add(j.job_id);
      else         SM.selectedJobIds.delete(j.job_id);
    }
    renderJobsTable();
  }

  function _filterJobsChanged() { renderJobsTable(); }

  // ── Step 3: Generate preview SQL ──────────────────────────────────────────
  // Combined script. Order matters because of FK and reference dependencies:
  //   1. Logins  — jobs may have these as owners
  //   2. Operators — jobs reference these via notify_email_operator_id
  //   3. Categories — jobs reference these via category_id
  //   4. Jobs (with steps, schedules, schedule attachments)
  // Each section is independently re-runnable due to IF NOT EXISTS guards.
  function buildPreviewSql() {
    const out = [];
    out.push('-- ============================================================================');
    out.push('-- Cygenix — Server Migration');
    out.push('-- Source: ' + SM.sourceDesc);
    out.push('-- Target: ' + SM.targetDesc);
    out.push('-- Generated: ' + new Date().toISOString());

    const selectedLogins = SM.categories.logins ? SM.logins.filter(l => SM.selectedLoginIds.has(l.principal_id)) : [];
    const selectedJobs   = SM.categories.jobs   ? SM.jobs.filter(j => SM.selectedJobIds.has(j.job_id)) : [];

    out.push('-- Logins selected: ' + selectedLogins.length + ' / Jobs selected: ' + selectedJobs.length);
    out.push('-- ============================================================================');
    out.push('');

    // ── Logins ──────────────────────────────────────────────────────────────
    if (selectedLogins.length) {
      out.push('-- ════════════════════════════════════════════════');
      out.push('-- LOGINS');
      out.push('-- ════════════════════════════════════════════════');
      out.push('');
      for (const l of selectedLogins) {
        out.push('-- ── ' + l.name + '   (' + l.type_desc + ') ──');
        out.push('IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N\'' + tsqlEsc(l.name) + '\')');
        out.push('BEGIN');
        out.push(buildCreateLogin(l));
        out.push('END');
        if (l.is_disabled) out.push('ALTER LOGIN ' + tsqlIdent(l.name) + ' DISABLE;');
        for (const role of l.server_roles) {
          out.push('IF NOT EXISTS (');
          out.push('  SELECT 1');
          out.push('  FROM   sys.server_role_members rm');
          out.push('  JOIN   sys.server_principals    r ON r.principal_id = rm.role_principal_id');
          out.push('  JOIN   sys.server_principals    m ON m.principal_id = rm.member_principal_id');
          out.push('  WHERE  r.name = N\'' + tsqlEsc(role) + '\' AND m.name = N\'' + tsqlEsc(l.name) + '\'');
          out.push(')');
          out.push('  ALTER SERVER ROLE ' + tsqlIdent(role) + ' ADD MEMBER ' + tsqlIdent(l.name) + ';');
        }
        out.push('GO');
        out.push('');
      }
    }

    // ── Operators (auto-included with jobs) ─────────────────────────────────
    if (selectedJobs.length && SM.operators?.length) {
      out.push('-- ════════════════════════════════════════════════');
      out.push('-- OPERATORS (auto-included with jobs)');
      out.push('-- ════════════════════════════════════════════════');
      out.push('');
      for (const op of SM.operators) {
        out.push('-- ── operator: ' + op.name + ' ──');
        out.push(buildOperator(op));
        out.push('GO');
        out.push('');
      }
    }

    // ── Categories (auto-included with jobs) ────────────────────────────────
    if (selectedJobs.length) {
      const seen = new Set();
      const cats = [];
      for (const j of selectedJobs) {
        if (j.category_name && !seen.has(j.category_name)) {
          seen.add(j.category_name);
          cats.push({ name: j.category_name, category_class: j.category_class, category_type: j.category_type });
        }
      }
      if (cats.length) {
        out.push('-- ════════════════════════════════════════════════');
        out.push('-- JOB CATEGORIES (auto-included with jobs)');
        out.push('-- ════════════════════════════════════════════════');
        out.push('');
        for (const c of cats) {
          // [Uncategorized (Local)] is created automatically with msdb so
          // we don't try to add it. Comment for transparency in preview.
          if (c.name === '[Uncategorized (Local)]') {
            out.push('-- (skipping built-in category: ' + c.name + ')');
            out.push('');
            continue;
          }
          out.push(buildCategory(c));
          out.push('GO');
          out.push('');
        }
      }
    }

    // ── Jobs ────────────────────────────────────────────────────────────────
    if (selectedJobs.length) {
      out.push('-- ════════════════════════════════════════════════');
      out.push('-- JOBS');
      out.push('-- ════════════════════════════════════════════════');
      out.push('');
      for (const j of selectedJobs) {
        out.push('-- ── job: ' + j.name + ' ──');
        if (j.warnings.length) {
          for (const w of j.warnings) out.push('-- ⚠ ' + w.detail);
        }
        out.push(buildJobBlock(j));
        out.push('GO');
        out.push('');
      }
    }

    SM.previewSql = out.join('\n');
  }

  function buildCreateLogin(l) {
    const ind = '  ';
    if (l.kind === 'sql') {
      const lines = [];
      lines.push(ind + 'CREATE LOGIN ' + tsqlIdent(l.name));
      if (!l.password_hash) {
        lines.push(ind + '  -- ⚠ Password hash unreadable on source — login will not be created.');
        return ind + '/*\n' + lines.join('\n') + '\n' + ind + '*/';
      }
      lines.push(ind + '  WITH PASSWORD = ' + l.password_hash + ' HASHED');
      if (l.sid) lines.push(ind + '     , SID = ' + l.sid);
      lines.push(ind + '     , DEFAULT_DATABASE = ' + tsqlIdent(l.default_database_name));
      lines.push(ind + '     , DEFAULT_LANGUAGE = ' + tsqlIdent(l.default_language_name));
      lines.push(ind + '     , CHECK_POLICY     = ' + (l.check_policy     ? 'ON' : 'OFF'));
      lines.push(ind + '     , CHECK_EXPIRATION = ' + (l.check_expiration ? 'ON' : 'OFF') + ';');
      return lines.join('\n');
    }
    const lines = [];
    lines.push(ind + 'CREATE LOGIN ' + tsqlIdent(l.name) + ' FROM WINDOWS');
    lines.push(ind + '  WITH DEFAULT_DATABASE = ' + tsqlIdent(l.default_database_name));
    lines.push(ind + '     , DEFAULT_LANGUAGE = ' + tsqlIdent(l.default_language_name) + ';');
    return lines.join('\n');
  }

  // sp_add_operator with IF NOT EXISTS guard. Idempotent re-runs skip
  // already-added operators.
  function buildOperator(op) {
    const lines = [];
    lines.push("IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysoperators WHERE name = N'" + tsqlEsc(op.name) + "')");
    lines.push("EXEC msdb.dbo.sp_add_operator");
    lines.push("    @name                     = N'" + tsqlEsc(op.name) + "'");
    lines.push("  , @enabled                  = " + (op.enabled ? 1 : 0));
    if (op.email_address)              lines.push("  , @email_address            = N'" + tsqlEsc(op.email_address) + "'");
    if (op.netsend_address)            lines.push("  , @netsend_address          = N'" + tsqlEsc(op.netsend_address) + "'");
    if (op.pager_address)              lines.push("  , @pager_address            = N'" + tsqlEsc(op.pager_address) + "'");
    lines.push("  , @weekday_pager_start_time  = " + (op.weekday_pager_start_time  || 0));
    lines.push("  , @weekday_pager_end_time    = " + (op.weekday_pager_end_time    || 235959));
    lines.push("  , @saturday_pager_start_time = " + (op.saturday_pager_start_time || 0));
    lines.push("  , @saturday_pager_end_time   = " + (op.saturday_pager_end_time   || 235959));
    lines.push("  , @sunday_pager_start_time   = " + (op.sunday_pager_start_time   || 0));
    lines.push("  , @sunday_pager_end_time     = " + (op.sunday_pager_end_time     || 235959));
    lines.push("  , @pager_days                = " + (op.pager_days || 0));
    lines.push("  , @category_name             = N'[Uncategorized]';");
    return lines.join('\n');
  }

  function buildCategory(c) {
    return [
      "IF NOT EXISTS (SELECT 1 FROM msdb.dbo.syscategories WHERE name = N'" + tsqlEsc(c.name) + "' AND category_class = " + (c.category_class || 1) + ")",
      "EXEC msdb.dbo.sp_add_category",
      "    @class = " + categoryClassName(c.category_class) + ",",
      "    @type  = " + categoryTypeName(c.category_type)   + ",",
      "    @name  = N'" + tsqlEsc(c.name) + "';",
    ].join('\n');
  }

  function categoryClassName(n) {
    // sysjobcategories.category_class: 1=JOB, 2=ALERT, 3=OPERATOR
    if (n === 2) return "N'ALERT'";
    if (n === 3) return "N'OPERATOR'";
    return "N'JOB'";
  }
  function categoryTypeName(n) {
    // 1=LOCAL, 2=MULTI-SERVER, 3=NONE
    if (n === 2) return "N'MULTI-SERVER'";
    if (n === 3) return "N'NONE'";
    return "N'LOCAL'";
  }

  // The full block to add a single job — header + steps + schedules
  // + server attachment. Wrapped in a transaction with rollback on error,
  // mirroring how SSMS scripts jobs. The IF NOT EXISTS at the top means
  // re-running the script is safe (we don't try to UPDATE an existing job
  // — that's a Phase 2.5 concern).
  function buildJobBlock(j) {
    const out = [];
    const jobNameLit = "N'" + tsqlEsc(j.name) + "'";

    // Owner remap: if owner doesn't exist on target, fall back to sa.
    const owner = j.owner_login_name && SM.targetLogins.has(j.owner_login_name) ? j.owner_login_name : 'sa';
    const ownerLit = "N'" + tsqlEsc(owner) + "'";
    if (owner !== j.owner_login_name) {
      out.push("-- Owner '" + j.owner_login_name + "' not present on target — falling back to 'sa'.");
    }

    out.push("IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = " + jobNameLit + ")");
    out.push("BEGIN");
    out.push("  DECLARE @JobId UNIQUEIDENTIFIER;");
    out.push("  DECLARE @ReturnCode INT = 0;");
    out.push("");
    out.push("  BEGIN TRANSACTION;");
    out.push("");
    out.push("  EXEC @ReturnCode = msdb.dbo.sp_add_job");
    out.push("      @job_name                = " + jobNameLit);
    out.push("    , @enabled                 = " + (SM.jobOptions.createDisabled ? 0 : (j.enabled ? 1 : 0)));
    out.push("    , @notify_level_eventlog   = 0");
    out.push("    , @notify_level_email      = 0");
    out.push("    , @delete_level            = 0");
    out.push("    , @description             = N'" + tsqlEsc(j.description || 'Migrated by Cygenix Server Migration') + "'");
    out.push("    , @category_name           = N'" + tsqlEsc(j.category_name) + "'");
    out.push("    , @owner_login_name        = " + ownerLit);
    if (j.notify_email_operator_name) {
      out.push("    , @notify_email_operator_name = N'" + tsqlEsc(j.notify_email_operator_name) + "'");
    }
    out.push("    , @job_id                  = @JobId OUTPUT;");
    out.push("  IF @ReturnCode <> 0 GOTO QuitWithRollback;");
    out.push("");

    // Steps. sp_add_jobstep, one per step, in step_id order so the
    // numbering matches the source.
    const steps = (j.steps || []).slice().sort((a, b) => a.step_id - b.step_id);
    for (const s of steps) {
      out.push("  -- step " + s.step_id + ": " + (s.step_name || '(unnamed)'));
      out.push("  EXEC @ReturnCode = msdb.dbo.sp_add_jobstep");
      out.push("      @job_id              = @JobId");
      out.push("    , @step_name           = N'" + tsqlEsc(s.step_name || '') + "'");
      out.push("    , @subsystem           = N'" + tsqlEsc(s.subsystem || 'TSQL') + "'");
      out.push("    , @command             = N'" + tsqlEsc(s.command || '') + "'");
      if (s.database_name)      out.push("    , @database_name       = N'" + tsqlEsc(s.database_name) + "'");
      if (s.database_user_name) out.push("    , @database_user_name  = N'" + tsqlEsc(s.database_user_name) + "'");
      out.push("    , @on_success_action   = " + (s.on_success_action || 1));
      out.push("    , @on_success_step_id  = " + (s.on_success_step_id || 0));
      out.push("    , @on_fail_action      = " + (s.on_fail_action    || 2));
      out.push("    , @on_fail_step_id     = " + (s.on_fail_step_id    || 0));
      out.push("    , @retry_attempts      = " + (s.retry_attempts || 0));
      out.push("    , @retry_interval      = " + (s.retry_interval || 0));
      if (s.output_file_name) out.push("    , @output_file_name    = N'" + tsqlEsc(s.output_file_name) + "'");
      out.push("    , @flags               = " + (s.flags || 0));
      // Proxy: only emit if it exists on target. Otherwise comment so the
      // user can see what's missing; step still gets created (will run as
      // Agent service account — almost certainly wrong but at least the
      // step exists for them to fix).
      if (s.proxy_name) {
        if (SM.targetProxies.has(s.proxy_name)) {
          out.push("    , @proxy_name          = N'" + tsqlEsc(s.proxy_name) + "'");
        } else {
          out.push("    -- ⚠ proxy '" + s.proxy_name + "' missing on target; step will run as Agent service account until proxy is created");
        }
      }
      out.push("  ;");
      out.push("  IF @ReturnCode <> 0 GOTO QuitWithRollback;");
      out.push("");
    }

    // Set the job's start step (always 1 unless overridden by source —
    // we don't preserve start_step_id from source in v1, deferred).
    out.push("  EXEC @ReturnCode = msdb.dbo.sp_update_job");
    out.push("      @job_id        = @JobId");
    out.push("    , @start_step_id = 1;");
    out.push("  IF @ReturnCode <> 0 GOTO QuitWithRollback;");
    out.push("");

    // Schedules. sp_add_jobschedule creates the schedule and attaches it
    // to the job atomically — one call per schedule.
    const scheds = (j.schedules || []);
    for (const sc of scheds) {
      out.push("  -- schedule: " + (sc.schedule_name || '(unnamed)'));
      out.push("  EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule");
      out.push("      @job_id                 = @JobId");
      out.push("    , @name                   = N'" + tsqlEsc(sc.schedule_name || ('schedule_' + sc.schedule_id)) + "'");
      out.push("    , @enabled                = " + (sc.schedule_enabled ? 1 : 0));
      out.push("    , @freq_type              = " + (sc.freq_type || 1));
      out.push("    , @freq_interval          = " + (sc.freq_interval || 0));
      out.push("    , @freq_subday_type       = " + (sc.freq_subday_type || 0));
      out.push("    , @freq_subday_interval   = " + (sc.freq_subday_interval || 0));
      out.push("    , @freq_relative_interval = " + (sc.freq_relative_interval || 0));
      out.push("    , @freq_recurrence_factor = " + (sc.freq_recurrence_factor || 0));
      out.push("    , @active_start_date      = " + (sc.active_start_date || 0));
      out.push("    , @active_end_date        = " + (sc.active_end_date   || 99991231));
      out.push("    , @active_start_time      = " + (sc.active_start_time || 0));
      out.push("    , @active_end_time        = " + (sc.active_end_time   || 235959));
      out.push("  ;");
      out.push("  IF @ReturnCode <> 0 GOTO QuitWithRollback;");
      out.push("");
    }

    // Attach the job to the local server. Required for Agent to actually
    // pick it up — without this it's defined but inert.
    out.push("  EXEC @ReturnCode = msdb.dbo.sp_add_jobserver");
    out.push("      @job_id      = @JobId");
    out.push("    , @server_name = N'(local)';");
    out.push("  IF @ReturnCode <> 0 GOTO QuitWithRollback;");
    out.push("");

    out.push("  COMMIT TRANSACTION;");
    out.push("  GOTO EndSave;");
    out.push("QuitWithRollback:");
    out.push("  IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;");
    out.push("EndSave:");
    out.push("END;");
    return out.join('\n');
  }

  function showPreview() {
    const anyLogin = SM.categories.logins && SM.selectedLoginIds.size > 0;
    const anyJob   = SM.categories.jobs   && SM.selectedJobIds.size > 0;
    if (!anyLogin && !anyJob) {
      alert('Select at least one login or job to preview.');
      return;
    }
    buildPreviewSql();
    const previewEl = $('sm-preview-pre');
    if (previewEl) {
      // Redact password hashes in the on-screen preview only. Real SQL sent
      // at execution still has them.
      const redacted = SM.previewSql.replace(/PASSWORD\s*=\s*0x[0-9a-fA-F]+/g, 'PASSWORD = 0x<REDACTED>');
      previewEl.textContent = redacted;
    }
    showStep('preview');
  }

  // ── Step 4: Execute ───────────────────────────────────────────────────────
  // Drives a sequence of per-object executions in dependency order:
  //   logins → operators → categories → jobs.
  // Each item is a single block; we feed it to impDbCall and catch errors
  // per-block so failures don't stop the run. Live status appended row by
  // row to the execute panel.
  async function executeNow() {
    if (!SM.previewSql) { alert('Generate the preview first.'); return; }

    const selectedLogins = SM.categories.logins ? SM.logins.filter(l => SM.selectedLoginIds.has(l.principal_id)) : [];
    const selectedJobs   = SM.categories.jobs   ? SM.jobs.filter(j => SM.selectedJobIds.has(j.job_id)) : [];
    const operators      = selectedJobs.length ? (SM.operators || []) : [];
    // Unique categories from selected jobs, excluding the built-in placeholder
    const seenCats = new Set();
    const categories = [];
    for (const j of selectedJobs) {
      if (j.category_name && j.category_name !== '[Uncategorized (Local)]' && !seenCats.has(j.category_name)) {
        seenCats.add(j.category_name);
        categories.push({ name: j.category_name, category_class: j.category_class, category_type: j.category_type });
      }
    }

    const totalUnits = selectedLogins.length + operators.length + categories.length + selectedJobs.length;
    if (totalUnits === 0) return;

    const parts = [];
    if (selectedLogins.length) parts.push(selectedLogins.length + ' login(s)');
    if (operators.length)      parts.push(operators.length + ' operator(s)');
    if (categories.length)     parts.push(categories.length + ' job categor' + (categories.length === 1 ? 'y' : 'ies'));
    if (selectedJobs.length)   parts.push(selectedJobs.length + ' job(s)');
    const confirmMsg =
      'About to deploy to:\n\n  ' + SM.targetDesc + '\n\n' +
      '  • ' + parts.join('\n  • ') + '\n\n' +
      (selectedJobs.length && SM.jobOptions.createDisabled
        ? 'Jobs will be created DISABLED. You can enable them after verification.\n\n'
        : '') +
      'Already-existing items are skipped via IF NOT EXISTS guards.\n\n' +
      'Proceed?';
    if (!confirm(confirmMsg)) return;

    SM.runId = 'smr_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7);
    SM.runResults = [];
    showStep('execute');

    const liveEl = $('sm-execute-live');
    if (liveEl) liveEl.innerHTML = '';
    const progressEl = $('sm-execute-progress');
    if (progressEl) progressEl.textContent = '0 / ' + totalUnits;

    const tgtConn = getTgtConn();
    let done = 0;

    // 1. Logins
    for (const l of selectedLogins) {
      const r = await executeLogin(tgtConn, l);
      SM.runResults.push(r); appendLiveRow(liveEl, r);
      done++; if (progressEl) progressEl.textContent = done + ' / ' + totalUnits;
    }
    // 2. Operators
    for (const op of operators) {
      const r = await executeOperator(tgtConn, op);
      SM.runResults.push(r); appendLiveRow(liveEl, r);
      done++; if (progressEl) progressEl.textContent = done + ' / ' + totalUnits;
    }
    // 3. Categories
    for (const c of categories) {
      const r = await executeCategory(tgtConn, c);
      SM.runResults.push(r); appendLiveRow(liveEl, r);
      done++; if (progressEl) progressEl.textContent = done + ' / ' + totalUnits;
    }
    // 4. Jobs
    for (const j of selectedJobs) {
      const r = await executeJob(tgtConn, j);
      SM.runResults.push(r); appendLiveRow(liveEl, r);
      done++; if (progressEl) progressEl.textContent = done + ' / ' + totalUnits;
    }

    saveAuditEntry();
    showStep('audit');
  }

  // Per-object executors. Each one builds the same SQL the preview shows,
  // sends it as one impDbCall, and verifies success with a follow-up
  // existence check. Each returns a structured result; never throws.

  async function executeLogin(tgtConn, l) {
    const result = { category: 'login', name: l.name, status: 'pending', message: '', timestamp: new Date().toISOString() };
    if (l.kind === 'sql' && !l.password_hash) {
      result.status = 'skipped'; result.message = 'Password hash unreadable on source';
      return result;
    }
    const stmts = [];
    stmts.push(`IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'${tsqlEsc(l.name)}')`);
    stmts.push('BEGIN');
    stmts.push(buildCreateLogin(l));
    stmts.push('END');
    if (l.is_disabled) stmts.push(`ALTER LOGIN ${tsqlIdent(l.name)} DISABLE;`);
    for (const role of l.server_roles) {
      stmts.push(`IF NOT EXISTS (SELECT 1 FROM sys.server_role_members rm JOIN sys.server_principals r ON r.principal_id = rm.role_principal_id JOIN sys.server_principals m ON m.principal_id = rm.member_principal_id WHERE r.name = N'${tsqlEsc(role)}' AND m.name = N'${tsqlEsc(l.name)}') ALTER SERVER ROLE ${tsqlIdent(role)} ADD MEMBER ${tsqlIdent(l.name)};`);
    }
    try {
      await dbCall(tgtConn, { action: 'execute', sql: stmts.join('\n') });
      const check = await dbCall(tgtConn, { action: 'execute', sql: `SELECT COUNT(*) AS n FROM sys.server_principals WHERE name = N'${tsqlEsc(l.name)}';` });
      const exists = (check?.recordset || [])[0]?.n >= 1;
      result.status = exists ? 'created' : 'failed';
      result.message = exists ? 'OK' : 'Login not present after CREATE LOGIN';
    } catch (e) {
      result.status = 'failed'; result.message = e.message || String(e);
    }
    return result;
  }

  async function executeOperator(tgtConn, op) {
    const result = { category: 'operator', name: op.name, status: 'pending', message: '', timestamp: new Date().toISOString() };
    try {
      await dbCall(tgtConn, { action: 'execute', sql: buildOperator(op) });
      const check = await dbCall(tgtConn, { action: 'execute', sql: `SELECT COUNT(*) AS n FROM msdb.dbo.sysoperators WHERE name = N'${tsqlEsc(op.name)}';` });
      const exists = (check?.recordset || [])[0]?.n >= 1;
      result.status = exists ? 'created' : 'failed';
      result.message = exists ? 'OK' : 'Operator not present after sp_add_operator';
    } catch (e) {
      result.status = 'failed'; result.message = e.message || String(e);
    }
    return result;
  }

  async function executeCategory(tgtConn, c) {
    const result = { category: 'category', name: c.name, status: 'pending', message: '', timestamp: new Date().toISOString() };
    try {
      await dbCall(tgtConn, { action: 'execute', sql: buildCategory(c) });
      const check = await dbCall(tgtConn, { action: 'execute', sql: `SELECT COUNT(*) AS n FROM msdb.dbo.syscategories WHERE name = N'${tsqlEsc(c.name)}' AND category_class = ${c.category_class || 1};` });
      const exists = (check?.recordset || [])[0]?.n >= 1;
      result.status = exists ? 'created' : 'failed';
      result.message = exists ? 'OK' : 'Category not present after sp_add_category';
    } catch (e) {
      result.status = 'failed'; result.message = e.message || String(e);
    }
    return result;
  }

  async function executeJob(tgtConn, j) {
    const result = { category: 'job', name: j.name, status: 'pending', message: '', timestamp: new Date().toISOString() };
    try {
      await dbCall(tgtConn, { action: 'execute', sql: buildJobBlock(j) });
      // Verify job and step counts on target. Aggregating with a LEFT JOIN
      // gets us both in one round trip.
      const check = await dbCall(tgtConn, { action: 'execute', sql:
        `SELECT COUNT(DISTINCT jj.job_id) AS n, COUNT(js.step_id) AS step_count
         FROM   msdb.dbo.sysjobs jj
         LEFT JOIN msdb.dbo.sysjobsteps js ON js.job_id = jj.job_id
         WHERE  jj.name = N'${tsqlEsc(j.name)}';`
      });
      const row = (check?.recordset || [])[0];
      const exists = row && row.n >= 1;
      result.status = exists ? 'created' : 'failed';
      if (exists) {
        const expected = (j.steps || []).length;
        const actual   = row.step_count || 0;
        if (actual < expected) {
          result.message = `OK (warning: ${actual}/${expected} steps created)`;
        } else {
          result.message = `OK — ${actual} step(s)` + (j.warnings.length ? ` · ${j.warnings.length} warning(s)` : '');
        }
      } else {
        result.message = 'Job not present after sp_add_job';
      }
    } catch (e) {
      result.status = 'failed'; result.message = e.message || String(e);
    }
    return result;
  }

  function appendLiveRow(container, r) {
    if (!container) return;
    const colour = r.status === 'created' ? 'var(--green)' :
                   r.status === 'skipped' ? 'var(--amber)' :
                                            'var(--red)';
    const symbol = r.status === 'created' ? '✓' : r.status === 'skipped' ? '–' : '✕';
    const catBadge =
      r.category === 'login'    ? '<span style="background:rgba(79,142,255,0.15);color:#6ea4ff;padding:1px 6px;border-radius:3px;font-size:10px;text-transform:uppercase">login</span>' :
      r.category === 'operator' ? '<span style="background:rgba(45,212,191,0.15);color:#2dd4bf;padding:1px 6px;border-radius:3px;font-size:10px;text-transform:uppercase">op</span>' :
      r.category === 'category' ? '<span style="background:rgba(167,139,250,0.15);color:#a78bfa;padding:1px 6px;border-radius:3px;font-size:10px;text-transform:uppercase">cat</span>' :
      r.category === 'job'      ? '<span style="background:rgba(245,158,11,0.15);color:#f59e0b;padding:1px 6px;border-radius:3px;font-size:10px;text-transform:uppercase">job</span>' :
                                  '';
    const row = document.createElement('div');
    row.style.cssText = 'display:flex;align-items:center;gap:0.75rem;padding:6px 10px;border-bottom:0.5px solid var(--border);font-size:12px;font-family:var(--mono)';
    row.innerHTML =
      '<span style="color:' + colour + ';width:14px;text-align:center">' + symbol + '</span>' +
      '<span style="width:50px">' + catBadge + '</span>' +
      '<span style="flex:1;color:var(--text)">' + escHtml(r.name) + '</span>' +
      '<span style="color:' + colour + ';font-size:11px;text-transform:uppercase">' + r.status + '</span>' +
      '<span style="color:var(--text3);font-size:11px;flex-basis:35%;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="' + escHtml(r.message) + '">' + escHtml(r.message) + '</span>';
    container.appendChild(row);
    container.scrollTop = container.scrollHeight;
  }

  // ── Step 5: Audit ─────────────────────────────────────────────────────────
  function saveAuditEntry() {
    // Split summary by category for clarity in the audit panel and JSON.
    const byCategory = {};
    const summary = SM.runResults.reduce((a, r) => { a[r.status] = (a[r.status] || 0) + 1; return a; }, {});
    for (const r of SM.runResults) {
      const c = r.category || 'unknown';
      if (!byCategory[c]) byCategory[c] = { created: 0, skipped: 0, failed: 0 };
      byCategory[c][r.status] = (byCategory[c][r.status] || 0) + 1;
    }
    const entry = {
      runId      : SM.runId,
      timestamp  : new Date().toISOString(),
      sourceDesc : SM.sourceDesc,
      targetDesc : SM.targetDesc,
      categories : { logins: SM.categories.logins, jobs: SM.categories.jobs },
      jobOptions : SM.categories.jobs ? { ...SM.jobOptions } : null,
      total      : SM.runResults.length,
      created    : summary.created || 0,
      skipped    : summary.skipped || 0,
      failed     : summary.failed  || 0,
      byCategory,
      results    : SM.runResults,
    };
    let history = [];
    try { history = JSON.parse(localStorage.getItem(AUDIT_KEY) || '[]'); } catch {}
    if (!Array.isArray(history)) history = [];
    history.unshift(entry);
    history = history.slice(0, 50);
    try { localStorage.setItem(AUDIT_KEY, JSON.stringify(history)); }
    catch (e) { console.error('[server-migration] Could not save audit:', e); }
    renderAuditSummary(entry);
  }

  function renderAuditSummary(entry) {
    const el = $('sm-audit-summary');
    if (!el) return;

    // Per-category breakdown rendered as a small table — easier to scan
    // than another row of giant numbers when there are 4 categories now.
    const catRows = Object.entries(entry.byCategory || {}).map(([cat, counts]) => {
      const total = (counts.created || 0) + (counts.skipped || 0) + (counts.failed || 0);
      return `
        <tr>
          <td style="padding:4px 8px;font-family:var(--mono);text-transform:uppercase;font-size:11px;color:var(--text2)">${escHtml(cat)}</td>
          <td style="padding:4px 8px;text-align:right">${total}</td>
          <td style="padding:4px 8px;text-align:right;color:var(--green)">${counts.created || 0}</td>
          <td style="padding:4px 8px;text-align:right;color:var(--amber)">${counts.skipped || 0}</td>
          <td style="padding:4px 8px;text-align:right;color:var(--red)">${counts.failed || 0}</td>
        </tr>`;
    }).join('');

    el.innerHTML = `
      <div style="display:flex;gap:1.5rem;flex-wrap:wrap;margin-bottom:1rem">
        <div><span style="color:var(--text3);font-size:11px;text-transform:uppercase">Total</span><div style="font-size:24px;font-weight:600">${entry.total}</div></div>
        <div><span style="color:var(--green);font-size:11px;text-transform:uppercase">Created</span><div style="font-size:24px;font-weight:600;color:var(--green)">${entry.created}</div></div>
        <div><span style="color:var(--amber);font-size:11px;text-transform:uppercase">Skipped</span><div style="font-size:24px;font-weight:600;color:var(--amber)">${entry.skipped}</div></div>
        <div><span style="color:var(--red);font-size:11px;text-transform:uppercase">Failed</span><div style="font-size:24px;font-weight:600;color:var(--red)">${entry.failed}</div></div>
      </div>
      ${catRows ? `
      <table style="width:100%;max-width:480px;border-collapse:collapse;font-size:12px;margin-bottom:1rem;border:0.5px solid var(--border);border-radius:var(--r)">
        <thead><tr style="background:var(--bg3);text-align:right">
          <th style="padding:6px 8px;text-align:left">Category</th>
          <th style="padding:6px 8px">Total</th>
          <th style="padding:6px 8px">Created</th>
          <th style="padding:6px 8px">Skipped</th>
          <th style="padding:6px 8px">Failed</th>
        </tr></thead>
        <tbody>${catRows}</tbody>
      </table>` : ''}
      <div style="font-size:12px;color:var(--text2)">
        Source: <span style="font-family:var(--mono)">${escHtml(entry.sourceDesc)}</span><br>
        Target: <span style="font-family:var(--mono)">${escHtml(entry.targetDesc)}</span><br>
        Run ID: <span style="font-family:var(--mono);color:var(--text3)">${escHtml(entry.runId)}</span>
      </div>`;
  }

  function downloadAudit() {
    let history = [];
    try { history = JSON.parse(localStorage.getItem(AUDIT_KEY) || '[]'); } catch {}
    const entry = history.find(h => h.runId === SM.runId) || history[0];
    if (!entry) { alert('No audit entry to download.'); return; }
    const blob = new Blob([JSON.stringify(entry, null, 2)], { type: 'application/json' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'cygenix-server-migration-' + entry.runId + '.json';
    a.click();
    URL.revokeObjectURL(a.href);
  }

  // ── Step navigation ───────────────────────────────────────────────────────
  function showStep(name) {
    const steps = ['verify','discover','preview','execute','audit'];
    for (const s of steps) {
      const panel = $('sm-step-' + s);
      if (panel) panel.style.display = (s === name) ? 'block' : 'none';
      const tab = $('sm-tab-' + s);
      if (tab) tab.classList.toggle('active', s === name);
    }
    // Show/hide the inner logins/jobs panels in the discover step based on
    // currently-selected categories. Done here rather than in discoverAll so
    // toggling the checkboxes after discovery still updates visibility.
    if (name === 'discover') {
      const loginsPanel = $('sm-discover-logins-panel');
      const jobsPanel   = $('sm-discover-jobs-panel');
      if (loginsPanel) loginsPanel.style.display = ($('sm-cat-logins')?.checked) ? 'block' : 'none';
      if (jobsPanel)   jobsPanel.style.display   = ($('sm-cat-jobs')?.checked)   ? 'block' : 'none';
    }
  }

  // Called when the user toggles a category checkbox at step 2. Just
  // shows/hides panels — doesn't trigger discovery. Discovery is explicit
  // via the Discover button.
  function _categoryToggled() {
    const loginsPanel = $('sm-discover-logins-panel');
    const jobsPanel   = $('sm-discover-jobs-panel');
    if (loginsPanel) loginsPanel.style.display = ($('sm-cat-logins')?.checked) ? 'block' : 'none';
    if (jobsPanel)   jobsPanel.style.display   = ($('sm-cat-jobs')?.checked)   ? 'block' : 'none';
  }

  function startOver() {
    SM.verified = false;
    SM.logins = [];
    SM.selectedLoginIds = new Set();
    SM.jobs = [];
    SM.selectedJobIds = new Set();
    SM.operators = [];
    SM.previewSql = '';
    SM.runId = '';
    SM.runResults = [];
    showStep('verify');
    const banner = $('sm-verify-banner');
    if (banner) {
      banner.style.borderColor = '';
      banner.style.background = '';
    }
    const proceedBtn = $('sm-proceed-btn');
    if (proceedBtn) proceedBtn.disabled = true;
    ['sm-src-dot','sm-tgt-dot'].forEach(id => { const d = $(id); if (d) d.style.background = 'var(--text3)'; });
    ['sm-src-msg','sm-tgt-msg'].forEach(id => { const m = $(id); if (m) m.textContent = 'Click Verify to test connection.'; });
  }

  function init() {
    showStep('verify');
    try {
      SM.sourceDesc = describeConn(getSrcConn());
      SM.targetDesc = describeConn(getTgtConn());
    } catch {}
    const srcMsg = $('sm-src-msg');
    const tgtMsg = $('sm-tgt-msg');
    if (srcMsg) srcMsg.textContent = SM.sourceDesc + ' — click Verify to test sysadmin.';
    if (tgtMsg) tgtMsg.textContent = SM.targetDesc + ' — click Verify to test sysadmin.';
  }

  // ── Public API ────────────────────────────────────────────────────────────
  window.ServerMigration = {
    init,
    verifyConnections,
    discoverAll,            // category-aware entry point
    showPreview,
    executeNow,
    downloadAudit,
    startOver,
    showStep,
    _toggleLogin,
    _toggleAllLogins,
    _filterLoginsChanged,
    _toggleJob,
    _toggleAllJobs,
    _filterJobsChanged,
    _categoryToggled,
    // Back-compat aliases — earlier dashboard.html may still bind to these:
    discoverLogins : discoverAll,
    _toggleOne     : _toggleLogin,
    _toggleAll     : _toggleAllLogins,
    _filterChanged : _filterLoginsChanged,
  };
})();
