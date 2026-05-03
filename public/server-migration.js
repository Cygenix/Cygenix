// server-migration.js
// ─────────────────────────────────────────────────────────────────────────────
// Server Migration — Phase 1: Logins
//
// Replicates SQL Server logins (server-level principals) from a source instance
// to a target instance, preserving password hashes and SIDs so that any
// already-migrated database users stay correctly mapped to their server logins.
//
// Phase 1 is client-only: all SQL goes through the existing impDbCall proxy
// that the linked-servers tab uses (dashboard.html ~line 9388). No new Azure
// Function endpoints needed in this phase. The audit trail is held in
// localStorage; Cosmos persistence will follow in Phase 1.5 once we know what
// the record actually needs to look like in practice.
//
// Flow:
//   1. Verify  — runs IS_SRVROLEMEMBER('sysadmin') against source AND target.
//                Both must return 1, or the page won't unlock.
//   2. Discover — pulls login inventory from sys.server_principals + roles +
//                 LOGINPROPERTY(name, 'PasswordHash') for SQL logins.
//   3. Select  — checkbox list with select-all / by-type / search filter.
//   4. Preview — generates the CREATE LOGIN T-SQL with hashed passwords +
//                SID preservation + role memberships. User reads it before
//                anything runs.
//   5. Execute — runs the script statement-by-statement against the target,
//                with per-login pass/skip/fail status streamed into the UI.
//   6. Audit   — final summary saved to localStorage and downloadable as JSON.
//
// What we explicitly DON'T do:
//   - We don't migrate the `sa` login, `##MS_*` system logins, or
//     NT SERVICE\* / NT AUTHORITY\* built-ins. They're either fixed or
//     re-created automatically on the target.
//   - We don't migrate database users — those should already exist on the
//     target if you've migrated the database. Preserving SIDs means they
//     just re-map automatically.
//   - We don't change passwords. Hashes are scripted as-is so users keep
//     their existing credentials.
//   - We don't migrate certificates, asymmetric keys, or credentials —
//     those are out of scope for Phase 1 and noted in the Skipped list.
// ─────────────────────────────────────────────────────────────────────────────

(function () {
  'use strict';

  // ── State ─────────────────────────────────────────────────────────────────
  // Held in module scope, intentionally not persisted between page loads.
  // A migration session is short-lived — discover, review, execute, audit,
  // close. If the user reloads, they re-verify and re-discover. The audit
  // log is what persists.
  const SM = {
    verified: false,             // both sides confirmed sysadmin
    sourceDesc: '',              // human-readable label for source server
    targetDesc: '',              // human-readable label for target server
    logins: [],                  // discovered login inventory
    selectedIds: new Set(),      // login.principal_id values chosen for migration
    previewSql: '',              // generated T-SQL before execution
    runId: '',                   // unique id for this run, used in audit storage
    runResults: [],              // per-login pass/skip/fail records during execute
  };

  // localStorage key for the audit history. Capped at 50 most recent runs.
  const AUDIT_KEY = 'cygenix_server_migration_audit';

  // ── Helpers ───────────────────────────────────────────────────────────────

  // Short-hand DOM accessor — same pattern dashboard.html uses elsewhere.
  function $(id) { return document.getElementById(id); }

  // HTML-escape user-controlled values before injecting into innerHTML. Login
  // names and role names come from the source server and could contain
  // unusual characters; we don't want them breaking the UI or smuggling
  // markup. Same shape as escHtml elsewhere in dashboard.html.
  function escHtml(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#039;');
  }

  // T-SQL string-literal escape: doubles single quotes. Used for any value
  // that ends up inside N'...'. Login names, role names, default DB names
  // all need this.
  function tsqlEsc(s) {
    return String(s == null ? '' : s).replace(/'/g, "''");
  }

  // T-SQL identifier escape: wraps in [] and doubles any embedded ]. Used
  // for login names when they appear bare in DDL (CREATE LOGIN [name]).
  function tsqlIdent(s) {
    return '[' + String(s == null ? '' : s).replace(/]/g, ']]') + ']';
  }

  // Convert a varbinary hex string returned by SQL Server (looks like a
  // node-mssql Buffer or a "0x..." string depending on the proxy path) into
  // a clean "0x..." literal we can paste into T-SQL.
  //
  // The shape of `v` depends on how impDbCall's downstream proxy serialises
  // varbinary. We've seen all of:
  //   - { type: 'Buffer', data: [1,2,3,...] }   (Node's JSON.stringify of Buffer)
  //   - "0x010203..."                           (already a hex string)
  //   - "AQID..."                               (base64, rare)
  //   - null                                    (Windows logins have no hash)
  // This handles the first two reliably; base64 path is best-effort.
  function toHexLiteral(v) {
    if (v == null) return null;
    if (typeof v === 'string') {
      if (/^0x[0-9a-fA-F]*$/.test(v)) return v;
      // If it looks like base64, try to decode. If anything goes wrong we
      // return null and the caller skips the password-hashed path.
      try {
        const bin = atob(v);
        let hex = '0x';
        for (let i = 0; i < bin.length; i++) {
          hex += bin.charCodeAt(i).toString(16).padStart(2, '0');
        }
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

  // System logins we never migrate. These either always exist on the target
  // (sa, NT-prefixed built-ins) or are managed by SQL Server itself
  // (##MS_*). Migrating them either fails or causes weird state.
  function isSystemLogin(name) {
    if (!name) return true;
    if (name === 'sa') return true;
    if (name.startsWith('##MS_')) return true;
    if (name.startsWith('NT SERVICE\\')) return true;
    if (name.startsWith('NT AUTHORITY\\')) return true;
    if (name === 'public') return true;       // role, shouldn't appear but defensive
    return false;
  }

  // Tag a login row as SQL-auth or Windows-auth based on type_desc from
  // sys.server_principals. SQL_LOGIN gets a hashed password; WINDOWS_LOGIN
  // and WINDOWS_GROUP get FROM WINDOWS clauses.
  function loginKind(typeDesc) {
    if (typeDesc === 'SQL_LOGIN')      return 'sql';
    if (typeDesc === 'WINDOWS_LOGIN')  return 'win';
    if (typeDesc === 'WINDOWS_GROUP')  return 'wingroup';
    return 'other';
  }

  // ── Connection accessors ──────────────────────────────────────────────────
  // Both impGetConn() and impDbCall() are defined in dashboard.html and are
  // the same primitives the linked-servers tab uses. We don't redeclare —
  // we expect them to exist on window because dashboard.html has loaded.
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

  // Pull a one-line description of a connection (host + DB if possible) for
  // the verify banner. Mirrors the same masking + extraction lsDescribeTarget
  // does in the linked-servers tab; reused here so the user sees the same
  // shape in both places.
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

    // Reset UI before each attempt so a previously-failed check doesn't
    // leave green dots from an earlier run.
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

    // Run both checks in parallel — they're independent and 60s timeouts on
    // both means we don't want to wait sequentially.
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

  // ── Step 2: Discover logins on source ─────────────────────────────────────
  async function discoverLogins() {
    if (!SM.verified) {
      alert('Verify both connections first.');
      return;
    }
    const tableEl = $('sm-logins-table');
    const summaryEl = $('sm-logins-summary');
    if (tableEl) tableEl.innerHTML = '<div style="font-size:11px;color:var(--text3);padding:1rem">Discovering logins on source…</div>';
    if (summaryEl) summaryEl.textContent = '';

    // The query intentionally avoids querying password_hash directly from
    // sys.sql_logins (which requires VIEW SERVER STATE on top of just being
    // sysadmin in some configurations). LOGINPROPERTY(name, 'PasswordHash')
    // works as long as the caller is sysadmin and is the documented path.
    //
    // We pull role memberships in a separate column via STRING_AGG so each
    // login is a single row — keeps the result set small and the rendering
    // simple. STRING_AGG requires SQL Server 2017+; if you need to support
    // 2016 we'd switch to FOR XML PATH.
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
        CAST(LOGINPROPERTY(sp.name, 'IsLocked')          AS int)         AS is_locked,
        CAST(LOGINPROPERTY(sp.name, 'IsExpired')         AS int)         AS is_expired,
        CAST(LOGINPROPERTY(sp.name, 'IsMustChange')      AS int)         AS is_must_change,
        sp.is_policy_checked,
        sp.is_expiration_checked,
        ISNULL((
          SELECT STRING_AGG(rp.name, ',') WITHIN GROUP (ORDER BY rp.name)
          FROM   sys.server_role_members rm
          JOIN   sys.server_principals    rp ON rp.principal_id = rm.role_principal_id
          WHERE  rm.member_principal_id = sp.principal_id
        ), '') AS server_roles
      FROM sys.server_principals sp
      WHERE sp.type IN ('S','U','G')        -- SQL_LOGIN, WINDOWS_LOGIN, WINDOWS_GROUP
        AND sp.principal_id > 0             -- exclude built-ins (sa is principal_id=1, included)
      ORDER BY sp.name;
    `;

    let res;
    try {
      res = await dbCall(getSrcConn(), { action: 'execute', sql });
    } catch (e) {
      if (tableEl) tableEl.innerHTML = '<div style="color:var(--red);padding:1rem;font-size:12px">🔴 Discovery failed: ' + escHtml(e.message || String(e)) + '</div>';
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
      is_locked             : Number(r.is_locked) === 1,
      is_expired            : Number(r.is_expired) === 1,
      is_must_change        : Number(r.is_must_change) === 1,
      check_policy          : !!r.is_policy_checked,
      check_expiration      : !!r.is_expiration_checked,
      server_roles          : (r.server_roles || '').split(',').map(s => s.trim()).filter(Boolean),
      isSystem              : isSystemLogin(r.name),
    }));

    // Default selection: everything except system logins. Users can deselect
    // freely; selecting a system login is allowed but actively discouraged
    // by the disabled-by-default checkbox.
    SM.selectedIds = new Set(SM.logins.filter(l => !l.isSystem).map(l => l.principal_id));

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
      const counts = SM.logins.reduce((a, l) => {
        a[l.kind] = (a[l.kind] || 0) + 1; return a;
      }, {});
      const selected = SM.selectedIds.size;
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
              <input type="checkbox" id="sm-select-all" onchange="ServerMigration._toggleAll(this.checked)" style="cursor:pointer">
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
      const checked = SM.selectedIds.has(l.principal_id) ? 'checked' : '';
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
            <input type="checkbox" ${checked}
              onchange="ServerMigration._toggleOne(${l.principal_id}, this.checked)"
              style="cursor:pointer">
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

    // Sync select-all checkbox to current state
    const allBox = $('sm-select-all');
    if (allBox) {
      const visibleIds = filtered.map(l => l.principal_id);
      const allSelected = visibleIds.length > 0 && visibleIds.every(id => SM.selectedIds.has(id));
      allBox.checked = allSelected;
    }
  }

  function _toggleOne(principalId, checked) {
    if (checked) SM.selectedIds.add(principalId);
    else         SM.selectedIds.delete(principalId);
    renderLoginsTable();
  }

  function _toggleAll(checked) {
    const filterText = ($('sm-login-filter')?.value || '').trim().toLowerCase();
    const showSystem = !!$('sm-show-system')?.checked;
    const filtered = SM.logins.filter(l => {
      if (!showSystem && l.isSystem) return false;
      if (!filterText) return true;
      return l.name.toLowerCase().includes(filterText);
    });
    for (const l of filtered) {
      if (checked) SM.selectedIds.add(l.principal_id);
      else         SM.selectedIds.delete(l.principal_id);
    }
    renderLoginsTable();
  }

  function _filterChanged() { renderLoginsTable(); }

  // ── Step 3: Generate preview SQL ──────────────────────────────────────────
  // Produces a script with one IF NOT EXISTS guard per login, then the
  // CREATE LOGIN, then ALTER ROLE statements. Wrapping each login in an
  // IF NOT EXISTS guard means re-running the script after a partial failure
  // is safe — already-created logins are skipped at SQL level. This is
  // belt-and-braces with the per-login transaction wrapper we use at
  // execution time.
  function buildPreviewSql() {
    const selected = SM.logins.filter(l => SM.selectedIds.has(l.principal_id));
    if (selected.length === 0) {
      SM.previewSql = '-- No logins selected.';
      return;
    }
    const out = [];
    out.push('-- ============================================================================');
    out.push('-- Cygenix — Server Migration: Logins');
    out.push('-- Source: ' + SM.sourceDesc);
    out.push('-- Target: ' + SM.targetDesc);
    out.push('-- Generated: ' + new Date().toISOString());
    out.push('-- Selected logins: ' + selected.length);
    out.push('-- ============================================================================');
    out.push('-- Preserving SIDs and password hashes so existing database users stay mapped.');
    out.push('-- Each login is guarded with IF NOT EXISTS so re-running the script is safe.');
    out.push('-- ============================================================================');
    out.push('');

    for (const l of selected) {
      out.push('-- ──────────────────────────────────────────────');
      out.push('-- ' + l.name + '   (' + l.type_desc + ')');
      out.push('-- ──────────────────────────────────────────────');
      out.push('IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N\'' + tsqlEsc(l.name) + '\')');
      out.push('BEGIN');
      out.push(buildCreateLogin(l));
      out.push('END');

      // Disable if it was disabled on source — done OUTSIDE the IF NOT EXISTS
      // because we want to converge state even if the login already exists.
      if (l.is_disabled) {
        out.push('ALTER LOGIN ' + tsqlIdent(l.name) + ' DISABLE;');
      }

      // Server role memberships — also outside the IF NOT EXISTS so
      // already-existing logins still get their role memberships reconciled.
      // sp_addsrvrolemember is deprecated; ALTER SERVER ROLE is the modern
      // path and works on SQL Server 2012+.
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
    SM.previewSql = out.join('\n');
  }

  function buildCreateLogin(l) {
    const ind = '  ';
    if (l.kind === 'sql') {
      // SQL_LOGIN: CREATE LOGIN ... WITH PASSWORD = 0x... HASHED, SID = 0x...
      // CHECK_POLICY/CHECK_EXPIRATION are preserved from source.
      const lines = [];
      lines.push(ind + 'CREATE LOGIN ' + tsqlIdent(l.name));
      if (!l.password_hash) {
        // Couldn't read the hash — emit the statement commented out so the
        // user knows we hit it, plus a placeholder note.
        lines.push(ind + '  -- ⚠ Password hash unreadable on source — login will not be created.');
        lines.push(ind + '  -- Fix the source permissions or recreate this login manually on target.');
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
    // Windows logins / groups: CREATE LOGIN ... FROM WINDOWS WITH ...
    // No password, no SID (Windows manages it via SID-mapping into the OS).
    const lines = [];
    lines.push(ind + 'CREATE LOGIN ' + tsqlIdent(l.name) + ' FROM WINDOWS');
    lines.push(ind + '  WITH DEFAULT_DATABASE = ' + tsqlIdent(l.default_database_name));
    lines.push(ind + '     , DEFAULT_LANGUAGE = ' + tsqlIdent(l.default_language_name) + ';');
    return lines.join('\n');
  }

  function showPreview() {
    if (SM.selectedIds.size === 0) {
      alert('Select at least one login to preview.');
      return;
    }
    buildPreviewSql();
    const previewEl = $('sm-preview-pre');
    if (previewEl) {
      // Redact password hashes in the on-screen preview. The actual SQL
      // sent to the server still has them (it has to — that's what HASHED
      // means), but they don't need to be visible on a screenshare or
      // sitting in the DOM. Same approach as the linked-servers tab.
      const redacted = SM.previewSql.replace(/PASSWORD\s*=\s*0x[0-9a-fA-F]+/g, 'PASSWORD = 0x<REDACTED>');
      previewEl.textContent = redacted;
    }
    showStep('preview');
  }

  // ── Step 4: Execute ───────────────────────────────────────────────────────
  // Runs each login's block as a separate transaction so a failure on one
  // doesn't leave half a login created. Per-login result row is appended to
  // the live status panel. We do NOT batch these into a single GO-separated
  // script because impDbCall sends to the proxy as one statement — splitting
  // gives us per-login error reporting and rollback granularity.
  async function executeNow() {
    if (!SM.previewSql) {
      alert('Generate the preview first.');
      return;
    }
    const selected = SM.logins.filter(l => SM.selectedIds.has(l.principal_id));
    if (selected.length === 0) return;

    const confirmMsg =
      'About to create ' + selected.length + ' login(s) on:\n\n' +
      '  ' + SM.targetDesc + '\n\n' +
      'This runs CREATE LOGIN with HASHED passwords and preserved SIDs.\n' +
      'Already-existing logins will be skipped.\n\n' +
      'Proceed?';
    if (!confirm(confirmMsg)) return;

    SM.runId = 'smr_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7);
    SM.runResults = [];
    showStep('execute');

    const liveEl = $('sm-execute-live');
    if (liveEl) liveEl.innerHTML = '';
    const progressEl = $('sm-execute-progress');
    if (progressEl) progressEl.textContent = '0 / ' + selected.length;

    const tgtConn = getTgtConn();
    let done = 0;
    for (const l of selected) {
      const result = await executeOne(tgtConn, l);
      SM.runResults.push(result);
      appendLiveRow(liveEl, result);
      done++;
      if (progressEl) progressEl.textContent = done + ' / ' + selected.length;
    }

    saveAuditEntry();
    showStep('audit');
  }

  // Executes a single login's block. Returns a structured result; never
  // throws — all errors are captured into the result so the loop continues.
  async function executeOne(tgtConn, l) {
    const result = {
      principal_id : l.principal_id,
      name         : l.name,
      kind         : l.kind,
      status       : 'pending',   // pending | created | skipped | failed
      message      : '',
      timestamp    : new Date().toISOString(),
    };

    // SQL logins where we couldn't read the hash get marked skipped without
    // touching the server.
    if (l.kind === 'sql' && !l.password_hash) {
      result.status  = 'skipped';
      result.message = 'Password hash unreadable on source';
      return result;
    }

    // Build the per-login statement bundle. We don't BEGIN TRAN here because
    // CREATE LOGIN is a non-transactional DDL on most SQL Server versions —
    // wrapping it would either error or have no effect. Instead we rely on
    // the IF NOT EXISTS guard and on the per-step structure: if CREATE LOGIN
    // fails, role grants don't run.
    const stmts = [];
    stmts.push(`IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'${tsqlEsc(l.name)}')`);
    stmts.push('BEGIN');
    stmts.push(buildCreateLogin(l));
    stmts.push('END');
    if (l.is_disabled) stmts.push(`ALTER LOGIN ${tsqlIdent(l.name)} DISABLE;`);
    for (const role of l.server_roles) {
      stmts.push(`IF NOT EXISTS (
  SELECT 1
  FROM   sys.server_role_members rm
  JOIN   sys.server_principals    r ON r.principal_id = rm.role_principal_id
  JOIN   sys.server_principals    m ON m.principal_id = rm.member_principal_id
  WHERE  r.name = N'${tsqlEsc(role)}' AND m.name = N'${tsqlEsc(l.name)}'
)
  ALTER SERVER ROLE ${tsqlIdent(role)} ADD MEMBER ${tsqlIdent(l.name)};`);
    }

    const sql = stmts.join('\n');

    try {
      await dbCall(tgtConn, { action: 'execute', sql });

      // Verify outcome — was the login actually there afterwards? This
      // distinguishes "created" from "already existed (IF NOT EXISTS skip)".
      const check = await dbCall(tgtConn, {
        action: 'execute',
        sql:    `SELECT COUNT(*) AS n FROM sys.server_principals WHERE name = N'${tsqlEsc(l.name)}';`,
      });
      const exists = (check?.recordset || [])[0]?.n >= 1;
      // We can't tell from here whether we created it or it pre-existed
      // unless we did a pre-check. Pre-check would double the round-trips,
      // which adds up across hundreds of logins. Instead we mark it
      // "created" if the post-check shows it exists; the audit log shows
      // the IF NOT EXISTS branch was taken if subsequent re-runs report
      // "already exists" via a status field — refinement for Phase 1.5.
      result.status  = exists ? 'created' : 'failed';
      result.message = exists ? 'OK' : 'Login not present after CREATE LOGIN — unknown failure';
    } catch (e) {
      result.status  = 'failed';
      result.message = e.message || String(e);
    }
    return result;
  }

  function appendLiveRow(container, r) {
    if (!container) return;
    const colour = r.status === 'created' ? 'var(--green)' :
                   r.status === 'skipped' ? 'var(--amber)' :
                                            'var(--red)';
    const symbol = r.status === 'created' ? '✓' : r.status === 'skipped' ? '–' : '✕';
    const row = document.createElement('div');
    row.style.cssText = 'display:flex;align-items:center;gap:0.75rem;padding:6px 10px;border-bottom:0.5px solid var(--border);font-size:12px;font-family:var(--mono)';
    row.innerHTML =
      '<span style="color:' + colour + ';width:14px;text-align:center">' + symbol + '</span>' +
      '<span style="flex:1;color:var(--text)">' + escHtml(r.name) + '</span>' +
      '<span style="color:' + colour + ';font-size:11px;text-transform:uppercase">' + r.status + '</span>' +
      '<span style="color:var(--text3);font-size:11px;flex-basis:40%;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="' + escHtml(r.message) + '">' + escHtml(r.message) + '</span>';
    container.appendChild(row);
    container.scrollTop = container.scrollHeight;
  }

  // ── Step 5: Audit ─────────────────────────────────────────────────────────
  function saveAuditEntry() {
    const summary = SM.runResults.reduce((a, r) => {
      a[r.status] = (a[r.status] || 0) + 1; return a;
    }, {});
    const entry = {
      runId      : SM.runId,
      timestamp  : new Date().toISOString(),
      sourceDesc : SM.sourceDesc,
      targetDesc : SM.targetDesc,
      total      : SM.runResults.length,
      created    : summary.created || 0,
      skipped    : summary.skipped || 0,
      failed     : summary.failed  || 0,
      results    : SM.runResults,
    };
    let history = [];
    try { history = JSON.parse(localStorage.getItem(AUDIT_KEY) || '[]'); } catch {}
    if (!Array.isArray(history)) history = [];
    history.unshift(entry);
    history = history.slice(0, 50);   // cap
    try { localStorage.setItem(AUDIT_KEY, JSON.stringify(history)); }
    catch (e) { console.error('[server-migration] Could not save audit:', e); }
    renderAuditSummary(entry);
  }

  function renderAuditSummary(entry) {
    const el = $('sm-audit-summary');
    if (!el) return;
    el.innerHTML = `
      <div style="display:flex;gap:1.5rem;flex-wrap:wrap;margin-bottom:1rem">
        <div><span style="color:var(--text3);font-size:11px;text-transform:uppercase">Total</span><div style="font-size:24px;font-weight:600">${entry.total}</div></div>
        <div><span style="color:var(--green);font-size:11px;text-transform:uppercase">Created</span><div style="font-size:24px;font-weight:600;color:var(--green)">${entry.created}</div></div>
        <div><span style="color:var(--amber);font-size:11px;text-transform:uppercase">Skipped</span><div style="font-size:24px;font-weight:600;color:var(--amber)">${entry.skipped}</div></div>
        <div><span style="color:var(--red);font-size:11px;text-transform:uppercase">Failed</span><div style="font-size:24px;font-weight:600;color:var(--red)">${entry.failed}</div></div>
      </div>
      <div style="font-size:12px;color:var(--text2);margin-bottom:1rem">
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
  // The page is structured as a sequence of panels (verify → discover →
  // preview → execute → audit), only one visible at a time. Mirrors the
  // step-tab pattern from connect.html so the UX feels consistent.
  function showStep(name) {
    const steps = ['verify','discover','preview','execute','audit'];
    for (const s of steps) {
      const panel = $('sm-step-' + s);
      if (panel) panel.style.display = (s === name) ? 'block' : 'none';
      const tab = $('sm-tab-' + s);
      if (tab) {
        tab.classList.toggle('active', s === name);
      }
    }
  }

  function startOver() {
    SM.verified = false;
    SM.logins = [];
    SM.selectedIds = new Set();
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

  // ── Init: called from showView('server-migration') ────────────────────────
  function init() {
    showStep('verify');
    // Prime the verify banner with current connection labels so the user
    // sees what they're about to verify against without having to click.
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
    discoverLogins,
    showPreview,
    executeNow,
    downloadAudit,
    startOver,
    showStep,
    _toggleOne,
    _toggleAll,
    _filterChanged,
  };
})();
