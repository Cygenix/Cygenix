/* ============================================================================
   cygenix-profiles.js — connection profiles as a first-class object
   ----------------------------------------------------------------------------
   Cygenix binds connections globally: one live Source and one live Target,
   inherited by every page. An artifact therefore does not HAVE a connection —
   it has whatever was selected when someone pressed run. This module supplies
   the missing objects:

     connection meta   env class / system domain / owner per SAVED connection
                       (the saved entry itself is untouched — meta lives here)
     profile           a governed pairing of one source + one target + a
                       project + a service account. Immutable once active:
                       a changed endpoint is a NEW profile that supersedes,
                       never an edit.
     binding           exactly one profile per runnable artifact
     run record        expected identity vs actual identity, every run,
                       success or failure

   plus the environment sentinel (cygenix_env_sentinel.sql) as generated,
   per-dialect SQL: install (DDL, operator action), assertion (SELECT-only,
   run before work), and the weaker fingerprint fallback.

   Field names deliberately match the Connection Profile Register workbook
   (Connections / Profiles / Bindings sheets) so a customer's interim
   register migrates by copy.

   Migration posture (per the handover, §6): while NO profile exists the
   product behaves as before — ambient connections, a warning banner. The
   moment the first profile is defined, writes require an active profile,
   an UNKNOWN environment blocks writes, and PRD writes need a typed
   confirmation. Reads stay permissive throughout.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object') root.CygenixProfiles = api;
})(typeof window !== 'undefined' ? window : this, function () {
'use strict';

var STORE_KEY = 'cygenix_profiles_v1';
var STORE_VERSION = 1;
var RUN_RECORD_CAP = 300;

/* Environment classes, ordered by blast radius. UNKNOWN is the migration
   default for unclassified connections — permissive for reads, blocking
   for writes, so the product ships without stopping the world but pressure
   exists to classify. The list is configurable per store (settings). */
var ENV_CLASSES = ['DEV', 'TEST', 'UAT', 'PRD', 'SANDBOX', 'UNKNOWN'];

/* semantic tier for the banner / chips — pages map tiers to their tokens */
function cpEnvTier(envClass) {
  switch (String(envClass || '').toUpperCase()) {
    case 'PRD': return 'danger';
    case 'UAT': return 'warn';
    case 'DEV': case 'TEST': case 'SANDBOX': return 'ok';
    default: return 'neutral';               /* UNKNOWN, unset */
  }
}

/* =======================================================================
   Store
   ======================================================================= */
function cpNewStore(now) {
  return {
    v: STORE_VERSION, createdAt: now || 0,
    connMeta: {},          /* savedConnectionId -> { envClass, systemDomain, owner, approver } */
    profiles: [],
    bindings: [],          /* { artifactType, artifactId, profileId, boundAt, boundBy, lastVerifiedAt } */
    runRecords: [],
    events: [],
    settings: { envClasses: ENV_CLASSES.slice(0, 5), activeProfileId: null },
  };
}

function cpEvent(store, ev, now) {
  store.events.push(Object.assign({ at: now || 0 }, ev));
  if (store.events.length > 500) store.events.splice(0, store.events.length - 500);
}

/* browser-side load/save; node tests construct stores directly */
function cpLoad() {
  try {
    var raw = (typeof localStorage !== 'undefined') && localStorage.getItem(STORE_KEY);
    if (raw) return JSON.parse(raw);
  } catch (e) { /* fall through */ }
  return cpNewStore(Date.now ? Date.now() : 0);
}
function cpSave(store) {
  try { localStorage.setItem(STORE_KEY, JSON.stringify(store)); } catch (e) { /* quota */ }
}

/* the feature is "adopted" once any profile exists — enforcement turns on */
function cpAdopted(store) {
  return !!(store && store.profiles && store.profiles.length);
}

/* =======================================================================
   Connection meta + the standard name
   ENV_ROLE_SYSTEM_DATABASE — environment first, so a list sorts by blast
   radius and a wrong-environment entry is visible without reading to the
   end of the string.
   ======================================================================= */
function cpSetConnMeta(store, connId, meta, user, now) {
  var m = store.connMeta[connId] || {};
  store.connMeta[connId] = {
    envClass: String(meta.envClass || m.envClass || 'UNKNOWN').toUpperCase(),
    systemDomain: (meta.systemDomain != null ? meta.systemDomain : m.systemDomain) || '',
    owner: (meta.owner != null ? meta.owner : m.owner) || '',
    approver: (meta.approver != null ? meta.approver : m.approver) || '',
  };
  cpEvent(store, { type: 'conn.meta', connId: connId, envClass: store.connMeta[connId].envClass, by: user }, now);
  return store.connMeta[connId];
}
function cpConnMeta(store, connId) {
  return store.connMeta[connId] || { envClass: 'UNKNOWN', systemDomain: '', owner: '', approver: '' };
}

/* database name out of a saved-connection entry, for the standard name */
function cpDatabaseOf(entry) {
  if (!entry) return '';
  if (entry.mode === 'azure' && entry.fnUrl) {
    var m = /https?:\/\/([^\/.:]+)/.exec(entry.fnUrl);
    return m ? m[1] : 'azure-fn';
  }
  var cs = entry.connString || '';
  var m2 = /(?:^|;)\s*database\s*=\s*([^;,]+)/i.exec(cs)
        || /^[a-z+]+:\/\/[^\/]+\/([^?;\s]+)/i.exec(cs);
  return m2 ? m2[1].trim() : '';
}

function cpStandardName(store, entry) {
  var meta = cpConnMeta(store, entry && entry.id);
  var role = entry && entry.side === 'src' ? 'SRC' : 'TGT';
  var db = cpDatabaseOf(entry) || (entry && entry.name) || '?';
  return meta.envClass + '_' + role + '_' + (meta.systemDomain || 'UNCLASSIFIED') + '_' + db;
}

/* Locked while referenced by a non-retired profile: editing or deleting the
   connection would silently rewrite the meaning of every artifact that ever
   ran under that profile, including historic reports. */
function cpIsConnLocked(store, connId) {
  return store.profiles.some(function (p) {
    return p.status !== 'retired' && (p.srcConnId === connId || p.tgtConnId === connId);
  });
}

/* =======================================================================
   Profiles — draft → active → retired. Never edited into a new meaning.
   ======================================================================= */
function profileOf(store, id) {
  return store.profiles.filter(function (p) { return p.id === id; })[0] || null;
}

function cpValidateProfile(store, draft, savedConns) {
  var errors = [];
  var byId = {};
  (savedConns || []).forEach(function (s) { byId[s.id] = s; });
  if (!draft.id || !String(draft.id).trim()) errors.push('Profile ID is required (e.g. FIN-PRD-01).');
  if (!draft.envClass || draft.envClass === 'UNKNOWN') errors.push('Environment class is required.');
  if (!draft.srcConnId) errors.push('A source connection is required.');
  if (!draft.tgtConnId) errors.push('A target connection is required.');
  var src = byId[draft.srcConnId], tgt = byId[draft.tgtConnId];
  if (draft.srcConnId && !src) errors.push('Source connection ' + draft.srcConnId + ' is not in the saved list.');
  if (draft.tgtConnId && !tgt) errors.push('Target connection ' + draft.tgtConnId + ' is not in the saved list.');
  if (src && src.side !== 'src') errors.push('The source connection is saved as a ' + src.side + ' connection.');
  if (tgt && tgt.side !== 'tgt') errors.push('The target connection is saved as a ' + tgt.side + ' connection.');
  /* the rule that catches cross-environment pairings before they run:
     profile env must equal BOTH connections' env */
  ['src', 'tgt'].forEach(function (side) {
    var id = side === 'src' ? draft.srcConnId : draft.tgtConnId;
    if (!id || !byId[id]) return;
    var env = cpConnMeta(store, id).envClass;
    if (env !== draft.envClass) {
      errors.push('The ' + side + ' connection is classified ' + env + ' but the profile is '
        + draft.envClass + ' — a profile must not span environments.');
    }
  });
  return errors;
}

function cpSaveProfile(store, draft, savedConns, user, now) {
  var existing = profileOf(store, draft.id);
  if (existing && existing.status === 'active'
      && ((draft.srcConnId && draft.srcConnId !== existing.srcConnId)
       || (draft.tgtConnId && draft.tgtConnId !== existing.tgtConnId))) {
    throw new Error('Profile ' + existing.id + ' is active — its connection references are immutable. '
      + 'A changed endpoint is a NEW profile that supersedes this one (retire, then create with '
      + 'supersedesProfileId), so every artifact that ran under it keeps its meaning.');
  }
  if (existing) {
    Object.assign(existing, draft, { id: existing.id, status: existing.status });
    cpEvent(store, { type: 'profile.updated', profileId: existing.id, by: user }, now);
    return existing;
  }
  var p = Object.assign({
    name: draft.id, envClass: 'UNKNOWN',
    srcConnId: null, tgtConnId: null,
    credentialRef: '', projectId: null, owner: user || '', approver: '',
    status: 'draft', effectiveFrom: null, retiredAt: null, supersedesProfileId: null,
    createdAt: now || 0, createdBy: user || '',
  }, draft);
  store.profiles.push(p);
  cpEvent(store, { type: 'profile.created', profileId: p.id, by: user }, now);
  return p;
}

function cpActivateProfile(store, id, savedConns, user, now) {
  var p = profileOf(store, id);
  if (!p) throw new Error('No profile ' + id);
  var errors = cpValidateProfile(store, p, savedConns);
  if (errors.length) throw new Error('Profile cannot activate: ' + errors.join(' '));
  p.status = 'active';
  p.effectiveFrom = p.effectiveFrom || (now || 0);
  cpEvent(store, { type: 'profile.activated', profileId: id, by: user }, now);
  return p;
}

function cpRetireProfile(store, id, user, now) {
  var p = profileOf(store, id);
  if (!p) throw new Error('No profile ' + id);
  p.status = 'retired';
  p.retiredAt = now || 0;
  if (store.settings.activeProfileId === id) store.settings.activeProfileId = null;
  cpEvent(store, { type: 'profile.retired', profileId: id, by: user }, now);
  return p;
}

/* =======================================================================
   Bindings — exactly one profile per runnable artifact. An unbound
   artifact cannot run once the feature is adopted; there is no implicit
   default at RUN time. (The selected profile is a default for NEWLY
   CREATED artifacts only — the stamp happens at creation, not at run.)
   ======================================================================= */
function cpBind(store, artifactType, artifactId, profileId, user, now) {
  var existing = store.bindings.filter(function (b) {
    return b.artifactType === artifactType && b.artifactId === artifactId;
  })[0];
  if (existing) {
    cpEvent(store, { type: 'binding.rebound', artifactType: artifactType, artifactId: artifactId,
      from: existing.profileId, to: profileId, by: user }, now);
    existing.profileId = profileId;
    existing.boundAt = now || 0;
    existing.boundBy = user || '';
    return existing;
  }
  var b = { artifactType: artifactType, artifactId: artifactId, profileId: profileId,
    boundAt: now || 0, boundBy: user || '', lastVerifiedAt: null };
  store.bindings.push(b);
  cpEvent(store, { type: 'binding.created', artifactType: artifactType, artifactId: artifactId,
    profileId: profileId, by: user }, now);
  return b;
}
function cpBindingOf(store, artifactType, artifactId) {
  return store.bindings.filter(function (b) {
    return b.artifactType === artifactType && b.artifactId === artifactId;
  })[0] || null;
}

/* the actual connection value a saved entry resolves to (direct string, or
   azure fn URL + code) — mirrors CygenixConnections.srcConn/tgtConn */
function cpConnValue(entry) {
  if (!entry) return '';
  if (entry.mode === 'azure' && entry.fnUrl) {
    return entry.fnKey
      ? entry.fnUrl + (entry.fnUrl.indexOf('?') >= 0 ? '&' : '?') + 'code=' + encodeURIComponent(entry.fnKey)
      : entry.fnUrl;
  }
  return entry.connString || '';
}

/* =======================================================================
   THE choke point. Connection resolution from the binding, never from UI
   state. Refusals name their reason and the fix.
   ======================================================================= */
function cpResolve(store, artifactType, artifactId, savedConns, opts) {
  var intent = (opts && opts.intent) || 'read';
  if (!cpAdopted(store)) {
    return { ok: false, checked: false,
      why: 'No connection profiles are defined yet — resolution falls back to the ambient connection.' };
  }
  var b = cpBindingOf(store, artifactType, artifactId);
  if (!b) return { ok: false, checked: true,
    why: 'This ' + artifactType + ' is not bound to a connection profile. An unbound artifact cannot run — bind it on the Profiles page.' };
  var p = profileOf(store, b.profileId);
  if (!p) return { ok: false, checked: true,
    why: 'Bound profile ' + b.profileId + ' no longer exists.' };
  if (p.status !== 'active') return { ok: false, checked: true,
    why: 'Profile ' + p.id + ' is ' + p.status + ' — only an active profile can run. '
      + (p.status === 'retired' ? 'Re-bind this artifact to the profile that superseded it.' : 'Activate it first.') };
  if (p.envClass === 'UNKNOWN' && intent === 'write') {
    return { ok: false, checked: true,
      why: 'Profile ' + p.id + ' has environment UNKNOWN — writes are blocked until the connections are classified.' };
  }
  var byId = {};
  (savedConns || []).forEach(function (s) { byId[s.id] = s; });
  var src = byId[p.srcConnId], tgt = byId[p.tgtConnId];
  if (!src || !tgt) return { ok: false, checked: true,
    why: 'A connection this profile references has been removed from the saved list ('
      + (!src ? p.srcConnId : p.tgtConnId) + '). Profiles lock their connections — restore it or supersede the profile.' };
  return { ok: true, checked: true, profile: p, binding: b,
    src: cpConnValue(src), tgt: cpConnValue(tgt),
    srcEntry: src, tgtEntry: tgt, envClass: p.envClass };
}

/* Write guard for tool pages (import / restore / generator / linked
   servers). Tools are not durable artifacts, so they run under the
   SELECTED profile; the same migration posture applies. */
function cpGuardWrite(store, opts) {
  if (!cpAdopted(store)) {
    return { ok: true, checked: false,
      why: 'No profiles defined — ambient connections, no environment guard.' };
  }
  var id = store.settings.activeProfileId;
  if (!id) return { ok: false, checked: true,
    why: 'Profiles are in use but none is selected as active. Writes are blocked — select the profile this work runs under (Profiles page).' };
  var p = profileOf(store, id);
  if (!p || p.status !== 'active') return { ok: false, checked: true,
    why: 'Selected profile ' + id + ' is ' + (p ? p.status : 'missing') + ' — writes are blocked.' };
  if (p.envClass === 'UNKNOWN') return { ok: false, checked: true,
    why: 'Profile ' + p.id + ' has environment UNKNOWN — writes are blocked until its connections are classified.' };
  return { ok: true, checked: true, profile: p, envClass: p.envClass,
    requiresTypedConfirm: p.envClass === 'PRD' };
}

/* =======================================================================
   The environment sentinel — generated from cygenix_env_sentinel.sql.
   Install (Parts 1+2) needs CREATE TABLE once per database: an operator
   action, never an automatic migration. The assertion (Part 3) needs
   SELECT only. The fingerprint (Part 4) is the no-DDL fallback: it
   detects a changed endpoint, not a changed meaning.

   Pitfalls preserved from the field-tested original — do not "simplify":
   - T-SQL reads via sp_executesql: static references to a missing table
     fail at compile time (Msg 208) before any friendly message runs
   - empty table guarded explicitly: NULL <> 'x' is UNKNOWN and the run
     would proceed against an unidentified database
   - THROW, not RAISERROR: no % substitution, so a % in a name is safe
   - no GO: it is a client directive, not T-SQL — through ADO.NET/JDBC/
     ODBC it is a syntax error and the assertion silently never runs
   - PostgreSQL reads via EXECUTE…INTO (compile-time resolution otherwise)
     with GET DIAGNOSTICS (EXECUTE does not set FOUND), IS DISTINCT FROM
     (NULL-safe), and a relkind check (to_regclass alone matches views)
   ======================================================================= */
function lit(v) { return "'" + String(v == null ? '' : v).replace(/'/g, "''") + "'"; }

function cpSentinelInstallSql(dialect, seed) {
  var s = seed || {};
  if (dialect === 'postgres') {
    return [
      '-- Cygenix environment sentinel — install + seed (run once per database).',
      '-- Re-run the INSERT after ANY restore: a production backup restored over a',
      '-- lower environment carries the production sentinel with it.',
      'CREATE TABLE IF NOT EXISTS public._cyg_env_marker (',
      '    marker_id       smallint     PRIMARY KEY CHECK (marker_id = 1),',
      '    env_class       varchar(16)  NOT NULL,',
      '    role_in_profile varchar(8)   NOT NULL,',
      '    profile_id      varchar(32)  NOT NULL,',
      '    system_domain   varchar(32)  NOT NULL,',
      '    standard_name   varchar(128) NOT NULL,',
      '    owner_name      varchar(128),',
      '    set_by          name         NOT NULL DEFAULT current_user,',
      '    set_at          timestamptz  NOT NULL DEFAULT now()',
      ');',
      '',
      'INSERT INTO public._cyg_env_marker',
      '    (marker_id, env_class, role_in_profile, profile_id, system_domain, standard_name, owner_name)',
      'VALUES',
      '    (1, ' + lit(s.envClass) + ', ' + lit(s.role) + ', ' + lit(s.profileId) + ', '
        + lit(s.systemDomain) + ', ' + lit(s.standardName) + ', ' + lit(s.owner) + ')',
      'ON CONFLICT (marker_id) DO UPDATE',
      'SET env_class       = EXCLUDED.env_class,',
      '    role_in_profile = EXCLUDED.role_in_profile,',
      '    profile_id      = EXCLUDED.profile_id,',
      '    system_domain   = EXCLUDED.system_domain,',
      '    standard_name   = EXCLUDED.standard_name,',
      '    owner_name      = EXCLUDED.owner_name,',
      '    set_by          = current_user,',
      '    set_at          = now();',
    ].join('\n');
  }
  return [
    '-- Cygenix environment sentinel — install + seed (run once per database).',
    '-- Re-run the MERGE after ANY restore: a production backup restored over a',
    '-- lower environment carries the production sentinel with it.',
    "IF OBJECT_ID('dbo._cyg_env_marker', 'U') IS NULL",
    'BEGIN',
    ' CREATE TABLE dbo._cyg_env_marker',
    '    (',
    '        marker_id       TINYINT       NOT NULL CONSTRAINT PK__cyg_env_marker PRIMARY KEY',
    ' CONSTRAINT CK__cyg_env_marker_single CHECK (marker_id = 1),',
    '        env_class       VARCHAR(16)   NOT NULL,',
    '        role_in_profile VARCHAR(8)    NOT NULL,',
    '        profile_id      VARCHAR(32)   NOT NULL,',
    '        system_domain   VARCHAR(32)   NOT NULL,',
    '        standard_name   VARCHAR(128)  NOT NULL,',
    '        owner_name      VARCHAR(128)  NULL,',
    '        -- ISNULL fallback: on Azure SQL SUSER_SNAME() can be NULL for',
    '        -- contained users and Entra principals',
    '        set_by          SYSNAME       NOT NULL',
    ' CONSTRAINT DF__cyg_env_by DEFAULT ISNULL(SUSER_SNAME(), USER_NAME()),',
    '        set_at          DATETIME2(0)  NOT NULL CONSTRAINT DF__cyg_env_at DEFAULT SYSUTCDATETIME()',
    '    );',
    'END;',
    '',
    'MERGE dbo._cyg_env_marker AS t',
    'USING (SELECT 1 AS marker_id,',
    '              ' + lit(s.envClass) + ' AS env_class,',
    '              ' + lit(s.role) + ' AS role_in_profile,',
    '              ' + lit(s.profileId) + ' AS profile_id,',
    '              ' + lit(s.systemDomain) + ' AS system_domain,',
    '              ' + lit(s.standardName) + ' AS standard_name,',
    '              ' + lit(s.owner) + ' AS owner_name) AS s',
    ' ON t.marker_id = s.marker_id',
    'WHEN MATCHED THEN',
    ' UPDATE SET env_class = s.env_class, role_in_profile = s.role_in_profile,',
    '               profile_id = s.profile_id, system_domain = s.system_domain,',
    '               standard_name = s.standard_name, owner_name = s.owner_name,',
    '               set_by = ISNULL(SUSER_SNAME(), USER_NAME()), set_at = SYSUTCDATETIME()',
    'WHEN NOT MATCHED THEN',
    ' INSERT (marker_id, env_class, role_in_profile, profile_id, system_domain, standard_name, owner_name)',
    ' VALUES (s.marker_id, s.env_class, s.role_in_profile, s.profile_id,',
    '            s.system_domain, s.standard_name, s.owner_name);',
  ].join('\n');
}

function cpAssertionSql(dialect, expected) {
  var e = expected || {};
  if (dialect === 'postgres') {
    return [
      '-- Cygenix pre-run assertion (SELECT-only). Run with ON_ERROR_STOP=1 or',
      '-- inside the same transaction as the work.',
      'DO $$',
      'DECLARE',
      '    expected_profile constant varchar(32) := ' + lit(e.profileId) + ';',
      '    expected_env     constant varchar(16) := ' + lit(e.envClass) + ';',
      '    expected_role    constant varchar(8)  := ' + lit(e.role) + ';',
      '    where_txt  text;',
      '    a_profile  varchar(32); a_env varchar(16); a_role varchar(8); a_name varchar(128);',
      '    n_rows     integer;',
      'BEGIN',
      "    where_txt := 'host ' || coalesce(host(inet_server_addr()) || ':' || inet_server_port()::text,",
      "                                     'local unix socket')",
      "              || ', database ' || current_database() || ', user ' || current_user;",
      '    -- relkind check: to_regclass alone would also match a view or sequence',
      ' IF NOT EXISTS (SELECT 1 FROM pg_class',
      "                    WHERE oid = to_regclass('public._cyg_env_marker')",
      "                      AND relkind IN ('r', 'p')) THEN",
      ' RAISE EXCEPTION',
      "            'ENV SENTINEL MISSING - run aborted. No public._cyg_env_marker table on %. '",
      "            'This database is not registered to any connection profile.', where_txt;",
      ' END IF;',
      '    -- EXECUTE…INTO: a static SELECT is resolved when the DO block is',
      '    -- compiled, so a missing table would abort before the check above.',
      "    EXECUTE 'SELECT profile_id, env_class, role_in_profile, standard_name",
      "               FROM public._cyg_env_marker WHERE marker_id = 1'",
      ' INTO a_profile, a_env, a_role, a_name;',
      '    -- GET DIAGNOSTICS, not NOT FOUND: EXECUTE does not set FOUND.',
      ' GET DIAGNOSTICS n_rows = ROW_COUNT;',
      ' IF n_rows = 0 OR a_profile IS NULL THEN',
      ' RAISE EXCEPTION',
      "            'ENV SENTINEL EMPTY - run aborted. public._cyg_env_marker holds no row on %. '",
      "            'Re-seed the sentinel for this database.', where_txt;",
      ' END IF;',
      '    -- IS DISTINCT FROM, not <>: a NULL operand makes <> UNKNOWN and lets the run through.',
      ' IF a_profile IS DISTINCT FROM expected_profile',
      ' OR a_env  IS DISTINCT FROM expected_env',
      ' OR a_role IS DISTINCT FROM expected_role THEN',
      ' RAISE EXCEPTION',
      "            'ENV MISMATCH - run aborted. Expected % / % / % but connected to % / % / % (%) on %.',",
      '            expected_profile, expected_env, expected_role,',
      '            a_profile, a_env, a_role, a_name, where_txt;',
      ' END IF;',
      "    RAISE NOTICE 'ENV OK - % (%) on %', a_name, a_profile, where_txt;",
      'END $$;',
    ].join('\n');
  }
  /* T-SQL. Deliberately NO "GO": it is a client directive, and through
     ADO.NET/JDBC/ODBC it is a syntax error that silently disables the check. */
  return [
    '-- Cygenix pre-run assertion (SELECT-only, SQL Server 2012+ for THROW).',
    '-- Configure the calling step to fail on error, or wrap assertion + work',
    '-- in one transaction. Contains no GO on purpose.',
    'SET NOCOUNT ON;',
    'DECLARE @expected_profile VARCHAR(32) = ' + lit(e.profileId) + ';',
    'DECLARE @expected_env     VARCHAR(16) = ' + lit(e.envClass) + ';',
    'DECLARE @expected_role    VARCHAR(8)  = ' + lit(e.role) + ';',
    "DECLARE @server NVARCHAR(128) = ISNULL(CONVERT(NVARCHAR(128), SERVERPROPERTY('ServerName')), N'(server name unavailable)');",
    '-- every operand needs ISNULL: one NULL nulls the whole string',
    "DECLARE @where  NVARCHAR(600) = N'server ' + @server",
    "                              + N', database ' + ISNULL(DB_NAME(), N'(unknown database)')",
    "                              + N', login '    + ISNULL(SUSER_SNAME(), ISNULL(ORIGINAL_LOGIN(), USER_NAME()));",
    'DECLARE @msg    NVARCHAR(2048);',
    '-- existence first; the read below is dynamic because a static reference',
    '-- to a missing table fails the whole batch (Msg 208) at compile time',
    "IF OBJECT_ID('dbo._cyg_env_marker', 'U') IS NULL",
    "   OR COL_LENGTH('dbo._cyg_env_marker', 'profile_id') IS NULL",
    'BEGIN',
    "    SET @msg = N'ENV SENTINEL MISSING OR MALFORMED - run aborted. No usable dbo._cyg_env_marker on ' + @where",
    "             + N'. This database is not registered to any connection profile.';",
    ' THROW 50001, @msg, 1;',
    'END;',
    'DECLARE @actual_profile VARCHAR(32), @actual_env VARCHAR(16), @actual_role VARCHAR(8), @actual_name VARCHAR(128);',
    'EXEC sp_executesql',
    "     N'SELECT TOP (1) @p = profile_id, @e = env_class, @r = role_in_profile, @n = standard_name",
    "       FROM dbo._cyg_env_marker WHERE marker_id = 1;',",
    "     N'@p VARCHAR(32) OUTPUT, @e VARCHAR(16) OUTPUT, @r VARCHAR(8) OUTPUT, @n VARCHAR(128) OUTPUT',",
    '     @p = @actual_profile OUTPUT, @e = @actual_env OUTPUT,',
    '     @r = @actual_role    OUTPUT, @n = @actual_name OUTPUT;',
    '-- empty table must fail loudly: NULL <> value is UNKNOWN and the run',
    '-- would proceed against an unidentified database',
    'IF @actual_profile IS NULL OR @actual_env IS NULL OR @actual_role IS NULL OR @actual_name IS NULL',
    'BEGIN',
    "    SET @msg = N'ENV SENTINEL EMPTY - run aborted. dbo._cyg_env_marker holds no usable row on '",
    "             + @where + N'. Re-seed the sentinel for this database.';",
    ' THROW 50002, @msg, 1;',
    'END;',
    'IF @actual_profile <> @expected_profile OR @actual_env <> @expected_env OR @actual_role <> @expected_role',
    'BEGIN',
    "    SET @msg = N'ENV MISMATCH - run aborted. Expected '",
    "             + @expected_profile + N' / ' + @expected_env + N' / ' + @expected_role",
    "             + N' but connected to '",
    "             + @actual_profile + N' / ' + @actual_env + N' / ' + @actual_role",
    "             + N' (' + @actual_name + N') on ' + @where + N'.';",
    ' THROW 50003, @msg, 1;   -- THROW, not RAISERROR: no % substitution',
    'END;',
    "SELECT @actual_profile AS profile_id, @actual_name AS standard_name,",
    '       @server AS server_name, DB_NAME() AS database_name,',
    ' SYSUTCDATETIME() AS checked_at_utc;',
  ].join('\n');
}

/* the weaker, no-DDL fallback: detects a changed endpoint, not a changed meaning */
function cpFingerprintSql(dialect) {
  if (dialect === 'postgres') {
    return "SELECT coalesce(host(inet_server_addr()) || ':' || inet_server_port()::text, 'local unix socket') AS server_endpoint,\n"
      + '       current_database() AS database_name, current_user AS login_name,\n'
      + "       now() AT TIME ZONE 'UTC' AS checked_at_utc;";
  }
  return "SELECT ISNULL(CONVERT(NVARCHAR(128), SERVERPROPERTY('ServerName')), N'(unavailable)') AS server_name,\n"
    + ' DB_NAME() AS database_name, SUSER_SNAME() AS login_name,\n'
    + ' SYSUTCDATETIME() AS checked_at_utc;';
}

/* SELECT-only sentinel read for engines the app can query directly — the
   classifier below turns the outcome into an assertion_result. */
function cpSentinelReadSql(dialect) {
  if (dialect === 'postgres') {
    return 'SELECT profile_id, env_class, role_in_profile, standard_name FROM public._cyg_env_marker WHERE marker_id = 1';
  }
  return 'SELECT TOP (1) profile_id, env_class, role_in_profile, standard_name FROM dbo._cyg_env_marker WHERE marker_id = 1';
}

/* Classify a sentinel check outcome:
   pass | mismatch | sentinel-missing | sentinel-empty | not-checked */
function cpClassifySentinel(expected, outcome) {
  var err = outcome && outcome.error ? String(outcome.error) : null;
  if (err) {
    if (/ENV MISMATCH/i.test(err)) return { result: 'mismatch', detail: err };
    if (/SENTINEL MISSING/i.test(err)) return { result: 'sentinel-missing', detail: err };
    if (/SENTINEL EMPTY/i.test(err)) return { result: 'sentinel-empty', detail: err };
    /* a plain "invalid object name" / "relation does not exist" from the raw
       read means the marker is not installed */
    if (/invalid object name|relation .* does not exist|does not exist/i.test(err)) {
      return { result: 'sentinel-missing', detail: err };
    }
    return { result: 'not-checked', detail: err };
  }
  var rows = (outcome && outcome.rows) || [];
  if (!rows.length) return { result: 'sentinel-empty', detail: 'marker table holds no row' };
  var r = rows[0];
  var a = { profileId: r.profile_id, envClass: r.env_class, role: r.role_in_profile, name: r.standard_name };
  if (a.profileId == null || a.envClass == null || a.role == null) {
    return { result: 'sentinel-empty', detail: 'marker row is incomplete' };
  }
  if (String(a.profileId) !== String(expected.profileId)
   || String(a.envClass) !== String(expected.envClass)
   || String(a.role) !== String(expected.role)) {
    return { result: 'mismatch', actual: a,
      detail: 'Expected ' + expected.profileId + ' / ' + expected.envClass + ' / ' + expected.role
        + ' but connected to ' + a.profileId + ' / ' + a.envClass + ' / ' + a.role + ' (' + a.name + ').' };
  }
  return { result: 'pass', actual: a };
}

/* =======================================================================
   Run records — every execution, success or failure, expected vs actual.
   ======================================================================= */
function cpRecordRun(store, rec, now) {
  var r = Object.assign({
    runId: 'run_' + (now || 0).toString(36) + '_' + store.runRecords.length,
    artifactType: null, artifactId: null, profileId: null,
    actualServer: null, actualDatabase: null, actualLogin: null,
    assertionResult: 'not-checked',
    outcome: null, startedAt: now || 0, endedAt: null, runBy: null,
  }, rec);
  store.runRecords.push(r);
  if (store.runRecords.length > RUN_RECORD_CAP) {
    store.runRecords.splice(0, store.runRecords.length - RUN_RECORD_CAP);
  }
  return r;
}

/* =======================================================================
   Banner + naming
   ======================================================================= */
function cpBannerInfo(store) {
  if (!store || !cpAdopted(store)) {
    return { tier: 'neutral', profileId: null,
      text: 'No connection profile — connections are ambient. Define profiles to pin every run to a governed pairing.' };
  }
  var id = store.settings.activeProfileId;
  var p = id && profileOf(store, id);
  if (!p) return { tier: 'warn', profileId: null,
    text: 'Profiles are defined but none is selected — writes are blocked until one is active.' };
  return { tier: cpEnvTier(p.envClass), profileId: p.id, envClass: p.envClass,
    text: p.id + ' · ' + p.envClass + (p.name && p.name !== p.id ? ' · ' + p.name : '') };
}

/* profile-led report naming: a governance record must name its database */
function cpReportName(store, fallbackLabel, date) {
  var d = date || new Date();
  var pad = function (n) { return String(n).padStart(2, '0'); };
  var stamp = d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate())
    + '_' + pad(d.getHours()) + pad(d.getMinutes());
  var id = store && store.settings && store.settings.activeProfileId;
  var p = id && profileOf(store, id);
  return (p ? p.id : fallbackLabel) + '_' + stamp;
}

return {
  STORE_KEY: STORE_KEY,
  STORE_VERSION: STORE_VERSION,
  ENV_CLASSES: ENV_CLASSES,
  RUN_RECORD_CAP: RUN_RECORD_CAP,

  cpNewStore: cpNewStore,
  cpLoad: cpLoad,
  cpSave: cpSave,
  cpAdopted: cpAdopted,
  cpEvent: cpEvent,

  cpEnvTier: cpEnvTier,
  cpSetConnMeta: cpSetConnMeta,
  cpConnMeta: cpConnMeta,
  cpDatabaseOf: cpDatabaseOf,
  cpStandardName: cpStandardName,
  cpIsConnLocked: cpIsConnLocked,

  cpValidateProfile: cpValidateProfile,
  cpSaveProfile: cpSaveProfile,
  cpActivateProfile: cpActivateProfile,
  cpRetireProfile: cpRetireProfile,

  cpBind: cpBind,
  cpBindingOf: cpBindingOf,
  cpConnValue: cpConnValue,
  cpResolve: cpResolve,
  cpGuardWrite: cpGuardWrite,

  cpSentinelInstallSql: cpSentinelInstallSql,
  cpAssertionSql: cpAssertionSql,
  cpFingerprintSql: cpFingerprintSql,
  cpSentinelReadSql: cpSentinelReadSql,
  cpClassifySentinel: cpClassifySentinel,

  cpRecordRun: cpRecordRun,
  cpBannerInfo: cpBannerInfo,
  cpReportName: cpReportName,
};
});
