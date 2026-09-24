/* ============================================================================
   cygenix-diagnostics.js — the Diagnostics tab (Govern > Audit log > Diagnostics)
   ----------------------------------------------------------------------------
   WHY THIS FILE EXISTS

   The Diagnostics tab had one button, "Run Tests", whose caption said it would
   "check your Netlify function setup", and whose onclick named a function that
   no longer existed anywhere in the codebase. Clicking it did nothing, in
   silence. It was developer-facing when it worked and dead by the time anyone
   looked, which is the worst combination a customer can find on a tab called
   Diagnostics.

   This module replaces it with the thing a customer actually needs before a
   migration: seventeen selectable checks, grouped, each answering one plain
   question — can Cygenix reach the server, does the login work, may it read
   the mapped tables, is the target big enough, is anything in the mapping
   pointing at a column that no longer exists — and each ending in a status
   word and, on anything but a pass, one sentence saying what to do.

   RULES THIS CODE KEEPS, AND WHY

   · The browser never sends SQL for a check. It names a probe; the server
     (netlify/functions/db-connect.js, diag-probe / diag-temp-table) owns the
     statement and binds the names. The only place SQL text appears in this
     file is the Azure-direct permission check, where the statement is a
     constant and the names still travel as parameters. A diagnostic gated
     as a connection test must never be a softer route to a statement the
     write gate would have refused.

   · Every check is read-only. Test 12 creates a temp table inside a
     transaction that is rolled back on the server, and the result carries
     "remainsAfterRollback" as a measured fact, so the panel can say nothing
     was left behind rather than assume it.

   · No credential reaches the screen, the report, or the console. Server
     names are cut down to the host. Every string that could have come from a
     driver error is passed through scrub(), which strips the connection
     values in play, any password= / pwd= / code= fragment, and the
     user:pass@ form of a URL. Nothing in this file calls console.log with a
     connection value, and the one console.warn carries a scrubbed message.

   · Calls run one at a time, each under its own 20s client timeout, so no
     single request goes near Netlify's 26s lambda cap and a hung server
     costs one test, not the run. Schema checks (13, 14) look at the mapped
     objects only, never the whole schema.

   · The Run button has an in-flight guard and a 3s minimum interval. The
     `running` flag is cleared in the finally of the run itself — never from
     a callback that the run could re-trigger — because a flag reset by its
     own completion handler is how a render loop starts.

   · Thresholds are constants at the top of this file, where a reader can see
     them, because "warn below the minimum" is only a useful sentence if the
     minimum is findable.

   The tab is target-agnostic. Nothing here knows a table name; the mapped
   objects come from the jobs bound to the chosen profile.
   ============================================================================ */
(function (root, factory) {
  'use strict';
  var api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  root.CygenixDiagnostics = api;
})(typeof window !== 'undefined' ? window : globalThis, function (root) {
  'use strict';

  /* ── Thresholds and settings — the numbers a reader will ask about ──────── */
  var MIN_VERSIONS = {
    mssql:    { major: 13, label: 'SQL Server 2016' },   // 13.x = 2016
    postgres: { major: 12, label: 'PostgreSQL 12' },
  };
  var PING_MS = { good: 100, slow: 500 };     // avg round trip: green < good, amber ≤ slow, red above
  var TEST_TIMEOUT_MS = 20000;                // per call, and per single-call test
  var MIN_RUN_INTERVAL_MS = 3000;             // Run selected cannot fire more often than this
  var LOW_SPACE_MB = 1024;                    // a data or log file, or its volume, with less than this free warns
  var HOSTING_REGION = 'UK South';            // where the console, Netlify functions and the Function App are
  var STORE_KEY = 'cygenix_diag_selection_v1';
  var MAX_TABLES_PER_PROBE = 200;

  /* ── The tests ───────────────────────────────────────────────────────────
     side: true    honours the Source / Target / Both toggle
           'src'   always the source side
           'tgt'   always the target side
           absent  not a per-side test                                        */
  var GROUPS = [
    { id: 'platform',    label: 'Cygenix platform',   note: 'The service itself, your sign-in and your plan.' },
    { id: 'connections', label: 'Connections',        note: 'The chosen profile’s source and target, from the network up.' },
    { id: 'permissions', label: 'Permissions',        note: 'Read-only checks of what the saved logins may do. Nothing is written.' },
    { id: 'readiness',   label: 'Migration readiness', note: 'The mapping against the live schema, space, collation and schedules.' },
  ];

  var TESTS = [
    { id: 't01', n: 1,  group: 'platform', name: 'Cygenix services reachable',
      desc: 'Calls the Cygenix web service and the data service and reports how long each took.' },
    { id: 't02', n: 2,  group: 'platform', name: 'Signed in and licence valid',
      desc: 'Checks your sign-in session is current and your plan is active and covers the features you use.' },
    { id: 't03', n: 3,  group: 'platform', name: 'Data region',
      desc: 'Shows where this console and its data are hosted and warns if the project or a connection expects somewhere else.' },
    { id: 't04', n: 4,  group: 'connections', side: true, name: 'Server reachable',
      desc: 'Reaches the database server on the network, telling a wrong address from a firewall from a refused port.' },
    { id: 't05', n: 5,  group: 'connections', side: true, name: 'Login works',
      desc: 'Signs in with the saved credentials.' },
    { id: 't06', n: 6,  group: 'connections', side: true, name: 'Database exists and opens',
      desc: 'Opens the named database with that login.' },
    { id: 't07', n: 7,  group: 'connections', side: true, name: 'Response time',
      desc: 'Runs a trivial query three times and reports the average: green under ' + PING_MS.good + ' ms, amber to ' + PING_MS.slow + ' ms, red above.' },
    { id: 't08', n: 8,  group: 'connections', side: true, name: 'Database version',
      desc: 'Reads the engine version and compatibility level and warns below ' + MIN_VERSIONS.mssql.label + ' or ' + MIN_VERSIONS.postgres.label + '.' },
    { id: 't09', n: 9,  group: 'permissions', side: 'src', name: 'Source read access',
      desc: 'Checks the source login can read every source table used in Object Mapping for this profile.' },
    { id: 't10', n: 10, group: 'permissions', side: 'tgt', name: 'Target write access',
      desc: 'Checks INSERT, UPDATE and DELETE on every mapped target table, and CREATE TABLE for staging.' },
    { id: 't11', n: 11, group: 'permissions', side: 'tgt', name: 'Bulk load permission',
      desc: 'Checks the target login may bulk load on SQL Server. Skipped on PostgreSQL, which needs no such right.' },
    { id: 't12', n: 12, group: 'permissions', side: 'tgt', name: 'Temp tables',
      desc: 'Creates a temporary table inside a transaction and rolls it back, then confirms nothing was left behind.' },
    { id: 't13', n: 13, group: 'readiness', name: 'Mapped objects still exist',
      desc: 'Every table and column in Object Mapping for this profile is still in the live source and target. Lists what has moved.' },
    { id: 't14', n: 14, group: 'readiness', name: 'Data type compatibility',
      desc: 'Flags mapped columns where the source value may not fit the target: length, precision, nullability, type. Warns, never fails.' },
    { id: 't15', n: 15, group: 'readiness', side: 'tgt', name: 'Target free space',
      desc: 'Reads free space in the target data and log files. On PostgreSQL, reports the database size and skips free space.' },
    { id: 't16', n: 16, group: 'readiness', name: 'Collation match',
      desc: 'Compares source and target database collations, and any mapped column pair that differs. Warns on a mismatch.' },
    { id: 't17', n: 17, group: 'readiness', name: 'Schedules point to valid profiles',
      desc: 'Every schedule linked to this profile still names connections that exist and that signed in (test 5).' },
  ];
  var BY_ID = {};
  TESTS.forEach(function (t) { BY_ID[t.id] = t; });

  /* ── Pure helpers (also exported, so tests/diagnostics.test.js can pin them) */

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  /* Which engine a saved connection value points at. An https URL is the
     Azure Function App, which reaches only the one database it is bound to. */
  function engineOf(value, entry) {
    if (entry && entry.mode === 'azure') return 'azure';
    var v = String(value || '').trim();
    if (/^https?:\/\//i.test(v)) return 'azure';
    if (/^(postgres|postgresql):\/\//i.test(v)) return 'postgres';
    if (/(^|;|\s)driver\s*=\s*postgres/i.test(v)) return 'postgres';
    if (/(^|\s)host\s*=/i.test(v) && /(^|\s)dbname\s*=/i.test(v)) return 'postgres';
    return 'mssql';
  }
  var ENGINE_LABEL = { mssql: 'SQL Server', postgres: 'PostgreSQL', azure: 'Azure-direct' };

  /* Host only — no port, no database, never a user or password. */
  function hostOf(value) {
    var v = String(value || '').trim();
    if (!v) return '';
    try {
      if (/^https?:\/\//i.test(v)) return new URL(v).hostname;
    } catch (e) { return ''; }
    var m = /^(?:mssql|sqlserver|postgres|postgresql):\/\/(?:[^@\/]*@)?([^:\/?#,]+)/i.exec(v);
    if (m) return m[1];
    var kv = /(?:^|;|\s)(?:server|data source|host|address|addr)\s*=\s*(?:tcp:)?([^;,\s]+)/i.exec(v);
    if (kv) return kv[1].replace(/^tcp:/i, '').split('\\')[0];
    return '';
  }

  /* Remove anything that could be a secret from a string that may have
     come from a driver error or a hint. The values in play are passed in so
     a message that echoes the whole string loses it. */
  function scrub(text, secrets) {
    var s = String(text == null ? '' : text);
    (secrets || []).forEach(function (sec) {
      if (sec && sec.length >= 4) s = s.split(sec).join('***');
    });
    s = s.replace(/((?:password|pwd|secret|fnkey|function key)\s*[=:]\s*)[^;&\s"']+/ig, '$1***');
    s = s.replace(/([?&]code=)[^&\s"']+/ig, '$1***');
    s = s.replace(/(:\/\/[^:\/@\s]+:)[^@\s]+@/g, '$1***@');
    return s;
  }

  /* "dbo.Ledger", "[dbo].[Ledger]", "\"fin\".\"ledger_entry\"", "Ledger" */
  function splitObject(name, defaultSchema) {
    var s = String(name || '').trim().replace(/[\[\]"`]/g, '');
    if (!s) return null;
    var parts = s.split('.');
    if (parts.length >= 2) return { schema: parts[parts.length - 2], name: parts[parts.length - 1] };
    return { schema: defaultSchema || 'dbo', name: parts[0] };
  }

  /* Where a connection attempt stopped, from the message db-connect sent.
     The stages are in order: a later stage means every earlier one passed,
     which is how tests 4, 5 and 6 share one call. */
  var STAGES = ['dns', 'timeout', 'refused', 'network', 'tls', 'config', 'session', 'rbac', 'login', 'database', 'ok'];
  function classifyConnError(msg) {
    var m = String(msg || '');
    if (/ENOTFOUND|getaddrinfo|EAI_AGAIN|could not resolve|no such host|Could not translate host name/i.test(m)) return 'dns';
    if (/ETIMEDOUT|ESOCKETTIMEDOUT|timed? ?out|timeout/i.test(m)) return 'timeout';
    if (/ECONNREFUSED|refused/i.test(m)) return 'refused';
    if (/SSL|TLS|certificate|self.signed|sslmode/i.test(m)) return 'tls';
    if (/Login failed|password authentication failed|28P01|28000|authentication failed|Cannot authenticate|no pg_hba\.conf entry/i.test(m)) return 'login';
    if (/Cannot open database|3D000|database "[^"]*" does not exist|does not exist/i.test(m)) return 'database';
    if (/ECONNRESET|EHOSTUNREACH|ENETUNREACH|Failed to connect|socket hang up|network/i.test(m)) return 'network';
    if (/Invalid connection string|connectionString is required|Entra authentication failed/i.test(m)) return 'config';
    if (/Auth error|Sign in and retry|Not signed in|Session expired|401/i.test(m)) return 'session';
    if (/Not permitted|Authorisation unavailable|403/i.test(m)) return 'rbac';
    return 'unknown';
  }
  function stageIndex(stage) { return STAGES.indexOf(stage); }

  /* The sentence for a failed connection, by stage. */
  function connAdvice(stage, port) {
    switch (stage) {
      case 'dns':     return 'Host not found: check the server name saved under Connections. It has to resolve from the internet, not only inside your network.';
      case 'timeout': return 'The server did not answer in time. This is usually a firewall: allow inbound connections on port ' + (port || 'the database port') + ' from Cygenix.';
      case 'refused': return 'The server refused the connection. Check the port, and that the database service is running and listening for remote connections.';
      case 'network': return 'The connection dropped part-way. Check the network path between Cygenix and the server, and any proxy or VPN in between.';
      case 'tls':     return 'The secure connection could not be set up. For SQL Server add trustServerCertificate=true, or install a trusted certificate; for PostgreSQL add sslmode=require.';
      case 'config':  return 'The saved connection could not be understood. Open it under Connections and rebuild it with the connection builder.';
      case 'session': return 'Your Cygenix sign-in has expired. Sign in again and re-run.';
      case 'rbac':    return 'Your Cygenix role does not allow this on this connection. A Platform Administrator can adjust roles on the Users & Roles page.';
      case 'login':   return 'Login failed: check the username and password saved for this connection under Connections.';
      case 'database':return 'The database was not found: check the database name saved under Connections, and that this login has access to it.';
      default:        return 'Open the connection under Connections and use Test there to see the full error.';
    }
  }

  function pingStatus(avgMs) {
    if (avgMs == null || isNaN(avgMs)) return 'fail';
    if (avgMs < PING_MS.good) return 'pass';
    if (avgMs <= PING_MS.slow) return 'warn';
    return 'fail';
  }

  /* { engine, version, serverVersionNum } → { status, major, min } */
  function versionStatus(engine, info) {
    var min = MIN_VERSIONS[engine];
    var major = null;
    if (engine === 'postgres') {
      if (info && info.serverVersionNum) major = Math.floor(Number(info.serverVersionNum) / 10000);
      else if (info && info.serverVersion) major = parseInt(String(info.serverVersion), 10);
      else if (info && info.version) major = parseInt(String(info.version).replace(/^PostgreSQL\s+/i, ''), 10);
    } else if (info && info.version) {
      major = parseInt(String(info.version), 10);
    }
    if (!min || major == null || isNaN(major)) return { status: 'warn', major: major, min: min || null, known: false };
    return { status: major < min.major ? 'warn' : 'pass', major: major, min: min, known: true };
  }

  /* Type families — the compatibility question is "can a value of the
     source family land in the target family without loss", not "are the
     type names equal". */
  function typeFamily(baseType) {
    var t = String(baseType || '').toLowerCase().replace(/\(.*$/, '').trim();
    if (!t) return 'unknown';
    if (/^(n?varchar|n?char|n?text|character varying|character|citext|string|json|jsonb|xml|sysname|enum)$/.test(t)) return 'string';
    if (/^(int|integer|bigint|smallint|tinyint|int2|int4|int8|serial|bigserial|smallserial)$/.test(t)) return 'integer';
    if (/^(decimal|numeric|money|smallmoney|float|real|double precision|double|float4|float8)$/.test(t)) return 'decimal';
    if (/^(date|datetime|datetime2|smalldatetime|datetimeoffset|time|timestamp|timestamp without time zone|timestamp with time zone|timestamptz|timetz|interval)$/.test(t)) return 'datetime';
    if (/^(binary|varbinary|image|bytea|rowversion|timestamp_binary)$/.test(t)) return 'binary';
    if (/^(bit|boolean|bool)$/.test(t)) return 'boolean';
    if (/^(uniqueidentifier|uuid)$/.test(t)) return 'uuid';
    return 'other';
  }
  var FAMILY_OK = {
    string:   { string: 1 },
    integer:  { integer: 1, decimal: 1, string: 1 },
    decimal:  { decimal: 1, string: 1 },
    datetime: { datetime: 1, string: 1 },
    binary:   { binary: 1 },
    boolean:  { boolean: 1, integer: 1, string: 1 },
    uuid:     { uuid: 1, string: 1 },
    other:    { other: 1, string: 1 },
    unknown:  { unknown: 1, string: 1, other: 1 },
  };
  var UNBOUNDED = /^(text|ntext|json|jsonb|xml|citext)$/i;

  /* One mapped pair → [] or a list of concerns, each a sentence fragment. */
  function typeCompat(src, tgt) {
    var out = [];
    if (!src || !tgt) return out;
    var sf = typeFamily(src.baseType || src.type), tf = typeFamily(tgt.baseType || tgt.type);
    if (!(FAMILY_OK[sf] || {})[tf]) {
      out.push('type ' + String(src.type || src.baseType) + ' to ' + String(tgt.type || tgt.baseType) + ' may not convert');
    }
    var sl = src.maxLength, tl = tgt.maxLength;
    var sUnb = UNBOUNDED.test(String(src.baseType || '')) || sl === -1;
    var tUnb = UNBOUNDED.test(String(tgt.baseType || '')) || tl === -1;
    if (sf === 'string' && tf === 'string' && !tUnb && tl != null && (sUnb || (sl != null && Number(sl) > Number(tl)))) {
      out.push('length ' + (sUnb ? 'MAX' : sl) + ' to ' + tl + ' may truncate');
    }
    if (sf === 'decimal' && tf === 'decimal' && src.precision != null && tgt.precision != null) {
      var sInt = Number(src.precision) - Number(src.scale || 0), tInt = Number(tgt.precision) - Number(tgt.scale || 0);
      if (sInt > tInt) out.push('precision ' + src.precision + ',' + (src.scale || 0) + ' to ' + tgt.precision + ',' + (tgt.scale || 0) + ' may overflow');
      else if (Number(src.scale || 0) > Number(tgt.scale || 0)) out.push('scale ' + (src.scale || 0) + ' to ' + (tgt.scale || 0) + ' will round');
    }
    if (sf === 'integer' && tf === 'integer') {
      var rank = { tinyint: 1, smallint: 2, int2: 2, int: 3, integer: 3, int4: 3, serial: 3, bigint: 4, int8: 4, bigserial: 4 };
      var sr = rank[String(src.baseType || '').toLowerCase()], tr = rank[String(tgt.baseType || '').toLowerCase()];
      if (sr && tr && sr > tr) out.push(String(src.type) + ' to ' + String(tgt.type) + ' may overflow');
    }
    if (src.nullable && tgt.nullable === false && (tgt['default'] == null || tgt['default'] === '') && !tgt.isIdentity) {
      out.push('source allows NULL but target does not and has no default');
    }
    return out;
  }

  function spaceStatus(space) {
    var concerns = [];
    var rows = 0, log = 0;
    (space && space.files || []).forEach(function (f) {
      var free = f.freeMb == null ? null : Number(f.freeMb);
      if (f.kind === 'LOG') log += free || 0; else rows += free || 0;
      if (free != null && free < LOW_SPACE_MB && !f.autogrow) {
        concerns.push(f.name + ' (' + (f.kind === 'LOG' ? 'log' : 'data') + ') has ' + fmtMb(free) + ' free and cannot grow');
      }
    });
    (space && space.volumes || []).forEach(function (v) {
      if (v.freeMb != null && Number(v.freeMb) < LOW_SPACE_MB) concerns.push('volume ' + v.mount + ' has ' + fmtMb(v.freeMb) + ' free');
    });
    return { status: concerns.length ? 'warn' : 'pass', rowsFreeMb: rows, logFreeMb: log, concerns: concerns };
  }

  function fmtMb(mb) {
    var n = Number(mb) || 0;
    if (n >= 1024 * 1024) return (n / 1024 / 1024).toFixed(1) + ' TB';
    if (n >= 1024) return (n / 1024).toFixed(1) + ' GB';
    return Math.round(n) + ' MB';
  }
  function fmtBytes(b) { return fmtMb((Number(b) || 0) / 1048576); }
  function fmtMs(ms) { return ms == null ? '-' : (Math.round(Number(ms)) + ' ms'); }
  function fmtWhen(iso) {
    try {
      var d = new Date(iso);
      return d.toLocaleDateString(undefined, { day: 'numeric', month: 'short', year: 'numeric' }) + ' ' +
             d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' });
    } catch (e) { return String(iso); }
  }
  function regionOfHost(host) {
    var m = /\.([a-z]+)-\d+\.azurewebsites\.net$/i.exec(String(host || ''));
    if (!m) return '';
    var r = m[1].toLowerCase();
    var names = { uksouth: 'UK South', ukwest: 'UK West', northeurope: 'North Europe', westeurope: 'West Europe',
      eastus: 'East US', westus: 'West US', australiaeast: 'Australia East' };
    return names[r] || r;
  }

  var STATUS_WORD = { none: 'Not run', running: 'Running', pass: 'Pass', warn: 'Warn', fail: 'Fail', skip: 'Skipped' };
  var RANK = { pass: 0, skip: 1, none: 1, warn: 2, fail: 3, running: 0 };
  function worst(list) {
    var w = 'pass';
    list.forEach(function (s) { if (RANK[s] > RANK[w]) w = s; });
    if (list.length && list.every(function (s) { return s === 'skip'; })) return 'skip';
    return w;
  }

  /* ── The report — plain text and JSON. Everything in it has been scrubbed. */
  function reportJson(state) {
    var out = {
      product: 'Cygenix diagnostics',
      generatedAt: state.lastRunAt ? new Date(state.lastRunAt).toISOString() : null,
      ranBy: state.user || null,
      profile: state.ctx ? { id: state.ctx.profile.id, name: state.ctx.profile.name, environment: state.ctx.profile.envClass || null } : null,
      source: state.ctx && state.ctx.conns.src ? { host: state.ctx.conns.src.host, engine: ENGINE_LABEL[state.ctx.conns.src.engine] } : null,
      target: state.ctx && state.ctx.conns.tgt ? { host: state.ctx.conns.tgt.host, engine: ENGINE_LABEL[state.ctx.conns.tgt.engine] } : null,
      runAgainst: state.side,
      results: [],
    };
    TESTS.forEach(function (t) {
      var r = state.results[t.id];
      if (!r) return;
      out.results.push({
        n: t.n, id: t.id, group: t.group, name: t.name, status: r.status, summary: r.summary || '',
        action: r.action || '', details: r.details || '',
        parts: (r.parts || []).map(function (p) {
          return { side: p.label, status: p.status, summary: p.summary || '', action: p.action || '', details: p.details || '' };
        }),
      });
    });
    return out;
  }

  function reportText(state) {
    var j = reportJson(state);
    var L = [];
    L.push('CYGENIX DIAGNOSTICS REPORT');
    L.push('Generated: ' + (j.generatedAt || '-') + (j.ranBy ? '   Ran by: ' + j.ranBy : ''));
    if (j.profile) L.push('Profile:   ' + j.profile.name + (j.profile.environment ? ' (' + j.profile.environment + ')' : ''));
    if (j.source) L.push('Source:    ' + (j.source.host || '-') + ' (' + j.source.engine + ')');
    if (j.target) L.push('Target:    ' + (j.target.host || '-') + ' (' + j.target.engine + ')');
    L.push('Run against: ' + ({ src: 'Source', tgt: 'Target', both: 'Source and target' }[j.runAgainst] || j.runAgainst));
    L.push('');
    GROUPS.forEach(function (g) {
      var rows = j.results.filter(function (r) { return r.group === g.id; });
      if (!rows.length) return;
      L.push('[' + g.label + ']');
      rows.forEach(function (r) {
        var head = (r.n < 10 ? ' ' : '') + r.n + '. ' + r.name;
        L.push(head + ' '.repeat(Math.max(1, 44 - head.length)) + STATUS_WORD[r.status].toUpperCase() + (r.summary ? '   ' + r.summary : ''));
        r.parts.forEach(function (p) {
          L.push('      ' + p.side + ': ' + STATUS_WORD[p.status].toUpperCase() + (p.summary ? ' — ' + p.summary : ''));
          if (p.action) L.push('        What to do: ' + p.action);
          if (p.details) L.push('        Details: ' + p.details.replace(/\n/g, '\n                 '));
        });
        if (!r.parts.length) {
          if (r.action) L.push('      What to do: ' + r.action);
          if (r.details) L.push('      Details: ' + r.details.replace(/\n/g, '\n               '));
        }
      });
      L.push('');
    });
    L.push('Every check is read-only. Test 12 creates a temporary table inside a transaction that is rolled back.');
    L.push('Server names are shown as host only. No credentials are included.');
    return L.join('\n');
  }

  /* ════════════════════════════════════════════════════════════════════════
     Everything below needs a browser.
     ════════════════════════════════════════════════════════════════════════ */
  var state = {
    mounted: false, running: false, lastRunStartedAt: 0, lastRunAt: null,
    selected: {}, side: 'both', profileId: null, results: {}, ctx: null, user: '', fmt: 'txt',
  };

  /* Whose selection this is. The dashboard's own helper reads the Entra
     account record; the two fallbacks are the keys the rest of the console
     writes at sign-in, so a browser that has one and not the other still
     files the selection under the person rather than under "_". */
  function userKey() {
    var v = '';
    try { if (typeof root.currentCygenixEmail === 'function') v = String(root.currentCygenixEmail() || ''); } catch (e) { v = ''; }
    if (!v) { try { v = String(localStorage.getItem('cygenix_active_user') || ''); } catch (e) { v = ''; } }
    if (!v) { try { var u = JSON.parse(localStorage.getItem('cygenix_user') || 'null'); v = String((u && (u.email || (u.user && u.user.email))) || ''); } catch (e) { v = ''; } }
    return v.trim().toLowerCase();
  }

  function loadPrefs() {
    var all = {};
    try { all = JSON.parse(localStorage.getItem(STORE_KEY) || '{}') || {}; } catch (e) { all = {}; }
    var mine = all[state.user || '_'] || null;
    if (mine && Array.isArray(mine.selected)) {
      state.selected = {};
      mine.selected.forEach(function (id) { if (BY_ID[id]) state.selected[id] = true; });
      state.side = ['src', 'tgt', 'both'].indexOf(mine.side) >= 0 ? mine.side : 'both';
      state.lastRunAt = mine.lastRunAt || null;
      state.profileId = mine.profileId || null;
      state.fmt = mine.fmt === 'json' ? 'json' : 'txt';
    } else {
      state.selected = {};
      TESTS.forEach(function (t) { state.selected[t.id] = true; });
    }
  }
  function savePrefs() {
    try {
      var all = JSON.parse(localStorage.getItem(STORE_KEY) || '{}') || {};
      all[state.user || '_'] = {
        selected: Object.keys(state.selected).filter(function (k) { return state.selected[k]; }),
        side: state.side, lastRunAt: state.lastRunAt, profileId: state.profileId, fmt: state.fmt,
      };
      localStorage.setItem(STORE_KEY, JSON.stringify(all));
    } catch (e) { /* storage full or blocked — the panel still works for this visit */ }
  }

  /* ── Profiles and connections ────────────────────────────────────────── */
  function profilesStore() {
    try { return root.CygenixProfiles && root.CygenixProfiles.cpLoad ? root.CygenixProfiles.cpLoad() : null; } catch (e) { return null; }
  }
  function savedConns() {
    try {
      if (typeof root.sconnGetAll === 'function') return root.sconnGetAll() || [];
      if (root.CygenixProfileApply && root.CygenixProfileApply.loadSavedConns) return root.CygenixProfileApply.loadSavedConns() || [];
    } catch (e) { /* fall through */ }
    return [];
  }
  function connValue(entry) {
    try { return root.CygenixProfiles && root.CygenixProfiles.cpConnValue ? root.CygenixProfiles.cpConnValue(entry) : (entry && entry.connString) || ''; }
    catch (e) { return ''; }
  }
  function describeConn(value, entry) {
    if (!value) return null;
    return { value: value, host: hostOf(value), engine: engineOf(value, entry), entry: entry || null,
             name: (entry && entry.name) || '' };
  }
  /* The list the selector shows: every profile, plus the plain project
     connections when no profile has been made yet. */
  function profileOptions() {
    var st = profilesStore();
    var list = [];
    if (st && st.profiles && st.profiles.length) {
      st.profiles.forEach(function (p) {
        list.push({ id: p.id, name: p.name || p.id, envClass: p.envClass || null, status: p.status || 'active', srcConnId: p.srcConnId, tgtConnId: p.tgtConnId });
      });
    }
    var amb = ambientConns();
    if (amb.src || amb.tgt) list.push({ id: '__ambient', name: 'Project connections (no profile)', envClass: null, status: 'active', ambient: amb });
    return { list: list, activeId: st && st.settings ? st.settings.activeProfileId : null };
  }
  function ambientConns() {
    var c = {};
    try { c = root.CygenixConnections && root.CygenixConnections.get ? (root.CygenixConnections.get() || {}) : {}; } catch (e) { c = {}; }
    // The key was never appended here, so the diagnostics connection test
    // answered 401 against a keyed Function App even when the product was
    // working — a test that fails when the thing under test is fine.
    var pick = function (fn, key, cs) {
      var A = root.CygenixActiveConn;
      var composed = (A && A.compose) ? A.compose(fn, key) : '';
      return composed || cs || '';
    };
    return { src: pick(c.srcFnUrl, c.srcFnKey, c.srcConnString), tgt: pick(c.tgtFnUrl, c.tgtFnKey, c.tgtConnString) };
  }
  function resolveProfile(id) {
    var opts = profileOptions();
    var p = opts.list.filter(function (x) { return x.id === id; })[0] || opts.list[0] || null;
    if (!p) return null;
    var conns = { src: null, tgt: null };
    if (p.ambient) {
      conns.src = describeConn(p.ambient.src, null);
      conns.tgt = describeConn(p.ambient.tgt, null);
    } else {
      var saved = savedConns();
      var byId = {};
      saved.forEach(function (s) { byId[s.id] = s; });
      var se = byId[p.srcConnId], te = byId[p.tgtConnId];
      conns.src = se ? describeConn(connValue(se), se) : null;
      conns.tgt = te ? describeConn(connValue(te), te) : null;
      conns.srcMissing = !se; conns.tgtMissing = !te;
    }
    return { profile: p, conns: conns };
  }

  /* Jobs bound to the profile, and the objects they map. */
  function mappedObjects(profile) {
    var jobs = [];
    try { jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]') || []; } catch (e) { jobs = []; }
    var st = profilesStore();
    var mine = jobs.filter(function (j) {
      if (!j || j.deleted || j.isDeleted) return false;
      if (!profile || profile.id === '__ambient') return true;
      try {
        var of = root.CygenixJobProfile && root.CygenixJobProfile.of ? root.CygenixJobProfile.of(j, st) : null;
        return !!(of && of.id === profile.id);
      } catch (e) { return false; }
    });
    var pairs = [];
    mine.forEach(function (j) {
      var s = splitObject(j.sourceTable || j.source, 'dbo');
      var t = splitObject(j.target || j.targetTable, 'dbo');
      if (!s || !t) return;
      pairs.push({ job: j.name || j.id, jobId: j.id, src: s, tgt: t,
        columns: (Array.isArray(j.columnMapping) ? j.columnMapping : []).filter(function (m) { return m && m.srcCol && m.tgtCol; })
          .map(function (m) { return { srcCol: String(m.srcCol), tgtCol: String(m.tgtCol) }; }) });
    });
    var uniq = function (list) {
      var seen = {}, out = [];
      list.forEach(function (o) { var k = (o.schema + '.' + o.name).toLowerCase(); if (!seen[k]) { seen[k] = 1; out.push(o); } });
      return out;
    };
    return { jobs: mine, pairs: pairs,
      srcTables: uniq(pairs.map(function (p) { return p.src; })).slice(0, MAX_TABLES_PER_PROBE),
      tgtTables: uniq(pairs.map(function (p) { return p.tgt; })).slice(0, MAX_TABLES_PER_PROBE) };
  }

  /* ── Network ─────────────────────────────────────────────────────────── */
  function withTimeout(promise, ms, label) {
    var timer;
    var t = new Promise(function (_, reject) {
      timer = setTimeout(function () { var e = new Error('Timed out'); e.timedOut = true; e.label = label; reject(e); }, ms);
    });
    return Promise.race([promise, t]).then(function (v) { clearTimeout(timer); return v; },
      function (e) { clearTimeout(timer); throw e; });
  }

  /* The same routing impDbCall uses — an https value is the Function App,
     a connection string goes to db-connect — with a 20s abort instead of
     impDbCall's 60s, and the hint kept on the error. The connection value is
     in the request body and nowhere else. */
  async function dbCall(conn, body) {
    var isFn = /^https?:\/\//i.test(conn.value);
    var f = typeof root.gfetch === 'function' ? root.gfetch : root.fetch.bind(root);
    var res;
    try {
      res = isFn
        ? await f(conn.value, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body), signal: AbortSignal.timeout(TEST_TIMEOUT_MS) })
        : await f('/.netlify/functions/db-connect', { method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(Object.assign({}, body, { connectionString: conn.value })), signal: AbortSignal.timeout(TEST_TIMEOUT_MS) });
    } catch (e) {
      var ne = new Error(e && e.name === 'TimeoutError' ? 'Timed out' : ('Network error: ' + (e && e.message || e)));
      ne.timedOut = e && e.name === 'TimeoutError';
      throw ne;
    }
    var data = await res.json().catch(function () { return { error: 'Non-JSON response' }; });
    if (!res.ok || data.success === false) {
      var err = new Error(data.error || data.errorMessage || data.message || res.statusText || ('HTTP ' + res.status));
      err.hint = data.hint || null; err.status = res.status;
      throw err;
    }
    return data;
  }

  /* The passwords inside a connection value, on their own. A driver that
     echoes the password bare ("Login failed ... the password is X") would
     slip past a scrubber that only knew the whole string. */
  function passwordsOf(value) {
    var v = String(value || ''), out = [];
    var m;
    var kv = /(?:^|;|\s)(?:password|pwd)\s*=\s*([^;\s]+)/ig;
    while ((m = kv.exec(v))) out.push(m[1]);
    var url = /^[a-z]+:\/\/[^:\/@\s]+:([^@\s]+)@/i.exec(v);
    if (url) { try { out.push(decodeURIComponent(url[1])); } catch (e) { /* keep raw */ } out.push(url[1]); }
    var code = /[?&]code=([^&\s]+)/i.exec(v);
    if (code) out.push(code[1]);
    return out;
  }
  function secretsOf(ctx) {
    var s = [];
    ['src', 'tgt'].forEach(function (k) {
      var c = ctx.conns[k];
      if (c && c.value) { s.push(c.value); s.push.apply(s, passwordsOf(c.value)); }
      if (c && c.entry && c.entry.fnKey) s.push(c.entry.fnKey);
      if (c && c.entry && c.entry.connString) { s.push(c.entry.connString); s.push.apply(s, passwordsOf(c.entry.connString)); }
    });
    // Longest first, so a value is removed before a fragment of it could
    // break the match.
    return s.filter(function (x) { return x && x.length >= 4; }).sort(function (a, b) { return b.length - a.length; });
  }
  function detailOf(e, ctx) {
    var msg = (e && e.message) || String(e);
    if (e && e.hint) msg += '\n' + e.hint;
    return scrub(msg, secretsOf(ctx));
  }

  /* ── The shared connection test (4, 5, 6, 17) ────────────────────────── */
  function connTest(ctx, side) {
    if (!ctx.cache.test[side]) {
      var c = ctx.conns[side];
      ctx.cache.test[side] = (async function () {
        if (!c) return { stage: 'missing', detail: '' };
        try {
          var d = await dbCall(c, { action: 'test' });
          return { stage: 'ok', version: d.version || '', database: d.database || '', user: d.user || '', detail: '' };
        } catch (e) {
          if (e.timedOut) return { stage: 'timeout', detail: 'Timed out after ' + (TEST_TIMEOUT_MS / 1000) + 's' };
          return { stage: classifyConnError(e.message), detail: detailOf(e, ctx) };
        }
      })();
    }
    return ctx.cache.test[side];
  }
  function portOf(c) {
    if (!c) return '';
    return c.engine === 'postgres' ? '5432' : c.engine === 'mssql' ? '1433' : '';
  }
  function missingPart(label, ctx, side) {
    var why = ctx.conns[side + 'Missing']
      ? 'The saved connection this profile references has been removed. Restore it under Connections or supersede the profile.'
      : 'No ' + (side === 'src' ? 'source' : 'target') + ' connection is saved for this profile.';
    return { label: label, status: 'skip', summary: 'No connection', action: why };
  }

  /* Per-side wrapper: runs fn for each side the test applies to, under a
     timeout each, and folds the parts into one result. */
  async function perSide(t, ctx, fn) {
    var sides = t.side === 'src' ? ['src'] : t.side === 'tgt' ? ['tgt'] : (state.side === 'both' ? ['src', 'tgt'] : [state.side]);
    var parts = [];
    for (var i = 0; i < sides.length; i++) {
      var side = sides[i], label = side === 'src' ? 'Source' : 'Target';
      if (!ctx.conns[side]) { parts.push(missingPart(label, ctx, side)); continue; }
      try {
        var p = await withTimeout(fn(side, ctx.conns[side]), TEST_TIMEOUT_MS * 1.5, t.id);
        p.label = label; parts.push(p);
      } catch (e) {
        parts.push({ label: label, status: 'fail',
          summary: e && e.timedOut ? 'Timed out' : 'Could not complete',
          action: e && e.timedOut ? 'The check did not finish within ' + (TEST_TIMEOUT_MS / 1000) + ' seconds. Check the connection tests (4 to 6) first.'
                                  : 'Open the connection under Connections and use Test there to see the full error.',
          details: detailOf(e, ctx) });
      }
    }
    return { status: worst(parts.map(function (p) { return p.status; })), parts: parts,
             summary: parts.length === 1 ? parts[0].summary : '' };
  }

  /* ── Test implementations ────────────────────────────────────────────── */
  var RUN = {};

  RUN.t01 = async function (ctx) {
    var out = [], status = 'pass';
    var t0 = performance.now();
    try {
      var r = await withTimeout(fetch('/api/health', { cache: 'no-store', signal: AbortSignal.timeout(TEST_TIMEOUT_MS) }), TEST_TIMEOUT_MS);
      var ms = performance.now() - t0;
      if (r.ok) out.push('Web service: ' + fmtMs(ms)); else { status = 'fail'; out.push('Web service: HTTP ' + r.status); }
    } catch (e) { status = 'fail'; out.push('Web service: ' + (e.timedOut || (e && e.name === 'TimeoutError') ? 'timed out' : 'unreachable')); }
    var t1 = performance.now();
    try {
      if (!root.CygenixDataApi || !root.CygenixDataApi.callResult) throw new Error('data api not loaded');
      var res = await withTimeout(root.CygenixDataApi.callResult('ping', { method: 'GET', signal: AbortSignal.timeout(TEST_TIMEOUT_MS) }), TEST_TIMEOUT_MS);
      var ms2 = performance.now() - t1;
      if (res.ok) out.push('Data service: ' + fmtMs(ms2));
      else { status = 'fail'; out.push('Data service: ' + (res.error && res.error.code === 'no-token' ? 'not signed in' : 'failed (' + (res.error && res.error.code) + ')')); }
    } catch (e) { status = 'fail'; out.push('Data service: ' + (e.timedOut ? 'timed out' : 'unreachable')); }
    return { status: status, summary: out.join(' · '),
      action: status === 'pass' ? '' : 'Check your internet connection, then status.cygenix.co.uk. If only the data service fails, sign out and in again.' };
  };

  RUN.t02 = async function (ctx) {
    var details = [];
    var token = '';
    try {
      token = typeof root.getCygenixIdTokenAsync === 'function' ? await withTimeout(root.getCygenixIdTokenAsync(), TEST_TIMEOUT_MS)
            : typeof root.getCygenixIdToken === 'function' ? root.getCygenixIdToken() : '';
    } catch (e) { token = ''; }
    if (!token) return { status: 'fail', summary: 'Not signed in', action: 'Your session has expired. Sign in again.' };
    var exp = null;
    try { exp = JSON.parse(atob(token.split('.')[1].replace(/-/g, '+').replace(/_/g, '/'))).exp; } catch (e) { exp = null; }
    if (exp) details.push('Session valid until ' + fmtWhen(exp * 1000));
    var who = null;
    try {
      var res = await withTimeout(root.CygenixDataApi.callResult('whoami', { method: 'GET', signal: AbortSignal.timeout(TEST_TIMEOUT_MS) }), TEST_TIMEOUT_MS);
      if (!res.ok) throw new Error(res.error && res.error.message || 'whoami failed');
      who = res.data || {};
    } catch (e) {
      return { status: 'fail', summary: 'Signed in, but the licence could not be read', action: 'The data service did not answer. Re-run test 1; if it passes, try again in a minute.', details: details.concat([detailOf(e, ctx)]).join('\n') };
    }
    var tier = who.tier || null, ts = who.tier_status || 'none';
    var tierLabel = tier;
    try { var tt = root.CygenixCapabilities && root.CygenixCapabilities.tier(tier); if (tt) tierLabel = tt.label; } catch (e) { /* keep id */ }
    var status = 'pass', action = '', summary;
    if (ts === 'active') summary = (tierLabel || 'Plan') + ' plan active';
    else if (ts === 'trialing') { status = 'warn'; summary = (tierLabel || 'Plan') + ' trial'; action = 'The trial ends ' + (who.trial_ends_at ? fmtWhen(who.trial_ends_at) : 'soon') + '. Choose a plan on the Billing page before then.'; }
    else if (!tier || ts === 'none') { status = 'fail'; summary = 'No plan'; action = 'No subscription is attached to this sign-in. Choose a plan on the Billing page.'; }
    else { status = 'fail'; summary = (tierLabel || 'Plan') + ' is ' + ts; action = 'The subscription is not active (' + ts + '). Check the payment method on the Billing page.'; }
    if (who.cancel_at_period_end) { if (status === 'pass') status = 'warn'; action = (action ? action + ' ' : '') + 'The plan is set to cancel on ' + (who.current_period_end ? fmtWhen(who.current_period_end) : 'the period end') + '.'; }
    // Features in use that the plan does not cover.
    var uses = featuresInUse();
    var uncovered = [];
    if (tier && root.CygenixCapabilities && root.CygenixCapabilities.tierAllows) {
      uses.forEach(function (u) {
        var a = root.CygenixCapabilities.tierAllows(tier, u.id);
        if (a && a.allowed === false && a.minTier) uncovered.push(u.label + ' (from the ' + a.minTier + ' plan)');
      });
    }
    if (uncovered.length) { if (status === 'pass') status = 'warn'; action = (action ? action + ' ' : '') + 'In use but not covered by this plan: ' + uncovered.join(', ') + '. Upgrade on the Billing page.'; }
    details.push('Signed in as ' + (who.email || state.user || '-'));
    details.push('Features detected in use: ' + (uses.length ? uses.map(function (u) { return u.label; }).join(', ') : 'none beyond migration'));
    return { status: status, summary: summary, action: action, details: details.join('\n') };
  };

  function featuresInUse() {
    var out = [];
    var has = function (prefix) { try { for (var i = 0; i < localStorage.length; i++) if (localStorage.key(i).indexOf(prefix) === 0) return true; } catch (e) { /* ignore */ } return false; };
    try {
      var jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]') || [];
      if (jobs.some(function (j) { return (j.columnMapping || []).some(function (m) { return m && (m.match || m.evidence); }); })) out.push({ id: 'ai_mapping', label: 'AI column mapping' });
    } catch (e) { /* ignore */ }
    if (has('cygenix_agentive')) out.push({ id: 'agentive', label: 'Agentive migration' });
    if (has('cygenix_datastream') || has('cygenix_stream')) out.push({ id: 'data_stream', label: 'Data Stream' });
    if (has('cygenix_server_migration') || has('cygenix_srvmig')) out.push({ id: 'server_objects', label: 'Server-level objects' });
    return out;
  }

  RUN.t03 = async function (ctx) {
    var details = ['Console and data: ' + HOSTING_REGION];
    var expects = [];
    try {
      var projects = JSON.parse(localStorage.getItem('cygenix_projects') || '[]') || [];
      var pid = localStorage.getItem('cygenix_active_project_id');
      var p = projects.filter(function (x) { return x && x.id === pid; })[0];
      var pr = p && (p.region || p.dataRegion || p.residency);
      if (pr) expects.push({ what: 'project "' + (p.name || p.id) + '"', region: String(pr) });
      else details.push('Project setting: none');
    } catch (e) { /* ignore */ }
    ['src', 'tgt'].forEach(function (k) {
      var c = ctx.conns[k];
      if (c && c.engine === 'azure') {
        var r = regionOfHost(c.host);
        if (r) expects.push({ what: (k === 'src' ? 'source' : 'target') + ' Function App', region: r });
      }
    });
    var norm = function (s) { return String(s || '').toLowerCase().replace(/[^a-z]/g, ''); };
    var mismatched = expects.filter(function (e) { return norm(e.region) !== norm(HOSTING_REGION); });
    expects.forEach(function (e) { details.push(e.what + ': ' + e.region); });
    if (mismatched.length) {
      return { status: 'warn', summary: HOSTING_REGION + ' — ' + mismatched.map(function (e) { return e.what + ' expects ' + e.region; }).join('; '),
        action: 'Your data is processed in ' + HOSTING_REGION + '. If the project requires another region, raise it with Cygenix support before migrating.', details: details.join('\n') };
    }
    return { status: 'pass', summary: HOSTING_REGION, details: details.join('\n') };
  };

  RUN.t04 = function (t, ctx) {
    return perSide(t, ctx, async function (side, c) {
      var r = await connTest(ctx, side);
      var reached = stageIndex(r.stage) >= stageIndex('login') || r.stage === 'ok';
      if (reached) return { status: 'pass', summary: 'Reached ' + c.host + (r.version ? ' · ' + r.version.split('\n')[0].slice(0, 60) : ''), details: '' };
      return { status: 'fail', summary: ({ dns: 'Host not found', timeout: 'Timed out', refused: 'Connection refused', network: 'Connection dropped', tls: 'Secure connection failed', config: 'Connection not understood', session: 'Cygenix session expired', rbac: 'Not permitted' })[r.stage] || 'Could not connect',
               action: connAdvice(r.stage, portOf(c)), details: r.detail };
    });
  };

  RUN.t05 = function (t, ctx) {
    return perSide(t, ctx, async function (side, c) {
      var r = await connTest(ctx, side);
      if (r.stage === 'ok' || r.stage === 'database') return { status: 'pass', summary: 'Signed in' + (r.user ? ' as ' + r.user : ''), details: '' };
      if (r.stage === 'login') return { status: 'fail', summary: 'Login failed', action: connAdvice('login'), details: r.detail };
      return { status: 'skip', summary: 'Server not reachable', action: 'Login was not attempted because the server could not be reached (test 4).', details: r.detail };
    });
  };

  RUN.t06 = function (t, ctx) {
    return perSide(t, ctx, async function (side, c) {
      var r = await connTest(ctx, side);
      if (r.stage === 'ok') return { status: 'pass', summary: 'Opened ' + (r.database || 'the database'), details: '' };
      if (r.stage === 'database') return { status: 'fail', summary: 'Database not found', action: connAdvice('database'), details: r.detail };
      return { status: 'skip', summary: r.stage === 'login' ? 'Login failed' : 'Server not reachable', action: 'The database was not opened because an earlier step failed (test ' + (r.stage === 'login' ? '5' : '4') + ').', details: r.detail };
    });
  };

  RUN.t07 = function (t, ctx) {
    return perSide(t, ctx, async function (side, c) {
      var avg, samples, note = '';
      if (c.engine === 'azure') {
        samples = [];
        for (var i = 0; i < 3; i++) { var t0 = performance.now(); await dbCall(c, { action: 'execute', sql: 'SELECT 1 AS one' }); samples.push(performance.now() - t0); }
        avg = samples.reduce(function (a, b) { return a + b; }, 0) / 3;
        note = ' (includes the hop to the Function App)';
      } else {
        var d = await dbCall(c, { action: 'diag-probe', probe: 'ping' });
        avg = d.avgMs; samples = d.samplesMs || [];
      }
      var st = pingStatus(avg);
      return { status: st, summary: 'Average ' + fmtMs(avg) + note,
        action: st === 'pass' ? '' : (st === 'warn' ? 'Responses are slow; each batch will take longer. Check latency between ' + HOSTING_REGION + ' and the server.' : 'Responses are very slow. Check the network path and the server’s load before running a large migration.'),
        details: 'Samples: ' + samples.map(function (s) { return fmtMs(s); }).join(', ') };
    });
  };

  RUN.t08 = function (t, ctx) {
    return perSide(t, ctx, async function (side, c) {
      var info, engine;
      if (c.engine === 'azure') {
        var d = await dbCall(c, { action: 'execute', sql: "SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)) AS version, CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) AS edition, (SELECT compatibility_level FROM sys.databases WHERE name = DB_NAME()) AS compatibilityLevel" });
        var row = (d.recordset || [])[0] || {};
        info = { version: row.version, edition: row.edition, compatibilityLevel: row.compatibilityLevel }; engine = 'mssql';
      } else {
        info = await dbCall(c, { action: 'diag-probe', probe: 'version' }); engine = info.engine || c.engine;
      }
      var v = versionStatus(engine, info);
      var summary = engine === 'postgres'
        ? 'PostgreSQL ' + (info.serverVersion || info.version || '?')
        : 'SQL Server ' + (info.version || '?') + (info.compatibilityLevel != null ? ' · compatibility level ' + info.compatibilityLevel : '') + (info.edition ? ' · ' + info.edition : '');
      return { status: v.status, summary: summary,
        action: v.status === 'pass' ? '' : (v.known ? 'Version ' + v.major + ' is below ' + v.min.label + ', the minimum Cygenix supports. Some features may not work; upgrade the server or expect to work around it.' : 'The version could not be read; check it by hand.'),
        details: '' };
    });
  };

  /* The Azure-direct permission check: a constant statement, names bound. */
  var AZ_PERMS_SQL = "SELECT CASE WHEN OBJECT_ID(QUOTENAME(@s) + '.' + QUOTENAME(@t)) IS NULL THEN 0 ELSE 1 END AS exists_, " +
    "HAS_PERMS_BY_NAME(QUOTENAME(@s) + '.' + QUOTENAME(@t), 'OBJECT', 'SELECT') AS [SELECT], " +
    "HAS_PERMS_BY_NAME(QUOTENAME(@s) + '.' + QUOTENAME(@t), 'OBJECT', 'INSERT') AS [INSERT], " +
    "HAS_PERMS_BY_NAME(QUOTENAME(@s) + '.' + QUOTENAME(@t), 'OBJECT', 'UPDATE') AS [UPDATE], " +
    "HAS_PERMS_BY_NAME(QUOTENAME(@s) + '.' + QUOTENAME(@t), 'OBJECT', 'DELETE') AS [DELETE]";
  async function permsOf(c, tables, probe) {
    if (c.engine !== 'azure') return dbCall(c, { action: 'diag-probe', probe: probe, tables: tables });
    var out = { tables: [], createTable: null, schemas: [] };
    for (var i = 0; i < tables.length; i++) {
      var t = tables[i];
      var d = await dbCall(c, { action: 'execute', sql: AZ_PERMS_SQL, params: [{ name: 's', value: t.schema }, { name: 't', value: t.name }] });
      var row = (d.recordset || [])[0] || {};
      var f = function (v) { return v == null ? null : Number(v) === 1; };
      out.tables.push({ schema: t.schema, name: t.name, exists: Number(row.exists_) === 1, select: f(row.SELECT), insert: f(row.INSERT), update: f(row.UPDATE), 'delete': f(row.DELETE) });
    }
    if (probe === 'write-access') {
      var ct = await dbCall(c, { action: 'execute', sql: "SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TABLE') AS ct" });
      out.createTable = Number(((ct.recordset || [])[0] || {}).ct) === 1;
    }
    return out;
  }
  function tname(t) { return t.schema + '.' + t.name; }

  RUN.t09 = function (t, ctx) {
    return perSide(t, ctx, async function (side, c) {
      var tables = ctx.mapped.srcTables;
      if (!tables.length) return { status: 'skip', summary: 'No objects are mapped for this profile', action: 'Map a source table in Object Mapping and bind the job to this profile.' };
      var d = await permsOf(c, tables, 'read-access');
      var missing = d.tables.filter(function (x) { return !x.exists; }), denied = d.tables.filter(function (x) { return x.exists && x.select !== true; });
      var details = d.tables.map(function (x) { return tname(x) + ': ' + (!x.exists ? 'not found' : x.select ? 'SELECT ok' : 'no SELECT'); }).join('\n');
      if (!missing.length && !denied.length) return { status: 'pass', summary: 'Can read all ' + d.tables.length + ' mapped source table' + (d.tables.length === 1 ? '' : 's'), details: details };
      var parts = [];
      if (denied.length) parts.push('no SELECT on ' + denied.map(tname).join(', '));
      if (missing.length) parts.push('not found: ' + missing.map(tname).join(', '));
      return { status: 'fail', summary: parts.join('; '),
        action: (denied.length ? 'Ask the database owner to GRANT SELECT on the listed tables to the source login. ' : '') + (missing.length ? 'The listed tables are not in the source database; see test 13.' : ''), details: details };
    });
  };

  RUN.t10 = function (t, ctx) {
    return perSide(t, ctx, async function (side, c) {
      var tables = ctx.mapped.tgtTables;
      if (!tables.length) return { status: 'skip', summary: 'No objects are mapped for this profile', action: 'Map a target table in Object Mapping and bind the job to this profile.' };
      var d = await permsOf(c, tables, 'write-access');
      var missing = d.tables.filter(function (x) { return !x.exists; });
      var denied = d.tables.filter(function (x) { return x.exists && !(x.insert && x.update && x['delete']); });
      var details = d.tables.map(function (x) {
        return tname(x) + ': ' + (!x.exists ? 'not found' : ['insert', 'update', 'delete'].map(function (p) { return p.toUpperCase() + ' ' + (x[p] ? 'ok' : 'NO'); }).join(', '));
      });
      details.push('CREATE TABLE: ' + (d.createTable === true ? 'ok' : d.createTable === false ? 'NO' : 'unknown'));
      (d.schemas || []).forEach(function (s) { details.push('Schema ' + s.schema + ': ' + (s.alter === true || s.create === true ? 'may create tables' : 'may not create tables')); });
      var status = 'pass', summary = 'Can write all ' + d.tables.length + ' mapped target table' + (d.tables.length === 1 ? '' : 's'), action = '';
      if (denied.length || missing.length) {
        status = 'fail';
        summary = (denied.length ? 'Missing write rights on ' + denied.map(tname).join(', ') : '') + (denied.length && missing.length ? '; ' : '') + (missing.length ? 'not found: ' + missing.map(tname).join(', ') : '');
        action = (denied.length ? 'Ask the database owner to GRANT INSERT, UPDATE, DELETE on the listed tables to the target login. ' : '') + (missing.length ? 'The listed tables are not in the target database; see test 13.' : '');
      }
      var noCreate = d.createTable === false || (d.schemas || []).some(function (s) { return s.alter === false || s.create === false; });
      if (noCreate) {
        if (status === 'pass') { status = 'warn'; summary += ', but cannot create tables'; }
        action += (action ? ' ' : '') + 'The target login cannot create tables, so staging tables and any new target table will fail. Grant CREATE TABLE (and ALTER on the schema) if the migration needs them.';
      }
      return { status: status, summary: summary, action: action, details: details.join('\n') };
    });
  };

  RUN.t11 = function (t, ctx) {
    return perSide(t, ctx, async function (side, c) {
      if (c.engine === 'postgres') return { status: 'skip', summary: 'Not supported on PostgreSQL', action: '' };
      if (c.engine === 'azure') return { status: 'skip', summary: 'Not available on Azure-direct connections', action: '' };
      var d = await dbCall(c, { action: 'diag-probe', probe: 'bulk' });
      if (d.supported === false) return { status: 'skip', summary: d.reason || 'Not supported', action: '' };
      var ok = d.serverPermission === true || d.bulkadmin === true || d.databasePermission === true;
      var details = 'ADMINISTER BULK OPERATIONS: ' + (d.serverPermission === true ? 'yes' : d.serverPermission === false ? 'no' : 'n/a') +
        '\nbulkadmin role: ' + (d.bulkadmin === true ? 'yes' : d.bulkadmin === false ? 'no' : 'n/a') +
        '\nADMINISTER DATABASE BULK OPERATIONS: ' + (d.databasePermission === true ? 'yes' : d.databasePermission === false ? 'no' : 'n/a');
      return ok ? { status: 'pass', summary: 'Bulk load permitted', details: details }
                : { status: 'warn', summary: 'Bulk load not permitted', action: 'Loads will fall back to batched inserts, which are slower. For a large migration, ask for ADMINISTER BULK OPERATIONS (or the bulkadmin role) on the target login.', details: details };
    });
  };

  RUN.t12 = function (t, ctx) {
    return perSide(t, ctx, async function (side, c) {
      if (c.engine === 'azure') return { status: 'skip', summary: 'Not available on Azure-direct connections', action: '' };
      var d = await dbCall(c, { action: 'diag-temp-table' });
      if (d.remainsAfterCleanup) return { status: 'fail', summary: 'A temporary table was left behind', action: 'The rollback did not remove the temporary table and neither did the cleanup. Ask the database owner to drop #cygenix_diag_probe and check the login’s transaction settings.', details: JSON.stringify(d) };
      if (d.remainsAfterRollback) return { status: 'warn', summary: 'Created, but the rollback did not remove it', action: 'The table was removed by the cleanup step instead. Check the login’s transaction settings; migrations rely on rollback to undo a failed batch.', details: JSON.stringify(d) };
      return { status: 'pass', summary: 'Created ' + (d.rows || 1) + ' row in a temporary table and rolled back; nothing left behind', details: 'remainsAfterRollback: false' };
    });
  };

  /* Live columns for a table, cached for tests 13, 14 and 16. */
  function liveColumns(ctx, side, obj) {
    var key = side + ':' + (obj.schema + '.' + obj.name).toLowerCase();
    if (!ctx.cache.cols[key]) {
      ctx.cache.cols[key] = (async function () {
        var c = ctx.conns[side];
        if (!c) return { missing: true, columns: [], error: 'no connection' };
        try {
          var d = await dbCall(c, { action: 'schema-columns', schemaName: obj.schema, tableName: obj.name });
          var cols = (d.table && d.table.columns) || [];
          return { missing: cols.length === 0, columns: cols };
        } catch (e) { return { missing: true, columns: [], error: detailOf(e, ctx) }; }
      })();
    }
    return ctx.cache.cols[key];
  }
  /* The overall budget for one test. Every network call is already cut off
     at TEST_TIMEOUT_MS by its own AbortSignal; this is the ceiling on the
     test as a whole, which for a two-sided test is two calls and for the
     schema checks is one call per mapped table per side. */
  function budgetFor(t, ctx) {
    if (t.id === 't13' || t.id === 't14' || t.id === 't16') return Math.min(120000, TEST_TIMEOUT_MS * Math.max(1, ctx.mapped.pairs.length * 2));
    if (t.side) return TEST_TIMEOUT_MS * 1.5 * 2 + 5000;
    return TEST_TIMEOUT_MS * 1.5;
  }

  RUN.t13 = async function (ctx) {
    var pairs = ctx.mapped.pairs;
    if (!pairs.length) return { status: 'skip', summary: 'No objects are mapped for this profile', action: 'Map a job in Object Mapping and bind it to this profile.' };
    if (!ctx.conns.src || !ctx.conns.tgt) return { status: 'skip', summary: 'Both connections are needed', action: 'This profile is missing a source or target connection.' };
    var missing = [], tables = 0, columns = 0;
    for (var i = 0; i < pairs.length; i++) {
      var p = pairs[i];
      var s = await liveColumns(ctx, 'src', p.src), t = await liveColumns(ctx, 'tgt', p.tgt);
      tables += 2;
      if (s.missing) missing.push('source table ' + tname(p.src) + (s.error ? ' (' + s.error.split('\n')[0] + ')' : ''));
      if (t.missing) missing.push('target table ' + tname(p.tgt) + (t.error ? ' (' + t.error.split('\n')[0] + ')' : ''));
      var sn = {}, tn = {};
      s.columns.forEach(function (c) { sn[String(c.name).toLowerCase()] = 1; });
      t.columns.forEach(function (c) { tn[String(c.name).toLowerCase()] = 1; });
      p.columns.forEach(function (m) {
        columns += 2;
        if (!s.missing && !sn[m.srcCol.toLowerCase()]) missing.push('source column ' + tname(p.src) + '.' + m.srcCol);
        if (!t.missing && !tn[m.tgtCol.toLowerCase()]) missing.push('target column ' + tname(p.tgt) + '.' + m.tgtCol);
      });
    }
    if (!missing.length) return { status: 'pass', summary: 'All ' + tables + ' tables and ' + columns + ' mapped columns are present', details: pairs.map(function (p) { return p.job + ': ' + tname(p.src) + ' to ' + tname(p.tgt) + ' (' + p.columns.length + ' columns)'; }).join('\n') };
    return { status: 'fail', summary: missing.length + ' mapped object' + (missing.length === 1 ? '' : 's') + ' no longer exist' + (missing.length === 1 ? 's' : ''),
      action: 'The schema has changed since the mapping was made. Open each job in Object Mapping and re-map the listed items, or restore them in the database.',
      details: missing.join('\n') };
  };

  RUN.t14 = async function (ctx) {
    var pairs = ctx.mapped.pairs;
    if (!pairs.length) return { status: 'skip', summary: 'No objects are mapped for this profile', action: '' };
    if (!ctx.conns.src || !ctx.conns.tgt) return { status: 'skip', summary: 'Both connections are needed', action: '' };
    var concerns = [], checked = 0, unread = 0;
    for (var i = 0; i < pairs.length; i++) {
      var p = pairs[i];
      var s = await liveColumns(ctx, 'src', p.src), t = await liveColumns(ctx, 'tgt', p.tgt);
      if (s.missing || t.missing) { unread++; continue; }
      var sBy = {}, tBy = {};
      s.columns.forEach(function (c) { sBy[String(c.name).toLowerCase()] = c; });
      t.columns.forEach(function (c) { tBy[String(c.name).toLowerCase()] = c; });
      p.columns.forEach(function (m) {
        var sc = sBy[m.srcCol.toLowerCase()], tc = tBy[m.tgtCol.toLowerCase()];
        if (!sc || !tc) return;
        checked++;
        typeCompat(sc, tc).forEach(function (why) { concerns.push(tname(p.src) + '.' + m.srcCol + ' to ' + tname(p.tgt) + '.' + m.tgtCol + ': ' + why); });
      });
    }
    if (!checked && unread) return { status: 'skip', summary: 'Columns could not be read', action: 'Run test 13 first; a table that is missing cannot be compared.' };
    if (!concerns.length) return { status: 'pass', summary: checked + ' column pair' + (checked === 1 ? '' : 's') + ' compatible' + (unread ? ' (' + unread + ' job' + (unread === 1 ? '' : 's') + ' not readable)' : ''), details: '' };
    return { status: 'warn', summary: concerns.length + ' column pair' + (concerns.length === 1 ? '' : 's') + ' may lose data',
      action: 'Widen the target column, add a transform in Object Mapping, or accept the truncation there. Preflight will stop on any row that does not fit.',
      details: concerns.join('\n') };
  };

  RUN.t15 = function (t, ctx) {
    return perSide(t, ctx, async function (side, c) {
      if (c.engine === 'azure') return { status: 'skip', summary: 'Not available on Azure-direct connections', action: '' };
      var d = await dbCall(c, { action: 'diag-probe', probe: 'space' });
      if (d.engine === 'postgres') return { status: 'skip', summary: 'Database size ' + fmtBytes(d.databaseBytes) + ' · free space is not exposed by PostgreSQL', action: '' };
      var s = spaceStatus(d);
      var details = (d.files || []).map(function (f) { return f.name + ' (' + (f.kind === 'LOG' ? 'log' : 'data') + '): ' + fmtMb(f.sizeMb) + ' allocated, ' + (f.freeMb == null ? '?' : fmtMb(f.freeMb)) + ' free' + (f.autogrow ? ', autogrow' : ', fixed') + (f.maxSizeMb ? ', max ' + fmtMb(f.maxSizeMb) : ''); });
      (d.volumes || []).forEach(function (v) { details.push('Volume ' + v.mount + ': ' + fmtMb(v.freeMb) + ' of ' + fmtMb(v.totalMb) + ' free'); });
      if (d.volumesUnavailable) details.push('Volume free space not readable (needs VIEW SERVER STATE).');
      return { status: s.status, summary: 'Data ' + fmtMb(s.rowsFreeMb) + ' free · log ' + fmtMb(s.logFreeMb) + ' free' + ((d.volumes || []).length ? ' · disk ' + fmtMb(d.volumes.reduce(function (a, v) { return a + (Number(v.freeMb) || 0); }, 0)) + ' free' : ''),
        action: s.status === 'pass' ? '' : 'Free space is low: ' + s.concerns.join('; ') + '. Grow the file or the disk before loading, or expect the load to stop part-way.',
        details: details.join('\n') };
    });
  };

  RUN.t16 = async function (ctx) {
    if (!ctx.conns.src || !ctx.conns.tgt) return { status: 'skip', summary: 'Both connections are needed', action: '' };
    if (ctx.conns.src.engine === 'azure' || ctx.conns.tgt.engine === 'azure') return { status: 'skip', summary: 'Not available on Azure-direct connections', action: '' };
    if (ctx.conns.src.engine !== ctx.conns.tgt.engine) return { status: 'skip', summary: 'Collations are not comparable across ' + ENGINE_LABEL[ctx.conns.src.engine] + ' and ' + ENGINE_LABEL[ctx.conns.tgt.engine], action: '' };
    var pairs = ctx.mapped.pairs;
    var s = await dbCall(ctx.conns.src, { action: 'diag-probe', probe: 'collation', tables: ctx.mapped.srcTables });
    var t = await dbCall(ctx.conns.tgt, { action: 'diag-probe', probe: 'collation', tables: ctx.mapped.tgtTables });
    var details = ['Source database: ' + (s.collation || '?'), 'Target database: ' + (t.collation || '?')];
    var diffs = [];
    var lookup = function (list, obj, col) {
      return (list || []).filter(function (x) { return x.schema.toLowerCase() === obj.schema.toLowerCase() && x.name.toLowerCase() === obj.name.toLowerCase() && String(x.column).toLowerCase() === col.toLowerCase(); })[0];
    };
    pairs.forEach(function (p) {
      p.columns.forEach(function (m) {
        var a = lookup(s.columns, p.src, m.srcCol), b = lookup(t.columns, p.tgt, m.tgtCol);
        if (a && b && a.collation && b.collation && a.collation !== b.collation) diffs.push(tname(p.src) + '.' + m.srcCol + ' (' + a.collation + ') to ' + tname(p.tgt) + '.' + m.tgtCol + ' (' + b.collation + ')');
      });
    });
    var dbDiff = s.collation && t.collation && s.collation !== t.collation;
    if (!dbDiff && !diffs.length) return { status: 'pass', summary: (s.collation || 'same collation') + ' on both', details: details.join('\n') };
    return { status: 'warn', summary: dbDiff ? 'Databases differ: ' + s.collation + ' vs ' + t.collation + (diffs.length ? ' · ' + diffs.length + ' column' + (diffs.length === 1 ? '' : 's') + ' differ' : '') : diffs.length + ' mapped column' + (diffs.length === 1 ? '' : 's') + ' differ in collation',
      action: 'Sorting and comparisons may differ after migration, and joins across the two may need COLLATE. Agree the target collation with the database owner before loading.',
      details: details.concat(diffs).join('\n') };
  };

  RUN.t17 = async function (ctx) {
    var schedules = [];
    try {
      if (typeof root.ta_sched !== 'function') throw new Error('scheduler not available on this page');
      var d = await withTimeout(root.ta_sched('list-schedules', {}), TEST_TIMEOUT_MS);
      schedules = (d && (d.schedules || d.items)) || [];
    } catch (e) {
      return { status: 'fail', summary: 'Schedules could not be read', action: 'The Task Agent did not answer. Open the Task Agent tab; if it loads there, re-run this check.', details: detailOf(e, ctx) };
    }
    var jobs = [];
    try { jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]') || []; } catch (e) { jobs = []; }
    var byJob = {};
    jobs.forEach(function (j) { if (j && j.id) byJob[j.id] = j; });
    var st = profilesStore();
    var ambient = ctx.profile.id === '__ambient';
    // A schedule whose job has gone cannot be attributed to any profile, and
    // it is broken whichever profile it belonged to — so it is always
    // reported rather than filtered out with the other profiles' schedules.
    var linked = schedules.filter(function (s) {
      if (!s || !s.jobId) return false;
      if (ambient) return true;
      var j = byJob[s.jobId];
      if (!j) return true;
      try { var of = root.CygenixJobProfile && root.CygenixJobProfile.of ? root.CygenixJobProfile.of(j, st) : null; return !!(of && of.id === ctx.profile.id); } catch (e) { return false; }
    });
    if (!linked.length) return { status: 'skip', summary: 'No schedules are linked to this profile', action: '' };
    var login = { src: await connTest(ctx, 'src'), tgt: await connTest(ctx, 'tgt') };
    var loginOk = function (r) { return r.stage === 'ok' || r.stage === 'database'; };
    // What the spec asks, and no more: the schedule's job exists, the
    // profile it names exists and is active, the connections that profile
    // references are still saved, and the logins passed (test 5). The
    // Profiles page's stricter "must be bound on the Profiles page" rule is
    // that page's to enforce; a job stamped with its profile at creation is
    // as linked as the scheduler needs.
    var problems = [];
    linked.forEach(function (s) {
      var name = s.name || s.id;
      var j = byJob[s.jobId];
      if (!j) { problems.push(name + ': job ' + s.jobId + ' no longer exists'); return; }
      if (!ambient) {
        if (ctx.profile.status && ctx.profile.status !== 'active') { problems.push(name + ': profile ' + ctx.profile.name + ' is ' + ctx.profile.status); return; }
        if (ctx.conns.srcMissing || ctx.conns.tgtMissing) { problems.push(name + ': a connection this profile references has been removed from the saved list'); return; }
      }
      if (!ctx.conns.src || !ctx.conns.tgt) { problems.push(name + ': the profile has no ' + (!ctx.conns.src ? 'source' : 'target') + ' connection'); return; }
      if (!loginOk(login.src)) problems.push(name + ': the source login failed (test 5)');
      if (!loginOk(login.tgt)) problems.push(name + ': the target login failed (test 5)');
    });
    if (!problems.length) return { status: 'pass', summary: linked.length + ' schedule' + (linked.length === 1 ? '' : 's') + ' checked', details: linked.map(function (s) { return (s.name || s.id) + (s.enabled === false ? ' (disabled)' : ''); }).join('\n') };
    return { status: 'fail', summary: problems.length + ' schedule' + (problems.length === 1 ? '' : 's') + ' will not run',
      action: 'Fix the connection or profile each schedule points at, or delete the schedule on the Task Agent tab.', details: problems.join('\n') };
  };

  /* ── Running ─────────────────────────────────────────────────────────── */
  function buildContext() {
    var r = resolveProfile(state.profileId);
    if (!r) return null;
    var ctx = { profile: r.profile, conns: r.conns, cache: { test: {}, cols: {} } };
    ctx.mapped = mappedObjects(r.profile);
    return ctx;
  }

  async function runOne(t, ctx) {
    var fn = RUN[t.id];
    var budget = budgetFor(t, ctx);
    try {
      var res = t.side ? await withTimeout(fn(t, ctx), budget, t.id) : await withTimeout(fn(ctx), budget, t.id);
      res.summary = scrub(res.summary || '', secretsOf(ctx));
      res.action = scrub(res.action || '', secretsOf(ctx));
      res.details = scrub(res.details || '', secretsOf(ctx));
      (res.parts || []).forEach(function (p) { p.summary = scrub(p.summary || '', secretsOf(ctx)); p.action = scrub(p.action || '', secretsOf(ctx)); p.details = scrub(p.details || '', secretsOf(ctx)); });
      return res;
    } catch (e) {
      var timed = e && e.timedOut;
      return { status: 'fail', summary: timed ? 'Timed out' : 'Could not complete',
        action: timed ? 'The check did not finish within its time limit. Check the connection tests (4 to 6) first.' : 'Something unexpected happened. The detail below is the message that came back.',
        details: detailOf(e, ctx) };
    }
  }

  async function run() {
    if (state.running) return false;
    var now = Date.now();
    if (now - state.lastRunStartedAt < MIN_RUN_INTERVAL_MS) return false;
    state.lastRunStartedAt = now;
    var ids = TESTS.filter(function (t) { return state.selected[t.id]; }).map(function (t) { return t.id; });
    if (!ids.length) return false;
    state.running = true;
    setRunButton();
    try {
      var ctx = buildContext();
      state.ctx = ctx;
      ids.forEach(function (id) { state.results[id] = { status: 'running' }; paintRow(id); });
      for (var i = 0; i < ids.length; i++) {
        var t = BY_ID[ids[i]];
        var needsConn = t.group !== 'platform';
        if (needsConn && !ctx) {
          state.results[t.id] = { status: 'skip', summary: 'No profile or connections', action: 'Save a source and target under Connections, or create a profile.' };
        } else {
          state.results[t.id] = await runOne(t, ctx || { profile: { id: '__none', name: '' }, conns: {}, cache: { test: {}, cols: {} }, mapped: { pairs: [], srcTables: [], tgtTables: [], jobs: [] } });
        }
        paintRow(t.id);
      }
      state.lastRunAt = Date.now();
      savePrefs();
      paintLastRun();
    } finally {
      // Cleared here and only here: the run's own completion, not a callback
      // any row repaint could reach.
      state.running = false;
      setRunButton();
    }
    return true;
  }

  /* ── Rendering ───────────────────────────────────────────────────────── */
  var CSS = [
    '#cyg-diag [hidden]{display:none !important}',
    '#cyg-diag .dg-head{margin-bottom:18px}',
    '#cyg-diag .dg-head .cx-head-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}',
    '#cyg-diag .dg-lastrun{font-size:13px;color:var(--color-neutral-600);margin-right:6px;white-space:nowrap}',
    '#cyg-diag .dg-fmt{height:32px;border:1px solid var(--color-neutral-300);background:var(--color-bg);color:var(--color-text);font:inherit;font-size:13px;padding:0 8px;border-radius:0}',
    '#cyg-diag .dg-bar{display:flex;align-items:center;flex-wrap:wrap;gap:10px 18px;padding:14px 18px;margin-bottom:22px}',
    '#cyg-diag .dg-bar label{font-family:var(--font-heading);font-size:12px;letter-spacing:0;color:var(--color-neutral-600);margin-right:6px}',
    '#cyg-diag .dg-bar select{height:32px;min-width:220px;border:1px solid var(--color-neutral-300);background:var(--color-bg);color:var(--color-text);font:inherit;font-size:14px;padding:0 8px;border-radius:0}',
    '#cyg-diag .dg-conn{font-size:13px;color:var(--color-text)}',
    '#cyg-diag .dg-conn .mono{font-family:var(--mono);font-size:12px}',
    '#cyg-diag .dg-conn .dim{color:var(--color-neutral-600)}',
    '#cyg-diag .dg-spacer{flex:1}',
    '#cyg-diag .dg-group{margin-bottom:22px}',
    '#cyg-diag .dg-ghead{display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid var(--color-divider)}',
    '#cyg-diag .dg-gtoggle{width:24px;height:24px;border:0;background:transparent;color:var(--color-neutral-700);cursor:pointer;font-size:14px;padding:0}',
    '#cyg-diag .dg-group.closed .dg-gtoggle{transform:rotate(-90deg)}',
    '#cyg-diag .dg-gsel{display:flex;align-items:center;gap:10px;cursor:pointer}',
    '#cyg-diag .dg-gsel .cx-h-sm{margin:0}',
    '#cyg-diag .dg-gnote{font-size:13px;color:var(--color-neutral-600)}',
    '#cyg-diag .dg-gcount{margin-left:auto;font-size:13px;color:var(--color-neutral-600);white-space:nowrap}',
    '#cyg-diag .dg-group.closed .dg-rows{display:none}',
    '#cyg-diag .dg-row{display:grid;grid-template-columns:28px 32px minmax(0,1fr) 150px;gap:0 10px;padding:12px 0;border-bottom:1px solid var(--color-divider);align-items:start}',
    '#cyg-diag .dg-row.off{opacity:.55}',
    '#cyg-diag .dg-row input[type=checkbox],#cyg-diag .dg-gsel input[type=checkbox]{width:16px;height:16px;margin:3px 0 0;accent-color:var(--color-accent-800)}',
    '#cyg-diag .dg-num{font-family:var(--font-heading);font-size:15px;color:var(--color-neutral-600);padding-top:1px}',
    '#cyg-diag .dg-name{font-family:var(--font-heading);font-size:17px;line-height:1.2;color:var(--color-text)}',
    '#cyg-diag .dg-desc{font-size:13px;line-height:1.45;color:var(--color-neutral-700);margin-top:2px}',
    '#cyg-diag .dg-out{margin-top:6px;font-size:13px;line-height:1.5}',
    '#cyg-diag .dg-out:empty{display:none}',
    '#cyg-diag .dg-part{display:flex;gap:8px;align-items:baseline;flex-wrap:wrap}',
    '#cyg-diag .dg-plabel{font-family:var(--font-heading);font-size:12px;letter-spacing:0;color:var(--color-neutral-600);min-width:52px}',
    '#cyg-diag .dg-summary{color:var(--color-text)}',
    '#cyg-diag .dg-action{margin-top:2px;color:var(--color-text)}',
    '#cyg-diag .dg-action b{font-weight:600}',
    '#cyg-diag details.dg-details{margin-top:2px}',
    '#cyg-diag details.dg-details summary{cursor:pointer;color:var(--color-neutral-600);font-size:12px;letter-spacing:.04em;font-family:var(--font-heading)}',
    '#cyg-diag details.dg-details pre{margin:4px 0 0;padding:8px 10px;background:var(--color-neutral-100);font-family:var(--mono);font-size:12px;line-height:1.5;white-space:pre-wrap;word-break:break-word;color:var(--color-text)}',
    '#cyg-diag .dg-status{display:flex;justify-content:flex-end;padding-top:1px}',
    '#cyg-diag .dg-st{display:inline-flex;align-items:center;gap:6px;font-family:var(--font-heading);font-size:12px;letter-spacing:0;color:var(--color-neutral-600);white-space:nowrap}',
    '#cyg-diag .dg-st::before{content:"";width:8px;height:8px;background:var(--color-neutral-400);flex:none}',
    '#cyg-diag .dg-st-pass{color:var(--state-ok)}#cyg-diag .dg-st-pass::before{background:var(--state-ok)}',
    '#cyg-diag .dg-st-warn{color:var(--state-warn)}#cyg-diag .dg-st-warn::before{background:var(--state-warn)}',
    '#cyg-diag .dg-st-fail{color:var(--state-fail)}#cyg-diag .dg-st-fail::before{background:var(--state-fail)}',
    '#cyg-diag .dg-st-skip{color:var(--color-neutral-600)}#cyg-diag .dg-st-skip::before{background:var(--color-neutral-400)}',
    '#cyg-diag .dg-st-running{color:var(--color-accent-800)}',
    '#cyg-diag .dg-st-running::before{width:10px;height:10px;border:2px solid var(--color-accent-300);border-top-color:var(--color-accent-800);border-radius:50%;background:transparent;animation:dg-spin .8s linear infinite}',
    '@keyframes dg-spin{to{transform:rotate(360deg)}}',
    '#cyg-diag .dg-note{font-size:13px;color:var(--color-neutral-600);margin-top:14px}',
    '#cyg-diag .cx-seg button{min-width:64px}',
    '@media (max-width:900px){#cyg-diag .dg-row{grid-template-columns:28px 28px minmax(0,1fr)}#cyg-diag .dg-status{grid-column:3;justify-content:flex-start;padding-top:4px}}',
  ].join('\n');

  function injectStyles() {
    if (document.getElementById('cyg-diag-css')) return;
    var s = document.createElement('style');
    s.id = 'cyg-diag-css';
    s.textContent = CSS;
    document.head.appendChild(s);
  }

  function shellHtml() {
    var h = '';
    h += '<div class="cx-head dg-head"><div>';
    h += '<div class="cx-kicker">Govern · Diagnostics</div>';
    h += '<h1 class="cx-title">Connection diagnostics</h1>';
    h += '<p class="cx-sub">Seventeen checks a migration depends on, from the service to the schema. Tick the ones you want and run them against a profile. Every check is read-only; the one temporary table is rolled back.</p>';
    h += '</div><div class="cx-head-actions">';
    h += '<span class="dg-lastrun" id="dg-lastrun">Not run yet</span>';
    h += '<button class="cx-btn" type="button" id="dg-copy" disabled>Copy report</button>';
    h += '<select class="dg-fmt" id="dg-fmt" aria-label="Report format"><option value="txt">.txt</option><option value="json">.json</option></select>';
    h += '<button class="cx-btn" type="button" id="dg-download" disabled>Download report</button>';
    h += '<button class="cx-btn cx-btn-primary" type="button" id="dg-run">Run selected (0)</button>';
    h += '</div></div>';

    h += '<div class="cx-blueprint dg-bar" id="dg-bar">';
    h += '<div><label for="dg-profile">Profile</label><select id="dg-profile"></select> <span class="cx-tag" id="dg-env" hidden></span></div>';
    h += '<div class="dg-conn" id="dg-src"></div><div class="dg-conn" id="dg-tgt"></div>';
    h += '<div><label>Run against</label><span class="cx-seg" id="dg-side" role="group" aria-label="Run against">';
    h += '<button type="button" data-side="src">Source</button><button type="button" data-side="tgt">Target</button><button type="button" data-side="both">Both</button></span></div>';
    h += '<span class="dg-spacer"></span>';
    h += '<div><button class="cx-btn cx-btn-sm" type="button" id="dg-all">Select all</button> <button class="cx-btn cx-btn-sm" type="button" id="dg-none">Clear</button></div>';
    h += '</div>';

    h += '<div id="dg-groups">';
    GROUPS.forEach(function (g) {
      // Not .cx-section: that class is the console's one-line section LABEL
      // (flex, uppercase, letter-spaced) and would style every row inside
      // it as a label. The group is a plain section with a heading row.
      h += '<section class="dg-group" data-group="' + g.id + '">';
      h += '<div class="dg-ghead"><button class="dg-gtoggle" type="button" aria-expanded="true" aria-label="Collapse ' + esc(g.label) + '">▾</button>';
      h += '<label class="dg-gsel"><input type="checkbox" data-group-sel="' + g.id + '" aria-label="Select every test in ' + esc(g.label) + '"><span class="cx-h-sm">' + esc(g.label) + '</span></label>';
      h += '<span class="dg-gnote">' + esc(g.note) + '</span>';
      h += '<span class="dg-gcount" id="dg-gcount-' + g.id + '"></span></div>';
      h += '<div class="dg-rows" role="list">';
      TESTS.filter(function (t) { return t.group === g.id; }).forEach(function (t) {
        h += '<div class="dg-row" data-test="' + t.id + '" role="listitem">';
        h += '<label class="dg-check"><input type="checkbox" data-test-sel="' + t.id + '" aria-label="Run test ' + t.n + ', ' + esc(t.name) + '"></label>';
        h += '<div class="dg-num">' + t.n + '</div>';
        h += '<div class="dg-main"><div class="dg-name">' + esc(t.name) + '</div><div class="dg-desc">' + esc(t.desc) + '</div><div class="dg-out" id="dg-out-' + t.id + '"></div></div>';
        h += '<div class="dg-status" id="dg-st-' + t.id + '"><span class="dg-st dg-st-none">Not run</span></div>';
        h += '</div>';
      });
      h += '</div></section>';
    });
    h += '</div>';
    h += '<p class="dg-note">Tests run one at a time, each with a ' + (TEST_TIMEOUT_MS / 1000) + '-second limit. Group 2 to 4 checks use the chosen profile’s saved connections; Azure-direct connections reach only the database the Function App is bound to, and skip the checks that need a direct login. Server names are shown as host only and the report never contains a credential.</p>';
    return h;
  }

  function chip(status) { return '<span class="dg-st dg-st-' + status + '">' + STATUS_WORD[status] + '</span>'; }
  function partHtml(p) {
    var h = '<div class="dg-part">' + chip(p.status) + (p.label ? '<span class="dg-plabel">' + esc(p.label) + '</span>' : '') + '<span class="dg-summary">' + esc(p.summary || '') + '</span></div>';
    if (p.action) h += '<div class="dg-action"><b>What to do:</b> ' + esc(p.action) + '</div>';
    if (p.details) h += '<details class="dg-details"><summary>Details</summary><pre>' + esc(p.details) + '</pre></details>';
    return h;
  }
  function paintRow(id) {
    var r = state.results[id];
    var st = document.getElementById('dg-st-' + id), out = document.getElementById('dg-out-' + id);
    if (!st || !out) return;
    if (!r) { st.innerHTML = chip('none'); out.innerHTML = ''; return; }
    st.innerHTML = chip(r.status);
    if (r.status === 'running') { out.innerHTML = ''; return; }
    out.innerHTML = r.parts && r.parts.length ? r.parts.map(partHtml).join('') : partHtml({ status: r.status, summary: r.summary, action: r.action, details: r.details });
  }
  function paintSelection() {
    var n = 0;
    TESTS.forEach(function (t) {
      var on = !!state.selected[t.id];
      if (on) n++;
      var cb = document.querySelector('#cyg-diag input[data-test-sel="' + t.id + '"]');
      if (cb) cb.checked = on;
      var row = document.querySelector('#cyg-diag .dg-row[data-test="' + t.id + '"]');
      if (row) row.classList.toggle('off', !on);
    });
    GROUPS.forEach(function (g) {
      var ts = TESTS.filter(function (t) { return t.group === g.id; });
      var on = ts.filter(function (t) { return state.selected[t.id]; }).length;
      var cb = document.querySelector('#cyg-diag input[data-group-sel="' + g.id + '"]');
      if (cb) { cb.checked = on === ts.length; cb.indeterminate = on > 0 && on < ts.length; }
      var c = document.getElementById('dg-gcount-' + g.id);
      if (c) c.textContent = on + ' of ' + ts.length + ' selected';
    });
    setRunButton(n);
  }
  function setRunButton(n) {
    var b = document.getElementById('dg-run');
    if (!b) return;
    if (n == null) n = TESTS.filter(function (t) { return state.selected[t.id]; }).length;
    b.textContent = state.running ? 'Running…' : 'Run selected (' + n + ')';
    b.disabled = state.running || n === 0;
    var has = Object.keys(state.results).some(function (k) { return state.results[k] && state.results[k].status !== 'running'; });
    ['dg-copy', 'dg-download'].forEach(function (id) { var e = document.getElementById(id); if (e) e.disabled = state.running || !has; });
  }
  function paintLastRun() {
    var e = document.getElementById('dg-lastrun');
    if (e) e.textContent = state.lastRunAt ? 'Last run ' + fmtWhen(state.lastRunAt) : 'Not run yet';
  }
  function paintSide() {
    document.querySelectorAll('#dg-side button').forEach(function (b) { b.classList.toggle('on', b.getAttribute('data-side') === state.side); });
  }
  function paintProfile() {
    var opts = profileOptions();
    var sel = document.getElementById('dg-profile');
    if (!sel) return;
    if (!opts.list.length) {
      sel.innerHTML = '<option value="">No connections saved</option>';
      state.profileId = null;
    } else {
      if (!state.profileId || !opts.list.some(function (p) { return p.id === state.profileId; })) state.profileId = opts.activeId && opts.list.some(function (p) { return p.id === opts.activeId; }) ? opts.activeId : opts.list[0].id;
      sel.innerHTML = opts.list.map(function (p) {
        return '<option value="' + esc(p.id) + '"' + (p.id === state.profileId ? ' selected' : '') + '>' + esc(p.name) + (p.envClass ? ' (' + esc(p.envClass) + ')' : '') + (p.status && p.status !== 'active' ? ' — ' + esc(p.status) : '') + '</option>';
      }).join('');
    }
    var r = state.profileId ? resolveProfile(state.profileId) : null;
    var env = document.getElementById('dg-env');
    if (env) {
      var ec = r && r.profile.envClass;
      env.hidden = !ec;
      env.textContent = ec === 'PRD' ? 'PROD' : (ec || '');
      env.className = 'cx-tag' + (ec === 'PRD' || ec === 'PROD' ? ' cx-tag-fail' : '');
    }
    var side = function (id, label, c, missing) {
      var e = document.getElementById(id);
      if (!e) return;
      if (!c) { e.innerHTML = '<span class="dim">' + label + ':</span> <span class="dim">' + (missing ? 'connection removed' : 'not set') + '</span>'; return; }
      e.innerHTML = '<span class="dim">' + label + ':</span> <span class="mono">' + esc(c.host || '(host unknown)') + '</span> <span class="dim">· ' + esc(ENGINE_LABEL[c.engine]) + '</span>';
    };
    side('dg-src', 'Source', r && r.conns.src, r && r.conns.srcMissing);
    side('dg-tgt', 'Target', r && r.conns.tgt, r && r.conns.tgtMissing);
  }

  function wire(rootEl) {
    rootEl.addEventListener('change', function (ev) {
      var t = ev.target;
      if (t.matches('input[data-test-sel]')) { state.selected[t.getAttribute('data-test-sel')] = t.checked; savePrefs(); paintSelection(); }
      else if (t.matches('input[data-group-sel]')) {
        var g = t.getAttribute('data-group-sel'), on = t.checked;
        TESTS.filter(function (x) { return x.group === g; }).forEach(function (x) { state.selected[x.id] = on; });
        savePrefs(); paintSelection();
      }
      else if (t.id === 'dg-profile') { state.profileId = t.value || null; state.results = {}; TESTS.forEach(function (x) { paintRow(x.id); }); savePrefs(); paintProfile(); setRunButton(); }
      else if (t.id === 'dg-fmt') { state.fmt = t.value === 'json' ? 'json' : 'txt'; savePrefs(); }
    });
    rootEl.addEventListener('click', function (ev) {
      var b = ev.target.closest('button');
      if (!b) return;
      if (b.id === 'dg-run') { run(); return; }
      if (b.id === 'dg-all') { TESTS.forEach(function (x) { state.selected[x.id] = true; }); savePrefs(); paintSelection(); return; }
      if (b.id === 'dg-none') { TESTS.forEach(function (x) { state.selected[x.id] = false; }); savePrefs(); paintSelection(); return; }
      if (b.id === 'dg-copy') { copyReport(); return; }
      if (b.id === 'dg-download') { downloadReport(); return; }
      if (b.classList.contains('dg-gtoggle')) {
        var sec = b.closest('.dg-group');
        var closed = sec.classList.toggle('closed');
        b.setAttribute('aria-expanded', closed ? 'false' : 'true');
        return;
      }
      if (b.hasAttribute('data-side')) { state.side = b.getAttribute('data-side'); savePrefs(); paintSide(); }
    });
  }

  function reportFor(fmt) { return fmt === 'json' ? JSON.stringify(reportJson(state), null, 2) : reportText(state); }
  function copyReport() {
    var text = reportText(state);
    var done = function () { var b = document.getElementById('dg-copy'); if (b) { b.textContent = 'Copied'; setTimeout(function () { b.textContent = 'Copy report'; }, 1600); } };
    if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(text).then(done, function () { fallbackCopy(text); done(); });
    else { fallbackCopy(text); done(); }
  }
  function fallbackCopy(text) {
    try {
      var ta = document.createElement('textarea');
      ta.value = text; ta.setAttribute('readonly', ''); ta.style.position = 'fixed'; ta.style.left = '-9999px';
      document.body.appendChild(ta); ta.select(); document.execCommand('copy'); document.body.removeChild(ta);
    } catch (e) { /* nothing more to try */ }
  }
  function downloadReport() {
    var fmt = state.fmt;
    var body = reportFor(fmt);
    var day = new Date(state.lastRunAt || Date.now()).toISOString().slice(0, 10);
    var blob = new Blob([body], { type: fmt === 'json' ? 'application/json' : 'text/plain;charset=utf-8' });
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'cygenix_diagnostics_' + day + '.' + fmt;
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 1000);
  }

  /* Called by showView('diagnostics'). Idempotent: the first call builds
     the panel, later calls refresh the profile list so a connection saved
     since is offered. */
  function init(mountId) {
    if (typeof document === 'undefined') return;
    var mount = document.getElementById(mountId || 'cyg-diag-mount');
    if (!mount) return;
    if (!state.mounted) {
      injectStyles();
      state.user = userKey();
      loadPrefs();
      mount.innerHTML = '<div id="cyg-diag">' + shellHtml() + '</div>';
      wire(mount);
      state.mounted = true;
      var fmt = document.getElementById('dg-fmt');
      if (fmt) fmt.value = state.fmt;
    }
    paintProfile();
    paintSelection();
    paintSide();
    paintLastRun();
  }

  return {
    init: init, run: run,
    TESTS: TESTS, GROUPS: GROUPS, MIN_VERSIONS: MIN_VERSIONS, PING_MS: PING_MS,
    TEST_TIMEOUT_MS: TEST_TIMEOUT_MS, MIN_RUN_INTERVAL_MS: MIN_RUN_INTERVAL_MS, LOW_SPACE_MB: LOW_SPACE_MB,
    STORE_KEY: STORE_KEY, HOSTING_REGION: HOSTING_REGION,
    engineOf: engineOf, hostOf: hostOf, scrub: scrub, splitObject: splitObject,
    classifyConnError: classifyConnError, connAdvice: connAdvice, pingStatus: pingStatus, versionStatus: versionStatus,
    typeFamily: typeFamily, typeCompat: typeCompat, spaceStatus: spaceStatus, worst: worst,
    reportText: reportText, reportJson: reportJson,
    _state: state,
  };
});
