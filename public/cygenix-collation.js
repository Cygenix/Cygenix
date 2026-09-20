/* ============================================================================
   cygenix-collation.js — collation matching for SQL Server migrations
   ----------------------------------------------------------------------------
   WHY THIS FILE EXISTS

   Two SQL Server databases can hold the same text and disagree about what it
   means. `Latin1_General_CI_AS` and `SQL_Latin1_General_CP1_CI_AS` sort
   differently; a case-sensitive source loaded into a case-insensitive target
   can collapse two distinct keys into one duplicate-key error; and a column
   whose code page differs from the target's turns every character outside
   that page into a literal question mark, silently, with no error and no row
   count to show for it. The last one is the worst kind of data loss there is:
   the migration reports success.

   Until now Cygenix handled this in exactly one place — the setup-difference
   query in dashboard-app.js pins COLLATE DATABASE_DEFAULT on both sides of
   its comparison, with a comment explaining the conflict error it was written
   to stop. Everywhere else, the collations were whatever the two databases
   happened to declare.

   This module makes it a setting rather than a surprise. Collations are
   detected from both databases, the differences are graded, and the choice of
   what to do about them is saved on the connection profile so that every
   module can ask the same question and get the same answer.

   SCOPE: SQL Server only. PostgreSQL has collations too, but they are a
   different model (per-database LC_COLLATE, per-column COLLATE with ICU or
   libc locales, no code pages) and mapping this vocabulary onto them would
   produce confident advice that is wrong. A non-SQL-Server connection gets
   one sentence saying so, and nothing else in the product changes.

   WHAT STAGE A DOES, AND WHAT IT DELIBERATELY DOES NOT

   Stage A detects, grades and saves. It changes no SQL anywhere. The settings
   it saves — the resolved collation, how temp tables are declared, whether a
   clash in generated SQL is fixed or merely reported, whether a case or code
   page risk blocks a job run — are read by nothing yet. That is on purpose:
   the settings have to exist and be trusted before anything acts on them, and
   a feature that started rewriting SQL on the same day it learned to read a
   collation name would be impossible to review.

   RULES THIS CODE KEEPS

   · The detection SQL is read-only and holds no caller input. The
     server-level query takes no parameters at all. The column-level query
     takes schema and table names, and they travel as BOUND PARAMETERS
     (@s0/@t0, @s1/@t1 …) — never concatenated into the statement text.

   · Nothing secret is ever written to the profile. The fingerprint that
     detects "you pointed this at a different database since" is the server
     host and the database name, both of which are already on screen.

   · The profile is read-modify-written. Only `collation` on the one profile
     is touched, and `updatedAt` is bumped so the sync merge (which picks the
     newer copy of a profile by id) keeps this change rather than an older
     machine's. The alternative — building a profile object and upserting it
     wholesale — is the shape of a data-loss bug this codebase has already
     paid for once.

   · Detect and Scan each have an in-flight guard and a three-second minimum
     interval, and the guard flag is cleared in the operation's own `finally`,
     never from a callback that the operation could itself re-trigger.

   · Every step is wrapped, and the real message is shown. A collation check
     that fails silently is worse than no collation check.

   CONNECTION VALUES ARE NOT TRUSTED (added in VERSION 2)

   A saved connection entry does not always hold a connection. One live
   profile had the word "API" in the field cpConnValue reads, and this card
   posted that word to the SQL driver: the card showed "Invalid connection
   string: Could not find server/host in connection string", which is a
   true sentence about the wrong problem. Three faults stacked up to make
   that happen, and all three are fixed here.

   · The value is checked against `looksLikeConnection` before it is used.
     An http(s) endpoint, a driver URL or a key=value string is a
     connection; a label word is not. Both forms matter — a source
     connection is frequently an mssql:// URL with no `server=` in it at
     all, so a validator built only for key=value strings would reject the
     connections that work.
   · The fallback to the ambient project connection used to run only when
     the profile value was empty. A junk value is not empty, so it blocked
     the fallback even where impGetConn was handing back a working URL.
     Now anything unusable is treated as nothing and the fallback runs.
   · engineOf used to end in `return 'mssql'`, so anything unrecognised was
     assumed to be SQL Server and dialled. It now returns no engine.

   AND ONE SIDE NEVER KILLS THE OTHER

   Detect ran both sides inside one try/catch, so a source that could not be
   reached aborted the run before the target was queried and both panels came
   back blank. Each side now succeeds or fails on its own and says which it
   was; an Azure Function App target is read even when the source is broken,
   and vice versa.
   ============================================================================ */
(function (root, factory) {
  'use strict';
  /* The rules ship twice — here and in azure-function/src/collation-rules.js
     — so the Task Agent decides the same way this screen does. Loading them
     as a separate script rather than inlining them is what makes that
     possible: the Function App is zipped from azure-function/ alone. */
  var Rules = (typeof module === 'object' && module.exports)
    ? require('./cygenix-collation-rules.js')
    : root.CygenixCollationRules;
  if (!Rules) throw new Error('cygenix-collation.js needs cygenix-collation-rules.js, which must load first.');
  var api = factory(root, Rules);
  if (typeof module === 'object' && module.exports) module.exports = api;
  root.cygCollation = api;
  root.CygenixCollation = api;          // the house naming, kept as an alias
})(typeof window !== 'undefined' ? window : globalThis, function (root, Rules) {
  'use strict';

  /* ── Constants a reader will want to find ────────────────────────────── */
  /* 2 — a saved connection value is validated before it is dialled, and
     each side detects independently of the other. See the CONNECTION
     VALUES note in the header. */
  var VERSION = 2;
  var UI_KEY = 'cygenix_collation_ui_v1';   // per-user card state (collapsed, filter)
  var MIN_RUN_INTERVAL_MS = 3000;           // Detect / Scan cannot fire more often
  var CALL_TIMEOUT_MS = 20000;              // per database call
  var TABLE_BATCH = 40;                     // tables per column-detection call
  var MAX_FINDING_ROWS = 500;               // rows drawn; the rest are counted

  /* The places in Cygenix where SQL crosses a collation boundary. Stage A
     uses this only to fill the "Where used" column, so a reader can see
     which parts of the product a finding actually bites. Stage B wires each
     one to the rules module; the ids are the contract between the two. */
  var CLASH_POINTS = [
    /* Consume an Object Mapping column pair, so a mapped-pair finding bites
       every one of them. Ordered by how directly they move the data. */
    { id: 'object-mapping', label: 'Object Mapping', kind: 'generates', where: 'browser', uses: 'mapping' },
    { id: 'validate',       label: 'Validate',       kind: 'generates', where: 'browser', uses: 'mapping' },
    { id: 'project-builder', label: 'Migration runs', kind: 'both',     where: 'browser', uses: 'mapping' },
    { id: 'task-agent',     label: 'Task Agent runs', kind: 'generates', where: 'azure',  uses: 'mapping' },
    { id: 'export-package', label: 'Export package', kind: 'generates', where: 'browser', uses: 'mapping' },
    { id: 'conversion-templates', label: 'Conversion Templates', kind: 'generates', where: 'browser', uses: 'mapping' },
    { id: 'preflight',      label: 'Preflight',      kind: 'generates', where: 'browser', uses: 'mapping' },
    /* Compare a source figure with a target figure. They pick their own
       columns rather than the mapping's, so a mapped-pair finding does not
       automatically apply; Stage B lints their generated SQL directly. */
    { id: 'balancing',      label: 'Balancing & Metrics', kind: 'generates', where: 'browser', uses: 'compare' },
    { id: 'assurance',      label: 'Assurance',      kind: 'both',      where: 'browser', uses: 'compare' },
    { id: 'nl-recon',       label: 'Reconciliation', kind: 'generates', where: 'browser', uses: 'compare' },
    /* Write to the target from their own column choices. */
    { id: 'data-import',    label: 'Data import',    kind: 'generates', where: 'browser', uses: 'target' },
    { id: 'data-generator', label: 'Data Generator', kind: 'generates', where: 'browser', uses: 'target' },
    { id: 'cleansing',      label: 'Cleansing & enrichment', kind: 'generates', where: 'browser', uses: 'target' },
    { id: 'server-migration', label: 'Linked servers', kind: 'generates', where: 'browser', uses: 'target' },
    /* Runs whatever the user wrote. */
    { id: 'sql-editor',     label: 'SQL editor',     kind: 'user',      where: 'browser', uses: 'any' },
  ];

  /* ── Pure helpers. Exported, so tests/collation.test.js can pin them
        without a browser. ─────────────────────────────────────────────── */

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  /* A collation name, read. SQL Server encodes the rules in the name:
       Latin1_General_CI_AS        case-insensitive, accent-sensitive
       Latin1_General_100_CS_AS_SC case-sensitive, version 100, supplementary
       Latin1_General_BIN2         binary, code-point order
       Latin1_General_100_CI_AS_SC_UTF8   UTF-8 encoded
       SQL_Latin1_General_CP1_CI_AS       the legacy SQL_ family
     BIN and BIN2 are case- AND accent-sensitive by definition and carry no
     _CS_/_AS_ tokens, which is why they are tested first. */
  /* The collation-name parser, the difference vocabulary and the saved
     shape all live in cygenix-collation-rules.js, which ships twice so the
     Function App runs the same bytes. They are re-exported below, so every
     caller that had them from this module still does. */
  var parseCollation = Rules.parseCollation;
  var collationDiff = Rules.collationDiff;
  var isSoftDiff = Rules.isSoftDiff;
  var isNonUnicodeText = Rules.isNonUnicodeText;
  var isTextType = Rules.isTextType;
  var resolvedCollation = Rules.resolvedCollation;
  var ISSUE = Rules.ISSUE;
  var defaults = Rules.defaults;
  var normalise = Rules.normalise;
  var sideDefaults = function () { return Rules.defaults().source; };

  /* server|database, and nothing else. This is what tells the card that the
     connection has been repointed since the collations were read. It must
     never carry a credential: a host and a database name are both already
     printed on the Connections page. */
  function hostOf(value) {
    var v = String(value || '').trim();
    if (!v) return '';
    try { if (/^https?:\/\//i.test(v)) return new URL(v).hostname; } catch (e) { return ''; }
    var m = /^(?:mssql|sqlserver|postgres|postgresql):\/\/(?:[^@\/]*@)?([^:\/?#,]+)/i.exec(v);
    if (m) return m[1];
    var kv = /(?:^|;|\s)(?:server|data source|address|addr|host)\s*=\s*(?:tcp:)?([^;,\s]+)/i.exec(v);
    if (kv) return kv[1].replace(/^tcp:/i, '').split('\\')[0];
    return '';
  }
  function databaseOf(value) {
    var v = String(value || '').trim();
    if (!v || /^https?:\/\//i.test(v)) return '';      // an Azure function URL hides it
    var m = /^(?:mssql|sqlserver|postgres|postgresql):\/\/[^\/]*\/([^?#]+)/i.exec(v);
    if (m) return decodeURIComponent(m[1]);
    var kv = /(?:^|;|\s)(?:database|initial catalog|dbname)\s*=\s*([^;,\s]+)/i.exec(v);
    return kv ? kv[1] : '';
  }
  function fingerprintOf(value) {
    var h = hostOf(value), d = databaseOf(value);
    return (h || d) ? (h + '|' + d) : '';
  }

  /* Is this string something the database layer could actually dial?

     This exists because a saved connection entry does not always hold a
     connection. impDbCall dispatches on the shape of the value — an http(s)
     URL goes to the Azure Function App, anything else is posted to
     db-connect as a connection string — and it has no way to tell a real
     connection string from a word. A profile whose entry held the label
     "API" sent that word to the SQL driver, which answered "Invalid
     connection string: Could not find server/host in connection string.",
     and the card printed a raw driver error for a problem that was really
     "this saved connection has nothing usable in it".

     The mssql:// branch carries its weight: a real source connection on
     this product is often an mssql:// URL with no semicolons and no
     server= at all, so a validator that only recognised key=value strings
     would reject the very connections that work. */
  function looksLikeConnection(value) {
    var v = String(value || '').trim();
    if (!v) return false;
    if (/^(https?|mssql|sqlserver|postgres|postgresql):\/\//i.test(v)) return true;
    if (/(^|;|\s)(server|data source|host|addr|address)\s*=/i.test(v)) return true;
    return false;
  }

  /* Which engine a saved connection value points at. An https URL is the
     Azure Function App, which only ever reaches the one database it is
     bound to — it is SQL Server, but not a database this card chose.

     The unusable-value check comes first and returns '' — no engine —
     rather than falling through to the mssql catch-all at the bottom.
     That catch-all is what let a label word be treated as a SQL Server
     connection and posted to the driver. */
  function engineOf(value) {
    var v = String(value || '').trim();
    if (!v) return '';
    if (!looksLikeConnection(v)) return '';
    if (/^https?:\/\//i.test(v)) return 'azure';
    if (/^(postgres|postgresql):\/\//i.test(v)) return 'postgres';
    if (/(^|;|\s)driver\s*=\s*postgres/i.test(v)) return 'postgres';
    if (/(^|\s)host\s*=/i.test(v) && /(^|\s)dbname\s*=/i.test(v)) return 'postgres';
    return 'mssql';
  }
  function isSqlServer(engine) { return engine === 'mssql' || engine === 'azure'; }

  /* "dbo.Ledger" / "[dbo].[Ledger]" / "Ledger" → { schema, name } */
  function splitObject(name, defaultSchema) {
    var s = String(name || '').trim().replace(/[\[\]"`]/g, '');
    if (!s) return null;
    var parts = s.split('.');
    if (parts.length >= 2) return { schema: parts[parts.length - 2], name: parts[parts.length - 1] };
    return { schema: defaultSchema || 'dbo', name: parts[0] };
  }
  function keyOf(o) { return (o.schema + '.' + o.name).toLowerCase(); }
  function colKey(schema, table, column) { return schema + '.' + table + '.' + column; }

  /* ── The grading. A pure function, because every severity rule in the
        brief is a claim that has to be testable without a database. ──────
     `pairs` is one entry per mapped column pair:
       { job, usedBy:[ids], src:{schema,table,column,dataType,collation,codePage,inUniqueKey,maxLength},
                            tgt:{ …same… } }
     Findings come back most serious first, each with a stable id so an
     acknowledgement survives a re-scan.                                   */
  var SEV_RANK = { high: 0, medium: 1, low: 2 };

  function buildFindings(input) {
    var model = input && input.model ? input.model : defaults();
    var pairs = (input && input.pairs) || [];
    var ack = {};
    (model.acknowledged || []).forEach(function (k) { ack[k] = true; });
    var resolved = input && input.resolved != null ? input.resolved : resolvedCollation(model);
    var out = [];

    var push = function (f) {
      f.acknowledged = !!ack[f.id];
      out.push(f);
    };

    pairs.forEach(function (p) {
      var s = p.src || {}, t = p.tgt || {};
      var object = colKey(t.schema, t.table, t.column);
      var srcObject = colKey(s.schema, s.table, s.column);
      var base = {
        object: object, sourceObject: srcObject,
        sourceCollation: s.collation || '', targetCollation: t.collation || '',
        usedBy: p.usedBy || [], job: p.job || '',
      };
      var ps = parseCollation(s.collation), pt = parseCollation(t.collation);
      var raised = false;

      /* HIGH — a non-Unicode target on a different code page. Characters the
         target's code page cannot represent become '?' with no error. */
      if (isNonUnicodeText(t.dataType) && s.codePage != null && t.codePage != null
          && Number(s.codePage) !== Number(t.codePage)) {
        push(Object.assign({}, base, {
          id: object + '|' + ISSUE.CODEPAGE, issueCode: ISSUE.CODEPAGE, severity: 'high',
          issue: 'Target ' + String(t.dataType).toLowerCase() + ' column uses code page ' + t.codePage
               + '; the source column uses ' + s.codePage + '. Characters outside the target code page are stored as "?".',
          recommendation: 'Change the target column to nvarchar, or give it a collation on code page ' + s.codePage + '.',
        }));
        raised = true;
      }

      /* HIGH — case-sensitive (or binary) source into a case-insensitive
         target, on a column the target treats as unique. Two source values
         that differ only in case become one value and the load fails on a
         duplicate key, part way through. */
      if (ps.known && pt.known && ps.cs === true && pt.cs === false && t.inUniqueKey) {
        push(Object.assign({}, base, {
          id: object + '|' + ISSUE.CASE_UNIQUE, issueCode: ISSUE.CASE_UNIQUE, severity: 'high',
          issue: 'Source is ' + (ps.bin ? 'binary' : 'case-sensitive') + ' and the target is case-insensitive, on a column in a primary key or unique index. Values differing only in case collide.',
          recommendation: 'Give the target column a case-sensitive collation, or de-duplicate the source values before loading.',
        }));
        raised = true;
      }

      /* MEDIUM — target is UTF-8 and the source is not, on a non-Unicode
         type. A UTF-8 varchar counts BYTES, so a 50-character value that
         fitted in varchar(50) can need more than 50 bytes and be cut. */
      if (isNonUnicodeText(t.dataType) && pt.utf8 && !ps.utf8) {
        push(Object.assign({}, base, {
          id: object + '|' + ISSUE.UTF8, issueCode: ISSUE.UTF8, severity: 'medium',
          issue: 'Target collation is UTF-8 and the source is not. A UTF-8 ' + String(t.dataType).toLowerCase()
               + ' counts bytes, so non-ASCII values can overflow ' + (t.maxLength > 0 ? t.maxLength + ' bytes' : 'the declared length') + '.',
          recommendation: 'Widen the target column, or change it to nvarchar.',
        }));
        raised = true;
      }

      if (raised) return;

      /* MEDIUM — the two collations simply differ. Comparing or joining them
         raises "Cannot resolve the collation conflict". LOW when the only
         difference is accent sensitivity or sort order, which changes
         ordering and matching but loses nothing. */
      var diff = collationDiff(s.collation, t.collation);
      if (diff.length) {
        var onlySoft = isSoftDiff(diff);
        push(Object.assign({}, base, {
          id: object + '|' + (onlySoft ? ISSUE.ACCENT : ISSUE.MISMATCH),
          issueCode: onlySoft ? ISSUE.ACCENT : ISSUE.MISMATCH,
          severity: onlySoft ? 'low' : 'medium',
          issue: onlySoft
            ? 'Collations differ in ' + diff.join(' and ') + '. Sorting and matching change; no data is lost.'
            : 'Mapped columns have different collations (' + diff.join(', ') + '). A comparison or join across them raises "Cannot resolve the collation conflict".',
          recommendation: onlySoft
            ? 'Accept, or apply COLLATE ' + (resolved || 'the resolved collation') + ' where order matters.'
            : 'Apply COLLATE ' + (resolved || 'the resolved collation') + ' to the comparison, which Cygenix can do for you.',
        }));
      }
    });

    /* MEDIUM — tempdb disagrees with the resolved collation. Every temp
       table Cygenix builds inherits tempdb's collation unless it is told
       otherwise, so a join between a temp table and a user table is the
       same conflict in a place nobody thinks to look. */
    var tempdb = (model.target && model.target.tempdbCollation) || '';
    if (tempdb && resolved && tempdb !== resolved) {
      push({
        id: 'tempdb|' + ISSUE.TEMPDB, issueCode: ISSUE.TEMPDB, severity: 'medium',
        object: 'tempdb', sourceObject: 'tempdb',
        sourceCollation: tempdb, targetCollation: resolved,
        issue: 'tempdb is ' + tempdb + ' and the resolved collation is ' + resolved + '. Temp tables inherit tempdb, so joins to them can raise a collation conflict.',
        recommendation: 'Set Temp tables to COLLATE DATABASE_DEFAULT, or leave it on the resolved collation so Cygenix declares it explicitly.',
        usedBy: ['migration-scripts', 'task-agent', 'balancing'], job: '',
      });
    }

    out.sort(function (a, b) {
      if (SEV_RANK[a.severity] !== SEV_RANK[b.severity]) return SEV_RANK[a.severity] - SEV_RANK[b.severity];
      return a.object < b.object ? -1 : a.object > b.object ? 1 : 0;
    });
    return out;
  }

  /* The chip counts only what has not been acknowledged. */
  function summarise(findings) {
    var s = { high: 0, medium: 0, low: 0, acknowledged: 0 };
    (findings || []).forEach(function (f) {
      if (f.acknowledged) { s.acknowledged++; return; }
      if (s[f.severity] != null) s[f.severity]++;
    });
    return s;
  }
  function chipOf(model, findings) {
    if (!model || !model.lastScan) return { key: 'none', word: 'Not checked' };
    var s = summarise(findings);
    if (s.high) return { key: 'high', word: 'High risk (' + s.high + ')' };
    var warn = s.medium + s.low;
    if (warn) return { key: 'warn', word: 'Warnings (' + warn + ')' };
    return { key: 'match', word: 'Match' };
  }

  /* The column-level detection query, with every name a bound parameter.
     Returns { sql, params } — exported so a test can prove no table name
     ever reaches the statement text. */
  function columnQuery(tables) {
    var list = (tables || []).filter(function (t) { return t && t.schema && t.name; });
    if (!list.length) return null;
    var preds = [], params = [];
    list.forEach(function (t, i) {
      preds.push('(s.name = @s' + i + ' AND t.name = @t' + i + ')');
      params.push({ name: 's' + i, value: String(t.schema) });
      params.push({ name: 't' + i, value: String(t.name) });
    });
    var sql =
      'SELECT s.name AS schema_name, t.name AS table_name, c.name AS column_name,\n' +
      '       ty.name AS data_type, c.max_length, c.collation_name,\n' +
      "       COLLATIONPROPERTY(c.collation_name,'CodePage') AS code_page,\n" +
      '       CASE WHEN EXISTS (\n' +
      '         SELECT 1 FROM sys.index_columns ic\n' +
      '         JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id\n' +
      '         WHERE ic.object_id = c.object_id AND ic.column_id = c.column_id\n' +
      '           AND (i.is_unique = 1 OR i.is_primary_key = 1)) THEN 1 ELSE 0 END AS in_unique_key\n' +
      'FROM sys.columns c\n' +
      'JOIN sys.tables  t  ON t.object_id = c.object_id\n' +
      'JOIN sys.schemas s  ON s.schema_id = t.schema_id\n' +
      'JOIN sys.types   ty ON ty.user_type_id = c.user_type_id\n' +
      'WHERE c.collation_name IS NOT NULL\n' +
      '  AND (' + preds.join('\n    OR ') + ')';
    return { sql: sql, params: params };
  }

  /* The server-level query. No parameters at all, which is the simplest
     possible proof that nothing caller-supplied reaches it. */
  var SERVER_QUERY =
    'SELECT\n' +
    "  CAST(SERVERPROPERTY('Collation') AS nvarchar(128))               AS server_collation,\n" +
    "  CAST(DATABASEPROPERTYEX(DB_NAME(),'Collation') AS nvarchar(128)) AS db_collation,\n" +
    "  (SELECT collation_name FROM sys.databases WHERE name = 'tempdb') AS tempdb_collation,\n" +
    "  COLLATIONPROPERTY(CAST(DATABASEPROPERTYEX(DB_NAME(),'Collation') AS nvarchar(128)),'CodePage') AS code_page,\n" +
    '  DB_NAME() AS database_name,\n' +
    "  CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(64)) AS product_version;";

  var VALID_COLLATIONS_QUERY = 'SELECT name FROM sys.fn_helpcollations();';

  /* ════════════════════════════════════════════════════════════════════════
     Everything below needs a browser.
     ════════════════════════════════════════════════════════════════════════ */
  var state = {
    mounted: false, open: false, user: '', profileId: null,
    model: defaults(), findings: [],
    live: { src: '', tgt: '' }, engines: { src: '', tgt: '' }, conns: { src: '', tgt: '' },
    detecting: false, scanning: false, saving: false,
    lastDetectAt: 0, lastScanAt: 0,
    dirty: false, error: '', note: '', filter: 'all',
    validCollations: null,
  };

  function userKey() {
    var v = '';
    try { if (typeof root.currentCygenixEmail === 'function') v = String(root.currentCygenixEmail() || ''); } catch (e) { v = ''; }
    if (!v) { try { v = String(localStorage.getItem('cygenix_active_user') || ''); } catch (e) { v = ''; } }
    return v.trim().toLowerCase();
  }
  function uiRead() {
    try {
      var all = JSON.parse(localStorage.getItem(UI_KEY) || '{}') || {};
      return all[state.user || '_'] || {};
    } catch (e) { return {}; }
  }
  function uiWrite(patch) {
    try {
      var all = JSON.parse(localStorage.getItem(UI_KEY) || '{}') || {};
      all[state.user || '_'] = Object.assign({}, all[state.user || '_'] || {}, patch);
      localStorage.setItem(UI_KEY, JSON.stringify(all));
    } catch (e) { /* blocked or full — the card still works for this visit */ }
  }

  /* ── Profile access. Read-modify-write, never a wholesale upsert. ────── */
  /* The profile store. cygenix-profiles.js is the canonical reader, but it is
     loaded on the Connections page and on almost none of the pages Stage B
     wires — Object Mapping, the SQL editor, Project Builder, Balancing and
     Assurance all lack it. Depending on it alone made settings() return null
     on every one of them, so the whole feature reported nothing, silently,
     on the screens it exists for. That is the exact failure this module's
     header says it will not have.

     So: use the module when it is there, and otherwise read the one key it
     would have read. cpLoad() does the same JSON.parse behind the same
     try/catch; nothing is interpreted differently, and no page has to grow a
     script tag to be told about a collation clash. */
  var STORE_KEY = 'cygenix_profiles_v1';
  function store() {
    try {
      if (root.CygenixProfiles && root.CygenixProfiles.cpLoad) return root.CygenixProfiles.cpLoad();
    } catch (e) { /* fall through to the raw read */ }
    try {
      var raw = localStorage.getItem(STORE_KEY);
      if (!raw) return null;
      var parsed = JSON.parse(raw);
      return (parsed && typeof parsed === 'object') ? parsed : null;
    } catch (e) { return null; }
  }
  function activeProfile(st) {
    st = st || store();
    if (!st || !st.profiles) return null;
    var id = st.settings && st.settings.activeProfileId;
    var byId = function (x) { return st.profiles.filter(function (p) { return p.id === x; })[0] || null; };
    return (id && byId(id)) || st.profiles.filter(function (p) { return p.status === 'active'; })[0] || st.profiles[0] || null;
  }
  function savedConns() {
    try {
      if (typeof root.sconnGetAll === 'function') return root.sconnGetAll() || [];
      if (root.CygenixProfileApply && root.CygenixProfileApply.loadSavedConns) return root.CygenixProfileApply.loadSavedConns() || [];
    } catch (e) { /* fall through */ }
    return [];
  }
  /* The two connection values this profile resolves to. Falls back to the
     ambient project connections when no profile has been made yet, so the
     card works before anybody has adopted profiles.

     The profile value is no longer taken on trust. cpConnValue returns
     entry.connString for anything that is not an Azure entry, and a
     save-path bug elsewhere can put a label into that field; the first
     version of this function handed whatever came back straight to the SQL
     driver. Worse, it only fell back to the ambient connection when the
     profile value was FALSY — so a junk word, being truthy, blocked the
     fallback even on a page where impGetConn('src') was returning a
     perfectly good mssql:// URL.

     So: a value that is not dialable is rejected rather than used, the
     ambient fallback runs whenever the profile gave us nothing usable, and
     the card is told which of those happened. The two extra flags per side
     are additive — refresh() still reads .src and .tgt and nothing else. */
  function connectionsFor(profile) {
    var out = { src: '', tgt: '',
                srcFallback: false, tgtFallback: false,
                srcRejected: false, tgtRejected: false };
    var P = root.CygenixProfiles;
    if (profile && P && P.cpConnValue) {
      var byId = {};
      savedConns().forEach(function (c) { byId[c.id] = c; });
      ['src', 'tgt'].forEach(function (side) {
        var id = side === 'src' ? profile.srcConnId : profile.tgtConnId;
        if (!byId[id]) return;
        var v = '';
        try { v = P.cpConnValue(byId[id]) || ''; } catch (e) { v = ''; }
        if (looksLikeConnection(v)) out[side] = v;
        else if (String(v).trim()) out[side + 'Rejected'] = true;
      });
    }
    ['src', 'tgt'].forEach(function (side) {
      if (out[side] || typeof root.impGetConn !== 'function') return;
      var v = '';
      try { v = root.impGetConn(side) || ''; } catch (e) { v = ''; }
      if (looksLikeConnection(v)) { out[side] = v; out[side + 'Fallback'] = true; }
      else if (String(v).trim()) out[side + 'Rejected'] = true;
    });
    return out;
  }

  /* Write the collation object back onto ONE profile and save the store.
     Everything else in the store, and every other field on that profile, is
     left exactly as it was. updatedAt is bumped so the sync merge — which
     picks the newer copy of a profile by id — keeps this version. */
  function saveToProfile(model) {
    var P = root.CygenixProfiles;
    if (!P || !P.cpLoad || !P.cpSave) throw new Error('Connection profiles are not loaded on this page.');
    var st = P.cpLoad();
    var id = state.profileId;
    var p = (st.profiles || []).filter(function (x) { return x.id === id; })[0];
    if (!p) throw new Error('No connection profile is selected. Create or select one on Profiles & integrations.');
    p.collation = JSON.parse(JSON.stringify(model));
    p.updatedAt = Date.now();
    if (P.cpEvent) { try { P.cpEvent(st, { type: 'collation.saved', profileId: id, by: state.user }, p.updatedAt); } catch (e) { /* event log is best effort */ } }
    P.cpSave(st);
    return p;
  }

  /* ── Network. One call, one guard, the real message on failure. ─────── */
  function withTimeout(promise, ms) {
    var timer;
    var t = new Promise(function (_, reject) {
      timer = setTimeout(function () { var e = new Error('Timed out after ' + Math.round(ms / 1000) + 's'); e.timedOut = true; reject(e); }, ms);
    });
    return Promise.race([promise, t]).then(
      function (v) { clearTimeout(timer); return v; },
      function (e) { clearTimeout(timer); throw e; });
  }
  async function runQuery(conn, sql, params) {
    if (!conn) throw new Error('No connection is configured for this side.');
    if (typeof root.impDbCall !== 'function') throw new Error('The database call helper is not loaded on this page.');
    var body = { action: 'execute', sql: sql };
    if (params && params.length) body.params = params;
    var data = await withTimeout(root.impDbCall(conn, body), CALL_TIMEOUT_MS);
    return (data && data.recordset) || [];
  }

  /* ── Detect ──────────────────────────────────────────────────────────── */
  async function detect() {
    if (state.detecting) return false;
    var now = Date.now();
    if (now - state.lastDetectAt < MIN_RUN_INTERVAL_MS) return false;
    state.lastDetectAt = now;
    state.detecting = true;
    state.error = ''; state.note = '';
    render();
    try {
      var sides = ['src', 'tgt'];
      /* Each side is attempted on its own and reports its own reason. One
         try/catch around the whole loop meant a source that could not be
         dialled aborted the run before the target was ever queried, and
         both panels came back blank for a fault in one of them. The
         failures are collected rather than thrown so the side that works
         still gets read and still gets drawn. */
      var errs = [];
      for (var i = 0; i < sides.length; i++) {
        var side = sides[i];
        var conn = state.conns[side];
        var field = side === 'src' ? 'source' : 'target';
        var label = side === 'src' ? 'Source' : 'Target';
        if (!conn) {
          state.model[field] = sideDefaults();
          errs.push(label + ': ' + (state.conns[side + 'Rejected']
            ? 'the connection saved on this profile is not a usable connection string or endpoint.'
            : 'no connection is configured.'));
          continue;
        }
        if (!isSqlServer(state.engines[side])) {
          state.model[field] = sideDefaults();
          errs.push(label + ': collation matching supports SQL Server connections only.');
          continue;
        }
        try {
          var rows = await runQuery(conn, SERVER_QUERY);
          var r = rows[0] || {};
          var pc = parseCollation(r.db_collation);
          state.model[field] = {
            server: hostOf(conn),
            database: r.database_name || databaseOf(conn),
            serverCollation: r.server_collation || '',
            dbCollation: r.db_collation || '',
            tempdbCollation: r.tempdb_collation || '',
            codePage: r.code_page == null ? null : Number(r.code_page),
            cs: pc.cs, as: pc.accent, utf8: pc.utf8,
            productVersion: r.product_version || '',
            detectedAt: new Date().toISOString(),
          };
          state.model.fingerprint[field] = fingerprintOf(conn);
        } catch (e) {
          state.model[field] = sideDefaults();
          errs.push(label + ': ' + ((e && e.message) || String(e)));
        }
      }
      state.model.resolvedCollation = resolvedCollation(state.model);
      state.dirty = true;
      state.error = errs.join('  |  ');
      state.note = errs.length
        ? ''
        : 'Collations detected. Run Scan to grade the mapped columns, then Save to profile.';
    } catch (e) {
      state.error = (e && e.message) || String(e);
    } finally {
      // Cleared here and only here — never from a callback the run could
      // itself re-enter.
      state.detecting = false;
      render();
    }
    return true;
  }

  /* ── Scan ─────────────────────────────────────────────────────────────
     Scope: every column used in Object Mapping for this profile. Table
     lists are batched so one call never approaches the 26s lambda cap. */
  function mappedPairs() {
    var jobs = [];
    try { jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]') || []; } catch (e) { jobs = []; }
    var st = store();
    var pid = state.profileId;
    var mine = jobs.filter(function (j) {
      if (!j || j.deleted || j.isDeleted) return false;
      if (!pid) return true;
      try {
        var of = root.CygenixJobProfile && root.CygenixJobProfile.of ? root.CygenixJobProfile.of(j, st) : null;
        return !!(of && of.id === pid);
      } catch (e) { return false; }
    });
    var out = [];
    mine.forEach(function (j) {
      var s = splitObject(j.sourceTable || j.source, 'dbo');
      var t = splitObject(j.target || j.targetTable, 'dbo');
      if (!s || !t) return;
      (Array.isArray(j.columnMapping) ? j.columnMapping : []).forEach(function (m) {
        if (!m || !m.srcCol || !m.tgtCol) return;
        out.push({ job: j.name || j.id,
          src: { schema: s.schema, table: s.name, column: String(m.srcCol) },
          tgt: { schema: t.schema, table: t.name, column: String(m.tgtCol) } });
      });
    });
    return out;
  }
  function uniqueTables(list, which) {
    var seen = {}, out = [];
    list.forEach(function (p) {
      var o = p[which];
      var k = (o.schema + '.' + o.table).toLowerCase();
      if (!seen[k]) { seen[k] = 1; out.push({ schema: o.schema, name: o.table }); }
    });
    return out;
  }
  async function columnsFor(side, tables) {
    var byKey = {};
    var conn = state.conns[side];
    if (!conn || !tables.length) return byKey;
    for (var i = 0; i < tables.length; i += TABLE_BATCH) {
      var q = columnQuery(tables.slice(i, i + TABLE_BATCH));
      if (!q) continue;
      var rows = await runQuery(conn, q.sql, q.params);
      rows.forEach(function (r) {
        byKey[colKey(r.schema_name, r.table_name, r.column_name).toLowerCase()] = {
          schema: r.schema_name, table: r.table_name, column: r.column_name,
          dataType: r.data_type, maxLength: r.max_length == null ? null : Number(r.max_length),
          collation: r.collation_name || '', codePage: r.code_page == null ? null : Number(r.code_page),
          inUniqueKey: Number(r.in_unique_key) === 1,
        };
      });
    }
    return byKey;
  }
  /* Which modules a mapped pair is reachable from. Every module that
     consumes an Object Mapping column is listed; the SQL editor is not,
     because it runs whatever the user wrote rather than the mapping. */
  function usedByFor() {
    return CLASH_POINTS.filter(function (c) { return c.uses === 'mapping'; }).map(function (c) { return c.id; });
  }

  async function scan() {
    if (state.scanning) return false;
    var now = Date.now();
    if (now - state.lastScanAt < MIN_RUN_INTERVAL_MS) return false;
    state.lastScanAt = now;
    state.scanning = true;
    state.error = ''; state.note = '';
    render();
    try {
      if (!state.model.source.dbCollation && !state.model.target.dbCollation) {
        throw new Error('Run Detect collations first — there is nothing to compare yet.');
      }
      var pairs = mappedPairs();
      if (!pairs.length) throw new Error('No Object Mapping columns are bound to this profile, so there is nothing to scan.');
      var srcCols = await columnsFor('src', uniqueTables(pairs, 'src'));
      var tgtCols = await columnsFor('tgt', uniqueTables(pairs, 'tgt'));
      var used = usedByFor();
      var graded = pairs.map(function (p) {
        var s = srcCols[colKey(p.src.schema, p.src.table, p.src.column).toLowerCase()];
        var t = tgtCols[colKey(p.tgt.schema, p.tgt.table, p.tgt.column).toLowerCase()];
        if (!s || !t) return null;                    // not a text column, or gone: test 13 in Diagnostics owns that
        return { job: p.job, usedBy: used, src: s, tgt: t };
      }).filter(Boolean);

      state.model.resolvedCollation = resolvedCollation(state.model);
      state.findings = buildFindings({ model: state.model, pairs: graded, resolved: state.model.resolvedCollation });
      var summary = summarise(state.findings);
      /* The columns as well as the findings. Stage B's linter resolves a
         column reference in a piece of SQL to a collation, and without this
         it would have to assume every column carries its database's
         collation — true most of the time, and wrong exactly where somebody
         has already had collation trouble and pinned one column. */
      var scannedColumns = [];
      [['src', srcCols], ['tgt', tgtCols]].forEach(function (pair) {
        Object.keys(pair[1]).forEach(function (k) {
          var c = pair[1][k];
          scannedColumns.push({ side: pair[0], schema: c.schema, table: c.table, column: c.column,
            collation: c.collation, dataType: c.dataType, codePage: c.codePage,
            inUniqueKey: c.inUniqueKey, maxLength: c.maxLength });
        });
      });
      state.model.lastScan = {
        at: new Date().toISOString(),
        summary: { high: summary.high, medium: summary.medium, low: summary.low },
        columns: scannedColumns,
        findings: state.findings.map(function (f) {
          return { id: f.id, severity: f.severity, object: f.object, issueCode: f.issueCode,
                   sourceCollation: f.sourceCollation, targetCollation: f.targetCollation,
                   issue: f.issue, recommendation: f.recommendation, usedBy: f.usedBy, job: f.job };
        }),
      };
      state.dirty = true;
      state.note = 'Scanned ' + graded.length + ' mapped text column pair' + (graded.length === 1 ? '' : 's') + '. Save to profile to keep the result.';
      if (summary.high) state.open = true;
    } catch (e) {
      state.error = (e && e.message) || String(e);
    } finally {
      state.scanning = false;
      render();
    }
    return true;
  }

  async function save() {
    if (state.saving) return false;
    state.saving = true;
    state.error = ''; state.note = '';
    render();
    try {
      state.model.resolvedCollation = resolvedCollation(state.model);
      saveToProfile(state.model);
      state.dirty = false;
      state.note = 'Saved to profile.';
    } catch (e) {
      state.error = (e && e.message) || String(e);
    } finally {
      state.saving = false;
      render();
    }
    return true;
  }

  /* Validate an explicit collation against the target, once. */
  async function loadValidCollations() {
    if (state.validCollations) return state.validCollations;
    var rows = await runQuery(state.conns.tgt, VALID_COLLATIONS_QUERY);
    state.validCollations = rows.map(function (r) { return String(r.name); });
    return state.validCollations;
  }

  /* ── Export ──────────────────────────────────────────────────────────── */
  function exportRows() {
    var rows = [['Severity', 'Object', 'Source collation', 'Target collation', 'Issue', 'Recommendation', 'Where used', 'Acknowledged']];
    (state.findings || []).forEach(function (f) {
      rows.push([f.severity, f.object, f.sourceCollation, f.targetCollation, f.issue, f.recommendation,
        labelsFor(f.usedBy).join('; '), f.acknowledged ? 'yes' : 'no']);
    });
    return rows;
  }
  function labelsFor(ids) {
    var by = {};
    CLASH_POINTS.forEach(function (c) { by[c.id] = c.label; });
    return (ids || []).map(function (i) { return by[i] || i; });
  }
  function exportExcel() {
    var rows = exportRows();
    var day = new Date().toISOString().slice(0, 10);
    var name = 'cygenix_collation_' + day;
    // SheetJS is injected by dashboard.html for the Data Analyser. When it
    // is there we write a real workbook; when it is not, a BOM'd CSV opens
    // natively in Excel rather than failing.
    var X = root.XLSX;
    if (X && X.utils && X.write) {
      try {
        var ws = X.utils.aoa_to_sheet(rows);
        var wb = X.utils.book_new();
        X.utils.book_append_sheet(wb, ws, 'Collation');
        X.writeFile(wb, name + '.xlsx');
        return 'xlsx';
      } catch (e) { /* fall through to CSV */ }
    }
    var cell = function (v) {
      var s = String(v == null ? '' : v);
      return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
    };
    var body = rows.map(function (r) { return r.map(cell).join(','); }).join('\r\n');
    download(new Blob(['\ufeff' + body], { type: 'text/csv;charset=utf-8' }), name + '.csv');
    return 'csv';
  }
  function download(blob, filename) {
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 1000);
  }

  /* ── Rendering ───────────────────────────────────────────────────────── */
  var CSS = [
    '#cyg-collation-card{ margin:0 0 26px; padding:0; }',
    '#cyg-collation-card .col-head{ display:flex;align-items:center;gap:12px;flex-wrap:wrap;padding:16px 20px;cursor:pointer; }',
    '#cyg-collation-card .col-toggle{ width:22px;height:22px;border:0;background:transparent;color:var(--color-neutral-700);cursor:pointer;font-size:14px;padding:0;line-height:1; }',
    '#cyg-collation-card.closed .col-toggle{ transform:rotate(-90deg); }',
    '#cyg-collation-card .col-title{ margin:0; }',
    '#cyg-collation-card .col-profile{ font-size:14px;color:var(--color-neutral-700);border:1px solid var(--color-divider);padding:1px 8px;white-space:nowrap; }',
    '#cyg-collation-card .col-resolved{ font-family:var(--mono);font-size:13px;color:var(--color-neutral-700);white-space:nowrap; }',
    '#cyg-collation-card .col-acts{ display:flex;gap:8px;margin-left:auto;flex-wrap:wrap; }',
    '#cyg-collation-card.closed .col-body{ display:none; }',
    '#cyg-collation-card .col-body{ padding:0 20px 20px; }',
    '#cyg-collation-card .col-chip{ display:inline-flex;align-items:center;gap:6px;font-family:var(--font-heading);font-weight:600;font-size:12px;letter-spacing:.08em;text-transform:uppercase;white-space:nowrap;color:var(--color-neutral-600); }',
    '#cyg-collation-card .col-chip::before{ content:"";width:8px;height:8px;background:var(--color-neutral-400);flex:none; }',
    '#cyg-collation-card .col-chip-match{ color:var(--state-ok); } #cyg-collation-card .col-chip-match::before{ background:var(--state-ok); }',
    '#cyg-collation-card .col-chip-warn{ color:var(--state-warn); } #cyg-collation-card .col-chip-warn::before{ background:var(--state-warn); }',
    '#cyg-collation-card .col-chip-high{ color:var(--state-fail); } #cyg-collation-card .col-chip-high::before{ background:var(--state-fail); }',
    '#cyg-collation-card .col-sec{ margin-top:18px;padding-top:14px;border-top:1px solid var(--color-divider); }',
    '#cyg-collation-card .col-sec-h{ font-family:var(--font-heading);font-weight:600;font-size:13px;letter-spacing:.14em;text-transform:uppercase;color:var(--color-neutral-700);margin-bottom:10px;display:flex;align-items:center;gap:8px; }',
    '#cyg-collation-card .col-sides{ display:grid;grid-template-columns:1fr 1fr;gap:20px; }',
    '@media(max-width:1100px){ #cyg-collation-card .col-sides{ grid-template-columns:1fr; } }',
    '#cyg-collation-card .col-side h4{ font-family:var(--font-heading);font-weight:600;font-size:15px;text-transform:uppercase;margin:0 0 8px;color:var(--color-text); }',
    '#cyg-collation-card .col-facts{ display:grid;grid-template-columns:auto 1fr;gap:4px 14px;font-size:13px;line-height:1.5; }',
    '#cyg-collation-card .col-facts dt{ color:var(--color-neutral-600);white-space:nowrap; }',
    '#cyg-collation-card .col-facts dd{ margin:0;color:var(--color-text);font-family:var(--mono);font-size:12px;word-break:break-all; }',
    '#cyg-collation-card .col-facts dd.plain{ font-family:var(--font-body);font-size:13px; }',
    '#cyg-collation-card .col-set{ display:grid;grid-template-columns:minmax(220px,auto) 1fr;gap:10px 16px;align-items:start;font-size:14px; }',
    '@media(max-width:800px){ #cyg-collation-card .col-set{ grid-template-columns:1fr; } }',
    '#cyg-collation-card .col-set > .k{ color:var(--color-neutral-700);padding-top:5px; }',
    '#cyg-collation-card .col-set .col-opts{ display:flex;gap:16px;flex-wrap:wrap;align-items:center; }',
    '#cyg-collation-card .col-set label{ display:inline-flex;align-items:center;gap:6px;cursor:pointer;font-size:14px;color:var(--color-text); }',
    '#cyg-collation-card .col-set input[type=radio]{ accent-color:var(--color-accent-800); }',
    '#cyg-collation-card .col-in{ height:30px;padding:0 8px;font-family:var(--mono);font-size:13px;background:var(--color-bg);border:1px solid var(--color-divider);color:var(--color-text);min-width:240px; }',
    '#cyg-collation-card .col-note{ font-size:13px;line-height:1.5;color:var(--color-neutral-700);margin-top:4px;max-width:86ch; }',
    '#cyg-collation-card table.col-tbl{ width:100%;border-collapse:collapse;font-size:13px; }',
    '#cyg-collation-card table.col-tbl th{ text-align:left;font-family:var(--font-heading);font-weight:600;font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:var(--color-neutral-600);padding:6px 10px 6px 0;border-bottom:1px solid var(--color-divider);white-space:nowrap; }',
    '#cyg-collation-card table.col-tbl td{ padding:7px 10px 7px 0;border-bottom:1px solid var(--color-divider);vertical-align:top;line-height:1.45; }',
    '#cyg-collation-card table.col-tbl tr.ackd td{ color:var(--color-neutral-600); }',
    '#cyg-collation-card .col-sev{ font-family:var(--font-heading);font-weight:600;font-size:12px;letter-spacing:.08em;text-transform:uppercase;white-space:nowrap;display:inline-flex;align-items:center;gap:6px; }',
    '#cyg-collation-card .col-sev::before{ content:"";width:8px;height:8px;flex:none;background:var(--color-neutral-400); }',
    '#cyg-collation-card .col-sev-high{ color:var(--state-fail); } #cyg-collation-card .col-sev-high::before{ background:var(--state-fail); }',
    '#cyg-collation-card .col-sev-medium{ color:var(--state-warn); } #cyg-collation-card .col-sev-medium::before{ background:var(--state-warn); }',
    '#cyg-collation-card .col-sev-low{ color:var(--color-neutral-600); }',
    // break-all would split fin.ledger_entry.code as "fin.ledge / r_entry.c
    // / ode". anywhere breaks only when a cell genuinely cannot fit, and
    // prefers the dots, so a qualified name stays readable.
    '#cyg-collation-card .col-mono{ font-family:var(--mono);font-size:12px;overflow-wrap:anywhere;word-break:normal; }',
    '#cyg-collation-card table.col-tbl td:nth-child(2){ min-width:15ch; }',
    '#cyg-collation-card table.col-tbl td:nth-child(3),#cyg-collation-card table.col-tbl td:nth-child(4){ min-width:16ch; }',
    '#cyg-collation-card .col-ovr{ display:flex;gap:8px;align-items:center;margin-bottom:6px;flex-wrap:wrap; }',
    '#cyg-collation-card .col-filters{ display:flex;gap:6px;margin-left:auto; }',
    '#cyg-collation-card .col-filters button{ background:transparent;border:1px solid var(--color-divider);color:var(--color-neutral-700);font-family:var(--font-heading);font-weight:600;font-size:12px;letter-spacing:.06em;text-transform:uppercase;padding:2px 10px;cursor:pointer; }',
    '#cyg-collation-card .col-filters button.on{ background:var(--color-accent-900);color:var(--color-bg);border-color:var(--color-accent-900); }',
    '#cyg-collation-card .col-msg{ margin-top:12px; }',
    '#cyg-collation-card .col-empty{ font-size:14px;color:var(--color-neutral-700);padding:10px 0; }',
    '#cyg-collation-card [hidden]{ display:none !important; }',
  ].join('\n');

  function injectStyles() {
    if (document.getElementById('cyg-collation-css')) return;
    var s = document.createElement('style');
    s.id = 'cyg-collation-css';
    s.textContent = CSS;
    document.head.appendChild(s);
  }

  function sideHtml(which) {
    var m = state.model[which === 'src' ? 'source' : 'target'];
    var label = which === 'src' ? 'Source' : 'Target';
    var engine = state.engines[which];
    var conn = state.conns[which];
    var h = '<div class="col-side"><h4>' + label + '</h4>';
    /* "Rejected" is a different fault from "missing" and needs a different
       sentence: the connection IS configured, it simply does not hold
       anything the database layer could dial, and the fix is to re-save it
       rather than to create one. */
    if (!conn) return h + '<div class="col-empty">' + (state.conns[which + 'Rejected']
      ? 'The ' + label.toLowerCase() + ' connection saved on this profile isn\'t a usable connection string or endpoint. Re-save it under Connections &rsaquo; Database connections.'
      : 'No ' + label.toLowerCase() + ' connection is configured.') + '</div></div>';
    if (!isSqlServer(engine)) return h + '<div class="col-empty">Collation matching supports SQL Server connections only.</div></div>';
    if (!m.dbCollation) return h + '<div class="col-empty">Not detected yet. Use <b>Detect collations</b>.</div></div>';
    var pc = parseCollation(m.dbCollation);
    var row = function (k, v, plain) { return '<dt>' + esc(k) + '</dt><dd' + (plain ? ' class="plain"' : '') + '>' + esc(v == null || v === '' ? '—' : v) + '</dd>'; };
    h += '<dl class="col-facts">';
    h += row('Server', m.server, true);
    h += row('Database', m.database, true);
    h += row('Server collation', m.serverCollation);
    h += row('Database collation', m.dbCollation);
    h += row('tempdb collation', m.tempdbCollation);
    h += row('Code page', m.codePage == null ? '—' : String(m.codePage), true);
    h += row('Case', pc.bin ? 'Binary (BIN)' : pc.cs === true ? 'Case-sensitive (CS)' : pc.cs === false ? 'Case-insensitive (CI)' : '—', true);
    h += row('Accent', pc.accent === true ? 'Accent-sensitive (AS)' : pc.accent === false ? 'Accent-insensitive (AI)' : '—', true);
    h += row('UTF-8', pc.utf8 ? 'Yes' : 'No', true);
    h += row('SQL Server version', m.productVersion, true);
    h += row('Detected', m.detectedAt ? new Date(m.detectedAt).toLocaleString('en-GB') : '—', true);
    h += '</dl></div>';
    return h;
  }

  function settingsHtml() {
    var m = state.model;
    var opt = function (nameAttr, value, current, label) {
      return '<label><input type="radio" name="' + nameAttr + '" value="' + value + '"' + (current === value ? ' checked' : '') + '> ' + esc(label) + '</label>';
    };
    var h = '<div class="col-set">';
    h += '<div class="k">Resolved collation strategy</div><div><div class="col-opts">'
       + opt('col-strategy', 'target', m.strategy, 'Use target database collation')
       + opt('col-strategy', 'source', m.strategy, 'Preserve source collation')
       + opt('col-strategy', 'explicit', m.strategy, 'Explicit collation')
       + '<input class="col-in" id="col-explicit" placeholder="Latin1_General_CI_AS" value="' + esc(m.explicitCollation || '') + '"' + (m.strategy === 'explicit' ? '' : ' disabled') + '></div>'
       + '<div class="col-note" id="col-explicit-note">Resolves to <b class="col-mono">' + esc(resolvedCollation(m) || 'nothing yet — detect first') + '</b>.'
       + (m.strategy === 'explicit' ? ' Checked against <span class="col-mono">sys.fn_helpcollations()</span> on the target when you save.' : '') + '</div></div>';

    h += '<div class="k">Temp tables</div><div class="col-opts">'
       + opt('col-temp', 'resolved', m.tempTables, 'Use resolved collation')
       + opt('col-temp', 'database_default', m.tempTables, 'COLLATE DATABASE_DEFAULT') + '</div>';

    h += '<div class="k">When a clash is found in SQL Cygenix generates</div><div class="col-opts">'
       + opt('col-gen', 'apply', m.generatedSqlMode, 'Apply fix automatically')
       + opt('col-gen', 'warn', m.generatedSqlMode, 'Warn only') + '</div>';

    h += '<div class="k">When a clash is found in SQL a user writes</div><div class="col-opts">'
       + opt('col-user', 'offer_fix', m.userSqlMode, 'Warn and offer fix')
       + opt('col-user', 'warn', m.userSqlMode, 'Warn only') + '</div>';

    h += '<div class="k">Case-sensitivity risk</div><div><div class="col-opts">'
       + opt('col-case', 'warn', m.caseRule, 'Warn')
       + opt('col-case', 'block', m.caseRule, 'Block job run') + '</div>'
       + '<div class="col-note">A case-sensitive or binary source loading into a case-insensitive target, on a column in a primary key or unique index.</div></div>';

    h += '<div class="k">Non-Unicode code page mismatch</div><div><div class="col-opts">'
       + opt('col-cp', 'warn', m.codePageRule, 'Warn')
       + opt('col-cp', 'block', m.codePageRule, 'Block job run') + '</div>'
       + '<div class="col-note">A char, varchar or text target whose code page differs from the source. Characters outside the target code page become "?".</div></div>';

    var keys = Object.keys(m.columnOverrides || {});
    h += '<div class="k">Column overrides</div><div id="col-ovr-wrap">';
    keys.forEach(function (k) {
      var o = m.columnOverrides[k] || {};
      h += '<div class="col-ovr"><span class="col-mono">' + esc(k) + '</span>'
         + '<span class="col-mono">' + esc(o.collation || '') + '</span>'
         + (o.note ? '<span class="col-note" style="margin:0">' + esc(o.note) + '</span>' : '')
         + '<button class="btn btn-ghost btn-sm" type="button" data-ovr-del="' + esc(k) + '">Remove</button></div>';
    });
    h += '<div class="col-ovr">'
       + '<input class="col-in" id="col-ovr-key" placeholder="schema.table.column">'
       + '<input class="col-in" id="col-ovr-coll" placeholder="Latin1_General_CI_AS">'
       + '<input class="col-in" id="col-ovr-note" placeholder="why" style="font-family:var(--font-body)">'
       + '<button class="btn btn-ghost btn-sm" type="button" id="col-ovr-add">Add</button></div>';
    h += '<div class="col-note">An override beats the strategy for that one column.</div></div>';
    h += '</div>';
    return h;
  }

  function findingsHtml() {
    var all = state.findings || [];
    var shown = state.filter === 'all' ? all : all.filter(function (f) { return f.severity === state.filter; });
    var s = summarise(all);
    var h = '<div class="col-sec-h">Findings'
      + '<span class="col-note" style="margin:0;text-transform:none;letter-spacing:0;font-family:var(--font-body)">'
      + (state.model.lastScan ? esc(all.length + ' finding' + (all.length === 1 ? '' : 's') + ' · scanned ' + new Date(state.model.lastScan.at).toLocaleString('en-GB')) : 'Not scanned yet')
      + '</span>'
      + '<span class="col-filters">'
      + ['all', 'high', 'medium', 'low'].map(function (k) {
          var n = k === 'all' ? all.length : all.filter(function (f) { return f.severity === k; }).length;
          return '<button type="button" data-filter="' + k + '"' + (state.filter === k ? ' class="on"' : '') + '>' + k + ' (' + n + ')</button>';
        }).join('')
      + '</span></div>';

    if (!state.model.lastScan) return h + '<div class="col-empty">Run <b>Scan</b> to grade every column used in Object Mapping for this profile.</div>';
    if (!all.length) return h + '<div class="col-empty">No collation differences found across the mapped columns.</div>';
    if (!shown.length) return h + '<div class="col-empty">No ' + esc(state.filter) + ' findings.</div>';

    var capped = shown.slice(0, MAX_FINDING_ROWS);
    h += '<table class="col-tbl"><thead><tr><th>Severity</th><th>Object</th><th>Source collation</th><th>Target collation</th><th>Issue</th><th>Recommendation</th><th>Where used</th><th>Acknowledge</th></tr></thead><tbody>';
    capped.forEach(function (f) {
      h += '<tr' + (f.acknowledged ? ' class="ackd"' : '') + '>'
        + '<td><span class="col-sev col-sev-' + f.severity + '">' + f.severity + '</span></td>'
        + '<td class="col-mono">' + esc(f.object) + '</td>'
        + '<td class="col-mono">' + esc(f.sourceCollation || '—') + '</td>'
        + '<td class="col-mono">' + esc(f.targetCollation || '—') + '</td>'
        + '<td>' + esc(f.issue) + '</td>'
        + '<td>' + esc(f.recommendation) + '</td>'
        + '<td>' + esc(labelsFor(f.usedBy).join(', ') || '—') + '</td>'
        + '<td><label><input type="checkbox" data-ack="' + esc(f.id) + '"' + (f.acknowledged ? ' checked' : '') + '> ' + (f.acknowledged ? 'Acknowledged' : 'Acknowledge') + '</label></td>'
        + '</tr>';
    });
    h += '</tbody></table>';
    if (shown.length > capped.length) h += '<div class="col-note">Showing ' + capped.length + ' of ' + shown.length + ' — export to see the rest.</div>';
    if (s.acknowledged) h += '<div class="col-note">' + s.acknowledged + ' acknowledged finding' + (s.acknowledged === 1 ? '' : 's') + ' listed but not counted.</div>';
    return h;
  }

  function driftedSides() {
    var out = [];
    ['src', 'tgt'].forEach(function (side) {
      var field = side === 'src' ? 'source' : 'target';
      var saved = state.model.fingerprint[field];
      if (!saved) return;
      var live = fingerprintOf(state.conns[side]);
      if (live && live !== saved) out.push(field);
    });
    return out;
  }

  function cardHtml() {
    var m = state.model;
    var drift = driftedSides();
    var chip = drift.length ? { key: 'none', word: 'Not checked' } : chipOf(m, state.findings);
    /* "Not SQL Server" means a connection we can read that points at another
       engine. A side holding nothing usable has no engine at all, and that
       is a missing or unreadable connection rather than a PostgreSQL one —
       sideHtml names it per side. Testing isSqlServer('') here instead made
       one unreadable side answer "SQL Server only" for the whole card and
       hide the other side, which was working. */
    var nonSql = ['src', 'tgt'].filter(function (s) { return state.engines[s] && !isSqlServer(state.engines[s]); });
    var bothSql = !nonSql.length;
    var anyConn = state.conns.src || state.conns.tgt;
    var anyRejected = !!(state.conns.srcRejected || state.conns.tgtRejected);

    var h = '<section class="cx-blueprint' + (state.open ? '' : ' closed') + '" id="cyg-collation-card">';
    h += '<span class="cx-corner tl"></span><span class="cx-corner tr"></span><span class="cx-corner bl"></span><span class="cx-corner br"></span>';
    h += '<div class="col-head" id="col-head">';
    h += '<button class="col-toggle" type="button" id="col-toggle" aria-expanded="' + (state.open ? 'true' : 'false') + '" aria-controls="col-body" aria-label="' + (state.open ? 'Collapse' : 'Expand') + ' the collation card">▾</button>';
    h += '<h2 class="cx-h col-title">Collation</h2>';
    h += '<span class="col-profile">' + esc(profileName()) + '</span>';
    h += '<span class="col-chip col-chip-' + chip.key + '" id="col-chip">' + esc(chip.word) + '</span>';
    h += '<span class="col-resolved">' + esc(resolvedCollation(m) || '—') + '</span>';
    h += '<span class="col-acts">';
    h += '<button class="btn btn-ghost btn-sm" type="button" id="col-detect"' + (bothSql && anyConn ? '' : ' disabled') + '>' + (state.detecting ? 'Detecting…' : 'Detect collations') + '</button>';
    h += '<button class="btn btn-ghost btn-sm" type="button" id="col-scan"' + (bothSql && anyConn ? '' : ' disabled') + '>' + (state.scanning ? 'Scanning…' : 'Scan') + '</button>';
    h += '<button class="btn btn-ghost btn-sm" type="button" id="col-export"' + (state.findings.length ? '' : ' disabled') + '>Export (Excel)</button>';
    h += '<button class="btn btn-primary btn-sm" type="button" id="col-save"' + (state.saving ? ' disabled' : '') + '>' + (state.saving ? 'Saving…' : 'Save to profile') + (state.dirty ? ' •' : '') + '</button>';
    h += '</span></div>';

    h += '<div class="col-body" id="col-body">';

    if (!anyConn && !anyRejected) {
      h += '<div class="col-empty">Configure a source and target above, then detect their collations.</div>';
    } else if (!bothSql) {
      h += '<div class="cx-attn cx-attn-warn"><div class="cx-h-sm">SQL Server only</div><p>Collation matching supports SQL Server connections only. '
        + 'Every other part of Cygenix behaves exactly as it does today.</p></div>';
    }

    if (drift.length) {
      h += '<div class="cx-attn cx-attn-warn" id="col-drift"><div class="cx-h-sm">Connection changed since collations were detected</div>'
        + '<p>The ' + drift.join(' and ') + ' now points at a different server or database than the one these collations were read from. Re-run <b>Detect collations</b>.</p></div>';
    }
    if (state.error) {
      h += '<div class="cx-attn cx-attn-fail col-msg" id="col-error"><div class="cx-h-sm">That did not finish</div><p>' + esc(state.error) + '</p></div>';
    } else if (state.note) {
      h += '<div class="col-note col-msg" id="col-note">' + esc(state.note) + '</div>';
    }

    if ((anyConn || anyRejected) && bothSql) {
      h += '<div class="col-sec"><div class="col-sec-h">Detected</div><div class="col-sides">' + sideHtml('src') + sideHtml('tgt') + '</div></div>';
      h += '<div class="col-sec"><div class="col-sec-h">Settings</div>' + settingsHtml() + '</div>';
      h += '<div class="col-sec">' + findingsHtml() + '</div>';
    }
    h += '</div></section>';
    return h;
  }

  function profileName() {
    var st = store();
    var p = activeProfile(st);
    return p ? (p.name || p.id) : 'No profile selected';
  }

  function render() {
    var mount = document.getElementById('cyg-collation-mount');
    if (!mount) return;
    mount.innerHTML = cardHtml();
  }

  /* One delegated listener on the mount, so a re-render never leaves a
     stale handler behind. */
  function wire(mount) {
    mount.addEventListener('click', function (ev) {
      var t = ev.target;
      var b = t.closest ? t.closest('button') : null;
      if (b && b.id === 'col-toggle') { setOpen(!state.open); return; }
      if (!b && t.closest && t.closest('#col-head') && !t.closest('.col-acts')) { setOpen(!state.open); return; }
      if (!b) return;
      if (b.id === 'col-detect') { detect(); return; }
      if (b.id === 'col-scan') { scan(); return; }
      if (b.id === 'col-export') { try { exportExcel(); } catch (e) { state.error = e.message; render(); } return; }
      if (b.id === 'col-save') { save(); return; }
      if (b.id === 'col-ovr-add') { addOverride(); return; }
      if (b.hasAttribute('data-ovr-del')) { delOverride(b.getAttribute('data-ovr-del')); return; }
      if (b.hasAttribute('data-filter')) { state.filter = b.getAttribute('data-filter'); uiWrite({ filter: state.filter }); render(); return; }
    });
    mount.addEventListener('change', function (ev) {
      var t = ev.target;
      if (t.name === 'col-strategy') { state.model.strategy = t.value; state.model.resolvedCollation = resolvedCollation(state.model); state.dirty = true; render(); return; }
      if (t.name === 'col-temp') { state.model.tempTables = t.value; state.dirty = true; return; }
      if (t.name === 'col-gen') { state.model.generatedSqlMode = t.value; state.dirty = true; return; }
      if (t.name === 'col-user') { state.model.userSqlMode = t.value; state.dirty = true; return; }
      if (t.name === 'col-case') { state.model.caseRule = t.value; state.dirty = true; return; }
      if (t.name === 'col-cp') { state.model.codePageRule = t.value; state.dirty = true; return; }
      if (t.id === 'col-explicit') { state.model.explicitCollation = t.value.trim() || null; state.model.resolvedCollation = resolvedCollation(state.model); state.dirty = true; render(); return; }
      if (t.hasAttribute && t.hasAttribute('data-ack')) { toggleAck(t.getAttribute('data-ack'), t.checked); return; }
    });
  }

  function setOpen(open) {
    state.open = !!open;
    uiWrite({ collapsed: !state.open });
    render();
  }
  function toggleAck(id, on) {
    var list = state.model.acknowledged || (state.model.acknowledged = []);
    var i = list.indexOf(id);
    if (on && i < 0) list.push(id);
    if (!on && i >= 0) list.splice(i, 1);
    state.findings.forEach(function (f) { if (f.id === id) f.acknowledged = !!on; });
    state.dirty = true;
    render();
  }
  function addOverride() {
    var k = (document.getElementById('col-ovr-key') || {}).value || '';
    var c = (document.getElementById('col-ovr-coll') || {}).value || '';
    var n = (document.getElementById('col-ovr-note') || {}).value || '';
    k = k.trim(); c = c.trim();
    if (!k || !c) { state.error = 'An override needs both schema.table.column and a collation.'; render(); return; }
    if (k.split('.').length !== 3) { state.error = 'Write the column as schema.table.column, for example dbo.Client.Name.'; render(); return; }
    state.model.columnOverrides[k] = { collation: c, note: n.trim() };
    state.error = ''; state.dirty = true;
    render();
  }
  function delOverride(k) {
    delete state.model.columnOverrides[k];
    state.dirty = true;
    render();
  }

  /* Read the world: which profile, which connections, what was saved. */
  function refresh() {
    var st = store();
    var p = activeProfile(st);
    state.profileId = p ? p.id : null;
    state.model = normalise(p && p.collation);
    state.findings = (state.model.lastScan && Array.isArray(state.model.lastScan.findings))
      ? buildFindingsFromSaved(state.model)
      : [];
    state.conns = connectionsFor(p);
    state.engines = { src: engineOf(state.conns.src), tgt: engineOf(state.conns.tgt) };
    state.dirty = false;
    state.error = ''; state.note = '';
  }
  /* A saved scan is replayed rather than recomputed: the columns it graded
     came from a database read that may no longer be reachable, and showing
     the last known answer beats showing nothing. */
  function buildFindingsFromSaved(model) {
    var ack = {};
    (model.acknowledged || []).forEach(function (k) { ack[k] = true; });
    return (model.lastScan.findings || []).map(function (f) {
      return Object.assign({}, f, { acknowledged: !!ack[f.id] });
    });
  }

  /* ════════════════════════════════════════════════════════════════════════
     THE PUBLIC API — what the rest of the product asks (Stage B)
     ────────────────────────────────────────────────────────────────────────
     Four questions, and the four answers. The rules behind them live in
     cygenix-collation-rules.js and run identically in the Function App;
     these are the thin wrappers that know where the settings are kept in a
     browser. Everything here is read-only: Stage B reports, it does not
     change a single character of anybody's SQL.
     ════════════════════════════════════════════════════════════════════════ */

  /* The saved settings for a profile, or null. No profile id means the
     active one, which is what every caller on a page actually wants. */
  function settings(profileId) {
    var st = store();
    if (!st || !st.profiles) return null;
    var p = profileId
      ? st.profiles.filter(function (x) { return x.id === profileId; })[0]
      : activeProfile(st);
    if (!p || !p.collation) return null;
    return normalise(p.collation);
  }
  function modelOr(model) { return model || settings() || null; }

  /* The collation to write for one column: override, then strategy. */
  function resolve(side, schema, table, column) {
    var m = settings();
    if (!m) return '';
    return Rules.resolveWith(m, side, schema, table, column);
  }

  /* May a job run? Blocking is opt-in per rule, so this answers yes with
     warnings unless the operator asked for a block. */
  function gate(profileId) {
    var m = settings(profileId);
    if (!m) return { ok: true, reasons: [], warnings: [], checked: false, resolvedCollation: '' };
    return Rules.gateWith(m);
  }

  /* Which table belongs to which side. Read from the jobs bound to the
     profile, because that is the only place the product records "this table
     is the source and that one is the target". */
  function tablesFromJobs(model) {
    var out = [], seen = {};
    var add = function (o, side) {
      if (!o) return;
      var k = (o.schema + '.' + o.name + '|' + side).toLowerCase();
      if (seen[k]) return;
      seen[k] = 1; out.push({ side: side, schema: o.schema, name: o.name });
    };
    try {
      var jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]') || [];
      var st = store();
      var pid = state.profileId || (st && st.settings && st.settings.activeProfileId);
      jobs.forEach(function (j) {
        if (!j || j.deleted || j.isDeleted) return;
        if (pid && root.CygenixJobProfile && root.CygenixJobProfile.of) {
          var of = null;
          try { of = root.CygenixJobProfile.of(j, st); } catch (e) { of = null; }
          if (of && of.id !== pid) return;
        }
        add(splitObject(j.sourceTable || j.source, 'dbo'), 'src');
        add(splitObject(j.target || j.targetTable, 'dbo'), 'tgt');
      });
    } catch (e) { /* no jobs on this page is not a fault */ }
    if (model) {
      /* The databases themselves, so a three-part name resolves even when no
         job mentions the table. */
      if (model.source && model.source.database) out.database = model.source.database;
    }
    return out;
  }

  /* The context findClashes needs: the settings, the columns the last scan
     read, and which table is which side. Per-column collations come from the
     scan when there was one; without it every text column is assumed to
     carry its database's collation, which is true far more often than not
     and is the assumption a person makes reading the same query. */
  function contextFor(opts) {
    opts = opts || {};
    var model = modelOr(opts.model);
    if (!model) return null;
    var columns = opts.columns
      || (model.lastScan && Array.isArray(model.lastScan.columns) ? model.lastScan.columns : []);
    var tables = opts.tables || tablesFromJobs(model);
    return { model: model, columns: columns, tables: tables };
  }

  /* findClashes(sql, context) — context is optional; without one it is built
     from the active profile. Returns [] rather than throwing on anything,
     because it is called from inside other modules' render paths and a
     linter that can break the screen it lints is worse than no linter. */
  function findClashes(sql, context) {
    try {
      var ctx = context && context.model ? context : contextFor(context);
      if (!ctx) return [];
      return Rules.findClashes(sql, ctx);
    } catch (e) { return []; }
  }

  /* One call for a module that wants to lint and draw. */
  function lint(sql, opts) {
    var ctx = contextFor(opts);
    var clashes = ctx ? findClashes(sql, ctx) : [];
    return { clashes: clashes, summary: Rules.summariseClashes(clashes),
             model: ctx ? ctx.model : null, configured: !!ctx };
  }

  /* ── Drawing, shared by every module that reports a clash ────────────── */
  var LINK = '/dashboard#goto=connections/databases';
  var WIRE_CSS = [
    '.cyg-coll-banner{ margin:0 0 12px; }',
    '.cyg-coll-banner .cyg-coll-list{ margin:6px 0 0; padding:0; list-style:none; }',
    '.cyg-coll-banner .cyg-coll-list li{ font-size:13px;line-height:1.5;color:var(--color-neutral-800,var(--text2,#444));margin-bottom:3px; }',
    '.cyg-coll-banner .cyg-coll-ln{ font-family:var(--mono,monospace);font-size:12px;color:var(--color-neutral-600,var(--text3,#777));margin-right:6px; }',
    '.cyg-coll-banner .cyg-coll-sql{ font-family:var(--mono,monospace);font-size:12px;word-break:break-word; }',
    '.cyg-coll-banner .cyg-coll-fix{ color:var(--color-neutral-700,var(--text2,#555)); }',
    '.cyg-coll-banner a{ color:var(--color-accent-700,var(--accent,#4a5bd6)); }',
    '.cyg-coll-badge{ display:inline-flex;align-items:center;gap:4px;font-family:var(--font-heading,inherit);font-weight:600;font-size:10px;letter-spacing:.06em;text-transform:uppercase;white-space:nowrap;padding:1px 5px;border:1px solid var(--color-divider,var(--border,#ddd));cursor:help; }',
    '.cyg-coll-badge::before{ content:"";width:6px;height:6px;flex:none;background:var(--color-neutral-400,#999); }',
    '.cyg-coll-badge.ok{ color:var(--state-ok,var(--green,#3f6b52)); } .cyg-coll-badge.ok::before{ background:var(--state-ok,var(--green,#3f6b52)); }',
    '.cyg-coll-badge.warn{ color:var(--state-warn,var(--amber,#9a6b1f)); } .cyg-coll-badge.warn::before{ background:var(--state-warn,var(--amber,#9a6b1f)); }',
    '.cyg-coll-badge.fail{ color:var(--state-fail,var(--red,#9c3f38)); } .cyg-coll-badge.fail::before{ background:var(--state-fail,var(--red,#9c3f38)); }',
  ].join('\n');
  function injectWireStyles() {
    if (typeof document === 'undefined' || document.getElementById('cyg-coll-wire-css')) return;
    var s = document.createElement('style');
    s.id = 'cyg-coll-wire-css';
    s.textContent = WIRE_CSS;
    (document.head || document.documentElement).appendChild(s);
  }

  var MAX_BANNER_ROWS = 8;
  function bannerHtml(clashes, opts) {
    opts = opts || {};
    if (!clashes || !clashes.length) return '';
    var high = clashes.filter(function (c) { return c.severity === 'high'; }).length;
    var tone = high ? 'cx-attn-fail' : 'cx-attn-warn';
    var title = high
      ? high + ' collation clash' + (high === 1 ? '' : 'es') + ' would fail when this runs'
      : clashes.length + ' collation difference' + (clashes.length === 1 ? '' : 's') + ' in this SQL';
    var h = '<div class="cx-attn ' + tone + ' cyg-coll-banner">';
    h += '<div class="cx-h-sm">' + esc(title) + '</div>';
    h += '<ul class="cyg-coll-list">';
    clashes.slice(0, MAX_BANNER_ROWS).forEach(function (c) {
      h += '<li><span class="cyg-coll-ln">line ' + c.line + '</span>'
        + '<span class="cyg-coll-sql">' + esc(c.expression) + '</span> — '
        + esc(c.leftCollation || '?') + ' against ' + esc(c.rightCollation || '?')
        + '. <span class="cyg-coll-fix">Fix: ' + esc(c.fix) + '</span></li>';
    });
    h += '</ul>';
    if (clashes.length > MAX_BANNER_ROWS) {
      h += '<div class="cyg-coll-fix" style="font-size:13px;margin-top:4px">and ' + (clashes.length - MAX_BANNER_ROWS) + ' more.</div>';
    }
    h += '<div class="cyg-coll-fix" style="font-size:13px;margin-top:6px">'
      + 'Nothing has been changed. ' + (opts.link === false ? '' : '<a href="' + LINK + '">Open the Collation card</a> to set what should happen.')
      + '</div></div>';
    return h;
  }

  /* Fill an element with the banner, or hide it when there is nothing to
     say. Returns the clashes so a caller can log or count them. */
  function renderBanner(el, sql, opts) {
    var node = typeof el === 'string' ? (typeof document !== 'undefined' ? document.getElementById(el) : null) : el;
    if (!node) return [];
    injectWireStyles();
    var res = lint(sql, opts);
    if (!res.clashes.length) { node.innerHTML = ''; node.style.display = 'none'; return []; }
    node.innerHTML = bannerHtml(res.clashes, opts);
    node.style.display = '';
    return res.clashes;
  }

  /* The per-pair badge for Object Mapping. Green when the two columns agree,
     amber when the difference is only accent or collation version, red when
     it is the kind that errors or loses data. */
  function badgeFor(srcRef, tgtRef, opts) {
    var m = modelOr(opts && opts.model);
    if (!m) return null;
    var ctx = contextFor(opts);
    var byKey = {};
    (ctx && ctx.columns || []).forEach(function (c) {
      byKey[(c.side + '|' + c.schema + '.' + c.table + '.' + c.column).toLowerCase()] = c;
    });
    var look = function (side, r) {
      if (!r || !r.column) return null;
      return byKey[(side + '|' + r.schema + '.' + r.table + '.' + r.column).toLowerCase()] || null;
    };
    var s = look('src', srcRef), t = look('tgt', tgtRef);
    var sc = s ? s.collation : (m.source && m.source.dbCollation) || '';
    var tc = t ? t.collation : (m.target && m.target.dbCollation) || '';
    /* A pair we have never read, on a side we have never detected, gets no
       badge at all rather than a green one. */
    if (!sc || !tc) return null;
    if (s && s.dataType && !isTextType(s.dataType)) return null;
    if (t && t.dataType && !isTextType(t.dataType)) return null;

    var diff = collationDiff(sc, tc);
    var state = !diff.length ? 'ok' : (isSoftDiff(diff) ? 'warn' : 'fail');
    var word = state === 'ok' ? 'Collation' : state === 'warn' ? 'Collation' : 'Collation';
    /* A High finding recorded against this target column outranks the name
       comparison: a code page difference can exist between two collations
       that differ only softly by name. */
    var key = tgtRef ? (tgtRef.schema + '.' + tgtRef.table + '.' + tgtRef.column).toLowerCase() : '';
    var ack = {};
    (m.acknowledged || []).forEach(function (k) { ack[k] = true; });
    ((m.lastScan && m.lastScan.findings) || []).forEach(function (f) {
      if (!f || f.severity !== 'high' || ack[f.id]) return;
      if (String(f.object || '').toLowerCase() === key) state = 'fail';
    });

    var resolved = resolvedCollation(m);
    var title = state === 'ok'
      ? 'Both sides are ' + sc + '.'
      : 'Source ' + sc + ' against target ' + tc + ' (' + diff.join(', ') + '). '
        + (resolved ? 'Fix: COLLATE ' + resolved + '.' : 'Detect the collations to get a fix.');
    return { state: state, word: word, title: title, sourceCollation: sc, targetCollation: tc,
             diff: diff, resolved: resolved,
             html: '<span class="cyg-coll-badge ' + state + '" title="' + esc(title) + '">' + esc(word) + '</span>' };
  }

  /* The one-line summary Object Mapping puts at the top of its screen. */
  function summaryLine(pairs, opts) {
    var m = modelOr(opts && opts.model);
    if (!m) return null;
    var counts = { ok: 0, warn: 0, fail: 0 };
    (pairs || []).forEach(function (p) {
      var b = badgeFor(p.src, p.tgt, opts);
      if (b) counts[b.state]++;
    });
    var total = counts.ok + counts.warn + counts.fail;
    if (!total) return null;
    var state = counts.fail ? 'fail' : counts.warn ? 'warn' : 'ok';
    var text = state === 'ok'
      ? 'All ' + total + ' mapped text column' + (total === 1 ? '' : 's') + ' share a collation.'
      : (counts.fail ? counts.fail + ' mapped column' + (counts.fail === 1 ? '' : 's') + ' will not compare cleanly' : '')
        + (counts.fail && counts.warn ? ', and ' : '')
        + (counts.warn ? counts.warn + ' differ' + (counts.warn === 1 ? 's' : '') + ' only in accent or version' : '')
        + '. Resolved collation: ' + (resolvedCollation(m) || 'not set') + '.';
    return { state: state, counts: counts, total: total, text: text, link: LINK };
  }

  /* ════════════════════════════════════════════════════════════════════════
     APPLYING A FIX (Stage C) — the generator-facing half
     ────────────────────────────────────────────────────────────────────────
     Every one of these returns a SUFFIX, so a generator concatenates it
     unconditionally:

         'CREATE TABLE #s (' + name + ' ' + type + cygCollation.tempCollate(type) + ')'

     With no settings, in warn mode, or on a profile whose collations already
     agree, they all return '' and the generated SQL is byte-identical to
     what it was before this feature existed. That is the point of the
     shape: no generator grows a branch it can get wrong.

     Each one also COUNTS what it emitted, so the script's header comment can
     say how many fixes it carries without the generator tracking it.
     ════════════════════════════════════════════════════════════════════════ */
  var _applied = { count: 0 };
  function resetApplied() { _applied.count = 0; return _applied.count; }
  function appliedCount() { return _applied.count; }
  function tally(s) { if (s) _applied.count++; return s; }

  /* Is this profile going to change any SQL at all? Modules ask before they
     bother building a header. */
  function applyMode() {
    var m = settings();
    if (!m) return null;
    return Rules.applies(m) ? 'apply' : m.generatedSqlMode;
  }
  /* The clause for one column in a comparison the generator is building. */
  function collateFor(side, schema, table, column, currentCollation) {
    return tally(Rules.collateForColumn(settings(), side, schema, table, column, currentCollation));
  }
  /* The clause for a text column in a temp or staging table. */
  function tempCollate(dataType) {
    return tally(Rules.collateForTempColumn(settings(), dataType));
  }
  /* Both sides of a comparison at once. */
  function comparisonCollate(left, right) {
    var r = Rules.collateForComparison(settings(), left, right);
    if (r.left) _applied.count++;
    if (r.right) _applied.count++;
    return r;
  }
  /* The header line for a script that carries fixes, or '' for one that does
     not. Call after generating, when the count is known. */
  function appliedHeader(count) {
    var n = count == null ? _applied.count : count;
    if (!n) return '';
    var m = settings();
    var st = store();
    var p = activeProfile(st);
    return Rules.headerComment(p ? (p.name || p.id) : '', n, resolvedCollation(m));
  }
  /* The fingerprint of the settings a script was generated under. Stored on
     the job so a later settings change can mark it for regeneration. */
  function stamp() { return Rules.settingsStamp(settings()); }
  /* Was this script generated under the settings in force now? A script with
     no stamp predates the feature and is not called stale — nobody should be
     told to regenerate something that was correct when it was made and has
     no collation work to do. */
  function stampIsCurrent(saved) {
    var now = stamp();
    if (!saved) return !now;
    return saved === now;
  }

  /* Called by switchConnTab('databases'). Idempotent: the first call builds
     the card, later ones re-read the profile so a connection saved since is
     reflected. */
  function init(mountId) {
    if (typeof document === 'undefined') return;
    var mount = document.getElementById(mountId || 'cyg-collation-mount');
    if (!mount) return;
    if (!state.mounted) {
      injectStyles();
      state.user = userKey();
      var ui = uiRead();
      state.filter = ['all', 'high', 'medium', 'low'].indexOf(ui.filter) >= 0 ? ui.filter : 'all';
      state.open = ui.collapsed === true ? false : true;
      wire(mount);
      state.mounted = true;
    }
    refresh();
    // The card opens itself when something needs attention: an unacknowledged
    // High finding, or a connection that has moved since detection.
    var s = summarise(state.findings);
    if (s.high > 0 || driftedSides().length) state.open = true;
    render();
  }

  /* Open the card from elsewhere in the product — Object Mapping and the
     job-run block both link here in Stage B. */
  function openCard() {
    try {
      if (typeof root.showView === 'function') root.showView('connections');
      if (typeof root.switchConnTab === 'function') root.switchConnTab('databases');
    } catch (e) { /* the caller may already be on the page */ }
    init('cyg-collation-mount');
    state.open = true;
    render();
    var el = document.getElementById('cyg-collation-card');
    if (el && el.scrollIntoView) el.scrollIntoView({ block: 'start', behavior: 'smooth' });
  }

  return {
    // The Stage B API: the four questions the rest of the product asks.
    settings: settings, resolve: resolve, findClashes: findClashes, gate: gate,
    // and the helpers that let a module report a clash in three lines.
    lint: lint, bannerHtml: bannerHtml, renderBanner: renderBanner,
    badgeFor: badgeFor, summaryLine: summaryLine, contextFor: contextFor,
    summariseClashes: Rules.summariseClashes, LINK: LINK,
    // Stage C: applying a fix where the SQL is built.
    applyMode: applyMode, collateFor: collateFor, tempCollate: tempCollate,
    comparisonCollate: comparisonCollate, appliedHeader: appliedHeader,
    resetApplied: resetApplied, appliedCount: appliedCount,
    stamp: stamp, stampIsCurrent: stampIsCurrent, MARKER: Rules.MARKER,
    // Rewriting SQL a person wrote — only ever behind a confirmation.
    applyFix: function (sql, clashes) { return Rules.applyFix(sql, clashes, settings()); },
    userMode: function () { var m = settings(); return m ? m.userSqlMode : null; },
    // lifecycle
    init: init, open: openCard, refresh: refresh, render: render,
    detect: detect, scan: scan, save: save, exportExcel: exportExcel,
    // constants
    VERSION: VERSION, UI_KEY: UI_KEY, MIN_RUN_INTERVAL_MS: MIN_RUN_INTERVAL_MS,
    CALL_TIMEOUT_MS: CALL_TIMEOUT_MS, TABLE_BATCH: TABLE_BATCH, MAX_FINDING_ROWS: MAX_FINDING_ROWS,
    CLASH_POINTS: CLASH_POINTS, ISSUE: ISSUE,
    SERVER_QUERY: SERVER_QUERY, VALID_COLLATIONS_QUERY: VALID_COLLATIONS_QUERY,
    // pure rules, for tests and for Stage B
    parseCollation: parseCollation, collationDiff: collationDiff, isSoftDiff: isSoftDiff,
    isNonUnicodeText: isNonUnicodeText, isTextType: isTextType,
    resolvedCollation: resolvedCollation, defaults: defaults, normalise: normalise,
    buildFindings: buildFindings, summarise: summarise, chipOf: chipOf,
    columnQuery: columnQuery, splitObject: splitObject,
    hostOf: hostOf, databaseOf: databaseOf, fingerprintOf: fingerprintOf, engineOf: engineOf,
    isSqlServer: isSqlServer, looksLikeConnection: looksLikeConnection, labelsFor: labelsFor,
    _state: state,
  };
});
