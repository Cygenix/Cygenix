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
   ============================================================================ */
(function (root, factory) {
  'use strict';
  var api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  root.cygCollation = api;
  root.CygenixCollation = api;          // the house naming, kept as an alias
})(typeof window !== 'undefined' ? window : globalThis, function (root) {
  'use strict';

  /* ── Constants a reader will want to find ────────────────────────────── */
  var VERSION = 1;
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
  var FLAG_TOKENS = { CS: 1, CI: 1, AS: 1, AI: 1, KS: 1, WS: 1, SC: 1, VSS: 1, BIN: 1, BIN2: 1, UTF8: 1 };

  function parseCollation(name) {
    var n = String(name || '').trim();
    if (!n) return { name: '', known: false, cs: null, accent: null, bin: false, utf8: false,
                     legacy: false, lineage: '', language: '', version: '' };
    var upper = n.toUpperCase();
    var bin = /_BIN2?(_|$)/.test(upper);
    var legacy = /^SQL_/.test(upper);
    /* Token by token rather than one global replace: a global replace that
       rewrites `_CI_` to `_` moves past the `_AS` that followed it, so
       Latin1_General_CI_AS came out as LATIN1_GENERAL_AS and two collations
       that share a language looked like different languages. */
    var parts = upper.split('_').filter(Boolean);
    var language = [], version = '';
    parts.forEach(function (tok, i) {
      if (i === 0 && tok === 'SQL') return;                 // lineage, held separately
      if (FLAG_TOKENS[tok]) return;                         // a sensitivity flag
      if (/^\d+$/.test(tok)) { version = tok; return; }     // the collation version
      language.push(tok);
    });
    return {
      name: n,
      known: true,
      bin: bin,
      // A binary collation compares byte for byte: case and accent both matter.
      cs: bin ? true : (/_CS(_|$)/.test(upper) ? true : (/_CI(_|$)/.test(upper) ? false : null)),
      accent: bin ? true : (/_AS(_|$)/.test(upper) ? true : (/_AI(_|$)/.test(upper) ? false : null)),
      utf8: /_UTF8(_|$)/.test(upper),
      legacy: legacy,
      lineage: legacy ? 'SQL' : 'WINDOWS',
      language: language.join('_'),
      version: version,
      family: (legacy ? 'SQL_' : '') + language.join('_') + (version ? '_' + version : ''),
    };
  }

  /* How two collations differ. The words are the vocabulary the severity
     rules are written in, so the difference between a cosmetic mismatch and
     a dangerous one is one list membership test rather than a second parse.

     `accent` and `version` are the soft pair: same language, same lineage,
     same case rule, same encoding, and the values still compare equal for
     everything but ordering and diacritics. Everything else — a different
     language, a Windows collation against a legacy SQL_ one, a case rule
     that flips, a UTF-8 target — changes what the data means. */
  var SOFT_DIFFS = { accent: 1, version: 1 };
  function collationDiff(a, b) {
    var pa = parseCollation(a), pb = parseCollation(b);
    var out = [];
    if (!pa.known || !pb.known) return out;
    if (pa.name === pb.name) return out;
    if (pa.cs !== pb.cs) out.push('case');
    if (pa.utf8 !== pb.utf8) out.push('encoding');
    if (pa.language !== pb.language) out.push('language');
    if (pa.lineage !== pb.lineage) out.push('lineage');
    if (pa.bin !== pb.bin) out.push('binary');
    if (pa.version !== pb.version) out.push('version');
    if (pa.accent !== pb.accent) out.push('accent');
    if (!out.length) out.push('version');   // names differ for a reason we did not name
    return out;
  }
  function isSoftDiff(diff) {
    return diff.length > 0 && diff.every(function (d) { return !!SOFT_DIFFS[d]; });
  }

  /* Non-Unicode types only. An nvarchar column stores UTF-16 whatever the
     collation says, so a code page difference cannot lose a character in
     one; a varchar column stores exactly the code page its collation
     names, and anything outside it becomes '?' on the way in. */
  function isNonUnicodeText(dataType) {
    return /^(char|varchar|text)$/i.test(String(dataType || '').trim());
  }
  function isTextType(dataType) {
    return /^(n?char|n?varchar|n?text|sysname)$/i.test(String(dataType || '').trim());
  }

  /* The collation this feature will use, before any per-column override. */
  function resolvedCollation(model) {
    if (!model) return '';
    if (model.strategy === 'explicit') return String(model.explicitCollation || '').trim();
    if (model.strategy === 'source') return (model.source && model.source.dbCollation) || '';
    return (model.target && model.target.dbCollation) || '';
  }

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

  /* Which engine a saved connection value points at. An https URL is the
     Azure Function App, which only ever reaches the one database it is
     bound to — it is SQL Server, but not a database this card chose. */
  function engineOf(value) {
    var v = String(value || '').trim();
    if (!v) return '';
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

  /* ── The saved shape ─────────────────────────────────────────────────── */
  function sideDefaults() {
    return { server: '', database: '', serverCollation: '', dbCollation: '', tempdbCollation: '',
             codePage: null, cs: null, as: null, utf8: false, productVersion: '', detectedAt: null };
  }
  function defaults() {
    return {
      version: VERSION,
      source: sideDefaults(),
      target: sideDefaults(),
      fingerprint: { source: '', target: '' },
      strategy: 'target',
      explicitCollation: null,
      resolvedCollation: '',
      tempTables: 'resolved',
      generatedSqlMode: 'apply',
      userSqlMode: 'offer_fix',
      caseRule: 'warn',
      codePageRule: 'warn',
      columnOverrides: {},
      acknowledged: [],
      lastScan: null,
    };
  }
  /* A stored object from an older build, or a hand-edited one, is filled in
     rather than thrown on — this runs on page load. */
  function normalise(saved) {
    var d = defaults();
    if (!saved || typeof saved !== 'object') return d;
    var pickSide = function (s) {
      var out = sideDefaults();
      if (s && typeof s === 'object') Object.keys(out).forEach(function (k) { if (s[k] !== undefined) out[k] = s[k]; });
      return out;
    };
    var one = function (v, allowed, fallback) { return allowed.indexOf(v) >= 0 ? v : fallback; };
    return {
      version: VERSION,
      source: pickSide(saved.source),
      target: pickSide(saved.target),
      fingerprint: {
        source: String((saved.fingerprint && saved.fingerprint.source) || ''),
        target: String((saved.fingerprint && saved.fingerprint.target) || ''),
      },
      strategy: one(saved.strategy, ['target', 'source', 'explicit'], 'target'),
      explicitCollation: saved.explicitCollation || null,
      resolvedCollation: String(saved.resolvedCollation || ''),
      tempTables: one(saved.tempTables, ['resolved', 'database_default'], 'resolved'),
      generatedSqlMode: one(saved.generatedSqlMode, ['apply', 'warn'], 'apply'),
      userSqlMode: one(saved.userSqlMode, ['offer_fix', 'warn'], 'offer_fix'),
      caseRule: one(saved.caseRule, ['warn', 'block'], 'warn'),
      codePageRule: one(saved.codePageRule, ['warn', 'block'], 'warn'),
      columnOverrides: (saved.columnOverrides && typeof saved.columnOverrides === 'object' && !Array.isArray(saved.columnOverrides)) ? saved.columnOverrides : {},
      acknowledged: Array.isArray(saved.acknowledged) ? saved.acknowledged.slice() : [],
      lastScan: saved.lastScan || null,
    };
  }

  /* ── The grading. A pure function, because every severity rule in the
        brief is a claim that has to be testable without a database. ──────
     `pairs` is one entry per mapped column pair:
       { job, usedBy:[ids], src:{schema,table,column,dataType,collation,codePage,inUniqueKey,maxLength},
                            tgt:{ …same… } }
     Findings come back most serious first, each with a stable id so an
     acknowledgement survives a re-scan.                                   */
  var ISSUE = {
    CODEPAGE: 'codepage',
    CASE_UNIQUE: 'case_unique',
    MISMATCH: 'mismatch',
    TEMPDB: 'tempdb',
    UTF8: 'utf8',
    ACCENT: 'accent',
  };
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
  function store() {
    try { return root.CygenixProfiles && root.CygenixProfiles.cpLoad ? root.CygenixProfiles.cpLoad() : null; } catch (e) { return null; }
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
     card works before anybody has adopted profiles. */
  function connectionsFor(profile) {
    var out = { src: '', tgt: '' };
    var P = root.CygenixProfiles;
    if (profile && P && P.cpConnValue) {
      var byId = {};
      savedConns().forEach(function (c) { byId[c.id] = c; });
      if (byId[profile.srcConnId]) out.src = P.cpConnValue(byId[profile.srcConnId]) || '';
      if (byId[profile.tgtConnId]) out.tgt = P.cpConnValue(byId[profile.tgtConnId]) || '';
    }
    if (!out.src && typeof root.impGetConn === 'function') { try { out.src = root.impGetConn('src') || ''; } catch (e) { /* ignore */ } }
    if (!out.tgt && typeof root.impGetConn === 'function') { try { out.tgt = root.impGetConn('tgt') || ''; } catch (e) { /* ignore */ } }
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
      for (var i = 0; i < sides.length; i++) {
        var side = sides[i];
        var conn = state.conns[side];
        var field = side === 'src' ? 'source' : 'target';
        if (!conn) { state.model[field] = sideDefaults(); continue; }
        if (!isSqlServer(state.engines[side])) { state.model[field] = sideDefaults(); continue; }
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
      }
      state.model.resolvedCollation = resolvedCollation(state.model);
      state.dirty = true;
      state.note = 'Collations detected. Run Scan to grade the mapped columns, then Save to profile.';
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
      state.model.lastScan = {
        at: new Date().toISOString(),
        summary: { high: summary.high, medium: summary.medium, low: summary.low },
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
    if (!conn) return h + '<div class="col-empty">No ' + label.toLowerCase() + ' connection is configured.</div></div>';
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
    var bothSql = isSqlServer(state.engines.src) && isSqlServer(state.engines.tgt);
    var anyConn = state.conns.src || state.conns.tgt;

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

    if (!anyConn) {
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

    if (anyConn && bothSql) {
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
    isSqlServer: isSqlServer, labelsFor: labelsFor,
    _state: state,
  };
});
