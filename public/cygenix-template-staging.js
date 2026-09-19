/* ============================================================================
   cygenix-template-staging.js — building the client's staging tables, one
   CREATE TABLE at a time, on a connection the operator picks
   ----------------------------------------------------------------------------
   Sep-2026. The Conversion Template already knows every staging table and,
   once the columns have been read, every column of every one of them. Until
   now the only way to turn that into a database was to download the .sql file
   and run it by hand. This runs it — carefully.

   WHAT IT WILL NEVER DO
   Create. That is the whole list. There is no DROP here, no TRUNCATE, no
   ALTER, no INSERT and no DELETE, and `assertCreateOnly` reads every statement
   this file produces before it is allowed near a connection — not as a
   formality, but because this runs against a database somebody else owns and
   the difference between "the table was already there" and "the table was
   already there and had rows in it" is a client's week of work. An existing
   table is SKIPPED, whatever is in it, whatever shape it is. If the existing
   table is the wrong shape, that is a fact for a person to look at, not
   something to fix by dropping it.

   ONE TABLE PER CALL
   Netlify cuts a function off at 26 seconds. A hundred CREATE TABLEs in one
   statement is one call that either finishes inside 26 seconds or leaves the
   operator with no idea which tables exist — the worst possible answer for
   something you are about to re-run. So each table is its own call, each one
   reports created / skipped / failed with the server's own words, and a run
   that dies half way through has still genuinely created the tables it says
   it created. Re-running it skips those and carries on.

   TARGET-AGNOSTIC
   Nothing here knows a module, a table or a column of any particular system.
   The names come from the template; the types come from the column snapshot
   the template stores; the dialect comes from the connection the operator
   picked. Two dialects are generated, SQL Server and PostgreSQL, and a type
   this file has never heard of is passed through as written and reported as
   unrecognised rather than quietly turned into something else.

   THE GUARDS, in one place:
     · `_running` is the in-flight flag. Set before the first await of the
       call that sets it, cleared in that same call's finally. Never by a
       progress callback, never by a later call.
     · `_lastRunAt` enforces the three-second minimum gap between runs.
   Neither is reset anywhere else in this file.

   No DOM, no storage, no direct fetch: the caller hands in an `execute(sql)`
   function, which on the page is CygenixSchemaGraph.execute bound to the
   chosen side. That keeps this file testable in Node and keeps every call on
   the one existing route (db-connect / the Azure relay, `action: 'execute'`),
   which is already RBAC-classified — a CREATE TABLE classifies as sql.write
   and a PROD connection refuses it for most roles. No new backend route.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixTemplateStaging) root.CygenixTemplateStaging = api;
})(typeof globalThis !== 'undefined' ? globalThis : (typeof window !== 'undefined' ? window : this), function () {
'use strict';

var RUN_MIN_INTERVAL_MS = 3000;

function str(v) { return v == null ? '' : String(v); }
function trim(v) { return str(v).trim(); }
function lower(v) { return trim(v).toLowerCase(); }

/* ── Which module a table belongs to ──────────────────────────────────────
   The same rule as CygenixTemplateModel.tmModuleActive and
   CygenixTemplateSpec.moduleActive: in the Configurator scope AND ticked as
   included. Restated for the same reason it is restated there — this file has
   no dependencies — and kept honest by tests/template-controls.test.js, which
   asserts all three agree. */
function moduleActive(m) { return !!m && m.inScope !== false && !!m.included; }

/* ── Dialect ──────────────────────────────────────────────────────────────
   Read from the connection VALUE, which is either a connection string or an
   Azure Function URL. A Function URL is the relay in front of a SQL Server,
   so it is mssql; a postgres:// or postgresql:// URL is postgres; anything
   else is mssql, which is what every other connection string in this product
   has been. The caller may override when it knows better. */
function dialectOf(connValue) {
  var v = trim(connValue);
  if (/^postgres(ql)?:\/\//i.test(v)) return 'postgres';
  return 'mssql';
}

/* ── Identifiers ──────────────────────────────────────────────────────────
   Quoted, never interpolated bare. SQL Server doubles a ] inside [ ];
   PostgreSQL doubles a " inside " ". These are the only escapes either
   dialect needs for an identifier, and a name that arrives with a quote
   character in it is a name, not an injection — quoting it correctly is both
   the safe answer and the right one. */
function quoteIdent(name, dialect) {
  var s = str(name);
  if (dialect === 'postgres') return '"' + s.replace(/"/g, '""') + '"';
  return '[' + s.replace(/]/g, ']]') + ']';
}
function quoteLiteral(v) { return "'" + str(v).replace(/'/g, "''") + "'"; }

/* ── Types ────────────────────────────────────────────────────────────────
   A column snapshot carries the type as the TARGET database spells it. The
   staging database may be a different engine, so the type is translated for
   the dialect being written. Only the families that take parameters get
   them: an INT(10,0) is what happens when that rule is not written down.

   A type neither table knows is passed through verbatim and reported in
   `unknownTypes`. Passing it through is deliberate: it is what the target
   actually said, it may well be valid, and if it is not, the CREATE for that
   one table fails with the server's own message and the other tables carry
   on. Substituting a guess would produce a staging table that loads and then
   silently mangles the data, which is far worse than a visible failure. */
var MSSQL_LENGTH_TYPES = ['char', 'varchar', 'nchar', 'nvarchar', 'binary', 'varbinary'];
var MSSQL_PRECISION_SCALE = ['decimal', 'numeric', 'dec'];
var MSSQL_PRECISION_ONLY = ['datetime2', 'datetimeoffset', 'time'];

// Everything either engine can name, so an unknown type really is unknown.
var MSSQL_KNOWN = MSSQL_LENGTH_TYPES.concat(MSSQL_PRECISION_SCALE, MSSQL_PRECISION_ONLY,
  ['int', 'bigint', 'smallint', 'tinyint', 'bit', 'float', 'real', 'money', 'smallmoney',
    'date', 'datetime', 'smalldatetime', 'uniqueidentifier', 'text', 'ntext', 'image',
    'xml', 'sql_variant', 'rowversion', 'timestamp', 'geography', 'geometry', 'hierarchyid']);

var PG_KNOWN = ['integer', 'int', 'int4', 'int2', 'int8', 'bigint', 'smallint', 'serial', 'bigserial',
  'boolean', 'bool', 'text', 'varchar', 'character varying', 'character', 'char', 'bpchar',
  'numeric', 'decimal', 'real', 'float4', 'double precision', 'float8', 'money',
  'date', 'time', 'time without time zone', 'time with time zone', 'timetz',
  'timestamp', 'timestamp without time zone', 'timestamp with time zone', 'timestamptz',
  'uuid', 'bytea', 'json', 'jsonb', 'xml', 'interval', 'inet', 'cidr', 'macaddr'];

/* mssql name → postgres name. Only where the two genuinely differ. */
var MSSQL_TO_PG = {
  'int': 'integer', 'tinyint': 'smallint', 'bit': 'boolean',
  'datetime': 'timestamp', 'smalldatetime': 'timestamp', 'datetime2': 'timestamp',
  'datetimeoffset': 'timestamptz', 'money': 'numeric(19,4)', 'smallmoney': 'numeric(10,4)',
  'float': 'double precision', 'uniqueidentifier': 'uuid',
  'nvarchar': 'varchar', 'nchar': 'char', 'ntext': 'text', 'text': 'text',
  'binary': 'bytea', 'varbinary': 'bytea', 'image': 'bytea',
  'sql_variant': 'text', 'rowversion': 'bytea', 'hierarchyid': 'text',
  'geography': 'text', 'geometry': 'text',
};

/* postgres name → mssql name. */
var PG_TO_MSSQL = {
  'integer': 'int', 'int4': 'int', 'int2': 'smallint', 'int8': 'bigint',
  'serial': 'int', 'bigserial': 'bigint', 'boolean': 'bit', 'bool': 'bit',
  'text': 'nvarchar(max)', 'character varying': 'nvarchar', 'varchar': 'nvarchar',
  'character': 'nchar', 'bpchar': 'nchar', 'char': 'nchar',
  'double precision': 'float', 'float8': 'float', 'float4': 'real',
  'timestamp': 'datetime2', 'timestamp without time zone': 'datetime2',
  'timestamp with time zone': 'datetimeoffset', 'timestamptz': 'datetimeoffset',
  'time without time zone': 'time', 'time with time zone': 'time', 'timetz': 'time',
  'uuid': 'uniqueidentifier', 'bytea': 'varbinary(max)',
  'json': 'nvarchar(max)', 'jsonb': 'nvarchar(max)',
  'interval': 'nvarchar(50)', 'inet': 'nvarchar(50)', 'cidr': 'nvarchar(50)', 'macaddr': 'nvarchar(50)',
};

function baseTypeOf(col) {
  var c = col || {};
  var t = lower(c.baseType || c.dataType || c.type);
  var paren = t.indexOf('(');
  return { name: paren > 0 ? t.slice(0, paren).trim() : t, assembled: paren > 0 ? t : '' };
}

/* The declaration for one column in one dialect, plus whether the type was
   recognised. `known:false` means "passed through as the target spelled it". */
function typeFor(col, dialect) {
  var c = col || {};
  var b = baseTypeOf(c);
  if (!b.name) return { sql: '', known: false };
  var pg = dialect === 'postgres';

  // Already assembled by the reader, e.g. "NVARCHAR(200)". Translate the head
  // if it needs translating; otherwise it is ready as it stands.
  var name = b.name;
  var known = pg ? (PG_KNOWN.indexOf(name) !== -1 || MSSQL_KNOWN.indexOf(name) !== -1)
    : (MSSQL_KNOWN.indexOf(name) !== -1 || PG_KNOWN.indexOf(name) !== -1);

  var mapped = pg
    ? (MSSQL_TO_PG[name] || (PG_KNOWN.indexOf(name) !== -1 ? name : null))
    : (PG_TO_MSSQL[name] || (MSSQL_KNOWN.indexOf(name) !== -1 ? name : null));
  if (mapped == null) return { sql: b.assembled || name, known: false };

  // A mapping that already carries its own parameters (money → numeric(19,4),
  // text → nvarchar(max)) is complete; adding a length would break it.
  if (mapped.indexOf('(') !== -1) return { sql: mapped, known: known };

  var len = c.maxLength == null ? null : Number(c.maxLength);
  var prec = c.precision == null ? null : Number(c.precision);
  var scale = c.scale == null ? null : Number(c.scale);

  // Length families. -1 is MAX in SQL Server; PostgreSQL has no MAX, and the
  // honest equivalent of "as long as you like" is text.
  var lengthFamily = ['char', 'varchar', 'nchar', 'nvarchar', 'binary', 'varbinary', 'bpchar', 'character', 'character varying'];
  if (lengthFamily.indexOf(mapped) !== -1) {
    if (len === -1 || len == null) return { sql: pg ? 'text' : mapped + '(max)', known: known };
    var chars = len;
    // nvarchar/nchar report bytes; the declaration is in characters.
    if (name === 'nvarchar' || name === 'nchar' || name === 'ntext') chars = Math.floor(len / 2) || len;
    if (chars <= 0) return { sql: pg ? 'text' : mapped + '(max)', known: known };
    return { sql: mapped + '(' + chars + ')', known: known };
  }
  if (['decimal', 'numeric', 'dec'].indexOf(mapped) !== -1 && prec != null) {
    return { sql: (pg ? 'numeric' : mapped) + '(' + prec + ',' + (scale == null ? 0 : scale) + ')', known: known };
  }
  if (!pg && MSSQL_PRECISION_ONLY.indexOf(mapped) !== -1 && scale != null) {
    return { sql: mapped + '(' + scale + ')', known: known };
  }
  if (b.assembled && mapped === name) return { sql: b.assembled, known: known };
  return { sql: mapped, known: known };
}

/* ── Safety ───────────────────────────────────────────────────────────────
   Read before anything is sent. It is not a substitute for building the SQL
   correctly — it is the check that the building stayed correct after somebody
   edited this file a year from now. Comments are stripped first so the prose
   above a statement cannot trip it, and so a comment cannot hide a statement
   from it either. */
var FORBIDDEN = /\b(drop|truncate|alter|delete|insert|update|merge|grant|revoke|exec|execute|sp_\w+|xp_\w+)\b/i;

/* Comments, string literals and QUOTED IDENTIFIERS all come out before the
   check looks at anything. Comments so the prose above a statement cannot
   trip it and cannot hide a statement from it. Quoted identifiers because a
   target system is perfectly entitled to a column called [Update] or a table
   called "Delete", and refusing to build a client's staging database over
   somebody else's column name would be a bug, not caution. What is left is
   the SQL's own keywords, which is exactly what this is asking about. */
function bareStatement(sql) {
  return str(sql)
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/--[^\n\r]*/g, ' ')
    .replace(/\[(?:[^\]]|\]\])*\]/g, ' "id" ')
    .replace(/"(?:[^"]|"")*"/g, ' "id" ')
    .replace(/'(?:[^']|'')*'/g, " 'lit' ");
}
function assertCreateOnly(sql) {
  var bare = bareStatement(sql);
  if (FORBIDDEN.test(bare)) {
    throw new Error('Refusing to run: this statement is not create-only. Nothing was sent.');
  }
  if (!/\bcreate\s+table\b/i.test(bare)) {
    throw new Error('Refusing to run: no CREATE TABLE in this statement. Nothing was sent.');
  }
  return true;
}

/* ── The statements ─────────────────────────────────────────────────────── */

/* One table. Guarded in the statement itself as well as by the plan's
   existence scan, because between the scan and the run somebody else may
   have created it — and the guard means that race ends in "already there",
   not in an error and not in a clobbered table.

   Every column is NULLABLE, with no keys, no indexes, no defaults, no
   identity and no computed columns, for the same reason the downloadable DDL
   says so: staging is a landing zone, and a NOT NULL here makes the client's
   first partial load fail at the door. */
function createTableSql(t, opts) {
  var o = opts || {};
  var dialect = o.dialect === 'postgres' ? 'postgres' : 'mssql';
  var schema = trim(o.schema) || (dialect === 'postgres' ? 'public' : 'dbo');
  var cols = (t.columns || []).filter(function (c) { return !c.isComputed; });
  if (!cols.length) return '';
  var nl = o.nl || '\n';
  var full = quoteIdent(schema, dialect) + '.' + quoteIdent(t.stagingTable, dialect);
  /* Collation (Stage C). `collateFor` is supplied by the caller — this
     module stays pure and testable, and the page decides from the saved
     profile. It returns a suffix (' COLLATE X') or '', so a profile with
     nothing to neutralise produces byte-identical DDL to before.
     SQL Server only: PostgreSQL spells collation differently and this
     feature does not claim to support it. */
  var collateFor = (dialect === 'mssql' && typeof o.collateFor === 'function')
    ? o.collateFor : function () { return ''; };
  var marker = o.marker || '';
  var body = cols.map(function (c, i) {
    var cl = collateFor(c) || '';
    return '    ' + quoteIdent(c.name, dialect) + ' ' + typeFor(c, dialect).sql + cl + ' NULL'
      + (i < cols.length - 1 ? ',' : '') + (cl && marker ? '  ' + marker : '');
  }).join(nl);

  if (dialect === 'postgres') {
    return 'CREATE TABLE IF NOT EXISTS ' + full + ' (' + nl + body + nl + ');';
  }
  return "IF OBJECT_ID(N'" + str(schema).replace(/'/g, "''") + '.' + str(t.stagingTable).replace(/'/g, "''") + "', N'U') IS NULL" + nl
    + 'CREATE TABLE ' + full + ' (' + nl + body + nl + ');';
}

/* The existence scan: ONE read for the whole run, listing what is already in
   the staging schema, so the review can say "exists — will skip" before the
   operator confirms rather than discovering it table by table afterwards. */
function existingTablesSql(opts) {
  var o = opts || {};
  var dialect = o.dialect === 'postgres' ? 'postgres' : 'mssql';
  var schema = trim(o.schema) || (dialect === 'postgres' ? 'public' : 'dbo');
  if (dialect === 'postgres') {
    return "SELECT table_name AS name FROM information_schema.tables WHERE table_schema = " + quoteLiteral(schema);
  }
  return 'SELECT t.name AS name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id'
    + ' WHERE s.name = N' + quoteLiteral(schema);
}

function namesFromResult(res) {
  var rows = (res && (res.recordset || res.rows)) || [];
  var out = {};
  rows.forEach(function (r) {
    var n = r && (r.name !== undefined ? r.name : (r.NAME !== undefined ? r.NAME : r.table_name));
    if (n) out[lower(n)] = true;
  });
  return out;
}

/* ── The plan ─────────────────────────────────────────────────────────────
   Every staging table this run would create, in load order, with its SQL and
   its verdict. Pure: the existence map is passed in, not fetched. A table
   with no column snapshot cannot be created and says so — it is not silently
   dropped from the list, because the operator needs to know the staging
   database they are about to hand over has a hole in it. */
function stagingPlan(tpl, opts) {
  var o = opts || {};
  var dialect = o.dialect === 'postgres' ? 'postgres' : 'mssql';
  var schema = trim(o.schema) || (dialect === 'postgres' ? 'public' : 'dbo');
  var existing = o.existing || null;          // { lowername: true } or null = not scanned
  var rows = [];
  var unknownTypes = [];

  ((tpl && tpl.modules) || []).forEach(function (m) {
    if (!moduleActive(m)) return;
    (m.tables || []).forEach(function (t) {
      if (o.requiredOnly && t.required === false) return;
      var cols = Array.isArray(t.columns) ? t.columns : [];
      var creatable = cols.filter(function (c) { return !c.isComputed; });
      var row = {
        module: m.module,
        targetTable: str(t.targetTable),
        stagingTable: str(t.stagingTable),
        schema: schema,
        loadOrder: Number(t.loadOrder) || 0,
        columnCount: creatable.length,
        columns: cols,
        status: 'pending',       // pending | exists | no-columns
        sql: '',
      };
      if (!creatable.length) {
        row.status = 'no-columns';
      } else if (existing && existing[lower(t.stagingTable)]) {
        row.status = 'exists';
        row.sql = createTableSql({ stagingTable: t.stagingTable, columns: cols }, { dialect: dialect, schema: schema, collateFor: o.collateFor, marker: o.marker });
      } else {
        row.sql = createTableSql({ stagingTable: t.stagingTable, columns: cols }, { dialect: dialect, schema: schema, collateFor: o.collateFor, marker: o.marker });
      }
      creatable.forEach(function (c) {
        if (!typeFor(c, dialect).known) {
          unknownTypes.push({ module: m.module, stagingTable: str(t.stagingTable), column: str(c.name),
            dataType: str(c.baseType || c.dataType || c.type) });
        }
      });
      rows.push(row);
    });
  });

  rows.sort(function (a, b) {
    return (a.loadOrder || 0) - (b.loadOrder || 0)
      || String(a.module).localeCompare(String(b.module))
      || String(a.stagingTable).localeCompare(String(b.stagingTable));
  });

  return {
    dialect: dialect, schema: schema, tables: rows,
    toCreate: rows.filter(function (r) { return r.status === 'pending'; }).length,
    toSkip: rows.filter(function (r) { return r.status === 'exists'; }).length,
    noColumns: rows.filter(function (r) { return r.status === 'no-columns'; }).length,
    scanned: !!existing,
    unknownTypes: unknownTypes,
  };
}

/* The whole thing as one reviewable, copyable, downloadable script. What the
   operator sees here is exactly the text that will be sent, statement for
   statement — with the tables that will be skipped included and commented so
   the script is still complete and still safe to run by hand. */
function planScript(plan, opts) {
  var o = opts || {};
  var now = o.now instanceof Date ? o.now : new Date();
  var nl = '\r\n';
  var out = [];
  out.push('/* =========================================================================');
  out.push('   Staging tables for: ' + str(o.templateName));
  out.push('   Version:        v' + (o.version || 1));
  out.push('   Connection:     ' + str(o.connectionLabel || 'the selected connection'));
  out.push('   Dialect:        ' + (plan.dialect === 'postgres' ? 'PostgreSQL' : 'SQL Server'));
  out.push('   Schema:         ' + plan.schema);
  out.push('   Generated:      ' + now.toISOString() + (o.by ? ' by ' + str(o.by) : ''));
  out.push('   To create:      ' + plan.toCreate);
  out.push('   Already there:  ' + plan.toSkip + ' (skipped, never altered)');
  if (plan.noColumns) out.push('   No columns yet:  ' + plan.noColumns + ' (cannot be created — read the columns first)');
  out.push('');
  out.push('   Every column is NULLABLE and there are no keys, indexes, defaults,');
  out.push('   identity or computed columns. Staging is a landing zone.');
  out.push('');
  out.push('   Create only. Nothing here drops, truncates or alters anything, and an');
  out.push('   existing table is left exactly as it is, contents included.');
  out.push('   ========================================================================= */');
  /* Collation (Stage C). One line, only when the script actually carries
     fixes, so a script with nothing to say reads exactly as it did before
     this feature existed. The caller supplies it; this module stays pure. */
  if (o.collationHeader) { out.push(''); out.push(str(o.collationHeader)); }
  out.push('');
  plan.tables.forEach(function (t) {
    var head = '/* ' + t.module + ' · ' + t.targetTable + (t.loadOrder ? ' · load order ' + t.loadOrder : '');
    if (t.status === 'exists') {
      out.push(head + ' · ALREADY EXISTS, will be skipped */');
      out.push(str(t.sql).split('\n').map(function (l) { return '-- ' + l; }).join(nl));
      out.push('');
      return;
    }
    if (t.status === 'no-columns') {
      out.push(head + ' · no column detail — run Refresh columns first */');
      out.push('');
      return;
    }
    out.push(head + ' */');
    out.push(str(t.sql).split('\n').join(nl));
    out.push('');
  });
  return out.join(nl);
}

/* ── Running it ───────────────────────────────────────────────────────────
   One table, one call. `execute` is supplied by the caller and is the page's
   existing route; this file never builds a URL. Progress is reported per
   table as it happens, because a run of eighty tables that says nothing for
   a minute is a run the operator will reload the page in the middle of. */
var _running = false;
var _lastRunAt = 0;

function runState() { return { busy: _running, lastAt: _lastRunAt }; }
function _resetRunStateForTests() { _running = false; _lastRunAt = 0; }

async function scanExisting(execute, opts) {
  var sql = existingTablesSql(opts);
  var res = await execute(sql);
  return namesFromResult(res);
}

async function runPlan(plan, opts) {
  var o = opts || {};
  var execute = o.execute;
  if (typeof execute !== 'function') return { ok: false, reason: 'No way to run SQL on this page.' };
  if (_running) return { ok: false, reason: 'Already creating tables — one moment.' };
  var now = o.now || Date.now();
  if (now - _lastRunAt < RUN_MIN_INTERVAL_MS) {
    return { ok: false, reason: 'Please wait a moment before running this again.' };
  }
  var todo = (plan && plan.tables || []).filter(function (t) { return t.status === 'pending'; });
  if (!todo.length) return { ok: false, reason: 'Nothing to create — every table either exists already or has no column detail.' };

  _running = true;
  _lastRunAt = now;
  var results = [];
  var created = 0, skipped = 0, failed = 0;
  try {
    // Pre-flight: read every statement BEFORE sending any of them, so a plan
    // with one bad statement in it is refused whole rather than half-run.
    todo.forEach(function (t) { assertCreateOnly(t.sql); });

    for (var i = 0; i < todo.length; i++) {
      var t = todo[i];
      var outcome = { module: t.module, stagingTable: t.stagingTable, targetTable: t.targetTable,
        status: 'created', error: '', hint: '' };
      try {
        await execute(t.sql);
        created++;
      } catch (e) {
        // "already exists" is not a failure. Both guards should have caught
        // it, but a concurrent creation lands here and it is a skip.
        var msg = (e && e.message) || 'could not create';
        if (/already exists|there is already an object named/i.test(msg)) { outcome.status = 'skipped'; skipped++; }
        else {
          outcome.status = 'failed';
          outcome.error = msg;
          // The server's advice, kept apart from its complaint. A refusal
          // reads the same on every row; what to do about it is worth saying
          // once, and the caller shows it once.
          outcome.hint = (e && e.hint) || '';
          failed++;
        }
      }
      t.status = outcome.status === 'failed' ? 'failed' : 'done';
      t.error = outcome.error;
      results.push(outcome);
      if (o.onProgress) {
        try { o.onProgress({ done: results.length, total: todo.length, table: t.stagingTable, outcome: outcome }); }
        catch (e2) { /* the run matters, the progress line does not */ }
      }
    }
    var hints = [];
    results.forEach(function (r) { if (r.hint && hints.indexOf(r.hint) === -1) hints.push(r.hint); });
    return { ok: true, created: created, skipped: skipped, failed: failed,
      results: results, total: todo.length, hints: hints };
  } catch (e) {
    return { ok: false, reason: (e && e.message) || 'The tables could not be created.',
      created: created, skipped: skipped, failed: failed, results: results };
  } finally {
    _running = false;
  }
}

return {
  RUN_MIN_INTERVAL_MS: RUN_MIN_INTERVAL_MS,
  moduleActive: moduleActive,
  dialectOf: dialectOf,
  quoteIdent: quoteIdent,
  typeFor: typeFor,
  assertCreateOnly: assertCreateOnly,
  bareStatement: bareStatement,
  createTableSql: createTableSql,
  existingTablesSql: existingTablesSql,
  namesFromResult: namesFromResult,
  stagingPlan: stagingPlan,
  planScript: planScript,
  scanExisting: scanExisting,
  runPlan: runPlan,
  runState: runState,
  _resetRunStateForTests: _resetRunStateForTests,
};
});
