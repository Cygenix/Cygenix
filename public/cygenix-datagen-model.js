/* ============================================================================
   cygenix-datagen-model.js — reading a real schema and working out how to fill
   it with believable, correctly-linked sample data
   ----------------------------------------------------------------------------
   Sep-2026, Phase 1. The Data Generator used to create four fixed demo tables
   — customers, addresses, products, orders — and fill those. Useful for a
   sales demo, useless for testing a migration script, which has to run against
   the shape of the actual source. This turns it round: the tables already
   exist, the schema is read from the database, and the generator's job is to
   produce rows that the database will accept.

   "The database will accept" is the whole difficulty, and it is what this file
   is about:

     · a row cannot set an identity, computed or rowversion column;
     · a value cannot be longer than the column, or have more decimal places;
     · a NOT NULL column needs a value, a nullable one usually should not
       always have one;
     · a PK or unique column may not repeat — including against rows that were
       already in the table before this run;
     · an FK must point at a key that really exists;
     · and children have to be inserted after their parents, which means
       knowing the order, which means knowing the graph.

   WHAT IS PURE AND WHAT IS NOT
   Everything here is pure: no DOM, no network, no storage, no clock it does
   not take as an argument. It does not even own the value generators — the
   page has had a good set of name-pattern rules and generators since the
   original demo build, and those are passed IN rather than copied, which is
   both the instruction and the right call: one set of rules about what an
   "email" column should contain, not two that drift.

   TARGET-AGNOSTIC. Nothing here knows a table, a column or a product. Every
   name comes from the database the user connected to.

   WHY NOT REUSE CygenixSchemaGraph.loadOrder()
   It does the same topological sort, correctly, and it is tested. It is also
   browser-only — it assigns to `window` and reads localStorage at module
   scope, so it cannot be required in Node, and the ordering here has to be
   testable without a browser. It also has to answer a question loadOrder does
   not: for a table caught in a cycle, WHICH nullable FK column can be left
   null on insert and filled in afterwards. The sort is thirty lines; the
   answer to that question is the reason this one exists.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixDataGenModel) root.CygenixDataGenModel = api;
})(typeof globalThis !== 'undefined' ? globalThis : (typeof window !== 'undefined' ? window : this), function () {
'use strict';

function str(v) { return v == null ? '' : String(v); }
function trim(v) { return str(v).trim(); }
function lower(v) { return trim(v).toLowerCase(); }

/* A table's identity through this whole file: schema and name, lower-cased,
   because SQL Server compares them case-insensitively by default and two
   spellings of one table would order it twice. */
function dgKey(schema, name) { return lower(schema || 'dbo') + '.' + lower(name); }
function dgSplitKey(key) {
  var i = str(key).indexOf('.');
  return i < 0 ? { schema: '', name: str(key) } : { schema: key.slice(0, i), name: key.slice(i + 1) };
}

/* ── Columns the generator must not write ─────────────────────────────────
   Three different reasons, all ending in "the database decides this, not us".
   They are named separately because the UI says WHY a column is greyed out,
   and "skipped" on its own invites someone to go looking for a bug. */
var SKIP = {
  IDENTITY:   'identity — the database assigns it',
  COMPUTED:   'computed — the database derives it',
  ROWVERSION: 'rowversion — the database stamps it',
};

// Types that are stamped by the engine and rejected on insert.
var ROWVERSION_TYPES = ['rowversion', 'timestamp'];

/* Postgres calls its timestamp type `timestamp`, and it is an ordinary
   date-time column that very much does want a value. SQL Server's `timestamp`
   is a rowversion and refuses one. Same word, opposite meaning, so the
   dialect has to be part of the question — getting this wrong either skips a
   real date column or fails every insert on a versioned table. */
function dgIsRowversion(col, dialect) {
  var t = lower(col && (col.baseType || col.type));
  if (dialect === 'postgres') return t === 'rowversion';
  return ROWVERSION_TYPES.indexOf(t) !== -1;
}

function dgSkipReason(col, dialect) {
  if (!col) return null;
  if (col.isIdentity) return SKIP.IDENTITY;
  if (col.isComputed) return SKIP.COMPUTED;
  if (dgIsRowversion(col, dialect)) return SKIP.ROWVERSION;
  return null;
}

/* ── The foreign-key graph ────────────────────────────────────────────────
   Built from the whole-database `schema-fks` read, which returns ONE ROW PER
   COLUMN PAIR. A composite key arrives as several rows and only the
   constraint name says they belong together — so they are grouped by name.
   Treating them as separate keys would let a child be linked to one parent
   for its first column and a different one for its second, which is a row no
   database would have produced and no migration script should be tested on. */
function dgBuildGraph(fkRows) {
  var byName = new Map();
  (fkRows || []).forEach(function (r) {
    if (!r || !r.fromTable || !r.toTable) return;
    var from = dgKey(r.fromSchema, r.fromTable);
    var to = dgKey(r.toSchema, r.toTable);
    // A key with no name of its own still has to group: the pair of tables is
    // the next best grouping, and a database that omits constraint names has
    // at most one anonymous key between any two tables in practice.
    var id = trim(r.name) || (from + '->' + to);
    if (!byName.has(id)) {
      byName.set(id, { name: trim(r.name), from: from, to: to, self: from === to, columns: [] });
    }
    byName.get(id).columns.push({ from: str(r.fromColumn), to: str(r.toColumn) });
  });

  var fks = [...byName.values()];
  var out = new Map();   // key -> fks where this table is the CHILD
  var incoming = new Map();
  fks.forEach(function (fk) {
    if (!out.has(fk.from)) out.set(fk.from, []);
    out.get(fk.from).push(fk);
    if (!incoming.has(fk.to)) incoming.set(fk.to, []);
    incoming.get(fk.to).push(fk);
  });

  return {
    fks: fks,
    fksOf: function (key) { return out.get(key) || []; },
    childrenOf: function (key) { return incoming.get(key) || []; },
    parentsOf: function (key) {
      return [...new Set((out.get(key) || []).filter(function (f) { return !f.self; })
        .map(function (f) { return f.to; }))];
    },
  };
}

/* Every table the chosen ones depend on, however far up. Returned with the
   originals so the caller can say which were added rather than silently
   growing the selection — "Also include parent tables" is a tick box, and a
   tick box that cannot be explained is a tick box nobody trusts. */
function dgParentClosure(graph, keys) {
  var want = new Set(keys || []);
  var added = [];
  var queue = [...want];
  var guard = 0;
  while (queue.length && guard++ < 10000) {
    var k = queue.shift();
    graph.parentsOf(k).forEach(function (p) {
      if (want.has(p)) return;
      want.add(p); added.push(p); queue.push(p);
    });
  }
  return { keys: [...want], added: added };
}

/* ── Insert order ─────────────────────────────────────────────────────────
   Kahn's algorithm: repeatedly take everything with no remaining unsatisfied
   parent. What comes out is a list of WAVES — each wave can be inserted in
   any order, or in parallel — and, if the graph has a cycle, whatever is left
   over when nothing can move.

   Self-references do not order anything: a table is not waiting for itself,
   it is waiting for rows of itself, which is a within-table problem and is
   solved by inserting the FK null and updating afterwards.

   For each table stranded in a cycle the report says whether that break is
   available: a nullable FK column it can be inserted without. If there is
   none, the table genuinely cannot be inserted without violating a
   constraint, and the honest answer is to name it and skip it rather than
   fail the whole run — which is what the caller does with `unbreakable`. */
function dgOrder(graph, keys, columnsByKey) {
  var wanted = new Set(keys || []);
  var indegree = {}, dependents = {};
  wanted.forEach(function (k) { indegree[k] = 0; dependents[k] = []; });

  graph.fks.forEach(function (fk) {
    if (fk.self) return;
    if (!wanted.has(fk.from) || !wanted.has(fk.to)) return;
    // Two keys between the same pair of tables must not count twice.
    if (dependents[fk.to].indexOf(fk.from) !== -1) return;
    dependents[fk.to].push(fk.from);
    indegree[fk.from]++;
  });

  var waves = [], placed = new Set();
  var frontier = [...wanted].filter(function (k) { return indegree[k] === 0; }).sort();
  while (frontier.length) {
    waves.push(frontier.slice());
    frontier.forEach(function (k) { placed.add(k); });
    var next = [];
    waves[waves.length - 1].forEach(function (k) {
      dependents[k].forEach(function (d) { if (--indegree[d] === 0) next.push(d); });
    });
    frontier = next.sort();
  }

  var cyclic = [...wanted].filter(function (k) { return !placed.has(k); }).sort();
  var breaks = [], unbreakable = [];
  cyclic.forEach(function (k) {
    var b = dgNullableBreak(graph, k, columnsByKey && columnsByKey[k], wanted);
    if (b) breaks.push(b); else unbreakable.push(k);
  });

  // Tables whose cycle CAN be broken still have to be inserted, after
  // everything that is not in a cycle. Their FK columns go in null and a
  // second pass links them.
  var order = waves.reduce(function (a, w) { return a.concat(w); }, [])
    .concat(breaks.map(function (b) { return b.table; }));

  // A self-reference needs the same two-pass treatment, even though it never
  // stops the table being ordered.
  [...wanted].forEach(function (k) {
    if (!placed.has(k)) return;
    var self = graph.fksOf(k).filter(function (f) { return f.self; });
    if (!self.length) return;
    var b = dgNullableBreak(graph, k, columnsByKey && columnsByKey[k], wanted, true);
    if (b && !breaks.some(function (x) { return x.table === k; })) breaks.push(b);
  });

  return { waves: waves, order: order, cyclic: cyclic, breaks: breaks, unbreakable: unbreakable };
}

/* Can this table be inserted with its FK columns left null and filled in
   afterwards? Only a NULLABLE column can, and a composite key can only be
   broken if EVERY one of its columns is nullable — half a key is not a legal
   row. Returns what to null and what to update later, or null for "no". */
function dgNullableBreak(graph, key, columns, wanted, selfOnly) {
  var byName = {};
  (columns || []).forEach(function (c) { byName[lower(c.name)] = c; });
  var usable = graph.fksOf(key).filter(function (fk) {
    if (selfOnly && !fk.self) return false;
    if (!fk.self && !wanted.has(fk.to)) return false;
    return fk.columns.every(function (p) {
      var col = byName[lower(p.from)];
      return col && col.nullable !== false;
    });
  });
  if (!usable.length) return null;
  var cols = [];
  usable.forEach(function (fk) { fk.columns.forEach(function (p) { cols.push(p.from); }); });
  return {
    table: key,
    nullFirst: [...new Set(cols)],
    linkAfter: usable.map(function (fk) {
      return { name: fk.name, to: fk.to, self: fk.self, columns: fk.columns };
    }),
  };
}

/* ── Row counts for parents nobody asked for ──────────────────────────────
   A parent added automatically needs a number, and the number should look
   like a real relationship. Five children per parent is the default because
   it reads as plausible — an order with five lines, a client with five
   matters — without making the parent as big as the child. The floor of ten
   stops "10 children" producing two parents, which makes the spread look
   broken when the point of the exercise is to see it. */
function dgDefaultParentRows(childRows, opts) {
  var o = opts || {};
  var per = o.childrenPerParent > 0 ? o.childrenPerParent : 5;
  var min = o.minimum > 0 ? o.minimum : 10;
  return Math.max(min, Math.ceil((Number(childRows) || 0) / per));
}

/* ── Planning one table ───────────────────────────────────────────────────
   Turns the raw introspection into the decisions: which columns are written,
   what generates each one, which must be unique, which point somewhere.
   Nothing is generated here — a plan is a thing the UI can show and a person
   can argue with before a single row exists. */
function dgPlanTable(meta, opts) {
  var o = opts || {};
  var dialect = o.dialect === 'postgres' ? 'postgres' : 'mssql';
  var infer = typeof o.inferColumnMeta === 'function' ? o.inferColumnMeta : function () { return {}; };
  var overrides = o.overrides || {};
  var key = dgKey(meta.schema, meta.name);
  var pk = (meta.primaryKeys || []).map(lower);
  var fkCols = {};
  (o.fks || []).forEach(function (fk) {
    fk.columns.forEach(function (p) { fkCols[lower(p.from)] = { to: fk.to, column: p.to, self: fk.self, name: fk.name }; });
  });

  // Single-column uniques are a per-value rule; composite ones are a rule
  // about the row. Kept apart because they are enforced differently and
  // muddling them makes a generator refuse legal data.
  var uniqueOne = {}, uniqueMany = [];
  (meta.uniques || []).forEach(function (u) {
    if (!u || !u.columns || !u.columns.length) return;
    if (u.columns.length === 1) uniqueOne[lower(u.columns[0])] = u.name || true;
    else uniqueMany.push({ name: u.name, columns: u.columns.map(lower) });
  });
  if (pk.length === 1) uniqueOne[pk[0]] = uniqueOne[pk[0]] || 'PRIMARY KEY';
  else if (pk.length > 1) uniqueMany.push({ name: 'PRIMARY KEY', columns: pk.slice() });

  var warnings = [];
  var columns = (meta.columns || []).map(function (c) {
    var name = str(c.name);
    var low = lower(name);
    var skip = dgSkipReason(c, dialect);
    var ov = overrides[low] || overrides[name] || null;
    var fk = fkCols[low] || null;

    var gen = null, source = '';
    if (skip) {
      source = 'skipped';
    } else if (ov && ov.mode) {
      gen = ov.mode; source = 'override';
    } else if (fk) {
      gen = 'fk'; source = 'foreign key';
    } else if (pk.indexOf(low) !== -1) {
      gen = 'unique'; source = 'primary key';
    } else {
      var guess = infer(name) || {};
      gen = guess.gen || 'shortText';
      source = 'inferred from the name';
    }

    /* A column the database can fill in itself, that we have no good guess
       for, is better left to the database. A generic random string in a
       column called `RegionCode` is worse than the default the DBA chose. */
    var leaveToDefault = !skip && !ov && !fk && trim(c.default) !== ''
      && source === 'inferred from the name' && gen === 'shortText';
    if (leaveToDefault) { source = 'left to the column default'; gen = null; }

    /* The TYPE has the last word over the NAME. Inferring from the column
       name is the only thing that can tell an `Email` from a `Postcode`, and
       it is right far more often than not — but it cannot know that
       `ClientRef` is a datetime, and it does not look at the type at all. A
       name-pattern generator pointed at a date column is how an entire
       table's insert fails at batch one: the generator hands over a postcode
       or an amount, and whatever dgCoerce can salvage from that is not a date
       anybody asked for. Where the two disagree about a DATE, the database
       wins, because the database is the one that will refuse the row. A user
       override is left alone — that is a choice, not a guess — and dgCoerce
       still clamps whatever it produces. */
    if (gen && gen !== 'fk' && source !== 'override' && gen !== 'datetime'
        && dgWantsDate(c.baseType || c.type)) {
      gen = 'datetime';
      source = 'inferred from the type';
    }

    if (!skip && !leaveToDefault && c.nullable === false && trim(c.default) === '' && gen === null) {
      warnings.push(name + ' is NOT NULL with no default and nothing to generate.');
    }

    return {
      name: name,
      type: str(c.type),
      baseType: lower(c.baseType || c.type),
      maxLength: c.maxLength == null ? null : Number(c.maxLength),
      precision: c.precision == null ? null : Number(c.precision),
      scale: c.scale == null ? null : Number(c.scale),
      nullable: c.nullable !== false,
      hasDefault: trim(c.default) !== '',
      isIdentity: !!c.isIdentity,
      isComputed: !!c.isComputed,
      ordinal: Number(c.ordinal) || 0,
      write: !skip && !leaveToDefault,
      skipReason: skip || (leaveToDefault ? 'has a column default and no better guess' : null),
      generator: gen,
      generatorSource: source,
      isPrimaryKey: pk.indexOf(low) !== -1,
      unique: uniqueOne[low] || null,
      fk: fk,
    };
  });

  if (!pk.length) warnings.push('No primary key — rows generated here cannot be linked to, or deleted by key afterwards.');

  return {
    key: key, schema: str(meta.schema), name: str(meta.name),
    columns: columns, primaryKeys: meta.primaryKeys || [],
    uniqueOne: uniqueOne, uniqueMany: uniqueMany,
    fks: o.fks || [], warnings: warnings,
    writable: columns.filter(function (c) { return c.write; }).length,
  };
}

/* ── Fitting a value to its column ────────────────────────────────────────
   The generators produce something plausible; this makes it something the
   column will actually take. Truncating a string is not a compromise — the
   alternative is an insert that fails on row 400 of 1,000 and takes the rest
   of the table with it. */
var TEXT_TYPES = ['char', 'varchar', 'nchar', 'nvarchar', 'text', 'ntext', 'character', 'character varying', 'bpchar'];
var INT_TYPES = ['int', 'integer', 'bigint', 'smallint', 'tinyint', 'int2', 'int4', 'int8'];
var DEC_TYPES = ['decimal', 'numeric', 'money', 'smallmoney', 'dec'];
var BOOL_TYPES = ['bit', 'boolean', 'bool'];
var DATE_TYPES = ['date', 'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset',
  'timestamp without time zone', 'timestamp with time zone', 'timestamptz',
  'time', 'time without time zone'];

/* ── Dates, and the two ways a generated one is rejected ──────────────────
   Both of these killed a real run against a real database, both with the
   same unhelpful message, so both are defended here rather than at the four
   places that build a statement.

   ONE — THE LITERAL IS NOT ISO UNLESS IT KEEPS THE 'T'. SQL Server reads
   'YYYY-MM-DD hh:mm:ss' into a `datetime` or a `smalldatetime` through the
   session's DATEFORMAT, NOT as ISO 8601. On a connection whose language is
   British English — which a UK-hosted server very often is — DATEFORMAT is
   dmy, so '2026-09-18 16:32:13' is read as day 09 of month 18, and the
   insert dies with "the conversion of a varchar data type to a datetime data
   type resulted in an out-of-range value". Every row whose day is past the
   12th fails, so a batch of a hundred fails as a batch and the table is
   skipped. 'YYYY-MM-DDThh:mm:ss.mmm' — the same string with the T left in —
   is read as ISO whatever the session language, on every version, and
   Postgres takes it too. That single character is the whole fix.

   TWO — THE RANGE IS NARROWER THAN JAVASCRIPT'S. `datetime` starts in 1753
   and `smalldatetime` runs only from 1900 to 6 June 2079, while a JS Date
   spans ±275,000 years. A value that arrived as a bare number, or a date
   typed into an AI pool, lands outside either and earns the same error.
   Clamping is the right answer for generated data: the row is invented, and
   a date a day off the edge it asked for is better than a run that dies
   part-way through a table. */
var DATE_LIMITS = {};
[['datetime', '1753-01-01T00:00:00.000Z', '9999-12-31T23:59:59.997Z'],
 ['smalldatetime', '1900-01-01T00:00:00.000Z', '2079-06-05T23:59:00.000Z'],
 ['*', '0001-01-01T00:00:00.000Z', '9999-12-31T23:59:59.999Z']]
  .forEach(function (r) { DATE_LIMITS[r[0]] = { min: Date.parse(r[1]), max: Date.parse(r[2]) }; });

/* A number is not a date, however willingly JavaScript pretends otherwise.
   `new Date("150000")` is the year 150000 and `new Date("999")` is the year
   999 — one is outside any SQL date type and the other is before `datetime`
   begins, and neither is remotely what was meant. A bare number reaching a
   date column means a name-pattern generator for an id or an amount was
   pointed at one, so the number is spent as an offset inside the last three
   years instead: still derived from the value, so the same input always
   gives the same date, and always a date the column can hold. */
function dgDateFromSeed(n, now) {
  var base = now == null ? Date.now() : Number(now);
  var seed = Math.abs(Math.floor(Number(n) || 0));
  return new Date(base - (seed % 1095) * 86400000 - (seed * 37) % 86400000);
}

function dgToDate(value, now) {
  if (value instanceof Date) return isNaN(value.getTime()) ? null : value;
  var s = trim(str(value));
  if (s === '') return null;
  if (/^-?\d+(\.\d+)?$/.test(s)) return dgDateFromSeed(s, now);
  var d = new Date(s);
  return isNaN(d.getTime()) ? null : d;
}

/* Does this column want a date? Asked when choosing a generator, because a
   date column is the only one where the name guessing it wrong is fatal: a
   string in a `bit` coerces to 0 and a number in an `nvarchar` stringifies,
   but a postcode in a `datetime` fails the insert and takes the batch with
   it. */
function dgWantsDate(type) {
  var t = lower(type);
  var p = t.indexOf('(');
  if (p > 0) t = t.slice(0, p).trim();
  return DATE_TYPES.indexOf(t) !== -1;
}

/* A GUID derived from whatever we were handed, so the same input always gives
   the same GUID. Not cryptographic and not trying to be — it exists so a
   uniqueidentifier column gets something of the right shape rather than a
   rejected insert. */
function dgGuidFrom(seed) {
  var s1 = str(seed) + '|' + Math.random();
  var h = 0x811c9dc5, out = '';
  for (var i = 0; i < 32; i++) {
    for (var j = 0; j < s1.length; j++) h = Math.imul(h ^ s1.charCodeAt(j), 16777619) >>> 0;
    h = Math.imul(h ^ i, 16777619) >>> 0;
    out += ((h >>> 28) & 15).toString(16);
  }
  return out.slice(0, 8) + '-' + out.slice(8, 12) + '-4' + out.slice(13, 16) + '-a'
    + out.slice(17, 20) + '-' + out.slice(20, 32);
}

function dgCoerce(value, col) {
  if (value == null) return null;
  var t = lower(col && (col.baseType || col.type));
  var paren = t.indexOf('(');
  if (paren > 0) t = t.slice(0, paren).trim();

  if (BOOL_TYPES.indexOf(t) !== -1) {
    var truthy = value === true || value === 1 || value === '1' || lower(value) === 'true';
    return truthy ? 1 : 0;
  }
  if (INT_TYPES.indexOf(t) !== -1) {
    var n = Math.round(Number(value) || 0);
    if (t === 'tinyint') return Math.max(0, Math.min(255, n));
    if (t === 'smallint' || t === 'int2') return Math.max(-32768, Math.min(32767, n));
    return n;
  }
  if (DEC_TYPES.indexOf(t) !== -1) {
    var num = Number(value) || 0;
    var scale = col && col.scale != null ? Number(col.scale) : 2;
    var prec = col && col.precision != null ? Number(col.precision) : 18;
    var rounded = Number(num.toFixed(Math.max(0, scale)));
    /* Precision is the TOTAL number of digits, so the largest value a
       DECIMAL(6,2) can hold is 9999.99 — four digits before the point and two
       after, not 9999. Clamping to the whole part would quietly throw away
       the decimals on every value that needed clamping, which is a rounding
       error the database never asked for. */
    var whole = Math.max(1, prec - Math.max(0, scale));
    var max = Math.pow(10, whole) - Math.pow(10, -Math.max(0, scale));
    if (Math.abs(rounded) > max) rounded = Number((max * (rounded < 0 ? -1 : 1)).toFixed(Math.max(0, scale)));
    return rounded;
  }
  if (t === 'uniqueidentifier' || t === 'uuid') {
    // A GUID column takes a GUID and nothing else. Whatever the generator
    // produced, what goes in is the canonical 8-4-4-4-12.
    var g = str(value);
    return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(g) ? g.toLowerCase() : dgGuidFrom(g);
  }
  if (DATE_TYPES.indexOf(t) !== -1) {
    var d = dgToDate(value);
    if (!d) return null;
    var lim = DATE_LIMITS[t] || DATE_LIMITS['*'];
    var ms = d.getTime();
    if (ms < lim.min) d = new Date(lim.min);
    else if (ms > lim.max) d = new Date(lim.max);
    var iso = d.toISOString();
    // A DATE column rejects a time; a time column wants only one. Sending the
    // whole ISO string to either is the commonest way a generated row bounces.
    if (t === 'date') return iso.slice(0, 10);
    if (t === 'time' || t === 'time without time zone') return iso.slice(11, 19);
    // A smalldatetime has no seconds — it rounds to the nearest minute, and a
    // value that rounds UP is out of range again at the very top of its range.
    if (t === 'smalldatetime') return iso.slice(0, 16) + ':00';
    // The T stays. See DATE_LIMITS above: without it this is not ISO and the
    // server is free to read the month as the day.
    return iso.slice(0, 23);
  }
  if (TEXT_TYPES.indexOf(t) !== -1) {
    var s = str(value);
    var max = col && col.maxLength != null ? Number(col.maxLength) : null;
    if (max != null && max > 0 && s.length > max) s = s.slice(0, max);
    return s;
  }
  return value;
}

/* ── Which columns a preview shows ────────────────────────────────────────
   A hundred-and-thirty-column table cannot be previewed in full and reading
   it would not help anyway. The first twelve, plus every key column wherever
   it sits, because the keys are what a person actually checks: is the FK
   pointing at something real, is the PK unique. The rest are a count and a
   button. */
function dgPreviewColumns(columns, opts) {
  var o = opts || {};
  var limit = o.limit > 0 ? o.limit : 12;
  if (o.all || (columns || []).length <= limit) {
    return { shown: (columns || []).slice(), hidden: 0, all: true };
  }
  var shown = [], taken = new Set();
  (columns || []).forEach(function (c, i) {
    var keep = i < limit || c.isPrimaryKey || !!c.fk || !!c.unique;
    if (keep && !taken.has(c.name)) { taken.add(c.name); shown.push(c); }
  });
  return { shown: shown, hidden: (columns || []).length - shown.length, all: false };
}

/* ── A preview, and only a preview ────────────────────────────────────────
   Rows in memory, for the screen. No connection, no insert, no run id. The
   FK columns show the parent they WOULD point at rather than a number that
   means nothing on its own, because "which client does this matter belong
   to" is the thing a person is checking when they look at a preview.

   `generate` is the page's existing generator registry, passed in. Its rules
   about what an email or a postcode look like are the ones this product has
   always used, and there is no reason for a second set. */
function dgPreviewRows(plan, count, generate, opts) {
  var o = opts || {};
  var n = Math.max(0, Math.min(Number(count) || 0, o.max || 5));
  var nullPct = o.nullPercent == null ? 10 : Number(o.nullPercent);
  var rnd = typeof o.random === 'function' ? o.random : function () { return 0.5; };
  var rows = [];
  for (var i = 0; i < n; i++) {
    var row = {};
    plan.columns.forEach(function (c) {
      if (!c.write) { row[c.name] = c.skipReason ? '(' + c.skipReason.split('—')[0].trim() + ')' : '(default)'; return; }
      if (c.fk) { row[c.name] = '→ ' + dgSplitKey(c.fk.to).name + '.' + c.fk.column; return; }
      // A nullable column is sometimes null, because a test data set in which
      // no optional column is ever empty tests only half the migration.
      if (c.nullable && !c.isPrimaryKey && !c.unique && rnd() * 100 < nullPct) { row[c.name] = null; return; }
      var v;
      try { v = generate(c, i); } catch (e) { v = null; }
      row[c.name] = dgCoerce(v, c);
    });
    rows.push(row);
  }
  return rows;
}

/* ══════════════════════════════════════════════════════════════════════════
   PHASE 2 — BUILDING THE WRITE
   ══════════════════════════════════════════════════════════════════════════
   Still pure. Nothing here opens a connection; it produces the statement text
   and the caller sends it. That is what lets the whole write path be tested
   without a database, which for a feature whose worst failure is "it wrote
   the wrong thing" is not a nicety. */

function dgQuote(name, dialect) {
  var n = str(name);
  if (dialect === 'postgres') return '"' + n.replace(/"/g, '""') + '"';
  return '[' + n.replace(/]/g, ']]') + ']';
}
function dgQualified(schema, name, dialect) {
  return dgQuote(schema || (dialect === 'postgres' ? 'public' : 'dbo'), dialect) + '.' + dgQuote(name, dialect);
}

/* A value as SQL text. Parameters would be better and are not available: the
   `execute` action this rides on takes a statement, not a parameter list, and
   adding a parameterised route is a backend change this phase does not need.
   So every literal is escaped here, in one function, rather than at each of
   the four call sites where it would eventually differ. */
function dgLiteral(v, col, dialect) {
  if (v === null || v === undefined) return 'NULL';
  var t = lower(col && (col.baseType || col.type));
  var paren = t.indexOf('('); if (paren > 0) t = t.slice(0, paren).trim();
  if (BOOL_TYPES.indexOf(t) !== -1) {
    return dialect === 'postgres' ? (v ? 'TRUE' : 'FALSE') : (v ? '1' : '0');
  }
  if (INT_TYPES.indexOf(t) !== -1 || DEC_TYPES.indexOf(t) !== -1) {
    var n = Number(v);
    return isFinite(n) ? String(n) : 'NULL';
  }
  var esc = str(v).replace(/'/g, "''");
  // N'' is SQL Server's way of saying "this string is Unicode". Without it an
  // accented character silently becomes a question mark in an NVARCHAR column.
  var prefix = (dialect !== 'postgres' && /^n?(var)?char$/.test(t.replace('n', 'n'))) ? 'N' : '';
  if (dialect !== 'postgres' && /^(nchar|nvarchar|ntext)$/.test(t)) prefix = 'N';
  return prefix + "'" + esc + "'";
}

/* ── Uniqueness, across the whole run ─────────────────────────────────────
   A pool per column that must not repeat. It is seeded with what is ALREADY
   in the table — otherwise the first run looks fine and the second collides
   with the first — and then holds everything this run has produced.

   `next` asks the generator for a value and, if it has been seen, asks again;
   after a few tries it stops asking and makes one, because a generator with a
   small vocabulary (a status, a country) will never produce a thousand
   distinct values however many times it is asked, and looping until it does
   is how a run hangs. */
function dgUniquePool(existing) {
  var seen = new Set((existing || []).map(function (v) { return lower(v); }));
  return {
    size: function () { return seen.size; },
    has: function (v) { return seen.has(lower(v)); },
    add: function (v) { seen.add(lower(v)); return v; },
    next: function (make, col, tries) {
      var n = tries || 6;
      for (var i = 0; i < n; i++) {
        var v = dgCoerce(make(i), col);
        if (v != null && !seen.has(lower(v))) { seen.add(lower(v)); return v; }
      }
      // Give up asking and derive one. Numbers count up from what is there;
      // text gets a short suffix that is cut to fit rather than overflowing.
      var t = lower(col && (col.baseType || col.type));
      if (INT_TYPES.indexOf(t) !== -1 || DEC_TYPES.indexOf(t) !== -1) {
        var k = seen.size + 1;
        while (seen.has(String(k))) k++;
        seen.add(String(k));
        return dgCoerce(k, col);
      }
      var base = str(dgCoerce(make(0), col) || 'v');
      var suffix, out, j = 1;
      do {
        suffix = '-' + (seen.size + j).toString(36);
        var max = col && col.maxLength > 0 ? col.maxLength : 4000;
        out = (base.slice(0, Math.max(0, max - suffix.length)) + suffix).slice(0, max);
        j++;
      } while (seen.has(lower(out)) && j < 1000);
      seen.add(lower(out));
      return out;
    },
  };
}

/* ── Spreading children across parents ────────────────────────────────────
   Not all on parent one, and not one each either. Round-robin over a shuffled
   list gives every parent roughly the same number of children, which is
   uniform in a way real data never is; so the index walks with a small random
   jitter, which clumps a little. A migration script that only ever sees one
   child per parent is a script whose GROUP BY has never been exercised. */
function dgSpread(count, parents, random) {
  var rnd = typeof random === 'function' ? random : Math.random;
  var out = [];
  if (!parents || !parents.length) return out;
  var i = 0;
  for (var n = 0; n < count; n++) {
    out.push(parents[i % parents.length]);
    i += 1 + (rnd() < 0.25 ? 1 : 0);
  }
  return out;
}

/* Existing key values in a parent nobody selected. Capped, because a parent
   with four million rows is not something to read in order to pick a handful
   of foreign keys, and ORDERed so two runs against a static table draw from
   the same set rather than whatever the engine felt like returning. */
function dgExistingKeysSql(schema, name, keyColumns, opts) {
  var o = opts || {};
  var dialect = o.dialect === 'postgres' ? 'postgres' : 'mssql';
  var limit = o.limit > 0 ? o.limit : 500;
  var cols = (keyColumns || []).map(function (c) { return dgQuote(c, dialect); }).join(', ');
  var from = dgQualified(schema, name, dialect);
  if (dialect === 'postgres') {
    return 'SELECT ' + cols + ' FROM ' + from + ' ORDER BY ' + cols + ' LIMIT ' + limit;
  }
  return 'SELECT TOP ' + limit + ' ' + cols + ' FROM ' + from + ' ORDER BY ' + cols;
}

/* ── The INSERT ───────────────────────────────────────────────────────────
   One statement per batch, and it hands the generated keys back.

   SQL Server: OUTPUT ... INTO a table variable, not a bare OUTPUT. A bare
   OUTPUT is refused outright on any table that has a trigger — "the target
   table cannot have any enabled triggers when the statement contains an
   OUTPUT clause without INTO" — and a source database of the sort this tool
   is pointed at is exactly where triggers live. The INTO form works either
   way, at the cost of declaring the variable, which needs the key columns'
   declared types. We have them.

   IDENTITY_INSERT is never used. The database assigns identities and we read
   back what it assigned; turning it off and writing our own would mean owning
   the sequence, colliding with whatever else writes to that table, and
   leaving the identity seed behind for somebody else to trip over. */
function dgBuildInsert(plan, rows, opts) {
  var o = opts || {};
  var dialect = o.dialect === 'postgres' ? 'postgres' : 'mssql';
  var cols = plan.columns.filter(function (c) { return c.write; });
  if (!cols.length || !rows.length) return null;

  var target = dgQualified(plan.schema, plan.name, dialect);
  var colList = '(' + cols.map(function (c) { return dgQuote(c.name, dialect); }).join(', ') + ')';
  var values = rows.map(function (r) {
    return '(' + cols.map(function (c) { return dgLiteral(r[c.name], c, dialect); }).join(', ') + ')';
  }).join(',\n  ');

  // Only the key columns the DATABASE fills in need reading back. A key we
  // generated ourselves we already know.
  var keyCols = (plan.primaryKeys || []).map(function (k) {
    return plan.columns.find(function (c) { return lower(c.name) === lower(k); });
  }).filter(Boolean);
  var wantBack = keyCols.length && keyCols.some(function (c) { return !c.write; });

  if (dialect === 'postgres') {
    var ret = keyCols.length ? '\nRETURNING ' + keyCols.map(function (c) { return dgQuote(c.name, dialect); }).join(', ') : '';
    return { sql: 'INSERT INTO ' + target + ' ' + colList + ' VALUES\n  ' + values + ret + ';',
      returnsKeys: !!keyCols.length, keyColumns: keyCols.map(function (c) { return c.name; }) };
  }

  if (!wantBack) {
    return { sql: 'INSERT INTO ' + target + ' ' + colList + ' VALUES\n  ' + values + ';',
      returnsKeys: false, keyColumns: keyCols.map(function (c) { return c.name; }) };
  }
  var decl = keyCols.map(function (c) { return dgQuote(c.name, dialect) + ' ' + (c.type || 'INT'); }).join(', ');
  var outList = keyCols.map(function (c) { return 'inserted.' + dgQuote(c.name, dialect); }).join(', ');
  var sel = keyCols.map(function (c) { return dgQuote(c.name, dialect); }).join(', ');
  return {
    sql: 'DECLARE @dgkeys TABLE (' + decl + ');\n'
      + 'INSERT INTO ' + target + ' ' + colList + '\n'
      + 'OUTPUT ' + outList + ' INTO @dgkeys\n'
      + 'VALUES\n  ' + values + ';\n'
      + 'SELECT ' + sel + ' FROM @dgkeys;',
    returnsKeys: true, keyColumns: keyCols.map(function (c) { return c.name; }),
  };
}

/* ── The second pass, for cycles and self-references ──────────────────────
   Rows that went in with their link empty, joined up now that the thing they
   point at exists. One UPDATE per row rather than a clever set-based join,
   because the pairing was decided in memory and there is nothing in the
   database to join on — and because a failure then names the row it was. */
function dgBuildLinkUpdate(plan, links, opts) {
  var o = opts || {};
  var dialect = o.dialect === 'postgres' ? 'postgres' : 'mssql';
  var target = dgQualified(plan.schema, plan.name, dialect);
  var stmts = (links || []).map(function (l) {
    var set = Object.keys(l.set).map(function (c) {
      var col = plan.columns.find(function (x) { return lower(x.name) === lower(c); }) || {};
      return dgQuote(c, dialect) + ' = ' + dgLiteral(l.set[c], col, dialect);
    }).join(', ');
    var where = Object.keys(l.where).map(function (c) {
      var col = plan.columns.find(function (x) { return lower(x.name) === lower(c); }) || {};
      return dgQuote(c, dialect) + ' = ' + dgLiteral(l.where[c], col, dialect);
    }).join(' AND ');
    return 'UPDATE ' + target + ' SET ' + set + ' WHERE ' + where + ';';
  });
  return stmts.length ? stmts.join('\n') : null;
}

/* ── What the database just complained about ──────────────────────────────
   A failed batch is not a failed run. The engines word it differently and the
   kind decides what happens next: a CHECK constraint means this table's rules
   are beyond guessing and the table is abandoned with the constraint named; a
   unique collision means try the batch again with fresh values; a foreign-key
   error means a parent key went missing under us. Anything else is reported
   as it came. */
function dgClassifyError(message) {
  var m = str(message);
  if (/CHECK constraint|check constraint|violates check constraint/i.test(m)) {
    return { kind: 'check', constraint: dgConstraintName(m) };
  }
  if (/UNIQUE KEY constraint|duplicate key|violates unique constraint|Cannot insert duplicate/i.test(m)) {
    return { kind: 'unique', constraint: dgConstraintName(m) };
  }
  if (/FOREIGN KEY constraint|violates foreign key constraint/i.test(m)) {
    return { kind: 'fk', constraint: dgConstraintName(m) };
  }
  if (/String or binary data would be truncated|value too long/i.test(m)) {
    return { kind: 'truncation', constraint: dgConstraintName(m) };
  }
  if (/Cannot insert the value NULL|null value in column/i.test(m)) {
    return { kind: 'null', constraint: dgConstraintName(m) };
  }
  /* Worth a kind of its own rather than "other": this one was reported from
     a real run and the message names neither the column nor the value, so
     "[other]" left nothing to act on. See DATE_LIMITS for what causes it. */
  if (/out-of-range value|conversion of a \w+ data type to a \w+ data type|date\/time field value out of range|invalid input syntax for type (date|time|timestamp)/i.test(m)) {
    return { kind: 'date', constraint: dgConstraintName(m) };
  }
  return { kind: 'other', constraint: dgConstraintName(m) };
}
function dgConstraintName(message) {
  var m = str(message).match(/constraint ["'`]?([A-Za-z0-9_.\[\]]+)["'`]?/i);
  return m ? m[1].replace(/[\[\]"'`]/g, '') : '';
}

/* ── Generating the rows to insert ────────────────────────────────────────
   The real thing, not the preview. Differences that matter:

     · foreign keys get an ACTUAL parent key — one generated earlier in this
       run, or one already in the table — never an invented number;
     · unique and primary-key columns go through the pool, so nothing repeats
       within the run or against what is already there;
     · a column caught in a cycle is written null and remembered, for the
       second pass to fill in.

   `parentKeys` is { fkName: [ {col:value,...}, ... ] } — the real keys each
   foreign key may draw from. A key with an empty list and a NOT NULL column
   is a table that cannot be generated, and the caller is told so rather than
   being handed rows the database will reject. */
function dgGenerateRows(plan, count, ctx) {
  var c = ctx || {};
  var rnd = typeof c.random === 'function' ? c.random : Math.random;
  var nullPct = c.nullPercent == null ? 10 : Number(c.nullPercent);
  var generate = typeof c.generate === 'function' ? c.generate : function () { return null; };
  var pools = c.pools || {};
  var parentKeys = c.parentKeys || {};
  var nullFirst = new Set((c.nullFirst || []).map(lower));

  // One parent per row per key, decided up front so the spread is over the
  // whole table rather than re-rolled each row.
  var assigned = {};
  (plan.fks || []).forEach(function (fk) {
    var pool = parentKeys[fk.name] || [];
    assigned[fk.name] = dgSpread(count, pool, rnd);
  });

  var rows = [], deferred = [];
  for (var i = 0; i < count; i++) {
    var row = {}, defer = null;
    plan.columns.forEach(function (col) {
      if (!col.write) return;
      var low = lower(col.name);

      if (col.fk) {
        var fkName = col.fk.name;
        if (nullFirst.has(low)) {
          row[col.name] = null;
          defer = defer || { index: i, set: {} };
          defer.set[col.name] = { fk: fkName, ref: col.fk.column };
          return;
        }
        var parent = (assigned[fkName] || [])[i];
        row[col.name] = parent ? dgCoerce(parent[col.fk.column], col) : null;
        return;
      }

      if (col.isPrimaryKey || col.unique) {
        var pool = pools[low] || (pools[low] = dgUniquePool([]));
        row[col.name] = pool.next(function (n) { return generate(col, i + n * 7919); }, col);
        return;
      }

      if (col.nullable && rnd() * 100 < nullPct) { row[col.name] = null; return; }
      row[col.name] = dgCoerce(generate(col, i), col);
    });
    rows.push(row);
    if (defer) deferred.push(defer);
  }
  return { rows: rows, deferred: deferred };
}

/* Which foreign keys of this table have nothing to point at, and whether that
   is fatal. A nullable key with no parents is a column left null; a NOT NULL
   one is a table that cannot be generated at all, and saying so before the
   run beats finding out on row one. */
function dgMissingParents(plan, parentKeys) {
  var out = [];
  (plan.fks || []).forEach(function (fk) {
    var pool = (parentKeys || {})[fk.name] || [];
    if (pool.length || fk.self) return;
    var required = fk.columns.some(function (p) {
      var col = plan.columns.find(function (c) { return lower(c.name) === lower(p.from); });
      return col && col.nullable === false;
    });
    out.push({ fk: fk.name, to: fk.to, blocking: required,
      columns: fk.columns.map(function (p) { return p.from; }) });
  });
  return out;
}

/* ══════════════════════════════════════════════════════════════════════════
   PHASE 3 — TAKING IT BACK OUT
   ══════════════════════════════════════════════════════════════════════════
   Two different deletes, and the difference matters.

   "Delete generated rows" removes the rows ONE RUN put there, by their keys,
   and nothing else. That is the only reason the run manifest exists: without
   the keys, "the rows we generated" is indistinguishable from "the rows that
   were already there", and the only delete anyone could offer would be one
   that emptied the table.

   "Empty selected tables" is that other delete, and it is exactly as
   dangerous as it sounds — it removes everything, including rows that were
   there before this tool ever ran. It is off by default, needs the words
   typed, and runs children-first so a foreign key does not stop it half way.

   Both are DELETE. Neither is TRUNCATE: truncate is refused outright on any
   table a foreign key points at, it cannot be filtered, and it resets the
   identity seed — three different ways of being the wrong tool here. */

/* Which statements a path may contain. The insert path may not delete; the
   delete path may not create, drop, truncate, alter or update. Each is
   checked against what it is actually allowed to do rather than against one
   shared list, because "no destructive verbs" is not a rule that survives a
   feature whose whole job is destructive. */
var DG_VERBS = ['create', 'drop', 'truncate', 'alter', 'delete', 'merge', 'insert',
  'update', 'grant', 'revoke', 'exec', 'execute'];

/* Quoted identifiers and string literals come out before the check looks at
   anything: a column called [Delete] or a value of 'created' is a name, and
   refusing to fill somebody's table over their column name would be a bug
   rather than caution. */
function dgBareStatement(sql) {
  return str(sql)
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/--[^\n\r]*/g, ' ')
    .replace(/\[(?:[^\]]|\]\])*\]/g, ' "id" ')
    .replace(/"(?:[^"]|"")*"/g, ' "id" ')
    .replace(/'(?:[^']|'')*'/g, " 'lit' ");
}
function dgAssertOnly(sql, allowed, what) {
  var bare = dgBareStatement(sql);
  var banned = DG_VERBS.filter(function (v) { return allowed.indexOf(v) === -1; });
  var hit = banned.find(function (v) { return new RegExp('\\b' + v + '\\b', 'i').test(bare); });
  if (hit) throw new Error('Refusing to run: that statement contains ' + hit.toUpperCase()
    + ', and this is the ' + what + ' path. Nothing was sent.');
  var has = allowed.some(function (v) { return new RegExp('\\b' + v + '\\b', 'i').test(bare); });
  if (!has) throw new Error('Refusing to run: no ' + allowed.join(' or ').toUpperCase()
    + ' in that statement. Nothing was sent.');
  return true;
}

/* ── Deleting one run's rows, by key ──────────────────────────────────────
   A single-column key becomes an IN list; a composite one becomes OR'd
   groups, because there is no portable way to write a tuple IN list that both
   engines accept. Chunked by the caller — a hundred thousand keys is not one
   statement. */
function dgBuildDeleteByKeys(plan, keyRows, opts) {
  var o = opts || {};
  var dialect = o.dialect === 'postgres' ? 'postgres' : 'mssql';
  var pk = (plan.primaryKeys || []);
  if (!pk.length || !keyRows || !keyRows.length) return null;
  var cols = pk.map(function (k) {
    return plan.columns.find(function (c) { return lower(c.name) === lower(k); }) || { name: k };
  });
  var target = dgQualified(plan.schema, plan.name, dialect);

  if (cols.length === 1) {
    var vals = keyRows.map(function (r) { return dgLiteral(r[cols[0].name], cols[0], dialect); });
    return 'DELETE FROM ' + target + ' WHERE ' + dgQuote(cols[0].name, dialect)
      + ' IN (' + vals.join(', ') + ');';
  }
  var groups = keyRows.map(function (r) {
    return '(' + cols.map(function (c) {
      return dgQuote(c.name, dialect) + ' = ' + dgLiteral(r[c.name], c, dialect);
    }).join(' AND ') + ')';
  });
  return 'DELETE FROM ' + target + ' WHERE ' + groups.join('\n   OR ') + ';';
}

/* ── Emptying a table ─────────────────────────────────────────────────────
   Everything, including what was there first. DELETE and not TRUNCATE: see
   the note at the top of this section. */
function dgBuildEmpty(schema, name, opts) {
  var o = opts || {};
  var dialect = o.dialect === 'postgres' ? 'postgres' : 'mssql';
  return 'DELETE FROM ' + dgQualified(schema, name, dialect) + ';';
}

/* Children first. Both deletes run in the reverse of the insert order, so a
   foreign key never stops one half way through with the other half gone. */
function dgReverseOrder(order) {
  return (order || []).slice().reverse();
}

/* What a recorded run says it did, for the list a person chooses from. Tables
   with no primary key are called out: their rows went in and cannot be picked
   back out again, because there is nothing to identify them by. */
function dgRunSummary(run) {
  var tables = Object.keys((run && run.tables) || {});
  var rows = 0, undeletable = [];
  tables.forEach(function (k) {
    var t = run.tables[k];
    rows += Number(t.inserted) || 0;
    if (!t.keyColumns || !t.keyColumns.length || !t.keys || !t.keys.length) {
      if (Number(t.inserted) > 0) undeletable.push(t.name || k);
    }
  });
  return {
    id: str(run && run.id), at: str(run && run.at), profile: str(run && run.profile),
    tableCount: tables.length, rows: rows, tables: tables, undeletable: undeletable,
  };
}

/* The insert order as a sentence, for the strip above the table list. The
   order is the thing most likely to be wrong in a way nobody notices until
   the data is already in, so it is shown rather than merely obeyed. */
function dgOrderLabel(order) {
  return (order || []).map(function (k, i) { return (i + 1) + '. ' + dgSplitKey(k).name; }).join(' → ');
}

return {
  SKIP: SKIP,
  dgKey: dgKey, dgSplitKey: dgSplitKey,
  dgIsRowversion: dgIsRowversion, dgSkipReason: dgSkipReason,
  dgBuildGraph: dgBuildGraph, dgParentClosure: dgParentClosure,
  dgOrder: dgOrder, dgNullableBreak: dgNullableBreak,
  dgDefaultParentRows: dgDefaultParentRows,
  dgPlanTable: dgPlanTable, dgCoerce: dgCoerce,
  dgPreviewColumns: dgPreviewColumns, dgPreviewRows: dgPreviewRows,
  dgOrderLabel: dgOrderLabel,
  dgQuote: dgQuote, dgQualified: dgQualified, dgLiteral: dgLiteral, dgGuidFrom: dgGuidFrom,
  dgUniquePool: dgUniquePool, dgSpread: dgSpread, dgExistingKeysSql: dgExistingKeysSql,
  dgBuildInsert: dgBuildInsert, dgBuildLinkUpdate: dgBuildLinkUpdate,
  dgClassifyError: dgClassifyError, dgGenerateRows: dgGenerateRows,
  dgMissingParents: dgMissingParents,
  dgBareStatement: dgBareStatement, dgAssertOnly: dgAssertOnly,
  dgBuildDeleteByKeys: dgBuildDeleteByKeys, dgBuildEmpty: dgBuildEmpty,
  dgReverseOrder: dgReverseOrder, dgRunSummary: dgRunSummary,
  dgToDate: dgToDate, dgDateFromSeed: dgDateFromSeed, dgWantsDate: dgWantsDate,
};
});
