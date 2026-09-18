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
};
});
