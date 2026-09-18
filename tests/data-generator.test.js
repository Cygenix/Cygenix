// tests/data-generator.test.js — the Data Generator, reworked to fill EXISTING
// tables in the source database. Phase 1: picker, introspection, FK graph,
// preview. No writes.
//
// The decisions this phase fixed, pinned as decisions:
//
//   * the schema comes from the database, not from a fixed demo set — and the
//     four demo tables, and every line that could create or drop a table, are
//     gone from the page rather than sitting behind a disabled button;
//   * a composite foreign key is ONE key over several columns, not several
//     keys — grouping them by constraint name is what stops a child being
//     linked to two different parents by its two halves;
//   * parents are inserted before children, and the order is shown rather than
//     merely obeyed;
//   * a table caught in a cycle goes in with its nullable link empty and is
//     joined up afterwards; one with no nullable link is named and skipped,
//     not allowed to fail the run;
//   * identity, computed and rowversion columns are never written — and
//     `timestamp` means opposite things in the two dialects;
//   * a value is cut to fit its column rather than failing the insert;
//   * the AI option can see column names and types and has no access to a row.
'use strict';

const fs = require('fs');
const path = require('path');
const DG = require('../public/cygenix-datagen-model.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

console.log('Data Generator — filling real tables\n');

/* A small but awkward schema: a chain, a composite key, a self-reference and
   a two-table loop. Deliberately not named after anything — this product is
   target-agnostic and so is its test data. */
const FKS = [
  { name: 'FK_Matter_Client', fromSchema: 'dbo', fromTable: 'Matter', fromColumn: 'ClientId',
    toSchema: 'dbo', toTable: 'Client', toColumn: 'Id' },
  { name: 'FK_Time_Matter', fromSchema: 'dbo', fromTable: 'Timecard', fromColumn: 'MatterId',
    toSchema: 'dbo', toTable: 'Matter', toColumn: 'Id' },
  // Composite: two rows, one key.
  { name: 'FK_Line_Order', fromSchema: 'sales', fromTable: 'Line', fromColumn: 'TenantId',
    toSchema: 'sales', toTable: 'Ord', toColumn: 'TenantId' },
  { name: 'FK_Line_Order', fromSchema: 'sales', fromTable: 'Line', fromColumn: 'OrderNo',
    toSchema: 'sales', toTable: 'Ord', toColumn: 'OrderNo' },
  // Self-reference.
  { name: 'FK_Emp_Mgr', fromSchema: 'hr', fromTable: 'Emp', fromColumn: 'ManagerId',
    toSchema: 'hr', toTable: 'Emp', toColumn: 'Id' },
  // A two-table loop.
  { name: 'FK_A_B', fromSchema: 'dbo', fromTable: 'A', fromColumn: 'BId', toSchema: 'dbo', toTable: 'B', toColumn: 'Id' },
  { name: 'FK_B_A', fromSchema: 'dbo', fromTable: 'B', fromColumn: 'AId', toSchema: 'dbo', toTable: 'A', toColumn: 'Id' },
];
const G = DG.dgBuildGraph(FKS);

/* ════════════════════════════════════════════════════════════════════════
   1. The graph
   ════════════════════════════════════════════════════════════════════════ */
console.log('— the graph —');

check('a table is identified by schema AND name, so two Invoices in two schemas stay two tables',
  DG.dgKey('Sales', 'Invoice') === 'sales.invoice'
  && DG.dgKey('Fin', 'Invoice') !== DG.dgKey('Sales', 'Invoice')
  && DG.dgKey(null, 'X') === 'dbo.x');

check('a composite key is ONE key over two columns, not two keys',
  (() => {
    const fks = G.fksOf('sales.line');
    return fks.length === 1 && fks[0].columns.length === 2
      && fks[0].columns.map(c => c.from).sort().join(',') === 'OrderNo,TenantId';
  })(), JSON.stringify(G.fksOf('sales.line')));

check('a self-reference is marked as one, and is not a parent of itself',
  G.fksOf('hr.emp')[0].self === true && G.parentsOf('hr.emp').length === 0);

check('parents are found, and the closure walks all the way up',
  (() => {
    const c = DG.dgParentClosure(G, ['dbo.timecard']);
    return c.keys.sort().join(',') === 'dbo.client,dbo.matter,dbo.timecard'
      && c.added.sort().join(',') === 'dbo.client,dbo.matter';
  })());

check('a key with no constraint name still groups by the pair of tables it joins',
  (() => {
    const g = DG.dgBuildGraph([
      { fromSchema: 'x', fromTable: 'C', fromColumn: 'a', toSchema: 'x', toTable: 'P', toColumn: 'a' },
      { fromSchema: 'x', fromTable: 'C', fromColumn: 'b', toSchema: 'x', toTable: 'P', toColumn: 'b' },
    ]);
    return g.fksOf('x.c').length === 1 && g.fksOf('x.c')[0].columns.length === 2;
  })());

/* ════════════════════════════════════════════════════════════════════════
   2. Insert order
   ════════════════════════════════════════════════════════════════════════ */
console.log('\n— insert order —');

check('parents come before children',
  (() => {
    const o = DG.dgOrder(G, ['dbo.timecard', 'dbo.matter', 'dbo.client'], {});
    return o.order.join(',') === 'dbo.client,dbo.matter,dbo.timecard' && !o.cyclic.length;
  })());

check('the order is shown as a sentence, numbered, in the order it will run',
  DG.dgOrderLabel(['dbo.client', 'dbo.matter']) === '1. client → 2. matter');

check('a parent that is NOT selected does not order anything — it is sampled, not inserted',
  DG.dgOrder(G, ['dbo.timecard'], {}).order.join(',') === 'dbo.timecard');

/* THE cycle rule. */
check('a loop with a nullable link goes in with the link empty and is joined up after',
  (() => {
    const o = DG.dgOrder(G, ['dbo.a', 'dbo.b'], {
      'dbo.a': [{ name: 'BId', nullable: true }],
      'dbo.b': [{ name: 'AId', nullable: false }],
    });
    // A can be broken (BId is nullable); B cannot.
    return o.cyclic.sort().join(',') === 'dbo.a,dbo.b'
      && o.breaks.length === 1 && o.breaks[0].table === 'dbo.a'
      && o.breaks[0].nullFirst.join(',') === 'BId'
      && o.unbreakable.join(',') === 'dbo.b'
      && o.order.indexOf('dbo.a') !== -1;
  })());

check('a loop with no nullable link anywhere names both tables and inserts neither',
  (() => {
    const o = DG.dgOrder(G, ['dbo.a', 'dbo.b'], {
      'dbo.a': [{ name: 'BId', nullable: false }],
      'dbo.b': [{ name: 'AId', nullable: false }],
    });
    return o.unbreakable.sort().join(',') === 'dbo.a,dbo.b' && !o.order.length;
  })());

check('a composite link can only be broken if EVERY one of its columns is nullable',
  (() => {
    const half = DG.dgNullableBreak(G, 'sales.line',
      [{ name: 'TenantId', nullable: true }, { name: 'OrderNo', nullable: false }], new Set(['sales.line', 'sales.ord']));
    const both = DG.dgNullableBreak(G, 'sales.line',
      [{ name: 'TenantId', nullable: true }, { name: 'OrderNo', nullable: true }], new Set(['sales.line', 'sales.ord']));
    return half === null && both !== null && both.nullFirst.length === 2;
  })());

check('a self-reference is ordered normally but still gets the two-pass treatment',
  (() => {
    const o = DG.dgOrder(G, ['hr.emp'], { 'hr.emp': [{ name: 'ManagerId', nullable: true }] });
    return o.order.join(',') === 'hr.emp' && !o.cyclic.length
      && o.breaks.length === 1 && o.breaks[0].linkAfter[0].self === true;
  })());

check('two keys between the same pair of tables do not count as two dependencies',
  (() => {
    const g = DG.dgBuildGraph([
      { name: 'K1', fromSchema: 'd', fromTable: 'C', fromColumn: 'p1', toSchema: 'd', toTable: 'P', toColumn: 'id' },
      { name: 'K2', fromSchema: 'd', fromTable: 'C', fromColumn: 'p2', toSchema: 'd', toTable: 'P', toColumn: 'id' },
    ]);
    return DG.dgOrder(g, ['d.c', 'd.p'], {}).order.join(',') === 'd.p,d.c';
  })());

/* ════════════════════════════════════════════════════════════════════════
   3. Planning a table
   ════════════════════════════════════════════════════════════════════════ */
console.log('\n— planning —');

const COLS = [
  { name: 'Id', type: 'INT', baseType: 'int', nullable: false, isIdentity: true, ordinal: 1 },
  { name: 'Email', type: 'NVARCHAR(50)', baseType: 'nvarchar', maxLength: 50, nullable: false, ordinal: 2 },
  { name: 'ClientId', type: 'INT', baseType: 'int', nullable: true, ordinal: 3 },
  { name: 'Total', type: 'DECIMAL(6,2)', baseType: 'decimal', precision: 6, scale: 2, nullable: true, ordinal: 4 },
  { name: 'FullName', type: 'NVARCHAR(200)', baseType: 'nvarchar', nullable: true, isComputed: true, ordinal: 5 },
  { name: 'Ver', type: 'TIMESTAMP', baseType: 'timestamp', nullable: false, ordinal: 6 },
  { name: 'RegionCode', type: 'NVARCHAR(10)', baseType: 'nvarchar', maxLength: 10, nullable: false, default: "('UK')", ordinal: 7 },
];
const infer = (n) => (/email/i.test(n) ? { gen: 'email' } : { gen: 'shortText' });
const plan = DG.dgPlanTable(
  { schema: 'dbo', name: 'Matter', columns: COLS, primaryKeys: ['Id'],
    uniques: [{ name: 'UQ_Email', columns: ['Email'] }, { name: 'UQ_Pair', columns: ['ClientId', 'Total'] }] },
  { dialect: 'mssql', inferColumnMeta: infer, fks: G.fksOf('dbo.matter') });
const col = (n) => plan.columns.find(c => c.name === n);

check('identity, computed and rowversion columns are never written, and each says why',
  col('Id').write === false && /identity/.test(col('Id').skipReason)
  && col('FullName').write === false && /computed/.test(col('FullName').skipReason)
  && col('Ver').write === false && /rowversion/.test(col('Ver').skipReason));

/* `timestamp` is a rowversion in SQL Server and an ordinary date-time in
   Postgres. Same word, opposite meaning: read it the wrong way round and you
   either skip a real date column or fail every insert on a versioned table. */
check('…and `timestamp` is read according to the dialect, not the word',
  DG.dgIsRowversion({ baseType: 'timestamp' }, 'mssql') === true
  && DG.dgIsRowversion({ baseType: 'timestamp' }, 'postgres') === false);

check('a foreign-key column is generated from its key, not from its name',
  col('ClientId').fk && col('ClientId').fk.to === 'dbo.client' && col('ClientId').generator === 'fk');

check('an ordinary column takes the generator its name suggests',
  col('Email').generator === 'email' && /inferred/.test(col('Email').generatorSource));

check('a column with a database default and no better guess is left to the database',
  col('RegionCode').write === false && /column default/.test(col('RegionCode').skipReason));

check('single-column uniques and composite uniques are kept apart',
  col('Email').unique === 'UQ_Email'
  && plan.uniqueMany.length === 1 && plan.uniqueMany[0].columns.join(',') === 'clientid,total');

check('a single-column primary key is a uniqueness rule in its own right',
  DG.dgPlanTable({ schema: 'd', name: 'T', columns: [{ name: 'Code', type: 'NVARCHAR(9)' }], primaryKeys: ['Code'] },
    { inferColumnMeta: infer }).columns[0].unique === 'PRIMARY KEY');

check('a table with no primary key is flagged — nothing can link to it or be deleted by key',
  DG.dgPlanTable({ schema: 'd', name: 'T', columns: [{ name: 'X' }], primaryKeys: [] }, { inferColumnMeta: infer })
    .warnings.some(w => /No primary key/.test(w)));

check('an override beats the inference, and says so',
  DG.dgPlanTable({ schema: 'dbo', name: 'Matter', columns: COLS, primaryKeys: ['Id'] },
    { inferColumnMeta: infer, overrides: { email: { mode: 'phone' } } })
    .columns.find(c => c.name === 'Email').generator === 'phone');

/* ════════════════════════════════════════════════════════════════════════
   4. Fitting a value to its column
   ════════════════════════════════════════════════════════════════════════ */
console.log('\n— values —');

check('a string too long for its column is cut rather than failing the insert',
  DG.dgCoerce('x'.repeat(80), { baseType: 'nvarchar', maxLength: 50 }).length === 50);

check('a decimal is rounded to its scale and held inside its precision',
  DG.dgCoerce(1.23456, { baseType: 'decimal', precision: 6, scale: 2 }) === 1.23
  && DG.dgCoerce(999999, { baseType: 'decimal', precision: 6, scale: 2 }) === 9999.99);

check('integers round, and the small ones are clamped to what they can hold',
  DG.dgCoerce(3.7, { baseType: 'int' }) === 4
  && DG.dgCoerce(400, { baseType: 'tinyint' }) === 255
  && DG.dgCoerce(99999, { baseType: 'smallint' }) === 32767);

check('a bit takes 1 or 0 whatever it is handed',
  DG.dgCoerce(true, { baseType: 'bit' }) === 1 && DG.dgCoerce('false', { baseType: 'boolean' }) === 0
  && DG.dgCoerce(0, { baseType: 'bit' }) === 0);

check('null stays null — a NOT NULL column is the planner\'s problem, not the coercer\'s',
  DG.dgCoerce(null, { baseType: 'nvarchar', maxLength: 5 }) === null);

/* ════════════════════════════════════════════════════════════════════════
   5. Preview
   ════════════════════════════════════════════════════════════════════════ */
console.log('\n— preview —');

check('a preview is five rows at most, generated in memory',
  DG.dgPreviewRows(plan, 500, () => 'x', { max: 5 }).length === 5);

check('a foreign key previews as the parent it would point at, not as a bare number',
  /^→ client\.Id$/.test(DG.dgPreviewRows(plan, 1, () => 'x')[0].ClientId));

check('a skipped column says what fills it instead of showing a made-up value',
  (() => {
    const r = DG.dgPreviewRows(plan, 1, () => 'x')[0];
    return /identity/.test(r.Id) && /computed/.test(r.FullName);
  })());

check('nullable columns are sometimes empty, and keys never are',
  (() => {
    const rows = DG.dgPreviewRows(plan, 5, () => 'x', { nullPercent: 100, random: () => 0 });
    return rows.every(r => r.Total === null) && rows.every(r => r.Email === 'x');
  })());

check('a value is fitted to its column on the way into the preview, not just on the way in to the database',
  DG.dgPreviewRows(plan, 1, () => 'y'.repeat(200))[0].Email.length === 50);

/* A hundred-and-thirty-column table is not a preview, it is a wall. */
check('a wide table shows the first twelve columns PLUS every key column, wherever it sits',
  (() => {
    const wide = [];
    for (let i = 0; i < 40; i++) wide.push({ name: 'c' + i, ordinal: i });
    wide[30].isPrimaryKey = true;
    wide[35].fk = { to: 'd.p', column: 'id' };
    const v = DG.dgPreviewColumns(wide, {});
    return v.shown.length === 14 && v.hidden === 26
      && v.shown.some(c => c.name === 'c30') && v.shown.some(c => c.name === 'c35');
  })());

check('…and asking for all of them gives all of them',
  DG.dgPreviewColumns(Array.from({ length: 40 }, (_, i) => ({ name: 'c' + i })), { all: true }).shown.length === 40);

check('a narrow table is shown whole, with nothing hidden',
  DG.dgPreviewColumns(COLS, {}).hidden === 0);

/* ════════════════════════════════════════════════════════════════════════
   6. Row counts for parents nobody asked for
   ════════════════════════════════════════════════════════════════════════ */
console.log('\n— parent row counts —');

check('a parent added automatically gets a fifth of its children, with a floor of ten',
  DG.dgDefaultParentRows(1000) === 200 && DG.dgDefaultParentRows(100) === 20
  && DG.dgDefaultParentRows(10) === 10 && DG.dgDefaultParentRows(0) === 10);

/* ════════════════════════════════════════════════════════════════════════
   7. Phase 2 — building the write
   ════════════════════════════════════════════════════════════════════════ */
console.log('\n— the write —');

const MPLAN = DG.dgPlanTable(
  { schema: 'dbo', name: 'Matter', primaryKeys: ['Id'], uniques: [{ name: 'UQ_Ref', columns: ['Ref'] }],
    columns: [
      { name: 'Id', type: 'INT', baseType: 'int', nullable: false, isIdentity: true, ordinal: 1 },
      { name: 'ClientId', type: 'INT', baseType: 'int', nullable: false, ordinal: 2 },
      { name: 'Ref', type: 'NVARCHAR(8)', baseType: 'nvarchar', maxLength: 8, nullable: false, ordinal: 3 },
      { name: 'Note', type: 'NVARCHAR(50)', baseType: 'nvarchar', maxLength: 50, nullable: true, ordinal: 4 },
    ] },
  { dialect: 'mssql', inferColumnMeta: infer, fks: G.fksOf('dbo.matter') });

const PARENTS = { FK_Matter_Client: [{ Id: 7 }, { Id: 8 }, { Id: 9 }] };
const genRows = (n, opts) => DG.dgGenerateRows(MPLAN, n, Object.assign(
  { generate: () => 'AB', parentKeys: PARENTS, random: () => 0.9, nullPercent: 0 }, opts || {}));

check('identity columns are never written — the database assigns them',
  (() => {
    const r = genRows(3).rows;
    return r.every(x => !('Id' in x)) && r.every(x => 'ClientId' in x);
  })());

/* THE rule this whole feature exists for. */
check('a foreign key only ever takes a key that really exists',
  (() => {
    const r = genRows(9).rows;
    return r.every(x => [7, 8, 9].indexOf(x.ClientId) !== -1);
  })());

check('children are spread across the parents, not piled on the first',
  (() => {
    const r = genRows(9).rows;
    return new Set(r.map(x => x.ClientId)).size === 3;
  })());

check('…and the spread clumps a little rather than being perfectly even',
  (() => {
    const even = DG.dgSpread(9, [1, 2, 3], () => 0.9);
    const clumped = DG.dgSpread(9, [1, 2, 3], () => 0.1);
    return even.join('') === '123123123' && clumped.join('') !== '123123123';
  })());

check('unique and primary-key columns never repeat within a run',
  (() => {
    const r = genRows(50).rows;
    return new Set(r.map(x => x.Ref)).size === 50;
  })());

/* Without this the first run looks fine and the second collides with it. */
check('…nor against values that were already in the table',
  (() => {
    const pools = { ref: DG.dgUniquePool(['AB', 'AB-2']) };
    const r = genRows(3, { pools }).rows;
    return r.every(x => x.Ref !== 'AB' && x.Ref !== 'AB-2') && new Set(r.map(x => x.Ref)).size === 3;
  })());

check('a unique value that cannot fit its column is shortened to make room, not overflowed',
  (() => {
    const pool = DG.dgUniquePool([]);
    const col = { baseType: 'nvarchar', maxLength: 8 };
    const vals = [];
    for (let i = 0; i < 30; i++) vals.push(pool.next(() => 'SAMEVALUE', col));
    return vals.every(v => v.length <= 8) && new Set(vals).size === 30;
  })());

check('a generator with a tiny vocabulary does not hang the run',
  (() => {
    const pool = DG.dgUniquePool([]);
    const out = [];
    for (let i = 0; i < 20; i++) out.push(pool.next(() => 'only', { baseType: 'int' }));
    return new Set(out).size === 20;
  })());

check('a nullable column is sometimes empty; a key never is',
  (() => {
    const r = genRows(20, { nullPercent: 100, random: () => 0 }).rows;
    return r.every(x => x.Note === null) && r.every(x => x.Ref && x.ClientId);
  })());

/* ── The statements ─────────────────────────────────────────────────────── */
const INS = DG.dgBuildInsert(MPLAN, genRows(2).rows, { dialect: 'mssql' });

check('the insert names only the columns it writes, and never the identity',
  /INSERT INTO \[dbo\]\.\[Matter\] \(\[ClientId\], \[Ref\], \[Note\]\)/.test(INS.sql)
  && !/\[Id\],/.test(INS.sql));

/* A bare OUTPUT is refused outright on any table that has a trigger, and a
   source database of the sort this is pointed at is exactly where triggers
   live. The INTO form works either way. */
check('SQL Server reads the keys back with OUTPUT ... INTO, which survives a trigger',
  /DECLARE @dgkeys TABLE \(\[Id\] INT\);/.test(INS.sql)
  && /OUTPUT inserted\.\[Id\] INTO @dgkeys/.test(INS.sql)
  && /SELECT \[Id\] FROM @dgkeys;/.test(INS.sql)
  && INS.returnsKeys === true);

/* The only mention anywhere is the comment saying why it is not used.
   Turning it off and writing our own identities would mean owning somebody
   else's sequence, colliding with whatever else writes to that table, and
   leaving the seed behind for the next person to trip over. */
check('IDENTITY_INSERT is never used — the database owns its own sequence',
  !/IDENTITY_INSERT/i.test(INS.sql)
  && !/IDENTITY_INSERT/i.test(read('public', 'data-generator.html'))
  && (read('public', 'cygenix-datagen-model.js').match(/IDENTITY_INSERT/gi) || []).length === 1);

check('Postgres uses RETURNING and its own quoting',
  (() => {
    const pg = DG.dgBuildInsert(MPLAN, genRows(1).rows, { dialect: 'postgres' });
    return /INSERT INTO "dbo"\."Matter"/.test(pg.sql) && /RETURNING "Id";/.test(pg.sql)
      && !/DECLARE/.test(pg.sql);
  })());

check('a key WE generate is not read back — we already know it',
  (() => {
    const p = DG.dgPlanTable({ schema: 'd', name: 'T', primaryKeys: ['Code'],
      columns: [{ name: 'Code', type: 'NVARCHAR(9)', baseType: 'nvarchar', maxLength: 9, nullable: false }] },
      { inferColumnMeta: infer });
    const b = DG.dgBuildInsert(p, [{ Code: 'A' }], { dialect: 'mssql' });
    return b.returnsKeys === false && !/OUTPUT/.test(b.sql);
  })());

check('a string literal is escaped, and a Unicode column gets its N prefix',
  DG.dgLiteral("O'Brien", { baseType: 'nvarchar' }, 'mssql') === "N'O''Brien'"
  && DG.dgLiteral("O'Brien", { baseType: 'nvarchar' }, 'postgres') === "'O''Brien'"
  && DG.dgLiteral(null, { baseType: 'int' }, 'mssql') === 'NULL');

check('a boolean is written the way each engine spells it',
  DG.dgLiteral(1, { baseType: 'bit' }, 'mssql') === '1'
  && DG.dgLiteral(1, { baseType: 'boolean' }, 'postgres') === 'TRUE');

check('an identifier with a closing bracket or a quote in it is escaped, not broken',
  DG.dgQuote('a]b', 'mssql') === '[a]]b]' && DG.dgQuote('a"b', 'postgres') === '"a""b"');

check('existing parent keys are read capped and ordered, in both dialects',
  /^SELECT TOP 500 \[Id\] FROM \[dbo\]\.\[Client\] ORDER BY \[Id\]$/.test(
    DG.dgExistingKeysSql('dbo', 'Client', ['Id'], { limit: 500 }))
  && /LIMIT 500$/.test(DG.dgExistingKeysSql('dbo', 'Client', ['Id'], { dialect: 'postgres', limit: 500 })));

/* ── Cycles, for real this time ─────────────────────────────────────────── */
check('a column being broken out of a cycle goes in NULL and is remembered for the second pass',
  (() => {
    const r = genRows(3, { nullFirst: ['ClientId'] });
    return r.rows.every(x => x.ClientId === null) && r.deferred.length === 3;
  })());

check('the second pass is an UPDATE per row, keyed on the primary key',
  (() => {
    const sql = DG.dgBuildLinkUpdate(MPLAN,
      [{ set: { ClientId: 7 }, where: { Id: 1 } }, { set: { ClientId: 8 }, where: { Id: 2 } }],
      { dialect: 'mssql' });
    return /UPDATE \[dbo\]\.\[Matter\] SET \[ClientId\] = 7 WHERE \[Id\] = 1;/.test(sql)
      && sql.split('\n').length === 2;
  })());

/* ── Refusing what is not an insert ─────────────────────────────────────── */
check('a table with a NOT NULL key and nothing to point at is named, not attempted',
  (() => {
    const m = DG.dgMissingParents(MPLAN, {});
    return m.length === 1 && m[0].blocking === true && m[0].to === 'dbo.client';
  })());

check('…but a NULLABLE key with no parents is just a column left empty',
  (() => {
    const p = DG.dgPlanTable({ schema: 'dbo', name: 'Matter', primaryKeys: ['Id'],
      columns: [{ name: 'Id', isIdentity: true }, { name: 'ClientId', baseType: 'int', nullable: true }] },
      { inferColumnMeta: infer, fks: G.fksOf('dbo.matter') });
    return DG.dgMissingParents(p, {})[0].blocking === false;
  })());

check('the database\'s own complaint is classified, and the constraint named',
  DG.dgClassifyError('The INSERT statement conflicted with the CHECK constraint "CK_Status".').kind === 'check'
  && DG.dgClassifyError('The INSERT statement conflicted with the CHECK constraint "CK_Status".').constraint === 'CK_Status'
  && DG.dgClassifyError('Violation of UNIQUE KEY constraint "UQ_Ref".').kind === 'unique'
  && DG.dgClassifyError('violates foreign key constraint "fk_a"').kind === 'fk'
  && DG.dgClassifyError('String or binary data would be truncated').kind === 'truncation'
  && DG.dgClassifyError('Cannot insert the value NULL into column').kind === 'null'
  && DG.dgClassifyError('Login failed').kind === 'other');

/* ════════════════════════════════════════════════════════════════════════
   8. The page
   ════════════════════════════════════════════════════════════════════════ */
console.log('\n— the page —');

const page = read('public', 'data-generator.html');
const db = read('netlify', 'functions', 'db-connect.js');

/* The reason for the whole rework: the page used to CREATE four demo tables
   and fill those. Pointed at a customer's real source database, a create-and-
   drop path one click away is not something to leave lying about, so it is
   gone from the file rather than hidden behind a disabled button. */
check('the four demo tables are gone, and so is every line that could create or drop one',
  !/DEFAULT_SCHEMA/.test(page)
  && !/buildCreateTableDdl|buildDropTableSql|ensureTable|dgDropAll|dgRestorePresets/.test(page)
  && !/CREATE TABLE/i.test(page) && !/DROP TABLE/i.test(page));

/* Comments are stripped first. Several of them describe what the page used
   to do, and a rule that cannot be explained in a comment is a rule that gets
   worked around. What matters is that no demo table name is left in the CODE. */
/* Checked as IDENTIFIERS rather than as words. "addresses" also appears as an
   ordinary English word in the AI prompt — "names, addresses and phone
   numbers" — and banning the word would be banning the language. What must
   not survive is any of the four as a table the code refers to. */
check('…and none of the four demo tables survives as something the code refers to',
  (() => {
    const code = page.replace(/<!--[\s\S]*?-->/g, ' ').replace(/\/\*[\s\S]*?\*\//g, ' ')
                     .replace(/^\s*\/\/.*$/gm, ' ');
    return !/['"`](customers|addresses|products|orders)['"`]/i.test(code)
      && !/dbo\.(customers|addresses|products|orders)/i.test(code);
  })());

check('the model is target-agnostic — nothing in it names a vendor or a product',
  !/elite|\b3E\b|vchr/i.test(read('public', 'cygenix-datagen-model.js'))
  && !/elite|\b3E\b|vchr/i.test(page));

check('the model is loaded before the page script that uses it',
  /cygenix-datagen-model\.js\?v=/.test(page)
  && page.indexOf('cygenix-datagen-model.js') < page.indexOf('// Cygenix Data Generator'));

check('the picker offers the SOURCE only, and says so when there is not one',
  /function dgOpenPicker/.test(page) && /readSourceConn\(\)/.test(page)
  && /No source connection/.test(page)
  && !/readTargetConn|tgtConnString/.test(page));

check('it lists tables grouped by schema, searchable, multi-select, with row counts',
  /dg-pick-grp/.test(page) && /dgRenderPicker/.test(page)
  && /type="checkbox"/.test(page) && /rowCount\.toLocaleString\(\)/.test(page));

check('"Also include parent tables" is offered and is on by default',
  /id="dg-pick-parents" checked/.test(page) && /INCLUDE_PARENTS = true/.test(page));

/* The page builds the numbered list itself rather than calling the model's
   label helper, because the model works in lower-cased keys — right for
   identity, wrong for a label — and the strip has to spell each table the way
   the database does. Both exist; the test names which one the page uses. */
check('the insert order is shown, not just obeyed, and spelled as the database spells it',
  /id="dg-order"/.test(page) && /Insert order:/.test(page)
  && /function dgDisplayName\(key\)/.test(page)
  && /ORDER\.order\.map\(\(k, i\) => \(i \+ 1\) \+ '\. ' \+ dgDisplayName\(k\)\)/.test(page));

check('tables that cannot be generated at all are named on screen rather than failing quietly',
  /unbreakable/.test(page) && /They will be skipped/.test(page));

/* The 26-second cap. Two calls answer the whole database; only the column
   read scales with the selection, and it runs four at a time. */
check('the whole database costs two calls, and only the per-table read scales',
  /action: 'schema-tables'/.test(page) && /action: 'schema-fks'/.test(page)
  && /DG_COLUMN_CONCURRENCY = 4/.test(page) && /action: 'schema-columns'/.test(page));

check('every database read is behind an in-flight guard and a three-second gap',
  /DG_MIN_INTERVAL_MS = 3000/.test(page)
  && /if \(_dgReading\)\{ logLine/.test(page)
  && /_dgReading = true;/.test(page)
  && /\} finally \{\s*\n\s*_dgReading = false;/.test(page));

/* Three occurrences and only three: the declaration, the one place it is
   set, and the finally of that same call. A fourth would mean something else
   had taken it upon itself to decide a read was over. */
check('the flag is cleared by the call that set it, and nowhere else',
  (page.match(/_dgReading = (true|false)/g) || []).length === 3);

check('a read that fails reports its message AND its stack, and one bad table does not stop the rest',
  /e\.stack/.test(page) && /One unreadable table must not take the other/.test(page));

/* Insert and the link-up UPDATE, and nothing else. `dgAssertInsertOnly`
   reads every statement before it is sent — quoted identifiers and string
   literals stripped first, so a column called [Delete] is a name rather than
   a refusal. */
check('the write path can only insert and link — no create, drop, truncate or delete',
  /function dgAssertInsertOnly/.test(page)
  && /DG_FORBIDDEN = \/\\b\(create\|drop\|truncate\|alter\|delete\|merge/.test(page)
  && /not an insert\. Nothing was sent/.test(page)
  && (page.match(/dgAssertInsertOnly\(/g) || []).length === 3);

check('production is a flat refusal, not a typed confirmation',
  /Sample data is never generated into production/.test(page)
  && /there is no confirmation for this, and none is offered/.test(page)
  && /env === 'PRD'/.test(page));

check('the write is behind its own in-flight flag and three-second gap',
  /let _dgWriting = false;/.test(page)
  && (page.match(/_dgWriting = (true|false)/g) || []).length === 3
  && /_dgLastWriteAt < DG_MIN_INTERVAL_MS/.test(page));

check('rows go in one batch per call, so no request can reach the 26-second cap',
  /for \(let i = 0; i < gen\.rows\.length; i \+= BATCH_SIZE\)/.test(page)
  && /const BATCH_SIZE = 500/.test(page));

check('each table has its own try/catch, and reports message and stack',
  /one that cannot be filled must not\n\s*\/\/ take the rest of the run with it/.test(page)
  && (page.match(/e\.stack/g) || []).length >= 3);

check('a CHECK failure names the constraint and moves to the next table',
  /failed a CHECK constraint/.test(page) && /the rest of this table is skipped/.test(page));

check('the run is recorded as it happens, because the keys exist nowhere else',
  /function dgSaveRun/.test(page)
  && /localStorage\.setItem\('cygenix_datagen_runs'/.test(page)
  && /cygenix_datagen_runs/.test(read('docs', 'storage-inventory.md')));

check('a parent nobody selected is SAMPLED from what is already in it, never invented',
  /dgExistingKeysSql/.test(page) && /drawing from/.test(page)
  && /DG_EXISTING_SAMPLE = 500/.test(page));

check('uniqueness is checked against rows that were already there',
  /DG_UNIQUE_SAMPLE = 2000/.test(page)
  && /existing value\(s\) read so nothing repeats/.test(page));

/* Column names and types only. Not a default — there is no code path that
   could send a value, because a toggle is one mis-click from a disclosure. */
/* Column names and types only. Not a default — there is no code path that
   could send a value, because a toggle labelled "off by default" is one
   mis-click from a disclosure. The check is that the function never reads a
   row: no query, no call to the database, nothing out of the preview. */
check('the AI option can see column names and types, and has no access to a row',
  (() => {
    const i = page.indexOf('async function dgImproveWithAI');
    const body = page.slice(i, page.indexOf('window.dgImproveWithAI', i) + 40);
    return /COLUMN NAMES AND TYPES ONLY/.test(page)
      && !/dbCall\(/.test(body)
      // Case-sensitive and followed by a space: `document.querySelector`
      // contains the letters of SELECT and is not a query.
      && !/\bSELECT\s/.test(body)
      && !/dgPreviewRows|existingRows/.test(body);
  })());

/* The inventory scanner reads public/ for a LITERAL key inside a localStorage
   call, so a key that only ever appears as a constant is never inventoried and
   never classified. The page spells it out for that reason; this keeps the two
   from drifting. */
check('the key the page reads is the key the constant names, and it is classified',
  /const DG_SEL_KEY = 'cygenix_datagen_selection'/.test(page)
  && /localStorage\.getItem\('cygenix_datagen_selection'\)/.test(page)
  && /cygenix_datagen_selection/.test(read('docs', 'storage-inventory.md')));

check('the row-count presets and the batch estimate are untouched',
  /const PRESETS = \[10, 100, 1000, 10000, 100000\]/.test(page) && /function estimateFor/.test(page));

check('the existing generators are reused rather than copied — the model is handed them',
  /const GENERATORS = \{/.test(page) && /dgValueForColumn/.test(page)
  && !/const GENERATORS/.test(read('public', 'cygenix-datagen-model.js')));

/* ── The backend ───────────────────────────────────────────────────────── */
check('unique constraints and unique indexes are read, in both dialects',
  (db.match(/groupUniques\(/g) || []).length === 3
  && /sys\.indexes/.test(db) && /pg_index/.test(db)
  && /is_primary_key = 0/.test(db) && /NOT i\.indisprimary/.test(db));

check('a composite unique stays one rule over several columns',
  (() => {
    const m = db.match(/function groupUniques\(rows\)\{[\s\S]*?\n\}/);
    return !!m && /byName\.get\(r\.name\)\.columns\.push/.test(m[0]);
  })());

check('Postgres gained the whole-database FK read it never had',
  /case 'schema-fks': \{[\s\S]{0,1200}pg_constraint/.test(db)
  && /con\.conname\s+AS fk_name/.test(db));

check('…and it reassembles a composite key, which needs the constraint name',
  /unnest\(con\.conkey, con\.confkey\) WITH ORDINALITY/.test(db));

check('both new reads are gated as schema reads, like every other introspection call',
  /'schema-fks': 'schema.read'/.test(db) && /'schema-columns': 'schema.read'/.test(db));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
if (fail) process.exit(1);
