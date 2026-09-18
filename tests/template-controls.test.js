// tests/template-controls.test.js — Conversion Templates: the three module
// controls added Sep-2026.
//
//   Exclude from publishing   the module stays, its tables stay, and Publish,
//                             the specification workbook, the staging DDL and
//                             Create staging tables all skip it. The
//                             readiness check ignores it. Existing templates
//                             load with nothing excluded.
//   Object Mapping            a module's staging → target pairs go to Object
//                             Mapping as tagged DRAFT jobs. Unticking removes
//                             ONLY those. A mapping built by hand is never
//                             touched, including one for the same pair. A
//                             send that would pass the hundred-job cap is
//                             refused whole, not trimmed to fit.
//   Create staging tables     CREATE TABLE per staging table, in the dialect
//                             of the chosen connection, one call per table,
//                             existing tables skipped. NEVER drop, truncate
//                             or alter. An in-flight guard and a three-second
//                             minimum gap between runs.
//
// The decisions are pinned here as decisions, not as a description of the
// current code, so that changing one of them fails a test with a name that
// says which promise was broken.
'use strict';

const fs = require('fs');
const path = require('path');
const TM = require('../public/cygenix-template-model.js');
const SPEC = require('../public/cygenix-template-spec.js');
const ST = require('../public/cygenix-template-staging.js');
const MAP = require('../public/cygenix-template-mapping.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

console.log('Conversion Templates — Map, Exclude and Create staging tables\n');

/* ── A template with three in-scope modules, one dropped ─────────────────── */
const COLS = [
  { name: 'Ident', ordinal: 1, baseType: 'int', isIdentity: true, isNullable: false },
  { name: 'Descr', ordinal: 2, baseType: 'nvarchar', maxLength: 200, isNullable: true },
  { name: 'Body', ordinal: 3, baseType: 'nvarchar', maxLength: -1, isNullable: true },
  { name: 'Amount', ordinal: 4, baseType: 'decimal', precision: 18, scale: 2, isNullable: false },
  { name: 'Total', ordinal: 5, baseType: 'decimal', precision: 18, scale: 2, isComputed: true },
];
function tpl() {
  const t = TM.tmNewTemplate({ name: 'T', projectId: 'p1', profileId: 'FIN_3E_UAT' });
  TM.tmSyncScope(t, ['AP', 'Matters', 'WIP']);
  [['AP', 'Vchr'], ['AP', 'VchrDetail'], ['Matters', 'Matter'], ['WIP', 'WIPItem']].forEach(([m, x], i) => {
    const row = TM.tmAddTable(t, m, { targetTable: x, loadOrder: i + 1 });
    TM.tmSetTableColumns(t, m, row.id, COLS, {});
  });
  return t;
}

(async function main() {
/* ════════════════════════════════════════════════════════════════════════
   1. The model: the flags and what honours them
   ════════════════════════════════════════════════════════════════════════ */
console.log('\n— the model —');

check('a new module is neither excluded nor mapped, and both are real booleans',
  (() => { const m = TM.tmNewModule('AP'); return m.excluded === false && m.mapped === false; })());

check('an older document loads with nothing excluded and nothing mapped',
  (() => {
    const t = tpl();
    t.modules.forEach(m => { delete m.excluded; delete m.mapped; });
    t.schema = 2;
    const after = TM.tmMigrate(t);
    return after.schema === TM.TM_SCHEMA_VERSION
      && after.modules.every(m => m.excluded === false && m.mapped === false);
  })());

check('excluding a module keeps it in scope and keeps its tables',
  (() => {
    const t = tpl();
    TM.tmSetModuleExcluded(t, 'AP', true, 'me');
    const m = TM.tmFindModule(t, 'AP');
    return m.inScope !== false && m.tables.length === 2 && m.excluded === true;
  })());

check('a module that has dropped out of the Configurator scope cannot be ticked either way',
  (() => {
    const t = tpl();
    TM.tmSyncScope(t, ['AP', 'Matters']);           // WIP drops out, tables kept
    return TM.tmSetModuleExcluded(t, 'WIP', true, 'me') === null
      && TM.tmSetModuleMapped(t, 'WIP', true, 'me') === null;
  })());

check('tmActiveModules is in scope AND not excluded, and names the excluded ones',
  (() => {
    const t = tpl();
    TM.tmSetModuleExcluded(t, 'Matters', true, 'me');
    const active = TM.tmActiveModules(t).map(m => m.module);
    return active.join(',') === 'AP,WIP' && TM.tmExcludedModules(t).join(',') === 'Matters';
  })());

check('the readiness check ignores an excluded module — even one with no tables at all',
  (() => {
    const t = tpl();
    TM.tmSyncScope(t, ['AP', 'Matters', 'WIP', 'Empty']);
    const before = TM.tmCanPublish(t);              // Empty has no tables: blocked
    TM.tmSetModuleExcluded(t, 'Empty', true, 'me');
    return before === false && TM.tmCanPublish(t) === true;
  })());

check('…but excluding EVERY module in scope blocks the publish, rather than publishing nothing',
  (() => {
    const t = tpl();
    ['AP', 'Matters', 'WIP'].forEach(m => TM.tmSetModuleExcluded(t, m, true, 'me'));
    return TM.tmCanPublish(t) === false
      && TM.tmValidate(t).some(i => i.level === 'error' && /every module in scope is excluded/i.test(i.message));
  })());

check('the exclusion is a warning on the readiness panel, named not just counted',
  (() => {
    const t = tpl();
    TM.tmSetModuleExcluded(t, 'WIP', true, 'me');
    const w = TM.tmValidate(t).filter(i => i.level === 'warning' && /excluded from publishing/.test(i.message));
    return w.length === 1 && /WIP/.test(w[0].message);
  })());

check('column coverage and the publish warning skip excluded modules',
  (() => {
    const t = tpl();
    const row = TM.tmAddTable(t, 'WIP', { targetTable: 'NoCols' }, 'me');   // no snapshot
    const before = TM.tmColumnCoverage(t).tablesMissing.length;
    TM.tmSetModuleExcluded(t, 'WIP', true, 'me');
    return before === 1 && TM.tmColumnCoverage(t).tablesMissing.length === 0
      && TM.tmPublishWarnings(t).length === 0 && !!row;
  })());

check('the summary counts the scope, names the exclusions, and says what will be published',
  (() => {
    const t = tpl();
    TM.tmSetModuleExcluded(t, 'AP', true, 'me');
    const s = TM.tmSummary(t);
    // 3 modules in scope, 4 tables; AP is 2 of those tables.
    return s.moduleCount === 3 && s.tableCount === 4 && s.excludedCount === 1
      && s.publishModuleCount === 2 && s.publishTableCount === 2;
  })());

check('publishing freezes WHICH modules were excluded, for the audit',
  (() => {
    const t = tpl();
    TM.tmSetModuleExcluded(t, 'Matters', true, 'me');
    const f = TM.tmPublish(t, 'me');
    return f && Array.isArray(f.excludedModules) && f.excludedModules.join(',') === 'Matters';
  })());

check('tmCanPublish is still a boolean — an array there would read as "yes" and publish',
  typeof TM.tmCanPublish(tpl()) === 'boolean');

/* ════════════════════════════════════════════════════════════════════════
   2. The workbook, the DDL and the staging plan all skip an excluded module
   ════════════════════════════════════════════════════════════════════════ */
console.log('\n— what exclusion actually leaves out —');

check('the three copies of "is this module in play" give the same answer for every case',
  [{ inScope: true, excluded: false }, { inScope: true, excluded: true },
   { inScope: false, excluded: false }, { inScope: false, excluded: true },
   { excluded: true }, {}].every(m =>
    TM.tmModuleActive(m) === SPEC.moduleActive(m) && SPEC.moduleActive(m) === ST.moduleActive(m)));

check('the specification workbook leaves an excluded module out of every sheet',
  (() => {
    const t = tpl();
    TM.tmSetModuleExcluded(t, 'AP', true, 'me');
    const rows = SPEC.buildSpecRows(t);
    return rows.tables.length === 2
      && !rows.tables.some(r => r.module === 'AP')
      && !rows.columns.some(c => c.module === 'AP')
      && !rows.loadOrder.some(r => r.module === 'AP');
  })());

check('the staging DDL leaves it out too — one filter, both artefacts',
  (() => {
    const t = tpl();
    TM.tmSetModuleExcluded(t, 'AP', true, 'me');
    const sql = SPEC.buildStagingDdl(t);
    return !/STG_Vchr\b/.test(sql) && /STG_Matter\b/.test(sql);
  })());

check('and Create staging tables leaves it out as well',
  (() => {
    const t = tpl();
    TM.tmSetModuleExcluded(t, 'AP', true, 'me');
    const plan = ST.stagingPlan(t, {});
    return plan.tables.length === 2 && !plan.tables.some(r => r.module === 'AP');
  })());

check('reading the target columns is NOT narrowed by exclusion — un-exclude and the detail is there',
  /Deliberately IN SCOPE, not active/.test(read('public', 'cygenix-template-spec.js')));

/* ════════════════════════════════════════════════════════════════════════
   3. Create staging tables
   ════════════════════════════════════════════════════════════════════════ */
console.log('\n— create staging tables —');

const plan = ST.stagingPlan(tpl(), {});
const apVchr = plan.tables.find(r => r.stagingTable === 'STG_Vchr');

check('every staging table in scope is planned, in load order', plan.tables.length === 4
  && plan.tables.map(r => r.stagingTable).join(',') === 'STG_Vchr,STG_VchrDetail,STG_Matter,STG_WIPItem');

check('a computed column is never created — the client cannot supply one',
  apVchr.columnCount === 4 && !/Total/.test(apVchr.sql));

check('every column is nullable, with no keys, identity, defaults or indexes',
  (() => {
    const body = apVchr.sql;
    return /\[Ident\] int NULL/.test(body) && !/IDENTITY/i.test(body)
      && !/PRIMARY KEY/i.test(body) && !/NOT NULL/i.test(body) && !/DEFAULT/i.test(body) && !/INDEX/i.test(body);
  })(), apVchr.sql);

check('SQL Server: guarded with OBJECT_ID so a re-run creates nothing twice',
  /^IF OBJECT_ID\(N'dbo\.STG_Vchr', N'U'\) IS NULL/m.test(apVchr.sql));

check('PostgreSQL: CREATE TABLE IF NOT EXISTS, double-quoted identifiers, in the public schema by default',
  (() => {
    const p = ST.stagingPlan(tpl(), { dialect: 'postgres' });
    const r = p.tables.find(x => x.stagingTable === 'STG_Vchr');
    return p.schema === 'public' && /^CREATE TABLE IF NOT EXISTS "public"\."STG_Vchr"/.test(r.sql)
      && /"Ident" integer NULL/.test(r.sql) && /"Body" text NULL/.test(r.sql)
      && /"Amount" numeric\(18,2\) NULL/.test(r.sql);
  })());

check('types are translated for the dialect, not copied across',
  ST.typeFor({ baseType: 'nvarchar', maxLength: 200 }, 'mssql').sql === 'nvarchar(100)'
  && ST.typeFor({ baseType: 'nvarchar', maxLength: -1 }, 'mssql').sql === 'nvarchar(max)'
  && ST.typeFor({ baseType: 'nvarchar', maxLength: -1 }, 'postgres').sql === 'text'
  && ST.typeFor({ baseType: 'bit' }, 'postgres').sql === 'boolean'
  && ST.typeFor({ baseType: 'boolean' }, 'mssql').sql === 'bit'
  && ST.typeFor({ baseType: 'uuid' }, 'mssql').sql === 'uniqueidentifier'
  && ST.typeFor({ baseType: 'money' }, 'postgres').sql === 'numeric(19,4)');

check('a type neither engine knows is passed through as the target spells it, and reported',
  (() => {
    const one = ST.typeFor({ baseType: 'hstore' }, 'mssql');
    const t = tpl();
    TM.tmSetTableColumns(t, 'Matters', TM.tmFindModule(t, 'Matters').tables[0].id,
      [{ name: 'X', ordinal: 1, baseType: 'hstore' }], {});
    const p = ST.stagingPlan(t, {});
    return one.sql === 'hstore' && one.known === false
      && p.unknownTypes.length === 1 && p.unknownTypes[0].column === 'X';
  })());

check('a table with no column detail is listed and NOT created — the hole is visible',
  (() => {
    const t = tpl();
    TM.tmAddTable(t, 'WIP', { targetTable: 'NoCols' }, 'me');
    const p = ST.stagingPlan(t, {});
    const row = p.tables.find(r => r.stagingTable === 'STG_NoCols');
    return p.noColumns === 1 && row.status === 'no-columns' && row.sql === '';
  })());

check('an existing table is marked "exists" before anything is confirmed, and is never in the run',
  (() => {
    const p = ST.stagingPlan(tpl(), { existing: { stg_vchr: true, stg_matter: true } });
    return p.scanned === true && p.toSkip === 2 && p.toCreate === 2
      && p.tables.filter(r => r.status === 'exists').map(r => r.stagingTable).sort().join(',') === 'STG_Matter,STG_Vchr';
  })());

check('the existence scan is one read of the catalog, per dialect',
  /sys\.tables/.test(ST.existingTablesSql({ schema: 'dbo' }))
  && /information_schema\.tables/.test(ST.existingTablesSql({ dialect: 'postgres', schema: 'stg' }))
  && /'stg'/.test(ST.existingTablesSql({ dialect: 'postgres', schema: 'stg' })));

check('identifiers are quoted for their dialect and internal quotes are doubled',
  ST.quoteIdent('a]b', 'mssql') === '[a]]b]' && ST.quoteIdent('a"b', 'postgres') === '"a""b"');

check('the dialect comes from the connection: a postgres URL, a Function URL, a connection string',
  ST.dialectOf('postgres://u:p@h:5432/db') === 'postgres'
  && ST.dialectOf('postgresql://u@h/db') === 'postgres'
  && ST.dialectOf('https://x.azurewebsites.net/api/db') === 'mssql'
  && ST.dialectOf('Server=s;Database=d;') === 'mssql');

/* THE safety test. */
check('NOTHING this file can produce drops, truncates or alters anything',
  (() => {
    const all = ST.stagingPlan(tpl(), {}).tables.map(r => r.sql).join('\n')
      + ST.stagingPlan(tpl(), { dialect: 'postgres' }).tables.map(r => r.sql).join('\n');
    const bare = ST.bareStatement(all);
    return !/\b(drop|truncate|alter|delete|insert|update|merge|grant|revoke|exec)\b/i.test(bare);
  })());

check('assertCreateOnly refuses a statement that is not create-only, and one with no CREATE TABLE',
  (() => {
    let a = false, b = false, c = false;
    try { ST.assertCreateOnly('DROP TABLE [x]; CREATE TABLE [y] (a int NULL);'); } catch (e) { a = /not create-only/.test(e.message); }
    try { ST.assertCreateOnly('SELECT 1'); } catch (e) { b = /no CREATE TABLE/.test(e.message); }
    c = ST.assertCreateOnly('CREATE TABLE [dbo].[x] ([a] int NULL);') === true;
    return a && b && c;
  })());

check('…but a column called [Update] is a column name, not a statement, and is allowed',
  ST.assertCreateOnly('CREATE TABLE [dbo].[x] ([Update] int NULL, [Drop] int NULL);') === true);

await (async function () {
check('the run is one call per table — Netlify cuts a function off at 26 seconds',
  await (async () => {
    ST._resetRunStateForTests();
    const sent = [];
    const p = ST.stagingPlan(tpl(), {});
    const r = await ST.runPlan(p, { execute: (sql) => { sent.push(sql); return Promise.resolve({}); }, now: 1e12 });
    return r.ok && r.created === 4 && sent.length === 4
      && sent.every(s => (s.match(/CREATE TABLE/g) || []).length === 1);
  })());

check('a table that fails does not stop the others, and the server\'s own message is kept',
  await (async () => {
    ST._resetRunStateForTests();
    const p = ST.stagingPlan(tpl(), {});
    const r = await ST.runPlan(p, {
      now: 1e12,
      execute: (sql) => /STG_Matter\b/.test(sql) ? Promise.reject(new Error('Invalid column name')) : Promise.resolve({}),
    });
    const bad = r.results.find(x => x.stagingTable === 'STG_Matter');
    return r.ok && r.created === 3 && r.failed === 1 && bad.status === 'failed' && bad.error === 'Invalid column name';
  })());

check('a table another session created in the meantime is a SKIP, not a failure',
  await (async () => {
    ST._resetRunStateForTests();
    const p = ST.stagingPlan(tpl(), {});
    const r = await ST.runPlan(p, {
      now: 1e12,
      execute: (sql) => /STG_Vchr\b(?!Detail)/.test(sql)
        ? Promise.reject(new Error("There is already an object named 'STG_Vchr' in the database."))
        : Promise.resolve({}),
    });
    return r.ok && r.skipped === 1 && r.failed === 0;
  })());

check('one bad statement refuses the WHOLE run — nothing is half-created',
  await (async () => {
    ST._resetRunStateForTests();
    const p = ST.stagingPlan(tpl(), {});
    p.tables[2].sql = 'DROP TABLE [dbo].[STG_Matter]; CREATE TABLE [dbo].[STG_Matter] (a int NULL);';
    let calls = 0;
    const r = await ST.runPlan(p, { execute: () => { calls++; return Promise.resolve({}); }, now: 1e12 });
    return r.ok === false && /not create-only/.test(r.reason) && calls === 0;
  })());

check('a second run inside three seconds is refused, and the flag is cleared by the call that set it',
  await (async () => {
    ST._resetRunStateForTests();
    const p = ST.stagingPlan(tpl(), {});
    const first = await ST.runPlan(p, { execute: () => Promise.resolve({}), now: 1e12 });
    const p2 = ST.stagingPlan(tpl(), {});
    const tooSoon = await ST.runPlan(p2, { execute: () => Promise.resolve({}), now: 1e12 + 2999 });
    const later = await ST.runPlan(p2, { execute: () => Promise.resolve({}), now: 1e12 + 3001 });
    return first.ok && tooSoon.ok === false && /wait a moment/i.test(tooSoon.reason)
      && later.ok === true && ST.runState().busy === false;
  })());

check('two runs at once: the second is refused rather than queued',
  await (async () => {
    ST._resetRunStateForTests();
    const p = ST.stagingPlan(tpl(), {});
    let release;
    const gate = new Promise((res) => { release = res; });
    const slow = ST.runPlan(p, { execute: () => gate, now: 1e12 });
    const second = await ST.runPlan(ST.stagingPlan(tpl(), {}), { execute: () => Promise.resolve({}), now: 1e12 + 5000 });
    release({});
    await slow;
    return second.ok === false && /already creating/i.test(second.reason);
  })());

check('nothing to create is said plainly rather than run',
  await (async () => {
    ST._resetRunStateForTests();
    const p = ST.stagingPlan(tpl(), { existing: { stg_vchr: true, stg_vchrdetail: true, stg_matter: true, stg_wipitem: true } });
    const r = await ST.runPlan(p, { execute: () => Promise.resolve({}), now: 1e12 });
    return r.ok === false && /Nothing to create/.test(r.reason);
  })());

})();

check('the reviewable script shows the skipped tables, commented out, so it is still safe to run by hand',
  (() => {
    const p = ST.stagingPlan(tpl(), { existing: { stg_vchr: true } });
    const s = ST.planScript(p, { templateName: 'T', version: 1 });
    return /ALREADY EXISTS, will be skipped/.test(s)
      && /^-- IF OBJECT_ID/m.test(s)
      && /Create only\./.test(s);
  })());

/* ════════════════════════════════════════════════════════════════════════
   4. Object Mapping
   ════════════════════════════════════════════════════════════════════════ */
console.log('\n— object mapping —');

const SEND = { projectId: 'p1', by: 'me@x', stagingSchema: 'dbo', targetSchema: 'dbo' };

check('a module sends one job per table, source = staging table, target = target table',
  (() => {
    const r = MAP.applySend(tpl(), 'AP', [], SEND);
    return r.ok && r.added === 2
      && r.created.every(j => j.jobType === 'simple-map' && j.type === 'migration')
      && r.created.map(j => j.sourceTable).sort().join(',') === 'dbo.STG_Vchr,dbo.STG_VchrDetail'
      && r.created.map(j => j.targetTable).sort().join(',') === 'dbo.Vchr,dbo.VchrDetail';
  })());

check('the jobs are DRAFTS with no column mapping — a "ready" job with nothing mapped moves no data',
  (() => {
    const r = MAP.applySend(tpl(), 'AP', [], SEND);
    return r.created.every(j => j.status === 'draft' && Array.isArray(j.columnMapping) && !j.columnMapping.length
      && !j.insertSQL && j.projectId === 'p1');
  })());

check('every job carries the template id and the module, so its origin is not a guess',
  (() => {
    const t = tpl();
    const r = MAP.applySend(t, 'AP', [], SEND);
    return r.created.every(j => j.fromTemplate && j.fromTemplate.templateId === t.id
      && j.fromTemplate.module === 'AP' && j.fromTemplate.tableId);
  })());

check('an existing pair is not sent twice, whoever made it',
  (() => {
    const t = tpl();
    const byHand = { id: 'j1', name: 'mine', sourceTable: 'DBO.stg_vchr', targetTable: 'dbo.VCHR', columnMapping: [{ a: 1 }] };
    const r = MAP.applySend(t, 'AP', [byHand], SEND);
    return r.ok && r.added === 1 && r.plan.duplicates.length === 1 && r.plan.duplicates[0].byHand === true;
  })());

check('sending the same module twice adds nothing the second time',
  (() => {
    const t = tpl();
    const first = MAP.applySend(t, 'AP', [], SEND);
    const second = MAP.applySend(t, 'AP', first.jobs, SEND);
    return second.ok && second.added === 0 && second.jobs.length === first.jobs.length;
  })());

/* THE test this feature exists to pass. */
check('unticking removes ONLY this template\'s jobs for this module — hand-made work is untouched',
  (() => {
    const t = tpl(), other = tpl();
    other.id = 'tpl_other';
    const byHand = { id: 'hand', name: 'my careful mapping', sourceTable: 'dbo.Something', targetTable: 'dbo.Else', columnMapping: [{ a: 1 }] };
    let jobs = [byHand];
    jobs = MAP.applySend(t, 'AP', jobs, SEND).jobs;
    jobs = MAP.applySend(t, 'Matters', jobs, SEND).jobs;
    jobs = MAP.applySend(other, 'WIP', jobs, SEND).jobs;
    const before = jobs.length;                                  // 1 + 2 + 1 + 1
    const r = MAP.applyRemove(t, 'AP', jobs);
    return before === 5 && r.removed === 2 && r.jobs.length === 3
      && r.jobs.some(j => j.id === 'hand')
      && r.jobs.some(j => j.fromTemplate && j.fromTemplate.module === 'Matters')
      && r.jobs.some(j => j.fromTemplate && j.fromTemplate.templateId === 'tpl_other');
  })());

check('a hand-made job for the SAME pair survives an untick — the stamp decides, not the pair',
  (() => {
    const t = tpl();
    const byHand = { id: 'hand', name: 'mine', sourceTable: 'dbo.STG_Vchr', targetTable: 'dbo.Vchr' };
    const jobs = MAP.applySend(t, 'AP', [byHand], SEND).jobs;
    const r = MAP.applyRemove(t, 'AP', jobs);
    return r.jobs.some(j => j.id === 'hand') && r.jobs.every(j => !MAP.isFromTemplate(j, t.id, 'AP'));
  })());

check('a job that started here and has since been worked on is named before it goes',
  (() => {
    const t = tpl();
    const jobs = MAP.applySend(t, 'AP', [], SEND).jobs;
    jobs[0].columnMapping = [{ srcCol: 'a', tgtCol: 'b' }];
    const p = MAP.planRemove(t, 'AP', jobs);
    return p.remove.length === 2 && p.edited.length === 1;
  })());

check('a send that would pass the hundred-job cap is REFUSED whole, not trimmed to fit',
  (() => {
    const t = tpl();
    const full = [];
    for (let i = 0; i < 99; i++) full.push({ id: 'j' + i, name: 'j' + i, sourceTable: 's' + i, targetTable: 't' + i });
    const r = MAP.applySend(t, 'AP', full, SEND);   // 99 + 2 = 101
    return r.ok === false && /would push 1 of the oldest out/.test(r.reason)
      && /Delete jobs you have finished with/.test(r.reason);
  })());

check('…and exactly filling the cap is allowed',
  (() => {
    const t = tpl();
    const full = [];
    for (let i = 0; i < 98; i++) full.push({ id: 'j' + i, name: 'j' + i, sourceTable: 's' + i, targetTable: 't' + i });
    const r = MAP.applySend(t, 'AP', full, SEND);
    return r.ok === true && r.added === 2 && r.jobs.length === 100 && r.jobs.length === MAP.JOB_CAP;
  })());

check('the count beside the heading is read from the jobs, not from the ticks',
  (() => {
    const t = tpl();
    const jobs = MAP.applySend(t, 'AP', [], SEND).jobs;
    TM.tmSetModuleMapped(t, 'Matters', true, 'me');      // ticked but never sent
    return MAP.countSent(t, jobs) === 2 && MAP.countSentForModule(t, 'AP', jobs) === 2
      && MAP.countSentForModule(t, 'Matters', jobs) === 0;
  })());

check('a bare table name is qualified with its schema, an already-qualified one is left alone',
  MAP.qualify('Vchr', 'dbo') === 'dbo.Vchr' && MAP.qualify('stg.Vchr', 'dbo') === 'stg.Vchr'
  && MAP.qualify('Vchr', '') === 'Vchr' && MAP.qualify('', 'dbo') === '');

/* ════════════════════════════════════════════════════════════════════════
   5. The page
   ════════════════════════════════════════════════════════════════════════ */
console.log('\n— the page —');

const page = read('public', 'conversion-templates.html');

check('the two new modules are loaded on the page',
  /cygenix-template-staging\.js\?v=/.test(page) && /cygenix-template-mapping\.js\?v=/.test(page));

check('both tick boxes are on the right of the row, under headings that say Map and Exclude',
  /class="ct-modhead"/.test(page) && />Map<\/span>/.test(page) && />Exclude<\/span>/.test(page)
  && /\.ct-mods li \.tk\{/.test(page) && /\.ct-modhead \.tk\{/.test(page));

check('each tick box explains itself on hover',
  /title="Send this module’s staging → target table pairs to Object Mapping/.test(page)
  && /title="Keep this module and its tables, but leave it out of Publish/.test(page));

check('a module no longer in scope gets no tick boxes, but keeps the row aligned',
  /cls === 'out'\s*\n?\s*\? '<span class="tk none"><\/span><span class="tk none"><\/span>'/.test(page)
  && /\.ct-mods li \.tk\.none\{visibility:hidden\}/.test(page));

check('ticking a box does not also change which module is selected',
  /onclick="event\.stopPropagation\(\)"/.test(page));

check('an excluded row is greyed AND labelled — colour alone is not a label',
  /\.ct-mods li\.ex\{opacity/.test(page) && /class="chip exc"/.test(page)
  && />excluded<\/span>/.test(page));

check('the greying uses theme tokens, so it works in dark and light alike',
  /\.ct-mods li \.chip\.exc\{background:var\(--amber-bg\);color:var\(--amber\)/.test(page));

check('the summary line says how many are excluded, without changing the module count',
  /excludedCount \? ' · ' \+ s\.excludedCount \+ ' excluded from publishing'/.test(page));

check('Tick all / Untick all are in the section header, with the count beside them',
  /id="ct-map-all"[^>]*onclick="ctMapAll\(true\)"/.test(page)
  && /id="ct-map-none"[^>]*onclick="ctMapAll\(false\)"/.test(page)
  && /id="ct-mods-sent"/.test(page)
  && /sent to Object Mapping/.test(page));

check('"Create staging tables" is in the toolbar beside Publish',
  /id="ct-publish"[\s\S]{0,700}id="ct-stage"[^>]*onclick="ctOpenStage\(\)"/.test(page));

check('the dialog reviews the SQL before anything runs, with Copy and Download .sql',
  /id="ct-stage-sql"[^>]*readonly/.test(page)
  && /onclick="ctStageCopy\(\)"/.test(page) && /onclick="ctStageDownload\(\)"/.test(page));

check('the connection picker offers the ACTIVE PROFILE\'s connections and nothing else',
  /function ctStageConnOptions\(\)/.test(page)
  && /\['src', 'tgt'\]\.forEach/.test(page)
  && /No connection on the active profile/.test(page));

check('the run is gated by the same write guard as everything else, and PRD must be typed',
  /cpGuardWrite/.test(page) && /requiresTypedConfirm/.test(page) && /Type CREATE to create/.test(page));

check('the page has its own in-flight flag and never sets one from inside a callback',
  (() => {
    const body = page.slice(page.indexOf('CT.staging = false;'));
    // Declaration, the set before the run, and the clear in that run's finally.
    return (page.match(/CT\.staging = /g) || []).length === 3
      && /CT\.staging = true;\s*\n\s*renderButtons\(\); renderStage\(\);/.test(page)
      && /finally \{\s*\n\s*CT\.staging = false;/.test(page) && !!body;
  })());

check('the progress line reports created, skipped or failed per table, with the error',
  /onProgress: \(p\) => \{[\s\S]{0,400}p\.outcome\.status === 'failed' \? 'failed: ' \+ p\.outcome\.error/.test(page));

check('nothing was added to netlify.toml and no new backend route was created',
  (() => {
    const toml = read('netlify.toml');
    return !/staging/i.test(toml)
      && !fs.existsSync(path.join(ROOT, 'netlify', 'functions', 'template-staging.js'))
      && /action: 'execute'/.test(read('public', 'cygenix-schema-graph.js'));
  })());

check('the three new client actions are on the audit allowlist',
  (() => {
    const a = read('netlify', 'functions', 'lib', 'audit-schema.js');
    return /'template\.create-staging': 'mapping'/.test(a)
      && /'template\.map-send':\s+'mapping'/.test(a)
      && /'template\.map-remove':\s+'mapping'/.test(a);
  })());

check('a run against a PRD profile is filed under the always-on category, by the server',
  /cls === 'PRD' \? 'PROD'/.test(page)
  && /environment === 'PROD' \? 'prod' : input\.category/.test(read('netlify', 'functions', 'lib', 'audit-schema.js')));

check('no emoji or banned glyphs were introduced (the arrow is the one the product already uses)',
  !/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/u.test(read('public', 'cygenix-template-staging.js')
    + read('public', 'cygenix-template-mapping.js')));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
if (fail) process.exit(1);
})();
