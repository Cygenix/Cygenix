// tests/data-analyser-handoff.test.js — a profiled file becomes a source
// without a single cell value leaving the browser that profiled it.
//
// WHY THIS SUITE IS SHAPED THE WAY IT IS
//
// The Data Analyser page says "nothing leaves this page". Handing a file to
// Object Mapping means writing a job record, and a job record is synced to
// the account. So the record may carry the column PROFILE and must not carry
// the data. That is not a convention to remember at each field — it is the
// first thing this suite checks, against the real engine, with a file whose
// cell values are recognisable: the serialised job must not contain any of
// them. Every later field somebody adds to the record runs into that check.
//
// The second half is the contract with Object Mapping: the srcTable this
// produces has to be indistinguishable, to selectTable() and everything
// downstream, from one connectSrc() built from a database — same keys, same
// column shape, SQL type strings rather than the engine's categories.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const P = (...s) => path.join(__dirname, '..', ...s);
const read = (...s) => fs.readFileSync(P(...s), 'utf8');

const CT = require(P('public', 'cygenix-analyser.js'));
const H = require(P('public', 'cygenix-analyser-handoff.js'));

console.log('Data Analyser → Object Mapping hand-off\n');

/* ── The file, with the profiler's traps and recognisable values ────────── */

const CSV = [
  'order_id,customer,order_date,amount,postcode,active,notes',
  ...Array.from({ length: 12 }, (_, i) => [
    1001 + i,
    i === 0 ? '"Smith, John"' : i === 1 ? '"O\'Brien, Ann"' : 'Customer ' + i,
    (i % 2 ? '03/04/2024' : '01/02/2024'),
    '£' + (1042.5 + i * 10).toFixed(2),
    i === 1 ? '00417' : 'LS1 4AP',
    i % 3 ? 'true' : 'false',
    i === 5 ? 'ZEBRA-MARKER-VALUE' : '',
  ].join(',')),
].join('\n');

const ds = CT.parse(CSV);
const cols = CT.profile(ds);
const result = { dataset: ds, columns: cols };
const sqlType = CT.dialects.sqlserver.type;

const job = H.buildFileSourceJob(result, { name: 'orders (final).csv', sqlType, projectId: 'p1', now: '2026-09-05T10:00:00.000Z' });
const json = JSON.stringify(job);

/* ── 1. Not one cell value leaves ───────────────────────────────────────── */

['Smith, John', "O'Brien", 'LS1 4AP', '00417', 'ZEBRA-MARKER-VALUE', '1042.5', '01/02/2024', 'Customer 3']
  .forEach((v) => check('the record does not contain the cell value ' + JSON.stringify(v), json.indexOf(v) === -1));
check('and carries no samples, min, max or distinct lists from the profile',
  !/"samples"|"min"|"max"|"distinct"|"mixedTypes"/.test(json),
  'those fields hold values from the data; only the profile may travel');
check('but it does carry every column name',
  ds.columns.every((c) => json.indexOf('"' + c + '"') !== -1));

/* ── 2. The record ──────────────────────────────────────────────────────── */

check('it is a simple-map migration job, the kind Object Mapping opens',
  job.jobType === 'simple-map' && job.type === 'migration');
check('the discriminator is set, and isFileJob reads it', job.sourceKind === 'file' && H.isFileJob(job));
check('a database job is not a file job',
  !H.isFileJob({ id: 'x', source: 'dbo.Customers', columnMapping: [] }) && !H.isFileJob(null));
check('the source reference cannot collide with a schema.table name',
  job.source === 'file:orders (final).csv' && job.sourceTable === job.source
  && job.source.indexOf(H.SOURCE_PREFIX) === 0);
check('the id is a job id, the name says what is still to choose',
  /^job_\d+$/.test(job.id) && job.name === 'orders (final).csv → (choose a target)');
check('it belongs to the project it was made in', job.projectId === 'p1');
check('it is a draft with no target and no mapping yet',
  job.status === 'draft' && job.target === '' && Array.isArray(job.columnMapping) && job.columnMapping.length === 0);
check('the source system names the format, for the job card',
  job.sourceSystem === 'File (CSV)' && job.fileSource.formatLabel === 'CSV');
check('row counts are the file\'s, not a sample\'s',
  job.fileSource.rowCount === 12 && job.fileSource.rowsProfiled === 12 && job.totalRows === 12);

/* ── 3. The columns, as Object Mapping needs them ───────────────────────── */

const byName = Object.fromEntries(job.fileSource.columns.map((c) => [c.name, c]));
check('every profiled column is present', job.fileSource.columns.length === 7);
check('types are SQL type strings, not engine categories',
  job.fileSource.columns.every((c) => /^[A-Z]/.test(c.type)),
  job.fileSource.columns.map((c) => c.name + ':' + c.type).join(', '));
check('an integer column is INT', byName.order_id.type === 'INT', byName.order_id.type);
check('a boolean column is BIT', byName.active.type === 'BIT', byName.active.type);
check('the money column is DECIMAL, with the engine\'s finer name alongside',
  /^DECIMAL\(/.test(byName.amount.type) && byName.amount.analyserType === 'money', byName.amount.type);
check('the postcode column with a leading-zero value is NVARCHAR, not a number',
  /^NVARCHAR\(/.test(byName.postcode.type) && byName.postcode.analyserType === 'string', byName.postcode.type);
check('nullability is carried', byName.notes.nullable === true && byName.order_id.nullable === false);
// `customer` is twelve distinct non-null strings, so the engine rightly
// calls it a candidate too — per-column candidacy is not a single choice.
// The DDL picks one; the record carries every candidate for Object Mapping.
check('the twelve-row unique integer is a primary-key candidate, carried on the record',
  byName.order_id.primaryKeyCandidate === true && job.fileSource.primaryKeys.includes('order_id'),
  job.fileSource.primaryKeys.join(','));
check('issue CODES are carried, never their example values',
  byName.order_date.issues.some((i) => i.code === 'AMBIGUOUS_DATE')
  && byName.amount.issues.some((i) => i.code === 'CURRENCY_SYMBOLS')
  && byName.order_date.issues.every((i) => Object.keys(i).sort().join() === 'code,severity'));
check('the job\'s warnings are sentences, highest severity first',
  job.warnings.length >= 2 && /^order_date: AMBIGUOUS_DATE — /.test(job.warnings[0]),
  job.warnings.slice(0, 2).join(' | '));

/* ── 4. srcTable, indistinguishable from a database one ─────────────────── */

const st = H.fileSourceToSrcTable(job);
check('it has every key connectSrc() puts on a table',
  ['value', 'label', 'schema', 'name', 'fullName', 'rowCount', 'objType', 'columns', 'primaryKeys', 'foreignKeys']
    .every((k) => k in st), Object.keys(st).join(','));
check('value matches the job\'s source reference, which is how checkEditMode finds it',
  st.value === job.sourceTable && st.fullName === job.sourceTable);
check('the label is the file name and the identifier is SQL-safe',
  st.label === 'orders (final).csv' && st.name === 'orders_final');
check('columns are {name, type} objects, the shape getAllSourceCols() reads',
  st.columns.every((c) => typeof c.name === 'string' && typeof c.type === 'string'));
check('objType is neither table nor view, and isFile says what it is',
  st.objType === 'file' && st.isFile === true);
check('foreignKeys is an array — code that spreads it must not meet undefined', Array.isArray(st.foreignKeys));
check('a database job produces no srcTable here', H.fileSourceToSrcTable({ source: 'dbo.X' }) === null);

check('the banner reads like connectSrc()\'s',
  H.describeFileSource(job) === 'Source: orders (final).csv · CSV · 7 columns · 12 rows · profiled by the Data Analyser',
  H.describeFileSource(job));
check('the runner\'s refusal names the file and the way forward',
  /orders \(final\)\.csv/.test(H.runRefusal(job)) && /Data import/.test(H.runRefusal(job)));

/* ── 5. Identifiers and guards ──────────────────────────────────────────── */

check('identifierFrom handles the ugly cases',
  H.identifierFrom('2024 sales (Q1).xlsx') === 'f_2024_sales_Q1' && H.identifierFrom('') === 'file'
  && H.identifierFrom('.csv') === 'file');
let threw = '';
try { H.buildFileSourceJob(result, {}); } catch (e) { threw = e.message; }
check('building without a type function fails loudly rather than emitting empty types', /sqlType/.test(threw), threw);
threw = '';
try { H.buildFileSourceJob(null, { sqlType }); } catch (e) { threw = e.message; }
check('and so does building without a result', /result/.test(threw), threw);

/* ── 6. The wiring on both pages ────────────────────────────────────────── */

const om = read('public', 'object-mapping-app.js');
check('Object Mapping installs a file source before its restore path polls',
  /installFileSource\(/.test(om) && /CygenixAnalyserHandoff\.fileSourceToSrcTable/.test(om));
check('connectSrc() cannot overwrite an installed file source when a real connection also exists',
  (om.match(/_fileSourceJob/g) || []).length >= 4,
  'the guard has to sit both before the fetch and after the await');
check('saveAsJob() keeps the file fields, which it otherwise rebuilds the record without',
  /sourceKind: *'file'/.test(om) && /fileSource: *_fileSourceJob\.fileSource/.test(om));
check('the evidence map refuses a file source rather than sampling a connection that is not there',
  /isFile[\s\S]{0,200}Evidence map/.test(om) || /Evidence map[\s\S]{0,300}isFile/.test(om));

const omPage = read('public', 'object_mapping.html');
check('the Object Mapping page loads the hand-off module before its app',
  /cygenix-analyser-handoff\.js/.test(omPage)
  && omPage.indexOf('cygenix-analyser-handoff.js') < omPage.indexOf('object-mapping-app.js'));

const runner = read('public', 'project-builder-app.js');
check('the runner refuses a file job with the hand-off\'s reason, instead of SELECTing from a file name',
  /CygenixAnalyserHandoff[\s\S]{0,300}\.isFileJob\(/.test(runner) && /\.runRefusal\(/.test(runner)
  && /SOURCE_PREFIX/.test(runner));

// The analyser lives on the Data import tab of the dashboard's Connections
// view now (it was its own page); the hand-off went with it.
const page = read('public', 'dashboard.html');
const app = read('public', 'dashboard-app.js');
check('the Data import tab offers the hand-off once there is a result',
  /Use as a source in Object Mapping/.test(page) && /onResult: function\(\)\{[\s\S]{0,200}handoff\.hidden = false/.test(app));
check('it flushes the sync queue BEFORE navigating, or ensureKey wipes the new job on arrival',
  /saveNow\(\)[\s\S]{0,600}\/object-mapping\?edit=/.test(app),
  'Object Mapping hydrates cygenix_jobs from the cloud on load and that overwrites local');
check('and the tab copy says what leaves when you hand it on',
  /Nothing leaves this page/.test(page) && /names and (inferred )?types, (never|not) the rows/.test(page));
check('and that the import steps are how the rows themselves get somewhere',
  /To put the rows themselves in a database, use the import steps below/.test(page));

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
