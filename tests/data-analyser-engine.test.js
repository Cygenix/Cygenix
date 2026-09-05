// tests/data-analyser-engine.test.js
//
// The Data Analyser engine's own test suite, brought into this repository's
// chain. It arrived as test/run.js beside the engine, with 123 assertions
// covering every format it reads, quoting and escaping edge cases, ragged and
// headerless input, BOM and CRLF, type inference, all four SQL dialects, round
// trips, column mapping, and robustness against empty, broken and
// unstructured input.
//
// It is kept as close to verbatim as possible on purpose: when the vendor
// ships a new engine, the honest way to take it is to diff their test file
// against this one and re-apply the small changes below, rather than to
// re-derive coverage. Those changes are:
//
//   1. It requires public/cygenix-analyser.js — the engine's name here, for
//      the collision reason given at the top of that file.
//   2. XML and HTML parsing use DOMParser, which Node does not have. Upstream
//      requires jsdom for it. jsdom is not a dependency of this repository,
//      so those SIX assertions are gated: they run when jsdom is installed
//      and are reported as SKIP, not as PASS, when it is not. Every other
//      assertion, including XML *detection* and XML *emission*, runs
//      regardless — only parsing needs a DOM.
//   3. Output is in the house PASS/FAIL shape so it reads like every other
//      suite in `npm test`.
'use strict';

const path = require('path');

let pass = 0, fail = 0, skip = 0;
function ok(name, cond, extra) {
  if (cond) { pass++; console.log('  PASS  ' + name); }
  else { fail++; console.log('  FAIL  ' + name + (extra !== undefined ? '  → ' + JSON.stringify(extra).slice(0, 300) : '')); }
}
function skipped(name, why) { skip++; console.log('  SKIP  ' + name + '  (' + why + ')'); }
function section(s) { console.log('\n== ' + s + ' =='); }

// A DOM if one is available. The engine only touches DOMParser inside the
// XML and HTML parsers, so its absence costs those and nothing else.
let hasDom = false;
try {
  const { JSDOM } = require('jsdom');
  global.DOMParser = new JSDOM('').window.DOMParser;
  hasDom = true;
} catch (e) { /* not installed — the six DOM-bound assertions are skipped below */ }
const NO_DOM = 'needs DOMParser; install jsdom to run';

const CT = require(path.join(__dirname, '..', 'public', 'cygenix-analyser.js'));

console.log('Data Analyser engine — the vendor suite, against the renamed module'
  + (hasDom ? '' : '\n(jsdom not installed: XML/HTML parse assertions will be skipped)'));

ok('the engine is exported under its new name and not the colliding one',
  typeof CT.detect === 'function' && typeof CT.parse === 'function' && typeof CT.profile === 'function');

/* ---------------- CSV ---------------- */
section('CSV');
const csv = `id,name,email,signup_date,balance,active
1,"Smith, John",john@example.com,2024-01-15,1042.50,true
2,"O""Brien, Ann",ann@example.co.uk,2024-02-03,,false
3,"Multi
line name",bob@example.com,2024-03-21,88888888888,true`;
let d = CT.detect(csv);
ok('detects csv', d.format === 'csv', d);
ok('csv confidence > 0.9', d.confidence > 0.9, d.confidence);
let ds = CT.parse(csv);
ok('3 rows', ds.rows.length === 3, ds.rows.length);
ok('6 columns', ds.columns.length === 6, ds.columns);
ok('quoted comma kept', ds.rows[0][1] === 'Smith, John', ds.rows[0][1]);
ok('escaped quote', ds.rows[1][1] === 'O"Brien, Ann', ds.rows[1][1]);
ok('embedded newline', ds.rows[2][1].indexOf('\n') > 0, ds.rows[2][1]);
let cols = CT.profile(ds);
ok('id -> integer', cols[0].type === 'integer', cols[0].type);
ok('email -> email', cols[2].type === 'email', cols[2].type);
ok('date -> date', cols[3].type === 'date', cols[3].type);
ok('balance -> decimal + nullable', cols[4].type === 'decimal' && cols[4].nullable, cols[4]);
ok('active -> boolean', cols[5].type === 'boolean', cols[5].type);
ok('3-row sample claims no PK', cols[0].primaryKeyCandidate === false && cols[0].keyEvidenceWeak === true, cols[0]);
ok('bigint overflow flagged', cols[4].issues.some(i => i.code === 'INT_OVERFLOW') || cols[4].max > 2147483647, cols[4].max);

/* ---------------- delimiters ---------------- */
section('Other delimiters');
ok('tsv', CT.detect('a\tb\tc\n1\t2\t3\n4\t5\t6').format === 'tsv');
ok('semicolon', CT.detect('a;b;c\n1;2;3\n4;5;6').format === 'csv-semicolon');
ok('pipe', CT.detect('a|b|c\n1|2|3\n4|5|6').format === 'psv');
const tsvDs = CT.parse('a\tb\tc\n1\t2\t3');
ok('tsv parses', tsvDs.columns.join(',') === 'a,b,c' && tsvDs.rows[0][2] === '3', tsvDs.rows);

/* ---------------- ragged / no header ---------------- */
section('Edge cases');
const ragged = 'a,b,c\n1,2\n3,4,5,6';
const rds = CT.parse(ragged);
ok('ragged padded to width', rds.rows.every(r => r.length === 3), rds.rows);
ok('ragged warning', rds.warnings.some(w => w.code === 'RAGGED_ROWS'), rds.warnings);
const noHead = CT.parse('1,2,3\n4,5,6\n7,8,9');
ok('no header detected', noHead.warnings.some(w => w.code === 'NO_HEADER'), noHead.columns);
const dupCols = CT.parse('id,id,id\n1,2,3');
ok('duplicate headers deduped', new Set(dupCols.columns).size === 3, dupCols.columns);
const leadingZero = CT.profile(CT.parse('postcode\n01234\n00987\n04567'));
ok('leading zeros stay string', leadingZero[0].type === 'string', leadingZero[0].type);

/* ---------------- JSON ---------------- */
section('JSON');
const json = JSON.stringify([
  { id: 1, customer: { name: 'Ann', address: { city: 'Leeds', postcode: 'LS1 1AA' } }, tags: ['a', 'b'], total: 10.5 },
  { id: 2, customer: { name: 'Bob', address: { city: 'Hull', postcode: 'HU1 1AA' } }, tags: ['c'], total: 3 }
]);
ok('detects json', CT.detect(json).format === 'json');
const jds = CT.parse(json);
ok('json flattened', jds.columns.indexOf('customer.address.city') >= 0, jds.columns);
ok('scalar array joined', jds.rows[0][jds.columns.indexOf('tags')] === 'a; b', jds.rows[0]);
ok('json 2 rows', jds.rows.length === 2);
const wrapped = CT.parse('{"status":"ok","data":[{"a":1},{"a":2}]}');
ok('unwraps data property', wrapped.rows.length === 2 && wrapped.warnings.some(w => w.code === 'UNWRAPPED'), wrapped.warnings);

/* ---------------- NDJSON ---------------- */
section('NDJSON');
const nd = '{"a":1,"b":"x"}\n{"a":2,"b":"y"}\n{"a":3,"b":"z"}';
ok('detects ndjson', CT.detect(nd).format === 'ndjson', CT.detect(nd));
const ndds = CT.parse(nd);
ok('ndjson 3 rows', ndds.rows.length === 3, ndds.rows.length);
const ndBad = CT.parse('{"a":1}\nnot json at all here\n{"a":2}', { format: 'ndjson' });
ok('bad ndjson lines skipped', ndBad.rows.length === 2 && ndBad.warnings.some(w => w.code === 'BAD_LINES'), ndBad.warnings);

/* ---------------- XML ---------------- */
section('XML');
const xml = `<?xml version="1.0"?><orders>
<order id="1"><customer>Ann</customer><total>10.50</total></order>
<order id="2"><customer>Bob</customer><total>3.00</total></order>
</orders>`;
ok('detects xml', CT.detect(xml).format === 'xml');
if (hasDom) {
  const xds = CT.parse(xml);
  ok('xml 2 rows', xds.rows.length === 2, xds.rows);
  ok('xml attribute captured', xds.columns.indexOf('@id') >= 0, xds.columns);
  ok('xml child element', xds.columns.indexOf('customer') >= 0, xds.columns);
} else {
  skipped('xml 2 rows', NO_DOM); skipped('xml attribute captured', NO_DOM); skipped('xml child element', NO_DOM);
}
const xmlNoDecl = CT.detect('<root><a>1</a></root>');
ok('xml without declaration', xmlNoDecl.format === 'xml', xmlNoDecl);

/* ---------------- HTML table ---------------- */
section('HTML table');
const html = '<!doctype html><html><body><table><tr><th>Name</th><th>Qty</th></tr><tr><td>Widget</td><td>4</td></tr><tr><td>Bolt</td><td>9</td></tr></table></body></html>';
ok('detects html-table', CT.detect(html).format === 'html-table', CT.detect(html));
if (hasDom) {
  const hds = CT.parse(html);
  ok('html 2 rows', hds.rows.length === 2, hds.rows);
  ok('html header from th', hds.columns.join(',') === 'Name,Qty', hds.columns);
} else {
  skipped('html 2 rows', NO_DOM); skipped('html header from th', NO_DOM);
}

/* ---------------- YAML ---------------- */
section('YAML');
const yaml = `- id: 1
  name: Ann
  active: true
- id: 2
  name: Bob
  active: false`;
ok('detects yaml', CT.detect(yaml).format === 'yaml', CT.detect(yaml));
const yds = CT.parse(yaml);
ok('yaml 2 rows', yds.rows.length === 2, yds.rows);
ok('yaml columns', yds.columns.join(',') === 'id,name,active', yds.columns);
ok('yaml subset warning', yds.warnings.some(w => w.code === 'YAML_SUBSET'), yds.warnings);

/* ---------------- INI ---------------- */
section('INI');
const ini = '[database]\nhost=localhost\nport=1433\n\n[auth]\nmode=integrated';
ok('detects ini', CT.detect(ini).format === 'ini', CT.detect(ini));
const ids = CT.parse(ini);
ok('ini 3 rows', ids.rows.length === 3, ids.rows);
ok('ini sections', ids.rows[0][0] === 'database' && ids.rows[2][0] === 'auth', ids.rows);

/* ---------------- SQL dump ---------------- */
section('SQL INSERT dump');
const dump = `INSERT INTO [dbo].[Customers] ([Id], [Name], [Balance]) VALUES (1, N'Ann O''Brien', 10.50), (2, N'Bob', NULL);
INSERT INTO [dbo].[Customers] ([Id], [Name], [Balance]) VALUES (3, N'Cy', 7.25);`;
ok('detects sql-insert', CT.detect(dump).format === 'sql-insert', CT.detect(dump));
const sds = CT.parse(dump);
ok('sql 3 rows', sds.rows.length === 3, sds.rows);
ok('sql escaped quote', sds.rows[0][1] === "Ann O'Brien", sds.rows[0][1]);
ok('sql NULL literal', sds.rows[1][2] === null, sds.rows[1]);
ok('sql table name', sds.meta.table === 'dbo.Customers', sds.meta.table);

/* ---------------- Fixed width ---------------- */
section('Fixed width');
const fw = [
  'ID    NAME            CITY      ',
  '1     Ann Smith       Leeds     ',
  '2     Bob Jones       Hull      ',
  '3     Cy Patel        Manchester'
].join('\n');
const fwd = CT.detect(fw);
ok('detects fixed-width', fwd.format === 'fixed-width', fwd);
const fds = CT.parse(fw);
ok('fixed-width 3 rows', fds.rows.length === 3, fds.rows);
ok('fixed-width trimmed', fds.rows[0][1] === 'Ann Smith', fds.rows[0]);

/* ---------------- Binary magic ---------------- */
section('Binary detection');
const zip = new Uint8Array([0x50, 0x4B, 0x03, 0x04, 0, 0, 0, 0]);
ok('xlsx magic bytes', CT.detect(zip.buffer).format === 'xlsx', CT.detect(zip.buffer));
const pdf = new Uint8Array([0x25, 0x50, 0x44, 0x46, 0x2d]);
ok('pdf magic bytes', CT.detect(pdf.buffer).format === 'pdf');
let threw = false;
try { CT.parse(pdf.buffer); } catch (e) { threw = /PDF/.test(e.message); }
ok('pdf parse throws helpfully', threw);

/* ---------------- Type inference edge cases ---------------- */
section('Type inference');
const amb = CT.profile(CT.parse('d\n01/02/2024\n03/04/2024\n05/06/2024'));
ok('ambiguous date flagged', amb[0].issues.some(i => i.code === 'AMBIGUOUS_DATE'), amb[0].issues);
const uuidCol = CT.profile(CT.parse('k\n3f2504e0-4f89-11d3-9a0c-0305e82c3301\n1b4e28ba-2fa1-11d2-883f-0016d3cca427'));
ok('uuid detected', uuidCol[0].type === 'uuid', uuidCol[0].type);
const money = CT.profile(CT.parse('price\n£1,024.50\n£3.00\n£12,000.00'));
ok('money detected', money[0].type === 'money', money[0].type);
ok('money issue raised', money[0].issues.some(i => i.code === 'CURRENCY_SYMBOLS'));
const mixed = CT.profile(CT.parse('v\n1\n2\nthree\n4\n5'));
ok('mixed types flagged', mixed[0].issues.some(i => i.code === 'MIXED_TYPES'), mixed[0]);
ok('mixed confidence < 1', mixed[0].confidence < 1, mixed[0].confidence);
const nulls = CT.profile(CT.parse('v\n1\nNULL\nN/A\n\n4'));
ok('null tokens counted', nulls[0].nulls === 3, nulls[0]);
const dt = CT.profile(CT.parse('v\n2024-01-01T10:00:00Z\n2024-02-01T11:30:00Z'));
ok('iso datetime', dt[0].type === 'datetime', dt[0].type);

/* ---------------- DDL ---------------- */
section('DDL generation');
const ddlSrc = CT.parse(csv);
const ddlCols = CT.profile(ddlSrc);
const tsql = CT.ddl(ddlSrc, { dialect: 'sqlserver', table: 'Customers' });
ok('t-sql create table', /CREATE TABLE \[dbo\]\.\[Customers\]/.test(tsql), tsql.split('\n').slice(0, 6));
ok('t-sql nvarchar', /NVARCHAR\(/.test(tsql));
ok('t-sql bit for boolean', /\[active\] BIT/.test(tsql), tsql);
ok('t-sql int for id', /\[id\] INT NOT NULL/.test(tsql), tsql);
const pg = CT.ddl(ddlSrc, { dialect: 'postgres', table: 'customers' });
ok('postgres quoting', /CREATE TABLE "customers"/.test(pg), pg.split('\n').slice(0, 6));
ok('postgres boolean', /"active" BOOLEAN/.test(pg), pg);
const my = CT.ddl(ddlSrc, { dialect: 'mysql', table: 'customers' });
ok('mysql backticks', /CREATE TABLE `customers`/.test(my), my.split('\n').slice(0, 5));
ok('mysql tinyint', /`active` TINYINT\(1\)/.test(my), my);
const lite = CT.ddl(ddlSrc, { dialect: 'sqlite', table: 'customers' });
ok('sqlite text/integer only', !/VARCHAR|NVARCHAR|DECIMAL/.test(lite), lite);
ok('ddl explains why no PK on a tiny sample', /too small a sample/.test(tsql), tsql);
const bigCsv = 'id,label\n' + Array.from({ length: 30 }, (_, i) => (i + 1) + ',row' + i).join('\n');
const bigDs2 = CT.parse(bigCsv), bigCols = CT.profile(bigDs2);
ok('30-row sample finds PK', bigCols[0].primaryKeyCandidate === true, bigCols[0]);
const bigDdl = CT.ddl(bigDs2, { dialect: 'sqlserver', table: 'T', columns: bigCols });
ok('only one column marked PK', (bigDdl.match(/primary key candidate/g) || []).length === 1, bigDdl);
ok('commas precede comments', !/--[^\n]*,\s*$/m.test(bigDdl), bigDdl);
ok('every column line but the last ends in a comma or comment', /\[id\] INT NOT NULL,\s+--/.test(bigDdl), bigDdl);
const badName = CT.ddl(CT.parse('first name,2nd col\na,b'), { dialect: 'postgres', table: 'x' });
ok('identifiers sanitised', /"first_name"/.test(badName) && /"c_2nd_col"/.test(badName), badName);
void ddlCols;

/* ---------------- SQL inserts ---------------- */
section('SQL inserts');
const ins = CT.sqlInserts(ddlSrc, { dialect: 'sqlserver', table: 'Customers' });
ok('inserts generated', /INSERT INTO \[dbo\]\.\[Customers\]/.test(ins));
ok('nulls emitted as NULL', /NULL/.test(ins));
ok('unicode string prefix', /N'/.test(ins));
ok('double-quote in value survives', /N'O"Brien, Ann'/.test(ins), ins.slice(0, 400));
const apos = CT.sqlInserts(CT.parse("a,b\nO'Brien,x"), { dialect: 'sqlserver', table: 't' });
ok("apostrophe doubled", /N'O''Brien'/.test(apos), apos);
const mysqlIns = CT.sqlInserts(CT.parse("a\nO'Brien"), { dialect: 'mysql', table: 't' });
ok('mysql escapes with backslash', /O\\'Brien/.test(mysqlIns), mysqlIns);
const batched = CT.sqlInserts(CT.parse('a\n' + Array.from({ length: 5 }, (_, i) => i).join('\n')), { table: 't', batchSize: 2 });
ok('batching splits statements', (batched.match(/INSERT INTO/g) || []).length === 3, batched);

/* ---------------- Round trips ---------------- */
section('Round trips');
const rtCsv = CT.transform(json, 'csv');
ok('json -> csv', rtCsv.output.split('\n')[0].indexOf('customer.address.city') >= 0, rtCsv.output.split('\n')[0]);
const backToJson = CT.parse(rtCsv.output);
ok('csv round trips row count', backToJson.rows.length === 2, backToJson.rows.length);
const rtJson = CT.transform(csv, 'json');
const parsedBack = JSON.parse(rtJson.output);
ok('csv -> json typed', parsedBack[0].id === 1 && parsedBack[0].active === true, parsedBack[0]);
ok('csv -> json null preserved', parsedBack[1].balance === null, parsedBack[1]);
const rtXml = CT.transform(csv, 'xml');
ok('csv -> xml', /<row>/.test(rtXml.output) && /<signup_date>/.test(rtXml.output));
if (hasDom) {
  const reparsedXml = CT.parse(rtXml.output);
  ok('xml round trips row count', reparsedXml.rows.length === 3, reparsedXml.rows.length);
} else {
  skipped('xml round trips row count', NO_DOM);
}
const rtYaml = CT.transform(csv, 'yaml');
ok('csv -> yaml', /^- id: 1/m.test(rtYaml.output), rtYaml.output.split('\n').slice(0, 3));
const rtMd = CT.transform(csv, 'markdown');
ok('csv -> markdown', /^\| id/.test(rtMd.output), rtMd.output.split('\n')[0]);
const rtNd = CT.transform(csv, 'ndjson');
ok('csv -> ndjson lines', rtNd.output.split('\n').length === 3, rtNd.output);
const rtTsv = CT.transform(csv, 'tsv');
ok('csv -> tsv redetects as tsv', CT.detect(rtTsv.output).format === 'tsv', CT.detect(rtTsv.output));
const nastyCsv = CT.to.csv(CT.parse('a,b\n"x,y","he said ""hi"""'));
ok('csv escaping round trips', CT.parse(nastyCsv).rows[0][1] === 'he said "hi"', CT.parse(nastyCsv).rows[0]);

/* ---------------- Column mapping ---------------- */
section('Column mapping');
const srcCols = CT.profile(CT.parse('cust_id,fname,lname,email_addr,dob,acct_balance\n1,Ann,Smith,a@b.com,1990-01-01,10.5'));
const target = [
  { name: 'CustomerID', type: 'integer', required: true },
  { name: 'FirstName', type: 'string' },
  { name: 'LastName', type: 'string' },
  { name: 'EmailAddress', type: 'string' },
  { name: 'DateOfBirth', type: 'date' },
  { name: 'AccountBalance', type: 'decimal' },
  { name: 'LoyaltyTier', type: 'string' }
];
const m = CT.map(srcCols, target);
const byTarget = Object.fromEntries(m.mappings.map(x => [x.target, x.source]));
ok('cust_id -> CustomerID', byTarget.CustomerID === 'cust_id', m.mappings);
ok('fname -> FirstName', byTarget.FirstName === 'fname', byTarget);
ok('lname -> LastName', byTarget.LastName === 'lname', byTarget);
ok('email_addr -> EmailAddress', byTarget.EmailAddress === 'email_addr', byTarget);
ok('dob -> DateOfBirth', byTarget.DateOfBirth === 'dob', byTarget);
ok('acct_balance -> AccountBalance', byTarget.AccountBalance === 'acct_balance', byTarget);
ok('LoyaltyTier unmapped', m.unmappedTarget.indexOf('LoyaltyTier') >= 0, m.unmappedTarget);
ok('no source left over', m.unmappedSource.length === 0, m.unmappedSource);
ok('every mapping has a confidence', m.mappings.every(x => x.confidence > 0 && x.confidence <= 1));
ok('one-to-one only', new Set(m.mappings.map(x => x.source)).size === m.mappings.length);
const typeClash = CT.map([{ name: 'amount', type: 'string' }], [{ name: 'amount', type: 'integer' }]);
ok('type clash noted', typeClash.mappings[0].notes.some(n => /conversion is required/.test(n)), typeClash.mappings[0]);

/* ---------------- transform() envelope ---------------- */
section('transform() envelope');
const t = CT.transform(csv, 'ddl', { dialect: 'postgres', table: 'customers' });
ok('envelope has detection', t.detection && t.detection.format === 'csv');
ok('envelope has columns', t.columns.length === 6);
ok('envelope rowCount', t.rowCount === 3);
ok('envelope mime', t.mime === 'application/sql');
ok('envelope issues array', Array.isArray(t.issues));
let badTarget = false;
try { CT.transform(csv, 'parquet'); } catch (e) { badTarget = /Unknown target/.test(e.message); }
ok('unknown target rejected', badTarget);

/* ---------------- Robustness ---------------- */
section('Robustness');
let emptyThrew = false;
try { CT.parse(''); } catch (e) { emptyThrew = /empty/i.test(e.message); }
ok('empty input rejected clearly', emptyThrew);
const garbage = CT.parse('just some prose about nothing\nanother line of prose here\nand a third');
ok('unstructured falls back to text', garbage.format === 'text' || garbage.rows.length === 3, garbage.format);
const brokenJson = CT.parse('{"a":1,');
ok('broken json still yields something', brokenJson.rows.length >= 1, brokenJson.format);
const bom = CT.parse('﻿a,b\n1,2');
ok('BOM stripped', bom.columns[0] === 'a', bom.columns);
const crlf = CT.parse('a,b\r\n1,2\r\n3,4');
ok('CRLF handled', crlf.rows.length === 2 && crlf.rows[1][1] === '4', crlf.rows);
const bigDs = CT.parse('a,b\n' + Array.from({ length: 5000 }, (_, i) => i + ',' + i * 2).join('\n'));
ok('5000 rows parsed', bigDs.rows.length === 5000, bigDs.rows.length);
const t0 = Date.now(); CT.profile(bigDs); const elapsed = Date.now() - t0;
ok('profiling 5000 rows under 500ms', elapsed < 500, elapsed + 'ms');
const limited = CT.parse('a\n1\n2\n3\n4\n5', { limit: 2 });
ok('limit option honoured', limited.rows.length === 2 && limited.meta.truncatedFrom === 5, limited.meta);

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed' + (skip ? ', ' + skip + ' skipped (no DOM)' : ''));
process.exit(fail ? 1 : 0);
