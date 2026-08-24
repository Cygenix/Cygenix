// Tests for schema-action parity between the two database backends.
//
// The Schema Explorer draws its maps from three actions: schema-tables,
// schema-fks and schema-columns. A direct connection string answers them via
// the Netlify db-connect function; an Azure Function connection answers them
// via /api/db. Only db-connect implemented schema-tables and schema-fks, so
// every graphical map came up empty on an Azure-mode connection with
// "Unknown action: schema-tables" while the same database drew fine on-prem.
//
// Two backends answering the same action have to answer it the same way.
// These tests pin that: same actions, same SQL, same response shape.
'use strict';
const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};
const P = (...p) => path.join(__dirname, '..', ...p);
const azure   = fs.readFileSync(P('azure-function', 'src', 'index.js'), 'utf8');
const netlify = fs.readFileSync(P('netlify', 'functions', 'db-connect.js'), 'utf8');
const graph   = fs.readFileSync(P('public', 'cygenix-schema-graph.js'), 'utf8');

console.log('Schema actions — the two backends answer alike\n');

// ── 1. The client's actions exist on both sides ─────────────────────────────
const used = [...new Set([...graph.matchAll(/action:\s*'(schema[a-z-]*)'/g)].map(m => m[1]))];
check('the map builder calls exactly the three schema actions',
  used.length === 3 && ['schema-tables', 'schema-fks', 'schema-columns'].every(a => used.includes(a)),
  used.join(', '));
for (const action of used) {
  check("both backends handle '" + action + "'",
    azure.includes("case '" + action + "':") && netlify.includes("case '" + action + "':"),
    'azure=' + azure.includes("case '" + action + "':") + ' netlify=' + netlify.includes("case '" + action + "':"));
}

// ── 2. Same question, same SQL ──────────────────────────────────────────────
// Normalised on whitespace only: the two files indent differently, but a
// difference in the query itself is a difference in what the map shows.
const norm = (s) => s.replace(/\s+/g, ' ').trim();
const caseBody = (src, action) => {
  const i = src.indexOf("case '" + action + "':");
  if (i < 0) return '';
  const j = src.indexOf("\n        case '", i + 10);
  const k = src.indexOf("\n      case '", i + 10);
  const end = Math.min(...[j, k, src.length].filter(x => x > 0));
  return src.slice(i, end);
};
const sqlIn = (body) => [...body.matchAll(/`([^`]*(?:SELECT|select)[^`]*)`/g)].map(m => norm(m[1]));

for (const action of ['schema-tables', 'schema-fks']) {
  const a = sqlIn(caseBody(azure, action));
  const n = sqlIn(caseBody(netlify, action));
  check("'" + action + "' runs the same query on both backends",
    a.length > 0 && a.length === n.length && a.every((q, i) => q === n[i]),
    a.length !== n.length ? ('azure has ' + a.length + ' quer(y|ies), netlify ' + n.length)
      : (a.find((q, i) => q !== n[i]) || '').slice(0, 110));
}

// ── 3. Same response shape ──────────────────────────────────────────────────
// buildGraph() reads these names verbatim; a rename on one side is an empty
// map on that backend and no error anywhere.
const tablesAz = caseBody(azure, 'schema-tables');
check('schema-tables returns the database name, the table list and the views',
  /success:\s*true/.test(tablesAz) && /database:/.test(tablesAz)
  && /tables:/.test(tablesAz) && /views:/.test(tablesAz));
// rowCount is null for a view and a number for a table. Two distinct facts —
// "not counted" and "empty" — so the null must survive rather than collapsing
// to 0; a BIGINT arriving as a string must still parse to a number.
check('each table carries schema, name, kind and a rowCount that keeps null distinct from 0',
  /schema:\s*t\.TABLE_SCHEMA/.test(tablesAz) && /name:\s*t\.TABLE_NAME/.test(tablesAz)
  && /kind:\s*t\.kind/.test(tablesAz)
  && /rowCount:\s*t\.row_count == null \? null : \(parseInt\(t\.row_count\)\s*\|\|\s*0\)/.test(tablesAz),
  'BIGINT can arrive as a string; a view has no count at all');
check('both backends report a view row count as NULL, never 0',
  /THEN NULL ELSE COALESCE\(p\.rows,0\)/.test(tablesAz)
  && /THEN NULL ELSE COALESCE\(p\.rows,0\)/.test(caseBody(netlify, 'schema-tables')));

const fksAz = caseBody(azure, 'schema-fks');
for (const field of ['fromSchema', 'fromTable', 'fromColumn', 'toSchema', 'toTable', 'toColumn', 'name']) {
  check('an FK edge carries ' + field, new RegExp(field + ':').test(fksAz));
}
check('the FK edge shape is identical on both backends',
  ['fromSchema', 'fromTable', 'fromColumn', 'toSchema', 'toTable', 'toColumn', 'name']
    .every(f => new RegExp(f + ':').test(caseBody(netlify, 'schema-fks'))));

// ── 4. Read-only, and no caller input in the SQL ────────────────────────────
// These two actions take no parameters at all, which is the simplest possible
// proof that nothing caller-supplied reaches the query text.
for (const action of ['schema-tables', 'schema-fks']) {
  const body = caseBody(azure, action);
  check("'" + action + "' is read-only",
    !/\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|EXEC|MERGE)\b/i.test(body.replace(/\/\/.*$/gm, '')));
  check("'" + action + "' interpolates nothing into its SQL",
    !/\$\{/.test(body.match(/`[^`]*`/g)?.join('') || '') && !/\.input\(/.test(body),
    'it takes no parameters, so there is nothing to inject');
}

// ── 5. The client can tell a missing action from a broken one ───────────────
check('an unknown action still answers success:false so the client raises it',
  /Unknown action/i.test(azure) && /data\.success === false && data\.error/.test(graph));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
