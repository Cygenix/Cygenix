// tests/lineage.test.js — live lineage + impact analysis.
//
// Two promises: click a target column and see the full path back to
// source with the transform at each hop and every rule attached; and
// "change this mapping and these N objects break" — enumerated by name,
// from metadata the product already holds.

'use strict';

const L = require('../public/cygenix-lineage.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Lineage + impact — engine\n');

const JOB = {
  id: 'j1', name: 'Customer load', sourceTable: 'src.customers', targetTable: 'dbo.clients',
  srcWhere: "status = 'ACTIVE'",
  columnMapping: [
    { srcCol: 'cust_ref', tgtCol: 'client_id', transform: 'NONE' },
    { srcCol: 'opn_bal', tgtCol: 'opening_balance', transform: 'CAST' },
    { srcCol: 'status_cd', tgtCol: 'status', transform: 'UPPER' },
    { srcCol: '', tgtCol: 'source_system', literalValue: "N'LEGACY'" },
    { srcCol: '', tgtCol: 'load_date', literalValue: '@@date' },
    { srcCol: '', tgtCol: 'notes' },
  ],
};

const CTX = {
  jobs: [
    JOB,
    { id: 'j2', name: 'Balance rollup', sourceTable: 'dbo.clients', targetTable: 'dbo.summary',
      columnMapping: [{ srcCol: 'opening_balance', tgtCol: 'total_open' }] },
    { id: 'task1', name: 'Nightly cutover', jobType: 'composite',
      childSteps: [{ jobId: 'j1', srcTable: 'src.customers', tgtTable: 'dbo.clients' }] },
  ],
  wasisRules: [
    { id: 'w1', srcTable: 'src.customers', srcField: 'status_cd', oldVal: 'A', newVal: 'ACTIVE' },
    { id: 'w2', srcTable: 'src.customers', srcField: 'status_cd', oldVal: 'C', newVal: 'CLOSED' },
    { id: 'w3', srcTable: 'src.orders', srcField: 'status_cd', oldVal: 'X', newVal: 'VOID' },
  ],
  reconChecks: [
    { id: 'r1', phrase: 'no customer lost their opening balance',
      spec: { jobId: 'j1', tgtTable: 'dbo.clients', kind: 'perkey',
              col: { srcCol: 'opn_bal', tgtCol: 'opening_balance' },
              key: { srcCol: 'cust_ref', tgtCol: 'client_id' } } },
  ],
  validationSources: [{ schema: 'dbo', table: 'clients', label: 'Client checks' }],
  preflight: { jobs: [{ tgtTable: 'dbo.clients',
    findings: [{ code: 'scale-round', column: 'opn_bal → opening_balance', projected: 1200 }] }] },
  fks: [{ fromTable: 'dbo.invoices', fromColumn: 'client_id', toTable: 'dbo.clients', toColumn: 'client_id' }],
};

// ── The lineage chain ─────────────────────────────────────────────────────
const chain = L.lnColumnLineage(JOB, 'status', CTX);
const kinds = chain.map(h => h.kind);
check('a transformed, rule-mapped column walks target ← transform ← Was/Is ← source ← filter',
  JSON.stringify(kinds) === JSON.stringify(['target', 'transform', 'wasis', 'source', 'filter']),
  JSON.stringify(kinds));
check('the Was/Is hop carries exactly the rules bound to this table+field',
  chain.find(h => h.kind === 'wasis').rules.length === 2);
check('the source hop names the source column and table',
  (() => { const s = chain.find(h => h.kind === 'source');
           return s.column === 'status_cd' && s.table === 'src.customers'; })());
check('the filter hop carries the WHERE clause',
  /ACTIVE/.test(chain.find(h => h.kind === 'filter').where));

check('a plain passthrough column has no transform or rule hops',
  JSON.stringify(L.lnColumnLineage(JOB, 'client_id', CTX).map(h => h.kind))
  === JSON.stringify(['target', 'source', 'filter']));
check('a fixed literal ends the chain — no source column',
  (() => { const c = L.lnColumnLineage(JOB, 'source_system', CTX);
           return c.some(h => h.kind === 'literal') && !c.some(h => h.kind === 'source'); })());
check('a parameter token says it resolves at run time',
  /run time/.test(L.lnColumnLineage(JOB, 'load_date', CTX).find(h => h.kind === 'literal').note));
check('an unmapped column says so instead of inventing a path',
  L.lnColumnLineage(JOB, 'notes', CTX).some(h => h.kind === 'unmapped'));

// ── Impact analysis ───────────────────────────────────────────────────────
const impact = L.lnImpact(JOB, 'opening_balance', CTX);
const types = impact.map(i => i.type).sort();
check('opening_balance impact finds the task, the recon check, the chained job, validation, preflight',
  types.includes('task') && types.includes('recon') && types.includes('job')
  && types.includes('validation') && types.includes('preflight'), JSON.stringify(impact, null, 1));
check('the task impact warns about the stale snapshot and pinned schedules',
  /re-create the task|OLD mapping/i.test(impact.find(i => i.type === 'task').detail));
check('the chained job is named, with the flow explained',
  (() => { const j = impact.find(i => i.type === 'job');
           return j.name === 'Balance rollup' && /reads dbo\.clients\.opening_balance/.test(j.detail); })());
check('the recon check is quoted by its phrase',
  /opening balance/.test(impact.find(i => i.type === 'recon').name));
check('the headline counts it out',
  /5 downstream objects are affected/.test(L.lnHeadline(impact)), L.lnHeadline(impact));

const statusImpact = L.lnImpact(JOB, 'status', CTX);
check('status impact finds its two Was/Is rules but not the other table\'s',
  statusImpact.filter(i => i.type === 'wasis').length === 2);

const fkImpact = L.lnImpact(JOB, 'client_id', CTX);
check('FK children appear when graph data is supplied',
  fkImpact.some(i => i.type === 'fk' && i.name === 'dbo.invoices.client_id'));
check('the key column also matters to the recon check (its grouping key)',
  fkImpact.some(i => i.type === 'recon'));

check('a column nothing depends on says it is safe to change',
  (() => {
    const quiet = L.lnImpact({ id: 'jq', sourceTable: 's.t', targetTable: 'dbo.lonely',
      columnMapping: [{ srcCol: 'a', tgtCol: 'b' }] }, 'b', { jobs: [], wasisRules: [], reconChecks: [], validationSources: [] });
    return quiet.length === 0 && /safe to change/i.test(L.lnHeadline(quiet));
  })());

// ── Table matching ────────────────────────────────────────────────────────
check('schema-qualified and bare names match; different tables never do',
  L.sameTable('dbo.clients', 'clients') && L.sameTable('[dbo].[clients]', 'dbo.clients')
  && !L.sameTable('dbo.clients', 'dbo.invoices'));

// ── Type metadata for the drawer ──────────────────────────────────────────
check('every impact type renders with an icon and label',
  [...new Set([...types, 'wasis', 'fk'])].every(t => L.TYPE_META[t] && L.TYPE_META[t].icon));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
