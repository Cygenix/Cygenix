// tests/staging-handoff.test.js — the Dev Console's "Load into target".
//
// After a staging build, one button makes the Object Mapping maps that load
// each staging table into its target table. What it may and may not do is
// decided by public/cygenix-staging-handoff.js, pinned here:
//   - only tables of modules ticked Include, and only ones the session built;
//   - a new map where there is none, stamped with the template;
//   - the template's own UNTOUCHED draft is pointed at the new schema;
//   - a map somebody worked on, or made by hand for that target, is NEVER
//     changed — it is left alone, with the reason;
//   - the hundred-job cap is respected the way Conversion Templates respects it;
//   - the jobs to generate come out in load order.
'use strict';

const path = require('path');
const H = require(path.join(__dirname, '..', 'public', 'cygenix-staging-handoff.js'));
const TMap = require(path.join(__dirname, '..', 'public', 'cygenix-template-mapping.js'));

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra !== undefined ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const section = (t) => console.log('\n' + t);

const T = (id, target, order) => ({ id, targetTable: target, stagingTable: 'STG_' + target, loadOrder: order, columns: [] });
const tpl = { id: 'tpl_a', name: 'Finance', version: 3, modules: [
  { module: 'Clients', inScope: true, included: true, tables: [T('t1', 'Client', 10), T('t2', 'ClientAddress', 20), T('t3', 'ClientNote', 30), T('t4', 'ClientType', 5)] },
  { module: 'Billing', inScope: true, included: true, tables: [T('t5', 'Invoice', 10), T('t6', 'InvoiceLine', 20)] },
  { module: 'Unticked', inScope: true, included: false, tables: [T('t7', 'Ledger', 10)] },
  { module: 'Gone', inScope: false, included: true, tables: [T('t8', 'Old', 10)] },
] };
const draft = (id, tableId, source, target, extra) => Object.assign({ id, name: 'STG_' + target + ' → ' + target, projectId: 'p1',
  source, sourceTable: source, target: 'dbo.' + target, targetTable: 'dbo.' + target, columnMapping: [], insertSQL: '', status: 'draft',
  fromTemplate: { templateId: 'tpl_a', module: 'Clients', tableId, version: 2 } }, extra || {});

section('1. What each template table becomes');
{
  const jobs = [
    draft('j_client', 't1', 'dbo.STG_Client', 'Client'),                                                     // untouched draft, old schema
    draft('j_addr', 't2', 'dbo.STG_ClientAddress', 'ClientAddress', { columnMapping: [{ srcCol: 'A', tgtCol: 'A' }] }),  // worked on
    draft('j_note', 't3', 'stg.STG_ClientNote', 'ClientNote'),                                              // already in the schema
    { id: 'j_hand', name: 'My invoice map', projectId: 'p1', source: 'dbo.OldInvoices', sourceTable: 'dbo.OldInvoices', target: 'dbo.Invoice', targetTable: 'dbo.Invoice', columnMapping: [{ srcCol: 'x', tgtCol: 'y' }] },
    draft('j_other_project', 't6', 'dbo.STG_InvoiceLine', 'InvoiceLine', { projectId: 'p2' }),
    draft('j_deleted', 't4', 'dbo.STG_ClientType', 'ClientType', { _deleted: true }),
  ];
  const before = JSON.stringify(jobs);
  const pl = H.plan(tpl, jobs, { stagingSchema: 'stg', projectId: 'p1', targetSchemaOf: () => 'dbo',
    built: ['STG_Client', 'stg_clientaddress', 'STG_ClientNote', 'STG_ClientType', 'STG_Invoice', 'STG_InvoiceLine'] });
  const row = (t) => pl.rows.find(r => r.targetTable === t) || {};
  check('only modules ticked Include are considered — not unticked, not out of scope', !row('Ledger').action && !row('Old').action && pl.rows.length === 6);
  check('REPOINT: the template\'s untouched draft moves to the new schema', row('Client').action === 'repoint' && row('Client').jobId === 'j_client' && row('Client').source === 'stg.STG_Client');
  check('KEEP: a draft somebody mapped columns on is left alone, and says why', row('ClientAddress').action === 'keep' && /Mapped by hand from dbo\.STG_ClientAddress/.test(row('ClientAddress').why));
  check('REGENERATE: a draft already reading from the schema just gets its SQL again', row('ClientNote').action === 'regenerate' && row('ClientNote').jobId === 'j_note');
  check('KEEP: a map made by hand for the same target is not duplicated or changed', row('Invoice').action === 'keep' && /Your map "My invoice map"/.test(row('Invoice').why));
  check('CREATE: no live map in this project — a deleted one, or one in another project, does not count', row('ClientType').action === 'create' && row('InvoiceLine').action === 'create');
  check('built-table names are matched ignoring case', row('ClientAddress').action !== 'not-built');
  check('the counts add up', JSON.stringify(pl.counts) === JSON.stringify({ create: 2, repoint: 1, regenerate: 1, keep: 2, 'not-built': 0 }), JSON.stringify(pl.counts));
  check('PLAN TOUCHES NOTHING', JSON.stringify(jobs) === before);

  const notBuilt = H.plan(tpl, jobs, { stagingSchema: 'stg', projectId: 'p1', built: ['STG_Client'] });
  check('a table the session did not build is "not built", and gets no map', notBuilt.rows.find(r => r.targetTable === 'Invoice').action === 'not-built' && notBuilt.counts['not-built'] === 5);

  const res = H.apply(tpl, jobs, pl, { projectId: 'p1', by: 'me@acme.test', now: 1790000000000 });
  const byId = Object.fromEntries(res.jobs.map(j => [j.id, j]));
  check('APPLY: the new maps go first, stamped with the template, the schema and the project, as drafts with no SQL yet',
    res.created === 2 && res.jobs.slice(0, 2).every(j => j.fromTemplate.templateId === 'tpl_a' && j.fromTemplate.stagingSchema === 'stg'
      && j.projectId === 'p1' && j.status === 'draft' && j.columnMapping.length === 0 && /^stg\.STG_/.test(j.source)));
  check('…the repointed draft reads from the new schema and says so', byId.j_client.source === 'stg.STG_Client' && byId.j_client.sourceTable === 'stg.STG_Client'
    && byId.j_client.fromTemplate.stagingSchema === 'stg' && res.repointed === 1);
  check('…and NOTHING ELSE CHANGED: the worked-on draft, the hand-made map, the other project\'s, the deleted one',
    ['j_addr', 'j_hand', 'j_other_project', 'j_deleted', 'j_note'].every(id => JSON.stringify(byId[id]) === JSON.stringify(jobs.find(j => j.id === id))));
  check('…the original list is not mutated', JSON.stringify(jobs) === before);
  const order = res.toGenerate.map(id => byId[id].targetTable);
  check('THE JOBS TO GENERATE: created, repointed and regenerated — not the kept ones — in load order',
    res.toGenerate.length === 4 && order.indexOf('dbo.Invoice') === -1 && order.indexOf('dbo.ClientAddress') === -1
    && order[0] === 'dbo.ClientType' && order.indexOf('dbo.Client') < order.indexOf('dbo.ClientNote'), order.join());
  check('the summary line reads as a sentence', H.describe(pl) === '2 new maps, 1 template draft pointed at "stg", 1 map regenerated, 2 left as they are', H.describe(pl));
  check('a refused plan applies nothing', H.apply(tpl, jobs, { refused: true, rows: [] }).toGenerate.length === 0);
}

section('2. The hundred-job cap');
{
  const many = Array.from({ length: 99 }, (_, i) => ({ id: 'j' + i, name: 'm' + i, projectId: 'p1', source: 'a.b' + i, target: 'c.d' + i, columnMapping: [{}] }));
  const pl = H.plan(tpl, many, { stagingSchema: 'stg', projectId: 'p1' });
  check('adding maps that would push live maps past a hundred is refused, with the numbers', pl.refused === true && /There are 99 maps and this would add 6/.test(pl.why), pl.why);
  const room = H.plan(tpl, many.slice(0, 90), { stagingSchema: 'stg', projectId: 'p1' });
  check('…with room, it is not', !room.refused && room.counts.create === 6);
  const withDeleted = many.slice(0, 90).concat(Array.from({ length: 8 }, (_, i) => ({ id: 'del' + i, _deleted: true })));
  const pl2 = H.plan(tpl, withDeleted, { stagingSchema: 'stg', projectId: 'p1' });
  check('deleted maps at the end of the list may fall off; live ones may not', !pl2.refused, pl2.why);
  const liveAtEnd = Array.from({ length: 8 }, (_, i) => ({ id: 'del' + i, _deleted: true })).concat(many.slice(0, 90));
  check('…so a list whose END is live is refused', H.plan(tpl, liveAtEnd, { stagingSchema: 'stg', projectId: 'p1' }).refused === true);
}

section('3. The job a new map is');
{
  const pl = H.plan(tpl, [], { stagingSchema: 'stg', projectId: '', targetSchemaOf: (n) => n === 'Invoice' ? 'fin' : 'dbo' });
  const res = H.apply(tpl, [], pl, { projectId: '' });
  const inv = res.jobs.find(j => /Invoice$/.test(j.targetTable));
  check('the target is qualified with the target\'s own schema', inv.targetTable === 'fin.Invoice' && inv.target === 'fin.Invoice');
  check('the job is the same shape Conversion Templates\' send makes', Object.keys(TMap.buildJob(tpl, pl.rows[0].pair, {})).every(k => k in inv));
  check('no live map means every ticked table gets one', res.created === 6 && res.toGenerate.length === 6);
}

section('4. Object Mapping keeps the stamp when it saves');
{
  // saveAsJob rebuilds the record from the form. The stamp is not on the
  // form, so it used to be dropped on every save — and with it the
  // template's knowledge of its own maps. Run the real helper.
  const fs = require('fs');
  const src = fs.readFileSync(path.join(__dirname, '..', 'public', 'object-mapping-app.js'), 'utf8');
  const m = src.match(/function omCarryTemplateStamp\(job, jobs\)\{[\s\S]*?\n\}/);
  check('the helper exists', !!m);
  const make = (editJobId) => new Function('editJobId', m[0] + '; return omCarryTemplateStamp;')(editJobId);
  const stored = [{ id: 'j1', fromTemplate: { templateId: 'tpl_a', module: 'Clients', tableId: 't1', version: 2 } }, { id: 'j2' }];
  const job = { id: 'j1', columnMapping: [{ srcCol: 'a', tgtCol: 'a' }] };
  make('j1')(job, stored);
  check('AN EDIT CARRIES THE STAMP FORWARD, as a copy', job.fromTemplate && job.fromTemplate.tableId === 't1' && job.fromTemplate !== stored[0].fromTemplate);
  const fresh = { id: 'job_new' };
  make(null)(fresh, stored);
  check('a new map is not given anybody\'s stamp', !fresh.fromTemplate);
  const hand = { id: 'j2' };
  make('j2')(hand, stored);
  check('a hand-made map stays unstamped', !hand.fromTemplate);
  check('saveAsJob calls it in both branches (single map and one-to-many), before the profile is attached',
    (src.match(/omCarryTemplateStamp\(job, jobs\);\n\s*try \{ if \(window\.CygenixJobProfile\) CygenixJobProfile\.attach\(job, jobs\); \}/g) || []).length === 2);
}

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
