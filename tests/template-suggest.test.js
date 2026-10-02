// tests/template-suggest.test.js — "Suggest all" / "Suggest" for Conversion Templates.
//
// WHY THIS FILE EXISTS
//
// Suggest asks Claude which module each target table belongs to. Since
// Oct-2026 it does that for EVERY target table, with its columns, keys and
// FK neighbours, judged against ALL the ticked modules at once, through the
// Message Batches API (azure-function/src/table-classifier.js, prompt in
// table-classifier-prompt.js). Pinned here without a network:
//
//   1. It can only ever name REAL tables and REAL modules. The backend drops
//      anything else, and reports tables Claude left out so they can be
//      retried rather than silently counted as "no module".
//   2. It can only ever ADD. Tables already on a module are "already added",
//      never re-added or removed; a load order a person typed is never
//      overwritten.
//   3. The run: every table's columns read, chunks of at most 40, every chunk
//      carrying every module, polling that stops at once on a refused key,
//      a failed chunk kept apart with its tables, retry of those tables only,
//      and Cancel cancelling the batches and keeping nothing.
//
// Plus the load order itself, the Shared flag and its note, the preview's
// summary and defaults (high and medium ticked, low not), and — because
// Cygenix is target-agnostic — that no table names or module→table maps
// appear in the code or the prompt.

'use strict';

const fs = require('fs');
const path = require('path');
const Module = require('module');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

// The backend, with the Functions runtime and auth stubbed.
const registered = {};
const realRequire = Module.prototype.require;
Module.prototype.require = function (id) {
  if (id === '@azure/functions') return { app: { http: (name, def) => { registered[name] = def; } } };
  if (id === './entra-auth') return { enforceAuth: async () => ({ ok: true }) };
  return realRequire.call(this, id);
};
const B = require(path.join(ROOT, 'azure-function', 'src', 'table-classifier.js'));
const PR = require(path.join(ROOT, 'azure-function', 'src', 'table-classifier-prompt.js'));
Module.prototype.require = realRequire;
const S = require(path.join(ROOT, 'public', 'cygenix-template-suggest.js'));
const TM = require(path.join(ROOT, 'public', 'cygenix-template-model.js'));

// A fake Anthropic client: batches held in memory.
function fakeClient(answer) {
  const batches = {};
  let n = 0;
  return {
    batches,
    messages: { batches: {
      create: async ({ requests }) => { const id = 'msgbatch_' + (++n); batches[id] = { id, requests, status: 'in_progress' }; return { id, processing_status: 'in_progress' }; },
      retrieve: async (id) => { const b = batches[id]; if (!b) { const e = new Error('nf'); e.status = 404; throw e; }
        return { id, processing_status: b.status, request_counts: { processing: b.status === 'ended' ? 0 : b.requests.length, succeeded: b.status === 'ended' ? b.requests.length : 0, errored: 0, canceled: 0, expired: 0 } }; },
      results: async (id) => (async function* () { for (const r of batches[id].requests) yield Object.assign({ custom_id: r.custom_id }, answer(r)); })(),
      cancel: async (id) => { batches[id].status = 'canceling'; return { id }; },
    } },
  };
}
const ok = (obj) => ({ result: { type: 'succeeded', message: { stop_reason: 'end_turn', content: [{ type: 'thinking', thinking: '' }, { type: 'text', text: JSON.stringify(obj) }] } } });

(async () => {
  console.log('Conversion Templates — Suggest all\n');

  /* ── 1. Backend ─────────────────────────────────────────────────────────── */
  section('1. The backend: one shared classifier, four batch routes, only real names');
  const routes = ['start', 'status', 'results', 'cancel'];
  check('four routes are registered under the /agent/ family the data proxy already forwards',
    routes.every(r => registered['table-classify-' + r] && registered['table-classify-' + r].route === 'agent/table-classify/' + r));
  check('all are function-key routes, as the other agent routes are', routes.every(r => registered['table-classify-' + r].authLevel === 'function'));
  const proxy = read('netlify', 'functions', 'data-proxy.js');
  const rule = /return bare === '\/me' \|\| bare === '\/narrative' \|\| (\/.*\/)\.test\(bare\);/.exec(proxy);
  const re = rule && new RegExp(rule[1].slice(1, -1));
  check('the proxy\'s path rule admits all four paths with no change to the proxy', !!re && routes.every(r => re.test('/agent/table-classify/' + r)));
  const idx = read('azure-function', 'src', 'index.js');
  check('index.js imports the classifier, and no longer the old two-step module',
    /require\('\.\/table-classifier'\);/.test(idx) && !/template-suggest-tables/.test(idx));
  check('the old shortlist/rank module is gone', !fs.existsSync(path.join(ROOT, 'azure-function', 'src', 'template-suggest-tables.js')));
  check('no function.json folder was created for it', !fs.existsSync(path.join(ROOT, 'azure-function', 'table-classify-start')));
  const host = JSON.parse(read('azure-function', 'host.json'));
  check('host.json carries no functionTimeout', !('functionTimeout' in host));

  // The prompt lives in its own file, and says what the brief asks.
  check('THE PROMPT IS IN ITS OWN FILE: CLASSIFY_SYSTEM, MODEL, EFFORT, TABLES_PER_CHUNK',
    typeof PR.CLASSIFY_SYSTEM === 'string' && PR.CLASSIFY_SYSTEM.length > 500 && PR.TABLES_PER_CHUNK === 40);
  check('the model is the current Opus unless the deployment overrides it, at high effort',
    PR.MODEL === (process.env.ANTHROPIC_MODEL_CLASSIFY || 'claude-opus-5-5') && PR.EFFORT === 'high');
  check('the prompt asks for module(s), confidence, required and a one-line reason, with "no module" allowed',
    /"modules"/.test(PR.CLASSIFY_SYSTEM) && /"confidence"/.test(PR.CLASSIFY_SYSTEM) && /"required"/.test(PR.CLASSIFY_SYSTEM)
    && /"reason"/.test(PR.CLASSIFY_SYSTEM) && /empty list when it belongs to none/.test(PR.CLASSIFY_SYSTEM));
  check('…judges each table against ALL the modules, and uses FK neighbours',
    /against all the modules together/.test(PR.CLASSIFY_SYSTEM) && /child or detail table usually belongs with its parent's module/.test(PR.CLASSIFY_SYSTEM));
  check('the page\'s chunk size matches the prompt file\'s', S.LIMITS.CHUNK_TABLES === PR.TABLES_PER_CHUNK);

  // The request Claude sees.
  const mods = B.cleanModules([{ name: 'Addresses', notes: 'postal  and\nsite addresses' }, { name: 'Contacts' }, { name: 'addresses' }, '']);
  check('modules are cleaned and deduped case-insensitively', mods.length === 2 && mods[0].notes === 'postal and site addresses');
  const wide = { name: 'Wide', columns: Array.from({ length: 160 }, (_, i) => ({ name: 'C' + i, type: 'int' })) };
  const cc = B.cleanChunks([{ id: 'c1', tables: [
    { name: 'SiteAddress', rows: 10, columns: [{ name: 'AddressLine1', dataType: 'nvarchar(100)' }], pk: ['Id'], refs: ['Site'], refBy: ['Note'] },
    { name: 'Orphan', columnsRead: false }, wide] }]);
  const prompt = B.chunkPrompt(mods, cc.chunks[0].tables);
  check('every chunk\'s prompt carries every module and its notes', /"Addresses" — notes: postal and site addresses/.test(prompt) && /"Contacts"/.test(prompt));
  check('…and each table\'s columns, key, rows and FK neighbours both ways',
    /### SiteAddress  \(10 rows\)/.test(prompt) && /AddressLine1:nvarchar\(100\)/.test(prompt) && /primary key: Id/.test(prompt)
    && /references: Site/.test(prompt) && /referenced by: Note/.test(prompt));
  check('…says when the columns could not be read', /Orphan[\s\S]*could not be read/.test(prompt));
  check('…and cuts a very wide table\'s columns, saying how many more', /\(\+10 more\)/.test(prompt));
  check('…and asks for one entry per table', /Return one entry for each of the 3 tables above\./.test(prompt));
  const req = B.batchRequest(mods, cc.chunks[0]);
  check('THE BATCH REQUEST: custom_id is the chunk, the prompt file\'s model, system and max_tokens',
    req.custom_id === 'c1' && req.params.model === PR.MODEL && req.params.system === PR.CLASSIFY_SYSTEM && req.params.max_tokens === PR.MAX_TOKENS);
  check('…with effort and a JSON-schema structured output',
    req.params.output_config.effort === 'high' && req.params.output_config.format.type === 'json_schema' && req.params.output_config.format.schema === B.OUTPUT_SCHEMA);
  check('…and no forced tool use or thinking budget (both rejected on this model)', !('tool_choice' in req.params) && !('thinking' in req.params));
  const sch = B.OUTPUT_SCHEMA.properties.tables.items;
  check('the schema requires table, modules, confidence, required and reason, and nothing else',
    sch.required.join() === 'table,modules,confidence,required,reason' && sch.additionalProperties === false && B.OUTPUT_SCHEMA.additionalProperties === false);
  check('a chunk id that is not a valid custom_id is refused', !!B.cleanChunks([{ id: 'bad id!', tables: [{ name: 'T' }] }]).error);
  check('more than 80 tables in one chunk is refused', !!B.cleanChunks([{ id: 'c', tables: Array.from({ length: 81 }, (_, i) => ({ name: 'T' + i })) }]).error);

  // Checking the answer.
  const v = B.validateChunk({ tables: [
    { table: 'siteaddress', modules: ['addresses', 'Invented', 'Contacts'], confidence: 'HIGH', required: true, reason: 'x'.repeat(200) },
    { table: 'Ghost', modules: ['Addresses'], confidence: 'high', required: true, reason: '' },
    { table: 'Note', modules: [], confidence: 'weird', reason: 'audit' },
  ] }, ['SiteAddress', 'Note', 'Region'], ['Addresses', 'Contacts']);
  check('ONLY REAL TABLES: an invented table is dropped, a real one spelled as the database spells it',
    v.rows.length === 2 && v.rows[0].table === 'SiteAddress');
  check('ONLY REAL MODULES: an invented module is dropped, real ones spelled as given', v.rows[0].modules.join() === 'Addresses,Contacts');
  check('confidence normalised, reason cut to 90, required kept', v.rows[0].confidence === 'high' && v.rows[0].reason.length === 90 && v.rows[0].required === true);
  check('an empty module list means "no module", and odd confidence becomes low', v.rows[1].modules.length === 0 && v.rows[1].confidence === 'low');
  check('…a missing "required" counts as required', v.rows[1].required === true);
  check('TABLES CLAUDE LEFT OUT ARE REPORTED, not counted as "no module"', v.unanswered.join() === 'Region');
  check('dropped names are counted', v.dropped === 2);

  const chunk = { id: 'c1', tables: ['SiteAddress'] };
  const rd = (r) => B.readResult({ custom_id: 'c1', result: r }, chunk, ['Addresses']);
  check('a refusal is a failed chunk, said plainly', /declined/.test(rd({ type: 'succeeded', message: { stop_reason: 'refusal', content: [] } }).error));
  check('an answer cut off is a failed chunk', /cut off/.test(rd({ type: 'succeeded', message: { stop_reason: 'max_tokens', content: [] } }).error));
  check('an errored request names its error type', /\(overloaded_error\)/.test(rd({ type: 'errored', error: { type: 'error', error: { type: 'overloaded_error' } } }).error));
  check('expired and cancelled are said as such', /Expired/.test(rd({ type: 'expired' }).error) && rd({ type: 'canceled' }).error === 'Cancelled');
  check('fenced JSON is tolerated', rd({ type: 'succeeded', message: { stop_reason: 'end_turn', content: [{ type: 'text', text: '```json\n{"tables":[{"table":"SiteAddress","modules":["Addresses"],"confidence":"high","required":true,"reason":"r"}]}\n```' }] } }).ok);

  // Handlers with a fake client.
  const fc = fakeClient((r) => ok({ tables: [{ table: 'SiteAddress', modules: ['Addresses'], confidence: 'high', required: false, reason: 'Line1' }] }));
  const st = await B.startHandler({ modules: [{ name: 'Addresses' }], chunks: [{ id: 'c1', tables: [{ name: 'SiteAddress' }] }, { id: 'c2', tables: [{ name: 'Region' }] }] }, fc);
  const stb = JSON.parse(st.body);
  check('START submits ONE batch with one request per chunk, and returns at once with its id',
    st.status === 200 && stb.batchId === 'msgbatch_1' && fc.batches.msgbatch_1.requests.length === 2 && stb.chunkIds.join() === 'c1,c2', st.body);
  const early = await B.resultsHandler({ batchId: 'msgbatch_1', modules: ['Addresses'], chunks: [{ id: 'c1', tables: ['SiteAddress'] }] }, fc);
  check('RESULTS before the batch has ended is a 409, not a wait', early.status === 409);
  const stat = JSON.parse((await B.statusHandler({ batchIds: ['msgbatch_1'] }, fc)).body);
  check('STATUS reports the status and the counts', stat.batches[0].status === 'in_progress' && stat.batches[0].counts.processing === 2);
  fc.batches.msgbatch_1.status = 'ended';
  const rs = JSON.parse((await B.resultsHandler({ batchId: 'msgbatch_1', modules: ['Addresses'],
    chunks: [{ id: 'c1', tables: ['SiteAddress'] }, { id: 'c2', tables: ['Region'] }, { id: 'c9', tables: ['Nowhere'] }] }, fc)).body);
  check('RESULTS checks each chunk\'s answer against that chunk\'s own tables',
    rs.chunks[0].ok && rs.chunks[0].rows[0].table === 'SiteAddress' && rs.chunks[0].rows[0].required === false
    && rs.chunks[1].ok && rs.chunks[1].rows.length === 0 && rs.chunks[1].unanswered.join() === 'Region', JSON.stringify(rs.chunks.slice(0, 2)));
  check('…and a chunk with no result says so', rs.chunks[2].ok === false && /No result/.test(rs.chunks[2].error));
  const cn = JSON.parse((await B.cancelHandler({ batchIds: ['msgbatch_1', 'msgbatch_x'] }, fc)).body);
  check('CANCEL cancels what it can and does not fail on the rest', cn.cancelled.join() === 'msgbatch_1');
  check('too many modules is refused', (await B.startHandler({ modules: Array.from({ length: 61 }, (_, i) => ({ name: 'M' + i })), chunks: [{ id: 'c', tables: [{ name: 'T' }] }] }, fc)).status === 413);

  // The wrapper: no key → refused before anything else; SDK errors mapped.
  const call = async (name, body, headers) => registered[name].handler({ method: 'POST',
    headers: { get: (h) => (headers || {})[h.toLowerCase()] || null }, json: async () => body }, { log: () => {} });
  const noKey = await call('table-classify-start', { modules: [{ name: 'a' }], chunks: [] }, {});
  check('without the caller\'s own Anthropic key the route refuses', noKey.status === 400 && /x-anthropic-key/.test(noKey.body));
  const realClient = B.deps.client;
  B.deps.client = () => ({ messages: { batches: { retrieve: async () => { const e = new Error('Unauthorized: echo of request'); e.status = 401; throw e; } } } });
  const unauth = await call('table-classify-status', { batchIds: ['msgbatch_1'] }, { 'x-anthropic-key': 'sk-ant-test' });
  check('an Anthropic 401 becomes a readable 400 that echoes nothing Anthropic sent', unauth.status === 400 && /check the API key/.test(unauth.body) && !/echo/.test(unauth.body));
  B.deps.client = () => ({ messages: { batches: { retrieve: async () => { throw new Error('kaboom'); } } } });
  const crash = await call('table-classify-status', { batchIds: ['msgbatch_1'] }, { 'x-anthropic-key': 'sk-ant-test' });
  check('an unexpected failure returns err.message and err.stack in the 500 body', crash.status === 500 && JSON.parse(crash.body).error === 'kaboom' && !!JSON.parse(crash.body).stack);
  B.deps.client = realClient;
  const src = read('azure-function', 'src', 'table-classifier.js');
  check('the key comes from userAnthropicKey — no key of Cygenix\'s own', /userAnthropicKey\(req\)/.test(src) && !/process\.env\.ANTHROPIC_API_KEY/.test(src));
  check('logs carry counts, never names, prompts or answers', (src.match(/ctx\.log\(/g) || []).length === 1 && /ctx\.log\('\[table-classify\] ' \+ label \+ ' status=' \+ res\.status \+ ' ms=' \+ \(Date\.now\(\) - started\)\);/.test(src));
  check('NOTHING IS STORED: no Cosmos, no blob, no file', !/@azure\/cosmos|cosmos|BlobServiceClient|writeFile/i.test(src.replace(/writes nothing to Cosmos/, '')));

  /* ── 2. Load order ──────────────────────────────────────────────────────── */
  section('2. Load order: parents first, gaps of ten, cycles flagged');
  const lo = S.computeLoadOrder(['Line', 'Header', 'Customer', 'Island', 'A', 'B'],
    [{ child: 'Line', parent: 'Header' }, { child: 'Header', parent: 'Customer' }, { child: 'A', parent: 'B' }, { child: 'B', parent: 'A' }, { child: 'Line', parent: 'Line' }]);
  check('a parent always loads before its child', lo.order.customer < lo.order.header && lo.order.header < lo.order.line, JSON.stringify(lo.order));
  check('numbers step by ten', Object.values(lo.order).every(n => n % 10 === 0));
  check('a table with no FKs still gets a number', lo.order.island > 0);
  check('A CYCLE SHARES ONE NUMBER and each member names the other', lo.order.a === lo.order.b && lo.cycles.a.join() === 'B' && lo.cycles.b.join() === 'A');
  check('a self-reference is ignored', !lo.cycles.line);

  /* ── 3. Preview and apply ───────────────────────────────────────────────── */
  section('3. Preview and apply: add-only, shared, unassigned, failed, and never over a person\'s number');
  const tpl = TM.tmNewTemplate({ projectId: 'p', name: 'T', stagingPrefix: 'stg_' });
  TM.tmSyncScope(tpl, ['Addresses', 'Contacts', 'Billing', 'Untouched']);
  const ad = TM.tmFindModule(tpl, 'Addresses');
  const hand1 = TM.tmAddTable(tpl, 'Addresses', { targetTable: 'Site' }, 'me');
  const hand2 = TM.tmAddTable(tpl, 'Addresses', { targetTable: 'SiteAddress' }, 'me');
  TM.tmUpdateTable(tpl, 'Addresses', hand2.id, { loadOrder: 5 }, 'me');
  check('a typed load order is marked "user"', hand2.loadOrderSource === 'user' && hand2.loadOrder === 5 && hand1.loadOrderSource === 'auto');
  TM.tmAddTable(tpl, 'Contacts', { targetTable: 'Person' }, 'me');
  const untouchedBefore = JSON.stringify(TM.tmFindModule(tpl, 'Untouched'));
  const runMods = [{ key: 'Addresses', name: 'Addresses' }, { key: 'Contacts', name: 'Contacts' }, { key: 'Billing', name: 'Billing' }];
  const R = (table, modules, confidence, required) => ({ table, modules, confidence, required: required !== false, reason: 'because ' + table });
  const outcomes = [
    { id: 'c1', ok: true, tables: ['SiteAddress', 'AddressType', 'Person', 'Region', 'AuditLog', 'PersonPhone'], unanswered: ['PersonPhone'],
      rows: [R('SiteAddress', ['Addresses'], 'high'), R('AddressType', ['Addresses'], 'medium', false), R('Person', ['Addresses'], 'low'),
             R('Region', ['Addresses', 'Contacts'], 'high'), R('AuditLog', [], 'high'), R('Invoice2', ['Untouched'], 'high')] },
    { id: 'c2', ok: false, error: 'Claude declined this batch of tables', tables: ['Invoice', 'InvoiceLine'] },
  ];
  const prev = S.buildPreview(tpl, runMods, outcomes);
  const G = (m) => prev.groups.find(g => g.module === m);
  const row = (m, t) => G(m).rows.find(x => x.table === t);
  check('grouped by every module in the run, in order — and only those', prev.groups.map(g => g.module).join() === 'Addresses,Contacts,Billing');
  check('ALREADY ADDED: a proposed table the module has is greyed and not ticked', row('Addresses', 'SiteAddress').already && !row('Addresses', 'SiteAddress').checked);
  check('DEFAULTS: high AND medium ticked, low not', row('Addresses', 'Region').checked && row('Addresses', 'AddressType').checked && !row('Addresses', 'Person').checked);
  check('required comes through', row('Addresses', 'AddressType').required === false && row('Addresses', 'Region').required === true);
  check('SHARED: proposed for two modules', row('Addresses', 'Region').shared.join() === 'Contacts' && row('Contacts', 'Region').shared.join() === 'Addresses');
  check('SHARED: already in another module', row('Addresses', 'Person').shared.join() === 'Contacts');
  check('UNASSIGNED: a table that fits no module is listed, with its reason, and offered nowhere',
    prev.unassigned.some(u => u.table === 'AuditLog' && u.reason === 'because AuditLog') && !prev.groups.some(g => g.rows.some(r => r.table === 'AuditLog')));
  check('a module outside the run is never offered a table — a table placed only there counts as unassigned',
    !prev.groups.some(g => g.module === 'Untouched') && prev.unassigned.some(u => u.table === 'Invoice2'));
  check('FAILED: the failed chunk and the left-out table are listed with their tables',
    prev.failed.length === 2 && prev.failed.some(f => f.tables.join() === 'Invoice,InvoiceLine' && /declined/.test(f.error)) && prev.failed.some(f => f.tables.join() === 'PersonPhone'));
  check('SUMMARY: X tables across Y modules, Z shared, N unassigned, and how many failed',
    prev.summary.tables === 3 && prev.summary.modules === 2 && prev.summary.shared === 2 && prev.summary.unassigned === 2 && prev.summary.failedTables === 3, JSON.stringify(prev.summary));
  check('rows are grouped high → medium → low', G('Addresses').rows.map(r => r.confidence).join() === 'high,high,medium,low');

  // A retry carries the earlier ticks over and clears what it classified.
  row('Addresses', 'AddressType').checked = false;
  const keep = S.ticksOf(prev);
  const retried = S.buildPreview(tpl, runMods, outcomes.concat([{ id: 'r1c1', ok: true, tables: ['Invoice', 'InvoiceLine', 'PersonPhone'],
    rows: [R('Invoice', ['Billing'], 'high'), R('InvoiceLine', ['Billing'], 'high'), R('PersonPhone', ['Contacts'], 'medium')] }]), keep);
  check('RETRY: the retried tables arrive and are no longer listed as failed', retried.failed.length === 0 && retried.groups[2].rows.length === 2);
  check('…and a tick the person removed stays removed', retried.groups[0].rows.find(r => r.table === 'AddressType').checked === false);

  const before = JSON.stringify(ad.tables.map(t => t.targetTable));
  const res = S.apply(tpl, retried, TM, 'me', [
    { child: 'SiteAddress', parent: 'Site' }, { child: 'SiteAddress', parent: 'AddressType' }, { child: 'Site', parent: 'Region' },
    { child: 'Person', parent: 'Region' }, { child: 'InvoiceLine', parent: 'Invoice' }]);
  const names = ad.tables.map(t => t.targetTable);
  check('APPLY ADDS ONLY: nothing that was there is removed, and only ticked rows are added',
    JSON.parse(before).every(n => names.includes(n)) && names.join() === 'Site,SiteAddress,Region', names.join());
  const region = ad.tables.find(t => t.targetTable === 'Region');
  check('added rows go through the model\'s add path: staging name uses the prefix', region.stagingTable === TM.tmStagingTableName('Region', 'stg_'), region.stagingTable);
  check('added rows carry source "ai", confidence, reason and Claude\'s required', region.source === 'ai' && region.aiConfidence === 'high' && /because Region/.test(region.aiReason) && region.required === true);
  check('A SHARED TABLE SAYS WHERE ELSE IT IS, in its notes', region.notes === 'Also in: Contacts' && TM.tmFindModule(tpl, 'Contacts').tables.find(t => t.targetTable === 'Region').notes === 'Also in: Addresses');
  check('…and a row that was already there is not touched', !TM.tmFindModule(tpl, 'Contacts').tables.find(t => t.targetTable === 'Person').notes);
  check('UNTICKED MODULES ARE NOT TOUCHED', JSON.stringify(TM.tmFindModule(tpl, 'Untouched')) === untouchedBefore);
  check('the summary counts added, modules, shared and ordered', res.added === 5 && res.modules === 3 && res.shared === 1 && res.ordered >= 5, JSON.stringify(res));
  const site = ad.tables.find(t => t.targetTable === 'Site');
  check('LOAD ORDER recalculated: parents before children', region.loadOrder < site.loadOrder && region.loadOrderSource === 'ai'
    && TM.tmFindModule(tpl, 'Billing').tables.find(t => t.targetTable === 'Invoice').loadOrder < TM.tmFindModule(tpl, 'Billing').tables.find(t => t.targetTable === 'InvoiceLine').loadOrder);
  check('A HAND-TYPED LOAD ORDER IS NOT CHANGED BY APPLY', ad.tables.find(t => t.targetTable === 'SiteAddress').loadOrder === 5);
  site.loadOrder = 77; site.loadOrderSource = 'ai';
  const legacy = TM.tmAddTable(tpl, 'Billing', { targetTable: 'Payment' }, 'me'); delete legacy.loadOrderSource; legacy.loadOrder = 3;
  S.applyLoadOrder(tpl, [{ child: 'Site', parent: 'Region' }], 'recalc');
  check('RECALCULATE replaces AI numbers…', site.loadOrder !== 77 && site.loadOrderSource === 'ai');
  check('…but never a typed one, nor a legacy row\'s', ad.tables.find(t => t.targetTable === 'SiteAddress').loadOrder === 5 && legacy.loadOrder === 3);
  TM.tmUpdateTable(tpl, 'Addresses', region.id, { notes: 'checked' }, 'me');
  check('EDITING AN AI ROW CLEARS THE BADGE', region.source === 'user' && !('aiConfidence' in region) && !('aiReason' in region));
  const cyc = TM.tmNewTemplate({ projectId: 'p', name: 'C' }); TM.tmSyncScope(cyc, ['M']);
  TM.tmAddTable(cyc, 'M', { targetTable: 'P' }, 'me'); TM.tmAddTable(cyc, 'M', { targetTable: 'Q' }, 'me');
  S.applyLoadOrder(cyc, [{ child: 'P', parent: 'Q' }, { child: 'Q', parent: 'P' }], 'recalc');
  S.applyLoadOrder(cyc, [{ child: 'P', parent: 'Q' }, { child: 'Q', parent: 'P' }], 'recalc');
  const [p, q] = TM.tmFindModule(cyc, 'M').tables;
  check('a cycle\'s rows share a number and say so in their notes, once', p.loadOrder === q.loadOrder && (p.notes.match(/FK cycle with Q/g) || []).length === 1);

  /* ── 4. Orchestration ───────────────────────────────────────────────────── */
  section('4. The run: every table\'s columns, chunks of 40, batches, polling, failures, retry, cancel');
  const N = 130;
  const graph = { ok: true, tables: Array.from({ length: N }, (_, i) => ({ schema: 'dbo', name: 'T' + String(N - i).padStart(3, '0'), key: 'dbo.T' + String(N - i).padStart(3, '0'), rowCount: i })),
    edges: [{ from: 'dbo.T002', to: 'dbo.T001', fromColumn: 'pid', toColumn: 'id' }] };
  const mods3 = [{ key: 'A', name: 'A', notes: 'first' }, { key: 'B', name: 'B', notes: '' }, { key: 'C', name: 'C', notes: '' }];
  const tpl2 = TM.tmNewTemplate({ projectId: 'p', name: 'T' }); TM.tmSyncScope(tpl2, ['A', 'B', 'C']);
  let colReads = 0;
  const columnsOf = async (t) => { colReads++; return { columns: [{ name: 'Id', dataType: 'int' }], primaryKeys: ['Id'] }; };
  // A fake backend: one batch per start, ended on the second status poll.
  function fakeBackend(opts) {
    const o = opts || {};
    const log = { start: [], status: 0, results: 0, cancel: [] };
    const batches = {};
    let n = 0;
    const post = async (pth, body) => {
      if (/start$/.test(pth)) { log.start.push(body); const id = 'b' + (++n); batches[id] = { chunks: body.chunks, polls: 0 }; return { batchId: id }; }
      if (/status$/.test(pth)) {
        log.status++;
        if (o.statusError) throw o.statusError;
        return { batches: body.batchIds.map(id => { const b = batches[id]; b.polls++;
          const ended = b.polls >= 2;
          return { id, status: ended ? 'ended' : 'in_progress', counts: { processing: ended ? 0 : b.chunks.length, succeeded: ended ? b.chunks.length : 0, errored: 0, canceled: 0, expired: 0 } }; }) };
      }
      if (/results$/.test(pth)) {
        log.results++;
        return { chunks: body.chunks.map(c => (o.failChunk && o.failChunk(c)) ? { id: c.id, ok: false, error: 'Claude could not process this batch of tables (overloaded_error)' }
          : { id: c.id, ok: true, rows: c.tables.map(t => ({ table: t, modules: [t === 'T001' ? 'A' : 'B'], confidence: 'high', required: true, reason: 'r' })), unanswered: [] }) };
      }
      if (/cancel$/.test(pth)) { log.cancel.push(body.batchIds); return { cancelled: body.batchIds }; }
      throw new Error('unexpected ' + pth);
    };
    return { post, log };
  }
  const fb = fakeBackend({ failChunk: (c) => c.id === 'c2' });
  const progress = [];
  const started = [];
  const out = await S.run({ tpl: tpl2, modules: mods3, graph, columnsOf, post: fb.post, pollMs: 0, sleep: async () => {},
    onProgress: (t) => progress.push(t), onStarted: (st) => started.push(st) });
  const sentChunks = fb.log.start[0].chunks;
  check('EVERY TABLE\'S COLUMNS ARE READ', colReads === N);
  check('chunks of at most 40, in alphabetical order, covering every table once',
    sentChunks.length === 4 && sentChunks.every(c => c.tables.length <= 40) && sentChunks[0].tables[0].name === 'T001'
    && sentChunks.reduce((n2, c) => n2 + c.tables.length, 0) === N);
  check('EVERY CHUNK GOES WITH THE FULL LIST OF TICKED MODULES (and their notes)', fb.log.start[0].modules.map(m => m.name).join() === 'A,B,C' && fb.log.start[0].modules[0].notes === 'first');
  check('each table carries its columns, key and FK neighbours',
    sentChunks[0].tables[1].name === 'T002' && sentChunks[0].tables[1].refs.join() === 'T001' && sentChunks[0].tables[0].refBy.join() === 'T002'
    && sentChunks[0].tables[0].pk.join() === 'Id' && sentChunks[0].tables[0].columns[0].name === 'Id');
  check('one batch for this size of database, polled until it ended, results fetched once', fb.log.start.length === 1 && fb.log.status === 2 && fb.log.results === 1);
  check('the run state is handed over for resume as soon as it is submitted — ids and names only, no columns',
    started.length === 1 && started[0].batches[0].id === 'b1' && typeof started[0].batches[0].chunks[0].tables[0] === 'string');
  check('progress: reading columns, then "Classifying: x of y chunks done…"',
    progress.some(t => /^Reading columns \(\d+ of 130 tables\)…$/.test(t)) && progress.some(t => t === 'Classifying: 4 of 4 chunks done…'), progress.slice(-3).join(' | '));
  check('ONE FAILED CHUNK IS KEPT APART with its tables; the rest are in the preview',
    out.preview.failed.length === 1 && out.preview.failed[0].tables.length === 40 && out.preview.summary.tables === 90, JSON.stringify(out.preview.summary));
  check('FKs are translated from schema keys to table names for the load order', out.fks.length === 1 && out.fks[0].child === 'T002' && out.fks[0].parent === 'T001');

  // Retry: only the failed tables, merged with what came back before.
  const failedNames = out.preview.failed[0].tables;
  const fb2 = fakeBackend();
  const again = await S.run({ tpl: tpl2, modules: mods3, graph, columnsOf, post: fb2.post, pollMs: 0, sleep: async () => {},
    onlyTables: failedNames, idPrefix: 'r1c', outcomes: out.outcomes, keep: S.ticksOf(out.preview) });
  check('RETRY SENDS ONLY THE FAILED TABLES', fb2.log.start[0].chunks.reduce((n2, c) => n2 + c.tables.length, 0) === 40
    && fb2.log.start[0].chunks.every(c => /^r1c\d+$/.test(c.id)) && fb2.log.start[0].chunks[0].tables.every(t => failedNames.indexOf(t.name) >= 0));
  check('…and the merged preview has every table and no failures', again.preview.failed.length === 0 && again.preview.summary.tables === N);

  // Resume: only collects.
  const fb3 = fakeBackend();
  const st3 = await S.submit({ tables: [], chunks: [{ id: 'c1', tables: [{ name: 'T001' }] }] }, { modules: mods3, post: fb3.post });
  const resumed = await S.resume(st3, { tpl: tpl2, modules: mods3, graph, post: fb3.post, pollMs: 0, sleep: async () => {} });
  check('RESUME collects the submitted batch without submitting again', fb3.log.start.length === 1 && resumed.preview.groups[0].rows[0].table === 'T001');

  // A refused key stops the polling at once.
  const e401 = Object.assign(new Error('Claude API error (401) — check the API key in Settings'), { status: 400 });
  const fb4 = fakeBackend({ statusError: e401 });
  let thrown = null;
  try { await S.run({ tpl: tpl2, modules: mods3, graph, columnsOf, post: fb4.post, pollMs: 0, sleep: async () => {} }); } catch (e) { thrown = e; }
  check('A REFUSED KEY STOPS POLLING AT ONCE — one status call, no flood', thrown === e401 && fb4.log.status === 1);
  const flaky = Object.assign(new Error('Upstream error (502)'), { status: 502 });
  const fb5 = fakeBackend({ statusError: flaky });
  thrown = null;
  try { await S.run({ tpl: tpl2, modules: mods3, graph, columnsOf, post: fb5.post, pollMs: 0, sleep: async () => {} }); } catch (e) { thrown = e; }
  check('a passing glitch is tolerated, but not for ever: five in a row and it gives up', thrown === flaky && fb5.log.status === 5);

  // Cancel during polling.
  const ctrl = new AbortController();
  const fb6 = fakeBackend();
  const cancelled = await S.run({ tpl: tpl2, modules: mods3, graph, columnsOf, post: fb6.post, pollMs: 0, signal: ctrl.signal,
    sleep: async () => { ctrl.abort(); } });
  check('CANCEL: the run ends as cancelled, the batches are cancelled, nothing is fetched',
    cancelled.cancelled === true && fb6.log.cancel.length === 1 && fb6.log.cancel[0].join() === 'b1' && fb6.log.results === 0);

  // A large database goes in several parts.
  const many = Array.from({ length: 250 }, (_, i) => ({ id: 'c' + i, tables: [{ name: 'T' + i }] }));
  check('more than 200 chunks are sent in several start calls', S.parts(many).length === 2 && S.parts(many)[0].length === 200);

  /* ── 5. Target-agnostic ─────────────────────────────────────────────────── */
  section('5. No hardcoded tables or module→table maps');
  const newCode = [src, read('azure-function', 'src', 'table-classifier-prompt.js'), read('public', 'cygenix-template-suggest.js')].join('\n');
  const threeE = ['VchrDetail', 'Matter', 'Timekeeper', 'Timecard', 'ChrgCard', 'Proforma', 'Voucher', 'CostCard', 'GLAcct', 'Client'];
  check('none of the target product\'s table names appear in the code or the prompt', threeE.every(n => !new RegExp('\\b' + n + '\\b').test(newCode)),
    threeE.filter(n => new RegExp('\\b' + n + '\\b').test(newCode)).join());
  check('no module→table map literal', !/\{\s*['"]?(Addresses|AP|Billing)['"]?\s*:\s*\[/.test(newCode));

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
