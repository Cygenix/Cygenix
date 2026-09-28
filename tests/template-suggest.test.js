// tests/template-suggest.test.js — AI "Suggest tables" for Conversion Templates.
//
// WHY THIS FILE EXISTS
//
// Suggest tables asks Claude which target tables belong to each Configurator
// module. Two things about that must hold whatever the model says, and both
// are pinned here without a network:
//
//   1. It can only ever name REAL tables. The backend drops anything not in
//      the list it was given (hallucination guard), dedupes, caps, and parses
//      a fenced or prose-wrapped answer; the page drops anything the backend
//      somehow let through by building the preview from its own schema.
//   2. It can only ever ADD. Tables already on a module are "already added",
//      never re-added or removed; a load order a person typed is never
//      overwritten — not by Apply, not by Recalculate.
//
// Plus the load order itself (parents before children, gaps of ten, a cycle
// sharing one number with a note), the Shared flag, the orchestration (chunks,
// batches, three-at-a-time, a failing module not sinking the rest, Cancel
// keeping nothing), the model's new optional fields, and — because Cygenix is
// target-agnostic — that no table names or module→table maps appear in the
// new code.

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

// The backend, with the Functions runtime and auth stubbed.
const registered = {};
const realRequire = Module.prototype.require;
Module.prototype.require = function (id) {
  if (id === '@azure/functions') return { app: { http: (name, def) => { registered[name] = def; } } };
  if (id === './entra-auth') return { enforceAuth: async () => ({ ok: true }) };
  return realRequire.call(this, id);
};
const B = require(path.join(ROOT, 'azure-function', 'src', 'template-suggest-tables.js'));
Module.prototype.require = realRequire;
const S = require(path.join(ROOT, 'public', 'cygenix-template-suggest.js'));
const TM = require(path.join(ROOT, 'public', 'cygenix-template-model.js'));

(async () => {
  console.log('Conversion Templates — Suggest tables\n');

  /* ── 1. Backend ─────────────────────────────────────────────────────────── */
  section('1. The backend: routes, parsing, and only real names');
  check('two routes are registered under the /agent/ family the data proxy already forwards',
    registered['template-suggest-shortlist'] && registered['template-suggest-shortlist'].route === 'agent/template-suggest/shortlist'
    && registered['template-suggest-rank'] && registered['template-suggest-rank'].route === 'agent/template-suggest/rank');
  check('both are function-key routes, as the other agent routes are',
    registered['template-suggest-shortlist'].authLevel === 'function' && registered['template-suggest-rank'].authLevel === 'function');
  const proxy = fs.readFileSync(path.join(ROOT, 'netlify', 'functions', 'data-proxy.js'), 'utf8');
  const rule = /return bare === '\/me' \|\| bare === '\/narrative' \|\| (\/.*\/)\.test\(bare\);/.exec(proxy);
  const re = rule && new RegExp(rule[1].slice(1, -1));
  check('the proxy\'s path rule admits both paths with no change to the proxy',
    !!re && re.test('/agent/template-suggest/shortlist') && re.test('/agent/template-suggest/rank'));
  const idx = fs.readFileSync(path.join(ROOT, 'azure-function', 'src', 'index.js'), 'utf8');
  check('index.js imports the module', /require\('\.\/template-suggest-tables'\);/.test(idx));
  check('no function.json folder was created for it', !fs.existsSync(path.join(ROOT, 'azure-function', 'template-suggest-shortlist')));
  const host = JSON.parse(fs.readFileSync(path.join(ROOT, 'azure-function', 'host.json'), 'utf8'));
  check('host.json carries no functionTimeout', !('functionTimeout' in host));

  check('JSON parsing survives code fences', JSON.stringify(B.parseModelJson('```json\n{"a":["X"]}\n```')) === '{"a":["X"]}');
  check('…and prose around the value', JSON.stringify(B.parseModelJson('Here you go: [{"table":"T"}] hope that helps')) === '[{"table":"T"}]');
  let threw = null; try { B.parseModelJson('no json here'); } catch (e) { threw = e; }
  check('…and says so when there is none', threw && threw.code === 'bad-json');

  const allowed = B.nameIndex(['Alpha', 'Beta', 'Gamma']);
  const v = B.validateShortlist({ m1: ['alpha', 'Invented', 'ALPHA', 'Beta'], m2: 'not a list' }, [{ key: 'm1' }, { key: 'm2' }, { key: 'm3' }], allowed, 40);
  check('SHORTLIST: invented names are dropped, real ones spelled as the database spells them, deduped',
    v.shortlist.m1.join() === 'Alpha,Beta' && v.dropped === 1, JSON.stringify(v));
  check('…and every module key is present, empty when the model gave nothing usable', v.shortlist.m2.length === 0 && v.shortlist.m3.length === 0);
  const capped = B.validateShortlist({ m: Array.from({ length: 60 }, (_, i) => 'T' + i) }, [{ key: 'm' }], B.nameIndex(Array.from({ length: 60 }, (_, i) => 'T' + i)), 40);
  check('…capped at 40 per module', capped.shortlist.m.length === 40);
  const r = B.validateRank([{ table: 'gamma', confidence: 'HIGH', reason: 'x'.repeat(200) }, { table: 'Nope', confidence: 'high' }, { table: 'Beta', confidence: 'weird' }, { table: 'Gamma' }], allowed);
  check('RANK: only candidates survive, confidence normalised, reasons cut to 90',
    r.ranked.length === 2 && r.ranked[0].table === 'Gamma' && r.ranked[0].confidence === 'high' && r.ranked[0].reason.length === 90
    && r.ranked[1].confidence === 'low' && r.dropped === 1, JSON.stringify(r));

  // Handlers with the model stubbed.
  const sl = await B.shortlistHandler({ modules: [{ key: 'addr', name: 'Addresses', notes: 'postal' }],
    tables: [{ name: 'SiteAddress', rows: 10 }, { name: 'Person', rows: null }] }, 'sk-ant-x',
    async (sys, user) => { check('the shortlist prompt carries the module, its notes and every name', /Addresses/.test(user) && /postal/.test(user) && /SiteAddress\t10/.test(user) && /\nPerson\n/.test(user + '\n'));
      return { addr: ['SiteAddress', 'MadeUp'] }; });
  const slBody = JSON.parse(sl.body);
  check('the shortlist handler returns only real names and the table count', sl.status === 200 && slBody.shortlist.addr.join() === 'SiteAddress' && slBody.tableCount === 2);
  const tooMany = await B.shortlistHandler({ modules: [{ key: 'm' }], tables: Array.from({ length: 1501 }, (_, i) => 'T' + i) }, 'k', async () => ({}));
  check('more than 1,500 names in one call is refused — the page chunks', tooMany.status === 413);
  const tooManyMods = await B.shortlistHandler({ modules: Array.from({ length: 9 }, (_, i) => ({ key: 'm' + i })), tables: ['T'] }, 'k', async () => ({}));
  check('more than 8 modules in one call is refused — the page batches', tooManyMods.status === 413);
  const rk = await B.rankHandler({ module: { key: 'addr', name: 'Addresses' }, candidates: [
      { name: 'SiteAddress', columns: [{ name: 'AddressLine1', type: 'nvarchar' }], fks: [{ child: 'SiteAddress', childColumn: 'SiteId', parent: 'Site', parentColumn: 'Id' }] },
      { name: 'Person', columns: [] }] }, 'k',
    async (sys, user) => { check('the rank prompt carries columns and FKs', /AddressLine1:nvarchar/.test(user) && /SiteAddress\.SiteId → Site\.Id/.test(user));
      return [{ table: 'SiteAddress', confidence: 'high', reason: 'AddressLine1, FK → Site' }, { table: 'Ghost', confidence: 'high' }]; });
  const rkBody = JSON.parse(rk.body);
  check('the rank handler returns validated rows and the FKs among them', rk.status === 200 && rkBody.ranked.length === 1
    && rkBody.fks.length === 1 && rkBody.fks[0].child === 'SiteAddress' && rkBody.fks[0].parent === 'Site', rk.body);

  // The wrapper: no key → refused before anything else; unexpected error → message + stack.
  const call = async (name, body, headers) => registered[name].handler({ method: 'POST',
    headers: { get: (h) => (headers || {})[h.toLowerCase()] || null }, json: async () => body }, { log: () => {} });
  const noKey = await call('template-suggest-rank', { module: { key: 'a' }, candidates: [] }, {});
  check('without the caller\'s own Anthropic key the route refuses', noKey.status === 400 && /x-anthropic-key/.test(noKey.body));
  const src = fs.readFileSync(path.join(ROOT, 'azure-function', 'src', 'template-suggest-tables.js'), 'utf8');
  check('an unexpected failure returns err.message and err.stack in the 500 body', /stack: \(e && e\.stack\)/.test(src) && /status: 500/.test(src));
  check('the key comes from userAnthropicKey — no key of Cygenix\'s own', /userAnthropicKey\(req\)/.test(src) && !/process\.env\.ANTHROPIC_API_KEY/.test(src));
  check('logs carry counts, never names, prompts or answers', !/ctx\.log\([^)]*(user|system|parsed|body|tables|shortlist\))/.test(src));
  check('a retry for bad JSON happens only if it still fits the proxy\'s budget', /if \(left < 6000\) throw e;/.test(src) && /BUDGET_MS = 23000/.test(src));

  /* ── 2. Load order ──────────────────────────────────────────────────────── */
  section('2. Load order: parents first, gaps of ten, cycles flagged');
  const lo = S.computeLoadOrder(['Line', 'Header', 'Customer', 'Island', 'A', 'B'],
    [{ child: 'Line', parent: 'Header' }, { child: 'Header', parent: 'Customer' }, { child: 'A', parent: 'B' }, { child: 'B', parent: 'A' }, { child: 'Line', parent: 'Line' }]);
  check('a parent always loads before its child', lo.order.customer < lo.order.header && lo.order.header < lo.order.line, JSON.stringify(lo.order));
  check('numbers step by ten', Object.values(lo.order).every(n => n % 10 === 0));
  check('a table with no FKs still gets a number', lo.order.island > 0);
  check('A CYCLE SHARES ONE NUMBER and each member names the other', lo.order.a === lo.order.b && lo.cycles.a.join() === 'B' && lo.cycles.b.join() === 'A');
  check('a self-reference is ignored', !lo.cycles.line);

  /* ── 3. Model + preview + apply ─────────────────────────────────────────── */
  section('3. Preview and apply: add-only, shared, and never over a person\'s number');
  const tpl = TM.tmNewTemplate({ projectId: 'p', name: 'T', stagingPrefix: 'stg_' });
  TM.tmSyncScope(tpl, ['Addresses', 'Contacts', 'Billing']);
  const ad = TM.tmFindModule(tpl, 'Addresses');
  const hand1 = TM.tmAddTable(tpl, 'Addresses', { targetTable: 'Site' }, 'me');
  const hand2 = TM.tmAddTable(tpl, 'Addresses', { targetTable: 'SiteAddress' }, 'me');
  check('a manual add gets a placeholder load order marked "auto"', hand1.loadOrderSource === 'auto' && hand1.loadOrder === 1);
  TM.tmUpdateTable(tpl, 'Addresses', hand2.id, { loadOrder: 5 }, 'me');
  check('a typed load order is marked "user"', hand2.loadOrderSource === 'user' && hand2.loadOrder === 5);
  TM.tmAddTable(tpl, 'Contacts', { targetTable: 'Person' }, 'me');
  const modules = [{ key: 'Addresses', name: 'Addresses' }, { key: 'Contacts', name: 'Contacts' }, { key: 'Billing', name: 'Billing' }];
  const prev = S.buildPreview(tpl, modules, [
    { ranked: [{ table: 'SiteAddress', confidence: 'high', reason: 'r' }, { table: 'AddressType', confidence: 'medium', reason: 'r' }, { table: 'Person', confidence: 'low', reason: 'r' }, { table: 'Region', confidence: 'high', reason: 'r' }] },
    { ranked: [{ table: 'Region', confidence: 'high', reason: 'r' }] },
    { ranked: [], error: 'Claude API error (500)' },
  ]);
  const g0 = prev.groups[0];
  const row = (g, t) => g.rows.find(x => x.table === t);
  check('ALREADY ADDED: a suggested table the module has is greyed and not ticked', row(g0, 'SiteAddress').already && !row(g0, 'SiteAddress').checked);
  check('defaults: High ticked, Medium and Low not', row(g0, 'Region').checked && !row(g0, 'AddressType').checked && !row(g0, 'Person').checked);
  check('SHARED: suggested for two modules', row(g0, 'Region').shared.join() === 'Contacts' && row(prev.groups[1], 'Region').shared.join() === 'Addresses');
  check('SHARED: already in another module', row(g0, 'Person').shared.join() === 'Contacts');
  check('a module that failed carries its error and the rest carry on', prev.groups[2].error === 'Claude API error (500)' && prev.groups[1].rows.length === 1);
  check('rows are grouped high → medium → low', g0.rows.map(r => r.confidence).join() === 'high,high,medium,low');

  row(g0, 'AddressType').checked = true;
  const before = JSON.stringify(ad.tables.map(t => t.targetTable));
  const res = S.apply(tpl, prev, TM, 'me', [
    { child: 'SiteAddress', parent: 'Site' }, { child: 'SiteAddress', parent: 'AddressType' }, { child: 'Site', parent: 'Region' }, { child: 'Person', parent: 'Region' }]);
  const names = ad.tables.map(t => t.targetTable);
  check('APPLY ADDS ONLY: nothing that was there is removed', JSON.parse(before).every(n => names.includes(n)) && names.join() === 'Site,SiteAddress,Region,AddressType', names.join());
  const region = ad.tables.find(t => t.targetTable === 'Region');
  check('added rows go through the model\'s add path: staging name uses the prefix', region.stagingTable === TM.tmStagingTableName('Region', 'stg_'), region.stagingTable);
  check('added rows carry source "ai", confidence and reason', region.source === 'ai' && region.aiConfidence === 'high' && region.aiReason === 'r');
  check('the summary counts added, modules, shared and ordered', res.added === 3 && res.modules === 2 && res.shared === 1 && res.ordered >= 3, JSON.stringify(res));
  const at = ad.tables.find(t => t.targetTable === 'AddressType'), site = ad.tables.find(t => t.targetTable === 'Site');
  check('LOAD ORDER: parents before children on the rows it filled', region.loadOrder < site.loadOrder && at.loadOrder % 10 === 0 && region.loadOrderSource === 'ai');
  check('A HAND-TYPED LOAD ORDER IS NOT CHANGED BY APPLY', ad.tables.find(t => t.targetTable === 'SiteAddress').loadOrder === 5);
  site.loadOrder = 77; site.loadOrderSource = 'ai';
  const legacy = TM.tmAddTable(tpl, 'Billing', { targetTable: 'Invoice' }, 'me'); delete legacy.loadOrderSource; legacy.loadOrder = 3;
  S.applyLoadOrder(tpl, [{ child: 'Site', parent: 'Region' }], 'recalc');
  check('RECALCULATE replaces AI numbers', site.loadOrder !== 77 && site.loadOrderSource === 'ai');
  check('…but never a typed one', ad.tables.find(t => t.targetTable === 'SiteAddress').loadOrder === 5);
  check('…nor a row from before sources existed, whose number may have been typed', legacy.loadOrder === 3);
  TM.tmUpdateTable(tpl, 'Addresses', region.id, { notes: 'checked' }, 'me');
  check('EDITING AN AI ROW CLEARS THE BADGE', region.source === 'user' && !('aiConfidence' in region) && !('aiReason' in region));
  const cyc = TM.tmNewTemplate({ projectId: 'p', name: 'C' }); TM.tmSyncScope(cyc, ['M']);
  TM.tmAddTable(cyc, 'M', { targetTable: 'P' }, 'me'); TM.tmAddTable(cyc, 'M', { targetTable: 'Q' }, 'me');
  S.applyLoadOrder(cyc, [{ child: 'P', parent: 'Q' }, { child: 'Q', parent: 'P' }], 'apply');
  const [p, q] = TM.tmFindModule(cyc, 'M').tables;
  check('a cycle\'s rows share a number and say so in their notes', p.loadOrder === q.loadOrder && /FK cycle with Q — review load order/.test(p.notes));
  S.applyLoadOrder(cyc, [{ child: 'P', parent: 'Q' }, { child: 'Q', parent: 'P' }], 'recalc');
  check('…once, not again on every recalculation', (p.notes.match(/FK cycle/g) || []).length === 1);
  const migrated = TM.tmMigrate(JSON.parse(JSON.stringify({ modules: [{ module: 'Old', tables: [{ id: 't', targetTable: 'X', loadOrder: 2 }] }] })));
  check('an old template with none of the new fields still loads unchanged', migrated.modules[0].tables[0].loadOrder === 2 && !('source' in migrated.modules[0].tables[0]));

  /* ── 4. Orchestration ───────────────────────────────────────────────────── */
  section('4. The run: chunks, batches, three at a time, failures, cancel');
  const bigGraph = { tables: Array.from({ length: 3100 }, (_, i) => ({ schema: 'dbo', name: 'T' + i, key: 'dbo.T' + i, rowCount: i })),
    edges: [{ from: 'dbo.T1', to: 'dbo.T0', fromColumn: 'a', toColumn: 'b' }] };
  const mods = Array.from({ length: 10 }, (_, i) => ({ key: 'k' + i, name: 'Mod ' + i, notes: '' }));
  const tpl2 = TM.tmNewTemplate({ projectId: 'p', name: 'T' }); TM.tmSyncScope(tpl2, mods.map(m => m.name));
  let slCalls = 0, maxTables = 0, maxMods = 0, rkActive = 0, rkPeak = 0, rankCalls = 0;
  const post = async (pth, body) => {
    if (/shortlist$/.test(pth)) {
      slCalls++; maxTables = Math.max(maxTables, body.tables.length); maxMods = Math.max(maxMods, body.modules.length);
      const out = {}; body.modules.forEach(m => { out[m.key] = [body.tables[0].name, 'T1']; }); return { shortlist: out };
    }
    rankCalls++; rkActive++; rkPeak = Math.max(rkPeak, rkActive);
    await new Promise(r => setTimeout(r, 5));
    rkActive--;
    if (body.module.key === 'k3') throw new Error('boom');
    return { ranked: body.candidates.map(c => ({ table: c.name, confidence: 'high', reason: 'ok' })), fks: [] };
  };
  const progress = [];
  const out = await S.run({ tpl: tpl2, modules: mods, graph: bigGraph, post, columnsOf: async () => [{ name: 'Id', dataType: 'int' }], onProgress: (t) => progress.push(t) });
  check('3,100 names go in chunks of at most 1,500, modules in batches of at most 8', maxTables <= 1500 && maxMods <= 8 && slCalls === 3 * 2, slCalls + ' calls');
  check('RANK RUNS AT MOST THREE AT A TIME', rkPeak <= 3 && rankCalls === 10, rkPeak);
  check('one failing module is reported, the rest are ranked', out.preview.groups[3].error === 'boom' && out.preview.groups[0].rows.length > 0);
  check('progress names the module being ranked', progress.some(t => /^Ranking Mod \d+ \(\d+\/10\)…$/.test(t)) && progress.some(t => /^Shortlisting/.test(t)), progress.slice(0, 4).join(' | '));
  check('FKs are translated from schema keys to table names for the load order', out.fks.length === 1 && out.fks[0].child === 'T1' && out.fks[0].parent === 'T0');

  const ctrl = new AbortController();
  let afterCancel = 0;
  const slow = async (pth, body, signal) => {
    if (/shortlist$/.test(pth)) { const o = {}; body.modules.forEach(m => { o[m.key] = ['T0']; }); return { shortlist: o }; }
    if (ctrl.signal.aborted) afterCancel++;
    ctrl.abort();
    return { ranked: [{ table: 'T0', confidence: 'high', reason: '' }] };
  };
  const cancelled = await S.run({ tpl: tpl2, modules: mods, graph: bigGraph, post: slow, columnsOf: async () => [], signal: ctrl.signal });
  check('CANCEL: the run ends as cancelled and starts no further calls', cancelled.cancelled === true && afterCancel === 0);

  /* ── 5. Target-agnostic ─────────────────────────────────────────────────── */
  section('5. No hardcoded tables or module→table maps');
  const newCode = [src, fs.readFileSync(path.join(ROOT, 'public', 'cygenix-template-suggest.js'), 'utf8')].join('\n');
  const threeE = ['VchrDetail', 'Matter', 'Timekeeper', 'Timecard', 'ChrgCard', 'Proforma', 'Voucher', 'CostCard', 'GLAcct', 'Client'];
  check('none of the target product\'s table names appear in the new code', threeE.every(n => !new RegExp('\\b' + n + '\\b').test(newCode)),
    threeE.filter(n => new RegExp('\\b' + n + '\\b').test(newCode)).join());
  check('no module→table map literal', !/\{\s*['"]?(Addresses|AP|Billing)['"]?\s*:\s*\[/.test(newCode));

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
