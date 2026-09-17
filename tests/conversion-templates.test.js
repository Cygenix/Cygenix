// tests/conversion-templates.test.js — Conversion Templates, Phase 1.
//
// Two halves. The MODEL half drives the supplied engine
// (public/cygenix-template-model.js) through the rules the brief fixed —
// scope from the Configurator's ticks, dropped modules kept, staging names
// derived as STG_ + target, publish blocked while a module has no tables,
// publish freezing a copy and bumping the draft. The engine is the single
// source of truth for the document, so nothing here re-derives its shape.
//
// The WIRING half reads the shipped files: the page loads the engine and
// the schema reader after the connections helper, every write goes through
// the guarded api(), the five actions exist on the Function App and the
// proxy, every write on the Function App is a single-document upsert, the
// sidebar item is where the brief put it, the audit actions are on the
// allowlist, and the clean address is generated. No Cosmos, no browser.
'use strict';

const fs = require('fs');
const path = require('path');
const TM = require('../public/cygenix-template-model.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

console.log('Conversion Templates — the table list a client builds staging to\n');

/* ── An estimate, the way the Configurator stores one ───────────────────── */
const estimate = (ticks) => ({
  v: 1, name: 'Acme 3E', modules: ['Addresses', 'AP', 'AP Master', 'Matters', 'WIP', 'Timekeepers'],
  ticks: ticks,
});

/* ── 1. Scope comes from the Configurator ───────────────────────────────── */
{
  const doc = estimate({ 'analysis|AP': 1, 'scripts|Matters': 1, 'uat|WIP': 1 });
  check('"any" mode: a module is in scope when ANY use case is ticked for it, in the estimate\'s order',
    JSON.stringify(TM.tmScopeFromEstimate(doc, 'any')) === JSON.stringify(['AP', 'Matters', 'WIP']));
  check('"scripts" mode: only the Script development tick counts',
    JSON.stringify(TM.tmScopeFromEstimate(doc, 'scripts')) === JSON.stringify(['Matters']));
  check('the default rule is "any"', TM.TM_DEFAULT_SCOPE_MODE === 'any'
    && JSON.stringify(TM.tmScopeFromEstimate(doc)) === JSON.stringify(['AP', 'Matters', 'WIP']));
  check('an estimate with no ticks puts nothing in scope', TM.tmScopeFromEstimate(estimate({})).length === 0);
}

/* ── 2. Sync keeps dropped modules, restores them, orders by the estimate ── */
{
  const t = TM.tmNewTemplate({ name: 'Finance', projectId: 'p1', profileId: 'FIN-DEV-01', estimateId: 'Acme 3E', createdBy: 'me' });
  let r = TM.tmSyncScope(t, ['AP', 'Matters', 'WIP'], 'me');
  check('first sync adds every in-scope module', r.added.length === 3 && t.modules.length === 3);
  TM.tmAddTable(t, 'Matters', { targetTable: 'Matter' }, 'me');
  TM.tmAddTable(t, 'Matters', { targetTable: 'MattDate' }, 'me');
  r = TM.tmSyncScope(t, ['AP', 'WIP'], 'me');
  const matters = TM.tmFindModule(t, 'Matters');
  check('unticking a module marks it out of scope and KEEPS its tables',
    r.removed[0] === 'Matters' && matters.inScope === false && matters.tables.length === 2);
  check('dropped modules sort last; in-scope ones keep the estimate\'s order',
    JSON.stringify(t.modules.map(m => m.module)) === JSON.stringify(['AP', 'WIP', 'Matters']));
  r = TM.tmSyncScope(t, ['Matters', 'AP', 'WIP'], 'me');
  check('re-ticking restores it with its tables intact, and the new order is honoured',
    r.restored[0] === 'Matters' && TM.tmFindModule(t, 'Matters').tables.length === 2
    && JSON.stringify(t.modules.map(m => m.module)) === JSON.stringify(['Matters', 'AP', 'WIP']));
  check('a no-change sync reports nothing', (() => { const x = TM.tmSyncScope(t, ['Matters', 'AP', 'WIP'], 'me'); return !x.added.length && !x.removed.length && !x.restored.length; })());
}

/* ── 3. Tables: staging names, duplicates, order ────────────────────────── */
{
  const t = TM.tmNewTemplate({ name: 'x', projectId: 'p1', profileId: 'P' });
  TM.tmSyncScope(t, ['AP'], 'me');
  const a = TM.tmAddTable(t, 'AP', { targetTable: 'VchrDetail' }, 'me');
  check('a staging name defaults to STG_ + the target name', a.stagingTable === 'STG_VchrDetail' && a.required === true && a.loadOrder === 1);
  check('the same target twice is refused', TM.tmAddTable(t, 'AP', { targetTable: 'vchrdetail' }, 'me') === null);
  check('a blank target is refused', TM.tmAddTable(t, 'AP', { targetTable: '  ' }, 'me') === null);
  check('a table cannot be added to a module that is not in the template', TM.tmAddTable(t, 'Nope', { targetTable: 'X' }, 'me') === null);
  TM.tmUpdateTable(t, 'AP', a.id, { stagingTable: 'STG_VOUCHER_LINES' }, 'me');
  check('the staging name can be overridden', TM.tmFindModule(t, 'AP').tables[0].stagingTable === 'STG_VOUCHER_LINES');
  TM.tmUpdateTable(t, 'AP', a.id, { targetTable: 'Vchr' }, 'me');
  check('renaming the target re-derives the staging name', TM.tmFindModule(t, 'AP').tables[0].stagingTable === 'STG_Vchr');
  check('a custom prefix is applied at add time', (() => { const u = TM.tmNewTemplate({ name: 'y', stagingPrefix: 'IN_' }); TM.tmSyncScope(u, ['AP']); return TM.tmAddTable(u, 'AP', { targetTable: 'Vchr' }).stagingTable === 'IN_Vchr'; })());
  check('a target name with odd characters still makes a legal staging name', TM.tmStagingTableName('Odd-Name 1') === 'STG_Odd_Name_1');
  check('removing a table works and says so', TM.tmRemoveTable(t, 'AP', a.id, 'me') === true && TM.tmRemoveTable(t, 'AP', a.id, 'me') === false);
}

/* ── 4. Validation and publish ──────────────────────────────────────────── */
{
  const t = TM.tmNewTemplate({ name: 'Finance', projectId: 'p1', profileId: 'FIN-DEV-01', estimateId: 'Acme 3E', createdBy: 'me' });
  TM.tmSyncScope(t, ['AP', 'Matters'], 'me');
  TM.tmAddTable(t, 'AP', { targetTable: 'VchrDetail' }, 'me');
  const v = TM.tmValidate(t);
  check('a module in scope with no tables blocks publish, in plain words',
    !TM.tmCanPublish(t) && v.some(i => i.level === 'error' && i.module === 'Matters' && /No target tables chosen/.test(i.message)));
  TM.tmAddTable(t, 'Matters', { targetTable: 'Matter' }, 'me');
  check('with every module covered it can publish', TM.tmCanPublish(t) && TM.tmValidate(t).length === 0);
  const noProfile = TM.tmNewTemplate({ name: 'x' }); TM.tmSyncScope(noProfile, ['AP']); TM.tmAddTable(noProfile, 'AP', { targetTable: 'A' });
  check('a template with no profile cannot publish — it belongs to one', !TM.tmCanPublish(noProfile)
    && TM.tmValidate(noProfile).some(i => /connection profile/.test(i.message)));
  TM.tmSyncScope(t, ['AP'], 'me');
  check('a dropped module that still has tables is a WARNING, not a block',
    TM.tmCanPublish(t) && TM.tmValidate(t).some(i => i.level === 'warning' && i.module === 'Matters'));
  TM.tmSyncScope(t, ['AP', 'Matters'], 'me');

  const frozen = TM.tmPublish(t, 'me');
  check('publish returns a frozen copy: published, stamped, same id and version',
    frozen && frozen.status === 'published' && !!frozen.publishedAt && frozen.publishedBy === 'me'
    && frozen.id === t.id && frozen.version === t.version);
  check('…and the working document itself is untouched', t.status === 'draft' && !t.publishedAt);
  const next = TM.tmNewDraftFrom(frozen, 'me');
  check('the next draft is version + 1, a new id, draft status, same tables',
    next.version === t.version + 1 && next.id !== t.id && next.status === 'draft' && !next.publishedAt
    && TM.tmSummary(next).tableCount === 2);
  check('tmPublish refuses an invalid template', TM.tmPublish(TM.tmNewTemplate({ name: 'empty' })) === null);
  check('the summary counts what the header shows',
    (() => { const s = TM.tmSummary(t); return s.moduleCount === 2 && s.tableCount === 2 && s.modulesWithoutTables === 0 && s.outOfScopeCount === 0; })());
}

/* ── 5. The page ────────────────────────────────────────────────────────── */
{
  const page = read('public', 'conversion-templates.html');
  const order = (a, b) => page.indexOf(a) > 0 && page.indexOf(b) > 0 && page.indexOf(a) < page.indexOf(b);
  check('the page mounts the sidebar on its own key and registers with the assistant',
    /data-active="conversion-templates"/.test(page) && /CygenixAssistant\.registerPage\('conversion-templates'\)/.test(page));
  check('it loads the engine and the schema reader after the connections helper, all cache-stamped',
    order('/connections.js?v=', '/cygenix-schema-graph.js?v=') && order('/cygenix-schema-graph.js?v=', '/cygenix-template-model.js?v=')
    && /cygenix-template-model\.js\?v=[0-9a-f]{10}/.test(page));
  check('it loads the profile engine so the template can name its profile', /cygenix-profiles\.js\?v=/.test(page) && /cygenix-job-profile\.js\?v=/.test(page));
  check('every call to the data layer goes through one guarded function — in-flight and a 3-second interval',
    (page.match(/CygenixDataApi\.callResult\(/g) || []).length === 1
    && /if \(CT\.inflight\)/.test(page) && /Date\.now\(\) - last < wait/.test(page) && /const wait = [^;]*3000/.test(page));
  check('the five actions are the only ones the page calls',
    (() => { const used = new Set([...page.matchAll(/api\('(template-[a-z]+)'/g)].map(m => m[1]));
      return used.size === 5 && ['template-list', 'template-get', 'template-save', 'template-delete', 'template-publish'].every(a => used.has(a)); })());
  check('publish freezes with tmPublish, bumps with tmNewDraftFrom, and only removes the old draft after the new one is stored',
    /TM\.tmPublish\(CT\.tpl, userName\(\)\)/.test(page) && /TM\.tmNewDraftFrom\(frozen, userName\(\)\)/.test(page)
    && page.indexOf("api('template-save', { method: 'POST', body: { template: next") < page.indexOf("api('template-delete', { method: 'POST', body: { id: oldId"));
  // The gate reads the merged list (the model's checks plus import flags)
  // since the import/export work — see tests/template-io.test.js.
  check('publish is refused in the page while any error is present', /issues\.some\(i => i\.level === 'error'\)/.test(page)
    && /const okToPublish = canPublishNow\(\);/.test(page) && /\$\('ct-publish'\)\.disabled = ro \|\| !!CT\.inflight \|\| !okToPublish/.test(page)
    && /return TM\.tmValidate\(CT\.tpl\)\.concat\(IO \? IO\.flagIssues\(CT\.tpl\) : \[\]\)/.test(page));
  check('scope is re-read with tmScopeFromEstimate then tmSyncScope, on load and on Refresh scope',
    /TM\.tmScopeFromEstimate\(e\.doc, CT\.tpl\.scopeMode\)/.test(page) && /TM\.tmSyncScope\(CT\.tpl, scope, userName\(\)\)/.test(page)
    && /ctRefreshScope\(true\);\s*\n\s*render\(\);/.test(page) && /onclick="ctRefreshScope\(false\)"/.test(page));
  check('the scope rule is a setting on the template, defaulting to any', /id="ct-scopemode"/.test(page) && /<option value="any"/.test(page) && /<option value="scripts"/.test(page));
  check('dropped modules are shown greyed, labelled, and never deleted',
    /no longer in scope/.test(page) && /\.ct-mods li\.out\{opacity/.test(page) && !/modules = .*filter\(m => m\.inScope/.test(page));
  check('the picker reads the target through the schema reader ONCE and filters in memory, capped',
    /CygenixSchemaGraph\.load\('tgt'\)/.test(page) && /if \(CT\.graph && CT\.graph\.ok\) return CT\.graph;/.test(page)
    && /hits\.slice\(0, 200\)/.test(page) && !/schema-tables/.test(page));
  check('…with a typed-name fallback when the target is unavailable', /ctPickManual/.test(page) && /Type the table name below instead/.test(page));
  // Phase 2 made the category follow the action — the specification and DDL
  // exports are Data out, the rest stay Mapping & SQL.
  check('save, publish and delete are audited, on the mapping category',
    ['template.save', 'template.publish', 'template.delete'].every(a => page.indexOf("audit('" + a + "'") !== -1)
    && /function auditCategory\(action\)/.test(page) && /: 'mapping'; \}/.test(page));
  check('no one-shot flag is reset inside its own callback; nothing writes the estimate store',
    !/ES_STORE, /.test(page) && !/setItem\(ES_STORE/.test(page) && /addEventListener\('storage'/.test(page));
  check('no Function App key and no hardcoded endpoint', !/code=/.test(page) && !/azurewebsites\.net/.test(page));
}

/* ── 6. The Function App and the proxy ──────────────────────────────────── */
{
  const idx = read('azure-function', 'src', 'index.js');
  const block = idx.slice(idx.indexOf("case 'template-list':"), idx.indexOf('default:\n          return err(404'));
  check('the five actions are on the /api/data/{action} dispatcher, in the existing switch',
    ['template-list', 'template-get', 'template-save', 'template-delete', 'template-publish'].every(a => new RegExp("case '" + a + "':").test(idx))
    && /template-list, template-get, template-save, template-delete, template-publish/.test(idx));
  check('the container is conversion_templates, partitioned on /projectId, and named in the error when missing',
    /const TEMPLATES = 'conversion_templates'/.test(block) && /partition key \/projectId/.test(block) && /does not exist — create it in the Portal/.test(block));
  check('every write is a single-document upsert — nothing reads a list and writes it back',
    (block.match(/\.items\.upsert\(env\)/g) || []).length === 2 && !/\.replace\(/.test(block) && !/fetchAll\(\)[\s\S]{0,400}upsert/.test(block));
  check('the list is a summary query, newest first, by projectId with an optional profileId',
    /ORDER BY c\.updatedAt DESC/.test(block) && /@profileId/.test(block) && !/c\.doc/.test(block.slice(0, block.indexOf('template-get'))));
  check('publish stores the frozen copy under its own id and leaves the draft alone',
    /'pub_' \+ String\(doc\.id\) \+ '_v'/.test(block) && /doc\.status !== 'published' \|\| !doc\.publishedAt/.test(block) && !/template-publish[\s\S]{0,900}\.delete\(/.test(block));
  check('a published template cannot be saved over', /A published template is frozen/.test(block));
  check('the 500 carries the message and the stack, because there is no Application Insights on this plan',
    /stack: e && e\.stack \? String\(e\.stack\) : null/.test(block));
  check('the template shape is the model\'s: the server wraps it in an envelope and validates only id and projectId',
    /doc:\s+doc,/.test(block) && /\/\^tpl_\[A-Za-z0-9_\]\+\$\//.test(block) && !/modules\.push|tmNewTable|stagingTable/.test(block));
  check('no function.json folders were added; host.json has no functionTimeout',
    !fs.existsSync(path.join(ROOT, 'azure-function', 'template-list')) && !/functionTimeout/.test(read('azure-function', 'host.json')));

  const proxy = read('netlify', 'functions', 'data-proxy.js');
  check('the proxy allows exactly those five, and nothing is passed through by wildcard',
    ['template-list', 'template-get', 'template-save', 'template-delete', 'template-publish'].every(a => proxy.indexOf("'" + a + "'") !== -1) && !/template-\*/.test(proxy));
}

/* ── 7. Sidebar, routes, audit, storage ─────────────────────────────────── */
{
  const sb = read('public', 'cygenix-sidebar.js');
  const i = sb.indexOf("key:'conversion-templates'"), j = sb.indexOf("key:'object-mapping'");
  check('the sidebar item is in Map & Build, directly above Object Mapping, with a clean href',
    i > 0 && j > i && sb.slice(i, j).split('\n').filter(l => /key:'/.test(l)).length === 1
    && /href:'\/conversion-templates'/.test(sb) && sb.indexOf("section: 'Map & Build'") < i);
  const redirects = read('public', '_redirects');
  check('the clean address is generated, and the .html form redirects to it',
    /^\/conversion-templates\s+\/conversion-templates\.html\s+200$/m.test(redirects)
    && /^\/conversion-templates\.html\s+\/conversion-templates\s+301!$/m.test(redirects));
  const audit = read('netlify', 'functions', 'lib', 'audit-schema.js');
  check('template.save / publish / delete are on the client allowlist',
    ['template.save', 'template.publish', 'template.delete'].every(a => new RegExp("'" + a.replace('.', '\\.') + "':\\s*'mapping'").test(audit)));
  const inv = read('scripts', 'storage-inventory.js');
  check('both local keys are classified — the mirror as a cache, the pointer as a preference',
    /'cygenix_template_last_v1':\s*\['C'/.test(inv) && /'cygenix_template_draft_v1::\*':\s*\['B'/.test(inv));
  check('the assistant\'s app map knows the page', /key: 'conversion-templates'/.test(read('public', 'cygenix-assistant-actions.js')));
  check('netlify.toml was not touched for this (routes come from build-routes.js)', !/conversion-templates/.test(read('netlify.toml')));
}

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
