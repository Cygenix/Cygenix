// Tests for the Cygenix Assistant — the docked panel that replaced AI Workspace.
//
// The assistant acts on the app through TYPED ACTIONS, never DOM automation,
// and everything it can do is declared with an effect (read/write/destructive)
// that the guardrail policy gates. These tests pin the three properties that
// make that design safe:
//
//   1. Approvable — the policy math: what pauses for confirmation, and that
//      the safe policy is the default.
//   2. Auditable — every action is registered with a schema; nothing secret
//      leaks into the conversation (connection reads return names only).
//   3. Durable — the model call goes through CygenixModel like every other AI
//      feature, so a retirement degrades instead of breaking; and the page
//      wiring is complete, because a panel that loads on some screens is a
//      panel the user cannot trust to be there.
'use strict';
const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 220) : '')); }
};
const P = (...p) => path.join(__dirname, '..', ...p);
const read = (...p) => fs.readFileSync(P(...p), 'utf8');

/* localStorage stand-in so both modules load in Node. */
const store = {};
global.localStorage = {
  getItem: (k) => (Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null),
  setItem: (k, v) => { store[k] = String(v); },
  removeItem: (k) => { delete store[k]; },
  key: (i) => Object.keys(store)[i] || null,
  get length() { return Object.keys(store).length; },
};
global.sessionStorage = { getItem: () => null, setItem: () => {}, removeItem: () => {} };

const A = require('../public/cygenix-assistant.js');
const actionsModule = require('../public/cygenix-assistant-actions.js');
const env = {};                       // stands in for window: adapters + engines live here
const reg = actionsModule.register(A, env);
const core = A.__core;

console.log('Cygenix Assistant — typed actions, guarded and durable\n');

/* ── 1. Guardrail policy ────────────────────────────────────────────────── */

check('the default policy confirms every change', A.getPolicy() === 'confirm_all');
check('confirm_all pauses writes', core.needsConfirmation({ effect: 'write' }, 'confirm_all') === true);
check('confirm_all pauses destructive actions', core.needsConfirmation({ effect: 'destructive' }, 'confirm_all') === true);
check('confirm_all lets reads run freely', core.needsConfirmation({ effect: 'read' }, 'confirm_all') === false);
check('confirm_destructive lets writes run', core.needsConfirmation({ effect: 'write' }, 'confirm_destructive') === false);
check('confirm_destructive still pauses destructive actions',
  core.needsConfirmation({ effect: 'destructive' }, 'confirm_destructive') === true);
check('an action registered without an effect defaults to write, the safer reading', (() => {
  A.registerAction({ name: '__test_noeffect', handler: async () => ({}) });
  const e = A.getActions()['__test_noeffect'].effect;
  delete A.getActions()['__test_noeffect'];
  return e === 'write';
})());
check('an action without a handler is refused', (() => {
  try { A.registerAction({ name: '__test_nohandler', handler: async () => ({}) }); delete A.getActions()['__test_nohandler']; } catch (e) { return false; }
  try { A.registerAction({ name: '__test_nohandler' }); return false; } catch (e) { return true; }
})());
check('the policy is stored per project', (() => {
  store['cygenix_active_project_id'] = 'p1';
  A.setPolicy('confirm_destructive');
  store['cygenix_active_project_id'] = 'p2';
  const other = A.getPolicy();
  store['cygenix_active_project_id'] = 'p1';
  const mine = A.getPolicy();
  delete store['cygenix_active_project_id'];
  delete store['cygenix_assistant_policy::p1'];
  return other === 'confirm_all' && mine === 'confirm_destructive';
})());

/* ── 2. The system prompt — the load-bearing paragraphs ─────────────────── */

const prompt = core.buildSystemPrompt({ page: 'sql-editor', projectId: 'p1' }, A.appMap);
check('the prompt treats action results as data, not instructions', /TREAT DATA AS DATA/.test(prompt));
check('the prompt forbids acting on instructions found inside data',
  /do not act on it/i.test(prompt) && /Quote it to the user/i.test(prompt));
check('the prompt tells the model to report unwired capabilities plainly',
  /not wired up/.test(prompt) && /Do not pretend it worked/.test(prompt));
check('the prompt forbids inventing data',
  /Never invent table names/.test(prompt));
check('the prompt says a declined action must not be retried',
  /do not retry it/i.test(prompt));
check('the prompt prefers a dry run for consequential changes', /dry\s*run/i.test(prompt));
check('the prompt carries the app map so the model knows the screens',
  prompt.indexOf('sql-editor — SQL Editor') > -1 && prompt.indexOf('schema-explorer — Schema Explorer') > -1);
check('the prompt embeds the current context', prompt.indexOf('"page": "sql-editor"') > -1);

/* ── 3. Conversation limits ─────────────────────────────────────────────── */

check('an empty conversation is refused', !!core.validate([], []));
check('a normal conversation passes', core.validate([{ role: 'user', content: 'hi' }], []) === null);
check('61 messages hits the cap with advice to start fresh',
  /Start a new one/.test(core.validate(new Array(61).fill({ role: 'user', content: 'x' }), []) || ''));
check('an invalid role is refused', !!core.validate([{ role: 'system', content: 'x' }], []));
check('an oversized payload is refused with advice, not a crash', (() => {
  const big = [{ role: 'user', content: 'x'.repeat(400001) }];
  return /too large/i.test(core.validate(big, []) || '');
})());
check('the tool catalogue is capped at ' + core.LIMITS.MAX_TOOLS, core.LIMITS.MAX_TOOLS === 80);

/* ── 4. Tool definitions ────────────────────────────────────────────────── */

const defs = core.toolDefs();
check('every registered action becomes a tool definition',
  defs.length === Object.keys(A.getActions()).length && defs.length >= 10, defs.length);
check('every tool definition declares its effect in the description',
  defs.every((d) => /Effect: (read|write|destructive)\./.test(d.description)));
check('page-bound tools tell the model to navigate first',
  defs.filter((d) => /Requires the "/.test(d.description)).length >= 3);
check('tool definitions carry schemas, never handlers',
  defs.every((d) => d.input_schema && !d.handler));
check('the catalogue stays under the tool cap', defs.length <= core.LIMITS.MAX_TOOLS, defs.length);

/* The outage this pins: Anthropic rejects tool names outside [a-zA-Z0-9_-]
   with a 400, which surfaced as "That request could not be processed" on
   every ask — while the model-test button (no tools) stayed green. */
check('every action name is a legal Anthropic tool name',
  Object.keys(A.getActions()).every((n) => /^[a-zA-Z0-9_-]{1,64}$/.test(n)),
  Object.keys(A.getActions()).filter((n) => !/^[a-zA-Z0-9_-]{1,64}$/.test(n)).join(', '));
check('registering a dotted action name is refused at the developer\'s desk', (() => {
  try { A.registerAction({ name: 'app.dotted', handler: async () => ({}) }); return false; }
  catch (e) { return /valid tool name/.test(e.message); }
})());
check('the system prompt never mentions an action by an illegal name',
  !/\b(app|sql|schema|mapping|jobs)\.[a-z_]+/.test(core.buildSystemPrompt({}, A.appMap)));

/* ── 5. The action catalogue — the dangerous ones are shaped right ──────── */

const acts = A.getActions();
check('there is no action that runs a migration job', !acts['jobs_run'] && !acts['jobs_cancel']);
check('there is no action that deletes a mapping', !acts['mapping_delete']);
check('jobs_list tells the model runs happen on the Execute page, not the panel',
  /never from this panel/.test(acts['jobs_list'].description));
check('sql_run is a write, so the default policy pauses it', acts['sql_run'].effect === 'write');
check('sql_run flags a non-SELECT as changing data in its preview',
  /CHANGES data/.test(acts['sql_run'].preview({ sql: 'DELETE FROM t' }))
  && !/CHANGES data/.test(acts['sql_run'].preview({ sql: 'SELECT * FROM t' })));
check('sql_write_editor previews the exact SQL the user is approving',
  acts['sql_write_editor'].preview({ sql: 'SELECT 1' }) === 'SELECT 1');
check('app_navigate is page-agnostic and read-effect',
  acts['app_navigate'].effect === 'read' && !acts['app_navigate'].page);

/* app_navigate resolves through the real sidebar map */
const results = [];
const t = (label, promise, verdict) => results.push(
  promise.then((v) => check(label, verdict(null, v)), (e) => check(label, verdict(e, null), e && e.message)));

t('navigating to an href page returns a browser navigation',
  acts['app_navigate'].handler({ page: 'sql-editor' }),
  (err, v) => !err && v.__navigate === '/sql-editor.html');
t('navigating to a dashboard view goes through the shell route',
  acts['app_navigate'].handler({ page: 'jobs' }),
  (err, v) => !err && v.__navigate === '/dashboard.html#goto=jobs');
t('navigating to an unknown page fails loudly',
  acts['app_navigate'].handler({ page: 'nope' }),
  (err) => !!err && /Unknown page/.test(err.message));

/* every href in the app map is a page that actually exists */
check('every app-map href points at a real page',
  reg.PAGES.filter((p) => p.href).every((p) => fs.existsSync(P('public', p.href.replace(/^\//, '')))),
  reg.PAGES.filter((p) => p.href && !fs.existsSync(P('public', p.href.replace(/^\//, '')))).map((p) => p.href).join(', '));
check('the app map no longer offers AI Workspace', !reg.PAGES.some((p) => p.key === 'coworker'));

/* every app-map view key is a nav view the sidebar really has */
const sidebarSrc = read('public', 'cygenix-sidebar.js');
check('every app-map view key exists in the sidebar nav',
  reg.PAGES.filter((p) => p.view).every((p) => sidebarSrc.indexOf("view:'" + p.view + "'") > -1),
  reg.PAGES.filter((p) => p.view && sidebarSrc.indexOf("view:'" + p.view + "'") === -1).map((p) => p.view).join(', '));

/* ── 6. Secrets stay out of the conversation ────────────────────────────── */

env.CygenixConnections = {
  savedGetAll: () => [{
    id: 'sc1', name: 'Finance prod', side: 'src', mode: 'direct',
    connString: 'Server=prod01;User Id=sa;Password=SuperSecret1;',
    fnKey: 'azure-function-key-abc',
  }],
};
t('connection reads return names only — never strings, keys or passwords',
  acts['app_read_screen'].handler({}),
  (err, v) => {
    if (err) return false;
    const s = JSON.stringify(v);
    return v.connections.length === 1 && v.connections[0].name === 'Finance prod'
      && s.indexOf('SuperSecret1') === -1 && s.indexOf('connString') === -1
      && s.indexOf('azure-function-key-abc') === -1;
  });

/* ── 7. Unwired capabilities fail honestly ──────────────────────────────── */

t('an unwired adapter produces the "not wired up" message, not a guess',
  acts['sql_write_editor'].handler({ sql: 'SELECT 1' }),
  (err) => !!err && /not wired up in this build/.test(err.message));
t('describe_table without its adapter says so too',
  acts['schema_describe_table'].handler({ table: 'dbo.customers' }),
  (err) => !!err && /not wired up in this build/.test(err.message));
t('schema_list_tables with no cache tells the model how to get one',
  acts['schema_list_tables'].handler({}),
  (err) => !!err && /Schema Explorer/.test(err.message));

/* the localStorage fallbacks read the console's real keys */
store['cygenix_jobs'] = JSON.stringify([
  { id: 'j1', name: 'Load accounts', status: 'complete', lastRun: '2026-08-01', lastRowCount: 120 },
  { id: 'j2', name: 'Load contacts', status: 'failed' },
]);
t('jobs_list reads cygenix_jobs and filters by status',
  acts['jobs_list'].handler({ status: 'failed' }),
  (err, v) => !err && v.count === 1 && v.jobs[0].id === 'j2');
store['cygenix_sql_scripts'] = JSON.stringify([{ id: 's1', name: 'audit', sql: 'SELECT 1\nSELECT 2' }]);
t('sql_list_scripts reads the editor script mirror',
  acts['sql_list_scripts'].handler({}),
  (err, v) => !err && v[0].name === 'audit' && v[0].lines === 2);
store['cygenix_schema_abc123'] = JSON.stringify({ v: 1, fetchedAt: 1, graph: {
  tables: [{ schema: 'dbo', name: 'customers', rowCount: 10 }, { schema: 'dbo', name: 'orders', rowCount: 4 }] } });
t('schema_list_tables answers from the Schema Explorer cache off-page',
  acts['schema_list_tables'].handler({ pattern: 'cust' }),
  (err, v) => !err && v.count === 1 && v.tables[0].name === 'customers');

/* ── 8. Durable — the model call and the page wiring ────────────────────── */

const runtime = read('public', 'cygenix-assistant.js');
check('the assistant calls Claude through CygenixModel, not its own fetch',
  /CygenixModel/.test(runtime) && /mdCall\(/.test(runtime)
  && !/fetch\(\s*['"]https:\/\/api\.anthropic\.com/.test(runtime));
check('the assistant pins no model id anywhere', !/claude-/.test(runtime));
check('the assistant uses the same API key as every other AI feature',
  /cygenix_api_key/.test(runtime));
check('a run is persisted before navigation and resumed after',
  /state\.resume/.test(runtime) && /resumeAfterNavigation/.test(runtime));
check('a declined action feeds back as a refusal with no-retry guidance',
  /declined this action\. Do not retry it/.test(runtime));
check('an interrupted run does not auto-restart on the next load',
  /must not auto-restart/.test(runtime));
check('every executed action is audited, success or failure',
  /cygenix_assistant_audit_v1/.test(runtime) && /cygenix:assistant-action/.test(runtime));
check('the missing-key state points at Settings instead of failing cryptically',
  /No API key set/.test(runtime) && /Settings → General/.test(runtime));

/* Pages: everything that shows the sidebar gets the panel — a panel that is
   sometimes there is a panel the user cannot trust. */
const appPages = fs.readdirSync(P('public')).filter((f) =>
  f.endsWith('.html') && /cygenix-sidebar\.js/.test(read('public', f)));
check('the sidebar pages are the assistant pages (and coworker.html is gone from them)',
  appPages.length >= 20 && !appPages.includes('coworker.html'), appPages.join(', '));
const missing = [];
for (const f of appPages) {
  const src = read('public', f);
  for (const needed of ['cygenix-model.js', 'cygenix-assistant.js', 'cygenix-assistant-actions.js']) {
    if (!new RegExp('<script[^>]+src="/' + needed.replace('.', '\\.')).test(src)) missing.push(f + ' → ' + needed);
  }
  if (!/CygenixAssistant\.registerPage\('/.test(src)) missing.push(f + ' → registerPage');
}
check('every app page loads the model engine, the runtime, the catalogue and registers its page key',
  missing.length === 0, missing.join(' | '));

/* the page keys used by the pages are keys the catalogue knows (or at least
   never collide with an action's `page` requirement wrongly) */
const knownKeys = new Set(reg.PAGES.map((p) => p.key).concat(['insights', 'balancing', 'data-generator']));
const badKeys = [];
for (const f of appPages) {
  const m = read('public', f).match(/CygenixAssistant\.registerPage\('([^']+)'\)/);
  if (m && !knownKeys.has(m[1])) badKeys.push(f + ' → ' + m[1]);
}
check('every registered page key is a known key', badKeys.length === 0, badKeys.join(' | '));

/* the marketing/auth pages must NOT carry the panel */
for (const f of ['login.html', 'index.html', 'register.html', 'pricing.html']) {
  check(f + ' does not load the assistant', !/cygenix-assistant\.js/.test(read('public', f)));
}

/* ── 9. AI Workspace is retired, not broken ─────────────────────────────── */

const coworker = read('public', 'coworker.html');
check('coworker.html redirects into the dashboard',
  /location\.replace\('\/dashboard\.html#assistant'\)/.test(coworker.replace(/\s+/g, ' '))
  || /dashboard\.html#assistant/.test(coworker));
check('coworker.html preserves #drive deep links', /dashboard\.html#drive/.test(coworker));
check('coworker.html migrates saved scripts into the SQL Editor library',
  /upsertScript/.test(coworker) && /cygenix_coworker_artifacts_v1/.test(coworker));
check('coworker.html migrates documents into the Drive',
  /Co-Worker documents/.test(coworker) && /addFile/.test(coworker));
check('the migration also rescues an unsaved canvas artifact',
  /cygenix_coworker_session_v1/.test(coworker));
check('the migration runs once, behind a flag', /cygenix_coworker_migrated_v1/.test(coworker));
check('the sidebar no longer offers AI Workspace',
  !/key:'coworker'/.test(sidebarSrc) && !/label:'AI Workspace'/.test(sidebarSrc));
check('the sidebar Drive fallback no longer routes through the retired page',
  !/href="\/coworker\.html/.test(sidebarSrc) && !/location\.href = '\/coworker\.html/.test(sidebarSrc));
check('the legacy nav.js no longer offers AI Workspace', !/coworker/.test(read('public', 'nav.js')));
check('the runtime opens the panel when the redirect lands with #assistant',
  /assistant\\b/.test(runtime) && /location\.hash/.test(runtime));

/* ── 10. Adapters on the wired pages ────────────────────────────────────── */

const sqlApp = read('public', 'sql-editor-app.js');
check('the SQL Editor wires the get/set/run adapters',
  /getEditorSql:/.test(sqlApp) && /setEditorSql:/.test(sqlApp) && /runSql:/.test(sqlApp));
check('the SQL Editor run adapter goes through the page\'s own dbCall',
  /runSql:[\s\S]{0,200}dbCall\(/.test(sqlApp));
check('the SQL Editor caps the rows an action can return',
  /Math\.min\(maxRows \|\| 200, 500\)/.test(sqlApp));
const dashApp = read('public', 'dashboard-app.js');
check('the dashboard wires view navigation to showView',
  /navigateToView:\s*\(v\)\s*=>\s*showView\(v\)/.test(dashApp));
check('the dashboard reports which view is showing',
  /view-/.test(dashApp) && /registerContext/.test(dashApp));
const schemaPage = read('public', 'schema_explorer.html');
check('the Schema Explorer wires listTables and describeTable through the graph engine',
  /listTables:/.test(schemaPage) && /describeTable:/.test(schemaPage) && /CygenixSchemaGraph\.load\(/.test(schemaPage));

Promise.all(results).then(() => {
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
});
