/* ============================================================================
   cygenix-assistant-actions.js — everything the assistant can do, declared.
   ----------------------------------------------------------------------------
   Load after cygenix-assistant.js, on every app page.

   Nothing is scraped from the DOM: each capability is a named action with a
   typed schema, an effect (read / write / destructive) and a handler. A
   redesign of a screen cannot silently change what the assistant does.

   WIRING
   Reads that can be satisfied from the console's own localStorage work as-is.
   Anything that executes against a real system goes through one adapter
   object, so there is exactly one place a page connects its own functions:

     window.CygenixAssistantAdapters = {
       navigateToView: v    => showView(v),                  // dashboard shell
       getEditorSql:   ()   => editor.value,                 // sql-editor
       setEditorSql:   sql  => { editor.value = sql; },
       runSql:         ({sql, maxRows}) => dbCall(sql),
       listTables:     ({side}) => CygenixSchemaGraph.load(side),
       describeTable:  ({table, side}) => CygenixSchemaGraph.columns(...),
       auditAction:    entry => ...                          // optional
     };

   An adapter left unimplemented produces a clear "not wired up in this build"
   result, which the model is instructed to report plainly instead of
   improvising. Deliberately NOT wired: running or cancelling migration jobs,
   and deleting mappings. The Execute page's own guard rails (RBAC, profile
   write-guards, typed confirmation) exist precisely so a click cannot start a
   migration casually — an assistant action that bypassed them would undo
   that. The assistant can list jobs and take the user to the Execute page;
   the run button stays human.

   SECRETS STAY OUT OF THE CONVERSATION
   Connection reads return names and modes only — never connection strings,
   function keys or credentials. Everything an action returns is sent to the
   model, and the conversation must never be the place secrets leak.
   ========================================================================== */
(function (root, factory) {
  var api = { register: factory };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && root.CygenixAssistant && typeof root.document !== 'undefined') {
    factory(root.CygenixAssistant, root);
  }
})(typeof window !== 'undefined' ? window : this, function (A, root) {
'use strict';
root = root || (typeof window !== 'undefined' ? window : {});

var adapters = root.CygenixAssistantAdapters = root.CygenixAssistantAdapters || {};

function need(name) {
  var fn = adapters[name];
  if (typeof fn !== 'function') {
    throw new Error('"' + name + '" is not wired up in this build. Tell the user this ' +
      'capability needs connecting in CygenixAssistantAdapters, and suggest what they can do manually.');
  }
  return fn;
}
function ls(key, fallback) {
  try { var v = localStorage.getItem(key); return v ? JSON.parse(v) : fallback; }
  catch (e) { return fallback; }
}
function activeProject() {
  try { return localStorage.getItem('cygenix_active_project_id') || null; } catch (e) { return null; }
}
function trim(obj, max) {
  // Keep tool results small — big payloads crowd out the conversation.
  var s = JSON.stringify(obj);
  if (s.length <= (max || 12000)) return obj;
  return { truncated: true, note: 'Result too large; showing the first entries only.',
    sample: Array.isArray(obj) ? obj.slice(0, 40) : s.slice(0, 4000) };
}
/* Saved connections, NAMES ONLY. The real records carry connection strings
   and function keys; those never go to the model. */
function connectionSummaries() {
  var list = [];
  try {
    if (root.CygenixConnections && typeof root.CygenixConnections.savedGetAll === 'function') {
      list = root.CygenixConnections.savedGetAll() || [];
    }
  } catch (e) { list = []; }
  return list.map(function (c) {
    return { id: c.id, name: c.name || null, side: c.side || null, mode: c.mode || null };
  });
}

/* ================================================================ *
 * App map — mirrors the sidebar's navigation tree
 * ================================================================ */

var PAGES = [
  { key: 'dashboard',          label: 'Home',               view: 'dashboard' },
  { key: 'search',             label: 'Search',             view: 'search' },
  { key: 'projects',           label: 'My Projects',        href: '/projects.html' },
  { key: 'effort-estimator',   label: 'Configurator',       href: '/effort_estimator.html' },
  { key: 'project-plan-grid',  label: 'Project Plan',       href: '/project_plan.html' },
  { key: 'connections',        label: 'Connections',        view: 'connections' },
  { key: 'profiles',           label: 'Profiles',           href: '/profiles.html' },
  { key: 'integrations',       label: 'Integrations',       view: 'integrations' },
  { key: 'project-settings',   label: 'Settings — General', view: 'project-settings' },
  { key: 'notifications',      label: 'Notifications',      view: 'notifications' },
  { key: 'system-parameters',  label: 'System Parameters',  view: 'system-parameters' },
  { key: 'user-roles',         label: 'Users & Roles',      href: '/user_roles.html' },
  { key: 'object-mapping',     label: 'Object Mapping',     href: '/object_mapping.html' },
  { key: 'schema-explorer',    label: 'Schema Explorer',    href: '/schema_explorer.html' },
  { key: 'sql-editor',         label: 'SQL Editor',         href: '/sql-editor.html' },
  { key: 'agentive-migration', label: 'AI Assist',          href: '/agentive_migration.html' },
  { key: 'jobs',               label: 'All Jobs',           view: 'jobs' },
  { key: 'project-builder',    label: 'Execute',            href: '/project-builder.html' },
  { key: 'task-agent',         label: 'Task Manager',       view: 'task-agent' },
  { key: 'server-migration',   label: 'Server Migration',   view: 'server-migration' },
  { key: 'assurance',          label: 'Assurance',          href: '/assurance.html' },
  { key: 'data-quality',       label: 'Quality Review',     href: '/data-quality.html' },
  { key: 'data-cleansing',     label: 'Cleansing',          href: '/data-cleansing.html' },
  { key: 'data-enrichment',    label: 'Data Enrichment',    href: '/data-enrichment.html' },
  { key: 'validation',         label: 'Validation',         href: '/validation.html' },
  { key: 'report-builder',     label: 'Report Builder',     href: '/reports.html' },
  { key: 'reports',            label: 'Conversion Report',  view: 'reports' },
  { key: 'inventory',          label: 'Project Artifacts',  view: 'inventory' },
  { key: 'privacy-security',   label: 'Governance',         view: 'privacy-security' },
  { key: 'audit',              label: 'Audit Log',          view: 'audit' },
  { key: 'performance',        label: 'Performance',        href: '/performance.html' }
];
A.appMap = PAGES.map(function (p) { return { key: p.key, label: p.label }; });

function findPage(key) {
  var k = String(key || '').toLowerCase();
  return PAGES.filter(function (p) {
    return p.key === k || p.label.toLowerCase() === k;
  })[0] || null;
}

/* ================================================================ *
 * Core — available on every page
 * ================================================================ */

A.registerActions([
  {
    name: 'app_navigate',
    title: 'Open a screen',
    effect: 'read',
    description: 'Move the user to another Cygenix screen. Use this before any action ' +
      'that requires a particular page. The page reloads, and you will receive the new ' +
      'page context in the result.',
    input_schema: {
      type: 'object',
      properties: {
        page: { type: 'string', enum: PAGES.map(function (p) { return p.key; }),
                description: 'Page key to open.' },
        reason: { type: 'string', description: 'One short line shown to the user explaining why.' }
      },
      required: ['page']
    },
    summary: function (i) { return (findPage(i.page) || {}).label || i.page; },
    preview: function (i) { return 'Open ' + ((findPage(i.page) || {}).label || i.page); },
    handler: async function (i) {
      var p = findPage(i.page);
      if (!p) throw new Error('Unknown page "' + i.page + '".');
      if (p.href) return { __navigate: p.href, __result: 'Opened ' + p.label + '.' };
      // Dashboard-embedded views: switch in place when the shell is here,
      // otherwise navigate the way the sidebar does.
      if (typeof adapters.navigateToView === 'function') {
        await adapters.navigateToView(p.view);
        return { ok: true, page: p.key, note: 'Switched to the ' + p.label + ' view.' };
      }
      return { __navigate: '/dashboard.html#goto=' + encodeURIComponent(p.view),
               __result: 'Opened ' + p.label + '.' };
    }
  },

  {
    name: 'app_read_screen',
    title: 'Read the current screen',
    effect: 'read',
    description: 'Return what the user is currently looking at: page, active project, ' +
      'saved connections (names only), and whatever state this screen has registered. ' +
      'Call this first when the user says "this", "here", or "what am I looking at".',
    input_schema: { type: 'object', properties: {} },
    handler: async function () {
      return trim({
        project: activeProject(),
        projects: (ls('cygenix_projects', []) || []).map(function (p) {
          return { id: p.id, name: p.name }; }),
        connections: connectionSummaries(),
        note: 'Screen-specific state is included in the context block of this turn.'
      });
    }
  },

  {
    name: 'app_list_capabilities',
    title: 'List what I can do here',
    effect: 'read',
    description: 'List the actions available, and which page each needs. Use when the ' +
      'user asks what you can do, or when you are unsure an action exists.',
    input_schema: { type: 'object', properties: {} },
    handler: async function () {
      var reg = A.getActions();
      return Object.keys(reg).map(function (n) {
        return { name: n, title: reg[n].title, effect: reg[n].effect, page: reg[n].page || 'any' };
      });
    }
  },

  {
    name: 'app_point_at',
    title: 'Point at a control',
    effect: 'read',
    description: 'Highlight an element on the current screen and scroll it into view, so ' +
      'the user can see exactly which control you mean. Prefer this over describing a ' +
      'location in words.',
    input_schema: {
      type: 'object',
      properties: {
        selector: { type: 'string', description: 'CSS selector of the element.' },
        label: { type: 'string', description: 'What it is, in a few words.' }
      },
      required: ['selector']
    },
    summary: function (i) { return i.label || i.selector; },
    handler: async function (i) {
      var node = null;
      try { node = document.querySelector(i.selector); } catch (e) { /* bad selector */ }
      if (!node) return { found: false, note: 'No element matches that selector on this page.' };
      A.highlight(i.selector);
      return { found: true, text: (node.textContent || '').trim().slice(0, 120) };
    }
  }
]);

/* ================================================================ *
 * SQL Editor
 * ================================================================ */

A.registerActions([
  {
    name: 'sql_read_editor',
    title: 'Read the SQL editor',
    effect: 'read', page: 'sql-editor',
    description: 'Return the SQL currently in the editor.',
    input_schema: { type: 'object', properties: {} },
    handler: async function () {
      var sql = (typeof adapters.getEditorSql === 'function')
        ? await adapters.getEditorSql()
        : (function () {
            // the handoff draft is stored as a raw string, not JSON
            try { return localStorage.getItem('cygenix_sql_editor_draft') || ''; }
            catch (e) { return ''; }
          })();
      return { sql: sql, length: (sql || '').length };
    }
  },
  {
    name: 'sql_write_editor',
    title: 'Write SQL into the editor',
    effect: 'write', page: 'sql-editor',
    description: 'Replace the editor contents. This does NOT execute anything — use it to ' +
      'put a query in front of the user for review.',
    input_schema: {
      type: 'object',
      properties: { sql: { type: 'string' } }, required: ['sql']
    },
    preview: function (i) { return i.sql; },
    summary: function (i) { return (i.sql || '').split('\n')[0].slice(0, 60); },
    highlight: '#sql-editor',
    handler: async function (i) {
      await need('setEditorSql')(i.sql);
      return { ok: true, note: 'Editor updated. Not executed.' };
    }
  },
  {
    name: 'sql_run',
    title: 'Run a SQL query',
    effect: 'write', page: 'sql-editor',
    description: 'Execute SQL against the connection selected in the editor and return ' +
      'the rows. Treat any statement that is not a SELECT as a change to the database ' +
      'and say so plainly before proposing it.',
    input_schema: {
      type: 'object',
      properties: {
        sql: { type: 'string' },
        maxRows: { type: 'integer', description: 'Default 200.' }
      },
      required: ['sql']
    },
    preview: function (i) {
      var destructive = !/^\s*(select|with|explain|show|describe)\b/i.test(i.sql || '');
      return (destructive ? 'WARNING — this statement CHANGES data:\n\n' : 'Run against the database:\n\n') + i.sql;
    },
    summary: function (i) { return (i.sql || '').split('\n')[0].slice(0, 60); },
    handler: async function (i) {
      var out = await need('runSql')({ sql: i.sql, maxRows: i.maxRows || 200 });
      return trim(out);
    }
  },
  {
    name: 'sql_list_scripts',
    title: 'List saved SQL scripts',
    effect: 'read',
    description: 'List the saved scripts in this project.',
    input_schema: { type: 'object', properties: {} },
    handler: async function () {
      return trim((ls('cygenix_sql_scripts', []) || []).map(function (s) {
        return { id: s.id, name: s.name, desc: s.desc || null,
                 lines: (s.sql || '').split('\n').length };
      }));
    }
  }
]);

/* ================================================================ *
 * Schema
 * ================================================================ */

A.registerActions([
  {
    name: 'schema_list_tables',
    title: 'List tables',
    effect: 'read',
    description: 'List tables in the source or target database, optionally filtered by ' +
      'a name pattern. Reads the live schema on the Schema Explorer page; elsewhere it ' +
      'answers from the locally cached schema, if one exists.',
    input_schema: {
      type: 'object',
      properties: {
        side: { type: 'string', enum: ['src', 'tgt'], description: 'Default src.' },
        pattern: { type: 'string', description: 'Case-insensitive substring filter.' }
      }
    },
    summary: function (i) { return (i.side || 'src') + (i.pattern ? ' ~ ' + i.pattern : ''); },
    handler: async function (i) {
      var tables = [];
      if (typeof adapters.listTables === 'function') {
        tables = await adapters.listTables({ side: i.side || 'src' });
      } else {
        // Fall back to whatever the Schema Explorer has cached locally.
        for (var k = 0; k < localStorage.length; k++) {
          var key = localStorage.key(k);
          if (key && key.indexOf('cygenix_schema_') === 0) {
            var doc = ls(key, null);
            var g = doc && doc.graph;
            (g && g.tables || []).forEach(function (t) {
              tables.push({ schema: t.schema, name: t.name, rowCount: t.rowCount == null ? null : t.rowCount });
            });
          }
        }
        if (!tables.length) {
          throw new Error('No schema is cached locally. Open the Schema Explorer once ' +
            '(app_navigate to schema-explorer) so the schema can be read.');
        }
      }
      if (i.pattern) {
        var p = i.pattern.toLowerCase();
        tables = tables.filter(function (t) { return String(t.name).toLowerCase().indexOf(p) !== -1; });
      }
      return trim({ count: tables.length, tables: tables.slice(0, 300) });
    }
  },
  {
    name: 'schema_describe_table',
    title: 'Describe a table',
    effect: 'read', page: 'schema-explorer',
    description: 'Return columns, types, nullability, and keys for one table.',
    input_schema: {
      type: 'object',
      properties: {
        table: { type: 'string', description: 'schema.table or bare table name.' },
        side: { type: 'string', enum: ['src', 'tgt'], description: 'Default src.' }
      },
      required: ['table']
    },
    summary: function (i) { return i.table; },
    handler: async function (i) { return trim(await need('describeTable')(i)); }
  }
]);

/* ================================================================ *
 * Object Mapping
 * ================================================================ */

A.registerActions([
  {
    name: 'mapping_list',
    title: 'List mappings',
    effect: 'read',
    description: 'List the object mapping work in progress, per source object.',
    input_schema: { type: 'object', properties: {} },
    handler: async function () {
      // WIP is stored per object under a shared prefix.
      var out = [];
      try {
        for (var k = 0; k < localStorage.length; k++) {
          var key = localStorage.key(k);
          if (key && key.indexOf('cygenix_objmap_wip_') === 0) {
            var wip = ls(key, null);
            if (wip && typeof wip === 'object') {
              out.push({ key: key.slice('cygenix_objmap_wip_'.length),
                mapped: Array.isArray(wip.mappings) ? wip.mappings.length
                  : (Array.isArray(wip.rows) ? wip.rows.length : null) });
            }
          }
        }
      } catch (e) { /* storage unavailable */ }
      if (!out.length) return { count: 0, note: 'No mapping work in progress is stored on this browser.' };
      return trim({ count: out.length, objects: out.slice(0, 250) });
    }
  }
]);

/* ================================================================ *
 * Jobs — read-only, deliberately.
 * Starting or cancelling a migration goes through the Execute page's own
 * guard rails (RBAC, profile write-guards, typed confirmation). The
 * assistant lists and navigates; the run button stays human.
 * ================================================================ */

A.registerActions([
  {
    name: 'jobs_list',
    title: 'List jobs',
    effect: 'read',
    description: 'List migration jobs and their most recent status. To run one, take ' +
      'the user to the Execute page (app_navigate to project-builder) — jobs are ' +
      'started there, behind its own confirmations, never from this panel.',
    input_schema: {
      type: 'object',
      properties: { status: { type: 'string', description: 'Filter, e.g. failed, running, complete, ready.' } }
    },
    handler: async function (i) {
      var jobs = ls('cygenix_jobs', []) || [];
      if (i.status) {
        jobs = jobs.filter(function (j) {
          return String(j.status || '').toLowerCase() === i.status.toLowerCase(); });
      }
      return trim({ count: jobs.length, jobs: jobs.slice(0, 120).map(function (j) {
        return { id: j.id, name: j.name, status: j.status || null,
                 lastRun: j.lastRun || null, lastRowCount: j.lastRowCount == null ? null : j.lastRowCount }; }) });
    }
  }
]);

/* ================================================================ *
 * Context + starter prompts
 * ================================================================ */

A.registerContext(function () {
  return {
    activeProject: activeProject(),
    connectionCount: connectionSummaries().length,
    jobCount: (ls('cygenix_jobs', []) || []).length
  };
});

A.suggestions = [
  'What am I looking at?',
  'Show me failed jobs',
  'Find a table and draft a query for it',
  'What can you do on this screen?'
];

return { PAGES: PAGES };
});
