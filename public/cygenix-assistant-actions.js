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

/* What the panel needs to name a click before it happens: the element's label
   and, when the runtime is asking the user to approve it, why it is asking.
   Never throws — this runs inside a render. */
function clickInfo(elementId) {
  var R = root.CygenixPageReader;
  if (!R || typeof R.clickConfirmation !== 'function') return { label: elementId, reason: '' };
  try { return R.clickConfirmation(elementId); }
  catch (e) { return { label: elementId, reason: '' }; }
}
function typeInfo(elementId) {
  var R = root.CygenixPageReader;
  if (!R || typeof R.typeConfirmation !== 'function') return { label: elementId, reason: '' };
  try { return R.typeConfirmation(elementId); }
  catch (e) { return { label: elementId, reason: '' }; }
}

/* ================================================================ *
 * App map — mirrors the sidebar's navigation tree
 * ================================================================ */

var PAGES = [
  { key: 'dashboard',          label: 'Home',               view: 'dashboard' },
  { key: 'search',             label: 'Search',             view: 'search' },
  { key: 'projects',           label: 'My Projects',        href: '/projects' },
  { key: 'effort-estimator',   label: 'Configurator',       href: '/configurator' },
  { key: 'project-plan-grid',  label: 'Project Plan',       href: '/project-plan' },
  { key: 'connections',        label: 'Connections',        view: 'connections' },
  { key: 'profiles',           label: 'Profiles',           href: '/profiles' },
  { key: 'integrations',       label: 'Integrations',       view: 'integrations' },
  { key: 'project-settings',   label: 'Settings — General', view: 'project-settings' },
  { key: 'notifications',      label: 'Notifications',      view: 'notifications' },
  { key: 'system-parameters',  label: 'System Parameters',  view: 'system-parameters' },
  { key: 'user-roles',         label: 'Users & Roles',      href: '/user-roles' },
  { key: 'object-mapping',     label: 'Object Mapping',     href: '/object-mapping' },
  { key: 'schema-explorer',    label: 'Schema Explorer',    href: '/schema-explorer' },
  { key: 'sql-editor',         label: 'SQL Editor',         href: '/sql-editor' },
  { key: 'agentive-migration', label: 'AI Assist',          href: '/agentive-migration' },
  { key: 'jobs',               label: 'All Jobs',           view: 'jobs' },
  { key: 'project-builder',    label: 'Execute',            href: '/project-builder' },
  { key: 'data-stream',        label: 'Data Stream',        href: '/data-stream' },
  { key: 'data-stream-store',  label: 'Stream Store',       href: '/data-stream-store' },
  { key: 'data-stream-events', label: 'Change Events',      href: '/data-stream-events' },
  { key: 'data-stream-monitor',label: 'Stream Monitor',     href: '/data-stream-monitor' },
  { key: 'task-agent',         label: 'Task Manager',       view: 'task-agent' },
  { key: 'server-migration',   label: 'Server Migration',   view: 'server-migration' },
  { key: 'assurance',          label: 'Assurance',          href: '/assurance' },
  { key: 'data-quality',       label: 'Quality Review',     href: '/data-quality' },
  { key: 'data-cleansing',     label: 'Cleansing',          href: '/data-cleansing' },
  { key: 'data-enrichment',    label: 'Data Enrichment',    href: '/data-enrichment' },
  { key: 'validation',         label: 'Validation',         href: '/validation' },
  { key: 'report-builder',     label: 'Report Builder',     href: '/reports' },
  { key: 'reports',            label: 'Conversion Report',  view: 'reports' },
  { key: 'inventory',          label: 'Project Artifacts',  view: 'inventory' },
  { key: 'privacy-security',   label: 'Governance',         view: 'privacy-security' },
  { key: 'audit',              label: 'Audit Log',          view: 'audit' },
  { key: 'performance',        label: 'Performance',        href: '/performance' }
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
      return { __navigate: '/dashboard#goto=' + encodeURIComponent(p.view),
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
    /* The generic eye.
     *
     * Every other action here is typed: it knows what a stream is, what a job
     * is, what the editor holds. That is the right shape for the things worth
     * naming, and the wrong shape for a console with fifty screens — most
     * controls will never have an action of their own, and until now the
     * assistant simply could not see them.
     *
     * This reads the page the way a person does: what it says, what kind of
     * control it is, which section it sits in. It reads only. Nothing here
     * clicks or types, and the description says so plainly, because a model
     * that believes it can press a button will tell the user it has.
     */
    name: 'read_page',
    title: 'Reading page',
    icon: '◉',
    effect: 'read',
    /* Looking at the screen is what makes a second click of the same element
       something other than pressing and hoping, so a read clears the guard. */
    clearsRepeatGuard: true,
    description: 'Look at the screen the user is on and return what is drawn: the page ' +
      'title, the route, the headings, and every visible control — buttons, links, ' +
      'fields, dropdowns — each with a stable id, its kind, its label and its section. ' +
      'Use this for anything the typed actions do not cover, and again after the screen ' +
      'changes. Prefer a typed action when one exists: app_read_screen knows the project ' +
      'and the connections, this only knows what is on screen. ' +
      'It reads and nothing else — you cannot click, type or submit with it, and the ' +
      'ids are valid only for the screen they were read from. ' +
      'Values in credential fields are withheld deliberately; do not ask the user to ' +
      'type them where you can see them.',
    input_schema: { type: 'object', properties: {} },
    handler: async function () {
      var R = root.CygenixPageReader;
      if (!R || typeof R.readPage !== 'function') {
        throw new Error('cygenix-page-reader.js is not loaded on this page, so the screen ' +
          'cannot be read. Tell the user, and work from the typed actions instead.');
      }
      return R.readPage();
    }
  },

  {
    /* The generic hand.
     *
     * Everything about this action is arranged so that it cannot press
     * something the assistant has not looked at, and cannot press something
     * consequential without the user saying so. The rules themselves live in
     * cygenix-page-reader.js next to the read they depend on; what is here is
     * the wiring: the confirmation hook the runtime consults BEFORE running
     * the handler, the question it puts to the user in the assistant's own
     * words, and the repeat guard that stops the same button being pressed
     * twice with nothing learned in between.
     */
    name: 'click',
    title: 'Clicking',
    icon: '◉',
    effect: 'write',
    description: 'Click one element on the current screen, by an id from the most recent ' +
      'read_page. Call read_page first — an id from an older read, from a different screen, ' +
      'or for an element that has since changed is refused, and the refusal tells you to ' +
      'read again. Most clicks ask the user to approve them first; you will see the result ' +
      'either way, and if they decline, do not try again — ask what they would prefer. ' +
      'Anything that deletes, removes, sends, submits, publishes or archives always asks. ' +
      'After a click, the screen has probably changed: read_page again rather than trusting ' +
      'the ids you already have.',
    input_schema: {
      type: 'object',
      properties: {
        element_id: { type: 'string',
          description: 'The id of the element, exactly as read_page returned it (el_…).' },
        reason: { type: 'string',
          description: 'One short line, in plain English, telling the user what pressing ' +
            'this is meant to achieve.' }
      },
      required: ['element_id']
    },
    /* Consulted by the runtime before the handler runs. The consequence of a
       click is a property of the thing clicked, not of the action, so the
       decision has to be made per element. It fails closed: anything it cannot
       resolve confirms. */
    confirms: function (i) {
      var R = root.CygenixPageReader;
      if (!R || typeof R.clickConfirmation !== 'function') return true;
      var d = R.clickConfirmation(i.element_id);
      return d.error ? true : d.confirm;
    },
    confirmTitle: function (i) {
      var d = clickInfo(i.element_id);
      return 'Assistant wants to click "' + (d.label || i.element_id) + '". Proceed?';
    },
    preview: function (i) {
      var d = clickInfo(i.element_id);
      return (i.reason ? i.reason + '\n\n' : '') +
        'Element: ' + (d.label || i.element_id) + '\n' +
        'Why this is being asked: ' + (d.reason || 'the guardrail policy for this project.');
    },
    trailTitle: function (i) {
      var d = clickInfo(i.element_id);
      return 'Clicked: ' + (d.label || i.element_id);
    },
    /* Two clicks of the same element with no read of the screen between them
       is a stall — the assistant is pressing and hoping. read_page clears it. */
    repeatGuard: function (i) { return 'click:' + i.element_id; },
    handler: async function (i) {
      var R = root.CygenixPageReader;
      if (!R || typeof R.click !== 'function') {
        throw new Error('cygenix-page-reader.js is not loaded on this page, so nothing can ' +
          'be clicked. Tell the user, and work from the typed actions instead.');
      }
      return R.click(i.element_id);
    }
  },

  {
    /* Filling in a field.
     *
     * Quieter than click and, on this console, sharper. Nothing typed into a
     * form reaches a system until something is pressed — and that press has
     * its own confirmation — so ordinary typing goes straight in. A credential
     * field is the exception: the reader already refuses to READ those, and
     * writing to one without the operator seeing it would be an odd place to
     * stop being careful.
     */
    name: 'type',
    title: 'Typing',
    icon: '◉',
    effect: 'write',
    description: 'Replace the contents of one text field, by an id from the most recent ' +
      'read_page. Works on a text field, a text area or an editable region; anything else ' +
      'is refused — use click for those. This does not save or submit anything: something ' +
      'still has to be pressed afterwards. ' +
      'Never invent a password, key, connection string or any other credential and type it ' +
      'in. If a field needs one, say so and let the user enter it themselves — typing into ' +
      'a credential field asks them first in any case, and the value is never read back.',
    input_schema: {
      type: 'object',
      properties: {
        element_id: { type: 'string',
          description: 'The id of the field, exactly as read_page returned it (el_…).' },
        text: { type: 'string',
          description: 'The full new contents. Whatever is in the field is replaced.' },
        reason: { type: 'string',
          description: 'One short line, in plain English, telling the user what this is for.' }
      },
      required: ['element_id', 'text']
    },
    confirms: function (i) {
      var R = root.CygenixPageReader;
      if (!R || typeof R.typeConfirmation !== 'function') return true;
      var d = R.typeConfirmation(i.element_id);
      return d.error ? true : d.confirm;
    },
    confirmTitle: function (i) {
      var d = typeInfo(i.element_id);
      return 'Assistant wants to type into "' + (d.label || i.element_id) + '". Proceed?';
    },
    preview: function (i) {
      var d = typeInfo(i.element_id);
      // The text IS shown. The whole point of asking is that the operator can
      // see what is about to go into their own field and judge it.
      return (i.reason ? i.reason + '\n\n' : '') +
        'Field: ' + (d.label || i.element_id) + '\n' +
        'New contents:\n' + String(i.text == null ? '' : i.text) +
        (d.reason ? '\n\nWhy this is being asked: ' + d.reason : '');
    },
    trailTitle: function (i) {
      var d = typeInfo(i.element_id);
      return 'Typed into: ' + (d.label || i.element_id);
    },
    handler: async function (i) {
      var R = root.CygenixPageReader;
      if (!R || typeof R.type !== 'function') {
        throw new Error('cygenix-page-reader.js is not loaded on this page, so nothing can ' +
          'be typed. Tell the user, and work from the typed actions instead.');
      }
      return R.type(i.element_id, i.text);
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
 * Data Stream — read and DRAFT only.
 *
 * The assistant may list a stream, explain why one is lagging, and put a
 * draft in front of the user in the Designer. It may NOT start, pause,
 * replay or delete one. Those are changes to a live production database:
 * starting begins reading it, replaying re-delivers records that were
 * already delivered, deleting throws away a capture position. Each is
 * destructive at BOTH guardrail levels, so none of them is an action here
 * at all — the panel takes the user to the screen where the confirmation
 * with the record count lives, and a human presses the button.
 * ================================================================ */

function dsState() {
  var DS = root.CygenixDataStream;
  if (!DS) {
    throw new Error('The Data Stream module is not loaded on this page. ' +
      'Use app_navigate to open data-stream first.');
  }
  return { DS: DS, state: DS.load(activeProject()) };
}
function dsSummary(s) {
  return {
    id: s.id, name: s.name, status: s.status,
    readsFrom: s.capture.side, connection: s.capture.connectionLabel,
    capture: s.capture.methodLabel,
    destination: s.destination.label, destinationKind: s.destination.kind,
    writeMode: s.destination.writeMode,
    tables: (s.objects || []).map(function (o) { return o.table; }),
    eventsPerMin: s.metrics.eventsPerMin,
    lagSeconds: s.metrics.lagSeconds,
    pendingInStore: s.metrics.pendingInStore,
    dlqDepth: s.metrics.dlqDepth,
    lastError: s.lastError,
    lastEventAt: s.lastEventAt,
  };
}

A.registerActions([
  {
    name: 'stream_list',
    title: 'List data streams',
    effect: 'read',
    description: 'List this project\'s data streams with their status, which side they read from, ' +
      'where they deliver, and how far behind they are. A stream is the always-on shape: ' +
      'capture, a retained Stream Store, then delivery.',
    input_schema: {
      type: 'object',
      properties: {
        status: { type: 'string', description: 'Filter, e.g. running, lagging, paused, failed, draft.' },
        side: { type: 'string', enum: ['source', 'target'], description: 'Which side it reads from.' }
      }
    },
    handler: async function (i) {
      var ctx = dsState();
      var list = ctx.state.streams || [];
      if (i.status) list = list.filter(function (s) { return s.status === i.status; });
      if (i.side) list = list.filter(function (s) { return s.capture.side === i.side; });
      return trim({ count: list.length, streams: list.map(dsSummary) });
    }
  },
  {
    name: 'stream_describe',
    title: 'Describe a stream',
    effect: 'read',
    description: 'Full configuration and current numbers for one stream, by id or by name.',
    input_schema: {
      type: 'object',
      properties: { stream: { type: 'string', description: 'Stream id or name.' } },
      required: ['stream']
    },
    summary: function (i) { return i.stream; },
    handler: async function (i) {
      var ctx = dsState();
      var q = String(i.stream).toLowerCase();
      var s = (ctx.state.streams || []).filter(function (x) {
        return x.id === i.stream || String(x.name).toLowerCase() === q; })[0];
      if (!s) throw new Error('No stream called "' + i.stream + '" in this project.');
      return trim(Object.assign(dsSummary(s), {
        snapshot: s.capture.snapshot,
        position: s.capture.position,
        delivery: s.delivery,
        retentionHours: (s.profile || {}).retentionHours || null,
        objects: (s.objects || []).map(function (o) {
          return { table: o.table, keys: o.keys, ops: o.ops, filter: o.filter || null,
                   masked: o.masked || [], rowsEstimate: o.rowsEstimate, state: o.state }; }),
      }));
    }
  },
  {
    name: 'stream_explain_lag',
    title: 'Explain a stream’s lag',
    effect: 'read',
    description: 'Work out WHY a stream is behind, from its own numbers: whether capture is outpacing ' +
      'delivery, whether the destination is failing, or whether it is still snapshotting. Report the ' +
      'evidence rather than guessing.',
    input_schema: {
      type: 'object',
      properties: { stream: { type: 'string', description: 'Stream id or name.' } },
      required: ['stream']
    },
    summary: function (i) { return i.stream; },
    handler: async function (i) {
      var ctx = dsState();
      var q = String(i.stream).toLowerCase();
      var s = (ctx.state.streams || []).filter(function (x) {
        return x.id === i.stream || String(x.name).toLowerCase() === q; })[0];
      if (!s) throw new Error('No stream called "' + i.stream + '" in this project.');
      var flow = ctx.DS.flowOf(s);
      var pts = (ctx.state.points || []).filter(function (p) { return p.streamId === s.id; }).slice(-30);
      return trim({
        stream: s.name,
        status: s.status,
        lagSeconds: s.metrics.lagSeconds,
        threshold: (s.delivery || {}).lagThresholdSeconds || 30,
        captureRatePerMin: flow.capture.rate,
        deliveryRatePerMin: flow.destination.rate,
        pendingInStore: s.metrics.pendingInStore,
        bottleneck: flow.bottleneck,
        dlqDepth: s.metrics.dlqDepth,
        lastError: s.lastError,
        recentLag: pts.map(function (p) { return p.lagSeconds; }),
        recentIn: pts.map(function (p) { return p.in; }),
        recentOut: pts.map(function (p) { return p.out; }),
        note: 'Lag is how long the oldest undelivered record has been waiting. It grows whenever ' +
          'capture outruns delivery and shrinks when delivery catches up.',
      });
    }
  },
  {
    name: 'stream_draft',
    title: 'Draft a data stream',
    effect: 'write',
    description: 'Put a proposed stream in front of the user in the Stream Designer, filled in and ' +
      'ready to review. This creates NOTHING and starts NOTHING — the user still walks the five steps ' +
      'and presses Create & start themselves. Use this when they describe a stream they want.',
    input_schema: {
      type: 'object',
      properties: {
        name: { type: 'string' },
        side: { type: 'string', enum: ['source', 'target'], description: 'Which side to read from.' },
        tables: { type: 'array', items: { type: 'string' },
                  description: 'Tables to carry, as schema.table.' },
        destinationKind: { type: 'string',
          enum: ['database', 'broker', 'webhook', 'file', 'cygenix-target'] },
        destinationLabel: { type: 'string', description: 'Where it delivers, in words.' },
        writeMode: { type: 'string', enum: ['upsert', 'append', 'merge', 'audit-table'] },
        captureMethod: { type: 'string', enum: ['log', 'poll', 'trigger', 'feed'] }
      },
      required: ['name']
    },
    summary: function (i) { return i.name; },
    preview: function (i) {
      return 'Open the Stream Designer with a draft:\n\n'
        + 'Name: ' + i.name + '\n'
        + 'Reads from: ' + (i.side || 'source') + '\n'
        + 'Tables: ' + ((i.tables || []).join(', ') || 'none yet') + '\n'
        + 'Delivers to: ' + (i.destinationLabel || '(not set)')
        + '\n\nNothing is created or started — this only fills in the form.';
    },
    handler: async function (i) {
      var ctx = dsState();
      var draft = ctx.DS.blankDraft(activeProject());
      draft.name = i.name;
      draft.capture.side = i.side || 'source';
      if (i.captureMethod) draft.capture.method = i.captureMethod;
      if (i.destinationKind) draft.destination.kind = i.destinationKind;
      if (i.destinationLabel) draft.destination.label = i.destinationLabel;
      if (i.writeMode) draft.destination.writeMode = i.writeMode;
      (i.tables || []).forEach(function (t) {
        draft.objects.push({ table: t, keys: [], ops: ['I', 'U', 'D'], filter: '',
          columnPolicy: 'all', excludedColumns: [], masked: [], rowsEstimate: 0, state: 'paused' });
      });
      ctx.DS.wipSave(activeProject(), draft);
      return { __navigate: '/data-stream-designer',
               __result: 'Opened the Stream Designer with a draft of "' + i.name + '". '
                 + 'Nothing is created or started until the user confirms.' };
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
    jobCount: (ls('cygenix_jobs', []) || []).length,
    // Counts only. A stream's connection label, position and payloads stay
    // on the screen — the context block goes to the model every turn.
    streamCount: (function () {
      try { return (dsState().state.streams || []).length; } catch (e) { return 0; }
    })()
  };
});

A.suggestions = [
  'What am I looking at?',
  'Show me failed jobs',
  'Find a table and draft a query for it',
  'Why is that stream lagging?',
  'What can you do on this screen?'
];

return { PAGES: PAGES };
});
