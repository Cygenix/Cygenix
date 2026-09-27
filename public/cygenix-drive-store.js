/* cygenix-drive-store.js — the Assistant's door to the Drive.
 *
 * WHAT THIS IS
 * The docked Assistant can read the SQL editor and run SQL, but it could not
 * see the user's Drive, keep working files, or remember a user's standing
 * rules between conversations. This module gives it a path-based view of the
 * Drive — `Claude/Demo/scripts/load.sql` rather than node ids — with the
 * rules of the road built in:
 *
 *   · it may READ the whole Drive (the Drive is used for project work only);
 *   · it may WRITE only inside `Claude/<active project>/`, its own workspace;
 *   · it never reads a file that looks like it holds a secret.
 *
 * WHY NOT CALL drive.js DIRECTLY
 * Three Drive UIs and the sync engine all read and write ONE IndexedDB store
 * ("cygenix_coworker_drive", store "nodes"); cygenix-drive-sync.js keeps that
 * in step with the cloud by a three-way merge. An Assistant that posted to
 * the cloud function itself would bypass that merge and the next sync would
 * fight it. So this writes to the same IndexedDB the UIs use, then asks the
 * sync engine to run — with the actor marked, so the server can apply its
 * own check (see netlify/functions/drive.js).
 *
 * THE WORKSPACE
 * A reserved top-level folder `Claude` (meta.reserved = 'claude'), with one
 * subfolder per project (meta.claudeProject = <projectId>). The project
 * folder is found by that id, never by name, so renaming a project in
 * Cygenix does not lose the folder — the folder is renamed on next use
 * instead. Each holds rules.md, notes.md, scripts/ and results/.
 *
 *   rules.md   the user's own standing instructions — a normal file they
 *              can see and edit, read at the start of every conversation.
 *   notes.md   the Assistant's own notes to itself between conversations.
 *   .history/  where an overwritten or deleted file's previous content goes
 *              (last 10 per file), so the Assistant cannot destroy anything.
 *
 * THE SECRET GUARD, AND WHAT IT IS NOT
 * Everything the Assistant reads is sent to Anthropic in a prompt. A Drive
 * used for project work still ends up holding local.settings.json, an .env,
 * a connection string pasted into a note. So a read is refused by PATH for
 * the folders and file names that hold credentials by convention, and by
 * CONTENT for the patterns a credential leaves — and the refusal names the
 * file and the reason, so the user knows what was not read and why.
 *
 * This is a guard against ACCIDENTS, not a security boundary. It runs in
 * the user's own browser under the user's own token; the user can read
 * every one of these files themselves, and a determined prompt could talk
 * around a pattern list. What it stops is the ordinary mistake: "summarise
 * my Drive" quietly shipping a password to a third party.
 *
 * NODE-REQUIRABLE
 * The IndexedDB calls, the sync call and the clock are injected, so the path
 * and guard rules run unchanged in tests/drive-store.test.js without a
 * browser. In the browser the defaults are the real ones and the instance
 * is exported as window.CygenixDriveStore.
 */
(function (root, factory) {
  var mod = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = mod;
  if (root && typeof root.indexedDB !== 'undefined' && !root.CygenixDriveStore) {
    root.CygenixDriveStore = mod.create();
    root.CygenixDriveStore.__module = mod;
  }
})(typeof window !== 'undefined' ? window : this, function () {
'use strict';

var DB = 'cygenix_coworker_drive';
var STORE = 'nodes';
var RESERVED_NAME = 'Claude';
var HISTORY_DIR = '.history';
var HISTORY_KEEP = 10;

/* ── Secret guard ───────────────────────────────────────────────────────
   One exported list, one test per entry (tests/drive-store.test.js). */
var SECRET_PATHS = {
  // Any path with one of these as a folder segment is hidden and refused.
  folders: ['.git', '.vs', '.vscode', 'node_modules', '.azure', '.ssh'],
  // A file whose NAME matches one of these is hidden and refused.
  files: [
    /^local\.settings\.json$/i,
    /^\.env$/i,
    /^\.env\..+$/i,
    /\.pem$/i,
    /\.pfx$/i,
    /\.key$/i,
    /^id_rsa/i,
    /\.publishsettings$/i,
  ],
};
var SECRET_CONTENT = [
  // A credential inside a connection string: the key, an equals sign and a
  // value. "-- reset the password" in a comment has no `=` and passes.
  { name: 'connection-string password', re: /\b(Password|Pwd|AccountKey)\s*=\s*[^;\s]+/i },
  { name: 'Anthropic API key',          re: /sk-ant-/ },
  { name: 'GitHub token',               re: /\b(ghp_[A-Za-z0-9]{6,}|github_pat_[A-Za-z0-9_]{6,})/ },
  { name: 'private key',                re: /-----BEGIN [A-Z ]*PRIVATE KEY/ },
  { name: 'shared access signature',    re: /SharedAccessSignature/i },
  { name: 'client secret',              re: /client_secret/i },
];
var TEXT_EXT = /\.(sql|md|txt|csv|json|xml|ya?ml|log)$/i;

function isBlockedPath(path) {
  var segs = splitPath(path);
  if (!segs) return true;
  for (var i = 0; i < segs.length; i++) {
    var s = segs[i].toLowerCase();
    if (i < segs.length - 1 && SECRET_PATHS.folders.indexOf(s) !== -1) return true;
    if (i === segs.length - 1) {
      if (SECRET_PATHS.folders.indexOf(s) !== -1) return true;    // the folder itself
      for (var k = 0; k < SECRET_PATHS.files.length; k++) if (SECRET_PATHS.files[k].test(segs[i])) return true;
    }
  }
  return false;
}
function findSecret(text) {
  var s = String(text || '');
  for (var i = 0; i < SECRET_CONTENT.length; i++) if (SECRET_CONTENT[i].re.test(s)) return SECRET_CONTENT[i].name;
  return null;
}
// A file in .history/ is named <original>.<yyyyMMdd-HHmmss>, optionally
// with the " (2)" a name clash adds, so its extension is no longer at the
// end. Strip those before deciding whether it is text — a kept version of a
// .sql file is as readable as the file it came from.
var HISTORY_SUFFIX = /\.\d{8}-\d{6}(?: \(\d+\))?$/;
function isTextName(name) { return TEXT_EXT.test(String(name || '').replace(HISTORY_SUFFIX, '')); }

/* ── Paths ──────────────────────────────────────────────────────────────
   `/`-separated names from the Drive root. Refused outright: a leading `/`,
   a backslash, `..`, an empty segment. Returns the segments or null. */
function splitPath(path) {
  if (path == null) return [];
  var p = String(path);
  if (p === '' || p === '.') return [];
  if (p.charAt(0) === '/' || p.indexOf('\\') !== -1) return null;
  var segs = p.split('/');
  for (var i = 0; i < segs.length; i++) {
    var s = segs[i];
    if (s === '' || s === '.' || s === '..' || /[\u0000-\u001f]/.test(s)) return null;
  }
  return segs;
}
function joinPath(segs) { return segs.join('/'); }

/* ── The instance ─────────────────────────────────────────────────────── */
function create(deps) {
  deps = deps || {};
  var idb = deps.idb || realIdb();
  var now = deps.now || function () { return Date.now(); };
  var uid = deps.uid || function () {
    try { if (typeof crypto !== 'undefined' && crypto.randomUUID) return crypto.randomUUID(); } catch (e) { /* fall through */ }
    return 'n' + now() + Math.random().toString(16).slice(2);
  };
  var makeContent = deps.makeContent || function (text, mime) {
    return (typeof Blob !== 'undefined') ? new Blob([text], { type: mime || 'text/plain' }) : String(text);
  };
  var readContent = deps.readContent || function (content) {
    if (content == null) return Promise.resolve('');
    if (typeof content === 'string') return Promise.resolve(content);
    if (typeof content.text === 'function') return content.text();
    return Promise.resolve(String(content));
  };
  var sizeOf = function (content) {
    if (content == null) return 0;
    if (typeof content === 'string') return content.length;
    return content.size || 0;
  };
  // Runs the sync engine with the actor marked. Optional: a page without the
  // engine still gets a local write, and the engine's own poll picks it up
  // as an ordinary (unmarked) change later.
  var syncFn = deps.sync || function () {
    try {
      var S = (typeof window !== 'undefined') && window.CygenixDriveSync;
      if (S && typeof S.sync === 'function') return S.sync({ force: true, actor: 'assistant' });
    } catch (e) { /* the local write stands */ }
    return Promise.resolve(null);
  };

  function children(pid) { return idb.children(pid || ''); }
  function sameName(a, b) { return String(a || '').toLowerCase() === String(b || '').toLowerCase(); }

  /* Walk a path from the root, case-insensitively. Two siblings with the
     same name is an error, not a guess. Returns { node, chain, missing }:
     `chain` is every node from the root down to the deepest one found and
     `missing` the segments left over (empty when the whole path resolved). */
  async function walk(segs) {
    var chain = [], cur = '', node = null;
    for (var i = 0; i < segs.length; i++) {
      var kids = await children(cur);
      var hits = kids.filter(function (k) { return sameName(k.name, segs[i]); });
      if (hits.length > 1) {
        var err = new Error('"' + joinPath(segs.slice(0, i + 1)) + '" matches ' + hits.length + ' items whose names differ only in case. Rename one of them first.');
        err.code = 'ambiguous'; throw err;
      }
      if (!hits.length) return { node: node, chain: chain, missing: segs.slice(i) };
      node = hits[0]; chain.push(node); cur = node.id;
      if (node.kind !== 'folder' && i < segs.length - 1) return { node: node, chain: chain, missing: segs.slice(i + 1), notFolder: true };
    }
    return { node: node, chain: chain, missing: [] };
  }
  async function resolve(path) {
    var segs = splitPath(path);
    if (!segs) { var e = new Error('Path "' + path + '" is not allowed: use forward slashes, no leading slash, no "..".'); e.code = 'bad-path'; throw e; }
    if (!segs.length) return { node: null, chain: [], missing: [], root: true };
    return walk(segs);
  }
  async function pathOf(node) {
    var parts = [], cur = node;
    while (cur) { parts.unshift(cur.name); cur = cur.parentId ? await idb.get(cur.parentId) : null; }
    return joinPath(parts);
  }

  /* ── The workspace ──────────────────────────────────────────────── */
  async function reservedRoot() {
    var top = await children('');
    var marked = top.filter(function (k) { return k.kind === 'folder' && k.meta && k.meta.reserved === 'claude'; });
    if (marked.length) return marked[0];
    return null;
  }
  async function projectFolder(rootNode, projectId) {
    if (!rootNode) return null;
    var kids = await children(rootNode.id);
    return kids.filter(function (k) { return k.kind === 'folder' && k.meta && k.meta.claudeProject === projectId; })[0] || null;
  }
  function safeName(s) { return String(s || 'untitled').trim().replace(/[\\/:*?"<>|]+/g, '_').slice(0, 120) || 'untitled'; }
  async function uniqueName(parentId, want, kind) {
    var kids = await children(parentId);
    var name = want, i = 2;
    while (kids.some(function (k) { return k.kind === kind && sameName(k.name, name); })) name = want + ' (' + (i++) + ')';
    return name;
  }
  async function mkdir(parentId, name, meta) {
    var n = { id: uid(), parentId: parentId || '', name: name, kind: 'folder', mtime: now() };
    if (meta) n.meta = meta;
    await idb.put(n);
    return n;
  }
  async function putFile(parentId, name, text, mime, meta) {
    var content = makeContent(text, mime);
    var n = { id: uid(), parentId: parentId || '', name: name, kind: 'file', size: sizeOf(content),
              mime: mime || 'text/plain', mtime: now(), content: content };
    if (meta) n.meta = meta;
    await idb.put(n);
    return n;
  }
  var RULES_TEMPLATE =
    '# Rules for the Assistant\n\n' +
    'Standing instructions the Assistant reads at the start of every conversation.\n' +
    'Edit freely. Keep each rule to one line.\n\n' +
    '## How to write scripts\n\n' +
    '## Where things may run\n\n' +
    '## Things never to do\n';
  var NOTES_TEMPLATE = "# Assistant's notes\n";

  async function ensureWorkspace(projectId, projectName) {
    if (!projectId) { var e = new Error('No active project — open a project before using the workspace.'); e.code = 'no-project'; throw e; }
    var rootNode = await reservedRoot();
    if (!rootNode) {
      // A user-made folder called "Claude" is theirs; the reserved one is
      // found by its mark, so both can exist and the reserved one gets a
      // distinct name.
      rootNode = await mkdir('', await uniqueName('', RESERVED_NAME, 'folder'), { reserved: 'claude' });
    }
    var proj = await projectFolder(rootNode, projectId);
    var wantName = safeName(projectName || projectId);
    if (!proj) {
      proj = await mkdir(rootNode.id, await uniqueName(rootNode.id, wantName, 'folder'), { claudeProject: projectId });
    } else if (!sameName(proj.name, wantName)) {
      // The project was renamed in Cygenix: rename the folder, unless a
      // sibling already has that name.
      var clash = (await children(rootNode.id)).some(function (k) { return k.id !== proj.id && sameName(k.name, wantName); });
      if (!clash) { proj.name = wantName; proj.mtime = now(); await idb.put(proj); }
    }
    var kids = await children(proj.id);
    var has = function (name, kind) { return kids.filter(function (k) { return k.kind === kind && sameName(k.name, name); })[0]; };
    if (!has('rules.md', 'file')) await putFile(proj.id, 'rules.md', RULES_TEMPLATE, 'text/markdown');
    if (!has('notes.md', 'file')) await putFile(proj.id, 'notes.md', NOTES_TEMPLATE, 'text/markdown');
    if (!has('scripts', 'folder')) await mkdir(proj.id, 'scripts');
    if (!has('results', 'folder')) await mkdir(proj.id, 'results');
    return { root: rootNode, project: proj, path: rootNode.name + '/' + proj.name };
  }

  /* ── The write guard ────────────────────────────────────────────────
     A path is writable only if the deepest EXISTING node on it sits inside
     the active project's workspace — decided from the resolved parent
     chain, never from the string. "Claude/Demo/x" typed against a Drive
     where "Claude" is the user's own folder resolves to the wrong chain and
     is refused; a folder marked claudeProject that the user has moved
     elsewhere is refused too, because its chain no longer reaches the
     reserved root. */
  async function workspaceOf(projectId) {
    var rootNode = await reservedRoot();
    var proj = await projectFolder(rootNode, projectId);
    return { root: rootNode, project: proj };
  }
  function insideWorkspace(chain, ws) {
    if (!ws.root || !ws.project) return false;
    var sawRoot = false, sawProj = false;
    for (var i = 0; i < chain.length; i++) {
      if (chain[i].id === ws.root.id && i === 0) sawRoot = true;
      if (chain[i].id === ws.project.id && i === 1 && sawRoot) sawProj = true;
    }
    return sawRoot && sawProj;
  }
  async function checkWrite(path, projectId) {
    var segs = splitPath(path);
    if (!segs) return { ok: false, reason: 'Path "' + path + '" is not allowed: use forward slashes, no leading slash, no "..".' };
    if (!segs.length) return { ok: false, reason: 'A file name is needed.' };
    var ws = await workspaceOf(projectId);
    if (!ws.project) return { ok: false, reason: 'The workspace for this project does not exist yet.' };
    var r = await walk(segs);
    if (r.notFolder) return { ok: false, reason: '"' + (await pathOf(r.node)) + '" is a file, not a folder.' };
    // The chain must run root → project → …; the target's own name must be
    // at depth ≥ 2 (never the project folder or the reserved root itself).
    var chainForCheck = r.missing.length ? r.chain : r.chain.slice(0, -1);
    if (r.chain.length && !r.missing.length && r.node.kind === 'folder') return { ok: false, reason: '"' + path + '" is a folder.' };
    if (!insideWorkspace(chainForCheck.length >= 2 ? chainForCheck : r.chain, ws) || (r.chain.length + r.missing.length) < 3) {
      return { ok: false, reason: 'The Assistant may only write inside ' + ws.root.name + '/' + ws.project.name + '/. "' + path + '" is outside it.' };
    }
    if (!insideWorkspace(r.chain.slice(0, 2), ws)) {
      return { ok: false, reason: 'The Assistant may only write inside ' + ws.root.name + '/' + ws.project.name + '/. "' + path + '" is outside it.' };
    }
    return { ok: true, ws: ws, resolved: r, segs: segs };
  }

  /* ── History ────────────────────────────────────────────────────────
     Nothing the Assistant overwrites or deletes is destroyed: the previous
     content goes to Claude/<project>/.history/<name>.<yyyyMMdd-HHmmss>,
     and the last ten versions of each name are kept. */
  function stamp(t) {
    var d = new Date(t), p = function (n) { return (n < 10 ? '0' : '') + n; };
    return d.getFullYear() + p(d.getMonth() + 1) + p(d.getDate()) + '-' + p(d.getHours()) + p(d.getMinutes()) + p(d.getSeconds());
  }
  async function historyFolder(ws) {
    var kids = await children(ws.project.id);
    var h = kids.filter(function (k) { return k.kind === 'folder' && sameName(k.name, HISTORY_DIR); })[0];
    return h || mkdir(ws.project.id, HISTORY_DIR);
  }
  async function moveToHistory(node, ws) {
    var h = await historyFolder(ws);
    var base = node.name;
    node.parentId = h.id;
    node.name = await uniqueName(h.id, base + '.' + stamp(now()), 'file');
    node.mtime = now();
    await idb.put(node);
    await pruneHistory(h.id, base);
  }
  async function pruneHistory(historyId, base) {
    var kids = (await children(historyId)).filter(function (k) {
      return k.kind === 'file' && k.name.toLowerCase().indexOf(base.toLowerCase() + '.') === 0;
    });
    kids.sort(function (a, b) { return (b.mtime || 0) - (a.mtime || 0) || String(b.name).localeCompare(String(a.name)); });
    for (var i = HISTORY_KEEP; i < kids.length; i++) await idb.del(kids[i].id);
  }

  /* ── The public surface ────────────────────────────────────────────── */
  async function list(path, opts) {
    opts = opts || {};
    var r = await resolve(path || '');
    if (!r.root && (r.missing.length || !r.node)) { var e = new Error('"' + path + '" was not found.'); e.code = 'not-found'; throw e; }
    if (!r.root && r.node.kind !== 'folder') { var e2 = new Error('"' + path + '" is a file.'); e2.code = 'not-folder'; throw e2; }
    var basePath = r.root ? '' : await pathOf(r.node);
    var out = [], truncated = false, cap = opts.cap || 500, maxDepth = opts.recursive ? Math.min(opts.depth || 4, 4) : 0;
    async function rec(pid, prefix, depth) {
      var kids = await children(pid);
      kids.sort(function (a, b) { return a.kind === b.kind ? String(a.name).localeCompare(String(b.name)) : (a.kind === 'folder' ? -1 : 1); });
      for (var i = 0; i < kids.length; i++) {
        var k = kids[i], p = prefix ? prefix + '/' + k.name : k.name;
        if (isBlockedPath(p)) continue;                              // hidden entirely
        if (out.length >= cap) { truncated = true; return; }
        out.push({ path: p, name: k.name, kind: k.kind, size: k.kind === 'file' ? (k.size || 0) : undefined,
                   modified: k.mtime ? new Date(k.mtime).toISOString() : null,
                   workspace: (k.meta && k.meta.reserved === 'claude') ? true : undefined });
        if (k.kind === 'folder' && depth < maxDepth) await rec(k.id, p, depth + 1);
      }
    }
    await rec(r.root ? '' : r.node.id, basePath, 0);
    return { path: basePath, entries: out, truncated: truncated, note: truncated ? 'Listing cut at ' + cap + ' entries.' : undefined };
  }

  async function readText(path, opts) {
    opts = opts || {};
    var maxChars = opts.maxChars || 200000;
    if (isBlockedPath(path)) { var e = new Error('Refused: "' + path + '" is the kind of file that holds credentials, and the Assistant does not read those.'); e.code = 'secret-path'; throw e; }
    var r = await resolve(path);
    if (r.root || r.missing.length || !r.node) { var e1 = new Error('"' + path + '" was not found.'); e1.code = 'not-found'; throw e1; }
    if (r.node.kind !== 'file') { var e2 = new Error('"' + path + '" is a folder.'); e2.code = 'not-file'; throw e2; }
    var real = await pathOf(r.node);
    if (!isTextName(r.node.name)) return { path: real, size: r.node.size || 0, readable: false, note: 'not readable by the Assistant yet' };
    var text = await readContent(r.node.content);
    var hit = findSecret(text);
    if (hit) { var e3 = new Error('Refused: "' + real + '" contains what looks like a ' + hit + ', and the Assistant does not read files that hold credentials.'); e3.code = 'secret-content'; throw e3; }
    var cut = text.length > maxChars;
    var body = cut ? text.slice(0, maxChars) : text;
    return { path: real, size: r.node.size || text.length, readable: true, text: body, truncated: cut,
             note: cut ? 'Cut at ' + maxChars + ' characters; the file is ' + text.length + '.' : undefined, lines: body.split('\n').length };
  }

  async function search(query, opts) {
    opts = opts || {};
    var q = String(query || '').trim();
    if (!q) return { hits: [], note: 'Empty query.' };
    var ql = q.toLowerCase(), hits = [], cap = opts.cap || 50, maxBytes = opts.maxBytes || 1048576, truncated = false;
    async function rec(pid, prefix) {
      var kids = await children(pid);
      for (var i = 0; i < kids.length && !truncated; i++) {
        var k = kids[i], p = prefix ? prefix + '/' + k.name : k.name;
        if (isBlockedPath(p)) continue;
        if (String(k.name).toLowerCase().indexOf(ql) !== -1) { hits.push({ path: p, kind: k.kind, match: 'name' }); if (hits.length >= cap) { truncated = true; return; } }
        if (k.kind === 'folder') { await rec(k.id, p); continue; }
        if (!isTextName(k.name) || (k.size || 0) > maxBytes) continue;
        var text = await readContent(k.content);
        if (findSecret(text)) continue;                                  // never surface a line from a file with a secret in it
        var lines = text.split('\n');
        for (var n = 0; n < lines.length; n++) {
          if (lines[n].toLowerCase().indexOf(ql) !== -1) {
            hits.push({ path: p, line: n + 1, text: lines[n].slice(0, 200) });
            if (hits.length >= cap) { truncated = true; return; }
          }
        }
      }
    }
    await rec('', '');
    return { hits: hits, truncated: truncated, note: truncated ? 'Cut at ' + cap + ' hits.' : undefined };
  }

  async function writeText(path, text, opts) {
    opts = opts || {};
    var projectId = opts.projectId;
    var g = await checkWrite(path, projectId);
    if (!g.ok) { var e = new Error(g.reason); e.code = 'outside-workspace'; throw e; }
    var mode = opts.mode === 'overwrite' ? 'overwrite' : 'create';
    var r = g.resolved, segs = g.segs;
    var existing = (!r.missing.length && r.node && r.node.kind === 'file') ? r.node : null;
    if (existing && mode === 'create') { var e1 = new Error('"' + path + '" already exists. Use mode "overwrite" to replace it.'); e1.code = 'exists'; throw e1; }
    // Taken BEFORE moveToHistory, which renames the node object in place to
    // its stamped history name — reading it afterwards created the
    // replacement under that name, which is the bug the test caught.
    var origName = existing ? existing.name : segs[segs.length - 1];
    var replaced = false;
    if (existing) {
      // The old content goes to .history under its old name; then a fresh
      // node takes the name, so the file's id changes and the sync engine
      // sees two ordinary edits (a move and a create) rather than a
      // rewrite it might merge the wrong way.
      await moveToHistory(existing, g.ws);
      replaced = true;
    }
    // Create any missing folders along the way, inside the workspace.
    var parentId = r.chain.length ? r.chain[r.chain.length - 1].id : '';
    if (existing) parentId = r.chain.length >= 2 ? r.chain[r.chain.length - 2].id : g.ws.project.id;
    var missing = existing ? [] : r.missing.slice(0, -1);
    for (var i = 0; i < missing.length; i++) parentId = (await mkdir(parentId, missing[i])).id;
    var name = origName;
    var mime = opts.mime || (/\.md$/i.test(name) ? 'text/markdown' : /\.json$/i.test(name) ? 'application/json' : 'text/plain');
    var node = await putFile(parentId, name, String(text == null ? '' : text), mime);
    var real = await pathOf(node);
    var synced = await syncFn();
    return { path: real, size: node.size, replaced: replaced, synced: !!synced };
  }

  async function remove(path, opts) {
    opts = opts || {};
    var g = await checkWrite(path, opts.projectId);
    if (!g.ok) { var e = new Error(g.reason); e.code = 'outside-workspace'; throw e; }
    var r = g.resolved;
    if (r.missing.length || !r.node || r.node.kind !== 'file') { var e1 = new Error('"' + path + '" was not found.'); e1.code = 'not-found'; throw e1; }
    var real = await pathOf(r.node);
    await moveToHistory(r.node, g.ws);
    var synced = await syncFn();
    return { path: real, movedTo: g.ws.root.name + '/' + g.ws.project.name + '/' + HISTORY_DIR + '/' + r.node.name, synced: !!synced };
  }

  return {
    list: list, readText: readText, search: search, writeText: writeText, remove: remove,
    ensureWorkspace: ensureWorkspace, resolve: resolve, pathOf: pathOf, checkWrite: checkWrite,
    workspaceOf: workspaceOf,
    isBlockedPath: isBlockedPath, findSecret: findSecret, isTextName: isTextName,
    SECRET_PATHS: SECRET_PATHS, SECRET_CONTENT: SECRET_CONTENT, HISTORY_KEEP: HISTORY_KEEP,
  };
}

/* ── The real IndexedDB, same DB and store as the three UIs ───────────── */
function realIdb() {
  function ddb() {
    return new Promise(function (res, rej) {
      var r = indexedDB.open(DB, 1);
      r.onupgradeneeded = function () {
        var db = r.result;
        if (!db.objectStoreNames.contains(STORE)) {
          var s = db.createObjectStore(STORE, { keyPath: 'id' });
          s.createIndex('parentId', 'parentId', { unique: false });
        }
      };
      r.onsuccess = function () { res(r.result); };
      r.onerror = function () { rej(r.error); };
    });
  }
  return {
    get: function (id) { return ddb().then(function (db) { return new Promise(function (res, rej) { var q = db.transaction(STORE, 'readonly').objectStore(STORE).get(id); q.onsuccess = function () { res(q.result || null); }; q.onerror = function () { rej(q.error); }; }); }); },
    put: function (n) { return ddb().then(function (db) { return new Promise(function (res, rej) { var t = db.transaction(STORE, 'readwrite'); t.objectStore(STORE).put(n); t.oncomplete = function () { res(n); }; t.onerror = function () { rej(t.error); }; }); }); },
    del: function (id) { return ddb().then(function (db) { return new Promise(function (res, rej) { var t = db.transaction(STORE, 'readwrite'); t.objectStore(STORE).delete(id); t.oncomplete = function () { res(); }; t.onerror = function () { rej(t.error); }; }); }); },
    children: function (pid) { return ddb().then(function (db) { return new Promise(function (res, rej) { var q = db.transaction(STORE, 'readonly').objectStore(STORE).index('parentId').getAll(pid || ''); q.onsuccess = function () { res(q.result || []); }; q.onerror = function () { rej(q.error); }; }); }); },
  };
}

return { create: create, splitPath: splitPath, isBlockedPath: isBlockedPath, findSecret: findSecret, isTextName: isTextName,
         SECRET_PATHS: SECRET_PATHS, SECRET_CONTENT: SECRET_CONTENT, RESERVED_NAME: RESERVED_NAME, HISTORY_DIR: HISTORY_DIR, HISTORY_KEEP: HISTORY_KEEP };
});
