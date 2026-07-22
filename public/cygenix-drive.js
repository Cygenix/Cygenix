/* cygenix-drive.js — shared Co-Worker Drive data layer.
 *
 * The Co-Worker Drive is a virtual filesystem stored per-origin in IndexedDB
 * (DB "cygenix_coworker_drive", store "nodes"). coworker.html owns the drive
 * UI; this module exposes the SAME data layer so OTHER pages (e.g. the SQL
 * Editor) can read/write the very same drive — one shared workspace.
 *
 * Node shape: { id, parentId('' = root), name, kind:'folder'|'file',
 *               size, mime, mtime, meta?, content:Blob (files only) }
 *
 * Exposes window.CygenixDrive with the low-level ops plus convenience helpers
 * for the SQL Editor (scripts saved as real .sql files under a "SQL Editor"
 * folder, with { conn, desc, … } carried in the node's `meta`).
 */
(function () {
  'use strict';
  if (window.CygenixDrive && window.CygenixDrive.__full) return;   // already loaded

  const DB = 'cygenix_coworker_drive';

  function ddb() {
    return new Promise((res, rej) => {
      const r = indexedDB.open(DB, 1);
      r.onupgradeneeded = () => {
        const db = r.result;
        if (!db.objectStoreNames.contains('nodes')) {
          const s = db.createObjectStore('nodes', { keyPath: 'id' });
          s.createIndex('parentId', 'parentId', { unique: false });
        }
      };
      r.onsuccess = () => res(r.result);
      r.onerror   = () => rej(r.error);
    });
  }
  const put      = (n)   => ddb().then(db => new Promise((res, rej) => { const t = db.transaction('nodes', 'readwrite'); t.objectStore('nodes').put(n); t.oncomplete = () => res(n); t.onerror = () => rej(t.error); }));
  const get      = (id)  => ddb().then(db => new Promise((res, rej) => { const rq = db.transaction('nodes', 'readonly').objectStore('nodes').get(id); rq.onsuccess = () => res(rq.result); rq.onerror = () => rej(rq.error); }));
  const del      = (id)  => ddb().then(db => new Promise((res, rej) => { const t = db.transaction('nodes', 'readwrite'); t.objectStore('nodes').delete(id); t.oncomplete = () => res(); t.onerror = () => rej(t.error); }));
  const all      = ()    => ddb().then(db => new Promise((res, rej) => { const rq = db.transaction('nodes', 'readonly').objectStore('nodes').getAll(); rq.onsuccess = () => res(rq.result || []); rq.onerror = () => rej(rq.error); }));
  const children = (pid) => ddb().then(db => new Promise((res, rej) => { const rq = db.transaction('nodes', 'readonly').objectStore('nodes').index('parentId').getAll(pid || ''); rq.onsuccess = () => res(rq.result || []); rq.onerror = () => rej(rq.error); }));

  function uid() { try { if (crypto && crypto.randomUUID) return crypto.randomUUID(); } catch (_) {} return 'n' + Date.now() + Math.random().toString(16).slice(2); }
  function safeName(s) { return String(s || 'untitled').trim().replace(/[\\/:*?"<>|]+/g, '_').slice(0, 120) || 'untitled'; }

  async function createFolder(parentId, name) {
    name = (name || 'New folder').trim().replace(/[\\/]+/g, '-'); if (!name) return null;
    const kids = await children(parentId); let f = name, i = 2;
    while (kids.some(k => k.kind === 'folder' && k.name.toLowerCase() === f.toLowerCase())) f = name + ' (' + (i++) + ')';
    const n = { id: uid(), parentId: parentId || '', name: f, kind: 'folder', mtime: Date.now() };
    await put(n); return n;
  }
  async function ensureFolderPath(parentId, segs) {
    let cur = parentId || '';
    for (const s of segs) { if (!s) continue; const kids = await children(cur); let f = kids.find(k => k.kind === 'folder' && k.name === s); if (!f) f = await createFolder(cur, s); cur = f.id; }
    return cur;
  }
  async function addFile(parentId, name, blobOrText, mime, opts) {
    opts = opts || {};
    const content = (blobOrText instanceof Blob) ? blobOrText : new Blob([String(blobOrText == null ? '' : blobOrText)], { type: mime || 'text/plain' });
    const kids = await children(parentId); const dot = name.lastIndexOf('.');
    const base = dot > 0 ? name.slice(0, dot) : name, ext = dot > 0 ? name.slice(dot) : ''; let f = name, i = 2;
    while (kids.some(k => k.kind === 'file' && k.name.toLowerCase() === f.toLowerCase())) f = base + ' (' + (i++) + ')' + ext;
    const n = { id: uid(), parentId: parentId || '', name: f, kind: 'file', size: content.size, mime: mime || content.type || '', mtime: opts.mtime || Date.now(), content };
    if (opts.meta) n.meta = opts.meta;
    await put(n); return n;
  }
  async function remove(id) { const n = await get(id); if (!n) return; if (n.kind === 'folder') { const kids = await children(id); for (const k of kids) await remove(k.id); } await del(id); }
  async function rename(id, name) { const n = await get(id); if (!n) return; name = (name || '').trim(); if (!name) return; n.name = name; n.mtime = Date.now(); await put(n); }
  async function readText(nodeOrId) { const n = typeof nodeOrId === 'string' ? await get(nodeOrId) : nodeOrId; if (!n || !n.content) return ''; try { return await n.content.text(); } catch (_) { return ''; } }

  // ── SQL Editor helpers — scripts live as real .sql files in "SQL Editor" ──
  const SCRIPTS_FOLDER = 'SQL Editor';
  const scriptsFolderId = () => ensureFolderPath('', [SCRIPTS_FOLDER]);

  async function upsertScript(script) {
    const fid = await scriptsFolderId();
    const kids = await children(fid);
    const meta = {
      scriptId: script.id, name: script.name, conn: script.conn || 'source', desc: script.desc || '',
      created: script.created || new Date().toISOString(), updated: script.updated || new Date().toISOString()
    };
    const content = new Blob([script.sql || ''], { type: 'text/plain' });
    const wantName = safeName(script.name) + '.sql';
    let node = kids.find(k => k.kind === 'file' && k.meta && k.meta.scriptId === script.id);
    if (node) {
      node.content = content; node.size = content.size; node.mime = 'text/plain'; node.meta = meta; node.mtime = Date.now();
      if (node.name !== wantName && !kids.some(k => k.id !== node.id && k.kind === 'file' && k.name.toLowerCase() === wantName.toLowerCase())) node.name = wantName;
      await put(node); return node;
    }
    return addFile(fid, wantName, content, 'text/plain', { meta });
  }
  async function listScripts() {
    const fid = await scriptsFolderId();
    const files = (await children(fid)).filter(k => k.kind === 'file' && k.meta && k.meta.scriptId);
    const out = [];
    for (const n of files) { let sql = ''; try { sql = await n.content.text(); } catch (_) {} out.push({ id: n.meta.scriptId, name: n.meta.name || n.name.replace(/\.sql$/i, ''), sql, conn: n.meta.conn || 'source', desc: n.meta.desc || '', created: n.meta.created, updated: n.meta.updated }); }
    out.sort((a, b) => new Date(b.updated || 0) - new Date(a.updated || 0));
    return out;
  }
  async function syncScripts(arr) {
    arr = arr || [];
    const fid = await scriptsFolderId();
    const kids = await children(fid);
    const ids = new Set(arr.map(s => s.id));
    for (const k of kids) { if (k.kind === 'file' && k.meta && k.meta.scriptId && !ids.has(k.meta.scriptId)) await del(k.id); }
    for (const s of arr) await upsertScript(s);
  }
  const jobsFolderId = () => ensureFolderPath('', [SCRIPTS_FOLDER, 'Jobs']);
  async function saveJobToDrive(name, sql, meta) {
    const fid = await jobsFolderId();
    return addFile(fid, safeName(name) + '.sql', new Blob([sql || ''], { type: 'text/plain' }), 'text/plain', { meta: meta || {} });
  }

  // Keep the "SQL Editor › Jobs" folder in step with the migration-jobs list.
  // jobs: [{ jobId, name, sql, target? }]. Files are keyed by meta.jobId; a
  // content rewrite only happens when the size changes (cheap churn guard).
  async function syncJobs(jobs) {
    jobs = jobs || [];
    const fid = await jobsFolderId();
    const kids = await children(fid);
    const byJob = {};
    kids.forEach(k => { if (k.kind === 'file' && k.meta && k.meta.jobId) byJob[k.meta.jobId] = k; });
    const ids = new Set(jobs.map(j => j.jobId));
    for (const j of jobs) {
      const content = new Blob([j.sql || ''], { type: 'text/plain' });
      const meta = { jobId: j.jobId, name: j.name || '', target: j.target || '', updated: new Date().toISOString() };
      const node = byJob[j.jobId];
      if (node) {
        if (node.size !== content.size) { node.content = content; node.size = content.size; node.meta = meta; node.mtime = Date.now(); await put(node); }
        else if (!node.meta || node.meta.name !== meta.name) { node.meta = meta; await put(node); }
      } else {
        await addFile(fid, safeName(j.name || ('job_' + j.jobId)) + '.sql', content, 'text/plain', { meta });
      }
    }
    for (const k of kids) { if (k.kind === 'file' && k.meta && k.meta.jobId && !ids.has(k.meta.jobId)) await del(k.id); }
  }

  // Every .sql file under the "SQL Editor" folder (recursively — includes the
  // Jobs subfolder), most-recent first. Used by the editor's "Open from Drive".
  async function listSqlFiles() {
    const fid = await scriptsFolderId();
    const out = [];
    async function walk(pid, prefix) {
      const kids = await children(pid);
      for (const k of kids) {
        if (k.kind === 'folder') await walk(k.id, prefix + k.name + '/');
        else if (/\.sql$/i.test(k.name)) out.push({ id: k.id, name: k.name, path: prefix + k.name, meta: k.meta || null, mtime: k.mtime });
      }
    }
    await walk(fid, SCRIPTS_FOLDER + '/');
    out.sort((a, b) => (b.mtime || 0) - (a.mtime || 0));
    return out;
  }

  window.CygenixDrive = {
    __full: true, ready: Promise.resolve(),
    all, get, children, put, del, uid, safeName,
    createFolder, ensureFolderPath, addFile, remove, rename, readText,
    // SQL Editor conveniences
    scriptsFolderId, jobsFolderId, upsertScript, listScripts, syncScripts,
    saveJobToDrive, syncJobs, listSqlFiles
  };
})();
