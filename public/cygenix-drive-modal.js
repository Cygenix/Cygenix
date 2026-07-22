/* cygenix-drive-modal.js — the Co-Worker Drive as a shared overlay.
 *
 * Lets ANY page open the Co-Worker's virtual Drive on top of whatever the
 * user is doing (via the sidebar's Drive button) instead of navigating to
 * coworker.html. It reads/writes the SAME IndexedDB drive
 * (DB "cygenix_coworker_drive") that coworker.html uses, so it's one shared
 * workspace everywhere.
 *
 * Everything here is namespaced with `cygdm-` / `CygenixDriveModal` so it can
 * safely co-exist with coworker.html's own (native) Drive UI — on that page
 * the sidebar prefers the native window.openDrive().
 *
 *   window.CygenixDriveModal.open()   // open the overlay
 *   window.CygenixDriveModal.close()
 *
 * Window controls: maximize / restore and minimize (docks to a small bar so
 * the page behind stays usable), plus the usual close.
 */
(function () {
  'use strict';
  if (window.CygenixDriveModal) return;                 // guard double-load

  // ── IndexedDB: the drive tree (shared with coworker.html) ─────────────────
  const DRIVE_DB = 'cygenix_coworker_drive';
  function ddb() {
    return new Promise((res, rej) => {
      const r = indexedDB.open(DRIVE_DB, 1);
      r.onupgradeneeded = () => { const db = r.result; if (!db.objectStoreNames.contains('nodes')) { const s = db.createObjectStore('nodes', { keyPath: 'id' }); s.createIndex('parentId', 'parentId', { unique: false }); } };
      r.onsuccess = () => res(r.result);
      r.onerror   = () => rej(r.error);
    });
  }
  const dput      = (n)   => ddb().then(db => new Promise((res, rej) => { const tx = db.transaction('nodes', 'readwrite'); tx.objectStore('nodes').put(n); tx.oncomplete = () => res(n); tx.onerror = () => rej(tx.error); }));
  const dget      = (id)  => ddb().then(db => new Promise((res, rej) => { const rq = db.transaction('nodes', 'readonly').objectStore('nodes').get(id); rq.onsuccess = () => res(rq.result); rq.onerror = () => rej(rq.error); }));
  const ddel      = (id)  => ddb().then(db => new Promise((res, rej) => { const tx = db.transaction('nodes', 'readwrite'); tx.objectStore('nodes').delete(id); tx.oncomplete = () => res(); tx.onerror = () => rej(tx.error); }));
  const dall      = ()    => ddb().then(db => new Promise((res, rej) => { const rq = db.transaction('nodes', 'readonly').objectStore('nodes').getAll(); rq.onsuccess = () => res(rq.result || []); rq.onerror = () => rej(rq.error); }));
  const dchildren = (pid) => ddb().then(db => new Promise((res, rej) => { const rq = db.transaction('nodes', 'readonly').objectStore('nodes').index('parentId').getAll(pid || ''); rq.onsuccess = () => res(rq.result || []); rq.onerror = () => rej(rq.error); }));

  // ── IndexedDB: small key/value store (map handle — shared with coworker) ──
  function idb() { return new Promise((res, rej) => { const r = indexedDB.open('cygenix_coworker', 1); r.onupgradeneeded = () => { try { r.result.createObjectStore('kv'); } catch (_) {} }; r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error); }); }
  async function idbSet(k, v) { const db = await idb(); return new Promise((res, rej) => { const tx = db.transaction('kv', 'readwrite'); tx.objectStore('kv').put(v, k); tx.oncomplete = () => res(); tx.onerror = () => rej(tx.error); }); }
  async function idbGet(k)    { const db = await idb(); return new Promise((res, rej) => { const tx = db.transaction('kv', 'readonly'); const rq = tx.objectStore('kv').get(k); rq.onsuccess = () => res(rq.result); rq.onerror = () => rej(rq.error); }); }
  async function idbDel(k)    { const db = await idb(); return new Promise((res, rej) => { const tx = db.transaction('kv', 'readwrite'); tx.objectStore('kv').delete(k); tx.oncomplete = () => res(); tx.onerror = () => rej(tx.error); }); }

  // ── Helpers ───────────────────────────────────────────────────────────────
  function uid() { try { if (crypto && crypto.randomUUID) return crypto.randomUUID(); } catch (_) {} return 'n' + Date.now() + Math.random().toString(16).slice(2); }
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c])); }
  function fmtSize(n) { if (n == null) return '—'; if (n < 1024) return n + ' B'; if (n < 1048576) return (n / 1024).toFixed(1) + ' KB'; if (n < 1073741824) return (n / 1048576).toFixed(1) + ' MB'; return (n / 1073741824).toFixed(2) + ' GB'; }
  function fileIcon(name) { const e = (name.split('.').pop() || '').toLowerCase();
    if (e === 'sql') return '🗄'; if (/^(md|markdown|txt|text|doc|rtf)$/.test(e)) return '📄';
    if (/^(csv|tsv|xls|xlsx)$/.test(e)) return '📊'; if (/^(json|xml|ya?ml|ini|cfg|conf)$/.test(e)) return '🔧';
    if (/^(png|jpe?g|gif|svg|webp|bmp)$/.test(e)) return '🖼'; if (/^(zip|bak|gz|7z|rar|tar)$/.test(e)) return '🗜'; return '📎'; }
  const fsSupported = () => 'showDirectoryPicker' in window;
  async function ensurePerm(h) { if (!h) return false; const o = { mode: 'readwrite' }; try { if ((await h.queryPermission(o)) === 'granted') return true; return (await h.requestPermission(o)) === 'granted'; } catch (_) { return false; } }

  // ── Drive tree ops ────────────────────────────────────────────────────────
  async function driveFolderPath(id) { const parts = []; let cur = id, g = 0; while (cur && g++ < 50) { const n = await dget(cur); if (!n) break; parts.unshift(n); cur = n.parentId || ''; } return parts; }
  async function driveCreateFolder(parentId, name) {
    name = (name || 'New folder').trim().replace(/[\\/]+/g, '-'); if (!name) return null;
    const kids = await dchildren(parentId); let f = name, i = 2;
    while (kids.some(k => k.kind === 'folder' && k.name.toLowerCase() === f.toLowerCase())) f = name + ' (' + (i++) + ')';
    const n = { id: uid(), parentId: parentId || '', name: f, kind: 'folder', mtime: Date.now() }; await dput(n); return n;
  }
  async function driveAddFile(parentId, name, blob, mime, mtime) {
    const kids = await dchildren(parentId); const dot = name.lastIndexOf('.');
    const base = dot > 0 ? name.slice(0, dot) : name, ext = dot > 0 ? name.slice(dot) : ''; let f = name, i = 2;
    while (kids.some(k => k.kind === 'file' && k.name.toLowerCase() === f.toLowerCase())) f = base + ' (' + (i++) + ')' + ext;
    const n = { id: uid(), parentId: parentId || '', name: f, kind: 'file', size: blob.size, mime: mime || blob.type || '', mtime: mtime || Date.now(), content: blob }; await dput(n); return n;
  }
  async function driveEnsureFolderPath(parentId, segs) { let cur = parentId || ''; for (const s of segs) { if (!s) continue; const kids = await dchildren(cur); let f = kids.find(k => k.kind === 'folder' && k.name === s); if (!f) f = await driveCreateFolder(cur, s); cur = f.id; } return cur; }
  async function driveRemove(id) { const n = await dget(id); if (!n) return; if (n.kind === 'folder') { const kids = await dchildren(id); for (const k of kids) await driveRemove(k.id); } await ddel(id); }
  async function driveRename(id, name) { const n = await dget(id); if (!n) return; name = (name || '').trim(); if (!name) return; n.name = name; n.mtime = Date.now(); await dput(n); }
  async function driveUploadFiles(fileList) {
    let count = 0;
    for (const file of fileList) {
      const rel = file.webkitRelativePath || ''; let target = cwd;
      if (rel && rel.includes('/')) { const segs = rel.split('/'); segs.pop(); target = await driveEnsureFolderPath(cwd, segs); }
      await driveAddFile(target, file.name, file, file.type); count++;
    }
    if (count) toast('Uploaded ' + count + ' file' + (count === 1 ? '' : 's') + ' to Drive');
    renderDrive();
  }

  // ── State ─────────────────────────────────────────────────────────────────
  let cwd = '', searchQ = '', mapHandle = null, mapMeta = { lastSync: 0 }, syncing = false, built = false;
  let $bg, $modal, $crumbs, $body, $footL, $storage, $map, $syncBtn, $fileInput, $folderInput, $maxBtn, $toast, toastT;

  function toast(msg) { if (!$toast) return; $toast.textContent = msg; $toast.classList.add('show'); clearTimeout(toastT); toastT = setTimeout(() => $toast.classList.remove('show'), 1800); }

  // ── Styles ────────────────────────────────────────────────────────────────
  function injectStyles() {
    if (document.getElementById('cygdm-styles')) return;
    const st = document.createElement('style'); st.id = 'cygdm-styles';
    st.textContent = `
      .cygdm-bg{position:fixed;inset:0;background:var(--modal-scrim,rgba(15,18,26,.5));z-index:4000;display:none;align-items:center;justify-content:center;padding:1.5rem;font-family:var(--sans,'IBM Plex Sans','Helvetica Neue',Arial,sans-serif)}
      .cygdm-bg.open{display:flex}
      .cygdm-modal{background:var(--bg2,#fff);border:1px solid var(--border2,#dfe3ea);border-radius:12px;width:100%;max-width:820px;height:82vh;max-height:82vh;display:flex;flex-direction:column;box-shadow:var(--shadow-strong,0 24px 60px -12px rgba(20,24,40,.4));overflow:hidden}
      /* Maximized — fill the viewport */
      .cygdm-bg.max{padding:0}
      .cygdm-bg.max .cygdm-modal{max-width:none;width:100vw;height:100vh;border-radius:0}
      /* Minimized — dock to a small bar bottom-right; page behind stays usable */
      .cygdm-bg.min{background:transparent;pointer-events:none;align-items:flex-end;justify-content:flex-end;padding:16px}
      .cygdm-bg.min .cygdm-modal{pointer-events:auto;width:340px;max-width:82vw;height:auto;max-height:none}
      .cygdm-bg.min .cygdm-bar,.cygdm-bg.min .cygdm-map,.cygdm-bg.min .cygdm-body,.cygdm-bg.min .cygdm-foot{display:none!important}
      .cygdm-bg.min .cygdm-h{cursor:pointer}
      /* While dragging a file out: dim the scrim and window so the editor
         behind is easy to see. We deliberately DON'T set pointer-events:none
         on the overlay (that would break the native drag, since the dragged
         row lives inside it). The host page catches the drop at the document
         level instead, so a drop anywhere — even on this overlay — works. */
      .cygdm-bg.dragging{background:transparent}
      .cygdm-bg.dragging .cygdm-modal{opacity:.5;transition:opacity .12s}
      .cygdm-row[draggable="true"]{cursor:grab}

      .cygdm-h{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:.9rem 1.05rem;border-bottom:1px solid var(--border,#eceef2)}
      .cygdm-h b{font-size:14px;color:var(--text,#1a1d21);display:flex;align-items:center;gap:8px;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
      .cygdm-ctrls{display:flex;align-items:center;gap:2px;flex-shrink:0}
      .cygdm-ctrls button{background:none;border:none;color:var(--text3,#7a8090);font-size:15px;cursor:pointer;width:30px;height:30px;border-radius:7px;display:flex;align-items:center;justify-content:center;line-height:1;transition:background .12s,color .12s}
      .cygdm-ctrls button:hover{background:var(--hover-tint,rgba(0,0,0,.06));color:var(--text,#1a1d21)}

      .cygdm-btn{font:inherit;font-size:12px;font-weight:500;color:var(--text2,#2a2f38);background:var(--bg2,#fff);border:1px solid var(--border2,#dfe3ea);border-radius:7px;padding:5px 11px;cursor:pointer;transition:all .12s}
      .cygdm-btn:hover{color:var(--text,#1a1d21);border-color:var(--text3,#7a8090)}

      .cygdm-bar{display:flex;gap:.45rem;align-items:center;padding:.6rem .9rem;border-bottom:1px solid var(--border,#eceef2);flex-wrap:wrap}
      .cygdm-crumbs{display:flex;align-items:center;gap:5px;font-size:12.5px;color:var(--text2,#2a2f38);flex:1;min-width:130px;flex-wrap:wrap}
      .cygdm-crumbs a{color:var(--accent,#4a5bd6);cursor:pointer;text-decoration:none}
      .cygdm-crumbs a:hover{text-decoration:underline}
      .cygdm-crumbs .sep{color:var(--text3,#7a8090)}
      .cygdm-search{font:inherit;font-size:12.5px;color:var(--text,#1a1d21);background:var(--bg,#fff);border:1px solid var(--border2,#dfe3ea);border-radius:7px;padding:5px 9px;min-width:150px}
      .cygdm-search:focus{outline:none;border-color:var(--accent,#4a5bd6);box-shadow:0 0 0 3px var(--accent-glow,rgba(74,91,214,.18))}

      .cygdm-body{flex:1;overflow-y:auto;padding:.4rem;position:relative}
      .cygdm-body.drag{outline:2px dashed var(--accent,#4a5bd6);outline-offset:-8px;background:var(--accent-glow,rgba(74,91,214,.12))}
      .cygdm-row{display:flex;align-items:center;gap:.65rem;padding:.5rem .6rem;border-radius:8px;cursor:pointer}
      .cygdm-row:hover{background:var(--hover-tint,rgba(0,0,0,.05))}
      .cygdm-ic{width:28px;height:28px;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-size:15px;border-radius:6px;background:var(--bg3,#f4f5f8)}
      .cygdm-nm{flex:1;font-size:13px;color:var(--text,#1a1d21);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
      .cygdm-meta{font-size:11px;color:var(--text3,#7a8090);flex-shrink:0}
      .cygdm-acts{display:flex;gap:.15rem;flex-shrink:0;opacity:0;transition:opacity .1s}
      .cygdm-row:hover .cygdm-acts,.cygdm-row:focus-within .cygdm-acts{opacity:1}
      .cygdm-acts button{background:none;border:none;color:var(--text3,#7a8090);cursor:pointer;font-size:13px;padding:3px 5px;border-radius:5px;line-height:1}
      .cygdm-acts button:hover{color:var(--accent,#4a5bd6);background:var(--bg3,#f4f5f8)}
      .cygdm-empty{padding:2.2rem 1rem;text-align:center;color:var(--text3,#7a8090);font-size:12.5px;line-height:1.7}

      .cygdm-foot{border-top:1px solid var(--border,#eceef2);padding:6px .9rem;font-size:11px;color:var(--text3,#7a8090);display:flex;align-items:center;justify-content:space-between;gap:.5rem;flex-wrap:wrap}
      .cygdm-storage{display:inline-flex;align-items:center;gap:7px;font-size:11px;color:var(--text3,#7a8090);cursor:default}
      .cygdm-storage svg{flex-shrink:0}
      .cygdm-storage b{color:var(--text2,#2a2f38);font-weight:600}

      .cygdm-mapb{align-items:center;gap:.5rem;padding:.45rem .9rem;background:var(--accent-glow,rgba(74,91,214,.12));border-bottom:1px solid var(--border,#eceef2);font-size:12px;color:var(--text2,#2a2f38);flex-wrap:wrap}
      .cygdm-mapb .dm-ic{color:var(--accent,#4a5bd6);font-size:14px;flex-shrink:0}
      .cygdm-mapb .dm-txt{flex:1;min-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
      .cygdm-mapb .dm-txt b{color:var(--text,#1a1d21)}
      .cygdm-mapb .cygdm-btn{padding:3px 9px;font-size:11.5px}

      .cygdm-toast{position:fixed;bottom:20px;left:50%;transform:translateX(-50%) translateY(20px);background:var(--text,#1a1d21);color:var(--bg2,#fff);font-size:12.5px;padding:8px 16px;border-radius:8px;opacity:0;transition:all .25s;z-index:4100;pointer-events:none}
      .cygdm-toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
    `;
    document.head.appendChild(st);
  }

  // ── Build the modal DOM once ──────────────────────────────────────────────
  function build() {
    if (built) return;
    injectStyles();
    $bg = document.createElement('div'); $bg.className = 'cygdm-bg'; $bg.id = 'cygdm-bg';
    $bg.innerHTML = `
      <div class="cygdm-modal" role="dialog" aria-label="Co-Worker Drive">
        <div class="cygdm-h" id="cygdm-h">
          <b>🗂 Drive — Co-Worker workspace</b>
          <div class="cygdm-ctrls">
            <button id="cygdm-min" title="Minimize" aria-label="Minimize">—</button>
            <button id="cygdm-max" title="Maximize" aria-label="Maximize">⤢</button>
            <button id="cygdm-close" title="Close" aria-label="Close">✕</button>
          </div>
        </div>
        <div class="cygdm-bar">
          <div class="cygdm-crumbs" id="cygdm-crumbs"></div>
          <button class="cygdm-btn" id="cygdm-newfolder" title="Create a folder here">＋ Folder</button>
          <button class="cygdm-btn" id="cygdm-upfiles" title="Upload files here">⬆ Files</button>
          <button class="cygdm-btn" id="cygdm-upfolder" title="Upload a whole folder (keeps its structure)">⬆ Folder</button>
          <button class="cygdm-btn" id="cygdm-sync" title="Map &amp; sync the Drive with a folder on your computer">⇄ Sync folder</button>
          <input type="search" class="cygdm-search" id="cygdm-search" placeholder="Search drive…" spellcheck="false">
        </div>
        <div class="cygdm-mapb" id="cygdm-map" style="display:none"></div>
        <div class="cygdm-body" id="cygdm-body"></div>
        <div class="cygdm-foot">
          <span id="cygdm-footl"></span>
          <span class="cygdm-storage" id="cygdm-storage" title="Storage for this site in your browser (includes the Drive)"></span>
        </div>
        <input type="file" id="cygdm-fileinput" multiple style="display:none">
        <input type="file" id="cygdm-folderinput" webkitdirectory style="display:none">
      </div>`;
    document.body.appendChild($bg);
    $toast = document.createElement('div'); $toast.className = 'cygdm-toast'; document.body.appendChild($toast);

    $modal       = $bg.querySelector('.cygdm-modal');
    $crumbs      = $bg.querySelector('#cygdm-crumbs');
    $body        = $bg.querySelector('#cygdm-body');
    $footL       = $bg.querySelector('#cygdm-footl');
    $storage     = $bg.querySelector('#cygdm-storage');
    $map         = $bg.querySelector('#cygdm-map');
    $syncBtn     = $bg.querySelector('#cygdm-sync');
    $fileInput   = $bg.querySelector('#cygdm-fileinput');
    $folderInput = $bg.querySelector('#cygdm-folderinput');
    $maxBtn      = $bg.querySelector('#cygdm-max');

    // Scrim click closes (only on the un-minimized backdrop).
    $bg.addEventListener('click', e => { if (e.target === $bg && !$bg.classList.contains('min')) close(); });
    // Window controls
    $bg.querySelector('#cygdm-close').addEventListener('click', close);
    $bg.querySelector('#cygdm-min').addEventListener('click', toggleMin);
    $maxBtn.addEventListener('click', toggleMax);
    // Clicking the header while minimized restores it.
    $bg.querySelector('#cygdm-h').addEventListener('click', e => {
      if ($bg.classList.contains('min') && !e.target.closest('.cygdm-ctrls')) toggleMin();
    });

    // Toolbar
    $bg.querySelector('#cygdm-newfolder').addEventListener('click', async () => { const name = prompt('New folder name:', 'New folder'); if (name == null) return; await driveCreateFolder(cwd, name); renderDrive(); });
    $bg.querySelector('#cygdm-upfiles').addEventListener('click', () => $fileInput.click());
    $bg.querySelector('#cygdm-upfolder').addEventListener('click', () => $folderInput.click());
    $syncBtn.addEventListener('click', syncButton);
    $bg.querySelector('#cygdm-search').addEventListener('input', e => { searchQ = (e.target.value || '').trim().toLowerCase(); renderDrive(); });
    $fileInput.addEventListener('change', async e => { if (e.target.files && e.target.files.length) await driveUploadFiles(e.target.files); e.target.value = ''; });
    $folderInput.addEventListener('change', async e => { if (e.target.files && e.target.files.length) await driveUploadFiles(e.target.files); e.target.value = ''; });

    // Crumb navigation (delegated)
    $crumbs.addEventListener('click', e => { const a = e.target.closest('a[data-id]'); if (!a) return; navigate(a.getAttribute('data-id')); });

    // Drag-and-drop uploads
    ['dragenter', 'dragover'].forEach(ev => $body.addEventListener(ev, e => { e.preventDefault(); $body.classList.add('drag'); }));
    $body.addEventListener('dragleave', e => { if (e.target === $body) $body.classList.remove('drag'); });
    $body.addEventListener('drop', async e => { e.preventDefault(); $body.classList.remove('drag'); const f = e.dataTransfer && e.dataTransfer.files; if (f && f.length) await driveUploadFiles(f); });

    document.addEventListener('keydown', e => { if (e.key === 'Escape' && $bg.classList.contains('open') && !$bg.classList.contains('min')) close(); });

    built = true;
  }

  // ── Window-control state ──────────────────────────────────────────────────
  function toggleMax() {
    const on = !$bg.classList.contains('max');
    $bg.classList.toggle('max', on); $bg.classList.remove('min');
    $maxBtn.textContent = on ? '⤡' : '⤢';
    $maxBtn.title = on ? 'Restore' : 'Maximize';
  }
  function toggleMin() {
    const on = !$bg.classList.contains('min');
    $bg.classList.toggle('min', on); $bg.classList.remove('max');
    $maxBtn.textContent = '⤢'; $maxBtn.title = 'Maximize';
  }

  // ── Rendering ─────────────────────────────────────────────────────────────
  function navigate(id) { cwd = id || ''; searchQ = ''; const s = $bg.querySelector('#cygdm-search'); if (s) s.value = ''; renderDrive(); }

  function rowEl(n) {
    const row = document.createElement('div'); row.className = 'cygdm-row';
    const isFolder = n.kind === 'folder';
    const ic = document.createElement('div'); ic.className = 'cygdm-ic'; ic.textContent = isFolder ? '📁' : fileIcon(n.name);
    const nm = document.createElement('div'); nm.className = 'cygdm-nm'; nm.textContent = n.name;
    const meta = document.createElement('div'); meta.className = 'cygdm-meta'; meta.textContent = isFolder ? 'folder' : fmtSize(n.size);
    const acts = document.createElement('div'); acts.className = 'cygdm-acts';
    if (isFolder) {
      row.onclick = e => { if (e.target.closest('.cygdm-acts')) return; navigate(n.id); };
    } else {
      row.onclick = e => { if (e.target.closest('.cygdm-acts')) return; downloadFile(n); };
      const dl = document.createElement('button'); dl.title = 'Download'; dl.textContent = '⭳'; dl.onclick = e => { e.stopPropagation(); downloadFile(n); }; acts.appendChild(dl);
      // Draggable so the file can be dropped straight into a host editor (e.g.
      // the SQL Editor). The overlay goes click-through during the drag so the
      // drop lands on the page behind it.
      row.draggable = true;
      row.title = 'Drag into the editor to open';
      row.addEventListener('dragstart', e => {
        try {
          e.dataTransfer.setData('application/x-cygenix-drive-file', JSON.stringify({ id: n.id, name: n.name }));
          e.dataTransfer.setData('text/plain', n.name);
          e.dataTransfer.effectAllowed = 'copy';
        } catch (_) {}
        if ($bg) $bg.classList.add('dragging');
      });
      row.addEventListener('dragend', () => { if ($bg) $bg.classList.remove('dragging'); });
    }
    const rn = document.createElement('button'); rn.title = 'Rename'; rn.textContent = '✎';
    rn.onclick = async e => { e.stopPropagation(); const nn = prompt('Rename to:', n.name); if (nn == null) return; await driveRename(n.id, nn); renderDrive(); }; acts.appendChild(rn);
    const del = document.createElement('button'); del.title = 'Delete'; del.textContent = '🗑';
    del.onclick = async e => { e.stopPropagation(); if (!confirm('Delete “' + n.name + '”' + (isFolder ? ' and everything inside it' : '') + '?')) return; await driveRemove(n.id); if (cwd === n.id) cwd = n.parentId || ''; renderDrive(); }; acts.appendChild(del);
    row.appendChild(ic); row.appendChild(nm); row.appendChild(meta); row.appendChild(acts);
    return row;
  }

  async function renderDrive() {
    if (!$body) return;
    const path = cwd ? await driveFolderPath(cwd) : [];
    let cr = '<a data-id="">🗂 Home</a>';
    for (const f of path) cr += '<span class="sep">/</span><a data-id="' + esc(f.id) + '">' + esc(f.name) + '</a>';
    if (searchQ) cr += '<span class="sep">/</span><span style="color:var(--text3)">search “' + esc(searchQ) + '”</span>';
    $crumbs.innerHTML = cr;

    let items;
    if (searchQ) items = (await dall()).filter(n => n.name.toLowerCase().includes(searchQ));
    else items = await dchildren(cwd);
    items.sort((a, b) => a.kind === b.kind ? a.name.localeCompare(b.name) : (a.kind === 'folder' ? -1 : 1));

    if (!items.length) {
      $body.innerHTML = '<div class="cygdm-empty">' + (searchQ
        ? 'No files or folders match “' + esc(searchQ) + '”.'
        : 'This folder is empty.<br>Drag files in, or use <b>⬆ Files</b> / <b>⬆ Folder</b> / <b>＋ Folder</b>.<br><br>Anything you put here becomes the co-worker\'s workspace — it can read and build on these files.') + '</div>';
    } else {
      $body.innerHTML = ''; items.forEach(n => $body.appendChild(rowEl(n)));
    }
    if ($footL) $footL.textContent = items.length + ' item' + (items.length === 1 ? '' : 's');
    renderStorage();
  }

  function downloadFile(n) { const a = document.createElement('a'); a.href = URL.createObjectURL(n.content); a.download = n.name; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 1000); }

  // ── Storage gauge ─────────────────────────────────────────────────────────
  function paintStorage(el, usage, quota, driveBytes) {
    const total = (quota != null && quota > 0) ? quota : null;
    const used  = usage != null ? usage : (driveBytes || 0);
    const pct   = total ? Math.min(100, Math.max(0, used / total * 100)) : ((driveBytes || 0) > 0 ? 3 : 0);
    const C = 2 * Math.PI * 8, off = C * (1 - pct / 100);
    const col = pct > 90 ? 'var(--red,#f04646)' : (pct > 75 ? 'var(--amber,#f59e0b)' : 'var(--accent,#4a5bd6)');
    const ring = '<svg width="20" height="20" viewBox="0 0 20 20" aria-hidden="true">' +
      '<circle cx="10" cy="10" r="8" fill="none" stroke="var(--border2,#dfe3ea)" stroke-width="3"/>' +
      '<circle cx="10" cy="10" r="8" fill="none" stroke="' + col + '" stroke-width="3" stroke-linecap="round" stroke-dasharray="' + C.toFixed(2) + '" stroke-dashoffset="' + off.toFixed(2) + '" transform="rotate(-90 10 10)"/></svg>';
    let txt;
    if (total != null) {
      txt = '<b>' + fmtSize(used) + '</b> used · ' + fmtSize(Math.max(0, total - used)) + ' free';
      el.title = 'Browser storage for this site (includes the Drive) — ' + fmtSize(used) + ' of ' + fmtSize(total) + ' used (' + Math.round(pct) + '%).' + (driveBytes != null ? (' Drive files: ' + fmtSize(driveBytes) + '.') : '');
    } else {
      txt = '<b>' + fmtSize(driveBytes || 0) + '</b> in Drive';
      el.title = 'Drive size. Your browser doesn’t report an overall storage quota.';
    }
    el.innerHTML = ring + '<span>' + txt + '</span>';
  }
  async function renderStorage() {
    if (!$storage) return;
    let usage = null, quota = null;
    try { if (navigator.storage && navigator.storage.estimate) { const e = await navigator.storage.estimate(); usage = e.usage; quota = e.quota; } } catch (_) {}
    paintStorage($storage, usage, quota, null);
    try { const b = (await dall()).reduce((s, n) => s + (n.kind === 'file' ? (n.size || 0) : 0), 0); paintStorage($storage, usage, quota, b); } catch (_) {}
  }

  // ── Map & sync with a local folder (File System Access API) ───────────────
  function renderMapBanner() {
    if (!$map) return;
    if (!mapHandle) { $map.style.display = 'none'; $map.innerHTML = ''; if ($syncBtn) $syncBtn.textContent = '⇄ Sync folder'; return; }
    if ($syncBtn) $syncBtn.textContent = '⇄ Sync now';
    const when = mapMeta.lastSync ? new Date(mapMeta.lastSync).toLocaleString() : 'not yet';
    $map.style.display = 'flex';
    $map.innerHTML =
      '<span class="dm-ic">⇄</span>' +
      '<span class="dm-txt">Synced with <b>' + esc(mapHandle.name) + '</b> · last sync ' + esc(when) + '</span>' +
      '<button class="cygdm-btn" id="cygdm-syncnow">Sync now</button>' +
      '<button class="cygdm-btn" id="cygdm-unmap">Unmap</button>';
    $map.querySelector('#cygdm-syncnow').addEventListener('click', syncNow);
    $map.querySelector('#cygdm-unmap').addEventListener('click', unmap);
  }
  async function syncButton() {
    if (!fsSupported()) { toast('Folder sync needs Chrome or Edge'); return; }
    if (!mapHandle) { await mapFolder(); return; }
    await syncNow();
  }
  async function mapFolder() {
    if (!fsSupported()) { toast('Folder sync needs a Chromium browser (Chrome or Edge)'); return; }
    try {
      const h = await window.showDirectoryPicker({ mode: 'readwrite', id: 'cygenix-drive-sync' });
      mapHandle = h; try { await idbSet('driveMapDir', h); } catch (_) {}
      renderMapBanner(); toast('Mapped to ' + h.name);
      if (confirm('Sync now?\n\nThis merges files both ways between the Drive and “' + h.name + '” — newest version wins. Nothing is deleted on either side.')) await syncNow();
    } catch (e) { if (e && e.name !== 'AbortError') toast('Could not open that folder'); }
  }
  async function unmap() {
    mapHandle = null; mapMeta = { lastSync: 0 };
    try { await idbDel('driveMapDir'); await idbDel('driveMapMeta'); } catch (_) {}
    renderMapBanner(); toast('Folder unmapped (files kept on both sides)');
  }
  async function walkDisk(dirHandle, prefix, out) {
    for await (const [name, handle] of dirHandle.entries()) {
      const path = (prefix ? prefix + '/' : '') + name;
      if (handle.kind === 'directory') { out.folders.push(path); await walkDisk(handle, path, out); }
      else { let f; try { f = await handle.getFile(); } catch (_) { continue; } out.files.push({ path, name, handle, lastModified: f.lastModified || 0, file: f }); }
    }
  }
  async function ensureDiskDir(root, segs) { let cur = root; for (const s of segs) { if (!s) continue; cur = await cur.getDirectoryHandle(s, { create: true }); } return cur; }
  async function buildIndex() {
    const all = await dall(); const byId = {}; all.forEach(n => byId[n.id] = n);
    const pathOf = n => { const parts = [n.name]; let p = n.parentId, g = 0; while (p && byId[p] && g++ < 50) { parts.unshift(byId[p].name); p = byId[p].parentId; } return parts.join('/'); };
    const filesByPath = new Map(); all.forEach(n => { if (n.kind === 'file') filesByPath.set(pathOf(n), n); });
    return { filesByPath };
  }
  async function syncNow() {
    if (!mapHandle) { toast('No folder mapped'); return; }
    if (syncing) return;
    if (!(await ensurePerm(mapHandle))) { toast('Folder permission needed'); return; }
    syncing = true; toast('Syncing…');
    try {
      const disk = { folders: [], files: [] }; await walkDisk(mapHandle, '', disk);
      const diskByPath = new Map(); disk.files.forEach(f => diskByPath.set(f.path, f));
      let idx = await buildIndex();
      let pulled = 0, pushed = 0, updated = 0; const TOL = 1500;
      for (const df of disk.files) {
        const dn = idx.filesByPath.get(df.path);
        if (!dn) { const segs = df.path.split('/'); const fname = segs.pop(); const folderId = segs.length ? await driveEnsureFolderPath('', segs) : ''; await driveAddFile(folderId, fname, df.file, df.file.type, df.lastModified); pulled++; }
        else if (df.lastModified > (dn.mtime || 0) + TOL) { dn.content = df.file; dn.size = df.file.size; dn.mime = df.file.type || dn.mime; dn.mtime = df.lastModified; await dput(dn); updated++; }
      }
      idx = await buildIndex();
      for (const [path, dn] of idx.filesByPath) {
        const df = diskByPath.get(path); const driveM = dn.mtime || 0;
        if (!df || driveM > (df.lastModified || 0) + TOL) {
          const segs = path.split('/'); const fname = segs.pop();
          const dir = segs.length ? await ensureDiskDir(mapHandle, segs) : mapHandle;
          const fh = await dir.getFileHandle(fname, { create: true });
          const w = await fh.createWritable(); await w.write(dn.content); await w.close();
          try { const nf = await fh.getFile(); dn.mtime = nf.lastModified; await dput(dn); } catch (_) {}
          pushed++;
        }
      }
      mapMeta = { lastSync: Date.now() }; try { await idbSet('driveMapMeta', mapMeta); } catch (_) {}
      renderMapBanner(); renderDrive();
      toast('Synced · ' + pulled + ' in, ' + pushed + ' out' + (updated ? (', ' + updated + ' updated') : ''));
    } catch (e) { toast('Sync failed: ' + ((e && e.message) || 'error')); }
    finally { syncing = false; }
  }
  async function restoreMap() {
    try {
      if (!fsSupported()) return;
      const h = await idbGet('driveMapDir'); if (!h) return;
      mapHandle = h; const m = await idbGet('driveMapMeta'); if (m) mapMeta = m;
      renderMapBanner();
    } catch (_) {}
  }

  // ── Public API ────────────────────────────────────────────────────────────
  function open() {
    build();
    searchQ = ''; const s = $bg.querySelector('#cygdm-search'); if (s) s.value = '';
    $bg.classList.remove('min'); // always come back into view
    renderDrive(); renderStorage(); renderMapBanner();
    $bg.classList.add('open');
    restoreMap();
  }
  function close() { if ($bg) { $bg.classList.remove('open', 'min', 'max'); if ($maxBtn) { $maxBtn.textContent = '⤢'; $maxBtn.title = 'Maximize'; } } }

  window.CygenixDriveModal = { open, close, isOpen: () => !!($bg && $bg.classList.contains('open')) };
})();
