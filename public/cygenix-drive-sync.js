/* cygenix-drive-sync.js — makes the Co-Worker Drive follow you across machines.
 *
 * THE PROBLEM THIS SOLVES
 * The Drive stores its files in IndexedDB ("cygenix_coworker_drive"), which is
 * per-browser and per-machine. Anything uploaded on one computer was invisible
 * on every other one. This module keeps that local Drive in step with a
 * private per-user cloud store (/.netlify/functions/drive), so the same files
 * appear wherever you sign in.
 *
 * WHY IT'S A SEPARATE FILE
 * Three places implement Drive UI (cygenix-drive.js, cygenix-drive-modal.js and
 * coworker.html's inline copy) but all three read and write the SAME IndexedDB
 * database and node shape. Syncing at the database level therefore covers all
 * of them at once, with no refactor of the duplicated logic.
 *
 * HOW THE MERGE WORKS (three-way, not last-write-wins)
 * Each sync compares three states: LOCAL (IndexedDB), REMOTE (cloud manifest)
 * and BASE (what we saw at the end of the previous sync, in localStorage).
 * BASE is what makes deletes work: a file missing locally but present in BASE
 * was deleted here (so delete it in the cloud), while a file missing remotely
 * but present in BASE was deleted on another machine (so delete it here).
 * Without BASE we couldn't tell a deletion from a file that simply hadn't
 * arrived yet, and deleted files would resurrect on every sync.
 * When a node exists on both sides, the newer mtime wins.
 *
 * NOT SYNCED, BY DESIGN
 *  - The "Sync folder" local-folder mapping. That's a File System Access API
 *    handle to a folder on one physical disk; it cannot be transferred to
 *    another machine, and the browser deliberately doesn't expose the path.
 *    It stays a per-machine convenience.
 *  - Files larger than the cloud limit (see maxContentBytes, 4 MB — Netlify
 *    caps function payloads at 6 MB and base64 adds ~33%). These remain
 *    local-only and are reported in the sync result rather than failing
 *    silently. Bigger files need a chunked upload path.
 *
 * Exposes window.CygenixDriveSync = { sync, status, onChange, lastResult }.
 * Emits a 'cygenix:drive-sync' CustomEvent on window whenever state changes,
 * so any Drive UI can re-render itself.
 */
(function () {
  'use strict';
  if (window.CygenixDriveSync) return;

  const API        = '/.netlify/functions/drive';
  const DRIVE_DB   = 'cygenix_coworker_drive';
  const STORE      = 'nodes';
  const BASE_KEY   = 'cygenix_drive_sync_base';
  const POLL_MS    = 45000;   // re-check for local changes while the tab is visible
  const MIN_GAP_MS = 5000;    // never run two syncs closer together than this

  let _syncing = null;        // in-flight promise (de-dupes concurrent calls)
  let _lastRunAt = 0;
  let _status = { state: 'idle', message: '', at: null };
  let _lastResult = null;

  // ── IndexedDB helpers (same DB/store the Drive UIs use) ───────────────────
  function ddb() {
    return new Promise((res, rej) => {
      const r = indexedDB.open(DRIVE_DB, 1);
      r.onupgradeneeded = () => {
        const db = r.result;
        if (!db.objectStoreNames.contains(STORE)) {
          const s = db.createObjectStore(STORE, { keyPath: 'id' });
          s.createIndex('parentId', 'parentId', { unique: false });
        }
      };
      r.onsuccess = () => res(r.result);
      r.onerror   = () => rej(r.error);
    });
  }
  const idbAll = () => ddb().then(db => new Promise((res, rej) => {
    const rq = db.transaction(STORE, 'readonly').objectStore(STORE).getAll();
    rq.onsuccess = () => res(rq.result || []); rq.onerror = () => rej(rq.error);
  }));
  const idbPut = (n) => ddb().then(db => new Promise((res, rej) => {
    const t = db.transaction(STORE, 'readwrite'); t.objectStore(STORE).put(n);
    t.oncomplete = () => res(n); t.onerror = () => rej(t.error);
  }));
  const idbDel = (id) => ddb().then(db => new Promise((res, rej) => {
    const t = db.transaction(STORE, 'readwrite'); t.objectStore(STORE).delete(id);
    t.oncomplete = () => res(); t.onerror = () => rej(t.error);
  }));

  // ── Base64 <-> Blob, chunked so large files don't blow the call stack ─────
  function blobToB64(blob) {
    return blob.arrayBuffer().then(buf => {
      const bytes = new Uint8Array(buf);
      let bin = '';
      const CHUNK = 0x8000;
      for (let i = 0; i < bytes.length; i += CHUNK) {
        bin += String.fromCharCode.apply(null, bytes.subarray(i, i + CHUNK));
      }
      return btoa(bin);
    });
  }
  function b64ToBlob(b64, mime) {
    const bin = atob(b64 || '');
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    return new Blob([bytes], { type: mime || '' });
  }

  // ── Identity + auth ───────────────────────────────────────────────────────
  function idToken() {
    try { return (typeof window.getCygenixIdToken === 'function') ? window.getCygenixIdToken() : ''; }
    catch { return ''; }
  }
  // Identifies whose baseline is stored locally, so switching accounts on a
  // shared machine can't merge one user's Drive into another's.
  function currentUserTag() {
    try {
      const raw = sessionStorage.getItem('cygenix_entra_account')
               || localStorage.getItem('cygenix_entra_account');
      if (raw) {
        const a = JSON.parse(raw);
        return String(a.email || a.userId || '').toLowerCase().trim();
      }
    } catch {}
    return '';
  }

  async function api(action, payload) {
    const token = idToken();
    if (!token) throw new Error('not-signed-in');
    const r = await fetch(API, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
      body: JSON.stringify({ action, ...(payload || {}) }),
      signal: AbortSignal.timeout(60000),
    });
    const data = await r.json().catch(() => ({ error: 'Non-JSON response (' + r.status + ')' }));
    if (!r.ok) throw new Error(data.error || ('HTTP ' + r.status));
    return data;
  }

  // ── Baseline (what both sides agreed on at the end of the last sync) ──────
  function loadBase() {
    try {
      const raw = JSON.parse(localStorage.getItem(BASE_KEY) || 'null');
      if (raw && raw.user === currentUserTag() && raw.ids && typeof raw.ids === 'object') return raw.ids;
    } catch {}
    return {};   // unknown baseline: treat everything as "new", nothing as deleted
  }
  function saveBase(ids) {
    try {
      localStorage.setItem(BASE_KEY, JSON.stringify({ user: currentUserTag(), ids, at: Date.now() }));
    } catch {}
  }

  function setStatus(state, message) {
    _status = { state, message: message || '', at: Date.now() };
    try {
      window.dispatchEvent(new CustomEvent('cygenix:drive-sync', {
        detail: { status: _status, result: _lastResult }
      }));
    } catch {}
  }

  // Metadata only — never send Blob content in the manifest payload.
  function metaOf(n) {
    const m = {
      id: n.id, parentId: n.parentId || '', name: n.name, kind: n.kind,
      size: n.size || 0, mime: n.mime || '', mtime: n.mtime || 0,
    };
    if (n.meta) m.meta = n.meta;
    return m;
  }

  // ── The three-way merge decision table ────────────────────────────────────
  // Pure function of (local, remote, base) so it can be reasoned about and
  // tested on its own — this is where a mistake would silently delete a
  // user's files or resurrect ones they deleted.
  //
  //   on both sides      → newer mtime wins
  //   local only,  in base → another machine deleted it   → delete here
  //   local only, not base → created here                 → upload
  //   remote only,  in base → deleted here                → delete remotely
  //   remote only, not base → created elsewhere           → download
  function planSync(local, remote, base) {
    const ids = new Set([
      ...Object.keys(local || {}),
      ...Object.keys(remote || {}),
      ...Object.keys(base || {}),
    ]);
    const toUpload = [], toDownload = [], toDeleteLocal = [], toDeleteRemote = [];
    for (const id of ids) {
      const L = local[id], R = remote[id];
      const inBase = Object.prototype.hasOwnProperty.call(base || {}, id);
      if (L && R) {
        const lm = L.mtime || 0, rm = R.mtime || 0;
        if (lm > rm) toUpload.push(L);
        else if (rm > lm) toDownload.push(R);
      } else if (L && !R) {
        if (inBase) toDeleteLocal.push(id);
        else        toUpload.push(L);
      } else if (!L && R) {
        if (inBase) toDeleteRemote.push(id);
        else        toDownload.push(R);
      }
      // neither side has it: it only lingers in the baseline and is dropped
      // when the new baseline is written from post-sync local state.
    }
    return { toUpload, toDownload, toDeleteLocal, toDeleteRemote };
  }

  // ── The sync itself ───────────────────────────────────────────────────────
  async function doSync(opts) {
    opts = opts || {};
    if (!idToken()) { setStatus('signed-out', 'Sign in to sync your Drive'); return { skipped: 'not-signed-in' }; }

    setStatus('syncing', 'Syncing Drive…');

    const remoteRes = await api('manifest');
    const remoteNodes = (remoteRes.manifest && remoteRes.manifest.nodes) || {};
    const maxBytes = remoteRes.maxContentBytes || (4 * 1024 * 1024);

    const localArr = await idbAll();
    const local = {};
    localArr.forEach(n => { if (n && n.id) local[n.id] = n; });
    let base = loadBase();

    // Safety valve. A wholly empty cloud manifest combined with a non-empty
    // baseline would instruct us to delete every local file — the shape a
    // backend fault or a wiped store produces, and indistinguishable from a
    // genuine "user deleted everything elsewhere". Local data is the harder
    // thing to recover, so treat it as authoritative: re-upload instead of
    // deleting, and let the user's next explicit delete propagate normally.
    const baseCount = Object.keys(base).length;
    if (baseCount && !Object.keys(remoteNodes).length && localArr.length) {
      console.warn('[drive-sync] cloud manifest is empty but a baseline exists — re-uploading local Drive instead of deleting it');
      saveBase({});                      // forget the baseline: nothing is "known deleted"
      base = {};
    }

    const { toUpload, toDownload, toDeleteLocal, toDeleteRemote } = planSync(local, remoteNodes, base);

    const skippedLarge = [];
    let uploaded = 0, downloaded = 0;

    // 1. Push deletions first so a delete+recreate of the same name can't race.
    if (toDeleteRemote.length) {
      await api('delete', { ids: toDeleteRemote });
    }
    for (const id of toDeleteLocal) {
      try { await idbDel(id); } catch {}
    }

    // 2. Upload local additions/edits. Content first, then metadata — a
    //    manifest entry with no content behind it would look like a broken
    //    file to every other machine.
    const metaBatch = [];
    for (const n of toUpload) {
      if (n.kind === 'file') {
        const size = (n.content && n.content.size) || n.size || 0;
        if (size > maxBytes) { skippedLarge.push(n.name); continue; }
        if (n.content) {
          try {
            const b64 = await blobToB64(n.content);
            await api('put-content', { id: n.id, contentB64: b64, mime: n.mime || n.content.type || '' });
          } catch (e) {
            if (String(e.message).indexOf('cloud-sync limit') !== -1) { skippedLarge.push(n.name); continue; }
            throw e;
          }
        }
      }
      metaBatch.push(metaOf(n));
      uploaded++;
    }
    // Chunk the manifest write so a very large Drive can't exceed the payload cap.
    for (let i = 0; i < metaBatch.length; i += 500) {
      await api('put-meta', { nodes: metaBatch.slice(i, i + 500) });
    }

    // 3. Pull remote additions/edits into IndexedDB.
    for (const R of toDownload) {
      const node = {
        id: R.id, parentId: R.parentId || '', name: R.name, kind: R.kind,
        size: R.size || 0, mime: R.mime || '', mtime: R.mtime || Date.now(),
      };
      if (R.meta) node.meta = R.meta;
      if (R.kind === 'file') {
        try {
          const c = await api('get-content', { id: R.id });
          node.content = b64ToBlob(c.contentB64, c.mime || R.mime);
          node.size = node.content.size;
        } catch (e) {
          // Content missing/unreadable — skip rather than create a file that
          // opens empty and looks like data loss.
          console.warn('[drive-sync] skipping', R.name, '-', e.message);
          continue;
        }
      }
      await idbPut(node);
      downloaded++;
    }

    // 4. New baseline = whatever both sides now agree on.
    const after = await idbAll();
    const newBase = {};
    after.forEach(n => { if (n && n.id) newBase[n.id] = n.mtime || 0; });
    saveBase(newBase);

    _lastResult = {
      uploaded, downloaded,
      deletedLocal: toDeleteLocal.length,
      deletedRemote: toDeleteRemote.length,
      skippedLarge,
      at: Date.now(),
    };

    const changed = uploaded + downloaded + toDeleteLocal.length + toDeleteRemote.length;
    let msg = changed ? `Drive synced — ${uploaded} up, ${downloaded} down` : 'Drive up to date';
    if (skippedLarge.length) {
      msg += ` · ${skippedLarge.length} file(s) too large to sync (over ${Math.round(maxBytes / 1048576)} MB)`;
    }
    setStatus('idle', msg);

    // Let any open Drive UI refresh itself when content actually moved.
    if (changed) {
      try { window.dispatchEvent(new CustomEvent('cygenix:drive-changed', { detail: _lastResult })); } catch {}
    }
    return _lastResult;
  }

  function sync(opts) {
    opts = opts || {};
    if (_syncing) return _syncing;
    if (!opts.force && (Date.now() - _lastRunAt) < MIN_GAP_MS) {
      return Promise.resolve(_lastResult || { skipped: 'throttled' });
    }
    _lastRunAt = Date.now();
    _syncing = doSync(opts)
      .catch(e => {
        if (String(e.message) === 'not-signed-in') {
          setStatus('signed-out', 'Sign in to sync your Drive');
          return { skipped: 'not-signed-in' };
        }
        console.warn('[drive-sync] failed:', e.message);
        setStatus('error', 'Drive sync failed: ' + e.message);
        return { error: e.message };
      })
      .finally(() => { _syncing = null; });
    return _syncing;
  }

  // ── Triggers ──────────────────────────────────────────────────────────────
  // Local edits go through three different UI code paths, so rather than
  // hooking each one we compare a cheap signature of the local node set
  // against the baseline and sync when it has moved. (getAll() returns Blob
  // references, not their bytes, so this stays inexpensive.)
  async function localChangedSinceBase() {
    try {
      const base = loadBase();
      const nodes = await idbAll();
      if (nodes.length !== Object.keys(base).length) return true;
      for (const n of nodes) {
        if (base[n.id] === undefined || base[n.id] !== (n.mtime || 0)) return true;
      }
      return false;
    } catch { return false; }
  }

  async function maybeSync() {
    if (document.hidden || !idToken()) return;
    if (await localChangedSinceBase()) sync();
  }

  // First sync shortly after load — after auth has settled, and late enough
  // not to compete with first paint.
  setTimeout(() => { if (idToken()) sync({ force: true }); }, 2500);

  // Pick up other machines' changes when the user returns to the tab, and
  // push anything edited here.
  setInterval(maybeSync, POLL_MS);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) maybeSync(); });

  // Best-effort flush of pending local edits when leaving the page.
  window.addEventListener('pagehide', () => {
    if (!_syncing && idToken()) { try { sync(); } catch {} }
  });

  window.CygenixDriveSync = {
    sync,
    status: () => _status,
    get lastResult() { return _lastResult; },
    onChange: (fn) => window.addEventListener('cygenix:drive-sync', fn),
    _plan: planSync,   // exposed for tests
  };
})();
