// dashboard-app.js — the page logic of dashboard.html, extracted verbatim from its inline
// <script> blocks (in original order) so the browser can cache and
// bytecode-cache it across navigations. Inline scripts are parser-
// blocking and re-parsed from scratch on every reload; as an external
// deferred file this runs identically (all blocks already executed
// against the full DOM semantics they observe today, and defer keeps
// document order) but is fetched once and code-cached thereafter.
// The shared libs (connections.js, cygenix-cosmos-sync.js, …) are
// earlier in <head> with defer, so they run first.

/* ═══ block 1 — extracted from an inline <script> in dashboard.html ═══ */

    // Rename the "Inventory" sidebar item to "Project Artifacts" without
    // forking the shared /cygenix-sidebar.js file. The sidebar renders
    // synchronously on script load; we run immediately after its <script>
    // tag has parsed so its DOM is already in #cyg-sidebar-mount. A
    // MutationObserver catches any later re-renders (the shared component
    // sometimes re-mounts on nav changes).
    //
    // Match strategy: look for any element whose onclick or data-view
    // routes to 'inventory', or whose text content is exactly "Inventory".
    // Don't touch the icon — just the visible label text node.
    (function renameInventoryMenuItem() {
      const NEW_LABEL = 'Project Artifacts';
      // Match "Inventory" as a whole word — not "InventoryFoo" or "ReInventory".
      // Case-sensitive because the existing label is capitalised; picks up
      // "Inventory", "Inventory items", "📦 Inventory", but not partial words.
      const RX = /\bInventory\b/g;

      function rename(root) {
        if (!root) return;
        const candidates = root.querySelectorAll(
          '[data-view="inventory"], [onclick*="inventory"], a[href*="inventory"]'
        );
        candidates.forEach(el => {
          // Walk all text nodes inside the element and rewrite any containing
          // the word "Inventory". Using a word-boundary regex means we catch
          // "📦 Inventory" (icon + label in same text node) without touching
          // unrelated words.
          const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT, null);
          let n;
          while ((n = walker.nextNode())) {
            if (RX.test(n.nodeValue || '')) {
              n.nodeValue = n.nodeValue.replace(RX, NEW_LABEL);
              RX.lastIndex = 0; // reset — /g keeps state across test()/replace()
            }
          }
          // Attribute-based labels (aria, title) also get rewritten
          const aria = el.getAttribute && el.getAttribute('aria-label');
          if (aria && RX.test(aria)) {
            el.setAttribute('aria-label', aria.replace(RX, NEW_LABEL));
            RX.lastIndex = 0;
          }
          if (el.title && RX.test(el.title)) {
            el.title = el.title.replace(RX, NEW_LABEL);
            RX.lastIndex = 0;
          }
        });
      }

      const mount = document.getElementById('cyg-sidebar-mount');
      rename(mount);
      // Observer catches re-renders. Debounce by one tick so several DOM
      // mutations from a single re-mount only trigger one pass.
      if (mount && typeof MutationObserver === 'function') {
        let pending = false;
        const obs = new MutationObserver(() => {
          if (pending) return;
          pending = true;
          setTimeout(() => { pending = false; rename(mount); }, 0);
        });
        obs.observe(mount, { childList: true, subtree: true, characterData: true });
      }
    })();

  

/* ═══ block 2 — extracted from an inline <script> in dashboard.html ═══ */

// ── Config ────────────────────────────────────────────────────────────────────
const HEALTH_URL = '/.netlify/functions/health';
const BATCH_SIZE = 100; // rows per INSERT batch

function getApiKey() { return sessionStorage.getItem('cygenix_api_key') || ''; }
function setApiKey(k) { sessionStorage.setItem('cygenix_api_key', k.trim()); }

// Load persisted jobs from localStorage on startup. Also runs a one-shot
// migration on legacy jobs: any job without `version` or `lastModified`
// fields gets sensible defaults seeded in. This way the version chip in
// the table renders for ALL rows from day one, not just new saves.
//
// IMPORTANT: this migration is read-only on first call — we don't write
// back to localStorage here because that would trigger CygenixSync to
// push to Cosmos, and we don't want to pollute every key with a metadata
// "save". The fields appear on the in-memory copy. The next real save
// (via safeMutateJob etc.) will persist the seeded defaults naturally.
let _legacyMigrationRun = false;
function loadPersistedJobs() {
  try {
    const raw = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]');
    if (!_legacyMigrationRun){
      let mutated = false;
      for (const j of raw){
        if (!j) continue;
        if (typeof j.version !== 'number'){ j.version = 1; mutated = true; }
        if (!j.lastModified){ j.lastModified = j.created || new Date().toISOString(); mutated = true; }
      }
      // Write the migrated shape back so subsequent loads don't re-migrate.
      // This is the only place we write a wholesale cygenix_jobs that didn't
      // originate from an explicit user action, and it runs only once per
      // page load. If mutated is false (nothing to upgrade), we don't write
      // — keeps the writes minimal so we don't trigger spurious Cosmos pushes.
      if (mutated){
        try { localStorage.setItem('cygenix_jobs', JSON.stringify(raw)); } catch {}
      }
      _legacyMigrationRun = true;
    }
    return raw;
  } catch { return []; }
}

// ── Suspicious-shrink guard ────────────────────────────────────────────────
//
// Defensive observer for the cygenix_jobs key. If anything writes a smaller
// (non-empty → non-zero shrink, OR active count drops) job list compared to
// what's currently in localStorage, log a console warning with the stack so
// future repeats of the May-2026 data loss incident are debuggable.
//
// We DON'T block the write — too brittle, and the user has legitimate
// pathways to shrink (hard-delete from Trash). We just leave a trail.
//
// Soft-delete is the structural defence; this is the audit trail.
(function installShrinkGuard(){
  if (typeof Storage === 'undefined' || !window.localStorage) return;
  const orig = Storage.prototype.setItem;
  // Only install once even on hot-reload
  if (orig._cygenixShrinkGuardInstalled) return;
  const guarded = function(key, value){
    try {
      if (key === 'cygenix_jobs' && typeof value === 'string'){
        const before = (function(){
          try { return JSON.parse(localStorage.getItem('cygenix_jobs') || '[]'); }
          catch { return []; }
        })();
        let after = [];
        try { after = JSON.parse(value); } catch {}
        if (Array.isArray(before) && Array.isArray(after)){
          const beforeActive = before.filter(j => j && !j._deleted).length;
          const afterActive  = after.filter(j => j && !j._deleted).length;
          // Threshold: warn on any drop of 2+ active jobs in a single write.
          // 1-job drops are normal (deleteJob, etc). 2+ in one write is the
          // signature of the Agentive batch wipe.
          if (beforeActive - afterActive >= 2){
            try {
              console.warn(
                '[Cygenix] cygenix_jobs shrink detected: ' +
                beforeActive + ' active → ' + afterActive + ' active. ' +
                'If this was not a deliberate batch delete, restore from "Show deleted" or Version History.'
              );
              console.warn('[Cygenix] shrink stack:', new Error('shrink-guard trace').stack);
            } catch {}
          }
        }
      }
    } catch {}
    return orig.apply(this, arguments);
  };
  guarded._cygenixShrinkGuardInstalled = true;
  Storage.prototype.setItem = guarded;
})();
function persistJobs() {
  try {
    // Store last 20 jobs, strip large SQL to keep storage under limits
    const toStore = state.jobs.slice(0, 20).map(j => ({
      ...j,
      schemaSQL:    j.schemaSQL    ? j.schemaSQL.slice(0, 5000)    : '',
      migrationSQL: j.migrationSQL ? j.migrationSQL.slice(0, 5000) : '',
      analysis:     j.analysis     ? j.analysis.slice(0, 3000)     : '',
    }));
    localStorage.setItem('cygenix_jobs', JSON.stringify(toStore));
  } catch {}
}

const state = {
  jobs: loadPersistedJobs(),
  files: [], parsedTables: [],
  selectedTarget: 'Microsoft SQL Server (on-premises)',
  totalFilesProcessed: 0, auditLog: [], currentJob: null
};

function $(id) { return document.getElementById(id); }

// ── API Key ───────────────────────────────────────────────────────────────────
function saveApiKey() {
  const input = $('api-key-input');
  const key = input ? input.value.trim() : '';
  if (!key.startsWith('sk-ant-')) {
    alert('Please paste a valid Anthropic API key — it starts with sk-ant-');
    return;
  }
  setApiKey(key);
  input.value = '';
  setBadge('ok');
  $('key-alert').classList.remove('visible');
}

function setBadge(s, label) {
  const b = $('conn-badge');
  if (!b) return;
  // s ∈ 'ok' | 'err' | 'warn' | 'checking'
  if (s === 'ok')       { b.textContent = label || 'Claude AI · Connected'; b.className = 'conn-badge ok'; }
  else if (s === 'err') { b.textContent = label || 'Enter API key ↓';        b.className = 'conn-badge err'; }
  else if (s === 'warn'){ b.textContent = label || 'API unreachable';        b.className = 'conn-badge err'; }
  else                  { b.textContent = label || 'Checking…';              b.className = 'conn-badge checking'; }
}

// ── API liveness check ────────────────────────────────────────────────────────
// The badge has historically been a pure config check (does an API key exist
// somewhere?) which is misleading — a key being set client-side does not mean
// Claude is actually reachable, and the previous implementation had both
// branches of an if/else doing the same thing so the badge stayed red even
// when the key was configured. This rewrite:
//   1. Confirms a key is available (client-side OR via the health function)
//   2. Pings the health endpoint with a short timeout to confirm reachability
//   3. Reports a specific failure mode in the badge so the cause is obvious
async function checkHealth() {
  setBadge('checking');
  const localKey = !!getApiKey();

  // Hit the health endpoint regardless — it tells us whether the platform
  // proxy is up AND whether a server-side key is configured. We tolerate
  // failure: if the network call dies, fall back to the local key check.
  let serverOk = false;
  let serverKey = false;
  try {
    const r = await fetch(HEALTH_URL, { signal: AbortSignal.timeout(6000) });
    if (r.ok) {
      serverOk = true;
      const d = await r.json().catch(() => ({}));
      serverKey = !!d.apiKeyConfigured;
    }
  } catch {
    serverOk = false;
  }

  // Decision tree — pick the most informative state
  if (localKey || serverKey) {
    if (serverOk) {
      setBadge('ok');
      $('key-alert')?.classList.remove('visible');
    } else {
      // Key is set but the platform is unreachable — distinct from "no key"
      setBadge('warn', 'Claude AI · API unreachable');
      $('key-alert')?.classList.remove('visible');
    }
  } else {
    setBadge('err', 'Enter API key ↓');
    $('key-alert')?.classList.add('visible');
  }
}

// Re-check periodically so a transient outage doesn't leave the dot stale.
// 60s is frequent enough to feel live, infrequent enough to be free — but
// only while the tab is actually visible; a backgrounded dashboard was
// polling the health endpoint all day.
setInterval(() => { if (!document.hidden) try { checkHealth(); } catch {} }, 60000);

// ── Navigation ────────────────────────────────────────────────────────────────
// server-migration.js is idle-injected (see the loader in <head>). If the
// user opens the view before the idle callback has run, inject it now and
// init on arrival — the sub-view works on first entry either way.
function ensureServerMigration() {
  if (window.ServerMigration) { window.ServerMigration.init(); return; }
  let s = document.getElementById('server-migration-js');
  if (!s) {
    s = document.createElement('script');
    s.id = 'server-migration-js';
    s.src = '/server-migration.js';
    document.head.appendChild(s);
  }
  s.addEventListener('load', () => { window.ServerMigration && window.ServerMigration.init(); }, { once: true });
}

function showView(v) {
  document.querySelectorAll('.view').forEach(el => el.classList.remove('active'));
  const el = $('view-' + v);
  if (!el) return;
  el.classList.add('active');
  // Scroll .main to top so the view is visible
  const mainEl = document.querySelector('.main');
  if (mainEl) { mainEl.scrollTop = 0; }
  // Tell the shared sidebar which item to highlight
  if (window.CygenixSidebar) window.CygenixSidebar.setActive(v);
  if (v === 'jobs')              renderAllJobs();
  if (v === 'search')            initGlobalSearch();
  if (v === 'schema')            renderSchemaLibrary();
  if (v === 'migration-sql')     renderMigrationLibrary();
  if (v === 'audit')             renderAuditLog();
  if (v === 'dashboard')         renderDashboard();
  if (v === 'reports')           renderReportsList();
  if (v === 'inventory')         renderInventory();
  if (v === 'connections')       initConnectionsView();
  if (v === 'project-settings')  { initProjectSettingsView(); initBackupView(); cygmmRender(); }
  if (v === 'backup')             { showView('project-settings'); return; }
  if (v === 'privacy-security')  initPrivacySecurityView();
  if (v === 'integrations')      initIntegrationsView();
  if (v === 'notifications')     initNotificationsView();
  if (v === 'system-parameters') initSystemParametersView();
  if (v === 'report-settings')   renderReportSettings();
  if (v === 'task-agent')        ta_init();
  if (v === 'server-migration')  ensureServerMigration();
}

function selectTarget(el, name) {
  document.querySelectorAll('.target-card').forEach(c => c.classList.remove('selected'));
  el.classList.add('selected');
  state.selectedTarget = name;
}

// ── File handling ─────────────────────────────────────────────────────────────
function handleDragOver(e) { e.preventDefault(); $('upload-zone').classList.add('drag'); }
function handleDragLeave() { $('upload-zone').classList.remove('drag'); }
function handleDrop(e) { e.preventDefault(); $('upload-zone').classList.remove('drag'); handleFiles(e.dataTransfer.files); }

function handleFiles(fileList) {
  Array.from(fileList).forEach(f => {
    if (!state.files.find(x => x.name === f.name))
      state.files.push({ name: f.name, size: f.size, type: f.type || '', fileObj: f, content: null, readStatus: 'pending' });
    readFile(state.files[state.files.length - 1]);
  });
  renderFileList();
}

function readFile(entry) {
  const textExts = ['sql','csv','txt','json','tsv'];
  const ext = entry.name.split('.').pop().toLowerCase();
  if (!textExts.includes(ext)) { entry.readStatus = 'binary'; renderFileList(); return; }
  const reader = new FileReader();
  reader.onload = e => { entry.content = e.target.result; entry.readStatus = 'read'; renderFileList(); };
  reader.onerror = () => { entry.readStatus = 'error'; renderFileList(); };
  reader.readAsText(entry.fileObj); // read the WHOLE file — we need all rows
}

function formatSize(b) {
  if (!b) return '0B';
  if (b < 1024) return b + 'B';
  if (b < 1048576) return (b/1024).toFixed(1) + 'KB';
  return (b/1048576).toFixed(1) + 'MB';
}
function getExt(name)      { return name?.split('.').pop().toLowerCase() || ''; }
function getExtUpper(name) { return getExt(name).toUpperCase().slice(0,4) || 'FILE'; }

function extClass(name) {
  const e = getExt(name);
  if (e === 'sql') return 'ext-sql';
  if (e === 'bak') return 'ext-bak';
  if (e === 'mdb' || e === 'accdb') return 'ext-mdb';
  return 'ext-default';
}

function removeFile(i) { state.files.splice(i, 1); renderFileList(); }

function renderFileList() {
  $('file-list').innerHTML = state.files.map((f, i) => `
    <div class="file-item">
      <div class="file-info">
        <span class="file-ext ${extClass(f.name)}">${getExtUpper(f.name)}</span>
        <span class="file-name">${f.name}</span>
        ${f.readStatus==='read'   ? `<span class="file-read-indicator file-read-ok">${f.content ? Math.round(f.content.length/1024)+'KB read' : 'read'}</span>` : ''}
        ${f.readStatus==='binary' ? '<span class="file-read-indicator file-read-meta">binary — metadata only</span>' : ''}
        ${f.readStatus==='pending'? '<span class="file-read-indicator" style="color:var(--text3)">reading…</span>' : ''}
        ${f.readStatus==='error'  ? '<span class="file-read-indicator" style="color:var(--red)">read error</span>' : ''}
      </div>
      <div style="display:flex;align-items:center;gap:0.5rem;flex-shrink:0">
        <span class="file-size">${formatSize(f.size)}</span>
        <button class="btn btn-ghost btn-sm" onclick="removeFile(${i})" style="padding:2px 8px">✕</button>
      </div>
    </div>`).join('');
  $('analyse-btn-wrap').style.display = state.files.length > 0 ? 'block' : 'none';
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── FILE PARSERS — extract actual tables and rows from uploaded files ──────────
// ═══════════════════════════════════════════════════════════════════════════════

// Master parse dispatcher
function parseFileContent(file) {
  const ext = getExt(file.name);
  if (!file.content) return [];
  if (ext === 'csv' || ext === 'tsv') return parseCSV(file.content, file.name, ext === 'tsv' ? '\t' : ',');
  if (ext === 'sql') return parseSQL(file.content);
  if (ext === 'json') return parseJSON(file.content, file.name);
  if (ext === 'txt') return parseCSV(file.content, file.name, detectDelimiter(file.content));
  return [];
}

// ── CSV / TSV parser ──────────────────────────────────────────────────────────
function parseCSV(content, filename, delim = ',') {
  const lines = content.split(/\r?\n/).filter(l => l.trim());
  if (lines.length < 2) return [];
  const headers = parseCSVLine(lines[0], delim);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = parseCSVLine(lines[i], delim);
    if (vals.length === 0) continue;
    const row = {};
    headers.forEach((h, idx) => { row[h] = vals[idx] !== undefined ? vals[idx] : null; });
    rows.push(row);
  }
  const tableName = filename.replace(/\.[^.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_');
  return [{ tableName, columns: headers, rows }];
}

function parseCSVLine(line, delim) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i+1] === '"') { current += '"'; i++; }
      else inQuotes = !inQuotes;
    } else if (ch === delim && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += ch;
    }
  }
  result.push(current.trim());
  return result;
}

function detectDelimiter(content) {
  const firstLine = content.split(/\r?\n/)[0] || '';
  const counts = { ',': 0, '\t': 0, '|': 0, ';': 0 };
  for (const ch of firstLine) if (counts[ch] !== undefined) counts[ch]++;
  return Object.entries(counts).sort((a,b) => b[1]-a[1])[0][0];
}

// ── SQL dump parser ───────────────────────────────────────────────────────────
function parseSQL(content) {
  const tables = {};

  // Extract CREATE TABLE definitions
  const createRe = /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`"\[]?(\w+)[`"\]]?\s*\(([\s\S]*?)\);/gi;
  let m;
  while ((m = createRe.exec(content)) !== null) {
    const tableName = m[1];
    const colDefs = m[2];
    const columns = [];
    for (const line of colDefs.split(',')) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      // Skip constraints
      if (/^(PRIMARY|UNIQUE|KEY|INDEX|CONSTRAINT|CHECK|FOREIGN)/i.test(trimmed)) continue;
      const colMatch = trimmed.match(/^[`"\[]?(\w+)[`"\]]?\s+(\S+)/);
      if (colMatch) columns.push(colMatch[1]);
    }
    tables[tableName] = { tableName, columns, rows: [], createDef: m[0] };
  }

  // Extract INSERT statements and parse rows
  const insertRe = /INSERT\s+INTO\s+[`"\[]?(\w+)[`"\]]?\s*(?:\(([^)]+)\))?\s*VALUES\s*([\s\S]*?);(?=\s*(?:INSERT|CREATE|DROP|ALTER|--|\/\*|$))/gi;
  while ((m = insertRe.exec(content)) !== null) {
    const tableName = m[1];
    const colList = m[2] ? m[2].split(',').map(c => c.trim().replace(/[`"[\]]/g,'')) : null;
    const valuesStr = m[3];

    if (!tables[tableName]) tables[tableName] = { tableName, columns: colList || [], rows: [] };

    // Parse each row of values
    const rowMatches = [...valuesStr.matchAll(/\(([^()]*(?:\([^()]*\)[^()]*)*)\)/g)];
    for (const rowMatch of rowMatches) {
      const vals = parseSQLValues(rowMatch[1]);
      if (colList) {
        const row = {};
        colList.forEach((c, i) => { row[c] = vals[i] !== undefined ? vals[i] : null; });
        tables[tableName].rows.push(row);
      } else if (tables[tableName].columns.length > 0) {
        const row = {};
        tables[tableName].columns.forEach((c, i) => { row[c] = vals[i] !== undefined ? vals[i] : null; });
        tables[tableName].rows.push(row);
      }
    }
  }

  return Object.values(tables);
}

function parseSQLValues(str) {
  const vals = [];
  let current = '';
  let inStr = false;
  let strChar = '';
  for (let i = 0; i < str.length; i++) {
    const ch = str[i];
    if (!inStr && (ch === "'" || ch === '"')) { inStr = true; strChar = ch; current += ch; }
    else if (inStr && ch === strChar) {
      if (str[i+1] === strChar) { current += ch + strChar; i++; }
      else { inStr = false; current += ch; }
    } else if (!inStr && ch === ',') {
      vals.push(parseSQLValue(current.trim()));
      current = '';
    } else {
      current += ch;
    }
  }
  if (current.trim()) vals.push(parseSQLValue(current.trim()));
  return vals;
}

function parseSQLValue(v) {
  if (v === 'NULL' || v === 'null') return null;
  if ((v.startsWith("'") && v.endsWith("'")) || (v.startsWith('"') && v.endsWith('"'))) {
    return v.slice(1, -1).replace(/''/g, "'").replace(/""/g, '"');
  }
  return v;
}

// ── JSON parser ───────────────────────────────────────────────────────────────
function parseJSON(content, filename) {
  let data;
  try { data = JSON.parse(content); } catch { return []; }
  const tableName = filename.replace(/\.[^.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_');

  if (Array.isArray(data) && data.length > 0 && typeof data[0] === 'object') {
    const columns = Object.keys(data[0]);
    return [{ tableName, columns, rows: data }];
  }
  // Object of arrays (one key per table)
  if (typeof data === 'object' && !Array.isArray(data)) {
    return Object.entries(data).map(([key, rows]) => {
      if (!Array.isArray(rows) || rows.length === 0) return null;
      return { tableName: key, columns: Object.keys(rows[0]), rows };
    }).filter(Boolean);
  }
  return [];
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── SQL SERVER TYPE INFERENCE ─────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

function inferSQLServerType(values) {
  const nonNull = values.filter(v => v !== null && v !== '' && v !== 'NULL');
  if (nonNull.length === 0) return 'NVARCHAR(255)';

  // BIT
  if (nonNull.every(v => ['0','1','true','false','yes','no'].includes(String(v).toLowerCase()))) return 'BIT';

  // INTEGER
  if (nonNull.every(v => /^-?\d+$/.test(String(v)))) {
    const max = Math.max(...nonNull.map(v => Math.abs(parseInt(v))));
    if (max <= 32767) return 'SMALLINT';
    if (max <= 2147483647) return 'INT';
    return 'BIGINT';
  }

  // DECIMAL
  if (nonNull.every(v => /^-?\d+\.\d+$/.test(String(v)))) {
    const decPlaces = Math.max(...nonNull.map(v => (String(v).split('.')[1]||'').length));
    const intPlaces = Math.max(...nonNull.map(v => (String(v).split('.')[0]||'').replace('-','').length));
    return `DECIMAL(${intPlaces + decPlaces + 2},${decPlaces})`;
  }

  // DATETIME2
  if (nonNull.every(v => /^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?/.test(String(v)))) {
    return nonNull.some(v => /[ T]\d{2}:\d{2}/.test(String(v))) ? 'DATETIME2' : 'DATE';
  }

  // UNIQUEIDENTIFIER (GUID)
  if (nonNull.every(v => /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(String(v)))) return 'UNIQUEIDENTIFIER';

  // NVARCHAR — pick appropriate length
  const maxLen = Math.max(...nonNull.map(v => String(v).length));
  if (maxLen <= 50)  return 'NVARCHAR(50)';
  if (maxLen <= 100) return 'NVARCHAR(100)';
  if (maxLen <= 255) return 'NVARCHAR(255)';
  if (maxLen <= 500) return 'NVARCHAR(500)';
  if (maxLen <= 2000) return 'NVARCHAR(2000)';
  return 'NVARCHAR(MAX)';
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── SQL GENERATOR — produces CREATE TABLE + batched INSERT statements ──────────
// ═══════════════════════════════════════════════════════════════════════════════

function generateCreateTable(tableName, columns, columnTypes, schemaMapping) {
  // schemaMapping comes from Claude — maps source col names to SQL Server col names + types
  const safeName = sqlIdent(tableName);
  const colDefs = columns.map(col => {
    const mapped = schemaMapping?.[col] || {};
    const sqlName = sqlIdent(mapped.name || col);
    const sqlType = mapped.type || columnTypes[col] || 'NVARCHAR(255)';
    const nullable = mapped.nullable !== false ? 'NULL' : 'NOT NULL';
    const defaultVal = mapped.default ? ` DEFAULT ${mapped.default}` : '';
    return `    ${sqlName} ${sqlType}${defaultVal} ${nullable}`;
  });

  // Add primary key if Claude identified one
  const pkCol = schemaMapping?.__primaryKey;
  const pkDef = pkCol ? `,\n    CONSTRAINT PK_${safeName} PRIMARY KEY (${sqlIdent(pkCol)})` : '';

  return `-- ============================================================\n-- Table: ${safeName}\n-- ============================================================\nIF OBJECT_ID(N'dbo.${safeName}', N'U') IS NOT NULL\n    DROP TABLE dbo.${safeName};\nGO\n\nCREATE TABLE dbo.${safeName} (\n${colDefs.join(',\n')}${pkDef}\n);\nGO\n`;
}

function generateInserts(tableName, columns, rows, schemaMapping, wasisRules, wasisLog) {
  if (!rows || rows.length === 0) return `-- No data rows found for ${tableName}\n`;

  const safeName = sqlIdent(tableName);
  const mappedCols = columns.map(col => {
    const mapped = schemaMapping?.[col] || {};
    return sqlIdent(mapped.name || col);
  });

  const colList = mappedCols.join(', ');
  let sql = '';

  // Check if table has identity column
  const hasIdentity = schemaMapping?.__identityColumn;
  if (hasIdentity) {
    sql += `SET IDENTITY_INSERT dbo.${safeName} ON;\nGO\n\n`;
  }

  // Generate batched INSERTs (100 rows per batch)
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(rows.length / BATCH_SIZE);

    sql += `-- Batch ${batchNum} of ${totalBatches} (rows ${i+1}–${Math.min(i+BATCH_SIZE, rows.length)} of ${rows.length})\n`;
    sql += `INSERT INTO dbo.${safeName} (${colList})\nVALUES\n`;

    const rowStrings = batch.map(row => {
      const vals = columns.map(col => {
        let rawVal = row[col];
        // Apply Was/Is substitution if rules exist
        if (wasisRules && wasisRules.length) {
          const result = applyWasis(tableName, col, rawVal, wasisRules);
          if (result.applied && wasisLog) {
            wasisLog.push({ col, oldVal: rawVal, newVal: result.value, applied: result.applied });
          }
          rawVal = result.value;
        }
        return formatSQLValue(rawVal, schemaMapping?.[col]?.type);
      });
      return `    (${vals.join(', ')})`;
    });
    sql += rowStrings.join(',\n') + ';\n';

    // Per-row fallback comment for any batch that might fail
    if (batch.length > 1) {
      sql += `-- If the above batch fails, use individual inserts:\n`;
      sql += batch.map(row => {
        const vals = columns.map(col => {
          let rawVal = row[col];
          if (wasisRules && wasisRules.length) rawVal = applyWasis(tableName, col, rawVal, wasisRules).value;
          return formatSQLValue(rawVal, schemaMapping?.[col]?.type);
        });
        return `-- INSERT INTO dbo.${safeName} (${colList}) VALUES (${vals.join(', ')});`;
      }).join('\n') + '\n';
    }
    sql += '\n';
  }

  if (hasIdentity) {
    sql += `SET IDENTITY_INSERT dbo.${safeName} OFF;\nGO\n\n`;
  }

  return sql;
}

function formatSQLValue(val, sqlType) {
  if (val === null || val === undefined || val === '' || String(val).toUpperCase() === 'NULL') return 'NULL';

  const t = (sqlType || '').toUpperCase();

  // Numeric types — no quotes
  if (['INT','BIGINT','SMALLINT','TINYINT','BIT'].some(n => t.startsWith(n)) ||
      t.startsWith('DECIMAL') || t.startsWith('NUMERIC') || t.startsWith('FLOAT') || t.startsWith('REAL') || t.startsWith('MONEY')) {
    const num = String(val).replace(/[^0-9.\-]/g, '');
    return num || 'NULL';
  }

  // UNIQUEIDENTIFIER
  if (t === 'UNIQUEIDENTIFIER') return `'${String(val).replace(/'/g, "''")}'`;

  // Date/time
  if (t === 'DATE' || t === 'DATETIME2' || t === 'DATETIME' || t === 'SMALLDATETIME') {
    return `'${String(val).replace(/'/g, "''")}'`;
  }

  // Everything else — string with escaped quotes
  return `N'${String(val).replace(/'/g, "''")}'`;
}

function sqlIdent(name) {
  // Wrap in brackets to handle reserved words and special chars
  return `[${String(name).replace(/]/g, ']]')}]`;
}


// ═══════════════════════════════════════════════════════════════════════════════
// ── WAS/IS MAPPING ─────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════
// The canonical Was/Is file is any inventory item of type 'wasis'.
// Format: source_table, old_value, new_value, description
// (first row is header, subsequent rows are rules)

function getWasisRules() {
  // Find the active wasis document in inventory
  const wasisDoc = invItems.find(d => d.type === 'wasis' && d.isText && d.content);
  if (!wasisDoc) return [];

  const lines = wasisDoc.content.split(/\r?\n/).filter(l => l.trim());
  if (lines.length < 2) return [];

  // Parse header to find column positions
  const delim = wasisDoc.ext === 'tsv' ? '\t' : ',';
  const parseL = line => {
    const r=[]; let c=''; let q=false;
    for(const ch of line){if(ch==='"')q=!q;else if(ch===delim&&!q){r.push(c.trim());c='';}else c+=ch;}
    r.push(c.trim()); return r;
  };

  const headers = parseL(lines[0]).map(h => h.toLowerCase().replace(/[^a-z0-9]/g,''));

  // Detect columns by name — canonical format: source_table, source_field, old_value, new_value, description
  const srcTableIdx = headers.findIndex(h => ['sourcetable','table','srctable','tablename'].includes(h));
  const srcFieldIdx = headers.findIndex(h => ['sourcefield','field','srcfield','fieldname','column','columnname','col'].includes(h));
  const oldValIdx   = headers.findIndex(h => ['oldvalue','was','old','from','sourcevalue','oldval'].includes(h));
  const newValIdx   = headers.findIndex(h => ['newvalue','is','new','to','targetvalue','newval'].includes(h));
  const descIdx     = headers.findIndex(h => ['description','desc','note','notes','reason'].includes(h));

  // Fallback positional order: source_table(0), source_field(1), old_value(2), new_value(3), description(4)
  const tIdx = srcTableIdx >= 0 ? srcTableIdx : 0;
  const fIdx = srcFieldIdx >= 0 ? srcFieldIdx : 1;
  const oIdx = oldValIdx   >= 0 ? oldValIdx   : 2;
  const nIdx = newValIdx   >= 0 ? newValIdx   : 3;
  const dIdx = descIdx     >= 0 ? descIdx     : 4;

  const rules = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = parseL(lines[i]);
    const srcTable = cols[tIdx]?.trim() || '';
    const srcField = cols[fIdx]?.trim() || '';
    const oldVal   = cols[oIdx]?.trim() || '';
    const newVal   = cols[nIdx]?.trim() || '';
    const desc     = cols[dIdx]?.trim() || '';
    if (oldVal !== '' || newVal !== '') {
      rules.push({
        srcTable: srcTable.toLowerCase(),
        srcField: srcField.toLowerCase(),
        oldVal, newVal, desc
      });
    }
  }
  return rules;
}

// Apply Was/Is rules to a single cell value — matched by table, field, and old value
function applyWasis(tableName, colName, value, wasisRules) {
  if (!wasisRules || wasisRules.length === 0) return { value, applied: null };
  const tbl  = (tableName || '').toLowerCase();
  const fld  = (colName   || '').toLowerCase();
  const strVal = String(value ?? '');

  // Priority 1: table + field + value exact match
  let rule = wasisRules.find(r =>
    (r.srcTable === tbl || r.srcTable === '') &&
    (r.srcField === fld || r.srcField === '') &&
    r.oldVal.toLowerCase() === strVal.toLowerCase()
  );

  // Priority 2: field + value match (any table)
  if (!rule) rule = wasisRules.find(r =>
    r.srcField === fld && r.srcField !== '' &&
    r.oldVal.toLowerCase() === strVal.toLowerCase()
  );

  if (rule) return { value: rule.newVal, applied: rule };
  return { value, applied: null };
}

// Build a summary of Was/Is rules applied
function buildWasisSummary(wasisRules, applicationLog) {
  if (!wasisRules.length) return '';
  const applied  = applicationLog.filter(l => l.applied);
  const byRule   = {};
  applied.forEach(l => {
    const tbl = l.applied.srcTable || '*';
    const fld = l.applied.srcField || '*';
    const key = `${tbl}.${fld}: "${l.applied.oldVal}" → "${l.applied.newVal}"`;
    byRule[key] = (byRule[key] || 0) + 1;
  });
  const lines = Object.entries(byRule)
    .sort((a,b) => b[1]-a[1])
    .map(([rule, count]) => `  ${rule} (${count.toLocaleString()} row(s))`);
  return `\n## Was/Is Value Translations Applied\nTotal substitutions: ${applied.length.toLocaleString()}\n${lines.join('\n')}`;
}

// Update the wasis banner on the new-job view
function updateWasisBanner() {
  const rules = getWasisRules();
  // Banner on new-job view
  const banner = $('wasis-banner');
  if (banner) {
    if (rules.length > 0) {
      $('wasis-rule-count').textContent = rules.length;
      banner.style.display = 'block';
    } else {
      banner.style.display = 'none';
    }
  }
  // Status panel on inventory left sidebar
  const statusPanel = $('wasis-status-panel');
  const statusText  = $('wasis-status-text');
  if (statusPanel) {
    if (rules.length > 0) {
      statusPanel.style.display = 'block';
      if (statusText) statusText.textContent = rules.length + ' rules ready for migration';
    } else {
      statusPanel.style.display = 'none';
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── CLAUDE SCHEMA ANALYSIS — gets intelligent column mapping ──────────────────
// ═══════════════════════════════════════════════════════════════════════════════

async function askClaudeForSchemaMapping(tables, targetServer, sourceSystem, apiKey, wasisRules) {
  // Send Claude a sample of the data (first 20 rows per table) + inferred types
  // Build Was/Is section for the prompt
  const wasisPromptSection = wasisRules && wasisRules.length > 0
    ? `\n\nWAS/IS VALUE MAPPING (${wasisRules.length} rules — apply these value substitutions during migration):\n` +
      `Format: source_table | old_value → new_value | description\n` +
      wasisRules.map(r => `  ${r.srcTable || '*'}.${r.srcField || '*'} | "${r.oldVal}" → "${r.newVal}"${r.desc ? ' | ' + r.desc : ''}`).join('\n') +
      `\n\nWhen you see these old values in the data, flag them in the notes for each affected table and column.`
    : '';

  const tableDescriptions = tables.map(t => {
    const sampleRows = t.rows.slice(0, 20);
    const colTypes = {};
    t.columns.forEach(col => {
      const vals = t.rows.map(r => r[col]);
      colTypes[col] = inferSQLServerType(vals);
    });
    return {
      tableName: t.tableName,
      rowCount: t.rows.length,
      columns: t.columns,
      inferredTypes: colTypes,
      sampleData: sampleRows
    };
  });

  const prompt = `You are Cygenix, an expert SQL Server database migration engineer.

I have parsed ${tables.length} table(s) from uploaded database files and auto-detected SQL Server types. Your job is to review and improve the schema mapping, then return a precise JSON object.

SOURCE SYSTEM: ${sourceSystem || 'Unknown'}
TARGET: ${targetServer}

TABLES WITH INFERRED SCHEMA:
${JSON.stringify(tableDescriptions, null, 2)}

Return ONLY a valid JSON object (no markdown, no explanation) in this exact format:
{
  "tables": {
    "<tableName>": {
      "columns": {
        "<sourceColName>": {
          "name": "<target SQL Server column name — clean, no spaces>",
          "type": "<SQL Server type e.g. NVARCHAR(100), INT, DATETIME2, BIT, DECIMAL(10,2)>",
          "nullable": true or false,
          "default": null or "<default value>"
        }
      },
      "__primaryKey": "<column name that should be PK, or null>",
      "__identityColumn": "<column name that is an identity/autoincrement, or null>",
      "indexes": ["<col1>", "<col2>"],
      "notes": "<any migration warnings for this table>"
    }
  },
  "globalNotes": "<overall migration warnings, encoding issues, reserved word conflicts>"
}

Rules:
- Fix any column names that are SQL Server reserved words (e.g. 'order', 'user', 'name', 'group') by appending an underscore or renaming sensibly
- Upgrade NVARCHAR(255) to larger sizes if sample data suggests it
- Detect identity/autoincrement columns (usually named id, *_id, with sequential integers)
- Detect likely primary keys
- Suggest useful indexes (foreign key columns, frequently-filtered columns)
- Flag any date format issues, encoding problems, or data quality concerns
${wasisPromptSection}`;

  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
      'anthropic-dangerous-direct-browser-access': 'true'
    },
    body: JSON.stringify({
      model: CygenixModel.primary(),
      max_tokens: 4096,
      messages: [{ role: 'user', content: prompt }]
    })
  });

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Claude API error (${res.status}): ${err.slice(0, 200)}`);
  }

  const data = await res.json();
  const text = data.content?.map(b => b.text || '').join('') || '';

  // Extract JSON from response (handle any markdown wrapping)
  const jsonMatch = text.match(/\{[\s\S]*\}/);
  if (!jsonMatch) throw new Error('Claude did not return valid JSON. Response: ' + text.slice(0, 300));

  return JSON.parse(jsonMatch[0]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── MAIN ANALYSIS FLOW ────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

async function startAnalysis() {
  const jobName = $('job-name-input').value.trim() || 'Untitled Migration';
  if (!state.files.length) { alert('Please upload at least one database file.'); return; }

  const apiKey = getApiKey();
  if (!apiKey) {
    $('key-alert').classList.add('visible');
    alert('Please enter your Anthropic API key first.');
    return;
  }

  // Check all readable files are finished reading
  const stillReading = state.files.filter(f => f.readStatus === 'pending');
  if (stillReading.length > 0) { alert('Files are still being read, please wait a moment.'); return; }

  const panel  = $('analysis-panel');
  const btn    = $('analyse-btn');
  panel.classList.add('visible');
  panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  btn.disabled = true;

  const setStatus = (msg, color) => {
    $('analysis-title').textContent = msg;
    $('ai-dot').style.background = color || 'var(--accent)';
    $('ai-dot').style.animationPlayState = color === 'var(--green)' || color === 'var(--red)' ? 'paused' : 'running';
  };

  const setBody = msg => { $('analysis-body').textContent = msg; };

  const resetBtn = () => {
    btn.disabled = false;
    btn.textContent = 'Analyse & Generate SQL with Claude';
  };

  const fail = msg => {
    setStatus('Analysis failed', 'var(--red)');
    setBody(msg);
    resetBtn();
  };

  try {
    // ── STEP 1: Parse all uploaded files ──────────────────────────────────────
    setStatus('Step 1 of 4 — Parsing uploaded files…');
    setBody('Reading file contents and extracting table data…');

    const allTables = [];
    const binaryFiles = [];

    for (const file of state.files) {
      if (file.content) {
        btn.textContent = `Parsing ${file.name}…`;
        const tables = parseFileContent(file);
        if (tables.length > 0) {
          allTables.push(...tables);
          setBody(`Parsed ${file.name}: found ${tables.length} table(s) with ${tables.reduce((s,t) => s+t.rows.length,0)} total rows`);
        } else {
          setBody(`Could not extract table data from ${file.name} — it may be an unsupported format`);
        }
      } else {
        binaryFiles.push(file.name);
      }
    }

    if (allTables.length === 0 && binaryFiles.length > 0) {
      fail(`No parseable data found.\n\nBinary files (${binaryFiles.join(', ')}) cannot be read directly in the browser.\n\nFor .bak files: use SSMS → Script Database As → Script to file → upload the .sql\nFor .mdb files: use Access → External Data → Export → Text File (CSV) → upload the CSV`);
      return;
    }

    if (allTables.length === 0) {
      fail('No table data could be extracted from the uploaded files.\n\nMake sure your files contain actual data rows, not just schema definitions.');
      return;
    }

    const totalRows = allTables.reduce((s, t) => s + t.rows.length, 0);
    setBody(`✓ Parsed ${allTables.length} table(s) with ${totalRows.toLocaleString()} total rows\n\nTables found:\n${allTables.map(t => `  • ${t.tableName}: ${t.rows.length.toLocaleString()} rows, ${t.columns.length} columns`).join('\n')}`);

    // ── STEP 2: Ask Claude to refine the schema mapping ───────────────────────
    setStatus('Step 2 of 4 — Claude is analysing schema & data types…');
    btn.textContent = 'Asking Claude…';

    const wasisRules = getWasisRules();
    let schemaResult;
    try {
      schemaResult = await askClaudeForSchemaMapping(
        allTables, state.selectedTarget,
        $('job-source-input').value.trim(), apiKey, wasisRules
      );
    } catch(e) {
      fail('Claude schema analysis failed:\n\n' + e.message);
      return;
    }

    setBody(`✓ Claude analysed ${allTables.length} table(s)\n\n${schemaResult.globalNotes || 'Schema mapping complete.'}`);

    // ── STEP 3: Generate CREATE TABLE statements ───────────────────────────────
    setStatus('Step 3 of 4 — Generating SQL Server schema…');
    btn.textContent = 'Generating CREATE TABLE…';

    let schemaSQL = `-- ============================================================\n-- Cygenix SQL Server Migration Script\n-- Generated: ${new Date().toISOString()}\n-- Source: ${state.files.map(f=>f.name).join(', ')}\n-- Target: ${state.selectedTarget}\n-- Tables: ${allTables.length} | Total rows: ${totalRows.toLocaleString()}\n-- ============================================================\n\nUSE [YourDatabaseName]; -- ← CHANGE THIS\nGO\n\n`;

    if (schemaResult.globalNotes) {
      schemaSQL += `-- MIGRATION NOTES:\n${schemaResult.globalNotes.split('\n').map(l => '-- ' + l).join('\n')}\n\n`;
    }

    const wasisApplicationLog = [];
    for (const table of allTables) {
      const tableMapping = schemaResult.tables?.[table.tableName];
      const colTypes = {};
      table.columns.forEach(col => {
        const vals = table.rows.map(r => r[col]);
        colTypes[col] = inferSQLServerType(vals);
      });

      if (tableMapping?.notes) {
        schemaSQL += `-- TABLE NOTE (${table.tableName}): ${tableMapping.notes}\n`;
      }

      schemaSQL += generateCreateTable(
        table.tableName,
        table.columns,
        colTypes,
        tableMapping?.columns
      );

      // Add indexes
      if (tableMapping?.indexes?.length > 0) {
        const safeName = sqlIdent(table.tableName);
        tableMapping.indexes.forEach(col => {
          const safeCol = sqlIdent(tableMapping?.columns?.[col]?.name || col);
          schemaSQL += `CREATE INDEX IX_${table.tableName}_${col} ON dbo.${safeName} (${safeCol});\nGO\n`;
        });
      }
      schemaSQL += '\n';
    }

    // ── STEP 4: Generate INSERT statements with actual data ────────────────────
    setStatus('Step 4 of 4 — Generating INSERT statements with all data rows…');

    let migrationSQL = `-- ============================================================\n-- Cygenix Data Migration INSERTs\n-- Generated: ${new Date().toISOString()}\n-- Total rows: ${totalRows.toLocaleString()}\n-- Batch size: ${BATCH_SIZE} rows per INSERT\n-- ============================================================\n\nUSE [YourDatabaseName]; -- ← CHANGE THIS\nGO\n\nSET NOCOUNT ON;\nGO\n\n`;

    for (const table of allTables) {
      const tableMapping = schemaResult.tables?.[table.tableName];
      btn.textContent = `Generating INSERTs for ${table.tableName} (${table.rows.length.toLocaleString()} rows)…`;
      setBody(`Generating INSERT statements for ${table.tableName}…\n${table.rows.length.toLocaleString()} rows → ${Math.ceil(table.rows.length/BATCH_SIZE)} batches of ${BATCH_SIZE}`);

      migrationSQL += `-- ============================================================\n-- Table: ${table.tableName} | ${table.rows.length.toLocaleString()} rows\n-- ============================================================\n\n`;
      migrationSQL += generateInserts(
        table.tableName,
        table.columns,
        table.rows,
        tableMapping?.columns,
        wasisRules,
        wasisApplicationLog
      );

      // Yield to browser to avoid blocking UI
      await new Promise(r => setTimeout(r, 0));
    }

    migrationSQL += `\n-- ============================================================\n-- Migration complete\n-- Verify row counts:\n${allTables.map(t => `-- SELECT COUNT(*) FROM dbo.${sqlIdent(t.tableName)}; -- Expected: ${t.rows.length.toLocaleString()}`).join('\n')}\n-- ============================================================\n`;

    // ── Done ──────────────────────────────────────────────────────────────────
    setStatus('SQL Migration Package Ready', 'var(--green)');

    // Build summary report
    const dataTypeMappings = allTables.map(t => {
      const tableMapping = schemaResult.tables?.[t.tableName];
      return `\n### ${t.tableName} (${t.rows.length.toLocaleString()} rows)\n` +
        t.columns.map(col => {
          const mapped = tableMapping?.columns?.[col] || {};
          const inferred = inferSQLServerType(t.rows.map(r => r[col]));
          const final = mapped.type || inferred;
          const renamedTo = mapped.name && mapped.name !== col ? ` → renamed to [${mapped.name}]` : '';
          return `  ${col}${renamedTo}: ${final}`;
        }).join('\n');
    }).join('\n');

    const fullReport = `# Cygenix SQL Migration Report
Generated: ${new Date().toLocaleString()}
Source files: ${state.files.map(f=>f.name).join(', ')}
Target: ${state.selectedTarget}

## Summary
- Tables migrated: ${allTables.length}
- Total rows: ${totalRows.toLocaleString()}
- Schema SQL: ${schemaSQL.length.toLocaleString()} chars
- Migration SQL: ${migrationSQL.length.toLocaleString()} chars (${Math.ceil(migrationSQL.length/1024)}KB)
- INSERT batches: ${allTables.reduce((s,t) => s+Math.ceil(t.rows.length/BATCH_SIZE),0)} total

## Table Summary
${allTables.map(t => `- ${t.tableName}: ${t.rows.length.toLocaleString()} rows, ${t.columns.length} columns`).join('\n')}

## Global Notes
${schemaResult.globalNotes || 'None'}

## Column Mapping & Data Types
${dataTypeMappings}

## Verification Queries
Run these after migration to confirm row counts:
${allTables.map(t => `SELECT '${t.tableName}' AS TableName, COUNT(*) AS ActualRows, ${t.rows.length} AS ExpectedRows FROM dbo.${sqlIdent(t.tableName)};`).join('\n')}
${buildWasisSummary(wasisRules, wasisApplicationLog)}`;

    $('analysis-body').textContent = fullReport;
    $('schema-code').textContent    = schemaSQL;
    $('migration-code').textContent = migrationSQL;
    $('result-tabs').style.display  = 'flex';
    $('analysis-cta').innerHTML = `
      <button class="btn btn-teal btn-sm" onclick="downloadSQL('schema','${escapeAttr(jobName)}')"><i class="ic ic-download"></i> Schema .sql (${Math.ceil(schemaSQL.length/1024)}KB)</button>
      <button class="btn btn-primary btn-sm" onclick="downloadSQL('migration','${escapeAttr(jobName)}')"><i class="ic ic-download"></i> Migration .sql (${Math.ceil(migrationSQL.length/1024)}KB)</button>`;

    // Build column mapping for report
    const jobColumnMapping = allTables.flatMap(t => {
      const tm = schemaResult.tables?.[t.tableName];
      return t.columns.map(col => ({
        srcCol: col,
        tgtCol: tm?.columns?.[col]?.name || col,
        tgtType: tm?.columns?.[col]?.type || inferSQLServerType(t.rows.map(r => r[col])),
        transform: 'NONE',
      }));
    });
    const wasisCount = wasisApplicationLog.filter(l => l.applied).length;
    const job = {
      id: 'job_' + Date.now(), name: jobName,
      source: $('job-source-input').value.trim(),
      target: state.selectedTarget,
      files: state.files.map(f => ({ name: f.name, size: f.size })),
      tables: allTables.map(t => ({ name: t.tableName, rows: t.rows.length, cols: t.columns.length })),
      analysis: fullReport, schemaSQL, migrationSQL,
      status: 'complete', created: new Date().toISOString(),
      totalRows,
      columnMapping: jobColumnMapping,
      wasisRulesApplied: wasisCount,
      wasisRulesTotal: wasisRules.length,
      warnings: wasisCount > 0 ? [`Was/Is mapping applied: ${wasisCount.toLocaleString()} value substitutions across ${wasisRules.length} rules`] : [],
    };

    state.jobs.unshift(job);
    persistJobs();
    // Schedule the first auto-version snapshot. This produces the v1
    // entry in Cosmos's job_versions container so the job has a recovery
    // point from the moment it's created, not from the first edit.
    if (typeof scheduleAutoVersion === 'function') scheduleAutoVersion(job.id, 'job created');
    state.totalFilesProcessed += state.files.length;
    addAudit(`Job "${jobName}" complete — ${allTables.length} tables, ${totalRows.toLocaleString()} rows, ${Math.ceil(migrationSQL.length/1024)}KB SQL generated`);
    updateStats();
    resetBtn();

  } catch(err) {
    fail('Unexpected error:\n\n' + err.message + '\n\n' + (err.stack || ''));
    resetBtn();
  }
}

// ── Tabs ──────────────────────────────────────────────────────────────────────
function switchTab(t) {
  ['full','schema','migration'].forEach(n => { $('tab-'+n).style.display = n===t?'block':'none'; });
  document.querySelectorAll('#result-tabs .tab').forEach((el,i) => {
    el.classList.toggle('active', ['full','schema','migration'][i]===t);
  });
}
function switchModalTab(t) {
  ['full-report','schema-sql','migration-sql-tab'].forEach(n => { $('modal-tab-'+n).style.display = n===t?'block':'none'; });
  document.querySelectorAll('#job-modal .tab').forEach((el,i) => {
    el.classList.toggle('active', ['full-report','schema-sql','migration-sql-tab'][i]===t);
  });
}
function copyCode(elId) {
  navigator.clipboard.writeText($(elId).textContent).then(() => {
    const btn = $(elId).parentElement.querySelector('.copy-btn');
    if (btn) { btn.textContent='Copied!'; setTimeout(()=>btn.textContent='Copy SQL',2000); }
  });
}

// ── Downloads ─────────────────────────────────────────────────────────────────
function downloadSQL(type, jobName) {
  const el = type==='schema' ? $('schema-code') : $('migration-code');
  const content = el?.textContent || '';
  if (!content) { alert('No SQL generated yet.'); return; }
  triggerDownload(content, (jobName||'migration').replace(/[^a-z0-9]/gi,'_') + (type==='schema'?'_schema':'_migration') + '.sql');
}
function downloadJobSQL(jobId, type) {
  const job = state.jobs.find(j=>j.id===jobId);
  if (!job) return;
  // Jobs use insertSQL (from saveJobAndFinish) or migrationSQL (legacy)
  const content = type==='schema'
    ? (job.schemaSQL || '')
    : (job.insertSQL || job.migrationSQL || '');
  if (!content) { alert('No SQL available for this job. Open Configure to generate SQL first.'); return; }
  triggerDownload(content, job.name.replace(/[^a-z0-9]/gi,'_') + (type==='schema'?'_schema':'_migration') + '.sql');
}
function downloadModalSQL(type) {
  const el = type==='schema' ? $('modal-schema-code') : $('modal-migration-code');
  const content = el?.textContent || '';
  if (!content) { alert('No SQL available.'); return; }
  const name = (state.currentJob?.name||'migration').replace(/[^a-z0-9]/gi,'_');
  triggerDownload(content, name + (type==='schema'?'_schema':'_migration') + '.sql');
}
function triggerDownload(content, filename) {
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([content], {type:'text/plain'}));
  a.download = filename;
  a.click();
}

// ── Job modal ─────────────────────────────────────────────────────────────────
function openJobModal(jobId) {
  const job = state.jobs.find(j=>j.id===jobId);
  if (!job) return;
  state.currentJob = job;
  $('modal-job-name').textContent   = job.name;
  $('modal-job-meta').textContent   = `${job.target} · ${new Date(job.created).toLocaleDateString('en-GB')} · ${job.files.length} file(s) · ${job.totalRows?.toLocaleString()||'?'} rows`;
  $('modal-full-text').textContent  = job.analysis || '';
  $('modal-schema-code').textContent    = job.schemaSQL || '';
  $('modal-migration-code').textContent = job.migrationSQL || '';
  switchModalTab('full-report');
  $('job-modal').classList.add('open');
}
function closeModal() { $('job-modal').classList.remove('open'); }

// ── Render ────────────────────────────────────────────────────────────────────
function addAudit(msg) { state.auditLog.unshift({ time: new Date().toLocaleTimeString(), msg }); }
// ── Job counting — one definition, shared by every panel ──────────────────
// The Dashboard counted jobs three different ways on a single screen:
//
//   renderRing()          all jobs in storage, executionStatus || status
//   updateStats()         all jobs in storage, status only
//   renderProjectStatus() jobs matching projectId, executionStatus || status
//
// so the same job could be counted differently by two panels an inch apart.
// A project run records its outcome on executionStatus and leaves status at
// 'ready', which made such a job Complete in the donut but not in the stat
// card. And trashed jobs were filtered only by the All Jobs table, so
// deleting a job shrank the table while the donut and the totals never moved.
//
// These helpers are now the only way a panel counts jobs or reads a status.

// Soft-deleted jobs are in the trash (restorable via restoreJob) and must not
// appear in any total — the number on screen has to match what the user sees
// in the jobs list.
function liveJobs(list){
  const arr = Array.isArray(list) ? list : safeArr('cygenix_jobs');
  return arr.filter(j => j && !j._deleted);
}

// A job's lifecycle bucket. executionStatus is written by a project run and
// reflects what actually happened, so it wins over the status the job was
// saved with. The extra spellings are accepted because project bundles
// imported through cygenix-integrations carry whatever status they were
// exported with.
function jobBucket(j){
  const s = String((j && (j.executionStatus || j.status)) || '').toLowerCase();
  if (s === 'complete' || s === 'completed' || s === 'success') return 'complete';
  if (s === 'ready'    || s === 'sql_ready' || s === 'sql-ready') return 'ready';
  if (s === 'failed'   || s === 'fail'      || s === 'error')     return 'failed';
  return 'pending';
}

// A job counts as analysed once it has at least one mapped target column.
function jobIsMapped(j){
  return Array.isArray(j && j.columnMapping) && j.columnMapping.some(m => m && m.tgtCol);
}

function updateStats() {
  // Null-guard every DOM update — if any of these elements is missing from the
  // page (e.g. during A/B layouts or partial renders), the unguarded `.textContent`
  // would throw TypeError and halt the entire init chain, including
  // initUserAndProject() which runs after renderDashboard(). That manifests as
  // a stuck "Loading..." user pill even though auth is working fine.
  const set = (id, v) => { const el = $(id); if (el) el.textContent = v; };
  const jobs = liveJobs(state.jobs);
  set('stat-total',    jobs.length);
  // "Analysed / Schema ready" was character-for-character the same expression
  // as Total Jobs, so the two cards could never show different numbers. It now
  // counts what its label claims.
  set('stat-analysed', jobs.filter(jobIsMapped).length);
  // "SQL Generated / Ready to run" — SQL exists once a job reaches ready, and
  // still exists after it has run. The old test was `status === 'complete'`,
  // which contradicted the card's own label and missed every project run.
  set('stat-done',     jobs.filter(j => { const b = jobBucket(j); return b === 'ready' || b === 'complete'; }).length);
  set('stat-files',    state.totalFilesProcessed);
  set('jobs-count',    jobs.length);
  renderDashboardProjects();
}

// ── Dashboard projects card ───────────────────────────────────────────────────
// Lists every project ("conversion") with status, jobs, artifacts and dates.
// Cards link to /projects.html for full CRUD; clicking a card sets it active
// and jumps to All Jobs filtered to that project.
//
// Status is derived from the project's jobs (no jobs → draft, all complete →
// complete, otherwise → active) unless the project has statusManual=true, in
// which case the explicit `status` field is used. Same logic as projects.html
// — keep them in sync if you change one.
function renderDashboardProjects(){
  const list = document.getElementById('dashboard-projects-list');
  const sub  = document.getElementById('dashboard-projects-sub');
  if (!list) return;

  // Read projects, then opportunistically migrate legacy single-project data
  // forward so users with only `cygenix_project_settings` see something here
  // instead of a "no projects yet" state. Mirror of the migration in
  // projects.html — first one to run wins, the other becomes a no-op.
  let projects = [];
  try { projects = JSON.parse(localStorage.getItem('cygenix_projects') || '[]'); } catch {}
  if (!Array.isArray(projects)) projects = [];

  if (projects.length === 0 && localStorage.getItem('cygenix_projects_migrated') !== '1') {
    let legacy = null;
    try { legacy = JSON.parse(localStorage.getItem('cygenix_project_settings') || 'null'); } catch {}
    if (legacy && legacy.name) {
      const id  = 'proj_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2,7);
      const now = new Date().toISOString();
      projects = [{
        id,
        name:        legacy.name,
        client:      legacy.client || '',
        ref:         legacy.ref || '',
        description: legacy.desc || legacy.notes || '',
        systems:     [legacy['src-system'], legacy['tgt-system']].filter(Boolean).join(' → '),
        status:      '',
        statusManual:false,
        created:     legacy.start || now,
        modified:    now,
      }];
      try {
        localStorage.setItem('cygenix_projects', JSON.stringify(projects));
        localStorage.setItem('cygenix_projects_migrated', '1');
        if (!localStorage.getItem('cygenix_active_project_id')) {
          localStorage.setItem('cygenix_active_project_id', id);
        }
      } catch {}
    } else {
      try { localStorage.setItem('cygenix_projects_migrated', '1'); } catch {}
    }
  }

  const activeId = localStorage.getItem('cygenix_active_project_id') || '';
  const allJobs  = liveJobs(state.jobs || []);   // project cards must not count the trash

  // Inventory (artifacts) — best-effort, may not exist yet
  let inv = [];
  try { inv = JSON.parse(localStorage.getItem('cygenix_inventory') || '[]'); } catch {}

  // Empty state
  if (!projects.length){
    list.innerHTML = '<div style="font-size:12px;color:var(--text3);font-family:var(--mono);padding:0.5rem 0">No projects yet — click "+ New Project" to create your first conversion.</div>';
    if (sub) sub.textContent = '0 projects';
    return;
  }

  // Sort: active first, then most recently modified
  const sorted = projects.slice().sort((a,b) => {
    if (a.id === activeId) return -1;
    if (b.id === activeId) return  1;
    return Date.parse(b.modified || b.created || 0) - Date.parse(a.modified || a.created || 0);
  });

  const cards = sorted.map(p => dashProjCardHtml(p, allJobs, inv, activeId));
  list.innerHTML = '<div class="dash-proj-grid">' + cards.join('') + '</div>';

  if (sub){
    const activeName = (projects.find(p => p.id === activeId) || {}).name;
    sub.textContent = projects.length + ' project' + (projects.length===1?'':'s') +
      (activeName ? ' · active: ' + activeName : '');
  }

  // Render the read-only Active Project summary card above the projects list
  renderActiveProjectSummary(projects, activeId, allJobs, inv);
}

// Read-only snapshot of the currently active project, shown directly above
// the Projects card on the dashboard. Hidden when no active project. All
// editing happens on /projects.html — there's an "Edit details" link in the
// header that takes the user there.
function renderActiveProjectSummary(projects, activeId, allJobs, inv){
  const wrap = document.getElementById('dashboard-active-summary');
  if (!wrap) return;
  if (!activeId) { wrap.style.display = 'none'; return; }
  const p = projects.find(x => x.id === activeId);
  if (!p) { wrap.style.display = 'none'; return; }

  const setText = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
  const fmt = ts => ts ? new Date(ts).toLocaleDateString('en-GB', { day:'numeric', month:'short', year:'numeric' }) : '—';
  const jobCount      = allJobs.filter(j => j.projectId === p.id).length;
  const artifactCount = inv.filter(a => a.projectId === p.id).length;
  const status = dashProjStatus(p, allJobs);

  setText('das-name', p.name || 'Untitled project');
  // Sub line — client + ref + description (truncated)
  const subBits = [];
  if (p.client) subBits.push(p.client);
  if (p.ref)    subBits.push(p.ref);
  const desc = (p.description || '').trim();
  if (desc) subBits.push(desc.length > 80 ? desc.slice(0, 80) + '…' : desc);
  setText('das-sub', subBits.length ? subBits.join(' · ') : 'No description');

  // Meta grid — six tiles with the most useful at-a-glance fields
  const grid = document.getElementById('das-meta-grid');
  if (grid) {
    const cell = (label, val) => `<div style="background:var(--bg2);border:0.5px solid var(--border);border-radius:10px;padding:0.55rem 0.75rem">
      <div style="font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:0.06em;font-family:var(--mono);margin-bottom:2px">${label}</div>
      <div style="font-size:12px;color:var(--text);font-family:var(--mono);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${escapeHtml(val || '—')}</div>
    </div>`;
    grid.innerHTML = [
      cell('Status',     status.value + (status.derived ? ' (auto)' : '')),
      cell('Jobs',       String(jobCount)),
      cell('Artifacts',  String(artifactCount)),
      cell('Phase',      p.phase || '—'),
      cell('Source',     p.srcSystem || '—'),
      cell('Target',     p.tgtSystem || '—'),
      cell('Start',      fmt(p.start)),
      cell('Target end', fmt(p.end)),
    ].join('');
  }
  wrap.style.display = '';
}

function dashProjStatus(p, jobs){
  // 'active' is reserved for the project whose id matches
  // cygenix_active_project_id. Mirrors statusFor() in projects.html — keep
  // these in sync if you change one.
  const activeId = localStorage.getItem('cygenix_active_project_id') || '';
  if (p.id === activeId) return { value: 'active', derived: false };
  if (p.statusManual && p.status && p.status !== 'active') return { value: p.status, derived: false };
  const projJobs = jobs.filter(j => j.projectId === p.id);
  let v;
  if (projJobs.length === 0) v = 'draft';
  // Shared bucketing. This used to test `j.status === 'complete'` alone, so a
  // project whose jobs had all been run to completion by project-builder —
  // which records the outcome on executionStatus and leaves status at 'ready'
  // — could never derive to 'complete'.
  else if (projJobs.every(j => jobBucket(j) === 'complete')) v = 'complete';
  else v = 'inactive';
  return { value: v, derived: true };
}

function dashProjCardHtml(p, allJobs, inv, activeId){
  const jobCount      = allJobs.filter(j => j.projectId === p.id).length;
  const artifactCount = inv.filter(a => a.projectId === p.id).length;
  const isActive      = p.id === activeId;
  const fmt = ts => ts ? new Date(ts).toLocaleDateString('en-GB', { day:'numeric', month:'short' }) : '—';
  const status = dashProjStatus(p, allJobs);

  return `<div class="dash-proj-card ${isActive ? 'active-project' : ''}" onclick="setActiveProjectAndJump('${escapeAttr(p.id)}')" title="Open ${escapeAttr(p.name||'project')}">
    <div class="dash-proj-card-head">
      <div style="min-width:0;flex:1">
        <div class="dash-proj-name">${escapeHtml(p.name || 'Untitled project')}</div>
        ${p.client ? `<div class="dash-proj-client">${escapeHtml(p.client)}</div>` : ''}
      </div>
      ${isActive
        ? '<span class="dash-proj-active-pill">active</span>'
        : `<span class="dash-status dash-status-${status.value}" title="${status.derived?'Derived from jobs':'Set manually'}">${status.value}</span>`}
    </div>
    <div class="dash-proj-grid-meta">
      <div>
        <div class="dash-proj-meta-label">Jobs</div>
        <div class="dash-proj-meta-val">${jobCount}</div>
      </div>
      <div>
        <div class="dash-proj-meta-label">Artifacts</div>
        <div class="dash-proj-meta-val">${artifactCount}</div>
      </div>
      <div>
        <div class="dash-proj-meta-label">Status</div>
        <div class="dash-proj-meta-val" style="color:var(--text2)">${status.value}${status.derived?' (auto)':''}</div>
      </div>
      <div>
        <div class="dash-proj-meta-label">Modified</div>
        <div class="dash-proj-meta-val">${fmt(p.modified)}</div>
      </div>
      <div>
        <div class="dash-proj-meta-label">Created</div>
        <div class="dash-proj-meta-val">${fmt(p.created)}</div>
      </div>
      <div>
        <div class="dash-proj-meta-label">Client</div>
        <div class="dash-proj-meta-val" style="color:var(--text2)">${escapeHtml(p.client || '—')}</div>
      </div>
    </div>
    <div style="display:flex;align-items:center;justify-content:space-between;gap:0.5rem;padding-top:0.55rem;border-top:0.5px solid var(--border)">
      ${isActive
        ? '<span style="font-size:10px;color:var(--accent);font-family:var(--mono);text-transform:uppercase;letter-spacing:0.06em">● active</span>'
        : `<button class="btn btn-ghost btn-sm" onclick="event.stopPropagation();setActiveProject('${escapeAttr(p.id)}')">Set as active</button>`}
      <span style="font-size:10px;color:var(--text3);font-family:var(--mono)">Open →</span>
    </div>
  </div>`;
}

// Set the project as active without navigating away from the dashboard.
// Mirrors setActiveProjectAndJump() but stays on the current view so users
// can toggle which project is active without being yanked to All Jobs.
// Only one project is active at a time — cygenix_active_project_id only ever
// holds a single id, so writing here implicitly demotes the previous one.
function setActiveProject(projectId){
  if (!projectId) return;
  try { localStorage.setItem('cygenix_active_project_id', projectId); } catch {}
  // Persist 'active' on the project record so the status field matches the
  // active-id source of truth. The previously-active project's status is
  // re-derived via dashProjStatus() — no explicit demotion write needed
  // since activeId is the source of truth for the "active" label.
  try {
    const projects = JSON.parse(localStorage.getItem('cygenix_projects') || '[]');
    const i = projects.findIndex(x => x.id === projectId);
    if (i >= 0) {
      projects[i] = { ...projects[i], status: 'active', statusManual: true, modified: new Date().toISOString() };
      localStorage.setItem('cygenix_projects', JSON.stringify(projects));
      sessionStorage.setItem('cygenix_active_project', JSON.stringify(projects[i]));
    }
  } catch {}
  // Re-render the dashboard pieces that depend on the active project so the
  // pill, summary card, and jobs table all update immediately.
  renderDashboardProjects();
  if (typeof renderAllJobs === 'function') renderAllJobs();
  if (typeof initUserAndProject === 'function') initUserAndProject();
}

function setActiveProjectAndJump(projectId){
  if (!projectId) return;
  setActiveProject(projectId);
  if (typeof showView === 'function') showView('jobs');
}

// ── Bulk-select + move jobs to project ─────────────────────────────────────
const selectedJobIds = new Set();

function onJobSelectToggle(){
  // Rebuild selectedJobIds from whatever boxes are currently ticked
  selectedJobIds.clear();
  document.querySelectorAll('.job-select-cb:checked').forEach(cb => {
    selectedJobIds.add(cb.dataset.jobId);
  });
  refreshJobsBulkBar();
}

function toggleAllJobsSelection(on){
  document.querySelectorAll('.job-select-cb').forEach(cb => {
    cb.checked = !!on;
    if (on) selectedJobIds.add(cb.dataset.jobId);
    else    selectedJobIds.delete(cb.dataset.jobId);
  });
  refreshJobsBulkBar();
}

function clearJobSelection(){
  selectedJobIds.clear();
  document.querySelectorAll('.job-select-cb').forEach(cb => cb.checked = false);
  const all = document.getElementById('jobs-select-all');
  if (all) all.checked = false;
  refreshJobsBulkBar();
}

function refreshJobsBulkBar(){
  const bar = document.getElementById('jobs-bulk-actions');
  const count = document.getElementById('jobs-selected-count');
  if (!bar) return;
  if (selectedJobIds.size === 0){
    bar.style.display = 'none';
    return;
  }
  bar.style.display = 'flex';
  count.textContent = selectedJobIds.size;

  // Populate the move-target dropdown with project options (+ "Unassigned")
  const target = document.getElementById('jobs-move-target');
  if (target && !target.options.length){
    refreshJobsMoveTargetDropdown();
  }
}

function refreshJobsMoveTargetDropdown(){
  const target = document.getElementById('jobs-move-target');
  if (!target) return;
  let projects = [];
  try { projects = JSON.parse(localStorage.getItem('cygenix_projects')||'[]'); } catch {}
  const sorted = [...projects].sort((a,b) => (a.name||'').localeCompare(b.name||''));
  target.innerHTML = '<option value="">— pick a destination —</option>'
    + sorted.map(p => `<option value="${p.id}">${(p.name||'(unnamed)').replace(/</g,'&lt;')}</option>`).join('')
    + '<option value="__unassigned">(Remove project — make unassigned)</option>';
}

function bulkMoveJobsToProject(){
  const target = document.getElementById('jobs-move-target');
  const destValue = target?.value || '';
  if (!destValue){ alert('Pick a destination project first.'); return; }
  if (!selectedJobIds.size){ return; }

  const destLabel = (destValue === '__unassigned')
    ? 'Unassigned'
    : (JSON.parse(localStorage.getItem('cygenix_projects')||'[]').find(p => p.id===destValue)?.name || destValue);
  // confirm removed — toast provides sufficient feedback

  try {
    const jobs = JSON.parse(localStorage.getItem('cygenix_jobs')||'[]');
    let moved = 0;
    for (const job of jobs){
      if (selectedJobIds.has(job.id)){
        job.projectId = (destValue === '__unassigned') ? '' : destValue;
        moved++;
      }
    }
    localStorage.setItem('cygenix_jobs', JSON.stringify(jobs));
    // Refresh local state if it's mirrored
    if (state && Array.isArray(state.jobs)){
      state.jobs = jobs;
    }
    clearJobSelection();
    renderAllJobs();
    showToast(`Moved ${moved} job(s) to ${destLabel}`);
  } catch(e){
    alert('Could not move jobs: ' + e.message);
  }
}

// ── Export Scripts ────────────────────────────────────────────────────────
// Resolves which jobs the user wants to export:
//   1. If any checkboxes are ticked  → use those jobs.
//   2. Otherwise                     → use whatever the current Project filter shows.
function getJobsForExport(){
  let all = [];
  try { all = JSON.parse(localStorage.getItem('cygenix_jobs')||'[]'); } catch { all = []; }
  if (!Array.isArray(all) || !all.length) return { jobs: [], source: 'none' };

  if (selectedJobIds && selectedJobIds.size > 0){
    return { jobs: all.filter(j => selectedJobIds.has(j.id)), source: 'selection' };
  }

  // Fall back to whatever the Project filter is currently set to
  const filterEl = document.getElementById('jobs-project-filter');
  const filterVal = filterEl ? filterEl.value : 'all';
  let activeId = '';
  try { activeId = JSON.parse(sessionStorage.getItem('cygenix_active_project')||'null')?.id || ''; } catch {}

  let filtered = all;
  if (filterVal === 'active' && activeId)       filtered = all.filter(j => j.projectId === activeId);
  else if (filterVal === 'unassigned')          filtered = all.filter(j => !j.projectId);
  else if (filterVal && filterVal !== 'all' && filterVal !== 'active'){
    filtered = all.filter(j => j.projectId === filterVal);
  }
  return { jobs: filtered, source: 'filter' };
}

function openExportScriptsModal(){
  const { jobs, source } = getJobsForExport();
  const scopeEl = document.getElementById('export-scripts-scope');
  const warnEl  = document.getElementById('exp-warn');
  const runBtn  = document.getElementById('exp-run-btn');

  if (!jobs.length){
    if (scopeEl) scopeEl.textContent = 'No jobs available to export.';
    if (warnEl){ warnEl.textContent = 'There are no jobs in the current view. Create a migration or tick rows to enable export.'; warnEl.classList.add('visible'); }
    if (runBtn) runBtn.disabled = true;
  } else {
    const label = source === 'selection'
      ? `Exporting ${jobs.length} selected job${jobs.length===1?'':'s'}.`
      : `No rows ticked — exporting all ${jobs.length} job${jobs.length===1?'':'s'} in the current filter.`;
    if (scopeEl) scopeEl.textContent = label;

    // Warn if any selected jobs have no SQL at all
    const empty = jobs.filter(j => !(j.schemaSQL || j.migrationSQL || j.insertSQL));
    if (empty.length && warnEl){
      warnEl.innerHTML = `<strong>${empty.length} job${empty.length===1?'':'s'} have no generated SQL yet</strong> — they'll be skipped: <code>${empty.slice(0,5).map(j => (j.name||'(unnamed)').replace(/</g,'&lt;')).join(', ')}${empty.length>5?'…':''}</code>`;
      warnEl.classList.add('visible');
    } else if (warnEl){
      warnEl.classList.remove('visible');
      warnEl.textContent = '';
    }
    if (runBtn) runBtn.disabled = false;
  }

  document.getElementById('export-scripts-modal').classList.add('open');
}

function closeExportScriptsModal(){
  const m = document.getElementById('export-scripts-modal');
  if (m) m.classList.remove('open');
}

// Sanitise a job name into a filesystem-safe stub
function _expSafeName(name){
  return (name || 'job').replace(/[^a-z0-9_\-]+/gi,'_').replace(/^_+|_+$/g,'').slice(0,80) || 'job';
}

// Lazy-load JSZip from cdnjs (only when needed for ZIP export)
function _loadJSZip(){
  return new Promise((resolve, reject) => {
    if (typeof window.JSZip !== 'undefined') return resolve(window.JSZip);
    const s = document.createElement('script');
    s.src = 'https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js';
    s.onload = () => resolve(window.JSZip);
    s.onerror = () => reject(new Error('Could not load JSZip from CDN.'));
    document.head.appendChild(s);
  });
}

async function runExportScripts(){
  const { jobs } = getJobsForExport();
  const includeSchema    = document.getElementById('exp-include-schema').checked;
  const includeMigration = document.getElementById('exp-include-migration').checked;
  const format = (document.querySelector('input[name="exp-format"]:checked')||{}).value || 'multi';

  if (!includeSchema && !includeMigration){
    alert('Pick at least one of Schema or Migration.');
    return;
  }
  if (!jobs.length){
    alert('No jobs to export.');
    return;
  }

  // Build {filename, content} entries — migrationSQL has the older `insertSQL` alias
  const entries = [];
  let skipped = 0;
  for (const j of jobs){
    const stub = _expSafeName(j.name);
    const schema    = j.schemaSQL || '';
    const migration = j.insertSQL || j.migrationSQL || '';
    let added = false;
    if (includeSchema && schema){
      entries.push({ filename: `${stub}_schema.sql`, content: schema, jobName: j.name, kind: 'Schema' });
      added = true;
    }
    if (includeMigration && migration){
      entries.push({ filename: `${stub}_migration.sql`, content: migration, jobName: j.name, kind: 'Migration' });
      added = true;
    }
    if (!added) skipped++;
  }

  if (!entries.length){
    alert('Nothing to export — none of the selected jobs have ' +
      (includeSchema && includeMigration ? 'schema or migration SQL.' :
       includeSchema ? 'schema SQL.' : 'migration SQL.'));
    return;
  }

  const stamp = new Date().toISOString().slice(0,19).replace(/[:T]/g,'-');

  if (format === 'single'){
    const banner = (e) => `-- ============================================================\n-- Job: ${e.jobName}\n-- Script: ${e.kind}\n-- ============================================================\n`;
    const header = `-- Cygenix combined export\n-- Generated: ${new Date().toISOString()}\n-- Scripts: ${entries.length} (${jobs.length} job${jobs.length===1?'':'s'})\n-- ============================================================\n\n`;
    const body = entries.map(e => banner(e) + '\n' + e.content + '\n').join('\nGO\n\n');
    triggerDownload(header + body, `cygenix_export_${stamp}.sql`);
    if (typeof showToast === 'function') showToast(`Exported 1 combined .sql (${entries.length} script${entries.length===1?'':'s'})`);
    if (typeof addAudit === 'function') addAudit(`Export Scripts: combined .sql with ${entries.length} script(s) from ${jobs.length} job(s)`);
  } else {
    let JSZip;
    try { JSZip = await _loadJSZip(); }
    catch(e){
      alert('Could not load ZIP library. Check your internet connection, or use the "Single combined .sql" option instead.');
      return;
    }
    const zip = new JSZip();
    const used = new Map();
    for (const e of entries){
      let fn = e.filename;
      if (used.has(fn)){
        const n = used.get(fn) + 1;
        used.set(fn, n);
        fn = fn.replace(/\.sql$/, `_${n}.sql`);
      } else {
        used.set(fn, 1);
      }
      zip.file(fn, e.content);
    }
    const manifest = [
      `Cygenix script export`,
      `Generated: ${new Date().toISOString()}`,
      `Jobs: ${jobs.length}`,
      `Files: ${entries.length}`,
      ``,
      ...entries.map((e,i) => `${String(i+1).padStart(3,' ')}. ${e.filename}  —  ${e.jobName} (${e.kind})`)
    ].join('\n');
    zip.file('_manifest.txt', manifest);

    const blob = await zip.generateAsync({ type: 'blob' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `cygenix_export_${stamp}.zip`;
    a.click();
    if (typeof showToast === 'function') showToast(`Exported ${entries.length} script${entries.length===1?'':'s'} as ZIP`);
    if (typeof addAudit === 'function') addAudit(`Export Scripts: ZIP with ${entries.length} script(s) from ${jobs.length} job(s)`);
  }

  if (skipped > 0 && typeof showToast === 'function'){
    setTimeout(() => showToast(`${skipped} job${skipped===1?'':'s'} skipped (no SQL generated)`), 800);
  }
  closeExportScriptsModal();
}

window.openExportScriptsModal  = openExportScriptsModal;
window.closeExportScriptsModal = closeExportScriptsModal;
window.runExportScripts        = runExportScripts;

// ── Bulk Generate SQL (hidden-iframe runner) ──────────────────────────────
// Strictly sequential — one iframe at a time, destroyed between jobs — to
// avoid Cosmos sync races on the shared cygenix_jobs key.
// Each iframe loads /object_mapping.html?edit=<id>&autosave=1, which on
// page load runs its normal hydration path (connect → load schema → restore
// mapping → tryAutoGenSQL/generateOTMSQL) and then calls saveAsJob() silently.
// The iframe sends a postMessage back when done or errored.
const _bulkGen = {
  running: false,
  cancelled: false,
  queue: [],
  skipped: [],      // {jobName, reason}
  results: [],      // {jobName, ok, message}
  current: null,    // {job, iframe, timeoutId}
  perJobTimeoutMs: 90000  // 90s — single big tables can take a while to fetch schema
};

function openBulkGenerateModal(){
  // Resolve scope identically to Export Scripts: selection wins, otherwise filter.
  const { jobs, source } = getJobsForExport();
  const scopeEl = document.getElementById('bulk-gen-scope');
  const warnEl  = document.getElementById('bulk-gen-warn');
  const startBtn= document.getElementById('bulk-gen-start-btn');

  // Reset state
  _bulkGen.running = false;
  _bulkGen.cancelled = false;
  _bulkGen.queue = [];
  _bulkGen.skipped = [];
  _bulkGen.results = [];
  document.getElementById('bulk-gen-progress-wrap').style.display = 'none';
  document.getElementById('bulk-gen-progress-list').innerHTML = '';
  document.getElementById('bulk-gen-status').style.display = 'none';
  document.getElementById('bulk-gen-status').textContent = '';
  startBtn.disabled = false;
  startBtn.textContent = 'Start';
  document.getElementById('bulk-gen-cancel-btn').textContent = 'Cancel';

  if (!jobs.length){
    if (scopeEl) scopeEl.textContent = 'No jobs available.';
    startBtn.disabled = true;
    document.getElementById('bulk-generate-modal').classList.add('open');
    return;
  }

  // Filter: only mapping jobs (single-map, one-to-many). SQL-editor jobs (jobType==='sql')
  // and jobs with no saved mapping are skipped with a reason.
  const runnable = [];
  for (const j of jobs){
    if (j.jobType === 'sql'){
      _bulkGen.skipped.push({ jobName: j.name, reason: 'Raw SQL job — open in SQL Editor instead' });
      continue;
    }
    // Need at least source + target to be runnable
    const hasMapping = (Array.isArray(j.columnMapping) && j.columnMapping.length > 0)
                   || (j.oneToManyConfig && Array.isArray(j.tables) && j.tables.length > 0);
    if (!hasMapping){
      _bulkGen.skipped.push({ jobName: j.name, reason: 'No saved mapping — open and configure first' });
      continue;
    }
    if (!j.sourceTable && !j.source){
      _bulkGen.skipped.push({ jobName: j.name, reason: 'No source table set' });
      continue;
    }
    runnable.push(j);
  }
  _bulkGen.queue = runnable;

  const totalIn = jobs.length;
  const skipN   = _bulkGen.skipped.length;
  const runN    = runnable.length;

  if (scopeEl){
    const scopeLabel = source === 'selection'
      ? `${totalIn} selected job${totalIn===1?'':'s'}`
      : `all ${totalIn} job${totalIn===1?'':'s'} in the current filter (nothing ticked)`;
    scopeEl.textContent = `Will run on ${scopeLabel}. ${runN} runnable, ${skipN} skipped.`;
  }

  // If any skipped, show them upfront so the user can cancel and re-pick
  if (skipN){
    const list = _bulkGen.skipped.slice(0, 8)
      .map(s => `<li><code>${(s.jobName||'(unnamed)').replace(/</g,'&lt;')}</code> — ${s.reason}</li>`).join('');
    const more = skipN > 8 ? `<li>…and ${skipN-8} more</li>` : '';
    warnEl.innerHTML = `<strong>${skipN} job${skipN===1?'':'s'} will be skipped:</strong><ul style="margin:0.4rem 0 0 1rem;padding:0">${list}${more}</ul>`;
    warnEl.classList.add('visible');
  } else {
    warnEl.innerHTML = '<strong>Before you start:</strong> connections must be configured (Dashboard → Connections). The runner uses your current source/target connections for every job.';
    warnEl.classList.add('visible');
  }

  if (runN === 0){
    startBtn.disabled = true;
    startBtn.textContent = 'Nothing to run';
  }

  document.getElementById('bulk-generate-modal').classList.add('open');
}

function closeBulkGenerateModal(){
  // If a run is active, this acts as Cancel.
  if (_bulkGen.running){
    _bulkGen.cancelled = true;
    document.getElementById('bulk-gen-status').textContent = 'Cancelling…';
    // Resolve the in-flight job's promise so the await in startBulkGenerate
    // can unblock; the loop's `if (_bulkGen.cancelled) break` then exits.
    if (_bulkGen.current){
      _bulkGenFinishCurrent(false, 'Cancelled');
    }
    return;
  }
  document.getElementById('bulk-generate-modal').classList.remove('open');
  _bulkGenCleanupIframe();
}

function _bulkGenCleanupIframe(){
  if (_bulkGen.current){
    if (_bulkGen.current.timeoutId) clearTimeout(_bulkGen.current.timeoutId);
    const ifr = _bulkGen.current.iframe;
    if (ifr && ifr.parentNode){
      // Navigate the iframe to about:blank BEFORE removing it. This forces
      // Chrome to discard the loaded page's globals (srcSchema with thousands
      // of tables, tgtSchema, MSAL session refs, etc.) immediately, instead
      // of waiting for the next GC cycle. Without this, the previous iframe's
      // 30+ MB of schema arrays can briefly overlap with the next iframe's
      // load — enough to OOM-crash a memory-pressured tab.
      try { ifr.src = 'about:blank'; } catch {}
      // Microtask delay so the navigation fires before removal; otherwise
      // some browsers skip the unload event entirely.
      setTimeout(() => {
        try { if (ifr.parentNode) ifr.parentNode.removeChild(ifr); } catch {}
      }, 0);
    }
    _bulkGen.current = null;
  }
}

function _bulkGenUpdateRow(jobId, status, message){
  const row = document.querySelector(`[data-bgrow="${jobId}"]`);
  if (!row) return;
  const icon = status === 'running' ? ''
             : status === 'ok'      ? '✓'
             : status === 'fail'    ? '✕'
             : status === 'queue'   ? '·'
             : '?';
  const colour = status === 'running' ? 'var(--accent)'
               : status === 'ok'      ? 'var(--green)'
               : status === 'fail'    ? 'var(--red)'
               : 'var(--text3)';
  row.querySelector('.bg-icon').textContent = icon;
  row.querySelector('.bg-icon').style.color = colour;
  if (message){
    const msgEl = row.querySelector('.bg-msg');
    msgEl.textContent = ' — ' + message;
    msgEl.style.color = colour;
  }
}

function _bulkGenRenderProgressList(){
  const wrap = document.getElementById('bulk-gen-progress-wrap');
  const list = document.getElementById('bulk-gen-progress-list');
  wrap.style.display = 'block';
  list.innerHTML = _bulkGen.queue.map(j => `
    <div data-bgrow="${j.id}" style="display:flex;align-items:flex-start;gap:0.5rem;padding:2px 0">
      <span class="bg-icon" style="width:14px;text-align:center;color:var(--text3)">·</span>
      <span style="flex:1;color:var(--text2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${(j.name||'(unnamed)').replace(/</g,'&lt;')}</span>
      <span class="bg-msg" style="color:var(--text3);font-size:11px"></span>
    </div>
  `).join('');
  document.getElementById('bulk-gen-status').style.display = 'block';
}

async function startBulkGenerate(){
  if (_bulkGen.running) return;
  if (!_bulkGen.queue.length) return;

  _bulkGen.running = true;
  _bulkGen.cancelled = false;
  document.getElementById('bulk-gen-start-btn').disabled = true;
  document.getElementById('bulk-gen-start-btn').textContent = 'Running…';
  document.getElementById('bulk-gen-cancel-btn').textContent = 'Cancel';
  _bulkGenRenderProgressList();

  // Replace the iframe host with a fresh DOM node before the run. Long-lived
  // hosts can accumulate detached references (drag listeners, focus handlers,
  // postMessage closures) from previous bulk runs. A clean node guarantees no
  // cross-run baggage. The original host stays in the DOM at its old position
  // but with no children — cheap and listener-free.
  (function refreshHost(){
    const oldHost = document.getElementById('bulk-gen-iframe-host');
    if (!oldHost) return;
    const fresh = oldHost.cloneNode(false);  // no children, no listeners
    oldHost.parentNode.replaceChild(fresh, oldHost);
  })();

  // Single shared postMessage listener for the lifetime of the run
  const onMsg = (ev) => {
    // Same-origin only — we don't accept results from foreign frames
    if (ev.origin !== window.location.origin) return;
    const d = ev.data;
    if (!d || d.source !== 'cygenix-autosave') return;
    if (!_bulkGen.current || d.jobId !== _bulkGen.current.job.id) return;
    // Mark current done, advance
    _bulkGenFinishCurrent(d.ok, d.message || (d.ok ? 'Done' : 'Failed'));
  };
  window.addEventListener('message', onMsg);

  for (let i = 0; i < _bulkGen.queue.length; i++){
    if (_bulkGen.cancelled) break;
    const job = _bulkGen.queue[i];

    document.getElementById('bulk-gen-status').textContent =
      `Running ${i+1} of ${_bulkGen.queue.length}: ${job.name}`;
    _bulkGenUpdateRow(job.id, 'running', 'starting…');

    await _bulkGenRunOne(job);

    // Inter-iframe pause: lets the previous iframe's about:blank navigation
    // settle and gives Chrome's GC a clear window to reclaim the schema arrays
    // before the next iframe starts loading its own. Skip on the last job —
    // no point waiting if the loop's about to exit.
    if (i < _bulkGen.queue.length - 1 && !_bulkGen.cancelled){
      await new Promise(r => setTimeout(r, 1000));
    }
  }

  window.removeEventListener('message', onMsg);
  _bulkGenCleanupIframe();
  _bulkGen.running = false;

  // Final summary
  const okN   = _bulkGen.results.filter(r => r.ok).length;
  const failN = _bulkGen.results.filter(r => !r.ok).length;
  const skipN = _bulkGen.skipped.length;
  const cancelTail = _bulkGen.cancelled ? ' (cancelled)' : '';
  document.getElementById('bulk-gen-status').innerHTML =
    `<span style="color:var(--green)">${okN} succeeded</span> · ` +
    `<span style="color:var(--red)">${failN} failed</span> · ` +
    `<span style="color:var(--text3)">${skipN} skipped</span>${cancelTail}`;
  document.getElementById('bulk-gen-start-btn').textContent = 'Done';
  document.getElementById('bulk-gen-cancel-btn').textContent = 'Close';

  if (typeof showToast === 'function'){
    showToast(`Bulk generate: ${okN} ok, ${failN} fail, ${skipN} skip${cancelTail}`);
  }
  if (typeof addAudit === 'function'){
    addAudit(`Bulk Generate SQL: ${okN} succeeded, ${failN} failed, ${skipN} skipped${cancelTail}`);
  }

  // Refresh the jobs table so updated SQL/badges show up
  if (typeof renderAllJobs === 'function'){
    try { state.jobs = JSON.parse(localStorage.getItem('cygenix_jobs')||'[]'); } catch {}
    renderAllJobs();
  }

  // Force the parent's CygenixSync to push the merged result to Cosmos.
  // Each iframe wrote to localStorage but the iframe's own push may race
  // (or be guarded). Writing here from the parent tab triggers the parent's
  // monkey-patched setItem, which queues a clean push of the final state.
  // No-op if sync is unavailable.
  try {
    const fresh = localStorage.getItem('cygenix_jobs');
    if (fresh != null) localStorage.setItem('cygenix_jobs', fresh);
  } catch {}
}

function _bulkGenRunOne(job){
  return new Promise(resolve => {
    const host = document.getElementById('bulk-gen-iframe-host');
    const iframe = document.createElement('iframe');
    iframe.src = `/object_mapping.html?edit=${encodeURIComponent(job.id)}&autosave=1`;
    // Small dimensions to minimise layout/paint cost. The autosave block
    // doesn't depend on visibility — schema fetches, mapping restoration,
    // and saveAsJob all run regardless of iframe size. Keeping this small
    // shaves memory off every iframe lifecycle.
    iframe.style.width  = '64px';
    iframe.style.height = '64px';
    iframe.style.border = '0';
    host.appendChild(iframe);

    // Hard per-job timeout. If the iframe never reports back (e.g. schema
    // fetch hangs, connection broken), we mark it failed and move on.
    const timeoutId = setTimeout(() => {
      _bulkGenFinishCurrent(false, `Timeout after ${Math.round(_bulkGen.perJobTimeoutMs/1000)}s`);
    }, _bulkGen.perJobTimeoutMs);

    _bulkGen.current = { job, iframe, timeoutId, resolve };
  });
}

function _bulkGenFinishCurrent(ok, message){
  if (!_bulkGen.current) return;
  const { job, resolve } = _bulkGen.current;
  _bulkGen.results.push({ jobName: job.name, ok, message });
  _bulkGenUpdateRow(job.id, ok ? 'ok' : 'fail', message);
  _bulkGenCleanupIframe();
  if (resolve) resolve();
}

window.openBulkGenerateModal  = openBulkGenerateModal;
window.closeBulkGenerateModal = closeBulkGenerateModal;
window.startBulkGenerate      = startBulkGenerate;

// ── Validate (dry-run INSERT against target with ROLLBACK) ────────────────
// Same selection rules as Export Scripts: ticked rows first, otherwise the
// current Project filter. Sequential — runs jobs one at a time against the
// target DB via impDbCall, the same primitive used by the data-import flow.
//
// What the wrapper SQL does, in order:
//   1. SET XACT_ABORT ON so any T-SQL error rolls back the transaction
//      automatically (rather than leaving an orphan).
//   2. BEGIN TRY / BEGIN TRAN — opens a transaction we WILL roll back.
//   3. The job's original INSERT, with TOP N injected into its SELECT so we
//      only attempt the first N source rows.
//   4. ROLLBACK TRAN — even on success. We're validating, not migrating.
//   5. CATCH block: re-ROLLBACK defensively then THROW so the driver sees
//      the real SQL Server error and we can surface it in the modal.
//
// Caveats (also shown in the modal):
//   • Identity columns burn ID values even on rollback. A TOP 100 validate
//     bumps the next real INSERT's IDs by 100.
//   • Triggers fire inside the transaction. Audit triggers that write to
//     other tables get rolled back along with everything else; queue/email
//     triggers that hit external systems via service broker DO NOT.
//   • Transaction log writes still happen and are released on rollback.
//     On a tight log file this can briefly grow it.
//   • FK references must exist in the target right now — if your migration
//     loads parents and children in the same run, validating a child
//     standalone will fail FK checks that would have passed in the real run.
function buildValidationSQL(originalSQL, sampleSize){
  const n = Math.max(1, parseInt(sampleSize, 10) || 100);
  // Inject TOP N into the first SELECT. We control the generated SQL's
  // shape (single-map: `INSERT … SELECT …`; OTM: `DECLARE cur CURSOR FOR
  // SELECT …`). Either way the first SELECT is the row source we want to
  // limit. Use case-insensitive replace, ONLY the first match.
  // Guard: if SELECT already has TOP, leave it alone.
  let injected = originalSQL;
  const m = /\bSELECT\b/i.exec(injected);
  if (m){
    // Look at the next ~30 chars after SELECT — if there's already a TOP,
    // don't add another. Otherwise inject ' TOP ' + n right after SELECT.
    const after = injected.substr(m.index + m[0].length, 30);
    if (!/^\s+TOP\b/i.test(after)){
      injected = injected.slice(0, m.index + m[0].length) + ' TOP ' + n + injected.slice(m.index + m[0].length);
    }
  }
  return [
    '-- =====================================================================',
    '-- CYGENIX VALIDATE — dry-run, always rolls back. No rows persist.',
    '-- =====================================================================',
    'SET XACT_ABORT ON;',
    'SET NOCOUNT ON;',
    'BEGIN TRY',
    ' BEGIN TRAN;',
    '',
    injected,
    '',
    ' IF @@TRANCOUNT > 0 ROLLBACK TRAN;',
    "  PRINT 'CYGENIX_VALIDATION_OK';",
    'END TRY',
    'BEGIN CATCH',
    ' IF @@TRANCOUNT > 0 ROLLBACK TRAN;',
    ' THROW;',
    'END CATCH;'
  ].join('\n');
}

const _validate = {
  running: false,
  cancelled: false,
  queue: [],
  skipped: [],
  results: [],
  current: null,
  abortController: null
};

function openValidateModal(){
  const { jobs, source } = getJobsForExport();   // reuses Export's selection
  const scopeEl = document.getElementById('validate-scope');
  const startBtn = document.getElementById('validate-start-btn');

  // Reset state
  _validate.running = false;
  _validate.cancelled = false;
  _validate.queue = [];
  _validate.skipped = [];
  _validate.results = [];
  document.getElementById('validate-progress-wrap').style.display = 'none';
  document.getElementById('validate-progress-list').innerHTML = '';
  document.getElementById('validate-status').style.display = 'none';
  document.getElementById('validate-status').textContent = '';
  startBtn.disabled = false;
  startBtn.textContent = 'Start';
  document.getElementById('validate-cancel-btn').textContent = 'Cancel';

  if (!jobs.length){
    if (scopeEl) scopeEl.textContent = 'No jobs available to validate.';
    startBtn.disabled = true;
    document.getElementById('validate-modal').classList.add('open');
    return;
  }

  // Runnable filter: must have generated SQL. Raw-SQL jobs are skipped (their
  // SQL may be arbitrary — wrapping in a tran could break it).
  const runnable = [];
  for (const j of jobs){
    const sql = j.insertSQL || j.migrationSQL || '';
    if (!sql){
      _validate.skipped.push({ jobName: j.name, reason: 'No generated SQL — run Generate first' });
      continue;
    }
    if (j.jobType === 'sql'){
      _validate.skipped.push({ jobName: j.name, reason: 'Raw-SQL job — run manually in SQL Editor' });
      continue;
    }
    runnable.push(j);
  }
  _validate.queue = runnable;

  const totalIn = jobs.length;
  const skipN = _validate.skipped.length;
  const runN  = runnable.length;
  if (scopeEl){
    const scopeLabel = source === 'selection'
      ? `${totalIn} selected job${totalIn===1?'':'s'}`
      : `all ${totalIn} job${totalIn===1?'':'s'} in the current filter (nothing ticked)`;
    scopeEl.textContent = `Will validate on ${scopeLabel}. ${runN} runnable, ${skipN} skipped.`;
  }

  if (runN === 0){
    startBtn.disabled = true;
    startBtn.textContent = 'Nothing to validate';
  }

  document.getElementById('validate-modal').classList.add('open');
}

function closeValidateModal(){
  // Cancel an in-flight run if active.
  if (_validate.running){
    _validate.cancelled = true;
    document.getElementById('validate-status').textContent = 'Cancelling…';
    // Abort the in-flight fetch so the loop unblocks.
    try { _validate.abortController?.abort(); } catch {}
    return;
  }
  document.getElementById('validate-modal').classList.remove('open');
}

function _validateRenderList(){
  const wrap = document.getElementById('validate-progress-wrap');
  const list = document.getElementById('validate-progress-list');
  wrap.style.display = 'block';
  list.innerHTML = _validate.queue.map(j => `
    <div data-vrow="${j.id}" style="display:flex;align-items:flex-start;gap:0.5rem;padding:2px 0">
      <span class="v-icon" style="width:14px;text-align:center;color:var(--text3)">·</span>
      <span style="flex:1;color:var(--text2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${(j.name||'(unnamed)').replace(/</g,'&lt;')}</span>
      <span class="v-msg" style="color:var(--text3);font-size:11px"></span>
    </div>
  `).join('');
  document.getElementById('validate-status').style.display = 'block';
}

function _validateUpdateRow(jobId, status, message){
  const row = document.querySelector(`[data-vrow="${jobId}"]`);
  if (!row) return;
  const icon = status === 'running' ? ''
             : status === 'ok'      ? '✓'
             : status === 'fail'    ? '✕'
             : '·';
  const colour = status === 'running' ? 'var(--accent)'
               : status === 'ok'      ? 'var(--green)'
               : status === 'fail'    ? 'var(--red)'
               : 'var(--text3)';
  const ic = row.querySelector('.v-icon');
  ic.textContent = icon; ic.style.color = colour;
  if (message){
    const m = row.querySelector('.v-msg');
    m.textContent = ' — ' + message;
    m.style.color = colour;
    // For long error messages, show full text on hover.
    m.title = message;
  }
}

// ── Topology helpers for cross-DB validation ──────────────────────────────
// The generated migration SQL uses bare 2-part names ([dbo].[Tbl]) for both
// source and target — designed to run in a single-server context. Our two
// connections might point at different databases on the same server (common)
// or at different servers entirely (also valid). Validation has to bridge
// that, so we probe both connections first and pick the right strategy.
async function _validateProbeTopology(srcConn, tgtConn){
  async function probe(conn){
    if (!conn) return { server: '', db: '', error: 'no connection configured' };
    try {
      const res = await impDbCall(conn, {
        action: 'execute',
        sql: "SELECT CAST(@@SERVERNAME AS NVARCHAR(255)) AS srv, CAST(DB_NAME() AS NVARCHAR(255)) AS db;"
      });
      // Response shape varies by backend: the Netlify db-connect proxy returns
      // results under `recordset`, the direct Azure-Function path under `rows`.
      // Read whichever is present.
      const rs = res?.recordset || res?.rows || [];
      const row = rs[0] || {};
      const server = String(row.srv || row.SRV || row.server || Object.values(row)[0] || '').trim();
      const db     = String(row.db  || row.DB  || row.database || Object.values(row)[1] || '').trim();
      // Empty result is a silent failure mode worth surfacing — without it
      // we'd misclassify as "different server" downstream. The caller can
      // show this to the user instead of guessing at the cause.
      if (!server && !db){
        const hint = res?.error ? String(res.error) :
                     (rs.length === 0 ? 'probe query returned no rows' :
                      'probe query returned a row with no recognised columns ('+Object.keys(row).join(',')+')');
        return { server: '', db: '', error: hint };
      }
      return { server, db };
    } catch (e) {
      return { server: '', db: '', error: e.message || String(e) };
    }
  }
  const [src, tgt] = await Promise.all([probe(srcConn), probe(tgtConn)]);
  // sameServer is meaningful only when both reads succeed AND both servers
  // are non-empty. An empty string match would falsely conclude "same".
  const sameServer = !!(src.server && tgt.server && src.server.toLowerCase() === tgt.server.toLowerCase());
  return { sameServer, src, tgt };
}

// Rewrite the bare 2-part source-table references in the generated SQL to
// 3-part [srcDb].[schema].[name] so the validation can run on the target
// connection and still resolve the source table via cross-database access
// (works when both DBs live on the same SQL Server instance, which is the
// common Cygenix Kennedys-style topology).
//
// We do a targeted, word-bounded replacement of the EXACT 2-part literal
// the generator emits. We deliberately don't try to parse the SQL — false
// positives on a coincidentally-named target table would be far worse than
// missing an edge case. If the user has joins to other source-side tables,
// those have their own references and would need similar treatment — for
// now we cover only the primary source table, which is what the failure
// mode in the field shows ('Invalid object name dbo.Client_DM').
function _validateRewriteForCrossDb(sql, sourceTableFull, srcDb){
  if (!srcDb || !sourceTableFull) return sql;
  // Build the bare and qualified forms. sourceTableFull is "schema.name".
  const parts = String(sourceTableFull).split('.');
  if (parts.length !== 2) return sql;
  const schema = parts[0];
  const name   = parts[1];
  // The generator quotes with square brackets — match that exact form.
  // Escape special regex chars in case schema/table names contain them.
  const esc = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const bare = `\\[${esc(schema)}\\]\\.\\[${esc(name)}\\]`;
  const qualified = `[${srcDb}].[${schema}].[${name}]`;
  // Don't rewrite if it's already 3-part somewhere in the SQL — defensive.
  const alreadyQualified = new RegExp(`\\[[^\\]]+\\]\\.\\[${esc(schema)}\\]\\.\\[${esc(name)}\\]`, 'i');
  if (alreadyQualified.test(sql)) return sql;
  const re = new RegExp(bare, 'gi');
  return sql.replace(re, qualified);
}

async function startValidate(){
  if (_validate.running) return;
  if (!_validate.queue.length) return;

  // Need both connections: target to RUN against, source to PROBE for db
  // name (and as fallback for cross-server data movement).
  const tgtConn = (typeof impGetConn === 'function') ? impGetConn('tgt') : '';
  const srcConn = (typeof impGetConn === 'function') ? impGetConn('src') : '';
  if (!tgtConn){
    alert('No target database connection configured. Set one in Dashboard → Connections.');
    return;
  }
  if (!srcConn){
    alert('No source database connection configured. The validator needs to discover the source database name to qualify cross-database references. Set the source in Dashboard → Connections.');
    return;
  }

  const sampleSize = parseInt(document.getElementById('validate-sample-size').value, 10) || 100;

  _validate.running = true;
  _validate.cancelled = false;
  document.getElementById('validate-start-btn').disabled = true;
  document.getElementById('validate-start-btn').textContent = 'Probing…';
  _validateRenderList();
  document.getElementById('validate-status').textContent = 'Checking source/target topology…';

  // Probe topology once for the whole run
  let topo;
  try {
    topo = await _validateProbeTopology(srcConn, tgtConn);
  } catch (e){
    document.getElementById('validate-status').innerHTML =
      `<span style="color:var(--red)">Could not probe connections: ${(e.message||e).toString().slice(0,200)}</span>`;
    document.getElementById('validate-start-btn').textContent = 'Failed';
    document.getElementById('validate-cancel-btn').textContent = 'Close';
    _validate.running = false;
    return;
  }
  if (topo.src.error || topo.tgt.error){
    const errs = [];
    if (topo.src.error) errs.push('Source: ' + topo.src.error);
    if (topo.tgt.error) errs.push('Target: ' + topo.tgt.error);
    document.getElementById('validate-status').innerHTML =
      `<span style="color:var(--red)">Topology probe failed — ${errs.join(' · ').slice(0,300)}</span>`;
    document.getElementById('validate-start-btn').textContent = 'Failed';
    document.getElementById('validate-cancel-btn').textContent = 'Close';
    _validate.running = false;
    return;
  }

  document.getElementById('validate-start-btn').textContent = 'Running…';

  // Pick a mode. "same-server" = run wrapped SQL on target, with source refs
  // rewritten to 3-part. "cross-server" = not implemented in this build —
  // fail with clear guidance so the user can set up a linked server or
  // request the literal-values fallback if that's not viable.
  const mode = topo.sameServer ? 'same-server' : 'cross-server';
  const banner = (mode === 'same-server')
    ? `Same-server topology (${topo.src.server || '?'}) — source [${topo.src.db}] qualified into target connection.`
    : `Different servers detected (src: ${topo.src.server || '?'}, tgt: ${topo.tgt.server || '?'}). Validation will be skipped — set up a linked server or run validation manually for now.`;

  document.getElementById('validate-status').textContent = banner;

  if (mode !== 'same-server'){
    // Mark every queued job as skipped with a clear reason.
    for (const j of _validate.queue){
      _validateUpdateRow(j.id, 'fail', 'Skipped — different-server topology not yet supported');
      _validate.results.push({ jobName: j.name, ok: false, message: 'Skipped — different-server topology' });
    }
    document.getElementById('validate-start-btn').textContent = 'Done';
    document.getElementById('validate-cancel-btn').textContent = 'Close';
    _validate.running = false;
    if (typeof showToast === 'function') showToast('Validate skipped: cross-server topology');
    return;
  }

  for (let i = 0; i < _validate.queue.length; i++){
    if (_validate.cancelled) break;
    const job = _validate.queue[i];
    document.getElementById('validate-status').textContent =
      `Validating ${i+1} of ${_validate.queue.length}: ${job.name} · ${banner}`;
    _validateUpdateRow(job.id, 'running', 'running…');

    const rawSql = job.insertSQL || job.migrationSQL || '';
    // Rewrite source 2-part → 3-part using the discovered source db name.
    // The job stores the source table as "schema.name" in sourceTable/source.
    const srcTableFull = job.sourceTable || job.source || '';
    const rewritten = _validateRewriteForCrossDb(rawSql, srcTableFull, topo.src.db);

    // If the rewrite changed the SQL, save it back to the job permanently
    // so future runs don't need rewriting and the SQL editor shows the correct SQL.
    if (rewritten !== rawSql) {
      try {
        const allJobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]');
        const idx = allJobs.findIndex(j => j.id === job.id);
        if (idx !== -1) {
          allJobs[idx].insertSQL = rewritten;
          job.insertSQL = rewritten; // update in-memory too
          localStorage.setItem('cygenix_jobs', JSON.stringify(allJobs));
          if (state && Array.isArray(state.jobs)) {
            const si = state.jobs.findIndex(j => j.id === job.id);
            if (si !== -1) state.jobs[si].insertSQL = rewritten;
          }
        }
      } catch(saveErr) { console.warn('Could not save rewritten SQL:', saveErr.message); }
    }
    const wrapped = buildValidationSQL(rewritten, sampleSize);

    try {
      _validate.abortController = new AbortController();
      const res = await impDbCall(tgtConn, { action: 'execute', sql: wrapped });
      if (res && res.error){
        _validate.results.push({ jobName: job.name, ok: false, message: String(res.error).slice(0, 400) });
        _validateUpdateRow(job.id, 'fail', String(res.error).slice(0, 200));
      } else {
        _validate.results.push({ jobName: job.name, ok: true, message: 'OK' });
        _validateUpdateRow(job.id, 'ok', `OK (${sampleSize} rows, rolled back)`);
      }
    } catch (e){
      const msg = (e && (e.name === 'AbortError') ? 'Cancelled' : (e?.message || String(e))).slice(0, 400);
      _validate.results.push({ jobName: job.name, ok: false, message: msg });
      _validateUpdateRow(job.id, 'fail', msg.slice(0, 200));
    }
  }

  _validate.running = false;
  _validate.abortController = null;

  const okN   = _validate.results.filter(r => r.ok).length;
  const failN = _validate.results.filter(r => !r.ok).length;
  const skipN = _validate.skipped.length;
  const tail  = _validate.cancelled ? ' (cancelled)' : '';
  document.getElementById('validate-status').innerHTML =
    `<span style="color:var(--green)">${okN} passed</span> · ` +
    `<span style="color:var(--red)">${failN} failed</span> · ` +
    `<span style="color:var(--text3)">${skipN} skipped</span>${tail} · ` +
    `<span style="color:var(--text3)">${mode}</span>`;
  document.getElementById('validate-start-btn').textContent = 'Done';
  document.getElementById('validate-cancel-btn').textContent = 'Close';

  if (typeof showToast === 'function') showToast(`Validate: ${okN} pass, ${failN} fail, ${skipN} skip${tail}`);
  if (typeof addAudit === 'function') addAudit(`Validate: ${okN} passed, ${failN} failed, ${skipN} skipped${tail} (sample ${sampleSize}, ${mode})`);
}

window.openValidateModal  = openValidateModal;
window.closeValidateModal = closeValidateModal;
window.startValidate      = startValidate;
window.buildValidationSQL = buildValidationSQL;  // exposed for unit-testing from console
window._validateProbeTopology    = _validateProbeTopology;
window._validateRewriteForCrossDb = _validateRewriteForCrossDb;

// ── Setup Check (lookup-table integrity report) ───────────────────────────
//
// A "setup" in this system's domain language is a target reference / lookup
// table: e.g. dbo.CliStatusType holds the valid statuses, and dbo.Client.
// CliStatusType is a column that FK-references it by Code. The convention
// for identifying setup fields is: a target column name that exactly
// matches some target table name.
//
// This report walks every selected job and, for every column mapping where
// tgtCol matches a target table name, runs a single SQL diff:
//
//   SELECT s.<srcCol> AS bad_value, COUNT(*) AS row_count
//   FROM [srcDb].[dbo].[srcTable] s
//   LEFT JOIN [tgtDb].[dbo].[setupTable] t ON t.[Code] = s.<srcCol>
//   WHERE t.[Code] IS NULL OR LTRIM(RTRIM(ISNULL(s.<srcCol>,''))) = ''
//   GROUP BY s.<srcCol>
//   ORDER BY COUNT(*) DESC
//   OFFSET 0 ROWS FETCH NEXT @topN ROWS ONLY;
//
// Memory model:
//   • One query per setup field, sequential. Source DB isn't hammered.
//   • Each query GROUP BYs and caps to topN — result set is tens to low-
//     hundreds of rows, never millions. We never pull source row data.
//   • Results held in JS only long enough to render + persist; nothing
//     leaks through closures.
//   • Saved to Cosmos under cygenix_setup_reports as a single keyed entry
//     (last run per project). Old runs are overwritten — keeps the key
//     under the 5MB Cosmos limit and avoids unbounded growth.
//
// Same topology constraint as Validate: source and target must be on the
// same SQL Server instance (we use 3-part qualified names; cross-server
// would need a linked server, which we don't auto-detect yet).

const _setupCheck = {
  running: false,
  cancelled: false,
  jobs: [],       // queue of jobs to scan
  topo: null,     // { sameServer, src:{server,db}, tgt:{server,db} }
  tgtTableSet: null,  // Set<lowercase-tablename>
  fields: [],     // queue of {jobId, jobName, srcTable, srcCol, tgtCol, setupTable}
  results: [],    // per-field results: {...field, badValues:[{value,count}], totalBad, error?}
  abortController: null
};

function openSetupCheckModal(){
  const { jobs, source } = getJobsForExport();   // reuses Export's selection
  const scopeEl = document.getElementById('setup-check-scope');
  const startBtn = document.getElementById('setup-check-start-btn');

  // Reset state
  _setupCheck.running = false;
  _setupCheck.cancelled = false;
  _setupCheck.jobs = [];
  _setupCheck.fields = [];
  _setupCheck.results = [];
  _setupCheck.tgtTableSet = null;
  document.getElementById('setup-check-progress-wrap').style.display = 'none';
  document.getElementById('setup-check-progress-list').innerHTML = '';
  document.getElementById('setup-check-results-wrap').style.display = 'none';
  document.getElementById('setup-check-results').innerHTML = '';
  document.getElementById('setup-check-status').style.display = 'none';
  document.getElementById('setup-check-status').textContent = '';
  document.getElementById('setup-check-export-btn').style.display = 'none';
  startBtn.disabled = false;
  startBtn.textContent = 'Start';
  document.getElementById('setup-check-cancel-btn').textContent = 'Cancel';

  if (!jobs.length){
    if (scopeEl) scopeEl.textContent = 'No jobs available for setup check.';
    startBtn.disabled = true;
    document.getElementById('setup-check-modal').classList.add('open');
    return;
  }

  // Filter: need a columnMapping to scan, and a source table.
  const runnable = jobs.filter(j =>
    Array.isArray(j.columnMapping) && j.columnMapping.length > 0 &&
    (j.sourceTable || j.source)
  );
  _setupCheck.jobs = runnable;

  if (scopeEl){
    const scopeLabel = source === 'selection'
      ? `${jobs.length} selected job${jobs.length===1?'':'s'}`
      : `all ${jobs.length} job${jobs.length===1?'':'s'} in the current filter (nothing ticked)`;
    scopeEl.textContent = `Scanning ${runnable.length}/${jobs.length} (skipping jobs with no mapping). ` +
      `For each, finds tgtCol names that match a target table — those are setup fields.`;
  }
  if (runnable.length === 0){
    startBtn.disabled = true;
    startBtn.textContent = 'Nothing to scan';
  }
  document.getElementById('setup-check-modal').classList.add('open');
}

function closeSetupCheckModal(){
  if (_setupCheck.running){
    _setupCheck.cancelled = true;
    document.getElementById('setup-check-status').textContent = 'Cancelling…';
    try { _setupCheck.abortController?.abort(); } catch {}
    return;
  }
  document.getElementById('setup-check-modal').classList.remove('open');
}

// Fetch the target DB's table-name list once. Lightweight query — we only
// need names, not columns. Returns a Set<lowercase-tablename> for fast
// lookups during the per-job scan.
async function _setupFetchTargetTables(tgtConn){
  const sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";
  const res = await impDbCall(tgtConn, { action: 'execute', sql });
  const rows = res?.recordset || res?.rows || [];
  const set = new Set();
  for (const r of rows){
    const name = r?.TABLE_NAME || r?.table_name || Object.values(r||{})[0];
    if (name) set.add(String(name).toLowerCase());
  }
  return set;
}

// Quote a SQL Server identifier safely (strip ] characters).
function _sqlIdent(s){ return '['+String(s||'').replace(/]/g,'')+']'; }

// Build the diff SQL for one setup field. Source table is qualified with
// the source DB; setup table is qualified with the target DB. Both live on
// the same instance per the topology probe — cross-database query works.
//
// joinKey is the column on the setup table to compare against (default 'Code'
// — overridden when _setupResolveJoinKey discovers a different convention).
//
// COLLATE DATABASE_DEFAULT on both sides of the comparison is required when
// source DB and target DB declare different collations. Without it SQL
// Server raises "Cannot resolve the collation conflict between …". The
// DATABASE_DEFAULT pseudo-collation evaluates to whichever the current
// connection's default is, which neutralises the mismatch for the comparison.
function _setupDiffSQL(srcDb, tgtDb, srcTable, srcCol, setupTable, joinKey, includeEmpty, topN){
  // srcTable may be schema-qualified ("dbo.Client_DM"). Split into parts.
  const srcParts = String(srcTable).split('.');
  const srcSchema = srcParts.length === 2 ? srcParts[0] : 'dbo';
  const srcName   = srcParts.length === 2 ? srcParts[1] : srcParts[0];
  const srcRef = _sqlIdent(srcDb)+'.'+_sqlIdent(srcSchema)+'.'+_sqlIdent(srcName);
  const tgtRef = _sqlIdent(tgtDb)+'.[dbo].'+_sqlIdent(setupTable);
  const srcColRef = _sqlIdent(srcCol);
  const keyCol = _sqlIdent(joinKey || 'Code');
  // Cast both sides to NVARCHAR(255) for type safety AND apply DATABASE_DEFAULT
  // collation so cross-DB collation differences don't error.
  const srcExpr = 'CAST(s.'+srcColRef+" AS NVARCHAR(255)) COLLATE DATABASE_DEFAULT";
  const tgtExpr = 'CAST(t.'+keyCol+" AS NVARCHAR(255)) COLLATE DATABASE_DEFAULT";
  const wherePred = includeEmpty
    ? "(t."+keyCol+" IS NULL OR LTRIM(RTRIM(ISNULL(CAST(s."+srcColRef+" AS NVARCHAR(MAX)),''))) = '')"
    : "(t."+keyCol+" IS NULL AND LTRIM(RTRIM(ISNULL(CAST(s."+srcColRef+" AS NVARCHAR(MAX)),''))) <> '')";
  return [
    "SELECT TOP "+topN+" CAST(s."+srcColRef+" AS NVARCHAR(255)) AS bad_value, COUNT(*) AS row_count",
    "FROM "+srcRef+" s",
    "LEFT JOIN "+tgtRef+" t ON "+tgtExpr+" = "+srcExpr,
    "WHERE "+wherePred,
    "GROUP BY CAST(s."+srcColRef+" AS NVARCHAR(255))",
    "ORDER BY COUNT(*) DESC;"
  ].join('\n');
}

// Discover which column on a setup table to join against. Tries the common
// 3E conventions in order and returns the first match found. Caches per
// table so we don't hammer the target DB for repeated lookups.
//
// Conventions tried (in order):
//   1. 'Code'             — the dominant 3E convention
//   2. '<TableName>Code'  — e.g. Entity.EntityCode, Language.LanguageCode
//   3. '<TableName>'      — table named after its own key column
//   4. Primary key column — fallback for non-standard tables
const _setupKeyCache = new Map();  // table.toLowerCase() → resolved column name OR null
async function _setupResolveJoinKey(tgtConn, tgtDb, setupTable){
  const cacheKey = String(setupTable).toLowerCase();
  if (_setupKeyCache.has(cacheKey)) return _setupKeyCache.get(cacheKey);

  // Single query asks: which of our candidate names exists as a column on
  // this table? Returns the matches in priority order via the ORDER BY.
  const candidates = ['Code', setupTable + 'Code', setupTable];
  const candidateList = candidates.map(c => "'"+c.replace(/'/g,"''")+"'").join(',');
  const sql =
    "SELECT c.name AS col_name " +
    "FROM "+_sqlIdent(tgtDb)+".sys.columns c " +
    "INNER JOIN "+_sqlIdent(tgtDb)+".sys.tables t ON t.object_id = c.object_id " +
    "INNER JOIN "+_sqlIdent(tgtDb)+".sys.schemas s ON s.schema_id = t.schema_id " +
    "WHERE s.name = 'dbo' AND t.name = '"+String(setupTable).replace(/'/g,"''")+"' " +
    "AND c.name IN ("+candidateList+");";
  try {
    const res = await impDbCall(tgtConn, { action: 'execute', sql });
    const rows = res?.recordset || res?.rows || [];
    const found = new Set(rows.map(r => String(r.col_name||r.COL_NAME||Object.values(r)[0]).toLowerCase()));
    // Resolve in priority order
    for (const cand of candidates){
      if (found.has(cand.toLowerCase())){
        _setupKeyCache.set(cacheKey, cand);
        return cand;
      }
    }
  } catch (e){
    // Probe failure shouldn't kill the whole run; caller treats null as
    // "no key — skip this setup with a clear error".
  }
  _setupKeyCache.set(cacheKey, null);
  return null;
}

async function startSetupCheck(){
  if (_setupCheck.running) return;
  if (!_setupCheck.jobs.length) return;

  const srcConn = (typeof impGetConn === 'function') ? impGetConn('src') : '';
  const tgtConn = (typeof impGetConn === 'function') ? impGetConn('tgt') : '';
  if (!srcConn || !tgtConn){
    alert('Both source and target connections must be configured (Dashboard → Connections).');
    return;
  }

  const includeEmpty = document.getElementById('setup-check-empty-too').checked;
  const topN = parseInt(document.getElementById('setup-check-topn').value, 10) || 100;

  _setupCheck.running = true;
  _setupCheck.cancelled = false;
  document.getElementById('setup-check-start-btn').disabled = true;
  document.getElementById('setup-check-start-btn').textContent = 'Probing…';
  document.getElementById('setup-check-status').style.display = 'block';
  document.getElementById('setup-check-status').textContent = 'Probing topology + loading target table list…';
  document.getElementById('setup-check-progress-wrap').style.display = 'block';
  document.getElementById('setup-check-progress-list').innerHTML = '';

  // Step 1: topology probe (re-use validate's prober)
  let topo;
  try { topo = await _validateProbeTopology(srcConn, tgtConn); }
  catch (e){
    document.getElementById('setup-check-status').innerHTML =
      `<span style="color:var(--red)">Topology probe failed: ${(e.message||e).toString().slice(0,200)}</span>`;
    document.getElementById('setup-check-start-btn').textContent = 'Failed';
    document.getElementById('setup-check-cancel-btn').textContent = 'Close';
    _setupCheck.running = false;
    return;
  }
  // Surface probe-level errors BEFORE checking sameServer — without this,
  // a silent probe failure (no rows, missing column, auth quirk) would be
  // misclassified as "different-server topology" and we'd show a wrong
  // explanation. Validate's flow has the same guard; this matches it.
  if (topo.src.error || topo.tgt.error){
    const errs = [];
    if (topo.src.error) errs.push('Source: ' + topo.src.error);
    if (topo.tgt.error) errs.push('Target: ' + topo.tgt.error);
    document.getElementById('setup-check-status').innerHTML =
      `<span style="color:var(--red)">Topology probe failed — ${errs.join(' · ').slice(0,400)}</span>`;
    document.getElementById('setup-check-start-btn').textContent = 'Failed';
    document.getElementById('setup-check-cancel-btn').textContent = 'Close';
    _setupCheck.running = false;
    return;
  }
  if (!topo.sameServer){
    document.getElementById('setup-check-status').innerHTML =
      `<span style="color:var(--red)">Different-server topology (src=${topo.src.server||'?'}, tgt=${topo.tgt.server||'?'}) — Setup Check needs source and target on the same SQL Server instance to do cross-DB joins.</span>`;
    document.getElementById('setup-check-start-btn').textContent = 'Skipped';
    document.getElementById('setup-check-cancel-btn').textContent = 'Close';
    _setupCheck.running = false;
    return;
  }
  _setupCheck.topo = topo;

  // Step 2: target table list (one cheap query, cached for this run)
  let tgtTableSet;
  try { tgtTableSet = await _setupFetchTargetTables(tgtConn); }
  catch (e){
    document.getElementById('setup-check-status').innerHTML =
      `<span style="color:var(--red)">Could not load target table list: ${(e.message||e).toString().slice(0,200)}</span>`;
    document.getElementById('setup-check-start-btn').textContent = 'Failed';
    document.getElementById('setup-check-cancel-btn').textContent = 'Close';
    _setupCheck.running = false;
    return;
  }
  _setupCheck.tgtTableSet = tgtTableSet;
  if (!tgtTableSet.size){
    document.getElementById('setup-check-status').innerHTML =
      `<span style="color:var(--red)">Target DB returned 0 tables — check permissions.</span>`;
    document.getElementById('setup-check-start-btn').textContent = 'Failed';
    document.getElementById('setup-check-cancel-btn').textContent = 'Close';
    _setupCheck.running = false;
    return;
  }

  // Step 3: build the per-field queue. A "setup field" is a column mapping
  // where tgtCol (case-insensitive) is the name of a target table.
  const fields = [];
  for (const j of _setupCheck.jobs){
    const srcTable = j.sourceTable || j.source || '';
    if (!srcTable) continue;
    for (const m of (j.columnMapping || [])){
      if (!m.srcCol || !m.tgtCol) continue;
      // Skip literal-value rows (no real source column to scan)
      if (m.literalValue) continue;
      const tgtLower = m.tgtCol.toLowerCase();
      if (tgtTableSet.has(tgtLower)){
        fields.push({
          jobId: j.id, jobName: j.name,
          srcTable,
          srcCol: m.srcCol,
          tgtCol: m.tgtCol,
          setupTable: m.tgtCol  // by definition — name matches
        });
      }
    }
  }
  _setupCheck.fields = fields;

  if (!fields.length){
    document.getElementById('setup-check-status').innerHTML =
      `<span style="color:var(--text3)">No setup fields found in the selected jobs (no tgtCol matches any target table name).</span>`;
    document.getElementById('setup-check-start-btn').textContent = 'Done';
    document.getElementById('setup-check-cancel-btn').textContent = 'Close';
    _setupCheck.running = false;
    return;
  }

  // Render progress list
  const listEl = document.getElementById('setup-check-progress-list');
  listEl.innerHTML = fields.map((f,i) => `
    <div data-sfrow="${i}" style="display:flex;align-items:flex-start;gap:0.5rem;padding:2px 0">
      <span class="sf-icon" style="width:14px;text-align:center;color:var(--text3)">·</span>
      <span style="flex:1;color:var(--text2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
        <span style="color:var(--text3)">${(f.jobName||'').replace(/</g,'&lt;')}</span>
        <span style="color:var(--text3)">·</span>
        ${(f.srcCol||'').replace(/</g,'&lt;')}
        <span style="color:var(--text3)">→</span>
        ${(f.setupTable||'').replace(/</g,'&lt;')}
      </span>
      <span class="sf-msg" style="color:var(--text3);font-size:11px"></span>
    </div>
  `).join('');

  document.getElementById('setup-check-start-btn').textContent = 'Running…';

  // Step 4: run each diff query sequentially
  for (let i = 0; i < fields.length; i++){
    if (_setupCheck.cancelled) break;
    const f = fields[i];
    document.getElementById('setup-check-status').textContent =
      `Checking ${i+1} of ${fields.length}: ${f.jobName} · ${f.srcCol} → ${f.setupTable}`;
    _setupCheckUpdateRow(i, 'running', 'resolving key…');

    // Resolve the setup's join key (Code / <Table>Code / <Table>). Cached
    // per-table for the run, so the second job referencing the same setup
    // is free.
    let joinKey;
    try {
      joinKey = await _setupResolveJoinKey(tgtConn, topo.tgt.db, f.setupTable);
    } catch (e) {
      joinKey = null;
    }
    if (!joinKey){
      const msg = `No recognised key column on ${f.setupTable} (tried Code, ${f.setupTable}Code, ${f.setupTable})`;
      _setupCheck.results.push({ ...f, badValues: [], totalBad: 0, error: msg, joinKey: null });
      _setupCheckUpdateRow(i, 'err', 'no key column');
      continue;
    }
    f.joinKey = joinKey;

    _setupCheckUpdateRow(i, 'running', 'querying ('+joinKey+')…');
    const sql = _setupDiffSQL(topo.src.db, topo.tgt.db, f.srcTable, f.srcCol, f.setupTable, joinKey, includeEmpty, topN);
    try {
      _setupCheck.abortController = new AbortController();
      const res = await impDbCall(srcConn, { action: 'execute', sql });
      const rows = res?.recordset || res?.rows || [];
      const badValues = rows.map(r => ({
        value: r.bad_value === null || r.bad_value === undefined ? '(NULL)' : String(r.bad_value),
        count: Number(r.row_count) || 0
      }));
      const totalBad = badValues.reduce((s,v) => s + v.count, 0);
      _setupCheck.results.push({ ...f, badValues, totalBad, joinKey });
      _setupCheckUpdateRow(i,
        totalBad > 0 ? 'fail' : 'ok',
        totalBad > 0 ? `${totalBad.toLocaleString()} bad row${totalBad===1?'':'s'}, ${badValues.length} distinct (key=${joinKey})` : `clean (key=${joinKey})`
      );
    } catch (e){
      const msg = (e?.message || String(e)).slice(0, 300);
      _setupCheck.results.push({ ...f, badValues: [], totalBad: 0, error: msg, joinKey });
      _setupCheckUpdateRow(i, 'err', 'error: ' + msg.slice(0,80));
    }
  }

  _setupCheck.running = false;
  _setupCheck.abortController = null;

  _setupCheckRenderResults();
  _setupCheckPersist();

  const cleanN  = _setupCheck.results.filter(r => !r.error && r.totalBad === 0).length;
  const dirtyN  = _setupCheck.results.filter(r => !r.error && r.totalBad > 0).length;
  const errN    = _setupCheck.results.filter(r => r.error).length;
  const tail    = _setupCheck.cancelled ? ' (cancelled)' : '';
  document.getElementById('setup-check-status').innerHTML =
    `<span style="color:var(--green)">${cleanN} clean</span> · ` +
    `<span style="color:var(--amber)">${dirtyN} have bad values</span> · ` +
    `<span style="color:var(--red)">${errN} errored</span>${tail}`;
  document.getElementById('setup-check-start-btn').textContent = 'Done';
  document.getElementById('setup-check-cancel-btn').textContent = 'Close';
  document.getElementById('setup-check-export-btn').style.display = (dirtyN+errN) > 0 ? '' : 'none';

  if (typeof showToast === 'function') showToast(`Setup Check: ${cleanN} clean, ${dirtyN} dirty, ${errN} err${tail}`);
  if (typeof addAudit === 'function') addAudit(`Setup Check: ${cleanN} clean, ${dirtyN} dirty, ${errN} errored${tail} across ${fields.length} setup fields`);
}

function _setupCheckUpdateRow(idx, status, message){
  const row = document.querySelector(`[data-sfrow="${idx}"]`);
  if (!row) return;
  const icon = status === 'running' ? ''
             : status === 'ok'      ? '✓'
             : status === 'fail'    ? '✕'
             : status === 'err'     ? '!'
             : '·';
  const colour = status === 'running' ? 'var(--accent)'
               : status === 'ok'      ? 'var(--green)'
               : status === 'fail'    ? 'var(--amber)'
               : status === 'err'     ? 'var(--red)'
               : 'var(--text3)';
  const ic = row.querySelector('.sf-icon');
  ic.textContent = icon; ic.style.color = colour;
  const msg = row.querySelector('.sf-msg');
  msg.textContent = ' — ' + (message||'');
  msg.style.color = colour;
}

function _setupCheckRenderResults(){
  // Group results by job for readable display.
  const byJob = new Map();
  for (const r of _setupCheck.results){
    if (!byJob.has(r.jobId)) byJob.set(r.jobId, { jobName: r.jobName, fields: [] });
    byJob.get(r.jobId).fields.push(r);
  }

  const html = Array.from(byJob.values()).map(g => {
    const dirtyFields = g.fields.filter(f => !f.error && f.totalBad > 0);
    const cleanCount = g.fields.filter(f => !f.error && f.totalBad === 0).length;
    const errCount = g.fields.filter(f => f.error).length;
    const totalBadInJob = dirtyFields.reduce((s,f) => s + f.totalBad, 0);

    const fieldsHtml = g.fields.map(f => {
      if (f.error){
        return `<div style="margin:0.3rem 0 0.3rem 1rem;color:var(--red);font-size:11px;font-family:var(--mono)">
          <strong>${f.srcCol} → ${f.setupTable}</strong>: ${f.error.replace(/</g,'&lt;')}
        </div>`;
      }
      if (f.totalBad === 0){
        return `<div style="margin:0.3rem 0 0.3rem 1rem;color:var(--green);font-size:11px;font-family:var(--mono)">
          ✓ <strong>${f.srcCol} → ${f.setupTable}</strong>: clean
        </div>`;
      }
      // Bad values table
      const valuesHtml = f.badValues.map(v => `
        <tr>
          <td style="padding:1px 0.5rem;font-family:var(--mono);font-size:11px;color:var(--amber)">${v.value === '(NULL)' ? '<em style="color:var(--text3)">(NULL or empty)</em>' : v.value.replace(/</g,'&lt;')}</td>
          <td style="padding:1px 0.5rem;font-family:var(--mono);font-size:11px;color:var(--text2);text-align:right">${v.count.toLocaleString()}</td>
        </tr>
      `).join('');
      return `<details style="margin:0.4rem 0 0.4rem 1rem">
        <summary style="cursor:pointer;color:var(--amber);font-size:12px;font-family:var(--mono)">
          ✕ <strong>${f.srcCol} → ${f.setupTable}.${f.joinKey||'Code'}</strong>
          <span style="color:var(--text3)">— ${f.totalBad.toLocaleString()} bad row${f.totalBad===1?'':'s'}, ${f.badValues.length} distinct value${f.badValues.length===1?'':'s'}</span>
        </summary>
        <table style="margin-top:0.4rem;width:100%;border-collapse:collapse">
          <thead><tr>
            <th style="padding:1px 0.5rem;text-align:left;font-size:10px;color:var(--text3);text-transform:uppercase">Bad value</th>
            <th style="padding:1px 0.5rem;text-align:right;font-size:10px;color:var(--text3);text-transform:uppercase">Rows</th>
          </tr></thead>
          <tbody>${valuesHtml}</tbody>
        </table>
      </details>`;
    }).join('');

    const headerColour = totalBadInJob>0 ? 'var(--amber)' : (errCount>0 ? 'var(--red)' : 'var(--green)');
    return `<div style="margin-bottom:1rem;border:0.5px solid var(--border2);border-radius:var(--r);padding:0.75rem;background:var(--bg3)">
      <div style="font-size:13px;font-weight:600;color:${headerColour}">
        ${g.jobName}
        <span style="font-size:11px;color:var(--text3);font-weight:normal">
          — ${g.fields.length} setup field${g.fields.length===1?'':'s'}
          ${totalBadInJob > 0 ? `· <strong style="color:var(--amber)">${totalBadInJob.toLocaleString()} bad row${totalBadInJob===1?'':'s'}</strong>` : ''}
          ${cleanCount > 0 ? `· ${cleanCount} clean` : ''}
          ${errCount > 0 ? `· <strong style="color:var(--red)">${errCount} error${errCount===1?'':'s'}</strong>` : ''}
        </span>
      </div>
      ${fieldsHtml}
    </div>`;
  }).join('');

  document.getElementById('setup-check-results').innerHTML = html;
  document.getElementById('setup-check-results-wrap').style.display = 'block';
}

// Persist last run to Cosmos. One key per project, overwritten each run —
// keeps the key bounded under the 5MB cap.
function _setupCheckPersist(){
  try {
    let activeId = '';
    try { activeId = JSON.parse(sessionStorage.getItem('cygenix_active_project')||'null')?.id || ''; } catch {}
    const key = 'cygenix_setup_reports';
    let store = {};
    try { store = JSON.parse(localStorage.getItem(key)||'{}'); } catch { store = {}; }
    store[activeId || '_no_project'] = {
      runAt: new Date().toISOString(),
      topo: _setupCheck.topo ? { srcDb: _setupCheck.topo.src.db, tgtDb: _setupCheck.topo.tgt.db, server: _setupCheck.topo.src.server } : null,
      fieldCount: _setupCheck.results.length,
      results: _setupCheck.results.map(r => ({
        jobId: r.jobId, jobName: r.jobName, srcCol: r.srcCol, tgtCol: r.tgtCol, setupTable: r.setupTable,
        joinKey: r.joinKey || null,
        totalBad: r.totalBad,
        badValues: (r.badValues||[]).slice(0, 100),  // cap stored values to 100/field
        error: r.error
      }))
    };
    localStorage.setItem(key, JSON.stringify(store));
  } catch(e){ /* persistence failure shouldn't block the user — they have the report on screen */ }
}

function exportSetupCheckReport(){
  // CSV export of the per-value detail across all fields.
  const rows = [['Job','SourceTable','SourceCol','SetupTable','BadValue','RowCount','Error']];
  for (const r of _setupCheck.results){
    if (r.error){
      rows.push([r.jobName, r.srcTable, r.srcCol, r.setupTable, '', '', r.error]);
      continue;
    }
    if (!r.badValues || !r.badValues.length){
      rows.push([r.jobName, r.srcTable, r.srcCol, r.setupTable, '(clean)', '0', '']);
      continue;
    }
    for (const v of r.badValues){
      rows.push([r.jobName, r.srcTable, r.srcCol, r.setupTable, v.value, String(v.count), '']);
    }
  }
  const esc = s => '"' + String(s||'').replace(/"/g,'""') + '"';
  const csv = rows.map(row => row.map(esc).join(',')).join('\r\n');
  const stamp = new Date().toISOString().slice(0,19).replace(/[:T]/g,'-');
  if (typeof triggerDownload === 'function'){
    triggerDownload(csv, `setup_check_${stamp}.csv`);
  } else {
    const a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([csv], {type:'text/csv'}));
    a.download = `setup_check_${stamp}.csv`;
    a.click();
  }
}

window.openSetupCheckModal  = openSetupCheckModal;
window.closeSetupCheckModal = closeSetupCheckModal;
window.startSetupCheck      = startSetupCheck;
window.exportSetupCheckReport = exportSetupCheckReport;

// ── Export Deployable Package ─────────────────────────────────────────────
//
// For each selected job, produces a SINGLE .sql file containing everything a
// DBA needs to replay the migration on any SQL Server, with NO Cygenix and
// NO source-database access:
//
//   Section 1: target CREATE TABLE (from j.schemaSQL)
//   Section 2: staging-table CREATE (built from live source schema)
//   Section 3: all source rows as multi-row INSERTs (batched at 1000/batch)
//   Section 4: rewritten migration script (source refs pointed at staging)
//   Section 5: cleanup DROP (physical mode only)
//
// Mode is picked once at the start and applied to every job in the run:
//   • temp     → "#<table>", one batch, no GO separators (whole script in
//                one shot). Disappears when the connection drops.
//   • physical → "dbo.X_Cygenix_<table>", GO between sections, explicit
//                DROP at the end. Cleanly persisted/cleaned, supports
//                bigger imports.
//
// Multi-job runs produce a ZIP. Single-job runs produce a bare .sql.
//
// Memory honesty: rows are pulled in full into memory before we stringify
// them. For tables with millions of rows this can briefly use hundreds of
// MB of heap. Sequential processing means only one job's data sits in
// memory at a time, and we null out the array after writing.

const _pkg = {
  running: false,
  cancelled: false,
  queue: [],
  results: [],
  current: null,
  abortController: null
};

function openExportPackageModal(){
  const { jobs, source } = getJobsForExport();
  const scopeEl = document.getElementById('pkg-scope');
  const startBtn = document.getElementById('pkg-start-btn');

  // Reset state
  _pkg.running = false;
  _pkg.cancelled = false;
  _pkg.queue = [];
  _pkg.results = [];
  document.getElementById('pkg-progress-wrap').style.display = 'none';
  document.getElementById('pkg-progress-list').innerHTML = '';
  document.getElementById('pkg-status').style.display = 'none';
  document.getElementById('pkg-status').textContent = '';
  startBtn.disabled = false;
  startBtn.textContent = 'Start';
  document.getElementById('pkg-cancel-btn').textContent = 'Cancel';

  if (!jobs.length){
    if (scopeEl) scopeEl.textContent = 'No jobs available to export.';
    startBtn.disabled = true;
    document.getElementById('export-package-modal').classList.add('open');
    return;
  }

  // Runnable filter: must have target schema + migration SQL + source table.
  const runnable = [];
  const skipped = [];
  for (const j of jobs){
    const schema = j.schemaSQL || '';
    const mig = j.insertSQL || j.migrationSQL || '';
    const srcTable = j.sourceTable || j.source || '';
    if (!schema){ skipped.push({ name: j.name, reason: 'no target schema' }); continue; }
    if (!mig)   { skipped.push({ name: j.name, reason: 'no migration SQL' });  continue; }
    if (!srcTable){ skipped.push({ name: j.name, reason: 'no source table' }); continue; }
    if (j.jobType === 'sql'){ skipped.push({ name: j.name, reason: 'raw-SQL job' }); continue; }
    runnable.push(j);
  }
  _pkg.queue = runnable;
  _pkg.skipped = skipped;

  if (scopeEl){
    const scopeLabel = source === 'selection'
      ? `${jobs.length} selected job${jobs.length===1?'':'s'}`
      : `all ${jobs.length} job${jobs.length===1?'':'s'} in the current filter`;
    scopeEl.textContent =
      `Will export ${runnable.length}/${jobs.length} job${jobs.length===1?'':'s'} from ${scopeLabel}. ` +
      (skipped.length ? skipped.length + ' skipped.' : '');
  }
  if (runnable.length === 0){
    startBtn.disabled = true;
    startBtn.textContent = 'Nothing to export';
  }
  document.getElementById('export-package-modal').classList.add('open');
}

function closeExportPackageModal(){
  if (_pkg.running){
    _pkg.cancelled = true;
    document.getElementById('pkg-status').textContent = 'Cancelling…';
    try { _pkg.abortController?.abort(); } catch {}
    return;
  }
  document.getElementById('export-package-modal').classList.remove('open');
}

// ── Helpers ────────────────────────────────────────────────────────────────

function _pkgSplitFullName(full){
  // "schema.table" → {schema, name}, fallback to dbo if unqualified.
  const parts = String(full||'').split('.');
  if (parts.length === 2) return { schema: parts[0], name: parts[1] };
  return { schema: 'dbo', name: parts[0] };
}

function _pkgStagingName(srcTableName, mode){
  // mode = 'temp' → ##Client_DM  ; 'physical' → dbo.X_Cygenix_Client_DM
  //
  // Global temp tables (##) are used instead of local (#) so the analyst
  // running the package in SSMS can open a SECOND tab and SELECT from the
  // staged data while the migration runs (or after it errors). Local #
  // temp tables are session-scoped — only the script's own connection can
  // see them, which defeats the inspection workflow.
  //
  // Tradeoffs of ##:
  //   • Lifetime — ## persists until the LAST session referencing it
  //     disconnects. We mitigate by emitting a defensive DROP IF EXISTS
  //     before the CREATE so re-runs work cleanly.
  //   • Name collisions — ##Client_DM is server-global. If two analysts
  //     run the same package concurrently they'll collide on the CREATE.
  //     Acceptable risk for the inspection workflow; rename the package
  //     manually if it bites.
  //   • Permissions — defaults to everyone on the server, which is fine
  //     for ephemeral inspection data.
  const safe = String(srcTableName||'').replace(/[^A-Za-z0-9_]/g, '');
  return mode === 'physical' ? '[dbo].[X_Cygenix_'+safe+']' : '[##'+safe+']';
}

// Fetch the source table's columns + types via INFORMATION_SCHEMA. We need
// type-with-precision so we can rebuild CREATE TABLE faithfully.
async function _pkgFetchSourceSchema(srcConn, srcTable){
  const parts = _pkgSplitFullName(srcTable);
  const sql =
    "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, " +
    "DATETIME_PRECISION, IS_NULLABLE, ORDINAL_POSITION " +
    "FROM INFORMATION_SCHEMA.COLUMNS " +
    "WHERE TABLE_SCHEMA = '"+parts.schema.replace(/'/g,"''")+"' " +
    "  AND TABLE_NAME = '"+parts.name.replace(/'/g,"''")+"' " +
    "ORDER BY ORDINAL_POSITION;";
  const res = await impDbCall(srcConn, { action: 'execute', sql });
  const rows = res?.recordset || res?.rows || [];
  return rows.map(r => ({
    name: r.COLUMN_NAME || r.column_name,
    type: _pkgRebuildType(r),
    nullable: (r.IS_NULLABLE || r.is_nullable) === 'YES'
  }));
}

// Rebuild a SQL type spec like "NVARCHAR(50)" or "DECIMAL(18,2)" from the
// pieces INFORMATION_SCHEMA returns. SQL Server stores VARCHAR(MAX) as
// CHARACTER_MAXIMUM_LENGTH = -1, which we render as MAX.
function _pkgRebuildType(r){
  const t = (r.DATA_TYPE || r.data_type || '').toUpperCase();
  const cml = r.CHARACTER_MAXIMUM_LENGTH ?? r.character_maximum_length;
  const np  = r.NUMERIC_PRECISION        ?? r.numeric_precision;
  const ns  = r.NUMERIC_SCALE            ?? r.numeric_scale;
  const dtp = r.DATETIME_PRECISION       ?? r.datetime_precision;

  // Char families take a length
  if (/^(N?VARCHAR|N?CHAR)$/i.test(t)){
    const len = cml === -1 ? 'MAX' : (cml ?? '255');
    return t + '(' + len + ')';
  }
  // Decimal/numeric take precision+scale
  if (/^(DECIMAL|NUMERIC)$/i.test(t)){
    if (np != null && ns != null) return t + '(' + np + ',' + ns + ')';
    return t;
  }
  // datetime2 / datetimeoffset / time take precision
  if (/^(DATETIME2|DATETIMEOFFSET|TIME)$/i.test(t) && dtp != null){
    return t + '(' + dtp + ')';
  }
  // Binary/varbinary take length
  if (/^(BINARY|VARBINARY)$/i.test(t)){
    const len = cml === -1 ? 'MAX' : (cml ?? '255');
    return t + '(' + len + ')';
  }
  // Everything else: no parameters (INT, BIT, UNIQUEIDENTIFIER, DATETIME,
  // SMALLDATETIME, DATE, MONEY, FLOAT, REAL, XML, TEXT, NTEXT, IMAGE…)
  return t;
}

// Pull every source row, paged. The Netlify db-connect proxy caps response
// payloads at 6 MB — a single SELECT * against a wide source table easily
// blows past that and you get a confusing 502 "ResponseSizeTooLarge". We
// page with OFFSET / FETCH NEXT to keep each response well under the cap.
//
// Page size is a balancing act: too small means many round-trips and high
// total time; too large means a single page can blow the cap. 500 rows /
// page is conservative for very wide tables (105+ columns) without being
// painfully slow for normal ones.
//
// We need a deterministic ORDER BY for OFFSET to be stable. SQL Server
// requires it. There may not be a clustered key on the source (a CSV
// import table often won't have one). We fall back to a synthetic
// (SELECT NULL) which works in modern SQL Server but skips the index path;
// fine for an export.
async function _pkgFetchSourceRows(srcConn, srcTable, progressCb){
  const parts = _pkgSplitFullName(srcTable);
  const ref = '['+parts.schema.replace(/]/g,'')+'].['+parts.name.replace(/]/g,'')+']';

  // Discover a stable order key — prefer the first column, fallback to
  // (SELECT NULL). One cheap probe.
  let orderBy = '(SELECT NULL)';
  try {
    const probe = await impDbCall(srcConn, {
      action: 'execute',
      sql:  "SELECT TOP 1 COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
            "WHERE TABLE_SCHEMA='"+parts.schema.replace(/'/g,"''")+"' " +
            "  AND TABLE_NAME='"+parts.name.replace(/'/g,"''")+"' " +
            "ORDER BY ORDINAL_POSITION"
    });
    const rs = probe?.recordset || probe?.rows || [];
    const firstCol = rs[0]?.COLUMN_NAME || rs[0]?.column_name;
    if (firstCol) orderBy = '['+firstCol.replace(/]/g,'')+']';
  } catch {/* fall through to (SELECT NULL) */}

  const all = [];
  let offset = 0;
  // Adaptive page size. Start at 500 and halve on any proxy/transport
  // failure that suggests the response was too big for the intermediary
  // to handle. Floors at 25 — below that, the table is exotic enough
  // to need a dedicated tool (BCP, SSIS) and we surface a clear message.
  let pageSize = 500;
  const MIN_PAGE = 25;
  // Safety upper bound — if a table somehow has >5M rows we want to bail
  // with a clear message rather than burn the heap silently.
  const MAX_TOTAL = 5_000_000;
  // Errors we treat as "proxy choked on payload size — halve and retry".
  // The first two are explicit size complaints from AWS Lambda / Azure
  // Function infrastructure. The rest are observed symptoms when a proxy
  // (Netlify Function, Azure Front Door, the browser fetch) bails on an
  // overlarge response without a structured error:
  //   - "Non-JSON response": proxy returned HTML (502/504/timeout page)
  //     and the JSON parser in impDbCall (line 16050) substituted this
  //     marker. Without matching it here, Export Package aborts hard at
  //     the first too-big page instead of shrinking — observed on
  //     site_to_site export, ~1,000 rows in. Added 29-May-2026.
  //   - "Bad Gateway" / "Gateway Timeout" / HTTP 502/504: same family,
  //     proxy intermediary failed. Worth retrying smaller.
  //   - "timeout": browser-side AbortSignal fired (60s in impDbCall).
  //     A smaller page should fit inside the timeout.
  //   - "Failed to fetch" / "NetworkError": transient connectivity blip;
  //     a smaller retry is likely to succeed.
  const SHRINKABLE_ERROR_RE = /ResponseSizeTooLarge|payload size|Non-JSON response|Bad Gateway|Gateway Timeout|HTTP 50[24]|timeout|Failed to fetch|NetworkError/i;
  while (true){
    const sql =
      "SELECT * FROM " + ref + " " +
      "ORDER BY " + orderBy + " " +
      "OFFSET " + offset + " ROWS FETCH NEXT " + pageSize + " ROWS ONLY;";
    let rows;
    try {
      const res = await impDbCall(srcConn, { action: 'execute', sql });
      rows = res?.recordset || res?.rows || [];
    } catch (e) {
      // Adaptive shrink. Halve and retry the SAME offset so we don't
      // drop rows. Sleep briefly first — if the proxy is stressed,
      // hammering it with an immediate retry just makes things worse.
      // Once we've shrunk to MIN_PAGE, give up with a clear message
      // so the user knows it isn't transient and points them at BCP.
      const em = (e?.message || String(e));
      if (SHRINKABLE_ERROR_RE.test(em) && pageSize > MIN_PAGE){
        const newSize = Math.max(MIN_PAGE, Math.floor(pageSize / 2));
        if (progressCb) progressCb(all.length, 'shrinking page size: ' + pageSize + ' → ' + newSize + ' (' + em.slice(0, 80) + ')');
        pageSize = newSize;
        // 500ms backoff. Lets a stressed proxy recover and avoids retry
        // storms when the issue is intermittent gateway flakiness.
        await new Promise(r => setTimeout(r, 500));
        continue;
      }
      // At MIN_PAGE and still failing — recoverable shrink path is
      // exhausted. Re-throw with a clearer message so the user knows
      // what to do next.
      if (SHRINKABLE_ERROR_RE.test(em) && pageSize <= MIN_PAGE){
        throw new Error(
          'Stalled at ' + all.length.toLocaleString() + ' rows after shrinking to ' +
          MIN_PAGE + '-row pages. The proxy still rejected the response (' + em.slice(0, 120) +
          '). Most likely cause: very wide rows (large NVARCHAR / XML / VARBINARY columns). ' +
          'Use SQL Server\'s native BCP or SSIS for tables this large or wide.'
        );
      }
      throw e;
    }
    if (!rows.length) break;
    for (const r of rows) all.push(r);
    if (progressCb) progressCb(all.length);
    if (rows.length < pageSize) break;       // last partial page
    offset += pageSize;
    if (all.length >= MAX_TOTAL){
      throw new Error('Aborted at '+all.length.toLocaleString()+' rows (safety cap) — table too large for in-browser export. Use SQL Server\'s native BCP / SSIS for very large exports.');
    }
  }
  return all;
}

// Format a single JS value into a SQL literal of the given type. This is
// the critical correctness helper — get it wrong and every package fails
// to load. Per-type rules:
//
//   NULL                       → NULL (no quotes)
//   BIT                        → 0 / 1 (no quotes)
//   INT/TINYINT/SMALLINT/BIGINT → bare number
//   DECIMAL/NUMERIC/MONEY/FLOAT/REAL → bare number
//   UNIQUEIDENTIFIER           → '<guid-string>'
//   DATE/DATETIME/DATETIME2/...→ '2024-01-15T13:45:22.123' (ISO 8601)
//   BINARY/VARBINARY           → 0xDEADBEEF (no quotes)
//   N?VARCHAR/N?CHAR/TEXT/NTEXT/XML → N'escaped string'
//
// Strings ALWAYS get the N prefix; cheap insurance against losing
// non-ASCII characters in client/legal data (the "O'Brien" / é problem).
function _pkgFormatSqlLiteral(value, type){
  if (value === null || value === undefined) return 'NULL';
  const t = String(type||'').toUpperCase().split('(')[0].trim();

  // Bit: JS may return as 0/1, true/false, or '0'/'1' depending on driver
  if (t === 'BIT'){
    if (value === true || value === 1 || value === '1') return '1';
    return '0';
  }

  // Numerics: no quotes. Could be JS number, string, or Decimal-like object.
  if (/^(TINYINT|SMALLINT|INT|BIGINT|DECIMAL|NUMERIC|MONEY|SMALLMONEY|FLOAT|REAL)$/i.test(t)){
    // tedious returns BigInt for BIGINT > 2^53 — convert to string safely
    if (typeof value === 'bigint') return value.toString();
    if (typeof value === 'number') return String(value);
    return String(value).trim();
  }

  // GUID. JS Date drivers return as plain string. Quoted, no N prefix needed.
  if (t === 'UNIQUEIDENTIFIER'){
    const s = String(value).replace(/'/g, "''");
    return "'" + s + "'";
  }

  // Date/time family. JS Date object OR string. Output ISO-8601, which
  // SQL Server parses unambiguously via implicit conversion.
  if (/^(DATE|TIME|DATETIME|DATETIME2|SMALLDATETIME|DATETIMEOFFSET)$/i.test(t)){
    if (value instanceof Date){
      // Strip the trailing Z so SQL Server interprets as local; we don't
      // know the source's TZ intent. ISO-8601 without Z is the safest bet.
      const iso = value.toISOString().replace(/Z$/, '');
      return "'" + iso + "'";
    }
    return "'" + String(value).replace(/'/g, "''") + "'";
  }

  // Binary: tedious returns Buffer; the proxy may have JSON-serialised as
  // base64 string or as a hex string with leading "0x". Handle both.
  if (/^(BINARY|VARBINARY|IMAGE|TIMESTAMP|ROWVERSION)$/i.test(t)){
    if (value && typeof value === 'object' && value.type === 'Buffer' && Array.isArray(value.data)){
      let hex = '0x';
      for (const b of value.data) hex += b.toString(16).padStart(2,'0').toUpperCase();
      return hex;
    }
    const s = String(value);
    if (/^0x[0-9a-fA-F]*$/.test(s)) return s;
    // Treat anything else as already-hex content
    return '0x' + s.replace(/[^0-9a-fA-F]/g,'');
  }

  // Default: string. NVARCHAR-safe quoting, single-quote escape.
  const s = String(value).replace(/'/g, "''");
  return "N'" + s + "'";
}

// Build the staging CREATE TABLE from a source schema. Caller passes the
// staging name as built by _pkgStagingName (already bracketed). For temp
// tables that name starts with [##…], which SQL Server treats correctly as
// a global temp table.
//
// Global temps persist until the last session referencing them disconnects.
// A re-run of the same script would then fail with "There is already an
// object…". We pre-emptively DROP IF EXISTS using OBJECT_ID against tempdb
// — the canonical idiom for global-temp existence checks.
function _pkgBuildCreateStaging(srcSchema, stagingName){
  const cols = srcSchema.map(c => '  ['+c.name+'] '+c.type+' '+(c.nullable?'NULL':'NOT NULL'));
  const isGlobalTemp = stagingName.startsWith('[##');
  const dropGuard = isGlobalTemp
    ? "IF OBJECT_ID('tempdb.." + stagingName.replace(/^\[|\]$/g,'') + "') IS NOT NULL DROP TABLE " + stagingName + ";\n"
    : '';
  return dropGuard + 'CREATE TABLE ' + stagingName + ' (\n' + cols.join(',\n') + '\n);';
}

// Build batched multi-row INSERT statements. SQL Server's hard limit is
// 1000 rows per VALUES clause. We chunk accordingly. mode determines
// whether GO separators are emitted between batches (physical) or not
// (temp — must run as one batch so the temp table is visible).
function _pkgBuildInsertBatches(stagingName, srcSchema, rows, mode){
  if (!rows.length) return '-- (source table is empty)\n';
  const BATCH = 1000;
  const colList = srcSchema.map(c => '['+c.name+']').join(', ');
  const out = [];
  for (let i = 0; i < rows.length; i += BATCH){
    const slice = rows.slice(i, i + BATCH);
    const valuesBlocks = slice.map(r => {
      const cells = srcSchema.map(c => _pkgFormatSqlLiteral(r[c.name], c.type));
      return '  (' + cells.join(', ') + ')';
    }).join(',\n');
    out.push('INSERT INTO ' + stagingName + ' (' + colList + ') VALUES\n' + valuesBlocks + ';');
    // GO separator between batches in physical mode for cleaner log
    // partitioning. In temp mode we must keep it one batch.
    if (mode === 'physical' && (i + BATCH) < rows.length) out.push('GO');
  }
  return out.join('\n');
}

// Rewrite the migration SQL's source references to point at the staging
// table. The job's stored SQL uses 2-part names like [dbo].[Client_DM];
// after our validate fix it may also have been generated with 3-part
// [Agentive].[dbo].[Client_DM]. Replace both forms with the staging name.
//
// We intentionally only target the source table's specific names — we do
// NOT do a generic global rewrite (target references like [dbo].[Client]
// must stay untouched).
function _pkgRewriteMigration(insertSQL, sourceTableFull, srcDb, stagingName){
  if (!insertSQL) return '';
  const parts = _pkgSplitFullName(sourceTableFull);
  const schema = parts.schema;
  const name   = parts.name;
  const esc = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

  let sql = insertSQL;

  // 3-part form [<srcDb>].[<schema>].[<name>]
  if (srcDb){
    const r3 = new RegExp('\\['+esc(srcDb)+'\\]\\.\\['+esc(schema)+'\\]\\.\\['+esc(name)+'\\]', 'gi');
    sql = sql.replace(r3, stagingName);
  }
  // 2-part form [<schema>].[<name>] — replace AFTER 3-part so we don't
  // double-rewrite. Use a negative lookbehind to avoid swallowing 3-part
  // references that happen to share a suffix (e.g. another DB's same name).
  // JS regex supports lookbehind in modern browsers; if older support is
  // ever needed, swap for a two-pass marker approach.
  const r2 = new RegExp('(?<!\\]\\.)\\['+esc(schema)+'\\]\\.\\['+esc(name)+'\\]', 'gi');
  sql = sql.replace(r2, stagingName);

  return sql;
}

// Build a single job's full deployable .sql package. Returns a string.
//
// opts.minimal (default false):
//   When true, strips the dry-run BEGIN TRY/BEGIN TRAN wrap from Section 4
//   AND removes the trailing `SELECT COUNT(*)` verify lines that
//   object_mapping.html appends to every generated job's insertSQL. Banner
//   is also shortened (no @Commit explanation). Used for hand-curated jobs
//   that have already been validated end-to-end and just need a clean
//   deployable artifact. Added 29-May-2026.
async function _pkgBuildJobPackage(job, mode, srcConn, srcDb, opts){
  const minimal = !!(opts && opts.minimal);
  const srcTable = job.sourceTable || job.source || '';
  const parts = _pkgSplitFullName(srcTable);
  const stagingName = _pkgStagingName(parts.name, mode);
  const goSep = mode === 'physical' ? '\nGO\n' : '\n';
  const sepLine = '-- ' + '='.repeat(70);

  // Section 1: banner
  // Two variants — the full version explains the @Commit dry-run flag, the
  // minimal version drops that since the wrap isn't present.
  const banner = minimal
    ? [
        sepLine,
        '-- CYGENIX DEPLOYABLE PACKAGE (minimal output)',
        '-- Job:        ' + (job.name || job.id),
        '-- Source:     ' + srcTable + (srcDb ? '  (db: '+srcDb+')' : ''),
        '-- Target:     ' + (job.targetTable || job.target || ''),
        '-- Staging:    ' + stagingName + '   (' + (mode==='physical'?'physical table, dropped at end':'global temp, visible to other sessions, auto-drops on last disconnect') + ')',
        '-- Generated:  ' + new Date().toISOString(),
        '-- Run on any SQL Server. No Cygenix or source connection required.',
        '--',
        '-- Minimal output: no dry-run wrap, no verify counts. The INSERT',
        '-- runs immediately when this file executes. Make sure you mean it.',
        sepLine,
        ''
      ].join('\n')
    : [
        sepLine,
        '-- CYGENIX DEPLOYABLE PACKAGE',
        '-- Job:        ' + (job.name || job.id),
        '-- Source:     ' + srcTable + (srcDb ? '  (db: '+srcDb+')' : ''),
        '-- Target:     ' + (job.targetTable || job.target || ''),
        '-- Staging:    ' + stagingName + '   (' + (mode==='physical'?'physical table, dropped at end':'global temp, visible to other sessions, auto-drops on last disconnect') + ')',
        '-- Generated:  ' + new Date().toISOString(),
        '-- Run on any SQL Server. No Cygenix or source connection required.',
        '--',
        '-- IMPORTANT — DRY-RUN BY DEFAULT:',
        '--   Section 4 wraps the migration INSERT in BEGIN TRAN. The flag',
        '--   @Commit at the top of Section 4 controls whether the transaction',
        '--   COMMITs or ROLLBACKs at the end:',
        '--       @Commit = 0  → dry-run (rolls back; nothing persists to target)',
        '--       @Commit = 1  → real load (commits to target)',
        '--   Default is 0 so re-running this file is safe. Flip to 1 when you',
        '--   have validated the dry-run output and are ready to persist.',
        '--   Identity values are consumed even on rollback — the first commit',
        '--   will not start at 1 if you ran dry-run first.',
        sepLine,
        ''
      ].join('\n');

  // Section 2: target CREATE TABLE (from the saved job)
  const targetSchema =
    sepLine + '\n-- Section 1: Target table schema\n' + sepLine + '\n' +
    (job.schemaSQL || '-- (no schemaSQL saved)') + '\n';

  // Section 3: source staging schema + section 4: data
  document.getElementById('pkg-status').textContent =
    'Fetching source schema for ' + srcTable + '…';
  const srcSchema = await _pkgFetchSourceSchema(srcConn, srcTable);
  if (!srcSchema.length) throw new Error('No columns returned for source table ' + srcTable);

  const stagingCreate =
    sepLine + '\n-- Section 2: Source staging table ('+(mode==='physical'?'physical':'temp')+')\n' + sepLine + '\n' +
    _pkgBuildCreateStaging(srcSchema, stagingName) + '\n';

  document.getElementById('pkg-status').textContent =
    'Pulling source rows for ' + srcTable + '…';
  const rows = await _pkgFetchSourceRows(srcConn, srcTable, function(n, message){
    // Live progress while paging: shows the user that we're making progress
    // and (crucially) gives a runtime feel of how slow large tables will be.
    // The optional `message` argument is set when the fetcher hits a
    // shrinkable error and is halving page size — surface it so the user
    // knows we're not stuck, just adapting.
    const base = 'Pulling source rows for ' + srcTable + ' — '+n.toLocaleString()+' so far';
    document.getElementById('pkg-status').textContent = message
      ? base + ' · ' + message
      : base + '…';
  });

  const dataSection =
    sepLine + '\n-- Section 3: Source data ('+rows.length.toLocaleString()+' rows)\n' + sepLine + '\n' +
    _pkgBuildInsertBatches(stagingName, srcSchema, rows, mode) + '\n';

  // Allow GC of the row array
  rows.length = 0;

  // Section 4: migration with rewritten source references.
  //
  // In default mode, wraps the migration in a dry-run-by-default BEGIN TRAN.
  // Semantics:
  //   • @Commit defaults to 0 → ROLLBACK at the end. Nothing persists.
  //   • Analyst flips to 1 → COMMIT. Real load.
  //   • Any T-SQL error inside the TRY auto-rolls back via XACT_ABORT ON.
  //   • The migration SQL may itself contain BEGIN TRAN/COMMIT/ROLLBACK
  //     (OTM scripts do this); nested transactions share the outermost
  //     decision, so an inner COMMIT bumps @@TRANCOUNT but doesn't make
  //     anything permanent — our outer ROLLBACK still wipes it. An inner
  //     ROLLBACK, however, rolls back everything to the outermost level
  //     and sets @@TRANCOUNT = 0, which can cause "no transaction to
  //     commit" errors at the outer COMMIT. SQL Server's nested-TRAN
  //     semantics are well known to be sharp; we add IF @@TRANCOUNT > 0
  //     guards on both branches to handle either case cleanly.
  //
  // In minimal mode, the wrap is dropped and the verify-count SELECTs that
  // object_mapping.html appends to insertSQL are stripped, leaving just the
  // bare INSERT.
  let migrationBody = _pkgRewriteMigration(job.insertSQL || job.migrationSQL || '', srcTable, srcDb, stagingName);

  if (minimal){
    // Strip the auto-appended verify block. object_mapping.html appends:
    //   -- Verify:
    //   SELECT COUNT(*) AS [migrated_rows] FROM ...;
    //   SELECT COUNT(*) AS [source_rows]   FROM ...;
    // We anchor on "-- Verify:" so we don't accidentally swallow a user's
    // own SELECT COUNT elsewhere. If "-- Verify:" isn't present, leave the
    // SQL alone — the user may have hand-edited it and removed the comment.
    const verifyMarker = migrationBody.lastIndexOf('-- Verify:');
    if (verifyMarker !== -1){
      migrationBody = migrationBody.slice(0, verifyMarker).replace(/\s+$/, '') + '\n';
    }
  }

  const migration = minimal
    ? sepLine + '\n-- Section 4: Migration (bare INSERT)\n' +
      '-- Source refs rewritten to ' + stagingName + '\n' + sepLine + '\n' +
      'SET XACT_ABORT ON;\n' +
      'SET NOCOUNT ON;\n' +
      '\n' +
      migrationBody + '\n'
    : sepLine + '\n-- Section 4: Migration (dry-run by default — set @Commit=1 to persist)\n' +
      '-- Source refs rewritten to ' + stagingName + '\n' + sepLine + '\n' +
      'DECLARE @Commit BIT = 0;  -- ← FLIP TO 1 TO ACTUALLY LOAD INTO TARGET\n' +
      '\n' +
      'SET XACT_ABORT ON;\n' +
      'SET NOCOUNT ON;\n' +
      'BEGIN TRY\n' +
      ' BEGIN TRAN;\n' +
      '\n' +
      migrationBody + '\n' +
      '\n' +
      ' IF @Commit = 1\n' +
      ' BEGIN\n' +
      ' IF @@TRANCOUNT > 0 COMMIT TRAN;\n' +
      "    PRINT 'CYGENIX: @Commit=1 → COMMITTED. Rows persisted to target.';\n" +
      ' END\n' +
      ' ELSE\n' +
      ' BEGIN\n' +
      ' IF @@TRANCOUNT > 0 ROLLBACK TRAN;\n' +
      "    PRINT 'CYGENIX: @Commit=0 → ROLLED BACK (dry-run). Set @Commit=1 and re-run to persist.';\n" +
      ' END\n' +
      'END TRY\n' +
      'BEGIN CATCH\n' +
      ' IF @@TRANCOUNT > 0 ROLLBACK TRAN;\n' +
      ' THROW;\n' +
      'END CATCH;\n';

  // Section 6: cleanup
  const cleanup = mode === 'physical'
    ? sepLine + '\n-- Section 5: Cleanup\n' + sepLine + '\nDROP TABLE ' + stagingName + ';\n'
    : '';

  return [banner, targetSchema, stagingCreate, dataSection, migration, cleanup]
    .filter(Boolean).join(goSep);
}

// ── Progress list helpers ─────────────────────────────────────────────────
function _pkgRenderProgressList(){
  const list = document.getElementById('pkg-progress-list');
  list.innerHTML = _pkg.queue.map(j => `
    <div data-pkrow="${j.id}" style="display:flex;align-items:flex-start;gap:0.5rem;padding:2px 0">
      <span class="pkg-icon" style="width:14px;text-align:center;color:var(--text3);flex-shrink:0">·</span>
      <span style="flex:0 0 auto;color:var(--text2);max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${(j.name||'(unnamed)').replace(/</g,'&lt;')}</span>
      <span class="pkg-msg" style="flex:1;color:var(--text3);font-size:11px;word-break:break-word;white-space:normal"></span>
    </div>
  `).join('');
  document.getElementById('pkg-progress-wrap').style.display = 'block';
  document.getElementById('pkg-status').style.display = 'block';
}

function _pkgUpdateRow(jobId, status, message){
  const row = document.querySelector(`[data-pkrow="${jobId}"]`);
  if (!row) return;
  const icon = status === 'running' ? ''
             : status === 'ok'      ? '✓'
             : status === 'fail'    ? '✕'
             : '·';
  const colour = status === 'running' ? 'var(--accent)'
               : status === 'ok'      ? 'var(--green)'
               : status === 'fail'    ? 'var(--red)'
               : 'var(--text3)';
  const ic = row.querySelector('.pkg-icon');
  ic.textContent = icon; ic.style.color = colour;
  if (message){
    const m = row.querySelector('.pkg-msg');
    m.textContent = ' — ' + message;
    m.style.color = colour;
    m.title = message;
  }
}

// ── Main orchestrator ─────────────────────────────────────────────────────
async function startExportPackage(){
  if (_pkg.running) return;
  if (!_pkg.queue.length) return;

  const srcConn = (typeof impGetConn === 'function') ? impGetConn('src') : '';
  if (!srcConn){
    alert('No source database connection configured. The export needs to read live source-row data. Set the source in Dashboard → Connections.');
    return;
  }
  const tgtConn = (typeof impGetConn === 'function') ? impGetConn('tgt') : '';
  // Probe topology only to get srcDb (used by the migration rewriter to
  // strip 3-part qualifiers). If tgtConn is missing we still try with a
  // null srcDb — the 2-part rewrite path covers the common case.
  let srcDb = '';
  if (tgtConn){
    try {
      const topo = await _validateProbeTopology(srcConn, tgtConn);
      srcDb = topo.src.db || '';
    } catch (e) { /* non-fatal */ }
  }

  // Read selected mode
  const modeRadios = document.querySelectorAll('input[name="pkg-mode"]');
  let mode = 'temp';
  for (const r of modeRadios) if (r.checked) mode = r.value;
  // Read minimal toggle (false by default — preserves existing behaviour)
  const minimal = !!document.getElementById('pkg-minimal')?.checked;

  _pkg.running = true;
  _pkg.cancelled = false;
  document.getElementById('pkg-start-btn').disabled = true;
  document.getElementById('pkg-start-btn').textContent = 'Running…';
  _pkgRenderProgressList();

  const fileMap = new Map();  // jobName → sql text, for ZIP packaging
  for (let i = 0; i < _pkg.queue.length; i++){
    if (_pkg.cancelled) break;
    const job = _pkg.queue[i];
    document.getElementById('pkg-status').textContent =
      `Building ${i+1} of ${_pkg.queue.length}: ${job.name}`;
    _pkgUpdateRow(job.id, 'running', 'fetching…');

    try {
      _pkg.abortController = new AbortController();
      const sql = await _pkgBuildJobPackage(job, mode, srcConn, srcDb, { minimal });
      const safe = (job.name||job.id).replace(/[^a-z0-9]/gi, '_');
      const fname = safe + '_package.sql';
      fileMap.set(fname, sql);
      _pkg.results.push({ jobName: job.name, ok: true, sizeKb: Math.round(sql.length/1024) });
      _pkgUpdateRow(job.id, 'ok', `built (${Math.round(sql.length/1024).toLocaleString()} KB)`);
    } catch (e){
      // Surface the full error message in the visible message column, not
      // just a generic "Error". The first 200 chars of the SQL exception
      // text is almost always enough to diagnose.
      const msg = (e?.message || String(e)).slice(0, 300);
      _pkg.results.push({ jobName: job.name, ok: false, message: msg });
      _pkgUpdateRow(job.id, 'fail', msg);
    }
  }

  _pkg.running = false;
  _pkg.abortController = null;

  // Single file: download bare. Multiple: ZIP.
  if (fileMap.size === 1){
    const [fname, content] = fileMap.entries().next().value;
    triggerDownload(content, fname);
  } else if (fileMap.size > 1){
    try {
      const JSZip = await _loadJSZip();
      const zip = new JSZip();
      for (const [fname, content] of fileMap.entries()) zip.file(fname, content);
      const blob = await zip.generateAsync({ type: 'blob' });
      const stamp = new Date().toISOString().slice(0,19).replace(/[:T]/g,'-');
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'cygenix_packages_' + stamp + '.zip';
      a.click();
      setTimeout(() => URL.revokeObjectURL(a.href), 5000);
    } catch (e){
      document.getElementById('pkg-status').innerHTML =
        `<span style="color:var(--red)">ZIP packaging failed: ${(e.message||e).toString().slice(0,200)}</span>`;
    }
  }

  const okN   = _pkg.results.filter(r => r.ok).length;
  const failN = _pkg.results.filter(r => !r.ok).length;
  const tail  = _pkg.cancelled ? ' (cancelled)' : '';
  document.getElementById('pkg-status').innerHTML =
    `<span style="color:var(--green)">${okN} built</span> · ` +
    `<span style="color:var(--red)">${failN} failed</span>${tail}`;
  document.getElementById('pkg-start-btn').textContent = 'Done';
  document.getElementById('pkg-cancel-btn').textContent = 'Close';
  if (typeof showToast === 'function') showToast(`Package export: ${okN} built, ${failN} failed${tail}`);
  if (typeof addAudit === 'function') addAudit(`Export Package: ${okN} built, ${failN} failed${tail} (${mode} staging)`);
}

window.openExportPackageModal  = openExportPackageModal;
window.closeExportPackageModal = closeExportPackageModal;
window.startExportPackage      = startExportPackage;

function jobsTableHTML(jobs) {
  if (!jobs.length) return '<div class="empty-state"><h3>No migration jobs yet</h3><p>Create your first SQL migration to get started.</p></div>';

  function jobStatusBadge(j) {
    // 'running' is a transient state with no bucket of its own — surface it
    // before falling back to the shared classification, so the badge and the
    // donut always tell the same story about a settled job.
    const raw = String(j.executionStatus || j.status || '').toLowerCase();
    if (raw === 'running') return '<span class="badge" style="background:var(--amber-bg);color:var(--amber)"><i class="ic-dot" style="color:var(--amber)"></i> Running</span>';
    const b = jobBucket(j);
    if (b === 'complete') return '<span class="badge badge-green"><i class="ic-dot" style="color:var(--green)"></i> Complete</span>';
    if (b === 'failed')   return '<span class="badge" style="background:var(--red-bg);color:var(--red)"><i class="ic-dot" style="color:var(--red)"></i> Failed</span>';
    if (b === 'ready')    return '<span class="badge badge-green">SQL Ready</span>';
    return '<span class="badge" style="background:var(--bg3);color:var(--text3)"><i class="ic-dot" style="color:var(--text3)"></i> Pending</span>';
  }

  function lastRunText(j) {
    const ts = j.lastRun || j.completedAt || j.executedAt;
    if (!ts) return '<span style="font-size:10px;color:var(--text3)">Never run</span>';
    const d = new Date(ts);
    const diff = Date.now() - d.getTime();
    if (diff < 60000)   return '<span style="font-size:10px;color:var(--green)">Just now</span>';
    if (diff < 3600000) return '<span style="font-size:10px;color:var(--text3)">' + Math.round(diff/60000) + 'm ago</span>';
    if (diff < 86400000)return '<span style="font-size:10px;color:var(--text3)">' + Math.round(diff/3600000) + 'h ago</span>';
    return '<span style="font-size:10px;color:var(--text3)">' + d.toLocaleDateString('en-GB') + '</span>';
  }

  return '<table class="jobs-table"><thead><tr>' +
    '<th style="width:22px" title="Drag rows to reorder"></th>' +
    '<th style="width:28px"><input type="checkbox" id="jobs-select-all" onchange="toggleAllJobsSelection(this.checked)" style="cursor:pointer"></th>' +
    '<th style="width:40px">#</th>' +
    '<th>Job Name</th><th>Source</th><th>Target</th><th>Tables</th><th>Rows</th>' +
    '<th>Exec Status</th><th>Last Run</th><th>Actions</th>' +
    '</tr></thead><tbody>' +
    jobs.map((j, idx) => {
      // Version + last-modified chip text. Falls back gracefully when a job
      // pre-dates auto-versioning (no j.version yet) by showing just the
      // created date.
      const v = (typeof j.version === 'number' && j.version > 0) ? ('v'+j.version) : '';
      const lm = j.lastModified || j.created || null;
      const lmStr = lm ? new Date(lm).toLocaleString('en-GB', {day:'2-digit',month:'2-digit',year:'2-digit',hour:'2-digit',minute:'2-digit'}) : '';
      // Combined chip — small, accent colour, sits next to the type badge.
      // Renders as "v3 · 24/05/26, 14:32" when both are present.
      const versionChip = (v || lmStr)
        ? `<span style="font-size:9px;padding:1px 6px;margin-left:4px;border-radius:4px;background:var(--accent-glow);color:var(--accent);font-family:var(--mono);font-weight:500" title="Auto-versioned on each meaningful change. Click to see full history.">${v}${v&&lmStr?' · ':''}${lmStr}</span>`
        : '';
      // Conditional action buttons. Soft-deleted rows show Restore + Hard
      // Delete only — everything else is hidden because edit/generate
      // operations on a deleted row don't make sense.
      const isDel = !!j._deleted;
      const actions = isDel ? `
          <button class="btn" style="background:var(--teal-bg);color:var(--teal);border:0.5px solid rgba(23,130,124,0.25)" onclick="restoreJob('${j.id}')" title="Restore from Trash">↩ Restore</button>
          <button class="btn" style="background:var(--red-bg);color:var(--red);border:0.5px solid rgba(240,70,70,0.25)" onclick="hardDeleteJob('${j.id}')" title="Permanent delete"><i class="ic ic-trash"></i> Delete forever</button>
        ` : `
          <button class="btn btn-ghost" onclick="editJob('${j.id}')" style="color:var(--purple);border-color:rgba(107,78,142,0.25)" title="Edit"><i class="ic ic-edit"></i> </button>
          <button class="btn btn-ghost" onclick="CygenixHistory.open('${j.id}')" style="color:var(--accent);border-color:var(--accent-glow)" title="Version history"><i class="ic ic-clock"></i> </button>
          ${(j.insertSQL||j.migrationSQL) ? `<button class="btn btn-ghost" onclick="window.location.href='/sql-editor.html?job=${j.id}'" style="color:var(--teal);border-color:rgba(23,130,124,0.25)" title="Open SQL in editor"><i class="ic ic-edit"></i> </button>` : ''}
          <button class="btn" style="background:var(--purple-bg);color:var(--purple);border:0.5px solid rgba(107,78,142,0.25)" onclick="openReport('${j.id}')" title="Report">Report</button>
          <button class="btn btn-ghost" onclick="renameJob(this)" data-jobid="${j.id}" title="Rename"><i class="ic ic-edit"></i> </button>
          <button class="btn btn-ghost" onclick="startEditSource('${j.id}')" title="Edit source table"><i class="ic ic-database"></i> </button>
          <button class="btn" style="background:var(--red-bg);color:var(--red);border:0.5px solid rgba(240,70,70,0.25)" onclick="deleteJob('${j.id}')" title="Move to Trash">✕</button>
        `;
      const rowStyle = isDel ? 'opacity:0.6;background:rgba(240,70,70,0.04)' : '';
      return `<tr id="job-row-${j.id}" data-job-id="${j.id}" style="${rowStyle}" ondragover="onJobRowDragOver(event)" ondragleave="onJobRowDragLeave(event)" ondrop="onJobRowDrop(event,'${j.id}')" ondragend="onJobRowDragEnd(event)">
      <td class="job-drag-cell" style="text-align:center;color:var(--text3);user-select:none" title="Drag to reorder"><span class="job-drag-handle" draggable="true" ondragstart="onJobRowDragStart(event,'${j.id}')" style="cursor:grab;font-size:14px;line-height:1;display:inline-block;padding:2px 4px">⠿</span></td>
      <td style="text-align:center"><input type="checkbox" class="job-select-cb" data-job-id="${j.id}" onchange="onJobSelectToggle()" style="cursor:pointer" ${selectedJobIds.has(j.id)?'checked':''}></td>
      <td style="font-family:var(--mono);font-size:11px;color:var(--text3);text-align:center">${idx+1}</td>
      <td>
        <span class="job-name" id="job-name-${j.id}" ondblclick="startRenameJob('${j.id}')" title="${escapeAttr(j.name)} — double-click to rename">${j.name}</span>
        ${j.jobType==='one-to-many'?' <span style="font-size:9px;padding:1px 5px;border-radius:4px;background:var(--teal-bg);color:var(--teal);font-family:var(--mono)">1→M</span>':j.jobType==='sql'?' <span style="font-size:9px;padding:1px 5px;border-radius:4px;background:var(--accent-glow);color:var(--accent);font-family:var(--mono)">SQL</span>':''}
        ${versionChip}
        ${isDel ? ` <span style="font-size:9px;padding:1px 5px;border-radius:4px;background:var(--red-bg);color:var(--red);font-family:var(--mono)" title="Deleted ${j._deletedAt? new Date(j._deletedAt).toLocaleString('en-GB'):''}">DELETED</span>` : ''}
        <span class="job-meta">${new Date(j.created).toLocaleDateString('en-GB')}</span>
      </td>
      <td><span class="job-source" id="job-source-${j.id}" ondblclick="startEditSource('${j.id}')" style="font-size:11px;color:var(--text3);font-family:var(--mono);cursor:text" title="${escapeAttr(j.sourceTable||j.source||'')} — double-click to edit source table">${j.sourceTable||j.source||'—'}</span></td>
      <td><span style="font-size:12px;color:var(--text2)">${j.target||'—'}</span></td>
      <td><span style="font-family:var(--mono);font-size:12px;color:var(--text2)">${j.tables?.length||j.files?.length||'—'}</span></td>
      <td><span style="font-family:var(--mono);font-size:12px;color:var(--text2)">${j.totalRows?.toLocaleString()||'—'}</span></td>
      <td>${jobStatusBadge(j)}</td>
      <td>${lastRunText(j)}</td>
      <td>
        <div class="jobs-actions">${actions}</div>
      </td></tr>`;
    }).join('') + '</tbody></table>';
}

// ── Row reordering (drag-and-drop) ────────────────────────────────────────
// Single-source-of-truth caveat: this writes order into state.jobs and
// localStorage, NOT into project-builder. If you also sequence the same
// jobs in project-builder, the two orderings can drift apart. Export
// Scripts (and any other consumer that reads state.jobs in array order)
// will use whichever order was last saved here.
//
// Reorder semantics on a filtered view: only the visible rows' positions
// in state.jobs change. Hidden jobs stay where they are. Concretely, when
// you drop A onto B and both are visible, we splice A into the position
// B currently occupies in state.jobs — leaving every non-visible job
// untouched.
let _draggedJobId = null;

function onJobRowDragStart(ev, jobId){
  _draggedJobId = jobId;
  // Some browsers require data on the event to allow drop.
  try { ev.dataTransfer.setData('text/plain', jobId); } catch {}
  try { ev.dataTransfer.effectAllowed = 'move'; } catch {}
  // Dim the source row so the user can see what they're carrying.
  const row = document.getElementById('job-row-' + jobId);
  if (row) row.style.opacity = '0.45';
  // Stop the event bubbling from the inner handle re-firing dragstart on
  // the parent <tr> (which would call this twice). Harmless but noisy.
  ev.stopPropagation();
}

function onJobRowDragOver(ev){
  if (!_draggedJobId) return;
  ev.preventDefault();          // allow drop
  ev.dataTransfer.dropEffect = 'move';
  const row = ev.currentTarget;
  if (row && row.dataset.jobId !== _draggedJobId){
    row.classList.add('job-row-drag-over');
  }
}

function onJobRowDragLeave(ev){
  const row = ev.currentTarget;
  if (row) row.classList.remove('job-row-drag-over');
}

function onJobRowDrop(ev, targetJobId){
  ev.preventDefault();
  ev.stopPropagation();
  const row = ev.currentTarget;
  if (row) row.classList.remove('job-row-drag-over');
  if (!_draggedJobId || _draggedJobId === targetJobId) return;
  reorderJobsInState(_draggedJobId, targetJobId);
  _draggedJobId = null;
}

function onJobRowDragEnd(ev){
  // Always clear opacity + the drag-over class everywhere, even if drop
  // happened outside any row (the browser still fires dragend).
  document.querySelectorAll('#all-jobs tr').forEach(r => {
    r.style.opacity = '';
    r.classList.remove('job-row-drag-over');
  });
  _draggedJobId = null;
}

function reorderJobsInState(sourceId, targetId){
  const arr = state.jobs;
  const srcIdx = arr.findIndex(j => j.id === sourceId);
  const tgtIdx = arr.findIndex(j => j.id === targetId);
  if (srcIdx < 0 || tgtIdx < 0 || srcIdx === tgtIdx) return;
  const [moved] = arr.splice(srcIdx, 1);
  // After removal, tgtIdx may need to shift down by 1 if source was above target
  const insertAt = srcIdx < tgtIdx ? tgtIdx - 1 : tgtIdx;
  // Drop "after" if dragging downward, "before" if dragging upward — feels
  // most natural for a row-grid. We approximate by inserting at insertAt
  // when going up (above target) and insertAt+1 when going down (below target).
  const finalIdx = srcIdx < tgtIdx ? insertAt + 1 : insertAt;
  arr.splice(finalIdx, 0, moved);

  // Persist. Direct setItem because saveJobs() is fine but this is clearer
  // about intent (and triggers the Cosmos sync hook the same way).
  try { localStorage.setItem('cygenix_jobs', JSON.stringify(arr)); } catch {}

  if (typeof showToast === 'function') showToast('Job order updated');
  if (typeof addAudit === 'function') addAudit(`Reordered job "${moved.name}"`);
  renderAllJobs();
}

window.onJobRowDragStart = onJobRowDragStart;
window.onJobRowDragOver  = onJobRowDragOver;
window.onJobRowDragLeave = onJobRowDragLeave;
window.onJobRowDrop      = onJobRowDrop;
window.onJobRowDragEnd   = onJobRowDragEnd;

function editJob(jobId) {
  const job = state.jobs.find(j => j.id === jobId);
  if (!job) return;
  // Route to the correct editor based on jobType
  switch (job.jobType) {
    case 'sql':
      window.location.href = '/sql-editor.html?edit=' + encodeURIComponent(jobId);
      break;
    default:
      // All mapping jobs (single-map, one-to-many, migration) → object_mapping.html
      window.location.href = '/object_mapping.html?edit=' + encodeURIComponent(jobId);
  }
}

function renameJob(btn) {
  const jobId = btn.getAttribute('data-jobid');
  if (!jobId) return;
  startRenameJob(jobId);
}

function startRenameJob(jobId) {
  // Find the job-name div in the same row as the button
  const row = document.querySelector('[data-job-id="' + jobId + '"]');
  let nameEl = row ? row.querySelector('.job-name') : document.getElementById('job-name-' + jobId);
  if (!nameEl) return;
  const current = nameEl.textContent.trim();
  // Build inline input
  const input = document.createElement('input');
  input.id    = 'rename-input-' + jobId;
  input.value = current;
  input.style.cssText = 'background:var(--bg3);border:0.5px solid var(--accent);border-radius:4px;padding:3px 8px;font-size:13px;color:var(--text);font-family:var(--serif);width:100%;outline:none';
  input.addEventListener('keydown', e => handleRenameKey(e, jobId));
  input.addEventListener('blur',    () => confirmRenameJob(jobId));
  nameEl.innerHTML = '';
  nameEl.appendChild(input);
  input.focus(); input.select();
}

function handleRenameKey(e, jobId) {
  if (e.key === 'Enter')  { e.preventDefault(); confirmRenameJob(jobId); }
  if (e.key === 'Escape') { e.preventDefault(); cancelRenameJob(jobId); }
}

function confirmRenameJob(jobId) {
  const input = document.getElementById('rename-input-' + jobId)
    || document.querySelector('[data-job-id="' + jobId + '"] input');
  if (!input) return;
  const newName = input.value.trim();
  if (!newName) { cancelRenameJob(jobId); return; }
  // Atomic mutation. Also updates the in-memory state.jobs for immediate
  // render, then schedules a version-create so the rename produces a
  // recoverable snapshot.
  const ok = safeMutateJob(jobId, j => { j.name = newName; });
  if (ok){
    const inMem = state.jobs.find(j => j.id === jobId);
    if (inMem) inMem.name = newName;
    if (typeof scheduleAutoVersion === 'function') scheduleAutoVersion(jobId, 'renamed');
    showToast('Job renamed to "' + newName + '"');
  }
  renderAllJobs();
}

function cancelRenameJob(jobId) {
  const job = state.jobs.find(j => j.id === jobId);
  const nameEl = document.getElementById('job-name-' + jobId);
  if (nameEl && job) nameEl.innerHTML = job.name;
}

// ── Inline source-table editing ───────────────────────────────────────────
// Double-click the source cell to correct the source table. This exists
// because the Agentive runner can propose a source table that doesn't exist
// in the source DB (or a stale/duplicate name), which makes Export Package
// fail when _pkgFetchSourceSchema can't find the table. There was no UI to
// fix it, so this restores that.
//
// CRITICAL: jobs are read everywhere as `job.sourceTable || job.source`
// (Export Package, Validate, Setup Check, cross-DB rewrite). Agent-created
// jobs populate `source`; older jobs may use `sourceTable`. If we write only
// one field the other can shadow it (sourceTable wins the || so a stale
// sourceTable would mask a corrected source). So we write BOTH fields to the
// same value — that is the regression that got lost before.
function startEditSource(jobId) {
  const row = document.querySelector('[data-job-id="' + jobId + '"]');
  let el = row ? row.querySelector('.job-source') : document.getElementById('job-source-' + jobId);
  if (!el) return;
  const job = state.jobs.find(j => j.id === jobId);
  const current = job ? (job.sourceTable || job.source || '') : el.textContent.trim();
  const input = document.createElement('input');
  input.id    = 'source-input-' + jobId;
  input.value = current === '—' ? '' : current;
  input.placeholder = 'dbo.TableName';
  input.style.cssText = 'background:var(--bg3);border:0.5px solid var(--accent);border-radius:4px;padding:3px 8px;font-size:11px;color:var(--text);font-family:var(--mono);width:100%;outline:none';
  input.addEventListener('keydown', e => handleEditSourceKey(e, jobId));
  input.addEventListener('blur',    () => confirmEditSource(jobId));
  el.innerHTML = '';
  el.appendChild(input);
  input.focus(); input.select();
}

function handleEditSourceKey(e, jobId) {
  if (e.key === 'Enter')  { e.preventDefault(); confirmEditSource(jobId); }
  if (e.key === 'Escape') { e.preventDefault(); cancelEditSource(jobId); }
}

function confirmEditSource(jobId) {
  const input = document.getElementById('source-input-' + jobId)
    || document.querySelector('[data-job-id="' + jobId + '"] .job-source input');
  if (!input) return;
  const newSource = input.value.trim();
  if (!newSource) { cancelEditSource(jobId); return; }
  // Write BOTH fields so no stale value can shadow the correction.
  const ok = safeMutateJob(jobId, j => { j.sourceTable = newSource; j.source = newSource; });
  if (ok){
    const inMem = state.jobs.find(j => j.id === jobId);
    if (inMem) { inMem.sourceTable = newSource; inMem.source = newSource; }
    if (typeof scheduleAutoVersion === 'function') scheduleAutoVersion(jobId, 'source table edited');
    showToast('Source table set to "' + newSource + '"');
  }
  renderAllJobs();
}

function cancelEditSource(jobId) {
  const job = state.jobs.find(j => j.id === jobId);
  const el = document.getElementById('job-source-' + jobId);
  if (el && job) el.innerHTML = (job.sourceTable || job.source || '—');
}

function deleteJob(jobId) {
  const job = state.jobs.find(j => j.id === jobId);
  if (!job) return;
  if (!confirm('Move job "' + job.name + '" to Trash? You can restore it from "Show deleted" next to + New Migration.')) return;
  // SOFT delete — keep the row, flag it. This is the core safety net:
  // never silently remove a job from the array. Hard-deletes only happen
  // from the "View deleted" panel with a second confirm.
  safeMutateJob(jobId, j => { j._deleted = true; j._deletedAt = new Date().toISOString(); });
  // Snapshot the deletion as a version-create so the user has a recovery
  // point if they hard-delete later and regret it.
  if (typeof scheduleAutoVersion === 'function') scheduleAutoVersion(jobId, 'soft-deleted');
  state.jobs = loadPersistedJobs();
  renderAllJobs();
  renderDashboard();
  showToast('Job moved to Trash');
}

// Permanent delete — only callable from the "Show deleted" panel. Two
// confirms because the row is gone for good (apart from version-control
// snapshots in Cosmos, which the user has to recover from manually).
function hardDeleteJob(jobId){
  const job = state.jobs.find(j => j.id === jobId);
  if (!job) return;
  if (!confirm('PERMANENTLY delete "' + job.name + '"? Version history in Cosmos remains, but the job will not appear in the dashboard again.')) return;
  if (!confirm('Are you sure? This cannot be undone from the UI.')) return;
  safeRemoveJob(jobId);
  state.jobs = loadPersistedJobs();
  renderAllJobs();
  renderDashboard();
  showToast('Job permanently deleted');
}

// Restore a soft-deleted job. Clears the _deleted flag.
function restoreJob(jobId){
  safeMutateJob(jobId, j => { delete j._deleted; delete j._deletedAt; });
  state.jobs = loadPersistedJobs();
  renderAllJobs();
  renderDashboard();
  showToast('Job restored');
}

// Toggle the "Show deleted" view. Backed by a single global flag that
// renderAllJobs reads when it filters out _deleted rows.
let _showDeletedJobs = false;
function toggleShowDeleted(){
  _showDeletedJobs = !_showDeletedJobs;
  renderAllJobs();
  // Update the dropdown's label so the user can see the active state.
  const btn = document.getElementById('show-deleted-btn');
  if (btn) btn.textContent = _showDeletedJobs ? '↩ Hide deleted' : 'Show deleted';
}

// ── safeMutateJob / safeRemoveJob / safeUpsertJob ──────────────────────────
//
// All writes to cygenix_jobs go through these helpers. They are the only
// places that touch the array. Each one does an ATOMIC read-modify-write
// with ID-based lookup so concurrent saves can't silently lose entries.
//
// The original 9-job wipe almost certainly came from a non-atomic save:
// reader saw an empty/stale array, modifier added one job, writer replaced
// the entire array with the modified one. The race window between read
// and write was wide because Cygenix-sync could pull a stale Cosmos copy
// in the middle. Single-entry mutations via these helpers close that gap.
//
// IMPORTANT: never call localStorage.setItem('cygenix_jobs', …) outside
// these helpers. The CygenixSync.setItem patch propagates to Cosmos —
// a bad write here = a bad write to the cloud.
function safeMutateJob(jobId, mutator){
  try {
    const jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]');
    const idx = jobs.findIndex(j => j && j.id === jobId);
    if (idx < 0) return false;
    mutator(jobs[idx]);
    localStorage.setItem('cygenix_jobs', JSON.stringify(jobs));
    return true;
  } catch (e) { return false; }
}

function safeRemoveJob(jobId){
  try {
    const jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]');
    const next = jobs.filter(j => !(j && j.id === jobId));
    if (next.length === jobs.length) return false;  // not found
    localStorage.setItem('cygenix_jobs', JSON.stringify(next));
    return true;
  } catch (e) { return false; }
}

// Add OR update a job by ID. Used by all bulk-create paths. The key
// safety property: if the job already exists, we MERGE the new fields
// into the existing record instead of overwriting it. This protects
// against an Agentive batch flow that "creates" a job whose ID already
// exists from a previous run — we want to preserve fields the previous
// run captured (e.g. version, lastModified) and only update what's new.
function safeUpsertJob(job){
  if (!job || !job.id){ return false; }
  try {
    const jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]');
    const idx = jobs.findIndex(j => j && j.id === job.id);
    if (idx >= 0){
      jobs[idx] = Object.assign({}, jobs[idx], job);
    } else {
      jobs.unshift(job);
    }
    localStorage.setItem('cygenix_jobs', JSON.stringify(jobs));
    return true;
  } catch (e) { return false; }
}

// ── Auto-versioning ────────────────────────────────────────────────────────
//
// When a job's meaningful content changes, fire a version-create call to
// Cosmos so a snapshot is preserved. "Meaningful" = mapping, schemaSQL,
// insertSQL, name, sourceTable, targetTable. We don't version on execution
// status, lastRun, drag-reorder, etc.
//
// De-bounced per-jobId so a burst of edits in object_mapping.html doesn't
// produce 20 versions. Content-hashed so a save without real changes
// is silent (no version, no Cosmos round-trip).
const _autoVersionDebounce = new Map();    // jobId → setTimeout handle
const _autoVersionLastHash = new Map();    // jobId → last seen hash
const VERSION_DEBOUNCE_MS = 3000;

async function _hashJob(j){
  // Stable JSON of meaningful fields only. Order matters for the hash; we
  // pick fields explicitly so accidental additions to the job shape don't
  // invalidate the hash.
  const stable = JSON.stringify({
    name:        j.name || '',
    sourceTable: j.sourceTable || j.source || '',
    targetTable: j.targetTable || j.target || '',
    schemaSQL:   j.schemaSQL   || '',
    insertSQL:   j.insertSQL   || j.migrationSQL || '',
    columnMapping: j.columnMapping || []
  });
  // Web Crypto API — produces SHA-256 hex. Matches backend's hash field shape.
  try {
    const buf = new TextEncoder().encode(stable);
    const out = await crypto.subtle.digest('SHA-256', buf);
    return Array.from(new Uint8Array(out)).map(b => b.toString(16).padStart(2,'0')).join('');
  } catch { return stable.length + ':' + (stable.charCodeAt(0)||0); }  // weak fallback
}

function scheduleAutoVersion(jobId, note){
  if (!jobId) return;
  // Clear any pending de-bounce for this jobId
  const existing = _autoVersionDebounce.get(jobId);
  if (existing) clearTimeout(existing);
  // Schedule the actual version-create call
  const handle = setTimeout(() => {
    _autoVersionDebounce.delete(jobId);
    _doAutoVersion(jobId, note);
  }, VERSION_DEBOUNCE_MS);
  _autoVersionDebounce.set(jobId, handle);
}

async function _doAutoVersion(jobId, note){
  try {
    if (typeof CygenixSync === 'undefined') return;
    const userId = CygenixSync.getUserId && CygenixSync.getUserId();
    if (!userId) return;
    const jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]');
    const job = jobs.find(j => j && j.id === jobId);
    if (!job) return;
    const hash = await _hashJob(job);
    if (_autoVersionLastHash.get(jobId) === hash) return;  // no-op — content unchanged
    const url = CygenixSync.apiBase + '/version-create' + (CygenixSync.funcCode ? '?code='+CygenixSync.funcCode : '');
    const body = {
      jobId,
      userId,
      label: 'auto',
      note: note || 'auto-save',
      hash,
      payload: job
    };
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-user-id': userId },
      body: JSON.stringify(body)
    });
    if (!r.ok) return;
    const result = await r.json().catch(() => null);
    _autoVersionLastHash.set(jobId, hash);
    // Update the in-job version + lastModified fields locally so the badge
    // renders correctly without needing a re-fetch.
    const newVersion = result?.version || ((job.version || 0) + 1);
    safeMutateJob(jobId, j => {
      j.version = newVersion;
      j.lastModified = new Date().toISOString();
    });
    // Refresh the table only — don't disrupt other state.
    if (typeof renderAllJobs === 'function') renderAllJobs();
  } catch {
    // Auto-version failures are deliberately silent. We don't want a
    // network blip to break the user's workflow; the next save will
    // retry. The user's data is already safely in cygenix_jobs locally
    // before we ever attempt this.
  }
}

window.safeMutateJob   = safeMutateJob;
window.safeRemoveJob   = safeRemoveJob;
window.safeUpsertJob   = safeUpsertJob;
window.restoreJob      = restoreJob;
window.hardDeleteJob   = hardDeleteJob;
window.toggleShowDeleted = toggleShowDeleted;
window.scheduleAutoVersion = scheduleAutoVersion;

function saveJobs() {
  // Kept for backward compatibility but discouraged for new code. Use
  // safeMutateJob / safeUpsertJob for per-job changes instead. This still
  // does a full-array write, which is the dangerous pattern we want to
  // retire. Any caller that absolutely needs a full re-serialise (e.g.
  // drag-reorder, where positional order is the change being saved) is
  // legitimate; everything else should migrate.
  try { localStorage.setItem('cygenix_jobs', JSON.stringify(state.jobs)); } catch {}
}

function showToast(msg) {
  const t = document.createElement('div');
  t.textContent = msg;
  t.style.cssText = 'position:fixed;bottom:1.5rem;right:1.5rem;background:var(--bg2);border:0.5px solid var(--border2);color:var(--green);padding:0.6rem 1.1rem;border-radius:8px;font-size:13px;z-index:999;font-family:var(--serif)';
  document.body.appendChild(t);
  setTimeout(() => t.remove(), 2500);
}

function renderDashboard() {
  // The Recent Jobs table has been replaced by the Project Status panel.
  // We still call updateStats() for the stat cards at the top.
  updateStats();
  renderRing();
  renderProjectStatus();
  renderCutoverConfidence();
  // Schedules need a network round trip, so they fill in behind the rest
  // rather than holding the whole view up.
  loadDashboardSchedules();
}

// ── Cutover Confidence ───────────────────────────────────────────────────────
// One honest number for the programme director: mapping confidence, the
// latest Preflight forecast (Execute page → Preflight) and delivery
// state, rolled up by cygenix-preflight.js. Components without data are
// excluded and named, never faked; no data at all shows as no data.
function renderCutoverConfidence() {
  const val = $('stat-confidence'), sub = $('stat-confidence-sub');
  if (!val || typeof CygenixPreflight === 'undefined') return;
  let preflight = null;
  try { preflight = CygenixPreflight.pfLoad(localStorage.getItem('cygenix_active_project_id') || 'default'); } catch {}
  const c = CygenixPreflight.pfConfidence({ jobs: state.jobs || [], preflight });
  const COLOR = { green: 'var(--green)', amber: 'var(--amber)', red: 'var(--red)', 'no-data': 'var(--text3)' };
  const LABEL = { green: 'Ready', amber: 'Caution', red: 'Not ready', 'no-data': 'No signals yet' };
  val.textContent = c.score === null ? '—' : c.score;
  val.style.color = COLOR[c.grade];
  sub.textContent = LABEL[c.grade];
  const detail = $('confidence-detail');
  if (detail) {
    detail.innerHTML = c.parts.map(p =>
      '<div style="display:flex;justify-content:space-between;gap:1rem;padding:4px 0;border-bottom:0.5px solid var(--border);font-size:12px">'
      + '<span>' + escHtml(p.label) + ' <span style="color:var(--text3)">· ' + escHtml(p.note) + '</span></span>'
      + '<b style="font-family:var(--mono)">' + (p.hasData ? p.score : '—') + '</b></div>').join('')
      + '<div style="font-size:11px;color:var(--text3);padding-top:6px">Weighted over components with data. '
      + 'Run <i class="ic ic-takeoff"></i> Preflight on the Execute page to feed the risk component.</div>';
  }
}
function toggleConfidenceDetail() {
  const d = $('confidence-detail');
  if (d) d.style.display = d.style.display === 'none' ? '' : 'none';
}

// ══════════════════════════════════════════════════════════════════════════
// HOME — schedules summary
// ══════════════════════════════════════════════════════════════════════════
// A read-only view of the Task Agent, so the state of the automation is
// visible on the screen everyone lands on rather than only after navigating
// to it. Deliberately not interactive: enabling, running and editing stay on
// the Task Agent page, which is where the confirmations and error handling
// live. Clicking a row goes there.
//
// The formatters (ta_fmtDateTime, ta_runStatusChip, ta_jobNameFor) are the
// Task Agent's own, so a trigger or a status can never read differently in
// the two places.

let _dashSchedLoading = false;

async function loadDashboardSchedules(){
  const list = document.getElementById('dashboard-schedules-list');
  const sub  = document.getElementById('dashboard-schedules-sub');
  if (!list) return;
  if (_dashSchedLoading) return;          // Refresh double-click
  _dashSchedLoading = true;
  if (sub) sub.textContent = 'Loading…';
  try {
    const data = await ta_sched('list-schedules', {});
    const schedules = data.schedules || [];
    // Share the fetch with the Task Agent view so opening it does not
    // immediately refetch what we already have.
    TA.schedules = schedules;
    renderDashboardSchedules(schedules);
  } catch (e) {
    // Not being able to reach the scheduler is not a broken dashboard — say
    // what happened, keep it small, and let the rest of the page stand.
    if (sub) sub.textContent = 'Could not load';
    list.innerHTML = '<div style="font-size:12px;color:var(--text3);padding:0.5rem 0">'
      + 'Could not reach the scheduler: ' + escapeHtml(e && e.message || 'unknown error')
      + ' · <a href="#" onclick="loadDashboardSchedules();return false" style="color:var(--accent)">retry</a></div>';
  } finally {
    _dashSchedLoading = false;
  }
}

function renderDashboardSchedules(schedules){
  const list = document.getElementById('dashboard-schedules-list');
  const sub  = document.getElementById('dashboard-schedules-sub');
  if (!list) return;

  if (!schedules.length){
    if (sub) sub.textContent = 'No schedules yet';
    list.innerHTML = '<div style="font-size:12px;color:var(--text3);padding:0.5rem 0">'
      + 'Nothing scheduled. <a href="#" onclick="showView(\'task-agent\');return false" style="color:var(--accent)">Create one in the Task Agent</a>.</div>';
    return;
  }

  const on  = schedules.filter(s => s.enabled).length;
  const off = schedules.length - on;
  // Failures are the thing worth noticing from the home screen, so they are
  // counted in the subtitle rather than left to be spotted in the rows.
  const failed = schedules.filter(s => s.lastRunStatus === 'failed').length;
  if (sub){
    sub.textContent = [
      schedules.length + ' schedule' + (schedules.length === 1 ? '' : 's'),
      on + ' active',
      off + ' paused',
    ].join(' · ') + (failed ? ' · ' + failed + ' last run failed' : '');
  }

  // Active first, then by name, so what is running is at the top.
  const sorted = schedules.slice().sort((a, b) => {
    if (!!b.enabled !== !!a.enabled) return b.enabled ? 1 : -1;
    return String(a.name || '').localeCompare(String(b.name || ''));
  });

  const rows = sorted.map(s => {
    const trigger = s.chainAfter
      ? '<span class="badge badge-purple" title="Fires after its parent succeeds"><i class="ic ic-chain"></i> Chained</span>'
      : '<span class="badge badge-blue" style="font-family:var(--mono)">' + escapeHtml(s.humanReadable || s.cron || '—') + '</span>';
    // A paused schedule may still carry a stale nextRunAt from when it was
    // enabled. Showing it would state a run that is not going to happen.
    const next = (s.enabled && s.nextRunAt)
      ? '<span style="font-family:var(--mono);font-size:11px;color:var(--text2)">' + ta_fmtDateTime(s.nextRunAt) + '</span>'
      : '<span style="font-size:10px;color:var(--text3)">' + (s.enabled ? '—' : 'paused') + '</span>';
    const last = s.lastRunAt
      ? ta_runStatusChip(s.lastRunStatus) + ' <span style="font-size:11px;color:var(--text3);font-family:var(--mono);margin-left:0.4rem">' + ta_fmtDateTime(s.lastRunAt) + '</span>'
      : '<span style="font-size:10px;color:var(--text3)">Never run</span>';
    return `<tr style="cursor:pointer" onclick="showView('task-agent')" title="Open in the Task Agent">
      <td>
        <span style="display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:0.5rem;vertical-align:middle;background:${s.enabled ? 'var(--green)' : 'var(--text3)'}"
              title="${s.enabled ? 'Active' : 'Paused'}"></span>
        <strong style="color:var(--text)">${escapeHtml(s.name || '(unnamed)')}</strong>
        <div style="font-size:11px;color:var(--text3);margin-top:2px;margin-left:1.05rem">${escapeHtml(ta_jobNameFor(s.jobId))}</div>
      </td>
      <td>${trigger}</td>
      <td>${next}</td>
      <td>${last}</td>
      <td><span style="font-size:11px;color:${s.enabled ? 'var(--green)' : 'var(--text3)'}">${s.enabled ? 'Active' : 'Paused'}</span></td>
    </tr>`;
  }).join('');

  list.innerHTML = '<table class="jobs-table"><thead><tr>'
    + '<th>Name / Job</th><th>Trigger</th><th>Next run</th><th>Last run</th><th>Status</th>'
    + '</tr></thead><tbody>' + rows + '</tbody></table>';
}

// ══════════════════════════════════════════════════════════════════════════
// RING CHART — job status breakdown
// Shows proportion of jobs in each lifecycle state. Uses real data only.
// ══════════════════════════════════════════════════════════════════════════
function renderRing(){
  const host = document.getElementById('dashboard-ring-wrap');
  if (!host) return;
  // Trashed jobs are excluded — the ring used to count them, so emptying the
  // trash shrank the All Jobs table and left this chart showing the old total.
  const jobs = liveJobs();

  // Bucketing is shared with the stat cards and the Project Status panel via
  // jobBucket(), so the same job can no longer be Complete in one panel and
  // Pending in another.
  const buckets = { complete: 0, ready: 0, failed: 0, pending: 0 };
  jobs.forEach(j => { buckets[jobBucket(j)]++; });
  const total = jobs.length;

  const segments = [
    { key:'complete', label:'Complete',     count: buckets.complete, color:'#22c97a' },
    { key:'ready',    label:'SQL Ready',    count: buckets.ready,    color:'#4A5BD6' },
    { key:'failed',   label:'Failed',       count: buckets.failed,   color:'#f04646' },
    { key:'pending',  label:'Pending',      count: buckets.pending,  color:'#6B4E8E' },
  ];

  // Empty state — zero jobs total. Still show the ring as a muted full circle
  // so the shape is there when the first job arrives, visually reassuring.
  if (total === 0){
    host.innerHTML = `
      <div class="ring-wrap">
        <div class="ring-svg-col">
          ${ringSVG([], 0, 'No jobs', 'yet')}
        </div>
        <div class="ring-info-col">
          <div class="ring-info-head">Job Status</div>
          <div class="ring-info-title">No jobs yet</div>
          <div class="ring-info-sub">Once you create a migration job, this chart tracks how jobs move through the lifecycle: pending → ready → complete.</div>
        </div>
      </div>`;
    return;
  }

  const completePct = Math.round((buckets.complete / total) * 100);
  const visibleSegs = segments.filter(s => s.count > 0);

  // Legend rows — shown in the info column alongside the ring
  const legendHTML = segments.map(s => {
    const pct = total ? Math.round((s.count/total)*100) : 0;
    const dim = s.count === 0;
    return `
      <div class="ring-leg-row" ${dim ? 'style="opacity:0.4"' : ''}>
        <span class="ring-leg-swatch" style="background:${s.color}"></span>
        <span class="ring-leg-label">${escP(s.label)}</span>
        <span class="ring-leg-count">${s.count}</span>
        <span class="ring-leg-pct">${pct}%</span>
      </div>`;
  }).join('');

  host.innerHTML = `
    <div class="ring-wrap">
      <div class="ring-svg-col">
        ${ringSVG(visibleSegs, total, String(completePct) + '%', 'complete')}
      </div>
      <div class="ring-info-col">
        <div class="ring-info-head">Job Status Breakdown</div>
        <div class="ring-info-title">${total} total job${total === 1 ? '' : 's'}</div>
        <div class="ring-info-sub">Every job across all projects, excluding the trash. The Project Status panel below is scoped to the active project.</div>
        <div class="ring-legend">${legendHTML}</div>
      </div>
    </div>`;
}

// Build an animated SVG ring with the given segments. Segments must already
// be filtered to ones with count > 0.
function ringSVG(segments, total, centreNum, centreLabel){
  const size = 180;
  const cx = size / 2;
  const cy = size / 2;
  const r  = 72;
  const circumference = 2 * Math.PI * r;

  // Each segment occupies (count / total) of the circumference, drawn
  // sequentially. We use stroke-dasharray + stroke-dashoffset to carve out
  // each segment on one shared circle path.
  let offsetAccum = 0;
  const segPaths = segments.map((s, i) => {
    const frac = total ? (s.count / total) : 0;
    const segLen = frac * circumference;
    // Animate each segment by revealing its stroke-dasharray over time.
    const animStyle = `animation: ringReveal${i} 0.9s cubic-bezier(0.22, 1, 0.36, 1) ${i * 120 + 100}ms forwards;`;
    // Inline @keyframes per segment — safe: generated once per render.
    const kf = `@keyframes ringReveal${i}{from{stroke-dasharray:0 ${circumference};}to{stroke-dasharray:${segLen - 2} ${circumference};}}`;
    const html = `
      <circle class="ring-seg" cx="${cx}" cy="${cy}" r="${r}"
              stroke="${s.color}"
              stroke-dasharray="0 ${circumference}"
              stroke-dashoffset="${-offsetAccum}"
              style="${animStyle}"/>
      <style>${kf}</style>`;
    offsetAccum += segLen;
    return html;
  }).join('');

  return `
    <svg class="ring-svg" width="${size}" height="${size}" viewBox="0 0 ${size} ${size}">
      <circle class="ring-bg" cx="${cx}" cy="${cy}" r="${r}"/>
      <g transform="rotate(-90 ${cx} ${cy})">${segPaths}</g>
      <text class="ring-centre ring-centre-num" x="${cx}" y="${cy - 6}">${escP(centreNum)}</text>
      <text class="ring-centre ring-centre-lbl" x="${cx}" y="${cy + 18}">${escP(centreLabel)}</text>
    </svg>`;
}

// ══════════════════════════════════════════════════════════════════════════
// MIGRATION PIPELINE — live visual of each module's state
// Every metric here reads real data from localStorage. Do not add demo values.
// ══════════════════════════════════════════════════════════════════════════
function renderPipeline(){
  const host = document.getElementById('dashboard-pipeline');
  if (!host) return;

  const jobs      = safeArr('cygenix_jobs');
  const projects  = safeArr('cygenix_projects');
  const wasis     = safeArr('cygenix_wasis_rules');
  const conns     = (window.CygenixConnections && window.CygenixConnections.get && window.CygenixConnections.get()) || {};
  const privacyMode = (window.CygenixPrivacy && window.CygenixPrivacy.getMode && window.CygenixPrivacy.getMode()) || 'full';

  // Metric derivations — each pulls from real data
  const mappedJobs   = jobs.filter(j => (j.columnMapping || []).some(m => m && m.tgtCol)).length;
  const sqlJobs      = jobs.filter(j => j.type === 'sql' || j.jobType === 'sql' || j.jobType === 'sql-script').length;
  const validated    = jobs.filter(j => j.verifySQL && String(j.verifySQL).trim().length > 20).length;
  const weekAgo      = Date.now() - 7 * 24 * 60 * 60 * 1000;
  const recentRuns   = jobs.filter(j => j.lastRun && Date.parse(j.lastRun) > weekAgo).length;
  const connsConfigured = (conns.srcConnString || conns.srcFnUrl ? 1 : 0) + (conns.tgtConnString || conns.tgtFnUrl || conns.fnUrl ? 1 : 0);

  // Governance mode mapped to visual status
  const govLabel = privacyMode === 'full' ? 'Full' : privacyMode === 'schema-only' ? 'Schema only' : 'No AI';
  const govStatus = privacyMode === 'full' ? 'warn' : privacyMode === 'no-ai' ? 'err' : 'ok';
  // NB: "warn" on full-mode isn't a criticism — it's the highest-access mode and the
  // dashboard surfaces that so operators don't forget they chose it.

  const nodes = [
    {
      key:'projects', label:'Projects', sub:'scoped migrations',
      metric: projects.length, unit: projects.length === 1 ? 'project' : 'projects',
      status: projects.length > 0 ? 'ok' : 'idle',
      icon: svgFolder(),
      onClick: () => showView('project-settings')
    },
    {
      key:'connections', label:'Connections', sub:'src · tgt databases',
      metric: connsConfigured, unit: '/ 2',
      status: connsConfigured === 2 ? 'ok' : connsConfigured === 1 ? 'warn' : 'idle',
      icon: svgPlug(),
      onClick: () => showView('connections')
    },
    {
      key:'mapper', label:'Object Mapping', sub:'columns mapped',
      metric: mappedJobs, unit: mappedJobs === 1 ? 'job' : 'jobs',
      status: mappedJobs > 0 ? 'ok' : 'idle',
      icon: svgMap(),
      onClick: () => { window.location.href = '/object_mapping.html'; }
    },
    {
      key:'sql', label:'SQL Editor', sub:'custom scripts',
      metric: sqlJobs, unit: sqlJobs === 1 ? 'script' : 'scripts',
      status: sqlJobs > 0 ? 'ok' : 'idle',
      icon: svgCode(),
      onClick: () => { window.location.href = '/sql-editor.html'; }
    },
    {
      key:'cleansing', label:'Cleansing', sub:'Was/Is rules',
      metric: wasis.length, unit: wasis.length === 1 ? 'rule' : 'rules',
      status: wasis.length > 0 ? 'ok' : 'idle',
      icon: svgSparkles(),
      onClick: () => { window.location.href = '/data-cleansing.html'; }
    },
    {
      key:'execution', label:'Execution', sub:'last 7 days',
      metric: recentRuns, unit: recentRuns === 1 ? 'run' : 'runs',
      status: recentRuns > 0 ? 'ok' : 'idle',
      icon: svgPlay(),
      onClick: () => { window.location.href = '/project-builder.html'; }
    },
    {
      key:'validation', label:'Validation', sub:'verify SQL set',
      metric: validated, unit: validated === 1 ? 'check' : 'checks',
      status: validated > 0 ? 'ok' : 'idle',
      icon: svgCheckCircle(),
      onClick: () => { window.location.href = '/validation.html'; }
    },
  ];

  // Governance badge sits at the very end — a policy gate, visually distinct
  nodes.push({
    key:'governance', label:'Governance', sub:'AI access mode',
    metric: govLabel, unit: '',
    isText: true,
    status: govStatus,
    icon: svgShield(),
    onClick: () => showView('privacy-security')
  });

  // Build markup
  const nodeHTML = nodes.map((n, i) => {
    const metricContent = n.isText
      ? `<div class="pipe-metric" style="font-size:0.95rem;font-weight:600;letter-spacing:0">${escP(n.metric)}</div>`
      : `<div class="pipe-metric">${Number(n.metric).toLocaleString()}<span class="pipe-metric-unit">${escP(n.unit)}</span></div>`;
    return `
      <div class="pipe-node" style="animation-delay:${(i*60+100)}ms">
        <div class="pipe-card status-${n.status}" data-key="${n.key}" title="Open ${escP(n.label)}">
          <span class="pipe-dot"></span>
          <div class="pipe-card-icon">${n.icon}</div>
          ${metricContent}
          <div class="pipe-label">${escP(n.label)}</div>
          <div class="pipe-sub">${escP(n.sub)}</div>
        </div>
      </div>`;
  }).join('');

  host.innerHTML = `
    <div class="pipeline" style="--pipe-count:${nodes.length}">
      <svg class="pipe-lines" id="pipe-lines" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">
        <defs>
          <linearGradient id="pipeGrad" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stop-color="#4A5BD6" stop-opacity="0.85"/>
            <stop offset="50%" stop-color="#17827C" stop-opacity="0.85"/>
            <stop offset="100%" stop-color="#6B4E8E" stop-opacity="0.85"/>
          </linearGradient>
          <linearGradient id="pipeGradBright" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stop-color="#4A5BD6"/>
            <stop offset="50%" stop-color="#17827C"/>
            <stop offset="100%" stop-color="#6B4E8E"/>
          </linearGradient>
        </defs>
        <path id="pipe-path-base" d="" stroke="url(#pipeGrad)"/>
        <path id="pipe-path-pulse" class="pulse" d="" stroke="url(#pipeGradBright)"/>
      </svg>
      ${nodeHTML}
    </div>
  `;

  // Wire clicks
  host.querySelectorAll('.pipe-card').forEach(el => {
    const key = el.dataset.key;
    const n = nodes.find(x => x.key === key);
    if (n && n.onClick) el.addEventListener('click', n.onClick);
  });

  // Draw connecting line once layout is settled. Two RAFs = after reflow.
  requestAnimationFrame(() => requestAnimationFrame(() => drawPipelineLine()));
}

function drawPipelineLine(){
  const wrap   = document.querySelector('.pipeline');
  const svg    = document.getElementById('pipe-lines');
  const base   = document.getElementById('pipe-path-base');
  const pulse  = document.getElementById('pipe-path-pulse');
  if (!wrap || !svg || !base || !pulse) return;

  const cards = wrap.querySelectorAll('.pipe-card');
  if (cards.length < 2) return;

  const wrapRect = wrap.getBoundingClientRect();
  svg.setAttribute('viewBox', `0 0 ${wrapRect.width} ${wrapRect.height}`);
  svg.setAttribute('width',  wrapRect.width);
  svg.setAttribute('height', wrapRect.height);

  // Route the line through a point just below each card, so it's visible against
  // the panel background rather than hidden inside the icon tiles.
  const points = Array.from(cards).map(c => {
    const r = c.getBoundingClientRect();
    return { x: r.left - wrapRect.left + r.width / 2, y: r.bottom - wrapRect.top + 14 };
  });

  // Build a smooth path using cubic bezier between each pair of points
  let d = `M ${points[0].x} ${points[0].y}`;
  for (let i = 1; i < points.length; i++){
    const prev = points[i-1];
    const cur  = points[i];
    const midX = (prev.x + cur.x) / 2;
    d += ` C ${midX} ${prev.y}, ${midX} ${cur.y}, ${cur.x} ${cur.y}`;
  }
  base.setAttribute('d', d);
  pulse.setAttribute('d', d);

  // Remove any existing anchor dots, then draw a small dot at each node point
  svg.querySelectorAll('.pipe-anchor').forEach(n => n.remove());
  const anchorColor = '#17827C';
  points.forEach((p, i) => {
    const c = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    c.setAttribute('cx', p.x);
    c.setAttribute('cy', p.y);
    c.setAttribute('r', '4');
    c.setAttribute('fill', anchorColor);
    c.setAttribute('class', 'pipe-anchor');
    c.style.animation = `pipeDotPop 0.4s ease-out ${i * 80 + 400}ms both`;
    c.style.opacity = '0';
    svg.appendChild(c);
  });
}

// Re-draw on resize so the path stays aligned
window.addEventListener('resize', () => {
  if (document.getElementById('dashboard-pipeline')) drawPipelineLine();
});

function safeArr(key){ try { return JSON.parse(localStorage.getItem(key) || '[]'); } catch { return []; } }
function escP(s){ return String(s==null?'':s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

// Inline SVG icons — consistent stroke-based style matching the sidebar
function svgFolder()     { return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V7z"/></svg>'; }
function svgPlug()       { return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 12h16"/><circle cx="4" cy="12" r="2.5"/><circle cx="20" cy="12" r="2.5"/><path d="M10 9v6M14 9v6"/></svg>'; }
function svgMap()        { return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 7h6l2-2M20 17h-6l-2 2"/><path d="M8 7l-4 4 4 4M16 17l4-4-4-4"/></svg>'; }
function svgCode()       { return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="16" rx="2"/><path d="M8 10l-2 2 2 2M16 10l2 2-2 2M13 9l-2 6"/></svg>'; }
function svgSparkles()   { return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3v4M12 17v4M3 12h4M17 12h4M6 6l2.5 2.5M15.5 15.5L18 18M6 18l2.5-2.5M15.5 8.5L18 6"/></svg>'; }
function svgPlay()       { return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M10 9l5 3-5 3z" fill="currentColor"/></svg>'; }
function svgCheckCircle(){ return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M8 12l3 3 5-6"/></svg>'; }
function svgShield()     { return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6z"/><path d="M9 12l2 2 4-4"/></svg>'; }

// ══════════════════════════════════════════════════════════════════════════
// DATABASE SNAPSHOT — per-project memory of database settings used
// ══════════════════════════════════════════════════════════════════════════
//
// Why this exists:
//   Connection strings live in `cygenix_project_connections` as a flat object
//   shared across the app. When a user switches between projects they lose
//   visibility of what database settings were used previously. This module
//   captures the *current* connection state into the *active* project's
//   `dbHistory[]` array, so each project keeps a record of which databases
//   it's run against — visible on the Project Status panel.
//
// Privacy / safety:
//   We parse out server, database, auth mode, username, and driver. We
//   NEVER store passwords or function keys, even though they're sometimes
//   embedded in connection strings. The parser strips them before write.

// Parse a SQL Server / Postgres connection string OR an Azure Function URL
// into a plain descriptor. Returns null if nothing identifiable can be
// extracted (e.g. empty input). This is a forgiving parser — it handles
// the three formats we know cygenix uses without choking on edge cases.
function parseDbConnection(connStr){
  if (!connStr || typeof connStr !== 'string') return null;
  const s = connStr.trim();
  if (!s) return null;

  // ─── Form 1: Azure Function URL ─────────────────────────────────────────
  // e.g. https://cygenix-db-api-xxx.uksouth-01.azurewebsites.net/api/db?code=...
  // We capture the host (everything before the first /api/) and strip the
  // function key. The host alone is enough to identify "which Azure Function
  // backend" — useful when the user has staging vs production deployments.
  if (/^https?:\/\//i.test(s)){
    try {
      const u = new URL(s);
      return {
        kind: 'azure-function',
        server: u.host,                       // e.g. cygenix-db-api-xxx.uksouth-01.azurewebsites.net
        database: '',                         // not directly visible from a function URL
        auth: 'function-key',
        username: '',
        driver: 'azure-function',
      };
    } catch { return null; }
  }

  // ─── Form 2: URL-style connection string ────────────────────────────────
  // e.g. mssql://user:pass@host:port/dbname  OR  postgresql://...
  // The Connections page stores these for direct DB connections that don't
  // go through an Azure Function.
  const urlStyle = /^(mssql|postgres|postgresql):\/\/(?:([^:@\/]+)(?::([^@\/]*))?@)?([^:\/]+)(?::(\d+))?(?:\/([^?#]+))?/i.exec(s);
  if (urlStyle){
    const driver = urlStyle[1].toLowerCase().replace('postgresql','postgres');
    const user   = urlStyle[2] || '';
    const host   = urlStyle[4] || '';
    const port   = urlStyle[5] || '';
    const db     = urlStyle[6] || '';
    return {
      kind: 'connection-string',
      server: host + (port ? ':' + port : ''),
      database: db,
      auth: user ? 'sql-auth' : 'integrated',
      username: user,                          // password deliberately discarded
      driver: driver === 'mssql' ? 'mssql' : 'postgres',
    };
  }

  // ─── Form 3: ADO.NET-style connection string ────────────────────────────
  // e.g. Server=tcp:foo.database.windows.net,1433;Database=foo;User ID=bar;Password=...;
  // Each segment is a `Key=Value` pair. We pull out the keys we recognise
  // and ignore everything else, which keeps us forward-compatible if Azure
  // adds new keys in future.
  if (s.includes('=') && (s.includes(';') || /server\s*=/i.test(s))){
    const parts = s.split(';').map(p => p.trim()).filter(Boolean);
    const map = {};
    for (const p of parts){
      const eq = p.indexOf('=');
      if (eq < 0) continue;
      const k = p.slice(0, eq).trim().toLowerCase();
      const v = p.slice(eq + 1).trim();
      map[k] = v;
    }
    const server = (map['server'] || map['data source'] || '').replace(/^tcp:/i, '');
    const db     = map['database'] || map['initial catalog'] || '';
    const user   = map['user id'] || map['uid'] || map['user'] || '';
    let auth = 'sql-auth';
    if (/yes|true|sspi/i.test(map['integrated security'] || '')) auth = 'integrated';
    if (/active directory managed identity/i.test(map['authentication'] || '')) auth = 'managed-identity';
    if (/active directory password/i.test(map['authentication'] || ''))         auth = 'aad-password';
    if (/active directory interactive/i.test(map['authentication'] || ''))      auth = 'aad-interactive';
    if (server || db){
      return { kind: 'connection-string', server, database: db, auth, username: user, driver: 'mssql' };
    }
  }

  return null;
}

// Read whatever connection info is currently configured (from
// cygenix_project_connections + sessionStorage fallbacks) and return
// {src, tgt} descriptors. Either side may be null if nothing's configured.
function readCurrentConnections(){
  // CRITICAL: connections are stored per-user inside the
  // cygenix_project_connections blob (keyed by email). Reading raw
  // localStorage gets you a confused mix of all users' data plus
  // some legacy flat-keyed fields that drift out of sync with the
  // namespaced slice. Always go through CygenixConnections.get() —
  // that's the wrapper that resolves to the current user's slice
  // (and it's what every other page uses to read the same data).
  let c = {};
  try {
    if (window.CygenixConnections && typeof window.CygenixConnections.get === 'function'){
      c = window.CygenixConnections.get() || {};
    } else {
      // Fallback only — shouldn't happen if connections.js loaded.
      // Read flat keys from raw localStorage, accepting they may be stale.
      const pc = JSON.parse(localStorage.getItem('cygenix_project_connections') || '{}');
      c = pc || {};
    }
  } catch {}

  const ss = (k) => { try { return sessionStorage.getItem(k) || ''; } catch { return ''; } };
  const get = (k) => (c[k] != null ? String(c[k]) : '');

  // Source — function URL preferred, then connection string. The
  // wrapper's keys are camelCase (srcConnString, srcFnUrl, etc).
  // sessionStorage still uses the snake_case names — left in as a
  // legacy fallback for any pre-wrapper data.
  const srcFn = get('srcFnUrl') || ss('cygenix_src_fn_url');
  const srcCs = get('srcConnString') || ss('cygenix_src_conn_string');
  const srcCandidate = (/^https?:\/\//i.test(srcFn) ? srcFn : '') || srcCs || srcFn;

  // Target — same priority. Choose whichever side actually has a
  // value, mirroring impGetConn's HTTP-vs-string routing.
  const tgtFn = get('tgtFnUrl') || ss('cygenix_fn_url');
  const tgtCs = get('tgtConnString') || ss('cygenix_conn_string');
  const tgtCandidate = (/^https?:\/\//i.test(tgtFn) ? tgtFn : '') || tgtCs || tgtFn;

  return {
    src: parseDbConnection(srcCandidate),
    tgt: parseDbConnection(tgtCandidate),
  };
}

// Append the current connection state to a project's dbHistory if it's
// not already recorded. Idempotent and side-effect-light: writes to
// localStorage only when a genuinely new descriptor is seen, so it's safe
// to call on every dashboard render. Returns the project's current
// dbHistory (post-append) for immediate use by the renderer.
function captureDbSnapshot(projectId){
  if (!projectId) return [];
  let projects = [];
  try { projects = JSON.parse(localStorage.getItem('cygenix_projects') || '[]'); } catch { return []; }
  const idx = projects.findIndex(p => p.id === projectId);
  if (idx < 0) return [];
  const project = projects[idx];

  const current = readCurrentConnections();
  const existing = Array.isArray(project.dbHistory) ? project.dbHistory.slice() : [];

  // Dedupe key — server + database + role + auth. Two identical descriptors
  // for the same role won't create duplicate history rows, but the same
  // server with a different database (common when migrating multiple
  // databases in one project) gets its own row.
  const keyOf = (entry) => [entry.role, entry.kind, entry.server, entry.database, entry.auth].join('|').toLowerCase();
  const seen = new Set(existing.map(keyOf));

  let changed = false;
  for (const role of ['src','tgt']){
    const desc = current[role];
    if (!desc) continue;
    const entry = {
      role,                                   // 'src' | 'tgt'
      kind: desc.kind,                        // 'azure-function' | 'connection-string'
      server: desc.server || '',
      database: desc.database || '',
      auth: desc.auth || '',
      username: desc.username || '',
      driver: desc.driver || '',
      firstSeen: new Date().toISOString(),
      lastSeen:  new Date().toISOString(),
    };
    const k = keyOf(entry);
    if (seen.has(k)){
      // Update lastSeen on the matching record so users can see this
      // database is still in active use.
      const match = existing.find(e => keyOf(e) === k);
      if (match){ match.lastSeen = entry.lastSeen; changed = true; }
    } else {
      existing.push(entry);
      seen.add(k);
      changed = true;
    }
  }

  if (changed){
    project.dbHistory = existing;
    project.modified = new Date().toISOString();
    projects[idx] = project;
    try { localStorage.setItem('cygenix_projects', JSON.stringify(projects)); } catch {}
  }

  return existing;
}

// Render a small list of database settings used by this project. Returns
// HTML or '' if nothing's been captured yet.
function renderDbHistory(history){
  if (!Array.isArray(history) || !history.length) return '';

  // Sort: target before source (matches the visual flow on the rest of
  // the page, source → target), and within each role newest first.
  const order = { tgt: 0, src: 1 };
  const sorted = history.slice().sort((a,b) => {
    if ((order[a.role]||9) !== (order[b.role]||9)) return (order[a.role]||9) - (order[b.role]||9);
    return String(b.lastSeen||'').localeCompare(String(a.lastSeen||''));
  });

  const fmtDate = ts => ts ? new Date(ts).toLocaleDateString('en-GB', { day:'numeric', month:'short', year:'numeric' }) : '';
  const authLabel = (a) => ({
    'sql-auth':         'SQL auth',
    'integrated':       'Windows auth',
    'managed-identity': 'Managed Identity',
    'aad-password':     'AAD password',
    'aad-interactive':  'AAD interactive',
    'function-key':     'Function key',
  })[a] || (a || 'unknown');

  // Build a lookup of saved connections, keyed by parsed identity.
  // For each user-saved connection we run its connString / fnUrl back
  // through parseDbConnection to get a {server, database, kind} tuple
  // we can match against dbHistory entries. The tuple key uses the
  // same shape as the dbHistory entries' (kind|server|database) so a
  // straightforward string lookup resolves the friendly name.
  // Users are likely to have multiple connections to the same host
  // (different databases on the same server), so the match key
  // includes database too. Multiple matches → most recent wins,
  // matching how sconnSetAll sorts entries by savedAt.
  const friendlyByKey = new Map();
  try {
    if (typeof sconnGetAll === 'function' && typeof parseDbConnection === 'function'){
      const saved = sconnGetAll();
      // Newest first so .set() with same key keeps the freshest name.
      const sortedSaved = saved.slice().sort((a,b) =>
        String(b.savedAt||'').localeCompare(String(a.savedAt||''))
      );
      for (const s of sortedSaved){
        if (!s || !s.name) continue;
        const raw = s.mode === 'azure'
          ? (s.fnUrl || '')
          // Note: connString is rehydrated by sconnGetAll on the user's
          // own browser. On a browser that doesn't have the local
          // secret, connString will be empty here — that's OK, we just
          // can't resolve the friendly name there. The host is enough
          // for Azure Function entries to still resolve.
          : (s.connString || '');
        const desc = parseDbConnection(raw);
        if (!desc) continue;
        const key = [desc.kind, desc.server || '', desc.database || ''].join('|').toLowerCase();
        if (!friendlyByKey.has(key)){
          friendlyByKey.set(key, { name: s.name, side: s.side });
        }
        // Also register a server-only fallback so Azure Function
        // entries (which have no database) still match dbHistory
        // entries that recorded a database name. Lower priority than
        // the full match — only set if not already present.
        const serverOnlyKey = [desc.kind, desc.server || '', ''].join('|').toLowerCase();
        if (!friendlyByKey.has(serverOnlyKey)){
          friendlyByKey.set(serverOnlyKey, { name: s.name, side: s.side });
        }
      }
    }
  } catch (e) {
    // Resolution is best-effort — never let it break the panel.
    console.warn('[ps-dbs] friendly-name lookup failed:', e);
  }

  function friendlyFor(e){
    const exact = friendlyByKey.get([e.kind, e.server || '', e.database || ''].join('|').toLowerCase());
    if (exact) return exact;
    const serverOnly = friendlyByKey.get([e.kind, e.server || '', ''].join('|').toLowerCase());
    return serverOnly || null;
  }

  const rows = sorted.map(e => {
    const role = e.role === 'tgt' ? 'Target' : 'Source';
    const roleClass = e.role === 'tgt' ? 'tgt' : 'src';
    const driver = e.kind === 'azure-function' ? 'Azure Fn' : (e.driver || 'sql');
    const f = friendlyFor(e);
    // Identity only: the saved connection's nickname and the database name.
    //
    // This row used to print the host and port ("86.20.94.140:1433") and the
    // login name alongside them. dbHistory never stores the password, so this
    // was not a password leak — but host + port + username is most of a
    // credential, sitting on the Dashboard's landing view where it shows up
    // in every screenshot and screen share of the project. It is also the
    // half an attacker cannot guess. Matches the Connections view, which
    // masks the same details behind an explicit Edit.
    const primary = f
      ? `<span class="ps-db-friendly" title="Saved connection name">${escP(f.name)}</span>`
      : `<span class="ps-db-friendly" title="Database">${escP(e.database || (e.kind === 'azure-function' ? 'Azure Function' : 'Direct connection'))}</span>`;
    // Only add the database as secondary text when the nickname is already
    // occupying the primary slot — otherwise it would appear twice.
    const secondary = (f && e.database)
      ? `<span class="ps-db-name" title="Database">${escP(e.database)}</span>`
      : '';
    return `
      <div class="ps-db-row">
        <span class="ps-db-role ${roleClass}">${role}</span>
        ${primary}
        ${secondary}
        <div class="ps-db-meta">
          <span title="Driver">${escP(driver)}</span>
          <span title="Authentication">${escP(authLabel(e.auth))}</span>
          <span title="First seen on this project">since ${fmtDate(e.firstSeen)}</span>
        </div>
      </div>`;
  }).join('');

  return `
    <div class="ps-dbs">
      <div class="ps-dbs-title">
        <span style="display:inline-flex;align-items:center;gap:0.45rem">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/></svg>
          Databases used
        </span>
        <button type="button" class="ps-dbs-clear" onclick="clearProjectDbHistory()"
                title="Wipe the captured history. The current active connections will be re-captured on next render.">
          Clear
        </button>
      </div>
      ${rows}
    </div>`;
}

// Wipe dbHistory for the active project and re-render. The capture
// runs again automatically on the next renderProjectStatus() pass,
// which re-populates with whatever's currently active — so this is
// effectively "reset to current state" rather than "delete forever".
function clearProjectDbHistory(){
  if (!confirm('Clear the captured database history for this project?\n\n'
      + 'Current active connections will be re-captured automatically on the next refresh.')) return;
  const activeId = localStorage.getItem('cygenix_active_project_id') || '';
  if (!activeId) return;
  try {
    const projects = JSON.parse(localStorage.getItem('cygenix_projects') || '[]');
    const idx = projects.findIndex(p => p.id === activeId);
    if (idx < 0) return;
    projects[idx].dbHistory = [];
    projects[idx].modified  = new Date().toISOString();
    localStorage.setItem('cygenix_projects', JSON.stringify(projects));
    renderProjectStatus();
  } catch (e) {
    console.error('[ps-dbs] Clear history failed:', e);
  }
}

// ══════════════════════════════════════════════════════════════════════════
// PROJECT STATUS — replaces the old Migration Pipeline visual
// Surfaces: description, job failures, mapping count + mode (AI vs system),
// conversion report status, data quality status, validation status, and
// (on demand) an AI-generated narrative summary of the project.
// All metrics read real data — no demo values.
// ══════════════════════════════════════════════════════════════════════════
function renderProjectStatus(){
  const host = document.getElementById('dashboard-project-status');
  if (!host) return;
  const subEl = document.getElementById('ps-sub');

  // Identify the active project. If none is set or it can't be found, show
  // an empty state pointing the user to Projects.
  const activeId = localStorage.getItem('cygenix_active_project_id') || '';
  const projects = safeArr('cygenix_projects');
  const project  = projects.find(p => p.id === activeId);

  if (!project){
    if (subEl) subEl.textContent = 'No active project selected';
    host.innerHTML = `
      <div class="ps-inner">
        <div class="ps-empty">
          No active project. <a href="/projects.html">Go to Projects</a> to create or activate one,
          and the status panel will populate automatically.
        </div>
      </div>`;
    return;
  }

  if (subEl) subEl.textContent = `Live snapshot of "${project.name || 'Untitled project'}"`;

  // ── Pull all per-project data ────────────────────────────────────────────
  const allJobs   = liveJobs();          // trashed jobs must not inflate the panel
  const projJobs  = allJobs.filter(j => j.projectId === project.id);
  const inv       = safeArr('cygenix_inventory');
  const projInv   = inv.filter(a => a.projectId === project.id);

  // ── Job stats & failures ─────────────────────────────────────────────────
  // Shared bucketing — this panel and the donut above it now classify a given
  // job identically.
  const failedJobs   = projJobs.filter(j => jobBucket(j) === 'failed');
  const completeJobs = projJobs.filter(j => jobBucket(j) === 'complete');

  // ── Mapping stats ────────────────────────────────────────────────────────
  // A "map" is a job that has at least one mapped column. We also count jobs
  // that look AI-assisted — heuristic: an explicit aiMapped/mappingSource
  // flag if present, otherwise jobs where columns have non-NONE transforms
  // or AI-derived target types (anything beyond a straight srcCol → tgtCol
  // copy). System-generated = jobs with mapping but no AI hints.
  const jobsWithMapping = projJobs.filter(j => Array.isArray(j.columnMapping) && j.columnMapping.some(m => m && m.tgtCol));
  const totalMaps = jobsWithMapping.length;

  function isAiMapped(j){
    if (j.aiMapped === true) return true;                    // explicit flag (future-proof)
    if (j.mappingSource && /ai|claude|agent/i.test(j.mappingSource)) return true;
    const cm = Array.isArray(j.columnMapping) ? j.columnMapping : [];
    if (!cm.length) return false;
    // Heuristic: any column with a non-trivial transform suggests AI involvement,
    // since the system generator only produces NONE / direct copies.
    return cm.some(m => m && m.transform && String(m.transform).toUpperCase() !== 'NONE');
  }
  const aiMappedJobs     = jobsWithMapping.filter(isAiMapped).length;
  const systemMappedJobs = totalMaps - aiMappedJobs;
  const mappingMode = totalMaps === 0
    ? { label: 'No mappings yet', tone: 'idle' }
    : aiMappedJobs > systemMappedJobs
      ? { label: 'Agentic (AI-assisted)', tone: 'info' }
      : aiMappedJobs > 0
        ? { label: 'Mixed AI + system', tone: 'info' }
        : { label: 'System-generated',   tone: 'ok'  };

  // ── Conversion report ────────────────────────────────────────────────────
  // Source of truth: the `project_reports` Cosmos container, queried via the
  // /.netlify/functions/reports endpoint (action: 'list'). The card renders
  // immediately in a "checking…" state and is updated asynchronously by
  // refreshConversionReportCard() once the real count comes back.
  //
  // Earlier versions of this code used a localStorage heuristic (any job
  // with tables populated counted as a "report produced") which over-
  // reported — the Reports page would be empty but the card still said 1+
  // were produced. That heuristic is gone.
  const reportPlaceholder = { state: 'loading' };  // updated post-render

  // ── Data quality review ──────────────────────────────────────────────────
  // Cygenix tracks data-cleansing rules (cygenix_wasis_rules) project-wide,
  // and per-job data quality is implied by import validation having been run.
  // We surface what we can verify: rules count + % of jobs that have any
  // tables[].rows recorded (i.e. data has been parsed and inspected).
  const wasisRules = safeArr('cygenix_wasis_rules');
  const projWasis  = wasisRules.filter(r => !r.projectId || r.projectId === project.id);
  const jobsInspected = projJobs.filter(j =>
    Array.isArray(j.tables) && j.tables.some(t => Number(t.rows) > 0)
  ).length;
  const dqRun  = jobsInspected > 0 || projWasis.length > 0;
  const dqPct  = projJobs.length > 0 ? Math.round((jobsInspected / projJobs.length) * 100) : 0;

  // ── Validation report ────────────────────────────────────────────────────
  // A job is "validated" if it has a verifySQL block of meaningful length.
  // "Run" status: lastRun timestamp exists.
  const validatedJobs = projJobs.filter(j => j.verifySQL && String(j.verifySQL).trim().length > 20);
  const validationRun = projJobs.some(j => j.verifySQL && (j.lastRun || j.completedAt || j.executedAt));
  const valPct = projJobs.length > 0 ? Math.round((validatedJobs.length / projJobs.length) * 100) : 0;

  // ── Migration description (what & where) ─────────────────────────────────
  const fmtDate = ts => ts ? new Date(ts).toLocaleDateString('en-GB', { day:'numeric', month:'short', year:'numeric' }) : '—';
  const projDesc = (project.description || '').trim();
  // Build a one-line "what / where" derived from project + jobs even if no
  // free-text description exists yet.
  const distinctSources = [...new Set(projJobs.map(j => j.source).filter(Boolean))];
  const distinctTargets = [...new Set(projJobs.map(j => j.target).filter(Boolean))];
  const totalRows = projJobs.reduce((s,j) => s + (Number(j.totalRows) || 0), 0);

  const whereLine = (() => {
    const src = project.srcSystem || (distinctSources.length === 1 ? distinctSources[0] : (distinctSources.length ? `${distinctSources.length} sources` : '—'));
    const tgt = project.tgtSystem || (distinctTargets.length === 1 ? distinctTargets[0] : (distinctTargets.length ? `${distinctTargets.length} targets` : '—'));
    return `${escP(src)} → ${escP(tgt)}`;
  })();

  // ── Card builders ────────────────────────────────────────────────────────
  function card({ title, icon, status, value, valueSub, body, foot, onClick }){
    const cls = ['ps-card', `status-${status || 'idle'}`, onClick ? 'is-clickable' : ''].filter(Boolean).join(' ');
    return `
      <div class="${cls}" ${onClick ? `data-action="${onClick}"` : ''}>
        <div class="ps-card-head">
          <div class="ps-card-title">${escP(title)}</div>
          <div class="ps-card-icon">${icon}</div>
        </div>
        ${value !== undefined ? `<div class="ps-card-value">${value}${valueSub ? `<span class="ps-card-value-sub">${valueSub}</span>` : ''}</div>` : ''}
        ${body || ''}
        ${foot ? `<div class="ps-card-foot">${foot}</div>` : ''}
      </div>`;
  }

  // ── Card 1: Failures ─────────────────────────────────────────────────────
  const failureCard = card({
    title: 'Job Failures',
    icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 8v5M12 16.5v0.5"/></svg>',
    status: failedJobs.length > 0 ? 'err' : (projJobs.length === 0 ? 'idle' : 'ok'),
    value: String(failedJobs.length),
    valueSub: ` of ${projJobs.length} ${projJobs.length === 1 ? 'job' : 'jobs'}`,
    body: failedJobs.length > 0
      ? `<ul class="ps-failed-list">${failedJobs.slice(0,3).map(j => `<li title="${escP(j.name)}">${escP(j.name || j.id)}</li>`).join('')}${failedJobs.length > 3 ? `<li class="more">+${failedJobs.length-3} more</li>` : ''}</ul>`
      : projJobs.length === 0
        ? '<div class="ps-card-sub">No jobs yet — create one from the New Job page.</div>'
        : `<div class="ps-card-sub">${completeJobs.length} complete · 0 failures</div>`,
    foot: `<span>Click to view all jobs</span><span>→</span>`,
    onClick: 'all-jobs',
  });

  // ── Card 2: Maps & Jobs ──────────────────────────────────────────────────
  const mappingCard = card({
    title: 'Maps & Jobs',
    icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M9 4l-5 2v14l5-2 6 2 5-2V4l-5 2z"/><path d="M9 4v14M15 6v14"/></svg>',
    status: totalMaps > 0 ? 'ok' : 'idle',
    value: String(totalMaps),
    valueSub: ` map${totalMaps === 1 ? '' : 's'} / ${projJobs.length} job${projJobs.length === 1 ? '' : 's'}`,
    body: `<div class="ps-card-sub">
      <span class="ps-pill ps-pill-${mappingMode.tone === 'info' ? 'info' : (mappingMode.tone === 'ok' ? 'ok' : 'idle')}">${escP(mappingMode.label)}</span>
      ${totalMaps > 0 ? `<div style="margin-top:0.5rem;font-family:var(--mono);font-size:11px;color:var(--text3)">${aiMappedJobs} AI-assisted · ${systemMappedJobs} system</div>` : ''}
    </div>`,
    foot: `<span>Open Object Mapping</span><span>→</span>`,
    onClick: 'mapping',
  });

  // ── Card 3: Conversion Report ────────────────────────────────────────────
  // Renders in a "checking…" state. refreshConversionReportCard() updates
  // it after the dashboard mounts, by fetching the real count from Cosmos.
  const reportCard = `
    <div class="ps-card status-idle" id="ps-card-report" data-action="reports" style="cursor:pointer">
      <div class="ps-card-head">
        <div class="ps-card-title">Conversion Report</div>
        <div class="ps-card-icon">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M6 3h9l5 5v13a1 1 0 0 1-1 1H6a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1z"/><path d="M14 3v6h6M9 14h6M9 17h6M9 11h2"/></svg>
        </div>
      </div>
      <div class="ps-card-value" id="ps-card-report-value">—</div>
      <div class="ps-card-sub" id="ps-card-report-body">
        <span class="ps-pill ps-pill-idle">Checking…</span>
      </div>
      <div class="ps-card-foot"><span>Open Reports</span><span>→</span></div>
    </div>`;

  // ── Card 4: Data Quality Review ──────────────────────────────────────────
  const dqCard = card({
    title: 'Data Quality Review',
    icon: svgSparkles(),
    status: !dqRun ? 'idle' : (dqPct >= 80 ? 'ok' : (dqPct >= 40 ? 'warn' : 'warn')),
    value: dqRun ? `${dqPct}%` : '—',
    valueSub: dqRun ? ` of jobs inspected` : '',
    body: `<div class="ps-card-sub">
      <span class="ps-pill ps-pill-${dqRun ? (dqPct >= 80 ? 'ok' : 'warn') : 'idle'}">${dqRun ? 'Run' : 'Not run'}</span>
      <div style="margin-top:0.5rem;font-family:var(--mono);font-size:11px;color:var(--text3)">${jobsInspected}/${projJobs.length} jobs · ${projWasis.length} cleansing rule${projWasis.length===1?'':'s'}</div>
    </div>`,
    foot: `<span>Open Cleansing</span><span>→</span>`,
    onClick: 'cleansing',
  });

  // ── Card 5: Validation Report ────────────────────────────────────────────
  const valCard = card({
    title: 'Validation Report',
    icon: svgCheckCircle(),
    status: validatedJobs.length === 0 ? 'idle' : (validationRun ? (valPct >= 80 ? 'ok' : 'warn') : 'warn'),
    value: validatedJobs.length === 0 ? '—' : `${valPct}%`,
    valueSub: validatedJobs.length === 0 ? '' : ` of jobs validated`,
    body: `<div class="ps-card-sub">
      <span class="ps-pill ps-pill-${validatedJobs.length === 0 ? 'idle' : (validationRun ? 'ok' : 'warn')}">${validatedJobs.length === 0 ? 'No checks set' : (validationRun ? 'Run' : 'Set, not run')}</span>
      <div style="margin-top:0.5rem;font-family:var(--mono);font-size:11px;color:var(--text3)">${validatedJobs.length}/${projJobs.length} jobs have verify SQL</div>
    </div>`,
    foot: `<span>Open Validation</span><span>→</span>`,
    onClick: 'validation',
  });

  // ── Card 6: Migration scope ──────────────────────────────────────────────
  const scopeCard = card({
    title: 'Migration Scope',
    icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 8h16M4 12h16M4 16h10"/></svg>',
    status: totalRows > 0 ? 'ok' : 'idle',
    value: totalRows > 0 ? totalRows.toLocaleString() : '—',
    valueSub: totalRows > 0 ? ` row${totalRows === 1 ? '' : 's'}` : '',
    body: `<div class="ps-card-sub" style="font-family:var(--mono);font-size:11.5px;line-height:1.55">
      <div style="color:var(--text2)">${whereLine}</div>
      <div style="color:var(--text3);margin-top:0.25rem">${distinctSources.length || 0} source · ${distinctTargets.length || 0} target tbl(s)</div>
    </div>`,
    foot: `<span>Phase: ${escP(project.phase || '—')}</span><span>${fmtDate(project.modified || project.created)}</span>`,
  });

  // Capture the current connection state into the project's dbHistory.
  // Idempotent — if the same descriptors are already recorded, this is a
  // cheap dedupe lookup. Returns the up-to-date dbHistory for rendering.
  const dbHistory = captureDbSnapshot(project.id);

  // ── Compose the panel ────────────────────────────────────────────────────
  const projHeadHtml = `
    <div class="ps-head">
      <div class="ps-head-left">
        <div class="ps-proj-name">
          ${escP(project.name || 'Untitled project')}
          <span class="ps-active-pill">active</span>
        </div>
        <div class="ps-proj-meta">
          ${project.client ? `<span>Client: ${escP(project.client)}</span>` : ''}
          ${project.ref ? `<span>Ref: ${escP(project.ref)}</span>` : ''}
          <span>Started: ${fmtDate(project.start)}</span>
          <span>Target end: ${fmtDate(project.end)}</span>
        </div>
        ${projDesc ? `<div class="ps-proj-desc">${escP(projDesc)}</div>` : ''}
        ${renderDbHistory(dbHistory)}
      </div>
    </div>`;

  // AI summary panel — gated behind a button so we don't burn API tokens on
  // every dashboard render. Cached per project; cache is invalidated on
  // project change or manual regen.
  const cacheKey = `cygenix_ps_ai_${project.id}`;
  let cached = null;
  try { cached = JSON.parse(localStorage.getItem(cacheKey) || 'null'); } catch {}

  const aiHtml = `
    <div class="ps-ai" id="ps-ai-block">
      <div class="ps-ai-head">
        <div class="ps-ai-title">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3l2 5 5 2-5 2-2 5-2-5-5-2 5-2z"/></svg>
          AI Summary
        </div>
        <div style="display:flex;gap:0.4rem;align-items:center">
          <button id="ps-ai-btn" class="btn btn-ghost btn-sm" onclick="generateProjectAiSummary()" style="font-size:11px;padding:0.35rem 0.7rem">
            ${cached ? '↻ Regenerate' : 'Generate summary'}
          </button>
        </div>
      </div>
      <div id="ps-ai-body" class="ps-ai-body${cached ? '' : ' empty'}">${cached ? escP(cached.text) : 'Click "Generate summary" for an AI-written narrative of the current state of this project.'}</div>
      ${cached ? `<div class="ps-ai-meta">Generated ${new Date(cached.ts).toLocaleString('en-GB')} · model ${escP(cached.model || CygenixModel.primary())}</div>` : ''}
    </div>`;

  host.innerHTML = `
    <div class="ps-inner">
      ${projHeadHtml}
      <div class="ps-grid">
        ${failureCard}
        ${mappingCard}
        ${reportCard}
        ${dqCard}
        ${valCard}
        ${scopeCard}
      </div>
      ${aiHtml}
    </div>`;

  // Wire card clicks. Using a small action map keeps inline handlers out of
  // the markup and lets us update navigation targets in one place.
  const actions = {
    'all-jobs':   () => showView('all-jobs'),
    'mapping':    () => { window.location.href = '/object_mapping.html'; },
    'reports':    () => showView('reports'),
    'cleansing':  () => { window.location.href = '/data-cleansing.html'; },
    'validation': () => { window.location.href = '/validation.html'; },
  };
  // Match every card with a data-action attribute — covers both the
  // generic .is-clickable cards built by the card() helper and the
  // bespoke report card (which has its own structure).
  host.querySelectorAll('.ps-card[data-action]').forEach(el => {
    const key = el.dataset.action;
    if (actions[key]) el.addEventListener('click', actions[key]);
  });

  // Kick off async refresh of the Conversion Report card. This runs out-of-
  // band so the rest of the dashboard renders synchronously without waiting
  // on Cosmos. Failures are surfaced inline on the card itself.
  refreshConversionReportCard(project.id);

  // Schedule a one-shot delayed re-render to catch the case where
  // Cosmos sync arrived after our first render. The first paint reads
  // from whatever's in localStorage right now; if the cloud has fresher
  // data (a saved connection added on another browser, secrets
  // rehydrated after this thread but before the sync settles), the
  // delayed re-render picks it up.
  //
  // CRITICAL: this is a ONE-SHOT scheduler, not a recurring one. The
  // flag is set the first time renderProjectStatus runs on this page
  // and NEVER reset — every subsequent call sees the flag set and
  // skips scheduling. Without this, the re-render in the setTimeout
  // calls renderProjectStatus again, which schedules another
  // re-render, which calls renderProjectStatus again, ad infinitum at
  // 1.2-second intervals. Each render fires a /api/data/reports call,
  // so the loop ALSO hammers the reports endpoint at ~1 req/s.
  if (!window._psDelayedRerenderFired) {
    window._psDelayedRerenderFired = true;  // never reset
    setTimeout(() => {
      try { renderProjectStatus(); } catch {}
    }, 1200);
  }
}

// ── Async refresh of the Conversion Report card ─────────────────────────

// ── Shared reports-list fetch ────────────────────────────────────────────
// The Home card, the Reports view and the search prefetch all want the same
// { action:'list' } response. Share one in-flight promise with a 30s TTL so
// a burst of view switches costs one request; anything that mutates reports
// (save/delete) calls invalidateReportsList() to force the next read fresh.
var _reportsListShared;          // { at, promise } — var: callers may boot above this line
function fetchReportsList() {
  const now = Date.now();
  if (_reportsListShared && now - _reportsListShared.at < 30000) return _reportsListShared.promise;
  const promise = (async () => {
    const r = await window.cygenixFetch('/.netlify/functions/reports', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'list' }),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || ('HTTP ' + r.status));
    return Array.isArray(data.reports) ? data.reports : [];
  })();
  _reportsListShared = { at: now, promise };
  promise.catch(() => { _reportsListShared = null; });
  return promise;
}
function invalidateReportsList() { _reportsListShared = null; }

// Queries the Cosmos `project_reports` container via the reports Netlify
// function and updates the card with the real count of saved reports
// scoped to the active project. Earlier dashboard versions inferred a
// "report exists" status from local heuristics (any job with rows
// counted), which over-reported. This is the source of truth.
async function refreshConversionReportCard(projectId){
  const cardEl = document.getElementById('ps-card-report');
  const valEl  = document.getElementById('ps-card-report-value');
  const bodyEl = document.getElementById('ps-card-report-body');
  if (!cardEl || !valEl || !bodyEl) return;

  // SAFETY: rate-limit this function to defend against ever
  // re-introducing a render loop. Two guards:
  //   1. In-flight guard — if a fetch is already running, drop the
  //      duplicate call entirely. Prevents fan-out when multiple
  //      pages of code converge on the same render path.
  //   2. Minimum-interval guard — if a successful (or even failed)
  //      fetch completed in the last 3 seconds, return the cached
  //      result without re-hitting the API. Three seconds is long
  //      enough to absorb an accidental re-render storm without
  //      noticeably delaying user-driven refreshes.
  // Both flags live on the function object itself so they survive
  // across calls without polluting window.
  const MIN_INTERVAL_MS = 3000;
  const now = Date.now();
  if (refreshConversionReportCard._inFlight) return;
  if (refreshConversionReportCard._lastCallAt &&
      now - refreshConversionReportCard._lastCallAt < MIN_INTERVAL_MS) return;
  refreshConversionReportCard._inFlight   = true;
  refreshConversionReportCard._lastCallAt = now;

  // The reports function requires an Entra ID token via Authorization
  // header. cygenixFetch attaches it automatically.
  if (typeof window.cygenixFetch !== 'function'){
    cardEl.classList.remove('status-ok','status-warn','status-err');
    cardEl.classList.add('status-idle');
    valEl.textContent = '—';
    bodyEl.innerHTML = '<span class="ps-pill ps-pill-idle">Auth not loaded</span>';
    refreshConversionReportCard._inFlight = false;
    return;
  }

  try {
    const all = await fetchReportsList();
    // Scope to the active project. Older reports may pre-date the
    // projectId field — those are excluded from the count rather than
    // attributed to the active project, since we can't verify ownership.
    const forThisProject = all.filter(rep => rep && rep.projectId === projectId);
    const count = forThisProject.length;

    cardEl.classList.remove('status-ok','status-warn','status-err','status-idle');
    if (count > 0){
      cardEl.classList.add('status-ok');
      valEl.innerHTML = String(count) + '<span class="ps-card-value-sub"> saved</span>';
      // Show the most recently saved report's date, if available.
      const latest = forThisProject
        .map(rep => rep.savedAt ? new Date(rep.savedAt).getTime() : 0)
        .reduce((a,b) => Math.max(a,b), 0);
      const latestStr = latest
        ? new Date(latest).toLocaleDateString('en-GB', { day:'numeric', month:'short', year:'numeric' })
        : '';
      bodyEl.innerHTML = '<span class="ps-pill ps-pill-ok">Generated</span>'
        + (latestStr
            ? '<div style="margin-top:0.5rem;color:var(--text3);font-size:11px;font-family:var(--mono)">Most recent: ' + latestStr + '</div>'
            : '');
    } else {
      cardEl.classList.add('status-idle');
      valEl.textContent = '—';
      // Distinguish "no reports for this project" from "no reports at all
      // across any project" — the latter is more useful for new users.
      const otherProjects = all.length - forThisProject.length;
      bodyEl.innerHTML = '<span class="ps-pill ps-pill-idle">Not yet produced</span>'
        + '<div style="margin-top:0.5rem;color:var(--text3);font-size:11px">'
        + (otherProjects > 0
            ? otherProjects + ' report' + (otherProjects===1?'':'s') + ' saved under other projects.'
            : 'Run a migration in Projects → Execute, then click Save Report.')
        + '</div>';
    }
  } catch (e) {
    const msg = String(e && e.message || e || '');
    const isAuth = /unauthor|missing authorization|bearer|token/i.test(msg);
    cardEl.classList.remove('status-ok','status-warn','status-err','status-idle');
    cardEl.classList.add(isAuth ? 'status-warn' : 'status-err');
    valEl.textContent = '—';
    bodyEl.innerHTML = '<span class="ps-pill ps-pill-' + (isAuth?'warn':'err') + '">'
      + (isAuth ? 'Sign in required' : 'Lookup failed')
      + '</span>'
      + '<div style="margin-top:0.5rem;color:var(--text3);font-size:11px">'
      + (isAuth ? 'Sign out and back in to refresh your session.' : msg.slice(0, 120))
      + '</div>';
  } finally {
    // Always clear the in-flight flag — both success and failure
    // paths land here. Without this, a single failed fetch would
    // permanently block all future refreshes of this card.
    refreshConversionReportCard._inFlight = false;
  }
}

// ── AI summary generator ─────────────────────────────────────────────────
// Calls Claude with a compact JSON snapshot of the current project state
// and renders the prose response inline. Result is cached in localStorage
// against the project id so re-opening the dashboard doesn't re-charge the
// API. User can hit "Regenerate" to refresh.
async function generateProjectAiSummary(){
  const btn      = document.getElementById('ps-ai-btn');
  const bodyEl   = document.getElementById('ps-ai-body');
  const blockEl  = document.getElementById('ps-ai-block');
  if (!btn || !bodyEl || !blockEl) return;

  const apiKey = (typeof getApiKey === 'function' && getApiKey())
              || localStorage.getItem('cygenix_api_key')
              || sessionStorage.getItem('cygenix_api_key')
              || '';
  if (!apiKey){
    bodyEl.classList.remove('empty');
    // The header "Claude AI · Connected" badge can be green when the server-side
    // key (Netlify function) is configured even if the browser has no key. The
    // AI summary feature calls Claude direct from the browser, so it needs a
    // key in localStorage/sessionStorage. Diagnose which case we're in and tell
    // the user the truth rather than a generic "no key set" message that
    // contradicts the green header badge.
    bodyEl.textContent = 'Checking key location…';
    let serverHasKey = false;
    try {
      const r = await fetch('/.netlify/functions/health', { signal: AbortSignal.timeout(5000) });
      if (r.ok){
        const d = await r.json().catch(() => ({}));
        serverHasKey = !!d.apiKeyConfigured;
      }
    } catch {}
    if (serverHasKey){
      bodyEl.innerHTML = 'The "Claude AI · Connected" badge is green because an API key is configured on the Cygenix server, '
        + 'but the AI summary calls Claude directly from your browser and needs a key here too. '
        + 'Open <strong>Settings → API Key</strong> and paste your Anthropic API key, then try again. '
        + '<span style="color:var(--text3);display:block;margin-top:0.4rem;font-size:11px;font-family:var(--mono)">'
        + '(A future update can route this through the server-side key — let me know if you\'d like that.)</span>';
    } else {
      bodyEl.textContent = 'No Anthropic API key set. Open Settings → API Key to add one, then try again.';
    }
    return;
  }

  const activeId = localStorage.getItem('cygenix_active_project_id') || '';
  const project  = safeArr('cygenix_projects').find(p => p.id === activeId);
  if (!project){ bodyEl.textContent = 'No active project.'; return; }

  // Build a compact, redacted snapshot — no row data, just structure & status.
  const allJobs  = liveJobs();           // don't describe trashed jobs to the model
  const projJobs = allJobs.filter(j => j.projectId === project.id);
  const projInv  = safeArr('cygenix_inventory').filter(a => a.projectId === project.id);
  const wasis    = safeArr('cygenix_wasis_rules').filter(r => !r.projectId || r.projectId === project.id);

  const snapshot = {
    project: {
      name: project.name || '',
      client: project.client || '',
      description: project.description || '',
      srcSystem: project.srcSystem || '',
      tgtSystem: project.tgtSystem || '',
      phase: project.phase || '',
      start: project.start || '',
      end: project.end || '',
    },
    jobCount: projJobs.length,
    // Bucketed, so the narrative the model writes matches the numbers the
    // user is looking at rather than the raw status spellings.
    statusCounts: projJobs.reduce((acc,j) => {
      const s = jobBucket(j);
      acc[s] = (acc[s] || 0) + 1;
      return acc;
    }, {}),
    failedJobNames: projJobs.filter(j => /fail|error/i.test(String(j.executionStatus||j.status||''))).map(j => j.name).slice(0,10),
    mappedJobs: projJobs.filter(j => Array.isArray(j.columnMapping) && j.columnMapping.some(m => m && m.tgtCol)).length,
    validatedJobs: projJobs.filter(j => j.verifySQL && String(j.verifySQL).trim().length > 20).length,
    totalRows: projJobs.reduce((s,j) => s + (Number(j.totalRows) || 0), 0),
    distinctSources: [...new Set(projJobs.map(j => j.source).filter(Boolean))].slice(0,15),
    distinctTargets: [...new Set(projJobs.map(j => j.target).filter(Boolean))].slice(0,15),
    cleansingRules: wasis.length,
    artifactCount: projInv.length,
    artifactTypes: [...new Set(projInv.map(a => a.type).filter(Boolean))],
    // Database history captured per project — useful to the engineer
    // because conn strings live in shared storage and this is the only
    // place per-project DB usage is recorded.
    dbHistory: (project.dbHistory || []).slice(0, 8).map(e => ({
      role: e.role, server: e.server, database: e.database, auth: e.auth, driver: e.driver,
    })),
  };

  const prompt = `You are summarising the live state of a data migration project for an experienced migration engineer's dashboard. Read the JSON below and write 4-6 plain-English sentences (around 80-120 words total) describing:
1. What is being migrated, from where to where, and the nature of the data.
2. Current progress (jobs complete vs failed vs pending).
3. Whether mapping, conversion reports, data-quality review, and validation have been done.
4. The most useful next action the engineer should take.

Be direct and factual — no marketing tone, no bullet points, no headers, just prose. If something is missing or not yet run, say so plainly. Use British English.

Project state:
${JSON.stringify(snapshot, null, 2)}`;

  // UI: loading
  btn.disabled = true;
  const originalLabel = btn.textContent;
  btn.innerHTML = '<span class="ps-ai-loading">Generating</span>';
  bodyEl.classList.remove('empty');
  bodyEl.textContent = 'Asking Claude…';

  try {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
        'anthropic-dangerous-direct-browser-access': 'true'
      },
      body: JSON.stringify({
        model: CygenixModel.primary(),
        max_tokens: 600,
        messages: [{ role: 'user', content: prompt }]
      })
    });
    if (!res.ok){
      const errText = await res.text();
      throw new Error(`Claude API error (${res.status}): ${errText.slice(0,200)}`);
    }
    const data = await res.json();
    const text = (data.content || []).map(b => b.text || '').join('').trim();
    if (!text) throw new Error('Claude returned an empty response.');

    // Cache and render
    const payload = { text, ts: Date.now(), model: CygenixModel.primary() };
    try { localStorage.setItem(`cygenix_ps_ai_${project.id}`, JSON.stringify(payload)); } catch {}

    bodyEl.textContent = text;
    btn.disabled = false;
    btn.textContent = '↻ Regenerate';

    // Add or replace the "Generated …" meta line
    let metaEl = blockEl.querySelector('.ps-ai-meta');
    if (!metaEl){
      metaEl = document.createElement('div');
      metaEl.className = 'ps-ai-meta';
      blockEl.appendChild(metaEl);
    }
    metaEl.textContent = `Generated ${new Date(payload.ts).toLocaleString('en-GB')} · model ${payload.model}`;
  } catch(err){
    bodyEl.textContent = `Could not generate summary: ${err.message}`;
    btn.disabled = false;
    btn.textContent = originalLabel || 'Generate summary';
  }
}

function renderAllJobs() {
  // Populate the project filter dropdown from cygenix_projects (+ Active / All / Unassigned entries)
  const select = document.getElementById('jobs-project-filter');
  let projects = [];
  try { projects = JSON.parse(localStorage.getItem('cygenix_projects')||'[]'); } catch {}
  const activeId = localStorage.getItem('cygenix_active_project_id') || '';
  const currentValue = select?.value || 'active';

  if (select){
    const sortedProjects = [...projects].sort((a,b) => (a.name||'').localeCompare(b.name||''));
    const options = [
      `<option value="active">Active project${activeId ? (' — ' + (projects.find(p=>p.id===activeId)?.name || '(unknown)')) : ''}</option>`,
      `<option value="__all">All projects</option>`,
      `<option value="__unassigned">Unassigned (no project)</option>`,
      `<option disabled>──────────</option>`,
      ...sortedProjects.map(p => `<option value="${p.id}">${(p.name||'(unnamed)').replace(/</g,'&lt;')}</option>`)
    ];
    select.innerHTML = options.join('');
    // Try to keep the previous selection; fall back to 'active'
    const valid = Array.from(select.options).some(o => o.value === currentValue);
    select.value = valid ? currentValue : 'active';
  }

  // Apply filter
  const filterValue = select?.value || 'active';
  let shown = state.jobs;
  if (filterValue === 'active'){
    shown = state.jobs.filter(j => (j.projectId||'') === activeId);
  } else if (filterValue === '__all'){
    shown = state.jobs;
  } else if (filterValue === '__unassigned'){
    shown = state.jobs.filter(j => !j.projectId);
  } else {
    shown = state.jobs.filter(j => j.projectId === filterValue);
  }

  // Soft-delete filter. By default we hide _deleted jobs from every project
  // view. When the user clicks "Show deleted" we flip to showing ONLY the
  // deleted ones — gives them a focused recovery view without polluting
  // the normal job list.
  if (_showDeletedJobs){
    shown = shown.filter(j => j._deleted);
  } else {
    shown = shown.filter(j => !j._deleted);
  }

  // Count display
  const countEl = document.getElementById('jobs-filter-count');
  if (countEl){
    // Denominator counts live jobs only. It was state.jobs.length, which
    // included the trash, so a single deleted job made an unfiltered view
    // read "4 of 5" with nothing hidden. When the user is deliberately
    // browsing the trash, the trash is the total.
    const denom = _showDeletedJobs
      ? state.jobs.filter(j => j && j._deleted).length
      : liveJobs(state.jobs).length;
    countEl.textContent = (filterValue === '__all' && !_showDeletedJobs)
      ? `${denom} total`
      : `${shown.length} of ${denom}`;
  }

  $('all-jobs').innerHTML = shown.length
    ? jobsTableHTML(shown)
    : '<div style="padding:2rem;text-align:center;color:var(--text3);font-size:12px;background:var(--bg2);border:0.5px solid var(--border);border-radius:var(--r-lg)">No jobs match this filter.</div>';

  // Sync bulk-select UI with what's on screen: drop any previously-selected IDs that are no longer visible
  const visibleIds = new Set(shown.map(j => j.id));
  for (const id of [...selectedJobIds]){
    if (!visibleIds.has(id)) selectedJobIds.delete(id);
  }
  refreshJobsMoveTargetDropdown();
  refreshJobsBulkBar();
}

function codePanel(job, type) {
  const content = type==='schema' ? job.schemaSQL : job.migrationSQL;
  const label   = type==='schema' ? 'Schema SQL' : 'Migration SQL';
  const btnClass = type==='schema' ? 'btn-teal' : 'btn-primary';
  return `<div class="panel" style="margin-bottom:1rem">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.75rem;gap:0.5rem;flex-wrap:wrap">
      <div style="font-size:14px;font-weight:600">${job.name}</div>
      <div style="display:flex;gap:0.5rem;align-items:center">
        <span style="font-size:11px;color:var(--text3);font-family:var(--mono)">${job.totalRows?.toLocaleString()||'?'} rows · ${Math.ceil((content||'').length/1024)}KB</span>
        <button class="btn ${btnClass} btn-sm" onclick="downloadJobSQL('${job.id}','${type}')"><i class="ic ic-download"></i> Download</button>
      </div>
    </div>
    <div style="position:relative">
      <button class="copy-btn" onclick="navigator.clipboard.writeText(this.nextElementSibling.textContent)">Copy</button>
      <div class="code-block">${escHtml(content||'')}</div>
    </div>
  </div>`;
}

function renderSchemaLibrary() {
  const w = state.jobs.filter(j=>j.schemaSQL);
  $('schema-library').innerHTML = w.length ? w.map(j=>codePanel(j,'schema')).join('')
    : '<div class="empty-state"><h3>No schema SQL yet</h3><p>Run a migration to generate SQL Server CREATE TABLE statements.</p></div>';
}
function renderMigrationLibrary() {
  const w = state.jobs.filter(j=>j.migrationSQL);
  $('migration-library').innerHTML = w.length ? w.map(j=>codePanel(j,'migration')).join('')
    : '<div class="empty-state"><h3>No migration SQL yet</h3><p>Run a migration to generate INSERT statements.</p></div>';
}
function renderAuditLog() {
  // Two trails. The organisation trail is server-side, append-only and
  // hash-chained — the evidence an auditor can use. The browser trail
  // below it is the legacy per-machine log, kept for continuity.
  const localHtml = state.auditLog.length
    ? '<div class="panel"><div style="font-size:12px;font-weight:600;margin-bottom:6px">This browser (legacy)</div>'
      + '<table class="jobs-table"><thead><tr><th>Time</th><th>Event</th></tr></thead><tbody>' +
      state.auditLog.map(e=>`<tr><td style="font-family:var(--mono);font-size:11px;color:var(--text3);white-space:nowrap">${e.time}</td><td style="font-size:12px;color:var(--text2)">${e.msg}</td></tr>`).join('') +
      '</tbody></table></div>'
    : '';
  $('audit-log-wrap').innerHTML =
    '<div class="panel" style="margin-bottom:1rem"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">'
    + '<div style="font-size:12px;font-weight:600">Organisation audit trail <span style="font-weight:400;color:var(--text3)">server-side · append-only · hash-chained</span></div>'
    + '<a class="btn btn-ghost btn-sm" href="/user_roles.html">Users &amp; Roles →</a></div>'
    + '<div id="server-audit-body" style="font-size:12px;color:var(--text2)">Loading…</div></div>'
    + (localHtml || '<div class="empty-state"><h3>No browser-local entries</h3><p>Server-side events appear in the organisation trail above.</p></div>');
  fetchServerAudit();
}
async function fetchServerAudit() {
  const el = $('server-audit-body');
  if (!el) return;
  try {
    const token = (typeof getCygenixIdToken === 'function') ? getCygenixIdToken() : '';
    if (!token) { el.textContent = 'Sign in to view the organisation trail.'; return; }
    const r = await fetch('/.netlify/functions/rbac-admin?what=audit&limit=50', { headers: { Authorization: 'Bearer ' + token } });
    const d = await r.json();
    if (!r.ok) throw new Error(d.error || ('HTTP ' + r.status));
    el.innerHTML = (d.entries && d.entries.length)
      ? '<table class="jobs-table"><thead><tr><th>When</th><th>Actor</th><th>Action</th><th>Env</th><th>Outcome</th></tr></thead><tbody>'
        + d.entries.map(e => '<tr>'
          + `<td style="font-family:var(--mono);font-size:11px;color:var(--text3);white-space:nowrap">${escHtml((e.occurredAt||'').slice(0,19).replace('T',' '))}</td>`
          + `<td style="font-size:11.5px">${escHtml(e.actorEmail||'—')}</td>`
          + `<td style="font-family:var(--mono);font-size:11.5px">${escHtml(e.action)}</td>`
          + `<td style="font-size:11px;color:${e.environment==='PROD'?'var(--red)':'var(--text2)'}">${escHtml(e.environment||'')}</td>`
          + `<td style="font-size:11.5px;color:${e.outcome==='denied'?'var(--red)':'var(--green)'}">${escHtml(e.outcome)}${e.detail&&e.detail.reason?` <span style=\"color:var(--text3)\">· ${escHtml(e.detail.reason)}</span>`:''}</td>`
          + '</tr>').join('')
        + '</tbody></table>'
        + `<div style="font-size:11px;color:var(--text3);margin-top:5px">${d.total} entr${d.total===1?'y':'ies'} total · latest ${d.entries.length} shown · full trail, verification and export on the Users &amp; Roles page</div>`
      : 'No entries yet — gated actions (role changes, classified writes, refusals) appear here as they happen.';
  } catch (e) {
    el.textContent = 'Organisation trail unavailable: ' + e.message;
  }
}

function escHtml(s) { return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function escapeAttr(s) { return s.replace(/'/g,"\\'").replace(/"/g,'&quot;'); }


// ── User + Project ─────────────────────────────────────────────────────────────
function initUserAndProject() {
  // Load user — check Entra session first, then legacy fallbacks.
  //
  // RETRY BEHAVIOUR: MSAL's account records are written to localStorage
  // asynchronously after the msal-browser.min.js script finishes loading
  // and initialising. If initUserAndProject runs before MSAL has
  // populated localStorage (a real race on slower networks or first
  // paint), every email-lookup branch comes up empty, the `if (email)`
  // block at the end never runs, and the user pill stays on the
  // "Loading…" placeholder forever.
  //
  // The fix: if no email is found AND we haven't exhausted retries yet,
  // schedule another attempt 250ms later. Caps at 8 attempts (~2s)
  // before giving up — long enough to cover MSAL init under load,
  // short enough that genuine "not signed in" cases still resolve
  // quickly. The retry is keyed on a single closure counter so multiple
  // calls don't pile up retry chains.
  const MAX_ATTEMPTS = 8;
  const RETRY_DELAY_MS = 250;
  initUserAndProject._attempts = (initUserAndProject._attempts || 0);

  // Load user — check Entra session first, then legacy fallbacks
  let resolvedEmail = '';
  try {
    let email = '', name = '';

    // -1. cygenix_entra_account — the deterministic record written by
    //     login.html after a successful MSAL sign-in. Contains email,
    //     name, userId, idToken, exp. We check this FIRST because:
    //     (a) it's exactly the same record cygenixFetch uses to attach
    //         the Authorization header — keeping the pill and the API
    //         calls in lockstep means a green pill implies a working
    //         token and vice versa.
    //     (b) MSAL's localStorage cache can accumulate multiple account
    //         entries across sessions (Demo + Cygenix Admin, or stale
    //         partial records from interrupted sign-ins), and
    //         getAllAccounts()[0] is non-deterministic across them — we
    //         saw the pill render as "Cygenix Admin" or a synthetic
    //         localAccountId GUID instead of the actually-signed-in
    //         identity. Trusting login.html's authoritative single
    //         write avoids that ambiguity entirely.
    //     If this record is missing or has no email, we fall through to
    //     the MSAL fallbacks below — those still work for first-load
    //     edge cases where login.html hasn't written yet.
    try {
      const entraRaw = sessionStorage.getItem('cygenix_entra_account')
                    || localStorage.getItem('cygenix_entra_account');
      if (entraRaw) {
        const entra = JSON.parse(entraRaw);
        if (entra && typeof entra.email === 'string' && entra.email.includes('@')) {
          email = entra.email.trim().toLowerCase();
          name  = entra.name || '';
        }
      }
    } catch {}

    // 0. MSAL account cache (primary since Entra migration).
    //    Works whether msal-browser is loaded as a global on this page or
    //    not — MSAL stores its accounts as JSON in localStorage under keys
    //    shaped `msal.account.keys` plus per-account entries. We prefer the
    //    library when available, then fall back to reading the cache
    //    directly. Same pattern as CygenixSync.getUserId().
    try {
      if (!email && typeof msal !== 'undefined') {
        const msalApp = new msal.PublicClientApplication({
          auth: {
            clientId:         'f3478996-b2b5-4b21-9a23-a6b97a0e5b13',
            authority:        'https://cygenix.ciamlogin.com/',
            knownAuthorities: ['cygenix.ciamlogin.com'],
          },
          cache: { cacheLocation: 'localStorage' },
        });
        const accounts = msalApp.getAllAccounts() || [];
        if (accounts.length) {
          const a = accounts[0];
          email = (a.username || a.idTokenClaims?.email || a.idTokenClaims?.preferred_username || '').trim().toLowerCase();
          name  = a.name || a.idTokenClaims?.name || '';
        }
      }
    } catch {}

    // 0b. MSAL library not loaded — read its localStorage cache directly.
    //     MSAL writes one localStorage entry per account, plus separate
    //     entries per token (id, access, refresh). A single browser can
    //     accumulate multiple account entries across sign-ins (e.g. after
    //     switching identities), so we can't just pick the first one — the
    //     first found may be stale. Strategy:
    //       1. Collect all MSAL account records AND ID tokens (keyed by homeAccountId)
    //       2. For each account, find a matching non-expired token
    //       3. Prefer accounts with a valid token; fall back to any account
    //       4. Decode the chosen account's ID token for the real email
    //          (Google-SSO and other federated logins have a synthetic
    //          @<tenant>.onmicrosoft.com as account.username; the real
    //          email lives in the ID token's `email` claim).
    if (!email) {
      try {
        const nowSec = Math.floor(Date.now() / 1000);
        const accounts = [];
        const tokenExpiries = new Map();  // homeId → latest expiresOn (seconds)
        const idTokens     = new Map();   // homeId → JWT secret string

        for (let i = 0; i < localStorage.length; i++) {
          const k = localStorage.key(i);
          if (!k || !k.includes('-login.windows.net-') && !k.includes('.ciamlogin.com-')) continue;
          const raw = localStorage.getItem(k);
          if (!raw || raw[0] !== '{') continue;
          let obj;
          try { obj = JSON.parse(raw); } catch { continue; }
          if (!obj) continue;

          // An account record
          if (obj.username && obj.authorityType && obj.homeAccountId) {
            accounts.push({ acc: obj, homeId: obj.homeAccountId });
            continue;
          }
          // An ID token record — secret is the JWT we can decode
          if (obj.credentialType === 'IdToken' && obj.homeAccountId && obj.secret) {
            idTokens.set(obj.homeAccountId, obj.secret);
          }
          // A token-expiry record (Access/Refresh tokens)
          if (obj.credentialType && obj.homeAccountId && obj.expiresOn) {
            const exp = parseInt(obj.expiresOn, 10);
            if (isNaN(exp)) continue;
            const prev = tokenExpiries.get(obj.homeAccountId) || 0;
            if (exp > prev) tokenExpiries.set(obj.homeAccountId, exp);
          }
        }

        // Prefer an account whose latest access/refresh token is still valid
        let chosen = accounts.find(a => (tokenExpiries.get(a.homeId) || 0) > nowSec);
        if (!chosen && accounts.length) chosen = accounts[0];

        if (chosen) {
          const a = chosen.acc;

          // Decode the ID token (if we have one for this account) to get
          // the real email claim — necessary for Google SSO etc. where
          // a.username is a synthetic opaque address.
          let claimEmail = a.idTokenClaims?.email || a.idTokenClaims?.preferred_username || '';
          let claimName  = a.idTokenClaims?.name || '';

          if (!claimEmail || !claimName) {
            const jwt = idTokens.get(chosen.homeId);
            if (jwt && jwt.split('.').length === 3) {
              try {
                let b64 = jwt.split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
                while (b64.length % 4) b64 += '=';
                const claims = JSON.parse(atob(b64));
                if (!claimEmail) {
                  claimEmail = claims.email || claims.preferred_username || '';
                  // Reject synthetic addresses — they're a last resort
                  if (claimEmail.endsWith('@cygenix.onmicrosoft.com') && claims.sub && claims.sub.includes('@')) {
                    claimEmail = claims.sub;
                  }
                }
                if (!claimName) claimName = claims.name || '';
              } catch {}
            }
          }

          // Final fallback: account.username (may be synthetic but better than nothing)
          email = (claimEmail || a.username || '').trim().toLowerCase();
          name  = claimName || a.name || '';
        }
      } catch {}
    }

    // 1. Entra External ID session (legacy custom key — kept for safety)
    if (!email) {
      const entraRaw = sessionStorage.getItem('cygenix_entra_account')
                    || localStorage.getItem('cygenix_entra_account');
      if (entraRaw) {
        const entra = JSON.parse(entraRaw);
        email = entra.email || entra.userId || '';
        name  = entra.name  || name;
      }
    }

    // 2. Legacy cygenix_user fallback
    if (!email) {
      const raw  = JSON.parse(sessionStorage.getItem('cygenix_user') || localStorage.getItem('cygenix_user') || '{}');
      const user = raw.user || raw;
      email = user.email || raw.email || '';
      name  = user.user_metadata?.full_name || raw.user_metadata?.full_name || name;
    }

    // 3. Decode from JWT token
    if (!email) {
      const token = sessionStorage.getItem('cygenix_token') || localStorage.getItem('cygenix_token');
      if (token) {
        const payload = JSON.parse(atob(token.split('.')[1]));
        email = payload.email || payload.preferred_username || payload.sub || '';
        name  = payload.name || '';
      }
    }

    if (email) {
      // Use email prefix if name is missing or literally "unknown"
      if (!name || name === 'unknown') name = email.split('@')[0];
      const initials = name.split(' ').map(n=>n[0]).join('').slice(0,2).toUpperCase();
      $('user-name')       && ($('user-name').textContent = name);
      $('user-avatar')     && ($('user-avatar').textContent = initials);
      $('user-email-menu') && ($('user-email-menu').textContent = email);
      console.log('[Dashboard] User:', email, '| Name:', name);
      resolvedEmail = email;
    }
  } catch(e) { console.warn('[Dashboard] initUserAndProject error:', e.message); }

  // If MSAL data wasn't ready yet, schedule a retry. This is the fix
  // for the "user pill stuck on Loading…" symptom — MSAL's
  // localStorage cache is populated asynchronously after the
  // msal-browser script loads, and our first synchronous lookup can
  // miss it. Retry quietly until we find an email or exhaust attempts.
  if (!resolvedEmail && initUserAndProject._attempts < MAX_ATTEMPTS){
    initUserAndProject._attempts++;
    setTimeout(initUserAndProject, RETRY_DELAY_MS);
    // Important: when we retry, we don't want to fall through to the
    // project-name block below because it'll just run again on the
    // retry. Return early. The project-name block executes on the
    // SUCCESSFUL attempt (when resolvedEmail is set).
    return;
  }
  // Reset attempt counter on both success AND exhaustion. Resetting on
  // exhaustion means a later manual call (e.g. after sign-in completes
  // with a delay) starts fresh and gets its own retry chain rather
  // than being permanently blocked.
  initUserAndProject._attempts = 0;

  // ── Active project name in topbar pill ─────────────────────────────────
  // sessionStorage.cygenix_active_project is a write-cache that other parts
  // of the app populate when the user "sets active". It can drift out of
  // sync with the canonical source — `cygenix_projects` array indexed by
  // `cygenix_active_project_id` — when:
  //   • the user renames the active project on /projects.html (the array
  //     gets the new name; sessionStorage may not, if setActive wasn't called)
  //   • the user signs in fresh and gets a migrated project, then renames
  //     it later (sessionStorage was never written by the migration)
  // Reconciliation: prefer the array entry when both exist. Update
  // sessionStorage to match so other readers (report headers, exports)
  // see the canonical name on next read.
  try {
    let proj = null;
    const activeId = localStorage.getItem('cygenix_active_project_id') || '';
    if (activeId) {
      try {
        const arr = JSON.parse(localStorage.getItem('cygenix_projects') || '[]');
        proj = (Array.isArray(arr) ? arr : []).find(p => p.id === activeId) || null;
      } catch {}
    }
    if (!proj) {
      try { proj = JSON.parse(sessionStorage.getItem('cygenix_active_project') || '{}'); } catch {}
    } else {
      // Refresh sessionStorage with the canonical entry so stale values
      // (typically the post-migration "My first project") get replaced.
      try { sessionStorage.setItem('cygenix_active_project', JSON.stringify(proj)); } catch {}
    }
    if (proj && proj.name) {
      $('project-name-badge').textContent = proj.name;
      $('project-name-badge').style.display = 'block';
    }
  } catch {}
}

function toggleUserMenu() {
  const menu = $('user-menu');
  menu.style.display = menu.style.display === 'none' ? 'block' : 'none';
}

// ── User pill enhancements ────────────────────────────────────────────────
// Calls /api/data/whoami once on load to:
//   1. Show the admin link if role === 'admin'
//   2. Show a tier badge (Starter / Pro / Demo / etc.) in the Subscription
//      menu entry so users can see their plan at a glance
//   3. Stash tier metadata on window.__cygenixTier so other parts of the app
//      can read it without a second whoami call
//
// Uses the same Function URL + code as cygenix-cosmos-sync.js. This call is
// best-effort — if it fails, the pill stays in its default state (admin link
// hidden, no tier badge), which is the safe failure mode.
(function enhanceUserPill() {
  const API_BASE  = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/data';
  const FUNC_CODE = 'WjSmoWxgtNdGnO_I5nKIspRUQqKCR1knsXgVmJr3dyYuAzFu-or-5Q==';

  function getUserIdFromMsal() {
    try {
      for (let i = 0; i < localStorage.length; i++) {
        const k = localStorage.key(i);
        if (!k) continue;
        if (!k.includes('-login.windows.net-') && !k.includes('.ciamlogin.com-')) continue;
        const raw = localStorage.getItem(k);
        if (!raw || raw[0] !== '{') continue;
        let obj; try { obj = JSON.parse(raw); } catch { continue; }
        if (obj && obj.username && obj.authorityType && obj.homeAccountId) {
          return String(obj.username || '').trim().toLowerCase();
        }
      }
    } catch {}
    return null;
  }

  function setTierBadge(label, isHighlight) {
    const el = document.getElementById('user-menu-tier-badge');
    if (!el) return;
    el.textContent = label;
    if (isHighlight) {
      el.style.background = 'var(--accent-glow,rgba(74,91,214,0.15))';
      el.style.borderColor = 'rgba(74,91,214,0.4)';
      el.style.color = 'var(--accent,#4A5BD6)';
    }
  }

  // Wait briefly for MSAL — same retry pattern as elsewhere in the codebase.
  let attempts = 0;
  const maxAttempts = 8;
  const interval = setInterval(async () => {
    attempts++;
    const userId = getUserIdFromMsal();
    if (!userId && attempts < maxAttempts) return;
    clearInterval(interval);
    if (!userId) return;

    try {
      const r = await fetch(`${API_BASE}/whoami?code=${FUNC_CODE}`, {
        headers: { 'x-user-id': userId }
      });
      if (!r.ok) return;
      const data = await r.json();

      // Stash for other readers
      window.__cygenixTier = {
        tier:                data.tier || null,
        tier_status:         data.tier_status || 'none',
        role:                data.role || null,
        billing_period:      data.billing_period || null,
        current_period_end:  data.current_period_end || null,
        trial_ends_at:       data.trial_ends_at || null,
        cancel_at_period_end: !!data.cancel_at_period_end
      };

      // 1. Reveal admin link only for admins
      if (data.role === 'admin') {
        const adm = document.getElementById('user-menu-admin-link');
        if (adm) adm.style.display = 'block';
      }

      // 2. Tier badge in the Subscription row
      if (data.role === 'admin') {
        setTierBadge('Admin', true);
      } else if (data.role === 'demo') {
        setTierBadge('Demo', true);
      } else if (data.tier) {
        const label = data.tier_status === 'trialing'
          ? `${data.tier} · trial`
          : data.tier;
        setTierBadge(label.charAt(0).toUpperCase() + label.slice(1), data.tier_status === 'trialing' || data.tier_status === 'active');
      } else {
        setTierBadge('No plan', false);
      }
    } catch (e) {
      console.warn('[user-pill] whoami failed:', e);
    }
  }, 500);
})();

// ── Open Stripe-hosted billing portal ─────────────────────────────────────
// Called from the "Subscription" entry in the user pill. Hits
// /api/data/billing-portal which returns a short-lived Stripe portal URL,
// then redirects there. Falls back to /pick-plan.html for users who don't
// have a subscription yet (no Stripe customer to manage).
async function openBillingPortal(linkEl) {
  // Admin/demo users have no real subscription — there's nothing for them
  // to manage in the Stripe portal. Send them to pricing/admin instead so
  // they're not confused by an error.
  const ctx = window.__cygenixTier || {};
  if (ctx.role === 'admin' || ctx.role === 'demo') {
    window.location.href = '/pricing';
    return;
  }
  if (!ctx.tier) {
    window.location.href = '/pick-plan.html';
    return;
  }

  // Visual feedback while we wait for the portal URL
  const original = linkEl ? linkEl.querySelector('span').textContent : null;
  if (linkEl) linkEl.querySelector('span').textContent = 'Opening…';

  try {
    const API_BASE  = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/data';
    const FUNC_CODE = 'WjSmoWxgtNdGnO_I5nKIspRUQqKCR1knsXgVmJr3dyYuAzFu-or-5Q==';

    const userId = (function() {
      for (let i = 0; i < localStorage.length; i++) {
        const k = localStorage.key(i);
        if (!k) continue;
        if (!k.includes('-login.windows.net-') && !k.includes('.ciamlogin.com-')) continue;
        const raw = localStorage.getItem(k);
        if (!raw || raw[0] !== '{') continue;
        let obj; try { obj = JSON.parse(raw); } catch { continue; }
        if (obj && obj.username && obj.authorityType && obj.homeAccountId) {
          return String(obj.username || '').trim().toLowerCase();
        }
      }
      return null;
    })();
    if (!userId) {
      alert('Not signed in. Please refresh and try again.');
      if (linkEl && original) linkEl.querySelector('span').textContent = original;
      return;
    }

    const r = await fetch(`${API_BASE}/billing-portal?code=${FUNC_CODE}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-user-id': userId },
      body: JSON.stringify({
        return_url: window.location.href
      })
    });
    const data = await r.json().catch(() => null);

    if (r.ok && data?.url) {
      window.location.href = data.url;
      return;
    }

    alert('Could not open billing portal: ' + (data?.error || `HTTP ${r.status}`));
    if (linkEl && original) linkEl.querySelector('span').textContent = original;
  } catch (e) {
    alert('Network error opening billing portal: ' + (e.message || 'unknown'));
    if (linkEl && original) linkEl.querySelector('span').textContent = original;
  }
}

// Close user menu when clicking outside
document.addEventListener('click', function(e) {
  const pill = $('user-pill');
  if (pill && !pill.contains(e.target)) {
    const menu = $('user-menu');
    if (menu) menu.style.display = 'none';
  }
});

async function signOut() {
  // Flag so login.html doesn't auto-resign-in via acquireTokenSilent
  sessionStorage.setItem('cygenix_just_signed_out', '1');

  // Clear our own app keys
  ['cygenix_token','cygenix_user','cygenix_expires',
   'cygenix_active_project','cygenix_entra_account',
   'cygenix_active_user'].forEach(k => {
    sessionStorage.removeItem(k);
    localStorage.removeItem(k);
  });

  // Use MSAL's own logout so it clears its full cache (not just msal.* keys)
  try {
    const msalInstance = new msal.PublicClientApplication({
      auth: {
        clientId:    'f3478996-b2b5-4b21-9a23-a6b97a0e5b13',
        authority:   'https://cygenix.ciamlogin.com/',
        knownAuthorities: ['cygenix.ciamlogin.com'],
        redirectUri: window.location.origin + '/login.html',
      },
      cache: { cacheLocation: 'localStorage' },
    });
    await msalInstance.initialize();

    const account = msalInstance.getAllAccounts()[0];
    await msalInstance.logoutRedirect({
      account,
      postLogoutRedirectUri: window.location.origin + '/login.html',
    });
  } catch (e) {
    console.warn('[signOut] MSAL logout failed, falling back:', e);
    // Hard fallback — nuke anything MSAL-shaped and redirect to Entra logout
    const CLIENT_ID = 'f3478996-b2b5-4b21-9a23-a6b97a0e5b13';
    Object.keys(localStorage).forEach(k => {
      if (k.includes('msal') || k.includes(CLIENT_ID) || k.startsWith('{')) {
        localStorage.removeItem(k);
      }
    });
    window.location.href = 'https://cygenix.ciamlogin.com/fc8dfc7a-645f-4a5c-8f59-6762f97c803f/oauth2/v2.0/logout'
      + '?post_logout_redirect_uri=' + encodeURIComponent(window.location.origin + '/login.html');
  }
}


// ── Conversion Report ──────────────────────────────────────────────────────────
function openReport(jobId) {
  const job = state.jobs.find(j => j.id === jobId);
  if (!job) { alert('Job not found. Please run a migration first.'); return; }

  // User info
  let userName = 'Migration Engineer', userEmail = '';
  try {
    const user = JSON.parse(sessionStorage.getItem('cygenix_user') || '{}');
    userName  = user.user_metadata?.full_name || user.email?.split('@')[0] || 'Migration Engineer';
    userEmail = user.email || '';
  } catch {}

  // Project name
  let projectName = job.name;
  try {
    const proj = JSON.parse(sessionStorage.getItem('cygenix_active_project') || '{}');
    if (proj.name) projectName = proj.name;
  } catch {}

  const totalRows = job.totalRows || job.tables?.reduce((s,t) => s+(t.rows||0), 0) || 0;

  // Build per-table results
  const tableResults = (job.tables || []).map(t => ({
    name:         t.name || t.tableName || 'Unknown',
    sourceRows:   t.rows   || t.sourceRows   || 0,
    insertedRows: t.rows   || t.insertedRows || 0,
    errors:       t.errors || 0,
    batches:      Math.ceil((t.rows || 0) / 100),
    status:       (t.errors || 0) === 0 ? 'success' : 'partial'
  }));

  if (!tableResults.length) {
    tableResults.push({
      name: job.target || 'migration',
      sourceRows: totalRows,
      insertedRows: totalRows,
      errors: 0,
      batches: Math.ceil(totalRows / 100),
      status: 'success'
    });
  }
  // Ensure all table results have the correct target table name
  tableResults.forEach(t => {
    if (!t.name || t.name === 'Unknown') t.name = job.target || 'migration';
  });

  // Build column mapping from schemaResult stored on job (if available)
  const columnMapping = (job.columnMapping || []).map(m => ({
    srcCol:    m.srcCol    || m.sourceCol || '',
    tgtCol:    m.tgtCol    || m.targetCol || '',
    tgtType:   m.tgtType   || m.type      || '',
    transform: m.transform || 'NONE',
  }));

  // Source files info
  const sourceFiles = job.files?.map(f => f.name) || [];
  const sourceSize  = job.files?.reduce((s,f) => s+(f.size||0), 0) || 0;

  // Source table name: job.source is set by saveJobAndFinish to the schema.table name
  const srcTableName = job.sourceTable || job.source || '';
  const tgtTableName = job.target || tableResults[0]?.name || '—';

  const reportData = {
    id:             job.id,
    projectId:      job.projectId || '',   // used by report.html to scope AI artifacts to this project's inventory folder
    projectName,
    userName,
    userEmail,
    organisation:   'Cygenix',
    sourceTable:    srcTableName,
    sourceSystem:   srcTableName || 'Source Database',
    targetSystem:   'Microsoft SQL Server',
    sourceFile:     srcTableName,
    sourceFiles:    srcTableName ? [srcTableName] : [],
    sourceFileSize: 0,
    targetServer:   job.targetServer || job.target || '—',
    targetDatabase: job.targetDatabase || '—',
    targetTable:    tgtTableName,
    authMethod:     job.authMethod || 'SQL Authentication',
    totalRows,
    insertedRows:   totalRows,
    columnsMapped:  (job.columnMapping || []).filter(m => m.tgtCol).length || (job.tables?.[0]?.cols || 0),
    errors:         tableResults.reduce((s,t) => s+(t.errors||0), 0),
    rowsBefore:     0,
    rowsAfter:      totalRows,
    tables:         tableResults.map(t => ({ ...t, sourceName: srcTableName })),
    columnMapping:  (job.columnMapping || []).map(m => ({
      srcCol: m.srcCol || '', tgtCol: m.tgtCol || '',
      // Table names and the fixed value were being dropped here, so the
      // report could not show WHERE a target column's value came from — a
      // fixed value or @@parameter looked identical to an unmapped column.
      srcTable: m.srcTable || '', tgtTable: m.tgtTable || '',
      literalValue: (m.literalValue !== undefined && m.literalValue !== null) ? m.literalValue : '',
      tgtType: m.tgtType || '', transform: m.transform || 'NONE',
      transformExpr: m.transformExpr || null,
      wasisRules: m.wasisRules || null, wasisCount: m.wasisCount || null
    })),
    wasisRules:     job.wasisRules || [],
    wasisRuleCount: job.wasisRuleCount || 0,
    wasisColCount:  job.wasisColCount || 0,
    warnings:       job.warnings || [],
    startedAt:      job.created,
    completedAt:    job.created,
  };

  sessionStorage.setItem('cygenix_report', JSON.stringify(reportData)); localStorage.setItem('cygenix_report', JSON.stringify(reportData));
  window.open('/report.html', '_blank');
}


// ── Conversion Reports list (Cosmos-backed) ─────────────────────────────────
//
// Previously this rendered per-job entries from localStorage (state.jobs —
// capped at 20, one per configured table). It now reads per-run reports saved
// by the user to Cosmos via /.netlify/functions/reports (action: 'list').
//
// Each row represents one full Projects → Execute run that the user chose to
// save. Max 10 per user (server-side cap — oldest is silently pruned when an
// 11th is saved). Rows show summary stats + buttons to view the full report
// or delete the entry.
//
// Signed-out / empty-account case: we render a clear message rather than
// trying to fetch. The reports endpoint would 400 without a userEmail.

function currentCygenixEmail() {
  try {
    const rawAcc = sessionStorage.getItem('cygenix_entra_account')
                || localStorage.getItem('cygenix_entra_account');
    if (!rawAcc) return '';
    const acc = JSON.parse(rawAcc);
    return ((acc.email || acc.userId) || '').toLowerCase().trim();
  } catch { return ''; }
}

function clearAllJobs() {
  if (!confirm('Clear all saved jobs from this browser? This cannot be undone.')) return;
  state.jobs = [];
  localStorage.removeItem('cygenix_jobs');
  updateStats();
  renderReportsList();
  renderDashboard();
}

async function renderReportsList() {
  const el = $('reports-list');
  if (!el) return;

  const email = currentCygenixEmail();
  if (!email) {
    el.innerHTML = '<div class="empty-state"><h3>Please sign in</h3><p>Conversion Reports are saved per user. Sign in to see yours.</p></div>';
    return;
  }

  el.innerHTML = '<div class="empty-state" style="padding:2rem"><p style="color:var(--text3)">Loading reports…</p></div>';

  let reports = [];
  try {
    reports = await fetchReportsList();
  } catch (e) {
    // Tailor the troubleshooting hint based on what actually went wrong.
    // Auth errors (401) come from the Netlify function rejecting a missing
    // or invalid Authorization header — pointing the user at Cosmos env
    // vars in that case is misleading. Server-side Cosmos / 5xx errors
    // are the only case where the env-vars check is a useful next step.
    const msg  = String(e && e.message || e || '');
    const isAuth = /unauthor|missing authorization|bearer|token/i.test(msg);
    const hint = isAuth
      ? '<p style="font-size:12px;color:var(--text3);margin-top:0.5rem">'
        + 'This is an authentication error, not a database error. '
        + 'Try signing out and back in to refresh your session token. '
        + 'If it persists, append <code>?authdebug=1</code> to the URL and reload — '
        + 'the browser console will then show why the token could not be attached. '
        + '(Recent change: the reports endpoint now requires a valid Entra ID token in the Authorization header.)'
        + '</p>'
      : '<p style="font-size:12px;color:var(--text3);margin-top:0.5rem">'
        + 'If this is the first deploy, check that COSMOS_ENDPOINT and COSMOS_KEY are set in Netlify environment variables, '
        + 'and that the <code>project_reports</code> container exists in the <code>cygenix</code> database.'
        + '</p>';
    el.innerHTML = '<div class="empty-state"><h3>Could not load reports</h3>'
                 + '<p style="color:var(--red)">' + (msg || 'Unknown error') + '</p>'
                 + hint
                 + '</div>';
    return;
  }

  if (!reports.length) {
    el.innerHTML = '<div class="empty-state"><h3>No saved reports yet</h3><p>After a migration finishes in Projects → Execute, click <strong>Save Report</strong> to archive it here.</p></div>';
    return;
  }

  el.innerHTML = reports.map(r => {
    const name  = escapeHtml(r.projectName || 'Untitled run');
    const date  = r.savedAt ? new Date(r.savedAt).toLocaleDateString('en-GB', { day:'numeric', month:'short', year:'numeric', hour:'2-digit', minute:'2-digit' }) : '—';
    const rows  = Number(r.totalRows || r.insertedRows || 0).toLocaleString();
    const src   = escapeHtml(r.sourceSystem || '—');
    const tgt   = escapeHtml(r.targetSystem || '—');
    const errs  = Number(r.errors || 0);
    const tables = Number(r.tableCount || 0);
    // Analyst: prefer userName, but if it's blank / 'unknown' / whitespace, fall back to email.
    // Google-SSO paths currently write "unknown" as the name on the account record — this is
    // a display fix, not a data fix. Proper name extraction from the JWT's given/family claims
    // is a separate task, tracked in the security backlog.
    const userNameRaw = String(r.userName || '').trim();
    const isUnnamed = !userNameRaw || /^unknown$/i.test(userNameRaw);
    const analyst = escapeHtml(isUnnamed ? (r.userEmail || '—') : userNameRaw);
    const errBadge = errs > 0
      ? '<span class="badge badge-red" style="font-size:11px">' + errs + ' error' + (errs === 1 ? '' : 's') + '</span>'
      : '<span class="badge badge-green" style="font-size:11px">Clean</span>';
    const tblText = tables > 0 ? (tables + ' table' + (tables === 1 ? '' : 's')) : '—';
    return '<div style="background:var(--bg2);border:0.5px solid var(--border);border-radius:var(--r-lg);padding:1.25rem;margin-bottom:1rem;display:flex;align-items:center;justify-content:space-between;gap:1rem;flex-wrap:wrap">'
      + '<div>'
      +   '<div style="font-size:14px;font-weight:600;margin-bottom:0.3rem">' + name + '</div>'
      +   '<div style="font-size:12px;color:var(--text3);font-family:var(--mono)">' + date + ' &nbsp;·&nbsp; ' + tblText + ' &nbsp;·&nbsp; ' + rows + ' rows</div>'
      +   '<div style="font-size:11px;color:var(--text3);margin-top:3px">Source: ' + src + ' &nbsp;→&nbsp; Target: ' + tgt + '</div>'
      +   '<div style="font-size:11px;color:var(--text3);margin-top:2px">Analyst: ' + analyst + '</div>'
      + '</div>'
      + '<div style="display:flex;gap:0.5rem;align-items:center;flex-shrink:0">'
      +   errBadge
      +   '<button class="btn btn-sm" style="background:transparent;color:var(--text2);border:0.5px solid var(--border2);font-size:12px;padding:0.45rem 0.9rem" onclick="openReportSettings(\'' + r.id + '\')" title="Reconciliation settings for this report\'s jobs"><i class="ic ic-cog"></i> Settings</button>'
      +   '<button class="btn btn-sm" style="background:var(--purple-bg);color:var(--purple);border:0.5px solid rgba(107,78,142,0.25);font-size:12px;padding:0.45rem 1.1rem" onclick="openSavedReport(\'' + r.id + '\')">View</button>'
      +   '<button class="btn btn-sm" style="background:transparent;color:var(--red);border:0.5px solid rgba(239,68,68,0.25);font-size:12px;padding:0.45rem 0.9rem" onclick="deleteSavedReport(\'' + r.id + '\', \'' + name.replace(/'/g,"\\'") + '\')" title="Delete this report">✕</button>'
      + '</div>'
      + '</div>';
  }).join('');
}

// View a saved report: fetch its full body, stash in sessionStorage, open report.html.
async function openSavedReport(id) {
  const email = currentCygenixEmail();
  if (!email) { alert('Please sign in first.'); return; }
  try {
    const r = await cygenixFetch('/.netlify/functions/reports', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'get', id }),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || ('HTTP ' + r.status));
    if (!data.report) throw new Error('Report not returned');

    // Backfill projectId if missing — older reports were saved before the
    // projectId-on-report patch landed. Self-heal by looking up the owning
    // job in local storage; if that doesn't resolve, fall back to matching
    // the saved report's projectName against cygenix_projects. Either path
    // succeeding lets report.html scope artifacts correctly without forcing
    // the user to re-run the migration.
    if (!data.report.projectId) {
      let resolvedProjectId = '';
      // 1. Try job lookup — the report's id usually matches a job id
      try {
        const jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]');
        const owningJob = jobs.find(j => j.id === data.report.id);
        if (owningJob && owningJob.projectId) resolvedProjectId = owningJob.projectId;
      } catch {}
      // 2. Fall back to matching projectName against the projects table
      if (!resolvedProjectId && data.report.projectName) {
        try {
          const projects = JSON.parse(localStorage.getItem('cygenix_projects') || '[]');
          const targetName = String(data.report.projectName).trim().toLowerCase();
          const matchedProject = projects.find(p =>
            String(p.name || '').trim().toLowerCase() === targetName);
          if (matchedProject) resolvedProjectId = matchedProject.id;
        } catch {}
      }
      if (resolvedProjectId) {
        data.report.projectId = resolvedProjectId;
        // Persist backfill to Cosmos so this self-heal is permanent — fire
        // and forget; failure here doesn't block the user from viewing.
        cygenixFetch('/.netlify/functions/reports', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            action: 'save-presentation', // reuse existing patch endpoint to update fields
            id: data.report.id,
            projectId: resolvedProjectId,
            // Send any existing presentationConfig so we don't accidentally
            // wipe it (the endpoint upserts the whole field).
            presentationConfig: data.report.presentationConfig || null,
          }),
        }).catch(e => console.warn('[Cygenix] projectId backfill to Cosmos failed:', e.message));
      }
    }

    // Same storage keys report.html reads
    sessionStorage.setItem('cygenix_report', JSON.stringify(data.report));
    try { localStorage.setItem('cygenix_report', JSON.stringify(data.report)); } catch {}
    window.open('/report.html', '_blank');
  } catch (e) {
    alert('Could not open report: ' + e.message);
  }
}

// Open the per-report reconciliation settings modal. Fetches the report,
// pulls the jobIds out of its steps[], then renders the existing recon card
// UI filtered to those jobs only. If the job records aren't in local
// storage (e.g. the report was created on a different device), we show a
// notice and a lookup-by-srcTable fallback so users on shared devices still
// get something useful. Saves go through reconSaveOne() and update the
// cygenix_jobs record like the old global Settings page did.
async function openReportSettings(reportId) {
  const modal  = document.getElementById('report-settings-modal');
  const list   = document.getElementById('report-recon-list');
  const notice = document.getElementById('report-settings-notice');
  const title  = document.getElementById('report-settings-title');
  const sub    = document.getElementById('report-settings-sub');
  if (!modal || !list) return;
  modal.classList.add('open');
  list.innerHTML = '<div class="empty-state" style="padding:1.5rem"><p style="color:var(--text3)">Loading report…</p></div>';
  if (notice) { notice.style.display = 'none'; notice.textContent = ''; }

  const email = currentCygenixEmail();
  if (!email) {
    list.innerHTML = '<div class="empty-state"><h3>Please sign in</h3><p>Sign in to view report settings.</p></div>';
    return;
  }

  let report;
  try {
    const r = await cygenixFetch('/.netlify/functions/reports', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'get', id: reportId }),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || ('HTTP ' + r.status));
    if (!data.report) throw new Error('Report not returned');
    report = data.report;
  } catch (e) {
    list.innerHTML = '<div class="empty-state"><h3>Could not load report</h3><p style="color:var(--red)">' + escapeHtml(e.message || String(e)) + '</p></div>';
    return;
  }

  // Update modal header with this report's name so the user knows what scope
  // they are editing.
  if (title) title.textContent = 'Reconciliation settings · ' + (report.projectName || 'Report');
  if (sub) sub.textContent = 'Reconciliation for the migration jobs that were part of this report. Changes save back to the job record and apply on the next run.';

  // Collect every jobId referenced by migration steps in this report. Older
  // reports (saved before jobId was captured) fall back to matching on
  // srcTable/tgtTable for a best-effort lookup.
  const steps = Array.isArray(report.steps) ? report.steps.filter(s => s && s.type === 'migration') : [];
  const jobIds = [...new Set(steps.map(s => s.jobId).filter(Boolean))];
  const tablePairs = steps
    .filter(s => !s.jobId && (s.srcTable || s.tgtTable))
    .map(s => (s.srcTable || '') + '→' + (s.tgtTable || ''));

  const allJobs = reconLoadJobs();
  let matched = allJobs.filter(j => jobIds.includes(j.id));
  if (matched.length < jobIds.length + tablePairs.length) {
    // Supplement id-matched jobs with srcTable/tgtTable matches for steps
    // missing a jobId, deduping by job id.
    const have = new Set(matched.map(j => j.id));
    tablePairs.forEach(tp => {
      const [src, tgt] = tp.split('→');
      const j = allJobs.find(j =>
        !have.has(j.id) &&
        (j.sourceTable || j.source) === src &&
        (j.targetTable || j.target) === tgt &&
        Array.isArray(j.columnMapping) && j.columnMapping.length > 0
      );
      if (j) { matched.push(j); have.add(j.id); }
    });
  }

  // Any steps we couldn't match up to a local job? Tell the user rather than
  // showing a misleadingly short list.
  const unmatched = steps.filter(s => {
    if (s.jobId && !matched.find(j => j.id === s.jobId)) return true;
    if (!s.jobId && !matched.find(j =>
      (j.sourceTable || j.source) === s.srcTable &&
      (j.targetTable || j.target) === s.tgtTable
    )) return true;
    return false;
  });
  if (unmatched.length && notice) {
    notice.style.display = 'block';
    notice.textContent = unmatched.length + ' migration step(s) in this report couldn\'t be matched to a job on this device — their reconciliation settings aren\'t editable here. Open this page on the browser where the jobs were created, or recreate them to configure reconciliation.';
  }

  if (!matched.length) {
    list.innerHTML = '<div class="empty-state"><h3>No editable jobs</h3><p>None of the migration jobs in this report are present in this browser\'s storage. Open this page on the browser where the jobs were created to edit their reconciliation settings.</p></div>';
    return;
  }

  // Reuse the exact same card UI the old global Settings view used — same
  // form fields, same save handler (reconSaveOne). This keeps behaviour
  // identical; all we've changed is the scope (which jobs appear).
  list.innerHTML = matched.map(job => reconRenderJobCard(job)).join('');
  reconBindChipsFields(list);

  // Mount the presentation panel with this report's data + saved config.
  // Lives beneath the recon cards in the same modal.
  if (typeof presMountGrids === 'function') {
    try { presMountGrids(report); } catch (e) { console.warn('[pres] mount failed', e); }
  }
}

function closeReportSettings() {
  // Guard against losing unsaved presentation changes
  if (typeof presIsDirty === 'function' && presIsDirty()) {
    if (!confirm('You have unsaved presentation changes. Discard them?')) return;
  }
  const modal = document.getElementById('report-settings-modal');
  if (modal) modal.classList.remove('open');
  // Reset the presentation panel state so the next open is clean
  const panel = document.getElementById('pres-panel');
  if (panel) { panel.style.display = 'none'; panel.setAttribute('data-open','false'); panel.setAttribute('data-dirty','false'); }
}

// ═══════════════════════════════════════════════════════════════════════════
// GLOBAL SEARCH
// ═══════════════════════════════════════════════════════════════════════════
// Client-side search across the four data sources surfaced in this app:
//   1. Jobs / Maps        — localStorage 'cygenix_jobs'
//   2. Projects           — localStorage 'cygenix_projects'
//   3. Inventory docs     — localStorage 'cygenix_inventory'
//   4. Conversion reports — Cosmos via /.netlify/functions/reports
//
// Reports are fetched once per view-open and cached on the window to keep
// typing fast (no per-keystroke network hits). Matches are case-insensitive
// substring; each match also remembers WHICH field matched so the user can
// see "name contains X" vs "SQL contains X" etc. Each row has a Go-to
// action that routes to the right editor/view for the item's type.
// ═══════════════════════════════════════════════════════════════════════════

let _searchReportCache = null;        // Cached reports list (null = not yet loaded)
let _searchReportLoading = false;     // Prevents concurrent fetches during fast re-open

async function initGlobalSearch() {
  // Reset any stale summary from a prior open, render the empty-state
  // prompt, focus the input, and kick off an async fetch of the user's
  // saved reports (cached for this session).
  const input = document.getElementById('search-input');
  const res   = document.getElementById('search-results');
  const sum   = document.getElementById('search-summary');
  if (input) { setTimeout(() => input.focus(), 50); }
  if (res) res.innerHTML = '<div class="empty-state" style="padding:2rem"><p style="color:var(--text3)">Start typing above to search across jobs, projects, artifacts, and saved reports.</p></div>';
  if (sum) sum.textContent = '';
  // Fire-and-forget report prefetch — we don't await so the UI stays responsive.
  // If it fails, search still works for the local sources; we just won't show
  // remote reports until it recovers on the next view-open.
  _prefetchReportsForSearch();
}

async function _prefetchReportsForSearch() {
  if (_searchReportCache !== null || _searchReportLoading) return;
  const email = currentCygenixEmail();
  if (!email) return;
  _searchReportLoading = true;
  try {
    const data = await fetchReportsList().catch(() => null);
    if (Array.isArray(data)) {
      _searchReportCache = data;
    } else {
      _searchReportCache = []; // empty marker so we don't retry on every keystroke
    }
  } catch {
    _searchReportCache = [];
  } finally {
    _searchReportLoading = false;
    // If the user has already typed something while we were fetching, re-run
    // the search so reports now appear in the results.
    const input = document.getElementById('search-input');
    if (input && input.value.trim()) runGlobalSearch();
  }
}

// Search re-ran in full on every keystroke; 150ms of quiet first.

var _globalSearchTimer;

function debouncedGlobalSearch(){

  if (_globalSearchTimer) clearTimeout(_globalSearchTimer);

  _globalSearchTimer = setTimeout(runGlobalSearch, 150);

}


function runGlobalSearch() {
  const input = document.getElementById('search-input');
  const scope = document.getElementById('search-scope');
  const res   = document.getElementById('search-results');
  const sum   = document.getElementById('search-summary');
  if (!input || !res) return;
  const qRaw  = input.value || '';
  const q     = qRaw.trim().toLowerCase();
  const scopeVal = scope ? scope.value : 'all';

  if (!q) {
    res.innerHTML = '<div class="empty-state" style="padding:2rem"><p style="color:var(--text3)">Start typing above to search across jobs, projects, artifacts, and saved reports.</p></div>';
    if (sum) sum.textContent = '';
    return;
  }

  const rows = [];

  // ── Jobs / Maps ──────────────────────────────────────────────────────────
  // Matches name, source/target tables, column names (src + tgt), and the
  // generated insertSQL body so users can find a job by any recognisable
  // fragment — table, column, or ad-hoc SQL comment.
  if (scopeVal === 'all' || scopeVal === 'job') {
    const jobs = safeArr('cygenix_jobs');
    jobs.forEach(j => {
      const matches = [];
      if (_matchField(j.name, q))         matches.push({ field: 'name',   text: j.name });
      if (_matchField(j.sourceTable, q))  matches.push({ field: 'source', text: j.sourceTable });
      if (_matchField(j.source, q) && j.source !== j.sourceTable) matches.push({ field: 'source', text: j.source });
      if (_matchField(j.targetTable, q))  matches.push({ field: 'target', text: j.targetTable });
      if (_matchField(j.target, q) && j.target !== j.targetTable) matches.push({ field: 'target', text: j.target });
      // Column names: any srcCol/tgtCol in columnMapping
      if (Array.isArray(j.columnMapping)) {
        const colHit = j.columnMapping.find(m =>
          (m.srcCol && String(m.srcCol).toLowerCase().includes(q)) ||
          (m.tgtCol && String(m.tgtCol).toLowerCase().includes(q))
        );
        if (colHit) {
          const col = colHit.srcCol && String(colHit.srcCol).toLowerCase().includes(q) ? colHit.srcCol : colHit.tgtCol;
          matches.push({ field: 'column', text: col });
        }
      }
      // SQL body — snippet around the match so users see context
      if (_matchField(j.insertSQL, q)) matches.push({ field: 'SQL', text: _snippet(j.insertSQL, q) });
      else if (_matchField(j.migrationSQL, q)) matches.push({ field: 'SQL', text: _snippet(j.migrationSQL, q) });
      else if (_matchField(j.sql, q)) matches.push({ field: 'SQL', text: _snippet(j.sql, q) });
      if (!matches.length) return;

      const kind = j.jobType === 'sql'          ? 'SQL job'
                 : j.jobType === 'one-to-many'  ? 'One-to-many map'
                 : j.jobType === 'simple-map'   ? 'Object map'
                 : 'Job';
      const sub = [j.sourceTable || j.source, j.targetTable || j.target]
        .filter(Boolean).join(' → ') || '';
      rows.push({
        type: 'Job',
        typeColor: 'var(--accent)',
        kind,
        name: j.name || '(unnamed job)',
        subtitle: sub,
        matches,
        gotoLabel: 'Edit',
        gotoAction: "editJob('" + _jsEsc(j.id) + "')",
      });
    });
  }

  // ── Projects ─────────────────────────────────────────────────────────────
  // Project records are loose in shape — most carry a name and a set of
  // job refs. We search the name only (deep-search of nested job refs
  // would double-count results since those jobs already show up above).
  if (scopeVal === 'all' || scopeVal === 'project') {
    const projects = safeArr('cygenix_projects');
    projects.forEach(p => {
      const matches = [];
      if (_matchField(p.name, q)) matches.push({ field: 'name', text: p.name });
      if (!matches.length) return;
      const groupCount = Array.isArray(p.groups) ? p.groups.length : 0;
      const stepCount  = Array.isArray(p.groups)
        ? p.groups.reduce((n,g) => n + (Array.isArray(g.steps) ? g.steps.length : 0), 0)
        : (Array.isArray(p.steps) ? p.steps.length : 0);
      const sub = stepCount
        ? (stepCount + ' step' + (stepCount===1?'':'s') + (groupCount>1 ? ' · ' + groupCount + ' groups' : ''))
        : '';
      rows.push({
        type: 'Project',
        typeColor: 'var(--green)',
        kind: 'Project',
        name: p.name || '(unnamed project)',
        subtitle: sub,
        matches,
        gotoLabel: 'Open',
        gotoAction: "_openProjectFromSearch('" + _jsEsc(p.id) + "')",
      });
    });
  }

  // ── Inventory ────────────────────────────────────────────────────────────
  // Matches filename, type label, and note content (for text notes). The
  // gotoAction switches to the Inventory view — we don't have a per-item
  // deep link so this is best-effort navigation.
  if (scopeVal === 'all' || scopeVal === 'inventory') {
    let inv = [];
    try { inv = JSON.parse(localStorage.getItem('cygenix_inventory') || '[]'); } catch {}
    const typeLabel = t => (DOC_TYPES && DOC_TYPES[t] && DOC_TYPES[t].label) || t || 'Doc';
    inv.forEach(d => {
      const matches = [];
      if (_matchField(d.name, q))     matches.push({ field: 'filename', text: d.name });
      if (_matchField(typeLabel(d.type), q)) matches.push({ field: 'type', text: typeLabel(d.type) });
      if (d.isNote && _matchField(d.content, q)) matches.push({ field: 'note', text: _snippet(d.content, q) });
      if (!matches.length) return;
      rows.push({
        type: 'Artifact',
        typeColor: 'var(--amber)',
        kind: typeLabel(d.type),
        name: d.name || '(unnamed document)',
        subtitle: (d.ext ? d.ext.toUpperCase() : '') +
                  (d.addedAt ? ' · ' + new Date(d.addedAt).toLocaleDateString('en-GB', {day:'numeric',month:'short',year:'numeric'}) : ''),
        matches,
        gotoLabel: 'Open',
        gotoAction: "_openInventoryFromSearch('" + _jsEsc(d.id) + "')",
      });
    });
  }

  // ── Conversion reports ──────────────────────────────────────────────────
  // Uses the cached list from _prefetchReportsForSearch. If the cache is
  // still loading we render the current results and the user will see
  // reports appear on the next keystroke (prefetch re-runs runGlobalSearch
  // when it finishes).
  if ((scopeVal === 'all' || scopeVal === 'report') && Array.isArray(_searchReportCache)) {
    _searchReportCache.forEach(r => {
      const matches = [];
      if (_matchField(r.projectName, q))  matches.push({ field: 'name',   text: r.projectName });
      if (_matchField(r.sourceSystem, q)) matches.push({ field: 'source', text: r.sourceSystem });
      if (_matchField(r.targetSystem, q)) matches.push({ field: 'target', text: r.targetSystem });
      if (_matchField(r.userName, q))     matches.push({ field: 'analyst', text: r.userName });
      if (_matchField(r.userEmail, q))    matches.push({ field: 'analyst', text: r.userEmail });
      if (!matches.length) return;
      const date = r.savedAt ? new Date(r.savedAt).toLocaleDateString('en-GB', {day:'numeric',month:'short',year:'numeric'}) : '';
      rows.push({
        type: 'Report',
        typeColor: 'var(--purple)',
        kind: 'Saved report',
        name: r.projectName || '(untitled report)',
        subtitle: date + (r.totalRows ? ' · ' + Number(r.totalRows).toLocaleString() + ' rows' : ''),
        matches,
        gotoLabel: 'View',
        gotoAction: "openSavedReport('" + _jsEsc(r.id) + "')",
      });
    });
  }

  // Summary line
  if (sum) {
    const bySource = rows.reduce((acc, r) => { acc[r.type] = (acc[r.type]||0) + 1; return acc; }, {});
    const parts = Object.keys(bySource).map(k => bySource[k] + ' ' + k.toLowerCase() + (bySource[k]===1?'':'s'));
    sum.textContent = rows.length
      ? rows.length + ' result' + (rows.length===1?'':'s') + (parts.length ? ' · ' + parts.join(', ') : '')
      + (_searchReportCache === null && (scopeVal==='all'||scopeVal==='report') ? ' · (reports still loading…)' : '')
      : 'No results';
  }

  if (!rows.length) {
    // Say what IS searched. The old placeholder promised "name, table,
    // column, SQL, filename", which reads as though it searches the connected
    // database — it doesn't. Table and column names are only found where they
    // appear in a saved job or map, so searching for a table that no job
    // references (e.g. one just created by the Data Generator) correctly
    // returns nothing, and the user had no way to know why.
    res.innerHTML = '<div class="empty-state" style="padding:2rem">' +
      '<h3>No matches</h3>' +
      '<p>Nothing matched "' + escapeHtml(qRaw) + '".</p>' +
      '<p style="color:var(--text3);font-size:12px;margin-top:0.75rem;line-height:1.7">' +
      'Search looks through your saved work — job and map names, their source/target tables, mapped column names, generated SQL, project names, artifact filenames and saved reports.<br>' +
      'It does <b>not</b> query the connected databases, so a table that no job or map refers to yet won\'t appear here. ' +
      'To browse live schema, use <b>Object Mapping</b> or the <b>SQL Editor</b>.' +
      '</p></div>';
    return;
  }

  // Render results table
  res.innerHTML =
    '<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:12px">' +
    '<thead><tr style="border-bottom:0.5px solid var(--border2);text-align:left">' +
      '<th style="padding:0.5rem 0.75rem;font-weight:500;color:var(--text3);font-size:10px;text-transform:uppercase;letter-spacing:0.07em">Type</th>' +
      '<th style="padding:0.5rem 0.75rem;font-weight:500;color:var(--text3);font-size:10px;text-transform:uppercase;letter-spacing:0.07em">What</th>' +
      '<th style="padding:0.5rem 0.75rem;font-weight:500;color:var(--text3);font-size:10px;text-transform:uppercase;letter-spacing:0.07em">Where matched</th>' +
      '<th style="padding:0.5rem 0.75rem;font-weight:500;color:var(--text3);font-size:10px;text-transform:uppercase;letter-spacing:0.07em;width:100px"></th>' +
    '</tr></thead><tbody>' +
    rows.map(r =>
      '<tr style="border-bottom:0.5px solid var(--border)">' +
        '<td style="padding:0.7rem 0.75rem;vertical-align:top">' +
          '<span style="display:inline-block;padding:2px 8px;border-radius:100px;font-size:10px;font-weight:500;background:rgba(255,255,255,0.04);border:0.5px solid ' + r.typeColor + ';color:' + r.typeColor + '">' + escapeHtml(r.type) + '</span>' +
          '<div style="font-size:10px;color:var(--text3);margin-top:3px">' + escapeHtml(r.kind) + '</div>' +
        '</td>' +
        '<td style="padding:0.7rem 0.75rem;vertical-align:top">' +
          '<div style="font-weight:500;color:var(--text)">' + _highlight(r.name, q) + '</div>' +
          (r.subtitle ? '<div style="font-size:11px;color:var(--text3);margin-top:2px;font-family:var(--mono)">' + _highlight(r.subtitle, q) + '</div>' : '') +
        '</td>' +
        '<td style="padding:0.7rem 0.75rem;vertical-align:top;font-size:11px">' +
          r.matches.map(m =>
            '<div style="margin-bottom:3px">' +
              '<span style="color:var(--text3);font-family:var(--mono);font-size:10px;text-transform:uppercase;margin-right:6px">' + escapeHtml(m.field) + '</span>' +
              '<span style="color:var(--text2);font-family:var(--mono)">' + _highlight(String(m.text||''), q) + '</span>' +
            '</div>'
          ).join('') +
        '</td>' +
        '<td style="padding:0.7rem 0.75rem;vertical-align:top;text-align:right">' +
          '<button class="btn btn-sm" style="background:var(--purple-bg);color:var(--purple);border:0.5px solid rgba(107,78,142,0.25);font-size:12px;padding:0.35rem 0.85rem" onclick="' + r.gotoAction + '">' + escapeHtml(r.gotoLabel) + ' →</button>' +
        '</td>' +
      '</tr>'
    ).join('') +
    '</tbody></table></div>';
}

// Helpers used by runGlobalSearch only — underscored to flag their private
// scope and keep the global namespace tidy.
function _matchField(v, q) {
  if (v == null) return false;
  return String(v).toLowerCase().indexOf(q) !== -1;
}

// Highlight each case-insensitive occurrence of the query inside an escaped
// string. We escape first, then splice in <mark> so user-controlled source
// content can't inject HTML through the search results.
// Strip credentials out of anything the search is about to display.
// Saved jobs and reports store full connection strings and Azure Function
// URLs, so search results were printing the function key in full — the same
// key the Connections page deliberately masks. Every displayed value goes
// through _highlight, so masking here covers names, subtitles and match
// snippets in one place.
function _maskSecrets(text) {
  return String(text == null ? '' : text)
    // Azure Function key: ?code=… / &code=…
    .replace(/([?&]code=)[^&\s"']+/gi, '$1***')
    // Connection-string secrets: Password=… / Pwd=… / AccountKey=… / SAS sig=…
    .replace(/\b(password|pwd|accountkey)\s*=\s*[^;&\s"']+/gi, '$1=***')
    .replace(/([?&]sig=)[^&\s"']+/gi, '$1***')
    // Credentials embedded in a URL: scheme://user:secret@host
    .replace(/(:\/\/[^:/\s]+):[^@/\s]+@/g, '$1:***@')
    // Bearer tokens pasted into notes or SQL comments
    .replace(/\b(bearer\s+)[A-Za-z0-9._~+/-]{12,}=*/gi, '$1***');
}

function _highlight(text, q) {
  const esc = escapeHtml(_maskSecrets(text));
  if (!q) return esc;
  const escQ = q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return esc.replace(new RegExp('(' + escQ + ')', 'gi'),
    '<mark style="background:rgba(245,158,11,0.25);color:var(--text);padding:0 2px;border-radius:2px">$1</mark>');
}

// Return a ~80-char window around the first match in a long string so long
// SQL / note bodies don't blow up the table. Returns the full string if it's
// already short.
function _snippet(text, q) {
  if (text == null) return '';
  const s = String(text);
  if (s.length <= 100) return s;
  const i = s.toLowerCase().indexOf(q);
  if (i < 0) return s.slice(0, 100) + '…';
  const start = Math.max(0, i - 30);
  const end   = Math.min(s.length, i + q.length + 50);
  return (start > 0 ? '…' : '') + s.slice(start, end) + (end < s.length ? '…' : '');
}

// JS string escape for values interpolated into an onclick="…" attribute.
// Prevents quote-breakout when a job id contains a single quote (unlikely
// but safer to handle).
function _jsEsc(s) {
  return String(s == null ? '' : s).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

// Go-to actions for sources that don't already have a top-level openFoo()
// helper. Projects open in the project-builder with the id pre-selected
// (the builder reads cygenix_active_project_id). Inventory just switches
// to the inventory view — we have no per-item deep link.
function _openProjectFromSearch(projectId) {
  if (projectId) {
    try { localStorage.setItem('cygenix_active_project_id', projectId); } catch {}
  }
  window.location.href = '/project-builder.html';
}

function _openInventoryFromSearch(_docId) {
  // Inventory doesn't deep-link to individual docs yet; the view re-renders
  // all items and the user can filter from there. If/when per-doc anchors
  // get added, stash the id in sessionStorage and have renderInventory()
  // scroll to it.
  showView('inventory');
}

// Delete confirm + refresh the list.
async function deleteSavedReport(id, name) {
  if (!confirm('Delete saved report "' + name + '"? This cannot be undone.')) return;
  const email = currentCygenixEmail();
  if (!email) { alert('Please sign in first.'); return; }
  try {
    const r = await cygenixFetch('/.netlify/functions/reports', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'delete', id }),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || ('HTTP ' + r.status));
    invalidateReportsList();
    renderReportsList();
  } catch (e) {
    alert('Could not delete report: ' + e.message);
  }
}

// Minimal html-entity escaper — we inline projectName etc. into HTML above so
// a naïvely-named run ("My <Bad> Run") can't break layout or inject markup.
function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

// ── Report settings: per-job reconciliation config ─────────────────────────
// Pilot scope: one metric per job. Two metric types (SUM, COUNT_DISTINCT).
// Data lives on the job record (cygenix_jobs in localStorage) under a
// `reconciliation` field. Runner reads it at run time in project-builder.html.
// No Cosmos/backend work — the results travel in the report document itself.

function reconLoadJobs() {
  try { return JSON.parse(localStorage.getItem('cygenix_jobs') || '[]'); }
  catch { return []; }
}

function reconSaveJobs(jobs) {
  try { localStorage.setItem('cygenix_jobs', JSON.stringify(jobs)); }
  catch (e) { console.error('[recon] save failed', e); alert('Could not save: ' + e.message); }
}

// Pulls the list of candidate columns from a job's columnMapping. For SUM we
// show every tgt column (user picks the numeric one themselves — we don't
// introspect types). For COUNT_DISTINCT any column is valid.
function reconColumnsForJob(job) {
  const cm = Array.isArray(job.columnMapping) ? job.columnMapping : [];
  // We offer BOTH source and target column names. Source SUMs the pre-migration
  // value, target SUMs the post-migration value — they usually match but may
  // differ (different transform / rename).
  const srcCols = cm.filter(m => m.srcCol).map(m => m.srcCol);
  const tgtCols = cm.filter(m => m.tgtCol).map(m => m.tgtCol);
  return { srcCols: [...new Set(srcCols)], tgtCols: [...new Set(tgtCols)] };
}

function renderReportSettings() {
  const el = document.getElementById('recon-settings-list');
  if (!el) return;
  const jobs = reconLoadJobs();
  const migrationJobs = jobs.filter(j => {
    // Heuristic: only migration-type jobs make sense for reconciliation.
    // SQL-script jobs (no columnMapping) don't have a natural source/target to compare.
    return Array.isArray(j.columnMapping) && j.columnMapping.length > 0;
  });
  if (migrationJobs.length === 0) {
    el.innerHTML = '<div class="empty-state"><h3>No migration jobs yet</h3><p>Create an Object Mapping job first, then come back here to add a reconciliation check.</p></div>';
    return;
  }
  el.innerHTML = migrationJobs.map(job => reconRenderJobCard(job)).join('');
  reconBindChipsFields(el);
}

// Normalise a reconciliation object to the new multi-check shape. Older jobs
// have a flat `{ enabled, metric, sourceColumn, targetColumn, groupByColumn,
// tolerance }` that we turn into a single-element `checks[]` array without
// losing the old flat fields (kept for any downstream code that still reads
// them — the runner always prefers `checks` when present).
//
// New shape:
//   reconciliation = {
//     enabled: boolean,           // master toggle (job-level)
//     checks: [
//       {
//         id: 'chk_xxx',
//         metric: 'SUM' | 'COUNT_DISTINCT',
//         sourceColumn, targetColumn,
//         groupByColumns: [ 'col1', 'col2', ... ],   // array, not single string
//         tolerance: number,
//       },
//       ...
//     ],
//   }
//
// Pass/fail semantics:
//   Each check has ONE tolerance. The check passes if the OVERALL aggregate
//   (ungrouped) is within tolerance. Grouped rows are informational — they
//   appear in the report for analysis but do not influence the check's pass
//   state. This is an explicit design choice; see the conversation notes.
function reconMigrateShape(r) {
  if (!r) return { enabled: false, checks: [] };
  // Already new shape
  if (Array.isArray(r.checks)) {
    return {
      enabled: !!r.enabled,
      checks: r.checks.map(reconNormaliseCheck),
    };
  }
  // Old flat shape → synthesise a single check
  if (r.metric || r.sourceColumn || r.targetColumn || r.groupByColumn) {
    return {
      enabled: !!r.enabled,
      checks: [reconNormaliseCheck({
        metric:         r.metric || 'SUM',
        sourceColumn:   r.sourceColumn || '',
        targetColumn:   r.targetColumn || '',
        groupByColumns: r.groupByColumn ? [r.groupByColumn] : [],
        tolerance:      r.tolerance != null ? r.tolerance : 0.01,
      })],
    };
  }
  return { enabled: !!r.enabled, checks: [] };
}

function reconNormaliseCheck(c) {
  return {
    id:             c.id || ('chk_' + Math.random().toString(36).slice(2, 9)),
    metric:         c.metric || 'SUM',
    sourceColumn:   c.sourceColumn || '',
    targetColumn:   c.targetColumn || '',
    groupByColumns: Array.isArray(c.groupByColumns)
      ? c.groupByColumns.filter(Boolean)
      : (c.groupByColumn ? [c.groupByColumn] : []),
    tolerance:      c.tolerance != null ? c.tolerance : 0.01,
  };
}

function reconRenderJobCard(job) {
  const config = reconMigrateShape(job.reconciliation);
  const { srcCols, tgtCols } = reconColumnsForJob(job);
  const jobIdEsc  = escapeHtml(job.id);
  const jobIdAttr = job.id.replace(/"/g, '&quot;');
  const srcTable  = escapeHtml(job.sourceTable || job.source || '—');
  const tgtTable  = escapeHtml(job.targetTable || job.target || '—');
  const enabled   = config.enabled ? 'checked' : '';

  const statusLabel = config.enabled
    ? '<span class="badge badge-green" style="font-size:10px">Active</span>'
    : '<span class="badge" style="font-size:10px;background:var(--bg3);color:var(--text3)">Off</span>';

  // Collapsed summary: count of configured checks, or "Not configured"
  let summary;
  if (config.enabled && config.checks.length) {
    const n = config.checks.length;
    summary = `${n} check${n === 1 ? '' : 's'} configured`;
  } else {
    summary = 'Not configured';
  }

  const checksHtml = config.checks.length
    ? reconRenderChecksTable(job.id, config.checks, srcCols, tgtCols)
    : '<div class="rc-empty">No checks yet. Add one below.</div>';

  return `<div class="recon-card" style="background:var(--bg2);border:0.5px solid var(--border);border-radius:var(--r-lg);margin-bottom:0.5rem" data-recon-job="${jobIdEsc}" data-recon-open="false">
    <div class="recon-card-head" onclick="reconToggleCard('${jobIdEsc}')" style="padding:0.9rem 1.25rem;display:flex;align-items:center;justify-content:space-between;gap:1rem;flex-wrap:wrap;cursor:pointer;user-select:none">
      <div style="display:flex;align-items:center;gap:0.75rem;flex:1;min-width:0">
        <span class="recon-chevron" style="color:var(--text3);font-size:10px;transition:transform 0.15s;display:inline-block;flex-shrink:0">▶</span>
        <div style="min-width:0">
          <div style="font-size:13px;font-weight:600">${escapeHtml(job.name || 'Unnamed job')}</div>
          <div style="font-size:11px;color:var(--text3);font-family:var(--mono);margin-top:2px">${srcTable} &nbsp;→&nbsp; ${tgtTable}</div>
          <div style="font-size:11px;color:var(--text3);margin-top:3px" class="recon-summary">${summary}</div>
        </div>
      </div>
      <div style="flex-shrink:0">${statusLabel}</div>
    </div>
    <div class="recon-card-body" style="display:none;padding:0 1.25rem 1.1rem;border-top:0.5px solid var(--border)">
      <div style="padding-top:0.9rem;margin-bottom:0.25rem;display:flex;align-items:center;justify-content:space-between;gap:1rem;flex-wrap:wrap">
        <label style="display:inline-flex;align-items:center;gap:6px;font-size:12px;color:var(--text2);cursor:pointer">
          <input type="checkbox" data-recon-enabled ${enabled} style="cursor:pointer">
          Enable reconciliation
        </label>
        <div style="font-size:10px;color:var(--text3)">Each check passes if the overall aggregate is within tolerance. Group-by columns give a per-group breakdown in the report.</div>
      </div>
      <div data-recon-checks-mount>${checksHtml}</div>
      <button type="button" class="rc-add-row" onclick="reconAddCheck('${jobIdEsc}')">+ Add check</button>
      <div style="margin-top:0.8rem;display:flex;justify-content:flex-end;gap:0.5rem">
        <button class="btn btn-sm btn-ghost" onclick="event.stopPropagation();reconSaveOne('${jobIdEsc}')" style="font-size:12px">Save</button>
      </div>
    </div>
  </div>`;
}

// Render the checks table (one row per check). Extracted so reconAddCheck and
// the remove-row handler can rebuild just this block without re-rendering the
// whole card (which would reset the enable-toggle and the rest of the body).
function reconRenderChecksTable(jobId, checks, srcCols, tgtCols) {
  const rows = checks.map((c, idx) => reconRenderCheckRow(jobId, c, idx, srcCols, tgtCols)).join('');
  return `<table class="rc-table" data-recon-checks-table>
    <thead><tr>
      <th style="width:120px">Metric</th>
      <th>Source col</th>
      <th>Target col</th>
      <th>Group by</th>
      <th style="width:100px">Tolerance</th>
      <th style="width:28px"></th>
    </tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
}

function reconRenderCheckRow(jobId, check, idx, srcCols, tgtCols) {
  const metricSum = check.metric === 'SUM'            ? 'selected' : '';
  const metricCnt = check.metric === 'COUNT_DISTINCT' ? 'selected' : '';
  const srcOpts = srcCols.map(c => `<option value="${escapeHtml(c)}"${check.sourceColumn === c ? ' selected' : ''}>${escapeHtml(c)}</option>`).join('');
  const tgtOpts = tgtCols.map(c => `<option value="${escapeHtml(c)}"${check.targetColumn === c ? ' selected' : ''}>${escapeHtml(c)}</option>`).join('');
  const chipsField = reconRenderChipsField(check.groupByColumns || [], srcCols);
  const tol = check.tolerance != null ? check.tolerance : 0.01;
  const chkIdEsc = escapeHtml(check.id);
  return `<tr data-recon-check="${chkIdEsc}">
    <td><select data-recon-check-field="metric">
      <option value="SUM" ${metricSum}>SUM</option>
      <option value="COUNT_DISTINCT" ${metricCnt}>COUNT DISTINCT</option>
    </select></td>
    <td><select data-recon-check-field="sourceColumn">
      <option value="">— pick —</option>${srcOpts}
    </select></td>
    <td><select data-recon-check-field="targetColumn">
      <option value="">— pick —</option>${tgtOpts}
    </select></td>
    <td>${chipsField}</td>
    <td><input type="number" step="0.01" min="0" data-recon-check-field="tolerance" value="${tol}"></td>
    <td style="text-align:center">
      <button type="button" class="rc-row-del" onclick="reconRemoveCheck('${escapeHtml(jobId)}','${chkIdEsc}')" title="Remove this check">✕</button>
    </td>
  </tr>`;
}

// Chip-style multi-select: visible chips for selected columns inside a field
// that opens a checkbox menu on click. Falls back to a disabled-looking empty
// state when the job has no mapping columns.
function reconRenderChipsField(selected, allCols) {
  const sel = Array.isArray(selected) ? selected.filter(Boolean) : [];
  const serialisedSel = escapeHtml(JSON.stringify(sel));
  const chips = sel.length
    ? sel.map(c => `<span class="rc-chip">${escapeHtml(c)}<span class="rc-chip-x" data-recon-chip-remove="${escapeHtml(c)}">×</span></span>`).join('')
    : '<span class="rc-chips-placeholder">— no grouping —</span>';
  const menuItems = allCols.length
    ? allCols.map(c => {
        const checked = sel.includes(c) ? 'checked' : '';
        return `<label class="rc-chips-menu-item" data-recon-chip-toggle="${escapeHtml(c)}">
          <input type="checkbox" ${checked}>${escapeHtml(c)}
        </label>`;
      }).join('')
    : '<div class="rc-chips-menu-empty">No columns available</div>';
  return `<div class="rc-chips-field" data-recon-chips data-recon-chips-value="${serialisedSel}" tabindex="0">
    <div class="rc-chips-body" style="display:flex;flex-wrap:wrap;gap:3px;align-items:center;flex:1;min-width:0">${chips}</div>
    <div class="rc-chips-menu">${menuItems}</div>
  </div>`;
}

// Delegated handler: wire up chip-field clicks within a recon card body. Runs
// after the card body content is (re)rendered so a freshly-mounted table
// becomes interactive immediately.
function reconBindChipsFields(scope) {
  const root = scope || document;
  // Outside-click closer — install once, idempotent
  if (!window._reconChipsCloserInstalled) {
    document.addEventListener('click', ev => {
      document.querySelectorAll('.rc-chips-field.open').forEach(f => {
        if (!f.contains(ev.target)) f.classList.remove('open');
      });
    });
    window._reconChipsCloserInstalled = true;
  }
  root.querySelectorAll('[data-recon-chips]').forEach(field => {
    if (field._reconBound) return;
    field._reconBound = true;
    field.addEventListener('click', ev => {
      const removeChip = ev.target.getAttribute && ev.target.getAttribute('data-recon-chip-remove');
      if (removeChip != null) {
        // Remove this column
        ev.stopPropagation();
        const sel = reconChipsGetSelected(field);
        const next = sel.filter(c => c !== removeChip);
        reconChipsSetSelected(field, next);
        return;
      }
      const toggleItem = ev.target.closest('[data-recon-chip-toggle]');
      if (toggleItem) {
        ev.stopPropagation();
        ev.preventDefault();
        const col = toggleItem.getAttribute('data-recon-chip-toggle');
        const sel = reconChipsGetSelected(field);
        const next = sel.includes(col) ? sel.filter(c => c !== col) : sel.concat([col]);
        reconChipsSetSelected(field, next);
        return;
      }
      // Click anywhere else in the field toggles the menu. Close any others first.
      document.querySelectorAll('.rc-chips-field.open').forEach(f => { if (f !== field) f.classList.remove('open'); });
      field.classList.toggle('open');
    });
  });
}

function reconChipsGetSelected(field) {
  try { return JSON.parse(field.getAttribute('data-recon-chips-value') || '[]'); }
  catch { return []; }
}

function reconChipsSetSelected(field, arr) {
  field.setAttribute('data-recon-chips-value', JSON.stringify(arr));
  // Re-render chips body + checkbox states without closing the menu
  const body = field.querySelector('.rc-chips-body');
  if (body) {
    body.innerHTML = arr.length
      ? arr.map(c => `<span class="rc-chip">${escapeHtml(c)}<span class="rc-chip-x" data-recon-chip-remove="${escapeHtml(c)}">×</span></span>`).join('')
      : '<span class="rc-chips-placeholder">— no grouping —</span>';
  }
  field.querySelectorAll('[data-recon-chip-toggle]').forEach(item => {
    const col = item.getAttribute('data-recon-chip-toggle');
    const cb  = item.querySelector('input[type="checkbox"]');
    if (cb) cb.checked = arr.includes(col);
  });
}

// Add a fresh empty check to a job's card. Mutates the in-DOM state only;
// nothing hits localStorage until the user hits Save.
function reconAddCheck(jobId) {
  const card = document.querySelector(`[data-recon-job="${CSS.escape(jobId)}"]`);
  if (!card) return;
  const mount = card.querySelector('[data-recon-checks-mount]');
  if (!mount) return;
  // Collect current check state from the DOM so we don't lose in-flight edits
  const job = reconLoadJobs().find(j => j.id === jobId);
  if (!job) return;
  const { srcCols, tgtCols } = reconColumnsForJob(job);
  const currentChecks = reconReadChecksFromDom(card);
  currentChecks.push(reconNormaliseCheck({
    metric: 'SUM', sourceColumn: '', targetColumn: '', groupByColumns: [], tolerance: 0.01,
  }));
  mount.innerHTML = reconRenderChecksTable(jobId, currentChecks, srcCols, tgtCols);
  reconBindChipsFields(mount);
}

function reconRemoveCheck(jobId, checkId) {
  const card = document.querySelector(`[data-recon-job="${CSS.escape(jobId)}"]`);
  if (!card) return;
  const row = card.querySelector(`[data-recon-check="${CSS.escape(checkId)}"]`);
  if (row) row.remove();
  // If last row removed, show the empty state card
  const tbody = card.querySelector('[data-recon-checks-table] tbody');
  if (tbody && tbody.children.length === 0) {
    const mount = card.querySelector('[data-recon-checks-mount]');
    if (mount) mount.innerHTML = '<div class="rc-empty">No checks yet. Add one below.</div>';
  }
}

// Read every check row currently in the DOM and build an array of objects.
// Used by reconSaveOne, reconAddCheck, etc.
function reconReadChecksFromDom(card) {
  const rows = card.querySelectorAll('[data-recon-check]');
  return Array.from(rows).map(tr => {
    const chk = { id: tr.getAttribute('data-recon-check') };
    tr.querySelectorAll('[data-recon-check-field]').forEach(inp => {
      const f = inp.getAttribute('data-recon-check-field');
      if (inp.type === 'number') chk[f] = parseFloat(inp.value);
      else                       chk[f] = (inp.value || '').trim();
    });
    const chips = tr.querySelector('[data-recon-chips]');
    chk.groupByColumns = chips ? reconChipsGetSelected(chips) : [];
    return reconNormaliseCheck(chk);
  });
}

function reconSaveOne(jobId) {
  const card = document.querySelector(`[data-recon-job="${CSS.escape(jobId)}"]`);
  if (!card) return;
  const jobs = reconLoadJobs();
  const job  = jobs.find(j => j.id === jobId);
  if (!job) { alert('Job not found'); return; }

  const enabledInput = card.querySelector('[data-recon-enabled]');
  const enabled = !!(enabledInput && enabledInput.checked);
  const checks = reconReadChecksFromDom(card);

  // Validation: if enabled, must have at least one check and every check
  // must have metric + at least one column. Empty (all-default) rows are OK
  // if master toggle is off — user may be mid-edit.
  if (enabled) {
    if (!checks.length) { alert('Add at least one check before enabling, or switch the master toggle off.'); return; }
    for (let i = 0; i < checks.length; i++) {
      const c = checks[i];
      if (!c.metric) { alert(`Check ${i + 1}: pick a metric.`); return; }
      if (!c.sourceColumn && !c.targetColumn) {
        alert(`Check ${i + 1}: pick at least one column.`); return;
      }
    }
  }

  job.reconciliation = {
    enabled,
    checks: checks.map(c => ({
      id:             c.id,
      metric:         c.metric || 'SUM',
      sourceColumn:   c.sourceColumn || '',
      targetColumn:   c.targetColumn || '',
      groupByColumns: Array.isArray(c.groupByColumns) ? c.groupByColumns : [],
      tolerance:      c.tolerance >= 0 ? c.tolerance : 0.01,
    })),
  };
  reconSaveJobs(jobs);

  // Visual confirm: Save → ✓ Saved for 1.5s, then re-render the card list to
  // refresh the collapsed summary + Active/Off badge. Same container-detection
  // logic as before (works whether the card is in the old global Settings
  // view or the per-report modal).
  const btn = card.querySelector('.recon-card-body button');
  const containerId = card.closest('#report-recon-list') ? 'report-recon-list' : 'recon-settings-list';
  if (btn) {
    const prev = btn.textContent;
    btn.textContent = '✓ Saved';
    btn.disabled = true;
    setTimeout(() => {
      btn.textContent = prev;
      btn.disabled = false;
      if (containerId === 'report-recon-list') {
        const host = document.getElementById('report-recon-list');
        if (host) {
          const visibleIds = [...host.querySelectorAll('[data-recon-job]')].map(n => n.getAttribute('data-recon-job'));
          const refreshed = reconLoadJobs().filter(j => visibleIds.includes(j.id));
          host.innerHTML = refreshed.map(j => reconRenderJobCard(j)).join('');
          reconBindChipsFields(host);
        }
      } else {
        renderReportSettings();
        reconBindChipsFields(document.getElementById('recon-settings-list'));
      }
      setTimeout(() => reconToggleCard(jobId), 50);
    }, 1500);
  }
}

// Toggle card open/closed. Ensures only ONE card is open at a time — clicking
// an already-closed card opens it and collapses all others. Clicking the open
// card collapses it. This keeps the page short and focused on the job being edited.
function reconToggleCard(jobId) {
  const cards = document.querySelectorAll('.recon-card');
  const target = document.querySelector(`[data-recon-job="${CSS.escape(jobId)}"]`);
  if (!target) return;
  const wasOpen = target.getAttribute('data-recon-open') === 'true';
  // Close everything first
  cards.forEach(c => {
    c.setAttribute('data-recon-open', 'false');
    const body = c.querySelector('.recon-card-body');
    const chev = c.querySelector('.recon-chevron');
    if (body) body.style.display = 'none';
    if (chev) chev.style.transform = 'rotate(0deg)';
  });
  // Then open the clicked one if it was closed
  if (!wasOpen) {
    target.setAttribute('data-recon-open', 'true');
    const body = target.querySelector('.recon-card-body');
    const chev = target.querySelector('.recon-chevron');
    if (body) body.style.display = 'block';
    if (chev) chev.style.transform = 'rotate(90deg)';
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── INVENTORY ─────────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

const INV_KEY = 'cygenix_inventory';
let invItems      = loadInventory();
let invPendingFiles = [];
let invCurrentFilter = 'all';
let invPreviewId  = null;

function loadInventory() {
  try { return JSON.parse(localStorage.getItem(INV_KEY) || '[]'); } catch { return []; }
}
function saveInventory() {
  try { localStorage.setItem(INV_KEY, JSON.stringify(invItems)); } catch {}
  updateInvCount();
}
function updateInvCount() {
  const el = document.getElementById('inv-count');
  if (el) el.textContent = invItems.length;
}

// ── Type config ───────────────────────────────────────────────────────────────
const DOC_TYPES = {
  artifact:   { label: 'Project Artifact',  icon: '<i class="ic ic-mail"></i>', cls: 'doc-type-artifact'   },
  wasis:      { label: 'Was/Is Mapping',   icon: '<i class="ic ic-sync"></i>', cls: 'doc-type-wasis'      },
  dictionary: { label: 'Data Dictionary',  icon: '<i class="ic ic-book"></i>', cls: 'doc-type-dictionary' },
  schema:     { label: 'Schema Mapping',   icon: '<i class="ic ic-archive"></i>', cls: 'doc-type-schema'     },
  rules:      { label: 'Business Rules',   icon: '<i class="ic ic-cog"></i>', cls: 'doc-type-rules'      },
  signoff:    { label: 'Client Sign-off',  icon: '<i class="ic ic-check-circle"></i>', cls: 'doc-type-signoff'    },
  notes:      { label: 'Notes',            icon: '<i class="ic ic-edit"></i>', cls: 'doc-type-notes'      },
  other:      { label: 'Other',            icon: '<i class="ic ic-doc"></i>', cls: 'doc-type-other'      },
};

// ── Upload flow ───────────────────────────────────────────────────────────────
function invDragOver(e)  { e.preventDefault(); document.getElementById('inv-upload-zone').classList.add('drag'); }
function invDragLeave()  { document.getElementById('inv-upload-zone').classList.remove('drag'); }
function invDrop(e)      { e.preventDefault(); document.getElementById('inv-upload-zone').classList.remove('drag'); invHandleFiles(e.dataTransfer.files); }

function invHandleFiles(fileList) {
  invPendingFiles = Array.from(fileList);
  if (!invPendingFiles.length) return;
  // Show type selector for first file (process one at a time)
  showInvTypeModal(invPendingFiles[0]);
}

function showInvTypeModal(file) {
  document.getElementById('inv-pending-name').textContent = file.name;
  document.getElementById('inv-type-modal').style.display = 'block';
  // Populate the project scope dropdown fresh every time — projects may have
  // been added since the last upload.
  const sel = document.getElementById('inv-pending-project');
  if (sel) {
    let projects = [];
    try { projects = JSON.parse(localStorage.getItem('cygenix_projects') || '[]'); } catch {}
    const activeId = localStorage.getItem('cygenix_active_project_id') || '';
    const optsHtml = ['<option value="">— Unassigned (not linked to any project) —</option>']
      .concat(projects.map(p => {
        const selected = p.id === activeId ? ' selected' : '';
        return `<option value="${escapeHtml(p.id)}"${selected}>${escapeHtml(p.name || p.id)}</option>`;
      }))
      .join('');
    sel.innerHTML = optsHtml;
  }
  document.querySelectorAll('.inv-type-btn').forEach(btn => {
    btn.onmouseenter = () => btn.style.borderColor = 'var(--accent)';
    btn.onmouseleave = () => btn.style.borderColor = 'var(--border2)';
  });
}

function invSelectType(type) {
  const file = invPendingFiles[0];
  if (!file) return;
  // Capture project scope BEFORE hiding the modal (that clears the dropdown selection)
  const projSel = document.getElementById('inv-pending-project');
  const projectId = projSel ? (projSel.value || '') : '';
  document.getElementById('inv-type-modal').style.display = 'none';

  const reader = new FileReader();
  reader.onload = e => {
    const item = {
      id:        'inv_' + Date.now() + '_' + Math.random().toString(36).slice(2,6),
      name:      file.name,
      type,
      projectId, // '' when unassigned; otherwise matches a cygenix_projects entry
      ext:       file.name.split('.').pop().toLowerCase(),
      size:      file.size,
      mimeType:  file.type || '',
      addedAt:   new Date().toISOString(),
      content:   e.target.result, // base64 for binary, text for text
      isText:    typeof e.target.result === 'string' && !e.target.result.startsWith('data:'),
    };
    invItems.unshift(item);
    saveInventory();
    renderInventory();

    // Process next file if any
    invPendingFiles.shift();
    if (invPendingFiles.length > 0) {
      setTimeout(() => showInvTypeModal(invPendingFiles[0]), 300);
    }
  };

  // Read as text for text-based files, dataURL for everything else
  const textExts = ['csv','txt','md','sql','json','tsv','xml','html','js','py'];
  if (textExts.includes(file.name.split('.').pop().toLowerCase())) {
    reader.readAsText(file);
  } else {
    reader.readAsDataURL(file);
  }
}

function invCancelUpload() {
  invPendingFiles = [];
  document.getElementById('inv-type-modal').style.display = 'none';
}

// ── Notes ─────────────────────────────────────────────────────────────────────
function invAddNote() {
  document.getElementById('inv-note-form').style.display = 'block';
  document.getElementById('inv-note-title').focus();
}

function invSaveNote() {
  const title = document.getElementById('inv-note-title').value.trim();
  const body  = document.getElementById('inv-note-body').value.trim();
  if (!title && !body) { alert('Please enter a title or note text.'); return; }
  const item = {
    id:      'inv_' + Date.now(),
    name:    title || 'Untitled note',
    type:    'notes',
    ext:     'txt',
    size:    body.length,
    addedAt: new Date().toISOString(),
    content: body,
    isText:  true,
    isNote:  true,
  };
  invItems.unshift(item);
  saveInventory();
  renderInventory();
  updateWasisBanner();
  document.getElementById('inv-note-form').style.display = 'none';
  document.getElementById('inv-note-title').value = '';
  document.getElementById('inv-note-body').value  = '';
}

// ── Filter ────────────────────────────────────────────────────────────────────
function invFilter(type) {
  invCurrentFilter = type;
  document.querySelectorAll('.inv-filter').forEach(btn => {
    const isActive = btn.dataset.filter === type;
    btn.style.background = isActive ? 'var(--accent)' : 'transparent';
    btn.style.color = isActive ? '#fff' : 'var(--text2)';
  });
  renderInventory();
}

function updateInvFilterCounts() {
  const types = ['wasis','dictionary','schema','rules','signoff','notes','other'];
  types.forEach(t => {
    const el = document.getElementById('inv-count-' + t);
    if (el) {
      const n = invItems.filter(d => d.type === t).length;
      el.textContent = n > 0 ? n : '';
    }
  });
  const allEl = document.getElementById('inv-count-all');
  if (allEl) allEl.textContent = invItems.length > 0 ? invItems.length : '';
}

// ── Render ────────────────────────────────────────────────────────────────────
function renderInventory() {
  updateInvCount();
  updateInvFilterCounts();
  const filtered = invCurrentFilter === 'all'
    ? invItems
    : invItems.filter(d => d.type === invCurrentFilter);

  const grid  = document.getElementById('inv-doc-grid');
  const empty = document.getElementById('inv-empty');
  if (!grid) return;

  if (!filtered.length) {
    grid.innerHTML  = '';
    if (empty) empty.style.display = 'block';
    return;
  }
  if (empty) empty.style.display = 'none';

  // ── Group docs by projectId ─────────────────────────────────────────────────
  // Each project gets its own folder. Unassigned docs (projectId='' or missing)
  // land in a special "Unassigned" folder at the bottom. Projects that exist
  // but have no docs are NOT rendered here — we only show folders that have
  // content (keeps the view uncluttered when projects are young).
  let projects = [];
  try { projects = JSON.parse(localStorage.getItem('cygenix_projects') || '[]'); } catch {}
  const projectById = new Map(projects.map(p => [p.id, p]));
  const activeProjectId = localStorage.getItem('cygenix_active_project_id') || '';

  // Bucket docs by projectId. Unknown projectId values (e.g. a project was
  // deleted but docs linger) fall into Unassigned — better than hiding them.
  const buckets = new Map();   // projectId → docs[]
  filtered.forEach(doc => {
    const pid = doc.projectId && projectById.has(doc.projectId) ? doc.projectId : '';
    if (!buckets.has(pid)) buckets.set(pid, []);
    buckets.get(pid).push(doc);
  });

  // Order: active project first (if it has docs), then other projects A→Z,
  // then Unassigned at the bottom.
  const keys = Array.from(buckets.keys());
  const sorted = [];
  if (activeProjectId && buckets.has(activeProjectId)) sorted.push(activeProjectId);
  keys
    .filter(k => k && k !== activeProjectId)
    .sort((a, b) => (projectById.get(a)?.name || '').localeCompare(projectById.get(b)?.name || ''))
    .forEach(k => sorted.push(k));
  if (buckets.has('')) sorted.push(''); // Unassigned last

  grid.innerHTML = sorted.map(pid => {
    const docs = buckets.get(pid);
    const proj = pid ? projectById.get(pid) : null;
    const title = proj ? escHtml(proj.name || proj.id) : 'Unassigned';
    const icon  = proj ? '' : '';
    const meta  = docs.length + ' doc' + (docs.length === 1 ? '' : 's');
    const activeBadge = pid && pid === activeProjectId
      ? '<span class="badge badge-blue" style="font-size:10px;margin-left:8px">Active</span>'
      : '';
    // Default-open the active project and the unassigned bucket; other folders
    // start collapsed so the view stays scannable when there are many projects.
    const defaultOpen = pid === activeProjectId || pid === '' ? 'true' : 'false';
    const unassigned  = pid === '' ? 'true' : 'false';
    const docCards = docs.map(doc => invRenderDocCard(doc, projectById)).join('');
    return `<div class="inv-folder" data-open="${defaultOpen}" data-unassigned="${unassigned}" data-project-id="${escHtml(pid)}">
      <div class="inv-folder-head" onclick="invToggleFolder(this)">
        <div class="inv-folder-title"><span class="chev">▶</span><span class="icon">${icon}</span>${title}${activeBadge}</div>
        <div class="inv-folder-meta">${meta}</div>
      </div>
      <div class="inv-folder-body">
        <div class="inv-folder-grid">${docCards}</div>
      </div>
    </div>`;
  }).join('');
}

// Click handler for the folder header row. Simple toggle — no "only one at a
// time" behaviour; users can keep multiple folders open.
function invToggleFolder(headEl) {
  const folder = headEl.closest('.inv-folder');
  if (!folder) return;
  folder.setAttribute('data-open', folder.getAttribute('data-open') === 'true' ? 'false' : 'true');
}

// Render a single doc card. Extracted from renderInventory so folder bodies
// can call it directly. Includes the new "Move to project" action.
function invRenderDocCard(doc, projectById) {
  const cfg  = DOC_TYPES[doc.type] || DOC_TYPES.other;
  const date = new Date(doc.addedAt).toLocaleDateString('en-GB', { day:'numeric', month:'short', year:'numeric' });
  const size = doc.isNote ? (doc.content || '').length + ' chars' : formatInvSize(doc.size);
  return `<div class="inv-doc-card">
    <div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:0.5rem">
      <div class="inv-type-pill ${cfg.cls}">${cfg.icon} ${cfg.label}</div>
      <button onclick="invDelete('${doc.id}')" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:14px;line-height:1;padding:2px 4px" title="Delete">✕</button>
    </div>
    <div class="inv-doc-name" title="${escHtml(doc.name)}">${escHtml(doc.name)}</div>
    <div class="inv-doc-meta">${doc.ext.toUpperCase()} · ${size} · ${date}</div>
    <div class="inv-doc-actions">
      <button class="btn btn-ghost btn-sm" onclick="invPreview('${doc.id}')" style="font-size:11px;flex:1;justify-content:center"><i class="ic ic-eye"></i> Preview</button>
      <button class="btn btn-ghost btn-sm" onclick="invMoveDoc('${doc.id}')" style="font-size:11px;flex:1;justify-content:center" title="Move to a project folder"><i class="ic ic-folder"></i> Move</button>
      <button class="btn btn-ghost btn-sm" onclick="invDownloadDoc('${doc.id}')" style="font-size:11px;flex:1;justify-content:center"><i class="ic ic-download"></i> </button>
    </div>
  </div>`;
}

// Move a doc to a different project folder (or unassign). Uses a proper
// styled modal instead of a browser prompt so the UX matches the rest of
// the dashboard. The target id is stashed on `window._invMovingDocId` for
// invConfirmMove to read — stateful in the DOM rather than in a closure so
// the handlers can be wired as inline onclicks like the other modals.
function invMoveDoc(id) {
  const doc = invItems.find(d => d.id === id);
  if (!doc) return;
  window._invMovingDocId = id;

  // Populate the dropdown with all projects + "Unassigned". Current project
  // is pre-selected; if the doc's projectId points at a deleted project, we
  // treat it as Unassigned.
  let projects = [];
  try { projects = JSON.parse(localStorage.getItem('cygenix_projects') || '[]'); } catch {}
  const sel = document.getElementById('move-doc-select');
  const current = doc.projectId || '';
  const currentExists = !current || projects.some(p => p.id === current);
  const effectiveCurrent = currentExists ? current : '';
  const opts = [{ id: '', name: '— Unassigned —' }]
    .concat(projects.map(p => ({ id: p.id, name: p.name || p.id })));
  sel.innerHTML = opts.map(o => {
    const selected = o.id === effectiveCurrent ? ' selected' : '';
    const suffix   = o.id === effectiveCurrent ? '  (current)' : '';
    return `<option value="${escapeHtml(o.id)}"${selected}>${escapeHtml(o.name)}${suffix}</option>`;
  }).join('');

  // Subtitle reflects which doc we're moving (and notes the broken-link case)
  const sub = document.getElementById('move-doc-sub');
  if (sub) {
    let msg = `Choose a project folder for <strong style="color:var(--text)">${escapeHtml(doc.name)}</strong>.`;
    if (!currentExists && current) {
      msg += ' <span style="color:var(--amber)">This document was linked to a project that no longer exists.</span>';
    }
    sub.innerHTML = msg;
  }

  document.getElementById('move-doc-modal').classList.add('open');
  // Focus the dropdown so keyboard users can arrow-select immediately
  setTimeout(() => sel.focus(), 30);
}

function invCloseMoveDocModal() {
  document.getElementById('move-doc-modal').classList.remove('open');
  window._invMovingDocId = null;
}

function invConfirmMove() {
  const id = window._invMovingDocId;
  if (!id) { invCloseMoveDocModal(); return; }
  const doc = invItems.find(d => d.id === id);
  if (!doc) { invCloseMoveDocModal(); return; }
  const sel = document.getElementById('move-doc-select');
  const newProjectId = sel ? sel.value : '';
  // No-op if it hasn't changed — close silently rather than churn the render
  if ((doc.projectId || '') === newProjectId) { invCloseMoveDocModal(); return; }
  doc.projectId = newProjectId;
  saveInventory();
  renderInventory();
  invCloseMoveDocModal();
}

function formatInvSize(b) {
  if (!b) return '0B';
  if (b < 1024) return b + 'B';
  if (b < 1048576) return (b/1024).toFixed(1) + 'KB';
  return (b/1048576).toFixed(1) + 'MB';
}

// ── Delete ────────────────────────────────────────────────────────────────────
function invDelete(id) {
  const doc = invItems.find(d => d.id === id);
  if (!doc) return;
  if (!confirm(`Delete "${doc.name}"?`)) return;
  invItems = invItems.filter(d => d.id !== id);
  saveInventory();
  renderInventory();
  updateWasisBanner();
}

// ── Preview ───────────────────────────────────────────────────────────────────
function invPreview(id) {
  const doc = invItems.find(d => d.id === id);
  if (!doc) return;
  invPreviewId = id;

  const cfg  = DOC_TYPES[doc.type] || DOC_TYPES.other;
  const date = new Date(doc.addedAt).toLocaleDateString('en-GB', {day:'numeric',month:'long',year:'numeric'});
  document.getElementById('preview-doc-name').textContent = doc.name;
  document.getElementById('preview-doc-meta').textContent = `${cfg.label} · ${doc.ext.toUpperCase()} · Added ${date}`;

  const body = document.getElementById('preview-doc-body');

  // Was/Is CSV — render as a two-column table
  if (doc.type === 'wasis' && (doc.ext === 'csv' || doc.ext === 'tsv' || doc.isText)) {
    body.innerHTML = renderWasIs(doc.content, doc.ext === 'tsv' ? '\t' : ',');
  }
  // Image
  else if (['png','jpg','jpeg','gif','webp','svg'].includes(doc.ext)) {
    const src = doc.content.startsWith('data:') ? doc.content : `data:image/${doc.ext};base64,${doc.content}`;
    body.innerHTML = `<img src="${src}" style="max-width:100%;border-radius:var(--r-lg)" alt="${escHtml(doc.name)}"/>`;
  }
  // PDF — embed in iframe
  else if (doc.ext === 'pdf') {
    const src = doc.content.startsWith('data:') ? doc.content : `data:application/pdf;base64,${doc.content}`;
    body.innerHTML = `<iframe src="${src}" style="width:100%;height:65vh;border:none;border-radius:var(--r)"></iframe>`;
  }
  // CSV/TSV — full table
  else if (doc.ext === 'csv' || doc.ext === 'tsv') {
    body.innerHTML = renderCSVTable(doc.content, doc.ext === 'tsv' ? '\t' : ',');
  }
  // SQL, JSON, Markdown, code — syntax-highlighted code block
  else if (['sql','json','md','js','py','xml','html','txt'].includes(doc.ext) || doc.isNote) {
    const escaped = escHtml(typeof doc.content === 'string' ? doc.content : 'Binary file — download to view');
    body.innerHTML = doc.isNote
      ? `<div class="notes-area">${escaped}</div>`
      : `<div class="code-block" style="color:var(--text2);max-height:65vh">${escaped}</div>`;
  }
  // Everything else
  else {
    body.innerHTML = `<div style="text-align:center;padding:3rem;color:var(--text3)">
      <div style="font-size:48px;margin-bottom:1rem">${cfg.icon}</div>
      <div style="font-size:14px;color:var(--text2);margin-bottom:0.5rem">${escHtml(doc.name)}</div>
      <div style="font-size:12px;margin-bottom:1.5rem">${doc.ext.toUpperCase()} files cannot be previewed directly.</div>
      <button class="btn btn-primary" onclick="invDownloadDoc('${doc.id}')"><i class="ic ic-download"></i> Download to view</button>
    </div>`;
  }

  document.getElementById('inv-preview-modal').classList.add('open');
}

function renderWasIs(content, delim) {
  const lines = content.split(/\r?\n/).filter(l => l.trim());
  if (!lines.length) return '<p style="color:var(--text3)">Empty file</p>';
  const parse = line => {
    const result=[]; let cur=''; let inQ=false;
    for (const ch of line) {
      if (ch==='"') inQ=!inQ;
      else if (ch===delim && !inQ) { result.push(cur.trim()); cur=''; }
      else cur+=ch;
    }
    result.push(cur.trim()); return result;
  };
  const headers = parse(lines[0]);
  const rows    = lines.slice(1).map(parse);
  // Detect Was/Is pattern: 2 cols, or cols named was/is/old/new/from/to
  const isWasIs = headers.length === 2 ||
    headers.some(h => /^(was|old|from|source|before)/i.test(h)) ||
    headers.some(h => /^(is|new|to|target|after)/i.test(h));

  if (isWasIs && headers.length === 2) {
    return `<table class="wasis-table">
      <thead><tr>
        <th style="color:var(--red)">← Was (${escHtml(headers[0])})</th>
        <th style="color:var(--green)">→ Is (${escHtml(headers[1])})</th>
      </tr></thead>
      <tbody>${rows.map(r => `<tr>
        <td class="wasis-was">${escHtml(r[0]||'')}</td>
        <td class="wasis-is">${escHtml(r[1]||'')}</td>
      </tr>`).join('')}</tbody>
    </table>`;
  }
  return renderCSVTable(content, delim);
}

function renderCSVTable(content, delim) {
  const lines = content.split(/\r?\n/).filter(l => l.trim());
  if (!lines.length) return '<p style="color:var(--text3)">Empty file</p>';
  const parse = line => { const r=[]; let c=''; let q=false; for(const ch of line){if(ch==='"')q=!q;else if(ch===delim&&!q){r.push(c.trim());c='';}else c+=ch;} r.push(c.trim()); return r; };
  const headers = parse(lines[0]);
  const rows    = lines.slice(1, 201).map(parse); // cap at 200 rows for performance
  return `<div style="overflow-x:auto"><table class="wasis-table">
    <thead><tr>${headers.map(h => `<th>${escHtml(h)}</th>`).join('')}</tr></thead>
    <tbody>${rows.map(r => `<tr>${headers.map((_,i) => `<td style="font-family:var(--mono);font-size:12px;color:var(--text2)">${escHtml(r[i]||'')}</td>`).join('')}</tr>`).join('')}</tbody>
  </table>${lines.length > 201 ? `<p style="font-size:11px;color:var(--text3);margin-top:0.5rem">Showing first 200 of ${lines.length-1} rows</p>` : ''}</div>`;
}

function closeInvPreview() {
  document.getElementById('inv-preview-modal').classList.remove('open');
  invPreviewId = null;
}

// ── Download ──────────────────────────────────────────────────────────────────
function invDownloadDoc(id) {
  const doc = invItems.find(d => d.id === id);
  if (!doc) return;
  const a = document.createElement('a');
  if (doc.content.startsWith('data:')) {
    a.href = doc.content;
  } else {
    a.href = URL.createObjectURL(new Blob([doc.content], { type: doc.mimeType || 'text/plain' }));
  }
  a.download = doc.name;
  a.click();
}

// Close preview on overlay click
document.getElementById('inv-preview-modal')?.addEventListener('click', function(e) {
  if (e.target === this) closeInvPreview();
});

// ── Cloud sync button ─────────────────────────────────────────────────────────
function syncFromCloud(){
  const btn = document.getElementById('sync-btn');
  if (!btn || typeof CygenixSync === 'undefined') return;
  btn.textContent = '↻ Syncing…';
  btn.disabled = true;
  CygenixSync.forceLoad().then(loaded => {
    if (loaded) {
      btn.textContent = '✓ Synced';
      setTimeout(() => { location.reload(); }, 800);
    } else {
      btn.textContent = '↻ Sync';
      btn.disabled = false;
    }
  });
}

// Show sync button once CygenixSync is ready
(function waitForSync(){
  let tries = 0;
  (function tick(){
    if (typeof CygenixSync !== 'undefined' && CygenixSync.getUserId()) {
      const btn = document.getElementById('sync-btn');
      if (btn) btn.style.display = 'inline-flex';
    } else if (tries++ < 30) {
      // Bounded: signed-out, this used to wake the main thread every second
      // for the life of the tab, forever, to look for a button to show.
      setTimeout(tick, 1000);
    }
  })();
})();

// ── Cloud sync ───────────────────────────────────────────────────────────────────
async function syncFromCloud(){
  const btn = $('sync-btn');
  if(btn){ btn.textContent = '↻ Syncing…'; btn.disabled = true; }
  try {
    if(typeof CygenixSync === 'undefined') { alert('Sync not available — please refresh the page.'); return; }
    const loaded = await CygenixSync.forceLoad();
    if(loaded){
      if(btn){ btn.textContent = '✓ Synced'; }
      setTimeout(() => {
        state.jobs = loadPersistedJobs();
        renderDashboard(); renderAllJobs(); renderInventory(); updateInvCount();
        if(btn){ btn.textContent = '↻ Sync'; btn.disabled = false; }
      }, 800);
    } else {
      if(btn){ btn.textContent = '↻ Sync'; btn.disabled = false; }
      alert('No cloud data found for your account yet.');
    }
  } catch(e) {
    if(btn){ btn.textContent = '↻ Sync'; btn.disabled = false; }
    console.error('Sync error:', e);
  }
}

// ── Init ──────────────────────────────────────────────────────────────────────
// Each step is wrapped in its own try/catch. Without this, a single
// throwing line halts the whole sequence — and several times today
// we've seen "init stops at line N, everything after never runs"
// symptoms (pill stuck on Loading, etc.) caused by an early step
// throwing silently. Now each step is independent; one breaking
// doesn't take the rest with it. Errors get logged so we still see
// what failed.
// Run at DOMContentLoaded, not top-level: the shared scripts in <head> are
// now `defer`red (they used to block first paint), and deferred scripts run
// before DOMContentLoaded — so their globals are guaranteed here.
document.addEventListener('DOMContentLoaded', () => {
try { $('today-date').textContent = 'SQL Migration workspace · ' + new Date().toLocaleDateString('en-GB',{weekday:'long',day:'numeric',month:'long',year:'numeric'}); } catch(e){ console.error('[init] today-date:', e); }
try { window.sconnSecretsMigrationOnce && window.sconnSecretsMigrationOnce(); } catch(e){ console.error('[init] sconn-migration:', e); }
try { renderDashboard();           } catch(e){ console.error('[init] renderDashboard:', e); }
try { checkHealth();               } catch(e){ console.error('[init] checkHealth:', e); }
try { initUserAndProject();        } catch(e){ console.error('[init] initUserAndProject:', e); }
try { renderInventory();           } catch(e){ console.error('[init] renderInventory:', e); }
try { updateInvCount();            } catch(e){ console.error('[init] updateInvCount:', e); }
try { updateWasisBanner();         } catch(e){ console.error('[init] updateWasisBanner:', e); }
});

// Safety net: re-run initUserAndProject after the page has fully
// settled. We've seen cases where the synchronous init-time call
// never reaches the function (specific cause untraced — possibly
// browser extension interference, scope quirk, or interaction with
// an earlier-failing step). A short delayed call catches this:
// if the original call worked, this one is a no-op (function
// just re-runs harmlessly). If the original didn't run, this one
// rescues the pill. Also gives MSAL extra time to populate
// localStorage if it hadn't on the first call.
setTimeout(() => {
  try {
    if (typeof initUserAndProject === 'function') {
      const pillEl = document.getElementById('user-name');
      // Re-run if the pill still looks unpopulated OR if it looks like a
      // raw localAccountId GUID (8-4-4-4-12 hex). The latter can happen
      // when MSAL has multiple cached accounts and getAllAccounts()[0]
      // returns a synthetic-username one whose local-part is a GUID; the
      // safety-net call picks up the now-stable cygenix_entra_account
      // and overwrites the pill with the right display name.
      const txt = (pillEl?.textContent || '').trim();
      const looksLoading = /loading/i.test(txt);
      const looksLikeBareGuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(txt);
      if (pillEl && (looksLoading || looksLikeBareGuid)) {
        initUserAndProject();
      }
    }
  } catch(e) { console.error('[init] safety-net initUserAndProject:', e); }
}, 800);

// Re-render the project status panel whenever Cosmos sync pulls new
// data from the cloud — covers the case where the user has saved a
// new connection on another browser and we want it to show up here
// without a manual refresh. Hooked via the `storage` event, which
// fires when ANOTHER tab writes to localStorage; for SAME-tab writes
// the cosmos sync layer calls localStorage.setItem directly so the
// event doesn't fire — we rely on the sync layer's own redraw hooks
// for that case where present.
window.addEventListener('storage', (e) => {
  if (!e || !e.key) return;
  if (e.key === 'cygenix_saved_connections' || e.key === 'cygenix_projects'){
    try { renderProjectStatus(); } catch {}
  }
  if (e.key === 'cygenix_projects') {
    try { refreshJobsMoveTargetDropdown(); } catch {}
    try { renderDashboardProjects(); } catch {}
  }
});

// ── First-run onboarding ───────────────────────────────────────────────────────
(function checkFirstRun(){
  const dismissed = localStorage.getItem('cygenix_onboarded');
  const ps = JSON.parse(localStorage.getItem('cygenix_project_settings')||'{}');
  const hasProject = !!(ps.name || ps['src-system']);
  if (!dismissed && !hasProject) {
    setTimeout(function(){
      const modal = document.getElementById('onboarding-modal');
      if (modal) modal.style.display = 'flex';
    }, 800);
  }
})();

function dismissOnboarding(){
  localStorage.setItem('cygenix_onboarded','1');
  const modal = document.getElementById('onboarding-modal');
  if (modal) modal.style.display = 'none';
}
function onboardGoTo(view){
  dismissOnboarding();
  if(view === 'mapper') { window.location.href='/object_mapping.html'; return; }
  if(view === 'help') { window.open('/help.html','_blank'); return; }
  showView(view);
}

// Handle cyg_goto navigation from nav.js sidebar links on other pages
(function(){
  const goto = sessionStorage.getItem('cyg_goto');
  if (goto) {
    sessionStorage.removeItem('cyg_goto');
    setTimeout(function(){ if(typeof showView==='function') showView(goto); }, 300);
  }
})();



/* ═══ block 3 — extracted from an inline <script> in dashboard.html ═══ */

// ══════════════════════════════════════════════════════════════════════════════
// REPORT PRESENTATION PANEL — inside the report settings modal
// ══════════════════════════════════════════════════════════════════════════════
// Lives beneath the per-job reconciliation cards. Lets the user configure how
// the rendered report presents its three tables (Migration Results,
// Reconciliation, Column Mapping) — sort, group, totals, pivot, calculated
// columns — plus an overall charts toggle. Saves to the report record's
// `presentationConfig` field via the `save-presentation` action on the
// /.netlify/functions/reports endpoint.
// ══════════════════════════════════════════════════════════════════════════════

// Current report in the settings modal (set by openReportSettings → presMountGrids)
window._presCurrentReport = null;
// Grid instances keyed by table name
window._presGrids = {};
// Dirty flag + initial snapshot for comparison
window._presDirty = false;
window._presInitialSnapshot = null;

// ── DataGrid (dashboard-flavoured; uses .dg-* CSS classes) ────────────────────
class DG {
  constructor(opts) {
    this.mount    = opts.mount;
    this.title    = opts.title || 'grid';
    this.columns  = (opts.columns || []).map(c => ({ ...c }));
    this.rows     = (opts.rows || []).map((r, i) => ({ ...r, __i: i }));
    this.emptyMsg = opts.emptyMsg || 'No data.';
    this.onChange = opts.onChange || null;
    this.filter   = opts.initialFilter || '';
    this.sort     = opts.initialSort || { key: null, dir: 'asc' };
    this.groupBy  = opts.initialGroupBy || null;
    this.showTotals = !!opts.initialShowTotals;
    this.pivotOpen  = !!(opts.initialPivotRow && opts.initialPivotVal);
    this.pivotRow = opts.initialPivotRow || null;
    this.pivotCol = opts.initialPivotCol || null;
    this.pivotVal = opts.initialPivotVal || null;
    this.pivotAgg = opts.initialPivotAgg || 'sum';
    if (Array.isArray(opts.calcColumns)) {
      opts.calcColumns.forEach(cc => {
        this.columns.push({
          key: cc.key || ('_calc_' + Math.random().toString(36).slice(2,8)),
          label: cc.label, type: 'calc', calc: cc.formula,
          align: 'right', mono: true,
        });
      });
    }
    if (this.mount) this.render();
  }
  // Serialise current config for persistence
  serialize() {
    return {
      sort:       this.sort.key ? { key: this.sort.key, dir: this.sort.dir } : null,
      groupBy:    this.groupBy || null,
      showTotals: this.showTotals || false,
      pivotRow:   this.pivotRow || null,
      pivotCol:   this.pivotCol || null,
      pivotVal:   this.pivotVal || null,
      pivotAgg:   this.pivotAgg || 'sum',
      calcColumns: this.columns.filter(c => c.type === 'calc')
        .map(c => ({ key: c.key, label: c.label, formula: c.calc })),
    };
  }
  notifyChange() { if (this.onChange) this.onChange(); }
  static esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, c =>
      ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }
  static fmtNum(v) {
    if (v == null || v === '') return '—';
    const n = Number(v);
    if (!isFinite(n)) return '—';
    return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  evalFormula(expr, row) {
    if (!expr) return null;
    const scope = {};
    this.columns.forEach(c => {
      let v = row[c.key];
      if (v == null || v === '') v = 0;
      else if (typeof v === 'string' && /^-?\d+(\.\d+)?$/.test(v)) v = Number(v);
      scope[c.key] = (typeof v === 'number') ? v : String(v);
    });
    const safeIds = new Set(Object.keys(scope).concat(
      ['Math','abs','round','floor','ceil','min','max','pow','sqrt','log','sign']));
    const ids = (expr.match(/[A-Za-z_][A-Za-z0-9_]*/g) || []);
    for (const id of ids) {
      if (!safeIds.has(id) && !['true','false','null'].includes(id))
        throw new Error('Unknown identifier: ' + id);
    }
    if (/\.\s*(?!abs|round|floor|ceil|min|max|pow|sqrt|log|sign)/.test(expr))
      throw new Error('Only Math.* property access is allowed');
    if (/[;={}]/.test(expr)) throw new Error('Invalid characters in formula');
    const keys = Object.keys(scope);
    const vals = keys.map(k => scope[k]);
    try {
      const fn = new Function(...keys, 'Math',
        'abs','round','floor','ceil','min','max','pow','sqrt','log','sign',
        'return (' + expr + ')');
      const m = Math;
      return fn(...vals, m, m.abs, m.round, m.floor, m.ceil, m.min, m.max,
                m.pow, m.sqrt, m.log, m.sign);
    } catch (e) { throw new Error('Formula error: ' + e.message); }
  }
  recompute() {
    let rows = this.rows.map(r => {
      const out = { ...r };
      this.columns.forEach(c => {
        if (c.type === 'calc' && c.calc) {
          try { out[c.key] = this.evalFormula(c.calc, out); }
          catch { out[c.key] = '#ERR'; }
        }
      });
      return out;
    });
    if (this.filter) {
      const q = this.filter.toLowerCase();
      const visible = this.columns.filter(c => !c.hidden);
      rows = rows.filter(r => visible.some(c => String(r[c.key] ?? '').toLowerCase().includes(q)));
    }
    if (this.sort.key) {
      const col = this.columns.find(c => c.key === this.sort.key);
      const dir = this.sort.dir === 'desc' ? -1 : 1;
      rows.sort((a, b) => {
        const va = a[this.sort.key], vb = b[this.sort.key];
        if (col && (col.type === 'number' || col.type === 'calc'))
          return dir * ((Number(va) || 0) - (Number(vb) || 0));
        return dir * String(va ?? '').localeCompare(String(vb ?? ''));
      });
    }
    return rows;
  }
  aggregate(rows, key, fn) {
    const nums = rows.map(r => Number(r[key])).filter(n => !isNaN(n) && isFinite(n));
    if (fn === 'count') return rows.length;
    if (!nums.length) return null;
    if (fn === 'sum') return nums.reduce((s, n) => s + n, 0);
    if (fn === 'avg') return nums.reduce((s, n) => s + n, 0) / nums.length;
    if (fn === 'min') return Math.min(...nums);
    if (fn === 'max') return Math.max(...nums);
    return null;
  }
  render() {
    if (!this.mount) return;
    const rows = this.recompute();
    this._lastViewRows = rows;
    const parts = [this.renderToolbar(), this.renderPivotPanel()];
    if (this.pivotRow && this.pivotVal) parts.push(this.renderPivot(rows));
    else parts.push(this.renderTable(rows));
    this.mount.innerHTML = parts.join('');
    this.bindEvents();
  }
  renderToolbar() {
    const groupOpts = '<option value="">— none —</option>' +
      this.columns.filter(c => c.type === 'text' || c.type === 'status')
        .map(c => `<option value="${c.key}"${this.groupBy === c.key ? ' selected' : ''}>${DG.esc(c.label)}</option>`).join('');
    const groupChip = this.groupBy
      ? `<span class="dg-chip">Group: ${DG.esc(this.columns.find(c=>c.key===this.groupBy)?.label||'')}<span class="dg-chip-x" data-act="clear-group">×</span></span>` : '';
    const sortChip = this.sort.key
      ? `<span class="dg-chip">Sort: ${DG.esc(this.columns.find(c=>c.key===this.sort.key)?.label||'')} ${this.sort.dir}<span class="dg-chip-x" data-act="clear-sort">×</span></span>` : '';
    return `
      <div class="dg-toolbar">
        <input class="dg-input" type="text" placeholder="Filter rows…" data-act="filter" value="${DG.esc(this.filter)}">
        <span class="dg-toolbar-label">Group by</span>
        <select class="dg-select" data-act="group">${groupOpts}</select>
        <button class="dg-btn ${this.showTotals ? 'active' : ''}" data-act="totals">Σ Totals</button>
        <button class="dg-btn ${this.pivotOpen ? 'active' : ''}" data-act="pivot-toggle">⊞ Pivot</button>
        <button class="dg-btn dg-btn-add" data-act="add-col">+ Add column</button>
        <div class="dg-spacer"></div>
        ${groupChip}${sortChip}
        <span class="dg-meta">${this._lastViewRows.length} of ${this.rows.length}</span>
        <button class="dg-btn" data-act="reset" title="Reset this table">↺</button>
      </div>`;
  }
  renderPivotPanel() {
    if (!this.pivotOpen) return '';
    const opts = (filter) => '<option value="">—</option>' +
      this.columns.filter(filter).map(c => `<option value="${c.key}">${DG.esc(c.label)}</option>`).join('');
    const sel = (html, key) => html.replace(new RegExp(`value="${key}"`), `value="${key}" selected`);
    let rowOpts = opts(c => c.type === 'text' || c.type === 'status');
    let colOpts = opts(c => c.type === 'text' || c.type === 'status');
    let valOpts = opts(c => c.type === 'number' || c.type === 'calc');
    if (this.pivotRow) rowOpts = sel(rowOpts, this.pivotRow);
    if (this.pivotCol) colOpts = sel(colOpts, this.pivotCol);
    if (this.pivotVal) valOpts = sel(valOpts, this.pivotVal);
    const aggOpts = ['sum','avg','count','min','max']
      .map(a => `<option value="${a}"${this.pivotAgg===a?' selected':''}>${a.toUpperCase()}</option>`).join('');
    return `<div class="dg-pivot-panel open">
      <div class="dg-pivot-slot"><label>Rows</label><select data-act="pivot-row">${rowOpts}</select></div>
      <div class="dg-pivot-slot"><label>Columns</label><select data-act="pivot-col">${colOpts}</select></div>
      <div class="dg-pivot-slot"><label>Values</label>
        <select data-act="pivot-val" style="margin-bottom:4px">${valOpts}</select>
        <select data-act="pivot-agg">${aggOpts}</select></div>
      </div>`;
  }
  renderTable(rows) {
    const visible = this.columns.filter(c => !c.hidden);
    if (!rows.length) return `<p style="font-size:12px;color:var(--text3);padding:1rem 0">${DG.esc(this.emptyMsg)}</p>`;
    const header = visible.map(c => {
      const sortCls = this.sort.key === c.key ? ' sort-' + this.sort.dir : '';
      const calcCls = c.type === 'calc' ? ' calc-col' : '';
      const style = c.align === 'right' || c.type === 'number' || c.type === 'calc' ? ' style="text-align:right"' : '';
      const removeBtn = c.type === 'calc'
        ? `<span style="margin-left:6px;opacity:0.5;cursor:pointer" data-act="del-col" data-key="${c.key}" title="Remove column">×</span>` : '';
      return `<th class="sortable${sortCls}${calcCls}"${style} data-sort="${c.key}">${DG.esc(c.label)}<span class="sort-ind">${this.sort.key===c.key?(this.sort.dir==='asc'?'▲':'▼'):'▲▼'}</span>${removeBtn}</th>`;
    }).join('');
    let body = '';
    if (this.groupBy) {
      const groups = new Map();
      rows.forEach(r => { const k = String(r[this.groupBy] ?? '—'); if (!groups.has(k)) groups.set(k, []); groups.get(k).push(r); });
      Array.from(groups.keys()).sort().forEach(k => {
        const groupRows = groups.get(k);
        const cells = visible.map((c, idx) => {
          if (idx === 0) return `<td><strong>${DG.esc(k)}</strong><span class="group-count">(${groupRows.length})</span></td>`;
          if (c.type === 'number' || c.type === 'calc') {
            const total = this.aggregate(groupRows, c.key, 'sum');
            return `<td style="text-align:right;font-family:var(--mono)"><strong>${DG.fmtNum(total)}</strong></td>`;
          }
          return '<td></td>';
        }).join('');
        body += `<tr class="group-header">${cells}</tr>`;
        body += groupRows.map(r => this.renderRow(r, visible)).join('');
      });
    } else {
      body = rows.map(r => this.renderRow(r, visible)).join('');
    }
    let foot = '';
    if (this.showTotals) {
      const cells = visible.map((c, idx) => {
        if (c.type === 'number' || c.type === 'calc') {
          const total = this.aggregate(rows, c.key, 'sum');
          return `<td style="text-align:right">${DG.fmtNum(total)}</td>`;
        }
        if (idx === 0) return '<td>Total</td>';
        return '<td></td>';
      }).join('');
      foot = `<tfoot><tr>${cells}</tr></tfoot>`;
    }
    return `<table class="dg-table"><thead><tr>${header}</tr></thead><tbody>${body}</tbody>${foot}</table>`;
  }
  renderRow(r, visible) {
    const cells = visible.map(c => {
      const v = r[c.key];
      if (c.type === 'status') {
        const lv = String(v||'').toLowerCase();
        const cls = lv === 'success' || lv === 'pass' || lv === 'passed' ? 'dg-badge-green'
                  : lv === 'failed'  || lv === 'fail'  || lv === 'error'  ? 'dg-badge-red'
                  : lv === 'partial' || lv === 'warn'  || lv === 'skipped'|| lv === 'warned' ? 'dg-badge-amber'
                  : 'dg-badge-blue';
        const label = lv === 'success' ? 'Success' : lv === 'partial' ? 'Partial' : lv === 'failed' ? 'Failed'
                    : lv === 'pass' ? 'Pass' : lv === 'warn' ? 'Warn' : lv === 'skipped' ? 'Skipped'
                    : DG.esc(String(v||''));
        return `<td><span class="dg-badge ${cls}">${label}</span></td>`;
      }
      const style = [];
      if (c.mono !== false) style.push('font-family:var(--mono)');
      if (c.size) style.push('font-size:' + c.size + 'px');
      if (c.muted) style.push('color:var(--text3)');
      if (c.bold) style.push('font-weight:500');
      if (c.color === 'accent') style.push('color:var(--accent)');
      if (c.color === 'green') style.push('color:#22c97a');
      if (c.align === 'right' || c.type === 'number' || c.type === 'calc') style.push('text-align:right');
      if (c.dynamicColor) { const col = c.dynamicColor(v); if (col) style.push('color:' + col); }
      const inner = c.render ? c.render(v, r)
        : (c.type === 'number' || c.type === 'calc') ? DG.fmtNum(v)
        : DG.esc(String(v ?? ''));
      const tdCls = c.type === 'calc' ? ' class="calc-col"' : '';
      return `<td${tdCls} style="${style.join(';')}">${inner}</td>`;
    }).join('');
    return `<tr>${cells}</tr>`;
  }
  renderPivot(rows) {
    const rowCol = this.columns.find(c => c.key === this.pivotRow);
    const valCol = this.columns.find(c => c.key === this.pivotVal);
    if (!rowCol || !valCol) return '<p style="font-size:12px;color:var(--text3);padding:1rem">Select a Rows axis and a Values column to see the pivot.</p>';
    const rowVals = Array.from(new Set(rows.map(r => String(r[this.pivotRow] ?? '—')))).sort();
    const colVals = this.pivotCol ? Array.from(new Set(rows.map(r => String(r[this.pivotCol] ?? '—')))).sort() : [null];
    const cellRows = rowVals.map(rv => {
      const rd = { _label: rv };
      colVals.forEach(cv => {
        const k = cv == null ? '__all' : cv;
        rd[k] = rows.filter(r => String(r[this.pivotRow] ?? '—') === rv && (cv == null || String(r[this.pivotCol] ?? '—') === cv));
      });
      return rd;
    });
    const headers = this.pivotCol
      ? ['<th>' + DG.esc(rowCol.label) + '</th>'].concat(
          colVals.map(cv => `<th style="text-align:right">${DG.esc(cv)}</th>`),
          ['<th style="text-align:right">Total</th>']).join('')
      : `<th>${DG.esc(rowCol.label)}</th><th style="text-align:right">${this.pivotAgg.toUpperCase()} of ${DG.esc(valCol.label)}</th>`;
    const body = cellRows.map(cr => {
      const cells = [`<td style="font-family:var(--mono)"><strong>${DG.esc(cr._label)}</strong></td>`];
      let rowTotal = 0;
      colVals.forEach(cv => {
        const k = cv == null ? '__all' : cv;
        const agg = this.aggregate(cr[k], this.pivotVal, this.pivotAgg);
        if (agg != null) rowTotal += agg;
        cells.push(`<td style="text-align:right;font-family:var(--mono)">${DG.fmtNum(agg)}</td>`);
      });
      if (this.pivotCol) cells.push(`<td style="text-align:right;font-family:var(--mono);font-weight:500;color:var(--accent)">${DG.fmtNum(rowTotal)}</td>`);
      return '<tr>' + cells.join('') + '</tr>';
    }).join('');
    return `<table class="dg-table"><thead><tr>${headers}</tr></thead><tbody>${body}</tbody></table>`;
  }
  bindEvents() {
    const root = this.mount;
    root.querySelectorAll('[data-act]').forEach(el => {
      const act = el.getAttribute('data-act');
      if (el.tagName === 'SELECT' || el.tagName === 'INPUT') {
        el.addEventListener('change', e => this.handleAction(act, e.target));
        if (el.tagName === 'INPUT') el.addEventListener('input', e => this.handleAction(act, e.target));
      } else {
        el.addEventListener('click', e => this.handleAction(act, el, e));
      }
    });
    root.querySelectorAll('th.sortable').forEach(th => {
      th.addEventListener('click', e => {
        if (e.target.getAttribute && e.target.getAttribute('data-act') === 'del-col') return;
        const k = th.getAttribute('data-sort'); if (!k) return;
        if (this.sort.key === k) this.sort.dir = this.sort.dir === 'asc' ? 'desc' : 'asc';
        else { this.sort.key = k; this.sort.dir = 'asc'; }
        this.render(); this.notifyChange();
      });
    });
  }
  handleAction(act, el) {
    switch (act) {
      case 'filter':       this.filter = el.value; this.render(); return; // don't mark dirty
      case 'group':        this.groupBy = el.value || null; break;
      case 'clear-group':  this.groupBy = null; break;
      case 'clear-sort':   this.sort = { key: null, dir: 'asc' }; break;
      case 'totals':       this.showTotals = !this.showTotals; break;
      case 'pivot-toggle': this.pivotOpen = !this.pivotOpen; break;
      case 'pivot-row':    this.pivotRow = el.value || null; break;
      case 'pivot-col':    this.pivotCol = el.value || null; break;
      case 'pivot-val':    this.pivotVal = el.value || null; break;
      case 'pivot-agg':    this.pivotAgg = el.value || 'sum'; break;
      case 'add-col':      this.openAddColumnModal(); return;
      case 'del-col':      this.removeCalcColumn(el.getAttribute('data-key')); break;
      case 'reset':        this.resetSelf(); break;
    }
    this.render(); this.notifyChange();
  }
  removeCalcColumn(key) {
    this.columns = this.columns.filter(c => c.key !== key);
    if (this.sort.key === key) this.sort = { key: null, dir: 'asc' };
  }
  resetSelf() {
    this.filter = ''; this.sort = { key: null, dir: 'asc' }; this.groupBy = null;
    this.showTotals = false; this.pivotOpen = false; this.pivotRow = null;
    this.pivotCol = null; this.pivotVal = null; this.pivotAgg = 'sum';
    this.columns = this.columns.filter(c => c.type !== 'calc');
  }
  openAddColumnModal() {
    const numericCols = this.columns.filter(c => c.type === 'number' || c.type === 'calc');
    const help = numericCols.map(c => '<code>' + DG.esc(c.key) + '</code>').join(', ');
    const wrap = document.createElement('div');
    wrap.innerHTML = `
      <div class="dg-modal-backdrop" data-act="modal-bg">
        <div class="dg-modal">
          <div class="dg-modal-head">
            <h3>Add calculated column</h3>
            <button class="dg-modal-close" data-act="modal-close">×</button>
          </div>
          <div class="dg-modal-body">
            <div><label>Column name</label><input class="dg-input" id="dgm-name" style="width:100%" placeholder="e.g. Success rate"></div>
            <div><label>Formula</label><input class="dg-input" id="dgm-formula" style="width:100%;font-family:var(--mono)" placeholder="e.g. inserted / sourceRows * 100"></div>
            <div class="help">Numeric fields: ${help || '(none)'}.<br>Operators: <code>+</code> <code>-</code> <code>*</code> <code>/</code> <code>(</code> <code>)</code>. Math: <code>Math.round()</code>, <code>Math.abs()</code>, etc.</div>
            <div class="err" id="dgm-err"></div>
          </div>
          <div class="dg-modal-foot">
            <button class="dg-btn" data-act="modal-close">Cancel</button>
            <button class="dg-btn dg-btn-add" data-act="modal-add">Add column</button>
          </div>
        </div>
      </div>`;
    const node = wrap.firstElementChild;
    document.body.appendChild(node);
    const close = () => node.remove();
    node.addEventListener('click', e => {
      const a = e.target.getAttribute && e.target.getAttribute('data-act');
      if (a === 'modal-bg' && e.target === node) close();
      if (a === 'modal-close') close();
      if (a === 'modal-add') {
        const name = node.querySelector('#dgm-name').value.trim();
        const expr = node.querySelector('#dgm-formula').value.trim();
        const err  = node.querySelector('#dgm-err');
        err.style.display = 'none';
        if (!name || !expr) { err.textContent = 'Both name and formula are required.'; err.style.display = 'block'; return; }
        try { this.evalFormula(expr, this.rows[0] || {}); }
        catch (ex) { err.textContent = ex.message; err.style.display = 'block'; return; }
        const key = '_calc_' + Date.now().toString(36);
        this.columns.push({ key, label: name, type: 'calc', calc: expr, align: 'right', mono: true });
        this.render(); this.notifyChange();
        close();
      }
    });
    setTimeout(() => node.querySelector('#dgm-name').focus(), 50);
  }
}

// ── Panel UI controllers ──────────────────────────────────────────────────────
function togglePresPanel() {
  const p = document.getElementById('pres-panel');
  if (!p) return;
  p.setAttribute('data-open', p.getAttribute('data-open') === 'true' ? 'false' : 'true');
}

function presToggleSubCard(which) {
  const card = document.getElementById('pres-card-' + which);
  if (!card) return;
  card.setAttribute('data-open', card.getAttribute('data-open') === 'true' ? 'false' : 'true');
}

function presMarkDirty() {
  window._presDirty = true;
  const panel = document.getElementById('pres-panel');
  if (panel) panel.setAttribute('data-dirty', 'true');
}

function presIsDirty() {
  return !!window._presDirty;
}

// Build the current config from panel + grids
function presBuildConfig() {
  const g = window._presGrids;
  return {
    showCharts: document.getElementById('pres-opt-charts').checked,
    grids: {
      results: g.results ? g.results.serialize() : {},
      recon:   g.recon   ? g.recon.serialize()   : {},
      mapping: g.mapping ? g.mapping.serialize() : {},
    },
    updatedAt: new Date().toISOString(),
  };
}

// Per-grid summary (shown in each sub-card head)
function presSummaryOf(gridCfg) {
  if (!gridCfg) return 'Not configured';
  const bits = [];
  if (gridCfg.sort)     bits.push('Sort: ' + gridCfg.sort.key + ' ' + gridCfg.sort.dir);
  if (gridCfg.groupBy)  bits.push('Group: ' + gridCfg.groupBy);
  if (gridCfg.showTotals) bits.push('Σ totals');
  if (gridCfg.pivotRow && gridCfg.pivotVal) bits.push('Pivot: ' + gridCfg.pivotRow + ' × ' + gridCfg.pivotVal);
  const calcCount = (gridCfg.calcColumns || []).length;
  if (calcCount) bits.push(calcCount + ' calc col' + (calcCount === 1 ? '' : 's'));
  return bits.length ? bits.join(' · ') : 'Not configured';
}

function presRefreshSummaries() {
  const cfg = presBuildConfig();
  ['results','recon','mapping'].forEach(k => {
    const el = document.getElementById('pres-summary-' + k);
    if (el) el.textContent = presSummaryOf(cfg.grids[k]);
  });
}

// ── Mount grids with the report's data + existing saved config ────────────────
function presMountGrids(report) {
  window._presCurrentReport = report;
  window._presDirty = false;
  const panel = document.getElementById('pres-panel');
  if (!panel) return;
  panel.style.display = 'block';
  panel.setAttribute('data-dirty', 'false');
  panel.setAttribute('data-open', 'false');

  const savedCfg = (report && report.presentationConfig) || {};
  // Charts toggle
  const chartsOn = savedCfg.showCharts !== false; // default true
  document.getElementById('pres-opt-charts').checked = chartsOn;

  // Build row sets from report data (matches report.html's renderer shape)
  const srcFallback = report.sourceTable || report.sourceFile || '—';
  const tgtTableName = report.targetTable || (report.tables?.[0]?.name) || '—';

  const resultsRows = (report.tables || []).map(t => ({
    source:     t.sourceName || srcFallback,
    table:      t.name,
    sourceRows: t.sourceRows || 0,
    inserted:   t.insertedRows || 0,
    errors:     t.errors || 0,
    batches:    t.batches || 0,
    status:     t.status || (t.errors > 0 ? 'partial' : 'success'),
  }));

  const reconRows = (report.reconciliation || []).map(e => ({
    step: e.stepName || '', srcTable: e.srcTable || '', tgtTable: e.tgtTable || '',
    metric: e.metric || '', column: e.sourceColumn || e.targetColumn || '',
    groupBy: e.groupByColumn || '',
    groupValue: e.groupByColumn ? (e.groupKey == null ? '' : String(e.groupKey)) : '',
    sourceValue: e.sourceValue != null ? Number(e.sourceValue) : null,
    targetValue: e.targetValue != null ? Number(e.targetValue) : null,
    delta: e.delta != null ? Number(e.delta) : null,
    tolerance: e.tolerance != null ? Number(e.tolerance) : null,
    result: e.passed === true ? 'Pass' : e.passed === false ? 'Warn' : 'Skipped',
  }));

  const mappingRows = (report.columnMapping || []).map(m => ({
    srcTable: m.srcTable || srcFallback, srcCol: m.srcCol || '',
    tgtTable: m.tgtTable || tgtTableName, tgtCol: m.tgtCol || '',
    tgtType: m.tgtType || '', transform: m.transform || 'NONE',
    mapped: m.tgtCol ? 1 : 0,
  }));

  const rCfg = (savedCfg.grids && savedCfg.grids.results) || {};
  const xCfg = (savedCfg.grids && savedCfg.grids.recon)   || {};
  const mCfg = (savedCfg.grids && savedCfg.grids.mapping) || {};

  window._presGrids.results = new DG({
    mount: document.getElementById('pres-grid-results'), title: 'migration_results',
    columns: [
      { key: 'source',     label: 'Source',      type: 'text',   mono: true, size: 10, muted: true },
      { key: 'table',      label: 'Table',       type: 'text',   mono: true, bold: true },
      { key: 'sourceRows', label: 'Source rows', type: 'number', align: 'right' },
      { key: 'inserted',   label: 'Inserted',    type: 'number', align: 'right', color: 'green', bold: true },
      { key: 'errors',     label: 'Errors',      type: 'number', align: 'right',
        dynamicColor: v => v > 0 ? '#f04646' : 'var(--text3)' },
      { key: 'batches',    label: 'Batches',     type: 'number', align: 'right' },
      { key: 'status',     label: 'Status',      type: 'status' },
    ],
    rows: resultsRows, emptyMsg: 'No migration results in this report.',
    initialSort: rCfg.sort, initialGroupBy: rCfg.groupBy, initialShowTotals: rCfg.showTotals,
    initialPivotRow: rCfg.pivotRow, initialPivotCol: rCfg.pivotCol, initialPivotVal: rCfg.pivotVal, initialPivotAgg: rCfg.pivotAgg,
    calcColumns: rCfg.calcColumns,
    onChange: () => { presMarkDirty(); presRefreshSummaries(); },
  });

  window._presGrids.recon = new DG({
    mount: document.getElementById('pres-grid-recon'), title: 'reconciliation',
    columns: [
      { key: 'step',        label: 'Step',         type: 'text',   mono: true, size: 10 },
      { key: 'metric',      label: 'Metric',       type: 'text',   mono: true, size: 10 },
      { key: 'column',      label: 'Column',       type: 'text',   mono: true, size: 10 },
      { key: 'groupBy',     label: 'Group by',     type: 'text',   mono: true, size: 10, muted: true, render: v => v || '—' },
      { key: 'groupValue',  label: 'Group value',  type: 'text',   mono: true, size: 10, render: v => v || '—' },
      { key: 'sourceValue', label: 'Source',       type: 'number', align: 'right' },
      { key: 'targetValue', label: 'Target',       type: 'number', align: 'right' },
      { key: 'delta',       label: 'Delta',        type: 'number', align: 'right' },
      { key: 'tolerance',   label: 'Tolerance',    type: 'number', align: 'right', muted: true },
      { key: 'result',      label: 'Result',       type: 'status' },
    ],
    rows: reconRows, emptyMsg: 'No reconciliation entries in this report.',
    initialSort: xCfg.sort, initialGroupBy: xCfg.groupBy, initialShowTotals: xCfg.showTotals,
    initialPivotRow: xCfg.pivotRow, initialPivotCol: xCfg.pivotCol, initialPivotVal: xCfg.pivotVal, initialPivotAgg: xCfg.pivotAgg,
    calcColumns: xCfg.calcColumns,
    onChange: () => { presMarkDirty(); presRefreshSummaries(); },
  });

  window._presGrids.mapping = new DG({
    mount: document.getElementById('pres-grid-mapping'), title: 'column_mapping',
    columns: [
      { key: 'srcTable',  label: 'Source table',  type: 'text', mono: true, size: 10, muted: true },
      { key: 'srcCol',    label: 'Source column', type: 'text', mono: true },
      { key: 'tgtTable',  label: 'Target table',  type: 'text', mono: true, size: 10, muted: true },
      { key: 'tgtCol',    label: 'Target column', type: 'text', mono: true, color: 'accent',
        render: v => v || '<em style="opacity:0.4">skipped</em>' },
      { key: 'tgtType',   label: 'Type',          type: 'text', mono: true, size: 10, muted: true },
      { key: 'transform', label: 'Transform',     type: 'text', mono: true, size: 10 },
      { key: 'mapped',    label: 'Mapped',        type: 'number', align: 'right', hidden: true },
    ],
    rows: mappingRows, emptyMsg: 'No column mapping data in this report.',
    initialSort: mCfg.sort, initialGroupBy: mCfg.groupBy, initialShowTotals: mCfg.showTotals,
    calcColumns: mCfg.calcColumns,
    onChange: () => { presMarkDirty(); presRefreshSummaries(); },
  });

  // Snapshot initial config so we can detect real diffs (optional dirty refinement)
  window._presInitialSnapshot = JSON.stringify(presBuildConfig());
  presRefreshSummaries();
}

// ── Reset and save ────────────────────────────────────────────────────────────
function presResetConfig() {
  if (!confirm('Reset all presentation settings for this report?')) return;
  Object.values(window._presGrids).forEach(g => { if (g) { g.resetSelf(); g.render(); } });
  document.getElementById('pres-opt-charts').checked = true;
  presMarkDirty();
  presRefreshSummaries();
}

async function presSaveConfig() {
  const btn = document.getElementById('pres-save-btn');
  if (!btn) return;
  const report = window._presCurrentReport;
  if (!report || !report.id) { alert('No report loaded.'); return; }
  const email = (typeof currentCygenixEmail === 'function') ? currentCygenixEmail() : null;
  if (!email) { alert('Please sign in first.'); return; }

  const prev = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Saving…';
  try {
    const cfg = presBuildConfig();
    const r = await cygenixFetch('/.netlify/functions/reports', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        action: 'save-presentation',
        id: report.id,
        presentationConfig: cfg,
      }),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || ('HTTP ' + r.status));
    // Update the in-memory report so a subsequent open reflects the new config
    // without a round-trip, and so the stashed copy used by report.html is current.
    report.presentationConfig = cfg;
    window._presDirty = false;
    const panel = document.getElementById('pres-panel');
    if (panel) panel.setAttribute('data-dirty', 'false');
    btn.textContent = '✓ Saved';
    setTimeout(() => { btn.textContent = prev; btn.disabled = false; }, 1500);
  } catch (e) {
    alert('Save failed: ' + e.message);
    btn.textContent = prev;
    btn.disabled = false;
  }
}


/* ═══ block 4 — extracted from an inline <script> in dashboard.html ═══ */

// ── Project Settings ──────────────────────────────────────────────────────────
const PS_KEY = 'cygenix_project_settings';
const APP_PREFS_KEY = 'cygenix_app_prefs';

// Default app-level preferences. Used to seed the form on first load and
// fallback for any missing fields when reading.
const DEFAULT_APP_PREFS = {
  theme:         'dark',
  landing:       'dashboard',
  reportFormat:  'docx',
  autoSnapshot:  'on',
};

// initProjectSettingsView — runs when the user navigates to the Settings
// view. Project metadata used to live here (name, client, dates, etc.) but
// has moved to /projects.html. This view now holds app-level config only:
// API key, credit gauge link, and general preferences.
function initProjectSettingsView() {
  // Populate API key field with the saved key (if any). The legacy field id
  // ps-api-key was on the old form; we now use settings-api-key but keep
  // back-compat by reading from the same storage key.
  const key = localStorage.getItem('cygenix_api_key') || sessionStorage.getItem('cygenix_api_key') || '';
  const keyInput = document.getElementById('settings-api-key');
  const status   = document.getElementById('settings-key-status');
  if (keyInput) {
    if (key) {
      keyInput.value = key;
      keyInput.placeholder = key.slice(0,12) + '…' + key.slice(-4);
    } else {
      keyInput.value = '';
      keyInput.placeholder = 'sk-ant-api03-...';
    }
  }
  if (status) {
    status.textContent = key
      ? '✓ Key saved (' + key.slice(0,12) + '…' + key.slice(-4) + ')'
      : 'No key set';
    status.style.color = key ? 'var(--green)' : 'var(--text3)';
  }

  // Populate preference selectors from saved prefs (or defaults)
  let prefs = {};
  try { prefs = JSON.parse(localStorage.getItem(APP_PREFS_KEY) || '{}'); } catch {}
  prefs = { ...DEFAULT_APP_PREFS, ...prefs };
  const set = (id, val) => { const el = document.getElementById(id); if (el) el.value = val; };
  set('pref-theme',         prefs.theme);
  set('pref-landing',       prefs.landing);
  set('pref-report-format', prefs.reportFormat);
  set('pref-auto-snapshot', prefs.autoSnapshot);
}

// saveProjectSettings is kept as a named export of sorts because other code
// paths (the topbar Save button, deep links) used to call it. It now just
// proxies to saveAppPreferences so any leftover wiring still works without
// silently failing.
function saveProjectSettings() { return saveAppPreferences(); }

function saveAppPreferences() {
  const get = id => document.getElementById(id)?.value;
  const prefs = {
    theme:         get('pref-theme')         || DEFAULT_APP_PREFS.theme,
    landing:       get('pref-landing')       || DEFAULT_APP_PREFS.landing,
    reportFormat:  get('pref-report-format') || DEFAULT_APP_PREFS.reportFormat,
    autoSnapshot:  get('pref-auto-snapshot') || DEFAULT_APP_PREFS.autoSnapshot,
  };
  try { localStorage.setItem(APP_PREFS_KEY, JSON.stringify(prefs)); } catch {}
  // Re-apply the theme on save. The dropdown's onchange already does live
  // preview, so this mostly matters when the user edited the value but never
  // triggered onchange (e.g. browser autofill) — and as a safety net.
  applyThemePreview(prefs.theme);
  const msg = document.getElementById('pref-saved-msg');
  if (msg) { msg.style.display = 'block'; setTimeout(() => msg.style.display = 'none', 2400); }
}

// applyThemePreview — flips the data-theme attribute on <html> immediately
// when the user picks a value in the Theme dropdown, so they see the change
// without needing to save first. The pre-paint script in <head> handles the
// equivalent on subsequent page loads. Resolves 'auto' against the OS at the
// moment of the call (we don't subscribe to OS changes — that would be a
// nice-to-have but isn't needed for v1).
function applyThemePreview(theme) {
  let resolved = theme || 'dark';
  if (resolved === 'auto') {
    resolved = (window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches)
      ? 'light' : 'dark';
  }
  document.documentElement.setAttribute('data-theme', resolved);
  // Keep the browser chrome colour (mobile address bar, etc.) in sync.
  // Financial uses the dark FT-style masthead, so its mobile chrome matches that.
  const meta = document.querySelector('meta[name="theme-color"]');
  if (meta) {
    const chrome = resolved === 'light'     ? '#13161d'
                 : resolved === 'financial' ? '#1A1614'
                 : '#0d0f14';
    meta.setAttribute('content', chrome);
  }
}

// API key handlers wired to the new Settings card. Same backing storage as
// the legacy saveApiKeyFromSettings/clearApiKeyFromSettings so any code
// reading `cygenix_api_key` keeps working.
function saveSettingsApiKey() {
  const input  = document.getElementById('settings-api-key');
  const status = document.getElementById('settings-key-status');
  const key = (input?.value || '').trim();
  if (!key) {
    if (status) { status.textContent = 'Enter a key first'; status.style.color = 'var(--amber)'; }
    return;
  }
  if (!key.startsWith('sk-ant-')) {
    if (status) { status.textContent = 'Invalid key — must start with sk-ant-'; status.style.color = 'var(--red)'; }
    return;
  }
  // Save to both stores so all pages pick it up
  localStorage.setItem('cygenix_api_key', key);
  sessionStorage.setItem('cygenix_api_key', key);
  const topbarInput = document.getElementById('api-key-input');
  if (topbarInput) topbarInput.value = key;
  setBadge('ok');
  if (input) {
    input.value = key;
    input.placeholder = key.slice(0,12) + '…' + key.slice(-4);
  }
  if (status) {
    status.textContent = '✓ Key saved (' + key.slice(0,12) + '…' + key.slice(-4) + ')';
    status.style.color = 'var(--green)';
  }
}

function clearSettingsApiKey() {
  if (!confirm('Clear the saved API key? AI features will stop working until a new key is entered.')) return;
  localStorage.removeItem('cygenix_api_key');
  sessionStorage.removeItem('cygenix_api_key');
  const input = document.getElementById('settings-api-key');
  if (input) { input.value = ''; input.placeholder = 'sk-ant-api03-...'; }
  const topbarInput = document.getElementById('api-key-input');
  if (topbarInput) topbarInput.value = '';
  setBadge('err');
  const status = document.getElementById('settings-key-status');
  if (status) { status.textContent = 'No key set'; status.style.color = 'var(--text3)'; }
}

function saveApiKeyFromSettings() {
  const input = document.getElementById('ps-api-key');
  const msg   = document.getElementById('ps-api-key-msg');
  const key   = (input?.value || '').trim();

  if (!key) {
    showApiKeyMsg('Enter a key first', 'var(--amber)', 'rgba(245,158,11,0.1)');
    return;
  }
  if (!key.startsWith('sk-ant-')) {
    showApiKeyMsg('Invalid key — must start with sk-ant-', 'var(--red)', 'var(--red-bg)');
    return;
  }

  // Save to both stores so all pages pick it up
  localStorage.setItem('cygenix_api_key', key);
  sessionStorage.setItem('cygenix_api_key', key);
  // Also update the topbar input if it exists
  const topbarInput = document.getElementById('api-key-input');
  if (topbarInput) topbarInput.value = key;
  setBadge('ok');

  // Mask the input after saving
  if (input) {
    input.value = key;
    input.placeholder = key.slice(0,12) + '…' + key.slice(-4) + '  (saved)';
  }
  showApiKeyMsg('✓ API key saved — all AI features will use this key', 'var(--green)', 'var(--green-bg)');
  // Refresh the gauge with the new key
  setTimeout(refreshCreditGauge, 300);
}

function clearApiKeyFromSettings() {
  if (!confirm('Clear the saved API key? AI features will stop working until a new key is entered.')) return;
  localStorage.removeItem('cygenix_api_key');
  sessionStorage.removeItem('cygenix_api_key');
  const input = document.getElementById('ps-api-key');
  if (input) { input.value = ''; input.placeholder = 'sk-ant-api03-…'; }
  const topbarInput = document.getElementById('api-key-input');
  if (topbarInput) topbarInput.value = '';
  setBadge('err');
  showApiKeyMsg('API key cleared', 'var(--red)', 'var(--red-bg)');
  const wrap = document.getElementById('credit-gauge-wrap');
  if (wrap) wrap.innerHTML = '<div style="font-size:12px;color:var(--text3);text-align:center;padding:1rem 0">Enter your API key above and click Refresh to check credit status</div>';
}

function showApiKeyMsg(text, color, bg) {
  const msg = document.getElementById('ps-api-key-msg');
  if (!msg) return;
  msg.textContent = text;
  msg.style.color = color;
  msg.style.background = bg;
  msg.style.border = '0.5px solid ' + color.replace('var(--', 'rgba(').replace(')', ',0.3)');
  msg.style.display = 'block';
  setTimeout(() => { msg.style.display = 'none'; }, 4000);
}

// ── Claude API Credit Gauge ───────────────────────────────────────────────────
async function refreshCreditGauge() {
  const key = (document.getElementById('ps-api-key')?.value || document.getElementById('api-key-input')?.value || localStorage.getItem('cygenix_api_key') || '').trim();
  const wrap = document.getElementById('credit-gauge-wrap');
  const btn  = document.getElementById('credit-refresh-btn');
  if (!wrap) return;

  if (!key) {
    wrap.innerHTML = '<div style="font-size:12px;color:var(--amber);padding:0.5rem 0"><i class="ic ic-warning"></i> Enter your Anthropic API key above first</div>';
    return;
  }

  if (btn) { btn.textContent = '…'; btn.disabled = true; }
  wrap.innerHTML = '<div style="font-size:12px;color:var(--text3);padding:0.5rem 0">Checking credit balance…</div>';

  try {
    // Use the usage API to get credit balance
    const res = await fetch('https://api.anthropic.com/v1/organizations/usage', {
      method: 'GET',
      headers: {
        'x-api-key': key,
        'anthropic-version': '2023-06-01',
        'anthropic-dangerous-direct-browser-access': 'true'
      }
    });

    // Anthropic doesn't expose a simple "balance" endpoint publicly,
    // so we use a lightweight test message and read back rate-limit headers
    // which include usage data. Fall back to a test call approach.
    if (!res.ok) throw new Error('Usage API not available');
    const data = await res.json();
    renderCreditGauge(wrap, data);

  } catch(e) {
    // Fallback: make a minimal API call and read the credit balance from response headers
    try {
      const testRes = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': key,
          'anthropic-version': '2023-06-01',
          'anthropic-dangerous-direct-browser-access': 'true'
        },
        body: JSON.stringify({
          model: CygenixModel.primary(),
          max_tokens: 1,
          messages: [{ role: 'user', content: 'hi' }]
        })
      });

      const data = await testRes.json();

      if (!testRes.ok) {
        // Credit exhausted or auth error
        if (testRes.status === 400 && data.error?.message?.includes('credit')) {
          renderCreditGauge(wrap, null, 'empty');
        } else if (testRes.status === 401) {
          wrap.innerHTML = '<div style="font-size:12px;color:var(--red);padding:0.5rem 0"><i class="ic-dot" style="color:var(--red)"></i> Invalid API key — check your key in the topbar</div>';
        } else {
          wrap.innerHTML = '<div style="font-size:12px;color:var(--red);padding:0.5rem 0">Error: ' + (data.error?.message || testRes.statusText) + '</div>';
        }
      } else {
        // Success — credits available, read usage from response
        renderCreditGauge(wrap, data, 'ok');
      }
    } catch(e2) {
      wrap.innerHTML = '<div style="font-size:12px;color:var(--red);padding:0.5rem 0">Could not check: ' + e2.message + '</div>';
    }
  }

  if (btn) { btn.textContent = '↻ Refresh'; btn.disabled = false; }
}

function renderCreditGauge(wrap, data, status) {
  // We can't get exact balance from the API directly in browser,
  // but we can show status: ok (credits available) or empty
  const isOk = status === 'ok' || (data && !status);

  // Read input_tokens from response if available to show recent usage
  const inputTokens  = data?.usage?.input_tokens  || 0;
  const outputTokens = data?.usage?.output_tokens || 0;

  // Build the gauge display
  const statusColour = isOk ? 'var(--green)'  : 'var(--red)';
  const statusBg     = isOk ? 'var(--green-bg)' : 'var(--red-bg)';
  const statusBorder = isOk ? 'rgba(34,201,122,0.3)' : 'rgba(240,70,70,0.25)';
  const statusIcon   = isOk ? '' : '';
  const statusText   = isOk ? 'Credits available — AI features are working' : 'Credits exhausted — please top up at console.anthropic.com';

  wrap.innerHTML = `
    <div style="background:${statusBg};border:0.5px solid ${statusBorder};border-radius:var(--r);padding:0.875rem 1rem;display:flex;flex-direction:column;gap:0.75rem">

      <!-- Status row -->
      <div style="display:flex;align-items:center;gap:0.75rem">
        <span style="font-size:28px;line-height:1">${statusIcon}</span>
        <div>
          <div style="font-size:13px;font-weight:600;color:${statusColour}">${isOk ? 'Credits Available' : 'Credits Exhausted'}</div>
          <div style="font-size:11px;color:var(--text3);margin-top:2px">${statusText}</div>
        </div>
        <div style="margin-left:auto">
          <div style="width:12px;height:12px;border-radius:50%;background:${statusColour};box-shadow:0 0 8px ${statusColour}"></div>
        </div>
      </div>

      <!-- Fuel gauge bar -->
      <div>
        <div style="display:flex;justify-content:space-between;font-size:10px;color:var(--text3);margin-bottom:4px">
          <span>Empty</span>
          <span>Credit status</span>
          <span>Full</span>
        </div>
        <div style="background:var(--bg3);border-radius:100px;height:16px;overflow:hidden;border:0.5px solid var(--border2);position:relative">
          <!-- Gauge segments -->
          <div style="position:absolute;inset:0;display:flex;gap:2px;padding:2px">
            ${[1,2,3,4,5,6,7,8,9,10].map(n => `
              <div style="flex:1;background:${isOk ? (n<=7 ? 'var(--green)' : 'rgba(34,201,122,0.3)') : 'var(--bg4)'};border-radius:100px;transition:background 0.3s"></div>
            `).join('')}
          </div>
          <!-- Needle marker -->
          <div style="position:absolute;top:0;bottom:0;left:${isOk ? '68%' : '3%'};width:3px;background:white;border-radius:2px;transition:left 0.5s;box-shadow:0 0 4px rgba(0,0,0,0.5)"></div>
        </div>
        <div style="display:flex;justify-content:space-between;font-size:10px;color:var(--text3);margin-top:4px">
          <span><i class="ic-dot" style="color:var(--red)"></i> Top up needed</span>
          <span>${isOk ? '<i class="ic-dot" style="color:var(--green)"></i> Good to go' : '<i class="ic ic-warning"></i> Please top up'}</span>
        </div>
      </div>

      ${isOk ? '' : `
      <a href="https://console.anthropic.com/settings/billing" target="_blank"
        style="display:inline-flex;align-items:center;gap:0.4rem;background:var(--accent);color:white;border:none;border-radius:var(--r);padding:6px 14px;font-size:12px;cursor:pointer;text-decoration:none;font-family:var(--serif);width:fit-content">
        Top up at console.anthropic.com →
      </a>`}

      <div style="font-size:10px;color:var(--text3);font-family:var(--mono)">
        Last checked: ${new Date().toLocaleTimeString('en-GB')} · 
        API key: ${key_preview()} ·
        <a href="https://console.anthropic.com/settings/billing" target="_blank" style="color:var(--accent);text-decoration:none">View billing →</a>
      </div>
    </div>`;
}

function key_preview() {
  const k = (document.getElementById('api-key-input')?.value || localStorage.getItem('cygenix_api_key') || '');
  return k.length > 8 ? k.slice(0,8) + '…' + k.slice(-4) : '(not set)';
}

// LEGACY saveProjectSettings DELETED — the new proxy version is defined
// earlier (alongside saveAppPreferences). The old version captured project
// metadata fields (name, client, analyst, etc.) from a form that no longer
// exists on this page (those moved to /projects.html), so calling it would
// silently no-op anyway. Removed to avoid the JS hoisting double-define
// where the second definition wins and shadows the proxy.

// ── Project Connections ───────────────────────────────────────────────────────
let srcMode = 'direct', tgtMode = 'azure';

// Clean up orphaned staging sessionStorage keys from before Staging was removed.
// Harmless for users who never had staging set, but tidies returning users.
try {
  ['cygenix_staging_conn_mode','cygenix_staging_conn_string',
   'cygenix_staging_fn_url','cygenix_staging_fn_key'
  ].forEach(k => sessionStorage.removeItem(k));
} catch {}

// ── Connection masking ────────────────────────────────────────────────────
// A connection string carries the password in cleartext
// (mssql://user:PASSWORD@host:1433/db), and this view used to render it in a
// plain text input the moment the page opened. Anyone glancing at the screen,
// any screen share, and any screenshot of the Connections tab leaked the
// database password — a stored credential exposed by simply navigating.
//
// So a *configured* connection is summarised instead of printed: the database
// name and, when it matches one the user saved, its nickname. The real fields
// only appear while the user is deliberately editing them, and they re-hide
// on save and whenever the view is re-opened.
//
// Nothing about storage or transport changes — this is purely what reaches
// the screen. An unconfigured side shows its fields as before, because there
// is nothing to hide and a first-time user has to be able to type.
const connRevealed = { src: false, tgt: false };

// The nickname of the saved connection matching what's currently in the
// fields, or '' when the live values don't correspond to a saved entry.
function connNicknameFor(side){
  let all = [];
  try { all = (typeof sconnGetAll === 'function' ? sconnGetAll() : []) || []; } catch { return ''; }
  const mode = side === 'src' ? srcMode : tgtMode;
  const val  = (document.getElementById(
    'proj-' + side + (mode === 'direct' ? '-cs' : '-fn-url'))?.value || '').trim();
  if (!val) return '';
  const hit = all.find(s => s && s.side === side && s.mode === mode &&
    ((mode === 'direct' ? s.connString : s.fnUrl) || '').trim() === val);
  return (hit && hit.name) || '';
}

// What the masked card shows. Returns null when this side has nothing
// configured, which is what keeps the fields visible for a new user.
function connSummaryFor(side){
  const mode = side === 'src' ? srcMode : tgtMode;
  const val  = (document.getElementById(
    'proj-' + side + (mode === 'direct' ? '-cs' : '-fn-url'))?.value || '').trim();
  if (!val) return null;
  const nickname = connNicknameFor(side);
  // An Azure Function URL doesn't name a database — the function decides
  // that server-side — so there is no database name to show. The host is
  // deliberately withheld: it identifies the backend endpoint, and the
  // nickname is the identifier the user chose for it.
  if (mode === 'azure') return { label: 'Connection', value: 'Azure Function', nickname };
  const desc = parseDbConnection(val);
  const db   = (desc && desc.database) || '';
  return { label: 'Database', value: db, nickname };
}

// Single place that decides whether a side shows its card or its fields.
// setSrcMode/setTgtMode delegate here rather than toggling the wraps
// themselves, so a mode switch can't leave a connection string on screen.
function renderConnLock(side){
  const card = document.getElementById(side + '-conn-locked');
  const mode = side === 'src' ? srcMode : tgtMode;
  const summary = connSummaryFor(side);
  const locked  = !!summary && !connRevealed[side];

  const show = (id, on, disp) => {
    const el = document.getElementById(id);
    if (el) el.style.display = on ? (disp || 'block') : 'none';
  };
  show(side + '-conn-locked', locked, 'flex');
  show(side + '-direct-wrap', !locked && mode === 'direct');
  show(side + '-azure-wrap',  !locked && mode === 'azure');
  // The re-hide control only makes sense once there's something to hide.
  show(side + '-conn-hide', !locked && !!summary, 'inline-flex');

  if (!locked || !card) return;
  const put = (id, text) => { const el = document.getElementById(id); if (el) el.textContent = text; };
  put(side + '-conn-locked-label', summary.label);
  const valEl = document.getElementById(side + '-conn-locked-value');
  if (valEl){
    // A direct connection string without a database segment is still a
    // configured connection — say so rather than showing a blank row.
    valEl.textContent = summary.value || 'Configured';
    valEl.classList.toggle('none', !summary.value);
  }
  const nickEl = document.getElementById(side + '-conn-locked-nick');
  if (nickEl){
    nickEl.textContent = summary.nickname;
    nickEl.style.display = summary.nickname ? 'inline-block' : 'none';
  }
}

// Show the real fields so they can be edited, and put the cursor in the one
// that matters. Called by the Edit button and by the mode toggles — picking
// a different mode is itself an act of configuring.
function connReveal(side){
  connRevealed[side] = true;
  renderConnLock(side);
  const mode = side === 'src' ? srcMode : tgtMode;
  const el = document.getElementById('proj-' + side + (mode === 'direct' ? '-cs' : '-fn-url'));
  if (el) try { el.focus(); } catch {}
}

// Put a revealed connection back behind the summary.
function connHide(side){
  connRevealed[side] = false;
  renderConnLock(side);
}

// Reachable from the inline onclick handlers in the tab markup regardless of
// which <script> block ran first — same pattern as the connio/sconn exports.
window.connReveal = connReveal;
window.connHide   = connHide;

function setSrcMode(mode) {
  srcMode = mode;
  const styles = (id, active) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.style.background = active ? 'var(--accent-glow)' : 'transparent';
    el.style.color = active ? 'var(--accent)' : 'var(--text2)';
    el.style.border = active ? '0.5px solid rgba(74,91,214,0.3)' : '0.5px solid var(--border2)';
  };
  styles('src-mode-direct', mode==='direct');
  styles('src-mode-azure',  mode==='azure');
  renderConnLock('src');
}
function setTgtMode(mode) {
  tgtMode = mode;
  const styles = (id, active) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.style.background = active ? 'var(--accent-glow)' : 'transparent';
    el.style.color = active ? 'var(--accent)' : 'var(--text2)';
    el.style.border = active ? '0.5px solid rgba(74,91,214,0.3)' : '0.5px solid var(--border2)';
  };
  styles('tgt-mode-direct', mode==='direct');
  styles('tgt-mode-azure',  mode==='azure');
  renderConnLock('tgt');
}

function initConnectionsView() {
  const c = CygenixConnections.get();
  srcMode = c.srcConnMode || 'direct';
  tgtMode = c.tgtConnMode || 'azure';
  setSrcMode(srcMode);
  setTgtMode(tgtMode);
  const set = (id, val) => { const el=document.getElementById(id); if(el) el.value=val||''; };
  set('proj-src-cs',     c.srcConnString);
  set('proj-src-fn-url', c.srcFnUrl);
  set('proj-src-fn-key', c.srcFnKey);
  set('proj-tgt-cs',     c.tgtConnString);
  set('proj-tgt-fn-url', c.tgtFnUrl);
  set('proj-tgt-fn-key', c.tgtFnKey);
  updateConnDots();
  // Render saved-connection chips for both sides
  sconnRender('src');
  sconnRender('tgt');
  // Re-mask on every visit. Revealing is a per-visit decision, so leaving
  // the view and coming back never leaves a password on screen — and the
  // fields have only just been populated, so the summaries are computed
  // from the real values rather than the empty inputs setSrcMode saw.
  connRevealed.src = connRevealed.tgt = false;
  renderConnLock('src');
  renderConnLock('tgt');
}

function updateConnDots() {
  // CygenixConnections.get() now reads from the per-user slice of the
  // localStorage blob at cygenix_project_connections, which persists
  // across reloads and syncs to Cosmos. No fallback chain needed.
  const c = CygenixConnections.get();
  const srcOk = !!(c.srcConnString || c.srcFnUrl);
  const tgtOk = !!(c.tgtFnUrl || c.tgtConnString);
  const setDot = (id, ok) => { const el=document.getElementById(id); if(el) el.style.background=ok?'var(--green)':'var(--red)'; };
  setDot('src-conn-dot', srcOk);
  setDot('tgt-conn-dot', tgtOk);
  const navDot = document.getElementById('conn-status-dot');
  if (navDot) navDot.style.background = srcOk&&tgtOk ? 'var(--green)' : (srcOk||tgtOk) ? 'var(--amber)' : 'var(--red)';
}

async function testProjConn(which) {
  const resultEl = document.getElementById(which+'-conn-result');
  const dotEl    = document.getElementById(which+'-conn-dot');
  resultEl.textContent='Connecting…'; resultEl.style.color='var(--accent)';
  let conn = '';
  if (which==='src') {
    conn = srcMode==='direct'
      ? (document.getElementById('proj-src-cs')?.value.trim()||'')
      : (document.getElementById('proj-src-fn-url')?.value.trim()||'');
    const key = document.getElementById('proj-src-fn-key')?.value.trim()||'';
    if (srcMode==='azure'&&key) conn += (conn.includes('?')?'&':'?')+'code='+encodeURIComponent(key);
  } else {
    conn = tgtMode==='direct'
      ? (document.getElementById('proj-tgt-cs')?.value.trim()||'')
      : (document.getElementById('proj-tgt-fn-url')?.value.trim()||'');
    const key = document.getElementById('proj-tgt-fn-key')?.value.trim()||'';
    if (tgtMode==='azure'&&key) conn += (conn.includes('?')?'&':'?')+'code='+encodeURIComponent(key);
  }
  if (!conn) { resultEl.textContent='Enter a connection string first.'; resultEl.style.color='var(--red)'; return; }

  // Helper — performs the actual fetch and returns either the parsed body
  // or throws. Wrapped so we can retry once on cold-start / auto-pause
  // failures without duplicating the request logic.
  const attempt = async () => {
    const isFn = conn.startsWith('https://')||conn.startsWith('http://');
    const res  = isFn
      ? await fetch(conn,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'test'}),signal:AbortSignal.timeout(30000)})
      : await fetch('/.netlify/functions/db-connect',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'test',connectionString:conn}),signal:AbortSignal.timeout(30000)});
    const data = await res.json().catch(()=>({error:'Non-JSON'}));
    if (!res.ok) throw new Error(data.error||res.statusText);
    return data;
  };

  // Determine whether an error is worth retrying. Cold-starts on the Azure
  // Function (503/timeout) and Azure SQL Free-tier auto-pause (40613/40197)
  // both clear themselves on a second attempt a couple of seconds later.
  // Auth errors (401/403) and bad-input errors are permanent — no retry.
  const isTransient = (msg) => {
    const m = String(msg || '').toLowerCase();
    if (m.includes('401') || m.includes('unauthor')) return false;
    if (m.includes('403') || m.includes('forbidden')) return false;
    if (m.includes('bad request') || m.includes('400')) return false;
    return /timeout|503|502|504|network|aborted|40613|40197|not currently available|resuming/.test(m);
  };

  try {
    let data;
    try {
      data = await attempt();
    } catch (e) {
      if (!isTransient(e.message)) throw e;
      // Cold start / auto-pause — wait and retry once
      resultEl.textContent='Warming up the database…';
      await new Promise(r => setTimeout(r, 2500));
      data = await attempt();
    }
    resultEl.textContent='Connected · '+data.database+' · '+(data.user||'').split('@')[0];
    resultEl.style.color='var(--green)';
    if (dotEl) dotEl.style.background='var(--green)';
    saveProjectConnections();
  } catch(e) {
    resultEl.textContent= e.message;
    resultEl.style.color='var(--red)';
    if (dotEl) dotEl.style.background='var(--red)';
  }
}

function saveProjectConnections() {
  // Read form values based on current UI mode, then write the whole active
  // pair through CygenixConnections.setActive(). Previously this wrote flat
  // sessionStorage keys and called save() to migrate them — that path
  // silently no-oped whenever MSAL hadn't populated cygenix_entra_account
  // (which turned out to be always), so connections weren't persisting.
  // Going through setActive() writes directly to the localStorage blob,
  // which the Cosmos sync picks up and propagates to Azure.
  const srcCs  = document.getElementById('proj-src-cs')?.value.trim()     || '';
  const srcUrl = document.getElementById('proj-src-fn-url')?.value.trim() || '';
  const srcKey = document.getElementById('proj-src-fn-key')?.value.trim() || '';
  const tgtCs  = document.getElementById('proj-tgt-cs')?.value.trim()     || '';
  const tgtUrl = document.getElementById('proj-tgt-fn-url')?.value.trim() || '';
  const tgtKey = document.getElementById('proj-tgt-fn-key')?.value.trim() || '';

  const fields = {
    srcConnString: srcMode === 'direct' ? srcCs : '',
    srcConnMode  : srcMode === 'direct' ? 'direct' : 'azure',
    srcFnUrl     : srcMode === 'azure'  ? srcUrl : '',
    srcFnKey     : srcMode === 'azure'  ? srcKey : '',
    tgtConnString: tgtMode === 'direct' ? tgtCs : '',
    tgtConnMode  : tgtMode === 'direct' ? 'direct' : 'azure',
    tgtFnUrl     : tgtMode === 'azure'  ? tgtUrl : '',
    tgtFnKey     : tgtMode === 'azure'  ? tgtKey : '',
  };

  const ok = (typeof CygenixConnections.setActive === 'function')
    ? CygenixConnections.setActive(fields)
    : (CygenixConnections.save(), true); // fall through to legacy if setActive missing

  if (!ok) {
    // Most likely cause: user signed out, or MSAL cache not ready yet.
    console.warn('[connections] Save refused — not signed in?');
  }

  updateConnDots();
  // Editing is finished, so put both sides back behind their summaries.
  connRevealed.src = connRevealed.tgt = false;
  renderConnLock('src');
  renderConnLock('tgt');
  const ind = document.getElementById('conn-save-indicator');
  if (ind) { ind.style.display = 'block'; setTimeout(() => ind.style.display = 'none', 2500); }
}

function clearProjectConnections() {
  // Barred entirely while an active profile references the saved set — a
  // cleared register would orphan every governed binding at once.
  try {
    const CP = window.CygenixProfiles;
    if (CP){
      const st = CP.cpLoad();
      const active = st.profiles.filter(p => p.status === 'active').length;
      if (active){
        alert('Clear all is barred: ' + active + ' active profile'
          + (active === 1 ? '' : 's') + ' reference the saved connections. Retire the profiles first (Profiles page).');
        return;
      }
    }
  } catch (e) { /* fall through to the confirm */ }
  if (!confirm('Clear all saved connections?')) return;
  CygenixConnections.clear();
  ['proj-src-cs','proj-src-fn-url','proj-src-fn-key','proj-tgt-cs','proj-tgt-fn-url','proj-tgt-fn-key']
    .forEach(id=>{const el=document.getElementById(id);if(el) el.value='';});
  ['src-conn-result','tgt-conn-result'].forEach(id=>{const el=document.getElementById(id);if(el) el.textContent='';});
  updateConnDots();
  // Nothing configured now, so the summaries have nothing to summarise and
  // the empty fields come back for re-entry.
  renderConnLock('src');
  renderConnLock('tgt');
}

// ══════════════════════════════════════════════════════════════════════════
// NAMED SAVED CONNECTIONS — page-contained, per-browser, localStorage-backed
// Lets users save the currently-entered source/target connection under a
// friendly name, then click to restore it later. Stored in localStorage
// (same posture as the existing sessionStorage connection cache — it's still
// per-browser, per-user, never leaves the device).
// ══════════════════════════════════════════════════════════════════════════

const SCONN_KEY = 'cygenix_saved_connections';  // legacy — retained only so any external ref still compiles. Real reads/writes now go through CygenixConnections.savedGetAll / savedSetAll which scope by user.

// Read the saved list. Delegates to CygenixConnections so the data is
// per-user-scoped (users can't see each other's saved connections). Returns
// [] if nobody is signed in. Never throws.
//
// As of the secrets-split refactor, the synced blob holds sanitised
// entries (no passwords, no function keys). We rehydrate from the
// local secrets store before returning so every downstream caller —
// the chip renderer, sconnLoad, the Connections form — sees entries
// in their original full shape. On a browser that doesn't have the
// local secrets (e.g. user just signed in on a new machine),
// connString/fnKey come back empty and the user is prompted to
// re-enter the secret when they try to load that saved connection.
function sconnGetAll(){
  try {
    let entries;
    if (CygenixConnections && typeof CygenixConnections.savedGetAll === 'function') {
      entries = CygenixConnections.savedGetAll();
    } else {
      // Fallback only — shouldn't happen if connections.js loaded correctly.
      const raw = localStorage.getItem(SCONN_KEY);
      entries = raw ? JSON.parse(raw) : [];
    }
    if (!Array.isArray(entries)) return [];

    if (window.CygenixSavedConnSecrets && typeof CygenixSavedConnSecrets.rehydrate === 'function'){
      CygenixSavedConnSecrets.rehydrate(entries);
    }
    return entries;
  } catch { return []; }
}

// Write the full list. Caps at 50 entries. Writes per-user via CygenixConnections.
// If nobody is signed in, the write is silently dropped to avoid leaking.
//
// PRIVACY-CRITICAL: This blob ends up in Cosmos via the SYNC_KEYS
// pipeline. The sanitiser below moves any password-bearing fields
// (connString, fnKey) out of each entry and into the local-only
// secrets store BEFORE we hand the blob to the sync layer. Database
// passwords and Azure Function keys must never reach the cloud.
function sconnSetAll(list){
  const capped = list.slice(0, 50);

  // Split each entry into a sanitised half (stays here, gets synced)
  // and a secret half (goes to a local-only store, never synced).
  // If CygenixSavedConnSecrets isn't loaded yet (e.g. someone deferred
  // the script tag), we fall through to the legacy unsanitised path
  // and warn loudly — better to keep working than to silently drop
  // saves, but make sure the issue is visible.
  let toWrite;
  if (window.CygenixSavedConnSecrets && typeof CygenixSavedConnSecrets.split === 'function'){
    toWrite = capped.map(entry => {
      const { sanitised, secrets } = CygenixSavedConnSecrets.split(entry);
      // Persist secrets locally, keyed by sconn id. If the entry has
      // no id (shouldn't happen — sconnSaveAs always assigns one),
      // skip secret storage rather than risk a cross-entry collision.
      if (sanitised && sanitised.id && secrets){
        CygenixSavedConnSecrets.set(sanitised.id, secrets);
      }
      return sanitised;
    });
    // Drop any orphaned secrets whose entries got removed from the list
    // (e.g. user deleted a saved connection). Without this, deleted
    // entries' passwords would linger in localStorage indefinitely.
    CygenixSavedConnSecrets.pruneTo(toWrite.map(e => e && e.id).filter(Boolean));
  } else {
    console.warn('[sconn] CygenixSavedConnSecrets not loaded — secrets will sync to cloud unstripped. Check that /cygenix-saved-conn-secrets.js is included before /cygenix-cosmos-sync.js.');
    toWrite = capped;
  }

  try {
    if (CygenixConnections && typeof CygenixConnections.savedSetAll === 'function') {
      // savedSetAll writes the unnamespaced cygenix_saved_connections blob
      // (per-user keyed inside the JSON), which is the same shape sconnGetAll
      // reads, AND it's the key the Cosmos sync watches. Earlier this function
      // bodged `cygenix_saved_connections::<email>` directly, which broke the
      // round-trip (reads never saw the data) AND the cloud sync (intercept
      // only fires for the unnamespaced key).
      CygenixConnections.savedSetAll(toWrite);
      return;
    }
    // Fallback only — shouldn't happen if connections.js loaded correctly.
    localStorage.setItem(SCONN_KEY, JSON.stringify(toWrite));
  } catch (e) { console.error('[sconn] localStorage write failed:', e); }
}

// One-shot migration: pull existing entries through the secrets-split,
// then save them back. The save goes through the standard SYNC_KEYS
// pipeline, so the next outbound sync replaces the tainted blob in
// Cosmos with the sanitised version.
//
// Crucially this MUST NOT run before the synced blob has loaded from
// Cosmos on a fresh browser — if it ran on an empty local store, the
// "migration" would push an empty list to Cosmos and wipe the user's
// saved connections from every other browser. So it runs only when
// the local list is non-empty AND any entry actually contains a
// secret. Empty list → nothing to migrate, return immediately.
//
// Tracked in a per-user localStorage flag so it doesn't re-run on
// every page load. Safe to call repeatedly: post-flag, it's a no-op.
// Exposed on `window` so the top-level init block (which lives in a
// different <script> tag earlier in the file) can call this. Function
// declarations don't hoist across <script> boundaries — without the
// window assignment, the init call gets a ReferenceError and silently
// breaks the rest of init.
function sconnSecretsMigrationOnce(){
  const FLAG = 'cygenix_sconn_secrets_migrated_v1';
  try {
    if (localStorage.getItem(FLAG) === '1') return;
    if (!window.CygenixSavedConnSecrets) return;  // helper not loaded; bail
    const entries = sconnGetAll();
    if (!Array.isArray(entries) || entries.length === 0){
      // Nothing to migrate. Mark the flag so we don't re-check forever
      // — if the user adds a connection later, the new sconnSetAll
      // path will sanitise it directly.
      localStorage.setItem(FLAG, '1');
      return;
    }
    // Detect whether any entry still has its secret embedded — if all
    // are already clean (e.g. user is on a brand-new browser that's
    // pulled an already-clean blob from Cosmos), skip the rewrite to
    // avoid an unnecessary sync round-trip.
    const hasSecretEmbedded = entries.some(e =>
      e && (e.connString || e.fnKey)
    );
    if (!hasSecretEmbedded){
      localStorage.setItem(FLAG, '1');
      return;
    }
    // Re-save through sconnSetAll, which now strips secrets to the
    // local store and writes the sanitised list back. The Cosmos sync
    // layer picks up the change and uploads the clean version,
    // overwriting whatever's currently up there.
    sconnSetAll(entries);
    localStorage.setItem(FLAG, '1');
    console.log('[sconn] Migration complete: ' + entries.length + ' saved-connection record(s) sanitised. Secrets retained locally; cloud copy will be overwritten on next sync.');
  } catch (e) {
    console.error('[sconn] Migration failed:', e);
    // Don't set the flag — let it retry next page load.
  }
}
// Make accessible across <script> blocks (see comment on the function).
window.sconnSecretsMigrationOnce = sconnSecretsMigrationOnce;
// Invoke from THIS script block since the top-level init block (in an
// earlier <script>) can't see this function — declarations don't hoist
// across script-block boundaries. Wrapped in try/catch so any failure
// stays isolated. Idempotent (gated by the localStorage flag inside
// the function) so it's safe to call multiple times.
// DOMContentLoaded so the deferred CygenixSavedConnSecrets helper is
// guaranteed loaded — invoked at top level, the internal
// `if (!window.CygenixSavedConnSecrets) return;` guard would silently
// skip the one-time migration forever.
document.addEventListener('DOMContentLoaded', () => {
try {
  sconnSecretsMigrationOnce();
  // After migration, saved-connection data may have changed shape
  // (secrets moved out of the synced blob). The Project Status panel
  // rendered earlier in init with the pre-migration shape — re-render
  // it now so friendly-name lookup sees the post-migration data.
  // The delayed re-render scheduled by renderProjectStatus also picks
  // this up at +1.2s, but doing it here as well saves the wait when
  // the page is on a fast connection.
  if (typeof renderProjectStatus === 'function') {
    try { renderProjectStatus(); } catch(e){ /* benign */ }
  }
} catch(e){ console.error('[init epilogue] sconn-migration:', e); }
});

// Tooltip text for a saved-connection chip.
//
// This used to be the connection string with the password blanked out, which
// still put the host, port and username in a tooltip that appears on hover —
// enough to hand someone half a credential. The chip already shows the
// nickname the user chose, so the tooltip only adds the database name.
function sconnPreview(entry){
  if (entry.mode === 'azure') return 'Azure Function';
  const desc = (typeof parseDbConnection === 'function')
    ? parseDbConnection(entry.connString || '')
    : null;
  return (desc && desc.database) ? 'Database: ' + desc.database : 'Direct connection';
}

// Dialect label for the chip (pure display).
function sconnDialectLabel(entry){
  if (entry.mode === 'azure') return 'AZURE';
  const cs = (entry.connString || '').trim().toLowerCase();
  if (cs.startsWith('postgres://') || cs.startsWith('postgresql://')) return 'PG';
  return 'MSSQL';
}

// ══════════════════════════════════════════════════════════════════════════
// IMPORT/EXPORT SAVED CONNECTIONS — manual transport for the secret half
// ══════════════════════════════════════════════════════════════════════════
// The sanitised half of every saved connection already round-trips via
// Cosmos sync (see sconnSetAll / SYNC_KEYS). The secret half — connString
// (with embedded password) and Azure Function fnKey — is stored only in
// CygenixSavedConnSecrets on the local device and never reaches the
// cloud. That's the deliberate security posture.
//
// This module is the manual escape hatch for moving those secrets
// between browsers/machines: a user-driven export to a plaintext JSON
// file and an import that puts the secrets back into the local store
// on another device.
//
// All entry points are wired from the System parameters → Import/Export
// connections tab. See switchSysTab('conn-io').
// ══════════════════════════════════════════════════════════════════════════

// Bundle format version. Bump when the shape changes so older imports
// can warn the user instead of silently misreading fields. The bundle
// is intentionally simple — just rehydrated sconn entries plus a small
// header — so future versions should stay additive (new fields ignored
// by older readers).
const CONNIO_BUNDLE_VERSION = 1;

// Pending picker state. Holds the rows being shown in the multi-select
// picker plus the mode flag so connioPickerConfirm knows which path to
// dispatch to. Reset on cancel/confirm and whenever a fresh picker is
// opened. Keeping this in module-level state (not on the DOM) means
// the picker survives DOM redraws and we don't have to parse checked
// states back out of the markup at confirm time.
let _connIoMode = null;   // 'export' | 'import' | null
let _connIoRows = [];     // [{ key, entry, checked }]  key is index in source list

// Status writer shared by both flows. `tone` is one of 'ok' | 'warn' |
// 'err' | '' (neutral). Empty `msg` clears the line.
function connioSetStatus(side, msg, tone){
  const el = document.getElementById(side === 'export' ? 'connio-export-status' : 'connio-import-status');
  if (!el) return;
  el.textContent = msg || '';
  el.style.color =
      tone === 'ok'   ? 'var(--green)'
    : tone === 'warn' ? 'var(--amber)'
    : tone === 'err'  ? 'var(--red)'
    :                   'var(--text3)';
}

// Reset and hide the picker. Called by Cancel, by Confirm after success,
// and by switchSysTab when the tab is reopened (so a half-completed
// picker from last time doesn't reappear).
function connioPickerCancel(){
  _connIoMode = null;
  _connIoRows = [];
  const pick = document.getElementById('connio-picker');
  if (pick) pick.style.display = 'none';
  const body = document.getElementById('connio-picker-body');
  if (body) body.innerHTML = '';
}

// Render the picker rows from _connIoRows. Called whenever the row list
// changes (initial open, toggle-all). Individual checkbox toggles
// update _connIoRows directly via the inline onchange and don't need
// a full re-render.
function connioPickerRender(titleText, confirmLabel){
  const pick = document.getElementById('connio-picker');
  const titleEl = document.getElementById('connio-picker-title');
  const body = document.getElementById('connio-picker-body');
  const confirmBtn = document.getElementById('connio-picker-confirm');
  if (!pick || !body || !titleEl || !confirmBtn) return;

  titleEl.textContent = titleText;
  confirmBtn.textContent = confirmLabel;
  pick.style.display = '';

  if (!_connIoRows.length){
    body.innerHTML = '<div style="padding:1.5rem;text-align:center;color:var(--text3);font-size:12px">No saved connections found.</div>';
    return;
  }

  // Build rows. Each row shows: checkbox, side (source/target), name,
  // dialect chip, sanitised preview. Index encoded as data-idx so the
  // change handler can update _connIoRows without a closure per row.
  const rowsHtml = _connIoRows.map((row, idx) => {
    const e = row.entry;
    const sideLabel = (e.side === 'source') ? 'SRC' : (e.side === 'target') ? 'TGT' : '?';
    const sideColor = (e.side === 'source') ? 'var(--accent)' : 'var(--green)';
    const dialect = sconnDialectLabel(e);
    const preview = sconnPreview(e);
    const name = e.name || '(unnamed)';
    return (
      '<label style="display:flex;align-items:center;gap:0.75rem;padding:0.5rem 1rem;cursor:pointer;border-bottom:0.5px solid var(--border)" '
      + 'onmouseover="this.style.background=\'var(--bg3)\'" onmouseout="this.style.background=\'\'">'
      + '<input type="checkbox" data-connio-idx="' + idx + '" ' + (row.checked ? 'checked' : '') + ' onchange="connioPickerToggleRow(this)">'
      + '<span style="font-family:var(--mono);font-size:10px;color:' + sideColor + ';min-width:32px">' + sideLabel + '</span>'
      + '<span style="font-family:var(--mono);font-size:10px;color:var(--text3);min-width:42px">' + dialect + '</span>'
      + '<span style="font-size:12px;font-weight:500;min-width:140px">' + connioEsc(name) + '</span>'
      + '<span style="font-family:var(--mono);font-size:11px;color:var(--text3);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + connioEsc(preview) + '</span>'
      + '</label>'
    );
  }).join('');

  body.innerHTML = rowsHtml;
}

// HTML escape helper — local copy so this module has no dependency on
// the rest of the file's helpers. Same shape as wiEsc.
function connioEsc(s){
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

// Single-row toggle handler. Bound inline via onchange; reads the
// row index from data-connio-idx and flips the checked flag in
// module state. No re-render — the DOM already reflects the new
// checkbox state.
function connioPickerToggleRow(input){
  const idx = parseInt(input.getAttribute('data-connio-idx'), 10);
  if (Number.isNaN(idx) || !_connIoRows[idx]) return;
  _connIoRows[idx].checked = !!input.checked;
}

// Select-all / clear-all. Toggles every row to the same state and
// re-renders so the checkboxes reflect it.
function connioPickerToggleAll(checked){
  _connIoRows.forEach(r => { r.checked = !!checked; });
  // Re-render in the same mode so the title/confirm label are preserved.
  const titleText = _connIoMode === 'export' ? 'Select connections to export' : 'Select connections to import';
  const confirmLabel = _connIoMode === 'export' ? 'Download bundle' : 'Import selected';
  connioPickerRender(titleText, confirmLabel);
}

// ─── EXPORT FLOW ──────────────────────────────────────────────────────────

// Open the multi-select picker pre-populated with every locally
// available saved connection. sconnGetAll() already rehydrates each
// entry's secrets from the local secrets store before returning, so
// the rows here have the same shape as a freshly-saved entry —
// connString and fnKey present. If the local secrets store is empty
// (e.g. user just signed in on a new browser), those fields will be
// blank and there's nothing useful to export from this device.
function connioOpenExportPicker(){
  const entries = sconnGetAll();
  if (!entries.length){
    connioSetStatus('export', 'No saved connections to export on this device.', 'warn');
    return;
  }
  // Warn (but don't block) if every entry is missing its secret — the
  // export would still write a file, but it'd contain only sanitised
  // data, which the user almost certainly doesn't want. Common case
  // when the user is exporting from the wrong machine.
  const anyHasSecret = entries.some(e => (e.connString && e.connString.length) || (e.fnKey && e.fnKey.length));
  if (!anyHasSecret){
    connioSetStatus('export', 'Warning: no secrets found locally for any saved connection. Export would contain names only. Are you on the right device?', 'warn');
  } else {
    connioSetStatus('export', '');
  }

  _connIoMode = 'export';
  _connIoRows = entries.map(e => ({ entry: e, checked: true }));
  connioPickerRender('Select connections to export', 'Download bundle');
}

// Build the JSON bundle from the ticked rows and trigger a download.
// Filename includes ISO date so multiple exports don't collide.
function connioDoExport(){
  const picked = _connIoRows.filter(r => r.checked).map(r => r.entry);
  if (!picked.length){
    connioSetStatus('export', 'Pick at least one connection to export.', 'warn');
    return;
  }
  const bundle = {
    type: 'cygenix-connections-bundle',
    version: CONNIO_BUNDLE_VERSION,
    exportedAt: new Date().toISOString(),
    count: picked.length,
    // Entries are the full rehydrated shape (id, name, side, mode,
    // connString, fnUrl, fnKey, database). We deliberately copy each
    // entry through JSON.stringify/parse below to drop any internal
    // helper fields the rehydrator may have attached.
    entries: picked,
  };
  let json;
  try {
    json = JSON.stringify(JSON.parse(JSON.stringify(bundle)), null, 2);
  } catch (e) {
    console.error('[connio] export serialisation failed:', e);
    connioSetStatus('export', 'Failed to build export bundle. See console.', 'err');
    return;
  }

  const blob = new Blob([json], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const today = new Date().toISOString().slice(0, 10);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'cygenix-connections-' + today + '.json';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  // Revoke after the click has had time to start the download.
  setTimeout(() => URL.revokeObjectURL(url), 1500);

  connioSetStatus('export', 'Exported ' + picked.length + ' connection(s). Handle the file carefully — it contains plaintext credentials.', 'ok');
  connioPickerCancel();
}

// ─── IMPORT FLOW ──────────────────────────────────────────────────────────

// File picker change handler. Reads the file, parses it, validates
// the bundle shape, and opens the multi-select picker so the user
// can choose which entries to actually import. The file input is
// cleared at the end so re-selecting the same file fires `change`
// again — browsers suppress the event when the value is unchanged.
function connioReadImportFile(input){
  const file = input && input.files && input.files[0];
  if (!file){
    return;
  }
  const reader = new FileReader();
  reader.onload = () => {
    let bundle;
    try {
      bundle = JSON.parse(String(reader.result || ''));
    } catch (e) {
      connioSetStatus('import', 'Could not parse file as JSON.', 'err');
      input.value = '';
      return;
    }
    if (!bundle || bundle.type !== 'cygenix-connections-bundle' || !Array.isArray(bundle.entries)){
      connioSetStatus('import', 'File is not a Cygenix connections bundle.', 'err');
      input.value = '';
      return;
    }
    if (typeof bundle.version === 'number' && bundle.version > CONNIO_BUNDLE_VERSION){
      connioSetStatus('import', 'Bundle was created by a newer Cygenix version (v' + bundle.version + '). Some fields may be ignored.', 'warn');
      // Continue anyway — fields we don't know about are dropped by the
      // shape filter below, so the worst case is a clean import of
      // whatever still matches the current schema.
    } else {
      connioSetStatus('import', 'Loaded ' + bundle.entries.length + ' entry/entries from ' + (bundle.exportedAt || 'unknown date') + '. Choose which to import.', 'ok');
    }

    // Normalise each entry: keep only known fields, default missing
    // ones. Drops anything alien while preserving the round-trip shape.
    const norm = bundle.entries
      .filter(e => e && typeof e === 'object')
      .map(e => ({
        id:         String(e.id || '').trim(),
        name:       String(e.name || '').trim(),
        side:       e.side === 'target' ? 'target' : 'source',
        mode:       e.mode === 'azure' ? 'azure' : 'direct',
        connString: typeof e.connString === 'string' ? e.connString : '',
        database:   typeof e.database === 'string' ? e.database : '',
        fnUrl:      typeof e.fnUrl === 'string' ? e.fnUrl : '',
        fnKey:      typeof e.fnKey === 'string' ? e.fnKey : '',
      }))
      .filter(e => e.name);  // drop blank-name entries — nothing to merge against

    if (!norm.length){
      connioSetStatus('import', 'Bundle had no usable entries (missing names).', 'warn');
      input.value = '';
      return;
    }

    _connIoMode = 'import';
    _connIoRows = norm.map(e => ({ entry: e, checked: true }));
    connioPickerRender('Select connections to import', 'Import selected');
    input.value = '';
  };
  reader.onerror = () => {
    connioSetStatus('import', 'Could not read file.', 'err');
    input.value = '';
  };
  reader.readAsText(file);
}

// Confirm handler for the import flow. Merges the picked entries
// into the existing saved-connections list:
//   • Match by name + side (case-insensitive). If an existing entry
//     matches, replace it in place (keeps the existing id so any
//     references stay stable). If not, append as a new entry with a
//     fresh id.
//   • All writes go through sconnSetAll, which splits each entry
//     into sanitised (synced) and secret (local-only) halves. So
//     the secrets land in CygenixSavedConnSecrets, the sanitised
//     half lands in cygenix_saved_connections, and the next Cosmos
//     sync push uploads the sanitised half.
function connioDoImport(){
  const picked = _connIoRows.filter(r => r.checked).map(r => r.entry);
  if (!picked.length){
    connioSetStatus('import', 'Pick at least one entry to import.', 'warn');
    return;
  }

  const existing = sconnGetAll();
  // Build a lookup of existing entries by lowercase name+side. Same key
  // shape used by the merge logic. Two entries with the same name on
  // opposite sides (source vs target) are different rows, so the side
  // is part of the key.
  const keyOf = e => (String(e.name || '').toLowerCase() + '\0' + (e.side || 'source'));
  const byKey = new Map();
  existing.forEach((e, i) => byKey.set(keyOf(e), i));

  let replaced = 0;
  let added = 0;
  const merged = existing.slice();

  picked.forEach(inc => {
    const k = keyOf(inc);
    const idx = byKey.get(k);
    if (idx != null){
      // Replace, but keep the existing id so any references survive.
      const keepId = merged[idx] && merged[idx].id ? merged[idx].id : (inc.id || ('sconn_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7)));
      merged[idx] = Object.assign({}, inc, { id: keepId });
      replaced++;
    } else {
      // Append. Ensure an id is present — sconnSetAll's sanitiser
      // requires it to route secrets correctly.
      const id = inc.id && inc.id.trim() ? inc.id : ('sconn_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7));
      merged.push(Object.assign({}, inc, { id }));
      byKey.set(k, merged.length - 1);
      added++;
    }
  });

  try {
    sconnSetAll(merged);
  } catch (e) {
    console.error('[connio] import write failed:', e);
    connioSetStatus('import', 'Import failed during save. See console.', 'err');
    return;
  }

  // Re-render saved-connection chips on the Connections page so the
  // import is visible immediately when the user navigates over.
  if (typeof sconnRender === 'function'){
    try { sconnRender('source'); sconnRender('target'); } catch(e){ /* benign */ }
  }

  connioSetStatus('import',
    'Import done — added ' + added + ', replaced ' + replaced + '. Cloud sync will push the names; secrets stay on this device.',
    'ok'
  );
  connioPickerCancel();
}

// Generic confirm dispatcher. The picker is shared between flows so
// the button needs to route based on the current mode.
function connioPickerConfirm(){
  if (_connIoMode === 'export'){ connioDoExport(); return; }
  if (_connIoMode === 'import'){ connioDoImport(); return; }
  // No active mode — nothing to confirm. Treat as cancel.
  connioPickerCancel();
}

// Expose to window so the inline onclick handlers in the tab markup
// can find them regardless of which <script> block executed first.
// Same pattern as sconnSecretsMigrationOnce above.
window.connioOpenExportPicker  = connioOpenExportPicker;
window.connioReadImportFile    = connioReadImportFile;
window.connioPickerToggleAll   = connioPickerToggleAll;
window.connioPickerToggleRow   = connioPickerToggleRow;
window.connioPickerCancel      = connioPickerCancel;
window.connioPickerConfirm     = connioPickerConfirm;

// ── Connection-profile write guard ────────────────────────────────────────
// Tools that write or destroy (Data import, Restore database, Linked
// servers; the Data Generator page carries its own copy) run under the
// SELECTED connection profile once profiles are in force. Before the first
// profile exists this is a no-op — the ambient model behaves exactly as it
// always has. An UNKNOWN environment blocks writes; a PRD profile demands
// the profile id typed back. Every decision writes a run record.
function cpPageGuardWrite(toolLabel){
  try {
    const CP = window.CygenixProfiles;
    if (!CP) return true;
    const store = CP.cpLoad();
    const g = CP.cpGuardWrite(store, {});
    const who = (typeof CygenixConnections !== 'undefined' && CygenixConnections.currentUserTag
      && CygenixConnections.currentUserTag()) || '';
    if (!g.checked) return true;                       // unadopted — ambient era
    if (!g.ok){
      alert(toolLabel + ' is blocked.\n\n' + g.why);
      CP.cpRecordRun(store, { artifactType: 'tool', artifactId: toolLabel,
        profileId: (store.settings && store.settings.activeProfileId) || null,
        assertionResult: 'not-checked', outcome: 'refused — ' + g.why, runBy: who }, Date.now());
      CP.cpSave(store);
      return false;
    }
    if (g.requiresTypedConfirm){
      const typed = prompt('This is a ' + g.envClass + ' profile (' + g.profile.id + ').\n'
        + toolLabel + ' will WRITE to this environment.\n\nType the profile id to continue:');
      if (typed !== g.profile.id){
        if (typed != null) alert('Confirmation did not match ' + g.profile.id + ' — nothing was run.');
        CP.cpRecordRun(store, { artifactType: 'tool', artifactId: toolLabel,
          profileId: g.profile.id, assertionResult: 'not-checked',
          outcome: 'refused — typed confirmation did not match', runBy: who }, Date.now());
        CP.cpSave(store);
        return false;
      }
    }
    CP.cpRecordRun(store, { artifactType: 'tool', artifactId: toolLabel,
      profileId: g.profile.id, assertionResult: 'not-checked',
      outcome: 'started (' + g.envClass + ')', runBy: who }, Date.now());
    CP.cpSave(store);
    return true;
  } catch (e) { console.warn('[profiles] write guard', e); return true; }
}
window.cpPageGuardWrite = cpPageGuardWrite;

// Save current connection under a name. Reads the live fields.
function sconnSaveAs(side){
  // Read the current mode + fields
  const mode = side === 'src' ? srcMode : tgtMode;
  const entry = { side, mode, savedAt: new Date().toISOString() };

  if (mode === 'direct'){
    const csEl = document.getElementById('proj-' + side + '-cs');
    const cs = (csEl?.value || '').trim();
    if (!cs){
      alert('Enter a connection string before saving.');
      return;
    }
    entry.connString = cs;
  } else {
    const urlEl = document.getElementById('proj-' + side + '-fn-url');
    const keyEl = document.getElementById('proj-' + side + '-fn-key');
    const url = (urlEl?.value || '').trim();
    if (!url){
      alert('Enter an Azure Function URL before saving.');
      return;
    }
    entry.fnUrl = url;
    entry.fnKey = (keyEl?.value || '').trim();
  }

  // Default name suggestion — domain from the URL or first 30 chars of CS
  let defaultName = '';
  if (entry.mode === 'azure') defaultName = (entry.fnUrl.match(/https?:\/\/([^\/]+)/) || [])[1] || 'Azure Function';
  else if (entry.connString.startsWith('postgres')) {
    defaultName = (entry.connString.match(/@([^:/]+)/) || [])[1] || 'Postgres';
  } else {
    defaultName = (entry.connString.match(/Server\s*=\s*([^;,]+)/i) || [])[1] || 'SQL Server';
  }

  const name = prompt('Name this connection:', defaultName);
  if (!name || !name.trim()) return;
  entry.name = name.trim().slice(0, 60);
  entry.id = 'sconn_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7);

  // Environment class — required so profiles can govern this connection.
  // Anything unrecognised records UNKNOWN, which is permissive for reads but
  // blocks writes once profiles are in force (pressure to classify, without
  // stopping the world).
  const envClasses = ['DEV', 'TEST', 'UAT', 'PRD', 'SANDBOX'];
  const envIn = prompt('Environment class for this connection:\n'
    + envClasses.join(' / ') + '\n\n(Anything else records UNKNOWN — reads work, writes are blocked once profiles are in force.)',
    'UNKNOWN');
  const envClass = envClasses.indexOf(String(envIn || '').trim().toUpperCase()) >= 0
    ? String(envIn).trim().toUpperCase() : 'UNKNOWN';

  const all = sconnGetAll();
  // Block exact-name duplicates for the same side — confirm overwrite
  const existing = all.find(s => s.side === side && s.name === entry.name);
  let finalId = entry.id;
  if (existing){
    if (!confirm(`A ${side === 'src' ? 'source' : 'target'} connection named "${entry.name}" already exists. Overwrite?`)) return;
    const idx = all.indexOf(existing);
    all[idx] = { ...entry, id: existing.id };  // keep the old id for stability
    finalId = existing.id;
  } else {
    all.push(entry);
  }
  sconnSetAll(all);
  try {
    const CP = window.CygenixProfiles;
    if (CP){
      const st = CP.cpLoad();
      CP.cpSetConnMeta(st, finalId, { envClass },
        (typeof CygenixConnections !== 'undefined' && CygenixConnections.currentUserTag
          && CygenixConnections.currentUserTag()) || '', Date.now());
      CP.cpSave(st);
    }
  } catch (e) { console.warn('[profiles] env class not recorded', e); }
  sconnRender(side);
}

// Render the chip list for one side.
function sconnRender(side){
  const container = document.getElementById('sconn-' + side + '-list');
  const counter   = document.getElementById('sconn-' + side + '-count');
  if (!container) return;

  const all = sconnGetAll().filter(s => s.side === side);
  if (counter) counter.textContent = all.length;

  if (all.length === 0){
    container.innerHTML = '<span class="sconn-empty">None saved yet — enter a connection above and click “Save as…”</span>';
    return;
  }

  container.innerHTML = all.map(entry => {
    const dialect = sconnDialectLabel(entry);
    const preview = sconnPreview(entry);
    const nameEsc = (entry.name || '').replace(/[<>&"]/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;'}[c]));
    const previewEsc = preview.replace(/[<>&"]/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;'}[c]));
    return `<span class="sconn-chip" title="${previewEsc}" data-sconn-id="${entry.id}">
      <span class="sconn-dialect">${dialect}</span>
      <span onclick="sconnLoad('${entry.id}')">${nameEsc}</span>
      <span class="sconn-actions">
        <button class="sconn-action-btn" onclick="sconnRename('${entry.id}')" title="Rename"><i class="ic ic-edit"></i> </button>
        <button class="sconn-action-btn danger" onclick="sconnDelete('${entry.id}')" title="Delete">✕</button>
      </span>
    </span>`;
  }).join('');
}

// Load a saved connection into the live fields. Switches mode if needed.
function sconnLoad(id){
  const all = sconnGetAll();
  const entry = all.find(s => s.id === id);
  if (!entry){
    console.warn('[sconn] entry not found:', id);
    return;
  }

  const side = entry.side;
  // Switch mode
  if (side === 'src') setSrcMode(entry.mode); else setTgtMode(entry.mode);

  // Populate fields
  if (entry.mode === 'direct'){
    const el = document.getElementById('proj-' + side + '-cs');
    if (el) el.value = entry.connString || '';
  } else {
    const urlEl = document.getElementById('proj-' + side + '-fn-url');
    const keyEl = document.getElementById('proj-' + side + '-fn-key');
    if (urlEl) urlEl.value = entry.fnUrl || '';
    if (keyEl) keyEl.value = entry.fnKey || '';
  }

  // Refresh the masked summary — picking a chip changes which database is
  // configured, and setSrcMode/setTgtMode ran before the fields were filled.
  renderConnLock(side);

  // Flash feedback
  const resultEl = document.getElementById(side + '-conn-result');
  if (resultEl){
    resultEl.textContent = '✓ Loaded “' + entry.name + '”. Click Connect & test to verify.';
    resultEl.style.color = 'var(--green)';
  }
}

// Refuse edits to a connection a non-retired profile references: editing in
// place silently rewrites the meaning of every artifact that ever ran under
// that profile, including historic reports.
function sconnLockedGuard(id, entry, verb){
  try {
    const CP = window.CygenixProfiles;
    if (!CP) return false;
    const st = CP.cpLoad();
    if (CP.cpIsConnLocked(st, id)){
      alert('“' + entry.name + '” is locked — a non-retired profile references it, so it cannot be '
        + verb + '. A changed endpoint is a NEW saved connection and a new profile that supersedes '
        + 'the old one (Profiles page).');
      return true;
    }
  } catch (e) { /* guard must never block on its own error */ }
  return false;
}

// Rename a saved connection.
function sconnRename(id){
  const all = sconnGetAll();
  const entry = all.find(s => s.id === id);
  if (!entry) return;
  if (sconnLockedGuard(id, entry, 'renamed')) return;
  const newName = prompt('Rename connection:', entry.name);
  if (!newName || !newName.trim()) return;
  entry.name = newName.trim().slice(0, 60);
  sconnSetAll(all);
  sconnRender(entry.side);
}

// Delete a saved connection (with confirm).
function sconnDelete(id){
  const all = sconnGetAll();
  const entry = all.find(s => s.id === id);
  if (!entry) return;
  if (sconnLockedGuard(id, entry, 'deleted')) return;
  if (!confirm('Delete “' + entry.name + '”?')) return;
  const next = all.filter(s => s.id !== id);
  sconnSetAll(next);
  sconnRender(entry.side);
}

// ══════════════════════════════════════════════════════════════════════════
// LINKED SERVERS — draft + run SQL Server sp_addlinkedserver setup against
// the target. Users draft configs in localStorage, review them, then click
// Run setup to actually create the linked server on the target DB.
// Also lists existing linked servers and allows dropping them.
// Used by SQL Editor to surface OPENQUERY / four-part-name snippets.
// ══════════════════════════════════════════════════════════════════════════

const LS_KEY = 'cygenix_linked_server_drafts';

// ─── Storage helpers ─────────────────────────────────────────────────────
function lsGetDrafts(){
  try {
    const raw = localStorage.getItem(LS_KEY);
    if (!raw) return [];
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch { return []; }
}

function lsSetDrafts(list){
  try { localStorage.setItem(LS_KEY, JSON.stringify(list.slice(0, 30))); }
  catch (e) { console.error('[linked-servers] localStorage write failed:', e); }
}

// ─── Draft form validation ───────────────────────────────────────────────
// Linked server names are used as T-SQL identifiers in OPENQUERY and
// four-part names. We accept [A-Za-z0-9_] only to avoid quoting headaches.
function lsValidateDraftName(){
  const el = document.getElementById('ls-draft-name');
  const errEl = document.getElementById('ls-draft-name-err');
  const v = (el?.value || '').trim();
  let err = '';
  if (v && !/^[A-Za-z_][A-Za-z0-9_]{0,63}$/.test(v)){
    err = 'Use letters, digits, underscore only (start with letter). Max 64 chars.';
  }
  if (errEl){
    errEl.textContent = err;
    errEl.style.display = err ? 'block' : 'none';
  }
  return !err;
}

function lsClearDraftForm(){
  ['ls-draft-name','ls-draft-host','ls-draft-port','ls-draft-database','ls-draft-user','ls-draft-pass']
    .forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
  const err = document.getElementById('ls-draft-name-err');
  if (err) err.style.display = 'none';
  const status = document.getElementById('ls-draft-status');
  if (status) status.textContent = '';
}

function lsSaveDraft(){
  if (!lsValidateDraftName()) return;

  const name     = (document.getElementById('ls-draft-name')?.value     || '').trim();
  const type     = document.getElementById('ls-draft-type')?.value;
  const host     = (document.getElementById('ls-draft-host')?.value     || '').trim();
  const port     = (document.getElementById('ls-draft-port')?.value     || '').trim();
  const database = (document.getElementById('ls-draft-database')?.value || '').trim();
  const user     = (document.getElementById('ls-draft-user')?.value     || '').trim();
  const pass     = (document.getElementById('ls-draft-pass')?.value     || '');

  if (!name){ alert('Linked server name is required.'); return; }
  if (!host){ alert('Remote host is required.'); return; }
  if (!database){ alert('Remote database is required.'); return; }

  // Default port per type if left blank
  const defaultPort = type === 'postgres' ? '5432' : '1433';
  const resolvedPort = port || defaultPort;

  const drafts = lsGetDrafts();
  const existingIdx = drafts.findIndex(d => d.name === name);
  const draft = {
    id: existingIdx >= 0 ? drafts[existingIdx].id : 'ls_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7),
    name, type, host, port: resolvedPort, database, user, pass,
    savedAt: new Date().toISOString(),
    applied: existingIdx >= 0 ? drafts[existingIdx].applied : false,
  };

  if (existingIdx >= 0){
    if (!confirm(`A draft named "${name}" already exists. Overwrite?`)) return;
    drafts[existingIdx] = draft;
  } else {
    drafts.push(draft);
  }
  lsSetDrafts(drafts);

  const status = document.getElementById('ls-draft-status');
  if (status){
    status.textContent = '✓ Draft saved';
    status.style.color = 'var(--green)';
    setTimeout(() => { status.textContent = ''; }, 2500);
  }
  lsClearDraftForm();
  lsRenderDrafts();
}

// ─── Draft list render ───────────────────────────────────────────────────
function lsRenderDrafts(){
  const container = document.getElementById('ls-drafts-list');
  const counter   = document.getElementById('ls-drafts-count');
  if (!container) return;
  const drafts = lsGetDrafts();
  if (counter) counter.textContent = drafts.length;

  if (drafts.length === 0){
    container.innerHTML = '<div style="font-size:11px;color:var(--text3);font-style:italic;padding:0.5rem 0">No drafts yet — fill the form above and click Save draft.</div>';
    return;
  }

  container.innerHTML = drafts.map(d => {
    const typeLabel = d.type === 'postgres' ? 'POSTGRES' : 'SQL SERVER';
    const typeColor = d.type === 'postgres' ? 'var(--teal)' : '#6ea4ff';
    const appliedBadge = d.applied
      ? '<span style="background:rgba(34,197,94,0.15);color:var(--green);font-family:var(--mono);font-size:9px;padding:1px 6px;border-radius:100px">APPLIED</span>'
      : '<span style="background:var(--bg4);color:var(--text3);font-family:var(--mono);font-size:9px;padding:1px 6px;border-radius:100px">DRAFT</span>';
    return `<div style="padding:0.75rem;border:0.5px solid var(--border2);border-radius:var(--r);margin-bottom:0.5rem;background:var(--bg2)">
      <div style="display:flex;align-items:center;gap:0.75rem;flex-wrap:wrap;margin-bottom:0.5rem">
        <strong style="font-family:var(--mono);font-size:13px;color:var(--text)">${escHtml(d.name)}</strong>
        <span style="font-family:var(--mono);font-size:10px;color:${typeColor}">${typeLabel}</span>
        ${appliedBadge}
        <span style="font-size:11px;color:var(--text3);margin-left:auto;font-family:var(--mono)">${escHtml(d.host)}:${escHtml(d.port)}/${escHtml(d.database)}</span>
      </div>
      <div style="display:flex;gap:0.4rem;flex-wrap:wrap">
        <button class="btn btn-primary btn-sm" onclick="lsRunSetup('${d.id}')" title="Execute sp_addlinkedserver on target">▶ Run setup</button>
        <button class="btn btn-ghost btn-sm" onclick="lsPreviewSql('${d.id}')"><i class="ic ic-eye"></i> Preview SQL</button>
        <button class="btn btn-ghost btn-sm" onclick="lsEditDraft('${d.id}')"><i class="ic ic-edit"></i> Edit</button>
        <button class="btn btn-ghost btn-sm" onclick="lsDeleteDraft('${d.id}')" style="color:var(--red)">✕ Delete draft</button>
      </div>
      <div id="ls-draft-result-${d.id}" style="font-size:11px;font-family:var(--mono);margin-top:0.5rem;display:none;padding:0.4rem 0.6rem;border-radius:var(--r)"></div>
    </div>`;
  }).join('');
}

function lsEditDraft(id){
  const drafts = lsGetDrafts();
  const d = drafts.find(x => x.id === id);
  if (!d) return;
  document.getElementById('ls-draft-name').value     = d.name || '';
  document.getElementById('ls-draft-type').value     = d.type || 'postgres';
  document.getElementById('ls-draft-host').value     = d.host || '';
  document.getElementById('ls-draft-port').value     = d.port || '';
  document.getElementById('ls-draft-database').value = d.database || '';
  document.getElementById('ls-draft-user').value     = d.user || '';
  document.getElementById('ls-draft-pass').value     = d.pass || '';
  // Scroll form into view
  document.getElementById('ls-draft-name').scrollIntoView({ behavior:'smooth', block:'center' });
}

function lsDeleteDraft(id){
  const drafts = lsGetDrafts();
  const d = drafts.find(x => x.id === id);
  if (!d) return;
  if (!confirm(`Delete draft "${d.name}"? This does NOT drop the linked server if it was already created on the target — use the Existing list below to drop that.`)) return;
  lsSetDrafts(drafts.filter(x => x.id !== id));
  lsRenderDrafts();
}

// ─── SQL generation ──────────────────────────────────────────────────────
// Build the sp_addlinkedserver + sp_addlinkedsrvlogin calls for a draft.
// We do all string escaping here (single-quote doubling) because user-supplied
// host/db/password go straight into the T-SQL.
function lsBuildSetupSql(draft){
  const esc = s => String(s || '').replace(/'/g, "''");
  const name = esc(draft.name);

  if (draft.type === 'postgres'){
    // Postgres via generic OLE DB (MSDASQL).
    //
    // ⚠ IMPORTANT: MSDASQL + psqlODBC has a well-known silent-failure mode
    // on modern SQL Server versions — the connection succeeds standalone via
    // DSN test but MSDASQL fails with Msg 7303/7399 and no useful error text.
    // This is a Microsoft-side issue (MSDASQL is deprecated) and can't be
    // worked around from SQL. If you hit it, the setup SQL ran fine but
    // OPENQUERY will fail at runtime. Options:
    //   1. Install a native Postgres OLE DB provider (PGNP or CData) and
    //      change @provider below from MSDASQL to the native provider name
    //   2. Use Cygenix's Project Builder / Data Import flows instead
    //      (they reach Postgres directly via the Netlify proxy)
    //
    // What's correct here vs what might look wrong:
    // - Credentials go in sp_addlinkedsrvlogin, NOT in @provstr. Putting
    //   Uid/Pwd in the ODBC string is redundant and some builds reject it.
    // - SSLmode=require is in @provstr because MSDASQL does NOT inherit
    //   settings from User or System DSNs — it uses the string verbatim.
    // - Driver name is "PostgreSQL Unicode(x64)" — the default for the
    //   official psqlODBC installer. If yours installed under a different
    //   name, use the Edit button, change the driver field, save again.
    const provStr = `Driver={PostgreSQL Unicode(x64)};Server=${esc(draft.host)};Port=${esc(draft.port)};Database=${esc(draft.database)};SSLmode=require;`;
    return [
      `-- Linked server "${name}" → PostgreSQL ${esc(draft.host)}:${esc(draft.port)}/${esc(draft.database)}`,
      `-- Requires: psqlODBC driver installed on the SQL Server host (System-wide, not just User DSN)`,
      `-- Warning: MSDASQL + psqlODBC is flaky on modern SQL Server. If OPENQUERY fails with`,
      `-- Msg 7303/7399 (no error detail), the Microsoft MSDASQL layer is the cause. Consider`,
      `-- a native Postgres OLE DB provider (PGNP, CData) or using Cygenix's direct-connection flows.`,
      ``,
      `-- 1. Make sure MSDASQL can run in-process (required on SQL Server 2005+)`,
      `EXEC master.dbo.sp_MSset_oledb_prop N'MSDASQL', N'AllowInProcess',      1;`,
      `EXEC master.dbo.sp_MSset_oledb_prop N'MSDASQL', N'DynamicParameters',   1;`,
      ``,
      `-- 2. Create the linked server`,
      `EXEC master.dbo.sp_addlinkedserver`,
      `    @server     = N'${name}',`,
      `    @srvproduct = N'PostgreSQL',`,
      `    @provider   = N'MSDASQL',`,
      `    @provstr    = N'${esc(provStr)}';`,
      ``,
      `-- 3. Set the remote login (credentials NOT in provstr — go here)`,
      `EXEC master.dbo.sp_addlinkedsrvlogin`,
      `    @rmtsrvname  = N'${name}',`,
      `    @useself     = N'FALSE',`,
      `    @locallogin  = NULL,`,
      `    @rmtuser     = N'${esc(draft.user)}',`,
      `    @rmtpassword = N'${esc(draft.pass)}';`,
      ``,
      `-- 4. Set server options that Postgres linked servers commonly need`,
      `EXEC master.dbo.sp_serveroption @server = N'${name}', @optname = N'rpc out',     @optvalue = N'true';`,
      `EXEC master.dbo.sp_serveroption @server = N'${name}', @optname = N'data access', @optvalue = N'true';`,
      ``,
      `-- Test (run separately after setup):`,
      `-- SELECT * FROM OPENQUERY([${name}], 'SELECT 1 AS test');`,
    ].join('\n');
  }

  // SQL Server → SQL Server via native SQL Native Client.
  // No MSDASQL here, no ODBC layer, no known flakiness. This is the path
  // that actually works reliably in modern SQL Server.
  const dataSrc = draft.port && draft.port !== '1433' ? `${esc(draft.host)},${esc(draft.port)}` : esc(draft.host);
  return [
    `-- Linked server "${name}" → SQL Server ${esc(draft.host)}:${esc(draft.port || '1433')}/${esc(draft.database)}`,
    ``,
    `EXEC master.dbo.sp_addlinkedserver`,
    `    @server     = N'${name}',`,
    `    @srvproduct = N'',`,
    `    @provider   = N'SQLNCLI',`,
    `    @datasrc    = N'${dataSrc}',`,
    `    @catalog    = N'${esc(draft.database)}';`,
    ``,
    `EXEC master.dbo.sp_addlinkedsrvlogin`,
    `    @rmtsrvname  = N'${name}',`,
    `    @useself     = N'FALSE',`,
    `    @locallogin  = NULL,`,
    `    @rmtuser     = N'${esc(draft.user)}',`,
    `    @rmtpassword = N'${esc(draft.pass)}';`,
    ``,
    `-- Test (run separately after setup):`,
    `-- SELECT * FROM OPENQUERY([${name}], 'SELECT 1 AS test');`,
  ].join('\n');
}

function lsPreviewSql(id){
  const drafts = lsGetDrafts();
  const d = drafts.find(x => x.id === id);
  if (!d) return;
  const sql = lsBuildSetupSql(d);
  // Show in the inline result box under this draft — password is in the SQL
  // by necessity (it gets sent to the server), but we DO redact it in the
  // preview so it's not visible on screen.
  const redacted = sql.replace(/@rmtpassword\s*=\s*N'[^']*'/g, "@rmtpassword = N'<REDACTED>'")
                      .replace(/Pwd=[^;]*;/g, 'Pwd=<REDACTED>;');
  const el = document.getElementById('ls-draft-result-' + id);
  if (el){
    el.style.display = 'block';
    el.style.background = 'var(--bg3)';
    el.style.color = 'var(--text2)';
    el.innerHTML = '<div style="font-weight:500;color:var(--text);margin-bottom:0.25rem">SQL that will be executed (password redacted in view):</div><pre style="white-space:pre-wrap;margin:0;font-size:11px;line-height:1.5">' + escHtml(redacted) + '</pre>';
  }
}

// ─── Run setup — execute the SQL against the target ─────────────────────
// Describe the target in a short, human-readable way. Extracts either a host
// from an Azure Function URL (the Function configures the actual server), or
// a host from a connection string. Used in the target banner and confirm
// dialog so users know which SQL Server the setup will run against.
function lsDescribeTarget(){
  const c = (window.CygenixConnections && window.CygenixConnections.get && window.CygenixConnections.get()) || {};
  const ss = k => { try { return sessionStorage.getItem(k) || ''; } catch { return ''; } };

  const fnUrl = c.tgtFnUrl || c.fnUrl || ss('cygenix_fn_url');
  if (fnUrl){
    const host = (fnUrl.match(/^https?:\/\/([^\/]+)/) || [])[1] || fnUrl;
    return { kind:'azure', label:'Azure Function → ' + host, raw:host };
  }
  const cs = c.tgtConnString || c.connString || ss('cygenix_conn_string');
  if (cs){
    // Try ADO.NET "Server=..." first, then URL form
    let host = (cs.match(/Server\s*=\s*([^;,]+)/i) || [])[1];
    let db   = (cs.match(/(?:Database|Initial Catalog)\s*=\s*([^;]+)/i) || [])[1];
    if (!host){
      const m = cs.match(/^[a-z]+:\/\/[^@]+@([^:/]+)/i);
      if (m) host = m[1];
    }
    if (host){
      return { kind:'direct', label: host + (db ? ' / ' + db : ''), raw:host };
    }
    return { kind:'direct', label:'Direct connection string', raw:'unknown' };
  }
  return { kind:'none', label:'Not configured', raw:null };
}

// Update the target banner at the top of the Linked servers tab.
function lsUpdateTargetBanner(){
  const txt = document.getElementById('ls-target-text');
  const status = document.getElementById('ls-target-status');
  if (!txt) return;
  const t = lsDescribeTarget();
  txt.textContent = t.label;
  if (t.kind === 'none'){
    txt.style.color = 'var(--red)';
    if (status) status.innerHTML = '<a href="#" onclick="switchConnTab(\'databases\');return false" style="color:var(--accent);text-decoration:none">Configure target →</a>';
  } else {
    txt.style.color = 'var(--text)';
    if (status) status.textContent = 'Drafts you run setup for will be created here';
  }
}

async function lsRunSetup(id){
  const drafts = lsGetDrafts();
  const d = drafts.find(x => x.id === id);
  if (!d) return;

  const target = lsDescribeTarget();
  if (target.kind === 'none'){
    lsShowResult(id, 'No target database connection configured. Set it up in the Database connections tab first.', 'err');
    return;
  }
  if (!cpPageGuardWrite('Linked server setup')) return;

  const confirmMsg = `Create linked server "${d.name}"?\n\n` +
    `On target:  ${target.label}\n` +
    `Remote:     ${d.host}:${d.port}/${d.database}\n\n` +
    `This will execute sp_addlinkedserver and sp_addlinkedsrvlogin against the target. ` +
    `You need ALTER ANY LINKED SERVER permission (or sysadmin) for this to succeed.`;
  if (!confirm(confirmMsg)) return;

  const tgtConn = impGetConn('tgt');
  if (!tgtConn){
    lsShowResult(id, 'No target database connection configured.', 'err');
    return;
  }

  const sql = lsBuildSetupSql(d);
  lsShowResult(id, 'Running sp_addlinkedserver on ' + target.label + '…', 'info');

  try {
    const res = await impDbCall(tgtConn, { action:'execute', sql });
    if (res && res.error){
      lsShowSetupError(id, res.error, d);
      return;
    }
    // Mark draft as applied
    const drafts2 = lsGetDrafts();
    const d2 = drafts2.find(x => x.id === id);
    if (d2){ d2.applied = true; d2.appliedAt = new Date().toISOString(); lsSetDrafts(drafts2); }
    lsShowResult(id, '✓ Linked server "' + d.name + '" created on ' + target.label + '. Use [' + d.name + '] in your SQL Editor.', 'ok');
    lsRenderDrafts();
    lsRefreshServerList();
  } catch (e) {
    lsShowSetupError(id, e.message || String(e), d);
  }
}

// Render a setup error with context-aware guidance. The most common failure
// by far is "User does not have permission" — for that we show the specific
// GRANT statement the DBA needs to run, and offer to copy the SQL to run manually.
function lsShowSetupError(id, rawMsg, draft){
  const el = document.getElementById('ls-draft-result-' + id);
  if (!el) return;
  const lower = String(rawMsg || '').toLowerCase();
  const isPerm = lower.includes('permission') || lower.includes('alter any linked server') || lower.includes('sysadmin');

  el.style.display = 'block';
  el.style.background = 'rgba(239,68,68,0.08)';
  el.style.color = 'var(--text)';
  el.style.padding = '10px 12px';
  el.style.whiteSpace = 'normal';

  if (isPerm){
    el.innerHTML = `
      <div style="color:var(--red);font-weight:500;margin-bottom:6px"><i class="ic-dot" style="color:var(--red)"></i> Permission denied</div>
      <div style="font-size:11.5px;color:var(--text2);line-height:1.5;margin-bottom:8px">
        The target SQL Server rejected the request: <em>${escHtml(rawMsg)}</em>
      </div>
      <div style="font-size:11.5px;color:var(--text2);line-height:1.6;margin-bottom:8px">
        Creating linked servers requires <code style="background:var(--bg4);padding:1px 4px;border-radius:3px;font-family:var(--mono);font-size:11px">ALTER ANY LINKED SERVER</code> or <code style="background:var(--bg4);padding:1px 4px;border-radius:3px;font-family:var(--mono);font-size:11px">sysadmin</code>. Your current login doesn't have that.
      </div>
      <div style="font-size:11.5px;color:var(--text2);line-height:1.6">
        <strong style="color:var(--text)">Three ways to fix this:</strong>
        <ol style="margin:6px 0 0;padding-left:20px;font-size:11px">
          <li style="margin-bottom:4px"><strong>Get a DBA to grant you the permission</strong>:<br>
            <code style="background:var(--bg3);padding:4px 8px;border-radius:3px;font-family:var(--mono);font-size:11px;display:inline-block;margin-top:2px">GRANT ALTER ANY LINKED SERVER TO [your_cygenix_login];</code>
          </li>
          <li style="margin-bottom:4px"><strong>Run the setup SQL manually as sysadmin</strong> — click <em>Preview SQL</em> above (password is redacted in view — use the draft values when you paste).</li>
          <li><strong>Point Cygenix at a sysadmin connection temporarily</strong>, run setup, then switch back.</li>
        </ol>
      </div>`;
  } else {
    el.innerHTML = `
      <div style="color:var(--red);font-weight:500;margin-bottom:6px"><i class="ic-dot" style="color:var(--red)"></i> Setup failed</div>
      <div style="font-size:11.5px;color:var(--text2);line-height:1.5;font-family:var(--mono);word-break:break-word">${escHtml(rawMsg)}</div>`;
  }
}

function lsShowResult(id, msg, kind){
  const el = document.getElementById('ls-draft-result-' + id);
  if (!el) return;
  el.style.display = 'block';
  el.style.whiteSpace = 'pre-wrap';
  el.style.padding = '0.4rem 0.6rem';
  if (kind === 'ok')      { el.style.background = 'rgba(34,197,94,0.1)';  el.style.color = 'var(--green)'; }
  else if (kind === 'err'){ el.style.background = 'rgba(239,68,68,0.1)';   el.style.color = 'var(--red)'; }
  else                    { el.style.background = 'var(--bg3)';             el.style.color = 'var(--text2)'; }
  el.textContent = msg;
}

// ─── Existing linked servers on target ───────────────────────────────────
// Queries sys.servers + sys.linked_logins for what's already set up on the
// target, so the user can see them and drop them.
async function lsRefreshServerList(){
  const container = document.getElementById('ls-remote-list');
  const counter   = document.getElementById('ls-remote-count');
  if (!container) return;

  const tgtConn = impGetConn('tgt');
  if (!tgtConn){
    container.innerHTML = '<div style="font-size:11px;color:var(--amber);padding:0.5rem 0"><i class="ic ic-warning"></i> No target database connection configured. Set it up in the Database connections tab first.</div>';
    if (counter) counter.textContent = '–';
    return;
  }

  container.innerHTML = '<div style="font-size:11px;color:var(--text3);padding:0.5rem 0">Loading…</div>';

  try {
    // sys.servers: server_id > 0 means linked servers (0 is the local server itself).
    const res = await impDbCall(tgtConn, { action:'execute', sql: `
      SELECT s.name, s.product, s.provider, s.data_source, s.catalog, s.is_data_access_enabled, s.modify_date
      FROM sys.servers s
      WHERE s.server_id > 0
      ORDER BY s.name
    `});
    const rows = res?.recordset || [];
    if (counter) counter.textContent = rows.length;

    if (rows.length === 0){
      container.innerHTML = '<div style="font-size:11px;color:var(--text3);font-style:italic;padding:0.5rem 0">No linked servers configured on target. Create one from a draft above.</div>';
      return;
    }

    container.innerHTML = rows.map(r => {
      const n = escHtml(r.name);
      const provider = escHtml(r.provider || '');
      const ds = escHtml(r.data_source || '');
      const cat = escHtml(r.catalog || '');
      return `<div style="padding:0.6rem 0.75rem;border:0.5px solid var(--border2);border-radius:var(--r);margin-bottom:0.4rem;background:var(--bg2)">
        <div style="display:flex;align-items:center;gap:0.75rem;flex-wrap:wrap">
          <strong style="font-family:var(--mono);font-size:12.5px">${n}</strong>
          <span style="font-family:var(--mono);font-size:10px;color:var(--text3)">${provider}</span>
          <span style="font-size:11px;color:var(--text3);font-family:var(--mono);margin-left:auto">${ds}${cat ? ' · ' + cat : ''}</span>
          <button class="btn btn-ghost btn-sm" onclick="lsDropServer('${r.name.replace(/'/g,"\\'")}')" style="color:var(--red)" title="sp_dropserver with droplogins">✕ Drop</button>
        </div>
      </div>`;
    }).join('');
  } catch (e) {
    container.innerHTML = '<div style="font-size:11px;color:var(--red);padding:0.5rem 0"><i class="ic-dot" style="color:var(--red)"></i> Could not list linked servers: ' + escHtml(e.message || String(e)) + '</div>';
    if (counter) counter.textContent = '–';
  }
}

async function lsDropServer(name){
  // Defensive — name comes from sys.servers and has already been quoted into HTML,
  // but we also validate against the same strict identifier regex.
  if (!/^[A-Za-z_][A-Za-z0-9_]{0,63}$/.test(name)){
    alert('Refusing to drop — name contains characters outside the allowed identifier set: ' + name);
    return;
  }
  if (!confirm(`Drop linked server "${name}"?\n\nThis runs sp_dropserver with droplogins. Any views, procedures, or jobs referencing this server will break.`)) return;
  if (!cpPageGuardWrite('Drop linked server')) return;

  const tgtConn = impGetConn('tgt');
  if (!tgtConn){ alert('No target database connection configured.'); return; }

  try {
    await impDbCall(tgtConn, { action:'execute', sql:
      `EXEC master.dbo.sp_dropserver @server = N'${name}', @droplogins = 'droplogins';`
    });
    lsRefreshServerList();
  } catch (e) {
    alert('Drop failed: ' + (e.message || String(e)));
  }
}

// ─── Helper used by the SQL Editor via window.cygenixLinkedServers ───────
// Returns [{ name, type, host, database, applied }] so SQL Editor can render
// a linked-servers dropdown and generate OPENQUERY / four-part-name snippets.
// Only "applied" drafts appear — unapplied ones don't exist on the server yet.
window.cygenixLinkedServers = {
  list(){
    return lsGetDrafts().filter(d => d.applied).map(d => ({
      name: d.name, type: d.type, host: d.host, database: d.database, applied: true,
    }));
  },
  // Convenience — build a snippet the editor can insert at the cursor.
  // kind: 'openquery' | 'fourpart'
  snippet(name, kind, opts){
    opts = opts || {};
    if (kind === 'openquery'){
      const inner = opts.innerSql || 'SELECT * FROM your_remote_table';
      return `SELECT * FROM OPENQUERY([${name}], '${inner.replace(/'/g, "''")}')`;
    }
    // four-part: [srv].[db].[schema].[table]
    const db = opts.database || 'db';
    const schema = opts.schema || 'dbo';
    const table = opts.table || 'table_name';
    return `[${name}].[${db}].[${schema}].[${table}]`;
  }
};

// Simple HTML escape used by the linked-servers renderers. If the page already
// has an escP/esc helper we'd reuse it — but define our own to avoid cross-
// dependency fragility.
function escHtml(s){
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
    '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'
  }[c]));
}

// ══════════════════════════════════════════════════════════════════════════
// DATA IMPORT — Session 1: CSV → SQL Server table via existing Azure Function
// Limits: CSV only, up to ~50k rows, all columns created as inferred type but
// nullable (no user override yet). Excel/JSON and type customisation come in
// Session 2.
// ══════════════════════════════════════════════════════════════════════════

// Parsed state lives in memory until Import runs. Cleared on reset.
// For files above IMP_STREAMING_THRESHOLD rows, impState.rows is NULL and
// impState.streamingFile holds the source File for the import-time pass.
const IMP_STREAMING_THRESHOLD = 50000;  // rows — above this, switch to streaming
const IMP_XLSX_HARD_CAP       = 200000; // rows — XLSX can't stream, must fit in RAM
const IMP_CSV_HARD_CAP        = 2000000; // rows — guardrail, browser starts to struggle past this

let impState = {
  fileName: null,
  rows: null,            // array of row objects keyed by column name (in-memory mode only)
  columns: null,         // array of { name, type, nullable, samples, nullCount }
  rowCount: 0,
  streaming: false,      // true → impState.rows is null, use impState.streamingFile instead
  streamingFile: null,   // File object — re-parsed during impExecute when streaming
};

// ─── Abort support (single-file flow) ────────────────────────────────────
// Set to true when the user clicks the Abort button during an import. The
// in-memory and streaming execute paths check this before each 500-row batch
// and exit cleanly with a partial-import message. The partial table is NOT
// rolled back — partly because we can't (each INSERT auto-commits unless we
// wrapped in a transaction, which we don't), partly because for create/over-
// write modes the user can just drop the table afterwards if they want.
let impAbortRequested = false;

function impAbortRun(){
  impAbortRequested = true;
  const ab = document.getElementById('imp-abort-btn');
  if (ab){ ab.disabled = true; ab.textContent = 'Aborting…'; }
}

function impSetAbortVisible(visible){
  const ab = document.getElementById('imp-abort-btn');
  if (!ab) return;
  ab.style.display = visible ? '' : 'none';
  if (visible){
    ab.disabled = false;
    ab.textContent = '✕ Abort import';
  }
}

// ═════════════════════════════════════════════════════════════════════════════
// Restore Database tab (Connections) — UI over cygenix-restore.js
// ═════════════════════════════════════════════════════════════════════════════
// The engine holds all the decisions (capability matrix, guardrails, SQL);
// this layer probes the target, drives the capability-aware controls, and
// polls progress. Everything the server runs goes through db-connect where
// the RBAC gate enforces restore.inspect / restore.execute.

const rstState = { caps: null, family: null, probe: null, header: null, files: null,
                   moves: null, validated: false, polling: null };

function rstTargetConn(){
  const c = (window.CygenixConnections && CygenixConnections.get && CygenixConnections.get()) || {};
  const fn = c.tgtFnUrl || '';
  if (fn) return c.tgtFnKey ? fn + '?code=' + encodeURIComponent(c.tgtFnKey) : fn;
  return c.tgtConnString || '';
}
function rstTargetLabel(){
  const c = (window.CygenixConnections && CygenixConnections.get && CygenixConnections.get()) || {};
  if (c.tgtFnUrl) return 'Azure Function target';
  const m = /\/([^/?]+)(\?|$)/.exec(c.tgtConnString || '');
  return (m && m[1]) ? m[1] : (c.tgtConnString ? 'Direct target' : 'no target configured');
}
function rstSourceDbName(){
  const c = (window.CygenixConnections && CygenixConnections.get && CygenixConnections.get()) || {};
  const m = /\/([^/?]+)(\?|$)/.exec(c.srcConnString || '');
  return (m && m[1]) || '';
}
async function rstDbCall(body){
  const conn = rstTargetConn();
  if (!conn) throw new Error('No target connection configured — set one on the Database connections tab.');
  const isFn = /^https?:\/\//i.test(conn);
  const url = isFn ? conn : '/.netlify/functions/db-connect';
  const payload = isFn ? body : { ...body, connectionString: conn };
  const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload), signal: AbortSignal.timeout(120000) });
  const data = await res.json().catch(() => ({ error: 'Non-JSON (' + res.status + ')' }));
  if (!res.ok) throw new Error(data.error || res.statusText);
  if (data && data.error) throw new Error(data.error);
  return data;
}

function rstInit(){
  $('rst-target-name').textContent = rstTargetLabel();
  if (!rstState.caps) rstProbe(false);
  else rstRenderCaps();
  rstModeChanged(); rstOptionsChanged();
}

async function rstProbe(force){
  const R = CygenixRestore;
  const btn = $('rst-probe-btn');
  if (rstState.caps && !force) return;
  if (btn){ btn.disabled = true; btn.textContent = 'Probing…'; }
  try {
    const r = await rstDbCall({ action: 'execute', sql: R.rdProbeSql });
    const row = (r.recordset && r.recordset[0]) || {};
    const cls = R.rdClassifyEngine(row.engine_edition);
    rstState.probe = row; rstState.family = cls.family;
    rstState.caps = R.rdCapabilities(cls.family);
    $('rst-target-family').textContent = cls.label;
    rstRenderCaps();
  } catch (e) {
    rstState.caps = CygenixRestore.rdCapabilities('unknown');
    $('rst-target-family').textContent = 'probe failed';
    $('rst-caps').textContent = e.message;
    rstRenderCaps();
  }
  if (btn){ btn.disabled = false; btn.textContent = 'Detect capabilities'; }
}

// Disable — never hide — what the target cannot do, with the reason.
function rstRenderCaps(){
  const caps = rstState.caps || CygenixRestore.rdCapabilities('unknown');
  const chip = (label, v) => v === true
    ? '<span style="color:var(--green)">' + label + ' ✓</span>'
    : '<span style="color:var(--text3)" title="' + escHtml(String(v)) + '">' + label + ' ✗</span>';
  $('rst-caps').innerHTML = [chip('FROM DISK', caps.fromDisk), chip('FROM URL', caps.fromUrl),
    chip('REPLACE', caps.replace), chip('WITH MOVE', caps.withMove), chip('Log chain', caps.logChain)]
    .join(' · ');
  const setRadio = (sel, cap) => {
    const label = document.getElementById(sel);
    if (!label) return;
    const input = label.querySelector('input');
    const ok = cap === true;
    input.disabled = !ok;
    label.style.opacity = ok ? '1' : '0.5';
    label.title = ok ? '' : String(cap);
    if (!ok && input.checked){
      const alt = document.querySelector('input[name="' + input.name + '"]:not(:disabled)');
      if (alt) alt.checked = true;
    }
  };
  setRadio('rst-mode-disk-label', caps.fromDisk);
  setRadio('rst-mode-url-label', caps.fromUrl);
  setRadio('rst-as-overwrite-label', caps.replace);
  rstModeChanged();
}

function rstMode(){ return (document.querySelector('input[name="rst-mode"]:checked') || {}).value || 'disk'; }
function rstModeChanged(){
  const url = rstMode() === 'url';
  $('rst-path').placeholder = url
    ? 'https://account.blob.core.windows.net/backups/Conversion_DM.bak'
    : 'D:\\backups\\Conversion_DM.bak';
  $('rst-cred-help').style.display = url ? '' : 'none';
  if (url){
    const container = String($('rst-path').value || 'https://account.blob.core.windows.net/backups/x.bak').replace(/\/[^/]*$/, '');
    $('rst-cred-sql').textContent = CygenixRestore.rdCredentialSql(container);
  }
  rstState.validated = false; $('rst-run-btn').disabled = true;
}
function rstOptionsChanged(){
  const overwrite = (document.querySelector('input[name="rst-as"]:checked') || {}).value === 'overwrite';
  $('rst-confirm-wrap').style.display = overwrite ? '' : 'none';
  $('rst-newname').placeholder = overwrite
    ? 'Existing database name to overwrite'
    : 'Restored database name, e.g. Conversion_DM_r1';
  rstState.validated = false; $('rst-run-btn').disabled = true;
}

// ── Inspect (never restore a file blind) ─────────────────────────────────
async function rstInspect(){
  const R = CygenixRestore;
  const path = String($('rst-path').value || '').trim();
  const status = $('rst-inspect-status');
  if (!path){ status.textContent = 'Enter a backup path or URL first.'; return; }
  const source = { kind: rstMode(), path };
  const sqls = R.rdInspectSql(source);
  const btn = $('rst-inspect-btn');
  btn.disabled = true; status.textContent = 'Inspecting…';
  try {
    const h = await rstDbCall({ action: 'execute', sql: sqls.headerSql });
    rstState.header = R.rdParseHeaderOnly(h.recordset || []);
    const f = await rstDbCall({ action: 'execute', sql: sqls.fileListSql });
    rstState.files = R.rdParseFileList(f.recordset || []);
    let verified = '';
    try { await rstDbCall({ action: 'execute', sql: sqls.verifySql }); verified = ' · checksum verified ✓'; }
    catch (e) { verified = ' · VERIFYONLY failed: ' + (R.rdTranslateError(e.message).friendly || e.message); }
    status.textContent = rstState.header.length + ' backup set(s), ' + rstState.files.length + ' file(s)' + verified;
    rstRenderContents();
    // Suggest the restored name BEFORE building the moves, so the file
    // paths are named for the destination database, not a placeholder.
    if (!$('rst-newname').value && rstState.header[0]){
      $('rst-newname').value = rstState.header[0].databaseName + '_r1';
    }
    rstRenderMoves();
    // Version downgrade — surfaced immediately, not at run time.
    const tgtMajor = rstState.probe && rstState.probe.major_version;
    const v = R.rdVersionBlock(rstState.header[0] && rstState.header[0].softwareVersionMajor, tgtMajor);
    if (v.blocked) status.innerHTML = '<span style="color:var(--red)"><i class="ic ic-blocked"></i> ' + escHtml(v.message) + '</span>';
  } catch (e) {
    const t = R.rdTranslateError(e.message);
    status.innerHTML = '<span style="color:var(--red)">' + escHtml(t.friendly || e.message) + '</span>';
    rstState.header = null; rstState.files = null;
  }
  btn.disabled = false;
  rstState.validated = false; $('rst-run-btn').disabled = true;
}

function rstRenderContents(){
  const rows = (rstState.header || []).map((s, i) =>
    '<tr><td><input type="radio" name="rst-set" value="' + s.position + '" ' + (i === 0 ? 'checked' : '') + '></td>'
    + '<td style="font-family:var(--mono)">' + escHtml(s.databaseName) + '</td>'
    + '<td>' + escHtml(s.type) + '</td>'
    + '<td style="font-family:var(--mono);font-size:11px">' + escHtml(String(s.startedAt || '').slice(0, 19).replace('T', ' ')) + '</td>'
    + '<td style="font-family:var(--mono);font-size:11px">' + escHtml(s.recoveryModel) + '</td>'
    + '<td style="font-family:var(--mono);font-size:11px">v' + s.softwareVersionMajor + ' · compat ' + s.compatibilityLevel + '</td>'
    + '<td style="font-family:var(--mono);font-size:11px">' + (s.sizeBytes ? (s.sizeBytes / 1048576).toFixed(0) + ' MB' : '—') + '</td></tr>').join('');
  $('rst-contents').innerHTML = rows
    ? '<table class="jobs-table" style="font-size:12px"><thead><tr><th></th><th>Database</th><th>Type</th><th>Started</th><th>Recovery</th><th>Version</th><th>Size</th></tr></thead><tbody>' + rows + '</tbody></table>'
    : '<div style="font-size:12px;color:var(--text3)">Nothing inspected yet.</div>';
  $('rst-contents-panel').style.display = rows ? '' : 'none';
}

function rstRenderMoves(){
  const caps = rstState.caps || {};
  if (caps.withMove !== true || !rstState.files || !rstState.files.length){
    $('rst-moves-wrap').style.display = 'none'; rstState.moves = null; return;
  }
  const p = rstState.probe || {};
  rstState.moves = CygenixRestore.rdBuildMoves(rstState.files, p.data_path || 'C:\\SQLData\\', p.log_path || p.data_path || 'C:\\SQLLogs\\',
    $('rst-newname').value || 'restored');
  $('rst-moves').innerHTML = rstState.moves.map((m, i) =>
    '<div style="display:flex;gap:0.4rem;align-items:center;margin-bottom:2px">'
    + '<span style="color:var(--text3);min-width:140px">' + escHtml(m.logicalName) + ' →</span>'
    + '<input class="form-input" style="flex:1;font-family:var(--mono);font-size:11px;padding:2px 6px" value="' + escHtml(m.physicalPath) + '"'
    + ' oninput="rstState.moves[' + i + '].physicalPath=this.value">'
    + '</div>').join('');
  $('rst-moves-wrap').style.display = '';
}

// ── Plan, validate, run ──────────────────────────────────────────────────
function rstCollectPlan(){
  const asOverwrite = (document.querySelector('input[name="rst-as"]:checked') || {}).value === 'overwrite';
  const name = String($('rst-newname').value || '').trim();
  return {
    targetDb: name, mode: rstMode(),
    source: { kind: rstMode(), path: String($('rst-path').value || '').trim() },
    restoreAs: asOverwrite ? { kind: 'overwrite' } : { kind: 'newDatabase', name },
    replace: asOverwrite,
    confirmName: String(($('rst-confirm') || {}).value || '').trim(),
    recovery: (document.querySelector('input[name="rst-recovery"]:checked') || {}).value || 'RECOVERY',
    fileIndex: Number((document.querySelector('input[name="rst-set"]:checked') || {}).value || 1),
    moves: rstState.moves || [],
  };
}

function rstValidate(){
  const R = CygenixRestore;
  const plan = rstCollectPlan();
  const hdr = (rstState.header || [])[0];
  const v = R.rdValidatePlan(plan, {
    capabilities: rstState.caps || R.rdCapabilities('unknown'),
    sourceDbName: rstSourceDbName(),
    backupMajor: hdr && hdr.softwareVersionMajor,
    targetMajor: rstState.probe && rstState.probe.major_version,
    headerType: hdr && hdr.typeCode,
  });
  if (!rstState.header) v.warnings.unshift('The backup has not been inspected — Inspect first so the restore is not blind.');
  const box = $('rst-validation');
  box.innerHTML =
    v.errors.map(e => '<div style="font-size:12px;color:var(--red);margin-bottom:2px">✕ ' + escHtml(e) + '</div>').join('')
    + v.warnings.map(w => '<div style="font-size:12px;color:var(--amber);margin-bottom:2px"><i class="ic ic-warning"></i> ' + escHtml(w) + '</div>').join('')
    + (v.ok ? '<div style="font-size:12px;color:var(--green)">✓ Plan is valid for this target.</div>' : '');
  if (v.ok){
    $('rst-sql').textContent = R.rdBuildRestoreSql(plan);
    $('rst-sql-wrap').style.display = '';
  }
  rstState.validated = v.ok && !!rstState.header;
  $('rst-run-btn').disabled = !rstState.validated;
  return v.ok;
}

async function rstRun(){
  if (!rstState.validated){ if (!rstValidate()) return; }
  if (!cpPageGuardWrite('Restore database')) return;
  const R = CygenixRestore;
  const plan = rstCollectPlan();
  const sql = R.rdBuildRestoreSql(plan);
  const status = $('rst-run-status');
  $('rst-run-btn').disabled = true;
  status.textContent = 'Restoring… (progress polls every 5s; large restores keep running server-side)';
  $('rst-progress').style.display = '';
  rstStartPolling();
  try {
    await rstDbCall({ action: 'execute', sql });
    rstStopPolling();
    $('rst-progress-bar').style.width = '100%';
    status.innerHTML = '<span style="color:var(--green)">✓ Restore completed'
      + (plan.recovery === 'NORECOVERY' ? ' — database left in RESTORING (use Bring online when the chain is done)' : '') + '</span>';
    if (plan.recovery === 'NORECOVERY') $('rst-online-btn').style.display = '';
  } catch (e) {
    const t = R.rdTranslateError(e.message);
    if (t.stillRunning){
      status.innerHTML = '<span style="color:var(--amber)">' + escHtml(t.friendly) + '</span>';
      return;                                    // keep polling — MI async rule
    }
    rstStopPolling();
    $('rst-progress').style.display = 'none';
    status.innerHTML = '<span style="color:var(--red)">✕ ' + escHtml(t.friendly || e.message) + '</span>'
      + (t.friendly ? '<div style="font-size:10.5px;color:var(--text3);margin-top:2px">' + escHtml(String(e.message).slice(0, 200)) + '</div>' : '');
    $('rst-online-btn').style.display = '';      // a failed chain can leave RESTORING
  }
  $('rst-run-btn').disabled = false;
}

function rstStartPolling(){
  rstStopPolling();
  rstState.polling = setInterval(async () => {
    try {
      const r = await rstDbCall({ action: 'execute', sql: CygenixRestore.rdProgressSql });
      const row = (r.recordset || [])[0];
      if (row){
        const pct = Math.round(Number(row.pct) || 0);
        $('rst-progress-bar').style.width = pct + '%';
        $('rst-progress-label').textContent = (row.command || 'RESTORE') + ' — ' + pct + '%'
          + (row.eta_ms ? ' · ~' + Math.round(row.eta_ms / 60000) + ' min remaining' : '');
      }
    } catch { /* polling is best-effort */ }
  }, 5000);
}
function rstStopPolling(){ if (rstState.polling){ clearInterval(rstState.polling); rstState.polling = null; } }

async function rstBringOnline(){
  const name = String($('rst-newname').value || '').trim();
  if (!name) return;
  const status = $('rst-run-status');
  try {
    await rstDbCall({ action: 'execute', sql: CygenixRestore.rdBringOnlineSql(name) });
    status.innerHTML = '<span style="color:var(--green)">✓ ' + escHtml(name) + ' brought online.</span>';
    $('rst-online-btn').style.display = 'none';
  } catch (e) {
    status.innerHTML = '<span style="color:var(--red)">✕ ' + escHtml(e.message) + '</span>';
  }
}

function switchConnTab(tab){
  document.querySelectorAll('#view-connections .cx-tab').forEach(el => {
    el.classList.toggle('cx-tab-active', el.dataset.tab === tab);
  });
  const db = document.getElementById('conn-tab-databases');
  const im = document.getElementById('conn-tab-import');
  const lk = document.getElementById('conn-tab-linked');
  const bl = document.getElementById('conn-tab-blob');
  const rs = document.getElementById('conn-tab-restore');
  if (db) db.style.display = tab === 'databases' ? 'block' : 'none';
  if (im) im.style.display = tab === 'import'    ? 'block' : 'none';
  if (lk) lk.style.display = tab === 'linked'    ? 'block' : 'none';
  if (bl) bl.style.display = tab === 'blob'      ? 'block' : 'none';
  if (rs) rs.style.display = tab === 'restore'   ? 'block' : 'none';
  if (tab === 'restore') { try { rstInit(); } catch(e){ console.warn('[restore] init:', e); } }
  // Hide save/clear buttons when not on db connections tab
  const s = document.getElementById('conn-tab-save-btn');
  const c = document.getElementById('conn-tab-clear-btn');
  if (s) s.style.display = tab === 'databases' ? '' : 'none';
  if (c) c.style.display = tab === 'databases' ? '' : 'none';
  // When switching TO the linked tab, load drafts and refresh remote list
  if (tab === 'linked'){
    lsUpdateTargetBanner();
    lsRenderDrafts();
    lsRefreshServerList();  // fire-and-forget; UI will update when done
  }
  // When switching TO the blob tab, render the saved-source chips
  if (tab === 'blob'){
    try { blobSourceRenderSaved(); } catch(e){ console.warn('[blob-source] render saved failed:', e); }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// AZURE BLOB SOURCE — client-owned storage, browser-direct (no Cygenix backend).
//
// Storage model: localStorage key `cygenix_blob_sources` holds a per-user map
// keyed by email (matching CygenixConnections.currentUserTag()), each value an
// array of saved-source entries:
//   { id, name, sasUrl, accountName, containerName, sasExpiresAt, createdAt }
//
// SAS URL contains the signature — same security posture as storing SQL conn
// strings with embedded passwords, which the rest of the app already does.
//
// The browser hits the Azure Blob REST API directly with the SAS:
//   list:   GET {scheme}://{account}.blob.core.windows.net/{container}?restype=container&comp=list&{sasToken}
//   fetch:  GET {scheme}://{account}.blob.core.windows.net/{container}/{blobName}?{sasToken}
// CORS must be enabled on the client's storage account for our origin or these
// requests will be blocked by the browser. There's a help line in the UI that
// flags this; the test step surfaces CORS failures with a recognisable message.
//
// After fetch, the bytes are wrapped in a `File` object (so file.name carries
// through) and passed to the existing impHandleFile() — same code path as a
// drag-drop in the Data Import tab. The user gets the schema preview, target
// picker and import button automatically; nothing duplicated.
//
// FOLLOW-UP (deferred): add `cygenix_blob_sources` to SYNC_KEYS in
// cygenix-cosmos-sync.js so saved sources roam across devices. Single-line
// addition once that file is to hand. v1 is browser-local only.
// ─────────────────────────────────────────────────────────────────────────────

const BLOB_SRC_LS_KEY = 'cygenix_blob_sources';

// Per-user blob: read the whole top-level map then slice by current user, same
// pattern as CygenixConnections. Returns [] if nobody signed in or no entries.
function blobSourceReadAll(){
  let uid = '';
  try {
    if (window.CygenixConnections && typeof CygenixConnections.currentUserTag === 'function'){
      uid = CygenixConnections.currentUserTag() || '';
    }
  } catch {}
  if (!uid) return [];
  let blob;
  try { blob = JSON.parse(localStorage.getItem(BLOB_SRC_LS_KEY) || '{}'); } catch { return []; }
  if (!blob || typeof blob !== 'object' || Array.isArray(blob)) return [];
  const arr = blob[uid];
  return Array.isArray(arr) ? arr : [];
}

function blobSourceWriteAll(list){
  let uid = '';
  try {
    if (window.CygenixConnections && typeof CygenixConnections.currentUserTag === 'function'){
      uid = CygenixConnections.currentUserTag() || '';
    }
  } catch {}
  if (!uid) return false;
  let blob;
  try { blob = JSON.parse(localStorage.getItem(BLOB_SRC_LS_KEY) || '{}'); } catch { blob = {}; }
  if (!blob || typeof blob !== 'object' || Array.isArray(blob)) blob = {};
  blob[uid] = Array.isArray(list) ? list.slice(0, 50) : [];
  try { localStorage.setItem(BLOB_SRC_LS_KEY, JSON.stringify(blob)); return true; }
  catch (e){ console.warn('[blob-source] write failed:', e); return false; }
}

// Parse a container SAS URL. Container SAS shape:
//   https://{account}.blob.core.windows.net/{container}?{sasParams}
// We tolerate a trailing slash on the container and any query-string ordering.
// Returns { ok, accountName, containerName, sasToken, sasExpiresAt, error }.
function blobSourceParseSas(sasUrl){
  if (!sasUrl || typeof sasUrl !== 'string'){
    return { ok:false, error:'No SAS URL provided.' };
  }
  let u;
  try { u = new URL(sasUrl.trim()); }
  catch { return { ok:false, error:'That doesn\'t look like a valid URL.' }; }

  if (u.protocol !== 'https:'){
    return { ok:false, error:'SAS URL must be https://, not ' + u.protocol };
  }
  const host = u.hostname.toLowerCase();
  // Match {account}.blob.core.windows.net (commercial Azure). Sovereign clouds
  // use different suffixes (.blob.core.chinacloudapi.cn etc) — accept anything
  // that contains ".blob." to be tolerant. Sovereign clouds are rare for our
  // client base but we don't want to hard-reject them.
  if (!/\.blob\./.test(host)){
    return { ok:false, error:'URL hostname doesn\'t look like Azure Blob Storage (expected *.blob.core.windows.net).' };
  }
  const accountName = host.split('.')[0];

  // Container = first non-empty path segment. Anything past that is a blob
  // name, which we don't accept for v1 (container SAS only).
  const segs = u.pathname.split('/').filter(Boolean);
  if (segs.length === 0){
    return { ok:false, error:'URL is missing the container name.' };
  }
  if (segs.length > 1){
    return { ok:false, error:'This looks like a single-blob SAS. Paste a container SAS URL (no file path after the container name).' };
  }
  const containerName = segs[0];

  const sasToken = u.search.startsWith('?') ? u.search.slice(1) : u.search;
  if (!sasToken){
    return { ok:false, error:'URL has no SAS token (missing ?sv=… query string).' };
  }
  // Check the SAS signature param is present — biggest single sign of a
  // pasted-but-malformed URL.
  if (!/[?&]sig=/.test('?' + sasToken)){
    return { ok:false, error:'SAS token is missing the signature (sig=) parameter. The URL may have been truncated.' };
  }

  // Pull expiry (`se=...`) for the UI warning. Optional — older SAS variants
  // may not have it.
  let sasExpiresAt = '';
  const params = new URLSearchParams(sasToken);
  const seRaw = params.get('se');
  if (seRaw){
    // se is ISO-8601 (may be URL-encoded — URLSearchParams already decoded it).
    const d = new Date(seRaw);
    if (!isNaN(d.getTime())) sasExpiresAt = d.toISOString();
  }

  return {
    ok: true,
    accountName,
    containerName,
    sasToken,
    sasExpiresAt,
  };
}

// Azure Function endpoint for the blob proxy. Uses the same hardcoded
// function key as the rest of dashboard.html (whoami, billing-portal,
// cygenix-cosmos-sync.js). Flagged in user memory as tech debt — proper
// fix is JWT Bearer auth mirroring reports.js, deferred until prioritised.
const BLOB_PROXY_BASE = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/data';
const BLOB_PROXY_CODE = 'WjSmoWxgtNdGnO_I5nKIspRUQqKCR1knsXgVmJr3dyYuAzFu-or-5Q==';

// Pull the caller's email from the MSAL local cache for the x-user-id header
// the Function dispatcher requires. Same retrieval pattern as enhanceUserPill
// elsewhere in this file. Returns '' if not signed in (the request will then
// 401 cleanly from the Function).
function blobSourceCurrentUserId(){
  // First try CygenixConnections.currentUserTag — it has the same logic plus
  // legacy fallbacks. If that fails, drop to direct MSAL cache scrape.
  try {
    if (window.CygenixConnections && typeof CygenixConnections.currentUserTag === 'function'){
      const t = CygenixConnections.currentUserTag();
      if (t) return t;
    }
  } catch {}
  try {
    for (let i = 0; i < localStorage.length; i++) {
      const k = localStorage.key(i);
      if (!k) continue;
      if (!k.includes('-login.windows.net-') && !k.includes('.ciamlogin.com-')) continue;
      const raw = localStorage.getItem(k);
      if (!raw || raw[0] !== '{') continue;
      let obj; try { obj = JSON.parse(raw); } catch { continue; }
      if (obj && obj.username && obj.authorityType && obj.homeAccountId) {
        return String(obj.username || '').trim().toLowerCase();
      }
    }
  } catch {}
  return '';
}

// Server-side proxy of the Azure Blob "List Blobs" REST call. Browser cannot
// hit the client storage account directly because CORS is unlikely to be
// enabled there — clients are reached via SaaS without a comms channel to
// their Azure admins. The Function makes the call server-to-server and
// returns the parsed blob array. Returns
//   { ok, blobs: [{name, size, lastModified, contentType}], error }
async function blobSourceListContainer(parsed){
  const userId = blobSourceCurrentUserId();
  if (!userId){
    return { ok:false, error:'You need to be signed in to browse blob containers.' };
  }
  // Reconstruct the SAS URL the Function will use. (Keeping `parsed` as the
  // input shape so callers don't need to change.)
  const sasUrl = 'https://' + parsed.accountName + '.blob.core.windows.net/'
    + encodeURIComponent(parsed.containerName) + '?' + parsed.sasToken;

  let resp;
  try {
    resp = await fetch(BLOB_PROXY_BASE + '/blob-list?code=' + encodeURIComponent(BLOB_PROXY_CODE), {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-user-id': userId,
      },
      body: JSON.stringify({ sasUrl }),
    });
  } catch (e){
    return { ok:false, error:'Could not reach Cygenix proxy. Check your internet connection. (' + (e && e.message || e) + ')' };
  }

  let payload = {};
  try { payload = await resp.json(); } catch {}
  if (!resp.ok){
    const msg = payload && payload.error ? payload.error : ('HTTP ' + resp.status);
    return { ok:false, error:'List failed: ' + msg };
  }
  const blobs = Array.isArray(payload.blobs) ? payload.blobs : [];
  return { ok:true, blobs };
}

// Format bytes for display.
function blobSourceFormatSize(b){
  if (!b && b !== 0) return '—';
  if (b < 1024) return b + ' B';
  if (b < 1024 * 1024) return (b / 1024).toFixed(1) + ' KB';
  if (b < 1024 * 1024 * 1024) return (b / 1024 / 1024).toFixed(1) + ' MB';
  return (b / 1024 / 1024 / 1024).toFixed(2) + ' GB';
}

// Filter to file types impHandleFile actually accepts.
function blobSourceIsImportable(name){
  return /\.(csv|tsv|txt|xlsx|xls)$/i.test(name || '');
}

// Backup files SQL Server can restore from — the ones worth offering a
// shortcut into the Restore tab for.
function blobSourceIsBackup(name){
  return /\.(bak|trn|dif)$/i.test(name || '');
}

// The blob's path inside its container, unencoded — what a person reads and
// what azcopy takes. Trailing slashes go for the same reason the azcopy
// builder strips them: some upload tools list files with one, and a trailing
// slash makes the name read as a directory.
function blobSourceRelativePath(blobName){
  return String(blobName || '').replace(/\/+$/, '');
}

// Encode a blob path for a URL: each segment encoded, the virtual-directory
// slashes left alone.
//
// encodeURI is NOT usable here. It leaves '?' and '#' intact, and a filename
// containing either would truncate the URL at the query or fragment boundary —
// silently handing the user a link to the wrong blob, or to no blob at all.
function blobSourceEncodePath(blobName){
  return blobSourceRelativePath(blobName).split('/').map(encodeURIComponent).join('/');
}

// The blob's URL — deliberately WITHOUT the SAS token.
//
// The Restore tab wants a bare URL because SQL Server holds the credential
// server-side, and a SAS on the clipboard is a live credential that leaks
// into tickets, chat messages and screen shares. The azcopy command is the
// single exception: it authenticates with nothing else.
function blobSourceBlobUrl(blobName){
  const parsed = BlobSourceState.activeParsed;
  if (!parsed) return '';
  return 'https://' + parsed.accountName + '.blob.core.windows.net/'
    + encodeURIComponent(parsed.containerName) + '/' + blobSourceEncodePath(blobName);
}

// Copy text, wherever the browser allows it. navigator.clipboard needs a
// secure context and a user gesture; on an http:// origin or an older browser
// it is simply absent, and the textarea fallback keeps copy working there.
async function blobSourceCopyText(text){
  try { await navigator.clipboard.writeText(text); return true; }
  catch {}
  try {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    const copied = document.execCommand('copy');
    document.body.removeChild(ta);
    return copied;
  } catch { return false; }
}

// Transient confirmation in the button itself, so the feedback lands on the
// row the user clicked rather than in a status line somewhere else. Nothing
// re-renders the table on a copy, so nothing races the restore.
function blobSourceFlashCopied(btn){
  if (!btn) return;
  if (btn._copyTimer) clearTimeout(btn._copyTimer);
  else btn._copyLabel = btn.innerHTML;
  btn.innerHTML = '✓ Copied';
  btn._copyTimer = setTimeout(() => {
    btn.innerHTML = btn._copyLabel;
    btn._copyTimer = null;
  }, 1400);
}

// Copy one of the two values the row offers. `which` is 'url' (the full blob
// URL, the primary) or 'path' (the container-relative path).
async function blobSourceCopyBlobPath(btn, blobNameEncoded, which){
  if (!BlobSourceState.activeParsed) return;
  const blobName = decodeURIComponent(blobNameEncoded);
  const text = which === 'path'
    ? blobSourceRelativePath(blobName)
    : blobSourceBlobUrl(blobName);
  if (!text) return;
  const copied = await blobSourceCopyText(text);
  if (copied) blobSourceFlashCopied(btn);
  // Clipboard refused — surface the value so it can be copied by hand, the
  // same fallback the azcopy button uses.
  else { try { window.prompt('Copy this:', text); } catch {} }
}

// Hand a backup blob to the Restore database tab: switch, pick the Azure Blob
// URL source, fill the path in. Nothing else.
function blobSourceUseInRestore(blobNameEncoded){
  if (!BlobSourceState.activeParsed){ alert('No blob source is open.'); return; }
  const url = blobSourceBlobUrl(decodeURIComponent(blobNameEncoded));
  if (!url) return;

  // Switch first: switchConnTab('restore') runs rstInit(), which calls
  // rstModeChanged() off the radio as it stands. Setting the radio and the
  // value afterwards, then calling rstModeChanged() again, leaves the panel
  // describing the source we actually chose.
  try { switchConnTab('restore'); }
  catch (e){ console.warn('[blob-source] could not switch tab:', e); }

  const radio = document.querySelector('input[name="rst-mode"][value="url"]');
  if (radio) radio.checked = true;
  const input = document.getElementById('rst-path');
  if (input) input.value = url;
  try { rstModeChanged(); }
  catch (e){ console.warn('[blob-source] rstModeChanged failed:', e); }

  // Prefill ONLY. Inspect reads the backup header and Run restore writes to
  // the server; both stay the user's decision, and rstModeChanged has just
  // re-disabled Run until the plan is validated again.
  if (input){
    input.focus();
    if (typeof input.scrollIntoView === 'function'){
      input.scrollIntoView({ behavior:'smooth', block:'center' });
    }
  }
}

// One POST to the Cygenix proxy with a JSON body and a JSON answer — the
// shape blob-list, blob-delete and blob-rename all share. Never throws:
// callers get { ok:false, error } and put it on screen.
async function blobSourceProxyJson(action, payload){
  const userId = blobSourceCurrentUserId();
  if (!userId) return { ok:false, error:'You need to be signed in.' };
  const parsed = BlobSourceState.activeParsed;
  if (!parsed) return { ok:false, error:'No blob source is open.' };
  const sasUrl = 'https://' + parsed.accountName + '.blob.core.windows.net/'
    + encodeURIComponent(parsed.containerName) + '?' + parsed.sasToken;

  let resp;
  try {
    resp = await fetch(BLOB_PROXY_BASE + '/' + action + '?code=' + encodeURIComponent(BLOB_PROXY_CODE), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-user-id': userId },
      body: JSON.stringify(Object.assign({ sasUrl }, payload || {})),
    });
  } catch (e){
    return { ok:false, error:'Could not reach the Cygenix proxy. (' + (e && e.message || e) + ')' };
  }
  let data = {};
  try { data = await resp.json(); } catch {}
  if (!resp.ok) return { ok:false, error:(data && data.error) || ('HTTP ' + resp.status), status: resp.status };
  return Object.assign({ ok:true }, data);
}

// ── Rename ─────────────────────────────────────────────────────────────────
// Azure Blob Storage HAS NO RENAME: the proxy does a server-side copy and
// then deletes the original, copy first so a failure leaves a duplicate
// rather than nothing. The editor is inline in the row — the current path is
// what you edit, which beats a prompt() that makes you retype it.
function blobSourceStartRename(blobNameEncoded){
  const perms = blobUploadPerms();
  if (perms && !perms.unknown && !(perms.canUpload && perms.delete)){
    alert('This SAS cannot rename files.\n\nA rename is a copy plus a delete, so it needs Create/Write and Delete '
      + '(sp=…cwd…). This one grants "' + perms.raw + '".');
    return;
  }
  BlobSourceState.renaming = decodeURIComponent(blobNameEncoded);
  blobSourceRenderFiles();
  const input = document.getElementById('blob-rename-input');
  if (input){
    input.focus();
    // Select the file's own name, leaving any folder prefix alone — renaming
    // usually means the last segment, and moving it is still possible by
    // editing the rest.
    const cut = input.value.lastIndexOf('/');
    const dot = input.value.lastIndexOf('.');
    input.setSelectionRange(cut + 1, dot > cut ? dot : input.value.length);
  }
}

function blobSourceCancelRename(){
  BlobSourceState.renaming = '';
  blobSourceRenderFiles();
}

// Enter commits, Escape cancels — a text field in a table row should behave
// like every other one.
function blobSourceRenameKey(ev, blobNameEncoded){
  if (ev.key === 'Enter'){ ev.preventDefault(); blobSourceCommitRename(blobNameEncoded); }
  else if (ev.key === 'Escape'){ ev.preventDefault(); blobSourceCancelRename(); }
}

async function blobSourceCommitRename(blobNameEncoded){
  const oldName = decodeURIComponent(blobNameEncoded);
  const input = document.getElementById('blob-rename-input');
  const newName = String(input ? input.value : '').trim();
  const resultEl = document.getElementById('blob-browser-result');
  const say = (msg, colour) => {
    if (!resultEl) return;
    resultEl.textContent = msg;
    resultEl.style.color = colour || 'var(--text3)';
  };

  if (!newName || newName === oldName){ blobSourceCancelRename(); return; }
  if (newName.includes('..') || newName.includes('\\') || newName.startsWith('/') || newName.endsWith('/')){
    say('That name is not valid: no "..", no backslashes, no leading or trailing slash.', 'var(--red)');
    return;
  }
  // The listing can be stale, which is why the proxy also sends a
  // precondition — but catching it here saves a round trip and says so
  // without the user losing what they typed.
  if ((BlobSourceState.activeBlobs || []).some(b => b.name === newName)){
    say('"' + newName + '" already exists in this container. Pick a different name.', 'var(--red)');
    return;
  }

  say('Renaming — Azure copies the blob, then removes the original…');
  const r = await blobSourceProxyJson('blob-rename', { blobName: oldName, newName });

  if (!r.ok){
    say('Rename failed: ' + r.error + ' The original is untouched.', 'var(--red)');
    return;
  }
  BlobSourceState.renaming = '';
  await blobSourceReloadFiles();
  // A partial rename — copied but the original still there — is reported as
  // the amber half-success it is, not as done.
  if (r.renamed) say('✓ Renamed to ' + newName + '.', 'var(--green)');
  else say('' + (r.message || 'The rename did not finish.'), 'var(--amber)');
}

// ── Delete ─────────────────────────────────────────────────────────────────
// Irreversible from here, in somebody else's storage account. One explicit
// confirmation naming the file and the container, and no claim about
// recoverability we cannot back: whether the bytes survive depends on that
// account's soft-delete setting, which a container SAS cannot see.
async function blobSourceDeleteFile(blobNameEncoded){
  const blobName = decodeURIComponent(blobNameEncoded);
  const parsed = BlobSourceState.activeParsed;
  if (!parsed){ alert('No blob source is open.'); return; }

  const perms = blobUploadPerms();
  if (perms && !perms.unknown && !perms.delete){
    alert('This SAS cannot delete files.\n\nDeleting needs the Delete permission (sp=…d…). '
      + 'This one grants "' + perms.raw + '".');
    return;
  }

  if (!confirm('Delete "' + blobName + '"?\n\nFrom ' + parsed.accountName + '/' + parsed.containerName
    + '\n\nCygenix cannot undo this. If the storage account has soft delete enabled, Azure may retain '
    + 'the file for its retention window; if not, it is gone.')) return;

  const resultEl = document.getElementById('blob-browser-result');
  if (resultEl){ resultEl.textContent = 'Deleting ' + blobName + '…'; resultEl.style.color = 'var(--text3)'; }

  const r = await blobSourceProxyJson('blob-delete', { blobName });
  if (!r.ok){
    if (resultEl){ resultEl.textContent = 'Delete failed: ' + r.error; resultEl.style.color = 'var(--red)'; }
    return;
  }
  await blobSourceReloadFiles();
  if (resultEl){
    resultEl.textContent = r.alreadyGone
      ? '✓ "' + blobName + '" was already gone.'
      : '✓ Deleted ' + blobName + '.';
    resultEl.style.color = 'var(--green)';
  }
}

// What a SAS is allowed to do, read off its `sp=` parameter. Azure's letters:
// r read, w write, l list, c create, d delete, a add.
//
// Upload needs create OR write — `c` alone covers a new blob, `w` is required
// to replace one. Reading this in the browser lets the UI say "your SAS is
// read-only, ask for one with Create and Write" before the user picks files,
// rather than after a 403 comes back from Azure.
//
// `sp` is absent from user-delegation SAS variants and some older tokens.
// Unknown is not denied: we let the attempt run and report what Azure says,
// because guessing "no" would block a SAS that actually works.
function blobSourceSasPerms(sasToken){
  let sp = '';
  try { sp = new URLSearchParams(String(sasToken || '')).get('sp') || ''; } catch {}
  const has = (ch) => sp.toLowerCase().includes(ch);
  const unknown = !sp;
  return {
    raw: sp, unknown,
    read: has('r'), write: has('w'), list: has('l'),
    create: has('c'), delete: has('d'), add: has('a'),
    canUpload:    unknown || has('c') || has('w'),
    canOverwrite: unknown || has('w'),
  };
}

// ── UI: form actions ────────────────────────────────────────────────────────

function blobSourceClearForm(){
  const n = document.getElementById('blob-add-name');
  const s = document.getElementById('blob-add-sas');
  const r = document.getElementById('blob-add-result');
  if (n) n.value = '';
  if (s) s.value = '';
  if (r){ r.textContent = ''; r.style.color = ''; }
}

function blobSourceSetAddResult(msg, kind){
  const el = document.getElementById('blob-add-result');
  if (!el) return;
  el.textContent = msg || '';
  if (kind === 'err')      el.style.color = 'var(--red)';
  else if (kind === 'ok')  el.style.color = 'var(--green)';
  else                     el.style.color = 'var(--text3)';
}

// Test & save: validates the SAS, calls list-container as a real-world probe,
// and (if it works) persists the source. We deliberately list rather than just
// HEAD the container — list is the actual permission we need at use time.
async function blobSourceTest(){
  const nameEl = document.getElementById('blob-add-name');
  const sasEl  = document.getElementById('blob-add-sas');
  const name = (nameEl?.value || '').trim();
  const sasUrl = (sasEl?.value || '').trim();

  if (!name){
    blobSourceSetAddResult('Give this source a friendly name first.', 'err');
    return;
  }
  if (!sasUrl){
    blobSourceSetAddResult('Paste the container SAS URL.', 'err');
    return;
  }
  const parsed = blobSourceParseSas(sasUrl);
  if (!parsed.ok){
    blobSourceSetAddResult(parsed.error, 'err');
    return;
  }

  // Refuse duplicate names — confusing in the saved-list and likely a mistake.
  const existing = blobSourceReadAll();
  if (existing.some(e => (e.name || '').toLowerCase() === name.toLowerCase())){
    blobSourceSetAddResult('A saved source with that name already exists. Pick another name or delete the existing one first.', 'err');
    return;
  }

  blobSourceSetAddResult('Testing — calling list-container on ' + parsed.accountName + '/' + parsed.containerName + '…');
  const result = await blobSourceListContainer(parsed);
  if (!result.ok){
    blobSourceSetAddResult('' + result.error, 'err');
    return;
  }

  // Success — save.
  const entry = {
    id: 'blob_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7),
    name,
    sasUrl,
    accountName: parsed.accountName,
    containerName: parsed.containerName,
    sasExpiresAt: parsed.sasExpiresAt,
    createdAt: new Date().toISOString(),
  };
  const next = existing.concat([entry]);
  const written = blobSourceWriteAll(next);
  if (!written){
    blobSourceSetAddResult('Connected, but failed to save locally (storage quota or signed out?).', 'err');
    return;
  }

  const importable = result.blobs.filter(b => blobSourceIsImportable(b.name)).length;
  blobSourceSetAddResult('✓ Saved. ' + result.blobs.length + ' blob(s) in container, ' + importable + ' importable (CSV/TSV/TXT/XLSX/XLS).', 'ok');
  blobSourceClearFormSoft();
  blobSourceRenderSaved();
}

// Clear name/SAS but keep the result line — used after a successful save so the
// user sees the confirmation.
function blobSourceClearFormSoft(){
  const n = document.getElementById('blob-add-name');
  const s = document.getElementById('blob-add-sas');
  if (n) n.value = '';
  if (s) s.value = '';
}

// ── UI: saved chips ──────────────────────────────────────────────────────────

function blobSourceRenderSaved(){
  const list = document.getElementById('blob-saved-list');
  const count = document.getElementById('blob-saved-count');
  if (!list) return;
  const entries = blobSourceReadAll();
  if (count) count.textContent = String(entries.length);
  if (entries.length === 0){
    list.innerHTML = '<div class="sconn-empty" style="font-size:11px;color:var(--text3);font-style:italic">No saved blob sources yet. Add one above.</div>';
    return;
  }
  list.innerHTML = entries.map(e => {
    const expiry = e.sasExpiresAt ? new Date(e.sasExpiresAt) : null;
    const now = new Date();
    let expiryHtml = '';
    if (expiry){
      const days = Math.round((expiry.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));
      if (days < 0){
        expiryHtml = '<span style="color:var(--red);font-family:var(--mono);font-size:10.5px">SAS expired ' + Math.abs(days) + 'd ago</span>';
      } else if (days <= 7){
        expiryHtml = '<span style="color:var(--amber);font-family:var(--mono);font-size:10.5px">SAS expires in ' + days + 'd</span>';
      } else {
        expiryHtml = '<span style="color:var(--text3);font-family:var(--mono);font-size:10.5px">SAS expires ' + expiry.toISOString().slice(0,10) + '</span>';
      }
    } else {
      expiryHtml = '<span style="color:var(--text3);font-family:var(--mono);font-size:10.5px">SAS expiry unknown</span>';
    }
    // Whether this SAS can be uploaded through, shown on the chip so the
    // answer is visible before the container is opened.
    const p = blobSourceParseSas(e.sasUrl);
    const perms = p.ok ? blobSourceSasPerms(p.sasToken) : null;
    const permHtml = (perms && !perms.unknown)
      ? (perms.canUpload
          ? '<span style="color:var(--green);font-family:var(--mono);font-size:10.5px">read + write</span>'
          : '<span style="color:var(--text3);font-family:var(--mono);font-size:10.5px">read-only</span>')
      : '';

    return ''
      + '<div style="display:flex;align-items:center;gap:0.75rem;padding:0.6rem 0.75rem;background:var(--bg3);border:0.5px solid var(--border);border-radius:var(--r)">'
      +   '<div style="flex:1;min-width:0">'
      +     '<div style="font-size:12.5px;font-weight:500;color:var(--text);margin-bottom:2px">' + escP(e.name) + '</div>'
      +     '<div style="font-family:var(--mono);font-size:10.5px;color:var(--text3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'
      +       escP(e.accountName) + ' / ' + escP(e.containerName) + ' &nbsp;·&nbsp; ' + expiryHtml
      +       (permHtml ? ' &nbsp;·&nbsp; ' + permHtml : '')
      +     '</div>'
      +   '</div>'
      +   '<button class="btn btn-sm" style="background:var(--accent-glow);color:var(--accent);border:0.5px solid rgba(74,91,214,0.35)" onclick="blobSourceOpenBrowser(\'' + e.id + '\')"><i class="ic ic-folder-open"></i> Browse files</button>'
      +   '<button class="btn btn-ghost btn-sm" style="color:var(--red)" onclick="blobSourceDelete(\'' + e.id + '\')" title="Remove this saved source">✕</button>'
      + '</div>';
  }).join('');
}

function blobSourceDelete(id){
  if (!id) return;
  if (BlobUploadState.running && BlobSourceState.activeId === id){
    alert('An upload to this source is still running. Wait for it to finish first.');
    return;
  }
  const entries = blobSourceReadAll();
  const target = entries.find(e => e.id === id);
  if (!target) return;
  if (!confirm('Remove saved blob source "' + target.name + '"?\n\nThis only removes the saved SAS from this browser. The client\'s storage account is unaffected.')) return;
  const next = entries.filter(e => e.id !== id);
  blobSourceWriteAll(next);
  // If the active browser is showing this source, close it.
  if (BlobSourceState.activeId === id) blobSourceCloseBrowser();
  blobSourceRenderSaved();
}

// ── UI: file browser ────────────────────────────────────────────────────────

// Module-level state for the open browser panel — what source we're showing
// and the parsed SAS so the download buttons don't need to re-parse.
const BlobSourceState = {
  activeId: '',
  activeParsed: null,
  activeBlobs: [],
  // Sorted list (importable first, alphabetic within group). Filter+render
  // works over this; we never re-fetch or re-sort on keystroke.
  orderedBlobs: [],
  // Current type filter chip. One of: 'all' | 'csv' | 'excel' | 'text'.
  typeFilter: 'all',
  // Blob name currently open in the inline rename editor, '' when none.
  renaming: '',
};

async function blobSourceOpenBrowser(id){
  const entries = blobSourceReadAll();
  const entry = entries.find(e => e.id === id);
  if (!entry){
    alert('That saved source no longer exists.');
    blobSourceRenderSaved();
    return;
  }
  const parsed = blobSourceParseSas(entry.sasUrl);
  if (!parsed.ok){
    alert('Saved SAS is invalid: ' + parsed.error);
    return;
  }
  BlobSourceState.activeId = id;
  BlobSourceState.activeParsed = parsed;
  BlobSourceState.activeBlobs = [];
  BlobSourceState.renaming = '';
  // A queue carried over from another container would upload the previous
  // source's files into this one.
  BlobUploadState.queue = [];
  blobUploadSetStatus('');

  const wrap = document.getElementById('blob-browser-wrap');
  const nameEl = document.getElementById('blob-browser-source-name');
  const metaEl = document.getElementById('blob-browser-meta');
  const resultEl = document.getElementById('blob-browser-result');
  const tbody = document.querySelector('#blob-files-table tbody');
  if (wrap) wrap.style.display = 'block';
  if (nameEl) nameEl.textContent = entry.name;
  if (metaEl) metaEl.textContent = parsed.accountName + ' / ' + parsed.containerName;
  if (resultEl){ resultEl.textContent = 'Listing files…'; resultEl.style.color = 'var(--text3)'; }
  if (tbody) tbody.innerHTML = '';

  // Upload panel: available as soon as the container is open, so an empty
  // container is somewhere you can put files rather than a dead end.
  blobUploadWireDrop();
  blobUploadSyncPanel();

  await blobSourceReloadFiles();

  // Scroll into view so the user sees the new panel without hunting.
  if (wrap && typeof wrap.scrollIntoView === 'function'){
    wrap.scrollIntoView({ behavior:'smooth', block:'nearest' });
  }
}

async function blobSourceReloadFiles(){
  if (!BlobSourceState.activeParsed) return;
  const resultEl = document.getElementById('blob-browser-result');
  const tbody = document.querySelector('#blob-files-table tbody');
  const filterBar = document.getElementById('blob-filter-bar');
  if (resultEl){ resultEl.textContent = 'Listing files…'; resultEl.style.color = 'var(--text3)'; }
  if (tbody) tbody.innerHTML = '';
  if (filterBar) filterBar.style.display = 'none';

  const r = await blobSourceListContainer(BlobSourceState.activeParsed);
  if (!r.ok){
    if (resultEl){ resultEl.textContent = r.error; resultEl.style.color = 'var(--red)'; }
    return;
  }
  BlobSourceState.activeBlobs = r.blobs;
  const importable = r.blobs.filter(b => blobSourceIsImportable(b.name));
  const rest = r.blobs.filter(b => !blobSourceIsImportable(b.name));

  if (r.blobs.length === 0){
    if (resultEl){ resultEl.textContent = 'Container is empty.'; resultEl.style.color = 'var(--text3)'; }
    return;
  }
  if (resultEl){
    resultEl.textContent = '✓ ' + r.blobs.length + ' blob(s) — ' + importable.length + ' importable, ' + rest.length + ' other.';
    resultEl.style.color = 'var(--text3)';
  }

  // Sort once at fetch time: importable first, alphabetic within each group.
  // Render-time filtering works over this ordered list, so we never re-sort
  // on keystroke.
  importable.sort((a,b) => a.name.localeCompare(b.name));
  rest.sort((a,b) => a.name.localeCompare(b.name));
  BlobSourceState.orderedBlobs = importable.concat(rest);

  // Reveal filter toolbar now that there's something to filter.
  if (filterBar) filterBar.style.display = 'flex';
  // Reset filter inputs on each fresh fetch — user may have switched sources.
  const searchEl = document.getElementById('blob-filter-search');
  if (searchEl) searchEl.value = '';
  BlobSourceState.typeFilter = 'all';
  // Update chip active state to match reset filter.
  document.querySelectorAll('#blob-filter-bar .blob-type-chip').forEach(el => {
    el.classList.toggle('blob-type-chip-active', el.dataset.type === 'all');
  });

  blobSourceRenderFiles();
}

// Render the file table from BlobSourceState.orderedBlobs, applying the
// current search-text and type-filter. Called on initial fetch and on every
// filter change. Pure DOM work — no network. Cheap enough that we don't
// debounce the search input.
function blobSourceRenderFiles(){
  const tbody = document.querySelector('#blob-files-table tbody');
  const countEl = document.getElementById('blob-filter-count');
  const searchEl = document.getElementById('blob-filter-search');
  if (!tbody) return;

  const all = BlobSourceState.orderedBlobs || [];
  const searchText = (searchEl?.value || '').trim().toLowerCase();
  const typeFilter = BlobSourceState.typeFilter || 'all';

  // Type filter — only constrains the *type* of file the import column shows
  // as actionable. Filtered-out files are hidden entirely (not just greyed)
  // because users picking "CSV" don't want to scroll past .bak files.
  function matchesType(name){
    if (typeFilter === 'all')   return true;
    if (typeFilter === 'csv')   return /\.csv$/i.test(name);
    if (typeFilter === 'excel') return /\.(xlsx|xls)$/i.test(name);
    if (typeFilter === 'text')  return /\.(tsv|txt)$/i.test(name);
    return true;
  }
  // Search — case-insensitive substring match against the full path.
  function matchesSearch(name){
    if (!searchText) return true;
    return name.toLowerCase().includes(searchText);
  }

  const filtered = all.filter(b => matchesType(b.name) && matchesSearch(b.name));

  if (countEl){
    if (filtered.length === all.length){
      countEl.textContent = all.length + ' file' + (all.length === 1 ? '' : 's');
    } else {
      countEl.textContent = 'Showing ' + filtered.length + ' of ' + all.length;
    }
  }

  if (filtered.length === 0){
    tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;padding:1.5rem;color:var(--text3);font-style:italic">No files match the current filter.</td></tr>';
    return;
  }

  // Server-side cap mirrors the Function's MAX_BYTES — anything above this
  // would 413 on attempt, so we surface it inline rather than letting the
  // user click and wait for a failed download.
  const MAX_BYTES = 100 * 1024 * 1024;

  // Read once per render, not once per row: what this SAS allows decides
  // whether Rename and Delete are live on every row alike.
  const perms = blobUploadPerms();

  tbody.innerHTML = filtered.map(b => {
    const usable = blobSourceIsImportable(b.name);
    const tooBig = (b.size || 0) > MAX_BYTES;
    const lastMod = b.lastModified ? new Date(b.lastModified) : null;
    const lastModStr = lastMod && !isNaN(lastMod.getTime())
      ? lastMod.toISOString().slice(0, 16).replace('T', ' ')
      : '—';

    // Action buttons: Import (importable, ≤ proxy cap), Download (any file
    // ≤ proxy cap), or "azcopy command" (files over the cap — proxy can't
    // handle them, so we hand the user a one-line command they can paste
    // into PowerShell/Terminal to download direct from Azure).
    //
    // encodeURIComponent leaves an apostrophe alone, and these values sit
    // inside single-quoted JS string literals in an onclick attribute — one
    // file called "it's.bak" would break every button on its row. %27 decodes
    // back to the same character on the far side.
    const enc = encodeURIComponent(b.name).replace(/'/g, '%27');
    const importBtn = (usable && !tooBig)
      ? '<button class="btn btn-sm" style="background:var(--accent-glow);color:var(--accent);border:0.5px solid rgba(74,91,214,0.35)" onclick="blobSourceImportFile(\'' + enc + '\')"><i class="ic ic-upload"></i> Import</button>'
      : '';
    const downloadBtn = !tooBig
      ? '<button class="btn btn-ghost btn-sm" onclick="blobSourceDownloadFile(\'' + enc + '\')" title="Download to your machine"><i class="ic ic-download"></i> Download</button>'
      : '<button class="btn btn-sm" style="background:rgba(245,158,11,0.12);color:var(--amber);border:0.5px solid rgba(245,158,11,0.35)" onclick="blobSourceCopyAzcopy(\'' + enc + '\')" title="File is over 100MB. Click to copy an azcopy command to your clipboard — paste into PowerShell or Terminal to download directly from Azure."><i class="ic ic-clipboard"></i> Copy azcopy</button>';

    // Straight into the Restore tab for the file types SQL Server restores
    // from. Not gated on the 100MB cap: nothing transfers through us here —
    // the server pulls the blob itself — so this is the ONLY route that works
    // for the multi-GB .bak files that are the whole point of the tab.
    const restoreBtn = blobSourceIsBackup(b.name)
      ? '<button class="btn btn-sm" style="background:var(--accent-glow);color:var(--accent);border:0.5px solid rgba(74,91,214,0.35)"'
        + ' onclick="blobSourceUseInRestore(\'' + enc + '\')"'
        + ' title="Open the Restore database tab with this blob URL filled in. Prefills only — nothing runs.">Use in restore →</button>'
      : '';

    // Copy controls. Neither value carries the SAS token — see
    // blobSourceBlobUrl. Full URL leads because it is what the Restore tab and
    // the Azure tooling want, and it is the one nobody can retype from memory.
    const copyBtns =
        '<button class="btn btn-ghost btn-sm" onclick="blobSourceCopyBlobPath(this,\'' + enc + '\',\'url\')"'
      + ' title="Copy the full blob URL — https://account.blob.core.windows.net/container/path, no SAS token">⧉ Copy URL</button>'
      + '<button class="btn btn-ghost btn-sm" onclick="blobSourceCopyBlobPath(this,\'' + enc + '\',\'path\')"'
      + ' title="Copy the path inside the container, unencoded — for azcopy and for reading">⧉ Path</button>';

    // Rename and delete write to the client's container. Both are disabled
    // rather than hidden when the SAS cannot do them, carrying the reason —
    // a control that vanishes reads as a broken page. Rename needs
    // Create/Write AND Delete, because Azure has no rename: it is a copy
    // followed by a delete of the original.
    const canRen = !perms || perms.unknown || (perms.canUpload && perms.delete);
    const canDel = !perms || perms.unknown || perms.delete;
    const manageBtns =
        '<button class="btn btn-ghost btn-sm"' + (canRen ? '' : ' disabled')
      + ' onclick="blobSourceStartRename(\'' + enc + '\')"'
      + ' title="' + (canRen
          ? 'Rename this blob. Azure has no rename — Cygenix copies it to the new name, then deletes the original.'
          : 'This SAS cannot rename: a rename needs Create/Write and Delete (sp=…cwd…).') + '"><i class="ic ic-edit"></i> Rename</button>'
      + '<button class="btn btn-ghost btn-sm" style="color:var(--red)"' + (canDel ? '' : ' disabled')
      + ' onclick="blobSourceDeleteFile(\'' + enc + '\')"'
      + ' aria-label="Delete this file"'
      + ' title="' + (canDel
          ? 'Delete this blob from the container. Cygenix cannot undo it.'
          : 'This SAS cannot delete: it needs the Delete permission (sp=…d…).') + '"><i class="ic ic-trash"></i> </button>';

    const actionCell = '<div style="display:inline-flex;gap:0.4rem;align-items:center;justify-content:flex-end;flex-wrap:wrap">'
      + importBtn + restoreBtn + downloadBtn + copyBtns + manageBtns + '</div>';

    // The row being renamed swaps its name cell for an editor and its actions
    // for save/cancel, so there is never a live Delete beside a half-typed
    // name. The full path is editable: renaming the last segment is the
    // common case, moving between folders is the same operation to Azure.
    if (BlobSourceState.renaming === b.name){
      return ''
        + '<tr>'
        +   '<td colspan="3">'
        +     '<input id="blob-rename-input" class="form-input" style="width:100%;font-family:var(--mono);font-size:11.5px"'
        +       ' value="' + escP(b.name) + '" aria-label="New blob name"'
        +       ' onkeydown="blobSourceRenameKey(event,\'' + enc + '\')">'
        +   '</td>'
        +   '<td style="text-align:right;white-space:nowrap">'
        +     '<button class="btn btn-sm" style="background:var(--accent-glow);color:var(--accent);border:0.5px solid rgba(74,91,214,0.35)"'
        +       ' onclick="blobSourceCommitRename(\'' + enc + '\')">✓ Save</button> '
        +     '<button class="btn btn-ghost btn-sm" onclick="blobSourceCancelRename()">✕ Cancel</button>'
        +   '</td>'
        + '</tr>';
    }

    // Rows for non-importable files are no longer dimmed — they're still
    // useful (downloadable) so full opacity is correct.
    return ''
      + '<tr>'
      +   '<td>' + escP(b.name) + '</td>'
      +   '<td>' + blobSourceFormatSize(b.size) + '</td>'
      +   '<td>' + lastModStr + '</td>'
      +   '<td style="text-align:right">' + actionCell + '</td>'
      + '</tr>';
  }).join('');
}

// Click handler for the type chips. Updates state + chip visual + re-renders.
function blobSourceSetTypeFilter(type){
  BlobSourceState.typeFilter = type;
  document.querySelectorAll('#blob-filter-bar .blob-type-chip').forEach(el => {
    el.classList.toggle('blob-type-chip-active', el.dataset.type === type);
  });
  blobSourceRenderFiles();
}

function blobSourceCloseBrowser(){
  // Closing mid-upload would leave the run writing into a container the UI no
  // longer shows, with no way to watch it finish.
  if (BlobUploadState.running){
    blobUploadSetStatus('Upload in progress — wait for it to finish before closing.', 'warn');
    return;
  }
  BlobSourceState.activeId = '';
  BlobSourceState.activeParsed = null;
  BlobSourceState.activeBlobs = [];
  BlobSourceState.orderedBlobs = [];
  BlobSourceState.typeFilter = 'all';
  BlobSourceState.renaming = '';
  BlobUploadState.queue = [];
  blobUploadSetStatus('');
  blobUploadRenderQueue();
  const wrap = document.getElementById('blob-browser-wrap');
  if (wrap) wrap.style.display = 'none';
}

// Fetch a single blob and hand it to the existing Data Import pipeline. The
// import lands the user on the Data import tab with the schema preview already
// rendered — same UX as a drag-drop, just sourced from blob.
async function blobSourceImportFile(blobNameEncoded){
  if (!BlobSourceState.activeParsed){
    alert('No blob source is open.');
    return;
  }
  const blobName = decodeURIComponent(blobNameEncoded);
  const resultEl = document.getElementById('blob-browser-result');
  if (resultEl){ resultEl.textContent = 'Downloading ' + blobName + ' via Cygenix proxy…'; resultEl.style.color = 'var(--text3)'; }

  const userId = blobSourceCurrentUserId();
  if (!userId){
    if (resultEl){
      resultEl.textContent = 'You need to be signed in to download files.';
      resultEl.style.color = 'var(--red)';
    }
    return;
  }

  // Reconstruct the SAS URL from the parsed shape — same approach as
  // blobSourceListContainer.
  const parsed = BlobSourceState.activeParsed;
  const sasUrl = 'https://' + parsed.accountName + '.blob.core.windows.net/'
    + encodeURIComponent(parsed.containerName) + '?' + parsed.sasToken;

  let resp;
  try {
    resp = await fetch(BLOB_PROXY_BASE + '/blob-download?code=' + encodeURIComponent(BLOB_PROXY_CODE), {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-user-id': userId,
      },
      body: JSON.stringify({ sasUrl, blobName }),
    });
  } catch (e){
    if (resultEl){
      resultEl.textContent = 'Could not reach Cygenix proxy. Check your internet connection. (' + (e && e.message || e) + ')';
      resultEl.style.color = 'var(--red)';
    }
    return;
  }

  if (!resp.ok){
    // Function returns JSON error payloads on non-200; binary body only on 200.
    let msg = 'HTTP ' + resp.status;
    try {
      const payload = await resp.json();
      if (payload && payload.error) msg = payload.error;
    } catch {}
    if (resp.status === 413){
      msg = 'File too large for proxy (max 100MB). ' + msg;
    }
    if (resultEl){
      resultEl.textContent = 'Download failed: ' + msg;
      resultEl.style.color = 'var(--red)';
    }
    return;
  }

  let arrayBuf;
  try { arrayBuf = await resp.arrayBuffer(); }
  catch (e){
    if (resultEl){
      resultEl.textContent = 'Failed to read download body: ' + (e && e.message || e);
      resultEl.style.color = 'var(--red)';
    }
    return;
  }

  // File extends Blob with a `name` property — `impHandleFile` reads file.name
  // to decide CSV vs Excel, so we must construct a real File. Strip any
  // trailing slash defensively: some Azure listings include blob names
  // ending with '/' which would yield an empty filename.
  const cleanName = blobName.replace(/\/+$/, '');
  const justName = cleanName.split('/').pop() || cleanName;
  let file;
  try {
    file = new File([arrayBuf], justName, { type: resp.headers.get('Content-Type') || '' });
  } catch {
    // Older Safari: File constructor missing. Fall back to a plain Blob with a
    // name field tacked on — impHandleFile only ever reads .name and .size.
    const b = new Blob([arrayBuf], { type: resp.headers.get('Content-Type') || '' });
    try { Object.defineProperty(b, 'name', { value: justName }); } catch {}
    file = b;
  }

  if (resultEl){
    resultEl.textContent = '✓ Downloaded ' + justName + ' (' + blobSourceFormatSize(file.size) + '). Handing off to Data Import…';
    resultEl.style.color = 'var(--green)';
  }

  // Switch tab so the user sees the schema preview render.
  try { switchConnTab('import'); } catch(e){ console.warn('[blob-source] could not switch tab:', e); }
  // Kick off the existing parse pipeline.
  try { impHandleFile(file); }
  catch (e){
    console.error('[blob-source] impHandleFile threw:', e);
    alert('Import pipeline failed: ' + (e && e.message || e));
  }
}

// Fetch a single blob via the proxy and trigger a browser download. Same
// endpoint and same 100MB cap as the import path — we just terminate
// differently (object URL + anchor click instead of impHandleFile). Works
// for any file type, importable or not: .bak, .zip, .sql are all valid.
async function blobSourceDownloadFile(blobNameEncoded){
  if (!BlobSourceState.activeParsed){
    alert('No blob source is open.');
    return;
  }
  const blobName = decodeURIComponent(blobNameEncoded);
  const resultEl = document.getElementById('blob-browser-result');
  if (resultEl){ resultEl.textContent = 'Downloading ' + blobName + ' via Cygenix proxy…'; resultEl.style.color = 'var(--text3)'; }

  const userId = blobSourceCurrentUserId();
  if (!userId){
    if (resultEl){
      resultEl.textContent = 'You need to be signed in to download files.';
      resultEl.style.color = 'var(--red)';
    }
    return;
  }

  const parsed = BlobSourceState.activeParsed;
  const sasUrl = 'https://' + parsed.accountName + '.blob.core.windows.net/'
    + encodeURIComponent(parsed.containerName) + '?' + parsed.sasToken;

  let resp;
  try {
    resp = await fetch(BLOB_PROXY_BASE + '/blob-download?code=' + encodeURIComponent(BLOB_PROXY_CODE), {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-user-id': userId,
      },
      body: JSON.stringify({ sasUrl, blobName }),
    });
  } catch (e){
    if (resultEl){
      resultEl.textContent = 'Could not reach Cygenix proxy. (' + (e && e.message || e) + ')';
      resultEl.style.color = 'var(--red)';
    }
    return;
  }

  if (!resp.ok){
    // Function returns JSON error payloads on non-200.
    let msg = 'HTTP ' + resp.status;
    try {
      const payload = await resp.json();
      if (payload && payload.error) msg = payload.error;
    } catch {}
    if (resp.status === 413){
      msg = 'File too large for proxy (max 100MB). ' + msg;
    }
    if (resultEl){
      resultEl.textContent = 'Download failed: ' + msg;
      resultEl.style.color = 'var(--red)';
    }
    return;
  }

  let arrayBuf;
  try { arrayBuf = await resp.arrayBuffer(); }
  catch (e){
    if (resultEl){
      resultEl.textContent = 'Failed to read download body: ' + (e && e.message || e);
      resultEl.style.color = 'var(--red)';
    }
    return;
  }

  // Build a Blob, an object URL, and a hidden anchor with `download="..."`
  // so the browser saves the file with its original name (last segment of
  // the path) rather than the default proxy URL filename. Strip any
  // trailing slash defensively — some Azure listings include blob names
  // ending with '/' which would yield an empty filename.
  const cleanName = blobName.replace(/\/+$/, '');
  const justName = cleanName.split('/').pop() || cleanName;
  const blob = new Blob([arrayBuf], { type: resp.headers.get('Content-Type') || 'application/octet-stream' });
  const url = URL.createObjectURL(blob);
  try {
    const a = document.createElement('a');
    a.href = url;
    a.download = justName;
    a.style.display = 'none';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
  } finally {
    // Revoke the object URL after a tick so the browser has time to start the
    // download. Holding it indefinitely would pin the bytes in memory.
    setTimeout(() => { try { URL.revokeObjectURL(url); } catch {} }, 2000);
  }

  if (resultEl){
    resultEl.textContent = '✓ Downloaded ' + justName + ' (' + blobSourceFormatSize(blob.size) + ').';
    resultEl.style.color = 'var(--green)';
  }
}

// Build an azcopy command for a single blob and copy it to the clipboard.
// Used for files over the proxy cap (100MB) — instead of failing, we give
// the user a one-line command they can paste into PowerShell/Terminal to
// download direct from Azure. azcopy is far faster than browser/proxy for
// large files anyway (multi-threaded, resumable), so this is genuinely a
// better path for .bak files and similar.
//
// The command uses the SAS URL verbatim — same credential the user already
// provided. azcopy is free from Microsoft: https://aka.ms/downloadazcopy
async function blobSourceCopyAzcopy(blobNameEncoded){
  if (!BlobSourceState.activeParsed){
    alert('No blob source is open.');
    return;
  }
  const blobName = decodeURIComponent(blobNameEncoded);
  const parsed = BlobSourceState.activeParsed;

  // Some Azure listings include blob names with a trailing slash (artefact
  // of certain upload tools). Trailing slashes make azcopy interpret the
  // target as a directory and fail with "cannot use directory as source
  // without --recursive". blobSourceRelativePath strips them — real files
  // never end in '/' — and blobSourceBlobUrl encodes each segment so
  // virtual-directory slashes survive but spaces do not.
  const cleanName = blobSourceRelativePath(blobName);

  // The one place the SAS belongs on the clipboard: azcopy has no other way
  // to authenticate. The Copy URL / Copy Path buttons never include it.
  const fileUrl = blobSourceBlobUrl(cleanName) + '?' + parsed.sasToken;

  // Quote the URL with double-quotes — works in both PowerShell and bash.
  // Destination "." downloads to the user's current working directory.
  const cmd = 'azcopy copy "' + fileUrl + '" "."';

  const resultEl = document.getElementById('blob-browser-result');
  // Shared with the Copy URL / Copy Path buttons: Clipboard API first, then
  // the textarea + execCommand path for insecure origins and older browsers.
  const copied = await blobSourceCopyText(cmd);

  if (resultEl){
    if (copied){
      resultEl.innerHTML = '✓ azcopy command copied. Paste into PowerShell or Terminal to download <strong>' + escP(cleanName.split('/').pop() || cleanName) + '</strong>. '
        + 'Need azcopy? <a href="https://aka.ms/downloadazcopy" target="_blank" rel="noopener" style="color:var(--accent)">Download from Microsoft</a>.';
      resultEl.style.color = 'var(--green)';
    } else {
      // Clipboard refused — surface the command in a prompt so the user
      // can copy it manually.
      resultEl.textContent = 'Could not access clipboard. Command shown in popup — copy manually.';
      resultEl.style.color = 'var(--amber)';
      try { window.prompt('Copy this azcopy command:', cmd); } catch {}
    }
  }
}

// ── UPLOAD ──────────────────────────────────────────────────────────────────
// Files go up through the same Cygenix proxy the download path uses, for the
// same reason: the client's storage account almost certainly has no CORS rule
// for cygenix.co.uk, and we reach these clients through SaaS without a channel
// to their Azure admins. The proxy PUTs server-to-server.
//
// Two things are deliberate, because this writes into somebody else's storage
// account and a lost file there is not an error anyone gets to retry:
//
//   * Overwrite is opt-in, and the guarantee is enforced by Azure, not by us —
//     the proxy sends `If-None-Match: *` unless overwrite is ticked, so a blob
//     created between our listing and the PUT still cannot be clobbered.
//   * A SAS that cannot write says so before the user picks anything, and the
//     Upload button stays visibly disabled with the reason attached, rather
//     than disappearing (a missing control reads as a broken page).
//
// Progress comes from XMLHttpRequest rather than fetch: fetch still has no
// upload-progress event, and a 100MB file with no feedback looks like a hang.
const BlobUploadState = {
  queue: [],        // [{ id, file, status, pct, error }]
  running: false,
};

let _blobUploadSeq = 0;

function blobUploadPerms(){
  const parsed = BlobSourceState.activeParsed;
  return parsed ? blobSourceSasPerms(parsed.sasToken) : null;
}

// Show or hide the whole upload panel and describe what the SAS allows.
// Called whenever a source is opened.
function blobUploadSyncPanel(){
  const panel = document.getElementById('blob-upload-panel');
  const permEl = document.getElementById('blob-upload-perm');
  if (!panel) return;
  const parsed = BlobSourceState.activeParsed;
  if (!parsed){ panel.style.display = 'none'; return; }
  panel.style.display = 'block';

  const perms = blobUploadPerms();
  if (permEl){
    if (!perms || perms.unknown){
      permEl.textContent = 'SAS permissions not declared — upload will be attempted and Azure has the final word.';
      permEl.style.color = 'var(--text3)';
    } else if (!perms.canUpload){
      permEl.textContent = 'This SAS is read-only (sp=' + perms.raw + ') — ask for one with Create and Write to upload.';
      permEl.style.color = 'var(--amber)';
    } else if (!perms.canOverwrite){
      permEl.textContent = 'sp=' + perms.raw + ' — can create new files, cannot replace existing ones.';
      permEl.style.color = 'var(--text3)';
    } else {
      permEl.textContent = 'sp=' + perms.raw + ' — create and replace allowed.';
      permEl.style.color = 'var(--text3)';
    }
  }
  blobUploadRenderQueue();
}

function blobUploadToggle(){
  const body = document.getElementById('blob-upload-body');
  const btn  = document.getElementById('blob-upload-toggle');
  if (!body) return;
  const hidden = body.style.display === 'none';
  body.style.display = hidden ? 'block' : 'none';
  if (btn) btn.textContent = hidden ? 'Hide' : 'Show';
}

// Accept files from the picker or a drop. Appends rather than replaces, so a
// second drop adds to the queue instead of discarding the first.
function blobUploadPick(fileList){
  const files = Array.from(fileList || []);
  if (!files.length) return;
  for (const f of files){
    BlobUploadState.queue.push({
      id: 'up_' + (++_blobUploadSeq),
      file: f,
      status: 'queued',   // queued | uploading | done | error | skipped
      pct: 0,
      error: '',
    });
  }
  // Let the same file be picked twice in a row — the input keeps its value
  // otherwise and onchange never fires again.
  const input = document.getElementById('blob-upload-input');
  if (input) input.value = '';
  blobUploadRenderQueue();
}

function blobUploadClearQueue(){
  if (BlobUploadState.running) return;
  BlobUploadState.queue = [];
  const st = document.getElementById('blob-upload-status');
  if (st) st.textContent = '';
  blobUploadRenderQueue();
}

// The destination folder, normalised: no leading slash, exactly one trailing
// slash, '..' refused outright (the proxy refuses it too, but a message here
// beats a 400 after the upload starts).
function blobUploadPrefix(){
  let p = (document.getElementById('blob-upload-prefix')?.value || '').trim();
  if (!p) return { ok:true, prefix:'' };
  p = p.replace(/\\/g, '/').replace(/^\/+/, '').replace(/\/+$/, '');
  if (!p) return { ok:true, prefix:'' };
  if (p.split('/').some(seg => seg === '..')){
    return { ok:false, error:'Destination folder cannot contain "..".' };
  }
  return { ok:true, prefix: p + '/' };
}

function blobUploadTargetName(item){
  const pre = blobUploadPrefix();
  const base = (item.file.name || 'upload').split(/[\\/]/).pop();
  return (pre.ok ? pre.prefix : '') + base;
}

const BLOB_UPLOAD_MAX = 100 * 1024 * 1024;

function blobUploadRenderQueue(){
  const host = document.getElementById('blob-upload-queue');
  const go   = document.getElementById('blob-upload-go');
  if (!host) return;

  const q = BlobUploadState.queue;
  host.style.display = q.length ? 'flex' : 'none';

  const perms = blobUploadPerms();
  const overwrite = !!document.getElementById('blob-upload-overwrite')?.checked;
  // Names already in the container — used to warn before a collision rather
  // than after one. The listing can be stale, which is exactly why the proxy
  // also sends a precondition.
  const existing = new Set((BlobSourceState.activeBlobs || []).map(b => b.name));

  host.innerHTML = q.map(item => {
    const target = blobUploadTargetName(item);
    const tooBig = item.file.size > BLOB_UPLOAD_MAX;
    const clash  = existing.has(target);

    let note = '', noteColour = 'var(--text3)';
    if (item.status === 'error'){ note = item.error; noteColour = 'var(--red)'; }
    else if (item.status === 'done'){ note = 'uploaded'; noteColour = 'var(--green)'; }
    else if (item.status === 'skipped'){ note = item.error || 'skipped'; noteColour = 'var(--amber)'; }
    else if (tooBig){ note = 'over 100MB — use azcopy for this one'; noteColour = 'var(--red)'; }
    else if (clash && overwrite){ note = 'will REPLACE the existing file'; noteColour = 'var(--amber)'; }
    else if (clash){ note = 'already exists — tick Overwrite to replace'; noteColour = 'var(--amber)'; }
    else if (item.status === 'uploading'){ note = item.pct + '%'; }

    const bar = item.status === 'uploading'
      ? '<div style="height:3px;background:var(--bg4);border-radius:2px;overflow:hidden;margin-top:4px">'
        + '<div style="height:100%;width:' + item.pct + '%;background:var(--accent);transition:width 0.2s"></div></div>'
      : '';

    const icon = item.status === 'done' ? '✓'
               : item.status === 'error' ? ''
               : item.status === 'skipped' ? ''
               : item.status === 'uploading' ? '' : '·';

    return ''
      + '<div style="display:flex;align-items:center;gap:0.6rem;padding:0.45rem 0.6rem;background:var(--bg2);border:0.5px solid var(--border);border-radius:var(--r)">'
      +   '<span style="font-size:11px;width:14px;text-align:center">' + icon + '</span>'
      +   '<div style="flex:1;min-width:0">'
      +     '<div style="font-family:var(--mono);font-size:11px;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">' + escP(target) + '</div>'
      +     (note ? '<div style="font-size:10.5px;color:' + noteColour + '">' + escP(note) + '</div>' : '')
      +     bar
      +   '</div>'
      +   '<span style="font-family:var(--mono);font-size:10.5px;color:var(--text3);white-space:nowrap">' + blobSourceFormatSize(item.file.size) + '</span>'
      +   (BlobUploadState.running ? ''
          : '<button class="btn btn-ghost btn-sm" style="color:var(--red);padding:2px 6px" onclick="blobUploadRemove(\'' + item.id + '\')" title="Remove from queue" aria-label="Remove from queue">✕</button>')
      + '</div>';
  }).join('');

  // The button carries its own reason for being disabled — a control that is
  // dead with no explanation is the thing users file bugs about.
  if (go){
    const pending = q.filter(i => i.status === 'queued' || i.status === 'error').length;
    let reason = '';
    if (BlobUploadState.running) reason = 'Upload in progress…';
    else if (!pending) reason = q.length ? 'Nothing left to upload.' : 'Choose files first.';
    else if (perms && !perms.canUpload) reason = 'This SAS cannot write. Ask for one with Create and Write permission.';
    else if (overwrite && perms && !perms.canOverwrite) reason = 'This SAS cannot replace existing files (needs Write).';
    go.disabled = !!reason;
    go.title = reason || ('Upload ' + pending + ' file' + (pending === 1 ? '' : 's'));
    go.textContent = pending > 1 ? ('Upload ' + pending + ' files') : 'Upload';
  }
}

function blobUploadRemove(id){
  if (BlobUploadState.running) return;
  BlobUploadState.queue = BlobUploadState.queue.filter(i => i.id !== id);
  blobUploadRenderQueue();
}

function blobUploadSetStatus(msg, kind){
  const el = document.getElementById('blob-upload-status');
  if (!el) return;
  el.textContent = msg || '';
  el.style.color = kind === 'err' ? 'var(--red)'
                 : kind === 'ok'  ? 'var(--green)'
                 : kind === 'warn'? 'var(--amber)' : 'var(--text3)';
}

// One file, one PUT through the proxy. XHR because it reports upload
// progress; fetch does not. Resolves { ok, error, status } — never rejects,
// so one bad file cannot abandon the rest of the queue.
function blobUploadOne(item, sasUrl, userId, overwrite){
  return new Promise((resolve) => {
    const target = blobUploadTargetName(item);
    const xhr = new XMLHttpRequest();
    xhr.open('POST', BLOB_PROXY_BASE + '/blob-upload?code=' + encodeURIComponent(BLOB_PROXY_CODE), true);
    xhr.setRequestHeader('x-user-id', userId);
    xhr.setRequestHeader('x-blob-sas', sasUrl);
    // Header values must be latin-1; unicode filenames survive as percent-
    // encoding, which the proxy decodes.
    xhr.setRequestHeader('x-blob-name', encodeURIComponent(target));
    xhr.setRequestHeader('x-blob-overwrite', overwrite ? '1' : '0');
    xhr.setRequestHeader('Content-Type', item.file.type || 'application/octet-stream');

    xhr.upload.onprogress = (e) => {
      if (!e.lengthComputable) return;
      item.pct = Math.min(99, Math.round((e.loaded / e.total) * 100));
      blobUploadRenderQueue();
    };
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300){
        item.pct = 100;
        resolve({ ok: true });
        return;
      }
      let msg = 'HTTP ' + xhr.status;
      try {
        const payload = JSON.parse(xhr.responseText || '{}');
        if (payload && payload.error) msg = payload.error;
      } catch {}
      resolve({ ok: false, error: msg, status: xhr.status });
    };
    xhr.onerror = () => resolve({ ok: false, error: 'Network error reaching the Cygenix proxy.' });
    xhr.ontimeout = () => resolve({ ok: false, error: 'Upload timed out.' });
    // A 100MB file over a slow line legitimately takes minutes.
    xhr.timeout = 15 * 60 * 1000;
    xhr.send(item.file);
  });
}

// Run the queue one file at a time. Sequential rather than parallel: the
// proxy buffers each file whole, and several 100MB uploads at once is how a
// Function App runs out of memory.
async function blobUploadStart(){
  if (BlobUploadState.running) return;
  const parsed = BlobSourceState.activeParsed;
  if (!parsed){ alert('No blob source is open.'); return; }

  const pre = blobUploadPrefix();
  if (!pre.ok){ blobUploadSetStatus('' + pre.error, 'err'); return; }

  const userId = blobSourceCurrentUserId();
  if (!userId){ blobUploadSetStatus('You need to be signed in to upload.', 'err'); return; }

  const overwrite = !!document.getElementById('blob-upload-overwrite')?.checked;
  const pending = BlobUploadState.queue.filter(i => i.status === 'queued' || i.status === 'error');
  if (!pending.length){ blobUploadSetStatus('Nothing to upload.', 'warn'); return; }

  // Replacing a client's file is worth one explicit confirmation, naming the
  // files that would be destroyed.
  if (overwrite){
    const existing = new Set((BlobSourceState.activeBlobs || []).map(b => b.name));
    const clashes = pending.map(blobUploadTargetName).filter(n => existing.has(n));
    if (clashes.length){
      const list = clashes.slice(0, 8).join('\n  ') + (clashes.length > 8 ? '\n  …and ' + (clashes.length - 8) + ' more' : '');
      if (!confirm('Overwrite ' + clashes.length + ' existing file' + (clashes.length === 1 ? '' : 's')
        + ' in ' + parsed.accountName + '/' + parsed.containerName + '?\n\n  ' + list
        + '\n\nThe current contents will be replaced and cannot be recovered by Cygenix.')) return;
    }
  }

  const sasUrl = 'https://' + parsed.accountName + '.blob.core.windows.net/'
    + encodeURIComponent(parsed.containerName) + '?' + parsed.sasToken;

  BlobUploadState.running = true;
  blobUploadRenderQueue();

  let done = 0, failed = 0;
  for (const item of pending){
    if (item.file.size > BLOB_UPLOAD_MAX){
      item.status = 'skipped';
      item.error = 'over the 100MB proxy limit — use azcopy for this file';
      failed++;
      blobUploadRenderQueue();
      continue;
    }
    item.status = 'uploading';
    item.pct = 0;
    item.error = '';
    blobUploadSetStatus('Uploading ' + blobUploadTargetName(item) + ' (' + (done + failed + 1) + ' of ' + pending.length + ')…');
    blobUploadRenderQueue();

    const r = await blobUploadOne(item, sasUrl, userId, overwrite);
    if (r.ok){ item.status = 'done'; done++; }
    else {
      // 409 is "already there", which is the expected answer to a
      // non-overwriting upload — not a failure of the tool.
      item.status = r.status === 409 ? 'skipped' : 'error';
      item.error = r.error || 'Upload failed';
      failed++;
    }
    blobUploadRenderQueue();
  }

  BlobUploadState.running = false;

  if (done && !failed)      blobUploadSetStatus('✓ Uploaded ' + done + ' file' + (done === 1 ? '' : 's') + '.', 'ok');
  else if (done && failed)  blobUploadSetStatus('✓ ' + done + ' uploaded · ' + failed + ' not uploaded — see the list.', 'warn');
  else                      blobUploadSetStatus('Nothing uploaded — see the list for why.', 'err');

  // Refresh the listing so the new files appear (and so a later overwrite
  // check is made against reality rather than a pre-upload snapshot).
  if (done) await blobSourceReloadFiles();
  blobUploadRenderQueue();
}

// Drag and drop onto the upload zone. Wired on first browser open rather than
// inline, so the handlers can preventDefault on dragover — without that the
// browser navigates away to the dropped file and the page is simply gone.
let _blobDropWired = false;
function blobUploadWireDrop(){
  if (_blobDropWired) return;
  const zone = document.getElementById('blob-upload-drop');
  if (!zone) return;
  _blobDropWired = true;
  const on = (el, ev, fn) => el.addEventListener(ev, fn);
  on(zone, 'dragover', (e) => {
    e.preventDefault();
    zone.style.background = 'var(--accent-glow)';
    zone.style.borderColor = 'var(--accent)';
  });
  on(zone, 'dragleave', () => {
    zone.style.background = '';
    zone.style.borderColor = '';
  });
  on(zone, 'drop', (e) => {
    e.preventDefault();
    zone.style.background = '';
    zone.style.borderColor = '';
    const files = e.dataTransfer && e.dataTransfer.files;
    if (files && files.length) blobUploadPick(files);
  });
  // Keyboard route to the same picker — the zone is a button, so Enter and
  // Space must do what a click does.
  on(zone, 'keydown', (e) => {
    if (e.key === 'Enter' || e.key === ' '){
      e.preventDefault();
      document.getElementById('blob-upload-input')?.click();
    }
  });
}

// Expose for inline onclick handlers — same defensive pattern used elsewhere
// in this file because function declarations don't reliably hoist across
// <script> block boundaries.
if (typeof window !== 'undefined'){
  window.blobUploadToggle       = blobUploadToggle;
  window.blobUploadPick         = blobUploadPick;
  window.blobUploadStart        = blobUploadStart;
  window.blobUploadClearQueue   = blobUploadClearQueue;
  window.blobUploadRemove       = blobUploadRemove;
  window.blobUploadRenderQueue  = blobUploadRenderQueue;
  window.blobSourceTest          = blobSourceTest;
  window.blobSourceClearForm     = blobSourceClearForm;
  window.blobSourceRenderSaved   = blobSourceRenderSaved;
  window.blobSourceOpenBrowser   = blobSourceOpenBrowser;
  window.blobSourceReloadFiles   = blobSourceReloadFiles;
  window.blobSourceCloseBrowser  = blobSourceCloseBrowser;
  window.blobSourceDelete        = blobSourceDelete;
  window.blobSourceImportFile    = blobSourceImportFile;
  window.blobSourceDownloadFile   = blobSourceDownloadFile;
  window.blobSourceCopyAzcopy     = blobSourceCopyAzcopy;
  window.blobSourceRenderFiles    = blobSourceRenderFiles;
  window.blobSourceSetTypeFilter  = blobSourceSetTypeFilter;
  window.blobSourceCopyBlobPath   = blobSourceCopyBlobPath;
  window.blobSourceUseInRestore   = blobSourceUseInRestore;
  window.blobSourceStartRename    = blobSourceStartRename;
  window.blobSourceCancelRename   = blobSourceCancelRename;
  window.blobSourceCommitRename   = blobSourceCommitRename;
  window.blobSourceRenameKey      = blobSourceRenameKey;
  window.blobSourceDeleteFile     = blobSourceDeleteFile;
}

function impDragOver(e){ e.preventDefault(); document.getElementById('imp-drop')?.classList.add('drag-over'); }
function impDragLeave(e){ document.getElementById('imp-drop')?.classList.remove('drag-over'); }
function impDrop(e){
  e.preventDefault();
  document.getElementById('imp-drop')?.classList.remove('drag-over');
  const files = e.dataTransfer?.files;
  if (files && files.length) impHandleFiles(files);
}

// Router: single file → existing schema-preview flow; multiple files → batch flow.
// Called from both the file input's onchange and impDrop.
function impHandleFiles(fileList){
  if (!fileList || fileList.length === 0) return;
  if (fileList.length === 1){
    // Make sure the batch panel is hidden in case it was left open from a prior batch
    const bw = document.getElementById('imp-batch-wrap');
    if (bw) bw.style.display = 'none';
    impHandleFile(fileList[0]);
  } else {
    // Hide the single-file schema/target panels so the UI focuses on the batch view
    const sw = document.getElementById('imp-schema-wrap');
    const tw = document.getElementById('imp-target-wrap');
    if (sw) sw.style.display = 'none';
    if (tw) tw.style.display = 'none';
    impBatchPrepare(Array.from(fileList));
  }
}

function impHandleFile(file){
  if (!file) return;
  const isCSV   = /\.(csv|tsv|txt)$/i.test(file.name);
  const isExcel = /\.(xlsx|xls)$/i.test(file.name);
  if (!isCSV && !isExcel){
    impShowError('Only CSV, TSV, TXT, XLSX, or XLS files are supported in this release.');
    return;
  }
  // CSV is streamed in passes, so we can handle up to ~1GB. Excel must be loaded
  // whole into memory by the parser, so we stay at 100MB to avoid browser OOM.
  if (isCSV && file.size > 1024 * 1024 * 1024){  // 1 GB
    impShowError('CSV file is over 1GB. That exceeds what the browser can stream reliably.');
    return;
  }
  if (isExcel && file.size > 100 * 1024 * 1024){  // 100 MB
    impShowError('Excel file is over 100MB. Save as CSV first — CSV files can be much larger.');
    return;
  }
  impState.fileName = file.name;

  const meta = document.getElementById('imp-file-meta');
  if (meta){
    meta.style.display = 'block';
    meta.style.color = '';
    meta.innerHTML = `<i class="ic ic-hourglass"></i> Parsing <strong>${escP(file.name)}</strong> (${(file.size/1024).toFixed(1)} KB)…`;
  }

  if (isCSV){
    impParseCSV(file);
  } else {
    impParseExcel(file);
  }
}

// CSV parser — two modes:
//   - In-memory (legacy): file size suggests <= IMP_STREAMING_THRESHOLD rows,
//     so we load everything, run schema inference, and hand to impFinalizeRows.
//     This preserves the existing validation-modal UX for small files.
//   - Streaming: large file. We stream pass-1 with Papa.parse({ step }), keep
//     the first 500 rows for inference and count the rest. impState.rows stays
//     null; the import-time path will re-stream the file in pass-2.
//
// File-size heuristic for picking the mode: most CSVs run ~150-300 bytes/row,
// so 10MB is a safe lower bound for files likely under 50k rows. Above that we
// stream regardless; we'll find out the exact row count during pass-1.
function impParseCSV(file){
  if (typeof Papa === 'undefined'){
    impShowError('CSV parser failed to load. Check your internet connection and reload the page.');
    return;
  }

  const SIZE_HEURISTIC = 10 * 1024 * 1024;  // 10 MB
  if (file.size <= SIZE_HEURISTIC){
    // Try in-memory mode first. If the row count comes back above the threshold
    // (we couldn't tell from file size alone — could be a CSV with tiny rows),
    // we'll fall through to streaming inside impFinalizeRows.
    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      transformHeader: h => String(h || '').trim(),
      complete: (result) => {
        if (result.errors && result.errors.length){
          console.warn('[data-import] CSV parse warnings:', result.errors.slice(0, 5));
        }
        const rows = result.data || [];
        if (rows.length > IMP_STREAMING_THRESHOLD){
          // Surprise — small file but lots of rows. Discard the in-memory array
          // and switch to streaming so we get the right UX/limits.
          console.log('[data-import] Small file but ' + rows.length + ' rows — switching to streaming mode.');
          impParseCSVStreaming(file);
        } else {
          impFinalizeRows(file.name, rows);
        }
      },
      error: (err) => {
        impShowError('CSV parse failed: ' + (err?.message || String(err)));
      }
    });
  } else {
    impParseCSVStreaming(file);
  }
}

// Pass-1 streaming parse: collect the first IMP_INFER_SAMPLE rows for schema
// inference, then keep streaming purely to count the total. Stops the parser
// once we have both (a) enough sample and (b) the rest of the file consumed.
function impParseCSVStreaming(file){
  const IMP_INFER_SAMPLE = 500;
  const sampleRows = [];
  let totalRows = 0;
  let cols = null;
  let abortedEarly = false;

  const meta = document.getElementById('imp-file-meta');
  if (meta){
    meta.innerHTML = `<i class="ic ic-hourglass"></i> Streaming <strong>${escP(file.name)}</strong> (${(file.size/1024/1024).toFixed(1)} MB) — counting rows…`;
  }

  // Throttled progress updates so we don't thrash the DOM on huge files.
  let lastProgressUpdate = 0;

  Papa.parse(file, {
    header: true,
    skipEmptyLines: true,
    dynamicTyping: false,
    transformHeader: h => String(h || '').trim(),
    step: (row, parser) => {
      if (!cols){
        // Capture column order from the first row Papa hands us
        cols = Object.keys(row.data || {}).filter(k => k != null && k !== '');
        if (cols.length === 0){
          parser.abort();
          abortedEarly = true;
          impShowError('No column headers detected. Make sure the first row contains header names.');
          return;
        }
      }
      if (sampleRows.length < IMP_INFER_SAMPLE){
        sampleRows.push(row.data);
      }
      totalRows++;
      if (totalRows > IMP_CSV_HARD_CAP){
        parser.abort();
        abortedEarly = true;
        impShowError(`File exceeds ${IMP_CSV_HARD_CAP.toLocaleString()} rows. Split the file or contact support.`);
        return;
      }
      const now = Date.now();
      if (now - lastProgressUpdate > 250 && meta){
        lastProgressUpdate = now;
        meta.innerHTML = `<i class="ic ic-hourglass"></i> Streaming <strong>${escP(file.name)}</strong> — ${totalRows.toLocaleString()} rows counted so far…`;
      }
    },
    complete: () => {
      if (abortedEarly) return;
      if (totalRows === 0){
        impShowError('The file appears to be empty or has no data rows.');
        return;
      }
      // Mark streaming mode and stash the file for pass-2 during import.
      impState.streaming     = true;
      impState.streamingFile = file;
      impState.rows          = null;
      impState.fileName      = file.name;
      impState.rowCount      = totalRows;
      impState.columns       = impInferSchema(cols, sampleRows);

      if (meta){
        meta.innerHTML = `✓ Streamed <strong>${escP(file.name)}</strong> — ${totalRows.toLocaleString()} rows, ${cols.length} columns (large file — data will be streamed at import time)`;
      }
      impRenderSchemaTable();
      document.getElementById('imp-schema-wrap').style.display = 'block';
      document.getElementById('imp-target-wrap').style.display = 'block';

      const stem = file.name.replace(/\.(csv|tsv|txt|xlsx|xls)$/i, '').replace(/[^A-Za-z0-9_]/g, '_');
      document.getElementById('imp-target-table').value = 'dbo.' + stem;
    },
    error: (err) => {
      impShowError('CSV stream failed: ' + (err?.message || String(err)));
    }
  });
}

function impParseExcel(file){
  if (typeof XLSX === 'undefined'){
    impShowError('Excel parser failed to load. Check your internet connection and reload the page.');
    return;
  }
  const reader = new FileReader();
  reader.onerror = () => impShowError('Failed to read file.');
  reader.onload = (ev) => {
    try {
      const data = new Uint8Array(ev.target.result);
      const wb = XLSX.read(data, { type:'array', cellDates:true });
      const sheets = wb.SheetNames || [];
      if (sheets.length === 0){
        impShowError('Workbook has no sheets.');
        return;
      }
      if (sheets.length === 1){
        impExtractSheet(file.name, wb, sheets[0]);
      } else {
        // Prompt the user to pick a sheet
        impOpenSheetModal(file.name, wb, sheets);
      }
    } catch (e) {
      impShowError('Excel parse failed: ' + (e?.message || String(e)));
    }
  };
  reader.readAsArrayBuffer(file);
}

function impOpenSheetModal(fileName, wb, sheets){
  const list = document.getElementById('imp-sheet-list');
  if (!list) return;
  list.innerHTML = sheets.map((name, idx) => {
    // Best-effort row count preview for each sheet
    const ws = wb.Sheets[name];
    let rowHint = '';
    try {
      const ref = ws && ws['!ref'];
      if (ref){
        const range = XLSX.utils.decode_range(ref);
        const rowCount = Math.max(0, range.e.r - range.s.r); // minus header
        const colCount = range.e.c - range.s.c + 1;
        rowHint = `${rowCount.toLocaleString()} rows · ${colCount} columns`;
      }
    } catch {}
    return `<div class="imp-exists-row" data-sheet-idx="${idx}">
      <div style="font-size:18px"><i class="ic ic-chart"></i> </div>
      <div>
        <strong>${escP(name)}</strong>
        <span>${rowHint || 'Click to import this sheet'}</span>
      </div>
    </div>`;
  }).join('');
  // Wire clicks
  list.querySelectorAll('.imp-exists-row').forEach(row => {
    row.addEventListener('click', () => {
      const idx = parseInt(row.dataset.sheetIdx, 10);
      document.getElementById('imp-sheet-modal').classList.remove('open');
      impExtractSheet(fileName, wb, sheets[idx]);
    });
  });
  document.getElementById('imp-sheet-modal').classList.add('open');
}
function impCloseSheetModal(){
  document.getElementById('imp-sheet-modal').classList.remove('open');
  impReset();
}

function impExtractSheet(fileName, wb, sheetName){
  const ws = wb.Sheets[sheetName];
  // defval:'' treats empty cells as empty strings so downstream null-detection works
  // raw:false converts cells to formatted strings (matching what the user sees in Excel)
  const rows = XLSX.utils.sheet_to_json(ws, { defval:'', raw:false, blankrows:false });
  // Track sheet name in fileName so the default table name includes it when multi-sheet
  const displayName = wb.SheetNames.length > 1 ? `${fileName} [${sheetName}]` : fileName;
  impFinalizeRows(displayName, rows, { sheetName, multiSheet: wb.SheetNames.length > 1 });
}

// Shared post-parse logic: row validation, schema inference, UI render.
// Used by Excel (any size, up to IMP_XLSX_HARD_CAP) and small CSVs (in-memory
// mode, under IMP_STREAMING_THRESHOLD). Large CSVs use impParseCSVStreaming
// instead and never hit this function.
function impFinalizeRows(fileName, rows, opts){
  opts = opts || {};
  if (rows.length === 0){
    impShowError('The file appears to be empty or has no data rows.');
    return;
  }
  if (rows.length > IMP_XLSX_HARD_CAP){
    impShowError('File has ' + rows.length.toLocaleString() + ' rows. Maximum supported is ' + IMP_XLSX_HARD_CAP.toLocaleString() + ' for Excel. Save as CSV to stream larger files.');
    return;
  }

  const firstRow = rows[0];
  const cols = Object.keys(firstRow || {}).filter(k => k != null && k !== '');
  if (cols.length === 0){
    impShowError('No column headers detected. Make sure the first row contains header names.');
    return;
  }

  impState.rows     = rows;
  impState.rowCount = rows.length;
  impState.columns  = impInferSchema(cols, rows);

  const meta = document.getElementById('imp-file-meta');
  if (meta){
    meta.innerHTML = `✓ Parsed <strong>${escP(fileName)}</strong> — ${rows.length.toLocaleString()} rows, ${cols.length} columns`;
  }
  impRenderSchemaTable();
  document.getElementById('imp-schema-wrap').style.display = 'block';
  document.getElementById('imp-target-wrap').style.display = 'block';

  // Default the target-table name. For Excel with a sheet, incorporate the sheet name.
  let stem;
  if (opts.multiSheet){
    const fnStem = fileName.replace(/\s*\[.*\]$/, '').replace(/\.(csv|tsv|txt|xlsx|xls)$/i, '');
    stem = (fnStem + '_' + opts.sheetName).replace(/[^A-Za-z0-9_]/g, '_');
  } else {
    stem = fileName.replace(/\.(csv|tsv|txt|xlsx|xls)$/i, '').replace(/[^A-Za-z0-9_]/g, '_');
  }
  document.getElementById('imp-target-table').value = 'dbo.' + stem;
}

// Type inference over a sample of rows. Returns column descriptors.
// Priority order: all rows empty → string; all parse as int → int;
// all parse as decimal → decimal; all parse as date → date;
// all parse as bool → bool; fallback → string.
// Type catalog — 9 entries, exposed via a dropdown in the schema preview.
// Order matters: this is how they'll appear in the menu.
const IMP_TYPE_CATALOG = [
  { key:'int',        label:'INT',              sql:'INT' },
  { key:'bigint',     label:'BIGINT',           sql:'BIGINT' },
  { key:'decimal',    label:'DECIMAL',          sql:'DECIMAL(18,6)' },
  { key:'money',      label:'MONEY',            sql:'MONEY' },
  { key:'datetime',   label:'DATETIME',         sql:'DATETIME2' },
  { key:'date',       label:'DATE',             sql:'DATE' },
  { key:'bool',       label:'BIT (Yes/No)',     sql:'BIT' },
  { key:'string',     label:'TEXT (auto-sized)',sql:null },  // computed per-column from maxLen
  { key:'max',        label:'TEXT (MAX)',       sql:'NVARCHAR(MAX)' },
];

function impInferSchema(cols, rows){
  const SAMPLE = Math.min(rows.length, 500);
  const isInt     = v => /^-?\d+$/.test(v) && Math.abs(Number(v)) < 2**31;       // INT fits
  const isBigint  = v => /^-?\d+$/.test(v) && Math.abs(Number(v)) < 2**53;       // BIGINT-ish; >INT range
  const isDecimal = v => /^-?\d+(\.\d+)?$/.test(v) || /^-?\.\d+$/.test(v);
  const isDate    = v => {
    if (!/^\d{4}-\d{2}-\d{2}|^\d{2}[\/\-]\d{2}[\/\-]\d{4}/.test(v)) return false;
    const d = new Date(v); return !isNaN(d.getTime());
  };
  const isBool    = v => /^(true|false|yes|no|y|n|1|0)$/i.test(v);
  const isNully   = v => v === null || v === undefined || v === '' || /^(null|n\/a|na|-|—)$/i.test(String(v));

  return cols.map(name => {
    const vals = [];
    let nullCount = 0;
    for (let i = 0; i < SAMPLE; i++){
      const v = rows[i]?.[name];
      if (isNully(v)) { nullCount++; continue; }
      vals.push(String(v).trim());
    }

    const samples = vals.slice(0, 3);
    if (vals.length === 0) return { name, inferredType:'string', chosenType:'string', maxLen:0, nullCount, samples, nullable:true };

    let inferred = 'string';
    if (vals.every(isInt))           inferred = 'int';
    else if (vals.every(isBigint))   inferred = 'bigint';
    else if (vals.every(isDecimal))  inferred = 'decimal';
    else if (vals.every(isDate))     inferred = 'date';    // date-only by default; user can flip to DATETIME
    else if (vals.every(isBool))     inferred = 'bool';

    // Track max length across ALL rows (not just sample) so TEXT auto-sizing is right
    let maxLen = 0;
    for (let i = 0; i < rows.length; i++){
      const v = rows[i]?.[name];
      if (!isNully(v)){
        const len = String(v).length;
        if (len > maxLen) maxLen = len;
        if (maxLen > 4000) break;
      }
    }
    return { name, inferredType:inferred, chosenType:inferred, maxLen, nullCount, samples, nullable:true };
  });
}

function impRenderSchemaTable(){
  const tbl = document.getElementById('imp-schema-table');
  if (!tbl) return;
  const tbody = tbl.querySelector('tbody');
  tbody.innerHTML = impState.columns.map((c, idx) => {
    const dropdown = IMP_TYPE_CATALOG.map(t => {
      const sel = t.key === c.chosenType ? 'selected' : '';
      // For the 'string' option, suffix the auto length so the user sees what they're getting
      const label = t.key === 'string'
        ? `TEXT (${impAutoNvarcharSize(c)})`
        : t.label;
      return `<option value="${t.key}" ${sel}>${label}</option>`;
    }).join('');
    const overridden = c.chosenType !== c.inferredType;
    const cls = 'imp-type-select' + (overridden ? ' overridden' : '');
    const sample = c.samples.length
      ? c.samples.map(s => escP(String(s).length > 30 ? String(s).slice(0,27)+'…' : s)).join(', ')
      : '<span style="color:var(--text3)">—</span>';
    const nullChecked = c.nullable ? 'checked' : '';
    return `<tr>
      <td>${escP(c.name)}</td>
      <td><select class="${cls}" onchange="impSetColType(${idx}, this.value)">${dropdown}</select></td>
      <td style="text-align:center"><input type="checkbox" class="imp-null-check" ${nullChecked} onchange="impSetColNullable(${idx}, this.checked)"></td>
      <td>${sample}</td>
      <td>${c.nullCount > 0 ? c.nullCount + '/500' : '—'}</td>
    </tr>`;
  }).join('');
}

// Per-column auto-size rule for NVARCHAR(n). Rounds up to the next 50 chars,
// with a floor of 50 and a cap of 4000 (beyond that we'd need NVARCHAR(MAX)).
function impAutoNvarcharSize(col){
  if (col.maxLen > 4000) return 'MAX';
  return Math.max(50, Math.ceil(col.maxLen/50)*50);
}

function impSetColType(idx, newType){
  if (!impState.columns || !impState.columns[idx]) return;
  impState.columns[idx].chosenType = newType;
  impRenderSchemaTable();
}

function impSetColNullable(idx, isNullable){
  if (!impState.columns || !impState.columns[idx]) return;
  impState.columns[idx].nullable = isNullable;
}

// "All to TEXT (MAX)" — the nuclear option. Flips every column to NVARCHAR(MAX)
// and re-renders. Good for pathological files where you just want the data IN.
function impAllToMax(){
  if (!impState.columns) return;
  impState.columns.forEach(c => { c.chosenType = 'max'; });
  impRenderSchemaTable();
  impRunResult('All columns set to TEXT (MAX). Type validation will be skipped — values go in as-is.', 'info');
}

// Return the SQL Server type string for a column, honouring the user's override.
function impSqlType(col){
  const t = col.chosenType || col.inferredType;
  if (t === 'string'){
    const size = impAutoNvarcharSize(col);
    return size === 'MAX' ? 'NVARCHAR(MAX)' : `NVARCHAR(${size})`;
  }
  const entry = IMP_TYPE_CATALOG.find(x => x.key === t);
  return entry?.sql || 'NVARCHAR(255)';
}

// Display label for the SQL type (used in a few places that still surface type badges).
function impDisplayType(col){
  return impSqlType(col);
}

function impShowError(msg){
  const meta = document.getElementById('imp-file-meta');
  if (meta){
    meta.style.display = 'block';
    meta.style.color = 'var(--red)';
    meta.innerHTML = escP(msg);
  }
  console.warn('[data-import]', msg);
}

function impReset(){
  impState = { fileName:null, rows:null, columns:null, rowCount:0, streaming:false, streamingFile:null };
  impAbortRequested = false;
  impSetAbortVisible(false);
  const fileInput = document.getElementById('imp-file');
  if (fileInput) fileInput.value = '';
  const meta = document.getElementById('imp-file-meta');
  if (meta){ meta.style.display = 'none'; meta.style.color = ''; meta.innerHTML = ''; }
  document.getElementById('imp-schema-wrap').style.display = 'none';
  document.getElementById('imp-target-wrap').style.display = 'none';
  const result = document.getElementById('imp-run-result');
  if (result){ result.style.display = 'none'; result.textContent = ''; }
  document.getElementById('imp-target-table').value = '';
}

// ── Execution flow ────────────────────────────────────────────────────────

function impRun(){
  // In streaming mode we don't have impState.rows — we have impState.streamingFile.
  // Either is enough to proceed; columns are always required.
  const haveData = impState.streaming ? !!impState.streamingFile : !!impState.rows;
  if (!haveData || !impState.columns){
    impShowError('Parse a file first.');
    return;
  }
  const tableName = (document.getElementById('imp-target-table').value || '').trim();
  if (!tableName){
    impRunResult('Enter a table name.', 'err');
    return;
  }
  if (!/^[A-Za-z0-9_\.\[\]]+$/.test(tableName)){
    impRunResult('Table name can only contain letters, numbers, underscore, dot, and square brackets.', 'err');
    return;
  }

  // Pre-flight: validate rows against chosen column types — only for in-memory
  // mode. Streaming files skip the validation modal because we'd need to load
  // the whole file to count issues. Instead, the streaming insert path auto-
  // coerces bad values to NULL inline (and triggers the same NVARCHAR(MAX)
  // fallback the batch flow uses if SQL Server still rejects the data).
  if (!impState.streaming){
    const issues = impValidateRows();
    if (issues.bad.length > 0){
      impOpenValidateModal(issues, tableName);
      return;
    }
  }

  // No issues → proceed to existence check, then create/overwrite/append flow
  impCheckTableExists(tableName);
}

// Scan every row against every column's chosenType. For each value that
// doesn't fit, record the issue. Returns { bad, byColumn, skipRowSet }.
//  - bad       : array of { rowIdx, col, val, expected } — capped at 100 for UI
//  - byColumn  : { colName: count } summary
//  - skipRowSet: Set<number> of row indices that have at least one bad value
// NVARCHAR(string)/NVARCHAR(MAX) columns are never flagged.
function impValidateRows(){
  const cols = impState.columns;
  const rows = impState.rows;
  const bad = [];
  const byColumn = {};
  const skipRowSet = new Set();
  const MAX_REPORT = 100;

  const fitsType = (v, type) => {
    if (v === null || v === undefined || v === '') return true;  // null is fine
    const s = String(v).trim();
    if (/^(null|n\/a|na|-|—)$/i.test(s)) return true;
    if (type === 'string' || type === 'max') return true;
    if (type === 'int'){
      if (!/^-?\d+$/.test(s)) return false;
      const n = Number(s);
      return Math.abs(n) < 2**31;  // INT range
    }
    if (type === 'bigint'){
      if (!/^-?\d+$/.test(s)) return false;
      return Math.abs(Number(s)) < 2**53;
    }
    if (type === 'decimal'){
      return /^-?\d+(\.\d+)?$/.test(s) || /^-?\.\d+$/.test(s);
    }
    if (type === 'money'){
      const cleaned = s.replace(/[£$€,\s]/g, '');
      return /^-?\d+(\.\d+)?$/.test(cleaned) || /^-?\.\d+$/.test(cleaned);
    }
    if (type === 'bool'){
      return /^(true|false|yes|no|y|n|1|0)$/i.test(s);
    }
    if (type === 'date' || type === 'datetime'){
      const d = new Date(s);
      return !isNaN(d.getTime());
    }
    return true;
  };

  for (let i = 0; i < rows.length; i++){
    const r = rows[i];
    for (const c of cols){
      const v = r[c.name];
      const type = c.chosenType || c.inferredType;
      if (!fitsType(v, type)){
        byColumn[c.name] = (byColumn[c.name] || 0) + 1;
        skipRowSet.add(i);
        if (bad.length < MAX_REPORT){
          bad.push({ rowIdx:i, col:c.name, val:String(v ?? ''), expected:impSqlType(c) });
        }
      }
    }
  }
  return { bad, byColumn, skipRowSet };
}

let _impPendingValidate = null;  // holds { issues, tableName } while modal is open

function impOpenValidateModal(issues, tableName){
  _impPendingValidate = { issues, tableName };
  const byCol = issues.byColumn;
  const summaryParts = Object.keys(byCol).map(name => `<span class="imp-issue-col">${escP(name)}</span>: ${byCol[name].toLocaleString()}`).join(' · ');
  document.getElementById('imp-validate-summary').innerHTML =
    `<strong>${issues.skipRowSet.size.toLocaleString()} rows</strong> have values that don't fit the chosen types. ` +
    `Affected columns: ${summaryParts}.`;

  // Show up to 20 example bad values
  const sample = issues.bad.slice(0, 20).map(b =>
    `row ${b.rowIdx+1} · <span class="imp-issue-col">${escP(b.col)}</span> = <span class="imp-issue-val">${escP(b.val.length > 60 ? b.val.slice(0,57)+'…' : b.val)}</span> (expected ${escP(b.expected)})`
  ).join('<br>');
  document.getElementById('imp-validate-issues').innerHTML = sample +
    (issues.bad.length > 20 ? `<br><span style="color:var(--text3)">…and ${(issues.bad.length - 20).toLocaleString()} more (showing first 20).</span>` : '');

  document.getElementById('imp-validate-modal').classList.add('open');
  impRunResult('Validation found issues. Choose how to proceed.', 'info');
}

function impCloseValidateModal(){
  _impPendingValidate = null;
  document.getElementById('imp-validate-modal').classList.remove('open');
  impRunResult('Cancelled.', 'info');
}

// User clicked one of the three options. Apply the fix, then resume the flow.
function impValidateProceed(mode){
  const pending = _impPendingValidate;
  document.getElementById('imp-validate-modal').classList.remove('open');
  _impPendingValidate = null;
  if (!pending) return;

  const { issues, tableName } = pending;

  if (mode === 'max'){
    // Switch affected columns to NVARCHAR(MAX) and re-validate (which should pass).
    Object.keys(issues.byColumn).forEach(colName => {
      const col = impState.columns.find(c => c.name === colName);
      if (col) col.chosenType = 'max';
    });
    impRenderSchemaTable();
    impRunResult(`Converted ${Object.keys(issues.byColumn).length} column(s) to TEXT (MAX). Retrying import…`, 'info');
    impCheckTableExists(tableName);
    return;
  }

  if (mode === 'skip'){
    // Remove affected rows from impState.rows — this permanently discards them
    // for this import run (user can Start Over to get them back).
    const keep = [];
    for (let i = 0; i < impState.rows.length; i++){
      if (!issues.skipRowSet.has(i)) keep.push(impState.rows[i]);
    }
    const dropped = impState.rows.length - keep.length;
    impState.rows = keep;
    impState.rowCount = keep.length;
    impRunResult(`Skipped ${dropped.toLocaleString()} row(s) with bad values. Proceeding with ${keep.length.toLocaleString()} rows.`, 'info');
    impCheckTableExists(tableName);
    return;
  }

  if (mode === 'null'){
    // Coerce bad values to null in-place by replacing them with empty string,
    // which impLiteral treats as NULL. We don't mutate values that were already fine.
    const badSet = new Map();
    issues.bad.forEach(b => {  // Note: bad is capped at 100; use full scan instead
      badSet.set(b.rowIdx + '/' + b.col, true);
    });
    // Full scan to catch the ones beyond MAX_REPORT
    const cols = impState.columns;
    let patched = 0;
    for (let i = 0; i < impState.rows.length; i++){
      const r = impState.rows[i];
      for (const c of cols){
        const type = c.chosenType || c.inferredType;
        if (type === 'string' || type === 'max') continue;
        const v = r[c.name];
        if (v === null || v === undefined || v === '') continue;
        const s = String(v).trim();
        if (/^(null|n\/a|na|-|—)$/i.test(s)) continue;
        // Re-check fitness inline (small duplication vs impValidateRows)
        let fits = true;
        if (type === 'int') fits = /^-?\d+$/.test(s) && Math.abs(Number(s)) < 2**31;
        else if (type === 'bigint') fits = /^-?\d+$/.test(s) && Math.abs(Number(s)) < 2**53;
        else if (type === 'decimal') fits = /^-?\d+(\.\d+)?$/.test(s) || /^-?\.\d+$/.test(s);
        else if (type === 'money'){ const cl = s.replace(/[£$€,\s]/g,''); fits = /^-?\d+(\.\d+)?$/.test(cl) || /^-?\.\d+$/.test(cl); }
        else if (type === 'bool') fits = /^(true|false|yes|no|y|n|1|0)$/i.test(s);
        else if (type === 'date' || type === 'datetime'){ fits = !isNaN(new Date(s).getTime()); }
        if (!fits){ r[c.name] = ''; patched++; }
      }
    }
    impRunResult(`Converted ${patched.toLocaleString()} bad value(s) to NULL. Proceeding with full row set.`, 'info');
    impCheckTableExists(tableName);
    return;
  }
}

async function impCheckTableExists(tableName){
  impRunResult('Checking if table exists…', 'info');
  const dbChoice = document.getElementById('imp-target-db').value;
  const conn = impGetConn(dbChoice);
  if (!conn){
    const target = dbChoice === 'src' ? 'source' : 'target';
    impRunResult(
      `No ${target} database connection configured. Open the “Database connections” tab above, enter the ${target} connection, click “Connect & test” (the dot should turn green), then press “Save connections”. See console for diagnostics.`,
      'err'
    );
    return;
  }

  // Probe with a harmless SELECT. If it returns, the table exists.
  // Using OBJECT_ID keeps the permission surface small.
  //
  // The result-row key isn't reliable across our two backends (Azure Function
  // direct vs Netlify db-connect proxy): one returns `{ exists: 1 }`, the
  // other has been observed to return `{ Exists: 1 }`, `{ '': 1 }`, or even
  // string values. So instead of checking a specific key, we scan every value
  // in the first row and treat any truthy/1-equivalent as "exists".
  const probeSQL = `SELECT CASE WHEN OBJECT_ID(N'${tableName.replace(/'/g, "''")}', 'U') IS NULL THEN 0 ELSE 1 END AS tbl_exists;`;
  try {
    const res = await impDbCall(conn, { action:'execute', sql: probeSQL });
    const exists = impRowSaysExists(res);
    if (exists){
      impOpenExistsModal(tableName);
    } else {
      // No existing table → just create fresh
      impExecute(tableName, 'create');
    }
  } catch (e) {
    // If probe fails (permissions, syntax, driver) — surface the error and stop.
    // We do NOT silently proceed; accidentally dropping a table we can't see
    // would be catastrophic.
    impRunResult('Could not verify target table: ' + (e.message || e) + '. Fix the connection or permissions and try again.', 'err');
  }
}

// Shared helper used by both single-file and batch probes. Given the result of
// a `SELECT CASE WHEN OBJECT_ID(...) IS NULL THEN 0 ELSE 1 END AS tbl_exists`
// query, return true if any value in the first row equals 1 (number) or '1'
// (string). Permissive on purpose — different backends and driver versions
// serialise the column name differently, but the value is always 0/1.
function impRowSaysExists(res){
  const row = res?.rows?.[0];
  if (!row || typeof row !== 'object') return false;
  for (const k of Object.keys(row)){
    const v = row[k];
    if (v === 1 || v === true) return true;
    if (typeof v === 'string' && v.trim() === '1') return true;
  }
  return false;
}

let _impPendingTable = null;
function impOpenExistsModal(tableName){
  _impPendingTable = tableName;
  document.getElementById('imp-exists-tablename').textContent = tableName;
  document.getElementById('imp-exists-modal').classList.add('open');
  impRunResult('Table exists. Choose how to proceed in the dialog.', 'info');
}
function impCloseExistsModal(){
  _impPendingTable = null;
  document.getElementById('imp-exists-modal').classList.remove('open');
  impRunResult('Cancelled.', 'info');
}
function impProceed(mode){
  const tableName = _impPendingTable;
  document.getElementById('imp-exists-modal').classList.remove('open');
  _impPendingTable = null;
  if (!tableName) return;
  impExecute(tableName, mode);   // 'append' or 'overwrite'
}

async function impExecute(tableName, mode){
  if (!cpPageGuardWrite('Data import')) return;
  if (impState.streaming){
    return impExecuteStreaming(tableName, mode);
  }
  return impExecuteInMemory(tableName, mode);
}

// In-memory execute (legacy path) — used for small CSVs and all Excel files
// where the full row array is held in impState.rows.
async function impExecuteInMemory(tableName, mode){
  const btn = document.getElementById('imp-run-btn');
  if (btn){ btn.disabled = true; btn.textContent = 'Importing…'; }
  impAbortRequested = false;
  impSetAbortVisible(true);

  const dbChoice = document.getElementById('imp-target-db').value;
  const conn = impGetConn(dbChoice);

  const cols = impState.columns;
  const rows = impState.rows;
  const totalRows = rows.length;

  try {
    // Build CREATE TABLE (or DROP+CREATE for overwrite). For 'append' we skip
    // DDL entirely — the table exists by definition, we're just inserting.
    // For 'create', we wrap in IF OBJECT_ID(...) IS NULL so a false-negative
    // probe (or a race with another import) can't crash us with msg 2714
    // "There is already an object named...".
    const createSQL = cols.map(c => {
      const nullness = c.nullable === false ? 'NOT NULL' : 'NULL';
      return `[${c.name.replace(/]/g, ']]')}] ${impSqlType(c)} ${nullness}`;
    }).join(',\n  ');
    const safeName = tableName.replace(/'/g, "''");
    if (mode === 'overwrite'){
      const ddl = `IF OBJECT_ID(N'${safeName}', 'U') IS NOT NULL DROP TABLE ${tableName};\nCREATE TABLE ${tableName} (\n  ${createSQL}\n);`;
      impRunResult('Dropping and recreating table…', 'info');
      await impDbCall(conn, { action:'execute', sql: ddl });
    } else if (mode === 'create'){
      const ddl = `IF OBJECT_ID(N'${safeName}', 'U') IS NULL\nCREATE TABLE ${tableName} (\n  ${createSQL}\n);`;
      impRunResult('Creating table…', 'info');
      await impDbCall(conn, { action:'execute', sql: ddl });
    }
    // For 'append' mode: no DDL — go straight to INSERT.

    // Insert in batches of 500 rows (stays well under SQL Server's 1000-row
    // INSERT VALUES limit and keeps request bodies manageable).
    const BATCH = 500;
    const colNames = cols.map(c => `[${c.name.replace(/]/g, ']]')}]`).join(',');
    const insertedSoFar = { n: 0 };

    for (let i = 0; i < totalRows; i += BATCH){
      if (impAbortRequested){
        impRunResult(`✕ Aborted by user — ${insertedSoFar.n.toLocaleString()} of ${totalRows.toLocaleString()} rows imported. The partial table remains in the database; drop or truncate it manually if you want to start fresh.`, 'info');
        return;
      }
      const slice = rows.slice(i, i + BATCH);
      const valuesSql = slice.map(r => {
        const tuple = cols.map(c => impLiteral(r[c.name], c.chosenType || c.inferredType));
        return '(' + tuple.join(',') + ')';
      }).join(',\n');
      const batchSql = `INSERT INTO ${tableName} (${colNames}) VALUES\n${valuesSql};`;
      await impDbCall(conn, { action:'execute', sql: batchSql });
      insertedSoFar.n += slice.length;
      impRunResult(`Imported ${insertedSoFar.n.toLocaleString()} / ${totalRows.toLocaleString()} rows…`, 'info');
    }

    impRunResult(`✓ Imported ${totalRows.toLocaleString()} rows into ${tableName}.`, 'ok');
  } catch (e) {
    impRunResult('Import failed: ' + (e.message || e), 'err');
  } finally {
    if (btn){ btn.disabled = false; btn.textContent = 'Import to database'; }
    impSetAbortVisible(false);
    impAbortRequested = false;
  }
}

// Streaming execute — for CSVs above IMP_STREAMING_THRESHOLD rows.
//
// Workflow:
//   1. CREATE/DROP+CREATE the table (skipped for append).
//   2. Re-stream the source file with Papa.parse({ step }), buffering rows
//      into a 500-row chunk. When the chunk fills, fire an INSERT, drop the
//      chunk, continue.
//   3. Inside each insert, any value that doesn't fit its column's chosen
//      type is silently coerced to NULL (no validation modal — we couldn't
//      show one without the whole file in memory anyway).
//   4. If a chunk INSERT fails with a type-error-shaped message, promote all
//      non-string columns to NVARCHAR(MAX) and retry: redo the CREATE (with
//      DROP first), then resume streaming from the start. This is the same
//      auto-recovery the batch path uses.
async function impExecuteStreaming(tableName, mode){
  const btn = document.getElementById('imp-run-btn');
  if (btn){ btn.disabled = true; btn.textContent = 'Importing…'; }
  impAbortRequested = false;
  impSetAbortVisible(true);

  const dbChoice = document.getElementById('imp-target-db').value;
  const conn = impGetConn(dbChoice);
  const cols = impState.columns;
  const file = impState.streamingFile;
  const totalRows = impState.rowCount;

  // Build the CREATE DDL from the (possibly column-modified) schema. Recomputed
  // on retry after we promote columns to MAX. We always wrap CREATE in
  // IF OBJECT_ID(...) IS NULL so a false-negative existence probe (or a race)
  // can't blow up with msg 2714. For 'overwrite' we DROP-then-CREATE; for
  // 'append' we return empty string (skip DDL entirely — table exists).
  const buildDDL = (m) => {
    if (m === 'append') return '';
    const colsSql = cols.map(c => {
      const nullness = c.nullable === false ? 'NOT NULL' : 'NULL';
      return `[${c.name.replace(/]/g, ']]')}] ${impSqlType(c)} ${nullness}`;
    }).join(',\n  ');
    const safeName = tableName.replace(/'/g, "''");
    if (m === 'overwrite'){
      return `IF OBJECT_ID(N'${safeName}', 'U') IS NOT NULL DROP TABLE ${tableName};\nCREATE TABLE ${tableName} (\n  ${colsSql}\n);`;
    }
    // 'create'
    return `IF OBJECT_ID(N'${safeName}', 'U') IS NULL\nCREATE TABLE ${tableName} (\n  ${colsSql}\n);`;
  };

  const looksLikeTypeError = (msg) => {
    if (!msg) return false;
    const m = String(msg).toLowerCase();
    return m.includes('convert') || m.includes('overflow') || m.includes('truncat')
        || m.includes('out of range') || m.includes('arithmetic')
        || m.includes('conversion failed');
  };

  // Stream-and-insert in one pass. Returns when complete or rejects on fatal
  // error. Resolves with { inserted } so the caller can report the final count.
  // If the user clicks Abort mid-stream, we stop the parser and resolve with
  // the partial count + an abort flag, so the caller can show a clean message
  // instead of an error.
  const streamAndInsert = () => new Promise((resolve, reject) => {
    const BATCH = 500;
    const colNames = cols.map(c => `[${c.name.replace(/]/g, ']]')}]`).join(',');
    let buffer = [];
    let inserted = 0;
    let lastUiUpdate = 0;
    let aborted = false;
    let userAborted = false;

    // Helper to flush the current buffer. Called from step (chunk full) and
    // from complete (final partial chunk). Pauses the parser during the await.
    const flush = async (parser) => {
      if (buffer.length === 0) return;
      const slice = buffer;
      buffer = [];
      const valuesSql = slice.map(r => {
        const tuple = cols.map(c => impLiteral(r[c.name], c.chosenType || c.inferredType));
        return '(' + tuple.join(',') + ')';
      }).join(',\n');
      const batchSql = `INSERT INTO ${tableName} (${colNames}) VALUES\n${valuesSql};`;
      await impDbCall(conn, { action:'execute', sql: batchSql });
      inserted += slice.length;
      const now = Date.now();
      if (now - lastUiUpdate > 250){
        lastUiUpdate = now;
        impRunResult(`Imported ${inserted.toLocaleString()} / ${totalRows.toLocaleString()} rows…`, 'info');
      }
    };

    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      transformHeader: h => String(h || '').trim(),
      step: (row, parser) => {
        if (aborted) return;
        // User clicked Abort — stop the parser and resolve with what we have
        if (impAbortRequested){
          aborted = true;
          userAborted = true;
          parser.abort();
          resolve({ inserted, aborted:true });
          return;
        }
        buffer.push(row.data);
        if (buffer.length >= BATCH){
          parser.pause();
          flush(parser).then(() => {
            if (impAbortRequested){
              aborted = true;
              userAborted = true;
              parser.abort();
              resolve({ inserted, aborted:true });
              return;
            }
            if (!aborted) parser.resume();
          }).catch(err => {
            aborted = true;
            parser.abort();
            reject(err);
          });
        }
      },
      complete: () => {
        if (aborted) return;
        flush().then(() => resolve({ inserted, aborted:false })).catch(reject);
      },
      error: (err) => {
        aborted = true;
        reject(new Error('CSV stream failed: ' + (err?.message || String(err))));
      }
    });
  });

  let attempts = 0;
  let currentMode = mode;
  try {
    while (attempts < 2){
      attempts++;
      try {
        const ddl = buildDDL(currentMode);
        if (ddl){
          impRunResult(currentMode === 'overwrite' ? 'Dropping and recreating table…' : 'Creating table…', 'info');
          await impDbCall(conn, { action:'execute', sql: ddl });
        }
        impRunResult(`Streaming ${totalRows.toLocaleString()} rows into ${tableName}…`, 'info');
        const { inserted, aborted: wasAborted } = await streamAndInsert();
        if (wasAborted){
          impRunResult(`✕ Aborted by user — ${inserted.toLocaleString()} of ${totalRows.toLocaleString()} rows imported. The partial table remains in the database; drop or truncate it manually if you want to start fresh.`, 'info');
          return;
        }
        impRunResult(`✓ Imported ${inserted.toLocaleString()} rows into ${tableName}.`, 'ok');
        return;
      } catch (e) {
        const msg = e?.message || String(e);
        // The MAX-promote retry only makes sense when we own the table schema
        // (create/overwrite). For append, the table already exists with fixed
        // column types — promoting our local schema to MAX wouldn't actually
        // change the table on the server, so retrying would hit the same error.
        if (attempts === 1 && looksLikeTypeError(msg) && currentMode !== 'append'){
          // Promote EVERY non-MAX column to NVARCHAR(MAX), including columns
          // we'd already typed as 'string' (NVARCHAR(N)). Schema inference
          // only samples the first 500 rows of a streamed CSV; a later row
          // with a longer-than-sampled value triggers SQL Server msg 8152
          // ("String or binary data would be truncated"). The fix is to give
          // up on sample-based sizing and use MAX for everything that wasn't
          // already MAX. We don't skip 'string' here (that was the bug —
          // it left the too-small NVARCHAR(N) columns alone and the retry
          // hit the same truncation error).
          cols.forEach(c => {
            if (c.chosenType !== 'max') c.chosenType = 'max';
          });
          impRenderSchemaTable();
          if (currentMode === 'create') currentMode = 'overwrite';
          console.warn(`[data-import] ${tableName}: type error ("${msg}"), retrying stream with all columns as NVARCHAR(MAX).`);
          impRunResult('Type mismatch (likely truncation) — converting all columns to TEXT (MAX) and restarting…', 'info');
          continue;
        }
        throw e;
      }
    }
  } catch (e) {
    if (e && e.aborted){
      impRunResult('✕ Aborted by user.', 'info');
    } else {
      impRunResult('Import failed: ' + (e.message || e), 'err');
    }
  } finally {
    if (btn){ btn.disabled = false; btn.textContent = 'Import to database'; }
    impSetAbortVisible(false);
    impAbortRequested = false;
  }
}

// Convert a raw value to a SQL Server literal, typed correctly.
// `type` is the column's chosenType key from IMP_TYPE_CATALOG.
function impLiteral(v, type){
  if (v === null || v === undefined || v === '') return 'NULL';
  const s = String(v).trim();
  if (/^(null|n\/a|na|-|—)$/i.test(s)) return 'NULL';

  if (type === 'int' || type === 'bigint'){
    return /^-?\d+$/.test(s) ? s : 'NULL';
  }
  if (type === 'decimal' || type === 'money'){
    // Strip common currency symbols and thousands separators for money
    const cleaned = type === 'money' ? s.replace(/[£$€,\s]/g, '') : s;
    return /^-?\d+(\.\d+)?$/.test(cleaned) || /^-?\.\d+$/.test(cleaned) ? cleaned : 'NULL';
  }
  if (type === 'bool'){
    if (/^(true|yes|y|1)$/i.test(s)) return '1';
    if (/^(false|no|n|0)$/i.test(s)) return '0';
    return 'NULL';
  }
  if (type === 'date'){
    // DATE — emit as N'yyyy-mm-dd' (no time component)
    const d = new Date(s);
    if (isNaN(d.getTime())) return 'NULL';
    return `N'${d.toISOString().slice(0,10)}'`;
  }
  if (type === 'datetime'){
    // DATETIME — emit as N'yyyy-mm-dd hh:mm:ss'
    const d = new Date(s);
    if (isNaN(d.getTime())) return 'NULL';
    return `N'${d.toISOString().replace('T',' ').replace(/\.\d+Z$/,'')}'`;
  }
  // string or max — escape single quotes, wrap in N''
  return `N'${s.replace(/'/g, "''")}'`;
}

function impRunResult(msg, kind){
  const el = document.getElementById('imp-run-result');
  if (!el) return;
  el.style.display = 'block';
  el.textContent = msg;
  el.style.color = kind === 'err' ? 'var(--red)'
                 : kind === 'ok'  ? 'var(--green)'
                 : 'var(--text2)';
}

// Resolve the active connection for 'src' or 'tgt' into a URL or connection
// string we can hand to the Azure Function.
// Handles both the CygenixConnections helper's shape AND a direct-sessionStorage
// fallback — because different pages on different deploy versions have used
// different key names.

function impGetConn(which){
  const c = (window.CygenixConnections && window.CygenixConnections.get && window.CygenixConnections.get()) || {};
  const ss = (k) => { try { return sessionStorage.getItem(k) || ''; } catch { return ''; } };
  // F24 — Third fallback. The active Connections view writes its state to
  // localStorage under `cygenix_project_connections` as a JSON wrapper object
  // (keys: cygenix_src_conn_string, cygenix_src_conn_mode, cygenix_fn_url,
  // cygenix_fn_key, cygenix_conn_mode, cygenix_conn_string). Without this
  // fallback the Data Import flow silently reports "No target database
  // connection configured" even when the user has working, green-dot
  // connections, because nothing populates `window.CygenixConnections` or
  // copies values into sessionStorage.
  let pc = {};
  try { pc = JSON.parse(localStorage.getItem('cygenix_project_connections') || '{}') || {}; } catch {}
  const pcGet = (k) => (pc[k] != null ? String(pc[k]) : '');
  // Helper to tell HTTP URLs apart from DB connection strings. The Connections
  // page has been observed saving e.g. "mssql://host:port/db" under cygenix_fn_url
  // when the UI mode is 'azure' but the field holds a raw connection string.
  // Without this check we'd try to fetch() that string as an HTTPS endpoint
  // and get a network error. Only treat it as a function URL if it's actually
  // http(s).
  const isHttpUrl = (s) => /^https?:\/\//i.test(s || '');

  // ─── Source ─────────────────────────────────────────────────────────
  // Mirrors the target logic: if the source is an Azure Function URL,
  // append the function key as ?code=. Otherwise treat as a connection
  // string. Without this, kicking off a conversion against an Azure
  // Function source returns 401 even though Connect & Test went green.
  if (which === 'src'){
    const srcRawFnUrl = c.srcFnUrl
                     || pcGet('cygenix_src_fn_url')
                     || ss('cygenix_src_fn_url');
    const srcFnKey    = c.srcFnKey
                     || pcGet('cygenix_src_fn_key')
                     || ss('cygenix_src_fn_key');
    if (isHttpUrl(srcRawFnUrl)){
      return srcFnKey
        ? srcRawFnUrl + (srcRawFnUrl.includes('?') ? '&' : '?') + 'code=' + encodeURIComponent(srcFnKey)
        : srcRawFnUrl;
    }
    return c.srcConnString
        || ss('cygenix_src_conn_string')
        || pcGet('cygenix_src_conn_string')
        || srcRawFnUrl  // legacy: connection string mis-saved under fn_url field
        || '';
  }

  // ─── Target ─────────────────────────────────────────────────────────
  // Priority: Azure Function URL (only if it's a real HTTP URL) → connection string.
  const rawFnUrl = c.tgtFnUrl || c.fnUrl || ss('cygenix_fn_url') || pcGet('cygenix_fn_url');
  const fnKey    = c.tgtFnKey || c.fnKey || ss('cygenix_fn_key') || pcGet('cygenix_fn_key');
  if (isHttpUrl(rawFnUrl)){
    return fnKey
      ? rawFnUrl + (rawFnUrl.includes('?') ? '&' : '?') + 'code=' + encodeURIComponent(fnKey)
      : rawFnUrl;
  }
  // Not an HTTP URL — treat anything in fn_url or conn_string as a connection string
  const cs = c.tgtConnString || c.connString || ss('cygenix_conn_string') || pcGet('cygenix_conn_string') || rawFnUrl;
  if (cs) return cs;

  // Diagnostic — surface what WAS present so the user can debug
  console.warn('[data-import] No target connection found. Inspected keys:', {
    'CygenixConnections.get()': c,
    'sessionStorage keys': Object.keys(sessionStorage).filter(k => k.startsWith('cygenix_')),
    'cygenix_project_connections keys': Object.keys(pc),
  });
  return '';
}

// Dispatch a DB call — direct URL (Azure Function) or connection-string via netlify proxy.
async function impDbCall(conn, body){
  const isFn = conn.startsWith('https://') || conn.startsWith('http://');
  const res = isFn
    ? await fetch(conn, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body), signal:AbortSignal.timeout(60000) })
    : await fetch('/.netlify/functions/db-connect', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({...body, connectionString: conn}), signal:AbortSignal.timeout(60000) });
  const data = await res.json().catch(() => ({ error:'Non-JSON response' }));
  // Netlify wraps function errors as { errorType, errorMessage } — the
  // original error-extraction only checked `error`, which left users
  // staring at a bare "Error" string when the Function bailed. Read all
  // three shapes so the user actually sees what went wrong.
  if (!res.ok) throw new Error(data.error || data.errorMessage || data.message || res.statusText || ('HTTP '+res.status));
  return data;
}

// ══════════════════════════════════════════════════════════════════════════
// DATA IMPORT — BATCH MODE
// ──────────────────────────────────────────────────────────────────────────
// Triggered when the user drops or selects two or more files. The single-file
// flow (impHandleFile → schema preview → manual target picker) is bypassed
// entirely. Instead, every file goes through the same parse/infer/import
// pipeline automatically with these defaults:
//
//   • Destination = source DB (configurable in the batch panel)
//   • Table name  = dbo.<filename-stem>
//                   (multi-sheet Excel → <stem>_<sheet>, one table per sheet)
//   • Type policy = use inferred types, but on ANY validation failure for a
//                   column, silently switch that column to NVARCHAR(MAX) and
//                   retry. No modal, no user prompt.
//   • If exists  = append rows by default (configurable: append / overwrite / skip)
//
// Files are processed sequentially so the Azure SQL Free tier isn't hammered
// and so per-file status reporting stays clean.
// ══════════════════════════════════════════════════════════════════════════

// In-memory state for the batch. Each entry tracks one file/sheet pair as a
// distinct import unit (so a single workbook with 3 sheets becomes 3 entries).
const impBatch = {
  // Array of { id, file, fileName, sheetName, displayName, tableName,
  //            status:'pending'|'parsing'|'parsed'|'importing'|'done'|'error',
  //            rows, columns, rowCount, error, importedRows,
  //            streaming, streamingFile, targetDb }
  // targetDb is 'src' or 'tgt', recorded at import time. Used by the View
  // button to send the user to the SQL editor with the correct connection.
  units: [],
  running: false,
  cancelled: false,
};

// ─── Batch persistence ─────────────────────────────────────────────────────
// We persist completed batch state to localStorage so users can navigate away
// (e.g. to the SQL editor via the View button) and come back to see what they
// imported. Only serialisable fields are saved — File objects are dropped, so
// restored units can be VIEWED but not re-imported or re-streamed. The
// localStorage key holds the most recent batch only; starting a new batch
// (drop more files) replaces it.
const IMP_BATCH_STORAGE_KEY = 'cygenix_batch_import_history';

function impBatchSerialize(){
  // Strip the File reference and the rows array (large, and rows are gone
  // anyway after a streaming import). Keep everything needed to render the
  // table + wire up the View button.
  return impBatch.units.map(u => ({
    id: u.id,
    fileName: u.fileName,
    sheetName: u.sheetName || null,
    displayName: u.displayName,
    tableName: u.tableName,
    status: u.status,
    rowCount: u.rowCount || null,
    columns: u.columns ? u.columns.map(c => ({ name: c.name })) : null,  // just names — full schema isn't needed post-import
    error: u.error || null,
    importedRows: u.importedRows || 0,
    targetDb: u.targetDb || null,
    // Mark restored units so the renderer can grey out actions that need a live File
    restored: true,
  }));
}

function impBatchSaveState(){
  try {
    if (!impBatch.units || impBatch.units.length === 0){
      localStorage.removeItem(IMP_BATCH_STORAGE_KEY);
      return;
    }
    localStorage.setItem(IMP_BATCH_STORAGE_KEY, JSON.stringify(impBatchSerialize()));
  } catch (e){
    console.warn('[batch-import] Failed to persist batch state:', e);
  }
}

function impBatchRestoreState(){
  try {
    const raw = localStorage.getItem(IMP_BATCH_STORAGE_KEY);
    if (!raw) return false;
    const units = JSON.parse(raw);
    if (!Array.isArray(units) || units.length === 0) return false;
    // Normalise any in-flight statuses — these can occur if the user
    // navigated away during an import. They'd otherwise render with a
    // spinner that never resolves.
    units.forEach(u => {
      if (u.status === 'importing' || u.status === 'parsing'){
        u.status = 'error';
        u.error = u.error || 'Import interrupted by page navigation';
      }
    });
    impBatch.units = units;
    const wrap = document.getElementById('imp-batch-wrap');
    if (wrap) wrap.style.display = 'block';
    impBatchRender();
    impBatchSummary(`Restored last batch (${units.length} file${units.length===1?'':'s'}). View imported tables via the eye icons; drop new files to start a fresh batch.`, 'info');
    return true;
  } catch (e){
    console.warn('[batch-import] Failed to restore batch state:', e);
    return false;
  }
}

// Slugify a filename or sheet name into a SQL-Server-safe identifier stem.
// Strips file extension, replaces non-alphanumeric with underscore, collapses
// runs of underscores, trims leading underscores. Returns 'Imported' as a
// last resort so we never produce an empty table name.
function impBatchSlug(s){
  if (!s) return 'Imported';
  let out = String(s)
    .replace(/\.(csv|tsv|txt|xlsx|xls)$/i, '')
    .replace(/[^A-Za-z0-9_]+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '');
  return out || 'Imported';
}

// Build the per-file table name. Sheet name appended for multi-sheet
// workbooks. Schema is always dbo (Cygenix doesn't expose a schema picker in
// the import flow yet). The table name is just the slugified filename — no
// suffix is added, so the user must make sure their file names don't collide
// with existing tables on the destination DB. The "If table exists" selector
// (Append / Overwrite / Skip) controls what happens when there's a collision.
function impBatchTableName(fileName, sheetName){
  const fileStem = impBatchSlug(fileName);
  if (sheetName){
    return `dbo.${fileStem}_${impBatchSlug(sheetName)}`;
  }
  return `dbo.${fileStem}`;
}

// Render the batch table. Called once on prepare and after every status flip.
function impBatchRender(){
  const tbl = document.getElementById('imp-batch-table');
  if (!tbl) return;
  const tbody = tbl.querySelector('tbody');
  if (!tbody) return;

  tbody.innerHTML = impBatch.units.map(u => {
    let statusHtml = '';
    if (u.status === 'pending')    statusHtml = '<span style="color:var(--text3)">Waiting…</span>';
    else if (u.status === 'parsing')   statusHtml = '<span style="color:var(--text2)"><i class="ic ic-hourglass"></i> Parsing…</span>';
    else if (u.status === 'parsed')    statusHtml = '<span style="color:var(--text2)">✓ Parsed · waiting to import</span>';
    else if (u.status === 'importing') statusHtml = `<span style="color:var(--text2)"><i class="ic ic-hourglass"></i> Importing ${(u.importedRows||0).toLocaleString()}/${(u.rowCount||0).toLocaleString()}…</span>`;
    else if (u.status === 'done')      statusHtml = `<span style="color:var(--green)">✓ Done · ${(u.rowCount||0).toLocaleString()} rows</span>`;
    else if (u.status === 'error')     statusHtml = `<span style="color:var(--red)" title="${escP(u.error||'')}"><i class="ic-dot" style="color:var(--red)"></i> ${escP((u.error||'Failed').slice(0,80))}</span>`;

    // View icon — opens the SQL editor with a SELECT TOP 1000 against this
    // unit's table on the connection it was imported into. Only enabled when
    // the unit reached the database (status === 'done' AND we know targetDb).
    // Tooltip reflects the click behaviour: same-tab when idle, new-tab when
    // an import is running (so we don't tear down the in-flight loop).
    let viewBtn = '';
    if (u.status === 'done' && u.targetDb){
      const tip = impBatch.running
        ? 'Open in SQL editor (new tab — import in progress)'
        : 'Open in SQL editor';
      viewBtn = `<button class="btn btn-ghost btn-sm" title="${tip}" onclick="impBatchView('${escP(u.id)}')" style="padding:2px 6px;font-size:13px;line-height:1"><i class="ic ic-eye"></i> </button>`;
    } else {
      viewBtn = `<span style="color:var(--text3);font-size:13px;opacity:0.3;padding:2px 6px" title="${u.status === 'done' ? 'Imported before View support — re-import to enable' : 'Available once import completes'}"><i class="ic ic-eye"></i> </span>`;
    }

    return `<tr>
      <td title="${escP(u.displayName)}">${escP(u.displayName.length > 50 ? u.displayName.slice(0,47)+'…' : u.displayName)}</td>
      <td style="font-family:var(--mono);font-size:11px">${escP(u.tableName)}</td>
      <td>${u.rowCount != null ? u.rowCount.toLocaleString() : '—'}</td>
      <td>${u.columns ? u.columns.length : '—'}</td>
      <td>${statusHtml}</td>
      <td style="text-align:center;width:36px">${viewBtn}</td>
    </tr>`;
  }).join('');

  const countEl = document.getElementById('imp-batch-count');
  if (countEl) countEl.textContent = impBatch.units.length + ' file' + (impBatch.units.length === 1 ? '' : 's');
}

// View button handler — opens the SQL editor with a SELECT TOP 1000 against
// the imported table on the connection it was imported into. The editor reads
// the URL params and auto-runs if ?run=1 is present.
//
// IMPORTANT — navigation behaviour depends on whether an import is running:
//
//   • Import IDLE: same-tab navigation. The user said they want to come back
//     to the import screen, and restoring batch state from localStorage on
//     return preserves the list. This is the original requested UX.
//
//   • Import RUNNING: new-tab open. Same-tab navigation would tear down the
//     dashboard's JS context, killing the in-flight import loop. The
//     currently-running INSERT continues on the server but the JS that
//     advances to the next file never runs, leaving remaining files marked
//     "Import interrupted by page navigation" on return. Opening a new tab
//     keeps the dashboard alive and the batch continues uninterrupted.
function impBatchView(unitId){
  const unit = impBatch.units.find(u => u.id === unitId);
  if (!unit){
    alert('Could not find that table in the batch — try refreshing the page.');
    return;
  }
  if (!unit.targetDb){
    alert('This row was imported before the View feature was added. Re-import to enable viewing.');
    return;
  }
  const conn = unit.targetDb === 'tgt' ? 'target' : 'source';
  // The table name is already fully-qualified (dbo.<name>), so just SELECT TOP 1000
  // from it. Using TOP 1000 instead of 100 is closer to SSMS's default and gives
  // a reasonable preview without saturating the results panel.
  const sql = `SELECT TOP 1000 * FROM ${unit.tableName};`;
  const params = new URLSearchParams({ sql: sql, conn: conn, run: '1' });
  const url = '/sql-editor.html?' + params.toString();

  if (impBatch.running){
    // Don't tear down the running import. Open in a new tab so the user can
    // peek at completed tables while the rest of the batch keeps importing.
    window.open(url, '_blank', 'noopener,noreferrer');
  } else {
    // Idle — same-tab navigation. Save state first so we restore on return.
    impBatchSaveState();
    window.location.href = url;
  }
}

function impBatchSummary(msg, kind){
  const el = document.getElementById('imp-batch-summary');
  if (!el) return;
  el.style.display = 'block';
  el.textContent = msg;
  el.style.color = kind === 'err' ? 'var(--red)'
                 : kind === 'ok'  ? 'var(--green)'
                 : 'var(--text2)';
}

// Prepare the batch by parsing every file in parallel, then rendering. The
// import itself still happens sequentially when the user hits Run.
async function impBatchPrepare(files){
  impBatch.units = [];
  impBatch.cancelled = false;
  // Dropping new files starts a fresh batch — wipe the persisted history so
  // restored "done" rows from a prior batch don't linger in the UI.
  try { localStorage.removeItem(IMP_BATCH_STORAGE_KEY); } catch {}

  // Reset the meta line from any prior single-file run
  const meta = document.getElementById('imp-file-meta');
  if (meta){ meta.style.display = 'none'; meta.innerHTML = ''; }

  // Validate file types up front; reject anything unsupported with a row entry
  // (so the user can see exactly what was ignored).
  for (const f of files){
    if (!/\.(csv|tsv|txt|xlsx|xls)$/i.test(f.name)){
      impBatch.units.push({
        id: 'u' + Math.random().toString(36).slice(2,9),
        file: f, fileName: f.name, sheetName: null,
        displayName: f.name, tableName: '—',
        status:'error', error:'Unsupported file type (need .csv/.tsv/.txt/.xlsx/.xls)',
        rows:null, columns:null, rowCount:null, importedRows:0,
      });
      continue;
    }
    // CSV is streamed (1GB cap), Excel must fit in browser memory (100MB cap).
    const isCsvFile = /\.(csv|tsv|txt)$/i.test(f.name);
    const sizeCap = isCsvFile ? (1024 * 1024 * 1024) : (100 * 1024 * 1024);
    if (f.size > sizeCap){
      impBatch.units.push({
        id: 'u' + Math.random().toString(36).slice(2,9),
        file: f, fileName: f.name, sheetName: null,
        displayName: f.name, tableName: '—',
        status:'error', error: isCsvFile ? 'CSV is over 1GB' : 'Excel file is over 100MB (save as CSV for larger files)',
        rows:null, columns:null, rowCount:null, importedRows:0,
      });
      continue;
    }
    // Stub a unit per file. For Excel we expand to one unit per sheet during
    // parse (so we can't know the final count until parsing completes).
    impBatch.units.push({
      id: 'u' + Math.random().toString(36).slice(2,9),
      file: f, fileName: f.name, sheetName: null,
      displayName: f.name,
      tableName: impBatchTableName(f.name, null),
      status:'pending',
      rows:null, columns:null, rowCount:null, error:null, importedRows:0,
      streaming:false, streamingFile:null,
    });
  }

  document.getElementById('imp-batch-wrap').style.display = 'block';
  impBatchRender();
  impBatchSummary('Parsing files…', 'info');

  // Parse all units in parallel. Each parse may expand a single unit into
  // multiple (one per Excel sheet), so we rebuild the units array as we go.
  const parsePromises = impBatch.units
    .filter(u => u.status === 'pending')
    .map(u => impBatchParseUnit(u));

  await Promise.allSettled(parsePromises);

  impBatchRender();
  const ok = impBatch.units.filter(u => u.status === 'parsed').length;
  const bad = impBatch.units.filter(u => u.status === 'error').length;
  if (ok === 0){
    impBatchSummary(`No files could be parsed (${bad} failed). Fix the issues above and try again.`, 'err');
  } else {
    impBatchSummary(`Parsed ${ok} table${ok===1?'':'s'} from ${files.length} file${files.length===1?'':'s'}${bad?` · ${bad} failed`:''}. Click “Import all files” to push them to the database.`, 'info');
  }
}

// Parse a single unit. CSV is streamed in pass-1 (sample + count) when the
// file is large; smaller files are loaded in-memory like before. Excel
// always goes through the full in-memory parse (library limitation) and may
// produce N sub-units (one per sheet), in which case we replace the original
// unit in impBatch.units with N new units.
function impBatchParseUnit(unit){
  return new Promise((resolve) => {
    unit.status = 'parsing';
    impBatchRender();

    const f = unit.file;
    const isCSV = /\.(csv|tsv|txt)$/i.test(f.name);

    if (isCSV){
      if (typeof Papa === 'undefined'){
        unit.status = 'error';
        unit.error = 'CSV parser failed to load (PapaParse missing). Reload the page.';
        return resolve();
      }

      // Files above 10MB go straight to streaming pass-1. Smaller files use the
      // in-memory parse; if the row count comes back above IMP_STREAMING_THRESHOLD
      // we fall through to streaming anyway.
      const SIZE_HEURISTIC = 10 * 1024 * 1024;

      const doStreamingParse = () => {
        const IMP_INFER_SAMPLE = 500;
        const sampleRows = [];
        let totalRows = 0;
        let cols = null;
        let aborted = false;

        Papa.parse(f, {
          header: true,
          skipEmptyLines: true,
          dynamicTyping: false,
          transformHeader: h => String(h || '').trim(),
          step: (row, parser) => {
            if (!cols){
              cols = Object.keys(row.data || {}).filter(k => k != null && k !== '');
              if (cols.length === 0){
                parser.abort();
                aborted = true;
                unit.status = 'error';
                unit.error = 'No column headers detected.';
                return;
              }
            }
            if (sampleRows.length < IMP_INFER_SAMPLE){
              sampleRows.push(row.data);
            }
            totalRows++;
            if (totalRows > IMP_CSV_HARD_CAP){
              parser.abort();
              aborted = true;
              unit.status = 'error';
              unit.error = `File exceeds ${IMP_CSV_HARD_CAP.toLocaleString()} rows.`;
              return;
            }
          },
          complete: () => {
            if (aborted) return resolve();
            if (totalRows === 0){
              unit.status = 'error';
              unit.error = 'File appears empty or has no data rows.';
              return resolve();
            }
            unit.streaming     = true;
            unit.streamingFile = f;
            unit.rows          = null;
            unit.rowCount      = totalRows;
            unit.columns       = impInferSchema(cols, sampleRows);
            unit.status        = 'parsed';
            resolve();
          },
          error: (err) => {
            unit.status = 'error';
            unit.error = 'CSV stream failed: ' + (err?.message || String(err));
            resolve();
          }
        });
      };

      if (f.size > SIZE_HEURISTIC){
        doStreamingParse();
        return;
      }

      // Small file — try in-memory; switch to streaming if surprisingly long.
      Papa.parse(f, {
        header: true,
        skipEmptyLines: true,
        dynamicTyping: false,
        transformHeader: h => String(h || '').trim(),
        complete: (result) => {
          try {
            const rows = result.data || [];
            if (rows.length > IMP_STREAMING_THRESHOLD){
              console.log('[batch-import] Small file but ' + rows.length + ' rows — switching to streaming.');
              doStreamingParse();
            } else {
              impBatchFinalizeUnit(unit, rows);
              resolve();
            }
          } catch (e){
            unit.status = 'error';
            unit.error = 'CSV finalize failed: ' + (e?.message || String(e));
            resolve();
          }
        },
        error: (err) => {
          unit.status = 'error';
          unit.error = 'CSV parse failed: ' + (err?.message || String(err));
          resolve();
        }
      });
      return;
    }

    // Excel
    if (typeof XLSX === 'undefined'){
      unit.status = 'error';
      unit.error = 'Excel parser failed to load (XLSX missing). Reload the page.';
      return resolve();
    }
    const reader = new FileReader();
    reader.onerror = () => { unit.status = 'error'; unit.error = 'Failed to read file.'; resolve(); };
    reader.onload = (ev) => {
      try {
        const data = new Uint8Array(ev.target.result);
        const wb = XLSX.read(data, { type:'array', cellDates:true });
        const sheets = wb.SheetNames || [];
        if (sheets.length === 0){
          unit.status = 'error';
          unit.error = 'Workbook has no sheets.';
          return resolve();
        }
        if (sheets.length === 1){
          const ws = wb.Sheets[sheets[0]];
          const rows = XLSX.utils.sheet_to_json(ws, { defval:'', raw:false, blankrows:false });
          impBatchFinalizeUnit(unit, rows);
          return resolve();
        }
        // Multi-sheet — expand into one unit per sheet, replacing this unit
        // in-place in impBatch.units.
        const idx = impBatch.units.findIndex(u => u.id === unit.id);
        const expanded = sheets.map(sheetName => {
          const ws = wb.Sheets[sheetName];
          const rows = XLSX.utils.sheet_to_json(ws, { defval:'', raw:false, blankrows:false });
          const sub = {
            id: 'u' + Math.random().toString(36).slice(2,9),
            file: unit.file,
            fileName: unit.fileName,
            sheetName,
            displayName: `${unit.fileName} [${sheetName}]`,
            tableName: impBatchTableName(unit.fileName, sheetName),
            status:'parsing',
            rows:null, columns:null, rowCount:null, error:null, importedRows:0,
            streaming:false, streamingFile:null,
          };
          impBatchFinalizeUnit(sub, rows);
          return sub;
        });
        if (idx >= 0){
          impBatch.units.splice(idx, 1, ...expanded);
        }
        resolve();
      } catch (e) {
        unit.status = 'error';
        unit.error = 'Excel parse failed: ' + (e?.message || String(e));
        resolve();
      }
    };
    reader.readAsArrayBuffer(f);
  });
}

// Apply parsed rows to a unit: row-count check, schema inference. Mirrors
// impFinalizeRows but writes into the unit object instead of impState. Only
// used for in-memory units (small CSVs + all Excel). Streamed CSVs set
// unit.streaming directly inside impBatchParseUnit and never hit this fn.
function impBatchFinalizeUnit(unit, rows){
  if (!rows || rows.length === 0){
    unit.status = 'error';
    unit.error = 'File appears empty or has no data rows.';
    return;
  }
  if (rows.length > IMP_XLSX_HARD_CAP){
    unit.status = 'error';
    unit.error = `File has ${rows.length.toLocaleString()} rows. Max ${IMP_XLSX_HARD_CAP.toLocaleString()} for Excel — save as CSV to stream more.`;
    return;
  }
  const firstRow = rows[0];
  const cols = Object.keys(firstRow || {}).filter(k => k != null && k !== '');
  if (cols.length === 0){
    unit.status = 'error';
    unit.error = 'No column headers detected. Make sure the first row contains header names.';
    return;
  }
  unit.rows = rows;
  unit.rowCount = rows.length;
  unit.columns = impInferSchema(cols, rows);
  unit.status = 'parsed';
}

// Top-level Run handler — sequential import across every parsed unit.
async function impBatchRun(){
  if (impBatch.running){
    impBatchSummary('Import already in progress…', 'info');
    return;
  }
  const ready = impBatch.units.filter(u => u.status === 'parsed');
  if (ready.length === 0){
    impBatchSummary('No parsed files to import. Drop some files first.', 'err');
    return;
  }

  const dbChoice = document.getElementById('imp-batch-db').value || 'src';
  const conn = impGetConn(dbChoice);
  if (!conn){
    const target = dbChoice === 'src' ? 'source' : 'target';
    impBatchSummary(`No ${target} database connection configured. Open the "Database connections" tab, configure ${target}, click "Connect & test" (green dot), then press "Save connections".`, 'err');
    return;
  }
  const existsMode = document.getElementById('imp-batch-exists').value || 'append';

  impBatch.running   = true;
  impBatch.cancelled = false;  // fresh run; clear any prior abort flag
  const btn = document.getElementById('imp-batch-run-btn');
  if (btn){ btn.disabled = true; btn.textContent = 'Importing…'; }
  impBatchSetAbortVisible(true);
  impBatchSummary(`Starting import of ${ready.length} file${ready.length===1?'':'s'} into ${dbChoice==='src'?'source':'target'} database…`, 'info');

  let done = 0, failed = 0;
  for (const unit of ready){
    if (impBatch.cancelled) break;
    // Record the destination on the unit itself so the View button can later
    // open the right connection in the SQL editor. Done per-unit (not batch-
    // wide) because in principle a future enhancement might let different
    // files target different DBs.
    unit.targetDb = dbChoice;
    try {
      await impBatchImportUnit(unit, conn, existsMode);
      // impBatchImportUnit may have set status='error' with an abort message;
      // count it as neither done nor failed in that case
      if (unit.status === 'done') done++;
      else if (unit.status === 'error') failed++;
    } catch (e){
      unit.status = 'error';
      unit.error = e?.message || String(e);
      failed++;
      impBatchRender();
    }
    // Persist after every unit so a mid-batch navigation/refresh preserves
    // whatever's already imported.
    impBatchSaveState();
  }

  impBatch.running = false;
  if (btn){ btn.disabled = false; btn.textContent = 'Import all files'; }
  impBatchSetAbortVisible(false);
  impBatchSaveState();  // final state — includes final statuses for all units
  const totalRowsImported = impBatch.units.filter(u=>u.status==='done').reduce((a,u)=>a+(u.rowCount||0),0);
  if (impBatch.cancelled){
    impBatchSummary(`✕ Aborted by user. ${done} succeeded, ${failed} failed/aborted before stop. ${totalRowsImported.toLocaleString()} rows imported in total.`, 'info');
  } else {
    impBatchSummary(`Finished. ${done} succeeded${failed?`, ${failed} failed`:''}. ${totalRowsImported.toLocaleString()} rows total.`, failed ? (done?'info':'err') : 'ok');
  }
}

// User clicked the batch Abort button. The flag is checked between units
// (impBatchRun loop), inside both insert loops, and inside the streaming
// step callback. We never tear down the connection — let the current
// HTTP-in-flight finish, then bail out at the next checkpoint.
function impBatchAbort(){
  impBatch.cancelled = true;
  const ab = document.getElementById('imp-batch-abort-btn');
  if (ab){ ab.disabled = true; ab.textContent = 'Aborting…'; }
}

function impBatchSetAbortVisible(visible){
  const ab = document.getElementById('imp-batch-abort-btn');
  if (!ab) return;
  ab.style.display = visible ? '' : 'none';
  if (visible){
    ab.disabled = false;
    ab.textContent = '✕ Abort batch';
  }
}

// Import one unit. Workflow:
//   1. Check if the target table exists
//   2. If yes, honour the user's "if exists" policy (append / overwrite / skip)
//   3. Build the CREATE TABLE (skipped for append)
//   4. Insert rows in 500-row batches
//   5. On ANY error during validation or insert that looks like a type mismatch,
//      promote ALL non-string columns to NVARCHAR(MAX) and retry from CREATE.
//      This is the auto-recovery the user asked for: "defaulting to nvarchar(max)
//      on problem fields".
async function impBatchImportUnit(unit, conn, existsMode){
  unit.status = 'importing';
  unit.importedRows = 0;
  impBatchRender();

  // ── Pre-flight: silently coerce any bad column to NVARCHAR(MAX) before we
  // even talk to the DB. This is the cleanest place to apply the
  // "automatically handle errors → default to NVARCHAR(MAX) on problem fields"
  // rule, because we have the parsed rows in memory and can scan them quickly.
  //
  // Skipped for streamed units (CSVs over ~50k rows) — they don't keep rows
  // in memory, so we couldn't scan them without a separate pass. The streaming
  // execute path catches type errors via the SQL-side retry instead.
  if (!unit.streaming){
    const issues = impBatchValidate(unit);
    if (issues.affectedCols.size > 0){
      issues.affectedCols.forEach(colName => {
        const col = unit.columns.find(c => c.name === colName);
        if (col) col.chosenType = 'max';
      });
    }
  }

  // ── Existence check. Uses the same robust probe as the single-file flow
  // (impRowSaysExists scans every key in the first row for a truthy/1 value,
  // because column-name casing/aliasing has varied between backends).
  const tableName = unit.tableName;
  const probeSQL = `SELECT CASE WHEN OBJECT_ID(N'${tableName.replace(/'/g, "''")}', 'U') IS NULL THEN 0 ELSE 1 END AS tbl_exists;`;
  let exists = false;
  try {
    const res = await impDbCall(conn, { action:'execute', sql: probeSQL });
    exists = impRowSaysExists(res);
  } catch (e){
    throw new Error('Probe failed: ' + (e?.message || e));
  }

  let mode = 'create';
  if (exists){
    if (existsMode === 'skip'){
      unit.status = 'error';
      unit.error = `Table ${tableName} already exists — skipped per batch policy.`;
      impBatchRender();
      return;
    }
    mode = existsMode;  // 'append' or 'overwrite'
  }

  await impBatchExecuteUnit(unit, conn, mode);
}

// Scan a unit's rows against its inferred types. Returns the set of column
// names with at least one value that won't fit. NVARCHAR (string/max) columns
// are skipped — they accept anything.
function impBatchValidate(unit){
  const cols = unit.columns || [];
  const rows = unit.rows || [];
  const affectedCols = new Set();

  const fitsType = (v, type) => {
    if (v === null || v === undefined || v === '') return true;
    const s = String(v).trim();
    if (/^(null|n\/a|na|-|—)$/i.test(s)) return true;
    if (type === 'string' || type === 'max') return true;
    if (type === 'int'){
      if (!/^-?\d+$/.test(s)) return false;
      return Math.abs(Number(s)) < 2**31;
    }
    if (type === 'bigint'){
      if (!/^-?\d+$/.test(s)) return false;
      return Math.abs(Number(s)) < 2**53;
    }
    if (type === 'decimal'){
      return /^-?\d+(\.\d+)?$/.test(s) || /^-?\.\d+$/.test(s);
    }
    if (type === 'money'){
      const cleaned = s.replace(/[£$€,\s]/g, '');
      return /^-?\d+(\.\d+)?$/.test(cleaned) || /^-?\.\d+$/.test(cleaned);
    }
    if (type === 'bool'){
      return /^(true|false|yes|no|y|n|1|0)$/i.test(s);
    }
    if (type === 'date' || type === 'datetime'){
      const d = new Date(s);
      return !isNaN(d.getTime());
    }
    return true;
  };

  for (let i = 0; i < rows.length; i++){
    const r = rows[i];
    for (const c of cols){
      if (affectedCols.has(c.name)) continue;  // already flagged — skip the rest
      const v = r[c.name];
      const type = c.chosenType || c.inferredType;
      if (!fitsType(v, type)) affectedCols.add(c.name);
    }
  }
  return { affectedCols };
}

// Build DDL + run INSERTs for one unit. On a DB-side error that looks like a
// type mismatch (msg includes "convert", "overflow", "truncat", or "incorrect
// syntax near 'NULL'"), we fall back to NVARCHAR(MAX) for ALL non-string
// columns and retry once. This is the safety net for cases where our client-
// side validation missed something (eg. exotic date formats SQL Server rejects
// but Date.parse accepts).
async function impBatchExecuteUnit(unit, conn, mode){
  const cols = unit.columns;
  const tableName = unit.tableName;

  const buildDDL = (m) => {
    if (m === 'append') return '';
    const colsSql = cols.map(c => {
      const nullness = c.nullable === false ? 'NOT NULL' : 'NULL';
      return `[${c.name.replace(/]/g, ']]')}] ${impSqlType(c)} ${nullness}`;
    }).join(',\n  ');
    const safeName = tableName.replace(/'/g, "''");
    if (m === 'overwrite'){
      return `IF OBJECT_ID(N'${safeName}', 'U') IS NOT NULL DROP TABLE ${tableName};\nCREATE TABLE ${tableName} (\n  ${colsSql}\n);`;
    }
    // 'create' — guarded so a probe false-negative or race doesn't trigger msg 2714
    return `IF OBJECT_ID(N'${safeName}', 'U') IS NULL\nCREATE TABLE ${tableName} (\n  ${colsSql}\n);`;
  };

  const looksLikeTypeError = (msg) => {
    if (!msg) return false;
    const m = String(msg).toLowerCase();
    return m.includes('convert') || m.includes('overflow') || m.includes('truncat')
        || m.includes('out of range') || m.includes('arithmetic')
        || m.includes('conversion failed');
  };

  // Two insert backends, picked by unit.streaming. Both honour the BATCH=500
  // chunk size and update unit.importedRows + re-render between batches.
  const colNames = cols.map(c => `[${c.name.replace(/]/g, ']]')}]`).join(',');
  const BATCH = 500;

  const insertFromMemory = async () => {
    const rows = unit.rows;
    unit.importedRows = 0;
    impBatchRender();
    for (let i = 0; i < rows.length; i += BATCH){
      if (impBatch.cancelled) return { aborted:true };
      const slice = rows.slice(i, i + BATCH);
      const valuesSql = slice.map(r => {
        const tuple = cols.map(c => impLiteral(r[c.name], c.chosenType || c.inferredType));
        return '(' + tuple.join(',') + ')';
      }).join(',\n');
      const batchSql = `INSERT INTO ${tableName} (${colNames}) VALUES\n${valuesSql};`;
      await impDbCall(conn, { action:'execute', sql: batchSql });
      unit.importedRows += slice.length;
      impBatchRender();
    }
    return { aborted:false };
  };

  // Streaming insert — re-parses unit.streamingFile from the start, buffers
  // BATCH rows, fires the INSERT, drops the buffer. Pauses the parser while
  // awaiting each flush so memory stays flat at ~BATCH rows. Honours the
  // global batch abort flag — sets aborted:true in the resolution if so.
  const insertFromStream = () => new Promise((resolve, reject) => {
    let buffer = [];
    let aborted = false;
    let userAborted = false;
    unit.importedRows = 0;
    impBatchRender();

    const flush = async () => {
      if (buffer.length === 0) return;
      const slice = buffer;
      buffer = [];
      const valuesSql = slice.map(r => {
        const tuple = cols.map(c => impLiteral(r[c.name], c.chosenType || c.inferredType));
        return '(' + tuple.join(',') + ')';
      }).join(',\n');
      const batchSql = `INSERT INTO ${tableName} (${colNames}) VALUES\n${valuesSql};`;
      await impDbCall(conn, { action:'execute', sql: batchSql });
      unit.importedRows += slice.length;
      impBatchRender();
    };

    Papa.parse(unit.streamingFile, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      transformHeader: h => String(h || '').trim(),
      step: (row, parser) => {
        if (aborted) return;
        if (impBatch.cancelled){
          aborted = true; userAborted = true;
          parser.abort();
          resolve({ aborted:true });
          return;
        }
        buffer.push(row.data);
        if (buffer.length >= BATCH){
          parser.pause();
          flush().then(() => {
            if (impBatch.cancelled){
              aborted = true; userAborted = true;
              parser.abort();
              resolve({ aborted:true });
              return;
            }
            if (!aborted) parser.resume();
          }).catch(err => {
            aborted = true;
            parser.abort();
            reject(err);
          });
        }
      },
      complete: () => {
        if (aborted) return;
        flush().then(() => resolve({ aborted:false })).catch(reject);
      },
      error: (err) => {
        aborted = true;
        reject(new Error('CSV stream failed: ' + (err?.message || String(err))));
      }
    });
  });

  let attempts = 0;
  while (attempts < 2){
    attempts++;
    try {
      const ddl = buildDDL(mode);
      if (ddl){
        await impDbCall(conn, { action:'execute', sql: ddl });
      }

      const result = unit.streaming
        ? await insertFromStream()
        : await insertFromMemory();

      if (result && result.aborted){
        unit.status = 'error';
        unit.error = `Aborted — ${(unit.importedRows||0).toLocaleString()}/${(unit.rowCount||0).toLocaleString()} rows imported`;
        impBatchRender();
        return;
      }

      unit.status = 'done';
      impBatchRender();
      return;
    } catch (e){
      const msg = e?.message || String(e);
      // MAX-promote retry only makes sense when we own the table schema
      // (create/overwrite). For append, the table already exists with fixed
      // column types — promoting our local schema to MAX wouldn't change the
      // table on the server, so the retry would hit the exact same error.
      if (attempts === 1 && looksLikeTypeError(msg) && mode !== 'append'){
        // Promote EVERY non-MAX column to NVARCHAR(MAX), including columns
        // we'd already typed as 'string' (NVARCHAR(N)). The schema-inference
        // sample only sees 500 rows; longer values later in the file cause
        // SQL Server msg 8152 ("String or binary data would be truncated").
        // Don't skip 'string' here — that was the bug; it left too-small
        // NVARCHAR(N) columns alone so the retry hit the same truncation.
        cols.forEach(c => {
          if (c.chosenType !== 'max') c.chosenType = 'max';
        });
        if (mode === 'create') mode = 'overwrite';
        console.warn(`[batch-import] ${tableName}: type error on attempt 1 ("${msg}"), retrying with all columns as NVARCHAR(MAX).`);
        continue;
      }
      // Either it's not a type error, we're in append mode, or we already retried.
      throw new Error(msg);
    }
  }
}

function impBatchReset(){
  impBatch.units = [];
  impBatch.running = false;
  impBatch.cancelled = false;
  impBatchSetAbortVisible(false);
  // Explicit clear → wipe persisted state too. (Just navigating away keeps it.)
  try { localStorage.removeItem(IMP_BATCH_STORAGE_KEY); } catch {}
  const wrap = document.getElementById('imp-batch-wrap');
  if (wrap) wrap.style.display = 'none';
  const summary = document.getElementById('imp-batch-summary');
  if (summary){ summary.style.display = 'none'; summary.textContent = ''; }
  const fileInput = document.getElementById('imp-file');
  if (fileInput) fileInput.value = '';
  const tbody = document.querySelector('#imp-batch-table tbody');
  if (tbody) tbody.innerHTML = '';
}

// Expose for inline handlers
window.impHandleFiles  = impHandleFiles;
window.impBatchRun     = impBatchRun;
window.impBatchReset   = impBatchReset;
window.impAbortRun     = impAbortRun;
window.impBatchAbort   = impBatchAbort;
window.impBatchView    = impBatchView;

// Update nav dot on load
setTimeout(updateConnDots, 500);

// Restore the most recent batch's status table on page load so users coming
// back from the SQL editor (via a View click) see their import history. The
// markup only exists once the Data Import tab is in the DOM, which it always
// is since the tab content is rendered up-front (only display:none toggled).
// Wrapped in its own try/catch + small delay so a malformed persisted blob
// can't break the rest of dashboard init.
setTimeout(() => {
  try { impBatchRestoreState(); }
  catch (e){ console.warn('[batch-import] Restore failed:', e); }
}, 600);



/* ═══ block 5 — extracted from an inline <script> in dashboard.html ═══ */

// ── Backup & Restore ───────────────────────────────────────────────────────────
const BACKUP_KEYS = [
  'cygenix_jobs','cygenix_projects','cygenix_current_project','cygenix_active_project_id',
  'cygenix_project_settings','cygenix_project_connections','cygenix_saved_connections',
  'cygenix_wasis_rules','cygenix_inventory','cygenix_sql_scripts','cygenix_issues',
  'cygenix_validation_sources','cygenix_sys_params','cygenix_conv_project',
  'cygenix_api_key','cygenix_project_name','cygenix_analyst_name','cygenix_client_name',
  'cygenix_active_project','cygenix_app_prefs'
];

function quickBackupNow() {
  backupAll();
  const el = document.getElementById('quick-backup-last-run');
  if (el) el.textContent = 'Last backup: ' + new Date().toLocaleString('en-GB');
}

function backupAll() {
  const data = { version: '1.0', created: new Date().toISOString(), data: {} };
  BACKUP_KEYS.forEach(k => {
    const v = localStorage.getItem(k);
    if (v) try { data.data[k] = JSON.parse(v); } catch { data.data[k] = v; }
  });
  // Also include insight column data
  Object.keys(localStorage).filter(k => k.startsWith('cygenix_insight_')).forEach(k => {
    try { data.data[k] = JSON.parse(localStorage.getItem(k)); } catch {}
  });
  const blob = new Blob([JSON.stringify(data, null, 2)], {type:'application/json'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'cygenix-backup-' + new Date().toISOString().slice(0,10) + '.json';
  a.click(); URL.revokeObjectURL(a.href);
  // Log auto-backup
  try {
    const hist = JSON.parse(localStorage.getItem('cygenix_backup_history')||'[]');
    hist.unshift({ date: new Date().toISOString(), keys: Object.keys(data.data).length });
    localStorage.setItem('cygenix_backup_history', JSON.stringify(hist.slice(0,10)));
  } catch {}
  showBackupHistory();
}

function handleRestoreDrop(e) {
  e.preventDefault();
  e.currentTarget.style.borderColor='';
  const file = e.dataTransfer.files[0];
  if (file) restoreBackup(file);
}

function restoreBackup(file) {
  if (!file) return;
  const st = document.getElementById('restore-status');
  st.textContent = 'Reading backup…'; st.style.color = 'var(--accent)';
  const reader = new FileReader();
  reader.onload = e => {
    try {
      const data = JSON.parse(e.target.result);
      if (!data.data || !data.version) throw new Error('Invalid backup file format');
      let count = 0;
      Object.entries(data.data).forEach(([k, v]) => {
        localStorage.setItem(k, typeof v === 'string' ? v : JSON.stringify(v));
        count++;
      });
      CygenixConnections.load(); // refresh sessions
      st.textContent = '✓ Restored ' + count + ' items from backup dated ' + (data.created||'').slice(0,10);
      st.style.color = 'var(--green)';
      // Reload state
      try { state.jobs = JSON.parse(localStorage.getItem('cygenix_jobs')||'[]'); renderDashboard(); renderAllJobs(); } catch {}
      showBackupHistory();
    } catch(err) {
      st.textContent = '✕ Restore failed: ' + err.message;
      st.style.color = 'var(--red)';
    }
  };
  reader.readAsText(file);
}

function showBackupHistory() {
  const el = document.getElementById('backup-history-list');
  if (!el) return;
  try {
    const hist = JSON.parse(localStorage.getItem('cygenix_backup_history')||'[]');
    if (!hist.length) { el.textContent = 'No backups yet — click Download backup to create one.'; return; }
    el.innerHTML = hist.map((h,i) => `
      <div style="display:flex;align-items:center;gap:1rem;padding:0.5rem 0;border-bottom:0.5px solid var(--border);font-family:var(--mono)">
        <span style="color:var(--text3)">#${i+1}</span>
        <span style="color:var(--text2)">${new Date(h.date).toLocaleString('en-GB')}</span>
        <span style="color:var(--text3)">${h.keys} items</span>
      </div>`).join('');
  } catch { el.textContent = 'Could not load history.'; }
}

function initBackupView() {
  showBackupHistory();
  // Update quick backup banner with last backup time
  try {
    const hist = JSON.parse(localStorage.getItem('cygenix_backup_history')||'[]');
    const el = document.getElementById('quick-backup-last-run');
    if (el && hist.length) {
      el.textContent = 'Last backup: ' + new Date(hist[0].date).toLocaleString('en-GB') + ' (' + hist[0].keys + ' items)';
    }
  } catch {}
}

// ═══════════════════════════════════════════════════════════════════════════
// PRIVACY & SECURITY VIEW
// ═══════════════════════════════════════════════════════════════════════════
function initPrivacySecurityView(){
  if (!window.CygenixPrivacy){ console.warn('CygenixPrivacy not loaded'); return; }
  psRenderMode();
  psRenderRedact();
  psRenderExclusions();
  psRenderLog();
}

function psEsc(s){ return String(s==null?'':s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

function psRenderMode(){
  const P = window.CygenixPrivacy;
  const mode = P.getMode();
  document.querySelectorAll('#view-privacy-security .ps-mode-card').forEach(el => el.classList.toggle('active', el.dataset.mode === mode));
  const ind = document.getElementById('ps-mode-indicator');
  if (ind) ind.textContent = mode.toUpperCase();
}
function psSetMode(mode){
  window.CygenixPrivacy.saveSettings({ mode });
  psRenderMode();
  psToast('Mode set to ' + mode.toUpperCase());
}

function psRenderRedact(){
  const on = window.CygenixPrivacy.getSettings().redactAIValues;
  document.getElementById('ps-tog-redact').classList.toggle('on', on);
}
function psToggleRedact(){
  const on = !window.CygenixPrivacy.getSettings().redactAIValues;
  window.CygenixPrivacy.saveSettings({ redactAIValues: on });
  psRenderRedact();
  psToast(on ? 'Redaction enabled' : 'Redaction disabled');
}

function psRenderExclusions(){
  const list = window.CygenixPrivacy.getSettings().columnExcludeList || [];
  const el = document.getElementById('ps-exclusion-list');
  el.innerHTML = list.length
    ? list.map((pat, i) => `<span class="ps-chip">${psEsc(pat)} <button class="ps-chip-x" onclick="psRemovePattern(${i})" title="Remove">×</button></span>`).join('')
    : '<div style="font-size:11px;color:var(--text3);font-style:italic">No patterns yet — add one above.</div>';
  document.getElementById('ps-chip-count').textContent = list.length;
}
function psAddPattern(){
  const inp = document.getElementById('ps-new-pattern');
  const pat = inp.value.trim();
  if (!pat) return;
  const list = [...(window.CygenixPrivacy.getSettings().columnExcludeList || [])];
  if (list.includes(pat)){ psToast('Already in the list'); return; }
  list.push(pat);
  window.CygenixPrivacy.saveSettings({ columnExcludeList: list });
  inp.value = '';
  psRenderExclusions();
  psToast('Added: ' + pat);
}
function psRemovePattern(idx){
  const list = [...(window.CygenixPrivacy.getSettings().columnExcludeList || [])];
  if (idx < 0 || idx >= list.length) return;
  const removed = list.splice(idx, 1)[0];
  window.CygenixPrivacy.saveSettings({ columnExcludeList: list });
  psRenderExclusions();
  psToast('Removed: ' + removed);
}

function psRenderLog(){
  const log = window.CygenixPrivacy.getLog();
  const el = document.getElementById('ps-log-container');
  if (!log.length){
    el.innerHTML = '<div class="ps-log-empty">No AI access yet. The log captures every call from a Cygenix page to Claude.</div>';
    return;
  }
  const rev = [...log].reverse();
  el.innerHTML = '<div class="ps-log-head"><div>Timestamp</div><div>Feature</div><div>Mode</div><div>Details</div></div>'
    + '<div class="ps-log-table">'
    + rev.map(entry => {
        const d = new Date(entry.ts);
        const ts = d.toLocaleString('en-GB', { year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit', second:'2-digit' });
        const details = typeof entry.details === 'object' ? JSON.stringify(entry.details) : String(entry.details||'');
        return `<div class="ps-log-row"><div class="ps-log-ts">${psEsc(ts)}</div><div class="ps-log-feat">${psEsc(entry.feature)}</div><div class="ps-log-mode">${psEsc(entry.mode)}</div><div class="ps-log-details" title="${psEsc(details)}">${psEsc(details)}</div></div>`;
      }).join('')
    + '</div>';
}

function psClearLog(){
  if (!confirm('Delete all audit log entries? This cannot be undone.')) return;
  window.CygenixPrivacy.clearLog();
  psRenderLog();
  psToast('Log cleared');
}

function psExportLog(){
  const log = window.CygenixPrivacy.getLog();
  if (!log.length){ psToast('Log is empty — nothing to export'); return; }
  const q = v => '"'+String(v==null?'':v).replace(/"/g,'""')+'"';
  const rows = [['timestamp','feature','mode','details'].map(q).join(',')];
  log.forEach(e => rows.push([e.ts, e.feature, e.mode, typeof e.details==='object'?JSON.stringify(e.details):e.details].map(q).join(',')));
  const blob = new Blob([rows.join('\n')], {type:'text/csv'});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = 'cygenix-privacy-log.csv';
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  URL.revokeObjectURL(url);
  psToast('Log exported');
}

// Prefer the existing dashboard toast if available, fall back to a simple console line
function psToast(msg){
  if (typeof showToast === 'function'){ showToast(msg); return; }
  console.log('[privacy]', msg);
}

// ═══════════════════════════════════════════════════════════════════════════
// INTEGRATIONS VIEW
// ═══════════════════════════════════════════════════════════════════════════
function initIntegrationsView(){
  if (!window.CygenixIntegrations){ console.warn('CygenixIntegrations not loaded'); return; }
  renderIntegrationsGrids();
}

function escIn(s){ return String(s==null?'':s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

function renderIntegrationsGrids(){
  const connectors = window.CygenixIntegrations.list();
  const available = connectors.filter(c => c.status === 'available');
  const coming    = connectors.filter(c => c.status === 'coming-soon');

  const cardHTML = (c) => {
    const enabled = c.status === 'available' && window.CygenixIntegrations.isEnabled(c.id);
    const badge = c.status === 'coming-soon'
      ? '<span class="in-card-badge soon">contact</span>'
      : enabled ? '<span class="in-card-badge enabled">● on</span>' : '';
    const onClick = c.status === 'available'
      ? `openIntegrationModal('${c.id}')`
      : `openContactSalesModal('${c.id}')`;
    return `
      <div class="in-card${enabled?' enabled':''}" onclick="${onClick}" data-connector="${c.id}">
        ${badge}
        <div class="in-card-top">
          <div class="in-card-icon">${c.icon}</div>
          <div class="in-card-meta">
            <div class="in-card-name">${escIn(c.name)}</div>
            <div class="in-card-category">${escIn(c.category)}</div>
          </div>
        </div>
        <div class="in-card-desc">${escIn(c.description)}</div>
      </div>`;
  };

  document.getElementById('in-available-grid').innerHTML = available.map(cardHTML).join('');
  document.getElementById('in-coming-grid').innerHTML    = coming.map(cardHTML).join('');
}

// ── Detail modal ─────────────────────────────────────────────────────────
function openIntegrationModal(id){
  const c = window.CygenixIntegrations.getConnector(id);
  if (!c || c.status !== 'available') return;
  const cfg = window.CygenixIntegrations.getConfig(id) || {};

  // Build modal content by connector type.
  let body = '';
  let primaryBtn = '';

  // Config fields (shared rendering)
  const configFieldsHTML = (c.configFields || []).map(f => `
    <div class="in-form-row">
      <label>${escIn(f.label)}</label>
      <input type="${escIn(f.type||'text')}" id="in-field-${escIn(f.key)}" placeholder="${escIn(f.placeholder||'')}" value="${escIn(cfg[f.key]||'')}">
    </div>
  `).join('');

  if (id === 'webhook'){
    body = configFieldsHTML + `
      <div class="in-form-note warn">
        <strong>CORS heads-up:</strong> browser-to-external-webhook calls are blocked by most services. A successful "test" below means your endpoint explicitly allows cross-origin POSTs. If your service doesn't, use a relay (e.g. webhook.site works; most SaaS webhooks don't accept browser calls).
      </div>
      <div class="in-enable-row">
        <input type="checkbox" id="in-enable-toggle" ${cfg.enabled?'checked':''}>
        <label for="in-enable-toggle">Enabled — receive Cygenix events</label>
      </div>
    `;
    primaryBtn = `<button class="btn btn-primary btn-sm" onclick="testIntegration('${id}')"><i class="ic ic-flask"></i> Test webhook</button>`;

  } else if (id === 'smtp'){
    body = configFieldsHTML + `
      <div class="in-form-note warn">
        <strong>Where these credentials go:</strong> a browser cannot speak SMTP, so unlike every other
        connector here this one needs a server. Your settings are stored only in this browser, but the
        host, username and password are sent to Cygenix over HTTPS each time a message goes out, and used
        to open the connection. They are never written to disk or logged on our side. Use an app password
        rather than your main account password where your provider offers one.
      </div>
      <div class="in-form-note">
        Common ports: <strong>587</strong> (STARTTLS, most providers) · <strong>465</strong> (SSL) ·
        <strong>25</strong> or <strong>2525</strong>. Several recipients can be comma-separated.
      </div>
      <div class="in-enable-row">
        <input type="checkbox" id="in-enable-toggle" ${cfg.enabled?'checked':''}>
        <label for="in-enable-toggle">Enabled — email me when a migration run finishes</label>
      </div>
    `;
    primaryBtn = `<button class="btn btn-primary btn-sm" onclick="testIntegration('${id}')"><i class="ic ic-flask"></i> Test &amp; send</button>`;

  } else if (id === 'json-export'){
    body = `
      <div style="font-size:12px;color:var(--text2);line-height:1.6;margin-bottom:0.5rem">
        Exports the currently-active project — its settings, groups, and all jobs attached to it — as a single JSON file. Useful for backups, handover, or feeding into external ETL pipelines.
      </div>
      <div class="in-form-note">
        The bundle includes: project metadata · source/target connection strings · groups and steps · associated jobs (with mappings + SQL). It does NOT include: API keys · privacy audit log · other projects.
      </div>
    `;
    primaryBtn = `<button class="btn btn-primary btn-sm" onclick="runIntegration('${id}')"><i class="ic ic-download"></i> Download bundle</button>`;

  } else if (id === 'json-import'){
    body = `
      <div style="font-size:12px;color:var(--text2);line-height:1.6;margin-bottom:0.75rem">
        Read a Cygenix JSON bundle from your computer and bring its project into your workspace.
      </div>
      <div class="in-form-row">
        <label>Bundle file (.json)</label>
        <input type="file" id="in-import-file" accept=".json" style="padding:0.4rem">
      </div>
      <div class="in-form-row">
        <label>Import mode</label>
        <div style="display:flex;gap:1.25rem;padding:0.4rem 0">
          <label style="display:flex;align-items:center;gap:6px;font-size:12px;color:var(--text2);cursor:pointer;text-transform:none;letter-spacing:0;font-family:var(--serif)">
            <input type="radio" name="in-import-mode" value="new" checked> Create as new project
          </label>
          <label style="display:flex;align-items:center;gap:6px;font-size:12px;color:var(--text2);cursor:pointer;text-transform:none;letter-spacing:0;font-family:var(--serif)">
            <input type="radio" name="in-import-mode" value="replace"> Replace current project
          </label>
        </div>
      </div>
      <div class="in-form-note warn">
        "Replace current project" overwrites the active project's groups, steps, connection strings, and jobs. There is no undo. Use "Create as new project" unless you're certain.
      </div>
    `;
    primaryBtn = `<button class="btn btn-primary btn-sm" onclick="runImportIntegration()"><i class="ic ic-download"></i> Import bundle</button>`;

  } else if (id === 'csv-jobs'){
    body = `
      <div style="font-size:12px;color:var(--text2);line-height:1.6">
        Exports every job in the library as a CSV — includes project name, job name, source, target, created date, and status. Useful for inventory review in Excel or Google Sheets.
      </div>
    `;
    primaryBtn = `<button class="btn btn-primary btn-sm" onclick="runIntegration('${id}')"><i class="ic ic-download"></i> Download CSV</button>`;
  }

  const modalHTML = `
    <div class="in-modal-overlay show" id="in-modal-overlay" onclick="if(event.target===this)closeIntegrationModal()">
      <div class="in-modal">
        <div class="in-modal-head">
          <div class="in-card-icon">${c.icon}</div>
          <div style="flex:1">
            <div style="font-size:14px;font-weight:600">${escIn(c.name)}</div>
            <div style="font-size:11px;color:var(--text3)">${escIn(c.description)}</div>
          </div>
          <button onclick="closeIntegrationModal()" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:18px;padding:0 4px">✕</button>
        </div>
        <div class="in-modal-body">
          ${body}
          <div id="in-result" style="display:none"></div>
        </div>
        <div class="in-modal-foot">
          <button class="btn btn-ghost btn-sm" onclick="saveIntegrationConfig('${id}')"><i class="ic ic-save"></i> Save</button>
          ${primaryBtn}
          <button class="btn btn-ghost btn-sm" onclick="closeIntegrationModal()">Close</button>
        </div>
      </div>
    </div>`;

  // Insert into DOM
  const existing = document.getElementById('in-modal-overlay');
  if (existing) existing.remove();
  document.getElementById('view-integrations').insertAdjacentHTML('beforeend', modalHTML);
}

function closeIntegrationModal(){
  const m = document.getElementById('in-modal-overlay');
  if (m) m.remove();
}

// Contact-sales modal for 'coming-soon' connectors. Provides a mailto link and
// a honest message about what's on offer. Doesn't pretend the connector is
// already built — we're gathering interest, not selling vapourware.
function openContactSalesModal(connectorId){
  const c = window.CygenixIntegrations.getConnector(connectorId);
  if (!c) return;
  const subject = encodeURIComponent('Cygenix extension request: ' + c.name);
  const body    = encodeURIComponent(
    "Hi,\n\n" +
    "I'd like to know more about the " + c.name + " integration for Cygenix.\n\n" +
    "My use case: \n\n" +
    "Thanks"
  );
  const mailto = 'mailto:sales@cygenix.co.uk?subject=' + subject + '&body=' + body;

  const existing = document.getElementById('in-modal-overlay');
  if (existing) existing.remove();

  const modalHTML = `
    <div class="in-modal-overlay show" id="in-modal-overlay" onclick="if(event.target===this)closeIntegrationModal()">
      <div class="in-modal">
        <div class="in-modal-head">
          <div class="in-card-icon">${c.icon}</div>
          <div style="flex:1">
            <div style="font-size:14px;font-weight:600">${escIn(c.name)}</div>
            <div style="font-size:11px;color:var(--text3)">${escIn(c.description)}</div>
          </div>
          <button onclick="closeIntegrationModal()" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:18px;padding:0 4px">✕</button>
        </div>
        <div class="in-modal-body">
          <div style="font-size:12px;color:var(--text2);line-height:1.6;margin-bottom:0.75rem">
            The <strong>${escIn(c.name)}</strong> connector is available as a paid extension.
            Get in touch to discuss your use case, implementation timeline, and pricing.
          </div>
          <div class="in-form-note">
            <strong>What to include in your message:</strong>
            <ul style="margin:0.4rem 0 0;padding-left:1.2rem;line-height:1.7">
              <li>Your target system version / edition</li>
              <li>Expected event volume (migrations per day / week)</li>
              <li>Any auth constraints (OAuth, SSO, service principals)</li>
              <li>Rough target go-live date</li>
            </ul>
          </div>
        </div>
        <div class="in-modal-foot">
          <button class="btn btn-ghost btn-sm" onclick="closeIntegrationModal()">Close</button>
          <a class="btn btn-primary btn-sm" href="${mailto}" style="text-decoration:none"><i class="ic ic-mail"></i> Email sales</a>
        </div>
      </div>
    </div>`;

  document.getElementById('view-integrations').insertAdjacentHTML('beforeend', modalHTML);
}

function saveIntegrationConfig(id){
  const c = window.CygenixIntegrations.getConnector(id);
  if (!c) return;
  const cfg = {};
  (c.configFields || []).forEach(f => {
    const el = document.getElementById('in-field-'+f.key);
    if (el) cfg[f.key] = el.value.trim();
  });
  const toggle = document.getElementById('in-enable-toggle');
  if (toggle) cfg.enabled = toggle.checked;
  window.CygenixIntegrations.saveConfig(id, cfg);
  showInResult('ok', '✓ Saved');
  renderIntegrationsGrids();
}

async function testIntegration(id){
  // Save first so test uses the current form values
  saveIntegrationConfig(id);
  showInResult('ok', 'Testing…');
  const r = await window.CygenixIntegrations.test(id);
  showInResult(r.ok ? 'ok' : 'err', (r.ok ? '✓ ' : '✕ ') + r.message);
}

async function runIntegration(id){
  showInResult('ok', 'Running…');
  const r = await window.CygenixIntegrations.run(id);
  showInResult(r.ok ? 'ok' : 'err', (r.ok ? '✓ ' : '✕ ') + r.message);
  if (r.ok && typeof showToast === 'function') showToast(r.message);
}

async function runImportIntegration(){
  const file = document.getElementById('in-import-file')?.files?.[0];
  if (!file){ showInResult('err', '✕ Pick a file first'); return; }
  const mode = (document.querySelector('input[name="in-import-mode"]:checked') || {value:'new'}).value;
  showInResult('ok', 'Reading…');
  try {
    const text = await file.text();
    const bundle = JSON.parse(text);
    const r = await window.CygenixIntegrations.run('json-import', { bundle, mode });
    showInResult(r.ok ? 'ok' : 'err', (r.ok ? '✓ ' : '✕ ') + r.message);
    if (r.ok && typeof showToast === 'function') showToast(r.message);
  } catch(e){
    showInResult('err', '✕ Could not read bundle: ' + e.message);
  }
}

function showInResult(kind, message){
  const el = document.getElementById('in-result');
  if (!el) return;
  el.className = 'in-result ' + kind;
  el.textContent = message;
  el.style.display = 'block';
}

// ═══════════════════════════════════════════════════════════════════════════
// SYSTEM PARAMETERS VIEW
// ═══════════════════════════════════════════════════════════════════════════
let spParams = [];
let spFilterQuery = '';
let spSortCol = null;  // 'name' | 'description' | 'value' | 'code' | null
let spSortDir = 1;     // 1 asc, -1 desc

function initSystemParametersView(){
  if (!window.CygenixParams){ console.warn('CygenixParams helper not loaded'); return; }
  spLoad();
  // Default to the Parameters tab on every (re-)entry. If the user was
  // previously on the Was/Is tab, switching views and coming back will
  // take them back to Parameters — consistent with the way the view
  // looked before tabs existed.
  switchSysTab('params');
  // Keep the Was/Is cache fresh in case another page (object_mapping.html)
  // edited the underlying localStorage while we were away.
  if (wiInitialized) wiLoad();
}

function spEsc(s){ return String(s==null?'':s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

function spLoad(){
  try { spParams = JSON.parse(localStorage.getItem('cygenix_sys_params') || '[]'); }
  catch { spParams = []; }
  // Normalise any older rows that may lack expected fields
  spParams = spParams.map(p => ({
    name:        p.name || '',
    description: p.description || '',
    value:       p.value != null ? String(p.value) : '',
    type:        CygenixParams.typeOf(p),
    code:        CygenixParams.codeFromName(p.name || '')
  }));
  spRender();
}

function spSave(){
  // Re-derive codes before persisting (keeps stored data clean)
  const toSave = spParams.map(p => ({
    name:        p.name || '',
    description: p.description || '',
    value:       p.value != null ? String(p.value) : '',
    type:        CygenixParams.typeOf(p),
    code:        CygenixParams.codeFromName(p.name || '')
  }));
  localStorage.setItem('cygenix_sys_params', JSON.stringify(toSave));
}

// Defensive flush + visible confirmation. Auto-save still runs on every edit
// via spSave(); this button is a "yes I'm done" affordance that re-runs the
// save and surfaces a transient status so the user has explicit feedback.
function spConfirmSave(){
  try {
    spSave();
    flashSaveStatus('sp-save-status', '✓ Saved ' + spParams.length + ' parameter' + (spParams.length === 1 ? '' : 's'), 'var(--green)');
  } catch (e) {
    console.error('[sp] confirm save failed', e);
    flashSaveStatus('sp-save-status', '✗ Save failed: ' + (e.message || 'unknown'), 'var(--red)');
  }
}

function spGetDisplayedRows(){
  const q = spFilterQuery.trim().toLowerCase();
  let indexed = spParams.map((p, origIdx) => ({
    row: {
      name: p.name || '',
      description: p.description || '',
      value: p.value != null ? String(p.value) : '',
      type: CygenixParams.typeOf(p),
      code: CygenixParams.codeFromName(p.name || '')
    },
    origIdx
  }));
  if (q){
    indexed = indexed.filter(({row}) =>
      row.name.toLowerCase().includes(q) ||
      row.description.toLowerCase().includes(q) ||
      row.value.toLowerCase().includes(q) ||
      row.type.toLowerCase().includes(q) ||
      row.code.toLowerCase().includes(q)
    );
  }
  if (spSortCol){
    indexed.sort((a, b) => {
      const av = (a.row[spSortCol] || '').toLowerCase();
      const bv = (b.row[spSortCol] || '').toLowerCase();
      if (av < bv) return -1 * spSortDir;
      if (av > bv) return  1 * spSortDir;
      return 0;
    });
  }
  return indexed;
}

function spRender(){
  const tbl         = document.getElementById('sp-params-table');
  const empty       = document.getElementById('sp-empty-state');
  const emptyFilter = document.getElementById('sp-empty-filter-state');
  const tbody       = document.getElementById('sp-params-tbody');
  const filterBar   = document.getElementById('sp-filter-bar');
  if (!tbl || !empty || !tbody || !filterBar) return;

  if (!spParams.length){
    tbl.style.display = 'none';
    empty.style.display = 'block';
    emptyFilter.style.display = 'none';
    filterBar.style.display = 'none';
    return;
  }
  empty.style.display = 'none';
  filterBar.style.display = 'flex';

  const displayed = spGetDisplayedRows();

  // Sort indicators
  ['name','description','type','value','code'].forEach(c => {
    const el = document.getElementById('sp-sort-ind-'+c);
    if (!el) return;
    if (c === spSortCol){
      el.textContent = spSortDir === 1 ? '▲' : '▼';
      el.classList.add('active');
    } else {
      el.textContent = '⇅';
      el.classList.remove('active');
    }
  });

  const count = document.getElementById('sp-filter-count');
  const clr   = document.getElementById('sp-filter-clear');
  if (count){
    if (spFilterQuery.trim() || spSortCol){
      count.textContent = displayed.length + ' of ' + spParams.length;
    } else {
      count.textContent = spParams.length + ' parameter' + (spParams.length === 1 ? '' : 's');
    }
  }
  if (clr) clr.style.display = spFilterQuery.trim() ? 'inline-flex' : 'none';

  if (!displayed.length){
    tbl.style.display = 'none';
    emptyFilter.style.display = 'block';
    return;
  }
  emptyFilter.style.display = 'none';
  tbl.style.display = '';

  // Make sure usageRows is populated so the "Used in" badge has data
  // to render. usageScan is fast (<10ms) and only runs once per spRender
  // when needed.
  if (!usageRows.length){
    try { usageRows = usageScan(); } catch { usageRows = []; }
  }

  tbody.innerHTML = displayed.map(({row, origIdx}) => {
    const code = row.code;
    const typeOpts = CygenixParams.PARAM_TYPES.map(t =>
      `<option value="${t}"${row.type === t ? ' selected' : ''}>${t}</option>`
    ).join('');
    // "Used in" badge — shows a count like "3" with an inline pill style.
    // Clicking switches to the Where used tab pre-filtered to this
    // parameter name, so the user can drill in instantly.
    const usageCount = usageCountForParam(row.name);
    const usageCell = usageCount > 0
      ? `<button class="sp-usage-pill" onclick="usageJumpToParam('${spEsc(row.name).replace(/'/g, "\\'")}')" title="View ${usageCount} reference${usageCount === 1 ? '' : 's'} to @@${spEsc(row.name)}">${usageCount} ref${usageCount === 1 ? '' : 's'}</button>`
      : `<span style="font-size:11px;color:var(--text3)">—</span>`;
    return `<tr>
      <td><input class="sp-cell-input" value="${spEsc(row.name)}" oninput="spUpdateField(${origIdx},'name',this.value)" placeholder="StartDate"></td>
      <td><input class="sp-cell-input" value="${spEsc(row.description)}" oninput="spUpdateField(${origIdx},'description',this.value)" placeholder="Migration cutoff date"></td>
      <td><select class="sp-cell-input" onchange="spUpdateField(${origIdx},'type',this.value)" title="number=unquoted, text/date=quoted, raw=verbatim">${typeOpts}</select></td>
      <td><input class="sp-cell-input mono" value="${spEsc(row.value)}" oninput="spUpdateField(${origIdx},'value',this.value)" placeholder="20260101"></td>
      <td><span class="sp-cell-code" id="sp-code-${origIdx}" title="Click to copy" onclick="spCopyCode('${spEsc(code)}')">${spEsc(code) || '—'}</span></td>
      <td>${usageCell}</td>
      <td style="text-align:right"><button class="sp-icon-btn danger" onclick="spRemoveRow(${origIdx})" title="Delete">✕</button></td>
    </tr>`;
  }).join('');
}

function spApplyFilter(){
  spFilterQuery = document.getElementById('sp-filter-input').value || '';
  spRender();
}

function spClearFilter(){
  spFilterQuery = '';
  const input = document.getElementById('sp-filter-input');
  if (input) input.value = '';
  spRender();
}

function spSortBy(col){
  if (spSortCol !== col){
    spSortCol = col; spSortDir = 1;    // new column → asc
  } else if (spSortDir === 1){
    spSortDir = -1;                     // asc → desc
  } else {
    spSortCol = null; spSortDir = 1;    // desc → clear
  }
  spRender();
}

function spUpdateField(i, field, value){
  if (!spParams[i]) return;
  spParams[i][field] = value;
  if (field === 'name'){
    // Live-update the code cell without a full re-render (keeps focus)
    const code = CygenixParams.codeFromName(value);
    const codeEl = document.getElementById('sp-code-'+i);
    if (codeEl){ codeEl.textContent = code || '—'; codeEl.setAttribute('onclick', "spCopyCode('"+spEsc(code)+"')"); }
    spParams[i].code = code;
  }
  spSave();
}

function spAddRow(){
  spParams.push({ name:'', description:'', value:'', type:'text', code:'' });
  // Clear any active filter so the new blank row is visible
  if (spFilterQuery.trim()){
    spFilterQuery = '';
    const input = document.getElementById('sp-filter-input');
    if (input) input.value = '';
  }
  spSave();
  spRender();
  // Focus the new row's name input
  setTimeout(() => {
    const inputs = document.querySelectorAll('#sp-params-tbody tr:last-child input');
    if (inputs[0]) inputs[0].focus();
  }, 50);
}

function spRemoveRow(i){
  const p = spParams[i];
  if (!p) return;
  if (p.name && !confirm('Remove parameter "' + p.name + '"?')) return;
  spParams.splice(i, 1);
  spSave();
  spRender();
}

function spCopyCode(code){
  if (!code) return;
  navigator.clipboard?.writeText(code).then(
    () => showToast('Copied: ' + code),
    () => showToast('Copy failed')
  );
}

// ── CSV/JSON export ──────────────────────────────────────────────────────
function spDownload(filename, content, mime){
  const blob = new Blob([content], { type: mime });
  const url  = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = filename;
  document.body.appendChild(a); a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function spExportJSON(){
  const data = spParams.map(p => ({
    name: p.name||'', description: p.description||'', value: p.value||'',
    code: CygenixParams.codeFromName(p.name||'')
  }));
  spDownload('cygenix-parameters.json', JSON.stringify(data, null, 2), 'application/json');
  showToast('Exported JSON');
}

function spExportCSV(){
  const quote = v => { const s = String(v||''); return '"' + s.replace(/"/g,'""') + '"'; };
  const rows = [ ['name','description','value','code'].join(',') ];
  spParams.forEach(p => rows.push([p.name||'', p.description||'', p.value||'', CygenixParams.codeFromName(p.name||'')].map(quote).join(',')));
  spDownload('cygenix-parameters.csv', rows.join('\n'), 'text/csv');
  showToast('Exported CSV');
}

// ── Import ───────────────────────────────────────────────────────────────
function spImportFile(input){
  const f = input.files && input.files[0];
  if (!f) return;
  const reader = new FileReader();
  reader.onload = e => {
    const text = e.target.result;
    let imported;
    try {
      if (f.name.toLowerCase().endsWith('.json')){
        imported = JSON.parse(text);
        if (!Array.isArray(imported)) throw new Error('JSON must be an array of objects');
      } else {
        imported = spParseCSV(text);
      }
    } catch(err){
      alert('Import failed: ' + err.message);
      input.value = '';
      return;
    }
    const replace = spParams.length === 0 || confirm(
      'Replace all ' + spParams.length + ' existing parameter(s)?\n\n' +
      'OK = replace all with imported ' + imported.length + '\n' +
      'Cancel = merge (imported overrides same-name, others kept)'
    );
    if (replace){
      spParams = imported.map(spNormaliseImported);
    } else {
      const byName = new Map(spParams.map(p => [((p.name||'').toLowerCase()), p]));
      imported.forEach(r => { const n = spNormaliseImported(r); if (n.name) byName.set(n.name.toLowerCase(), n); });
      spParams = Array.from(byName.values());
    }
    spSave();
    spRender();
    showToast('Imported ' + imported.length + ' parameter(s)');
    input.value = '';
  };
  reader.readAsText(f);
}

function spNormaliseImported(r){
  const name = (r.name || r.Name || '').toString().trim();
  const rawType = (r.type || r.Type || '').toString().toLowerCase().trim();
  const value = (r.value != null ? r.value : (r.Value != null ? r.Value : '')).toString();
  const typed = rawType ? { type: rawType, value } : { value };
  return {
    name,
    description: (r.description || r.Description || '').toString(),
    value,
    type:        CygenixParams.typeOf(typed),
    code:        CygenixParams.codeFromName(name)
  };
}

// Minimal CSV parser — handles quoted fields, commas inside quotes, "" escape.
function spParseCSV(text){
  const lines = [];
  let cur = []; let field = ''; let inQ = false; let i = 0;
  while (i < text.length){
    const c = text[i];
    if (inQ){
      if (c === '"'){
        if (text[i+1] === '"'){ field += '"'; i += 2; continue; }
        inQ = false; i++; continue;
      }
      field += c; i++; continue;
    }
    if (c === '"'){ inQ = true; i++; continue; }
    if (c === ','){ cur.push(field); field = ''; i++; continue; }
    if (c === '\r'){ i++; continue; }
    if (c === '\n'){ cur.push(field); lines.push(cur); cur = []; field = ''; i++; continue; }
    field += c; i++;
  }
  if (field.length || cur.length){ cur.push(field); lines.push(cur); }
  if (!lines.length) return [];
  const header = lines[0].map(h => h.trim().toLowerCase());
  const nameIdx = header.indexOf('name');
  const descIdx = header.indexOf('description');
  const valIdx  = header.indexOf('value');
  const typeIdx = header.indexOf('type');
  if (nameIdx === -1) throw new Error('CSV is missing a "name" header column');
  const out = [];
  for (let k = 1; k < lines.length; k++){
    const row = lines[k];
    if (!row.length || (row.length === 1 && !row[0])) continue;
    out.push({
      name:        row[nameIdx] || '',
      description: descIdx !== -1 ? (row[descIdx] || '') : '',
      value:       valIdx  !== -1 ? (row[valIdx]  || '') : '',
      type:        typeIdx !== -1 ? (row[typeIdx] || '') : ''
    });
  }
  return out;
}

// ═══════════════════════════════════════════════════════════════════════════
// PARAMETER USAGE TRACKING ("Where used" tab)
// ═══════════════════════════════════════════════════════════════════════════
//
// Scans every job in localStorage's `cygenix_jobs` for places that
// reference a system parameter via @@Token. Three places are scanned:
//
//   1. columnMapping[].literalValue / fixedValue
//      User typed @@Client as a fixed value in the mapping editor — the
//      most common path.
//
//   2. columnMapping[].wasisRules[].newVal
//      A Was/Is rule whose replacement value is a parameter — e.g. "when
//      source = 'legacy' use @@Client". The wasis path that was added
//      after parameter expansion landed in the runner.
//
//   3. job.insertSQL and job.otmSQL
//      Any inline @@Token in saved INSERT SQL or OTM cursor SQL. Caught
//      via regex; we attribute these to the job as a whole rather than
//      to a specific column (the SQL string isn't easily parsed back to
//      individual columns from here).
//
// Output is a flat array of usage records. Same shape regardless of source,
// so the table rendering doesn't need to special-case anything.

const USAGE_TOKEN_RE = /@@([A-Za-z_][A-Za-z0-9_]*)/g;

let usageRows = [];                  // all scanned usages
let usageFilterQuery = '';
let usageSortCol = 'paramName';      // default: group by parameter
let usageSortDir = 1;                // 1 ascending, -1 descending

// Extract distinct @@Token names from a string. Excludes SQL Server
// @@globals like @@ROWCOUNT, @@TRANCOUNT, @@VERSION, @@SPID, etc. so
// the usage view focuses on Cygenix parameters rather than SQL builtins.
const _SQL_GLOBALS = new Set([
  'rowcount', 'trancount', 'version', 'spid', 'servername', 'identity',
  'error', 'fetch_status', 'nestlevel', 'options', 'datefirst', 'language',
  'lock_timeout', 'max_connections', 'max_precision', 'cursor_rows',
  'remserver', 'idle', 'cpu_busy', 'io_busy', 'pack_received', 'pack_sent',
  'packet_errors', 'connections', 'procid', 'textsize', 'total_errors',
  'total_read', 'total_write'
]);
function usageExtractTokens(s){
  const out = [];
  const seen = new Set();
  let m;
  USAGE_TOKEN_RE.lastIndex = 0;
  while ((m = USAGE_TOKEN_RE.exec(s)) !== null){
    const name = m[1];
    const lower = name.toLowerCase();
    if (_SQL_GLOBALS.has(lower)) continue;
    if (seen.has(name)) continue;
    seen.add(name);
    out.push(name);
  }
  return out;
}

// Build the flat usage list. Idempotent — called on tab open and on the
// Refresh button. Cheap (tens of jobs × hundreds of columns = <10ms).
function usageScan(){
  const out = [];
  let jobs = [];
  try {
    jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]') || [];
  } catch { jobs = []; }
  for (const job of jobs){
    if (!job || typeof job !== 'object') continue;
    const jobName = job.name || job.id || '(unnamed)';
    const jobId   = job.id   || '';
    const srcTable = job.source || job.srcTable || job.fromTable || '';
    const tgtTable = job.target || job.tgtTable || job.toTable   || '';

    // Path 1 + 2: column mapping entries
    const mapping = Array.isArray(job.columnMapping) ? job.columnMapping : [];
    for (const m of mapping){
      if (!m || typeof m !== 'object') continue;
      const srcCol = m.srcCol || '';
      const tgtCol = m.tgtCol || '';
      // literalValue / fixedValue — primary path
      for (const fieldName of ['literalValue', 'fixedValue']){
        const v = m[fieldName];
        if (typeof v !== 'string' || !v.includes('@@')) continue;
        for (const tok of usageExtractTokens(v)){
          out.push({
            paramName: tok,
            paramToken: '@@' + tok,
            jobName, jobId, srcTable, tgtTable,
            srcCol, tgtCol,
            source: fieldName === 'literalValue' ? 'literal value' : 'fixed value',
            expression: v
          });
        }
      }
      // Was/Is rule newVal
      const rules = Array.isArray(m.wasisRules) ? m.wasisRules : [];
      for (const r of rules){
        if (!r) continue;
        const newVal = r.newVal ?? r.is ?? r.to;
        if (typeof newVal !== 'string' || !newVal.includes('@@')) continue;
        const oldVal = r.oldVal ?? r.was ?? r.from ?? '';
        for (const tok of usageExtractTokens(newVal)){
          out.push({
            paramName: tok,
            paramToken: '@@' + tok,
            jobName, jobId, srcTable, tgtTable,
            srcCol, tgtCol,
            source: 'Was/Is rule',
            expression: String(oldVal) + ' → ' + newVal
          });
        }
      }
    }

    // Path 3: inline SQL — capture but don't attribute to a column
    for (const fieldName of ['insertSQL', 'otmSQL']){
      const sql = job[fieldName];
      if (typeof sql !== 'string' || !sql.includes('@@')) continue;
      // Dedupe tokens within a single SQL string — a token referenced
      // 50 times in one INSERT statement shows up once per job.
      const seen = new Set();
      for (const tok of usageExtractTokens(sql)){
        if (seen.has(tok)) continue;
        seen.add(tok);
        out.push({
          paramName: tok,
          paramToken: '@@' + tok,
          jobName, jobId, srcTable, tgtTable,
          srcCol: '', tgtCol: '',
          source: fieldName === 'insertSQL' ? 'inline INSERT SQL' : 'inline cursor SQL',
          expression: '(inside ' + fieldName + ')'
        });
      }
    }
  }
  return out;
}

// Public refresh API. Called by the Refresh button, by switchSysTab when
// the Usage tab is opened, and by usageSortBy. Also re-renders the
// Parameters tab so the "Used in" badge stays in sync with the scan.
function usageRefresh(){
  usageRows = usageScan();
  usageRender();
  // Keep the Parameters tab's "Used in" badge consistent with the latest
  // scan, in case the user came from the Usage tab back to Parameters
  // after editing a job.
  if (typeof spRender === 'function') spRender();
}

function usageApplyFilter(){
  usageFilterQuery = (document.getElementById('usage-filter-input')?.value || '').trim().toLowerCase();
  usageRender();
}
function usageClearFilter(){
  const el = document.getElementById('usage-filter-input');
  if (el){ el.value = ''; }
  usageFilterQuery = '';
  usageRender();
}
function usageSortBy(col){
  if (usageSortCol === col){ usageSortDir = -usageSortDir; }
  else { usageSortCol = col; usageSortDir = 1; }
  usageRender();
}

function usageRender(){
  const tbl    = document.getElementById('usage-table');
  const tbody  = document.getElementById('usage-tbody');
  const empty  = document.getElementById('usage-empty-state');
  const emptyF = document.getElementById('usage-empty-filter-state');
  const filterBar = document.getElementById('usage-filter-bar');
  const count  = document.getElementById('usage-filter-count');
  if (!tbl || !tbody || !empty || !filterBar) return;

  if (!usageRows.length){
    tbl.style.display = 'none';
    emptyF.style.display = 'none';
    empty.style.display = 'block';
    filterBar.style.display = 'none';
    return;
  }
  empty.style.display = 'none';
  filterBar.style.display = 'flex';

  // Filter
  const q = usageFilterQuery;
  let rows = usageRows;
  if (q){
    rows = rows.filter(r =>
      (r.paramName || '').toLowerCase().includes(q) ||
      (r.jobName   || '').toLowerCase().includes(q) ||
      (r.srcTable  || '').toLowerCase().includes(q) ||
      (r.tgtTable  || '').toLowerCase().includes(q) ||
      (r.srcCol    || '').toLowerCase().includes(q) ||
      (r.tgtCol    || '').toLowerCase().includes(q) ||
      (r.source    || '').toLowerCase().includes(q) ||
      (r.expression|| '').toLowerCase().includes(q)
    );
  }
  // Sort. Secondary sort always by paramName so usages of the same param
  // cluster together regardless of primary sort.
  rows = rows.slice().sort((a, b) => {
    const av = String(a[usageSortCol] || '').toLowerCase();
    const bv = String(b[usageSortCol] || '').toLowerCase();
    if (av < bv) return -1 * usageSortDir;
    if (av > bv) return  1 * usageSortDir;
    const an = String(a.paramName || '').toLowerCase();
    const bn = String(b.paramName || '').toLowerCase();
    if (an < bn) return -1;
    if (an > bn) return  1;
    return 0;
  });

  // Sort indicators
  ['paramName','jobName','srcTable','tgtCol','source'].forEach(c => {
    const el = document.getElementById('usage-sort-ind-'+c);
    if (!el) return;
    if (c === usageSortCol){
      el.textContent = usageSortDir === 1 ? '▲' : '▼';
      el.classList.add('active');
    } else {
      el.textContent = '⇅';
      el.classList.remove('active');
    }
  });

  if (count){
    if (q){
      count.textContent = rows.length + ' of ' + usageRows.length + ' reference' + (usageRows.length === 1 ? '' : 's');
    } else {
      count.textContent = usageRows.length + ' reference' + (usageRows.length === 1 ? '' : 's') + ' across ' +
        new Set(usageRows.map(r => r.jobName)).size + ' job(s)';
    }
  }
  const clr = document.getElementById('usage-filter-clear');
  if (clr) clr.style.display = q ? 'inline-flex' : 'none';

  if (!rows.length){
    tbl.style.display = 'none';
    emptyF.style.display = 'block';
    return;
  }
  emptyF.style.display = 'none';
  tbl.style.display = '';

  // Group rows visually by paramName when sort col is paramName — adds
  // a subtle row-divider so users can scan "all usages of @@Client" at
  // a glance.
  let prevParam = null;
  tbody.innerHTML = rows.map(r => {
    const firstOfGroup = (usageSortCol === 'paramName' && prevParam !== r.paramName);
    prevParam = r.paramName;
    const divider = firstOfGroup ? 'border-top:1px solid var(--border2)' : '';
    const exprDisp = r.expression.length > 80 ? r.expression.slice(0, 80) + '…' : r.expression;
    return `<tr style="${divider}">
      <td><code class="sp-cell-code" style="display:inline-block;padding:2px 6px">${spEsc(r.paramToken)}</code></td>
      <td style="font-size:12px">${spEsc(r.jobName)}</td>
      <td style="font-family:var(--mono);font-size:11px;color:var(--text2)">${spEsc(r.srcTable || '—')}</td>
      <td style="font-family:var(--mono);font-size:11px;color:var(--text2)">${spEsc(r.tgtCol || '—')}</td>
      <td style="font-size:11px;color:var(--text3)">${spEsc(r.source)}</td>
      <td style="font-family:var(--mono);font-size:11px;color:var(--text3);word-break:break-all" title="${spEsc(r.expression)}">${spEsc(exprDisp)}</td>
    </tr>`;
  }).join('');
}

// Per-parameter usage count — used by the Parameters tab's "Used in"
// badge so each parameter row can show "3 jobs" inline. Reads from the
// most recent usageRows scan; spRender calls usageScan if usageRows is
// empty so the count appears on first load too.
function usageCountForParam(paramName){
  if (!usageRows.length) return 0;
  return usageRows.filter(r => r.paramName.toLowerCase() === paramName.toLowerCase()).length;
}

// Jump from a parameter row's "X refs" button straight into the Where
// used tab, pre-filtered to that parameter. Keeps the UX one click —
// the user doesn't have to switch tabs and re-type the filter.
function usageJumpToParam(paramName){
  switchSysTab('usage');
  // Wait a tick for the tab to render, then set filter input + fire it.
  setTimeout(() => {
    const el = document.getElementById('usage-filter-input');
    if (el){
      el.value = paramName;
      usageApplyFilter();
    }
  }, 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// SYSTEM PARAMETERS — TAB SWITCHER (Parameters ↔ Was/Is ↔ Where used)
// ═══════════════════════════════════════════════════════════════════════════
function switchSysTab(tab){
  document.querySelectorAll('#view-system-parameters .cx-tab').forEach(el => {
    el.classList.toggle('cx-tab-active', el.dataset.spTab === tab);
  });
  const tabParams = document.getElementById('sp-tab-params');
  const tabWasis  = document.getElementById('sp-tab-wasis');
  const tabUsage  = document.getElementById('sp-tab-usage');
  const toolParams = document.getElementById('sp-toolbar-params');
  const toolWasis  = document.getElementById('sp-toolbar-wasis');
  const subParams  = document.getElementById('sp-view-sub-params');
  const subWasis   = document.getElementById('sp-view-sub-wasis');
  const title      = document.getElementById('sp-view-title');

  // Three-way tab visibility. The toolbars and sub-titles toggle alongside
  // the panels — the Usage tab has its own toolbar inline (Filter, Refresh)
  // so it doesn't need a top-bar toolbar like the others.
  const onParams = tab === 'params';
  const onWasis  = tab === 'wasis';
  const onUsage  = tab === 'usage';
  if (tabParams) tabParams.style.display = onParams ? '' : 'none';
  if (tabWasis)  tabWasis.style.display  = onWasis  ? '' : 'none';
  if (tabUsage)  tabUsage.style.display  = onUsage  ? '' : 'none';
  // Existing toolbars only apply to Params/Was-Is — Usage handles its own.
  if (toolParams) toolParams.style.display = onParams ? '' : 'none';
  if (toolWasis)  toolWasis.style.display  = onWasis  ? '' : 'none';
  if (subParams) subParams.style.display = onParams ? '' : 'none';
  if (subWasis)  subWasis.style.display  = onWasis  ? '' : 'none';
  if (title){
    title.textContent = onParams ? 'System parameters'
                      : onWasis  ? 'Was/Is value mapping'
                      :            'Where parameters are used';
  }

  // Lazy-init the Was/Is tab the first time it's opened
  if (onWasis && !wiInitialized){ wiLoad(); wiInitialized = true; }
  // Same pattern for Usage — scan only when the user actually opens it,
  // not on every page load. Re-scan on every tab open so changes made
  // in other tabs/pages show up.
  if (onUsage){ usageRefresh(); }
}

// ═══════════════════════════════════════════════════════════════════════════
// WAS/IS VALUE MAPPING VIEW
// ═══════════════════════════════════════════════════════════════════════════
// Storage key kept the same as object_mapping.html so both pages share data.
const WI_STORAGE_KEY = 'cygenix_wasis_rules';
let wiRules = [];
let wiFilterQuery = '';
let wiSortCol = null;  // 'srcTable' | 'srcField' | 'oldVal' | 'newVal' | 'desc' | null
let wiSortDir = 1;     // 1 asc, -1 desc
let wiInitialized = false;
let _wiSourceSchemaCache = null; // Map<lowercaseField, Set<tableName>> — see wiFetchSourceSchema

function wiEsc(s){ return String(s==null?'':s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

function wiNormaliseRule(r){
  // Preserve the rule id when present so saveRules can route the rule
  // through the union-by-id merge in cygenix-cosmos-sync.js. If id is
  // missing, defer to CygenixWasis.genId() — that helper owns the id
  // format so all callers stay in sync. Falls back to a local-shape id
  // only if the helper isn't loaded (defensive; shouldn't happen in
  // production where /cygenix-wasis.js is included before this file
  // executes).
  const existingId = r && r.id != null && String(r.id).trim() ? String(r.id) : '';
  const newId = existingId
    || (typeof CygenixWasis !== 'undefined' && CygenixWasis.genId ? CygenixWasis.genId() : 'wir_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7));
  return {
    id:       newId,
    srcTable: (r && r.srcTable != null ? String(r.srcTable) : '').toLowerCase().trim(),
    srcField: (r && r.srcField != null ? String(r.srcField) : '').toLowerCase().trim(),
    oldVal:   r && r.oldVal   != null ? String(r.oldVal)   : '',
    newVal:   r && r.newVal   != null ? String(r.newVal)   : '',
    desc:     r && r.desc     != null ? String(r.desc)     : ''
  };
}

function wiLoad(){
  // Clear the source-schema cache on (re)load so a freshly reconnected source
  // DB is picked up by the "copy to all tables" wand button.
  _wiSourceSchemaCache = null;
  // Prefer the shared helper when available so we benefit from inventory
  // fallback and stay consistent with object_mapping.html. Fall back to the
  // original localStorage read if the helper script didn't load for some
  // reason.
  if (typeof CygenixWasis !== 'undefined') {
    wiRules = CygenixWasis.getRules();
  } else {
    try { wiRules = JSON.parse(localStorage.getItem(WI_STORAGE_KEY) || '[]'); }
    catch { wiRules = []; }
    if (!Array.isArray(wiRules)) wiRules = [];
    wiRules = wiRules.map(wiNormaliseRule);
  }
  wiRender();
}

function wiSave(){
  // object_mapping.html expects srcTable/srcField lowercased. Preserve that.
  const toSave = wiRules.map(wiNormaliseRule);
  if (typeof CygenixWasis !== 'undefined') {
    CygenixWasis.saveRules(toSave);
  } else {
    try { localStorage.setItem(WI_STORAGE_KEY, JSON.stringify(toSave)); }
    catch(e){ console.warn('wiSave failed', e); }
  }
}

// Defensive flush + visible confirmation. Auto-save still runs on every edit
// via wiSave(); this button is a "yes I'm done" affordance that re-runs the
// save and surfaces a transient status so the user has explicit feedback.
function wiConfirmSave(){
  try {
    wiSave();
    flashSaveStatus('wi-save-status', '✓ Saved ' + wiRules.length + ' rule' + (wiRules.length === 1 ? '' : 's'), 'var(--green)');
  } catch (e) {
    console.error('[wi] confirm save failed', e);
    flashSaveStatus('wi-save-status', '✗ Save failed: ' + (e.message || 'unknown'), 'var(--red)');
  }
}

// Shared helper for both Save buttons. Shows a status pill next to the
// button for a couple of seconds, then fades out. Idempotent if called
// repeatedly — clears any prior fade-out timer first.
function flashSaveStatus(elId, text, color){
  const el = document.getElementById(elId);
  if (!el) return;
  el.textContent = text;
  el.style.color = color || 'var(--text2)';
  el.style.opacity = '1';
  if (el._fadeTimer) clearTimeout(el._fadeTimer);
  el._fadeTimer = setTimeout(() => { el.style.opacity = '0'; }, 2500);
}

function wiGetDisplayedRows(){
  const q = wiFilterQuery.trim().toLowerCase();
  let indexed = wiRules.map((r, origIdx) => ({ row: wiNormaliseRule(r), origIdx }));
  if (q){
    indexed = indexed.filter(({row}) =>
      row.srcTable.toLowerCase().includes(q) ||
      row.srcField.toLowerCase().includes(q) ||
      row.oldVal.toLowerCase().includes(q)   ||
      row.newVal.toLowerCase().includes(q)   ||
      row.desc.toLowerCase().includes(q)
    );
  }
  if (wiSortCol){
    indexed.sort((a, b) => {
      const av = (a.row[wiSortCol] || '').toLowerCase();
      const bv = (b.row[wiSortCol] || '').toLowerCase();
      if (av < bv) return -1 * wiSortDir;
      if (av > bv) return  1 * wiSortDir;
      return 0;
    });
  }
  return indexed;
}

function wiRender(){
  // Reflect the current connection state in the read-only banner. We do
  // this on every render so the dots stay accurate after any background
  // change (e.g. user re-saved a connection in another tab and the storage
  // event fired). Keep this fast — it only reads CygenixConnections, no
  // network calls.
  wiUpdateConnBanner();
  const tbl         = document.getElementById('wi-rules-table');
  const empty       = document.getElementById('wi-empty-state');
  const emptyFilter = document.getElementById('wi-empty-filter-state');
  const tbody       = document.getElementById('wi-rules-tbody');
  const filterBar   = document.getElementById('wi-filter-bar');
  if (!tbl || !empty || !tbody || !filterBar) return;

  if (!wiRules.length){
    tbl.style.display = 'none';
    empty.style.display = 'block';
    emptyFilter.style.display = 'none';
    filterBar.style.display = 'none';
    return;
  }
  empty.style.display = 'none';
  filterBar.style.display = 'flex';

  const displayed = wiGetDisplayedRows();

  // Sort indicators
  ['srcTable','srcField','oldVal','newVal','desc'].forEach(c => {
    const el = document.getElementById('wi-sort-ind-'+c);
    if (!el) return;
    if (c === wiSortCol){
      el.textContent = wiSortDir === 1 ? '▲' : '▼';
      el.classList.add('active');
    } else {
      el.textContent = '⇅';
      el.classList.remove('active');
    }
  });

  const count = document.getElementById('wi-filter-count');
  const clr   = document.getElementById('wi-filter-clear');
  if (count){
    if (wiFilterQuery.trim() || wiSortCol){
      count.textContent = displayed.length + ' of ' + wiRules.length;
    } else {
      count.textContent = wiRules.length + ' rule' + (wiRules.length === 1 ? '' : 's');
    }
  }
  if (clr) clr.style.display = wiFilterQuery.trim() ? 'inline-flex' : 'none';

  if (!displayed.length){
    tbl.style.display = 'none';
    emptyFilter.style.display = 'block';
    return;
  }
  emptyFilter.style.display = 'none';
  tbl.style.display = '';

  tbody.innerHTML = displayed.map(({row, origIdx}) => {
    const canFan = wiCanFanOut(row);
    const fanTitle = canFan
      ? 'Copy to all source tables containing field "' + row.srcField + '"'
      : 'Field, Old value, and New value must all be filled in to copy to other tables';
    const fanAttrs = canFan ? '' : 'disabled style="opacity:0.35;cursor:not-allowed"';
    return `<tr>
      <td><input class="sp-cell-input mono" value="${wiEsc(row.srcTable)}" oninput="wiUpdateField(${origIdx},'srcTable',this.value)" placeholder="any table"></td>
      <td><input class="sp-cell-input mono" value="${wiEsc(row.srcField)}" oninput="wiUpdateField(${origIdx},'srcField',this.value)" placeholder="any field"></td>
      <td><input class="sp-cell-input mono" value="${wiEsc(row.oldVal)}" oninput="wiUpdateField(${origIdx},'oldVal',this.value)" placeholder="old value"></td>
      <td><input class="sp-cell-input mono" value="${wiEsc(row.newVal)}" oninput="wiUpdateField(${origIdx},'newVal',this.value)" placeholder="new value"></td>
      <td><input class="sp-cell-input" value="${wiEsc(row.desc)}" oninput="wiUpdateField(${origIdx},'desc',this.value)" placeholder="optional note"></td>
      <td style="text-align:right;white-space:nowrap"><button id="wi-fan-${origIdx}" class="sp-icon-btn" onclick="wiCopyToAllTables(${origIdx})" title="${wiEsc(fanTitle)}" ${fanAttrs}><i class="ic ic-wand"></i> <span style="font-size:10px;color:var(--text3);margin-left:2px">copy to</span></button> <button class="sp-icon-btn danger" onclick="wiRemoveRow(${origIdx})" title="Delete">✕</button></td>
    </tr>`;
  }).join('');
}

function wiApplyFilter(){
  wiFilterQuery = document.getElementById('wi-filter-input').value || '';
  wiRender();
}

function wiClearFilter(){
  wiFilterQuery = '';
  const input = document.getElementById('wi-filter-input');
  if (input) input.value = '';
  wiRender();
}

function wiSortBy(col){
  if (wiSortCol !== col){
    wiSortCol = col; wiSortDir = 1;    // new column → asc
  } else if (wiSortDir === 1){
    wiSortDir = -1;                     // asc → desc
  } else {
    wiSortCol = null; wiSortDir = 1;    // desc → clear
  }
  wiRender();
}

function wiUpdateField(i, field, value){
  if (!wiRules[i]) return;
  // Lowercase srcTable and srcField at save time (matches object_mapping
  // expectations — SQL generation compares against r.srcField.toLowerCase()).
  // Keep the input value as-typed for this edit; renormalise on save.
  if (field === 'srcTable' || field === 'srcField'){
    wiRules[i][field] = (value || '').toLowerCase();
  } else {
    wiRules[i][field] = value;
  }
  wiSave();
  // The wand button's enabled/disabled state was baked into the row HTML at
  // render time and won't update on its own. We can't full-re-render here —
  // that would steal focus from the input the user is typing into. Instead
  // we update just this row's wand button in place. Only the three fields
  // that gate fan-out can flip the state, so skip the work otherwise.
  if (field === 'srcField' || field === 'oldVal' || field === 'newVal'){
    wiRefreshFanBtn(i);
  }
}

// Update the enabled/disabled state of one row's wand button without re-
// rendering the table. Mirrors the HTML produced in wiRender; keep the
// styling/title strings in sync with that template.
function wiRefreshFanBtn(i){
  const row = wiRules[i];
  if (!row) return;
  const btn = document.getElementById('wi-fan-' + i);
  if (!btn) return;
  const canFan = wiCanFanOut(row);
  if (canFan){
    btn.disabled = false;
    btn.style.opacity = '';
    btn.style.cursor = '';
    btn.title = 'Copy to all source tables containing field "' + (row.srcField || '') + '"';
  } else {
    btn.disabled = true;
    btn.style.opacity = '0.35';
    btn.style.cursor = 'not-allowed';
    btn.title = 'Field, Old value, and New value must all be filled in to copy to other tables';
  }
}

function wiAddRow(){
  // Assign id at creation. wiNormaliseRule would back-fill one on save, but
  // doing it here means the row has a stable identity from the moment it
  // exists — same pattern as job/project creation elsewhere.
  const newId = (typeof CygenixWasis !== 'undefined' && CygenixWasis.genId)
    ? CygenixWasis.genId()
    : 'wir_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7);
  wiRules.push({ id: newId, srcTable:'', srcField:'', oldVal:'', newVal:'', desc:'' });
  // Clear any active filter so the new blank row is visible
  if (wiFilterQuery.trim()){
    wiFilterQuery = '';
    const input = document.getElementById('wi-filter-input');
    if (input) input.value = '';
  }
  wiSave();
  wiRender();
  // Focus the new row's first input
  setTimeout(() => {
    const inputs = document.querySelectorAll('#wi-rules-tbody tr:last-child input');
    if (inputs[0]) inputs[0].focus();
  }, 50);
}

function wiRemoveRow(i){
  const r = wiRules[i];
  if (!r) return;
  const label = r.oldVal || r.newVal || r.srcField || r.srcTable;
  if (label && !confirm('Remove Was/Is rule "' + label + '"?')) return;
  wiRules.splice(i, 1);
  wiSave();
  wiRender();
}

// ── Copy-to-all-tables ─────────────────────────────────────────────────────
// Lets the user fan out a single Was/Is rule to every table in the SOURCE
// database that contains a column matching srcField. Saves typing the same
// oldVal→newVal across N tables and avoids forgetting any. The user can review
// and delete unwanted rows afterwards.

// Gate: row needs srcField + oldVal + newVal. srcTable is optional — its only
// purpose here is as the "originating" row, but the operation populates a
// table per match regardless.
function wiCanFanOut(row){
  return !!(row && (row.srcField || '').trim()
                && (row.oldVal   != null && String(row.oldVal).length > 0)
                && (row.newVal   != null && String(row.newVal).length > 0));
}

// Read connection settings directly from localStorage, bypassing
// CygenixConnections.
//
// Why bypass? Diagnostic on a real session showed `window.CygenixConnections`
// is undefined when the Was/Is panel renders — `connections.js` either
// fails to load on this view or fails to set its global. The data itself
// is fine: it sits in `cygenix_project_connections` in two possible shapes,
// either of which we accept here:
//
//   A) Flat (legacy save):
//      { srcConnString, srcConnMode, tgtConnString, tgtConnMode,
//        tgtFnUrl, tgtFnKey }
//
//   B) Per-user (current save path, keyed by email):
//      { "user@example.com": { srcConnString, ... }, ...flat keys also }
//
// In observed sessions both shapes coexist in the same blob (the per-user
// slice is added on top of the flat keys). We prefer the per-user slice
// matching the active account, fall back to flat, then to any per-user
// slice that has data.
//
// Returns: { srcConnString, tgtConnString, tgtFnUrl, tgtFnKey } — all strings,
// possibly empty. Never throws.
function wiReadConns(){
  let pc = {};
  try { pc = JSON.parse(localStorage.getItem('cygenix_project_connections') || '{}') || {}; } catch {}

  // 1) Per-user slice for the active user, if present.
  let activeUser = '';
  try { activeUser = localStorage.getItem('cygenix_active_user') || ''; } catch {}
  if (activeUser && pc[activeUser] && typeof pc[activeUser] === 'object'){
    const slice = pc[activeUser];
    if (slice.srcConnString || slice.srcFnUrl || slice.tgtConnString || slice.tgtFnUrl){
      return {
        srcConnString: slice.srcConnString || '',
        srcFnUrl:      slice.srcFnUrl      || '',
        srcFnKey:      slice.srcFnKey      || '',
        tgtConnString: slice.tgtConnString || '',
        tgtFnUrl:      slice.tgtFnUrl      || '',
        tgtFnKey:      slice.tgtFnKey      || '',
      };
    }
  }

  // 2) Flat top-level keys.
  if (pc.srcConnString || pc.srcFnUrl || pc.tgtConnString || pc.tgtFnUrl){
    return {
      srcConnString: pc.srcConnString || '',
      srcFnUrl:      pc.srcFnUrl      || '',
      srcFnKey:      pc.srcFnKey      || '',
      tgtConnString: pc.tgtConnString || '',
      tgtFnUrl:      pc.tgtFnUrl      || '',
      tgtFnKey:      pc.tgtFnKey      || '',
    };
  }

  // 3) Any per-user slice that has data (last resort — handles the case
  //    where active-user tag drifted but data exists under another tag).
  for (const k of Object.keys(pc)){
    const v = pc[k];
    if (v && typeof v === 'object' && (v.srcConnString || v.srcFnUrl || v.tgtConnString || v.tgtFnUrl)){
      return {
        srcConnString: v.srcConnString || '',
        srcFnUrl:      v.srcFnUrl      || '',
        srcFnKey:      v.srcFnKey      || '',
        tgtConnString: v.tgtConnString || '',
        tgtFnUrl:      v.tgtFnUrl      || '',
        tgtFnKey:      v.tgtFnKey      || '',
      };
    }
  }

  return { srcConnString:'', srcFnUrl:'', srcFnKey:'', tgtConnString:'', tgtFnUrl:'', tgtFnKey:'' };
}

// Read-only connection banner refresher. Reads localStorage directly (see
// wiReadConns above for why) and writes to the Was/Is panel's dots/labels.
// Called from wiRender(), so the banner is always fresh when the user
// opens the panel. No network calls.
function wiUpdateConnBanner(){
  const c = wiReadConns();
  const srcOk = !!(c.srcConnString || c.srcFnUrl);
  const tgtOk = !!(c.tgtFnUrl || c.tgtConnString);

  const setDot = (id, configured) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.className = 'conn-dot ' + (configured ? 'ok' : 'idle');
  };
  const setLabel = (id, configured, label, detail) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = configured ? (label + ': ' + detail) : (label + ': not configured');
  };

  // Best-effort DB name parse so the banner reads e.g. "Source: pfmegrnargs"
  // (matching object_mapping.html) rather than just "Source: configured".
  const parseDbName = (conn) => {
    if (!conn) return '';
    try {
      if (/^https?:\/\//i.test(conn)) {
        return new URL(conn).hostname.split('.')[0];
      }
      const m = conn.match(/(?:Database|Initial Catalog)\s*=\s*([^;]+)/i);
      if (m) return m[1].trim();
      const m2 = conn.match(/^[a-z]+:\/\/[^/]+\/([^?#]+)/i);
      if (m2) return decodeURIComponent(m2[1]);
    } catch {}
    return 'configured';
  };

  setDot('wi-src-dot', srcOk);
  setLabel('wi-src-label', srcOk, 'Source', parseDbName(c.srcConnString || c.srcFnUrl));
  setDot('wi-tgt-dot', tgtOk);
  setLabel('wi-tgt-label', tgtOk, 'Target', parseDbName(c.tgtFnUrl || c.tgtConnString));
}

// Cached source-schema lookup. Cleared on Was/Is panel reload so a freshly
// reconnected source is picked up. Returns Map<lowercaseField, Set<tableName>>.
async function wiFetchSourceSchema(){
  if (_wiSourceSchemaCache) return _wiSourceSchemaCache;

  // Resolve the source connection. We bypass CygenixConnections (which is
  // observed undefined on this view) and read localStorage directly via
  // wiReadConns — see that helper for the storage-shape contract. If the
  // source is an Azure Function, append the function key as ?code= so the
  // request authenticates (matching the target-side behaviour).
  const conns = wiReadConns();
  let conn = '';
  if (conns.srcFnUrl){
    conn = conns.srcFnUrl + (conns.srcFnKey
      ? (conns.srcFnUrl.includes('?') ? '&' : '?') + 'code=' + encodeURIComponent(conns.srcFnKey)
      : '');
  } else {
    conn = conns.srcConnString || '';
  }
  if (!conn){
    throw new Error('Source connection not configured. Open Connections → Source and save a connection first.');
  }

  // Inline copies of the dbCall/isFn helpers from object_mapping.html. We
  // duplicate (rather than depend on) those because they're scoped to that
  // page's script. Behaviour must stay identical: HTTP URL → POST directly
  // to the Function App; otherwise → POST to /.netlify/functions/db-connect
  // with connectionString in the body.
  const _isFn = (c) => !!(c && (c.startsWith('https://') || c.startsWith('http://')));
  const _dbCall = async (body) => {
    const url = _isFn(conn) ? conn : '/.netlify/functions/db-connect';
    const payload = _isFn(conn) ? body : { ...body, connectionString: conn };
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(60000),
    });
    const data = await res.json().catch(() => ({ error: 'Non-JSON (' + res.status + ')' }));
    if (!res.ok) throw new Error(data.error || res.statusText);
    return data;
  };

  // Try paginated schema-tables first (the same smart-fetch object_mapping
  // uses); fall back to legacy `schema` if the backend doesn't know it.
  // For our purposes, schema-tables alone is enough — it returns
  // tables[].columns for legacy backends, and tables[].name for paginated
  // backends. We only need column NAMES, so for paginated backends we then
  // need to fetch columns per table — which would be N round trips for a
  // 9k-table DB. To avoid that, we go straight to legacy `schema` here:
  // even on paginated-capable backends, `schema` still works and returns
  // everything in one call. The Was/Is fan-out is a one-shot user action,
  // not a hot path, so paying the bigger payload cost once is fine.
  const res = await _dbCall({ action: 'schema' });
  const tables = (res && res.tables) || [];

  const byField = new Map();
  tables.forEach(t => {
    const tname = t.name || '';
    const cols  = t.columns || [];
    if (!tname || !Array.isArray(cols)) return;
    cols.forEach(col => {
      // Columns can be {name, type, ...} objects or plain strings depending on
      // backend version — handle both.
      const cname = (typeof col === 'string') ? col : (col && col.name) || '';
      if (!cname) return;
      const key = cname.toLowerCase();
      if (!byField.has(key)) byField.set(key, new Set());
      byField.get(key).add(tname);
    });
  });

  _wiSourceSchemaCache = byField;
  return byField;
}

async function wiCopyToAllTables(i){
  const src = wiRules[i];
  if (!src) return;
  if (!wiCanFanOut(src)){
    alert('To copy this rule to other tables, fill in Field, Old value, and New value first.');
    return;
  }
  const fieldKey = src.srcField.toLowerCase().trim();

  let schema;
  try {
    schema = await wiFetchSourceSchema();
  } catch (e) {
    alert('Could not read source schema: ' + (e.message || e));
    return;
  }

  const tableSet = schema.get(fieldKey);
  if (!tableSet || tableSet.size === 0){
    alert('No tables in the source database contain a column named "' + src.srcField + '".');
    return;
  }
  const tables = Array.from(tableSet).sort();

  // Dedupe: skip any (table|field|oldVal) that already exists. Match how
  // wiNormaliseRule lowercases srcTable/srcField so comparisons are consistent.
  const existingKeys = new Set(
    wiRules.map(r => {
      const n = wiNormaliseRule(r);
      return n.srcTable + '|' + n.srcField + '|' + n.oldVal;
    })
  );

  const toAdd = [];
  let skipped = 0;
  const _genId = (typeof CygenixWasis !== 'undefined' && CygenixWasis.genId)
    ? CygenixWasis.genId
    : () => 'wir_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7);
  tables.forEach(t => {
    const key = t.toLowerCase() + '|' + fieldKey + '|' + src.oldVal;
    if (existingKeys.has(key)){ skipped++; return; }
    toAdd.push({
      id:       _genId(),
      srcTable: t.toLowerCase(),
      srcField: fieldKey,
      oldVal:   src.oldVal,
      newVal:   src.newVal,
      desc:     src.desc || ''
    });
  });

  if (toAdd.length === 0){
    alert('All ' + tables.length + ' matching tables already have this rule. Nothing to add.');
    return;
  }

  const msg = 'Found ' + tables.length + ' source table' + (tables.length === 1 ? '' : 's')
            + ' with field "' + src.srcField + '".\n\n'
            + 'This will add ' + toAdd.length + ' new rule' + (toAdd.length === 1 ? '' : 's')
            + (skipped ? ' (' + skipped + ' duplicate' + (skipped === 1 ? '' : 's') + ' skipped)' : '')
            + ' for "' + src.oldVal + '" → "' + src.newVal + '".\n\n'
            + 'You can delete any unwanted rows after review. Continue?';
  if (!confirm(msg)) return;

  wiRules.push(...toAdd);
  wiSave();
  wiRender();
  if (typeof showToast === 'function') showToast('Added ' + toAdd.length + ' rule' + (toAdd.length === 1 ? '' : 's'));
}

// ── CSV/JSON export ──────────────────────────────────────────────────────
function wiDownload(filename, content, mime){
  const blob = new Blob([content], { type: mime });
  const url  = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = filename;
  document.body.appendChild(a); a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function wiExportJSON(){
  const data = wiRules.map(wiNormaliseRule);
  wiDownload('cygenix-wasis-rules.json', JSON.stringify(data, null, 2), 'application/json');
  if (typeof showToast === 'function') showToast('Exported JSON');
}

function wiExportCSV(){
  const quote = v => { const s = String(v==null?'':v); return '"' + s.replace(/"/g,'""') + '"'; };
  const rows = [ ['table','field','oldVal','newVal','description'].join(',') ];
  wiRules.forEach(r => {
    const n = wiNormaliseRule(r);
    rows.push([n.srcTable, n.srcField, n.oldVal, n.newVal, n.desc].map(quote).join(','));
  });
  wiDownload('cygenix-wasis-rules.csv', rows.join('\n'), 'text/csv');
  if (typeof showToast === 'function') showToast('Exported CSV');
}

// ── Import (CSV / TSV / JSON / XLSX) ─────────────────────────────────────
function wiImportFile(input){
  const f = input.files && input.files[0];
  if (!f) return;
  const ext = (f.name.split('.').pop() || '').toLowerCase();

  const onDone = imported => {
    if (!Array.isArray(imported) || !imported.length){
      alert('No valid Was/Is rows found in the file.');
      input.value = '';
      return;
    }
    const replace = wiRules.length === 0 || confirm(
      'Replace all ' + wiRules.length + ' existing rule(s)?\n\n' +
      'OK = replace all with imported ' + imported.length + '\n' +
      'Cancel = append imported to existing'
    );
    if (replace){
      wiRules = imported.map(wiNormaliseRule);
    } else {
      wiRules = wiRules.concat(imported.map(wiNormaliseRule));
    }
    wiSave();
    wiRender();
    if (typeof showToast === 'function') showToast('Imported ' + imported.length + ' rule(s)');
    input.value = '';
  };

  const onFail = msg => { alert('Import failed: ' + msg); input.value = ''; };

  if (ext === 'json'){
    const reader = new FileReader();
    reader.onload = e => {
      try {
        const parsed = JSON.parse(e.target.result);
        if (!Array.isArray(parsed)) throw new Error('JSON must be an array of objects');
        onDone(parsed);
      } catch(err){ onFail(err.message); }
    };
    reader.readAsText(f);
    return;
  }

  if (ext === 'xlsx' || ext === 'xls'){
    // Load SheetJS dynamically if needed (same approach as object_mapping.html)
    const doParse = () => {
      const reader = new FileReader();
      reader.onload = e => {
        try {
          const wb = window.XLSX.read(e.target.result, { type:'array' });
          const ws = wb.Sheets[wb.SheetNames[0]];
          const rows = window.XLSX.utils.sheet_to_json(ws, { header:1, defval:'' });
          onDone(wiParseRows(rows));
        } catch(err){ onFail(err.message); }
      };
      reader.readAsArrayBuffer(f);
    };
    if (window.XLSX){ doParse(); return; }
    const s = document.createElement('script');
    s.src = 'https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js';
    s.onload = doParse;
    s.onerror = () => onFail('Failed to load XLSX library');
    document.head.appendChild(s);
    return;
  }

  // CSV / TSV / TXT
  const reader = new FileReader();
  reader.onload = e => {
    try { onDone(wiParseCSV(e.target.result)); }
    catch(err){ onFail(err.message); }
  };
  reader.readAsText(f);
}

function wiParseCSV(text){
  const delim = text.includes('\t') ? '\t' : ',';
  const rows = text.split('\n')
    .filter(l => l.trim())
    .map(l => l.split(delim).map(c => c.replace(/^"|"$/g,'').trim()));
  return wiParseRows(rows);
}

// Accepts a 2D array of cells (header row first) and returns normalised rules.
// Column names are matched loosely to tolerate the various header styles the
// old Wasis loader accepted (table/field/old/new/desc and their synonyms).
function wiParseRows(rows){
  if (!rows || !rows.length) return [];
  const hdr = rows[0].map(h => String(h||'').toLowerCase().replace(/\s+/g,''));
  const col = k => { const i = hdr.findIndex(h => h.includes(k)); return i >= 0 ? i : -1; };
  const iTable = (()=>{ const a = col('table'); return a >= 0 ? a : col('srctable'); })();
  const iField = (()=>{ const a = col('field'); if (a >= 0) return a; const b = col('col'); return b >= 0 ? b : col('column'); })();
  const iOld   = (()=>{ const a = col('old'); if (a >= 0) return a; const b = col('was'); if (b >= 0) return b; const c = col('from'); return c >= 0 ? c : col('source'); })();
  const iNew   = (()=>{ const a = col('new'); if (a >= 0) return a; const b = col('is');  if (b >= 0) return b; const c = col('to');   return c >= 0 ? c : col('target'); })();
  const iDesc  = (()=>{ const a = col('desc'); if (a >= 0) return a; const b = col('description'); return b >= 0 ? b : col('note'); })();

  return rows.slice(1)
    .filter(r => r && r.length >= 2 && r.join('').trim())
    .map(r => wiNormaliseRule({
      srcTable: iTable >= 0 ? r[iTable] : '',
      srcField: iField >= 0 ? r[iField] : '',
      oldVal:   iOld   >= 0 ? r[iOld]   : '',
      newVal:   iNew   >= 0 ? r[iNew]   : '',
      desc:     iDesc  >= 0 ? r[iDesc]  : ''
    }))
    .filter(r => r.oldVal !== '' || r.newVal !== '' || r.srcField !== '' || r.srcTable !== '');
}

// ═══ TASK AGENT ══════════════════════════════════════════════════════════
// Scheduler UI. Talks to /.netlify/functions/scheduler — a single Netlify
// Function that owns all reads/writes to the Cosmos scheduler containers
// (schedules, runs, job_versions). This works identically for Azure-target
// and direct-SQL-target projects because metadata storage is target-agnostic.
//
// Endpoint actions (POST body: { action, ...payload }, header: x-user-id):
//   • list-schedules  • create-schedule  • update-schedule
//   • delete-schedule • toggle-enabled   • list-runs   • get-run
//   • version-list    • version-create
//
// Execution: run-now is wired up in phase 2 (the ▶ Run button executes
// against the schedule's stored tgtConn via mssql server-side). Auto-
// triggers (cron firing without a browser open) arrive in phase 3.

const TA = {
  schedules: [],
  runs: [],
  versionsByJob: {},
  editingScheduleId: null,
  activeTab: 'schedules',
};

// Scheduler API base. Single Netlify Function endpoint — no Azure Function
// URL/key resolution needed any more. Kept as a function (not a const) so
// it's easy to swap to a different base in tests if needed.
function ta_apiBase() {
  return '/.netlify/functions/scheduler';
}

async function ta_apiCall(action, body) {
  const email = currentCygenixEmail();
  if (!email) throw new Error('Please sign in first.');

  // The scheduler endpoint now authenticates via the verified Entra token —
  // the x-user-id header is kept only as a transitional hint for logging.
  const idToken = (typeof getCygenixIdToken === 'function') ? getCygenixIdToken() : '';
  if (!idToken) throw new Error('Session expired — please sign in again.');

  const payload = { action, ...(body || {}) };
  const r = await fetch(ta_apiBase(), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + idToken,
      'x-user-id':    email,
    },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(60000),
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data.error || ('HTTP ' + r.status));
  return data;
}

// Both helpers now route through the same Netlify scheduler function with an
// action name in the body. Kept as two helpers (rather than collapsing to one)
// so the call sites in the rest of the Task Agent code stay readable: "this
// is a schedule action" vs "this is a version/data action".
function ta_sched(action, body) { return ta_apiCall(action, body); }
function ta_data(action, body)  {
  // Legacy callers passed query-string-style paths like
  // 'version-list?jobId=X'. Strip the query string and fold any params into
  // the body so the unified Netlify endpoint sees a clean { action, ...body }.
  const qIdx = action.indexOf('?');
  if (qIdx < 0) return ta_apiCall(action, body);
  const cleanAction = action.slice(0, qIdx);
  const params = {};
  new URLSearchParams(action.slice(qIdx + 1)).forEach((v, k) => { params[k] = v; });
  return ta_apiCall(cleanAction, { ...params, ...(body || {}) });
}

function ta_init() {
  ta_switchTab(TA.activeTab);
}

// Re-render the jobs table when a job is reverted via the History modal.
// CygenixHistory writes the reverted snapshot to cygenix_jobs in localStorage
// and emits this event — host pages re-read state and re-render. This
// mirrors the existing pattern at line ~11766 where state.jobs is re-hydrated
// from localStorage and then renderAllJobs() repaints the table.
window.addEventListener('cygenix:job-reverted', () => {
  try {
    state.jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]');
    if (typeof renderAllJobs === 'function') renderAllJobs();
  } catch (e) {
    console.warn('Could not refresh jobs after revert:', e);
  }
});

function ta_refresh() {
  if (TA.activeTab === 'schedules') ta_loadSchedules();
  else if (TA.activeTab === 'runs') ta_loadRuns();
}

function ta_switchTab(tab) {
  TA.activeTab = tab;
  ['schedules','runs'].forEach(t => {
    const tabEl = document.getElementById('ta-tab-' + t);
    const paneEl = document.getElementById('ta-pane-' + t);
    if (tabEl)  tabEl.classList.toggle('active', t === tab);
    if (paneEl) paneEl.style.display = (t === tab) ? '' : 'none';
  });
  if (tab === 'schedules') ta_loadSchedules();
  if (tab === 'runs')      ta_loadRuns();
}

// ── Schedules list ─────────────────────────────────────────────────────
async function ta_loadSchedules() {
  const el = document.getElementById('ta-schedules-list');
  el.innerHTML = '<div class="empty-state" style="padding:2rem"><p style="color:var(--text3)">Loading…</p></div>';
  try {
    const data = await ta_sched('list-schedules', {});
    TA.schedules = data.schedules || [];
    ta_renderSchedules();
  } catch (e) {
    el.innerHTML = '<div class="empty-state"><h3>Could not load schedules</h3><p style="color:var(--red)">' + escapeHtml(e.message) + '</p><p style="font-size:12px;color:var(--text3);margin-top:0.5rem">Check that the <code>schedules</code>, <code>runs</code>, and <code>job_versions</code> containers exist in the Cygenix Cosmos database, and that <code>COSMOS_CONNECTION_STRING</code> is set in Netlify.</p></div>';
  }
}

// Compute composite jobs (Project Builder → Create task) that don't yet
// have a schedule pointing at them. These live in localStorage.cygenix_jobs
// — the Task Agent's primary value-add is scheduling them, so surfacing
// them on the main screen is much more discoverable than hiding them
// inside the + New schedule dropdown.
function ta_unscheduledTasks() {
  const jobs = safeArr('cygenix_jobs');
  // Show two kinds of jobs in this panel:
  //   * composite — bundled multi-step migrations from Project Builder
  //   * profile-build — overnight Claude-AI schema analyses from Insights
  // Both arrive via a "📦 Create task" button in their respective pages
  // and exist precisely to be scheduled from here. Single-table import
  // jobs are excluded because they're rarely scheduled.
  const composites = jobs.filter(j =>
    j && (j.jobType === 'composite' || j.type === 'composite' || j.jobType === 'profile-build')
  );
  if (!composites.length) return [];
  const scheduledJobIds = new Set((TA.schedules || []).map(s => s.jobId).filter(Boolean));
  return composites.filter(j => !scheduledJobIds.has(j.id));
}

function ta_renderUnscheduledPanel() {
  const tasks = ta_unscheduledTasks();
  if (!tasks.length) return '';
  const rows = tasks.map(j => {
    // Build a meta line that fits the job type. Composite migration tasks
    // get step/group counts; profile-build tasks get database/role.
    let meta = '';
    if (j.jobType === 'profile-build') {
      const p = (j.profileBuildPayload || {});
      const parts = [
        'Profile build',
        p.database ? ('· ' + p.database) : '',
        p.role     ? ('(' + p.role + ')') : '',
      ].filter(Boolean);
      meta = parts.join(' ');
    } else {
      const stepCount = j.childStepCount || (Array.isArray(j.childSteps) ? j.childSteps.length : 0);
      const groups    = j.groupCount || '';
      meta = [
        stepCount ? (stepCount + ' step' + (stepCount === 1 ? '' : 's')) : '',
        groups    ? (groups + ' group' + (groups === 1 ? '' : 's'))     : '',
        j.projectName ? ('from ' + j.projectName) : '',
      ].filter(Boolean).join(' · ');
    }
    return `<div style="display:flex;align-items:center;justify-content:space-between;gap:1rem;padding:0.7rem 0.9rem;border:0.5px solid var(--border);border-radius:var(--r);background:var(--bg2);margin-bottom:0.5rem">
      <div style="min-width:0;flex:1">
        <div style="display:flex;align-items:center;gap:0.5rem">
          <span class="badge badge-amber" style="font-size:10px"><i class="ic ic-cog"></i> needs schedule</span>
          <strong style="color:var(--text)">${escapeHtml(j.name || '(unnamed)')}</strong>
        </div>
        ${meta ? '<div style="font-size:11px;color:var(--text3);margin-top:3px">' + escapeHtml(meta) + '</div>' : ''}
      </div>
      <div style="display:flex;align-items:center;gap:0.4rem">
        <button class="btn btn-primary btn-sm" onclick="ta_scheduleJob('${escapeAttr(j.id)}')" title="Open the schedule dialog with this task preselected">Schedule it</button>
        <button class="btn btn-sm" style="background:var(--red-bg);color:var(--red);border:0.5px solid rgba(240,70,70,0.25)" onclick="ta_deleteUnscheduledTask('${escapeAttr(j.id)}')" title="Delete this packaged task">✕</button>
      </div>
    </div>`;
  }).join('');
  return `<div style="margin-bottom:1.25rem">
    <div style="font-size:11px;color:var(--text3);text-transform:uppercase;letter-spacing:0.07em;margin-bottom:0.5rem">
      Tasks awaiting a schedule
      <span style="color:var(--text3);text-transform:none;letter-spacing:0;font-weight:400;margin-left:0.4rem">${tasks.length} packaged but not yet scheduled</span>
    </div>
    ${rows}
  </div>`;
}

// Open the schedule modal with a job preselected — used by the "Schedule it"
// button on the unscheduled-tasks panel.
function ta_scheduleJob(jobId) {
  ta_openScheduleModal(null, jobId);
}

// Delete a packaged-but-unscheduled task from this browser's cygenix_jobs.
// This only removes the local composite-job record — there's no Cosmos
// scheduler row to worry about (that's what "unscheduled" means). If the
// task DID have a schedule pointing at it, this row wouldn't be showing
// in the unscheduled panel in the first place, so the check is belt-and-
// braces only.
function ta_deleteUnscheduledTask(jobId) {
  let jobs = [];
  try { jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]'); } catch {}
  if (!Array.isArray(jobs)) jobs = [];
  const job = jobs.find(j => j && j.id === jobId);
  const name = (job && job.name) || '(unnamed task)';
  if (!confirm('Delete packaged task "' + name + '"? This removes it from this browser only — it can be re-packaged from Project Builder if needed.')) return;
  const next = jobs.filter(j => !(j && j.id === jobId));
  try {
    localStorage.setItem('cygenix_jobs', JSON.stringify(next));
  } catch (e) {
    alert('Could not delete task: ' + (e && e.message || 'storage error'));
    return;
  }
  ta_renderSchedules();
  if (typeof showToast === 'function') showToast('Task deleted');
}

function ta_renderSchedules() {
  const el = document.getElementById('ta-schedules-list');
  const unscheduledHtml = ta_renderUnscheduledPanel();

  if (!TA.schedules.length) {
    // Empty state — but still show unscheduled tasks above if there are any,
    // because that's the most likely reason the user opened this screen.
    el.innerHTML = unscheduledHtml +
      '<div class="empty-state" style="padding:2.5rem 2rem"><h3>No schedules yet</h3><p>' +
      (unscheduledHtml
        ? 'Click <strong>Schedule it</strong> above, or <strong>+ New schedule</strong> for a new one.'
        : 'Click <strong>+ New schedule</strong> to run a job on a cron or chain it after another.') +
      '</p></div>';
    return;
  }
  const rows = TA.schedules.map(s => {
    const trigger = s.chainAfter
      ? '<span class="badge badge-purple" title="Fires after parent succeeds"><i class="ic ic-chain"></i> Chained</span>'
      : '<span class="badge badge-blue" style="font-family:var(--mono)">' + escapeHtml(s.humanReadable || s.cron || '—') + '</span>';
    const next = s.nextRunAt
      ? '<span style="font-family:var(--mono);font-size:11px;color:var(--text2)">' + ta_fmtDateTime(s.nextRunAt) + '</span>'
      : '<span style="font-size:10px;color:var(--text3)">—</span>';
    const last = s.lastRunAt
      ? ta_runStatusChip(s.lastRunStatus) + ' <span style="font-size:11px;color:var(--text3);font-family:var(--mono);margin-left:0.4rem">' + ta_fmtDateTime(s.lastRunAt) + '</span>'
      : '<span style="font-size:10px;color:var(--text3)">Never run</span>';
    const jobName = ta_jobNameFor(s.jobId);
    const verLabel = s.jobVersionNumber ? 'v' + s.jobVersionNumber : 'version missing';
    return `<tr>
      <td><strong style="color:var(--text)">${escapeHtml(s.name)}</strong><div style="font-size:11px;color:var(--text3);margin-top:2px">${escapeHtml(jobName)} · ${verLabel}</div></td>
      <td>${trigger}</td>
      <td>${next}</td>
      <td>${last}</td>
      <td>
        <label style="display:inline-flex;align-items:center;gap:0.4rem;cursor:pointer" title="${s.enabled ? 'Disable' : 'Enable'}">
          <input type="checkbox" ${s.enabled ? 'checked' : ''} onchange="ta_toggleEnabled('${s.id}', this.checked)">
          <span style="font-size:11px;color:${s.enabled ? 'var(--green)' : 'var(--text3)'}">${s.enabled ? 'On' : 'Off'}</span>
        </label>
      </td>
      <td>
        <div class="jobs-actions">
          <button class="btn btn-primary btn-sm" onclick="ta_runNow('${s.id}', this)" title="Trigger now">▶ Run</button>
          <button class="btn btn-ghost btn-sm" onclick="ta_openScheduleModal('${s.id}')" title="Edit"><i class="ic ic-edit"></i> </button>
          <button class="btn btn-sm" style="background:var(--red-bg);color:var(--red);border:0.5px solid rgba(240,70,70,0.25)" onclick="ta_deleteSchedule('${s.id}')" title="Delete">✕</button>
        </div>
      </td>
    </tr>`;
  }).join('');
  el.innerHTML = unscheduledHtml +
    '<table class="jobs-table"><thead><tr>' +
    '<th>Name / Job</th><th>Trigger</th><th>Next run</th><th>Last run</th><th>Status</th><th>Actions</th>' +
    '</tr></thead><tbody>' + rows + '</tbody></table>';
}

function ta_runStatusChip(status) {
  if (status === 'success') return '<span class="badge badge-green" style="font-size:10px">✓ ok</span>';
  if (status === 'failed')  return '<span class="badge badge-red" style="font-size:10px">✕ failed</span>';
  if (status === 'running') return '<span class="badge badge-amber" style="font-size:10px">● running</span>';
  return '<span class="badge" style="font-size:10px">' + escapeHtml(status || '?') + '</span>';
}

function ta_fmtDateTime(iso) {
  try {
    const d = new Date(iso);
    return d.toLocaleString('en-GB', { day:'2-digit', month:'short', hour:'2-digit', minute:'2-digit' });
  } catch { return iso; }
}

function ta_jobNameFor(jobId) {
  try {
    const jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]');
    const j = jobs.find(j => j.id === jobId);
    // ta_jobBaseName rather than j.name: a job saved without one would
    // otherwise render the string "undefined" here, the same way it used to
    // in the schedule job picker.
    return j ? ta_jobBaseName(j) : '(job not on this browser)';
  } catch { return '—'; }
}

async function ta_toggleEnabled(id, enabled) {
  try {
    await ta_sched('toggle-enabled', { id, enabled });
    ta_loadSchedules();
    if (typeof showToast === 'function') showToast(enabled ? 'Schedule enabled' : 'Schedule disabled');
  } catch (e) { alert('Could not toggle: ' + e.message); }
}

async function ta_deleteSchedule(id) {
  const s = TA.schedules.find(x => x.id === id);
  if (!confirm('Delete schedule "' + (s && s.name || id) + '"? Run history is kept.')) return;
  try {
    await ta_sched('delete-schedule', { id });
    ta_loadSchedules();
    if (typeof showToast === 'function') showToast('Schedule deleted');
  } catch (e) { alert('Delete failed: ' + e.message); }
}

async function ta_runNow(id, btn) {
  if (!confirm('Run this schedule now?')) return;
  const orig = btn.innerHTML;
  btn.disabled = true; btn.innerHTML = '… queueing';
  try {
    // Phase 2b: run-now returns immediately with { status: 'queued', runId }
    // — the actual execution happens in the background function. We poll
    // get-run every 3 seconds until status flips to success/failed or 15
    // minutes elapse (matching the background function's hard cap).
    const queued = await ta_sched('run-now', { id });
    // Dispatched here rather than before the call: until the backend has
    // accepted the run there is nothing to announce, and a failed queueing
    // would otherwise send a "started" for a run that never began.
    notifyRunStarted(id, queued && queued.runId);
    if (queued.status !== 'queued' || !queued.runId) {
      // Belt-and-braces: support a future synchronous path returning final
      // status directly, so this code doesn't fight a later refactor.
      if (queued.status === 'success') {
        if (typeof showToast === 'function') showToast('Run succeeded — ' + (queued.rowsAffected || 0) + ' rows affected');
      } else if (queued.status === 'failed') {
        alert('Run failed:\n\n' + (queued.errorMessage || 'unknown error')
          + (queued.targetSummary ? '\n\nTarget it actually ran against:\n' + queued.targetSummary : ''));
      }
      ta_loadSchedules();
      return;
    }

    btn.innerHTML = '… running';
    if (typeof showToast === 'function') showToast('Run started in background — polling for completion');

    const POLL_MS = 3000;
    const MAX_POLLS = (15 * 60 * 1000) / POLL_MS; // 15 min cap
    let polls = 0;
    let finalRun = null;
    while (polls < MAX_POLLS) {
      await new Promise(r => setTimeout(r, POLL_MS));
      polls++;
      let r;
      try {
        r = await ta_sched('get-run', { id: queued.runId, scheduleId: id });
      } catch (e) {
        // Transient: keep polling rather than failing the whole flow.
        continue;
      }
      const status = r && r.run && r.run.status;
      // Update the button periodically so the user sees progress
      if (status === 'queued')  btn.innerHTML = '… queued';
      if (status === 'running') btn.innerHTML = '… running (' + (polls * POLL_MS / 1000) + 's)';
      if (status === 'success' || status === 'failed') {
        finalRun = r.run;
        break;
      }
    }

    if (!finalRun) {
      alert('Run did not complete within 15 minutes. Check Run history for status — it may still complete.');
    } else if (finalRun.status === 'success') {
      const rows = finalRun.rowsAffected || 0;
      const stepCount = (finalRun.stepResults || []).length;
      if (typeof showToast === 'function') showToast('Run succeeded — ' + rows.toLocaleString() + ' rows across ' + stepCount + ' step' + (stepCount === 1 ? '' : 's'));
    } else {
      alert('Run failed:\n\n' + (finalRun.errorMessage || 'unknown error')
        + (finalRun.targetSummary ? '\n\nTarget it actually ran against:\n' + finalRun.targetSummary
            + '\n\nA schedule uses the connection stored when it was created, which may differ from Connections.' : '')
        + '\n\nSee Run history for step-level detail.');
    }
    // Notify the enabled integrations. Until now nothing in the app called
    // emit(), so the "Enabled" checkbox on a connector controlled nothing and
    // Webhook only ever fired from its own Test button. This is the terminal
    // state of a run — the event those connectors were built to carry.
    if (finalRun) notifyIntegrations(finalRun, id);
    ta_loadSchedules();
  } catch (e) {
    alert('Run failed: ' + e.message);
  } finally {
    btn.disabled = false; btn.innerHTML = orig;
  }
}

// Fire-and-forget dispatch of a finished run to every enabled connector.
//
// Deliberately not awaited by the caller and never allowed to throw: a mail
// server that refuses the login, or a webhook URL that has gone dead, must not
// turn a successful migration into a visible failure. Failures are logged by
// emit() and surface in the connector's own Test button.
// ══════════════════════════════════════════════════════════════════════════
// TASK AGENT — job picker
// ══════════════════════════════════════════════════════════════════════════
// The picker used to render every entry in cygenix_jobs as its bare name.
// Three things made that unusable:
//
//   1. Trashed jobs were listed. Soft-deleted jobs are hidden everywhere else
//      and cannot sensibly be scheduled.
//   2. Jobs saved by the Connect flow are named after the PROJECT, not the
//      table pair — so a dozen conversion runs under "Demo" produced a dozen
//      entries all reading "Demo". They are distinct jobs, not duplicates,
//      but nothing on screen said which was which.
//   3. A job with no name at all rendered the string "undefined".
//
// Names are left alone; the label is what changes. A name that is already
// unique is shown as-is, and only a name shared with another job gains the
// route and date that tell them apart — so clean lists stay clean.

// Names used by more than one job, lower-cased. Only these need qualifying.
function ta_duplicateJobNames(jobs){
  const seen = new Map();
  jobs.forEach(j => {
    const k = ta_jobBaseName(j).toLowerCase();
    seen.set(k, (seen.get(k) || 0) + 1);
  });
  const dupes = new Set();
  seen.forEach((n, k) => { if (n > 1) dupes.add(k); });
  return dupes;
}

function ta_jobBaseName(j){
  const n = String((j && j.name) || '').trim();
  if (n) return n;
  // No name — fall back to something that identifies it rather than printing
  // "undefined", which is what the old picker did.
  const route = [j && (j.sourceTable || j.source), j && (j.targetTable || j.target)]
    .filter(Boolean).join(' → ');
  return route || 'Untitled job';
}

function ta_jobLabel(j, dupes){
  const base = ta_jobBaseName(j);
  if (!dupes.has(base.toLowerCase())) return base;
  const route = [j.sourceTable || j.source, j.targetTable || j.target].filter(Boolean).join(' → ');
  let when = '';
  if (j.created){
    const d = new Date(j.created);
    if (!isNaN(d)) when = d.toLocaleDateString('en-GB', { day:'numeric', month:'short' });
  }
  const extra = [route, when].filter(Boolean).join(' · ');
  return extra ? base + '  ·  ' + extra : base;
}

// Every job the picker will offer, newest first. Callers that reason about
// the selection — the preselect and default-pick logic in the schedule modal
// — must use this rather than reading cygenix_jobs, or they end up disagreeing
// with what is on screen about which jobs exist.
function ta_pickableJobs(){
  return liveJobs().slice().sort((a, b) =>
    String(b.created || '').localeCompare(String(a.created || '')));
}

// Jobs belonging to the active project are grouped first. Nothing is hidden —
// scheduling a job from another project is legitimate, so the others stay
// listed under their own heading rather than being filtered out.
function ta_jobOptions(){
  const jobs = ta_pickableJobs();
  if (!jobs.length) return '<option value="">No jobs saved on this browser</option>';

  const activeId = (localStorage.getItem('cygenix_active_project_id') || '').trim();
  const projects = safeArr('cygenix_projects');
  const active   = projects.find(p => p.id === activeId);
  const dupes    = ta_duplicateJobNames(jobs);
  const opt = j => '<option value="' + escapeAttr(j.id) + '">' + escapeHtml(ta_jobLabel(j, dupes)) + '</option>';

  if (!activeId) return jobs.map(opt).join('');

  // A job with no projectId predates the field or was saved by a flow that
  // never set it; treat it as belonging to whatever is active rather than
  // stranding it under "Other projects".
  const mine   = jobs.filter(j => !j.projectId || j.projectId === activeId);
  const others = jobs.filter(j => j.projectId && j.projectId !== activeId);
  if (!others.length) return mine.map(opt).join('');

  const label = (active && active.name) ? active.name : 'This project';
  return '<optgroup label="' + escapeAttr(label) + '">' + mine.map(opt).join('') + '</optgroup>' +
         '<optgroup label="Other projects">' + others.map(opt).join('') + '</optgroup>';
}

// ══════════════════════════════════════════════════════════════════════════
// NOTIFICATIONS VIEW — Settings → Notifications
// ══════════════════════════════════════════════════════════════════════════
// Two independent switches govern delivery, and the page says so rather than
// letting a user turn an event on and wonder why nothing arrives:
//
//   1. the event, here          — which run outcomes are worth a message
//   2. the connector, in Integrations — where those messages go
//
// The connector list below is read-only and reflects Integrations, so this
// page can show whether anything would actually receive an event.

const NOTIFY_EVENT_META = [
  { key:'started',   label:'Run started',
    event:'cygenix.migration.started',
    detail:'Sent as soon as the backend accepts a run. Useful for long migrations; doubles the number of messages.' },
  { key:'completed', label:'Run completed',
    event:'cygenix.migration.success',
    detail:'Sent when a run finishes successfully. Carries the row count and the number of steps.' },
  { key:'failed',    label:'Run failed',
    event:'cygenix.migration.failed',
    detail:'Sent when a run ends in failure. Carries the error message from the run.' },
];

function initNotificationsView(){
  renderNotifyEvents();
  renderNotifyConnectors();
  loadNotifyServer();
  loadNotifyLog();
}

// ── Server-side delivery ──────────────────────────────────────────────────
// The connectors live in this browser: their URLs and credentials are in
// localStorage and emit() runs on this page. That is fine for Run now, and
// useless for a schedule — a cron tick fires in Azure with no browser
// anywhere, which is why scheduled runs never delivered to a connector no
// matter what the switches above said.
//
// Turning this on copies the connector settings and the event switches to
// your Cygenix account, so the Function App can deliver them itself. It is
// opt-in because it moves an SMTP password out of this browser, and the
// Integrations page promises credentials stay here.
const NT = { server: null, log: null };

async function loadNotifyServer(){
  const host = document.getElementById('nt-server');
  if (host) host.innerHTML = '<div style="font-size:12px;color:var(--text3);padding:0.75rem 0">Loading…</div>';
  try {
    const r = await ta_sched('get-notify-settings', {});
    NT.server = r.settings || null;
  } catch (e) {
    NT.server = { __error: (e && e.message) || 'could not load' };
  }
  renderNotifyServer();
}

function renderNotifyServer(){
  const host = document.getElementById('nt-server');
  if (!host) return;
  const s = NT.server;

  if (s && s.__error){
    host.innerHTML = '<div class="in-form-note warn" style="margin-top:0.5rem">Could not read your account settings: '
      + escIn(s.__error) + '</div>';
    return;
  }
  if (!s){ host.innerHTML = '<div style="font-size:12px;color:var(--text3);padding:0.75rem 0">Loading…</div>'; return; }

  const I = window.CygenixIntegrations;
  const localOn = ['webhook','smtp'].filter(id => I && I.isEnabled(id));
  const synced  = s.updatedAt ? new Date(s.updatedAt).toLocaleString() : null;

  // What the server currently holds, so "it says it's on but nothing arrives"
  // has a visible cause rather than being a guess.
  const serverOn = ['webhook','smtp'].filter(id => s[id] && s[id].enabled);

  let detail = '';
  if (s.serverDelivery){
    detail = serverOn.length
      ? '<div style="font-size:12px;color:var(--text2);margin-top:0.5rem;line-height:1.6">The server will deliver to '
        + '<strong>' + serverOn.map(escIn).join('</strong> and <strong>') + '</strong>'
        + (synced ? ', from settings copied ' + escIn(synced) : '') + '.</div>'
      : '<div class="in-form-note warn" style="margin-top:0.5rem">Server delivery is on, but no connector was copied up. '
        + 'Enable Webhook or Email (SMTP) in Integrations, then use “Copy settings to my account” below.</div>';
    // Drift between the two copies is invisible otherwise: a user edits SMTP
    // in Integrations and the schedule keeps using the old settings.
    if (localOn.join() !== serverOn.join()){
      detail += '<div class="in-form-note warn" style="margin-top:0.5rem">This browser has '
        + (localOn.length ? '<strong>' + localOn.map(escIn).join('</strong>, <strong>') + '</strong>' : 'nothing')
        + ' enabled, which does not match the account copy. Copy the settings up to bring them into line.</div>';
    }
  }

  host.innerHTML = `
    <div class="in-enable-row" style="border-top:0;padding:0.75rem 0;margin:0;align-items:flex-start">
      <input type="checkbox" id="nt-server-on" ${s.serverDelivery ? 'checked' : ''}
             onchange="onServerDeliveryToggle(this.checked)" style="margin-top:2px">
      <label for="nt-server-on" style="display:block;cursor:pointer">
        <span style="font-size:13px;color:var(--text);font-weight:500">Deliver notifications for scheduled runs</span>
        <div style="font-size:11.5px;color:var(--text3);margin-top:2px;line-height:1.5;text-transform:none;letter-spacing:0">
          Copies your enabled connectors and the event switches above to your Cygenix account so the
          server can send them when a schedule fires. <strong>This includes the SMTP password and any
          webhook secret</strong> — they leave this browser and are stored against your account.
          Leave it off and only runs you start yourself will notify.
        </div>
      </label>
    </div>
    ${detail}
    <div style="margin-top:0.9rem;display:flex;align-items:center;gap:0.6rem;flex-wrap:wrap">
      <button class="btn btn-sm" onclick="pushNotifySettings(true)" ${s.serverDelivery ? '' : 'disabled'}><i class="ic ic-upload"></i> Copy settings to my account</button>
      <span id="nt-server-result" style="font-size:11.5px;font-family:var(--mono)"></span>
    </div>`;
}

async function onServerDeliveryToggle(on){
  // Turning it on has to upload something, or the switch is on and the
  // account is empty. Turning it off clears the stored credentials rather
  // than leaving them sitting there unused.
  await pushNotifySettings(true, on);
}

// Build the account copy from what this browser actually has. The connector
// settings are read live rather than from a cached copy so the upload cannot
// disagree with what Integrations shows.
function notifySettingsPayload(serverDelivery){
  const I = window.CygenixIntegrations;
  const w = (I && I.getConfig('webhook')) || {};
  const s = (I && I.getConfig('smtp'))    || {};
  const on = !!serverDelivery;
  return {
    serverDelivery: on,
    events: notifyEvents(),
    webhook: {
      enabled: on && !!(I && I.isEnabled('webhook')),
      url:     on ? (w.url || '')    : '',
      secret:  on ? (w.secret || '') : '',
    },
    smtp: {
      enabled: on && !!(I && I.isEnabled('smtp')),
      host: on ? (s.host || '') : '',
      port: on ? String(s.port || '') : '',
      user: on ? (s.user || '') : '',
      pass: on ? (s.pass || '') : '',
      from: on ? (s.from || '') : '',
      to:   on ? (s.to   || '') : '',
    },
  };
}

async function pushNotifySettings(showResult, forceOn){
  const el = showResult ? document.getElementById('nt-server-result') : null;
  const on = (forceOn === undefined)
    ? !!(NT.server && NT.server.serverDelivery)
    : !!forceOn;
  if (el){ el.textContent = 'saving…'; el.style.color = 'var(--text3)'; }
  try {
    const r = await ta_sched('save-notify-settings', { settings: notifySettingsPayload(on) });
    NT.server = r.settings || null;
    renderNotifyServer();
    const el2 = document.getElementById('nt-server-result');
    if (el2 && showResult){
      el2.textContent = on ? '✓ saved to your account' : '✓ removed from your account';
      el2.style.color = 'var(--green)';
    }
  } catch (e) {
    const el2 = document.getElementById('nt-server-result');
    if (el2 && showResult){
      el2.textContent = '✕ ' + ((e && e.message) || 'save failed');
      el2.style.color = 'var(--red)';
    }
  }
}

// ── The delivery log ──────────────────────────────────────────────────────
// Every send the server attempts is recorded, successful or not. Without
// this, "I never got an email" has no answer short of reading Cosmos.
async function loadNotifyLog(){
  const host = document.getElementById('nt-log');
  if (host) host.innerHTML = '<div style="font-size:12px;color:var(--text3);padding:0.75rem 0">Loading…</div>';
  try {
    const r = await ta_sched('list-notifications', { limit: 25 });
    NT.log = r;
  } catch (e) {
    NT.log = { error: (e && e.message) || 'could not load' };
  }
  renderNotifyLog();
}

const NT_CHANNEL_LABEL = { resend: 'Email', smtp: 'Email (SMTP)', webhook: 'Webhook' };

function renderNotifyLog(){
  const host = document.getElementById('nt-log');
  if (!host) return;
  const r = NT.log || {};

  if (r.error){
    host.innerHTML = '<div class="in-form-note warn" style="margin-top:0.5rem">Could not read the log: '
      + escIn(r.error) + '</div>';
    return;
  }
  if (r.unavailable){
    host.innerHTML = '<div class="in-form-note warn" style="margin-top:0.5rem">' + escIn(r.unavailable) + '</div>';
    return;
  }
  const rows = r.notifications || [];
  if (!rows.length){
    host.innerHTML = '<div style="font-size:12px;color:var(--text3);padding:0.75rem 0">'
      + 'Nothing yet. A row appears here each time the server tries to notify you — including when it fails.</div>';
    return;
  }

  host.innerHTML = rows.map(n => {
    const okStatus = n.status === 'sent';
    const colour = okStatus ? 'var(--green)' : (n.status === 'skipped' ? 'var(--text3)' : 'var(--red)');
    const bg     = okStatus ? 'var(--green-bg)' : 'var(--bg3)';
    const when   = n.sentAt ? new Date(n.sentAt).toLocaleString() : '';
    return `
      <div style="display:flex;align-items:flex-start;gap:0.6rem;padding:0.6rem 0;border-top:0.5px solid var(--border)">
        <span style="font-size:10px;font-family:var(--mono);padding:2px 8px;border-radius:100px;background:${bg};color:${colour};flex:none;margin-top:1px">${escIn(n.status || '?')}</span>
        <div style="min-width:0;flex:1">
          <div style="font-size:12.5px;color:var(--text)">
            ${escIn(NT_CHANNEL_LABEL[n.channel] || n.channel || 'Email')}
            ${n.to ? '<span style="color:var(--text3)"> → ' + escIn(n.to) + '</span>' : ''}
          </div>
          <div style="font-size:11px;color:var(--text3);margin-top:2px;line-height:1.5;word-break:break-word">
            ${escIn(n.type || '')}${n.source ? ' · ' + escIn(n.source) : ''}
            ${n.error ? '<br><span style="color:var(--red)">' + escIn(n.error) + '</span>' : ''}
          </div>
        </div>
        <span style="font-size:10.5px;color:var(--text3);font-family:var(--mono);flex:none;margin-top:1px">${escIn(when)}</span>
      </div>`;
  }).join('');
}

function renderNotifyEvents(){
  const host = document.getElementById('nt-events');
  if (!host) return;
  const on = notifyEvents();
  host.innerHTML = NOTIFY_EVENT_META.map(e => `
    <div class="in-enable-row" style="border-top:0.5px solid var(--border);padding:0.75rem 0;margin:0;align-items:flex-start">
      <input type="checkbox" id="nt-ev-${escIn(e.key)}" ${on[e.key] ? 'checked' : ''}
             onchange="onNotifyEventToggle('${escIn(e.key)}', this.checked)" style="margin-top:2px">
      <label for="nt-ev-${escIn(e.key)}" style="display:block;cursor:pointer">
        <span style="font-size:13px;color:var(--text);font-weight:500">${escIn(e.label)}</span>
        <span style="font-family:var(--mono);font-size:10px;color:var(--text3);margin-left:0.5rem">${escIn(e.event)}</span>
        <div style="font-size:11.5px;color:var(--text3);margin-top:2px;line-height:1.5;text-transform:none;letter-spacing:0">${escIn(e.detail)}</div>
      </label>
    </div>`).join('');
}

function onNotifyEventToggle(key, on){
  setNotifyEvent(key, on);
  renderNotifyConnectors();   // the "nothing is listening" warning may change
  // Keep the account copy in step, or a switch flipped here would go on
  // applying to Run now while scheduled runs kept the old answer.
  if (NT.server && NT.server.serverDelivery) pushNotifySettings(false);
  if (typeof showToast === 'function'){
    const meta = NOTIFY_EVENT_META.find(e => e.key === key);
    showToast((meta ? meta.label : key) + (on ? ' notifications on' : ' notifications off'));
  }
}

// Read-only mirror of Integrations, so this page can answer "will anything
// actually receive this?" without the user having to go and look.
function renderNotifyConnectors(){
  const host = document.getElementById('nt-connectors');
  if (!host) return;
  const I = window.CygenixIntegrations;
  if (!I){ host.innerHTML = '<div style="font-size:12px;color:var(--text3)">Integrations unavailable.</div>'; return; }

  // Only connectors that emit() actually dispatches to — listing the export
  // and import connectors here would imply they receive events, which they
  // do not.
  const receivers = ['webhook', 'smtp']
    .map(id => I.getConnector(id))
    .filter(Boolean)
    .map(c => ({ ...c, enabled: I.isEnabled(c.id) }));

  const anyEvent = NOTIFY_EVENT_META.some(e => notifyEvents()[e.key]);
  const anyConn  = receivers.some(c => c.enabled);

  const rows = receivers.map(c => `
    <div style="display:flex;align-items:center;gap:0.6rem;padding:0.6rem 0;border-top:0.5px solid var(--border)">
      <span style="font-size:16px">${c.icon}</span>
      <span style="font-size:12.5px;color:var(--text)">${escIn(c.name)}</span>
      <span style="margin-left:auto;font-size:10px;font-family:var(--mono);padding:2px 8px;border-radius:100px;${
        c.enabled ? 'background:var(--green-bg);color:var(--green)' : 'background:var(--bg3);color:var(--text3)'
      }">${c.enabled ? '● receiving' : 'off'}</span>
      <button class="btn btn-sm" onclick="showView('integrations')">Configure</button>
    </div>`).join('');

  // Say plainly when a switch is on but nothing can act on it — that gap is
  // otherwise silent, and looks like the feature is broken.
  let warn = '';
  if (anyEvent && !anyConn){
    warn = `<div class="in-form-note warn" style="margin-top:0.75rem">
      Events are switched on above, but no connector is enabled — nothing will be delivered.
      Enable Webhook or Email (SMTP) in Integrations.</div>`;
  } else if (!anyEvent && anyConn){
    warn = `<div class="in-form-note warn" style="margin-top:0.75rem">
      A connector is enabled, but every event above is switched off — nothing will be sent.</div>`;
  }

  const testBtn = anyConn
    ? `<div style="margin-top:0.9rem;display:flex;align-items:center;gap:0.6rem;flex-wrap:wrap">
         <button class="btn btn-sm" onclick="sendTestNotification()"><i class="ic ic-flask"></i> Send a test event</button>
         <span style="font-size:11px;color:var(--text3)">Delivers a sample "completed" event to every enabled connector.</span>
         <span id="nt-test-result" style="font-size:11.5px;font-family:var(--mono)"></span>
       </div>` : '';

  host.innerHTML = rows + warn + testBtn;
}

// Ignores the event switches on purpose: this proves the connectors work,
// which is a different question from whether a given event is switched on.
async function sendTestNotification(){
  const el = document.getElementById('nt-test-result');
  if (el){ el.textContent = 'sending…'; el.style.color = 'var(--text3)'; }
  try {
    const results = await window.CygenixIntegrations.emit('cygenix.migration.success', {
      status: 'success', project: 'Test', job: 'Test notification',
      rows: 0, steps: 0, message: 'This is a test event from Settings → Notifications.',
    });
    if (!el) return;
    if (!results.length){ el.textContent = '✕ no connector enabled'; el.style.color = 'var(--amber)'; return; }
    const bad = results.filter(r => !r.ok);
    if (bad.length){
      el.textContent = '✕ ' + bad.map(r => r.id + ': ' + r.message).join(' · ');
      el.style.color = 'var(--red)';
    } else {
      el.textContent = '✓ delivered to ' + results.map(r => r.id).join(', ');
      el.style.color = 'var(--green)';
    }
  } catch (e) {
    if (el){ el.textContent = '✕ ' + (e && e.message || 'failed'); el.style.color = 'var(--red)'; }
  }
}

// ── Which run events are allowed to notify ────────────────────────────────
// Settings → Notifications writes this. Two switches have to be on for
// anything to be delivered: the event here, and the connector itself in
// Integrations. Keeping them separate means "stop emailing me about starts"
// does not require turning the whole connector off.
//
// Defaults deliberately mirror what the app did before this page existed:
// completed and failed on, started off. Turning `started` on doubles the
// message volume, so it is opt-in.
const NOTIFY_EVENTS_KEY = 'cygenix_notify_events';
const NOTIFY_EVENT_DEFAULTS = { started: false, completed: true, failed: true };

function notifyEvents(){
  let saved = {};
  try { saved = JSON.parse(localStorage.getItem(NOTIFY_EVENTS_KEY) || '{}') || {}; } catch {}
  // Spread over the defaults rather than replacing them, so a key added in a
  // later release is on/off by its default rather than undefined.
  return { ...NOTIFY_EVENT_DEFAULTS, ...saved };
}

function setNotifyEvent(key, on){
  const next = { ...notifyEvents(), [key]: !!on };
  try { localStorage.setItem(NOTIFY_EVENTS_KEY, JSON.stringify(next)); } catch {}
  return next;
}

// Map a run to its event key. 'started' is dispatched separately at kick-off.
function runEventKey(status){
  return status === 'success' ? 'completed' : 'failed';
}

function notifyIntegrations(finalRun, scheduleId){
  try {
    if (!window.CygenixIntegrations || typeof window.CygenixIntegrations.emit !== 'function') return;
    const key = runEventKey(finalRun.status);
    if (!notifyEvents()[key]) return;      // event switched off in Settings
    const ok = finalRun.status === 'success';
    window.CygenixIntegrations.emit(
      ok ? 'cygenix.migration.success' : 'cygenix.migration.failed',
      { ...notifyContext(scheduleId),
        status:  finalRun.status,
        rows:    finalRun.rowsAffected || 0,
        steps:   (finalRun.stepResults || []).length,
        runId:   finalRun.id || '',
        message: ok ? '' : (finalRun.errorMessage || 'unknown error'),
      }
    ).catch(e => console.warn('[integrations] emit failed:', e && e.message));
  } catch (e) {
    console.warn('[integrations] emit skipped:', e && e.message);
  }
}

// Fired when a run is handed to the backend, before it finishes. Useful as a
// "long job has begun" ping; off by default because it doubles the volume.
function notifyRunStarted(scheduleId, runId){
  try {
    if (!window.CygenixIntegrations || typeof window.CygenixIntegrations.emit !== 'function') return;
    if (!notifyEvents().started) return;
    window.CygenixIntegrations.emit('cygenix.migration.started',
      { ...notifyContext(scheduleId), status:'started', runId: runId || '' }
    ).catch(e => console.warn('[integrations] emit failed:', e && e.message));
  } catch (e) {
    console.warn('[integrations] emit skipped:', e && e.message);
  }
}

// The project/job labels every event carries, so a recipient can tell which
// migration a message is about without opening the app.
function notifyContext(scheduleId){
  const sched = (TA.schedules || []).find(s => s.id === scheduleId);
  const projects = safeArr('cygenix_projects');
  const activeId = localStorage.getItem('cygenix_active_project_id') || '';
  const project  = projects.find(p => p.id === activeId);
  return {
    project: project ? (project.name || '') : '',
    job:     sched ? (sched.name || '') : '',
  };
}

// ── Schedule modal (create + edit) ──────────────────────────────────────
// `existingId`     — if present, opens in edit mode for that schedule.
// `preselectJobId` — only used in create mode (existingId null). When set,
//                    the Job dropdown opens already pointing at that job and
//                    the Name field is prefilled — used by the "Schedule it"
//                    button on the unscheduled-tasks panel so the user goes
//                    straight from "I packaged a task" to "configure the
//                    trigger" with no hunting in the dropdown.
async function ta_openScheduleModal(existingId, preselectJobId) {
  TA.editingScheduleId = existingId || null;
  document.getElementById('ta-sched-modal-title').textContent = existingId ? 'Edit schedule' : 'New schedule';

  // Render the dynamic trigger pickers once per modal open. Idempotent —
  // both helpers no-op after first call thanks to a data-rendered flag.
  ta_renderWeeklyDays();
  ta_renderMonthlyDom();

  const jobsSel = document.getElementById('ta-sched-job');
  jobsSel.innerHTML = ta_jobOptions();
  // The same set the picker just rendered — the preselect and default-pick
  // logic below has to agree with what is actually on screen, so it must not
  // read cygenix_jobs directly (that would still include trashed jobs).
  const jobs = ta_pickableJobs();

  let schedResp = { schedules: [] };
  try { schedResp = await ta_sched('list-schedules', {}); } catch {}
  TA.schedules = schedResp.schedules || [];

  const chainSel = document.getElementById('ta-sched-chain');
  chainSel.innerHTML = '<option value="">— pick a parent schedule —</option>' +
    TA.schedules.filter(s => s.id !== existingId).map(s =>
      '<option value="' + escapeAttr(s.id) + '">' + escapeHtml(s.name) + '</option>'
    ).join('');

  const modeSel = document.getElementById('ta-trigger-mode');

  if (existingId) {
    const s = TA.schedules.find(x => x.id === existingId);
    if (s) {
      document.getElementById('ta-sched-name').value    = s.name || '';
      jobsSel.value = s.jobId || '';
      document.getElementById('ta-sched-enabled').checked = !!s.enabled;
      if (s.chainAfter) {
        modeSel.value = 'chain';
        chainSel.value = s.chainAfter;
      } else {
        // Always populate the cron block in case parsing falls back to it.
        document.getElementById('ta-sched-cron').value = s.cron || '';
        document.getElementById('ta-sched-tz').value   = s.timezone || 'Europe/London';
        // Try to project the cron into a friendlier mode. oneShot is the
        // authoritative signal for "once" mode — a cron that looks like
        // "31 13 18 5 *" is a Once IF it was created as one, otherwise
        // it's an annual recurring (which the parser can't represent and
        // would fall back to 'cron' for). The flag disambiguates.
        let mode = ta_parseCronIntoUI(s.cron || '', s);
        if (s.oneShot && mode === 'cron') mode = 'once';
        if (s.oneShot === false && mode === 'once') mode = 'cron'; // ambiguous → safe path
        modeSel.value = mode;
        // Copy the timezone into whichever mode block we landed on so the
        // dropdown shows the right value on open.
        const tzId = 'ta-' + mode + '-tz';
        const tzEl = document.getElementById(tzId);
        if (tzEl) tzEl.value = s.timezone || 'Europe/London';
      }
      ta_toggleTriggerMode();
      await ta_onJobPicked(s.jobVersionId);
    }
  } else {
    // Create mode. Default to "daily" (most common) with no preselected
    // job unless the caller passed one in.
    document.getElementById('ta-sched-name').value = '';
    document.getElementById('ta-sched-cron').value = '';
    document.getElementById('ta-sched-enabled').checked = true;
    modeSel.value = 'daily';
    ta_toggleTriggerMode();
    if (preselectJobId && jobs.some(j => j.id === preselectJobId)) {
      jobsSel.value = preselectJobId;
      const preJob = jobs.find(j => j.id === preselectJobId);
      if (preJob && preJob.name) {
        document.getElementById('ta-sched-name').value = preJob.name;
      }
      await ta_onJobPicked();
    } else if (jobs.length) {
      await ta_onJobPicked();
    }
  }
  ta_updateCronPreview();
  document.getElementById('ta-schedule-modal').classList.add('open');
}

function ta_closeScheduleModal() {
  document.getElementById('ta-schedule-modal').classList.remove('open');
  TA.editingScheduleId = null;
}

// ── Trigger mode helpers ────────────────────────────────────────────────
// The Trigger row exposes six modes via a single dropdown. Five of them
// (once / daily / weekly / monthly / cron) all ultimately produce a cron
// expression — the backend only knows cron. The "chain" mode is different:
// no cron, just a parent schedule id. Two helpers do the heavy lifting:
//   ta_recomputeCronFromUI(): friendly inputs → cron string + preview text
//   ta_parseCronIntoUI():    cron string → friendly inputs (best effort,
//                            falls back to 'cron' mode if too complex)
// The Save button always reads the final cron from the SAME field
// (ta-sched-cron) regardless of which mode the user picked — that keeps
// ta_saveSchedule simple and means the preview line is always honest.

const TA_TRIGGER_BLOCKS = ['once', 'daily', 'weekly', 'monthly', 'cron', 'chain'];

function ta_currentTriggerMode() {
  const sel = document.getElementById('ta-trigger-mode');
  return sel ? sel.value : 'daily';
}

function ta_toggleTriggerMode() {
  const mode = ta_currentTriggerMode();
  TA_TRIGGER_BLOCKS.forEach(b => {
    const el = document.getElementById('ta-' + b + '-block');
    if (el) el.style.display = (b === mode) ? '' : 'none';
  });
  // Preview hides for "chain" — there's no cron to show.
  const prev = document.getElementById('ta-cron-preview');
  if (prev) prev.style.display = (mode === 'chain') ? 'none' : '';
  // Cron in this mode isn't user-typed — recompute it now so the hidden
  // ta-sched-cron field always has a valid value when Save is clicked.
  if (mode !== 'cron' && mode !== 'chain') ta_recomputeCronFromUI();
  if (mode === 'cron') ta_updateCronPreview();
}

// "Once" works as a cron expression that matches a single specific date.
// Cron stores it as `M H D MONTH *` (day-of-week wildcard) so the same
// minute / hour / day-of-month / month combo fires once. The schedule is
// marked oneShot:true so scheduled-runner.js disables it after firing.
function ta_buildCronOnce(dateStr, timeStr) {
  if (!dateStr || !timeStr) return null;
  const [y, m, d] = dateStr.split('-').map(Number);
  const [hh, mi]  = timeStr.split(':').map(Number);
  if (!y || !m || !d || isNaN(hh) || isNaN(mi)) return null;
  // Field order: minute hour day-of-month month day-of-week
  return mi + ' ' + hh + ' ' + d + ' ' + m + ' *';
}
function ta_buildCronDaily(timeStr) {
  if (!timeStr) return null;
  const [hh, mi] = timeStr.split(':').map(Number);
  if (isNaN(hh) || isNaN(mi)) return null;
  return mi + ' ' + hh + ' * * *';
}
function ta_buildCronWeekly(daysSet, timeStr) {
  if (!timeStr) return null;
  if (!daysSet || !daysSet.size) return null;
  const [hh, mi] = timeStr.split(':').map(Number);
  if (isNaN(hh) || isNaN(mi)) return null;
  // Cron day-of-week: 0=Sun .. 6=Sat (the scheduled-runner parser matches
  // JS Date.getDay() output exactly, so 0..6 is the right encoding).
  const sorted = Array.from(daysSet).sort((a, b) => a - b);
  return mi + ' ' + hh + ' * * ' + sorted.join(',');
}
function ta_buildCronMonthly(dom, timeStr) {
  if (!dom || !timeStr) return null;
  const [hh, mi] = timeStr.split(':').map(Number);
  if (isNaN(hh) || isNaN(mi)) return null;
  return mi + ' ' + hh + ' ' + dom + ' * *';
}

// Recompose ta-sched-cron and the preview from whichever friendly block
// is active. Called on every input change in the friendly modes.
function ta_recomputeCronFromUI() {
  const mode = ta_currentTriggerMode();
  let cron = null, tz = 'Europe/London';
  if (mode === 'once') {
    cron = ta_buildCronOnce(
      document.getElementById('ta-once-date').value,
      document.getElementById('ta-once-time').value
    );
    tz = document.getElementById('ta-once-tz').value || tz;
  } else if (mode === 'daily') {
    cron = ta_buildCronDaily(document.getElementById('ta-daily-time').value);
    tz   = document.getElementById('ta-daily-tz').value || tz;
  } else if (mode === 'weekly') {
    const days = new Set();
    document.querySelectorAll('#ta-weekly-days input[type=checkbox]:checked')
      .forEach(c => days.add(parseInt(c.value, 10)));
    cron = ta_buildCronWeekly(days, document.getElementById('ta-weekly-time').value);
    tz   = document.getElementById('ta-weekly-tz').value || tz;
  } else if (mode === 'monthly') {
    cron = ta_buildCronMonthly(
      document.getElementById('ta-monthly-dom').value,
      document.getElementById('ta-monthly-time').value
    );
    tz   = document.getElementById('ta-monthly-tz').value || tz;
  }
  // Mirror onto the hidden cron field so ta_saveSchedule reads one place.
  document.getElementById('ta-sched-cron').value = cron || '';
  document.getElementById('ta-sched-tz').value   = tz;
  ta_updateCronPreview();
}

function ta_setCron(expr) {
  document.getElementById('ta-sched-cron').value = expr;
  // If the user clicked a preset chip, leave them in cron mode — the chip
  // is part of the cron block. No mode switch needed.
  ta_updateCronPreview();
}

function ta_updateCronPreview() {
  const expr = document.getElementById('ta-sched-cron').value.trim();
  const preview = document.getElementById('ta-cron-preview');
  if (!preview) return;
  if (!expr) { preview.textContent = ''; return; }
  const parts = expr.split(/\s+/);
  if (parts.length !== 5) { preview.textContent = 'cron must have 5 fields'; preview.style.color='var(--amber)'; return; }
  preview.style.color = 'var(--text3)';
  const [mi, h, dom, mo, dow] = parts;
  const mode = ta_currentTriggerMode();
  // Mode-aware preview — much friendlier than the generic cron text for
  // users who haven't seen cron before.
  if (mode === 'once') {
    if (/^\d+$/.test(dom) && /^\d+$/.test(mo)) {
      const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
      preview.textContent = '→ once on ' + parseInt(dom,10) + ' ' + months[parseInt(mo,10)-1] +
                            ' at ' + h.padStart(2,'0') + ':' + mi.padStart(2,'0');
      return;
    }
  }
  if (mode === 'daily' && /^\d+$/.test(mi) && /^\d+$/.test(h)) {
    preview.textContent = '→ every day at ' + h.padStart(2,'0') + ':' + mi.padStart(2,'0');
    return;
  }
  if (mode === 'weekly' && /^\d+$/.test(mi) && /^\d+$/.test(h)) {
    const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
    const dows = dow.split(',').map(s => parseInt(s,10)).filter(n => n >= 0 && n <= 6);
    if (dows.length) {
      preview.textContent = '→ every ' + dows.map(n => dayNames[n]).join(', ') +
                            ' at ' + h.padStart(2,'0') + ':' + mi.padStart(2,'0');
      return;
    }
  }
  if (mode === 'monthly' && /^\d+$/.test(dom) && /^\d+$/.test(mi) && /^\d+$/.test(h)) {
    const suffix = (n) => (n === 1 || n === 21 || n === 31) ? 'st'
                         : (n === 2 || n === 22) ? 'nd'
                         : (n === 3 || n === 23) ? 'rd' : 'th';
    const d = parseInt(dom,10);
    preview.textContent = '→ every month on the ' + d + suffix(d) +
                          ' at ' + h.padStart(2,'0') + ':' + mi.padStart(2,'0');
    return;
  }
  // Fall back to the original generic preview text.
  if (mi==='*' && h==='*') preview.textContent = '→ every minute';
  else if (h==='*' && dom==='*' && mo==='*' && dow==='*' && /^\d+$/.test(mi)) preview.textContent = '→ every hour at :' + mi.padStart(2,'0');
  else if (dom==='*' && mo==='*' && dow==='*' && /^\d+$/.test(mi) && /^\d+$/.test(h)) preview.textContent = '→ daily at ' + h.padStart(2,'0') + ':' + mi.padStart(2,'0');
  else preview.textContent = '→ ' + expr;
}

// Render the weekly day-pickers (Mon..Sun chips). Called once when the
// modal opens — the checkboxes are stable across opens so we don't have
// to recreate them, but it's cheap and keeps the markup out of HTML.
function ta_renderWeeklyDays() {
  const host = document.getElementById('ta-weekly-days');
  if (!host || host.dataset.rendered) return;
  host.dataset.rendered = '1';
  // Order shown left-to-right: Mon..Sun (matches what most users expect),
  // but the value attribute is the cron day-of-week index (Sun=0..Sat=6).
  const days = [
    { label: 'Mon', val: 1 }, { label: 'Tue', val: 2 }, { label: 'Wed', val: 3 },
    { label: 'Thu', val: 4 }, { label: 'Fri', val: 5 }, { label: 'Sat', val: 6 },
    { label: 'Sun', val: 0 },
  ];
  host.innerHTML = days.map(d => `
    <label style="display:inline-flex;align-items:center;gap:0.3rem;padding:0.3rem 0.55rem;border:0.5px solid var(--border);border-radius:var(--r);background:var(--bg2);font-size:11px;cursor:pointer">
      <input type="checkbox" value="${d.val}" onchange="ta_recomputeCronFromUI()" style="margin:0">
      ${d.label}
    </label>
  `).join('');
}
function ta_weeklyPreset(which) {
  const wanted = (
    which === 'weekdays' ? [1,2,3,4,5] :
    which === 'weekends' ? [0,6]       :
    which === 'all'      ? [0,1,2,3,4,5,6] : []
  );
  document.querySelectorAll('#ta-weekly-days input[type=checkbox]').forEach(c => {
    c.checked = wanted.includes(parseInt(c.value, 10));
  });
  ta_recomputeCronFromUI();
}

// Render the monthly day-of-month select. 1..31. Same pattern.
function ta_renderMonthlyDom() {
  const sel = document.getElementById('ta-monthly-dom');
  if (!sel || sel.dataset.rendered) return;
  sel.dataset.rendered = '1';
  const opts = [];
  for (let d = 1; d <= 31; d++) {
    const suf = (d === 1 || d === 21 || d === 31) ? 'st'
              : (d === 2 || d === 22) ? 'nd'
              : (d === 3 || d === 23) ? 'rd' : 'th';
    opts.push('<option value="' + d + '">' + d + suf + '</option>');
  }
  sel.innerHTML = opts.join('');
}

// Best-effort parse an existing cron string back into the friendly UI.
// Returns the mode it resolved to (or 'cron' if we can't simplify).
// ── One-shot schedules and the missing year ───────────────────────────────
// A cron expression has five fields and none of them is a year, so
// "25 14 15 8 *" means 14:25 on 15 August in EVERY year. That is right for a
// repeating schedule and wrong for a one-shot, which is a single instant.
//
// The year is therefore stored separately, as runOnceAt, and these two
// helpers are the only places that write and read it.

// The instant the one-shot fields describe, as an ISO string.
//
// The date and time are a wall-clock reading in the timezone chosen beside
// them, so they have to be converted through that zone. Treating them as UTC
// put every one-shot an hour late through British Summer Time: 14:50
// Europe/London is 13:50Z, not 14:50Z.
//
// Same algorithm as zonedTimeToUtc in netlify/functions/lib/cron.js, which is
// the authority for the repeating case. Duplicated rather than shared because
// one runs in the browser and the other in a Netlify function; keep them in
// step if either changes.
function ta_zoneOffsetMs(date, tz){
  const f = new Intl.DateTimeFormat('en-US', {
    timeZone: tz, hour12: false,
    year:'numeric', month:'2-digit', day:'2-digit',
    hour:'2-digit', minute:'2-digit', second:'2-digit',
  });
  const p = {};
  for (const part of f.formatToParts(date)) p[part.type] = part.value;
  return Date.UTC(+p.year, +p.month - 1, +p.day, (+p.hour) % 24, +p.minute, +p.second) - date.getTime();
}

function ta_onceIso(){
  const d = (document.getElementById('ta-once-date') || {}).value || '';
  const t = (document.getElementById('ta-once-time') || {}).value || '';
  if (!d || !t) return null;
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(d);
  const n = /^(\d{2}):(\d{2})/.exec(t);
  if (!m || !n) return null;

  let tz = (document.getElementById('ta-once-tz') || {}).value || 'UTC';
  try { new Intl.DateTimeFormat('en-US', { timeZone: tz }); }
  catch { tz = 'UTC'; }   // unknown zone → don't throw, just don't shift

  const guess = Date.UTC(+m[1], +m[2] - 1, +m[3], +n[1], +n[2], 0);
  // Sample the offset at the answer rather than the guess, or a time within an
  // hour of a DST boundary lands an hour out.
  const off1 = ta_zoneOffsetMs(new Date(guess), tz);
  let ms = guess - off1;
  const off2 = ta_zoneOffsetMs(new Date(ms), tz);
  if (off2 !== off1) ms = guess - off2;
  return new Date(ms).toISOString();
}

// The year to show for a one-shot, in order of trustworthiness:
//
//   1. runOnceAt        — what the user actually saved. Authoritative.
//   2. nextRunAt        — schedules saved before runOnceAt existed still have
//                         the instant the backend computed at save time.
//   3. the old guess    — nothing stored, so fall back to "this year, or next
//                         if that date has already passed". Only reachable for
//                         a legacy schedule whose nextRunAt has been cleared,
//                         which is exactly the case that used to be wrong; it
//                         is now the last resort rather than the only rule.
function ta_onceYearFor(schedule, mo, dom, h, mi){
  const stored = schedule && (schedule.runOnceAt || schedule.nextRunAt);
  if (stored){
    const d = new Date(stored);
    if (!isNaN(d.getTime())){
      // Read the year in the schedule's own zone. runOnceAt is an instant, and
      // near midnight the UTC date can be the day — and so the year — before
      // the local one: 1 Jan 00:30 Europe/London is 31 Dec 23:30Z.
      const tz = (schedule && schedule.timezone) || 'UTC';
      try {
        return +new Intl.DateTimeFormat('en-US', { timeZone: tz, year: 'numeric' }).format(d);
      } catch { return d.getUTCFullYear(); }
    }
  }
  const now = new Date();
  const candidate = new Date(now.getFullYear(), parseInt(mo,10) - 1, parseInt(dom,10),
                             parseInt(h,10), parseInt(mi,10));
  return candidate >= now ? now.getFullYear() : now.getFullYear() + 1;
}

function ta_parseCronIntoUI(cronExpr, schedule) {
  if (!cronExpr || typeof cronExpr !== 'string') return 'daily';
  const parts = cronExpr.trim().split(/\s+/);
  if (parts.length !== 5) return 'cron';
  const [mi, h, dom, mo, dow] = parts;

  const pad = (s) => String(s).padStart(2, '0');
  const setTime = (mode, mi, h) => {
    const t = pad(h) + ':' + pad(mi);
    const el = document.getElementById('ta-' + mode + '-time');
    if (el) el.value = t;
  };

  // ONCE: specific minute/hour/dom/month, dow wildcard
  if (/^\d+$/.test(mi) && /^\d+$/.test(h) && /^\d+$/.test(dom) && /^\d+$/.test(mo) && dow === '*') {
    setTime('once', mi, h);
    // The year comes from the schedule's stored runOnceAt, never from the
    // cron — a cron has no year field. This used to guess "the next year in
    // which this date has not passed", so re-opening a one-shot after its
    // moment silently showed the year after the one the user saved. Saving
    // from that screen then wrote a nextRunAt more than 60 days out, which
    // computeNextRun returns null for, and the schedule could never fire.
    const y = ta_onceYearFor(schedule, mo, dom, h, mi);
    document.getElementById('ta-once-date').value =
      y + '-' + pad(mo) + '-' + pad(dom);
    return 'once';
  }

  // DAILY: minute/hour set, everything else wildcard
  if (/^\d+$/.test(mi) && /^\d+$/.test(h) && dom === '*' && mo === '*' && dow === '*') {
    setTime('daily', mi, h);
    return 'daily';
  }

  // WEEKLY: minute/hour set, dom and month wildcard, dow specific (list)
  if (/^\d+$/.test(mi) && /^\d+$/.test(h) && dom === '*' && mo === '*' && dow !== '*') {
    // Accept "1-5" as 1,2,3,4,5
    let dows = [];
    if (/^\d+(,\d+)*$/.test(dow)) {
      dows = dow.split(',').map(n => parseInt(n,10));
    } else if (/^\d+-\d+$/.test(dow)) {
      const [a,b] = dow.split('-').map(n => parseInt(n,10));
      for (let i = a; i <= b; i++) dows.push(i);
    }
    if (dows.length) {
      setTime('weekly', mi, h);
      document.querySelectorAll('#ta-weekly-days input[type=checkbox]').forEach(c => {
        c.checked = dows.includes(parseInt(c.value, 10));
      });
      return 'weekly';
    }
  }

  // MONTHLY: minute/hour/dom set, month and dow wildcard
  if (/^\d+$/.test(mi) && /^\d+$/.test(h) && /^\d+$/.test(dom) && mo === '*' && dow === '*') {
    setTime('monthly', mi, h);
    document.getElementById('ta-monthly-dom').value = String(parseInt(dom, 10));
    return 'monthly';
  }

  // Anything else — drop to advanced cron.
  return 'cron';
}

async function ta_onJobPicked(preselectVersionId) {
  const jobId = document.getElementById('ta-sched-job').value;
  const verSel = document.getElementById('ta-sched-version');
  if (!jobId) { verSel.innerHTML = '<option value="">—</option>'; return; }
  verSel.innerHTML = '<option value="">loading…</option>';
  try {
    const data = await ta_data('version-list?jobId=' + encodeURIComponent(jobId), null, 'GET');
    const vers = data.versions || [];
    TA.versionsByJob[jobId] = vers;
    if (!vers.length) {
      verSel.innerHTML = '<option value="">No snapshots — use the button below</option>';
      return;
    }
    verSel.innerHTML = vers.map(v =>
      '<option value="' + escapeAttr(v.id) + '">v' + v.version + ' · ' + ta_fmtDateTime(v.createdAt) + (v.label && v.label !== 'auto' ? ' · ' + escapeHtml(v.label) : '') + '</option>'
    ).join('');
    if (preselectVersionId) verSel.value = preselectVersionId;
  } catch (e) {
    verSel.innerHTML = '<option value="">error: ' + escapeHtml(e.message) + '</option>';
  }
}

async function ta_createVersionFromSelected() {
  const jobId = document.getElementById('ta-sched-job').value;
  if (!jobId) return alert('Pick a job first.');
  const jobs = safeArr('cygenix_jobs');
  const job = jobs.find(j => j.id === jobId);
  if (!job) return alert('Job not on this browser.');

  // Confirm with a clear summary so accidental clicks don't surprise the
  // user, but DON'T use prompt() — pressing Escape on a prompt returns null
  // and silently aborts the whole function, which earlier looked exactly
  // like the snapshot button being broken. confirm() is binary: OK or
  // Cancel, no hidden third state.
  const childStepCount = Array.isArray(job.childSteps) ? job.childSteps.length : 0;
  const summary = childStepCount
    ? ('Snapshot "' + job.name + '" (' + childStepCount + ' step' + (childStepCount === 1 ? '' : 's') + ')?')
    : ('Snapshot "' + job.name + '"?');
  if (!confirm(summary + '\n\nA new pinned version will be created in Cosmos with the current SQL and parameter substitutions.')) {
    if (typeof showToast === 'function') showToast('Snapshot cancelled');
    return;
  }
  const label = 'manual';

  // ── Phase 2a — snapshot parameter substitution ──────────────────────────
  // Scheduled runs execute server-side with no browser context. CygenixParams
  // tokens (e.g. {client_id}, {conv_number}) get resolved here at snapshot
  // time so the scheduled run sees pre-substituted SQL. The substitution
  // also gets applied to childSteps[*].sql and childSteps[*].insertSQL so
  // composite tasks behave the same.
  //
  // Was/Is rules don't need a separate step — they're already baked into
  // each step's generated SQL via CASE WHEN blocks at SQL-generation time
  // (project-builder.html line ~4894). The structured rules also travel
  // along on each columnMapping[].wasisRules for traceability/reporting.
  //
  // Reconciliation, one-to-many cursor SQL, and excluded-row capture are
  // NOT yet supported in scheduled runs — phase 2b will handle these
  // explicitly with clear "not supported yet" reasons in the run record.
  const snapshot = JSON.parse(JSON.stringify(job)); // deep clone — don't mutate localStorage job
  const substitutedTokens = [];
  const trySub = (s) => {
    if (!s || typeof s !== 'string') return s;
    if (!window.CygenixParams || typeof window.CygenixParams.substituteParams !== 'function') return s;
    const before = s;
    const after = window.CygenixParams.substituteParams(s);
    if (after !== before && typeof window.CygenixParams.findTokens === 'function') {
      try {
        const tokens = window.CygenixParams.findTokens(before) || [];
        tokens.forEach(t => {
          const resolved = window.CygenixParams.substituteParams(t);
          if (resolved !== t && !substitutedTokens.find(x => x.token === t)) {
            substitutedTokens.push({ token: t, resolved });
          }
        });
      } catch {}
    }
    return after;
  };
  // Top-level SQL fields (raw-SQL jobs).
  if (snapshot.sql)       snapshot.sql       = trySub(snapshot.sql);
  if (snapshot.insertSQL) snapshot.insertSQL = trySub(snapshot.insertSQL);
  // Composite-job children.
  if (Array.isArray(snapshot.childSteps)) {
    snapshot.childSteps.forEach(step => {
      if (step.sql)       step.sql       = trySub(step.sql);
      if (step.insertSQL) step.insertSQL = trySub(step.insertSQL);
      if (step.srcWhere)  step.srcWhere  = trySub(step.srcWhere);
      // Column literals. A migration step is NOT executed from insertSQL —
      // the runner rebuilds each INSERT from columnMapping, reading
      // literalValue/fixedValue per row. Substituting only the SQL fields
      // left those literals raw, so a mapping with "@@date" wrote the text
      // "@@date" into the target while the generated SQL on screen showed
      // the resolved value. The browser runner already expands them
      // per row (see applyColumnMappingValue); this is the same resolution
      // for the snapshot a scheduled run executes.
      if (Array.isArray(step.columnMapping)) {
        step.columnMapping.forEach(m => {
          if (!m) return;
          if (m.literalValue) m.literalValue = trySub(m.literalValue);
          if (m.fixedValue)   m.fixedValue   = trySub(m.fixedValue);
        });
      }
    });
  }
  // Single-map jobs carry the mapping at the top level.
  if (Array.isArray(snapshot.columnMapping)) {
    snapshot.columnMapping.forEach(m => {
      if (!m) return;
      if (m.literalValue) m.literalValue = trySub(m.literalValue);
      if (m.fixedValue)   m.fixedValue   = trySub(m.fixedValue);
    });
  }
  if (snapshot.srcWhere) snapshot.srcWhere = trySub(snapshot.srcWhere);
  // Stamp the snapshot with substitution metadata so phase 2b's runner —
  // and any future audit — can see what was resolved and when.
  snapshot._capturedAt          = new Date().toISOString();
  snapshot._substitutedTokens   = substitutedTokens;
  snapshot._captureClientNote   = 'Parameter values were resolved from the browser at version-create time. Was/Is rules are pre-baked into step SQL.';

  try {
    const r = await ta_data('version-create', {
      jobId,
      snapshot,
      label: label || 'manual',
      note:  substitutedTokens.length ? (substitutedTokens.length + ' parameter(s) resolved at snapshot') : ''
    });
    if (r.created) {
      const tokenSuffix = substitutedTokens.length ? ' (' + substitutedTokens.length + ' param' + (substitutedTokens.length === 1 ? '' : 's') + ' resolved)' : '';
      if (typeof showToast === 'function') showToast('Snapshot v' + r.version + ' created' + tokenSuffix);
    } else {
      if (typeof showToast === 'function') showToast('No change — matches existing v' + r.version);
    }
    await ta_onJobPicked(r.id || undefined);
  } catch (e) { alert('Snapshot failed: ' + e.message); }
}

async function ta_saveSchedule() {
  const name = document.getElementById('ta-sched-name').value.trim();
  const jobId = document.getElementById('ta-sched-job').value;
  const jobVersionId = document.getElementById('ta-sched-version').value;
  const enabled = document.getElementById('ta-sched-enabled').checked;
  const mode = ta_currentTriggerMode();

  // Mode-specific extraction. All cron-based modes have already written
  // ta-sched-cron via ta_recomputeCronFromUI on every change, so we just
  // read from there. Timezone is also mirrored onto ta-sched-tz by the
  // recompute helper. Only chain mode breaks the pattern.
  let cron       = null;
  let timezone   = null;
  let chainAfter = null;
  let oneShot    = false;
  // A cron expression has no year field. For a repeating schedule that is
  // exactly right, but a one-shot is a single instant and the year matters —
  // without it, "15 Aug 14:25" is ambiguous the moment that date passes, and
  // both the editor and the backend have to guess. runOnceAt records the
  // instant so nothing has to.
  let runOnceAt  = null;

  if (mode === 'chain') {
    chainAfter = document.getElementById('ta-sched-chain').value;
  } else {
    cron     = document.getElementById('ta-sched-cron').value.trim();
    timezone = document.getElementById('ta-sched-tz').value;
    if (mode === 'once') {
      oneShot   = true;
      runOnceAt = ta_onceIso();
    }
  }

  if (!name) return alert('Name is required');
  if (!jobId) return alert('Pick a job');
  if (!jobVersionId) return alert('Pick a pinned version (or create one)');
  if (mode === 'chain' && !chainAfter) return alert('Pick a parent schedule');
  if (mode !== 'chain' && !cron) {
    // Most likely cause: the user picked Weekly with no days ticked, or
    // entered a malformed cron in Custom. Either way the friendly inputs
    // didn't compose into a valid cron.
    return alert(
      mode === 'weekly' ? 'Pick at least one day of the week.' :
      mode === 'once'   ? 'Pick a date and time.'             :
      'Enter a cron expression.'
    );
  }
  if (mode === 'once') {
    // Sanity check: don't let users schedule something in the past — the
    // cron parser would just never match it and the schedule would sit
    // idle forever. The check uses the user's local clock; close enough
    // to the user's chosen timezone for a soft warning.
    // Checked against the same instant that gets stored, so the warning and
    // the saved nextRunAt can never disagree about whether it is in the past.
    const iso = ta_onceIso();
    if (iso) {
      const target = new Date(iso);
      if (!isNaN(target.getTime()) && target.getTime() < Date.now() - 60_000) {
        if (!confirm('The date/time you picked is in the past. Save anyway? The schedule won\'t fire until you edit it to a future time.')) return;
      }
    }
  }

  // Capture connection strings so the server can execute the schedule
  // without a browser open. impGetConn returns either an mssql:// URL or an
  // https:// Azure Function URL — both are valid to store.
  //
  // Profile-build jobs are an exception: their connection lives inside the
  // pinned snapshot, not on the schedule row, so we skip the tgtConn
  // requirement when the picked job is type 'profile-build'.
  const srcConn = (typeof impGetConn === 'function') ? impGetConn('src') : '';
  const tgtConn = (typeof impGetConn === 'function') ? impGetConn('tgt') : '';
  let isProfileBuild = false;
  try {
    const jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]');
    const job  = (Array.isArray(jobs) ? jobs : []).find(j => j && j.id === jobId);
    isProfileBuild = !!(job && job.jobType === 'profile-build');
  } catch {}
  if (!isProfileBuild && !tgtConn) {
    return alert('No target connection configured. Open Connections → Target and save a connection before scheduling.');
  }

  const btn = document.getElementById('ta-sched-save-btn');
  btn.disabled = true; const orig = btn.innerHTML; btn.innerHTML = 'Saving…';
  try {
    if (TA.editingScheduleId) {
      await ta_sched('update-schedule', {
        id: TA.editingScheduleId,
        patch: { name, jobVersionId, cron, timezone, chainAfter, oneShot, runOnceAt, enabled, srcConn, tgtConn },
      });
    } else {
      await ta_sched('create-schedule', {
        name, jobId, jobVersionId, cron, timezone, chainAfter, oneShot, runOnceAt, enabled, srcConn, tgtConn,
      });
    }
    ta_closeScheduleModal();
    ta_loadSchedules();
    if (typeof showToast === 'function') showToast('Schedule saved');
  } catch (e) {
    alert('Save failed: ' + e.message);
  } finally {
    btn.disabled = false; btn.innerHTML = orig;
  }
}

// ── Run history ─────────────────────────────────────────────────────────
async function ta_loadRuns() {
  const el = document.getElementById('ta-runs-list');
  el.innerHTML = '<div class="empty-state" style="padding:2rem"><p style="color:var(--text3)">Loading…</p></div>';

  try {
    const s = await ta_sched('list-schedules', {});
    TA.schedules = s.schedules || [];
  } catch {}
  const filt = document.getElementById('ta-run-filter');
  const prev = filt.value;
  filt.innerHTML = '<option value="">All schedules</option>' +
    TA.schedules.map(s => '<option value="' + escapeAttr(s.id) + '">' + escapeHtml(s.name) + '</option>').join('');
  if (prev) filt.value = prev;

  try {
    const scheduleId = filt.value || null;
    const data = await ta_sched('list-runs', { scheduleId, limit: 50 });
    TA.runs = data.runs || [];
    ta_renderRuns();
  } catch (e) {
    el.innerHTML = '<div class="empty-state"><h3>Could not load runs</h3><p style="color:var(--red)">' + escapeHtml(e.message) + '</p></div>';
  }
}

function ta_renderRuns() {
  const el = document.getElementById('ta-runs-list');
  if (!TA.runs.length) {
    el.innerHTML = '<div class="empty-state" style="padding:2.5rem 2rem"><h3>No runs yet</h3><p>Run history appears after the first scheduled or manual execution.</p></div>';
    return;
  }
  const nameOf = id => (TA.schedules.find(s => s.id === id) || {}).name || '(deleted)';
  const rows = TA.runs.map(r => {
    const duration = r.startedAt && r.finishedAt
      ? Math.round((new Date(r.finishedAt) - new Date(r.startedAt)) / 1000) + 's'
      : '—';
    const trigChip = r.triggeredBy === 'manual'
      ? '<span class="badge badge-teal" style="font-size:10px">manual</span>'
      : r.triggeredBy === 'chain'
      ? '<span class="badge badge-purple" style="font-size:10px">chain</span>'
      : '<span class="badge badge-blue" style="font-size:10px">cron</span>';
    return `<tr onclick="ta_openRunDetail('${r.id}','${r.scheduleId}')" style="cursor:pointer" title="View run detail">
      <td>${ta_runStatusChip(r.status)}</td>
      <td><strong>${escapeHtml(nameOf(r.scheduleId))}</strong></td>
      <td>${trigChip}</td>
      <td style="font-family:var(--mono);font-size:11px;color:var(--text2)">${ta_fmtDateTime(r.startedAt)}</td>
      <td style="font-family:var(--mono);font-size:11px;color:var(--text3)">${duration}</td>
      <td style="font-family:var(--mono);font-size:11px;color:var(--text2)">${r.rowsAffected != null ? r.rowsAffected.toLocaleString() : '—'}</td>
    </tr>`;
  }).join('');
  el.innerHTML = '<table class="jobs-table"><thead><tr>' +
    '<th>Status</th><th>Schedule</th><th>Trigger</th><th>Started</th><th>Duration</th><th>Rows</th>' +
    '</tr></thead><tbody>' + rows + '</tbody></table>';
}

async function ta_openRunDetail(runId, scheduleId) {
  try {
    const data = await ta_sched('get-run', { id: runId, scheduleId });
    const r = data.run;
    document.getElementById('ta-run-title').textContent = 'Run · ' + ta_fmtDateTime(r.startedAt);
    document.getElementById('ta-run-sub').textContent   = 'Status: ' + r.status + ' · trigger: ' + r.triggeredBy;
    const body = document.getElementById('ta-run-body');
    const rows = [
      ['Schedule',    (TA.schedules.find(s => s.id === r.scheduleId) || {}).name || r.scheduleId],
      ['Job',         ta_jobNameFor(r.jobId) + ' (version ' + r.jobVersionId + ')'],
      ['Started',     ta_fmtDateTime(r.startedAt)],
      ['Finished',    r.finishedAt ? ta_fmtDateTime(r.finishedAt) : '—'],
      ['Rows affected', r.rowsAffected != null ? r.rowsAffected.toLocaleString() : '—'],
    ];
    let html = '<table style="width:100%;border-collapse:collapse">' + rows.map(([k,v]) =>
      '<tr><td style="padding:0.4rem 0.6rem;color:var(--text3);width:140px">' + escapeHtml(k) + '</td><td style="padding:0.4rem 0.6rem;color:var(--text)">' + escapeHtml(String(v)) + '</td></tr>'
    ).join('') + '</table>';
    if (r.errorMessage) {
      html += '<div style="margin-top:1rem"><div style="font-size:11px;color:var(--text3);text-transform:uppercase;letter-spacing:0.07em;margin-bottom:0.4rem">Error</div>'
           + '<div class="code-block" style="color:var(--red);max-height:30vh;white-space:pre-wrap">' + escapeHtml(r.errorMessage) + '</div></div>';
    }
    body.innerHTML = html;
    document.getElementById('ta-run-modal').classList.add('open');
  } catch (e) { alert('Could not load run: ' + e.message); }
}

function ta_closeRunModal() {
  document.getElementById('ta-run-modal').classList.remove('open');
}


/* ══════════════════════════════════════════════════════════════════════════
   Settings → Co-Worker model
   ──────────────────────────────────────────────────────────────────────────
   Every decision lives in cygenix-model.js; this only draws the panel and
   calls it. The list of models comes from the operator's own key, live —
   a hardcoded catalogue would be the same mistake as a hardcoded model.
   ═══════════════════════════════════════════════════════════════════════ */
let _cygmmAvailable = null;      // ids from the live list, or null if unread
let _cygmmDraft = null;          // { primary, fallbacks } being edited
let _cygmmCheckedAt = null;

function cygmmModel() { return window.CygenixModel; }
function cygmmKey() {
  return localStorage.getItem('cygenix_api_key') || sessionStorage.getItem('cygenix_api_key') || '';
}
function cygmmDirty() {
  const s = cygmmModel().mdSettings();
  return !!_cygmmDraft && (_cygmmDraft.primary !== s.primary
    || _cygmmDraft.fallbacks.join(',') !== s.fallbacks.join(','));
}
function cygmmSetStatus(tone, title, detail, warnings) {
  const colour = { ok: 'var(--green)', warn: 'var(--amber)', bad: 'var(--red)', neutral: 'var(--text3)' }[tone];
  const bg = { ok: 'var(--green-bg)', warn: 'var(--amber-bg)', bad: 'var(--red-bg)', neutral: 'var(--bg3)' }[tone];
  const el = document.getElementById('cygmm-status');
  if (!el) return;
  el.style.background = bg;
  el.style.borderColor = tone === 'neutral' ? 'var(--border2)' : colour;
  el.innerHTML = '<i class="ic-dot" style="color:' + colour + ';margin-top:5px"></i><div><b>' + escP(title) + '</b>'
    + '<div style="color:var(--text2)">' + detail + '</div>'
    + ((warnings && warnings.length)
      ? '<ul style="margin:5px 0 0;padding-left:16px">' + warnings.map(w => '<li>' + escP(w) + '</li>').join('') + '</ul>'
      : '') + '</div>';
}
function cygmmMessage(tone, html) {
  const el = document.getElementById('cygmm-msg');
  if (!el) return;
  if (!tone) { el.style.display = 'none'; el.innerHTML = ''; return; }
  const colour = { ok: 'var(--green)', warn: 'var(--amber)', bad: 'var(--red)', neutral: 'var(--text3)' }[tone];
  const bg = { ok: 'var(--green-bg)', warn: 'var(--amber-bg)', bad: 'var(--red-bg)', neutral: 'var(--bg3)' }[tone];
  el.style.display = '';
  el.style.background = bg;
  el.style.border = '0.5px solid ' + colour;
  el.innerHTML = html;
}

function cygmmRender() {
  const M = cygmmModel();
  if (!M || !document.getElementById('cygmm')) return;
  if (!_cygmmDraft) {
    const s = M.mdSettings();
    _cygmmDraft = { primary: s.primary, fallbacks: s.fallbacks.slice() };
  }
  const live = (id) => !_cygmmAvailable || _cygmmAvailable.indexOf(id) > -1;
  const known = (_cygmmAvailable || M.KNOWN.map(m => m.id));
  const labelOf = (id) => (M.KNOWN.find(k => k.id === id) || {}).label || id;

  /* status, from the engine's own resolution */
  const r = M.mdResolve(_cygmmAvailable);
  if (!cygmmKey()) {
    cygmmSetStatus('bad', 'No API key set',
      'Add an Anthropic API key above — without one nothing can be checked or called.');
  } else if (r.source === 'unverified') {
    cygmmSetStatus('neutral', 'Not verified',
      'Using <code>' + escP(r.model) + '</code> without confirming it exists. Press <b>Check now</b>.', r.warnings);
  } else if (r.source === 'primary') {
    cygmmSetStatus('ok', 'Healthy',
      'Using <code>' + escP(r.model) + '</code>, which is available on this key.', r.warnings);
  } else if (r.source === 'fallback') {
    cygmmSetStatus('warn', 'Running on a fallback',
      'The primary is unavailable, so calls have degraded to <code>' + escP(r.model)
      + '</code>. Cost and behaviour differ — set a current primary below.', r.warnings);
  } else {
    cygmmSetStatus('bad', 'No usable model',
      'None of the configured models are available. Every Claude call will fail until a current model is picked below.', r.warnings);
  }

  /* primary */
  const sel = document.getElementById('cygmm-primary');
  const opts = known.filter(id => _cygmmDraft.fallbacks.indexOf(id) === -1)
    .map(id => '<option value="' + escP(id) + '"' + (id === _cygmmDraft.primary ? ' selected' : '') + '>'
      + escP(labelOf(id)) + ' — ' + escP(id) + '</option>');
  // a configured-but-retired model stays visible rather than vanishing
  if (_cygmmDraft.primary && known.indexOf(_cygmmDraft.primary) === -1) {
    opts.unshift('<option value="' + escP(_cygmmDraft.primary) + '" selected>'
      + escP(_cygmmDraft.primary) + ' — unavailable</option>');
  }
  sel.innerHTML = opts.join('') || '<option value="">No models available</option>';
  document.getElementById('cygmm-primary-hint').innerHTML = live(_cygmmDraft.primary)
    ? 'Used for every Claude call the console makes.'
    : '<b style="color:var(--red)">Not available — most likely retired. Pick a current one.</b>';

  /* fallback chain */
  const chain = document.getElementById('cygmm-chain');
  chain.innerHTML = _cygmmDraft.fallbacks.length ? _cygmmDraft.fallbacks.map((id, i) =>
    '<li style="display:flex;align-items:center;gap:7px;padding:6px 8px;background:var(--bg3);border:0.5px solid var(--border);border-radius:var(--r);font-size:12px">'
    + '<span style="font-family:var(--mono);font-size:10.5px;color:var(--text3)">' + (i + 1) + '</span>'
    + '<span style="flex:1;font-family:var(--mono);font-size:11.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + escP(id) + '</span>'
    + '<span class="badge" style="font-size:9.5px;padding:1px 6px;border-radius:99px;background:'
      + (live(id) ? 'var(--green-bg);color:var(--green)">available' : 'var(--red-bg);color:var(--red)">unavailable') + '</span>'
    + '<button class="btn btn-sm" style="padding:1px 6px" onclick="cygmmMove(' + i + ',-1)"' + (i === 0 ? ' disabled' : '') + '>↑</button>'
    + '<button class="btn btn-sm" style="padding:1px 6px" onclick="cygmmMove(' + i + ',1)"' + (i === _cygmmDraft.fallbacks.length - 1 ? ' disabled' : '') + '>↓</button>'
    + '<button class="btn btn-sm" style="padding:1px 6px" onclick="cygmmRemove(' + i + ')">✕</button></li>').join('')
    : '<li style="font-size:11.5px;color:var(--text3);font-style:italic;padding:6px 0">No fallbacks — a retirement takes the assistant offline.</li>';

  const used = [_cygmmDraft.primary].concat(_cygmmDraft.fallbacks);
  const addable = known.filter(id => used.indexOf(id) === -1);
  const add = document.getElementById('cygmm-add');
  add.innerHTML = addable.map(id => '<option value="' + escP(id) + '">' + escP(id) + '</option>').join('')
    || '<option value="">Nothing left to add</option>';

  /* facts */
  const s = M.mdSettings();
  document.getElementById('cygmm-serving').textContent = r.model || 'none';
  document.getElementById('cygmm-changed').textContent = s.updatedAt
    ? new Date(s.updatedAt).toLocaleString('en-GB') + (s.updatedBy ? ' · ' + s.updatedBy : '')
    : 'never — using the built-in default';
  document.getElementById('cygmm-checked').textContent = _cygmmCheckedAt
    ? new Date(_cygmmCheckedAt).toLocaleString('en-GB') : 'never';

  document.getElementById('cygmm-save').disabled = !cygmmDirty();
  document.getElementById('cygmm-revert').disabled = !cygmmDirty();
}

function cygmmPick(v) { _cygmmDraft.primary = v;
  _cygmmDraft.fallbacks = _cygmmDraft.fallbacks.filter(m => m !== v); cygmmMessage(null); cygmmRender(); }
function cygmmAddFallback() {
  const v = document.getElementById('cygmm-add').value;
  if (v && _cygmmDraft.fallbacks.indexOf(v) === -1 && v !== _cygmmDraft.primary) {
    _cygmmDraft.fallbacks.push(v); cygmmMessage(null); cygmmRender();
  }
}
function cygmmMove(i, d) {
  const f = _cygmmDraft.fallbacks;
  f.splice(i + d, 0, f.splice(i, 1)[0]); cygmmRender();
}
function cygmmRemove(i) { _cygmmDraft.fallbacks.splice(i, 1); cygmmRender(); }
function cygmmRevert() { _cygmmDraft = null; cygmmMessage(null); cygmmRender(); }

function cygmmSave() {
  const M = cygmmModel();
  const unknown = [_cygmmDraft.primary].concat(_cygmmDraft.fallbacks)
    .filter(m => m && _cygmmAvailable && _cygmmAvailable.indexOf(m) === -1);
  if (unknown.length) {
    cygmmMessage('warn', 'Not available on this key: <span class="mono">' + escP(unknown.join(', '))
      + '</span>. Saving one anyway will fail at call time.'
      + ' <button class="btn btn-sm" style="margin-left:6px" onclick="cygmmSaveForce()">Save anyway</button>');
    return;
  }
  cygmmSaveForce();
}
function cygmmSaveForce() {
  const M = cygmmModel();
  const who = (JSON.parse(localStorage.getItem('cygenix_user') || '{}').email) || null;
  M.mdSaveSettings(_cygmmDraft, who, Date.now());
  _cygmmDraft = null;
  cygmmMessage('ok', 'Saved. Every Claude call from the next request uses <span class="mono">'
    + escP(M.mdPrimary()) + '</span>.');
  cygmmRender();
}

async function cygmmRefresh() {
  const M = cygmmModel();
  const btn = document.getElementById('cygmm-refresh');
  const key = cygmmKey();
  if (!key) { cygmmRender(); return; }
  btn.disabled = true;
  cygmmMessage('neutral', 'Reading the model list from your API key…');
  try {
    const models = await M.mdListModels(key);
    _cygmmAvailable = models.map(m => m.id);
    // the live list is the authority — show anything the key can actually use
    M.KNOWN.length = 0;
    models.forEach(m => M.KNOWN.push({ id: m.id, label: m.label }));
    _cygmmCheckedAt = Date.now();
    cygmmMessage(null);
  } catch (e) {
    const hint = (e && e.mapped && e.mapped.adminHint) || e.message;
    cygmmMessage('bad', escP(hint));
  }
  btn.disabled = false;
  cygmmRender();
}

async function cygmmTest() {
  const M = cygmmModel();
  const model = _cygmmDraft.primary;
  const key = cygmmKey();
  if (!model || !key) { cygmmMessage('bad', 'A model and an API key are both needed to test.'); return; }
  const btn = document.getElementById('cygmm-test');
  btn.disabled = true;
  cygmmMessage('neutral', 'Sending a one-token request to <span class="mono">' + escP(model) + '</span>…');
  try {
    const res = await M.mdTestModel(model, key);
    cygmmMessage(res.ok ? 'ok' : 'bad', res.ok
      ? '<span class="mono">' + escP(model) + '</span> answered in ' + res.ms + ' ms.'
      : '<span class="mono">' + escP(model) + '</span> failed (' + escP(res.code) + '). ' + escP(res.detail || ''));
  } catch (e) {
    cygmmMessage('bad', 'Test failed: ' + escP(e.message));
  }
  btn.disabled = false;
}


/* ══════════════════════════════════════════════════════════════════════════
   Assistant adapters — what the docked panel can do on the dashboard shell
   ──────────────────────────────────────────────────────────────────────────
   The shell's screens are in-page views, so app.navigate switches them here
   without a reload, and the context provider tells the assistant which view
   is actually showing (the page key alone would just say "dashboard").
   ═══════════════════════════════════════════════════════════════════════ */
Object.assign(window.CygenixAssistantAdapters = window.CygenixAssistantAdapters || {}, {
  navigateToView: (v) => showView(v),
});
// The assistant script loads after this file — register once it exists.
(function wireAssistant() {
  if (!window.CygenixAssistant) { document.addEventListener('DOMContentLoaded', wireAssistant, { once: true }); return; }
  CygenixAssistant.registerContext(() => {
    const active = document.querySelector('.view.active');
    return { view: active ? active.id.replace(/^view-/, '') : 'dashboard' };
  });
})();
