// ── Config ────────────────────────────────────────────────────────────────────
const HEALTH_URL = '/.netlify/functions/health';
const BATCH_SIZE = 100; // rows per INSERT batch

function getApiKey() { return sessionStorage.getItem('cygenix_api_key') || ''; }
function setApiKey(k) { sessionStorage.setItem('cygenix_api_key', k.trim()); }

const state = {
  jobs: [], files: [], parsedTables: [],
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

function setBadge(s) {
  const b = $('conn-badge');
  if (s === 'ok')  { b.textContent = 'Claude AI · Connected'; b.className = 'conn-badge ok'; }
  else if (s === 'err') { b.textContent = 'Enter API key ↓'; b.className = 'conn-badge err'; }
  else { b.textContent = 'Checking…'; b.className = 'conn-badge checking'; }
}

async function checkHealth() {
  if (getApiKey()) { setBadge('ok'); $('key-alert').classList.remove('visible'); return; }
  try {
    const r = await fetch(HEALTH_URL, { signal: AbortSignal.timeout(6000) });
    const d = await r.json();
    if (d.apiKeyConfigured) { setBadge('err'); $('key-alert').classList.add('visible'); }
    else { setBadge('err'); $('key-alert').classList.add('visible'); }
  } catch { setBadge('err'); $('key-alert').classList.add('visible'); }
}

// ── Navigation ────────────────────────────────────────────────────────────────
function showView(v) {
  document.querySelectorAll('.view').forEach(el => el.classList.remove('active'));
  const el = $('view-' + v); if (el) el.classList.add('active');
  document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
  const map = { dashboard:0,'new-job':1,jobs:2,schema:3,'migration-sql':4,audit:5,supported:6 };
  document.querySelectorAll('.nav-item')[map[v] ?? 0]?.classList.add('active');
  if (v === 'jobs')          renderAllJobs();
  if (v === 'schema')        renderSchemaLibrary();
  if (v === 'migration-sql') renderMigrationLibrary();
  if (v === 'audit')         renderAuditLog();
  if (v === 'dashboard')     renderDashboard();
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

function generateInserts(tableName, columns, rows, schemaMapping) {
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
      const vals = columns.map(col => formatSQLValue(row[col], schemaMapping?.[col]?.type));
      return `    (${vals.join(', ')})`;
    });
    sql += rowStrings.join(',\n') + ';\n';

    // Per-row fallback comment for any batch that might fail
    if (batch.length > 1) {
      sql += `-- If the above batch fails, use individual inserts:\n`;
      sql += batch.map(row => {
        const vals = columns.map(col => formatSQLValue(row[col], schemaMapping?.[col]?.type));
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
// ── CLAUDE SCHEMA ANALYSIS — gets intelligent column mapping ──────────────────
// ═══════════════════════════════════════════════════════════════════════════════

async function askClaudeForSchemaMapping(tables, targetServer, sourceSystem, apiKey) {
  // Send Claude a sample of the data (first 20 rows per table) + inferred types
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
- Flag any date format issues, encoding problems, or data quality concerns`;

  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
      'anthropic-dangerous-direct-browser-access': 'true'
    },
    body: JSON.stringify({
      model: 'claude-sonnet-4-20250514',
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
          setBody(`⚠ Could not extract table data from ${file.name} — it may be an unsupported format`);
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

    let schemaResult;
    try {
      schemaResult = await askClaudeForSchemaMapping(
        allTables, state.selectedTarget,
        $('job-source-input').value.trim(), apiKey
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
        tableMapping?.columns
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
${allTables.map(t => `SELECT '${t.tableName}' AS TableName, COUNT(*) AS ActualRows, ${t.rows.length} AS ExpectedRows FROM dbo.${sqlIdent(t.tableName)};`).join('\n')}`;

    $('analysis-body').textContent = fullReport;
    $('schema-code').textContent    = schemaSQL;
    $('migration-code').textContent = migrationSQL;
    $('result-tabs').style.display  = 'flex';
    $('analysis-cta').innerHTML = `
      <button class="btn btn-teal btn-sm" onclick="downloadSQL('schema','${escapeAttr(jobName)}')">⬇ Schema .sql (${Math.ceil(schemaSQL.length/1024)}KB)</button>
      <button class="btn btn-primary btn-sm" onclick="downloadSQL('migration','${escapeAttr(jobName)}')">⬇ Migration .sql (${Math.ceil(migrationSQL.length/1024)}KB)</button>`;

    const job = {
      id: 'job_' + Date.now(), name: jobName,
      source: $('job-source-input').value.trim(),
      target: state.selectedTarget,
      files: state.files.map(f => ({ name: f.name, size: f.size })),
      tables: allTables.map(t => ({ name: t.tableName, rows: t.rows.length, cols: t.columns.length })),
      analysis: fullReport, schemaSQL, migrationSQL,
      status: 'complete', created: new Date().toISOString(),
      totalRows
    };

    state.jobs.unshift(job);
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
  const content = type==='schema' ? job.schemaSQL : job.migrationSQL;
  if (!content) { alert('No SQL available.'); return; }
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
function updateStats() {
  $('stat-total').textContent    = state.jobs.length;
  $('stat-analysed').textContent = state.jobs.length;
  $('stat-done').textContent     = state.jobs.filter(j=>j.status==='complete').length;
  $('stat-files').textContent    = state.totalFilesProcessed;
  $('jobs-count').textContent    = state.jobs.length;
}

function jobsTableHTML(jobs) {
  if (!jobs.length) return '<div class="empty-state"><h3>No migration jobs yet</h3><p>Create your first SQL migration to get started.</p></div>';
  return '<table class="jobs-table"><thead><tr><th>Job Name</th><th>Source</th><th>Target</th><th>Tables</th><th>Rows</th><th>Status</th><th>Actions</th></tr></thead><tbody>' +
    jobs.map(j => `<tr>
      <td><div class="job-name">${j.name}</div><div class="job-meta">${new Date(j.created).toLocaleDateString('en-GB')}</div></td>
      <td><span style="font-size:11px;color:var(--text3);font-family:var(--mono)">${j.source||'—'}</span></td>
      <td><span style="font-size:12px;color:var(--text2)">${j.target}</span></td>
      <td><span style="font-family:var(--mono);font-size:12px;color:var(--text2)">${j.tables?.length||j.files.length}</span></td>
      <td><span style="font-family:var(--mono);font-size:12px;color:var(--text2)">${j.totalRows?.toLocaleString()||'—'}</span></td>
      <td><span class="badge badge-green">SQL Ready</span></td>
      <td style="display:flex;gap:6px;padding-top:0.75rem;flex-wrap:wrap">
        <button class="btn btn-ghost btn-sm" onclick="openJobModal('${j.id}')">View</button>
        <button class="btn btn-teal btn-sm" onclick="downloadJobSQL('${j.id}','schema')">⬇ Schema</button>
        <button class="btn btn-primary btn-sm" onclick="downloadJobSQL('${j.id}','migration')">⬇ Migration</button>
      </td></tr>`).join('') + '</tbody></table>';
}

function renderDashboard()     { $('dashboard-jobs').innerHTML = jobsTableHTML(state.jobs.slice(0,5)); updateStats(); }
function renderAllJobs()       { $('all-jobs').innerHTML       = jobsTableHTML(state.jobs); }

function codePanel(job, type) {
  const content = type==='schema' ? job.schemaSQL : job.migrationSQL;
  const label   = type==='schema' ? 'Schema SQL' : 'Migration SQL';
  const btnClass = type==='schema' ? 'btn-teal' : 'btn-primary';
  return `<div class="panel" style="margin-bottom:1rem">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.75rem;gap:0.5rem;flex-wrap:wrap">
      <div style="font-size:14px;font-weight:600">${job.name}</div>
      <div style="display:flex;gap:0.5rem;align-items:center">
        <span style="font-size:11px;color:var(--text3);font-family:var(--mono)">${job.totalRows?.toLocaleString()||'?'} rows · ${Math.ceil((content||'').length/1024)}KB</span>
        <button class="btn ${btnClass} btn-sm" onclick="downloadJobSQL('${job.id}','${type}')">⬇ Download</button>
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
  $('audit-log-wrap').innerHTML = state.auditLog.length
    ? '<div class="panel"><table class="jobs-table"><thead><tr><th>Time</th><th>Event</th></tr></thead><tbody>' +
      state.auditLog.map(e=>`<tr><td style="font-family:var(--mono);font-size:11px;color:var(--text3);white-space:nowrap">${e.time}</td><td style="font-size:12px;color:var(--text2)">${e.msg}</td></tr>`).join('') +
      '</tbody></table></div>'
    : '<div class="empty-state"><h3>No audit entries yet</h3></div>';
}

function escHtml(s) { return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function escapeAttr(s) { return s.replace(/'/g,"\\'").replace(/"/g,'&quot;'); }

// ── Init ──────────────────────────────────────────────────────────────────────
$('today-date').textContent = 'SQL Migration workspace · ' + new Date().toLocaleDateString('en-GB',{weekday:'long',day:'numeric',month:'long',year:'numeric'});
renderDashboard();
checkHealth();
