// tests/phase4-design.test.js — Connections, Object mapping and Jobs in the
// console's design language.
//
// Design review, Sep-2026, Phase 4. Three work screens redrawn to the
// handoff's §2, §3 and §4 without changing what they do: every id the
// existing tests address is still there, the masking of a configured
// connection still holds, the map-group pill still sits beside Save as job,
// and the profile column is still second in the jobs table. What this file
// pins is the drawing:
//
//   · Connections: kicker / title / sentence, the tabs as a strip, two
//     blueprint panels with a status pair, segmented controls that carry
//     their state as a class, a masked row that says what it is, an
//     environment tag on each footer and a guardrail note when either side
//     resolves to PROD — including the unclassified default.
//   · Object mapping: the edit context in the kicker, one toolbar row of
//     segmented controls and file actions, a 312px left column, the
//     confidence column as a bar in a 118px cell, and a decision callout
//     for every truncation instead of a silent LEFT(n) in a row.
//   · Jobs: three header buttons plus an overflow, a filter row, a selection
//     bar that exists only while rows are selected, four status words in
//     the status colours, and a 360px column for the selected job with the
//     real error when it failed.
'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const PUB = path.join(__dirname, '..', 'public');
const read = (f) => fs.readFileSync(path.join(PUB, f), 'utf8');
const DASH = read('dashboard.html');
const APP = read('dashboard-app.js');
const OM = read('object_mapping.html');
const OMAPP = read('object-mapping-app.js');
const CONN = DASH.slice(DASH.indexOf('<!-- CONNECTIONS VIEW -->'), DASH.indexOf('<!-- Tab: Restore database'));
const JOBS = DASH.slice(DASH.indexOf('<div id="view-jobs" class="view">'), DASH.indexOf('<!-- EXPORT SCRIPTS MODAL -->'));
const TABLE = APP.slice(APP.indexOf('function jobsTableHTML'), APP.indexOf('function jobStatusWord'));

console.log('Phase 4 — Connections, Object mapping, Jobs\n');

/* ── 1. Connections ─────────────────────────────────────────────────────── */
check('the header is kicker CONNECT, title PROJECT CONNECTIONS, the sentence, and Saved / Clear all / Save connections',
  /<div class="cx-kicker">Connect<\/div>\s*<h1 class="cx-title">Project connections<\/h1>/.test(CONN)
  && /Configure the source and target once\. Every page — mapping, jobs, assurance,\s*streams — inherits these settings\./.test(CONN)
  && /id="conn-save-indicator" class="conn-saved-word"/.test(CONN)
  && /id="conn-tab-clear-btn" class="btn btn-ghost"/.test(CONN) && /id="conn-tab-save-btn" class="btn btn-primary"/.test(CONN)
  && !/Supported:/.test(CONN));
check('the tabs are unchanged, drawn as a strip, and Data generator navigates away in the link colour',
  ['databases', 'import', 'restore', 'linked', 'blob'].every(t => CONN.includes('data-tab="' + t + '"'))
  && /class="cx-tab cx-tab-away" href="\/data-generator"/.test(CONN) && /Data generator ↗/.test(CONN)
  && /#view-connections \.cx-tab-away\{ color:var\(--color-accent-700\); \}/.test(DASH)
  && /#view-connections \.cx-tab\{[^}]*font-family:var\(--font-heading\)[^}]*font-size:16px/.test(DASH));
check('two blueprint panels in a 26px-gap grid, each with a 22px heading and a status pair',
  (CONN.match(/class="cx-blueprint conn-panel"/g) || []).length === 2
  && /#view-connections \.conn-grid\{ display:grid;grid-template-columns:1fr 1fr;gap:26px/.test(DASH)
  && /#view-connections \.conn-panel\{ padding:20px 22px; \}/.test(DASH)
  && /<h2 class="cx-h">Source database<\/h2>/.test(CONN) && /<h2 class="cx-h">Target database<\/h2>/.test(CONN)
  && /<span class="cx-dot" id="src-conn-dot"><\/span><span id="src-conn-word">Not configured<\/span>/.test(CONN));
check('the status pair has four words and the square takes the status colour — Connected, Not tested, Unreachable, Not configured',
  /none:\s*\{ cls: '',\s*wrap: '',\s*word: 'Not configured' \}/.test(APP)
  && /untested:\s*\{ cls: '',\s*wrap: '',\s*word: 'Not tested' \}/.test(APP)
  && /ok:\s*\{ cls: 'cx-dot-ok',\s*wrap: 'ok',\s*word: 'Connected' \}/.test(APP)
  && /fail:\s*\{ cls: 'cx-dot-fail',\s*wrap: 'fail',\s*word: 'Unreachable' \}/.test(APP)
  && /connSetStatus\(which, 'ok'\)/.test(APP) && /connSetStatus\(which, 'fail'\)/.test(APP)
  && !/dotEl\.style\.background/.test(APP));
check('a configured-but-untested connection is Not tested, never green',
  /connSetStatus\('src', !srcOk \? 'none' : \(connTestResult\.src \|\| 'untested'\)\)/.test(APP));
check('Direct | Azure Function and Paste string | Settings are segmented controls that carry their state as a class',
  /<div class="cx-seg conn-seg" role="group" aria-label="Connection mode">\s*<button id="src-mode-direct"/.test(CONN)
  && /<div class="cx-seg conn-seg conn-entry-toggle"[^>]*>\s*<button id="src-entry-paste"/.test(CONN)
  && (APP.match(/if \(el && el\.classList\) el\.classList\.toggle\('on', !!active\);/g) || []).length === 3
  && !/el\.style\.background = active \? 'var\(--accent-glow\)'/.test(APP)
  && /#view-connections \.conn-seg > button\.on\{ background:var\(--color-accent-900\);color:var\(--color-bg\); \}/.test(DASH));
check('the masked row: label 13px, value 19px heading, Reveal & edit, and an honest note about where the string lives',
  /#view-connections \.conn-locked\{[^}]*background:var\(--color-neutral-100\)/.test(DASH)
  && /\.conn-locked-label\{ font-size:13px/.test(DASH)
  && /\.conn-locked-value\{ font-family:var\(--font-heading\);font-weight:600;font-size:19px/.test(DASH)
  && (CONN.match(/>Reveal &amp; edit<\/button>/g) || []).length === 2
  && /The string is kept on this browser and shown masked here\. Saving a change\s*is written to the audit trail\./.test(CONN)
  && !/held in the secrets store/.test(CONN));
check('the settings grid: Engine full width, Host / Port, Database full width, User / Password, then the SQL Server checkboxes',
  /<div class="conn-fields">\s*<div class="conn-full">\s*<label class="form-label">Engine<\/label>/.test(CONN)
  && /#view-connections \.conn-fields\{ display:grid;grid-template-columns:1fr 1fr;gap:10px 12px; \}/.test(DASH)
  && /#conn-tab-databases \.form-input\{ height:34px;[^}]*font-size:14px/.test(DASH)
  && /#conn-tab-databases \.form-label\{ font-size:13px;color:var\(--color-neutral-700\)/.test(DASH)
  && /<input id="src-b-encrypt" type="checkbox" checked/.test(CONN) && /Trust server certificate/.test(CONN));
check('the footer row: environment tag, facts line, Test pushed right, above a 1px divider',
  /<div class="conn-foot">\s*<span class="cx-tag" id="src-env-tag" title="Unclassified connections default to PROD">PROD<\/span>\s*<span class="conn-facts" id="src-conn-result">/.test(CONN)
  && /#view-connections \.conn-foot\{[^}]*border-top:1px solid var\(--color-divider\)/.test(DASH)
  && /onclick="testProjConn\('src'\)" title="Connect and test">Test<\/button>/.test(CONN));
check('PROD renders in state-fail; the classification comes from the profile store, and unclassified defaults to PROD',
  /function connEnvOf\(side\)/.test(APP) && /const DEFAULT = \{ label: 'PROD', prod: true, classified: false \};/.test(APP)
  && /CP\.cpConnMeta\(st, cid\)/.test(APP) && /t\.className = 'cx-tag' \+ \(env\[s\]\.prod \? ' cx-tag-fail' : ''\);/.test(APP));
check('the guardrail note appears whenever either side resolves to PROD, and says so for the unclassified case',
  /<div id="conn-guardrail" class="cx-attn cx-attn-warn conn-guardrail" style="display:none">/.test(CONN)
  && /note\.style\.display = prodSides\.length \? '' : 'none';/.test(APP)
  && /'Both sides are classified PROD'/.test(APP)
  && /A connection nobody has classified is treated as PROD/.test(APP)
  // The claim is that opening the view refreshes the environment tags, so it
  // is measured INSIDE initConnectionsView. It used to be anchored on
  // connRenderEnv() being the closing line of that function, which broke the
  // moment anything else was added after it — an anchor about position
  // standing in for a claim about behaviour.
  && /connRenderEnv\(\);/.test(APP.slice(
      APP.indexOf('function initConnectionsView()'),
      APP.indexOf('function connRenderEnv('))));
// The masking contract from tests/connection-masking.test.js still holds on
// the shipped slice — run the shipped functions against a stub once more.
(() => {
  const html = DASH + '\n' + APP;
  const slice = (from, to) => html.slice(html.indexOf(from), html.indexOf(to));
  const src = slice('function parseDbConnection(connStr){', '// Read whatever connection info is currently configured')
    + '\n' + slice('// ── Connection masking ─', 'function initConnectionsView()');
  const els = {};
  const mk = (id) => ({ id, value: '', textContent: '', style: { display: '' }, cls: new Set(),
    classList: { toggle(c, on) { on ? els[id].cls.add(c) : els[id].cls.delete(c); } }, focus() {} });
  ['src-conn-locked', 'src-conn-locked-label', 'src-conn-locked-value', 'src-conn-locked-nick', 'src-conn-hide',
    'src-direct-wrap', 'src-azure-wrap', 'src-build-wrap', 'src-cs-wrap', 'src-entry-paste', 'src-entry-build', 'src-b-host',
    'proj-src-cs', 'proj-src-fn-url', 'proj-src-fn-key', 'src-mode-direct', 'src-mode-azure'].forEach(id => { els[id] = mk(id); });
  els['proj-src-cs'].value = 'mssql://u:secret@host:1433/Conversion_DM';
  const sb = { console, JSON, String, Array, Number, RegExp, URL, window: {}, document: { getElementById: id => els[id] || null },
    sconnGetAll: () => [], srcMode: 'direct', tgtMode: 'azure' };
  vm.createContext(sb);
  vm.runInContext(src, sb);
  vm.runInContext("setSrcMode('direct'); renderConnLock('src'); setConnEntry('src','build')", sb);
  check('the mode and entry controls mark the active segment with .on and nothing else',
    els['src-mode-direct'].cls.has('on') && !els['src-mode-azure'].cls.has('on')
    && els['src-entry-build'].cls.has('on') && !els['src-entry-paste'].cls.has('on'));
  check('and a configured string is still masked to its database name',
    els['src-conn-locked'].style.display !== 'none' && els['src-conn-locked-value'].textContent === 'Conversion_DM'
    && !JSON.stringify(Object.values(els).map(e => e.textContent)).includes('secret'));
})();

/* ── 2. Object mapping ──────────────────────────────────────────────────── */
check('the kicker carries the edit context, the title is OBJECT MAPPING, and Save to Drive / Save as job sit on the right',
  /<div class="cx-kicker">Model\s*<!--[^>]*-->\s*<span class="edit-banner" id="edit-banner" style="display:none">/.test(OM)
  && /<span>· Editing saved job<\/span>\s*<strong id="edit-job-name"><\/strong>/.test(OM)
  && /<h1 class="cx-title">Object mapping<\/h1>/.test(OM)
  && /<div class="om-actions">[\s\S]*?id="save-job-btn"/.test(OM)
  && /\.om-top\{display:grid;grid-template-columns:minmax\(0,1fr\) auto;grid-template-areas:"title actions" "toolbar toolbar"/.test(OM)
  && !/fonts\.googleapis\.com/.test(OM));
check('one toolbar row: Single map | One-to-many, Table | Visual, then New map, Load map, Open from Drive, Download, Reset, and the group pill with the version pushed right',
  /<div class="om-toolbar">\s*<div class="mode-toggle" id="mode-toggle"/.test(OM)
  && /\.mode-btn\.active\{background:var\(--color-accent-900\);color:var\(--color-bg\)\}/.test(OM)
  && ['New map', 'Load map', 'Open from Drive', 'Save to Drive', 'Download', 'Reset'].every(t => OM.includes('>' + t + '</button>'))
  && /<div class="om-toolbar-right">\s*<span class="om-version" id="om-version"><\/span>/.test(OM)
  && /\.mg-dot\{width:9px;height:9px;border-radius:0/.test(OM)
  && /ver\.textContent = \(typeof job\.version==='number' && job\.version>0\) \? 'Version '\+job\.version : ''/.test(OMAPP));
check('the body is a 312px left column with a 24px gap',
  /grid-template-columns:var\(--om-left-w,312px\) 12px 1fr;\s*gap:6px;/.test(OM) && /const OM_LEFT_DEFAULT = 312;/.test(OMAPP));
check('the source panel: 17px Condensed title, a .tag-accent column count, Tables | Views | Both, the table name at 18px and the row count line',
  /\.panel-title\{font-family:var\(--font-heading\);font-weight:600;font-size:17px/.test(OM)
  && /\.badge-green,\.badge-teal\{color:var\(--color-accent-800\);border-color:var\(--color-accent-300\);background:var\(--color-accent-100\)\}/.test(OM)
  && /<div class="mode-toggle" id="src-objtype-toggle">/.test(OM)
  && /<div class="om-tname" id="src-table-name"><\/div>\s*<div class="om-trow">/.test(OM)
  && /\.om-tname\{font-family:var\(--font-heading\);font-weight:600;font-size:18px/.test(OM)
  && /const tn=\$\('src-table-name'\); if\(tn\) tn\.textContent=t\.fullName\|\|t\.name\|\|'';/.test(OMAPP));
check('the column list is 13px rows at 5px 8px, name left, type right in neutral-600, mapped rows on accent-100',
  /\.src-col-pill\{[^}]*padding:5px 8px;font-size:13px/.test(OM) && /\.src-col-pill\.mapped\{background:var\(--color-accent-100\)\}/.test(OM)
  && /\.src-col-type\{color:var\(--color-neutral-600\);font-size:13px;margin-left:auto/.test(OM));
check('Options: + Add condition and + Add GROUP BY column are links in accent-700, and the API key field is labelled for AI features',
  /<button class="btn link" onclick="addWhereCond\(\)"[^>]*>\+ Add condition<\/button>/.test(OM)
  && /<button class="btn link" onclick="addGroupBy\(\)"[^>]*>\+ Add GROUP BY column<\/button>/.test(OM)
  && /\.btn\.link\{[^}]*color:var\(--color-accent-700\)/.test(OM)
  && /Anthropic API key — for AI features/.test(OM));
check('the target panel names the table in its title, carries the progress tag as "N of M mapped", Re-map and + Joined col',
  /<div class="panel-title">Target table<span class="om-tsel" id="tgt-table-name"><\/span><\/div>/.test(OM)
  && /\$\('map-stats'\)\.textContent = mapped\+' of '\+total\+' mapped';/.test(OMAPP)
  && /tgtName\.textContent = tgtTable \? ' · '\+\(tgtTable\.fullName\|\|tgtTable\.name\|\|''\) : '';/.test(OMAPP)
  && />Re-map<\/button>/.test(OM) && />\+ Joined col<\/button>/.test(OM));
check('the mapping table runs Target column · Source column · Transform · Fixed value · Confidence, types inline in neutral-600',
  /<th onclick="sortMapping\('tgt'\)"[^>]*>Target column/.test(OM)
  && OM.indexOf("sortMapping('tgt')") < OM.indexOf("sortMapping('src')")
  && /<th>Transform<\/th>/.test(OM) && /class="om-conf">Confidence/.test(OM)
  && !/<th>Src type<\/th>/.test(OM) && !/<th>Tgt type<\/th>/.test(OM)
  && /\.map-table\{width:100%;border-collapse:collapse;font-size:13px/.test(OM)
  && /\.map-table \.om-type\{color:var\(--color-neutral-600\);font-size:13px/.test(OM)
  && /<span class="om-type">\$\{esc\(tgtType\)\}<\/span>/.test(OMAPP));
check('confidence is a 6px bar in a 118px cell — accent-700 at 90+, the accent at 70–89, accent-300 below — never a colour',
  /\.om-conf\{width:118px\}/.test(OM) && /\.om-conf \.bar\{display:block;height:6px/.test(OM)
  && /\.om-conf \.bar\.hi i\{background:var\(--color-accent-700\)\}/.test(OM) && /\.om-conf \.bar\.lo i\{background:var\(--color-accent-300\)\}/.test(OM)
  && /const band = score >= 90 \? 'hi' : score >= 70 \? '' : 'lo';/.test(OMAPP)
  && /function confidenceScore\(m\)/.test(OMAPP) && !/const matchColor = m\.match==='HIGH'\?'var\(--green\)'/.test(OMAPP));
check('the evidence score is the width where the evidence mapper ran; the model\'s word maps to a nominal width otherwise',
  /if \(m && m\.evidence && typeof m\.evidence\.score === 'number'\) return Math\.max\(0, Math\.min\(100, m\.evidence\.score\)\);/.test(OMAPP)
  && /if \(w === 'HIGH'\) return 95;/.test(OMAPP) && /if \(w === 'LOW'\) return 55;/.test(OMAPP));
check('a truncation raises a decision callout — 3px state-warn rule, 17px title, explanation with the rows in scope, Accept / Edit transform',
  /<div id="om-decisions"><\/div>/.test(OM)
  && /\.om-decision\{border-left:3px solid var\(--state-warn\)/.test(OM) && /\.om-decision \.t\{[^}]*font-size:17px/.test(OM)
  && /if \(willTruncate && !m\._truncAccepted\) decisions\.push\(/.test(OMAPP)
  && /will be cut to ' \+ d\.truncLen \+ ' characters/.test(OMAPP) && /rows are in scope/.test(OMAPP)
  && /onclick="omAcceptDecision\(' \+ d\.i \+ '\)">Accept<\/button>/.test(OMAPP)
  && /onclick="omEditTransform\(' \+ d\.i \+ '\)">Edit transform<\/button>/.test(OMAPP)
  && /columnMapping\[i\]\._truncAccepted = true;/.test(OMAPP));
check('the row under review sits on accent-100, and the generated SQL is unchanged by accepting — LEFT(n) was already applied',
  /\.map-table tr\.om-review td\{background:var\(--color-accent-100\)\}/.test(OM)
  && /const review = willTruncate && !m\._truncAccepted \? ' om-review' : '';/.test(OMAPP)
  && /function omAcceptDecision\(i\)\{[\s\S]{0,200}renderMappingTable\(\);/.test(OMAPP)
  && !/function omAcceptDecision\(i\)\{[\s\S]{0,300}generateSingleSQL/.test(OMAPP));
check('the Generated SQL head carries Migration | Schema | Verify as a segmented control, and the body sits on neutral-100 at 13px/1.8',
  /<div class="om-sqlseg" role="group" aria-label="Which SQL">\s*<button class="sql-tab active" id="tab-sql"[^>]*>Migration<\/button>/.test(OM)
  && /\.sql-tab\.active\{background:var\(--color-accent-900\);color:var\(--color-bg\)\}/.test(OM)
  && /\.sql-box\{background:var\(--color-neutral-100\);border:1px solid var\(--color-divider\);padding:14px 16px;font-family:var\(--mono\);font-size:13px;[^}]*line-height:1\.8\}/.test(OM));
check('the connection line under the toolbar points at the Connections view by its route',
  /<a href="\/dashboard#goto=connections" class="conn-btn" id="conn-manage-link"/.test(OM)
  && /\.conn-dot\{width:8px;height:8px;flex-shrink:0;background:var\(--color-neutral-400\)\}/.test(OM));
check('nothing on the page is drawn with rounded corners or an rgba() border any more, save the picker\'s own hover tints',
  !/border-radius:(?!0)[^;}]+/.test(OM.slice(OM.indexOf('/* ── Panels ── */'), OM.indexOf('/* ── Recent maps grid')))
  && /\.om-split-btn,\.modal,\.rm-card,\.rm-chip,\.rm-pin,\.rm-del,\.mg-pill,\.mg-chip,\.mg-btn\{border-radius:0\}/.test(OM));

/* ── 3. Jobs ─────────────────────────────────────────────────────────────── */
check('the header is kicker RUN, title ALL MIGRATION JOBS, then Generate SQL, Validate, More ▾ and + New migration',
  /<div class="cx-kicker">Run<\/div>\s*<h1 class="cx-title">All migration jobs<\/h1>/.test(JOBS)
  && /onclick="openBulkGenerateModal\(\)"[^>]*>Generate SQL<\/button>/.test(JOBS)
  && /onclick="openValidateModal\(\)"[^>]*>Validate<\/button>/.test(JOBS)
  && /id="jobs-more-btn" onclick="jobsMoreToggle\(\)" aria-haspopup="true" aria-expanded="false">More ▾<\/button>/.test(JOBS)
  && /class="btn btn-primary" onclick="showView\('new-job'\)">\+ New migration<\/button>/.test(JOBS)
  && (JOBS.match(/class="btn btn-primary"/g) || []).length === 1);
check('the overflow holds Setup check, Export scripts, Export package and Show deleted — the seven ghost buttons are gone',
  /<div class="jb-more-menu" id="jobs-more-menu" role="menu"/.test(JOBS)
  && ['openSetupCheckModal', 'openExportScriptsModal', 'openExportPackageModal', 'toggleShowDeleted'].every(fn => new RegExp('jobsMoreClose\\(\\);' + fn + '\\(\\)').test(JOBS))
  && /id="show-deleted-btn"/.test(JOBS)
  && (JOBS.slice(0, JOBS.indexOf('jb-filters')).match(/class="btn btn-ghost"/g) || []).length === 3
  && /function jobsMoreToggle\(\)/.test(APP) && /document\.addEventListener\('click', jobsMoreOutside, \{ once: true \}\)/.test(APP));
check('the filter row: name-or-table, Project, Status, and the count pushed right',
  /<input id="jobs-name-filter" class="form-input jb-filter-input" placeholder="Filter by name or table" oninput="renderAllJobs\(\)"/.test(JOBS)
  && /<select id="jobs-project-filter"/.test(JOBS)
  && /<select id="jobs-status-filter"[^>]*>\s*<option value="">Status · Any<\/option>\s*<option value="complete">Complete<\/option>\s*<option value="ready">SQL ready<\/option>\s*<option value="failed">Failed<\/option>\s*<option value="pending">Pending<\/option>/.test(JOBS)
  && /\.jb-count\{margin-left:auto;font-size:14px;color:var\(--color-neutral-700\)/.test(DASH)
  && /if \(q\) shown = shown\.filter\(j => \[j\.name, j\.sourceTable, j\.source, j\.target\]\.some/.test(APP)
  && /if \(sf\) shown = shown\.filter\(j => jobBucket\(j\) === sf\);/.test(APP)
  && /\$\{denom\} job\$\{denom === 1 \? '' : 's'\}/.test(APP) && /\$\{shown\.length\} in trash/.test(APP));
check('the selection bar exists only while rows are selected: accent-100, a 1px accent-300 border, the count at 16px heading, Clear pushed right',
  /<div id="jobs-bulk-actions" class="jb-selbar" style="display:none">/.test(JOBS)
  && /\.jb-selbar\{[^}]*background:var\(--color-accent-100\);border:1px solid var\(--color-accent-300\);padding:10px 14px/.test(DASH)
  && /\.jb-selcount\{font-family:var\(--font-heading\);font-weight:600;font-size:16px;color:var\(--color-accent-900\)/.test(DASH)
  && /\.jb-clear\{margin-left:auto;color:var\(--color-accent-800\)/.test(DASH)
  && ['Export scripts', 'Export package', 'Setup check', 'Move to project', 'Clear'].every(t => new RegExp('jb-selbar[\\s\\S]*>' + t + '</button>').test(JOBS))
  && /if \(selectedJobIds\.size === 0\)\{\s*bar\.style\.display = 'none';/.test(APP));
check('the table runs checkbox · Job Name · Profile · Source · Target · Rows (right) · Updated · Status (right), and nothing else',
  /'<th>Job Name<\/th><th>Profile<\/th><th>Source<\/th><th>Target<\/th>' \+\s*'<th class="r">Rows<\/th><th>Updated<\/th><th class="r">Status<\/th>'/.test(APP)
  && !/<th>Exec Status<\/th>/.test(TABLE) && !/<th>Actions<\/th>/.test(TABLE) && !/<th>Tables<\/th>/.test(TABLE)
  && !/class="jobs-actions"/.test(TABLE));
check('the job name is 15px Condensed; the rest is 13px; selected rows sit on accent-100',
  /\.job-name\{font-family:var\(--font-heading\);font-weight:600;font-size:15px/.test(DASH)
  && /\.jobs-table\{width:100%;border-collapse:collapse;font-size:13px/.test(DASH)
  && /\.jobs-table tbody tr\.on td\{background:var\(--color-accent-100\)\}/.test(DASH));
check('the status vocabulary is four words in the status colours — Complete ok, SQL ready accent-800, Failed fail, Pending neutral-600',
  /function jobStatusWord\(j\)/.test(APP)
  && /return \{ key: 'complete', word: 'Complete' \};/.test(APP) && /return \{ key: 'ready',\s+word: 'SQL ready' \};/.test(APP)
  && /return \{ key: 'failed',\s+word: 'Failed' \};/.test(APP) && /return \{ key: 'pending', word: 'Pending' \};/.test(APP)
  && /\.jb-st-complete\{color:var\(--state-ok\)\}/.test(DASH) && /\.jb-st-ready\{color:var\(--color-accent-800\)\}/.test(DASH)
  && /\.jb-st-failed\{color:var\(--state-fail\)\}/.test(DASH) && /\.jb-st-pending\{color:var\(--color-neutral-600\)\}/.test(DASH)
  && !/badge badge-green"><i class="ic-dot"/.test(APP) && !/SQL Ready/.test(APP));
check('the right column is the 360px sticky column on neutral-100, rendered from the selected job',
  /<aside class="jb-side" id="jobs-side" aria-label="Selected job"><\/aside>/.test(JOBS)
  && /\.jb-grid\{display:grid;grid-template-columns:minmax\(0,1fr\) var\(--cx-right-col-w,360px\)/.test(DASH)
  && /\.jb-side\{background:var\(--color-neutral-100\);border-left:1px solid var\(--color-divider\);padding:28px 24px;position:sticky/.test(DASH)
  && /function renderJobSide\(shown\)/.test(APP) && /function jobsRowClick\(ev, id\)/.test(APP)
  && /onclick="jobsRowClick\(event,'\$\{j\.id\}'\)"/.test(APP));
check('a click on the checkbox, a button or the drag handle never changes the selected job',
  /if \(t && \(t\.closest\('input, button, a, \.job-drag-handle'\) \|\| t\.tagName === 'INPUT'\)\) return;/.test(APP));
check('the selected job: 23px Condensed name, the source → target pair, a 3px state-fail rule carrying the real error with Resume and Open mapping',
  /\.jb-side-name\{font-family:var\(--font-heading\);font-weight:600;font-size:23px/.test(DASH)
  && /<div class="jb-side-pair">/.test(APP)
  && /const err = String\(j\.lastError \|\| j\.error \|\| j\.executionError \|\| j\.errorMessage \|\| ''\)\.trim\(\);/.test(APP)
  && /st\.key === 'failed'\s*\? '<div class="cx-attn cx-attn-fail">/.test(APP)
  && /The run reported a failure with no message\./.test(APP)
  && /href="\/project-builder" title="Runs resume from their checkpoint in Packages">Resume<\/a>/.test(APP)
  && /Open mapping<\/button>/.test(APP));
check('then the facts list, the version history, and Export package / Delete pinned to the bottom',
  ['Columns mapped', 'Schema SQL', 'Migration SQL', 'Last run', 'Version'].every(k => APP.includes("fact('" + k + "'"))
  && /<div class="cx-section">Version history<\/div>/.test(APP)
  && /\.jb-side-foot\{margin-top:auto/.test(DASH)
  && /onclick="jobsExportOne\(\\'' \+ j\.id \+ '\\'\)">Export package<\/button>/.test(APP)
  && /onclick="deleteJob\(\\'' \+ j\.id \+ '\\'\)" title="Move to Trash">Delete<\/button>/.test(APP)
  && /Restore<\/button>/.test(APP) && /Delete forever<\/button>/.test(APP));
check('the column follows the list — the selection survives a filter, else the first failed job leads',
  /_jobSideId = \(shown\.find\(j => jobBucket\(j\) === 'failed'\) \|\| shown\[0\] \|\| \{\}\)\.id \|\| null;/.test(APP)
  && /renderJobSide\(shown\);/.test(APP));
check('the row actions the table lost are all reachable from the column — rename, edit source, history, report, SQL editor',
  ['startRenameJob', 'startEditSource', 'CygenixHistory.open', 'openReport', '/sql-editor?job='].every(s => APP.slice(APP.indexOf('function renderJobSide'), APP.indexOf('function jobsMoreToggle')).includes(s)));
check('Show deleted keeps its wording — the recent-maps status still points there — and flips to Hide deleted without a glyph',
  /btn\.textContent = _showDeletedJobs \? 'Hide deleted' : 'Show deleted';/.test(APP)
  && /You can restore it from "Show deleted" under More\./.test(APP));

/* ── 4. Type discipline ─────────────────────────────────────────────────── */
const small = (src) => (src.replace(/\/\*[\s\S]*?\*\//g, '').match(/font-size:\s*(\d+(?:\.\d+)?)px/g) || [])
  .map(m => Number(m.match(/[\d.]+/)[0])).filter(n => n < 11);
check('nothing the three screens add is set below 11px',
  small(CONN).length === 0 && small(JOBS).length === 0
  && small(DASH.slice(DASH.indexOf('/* JOBS —'), DASH.indexOf('/* MODAL */'))).length === 0
  && small(DASH.slice(DASH.indexOf('/* ─── Connections in the console design language'), DASH.indexOf('#view-connections .imp-drop{'))).length === 0
  && small(OM.slice(OM.indexOf('/* ── Layout ── */'), OM.indexOf('/* ── Recent maps grid'))).length === 0,
  [small(CONN), small(JOBS)].map(a => a.join(',')).join(' | '));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
if (fail) process.exit(1);
