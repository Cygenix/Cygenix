/* ============================================================================
   cygenix-analyser-widget.js — the Data Analyser's UI.
   ----------------------------------------------------------------------------
   The drop-in front end for cygenix-analyser.js. It injects its own styles,
   scoped under .cygx, and mounts into whatever element the page hands it —
   see public/data-analyser.html for the one call that does so, and for the
   page-level overrides that put the console's accent and type on it.

   Renamed from cygenix-transform-widget.js for the reason given at the top of
   cygenix-analyser.js: the engine's original global collided with a module
   this repository already had. The widget reads the engine from
   root.CygenixAnalyser and exposes itself as CygenixAnalyserWidget. The
   vendor's header follows.
   ========================================================================== */
/*!
 * cygenix-analyser-widget.js — drop-in UI for cygenix-analyser.js
 *
 *   <div id="converter"></div>
 *   <script src="cygenix-analyser.js"></script>
 *   <script src="cygenix-analyser-widget.js"></script>
 *   <script>CygenixAnalyserWidget.mount('#converter');</script>
 *
 * Options: { theme:'dark'|'light', target:'sql', dialect:'sqlserver', table:'imported_data',
 *            samples:true, mapping:true, maxPreviewRows:50, onResult:fn }
 * Styles are injected once and scoped under .cygx — nothing leaks into the host page.
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(require('./cygenix-analyser.js'));
  else root.CygenixAnalyserWidget = factory(root.CygenixAnalyser);
}(typeof self !== 'undefined' ? self : this, function (CT) {
  'use strict';

  if (!CT) throw new Error('cygenix-analyser.js must be loaded before the widget.');

  var CSS = `
.cygx{--bg:#08090b;--panel:#0e1013;--panel-2:#131619;--line:#22262c;--line-soft:#191d22;
  --text:#e7eaee;--dim:#8b929c;--faint:#5d646e;--accent:#5ac8fa;--accent-dim:#1d3a47;
  --ok:#4ade80;--warn:#fbbf24;--bad:#f87171;--mono:ui-monospace,SFMono-Regular,"SF Mono",Menlo,Consolas,monospace;
  color:var(--text);background:var(--bg);font:14px/1.5 ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;
  border:1px solid var(--line);border-radius:12px;overflow:hidden;-webkit-font-smoothing:antialiased}
.cygx[data-theme=light]{--bg:#ffffff;--panel:#f7f8f9;--panel-2:#eef0f2;--line:#dfe3e8;--line-soft:#eaedf0;
  --text:#14171a;--dim:#5c646e;--faint:#8b929c;--accent:#0b6fa4;--accent-dim:#d6ecf7}
.cygx *,.cygx *::before,.cygx *::after{box-sizing:border-box}
.cygx button,.cygx select,.cygx input,.cygx textarea{font:inherit;color:inherit}

.cygx-head{display:flex;align-items:center;gap:10px;padding:12px 16px;border-bottom:1px solid var(--line);background:var(--panel)}
.cygx-mark{width:9px;height:9px;border-radius:2px;background:var(--accent);flex:none}
.cygx-title{font-weight:600;letter-spacing:-.01em}
.cygx-ver{margin-left:auto;font:11px/1 var(--mono);color:var(--faint)}

.cygx-drop{margin:16px;border:1.5px dashed var(--line);border-radius:10px;padding:28px 20px;text-align:center;
  transition:border-color .15s,background .15s;cursor:pointer;background:var(--panel)}
.cygx-drop:hover,.cygx-drop:focus-visible{border-color:var(--accent);background:var(--panel-2);outline:none}
.cygx-drop.is-over{border-color:var(--accent);background:var(--accent-dim)}
.cygx-drop h3{margin:0 0 4px;font-size:15px;font-weight:600}
.cygx-drop p{margin:0;color:var(--dim);font-size:13px}
.cygx-paste{width:calc(100% - 32px);margin:0 16px 12px;min-height:96px;resize:vertical;padding:10px 12px;
  background:var(--panel);border:1px solid var(--line);border-radius:8px;font:12.5px/1.55 var(--mono);color:var(--text)}
.cygx-paste:focus{outline:none;border-color:var(--accent)}
.cygx-paste::placeholder{color:var(--faint)}

.cygx-chips{display:flex;flex-wrap:wrap;gap:6px;padding:0 16px 16px;align-items:center}
.cygx-chips .cygx-lab{color:var(--faint);font-size:12px;margin-right:2px}
.cygx-chip{background:var(--panel);border:1px solid var(--line);color:var(--dim);border-radius:999px;
  padding:4px 11px;font-size:12px;cursor:pointer;transition:.12s}
.cygx-chip:hover{border-color:var(--accent);color:var(--text)}

.cygx-result{border-top:1px solid var(--line)}
.cygx-verdict{display:flex;flex-wrap:wrap;gap:16px 24px;align-items:flex-start;padding:16px;background:var(--panel)}
.cygx-fmt{font:600 20px/1.2 var(--mono);letter-spacing:-.02em}
.cygx-fmt small{display:block;font:400 12px/1.4 ui-sans-serif,system-ui,sans-serif;color:var(--dim);letter-spacing:0;margin-top:3px}
.cygx-conf{width:200px;flex:none}
.cygx-bar{height:5px;border-radius:3px;background:var(--panel-2);overflow:hidden;margin-top:6px}
.cygx-bar span{display:block;height:100%;background:var(--accent);border-radius:3px}
.cygx-bar.is-warn span{background:var(--warn)}.cygx-bar.is-bad span{background:var(--bad)}
.cygx-kv{display:flex;gap:20px;margin-left:auto;flex-wrap:wrap}
.cygx-kv div{text-align:right}
.cygx-kv b{display:block;font:600 17px/1.2 var(--mono)}
.cygx-kv span{font-size:11px;color:var(--faint);text-transform:uppercase;letter-spacing:.06em}
.cygx-why{padding:0 16px 14px;background:var(--panel);color:var(--dim);font-size:12.5px}
.cygx-why li{margin:2px 0}
.cygx-why ul{margin:0;padding-left:18px}

.cygx-flags{padding:0 16px 14px;background:var(--panel);display:grid;gap:6px}
.cygx-flag{display:flex;gap:9px;align-items:flex-start;padding:8px 11px;border-radius:7px;font-size:12.5px;
  background:var(--panel-2);border-left:3px solid var(--dim)}
.cygx-flag.sev-high{border-left-color:var(--bad)}
.cygx-flag.sev-medium{border-left-color:var(--warn)}
.cygx-flag.sev-low{border-left-color:var(--faint)}
.cygx-flag code{font:11.5px var(--mono);color:var(--accent);flex:none}

.cygx-tabs{display:flex;gap:2px;padding:0 12px;border-top:1px solid var(--line);border-bottom:1px solid var(--line);background:var(--panel)}
.cygx-tab{background:none;border:none;border-bottom:2px solid transparent;padding:11px 12px;color:var(--dim);
  cursor:pointer;font-size:13px;margin-bottom:-1px}
.cygx-tab:hover{color:var(--text)}
.cygx-tab[aria-selected=true]{color:var(--text);border-bottom-color:var(--accent);font-weight:600}

.cygx-pane{padding:16px;max-height:460px;overflow:auto}
.cygx-pane[hidden]{display:none}
.cygx-table{width:100%;border-collapse:collapse;font-size:12.5px}
.cygx-table th{text-align:left;font:600 11px/1 ui-sans-serif,system-ui,sans-serif;text-transform:uppercase;
  letter-spacing:.06em;color:var(--faint);padding:0 12px 8px 0;border-bottom:1px solid var(--line);white-space:nowrap;
  position:sticky;top:0;background:var(--bg)}
.cygx-table td{padding:7px 12px 7px 0;border-bottom:1px solid var(--line-soft);vertical-align:top}
.cygx-table tbody tr:hover{background:var(--panel)}
.cygx-name{font:12.5px var(--mono);color:var(--text)}
.cygx-type{font:11.5px var(--mono);color:var(--accent);background:var(--accent-dim);padding:2px 7px;border-radius:4px;white-space:nowrap}
.cygx-num{font:12px var(--mono);color:var(--dim);text-align:right;white-space:nowrap}
.cygx-table th.num{text-align:right}
.cygx-note{color:var(--faint);font:11.5px/1.5 var(--mono);white-space:normal;min-width:200px}
.cygx-conf-lab{font:11px/1 ui-sans-serif,system-ui,sans-serif;text-transform:uppercase;letter-spacing:.06em;color:var(--faint);display:flex;justify-content:space-between;gap:12px}
.cygx-conf-lab b{font:600 12px var(--mono);color:var(--text);letter-spacing:0}
.cygx-samp{color:var(--faint);font:11.5px var(--mono);max-width:230px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.cygx-pk{font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:var(--ok);border:1px solid currentColor;
  border-radius:3px;padding:1px 5px;margin-left:6px;white-space:nowrap}
.cygx-miniwarn{color:var(--warn);font-size:11px;display:block;margin-top:3px}
.cygx-scroll{overflow:auto}
.cygx-preview td{font:12px var(--mono);white-space:nowrap;max-width:260px;overflow:hidden;text-overflow:ellipsis}
.cygx-null{color:var(--faint);font-style:italic}

.cygx-ctl{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:12px}
.cygx-ctl label{font-size:11px;color:var(--faint);text-transform:uppercase;letter-spacing:.06em}
.cygx-sel,.cygx-in{background:var(--panel);border:1px solid var(--line);border-radius:7px;padding:6px 9px;font-size:13px}
.cygx-sel:focus,.cygx-in:focus{outline:none;border-color:var(--accent)}
.cygx-in{font-family:var(--mono);font-size:12.5px;width:150px}
.cygx-btn{background:var(--panel);border:1px solid var(--line);border-radius:7px;padding:6px 13px;font-size:13px;
  cursor:pointer;color:var(--text);transition:.12s}
.cygx-btn:hover{border-color:var(--accent)}
.cygx-btn.is-primary{background:var(--accent);border-color:var(--accent);color:#04141c;font-weight:600}
.cygx-btn.is-primary:hover{filter:brightness(1.08)}
.cygx-spacer{margin-left:auto}
.cygx-out{margin:0;padding:13px;background:var(--panel);border:1px solid var(--line);border-radius:8px;
  font:12px/1.6 var(--mono);white-space:pre;overflow:auto;max-height:330px;color:var(--text)}

.cygx-map textarea{width:100%;min-height:72px;resize:vertical;padding:9px 11px;background:var(--panel);
  border:1px solid var(--line);border-radius:8px;font:12.5px var(--mono);color:var(--text);margin-bottom:10px}
.cygx-map textarea:focus{outline:none;border-color:var(--accent)}
.cygx-hint{color:var(--faint);font-size:12px;margin:0 0 8px}
.cygx-arrow{color:var(--faint);padding:0 4px}
.cygx-review{color:var(--warn)}
.cygx-unmapped{margin-top:14px;font-size:12.5px;color:var(--dim)}
.cygx-unmapped b{color:var(--text);font-weight:600}
.cygx-unmapped code{font:11.5px var(--mono);background:var(--panel-2);padding:1px 6px;border-radius:4px;margin-right:4px;display:inline-block}

.cygx-err{margin:16px;padding:12px 14px;border-radius:8px;background:var(--panel);border-left:3px solid var(--bad);font-size:13px}
.cygx-empty{color:var(--faint);font-size:13px;padding:8px 0}
@media (max-width:620px){.cygx-kv{margin-left:0}.cygx-pane{padding:12px}}
`;

  var SAMPLES = {
    'CSV': 'order_ref,customer,order_date,net_amount,vat,settled\nORD-1001,"Whitfield & Sons, Ltd",2026-03-04,1042.50,208.50,true\nORD-1002,"O\'Brien Logistics",2026-03-05,86.00,17.20,false\nORD-1003,"Halcyon Media",2026-03-05,,0.00,false\nORD-1004,"Redgrave Legal LLP",2026-03-06,12500.00,2500.00,true\nORD-1005,"Whitfield & Sons, Ltd",2026-03-09,340.75,68.15,true',
    'JSON': '[\n  {"id":1,"customer":{"name":"Ann Whitfield","address":{"city":"Leeds","postcode":"LS1 4AP"}},"tags":["priority","legal"],"total":1042.5},\n  {"id":2,"customer":{"name":"Bob O\'Brien","address":{"city":"Hull","postcode":"HU1 2BB"}},"tags":["standard"],"total":86},\n  {"id":3,"customer":{"name":"Cy Patel","address":{"city":"Manchester","postcode":"M1 3AA"}},"tags":[],"total":12500}\n]',
    'NDJSON': '{"ts":"2026-03-04T09:14:02Z","level":"INFO","job":"nightly-sync","rows":18422}\n{"ts":"2026-03-04T09:15:41Z","level":"WARN","job":"nightly-sync","rows":0}\n{"ts":"2026-03-04T09:16:03Z","level":"ERROR","job":"nightly-sync","rows":0}',
    'XML': '<?xml version="1.0" encoding="UTF-8"?>\n<invoices>\n  <invoice ref="INV-88">\n    <client>Whitfield &amp; Sons</client>\n    <issued>2026-02-28</issued>\n    <net>1042.50</net>\n  </invoice>\n  <invoice ref="INV-89">\n    <client>Redgrave Legal LLP</client>\n    <issued>2026-03-01</issued>\n    <net>12500.00</net>\n  </invoice>\n</invoices>',
    'Pipe': 'EMP_ID|SURNAME|DEPT|START_DT|SALARY\n00417|Whitfield|FINANCE|01/03/2019|48250\n00418|O\'Brien|LEGAL|04/07/2021|61000\n00419|Patel|IT|02/11/2023|55500',
    'SQL dump': "INSERT INTO [dbo].[Customers] ([CustomerID], [CompanyName], [Country], [CreditLimit]) VALUES\n(1, N'Whitfield & Sons, Ltd', N'GB', 50000.00),\n(2, N'O''Brien Logistics', N'IE', 12000.00),\n(3, N'Halcyon Media', N'GB', NULL);",
    'Fixed width': 'ACCT      SURNAME         BRANCH      OPENED      BALANCE\n10044821  WHITFIELD       LEEDS       2019-03-01   1042.50\n10044822  OBRIEN          HULL        2021-07-14     86.00\n10044823  PATEL           MANCHESTER  2023-11-02  12500.00',
    'YAML': '- table: orders\n  source_rows: 18422\n  migrated: true\n  last_run: 2026-03-04\n- table: customers\n  source_rows: 941\n  migrated: false\n  last_run: 2026-03-04\n- table: invoice_lines\n  source_rows: 204118\n  migrated: true\n  last_run: 2026-03-03'
  };

  var TARGET_LABELS = [
    ['sql', 'SQL — DDL + INSERTs'], ['ddl', 'SQL — CREATE TABLE only'], ['csv', 'CSV'], ['tsv', 'TSV'],
    ['json', 'JSON'], ['ndjson', 'NDJSON'], ['xml', 'XML'], ['yaml', 'YAML'],
    ['markdown', 'Markdown table'], ['html', 'HTML table']
  ];

  function out(text, ext, mime) { return { text: text, ext: ext, mime: mime }; }
  var EMITTERS = {
    sql: function (ds, o) { return out(CT.ddl(ds, o) + '\n\n' + CT.sqlInserts(ds, o), 'sql', 'application/sql'); },
    ddl: function (ds, o) { return out(CT.ddl(ds, o), 'sql', 'application/sql'); },
    csv: function (ds) { return out(CT.to.csv(ds), 'csv', 'text/csv'); },
    tsv: function (ds) { return out(CT.to.csv(ds, { delimiter: '\t' }), 'tsv', 'text/tab-separated-values'); },
    json: function (ds, o) { return out(CT.to.json(ds, o), 'json', 'application/json'); },
    ndjson: function (ds, o) { return out(CT.to.ndjson(ds, o), 'ndjson', 'application/x-ndjson'); },
    xml: function (ds) { return out(CT.to.xml(ds), 'xml', 'application/xml'); },
    yaml: function (ds, o) { return out(CT.to.yaml(ds, o), 'yaml', 'application/yaml'); },
    markdown: function (ds) { return out(CT.to.markdown(ds), 'md', 'text/markdown'); },
    html: function (ds) { return out(CT.to.html(ds), 'html', 'text/html'); }
  };

  var stylesInjected = false;
  function injectStyles() {
    if (stylesInjected || typeof document === 'undefined') return;
    var s = document.createElement('style');
    s.setAttribute('data-cygx', '');
    s.textContent = CSS;
    document.head.appendChild(s);
    stylesInjected = true;
  }

  function el(tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text !== undefined) n.textContent = text;
    return n;
  }
  function pct(n) { return Math.round(n * 100) + '%'; }
  function fmtNum(n) { return typeof n === 'number' ? n.toLocaleString() : n; }

  function mount(target, options) {
    injectStyles();
    var host = typeof target === 'string' ? document.querySelector(target) : target;
    if (!host) throw new Error('Mount target not found: ' + target);

    var opts = Object.assign({
      theme: 'dark', target: 'sql', dialect: 'sqlserver', table: 'imported_data',
      samples: true, mapping: true, maxPreviewRows: 50, onResult: null
    }, options || {});

    var state = { ds: null, cols: null, name: 'data' };

    host.className = (host.className ? host.className + ' ' : '') + 'cygx';
    host.setAttribute('data-theme', opts.theme);
    host.innerHTML = '';

    /* ---- header ---- */
    var head = el('div', 'cygx-head');
    head.appendChild(el('span', 'cygx-mark'));
    head.appendChild(el('span', 'cygx-title', 'Data Analyser'));
    head.appendChild(el('span', 'cygx-ver', 'v' + CT.version + ' · runs in your browser'));
    host.appendChild(head);

    /* ---- input ---- */
    var drop = el('div', 'cygx-drop');
    drop.tabIndex = 0;
    drop.setAttribute('role', 'button');
    drop.innerHTML = '<h3>Drop a file, or click to browse</h3>' +
      '<p>CSV · TSV · pipe &amp; fixed-width · JSON · NDJSON · XML · HTML tables · YAML · INI · SQL dumps · Excel</p>';
    var fileInput = el('input');
    fileInput.type = 'file';
    fileInput.style.display = 'none';
    host.appendChild(drop);
    host.appendChild(fileInput);

    var paste = el('textarea', 'cygx-paste');
    paste.placeholder = '…or paste a sample here and it will be identified as you type.';
    paste.setAttribute('aria-label', 'Paste data to identify');
    host.appendChild(paste);

    if (opts.samples) {
      var chips = el('div', 'cygx-chips');
      chips.appendChild(el('span', 'cygx-lab', 'Try:'));
      Object.keys(SAMPLES).forEach(function (k) {
        var c = el('button', 'cygx-chip', k);
        c.type = 'button';
        c.addEventListener('click', function () { paste.value = SAMPLES[k]; state.name = k.toLowerCase().replace(/\s+/g, '_'); run(SAMPLES[k]); });
        chips.appendChild(c);
      });
      host.appendChild(chips);
    }

    var results = el('div', 'cygx-result');
    results.hidden = true;
    host.appendChild(results);

    /* ---- input wiring ---- */
    drop.addEventListener('click', function () { fileInput.click(); });
    drop.addEventListener('keydown', function (e) { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); fileInput.click(); } });
    ['dragenter', 'dragover'].forEach(function (t) {
      drop.addEventListener(t, function (e) { e.preventDefault(); drop.classList.add('is-over'); });
    });
    ['dragleave', 'drop'].forEach(function (t) {
      drop.addEventListener(t, function (e) { e.preventDefault(); drop.classList.remove('is-over'); });
    });
    drop.addEventListener('drop', function (e) { if (e.dataTransfer.files && e.dataTransfer.files[0]) loadFile(e.dataTransfer.files[0]); });
    fileInput.addEventListener('change', function () { if (fileInput.files[0]) loadFile(fileInput.files[0]); });

    var debounce;
    paste.addEventListener('input', function () {
      clearTimeout(debounce);
      debounce = setTimeout(function () {
        if (paste.value.trim() === '') { results.hidden = true; return; }
        state.name = 'pasted_data';
        run(paste.value);
      }, 220);
    });

    function loadFile(file) {
      state.name = (file.name || 'data').replace(/\.[^.]+$/, '');
      CT.util.readFile(file).then(function (r) {
        if (typeof r.data === 'string') paste.value = r.data.slice(0, 200000);
        run(r.data);
      }).catch(function (e) { showError(e.message); });
    }

    function showError(msg) {
      results.hidden = false;
      results.innerHTML = '';
      results.appendChild(el('div', 'cygx-err', msg));
    }

    /* ---- render ---- */
    function run(input) {
      var ds, cols;
      try {
        ds = CT.parse(input, { limit: 20000 });
        cols = CT.profile(ds);
      } catch (e) { showError(e.message); return; }
      state.ds = ds; state.cols = cols;
      render(ds, cols);
      if (opts.onResult) opts.onResult({ dataset: ds, columns: cols });
    }

    function render(ds, cols) {
      results.hidden = false;
      results.innerHTML = '';
      var det = ds.detection || { format: ds.format, confidence: 1, evidence: [] };

      /* verdict */
      var verdict = el('div', 'cygx-verdict');
      var fmt = el('div', 'cygx-fmt');
      fmt.appendChild(document.createTextNode(det.format));
      var sub = el('small');
      sub.textContent = ds.format !== det.format ? 'loaded as ' + ds.format : 'recognised format';
      fmt.appendChild(sub);
      verdict.appendChild(fmt);

      var conf = el('div', 'cygx-conf');
      var clab = el('div', 'cygx-conf-lab');
      clab.appendChild(el('span', null, 'Confidence'));
      clab.appendChild(el('b', null, pct(det.confidence)));
      conf.appendChild(clab);
      var bar = el('div', 'cygx-bar' + (det.confidence < 0.5 ? ' is-bad' : det.confidence < 0.8 ? ' is-warn' : ''));
      var fill = el('span'); fill.style.width = Math.max(3, det.confidence * 100) + '%';
      bar.appendChild(fill); conf.appendChild(bar);
      verdict.appendChild(conf);

      var kv = el('div', 'cygx-kv');
      [[fmtNum(ds.rows.length) + (ds.meta.truncatedFrom ? '+' : ''), 'rows'],
       [fmtNum(cols.length), 'columns'],
       [String(cols.filter(function (c) { return c.issues.length; }).length), 'flagged']].forEach(function (p) {
        var d = el('div'); d.appendChild(el('b', null, p[0])); d.appendChild(el('span', null, p[1])); kv.appendChild(d);
      });
      verdict.appendChild(kv);
      results.appendChild(verdict);

      /* evidence */
      if (det.evidence && det.evidence.length) {
        var why = el('div', 'cygx-why');
        var ul = el('ul');
        det.evidence.forEach(function (e) { ul.appendChild(el('li', null, e)); });
        why.appendChild(ul);
        results.appendChild(why);
      }

      /* warnings + column issues */
      var flags = [];
      (ds.warnings || []).forEach(function (w) { flags.push({ code: w.code, severity: 'medium', message: w.message }); });
      cols.forEach(function (c) {
        c.issues.forEach(function (i) { flags.push({ code: i.code, severity: i.severity, message: c.name + ' — ' + i.message }); });
      });
      if (flags.length) {
        var fw = el('div', 'cygx-flags');
        var rank = { high: 0, medium: 1, low: 2 };
        flags.sort(function (a, b) { return rank[a.severity] - rank[b.severity]; }).slice(0, 12).forEach(function (f) {
          var d = el('div', 'cygx-flag sev-' + f.severity);
          d.appendChild(el('code', null, f.code));
          d.appendChild(el('span', null, f.message));
          fw.appendChild(d);
        });
        results.appendChild(fw);
      }

      /* tabs */
      var tabNames = ['Schema', 'Preview', 'Convert'];
      if (opts.mapping) tabNames.push('Map to target');
      var tabs = el('div', 'cygx-tabs');
      var panes = [];
      tabNames.forEach(function (name, i) {
        var b = el('button', 'cygx-tab', name);
        b.type = 'button';
        b.setAttribute('role', 'tab');
        b.setAttribute('aria-selected', i === 0 ? 'true' : 'false');
        b.addEventListener('click', function () {
          tabs.querySelectorAll('.cygx-tab').forEach(function (t, j) { t.setAttribute('aria-selected', j === i ? 'true' : 'false'); });
          panes.forEach(function (p, j) { p.hidden = j !== i; });
        });
        tabs.appendChild(b);
      });
      results.appendChild(tabs);

      panes.push(schemaPane(cols));
      panes.push(previewPane(ds));
      panes.push(convertPane(ds, cols));
      if (opts.mapping) panes.push(mappingPane(cols));
      panes.forEach(function (p, i) { p.hidden = i !== 0; results.appendChild(p); });
    }

    function schemaPane(cols) {
      var pane = el('div', 'cygx-pane');
      var t = el('table', 'cygx-table');
      t.innerHTML = '<thead><tr><th>Column</th><th>Inferred type</th><th class="num">Match</th>' +
        '<th class="num">Null</th><th class="num">Distinct</th><th class="num">Max len</th>' +
        '<th>Sample</th></tr></thead>';
      var tb = el('tbody');
      cols.forEach(function (c) {
        var tr = el('tr');
        var td1 = el('td');
        var nm = el('span', 'cygx-name', c.name); td1.appendChild(nm);
        if (c.primaryKeyCandidate) td1.appendChild(el('span', 'cygx-pk', 'PK?'));
        if (c.mixedTypes) {
          td1.appendChild(el('span', 'cygx-miniwarn', 'also: ' + c.mixedTypes.slice(1, 3).map(function (m) { return m.type + '×' + m.count; }).join(', ')));
        }
        tr.appendChild(td1);
        var td2 = el('td'); td2.appendChild(el('span', 'cygx-type', c.type)); tr.appendChild(td2);
        tr.appendChild(el('td', 'cygx-num', pct(c.confidence)));
        tr.appendChild(el('td', 'cygx-num', pct(c.nullRate)));
        tr.appendChild(el('td', 'cygx-num', String(c.distinct)));
        tr.appendChild(el('td', 'cygx-num', c.maxLength ? String(c.maxLength) : '—'));
        tr.appendChild(el('td', 'cygx-samp', c.samples.join(' · ')));
        tb.appendChild(tr);
      });
      t.appendChild(tb);
      var wrap = el('div', 'cygx-scroll'); wrap.appendChild(t);
      pane.appendChild(wrap);
      return pane;
    }

    function previewPane(ds) {
      var pane = el('div', 'cygx-pane');
      var t = el('table', 'cygx-table cygx-preview');
      var thead = el('thead'), htr = el('tr');
      ds.columns.forEach(function (c) { htr.appendChild(el('th', null, c)); });
      thead.appendChild(htr); t.appendChild(thead);
      var tb = el('tbody');
      ds.rows.slice(0, opts.maxPreviewRows).forEach(function (r) {
        var tr = el('tr');
        for (var i = 0; i < ds.columns.length; i++) {
          var v = r[i];
          var td = el('td');
          if (v === null || v === undefined || v === '') { td.className = 'cygx-null'; td.textContent = 'null'; }
          else td.textContent = typeof v === 'object' ? JSON.stringify(v) : String(v);
          tr.appendChild(td);
        }
        tb.appendChild(tr);
      });
      t.appendChild(tb);
      var wrap = el('div', 'cygx-scroll'); wrap.appendChild(t);
      pane.appendChild(wrap);
      if (ds.rows.length > opts.maxPreviewRows) {
        pane.appendChild(el('p', 'cygx-hint', 'Showing the first ' + opts.maxPreviewRows + ' of ' + fmtNum(ds.rows.length) + ' rows.'));
      }
      return pane;
    }

    function convertPane(ds, cols) {
      var pane = el('div', 'cygx-pane');
      var ctl = el('div', 'cygx-ctl');

      var tSel = el('select', 'cygx-sel');
      TARGET_LABELS.forEach(function (p) {
        var o = el('option', null, p[1]); o.value = p[0];
        if (p[0] === opts.target) o.selected = true;
        tSel.appendChild(o);
      });
      var dSel = el('select', 'cygx-sel');
      Object.keys(CT.dialects).forEach(function (k) {
        var o = el('option', null, CT.dialects[k].label); o.value = k;
        if (k === opts.dialect) o.selected = true;
        dSel.appendChild(o);
      });
      var tableIn = el('input', 'cygx-in');
      tableIn.value = ds.meta.table || ds.meta.sheet || opts.table;
      tableIn.setAttribute('aria-label', 'Target table name');

      ctl.appendChild(el('label', null, 'Output')); ctl.appendChild(tSel);
      var dWrap = el('span', 'cygx-ctl');
      dWrap.appendChild(el('label', null, 'Dialect')); dWrap.appendChild(dSel);
      dWrap.appendChild(el('label', null, 'Table')); dWrap.appendChild(tableIn);
      ctl.appendChild(dWrap);

      var copyBtn = el('button', 'cygx-btn', 'Copy'); copyBtn.type = 'button';
      var dlBtn = el('button', 'cygx-btn is-primary', 'Download'); dlBtn.type = 'button';
      var sp = el('span', 'cygx-spacer');
      ctl.appendChild(sp); ctl.appendChild(copyBtn); ctl.appendChild(dlBtn);
      pane.appendChild(ctl);

      var out = el('pre', 'cygx-out');
      pane.appendChild(out);

      var current = { text: '', ext: 'txt', mime: 'text/plain' };
      function refresh() {
        var tgt = tSel.value;
        var isSql = tgt === 'sql' || tgt === 'ddl';
        dWrap.style.display = isSql ? 'flex' : 'none';
        // The dataset is already parsed and profiled, so emit from it directly
        // rather than round-tripping through transform().
        var o = { dialect: dSel.value, table: tableIn.value || 'imported_data', columns: cols };
        try {
          current = EMITTERS[tgt](ds, o);
        } catch (e) {
          current = { text: '-- Could not produce ' + tgt + ': ' + e.message, ext: 'txt', mime: 'text/plain' };
        }
        out.textContent = current.text.length > 120000
          ? current.text.slice(0, 120000) + '\n\n… truncated for display — Download gives the full output.'
          : current.text;
      }
      tSel.addEventListener('change', refresh);
      dSel.addEventListener('change', refresh);
      tableIn.addEventListener('input', refresh);
      copyBtn.addEventListener('click', function () {
        navigator.clipboard.writeText(current.text).then(function () {
          copyBtn.textContent = 'Copied';
          setTimeout(function () { copyBtn.textContent = 'Copy'; }, 1400);
        });
      });
      dlBtn.addEventListener('click', function () {
        CT.util.download(current.text, state.name + '.' + current.ext, current.mime);
      });
      refresh();
      return pane;
    }

    function mappingPane(cols) {
      var pane = el('div', 'cygx-pane cygx-map');
      pane.appendChild(el('p', 'cygx-hint',
        'Paste the destination column list — one per line, optionally "name type" — to see which source column each one should take, and how sure that pairing is.'));
      var ta = el('textarea');
      ta.placeholder = 'CustomerID integer\nCompanyName string\nCountryCode string\nCreditLimit decimal';
      ta.setAttribute('aria-label', 'Target schema');
      pane.appendChild(ta);
      var btn = el('button', 'cygx-btn is-primary', 'Match columns'); btn.type = 'button';
      pane.appendChild(btn);
      var outWrap = el('div');
      pane.appendChild(outWrap);

      btn.addEventListener('click', function () {
        var targets = ta.value.split(/\r?\n/).map(function (l) { return l.trim(); }).filter(Boolean).map(function (l) {
          var parts = l.split(/[\s,|]+/);
          return { name: parts[0], type: parts[1] ? parts[1].toLowerCase() : undefined };
        });
        outWrap.innerHTML = '';
        if (!targets.length) { outWrap.appendChild(el('p', 'cygx-empty', 'Add at least one destination column.')); return; }
        var m = CT.map(cols, targets);
        var t = el('table', 'cygx-table');
        t.innerHTML = '<thead><tr><th>Source</th><th></th><th>Target</th><th class="num">Confidence</th><th>Notes</th></tr></thead>';
        var tb = el('tbody');
        m.mappings.forEach(function (x) {
          var tr = el('tr');
          tr.appendChild(el('td', 'cygx-name', x.source));
          tr.appendChild(el('td', 'cygx-arrow', '→'));
          tr.appendChild(el('td', 'cygx-name', x.target));
          var c = el('td', 'cygx-num' + (x.review ? ' cygx-review' : ''), pct(x.confidence));
          tr.appendChild(c);
          tr.appendChild(el('td', 'cygx-note', x.notes.length ? x.notes.join('; ') : 'exact match'));
          tb.appendChild(tr);
        });
        t.appendChild(tb);
        var sc = el('div', 'cygx-scroll'); sc.appendChild(t);
        outWrap.appendChild(sc);

        if (m.unmappedSource.length || m.unmappedTarget.length) {
          var u = el('div', 'cygx-unmapped');
          if (m.unmappedTarget.length) {
            var d1 = el('div'); d1.appendChild(el('b', null, 'No source for: '));
            m.unmappedTarget.forEach(function (n) { d1.appendChild(el('code', null, n)); });
            u.appendChild(d1);
          }
          if (m.unmappedSource.length) {
            var d2 = el('div'); d2.appendChild(el('b', null, 'Nowhere to put: '));
            m.unmappedSource.forEach(function (n) { d2.appendChild(el('code', null, n)); });
            u.appendChild(d2);
          }
          outWrap.appendChild(u);
        }
      });
      return pane;
    }

    return {
      load: function (input, name) { if (name) state.name = name; if (typeof input === 'string') paste.value = input; run(input); },
      get result() { return state.ds ? { dataset: state.ds, columns: state.cols } : null; },
      destroy: function () { host.innerHTML = ''; host.classList.remove('cygx'); }
    };
  }

  return { mount: mount, samples: SAMPLES, version: CT.version };
}));
