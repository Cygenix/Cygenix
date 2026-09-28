/* cygenix-template-suggest.js — AI "Suggest tables" for Conversion Templates.
 *
 * WHAT IT DOES
 * Proposes which TARGET tables belong to each in-scope Configurator module,
 * as a preview the person reviews and ticks. It never removes or replaces a
 * table: suggestions are only ever added, and only the ones ticked.
 *
 * HOW A RUN GOES
 *   1. The page's own schema reader (CygenixSchemaGraph — the same reader
 *      Add table uses, so the same connection, and a customer Function URL
 *      never goes near Netlify) supplies every target table name, row count
 *      and foreign key.
 *   2. SHORTLIST: the names go to agent/template-suggest/shortlist in chunks
 *      of at most 1,500, a few modules per call, so every call fits the data
 *      proxy's 26-second limit. Up to 40 candidates come back per module.
 *   3. RANK: once per module, at most three at a time, with the candidates'
 *      columns (read through the same reader) and their FKs in both
 *      directions. Each table comes back high / medium / low with a reason.
 *      A module that fails is reported in the preview; the rest carry on.
 *   4. The preview marks what the module already has as "already added" and
 *      flags a table suggested for, or already in, more than one module as
 *      Shared. High is ticked by default; medium and low are not.
 *   5. APPLY adds the ticked tables through the model's own add path — the
 *      staging name is derived exactly as for a manual add — then fills the
 *      load order from the foreign keys.
 *   Cancel aborts the calls in flight, starts no more, and keeps nothing.
 *
 * LOAD ORDER, WITHOUT AI
 * Parents before children, from the FK graph over every target table in the
 * template (all modules), with gaps of ten so a person can slot rows in. A
 * cycle's tables share one number and say so in their notes. Where a number
 * came from is recorded as loadOrderSource:
 *   'user' — typed, or set by moving a row: NEVER overwritten
 *   'ai'   — set here
 *   'auto' — the placeholder the model assigns on any add (row count + 1),
 *            which nobody chose
 * Apply fills rows with no number or an 'auto' one. "Recalculate load order"
 * also overwrites 'ai' ones. A row from before this existed carries no
 * source; its number may have been typed, so it is left alone unless empty.
 *
 * TARGET-AGNOSTIC: no table names, keywords or module→table maps live here.
 *
 * Node-requirable: the merging, flagging and load-order logic and the
 * orchestration (with the network injected) are tested in
 * tests/template-suggest.test.js.
 */
(function (root, factory) {
  'use strict';
  var api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root && root.document) root.CygenixTemplateSuggest = api;
})(typeof window !== 'undefined' ? window : this, function (root) {
  'use strict';

  var CHUNK = 1500, MODULE_BATCH = 8, PER_MODULE = 40, CONCURRENCY = 3, COL_CONCURRENCY = 6;
  var STEP = 10;
  var CONF_ORDER = { high: 0, medium: 1, low: 2 };
  var lc = function (s) { return String(s == null ? '' : s).trim().toLowerCase(); };

  /* ── Small concurrency pool with cancel ─────────────────────────────── */
  function pool(items, limit, worker, signal) {
    var i = 0, active = 0, results = new Array(items.length);
    return new Promise(function (resolve) {
      function next() {
        if ((signal && signal.aborted) || (i >= items.length && active === 0)) {
          if (active === 0) resolve(results);
          return;
        }
        while (active < limit && i < items.length && !(signal && signal.aborted)) {
          (function (k) {
            active++;
            Promise.resolve().then(function () { return worker(items[k], k); })
              .then(function (v) { results[k] = { ok: true, value: v }; },
                    function (e) { results[k] = { ok: false, error: e }; })
              .then(function () { active--; next(); });
          })(i++);
        }
        if (active === 0) resolve(results);
      }
      next();
    });
  }

  /* ── Graph helpers ─────────────────────────────────────────────────────
     The graph is CygenixSchemaGraph's: tables {schema, name, key, rowCount},
     edges {from, to, fromColumn, toColumn, self} keyed "schema.name". The
     template names tables by bare name (what the picker adds), so FKs are
     translated to names here. */
  function edgesByName(graph) {
    var byKey = {};
    (graph && graph.tables || []).forEach(function (t) { byKey[t.key] = t.name; });
    var out = [];
    (graph && graph.edges || []).forEach(function (e) {
      var child = byKey[e.from], parent = byKey[e.to];
      if (!child || !parent || e.self || lc(child) === lc(parent)) return;
      out.push({ child: child, childColumn: e.fromColumn || '', parent: parent, parentColumn: e.toColumn || '' });
    });
    return out;
  }

  /* ── Load order: Tarjan's SCCs, then Kahn over the condensation ─────── */
  function computeLoadOrder(names, fks) {
    var nodes = [], seen = {};
    (names || []).forEach(function (n) { var k = lc(n); if (k && !seen[k]) { seen[k] = n; nodes.push(k); } });
    var adj = {};                            // parent → children (parents load first)
    nodes.forEach(function (k) { adj[k] = []; });
    (fks || []).forEach(function (f) {
      var c = lc(f.child), p = lc(f.parent);
      if (!adj[c] || !adj[p] || c === p) return;
      if (adj[p].indexOf(c) < 0) adj[p].push(c);
    });
    nodes.sort();
    // Tarjan, iterative.
    var index = 0, idx = {}, low = {}, onStack = {}, stack = [], comps = [];
    nodes.forEach(function (start) {
      if (idx[start] != null) return;
      var work = [[start, 0]];
      idx[start] = low[start] = index++; stack.push(start); onStack[start] = true;
      while (work.length) {
        var top = work[work.length - 1], v = top[0], kids = adj[v].slice().sort();
        if (top[1] < kids.length) {
          var w = kids[top[1]++];
          if (idx[w] == null) {
            idx[w] = low[w] = index++; stack.push(w); onStack[w] = true;
            work.push([w, 0]);
          } else if (onStack[w]) low[v] = Math.min(low[v], idx[w]);
        } else {
          work.pop();
          if (work.length) { var u = work[work.length - 1][0]; low[u] = Math.min(low[u], low[v]); }
          if (low[v] === idx[v]) {
            var comp = [], x;
            do { x = stack.pop(); onStack[x] = false; comp.push(x); } while (x !== v);
            comps.push(comp.sort());
          }
        }
      }
    });
    var compOf = {};
    comps.forEach(function (c, i) { c.forEach(function (k) { compOf[k] = i; }); });
    var indeg = comps.map(function () { return 0; }), out = comps.map(function () { return []; });
    nodes.forEach(function (p) { adj[p].forEach(function (c) {
      var a = compOf[p], b = compOf[c];
      if (a !== b && out[a].indexOf(b) < 0) { out[a].push(b); indeg[b]++; }
    }); });
    var ready = [];
    comps.forEach(function (c, i) { if (!indeg[i]) ready.push(i); });
    var byName = function (a, b) { return comps[a][0] < comps[b][0] ? -1 : comps[a][0] > comps[b][0] ? 1 : 0; };
    var order = {}, cycles = {}, n = 0;
    while (ready.length) {
      ready.sort(byName);
      var i = ready.shift();
      n++;
      comps[i].forEach(function (k) {
        order[k] = n * STEP;
        if (comps[i].length > 1) cycles[k] = comps[i].filter(function (o) { return o !== k; }).map(function (o) { return seen[o]; });
      });
      out[i].forEach(function (j) { if (--indeg[j] === 0) ready.push(j); });
    }
    return { order: order, cycles: cycles };
  }

  function templateTableNames(tpl) {
    var names = [];
    (tpl && tpl.modules || []).forEach(function (m) { (m.tables || []).forEach(function (t) { if (t.targetTable) names.push(t.targetTable); }); });
    return names;
  }

  // mode 'apply': fill rows with no number, or a placeholder nobody chose.
  // mode 'recalc': also replace numbers this feature set before.
  // A 'user' number is never touched, and neither is a legacy row's number.
  function fillable(t, mode) {
    if (!Number(t.loadOrder)) return true;
    if (t.loadOrderSource === 'auto') return true;
    return mode === 'recalc' && t.loadOrderSource === 'ai';
  }
  function applyLoadOrder(tpl, fks, mode) {
    var lo = computeLoadOrder(templateTableNames(tpl), fks);
    var set = 0;
    (tpl && tpl.modules || []).forEach(function (m) {
      (m.tables || []).forEach(function (t) {
        var k = lc(t.targetTable);
        if (!lo.order[k] || !fillable(t, mode)) return;
        t.loadOrder = lo.order[k];
        t.loadOrderSource = 'ai';
        var cyc = lo.cycles[k];
        if (cyc && cyc.length && String(t.notes || '').indexOf('FK cycle with') < 0) {
          t.notes = (t.notes ? t.notes + ' · ' : '') + 'FK cycle with ' + cyc.join(', ') + ' — review load order';
        }
        set++;
      });
    });
    return { set: set, cycles: Object.keys(lo.cycles).length };
  }

  /* ── Shared: which modules each table is in (or suggested for) ───────── */
  function membership(tpl, extra) {
    var map = {};
    var add = function (table, mod) {
      var k = lc(table); if (!k) return;
      (map[k] = map[k] || []);
      if (map[k].indexOf(mod) < 0) map[k].push(mod);
    };
    (tpl && tpl.modules || []).forEach(function (m) { (m.tables || []).forEach(function (t) { add(t.targetTable, m.module); }); });
    (extra || []).forEach(function (e) { add(e.table, e.module); });
    return map;
  }
  function sharedWith(map, table, mod) {
    return (map[lc(table)] || []).filter(function (x) { return x !== mod; });
  }

  /* ── Preview ─────────────────────────────────────────────────────────── */
  function buildPreview(tpl, modules, ranks) {
    var groups = modules.map(function (m, i) {
      var r = ranks[i] || {};
      var mod = (tpl.modules || []).filter(function (x) { return x.module === m.name; })[0] || { tables: [] };
      var have = {};
      (mod.tables || []).forEach(function (t) { have[lc(t.targetTable)] = true; });
      var rows = (r.ranked || []).slice().sort(function (a, b) {
        return (CONF_ORDER[a.confidence] - CONF_ORDER[b.confidence]) || String(a.table).localeCompare(String(b.table));
      }).map(function (x) {
        var already = !!have[lc(x.table)];
        return { table: x.table, confidence: x.confidence, reason: x.reason || '',
          already: already, checked: !already && x.confidence === 'high', shared: [] };
      });
      return { module: m.name, key: m.key, error: r.error || null, rows: rows };
    });
    var extra = [];
    groups.forEach(function (g) { g.rows.forEach(function (r) { if (!r.already) extra.push({ table: r.table, module: g.module }); }); });
    var map = membership(tpl, extra);
    groups.forEach(function (g) { g.rows.forEach(function (r) { r.shared = sharedWith(map, r.table, g.module); }); });
    return { groups: groups };
  }

  /* ── Apply ───────────────────────────────────────────────────────────── */
  function apply(tpl, preview, TM, who, fks) {
    var added = 0, modulesTouched = 0, addedNames = {};
    preview.groups.forEach(function (g) {
      var n = 0;
      g.rows.forEach(function (r) {
        if (!r.checked || r.already) return;
        // The model's own add: the staging name, prefix and duplicate check
        // are exactly those of a manual add.
        var t = TM.tmAddTable(tpl, g.module, { targetTable: r.table }, who);
        if (!t) return;
        t.source = 'ai';
        t.aiConfidence = r.confidence;
        t.aiReason = r.reason;
        n++; added++; addedNames[lc(r.table)] = true;
      });
      if (n) modulesTouched++;
    });
    var lo = added ? applyLoadOrder(tpl, fks, 'apply') : { set: 0, cycles: 0 };
    var map = membership(tpl);
    var shared = Object.keys(addedNames).filter(function (k) { return (map[k] || []).length > 1; }).length;
    return { added: added, modules: modulesTouched, shared: shared, ordered: lo.set, cycles: lo.cycles };
  }

  /* ── Transport: through the data proxy, as the Agentive page does ────── */
  var PROXY = '/.netlify/functions/data-proxy';
  function httpPost(path, body, signal) {
    var headers = { 'Content-Type': 'application/json' };
    try {
      var tok = typeof root.getCygenixIdToken === 'function' ? root.getCygenixIdToken() : '';
      if (tok) headers.Authorization = 'Bearer ' + tok;
      if (root.CygenixModel && root.CygenixModel.userKeyHeader) Object.assign(headers, root.CygenixModel.userKeyHeader());
    } catch (e) { /* the server answers with what is missing */ }
    return fetch(PROXY + '?path=' + encodeURIComponent(path), {
      method: 'POST', headers: headers, body: JSON.stringify(body), signal: signal,
    }).then(function (res) {
      return res.text().then(function (text) {
        var data = null;
        try { data = JSON.parse(text); } catch (e) { data = null; }
        if (!res.ok || !data) {
          var msg = (data && (data.error || data.detail)) || ('HTTP ' + res.status);
          var err = new Error(String(msg).slice(0, 300));
          err.status = res.status;
          throw err;
        }
        return data;
      });
    });
  }

  /* ── The run ─────────────────────────────────────────────────────────── */
  // opts: { tpl, modules:[{key,name,notes}], graph, columnsOf(table)→Promise<cols>,
  //         post(path, body, signal), onProgress(text, done, total), signal }
  // Resolves { preview, fks, tableCount } or { cancelled:true }. Throws only
  // when nothing at all could be done (schema empty, every shortlist failed).
  function run(opts) {
    var signal = opts.signal;
    var post = opts.post || httpPost;
    var progress = opts.onProgress || function () {};
    var modules = opts.modules || [];
    var graph = opts.graph || { tables: [], edges: [] };
    var cancelled = function () { return !!(signal && signal.aborted); };

    var seenName = {}, tables = [];
    (graph.tables || []).forEach(function (t) {
      var k = lc(t.name);
      if (!k || seenName[k]) return;
      seenName[k] = t;
      tables.push({ name: t.name, rows: typeof t.rowCount === 'number' ? t.rowCount : null });
    });
    if (!tables.length) return Promise.reject(new Error('The target database has no tables to suggest from.'));
    var fks = edgesByName(graph);

    // Shortlist calls: every chunk × every module batch.
    var chunks = [], batches = [], calls = [];
    for (var i = 0; i < tables.length; i += CHUNK) chunks.push(tables.slice(i, i + CHUNK));
    for (var j = 0; j < modules.length; j += MODULE_BATCH) batches.push(modules.slice(j, j + MODULE_BATCH));
    chunks.forEach(function (c) { batches.forEach(function (b) { calls.push({ tables: c, modules: b }); }); });
    var shortlist = {}, slDone = 0, slErrors = [];
    modules.forEach(function (m) { shortlist[m.key] = []; });
    progress('Shortlisting…', 0, calls.length);

    return pool(calls, CONCURRENCY, function (c) {
      return post('/agent/template-suggest/shortlist', { modules: c.modules, tables: c.tables }, signal).then(function (d) {
        var sl = (d && d.shortlist) || {};
        c.modules.forEach(function (m) {
          (sl[m.key] || []).forEach(function (n) {
            if (shortlist[m.key].length < PER_MODULE && shortlist[m.key].indexOf(n) < 0) shortlist[m.key].push(n);
          });
        });
        slDone++;
        progress('Shortlisting… (' + slDone + '/' + calls.length + ')', slDone, calls.length);
      });
    }, signal).then(function (res) {
      if (cancelled()) return { cancelled: true };
      res.forEach(function (r) { if (r && !r.ok) slErrors.push(r.error); });
      if (slErrors.length === calls.length) throw slErrors[0] || new Error('The shortlist failed.');

      // Rank, three modules at a time.
      var done = 0;
      var colCache = {};
      var columnsFor = function (name) {
        var k = lc(name);
        if (!colCache[k]) colCache[k] = Promise.resolve(opts.columnsOf ? opts.columnsOf(seenName[k] || { name: name }) : [])
          .catch(function () { return []; });
        return colCache[k];
      };
      progress('Ranking… (0/' + modules.length + ')', 0, modules.length);
      return pool(modules, CONCURRENCY, function (m) {
        var cands = shortlist[m.key] || [];
        if (!cands.length) {
          done++;
          progress('Ranked ' + m.name + ' (' + done + '/' + modules.length + ')', done, modules.length);
          return { ranked: [], fks: [] };
        }
        progress('Ranking ' + m.name + ' (' + (done + 1) + '/' + modules.length + ')…', done, modules.length);
        return pool(cands, COL_CONCURRENCY, columnsFor, signal).then(function (cols) {
          if (cancelled()) return { ranked: [] };
          var candSet = {};
          cands.forEach(function (c) { candSet[lc(c)] = true; });
          var body = { module: m, candidates: cands.map(function (c, k) {
            var t = seenName[lc(c)] || {};
            return {
              name: c,
              rows: typeof t.rowCount === 'number' ? t.rowCount : null,
              columns: ((cols[k] && cols[k].ok && cols[k].value) || []).map(function (x) {
                return { name: x.name, type: x.dataType || x.type || '' };
              }),
              fks: fks.filter(function (f) { return lc(f.child) === lc(c) || lc(f.parent) === lc(c); }),
            };
          }) };
          return post('/agent/template-suggest/rank', body, signal);
        }).then(function (d) {
          done++;
          progress('Ranked ' + m.name + ' (' + done + '/' + modules.length + ')', done, modules.length);
          return d;
        });
      }, signal).then(function (ranks) {
        if (cancelled()) return { cancelled: true };
        var shaped = ranks.map(function (r) {
          if (!r) return { ranked: [], error: 'Not run' };
          if (!r.ok) return { ranked: [], error: (r.error && r.error.message) || 'Failed' };
          return { ranked: (r.value && r.value.ranked) || [] };
        });
        return { preview: buildPreview(opts.tpl, modules, shaped), fks: fks, tableCount: tables.length,
          shortlistErrors: slErrors.length };
      });
    });
  }

  return {
    run: run, buildPreview: buildPreview, apply: apply, applyLoadOrder: applyLoadOrder,
    computeLoadOrder: computeLoadOrder, membership: membership, sharedWith: sharedWith,
    edgesByName: edgesByName, templateTableNames: templateTableNames, fillable: fillable,
    httpPost: httpPost, pool: pool,
    LIMITS: { CHUNK: CHUNK, MODULE_BATCH: MODULE_BATCH, PER_MODULE: PER_MODULE, CONCURRENCY: CONCURRENCY, STEP: STEP },
  };
});
