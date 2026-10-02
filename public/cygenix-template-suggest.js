/* cygenix-template-suggest.js — AI "Suggest all" / "Suggest" for Conversion Templates.
 *
 * WHAT IT DOES
 * Sorts the TARGET database's tables into the template's modules (business
 * subject areas), as a preview the person reviews and ticks. It never removes
 * or replaces a table: suggestions are only ever added, and only the ones
 * ticked. "Suggest all" runs for every ticked (Include) module; the module
 * panel's "Suggest" is the same run with one module.
 *
 * HOW A RUN GOES (Oct-2026)
 *   1. The page's own schema reader (CygenixSchemaGraph — the same reader Add
 *      table uses, so the same connection, and a customer Function URL never
 *      goes near Netlify) supplies every target table, its row count and
 *      every foreign key; then the columns and primary key of EVERY table,
 *      six at a time, cached by the reader.
 *   2. The tables are cut into chunks of 40, alphabetically (so tables that
 *      share a prefix tend to land together), each table carrying its
 *      columns, key, and its FK neighbours in both directions.
 *   3. The chunks go to agent/table-classify/start, which submits them to
 *      Anthropic's Message Batches API, every chunk with the FULL list of
 *      modules — so each table is judged against every subject area at once.
 *      A large database is sent in several parts, each a batch of its own.
 *   4. The page polls agent/table-classify/status every 15 seconds and
 *      collects each batch's answers as it finishes (agent/table-classify/
 *      results). Nothing waits on Claude inside a request, so no call comes
 *      near the 26 seconds the data proxy allows.
 *   5. The preview groups the proposals by module — "already added" where the
 *      module has the table, Shared where it is proposed for, or already in,
 *      another module — and lists the tables that fit no module, and any
 *      chunk that failed, with a retry for those tables only. High and
 *      medium are ticked by default; low is not.
 *   6. APPLY adds the ticked tables through the model's own add path — the
 *      staging name is derived exactly as for a manual add — with Claude's
 *      required yes/no, "Also in: …" in the notes of a shared table, then
 *      recalculates the load order from the foreign keys.
 *   Cancel stops polling, cancels the batches still running, and keeps
 *   nothing.
 *
 * The prompt lives in azure-function/src/table-classifier-prompt.js and the
 * checking of Claude's answer in table-classifier.js; this file never decides
 * which module a table is in, it only carries the question and the answer.
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
 * "Recalculate load order" (and Apply, which runs the same thing) fills rows
 * with no number, an 'auto' one or an 'ai' one. A row from before this
 * existed carries no source; its number may have been typed, so it is left
 * alone unless empty.
 *
 * TARGET-AGNOSTIC: no table names, keywords or module→table maps live here.
 *
 * Node-requirable: the chunking, merging, flagging and load-order logic and
 * the orchestration (with the network injected) are tested in
 * tests/template-suggest.test.js.
 */
(function (root, factory) {
  'use strict';
  var api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root && root.document) root.CygenixTemplateSuggest = api;
})(typeof window !== 'undefined' ? window : this, function (root) {
  'use strict';

  var CHUNK_TABLES = 40;          // matches TABLES_PER_CHUNK in table-classifier-prompt.js
  var CHUNK_CHARS = 60000;        // a very wide table set gets smaller chunks
  var PART_CHUNKS = 200;          // chunks per start call (the backend allows 250)
  var PART_CHARS = 900000;        // and well under Netlify's 6 MB body limit
  var COL_CONCURRENCY = 6;
  var POLL_MS = 15000;
  var STATUS_IDS = 50;
  var MAX_POLL_FAILURES = 5;
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

  /* ── Shared: which modules each table is in (or proposed for) ────────── */
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

  /* ── Step 1–2: read the schema and cut it into chunks ─────────────────── */
  // opts: { graph, columnsOf(table)→Promise<{columns, primaryKeys} | columns[]>,
  //         onlyTables?: [names], idPrefix?, onProgress, signal }
  // Resolves { tables, chunks, fks } or { cancelled: true }.
  function prepare(opts) {
    var progress = opts.onProgress || function () {};
    var signal = opts.signal;
    var graph = opts.graph || { tables: [], edges: [] };
    var only = null;
    if (opts.onlyTables) { only = {}; opts.onlyTables.forEach(function (n) { only[lc(n)] = true; }); }
    var seen = {}, tables = [];
    (graph.tables || []).forEach(function (t) {
      var k = lc(t.name);
      if (!k || seen[k] || (only && !only[k])) return;
      seen[k] = true;
      tables.push(t);
    });
    if (!tables.length) return Promise.reject(new Error('The target database has no tables to suggest from.'));
    tables.sort(function (a, b) { return lc(a.name) < lc(b.name) ? -1 : lc(a.name) > lc(b.name) ? 1 : 0; });
    var fks = edgesByName(graph);
    var refs = {}, refBy = {};
    fks.forEach(function (f) {
      var c = lc(f.child), p = lc(f.parent);
      (refs[c] = refs[c] || []).indexOf(f.parent) < 0 && refs[c].push(f.parent);
      (refBy[p] = refBy[p] || []).indexOf(f.child) < 0 && refBy[p].push(f.child);
    });

    var done = 0;
    progress('Reading columns (0 of ' + tables.length + ' tables)…', 0, tables.length);
    return pool(tables, COL_CONCURRENCY, function (t) {
      return Promise.resolve(opts.columnsOf ? opts.columnsOf(t) : null).then(function (node) {
        done++;
        if (done % 10 === 0 || done === tables.length) progress('Reading columns (' + done + ' of ' + tables.length + ' tables)…', done, tables.length);
        return node;
      });
    }, signal).then(function (cols) {
      if (signal && signal.aborted) return { cancelled: true };
      var detailed = tables.map(function (t, i) {
        var r = cols[i], node = r && r.ok ? r.value : null;
        var list = Array.isArray(node) ? node : (node && node.columns) || null;
        var pk = node && !Array.isArray(node) && Array.isArray(node.primaryKeys) ? node.primaryKeys : [];
        return {
          name: t.name,
          rows: typeof t.rowCount === 'number' ? t.rowCount : null,
          columns: (list || []).map(function (c) { return { name: c.name, type: c.dataType || c.type || '' }; }),
          columnsRead: !!list,
          pk: pk.map(function (p) { return typeof p === 'string' ? p : (p && p.name) || ''; }).filter(Boolean),
          refs: refs[lc(t.name)] || [],
          refBy: refBy[lc(t.name)] || [],
        };
      });
      return { tables: detailed, chunks: makeChunks(detailed, opts.idPrefix || 'c'), fks: fks };
    });
  }

  function sizeOf(t) { return JSON.stringify(t).length; }
  function makeChunks(tables, prefix) {
    var chunks = [], cur = [], chars = 0;
    tables.forEach(function (t) {
      var n = sizeOf(t);
      if (cur.length && (cur.length >= CHUNK_TABLES || chars + n > CHUNK_CHARS)) {
        chunks.push(cur); cur = []; chars = 0;
      }
      cur.push(t); chars += n;
    });
    if (cur.length) chunks.push(cur);
    return chunks.map(function (c, i) { return { id: prefix + (i + 1), tables: c }; });
  }

  // Several start calls for a big database, each comfortably inside the
  // proxy's body limit and the backend's chunk limit.
  function parts(chunks) {
    var out = [], cur = [], chars = 0;
    chunks.forEach(function (c) {
      var n = JSON.stringify(c).length;
      if (cur.length && (cur.length >= PART_CHUNKS || chars + n > PART_CHARS)) { out.push(cur); cur = []; chars = 0; }
      cur.push(c); chars += n;
    });
    if (cur.length) out.push(cur);
    return out;
  }

  function modulesForWire(modules) {
    return (modules || []).map(function (m) { return { name: m.name, notes: m.notes || '' }; });
  }

  /* ── Step 3: submit ───────────────────────────────────────────────────── */
  // Resolves the run state — everything needed to collect the answers later,
  // including after a page reload: batch ids and the table NAMES per chunk.
  function submit(prep, opts) {
    var post = opts.post || httpPost;
    var progress = opts.onProgress || function () {};
    var signal = opts.signal;
    var list = parts(prep.chunks);
    var state = { v: 1, modules: modulesForWire(opts.modules), batches: [], tableCount: prep.tables.length, startedAt: Date.now() };
    var k = 0;
    function nextPart() {
      if (signal && signal.aborted) return Promise.resolve(state);
      if (k >= list.length) return Promise.resolve(state);
      var part = list[k++];
      progress('Sending to Claude (part ' + k + ' of ' + list.length + ')…', k - 1, list.length);
      return post('/agent/table-classify/start', { modules: state.modules, chunks: part }, signal).then(function (d) {
        state.batches.push({ id: d.batchId, fetched: false,
          chunks: part.map(function (c) { return { id: c.id, tables: c.tables.map(function (t) { return t.name; }) }; }) });
        return nextPart();
      });
    }
    return nextPart();
  }

  /* ── Step 4: poll and collect ─────────────────────────────────────────── */
  function defaultSleep(ms, signal) {
    return new Promise(function (resolve) {
      var t = setTimeout(resolve, ms);
      if (signal) signal.addEventListener('abort', function () { clearTimeout(t); resolve(); }, { once: true });
    });
  }
  function chunkTotal(state) { return state.batches.reduce(function (n, b) { return n + b.chunks.length; }, 0); }

  // Resolves { outcomes: [chunk outcome…], state } or { cancelled: true }.
  // A chunk outcome: { id, ok, rows?, unanswered?, error?, tables:[names] }.
  function collect(state, opts) {
    var post = opts.post || httpPost;
    var progress = opts.onProgress || function () {};
    var signal = opts.signal;
    var sleep = opts.sleep || defaultSleep;
    var pollMs = opts.pollMs == null ? POLL_MS : opts.pollMs;
    var outcomes = opts.outcomes || [];
    var total = chunkTotal(state);
    var failures = 0;
    var names = (state.modules || []).map(function (m) { return m.name; });

    function cancelRest() {
      var ids = state.batches.filter(function (b) { return !b.fetched; }).map(function (b) { return b.id; });
      if (ids.length) post('/agent/table-classify/cancel', { batchIds: ids }).catch(function () {});
      return { cancelled: true };
    }
    function fetchBatch(b) {
      return post('/agent/table-classify/results', { batchId: b.id, modules: names, chunks: b.chunks }, signal).then(function (d) {
        var byId = {};
        b.chunks.forEach(function (c) { byId[c.id] = c; });
        (d && d.chunks || []).forEach(function (o) {
          if (!byId[o.id]) return;
          outcomes.push(Object.assign({}, o, { tables: byId[o.id].tables }));
          delete byId[o.id];
        });
        Object.keys(byId).forEach(function (id) { outcomes.push({ id: id, ok: false, error: 'No result came back', tables: byId[id].tables }); });
        b.fetched = true;
      });
    }
    function round() {
      if (signal && signal.aborted) return Promise.resolve(cancelRest());
      var open = state.batches.filter(function (b) { return !b.fetched; });
      if (!open.length) return Promise.resolve({ outcomes: outcomes, state: state });
      var ids = open.map(function (b) { return b.id; }).slice(0, STATUS_IDS);
      return post('/agent/table-classify/status', { batchIds: ids }, signal).then(function (d) {
        var byId = {};
        (d && d.batches || []).forEach(function (x) { byId[x.id] = x; });
        var settled = outcomes.length;
        open.forEach(function (b) {
          var x = byId[b.id]; if (!x || !x.counts) return;
          settled += x.counts.succeeded + x.counts.errored + x.counts.canceled + x.counts.expired;
        });
        progress('Classifying: ' + Math.min(settled, total) + ' of ' + total + ' chunk' + (total === 1 ? '' : 's') + ' done…', Math.min(settled, total), total);
        var ended = open.filter(function (b) { return byId[b.id] && byId[b.id].status === 'ended'; });
        return ended.reduce(function (p, b) { return p.then(function () { return signal && signal.aborted ? null : fetchBatch(b); }); }, Promise.resolve());
      }).then(function () { failures = 0; }, function (e) {
        // A wrong key, a missing batch or a refused request will not get
        // better by asking again every 15 seconds: stop at once.
        if (e && (e.status === 400 || e.status === 401 || e.status === 403 || e.status === 404)) throw e;
        if (++failures >= MAX_POLL_FAILURES) throw e;
      }).then(function () {
        if (signal && signal.aborted) return cancelRest();
        if (!state.batches.some(function (b) { return !b.fetched; })) return { outcomes: outcomes, state: state };
        return sleep(pollMs, signal).then(round);
      });
    }
    progress('Classifying: 0 of ' + total + ' chunk' + (total === 1 ? '' : 's') + ' done…', 0, total);
    return round();
  }

  /* ── Step 5: the preview ──────────────────────────────────────────────── */
  // modules: [{key, name}] in the order the page lists them.
  // outcomes: chunk outcomes from collect (several runs' worth after a retry).
  // keep: optional { 'module|table': checked } from an earlier preview, so a
  //       retry does not undo the ticks the person already changed.
  function buildPreview(tpl, modules, outcomes, keep) {
    var byTable = {}, unassigned = [], failed = [];
    (outcomes || []).forEach(function (o) {
      if (!o.ok) { failed.push({ id: o.id, error: o.error || 'Failed', tables: (o.tables || []).slice() }); return; }
      (o.rows || []).forEach(function (r) { byTable[lc(r.table)] = r; });
      if (o.unanswered && o.unanswered.length) failed.push({ id: o.id + '-left-out', error: 'Claude left these tables out of its answer', tables: o.unanswered.slice() });
    });
    // A table that came back in a later (retried) chunk is no longer failed.
    failed = failed.map(function (f) {
      return Object.assign({}, f, { tables: f.tables.filter(function (t) { return !byTable[lc(t)]; }) });
    }).filter(function (f) { return f.tables.length; });

    var runMods = {};
    modules.forEach(function (m) { runMods[m.name] = true; });
    var proposals = [];                     // { table, module, row }
    Object.keys(byTable).sort().forEach(function (k) {
      var r = byTable[k];
      var mods = (r.modules || []).filter(function (m) { return runMods[m]; });
      if (!mods.length) { unassigned.push({ table: r.table, reason: r.reason || '' }); return; }
      mods.forEach(function (m) { proposals.push({ table: r.table, module: m, row: r }); });
    });

    var map = membership(tpl, proposals.map(function (p) { return { table: p.table, module: p.module }; }));
    var groups = modules.map(function (m) {
      var mod = (tpl.modules || []).filter(function (x) { return x.module === m.name; })[0] || { tables: [] };
      var have = {};
      (mod.tables || []).forEach(function (t) { have[lc(t.targetTable)] = true; });
      var rows = proposals.filter(function (p) { return p.module === m.name; }).map(function (p) {
        var already = !!have[lc(p.table)];
        var key = m.name + '|' + lc(p.table);
        var dflt = !already && (p.row.confidence === 'high' || p.row.confidence === 'medium');
        return { table: p.table, confidence: p.row.confidence, required: p.row.required !== false, reason: p.row.reason || '',
          already: already, checked: already ? false : (keep && key in keep ? !!keep[key] : dflt),
          shared: sharedWith(map, p.table, m.name) };
      }).sort(function (a, b) {
        return (CONF_ORDER[a.confidence] - CONF_ORDER[b.confidence]) || String(a.table).localeCompare(String(b.table));
      });
      return { module: m.name, key: m.key, rows: rows };
    });

    var fresh = {}, freshShared = {}, modsWith = 0;
    groups.forEach(function (g) {
      var any = false;
      g.rows.forEach(function (r) {
        if (r.already) return;
        any = true; fresh[lc(r.table)] = true;
        if (r.shared.length) freshShared[lc(r.table)] = true;
      });
      if (any) modsWith++;
    });
    return {
      groups: groups,
      unassigned: unassigned,
      failed: failed,
      summary: { tables: Object.keys(fresh).length, modules: modsWith, shared: Object.keys(freshShared).length,
        unassigned: unassigned.length, failedTables: failed.reduce(function (n, f) { return n + f.tables.length; }, 0) },
    };
  }

  // The ticks in a preview, keyed so buildPreview can carry them over.
  function ticksOf(preview) {
    var keep = {};
    (preview && preview.groups || []).forEach(function (g) {
      g.rows.forEach(function (r) { if (!r.already) keep[g.module + '|' + lc(r.table)] = !!r.checked; });
    });
    return keep;
  }

  /* ── Step 6: apply ────────────────────────────────────────────────────── */
  function apply(tpl, preview, TM, who, fks) {
    var added = [], modulesTouched = 0;
    preview.groups.forEach(function (g) {
      var n = 0;
      g.rows.forEach(function (r) {
        if (!r.checked || r.already) return;
        // The model's own add: the staging name, prefix and duplicate check
        // are exactly those of a manual add.
        var t = TM.tmAddTable(tpl, g.module, { targetTable: r.table, required: r.required !== false }, who);
        if (!t) return;
        t.source = 'ai';
        t.aiConfidence = r.confidence;
        t.aiReason = r.reason;
        added.push({ row: t, module: g.module });
        n++;
      });
      if (n) modulesTouched++;
    });
    // Shared is said in the notes of the rows just added, from what the
    // template now actually holds — not from what was proposed, since some
    // proposals may have been unticked. Rows that were already there are not
    // touched: Suggest only ever adds.
    var map = membership(tpl);
    var shared = {};
    added.forEach(function (a) {
      var others = sharedWith(map, a.row.targetTable, a.module);
      if (!others.length) return;
      shared[lc(a.row.targetTable)] = true;
      a.row.notes = (a.row.notes ? a.row.notes + ' · ' : '') + 'Also in: ' + others.join(', ');
    });
    var lo = added.length ? applyLoadOrder(tpl, fks, 'recalc') : { set: 0, cycles: 0 };
    return { added: added.length, modules: modulesTouched, shared: Object.keys(shared).length, ordered: lo.set, cycles: lo.cycles };
  }

  /* ── Transport: through the data proxy ───────────────────────────────── */
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

  /* ── The whole run ───────────────────────────────────────────────────── */
  // opts: { tpl, modules:[{key,name,notes}], graph, columnsOf, post, onProgress,
  //         onStarted(state), signal, sleep, pollMs, onlyTables, idPrefix,
  //         outcomes (earlier ones, for a retry), keep (earlier ticks) }
  // Resolves { preview, outcomes, state, fks, tableCount } or { cancelled:true }.
  function run(opts) {
    var signal = opts.signal;
    return prepare(opts).then(function (prep) {
      if (prep.cancelled || (signal && signal.aborted)) return { cancelled: true };
      return submit(prep, opts).then(function (state) {
        if (signal && signal.aborted) {
          var ids = state.batches.map(function (b) { return b.id; });
          if (ids.length) (opts.post || httpPost)('/agent/table-classify/cancel', { batchIds: ids }).catch(function () {});
          return { cancelled: true };
        }
        if (opts.onStarted) { try { opts.onStarted(state); } catch (e) { /* storage is a convenience */ } }
        return finish(state, prep.fks, prep.tables.length, opts);
      });
    });
  }

  // Pick up a run whose batches were submitted earlier (a page reload).
  function resume(state, opts) {
    return finish(state, edgesByName(opts.graph), state.tableCount || 0, opts);
  }

  function finish(state, fks, tableCount, opts) {
    return collect(state, Object.assign({}, opts, { outcomes: (opts.outcomes || []).slice() })).then(function (c) {
      if (c.cancelled) return c;
      return { preview: buildPreview(opts.tpl, opts.modules, c.outcomes, opts.keep), outcomes: c.outcomes,
        state: c.state, fks: fks, tableCount: tableCount };
    });
  }

  return {
    run: run, resume: resume, prepare: prepare, submit: submit, collect: collect,
    buildPreview: buildPreview, ticksOf: ticksOf, apply: apply, applyLoadOrder: applyLoadOrder,
    computeLoadOrder: computeLoadOrder, membership: membership, sharedWith: sharedWith,
    edgesByName: edgesByName, templateTableNames: templateTableNames, fillable: fillable,
    makeChunks: makeChunks, parts: parts, httpPost: httpPost, pool: pool,
    LIMITS: { CHUNK_TABLES: CHUNK_TABLES, CHUNK_CHARS: CHUNK_CHARS, PART_CHUNKS: PART_CHUNKS, PART_CHARS: PART_CHARS,
      COL_CONCURRENCY: COL_CONCURRENCY, POLL_MS: POLL_MS, STEP: STEP },
  };
});
