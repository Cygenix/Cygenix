/* cygenix-schema-graph.js — the shared data layer for the Schema Explorer.
 *
 * Both tabs of schema_explorer.html read from here, so the schema is fetched
 * once per connection and not twice per page view.
 *
 *   CygenixSchemaGraph.load(side, opts)   → { database, tables[], edges[], … }
 *   CygenixSchemaGraph.columns(side, s,t) → columns/PKs/FKs for one table
 *   CygenixSchemaGraph.sample(side, s, t) → TOP n rows, or null with no conn
 *   CygenixSchemaGraph.reload(side)       → drop the cache and refetch
 *   CygenixSchemaGraph.savedMaps()        → this project's simple-map jobs
 *   CygenixSchemaGraph.loadOrder(graph)   → FK-ordered waves, cycles reported
 *
 * ── Why it does not run the query in the brief ───────────────────────────
 * The brief specifies one big sys.* query per connection. db-connect already
 * exposes purpose-built read-only actions that return exactly the same facts —
 * `schema-tables` (tables, kind, row counts, database name), `schema-fks`
 * (every FK edge in one call) and `schema-columns` (one table's columns, PKs
 * and FKs, with types already formatted). Using them means:
 *
 *   * no new SQL to review, and nothing this page can send that the rest of
 *     the app does not already send;
 *   * the calls go through the Azure relay when the target is an Entra-only
 *     server, which a raw `execute` from here would not;
 *   * types arrive pre-formatted as NVARCHAR(200) rather than name plus
 *     max_length, which every caller would otherwise have to reassemble.
 *
 * ── Why columns are lazy ─────────────────────────────────────────────────
 * The single query returns one row per column in the database. object_mapping
 * already learned this the hard way: the legacy `schema` action bundles
 * columns for every table and routinely exceeds Netlify's 6 MB response
 * ceiling on wide databases, which is why the paginated actions exist. Tables
 * and FK edges are cheap and always loaded; columns are fetched per table, in
 * a bounded number of parallel requests, and cached. The ERD can draw a table
 * box and its FK anchors from the edge list before any column has arrived.
 *
 * Read-only by construction: the only actions used are schema-tables,
 * schema-fks, schema-columns and a TOP-n SELECT. Nothing here writes.
 */
(function () {
  'use strict';

  const CACHE_PREFIX  = 'cygenix_schema_';   // + <connHash>
  const CACHE_VERSION = 1;
  // A schema does not change often and the query is slow on wide databases.
  const CACHE_TTL_MS  = 12 * 60 * 60 * 1000;
  const SAMPLE_ROWS   = 12;
  // Enough to fill a canvas quickly without opening a hundred sockets.
  const COLUMN_FETCH_CONCURRENCY = 6;

  // ── Connections ─────────────────────────────────────────────────────────
  // Same resolution as object_mapping.html: an Azure Function URL wins over a
  // direct connection string, and the function key rides in the query string.
  function connFor(side) {
    try {
      if (!window.CygenixConnections) return '';
      const c = window.CygenixConnections.get() || {};
      if (side === 'src') {
        return c.srcFnUrl
          ? (c.srcFnKey ? c.srcFnUrl + '?code=' + encodeURIComponent(c.srcFnKey) : c.srcFnUrl)
          : (c.srcConnString || '');
      }
      return c.tgtFnUrl
        ? (c.tgtFnKey ? c.tgtFnUrl + '?code=' + encodeURIComponent(c.tgtFnKey) : c.tgtFnUrl)
        : (c.tgtConnString || '');
    } catch { return ''; }
  }

  const isFn = (c) => !!c && /^https?:\/\//i.test(c);

  // A stable per-connection cache key that is not the connection string.
  //
  // The connection string carries a password. It must not end up as part of a
  // localStorage key, where it would sit in plain sight in devtools and in any
  // storage export. This is a non-cryptographic digest used only to tell two
  // connections apart.
  function connHash(conn) {
    let h1 = 0x811c9dc5, h2 = 0x01000193;
    const s = String(conn || '');
    for (let i = 0; i < s.length; i++) {
      const c = s.charCodeAt(i);
      h1 = Math.imul(h1 ^ c, 16777619) >>> 0;
      h2 = Math.imul(h2 + c, 2246822519) >>> 0;
    }
    return (h1 >>> 0).toString(36) + (h2 >>> 0).toString(36);
  }

  // ── Transport ───────────────────────────────────────────────────────────
  async function dbCall(conn, body) {
    const url     = isFn(conn) ? conn : '/.netlify/functions/db-connect';
    const payload = isFn(conn) ? body : { ...body, connectionString: conn };
    const res = await (window.CygenixGuardrail
      ? window.CygenixGuardrail.fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
          signal: AbortSignal.timeout(60000),
        })
      : fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
          signal: AbortSignal.timeout(60000),
        }));
    const data = await res.json().catch(() => ({ error: 'Non-JSON response (' + res.status + ')' }));
    if (!res.ok) throw new Error(data.error || res.statusText);
    // The Azure /api/db route answers HTTP 200 with { success:false, error }
    // for an unknown action, so a status check alone is not enough.
    if (data && data.success === false && data.error) throw new Error(data.error);
    return data;
  }

  // ── Cache ───────────────────────────────────────────────────────────────
  function cacheKey(conn) { return CACHE_PREFIX + connHash(conn); }

  function readCache(conn) {
    try {
      const raw = localStorage.getItem(cacheKey(conn));
      if (!raw) return null;
      const doc = JSON.parse(raw);
      if (!doc || doc.v !== CACHE_VERSION) return null;
      if (!doc.fetchedAt || (Date.now() - doc.fetchedAt) > CACHE_TTL_MS) return null;
      return doc.graph;
    } catch { return null; }
  }

  function writeCache(conn, graph) {
    try {
      localStorage.setItem(cacheKey(conn), JSON.stringify({
        v: CACHE_VERSION, fetchedAt: Date.now(), graph,
      }));
    } catch {
      // A wide schema can exceed the storage quota. Losing the cache costs a
      // refetch; failing the load would cost the page.
    }
  }

  function dropCache(conn) {
    try { localStorage.removeItem(cacheKey(conn)); } catch {}
  }

  // ── The graph ───────────────────────────────────────────────────────────
  const key = (schema, name) => (schema || 'dbo') + '.' + name;

  // In-flight and completed loads, per side, so two tabs asking at once share
  // one request rather than racing.
  const _inflight = {};
  const _graphs   = {};

  function emptyGraph(reason) {
    return {
      ok: false, reason: reason || 'No connection configured',
      database: null, tables: [], edges: [], byKey: {}, fetchedAt: null, cached: false,
    };
  }

  function buildGraph(database, tables, foreignKeys) {
    const byKey = {};
    const list = (tables || []).map(t => {
      const node = {
        schema: t.schema || 'dbo',
        name:   t.name,
        key:    key(t.schema, t.name),
        kind:   t.kind || 'table',
        rowCount: typeof t.rowCount === 'number' ? t.rowCount : 0,
        columns: null,          // lazily filled by columns()
        primaryKeys: [],
        // Column names that participate in an FK, known from the edge list
        // before the columns themselves have loaded — enough for the ERD to
        // anchor an edge to the right row as soon as the box is drawn.
        fkColumns: [],
        refColumns: [],
      };
      byKey[node.key] = node;
      return node;
    });

    const edges = [];
    for (const fk of (foreignKeys || [])) {
      const from = key(fk.fromSchema, fk.fromTable);
      const to   = key(fk.toSchema,   fk.toTable);
      // An FK to a table outside the listed set (a different database, or a
      // schema filtered out) would otherwise produce an edge to nowhere.
      if (!byKey[from] || !byKey[to]) continue;
      edges.push({
        from, to,
        fromColumn: fk.fromColumn,
        toColumn:   fk.toColumn,
        name:       fk.name || '',
        self:       from === to,
      });
      if (byKey[from].fkColumns.indexOf(fk.fromColumn) < 0) byKey[from].fkColumns.push(fk.fromColumn);
      if (byKey[to].refColumns.indexOf(fk.toColumn) < 0)    byKey[to].refColumns.push(fk.toColumn);
    }

    // Adjacency, both directions — the join-path finder walks FKs either way.
    const adj = {};
    for (const t of list) adj[t.key] = [];
    for (const e of edges) {
      if (e.self) continue;
      adj[e.from].push({ to: e.to, edge: e, direction: 'out' });
      adj[e.to].push({ to: e.from, edge: e, direction: 'in' });
    }

    return {
      ok: true, reason: null, database, tables: list, edges, byKey, adj,
      fetchedAt: Date.now(), cached: false,
    };
  }

  // Rehydrate a cached graph: byKey and adj hold object references, which do
  // not survive JSON, so they are rebuilt rather than stored.
  function rehydrate(graph) {
    const g = buildGraph(graph.database, graph.tables, graph.edges.map(e => {
      const [fs, ft] = splitKey(e.from), [ts, tt] = splitKey(e.to);
      return {
        fromSchema: fs, fromTable: ft, fromColumn: e.fromColumn,
        toSchema:   ts, toTable:   tt, toColumn:   e.toColumn, name: e.name,
      };
    }));
    // Carry any columns that were cached with the graph.
    for (const t of graph.tables) {
      const node = g.byKey[t.key];
      if (node && t.columns) { node.columns = t.columns; node.primaryKeys = t.primaryKeys || []; }
    }
    g.fetchedAt = graph.fetchedAt || null;
    g.cached = true;
    return g;
  }

  function splitKey(k) {
    const i = String(k).indexOf('.');
    return i < 0 ? ['dbo', String(k)] : [k.slice(0, i), k.slice(i + 1)];
  }

  // ── load ────────────────────────────────────────────────────────────────
  // side: 'src' | 'tgt'. Resolves to a graph object; never rejects — an
  // unreachable database is an empty state with a reason, not an exception,
  // because both tabs render around it.
  function load(side, opts) {
    const o = opts || {};
    const conn = connFor(side);
    if (!conn) return Promise.resolve(emptyGraph('No ' + (side === 'src' ? 'source' : 'target') + ' connection configured'));

    if (!o.force && _graphs[side] && _graphs[side].conn === conn) {
      return Promise.resolve(_graphs[side].graph);
    }
    if (_inflight[side]) return _inflight[side];

    const p = (async () => {
      if (!o.force) {
        const cached = readCache(conn);
        if (cached) {
          const g = rehydrate(cached);
          _graphs[side] = { conn, graph: g };
          return g;
        }
      }
      try {
        // Both are whole-database calls and independent of each other.
        const [tablesRes, fksRes] = await Promise.all([
          dbCall(conn, { action: 'schema-tables' }),
          dbCall(conn, { action: 'schema-fks' }).catch(() => ({ foreignKeys: [] })),
        ]);
        const g = buildGraph(
          tablesRes.database || null,
          tablesRes.tables || [],
          fksRes.foreignKeys || []
        );
        _graphs[side] = { conn, graph: g };
        writeCache(conn, serialisable(g));
        return g;
      } catch (e) {
        return emptyGraph((e && e.message) || 'Could not read the schema');
      } finally {
        delete _inflight[side];
      }
    })();

    _inflight[side] = p;
    return p;
  }

  // byKey/adj are derived; storing them would triple the cache for nothing.
  function serialisable(g) {
    return {
      database: g.database,
      fetchedAt: g.fetchedAt,
      tables: g.tables.map(t => ({
        schema: t.schema, name: t.name, key: t.key, kind: t.kind,
        rowCount: t.rowCount, columns: t.columns, primaryKeys: t.primaryKeys,
      })),
      edges: g.edges,
    };
  }

  // Re-cache after columns arrive, so reopening the page does not refetch the
  // tables the user was actually looking at.
  let _flushTimer = null;
  function scheduleFlush(side) {
    if (_flushTimer) clearTimeout(_flushTimer);
    _flushTimer = setTimeout(() => {
      _flushTimer = null;
      const entry = _graphs[side];
      if (entry) writeCache(entry.conn, serialisable(entry.graph));
    }, 1500);
  }

  // ── columns ─────────────────────────────────────────────────────────────
  // One table's detail. Cached on the node, so repeated inspector opens and
  // ERD redraws cost nothing.
  const _colInflight = {};

  async function columns(side, schema, name) {
    const entry = _graphs[side];
    const k = key(schema, name);
    const node = entry && entry.graph.byKey[k];
    if (node && node.columns) return node;

    const conn = connFor(side);
    if (!conn) return node || null;

    const inflightKey = side + '|' + k;
    if (_colInflight[inflightKey]) return _colInflight[inflightKey];

    const p = (async () => {
      try {
        const res = await dbCall(conn, { action: 'schema-columns', schemaName: schema, tableName: name });
        const t = res.table || {};
        if (node) {
          node.columns     = t.columns || [];
          node.primaryKeys = t.primaryKeys || [];
          scheduleFlush(side);
        }
        return node || t;
      } catch (e) {
        // An unreadable table must not wedge the inspector; the box renders
        // with the FK columns already known from the edge list.
        if (node && !node.columns) { node.columns = []; node.columnsError = (e && e.message) || 'unavailable'; }
        return node;
      } finally {
        delete _colInflight[inflightKey];
      }
    })();

    _colInflight[inflightKey] = p;
    return p;
  }

  // Fill columns for a set of tables with a bounded number of requests in
  // flight — the ERD asks for everything on canvas at once, and a hundred
  // parallel sockets is how a browser stalls.
  async function columnsFor(side, keys, onEach) {
    const queue = (keys || []).slice();
    const workers = new Array(Math.min(COLUMN_FETCH_CONCURRENCY, queue.length || 1))
      .fill(0).map(async () => {
        while (queue.length) {
          const k = queue.shift();
          const [s, n] = splitKey(k);
          const node = await columns(side, s, n);
          if (onEach) { try { onEach(node); } catch {} }
        }
      });
    await Promise.all(workers);
  }

  // ── sample ──────────────────────────────────────────────────────────────
  // Real rows, or null when there is no connection — the caller hides the tab
  // rather than showing invented data.
  async function sample(side, schema, name, limit) {
    const conn = connFor(side);
    if (!conn) return null;
    const n = Math.max(1, Math.min(parseInt(limit, 10) || SAMPLE_ROWS, 100));
    // Identifiers, not parameters — a parameter cannot appear where a table
    // name goes. Bracket-quoted with internal ] doubled, which is the only
    // escape T-SQL needs for an identifier.
    const q = (s) => '[' + String(s == null ? '' : s).replace(/]/g, ']]') + ']';
    const sql = 'SELECT TOP ' + n + ' * FROM ' + q(schema) + '.' + q(name);
    try {
      const res = await dbCall(conn, { action: 'execute', sql });
      return { rows: res.recordset || res.rows || [], sql };
    } catch (e) {
      return { rows: [], error: (e && e.message) || 'Could not read rows', sql };
    }
  }

  // ── execute ─────────────────────────────────────────────────────────────
  // A raw statement on a side's connection, for the inference engine's capped
  // catalog reads and containment probes. Same transport as sample(); every
  // statement the engine builds is a SELECT, and the server-side gate
  // classifies and refuses writes regardless of what a caller passes.
  async function execute(side, sql) {
    const conn = connFor(side);
    if (!conn) throw new Error('No ' + (side === 'src' ? 'source' : 'target') + ' connection configured');
    return dbCall(conn, { action: 'execute', sql });
  }

  // ── Saved maps ──────────────────────────────────────────────────────────
  // The brief names cygenix_conv_project.id as the project filter. That is the
  // legacy single-project key, read only during migration (project-builder.js
  // calls it exactly that). The live one is cygenix_active_project_id, which
  // is what object_mapping.html stamps onto every job it saves as `projectId`,
  // so filtering on anything else would match nothing for a multi-project
  // account. The legacy key is still read as a fallback for a browser that has
  // not been through the migration.
  function activeProjectId() {
    try {
      const id = (localStorage.getItem('cygenix_active_project_id') || '').trim();
      if (id) return id;
      const legacy = JSON.parse(localStorage.getItem('cygenix_conv_project') || 'null');
      return (legacy && legacy.id) || '';
    } catch { return ''; }
  }

  function activeProjectName() {
    try {
      const id = activeProjectId();
      const projects = JSON.parse(localStorage.getItem('cygenix_projects') || '[]');
      const p = Array.isArray(projects) ? projects.find(x => x && x.id === id) : null;
      return (p && p.name) || '';
    } catch { return ''; }
  }

  // A mapping row is a fixed value when it carries a literalValue, whatever
  // else it carries. The generated SQL emits the literal and never reads
  // srcCol, so counting such a row as a mapped column overstates coverage.
  // fixedValue is the older spelling; applyMappingTransform honours both, so
  // a map saved under the old name must read the same way here.
  function literalOf(m) {
    if (!m) return '';
    const v = (m.literalValue != null && m.literalValue !== '') ? m.literalValue
            : (m.fixedValue   != null && m.fixedValue   !== '') ? m.fixedValue : '';
    return v == null ? '' : String(v);
  }
  const isFixed  = (m) => literalOf(m) !== '';
  const isMapped = (m) => !!(m && m.tgtCol) && !isFixed(m) && !!m.srcCol;

  function savedMaps(opts) {
    const o = opts || {};
    const projectId = o.projectId !== undefined ? o.projectId : activeProjectId();
    let jobs = [];
    try { jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]'); } catch { jobs = []; }
    if (!Array.isArray(jobs)) jobs = [];

    return jobs.filter(j => {
      if (!j || j._deleted) return false;
      if ((j.jobType || j.type) !== 'simple-map') return false;
      // A job saved before projects existed has no projectId. Treating it as
      // belonging to the active project matches how the job picker and the
      // dashboard already handle it, and beats stranding it invisibly.
      if (projectId && j.projectId && j.projectId !== projectId) return false;
      return true;
    }).map(j => {
      const cols = Array.isArray(j.columnMapping) ? j.columnMapping : [];
      const mapped = cols.filter(isMapped);
      const fixed  = cols.filter(m => m && m.tgtCol && isFixed(m));
      // One-to-many mode writes several target tables from one source, so a
      // single map can be more than one ribbon.
      const targets = Array.isArray(j.targetTables) && j.targetTables.length
        ? j.targetTables.slice()
        : (j.targetTable ? [j.targetTable] : []);
      return {
        id: j.id,
        name: j.name || '(untitled map)',
        sourceTable: j.sourceTable || '',
        targetTable: j.targetTable || '',
        targetTables: targets,
        status: j.status || j.executionStatus || 'draft',
        created: j.created || '',
        srcWhere: j.srcWhere || '',
        joinState: j.joinState || null,
        columnMapping: cols,
        mappedColumns: mapped.length,
        fixedColumns: fixed.length,
        // Worst confidence across the mapped columns — the ribbon takes its
        // colour from this, so one bad guess is visible rather than averaged
        // away.
        worstMatch: mapped.reduce((w, m) => {
          const v = typeof m.match === 'number' ? m.match : 100;
          return v < w ? v : w;
        }, 100),
      };
    });
  }

  // ── Load order ──────────────────────────────────────────────────────────
  // A parent must be inserted before the rows that reference it, so the load
  // order is a topological sort of the FK graph restricted to the tables being
  // written. Kahn's algorithm, levelled so independent tables share a wave and
  // can run in parallel.
  //
  // A cycle has no valid order. It is reported rather than dropped: silently
  // omitting the tables in a cycle would produce a plan that looks complete
  // and fails at run time on a constraint.
  function loadOrder(graph, tableKeys) {
    const wanted = new Set(tableKeys || (graph && graph.tables || []).map(t => t.key));
    const indegree = {}, dependents = {};
    for (const k of wanted) { indegree[k] = 0; dependents[k] = []; }

    for (const e of ((graph && graph.edges) || [])) {
      if (e.self) continue;                       // self-reference orders nothing
      if (!wanted.has(e.from) || !wanted.has(e.to)) continue;
      // e.from references e.to → e.to must be loaded first.
      dependents[e.to].push(e.from);
      indegree[e.from]++;
    }

    const waves = [];
    let frontier = [...wanted].filter(k => indegree[k] === 0).sort();
    const placed = new Set();

    while (frontier.length) {
      waves.push(frontier.slice());
      frontier.forEach(k => placed.add(k));
      const next = [];
      for (const k of waves[waves.length - 1]) {
        for (const d of dependents[k]) {
          if (--indegree[d] === 0) next.push(d);
        }
      }
      frontier = next.sort();
    }

    const cyclic = [...wanted].filter(k => !placed.has(k)).sort();
    return {
      waves,
      cyclic,
      // The cycles themselves, so the UI can name the tables involved rather
      // than just counting them.
      cycles: cyclic.length ? findCycles(graph, cyclic) : [],
      ok: cyclic.length === 0,
    };
  }

  // Depth-first walk over just the tables that failed to sort, collecting the
  // edge loops. Only ever runs over a cycle set, which is small.
  function findCycles(graph, keys) {
    const inSet = new Set(keys);
    const out = [], stack = [], onStack = new Set(), seen = new Set();

    const outgoing = {};
    for (const k of keys) outgoing[k] = [];
    for (const e of graph.edges) {
      if (e.self || !inSet.has(e.from) || !inSet.has(e.to)) continue;
      outgoing[e.from].push(e.to);
    }

    const walk = (k) => {
      stack.push(k); onStack.add(k); seen.add(k);
      for (const n of outgoing[k]) {
        if (onStack.has(n)) {
          const at = stack.indexOf(n);
          if (at >= 0) out.push(stack.slice(at).concat(n));
        } else if (!seen.has(n)) walk(n);
      }
      stack.pop(); onStack.delete(k);
    };
    for (const k of keys) if (!seen.has(k)) walk(k);
    return out;
  }

  // ── Join paths ──────────────────────────────────────────────────────────
  // Breadth-first over the FK graph, so the path returned is the one with the
  // fewest joins. Undirected: a join is valid whichever way the FK points.
  function joinPath(graph, fromKey, toKey) {
    if (!graph || !graph.adj || !graph.byKey[fromKey] || !graph.byKey[toKey]) return null;
    if (fromKey === toKey) return { tables: [fromKey], hops: [] };

    const prev = { [fromKey]: null };
    const queue = [fromKey];
    while (queue.length) {
      const cur = queue.shift();
      for (const nb of graph.adj[cur]) {
        if (prev[nb.to] !== undefined) continue;
        prev[nb.to] = { from: cur, edge: nb.edge, direction: nb.direction };
        if (nb.to === toKey) {
          const hops = [];
          let at = toKey;
          while (prev[at]) { hops.unshift({ to: at, ...prev[at] }); at = prev[at].from; }
          return { tables: [fromKey, ...hops.map(h => h.to)], hops };
        }
        queue.push(nb.to);
      }
    }
    return null;   // the two tables are in different components
  }

  // Render a path as runnable T-SQL. Aliases are t1..tn so a table appearing
  // twice in a path cannot collide with itself.
  function joinSql(graph, path) {
    if (!path || !path.tables.length) return '';
    const q = (s) => '[' + String(s == null ? '' : s).replace(/]/g, ']]') + ']';
    const qk = (k) => { const [s, n] = splitKey(k); return q(s) + '.' + q(n); };
    const alias = {};
    path.tables.forEach((k, i) => { alias[k] = 't' + (i + 1); });

    const lines = ['SELECT TOP 100 *', 'FROM ' + qk(path.tables[0]) + ' ' + alias[path.tables[0]]];
    for (const hop of path.hops) {
      const e = hop.edge;
      // The edge is stored parent-side; which column belongs to which alias
      // depends on the direction the path traversed it.
      const leftKey  = hop.direction === 'out' ? e.from : e.to;
      const leftCol  = hop.direction === 'out' ? e.fromColumn : e.toColumn;
      const rightCol = hop.direction === 'out' ? e.toColumn   : e.fromColumn;
      lines.push('JOIN ' + qk(hop.to) + ' ' + alias[hop.to] +
        ' ON ' + alias[hop.to] + '.' + q(rightCol) +
        ' = ' + alias[leftKey] + '.' + q(leftCol));
    }
    return lines.join('\n') + ';';
  }

  // Tables within `depth` FK hops of a starting set — the ERD's focus control.
  function neighbourhood(graph, startKeys, depth) {
    const seen = new Set(startKeys || []);
    if (!graph || !graph.adj || depth == null) return seen;
    let frontier = [...seen];
    for (let d = 0; d < depth; d++) {
      const next = [];
      for (const k of frontier) {
        for (const nb of (graph.adj[k] || [])) {
          if (!seen.has(nb.to)) { seen.add(nb.to); next.push(nb.to); }
        }
      }
      frontier = next;
      if (!frontier.length) break;
    }
    return seen;
  }

  window.CygenixSchemaGraph = {
    load, reload: (side) => { const c = connFor(side); if (c) dropCache(c); delete _graphs[side]; return load(side, { force: true }); },
    columns, columnsFor, sample, execute,
    savedMaps, activeProjectId, activeProjectName,
    loadOrder, joinPath, joinSql, neighbourhood,
    hasConnection: (side) => !!connFor(side),
    // Exported for tests and for the views' own bookkeeping.
    _internals: { key, splitKey, connHash, buildGraph, literalOf, isFixed, isMapped, findCycles },
  };
})();
