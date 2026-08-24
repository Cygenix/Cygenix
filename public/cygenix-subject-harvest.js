/* cygenix-subject-harvest.js — bulk metadata harvest for the subject mapper.
 *
 * The semantic classifier needs column names, types and primary keys for
 * EVERY table on both sides. The graph cache lazy-loads columns per table
 * (60 of 12,245 on the measured target), so before this module existed the
 * classifier only ever saw table names — which is how 88% of a real estate
 * ended up in "Other, needs triage".
 *
 * One paged INFORMATION_SCHEMA pull per side, no data values read. Paging is
 * not optional: the execute path silently truncates flat results at 5,000
 * rows, so every query here stays well under that and keeps fetching until a
 * short page says it has everything.
 *
 * Storage: the harvest CANNOT go in localStorage. Measured, the two sides
 * come to ~4.7MB of column signatures against a ~5MB origin quota already
 * carrying a 3.27MB graph cache — a QuotaExceededError mid-write corrupts
 * the cache for that connection. It goes in IndexedDB, where these sizes are
 * nothing, with an in-memory fallback for environments without it. Version
 * v2 by construction: there is no v1 harvest, and a graph cache without
 * columns must fail the adapter assertion loudly, never rehydrate into
 * "everything Unclassified".
 */
(function () {
  'use strict';

  const HARVEST_VERSION = 3;   // v3: + object inventory (usage, deps, descriptions)
  const TTL_MS = 12 * 60 * 60 * 1000;     // matches the graph cache's outlook
  const COL_PAGE = 500;                   // tables per columns page
  const KEY_PAGE = 4000;                  // rows per pk / rowcount page
  const DB_NAME = 'cygenix-subjectmap';
  const STORE = 'harvest';

  // ── SQL builders — read-only catalog queries, always paged ───────────────
  // Unqualified on purpose: they run against whatever database the side's
  // connection lands in. Qualifying with a database name only ever worked in
  // the investigation environment by accident.
  function shColumnsSql(off, page) {
    const p = page || COL_PAGE;
    return 'WITH t AS (\n'
      + ' SELECT TABLE_SCHEMA s, TABLE_NAME n\n'
      + ' FROM INFORMATION_SCHEMA.TABLES\n'
      + ' ORDER BY TABLE_SCHEMA, TABLE_NAME\n'
      + ' OFFSET ' + (off | 0) + ' ROWS FETCH NEXT ' + p + ' ROWS ONLY\n'
      + ')\n'
      + 'SELECT t.s, t.n,\n'
      + "       STRING_AGG(CAST(c.COLUMN_NAME + ':' + LEFT(c.DATA_TYPE,3) AS varchar(max)), ',') AS c\n"
      + 'FROM t\n'
      + 'JOIN INFORMATION_SCHEMA.COLUMNS c\n'
      + ' ON c.TABLE_SCHEMA = t.s AND c.TABLE_NAME = t.n\n'
      + 'GROUP BY t.s, t.n;';
  }
  function shPkSql(off, page) {
    const p = page || KEY_PAGE;
    return 'SELECT tc.TABLE_SCHEMA s, tc.TABLE_NAME n,\n'
      + "       STRING_AGG(CAST(k.COLUMN_NAME AS varchar(max)), ',') AS pk\n"
      + 'FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc\n'
      + 'JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k\n'
      + ' ON  k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME\n'
      + ' AND k.TABLE_SCHEMA    = tc.TABLE_SCHEMA\n'
      + ' AND k.TABLE_NAME      = tc.TABLE_NAME\n'
      + "WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'\n"
      + 'GROUP BY tc.TABLE_SCHEMA, tc.TABLE_NAME\n'
      + 'ORDER BY 1,2 OFFSET ' + (off | 0) + ' ROWS FETCH NEXT ' + p + ' ROWS ONLY;';
  }
  function shRowsSql(off, page) {
    const p = page || KEY_PAGE;
    return "SELECT s.name + '.' + t.name AS k, SUM(p.rows) AS r\n"
      + 'FROM sys.tables t\n'
      + 'JOIN sys.schemas s     ON s.schema_id = t.schema_id\n'
      + 'JOIN sys.partitions p  ON p.object_id = t.object_id AND p.index_id IN (0,1)\n'
      + "GROUP BY s.name + '.' + t.name\n"
      + 'ORDER BY 1 OFFSET ' + (off | 0) + ' ROWS FETCH NEXT ' + p + ' ROWS ONLY;';
  }

  const rows = (res) => (res && (res.recordset || res.rows)) || [];

  // Fetch every page of one query. `make(off)` builds the page; a page
  // shorter than `page` is the last one.
  async function shAllPages(execute, make, page, onPage) {
    const out = [];
    for (let off = 0; ; off += page) {
      const batch = rows(await execute(make(off, page)));
      out.push(...batch);
      if (onPage) { try { onPage(out.length); } catch (e) {} }
      if (batch.length < page) break;
    }
    return out;
  }

  // ── The harvest itself ───────────────────────────────────────────────────
  // Returns { v, fetchedAt, database, tables: { 'schema.name': { c, pk, r } } }
  async function shHarvest(execute, opts) {
    const o = opts || {};
    const say = (stage, n) => { try { if (o.onProgress) o.onProgress({ stage, n }); } catch (e) {} };
    const tables = {};
    const key = (s, n) => (s || 'dbo') + '.' + n;

    say('columns', 0);
    const cols = await shAllPages(execute, shColumnsSql, o.colPage || COL_PAGE, n => say('columns', n));
    for (const r of cols) {
      const k = key(r.s, r.n);
      tables[k] = tables[k] || {};
      tables[k].c = String(r.c || '');
    }

    say('keys', 0);
    const pks = await shAllPages(execute, shPkSql, o.keyPage || KEY_PAGE, n => say('keys', n));
    for (const r of pks) {
      const k = key(r.s, r.n);
      tables[k] = tables[k] || {};
      tables[k].pk = String(r.pk || '').split(',').filter(Boolean);
    }

    say('rows', 0);
    const counts = await shAllPages(execute, shRowsSql, o.keyPage || KEY_PAGE, n => say('rows', n));
    for (const r of counts) {
      const k = String(r.k || '');
      if (!k) continue;
      tables[k] = tables[k] || {};
      tables[k].r = Number(r.r) || 0;
    }

    // Object inventory for triage (v2 spec §3.1): usage stats, inbound
    // references and FKs, modify dates, descriptions. Optional — the SQL
    // builders live in the triage module and ride in via opts — and failure
    // is tolerated with the reason recorded: dm_* views need VIEW SERVER
    // STATE on some estates, and triage degrades honestly without them.
    let serverStart = null, inventoryError = '';
    if (o.triage) {
      try {
        say('inventory', 0);
        const inv = await shAllPages(execute, o.triage.tgInventorySql, o.invPage || 2000, n => say('inventory', n));
        for (const r of inv) {
          const k = key(r.s, r.n);
          tables[k] = tables[k] || {};
          tables[k].inv = {
            kind: /view/i.test(String(r.td || '')) ? 'view' : 'table',
            created: r.cd || null, modified: r.md || null,
            rows: Number(r.r) || 0, cols: Number(r.cols) || 0,
            hasPk: !!Number(r.haspk), hasUnique: !!Number(r.hasuq),
            indexCount: Number(r.ixn) || 0,
            inboundRefs: Number(r.inref) || 0, inboundFks: Number(r.infk) || 0,
            lastRead: r.lseek || r.lscan || null,
            readOps: Number(r.rops) || 0, writeOps: Number(r.wops) || 0,
            hasUsageRow: !!Number(r.hasusage),
            description: r.descr || null,
          };
        }
        try {
          const st = rows(await execute(o.triage.tgServerStartSql()))[0];
          serverStart = (st && st.started) || null;
        } catch (e) { /* start time is a nicety; the window just reads 0 */ }
      } catch (e) {
        inventoryError = (e && e.message) || 'inventory unavailable';
      }
    }

    return { v: HARVEST_VERSION, fetchedAt: Date.now(), database: o.database || null,
             serverStart, inventoryError, tables };
  }

  // ── IndexedDB cache, in-memory fallback ──────────────────────────────────
  const _mem = new Map();

  function idbOpen() {
    return new Promise((resolve) => {
      if (typeof indexedDB === 'undefined') return resolve(null);
      let req;
      try { req = indexedDB.open(DB_NAME, 1); } catch { return resolve(null); }
      req.onupgradeneeded = () => { try { req.result.createObjectStore(STORE); } catch (e) {} };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => resolve(null);
      req.onblocked = () => resolve(null);
    });
  }
  function idbGet(db, key) {
    return new Promise((resolve) => {
      try {
        const tx = db.transaction(STORE, 'readonly').objectStore(STORE).get(key);
        tx.onsuccess = () => resolve(tx.result || null);
        tx.onerror = () => resolve(null);
      } catch { resolve(null); }
    });
  }
  function idbPut(db, key, value) {
    return new Promise((resolve) => {
      try {
        const tx = db.transaction(STORE, 'readwrite').objectStore(STORE).put(value, key);
        tx.onsuccess = () => resolve(true);
        tx.onerror = () => resolve(false);
      } catch { resolve(false); }
    });
  }

  async function shReadCache(cacheKey) {
    const db = await idbOpen();
    const doc = db ? await idbGet(db, cacheKey) : _mem.get(cacheKey);
    if (db) { try { db.close(); } catch (e) {} }
    if (!doc || doc.v !== HARVEST_VERSION) return null;
    if (!doc.fetchedAt || Date.now() - doc.fetchedAt > TTL_MS) return null;
    return doc;
  }
  async function shWriteCache(cacheKey, doc) {
    const db = await idbOpen();
    const ok = db ? await idbPut(db, cacheKey, doc) : false;
    if (db) { try { db.close(); } catch (e) {} }
    if (!ok) _mem.set(cacheKey, doc);
    return true;
  }

  // ── ensure: cached harvest for one side, or fetch it ─────────────────────
  // io: { execute(sql)→result, database, cacheKey, force, onProgress }
  async function shEnsure(io) {
    const cacheKey = io.cacheKey || ('side|' + (io.database || 'unknown'));
    if (!io.force) {
      const hit = await shReadCache(cacheKey);
      if (hit) return hit;
    }
    const doc = await shHarvest(io.execute, io);
    await shWriteCache(cacheKey, doc);
    return doc;
  }

  // ── merge into the in-memory graph ───────────────────────────────────────
  // colsig/pk land on the NODES ONLY. The graph's localStorage cache does not
  // serialise colsig, so the harvest can never push that cache over quota —
  // the harvest has its own store.
  function shApply(graph, harvest) {
    if (!graph || !graph.ok || !harvest || !harvest.tables) return 0;
    let hit = 0;
    for (const t of graph.tables) {
      const h = harvest.tables[t.key] || harvest.tables[t.schema + '.' + t.name];
      if (!h) continue;
      hit++;
      if (h.c != null) t.colsig = h.c;
      if (h.pk && h.pk.length && !(t.primaryKeys || []).length) t.primaryKeys = h.pk.slice();
      if (typeof h.r === 'number' && h.r >= 0) t.rowCount = h.r;
    }
    return hit;
  }

  const api = {
    HARVEST_VERSION, COL_PAGE, KEY_PAGE,
    shColumnsSql, shPkSql, shRowsSql,
    shAllPages, shHarvest, shEnsure, shApply,
    shReadCache, shWriteCache,
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (typeof window !== 'undefined') window.CygenixSubjectHarvest = api;
})();
