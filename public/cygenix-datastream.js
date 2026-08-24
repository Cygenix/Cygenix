/* ============================================================================
   cygenix-datastream.js — the Data Stream module's engine.
   ----------------------------------------------------------------------------
   WHAT THIS MODULE IS FOR

   Everything else in the console moves data in BATCHES: you map objects, you
   run a job, it finishes. A data stream is the opposite shape — it is always
   on. It watches one side of the project (source or target) for change, turns
   each change into a record, holds those records briefly in the Stream Store,
   and delivers them to a downstream system.

   Three ideas, and each one is a real thing in here rather than a word in the
   UI copy:

     Streaming Database  the Stream Store — the retained, queryable buffer
                         every captured change passes through between capture
                         and delivery. topicsOf() is what it holds.
     Data Stream         the configured pipeline object: capture → store →
                         delivery → destination. That is the Stream shape.
     Change Data Capture how a stream reads a relational database. capture.method
                         and capabilityCheck() are where it lives.

   WHY THE LOGIC IS HERE AND NOT IN THE PAGES

   Five screens read this state and two of them can change it. Left in the
   pages, the tick would run five slightly different ways and the numbers on
   the Monitor would stop agreeing with the numbers on the Streams list. One
   engine, node-requirable, means the arithmetic can be tested on its own and
   there is one answer to "how far behind is this stream".

   DETERMINISM, AND WHY IT IS NOT AT ODDS WITH PERSISTENCE

   The brief asks for reproducible demo figures AND for a paused stream to
   still be paused after a refresh. Those pull against each other if the
   randomness is a running sequence — a refresh would resume mid-sequence and
   the figures would differ.

   So the demo world is a pure function of a TICK NUMBER. Every draw is
   hash(seed, tickNo, streamId, purpose); nothing accumulates in a generator.
   Two fresh loads both start at tick 0 and therefore agree exactly, while a
   refresh resumes at the persisted tick and carries on where it was.
   Math.random() is never called.

   WHAT IS DELIBERATELY NOT HERE

   Real connectivity. Nothing in this file opens a socket, reads a transaction
   log or writes to a broker — v1 is the module's shape, its state machine and
   its arithmetic, running against seeded demo data. The places a real backend
   would attach are marked.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixDataStream) root.CygenixDataStream = api;
})(typeof window !== 'undefined' ? window : this, function () {
'use strict';

/* ── Storage keys ────────────────────────────────────────────────────────── */
var STORE_PREFIX = 'cygenix_datastream_v1::';
var WIP_PREFIX   = 'cygenix_datastream_wip_';
var TICK_MS      = 2000;
var MAX_EVENTS   = 500;    // in-memory tail cap, per the brief
var MAX_POINTS   = 720;    // 720 × 2s ≈ 24 minutes of chart history
var MAX_AUDIT    = 300;
var SEED         = 'cygenix-data-stream-v1';

/* ── Enums. The UI never invents a status string; it asks here. ──────────── */
var ENUMS = {
  status:      ['draft', 'snapshotting', 'running', 'lagging', 'paused', 'failed', 'stopped'],
  side:        ['source', 'target'],
  method:      ['log', 'poll', 'trigger', 'feed'],
  snapshot:    ['none', 'initial-then-stream', 'snapshot-only'],
  destKind:    ['database', 'broker', 'webhook', 'file', 'cygenix-target'],
  writeMode:   ['upsert', 'append', 'merge', 'audit-table'],
  ordering:    ['per-key', 'global', 'none'],
  schemaDrift: ['pause', 'evolve', 'ignore-new-columns'],
  op:          ['I', 'U', 'D', 'T', 'DDL'],
  deliveryState: ['pending', 'delivered', 'retrying', 'dead'],
  guardrail:   ['confirm_all', 'confirm_destructive'],
};

var METHOD_LABEL = {
  log:     'Log-based CDC',
  poll:    'Polling',
  trigger: 'Triggers',
  feed:    'External feed',
};
var METHOD_BLURB = {
  log:     'reads the database’s own transaction log; lowest load on the source, catches deletes.',
  poll:    're-reads rows whose timestamp/rowversion changed; works anywhere, misses hard deletes.',
  trigger: 'shadow tables written by triggers; use only when the log is unavailable.',
  feed:    'sensors, application logs, clickstream or event payloads posted to Cygenix.',
};
var WRITE_MODE_BLURB = {
  upsert:        'keep the destination row current',
  append:        'add every change as a new row',
  merge:         'upsert, and apply deletes',
  'audit-table': 'immutable change history — one row per change, never overwritten',
};
var DEST_KIND_LABEL = {
  database: 'Database', broker: 'Broker topic', webhook: 'Webhook',
  file: 'File landing zone', 'cygenix-target': 'Project target',
};

/* Statuses that are actively moving data. Used everywhere a screen asks
   "is this stream live?", so the answer cannot differ between screens. */
function isLive(s) { return s === 'running' || s === 'lagging' || s === 'snapshotting'; }

/* ══════════════════════════════════════════════════════════════════════════
   Deterministic randomness
   ══════════════════════════════════════════════════════════════════════════
   FNV-1a over the joined parts. Every draw names what it is for, so adding a
   new draw somewhere cannot shift the numbers an existing one produces —
   which is the failure mode of a sequential generator. */
function hash32(str) {
  var h = 2166136261;
  for (var i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}
function rand() {
  return hash32(SEED + '|' + Array.prototype.join.call(arguments, '|')) / 4294967296;
}
/* Integer in [lo, hi]. */
function randInt(lo, hi, parts) {
  return lo + Math.floor(rand.apply(null, parts) * (hi - lo + 1));
}
function pick(list, parts) { return list[randInt(0, list.length - 1, parts)]; }

/* Deterministic ids: same inputs, same id. A stream created on the same tick
   from the same draft gets the same id on every machine, which is what makes
   the seeded demo reproducible. */
function makeId(prefix, parts) {
  return prefix + '_' + hash32(SEED + '|id|' + parts.join('|')).toString(36).slice(0, 6);
}

/* ══════════════════════════════════════════════════════════════════════════
   Persistence
   ══════════════════════════════════════════════════════════════════════════ */
function storeKey(projectId) { return STORE_PREFIX + (projectId || 'default'); }
function wipKey(projectId)   { return WIP_PREFIX + (projectId || 'default'); }

function readLS(key, fallback) {
  try {
    if (typeof localStorage === 'undefined') return fallback;
    var raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : fallback;
  } catch (e) { return fallback; }
}
function writeLS(key, value) {
  try {
    if (typeof localStorage === 'undefined') return false;
    localStorage.setItem(key, JSON.stringify(value));
    return true;
  } catch (e) { return false; }
}

function emptyState(projectId) {
  return {
    version: 1,
    projectId: projectId || 'default',
    tickNo: 0,
    streams: [],
    events: [],       // newest first, capped at MAX_EVENTS
    points: [],       // oldest first, capped at MAX_POINTS
    audit: [],        // newest first
    seeded: false,
  };
}

/* Load, upgrading anything missing rather than throwing. A state written by
   an older build must still open. */
function load(projectId) {
  var s = readLS(storeKey(projectId), null);
  if (!s || typeof s !== 'object') return emptyState(projectId);
  var base = emptyState(projectId);
  Object.keys(base).forEach(function (k) { if (s[k] === undefined) s[k] = base[k]; });
  if (!Array.isArray(s.streams)) s.streams = [];
  if (!Array.isArray(s.events))  s.events  = [];
  if (!Array.isArray(s.points))  s.points  = [];
  if (!Array.isArray(s.audit))   s.audit   = [];
  s.projectId = projectId || s.projectId || 'default';
  return s;
}
function save(state) {
  if (!state) return false;
  // Cap before writing: an uncapped tail would grow localStorage without limit.
  state.events = state.events.slice(0, MAX_EVENTS);
  state.points = state.points.slice(-MAX_POINTS);
  state.audit  = state.audit.slice(0, MAX_AUDIT);
  return writeLS(storeKey(state.projectId), state);
}
function reset(projectId) {
  try { if (typeof localStorage !== 'undefined') localStorage.removeItem(storeKey(projectId)); }
  catch (e) { /* storage unavailable */ }
  return emptyState(projectId);
}

/* Designer draft. Separate key so an abandoned draft never corrupts the
   streams themselves — the same split object_mapping uses. */
function wipSave(projectId, draft) { return writeLS(wipKey(projectId), draft); }
function wipLoad(projectId)        { return readLS(wipKey(projectId), null); }
function wipClear(projectId) {
  try { if (typeof localStorage !== 'undefined') localStorage.removeItem(wipKey(projectId)); }
  catch (e) { /* storage unavailable */ }
}

/* ══════════════════════════════════════════════════════════════════════════
   Audit
   ══════════════════════════════════════════════════════════════════════════
   Every state change writes one of these. The organisation's real trail is
   server-side and hash-chained (Report & Govern → Audit Log); this is the
   module's own record of what was done to a stream, which is what the Monitor
   screen shows and what a replay has to be able to point at afterwards. */
function audit(state, action, streamId, detail) {
  var entry = {
    at: nowIso(state),
    action: action,
    streamId: streamId || null,
    actor: currentActor(),
    detail: detail || null,
  };
  state.audit.unshift(entry);
  state.audit = state.audit.slice(0, MAX_AUDIT);
  return entry;
}
function currentActor() {
  try {
    var u = JSON.parse(localStorage.getItem('cygenix_user') || 'null');
    return (u && (u.email || u.name)) || 'unknown';
  } catch (e) { return 'unknown'; }
}

/* The clock. Wall time drives display; the tick number drives arithmetic, so
   a machine whose clock jumps does not distort the figures. */
function nowIso(state) {
  var t = (state && state.clockNow) || Date.now();
  return new Date(t).toISOString();
}

/* ══════════════════════════════════════════════════════════════════════════
   Demo world
   ══════════════════════════════════════════════════════════════════════════
   Five streams, covering every status and both sides. The profile fields
   (baseRate/capacityRate) are what the tick runs on — they are the demo's
   physics, not display values. */
var DEMO_TABLES = {
  source: [
    { table: 'dbo.customers',  keys: ['id'],           rows: 2600,  sensitive: ['email'] },
    { table: 'dbo.addresses',  keys: ['address_id'],   rows: 4120,  sensitive: [] },
    { table: 'dbo.orders',     keys: ['order_id'],     rows: 88400, sensitive: [] },
    { table: 'dbo.order_line', keys: ['order_id', 'line_no'], rows: 412000, sensitive: [] },
    { table: 'dbo.matters',    keys: ['matter_id'],    rows: 15200, sensitive: [] },
  ],
  target: [
    { table: 'dbo.client',     keys: ['clid'],  rows: 2590,  sensitive: ['clemail'] },
    { table: 'dbo.matter',     keys: ['mid'],   rows: 15180, sensitive: [] },
    { table: 'dbo.ledger',     keys: ['lid'],   rows: 240500, sensitive: [] },
  ],
};

function demoObject(spec, i, streamId) {
  return {
    table: spec.table,
    keys: spec.keys.slice(),
    ops: ['I', 'U', 'D'],
    filter: i === 0 ? "status <> 'TEST'" : '',
    columnPolicy: spec.sensitive.length ? 'all-except' : 'all',
    excludedColumns: [],
    masked: spec.sensitive.slice(),
    rowsEstimate: spec.rows,
    state: 'streaming',
    eventsToday: randInt(120, 9000, ['demo-events', streamId, spec.table]),
  };
}

var DEMO_STREAMS = [
  { name: 'Source → Reporting warehouse', status: 'running', side: 'source', method: 'log',
    connectionLabel: 'LEGACY-SQL01 · dbo', destKind: 'database', destLabel: 'SNOWFLAKE-EU · RAW_LEGACY',
    writeMode: 'upsert', tables: 4, baseRate: 412, capacityRate: 520, retentionHours: 72 },
  { name: 'Source → Kafka change topic', status: 'lagging', side: 'source', method: 'log',
    connectionLabel: 'LEGACY-SQL01 · dbo', destKind: 'broker', destLabel: 'events.legacy.cdc',
    writeMode: 'append', tables: 5, baseRate: 980, capacityRate: 640, retentionHours: 48 },
  { name: 'Target → Legacy finance feed', status: 'running', side: 'target', method: 'poll',
    connectionLabel: 'AZSQL-TARGET · dbo', destKind: 'webhook', destLabel: 'https://finance.internal/hooks/cygenix',
    writeMode: 'append', tables: 2, baseRate: 96, capacityRate: 300, retentionHours: 24 },
  { name: 'Target → Data lake landing', status: 'paused', side: 'target', method: 'trigger',
    connectionLabel: 'AZSQL-TARGET · dbo', destKind: 'file', destLabel: 'adls://cygenix/landing/target',
    writeMode: 'audit-table', tables: 3, baseRate: 140, capacityRate: 400, retentionHours: 72 },
  { name: 'Source → Cutover delta into target', status: 'failed', side: 'source', method: 'log',
    connectionLabel: 'LEGACY-SQL01 · dbo', destKind: 'cygenix-target', destLabel: 'AZSQL-TARGET · dbo',
    writeMode: 'merge', tables: 2, baseRate: 260, capacityRate: 0, retentionHours: 72 },
];

function seedDemo(projectId, opts) {
  var o = opts || {};
  var state = emptyState(projectId);
  state.seeded = true;
  state.clockNow = o.now || null;
  var base = o.now || Date.parse('2026-08-24T09:41:07Z');

  DEMO_STREAMS.forEach(function (d, i) {
    var id = makeId('str', ['demo', projectId, i]);
    var pool = DEMO_TABLES[d.side];
    var objects = pool.slice(0, Math.min(d.tables, pool.length))
      .map(function (spec, j) { return demoObject(spec, j, id); });

    // Store depth is seeded from the profile rather than picked, so the
    // in/out/depth arithmetic is already consistent before the first tick.
    // Rounded, because this is a COUNT OF RECORDS. ratePerTick returns a rate
    // per 2s window and is fractional by nature; letting that reach the store
    // depth put "357.66666666666663 pending" on the Flow diagram.
    var perTickIn  = ratePerTick(d.baseRate);
    var perTickOut = ratePerTick(d.capacityRate);
    var pending = Math.round(
        d.status === 'lagging' ? perTickIn * 40
      : d.status === 'failed'  ? perTickIn * 25
      : d.status === 'paused'  ? 0
      : Math.max(0, perTickIn - perTickOut) * 10 + randInt(8, 140, ['pending', id]));

    state.streams.push({
      id: id,
      name: d.name,
      projectId: projectId || 'default',
      status: d.status,
      capture: {
        side: d.side,
        connectionId: 'conn_' + (d.side === 'source' ? 'src' : 'tgt') + '_01',
        connectionLabel: d.connectionLabel,
        method: d.method,
        methodLabel: methodLabel(d.method, d.connectionLabel),
        snapshot: 'initial-then-stream',
        position: { lsn: fakeLsn(id, 0), capturedAt: new Date(base).toISOString() },
      },
      destination: {
        kind: d.destKind,
        connectionId: 'conn_dest_' + i,
        label: d.destLabel,
        writeMode: d.writeMode,
        objectPrefix: d.destKind === 'database' ? 'stg_' : '',
      },
      objects: objects,
      delivery: {
        batchSize: 500, maxIntervalMs: 2000, ordering: 'per-key',
        retries: 5, backoff: 'exponential', dlq: true, onSchemaDrift: 'pause',
        maxEventsPerMin: 0, activeWindow: null, guardrail: 'confirm_destructive',
        lagThresholdSeconds: 30,
      },
      profile: { baseRate: d.baseRate, capacityRate: d.capacityRate, retentionHours: d.retentionHours },
      metrics: {
        eventsPerMin: isLive(d.status) ? d.baseRate : 0,
        lagSeconds: lagFromPending(pending, d.capacityRate),
        lagPeak24h: d.status === 'lagging' ? 68 : d.status === 'failed' ? 240 : 12,
        pendingInStore: pending,
        deliveredToday: randInt(4000, 210000, ['delivered', id]),
        failedToday: d.status === 'failed' ? randInt(2, 40, ['failed', id]) : 0,
        dlqDepth: d.status === 'failed' ? randInt(12, 60, ['dlq', id]) : 0,
        storeRecords: randInt(9000, 60000, ['records', id]),
        spark: [],
      },
      createdBy: currentActor(),
      createdAt: new Date(base - 13 * 86400000).toISOString(),
      startedAt: d.status === 'draft' ? null : new Date(base - 3.5 * 3600000).toISOString(),
      lastEventAt: isLive(d.status) ? new Date(base).toISOString() : new Date(base - 900000).toISOString(),
      lastError: d.status === 'failed'
        ? 'destination rejected: string truncation on stg_customers.name'
        : null,
    });
  });

  // A draft, so the list shows the "created but never started" state and the
  // Designer has something to open in edit mode.
  var draftId = makeId('str', ['demo', projectId, 'draft']);
  state.streams.push({
    id: draftId,
    name: 'Source → Client portal (draft)',
    projectId: projectId || 'default',
    status: 'draft',
    capture: { side: 'source', connectionId: 'conn_src_01', connectionLabel: 'LEGACY-SQL01 · dbo',
      method: 'poll', methodLabel: methodLabel('poll', 'LEGACY-SQL01'), snapshot: 'none',
      position: { lsn: null, capturedAt: null } },
    destination: { kind: 'webhook', connectionId: null, label: 'https://portal.example/hooks/changes',
      writeMode: 'append', objectPrefix: '' },
    objects: [demoObject(DEMO_TABLES.source[0], 0, draftId)],
    delivery: { batchSize: 200, maxIntervalMs: 5000, ordering: 'per-key', retries: 3,
      backoff: 'exponential', dlq: true, onSchemaDrift: 'pause', maxEventsPerMin: 0,
      activeWindow: null, guardrail: 'confirm_all', lagThresholdSeconds: 60 },
    profile: { baseRate: 40, capacityRate: 200, retentionHours: 24 },
    metrics: { eventsPerMin: 0, lagSeconds: 0, lagPeak24h: 0, pendingInStore: 0, deliveredToday: 0,
      failedToday: 0, dlqDepth: 0, storeRecords: 0, spark: [] },
    createdBy: currentActor(),
    createdAt: new Date(base - 2 * 86400000).toISOString(),
    startedAt: null,
    lastEventAt: null,
    lastError: null,
  });

  // Prime the sparklines and the chart history so the first paint is not a
  // flat line waiting twelve ticks to mean something.
  for (var t = 0; t < 12; t++) tick(state, { silent: true, at: base - (12 - t) * TICK_MS });
  audit(state, 'demo.seeded', null, { streams: state.streams.length });
  return state;
}

function methodLabel(method, connLabel) {
  var engine = /postgres/i.test(connLabel || '') ? 'PostgreSQL'
             : /mysql/i.test(connLabel || '')    ? 'MySQL'
             : 'SQL Server';
  if (method === 'log')     return 'Log-based CDC (' + engine + ')';
  if (method === 'poll')    return 'Timestamp/rowversion polling';
  if (method === 'trigger') return 'Shadow-table triggers';
  return 'External feed';
}
function fakeLsn(streamId, tickNo) {
  var a = hash32(streamId + '|lsn-a|' + tickNo).toString(16).toUpperCase();
  var b = hash32(streamId + '|lsn-b|' + tickNo).toString(16).toUpperCase();
  return '0x' + a.padStart(8, '0').slice(0, 8) + ':' + b.padStart(8, '0').slice(0, 8) + ':0003';
}

/* ══════════════════════════════════════════════════════════════════════════
   The tick
   ══════════════════════════════════════════════════════════════════════════
   ONE invariant governs every number on every screen:

       pendingInStore(after) = pendingInStore(before) + captured - delivered

   Lag, throughput, store depth and the Flow view's bottleneck arrow are all
   read off that. Getting it exactly right — not approximately — is what stops
   the Monitor and the Streams list telling different stories. */
function ratePerTick(perMin) { return (perMin || 0) * (TICK_MS / 60000); }
function lagFromPending(pending, capacityPerMin) {
  if (!pending) return 0;
  if (!capacityPerMin) return Math.min(9999, Math.round(pending / 2));  // nothing draining
  return Math.round(pending / (capacityPerMin / 60));
}

function tick(state, opts) {
  var o = opts || {};
  var at = o.at || Date.now();
  state.clockNow = at;
  state.tickNo = (state.tickNo || 0) + 1;
  var n = state.tickNo;
  var newEvents = [];

  state.streams.forEach(function (s) {
    var m = s.metrics, p = s.profile || { baseRate: 0, capacityRate: 0 };
    var capturedThisTick = 0;

    if (!isLive(s.status)) {
      // Paused/failed/stopped: capture stops, and so does the arithmetic. A
      // paused stream keeps whatever is already in the store — that backlog
      // is the whole reason pausing is safe.
      if (s.status === 'failed') {
        // Capture continues, delivery does not: the backlog and the
        // dead-letter queue are what the operator has to see.
        capturedThisTick = Math.round(ratePerTick(p.baseRate) * (0.7 + rand(n, s.id, 'jitter') * 0.6));
        m.pendingInStore += capturedThisTick;
        m.storeRecords += capturedThisTick;
        if (n % 5 === 0) { m.dlqDepth += 1; m.failedToday += 1; }
        m.eventsPerMin = p.baseRate;
        m.lagSeconds = lagFromPending(m.pendingInStore, p.capacityRate);
        s.lastEventAt = new Date(at).toISOString();
        s.capture.position = { lsn: fakeLsn(s.id, n), capturedAt: new Date(at).toISOString() };
        pushSpark(m, capturedThisTick);
        // A failed stream still emits change records — they are exactly the
        // ones an operator needs to look at. Without them the dead-letter
        // count on the Streams screen would have nothing behind it on the
        // Change Events screen, and "23 events · destination rejected …"
        // would be a number with no records to inspect or requeue.
        var emitF = Math.min(4, capturedThisTick);
        for (var k = 0; k < emitF; k++) newEvents.push(makeEvent(state, s, k, at, true));
      } else {
        m.eventsPerMin = 0;
        pushSpark(m, 0);
      }
      // The metric point reports what actually happened, including on a
      // failed stream — it captured and delivered nothing, and the Monitor's
      // "in vs out" chart has to show exactly that gap. Reporting 0 in would
      // hide a stream that is filling its store with undeliverable records.
      pushPoint(state, s, capturedThisTick, 0, at);
      return;
    }

    // Captured this tick — jittered around the profile rate.
    var inN = Math.max(0, Math.round(ratePerTick(p.baseRate) * (0.6 + rand(n, s.id, 'in') * 0.8)));

    // Snapshotting streams also drain their backlog of existing rows.
    if (s.status === 'snapshotting') {
      s.snapshotDone = (s.snapshotDone || 0) + Math.round(snapshotRows(s) * 0.04);
      if (s.snapshotDone >= snapshotRows(s)) {
        s.snapshotDone = snapshotRows(s);
        s.status = 'running';
        s.objects.forEach(function (ob) { ob.state = 'streaming'; });
        audit(state, 'stream.snapshot_complete', s.id, { rows: s.snapshotDone });
      }
    }

    // Delivered this tick — never more than is actually in the store.
    var capacity = Math.round(ratePerTick(p.capacityRate));
    var available = m.pendingInStore + inN;
    var outN = Math.min(available, capacity);

    m.pendingInStore = available - outN;              // the invariant, exactly
    m.storeRecords += inN;
    m.deliveredToday += outN;
    m.eventsPerMin = Math.round(inN * (60000 / TICK_MS));
    m.lagSeconds = lagFromPending(m.pendingInStore, p.capacityRate);
    if (m.lagSeconds > m.lagPeak24h) m.lagPeak24h = m.lagSeconds;
    s.lastEventAt = new Date(at).toISOString();
    s.capture.position = { lsn: fakeLsn(s.id, n), capturedAt: new Date(at).toISOString() };
    pushSpark(m, inN);

    // A stream is "lagging" when it is past its own threshold, and stops
    // being so when it recovers. The status is derived, never set by hand,
    // so a resumed stream announces its own recovery.
    var thr = (s.delivery && s.delivery.lagThresholdSeconds) || 30;
    if (s.status === 'running' && m.lagSeconds > thr) {
      s.status = 'lagging';
    } else if (s.status === 'lagging' && m.lagSeconds <= thr) {
      s.status = 'running';
      audit(state, 'stream.recovered', s.id, { lagSeconds: m.lagSeconds });
    }

    // Change records. Emitting one per captured event would flood the tail
    // for no gain, so emit a sample and say so on the screen.
    var emit = Math.min(8, inN);
    for (var i = 0; i < emit; i++) newEvents.push(makeEvent(state, s, i, at, outN > 0));
    pushPoint(state, s, inN, outN, at);
  });

  if (newEvents.length) {
    state.events = newEvents.concat(state.events).slice(0, MAX_EVENTS);
  }
  return { events: newEvents, tickNo: n };
}

function snapshotRows(s) {
  return (s.objects || []).reduce(function (a, o) { return a + (o.rowsEstimate || 0); }, 0) || 1;
}
function pushSpark(m, v) {
  if (!Array.isArray(m.spark)) m.spark = [];
  m.spark.push(v);
  if (m.spark.length > 12) m.spark = m.spark.slice(-12);
}
function pushPoint(state, s, inN, outN, at) {
  state.points.push({
    t: new Date(at).toISOString(), streamId: s.id,
    in: inN, out: outN,
    lagSeconds: s.metrics.lagSeconds, errors: s.status === 'failed' ? 1 : 0,
    storeDepth: s.metrics.pendingInStore,
  });
  if (state.points.length > MAX_POINTS) state.points = state.points.slice(-MAX_POINTS);
}

function makeEvent(state, s, i, at, delivering) {
  var n = state.tickNo;
  var obj = s.objects[randInt(0, Math.max(0, s.objects.length - 1), ['obj', s.id, n, i])];
  if (!obj) obj = { table: 'unknown', keys: ['id'], masked: [] };
  var op = pick(['I', 'U', 'U', 'U', 'D'], ['op', s.id, n, i]);
  var keyVal = randInt(10000, 99999, ['key', s.id, n, i]);
  var key = {}; key[obj.keys[0] || 'id'] = keyVal;

  var before = op === 'I' ? null : sampleRow(s, obj, n, i, 'before');
  var after  = op === 'D' ? null : sampleRow(s, obj, n, i, 'after');
  var changed = [];
  if (before && after) {
    Object.keys(after).forEach(function (k) { if (before[k] !== after[k]) changed.push(k); });
  }

  var deliveryState = !delivering ? 'pending'
    : (s.status === 'failed' ? 'dead'
    : (rand(n, s.id, 'fail', i) < 0.02 ? 'retrying' : 'delivered'));

  return {
    id: 'evt_' + String(hash32(s.id + '|' + n + '|' + i)).slice(0, 8),
    streamId: s.id,
    ts: new Date(at + i).toISOString(),
    table: obj.table,
    op: op,
    key: key,
    position: fakeLsn(s.id, n),
    before: before,
    after: after,
    changedColumns: changed,
    deliveryState: deliveryState,
    attempts: deliveryState === 'retrying' ? 2 : 1,
    latencyMs: randInt(120, 1800, ['lat', s.id, n, i]),
    error: deliveryState === 'dead' ? (s.lastError || 'destination rejected the record') : null,
  };
}

var DEMO_NAMES = ['Acme Ltd', 'Acme Holdings Ltd', 'Northgate Ltd', 'Baxter & Co', 'Pinehurst LLP'];
var DEMO_COUNTRIES = ['GB', 'IE', 'FR', 'DE', 'NL'];
var DEMO_STATUS = ['ACTIVE', 'ACTIVE', 'ACTIVE', 'DORMANT'];
function sampleRow(s, obj, n, i, which) {
  var salt = which === 'after' ? 1 : 0;
  var row = {
    name:    pick(DEMO_NAMES,     ['nm', s.id, n, i, salt]),
    country: pick(DEMO_COUNTRIES, ['co', s.id, n, i]),
    status:  pick(DEMO_STATUS,    ['st', s.id, n, i]),
  };
  // Masking is applied at capture, so a masked column never reaches the
  // event payload in readable form — including in this tail.
  (obj.masked || []).forEach(function (c) { row[c] = '••••••'; });
  return row;
}

/* ══════════════════════════════════════════════════════════════════════════
   Derived reads
   ══════════════════════════════════════════════════════════════════════════ */

/* The KPI strip. `errorsLastHour` counts what actually failed rather than
   how many streams are unwell — an operator needs the record count. */
function kpis(state) {
  var streams = state.streams || [];
  var live = streams.filter(function (s) { return isLive(s.status); });
  var paused = streams.filter(function (s) { return s.status === 'paused'; });
  var failed = streams.filter(function (s) { return s.status === 'failed'; });
  var eventsPerMin = live.reduce(function (a, s) { return a + (s.metrics.eventsPerMin || 0); }, 0);

  var worst = null;
  live.concat(failed).forEach(function (s) {
    if (!worst || s.metrics.lagSeconds > worst.metrics.lagSeconds) worst = s;
  });

  return {
    running: live.length,
    total: streams.length,
    paused: paused.length,
    failed: failed.length,
    eventsPerMin: eventsPerMin,
    maxLagSeconds: worst ? worst.metrics.lagSeconds : 0,
    worstStream: worst ? worst.name : null,
    errorsLastHour: streams.reduce(function (a, s) { return a + (s.metrics.failedToday || 0); }, 0),
    dlqDepth: streams.reduce(function (a, s) { return a + (s.metrics.dlqDepth || 0); }, 0),
  };
}

/* Sort by how much attention a stream needs, then by lag. A screen that
   sorted alphabetically would bury the failed stream. */
var SEVERITY = { failed: 0, lagging: 1, snapshotting: 2, running: 3, paused: 4, stopped: 5, draft: 6 };
function sortStreams(list) {
  return list.slice().sort(function (a, b) {
    var d = (SEVERITY[a.status] === undefined ? 9 : SEVERITY[a.status])
          - (SEVERITY[b.status] === undefined ? 9 : SEVERITY[b.status]);
    if (d) return d;
    return (b.metrics.lagSeconds || 0) - (a.metrics.lagSeconds || 0);
  });
}

function filterStreams(list, f) {
  var q = f || {};
  var text = String(q.text || '').trim().toLowerCase();
  return list.filter(function (s) {
    if (q.status && q.status !== 'all') {
      if (q.status === 'running' && !isLive(s.status)) return false;
      if (q.status !== 'running' && s.status !== q.status) return false;
    }
    if (q.side && q.side !== 'any' && s.capture.side !== q.side) return false;
    if (q.destKind && q.destKind !== 'any' && s.destination.kind !== q.destKind) return false;
    if (q.errorsOnly && !(s.metrics.failedToday > 0 || s.status === 'failed')) return false;
    if (text) {
      var hay = [s.name, s.capture.connectionLabel, s.destination.label]
        .concat((s.objects || []).map(function (o) { return o.table; }))
        .join(' ').toLowerCase();
      if (hay.indexOf(text) === -1) return false;
    }
    return true;
  });
}

/* The Flow view, as data. Three nodes and two arrows; the bottleneck is
   whichever hop cannot keep up, and it is a fact about the rates rather than
   a colour someone chose. */
function flowOf(s) {
  var inRate = s.metrics.eventsPerMin || 0;
  var outRate = Math.round((s.profile && s.profile.capacityRate) || 0);
  var live = isLive(s.status);
  var bottleneck = null;
  if (s.status === 'failed') bottleneck = 'delivery';
  else if (live && outRate < inRate) bottleneck = 'delivery';
  else if (live && s.status === 'snapshotting') bottleneck = 'capture';

  return {
    capture: {
      label: s.capture.connectionLabel,
      side: s.capture.side,
      rate: live || s.status === 'failed' ? inRate : 0,
      method: s.capture.methodLabel,
    },
    store: {
      label: 'Stream Store',
      pending: s.metrics.pendingInStore,
      retentionHours: (s.profile && s.profile.retentionHours) || 72,
      records: s.metrics.storeRecords,
    },
    destination: {
      label: s.destination.label,
      kind: s.destination.kind,
      writeMode: s.destination.writeMode,
      rate: s.status === 'failed' ? 0 : (live ? Math.min(outRate, inRate + s.metrics.pendingInStore) : 0),
    },
    bottleneck: bottleneck,
  };
}

/* The Stream Store, per topic. One topic per streamed object — that is what
   makes the store inspectable rather than an abstraction.

   The topic name carries the STREAM as well as the side and the table.
   Without it, two streams reading dbo.customers from the same source — one
   into a warehouse, one into a broker, which is an ordinary thing to want —
   would both be called src.dbo.customers, and a topic name would no longer
   identify a checkpoint, a consumer or a retention window. A real broker
   namespaces per connector for the same reason. */
function topicsOf(state) {
  var out = [];
  (state.streams || []).forEach(function (s) {
    var prefix = (s.capture.side === 'source' ? 'src' : 'tgt')
      + '.' + String(s.id).replace(/^str_/, '');
    var share = s.objects.length || 1;
    s.objects.forEach(function (o, i) {
      var records = Math.round((s.metrics.storeRecords || 0) / share);
      var pending = Math.round((s.metrics.pendingInStore || 0) / share);
      var retention = (s.profile && s.profile.retentionHours) || 72;
      var newest = s.lastEventAt || s.createdAt;
      out.push({
        streamId: s.id,
        streamName: s.name,
        streamStatus: s.status,
        topic: prefix + '.' + o.table,
        table: o.table,
        retentionHours: retention,
        records: records,
        bytes: records * 435,          // demo record size; a real store measures it
        oldestRecordAt: newest ? new Date(Date.parse(newest) - retention * 3600000).toISOString() : null,
        newestRecordAt: newest,
        checkpoint: s.capture.position.lsn,
        consumers: [{ name: 'delivery:' + (s.destination.connectionId || s.destination.kind),
                      offsetBehind: pending }],
      });
    });
  });
  return out;
}

function storeTotals(state) {
  var topics = topicsOf(state);
  var records = topics.reduce(function (a, t) { return a + t.records; }, 0);
  var bytes = topics.reduce(function (a, t) { return a + t.bytes; }, 0);
  var retentions = topics.map(function (t) { return t.retentionHours; });
  var oldest = topics.map(function (t) { return t.oldestRecordAt; })
    .filter(Boolean).sort()[0] || null;
  return {
    topics: topics.length,
    records: records,
    bytes: bytes,
    minRetentionHours: retentions.length ? Math.min.apply(null, retentions) : 0,
    maxRetentionHours: retentions.length ? Math.max.apply(null, retentions) : 0,
    oldestRecordAt: oldest,
  };
}

/* Alert rules. Each one is a predicate over live state, evaluated on the
   tick, so an alert is never stale relative to the numbers beside it. */
var ALERT_RULES = [
  { id: 'lag',   label: 'Lag over 60s for 5 minutes',
    test: function (s) { return isLive(s.status) && s.metrics.lagSeconds > 60; } },
  { id: 'dlq',   label: 'Dead-letter queue is not empty',
    test: function (s) { return (s.metrics.dlqDepth || 0) > 0; } },
  { id: 'quiet', label: 'No events for 30 minutes on an active stream',
    test: function (s, now) {
      if (!isLive(s.status) || !s.lastEventAt) return false;
      return (now - Date.parse(s.lastEventAt)) > 30 * 60000;
    } },
  { id: 'drift', label: 'Schema drift detected',
    test: function (s) { return !!s.schemaDrift; } },
];
function alertsOf(state, now) {
  var t = now || state.clockNow || Date.now();
  var out = [];
  ALERT_RULES.forEach(function (rule) {
    var firing = (state.streams || []).filter(function (s) { return rule.test(s, t); });
    out.push({
      id: rule.id,
      label: rule.label,
      state: firing.length ? 'firing' : 'ok',
      count: firing.length,
      streams: firing.map(function (s) { return { id: s.id, name: s.name }; }),
      lastFiredAt: firing.length ? new Date(t).toISOString() : null,
    });
  });
  return out;
}

/* Per-table × time-bucket volume, for the Monitor's heat grid. A hot table
   is the thing a heat grid exists to make obvious. */
function heatGrid(state, bucketMs, buckets) {
  var size = bucketMs || 300000;
  var count = buckets || 12;
  var now = state.clockNow || Date.now();
  var end = Math.ceil(now / size) * size;
  var byTable = {};
  (state.events || []).forEach(function (e) {
    var idx = count - 1 - Math.floor((end - Date.parse(e.ts)) / size);
    if (idx < 0 || idx >= count) return;
    if (!byTable[e.table]) byTable[e.table] = new Array(count).fill(0);
    byTable[e.table][idx]++;
  });
  var rows = Object.keys(byTable).sort().map(function (t) {
    return { table: t, cells: byTable[t], total: byTable[t].reduce(function (a, b) { return a + b; }, 0) };
  }).sort(function (a, b) { return b.total - a.total; });
  var peak = rows.reduce(function (m, r) {
    return Math.max(m, Math.max.apply(null, r.cells)); }, 0);
  return { buckets: count, bucketMs: size, endsAt: end, rows: rows, peak: peak };
}

/* ══════════════════════════════════════════════════════════════════════════
   The plain-English review sentence (Designer step 5)
   ══════════════════════════════════════════════════════════════════════════
   The config in a sentence, because a five-step form is easy to misread and
   a paragraph is not. */
function reviewSentence(d) {
  if (!d) return '';
  var tables = (d.objects || []).length;
  var masked = (d.objects || []).reduce(function (a, o) { return a + (o.masked || []).length; }, 0);
  var deletes = (d.objects || []).some(function (o) { return (o.ops || []).indexOf('D') !== -1; });
  var retention = (d.profile && d.profile.retentionHours) || 72;
  var batch = (d.delivery && d.delivery.batchSize) || 500;
  var every = Math.round(((d.delivery && d.delivery.maxIntervalMs) || 2000) / 1000);

  var parts = [];
  parts.push('Reads changes from ' + (d.capture.connectionLabel || 'the selected connection')
    + ' (' + d.capture.side + ') using ' + (METHOD_LABEL[d.capture.method] || d.capture.method).toLowerCase());
  parts.push(d.capture.method === 'feed'
    ? 'from the ' + (d.capture.feedName || 'external') + ' feed'
    : 'across ' + tables + ' ' + (tables === 1 ? 'table' : 'tables'));
  parts.push('holds them for ' + retention + ' hours');
  parts.push('and ' + verbFor(d.destination.writeMode) + ' them into '
    + (d.destination.label || 'the destination')
    + ' in batches of ' + batch + ' or every ' + every + ' second' + (every === 1 ? '' : 's') + '.');

  var sentence = parts.join(', ').replace(', and ', ' and ');
  sentence += deletes ? ' Deletes are applied.' : ' Deletes are not captured.';
  if (masked) sentence += ' ' + masked + ' column' + (masked === 1 ? ' is' : 's are') + ' masked.';
  return sentence;
}
function verbFor(mode) {
  return mode === 'append' ? 'appends'
       : mode === 'merge' ? 'merges'
       : mode === 'audit-table' ? 'records'
       : 'upserts';
}

/* Snapshot cost, so "Create & start" is not a blind click on a production
   database. Rows are the estimate the Schema Explorer already produced. */
function snapshotEstimate(d) {
  if (!d || d.capture.snapshot === 'none') return { rows: 0, minutes: 0, note: 'No initial snapshot — new changes only.' };
  var rows = (d.objects || []).reduce(function (a, o) { return a + (o.rowsEstimate || 0); }, 0);
  var perMin = 250000;                       // demo throughput for the estimate
  var minutes = Math.max(1, Math.ceil(rows / perMin));
  return {
    rows: rows, minutes: minutes,
    note: rows.toLocaleString() + ' rows to snapshot before streaming begins — roughly '
      + minutes + ' minute' + (minutes === 1 ? '' : 's') + ' of read load on '
      + (d.capture.connectionLabel || 'the source') + '.',
  };
}

/* ══════════════════════════════════════════════════════════════════════════
   Capability check (Designer step 1)
   ══════════════════════════════════════════════════════════════════════════
   Never silently downgrade a capture method. If the log is unavailable the
   check says so and offers the two honest remedies: enable it, or pick
   polling deliberately. */
function capabilityCheck(conn, method) {
  var label = (conn && (conn.label || conn.name)) || 'this connection';
  var engine = /postgres/i.test(label) ? 'postgres' : /mysql/i.test(label) ? 'mysql' : 'mssql';
  var cdcOn = !!(conn && conn.cdcEnabled);

  if (method === 'feed') {
    return { ok: true, method: 'feed', findings: [
      { level: 'info', text: 'An external feed does not read a database — Cygenix receives payloads you post to it.' }] };
  }
  if (method !== 'log') {
    return { ok: true, method: method, findings: [
      { level: 'info', text: METHOD_LABEL[method] + ' works against ' + label + ' without extra configuration.' },
      method === 'poll'
        ? { level: 'warn', text: 'Polling cannot see hard deletes. Rows removed from the source stay in the destination.' }
        : { level: 'warn', text: 'Triggers add write cost to every transaction on the captured tables.' },
    ] };
  }
  if (cdcOn) {
    return { ok: true, method: 'log', findings: [
      { level: 'ok', text: 'Log-based CDC is available on ' + label + '.' }] };
  }
  return {
    ok: false, method: 'log',
    findings: [{ level: 'error', text: 'CDC is not enabled on ' + label + '.' }],
    remedies: [
      { id: 'sql', label: 'Show me the SQL to enable it', sql: enableCdcSql(engine) },
      { id: 'poll', label: 'Use polling instead', method: 'poll' },
    ],
  };
}
function enableCdcSql(engine) {
  if (engine === 'postgres') {
    return '-- Log-based capture on PostgreSQL uses a logical replication slot.\n'
      + '-- Requires wal_level = logical (a server restart) and REPLICATION rights.\n'
      + "ALTER SYSTEM SET wal_level = 'logical';\n"
      + "SELECT pg_create_logical_replication_slot('cygenix_stream', 'pgoutput');";
  }
  if (engine === 'mysql') {
    return '-- Log-based capture on MySQL reads the binary log.\n'
      + "SET GLOBAL binlog_format = 'ROW';\n"
      + "SET GLOBAL binlog_row_image = 'FULL';\n"
      + "-- The capture account needs REPLICATION SLAVE, REPLICATION CLIENT.";
  }
  return '-- Enable Change Data Capture on the database, then on each table.\n'
    + '-- Requires sysadmin for the database step and db_owner for the tables.\n'
    + 'EXEC sys.sp_cdc_enable_db;\n\n'
    + 'EXEC sys.sp_cdc_enable_table\n'
    + "  @source_schema = N'dbo',\n"
    + "  @source_name   = N'customers',\n"
    + '  @role_name     = NULL,\n'
    + '  @supports_net_changes = 1;';
}

/* ══════════════════════════════════════════════════════════════════════════
   Designer validation
   ══════════════════════════════════════════════════════════════════════════
   Blocks only where a step is genuinely invalid; everything else is a warning
   the operator can accept. A form that blocks on judgement calls gets fought
   rather than read. */
function validateDesign(d) {
  var steps = [[], [], [], [], []];
  var warn = [];
  if (!d) return { errors: steps, warnings: warn, valid: false, firstInvalidStep: 1 };

  // 1 — source of change
  if (!d.capture || !d.capture.connectionId) steps[0].push('Pick the connection to read changes from.');
  if (!d.capture || !d.capture.method) steps[0].push('Pick a capture method.');
  if (d.capture && d.capture.method === 'feed' && !d.capture.feedName) {
    steps[0].push('Name the external feed.');
  }

  // 2 — objects
  var isFeed = d.capture && d.capture.method === 'feed';
  if (!isFeed && !(d.objects || []).length) steps[1].push('Select at least one table to stream.');
  (d.objects || []).forEach(function (o) {
    var upsertish = d.destination && (d.destination.writeMode === 'upsert' || d.destination.writeMode === 'merge');
    if (upsertish && !(o.keys || []).length) {
      steps[1].push(o.table + ' has no key column, which ' + d.destination.writeMode + ' needs.');
    }
    if (!(o.keys || []).length) warn.push(o.table + ' has no primary key — every change will be appended.');
    if ((o.rowsEstimate || 0) > 1000000 && d.capture.snapshot === 'initial-then-stream') {
      warn.push(o.table + ' holds ' + (o.rowsEstimate).toLocaleString()
        + ' rows; snapshotting it first will take a while.');
    }
    if ((o.masked || []).length === 0 && (o.sensitiveColumns || []).length) {
      warn.push(o.table + ' carries a column classified as sensitive with no mask on it.');
    }
    if (!(o.ops || []).length) steps[1].push(o.table + ' has no operations selected.');
  });

  // 3 — destination
  if (!d.destination || !d.destination.kind) steps[2].push('Pick where the changes go.');
  if (d.destination && !d.destination.label) steps[2].push('Pick or name the destination.');

  // 4 — delivery
  var del = d.delivery || {};
  if (!(del.batchSize > 0)) steps[3].push('Batch size must be at least 1.');
  if (!(del.maxIntervalMs >= 250)) steps[3].push('Flush interval must be at least 250ms.');
  if (del.retries < 0) steps[3].push('Retries cannot be negative.');

  // 5 — review (nothing of its own; it is valid when 1–4 are)
  if (!d.name || !String(d.name).trim()) steps[4].push('Give the stream a name.');

  var firstInvalid = 0;
  for (var i = 0; i < steps.length; i++) { if (steps[i].length) { firstInvalid = i + 1; break; } }
  return {
    errors: steps, warnings: warn,
    valid: firstInvalid === 0,
    firstInvalidStep: firstInvalid || null,
  };
}

/* A blank draft, with the defaults the brief calls for. */
function blankDraft(projectId) {
  return {
    id: null,
    name: '',
    projectId: projectId || 'default',
    status: 'draft',
    capture: { side: 'source', connectionId: null, connectionLabel: '', method: 'log',
      methodLabel: '', snapshot: 'initial-then-stream', feedName: '', feedShape: '', feedKeyPath: '',
      position: { lsn: null, capturedAt: null } },
    destination: { kind: 'database', connectionId: null, label: '', writeMode: 'upsert',
      objectPrefix: 'stg_', schemaOverride: '', mappingId: null, overrides: {} },
    objects: [],
    delivery: { batchSize: 500, maxIntervalMs: 2000, ordering: 'per-key', retries: 5,
      backoff: 'exponential', dlq: true, onSchemaDrift: 'pause', maxEventsPerMin: 0,
      activeWindow: null, guardrail: 'confirm_destructive', lagThresholdSeconds: 30 },
    profile: { baseRate: 120, capacityRate: 400, retentionHours: 72 },
  };
}

/* The destination name a table resolves to, shown as a preview in step 3 so
   the naming rules are visible before they are applied. */
function resolveObjectName(d, table) {
  var bare = String(table || '').split('.').pop();
  var dest = d.destination || {};
  if (dest.overrides && dest.overrides[table]) return dest.overrides[table];
  var schema = dest.schemaOverride || (dest.kind === 'database' ? 'RAW' : '');
  var name = (dest.objectPrefix || '') + bare + (dest.objectSuffix || '');
  return schema ? schema + '.' + name : name;
}

/* ══════════════════════════════════════════════════════════════════════════
   Actions — every one of them audited
   ══════════════════════════════════════════════════════════════════════════
   These are the only functions that change a stream. Confirmation is the
   caller's job (it is a UI concern), but the audit entry is not — leaving it
   to five screens would mean four of them eventually forgetting. */
function createStream(state, draft, opts) {
  var o = opts || {};
  var v = validateDesign(draft);
  if (!v.valid) throw new Error('This stream is not ready: ' + (v.errors[(v.firstInvalidStep || 1) - 1] || [])[0]);

  var s = JSON.parse(JSON.stringify(draft));
  s.id = s.id || makeId('str', ['new', state.projectId, state.tickNo, draft.name]);
  s.projectId = state.projectId;
  s.createdBy = currentActor();
  s.createdAt = nowIso(state);
  s.startedAt = null;
  s.lastEventAt = null;
  s.lastError = null;
  s.capture.methodLabel = s.capture.methodLabel || methodLabel(s.capture.method, s.capture.connectionLabel);
  s.metrics = { eventsPerMin: 0, lagSeconds: 0, lagPeak24h: 0, pendingInStore: 0,
    deliveredToday: 0, failedToday: 0, dlqDepth: 0, storeRecords: 0, spark: [] };
  s.status = 'draft';
  s.objects = (s.objects || []).map(function (ob) {
    return Object.assign({ state: 'paused', eventsToday: 0 }, ob); });

  state.streams.push(s);
  audit(state, 'stream.created', s.id, { name: s.name, side: s.capture.side,
    method: s.capture.method, destination: s.destination.kind, objects: s.objects.length });
  if (o.start) startStream(state, s.id);
  return s;
}

function updateStream(state, id, draft) {
  var i = state.streams.findIndex(function (s) { return s.id === id; });
  if (i < 0) throw new Error('No stream with id ' + id + '.');
  var was = state.streams[i];
  var next = JSON.parse(JSON.stringify(draft));
  // Identity, history and live counters belong to the stream, not the draft.
  next.id = was.id;
  next.projectId = was.projectId;
  next.createdAt = was.createdAt;
  next.createdBy = was.createdBy;
  next.startedAt = was.startedAt;
  next.lastEventAt = was.lastEventAt;
  next.metrics = was.metrics;
  next.status = was.status;
  state.streams[i] = next;
  audit(state, 'stream.updated', id, { name: next.name });
  return next;
}

function getStream(state, id) {
  return (state.streams || []).filter(function (s) { return s.id === id; })[0] || null;
}

function startStream(state, id) {
  var s = mustGet(state, id);
  var snapshotting = s.capture.snapshot === 'initial-then-stream' || s.capture.snapshot === 'snapshot-only';
  s.status = snapshotting ? 'snapshotting' : 'running';
  s.snapshotDone = 0;
  s.startedAt = nowIso(state);
  s.lastError = null;
  s.objects.forEach(function (o) { o.state = snapshotting ? 'snapshotting' : 'streaming'; });
  audit(state, 'stream.started', id, { snapshot: s.capture.snapshot });
  return s;
}
function pauseStream(state, id) {
  var s = mustGet(state, id);
  s.status = 'paused';
  s.metrics.eventsPerMin = 0;
  s.objects.forEach(function (o) { o.state = 'paused'; });
  audit(state, 'stream.paused', id, { pendingInStore: s.metrics.pendingInStore });
  return s;
}
function resumeStream(state, id) {
  var s = mustGet(state, id);
  s.status = 'running';
  s.objects.forEach(function (o) { o.state = 'streaming'; });
  audit(state, 'stream.resumed', id, { pendingInStore: s.metrics.pendingInStore });
  return s;
}
function stopStream(state, id) {
  var s = mustGet(state, id);
  s.status = 'stopped';
  s.metrics.eventsPerMin = 0;
  s.objects.forEach(function (o) { o.state = 'paused'; });
  audit(state, 'stream.stopped', id, null);
  return s;
}
function deleteStream(state, id) {
  var s = mustGet(state, id);
  state.streams = state.streams.filter(function (x) { return x.id !== id; });
  state.events = state.events.filter(function (e) { return e.streamId !== id; });
  state.points = state.points.filter(function (p) { return p.streamId !== id; });
  audit(state, 'stream.deleted', id, { name: s.name });
  return true;
}
function duplicateStream(state, id) {
  var s = mustGet(state, id);
  var copy = JSON.parse(JSON.stringify(s));
  copy.id = null;
  copy.name = s.name + ' (copy)';
  copy.status = 'draft';
  return copy;
}

/* Resync one object: re-snapshot that table without disturbing the others.
   The point of naming the object is that a single bad table does not cost a
   full re-snapshot of the stream. */
function resyncObject(state, id, table) {
  var s = mustGet(state, id);
  var o = (s.objects || []).filter(function (x) { return x.table === table; })[0];
  if (!o) throw new Error(table + ' is not in this stream.');
  o.state = 'snapshotting';
  o.snapshotDone = 0;
  if (s.status === 'paused' || s.status === 'stopped') s.status = 'snapshotting';
  audit(state, 'stream.resync', id, { table: table, rows: o.rowsEstimate || null });
  return o;
}

/* Replay. Always a destructive-class action: it re-delivers records that were
   already delivered once, and the caller must have said what that costs. */
function replayCount(state, id, from) {
  var s = mustGet(state, id);
  var retention = (s.profile && s.profile.retentionHours) || 72;
  var records = s.metrics.storeRecords || 0;
  if (!from || from.kind === 'retention') return records;
  if (from.kind === 'time') {
    var now = state.clockNow || Date.now();
    var span = Math.max(0, now - Date.parse(from.at || ''));
    var frac = Math.min(1, span / (retention * 3600000));
    return Math.round(records * frac);
  }
  if (from.kind === 'position') return Math.round(records * 0.25);
  return records;
}
function replay(state, id, from) {
  var s = mustGet(state, id);
  var count = replayCount(state, id, from);
  s.metrics.pendingInStore += count;
  s.metrics.lagSeconds = lagFromPending(s.metrics.pendingInStore, (s.profile || {}).capacityRate);
  audit(state, 'stream.replay', id, { from: from || { kind: 'retention' }, records: count,
    destination: s.destination.label });
  return { records: count, destination: s.destination.label };
}

/* Dead letters back into the queue. They are re-delivered, so this is a
   change to the destination and confirmed like one. */
function requeueDlq(state, id, n) {
  var s = mustGet(state, id);
  var count = n == null ? s.metrics.dlqDepth : Math.min(n, s.metrics.dlqDepth);
  if (!count) return { records: 0 };
  s.metrics.dlqDepth -= count;
  s.metrics.pendingInStore += count;
  state.events.forEach(function (e) {
    if (e.streamId === id && e.deliveryState === 'dead' && count-- > 0) {
      e.deliveryState = 'retrying';
      e.attempts = (e.attempts || 1) + 1;
    }
  });
  audit(state, 'stream.dlq_requeue', id, { records: n == null ? 'all' : n });
  return { records: n == null ? s.metrics.dlqDepth : n, destination: s.destination.label };
}

function redeliverEvent(state, eventId) {
  var e = (state.events || []).filter(function (x) { return x.id === eventId; })[0];
  if (!e) throw new Error('That event is no longer in the retained tail.');
  e.deliveryState = 'retrying';
  e.attempts = (e.attempts || 1) + 1;
  audit(state, 'event.redeliver', e.streamId, { event: eventId, table: e.table });
  return e;
}

/* Retention. Shortening it destroys replay history, so the caller is told
   exactly how much before it is applied. */
function retentionImpact(state, id, hours) {
  var s = mustGet(state, id);
  var current = (s.profile && s.profile.retentionHours) || 72;
  if (hours >= current) return { losesHistory: false, hoursLost: 0, recordsLost: 0 };
  var frac = (current - hours) / current;
  return {
    losesHistory: true,
    hoursLost: current - hours,
    recordsLost: Math.round((s.metrics.storeRecords || 0) * frac),
  };
}
function setRetention(state, id, hours) {
  var s = mustGet(state, id);
  var impact = retentionImpact(state, id, hours);
  var was = (s.profile && s.profile.retentionHours) || 72;
  s.profile = s.profile || {};
  s.profile.retentionHours = hours;
  if (impact.losesHistory) s.metrics.storeRecords = Math.max(0, s.metrics.storeRecords - impact.recordsLost);
  audit(state, 'stream.retention', id, { from: was, to: hours, recordsDropped: impact.recordsLost });
  return impact;
}
function setEvictionPolicy(state, id, policy) {
  var s = mustGet(state, id);
  s.profile = s.profile || {};
  s.profile.eviction = policy;
  audit(state, 'stream.eviction', id, { policy: policy });
  return s;
}

/* Cutover assist — the reason a stream and a batch belong to the same
   console. Pause capture, let delivery drain, and report the delta window
   closed. Returns the plan; the caller runs it against the tick. */
function cutoverPlan(state, id) {
  var s = mustGet(state, id);
  var drainSeconds = s.metrics.lagSeconds || 0;
  return {
    streamId: id,
    streamName: s.name,
    pending: s.metrics.pendingInStore,
    drainSeconds: drainSeconds,
    steps: [
      'Pause capture on ' + s.capture.connectionLabel + ' — no new changes are read.',
      'Let delivery drain the ' + (s.metrics.pendingInStore || 0).toLocaleString()
        + ' record(s) still in the Stream Store.',
      'Confirm lag is zero, then stop the stream. The delta window is closed.',
    ],
  };
}
function beginCutover(state, id) {
  var s = mustGet(state, id);
  s.cutover = true;
  s.status = 'running';                        // delivery keeps going
  s.profile = s.profile || {};
  s.profile.baseRate = 0;                      // capture is what stops
  s.objects.forEach(function (o) { o.state = 'draining'; });
  audit(state, 'stream.cutover_begin', id, { pending: s.metrics.pendingInStore });
  return cutoverPlan(state, id);
}
function cutoverComplete(state, id) {
  var s = mustGet(state, id);
  return !!s.cutover && s.metrics.pendingInStore === 0;
}

function mustGet(state, id) {
  var s = getStream(state, id);
  if (!s) throw new Error('No stream with id ' + id + '.');
  return s;
}

/* Streams touching a given table, for the Batches screen's cross-link. */
function streamsForTable(state, table) {
  var t = String(table || '').toLowerCase();
  return (state.streams || []).filter(function (s) {
    return (s.objects || []).some(function (o) { return String(o.table).toLowerCase() === t; });
  });
}

/* ══════════════════════════════════════════════════════════════════════════
   Routing
   ══════════════════════════════════════════════════════════════════════════
   Every workflow state in this module is addressable, and NOTHING that
   identifies data may travel in a URL. A stream id is a name; a key value is
   a row. The allow-list below is the difference, and buildQuery drops
   anything not on it rather than trusting the caller. */
var URL_KEYS = ['stream', 'view', 'step', 'clone', 'connection', 'tables',
                'state', 'topic', 'range', 'table', 'op', 'from'];

function parseQuery(search) {
  var out = {};
  String(search || '').replace(/^\?/, '').split('&').forEach(function (pair) {
    if (!pair) return;
    var i = pair.indexOf('=');
    var k = decodeURIComponent(i < 0 ? pair : pair.slice(0, i));
    var v = i < 0 ? '' : decodeURIComponent(pair.slice(i + 1).replace(/\+/g, ' '));
    if (URL_KEYS.indexOf(k) !== -1) out[k] = v;
  });
  return out;
}
function buildQuery(obj) {
  var parts = [];
  URL_KEYS.forEach(function (k) {
    var v = obj && obj[k];
    if (v === undefined || v === null || v === '' || v === false) return;
    parts.push(encodeURIComponent(k) + '=' + encodeURIComponent(String(v)));
  });
  return parts.length ? '?' + parts.join('&') : '';
}
/* Would this value be safe in a URL? Ids, table names, view/step tokens are;
   anything with a payload shape is not. Used by the tests, and by any caller
   that builds a link from user data. */
function isSafeUrlValue(v) {
  var s = String(v == null ? '' : v);
  if (s.length > 200) return false;
  return /^[A-Za-z0-9_.,:@ -]*$/.test(s);
}

/* ══════════════════════════════════════════════════════════════════════════
   Continuous queries (Stream Store screen)
   ══════════════════════════════════════════════════════════════════════════
   The point of the Stream Store screen is that the buffer is QUERYABLE. These
   three run for real against the retained events — they are not screenshots
   of SQL. A stream SQL engine is out of scope; each canned query has a
   matching evaluator, and anything else is refused rather than faked. */
var CANNED_QUERIES = [
  {
    id: 'by-table',
    label: 'Events per table, last 5 minutes',
    sql: "SELECT table_name, count(*) AS events\n  FROM stream_events\n WHERE ts > now() - interval '5 minutes'\n GROUP BY table_name\n ORDER BY events DESC",
  },
  {
    id: 'tumbling',
    label: 'One-minute tumbling window, per operation',
    sql: "SELECT window_start, op, count(*) AS events\n  FROM TUMBLE(stream_events, ts, interval '1 minute')\n GROUP BY window_start, op\n ORDER BY window_start DESC, op",
  },
  {
    id: 'join',
    label: 'Change stream joined to the target table',
    sql: "SELECT e.table_name, e.op, e.key_value, t.row_count AS target_rows\n  FROM stream_events e\n  JOIN target_tables t ON t.table_name = e.table_name\n WHERE e.ts > now() - interval '1 minute'",
  },
];

function runCannedQuery(state, id, opts) {
  var o = opts || {};
  var now = o.now || state.clockNow || Date.now();
  var events = state.events || [];

  if (id === 'by-table') {
    var since = now - 5 * 60000;
    var byTable = {};
    events.forEach(function (e) {
      if (Date.parse(e.ts) < since) return;
      byTable[e.table] = (byTable[e.table] || 0) + 1;
    });
    return {
      columns: ['table_name', 'events'],
      rows: Object.keys(byTable).map(function (t) { return [t, byTable[t]]; })
        .sort(function (a, b) { return b[1] - a[1]; }),
    };
  }
  if (id === 'tumbling') {
    var buckets = {};
    events.forEach(function (e) {
      var w = new Date(Math.floor(Date.parse(e.ts) / 60000) * 60000).toISOString().slice(11, 16);
      var k = w + '|' + e.op;
      buckets[k] = (buckets[k] || 0) + 1;
    });
    return {
      columns: ['window_start', 'op', 'events'],
      rows: Object.keys(buckets).sort().reverse().slice(0, 24).map(function (k) {
        var p = k.split('|'); return [p[0], p[1], buckets[k]];
      }),
    };
  }
  if (id === 'join') {
    var since2 = now - 60000;
    var rowsByTable = {};
    (state.streams || []).forEach(function (s) {
      (s.objects || []).forEach(function (ob) { rowsByTable[ob.table] = ob.rowsEstimate || 0; });
    });
    return {
      columns: ['table_name', 'op', 'key_value', 'target_rows'],
      rows: events.filter(function (e) { return Date.parse(e.ts) >= since2; }).slice(0, 40)
        .map(function (e) {
          return [e.table, e.op, String(Object.keys(e.key).map(function (k) { return e.key[k]; })[0]),
                  rowsByTable[e.table] || 0];
        }),
    };
  }
  throw new Error('Only the canned queries run here. A stream SQL engine is not part of this build.');
}

/* ══════════════════════════════════════════════════════════════════════════
   Formatting helpers shared by every screen
   ══════════════════════════════════════════════════════════════════════════ */
function relativeTime(iso, now) {
  if (!iso) return 'never';
  var ms = (now || Date.now()) - Date.parse(iso);
  if (!isFinite(ms)) return 'never';
  if (ms < 0) ms = 0;
  var s = Math.round(ms / 1000);
  if (s < 60) return s + 's ago';
  var mn = Math.round(s / 60);
  if (mn < 60) return mn + 'm ago';
  var h = Math.round(mn / 60);
  if (h < 24) return h + 'h ago';
  return Math.round(h / 24) + 'd ago';
}
function formatCount(n) {
  if (n == null || !isFinite(n)) return '—';
  if (n < 1000) return String(n);
  if (n < 1000000) return (n / 1000).toFixed(n < 10000 ? 1 : 0).replace(/\.0$/, '') + 'k';
  return (n / 1000000).toFixed(1).replace(/\.0$/, '') + 'M';
}
function formatBytes(b) {
  if (!b) return '0 B';
  var u = ['B', 'KB', 'MB', 'GB', 'TB'], i = 0, v = b;
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return (i === 0 ? v : v.toFixed(1)) + ' ' + u[i];
}
function formatLag(seconds) {
  if (seconds == null) return '—';
  if (seconds < 60) return seconds + 's';
  var m = Math.floor(seconds / 60);
  if (m < 60) return m + 'm ' + (seconds % 60) + 's';
  return Math.floor(m / 60) + 'h ' + (m % 60) + 'm';
}
/* Amber past the stream's own threshold, red past five times it. The
   threshold is the stream's, not a global — a nightly feed and a cutover
   delta do not have the same idea of "late". */
function lagLevel(seconds, threshold) {
  var t = threshold || 30;
  if (seconds > t * 5) return 'red';
  if (seconds > t) return 'amber';
  return 'ok';
}
/* Status → what the pill says. Every status has words; colour is never the
   only carrier. */
function statusLabel(s, stream) {
  if (s === 'snapshotting' && stream) {
    var total = snapshotRows(stream);
    var pct = total ? Math.min(99, Math.round(((stream.snapshotDone || 0) / total) * 100)) : 0;
    return 'Snapshotting ' + pct + '%';
  }
  if (s === 'lagging' && stream) return 'Lagging ' + formatLag(stream.metrics.lagSeconds);
  return { running: 'Running', paused: 'Paused', failed: 'Failed',
           stopped: 'Stopped', draft: 'Draft', snapshotting: 'Snapshotting' }[s] || s;
}

return {
  // constants
  ENUMS: ENUMS, TICK_MS: TICK_MS, MAX_EVENTS: MAX_EVENTS, SEED: SEED,
  METHOD_LABEL: METHOD_LABEL, METHOD_BLURB: METHOD_BLURB,
  WRITE_MODE_BLURB: WRITE_MODE_BLURB, DEST_KIND_LABEL: DEST_KIND_LABEL,
  CANNED_QUERIES: CANNED_QUERIES, ALERT_RULES: ALERT_RULES,
  STORE_PREFIX: STORE_PREFIX, WIP_PREFIX: WIP_PREFIX,

  // determinism
  hash32: hash32, rand: rand, randInt: randInt, makeId: makeId,

  // state
  emptyState: emptyState, load: load, save: save, reset: reset, seedDemo: seedDemo,
  wipSave: wipSave, wipLoad: wipLoad, wipClear: wipClear,
  audit: audit, storeKey: storeKey, wipKey: wipKey,

  // simulation
  tick: tick, ratePerTick: ratePerTick, lagFromPending: lagFromPending, isLive: isLive,

  // reads
  kpis: kpis, sortStreams: sortStreams, filterStreams: filterStreams, flowOf: flowOf,
  topicsOf: topicsOf, storeTotals: storeTotals, alertsOf: alertsOf, heatGrid: heatGrid,
  getStream: getStream, streamsForTable: streamsForTable,

  // designer
  blankDraft: blankDraft, validateDesign: validateDesign, reviewSentence: reviewSentence,
  snapshotEstimate: snapshotEstimate, capabilityCheck: capabilityCheck,
  enableCdcSql: enableCdcSql, resolveObjectName: resolveObjectName, methodLabel: methodLabel,

  // actions
  createStream: createStream, updateStream: updateStream, duplicateStream: duplicateStream,
  startStream: startStream, pauseStream: pauseStream, resumeStream: resumeStream,
  stopStream: stopStream, deleteStream: deleteStream, resyncObject: resyncObject,
  replay: replay, replayCount: replayCount, requeueDlq: requeueDlq, redeliverEvent: redeliverEvent,
  setRetention: setRetention, retentionImpact: retentionImpact, setEvictionPolicy: setEvictionPolicy,
  cutoverPlan: cutoverPlan, beginCutover: beginCutover, cutoverComplete: cutoverComplete,

  // routing
  parseQuery: parseQuery, buildQuery: buildQuery, isSafeUrlValue: isSafeUrlValue, URL_KEYS: URL_KEYS,

  // queries
  runCannedQuery: runCannedQuery,

  // formatting
  relativeTime: relativeTime, formatCount: formatCount, formatBytes: formatBytes,
  formatLag: formatLag, lagLevel: lagLevel, statusLabel: statusLabel,
};
});
