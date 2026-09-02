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
/* The KPI strip's own history. There is no metrics-history endpoint to ask —
   the stream engine is a client-side simulator with a 2s tick — so the trend
   behind each tile is a rolling buffer kept beside the state and persisted
   with it. Sixty samples is a minute of ticks, which is what the tiles claim
   to be showing. */
var KPI_HISTORY  = 60;
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
    kpiHistory: [],   // oldest first, one sample per tick, capped at KPI_HISTORY
    globalPause: null,// { at, actor, reason, streams:[{id, priorStatus}] } — see pauseAll
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
  if (!Array.isArray(s.kpiHistory)) s.kpiHistory = [];
  if (!Array.isArray(s.audit))   s.audit   = [];
  // Streams written before consumers existed open without them rather than
  // throwing on a missing array in every render path.
  s.streams.forEach(function (st) { if (!Array.isArray(st.consumers)) st.consumers = []; });
  s.projectId = projectId || s.projectId || 'default';
  return s;
}
function save(state) {
  if (!state) return false;
  // Cap before writing: an uncapped tail would grow localStorage without limit.
  state.events = state.events.slice(0, MAX_EVENTS);
  state.points = state.points.slice(-MAX_POINTS);
  if (Array.isArray(state.kpiHistory)) state.kpiHistory = state.kpiHistory.slice(-KPI_HISTORY);
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
    writeMode: 'upsert', tables: 4, baseRate: 412, capacityRate: 520, retentionHours: 72,
    consumers: [{ name: 'Finance reporting', owner: 'finance-data@example.internal', notifyOnStall: true },
                { name: 'Exec dashboard', owner: 'bi@example.internal' }] },
  { name: 'Source → Kafka change topic', status: 'lagging', side: 'source', method: 'log',
    connectionLabel: 'LEGACY-SQL01 · dbo', destKind: 'broker', destLabel: 'events.legacy.cdc',
    writeMode: 'append', tables: 5, baseRate: 980, capacityRate: 640, retentionHours: 48 },
  { name: 'Target → Legacy finance feed', status: 'running', side: 'target', method: 'poll',
    connectionLabel: 'AZSQL-TARGET · dbo', destKind: 'webhook', destLabel: 'https://finance.internal/hooks/cygenix',
    writeMode: 'append', tables: 2, baseRate: 96, capacityRate: 300, retentionHours: 24 },
  { name: 'Target → Data lake landing', status: 'paused', side: 'target', method: 'trigger',
    connectionLabel: 'AZSQL-TARGET · dbo', destKind: 'file', destLabel: 'adls://cygenix/landing/target',
    writeMode: 'audit-table', tables: 3, baseRate: 140, capacityRate: 400, retentionHours: 72,
    consumers: [{ name: 'Data science sandbox', owner: 'ds@example.internal' }] },
  { name: 'Source → Cutover delta into target', status: 'failed', side: 'source', method: 'log',
    connectionLabel: 'LEGACY-SQL01 · dbo', destKind: 'cygenix-target', destLabel: 'AZSQL-TARGET · dbo',
    writeMode: 'merge', tables: 2, baseRate: 260, capacityRate: 0, retentionHours: 72,
    consumers: [{ name: 'Cutover reconciliation', owner: 'migration@example.internal', notifyOnStall: true }] },
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
      consumers: (d.consumers || []).slice(),
      delivery: {
        batchSize: 500, maxIntervalMs: 2000, ordering: 'per-key',
        retries: 5, backoff: 'exponential', dlq: true, onSchemaDrift: 'pause',
        maxEventsPerMin: 0, activeWindow: null, guardrail: 'confirm_destructive',
        lagThresholdSeconds: 30,
      },
      profile: { baseRate: d.baseRate, capacityRate: d.capacityRate, retentionHours: d.retentionHours },
      metrics: {
        eventsPerMin: isLive(d.status) ? d.baseRate : 0,
        deliveredPerMin: isLive(d.status) ? Math.min(d.baseRate, d.capacityRate) : 0,
        consecutiveFailures: d.status === 'failed' ? 12 : 0,
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
      /* A failed demo stream opens with a history, because a stream that has
         only just started failing is not the state anybody needs to see
         modelled. Thirteen minutes ago it was delivering; it has not since. */
      lastGoodDeliveryAt: d.status === 'failed' ? new Date(base - 13 * 60000).toISOString()
        : isLive(d.status) ? new Date(base).toISOString() : null,
      failingSince: d.status === 'failed' ? new Date(base - 13 * 60000).toISOString() : null,
      oldestPendingAt: pending > 0 ? new Date(base - 13 * 60000).toISOString() : null,
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
    consumers: [],
    delivery: { batchSize: 200, maxIntervalMs: 5000, ordering: 'per-key', retries: 3,
      backoff: 'exponential', dlq: true, onSchemaDrift: 'pause', maxEventsPerMin: 0,
      activeWindow: null, guardrail: 'confirm_all', lagThresholdSeconds: 60 },
    profile: { baseRate: 40, capacityRate: 200, retentionHours: 24 },
    metrics: { eventsPerMin: 0, deliveredPerMin: 0, consecutiveFailures: 0, lagSeconds: 0,
      lagPeak24h: 0, pendingInStore: 0, deliveredToday: 0,
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
        if (m.pendingInStore === 0 && capturedThisTick > 0 && !s.oldestPendingAt) {
          s.oldestPendingAt = new Date(at).toISOString();
        }
        m.pendingInStore += capturedThisTick;
        m.storeRecords += capturedThisTick;
        if (n % 5 === 0) { m.dlqDepth += 1; m.failedToday += 1; }
        // How long it has been failing is the question an operator actually
        // asks, and the lag figure does not answer it: lag says how far
        // behind the data is, not when the trouble started.
        if (!s.failingSince) s.failingSince = new Date(at).toISOString();
        if (!s.lastErrorAt) s.lastErrorAt = new Date(at).toISOString();
        m.eventsPerMin = p.baseRate;
        // Capture is running. Delivery is not. One number cannot say that.
        m.deliveredPerMin = 0;
        if (n % 5 === 0) m.consecutiveFailures = (m.consecutiveFailures || 0) + 1;
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
        m.deliveredPerMin = 0;
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

    var wasEmpty = m.pendingInStore === 0;
    m.pendingInStore = available - outN;              // the invariant, exactly
    /* The store's clock. Retention expires records by AGE, so what matters is
       when the oldest unacknowledged change arrived — not how many there are.
       It starts when the store stops being empty and resets when it drains. */
    if (m.pendingInStore === 0) s.oldestPendingAt = null;
    else if (wasEmpty || !s.oldestPendingAt) s.oldestPendingAt = new Date(at).toISOString();
    m.storeRecords += inN;
    m.deliveredToday += outN;
    // "Last successful delivery" only means something if it is recorded when
    // one happens. A recovered stream also stops failing, so the clock resets.
    if (outN > 0) {
      s.lastGoodDeliveryAt = new Date(at).toISOString();
      s.failingSince = null;
    }
    m.eventsPerMin = Math.round(inN * (60000 / TICK_MS));
    m.deliveredPerMin = Math.round(outN * (60000 / TICK_MS));
    if (outN > 0) m.consecutiveFailures = 0;
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
  pushKpiSample(state, at);
  return { events: newEvents, tickNo: n };
}

/**
 * Shift a seeded state onto the live clock.
 *
 * The demo seeds at a FIXED instant so two seeds agree exactly — that is what
 * makes the figures reproducible and the tests meaningful. The live tick then
 * uses the real clock, and the gap between the two lands in every "how long
 * ago" on the screen: a stream seeded as failing twenty seconds ago reads as
 * "failing for 224h 31m" the moment the first live tick arrives, and a paused
 * one reads as nine days idle. Both are arithmetically correct and useless.
 *
 * So the page rebases once, when a seeded state first meets the real clock:
 * every timestamp moves by the same delta, which keeps every interval between
 * them exactly as seeded.
 */
function rebaseToNow(state, now) {
  if (!state || state.rebasedAt) return state;
  var at = now || Date.now();
  var delta = at - (state.clockNow || at);
  state.rebasedAt = at;
  if (Math.abs(delta) < 60000) return state;      // already close enough

  var shift = function (iso) {
    if (!iso) return iso;
    var t = Date.parse(iso);
    return isFinite(t) ? new Date(t + delta).toISOString() : iso;
  };
  (state.streams || []).forEach(function (s) {
    s.createdAt = shift(s.createdAt);
    s.startedAt = shift(s.startedAt);
    s.lastEventAt = shift(s.lastEventAt);
    s.failingSince = shift(s.failingSince);
    s.lastGoodDeliveryAt = shift(s.lastGoodDeliveryAt);
    s.lastErrorAt = shift(s.lastErrorAt);
    s.oldestPendingAt = shift(s.oldestPendingAt);
    if (s.capture && s.capture.position) s.capture.position.capturedAt = shift(s.capture.position.capturedAt);
  });
  (state.events || []).forEach(function (e) { e.ts = shift(e.ts); });
  (state.points || []).forEach(function (p) { if (p.at) p.at += delta; });
  (state.kpiHistory || []).forEach(function (p) { if (p.at) p.at += delta; });
  (state.audit || []).forEach(function (a) { a.at = shift(a.at); });
  state.clockNow = at;
  return state;
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
    storeDepth: s.metrics.pendingInStore, dlq: s.metrics.dlqDepth || 0,
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

/* ══════════════════════════════════════════════════════════════════════════
   Trend — the part of a KPI tile that carries the meaning
   ══════════════════════════════════════════════════════════════════════════
   A tile showing 191 says nothing. 191 rising by about 2 a minute says the
   thing an operator needs. The sparkline is secondary to that sentence; it is
   there to show the shape, not to be read off.
   ══════════════════════════════════════════════════════════════════════════ */

/* One sample per tick, so every tile draws from the same history and cannot
   disagree with its neighbour about what "the last minute" means. */
function pushKpiSample(state, at) {
  var k = kpis(state);
  if (!Array.isArray(state.kpiHistory)) state.kpiHistory = [];
  state.kpiHistory.push({
    at: at,
    running: k.running,
    eventsPerMin: k.eventsPerMin,
    maxLagSeconds: k.maxLagSeconds,
    errors: k.errorsLastHour,
  });
  if (state.kpiHistory.length > KPI_HISTORY) {
    state.kpiHistory = state.kpiHistory.slice(-KPI_HISTORY);
  }
}

function kpiSeries(state, key) {
  return (state && Array.isArray(state.kpiHistory) ? state.kpiHistory : [])
    .map(function (p) { return Number(p[key]) || 0; });
}

/* Below this many points there is no trend, only noise pretending to be one.
   A flat line drawn from three samples is a lie told in a confident font. */
var SPARK_MIN_POINTS = 8;
/* And a move smaller than this is the system breathing, not a trend. Colour
   is for problems; painting a tile red every time a number wobbles teaches
   people to ignore the colour. */
var SPARK_SIGNIFICANT = 0.10;

/**
 * What the series is doing, as numbers and as a sentence.
 * `higherIsBetter` decides which direction is bad: false for errors and lag,
 * true for throughput and streams running.
 */
function trend(series, higherIsBetter, opts) {
  var o = opts || {};
  var pts = (series || []).filter(function (v) { return typeof v === 'number' && isFinite(v); });
  if (pts.length < SPARK_MIN_POINTS) {
    return { enough: false, points: pts, label: '', adverse: false,
             sentence: o.label ? o.label + ': ' + (o.value != null ? o.value : (pts.length ? pts[pts.length - 1] : 'no value'))
               + ', still collecting history.' : '' };
  }
  var first = pts[0], last = pts[pts.length - 1];
  var delta = last - first;
  var span = pts.length - 1;
  var perTick = delta / span;
  var base = Math.abs(first) || 1;
  var pctChange = delta / base;
  var rising = delta > 0;
  // Adverse means moving the wrong way AND far enough to mean it.
  var adverse = Math.abs(pctChange) >= SPARK_SIGNIFICANT
    && (higherIsBetter ? delta < 0 : delta > 0);

  var label;
  if (delta === 0) label = 'level';
  else if (o.unit === 'per-min') {
    label = (rising ? '+' : '−') + Math.abs(Math.round(perTick * (60000 / TICK_MS) * 10) / 10) + '/min';
  } else {
    label = (rising ? '↑ ' : '↓ ') + Math.abs(Math.round(pctChange * 100)) + '%';
  }

  return {
    enough: true, points: pts, first: first, last: last, delta: delta,
    pctChange: pctChange, rising: rising, adverse: adverse, label: label,
    sentence: trendSentence(o.label, o.value != null ? o.value : last, delta, perTick, o),
  };
}

/* The accessible name is a sentence, not a list of numbers — it is what a
   screen-reader user gets INSTEAD of the shape, and "194, 192, 193…" is not
   an alternative to a picture, it is a worse version of one. */
function trendSentence(label, value, delta, perTick, o) {
  var name = label || 'Value';
  if (delta === 0) return name + ': ' + value + ', level over the last minute.';
  var dir = delta > 0 ? 'rising' : 'falling';
  var amount;
  if ((o || {}).unit === 'per-min') {
    amount = 'about ' + Math.abs(Math.round(perTick * (60000 / TICK_MS) * 10) / 10) + ' per minute';
  } else {
    amount = 'by about ' + Math.abs(Math.round(delta)) + (o && o.unitWord ? ' ' + o.unitWord : '');
  }
  return name + ': ' + value + ', ' + dir + ' ' + amount + ' over the last minute.';
}

/**
 * Points for an inline <polyline>, in a 0..w × 0..h box. Thirty lines of
 * arithmetic instead of a charting dependency, which is the correct trade for
 * a 24px sparkline.
 */
function sparkPoints(series, w, h) {
  var pts = (series || []).filter(function (v) { return typeof v === 'number' && isFinite(v); });
  if (pts.length < 2) return '';
  var width = w || 100, height = h || 24;
  var min = Math.min.apply(null, pts), max = Math.max.apply(null, pts);
  var span = max - min;
  return pts.map(function (v, i) {
    var x = (i / (pts.length - 1)) * width;
    // A genuinely flat series draws down the middle rather than along the
    // floor, which would read as "zero" instead of "unchanging".
    var y = span === 0 ? height / 2 : height - ((v - min) / span) * height;
    return (Math.round(x * 10) / 10) + ',' + (Math.round(y * 10) / 10);
  }).join(' ');
}

/* ══════════════════════════════════════════════════════════════════════════
   Failure detail — why, not just where
   ══════════════════════════════════════════════════════════════════════════ */

var ERROR_CLASSES = [
  { re: /truncat/i,                    cls: 'String truncation' },
  { re: /constraint|foreign key|dupl/i, cls: 'Constraint violation' },
  { re: /permission|denied|login/i,    cls: 'Permission denied' },
  { re: /timeout|timed out/i,          cls: 'Timeout' },
  { re: /connect|network|refused/i,    cls: 'Connection failure' },
  { re: /schema|column|type/i,         cls: 'Schema mismatch' },
];
function errorClass(message) {
  for (var i = 0; i < ERROR_CLASSES.length; i++) {
    if (ERROR_CLASSES[i].re.test(String(message || ''))) return ERROR_CLASSES[i].cls;
  }
  return message ? 'Delivery rejected' : null;
}

/** Field names, no values. What broke is structure; the values are payload. */
function redactRow(row) {
  var out = {};
  Object.keys(row || {}).forEach(function (k) { out[k] = '••••••'; });
  return out;
}

/**
 * Everything the operator needs to know about a failure, in one shape.
 * `now` is passed rather than read so this stays testable.
 */
function failureDetail(state, stream, now) {
  var s = stream;
  if (!s) return null;
  var at = now || (state && state.clockNow) || Date.now();
  var dead = ((state && state.events) || []).filter(function (e) {
    return e.streamId === s.id && e.deliveryState === 'dead';
  });
  var sample = dead[0] || null;
  var since = s.failingSince ? Date.parse(s.failingSince) : null;
  var good = s.lastGoodDeliveryAt ? Date.parse(s.lastGoodDeliveryAt) : null;

  return {
    failing: s.status === 'failed' || (s.metrics.failedToday || 0) > 0,
    message: s.lastError || null,
    errorClass: errorClass(s.lastError),
    firstSeen: s.failingSince || null,
    failingForSeconds: since ? Math.max(0, Math.round((at - since) / 1000)) : null,
    lastGoodDeliveryAt: s.lastGoodDeliveryAt || null,
    lastGoodAgoSeconds: good ? Math.max(0, Math.round((at - good) / 1000)) : null,
    checkpoint: (s.capture.position && s.capture.position.lsn) || null,
    dlqDepth: s.metrics.dlqDepth || 0,
    sample: sample ? {
      id: sample.id, table: sample.table, op: sample.op, ts: sample.ts,
      fields: Object.keys(sample.after || sample.before || {}),
      // Redacted by default, always. A status screen is looked at in open
      // offices and pasted into tickets; customer rows do not belong in one
      // unless somebody deliberately asks.
      redacted: redactRow(sample.after || sample.before),
      error: sample.error || s.lastError || null,
    } : null,
  };
}

/** The values behind a redacted sample. Only ever called from a click. */
function revealSample(state, streamId, eventId) {
  var e = ((state && state.events) || []).filter(function (x) {
    return x.id === eventId && x.streamId === streamId;
  })[0];
  return e ? (e.after || e.before || null) : null;
}

/* ══════════════════════════════════════════════════════════════════════════
   BLOCKED — a stream that is not delivering must not look like one that is
   ══════════════════════════════════════════════════════════════════════════
   The failure this exists for, from the live build:

     Source → Cutover delta into target
     Status      Failed
     Throughput  260/min        ← reassuring
     Last event  0s ago         ← reassuring
     Actually    capture 260/min, DELIVER 0/min, 1,600 pending and climbing,
                 every recent event dead, ~95 in the dead-letter queue,
                 "destination rejected: string truncation on stg_customers.name"

   Two of the three numbers on that row are comforting and both are true. It
   IS reading 260 changes a minute and it DID see an event a second ago.
   Nothing is arriving at the destination. The cause was three clicks away in
   a tab nobody had a reason to open.

   Blocked is therefore derived from the SERIES, not from the status label —
   a status can say "Failed" while the numbers beside it say "busy", and the
   numbers are what people read.
   ══════════════════════════════════════════════════════════════════════════ */

var BLOCK_NO_DELIVERY_MS = 3 * 60000;    // (a) capturing, delivering nothing
var BLOCK_DLQ_WINDOW_MS = 5 * 60000;     // (b) dead-letter queue growing
var BLOCK_PENDING_MS = 10 * 60000;       // (d) store rising and never falling

function pointsFor(state, streamId, sinceMs, now) {
  var at = now || (state && state.clockNow) || Date.now();
  return ((state && state.points) || []).filter(function (p) {
    if (p.streamId !== streamId) return false;
    var t = Date.parse(p.t);
    return isFinite(t) && at - t <= sinceMs;
  });
}

/**
 * Is this stream stopped, struggling, or fine — and on what evidence.
 *
 * Hard-blocked (a, c) means nothing is arriving at the destination.
 * Degraded (b, d) means it is delivering AND losing some: a stream doing
 * 900/min while dead-lettering 5/min is losing data but is not stopped, and
 * lumping the two together would make the loud state meaningless.
 */
function blockedState(state, stream, now) {
  var s = stream;
  if (!s) return null;
  var at = now || (state && state.clockNow) || Date.now();
  var m = s.metrics || {};
  var reasons = [];
  var hard = false, degraded = false;

  // (a) Capturing, delivering nothing, for long enough that it is not a gap
  //     between batches.
  var recent = pointsFor(state, s.id, BLOCK_NO_DELIVERY_MS, at);
  var enough = recent.length >= Math.floor(BLOCK_NO_DELIVERY_MS / TICK_MS) * 0.6;
  var noDelivery = enough && recent.every(function (p) { return (p.out || 0) === 0; })
    && recent.some(function (p) { return (p.in || 0) > 0; });
  // A stream that has only just started failing has no three minutes of
  // history yet; its own failure clock answers the same question.
  var failingFor = s.failingSince ? at - Date.parse(s.failingSince) : 0;
  if (noDelivery || (failingFor >= BLOCK_NO_DELIVERY_MS && (m.deliveredPerMin || 0) === 0
      && (m.eventsPerMin || 0) > 0)) {
    hard = true;
    reasons.push({ code: 'no-delivery', text: 'capturing but delivering nothing' });
  }

  // (c) The same error, past the point where retrying is going to help.
  var retries = (s.delivery && s.delivery.retries) || 5;
  if ((m.consecutiveFailures || 0) > retries) {
    hard = true;
    reasons.push({ code: 'retries-exhausted',
      text: m.consecutiveFailures + ' consecutive failures, past the ' + retries + ' configured retries' });
  }

  // (b) The dead-letter queue is growing: records are being given up on.
  var dlqWindow = pointsFor(state, s.id, BLOCK_DLQ_WINDOW_MS, at);
  if (dlqWindow.length > 1) {
    var dlqRise = (dlqWindow[dlqWindow.length - 1].dlq || 0) - (dlqWindow[0].dlq || 0);
    if (dlqRise > 0) {
      degraded = true;
      reasons.push({ code: 'dlq-growing',
        text: dlqRise + ' more record(s) given up on in the last 5 minutes' });
    }
  }

  // (d) The store only goes up. Not stopped, but not keeping up either.
  var pendWindow = pointsFor(state, s.id, BLOCK_PENDING_MS, at);
  if (pendWindow.length >= Math.floor(BLOCK_PENDING_MS / TICK_MS) * 0.6) {
    var monotonic = true;
    for (var i = 1; i < pendWindow.length; i++) {
      if ((pendWindow[i].storeDepth || 0) < (pendWindow[i - 1].storeDepth || 0)) { monotonic = false; break; }
    }
    if (monotonic && (pendWindow[pendWindow.length - 1].storeDepth || 0) > (pendWindow[0].storeDepth || 0)) {
      degraded = true;
      reasons.push({ code: 'backlog-growing', text: 'the store has only grown for 10 minutes' });
    }
  }

  if (!hard && !degraded) return null;

  var stoppedSince = s.lastGoodDeliveryAt ? Date.parse(s.lastGoodDeliveryAt)
    : (s.failingSince ? Date.parse(s.failingSince) : null);
  var stoppedFor = stoppedSince ? Math.max(0, Math.round((at - stoppedSince) / 1000)) : null;

  return {
    level: hard ? 'blocked' : 'degraded',
    // A paused stream nobody expects to be running should not carry a live
    // "13 minutes and counting" — the clock is frozen and labelled instead.
    paused: s.status === 'paused' || s.status === 'stopped',
    reasons: reasons,
    stoppedForSeconds: stoppedFor,
    deadline: retentionDeadline(s, at),
  };
}

/** Every blocked or degraded stream, worst first. */
function blockedStreams(state, now) {
  var at = now || (state && state.clockNow) || Date.now();
  return ((state && state.streams) || [])
    .map(function (s) {
      var b = blockedState(state, s, at);
      return b ? { stream: s, blocked: b } : null;
    })
    .filter(Boolean)
    .sort(function (a, b) {
      if (a.blocked.level !== b.blocked.level) return a.blocked.level === 'blocked' ? -1 : 1;
      return a.blocked.deadline.secondsRemaining - b.blocked.deadline.secondsRemaining;
    });
}

/* ── What it MEANS, which is the bit nobody writes ─────────────────────────
 * Derived from the write mode and from what is actually stuck, not
 * hardcoded: an append-only webhook feed losing records is a different
 * sentence from a merge stream that is not applying deletes.
 */
function blockedConsequence(state, stream) {
  var s = stream;
  if (!s) return '';
  var dest = (s.destination && s.destination.label) || 'the destination';
  var mode = (s.destination && s.destination.writeMode) || 'append';
  var dead = ((state && state.events) || []).filter(function (e) {
    return e.streamId === s.id && e.deliveryState === 'dead';
  });
  var ops = {};
  dead.forEach(function (e) { ops[e.op] = (ops[e.op] || 0) + 1; });
  var tables = {};
  dead.forEach(function (e) { tables[e.table] = 1; });
  var tableList = Object.keys(tables).slice(0, 3).join(', ');

  if (mode === 'merge' || mode === 'upsert') {
    if (ops.D) {
      return 'Deletions are not being applied to ' + dest + '. The target is drifting '
        + 'further out of date every minute'
        + (tableList ? ', on ' + tableList : '') + '.';
    }
    return 'Updates are not reaching ' + dest + (tableList ? ' for ' + tableList : '')
      + ', so it is serving values that have already changed.';
  }
  if (mode === 'append') {
    return 'New records are not reaching ' + dest + '. Anything reading it sees a feed '
      + 'that simply stops, with no gap to notice.';
  }
  if (mode === 'audit-table') {
    return 'The audit trail at ' + dest + ' has a hole in it from the moment delivery stopped.';
  }
  return 'Nothing is reaching ' + dest + ' while this is stopped.';
}

/** One paste-ready block for a ticket or a chat. */
function blockedDiagnostics(state, stream, now) {
  var s = stream;
  var b = blockedState(state, s, now);
  var d = retentionDeadline(s, now || (state && state.clockNow));
  var lines = [
    'Stream:        ' + s.name,
    'Status:        ' + statusLabel(s.status) + (b ? ' (' + b.level + ')' : ''),
    'Route:         ' + ((s.capture && s.capture.connectionLabel) || '?') + ' -> '
      + ((s.destination && s.destination.label) || '?') + ' (' + ((s.destination && s.destination.writeMode) || '?') + ')',
    'Capture:       ' + (s.metrics.eventsPerMin || 0) + '/min',
    'Deliver:       ' + (s.metrics.deliveredPerMin || 0) + '/min',
    'Last delivery: ' + (s.lastGoodDeliveryAt || 'none recorded'),
    'Pending:       ' + (s.metrics.pendingInStore || 0),
    'Dead-letter:   ' + (s.metrics.dlqDepth || 0),
    'Error:         ' + (s.lastError || 'none recorded'),
    'Retention:     ' + d.retentionHours + 'h, ' + d.line,
    'Checkpoint:    ' + ((s.capture && s.capture.position && s.capture.position.lsn) || 'none'),
  ];
  if (b) lines.push('Detected:      ' + b.reasons.map(function (r) { return r.text; }).join('; '));
  lines.push('Consequence:   ' + blockedConsequence(state, s));
  return lines.join('\n');
}

/* Pull the table and column out of a destination's own words where the shape
   allows it, so the band can point at them. The message is never rewritten —
   this only says which part of it to highlight. */
function parseErrorTarget(message) {
  var m = /\b((?:\[[^\]]+\]|\w+)(?:\.(?:\[[^\]]+\]|\w+))+)\b/.exec(String(message || ''));
  if (!m) return null;
  var parts = m[1].split('.');
  return { full: m[1], table: parts.slice(0, -1).join('.'), column: parts[parts.length - 1] };
}

/* ══════════════════════════════════════════════════════════════════════════
   The retention deadline — the number nobody was showing
   ══════════════════════════════════════════════════════════════════════════
   A stream's store keeps records for a fixed window. While delivery is
   stopped — paused, blocked, whatever — captured changes sit there ageing.
   When the OLDEST one passes the window, that backlog is gone: the stream
   can no longer replay, it needs a full resync, which on a large table is a
   different order of operation entirely.

   That deadline is the single most decision-relevant number about a stopped
   stream, and it appeared nowhere. It is computed here, once, so the pause
   dialog and the blocked band cannot disagree about how long somebody has.
   ══════════════════════════════════════════════════════════════════════════ */

var DEADLINE_AMBER_H = 12;
var DEADLINE_RED_H = 4;

/**
 * How long before this stream's backlog expires.
 *
 * Age, not volume: retention throws records away by how old they are, so a
 * stream with eleven pending records and a 24h window is in exactly as much
 * trouble as one with eleven million, if both stopped delivering at the same
 * moment.
 */
function retentionDeadline(stream, now) {
  var s = stream;
  if (!s) return null;
  var at = now || Date.now();
  var hours = (s.profile && s.profile.retentionHours) || 72;
  var windowSeconds = hours * 3600;
  var pending = (s.metrics && s.metrics.pendingInStore) || 0;
  var capturePerMin = (s.metrics && s.metrics.eventsPerMin) || 0;

  var oldest = s.oldestPendingAt ? Date.parse(s.oldestPendingAt) : null;
  var ageSeconds;
  if (oldest && isFinite(oldest)) {
    ageSeconds = Math.max(0, Math.round((at - oldest) / 1000));
  } else if (pending > 0 && capturePerMin > 0) {
    // No mark to read — estimate from how long this backlog would have taken
    // to accumulate at the rate it is arriving. Never claims more than the
    // window, because a backlog older than the window has already expired.
    ageSeconds = Math.min(windowSeconds, Math.round((pending / capturePerMin) * 60));
  } else {
    ageSeconds = 0;
  }

  var remaining = windowSeconds - ageSeconds;
  var band = remaining <= 0 ? 'expired'
    : remaining <= DEADLINE_RED_H * 3600 ? 'red'
    : remaining <= DEADLINE_AMBER_H * 3600 ? 'amber' : 'ok';

  return {
    retentionHours: hours,
    pending: pending,
    dlqDepth: (s.metrics && s.metrics.dlqDepth) || 0,
    capturePerMin: capturePerMin,
    oldestPendingAt: s.oldestPendingAt || null,
    backlogAgeSeconds: ageSeconds,
    secondsRemaining: Math.max(0, remaining),
    expired: remaining <= 0,
    band: band,
    /* "48h retention · ~46h before backlog expires". An empty store is
       phrased differently on purpose: there is no backlog to lose yet, and
       saying "~72h before this backlog expires" about nothing invents one. */
    line: hours + 'h retention · '
      + (remaining <= 0
          ? 'backlog has already expired — a full resync is needed, not a replay'
          : pending === 0
            ? 'nothing queued yet, so ' + roughDuration(remaining) + ' of headroom once it starts'
            : '~' + roughDuration(remaining) + ' before this backlog expires'),
  };
}

/** "~46h", "~13h", "~35m" — a deadline does not need seconds. */
function roughDuration(seconds) {
  if (seconds === null || seconds === undefined) return 'an unknown time';
  if (seconds < 90 * 60) return Math.max(1, Math.round(seconds / 60)) + 'm';
  if (seconds < 48 * 3600) return Math.round(seconds / 3600) + 'h';
  return Math.round(seconds / 86400) + 'd';
}

/* ══════════════════════════════════════════════════════════════════════════
   Global pause and resume
   ══════════════════════════════════════════════════════════════════════════
   Stopping replication during an incident, a cutover window or maintenance on
   the source meant clicking through every stream in turn. This does it in one
   action — and the part that has to be right is not the stopping, it is the
   starting again.

   A stream somebody deliberately paused a fortnight ago must not come back to
   life because a colleague pressed global resume. So the prior state of every
   stream is recorded when the global pause is applied, and resume restores
   ONLY what was live at that moment. The snapshot lives in the stream state,
   which is persisted and synced — it survives a reload, another browser and
   another operator, which is the whole requirement.
   ══════════════════════════════════════════════════════════════════════════ */

/** Statuses a global pause acts on. Draft has never started; paused is
    somebody's decision and is not ours to undo. */
var GLOBAL_PAUSABLE = { running: 1, lagging: 1, failed: 1, snapshotting: 1 };

/** What the switch should say, computed rather than remembered. */
function globalPauseState(state) {
  var streams = (state && state.streams) || [];
  var live = streams.filter(function (s) { return isLive(s.status) || s.status === 'failed'; });
  var eligible = streams.filter(function (s) { return s.status !== 'draft'; });
  var snap = (state && state.globalPause) || null;
  var mode = !eligible.length ? 'empty'
    : live.length === 0 ? 'paused'
    : live.length === eligible.length ? 'running' : 'mixed';
  return {
    mode: mode,
    running: live.length,
    total: eligible.length,
    snapshot: snap,
    // "Partially paused — 3 of 6 running"
    label: mode === 'empty' ? 'No streams yet'
      : mode === 'running' ? 'All streams running'
      : mode === 'paused' ? 'All streams paused'
      : 'Partially paused — ' + live.length + ' of ' + eligible.length + ' running',
  };
}

/** Which streams a global pause would touch, and what it would cost each. */
function pauseAllPreview(state, now) {
  var at = now || (state && state.clockNow) || Date.now();
  var affected = ((state && state.streams) || [])
    .filter(function (s) { return GLOBAL_PAUSABLE[s.status]; })
    .map(function (s) {
      return {
        id: s.id, name: s.name, status: s.status,
        from: (s.capture && s.capture.connectionLabel) || '',
        to: (s.destination && s.destination.label) || '',
        deadline: retentionDeadline(s, at),
      };
    });
  // The worst case leads, because it is the one that sets the clock.
  var soonest = affected.slice().sort(function (a, b) {
    return a.deadline.secondsRemaining - b.deadline.secondsRemaining;
  })[0] || null;
  return {
    streams: affected,
    soonest: soonest,
    skipped: ((state && state.streams) || []).filter(function (s) { return !GLOBAL_PAUSABLE[s.status]; })
      .map(function (s) { return { id: s.id, name: s.name, status: s.status }; }),
    headline: !soonest ? 'No stream is currently capturing, so nothing is at risk.'
      : soonest.deadline.expired
        ? 'Data already lost: ' + soonest.name + '\u2019s backlog has passed its '
          + soonest.deadline.retentionHours + 'h window and needs a full resync'
        : 'Earliest data loss: ~' + roughDuration(soonest.deadline.secondsRemaining)
          + ' (' + soonest.name + ', ' + soonest.deadline.retentionHours + 'h retention)',
  };
}

/* Pausing six streams is a sequence, not an instant, and the screen has to be
   able to say which of them have actually stopped. So the bookkeeping is
   separated from the loop: `pauseAllPlan` says who is in scope, the caller
   applies them one at a time at whatever pace lets it repaint, and
   `commitGlobalPause` writes the snapshot and the single audit entry once.
   `pauseAll` is the synchronous wrapper for callers that do not need to
   render progress. Either way there is one snapshot and one audit entry. */

function pauseAllPlan(state) {
  return (state.streams || [])
    .filter(function (s) { return GLOBAL_PAUSABLE[s.status]; })
    .map(function (s) {
      return { id: s.id, name: s.name, priorStatus: s.status,
               dlqDepth: (s.metrics && s.metrics.dlqDepth) || 0 };
    });
}

/**
 * Record what was stopped and why. `results` is what actually happened, one
 * entry per attempted stream — only the successes reach the snapshot, because
 * a snapshot claiming to have stopped something it did not is worse than none.
 */
function commitGlobalPause(state, opts) {
  var o = opts || {};
  var results = o.results || [];
  var snapshot = state.globalPause || {
    at: nowIso(state), actor: currentActor(), reason: o.reason || '', streams: [],
  };
  results.filter(function (r) { return r.ok; }).forEach(function (r) {
    if (snapshot.streams.some(function (x) { return x.id === r.id; })) return;   // idempotent
    snapshot.streams.push({ id: r.id, name: r.name, priorStatus: r.priorStatus,
                            dlqDepth: r.dlqDepth || 0 });
  });
  if (snapshot.streams.length) state.globalPause = snapshot;

  var failed = results.filter(function (r) { return !r.ok; });
  // One entry for the action, not one per stream: the operator did one thing.
  audit(state, 'streams.pause_all', null, {
    requested: results.length, paused: results.length - failed.length,
    failed: failed.length, reason: o.reason || null,
    prior: snapshot.streams.map(function (x) { return x.id + ':' + x.priorStatus; }),
  });
  return { ok: !failed.length, results: results, snapshot: snapshot,
           paused: results.length - failed.length, failed: failed };
}

/**
 * Pause every eligible stream, recording what each was first.
 * Returns a per-stream result so a caller can render a partial failure —
 * "all or nothing" is not one of the outcomes this can have.
 * Idempotent: a second call adds nothing to the snapshot and reports ok.
 */
function pauseAll(state, opts) {
  var o = opts || {};
  var results = pauseAllPlan(state).map(function (t) {
    try {
      if (typeof o.apply === 'function') o.apply(state, t.id);
      else pauseStream(state, t.id);
      return { id: t.id, name: t.name, ok: true, priorStatus: t.priorStatus, dlqDepth: t.dlqDepth };
    } catch (e) {
      return { id: t.id, name: t.name, ok: false, priorStatus: t.priorStatus,
               error: e && e.message ? e.message : String(e) };
    }
  });
  return commitGlobalPause(state, { results: results, reason: o.reason });
}

/** What a resume would and would not restart, and what would fail again. */
function resumeAllPreview(state) {
  var snap = (state && state.globalPause) || null;
  var byId = {};
  ((state && state.streams) || []).forEach(function (s) { byId[s.id] = s; });

  var willResume = [], willFailAgain = [], stayPaused = [];
  ((snap && snap.streams) || []).forEach(function (rec) {
    var s = byId[rec.id];
    if (!s) return;
    // A stream that was failing with records already given up on will fail
    // again the moment it starts, and add to the pile. That is an opt-in.
    if (rec.priorStatus === 'failed' && (s.metrics.dlqDepth || 0) > 0) {
      willFailAgain.push({ id: s.id, name: s.name, error: s.lastError || 'previously failed',
                           dlqDepth: s.metrics.dlqDepth });
    } else {
      willResume.push({ id: s.id, name: s.name, priorStatus: rec.priorStatus });
    }
  });
  ((state && state.streams) || []).forEach(function (s) {
    if (s.status !== 'paused') return;
    if (((snap && snap.streams) || []).some(function (r) { return r.id === s.id; })) return;
    // Paused before the global pause, by a person, for a reason nobody here
    // knows. It stays paused.
    stayPaused.push({ id: s.id, name: s.name });
  });
  return { willResume: willResume, willFailAgain: willFailAgain, stayPaused: stayPaused, snapshot: snap };
}

/** Which stream ids a resume would act on, given the failed opt-in. */
function resumeAllPlan(state, includeFailed) {
  var pv = resumeAllPreview(state);
  return pv.willResume.concat(includeFailed ? pv.willFailAgain : []);
}

/** Clear the snapshot for what was restarted, and write the one audit entry. */
function commitGlobalResume(state, opts) {
  var o = opts || {};
  var results = o.results || [];
  var failed = results.filter(function (r) { return !r.ok; });
  // The snapshot is only cleared once everything it held has been dealt with.
  // Clearing it on a partial resume would strand the rest with nothing left
  // recording what they used to be.
  var remaining = ((state.globalPause && state.globalPause.streams) || []).filter(function (rec) {
    return !results.some(function (r) { return r.ok && r.id === rec.id; });
  });
  if (!remaining.length) state.globalPause = null;
  else state.globalPause = Object.assign({}, state.globalPause, { streams: remaining });

  audit(state, 'streams.resume_all', null, {
    resumed: results.length - failed.length, failed: failed.length,
    heldBack: (o.stayPaused || []).length,
    skippedFailed: (o.skippedFailed || []).length,
    reason: o.reason || null,
  });
  return { ok: !failed.length, results: results, resumed: results.length - failed.length,
           failed: failed, stayPaused: o.stayPaused || [], skippedFailed: o.skippedFailed || [] };
}

/**
 * Restore what the global pause stopped, and only that.
 * `opts.includeFailed` opts in to restarting streams that were failing with a
 * non-empty dead-letter queue; without it they stay paused.
 */
function resumeAll(state, opts) {
  var o = opts || {};
  var pv = resumeAllPreview(state);
  var list = resumeAllPlan(state, o.includeFailed);
  var results = list.map(function (rec) {
    try {
      if (typeof o.apply === 'function') o.apply(state, rec.id);
      else resumeStream(state, rec.id);
      return { id: rec.id, name: rec.name, ok: true };
    } catch (e) {
      return { id: rec.id, name: rec.name, ok: false, error: e && e.message ? e.message : String(e) };
    }
  });
  return commitGlobalResume(state, {
    results: results, reason: o.reason, stayPaused: pv.stayPaused,
    skippedFailed: o.includeFailed ? [] : pv.willFailAgain,
  });
}

/* ══════════════════════════════════════════════════════════════════════════
   Downstream consumers — who is reading this, and since when
   ══════════════════════════════════════════════════════════════════════════
   A stream paused for eight days is not a fact about a stream. Somewhere a
   reporting team is looking at eight-day-old numbers and making decisions on
   them, and the screen that knows the stream is paused is the only thing in
   the building that could have said so.
   ══════════════════════════════════════════════════════════════════════════ */

var STALLED = { paused: 1, failed: 1, stopped: 1, lagging: 1 };

/**
 * The impact line for a stalled stream with consumers, or null.
 * Named consumers only — "consumers may be affected" is not information.
 */
function consumerImpact(stream, now) {
  var s = stream;
  if (!s || !STALLED[s.status]) return null;
  var consumers = (s.consumers || []).filter(function (c) { return c && c.name; });
  if (!consumers.length) return null;

  var at = now || Date.now();
  /* Since when there has been nothing new AT THE OTHER END. The last
     successful delivery is the honest answer; failing that, when the failure
     started, because a failed stream goes on capturing — its lastEventAt is
     seconds old while the consumer has had nothing for an hour, and reading
     that as "no new data for 1 second" would be exactly backwards. A paused
     stream has stopped capturing too, so there its last event is the right
     mark. */
  var sinceIso = s.lastGoodDeliveryAt
    || (s.status === 'failed' ? s.failingSince : null)
    || s.lastEventAt || s.startedAt || null;
  var since = sinceIso ? Date.parse(sinceIso) : null;
  var seconds = since ? Math.max(0, Math.round((at - since) / 1000)) : null;

  var names = consumers.map(function (c) { return c.name; });
  var who = names.length === 1 ? names[0]
    : names.slice(0, -1).join(', ') + ' and ' + names[names.length - 1];
  var verb = names.length === 1 ? 'has' : 'have';

  return {
    consumers: consumers,
    names: names,
    sinceIso: sinceIso,
    seconds: seconds,
    line: who + ' ' + verb + ' had no new data for ' + durationWords(seconds) + '.',
  };
}

function durationWords(seconds) {
  if (seconds === null || seconds === undefined) return 'an unknown length of time';
  if (seconds < 90) {
    var n = Math.max(1, Math.round(seconds));
    return n + ' second' + (n === 1 ? '' : 's');
  }
  var mins = Math.round(seconds / 60);
  if (mins < 90) return mins + ' minute' + (mins === 1 ? '' : 's');
  var hours = Math.round(seconds / 3600);
  if (hours < 48) return hours + ' hour' + (hours === 1 ? '' : 's');
  var days = Math.round(seconds / 86400);
  return days + ' day' + (days === 1 ? '' : 's');
}

/* Sort by how much attention a stream needs, then by lag. A screen that
   sorted alphabetically would bury the failed stream. */
var SEVERITY = { failed: 0, lagging: 1, snapshotting: 2, running: 3, paused: 4, stopped: 5, draft: 6 };
/**
 * Sorted by what needs attention first — and now that is a fact about the
 * numbers rather than about the status label. Pass `state` and a blocked
 * stream ranks above every status, ordered by how long it has left before its
 * backlog expires: the one with four hours goes above the one with three days.
 */
function sortStreams(list, now, state) {
  var at = now || (state && state.clockNow) || Date.now();
  var blocked = {};
  if (state) {
    blockedStreams(state, at).forEach(function (e) { blocked[e.stream.id] = e.blocked; });
  }
  var sev = function (s) {
    var b = blocked[s.id];
    if (b && b.level === 'blocked') return -10 + Math.min(9, b.deadline.secondsRemaining / 86400);
    if (b && b.level === 'degraded') return -1;
    var base = SEVERITY[s.status] === undefined ? 9 : SEVERITY[s.status];
    // A stalled stream somebody is READING outranks one nobody is. Both are
    // broken; only one of them is quietly feeding stale numbers to a person.
    // Half a step, so it lifts within its band rather than jumping the queue
    // ahead of an outright failure.
    return consumerImpact(s, at) ? base - 0.5 : base;
  };
  return list.slice().sort(function (a, b) {
    var d = sev(a) - sev(b);
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
    // Who reads what comes out of this. Optional, and empty is honest — but
    // when it is filled in, a stall stops being a fact about a stream and
    // becomes a fact about the people downstream of it.
    consumers: [],
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
  s.metrics = { eventsPerMin: 0, deliveredPerMin: 0, consecutiveFailures: 0, lagSeconds: 0,
    lagPeak24h: 0, pendingInStore: 0,
    deliveredToday: 0, failedToday: 0, dlqDepth: 0, storeRecords: 0, spark: [],
    deliveredPerMin: 0, consecutiveFailures: 0 };
  if (!Array.isArray(s.consumers)) s.consumers = [];
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
  // The failure clock belongs to the stream too: editing a batch size does not
  // mean the destination started accepting records again.
  next.lastError = was.lastError;
  next.failingSince = was.failingSince;
  next.lastGoodDeliveryAt = was.lastGoodDeliveryAt;
  if (!Array.isArray(next.consumers)) next.consumers = was.consumers || [];
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
/**
 * Wind the stream back to the last position it is known to have delivered
 * from, and clear the failure. Everything captured since is still in the
 * Stream Store, so this re-delivers rather than loses — which is the whole
 * reason a checkpoint is worth keeping.
 */
function resetToCheckpoint(state, id) {
  var s = mustGet(state, id);
  var from = s.capture.position && s.capture.position.lsn;
  s.lastError = null;
  s.failingSince = null;
  s.metrics.failedToday = 0;
  if (s.status === 'failed') s.status = 'paused';
  audit(state, 'stream.checkpoint_reset', id, {
    position: from, pending: s.metrics.pendingInStore, dlq: s.metrics.dlqDepth });
  return { position: from, pending: s.metrics.pendingInStore };
}

/** Replace the downstream consumer list. Editable from the stream's settings. */
function setConsumers(state, id, list) {
  var s = mustGet(state, id);
  s.consumers = (list || [])
    .filter(function (c) { return c && String(c.name || '').trim(); })
    .map(function (c) {
      return { name: String(c.name).trim().slice(0, 60),
               owner: c.owner ? String(c.owner).trim().slice(0, 120) : undefined,
               notifyOnStall: !!c.notifyOnStall };
    });
  audit(state, 'stream.consumers_set', id, { count: s.consumers.length });
  return s.consumers;
}

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
                'state', 'topic', 'range', 'table', 'op', 'from', 'global'];

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
  tick: tick, rebaseToNow: rebaseToNow, ratePerTick: ratePerTick, lagFromPending: lagFromPending, isLive: isLive,

  // reads
  kpis: kpis, sortStreams: sortStreams, filterStreams: filterStreams, flowOf: flowOf,
  kpiSeries: kpiSeries, pushKpiSample: pushKpiSample, trend: trend, sparkPoints: sparkPoints,
  failureDetail: failureDetail, revealSample: revealSample, errorClass: errorClass,
  retentionDeadline: retentionDeadline, roughDuration: roughDuration,
  blockedState: blockedState, blockedStreams: blockedStreams,
  blockedConsequence: blockedConsequence, blockedDiagnostics: blockedDiagnostics,
  parseErrorTarget: parseErrorTarget,
  globalPauseState: globalPauseState, pauseAllPreview: pauseAllPreview, pauseAll: pauseAll,
  pauseAllPlan: pauseAllPlan, commitGlobalPause: commitGlobalPause,
  resumeAllPreview: resumeAllPreview, resumeAll: resumeAll,
  resumeAllPlan: resumeAllPlan, commitGlobalResume: commitGlobalResume,
  redactRow: redactRow, consumerImpact: consumerImpact, durationWords: durationWords,
  SPARK_MIN_POINTS: SPARK_MIN_POINTS, SPARK_SIGNIFICANT: SPARK_SIGNIFICANT,
  KPI_HISTORY: KPI_HISTORY,
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
  resetToCheckpoint: resetToCheckpoint, setConsumers: setConsumers,
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
