// tests/data-stream.test.js — the Data Stream module.
//
// What is worth pinning here, and why:
//
//   The invariant.   Every number on every screen is read off one equation:
//                    pending(after) = pending(before) + captured − delivered.
//                    Lag, throughput, store depth and the Flow view's
//                    bottleneck all derive from it. If it drifts, the Monitor
//                    and the Streams list start telling different stories
//                    about the same stream, and nobody can tell which is
//                    right. So it is asserted exactly, not approximately.
//
//   Determinism.     The brief asks for reproducible figures AND for a paused
//                    stream to still be paused after a refresh. Those pull
//                    against each other unless the randomness is a pure
//                    function of a tick number. Two independent seeds must
//                    agree exactly, and Math.random must never be called.
//
//   URL safety.      Every workflow state is addressable, which is only safe
//                    to paste to a colleague if identifiers travel and data
//                    never does. buildQuery is the choke point, so it is
//                    tested as one.
//
//   The guardrail.   The assistant may draft and open. It may not start,
//                    pause, replay or delete — those change a production
//                    database. That is a claim about the action catalogue,
//                    so it is checked against the catalogue.
'use strict';

const fs = require('fs');
const path = require('path');

const P = (...p) => path.join(__dirname, '..', ...p);
const read = (...p) => fs.readFileSync(P(...p), 'utf8');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 220) : '')); }
};

console.log('Data Stream — capture, Stream Store, delivery\n');

/* localStorage stub: the engine persists, and the persistence path is part of
   what is being tested (a paused stream must survive a reload). */
const store = {};
global.localStorage = {
  getItem: (k) => (k in store ? store[k] : null),
  setItem: (k, v) => { store[k] = String(v); },
  removeItem: (k) => { delete store[k]; },
  get length() { return Object.keys(store).length; },
  key: (i) => Object.keys(store)[i] || null,
};

const DS = require('../public/cygenix-datastream.js');
const U  = require('../public/cygenix-datastream-ui.js');

/* ══ 1. Determinism ══════════════════════════════════════════════════════ */

check('the engine loads and exports its enums', !!(DS && DS.ENUMS && DS.ENUMS.status.length === 7));

{
  const a = DS.seedDemo('proj_t', { now: Date.parse('2026-08-24T09:41:07Z') });
  const b = DS.seedDemo('proj_t', { now: Date.parse('2026-08-24T09:41:07Z') });
  check('two fresh seeds produce identical streams — screenshots reproduce',
    JSON.stringify(a.streams) === JSON.stringify(b.streams));
  check('and identical change records',
    JSON.stringify(a.events) === JSON.stringify(b.events));
  check('the demo covers every status the UI has a colour for',
    ['running', 'lagging', 'paused', 'failed', 'draft']
      .every(s => a.streams.some(x => x.status === s)),
    a.streams.map(s => s.status).join(','));
  check('and both sides, because "which side" is the first question asked',
    a.streams.some(s => s.capture.side === 'source') && a.streams.some(s => s.capture.side === 'target'));
}

{
  // A sequential generator would make a refresh resume mid-sequence and the
  // figures would differ. Every draw names what it is for instead.
  const src = read('public', 'cygenix-datastream.js');
  check('Math.random is never called — the demo world is a function of the tick',
    !/Math\.random\s*\(/.test(src.replace(/\/\*[\s\S]*?\*\//g, '')));
  check('draws are hashed from (seed, tick, stream, purpose), not a running counter',
    /function hash32/.test(src) && /rand\(n, s\.id, '/.test(src));
  const r1 = DS.rand(5, 'str_a', 'in');
  const r2 = DS.rand(5, 'str_a', 'in');
  const r3 = DS.rand(5, 'str_a', 'out');
  check('the same draw twice gives the same number', r1 === r2);
  check('a different purpose gives a different number', r1 !== r3);
  check('draws land inside [0,1)', r1 >= 0 && r1 < 1 && r3 >= 0 && r3 < 1);
}

/* ══ 2. The invariant ════════════════════════════════════════════════════ */
{
  const s = DS.seedDemo('proj_inv', { now: Date.parse('2026-08-24T09:00:00Z') });
  let violations = 0, checked = 0;
  for (let t = 0; t < 40; t++) {
    const before = {};
    s.streams.forEach(x => { before[x.id] = x.metrics.pendingInStore; });
    const pointsBefore = s.points.length;
    DS.tick(s, { at: Date.parse('2026-08-24T09:00:00Z') + t * DS.TICK_MS });
    const fresh = s.points.slice(pointsBefore);
    fresh.forEach(p => {
      const stream = DS.getStream(s, p.streamId);
      if (!DS.isLive(stream.status) && stream.status !== 'failed') return;
      checked++;
      // pending(after) = pending(before) + in − out, exactly.
      if (before[p.streamId] + p.in - p.out !== stream.metrics.pendingInStore) violations++;
    });
  }
  check('in − out is exactly the change in store depth, every tick, every stream',
    violations === 0 && checked > 50, violations + ' violation(s) over ' + checked + ' checks');

  // Records are counted, not measured. A rate per tick is fractional by
  // nature; letting one reach a record count printed
  // "357.66666666666663 pending" on the Flow diagram.
  const fractional = [];
  s.streams.forEach(x => {
    ['pendingInStore', 'storeRecords', 'deliveredToday', 'dlqDepth', 'failedToday', 'lagSeconds']
      .forEach(k => { if (!Number.isInteger(x.metrics[k])) fractional.push(x.name + '.' + k + '=' + x.metrics[k]); });
  });
  check('every record count is a whole number — none of them is a rate in disguise',
    fractional.length === 0, fractional.join(', '));
}

{
  const s = DS.seedDemo('proj_lag', { now: Date.parse('2026-08-24T09:00:00Z') });
  const lagging = s.streams.filter(x => x.status === 'lagging')[0];
  const before = lagging.metrics.lagSeconds;
  for (let t = 0; t < 30; t++) DS.tick(s, { at: Date.parse('2026-08-24T09:00:00Z') + t * DS.TICK_MS });
  check('a stream whose delivery cannot keep up falls further behind',
    lagging.metrics.lagSeconds >= before, before + ' → ' + lagging.metrics.lagSeconds);

  // Recovery is DERIVED from the numbers, never set by hand — so a stream
  // announces its own recovery rather than waiting to be told.
  lagging.profile.capacityRate = 5000;
  for (let t = 0; t < 60; t++) DS.tick(s, { at: Date.parse('2026-08-24T09:10:00Z') + t * DS.TICK_MS });
  check('and recovers to Running on its own once delivery catches up',
    lagging.status === 'running' && lagging.metrics.lagSeconds <= 30,
    lagging.status + ' lag=' + lagging.metrics.lagSeconds);
  check('the recovery is written to the audit trail',
    s.audit.some(a => a.action === 'stream.recovered'));
}

{
  const s = DS.seedDemo('proj_paused', { now: Date.parse('2026-08-24T09:00:00Z') });
  const paused = s.streams.filter(x => x.status === 'paused')[0];
  const held = paused.metrics.pendingInStore;
  for (let t = 0; t < 20; t++) DS.tick(s, { at: Date.parse('2026-08-24T09:00:00Z') + t * DS.TICK_MS });
  check('a paused stream captures nothing', paused.metrics.eventsPerMin === 0);
  check('but keeps what is already in the Stream Store — that is why pausing is safe',
    paused.metrics.pendingInStore === held);

  const failed = s.streams.filter(x => x.status === 'failed')[0];
  check('a failed stream keeps capturing and its backlog grows',
    failed.metrics.pendingInStore > 0 && failed.metrics.eventsPerMin > 0);
  check('and its dead-letter queue fills, which is what the operator must see',
    failed.metrics.dlqDepth > 0);
}

/* ══ 3. Persistence — a state change must survive a refresh ═════════════ */
{
  const s = DS.seedDemo('proj_persist', { now: Date.parse('2026-08-24T09:00:00Z') });
  const running = s.streams.filter(x => x.status === 'running')[0];
  DS.pauseStream(s, running.id);
  DS.save(s);

  const reloaded = DS.load('proj_persist');
  const same = DS.getStream(reloaded, running.id);
  check('a pause survives a reload — the state is stored, not simulated afresh',
    same && same.status === 'paused');
  check('and the tick number carries, so the world resumes rather than restarts',
    reloaded.tickNo === s.tickNo && reloaded.tickNo > 0);
  check('the pause is in the audit trail with the backlog it left behind',
    reloaded.audit.some(a => a.action === 'stream.paused' && a.detail
      && typeof a.detail.pendingInStore === 'number'));

  DS.replay(reloaded, running.id, { kind: 'retention' });
  DS.save(reloaded);
  check('a replay survives a reload too',
    DS.load('proj_persist').audit.some(a => a.action === 'stream.replay'));
}

{
  // The retained tail is capped, or localStorage grows without limit.
  const s = DS.seedDemo('proj_cap', { now: Date.parse('2026-08-24T09:00:00Z') });
  for (let t = 0; t < 200; t++) DS.tick(s, { at: Date.parse('2026-08-24T09:00:00Z') + t * DS.TICK_MS });
  DS.save(s);
  check('the in-memory event buffer is capped at 500 records',
    s.events.length <= DS.MAX_EVENTS && DS.MAX_EVENTS === 500, s.events.length);
  check('and the metric history is capped too', s.points.length <= 720, s.points.length);
}

/* ══ 4. Actions and their guard rails ════════════════════════════════════ */
{
  const s = DS.seedDemo('proj_act', { now: Date.parse('2026-08-24T09:00:00Z') });
  const draftStream = s.streams.filter(x => x.status === 'draft')[0];
  check('the demo includes a draft — created but deliberately not started',
    !!draftStream && !draftStream.startedAt);

  DS.startStream(s, draftStream.id);
  check('starting a draft with no snapshot goes straight to running, with a start time',
    draftStream.status === 'running' && !!draftStream.startedAt, draftStream.status);

  const snap = s.streams.filter(x => x.capture.snapshot === 'initial-then-stream')[0];
  DS.startStream(s, snap.id);
  check('starting one that snapshots first reports Snapshotting, not Running',
    snap.status === 'snapshotting');
  check('and its objects say so too, so the inspector can show progress',
    snap.objects.every(o => o.state === 'snapshotting'));
  for (let t = 0; t < 40; t++) DS.tick(s, { at: Date.parse('2026-08-24T10:00:00Z') + t * DS.TICK_MS });
  check('the snapshot finishes and the stream becomes Running by itself',
    snap.status === 'running', snap.status);

  const before = s.streams.length;
  DS.deleteStream(s, draftStream.id);
  check('deleting removes the stream', s.streams.length === before - 1);
  check('and its retained records with it — nothing orphaned in the store',
    !s.events.some(e => e.streamId === draftStream.id)
    && !s.points.some(p => p.streamId === draftStream.id));
  check('the deletion is audited with the name, since the id means nothing afterwards',
    s.audit.some(a => a.action === 'stream.deleted' && a.detail && a.detail.name));
}

{
  const s = DS.seedDemo('proj_replay', { now: Date.parse('2026-08-24T09:00:00Z') });
  const st = s.streams[0];
  const all = DS.replayCount(s, st.id, { kind: 'retention' });
  const some = DS.replayCount(s, st.id, { kind: 'time', at: new Date(s.clockNow - 3600000).toISOString() });
  check('replaying from the beginning of retention counts everything retained',
    all === st.metrics.storeRecords);
  check('replaying from an hour ago counts less than everything',
    some < all && some >= 0, some + ' vs ' + all);

  const pendingBefore = st.metrics.pendingInStore;
  const out = DS.replay(s, st.id, { kind: 'retention' });
  check('a replay puts the records back in the queue to be delivered again',
    st.metrics.pendingInStore === pendingBefore + all && out.records === all);
  check('and names the destination it will re-deliver to',
    out.destination === st.destination.label);

  const failed = s.streams.filter(x => x.status === 'failed')[0];
  const dlq = failed.metrics.dlqDepth;
  const pend = failed.metrics.pendingInStore;
  DS.requeueDlq(s, failed.id);
  check('requeueing dead letters empties the queue into the pending count',
    failed.metrics.dlqDepth === 0 && failed.metrics.pendingInStore === pend + dlq);
}

{
  const s = DS.seedDemo('proj_ret', { now: Date.parse('2026-08-24T09:00:00Z') });
  const st = s.streams[0];
  const grow = DS.retentionImpact(s, st.id, 200);
  check('lengthening retention loses nothing', grow.losesHistory === false && grow.recordsLost === 0);
  const shrink = DS.retentionImpact(s, st.id, 24);
  check('shortening it says how much replay history goes',
    shrink.losesHistory === true && shrink.recordsLost > 0 && shrink.hoursLost === 48,
    JSON.stringify(shrink));
  const recordsBefore = st.metrics.storeRecords;
  DS.setRetention(s, st.id, 24);
  check('and applying it actually drops those records rather than only warning',
    st.metrics.storeRecords < recordsBefore && st.profile.retentionHours === 24);
  check('the change is audited with both the old and new window',
    s.audit.some(a => a.action === 'stream.retention' && a.detail.from === 72 && a.detail.to === 24));
}

{
  const s = DS.seedDemo('proj_cut', { now: Date.parse('2026-08-24T09:00:00Z') });
  const st = s.streams.filter(x => x.status === 'running')[0];
  const plan = DS.cutoverPlan(s, st.id);
  check('the cutover plan is three steps in the order that cannot lose data',
    plan.steps.length === 3 && /Pause capture/.test(plan.steps[0])
    && /drain/i.test(plan.steps[1]) && /stop/i.test(plan.steps[2]));
  DS.beginCutover(s, st.id);
  check('beginning a cutover stops CAPTURE, not delivery',
    st.profile.baseRate === 0 && st.status === 'running');
  for (let t = 0; t < 200; t++) DS.tick(s, { at: Date.parse('2026-08-24T11:00:00Z') + t * DS.TICK_MS });
  check('the Stream Store drains to zero and the delta window closes',
    st.metrics.pendingInStore === 0 && DS.cutoverComplete(s, st.id) === true,
    'pending=' + st.metrics.pendingInStore);
}

/* ══ 5. Derived reads ════════════════════════════════════════════════════ */
{
  const s = DS.seedDemo('proj_read', { now: Date.parse('2026-08-24T09:00:00Z') });
  const k = DS.kpis(s);
  check('the KPI strip counts running, paused and failed against the total',
    k.running + k.paused + k.failed <= k.total && k.total === s.streams.length);
  check('max lag names the stream it belongs to, so the figure is actionable',
    typeof k.worstStream === 'string' && k.worstStream.length > 0);

  const sorted = DS.sortStreams(s.streams);
  check('sorting puts what needs attention first — failed before running',
    sorted[0].status === 'failed', sorted.map(x => x.status).join(','));
  check('and a draft last, since nothing is happening to it',
    sorted[sorted.length - 1].status === 'draft');

  check('filtering by side does what it says',
    DS.filterStreams(s.streams, { side: 'target' }).every(x => x.capture.side === 'target'));
  check('filtering by text searches the tables a stream carries, not just its name',
    DS.filterStreams(s.streams, { text: 'order_line' }).length > 0);
  check('the errors-only filter finds the failed stream',
    DS.filterStreams(s.streams, { errorsOnly: true }).some(x => x.status === 'failed'));
}

{
  const s = DS.seedDemo('proj_flow', { now: Date.parse('2026-08-24T09:00:00Z') });
  const lagging = s.streams.filter(x => x.status === 'lagging')[0];
  const flow = DS.flowOf(lagging);
  check('the Flow view names delivery as the bottleneck when it cannot keep up',
    flow.bottleneck === 'delivery', flow.bottleneck + ' in=' + flow.capture.rate + ' out=' + flow.destination.rate);
  const healthy = s.streams.filter(x => x.status === 'running')[0];
  check('and names nothing when the hops are keeping pace',
    DS.flowOf(healthy).bottleneck === null);
  check('a failed stream delivers at zero, which is the honest number',
    DS.flowOf(s.streams.filter(x => x.status === 'failed')[0]).destination.rate === 0);
}

{
  const s = DS.seedDemo('proj_store', { now: Date.parse('2026-08-24T09:00:00Z') });
  const topics = DS.topicsOf(s);
  const objects = s.streams.reduce((a, x) => a + x.objects.length, 0);
  check('the Stream Store has one topic per streamed object', topics.length === objects);
  check('every topic names its stream, its checkpoint and its consumer lag',
    topics.every(t => t.streamId && t.topic && t.consumers.length === 1
      && typeof t.consumers[0].offsetBehind === 'number'));
  check('a topic is prefixed by the side it reads, so src and tgt never collide',
    topics.some(t => t.topic.indexOf('src.') === 0) && topics.some(t => t.topic.indexOf('tgt.') === 0));
  // Two streams carrying the same table from the same side is ordinary — one
  // into a warehouse, one into a broker. If they shared a topic name, a topic
  // would no longer identify a checkpoint, a consumer or a retention window.
  check('every topic name is unique, even across streams carrying the same table',
    new Set(topics.map(t => t.topic)).size === topics.length,
    topics.map(t => t.topic).join(', '));
  check('and the topic name carries its stream, which is what makes it unique',
    topics.every(t => t.topic.indexOf(String(t.streamId).replace('str_', '')) !== -1),
    topics[0].topic);

  const totals = DS.storeTotals(s);
  check('the totals add up to the topics',
    totals.records === topics.reduce((a, t) => a + t.records, 0) && totals.topics === topics.length);
}

{
  const s = DS.seedDemo('proj_alert', { now: Date.parse('2026-08-24T09:00:00Z') });
  const alerts = DS.alertsOf(s);
  check('there are four alert rules, each with a state and the streams firing it',
    alerts.length === 4 && alerts.every(a => a.state && Array.isArray(a.streams)));
  const dlq = alerts.filter(a => a.id === 'dlq')[0];
  check('the dead-letter rule fires because a stream actually has dead letters',
    dlq.state === 'firing' && dlq.count > 0);
  const drift = alerts.filter(a => a.id === 'drift')[0];
  check('and the schema-drift rule does not fire, because nothing has drifted',
    drift.state === 'ok');
}

{
  const s = DS.seedDemo('proj_heat', { now: Date.parse('2026-08-24T09:00:00Z') });
  for (let t = 0; t < 30; t++) DS.tick(s, { at: Date.parse('2026-08-24T09:00:00Z') + t * DS.TICK_MS });
  const grid = DS.heatGrid(s, 300000, 12);
  check('the heat grid is table × bucket with a peak to scale against',
    grid.rows.length > 0 && grid.rows[0].cells.length === 12 && grid.peak > 0);
  check('and the hottest table is first, which is the whole point of a heat grid',
    grid.rows.every((r, i) => i === 0 || grid.rows[i - 1].total >= r.total));
}

/* ══ 6. Continuous queries — real, or refused ════════════════════════════ */
{
  const s = DS.seedDemo('proj_q', { now: Date.parse('2026-08-24T09:00:00Z') });
  for (let t = 0; t < 20; t++) DS.tick(s, { at: Date.parse('2026-08-24T09:00:00Z') + t * DS.TICK_MS });
  check('three canned queries ship, each with its SQL', DS.CANNED_QUERIES.length === 3
    && DS.CANNED_QUERIES.every(q => q.sql && q.label));

  const byTable = DS.runCannedQuery(s, 'by-table', { now: s.clockNow });
  check('the per-table count runs against the retained records for real',
    byTable.rows.length > 0 && byTable.columns[0] === 'table_name');
  check('and is ordered by volume, descending',
    byTable.rows.every((r, i) => i === 0 || byTable.rows[i - 1][1] >= r[1]));

  const tumbling = DS.runCannedQuery(s, 'tumbling', { now: s.clockNow });
  check('the tumbling window buckets by minute and operation',
    tumbling.columns.join(',') === 'window_start,op,events' && tumbling.rows.length > 0);

  const joined = DS.runCannedQuery(s, 'join', { now: s.clockNow });
  check('the stream-to-table join returns the target row count alongside the change',
    joined.columns.indexOf('target_rows') !== -1);

  let refused = false;
  try { DS.runCannedQuery(s, 'select * from anything'); } catch (e) { refused = /not part of this build/.test(e.message); }
  check('anything else is refused rather than faked — no half-built SQL engine', refused);
}

/* ══ 7. Designer — validation, the sentence, the capability check ═══════ */
{
  const blank = DS.blankDraft('proj_d');
  const v0 = DS.validateDesign(blank);
  check('a blank draft is invalid and points at step 1',
    !v0.valid && v0.firstInvalidStep === 1);

  const d = DS.blankDraft('proj_d');
  d.name = 'Source → Reporting warehouse';
  d.capture.connectionId = 'conn_src_01';
  d.capture.connectionLabel = 'LEGACY-SQL01 · dbo';
  d.destination.label = 'SNOWFLAKE-EU · RAW_LEGACY';
  d.objects = [{ table: 'dbo.customers', keys: ['id'], ops: ['I', 'U', 'D'],
                 masked: ['email'], rowsEstimate: 2600 }];
  const v1 = DS.validateDesign(d);
  check('a complete draft validates', v1.valid, JSON.stringify(v1.errors));

  // A key is not a nicety under upsert — it is the thing upsert works on.
  const noKey = JSON.parse(JSON.stringify(d));
  noKey.objects[0].keys = [];
  const v2 = DS.validateDesign(noKey);
  check('upsert without a key column is blocked, and the message says why',
    !v2.valid && v2.errors[1].some(e => /no key column/.test(e)), JSON.stringify(v2.errors[1]));
  noKey.destination.writeMode = 'append';
  check('the same draft is valid as an append — the block was about upsert, not fuss',
    DS.validateDesign(noKey).valid);

  // Warnings do not block. Judgement calls belong to the operator.
  const big = JSON.parse(JSON.stringify(d));
  big.objects[0].rowsEstimate = 5000000;
  const v3 = DS.validateDesign(big);
  check('a very large snapshot warns without blocking',
    v3.valid && v3.warnings.some(w => /take a while/.test(w)));
  const sensitive = JSON.parse(JSON.stringify(d));
  sensitive.objects[0].masked = [];
  sensitive.objects[0].sensitiveColumns = ['email'];
  check('an unmasked sensitive column raises a governance warning at step 2',
    DS.validateDesign(sensitive).warnings.some(w => /classified as sensitive/.test(w)));

  const sentence = DS.reviewSentence(d);
  check('the review sentence names the connection, the side, the method and the destination',
    /LEGACY-SQL01/.test(sentence) && /source/.test(sentence)
    && /log-based cdc/i.test(sentence) && /SNOWFLAKE-EU/.test(sentence), sentence);
  check('and states plainly whether deletes are applied',
    /Deletes are applied\./.test(sentence), sentence);
  const noDel = JSON.parse(JSON.stringify(d));
  noDel.objects[0].ops = ['I', 'U'];
  check('saying the opposite when they are not',
    /Deletes are not captured\./.test(DS.reviewSentence(noDel)));
  check('and counting the masked columns',
    /1 column is masked\./.test(sentence), sentence);

  const est = DS.snapshotEstimate(d);
  check('the snapshot estimate states the read load before anyone presses start',
    est.rows === 2600 && /read load on LEGACY-SQL01/.test(est.note), est.note);
  const none = JSON.parse(JSON.stringify(d));
  none.capture.snapshot = 'none';
  check('and says so plainly when there is no snapshot at all',
    DS.snapshotEstimate(none).rows === 0);
}

{
  // Never silently downgrade a capture method: say what is wrong and offer
  // the two honest remedies.
  const bad = DS.capabilityCheck({ label: 'LEGACY-SQL01', cdcEnabled: false }, 'log');
  check('log-based capture on a database without CDC fails the check',
    bad.ok === false && bad.findings[0].level === 'error');
  check('and names the database rather than saying "not supported"',
    /LEGACY-SQL01/.test(bad.findings[0].text), bad.findings[0].text);
  check('with two remedies: the SQL to enable it, or polling chosen deliberately',
    bad.remedies.length === 2 && bad.remedies[0].sql && bad.remedies[1].method === 'poll');
  check('the SQL is the real enable path, not a placeholder',
    /sp_cdc_enable_db/.test(bad.remedies[0].sql));
  check('and it is engine-specific — Postgres gets a replication slot',
    /pg_create_logical_replication_slot/.test(DS.enableCdcSql('postgres'))
    && /binlog/.test(DS.enableCdcSql('mysql')));

  const ok = DS.capabilityCheck({ label: 'AZSQL-TARGET', cdcEnabled: true }, 'log');
  check('a database with CDC on passes', ok.ok === true && !ok.remedies);
  const poll = DS.capabilityCheck({ label: 'LEGACY-SQL01' }, 'poll');
  check('polling passes but is honest that it cannot see hard deletes',
    poll.ok === true && poll.findings.some(x => /hard deletes/.test(x.text)));
}

{
  const d = DS.blankDraft('proj_n');
  d.destination.objectPrefix = 'stg_';
  d.destination.schemaOverride = 'RAW_LEGACY';
  check('a destination name resolves prefix and schema together',
    DS.resolveObjectName(d, 'dbo.customers') === 'RAW_LEGACY.stg_customers');
  d.destination.overrides = { 'dbo.customers': 'CUSTOM_NAME' };
  check('and a per-table override beats both',
    DS.resolveObjectName(d, 'dbo.customers') === 'CUSTOM_NAME');
}

/* ══ 8. Routing — identifiers travel, data never does ═══════════════════ */
{
  const q = DS.parseQuery('?stream=str_7k2m&view=flow&step=3');
  check('query parameters parse into the state a screen opens with',
    q.stream === 'str_7k2m' && q.view === 'flow' && q.step === '3');
  check('an unknown parameter is dropped rather than trusted',
    DS.parseQuery('?stream=a&evil=1').evil === undefined);

  check('building a URL keeps only the allow-listed keys',
    DS.buildQuery({ stream: 'str_1', before: { name: 'Acme' }, key: 42 }) === '?stream=str_1');
  check('and drops empty values instead of writing ?stream=',
    DS.buildQuery({ stream: '', view: 'flow' }) === '?view=flow');
  check('a value is encoded, so a table name with a space cannot break the link',
    DS.buildQuery({ table: 'dbo.my table' }) === '?table=dbo.my%20table');

  // The rule the whole deep-linking design rests on.
  check('an id, a table name and a view token are all safe to put in a URL',
    ['str_7k2m', 'dbo.customers', 'flow', 'conn_src_01'].every(DS.isSafeUrlValue));
  check('a row payload is not',
    !DS.isSafeUrlValue('{"name":"Acme Ltd","email":"a@b.c"}')
    && !DS.isSafeUrlValue('x'.repeat(300)));

  const round = DS.parseQuery(DS.buildQuery({ stream: 'str_a', state: 'dead', topic: 'src.dbo.customers' }));
  check('a built URL parses back to what went in',
    round.stream === 'str_a' && round.state === 'dead' && round.topic === 'src.dbo.customers');
}

/* ══ 9. Shared UI vocabulary ════════════════════════════════════════════ */
{
  const s = DS.seedDemo('proj_ui', { now: Date.parse('2026-08-24T09:00:00Z') });
  const lagging = s.streams.filter(x => x.status === 'lagging')[0];

  // Colour is never the only carrier: a pill has to say its state in words.
  DS.ENUMS.status.forEach(st => {
    const pill = U.statusPill({ status: st, metrics: { lagSeconds: 5 }, objects: [], snapshotDone: 0 });
    if (!/>[^<]*[A-Za-z]/.test(pill.replace(/<i[^>]*><\/i>/, ''))) {
      check('status "' + st + '" has a text label, not colour alone', false, pill);
    }
  });
  check('every status pill carries its own words, so colour is never the only signal', true);
  check('a lagging pill states how far behind it is',
    /Lagging/.test(U.statusPill(lagging)) && /\d/.test(U.statusPill(lagging)));

  check('SOURCE is teal and TARGET is purple, from one definition',
    /ds-side-source/.test(U.sideBadge('source')) && /SOURCE/.test(U.sideBadge('source'))
    && /ds-side-target/.test(U.sideBadge('target')) && /TARGET/.test(U.sideBadge('target')));

  check('lag colours against the STREAM\'s threshold, not a global one',
    /ds-lag-amber/.test(U.lagCell({ status: 'running', metrics: { lagSeconds: 40 },
      delivery: { lagThresholdSeconds: 30 } }))
    && /ds-lag-ok|ds-lag"/.test(U.lagCell({ status: 'running', metrics: { lagSeconds: 40 },
      delivery: { lagThresholdSeconds: 300 } })));
  check('a stream that is not live shows no lag figure at all, rather than a stale one',
    /—/.test(U.lagCell({ status: 'paused', metrics: { lagSeconds: 99 }, delivery: {} })));

  check('operation badges are letters with a title, not colours',
    /title="Insert"/.test(U.opBadge('I')) && />I</.test(U.opBadge('I'))
    && /title="Schema change"/.test(U.opBadge('DDL')));

  check('markup is escaped — a table called <script> cannot execute',
    U.esc('<script>alert(1)</script>').indexOf('<script') === -1);
  check('and a name with an apostrophe cannot break out of an onclick argument',
    U.escArg("O'Brien") === "O\\'Brien");

  const spark = U.sparkline([1, 5, 3, 9, 2]);
  check('a sparkline is inline SVG with no external dependency',
    /<svg/.test(spark) && /polyline/.test(spark) && !/http/.test(spark));
  check('and is hidden from screen readers, since the figure beside it is the content',
    /aria-hidden="true"/.test(spark));
  check('too few points draws nothing rather than a misleading flat line',
    !/polyline/.test(U.sparkline([4])));

  const chart = U.lineChart([1, 2, 3], { threshold: 2, label: 'Lag' });
  check('a line chart draws its threshold as a dashed rule',
    /ds-chart-rule/.test(chart) && /aria-label="Lag"/.test(chart));

  const flow = U.flowDiagram(lagging);
  check('the Flow diagram is three nodes and two arrows',
    (flow.match(/ds-flow-node/g) || []).length === 3
    && (flow.match(/ds-flow-arrow/g) || []).length === 2);
  check('and marks the real bottleneck rather than decorating both arrows',
    (flow.match(/ds-flow-arrow hot/g) || []).length === 1);
  check('the Stream Store node states what it is holding and for how long',
    /STREAM STORE/.test(flow) && /pending/.test(flow) && /retention/.test(flow));

  check('a relative timestamp carries the absolute instant in its title',
    /title="2026-08-24T09:00:00.000Z"/.test(U.timeAgo('2026-08-24T09:00:00.000Z', Date.parse('2026-08-24T09:00:30Z'))));
}

{
  // A destructive confirmation states the blast radius. "Are you sure?" is a
  // speed bump; the number is the confirmation.
  const replay = U.confirmText('replay', { records: 4182, destination: 'SNOWFLAKE-EU' });
  check('a replay confirmation names the record count and the destination',
    /4,182/.test(replay) && /SNOWFLAKE-EU/.test(replay), replay);
  const del = U.confirmText('delete', { name: 'X' });
  check('a delete confirmation says what is NOT touched, which is the real worry',
    /source and destination databases are not touched/.test(del));
  const pause = U.confirmText('pause', { name: 'X', pending: 118 });
  check('a pause confirmation says the backlog is kept',
    /118/.test(pause) && /kept/.test(pause));
  const ret = U.confirmText('retention', { from: 72, to: 24, recordsLost: 900 });
  check('shortening retention says how much replay history is lost',
    /900/.test(ret) && /no longer be able to replay/.test(ret));
  const requeue = U.confirmText('requeue', { records: 23, destination: 'X' });
  check('a requeue confirmation is honest that they failed once already',
    /failed once already/.test(requeue));
}

/* ══ 10. Streams ↔ Batches ══════════════════════════════════════════════ */
{
  const s = DS.seedDemo('proj_x', { now: Date.parse('2026-08-24T09:00:00Z') });
  const carrying = DS.streamsForTable(s, 'dbo.customers');
  check('a table can be traced back to the streams carrying it',
    carrying.length > 0 && carrying.every(x => x.objects.some(o => o.table === 'dbo.customers')));
  check('the lookup is case-insensitive, because table names are written both ways',
    DS.streamsForTable(s, 'DBO.CUSTOMERS').length === carrying.length);
  check('a table nothing streams returns nothing rather than throwing',
    DS.streamsForTable(s, 'dbo.nothing_here').length === 0);
}

/* ══ 11. Wiring — nav, pages, assistant ═════════════════════════════════ */
const PAGES = ['data_stream.html', 'data_stream_designer.html', 'data_stream_events.html',
               'data_stream_store.html', 'data_stream_monitor.html'];
{
  PAGES.forEach(f => {
    check(f + ' exists', fs.existsSync(P('public', f)));
  });

  const sidebar = read('public', 'cygenix-sidebar.js');
  check('Data Stream is a group in the RUN section',
    /key:'datastream-group'/.test(sidebar) && /label:'Data Stream'/.test(sidebar));
  check('with all four children pointing at their pages',
    ['data-stream', 'data-stream-store', 'data-stream-events', 'data-stream-monitor']
      .every(h => sidebar.indexOf("href:'/" + h + "'") !== -1));

  // The Designer is reachable, shareable and refreshable — but not a rail
  // entry, because it is a step inside the Streams workflow.
  check('the Designer is NOT a sidebar entry', sidebar.indexOf('data_stream_designer.html') === -1);
  check('but the Streams screen links to it',
    /\/data-stream-designer\b/.test(read('public', 'data_stream.html')));

  // Placement: between Jobs and Task Manager, per the brief.
  const runSection = sidebar.slice(sidebar.indexOf("section: 'Run'"), sidebar.indexOf("section: 'Validate'"));
  check('it sits after Jobs and before Task Manager',
    runSection.indexOf('jobs-group') < runSection.indexOf('datastream-group')
    && runSection.indexOf('datastream-group') < runSection.indexOf('task-agent'));
}

{
  // Every module page must carry the console's chrome, or it is a different
  // application that happens to share a URL.
  PAGES.forEach(f => {
    const src = read('public', f);
    const missing = [];
    if (!/id="cyg-sidebar-mount"/.test(src)) missing.push('sidebar mount');
    if (!/cygenix-icons\.css/.test(src)) missing.push('icons');
    if (!/cygenix-mobile\.css/.test(src)) missing.push('mobile');
    if (!/cygenix-datastream\.css/.test(src)) missing.push('module css');
    if (!/name="viewport"/.test(src)) missing.push('viewport');
    if (!/CygenixAssistant\.registerPage/.test(src)) missing.push('assistant');
    if (!/cygenix-datastream\.js/.test(src)) missing.push('engine');
    check(f + ' carries the console chrome', missing.length === 0, missing.join(', '));
  });

  // The engine must load before the page module that calls into it.
  PAGES.forEach(f => {
    const src = read('public', f);
    check(f + ' loads the engine before the page boot that uses it',
      src.indexOf('cygenix-datastream.js') < src.indexOf('cygenix-datastream-page.js'));
  });

  // One <style> per page holds only page-local rules; the shared vocabulary
  // is one file, so five screens cannot drift apart.
  const css = read('public', 'cygenix-datastream.css');
  ['.ds-pill', '.ds-side-source', '.ds-side-target', '.ds-flow', '.ds-kpi', '.ds-op-I']
    .forEach(sel => check('the shared stylesheet defines ' + sel, css.indexOf(sel) !== -1));
  PAGES.forEach(f => {
    const style = (read('public', f).match(/<style>([\s\S]*?)<\/style>/) || [])[1] || '';
    check(f + ' does not redefine the shared status pills locally',
      style.indexOf('.ds-st-running') === -1 && style.indexOf('.ds-side-source') === -1);
  });
}

{
  const actions = read('public', 'cygenix-assistant-actions.js');
  ['stream_list', 'stream_describe', 'stream_explain_lag', 'stream_draft'].forEach(n =>
    check('the assistant can ' + n.replace(/_/g, ' '), new RegExp("name: '" + n + "'").test(actions)));

  // The guard rail this module rests on: draft and open, never start.
  ['stream_start', 'stream_pause', 'stream_replay', 'stream_delete', 'stream_stop'].forEach(n =>
    check('the assistant deliberately cannot ' + n.replace(/_/g, ' '),
      actions.indexOf("name: '" + n + "'") === -1));
  check('and the reason is written down where the next person will read it',
    /destructive at BOTH guardrail levels/.test(actions));
  check('drafting a stream creates and starts nothing',
    /creates NOTHING and starts NOTHING/.test(actions));

  // Tool names must match Anthropic's ^[a-zA-Z0-9_-]{1,64}$ — a dotted name
  // is a 400 from the real API, which is how this was learned.
  const names = [...actions.matchAll(/name: '([a-z_]+)',\n\s+title:/g)].map(m => m[1]);
  check('every action name is a legal tool name',
    names.length > 10 && names.every(n => /^[a-zA-Z0-9_-]{1,64}$/.test(n)), names.join(','));

  // Everything an action returns goes to the model. Connection labels are
  // fine; connection strings are not.
  check('the assistant\'s stream summaries carry no credentials',
    !/connectionString|fnKey|password|secret/i.test(
      actions.slice(actions.indexOf('function dsSummary'), actions.indexOf('A.registerActions([\n  {\n    name: \'stream_list\''))));

  const pages = [...actions.matchAll(/key: '(data-stream[a-z-]*)'/g)].map(m => m[1]);
  check('all four screens are in the assistant\'s app map',
    ['data-stream', 'data-stream-store', 'data-stream-events', 'data-stream-monitor']
      .every(k => pages.indexOf(k) !== -1), pages.join(','));
}

{
  // Cross-module links, both directions.
  const se = read('public', 'schema_explorer.html');
  check('the Schema Explorer can send a table to a new stream',
    /Add to data stream/.test(se) && /\/data-stream-designer\b/.test(se));
  check('and sends identifiers only — a connection id and a table name',
    /params\.set\('connection'/.test(se) && /params\.set\('tables'/.test(se)
    && !/params\.set\('rows'|params\.set\('sample'/.test(se));

  const pb = read('public', 'project-builder-app.js');
  check('the Batches screen shows when a table is also being streamed',
    /Also streaming since/.test(pb) && /function streamsCovering/.test(pb));
  check('and offers the cutover assist that closes the delta window',
    /function cutoverAssist/.test(pb) && /beginCutover/.test(pb));
  check('the cross-link is guarded, so a build without the module does not throw',
    /typeof CygenixDataStream === 'undefined'/.test(pb));
  check('and the Batches page actually loads the engine it reads from',
    /cygenix-datastream\.js/.test(read('public', 'project-builder.html')));
}

{
  // The tick runs in ONE place. Five screens each running their own would
  // advance the world five times as fast with five tabs open.
  const pageJs = read('public', 'cygenix-datastream-page.js');
  check('the tick lives in the shared page module, not in the screens',
    /setInterval\(/.test(pageJs) && /DS\.tick\(state\)/.test(pageJs));
  PAGES.forEach(f => {
    const body = (read('public', f).match(/<script>\n'use strict'[\s\S]*/) || [''])[0];
    check(f + ' does not run a tick of its own',
      !/setInterval\s*\(/.test(body) && !/DS\.tick\(/.test(body));
  });
  check('a hidden tab stops ticking rather than simulating events nobody sees',
    /visibilitychange/.test(pageJs) && /document\.hidden/.test(pageJs));
  check('another tab\'s change is picked up rather than overwritten',
    /addEventListener\('storage'/.test(pageJs) && /DS\.load\(projectId\)/.test(pageJs));
}

{
  // The live tail must be pausable and must buffer while paused.
  const events = read('public', 'data_stream_events.html');
  check('the tail can be paused', /function dsToggleTail/.test(events));
  check('and buffers while paused instead of dropping records',
    /bufferedIds\.add/.test(events) && /new event/.test(events));
  check('the buffer count is offered as a way back to live',
    /click to resume/.test(events));
  check('pausing stops the SCREEN, not the world — the engine keeps ticking',
    /Paused means the SCREEN stops, not the world/.test(events));
}

{
  // Storage keys follow the console's convention, and the module writes only
  // its own two.
  check('state is stored per project under the cygenix_ prefix',
    DS.STORE_PREFIX === 'cygenix_datastream_v1::' && DS.WIP_PREFIX === 'cygenix_datastream_wip_');
  check('and the designer draft is a separate key, so an abandoned draft cannot corrupt the streams',
    DS.storeKey('p') !== DS.wipKey('p'));

  DS.wipSave('proj_w', { name: 'half-finished' });
  check('a draft survives being written and read back',
    (DS.wipLoad('proj_w') || {}).name === 'half-finished');
  DS.wipClear('proj_w');
  check('and clears completely', DS.wipLoad('proj_w') === null);
}

/* ══ 12. Formatting ═════════════════════════════════════════════════════ */
{
  check('counts abbreviate the way an operator reads them',
    DS.formatCount(412) === '412' && DS.formatCount(1200) === '1.2k'
    && DS.formatCount(184203) === '184k' && DS.formatCount(2400000) === '2.4M');
  check('lag reads in the unit that fits',
    DS.formatLag(3) === '3s' && DS.formatLag(68) === '1m 8s' && DS.formatLag(7200) === '2h 0m');
  check('bytes too', DS.formatBytes(0) === '0 B' && DS.formatBytes(18213440) === '17.4 MB');
  check('relative time degrades to "never" rather than "NaN ago"',
    DS.relativeTime(null) === 'never' && DS.relativeTime('nonsense') === 'never');
  check('a clock skewed into the future reads as just now, not negative',
    DS.relativeTime(new Date(Date.now() + 60000).toISOString()) === '0s ago');
}

/* ── Enriched stream cards ───────────────────────────────────────────────
   A tile reading 191 says nothing; 191 rising about 2 a minute says the thing
   an operator needs. The trend, the redaction and the impact line are all
   derivations, so they are pinned here rather than eyeballed on a screen that
   changes every two seconds. */
{
  const st = DS.seedDemo('trend');

  /* Sparklines and the trend behind them */
  check('the KPI strip keeps its own history, one sample per tick',
    st.kpiHistory.length > 0 && typeof st.kpiHistory[0].errors === 'number',
    JSON.stringify(st.kpiHistory[0]));
  check('and every tile reads the same history, so two tiles cannot disagree',
    ['running', 'eventsPerMin', 'maxLagSeconds', 'errors']
      .every((k) => DS.kpiSeries(st, k).length === st.kpiHistory.length));

  check('under eight points there is no sparkline — a flat line from three '
      + 'samples is a lie told in a confident font',
    DS.trend([1, 2, 3], false).enough === false);
  check('and the tile says it is still collecting rather than drawing nothing',
    /still collecting/.test(DS.trend([1, 2, 3], false, { label: 'Delivery errors', value: 3 }).sentence));

  const rising = [10, 11, 12, 13, 14, 15, 16, 20];
  const falling = rising.slice().reverse();
  check('errors rising is adverse; errors falling is not',
    DS.trend(rising, false).adverse === true && DS.trend(falling, false).adverse === false);
  check('throughput falling is adverse; throughput rising is not',
    DS.trend(falling, true).adverse === true && DS.trend(rising, true).adverse === false);
  check('a small wobble is not painted red — significance is a 10% move',
    DS.trend([100, 100, 101, 100, 102, 101, 100, 103], false).adverse === false,
    'colour that fires on noise teaches people to ignore colour');
  check('a delta label reads as a rate for errors and a percentage otherwise',
    /\/min$/.test(DS.trend(rising, false, { unit: 'per-min' }).label)
    && /%$/.test(DS.trend(rising, true).label),
    DS.trend(rising, false, { unit: 'per-min' }).label + ' | ' + DS.trend(rising, true).label);
  check('a level series says level rather than +0',
    DS.trend([5, 5, 5, 5, 5, 5, 5, 5], false).label === 'level');

  check('the accessible name is a sentence, not a list of numbers',
    (() => {
      const t = DS.trend(rising, false, { label: 'Delivery errors', value: 194, unit: 'per-min' });
      return /^Delivery errors: 194, rising about [\d.]+ per minute over the last minute\.$/.test(t.sentence);
    })(), DS.trend(rising, false, { label: 'Delivery errors', value: 194, unit: 'per-min' }).sentence);

  check('the polyline spans the box and has a point per sample',
    DS.sparkPoints([0, 5, 10], 100, 24).split(' ').length === 3
    && /^0,/.test(DS.sparkPoints([0, 5, 10], 100, 24))
    && /100,0$/.test(DS.sparkPoints([0, 5, 10], 100, 24)),
    DS.sparkPoints([0, 5, 10], 100, 24));
  check('a genuinely flat series draws down the middle, not along the floor',
    DS.sparkPoints([7, 7, 7], 100, 24) === '0,12 50,12 100,12',
    'a line on the floor reads as zero rather than as unchanging');
  check('one point draws nothing at all', DS.sparkPoints([4], 100, 24) === '');

  /* Failure detail — why, not just where */
  const failed = st.streams.filter((s) => s.status === 'failed')[0];
  const f = DS.failureDetail(st, failed, st.clockNow);
  check('a failure is classified, not just quoted',
    f.errorClass === 'String truncation', f.errorClass + ' ← ' + f.message);
  check('every class the destination can throw maps to words',
    DS.errorClass('FK constraint violated') === 'Constraint violation'
    && DS.errorClass('login failed for user') === 'Permission denied'
    && DS.errorClass('the operation timed out') === 'Timeout'
    && DS.errorClass('connection refused') === 'Connection failure'
    && DS.errorClass(null) === null);
  check('the checkpoint is carried, so an engineer can still replay by position',
    typeof f.checkpoint === 'string' && f.checkpoint.length > 0);
  check('a dead-lettered record is offered as a sample',
    !!f.sample && f.sample.fields.length > 0, JSON.stringify(f.sample && f.sample.fields));
  check('with its field NAMES shown and its values redacted by default',
    f.sample.fields.every((k) => f.sample.redacted[k] === '••••••'),
    'customer rows do not belong in a status screen unless somebody asks');
  check('and the real values only come back from an explicit call',
    (() => {
      const real = DS.revealSample(st, failed.id, f.sample.id);
      return real && Object.keys(real).length === f.sample.fields.length
        && Object.keys(real).some((k) => real[k] !== '••••••');
    })());

  /* How long, not just how far behind */
  {
    const s2 = DS.seedDemo('clock');
    const fs2 = s2.streams.filter((x) => x.status === 'failed')[0];
    const d = DS.failureDetail(s2, fs2, Date.parse(fs2.failingSince) + 3720000);
    check('"failing for" is measured from when it started, not from the lag',
      d.failingForSeconds === 3720, d.failingForSeconds);
    check('and reads in the page\'s own duration idiom rather than raw seconds',
      DS.formatLag(d.failingForSeconds) === '1h 2m', DS.formatLag(d.failingForSeconds));
    check('while the consumer line uses the grain that suits days',
      DS.durationWords(8 * 86400) === '8 days' && DS.durationWords(45) === '45 seconds');
    check('a stream that has never delivered says so rather than showing a blank',
      DS.failureDetail(s2, { id: 'x', status: 'failed', metrics: { dlqDepth: 0, failedToday: 1 },
        capture: { position: {} } }, Date.now()).lastGoodDeliveryAt === null);
  }

  /* Downstream consumers */
  const paused = st.streams.filter((s) => s.status === 'paused')[0];
  const eightDays = Date.parse(paused.lastEventAt) + 8 * 86400000;
  const impact = DS.consumerImpact(paused, eightDays);
  check('a stalled stream with consumers says who it is affecting',
    !!impact && /Data science sandbox has had no new data for 8 days\./.test(impact.line),
    impact && impact.line);
  check('a running stream reports no impact, however many consumers it has',
    DS.consumerImpact(st.streams.filter((s) => s.status === 'running')[0], eightDays) === null);
  check('and a stalled stream nobody reads reports none either — "consumers may '
      + 'be affected" is not information',
    DS.consumerImpact({ status: 'paused', consumers: [], metrics: {} }, Date.now()) === null);
  check('two consumers are named as two, not as a count',
    (() => {
      const many = JSON.parse(JSON.stringify(paused));
      many.consumers = [{ name: 'Finance reporting' }, { name: 'Client portal' }];
      const im = DS.consumerImpact(many, eightDays);
      return /Finance reporting and Client portal have had no new data/.test(im.line);
    })());

  check('a stalled stream somebody is reading sorts above one nobody is',
    (() => {
      const a = { id: 'a', status: 'paused', consumers: [], metrics: { lagSeconds: 900 } };
      const b = { id: 'b', status: 'paused', consumers: [{ name: 'Finance reporting' }],
                  metrics: { lagSeconds: 1 }, lastEventAt: new Date(Date.now() - 8.64e8).toISOString() };
      return DS.sortStreams([a, b])[0].id === 'b';
    })(),
    'both are broken; only one is quietly feeding stale numbers to a person');
  check('but it never jumps ahead of an outright failure',
    (() => {
      const failedOne = { id: 'f', status: 'failed', consumers: [], metrics: { lagSeconds: 1 } };
      const pausedRead = { id: 'p', status: 'paused', consumers: [{ name: 'X' }],
                           metrics: { lagSeconds: 1 }, lastEventAt: new Date().toISOString() };
      return DS.sortStreams([pausedRead, failedOne])[0].id === 'f';
    })());

  /* Consumers survive a round trip through the editor */
  check('consumers are kept when a stream is edited',
    (() => {
      const s3 = DS.seedDemo('edit');
      const target = s3.streams[0];
      const draft = JSON.parse(JSON.stringify(target));
      delete draft.consumers;                       // an older draft shape
      DS.updateStream(s3, target.id, draft);
      return (DS.getStream(s3, target.id).consumers || []).length > 0;
    })(), 'editing a batch size must not silently forget who is downstream');
  check('and setConsumers drops the blank lines a textarea produces',
    (() => {
      const s4 = DS.seedDemo('set');
      const out = DS.setConsumers(s4, s4.streams[0].id,
        [{ name: ' Finance ' }, { name: '' }, { name: 'BI', owner: 'bi@x' }]);
      return out.length === 2 && out[0].name === 'Finance' && out[1].owner === 'bi@x';
    })());

  /* Reset to the last good checkpoint */
  check('resetting to the checkpoint clears the failure without losing the backlog',
    (() => {
      const s5 = DS.seedDemo('reset');
      const target = s5.streams.filter((x) => x.status === 'failed')[0];
      const pending = target.metrics.pendingInStore;
      const r = DS.resetToCheckpoint(s5, target.id);
      return r.pending === pending && target.lastError === null
        && target.failingSince === null && target.status === 'paused';
    })(), 'the Stream Store still holds everything captured since — that is the point of a checkpoint');
  check('and it is written to the audit trail like every other production change',
    (() => {
      const s6 = DS.seedDemo('audit');
      const target = s6.streams.filter((x) => x.status === 'failed')[0];
      DS.resetToCheckpoint(s6, target.id);
      return s6.audit.some((a) => a.action === 'stream.checkpoint_reset');
    })());

  /* The seeded clock, and the live one */
  check('a seeded demo rebases onto the real clock exactly once',
    (() => {
      const s7 = DS.seedDemo('rebase');
      const before = Date.parse(s7.streams[0].lastEventAt);
      DS.rebaseToNow(s7, Date.parse('2027-01-01T00:00:00Z'));
      const after = Date.parse(s7.streams[0].lastEventAt);
      const stamp = s7.rebasedAt;
      DS.rebaseToNow(s7, Date.parse('2028-01-01T00:00:00Z'));   // ignored
      return after > before && s7.rebasedAt === stamp
        && Date.parse(s7.streams[0].lastEventAt) === after;
    })(),
    'twice would double the shift and put the demo in the future');
  check('and every interval inside it survives the shift',
    (() => {
      const s8 = DS.seedDemo('intervals');
      const f8 = s8.streams.filter((x) => x.status === 'failed')[0];
      const gap = Date.parse(f8.lastEventAt) - Date.parse(f8.failingSince);
      DS.rebaseToNow(s8, Date.now() + 5 * 86400000);
      return Date.parse(f8.lastEventAt) - Date.parse(f8.failingSince) === gap;
    })(),
    'a stream seeded as failing for 22s must not read as failing for nine days');

  /* Old state still opens */
  check('a state written before any of this opens without throwing',
    (() => {
      const old = { version: 1, projectId: 'legacy', tickNo: 3,
        streams: [{ id: 's', status: 'paused', metrics: {}, capture: {}, destination: {} }],
        events: [], points: [], audit: [], seeded: true };
      try {
        if (typeof localStorage === 'undefined') {
          global.localStorage = {
            _d: {}, getItem(k) { return this._d[k] || null; },
            setItem(k, v) { this._d[k] = String(v); }, removeItem(k) { delete this._d[k]; },
          };
        }
        localStorage.setItem(DS.storeKey('legacy'), JSON.stringify(old));
        const loaded = DS.load('legacy');
        return Array.isArray(loaded.kpiHistory) && Array.isArray(loaded.streams[0].consumers);
      } catch (e) { return false; }
    })());
}

/* ── Global pause and resume ─────────────────────────────────────────────
   The stopping is the easy half. The half that has to be right is the
   starting again: a stream somebody deliberately paused a fortnight ago must
   not come back to life because a colleague pressed global resume. */
{
  /* Mixed state */
  {
    const st = DS.seedDemo('mixed');
    const g = DS.globalPauseState(st);
    check('the switch reports a mixed state rather than forcing a binary',
      g.mode === 'mixed' && /Partially paused — \d of \d running/.test(g.label), g.label);
    check('and the label counts only streams that could be running',
      g.total === st.streams.filter((s) => s.status !== 'draft').length,
      g.total + ' vs ' + st.streams.length + ' — a draft has never started, so it is not "not running"');
    check('all running reads as all running',
      (() => {
        const s2 = DS.seedDemo('allrun');
        s2.streams.forEach((x) => { if (x.status !== 'draft') x.status = 'running'; });
        return DS.globalPauseState(s2).label === 'All streams running';
      })());
    check('all paused reads as all paused',
      (() => {
        const s3 = DS.seedDemo('allpaused');
        s3.streams.forEach((x) => { if (x.status !== 'draft') x.status = 'paused'; });
        return DS.globalPauseState(s3).label === 'All streams paused';
      })());
  }

  /* Scope */
  {
    const st = DS.seedDemo('scope');
    const draftBefore = st.streams.filter((s) => s.status === 'draft').map((s) => s.status);
    const pausedBefore = st.streams.filter((s) => s.status === 'paused').map((s) => s.id);
    const r = DS.pauseAll(st, { reason: 'maintenance' });
    check('a global pause stops everything that was live, including failures',
      r.results.every((x) => x.ok) && r.paused >= 3, JSON.stringify(r.results.map((x) => x.name)));
    check('drafts are untouched',
      st.streams.filter((s) => s.status === 'draft').map((s) => s.status).join() === draftBefore.join());
    check('and a stream somebody had already paused is not in the snapshot',
      pausedBefore.every((id) => !st.globalPause.streams.some((x) => x.id === id)),
      'it was not ours to stop, so it is not ours to restart');

    const before = st.streams.map((s) => s.id + '=' + s.status).join('|');
    DS.pauseAll(st, {});
    check('pausing twice is idempotent — the snapshot does not grow',
      st.streams.map((s) => s.id + '=' + s.status).join('|') === before
      && st.globalPause.streams.length === r.snapshot.streams.length);

    const rr = DS.resumeAll(st, {});
    check('global resume restores only what the global pause stopped',
      pausedBefore.every((id) => DS.getStream(st, id).status === 'paused'),
      'a stream paused two weeks ago must not come back to life');
    check('and reports what it deliberately left alone',
      rr.stayPaused.length === pausedBefore.length, JSON.stringify(rr.stayPaused));
    check('drafts are untouched by resume too',
      st.streams.filter((s) => s.status === 'draft').map((s) => s.status).join() === draftBefore.join());
  }

  /* The failed opt-in */
  {
    const st = DS.seedDemo('failed');
    DS.pauseAll(st, {});
    const pv = DS.resumeAllPreview(st);
    check('a stream that was failing with a dead-letter queue is listed separately',
      pv.willFailAgain.length === 1 && pv.willFailAgain[0].dlqDepth > 0,
      JSON.stringify(pv.willFailAgain.map((x) => x.name)));
    check('with the error message, so the dialog can say why it would fail again',
      /truncation/.test(pv.willFailAgain[0].error), pv.willFailAgain[0].error);

    const r1 = DS.resumeAll(st, {});                       // opt-in NOT given
    check('and it is not resumed unless somebody opts in',
      DS.getStream(st, pv.willFailAgain[0].id).status === 'paused'
      && r1.skippedFailed.length === 1);
    check('the snapshot keeps holding it, so its prior state is not lost',
      !!st.globalPause && st.globalPause.streams.length === 1,
      'clearing the snapshot on a partial resume would strand the rest');

    const r2 = DS.resumeAll(st, { includeFailed: true });
    check('opting in resumes it',
      r2.resumed === 1 && DS.getStream(st, pv.willFailAgain[0].id).status !== 'paused');
    check('and now the snapshot is finished with',
      st.globalPause === null);
  }

  /* Partial failure */
  {
    const st = DS.seedDemo('partial');
    const targets = DS.pauseAllPlan(st).map((t) => t.id);
    const doomed = targets[1];
    const r = DS.pauseAll(st, { apply: (state, id) => {
      if (id === doomed) throw new Error('the change could not be saved');
      DS.pauseStream(state, id);
    } });
    check('a partial failure is a real outcome, reported per stream',
      r.ok === false && r.failed.length === 1 && r.failed[0].id === doomed,
      JSON.stringify(r.results));
    check('the streams that did stop are still recorded',
      st.globalPause.streams.length === targets.length - 1);
    check('and the one that did not is NOT in the snapshot',
      !st.globalPause.streams.some((x) => x.id === doomed),
      'a snapshot claiming to have stopped something it did not is worse than none');
    check('the switch lands in a mixed state, not a success',
      DS.globalPauseState(st).mode === 'mixed', DS.globalPauseState(st).label);
  }

  /* The deadline the dialog is for */
  {
    const st = DS.seedDemo('deadline');
    DS.rebaseToNow(st);
    const pv = DS.pauseAllPreview(st, st.clockNow);
    check('every affected stream is costed from its OWN retention window',
      pv.streams.every((x) => x.deadline.retentionHours === (DS.getStream(st, x.id).profile.retentionHours)),
      pv.streams.map((x) => x.deadline.retentionHours).join(','));
    check('and the worst case leads, because it is the one that sets the clock',
      /^Earliest data loss: ~\d+[mhd] \(.+, \d+h retention\)$/.test(pv.headline), pv.headline);
    check('the soonest really is the soonest',
      pv.streams.every((x) => x.deadline.secondsRemaining >= pv.soonest.deadline.secondsRemaining));
    check('a stream that is already past its window says so instead of a countdown',
      (() => {
        const s2 = DS.seedDemo('expired');
        const one = s2.streams[0];
        one.metrics.pendingInStore = 5000;
        one.oldestPendingAt = new Date(Date.parse(s2.clockNow ? new Date(s2.clockNow).toISOString() : new Date().toISOString()) - 100 * 3600000).toISOString();
        const d = DS.retentionDeadline(one, s2.clockNow);
        return d.expired && /full resync/.test(d.line);
      })());
    check('an empty store is not given a backlog it does not have',
      (() => {
        const s3 = DS.seedDemo('empty');
        const one = s3.streams[0];
        one.metrics.pendingInStore = 0; one.oldestPendingAt = null;
        const d = DS.retentionDeadline(one, s3.clockNow);
        return /nothing queued yet/.test(d.line) && !d.expired;
      })());
    check('the deadline goes amber under 12h and red under 4h',
      (() => {
        const mk = (hoursLeft) => {
          const s4 = DS.seedDemo('band' + hoursLeft);
          const one = s4.streams[0];
          one.profile.retentionHours = 24;
          one.metrics.pendingInStore = 10;
          one.oldestPendingAt = new Date(s4.clockNow - (24 - hoursLeft) * 3600000).toISOString();
          return DS.retentionDeadline(one, s4.clockNow).band;
        };
        return mk(20) === 'ok' && mk(8) === 'amber' && mk(2) === 'red';
      })());
  }

  /* Persistence — the requirement is that this survives a reload */
  {
    if (typeof localStorage === 'undefined') {
      global.localStorage = { _d: {}, getItem(k) { return this._d[k] || null; },
        setItem(k, v) { this._d[k] = String(v); }, removeItem(k) { delete this._d[k]; } };
    }
    const st = DS.seedDemo('persist');
    st.projectId = 'persist';
    DS.pauseAll(st, { reason: 'cutover window' });
    DS.save(st);
    const reloaded = DS.load('persist');          // a different session, same store
    check('the prior-state snapshot survives a reload',
      !!reloaded.globalPause && reloaded.globalPause.streams.length === st.globalPause.streams.length,
      'component state would not survive, and this has to');
    check('with the prior status of each stream, and the reason',
      reloaded.globalPause.streams.every((x) => !!x.priorStatus)
      && reloaded.globalPause.reason === 'cutover window');
    const priorById = {};
    reloaded.globalPause.streams.forEach((x) => { priorById[x.id] = x.priorStatus; });
    const rr = DS.resumeAll(reloaded, { includeFailed: true });
    check('and a different operator resuming restores exactly what was running',
      rr.results.length > 0 && rr.results.every((r) => r.ok)
      && rr.results.every((r) => priorById[r.id] && DS.getStream(reloaded, r.id).status !== 'paused'),
      JSON.stringify(rr.results.map((r) => r.name + '=' + DS.getStream(reloaded, r.id).status)));
  }

  /* Audit */
  {
    const st = DS.seedDemo('audit2');
    DS.pauseAll(st, { reason: 'incident 4412' });
    const entries = st.audit.filter((a) => a.action === 'streams.pause_all');
    check('one audit entry per global action, not one per stream',
      entries.length === 1, entries.length);
    check('carrying the actor, the count, the reason and the prior state',
      !!entries[0].actor && entries[0].detail.reason === 'incident 4412'
      && Array.isArray(entries[0].detail.prior) && entries[0].detail.prior.length > 0,
      JSON.stringify(entries[0].detail));
    DS.resumeAll(st, {});
    check('and one for the resume, saying what it held back',
      st.audit.filter((a) => a.action === 'streams.resume_all').length === 1
      && st.audit.filter((a) => a.action === 'streams.resume_all')[0].detail.skippedFailed >= 0);
  }
}

/* ── Wiring ──────────────────────────────────────────────────────────────── */
{
  const page = fs.readFileSync(path.join(__dirname, '..', 'public', 'data_stream.html'), 'utf8');
  const css  = fs.readFileSync(path.join(__dirname, '..', 'public', 'cygenix-datastream.css'), 'utf8');

  check('every KPI tile draws its own series',
    (page.match(/series: DS\.kpiSeries\(s, '/g) || []).length === 4);
  check('the sparkline is inline SVG — no charting dependency was added',
    /<polyline points/.test(page) && /vector-effect="non-scaling-stroke"/.test(page)
    && !/\b(chart\.js|d3\.min\.js|recharts|apexcharts|highcharts)\b/i.test(page));
  check('the svg is hidden from screen readers and the sentence carries the meaning',
    /aria-hidden="true" focusable="false"/.test(page) && /aria-label="' \+ U\.esc\(t\.sentence/.test(page));
  check('the hex checkpoint is demoted to a chip rather than shown as a headline',
    /class="ds-checkpoint"/.test(page) && /ds-checkpoint \{/.test(css));
  check('the dead-letter sample is rendered from the redacted copy',
    /sample\.redacted\[k2\]/.test(page) && !/sample\.after\[/.test(page));
  check('revealing values is a separate, deliberate click',
    /function dsRevealSample/.test(page) && /Reveal values/.test(page));
  check('the three failure actions go through the existing guardrail, not a bespoke confirm',
    /U\.confirmText\('checkpoint'/.test(page) && /dsRequeue/.test(page) && /dsPause/.test(page));
  check('consumers are rendered on the row and in the flow card',
    (page.match(/consumersLine\(r\)/g) || []).length >= 2);
  /* The global switch, on the page */
  check('the switch is a labelled control with a real mixed state',
    /role="switch"/.test(page) && /aria-checked="' \+ \(mixed \? 'mixed'/.test(page)
    && /ds-gsw mixed|\.ds-gsw\.mixed/.test(page + css));
  check('its accessible name carries the count, not just on or off',
    /aria-label="' \+ U\.esc\(g\.label/.test(page));
  check('the pause dialog leads with the worst case and costs every stream',
    /ds-gx-headline/.test(page) && /deadline\.line/.test(page)
    && /pending<\/div>/.test(page));
  check('focus lands on Cancel, never on the destructive action',
    /getElementById\('ds-pauseall-cancel'\)\.focus\(\)/.test(page));
  check('a stream set to "confirm every change" forces the typed word',
    /guardrail === 'confirm_all'/.test(page) && /Type <b>PAUSE<\/b>/.test(page)
    && /toUpperCase\(\) !== 'PAUSE'/.test(page),
    'a global action takes the strictest guardrail in scope');
  check('the transition is applied one stream at a time, with a real frame between',
    /await dsFrame\(\)/.test(page) && /gxBusy\.done = results\.length/.test(page));
  check('and a row waiting its turn shows that rather than a status about to change',
    /function statusOrPending/.test(page) && /ds-pendingpill/.test(page),
    'optimistic UI here means telling an operator replication has stopped when it has not');
  check('a write that does not land counts as a failure',
    /if \(!P\.persist\(\)\) throw/.test(page)
    && /return state \? DS\.save\(state\) : false/.test(
        fs.readFileSync(path.join(__dirname, '..', 'public', 'cygenix-datastream-page.js'), 'utf8')));
  check('a partial result is never announced as a success',
    /A partial result is not a success/.test(page) && /Retry failed/.test(page));
  check('and Home mirrors the same state rather than keeping a second copy',
    (() => {
      const app = fs.readFileSync(path.join(__dirname, '..', 'public', 'dashboard-app.js'), 'utf8');
      return /CygenixDataStream\.globalPauseState\(st\)/.test(app)
        && /renderStreamControlTile/.test(app);
    })());
  check('Home hands the confirmation to the page that owns it',
    (() => {
      const app = fs.readFileSync(path.join(__dirname, '..', 'public', 'dashboard-app.js'), 'utf8');
      return /data-stream\?global=/.test(app) && !/Earliest data loss/.test(app);
    })(),
    'two copies of the same warning is two things to keep in step');

  check('and the impact line is amber with words, not a colour alone',
    /ds-impact/.test(page) && /ds-impact \{[^}]*var\(--amber\)/.test(css));
}

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
