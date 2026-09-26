// tests/data-stream-profiles.test.js — a stream belongs to a profile.
//
// Phase 1 of "make the Data Stream profile-aware". Three decisions were made
// before any code and are pinned here as decisions, not rediscovered:
//
//   1. one stream belongs to exactly one profile and reads ONE side of it;
//   2. existing streams are Unassigned — nothing guesses a profile for them;
//   3. the screens show the active profile's streams by default.
//
// And two design calls from the discovery report, agreed before building:
//
//   * readsFromSide is the ACCESSOR over capture.side, not a second stored
//     field — a copy that must always equal the original is a drift waiting
//     to happen;
//   * there is no stream backend, so the rules the brief put "server-side"
//     live where the runner actually is: the engine's tick. A needs-attention
//     stream genuinely does not tick, and a start is genuinely refused, in
//     the one Node-testable file that will still be the rulebook when a real
//     backend arrives.
//
// The whole point of the link is that the connection is LOOKED UP, never
// copied: change the profile's source connection and every stream reading
// that side follows with no edit to the stream. That is the central check.
'use strict';

const fs = require('fs');
const path = require('path');
const DS = require('../public/cygenix-datastream.js');
const U  = require('../public/cygenix-datastream-ui.js');
const JP = require('../public/cygenix-job-profile.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

console.log('Data Stream — profile ownership\n');

const NOW = Date.parse('2026-09-16T09:00:00Z');
const world = () => DS.seedDemo('proj_prof', { now: NOW });
const store = () => ({
  v: 1,
  profiles: [
    { id: 'FIN_3E_UAT', name: 'Finance UAT', envClass: 'UAT', status: 'active', srcConnId: 'c_src', tgtConnId: 'c_tgt' },
    { id: 'FIN_3E_PRD', name: 'Finance production', envClass: 'PRD', status: 'active', srcConnId: 'c_psrc', tgtConnId: 'c_ptgt' },
    { id: 'OLD', name: 'Retired', envClass: 'TEST', status: 'retired', srcConnId: 'c_src', tgtConnId: 'c_tgt' },
  ],
  bindings: [], settings: { activeProfileId: 'FIN_3E_UAT' },
});
const conns = () => [
  { id: 'c_src',  name: 'LEGACY-SQL01', side: 'src', connString: 'x' },
  { id: 'c_tgt',  name: 'AZSQL-TARGET', side: 'tgt', connString: 'y' },
  { id: 'c_psrc', name: 'PROD-SQL01',   side: 'src', connString: 'p' },
  { id: 'c_ptgt', name: 'PROD-AZSQL',   side: 'tgt', connString: 'q' },
];
const P = (st, id) => st.profiles.filter(p => p.id === id)[0];

/* ── 1. The demo world starts Unassigned ────────────────────────────────── */

{
  const w = world();
  check('all six demo streams are Unassigned — nothing guessed a profile for them',
    DS.unassignedCount(w.streams) === 6 && w.streams.every(DS.isUnassigned));
  check('the old connection fields are still on every record, untouched',
    w.streams.every(s => s.capture.connectionId && s.capture.connectionLabel));
  check('readsFromSide reads capture.side rather than a second field',
    w.streams.every(s => DS.readsFromSide(s) === s.capture.side)
    && w.streams.every(s => !('readsFromSide' in s)));
  check('an unassigned stream resolves to its own saved label, marked as such',
    (() => { const r = DS.resolveCapture(w.streams[0], store(), conns());
      return r.ok === false && r.unassigned === true && r.label === w.streams[0].capture.connectionLabel; })());
}

/* ── 2. Unassigned cannot run ───────────────────────────────────────────── */

{
  const w = world();
  const draft = w.streams.filter(s => s.status === 'draft')[0];
  const paused = w.streams.filter(s => s.status === 'paused')[0];
  check('starting an unassigned stream is refused, and the reason says what to do',
    (() => { try { DS.startStream(w, draft.id); return false; }
      catch (e) { return /not assigned to a connection profile/i.test(e.message) && /row menu/.test(e.message); } })());
  check('resuming one is refused the same way',
    (() => { try { DS.resumeStream(w, paused.id); return false; }
      catch (e) { return /not assigned/i.test(e.message); } })());
  check('and the stream is left exactly as it was',
    draft.status === 'draft' && !draft.startedAt && paused.status === 'paused');
  check('cannotRunReason is the same sentence the engine throws, so the UI and the guard agree',
    (() => { const why = DS.cannotRunReason(draft);
      try { DS.startStream(w, draft.id); return false; } catch (e) { return e.message.indexOf(why) !== -1; } })());
  // The refusal that needs no store must not depend on one being passed.
  check('the unassigned rule needs no profile store to apply', DS.canRun(draft).ok === false);
}

/* ── 3. Assigning ───────────────────────────────────────────────────────── */

{
  const w = world(), st = store(), cs = conns();
  const s = w.streams[0];                         // 'Source → Reporting warehouse', saved as LEGACY-SQL01 · dbo
  const before = w.audit.length;
  const r = DS.assignProfile(w, s.id, P(st, 'FIN_3E_UAT'), 'source', { savedConns: cs });
  check('assigning sets the same two fields a job carries — profileId and profileName',
    s.profileId === 'FIN_3E_UAT' && s.profileName === 'Finance UAT');
  check('and the side, which is what readsFromSide now reports',
    s.capture.side === 'source' && DS.readsFromSide(s) === 'source');
  check('the connection now RESOLVES from the profile rather than the stream',
    (() => { const c = DS.resolveCapture(s, st, cs); return c.ok && c.label === 'LEGACY-SQL01' && c.connId === 'c_src'; })());
  // Requirement 1c: warn on a mismatch, still allow.
  check('a server that does not match the profile\'s side is WARNED, not refused',
    !!r.mismatch && /LEGACY-SQL01 · dbo/.test(r.mismatch) && /LEGACY-SQL01"/.test(r.mismatch)
    && s.profileId === 'FIN_3E_UAT', r.mismatch);
  check('the assignment is audited, with the mismatch on the record',
    w.audit.length === before + 1 && w.audit[0].action === 'stream.assigned'
    && w.audit[0].detail.profileId === 'FIN_3E_UAT' && w.audit[0].detail.side === 'source'
    && !!w.audit[0].detail.mismatch);
  check('assigning it is a lifecycle event, so it reaches the organisation trail',
    /'stream\.assigned':\s+'Assigned a profile to'/.test(read('public', 'cygenix-datastream.js')));
  check('and the trail\'s allowlist accepts it',
    /'stream\.assigned':\s+'stream'/.test(read('netlify', 'functions', 'lib', 'audit-schema.js'))
    && /'stream\.needs_attention':\s+'stream'/.test(read('netlify', 'functions', 'lib', 'audit-schema.js')));
  check('a bad side or a missing profile is refused',
    (() => { try { DS.assignProfile(w, s.id, P(st, 'FIN_3E_UAT'), 'sideways'); return false; } catch (e) { return true; } })()
    && (() => { try { DS.assignProfile(w, s.id, null, 'source'); return false; } catch (e) { return true; } })());
  check('once assigned, the stream can start',
    (() => { const d = w.streams.filter(x => x.status === 'draft')[0];
      DS.assignProfile(w, d.id, P(st, 'FIN_3E_UAT'), 'source', { savedConns: cs });
      DS.startStream(w, d.id, { profileStore: st, savedConns: cs });
      return DS.isLive(d.status); })());
}

/* ── 4. Look-up, not copy — the central check ───────────────────────────── */

{
  const w = world(), st = store(), cs = conns();
  const s = w.streams[0];
  DS.assignProfile(w, s.id, P(st, 'FIN_3E_UAT'), 'source', { savedConns: cs });
  const labelBefore = DS.resolveCapture(s, st, cs).label;
  // Change the PROFILE's source connection. The stream is not touched.
  P(st, 'FIN_3E_UAT').srcConnId = 'c_psrc';
  const labelAfter = DS.resolveCapture(s, st, cs).label;
  check('changing the profile\'s source connection changes what the stream reads, with no edit to the stream',
    labelBefore === 'LEGACY-SQL01' && labelAfter === 'PROD-SQL01', labelBefore + ' → ' + labelAfter);
  check('the stream record carries no connection string of its own',
    !JSON.stringify(s).match(/connString|fnKey|password/i));
  check('a Project-target destination is the profile\'s OTHER side, resolved the same way',
    (() => { const cut = w.streams.filter(x => x.destination.kind === 'cygenix-target')[0];
      DS.assignProfile(w, cut.id, P(st, 'FIN_3E_UAT'), 'source', { savedConns: cs });
      const d = DS.resolveDestination(cut, st, cs);
      return d.ok && d.side === 'target' && d.label === 'AZSQL-TARGET'; })());
  check('every other destination kind keeps its inline settings in this phase',
    (() => { const d = DS.resolveDestination(w.streams[1], st, cs); return d.ok && d.inline && d.label === w.streams[1].destination.label; })());
}

/* ── 5. Needs attention ─────────────────────────────────────────────────── */

{
  const w = world(), st = store(), cs = conns();
  const s = w.streams[0];
  DS.assignProfile(w, s.id, P(st, 'FIN_3E_UAT'), 'source', { savedConns: cs });
  DS.startStream(w, s.id, { profileStore: st, savedConns: cs });
  const runningBefore = DS.isLive(s.status);

  // Break the connection the profile points at.
  const broken = cs.filter(c => c.id !== 'c_src');
  const changed = DS.attentionCheck(w, st, broken);
  check('a stream whose profile connection has gone moves to needs-attention, with the reason on it',
    runningBefore && s.status === 'needs-attention' && changed.length === 1
    // The demo stream snapshots first, so the status it was parked FROM is
    // 'snapshotting' — what matters is that it was live, and that is recorded.
    && /no longer exists/.test(s.attention.reason) && DS.isLive(s.attention.priorStatus),
    JSON.stringify(s.attention));
  check('its objects are paused and its capture rate is zero — nothing pretends to run',
    s.objects.every(o => o.state === 'paused') && s.metrics.eventsPerMin === 0);
  check('it is not live, so the tick leaves it alone',
    !DS.isLive('needs-attention') && (() => { const p = s.metrics.pendingInStore; DS.tick(w, { at: NOW + 2000 }); return s.metrics.pendingInStore === p; })());
  check('it cannot be resumed while it needs attention',
    (() => { try { DS.resumeStream(w, s.id, { profileStore: st, savedConns: broken }); return false; }
      catch (e) { return /no longer exists/.test(e.message); } })());
  check('the transition is audited under its own action', w.audit[0].action === 'stream.needs_attention'
    || w.audit.some(a => a.action === 'stream.needs_attention'));
  check('an UNASSIGNED stream is never put into needs-attention — unassigned is its own state',
    (() => { const u = w.streams[1]; DS.attentionCheck(w, st, []); return u.status !== 'needs-attention'; })());
  check('a retired profile counts as broken, and says which profile to move to',
    (() => { const w2 = world(); DS.assignProfile(w2, w2.streams[0].id, P(st, 'OLD'), 'source');
      const r = DS.resolveCapture(w2.streams[0], st, cs); return !r.ok && /retired/.test(r.reason) && /superseded/.test(r.reason); })());
  check('a deleted profile counts as broken too, and keeps the name the stream remembers',
    (() => { const w2 = world(); DS.assignProfile(w2, w2.streams[0].id, { id: 'GONE', name: 'Gone UAT' }, 'source');
      const r = DS.resolveCapture(w2.streams[0], st, cs); return !r.ok && /GONE/.test(r.reason) && /Gone UAT/.test(r.reason); })());

  // Fix the connection: it comes back PAUSED, not running.
  const back = DS.attentionCheck(w, st, cs);
  check('when the profile resolves again the stream comes back PAUSED — a person restarts it',
    back.length === 1 && s.status === 'paused' && s.attention === null, s.status);
  check('and that is audited as cleared', w.audit[0].action === 'stream.attention_cleared');
  check('running the check again changes nothing — it is idempotent',
    DS.attentionCheck(w, st, cs).length === 0);
  check('needs-attention has a label and a colour of its own',
    DS.statusLabel('needs-attention') === 'Needs attention'
    && /\.ds-st-needs-attention/.test(read('public', 'cygenix-datastream.css'))
    && /Needs attention/.test(U.statusPill({ status: 'needs-attention', metrics: {}, objects: [] })));
  check('and sorts between failed and lagging', (() => {
    const a = { id: 'a', status: 'lagging', metrics: { lagSeconds: 1 } };
    const b = { id: 'b', status: 'needs-attention', metrics: { lagSeconds: 0 } };
    const c = { id: 'c', status: 'failed', metrics: { lagSeconds: 0 } };
    return DS.sortStreams([a, b, c]).map(x => x.id).join() === 'c,b,a'; })());
}

/* ── 6. Scoping and the KPIs ────────────────────────────────────────────── */

{
  const w = world(), st = store(), cs = conns();
  DS.assignProfile(w, w.streams[0].id, P(st, 'FIN_3E_UAT'), 'source', { savedConns: cs });
  DS.assignProfile(w, w.streams[1].id, P(st, 'FIN_3E_UAT'), 'source', { savedConns: cs });
  DS.assignProfile(w, w.streams[2].id, P(st, 'FIN_3E_PRD'), 'target', { savedConns: cs });
  const mine = DS.scopeStreams(w.streams, { mode: 'this', profileId: 'FIN_3E_UAT' });
  check('"This profile" is exactly the active profile\'s streams', mine.length === 2 && mine.every(s => s.profileId === 'FIN_3E_UAT'));
  check('"All profiles" is everything', DS.scopeStreams(w.streams, { mode: 'all' }).length === 6);
  check('this-profile with no active profile is nothing, not everything',
    DS.scopeStreams(w.streams, { mode: 'this', profileId: null }).length === 0);
  const k = DS.kpis(w, mine);
  check('the KPI tiles count only the streams in view', k.total === 2 && k.running <= 2
    && DS.kpis(w).total === 6);
  check('the Store totals and topics take the same list',
    DS.topicsOf(w, mine).every(t => mine.some(s => s.id === t.streamId))
    && DS.storeTotals(w, mine).topics === DS.topicsOf(w, mine).length);
  check('the alerts do too', DS.alertsOf(w, NOW, mine).every(a => a.streams.every(x => mine.some(s => s.id === x.id))));
  check('the Unassigned filter and the profile filter both work through filterStreams',
    DS.filterStreams(w.streams, { unassigned: true }).length === 3
    && DS.filterStreams(w.streams, { profileId: 'FIN_3E_PRD' }).length === 1);
  check('the side filter is readsFromSide',
    DS.filterStreams(w.streams, { side: 'target' }).every(s => DS.readsFromSide(s) === 'target'));
  check('text search finds a stream by its profile', DS.filterStreams(w.streams, { text: 'fin_3e_prd' }).length === 1);
}

/* ── 7. New streams ─────────────────────────────────────────────────────── */

{
  const w = world(), st = store(), cs = conns();
  const d = DS.blankDraft('proj_prof');
  d.name = 'New one';
  d.objects = [{ table: 'dbo.x', keys: ['id'], ops: ['I'] }];
  d.destination.label = 'somewhere';
  // Stamped the same way a job is, by the same helper.
  JP.stamp(d, st);
  check('a new stream is stamped with the active profile by the shared job helper',
    d.profileId === 'FIN_3E_UAT' && d.profileName === 'Finance UAT');
  d.capture.connectionId = null; d.capture.connectionLabel = '';
  const v = DS.validateDesign(d);
  check('a profile-linked draft needs no connection of its own to validate',
    !v.errors[0].some(e => /Pick the connection/.test(e)), JSON.stringify(v.errors[0]));
  check('but an unassigned draft still does',
    (() => { const u = DS.blankDraft('p'); u.capture.connectionId = null;
      return DS.validateDesign(u).errors[0].some(e => /Pick the connection/.test(e)); })());
  const created = DS.createStream(w, Object.assign(d, { name: 'New one' }), { start: false });
  check('the created stream carries profileId and readsFromSide, and no connection of its own',
    created.profileId === 'FIN_3E_UAT' && DS.readsFromSide(created) === 'source'
    && !created.capture.connectionId && !JSON.stringify(created).match(/connString/));
  check('and it can start straight away', (() => { DS.startStream(w, created.id, { profileStore: st, savedConns: cs }); return DS.isLive(created.status); })());
}

/* ── 8. Wiring ──────────────────────────────────────────────────────────── */

// Each screen's code is in its own deferred file now; the page holds the
// markup and the tag. Reading both keeps every pin below meaning what it did.
const withApp = (page) => read('public', page + '.html')
  + read('public', page.replace(/_/g, '-') + '-app.js');
const PAGE = withApp('data_stream');
const GLUE = read('public', 'cygenix-datastream-page.js');
const DESIGN = withApp('data_stream_designer');
const pages = ['data_stream', 'data_stream_store', 'data_stream_events', 'data_stream_monitor', 'data_stream_designer'];

check('every stream page loads the shared profile helper and the connections module',
  pages.every(p => /cygenix-job-profile\.js\?v=[a-f0-9]{10}/.test(read('public', p + '.html'))
    && /connections\.js\?v=[a-f0-9]{10}/.test(read('public', p + '.html'))));
check('the page glue scopes through the engine and listens for the profile changing',
  /DS\.scopeStreams\(s\.streams, \{ mode: scope\(\), profileId: p \? p\.id : null \}\)/.test(GLUE)
  && /addEventListener\('cygenix:profile-status', onProfileEvent\)/.test(GLUE)
  && /addEventListener\('cygenix:profiles-changed', onProfileEvent\)/.test(GLUE)
  && /e\.key === 'cygenix_profiles_v1'/.test(GLUE));
// The render-loop rule: the change detector compares ids; nothing arms a flag
// that its own callback resets.
check('a profile switch resets the toggle to This profile, by comparing ids rather than arming a flag',
  /var switched = lastProfileId !== undefined && id !== lastProfileId;/.test(GLUE)
  && /if \(switched\) setScope\('this'\);/.test(GLUE)
  && !/oneShot|_armed|pending\s*=\s*true/.test(GLUE));
check('the attention check runs before every tick, so a broken stream never gets one more',
  /checkAttention\(\);\s*\n\s*DS\.tick\(state\);/.test(GLUE));
check('the scope choice is remembered per user, in a classified key',
  /SCOPE_KEY = 'cygenix_datastream_scope'/.test(GLUE)
  && /cygenix_datastream_scope\*/.test(read('scripts', 'storage-inventory.js')));
check('the Streams page paints the view, not the world: rows, KPIs and the slim banner all take inView',
  /const inView = P\.visible\(s\);\s*\n\s*paintSlim\(s, inView\);\s*\n\s*paintKpis\(s, inView\);/.test(PAGE)
  && /DS\.filterStreams\(inView, filter\)/.test(PAGE));
check('the KPI tiles say which view they count',
  /paused · ' \+ k\.failed \+ ' failed · ' \+ words/.test(PAGE));
check('the This profile / All profiles toggle is in the filter bar, with Unassigned only in All view',
  /U\.scopeToggle\(P\.scope\(\), P\.activeProfile\(\)\)/.test(PAGE)
  && /if \(P\.scope\(\) === 'all'\) \{[\s\S]{0,300}Unassigned/.test(PAGE));
check('the Profile column exists only in All view, and is part of the table\'s identity so it re-renders',
  /\(profCol \? '<th>Profile<\/th>' : ''\)/.test(PAGE)
  && /const key = P\.scope\(\) \+ '\|' \+ rows\.map/.test(PAGE));
check('an unassigned row is badged, and its Reads-from shows the saved label marked as such',
  /U\.unassignedBadge\(\)/.test(PAGE) && /saved on stream/.test(PAGE));
check('Start and Resume are disabled with the engine\'s own reason as the tooltip',
  /const why = DS\.cannotRunReason\(s, P\.profileStore\(\), P\.savedConns\(\)\);/.test(PAGE)
  && /disabled: !!why, title: why \|\| ''/.test(PAGE)
  && /if \(it\.title\) b\.title = it\.title;/.test(GLUE));
check('Assign to profile is on the row menu and opens a dialog with profile and side',
  /Assign to profile…/.test(PAGE) && /id="ds-assign"/.test(PAGE)
  && /name="ds-as-side"/.test(PAGE) && /DS\.assignProfile\(P\.state, assignId, c\.profile, c\.side/.test(PAGE));
check('the dialog warns on a mismatch and still allows it',
  /You can still assign it/.test(PAGE));
check('the slim banner links to All profiles filtered to Unassigned',
  /function dsViewUnassigned\(\) \{\s*\n\s*P\.setScope\('all'\);\s*\n\s*filter\.unassigned = true;/.test(PAGE));
check('New stream is disabled without an active profile, with the reason on it',
  /btn\.title = p \? 'Create a stream under ' \+ p\.id : 'Select a profile to create a stream\.'/.test(PAGE)
  && /Select a profile to create a stream\./.test(DESIGN));
check('the engine is handed the store when a stream is started from the page',
  // Phase 2 builds the same two into an opts object (the production guard
  // adds confirmedProfileId to it), so the shape is checked, not the literal.
  /const opts = \{ profileStore: store, savedConns: conns \};[\s\S]{0,600}DS\.startStream\(P\.state, id, opts\)/.test(PAGE));
check('the Designer stamps a NEW draft with the shared helper and drops the connection fields',
  /CygenixJobProfile\.stamp\(draft\)/.test(DESIGN)
  && /if \(draft\.profileId\) \{ draft\.capture\.connectionId = null; draft\.capture\.connectionLabel = ''; \}/.test(DESIGN));
check('the Designer shows the profile read-only and the connection as what the side resolves to',
  /Profile: ' \+ U\.profileLabel\(prof\)/.test(DESIGN)
  && /Reads from <b>' \+ U\.esc\(res\.label\)/.test(DESIGN)
  && /the stream stores no connection of its own/.test(DESIGN));
check('the Designer lists tables from the schema cached for the profile\'s side',
  /CygenixSchemaGraph\.cacheKeyFor\(value\)/.test(DESIGN)
  && /cacheKeyFor: \(connValue\) => cacheKey\(connValue\)/.test(read('public', 'cygenix-schema-graph.js')));
for (const p of ['data_stream_store', 'data_stream_events', 'data_stream_monitor']) {
  const src = withApp(p);
  check(p + ' carries the toggle and paints through P.visible',
    /U\.scopeToggle\(P\.scope\(\), P\.activeProfile\(\)\)/.test(src) && /P\.visible(Ids)?\(/.test(src)
    && /function dsScope\(mode\)/.test(src), p);
}
check('no page dispatches the events it listens for — no loop',
  !/dispatchEvent\(new CustomEvent\('cygenix:profile/.test(GLUE + PAGE + DESIGN));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
