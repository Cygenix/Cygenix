// tests/audit-state.test.js — capture state, the always-on lock, and the
// storage rules that hang off them.
//
// The acceptance criteria this evidences, in the brief's own words:
//
//   "While Paused, a settings change is not recorded, but a role change, a
//    PROD sql.write and a sign-in are."
//   "A pause ends on time with no one touching it; audit.resume is written
//    by system."
//   "Locked categories can't be turned off through the UI or the API."
//
// The second one is why the clock is a parameter everywhere in
// audit-state.js: a pause that expires after four hours is a test that runs
// in a millisecond, not a test nobody runs.
//
// The blob store is faked in-process rather than mocked — a Map with the
// same get/setJSON/delete surface — because the interesting behaviour is
// the ORDER of the reads and writes (build, decide, chain, entry, re-read
// head, advance head, index), and a mock that asserts calls would test the
// implementation instead of the outcome.

'use strict';

const S = require('../netlify/functions/lib/audit-state');
const schema = require('../netlify/functions/lib/audit-schema');
const org = require('../netlify/functions/lib/org-store');
const rbac = require('../netlify/functions/lib/rbac');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Audit capture state — pause, off, and what survives them\n');

const HOUR = 3600 * 1000;
const T0 = Date.parse('2026-09-11T12:00:00.000Z');

// ── Settings and the always-on lock ───────────────────────────────────────
console.log('Settings and the always-on lock');

const def = S.normaliseSettings(null);
check('every category defaults to on', schema.CATEGORY_KEYS.every(k => def.categories[k] === true));
check('retention defaults to one year', def.retentionDays === 365);
check('the pause ceiling defaults to four hours', def.pauseMaxMinutes === 240);

// Stored state that claims security is off must not be able to switch
// security auditing off just by existing. The lock is re-applied on read.
const tampered = S.normaliseSettings({ categories: { security: false, access: false, settings: false } });
check('a stored setting that disables security is forced back on',
  tampered.categories.security === true);
check('and one that disables access is forced back on',
  tampered.categories.access === true);
check('while a genuinely optional category stays off',
  tampered.categories.settings === false);

check('the API refuses to disable a locked category',
  S.validateSettings({ categories: { prod: false } }, null).ok === false);
check('and names which one',
  /prod/.test(S.validateSettings({ categories: { prod: false } }, null).reason));
check('the API accepts disabling an optional category',
  S.validateSettings({ categories: { mapping: false } }, null).ok === true);
check('an unknown category is refused rather than stored',
  S.validateSettings({ categories: { wibble: true } }, null).ok === false);
check('a retention value outside the three offered is refused',
  S.validateSettings({ retentionDays: 4 }, null).ok === false);
check('90 days, 1 year and 7 years are all accepted',
  [90, 365, 2555].every(d => S.validateSettings({ retentionDays: d }, null).ok));
check('a pause ceiling outside 4h/24h is refused',
  S.validateSettings({ pauseMaxMinutes: 99999 }, null).ok === false);
check('a settings change reports its own diff, so the audit entry has one',
  S.validateSettings({ retentionDays: 90 }, null).changes.some(c => c.field === 'retentionDays'));

// ── Resolving the stored state against the clock ──────────────────────────
console.log('\nResolving state against the clock');

const paused = { state: 'paused', reason: 'bulk import', changedBy: 'a@b.c',
                 pausedUntil: new Date(T0 + 2 * HOUR).toISOString() };

check('a live pause resolves as paused', S.resolveState(paused, T0).state === 'paused');
check('and reports how long is left',
  S.resolveState(paused, T0).msRemaining === 2 * HOUR);
check('a pause resolves as recording the instant it expires',
  S.resolveState(paused, T0 + 2 * HOUR).state === 'recording');
check('and flags that it expired, so the resume event gets written',
  S.resolveState(paused, T0 + 2 * HOUR).expired === true);
check('one second before expiry it is still paused',
  S.resolveState(paused, T0 + 2 * HOUR - 1000).state === 'paused');
check('the stored claim is kept alongside the resolution, for the UI',
  S.resolveState(paused, T0 + 3 * HOUR).storedState === 'paused');

// A paused state with no end time is a bug upstream. The safe reading of a
// bug in an audit log is "record everything".
check('a pause with no end time resolves as recording, not as forever',
  S.resolveState({ state: 'paused', pausedUntil: null }, T0).state === 'recording');
check('a pause with an unparseable end time does the same',
  S.resolveState({ state: 'paused', pausedUntil: 'never' }, T0).state === 'recording');

// Off has no timer by design — that is the difference between the two.
check('off stays off however long you wait',
  S.resolveState({ state: 'off' }, T0 + 1000 * HOUR).state === 'off');
check('off never reports itself as expired',
  S.resolveState({ state: 'off' }, T0 + 1000 * HOUR).expired === false);
check('an unrecognised stored state falls back to recording',
  S.resolveState({ state: 'banana' }, T0).state === 'recording');

// ── Transitions ───────────────────────────────────────────────────────────
console.log('\nTransitions');

const rec = S.defaultConfig();
check('a pause needs a reason',
  S.validateTransition({ state: 'paused', pauseMinutes: 60 }, rec, T0).ok === false);
check('a four-character reason is not enough to be called a reason',
  S.validateTransition({ state: 'paused', pauseMinutes: 60, reason: 'x' }, rec, T0).ok === false);
check('a pause needs a duration',
  S.validateTransition({ state: 'paused', reason: 'bulk import' }, rec, T0).ok === false);
check('a pause within the ceiling is accepted',
  S.validateTransition({ state: 'paused', reason: 'bulk import', pauseMinutes: 120 }, rec, T0).ok);
check('and computes its own end time',
  S.validateTransition({ state: 'paused', reason: 'bulk import', pauseMinutes: 120 }, rec, T0)
    .next.pausedUntil === new Date(T0 + 2 * HOUR).toISOString());
check('a pause beyond the ceiling is refused',
  S.validateTransition({ state: 'paused', reason: 'bulk import', pauseMinutes: 600 }, rec, T0).ok === false);
check('the ceiling is the stored setting, not a constant',
  S.validateTransition({ state: 'paused', reason: 'bulk import', pauseMinutes: 600 },
    { ...rec, settings: { ...rec.settings, pauseMaxMinutes: 1440 } }, T0).ok === true);
check('every preset the UI offers is inside the default ceiling',
  S.PAUSE_PRESETS_MIN.every(m => m <= S.DEFAULT_SETTINGS.pauseMaxMinutes));

check('turning capture off needs a reason',
  S.validateTransition({ state: 'off', confirm: 'OFF' }, rec, T0).ok === false);
check('turning capture off needs OFF typed out',
  S.validateTransition({ state: 'off', reason: 'decommissioning' }, rec, T0).ok === false);
check('a near miss does not count',
  S.validateTransition({ state: 'off', reason: 'decommissioning', confirm: 'of' }, rec, T0).ok === false);
check('reason plus OFF is accepted',
  S.validateTransition({ state: 'off', reason: 'decommissioning', confirm: 'OFF' }, rec, T0).ok);
check('and writes audit.disable',
  S.validateTransition({ state: 'off', reason: 'decommissioning', confirm: 'OFF' }, rec, T0)
    .action === 'audit.disable');

const offCfg = { ...rec, state: 'off' };
check('coming back from off writes audit.enable, not audit.resume',
  S.validateTransition({ state: 'recording' }, offCfg, T0).action === 'audit.enable');
check('coming back from a pause writes audit.resume',
  S.validateTransition({ state: 'recording' }, { ...rec, state: 'paused',
    pausedUntil: new Date(T0 + HOUR).toISOString() }, T0).action === 'audit.resume');
check('resuming needs no reason — restoring the record is never the risky direction',
  S.validateTransition({ state: 'recording' }, offCfg, T0).ok);
check('resuming when already recording is refused rather than logged as a no-op',
  S.validateTransition({ state: 'recording' }, rec, T0).ok === false);
check('an unknown target state is refused',
  S.validateTransition({ state: 'sleeping' }, rec, T0).ok === false);

// ── The drop decision ─────────────────────────────────────────────────────
console.log('\nWhat survives a pause (acceptance criterion 3)');

const pausedNow = S.resolveState(paused, T0);
check('while paused, a settings change is NOT recorded',
  S.shouldRecord('settings', pausedNow).record === false);
check('while paused, a mapping change is NOT recorded',
  S.shouldRecord('mapping', pausedNow).record === false);
check('while paused, a role change IS recorded',
  S.shouldRecord('access', pausedNow).record === true);
check('while paused, a sign-in IS recorded',
  S.shouldRecord('security', pausedNow).record === true);
check('while paused, a PROD write IS recorded',
  S.shouldRecord('prod', pausedNow).record === true);
check('while paused, the log\'s own events ARE recorded',
  S.shouldRecord('audit', pausedNow).record === true);

const offNow = S.resolveState({ state: 'off' }, T0);
check('while off, the same four categories still record',
  ['security', 'access', 'prod', 'audit'].every(c => S.shouldRecord(c, offNow).record));
check('while off, everything else does not',
  ['settings', 'mapping', 'jobs', 'stream', 'data', 'projects', 'connections']
    .every(c => S.shouldRecord(c, offNow).record === false));

const recNow = S.resolveState(rec, T0);
check('while recording with a category disabled, that category drops',
  S.shouldRecord('mapping', recNow, { categories: { mapping: false } }).record === false);
check('but a locked category cannot be dropped that way either',
  S.shouldRecord('prod', recNow, { categories: { prod: false } }).record === true);
check('an unknown category is recorded, not silently dropped',
  S.shouldRecord('wibble', recNow).record === true);
check('a drop always says why',
  S.shouldRecord('settings', pausedNow).reason.length > 0);

// ── Gap windows ───────────────────────────────────────────────────────────
console.log('\nGap windows for the timeline');

const gapEvents = [
  { action: 'audit.pause', occurredAt: '2026-09-11T09:00:00.000Z', actorEmail: 'a@b.c',
    detail: { reason: 'bulk import', pausedUntil: '2026-09-11T11:00:00.000Z' } },
  { action: 'audit.resume', occurredAt: '2026-09-11T11:00:00.000Z', actorType: 'system' },
  { action: 'audit.disable', occurredAt: '2026-09-11T13:00:00.000Z', actorEmail: 'a@b.c',
    detail: { reason: 'decommissioning' } },
  { action: 'audit.enable', occurredAt: '2026-09-11T14:00:00.000Z', actorEmail: 'a@b.c' },
];
const gaps = S.gapWindows(gapEvents, T0);
check('two windows are built from four events', gaps.length === 2);
check('the first is a pause', gaps[0].kind === 'paused');
check('it carries the reason somebody gave', gaps[0].reason === 'bulk import');
check('it names who asked for it', gaps[0].by === 'a@b.c');
check('and records that the system ended it, not a person', gaps[0].endedBy === 'system');
check('the second is an off window', gaps[1].kind === 'off');
check('both are closed', gaps.every(g => g.open === false));

// Entries arrive newest-first from readAudit, so the order is sorted here
// rather than assumed — the same four events reversed must give the same
// two windows.
const reversed = S.gapWindows(gapEvents.slice().reverse(), T0);
check('the same events in reverse order produce the same windows',
  JSON.stringify(reversed) === JSON.stringify(gaps));

const stillOff = S.gapWindows([gapEvents[2]], T0);
check('an unclosed window is returned as open', stillOff[0].open === true);

// The case worth noticing: a pause whose end time passed with no resume
// ever written means nothing has been recorded since and nobody looked.
const overdue = S.gapWindows([{ action: 'audit.pause', occurredAt: '2026-01-01T00:00:00.000Z',
  detail: { reason: 'x', pausedUntil: '2026-01-01T02:00:00.000Z' } }], T0);
check('a pause that never got its resume is flagged overdue', overdue[0].overdue === true);
check('non-state events are ignored entirely',
  S.gapWindows([{ action: 'role.assign', occurredAt: '2026-09-11T10:00:00.000Z' }], T0).length === 0);

// ── Storage: the rules above, actually applied on the way in ──────────────
console.log('\nStorage — the drop decision applied at write time');

// The session event is written without being awaited — deliberately, since it
// must not add latency to the request path — so it is polled for.
const until = (fn, ms) => new Promise((resolve, reject) => {
  const t0 = Date.now();
  (async function spin() {
    if (await fn()) return resolve();
    if (Date.now() - t0 > (ms || 2000)) return reject(new Error('timed out waiting'));
    setTimeout(spin, 10);
  })();
});

function fakeStore(seed) {
  const m = new Map(Object.entries(seed || {}));
  return {
    _m: m,
    async get(k) { return m.has(k) ? JSON.parse(JSON.stringify(m.get(k))) : null; },
    async setJSON(k, v) { m.set(k, JSON.parse(JSON.stringify(v))); },
    async delete(k) { m.delete(k); },
  };
}

(async function storageTests() {
  // ── A plain append ──
  org.invalidateAuditConfig();
  let store = fakeStore();
  const e1 = await org.appendAudit(store, {
    actorOid: 'oid-1', actorEmail: 'lead@example.com', effectiveRoles: ['ML'],
    action: 'sysparam.update', outcome: 'allowed', environment: 'DEV',
  });
  check('an append returns the chained entry', !!(e1 && e1.entryHash));
  check('it is the first link (prevHash empty)', e1.prevHash === '');
  check('the head advanced to it',
    (await store.get('audit/head')).seq === 1);
  check('a legacy flat event gains a category without its call site changing',
    e1.category === 'settings');
  check('and is marked as observed by the server', e1.source === 'server');
  check('an index row was written beside it',
    (await store.get(schema.indexKeyFor(e1.occurredAt))).rows.length === 1);

  const e2 = await org.appendAudit(store, {
    actorOid: 'oid-1', actorEmail: 'lead@example.com', effectiveRoles: ['ML'],
    action: 'role.assign', outcome: 'allowed',
  });
  check('the second entry chains onto the first', e2.prevHash === e1.entryHash);
  check('the chain verifies', (await org.verifyChain(store, {})).ok);

  // ── Paused: the acceptance criterion, through the storage path ──
  org.invalidateAuditConfig();
  store = fakeStore({
    'audit/config': { state: 'paused', reason: 'bulk import', changedBy: 'a@b.c',
                      pausedUntil: new Date(Date.now() + HOUR).toISOString(),
                      settings: S.DEFAULT_SETTINGS },
  });
  const dropped = await org.appendAudit(store, { action: 'sysparam.update', outcome: 'allowed' });
  check('while paused, a settings event is dropped by the SERVER',
    dropped && dropped.dropped === true);
  check('and the chain did not move',
    ((await store.get('audit/head')) || { seq: 0 }).seq === 0);

  const kept = await org.appendAudit(store, { action: 'role.assign', outcome: 'allowed' });
  check('while paused, a role change is still written', !!(kept && kept.entryHash));
  const prodKept = await org.appendAudit(store, {
    action: 'sql.write', outcome: 'allowed', environment: 'PROD' });
  check('while paused, a PROD write is still written', !!(prodKept && prodKept.entryHash));
  check('and is filed under the always-on prod category', prodKept.category === 'prod');

  // ── The pause ending by itself ──
  org.invalidateAuditConfig();
  store = fakeStore({
    'audit/config': { state: 'paused', reason: 'bulk import', changedBy: 'a@b.c',
                      pausedUntil: new Date(Date.now() - 60000).toISOString(),
                      settings: S.DEFAULT_SETTINGS },
  });
  const afterExpiry = await org.appendAudit(store, { action: 'sysparam.update', outcome: 'allowed' });
  check('an event after the pause expires is recorded, with no scheduler involved',
    !!(afterExpiry && afterExpiry.entryHash));
  check('the stored state was flipped back to recording',
    (await store.get('audit/config')).state === 'recording');

  const all = await org.readAudit(store, { limit: 50 });
  const resume = all.entries.find(e => e.action === 'audit.resume');
  check('an audit.resume was written', !!resume);
  check('by system, not by whoever happened to arrive next',
    resume && resume.actorType === 'system' && resume.actorEmail === 'system');
  check('it records the pause it ended',
    resume && resume.detail && resume.detail.pauseReason === 'bulk import');
  check('the resume is filed in the always-on audit category',
    resume && resume.category === 'audit');
  check('the chain is still intact after all of that',
    (await org.verifyChain(store, {})).ok);

  // ── Off ──
  org.invalidateAuditConfig();
  store = fakeStore({
    'audit/config': { state: 'off', reason: 'decommissioning', settings: S.DEFAULT_SETTINGS },
  });
  check('while off, a job event is dropped',
    (await org.appendAudit(store, { action: 'jobs.reorder', outcome: 'allowed' })).dropped === true);
  check('while off, a sign-in is still written',
    !!(await org.appendAudit(store, { action: 'auth.signin', outcome: 'allowed' })).entryHash);
  check('while off, a denial is still written (it is an access event)',
    !!(await org.appendAudit(store, { action: 'role.assign', outcome: 'denied' })).entryHash);

  // ── Fail closed ──
  org.invalidateAuditConfig();
  const brokenStore = fakeStore();
  // A head that reports a different sequence on every read is the race the
  // retry loop exists for; three failures means the write is genuinely lost.
  let n = 0;
  brokenStore.get = async (k) => (k === 'audit/head' ? { seq: n++, hash: '' } : null);
  let threw = false;
  try {
    await org.appendAudit(brokenStore, { action: 'sql.write', environment: 'PROD',
      outcome: 'allowed' }, { required: true });
  } catch (e) { threw = !!e; }
  check('a PROD write whose audit record is lost fails the action (fail closed)', threw);

  org.invalidateAuditConfig();
  const brokenStore2 = fakeStore();
  let n2 = 0;
  brokenStore2.get = async (k) => (k === 'audit/head' ? { seq: n2++, hash: '' } : null);
  const lost = await org.appendAudit(brokenStore2, { action: 'jobs.reorder', outcome: 'allowed' });
  check('but an ordinary event that cannot be recorded does not block the user',
    lost === null);

  // ── Queries read the index, not the chain ──
  org.invalidateAuditConfig();
  store = fakeStore();
  for (const spec of [
    { action: 'role.assign', actorEmail: 'a@b.c', outcome: 'allowed' },
    { action: 'sql.write', actorEmail: 'd@e.f', outcome: 'denied', environment: 'PROD' },
    { action: 'jobs.reorder', actorEmail: 'a@b.c', outcome: 'allowed' },
  ]) await org.appendAudit(store, { effectiveRoles: [], ...spec });

  const q1 = await org.queryAudit(store, {});
  check('an unfiltered query returns every event', q1.total === 3);
  check('newest first', q1.entries[0].action === 'jobs.reorder');
  check('and says it used the index', q1.indexed === true);
  check('filtering by actor narrows it',
    (await org.queryAudit(store, { actor: 'a@b.c' })).total === 2);
  check('filtering by outcome narrows it',
    (await org.queryAudit(store, { outcome: 'denied' })).total === 1);
  check('filtering by environment narrows it',
    (await org.queryAudit(store, { env: 'PROD' })).total === 1);
  check('filtering by category narrows it',
    (await org.queryAudit(store, { category: 'access' })).total === 1);
  check('free text searches action, actor and summary',
    (await org.queryAudit(store, { q: 'reorder' })).total === 1);
  check('a filter matching nothing returns nothing rather than everything',
    (await org.queryAudit(store, { actor: 'nobody@example.com' })).total === 0);

  const page = await org.queryAudit(store, { limit: 2 });
  check('a page is capped at the limit', page.entries.length === 2);
  check('and hands back a cursor', page.nextCursor !== null);
  const page2 = await org.queryAudit(store, { limit: 2, cursor: page.nextCursor });
  check('the cursor continues rather than repeating',
    page2.entries.length === 1 && page2.entries[0].seq === 1);

  // ── The column filters ──
  //
  // TARGET had no filter at all until the headers grew one, and it is the
  // column you most want one on: "everything that touched CRM_PROD" is the
  // question an incident starts with.
  org.invalidateAuditConfig();
  const tstore = fakeStore();
  await org.appendAudit(tstore, { action: 'sql.write', actorEmail: 'a@b.c', outcome: 'allowed',
    target: { type: 'connection', id: 'crm', label: 'Target: CRM_PROD' } });
  await org.appendAudit(tstore, { action: 'jobs.reorder', actorEmail: 'd@e.f', outcome: 'allowed',
    target: { type: 'job', id: 'j1', label: 'Job: Nightly Customers' } });
  // The trap: an event whose ACTION contains the word but whose target does
  // not. A target filter folded into the general search text would catch it.
  await org.appendAudit(tstore, { action: 'crm.sync', actorEmail: 'd@e.f', outcome: 'allowed',
    target: { type: 'job', id: 'j2', label: 'Job: Invoices' } });

  check('the target filter narrows on the target',
    (await org.queryAudit(tstore, { target: 'crm_prod' })).total === 1);
  check('and not on an action that happens to share a word',
    (await org.queryAudit(tstore, { target: 'crm' })).total === 1);
  check('while the free-text search DOES span both, as it should',
    (await org.queryAudit(tstore, { q: 'crm' })).total === 2);
  check('the target filter is case-insensitive',
    (await org.queryAudit(tstore, { target: 'CRM_PROD' })).total === 1);

  check('the source filter separates browser claims from server observations',
    (await org.queryAudit(tstore, { source: 'server' })).total === 3 &&
    (await org.queryAudit(tstore, { source: 'client' })).total === 0);

  // Facets drive the header dropdowns.
  const fq = await org.queryAudit(tstore, {});
  check('a query returns the people who appear in the window',
    fq.facets.actors.length === 2);
  check('with a count each, so a dropdown can say how much is behind an option',
    fq.facets.actors.every(a => a.count > 0));
  check('and the actions too', fq.facets.actions.length === 3);
  check('sorted by value, so the list does not reorder itself as counts move',
    fq.facets.actors[0].value < fq.facets.actors[1].value);

  // The one that matters: a dropdown must not remove its own options as you
  // use it, or there is no way back to the others without clearing the
  // filter you just set.
  const filtered = await org.queryAudit(tstore, { actor: 'd@e.f' });
  check('filtering by a person narrows the ROWS', filtered.total === 2);
  check('but leaves every action still offered in the dropdown',
    filtered.facets.actions.length === 3);
  check('and every person', filtered.facets.actors.length === 2);

  // Entries written before the index existed still have to be findable. An
  // empty index is not proof of an empty log.
  const idxKey = schema.indexKeyFor(new Date().toISOString());
  await store.delete(idxKey);
  const unindexed = await org.queryAudit(store, {});
  check('an event with no index row is still found by walking the chain',
    unindexed.total === 3);
  check('and the response says the index was not used, rather than hiding it',
    unindexed.indexed === false);

  // ── KPI counts ──
  const stats = await org.auditStats(store, { now: Date.now() });
  check('stats fall back gracefully with no index', stats.events >= 0);
  await org.appendAudit(store, { action: 'role.assign', actorEmail: 'z@z.z', outcome: 'allowed' });
  const stats2 = await org.auditStats(store, { now: Date.now() });
  check('stats count events in the window', stats2.events >= 1);
  check('and distinct actors', stats2.actors >= 1);

  // ── Redaction all the way through the storage path ──
  org.invalidateAuditConfig();
  store = fakeStore();
  await org.appendAudit(store, {
    action: 'connection.edit', outcome: 'allowed',
    detail: { server: 'db1', password: 'hunter2',
              target: 'Server=db1;User Id=sa;Password=hunter2;' },
  });
  const stored = JSON.stringify(await store.get('audit/e/0000000001'));
  check('no plaintext secret reaches the blob store', stored.indexOf('hunter2') === -1);
  check('but the entry still records that a connection was edited',
    stored.indexOf('connection.edit') !== -1);

  // ── The session event ──
  //
  // The brief asks for sign-in, sign-out and failed sign-in. Two of the three
  // cannot be observed from here: a failed sign-in never reaches our code
  // (Entra rejects it and no token is issued), and a sign-out is a client-side
  // MSAL call with no request to watch. What IS observable is an
  // authenticated request after a gap, and it is named for that rather than
  // for what it resembles.
  console.log('\nStorage — the session event');
  org.invalidateAuditConfig();
  store = fakeStore({
    'rbac/users': { users: { 'oid-1': { email: 'a@b.c', isActive: true,
      firstSeenAt: '2026-01-01T00:00:00.000Z',
      lastSeenAt: new Date(Date.now() - 40 * 60000).toISOString() } } },
    'rbac/assignments': { assignments: [{ id: 'r1', oid: 'oid-1', role: 'ML', revokedAt: null }] },
    'rbac/classifications': { byKey: {} },
  });
  org.invalidate();
  await org.resolveActor(store, { oid: 'oid-1', email: 'a@b.c' }, {});
  await until(async () => (await org.readAudit(store, { limit: 20 })).entries
    .some(e => e.action === 'auth.session.start'));
  const sess = (await org.readAudit(store, { limit: 20 })).entries
    .find(e => e.action === 'auth.session.start');
  check('a request after a long gap records a session start', !!sess);
  check('in the always-on security category, so a pause cannot hide it',
    sess && sess.category === 'security');
  check('and says it was derived rather than reported by the identity provider',
    sess && /not an identity-provider event/.test(sess.detail.derivedFrom));

  org.invalidate();
  const beforeSecond = (await store.get('audit/head')).seq;
  await org.resolveActor(store, { oid: 'oid-1', email: 'a@b.c' }, {});
  await new Promise(r => setTimeout(r, 30));
  check('a second request moments later records nothing — it is a session, not a request log',
    (await store.get('audit/head')).seq === beforeSecond);

  // ── Tamper detection still works on the extended entry ──
  const e = await store.get('audit/e/0000000001');
  check('an untouched extended entry verifies',
    rbac.hashEntry(e.prevHash, e) === e.entryHash);
  const edited = { ...e, summary: 'something else' };
  check('editing any added field breaks the hash, same as an original one',
    rbac.hashEntry(edited.prevHash, edited) !== edited.entryHash);

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
