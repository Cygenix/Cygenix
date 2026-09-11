// tests/audit-retention.test.js — ageing entries out of the chain without
// destroying the evidential value of the ones that remain.
//
// Retention and a hash chain are in direct conflict, and this file is mostly
// about that conflict rather than about deleting blobs.
//
// Verification walks from the first entry, whose prevHash is '', checking
// each entry against the one before it. Delete the beginning of the chain
// and the first surviving entry points at something that no longer exists:
// the walk cannot start, and every entry you KEPT becomes unverifiable. A
// naive purge does not lose the old events, it loses the value of the new
// ones. So the tests that matter here are:
//
//   * the chain still verifies after a purge, anchored on the checkpoint;
//   * tampering is still caught after a purge — on the anchor itself, which
//     is the entry a careless implementation would have to trust blindly;
//   * a purge that cannot be recorded does not happen.
//
// The blob store is faked in-process, same as audit-state.test.js, because
// the interesting behaviour is the ORDER of the writes — record, checkpoint,
// then delete — and a mock asserting calls would test the implementation
// rather than the outcome.

'use strict';

const org = require('../netlify/functions/lib/org-store');
const retention = require('../netlify/functions/lib/audit-retention');
const schema = require('../netlify/functions/lib/audit-schema');
const astate = require('../netlify/functions/lib/audit-state');
const rbac = require('../netlify/functions/lib/rbac');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

const DAY = 86400000;
const pad = (n) => String(n).padStart(10, '0');

function fakeStore(seed) {
  const m = new Map(Object.entries(seed || {}));
  return {
    _m: m,
    async get(k) { return m.has(k) ? JSON.parse(JSON.stringify(m.get(k))) : null; },
    async setJSON(k, v) { m.set(k, JSON.parse(JSON.stringify(v))); },
    async delete(k) { m.delete(k); },
  };
}

// Build a chain whose entries are `ages` days old, in order, oldest first.
// Written through the real appendAudit so they are really chained and really
// indexed — the point of this file is what happens to a real chain.
async function seedChain(store, ages) {
  for (let i = 0; i < ages.length; i++) {
    const at = new Date(Date.now() - ages[i] * DAY).toISOString();
    await org.appendAudit(store, schema.buildEntry({
      action: 'jobs.reorder', category: 'jobs', outcome: 'allowed',
      summary: 'event ' + (i + 1),
    }, { actor: { oid: 'o1', email: 'a@b.c', roles: ['ML'] }, now: Date.parse(at) }));
  }
}

(async () => {
  console.log('Audit retention — purging without breaking the chain\n');

  // ── Nothing to do ───────────────────────────────────────────────────────
  section('1. When there is nothing to purge');

  org.invalidateAuditConfig();
  let store = fakeStore();
  check('an empty chain is a no-op, not an error',
    (await retention.runRetention(store, {})).purged === 0);

  org.invalidateAuditConfig();
  store = fakeStore();
  await seedChain(store, [10, 5, 1]);
  let r = await retention.runRetention(store, {});
  check('a chain entirely inside the retention window is left alone', r.purged === 0);
  check('and says why rather than returning a bare zero', /aged out/.test(r.reason));
  check('every entry is still there', (await store.get('audit/e/' + pad(1))) !== null);
  check('and no checkpoint was written for a purge that did not happen',
    (await store.get(retention.CHECKPOINT_KEY)) === null);

  // The guard that matters most on a misconfigured clock or retention: a
  // chain where EVERYTHING has aged out must still keep an anchor, or the
  // log cannot be verified, read, or continued.
  org.invalidateAuditConfig();
  store = fakeStore();
  await seedChain(store, [900]);
  r = await retention.runRetention(store, {});
  check('a single ancient entry is not purged — something must anchor the chain',
    r.purged === 0);
  check('and the refusal says so', /only remaining entry/.test(r.reason));

  // ── A real purge ────────────────────────────────────────────────────────
  section('2. A purge, and the chain after it');

  org.invalidateAuditConfig();
  store = fakeStore();
  // Six entries: four older than a year, two inside it.
  await seedChain(store, [500, 480, 460, 440, 10, 1]);
  const headBefore = (await store.get('audit/head')).seq;
  check('the chain verifies before anything is purged',
    (await org.verifyChain(store, {})).ok);

  r = await retention.runRetention(store, {});
  check('the four expired entries are purged', r.purged === 4);
  check('the two inside the window are not', r.chainStartsAt === 5);
  check('the retention period it used is the stored setting', r.retentionDays === 365);

  check('the purged entries are gone from the live chain',
    (await store.get('audit/e/' + pad(1))) === null &&
    (await store.get('audit/e/' + pad(4))) === null);
  check('and the kept ones are not',
    (await store.get('audit/e/' + pad(5))) !== null);

  // THE test. Everything else in this file is in service of this line.
  const after = await org.verifyChain(store, {});
  check('THE CHAIN STILL VERIFIES AFTER THE PURGE', after.ok, JSON.stringify(after));
  check('and says it was anchored on the checkpoint rather than walked from the start',
    after.anchoredOnCheckpoint === true);
  check('reporting where the chain now begins', after.chainStartsAt === 5);
  check('and how many entries have been purged over its life', after.purgedTotal === 4);

  // ── The checkpoint ──────────────────────────────────────────────────────
  section('3. The checkpoint');

  const cp = await store.get(retention.CHECKPOINT_KEY);
  check('a checkpoint was written', !!cp);
  check('anchored on the first SURVIVING entry, not the last purged one',
    cp.anchorSeq === 5);
  check('carrying that entry\'s hash, so a tampered anchor is detectable',
    cp.anchorHash === (await store.get('audit/e/' + pad(5))).entryHash);
  check('and naming the boundary', cp.purgedThroughSeq === 4);
  check('it is versioned', cp.version === 1);

  // The purge is itself in the chain, which is what makes the checkpoint
  // trustworthy without a signing key: altering the checkpoint blob makes it
  // disagree with this entry, and altering this entry breaks every hash
  // after it.
  const trail = await org.readAudit(store, { limit: 50 });
  const purgeEntry = trail.entries.filter((e) => e.action === 'audit.retention.purge')[0];
  check('the purge wrote its own entry into the chain', !!purgeEntry);
  check('by system, in the always-on audit category',
    purgeEntry.actorType === 'system' && purgeEntry.category === 'audit');
  check('carrying the checkpoint, so the chain attests it',
    purgeEntry.detail.checkpoint.anchorHash === cp.anchorHash);
  check('and the count, as the brief asks',
    purgeEntry.changes.some((c) => c.field === 'entriesPurged' && c.after === 4));
  check('the purge entry survives its own purge — it is newer than the boundary',
    purgeEntry.seq > cp.purgedThroughSeq);

  // ── Tampering, after a purge ────────────────────────────────────────────
  section('4. Tampering is still caught across the boundary');

  // The anchor is the entry a careless implementation has to trust, because
  // there is nothing behind it to check it against. The checkpoint is what
  // stops that being a blind spot.
  const anchorKey = 'audit/e/' + pad(5);
  const realAnchor = await store.get(anchorKey);
  await store.setJSON(anchorKey, Object.assign({}, realAnchor, { summary: 'quietly edited' }));
  let broken = await org.verifyChain(store, {});
  check('editing the ANCHOR is detected', !broken.ok);
  check('and named as the entry it is', broken.brokenAt === 5);
  await store.setJSON(anchorKey, realAnchor);
  check('restoring it verifies again', (await org.verifyChain(store, {})).ok);

  // A forged anchor that hashes to itself correctly but is not the entry the
  // checkpoint attested — the attack the self-check alone would miss.
  const forged = rbac.chainEntry(realAnchor.prevHash,
    Object.assign({}, realAnchor, { entryHash: undefined, summary: 'forged but self-consistent' }));
  check('the forgery is internally consistent, so a self-check alone passes it',
    rbac.hashEntry(forged.prevHash, forged) === forged.entryHash);
  await store.setJSON(anchorKey, forged);
  broken = await org.verifyChain(store, {});
  check('but it does not match the checkpoint, and is caught', !broken.ok);
  check('with a reason naming the checkpoint rather than a generic mismatch',
    /checkpoint/.test(broken.reason), broken.reason);
  await store.setJSON(anchorKey, realAnchor);

  // And an edit further along still breaks in the ordinary way.
  const sixKey = 'audit/e/' + pad(6);
  const real6 = await store.get(sixKey);
  await store.setJSON(sixKey, Object.assign({}, real6, { summary: 'edited' }));
  broken = await org.verifyChain(store, {});
  check('an edit after the boundary breaks the chain as it always did',
    !broken.ok && broken.brokenAt === 6);
  await store.setJSON(sixKey, real6);
  check('and the chain is intact once more', (await org.verifyChain(store, {})).ok);

  // ── Archive vs erasure ──────────────────────────────────────────────────
  section('5. Archive, or erase — the administrator decides');

  check('by default the entries are archived, not destroyed', r.archived === true);
  const archive = await store.get(r.archiveKey);
  check('the archive holds every purged entry', archive.count === 4 && archive.entries.length === 4);
  check('verbatim, so they can still be checked against the hashes they carried',
    rbac.hashEntry(archive.entries[1].prevHash, archive.entries[1]) === archive.entries[1].entryHash);
  check('and the archive chains internally, exactly as it did in the live log',
    archive.entries[1].prevHash === archive.entries[0].entryHash);
  check('the archive names the range and the day it was taken',
    archive.from === 1 && archive.to === 4 && !!archive.archivedAt);

  // Erasure is a deliberate choice, for a tenant whose obligation is data
  // minimisation rather than retention.
  org.invalidateAuditConfig();
  store = fakeStore({
    'audit/config': { state: 'recording',
                      settings: Object.assign({}, astate.DEFAULT_SETTINGS,
                        { retentionDays: 90, archiveBeforePurge: false }) },
  });
  await seedChain(store, [200, 180, 5]);
  r = await retention.runRetention(store, {});
  check('with archiving off, entries are erased outright', r.purged === 2 && r.archived === false);
  check('and no archive blob is written', r.archiveKey === null);
  check('the erasure is recorded as an erasure, not as an archive',
    /erased, not archived/i.test(
      (await org.readAudit(store, { limit: 20 })).entries
        .filter((e) => e.action === 'audit.retention.purge')[0].summary));
  check('and the chain still verifies', (await org.verifyChain(store, {})).ok);
  check('the shorter retention period was honoured', r.retentionDays === 90);

  // ── Fail closed ─────────────────────────────────────────────────────────
  section('6. A purge that cannot be recorded does not happen');

  org.invalidateAuditConfig();
  store = fakeStore();
  await seedChain(store, [500, 480, 1]);
  const liveHead = (await store.get('audit/head')).seq;

  // A head that reports a different sequence on every read is the race the
  // append retry exists for; three failures means the write is genuinely
  // lost, and required:true then throws.
  //
  // The FIRST head read is left alone: runRetention reads it to find the end
  // of the chain, and a flapping value there would make it bail before ever
  // reaching the append — which would pass this test for the wrong reason.
  // The sabotage starts once the scan is done.
  const realGet = store.get.bind(store);
  let headReads = 0;
  let n = 0;
  store.get = async (k) => {
    if (k !== 'audit/head') return realGet(k);
    if (headReads++ === 0) return realGet(k);
    return { seq: n++, hash: '' };
  };
  let threw = false;
  try { await retention.runRetention(store, {}); } catch (e) { threw = !!e.auditFailure; }
  store.get = realGet;

  check('a purge whose record cannot be written throws rather than proceeding', threw);
  check('AND NOTHING WAS DELETED',
    (await store.get('audit/e/' + pad(1))) !== null &&
    (await store.get('audit/e/' + pad(2))) !== null);
  check('no checkpoint was written either',
    (await store.get(retention.CHECKPOINT_KEY)) === null);
  check('so the chain is untouched and still verifies',
    (await store.get('audit/head')).seq === liveHead &&
    (await org.verifyChain(store, {})).ok);

  // A missing anchor is the other refusal: without the first surviving entry
  // there is no checkpoint to write, and purging anyway would leave the log
  // unverifiable forever.
  org.invalidateAuditConfig();
  store = fakeStore();
  await seedChain(store, [500, 480, 1]);
  await store.delete('audit/e/' + pad(3));
  r = await retention.runRetention(store, {});
  check('a missing anchor refuses the purge', r.purged === 0 && r.error === true);
  check('and says exactly why', /no checkpoint can be anchored/.test(r.reason));
  check('leaving the expired entries in place',
    (await store.get('audit/e/' + pad(1))) !== null);

  // ── Budget and resumption ───────────────────────────────────────────────
  section('7. A backlog is worked down, not attempted in one pass');

  org.invalidateAuditConfig();
  store = fakeStore();
  await seedChain(store, [500, 499, 498, 497, 496, 495, 1]);
  r = await retention.runRetention(store, { maxPerRun: 2 });
  check('a budgeted run purges only its budget', r.purged === 2);
  check('and says there is more to do', r.more === true);
  check('the chain verifies mid-backlog', (await org.verifyChain(store, {})).ok);

  const firstCp = await store.get(retention.CHECKPOINT_KEY);
  r = await retention.runRetention(store, { maxPerRun: 2 });
  check('the next run resumes from the checkpoint rather than rescanning',
    r.purged === 2 && r.checkpoint.previousAnchorSeq === firstCp.anchorSeq);
  check('the running total carries forward', r.checkpoint.purgedTotal === 4);
  check('and the chain still verifies', (await org.verifyChain(store, {})).ok);

  r = await retention.runRetention(store, { maxPerRun: 50 });
  check('a final run clears the rest', r.purged === 2 && r.more === false);
  check('leaving only the entries inside the window, plus the purge records',
    (await org.verifyChain(store, {})).chainStartsAt === 7);
  check('and it all still verifies', (await org.verifyChain(store, {})).ok);
  check('running again once there is nothing left to do is a clean no-op',
    (await retention.runRetention(store, {})).purged === 0);

  // ── The index ───────────────────────────────────────────────────────────
  section('8. The search index follows the chain');

  const idxKey = schema.indexKeyFor(new Date(Date.now() - 500 * DAY).toISOString());
  const oldPage = await store.get(idxKey);
  check('the index page for a fully purged month is removed rather than left ' +
        'offering rows whose entries are gone', oldPage === null);

  const q = await org.queryAudit(store, {});
  const missing = [];
  for (const e of q.entries) {
    if (!(await store.get('audit/e/' + pad(e.seq)))) missing.push(e.seq);
  }
  check('no query result points at an entry that no longer exists',
    missing.length === 0, missing.join(', '));

  // ── What the screen is told ─────────────────────────────────────────────
  section('9. Reporting');

  const st = await retention.checkpointStatus(store);
  check('the status says the chain has been purged', st.purged === true);
  check('where it now starts', st.chainStartsAt === 7);
  check('how many entries have gone over its life', st.purgedTotal === 6);
  check('whether they were archived or erased', st.archived === true);
  check('and when the last purge ran', !!st.lastPurgeAt);

  org.invalidateAuditConfig();
  const virgin = fakeStore();
  await seedChain(virgin, [1]);
  const st2 = await retention.checkpointStatus(virgin);
  check('a chain that has never been purged says so plainly',
    st2.purged === false && st2.chainStartsAt === 1);

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
