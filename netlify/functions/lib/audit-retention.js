// netlify/functions/lib/audit-retention.js
//
// Retention: aging entries out of the audit chain without breaking the chain.
//
// ── The problem this has to solve ─────────────────────────────────────────
//
// Retention and a hash chain are in direct conflict, and the conflict is not
// incidental — it is the whole reason this file is more than a delete loop.
//
// Verification walks from the first entry, whose prevHash is '', and checks
// that every entry's prevHash matches the one before it. Delete entries
// 1..N and the first surviving entry's prevHash points at something that no
// longer exists, so the walk cannot start and every entry after it is
// unverifiable. A naive purge does not lose the old events; it loses the
// evidential value of the ones you kept.
//
// The resolution is a CHECKPOINT: a record of what the chain looked like at
// the purge boundary — the first surviving sequence and its hash —
// verification can start from instead of from ''. The format shipped before
// this job existed (audit-schema.checkpoint) precisely so entries written in
// the meantime would verify against a checkpoint written later without
// anything being re-chained.
//
// ── "Signed", and what is actually true ──────────────────────────────────
//
// The brief asks for a *signed* checkpoint. There is no server-held signing
// key in this deployment — the same gap that stops the Off state emailing
// administrators — and inventing one that lives beside the data it attests
// would be theatre: anybody who could alter the checkpoint could re-sign it.
//
// What the checkpoint has instead is better than a key kept next to the
// lock. Every purge writes an `audit.retention.purge` entry INTO THE CHAIN
// carrying the checkpoint, before anything is deleted. So the checkpoint is
// attested by the chain that continues after it: altering the checkpoint
// blob makes it disagree with the purge entry, and altering the purge entry
// breaks every hash after it. That is a real tamper-evidence argument, and
// it is the one the Integrity tab makes.
//
// ── Archive, then purge — and what "purge" is allowed to mean ────────────
//
// The brief says "archives then purges", and the honest question is where
// the archive goes. Writing it to the same blob store is not cold storage
// and does not reduce what the organisation holds; claiming otherwise on a
// settings screen would be a lie. But it IS worth doing: it takes old
// entries off the query path and out of the verification walk, which is
// most of what retention buys operationally, while destroying nothing.
//
// So there are two behaviours and the administrator picks, rather than this
// file picking for them:
//
//   archiveBeforePurge: true (default)  entries are copied to
//     audit/archive/<from>-<to> and then removed from the live chain. The
//     organisation still holds them. This is the safe default because an
//     audit trail is the last thing that should be destroyed by a setting
//     somebody changed without reading it.
//
//   archiveBeforePurge: false  entries are deleted outright. This is real
//     erasure, for a tenant whose obligation is data minimisation rather
//     than retention. Turning it off is itself an audited settings change,
//     so the decision is on the record.
//
// ── Fail closed ──────────────────────────────────────────────────────────
//
// The purge entry is appended with { required: true } BEFORE anything is
// deleted. If it cannot be written, nothing is purged. A purge nobody can
// see the record of is exactly the operation an auditor would refuse to
// accept, and the ordering is what makes "we deleted these and here is the
// entry saying so" true rather than hopeful.

'use strict';

const org = require('./org-store');
const schema = require('./audit-schema');
const astate = require('./audit-state');

const CHECKPOINT_KEY = 'audit/checkpoint';
const ARCHIVE_PREFIX = 'audit/archive/';

// One night's budget. A Netlify function has ~26 seconds and each entry is a
// blob read plus a blob delete, so a backlog is worked down over successive
// nights rather than attempted in one run that times out halfway and leaves
// the chain in a state nobody planned. Successive runs are idempotent: the
// second one starts from the checkpoint the first one left.
const MAX_PER_RUN = 500;

const pad = (n) => String(n).padStart(10, '0');
const entryKey = (seq) => 'audit/e/' + pad(seq);

async function loadCheckpoint(store) {
  return (await store.get(CHECKPOINT_KEY, { type: 'json' }).catch(() => null)) || null;
}

// Where the live chain now begins. Without a checkpoint that is sequence 1;
// with one it is the anchor the last purge left behind.
function floorOf(checkpoint) {
  return (checkpoint && checkpoint.anchorSeq) ? checkpoint.anchorSeq : 1;
}

async function runRetention(store, opts) {
  opts = opts || {};
  const now = opts.now === undefined ? Date.now() : opts.now;
  const maxPerRun = opts.maxPerRun || MAX_PER_RUN;

  const cfg = await org.loadAuditConfig(store, { fresh: true });
  const settings = astate.normaliseSettings(cfg.settings);
  const days = settings.retentionDays;
  const cutoffMs = now - days * 86400000;

  const head = (await store.get('audit/head', { type: 'json' })) || { seq: 0 };
  if (!head.seq) return { ran: true, purged: 0, reason: 'the chain is empty' };

  const checkpoint = await loadCheckpoint(store);
  const floor = floorOf(checkpoint);

  // Walk forward from the floor for the oldest entry still inside the
  // retention window. Entries are appended in time order, so the first one
  // at or after the cutoff ends the search — there is nothing older behind
  // it. Bounded by the run budget.
  let lastExpired = 0;
  let scanned = 0;
  let reachedWindow = false;
  for (let seq = floor; seq <= head.seq && scanned < maxPerRun; seq++) {
    const e = await store.get(entryKey(seq), { type: 'json' }).catch(() => null);
    scanned++;
    if (!e) continue;                       // already gone; a previous run stopped here
    if (Date.parse(e.occurredAt) >= cutoffMs) { reachedWindow = true; break; }
    lastExpired = seq;
  }

  if (!lastExpired) {
    return { ran: true, purged: 0, retentionDays: days,
             reason: 'no entry has aged out of the ' + days + '-day window' };
  }

  // NEVER purge the whole chain. A log with no entries cannot be verified,
  // cannot be read, and cannot anchor the next checkpoint — and a clock skew
  // or a mis-set retention is exactly how that would otherwise happen.
  if (lastExpired >= head.seq) lastExpired = head.seq - 1;
  if (lastExpired < floor) {
    return { ran: true, purged: 0, retentionDays: days,
             reason: 'refusing to purge the only remaining entry' };
  }

  // The anchor is the first entry that survives. Without it there is no
  // checkpoint to write, and without a checkpoint the purge would leave the
  // chain unverifiable — so this is a hard stop rather than a warning.
  const anchor = await store.get(entryKey(lastExpired + 1), { type: 'json' }).catch(() => null);
  if (!anchor || !anchor.entryHash) {
    return { ran: true, purged: 0, retentionDays: days, error: true,
             reason: 'the first surviving entry (' + (lastExpired + 1) + ') is missing, ' +
                     'so no checkpoint can be anchored — refusing to purge' };
  }

  const seqs = [];
  for (let s = floor; s <= lastExpired; s++) seqs.push(s);
  const doomed = (await Promise.all(
    seqs.map((s) => store.get(entryKey(s), { type: 'json' }).catch(() => null))
  )).filter(Boolean);

  if (!doomed.length) {
    return { ran: true, purged: 0, retentionDays: days,
             reason: 'the expired range is already empty' };
  }

  // ── 1. Archive, if the organisation has not chosen erasure ──
  let archiveKey = null;
  if (settings.archiveBeforePurge) {
    archiveKey = ARCHIVE_PREFIX + pad(floor) + '-' + pad(lastExpired);
    await store.setJSON(archiveKey, {
      version: 1,
      from: floor, to: lastExpired, count: doomed.length,
      archivedAt: new Date(now).toISOString(),
      retentionDays: days,
      // Verbatim. An archive that reshapes the entries cannot be checked
      // against the hashes that were in the chain, which is the one thing
      // an archived audit entry is for.
      entries: doomed,
    });
  }

  const next = schema.checkpoint(anchor, doomed.length, days);
  next.purgedTotal = ((checkpoint && checkpoint.purgedTotal) || 0) + doomed.length;
  next.archived = !!settings.archiveBeforePurge;
  next.archiveKey = archiveKey;
  next.previousAnchorSeq = (checkpoint && checkpoint.anchorSeq) || null;

  // ── 2. Record the purge BEFORE performing it, and fail closed ──
  //
  // required:true throws if the entry cannot be written, which aborts here
  // with nothing deleted. This ordering is what makes the checkpoint
  // trustworthy: it is inside the chain that continues after it, so it
  // cannot be altered without breaking every hash that follows.
  await org.appendAudit(store, {
    actorOid: 'system', actorEmail: 'system', effectiveRoles: [],
    actorType: 'system',
    action: 'audit.retention.purge', category: 'audit',
    outcome: 'allowed', severity: 'notice',
    resourceType: 'audit_chain', resourceId: 'retention',
    summary: 'Purged ' + doomed.length + ' event' + (doomed.length === 1 ? '' : 's') +
             ' older than ' + days + ' days' +
             (settings.archiveBeforePurge ? ' (archived first)' : ' (erased, not archived)'),
    changes: [
      { field: 'chainStartsAt', before: floor, after: lastExpired + 1 },
      { field: 'entriesPurged', before: 0, after: doomed.length },
    ],
    detail: {
      retentionDays: days,
      archived: !!settings.archiveBeforePurge,
      archiveKey: archiveKey,
      checkpoint: next,
      oldestKept: anchor.occurredAt,
      newestPurged: doomed[doomed.length - 1].occurredAt,
    },
  }, { required: true });

  // ── 3. Checkpoint, then delete ──
  //
  // The checkpoint lands before the deletions so that a run interrupted
  // mid-delete leaves verification working from the new anchor rather than
  // from a boundary that no longer has entries behind it.
  await store.setJSON(CHECKPOINT_KEY, next);

  let deleted = 0;
  for (const s of seqs) {
    try { await store.delete(entryKey(s)); deleted++; } catch (e) { /* next run retries */ }
  }

  const indexPages = await pruneIndex(store, floor, lastExpired, doomed);

  return {
    ran: true,
    purged: doomed.length,
    deleted,
    retentionDays: days,
    archived: !!settings.archiveBeforePurge,
    archiveKey,
    chainStartsAt: lastExpired + 1,
    checkpoint: next,
    indexPagesRewritten: indexPages,
    // A run that hit its budget has more to do; the caller reports it and
    // the next night picks up from the checkpoint this one wrote.
    more: !reachedWindow && scanned >= maxPerRun,
  };
}

// The monthly index is a cache of the chain, so purged rows must come out of
// it or a filtered query would offer a row whose entry no longer exists.
// Failing here costs a stale search result, never an entry — which is why it
// runs last and does not abort the purge.
async function pruneIndex(store, from, to, purgedEntries) {
  const keys = {};
  for (const e of purgedEntries) keys[schema.indexKeyFor(e.occurredAt)] = true;
  let rewritten = 0;
  for (const k of Object.keys(keys)) {
    try {
      const page = await store.get(k, { type: 'json' });
      if (!page || !Array.isArray(page.rows)) continue;
      const kept = page.rows.filter((r) => r.seq < from || r.seq > to);
      if (kept.length === page.rows.length) continue;
      if (kept.length) await store.setJSON(k, { rows: kept });
      else await store.delete(k);
      rewritten++;
    } catch (e) {
      console.error('[audit-retention] index prune failed for ' + k + ': ' + e.message);
    }
  }
  return rewritten;
}

// What the Integrity tab shows, and what verifyChain anchors on.
async function checkpointStatus(store) {
  const cp = await loadCheckpoint(store);
  if (!cp) return { purged: false, chainStartsAt: 1 };
  return {
    purged: true,
    chainStartsAt: cp.anchorSeq,
    anchorHash: cp.anchorHash,
    anchorAt: cp.anchorAt,
    purgedThroughSeq: cp.purgedThroughSeq,
    purgedTotal: cp.purgedTotal || cp.purgedCount || 0,
    lastPurgeAt: cp.createdAt,
    archived: !!cp.archived,
    archiveKey: cp.archiveKey || null,
    retentionDays: cp.retentionDays || null,
  };
}

module.exports = {
  runRetention, checkpointStatus, loadCheckpoint, floorOf,
  CHECKPOINT_KEY, ARCHIVE_PREFIX, MAX_PER_RUN,
};
