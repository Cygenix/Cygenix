// netlify/functions/audit-retention.js
//
// The nightly retention job. Netlify's cron invokes it; there is no HTTP
// route, no caller input and no token — which is exactly why the work itself
// lives in lib/audit-retention.js and this file is only the trigger.
//
// ── Why this is a separate function from scheduled-runner ────────────────
//
// scheduled-runner fires every minute and dispatches customer migration
// work. Retention runs once a night and edits the audit chain. Sharing a
// function would mean a bug in one could take out the other, and would put
// a minute-frequency hot path in the same deployment unit as the one thing
// in this system that deletes evidence. They are kept apart deliberately.
//
// ── Why 03:00 UTC ────────────────────────────────────────────────────────
//
// Far from the top of the hour, where every other cron in the world fires,
// and inside the quiet window for a UK-hours product. The exact minute does
// not matter — retention is measured in days, and a run that slips an hour
// changes nothing — but a run that collides with the scheduler's busiest
// tick would be contending for the same blob store for no reason.
//
// ── What happens if a run does not finish ────────────────────────────────
//
// Nothing bad, and nothing is lost. Each run purges at most MAX_PER_RUN
// entries and writes a checkpoint for what it did; the next run starts from
// that checkpoint. A backlog — the first run after this ships, on a chain
// older than the retention period — is worked down over successive nights
// rather than attempted in one pass that times out halfway through a delete
// loop. The `more` flag in the result says whether there is another night's
// work outstanding, and it is logged so an operator can see the backlog
// shrinking rather than guessing.

'use strict';

const org = require('./lib/org-store');
const retention = require('./lib/audit-retention');

exports.handler = async function () {
  const startedAt = Date.now();
  let store;
  try {
    store = org.orgStore();
  } catch (e) {
    // No blob credentials — the same condition that makes every other
    // org-store call fail. Log and stop; there is nobody to return an error
    // to, and retrying inside the same tick would not fix it.
    console.error('[audit-retention] store unavailable: ' + e.message);
    return { statusCode: 200, body: 'store unavailable' };
  }

  try {
    const result = await retention.runRetention(store, {});
    const took = Date.now() - startedAt;

    if (result.error) {
      // A refusal, not a crash: the job decided it was not safe to purge.
      // Loud, because a retention job that quietly declines every night
      // looks identical to one that has nothing to do.
      console.error('[audit-retention] REFUSED after ' + took + 'ms: ' + result.reason);
      return { statusCode: 200, body: 'refused: ' + result.reason };
    }

    if (!result.purged) {
      console.log('[audit-retention] nothing to do after ' + took + 'ms — ' + result.reason);
      return { statusCode: 200, body: 'nothing to do' };
    }

    console.log('[audit-retention] purged ' + result.purged + ' entr' +
      (result.purged === 1 ? 'y' : 'ies') +
      ' older than ' + result.retentionDays + ' days in ' + took + 'ms; ' +
      (result.archived ? 'archived to ' + result.archiveKey : 'ERASED, not archived') +
      '; chain now starts at ' + result.chainStartsAt +
      (result.more ? '; MORE REMAINING — the next run continues' : ''));

    return { statusCode: 200, body: 'purged ' + result.purged };
  } catch (e) {
    // The one error worth shouting about is a failed audit write, because
    // that is the fail-closed path doing its job: nothing was deleted.
    console.error('[audit-retention] run failed after ' + (Date.now() - startedAt) + 'ms: ' +
      e.message + (e.auditFailure ? ' (nothing was purged)' : ''));
    return { statusCode: 200, body: 'failed: ' + e.message };
  }
};
