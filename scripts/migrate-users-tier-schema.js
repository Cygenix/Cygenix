/**
 * migrate-users-tier-schema.js
 *
 * One-shot migration to add tier fields to existing user documents in the
 * Cosmos `users` container. Safe to run multiple times (idempotent).
 *
 * Defaults applied:
 *   - admin / demo users     → no change to tier (they bypass tier checks
 *                               anyway via role). tier_status set to 'none'
 *                               for visibility.
 *   - everyone else          → tier:null, tier_status:'none' so they hit
 *                               pick-plan.html on next sign-in.
 *
 * Backfills the existing schema:
 *   - plan          → tier (only if the value is starter/pro/business/enterprise)
 *   - stripeId      → stripe_customer_id
 *   - status        → tier_status (mapped: 'active'→'active', 'invited'/'trial'→'none')
 *
 * Usage:
 *   1. Set env vars:
 *        export COSMOS_ENDPOINT=https://your-cosmos.documents.azure.com:443/
 *        export COSMOS_KEY=...
 *        export COSMOS_DATABASE=cygenix    # optional, defaults to 'cygenix'
 *   2. From the Function project root (so node_modules/@azure/cosmos is available):
 *        node migrate-users-tier-schema.js --dry-run
 *        node migrate-users-tier-schema.js --apply
 *
 * --dry-run prints what WOULD change without writing anything. Run this
 * first and read the output before --apply.
 */
const { CosmosClient } = require('@azure/cosmos');

const DRY_RUN = process.argv.includes('--dry-run');
const APPLY   = process.argv.includes('--apply');

if (!DRY_RUN && !APPLY) {
  console.error('Pass --dry-run or --apply');
  process.exit(1);
}

if (!process.env.COSMOS_ENDPOINT || !process.env.COSMOS_KEY) {
  console.error('COSMOS_ENDPOINT and COSMOS_KEY must be set');
  process.exit(1);
}

const client = new CosmosClient({
  endpoint: process.env.COSMOS_ENDPOINT,
  key:      process.env.COSMOS_KEY
});
const db = client.database(process.env.COSMOS_DATABASE || 'cygenix');
const container = db.container('users');

// Map old `plan` values to the new `tier` enum. Only known plan values
// are migrated; anything else is treated as "no plan" and tier stays null.
const PLAN_TO_TIER = {
  'starter':    'starter',
  'pro':        'pro',
  'business':   'business',
  'enterprise': 'enterprise'
};

// Map old `status` values to the new `tier_status` enum.
const STATUS_TO_TIER_STATUS = {
  'active':   'active',
  'trialing': 'trialing',
  'trial':    'none',     // old "trial" label was pre-Stripe — treat as no real subscription
  'invited':  'none',
  'past_due': 'past_due',
  'cancelled':'cancelled',
  'canceled': 'cancelled'
};

function plan(o) {
  // Build the patch — fields to set on this document.
  const patch = {};

  // tier: only set if not already present and we can derive it from plan.
  if (o.tier === undefined) {
    const fromPlan = PLAN_TO_TIER[String(o.plan || '').toLowerCase()] || null;
    patch.tier = fromPlan;
  }

  // tier_status: only set if not already present.
  if (o.tier_status === undefined) {
    const fromStatus = STATUS_TO_TIER_STATUS[String(o.status || '').toLowerCase()];
    patch.tier_status = fromStatus !== undefined ? fromStatus : 'none';
  }

  // billing_period: leave null until Stripe tells us.
  if (o.billing_period === undefined) patch.billing_period = null;

  // Stripe identifiers
  if (o.stripe_customer_id === undefined) {
    patch.stripe_customer_id = o.stripeId || null;
  }
  if (o.stripe_subscription_id === undefined) {
    patch.stripe_subscription_id = null;
  }

  // Period bookkeeping
  if (o.current_period_end === undefined)   patch.current_period_end = null;
  if (o.cancel_at_period_end === undefined) patch.cancel_at_period_end = false;
  if (o.pending_tier_change === undefined)  patch.pending_tier_change = null;

  // Trial timestamp — keep the existing trialEndsAt under its original
  // name AND mirror to the new snake_case name. cygenix-cosmos-sync.js
  // and admin.html still read trialEndsAt; the new schema uses
  // trial_ends_at. Both will work.
  if (o.trial_ends_at === undefined && o.trialEndsAt) {
    patch.trial_ends_at = o.trialEndsAt;
  } else if (o.trial_ends_at === undefined) {
    patch.trial_ends_at = null;
  }

  // Monthly counters — start at 0
  if (o.monthly_jobs_used === undefined)     patch.monthly_jobs_used = 0;
  if (o.monthly_jobs_reset_at === undefined) patch.monthly_jobs_reset_at = null;

  // Audit
  if (o.tier_updated_at === undefined) patch.tier_updated_at = null;

  return patch;
}

(async function main() {
  console.log('Cosmos:', process.env.COSMOS_ENDPOINT);
  console.log('DB:    ', process.env.COSMOS_DATABASE || 'cygenix');
  console.log('Mode:  ', DRY_RUN ? 'DRY RUN (no writes)' : 'APPLY (writing changes)');
  console.log('');

  let total = 0, updated = 0, skipped = 0, admins = 0, demos = 0;

  const iter = container.items.query('SELECT * FROM c').getAsyncIterator();
  for await (const page of iter) {
    for (const doc of page.resources) {
      total++;
      const patch = plan(doc);
      const changed = Object.keys(patch).length > 0;

      const role = doc.role || '(none)';
      if (doc.role === 'admin') admins++;
      if (doc.role === 'demo')  demos++;

      if (!changed) {
        skipped++;
        console.log(`  - ${doc.id}  [role:${role}]  already migrated`);
        continue;
      }

      console.log(`  ✓ ${doc.id}  [role:${role}]  →`, JSON.stringify(patch));

      if (APPLY) {
        const merged = { ...doc, ...patch, updatedAt: new Date().toISOString() };
        try {
          await container.items.upsert(merged);
          updated++;
        } catch (e) {
          console.error(`    !! upsert failed for ${doc.id}: ${e.message}`);
        }
      } else {
        updated++;  // counted as "would update" in dry run
      }
    }
  }

  console.log('');
  console.log('─────────────────────────────────────');
  console.log(`Total docs:        ${total}`);
  console.log(`  admins:          ${admins}`);
  console.log(`  demos:           ${demos}`);
  console.log(`  regular users:   ${total - admins - demos}`);
  console.log(`Already migrated:  ${skipped}`);
  console.log(DRY_RUN ? `Would update:      ${updated}` : `Updated:           ${updated}`);
  console.log('─────────────────────────────────────');
  if (DRY_RUN) console.log('\nThis was a DRY RUN. Re-run with --apply to make these changes.');
})().catch(e => {
  console.error('FATAL:', e);
  process.exit(1);
});
