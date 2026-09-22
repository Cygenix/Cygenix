#!/usr/bin/env node
/* scripts/grant-role.js — grant a Cygenix role from outside the product.
 *
 * ── WHY THIS EXISTS ───────────────────────────────────────────────────────
 *
 * Role assignments are handed out on the Users & Roles page, and only an
 * Organisation Owner or Platform Administrator may hand them out. The first
 * person ever to sign in is given OW, PA, ML and EN automatically so a
 * tenant cannot lock itself out (org-store.resolveActor).
 *
 * That bootstrap fires exactly once, under one condition: the stored user
 * list is completely empty AND the person signing in has no record yet. If
 * the user records were written and the assignment write did not land, or
 * if the account that needs administering was not the first to sign in, the
 * organisation ends up with users and nobody able to grant anything. There
 * is no way back from that inside the product.
 *
 * This is the way back. It writes the same record the Users & Roles page
 * writes, into the same store, and records the grant in the audit chain
 * through the product's own appendAudit — so a role that appeared out of
 * band is still visible in the trail rather than materialising from nowhere.
 *
 * ── WHAT IT NEEDS ─────────────────────────────────────────────────────────
 *
 * The same two variables the Netlify functions use to reach the store:
 *
 *   NETLIFY_SITE_ID     Netlify → Site configuration → General → Site ID
 *   NETLIFY_API_TOKEN   Netlify → User settings → Applications → new token
 *
 * Run it from the repository root, where node_modules holds @netlify/blobs.
 *
 * ── HOW TO RUN IT ─────────────────────────────────────────────────────────
 *
 *   # See who exists and what they hold. Changes nothing.
 *   node scripts/grant-role.js --list
 *
 *   # Dry run: says exactly what it would write.
 *   node scripts/grant-role.js --email admin@cygenix.onmicrosoft.com --role PA
 *
 *   # Do it.
 *   node scripts/grant-role.js --email admin@cygenix.onmicrosoft.com --role PA --apply
 *
 * A dry run is the default. --apply backs up rbac/users and rbac/assignments
 * to a timestamped folder BEFORE writing, and aborts if that backup cannot
 * be written. Re-running after a successful grant does nothing and says so.
 *
 * ── WHAT IT WILL NOT DO ───────────────────────────────────────────────────
 *
 * It will not invent a user. A person must have signed in at least once, so
 * that Entra has issued them an object id and the product has a record to
 * attach the role to. Use --oid to name one explicitly if the address in
 * the store differs from the one you expect.
 *
 * It will not revoke anything, deactivate anyone, or touch any store but
 * the two role keys and the audit chain.
 */
'use strict';

const fs = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, '..');
let org, rbac;
try {
  org = require(path.join(ROOT, 'netlify', 'functions', 'lib', 'org-store.js'));
  rbac = require(path.join(ROOT, 'netlify', 'functions', 'lib', 'rbac.js'));
} catch (e) {
  console.error('Could not load the product modules: ' + e.message);
  console.error('Run this from the repository root, after npm install.');
  process.exit(1);
}

/* ── Arguments ──────────────────────────────────────────────────────────── */
function arg(name) {
  const i = process.argv.indexOf('--' + name);
  return i !== -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--')
    ? process.argv[i + 1] : null;
}
const has = (name) => process.argv.includes('--' + name);

const LIST = has('list');
const APPLY = has('apply');
const EMAIL = (arg('email') || '').trim().toLowerCase();
const OID = (arg('oid') || '').trim();
const ROLE = (arg('role') || '').trim().toUpperCase();

const BACKUP_DIR = path.join(process.cwd(),
  'rbac-backup-' + new Date().toISOString().replace(/[:.]/g, '-'));

function usage(msg) {
  if (msg) console.error('\n' + msg + '\n');
  console.error('Usage:');
  console.error('  node scripts/grant-role.js --list');
  console.error('  node scripts/grant-role.js --email <address> --role <CODE> [--apply]');
  console.error('  node scripts/grant-role.js --oid <object-id> --role <CODE> [--apply]');
  console.error('\nRole codes: ' + rbac.ROLE_CODES.join(', '));
  rbac.ROLE_CODES.forEach((c) => {
    console.error('  ' + c.padEnd(3) + rbac.ROLES[c].name);
  });
  process.exit(msg ? 1 : 0);
}

(async () => {
  if (!process.env.NETLIFY_SITE_ID || !process.env.NETLIFY_API_TOKEN) {
    usage('Set NETLIFY_SITE_ID and NETLIFY_API_TOKEN first.\n'
      + '  Site ID: Netlify → Site configuration → General\n'
      + '  Token:   Netlify → User settings → Applications → Personal access tokens');
  }

  let store;
  try { store = org.orgStore(); }
  catch (e) { console.error('Could not open the authorisation store: ' + e.message); process.exit(1); }

  const state = await org.loadAll(store);
  const users = state.users || {};
  const assignments = (state.assignments || []).slice();

  const rolesOf = (oid) => assignments
    .filter((a) => a.oid === oid && !a.revokedAt).map((a) => a.role);

  /* ── --list ───────────────────────────────────────────────────────────── */
  const entries = Object.entries(users);
  if (LIST || (!EMAIL && !OID)) {
    console.log('\nStore: cygenix-org   keys: rbac/users, rbac/assignments\n');
    if (!entries.length) {
      console.log('NO USERS AT ALL. Nobody has signed in, or the store was cleared.');
      console.log('In that state the product bootstraps itself: the next person to sign in');
      console.log('is given ' + rbac.BOOTSTRAP_ROLES.join(', ') + ' automatically. Sign in and check again');
      console.log('before using this script.\n');
      process.exit(0);
    }
    console.log(entries.length + ' user(s):\n');
    entries.forEach(([oid, u]) => {
      const rs = rolesOf(oid);
      console.log('  ' + (u.email || '(no address)'));
      console.log('    oid      ' + oid);
      console.log('    roles    ' + (rs.length ? rs.join(', ') : '(none)'));
      console.log('    active   ' + (u.isActive !== false) + '    first seen ' + (u.firstSeenAt || '?'));
      console.log('');
    });
    const admins = entries.filter(([oid]) => {
      const rs = rolesOf(oid);
      return rs.includes('OW') || rs.includes('PA');
    });
    console.log(admins.length
      ? 'Someone can already grant roles: ' + admins.map(([, u]) => u.email).join(', ')
        + '\nPrefer the Users & Roles page over this script.'
      : 'NOBODY holds OW or PA. That is the locked-out state this script exists for.');
    console.log('');
    if (!LIST) usage();
    process.exit(0);
  }

  /* ── Validate the request ─────────────────────────────────────────────── */
  if (!ROLE) usage('Name a role with --role.');
  if (!rbac.ROLE_CODES.includes(ROLE)) {
    usage('Unknown role "' + ROLE + '". Valid codes: ' + rbac.ROLE_CODES.join(', '));
  }

  let oid = OID;
  if (!oid) {
    const hit = entries.filter(([, u]) => String(u.email || '').toLowerCase() === EMAIL);
    if (!hit.length) {
      console.error('\nNo user with the address ' + EMAIL + '.');
      console.error('They must sign in to Cygenix at least once before a role can be attached.');
      console.error('Run with --list to see who is on record.\n');
      process.exit(1);
    }
    if (hit.length > 1) {
      console.error('\nMore than one record has that address. Name one with --oid:');
      hit.forEach(([o]) => console.error('  ' + o));
      process.exit(1);
    }
    oid = hit[0][0];
  }
  const user = users[oid];
  if (!user) {
    console.error('\nNo user record for oid ' + oid + '. Run with --list to see who is on record.\n');
    process.exit(1);
  }

  const before = rolesOf(oid);
  console.log('\nUser   ' + (user.email || '(no address)'));
  console.log('oid    ' + oid);
  console.log('Roles  ' + (before.length ? before.join(', ') : '(none)'));
  console.log('Grant  ' + ROLE + '  (' + rbac.ROLES[ROLE].name + ')\n');

  if (before.includes(ROLE)) {
    console.log('Already granted and not revoked. Nothing to do.\n');
    process.exit(0);
  }

  const now = new Date().toISOString();
  // The same shape rbac-admin.js writes on an assign, so a record made here
  // is indistinguishable from one made on the Users & Roles page — except
  // for grantedBy, which says plainly where it came from.
  const record = {
    id: 'ra_' + Date.now(),
    oid,
    role: ROLE,
    scope: 'organisation',
    grantedBy: 'scripts/grant-role.js',
    grantedAt: now,
    revokedAt: null,
  };

  console.log('Would append to rbac/assignments:');
  console.log(JSON.stringify(record, null, 2));
  console.log('\nResulting roles: ' + before.concat(ROLE).join(', '));

  // An array of { pair, note } for every conflicting pair the account would
  // then hold. Empty means none. Worth printing: holding both halves of a
  // pair is permitted for a small team, but it is a thing to have decided
  // rather than discovered later in an evidence pack.
  const collapsed = typeof rbac.collapsedSegregation === 'function'
    ? rbac.collapsedSegregation(before.concat(ROLE)) : [];
  if (Array.isArray(collapsed) && collapsed.length) {
    console.log('\nThis account will then hold roles the model keeps apart:');
    collapsed.forEach((c) => console.log('  ' + c.note + '   (' + c.pair + ')'));
    console.log('Permitted, and surfaced in the product rather than hidden.');
  }

  if (!APPLY) {
    console.log('\nDRY RUN — nothing was written. Re-run with --apply to make the change.\n');
    process.exit(0);
  }

  /* ── Back up, then write ──────────────────────────────────────────────── */
  try {
    fs.mkdirSync(BACKUP_DIR, { recursive: true });
    fs.writeFileSync(path.join(BACKUP_DIR, 'rbac-users.json'),
      JSON.stringify({ users }, null, 2));
    fs.writeFileSync(path.join(BACKUP_DIR, 'rbac-assignments.json'),
      JSON.stringify({ assignments }, null, 2));
  } catch (e) {
    console.error('\nCould not write the backup, so nothing was changed: ' + e.message + '\n');
    process.exit(1);
  }
  console.log('\nBacked up both keys to:\n  ' + BACKUP_DIR);

  assignments.push(record);
  try {
    await store.setJSON('rbac/assignments', { assignments });
  } catch (e) {
    console.error('\nThe write failed and nothing changed: ' + e.message);
    console.error('The backup above is the state as it was.\n');
    process.exit(1);
  }
  org.invalidate();
  console.log('Granted.');

  // The trail matters more here than anywhere: a role that appeared out of
  // band must still be visible to whoever reads the log afterwards. Written
  // through the product's own appendAudit so the entry is properly chained.
  // A failure here does not undo the grant — it is reported instead, because
  // silently rolling back a role somebody is now relying on would be worse.
  try {
    await org.appendAudit(store, {
      actorOid: 'system', actorEmail: 'system',
      action: 'role.assign', category: 'access',
      resourceType: 'role_assignment', resourceId: oid,
      outcome: 'allowed', severity: 'high',
      summary: 'Granted ' + ROLE + ' to ' + (user.email || oid) + ' with scripts/grant-role.js',
      detail: {
        role: ROLE, user: user.email || null, grantedBy: 'scripts/grant-role.js',
        why: 'out-of-band grant: no account held OW or PA, so the product could not do it',
        rolesBefore: before, rolesAfter: before.concat(ROLE),
      },
    });
    console.log('Recorded in the audit chain.');
  } catch (e) {
    console.error('The role WAS granted, but the audit entry failed: ' + e.message);
    console.error('Say so in any evidence pack rather than leaving the gap unexplained.');
  }

  console.log('\nTell ' + (user.email || oid) + ' to sign out and back in.');
  console.log('Stored roles are re-read per request, so a reload may be enough,');
  console.log('but a fresh sign-in removes all doubt.\n');
})().catch((e) => {
  console.error('\n' + (e && e.stack ? e.stack : e) + '\n');
  process.exit(1);
});
