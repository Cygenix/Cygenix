// tests/grant-role.test.js — the out-of-band role grant.
//
// WHY THE SCRIPT EXISTS
// Roles are handed out on the Users & Roles page, and only an Organisation
// Owner or Platform Administrator may hand them out. The first person ever
// to sign in is given OW, PA, ML and EN automatically so a tenant cannot
// lock itself out — but that bootstrap fires under one condition only: the
// stored user list is empty AND the person signing in has no record. Once
// there are users and no administrator among them, nothing inside the
// product can grant anything. scripts/grant-role.js is the way back.
//
// WHY IT IS TESTED AT ALL
// It writes role assignments. A tool that silently wrote the wrong shape,
// or overwrote the assignment list instead of appending to it, would take
// everyone else's roles away while appearing to succeed — on the one day
// somebody is already locked out and least able to check.
//
// HOW
// The script is run as a real child process with an in-memory store
// preloaded in front of it (tests/fixtures/grant-role-stub.js). rbac.js is
// NOT stubbed: the role codes and the conflicting-pair rules are part of
// what is under test.
'use strict';

const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));
const ROOT = path.join(__dirname, '..');
const SCRIPT = path.join(ROOT, 'scripts', 'grant-role.js');
const STUB = path.join(__dirname, 'fixtures', 'grant-role-stub.js');

const OID_A = 'oid-admin-1';
const OID_B = 'oid-other-2';
const SEED = {
  users: {
    [OID_A]: { email: 'admin@cygenix.onmicrosoft.com', name: 'An Admin', isActive: true, firstSeenAt: '2026-09-01T00:00:00.000Z' },
    [OID_B]: { email: 'someone@example.test', name: 'Someone', isActive: true, firstSeenAt: '2026-09-02T00:00:00.000Z' },
  },
  assignments: [
    { id: 'ra_old', oid: OID_B, role: 'EN', scope: 'organisation', grantedBy: 'bootstrap', grantedAt: '2026-09-02T00:00:00.000Z', revokedAt: null },
  ],
};

// Run the script with a seeded in-memory store. Returns stdout, the exit
// code, and whatever state the stub was left holding.
function run(args, seed) {
  const r = spawnSync(process.execPath, ['-r', STUB, SCRIPT].concat(args), {
    cwd: ROOT,
    env: Object.assign({}, process.env, {
      NETLIFY_SITE_ID: 'site-test',
      NETLIFY_API_TOKEN: 'token-test',
      GRANT_ROLE_SEED: JSON.stringify(seed || SEED),
    }),
    encoding: 'utf8',
  });
  const out = (r.stdout || '') + (r.stderr || '');
  const m = out.match(/__RESULT__(\{[\s\S]*?\})\n/);
  return { out, code: r.status, state: m ? JSON.parse(m[1]) : null };
}

console.log('Out-of-band role grant — the way back from a locked-out organisation\n');

/* ── 1. It refuses before it acts ────────────────────────────────────────── */
section('1. What it refuses');
{
  const noCreds = spawnSync(process.execPath, [SCRIPT, '--email', 'a@b.test', '--role', 'PA'], {
    cwd: ROOT, encoding: 'utf8',
    env: Object.assign({}, process.env, { NETLIFY_SITE_ID: '', NETLIFY_API_TOKEN: '' }),
  });
  const credOut = (noCreds.stdout || '') + (noCreds.stderr || '');
  check('without credentials it explains which two are missing and where to find them',
    /NETLIFY_SITE_ID and NETLIFY_API_TOKEN/.test(credOut) && /Site configuration/.test(credOut));
  check('and it names no default site, so it cannot reach the wrong one',
    !/cygenix\.co\.uk|site_id\s*=|[0-9a-f]{8}-[0-9a-f]{4}/.test(credOut));

  const bad = run(['--email', 'admin@cygenix.onmicrosoft.com', '--role', 'ADMIN']);
  check('an unknown role code is refused, and the valid ones are listed',
    bad.code === 1 && /Unknown role "ADMIN"/.test(bad.out) && /OW, PA, ML/.test(bad.out), bad.out.slice(0, 200));
  check('nothing was written for it', !bad.state || !bad.state.wroteKeys.length);

  const missing = run(['--email', 'nobody@example.test', '--role', 'PA', '--apply']);
  check('AN ADDRESS NOBODY HAS SIGNED IN WITH IS REFUSED — a role cannot hang off a user that does not exist',
    missing.code === 1 && /must sign in to Cygenix at least once/.test(missing.out));
  check('and that refusal writes nothing, even with --apply',
    !missing.state || !missing.state.wroteKeys.length, JSON.stringify(missing.state));
}

/* ── 2. The dry run ──────────────────────────────────────────────────────── */
section('2. The dry run is the default');
{
  const dry = run(['--email', 'admin@cygenix.onmicrosoft.com', '--role', 'PA']);
  check('it succeeds and shows the record it would write',
    dry.code === 0 && /"role": "PA"/.test(dry.out) && /"scope": "organisation"/.test(dry.out), dry.out.slice(-400));
  check('IT WRITES NOTHING WITHOUT --apply', dry.state && dry.state.wroteKeys.length === 0, JSON.stringify(dry.state));
  check('and says so in as many words', /DRY RUN — nothing was written/.test(dry.out));
  check('it names the person and their current roles before the change',
    /admin@cygenix\.onmicrosoft\.com/.test(dry.out) && /Roles\s+\(none\)/.test(dry.out));
  check('the grant is attributed to the script, not to a person who did not do it',
    /"grantedBy": "scripts\/grant-role\.js"/.test(dry.out));
  check('no audit entry is written on a dry run either', dry.state && dry.state.audits.length === 0);
}

/* ── 3. The grant ────────────────────────────────────────────────────────── */
section('3. Applying it');
{
  const go = run(['--email', 'admin@cygenix.onmicrosoft.com', '--role', 'PA', '--apply']);
  check('it succeeds', go.code === 0 && /Granted\./.test(go.out), go.out.slice(-300));
  check('exactly one key is written, and it is the assignments key',
    go.state.wroteKeys.length === 1 && go.state.wroteKeys[0] === 'rbac/assignments', JSON.stringify(go.state.wroteKeys));
  check('THE USER RECORDS ARE NEVER TOUCHED', go.state.wroteKeys.indexOf('rbac/users') === -1);

  const added = go.state.assignments.filter((a) => a.oid === OID_A && a.role === 'PA');
  check('the new assignment is there, once', added.length === 1, JSON.stringify(added));
  const rec = added[0];
  check('it carries the same shape the Users & Roles page writes',
    rec.id && /^ra_\d+$/.test(rec.id) && rec.scope === 'organisation'
    && rec.revokedAt === null && /^\d{4}-\d\d-\d\dT.*Z$/.test(rec.grantedAt), JSON.stringify(rec));
  check('and says plainly where it came from', rec.grantedBy === 'scripts/grant-role.js');

  check('THE EXISTING ASSIGNMENT IS STILL THERE — it appends, it does not replace the list',
    go.state.assignments.some((a) => a.oid === OID_B && a.role === 'EN' && !a.revokedAt),
    JSON.stringify(go.state.assignments));
  check('nobody else gained or lost anything', go.state.assignments.length === 2);
  check('the cached role state is invalidated, so the next request re-reads it', go.state.invalidated >= 1);

  const ev = go.state.audits[0];
  check('the grant is recorded in the audit chain', go.state.audits.length === 1 && !!ev);
  check('under role.assign, in the always-on access category, at high severity',
    ev.action === 'role.assign' && ev.category === 'access' && ev.severity === 'high', JSON.stringify(ev));
  check('naming the user, the role and the reason it happened out of band',
    ev.resourceId === OID_A && ev.detail.role === 'PA'
    && ev.detail.user === 'admin@cygenix.onmicrosoft.com'
    && /no account held OW or PA/.test(ev.detail.why), JSON.stringify(ev.detail));
  check('and recording the roles before and after, so the change is readable without a second source',
    Array.isArray(ev.detail.rolesBefore) && ev.detail.rolesBefore.length === 0
    && ev.detail.rolesAfter.join() === 'PA', JSON.stringify(ev.detail));
  check('it tells the reader to sign in again', /sign out and back in/.test(go.out));

  // A backup folder is written beside the working directory before the write.
  const backups = fs.readdirSync(ROOT).filter((f) => /^rbac-backup-/.test(f));
  check('a backup of both keys is written before the change',
    backups.length >= 1
    && fs.existsSync(path.join(ROOT, backups[0], 'rbac-users.json'))
    && fs.existsSync(path.join(ROOT, backups[0], 'rbac-assignments.json')), backups.join(', '));
  if (backups.length) {
    const saved = JSON.parse(fs.readFileSync(path.join(ROOT, backups[0], 'rbac-assignments.json'), 'utf8'));
    check('and the backup holds the state as it was BEFORE the grant, which is what makes it a backup',
      saved.assignments.length === 1 && saved.assignments[0].oid === OID_B, JSON.stringify(saved));
  }
  backups.forEach((f) => fs.rmSync(path.join(ROOT, f), { recursive: true, force: true }));
}

/* ── 4. Running it twice ─────────────────────────────────────────────────── */
section('4. Running it again');
{
  const already = Object.assign({}, SEED, {
    assignments: SEED.assignments.concat([
      { id: 'ra_have', oid: OID_A, role: 'PA', scope: 'organisation', grantedBy: 'x', grantedAt: '2026-09-10T00:00:00.000Z', revokedAt: null },
    ]),
  });
  const again = run(['--email', 'admin@cygenix.onmicrosoft.com', '--role', 'PA', '--apply'], already);
  check('a role the account already holds is a no-op that succeeds',
    again.code === 0 && /Already granted and not revoked/.test(again.out));
  check('AND WRITES NOTHING — re-running is safe', again.state.wroteKeys.length === 0 && again.state.audits.length === 0,
    JSON.stringify(again.state));

  // A revoked assignment must not count as held, or a revoked role could
  // never be granted again.
  const revoked = Object.assign({}, SEED, {
    assignments: SEED.assignments.concat([
      { id: 'ra_rev', oid: OID_A, role: 'PA', scope: 'organisation', grantedBy: 'x', grantedAt: '2026-09-10T00:00:00.000Z', revokedAt: '2026-09-11T00:00:00.000Z' },
    ]),
  });
  const regrant = run(['--email', 'admin@cygenix.onmicrosoft.com', '--role', 'PA', '--apply'], revoked);
  check('a previously REVOKED role can be granted again', regrant.code === 0 && /Granted\./.test(regrant.out));
  check('and the revoked record is left as history rather than edited',
    regrant.state.assignments.filter((a) => a.oid === OID_A && a.role === 'PA').length === 2,
    JSON.stringify(regrant.state.assignments));
  fs.readdirSync(ROOT).filter((f) => /^rbac-backup-/.test(f))
    .forEach((f) => fs.rmSync(path.join(ROOT, f), { recursive: true, force: true }));
}

/* ── 5. When something breaks ────────────────────────────────────────────── */
section('5. When something breaks');
{
  const wf = run(['--email', 'admin@cygenix.onmicrosoft.com', '--role', 'PA', '--apply'],
    Object.assign({}, SEED, { failWrite: true }));
  check('a failed write is reported and the run stops',
    wf.code === 1 && /The write failed and nothing changed/.test(wf.out), wf.out.slice(-250));
  check('and it points at the backup as the state to trust', /backup above is the state as it was/.test(wf.out));

  const af = run(['--email', 'admin@cygenix.onmicrosoft.com', '--role', 'PA', '--apply'],
    Object.assign({}, SEED, { failAudit: true }));
  check('A FAILED AUDIT ENTRY DOES NOT UNDO THE GRANT — silently rolling back a role somebody now relies on is worse',
    af.code === 0 && /The role WAS granted, but the audit entry failed/.test(af.out), af.out.slice(-300));
  check('and the reader is told to declare the gap rather than leave it unexplained',
    /Say so in any evidence pack/.test(af.out));
  fs.readdirSync(ROOT).filter((f) => /^rbac-backup-/.test(f))
    .forEach((f) => fs.rmSync(path.join(ROOT, f), { recursive: true, force: true }));
}

/* ── 6. Listing ──────────────────────────────────────────────────────────── */
section('6. Looking before touching');
{
  const list = run(['--list']);
  check('--list shows every user, their oid and their roles',
    list.code === 0 && /admin@cygenix\.onmicrosoft\.com/.test(list.out)
    && new RegExp(OID_A).test(list.out) && /someone@example\.test/.test(list.out), list.out.slice(0, 400));
  check('it writes nothing', list.state.wroteKeys.length === 0 && list.state.audits.length === 0);
  check('and says outright that nobody can grant roles, which is the diagnosis',
    /NOBODY holds OW or PA/.test(list.out));

  const withAdmin = Object.assign({}, SEED, {
    assignments: SEED.assignments.concat([
      { id: 'ra_pa', oid: OID_B, role: 'PA', scope: 'organisation', grantedBy: 'bootstrap', grantedAt: '2026-09-02T00:00:00.000Z', revokedAt: null },
    ]),
  });
  const listed = run(['--list'], withAdmin);
  check('when somebody CAN grant roles it names them and says to use the product instead',
    /Someone can already grant roles: someone@example\.test/.test(listed.out)
    && /Prefer the Users & Roles page/.test(listed.out), listed.out.slice(-300));

  const empty = run(['--list'], { users: {}, assignments: [] });
  check('an empty store is explained as the bootstrap case, not as a fault',
    /NO USERS AT ALL/.test(empty.out) && /next person to sign in/.test(empty.out)
    && /OW, PA, ML, EN/.test(empty.out), empty.out.slice(0, 400));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
