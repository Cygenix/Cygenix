// tests/tenancy.test.js — the tenant boundary, the invitation path, and the
// guardrail as a control rather than a dialog box.
//
// Everything here runs against an in-memory stand-in for Netlify Blobs, so
// lib/tenancy.js is exercised for real: the same reads, the same writes, the
// same races through the same code. Nothing about the policy is mocked.

'use strict';

const path = require('path');
const Module = require('module');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

const LIB = path.join(__dirname, '..', 'netlify', 'functions', 'lib');
const rbac = require(path.join(LIB, 'rbac.js'));
const tenancy = require(path.join(LIB, 'tenancy.js'));

function fakeStore() {
  const mem = new Map();
  return {
    get: async (k) => (mem.has(k) ? JSON.parse(mem.get(k)) : null),
    setJSON: async (k, v) => { mem.set(k, JSON.stringify(v)); },
    delete: async (k) => { mem.delete(k); },
    list: async () => ({ blobs: [...mem.keys()].map(key => ({ key })) }),
    _mem: mem,
  };
}

const actorFor = (oid, email) => ({ oid, email, roles: ['ML', 'EN'] });

(async () => {
  console.log('Tenancy — the boundary, the invitation, the guardrail\n');

  // ── 1. Resolution ───────────────────────────────────────────────────────
  section('1. Tenant resolution');
  {
    tenancy.invalidate();
    const store = fakeStore();
    // A store that already has RBAC users in it: the existing deployment.
    const existingUsers = {
      'oid-a': { email: 'a@acme.test', isActive: true },
      'oid-b': { email: 'b@acme.test', isActive: true },
    };
    const first = await tenancy.resolveTenant(store, actorFor('oid-a', 'a@acme.test'), existingUsers);
    check('the first signer creates a tenant', first.created === true && !!first.tenant.id);
    check('and it is marked as inherited from the implicit organisation',
      first.tenant.migratedFrom === 'implicit-org');
    check('every signer the RBAC directory already knew is enrolled in it',
      first.migratedMembers === true &&
      (await tenancy.loadTenancy(store)).members.filter(m => m.tenantId === first.tenant.id).length === 2,
      JSON.stringify((await tenancy.loadTenancy(store)).members));
    check('a migrated tenant starts with guardrails OFF, so a live deployment does not break',
      first.tenant.guardrails.mode === 'off' && first.tenant.guardrails.twoPersonRule === false);

    tenancy.invalidate();
    const again = await tenancy.resolveTenant(store, actorFor('oid-a', 'a@acme.test'), existingUsers);
    check('resolving the same signer again returns the same tenant, not a new one',
      again.created === false && again.tenant.id === first.tenant.id);

    tenancy.invalidate();
    const b = await tenancy.resolveTenant(store, actorFor('oid-b', 'b@acme.test'), existingUsers);
    check('a signer enrolled by the migration lands in that tenant',
      b.created === false && b.tenant.id === first.tenant.id);

    // The hole this deliberately does not have.
    tenancy.invalidate();
    const stranger = await tenancy.resolveTenant(store, actorFor('oid-z', 'z@acme.test'), existingUsers);
    check('a stranger sharing the email domain does NOT join the tenant',
      stranger.tenant.id !== first.tenant.id && stranger.created === true);
    check('they get their own boundary instead of an error or a shared one',
      (await tenancy.loadTenancy(store)).members.filter(m => m.tenantId === stranger.tenant.id).length === 1);
    check('and a tenant created after the migration starts WITH guardrails on',
      stranger.tenant.guardrails.mode === 'confirm-destructive');
  }

  // ── 2. Invitations ──────────────────────────────────────────────────────
  section('2. Invitations are the only way in');
  {
    tenancy.invalidate();
    const store = fakeStore();
    const owner = await tenancy.resolveTenant(store, actorFor('oid-own', 'own@acme.test'), null);
    const tid = owner.tenant.id;

    const bad = tenancy.validateInvite({ email: 'nope', roles: ['EN'], actorRoles: ['OW'] });
    check('an invitation to a malformed address is refused', bad.ok === false);

    const noRole = tenancy.validateInvite({ email: 'x@acme.test', roles: [], actorRoles: ['OW'] });
    check('an invitation naming no role is refused', noRole.ok === false);

    const asEngineer = tenancy.validateInvite({ email: 'x@acme.test', roles: ['EN'], actorRoles: ['EN'] });
    check('an Engineer cannot invite — assigning roles is not theirs to do', asEngineer.ok === false);

    const adminByPa = tenancy.validateInvite({ email: 'x@acme.test', roles: ['PA'], actorRoles: ['PA'] });
    check('a Platform Administrator cannot appoint another administrator by invitation',
      adminByPa.ok === false && /Organisation Owner/.test(adminByPa.reason), adminByPa.reason);

    const adminByOw = tenancy.validateInvite({ email: 'x@acme.test', roles: ['PA'], actorRoles: ['OW'] });
    check('the Organisation Owner can', adminByOw.ok === true);

    const auto = tenancy.validateInvite({ email: 'x@acme.test', roles: ['SP'], actorRoles: ['OW'] });
    check('the Automation Principal is not invitable — it is not a person', auto.ok === false);

    const good = tenancy.validateInvite({ email: 'New.Person@ACME.test', roles: ['EN'], actorRoles: ['OW'] });
    check('a valid invitation normalises the address', good.ok === true && good.email === 'new.person@acme.test');

    const inv = await tenancy.createInvite(store, { tenantId: tid, email: good.email, roles: ['EN'], invitedBy: 'oid-own' });
    check('the invitation expires — it is not a standing key',
      Date.parse(inv.expiresAt) > Date.now() && Date.parse(inv.expiresAt) - Date.now() <= tenancy.INVITE_TTL_MS + 1000);

    tenancy.invalidate();
    const joined = await tenancy.resolveTenant(store, actorFor('oid-new', 'New.Person@acme.test'), null);
    check('signing in with the invited address joins that tenant, matched case-insensitively',
      joined.joined === true && joined.tenant.id === tid);
    check('and the invitation names the roles the joiner will hold',
      joined.invite && joined.invite.roles[0] === 'EN');

    tenancy.invalidate();
    const state = await tenancy.loadTenancy(store);
    check('a spent invitation cannot be spent twice',
      state.invites.filter(i => i.id === inv.id && i.acceptedAt).length === 1);

    // A revoked invitation admits nobody.
    const inv2 = await tenancy.createInvite(store, { tenantId: tid, email: 'gone@acme.test', roles: ['VA'], invitedBy: 'oid-own' });
    await tenancy.revokeInvite(store, { tenantId: tid, inviteId: inv2.id, revokedBy: 'oid-own' });
    tenancy.invalidate();
    const rejected = await tenancy.resolveTenant(store, actorFor('oid-gone', 'gone@acme.test'), null);
    check('a revoked invitation admits nobody', rejected.joined !== true && rejected.tenant.id !== tid);

    const already = await tenancy.revokeInvite(store, { tenantId: tid, inviteId: inv.id, revokedBy: 'oid-own' });
    check('an accepted invitation cannot be revoked after the fact', already.ok === false);

    // Cross-tenant: revoking somebody else's invitation is a 'no such thing'.
    const other = await tenancy.revokeInvite(store, { tenantId: 'tn_someoneelse', inviteId: inv2.id, revokedBy: 'oid-own' });
    check('an invitation in another tenant is simply not found', other.ok === false && /no such/.test(other.reason));
  }

  // ── 3. Membership removal ───────────────────────────────────────────────
  section('3. Removing a member');
  {
    tenancy.invalidate();
    const store = fakeStore();
    const t = await tenancy.resolveTenant(store, actorFor('oid-own', 'own@acme.test'), { 'oid-own': {}, 'oid-two': { email: 'two@acme.test' } });
    const tid = t.tenant.id;

    const self = await tenancy.removeMember(store, { tenantId: tid, oid: 'oid-own', removedBy: 'oid-own' });
    check('you cannot remove yourself', self.ok === false);

    const lockout = await tenancy.removeMember(store, { tenantId: tid, oid: 'oid-two', removedBy: 'oid-own', remainingAdmins: 0 });
    check('the last administrator cannot be removed — the tenant would be locked out',
      lockout.ok === false && /locked out/.test(lockout.reason));

    const done = await tenancy.removeMember(store, { tenantId: tid, oid: 'oid-two', removedBy: 'oid-own', remainingAdmins: 1 });
    check('otherwise the membership ends', done.ok === true);
    tenancy.invalidate();
    check('and they are no longer a member',
      tenancy.membershipOf(await tenancy.loadTenancy(store), 'oid-two') === null);

    tenancy.invalidate();
    const back = await tenancy.resolveTenant(store, actorFor('oid-two', 'two@acme.test'), null);
    check('a removed member does not silently rejoin the tenant they left',
      back.tenant.id !== tid);
  }

  // ── 4. Project visibility ───────────────────────────────────────────────
  section('4. Sharing is opt-in, and migration does not disclose');
  {
    const mine = { ownerOid: 'me', visibility: 'private' };
    const shared = { ownerOid: 'me', visibility: 'tenant' };
    check('a private project is visible to its owner', tenancy.canSeeProject('me', mine) === true);
    check('and to nobody else — not even an administrator, who holds no data-plane grant',
      tenancy.canSeeProject('someone', mine) === false);
    check('a tenant-visible project is visible to a colleague', tenancy.canSeeProject('someone', shared) === true);
    check('a record with no visibility set defaults to tenant-visible',
      tenancy.visibilityOf({}) === 'tenant');
    check('but migrated work defaults to PRIVATE — tenancy is not licence to disclose',
      tenancy.MIGRATED_VISIBILITY === 'private');
    check('only the owner re-shares', tenancy.canSetVisibility('me', mine) === true &&
      tenancy.canSetVisibility('someone', mine) === false);
    check('the project-key pattern does not match the migration marker, so a listing never sees it',
      tenancy.PROJECT_KEY_RE.test('proj_123') === true &&
      tenancy.PROJECT_KEY_RE.test(tenancy.migrationMarkerKey('oid-a')) === false);
    check('the tenant store name is distinct from the per-user store it replaces',
      tenancy.projectStoreName('tn_abc') !== tenancy.legacyProjectStoreName('tn_abc'));
  }

  // ── 5. Guardrail requirements ───────────────────────────────────────────
  section('5. What the guardrail asks for');
  {
    const off  = { mode: 'off' };
    const dest = { mode: 'confirm-destructive' };
    const all  = { mode: 'confirm-all' };
    const two  = { mode: 'confirm-destructive', twoPersonRule: true };
    const write = { mutating: true, environment: 'PROD', destructive: [] };
    const destructive = { mutating: true, environment: 'PROD', destructive: [{ type: 'DELETE', table: 'orders', bounded: false }] };

    check('a read is never gated', tenancy.requirementFor(all, { mutating: false, environment: 'PROD' }) === null);
    check('mode off gates nothing', tenancy.requirementFor(off, destructive) === null);
    check('confirm-destructive lets an ordinary write through', tenancy.requirementFor(dest, write) === null);
    check('and stops a DELETE', tenancy.requirementFor(dest, destructive) === 'confirm');
    check('confirm-all stops every write', tenancy.requirementFor(all, write) === 'confirm');
    check('a DEV target is not gated', tenancy.requirementFor(all, { ...write, environment: 'DEV' }) === null);
    check('an unclassified target counts as PROD and IS gated',
      tenancy.requirementFor(all, { mutating: true, destructive: [] }) === 'confirm');
    check('the two-person rule escalates a confirmation into an approval',
      tenancy.requirementFor(two, destructive) === 'second-person');
    check('but does not gate what the mode does not cover',
      tenancy.requirementFor(two, write) === null);
  }

  // ── 6. The approval binds to one exact act ──────────────────────────────
  section('6. An approval authorises one statement, once');
  {
    const base = { policyAction: 'sql.write', resourceId: 'host|db', environment: 'PROD' };
    const a = tenancy.actHash({ ...base, sql: 'DELETE FROM orders WHERE id = 1' });
    const b = tenancy.actHash({ ...base, sql: 'DELETE FROM orders' });
    check('two different statements hash differently', a !== b);
    check('the same statement hashes the same', a === tenancy.actHash({ ...base, sql: 'DELETE FROM orders WHERE id = 1' }));
    check('the same statement against a different target hashes differently',
      a !== tenancy.actHash({ ...base, resourceId: 'other|db', sql: 'DELETE FROM orders WHERE id = 1' }));
    check('whitespace is NOT normalised away — an edited statement is a different act',
      a !== tenancy.actHash({ ...base, sql: 'DELETE  FROM orders WHERE id = 1' }));
    check('the stored preview carries no string literals out of the SQL',
      !/mysecret/.test(tenancy.previewOf("UPDATE t SET pw = 'mysecret' WHERE id = 1")),
      tenancy.previewOf("UPDATE t SET pw = 'mysecret' WHERE id = 1"));
  }

  section('7. Spending an approval');
  {
    tenancy.invalidate();
    const store = fakeStore();
    const t = await tenancy.resolveTenant(store, actorFor('oid-req', 'req@acme.test'), null);
    const tid = t.tenant.id;
    const act = { policyAction: 'sql.write', resourceId: 'host|db', environment: 'PROD',
                  sql: 'DELETE FROM orders', destructive: [{ type: 'DELETE', table: 'orders', bounded: false }] };
    const hash = tenancy.actHash(act);

    // Confirmation path.
    const conf = await tenancy.requestApproval(store, {
      tenantId: tid, act, requirement: 'confirm', requestedBy: 'oid-req', requestedByEmail: 'req@acme.test' });
    const wrongAct = await tenancy.consumeApproval(store, {
      tenantId: tid, approvalId: conf.id, actHash: tenancy.actHash({ ...act, sql: 'DROP TABLE orders' }), actorOid: 'oid-req' });
    check('an approval cannot be spent on a different statement',
      wrongAct.ok === false && /different statement/.test(wrongAct.reason), wrongAct.reason);

    const wrongTenant = await tenancy.consumeApproval(store, {
      tenantId: 'tn_elsewhere', approvalId: conf.id, actHash: hash, actorOid: 'oid-req' });
    check('nor by another tenant', wrongTenant.ok === false);

    const wrongActor = await tenancy.consumeApproval(store, {
      tenantId: tid, approvalId: conf.id, actHash: hash, actorOid: 'oid-someone' });
    check('nor by somebody other than the person who confirmed it', wrongActor.ok === false);

    const spend = await tenancy.consumeApproval(store, { tenantId: tid, approvalId: conf.id, actHash: hash, actorOid: 'oid-req' });
    check('the right actor with the right act spends it', spend.ok === true);
    const replay = await tenancy.consumeApproval(store, { tenantId: tid, approvalId: conf.id, actHash: hash, actorOid: 'oid-req' });
    check('and it cannot be replayed', replay.ok === false && /already been spent/.test(replay.reason), replay.reason);

    // Two-person path.
    const req = await tenancy.requestApproval(store, {
      tenantId: tid, act, requirement: 'second-person', requestedBy: 'oid-req', requestedByEmail: 'req@acme.test' });
    const unapproved = await tenancy.consumeApproval(store, { tenantId: tid, approvalId: req.id, actHash: hash, actorOid: 'oid-req' });
    check('a two-person request is not usable before anyone approves it',
      unapproved.ok === false && /awaiting approval/.test(unapproved.reason), unapproved.reason);

    const selfApprove = await tenancy.grantApproval(store, {
      tenantId: tid, approvalId: req.id, approverOid: 'oid-req', approverRoles: ['ML', 'AP'] });
    check('the author cannot approve their own change (SoD-1)',
      selfApprove.ok === false && /author of a change/.test(selfApprove.reason), selfApprove.reason);

    const wrongRole = await tenancy.grantApproval(store, {
      tenantId: tid, approvalId: req.id, approverOid: 'oid-eng', approverRoles: ['EN'] });
    check('an Engineer cannot approve — the approve grant is not theirs',
      wrongRole.ok === false && /do not authorise/.test(wrongRole.reason), wrongRole.reason);

    const granted = await tenancy.grantApproval(store, {
      tenantId: tid, approvalId: req.id, approverOid: 'oid-ap', approverEmail: 'ap@acme.test', approverRoles: ['AP'] });
    check('an Approver can', granted.ok === true);

    const runsNow = await tenancy.consumeApproval(store, { tenantId: tid, approvalId: req.id, actHash: hash, actorOid: 'oid-req' });
    check('and the requester may then run it', runsNow.ok === true);

    // SoD-2: the sole approver may not also be the one who executes.
    const req2 = await tenancy.requestApproval(store, {
      tenantId: tid, act, requirement: 'second-person', requestedBy: 'oid-req' });
    await tenancy.grantApproval(store, { tenantId: tid, approvalId: req2.id, approverOid: 'oid-ap', approverRoles: ['AP'] });
    const soleApproverExecutes = await tenancy.consumeApproval(store, {
      tenantId: tid, approvalId: req2.id, actHash: hash, actorOid: 'oid-ap' });
    check('the sole approver of a change cannot be the one to execute it (SoD-2)',
      soleApproverExecutes.ok === false && /sole approver/.test(soleApproverExecutes.reason), soleApproverExecutes.reason);

    // Expiry.
    const stale = await tenancy.requestApproval(store, { tenantId: tid, act, requirement: 'confirm', requestedBy: 'oid-req' });
    const st = await tenancy.loadTenancy(store);
    st.approvals.find(x => x.id === stale.id).expiresAt = new Date(Date.now() - 1000).toISOString();
    const expired = await tenancy.consumeApproval(store, { tenantId: tid, approvalId: stale.id, actHash: hash, actorOid: 'oid-req' });
    check('an expired approval is not an approval', expired.ok === false && /expired/.test(expired.reason), expired.reason);

    const queue = await tenancy.listApprovals(store, { tenantId: tid });
    check('the pending queue lists only live two-person requests',
      queue.every(q => q.requirement === 'second-person' && !q.consumedAt && !q.revokedAt), JSON.stringify(queue.map(q => q.id)));
  }

  // ── 8. The policy module agrees with itself ─────────────────────────────
  section('8. The matrix rows this workflow needed');
  {
    check("run.approve exists and gives the Approver the 'A' grant the model always described",
      rbac.MATRIX['run.approve'] && rbac.MATRIX['run.approve'].AP === 'A');
    check('an approve-only grant still cannot PERFORM the action',
      rbac.can({ roles: ['AP'], isActive: true }, 'run.approve', { mutating: true }).allow === false);
    check('the approver roles named by tenancy all hold a grant on it',
      tenancy.APPROVER_ROLES.every(r => !!rbac.MATRIX['run.approve'][r]), tenancy.APPROVER_ROLES.join(','));
    check('project.write exists, so the console working state is no longer ungated',
      !!rbac.MATRIX['project.write']);
    check('a signer with no role assignment can neither read nor write a project (A-01)',
      rbac.can({ roles: [], isActive: true }, 'project.read', {}).allow === false &&
      rbac.can({ roles: [], isActive: true }, 'project.write', { mutating: true }).allow === false);
    check('a Member reads but does not write',
      rbac.can({ roles: ['MB'], isActive: true }, 'project.read', {}).allow === true &&
      rbac.can({ roles: ['MB'], isActive: true }, 'project.write', { mutating: true }).allow === false);
    check('an Engineer writes',
      rbac.can({ roles: ['EN'], isActive: true }, 'project.write', { mutating: true }).allow === true);
    check('guardrail policy is a control-plane act — the Owner sets it, the Engineer does not',
      rbac.can({ roles: ['OW'], isActive: true }, 'tenant.guardrails', { mutating: true }).allow === true &&
      rbac.can({ roles: ['EN'], isActive: true }, 'tenant.guardrails', { mutating: true }).allow === false);
  }

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
