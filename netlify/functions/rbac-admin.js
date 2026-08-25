// netlify/functions/rbac-admin.js
//
// The control plane the access-control spec found missing: a user
// directory with visible, revocable role assignments (R-03), environment
// classification of connections (R-15), and the server-side audit trail
// (R-30) — read, verified and exported here, written from every gated
// endpoint via lib/org-store.
//
// GET  ?what=me               → caller's user record, effective roles,
//                               collapsed-segregation warnings (SoD-7)
// GET  ?what=users            → directory + assignments   (admin.read)
// GET  ?what=tenant           → tenant, members, open invitations,
//                               guardrail policy            (admin.read)
// GET  ?what=approvals        → the two-person queue        (run.approve)
// GET  ?what=classifications  → connection classifications (connection.view)
// GET  ?what=audit&limit=n    → audit entries (audit.read; delivery roles
//                               see their own entries only)
// GET  ?what=audit&verify=1   → walk the hash chain        (audit.read F)
// GET  ?what=audit&export=1   → full export                (audit.export)
// POST { op:'assign'|'revoke', oid, role }                 (role.assign;
//        OW/PA appointments require the Owner — role.assign-admin)
// POST { op:'deactivate'|'activate', oid }                 (user.manage)
// POST { op:'classify', server, database, environment }    (connection.classify)
// POST { op:'invite', email, roles }                       (user.manage +
//        role.assign, or role.assign-admin for an OW/PA invitation)
// POST { op:'revoke-invite', inviteId }                    (user.manage)
// POST { op:'remove-member', oid }                         (user.manage)
// POST { op:'guardrails', mode, twoPersonRule, environments } (tenant.guardrails)
// POST { op:'approve', approvalId }                        (run.approve)
// POST { op:'withdraw-approval', approvalId }              (requester or approver)
//
// Members join by invitation and are given their roles by it. There is no
// step in any of this that happens in an identity provider's admin console.
//
// Every state-changing call writes an audit entry, and every denial does
// too — a refused act is evidence the control fired (spec Section 10).

'use strict';

const authz = require('./lib/authz');
const rbac = require('./lib/rbac');
const org  = require('./lib/org-store');
const tenancy = require('./lib/tenancy');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type': 'application/json',
};
const ok   = (data)          => ({ statusCode: 200,  headers: CORS, body: JSON.stringify(data) });
const fail = (msg, code=500) => ({ statusCode: code, headers: CORS, body: JSON.stringify({ error: msg }) });

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };

  // The one door. `action: null` because this endpoint gates each op
  // individually below — but it still cannot skip the call, because the
  // actor, the tenant and the audit closure all come out of it.
  let ctx;
  try {
    ctx = await authz.authorize(event, { route: 'rbac-admin', action: null });
  } catch (e) {
    return authz.errorResponse(e, CORS);
  }
  const { store, actor, tenant, audit } = ctx;

  // A denial both refuses and records — the record is the point.
  const denied = async (action, decision, detail) => {
    await audit({ action, outcome: 'denied', severity: decision.severity || 'notice',
                  detail: { reason: decision.reason, ...(detail || {}) } });
    return fail('Not permitted: ' + decision.reason, 403);
  };

  try {
    if (event.httpMethod === 'GET') {
      const q = event.queryStringParameters || {};
      const what = q.what || 'me';

      if (what === 'me') {
        return ok({
          oid: actor.oid, email: actor.email, name: actor.name,
          isActive: actor.isActive, roles: actor.roles,
          roleNames: actor.roles.map(r => rbac.ROLES[r] && rbac.ROLES[r].name).filter(Boolean),
          collapsed: rbac.collapsedSegregation(actor.roles),
          bootstrapped: actor.bootstrapped || false,
          roleRef: rbac.ROLES,   // the Appendix 16 quick reference — not sensitive
        });
      }

      if (what === 'users') {
        const d = rbac.can(actor, 'admin.read', {});
        if (!d.allow) return denied('admin.read', d);
        const state = await org.loadAll(store);
        const users = Object.entries(state.users).map(([oid, u]) => ({
          oid, email: u.email, name: u.name, isActive: u.isActive !== false,
          firstSeenAt: u.firstSeenAt, lastSeenAt: u.lastSeenAt,
          roles: state.assignments.filter(a => a.oid === oid && !a.revokedAt).map(a => a.role),
        }));
        users.forEach(u => { u.collapsed = rbac.collapsedSegregation(u.roles); });
        return ok({ users, roles: rbac.ROLES, readOnly: d.grant === 'R' });
      }

      if (what === 'tenant') {
        const d = rbac.can(actor, 'admin.read', {});
        if (!d.allow) return denied('admin.read', d);
        const state = await tenancy.loadTenancy(store);
        const orgState = await org.loadAll(store);
        const members = tenancy.membersOf(state, tenant.id).map(m => ({
          oid: m.oid, email: m.email || (orgState.users[m.oid] || {}).email || null,
          name: (orgState.users[m.oid] || {}).name || '',
          joinedAt: m.joinedAt, invitedBy: m.invitedBy,
          isActive: (orgState.users[m.oid] || {}).isActive !== false,
          roles: orgState.assignments.filter(a => a.oid === m.oid && !a.revokedAt).map(a => a.role),
        }));
        const invites = state.invites
          .filter(i => i.tenantId === tenant.id && !i.acceptedAt && !i.revokedAt &&
                       Date.parse(i.expiresAt || 0) > Date.now())
          .map(i => ({ id: i.id, email: i.email, roles: i.roles, invitedAt: i.invitedAt, expiresAt: i.expiresAt }));
        return ok({
          tenant: { id: tenant.id, name: tenant.name, createdAt: tenant.createdAt,
                    migratedFrom: tenant.migratedFrom || null },
          members, invites,
          guardrails: tenancy.normaliseGuardrails(tenant.guardrails),
          guardrailModes: tenancy.GUARDRAIL_MODES,
          environments: rbac.ENVIRONMENTS,
          canManageMembers: rbac.can(actor, 'user.manage', { mutating: true }).allow,
          canSetGuardrails: rbac.can(actor, 'tenant.guardrails', { mutating: true }).allow,
          canApprove: tenancy.canApprove(actor.roles),
          roles: rbac.ROLES,
        });
      }

      if (what === 'approvals') {
        // Anyone inside the tenant sees the queue — a requester needs to
        // watch their own request, and a queue only its approvers can see is
        // a queue nobody chases. Acting on one is the gated part.
        const pending = await tenancy.listApprovals(store, { tenantId: tenant.id });
        return ok({
          approvals: pending.map(a => ({
            id: a.id, requirement: a.requirement,
            act: a.act, requestedBy: a.requestedBy, requestedByEmail: a.requestedByEmail,
            requestedAt: a.requestedAt, expiresAt: a.expiresAt,
            approvedBy: a.approvedBy,
            mine: a.requestedBy === actor.oid,
          })),
          canApprove: tenancy.canApprove(actor.roles),
          me: actor.oid,
        });
      }

      if (what === 'classifications') {
        const d = rbac.can(actor, 'connection.view', {});
        if (!d.allow) return denied('connection.view', d);
        const state = await org.loadAll(store);
        return ok({ classifications: state.classifications,
                    environments: rbac.ENVIRONMENTS, defaultEnvironment: rbac.DEFAULT_ENVIRONMENT,
                    canClassify: rbac.can(actor, 'connection.classify', {}).allow });
      }

      if (what === 'audit') {
        const d = rbac.can(actor, 'audit.read', {});
        if (!d.allow) return denied('audit.read', d);
        if (q.export === '1') {
          const de = rbac.can(actor, 'audit.export', {});
          if (!de.allow) return denied('audit.export', de);
          const full = await org.readAudit(store, { limit: 10000 });
          await audit({ action: 'audit.export', outcome: 'allowed', severity: 'notice',
                        detail: { entries: full.entries.length } });
          return ok(full);
        }
        if (q.verify === '1') {
          if (d.selfOnly) return denied('audit.verify', { reason: 'chain verification needs organisation-wide audit read' });
          return ok(await org.verifyChain(store, {}));
        }
        const limit = Math.min(1000, parseInt(q.limit, 10) || 200);
        return ok(await org.readAudit(store, { limit, selfOid: d.selfOnly ? actor.oid : null }));
      }

      return fail('Unknown query: ' + what, 400);
    }

    if (event.httpMethod === 'POST') {
      let body = {};
      try { body = JSON.parse(event.body || '{}'); } catch {}
      const op = body.op;

      if (op === 'assign' || op === 'revoke') {
        const state = await org.loadAll(store);
        // Lockout guard input: active admins after this change would apply.
        const admins = new Set(state.assignments
          .filter(a => !a.revokedAt && (a.role === 'OW' || a.role === 'PA'))
          .filter(a => (state.users[a.oid] || {}).isActive !== false)
          .map(a => a.oid));
        const remainingAdmins = op === 'revoke' && (body.role === 'OW' || body.role === 'PA')
          ? [...admins].filter(oid => oid !== body.oid ||
              state.assignments.some(a => a.oid === oid && !a.revokedAt &&
                (a.role === 'OW' || a.role === 'PA') && a.role !== body.role)).length
          : admins.size;
        const v = rbac.validateAssignmentChange({
          actorRoles: actor.roles, op, targetRole: body.role, remainingAdmins,
        });
        if (!v.ok) return denied('role.' + op, { reason: v.reason }, { oid: body.oid, role: body.role });
        if (!state.users[body.oid]) return fail('Unknown user', 404);

        const now = new Date().toISOString();
        if (op === 'assign') {
          if (!state.assignments.some(a => a.oid === body.oid && a.role === body.role && !a.revokedAt)) {
            state.assignments.push({ id: 'ra_' + Date.now(), oid: body.oid, role: body.role,
              scope: 'organisation', grantedBy: actor.oid, grantedAt: now, revokedAt: null });
          }
        } else {
          state.assignments.forEach(a => {
            if (a.oid === body.oid && a.role === body.role && !a.revokedAt) a.revokedAt = now;
          });
        }
        await store.setJSON('rbac/assignments', { assignments: state.assignments });
        org.invalidate();
        await audit({ action: 'role.' + op, outcome: 'allowed', severity: 'notice',
                      resourceType: 'role_assignment', resourceId: body.oid,
                      detail: { role: body.role, user: (state.users[body.oid] || {}).email } });
        return ok({ done: true });
      }

      if (op === 'deactivate' || op === 'activate') {
        const d = rbac.can(actor, 'user.manage', { mutating: true });
        if (!d.allow) return denied('user.manage', d, { oid: body.oid });
        if (body.oid === actor.oid && op === 'deactivate') return fail('Refusing to deactivate yourself', 400);
        const state = await org.loadAll(store);
        const u = state.users[body.oid];
        if (!u) return fail('Unknown user', 404);
        u.isActive = op === 'activate';
        await store.setJSON('rbac/users', { users: state.users });
        org.invalidate();
        await audit({ action: 'user.' + op, outcome: 'allowed', severity: 'notice',
                      resourceType: 'app_user', resourceId: body.oid, detail: { user: u.email } });
        return ok({ done: true });
      }

      if (op === 'classify') {
        const d = rbac.can(actor, 'connection.classify', { mutating: true });
        if (!d.allow) return denied('connection.classify', d, { server: body.server, database: body.database });
        if (!rbac.ENVIRONMENTS.includes(body.environment)) return fail('environment must be one of ' + rbac.ENVIRONMENTS.join('|'), 400);
        if (!body.server) return fail('server is required', 400);
        const state = await org.loadAll(store);
        const key = rbac.connKey(body.server, body.database);
        state.classifications[key] = {
          environment: body.environment, classifiedBy: actor.oid,
          classifiedAt: new Date().toISOString(),
          server: String(body.server).trim(), database: String(body.database || '').trim(),
        };
        await store.setJSON('rbac/classifications', { byKey: state.classifications });
        org.invalidate();
        await audit({ action: 'connection.classify', outcome: 'allowed',
                      severity: body.environment === 'PROD' ? 'high' : 'notice',
                      resourceType: 'connection', resourceId: key,
                      detail: { environment: body.environment } });
        return ok({ done: true, key });
      }

      // ── Tenancy ──────────────────────────────────────────────────────────

      if (op === 'invite') {
        const d = rbac.can(actor, 'user.manage', { mutating: true });
        if (!d.allow) return denied('user.manage', d, { email: body.email });
        const state = await tenancy.loadTenancy(store);
        const orgState = await org.loadAll(store);
        const existing = tenancy.membersOf(state, tenant.id)
          .map(m => m.email || (orgState.users[m.oid] || {}).email)
          .filter(Boolean);
        const v = tenancy.validateInvite({
          email: body.email, roles: body.roles,
          actorRoles: actor.roles, existingMemberEmails: existing,
        });
        if (!v.ok) return denied('tenant.invite', { reason: v.reason }, { email: body.email, roles: body.roles });
        const invite = await tenancy.createInvite(store, {
          tenantId: tenant.id, email: v.email, roles: v.roles, invitedBy: actor.oid,
        });
        await audit({ action: 'tenant.invite', outcome: 'allowed', severity: 'notice',
                      resourceType: 'invitation', resourceId: invite.id,
                      detail: { email: invite.email, roles: invite.roles } });
        // The invitation is redeemed by signing in with that address — there
        // is no link to leak and no token to steal.
        return ok({ done: true, invite: { id: invite.id, email: invite.email, roles: invite.roles, expiresAt: invite.expiresAt } });
      }

      if (op === 'revoke-invite') {
        const d = rbac.can(actor, 'user.manage', { mutating: true });
        if (!d.allow) return denied('user.manage', d, { inviteId: body.inviteId });
        const r = await tenancy.revokeInvite(store, { tenantId: tenant.id, inviteId: body.inviteId, revokedBy: actor.oid });
        if (!r.ok) return fail(r.reason, 400);
        await audit({ action: 'tenant.invite-revoke', outcome: 'allowed', severity: 'notice',
                      resourceType: 'invitation', resourceId: body.inviteId,
                      detail: { email: r.invite.email } });
        return ok({ done: true });
      }

      if (op === 'remove-member') {
        const d = rbac.can(actor, 'user.manage', { mutating: true });
        if (!d.allow) return denied('user.manage', d, { oid: body.oid });
        const state = await tenancy.loadTenancy(store);
        const orgState = await org.loadAll(store);
        // Would this leave the tenant without an administrator?
        const admins = tenancy.membersOf(state, tenant.id).filter(m =>
          m.oid !== body.oid &&
          (orgState.users[m.oid] || {}).isActive !== false &&
          orgState.assignments.some(a => a.oid === m.oid && !a.revokedAt && (a.role === 'OW' || a.role === 'PA')));
        const r = await tenancy.removeMember(store, {
          tenantId: tenant.id, oid: body.oid, removedBy: actor.oid, remainingAdmins: admins.length,
        });
        if (!r.ok) return denied('tenant.remove-member', { reason: r.reason }, { oid: body.oid });
        // Membership ends and every role goes with it. A revoked assignment
        // takes effect on the next request (R-04), not the next sign-in.
        let revoked = 0;
        orgState.assignments.forEach(a => {
          if (a.oid === body.oid && !a.revokedAt) { a.revokedAt = new Date().toISOString(); revoked++; }
        });
        if (revoked) {
          await store.setJSON('rbac/assignments', { assignments: orgState.assignments });
          org.invalidate();
        }
        await audit({ action: 'tenant.remove-member', outcome: 'allowed', severity: 'high',
                      resourceType: 'membership', resourceId: body.oid,
                      detail: { rolesRevoked: revoked, email: (orgState.users[body.oid] || {}).email } });
        return ok({ done: true, rolesRevoked: revoked });
      }

      if (op === 'guardrails') {
        const d = rbac.can(actor, 'tenant.guardrails', { mutating: true });
        if (!d.allow) return denied('tenant.guardrails', d, { mode: body.mode });
        if (body.mode && !tenancy.GUARDRAIL_MODES.includes(body.mode)) {
          return fail('mode must be one of ' + tenancy.GUARDRAIL_MODES.join('|'), 400);
        }
        const before = tenancy.normaliseGuardrails(tenant.guardrails);
        const r = await tenancy.setGuardrails(store, {
          tenantId: tenant.id,
          guardrails: {
            mode: body.mode || before.mode,
            twoPersonRule: body.twoPersonRule === undefined ? before.twoPersonRule : body.twoPersonRule === true,
            environments: Array.isArray(body.environments) ? body.environments : before.environments,
          },
        });
        if (!r.ok) return fail(r.reason, 400);
        await audit({ action: 'tenant.guardrails', outcome: 'allowed', severity: 'high',
                      resourceType: 'tenant', resourceId: tenant.id,
                      detail: { from: before, to: r.guardrails } });
        return ok({ done: true, guardrails: r.guardrails });
      }

      if (op === 'approve') {
        // The approve-only grant, doing the one thing it is for. can() is
        // asked and expected to refuse — an 'A' grant may not PERFORM the
        // act — so the check here is the approver-role test, and the SoD-1
        // rule inside grantApproval stops an author approving their own.
        const r = await tenancy.grantApproval(store, {
          tenantId: tenant.id, approvalId: body.approvalId,
          approverOid: actor.oid, approverEmail: actor.email, approverRoles: actor.roles,
        });
        if (!r.ok) return denied('run.approve', { reason: r.reason }, { approvalId: body.approvalId });
        await audit({ action: 'run.approve', outcome: 'allowed', severity: 'high',
                      resourceType: 'approval', resourceId: r.approval.id,
                      environment: r.approval.act && r.approval.act.environment,
                      detail: { requestedBy: r.approval.requestedBy,
                                policyAction: r.approval.act && r.approval.act.policyAction,
                                target: r.approval.act && r.approval.act.resourceId,
                                preview: r.approval.act && r.approval.act.preview,
                                already: r.already || false } });
        return ok({ done: true, approval: { id: r.approval.id, approvedBy: r.approval.approvedBy } });
      }

      if (op === 'withdraw-approval') {
        const r = await tenancy.revokeApproval(store, {
          tenantId: tenant.id, approvalId: body.approvalId,
          actorOid: actor.oid, actorRoles: actor.roles,
        });
        if (!r.ok) return denied('run.approve', { reason: r.reason }, { approvalId: body.approvalId });
        await audit({ action: 'run.approve-withdraw', outcome: 'allowed', severity: 'notice',
                      resourceType: 'approval', resourceId: body.approvalId,
                      detail: { requestedBy: r.approval.requestedBy } });
        return ok({ done: true });
      }

      return fail('Unknown op: ' + op, 400);
    }

    return fail('Method not allowed', 405);
  } catch (e) {
    console.error('[rbac-admin]', e.message);
    return fail('Operation failed: ' + e.message, e.statusCode || 500);
  }
};
