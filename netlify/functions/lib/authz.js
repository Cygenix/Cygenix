// netlify/functions/lib/authz.js
//
// One door. Every authenticated route calls authorize() and receives its
// actor, its tenant and a decision together, or it receives nothing and an
// error with the right status code on it.
//
// The brief asked for authorisation that "is not middleware that can be
// forgotten". Middleware is forgettable because it runs beside the handler;
// this runs *inside* it and produces the values the handler needs — the
// actor's identity, the tenant's store keys, the audit closure. A handler
// that skips it has no actor to work with, and tests/route-authz.test.js
// proves the point from the outside by calling every route in the functions
// directory with no token and then with a token carrying no roles, and
// failing the build on any 2xx.
//
// Routes that are genuinely open say so here, in source, rather than in the
// test — an exemption that lives in the test is an exemption nobody reviews.

'use strict';

const { verifyAuthHeader } = require('./entra-auth');
const rbac = require('./rbac');
const org = require('./org-store');
const tenancy = require('./tenancy');

// ── Routes with no authentication, and why ────────────────────────────────
//
// Adding a name here is a security decision. The coverage test reads this
// list, so an addition shows up in the diff as one.
const PUBLIC_ROUTES = {
  'health':   'Liveness only. Returns a status literal and no state.',
  'waitlist': 'Pre-signup capture from the marketing site. Anonymous by definition; ' +
              'parses and re-serialises the body rather than forwarding it.',
  // Invoked by Netlify cron, not by a URL. It authenticates to the services
  // it calls with server-held credentials and reads no caller input.
  'scheduled-runner': 'Netlify scheduled function. No HTTP route; triggered by the platform cron.',
};

class AuthzError extends Error {
  constructor(message, statusCode, detail) {
    super(message);
    this.name = 'AuthzError';
    this.statusCode = statusCode || 403;
    this.detail = detail || null;
  }
}

// ── The call every route makes ────────────────────────────────────────────
//
//   const ctx = await authz.authorize(event, {
//     route: 'projects', action: 'project.create', mutating: true,
//   });
//
// Throws AuthzError(401) when the token does not verify, AuthzError(403)
// when the policy refuses — and audits the refusal before it throws, because
// a denial that leaves no record is a control nobody can evidence.
//
// `action: null` is permitted and means "authenticate and resolve the
// tenant, but this route's own gate decides" — db-connect maps a dozen wire
// actions onto policy actions itself, and forcing a placeholder action here
// would be a fiction. Such a route still cannot skip the call, because it
// needs the actor and the tenant this returns.
async function authorize(event, spec) {
  spec = spec || {};

  let authed;
  try {
    authed = await verifyAuthHeader(event);
  } catch (e) {
    throw new AuthzError('Auth error: ' + e.message, 401);
  }

  let store, actor;
  try {
    store = org.orgStore();
    actor = await org.resolveActor(store, authed, authed);
  } catch (e) {
    throw new AuthzError(e.message, e.statusCode || 500);
  }

  const orgState = await org.loadAll(store);
  const resolved = await tenancy.resolveTenant(store, actor, orgState.users);
  const tenant = resolved.tenant;

  // The tenant id and the route belong on every entry, and they are merged
  // last so a caller-supplied detail cannot drop them.
  const audit = (evt) => org.appendAudit(store, {
    actorOid: actor.oid, actorEmail: actor.email, effectiveRoles: actor.roles,
    ...evt,
    detail: { ...(evt.detail || {}), tenantId: tenant.id, route: spec.route },
  });

  if (resolved.created) {
    await audit({
      action: 'tenant.create', outcome: 'allowed', severity: 'high',
      resourceType: 'tenant', resourceId: tenant.id,
      detail: { name: tenant.name, migratedFrom: tenant.migratedFrom || null,
                migratedMembers: resolved.migratedMembers || false },
    });
  }
  if (resolved.joined) {
    await audit({
      action: 'tenant.join', outcome: 'allowed', severity: 'notice',
      resourceType: 'tenant', resourceId: tenant.id,
      detail: { via: 'invitation', inviteId: resolved.invite && resolved.invite.id },
    });
    // The invitation named roles; grant them now, so admission and
    // entitlement are one act rather than two that can drift.
    await applyInvitedRoles(store, actor, resolved.invite, audit);
  }

  const ctx = {
    authed, store, actor, tenant, membership: resolved.membership,
    audit, route: spec.route,
    guardrails: tenancy.normaliseGuardrails(tenant.guardrails),
  };

  if (!spec.action) return ctx;

  const resource = { ...(spec.resource || {}), mutating: !!spec.mutating };
  const decision = rbac.can(actor, spec.action, resource);
  ctx.decision = decision;
  if (!decision.allow) {
    await audit({
      action: spec.action, outcome: 'denied', severity: decision.severity,
      environment: decision.environment,
      resourceType: spec.resourceType || null, resourceId: spec.resourceId || null,
      detail: { reason: decision.reason },
    });
    throw new AuthzError('Not permitted: ' + decision.reason, 403, { reason: decision.reason });
  }
  return ctx;
}

// Roles an invitation carried, granted on first sign-in. Re-uses the RBAC
// assignment shape so the Users & Roles screen shows them exactly as it
// shows a hand-assigned role — there is no second kind of grant.
async function applyInvitedRoles(store, actor, invite, audit) {
  if (!invite || !Array.isArray(invite.roles) || !invite.roles.length) return;
  const state = await org.loadAll(store);
  const now = new Date().toISOString();
  let added = 0;
  for (const role of invite.roles) {
    if (!rbac.ROLE_CODES.includes(role) || role === 'SP') continue;
    if (state.assignments.some(a => a.oid === actor.oid && a.role === role && !a.revokedAt)) continue;
    state.assignments.push({
      id: 'ra_' + Date.now() + '_' + role, oid: actor.oid, role,
      scope: 'organisation', grantedBy: invite.invitedBy || 'invitation',
      grantedAt: now, revokedAt: null, viaInvite: invite.id,
    });
    added++;
  }
  if (!added) return;
  await store.setJSON('rbac/assignments', { assignments: state.assignments });
  org.invalidate();
  await audit({
    action: 'role.assign', outcome: 'allowed', severity: 'notice',
    resourceType: 'role_assignment', resourceId: actor.oid,
    detail: { roles: invite.roles, via: 'invitation', inviteId: invite.id },
  });
}

// ── Cross-tenant reads ────────────────────────────────────────────────────
//
// The answer is 404 either way: telling an outsider that an id exists is
// itself a disclosure, and the difference between "no such project" and "not
// yours" is exactly the fact worth hiding. The difference is recorded on our
// side instead, at high severity, because a caller reaching for another
// tenant's id is either a bug or a probe and both are worth knowing about.
async function notFound(ctx, { objectId, resourceType = 'project' }) {
  if (objectId) {
    const owner = await tenancy.tenantOfObject(ctx.store, objectId);
    if (owner && owner !== ctx.tenant.id) {
      await ctx.audit({
        action: 'tenant.cross-access', outcome: 'denied', severity: 'high',
        resourceType, resourceId: objectId,
        detail: { reason: 'object belongs to another tenant', objectTenantId: owner },
      });
    }
  }
  return new AuthzError('Not found', 404);
}

// Standard error shaping, so every route answers an AuthzError the same way.
function errorResponse(e, headers) {
  const status = (e && e.statusCode) || 500;
  return {
    statusCode: status,
    headers: headers || { 'Content-Type': 'application/json' },
    body: JSON.stringify({ error: e && e.message ? e.message : 'Operation failed' }),
  };
}

module.exports = { authorize, AuthzError, notFound, errorResponse, PUBLIC_ROUTES };
