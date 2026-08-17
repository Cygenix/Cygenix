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
// GET  ?what=classifications  → connection classifications (connection.view)
// GET  ?what=audit&limit=n    → audit entries (audit.read; delivery roles
//                               see their own entries only)
// GET  ?what=audit&verify=1   → walk the hash chain        (audit.read F)
// GET  ?what=audit&export=1   → full export                (audit.export)
// POST { op:'assign'|'revoke', oid, role }                 (role.assign;
//        OW/PA appointments require the Owner — role.assign-admin)
// POST { op:'deactivate'|'activate', oid }                 (user.manage)
// POST { op:'classify', server, database, environment }    (connection.classify)
//
// Every state-changing call writes an audit entry, and every denial does
// too — a refused act is evidence the control fired (spec Section 10).

'use strict';

const { verifyAuthHeader } = require('./lib/entra-auth');
const rbac = require('./lib/rbac');
const org  = require('./lib/org-store');

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

  let authed;
  try { authed = await verifyAuthHeader(event); }
  catch (e) { return fail('Auth error: ' + e.message, 401); }

  let store, actor;
  try {
    store = org.orgStore();
    actor = await org.resolveActor(store, authed, authed);
  } catch (e) {
    return fail(e.message, e.statusCode || 500);
  }

  const audit = (evt) => org.appendAudit(store, {
    actorOid: actor.oid, actorEmail: actor.email, effectiveRoles: actor.roles, ...evt,
  });

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

      return fail('Unknown op: ' + op, 400);
    }

    return fail('Method not allowed', 405);
  } catch (e) {
    console.error('[rbac-admin]', e.message);
    return fail('Operation failed: ' + e.message, e.statusCode || 500);
  }
};
