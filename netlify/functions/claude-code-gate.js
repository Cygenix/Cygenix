// netlify/functions/claude-code-gate.js
//
// The yes/no the Azure Function App asks before it touches the Anthropic
// Managed Agents API on somebody's behalf.
//
// WHY THIS LIVES HERE AND NOT IN AZURE
// Roles live in Netlify Blobs (cygenix-org: rbac/users, rbac/assignments)
// and the organisation's Claude Code policy lives beside the guardrails on
// the tenant record. The Function App can read neither. Two ways round that
// were on the table:
//
//   1. Check in data-proxy and let Azure trust the result. Rejected: the two
//      Function App host keys were once published in the client and their
//      rotation is still an open item, so anybody holding one could call the
//      Azure route directly and a proxy-only check would never run.
//   2. Azure asks THIS function, forwarding the caller's own Entra token.
//      Chosen. The token is the credential, not a shared secret, so a host
//      key alone gets an Azure caller nothing: it still needs a token that
//      verifies, for a user whose roles and organisation say yes.
//
// Azure caches a yes for 30 seconds per user, so a console polling every
// three seconds costs one gate call in ten, not one in one.
//
// POST { act, record?, detail? }
//   act 'probe'    workspace connectivity test — claudecode.configure (OW, PA)
//   act 'use'      open or drive a read-only session — claudecode.use, AND
//                  the organisation switch is on, AND the actor holds a role
//                  on the organisation's allow-list
//   act 'changes'  the same, for a session allowed to change data — a
//                  mutating act, which the Auditor's R grant refuses
//   record         optional: the name of the event this call IS, to be
//                  written to the trail on a yes — 'probe', 'session.start',
//                  'session.stop', 'session.resume' (with act 'use') or
//                  'session.changes-on', 'session.staging' (with act
//                  'changes'). Filed as claudecode.<name>. A past session
//                  continued from the Sessions list (Oct-2026) is
//                  session.resume, naming whether it carried on in the same
//                  workspace or a new one; "Allow changes" is off again
//                  after it, so switching it back on is a session.changes-on
//                  of its own.
//
// 200 { allowed: true, roles, tenantId }        — go ahead
// 403 { error, reason }                         — no, with a sentence a user
//                                                 can act on
// Every refusal is audited (authz does it for the matrix; this file does it
// for the organisation policy). A plain allowed 'use' is NOT audited — it is
// asked every half-minute of every session — only the named events are.

'use strict';

const authz = require('./lib/authz');
const rbac = require('./lib/rbac');
const tenancy = require('./lib/tenancy');

const HEADERS = { 'Content-Type': 'application/json' };
const reply = (statusCode, data) => ({ statusCode, headers: HEADERS, body: JSON.stringify(data) });

const ACTS = {
  probe:   { action: 'claudecode.configure', mutating: true,  policy: false, records: ['probe'] },
  use:     { action: 'claudecode.use',       mutating: false, policy: true,  records: ['session.start', 'session.stop', 'session.resume'] },
  changes: { action: 'claudecode.use',       mutating: true,  policy: true,  records: ['session.changes-on', 'session.staging'] },
};
// Handing a database login to an agent is worth a notice; telling the agent
// it may change the data is worth more than one.
// A staging session (Oct-2026) is a session that may change one schema from
// its first message, so it is recorded like changes being allowed, naming the
// schema.
const RECORD_SEVERITY = { 'session.changes-on': 'high', 'session.staging': 'high' };

// Only these keys of a caller's detail reach the audit trail, and only as
// short strings — the gate is asked by a server, but it is still input.
// 'workspace' and 'resumed' (Oct-2026) say how a past session was continued:
// in the same workspace or a new one, and that a staging grant is a resume.
const DETAIL_KEYS = ['host', 'port', 'network', 'sessionId', 'profile', 'connection', 'schema', 'workspace', 'resumed'];
function cleanDetail(d) {
  const out = {};
  if (!d || typeof d !== 'object') return out;
  DETAIL_KEYS.forEach(k => {
    if (d[k] !== undefined && d[k] !== null) out[k] = String(d[k]).slice(0, 200);
  });
  return out;
}

exports.handler = async function (event) {
  if (event.httpMethod !== 'POST') return reply(405, { error: 'POST only' });

  let ctx;
  try {
    ctx = await authz.authorize(event, { route: 'claude-code-gate', action: null });
  } catch (e) {
    return authz.errorResponse(e, HEADERS);
  }
  const { actor, tenant, audit } = ctx;

  let body;
  try { body = JSON.parse(event.body || '{}'); }
  catch (e) { return reply(400, { error: 'Body is not JSON' }); }

  const spec = ACTS[body.act];
  if (!spec) return reply(400, { error: 'act must be one of ' + Object.keys(ACTS).join('|') });
  const record = body.record ? String(body.record) : '';
  if (record && spec.records.indexOf(record) === -1) {
    return reply(400, { error: 'record must be one of ' + spec.records.join('|') + ' for act ' + body.act });
  }
  const detail = cleanDetail(body.detail);

  const refuse = async (reason, message, severity) => {
    await audit({ action: spec.action, outcome: 'denied', severity: severity || 'notice',
                  resourceType: 'tenant', resourceId: tenant.id,
                  detail: Object.assign({ act: body.act, reason }, detail) });
    return reply(403, { error: message, reason });
  };

  try {
    const decision = rbac.can(actor, spec.action, { mutating: spec.mutating });
    if (!decision.allow) {
      return refuse(decision.reason, body.act === 'probe'
        ? 'Only an Organisation Owner or Platform Administrator can run the Dev Console connection test.'
        : body.act === 'changes'
          ? 'Your role cannot allow the Dev Console to change data.'
          : 'Not enabled for your role — ask an Owner to enable it in Governance.',
        decision.severity);
    }

    if (spec.policy) {
      const policy = tenancy.normaliseClaudeCode(tenant.claudeCode);
      if (!policy.enabled) {
        return refuse('console switched off',
          'The Dev Console is switched off for this organisation — ask an Owner to enable it in Governance.');
      }
      if (!actor.roles.some(r => policy.roles.indexOf(r) !== -1)) {
        return refuse('role not on the allow-list',
          'Not enabled for your role — ask an Owner to enable it in Governance.');
      }
    }

    // Recorded when the caller names the event this call is — a test or a
    // session starting, data changes being allowed, a session stopping —
    // and not on the routine permission checks in between.
    if (record) {
      await audit({ action: 'claudecode.' + record, outcome: 'allowed', severity: RECORD_SEVERITY[record] || 'notice',
                    resourceType: record === 'probe' ? 'tenant' : 'claudecode_session',
                    resourceId: record === 'probe' ? tenant.id : (detail.sessionId || tenant.id), detail });
    }
    return reply(200, { allowed: true, roles: actor.roles, tenantId: tenant.id });
  } catch (e) {
    return reply(500, { error: e.message, stack: e.stack });
  }
};

// For tests.
exports._internals = { ACTS, cleanDetail };
