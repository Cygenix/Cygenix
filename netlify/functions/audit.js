// netlify/functions/audit.js
//
// The audit log's own API: read it, filter it, export it, verify it, and
// change what it captures.
//
// Split from rbac-admin.js rather than added to it, because rbac-admin is
// the CONTROL PLANE — the directory, role assignments, invitations,
// guardrails — and this is the RECORD of what that control plane and
// everything else did. Keeping them in one file would mean the endpoint
// that can change a role and the endpoint that records role changes share a
// deployment unit and a blast radius. rbac-admin's existing ?what=audit
// routes stay where they are so the Users & Roles page keeps working; they
// are the same data through a narrower door.
//
//   GET  ?what=status                    current state, settings, KPIs, gaps
//   GET  ?what=events&…                  filtered, cursor-paged, newest first
//   GET  ?what=verify                    walk the chain
//   GET  ?what=export&format=csv|json    the current filter, as a file
//   POST { op:'record', … }              append one event
//   POST { op:'status', state, … }       pause / resume / disable / enable
//   POST|PUT { op:'settings', … }        category toggles, retention, flags
//
// ── Who may do what ───────────────────────────────────────────────────────
//
// Not "admins only" — this product has ten roles and no role called Admin,
// and the permission matrix already had opinions about the audit trail
// before this module existed. Those opinions are kept:
//
//   audit.read       OW/PA read the organisation's trail, AU reads it in
//                    full, and the delivery roles (ML/EN/AP/DO/VA) hold an
//                    'L' grant that resolves to selfOnly — their own acts,
//                    not everyone's. A developer being able to check what
//                    the system recorded about them is a feature, and the
//                    alternative (403 for everyone but two roles) would
//                    also 403 the Auditor, whose entire job is this screen.
//   audit.export     PA and AU. Taking the record OUT of the system is a
//                    narrower act than reading it inside one.
//   audit.configure  OW/PA change it; AU may see the configuration and
//                    change nothing. An auditor who can quieten the trail
//                    they report on is not an auditor.
//
// A refused read is recorded as audit.view.denied — in the always-on
// `audit` category, so it is written even while capture is paused or off.
// Somebody probing this endpoint is exactly the event a paused log must not
// miss.
//
// ── The record endpoint, and why it is the risky one ──────────────────────
//
// POST record is open to any signed-in member, because a great many things
// worth recording happen entirely in the browser and never reach a
// function: an export, a reorder, a bulk generate. That is a real hole, and
// it is bounded rather than denied:
//
//   * the actor, the timestamp and the hash come from the verified token
//     and the server clock — the body's claims about who acted are dropped;
//   * audit-schema.CLIENT_ACTIONS is an allowlist, so the browser cannot
//     assert role.assign or sql.write however it is called;
//   * every such entry is stamped source:'client', so the trail
//     distinguishes a fact the server observed from a claim the browser
//     made — the distinction that actually matters, and the one a single
//     undifferentiated stream destroys;
//   * a per-invocation ceiling stops a loop turning the chain into a
//     denial-of-service against its own storage.
//
// A signed-in caller with NO role assignment reaches none of this (A-01) —
// a verified token is not an entitlement.

'use strict';

const authz  = require('./lib/authz');
const rbac   = require('./lib/rbac');
const org    = require('./lib/org-store');
const schema = require('./lib/audit-schema');
const astate = require('./lib/audit-state');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, OPTIONS',
  'Content-Type': 'application/json',
};
const ok   = (data)          => ({ statusCode: 200,  headers: CORS, body: JSON.stringify(data) });
const fail = (msg, code=500) => ({ statusCode: code, headers: CORS, body: JSON.stringify({ error: msg }) });

// A browser that posts events in a tight loop would otherwise be able to
// grow the chain without limit, and the chain is the one structure here
// that cannot be pruned cheaply. One event per call, and the client helper
// batches nothing.
const MAX_SUMMARY = 500;

// ── CSV, RFC 4180 ─────────────────────────────────────────────────────────
//
// The escaping is the same as the one validation.html uses for its
// exports: quote anything containing a delimiter, a quote or a newline, and
// double an embedded quote. The BOM is there because the overwhelmingly
// likely destination is Excel, which reads a BOM-less UTF-8 file as the
// system code page and turns every accented name into mojibake.
function csvCell(v) {
  if (v === null || v === undefined) return '';
  const s = typeof v === 'object' ? JSON.stringify(v) : String(v);
  return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}

const EXPORT_COLUMNS = [
  ['seq', e => e.seq],
  ['id', e => e.id],
  ['when', e => e.occurredAt],
  ['actor', e => e.actorEmail],
  ['actor_type', e => e.actorType],
  ['on_behalf_of', e => e.onBehalfOf],
  ['roles', e => (e.effectiveRoles || []).join(' ')],
  ['action', e => e.action],
  ['category', e => e.category],
  ['target_type', e => e.resourceType],
  ['target_id', e => e.resourceId],
  ['target_label', e => e.target && e.target.label],
  ['environment', e => e.environment],
  ['outcome', e => e.outcome],
  ['severity', e => e.severity],
  ['summary', e => e.summary],
  ['changes', e => (e.changes || []).map(c => c.field + ': ' + c.before + ' -> ' + c.after).join('; ')],
  ['ip', e => e.context && e.context.ip],
  ['source', e => e.source],
  ['prev_hash', e => e.prevHash],
  ['hash', e => e.entryHash],
];

function toCsv(entries) {
  const rows = ['﻿' + EXPORT_COLUMNS.map(c => c[0]).join(',')];
  for (const e of entries) rows.push(EXPORT_COLUMNS.map(c => csvCell(c[1](e))).join(','));
  return rows.join('\r\n') + '\r\n';
}

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };

  let ctx;
  try {
    ctx = await authz.authorize(event, { route: 'audit', action: null });
  } catch (e) {
    return authz.errorResponse(e, CORS);
  }
  const { store, actor, tenant, audit } = ctx;

  const headers = event.headers || {};
  const clientCtx = {
    actor: { oid: actor.oid, email: actor.email, name: actor.name, roles: actor.roles },
    tenantId: tenant.id,
    route: 'audit',
    ip: headers['x-nf-client-connection-ip'] || headers['client-ip'] ||
        (headers['x-forwarded-for'] || '').split(',')[0].trim() || null,
    userAgent: headers['user-agent'] || null,
    requestId: headers['x-nf-request-id'] || null,
  };

  // A refusal is itself evidence, and it is written in the always-on `audit`
  // category so a paused log still catches somebody trying the door.
  const denied = async (action, decision) => {
    await audit({ action: 'audit.view.denied', category: 'audit',
                  outcome: 'denied', severity: 'notice',
                  summary: 'Refused ' + action,
                  detail: { attempted: action, reason: decision.reason, roles: actor.roles } });
    return fail('Not permitted: ' + decision.reason, 403);
  };

  try {
    // ── GET ───────────────────────────────────────────────────────────────
    if (event.httpMethod === 'GET') {
      const q = event.queryStringParameters || {};
      const what = q.what || 'status';

      const read = rbac.can(actor, 'audit.read', {});
      if (!read.allow) return denied('audit.read', read);
      const selfOnly = !!read.selfOnly;

      if (what === 'status') {
        // Resolving here as well as at write time is what makes an expired
        // pause impossible to observe: the screen cannot show "paused" for
        // a pause that has run out, because reading the status is itself a
        // resolution and writes the audit.resume.
        const resolved = await org.resolveCapture(store);
        const stats = selfOnly ? null : await org.auditStats(store, {});
        const recent = await org.queryAudit(store, { category: 'audit', limit: 100 });
        return ok({
          state: resolved.state,
          storedState: resolved.storedState,
          pausedUntil: resolved.pausedUntil || null,
          msRemaining: resolved.msRemaining || null,
          reason: resolved.reason, changedBy: resolved.changedBy, changedAt: resolved.changedAt,
          settings: resolved.settings,
          categories: schema.CATEGORIES,
          alwaysOn: schema.ALWAYS_ON,
          pausePresets: astate.PAUSE_PRESETS_MIN,
          pauseMaxOptions: astate.PAUSE_MAX_OPTIONS_MIN,
          retentionOptions: astate.RETENTION_OPTIONS_DAYS,
          stats,
          gaps: astate.gapWindows(recent.entries, Date.now()),
          scope: selfOnly ? 'self' : 'organisation',
          canConfigure: rbac.can(actor, 'audit.configure', { mutating: true }).allow,
          canExport: rbac.can(actor, 'audit.export', {}).allow,
          canVerify: !selfOnly,
          me: actor.email,
        });
      }

      if (what === 'events') {
        const res = await org.queryAudit(store, {
          from: q.from, to: q.to,
          // A delivery role's 'L' grant is not a filter the caller chooses.
          actor: selfOnly ? actor.email : q.actor,
          category: q.category, action: q.action, outcome: q.outcome,
          env: q.env, projectId: q.projectId, q: q.q,
          limit: q.limit, cursor: q.cursor,
        });
        return ok({ ...res, scope: selfOnly ? 'self' : 'organisation' });
      }

      if (what === 'verify') {
        // Verification is a statement about the WHOLE chain, so it needs
        // organisation-wide read. A self-only actor verifying their own
        // slice would be asserting something they cannot see.
        if (selfOnly) {
          return denied('audit.verify',
            { reason: 'chain verification needs organisation-wide audit read' });
        }
        const started = Date.now();
        const result = await org.verifyChain(store, { limit: parseInt(q.limit, 10) || 2000 });
        await audit({ action: 'audit.verify', category: 'audit',
                      outcome: result.ok ? 'allowed' : 'failed',
                      severity: result.ok ? 'info' : 'high',
                      summary: result.ok
                        ? 'Chain verified — ' + (result.count || 0) + ' entries intact'
                        : 'Chain verification FAILED at entry ' + result.brokenAt,
                      detail: { ...result, tookMs: Date.now() - started } });
        return ok({ ...result, verifiedAt: new Date().toISOString(),
                    tookMs: Date.now() - started });
      }

      if (what === 'export') {
        const de = rbac.can(actor, 'audit.export', {});
        if (!de.allow) return denied('audit.export', de);
        const res = await org.queryAudit(store, {
          from: q.from, to: q.to, actor: q.actor, category: q.category,
          action: q.action, outcome: q.outcome, env: q.env, q: q.q,
          limit: 10000,
        });
        // Exporting the record is itself part of the record: an evidence
        // pack leaving the building is a fact somebody may later need.
        await audit({ action: 'audit.export', category: 'audit',
                      outcome: 'allowed', severity: 'notice',
                      summary: 'Exported ' + res.entries.length + ' events as '
                               + (q.format === 'csv' ? 'CSV' : 'JSON'),
                      detail: { entries: res.entries.length, format: q.format || 'json',
                                filters: { from: q.from || null, to: q.to || null,
                                           actor: q.actor || null, category: q.category || null,
                                           action: q.action || null, outcome: q.outcome || null,
                                           env: q.env || null, q: q.q || null } } });
        const stamp = new Date().toISOString().slice(0, 10);
        if (q.format === 'csv') {
          return {
            statusCode: 200,
            headers: { ...CORS, 'Content-Type': 'text/csv; charset=utf-8',
                       'Content-Disposition': 'attachment; filename="cygenix_audit_' + stamp + '.csv"' },
            body: toCsv(res.entries),
          };
        }
        return {
          statusCode: 200,
          headers: { ...CORS, 'Content-Disposition': 'attachment; filename="cygenix_audit_' + stamp + '.json"' },
          body: JSON.stringify({ exportedAt: new Date().toISOString(),
                                 exportedBy: actor.email, tenantId: tenant.id,
                                 count: res.entries.length, entries: res.entries }, null, 2),
        };
      }

      return fail('Unknown query: ' + what, 400);
    }

    // ── POST / PUT ────────────────────────────────────────────────────────
    if (event.httpMethod === 'POST' || event.httpMethod === 'PUT') {
      let body = {};
      try { body = JSON.parse(event.body || '{}'); } catch { return fail('Invalid JSON body', 400); }
      const op = body.op || (event.httpMethod === 'PUT' ? 'settings' : '');

      if (op === 'record') {
        // Any signed-in MEMBER, not any signed-in token. A verified token
        // with no role assignment reaches nothing (A-01), and MB — the
        // read-only baseline every member holds — is the floor.
        const d = rbac.can(actor, 'project.read', {});
        if (!d.allow) return denied('audit.record', d);

        const action = String(body.action || '');
        if (!schema.isClientAction(action)) {
          // Refused, and the refusal is recorded: a browser reaching for an
          // action it may not assert is worth knowing about, whether it is
          // a bug in our own page or somebody with curl.
          await audit({ action: 'audit.record.refused', category: 'audit',
                        outcome: 'denied', severity: 'notice',
                        summary: 'Refused a client-asserted event: ' + (action || '(none)'),
                        detail: { attempted: action } });
          return fail('Action not recordable from the browser: ' + (action || '(none)'), 400);
        }

        const resolved = await org.resolveCapture(store);
        if (body.actorType === 'assistant' && resolved.settings.recordAssistant === false) {
          return ok({ recorded: false, reason: 'assistant events are not recorded' });
        }

        const entry = schema.buildEntry({
          ...body,
          category: body.category || schema.CLIENT_ACTIONS[action],
          summary: body.summary ? String(body.summary).slice(0, MAX_SUMMARY) : null,
          // Two settings the operator controls, applied here rather than in
          // the page, because a client that decides what to send has
          // already decided what the log will contain.
          changes: resolved.settings.storeDiffs ? body.changes : null,
        }, { ...clientCtx, source: 'client',
             ip: resolved.settings.storeIp ? clientCtx.ip : null });

        // PROD is the one place a lost audit write takes the action with
        // it. The browser has already done the thing by the time it posts,
        // so "fail closed" here means telling it plainly that the record
        // did not land, which is the honest answer and the one the client
        // helper surfaces rather than swallows.
        const required = entry.environment === 'PROD';
        const written = await org.appendAudit(store, entry, { required });
        if (written && written.dropped) {
          return ok({ recorded: false, reason: written.reason, state: resolved.state });
        }
        if (!written) return fail('The audit write did not land', 503);
        return ok({ recorded: true, id: written.id, seq: written.seq });
      }

      if (op === 'status') {
        const d = rbac.can(actor, 'audit.configure', { mutating: true });
        if (!d.allow) return denied('audit.configure', d);

        const cfg = await org.loadAuditConfig(store, { fresh: true });
        const v = astate.validateTransition(body, cfg, Date.now());
        if (!v.ok) {
          await audit({ action: 'audit.state.refused', category: 'audit',
                        outcome: 'denied', severity: 'notice',
                        summary: 'Refused a capture state change: ' + v.reason,
                        detail: { requested: body.state, reason: v.reason } });
          return fail(v.reason, 400);
        }

        const before = astate.resolveState(cfg, Date.now()).state;
        const saved = await org.saveAuditConfig(store, {
          ...cfg, ...v.next,
          changedBy: actor.email, changedAt: new Date().toISOString(),
        });

        // The state change goes into the always-on `audit` category, so a
        // pause records the fact of its own beginning and an "off" records
        // the fact of its own beginning too. A log that could stop without
        // saying so would be worse than no log.
        const entry = await audit({
          action: v.action, category: 'audit',
          outcome: 'allowed', severity: v.action === 'audit.disable' ? 'high' : 'notice',
          resourceType: 'audit_capture', resourceId: 'capture',
          reason: v.next.reason,
          summary: {
            'audit.pause':   'Capture paused for ' + (v.pauseMinutes || 0) + ' minutes',
            'audit.resume':  'Capture resumed',
            'audit.disable': 'Capture turned OFF',
            'audit.enable':  'Capture turned back on',
          }[v.action] || v.action,
          changes: [{ field: 'state', before, after: v.next.state }],
          detail: { reason: v.next.reason, pausedUntil: v.next.pausedUntil,
                    pauseMinutes: v.pauseMinutes || null },
        });

        // Turning capture off is supposed to email every administrator. It
        // cannot, today, and saying so is better than pretending:
        // send-email.js has no server-held SMTP credentials — the caller
        // supplies { smtp: { host, user, pass } } — so there is no way for
        // a function to send mail on the organisation's behalf. The flag
        // below is what the UI turns into a banner every admin sees on
        // their next load, and the gap is reported rather than hidden.
        const notice = v.action === 'audit.disable'
          ? { adminsEmailed: false,
              reason: 'no server-held SMTP credentials — administrators are notified in-app' }
          : null;

        return ok({ done: true, state: v.next.state, pausedUntil: v.next.pausedUntil,
                    seq: entry && entry.seq, notify: notice,
                    settings: saved.settings });
      }

      if (op === 'settings') {
        const d = rbac.can(actor, 'audit.configure', { mutating: true });
        if (!d.allow) return denied('audit.configure', d);

        const cfg = await org.loadAuditConfig(store, { fresh: true });
        const v = astate.validateSettings(body.settings || body, cfg.settings);
        if (!v.ok) {
          // The locked-category refusal is an acceptance criterion in its
          // own right, and it has to be recorded: an attempt to switch off
          // security auditing is a security event.
          await audit({ action: 'audit.settings.refused', category: 'audit',
                        outcome: 'denied', severity: 'high',
                        summary: 'Refused an audit settings change: ' + v.reason,
                        detail: { reason: v.reason, requested: body.settings || body } });
          return fail(v.reason, 400);
        }
        if (!v.changes.length) return ok({ done: true, settings: v.settings, unchanged: true });

        const saved = await org.saveAuditConfig(store, { ...cfg, settings: v.settings });
        await audit({ action: 'audit.settings', category: 'audit',
                      outcome: 'allowed', severity: 'notice',
                      resourceType: 'audit_settings', resourceId: 'settings',
                      summary: 'Changed audit settings: ' + v.changes.map(c => c.field).join(', '),
                      changes: v.changes, detail: { fields: v.changes.map(c => c.field) } });
        return ok({ done: true, settings: saved.settings, changes: v.changes });
      }

      return fail('Unknown op: ' + (op || '(none)'), 400);
    }

    return fail('Method not allowed', 405);
  } catch (e) {
    console.error('[audit]', e.message);
    return fail('Operation failed: ' + e.message, e.statusCode || 500);
  }
};
