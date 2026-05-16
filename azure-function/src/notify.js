// azure-function/src/notify.js
//
// /api/notify — Resend-backed email notification endpoint + internal helper.
//
// ── What this module exposes ──────────────────────────────────────────────
// 1. HTTP route POST /api/notify
//    Body: { userId, type, data }
//    Used by external callers (e.g. dashboard.html, scheduler.js) if they
//    want to fire a notification without going through one of the runners.
//
// 2. Module export: sendNotification(userId, type, data, ctx)
//    Used internally by run-migration.js (and any other module in this
//    project) via require('./notify').sendNotification(...). Sends via
//    Resend and logs the result to the Cosmos `notifications` container.
//
// ── Event types supported ─────────────────────────────────────────────────
//   migration-success   — fired when a scheduled migration completes ok
//   migration-failed    — fired when a scheduled migration errors out
//
// More types can be added below in TEMPLATES. Add a (subject, html) builder
// keyed by the type string and the dispatcher will pick it up automatically.
//
// ── Configuration (Azure Function app settings) ───────────────────────────
//   RESEND_API_KEY        — required, starts with `re_`
//   NOTIFY_FROM_ADDRESS   — optional, defaults to 'Cygenix <notifications@cygenix.co.uk>'
//   NOTIFY_REPLY_TO       — optional, defaults to 'sales@cygenix.co.uk'
//   NOTIFY_DASHBOARD_URL  — optional, link rendered in email CTAs.
//                           Defaults to 'https://cygenix.co.uk/dashboard.html'
//
// ── Failure policy ────────────────────────────────────────────────────────
// Notification failures NEVER throw to the caller. The migration runner
// must not fail because the email provider is down. All errors are logged
// to Cosmos `notifications` with status:'failed' and the error message,
// plus written to ctx.log. Callers can ignore the return value.

const { app } = require('@azure/functions');

// ── Cosmos client (lazy singleton, matches index.js / run-migration.js) ───
let _cosmos = null;
function getCosmosContainer(containerName) {
  if (!_cosmos) {
    const { CosmosClient } = require('@azure/cosmos');
    _cosmos = new CosmosClient({
      endpoint: process.env.COSMOS_ENDPOINT,
      key:      process.env.COSMOS_KEY,
    });
  }
  return _cosmos
    .database(process.env.COSMOS_DATABASE || 'cygenix')
    .container(containerName);
}

// ── CORS / response helpers (match index.js style) ────────────────────────
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, x-user-id',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};
const ok  = (body)      => ({ status: 200, headers: CORS, body: JSON.stringify(body) });
const err = (code, msg) => ({ status: code, headers: CORS, body: JSON.stringify({ error: msg }) });

// ── Defaults ──────────────────────────────────────────────────────────────
const DEFAULT_FROM = 'Cygenix <notifications@cygenix.co.uk>';
const DEFAULT_REPLY_TO = 'sales@cygenix.co.uk';
const DEFAULT_DASHBOARD_URL = 'https://cygenix.co.uk/dashboard.html';

// ── HTML escape (defensive — schedule names come from user input) ─────────
function escapeHtml(str) {
  return String(str == null ? '' : str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// ── Shared email chrome ───────────────────────────────────────────────────
// Branded wrapper around the per-event body. Inline styles only — most
// email clients strip <style> blocks.
function wrapEmail(innerHtml) {
  return `<!doctype html>
<html lang="en">
<body style="margin:0;padding:0;background:#f5f7fa;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;color:#1a1a1a;">
  <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#f5f7fa;padding:32px 16px;">
    <tr>
      <td align="center">
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="600" style="max-width:600px;background:#ffffff;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,0.06);overflow:hidden;">
          <tr>
            <td style="padding:24px 32px;border-bottom:1px solid #eaecef;">
              <div style="font-size:20px;font-weight:600;color:#0b5fff;letter-spacing:-0.01em;">Cygenix</div>
            </td>
          </tr>
          <tr>
            <td style="padding:32px;font-size:15px;line-height:1.55;">
              ${innerHtml}
            </td>
          </tr>
          <tr>
            <td style="padding:20px 32px;border-top:1px solid #eaecef;background:#fafbfc;font-size:12px;color:#6b7280;">
              You're receiving this because a Cygenix scheduled job ran on your account.<br>
              Reply to this email or write to <a href="mailto:${DEFAULT_REPLY_TO}" style="color:#0b5fff;text-decoration:none;">${DEFAULT_REPLY_TO}</a> if you need help.
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>`;
}

// ── Per-event templates ───────────────────────────────────────────────────
// Each builder takes a `data` object (whatever the caller passed) and
// returns { subject, html }. The keys here are the `type` values accepted
// by sendNotification / /api/notify.
const TEMPLATES = {
  'migration-success': (data) => {
    const name        = escapeHtml(data.scheduleName || 'Untitled job');
    const rows        = Number(data.rowsAffected || 0).toLocaleString('en-GB');
    const elapsed     = data.elapsedSec ? `${data.elapsedSec}s` : '—';
    const dashboard   = process.env.NOTIFY_DASHBOARD_URL || DEFAULT_DASHBOARD_URL;
    const subject     = `Migration succeeded — ${data.scheduleName || 'Untitled job'}`;
    const inner = `
      <p style="margin:0 0 16px 0;font-size:18px;font-weight:600;color:#0b8a3e;">Migration succeeded</p>
      <p style="margin:0 0 24px 0;">Your scheduled migration <strong>${name}</strong> has completed successfully.</p>
      <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#f8fafc;border-radius:6px;padding:16px;margin:0 0 24px 0;">
        <tr><td style="padding:6px 12px;font-size:13px;color:#6b7280;">Rows affected</td><td style="padding:6px 12px;font-size:13px;text-align:right;font-weight:600;">${rows}</td></tr>
        <tr><td style="padding:6px 12px;font-size:13px;color:#6b7280;">Elapsed</td><td style="padding:6px 12px;font-size:13px;text-align:right;font-weight:600;">${elapsed}</td></tr>
      </table>
      <p style="margin:0;">
        <a href="${dashboard}" style="display:inline-block;background:#0b5fff;color:#ffffff;text-decoration:none;padding:10px 20px;border-radius:6px;font-weight:500;">Open dashboard</a>
      </p>
    `;
    return { subject, html: wrapEmail(inner) };
  },

  'migration-failed': (data) => {
    const name        = escapeHtml(data.scheduleName || 'Untitled job');
    const error       = escapeHtml(data.errorMessage || 'No details available');
    const dashboard   = process.env.NOTIFY_DASHBOARD_URL || DEFAULT_DASHBOARD_URL;
    const subject     = `Migration failed — ${data.scheduleName || 'Untitled job'}`;
    const inner = `
      <p style="margin:0 0 16px 0;font-size:18px;font-weight:600;color:#b91c1c;">Migration failed</p>
      <p style="margin:0 0 16px 0;">Your scheduled migration <strong>${name}</strong> did not complete. Details are below.</p>
      <div style="background:#fef2f2;border-left:3px solid #b91c1c;border-radius:4px;padding:12px 16px;margin:0 0 24px 0;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:13px;color:#7f1d1d;word-break:break-word;">
        ${error}
      </div>
      <p style="margin:0;">
        <a href="${dashboard}" style="display:inline-block;background:#0b5fff;color:#ffffff;text-decoration:none;padding:10px 20px;border-radius:6px;font-weight:500;">Open dashboard</a>
      </p>
    `;
    return { subject, html: wrapEmail(inner) };
  },
};

// ── Recipient resolution ──────────────────────────────────────────────────
// userId in this codebase IS the user's verified Entra email (per the
// project_reports partition convention, lowercased). We send notifications
// to that address. If it doesn't look like an email, we refuse and log.
function isEmail(s) {
  return typeof s === 'string' && /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(s);
}

// ── Audit-log writer ──────────────────────────────────────────────────────
async function logNotification({ userId, type, to, subject, status, resendId, error }, ctx) {
  try {
    const doc = {
      id: `${Date.now()}_${Math.random().toString(36).slice(2, 10)}`,
      userId,
      type,
      to,
      subject,
      status,                        // 'sent' | 'failed' | 'skipped'
      resendId: resendId || null,
      error:    error    || null,
      sentAt:   new Date().toISOString(),
    };
    await getCosmosContainer('notifications').items.create(doc);
  } catch (e) {
    // Audit-log failure must never break the caller. Log and move on.
    ctx && ctx.log && ctx.log('[notify] audit write failed:', e.message);
  }
}

// ── Core sender ───────────────────────────────────────────────────────────
// Returns { ok: true, resendId } on success, { ok: false, error } on
// failure. Never throws.
async function sendNotification(userId, type, data, ctx) {
  // Guards — log and skip on bad input rather than throwing
  if (!userId)             { ctx && ctx.log && ctx.log('[notify] skip: no userId'); return { ok: false, error: 'no userId' }; }
  if (!isEmail(userId))    { ctx && ctx.log && ctx.log('[notify] skip: userId is not an email:', userId); await logNotification({ userId, type, to: null, subject: null, status: 'skipped', error: 'userId not an email' }, ctx); return { ok: false, error: 'userId not an email' }; }
  if (!TEMPLATES[type])    { ctx && ctx.log && ctx.log('[notify] skip: unknown type:', type); return { ok: false, error: 'unknown type' }; }

  const apiKey = process.env.RESEND_API_KEY;
  if (!apiKey) {
    ctx && ctx.log && ctx.log('[notify] RESEND_API_KEY not set');
    await logNotification({ userId, type, to: userId, subject: null, status: 'failed', error: 'RESEND_API_KEY not configured' }, ctx);
    return { ok: false, error: 'RESEND_API_KEY not configured' };
  }

  const from    = process.env.NOTIFY_FROM_ADDRESS || DEFAULT_FROM;
  const replyTo = process.env.NOTIFY_REPLY_TO     || DEFAULT_REPLY_TO;
  const { subject, html } = TEMPLATES[type](data || {});

  try {
    const resp = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type':  'application/json',
      },
      body: JSON.stringify({
        from,
        to: [userId],
        reply_to: replyTo,
        subject,
        html,
      }),
    });

    if (!resp.ok) {
      const errText = await resp.text().catch(() => '');
      ctx && ctx.log && ctx.log('[notify] Resend error', resp.status, errText);
      await logNotification({ userId, type, to: userId, subject, status: 'failed', error: `Resend ${resp.status}: ${errText.slice(0, 500)}` }, ctx);
      return { ok: false, error: `Resend ${resp.status}` };
    }

    const json = await resp.json().catch(() => ({}));
    const resendId = json && json.id ? json.id : null;
    ctx && ctx.log && ctx.log('[notify] sent', { type, to: userId, resendId });
    await logNotification({ userId, type, to: userId, subject, status: 'sent', resendId }, ctx);
    return { ok: true, resendId };

  } catch (e) {
    ctx && ctx.log && ctx.log('[notify] send threw:', e.message);
    await logNotification({ userId, type, to: userId, subject, status: 'failed', error: e.message || String(e) }, ctx);
    return { ok: false, error: e.message || String(e) };
  }
}

// ── HTTP route registration ───────────────────────────────────────────────
// POST /api/notify
// Body: { userId, type, data }
// Returns 200 { ok: true, resendId } on success, 200 { ok: false, error }
// on failure (so callers don't have to handle non-2xx separately). Bad
// request payloads return 400.
app.http('notify', {
  methods: ['POST', 'OPTIONS'],
  authLevel: 'function',
  route: 'notify',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 204, headers: CORS };

    let body;
    try { body = await req.json(); }
    catch { return err(400, 'Invalid JSON body'); }

    const { userId, type, data } = body || {};
    if (!userId) return err(400, 'userId is required');
    if (!type)   return err(400, 'type is required');

    const result = await sendNotification(userId, type, data || {}, ctx);
    return ok(result);
  },
});

// ── Module exports ────────────────────────────────────────────────────────
// Other modules in this Function app can require this file and call
// sendNotification directly — that's the path run-migration.js uses.
module.exports = { sendNotification };
