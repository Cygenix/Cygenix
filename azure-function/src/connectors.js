// connectors.js — server-side delivery of run notifications.
//
// ── Why this exists ────────────────────────────────────────────────────────
// Cygenix has two notification paths and, until now, only one of them worked
// for a scheduled run.
//
//   1. notify.js sends a fixed Resend email to the schedule owner. It runs
//      inside this Function App and always worked.
//   2. The connectors — Webhook and Email (SMTP), gated by the switches on
//      Settings → Notifications — lived entirely in the browser.
//      dashboard.html's notifyIntegrations() is called from the Run-now poll
//      loop, and the connector configuration is kept in localStorage. A cron
//      tick has no browser and no localStorage, so a scheduled run could
//      never deliver to them, whatever the switches said.
//
// This module is path 2 for the server. It reads the user's connector
// settings out of Cosmos and delivers to them directly.
//
// ── Where the settings live ────────────────────────────────────────────────
// One document per user in the existing `schedules` container, which is
// already partitioned by /userId:
//
//   { id: 'notify-settings', docType: 'notify-settings', userId,
//     events: { started, completed, failed },
//     webhook: { enabled, url, secret },
//     smtp:    { enabled, host, port, user, pass, from, to },
//     updatedAt }
//
// Reusing that container is deliberate: scheduler.js states that it will not
// create containers on demand, so a new one would mean a manual provisioning
// step before any of this worked. The discriminator keeps the two document
// kinds apart, and the list queries in scheduler.js and schedules.js filter
// on it.
//
// ── The credential trade-off, stated plainly ───────────────────────────────
// The Integrations page promises connector credentials stay in the browser.
// For a server to send mail on a schedule, that cannot hold: the SMTP
// password has to be somewhere the server can read. So server-side delivery
// is opt-in per account — the browser uploads nothing until the user turns
// on "Deliver from the server" — and the UI says what turning it on means.
// Nothing here ever writes a credential to the notification log.
//
// ── Failure policy ─────────────────────────────────────────────────────────
// Nothing in this file throws to the caller. A dead webhook URL or a mail
// server that refuses the login is not a migration failure. Every attempt,
// including every failure, is written to the Cosmos `notifications`
// container so Settings → Notifications can show what happened.

const { logNotification } = require('./notify-log');

// ── Cosmos (same lazy singleton pattern as the rest of the app) ────────────
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

const SETTINGS_ID   = 'notify-settings';
const SETTINGS_TYPE = 'notify-settings';

// Event key → the event name the payload carries. These strings are the ones
// already shown on Settings → Notifications and sent by the browser, so a
// webhook receiver cannot tell whether a delivery came from a browser run or
// a scheduled one — which is the point.
const EVENT_NAMES = {
  started:   'cygenix.migration.started',
  completed: 'cygenix.migration.success',
  failed:    'cygenix.migration.failed',
};

// Mirrors NOTIFY_EVENT_DEFAULTS in dashboard.html: completed and failed on,
// started off, because turning started on doubles the message volume.
const EVENT_DEFAULTS = { started: false, completed: true, failed: true };

// Read a user's connector settings. Returns null when the user has never
// opted in, which is the common case and not an error.
async function loadConnectorSettings(userId, ctx) {
  if (!userId) return null;
  try {
    const { resource } = await getCosmosContainer('schedules')
      .item(SETTINGS_ID, userId).read();
    if (!resource || resource.docType !== SETTINGS_TYPE) return null;
    return resource;
  } catch (e) {
    // 404 is "not configured", not a failure worth logging as one.
    if (e && e.code === 404) return null;
    ctx && ctx.log && ctx.log('[connectors] settings read failed:', e.message);
    return null;
  }
}

// ── Webhook ───────────────────────────────────────────────────────────────
// Same contract as the browser's runWebhook: POST the JSON payload, add the
// shared secret as X-Cygenix-Signature when one is set.
async function deliverWebhook(cfg, payload) {
  const url = String((cfg && cfg.url) || '').trim();
  if (!url) return { ok: false, message: 'Webhook URL not set' };
  if (!/^https?:\/\//i.test(url)) return { ok: false, message: 'Webhook URL must be http(s)' };

  const headers = { 'Content-Type': 'application/json' };
  if (cfg.secret) headers['X-Cygenix-Signature'] = cfg.secret;

  try {
    // A hung endpoint must not hold the migration's completion path open.
    const res = await fetch(url, {
      method: 'POST', headers, body: JSON.stringify(payload),
      signal: AbortSignal.timeout(15_000),
    });
    return res.ok
      ? { ok: true,  message: 'delivered (' + res.status + ')' }
      : { ok: false, message: 'HTTP ' + res.status };
  } catch (e) {
    return { ok: false, message: (e && e.message) || 'network error' };
  }
}

// ── SMTP ──────────────────────────────────────────────────────────────────
// The rules here are copied from netlify/functions/send-email.js rather than
// re-derived. They are the security surface of sending mail on someone's
// behalf and they matter more here, not less: this runs unattended.

// Connecting to an arbitrary host:port from a server is an SSRF primitive.
// SMTP is only ever spoken on these ports.
const ALLOWED_PORTS = new Set([25, 465, 587, 2525]);

function validateSmtp(cfg) {
  if (!cfg || typeof cfg !== 'object') return 'SMTP settings missing';
  const host = String(cfg.host || '').trim();
  if (!host) return 'SMTP host is required';
  if (/^[a-z]+:\/\//i.test(host)) return 'SMTP host should be a hostname, not a URL';
  const port = Number(cfg.port);
  if (!Number.isInteger(port)) return 'SMTP port is required';
  if (!ALLOWED_PORTS.has(port)) {
    return 'Port ' + port + ' is not an SMTP port. Use 587, 465, 25 or 2525.';
  }
  if (!String(cfg.user || '').trim()) return 'SMTP username is required';
  if (!cfg.pass) return 'SMTP password is required';
  if (!String(cfg.to || '').trim()) return 'No recipient set';
  return null;
}

// SMTP servers habitually echo the login back in a rejection ("535 5.7.8
// Username and Password not accepted"), and the notification log is shown in
// the UI, so the password is scrubbed out of anything that leaves this file.
function redact(text, secret) {
  const s = String(text == null ? '' : text);
  return secret ? s.split(secret).join('***') : s;
}

// A newline in an address is a header-injection primitive: it lets the value
// close the To: header and start one of its own.
function cleanAddr(v) {
  return String(v == null ? '' : v).replace(/[\r\n]+/g, ' ').trim();
}

// Turn an emitted event into something worth reading in an inbox. Same shape
// as the browser's smtpFormat, so a scheduled run's message and a Run-now
// message look identical.
function smtpFormat(payload) {
  const p = payload || {};
  const ev = p.event || 'cygenix.event';
  const bits = [];
  if (p.project) bits.push('Project: ' + p.project);
  if (p.job)     bits.push('Job: ' + p.job);
  if (p.status)  bits.push('Status: ' + p.status);
  if (p.rows != null)  bits.push('Rows: ' + p.rows);
  if (p.message) bits.push('', p.message);
  return {
    subject: 'Cygenix · ' + ev + (p.status ? ' · ' + p.status : ''),
    text: [ev, 'Sent: ' + (p.sentAt || new Date().toISOString()), '']
      .concat(bits.length ? bits : ['No further detail was supplied with this event.'])
      .join('\n'),
  };
}

async function deliverSmtp(cfg, payload) {
  const bad = validateSmtp(cfg);
  if (bad) return { ok: false, message: bad };

  let nodemailer;
  try {
    nodemailer = require('nodemailer');
  } catch {
    return { ok: false, message: 'nodemailer is not installed in this runtime' };
  }

  const port = Number(cfg.port);
  const { subject, text } = smtpFormat(payload);

  try {
    const transport = nodemailer.createTransport({
      host: String(cfg.host).trim(),
      port,
      secure: port === 465,          // 587/25/2525 upgrade via STARTTLS
      auth: { user: String(cfg.user).trim(), pass: cfg.pass },
      connectionTimeout: 15_000,
      greetingTimeout:   15_000,
      socketTimeout:     20_000,
    });
    const info = await transport.sendMail({
      from:    cleanAddr(cfg.from) || cleanAddr(cfg.user),
      to:      cleanAddr(cfg.to),
      subject: cleanAddr(subject),
      text,
    });
    const rejected = (info && info.rejected && info.rejected.length) || 0;
    return rejected
      ? { ok: false, message: rejected + ' recipient(s) rejected' }
      : { ok: true,  message: 'sent to ' + cleanAddr(cfg.to) };
  } catch (e) {
    return { ok: false, message: redact((e && e.message) || 'send failed', cfg.pass) };
  }
}

// ── Dispatch ──────────────────────────────────────────────────────────────
// Delivers one run event to every connector the user has enabled for
// server-side delivery. Returns the per-connector results for logging;
// never throws, and never rejects.
//
//   eventKey — 'started' | 'completed' | 'failed'
//   payload  — project/job/status/rows/steps/runId/message
async function dispatchConnectors(userId, eventKey, payload, ctx) {
  const eventName = EVENT_NAMES[eventKey];
  if (!eventName) return { skipped: 'unknown event ' + eventKey, results: [] };

  const settings = await loadConnectorSettings(userId, ctx);
  if (!settings) return { skipped: 'no server-side connector settings', results: [] };
  // The account-level opt-in. Without it the browser has uploaded nothing
  // worth acting on, and acting anyway would deliver from settings the user
  // believes are local to their machine.
  if (!settings.serverDelivery) return { skipped: 'server delivery is off', results: [] };

  const events = { ...EVENT_DEFAULTS, ...(settings.events || {}) };
  if (!events[eventKey]) return { skipped: eventKey + ' is switched off', results: [] };

  const out = { event: eventName, sentAt: new Date().toISOString(), ...(payload || {}) };

  const targets = [
    ['webhook', settings.webhook, deliverWebhook],
    ['smtp',    settings.smtp,    deliverSmtp],
  ].filter(([, cfg]) => cfg && cfg.enabled);

  if (!targets.length) return { skipped: 'no connector enabled', results: [] };

  const results = await Promise.all(targets.map(async ([id, cfg, fn]) => {
    let r;
    try { r = await fn(cfg, out); }
    catch (e) { r = { ok: false, message: (e && e.message) || 'failed' }; }

    // The log is the only record a user has that a scheduled run tried to
    // notify them, so a failure is logged exactly as loudly as a success.
    await logNotification({
      userId,
      type:    eventName,
      channel: id,
      to:      id === 'smtp' ? cleanAddr(cfg.to) : String(cfg.url || ''),
      subject: smtpFormat(out).subject,
      status:  r.ok ? 'sent' : 'failed',
      error:   r.ok ? null : r.message,
      source:  'scheduled-run',
      runId:   out.runId || null,
    }, ctx);

    if (!r.ok) {
      ctx && ctx.log && ctx.log('[connectors] ' + id + ' delivery failed:', r.message);
    }
    return { id, ...r };
  }));

  return { event: eventName, results };
}

module.exports = {
  dispatchConnectors,
  loadConnectorSettings,
  // Exported for tests.
  deliverWebhook,
  deliverSmtp,
  validateSmtp,
  smtpFormat,
  cleanAddr,
  redact,
  EVENT_NAMES,
  EVENT_DEFAULTS,
  SETTINGS_ID,
  SETTINGS_TYPE,
};
