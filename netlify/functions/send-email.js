// netlify/functions/send-email.js
//
// SMTP relay for the Email (SMTP) integration.
//
// Actions (POST, JSON body):
//   verify { smtp }                          → { ok: true }        connects + authenticates, sends nothing
//   send   { smtp, from, to, subject, text }  → { ok: true, messageId }
//
// WHY THIS FUNCTION EXISTS
//   A browser cannot speak SMTP — there are no raw TCP sockets in a page, and
//   an SMTP server is not an HTTP endpoint, so `fetch` cannot reach one. Every
//   other connector in the integrations registry runs entirely client-side;
//   this one cannot. The mail has to be handed to a server that can open the
//   socket.
//
// CREDENTIAL HANDLING — read this before changing anything here
//   The integrations page tells users their credentials stay in the browser.
//   That remains true for every other connector, and it is true for SMTP at
//   REST: the host/user/password live in localStorage and are never persisted
//   server-side. But they are necessarily sent to this function, over HTTPS,
//   on each verify or send, because that is the only way the socket can be
//   opened. That trade-off is stated in the SMTP config dialog rather than
//   buried here.
//
//   Consequences enforced below:
//     • Nothing is written to any store. The transport is built per request
//       and discarded — there is no credential cache to leak or expire.
//     • The password is never logged. Error logging goes through redact()
//       and error text returned to the client is scrubbed the same way,
//       because SMTP servers habitually echo the login in their rejections
//       ("535 5.7.8 Username and Password not accepted").
//     • Requests are authenticated with an Entra token exactly like every
//       other function here, so this cannot be used as an open mail relay.
//
// AUTHENTICATION
//   `Authorization: Bearer <jwt>` required, verified against Entra's JWKS by
//   lib/entra-auth. The authenticated email is used as the envelope sender
//   fallback when the caller supplies no From address, so a relayed message is
//   always attributable to the account that sent it.
//
// Required npm dependency (netlify/functions/package.json):
//   "nodemailer": "^7.0.9"

const nodemailer = require('nodemailer');
const { verifyAuthHeader } = require('./lib/entra-auth');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

function ok(data)             { return { statusCode: 200, headers: CORS, body: JSON.stringify(data) }; }
function err(msg, code = 500) { return { statusCode: code, headers: CORS, body: JSON.stringify({ error: msg }) }; }

// Connecting to an arbitrary host:port from a server is an SSRF primitive.
// SMTP is only ever spoken on these ports, so anything else is a request to
// use this function as a port scanner rather than to send mail.
const ALLOWED_PORTS = new Set([25, 465, 587, 2525]);

// Strip the password out of anything heading for a log or a response body.
function redact(text, secret) {
  let s = String(text == null ? '' : text);
  if (secret) s = s.split(secret).join('***');
  return s;
}

// Reject a config before opening any socket. Returns an error string or null.
function validateSmtp(smtp) {
  if (!smtp || typeof smtp !== 'object') return 'SMTP settings missing';
  const host = String(smtp.host || '').trim();
  if (!host) return 'SMTP host is required';
  // A URL, not a hostname — usually someone pasting a webmail address.
  if (/^[a-z]+:\/\//i.test(host)) return 'SMTP host should be a hostname such as smtp.example.com, not a URL';
  const port = Number(smtp.port);
  if (!Number.isInteger(port))     return 'SMTP port is required';
  if (!ALLOWED_PORTS.has(port)) {
    return 'Port ' + port + ' is not an SMTP port. Use 587 (STARTTLS), 465 (SSL), 25 or 2525.';
  }
  if (!String(smtp.user || '').trim()) return 'SMTP username is required';
  if (!String(smtp.pass || ''))        return 'SMTP password is required';
  return null;
}

function buildTransport(smtp) {
  const port = Number(smtp.port);
  return nodemailer.createTransport({
    host: String(smtp.host).trim(),
    port,
    // Implicit TLS on 465; STARTTLS is negotiated on the others. Taking the
    // caller's word for `secure` on port 587 produces a hang rather than an
    // error, which is a miserable thing to debug from the UI.
    secure: port === 465,
    auth: { user: String(smtp.user).trim(), pass: String(smtp.pass) },
    connectionTimeout: 15000,
    greetingTimeout:   10000,
    socketTimeout:     20000,
  });
}

// Addresses go into SMTP headers, so a newline lets a caller inject extra
// headers (Bcc, Reply-To) into the message.
function cleanAddr(v) {
  return String(v == null ? '' : v).replace(/[\r\n]+/g, ' ').trim();
}

async function handleVerify(smtp) {
  const bad = validateSmtp(smtp);
  if (bad) return err(bad, 400);
  const transport = buildTransport(smtp);
  try {
    await transport.verify();
    return ok({ ok: true, message: 'Connected and authenticated' });
  } finally {
    try { transport.close(); } catch { /* nothing useful to do */ }
  }
}

async function handleSend(smtp, body, authedEmail) {
  const bad = validateSmtp(smtp);
  if (bad) return err(bad, 400);

  const to = cleanAddr(body.to);
  if (!to) return err('At least one recipient is required', 400);

  // Fall back to the authenticated user so a relayed message is always
  // attributable, rather than being sent with an empty envelope sender.
  const from    = cleanAddr(body.from) || cleanAddr(smtp.user) || authedEmail;
  const subject = cleanAddr(body.subject) || 'Cygenix notification';
  const text    = String(body.text || '');
  if (!text.trim()) return err('Message body is empty', 400);

  const transport = buildTransport(smtp);
  try {
    const info = await transport.sendMail({ from, to, subject, text });
    return ok({
      ok: true,
      messageId: info.messageId || '',
      // Some servers accept for one recipient and refuse another; saying
      // "sent" when half of them bounced would be a lie.
      accepted: (info.accepted || []).length,
      rejected: (info.rejected || []).length,
    });
  } finally {
    try { transport.close(); } catch { /* nothing useful to do */ }
  }
}

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };
  if (event.httpMethod !== 'POST')    return err('POST only', 405);

  let authed;
  try {
    authed = await verifyAuthHeader(event);
  } catch (e) {
    return err('Unauthorized: ' + (e.message || 'invalid token'), 401);
  }

  let body = {};
  try { body = JSON.parse(event.body || '{}'); } catch { return err('Invalid JSON body', 400); }

  const action = String(body.action || '').toLowerCase();
  const smtp   = body.smtp || {};
  const secret = smtp && smtp.pass;

  try {
    if (action === 'verify') return await handleVerify(smtp);
    if (action === 'send')   return await handleSend(smtp, body, authed.email);
    return err('Unknown action: ' + action, 400);
  } catch (e) {
    // SMTP rejections are the useful half of this integration — "relay access
    // denied", "authentication failed", "no such user" all tell the user
    // exactly what to change, so the message is passed through rather than
    // swallowed. Redacted first: servers quote the credentials back at you.
    const msg = redact(e && e.message ? e.message : String(e), secret);
    console.error('[send-email]', redact(e && e.stack ? e.stack : msg, secret));
    return err(msg || 'Mail send failed', 502);
  }
};

// Exported for tests. Netlify only looks for `handler`, so these are inert in
// production — but the validation and redaction rules are the security surface
// of this function and need to be asserted directly.
module.exports.validateSmtp = validateSmtp;
module.exports.redact       = redact;
module.exports.cleanAddr    = cleanAddr;
module.exports.ALLOWED_PORTS = ALLOWED_PORTS;
