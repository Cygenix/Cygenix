'use strict';
/* user-anthropic-key.js — the only way an Anthropic key enters this Function App.
 * ---------------------------------------------------------------------------
 * THE RULE THIS ENFORCES
 * Every Claude call made by Cygenix is paid for by the person who asked for it.
 * There is no Cygenix-owned key, no default, no fallback and no "just this
 * once". A request that does not carry the caller's own key does not reach
 * Anthropic at all.
 *
 * WHAT THIS REPLACED (removed 2026-08-28)
 * Five call sites read `process.env.ANTHROPIC_API_KEY` and billed the app
 * owner's personal Anthropic account for work any signed-in user could start —
 * and, in the case of profile-build, for work the Netlify cron scheduler
 * started overnight with nobody watching. That environment variable is now
 * read nowhere in this codebase, and tests/anthropic-billing.test.js fails the
 * build if it comes back.
 *
 * WHY A HEADER, AND ONLY A HEADER
 * A query parameter lands in every access log, proxy log and browser history
 * entry between here and the client. A body field is safer but forces every
 * caller to reshape its payload and cannot be read before the body is parsed.
 * A header is neither logged by default nor part of the payload.
 *
 * WHY THE KEY IS NEVER LOGGED
 * Not in an error message, not in a debug line, not truncated "just to show
 * which key it was". A key that appears in a log is a key that has leaked; the
 * only permitted representation is REDACTED, below.
 */

const HEADER = 'x-anthropic-key';

/* Anthropic keys start sk-ant-. Checking the shape catches the common mistake
   — a Cygenix function key or an Entra token pasted into the wrong box — at
   the door, with a message that says what was wrong, instead of as a 401 from
   Anthropic that reads like the feature is broken. */
const PREFIX = 'sk-ant-';
const CODE = 'USER_KEY_REQUIRED';

/** The ONLY representation of a key permitted in a log or an error body. */
const REDACTED = 'sk-ant-***REDACTED***';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, x-user-id, Authorization, x-anthropic-key',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
  'Content-Type': 'application/json'
};

function body(status, error) {
  return { status: status, headers: CORS, body: JSON.stringify({ error: error, code: CODE }) };
}

/**
 * Pull the caller's Anthropic key off a request.
 *
 * Returns { ok: true, key } or { ok: false, response } — never a key from
 * anywhere but this request, and never a partial success. Callers must check
 * `ok` before doing anything; there is deliberately no variant that returns a
 * usable default.
 */
function userAnthropicKey(req) {
  let raw = '';
  try {
    raw = (req && req.headers && typeof req.headers.get === 'function'
      ? req.headers.get(HEADER)
      : (req && req.headers ? req.headers[HEADER] : '')) || '';
  } catch (e) { raw = ''; }

  const key = String(raw).trim();

  if (!key) {
    return { ok: false, response: body(400,
      'This feature runs on your own Anthropic API key. Send it in the ' +
      HEADER + ' header. Cygenix does not supply one.') };
  }
  if (key.indexOf(PREFIX) !== 0) {
    // Deliberately does not echo what WAS sent — that string may be a
    // credential of some other kind, and repeating it into a response body
    // puts it somewhere new.
    return { ok: false, response: body(400,
      'The ' + HEADER + ' header does not look like an Anthropic API key ' +
      '(they begin "' + PREFIX + '"). Check the key in Settings.') };
  }
  return { ok: true, key: key };
}

/**
 * The answer for an endpoint that CANNOT source a user key, because nobody is
 * there to supply one — an overnight scheduled run, a webhook. It refuses
 * rather than reaching for a key of its own.
 */
function blockedUnattended(what) {
  return {
    status: 503,
    headers: CORS,
    body: JSON.stringify({
      error: (what ? what + ' is' : 'This feature is') +
        ' temporarily disabled while user-supplied API key support is being ' +
        'wired up. It will be re-enabled shortly.',
      code: CODE
    })
  };
}

module.exports = { userAnthropicKey, blockedUnattended, HEADER, PREFIX, CODE, REDACTED };
