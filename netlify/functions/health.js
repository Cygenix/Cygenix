// netlify/functions/health.js
//
// This used to report `apiKeyConfigured: !!process.env.ANTHROPIC_API_KEY`.
// Removed 2026-08-28, for two reasons and either would have been enough.
//
// The small one: it is an unauthenticated endpoint telling the internet
// something about the site's secret configuration. Whether a key is present is
// not the key, but it is a fact about it, and there is no reason for a stranger
// to have it.
//
// The load-bearing one: Cygenix does not hold an Anthropic key at all any more.
// Every Claude call is paid for by the person who asked for it, with a key
// their browser supplies per request. A health check reporting on a
// Cygenix-owned key would be reporting on something that must not exist — and
// the day it answered `true`, it would mean the guarantee had been broken.
exports.handler = async function () {
  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      status: 'ok',
      timestamp: new Date().toISOString(),
    }),
  };
};
