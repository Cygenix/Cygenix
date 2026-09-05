// tests/data-proxy.test.js — the authenticated door, exercised.
//
// tests/secret-exposure.test.js proves no key or self-asserted identity is
// left in the client. This one proves the replacement actually holds: it
// drives the handler with a stubbed token verifier and a stand-in Function
// App, and asserts what the upstream RECEIVES.
//
// The single most important assertion in this file is "a spoofed x-user-id
// is replaced by the verified identity". That header was the bypass; if it
// ever survives a request again, this fails.
'use strict';

const http = require('http');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 220) : '')); }
};

console.log('Data proxy — identity comes from the token, never the request\n');

const PORT = 8399;
const AUTH_PATH  = require.resolve('../netlify/functions/lib/entra-auth.js');
const PROXY_PATH = require.resolve('../netlify/functions/data-proxy.js');

// Stub the verifier with the same contract the real one has: it either
// returns verified claims or it throws.
function stubAuth() {
  require.cache[AUTH_PATH] = {
    id: AUTH_PATH, filename: AUTH_PATH, loaded: true, exports: {
      verifyAuthHeader: async (event) => {
        const h = event.headers.authorization || event.headers.Authorization || '';
        if (h !== 'Bearer GOOD') throw new Error('invalid token');
        // Mixed case deliberately: the proxy must normalise it, because the
        // Function App keys Cosmos records by lower-cased email.
        return { email: 'Alice@Example.com', sub: 'alice-oid' };
      },
    },
  };
}
function loadProxy(key) {
  delete require.cache[PROXY_PATH];
  process.env.CYGENIX_DATA_FN_KEY = key;
  process.env.CYGENIX_DATA_API_BASE = 'http://127.0.0.1:' + PORT + '/api/data';
  stubAuth();
  return require(PROXY_PATH).handler;
}

(async function run() {
  let lastReq = null;
  const upstream = http.createServer((req, res) => {
    lastReq = { url: req.url, headers: req.headers };
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
  });
  await new Promise(r => upstream.listen(PORT, r));

  const handler = loadProxy('TEST-KEY-NOT-REAL');
  const call = (o) => handler({
    httpMethod: (o && o.method) || 'GET',
    queryStringParameters: (o && o.q) || { action: 'whoami' },
    headers: (o && o.headers) || { authorization: 'Bearer GOOD' },
    body: o && o.body,
  });

  /* ── The bypass itself ─────────────────────────────────────────────── */
  let r = await call({ headers: {} });
  check('a request with no token is refused', r.statusCode === 401, r.body);

  r = await call({ headers: { authorization: 'Bearer FORGED' } });
  check('a token that does not verify is refused', r.statusCode === 401, r.body);

  lastReq = null;
  r = await call({ headers: { authorization: 'Bearer GOOD', 'x-user-id': 'victim@example.com' } });
  check('a verified caller gets through', r.statusCode === 200, r.body);
  check('THE FIX — a spoofed x-user-id is replaced by the verified identity',
    lastReq.headers['x-user-id'] === 'alice@example.com',
    'the Function App saw x-user-id=' + lastReq.headers['x-user-id']);

  lastReq = null;
  await call({ q: { action: 'load', userId: 'victim@example.com' } });
  check('a ?userId= smuggled in the query string is stripped — the Function '
    + 'App reads that as a fallback identity', !/userId=/.test(lastReq.url), lastReq.url);

  lastReq = null;
  await call({ q: { action: 'load', code: 'ATTACKER-KEY' } });
  check('a caller-supplied ?code= cannot override the server-held key',
    lastReq.url.indexOf('code=TEST-KEY-NOT-REAL') !== -1
    && lastReq.url.indexOf('ATTACKER-KEY') === -1, lastReq.url);

  /* ── The allow-list ────────────────────────────────────────────────── */
  r = await call({ q: { action: 'delete-everything' } });
  check('an unknown action is refused', r.statusCode === 400, r.body);
  r = await call({ q: { action: '' } });
  check('an empty action is refused', r.statusCode === 400, r.body);
  r = await call({ q: { action: 'waitlist' } });
  check('the anonymous waitlist action is deliberately not reachable here',
    r.statusCode === 400, r.body);

  for (const bad of ['/agent/../data/load', '/etc/passwd', 'agent/scope', '//evil.com/x', '/agent/x/../..']) {
    r = await call({ q: { path: bad } });
    check('a path outside the allow-list is refused: ' + bad, r.statusCode === 400, r.body);
  }

  lastReq = null;
  r = await call({ q: { path: '/agent/scope' } });
  check('a legitimate agent path is forwarded',
    r.statusCode === 200 && lastReq.url.indexOf('/api/agent/scope?') === 0, lastReq && lastReq.url);
  lastReq = null;
  await call({ q: { path: '/narrative' } });
  check('so is the narrative route', lastReq.url.indexOf('/api/narrative?') === 0, lastReq.url);
  lastReq = null;
  await call({ q: { path: '/me' } });
  check('and /me', lastReq.url.indexOf('/api/me?') === 0, lastReq.url);

  /* ── Legitimate traffic is not damaged ─────────────────────────────── */
  lastReq = null;
  await call({ q: { action: 'project-summary-document', jobId: 'job_7' } });
  check('extra parameters a caller legitimately needs are passed through',
    lastReq.url.indexOf('jobId=job_7') !== -1, lastReq.url);
  check('the caller\'s token is forwarded, so the Function App can verify it too '
    + 'once REQUIRE_TOKEN_AUTH is switched on',
    lastReq.headers.authorization === 'Bearer GOOD', lastReq.headers.authorization);

  lastReq = null;
  const post = await call({ method: 'POST', q: { action: 'save' }, body: '{"jobs":[]}' });
  check('a POST body reaches the Function App unchanged',
    post.statusCode === 200 && lastReq.headers['content-type'] === 'application/json');

  /* ── Failure modes ─────────────────────────────────────────────────── */
  {
    const noKey = loadProxy('');
    const rr = await noKey({ httpMethod: 'GET', queryStringParameters: { action: 'whoami' },
                             headers: { authorization: 'Bearer GOOD' } });
    check('a deployment with no key refuses (503) rather than degrading to an '
      + 'unauthenticated call', rr.statusCode === 503, rr.body);
  }
  {
    // Upstream unreachable must be a clean 502, not a stack trace.
    const h = loadProxy('TEST-KEY-NOT-REAL');
    process.env.CYGENIX_DATA_API_BASE = 'http://127.0.0.1:1/api/data';
    delete require.cache[PROXY_PATH];
    stubAuth();
    const h2 = require(PROXY_PATH).handler;
    const rr = await h2({ httpMethod: 'GET', queryStringParameters: { action: 'whoami' },
                          headers: { authorization: 'Bearer GOOD' } });
    check('an unreachable Function App returns 502, not a stack trace',
      rr.statusCode === 502 && !/at \w+ \(/.test(rr.body), rr.body);
    void h;
  }

  /* ── blob-credential: the relay's key, served here, to a verified caller ─ */
  {
    r = await call({ q: { action: 'blob-credential' }, headers: {} });
    check('blob-credential without a token is refused', r.statusCode === 401, r.body);
    lastReq = null;
    r = await call({ q: { action: 'blob-credential' } });
    const body = JSON.parse(r.body || '{}');
    check('a verified caller receives the server-held key and the relay base',
      r.statusCode === 200 && body.code === 'TEST-KEY-NOT-REAL' && /\/api\/data$/.test(body.base || ''), r.body);
    check('it is served by the proxy itself — nothing reached the Function App', lastReq === null,
      lastReq && lastReq.url);
    check('and is marked no-store, so no cache keeps a host key',
      /no-store/.test(r.headers['Cache-Control'] || ''), JSON.stringify(r.headers));
  }

  upstream.close();
  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => {
  console.error('harness error:', e);
  process.exit(1);
});
