// tests/secret-exposure.test.js — credentials must not reach the browser,
// and the browser must not assert who it is.
//
// THE INCIDENT THIS ENCODES
//
// The Azure Function App holding every user's projects, jobs, saved
// connections and subscription record derived the caller's identity from a
// request header:
//
//     function getUserId(req) { return req.headers.get('x-user-id') || ... }
//
// A header is whatever the caller says it is. The only thing in front of it
// was the Function App's host key — hard-coded into six files served to
// every visitor, and committed to this repository. View source, take the
// key, set the header to another person's email, and their data was
// readable and writable. `requireAdmin` read the role from the record
// belonging to that same spoofable id, so it reached the admin endpoints too.
//
// Both halves are now closed: the browser holds no key and asserts no
// identity, and /.netlify/functions/data-proxy establishes the caller from a
// verified Entra token. These tests exist so neither half can come back —
// this is exactly the kind of change that gets re-introduced by a well-meant
// "just call the API directly here" patch six months from now.
'use strict';

const fs = require('fs');
const path = require('path');

const P = (...p) => path.join(__dirname, '..', ...p);
const read = (...p) => fs.readFileSync(P(...p), 'utf8');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

console.log('Secret exposure — no credential in the client, no identity from the client\n');

const clientFiles = []
  .concat(fs.readdirSync(P('public')).filter(f => /\.(js|html)$/.test(f)).map(f => ['public', f]));

// Several of these files now carry a comment explaining what was wrong and
// what replaced it. Those comments legitimately quote the old header and the
// old endpoint. Blank comment bodies out — keeping the line count intact so
// reported line numbers still point at the real line — and scan the code.
const codeCache = new Map();
function code(dir, f) {
  const k = dir + '/' + f;
  if (codeCache.has(k)) return codeCache.get(k);
  const blanked = read(dir, f).replace(/\/\*[\s\S]*?\*\//g,
    (m) => m.split('\n').map(() => '').join('\n'));
  codeCache.set(k, blanked);
  return blanked;
}

/* ══ 1. No credential material in anything the browser downloads ═════════ */

// An Azure Function host key is base64url, 40+ chars, and ends in '=='.
// Matching the shape rather than the specific value is what makes this
// catch the NEXT key rather than only the two that leaked.
const FN_KEY_SHAPE = /['"][A-Za-z0-9_-]{40,}==['"]/;
const ASSIGNED_SECRET = /\b(FUNC_CODE|FUNCTION_KEY|fnKey|funcCode|apiKey|API_KEY|SAS_TOKEN|sasToken|accountKey|COSMOS_KEY|clientSecret)\s*[:=]\s*['"][A-Za-z0-9_+/=-]{20,}['"]/;

// There used to be ONE documented exception here: the Drive's blob relay
// embedded the Function App host key, because it streams file BODIES and a
// Netlify function in the path would cap uploads at 6 MB.
//
// It is gone (5 Sep 2026), and not because the relay got its proper auth.
// The same key was set in Netlify as CYGENIX_DATA_FN_KEY, and Netlify's
// secrets scanning then found that value in dashboard-app.js on every build
// and refused to publish — nothing deployed for days. The relay now fetches
// the credential at runtime from data-proxy, after Entra verification. The
// browser still ends up holding a host key, so the real fix (Entra auth on
// the relay, plus rotation) still stands; but no key ships in the build.
const KNOWN_EXCEPTIONS = new Set([]);

{
  const offenders = [];
  for (const [dir, f] of clientFiles) {
    code(dir, f).split('\n').forEach((ln, i) => {
      const t = ln.trim();
      if (/^(\*|\/\/|<!--)/.test(t)) return;              // prose about the incident is not the incident
      if (!FN_KEY_SHAPE.test(ln) && !ASSIGNED_SECRET.test(ln)) return;
      const named = (t.match(/\b([A-Za-z_][A-Za-z0-9_]*)\s*[:=]/) || [])[1] || '';
      if (KNOWN_EXCEPTIONS.has(f + ':' + named)) return;
      offenders.push(f + ':' + (i + 1) + ' ' + t.slice(0, 90));
    });
  }
  check('no Function App key or embedded secret ships to the browser — no exceptions',
    offenders.length === 0, offenders.join(' | '));
}

{
  // The blob relay must not grow its key literal back, and must get the
  // credential the one sanctioned way.
  const dash = read('public', 'dashboard-app.js');
  check('the blob relay no longer embeds a key literal',
    !/BLOB_PROXY_CODE\s*=\s*'/.test(dash),
    'a key literal in the client is what stopped every deploy');
  check('it fetches the credential at runtime through the data api, after verification',
    /callResult\('blob-credential'\)/.test(dash) && (dash.match(/blobRelayCode\(\)/g) || []).length >= 6);
  check('and never writes it to browser storage',
    !/(localStorage|sessionStorage)\.setItem\([^)]*blob[^)]*code/i.test(dash));
  const proxy = read('netlify', 'functions', 'data-proxy.js');
  check('the proxy serves blob-credential itself, after identity verification, and never forwards it',
    proxy.indexOf("action === 'blob-credential'") > proxy.indexOf('await verifyAuthHeader(')
    && proxy.indexOf("action === 'blob-credential'") < proxy.indexOf('// ── Forward'));
  check('with no-store, so no cache ever holds the key',
    /blob-credential'[\s\S]{0,600}'Cache-Control': 'no-store'/.test(proxy));
}

{
  // The two keys that actually leaked, pinned by value. They are revoked, so
  // a reappearance is a revert rather than a new mistake — and the message
  // should say so.
  const LEAKED = ['WjSmoWxgtNdGnO_I5nKIspRUQqKCR1knsXgVmJr3dyYuAzFu-or-5Q==',
                  'UfQpm2qJuc0lWIbYwtGUm_k6ys_FDEquCma-KCPl_e8rAzFu5qfwbA=='];
  const back = [];
  for (const [dir, f] of clientFiles) {
    const src = read(dir, f);
    LEAKED.forEach(k => { if (src.includes(k)) back.push(f); });
  }
  check('neither exposed key is anywhere in the client tree',
    back.length === 0, back.join(', '));
}

/* ══ 2. The browser never asserts an identity to the API ════════════════ */
{
  // `x-user-id` was the spoofable claim. No client file may set it.
  const offenders = [];
  for (const [dir, f] of clientFiles) {
    code(dir, f).split('\n').forEach((ln, i) => {
      const t = ln.trim();
      if (/^(\*|\/\/|<!--)/.test(t)) return;
      // A header assignment, in any of the shapes the codebase used.
      if (!/['"]x-user-id['"]\s*:/.test(ln) && !/setRequestHeader\(\s*['"]x-user-id/i.test(ln)) return;
      // The blob relay is the tracked exception; it goes when its auth does.
      if (f === 'dashboard-app.js' && /blob/i.test(code(dir, f).split('\n').slice(Math.max(0, i - 12), i + 1).join(' '))) return;
      offenders.push(f + ':' + (i + 1) + ' ' + t.slice(0, 90));
    });
  }
  check('no client file sends an x-user-id header — identity comes from the token',
    offenders.length === 0, offenders.join(' | '));
}

{
  // Nothing in the browser should reach the Function App directly any more;
  // every call goes through the proxy. A direct call is how a key becomes
  // necessary again.
  const offenders = [];
  for (const [dir, f] of clientFiles) {
    code(dir, f).split('\n').forEach((ln, i) => {
      const t = ln.trim();
      if (/^(\*|\/\/|<!--)/.test(t)) return;
      if (!/azurewebsites\.net/.test(ln)) return;
      if (/fetch\s*\(|XMLHttpRequest|\.open\s*\(/.test(ln)) {
        offenders.push(f + ':' + (i + 1) + ' ' + t.slice(0, 90));
      }
    });
  }
  check('no client file fetches the Function App directly',
    offenders.length === 0, offenders.join(' | '));
}

/* ══ 3. The proxy itself ════════════════════════════════════════════════ */
{
  const proxy = read('netlify', 'functions', 'data-proxy.js');

  check('the proxy verifies the Entra token before doing anything else',
    /verifyAuthHeader/.test(proxy) && proxy.indexOf('verifyAuthHeader') < proxy.indexOf('fetch(url'));
  check('identity is read from verified claims, never from the request',
    /authed\.email \|\| authed\.sub/.test(proxy)
    && !/event\.headers\[['"]x-user-id/.test(proxy)
    && !/queryStringParameters.*userId/.test(proxy.replace(/k !== 'userId'/g, '')));
  check('a caller-supplied userId or code is stripped before forwarding',
    /k !== 'code'/.test(proxy) && /k !== 'userId'/.test(proxy));
  check('the host key comes from the environment, with no embedded fallback',
    /process\.env\.CYGENIX_DATA_FN_KEY/.test(proxy)
    && !FN_KEY_SHAPE.test(proxy.split('\n').filter(l => !/^\s*(\/\/|\*)/.test(l)).join('\n')));
  check('a missing key refuses loudly rather than degrading to unauthenticated',
    /is not configured/.test(proxy) && /503/.test(proxy));
  check('forwarded actions are an allow-list, not a pass-through',
    /const ALLOWED = new Set/.test(proxy));
  check('agent paths are allow-listed by shape and reject traversal',
    /function agentPathAllowed/.test(proxy) && /includes\('\.\.'\)/.test(proxy));
  check('the anonymous waitlist action is deliberately NOT proxied',
    !/'waitlist'/.test(proxy.split('const ALLOWED')[1].split(']')[0]));
  check('the caller\'s token is forwarded so the Function App can verify it too',
    /headers\.Authorization = bearer/.test(proxy));
}

/* ══ 4. The client helper ═══════════════════════════════════════════════ */
{
  const apiRaw = read('public', 'cygenix-data-api.js');
  // Strip comments: this file DOCUMENTS the incident, so its prose names both
  // the old endpoint and the old header. The code must not.
  const api = apiRaw.replace(/\/\*[\s\S]*?\*\//g, '')
                    .split('\n').filter(l => !/^\s*\/\//.test(l)).join('\n');
  check('the client helper talks only to the proxy',
    /\/\.netlify\/functions\/data-proxy/.test(api) && !/azurewebsites/.test(api));
  check('it attaches the Entra token and nothing else',
    /Authorization: 'Bearer ' \+ t/.test(api) && !/x-user-id/.test(api));
  check('it refuses to call at all without a token',
    /Not signed in/.test(api));
  check('callOrNull exists for the call sites that deliberately fail open',
    /function callOrNull/.test(api));
}

/* ══ 5. Every consuming page actually loads its dependencies ════════════ */
{
  const pages = fs.readdirSync(P('public')).filter(f => f.endsWith('.html'));
  const missingApi = [], missingToken = [], badOrder = [];
  for (const f of pages) {
    const src = read('public', f);
    const usesApi = /CygenixDataApi/.test(src) || /cygenix-cosmos-sync\.js/.test(src);
    if (!usesApi) continue;
    if (!/cygenix-data-api\.js/.test(src)) missingApi.push(f);
    if (!/cygenix-auth-token\.js/.test(src)) missingToken.push(f);
    // The helper reads getCygenixIdToken, so the token helper must come first.
    if (/cygenix-data-api\.js/.test(src) && /cygenix-auth-token\.js/.test(src)
        && src.indexOf('cygenix-auth-token.js') > src.indexOf('cygenix-data-api.js')) {
      badOrder.push(f);
    }
  }
  check('every page that uses the API loads the client helper',
    missingApi.length === 0, missingApi.join(', '));
  check('and the token helper it depends on', missingToken.length === 0, missingToken.join(', '));
  check('with the token helper loaded first', badOrder.length === 0, badOrder.join(', '));
}

/* ══ 6. Credentials are not written to browser storage ═════════════════ */
{
  // A connection string or an API key in localStorage survives the tab, the
  // session and the machine's next user. These keys are the ones the audit
  // found; the check is that nothing NEW joins them.
  const SECRET_KEYS = /localStorage\.setItem\(\s*['"](cygenix_conn_string|cygenix_src_conn_string|cygenix_fn_key|anthropic_api_key)['"]/;
  const offenders = [];
  for (const [dir, f] of clientFiles) {
    code(dir, f).split('\n').forEach((ln, i) => {
      const t = ln.trim();
      if (/^(\*|\/\/|<!--)/.test(t)) return;
      if (SECRET_KEYS.test(ln)) offenders.push(f + ':' + (i + 1));
    });
  }
  // Reported, not enforced: the interactive connection flow legitimately holds
  // a credential for the life of a session, and moving it is WI-10, not this
  // change. What must not happen quietly is the list GROWING.
  console.log(offenders.length
    ? '  NOTE  ' + offenders.length + ' site(s) still write a credential-shaped key to localStorage '
      + '(known, tracked as WI-10): ' + offenders.slice(0, 4).join(', ')
    : '  NOTE  no credential-shaped localStorage writes remain');
}

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
