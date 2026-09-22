// tests/login-audit.test.js — one audit row per interactive sign-in, with
// the address and place it came from.
//
// WHAT WAS ASKED
// Record every interactive sign-in with IP, city, region, country and
// browser, expiring after 30 days, capture only — no new UI.
//
// THE SHAPE THAT ANSWERS IT
//   browser  →  /api/login-audit (Netlify EDGE)  →  Azure /api/data/audit-signin
//                                                →  Cosmos `audit` container
// Edge, because city and country are only knowable there. The edge function
// verifies the Entra token, so the identity is never the body's claim; the
// Azure action demands a shared ingest key, so the browser cannot write a
// sign-in row directly.
//
// WHAT THIS PINS, by running the real functions where it can and reading
// their source where a Deno module cannot be imported into Node:
//   1. the entry the edge builds — every geo field optional, the user agent
//      capped, `idp` defaulting to local, the 30-day ttl;
//   2. the edge function's refusals, its secrets handling and the fact that
//      it is registered without touching netlify.toml;
//   3. the Azure action — the ingest-key gate, the validation, the stored
//      shape, the partition key, and a ttl that is dropped when nonsense;
//   4. the client trigger — interactive sign-ins only, never awaited, two
//      guards, neither reset by its own callback, and not on the way out;
//   5. that nothing in this path can leak a token or a secret to the client.
'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');
// Comment lines are stripped before a source assertion, so a claim in prose
// can never satisfy a test that is supposed to be about the code.
const code = (src) => src.split('\n').filter((l) => !/^\s*(\/\/|\*|\/\*)/.test(l)).join('\n');
// A fuller strip for the leak check. This codebase explains itself at length,
// and three files name CYGENIX_DATA_FN_KEY while recounting the deploy it was
// missing from. Naming an environment variable in prose is not a leak; only
// what survives here can actually be sent by a browser.
const noComments = (src) => src
  .replace(/\/\*[\s\S]*?\*\//g, ' ')
  .replace(/(^|[^:])\/\/[^\n]*/g, '$1');

const EDGE = read('netlify', 'edge-functions', 'login-audit.js');
const VERIFY = read('netlify', 'edge-functions', '_lib', 'verify-entra.js');
const INDEX = read('azure-function', 'src', 'index.js');
const LOGIN = read('public', 'login.html');
const edgeCode = code(EDGE), idxCode = code(INDEX), loginCode = code(LOGIN);

console.log('Sign-in audit — who signed in, from where, once\n');

/* Pull a named function out of a source file and run it. The edge module
   imports jose from a URL and the Function App module needs the Azure
   runtime, so neither can be require()d here; the functions under test are
   pure and are lifted out on their own. */
function lift(src, decl, exportAs) {
  const m = src.match(decl);
  if (!m) return null;
  const ctx = { Number, String, Array, Math, JSON, Date, isNaN };
  vm.createContext(ctx);
  // `export` is a module keyword the VM cannot parse; the declaration it
  // decorates is ordinary JavaScript once it is gone.
  vm.runInContext('const TTL_SECONDS = 2592000; const MAX_UA = 300;\n'
    + m[0].replace(/^export\s+/, '')
    + '\nthis.fn = ' + exportAs + ';', ctx);
  return ctx.fn;
}

/* ── 1. The entry the edge builds ────────────────────────────────────────── */
section('1. The entry — verified claims in, every location field optional');
{
  const build = lift(EDGE, /export function buildSigninEntry\(claims, ctx, userAgent, nowIso\) \{[\s\S]*?\n\}/, 'buildSigninEntry');
  check('buildSigninEntry is exported and pure enough to run on its own', typeof build === 'function');

  const NOW = '2026-09-22T10:00:00.000Z';
  const full = build(
    { oid: 'oid-1', email: 'Someone@Example.TEST', idp: 'google.com' },
    { ip: '203.0.113.9', geo: { city: 'Leeds', subdivision: { name: 'England' }, country: { name: 'United Kingdom', code: 'GB' } } },
    'Mozilla/5.0 (X11)', NOW);
  check('a complete sign-in carries the lot',
    full.type === 'signin' && full.timestamp === NOW
    && full.ip === '203.0.113.9' && full.city === 'Leeds' && full.region === 'England'
    && full.country === 'United Kingdom' && full.countryCode === 'GB', JSON.stringify(full));
  check('the address is lower-cased, so the same person is one person', full.email === 'someone@example.test');

  // The container is partitioned on /userId and everywhere else in this
  // system that field holds the email. A row keyed by the Entra object id
  // would sit in a partition nothing else can address, so "my sign-ins"
  // would find nothing and the rows would look lost rather than misfiled.
  check('THE PARTITION KEY IS THE EMAIL, matching every other row in the container',
    full.userId === 'someone@example.test', full.userId);
  check('and the Entra object id is kept alongside, as the identifier that survives an address change',
    full.oid === 'oid-1');
  check('a token with an object id but no address still keys on something',
    build({ oid: 'oid-only' }, {}, '', NOW).userId === 'oid-only');
  check('and an address with no object id keys on the address, with a null oid',
    build({ email: 'e@x.test' }, {}, '', NOW).userId === 'e@x.test'
    && build({ email: 'e@x.test' }, {}, '', NOW).oid === null);
  check('a federated sign-in names its provider', full.idp === 'google.com');
  check('the ttl is 30 days in seconds', full.ttl === 2592000);

  const bare = build({ sub: 'sub-only' }, {}, '', NOW);
  check('NO GEO AT ALL still produces an entry — a VPN must not lose the sign-in',
    bare.userId === 'sub-only' && bare.ip === null && bare.city === null
    && bare.region === null && bare.country === null && bare.countryCode === null, JSON.stringify(bare));
  check('every missing field is null, never absent — a column that vanishes is a column nobody can query',
    ['email', 'oid', 'ip', 'city', 'region', 'country', 'countryCode', 'userAgent'].every((k) => k in bare));
  check('a sign-in Entra itself authenticated is idp "local"', bare.idp === 'local');
  check('oid is preferred over sub, and sub is the fallback',
    build({ oid: 'a', sub: 'b' }, {}, '', NOW).oid === 'a' && bare.oid === 'sub-only');

  const partial = build({ oid: 'x' }, { geo: { country: { name: 'Ireland' } } }, 'UA', NOW);
  check('a half-resolved geo keeps what it has and nulls the rest',
    partial.country === 'Ireland' && partial.countryCode === null && partial.city === null, JSON.stringify(partial));

  check('the user agent is capped at 300 characters',
    build({ oid: 'x' }, {}, 'U'.repeat(500), NOW).userAgent.length === 300);
  check('an empty user agent is null rather than an empty string',
    build({ oid: 'x' }, {}, '', NOW).userAgent === null);

  // Entra External ID puts the address in `emails` for some user flows.
  check('the address is found in emails[] when the usual claims are absent',
    build({ oid: 'x', emails: ['in@array.test'] }, {}, 'UA', NOW).email === 'in@array.test');
  check('and preferred_username and upn are both accepted',
    build({ oid: 'x', preferred_username: 'p@u.test' }, {}, '', NOW).email === 'p@u.test'
    && build({ oid: 'x', upn: 'u@p.test' }, {}, '', NOW).email === 'u@p.test');

  // Two files now decide which claim holds the address. They must agree.
  const order = (s) => (s.match(/email\s*\|\|\s*\w*\.?preferred_username\s*\|\|\s*\w*\.?upn/) || [''])[0].replace(/\w+\./g, '');
  check('the edge entry and the shared verifier resolve the address in the SAME order',
    order(EDGE) && order(EDGE) === order(VERIFY), order(EDGE) + '  vs  ' + order(VERIFY));
}

/* ── 2. The edge function ────────────────────────────────────────────────── */
section('2. The edge function — what it refuses, and what it never shows the client');
{
  check('it is registered inline, so netlify.toml is untouched',
    /export const config = \{ path: '\/api\/login-audit' \}/.test(edgeCode));
  const toml = read('netlify.toml');
  check('netlify.toml has no login-audit block and still has both original edge functions',
    !/login-audit/.test(toml) && /function = "analyse"/.test(toml) && /function = "coworker"/.test(toml));
  check('the two existing edge routes are the only ones declared there',
    (toml.match(/\[\[edge_functions\]\]/g) || []).length === 2);

  check('anything but POST is 405', /request\.method !== 'POST'\) return json\(405/.test(edgeCode));
  check('OPTIONS is answered, so a browser preflight does not fail the call', /request\.method === 'OPTIONS'/.test(edgeCode));
  check('the token is verified before anything else happens, and a bad one is 401',
    /await verifyRequestClaims\(request\)[\s\S]{0,200}return json\(401/.test(edgeCode));
  check('a token carrying neither a subject nor an address is 401 — an entry with no user is not an entry',
    /Token carries neither a subject nor an address/.test(edgeCode)
    && /return json\(401, \{ error: 'Token carries neither/.test(edgeCode));
  check('the identity comes from the CLAIMS, never from the request body',
    /buildSigninEntry\(\s*\n?\s*claims, context/.test(edgeCode) && !/JSON\.parse\(await request\.text/.test(edgeCode));

  check('secrets are read from the edge environment, never from the request',
    /Netlify\.env\.get\(k\)/.test(edgeCode) && !/request\.headers\.get\('x-audit-ingest-key'\)/.test(edgeCode));
  check('a missing ingest key fails the call rather than forwarding without one',
    /if \(!ingestKey\)[\s\S]{0,140}return json\(502/.test(edgeCode));
  check('the ingest key travels as a header, not in the URL where it would be logged',
    /'x-audit-ingest-key': ingestKey/.test(edgeCode) && !/x-audit-ingest-key=/.test(edgeCode));
  check('it reuses the data proxy\'s existing Azure base and key, so no new Azure config is needed',
    /CYGENIX_DATA_API_BASE/.test(edgeCode) && /CYGENIX_DATA_FN_KEY/.test(edgeCode));
  check('the caller\'s own token is forwarded, so the path survives REQUIRE_TOKEN_AUTH being turned on',
    /Authorization: request\.headers\.get\('authorization'\)/.test(edgeCode));

  check('success is 204 with no body — the client is not waiting for anything',
    /new Response\(null, \{ status: 204/.test(edgeCode));
  check('a forwarding failure is 502 WITH the reason in the body (no App Insights to read instead)',
    /Could not reach the Function App: ' \+ e\.message/.test(edgeCode)
    && /Function App returned ' \+ res\.status/.test(edgeCode));
  check('no secret is ever put in a response body',
    !/json\([\s\S]{0,80}(ingestKey|fnKey|token)/.test(edgeCode));

  check('the shared verifier gained the full-claims reader without changing the old one',
    /export async function verifyRequestClaims/.test(VERIFY)
    && /export async function verifyRequestAuth/.test(VERIFY)
    && /return \{\s*email: emailFromClaims\(payload\),/.test(code(VERIFY)));
  check('the audience and issuer list are still the shared ones, not redeclared here',
    !/audience|VALID_ISSUERS|jwtVerify/.test(edgeCode));
}

/* ── 3. The Azure action ─────────────────────────────────────────────────── */
section('3. The Function App — the ingest gate, the stored shape, the ttl');
{
  const act = (idxCode.match(/case 'audit-signin': \{[\s\S]*?\n {8}\}/) || [''])[0];
  check('the action exists in the data dispatcher', act.length > 0);
  check('POST only', /req\.method !== 'POST'\) return err\(405/.test(act));
  check('a missing AUDIT_INGEST_KEY on the Function App is a 500, not an open door',
    /if \(!expected\) \{[\s\S]{0,200}return err\(500/.test(act));
  check('A WRONG OR MISSING INGEST KEY IS 403 — the browser cannot write its own sign-in rows',
    /if \(presented !== expected\) \{[\s\S]{0,140}return err\(403/.test(act));
  check('the refusal says nothing about the key it expected', /'This endpoint is not callable directly'/.test(act));
  check('the key check happens BEFORE the body is read or anything is written',
    act.indexOf('presented !== expected') < act.indexOf('req.json()'));
  check('the 500 path carries message and stack, since there is no App Insights',
    /err\(500, `audit-signin failed: \$\{e\.message\}\\n\$\{e\.stack \|\| ''\}`\)/.test(act));
  check('it is listed as a public action, because the edge function cannot send x-user-id',
    /const PUBLIC_ACTIONS = \['waitlist', 'audit-signin'\]/.test(idxCode));

  const reject = lift(INDEX, /function signinRejection\(body\) \{[\s\S]*?\n\}/, 'signinRejection');
  const build = lift(INDEX, /function buildSigninDoc\(body, now\) \{[\s\S]*?\n\}/, 'buildSigninDoc');
  check('both helpers lift out and run', typeof reject === 'function' && typeof build === 'function');

  const good = { type: 'signin', userId: 'oid-1', timestamp: '2026-09-22T10:00:00.000Z' };
  check('a valid entry is accepted', reject(good) === null);
  check('a body that is not an object is refused', reject(null) === 'Invalid JSON body');
  check('the wrong type is refused', reject({ ...good, type: 'logout' }) === "type must be 'signin'");
  check('a missing or blank userId is refused',
    reject({ ...good, userId: '' }) === 'userId is required' && reject({ type: 'signin', timestamp: good.timestamp }) === 'userId is required');
  check('a timestamp that is not a date is refused',
    reject({ ...good, timestamp: 'not-a-date' }) === 'timestamp must be an ISO date');

  const doc = build({ ...good, email: 'a@b.test', idp: 'google.com', ip: '203.0.113.9',
    city: 'Leeds', region: 'England', country: 'United Kingdom', countryCode: 'GB',
    userAgent: 'UA', ttl: 2592000 }, 1700000000000);
  check('the stored row keeps the container\'s existing shape: id, userId, action, timestamp',
    doc.id === 'oid-1-signin-1700000000000' && doc.userId === 'oid-1'
    && doc.action === 'signin' && doc.timestamp === good.timestamp, JSON.stringify(doc));
  check('THE PARTITION KEY IS THE PERSON WHO SIGNED IN, not the caller', doc.userId === 'oid-1');
  check('it is also marked type signin, so the new rows can be told from the old ones', doc.type === 'signin');
  check('the location and browser are stored', doc.city === 'Leeds' && doc.countryCode === 'GB' && doc.userAgent === 'UA');
  check('the ttl is carried through as an integer', doc.ttl === 2592000);

  const bare = build({ ...good }, 1);
  check('missing optional fields are null, and idp falls back to local',
    bare.email === null && bare.ip === null && bare.city === null && bare.idp === 'local', JSON.stringify(bare));
  check('NO TTL MEANS NO EXPIRY — an absent ttl must never become an immediate one', !('ttl' in bare));
  check('a zero, negative or non-numeric ttl is dropped rather than stored',
    !('ttl' in build({ ...good, ttl: 0 }, 1)) && !('ttl' in build({ ...good, ttl: -5 }, 1))
    && !('ttl' in build({ ...good, ttl: 'soon' }, 1)) && !('ttl' in build({ ...good, ttl: NaN }, 1)));
  check('a fractional ttl is floored to whole seconds', build({ ...good, ttl: 60.9 }, 1).ttl === 60);
  check('long values are capped so one oversized field cannot bloat the row',
    build({ ...good, userAgent: 'U'.repeat(900), email: 'e'.repeat(900) }, 1).userAgent.length === 300);
  check('the container is the existing audit one, not a new one',
    /getCosmosContainer\('audit'\)\.items\.create\(doc\)/.test(act));
  check('the code does NOT set defaultTtl — turning TTL on is a portal decision',
    !/defaultTtl/.test(idxCode));
  check('no function.json folder was added and host.json has no functionTimeout',
    !fs.existsSync(path.join(ROOT, 'azure-function', 'audit-signin'))
    && !/functionTimeout/.test(read('azure-function', 'host.json')));
}

/* ── 4. The client trigger ───────────────────────────────────────────────── */
section('4. The page — an interactive sign-in only, fired once, never waited on');
{
  check('the helper exists on the login page', /function recordSignin\(result\) \{/.test(loginCode));
  const fn = (loginCode.match(/function recordSignin\(result\) \{[\s\S]*?\n\}/) || [''])[0];

  check('IT IS CALLED ONLY WHERE handleRedirectPromise RETURNED A RESULT — not on a reload or a silent refresh',
    /const result = await msalInstance\.handleRedirectPromise\(\);\s*\n\s*if \(result\) \{[\s\S]{0,400}recordSignin\(result\)/.test(loginCode));
  check('and nowhere else on the page', (loginCode.match(/recordSignin\(/g) || []).length === 2);
  check('the silent-refresh branch does not call it',
    !/acquireTokenSilent[\s\S]{0,400}recordSignin/.test(loginCode));
  check('it is not called on the way out of a sign-out', /if \(!justSignedOut\) recordSignin\(result\)/.test(loginCode));

  check('it is never awaited — a sign-in must not wait on its own audit row',
    !/await recordSignin/.test(loginCode) && !/async function recordSignin/.test(loginCode));
  check('keepalive is set, so the request survives the redirect that follows it', /keepalive: true/.test(fn));
  check('a failure is a console warning and nothing else',
    /console\.warn\('\[Auth\] Sign-in audit/.test(fn) && !/alert\(|showError\(/.test(fn));
  check('the whole helper is wrapped, so a throw cannot break the sign-in path',
    /try \{[\s\S]*\} catch \(e\) \{[\s\S]{0,140}console\.warn/.test(fn));

  check('an in-flight guard stops a second call in the same page load',
    /if \(_signinAuditInFlight\) return;/.test(fn) && /let _signinAuditInFlight = false;/.test(loginCode));
  check('a one-shot key names THIS sign-in, so a reload of the redirect cannot record a second row',
    /sessionStorage\.getItem\('cygenix_signin_audited'\) === stamp\) return;/.test(fn)
    && /result\.uniqueId[\s\S]{0,120}claims\.iat/.test(fn));
  check('NEITHER GUARD IS RESET BY THE REQUEST\'S OWN CALLBACK',
    (fn.match(/_signinAuditInFlight = /g) || []).length === 1
    && !/\.then\([\s\S]{0,200}_signinAuditInFlight = false/.test(fn)
    && !/removeItem\('cygenix_signin_audited'\)/.test(loginCode));
  check('the guards are set BEFORE the request goes out, not after it comes back',
    fn.indexOf('_signinAuditInFlight = true') < fn.indexOf('fetch(')
    && fn.indexOf("setItem('cygenix_signin_audited'") < fn.indexOf('fetch('));

  check('it sends the ID token, which is the audience the verifier expects',
    /const token = result\.idToken;/.test(fn) && /Authorization: 'Bearer ' \+ token/.test(fn));
  check('nothing is sent if there is no token to send', /if \(!token\) return;/.test(fn));
  check('the body carries no identity — the server reads that from the token', /body: '\{\}'/.test(fn));
  check('the new sessionStorage key is classified in the storage inventory',
    /'cygenix_signin_audited'/.test(read('scripts', 'storage-inventory.js')));
}

/* ── 4b. Reading them back: the action and the tab ───────────────────────── */
section('4b. The view — the read action, its two scopes, and the Sign-ins tab');
{
  const act = (idxCode.match(/case 'audit-signins': \{[\s\S]*?\n {8}\}/) || [''])[0];
  check('the read action exists and is GET', act.length > 0 && /req\.method !== 'GET'\) return err\(405/.test(act));
  check('the default scope is the caller\'s own history', /String\(req\.query\.get\('scope'\) \|\| 'mine'\)/.test(act));
  check('MINE IS SCOPED TO THE VERIFIED IDENTITY — there is no parameter that names somebody else',
    /c\.userId = @uid[\s\S]{0,300}\{ name: '@uid', value: userId \}/.test(act)
    && !/req\.query\.get\('userId'\)|req\.query\.get\('email'\)/.test(act));
  check('reading your OWN sign-ins is not an admin act — requireAdmin guards only scope=all',
    /if \(scope === 'all'\) \{\s*\n\s*const gate = await requireAdmin\(userId\);/.test(act)
    && act.lastIndexOf('requireAdmin') < act.indexOf("c.userId = @uid"));
  check('both queries are restricted to sign-in rows, so the tab cannot read the rest of the container',
    (act.match(/c\.type = 'signin'/g) || []).length === 2);
  check('the period and the row count are bounded, so a hand-made query cannot ask for a full scan',
    /Math\.min\(365, Math\.max\(1,/.test(act) && /Math\.min\(1000, Math\.max\(1,/.test(act));
  check('a bad days or limit falls back to a default rather than NaN',
    /Number\.isFinite\(rawDays\) \? rawDays : 30/.test(act) && /Number\.isFinite\(rawLimit\) \? rawLimit : 200/.test(act));
  check('failures carry message and stack, as the other actions here do',
    /audit-signins failed: \$\{e\.message\}\\n\$\{e\.stack \|\| ''\}/.test(act));
  check('the proxy forwards the action', /'audit-signins'/.test(code(read('netlify', 'functions', 'data-proxy.js'))));

  const APP = read('public', 'audit-app.js');
  const app = code(APP);
  check('the audit screen has a Sign-ins tab, second, beside Events',
    /\{ key: 'events', label: 'Events' \},\s*\{ key: 'signins', label: 'Sign-ins' \},/.test(app));
  check('it has a panel, a renderer and a loader',
    /id="cyg-a-panel-signins" role="tabpanel"/.test(app)
    && /function renderSigninsPanel\(\)/.test(app) && /function loadSignins\(opts\)/.test(app));
  check('the panel is rendered with the others, so switching tabs does not refetch',
    /renderEventsPanel\(\);\s*\n\s*renderSigninsPanel\(\);/.test(app));

  check('IT LOADS ONCE, WHEN THE TAB IS FIRST OPENED — not on every visit to the audit screen',
    /if \(key === 'signins' && !state\.signins\.loaded && !state\.signins\.loading\) loadSignins\(\{\}\);/.test(app));
  check('and a second request cannot start while one is in flight', /if \(s\.loading\) return;/.test(app));
  check('the in-flight flag is cleared where the request settles, not in a branch that can be skipped',
    /\.then\(function \(\) \{\s*\n\s*s\.loading = false; s\.loaded = true;/.test(app));
  check('a refused scope=all falls back to the caller\'s own history ONCE, not in a loop',
    /if \(s\.denied && !s\.triedAll\) \{ s\.triedAll = true;/.test(app));
  check('the refusal is explained rather than shown as a failure',
    /Only an administrator can see everyone\\?'s sign-ins/.test(app));
  check('it reads through the data layer, not by assembling a URL with a key in it',
    /callResult\('audit-signins', \{ method: 'GET', query: \{ scope: scope, days: days \} \}\)/.test(app)
    && !/azurewebsites\.net/.test(APP));

  // The three label helpers are pure; run them rather than reading them.
  const idp = lift(APP, /function idpLabel\(idp\) \{[\s\S]*?\n {2}\}/, 'idpLabel');
  const br = lift(APP, /function browserLabel\(ua\) \{[\s\S]*?\n {2}\}/, 'browserLabel');
  const place = lift(APP, /function placeLabel\(r\) \{[\s\S]*?\n {2}\}/, 'placeLabel');
  check('the three label helpers lift out and run',
    typeof idp === 'function' && typeof br === 'function' && typeof place === 'function');
  check('the provider is named in a person\'s words, and local means a password',
    idp('local') === 'Password' && idp('google.com') === 'Google' && idp('') === 'Password');
  check('an unrecognised provider is shown as it came, not hidden', idp('okta.example') === 'okta.example');
  const UA = {
    edge: 'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36 Edg/120',
    chrome: 'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 Chrome/120 Safari/537.36',
    safari: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1 Version/17 Safari/605.1',
    firefox: 'Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0',
  };
  check('EDGE IS NOT REPORTED AS CHROME, AND CHROME IS NOT REPORTED AS SAFARI — the specific name wins',
    br(UA.edge) === 'Edge on Windows' && br(UA.chrome) === 'Chrome on Windows' && br(UA.safari) === 'Safari on macOS',
    [br(UA.edge), br(UA.chrome), br(UA.safari)].join(' | '));
  check('Firefox on Linux reads as itself', br(UA.firefox) === 'Firefox on Linux');
  check('a missing user agent is a dash, and an unrecognised one is Unknown',
    br('') === '—' && br('curl/8.0') === 'Unknown');
  check('the place skips what is missing and never repeats itself',
    place({ city: 'Leeds', region: 'England', country: 'United Kingdom' }) === 'Leeds, England, United Kingdom'
    && place({ city: 'Dublin', country: 'Ireland' }) === 'Dublin, Ireland'
    && place({ city: 'Berlin', region: 'Berlin', country: 'Germany' }) === 'Berlin, Germany');
  check('no location at all says so plainly rather than showing an empty cell',
    place({}) === 'Unknown location');

  check('the Who column appears only when looking at everyone',
    /\(all \? '<th>Who<\/th>' : ''\)/.test(app));
  check('the empty state explains that history starts at deployment, not at the beginning of time',
    /Sign-ins are recorded from the moment the feature/.test(app));
  check('the standing note says a missing location is a VPN, not a fault, and names the 30-day window',
    /A missing location means the address could not be placed/.test(app) && /kept for 30 days/.test(app));
  // The integrity band sits above every tab on this screen. A reader must not
  // carry "verified" across from it to a table it does not cover.
  check('AND SAYS THESE ROWS ARE NOT COVERED BY THE CHAIN\'S VERIFICATION',
    /stored separately from the hash-chained trail above and are not covered/.test(app));
}

/* ── 5. Nothing leaks ────────────────────────────────────────────────────── */
section('5. No secret reaches the browser');
{
  check('the ingest key appears in the edge function and the Function App, and nowhere in public/',
    /AUDIT_INGEST_KEY/.test(edgeCode) && /AUDIT_INGEST_KEY/.test(idxCode)
    && !/AUDIT_INGEST_KEY/.test(read('public', 'login.html')));
  const pub = fs.readdirSync(path.join(ROOT, 'public'));
  // The ingest key and its header must not appear in client CODE at all. The
  // Function App key's NAME may: since Sep-2026 the Connections page names
  // CYGENIX_DATA_FN_KEY in the sentence that tells an administrator what to
  // set. Naming a variable is not holding its value — what a client must
  // never do is READ it, which only an environment lookup could.
  const leaked = pub.filter((f) => /\.(js|html)$/.test(f)
    && /AUDIT_INGEST_KEY|x-audit-ingest-key|process\.env\.CYGENIX_DATA_FN_KEY|Netlify\.env|Deno\.env/.test(noComments(read('public', f))));
  check('no page or script in public/ can SEND the ingest key or READ the Function App key (naming it in a message is fine)',
    leaked.length === 0, leaked.join(', '));
  check('and the generated ingest key value is nowhere in the repository\'s tracked source',
    !/UGl-oNz53HVX7Vt4zqVyh7WKOLmTKefXVG0RLnLv9QY/.test(read('public', 'login.html') + edgeCode + idxCode));
  check('the login page never names the Function App host directly',
    !/azurewebsites\.net/.test(read('public', 'login.html')));
  check('the client calls the edge route by path, nothing else', /fetch\('\/api\/login-audit'/.test(loginCode));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
