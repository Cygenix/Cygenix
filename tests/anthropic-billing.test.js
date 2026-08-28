// tests/anthropic-billing.test.js — nobody's Claude call bills Cygenix.
//
// THE RULE
// Every Anthropic call this system makes is paid for by the person who asked
// for it. Cygenix holds no Anthropic key: not in an environment variable, not
// in a config file, not as a fallback for when the user's key is missing.
// A request without the caller's own key does not reach Anthropic at all.
//
// WHAT WENT WRONG (fixed 2026-08-28)
// Nine call sites across the Azure Function App and the Netlify edge read a
// Cygenix-owned key from the environment:
//
//   - Two edge functions whose own comments said, in as many words, "this
//     endpoint spends the site's Anthropic API budget". Any signed-in user
//     could spend it, and one of them was reachable from the Diagnostics page.
//   - Three routes in the Function App: narrative, quality-suggest-rels and
//     profile-classify-subjects.
//   - The Agentive Migration agent, whose Anthropic client was a module-level
//     singleton built once from the environment — so every run by every user
//     spent the same account.
//   - The profile builder, dispatched by the Netlify cron overnight. A
//     schedule set up once billed the owner nightly, unattended, forever.
//   - A health endpoint that told the unauthenticated internet whether a key
//     was configured.
//
// These checks are deliberately blunt: a grep for the environment variable and
// a grep for a fallback. Blunt is what survives. A subtle test of intent can be
// satisfied by code that reintroduces the bill; "this string does not appear"
// cannot.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const ROOT = path.join(__dirname, '..');
const P = (...p) => path.join(ROOT, ...p);
const read = (...p) => fs.readFileSync(P(...p), 'utf8');

console.log('Anthropic billing — every call is on the caller\'s own key\n');

/* Every file the deploy actually ships: the browser bundle, both Netlify
   surfaces, and the Function App. Not tests (they describe the rule) and not
   node_modules. */
function walk(dir, out) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    if (e.name === 'node_modules' || e.name.startsWith('.')) continue;
    const full = path.join(dir, e.name);
    if (e.isDirectory()) walk(full, out);
    else if (/\.(js|html|json|toml|md)$/.test(e.name)) out.push(full);
  }
  return out;
}
const SHIPPED = ['public', 'netlify', 'azure-function/src', 'scripts']
  .flatMap((d) => (fs.existsSync(P(d)) ? walk(P(d), []) : []));
const rel = (f) => path.relative(ROOT, f);

/* A comment recording that the read was removed is the one permitted mention.
   Blank comment bodies out and scan the code that is left. */
function code(file) {
  return fs.readFileSync(file, 'utf8')
    .replace(/\/\*[\s\S]*?\*\//g, (m) => m.split('\n').map(() => '').join('\n'))
    .split('\n')
    .map((ln) => (/^\s*(\/\/|\*|<!--|#)/.test(ln) ? '' : ln))
    .join('\n');
}

/* ══ 1. There is no Cygenix-owned Anthropic key anywhere ══════════════════ */

const ENV_READ = /(process\.env|Deno\.env\.get\(['"]?)\s*[.(]?\s*['"]?ANTHROPIC_API_KEY/;
const envHits = SHIPPED.filter((f) => ENV_READ.test(code(f))).map(rel);
check('no shipped file reads an Anthropic key from the environment',
  envHits.length === 0, envHits.join(', '));

/* The variable name may survive in prose explaining its removal. It must not
   survive anywhere a value could be read from. */
const anyMention = SHIPPED.filter((f) => /ANTHROPIC_API_KEY/.test(fs.readFileSync(f, 'utf8')));
const undocumented = anyMention.filter((f) => !/removed|Removed|REMOVED|disabled|DISABLED/.test(fs.readFileSync(f, 'utf8')));
check('every surviving mention of the variable is an explanation of its removal',
  undocumented.length === 0, undocumented.map(rel).join(', '));

/* ══ 2. Nothing falls back to a key of Cygenix's ══════════════════════════ */

// The dangerous shape is `userKey || somethingElse`. A key that has an "or"
// after it is a key with a backup, and the backup is the bill.
const FALLBACK = /(apiKey|anthropicKey|userKey|ANTHROPIC_API_KEY)\s*\|\|\s*(process\.env|Deno\.env|['"]sk-)/;
const fallbackHits = SHIPPED.filter((f) => FALLBACK.test(code(f))).map(rel);
check('no key expression falls back to an environment value or a literal',
  fallbackHits.length === 0, fallbackHits.join(', '));

/* A literal key committed to the repository would bill whoever owns it. */
const LITERAL = /['"]sk-ant-[A-Za-z0-9_-]{8,}/;
const literalHits = SHIPPED.filter((f) => {
  const src = code(f);
  return LITERAL.test(src) && !/REDACTED|sk-ant-\*\*\*/.test(src);
}).map(rel);
check('no Anthropic key literal is committed anywhere', literalHits.length === 0, literalHits.join(', '));

/* ══ 3. The one door into the Function App ═══════════════════════════════ */

const helper = read('azure-function', 'src', 'user-anthropic-key.js');
check('the helper reads the key from a header', /const HEADER = 'x-anthropic-key'/.test(helper));
check('and validates the prefix', /const PREFIX = 'sk-ant-'/.test(helper)
  && /key\.indexOf\(PREFIX\) !== 0/.test(helper));
check('a missing key is refused with 400 and USER_KEY_REQUIRED',
  /body\(400,/.test(helper) && /const CODE = 'USER_KEY_REQUIRED'/.test(helper));
check('an unattended endpoint refuses with 503 and the same code',
  /function blockedUnattended/.test(helper) && /status: 503/.test(helper)
  && /temporarily disabled while user-supplied API key support is being/.test(helper));
check('the helper never reads the environment', !/process\.env/.test(code(P('azure-function', 'src', 'user-anthropic-key.js'))));
check('there is no variant that returns a usable default',
  !/return \{ ok: true, key: (process\.env|''|null)/.test(helper));
check('the only permitted representation of a key in a log is redacted',
  /REDACTED = 'sk-ant-\*\*\*REDACTED\*\*\*'/.test(helper));

/* ══ 4. The key never travels as a query parameter, and is never logged ══ */

const QUERY_KEY = /[?&](api[_-]?key|anthropic[_-]?key|key)=\s*['"+]*\s*(apiKey|anthropicKey|userKey|keyCheck)/i;
const queryHits = SHIPPED.filter((f) => QUERY_KEY.test(code(f))).map(rel);
check('no surface puts an Anthropic key in a query string', queryHits.length === 0, queryHits.join(', '));

const LOGGED = /(console\.(log|error|warn)|ctx\.log(\.\w+)?)\s*\([^)]*\b(apiKey|anthropicKey|userKey)\b/;
const logHits = SHIPPED.filter((f) => LOGGED.test(code(f))).map(rel);
check('no surface logs a key', logHits.length === 0, logHits.join(', '));

/* ══ 5. Every server-side Anthropic caller takes the key from the request ══ */

const SERVER = [
  ['azure-function/src/index.js', 'narrative, quality-suggest-rels, profile-classify-subjects'],
  ['azure-function/src/profile-builder.js', 'profile-build'],
  ['azure-function/src/agent.js', 'agent/migrate'],
  ['azure-function/src/agent-suggest-criteria.js', 'agent/suggest-criteria'],
];
for (const [f, what] of SERVER) {
  const src = read(...f.split('/'));
  check(f.split('/').pop() + ' (' + what + ') takes its key from the request',
    /userAnthropicKey\(req\)/.test(src) || /blockedUnattended/.test(src));
}
// One call site per key read: three in index.js, one each elsewhere.
check('index.js sources a key at every one of its three Claude routes',
  (read('azure-function', 'src', 'index.js').match(/userAnthropicKey\(req\)/g) || []).length === 3,
  (read('azure-function', 'src', 'index.js').match(/userAnthropicKey\(req\)/g) || []).length);

/* The agent's client used to be a module-level singleton built from the
   environment — one client, one account, every user's runs. */
const agent = read('azure-function', 'src', 'agent.js');
check('the agent builds a client per run, not a singleton',
  /function makeAnthropic\(apiKey\)/.test(agent) && !/let _anthropic/.test(agent)
  && !/getAnthropic\(/.test(agent));
check('and refuses to build one without a key',
  /if \(!apiKey\) throw new Error\('No Anthropic API key was supplied/.test(agent));

/* profile-build runs detached, after the response. Its key must therefore be a
   parameter — there is no request left to read by the time it runs. */
const pb = read('azure-function', 'src', 'profile-builder.js');
check('the profile builder takes its key as a parameter, not from the environment',
  /async function runProfileBuild\(task, conn, apiKey, ctx\)/.test(pb));
check('and the unattended cron dispatch is refused outright',
  /blockedUnattended\('Scheduled profile building'\)/.test(pb));

/* ══ 6. The blocked endpoints make no Anthropic call at all ═══════════════ */

for (const f of ['coworker.js', 'analyse.js']) {
  const src = read('netlify', 'edge-functions', f);
  check('the ' + f.replace('.js', '') + ' edge function no longer calls Anthropic',
    !/api\.anthropic\.com/.test(src));
  // coworker's json() helper takes the status positionally; analyse builds a
  // Response directly. Accept either shape — what matters is the 503.
  check('and answers 503 with USER_KEY_REQUIRED',
    /(status: 503|\}, 503,)/.test(src) && /USER_KEY_REQUIRED/.test(src));
}

/* ══ 7. The scheduler cannot start work it has no key for ════════════════ */

const runner = read('netlify', 'functions', 'scheduled-runner.js');
check('the cron refuses to dispatch a profile build',
  /profile-build requires a user API key/.test(runner)
  && runner.indexOf('return { fired: false, reason: \'profile-build requires a user API key\'')
     < runner.indexOf('bgUrl = getProfileBuildUrl()'),
  'the refusal must come BEFORE the dispatch is built');

/* ══ 8. The health endpoint reports nothing about a key ══════════════════ */

check('the health endpoint no longer reports whether a key is configured',
  !/apiKeyConfigured/.test(code(P('netlify', 'functions', 'health.js'))));

/* ══ 9. The browser sends the user's own key where a server needs one ════ */

const model = read('public', 'cygenix-model.js');
check('the browser has one helper for the user\'s key',
  /function mdUserKeyHeader\(\)/.test(model) && /'x-anthropic-key'/.test(model));
check('and it yields no header at all when there is no key, so the server says "no key"',
  /return k \? \{ 'x-anthropic-key': k \} : \{\};/.test(model));

for (const [page, what] of [
  ['insights.html', 'profile-classify-subjects'],
  ['data-quality.html', 'quality-suggest-rels'],
  ['agentive_migration.html', 'agent/migrate'],
  ['report.html', 'narrative'],
]) {
  check(page + ' sends the user\'s key with its Claude-backed call (' + what + ')',
    /CygenixModel\.userKeyHeader\(\)/.test(read('public', page)));
}
check('report.html loads the module that provides it',
  /<script src="\/cygenix-model\.js/.test(read('public', 'report.html')));

const proxy = read('netlify', 'functions', 'data-proxy.js');
check('the data proxy forwards the key to the Function App',
  /headers\['x-anthropic-key'\] = userKey/.test(proxy));
check('and allows the browser to send it', /Access-Control-Allow-Headers[^\n]*x-anthropic-key/.test(proxy));
check('the proxy does not read or substitute a key of its own',
  !/process\.env\.ANTHROPIC/.test(code(P('netlify', 'functions', 'data-proxy.js'))));

/* Every Function App surface that accepts the header must allow it through
   CORS, or the browser silently never sends it and the endpoint reports a
   missing key for a caller that supplied one. */
for (const f of ['index.js', 'agent.js', 'agent-suggest-criteria.js', 'user-anthropic-key.js']) {
  const src = read('azure-function', 'src', f);
  if (!/Access-Control-Allow-Headers/.test(src)) continue;
  check(f + ' allows x-anthropic-key through CORS',
    /Access-Control-Allow-Headers[^\n]*x-anthropic-key/.test(src));
}

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
