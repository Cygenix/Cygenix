// Tests for the Claude model configuration — cygenix-model.js and its call sites.
//
// THE FAILURE THESE PIN
//
// Claude model ids are pinned snapshots, not evergreen names, and each one is
// retired eventually. `claude-sonnet-4-20250514` was hardcoded in twenty-odd
// places here; when it retired every AI feature answered
//
//     Anthropic API error (404): model: claude-sonnet-4-20250514
//
// and the code around it told the operator to check ANTHROPIC_API_KEY. The key
// was fine. The wrong diagnosis cost more than the outage.
//
// So there are two kinds of test below. The first kind pins the RULES: a 404 is
// a retired model and never a credentials problem, an auth failure stops rather
// than burning the fallback chain, a stored model that has since retired is
// ignored instead of obeyed. The second kind pins the WIRING: no pinned model id
// survives anywhere in the source, every page that resolves a model loads the
// module that resolves it, and each server surface reads its model from the
// environment. Rules that hold while one call site still carries a literal would
// show green through the next retirement.
'use strict';
const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};
const P = (...p) => path.join(__dirname, '..', ...p);
const read = (...p) => fs.readFileSync(P(...p), 'utf8');

/* A localStorage stand-in, so the settings half of the module is testable
   without a browser. It has to exist before the module is required. */
const store = {};
global.localStorage = {
  getItem: (k) => (Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null),
  setItem: (k, v) => { store[k] = String(v); },
  removeItem: (k) => { delete store[k]; },
};
const setPrefs = (model) => {
  if (model === null) delete store.cygenix_app_prefs;
  else store.cygenix_app_prefs = JSON.stringify({ model: model });
};

const M = require('../public/cygenix-model.js');
const RETIRED_ID = 'claude-sonnet-4-20250514';   // the id from the actual outage

console.log('Claude model configuration — retirement is survivable\n');

/* ── 1. Error taxonomy — the actual bug ─────────────────────────────────────
   A 404 from /v1/messages means the model is gone. It has never meant the key
   is wrong, and the user-facing half must not say so either. */

const notFound = M.mdMapError(404,
  { error: { type: 'not_found_error', message: 'model: ' + RETIRED_ID } }, RETIRED_ID);
check('404 maps to model_not_found', notFound.code === 'model_not_found', notFound.code);
check('the 404 admin hint says the model is retired', /retired/i.test(notFound.adminHint));
check('the 404 admin hint says explicitly not to change the API key',
  /do not change the api key/i.test(notFound.adminHint), notFound.adminHint);
check('the 404 admin hint names where to fix it',
  /Settings → Co-Worker Model/.test(notFound.adminHint), notFound.adminHint);
check('the 404 user message never mentions the API key',
  !/api.?key/i.test(notFound.userMessage), notFound.userMessage);
check('the 404 user message never leaks the model id',
  !notFound.userMessage.includes(RETIRED_ID), notFound.userMessage);
check('a 404 whose message mentions a model is a model error even without the type',
  M.mdIsModelNotFound(404, { error: { message: 'model: whatever' } }) === true);
check('a 404 that is not about a model is not a model error',
  M.mdMapError(404, { error: { type: 'invalid_request_error', message: 'no route' } }).code === 'not_found');

const auth401 = M.mdMapError(401, { error: { message: 'invalid x-api-key' } });
check('401 maps to auth', auth401.code === 'auth', auth401.code);
check('the auth hint says it is NOT a model problem',
  /NOT a model problem/.test(auth401.adminHint), auth401.adminHint);
check('403 maps to auth as well', M.mdMapError(403, {}).code === 'auth');

check('429 is retryable', M.mdMapError(429, {}).retryable === true);
check('529 is retryable', M.mdMapError(529, {}).retryable === true);
check('500 is retryable', M.mdMapError(500, {}).retryable === true);
check('400 is not retryable', M.mdMapError(400, {}).retryable === false);
check('404 is not retryable', notFound.retryable === false);
check('413 tells the user to shorten the conversation',
  M.mdMapError(413, {}).code === 'too_large' && /shorten|too long/i.test(M.mdMapError(413, {}).userMessage));
check('every mapped error carries user text, an admin hint and a log line',
  [400, 401, 403, 404, 413, 429, 500, 529, 418].every((s) => {
    const m = M.mdMapError(s, {});
    return m.code && m.userMessage && m.adminHint && m.logMessage;
  }));

/* ── 2. Settings — a retired id is ignored, not obeyed ─────────────────────── */

setPrefs(null);
check('with nothing stored the primary is the current default',
  M.mdPrimary() === M.DEFAULT_PRIMARY && M.DEFAULT_PRIMARY === 'claude-sonnet-5', M.mdPrimary());
check('with nothing stored the chain is primary then fallback',
  JSON.stringify(M.mdChain()) === JSON.stringify([M.DEFAULT_PRIMARY].concat(M.DEFAULT_FALLBACKS)),
  M.mdChain().join(', '));

setPrefs({ primary: RETIRED_ID, fallbacks: ['claude-haiku-4-5-20251001'] });
const stale = M.mdSettings();
check('a stored primary that has been retired is ignored',
  stale.primary === M.DEFAULT_PRIMARY, stale.primary);
check('the panel is told which retired model was dropped',
  stale.supersededPrimary === RETIRED_ID, String(stale.supersededPrimary));
check('a retired id never survives into the chain',
  M.mdChain().indexOf(RETIRED_ID) === -1, M.mdChain().join(', '));

setPrefs({ primary: 'claude-opus-5', fallbacks: ['claude-opus-5', RETIRED_ID, 'claude-sonnet-5'] });
const cleaned = M.mdSettings();
check('the primary is not repeated in its own fallbacks',
  cleaned.fallbacks.indexOf('claude-opus-5') === -1, cleaned.fallbacks.join(', '));
check('retired ids are filtered out of stored fallbacks',
  cleaned.fallbacks.indexOf(RETIRED_ID) === -1, cleaned.fallbacks.join(', '));
check('a healthy stored primary is honoured', cleaned.primary === 'claude-opus-5', cleaned.primary);

const saved = M.mdSaveSettings(
  { primary: 'claude-opus-5', fallbacks: ['claude-opus-5', ' claude-sonnet-5 ', ''] },
  'demo@cygenix', 1700000000000);
check('saving de-duplicates the primary out of the fallbacks',
  JSON.stringify(saved.fallbacks) === JSON.stringify(['claude-sonnet-5']), saved.fallbacks.join(', '));
check('saving records who changed it and when',
  saved.updatedBy === 'demo@cygenix' && saved.updatedAt === 1700000000000);
check('saving round-trips through the shared prefs key',
  JSON.parse(store[M.PREFS_KEY]).model.primary === 'claude-opus-5');
check('saving without a primary is refused', (() => {
  try { M.mdSaveSettings({ primary: '  ' }); return false; } catch (e) { return true; }
})());
check('the model settings ride in cygenix_app_prefs, which already syncs',
  M.PREFS_KEY === 'cygenix_app_prefs', M.PREFS_KEY);

/* ── 3. Resolution — the retirement is detected before a user hits it ──────── */

setPrefs({ primary: RETIRED_ID, fallbacks: ['claude-sonnet-5', 'claude-haiku-4-5-20251001'] });
const LIVE = ['claude-opus-5', 'claude-sonnet-5', 'claude-fable-5', 'claude-haiku-4-5-20251001'];
const rStale = M.mdResolve(LIVE);
check('a retired stored primary resolves to a live model, not to nothing',
  LIVE.indexOf(rStale.model) > -1, rStale.model);
check('resolution warns that the saved model is retired',
  rStale.warnings.some((w) => /retired/i.test(w)), rStale.warnings.join(' | '));

setPrefs({ primary: 'claude-not-a-model', fallbacks: ['claude-sonnet-5'] });
const rFallback = M.mdResolve(LIVE);
check('a primary that is gone resolves down to the fallback',
  rFallback.source === 'fallback' && rFallback.model === 'claude-sonnet-5',
  rFallback.source + '/' + rFallback.model);
check('running on a fallback is reported as such, not silently',
  rFallback.warnings.some((w) => /not available/i.test(w)), rFallback.warnings.join(' | '));

setPrefs({ primary: 'claude-sonnet-5', fallbacks: ['claude-haiku-4-5-20251001'] });
const rOk = M.mdResolve(LIVE);
check('a healthy configuration reports source=primary with no warnings',
  rOk.source === 'primary' && rOk.model === 'claude-sonnet-5' && rOk.warnings.length === 0,
  rOk.source + '/' + rOk.warnings.join(' | '));

const rBlind = M.mdResolve(null);
check('an unreadable model list degrades to unverified, not a crash',
  rBlind.source === 'unverified' && rBlind.model === 'claude-sonnet-5', rBlind.source);

const rNone = M.mdResolve(['claude-something-else']);
check('nothing in the chain alive resolves to source=none',
  rNone.source === 'none', rNone.source);
check('source=none says none of the configured models are available',
  rNone.warnings.some((w) => /none of the configured models/i.test(w)), rNone.warnings.join(' | '));
// The last operator to see this went and rotated the key. No warning the panel
// can show is allowed to point there — it is always the model setting.
check('no resolution warning mentions the API key',
  [rNone, rFallback, rStale, rBlind].every((r) => r.warnings.every((w) => !/api.?key/i.test(w))),
  [].concat(rNone.warnings, rFallback.warnings, rStale.warnings).join(' | '));

/* ── 4. Calling — a request still succeeds while degraded ───────────────────
   A scripted fetch, so the chain-walking is tested without a network or a key. */

const calls = [];
const respond = (status, body) => Promise.resolve({
  ok: status >= 200 && status < 300, status,
  text: () => Promise.resolve(JSON.stringify(body)),
});
const fakeFetch = (script) => (url, init) => {
  const model = JSON.parse(init.body).model;
  calls.push(model);
  const b = script[model];
  if (b === 'ok') return respond(200, { content: [] });
  if (b === 'auth') return respond(401, { error: { type: 'authentication_error', message: 'invalid x-api-key' } });
  if (b === 'overloaded') return respond(529, { error: { type: 'overloaded_error', message: 'Overloaded' } });
  return respond(404, { error: { type: 'not_found_error', message: 'model: ' + model } });
};
const body = { max_tokens: 16, messages: [{ role: 'user', content: 'hi' }] };

const results = [];
const t = (label, promise, verdict) => results.push(
  promise.then((v) => check(label, verdict(null, v)), (e) => check(label, verdict(e, null), e && e.message)));

// The primary is dead; the fallback answers. The dead model is tried once, the
// live one answers, and the caller is told the answer is degraded.
setPrefs({ primary: 'claude-dead-5', fallbacks: ['claude-sonnet-5'] });
calls.length = 0;
t('a call falls through the chain to a live model',
  M.mdCall(body, { apiKey: 'sk-test', fetch: fakeFetch({ 'claude-sonnet-5': 'ok' }) }),
  (err, v) => !err && v.model === 'claude-sonnet-5' && v.degraded === true);

t('a degraded call reports which dead model it stepped over',
  M.mdCall(body, { apiKey: 'sk-test', fetch: fakeFetch({ 'claude-sonnet-5': 'ok' }) }),
  (err, v) => !err && v.tried.length === 1 && v.tried[0] === 'claude-dead-5');

// An auth failure is not a retirement: stop, do not burn the rest of the chain
// re-proving that the same rejected key is still rejected.
const authCalls = [];
const authFetch = (url, init) => {
  authCalls.push(JSON.parse(init.body).model);
  return respond(401, { error: { type: 'authentication_error', message: 'invalid x-api-key' } });
};
setPrefs({ primary: 'claude-sonnet-5', fallbacks: ['claude-haiku-4-5-20251001'] });
t('an auth error aborts immediately instead of burning the chain',
  M.mdCall(body, { apiKey: 'sk-test', fetch: authFetch }),
  (err) => !!err && err.mapped && err.mapped.code === 'auth' && authCalls.length === 1);

// Everything dead: the caller gets an actionable hint, not a shrug.
setPrefs({ primary: 'claude-dead-a', fallbacks: ['claude-dead-b'] });
t('every model dead throws with an actionable admin hint',
  M.mdCall(body, { apiKey: 'sk-test', fetch: fakeFetch({}) }),
  (err) => !!err && /Settings → Co-Worker Model/.test(err.mapped.adminHint)
    && /claude-dead-a/.test(err.mapped.adminHint) && /claude-dead-b/.test(err.mapped.adminHint));

// A transient failure is not a retirement either — do not silently downgrade
// the model because the good one was busy for a second.
const busyCalls = [];
const busyFetch = (url, init) => {
  busyCalls.push(JSON.parse(init.body).model);
  return respond(529, { error: { type: 'overloaded_error', message: 'Overloaded' } });
};
setPrefs({ primary: 'claude-sonnet-5', fallbacks: ['claude-haiku-4-5-20251001'] });
t('an overload does not walk the chain — it is retryable, not retired',
  M.mdCall(body, { apiKey: 'sk-test', fetch: busyFetch }),
  (err) => !!err && err.mapped.code === 'overloaded' && err.mapped.retryable === true && busyCalls.length === 1);

t('a call with no API key fails as auth, not as a model problem',
  M.mdCall(body, { fetch: fakeFetch({}) }),
  (err) => !!err && err.mapped.code === 'auth');

setPrefs({ primary: 'claude-dead-5', fallbacks: ['claude-sonnet-5'] });
t('allowFallback:false pins the primary and does not degrade behind the caller',
  M.mdCall(body, { apiKey: 'sk-test', allowFallback: false, fetch: fakeFetch({ 'claude-sonnet-5': 'ok' }) }),
  (err) => !!err && err.mapped.code === 'model_not_found');

/* ── 5. Wiring — no pinned model id survives in the source ─────────────────── */

const skip = /(^|\/)(node_modules|\.git|dist|build|coverage)(\/|$)/;
const walk = (dir, out) => {
  for (const name of fs.readdirSync(dir)) {
    const full = path.join(dir, name);
    const rel = path.relative(P(), full);
    if (skip.test(rel)) continue;
    const st = fs.statSync(full);
    if (st.isDirectory()) walk(full, out);
    else if (/\.(js|html|md|json|ts|yml|yaml)$/.test(name)) out.push(rel);
  }
  return out;
};
const files = walk(P(), []);
const SELF = ['public/cygenix-model.js', 'tests/model-config.test.js'];

// A model id is allowed to appear in exactly two shapes: inside this module's
// own catalogue, and as the default beside an environment variable — a server
// surface has to name something when ANTHROPIC_MODEL is unset. Any other
// quoted id is a call site that will break on its own schedule. (Prose and
// comments may name a model freely; naming the dead one is how the next person
// finds out what happened.)
const ENV_DEFAULT = /(?:process\.env\.[A-Z_]+|Deno\.env\.get\('[A-Z_]+'\))[^\n]*?\|\|\s*'[^']*'/g;
const executable = (rel) => fs.readFileSync(P(rel), 'utf8').replace(ENV_DEFAULT, '');

const datedPins = [];
for (const rel of files) {
  if (SELF.includes(rel)) continue;
  const hits = executable(rel).match(/['"]claude-[a-z0-9.-]*-20\d{6}['"]/g);
  if (hits) datedPins.push(rel + ': ' + [...new Set(hits)].join(', '));
}
check('no dated model snapshot is pinned outside cygenix-model.js and the env defaults',
  datedPins.length === 0, datedPins.join(' | '));

const retiredPins = [];
for (const rel of files) {
  if (SELF.includes(rel)) continue;
  const src = executable(rel);
  const hits = M.RETIRED.filter((id) => new RegExp("['\"]" + id + "['\"]").test(src));
  if (hits.length) retiredPins.push(rel + ': ' + hits.join(', '));
}
check('no retired model id is passed as a value anywhere in the source',
  retiredPins.length === 0, retiredPins.join(' | '));

// The env defaults themselves have to be current, or the fix ships already broken.
const envDefaults = [];
for (const rel of files) {
  if (SELF.includes(rel)) continue;
  const src = fs.readFileSync(P(rel), 'utf8');
  for (const hit of src.match(ENV_DEFAULT) || []) {
    const id = (hit.match(/'(claude-[^']+)'/) || [])[1];
    if (id && M.RETIRED.includes(id)) envDefaults.push(rel + ': ' + id);
  }
}
check('no environment default names a retired model', envDefaults.length === 0, envDefaults.join(' | '));

// Client pages resolve through the module; each one has to load it.
const clientFiles = files.filter((f) => f.startsWith('public/'));
const usesEngine = clientFiles.filter((f) => /CygenixModel\.primary\(\)/.test(fs.readFileSync(P(f), 'utf8')));
check('the client resolves its model through CygenixModel in many places',
  usesEngine.length >= 10, usesEngine.length + ' file(s)');

const pageFor = (f) => (f.endsWith('.html') ? [f] : clientFiles.filter((h) =>
  h.endsWith('.html') && fs.readFileSync(P(h), 'utf8').includes('/' + path.basename(f))));
const unloaded = [];
for (const f of usesEngine) {
  const pages = pageFor(f);
  if (!pages.length) { unloaded.push(f + ' (no page loads it)'); continue; }
  for (const p of pages) {
    if (!/<script[^>]+src="\/cygenix-model\.js/.test(fs.readFileSync(P(p), 'utf8'))) {
      unloaded.push(p + ' → ' + f);
    }
  }
}
check('every page that resolves a model loads /cygenix-model.js',
  unloaded.length === 0, unloaded.join(' | '));

check('a page that never resolves a model is not made to load the module',
  !/<script[^>]+src="\/cygenix-model\.js/.test(read('public', 'login.html')));

// Anthropic calls in the client go through the module rather than round their own way.
const clientPins = [];
for (const f of clientFiles) {
  if (SELF.includes(f)) continue;
  const src = fs.readFileSync(P(f), 'utf8');
  const hits = src.match(/model:\s*['"]claude-[^'"]*['"]/g);
  if (hits) clientPins.push(f + ': ' + hits.join(', '));
}
check('no client file passes a literal model id to the API',
  clientPins.length === 0, clientPins.join(' | '));

/* ── 6. Wiring — the server surfaces read their model from the environment ─── */

/* The two edge functions used to be checked here for reading ANTHROPIC_MODEL
   and sending the resolved id. Both were DISABLED on 2026-08-28: they read a
   Cygenix-owned ANTHROPIC_API_KEY from the Netlify environment and spent the
   site owner's personal Anthropic account on behalf of any signed-in user.
   They now answer 503 and make no model call at all, so there is no model to
   check — what there is to check is that they stay silent, and
   tests/anthropic-billing.test.js owns that. */
const edgeCoworker = read('netlify', 'edge-functions', 'coworker.js');
const edgeAnalyse = read('netlify', 'edge-functions', 'analyse.js');
for (const [label, src] of [['coworker', edgeCoworker], ['analyse', edgeAnalyse]]) {
  check('the ' + label + ' edge function makes no model call to pin a model for',
    !/api\.anthropic\.com/.test(src) && !/model:\s*MODEL/.test(src));
}

const azureFiles = ['index.js', 'agent.js', 'profile-builder.js', 'agent-suggest-criteria.js'];
for (const name of azureFiles) {
  const src = read('azure-function', 'src', name);
  if (!/api\.anthropic\.com/.test(src) && !/model:/.test(src)) continue;
  check('azure ' + name + ' reads its model from process.env',
    /process\.env\.ANTHROPIC_MODEL(_FAST)?\s*\|\|/.test(src));
  check('azure ' + name + ' passes no literal model id',
    !/model:\s*['"]claude-/.test(src),
    (src.match(/model:\s*['"]claude-[^'"]*['"]/g) || []).join(', '));
}
check('the fast path has its own env var so it is not forced onto the big model',
  /ANTHROPIC_MODEL_FAST/.test(read('azure-function', 'src', 'agent-suggest-criteria.js')));

/* ── 7. The settings panel exists and is wired to the view ─────────────────── */

const dash = read('public', 'dashboard.html');
const dashApp = read('public', 'dashboard-app.js');
check('the Settings page has a Co-Worker model panel', dash.includes('id="cygmm"'));
for (const id of ['cygmm-status', 'cygmm-primary', 'cygmm-chain', 'cygmm-save', 'cygmm-test', 'cygmm-refresh']) {
  check('the panel has #' + id, dash.includes('id="' + id + '"'));
}
check('the panel is rendered when the settings view opens',
  /cygmmRender\(\)/.test(dashApp) && /project-settings[\s\S]{0,200}cygmmRender\(\)/.test(dashApp));
check('the panel saves through the engine rather than writing prefs itself',
  /mdSaveSettings\(/.test(dashApp)
  && /function cygmmModel\(\)\s*\{\s*return window\.CygenixModel;/.test(dashApp)
  && !/cygenix_app_prefs/.test(dashApp.slice(dashApp.indexOf('function cygmmModel'))));
check('the panel reads the live model list rather than a hardcoded catalogue',
  /mdListModels\(/.test(dashApp));
check('the panel probes a model before trusting it',
  /mdTestModel\(/.test(dashApp));
check('dashboard.html loads the model module', /<script[^>]+src="\/cygenix-model\.js/.test(dash));

Promise.all(results).then(() => {
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
});
