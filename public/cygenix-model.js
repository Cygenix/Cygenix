/* ============================================================================
   cygenix-model.js — which Claude model the console uses, and what to do when
   it stops existing.
   ----------------------------------------------------------------------------
   THE FAILURE THIS FIXES

   Claude model ids are pinned snapshots, not evergreen names. Each is retired
   eventually, with notice. `claude-sonnet-4-20250514` was pinned in twenty
   places across this codebase; when it retired, every AI feature answered

       Anthropic API error (404): model: claude-sonnet-4-20250514

   and the surrounding code told the operator to check ANTHROPIC_API_KEY —
   which was fine, and which sent them looking in the wrong place entirely.

   THE SHAPE OF THE FIX HERE

   The uploaded reference design assumed an Express app with a writable disk:
   a settings JSON file and a mounted router. This console has neither. The
   browser calls api.anthropic.com directly with the operator's own key
   (anthropic-dangerous-direct-browser-access), and the server halves are
   Netlify Edge (Deno) and Azure Functions — serverless, no durable local
   disk. So:

     - the model lives in ONE place per surface: this module in the browser,
       ANTHROPIC_MODEL in each function's environment;
     - the operator's override rides in cygenix_app_prefs, which already
       syncs like every other preference;
     - a retirement degrades down a fallback chain instead of failing;
     - a 404 is reported as a retired model, never as a credentials problem.

   The error taxonomy and the resolve/fallback logic are the reference
   design's, which were the portable parts.

   Node-requirable so the rules are tested without a browser.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object') root.CygenixModel = api;
})(typeof window !== 'undefined' ? window : this, function () {
'use strict';

var PREFS_KEY = 'cygenix_app_prefs';
var API_BASE = 'https://api.anthropic.com';
var API_VERSION = '2023-06-01';

/* The current models, most capable first. This list is the fallback when the
   live /v1/models call cannot be made (no key yet, offline, panel not opened);
   it is NOT the authority. The panel reads the real list from the API, because
   a hardcoded catalogue is the same mistake as a hardcoded model, one level up. */
var KNOWN = [
  { id: 'claude-opus-5',              label: 'Claude Opus 5',   note: 'most capable' },
  { id: 'claude-sonnet-5',            label: 'Claude Sonnet 5', note: 'balanced — the default' },
  { id: 'claude-fable-5',             label: 'Claude Fable 5',  note: 'writing' },
  { id: 'claude-haiku-4-5-20251001',  label: 'Claude Haiku 4.5', note: 'fastest, cheapest' },
];

/* Sonnet for the primary, Haiku behind it: a retirement should degrade to
   something cheap and fast that still answers, not to nothing. */
var DEFAULT_PRIMARY = 'claude-sonnet-5';
var DEFAULT_FALLBACKS = ['claude-haiku-4-5-20251001'];

/* Retired ids that must never be written back into settings. Anything still
   carrying one is a config from before this module existed. */
var RETIRED = ['claude-sonnet-4-20250514', 'claude-opus-4-20250514',
  'claude-3-5-sonnet-20240620', 'claude-3-opus-20240229', 'claude-sonnet-4-5', 'claude-sonnet-4'];

/* ── settings ───────────────────────────────────────────────────────────── */

function readPrefs() {
  try {
    if (typeof localStorage === 'undefined') return {};
    return JSON.parse(localStorage.getItem(PREFS_KEY) || '{}') || {};
  } catch (e) { return {}; }
}

/* A stored model that has since been retired is ignored, not obeyed: honouring
   it would reproduce the outage this module exists to end. */
function mdSettings() {
  var p = readPrefs().model || {};
  var primary = typeof p.primary === 'string' ? p.primary.trim() : '';
  var fallbacks = Array.isArray(p.fallbacks) ? p.fallbacks.slice() : null;
  var stale = primary && RETIRED.indexOf(primary) > -1;
  return {
    primary: (!primary || stale) ? DEFAULT_PRIMARY : primary,
    fallbacks: fallbacks ? fallbacks.filter(function (m) {
      return m && RETIRED.indexOf(m) === -1 && m !== primary;
    }) : DEFAULT_FALLBACKS.slice(),
    updatedAt: p.updatedAt || null,
    updatedBy: p.updatedBy || null,
    /* true when a saved primary was dropped for being retired — the panel
       says so rather than silently showing a model nobody chose */
    supersededPrimary: stale ? primary : null,
  };
}

function mdSaveSettings(next, user, now) {
  var primary = String((next && next.primary) || '').trim();
  if (!primary) throw new Error('A primary model is required.');
  var fallbacks = ((next && next.fallbacks) || [])
    .map(function (m) { return String(m).trim(); })
    .filter(function (m) { return m && m !== primary; });
  var prefs = readPrefs();
  prefs.model = { primary: primary, fallbacks: fallbacks,
    updatedAt: now || 0, updatedBy: user || null };
  try {
    if (typeof localStorage !== 'undefined') localStorage.setItem(PREFS_KEY, JSON.stringify(prefs));
  } catch (e) { /* quota — the in-memory value still applies for this session */ }
  return prefs.model;
}

function mdPrimary() { return mdSettings().primary; }
function mdChain() {
  var s = mdSettings();
  return [s.primary].concat(s.fallbacks);
}

/* ── error taxonomy ─────────────────────────────────────────────────────────
   The whole point of this file: a 404 means the MODEL is gone. It has never
   meant the key is wrong, and saying so cost an afternoon. ─────────────── */

function mdIsModelNotFound(status, json) {
  if (status !== 404) return false;
  var type = (json && json.error && json.error.type) || '';
  var msg = (json && json.error && json.error.message) || '';
  return type === 'not_found_error' || /\bmodel\b/i.test(msg);
}

function mdMapError(status, json, model) {
  var msg = (json && json.error && json.error.message) || '';
  if (status === 401 || status === 403) {
    return { code: 'auth', retryable: false,
      userMessage: 'The assistant is not configured correctly — the API key was rejected.',
      adminHint: 'The Anthropic API key is missing, invalid, revoked, or lacks permission. This is NOT a model problem.',
      logMessage: 'Anthropic auth failure (' + status + '): ' + msg };
  }
  if (mdIsModelNotFound(status, json)) {
    return { code: 'model_not_found', retryable: false,
      userMessage: 'The assistant is unavailable while its model setting is updated.',
      adminHint: 'Model "' + (model || 'configured') + '" does not exist for this API key — it has most likely been retired. '
        + 'Pick a current model in Settings → Co-Worker Model. Do not change the API key.',
      logMessage: 'Model unavailable (404): ' + msg };
  }
  if (status === 404) {
    return { code: 'not_found', retryable: false,
      userMessage: 'The assistant is temporarily unavailable.',
      adminHint: 'A 404 that is not a model error — check the request URL.',
      logMessage: 'Anthropic 404: ' + msg };
  }
  if (status === 400) {
    return { code: 'invalid_request', retryable: false,
      userMessage: 'That request could not be processed.',
      adminHint: 'Malformed request — check max_tokens, roles and content blocks.',
      logMessage: 'Invalid request (400): ' + msg };
  }
  if (status === 413) {
    return { code: 'too_large', retryable: false,
      userMessage: 'That conversation is too long. Start a new one or shorten the input.',
      adminHint: 'Request exceeded the size limit — trim history before sending.',
      logMessage: 'Request too large (413): ' + msg };
  }
  if (status === 429) {
    return { code: 'rate_limit', retryable: true,
      userMessage: 'The assistant is busy right now. Try again in a moment.',
      adminHint: 'Rate limit or quota exhausted — check usage limits and billing.',
      logMessage: 'Rate limited (429): ' + msg };
  }
  if (status === 529) {
    return { code: 'overloaded', retryable: true,
      userMessage: 'The assistant is temporarily overloaded. Try again shortly.',
      adminHint: 'Upstream overloaded (529) — transient, retry with backoff.',
      logMessage: 'Overloaded (529): ' + msg };
  }
  if (status >= 500) {
    return { code: 'upstream', retryable: true,
      userMessage: 'The assistant is temporarily unavailable. Try again shortly.',
      adminHint: 'Upstream error ' + status + ' — transient.',
      logMessage: 'Upstream error (' + status + '): ' + msg };
  }
  return { code: 'unknown', retryable: false,
    userMessage: 'Something went wrong reaching the assistant.',
    adminHint: 'Unexpected status ' + status + '.',
    logMessage: 'Unexpected status ' + status + ': ' + msg };
}

/* ── the live model list ────────────────────────────────────────────────── */

function mdListModels(apiKey, fetchImpl) {
  var f = fetchImpl || (typeof fetch !== 'undefined' ? fetch : null);
  if (!f) return Promise.reject(new Error('No fetch available'));
  if (!apiKey) return Promise.reject(new Error('No API key — add one in Settings to check the model list.'));
  var models = [];
  function page(afterId, depth) {
    var url = API_BASE + '/v1/models?limit=100' + (afterId ? '&after_id=' + encodeURIComponent(afterId) : '');
    return f(url, { headers: { 'x-api-key': apiKey, 'anthropic-version': API_VERSION,
      'anthropic-dangerous-direct-browser-access': 'true' } })
      .then(function (res) {
        return res.text().then(function (t) {
          var json = null;
          try { json = t ? JSON.parse(t) : null; } catch (e) { /* non-JSON */ }
          if (!res.ok) {
            var m = mdMapError(res.status, json);
            var e = new Error(m.adminHint);
            e.mapped = m;
            throw e;
          }
          var batch = (json && json.data) || [];
          batch.forEach(function (b) { models.push({ id: b.id, label: b.display_name || b.id }); });
          if (json && json.has_more && batch.length && depth < 10) {
            return page(batch[batch.length - 1].id, depth + 1);
          }
          return models;
        });
      });
  }
  return page(null, 0);
}

/* ── resolution ─────────────────────────────────────────────────────────────
   available: the ids from the live list, or null when it could not be read.
   source: 'primary'    — configured model, confirmed live
           'fallback'   — primary is gone, running on a backup (alert-worthy)
           'unverified' — list unreadable; using the primary anyway
           'none'       — nothing in the chain exists (hard failure) ─────── */

function mdResolve(available) {
  var s = mdSettings();
  var chain = [s.primary].concat(s.fallbacks).filter(Boolean);
  var warnings = [];
  if (s.supersededPrimary) {
    warnings.push('Saved model "' + s.supersededPrimary + '" is retired — using "' + s.primary + '" instead.');
  }
  if (!available) {
    return { model: chain[0] || null, source: 'unverified', chain: chain, warnings: warnings };
  }
  var live = chain.filter(function (m) { return available.indexOf(m) > -1; });
  if (!live.length) {
    /* Deliberately not phrased as "…for this API key". The key is what the last
       person went and rotated; the models are what is actually wrong. */
    warnings.push('None of the configured models are available: ' + chain.join(', ')
      + '. They have most likely been retired.');
    return { model: chain[0] || null, source: 'none', chain: chain, warnings: warnings };
  }
  var model = live[0];
  if (model !== s.primary) {
    warnings.push('Primary model "' + s.primary + '" is not available — running on fallback "' + model + '".');
  }
  chain.filter(function (m) { return available.indexOf(m) === -1; }).forEach(function (m) {
    warnings.push('Configured model "' + m + '" is not available (likely retired).');
  });
  return { model: model, source: model === s.primary ? 'primary' : 'fallback',
    chain: chain, warnings: warnings };
}

/* ── calling ────────────────────────────────────────────────────────────────
   Posts to /v1/messages with the resolved model and walks down the chain if a
   model turns out to be retired mid-flight. Resolves with
   { model, degraded, tried, response } — response is the raw Response, so a
   streaming caller can read the body itself. Rejects with err.mapped set. ── */

function mdCall(body, opts) {
  var o = opts || {};
  var f = o.fetch || (typeof fetch !== 'undefined' ? fetch : null);
  if (!f) return Promise.reject(new Error('No fetch available'));
  var key = o.apiKey;
  if (!key) {
    var e = new Error('No Anthropic API key configured.');
    e.mapped = mdMapError(401, { error: { message: 'no key' } });
    return Promise.reject(e);
  }
  var settings = mdSettings();
  var chain = o.allowFallback === false ? [settings.primary] : mdChain();
  var tried = [];

  function attempt(i) {
    if (i >= chain.length) {
      var m = mdMapError(404, { error: { type: 'not_found_error', message: 'all models exhausted' } }, chain[0]);
      m.adminHint = 'Every configured model failed as unavailable (tried ' + tried.join(', ')
        + '). Pick a current model in Settings → Co-Worker Model.';
      var err = new Error(m.logMessage);
      err.mapped = m;
      return Promise.reject(err);
    }
    var model = chain[i];
    var payload = {};
    for (var k in body) if (Object.prototype.hasOwnProperty.call(body, k)) payload[k] = body[k];
    payload.model = model;
    return f(API_BASE + '/v1/messages', {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-api-key': key,
        'anthropic-version': API_VERSION, 'anthropic-dangerous-direct-browser-access': 'true' },
      body: JSON.stringify(payload),
    }).then(function (res) {
      if (res.ok) {
        return { model: model, degraded: model !== settings.primary, tried: tried, response: res };
      }
      return res.text().then(function (t) {
        var json = null;
        try { json = t ? JSON.parse(t) : null; } catch (e2) { /* non-JSON */ }
        var mapped = mdMapError(res.status, json, model);
        if (mapped.code === 'model_not_found') {
          tried.push(model);
          return attempt(i + 1);        // retired — try the next one down
        }
        var err2 = new Error(mapped.logMessage);
        err2.mapped = mapped;
        err2.model = model;
        throw err2;
      });
    });
  }
  return attempt(0);
}

/* One-token liveness probe for a single model, for the settings panel. */
function mdTestModel(model, apiKey, fetchImpl) {
  var f = fetchImpl || (typeof fetch !== 'undefined' ? fetch : null);
  var started = Date.now();
  return f(API_BASE + '/v1/messages', {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-api-key': apiKey,
      'anthropic-version': API_VERSION, 'anthropic-dangerous-direct-browser-access': 'true' },
    body: JSON.stringify({ model: model, max_tokens: 1, messages: [{ role: 'user', content: 'ping' }] }),
  }).then(function (res) {
    var ms = Date.now() - started;
    return res.text().then(function (t) {
      if (res.ok) return { ok: true, model: model, ms: ms };
      var json = null;
      try { json = t ? JSON.parse(t) : null; } catch (e) { /* non-JSON */ }
      var mapped = mdMapError(res.status, json, model);
      return { ok: false, model: model, ms: ms, status: res.status,
        code: mapped.code, detail: mapped.adminHint };
    });
  });
}

return {
  KNOWN: KNOWN, RETIRED: RETIRED,
  DEFAULT_PRIMARY: DEFAULT_PRIMARY, DEFAULT_FALLBACKS: DEFAULT_FALLBACKS,
  PREFS_KEY: PREFS_KEY,
  mdSettings: mdSettings, mdSaveSettings: mdSaveSettings,
  mdPrimary: mdPrimary, mdChain: mdChain,
  mdIsModelNotFound: mdIsModelNotFound, mdMapError: mdMapError,
  mdListModels: mdListModels, mdResolve: mdResolve,
  mdCall: mdCall, mdTestModel: mdTestModel,
  /* the short name the twenty call sites use */
  primary: mdPrimary,
};
});
