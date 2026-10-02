// table-classifier.js
//
// POST /api/agent/table-classify/start     { modules, chunks }          → { batchId }
// POST /api/agent/table-classify/status    { batchIds }                 → { batches }
// POST /api/agent/table-classify/results   { batchId, modules, chunks } → { chunks }
// POST /api/agent/table-classify/cancel    { batchIds }                 → { cancelled }
//
// THE ONE PLACE TABLES ARE SORTED INTO MODULES (Oct-2026)
// Conversion Templates' "Suggest all" and the per-module "Suggest" button
// both come here; the per-module button is simply a run with one module. The
// words Claude is given live in table-classifier-prompt.js, so the prompt can
// be refined without touching this file or the page.
//
// WHAT CHANGED, AND WHY
// The earlier version (template-suggest-tables.js, removed) asked in two
// steps: shortlist by table NAME, then rank each module's shortlist ALONE.
// Two weaknesses followed. A table with a cryptic name never reached the
// second step, so its columns were never looked at; and no call ever saw two
// modules side by side, so nothing weighed "Billing or Matters?" — each
// module grabbed what looked like its own. Now every table goes in, with its
// columns, keys and relationships, and every request carries the full list
// of modules, so each table is placed where it fits best among all of them.
//
// WHY THE BATCHES API
// Doing that for a whole database is many requests, each one a careful
// judgement — too slow for the 26 seconds Netlify allows the data proxy, and
// the browser reaches this Function App only through that proxy. The
// Message Batches API takes the whole run in one call and answers later:
// "start" submits it and returns at once; the page polls "status"; "results"
// collects a finished batch. No request here waits on Claude, so none comes
// near the proxy's limit — and Batches bill at half price, which pays for the
// stronger model this deserves.
//
// WHY NOTHING IS STORED
// The caller's own Anthropic key arrives in the x-anthropic-key header on
// every call (user-anthropic-key.js) and is used for that call only. The
// batch lives in the caller's own Anthropic account; the page keeps its id.
// This file keeps no state, writes nothing to Cosmos, and logs counts —
// never table names, notes, prompts or answers.
//
// TARGET-AGNOSTIC
// No table names, keywords or module→table maps here. Claude works from the
// live schema the page sends, the module names and the analyst's notes. And
// whatever Claude says, only tables and modules that were sent survive.

'use strict';

const { app } = require('@azure/functions');
const { userAnthropicKey } = require('./user-anthropic-key');
const { enforceAuth } = require('./entra-auth');
const P = require('./table-classifier-prompt');

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, x-anthropic-key',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type':                 'application/json',
};
const ok  = (body)      => ({ status: 200, headers: CORS, body: JSON.stringify(body) });
const bad = (code, msg) => ({ status: code, headers: CORS, body: JSON.stringify({ error: msg }) });
// The house rule for an unexpected failure: the message and the stack, so the
// person reading the network tab has something to act on.
const boom = (e) => ({ status: 500, headers: CORS,
  body: JSON.stringify({ error: (e && e.message) || String(e), stack: (e && e.stack) || '' }) });

const LIMITS = {
  MODULES: 60,              // modules per run
  CHUNKS: 250,              // requests per start call (the page splits bigger runs)
  TABLES_PER_CHUNK: 80,     // the page sends TABLES_PER_CHUNK (40); this is the ceiling
  COLUMNS: 150,             // columns per table written into the prompt
  RELATED: 25,              // FK neighbours per direction named in the prompt; the rest are counted
  BATCHES: 50,              // batch ids per status / cancel call
  NAME: 256,
  MODULE_NAME: 120,
  NOTES: 1200,
  REASON: 90,
};
const CUSTOM_ID = /^[A-Za-z0-9_-]{1,64}$/;

// ── The answer's shape, enforced by structured outputs ────────────────────
// Claude's reply must match this; the checks below then make sure every name
// in it is one we sent.
const OUTPUT_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['tables'],
  properties: {
    tables: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['table', 'modules', 'confidence', 'required', 'reason'],
        properties: {
          table:      { type: 'string' },
          modules:    { type: 'array', items: { type: 'string' } },
          confidence: { type: 'string', enum: ['high', 'medium', 'low'] },
          required:   { type: 'boolean' },
          reason:     { type: 'string' },
        },
      },
    },
  },
};

// ── Cleaning what the page sent ──────────────────────────────────────────
const str = (v, n) => String(v == null ? '' : v).replace(/\s+/g, ' ').trim().slice(0, n || LIMITS.NAME);

function cleanModules(list) {
  const out = [], seen = new Set();
  for (const m of Array.isArray(list) ? list : []) {
    const name = str(m && typeof m === 'object' ? (m.name || m.key) : m, LIMITS.MODULE_NAME);
    if (!name || seen.has(name.toLowerCase())) continue;
    seen.add(name.toLowerCase());
    out.push({ name, notes: str(m && m.notes, LIMITS.NOTES) });
  }
  return out;
}

function cleanTable(t) {
  if (!t || typeof t !== 'object') return null;
  const name = str(t.name);
  if (!name) return null;
  const rows = t.rows != null && t.rows !== '' && Number.isFinite(Number(t.rows)) ? Number(t.rows) : null;
  const cols = (Array.isArray(t.columns) ? t.columns : []).map(c => (c && typeof c === 'object')
    ? { name: str(c.name, 128), type: str(c.type || c.dataType, 40) } : { name: str(c, 128), type: '' })
    .filter(c => c.name);
  const names = (a) => (Array.isArray(a) ? a : []).map(x => str(x, 128)).filter(Boolean);
  const count = (v) => (Number.isFinite(Number(v)) && Number(v) > 0 ? Math.floor(Number(v)) : 0);
  // FK neighbours: at most RELATED named per direction, the rest as a count.
  // A hub table (one most of a subject area points at) can have hundreds of
  // "referenced by" names; listed in full they swamped the request and the
  // answer came back with most of the chunk missing (Oct-2026). The page
  // already caps and counts; this holds the line for any other caller.
  const refs = names(t.refs), refBy = names(t.refBy);
  return {
    name, rows,
    columns: cols,
    columnsRead: t.columnsRead !== false,
    pk: names(t.pk),
    refs: refs.slice(0, LIMITS.RELATED),
    refsMore: count(t.refsMore) + Math.max(0, refs.length - LIMITS.RELATED),
    refBy: refBy.slice(0, LIMITS.RELATED),
    refByMore: count(t.refByMore) + Math.max(0, refBy.length - LIMITS.RELATED),
  };
}

function cleanChunks(list) {
  const out = [], seen = new Set();
  for (const c of Array.isArray(list) ? list : []) {
    const id = String(c && c.id || '');
    if (!CUSTOM_ID.test(id) || seen.has(id)) return { error: 'Each chunk needs a unique id of letters, digits, - or _ (at most 64).' };
    seen.add(id);
    const tables = [], tseen = new Set();
    for (const t of Array.isArray(c.tables) ? c.tables : []) {
      const ct = cleanTable(t);
      if (!ct || tseen.has(ct.name.toLowerCase())) continue;
      tseen.add(ct.name.toLowerCase());
      tables.push(ct);
    }
    if (!tables.length) return { error: 'Chunk ' + id + ' has no tables.' };
    if (tables.length > LIMITS.TABLES_PER_CHUNK) return { error: 'At most ' + LIMITS.TABLES_PER_CHUNK + ' tables per chunk — chunk ' + id + ' has ' + tables.length + '.' };
    out.push({ id, tables });
  }
  return { chunks: out };
}

// ── The request Claude sees ──────────────────────────────────────────────
function chunkPrompt(modules, tables) {
  const mods = modules.map(m => '- "' + m.name + '"' + (m.notes ? ' — notes: ' + m.notes : '')).join('\n');
  const body = tables.map(t => {
    const cols = t.columns.slice(0, LIMITS.COLUMNS).map(c => c.name + (c.type ? ':' + c.type : '')).join(', ');
    const more = t.columns.length > LIMITS.COLUMNS ? ' (+' + (t.columns.length - LIMITS.COLUMNS) + ' more)' : '';
    return '### ' + t.name + (t.rows != null ? '  (' + t.rows + ' rows)' : '')
      + (t.pk.length ? '\n  primary key: ' + t.pk.join(', ') : '')
      + '\n  columns: ' + (t.columnsRead ? (cols || '(none)') + more : '(could not be read — judge from the name and relationships)')
      + (t.refs.length ? '\n  references: ' + t.refs.join(', ') + (t.refsMore ? ' (+' + t.refsMore + ' more)' : '') : '')
      + (t.refBy.length ? '\n  referenced by: ' + t.refBy.join(', ') + (t.refByMore ? ' (+' + t.refByMore + ' more)' : '') : '');
  }).join('\n\n');
  // The closing checklist names every table again: an answer that stops
  // early, or skips the ones that seemed obvious, has a list to check
  // itself against.
  return 'Modules (' + modules.length + '):\n' + mods
    + '\n\nTables in this batch (' + tables.length + '):\n\n' + body
    + '\n\nTables to answer (' + tables.length + '): ' + tables.map(t => t.name).join(', ')
    + '\n\nReturn exactly one entry for each of these ' + tables.length + ' tables — none skipped, none added.';
}

function batchRequest(modules, chunk) {
  return {
    custom_id: chunk.id,
    params: {
      model: P.MODEL,
      max_tokens: P.MAX_TOKENS,
      system: P.CLASSIFY_SYSTEM,
      messages: [{ role: 'user', content: chunkPrompt(modules, chunk.tables) }],
      output_config: { effort: P.EFFORT, format: { type: 'json_schema', schema: OUTPUT_SCHEMA } },
    },
  };
}

// ── Reading Claude's answer ───────────────────────────────────────────────
// Structured outputs should make this plain JSON; fences and prose are
// tolerated anyway, because a parse failure costs the person a retry.
function parseModelJson(text) {
  let t = String(text || '').trim();
  t = t.replace(/^```(?:json)?\s*/i, '').replace(/\s*```\s*$/i, '').trim();
  try { return JSON.parse(t); } catch (e) { /* fall through */ }
  const i = t.indexOf('{'), j = t.lastIndexOf('}');
  if (i !== -1 && j > i) { try { return JSON.parse(t.slice(i, j + 1)); } catch (e) { /* below */ } }
  const err = new Error('Claude did not return usable JSON for this batch of tables');
  err.code = 'bad-json';
  throw err;
}

const CONF = new Set(['high', 'medium', 'low']);
// Keep only tables we sent and modules we sent, spelled as we sent them.
// Tables Claude left out are reported as `unanswered`, so the page can offer
// them for a retry instead of quietly counting them as "no module".
function validateChunk(parsed, tableNames, moduleNames) {
  const tIdx = new Map(), mIdx = new Map();
  tableNames.forEach(n => { if (!tIdx.has(n.toLowerCase())) tIdx.set(n.toLowerCase(), n); });
  moduleNames.forEach(n => { if (!mIdx.has(n.toLowerCase())) mIdx.set(n.toLowerCase(), n); });
  const arr = parsed && Array.isArray(parsed.tables) ? parsed.tables : (Array.isArray(parsed) ? parsed : []);
  const rows = [], seen = new Set();
  let dropped = 0;
  // A name written with a schema or brackets ("dbo.X", "[X]") is the same
  // table; without this it was dropped and the table reported as left out.
  const lookup = (raw) => {
    const t = String(raw || '').trim().toLowerCase();
    if (tIdx.has(t)) return tIdx.get(t);
    const bare = t.split('.').pop().replace(/^[\["`]|[\]"`]$/g, '');
    return tIdx.get(bare) || null;
  };
  for (const r of arr) {
    const real = r && lookup(r.table);
    if (!real) { dropped++; continue; }
    if (seen.has(real)) continue;
    seen.add(real);
    const mods = [];
    for (const m of Array.isArray(r.modules) ? r.modules : []) {
      const mm = mIdx.get(String(m || '').trim().toLowerCase());
      if (mm && mods.indexOf(mm) < 0) mods.push(mm);
      else if (!mm) dropped++;
    }
    const c = String(r.confidence || '').toLowerCase();
    rows.push({
      table: real,
      modules: mods,
      confidence: CONF.has(c) ? c : 'low',
      required: r.required !== false,
      reason: str(r.reason, LIMITS.REASON),
    });
  }
  const unanswered = tableNames.filter(n => !seen.has(n));
  return { rows, unanswered, dropped };
}

// What one batch result means for the chunk it belongs to. Every succeeded
// result also carries why the answer stopped (stopReason) and how long it was
// (outputTokens), and how many names in it matched nothing we sent
// (dropped) — so a chunk that comes back short says whether it was cut off,
// skipped tables, or misnamed them, instead of leaving that to guesswork.
function readResult(res, chunk, moduleNames) {
  const r = res && res.result;
  if (!r) return { id: chunk.id, ok: false, error: 'No result came back for this batch of tables' };
  if (r.type === 'succeeded') {
    const msg = r.message || {};
    const facts = { stopReason: msg.stop_reason || null,
      outputTokens: (msg.usage && Number.isFinite(msg.usage.output_tokens)) ? msg.usage.output_tokens : null };
    if (msg.stop_reason === 'refusal') return Object.assign({ id: chunk.id, ok: false, error: 'Claude declined this batch of tables' }, facts);
    if (msg.stop_reason === 'max_tokens') return Object.assign({ id: chunk.id, ok: false, error: 'The answer was cut off before the end' }, facts);
    const text = (msg.content || []).filter(b => b && b.type === 'text').map(b => b.text).join('');
    try {
      const v = validateChunk(parseModelJson(text), chunk.tables, moduleNames);
      return Object.assign({ id: chunk.id, ok: true, rows: v.rows, unanswered: v.unanswered, dropped: v.dropped }, facts);
    } catch (e) {
      return Object.assign({ id: chunk.id, ok: false, error: e.message }, facts);
    }
  }
  if (r.type === 'errored') {
    const t = (r.error && ((r.error.error && r.error.error.type) || r.error.type)) || 'error';
    return { id: chunk.id, ok: false, error: 'Claude could not process this batch of tables (' + t + ')' };
  }
  if (r.type === 'expired') return { id: chunk.id, ok: false, error: 'Expired before it was processed — retry' };
  if (r.type === 'canceled') return { id: chunk.id, ok: false, error: 'Cancelled' };
  return { id: chunk.id, ok: false, error: 'Unexpected result (' + r.type + ')' };
}

// ── Anthropic errors, without echoing anything they carried ───────────────
function apiError(e) {
  const s = e && typeof e.status === 'number' ? e.status : 0;
  if (!s) return null;
  if (s === 401 || s === 403) return bad(400, 'Claude API error (' + s + ') — check the API key in Settings');
  if (s === 404) return bad(404, 'That batch was not found — it may have been made with a different API key');
  if (s === 429) return bad(429, 'Claude API rate limit reached — wait a minute and try again');
  return bad(502, 'Claude API error (' + s + ')');
}

// ── Handlers (exported for tests, with the client injectable) ─────────────
async function startHandler(body, client) {
  const modules = cleanModules(body && body.modules);
  if (!modules.length) return bad(400, 'modules is required and must be non-empty');
  if (modules.length > LIMITS.MODULES) return bad(413, 'At most ' + LIMITS.MODULES + ' modules per run.');
  const cc = cleanChunks(body && body.chunks);
  if (cc.error) return bad(400, cc.error);
  if (!cc.chunks.length) return bad(400, 'chunks is required and must be non-empty');
  if (cc.chunks.length > LIMITS.CHUNKS) return bad(413, 'At most ' + LIMITS.CHUNKS + ' chunks per start call — send them in parts.');
  const batch = await client.messages.batches.create({ requests: cc.chunks.map(c => batchRequest(modules, c)) });
  return ok({
    batchId: batch.id,
    status: batch.processing_status,
    chunkIds: cc.chunks.map(c => c.id),
    tableCount: cc.chunks.reduce((n, c) => n + c.tables.length, 0),
    model: P.MODEL,
  });
}

function batchIdsOf(body) {
  const ids = Array.isArray(body && body.batchIds) ? body.batchIds.map(String) : [];
  return ids.filter(id => /^[A-Za-z0-9_-]{1,128}$/.test(id));
}

async function statusHandler(body, client) {
  const ids = batchIdsOf(body);
  if (!ids.length) return bad(400, 'batchIds is required');
  if (ids.length > LIMITS.BATCHES) return bad(413, 'At most ' + LIMITS.BATCHES + ' batches per call.');
  const batches = [];
  for (const id of ids) {
    const b = await client.messages.batches.retrieve(id);
    const rc = b.request_counts || {};
    batches.push({ id: b.id, status: b.processing_status,
      counts: { processing: rc.processing || 0, succeeded: rc.succeeded || 0, errored: rc.errored || 0, canceled: rc.canceled || 0, expired: rc.expired || 0 } });
  }
  return ok({ batches });
}

async function resultsHandler(body, client) {
  const id = batchIdsOf({ batchIds: [body && body.batchId] })[0];
  if (!id) return bad(400, 'batchId is required');
  const modules = cleanModules(body && body.modules).map(m => m.name);
  if (!modules.length) return bad(400, 'modules is required');
  // The chunks as names only: enough to check Claude's answer against.
  const chunks = [];
  for (const c of Array.isArray(body.chunks) ? body.chunks : []) {
    const cid = String(c && c.id || '');
    if (!CUSTOM_ID.test(cid)) continue;
    const names = (Array.isArray(c.tables) ? c.tables : []).map(t => str(t && typeof t === 'object' ? t.name : t)).filter(Boolean);
    chunks.push({ id: cid, tables: names });
  }
  if (!chunks.length) return bad(400, 'chunks is required');
  const b = await client.messages.batches.retrieve(id);
  if (b.processing_status !== 'ended') return bad(409, 'This batch is still being processed.');
  const byId = new Map(chunks.map(c => [c.id, c]));
  const out = new Map();
  for await (const res of await client.messages.batches.results(id)) {
    const chunk = byId.get(res && res.custom_id);
    if (chunk) out.set(chunk.id, readResult(res, chunk, modules));
  }
  const result = chunks.map(c => out.get(c.id) || readResult(null, c, modules));
  const res = ok({ chunks: result });
  // For the log line: counts and stop reasons only — never names.
  const short = result.filter(c => c.ok && c.unanswered && c.unanswered.length).length;
  res.summary = 'chunks=' + result.length + ' ok=' + result.filter(c => c.ok).length + ' short=' + short
    + ' maxTokens=' + result.filter(c => c.stopReason === 'max_tokens').length
    + ' dropped=' + result.reduce((n, c) => n + (c.dropped || 0), 0)
    + ' outTokensMax=' + result.reduce((n, c) => Math.max(n, c.outputTokens || 0), 0);
  return res;
}

async function cancelHandler(body, client) {
  const ids = batchIdsOf(body);
  if (!ids.length) return bad(400, 'batchIds is required');
  const cancelled = [];
  for (const id of ids.slice(0, LIMITS.BATCHES)) {
    try { await client.messages.batches.cancel(id); cancelled.push(id); } catch (e) { /* already ended, or gone */ }
  }
  return ok({ cancelled });
}

// ── Routes ───────────────────────────────────────────────────────────────
// The SDK is loaded on first use, and the client is made per request with
// the caller's own key: there is no shared client holding anybody's key.
const deps = {
  client(apiKey) {
    const Anthropic = require('@anthropic-ai/sdk');
    return new Anthropic({ apiKey, maxRetries: 1, timeout: 20000 });
  },
};

function register(name, route, handler, label) {
  app.http(name, {
    methods: ['POST', 'OPTIONS'],
    authLevel: 'function',
    route,
    handler: async (req, ctx) => {
      if (req.method === 'OPTIONS') return { status: 204, headers: CORS, body: '' };
      try {
        const auth = await enforceAuth(req, ctx);
        if (!auth.ok) return auth.response;
        const keyCheck = userAnthropicKey(req);
        if (!keyCheck.ok) return keyCheck.response;
        const body = await req.json().catch(() => null);
        if (!body || typeof body !== 'object') return bad(400, 'Invalid JSON body');
        const started = Date.now();
        const res = await handler(body, deps.client(keyCheck.key));
        const summary = res.summary ? ' ' + res.summary : '';
        delete res.summary;
        // Counts only — never names, notes, prompts or answers.
        ctx.log('[table-classify] ' + label + ' status=' + res.status + ' ms=' + (Date.now() - started) + summary);
        return res;
      } catch (e) {
        return apiError(e) || boom(e);
      }
    },
  });
}

register('table-classify-start',   'agent/table-classify/start',   startHandler,   'start');
register('table-classify-status',  'agent/table-classify/status',  statusHandler,  'status');
register('table-classify-results', 'agent/table-classify/results', resultsHandler, 'results');
register('table-classify-cancel',  'agent/table-classify/cancel',  cancelHandler,  'cancel');

module.exports = {
  LIMITS, OUTPUT_SCHEMA, cleanModules, cleanChunks, chunkPrompt, batchRequest, parseModelJson,
  validateChunk, readResult, apiError, startHandler, statusHandler, resultsHandler, cancelHandler, deps,
};
