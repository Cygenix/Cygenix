// template-suggest-tables.js
//
// POST /api/agent/template-suggest/shortlist?code=<FUNC_KEY>
// POST /api/agent/template-suggest/rank?code=<FUNC_KEY>
//
// AI-assisted table suggestion for Conversion Templates: which of the TARGET
// database's tables belong to each in-scope Configurator module. The page
// shows the answer as a preview the person reviews and ticks; nothing here
// changes a template.
//
// WHY THE PAGE SENDS THE SCHEMA, AND THIS FILE NEVER CONNECTS TO A DATABASE
// The page already reads the target's tables, columns and foreign keys the
// way Add table and the Schema Explorer do — through the customer's own
// Function URL (Managed-Identity-backed /api/db, never Netlify) when one is
// set, and through db-connect otherwise. Reading them here as well would mean
// sending the connection — a connection string carries a password — to a
// second service, for no gain: the names this file needs are the names the
// page has already read. So the page sends names, columns and FKs, and this
// file does two things only: ask Claude, and refuse anything Claude says that
// is not in the list it was given.
//
// WHY TWO SHORT ROUTES
// The browser reaches this Function App only through netlify/functions/
// data-proxy.js, which Netlify kills at 26 seconds. So no request here does
// more than one bounded Claude call (plus, at most, one retry that still fits
// the budget): the page sends at most 1,500 table names per shortlist call and
// a few modules at a time, and calls rank once per module, three at a time.
// The routes live under /agent/ because that is the path family the proxy
// already forwards — no new door is opened in the proxy for this.
//
// TARGET-AGNOSTIC
// Cygenix is not a tool for one product's schema. There is no table name, no
// keyword list and no module→table map in this file: the model works from the
// module's name, its notes and the live target schema, nothing else.
//
// WHO PAYS
// The caller, with their own key in the x-anthropic-key header — see
// user-anthropic-key.js. The key is never logged, echoed or stored; neither are
// the prompts, the schema or the answers. Logs carry counts.

'use strict';

const { app } = require('@azure/functions');
const { userAnthropicKey } = require('./user-anthropic-key');
const { enforceAuth } = require('./entra-auth');

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
  SHORTLIST_TABLES: 1500,   // names per shortlist call (the page chunks)
  SHORTLIST_MODULES: 8,     // modules per shortlist call (the page batches)
  SHORTLIST_PER_MODULE: 40,
  RANK_CANDIDATES: 40,
  RANK_COLUMNS: 40,         // columns per candidate sent to the model
  REASON: 90,
  NAME: 256,
  NOTES: 600,
};
// Netlify gives the whole round trip 26s. One call gets 20s; a retry is only
// attempted if it can still finish inside the budget.
const CALL_TIMEOUT_MS = 20000;
const BUDGET_MS = 23000;

// ── Prompts ──────────────────────────────────────────────────────────────
const SHORTLIST_SYSTEM = `You help a data-migration analyst decide which tables of a TARGET database hold the data for each business module of a migration.

You are given a list of modules (name, and sometimes notes) and a list of table names from the target database, one per line, sometimes with a row count.

For each module, return up to 40 candidate tables from the list that plausibly hold that module's data — its main tables, their detail/line/child tables, and lookup tables that exist mainly for it. Cast a wide net: a later step checks the columns. A table may be a candidate for more than one module.

Rules:
- Use ONLY names that appear in the list, spelled exactly as given. Never invent a name.
- If nothing in the list plausibly fits a module, return an empty list for it.
- Return ONLY JSON, no prose, no markdown: {"<module key>": ["TableName", ...], ...} with every module key present.`;

const RANK_SYSTEM = `You help a data-migration analyst decide which TARGET tables belong to one business module.

You are given the module (name, and sometimes notes) and candidate tables with their columns (name:type) and foreign keys. Judge each candidate from its name, its columns and its relationships.

Return ONLY JSON, no prose, no markdown: an array of
{"table": "<exact candidate name>", "confidence": "high" | "medium" | "low", "reason": "<at most 90 characters, citing columns or FKs>"}

Rules:
- Include only tables that belong to this module; leave out the ones that do not.
- high: clearly this module's data. medium: probably, or shared with other modules. low: plausible but doubtful.
- Use ONLY candidate names, spelled exactly as given. Never invent a table or a column.
- The reason names concrete evidence, e.g. "AddressLine1, PostCode, FK → Site".`;

// ── Pure helpers (exported for tests) ────────────────────────────────────
const str = (v, n) => String(v == null ? '' : v).slice(0, n || LIMITS.NAME);

// Strip ``` fences and any prose around the JSON value, then parse.
function parseModelJson(text) {
  let t = String(text || '').trim();
  t = t.replace(/^```(?:json)?\s*/i, '').replace(/\s*```\s*$/i, '').trim();
  try { return JSON.parse(t); } catch (e) { /* fall through */ }
  // The outermost value: whichever of { or [ opens FIRST, to its last
  // matching closer — so "[{…}]" in prose is the array, not its first item.
  const pairs = [['{', '}'], ['[', ']']]
    .map(([a, b]) => ({ i: t.indexOf(a), j: t.lastIndexOf(b) }))
    .filter(p => p.i !== -1 && p.j > p.i)
    .sort((x, y) => x.i - y.i);
  for (const p of pairs) {
    try { return JSON.parse(t.slice(p.i, p.j + 1)); } catch (e) { /* next */ }
  }
  const err = new Error('Model returned non-JSON content');
  err.code = 'bad-json';
  throw err;
}

function cleanModules(list) {
  const out = [], seen = new Set();
  for (const m of Array.isArray(list) ? list : []) {
    if (!m) continue;
    const key = str(m.key || m.name, 120).trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push({ key, name: str(m.name || m.key, 120).trim(), notes: str(m.notes, LIMITS.NOTES).trim() });
  }
  return out;
}

// Case-insensitive lookup from the names the page sent to their exact spelling.
function nameIndex(names) {
  const idx = new Map();
  for (const n of names) {
    const s = str(n).trim();
    if (s && !idx.has(s.toLowerCase())) idx.set(s.toLowerCase(), s);
  }
  return idx;
}

// Keep only real names, spelled as the database spells them; dedupe; cap.
function validateShortlist(parsed, modules, allowed, cap) {
  const out = {};
  const src = parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  let dropped = 0;
  for (const m of modules) {
    const raw = Array.isArray(src[m.key]) ? src[m.key] : [];
    const seen = new Set(), list = [];
    for (const n of raw) {
      const real = allowed.get(String(n || '').trim().toLowerCase());
      if (!real) { dropped++; continue; }
      if (seen.has(real)) continue;
      seen.add(real); list.push(real);
      if (list.length >= (cap || LIMITS.SHORTLIST_PER_MODULE)) break;
    }
    out[m.key] = list;
  }
  return { shortlist: out, dropped };
}

const CONF = new Set(['high', 'medium', 'low']);
function validateRank(parsed, allowed) {
  const arr = Array.isArray(parsed) ? parsed
    : (parsed && Array.isArray(parsed.tables) ? parsed.tables : []);
  const out = [], seen = new Set();
  let dropped = 0;
  for (const r of arr) {
    if (!r || typeof r !== 'object') { dropped++; continue; }
    const real = allowed.get(String(r.table || '').trim().toLowerCase());
    if (!real) { dropped++; continue; }
    if (seen.has(real)) continue;
    seen.add(real);
    const c = String(r.confidence || '').toLowerCase();
    out.push({ table: real, confidence: CONF.has(c) ? c : 'low',
      reason: String(r.reason || '').replace(/\s+/g, ' ').trim().slice(0, LIMITS.REASON) });
  }
  return { ranked: out, dropped };
}

function shortlistPrompt(modules, tables) {
  const mods = modules.map(m => '- key: ' + JSON.stringify(m.key) + ' · name: ' + m.name
    + (m.notes ? ' · notes: ' + m.notes.replace(/\s+/g, ' ') : '')).join('\n');
  const list = tables.map(t => t.rows != null ? t.name + '\t' + t.rows : t.name).join('\n');
  return 'Modules:\n' + mods + '\n\nTarget tables (' + tables.length + ', name then row count where known):\n' + list
    + '\n\nReturn the JSON object now.';
}

function rankPrompt(module, candidates) {
  const lines = candidates.map(c => {
    const cols = (c.columns || []).slice(0, LIMITS.RANK_COLUMNS)
      .map(x => str(x.name, 128) + ':' + str(x.type || x.dataType || '?', 40)).join(', ');
    const fks = (c.fks || []).map(f => str(f.child) + '.' + str(f.childColumn, 128) + ' → ' + str(f.parent) + '.' + str(f.parentColumn, 128));
    return '- ' + c.name + (c.rows != null ? ' (' + c.rows + ' rows)' : '')
      + '\n  columns: ' + (cols || '(not read)')
      + (fks.length ? '\n  fks: ' + fks.slice(0, 20).join('; ') : '');
  }).join('\n');
  return 'Module: ' + module.name + (module.notes ? '\nNotes: ' + module.notes.replace(/\s+/g, ' ') : '')
    + '\n\nCandidate tables:\n' + lines + '\n\nReturn the JSON array now.';
}

// ── Claude ────────────────────────────────────────────────────────────────
async function callClaude(apiKey, system, user, maxTokens, timeoutMs) {
  const resp = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
    body: JSON.stringify({
      // The fast model: every call has to finish inside the proxy's 26s,
      // and the output is a bounded, validated list.
      model: process.env.ANTHROPIC_MODEL_FAST || 'claude-haiku-4-5-20251001',
      max_tokens: maxTokens,
      system,
      messages: [{ role: 'user', content: user }],
    }),
    signal: AbortSignal.timeout(timeoutMs),
  });
  if (!resp.ok) {
    // The status only: Anthropic's error body can echo the request.
    const e = new Error('Claude API error (' + resp.status + ')' + (resp.status === 401 ? ' — check the API key in Settings' : ''));
    e.status = resp.status === 401 || resp.status === 403 ? 400 : 502;
    throw e;
  }
  const data = await resp.json();
  return (data.content || []).filter(b => b.type === 'text').map(b => b.text).join('').trim();
}

// One call, and one retry with a "JSON only" reminder if the answer does not
// parse — but only if the retry can still finish inside the budget.
async function askJson(apiKey, system, user, maxTokens, started) {
  const first = await callClaude(apiKey, system, user, maxTokens,
    Math.min(CALL_TIMEOUT_MS, BUDGET_MS - (Date.now() - started)));
  try { return parseModelJson(first); } catch (e) {
    const left = BUDGET_MS - (Date.now() - started);
    if (left < 6000) throw e;
    const again = await callClaude(apiKey, system,
      user + '\n\nYour previous answer was not valid JSON. Return ONLY the JSON value — no prose, no code fences.',
      maxTokens, Math.min(CALL_TIMEOUT_MS, left));
    return parseModelJson(again);
  }
}

// ── Handlers (exported for tests, with the Claude call injectable) ─────────
async function shortlistHandler(body, apiKey, ask) {
  const modules = cleanModules(body && body.modules);
  if (!modules.length) return bad(400, 'modules is required and must be non-empty');
  if (modules.length > LIMITS.SHORTLIST_MODULES) return bad(413, 'At most ' + LIMITS.SHORTLIST_MODULES + ' modules per shortlist call — send them in batches.');
  const rawTables = Array.isArray(body.tables) ? body.tables : [];
  const tables = [];
  const seen = new Set();
  for (const t of rawTables) {
    const name = str(t && typeof t === 'object' ? t.name : t).trim();
    if (!name || seen.has(name.toLowerCase())) continue;
    seen.add(name.toLowerCase());
    const rows = t && typeof t === 'object' && Number.isFinite(Number(t.rows)) && t.rows !== null && t.rows !== '' ? Number(t.rows) : null;
    tables.push({ name, rows });
  }
  if (!tables.length) return bad(400, 'tables is required and must be non-empty');
  if (tables.length > LIMITS.SHORTLIST_TABLES) return bad(413, 'At most ' + LIMITS.SHORTLIST_TABLES + ' table names per shortlist call — send them in chunks.');
  const allowed = nameIndex(tables.map(t => t.name));
  const parsed = await ask(SHORTLIST_SYSTEM, shortlistPrompt(modules, tables),
    Math.min(8000, 400 + modules.length * LIMITS.SHORTLIST_PER_MODULE * 14));
  const v = validateShortlist(parsed, modules, allowed, LIMITS.SHORTLIST_PER_MODULE);
  return ok({ shortlist: v.shortlist, tableCount: tables.length, dropped: v.dropped });
}

async function rankHandler(body, apiKey, ask) {
  const module = cleanModules([body && body.module])[0];
  if (!module) return bad(400, 'module is required');
  const raw = Array.isArray(body.candidates) ? body.candidates : [];
  const candidates = [];
  const seen = new Set();
  for (const c of raw) {
    const name = str(c && typeof c === 'object' ? c.name : c).trim();
    if (!name || seen.has(name.toLowerCase())) continue;
    seen.add(name.toLowerCase());
    candidates.push({
      name,
      rows: c && Number.isFinite(Number(c.rows)) && c.rows !== null && c.rows !== '' ? Number(c.rows) : null,
      columns: Array.isArray(c && c.columns) ? c.columns : [],
      fks: Array.isArray(c && c.fks) ? c.fks : [],
    });
  }
  if (!candidates.length) return ok({ ranked: [], fks: [], dropped: 0 });
  if (candidates.length > LIMITS.RANK_CANDIDATES) return bad(413, 'At most ' + LIMITS.RANK_CANDIDATES + ' candidates per rank call.');
  const allowed = nameIndex(candidates.map(c => c.name));
  const parsed = await ask(RANK_SYSTEM, rankPrompt(module, candidates), 2400);
  const v = validateRank(parsed, allowed);
  // The FKs among the returned tables, child → parent, for the load order.
  // Only those the page supplied: nothing here is inferred.
  const inSet = new Set(v.ranked.map(r => r.table.toLowerCase()));
  const fks = [], fseen = new Set();
  for (const c of candidates) for (const f of c.fks) {
    const child = allowed.get(str(f.child).toLowerCase()), parent = str(f.parent);
    if (!child || !inSet.has(child.toLowerCase()) || !parent) continue;
    const k = child.toLowerCase() + '>' + parent.toLowerCase();
    if (fseen.has(k)) continue;
    fseen.add(k); fks.push({ child, parent });
  }
  return ok({ ranked: v.ranked, fks, dropped: v.dropped });
}

// ── Routes ───────────────────────────────────────────────────────────────
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
        const ask = (system, user, maxTokens) => askJson(keyCheck.key, system, user, maxTokens, started);
        const res = await handler(body, keyCheck.key, ask);
        // Counts only — never names, prompts or answers.
        ctx.log('[template-suggest] ' + label + ' status=' + res.status + ' ms=' + (Date.now() - started));
        return res;
      } catch (e) {
        if (e && e.code === 'bad-json') return bad(502, 'The model did not return usable JSON twice in a row. Try again.');
        if (e && e.name === 'TimeoutError') return bad(504, 'The model took too long to answer. Try again, or suggest for fewer modules at once.');
        if (e && e.status) return bad(e.status, e.message);
        return boom(e);
      }
    },
  });
}

register('template-suggest-shortlist', 'agent/template-suggest/shortlist', shortlistHandler, 'shortlist');
register('template-suggest-rank', 'agent/template-suggest/rank', rankHandler, 'rank');

module.exports = {
  LIMITS, parseModelJson, cleanModules, nameIndex, validateShortlist, validateRank,
  shortlistPrompt, rankPrompt, shortlistHandler, rankHandler, SHORTLIST_SYSTEM, RANK_SYSTEM,
};
