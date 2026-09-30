/* ============================================================================
   claude-code.js — the Claude Code console's server side, on the Anthropic
   Managed Agents API (beta header managed-agents-2026-04-01, set by the SDK)
   ----------------------------------------------------------------------------
   Sep-2026. THIS FILE HOLDS THE CONNECTIVITY TEST ONLY. The console proper
   (sessions, messages, events, stop, transcripts) is built on it next. The
   test comes first because the documentation leaves open the two questions
   the whole console depends on:

     1. Can a Managed Agents cloud workspace open a raw TCP connection to a
        database (SQL Server 1433, PostgreSQL 5432)? The environment's
        `allowed_hosts` takes host names with no port, and nothing says
        whether non-HTTP egress is allowed out at all.
     2. From which IP address does it arrive? Anthropic publishes none, and
        a customer database that admits only known addresses (Azure SQL's
        firewall, typically) needs one to allow.

   So an Owner or Platform Administrator points the test at a host and port.
   A throwaway session runs a fixed Python script that resolves the name,
   opens the socket, and speaks the first bytes of the database's own
   protocol — a SQL Server PRELOGIN packet, a PostgreSQL SSLRequest — so a
   transparent proxy that accepts every connection cannot pass for a
   database that answered. It then asks api.ipify.org for its public
   address. No login, no password, no data: the test needs none, and so it
   is given none.

   It runs twice if asked: once under `limited` networking (the allow-list
   the console will use — the host plus the one address-echo service) and
   once `unrestricted`, so a failure can be pinned on the allow-list or on
   the network itself.

   WHOSE ACCOUNT, WHOSE MONEY
   Everything here runs on the CALLER'S Anthropic API key, read from the
   x-anthropic-key header by user-anthropic-key.js exactly as every other AI
   route does. The key is used in memory for the length of the request and
   is never stored, logged or returned. Agents and environments are
   therefore created in the caller's own Anthropic account: one agent per
   account (found again by its metadata, updated in place when this file's
   spec changes — never re-created per run), one environment per network
   shape (found again by name). Every session carries a hard spend cap —
   CLAUDE_CODE_BUDGET_CENTS, default 600 = US$6.00, roughly £5, because the
   API prices budgets in USD only.

   WHO MAY
   Roles and the organisation's Claude Code policy live in Netlify Blobs,
   which this Function App cannot read, so every call asks
   /.netlify/functions/claude-code-gate — on CYGENIX_SITE_URL, the setting
   the Stripe routes already use — forwarding the caller's OWN Entra token.
   A leaked host key therefore buys nothing here: the token must verify
   here (strictly, whatever REQUIRE_TOKEN_AUTH says, as conn-secrets.js
   does) and again there, for a person the organisation says yes to.
   ========================================================================== */
'use strict';

const crypto = require('crypto');
const { app } = require('@azure/functions');
const { userAnthropicKey } = require('./user-anthropic-key');
const { verifyJwt } = require('./entra-auth');

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, x-anthropic-key',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type':                 'application/json',
};
const ok  = (body)      => ({ status: 200, headers: CORS, body: JSON.stringify(body) });
const bad = (code, msg, extra) => ({ status: code, headers: CORS, body: JSON.stringify(Object.assign({ error: msg }, extra || {})) });
const boom = (e) => ({ status: 500, headers: CORS,
  body: JSON.stringify({ error: (e && e.message) || String(e), stack: (e && e.stack) || '' }) });

const MODEL = () => process.env.CLAUDE_CODE_MODEL || 'claude-opus-5-5';
// Bump when the agent's configuration below changes; the next call finds
// the account's agent carrying an older spec and updates it in place.
const AGENT_SPEC = '1';
const AGENT_NAME = 'Cygenix Claude Code';
const GATE_TTL_MS = 30 * 1000;
const RESOLVE_TTL_MS = 10 * 60 * 1000;
const LIST_CAP = 200;               // never walk more than this many agents/environments
const IP_ECHO_HOST = 'api.ipify.org';

// The spend cap, in US cents as the API wants it: an integer string, > 0.
function budget() {
  const raw = String(process.env.CLAUDE_CODE_BUDGET_CENTS || '600').trim();
  const cents = /^[1-9][0-9]{0,6}$/.test(raw) ? raw : '600';
  return { type: 'limit', max_list_cost: { amount: cents, currency: 'USD' } };
}

// ── Dependencies (swapped in tests) ──────────────────────────────────────
const deps = {
  verify: verifyJwt,
  fetch: (...a) => fetch(...a),
  makeClient: (apiKey) => {
    const Anthropic = require('@anthropic-ai/sdk');
    // One attempt plus one retry, each short: the whole round trip has to
    // fit inside Netlify's 26 seconds, and a stuck call is worse than a
    // clear "try again".
    return new Anthropic({ apiKey, maxRetries: 1, timeout: 15000 });
  },
  now: () => Date.now(),
};

// ── Identity: strict, like conn-secrets.js ───────────────────────────────
async function identify(req) {
  const raw = req.headers.get('authorization') || '';
  const m = /^Bearer\s+(.+)$/i.exec(raw.trim());
  if (!m) return { ok: false, response: bad(401, 'Sign in again — this needs your Cygenix sign-in token.') };
  let claims;
  try { claims = await deps.verify(m[1].trim()); }
  catch (e) { return { ok: false, response: bad(401, 'Your sign-in token was not accepted: ' + ((e && e.message) || e)) }; }
  const oid = String(claims.oid || claims.sub || '').trim();
  if (!oid) return { ok: false, response: bad(401, 'Your sign-in token carries no user id.') };
  return { ok: true, oid, bearer: raw.trim() };
}

// ── The gate ─────────────────────────────────────────────────────────────
const gateCache = new Map();   // oid|act → expiry; a yes only, never a no
function siteUrl() {
  return String(process.env.CYGENIX_SITE_URL || 'https://cygenix.co.uk').replace(/\/+$/, '');
}
async function gate(who, act, opts) {
  const o = opts || {};
  const key = who.oid + '|' + act;
  if (!o.record && (gateCache.get(key) || 0) > deps.now()) return { ok: true };
  let res, text;
  try {
    res = await deps.fetch(siteUrl() + '/.netlify/functions/claude-code-gate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: who.bearer },
      body: JSON.stringify({ act, record: !!o.record, detail: o.detail || {} }),
      signal: AbortSignal.timeout(8000),
    });
    text = await res.text();
  } catch (e) {
    return { ok: false, response: bad(503, 'Could not reach the Cygenix permission check at ' + siteUrl() + ' (' + ((e && e.message) || e) + '). Check the CYGENIX_SITE_URL app setting.') };
  }
  let data = {};
  try { data = JSON.parse(text || '{}'); } catch (e) { /* keep {} */ }
  if (res.status === 200 && data.allowed === true) {
    gateCache.set(key, deps.now() + GATE_TTL_MS);
    return { ok: true, roles: data.roles || [] };
  }
  gateCache.delete(key);
  const status = res.status === 401 ? 401 : (res.status === 403 ? 403 : 502);
  return { ok: false, response: bad(status, data.error || ('Permission check answered ' + res.status)) };
}

// ── Resolving the agent and the environment in the caller's account ───────
// Held in this process's memory only, keyed by a hash of the key so one
// person's ids are never handed to another account. Never written anywhere.
const resolved = new Map();
function keyTag(apiKey) { return crypto.createHash('sha256').update(apiKey).digest('hex').slice(0, 24); }
function remembered(tag, what) {
  const hit = resolved.get(tag + '|' + what);
  return hit && hit.until > deps.now() ? hit.id : null;
}
function remember(tag, what, id) { resolved.set(tag + '|' + what, { id, until: deps.now() + RESOLVE_TTL_MS }); }

async function findFirst(page, test) {
  let seen = 0;
  for await (const item of page) {
    if (test(item)) return item;
    if (++seen >= LIST_CAP) break;
  }
  return null;
}

// The agent's standing configuration. Web search and fetch are OFF: they run
// on Anthropic's servers, outside the environment's network allow-list, and
// a console whose promise is "the workspace reaches your database and the
// package registries, nothing else" cannot have a side door.
function agentSpec() {
  return {
    name: AGENT_NAME,
    model: { id: MODEL() },
    system: 'You work inside Cygenix, a data migration console, on behalf of the signed-in user. '
      + 'Each session tells you what it is for; follow it.',
    tools: [{
      type: 'agent_toolset_20260401',
      configs: [{ name: 'web_search', enabled: false }, { name: 'web_fetch', enabled: false }],
    }],
    metadata: { cygenix: 'claude-code', spec: AGENT_SPEC },
  };
}

async function ensureAgent(client, tag) {
  const known = remembered(tag, 'agent');
  if (known) return known;
  const found = await findFirst(client.beta.agents.list(),
    a => !a.archived_at && a.metadata && a.metadata.cygenix === 'claude-code');
  let id;
  if (found) {
    id = found.id;
    if (found.metadata.spec !== AGENT_SPEC) await client.beta.agents.update(id, agentSpec());
  } else {
    id = (await client.beta.agents.create(agentSpec())).id;
  }
  remember(tag, 'agent', id);
  return id;
}

// One environment per network shape. The name carries a hash of the allowed
// hosts rather than the hosts themselves: it lives in the customer's own
// account, but a list of their database servers is not a label.
function environmentName(network, hosts) {
  const h = crypto.createHash('sha256').update(network + '|' + hosts.slice().sort().join(',')).digest('hex').slice(0, 16);
  return 'cygenix-cc-' + network + '-' + h;
}
function environmentConfig(network, hosts) {
  return {
    type: 'cloud',
    networking: network === 'open'
      ? { type: 'unrestricted' }
      : { type: 'limited', allowed_hosts: hosts.slice(), allow_package_managers: false, allow_mcp_servers: false },
  };
}
async function ensureEnvironment(client, tag, network, hosts) {
  const name = environmentName(network, hosts);
  const known = remembered(tag, name);
  if (known) return known;
  const byName = () => findFirst(client.beta.environments.list(), e => e.name === name && !e.archived_at);
  let env = await byName();
  if (!env) {
    try {
      env = await client.beta.environments.create({
        name, config: environmentConfig(network, hosts), metadata: { cygenix: 'claude-code' },
      });
    } catch (e) {
      // Two tabs raced us to the same name: the other one won, use theirs.
      if (e && e.status === 409) env = await byName();
      if (!env) throw e;
    }
  }
  remember(tag, name, env.id);
  return env.id;
}

// ── The connectivity test ────────────────────────────────────────────────
const HOST_RE = /^(?=.{1,253}$)[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$/;
const KINDS = ['sqlserver', 'postgres', 'other'];
const NETWORKS = ['limited', 'open'];

function validateProbe(body) {
  const host = String((body && body.host) || '').trim().toLowerCase();
  const port = Number(body && body.port);
  const kind = KINDS.indexOf(body && body.kind) !== -1 ? body.kind : 'other';
  const network = NETWORKS.indexOf(body && body.network) !== -1 ? body.network : 'limited';
  if (!HOST_RE.test(host)) return { ok: false, error: 'Enter a host name or IPv4 address, without a scheme, port or path.' };
  if (!Number.isInteger(port) || port < 1 || port > 65535) return { ok: false, error: 'Enter a port between 1 and 65535.' };
  return { ok: true, host, port, kind, network };
}

// Fixed script; the three values go in as JSON literals, which are valid
// Python literals for anything validateProbe lets through. The handshake
// bytes are the first message each server expects from a client:
//   SQL Server — a TDS PRELOGIN packet (type 0x12, 26 bytes: VERSION and
//                ENCRYPTION options). A server answers with a packet of
//                type 0x04.
//   PostgreSQL — SSLRequest (length 8, code 80877103). A server answers
//                with a single 'S' or 'N'.
function probeScript(p) {
  return [
    'import socket, json, time, urllib.request',
    'H = ' + JSON.stringify(p.host),
    'P = ' + JSON.stringify(p.port),
    'KIND = ' + JSON.stringify(p.kind),
    'r = {"host": H, "port": P, "kind": KIND}',
    'try:',
    '    r["dns"] = sorted({a[4][0] for a in socket.getaddrinfo(H, P, proto=socket.IPPROTO_TCP)})',
    'except Exception as e:',
    '    r["dns_error"] = type(e).__name__ + ": " + str(e)',
    't = time.time()',
    'try:',
    '    s = socket.create_connection((H, P), timeout=10)',
    '    r["tcp"] = "open"',
    '    try:',
    '        s.settimeout(8)',
    '        if KIND == "sqlserver":',
    '            s.sendall(bytes.fromhex("1201001a00000100" + "00000b0006" + "0100110001" + "ff" + "000000000000" + "02"))',
    '            b = s.recv(8)',
    '            r["handshake"] = "sqlserver-replied" if b[:1] == b"\\x04" else ("unexpected:" + b[:8].hex() if b else "no-reply")',
    '        elif KIND == "postgres":',
    '            s.sendall(bytes.fromhex("0000000804d2162f"))',
    '            b = s.recv(1)',
    '            r["handshake"] = "postgres-replied" if b in (b"S", b"N") else ("unexpected:" + b.hex() if b else "no-reply")',
    '    except Exception as e:',
    '        r["handshake"] = "error: " + type(e).__name__ + ": " + str(e)',
    '    s.close()',
    'except Exception as e:',
    '    r["tcp"] = "failed"',
    '    r["tcp_error"] = type(e).__name__ + ": " + str(e)',
    'r["tcp_ms"] = int((time.time() - t) * 1000)',
    'try:',
    '    r["egress_ip"] = urllib.request.urlopen("https://' + IP_ECHO_HOST + '", timeout=10).read().decode().strip()',
    'except Exception as e:',
    '    r["egress_ip_error"] = type(e).__name__ + ": " + str(e)',
    'print("CYGPROBE_RESULT " + json.dumps(r))',
  ].join('\n');
}

const PROBE_SYSTEM = 'You run one automated network test for Cygenix and report its output. '
  + 'Run exactly what you are given, change nothing, install nothing, run nothing else.';

function probeInstruction(p) {
  return 'Use the bash tool to write the Python script below to /tmp/cygprobe.py and run it with python3. '
    + 'Do not change it and do not run anything else. When it finishes, reply with only the line of '
    + 'its output that begins with CYGPROBE_RESULT.\n\n```python\n' + probeScript(p) + '\n```';
}

// Pull the result line out of whatever the session said. The tool result is
// preferred — it is the script's own stdout — and the agent's reply is the
// fallback for when it paraphrased the command but echoed the line.
function textsOf(ev) {
  return (Array.isArray(ev && ev.content) ? ev.content : [])
    .filter(b => b && b.type === 'text' && typeof b.text === 'string').map(b => b.text);
}
function parseProbeEvents(events) {
  let result = null;
  const errors = [];
  const pick = (types) => {
    for (const ev of events) {
      if (types.indexOf(ev.type) === -1) continue;
      for (const t of textsOf(ev)) {
        const m = /CYGPROBE_RESULT (\{.*\})/.exec(t);
        if (m) { try { return JSON.parse(m[1]); } catch (e) { /* keep looking */ } }
      }
    }
    return null;
  };
  result = pick(['agent.tool_result']) || pick(['agent.message']);
  events.filter(ev => ev.type === 'session.error').forEach(ev => {
    const er = ev.error || {};
    errors.push(String(er.message || er.type || 'session error'));
  });
  const idle = events.filter(ev => ev.type === 'session.status_idle').pop();
  return { result, errors, stopReason: idle && idle.stop_reason ? idle.stop_reason.type : null };
}

// Plain-language verdict, so the page does not have to interpret the JSON.
function verdict(r) {
  if (!r) return null;
  if (r.dns_error) return { ok: false, text: 'The workspace could not resolve ' + r.host + ' (' + r.dns_error + ').' };
  if (r.tcp !== 'open') return { ok: false, text: 'The workspace could not open a connection to ' + r.host + ':' + r.port + ' (' + (r.tcp_error || 'failed') + ').' };
  if (r.kind === 'other') return { ok: true, text: 'A connection to ' + r.host + ':' + r.port + ' opened.' };
  if (/-replied$/.test(r.handshake || '')) return { ok: true, text: 'The database at ' + r.host + ':' + r.port + ' answered.' };
  return { ok: false, text: 'A connection opened, but no database answered on it (' + (r.handshake || 'no reply') + ') — something between the workspace and ' + r.host + ' accepted the connection without passing it on.' };
}

async function probeStart(req, who, apiKey, body) {
  const p = validateProbe(body);
  if (!p.ok) return bad(400, p.error);
  const g = await gate(who, 'probe', { record: true, detail: { host: p.host, port: p.port, network: p.network } });
  if (!g.ok) return g.response;
  const client = deps.makeClient(apiKey);
  const tag = keyTag(apiKey);
  const hosts = [p.host, IP_ECHO_HOST];
  const agentId = await ensureAgent(client, tag);
  const environmentId = await ensureEnvironment(client, tag, p.network, hosts);
  const session = await client.beta.sessions.create({
    agent: { type: 'agent_with_overrides', id: agentId, model: { id: MODEL(), effort: 'low' }, system: PROBE_SYSTEM },
    environment_id: environmentId,
    title: 'Cygenix connection test',
    metadata: { cygenix: 'probe', cyg_oid: who.oid },
    budget: budget(),
    initial_events: [{ type: 'user.message', content: [{ type: 'text', text: probeInstruction(p) }] }],
  });
  return ok({ sessionId: session.id, status: session.status, network: p.network, host: p.host, port: p.port });
}

async function probeResult(req, who, apiKey, sessionId) {
  if (!/^[A-Za-z0-9_-]{6,128}$/.test(String(sessionId || ''))) return bad(400, 'sessionId is missing or malformed.');
  const g = await gate(who, 'probe');
  if (!g.ok) return g.response;
  const client = deps.makeClient(apiKey);
  let session;
  try { session = await client.beta.sessions.retrieve(sessionId); }
  catch (e) { if (e && e.status === 404) return bad(404, 'No such test session.'); throw e; }
  // Somebody else's session in a shared Anthropic account is not found.
  if (!session.metadata || session.metadata.cyg_oid !== who.oid || session.metadata.cygenix !== 'probe') {
    return bad(404, 'No such test session.');
  }
  const events = [];
  for await (const ev of client.beta.sessions.events.list(sessionId, { order: 'asc', limit: 100 })) {
    events.push(ev);
    if (events.length >= 400) break;
  }
  const parsed = parseProbeEvents(events);
  const finished = session.status === 'terminated'
    || (session.status === 'idle' && (parsed.stopReason || parsed.result || parsed.errors.length));
  if (finished && !session.archived_at) {
    // Routine cleanup for a single-use session; failure changes nothing.
    try { await client.beta.sessions.archive(sessionId); } catch (e) { /* ignore */ }
  }
  const cost = session.usage && session.usage.list_cost;
  return ok({
    sessionId, status: session.status, done: !!finished,
    result: parsed.result, verdict: verdict(parsed.result), errors: parsed.errors,
    stopReason: parsed.stopReason,
    costCents: cost && cost.amount != null ? Number(cost.amount) : null,
  });
}

// Anthropic's own refusals, in words the page can show as they are.
function fromAnthropic(e) {
  const s = e && e.status;
  if (s === 401) return bad(401, 'Anthropic did not accept your API key. Check it in Settings.');
  if (s === 403) return bad(403, 'Your Anthropic account does not have access to Managed Agents: ' + (e.message || ''));
  if (s === 429) return bad(429, 'Anthropic is rate-limiting this account. Wait a minute and try again.');
  if (s && s >= 400 && s < 600) return bad(s === 529 ? 503 : s, 'Anthropic: ' + (e.message || 'request failed'));
  return null;
}

app.http('claude-code-probe', {
  methods: ['GET', 'POST', 'OPTIONS'],
  authLevel: 'function',
  route: 'agent/claude-code/probe',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 204, headers: CORS, body: '' };
    const started = deps.now();
    try {
      const who = await identify(req);
      if (!who.ok) return who.response;
      const keyCheck = userAnthropicKey(req);
      if (!keyCheck.ok) return keyCheck.response;
      let res;
      if (req.method === 'POST') {
        const body = await req.json().catch(() => null);
        if (!body || typeof body !== 'object') return bad(400, 'Invalid JSON body');
        res = await probeStart(req, who, keyCheck.key, body);
      } else {
        res = await probeResult(req, who, keyCheck.key, req.query.get('sessionId'));
      }
      // Method, status and time only — never the host, the key or output.
      ctx.log('[claude-code] probe ' + req.method + ' status=' + res.status + ' ms=' + (deps.now() - started));
      return res;
    } catch (e) {
      return fromAnthropic(e) || boom(e);
    }
  },
});

module.exports = {
  deps, identify, gate, siteUrl, budget, agentSpec, ensureAgent, ensureEnvironment,
  environmentName, environmentConfig, validateProbe, probeScript, probeInstruction,
  parseProbeEvents, verdict, probeStart, probeResult, fromAnthropic,
  _reset: () => { gateCache.clear(); resolved.clear(); },
  AGENT_SPEC, IP_ECHO_HOST, PROBE_SYSTEM,
};
