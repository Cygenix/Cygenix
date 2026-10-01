/* ============================================================================
   claude-code.js — the Claude Code console's server side, on the Anthropic
   Managed Agents API (beta header managed-agents-2026-04-01, set by the SDK)
   ----------------------------------------------------------------------------
   Sep-2026. A person chats with Claude, and Claude writes AND RUNS code —
   Python, Node, shell, SQL — in an isolated workspace hosted by Anthropic
   that connects straight to one of the person's databases. Cygenix never
   runs the code; it opens the session, relays the messages, keeps the
   transcript and closes the session. Every route here is one of those.

     POST agent/claude-code/session   open a session on one connection
     POST agent/claude-code/message   say something to it
     GET  agent/claude-code/events    what has happened since last time
     POST agent/claude-code/mode      allow, or stop allowing, data changes
     POST agent/claude-code/stop      interrupt it and close it
     GET  agent/claude-code/sessions  the caller's past sessions
     GET  agent/claude-code/session   one past session, for read-only replay
     POST agent/claude-code/upload    attach a file to the workspace (phase 2)
     GET  agent/claude-code/outputs   the files Claude wrote, and the uploads
     GET  agent/claude-code/download  one of those files, base64
     POST agent/claude-code/check     a two-minute pass to check the bridge
     POST agent/claude-code-bridge/redeem   server to server: a pass, for
                                      the connection it opens (see below)

   WHOSE ACCOUNT, WHOSE MONEY
   Everything runs on the CALLER'S Anthropic API key, read from the
   x-anthropic-key header by user-anthropic-key.js exactly as every other AI
   route does. The key is used in memory for the length of the request and
   is never stored, logged or returned. Agents and environments are created
   in the caller's own Anthropic account: one agent per account (found again
   by its metadata, updated in place when AGENT_SPEC changes — never
   re-created per run), one environment per network shape (found again by
   name). Every session carries a hard spend cap, CLAUDE_CODE_BUDGET_CENTS,
   default 600 = US$6.00, roughly £5, because the API prices budgets in USD.

   WHO MAY
   Roles and the organisation's Claude Code policy live in Netlify Blobs,
   which this Function App cannot read, so every call asks
   /.netlify/functions/claude-code-gate — on CYGENIX_SITE_URL, the setting
   the Stripe routes already use — forwarding the caller's OWN Entra token.
   A leaked host key therefore buys nothing here: the token must verify here
   (strictly, whatever REQUIRE_TOKEN_AUTH says, as conn-secrets.js does) and
   again there, for a person the organisation says yes to. A yes is cached
   for 30 seconds per person, so a console polling every three seconds costs
   one gate call in ten. The three events worth keeping — a session opened,
   data changes allowed, a session stopped — are recorded by the gate on the
   organisation's hash-chained trail, which this app cannot write to itself.

   HOW CLAUDE REACHES THE DATABASE — THE BRIDGE (Oct-2026)
   It does not connect to it. Anthropic's workspace may only make web
   connections: SQL Server's and PostgreSQL's ports time out to every
   destination, a server built to answer on any port included, whatever
   the environment's networking allows. A day of firewall and router work
   on a real customer server established that; it is not the customer's
   network. So the workspace never holds a database login any more.

   Instead the agent has one MCP server, Cygenix's own, on cygenix.co.uk
   (/.netlify/functions/cc-mcp), and MCP calls are made by Anthropic's
   platform over HTTPS, which is allowed. Its tools list tables, describe a
   table and run a query; Cygenix runs each one along the road the SQL
   editor already uses, enforces "Allow changes to data" itself rather than
   asking Claude to behave, refuses destructive statements outright, caps
   the rows, and records every query on the organisation's audit trail.

   Each session gets its own pass: random, shown once to Anthropic's vault
   (static_bearer, keyed by the MCP URL — never to Claude), stored here only
   as a SHA-256 hash with a lookup id, a day's expiry and the session it
   belongs to. Stop or the session ending revokes it and deletes the vault.
   The MCP server redeems the pass at agent/claude-code-bridge/redeem —
   host key only, no person behind it — and gets back the connection for
   that one session and nothing else; the credential is unsealed fresh from
   conn_secrets each time, so a re-saved password takes effect at once.

   WHAT IS KEPT HERE
   Cosmos container claude_code_sessions, partitioned on /userId (the
   verified email, the house convention): one document per session, and the
   events in child documents of EVENT_CHUNK each — a long session's tool
   output would otherwise walk a single item towards the 2 MB limit. Before
   any event text is stored OR returned, the password and the connection
   string are replaced with '••••••', by literal and by pattern. The person
   can still see them inside the workspace if they ask; we do not keep them.

   ========================================================================== */
'use strict';

const crypto = require('crypto');
const { app } = require('@azure/functions');
const { userAnthropicKey } = require('./user-anthropic-key');
const { verifyJwt } = require('./entra-auth');
const connSecrets = require('./conn-secrets');

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
const AGENT_SPEC = '3';
const AGENT_NAME = 'Cygenix Dev Console';
const GATE_TTL_MS = 30 * 1000;
const RESOLVE_TTL_MS = 10 * 60 * 1000;
const LIST_CAP = 200;               // never walk more than this many agents/environments
const IP_ECHO_HOST = 'api.ipify.org';
const PACKAGE_HOSTS = ['pypi.org', 'files.pythonhosted.org', 'registry.npmjs.org'];
// Where a file mounted at mount_path actually appears in the sandbox. The
// documentation does not say; every session so far has found a file
// mounted at /workspace/x under /mnt/session/uploads/workspace/x, and
// reported the path we gave it as wrong. So the model is told the real
// location of what the person attaches.
const MOUNT_ROOT = '/mnt/session/uploads';
const mountedAt = (p) => MOUNT_ROOT + p;
// The bridge. One MCP server, by this name, at this address.
const MCP_NAME = 'cygenix';
const BRIDGE_TTL_MS = 24 * 60 * 60 * 1000;   // a session's pass: a day, or until Stop
const CHECK_TTL_MS = 2 * 60 * 1000;          // "Check the bridge": two minutes
const BRIDGE_TOKEN_RE = /^cyb_([0-9a-f]{18})\.([A-Za-z0-9_-]{43})$/;
function mcpUrl() { return siteUrl() + '/.netlify/functions/cc-mcp'; }
const CONTAINER = 'claude_code_sessions';
const EVENT_CHUNK = 200;
const EVENTS_PER_POLL = 100;
const SESSION_LIST_CAP = 50;
const MASK = '••••••';
const CONN_ID_RE = /^sconn_[A-Za-z0-9_]{1,80}$/;
const SESSION_ID_RE = /^[A-Za-z0-9_-]{6,128}$/;
const TEXT_MAX = 20000;
// Phase 2 (Oct-2026): files in and out. A file the person attaches is
// uploaded through the Files API and mounted read-only under
// /workspace/uploads; a file Claude writes to /mnt/session/outputs is what
// the API scopes to the session and hands back. Both travel as base64 in
// JSON because the browser's only road to this app forwards a text body:
// 4 MB in is 5.4 MB of JSON, under Netlify's 6 MB.
const UPLOAD_DIR = '/workspace/uploads/';
const UPLOAD_MAX = 4 * 1024 * 1024;
const DOWNLOAD_MAX = 8 * 1024 * 1024;
const UPLOAD_TTL_S = 7 * 24 * 60 * 60;
const UPLOADS_PER_SESSION = 50;
const OUTPUTS_CAP = 100;
const FILE_ID_RE = /^[A-Za-z0-9_-]{6,128}$/;

// The spend cap, in US cents as the API wants it: an integer string, > 0.
function budget() {
  const raw = String(process.env.CLAUDE_CODE_BUDGET_CENTS || '600').trim();
  const cents = /^[1-9][0-9]{0,6}$/.test(raw) ? raw : '600';
  return { type: 'limit', max_list_cost: { amount: cents, currency: 'USD' } };
}

// ── Cosmos ───────────────────────────────────────────────────────────────
// The same lazy singleton every module here carries, with the container
// created on first use (as conn-secrets.js does) so nobody has to make it by
// hand before the feature works.
let _cosmos = null, _ensured = null;
function realContainer() {
  if (!_cosmos) {
    const { CosmosClient } = require('@azure/cosmos');
    _cosmos = new CosmosClient({ endpoint: process.env.COSMOS_ENDPOINT, key: process.env.COSMOS_KEY });
  }
  const db = _cosmos.database(process.env.COSMOS_DATABASE || 'cygenix');
  if (!_ensured) {
    _ensured = db.containers.createIfNotExists({ id: CONTAINER, partitionKey: { paths: ['/userId'] } })
      .catch((e) => { _ensured = null; throw e; });
  }
  return _ensured.then(() => db.container(CONTAINER));
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
  toFile: (buf, name) => require('@anthropic-ai/sdk').toFile(buf, name, { type: 'text/plain' }),
  readSecret: (oid, connId) => connSecrets.readSecret(oid, connId),
  container: realContainer,
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
  const email = String(claims.email || claims.preferred_username || claims.upn || '').trim().toLowerCase();
  return { ok: true, oid, email: email || oid, bearer: raw.trim() };
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
      body: JSON.stringify({ act, record: o.record || '', detail: o.detail || {} }),
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
    return { ok: true, roles: data.roles || [], tenantId: data.tenantId || '' };
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
// the console's database reach is the bridge and nothing else. The bridge
// is the one MCP server, its tools always allowed — the server enforces
// what may run, so an approval prompt per query would add a click and no
// safety. The system prompt here is a placeholder; every session overrides
// it with its own.
//
// The spec tag carries the MCP address too, so an agent made when
// CYGENIX_SITE_URL said something else is updated, not kept pointing at
// the old one.
function specTag() {
  return AGENT_SPEC + '-' + crypto.createHash('sha256').update(mcpUrl()).digest('hex').slice(0, 8);
}
function agentSpec() {
  return {
    name: AGENT_NAME,
    model: { id: MODEL() },
    system: 'You work inside Cygenix, a data migration console, on behalf of the signed-in user. '
      + 'Each session tells you what it is for; follow it.',
    mcp_servers: [{ type: 'url', name: MCP_NAME, url: mcpUrl() }],
    tools: [{
      type: 'agent_toolset_20260401',
      configs: [{ name: 'web_search', enabled: false }, { name: 'web_fetch', enabled: false }],
    }, {
      type: 'mcp_toolset',
      mcp_server_name: MCP_NAME,
      default_config: { permission_policy: { type: 'always_allow' } },
    }],
    metadata: { cygenix: 'claude-code', spec: specTag() },
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
    if (found.metadata.spec !== specTag()) await client.beta.agents.update(id, agentSpec());
  } else {
    id = (await client.beta.agents.create(agentSpec())).id;
  }
  remember(tag, 'agent', id);
  return id;
}

// One environment, the same for every session now that no database host is
// in it: the package registries and the address echo, and the bridge, which
// a limited environment blocks unless allow_mcp_servers says otherwise (a
// session would be refused with a 400). The name carries a hash of the
// shape, with a version, so the environments made before the bridge — no
// MCP allowed, a database host listed — are left alone, not reused.
function environmentName(network, hosts) {
  const h = crypto.createHash('sha256').update('bridge1|' + network + '|' + hosts.slice().sort().join(',')).digest('hex').slice(0, 16);
  return 'cygenix-cc-' + network + '-' + h;
}
function environmentConfig(network, hosts) {
  return {
    type: 'cloud',
    networking: network === 'open'
      ? { type: 'unrestricted' }
      : { type: 'limited', allowed_hosts: hosts.slice(), allow_package_managers: false, allow_mcp_servers: true },
  };
}
const SESSION_HOSTS = PACKAGE_HOSTS.concat([IP_ECHO_HOST]);
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

// ── Reading a connection string ──────────────────────────────────────────
// Every shape the product stores one in:
//   ADO / ODBC     Server=tcp:host,1433;Database=…;User ID=…;Password=…
//   URL            mssql://u:p@host:1433/db   postgres(ql)://u:p@host/db
//   libpq          host=… port=… dbname=… user=… password=…
// Returns { ok, kind, host, port, database, user, password } — the password
// is a field of an object that lives for one request and is never logged.
const HOST_RE = /^(?=.{1,253}$)[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$/;
const DEFAULT_PORT = { sqlserver: 1433, postgres: 5432 };
function dec(s) { try { return decodeURIComponent(s); } catch (e) { return s; } }
// ADO strings separate on ';' — except inside a quoted value, which is how a
// value that itself contains ';' or '=' is written. SQL Server's own form is
// braces, {p;w=x}, with '}}' for a literal '}'; double and single quotes work
// too, doubled to escape. The Connections form writes braces whenever a
// password holds ';', '=' or '}' — and this reader once kept them, so the
// server was sent "{password}" and refused the login.
function adoParts(s) {
  const parts = []; let cur = '', close = '', atValue = false;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (close) {
      cur += ch;
      if (ch === close) {
        if (s[i + 1] === close) { cur += close; i++; } else close = '';
      }
      continue;
    }
    if (ch === ';') { parts.push(cur); cur = ''; atValue = false; continue; }
    if (ch === '=' && !atValue) { atValue = true; cur += ch; continue; }
    if (atValue && !cur.slice(cur.indexOf('=') + 1).trim() && (ch === '{' || ch === '"' || ch === "'")) {
      close = ch === '{' ? '}' : ch; cur += ch; continue;
    }
    cur += ch;
  }
  parts.push(cur);
  return parts;
}
function adoValue(v) {
  const t = v.trim();
  if (t.length >= 2 && t[0] === '{' && t[t.length - 1] === '}') return t.slice(1, -1).replace(/\}\}/g, '}');
  if (t.length >= 2 && t[0] === '"' && t[t.length - 1] === '"') return t.slice(1, -1).replace(/""/g, '"');
  if (t.length >= 2 && t[0] === "'" && t[t.length - 1] === "'") return t.slice(1, -1).replace(/''/g, "'");
  return t;
}
function parseConn(connString) {
  const s = String(connString || '').trim();
  if (!s) return { ok: false, why: 'The connection has no connection string.' };
  const out = { kind: 'sqlserver', host: '', port: null, database: '', user: '', password: '' };

  const url = /^(mssql|sqlserver|postgres|postgresql):\/\//i.exec(s);
  if (url) {
    out.kind = /^postgres/i.test(url[1]) ? 'postgres' : 'sqlserver';
    const rest = s.slice(url[0].length);
    let end = rest.search(/[\/;?]/); if (end === -1) end = rest.length;
    const at = rest.lastIndexOf('@', end);
    if (at !== -1) {
      const cred = rest.slice(0, at);
      const c = cred.indexOf(':');
      out.user = dec(c === -1 ? cred : cred.slice(0, c));
      out.password = c === -1 ? '' : dec(cred.slice(c + 1));
    }
    const hp = rest.slice(at + 1, end);
    const pi = hp.lastIndexOf(':');
    if (pi !== -1 && /^\d+$/.test(hp.slice(pi + 1))) { out.port = Number(hp.slice(pi + 1)); out.host = hp.slice(0, pi); }
    else out.host = hp;
    const tail = rest.slice(end);
    const db = /^\/([^;?]*)/.exec(tail);
    if (db && db[1]) out.database = dec(db[1]);
    const q = tail.indexOf('?') !== -1 ? tail.slice(tail.indexOf('?') + 1) : (tail.indexOf(';') !== -1 ? tail.slice(tail.indexOf(';') + 1) : '');
    q.split(/[&;]/).forEach(kv => {
      const i = kv.indexOf('='); if (i === -1) return;
      const k = kv.slice(0, i).trim().toLowerCase(), v = dec(kv.slice(i + 1).trim());
      if (!out.database && (k === 'database' || k === 'databasename' || k === 'dbname')) out.database = v;
      if (!out.user && (k === 'user' || k === 'username' || k === 'uid')) out.user = v;
      if (!out.password && (k === 'password' || k === 'pwd')) out.password = v;
    });
  } else if (/(^|;)\s*(server|data source|address|addr|network address)\s*=/i.test(s)) {
    adoParts(s).forEach(part => {
      const i = part.indexOf('='); if (i === -1) return;
      const k = part.slice(0, i).trim().toLowerCase(), v = adoValue(part.slice(i + 1));
      if (['server', 'data source', 'address', 'addr', 'network address'].indexOf(k) !== -1) out.host = v.replace(/^tcp:/i, '');
      else if (k === 'database' || k === 'initial catalog') out.database = v;
      else if (k === 'user id' || k === 'uid' || k === 'user') out.user = v;
      else if (k === 'password' || k === 'pwd') out.password = v;
    });
    const inst = out.host.indexOf('\\');
    if (inst !== -1) out.host = out.host.slice(0, inst);
    const comma = out.host.lastIndexOf(',');
    if (comma !== -1 && /^\d+$/.test(out.host.slice(comma + 1))) { out.port = Number(out.host.slice(comma + 1)); out.host = out.host.slice(0, comma); }
  } else if (/(^|\s)host\s*=/i.test(s)) {
    out.kind = 'postgres';
    const kv = /(\w+)\s*=\s*(?:'((?:[^'\\]|\\.)*)'|(\S+))/g;
    let m;
    while ((m = kv.exec(s))) {
      const k = m[1].toLowerCase(), v = m[2] !== undefined ? m[2].replace(/\\(.)/g, '$1') : m[3];
      if (k === 'host') out.host = v; else if (k === 'port') out.port = Number(v);
      else if (k === 'dbname') out.database = v; else if (k === 'user') out.user = v;
      else if (k === 'password') out.password = v;
    }
  } else {
    return { ok: false, why: 'Could not read a server name out of this connection string.' };
  }
  out.host = String(out.host || '').trim().replace(/^\[|\]$/g, '').toLowerCase();
  if (!HOST_RE.test(out.host)) return { ok: false, why: 'The server name "' + out.host + '" is not one the workspace can use.' };
  if (out.host === 'localhost' || /^127\./.test(out.host)) return { ok: false, why: 'This connection points at this computer (' + out.host + '). Anthropic\'s workspace cannot reach it.' };
  if (!out.port) out.port = DEFAULT_PORT[out.kind];
  out.ok = true;
  return out;
}

// ── The system prompt ────────────────────────────────────────────────────
// Short, and target-agnostic: nothing here knows which product the
// database belongs to. Names of things, never their values.
const MODE_TEXT = {
  readonly: 'DATA-CHANGE MODE: READ-ONLY. The user has not allowed changes to data in this session, and Cygenix '
    + 'enforces it: run_query refuses anything but reads, and runs reads so that nothing they did could be kept. '
    + 'If the user asks for a change, explain that "Allow changes to data this session" is off and ask them to '
    + 'switch it on first.',
  changes: 'DATA-CHANGE MODE: CHANGES ALLOWED. The user has allowed changes to data in this session. Before running '
    + 'anything that modifies data or schema, show the exact SQL and say what it will do. Cygenix still refuses '
    + 'destructive statements (DROP, TRUNCATE, and DELETE or UPDATE without a WHERE clause); those are for the SQL '
    + 'editor, where they go through the organisation\'s approvals.',
};
function systemPrompt(o) {
  const kind = o.dbType === 'postgres' ? 'PostgreSQL' : 'SQL Server';
  return [
    'You are working inside Cygenix, a data migration console, on data migration work for the signed-in user.',
    'The database for this session is ' + kind + '. You reach it ONLY through the "' + MCP_NAME + '" MCP tools: '
      + 'list_tables, describe_table and run_query. Cygenix runs each query for you and returns the rows. '
      + 'Do not try to connect to the database from the workspace: there is no login there, and the workspace\'s '
      + 'network does not allow database connections.',
    'run_query returns at most 1,000 rows. For anything larger, aggregate or filter in SQL rather than fetching '
      + 'everything. Write ' + kind + ' SQL, one statement per call; only the first result set is returned.',
    'You can still write and run Python, Node and shell in the workspace to analyse what the tools return — save a '
      + 'result to a file, chart it, compare two queries — and files you save under /mnt/session/outputs can be '
      + 'downloaded by the user. The workspace can reach the Python and npm package registries and nothing else.',
    MODE_TEXT[o.mode === 'changes' ? 'changes' : 'readonly'],
    'Keep replies short and concrete: what you ran, what came back, what it means.',
  ].join('\n\n');
}

// ── Redaction ────────────────────────────────────────────────────────────
// Every string in every event is passed through this before it is stored
// or returned. By literal for what we know (the password, the connection
// string, and their URL-encoded forms), and by pattern for what a script
// might print in another shape.
const REDACT_PATTERNS = [
  /(\b(?:password|pwd)\s*=\s*)(?:'(?:[^'\\]|\\.)*'|"[^"]*"|[^;\s'"&]+)/gi,
  /(\bCYG_DB_(?:PASSWORD|CONNSTR)\s*=\s*)(?:'(?:[^'\\]|\\.)*'|"[^"]*"|\S+)/g,
  /((?:mssql|sqlserver|postgres|postgresql):\/\/[^:\/\s@]+:)[^@\s]+(@)/gi,
];
function makeRedactor(secrets) {
  const literals = [];
  (secrets || []).forEach(s => {
    const v = String(s || '');
    if (v.length < 3) return;
    literals.push(v);
    const enc = encodeURIComponent(v);
    if (enc !== v) literals.push(enc);
  });
  literals.sort((a, b) => b.length - a.length);
  const redactString = (str) => {
    let out = str;
    literals.forEach(l => { out = out.split(l).join(MASK); });
    out = out.replace(REDACT_PATTERNS[0], '$1' + MASK)
             .replace(REDACT_PATTERNS[1], '$1' + MASK)
             .replace(REDACT_PATTERNS[2], '$1' + MASK + '$2');
    return out;
  };
  const walk = (v) => {
    if (typeof v === 'string') return redactString(v);
    if (Array.isArray(v)) return v.map(walk);
    if (v && typeof v === 'object') { const o = {}; Object.keys(v).forEach(k => { o[k] = walk(v[k]); }); return o; }
    return v;
  };
  return { string: redactString, event: walk };
}

// ── Session documents ────────────────────────────────────────────────────
const chunkId = (sessionId, n) => sessionId + ':' + String(n).padStart(4, '0');
async function loadSession(container, who, sessionId) {
  if (!SESSION_ID_RE.test(String(sessionId || ''))) return { error: bad(400, 'sessionId is missing or malformed.') };
  let doc = null;
  try { doc = (await container.item(sessionId, who.email).read()).resource || null; }
  catch (e) { if (!e || e.code !== 404) throw e; }
  // Somebody else's session, in this Cosmos or in a shared Anthropic
  // account, is not found — not forbidden, not found.
  if (!doc || doc.kind !== 'session' || doc.oid !== who.oid) return { error: bad(404, 'No such session.') };
  return { doc };
}
function publicSession(doc) {
  return {
    id: doc.id, title: doc.title || '', status: doc.status, dataChangesAllowed: !!doc.dataChangesAllowed,
    profileId: doc.profileId || '', profileName: doc.profileName || '', connectionId: doc.connectionId || '',
    connectionName: doc.connectionName || '', side: doc.side || '', dbType: doc.dbType || '',
    dbHost: doc.dbHost || '', dbName: doc.dbName || '', createdAt: doc.createdAt, endedAt: doc.endedAt || null,
    costCents: doc.costCents == null ? null : doc.costCents, eventCount: doc.eventCount || 0,
    stopReason: doc.stopReason || null,
    uploads: (doc.uploads || []).map(u => ({ fileId: u.fileId, name: u.name, path: u.path, size: u.size, at: u.at })),
  };
}
async function appendEvents(container, doc, events) {
  if (!events.length) return;
  let n = doc.chunkCount || 0;
  let chunk = null;
  if (n > 0) {
    try { chunk = (await container.item(chunkId(doc.id, n), doc.userId).read()).resource || null; }
    catch (e) { if (!e || e.code !== 404) throw e; }
  }
  for (const ev of events) {
    if (!chunk || chunk.events.length >= EVENT_CHUNK) {
      if (chunk) await container.items.upsert(chunk);
      n += 1;
      chunk = { id: chunkId(doc.id, n), kind: 'events', userId: doc.userId, sessionId: doc.id, n, events: [] };
    }
    chunk.events.push(ev);
  }
  await container.items.upsert(chunk);
  doc.chunkCount = n;
  doc.eventCount = (doc.eventCount || 0) + events.length;
}
async function readAllEvents(container, doc) {
  const out = [];
  for (let n = 1; n <= (doc.chunkCount || 0); n++) {
    let chunk = null;
    try { chunk = (await container.item(chunkId(doc.id, n), doc.userId).read()).resource || null; }
    catch (e) { if (!e || e.code !== 404) throw e; }
    if (chunk && Array.isArray(chunk.events)) out.push(...chunk.events);
  }
  return out;
}

// Anthropic's session status, and the stop reason it carried, into the four
// words the console shows: idle | running | stopped | error.
function ourStatus(remote, doc) {
  if (doc.status === 'stopped') return 'stopped';
  if (remote === 'terminated') return doc.status === 'error' ? 'error' : 'stopped';
  if (remote === 'running' || remote === 'rescheduling') return 'running';
  return 'idle';
}

async function deleteFile(client, fileId) {
  if (!fileId) return;
  try { await client.beta.files.delete(fileId); } catch (e) { /* it expires on its own */ }
}
// The person's uploads go with the session; what Claude wrote (the outputs)
// stays, because that is the work product and the replay offers it.
async function deleteUploads(client, doc) {
  for (const u of (doc.uploads || [])) { await deleteFile(client, u.fileId); u.fileId = null; }
}

// ── The bridge pass ──────────────────────────────────────────────────────
// cyb_<lookup id>.<secret>. The lookup id finds the one document (a query
// across partitions, because the MCP server does not know whose it is);
// only a SHA-256 of the secret is stored, and compared in constant time.
function sha256(v) { return crypto.createHash('sha256').update(String(v)).digest('hex'); }
function newBridgePass() {
  const lid = crypto.randomBytes(9).toString('hex');
  const secret = crypto.randomBytes(32).toString('base64url');
  return { lid, token: 'cyb_' + lid + '.' + secret, hash: sha256(secret) };
}
function splitPass(token) {
  const m = BRIDGE_TOKEN_RE.exec(String(token || ''));
  return m ? { lid: m[1], secret: m[2] } : null;
}
function sameHash(a, b) {
  const x = Buffer.from(String(a || ''), 'utf8'), y = Buffer.from(String(b || ''), 'utf8');
  return x.length === y.length && x.length > 0 && crypto.timingSafeEqual(x, y);
}

// A Function App connection is its base URL (not secret, sent by the page)
// and a key (secret, in conn_secrets — or none, for the product's own
// Function App, which the bridge reaches with the product key).
const FN_URL_RE = /^https:\/\/[A-Za-z0-9.-]+(?::\d{1,5})?\/[^\s]{0,400}$/;
function str(v, max) { return String(v == null ? '' : v).trim().slice(0, max || 200); }

// The connection a session or a check is for, as far as this app needs to
// know it: which kind, where, and that a credential exists. Never the
// credential itself — that is unsealed only when the bridge redeems a pass.
async function resolveConnection(who, body) {
  const connId = str(body.connId, 100);
  if (!CONN_ID_RE.test(connId)) return { error: bad(400, 'Choose a connection first.') };
  const names = { profileId: str(body.profileId, 100), profileName: str(body.profileName, 120),
                  connectionName: str(body.connectionName, 120) || connId };
  const side = body.side === 'src' ? 'src' : 'tgt';
  const mode = body.mode === 'azure' ? 'azure' : 'direct';
  const secret = await deps.readSecret(who.oid, connId);
  if (!secret.ok && secret.code === 'no-secrets-key') return { error: bad(503, 'The secrets store is not configured on the server (CONN_SECRETS_KEY).') };
  if (!secret.ok && secret.code === 'undecryptable') return { error: bad(409, 'The saved credential for "' + names.connectionName + '" cannot be opened with the server\'s current key. Save the connection again.') };
  if (mode === 'azure') {
    const fnUrl = str(body.fnUrl, 500).replace(/[?#].*$/, '');
    if (!FN_URL_RE.test(fnUrl)) return { error: bad(400, 'The Function App address for "' + names.connectionName + '" is not a valid https URL.') };
    let host = '';
    try { host = new URL(fnUrl).host; } catch (e) { /* the RE already held */ }
    return { connId, names, side, conn: { mode, fnUrl, dbType: 'sqlserver', dbHost: host, dbName: '' } };
  }
  if (!secret.ok) {
    return { error: bad(409, 'The credential for "' + names.connectionName + '" has not been saved to the cloud from this browser. Open Connections, check it is there, and let it sync — then try again.') };
  }
  if (!secret.bundle.connString) {
    return { error: bad(409, '"' + names.connectionName + '" is a stream destination, not a database the Dev Console can query.') };
  }
  const p = parseConn(secret.bundle.connString);
  if (!p.ok) return { error: bad(409, p.why) };
  return { connId, names, side, conn: { mode, fnUrl: '', dbType: p.kind, dbHost: p.host, dbName: p.database } };
}

// Revoke a session's pass and delete its vault: nothing can redeem it after.
async function revokeBridge(client, doc) {
  if (doc.vaultId) { try { await client.beta.vaults.delete(doc.vaultId); } catch (e) { /* gone already */ } }
  doc.vaultId = null; doc.bridgeHash = null; doc.bridgeLid = null; doc.bridgeExp = null;
}

// ── session ──────────────────────────────────────────────────────────────
async function sessionStart(who, apiKey, body, ctx) {
  const r = await resolveConnection(who, body);
  if (r.error) return r.error;
  const { connId, names, side, conn } = r;
  const container = await deps.container();

  const g = await gate(who, 'use', { record: 'session.start', detail: { profile: names.profileName, connection: names.connectionName, host: conn.dbHost } });
  if (!g.ok) return g.response;

  const client = deps.makeClient(apiKey);
  const tag = keyTag(apiKey);
  const agentId = await ensureAgent(client, tag);
  const environmentId = await ensureEnvironment(client, tag, 'limited', SESSION_HOSTS);

  // The pass goes into a vault of its own, for this session only, so
  // Anthropic presents it to the bridge and Claude never sees it.
  const pass = newBridgePass();
  const vault = await client.beta.vaults.create({ display_name: 'Cygenix Dev Console bridge', metadata: { cygenix: 'console', cyg_oid: who.oid } });
  let session;
  try {
    await client.beta.vaults.credentials.create(vault.id, { display_name: 'Cygenix bridge',
      auth: { type: 'static_bearer', token: pass.token, mcp_server_url: mcpUrl() } });
    session = await client.beta.sessions.create({
      agent: { type: 'agent_with_overrides', id: agentId, model: { id: MODEL(), effort: 'medium' },
               system: systemPrompt({ dbType: conn.dbType, mode: 'readonly' }) },
      environment_id: environmentId,
      title: 'Cygenix Dev Console — ' + names.connectionName,
      metadata: { cygenix: 'console', cyg_oid: who.oid },
      budget: budget(),
      vault_ids: [vault.id],
    });
  } catch (e) {
    try { await client.beta.vaults.delete(vault.id); } catch (e2) { /* best effort */ }
    throw e;
  }
  const now = new Date(deps.now()).toISOString();
  const doc = {
    id: session.id, kind: 'session', userId: who.email, oid: who.oid, tenantId: g.tenantId || '',
    title: '', status: 'idle', dataChangesAllowed: false,
    profileId: names.profileId, profileName: names.profileName, connectionId: connId,
    connectionName: names.connectionName, side, dbType: conn.dbType, dbHost: conn.dbHost, dbName: conn.dbName,
    connMode: conn.mode, fnUrl: conn.fnUrl,
    createdAt: now, updatedAt: now, endedAt: null,
    agentId, environmentId, model: MODEL(),
    vaultId: vault.id, bridgeLid: pass.lid, bridgeHash: pass.hash, bridgeExp: deps.now() + BRIDGE_TTL_MS,
    cursorAt: null, cursorIds: [], chunkCount: 0, eventCount: 0, costCents: null, stopReason: null,
  };
  await container.items.upsert(doc);
  ctx.log('[claude-code] session opened ' + session.id + ' db=' + conn.dbType + ' via=' + conn.mode);
  return ok({ session: publicSession(doc) });
}

// ── check: a two-minute pass for "Check the bridge" ──────────────────────
// The page takes the pass straight to the MCP server and runs SELECT 1, so
// the check goes down exactly the road Claude's queries take. A check pass
// is read-only whatever the person's roles, and good for two minutes.
async function sessionCheck(who, apiKey, body) {
  const r = await resolveConnection(who, body);
  if (r.error) return r.error;
  const g = await gate(who, 'use');
  if (!g.ok) return g.response;
  const container = await deps.container();
  const pass = newBridgePass();
  await container.items.upsert({
    id: 'chk_' + pass.lid, kind: 'bridgecheck', userId: who.email, oid: who.oid, tenantId: g.tenantId || '',
    connectionId: r.connId, connectionName: r.names.connectionName, profileName: r.names.profileName, side: r.side,
    dbType: r.conn.dbType, dbHost: r.conn.dbHost, connMode: r.conn.mode, fnUrl: r.conn.fnUrl,
    bridgeLid: pass.lid, bridgeHash: pass.hash, bridgeExp: deps.now() + CHECK_TTL_MS,
    createdAt: new Date(deps.now()).toISOString(),
  });
  return ok({ token: pass.token, mcpUrl: mcpUrl(), expiresInSeconds: CHECK_TTL_MS / 1000 });
}

// ── message ──────────────────────────────────────────────────────────────
async function sessionMessage(who, apiKey, body) {
  const text = String(body.text == null ? '' : body.text).trim();
  if (!text) return bad(400, 'Type a message first.');
  if (text.length > TEXT_MAX) return bad(413, 'That message is too long (over ' + TEXT_MAX + ' characters).');
  const container = await deps.container();
  const s = await loadSession(container, who, body.sessionId);
  if (s.error) return s.error;
  const doc = s.doc;
  if (doc.status === 'stopped' || doc.status === 'error') return bad(409, 'This session has ended. Start a new one.');
  const g = await gate(who, 'use');
  if (!g.ok) return g.response;
  const client = deps.makeClient(apiKey);
  await client.beta.sessions.events.send(doc.id, { events: [{ type: 'user.message', content: [{ type: 'text', text }] }] });
  doc.status = 'running';
  if (!doc.title) doc.title = text.replace(/\s+/g, ' ').slice(0, 80);
  doc.updatedAt = new Date(deps.now()).toISOString();
  await container.items.upsert(doc);
  return ok({ ok: true, title: doc.title, status: 'running' });
}

// ── events ───────────────────────────────────────────────────────────────
async function sessionEvents(who, apiKey, sessionId) {
  const container = await deps.container();
  const s = await loadSession(container, who, sessionId);
  if (s.error) return s.error;
  const doc = s.doc;
  if (doc.status === 'stopped' || doc.status === 'error') {
    return ok({ status: doc.status, events: [], costCents: doc.costCents, dataChangesAllowed: !!doc.dataChangesAllowed, stopReason: doc.stopReason, done: true });
  }
  const g = await gate(who, 'use');
  if (!g.ok) return g.response;
  const client = deps.makeClient(apiKey);

  let remote;
  try { remote = await client.beta.sessions.retrieve(doc.id); }
  catch (e) { if (e && e.status === 404) return bad(404, 'Anthropic no longer has this session.'); throw e; }

  // The secret again, only to know what to blank out. Its absence is not an
  // error here: the patterns still run.
  const secretsToMask = [];
  try {
    const sec = await deps.readSecret(who.oid, doc.connectionId);
    if (sec.ok && sec.bundle.connString) {
      secretsToMask.push(sec.bundle.connString);
      const p = parseConn(sec.bundle.connString);
      if (p.ok && p.password) secretsToMask.push(p.password);
    }
    if (sec.ok && sec.bundle.fnKey) secretsToMask.push(sec.bundle.fnKey);
  } catch (e) { /* patterns only */ }
  const redact = makeRedactor(secretsToMask);

  const params = { order: 'asc', limit: EVENTS_PER_POLL };
  if (doc.cursorAt) params['created_at[gte]'] = doc.cursorAt;
  const fresh = [];
  const seenAtCursor = new Set(doc.cursorIds || []);
  for await (const ev of client.beta.sessions.events.list(doc.id, params)) {
    if (!ev || !ev.processed_at) continue;          // still queued; it comes round again
    if (doc.cursorAt && ev.processed_at === doc.cursorAt && seenAtCursor.has(ev.id)) continue;
    fresh.push(redact.event(ev));
    if (fresh.length >= EVENTS_PER_POLL) break;
  }
  if (fresh.length) {
    const last = fresh[fresh.length - 1].processed_at;
    doc.cursorIds = fresh.filter(e => e.processed_at === last).map(e => e.id)
      .concat(last === doc.cursorAt ? (doc.cursorIds || []) : []);
    doc.cursorAt = last;
    await appendEvents(container, doc, fresh);
  }
  const idle = fresh.filter(e => e.type === 'session.status_idle').pop();
  if (idle && idle.stop_reason) doc.stopReason = idle.stop_reason.type || null;
  const cost = remote.usage && remote.usage.list_cost;
  if (cost && cost.amount != null) doc.costCents = Number(cost.amount);
  const status = ourStatus(remote.status, doc);
  if (remote.status === 'terminated') {
    doc.status = fresh.some(e => e.type === 'session.error') ? 'error' : 'stopped';
    doc.endedAt = doc.endedAt || new Date(deps.now()).toISOString();
    await revokeBridge(client, doc);
    await deleteUploads(client, doc);
  } else {
    doc.status = status;
  }
  doc.updatedAt = new Date(deps.now()).toISOString();
  await container.items.upsert(doc);
  return ok({ status: doc.status, events: fresh, costCents: doc.costCents, dataChangesAllowed: !!doc.dataChangesAllowed,
              stopReason: doc.stopReason, done: doc.status === 'stopped' || doc.status === 'error' });
}

// ── mode ─────────────────────────────────────────────────────────────────
async function sessionMode(who, apiKey, body) {
  const on = body.dataChangesAllowed === true;
  const container = await deps.container();
  const s = await loadSession(container, who, body.sessionId);
  if (s.error) return s.error;
  const doc = s.doc;
  if (doc.status === 'stopped' || doc.status === 'error') return bad(409, 'This session has ended.');
  const g = on
    ? await gate(who, 'changes', { record: 'session.changes-on', detail: { sessionId: doc.id, profile: doc.profileName, connection: doc.connectionName } })
    : await gate(who, 'use');
  if (!g.ok) return g.response;
  const client = deps.makeClient(apiKey);
  await client.beta.sessions.events.send(doc.id, { events: [{ type: 'system.message',
    content: [{ type: 'text', text: MODE_TEXT[on ? 'changes' : 'readonly'] }] }] });
  doc.dataChangesAllowed = on;
  doc.updatedAt = new Date(deps.now()).toISOString();
  await container.items.upsert(doc);
  return ok({ ok: true, dataChangesAllowed: on });
}

// ── stop ─────────────────────────────────────────────────────────────────
async function sessionStop(who, apiKey, body) {
  const container = await deps.container();
  const s = await loadSession(container, who, body.sessionId);
  if (s.error) return s.error;
  const doc = s.doc;
  if (doc.status === 'stopped' || doc.status === 'error') return ok({ ok: true, status: doc.status });
  const g = await gate(who, 'use', { record: 'session.stop', detail: { sessionId: doc.id, profile: doc.profileName, connection: doc.connectionName } });
  if (!g.ok) return g.response;
  const client = deps.makeClient(apiKey);
  // Interrupt, then archive: nothing more can run and nothing more can be
  // sent. Either may already have happened on Anthropic's side.
  try { await client.beta.sessions.events.send(doc.id, { events: [{ type: 'user.interrupt' }] }); } catch (e) { /* already over */ }
  try { await client.beta.sessions.archive(doc.id); } catch (e) { /* already archived or gone */ }
  await revokeBridge(client, doc);
  await deleteUploads(client, doc);
  doc.status = 'stopped';
  doc.endedAt = new Date(deps.now()).toISOString();
  doc.updatedAt = doc.endedAt;
  await container.items.upsert(doc);
  return ok({ ok: true, status: 'stopped' });
}

// ── upload ───────────────────────────────────────────────────────────────
// A file name is the only thing the person controls here: it becomes a
// mount path inside the workspace, so it is reduced to a safe basename.
function safeName(raw) {
  let n = String(raw == null ? '' : raw).split(/[\\/]/).pop().trim().replace(/[^A-Za-z0-9._ -]+/g, '_').replace(/^\.+/, '').slice(0, 100);
  return n || 'file';
}
function uniquePath(doc, name) {
  // Compare mount paths: u.path is now where the file really appears
  // (under MOUNT_ROOT); older records have only path, which was the mount path.
  const taken = new Set((doc.uploads || []).map(u => u.mountPath || u.path));
  let candidate = UPLOAD_DIR + name;
  for (let i = 2; taken.has(candidate) && i < 1000; i++) {
    const dot = name.lastIndexOf('.');
    candidate = UPLOAD_DIR + (dot > 0 ? name.slice(0, dot) + '-' + i + name.slice(dot) : name + '-' + i);
  }
  return candidate;
}
function decodeBase64(s) {
  const t = String(s || '').replace(/^data:[^,]*,/, '').replace(/\s+/g, '');
  if (!t || !/^[A-Za-z0-9+/]+=*$/.test(t)) return null;
  return Buffer.from(t, 'base64');
}
async function sessionUpload(who, apiKey, body) {
  const container = await deps.container();
  const s = await loadSession(container, who, body.sessionId);
  if (s.error) return s.error;
  const doc = s.doc;
  if (doc.status === 'stopped' || doc.status === 'error') return bad(409, 'This session has ended. Start a new one to attach files.');
  if ((doc.uploads || []).length >= UPLOADS_PER_SESSION) return bad(409, 'This session already has ' + UPLOADS_PER_SESSION + ' files attached.');
  const name = safeName(body.name);
  const buf = decodeBase64(body.contentBase64);
  if (!buf || !buf.length) return bad(400, 'The file is empty or not readable.');
  if (buf.length > UPLOAD_MAX) return bad(413, 'Files up to 4 MB can be attached; "' + name + '" is ' + (buf.length / 1048576).toFixed(1) + ' MB.');
  const g = await gate(who, 'use');
  if (!g.ok) return g.response;
  const client = deps.makeClient(apiKey);
  const file = await client.beta.files.upload({ file: await deps.toFile(buf, name), expires_in_seconds: UPLOAD_TTL_S });
  const mountPath = uniquePath(doc, name);
  try {
    await client.beta.sessions.resources.add(doc.id, { type: 'file', file_id: file.id, mount_path: mountPath });
  } catch (e) {
    await deleteFile(client, file.id);
    throw e;
  }
  // Tell Claude where it is, without starting a turn: a system message
  // waits for the next one. If the API declines it, the page shows the
  // path and the person can say so themselves.
  try {
    await client.beta.sessions.events.send(doc.id, { events: [{ type: 'system.message',
      content: [{ type: 'text', text: 'The user attached a file: ' + name + ' (' + buf.length + ' bytes), mounted read-only at ' + mountedAt(mountPath) + '.' }] }] });
  } catch (e) { /* the path is shown to the person */ }
  const rec = { fileId: file.id, name, path: mountedAt(mountPath), mountPath, size: buf.length, at: new Date(deps.now()).toISOString() };
  doc.uploads = (doc.uploads || []).concat([rec]);
  doc.updatedAt = rec.at;
  await container.items.upsert(doc);
  return ok({ upload: rec });
}

// ── outputs, download ────────────────────────────────────────────────────
async function listOutputs(client, sessionId) {
  const out = [];
  for await (const f of client.beta.files.list({ scope_id: sessionId, betas: ['managed-agents-2026-04-01'] })) {
    out.push({ id: f.id, name: f.filename, size: f.size_bytes, at: f.created_at });
    if (out.length >= OUTPUTS_CAP) break;
  }
  return out;
}
async function sessionOutputs(who, apiKey, sessionId) {
  const container = await deps.container();
  const s = await loadSession(container, who, sessionId);
  if (s.error) return s.error;
  const g = await gate(who, 'use');
  if (!g.ok) return g.response;
  const client = deps.makeClient(apiKey);
  return ok({ outputs: await listOutputs(client, s.doc.id), uploads: publicSession(s.doc).uploads });
}
async function sessionDownload(who, apiKey, sessionId, fileId) {
  if (!FILE_ID_RE.test(String(fileId || ''))) return bad(400, 'fileId is missing or malformed.');
  const container = await deps.container();
  const s = await loadSession(container, who, sessionId);
  if (s.error) return s.error;
  const g = await gate(who, 'use');
  if (!g.ok) return g.response;
  const client = deps.makeClient(apiKey);
  // Only a file that belongs to THIS session — one Claude wrote for it, or
  // one the person attached to it — ever comes back through here.
  const outputs = await listOutputs(client, s.doc.id);
  const own = outputs.find(f => f.id === fileId) || (s.doc.uploads || []).filter(u => u.fileId === fileId).map(u => ({ id: u.fileId, name: u.name, size: u.size }))[0];
  if (!own) return bad(404, 'No such file in this session.');
  if (own.size != null && own.size > DOWNLOAD_MAX) return bad(413, '"' + own.name + '" is ' + (own.size / 1048576).toFixed(1) + ' MB; files up to 8 MB can be downloaded here.');
  const res = await client.beta.files.download(fileId);
  const buf = Buffer.from(await res.arrayBuffer());
  if (buf.length > DOWNLOAD_MAX) return bad(413, '"' + own.name + '" is too large to download here (over 8 MB).');
  return ok({ name: own.name, size: buf.length, contentBase64: buf.toString('base64') });
}

// ── sessions, session ────────────────────────────────────────────────────
async function sessionList(who) {
  const container = await deps.container();
  const { resources } = await container.items.query({
    query: 'SELECT TOP @n c.id, c.title, c.status, c.dataChangesAllowed, c.profileId, c.profileName, c.connectionId, '
      + 'c.connectionName, c.side, c.dbType, c.dbHost, c.dbName, c.createdAt, c.endedAt, c.costCents, c.eventCount, c.stopReason '
      + 'FROM c WHERE c.userId = @u AND c.kind = @k AND c.oid = @o ORDER BY c.createdAt DESC',
    parameters: [{ name: '@n', value: SESSION_LIST_CAP }, { name: '@u', value: who.email }, { name: '@k', value: 'session' }, { name: '@o', value: who.oid }],
  }, { partitionKey: who.email }).fetchAll();
  return ok({ sessions: (resources || []).map(publicSession) });
}
async function sessionGet(who, sessionId) {
  const container = await deps.container();
  const s = await loadSession(container, who, sessionId);
  if (s.error) return s.error;
  const events = await readAllEvents(container, s.doc);
  return ok({ session: publicSession(s.doc), events });
}

// ── The connectivity test ────────────────────────────────────────────────
// ── The bridge redeems a pass (server to server) ─────────────────────────
// Called by the MCP server on cygenix.co.uk with this app's host key, for a
// pass Anthropic presented to it. No person is behind this call and nothing
// in it is trusted but the pass: the answer is the one connection that pass
// was issued for, for as long as it lives, and nothing else.
const JSON_ONLY = { 'Content-Type': 'application/json' };
const reply = (status, body) => ({ status, headers: JSON_ONLY, body: JSON.stringify(body) });
async function findByLid(container, lid) {
  const { resources } = await container.items.query({
    query: 'SELECT * FROM c WHERE c.bridgeLid = @lid',
    parameters: [{ name: '@lid', value: lid }],
  }).fetchAll();
  return (resources || [])[0] || null;
}
async function bridgeRedeem(body) {
  const refused = (why) => reply(401, { error: why });
  const parts = splitPass(body && body.token);
  if (!parts) return refused('This bridge pass is not valid.');
  const container = await deps.container();
  const doc = await findByLid(container, parts.lid);
  if (!doc || (doc.kind !== 'session' && doc.kind !== 'bridgecheck') || !sameHash(sha256(parts.secret), doc.bridgeHash)) {
    return refused('This bridge pass is not valid.');
  }
  if (!doc.bridgeExp || doc.bridgeExp < deps.now()) {
    if (doc.kind === 'bridgecheck') { try { await container.item(doc.id, doc.userId).delete(); } catch (e) { /* tidy only */ } }
    return refused('This bridge pass has expired. Start a new session or check again.');
  }
  if (doc.kind === 'session' && (doc.status === 'stopped' || doc.status === 'error')) {
    return refused('This Dev Console session has ended.');
  }
  // Unsealed fresh every time, so a password saved again since the session
  // opened takes effect at once, and a deleted one stops the bridge.
  const sec = await deps.readSecret(doc.oid, doc.connectionId);
  const mode = doc.connMode === 'azure' ? 'azure' : 'direct';
  if (mode === 'direct' && !(sec.ok && sec.bundle && sec.bundle.connString)) {
    return reply(409, { error: 'The credential for "' + (doc.connectionName || doc.connectionId) + '" is no longer saved on the server. Save the connection again.' });
  }
  return reply(200, {
    ok: true, kind: doc.kind, sessionId: doc.kind === 'session' ? doc.id : null,
    oid: doc.oid, email: doc.userId, tenantId: doc.tenantId || '',
    connectionId: doc.connectionId, connectionName: doc.connectionName || '', profileName: doc.profileName || '',
    side: doc.side || '', dbType: doc.dbType || 'sqlserver', mode,
    connString: mode === 'direct' ? sec.bundle.connString : null,
    fnUrl: mode === 'azure' ? doc.fnUrl : null,
    fnKey: mode === 'azure' && sec.ok && sec.bundle ? (sec.bundle.fnKey || null) : null,
    readOnly: doc.kind === 'bridgecheck' ? true : !doc.dataChangesAllowed,
  });
}
async function bridgeHandler(req, ctx) {
  const action = String((req.params && req.params.action) || '').toLowerCase();
  if (action !== 'redeem') return reply(404, { error: 'Unknown bridge action: ' + action });
  if (req.method !== 'POST') return reply(405, { error: 'POST only' });
  try {
    const body = await req.json().catch(() => null);
    const res = await bridgeRedeem(body || {});
    // Status only: never the pass, the connection or who it was for.
    ctx.log('[claude-code-bridge] redeem status=' + res.status);
    return res;
  } catch (e) {
    return boom(e);
  }
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

// ── The route ────────────────────────────────────────────────────────────
const ACTIONS = {
  check:    { POST: (who, key, req, body) => sessionCheck(who, key, body) },
  session:  { GET: (who, key, req) => sessionGet(who, req.query.get('id')), POST: (who, key, req, body, ctx) => sessionStart(who, key, body, ctx) },
  message:  { POST: (who, key, req, body) => sessionMessage(who, key, body) },
  events:   { GET: (who, key, req) => sessionEvents(who, key, req.query.get('sessionId')) },
  mode:     { POST: (who, key, req, body) => sessionMode(who, key, body) },
  stop:     { POST: (who, key, req, body) => sessionStop(who, key, body) },
  sessions: { GET: (who) => sessionList(who) },
  upload:   { POST: (who, key, req, body) => sessionUpload(who, key, body) },
  outputs:  { GET: (who, key, req) => sessionOutputs(who, key, req.query.get('sessionId')) },
  download: { GET: (who, key, req) => sessionDownload(who, key, req.query.get('sessionId'), req.query.get('fileId')) },
};

async function handler(req, ctx) {
  if (req.method === 'OPTIONS') return { status: 204, headers: CORS, body: '' };
  const action = String((req.params && req.params.action) || '').toLowerCase();
  const started = deps.now();
  try {
    const spec = ACTIONS[action];
    if (!spec) return bad(404, 'Unknown Dev Console action: ' + action);
    const fn = spec[req.method];
    if (!fn) return bad(405, action + ' is ' + Object.keys(spec).join(' or '));
    const who = await identify(req);
    if (!who.ok) return who.response;
    const keyCheck = userAnthropicKey(req);
    if (!keyCheck.ok) return keyCheck.response;
    let body = {};
    if (req.method === 'POST') {
      body = await req.json().catch(() => null);
      if (!body || typeof body !== 'object') return bad(400, 'Invalid JSON body');
    }
    const res = await fn(who, keyCheck.key, req, body, ctx);
    // Action, method, status and time only — never a host, a key, a message
    // or an event.
    ctx.log('[claude-code] ' + action + ' ' + req.method + ' status=' + res.status + ' ms=' + (deps.now() - started));
    return res;
  } catch (e) {
    return fromAnthropic(e) || boom(e);
  }
}

app.http('claude-code', {
  methods: ['GET', 'POST', 'OPTIONS'],
  authLevel: 'function',
  route: 'agent/claude-code/{action}',
  handler,
});

// The bridge's own door: host key only, and data-proxy refuses to forward
// to it, so the only caller is the MCP server holding the key.
app.http('claude-code-bridge', {
  methods: ['POST'],
  authLevel: 'function',
  route: 'agent/claude-code-bridge/{action}',
  handler: bridgeHandler,
});

module.exports = {
  deps, handler, identify, gate, siteUrl, budget, agentSpec, ensureAgent, ensureEnvironment,
  environmentName, environmentConfig, parseConn, systemPrompt, MODE_TEXT, makeRedactor,
  ourStatus, publicSession, chunkId,
  sessionStart, sessionMessage, sessionEvents, sessionMode, sessionStop, sessionList, sessionGet,
  sessionUpload, sessionOutputs, sessionDownload, safeName, uniquePath, decodeBase64, UPLOAD_DIR, UPLOAD_MAX, DOWNLOAD_MAX,
  sessionCheck, bridgeRedeem, bridgeHandler, newBridgePass, splitPass, sha256, mcpUrl, specTag, resolveConnection,
  MOUNT_ROOT, MCP_NAME, SESSION_HOSTS, BRIDGE_TTL_MS, CHECK_TTL_MS, fromAnthropic,
  _reset: () => { gateCache.clear(); resolved.clear(); },
  AGENT_SPEC, IP_ECHO_HOST, PACKAGE_HOSTS, CONTAINER, EVENT_CHUNK, MASK,
};
