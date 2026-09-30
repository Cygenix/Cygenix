/* ============================================================================
   claude-code.js — the Claude Code console's server side, on the Anthropic
   Managed Agents API (beta header managed-agents-2026-04-01, set by the SDK)
   ----------------------------------------------------------------------------
   Sep-2026. A person chats with Claude, and Claude writes AND RUNS code —
   Python, Node, shell, SQL — in an isolated workspace hosted by Anthropic
   that connects straight to one of the person's databases. Cygenix never
   runs the code; it opens the session, relays the messages, keeps the
   transcript and closes the session. Every route here is one of those.

     POST agent/claude-code/probe     the connectivity test (see below)
     GET  agent/claude-code/probe     …and its result
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

   HOW THE WORKSPACE GETS THE DATABASE
   The connection's credential is unsealed from conn_secrets (the encrypted
   store the browser syncs to) for the verified owner only, parsed into its
   parts, written as KEY='value' lines and uploaded as ONE small file that
   the session mounts read-only at /workspace/.cygenix/db.env. Not the
   system prompt (which is kept in the session history), and not an
   environment variable, because a cloud workspace has none to set: the
   API's only secret mechanism keeps the value out of the sandbox and
   substitutes it into web requests, and a database login is not a web
   request. The file is deleted when the session stops or ends, and expires
   on its own after a day in case that never happens. The owner made this
   trade knowingly: their own code sees their own password, and if Claude
   prints it, Anthropic's copy of the session holds it.

   WHAT IS KEPT HERE
   Cosmos container claude_code_sessions, partitioned on /userId (the
   verified email, the house convention): one document per session, and the
   events in child documents of EVENT_CHUNK each — a long session's tool
   output would otherwise walk a single item towards the 2 MB limit. Before
   any event text is stored OR returned, the password and the connection
   string are replaced with '••••••', by literal and by pattern. The person
   can still see them inside the workspace if they ask; we do not keep them.

   THE CONNECTIVITY TEST
   Built first, because the documentation left two questions open: can a
   cloud workspace open a raw TCP connection to a database, and from which
   address. A throwaway session runs a fixed Python script that resolves the
   name, opens the socket, speaks the first bytes of the database's own
   protocol (a SQL Server PRELOGIN packet, a PostgreSQL SSLRequest) so a
   proxy that accepts every connection cannot pass for a database, and asks
   api.ipify.org for its public address. No login, no data. It answered the
   question — the workspace reached the database — and it stays for the
   next customer's firewall.
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
const AGENT_SPEC = '2';
const AGENT_NAME = 'Cygenix Dev Console';
const GATE_TTL_MS = 30 * 1000;
const RESOLVE_TTL_MS = 10 * 60 * 1000;
const LIST_CAP = 200;               // never walk more than this many agents/environments
const IP_ECHO_HOST = 'api.ipify.org';
const PACKAGE_HOSTS = ['pypi.org', 'files.pythonhosted.org', 'registry.npmjs.org'];
const CRED_PATH = '/workspace/.cygenix/db.env';
const CRED_TTL_S = 24 * 60 * 60;    // the file expires by itself if a stop never comes
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
// a console whose promise is "the workspace reaches your database and the
// package registries, nothing else" cannot have a side door. The system
// prompt here is a placeholder; every session overrides it with its own.
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
// The connectivity test uses the same shape, and the same environment for
// the same host: an earlier version gave it a variant with package managers
// on and drivers pre-installed, and that was one of the differences under
// suspicion when its turns came back empty. Package managers stay off; the
// registries are reachable by name, so pip and npm work all the same.
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
// password that itself contains ';' is written (Password="p;w" or 'p;w').
function adoParts(s) {
  const parts = []; let cur = '', q = '';
  for (const ch of s) {
    if (q) { cur += ch; if (ch === q) q = ''; continue; }
    if (ch === '"' || ch === "'") { q = ch; cur += ch; continue; }
    if (ch === ';') { parts.push(cur); cur = ''; continue; }
    cur += ch;
  }
  parts.push(cur);
  return parts;
}
function adoValue(v) {
  const t = v.trim();
  const m = /^"(.*)"$/s.exec(t) || /^'(.*)'$/s.exec(t);
  return m ? m[1] : t;
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

// The one file the workspace gets. KEY='value' lines, single-quoted so a
// shell can source them ( set -a; . the file; set +a ) and a Python one-liner
// can split them. Every value is the connection's own, nothing invented.
function shq(v) { return "'" + String(v == null ? '' : v).replace(/'/g, "'\\''") + "'"; }
function credFile(p, connString) {
  return [
    '# Cygenix Dev Console — this session\'s database connection. Read-only.',
    'CYG_DB_TYPE=' + shq(p.kind),
    'CYG_DB_HOST=' + shq(p.host),
    'CYG_DB_PORT=' + shq(p.port),
    'CYG_DB_NAME=' + shq(p.database),
    'CYG_DB_USER=' + shq(p.user),
    'CYG_DB_PASSWORD=' + shq(p.password),
    'CYG_DB_CONNSTR=' + shq(connString),
    '',
  ].join('\n');
}

// ── The system prompt ────────────────────────────────────────────────────
// Short, and target-agnostic: nothing here knows which product the
// database belongs to. Names of things, never their values.
const MODE_TEXT = {
  readonly: 'DATA-CHANGE MODE: READ-ONLY. The user has not allowed changes to data in this session. '
    + 'Run only reads — SELECT statements and read-only scripts. Do not INSERT, UPDATE, DELETE, MERGE, TRUNCATE, '
    + 'create, alter or drop anything, or run any statement that changes data or schema. If the user asks for a '
    + 'change, explain that "Allow changes to data this session" is off and ask them to switch it on first.',
  changes: 'DATA-CHANGE MODE: CHANGES ALLOWED. The user has allowed changes to data in this session. Before running '
    + 'anything that modifies data or schema, show the exact SQL or code and say what it will do. Prefer a '
    + 'transaction for multi-statement changes so a failure part-way leaves nothing half done.',
};
function systemPrompt(o) {
  const kind = o.dbType === 'postgres' ? 'PostgreSQL' : 'SQL Server';
  return [
    'You are working inside Cygenix, a data migration console, on data migration work for the signed-in user. '
      + 'You can write and run code in this workspace: Python, Node, shell and SQL.',
    // The connection test (credentials: false) carries no login and no
    // file: the same prompt otherwise, with that one paragraph replaced.
    o.credentials === false
      ? 'The database for this session is ' + kind + '. This session carries no database login: it is a connectivity check '
        + 'the user runs before saving one.'
      : 'The database for this session is ' + kind + '. Its connection details are in the read-only file ' + CRED_PATH
      + ", as KEY='value' lines: CYG_DB_TYPE, CYG_DB_HOST, CYG_DB_PORT, CYG_DB_NAME, CYG_DB_USER, CYG_DB_PASSWORD and "
      + 'CYG_DB_CONNSTR (the full connection string). Load them into the environment before running code, for example '
      + '`set -a; . ' + CRED_PATH + '; set +a`, and read them from the environment in your code.',
    o.dbType === 'postgres'
      ? 'psql is installed. From Python use psycopg (pip install "psycopg[binary]"); from Node use pg.'
      : 'From Python use pymssql (pip install pymssql) — pyodbc needs a driver this workspace cannot download. From Node use mssql.',
    'The workspace can reach the database host, the Python and npm package registries, and ' + IP_ECHO_HOST
      + ' (so `curl -s https://' + IP_ECHO_HOST + '` tells the user this workspace\'s public IP address, for a firewall rule) — and nothing else on the network.',
    MODE_TEXT[o.mode === 'changes' ? 'changes' : 'readonly'],
    'Show SQL or code before running anything that modifies data.' + (o.credentials === false ? '' : ' Never print the password or the contents of '
      + CRED_PATH + ' unless the user explicitly asks you to.'),
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

// ── session ──────────────────────────────────────────────────────────────
function str(v, max) { return String(v == null ? '' : v).trim().slice(0, max || 200); }
async function sessionStart(who, apiKey, body, ctx) {
  const connId = str(body.connId, 100);
  if (!CONN_ID_RE.test(connId)) return bad(400, 'Choose a connection first.');
  const side = body.side === 'src' ? 'src' : 'tgt';
  const names = { profileId: str(body.profileId, 100), profileName: str(body.profileName, 120),
                  connectionName: str(body.connectionName, 120) || connId };
  const container = await deps.container();

  const secret = await deps.readSecret(who.oid, connId);
  if (!secret.ok) {
    if (secret.code === 'no-secrets-key') return bad(503, 'The secrets store is not configured on the server (CONN_SECRETS_KEY).');
    if (secret.code === 'undecryptable') return bad(409, 'The saved credential for "' + names.connectionName + '" cannot be opened with the server\'s current key. Save the connection again.');
    return bad(409, 'The credential for "' + names.connectionName + '" has not been saved to the cloud from this browser. Open Connections, check it is there, and let it sync — then try again.');
  }
  if (!secret.bundle.connString) {
    return bad(409, 'The Dev Console needs a direct database connection. "' + names.connectionName + '" is a '
      + (secret.bundle.fnKey ? 'Function App connection' : 'stream destination') + ', which the workspace cannot use.');
  }
  const p = parseConn(secret.bundle.connString);
  if (!p.ok) return bad(409, p.why);

  const g = await gate(who, 'use', { record: 'session.start', detail: { profile: names.profileName, connection: names.connectionName, host: p.host } });
  if (!g.ok) return g.response;

  const client = deps.makeClient(apiKey);
  const tag = keyTag(apiKey);
  const agentId = await ensureAgent(client, tag);
  const environmentId = await ensureEnvironment(client, tag, 'limited', [p.host].concat(PACKAGE_HOSTS, [IP_ECHO_HOST]));

  const file = await client.beta.files.upload({
    file: await deps.toFile(Buffer.from(credFile(p, secret.bundle.connString), 'utf8'), 'db.env'),
    expires_in_seconds: CRED_TTL_S,
  });
  let session;
  try {
    session = await client.beta.sessions.create({
      agent: { type: 'agent_with_overrides', id: agentId, model: { id: MODEL(), effort: 'medium' },
               system: systemPrompt({ dbType: p.kind, mode: 'readonly' }) },
      environment_id: environmentId,
      title: 'Cygenix Dev Console — ' + names.connectionName,
      metadata: { cygenix: 'console', cyg_oid: who.oid },
      budget: budget(),
      resources: [{ type: 'file', file_id: file.id, mount_path: CRED_PATH }],
    });
  } catch (e) {
    await deleteFile(client, file.id);
    throw e;
  }
  const now = new Date(deps.now()).toISOString();
  const doc = {
    id: session.id, kind: 'session', userId: who.email, oid: who.oid, tenantId: g.tenantId || '',
    title: '', status: 'idle', dataChangesAllowed: false,
    profileId: names.profileId, profileName: names.profileName, connectionId: connId,
    connectionName: names.connectionName, side, dbType: p.kind, dbHost: p.host, dbName: p.database,
    createdAt: now, updatedAt: now, endedAt: null,
    agentId, environmentId, fileId: file.id, model: MODEL(),
    cursorAt: null, cursorIds: [], chunkCount: 0, eventCount: 0, costCents: null, stopReason: null,
  };
  await container.items.upsert(doc);
  ctx.log('[claude-code] session opened ' + session.id + ' db=' + p.kind);
  return ok({ session: publicSession(doc) });
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
    await deleteFile(client, doc.fileId);
    doc.fileId = null;
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
  await deleteFile(client, doc.fileId);
  doc.fileId = null;
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
  const taken = new Set((doc.uploads || []).map(u => u.path));
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
      content: [{ type: 'text', text: 'The user attached a file: ' + name + ' (' + buf.length + ' bytes), mounted read-only at ' + mountPath + '.' }] }] });
  } catch (e) { /* the path is shown to the person */ }
  const rec = { fileId: file.id, name, path: mountPath, size: buf.length, at: new Date(deps.now()).toISOString() };
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
// Python literals for anything validateProbe lets through.
//
// History, because every version below was built on a wrong guess. v1 spoke
// each database's opening handshake in raw bytes and the model's turn came
// back empty — zero output tokens, no refusal, no error. v2 replaced that
// with a normal driver login under a made-up account: still empty. v3 moved
// the target into a mounted file so the prompt named no address: still
// empty. v4 mounted the script too and asked in one sentence: still empty.
// The raw model request finally showed the shape of it: ~4,800 input
// tokens, 0 output, is_error false, three requests running. Meanwhile an
// ordinary console session on the same key, the same agent and the same
// model works every time. So the fault was in what the test did
// DIFFERENTLY from the console — its own system prompt, its own
// environment shape (package managers on, drivers pre-installed), its
// message sent as an initial event — not in the script or the target.
//
// v5 therefore does exactly what a console session does: the console's
// system prompt, the console's environment shape, a session created idle
// and then ONE ordinary user message with the script inline. Only the
// networking setting differs (allow-list or open). The script tries a
// driver login under an obviously made-up account when the driver is
// there — "Login failed" IS the answer wanted, the server was reached and
// replied — and says 'no-driver' otherwise. No credential of the person's
// is involved at any point.
function probeScript(p) {
  const H = JSON.stringify(p.host); const P = JSON.stringify(p.port); const K = JSON.stringify(p.kind);
  return [
    'import socket, json, time, urllib.request',
    'H = ' + H, 'P = ' + P, 'KIND = ' + K,
    '# A made-up account: the server refusing it is the reply being checked for.',
    'U = "cygenix_probe"; PW = "cygenix-probe"',
    'r = {"host": H, "port": P, "kind": KIND}',
    'try:',
    '    r["dns"] = sorted({a[4][0] for a in socket.getaddrinfo(H, P, proto=socket.IPPROTO_TCP)})',
    'except Exception as e:',
    '    r["dns_error"] = type(e).__name__ + ": " + str(e)',
    't = time.time()',
    'try:',
    '    s = socket.create_connection((H, P), timeout=10)',
    '    r["tcp"] = "open"',
    '    s.close()',
    'except Exception as e:',
    '    r["tcp"] = "failed"',
    '    r["tcp_error"] = type(e).__name__ + ": " + str(e)',
    'r["tcp_ms"] = int((time.time() - t) * 1000)',
    'if r["tcp"] == "open" and KIND == "sqlserver":',
    '    try:',
    '        import pymssql',
    '        try:',
    '            pymssql.connect(server=H, port=P, user=U, password=PW, database="master", login_timeout=10)',
    '            r["handshake"] = "sqlserver-replied"',
    '        except Exception as e:',
    '            m = str(e)',
    '            r["handshake"] = "sqlserver-replied" if ("Login failed" in m or "18456" in m or "Cannot open database" in m or ("login" in m.lower() and "timeout" not in m.lower())) else "error: " + m[:300]',
    '    except ImportError:',
    '        r["handshake"] = "no-driver"',
    'elif r["tcp"] == "open" and KIND == "postgres":',
    '    try:',
    '        import psycopg',
    '        try:',
    '            psycopg.connect(host=H, port=P, user=U, password=PW, dbname="postgres", connect_timeout=10)',
    '            r["handshake"] = "postgres-replied"',
    '        except Exception as e:',
    '            m = str(e)',
    '            r["handshake"] = "postgres-replied" if ("authentication failed" in m or "does not exist" in m or "pg_hba.conf" in m or "no encryption" in m or "SSL" in m) else "error: " + m[:300]',
    '    except ImportError:',
    '        r["handshake"] = "no-driver"',
    'try:',
    '    r["egress_ip"] = urllib.request.urlopen("https://' + IP_ECHO_HOST + '", timeout=10).read().decode().strip()',
    'except Exception as e:',
    '    r["egress_ip_error"] = type(e).__name__ + ": " + str(e)',
    'print("CYGPROBE_RESULT " + json.dumps(r))',
  ].join('\n');
}

function probeInstruction(p) {
  const driver = p.kind === 'sqlserver' ? 'pymssql' : p.kind === 'postgres' ? '"psycopg[binary]"' : '';
  return 'Run this Python script with bash and paste its output verbatim (it only checks whether this workspace can reach '
    + 'my database server, for a firewall rule).' + (driver ? ' If the driver is missing, `pip install ' + driver + '` first.' : '')
    + '\n\n```python\n' + probeScript(p) + '\n```';
}

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
  // What the workspace said and did, for the case where no result line came
  // back: without it a person sees "no result" and nothing to act on. No
  // credential is involved in a test, so nothing here needs blanking.
  const clip = (t) => String(t || '').replace(/\s+$/, '').slice(-1200);
  const transcript = [];
  // Every event type, in order, so an unexpected shape is visible too.
  // ...with the model's output-token count on each request and on the
  // usage line: it tells an empty turn with nothing written apart from
  // one whose reply never reached the event list.
  const outTok = (ev) => {
    const u = ev.model_usage || ev.usage || ev;
    return u && typeof u.output_tokens === 'number' ? ':out=' + u.output_tokens : '';
  };
  const eventTypes = events.map(ev => ev.type + (ev.stop_reason && ev.stop_reason.type ? ':' + ev.stop_reason.type : '')
    + (ev.is_error ? ':error' : '') + (ev.error && ev.error.type ? ':' + ev.error.type : '')
    + (ev.type === 'span.model_request_end' || ev.type === 'session.usage' ? outTok(ev) : ''));
  events.forEach(ev => {
    if (ev.type === 'agent.message') {
      const blocks = Array.isArray(ev.content) ? ev.content : [];
      // A reply Anthropic's safety system withheld arrives as a redacted
      // block and nothing else. Say so, rather than showing a blank.
      if (blocks.length && blocks.every(b => b && b.type === 'redacted')) {
        transcript.push({ kind: 'refused', text: 'Claude\'s reply was withheld by Anthropic\'s safety system — the request was declined.' });
      } else transcript.push({ kind: 'message', text: clip(textsOf(ev).join('\n')) });
    } else if (ev.type === 'span.model_request_end' && ev.is_error) {
      transcript.push({ kind: 'error', text: 'A model request failed.' });
    } else if (ev.type === 'agent.tool_use') {
      const input = ev.input || {};
      transcript.push({ kind: 'tool', text: clip(ev.name === 'bash' ? input.command : (ev.name || 'tool') + ' ' + JSON.stringify(input)) });
    } else if (ev.type === 'agent.tool_result') transcript.push({ kind: ev.is_error ? 'error' : 'output', text: clip(textsOf(ev).join('\n')) });
  });
  // The last model request and the usage line, exactly as Anthropic sent
  // them: our summary has been wrong about what an empty turn holds, so
  // the page shows the real thing. Scrubbed of anything key-shaped.
  const raw = scrubKeys({
    modelRequestEnd: events.filter(ev => ev.type === 'span.model_request_end').pop() || null,
    usage: events.filter(ev => ev.type === 'session.usage').pop() || null,
  });
  // A turn in which the model wrote nothing at all: zero output tokens on
  // the last request, no error, nothing said or run. Named as such, rather
  // than folded into "no reply".
  const mu = raw && raw.modelRequestEnd && raw.modelRequestEnd.model_usage;
  const emptyTurn = !!(mu && mu.output_tokens === 0 && !raw.modelRequestEnd.is_error && transcript.length === 0);
  return { result, errors, stopReason: idle && idle.stop_reason ? idle.stop_reason.type : null, transcript: transcript.slice(-12), eventTypes, raw, emptyTurn };
}
function scrubKeys(o) {
  try { return JSON.parse(JSON.stringify(o).replace(/sk-ant-[A-Za-z0-9_-]{8,}/g, '[key]')); }
  catch (e) { return null; }
}

function verdict(r) {
  if (!r) return null;
  if (r.dns_error) return { ok: false, text: 'The workspace could not resolve ' + r.host + ' (' + r.dns_error + ').' };
  if (r.tcp !== 'open') return { ok: false, text: 'The workspace could not open a connection to ' + r.host + ':' + r.port + ' (' + (r.tcp_error || 'failed') + ').' };
  if (r.kind === 'other') return { ok: true, text: 'A connection to ' + r.host + ':' + r.port + ' opened.' };
  if (/-replied$/.test(r.handshake || '')) return { ok: true, text: 'The database at ' + r.host + ':' + r.port + ' answered.' };
  if (r.handshake === 'no-driver') return { ok: true, text: 'A connection to ' + r.host + ':' + r.port + ' opened; the database driver was not available in the workspace to confirm a reply.' };
  return { ok: false, text: 'A connection opened, but the database did not answer a login attempt on it (' + (r.handshake || 'no reply') + ') — something between the workspace and ' + r.host + ' may be accepting the connection without passing it on.' };
}

async function probeStart(who, apiKey, body) {
  const p = validateProbe(body);
  if (!p.ok) return bad(400, p.error);
  const g = await gate(who, 'probe', { record: 'probe', detail: { host: p.host, port: p.port, network: p.network } });
  if (!g.ok) return g.response;
  const client = deps.makeClient(apiKey);
  const tag = keyTag(apiKey);
  const agentId = await ensureAgent(client, tag);
  // The console's environment shape exactly — the host, the registries,
  // the address echo — with only the networking setting under test.
  const environmentId = await ensureEnvironment(client, tag, p.network, [p.host].concat(PACKAGE_HOSTS, [IP_ECHO_HOST]));
  const session = await client.beta.sessions.create({
    agent: { type: 'agent_with_overrides', id: agentId, model: { id: MODEL(), effort: 'medium' },
             system: systemPrompt({ dbType: p.kind, mode: 'readonly', credentials: false }) },
    environment_id: environmentId,
    title: 'Cygenix connection test',
    metadata: { cygenix: 'probe', cyg_oid: who.oid },
    budget: budget(),
  });
  // Then the message, the way the console sends its first one.
  await client.beta.sessions.events.send(session.id, { events: [{ type: 'user.message', content: [{ type: 'text', text: probeInstruction(p) }] }] });
  return ok({ sessionId: session.id, status: 'running', network: p.network, host: p.host, port: p.port });
}

async function probeResult(who, apiKey, sessionId) {
  if (!SESSION_ID_RE.test(String(sessionId || ''))) return bad(400, 'sessionId is missing or malformed.');
  const g = await gate(who, 'probe');
  if (!g.ok) return g.response;
  const client = deps.makeClient(apiKey);
  let session;
  try { session = await client.beta.sessions.retrieve(sessionId); }
  catch (e) { if (e && e.status === 404) return bad(404, 'No such test session.'); throw e; }
  if (!session.metadata || session.metadata.cyg_oid !== who.oid || session.metadata.cygenix !== 'probe') {
    return bad(404, 'No such test session.');
  }
  const events = [];
  for await (const ev of client.beta.sessions.events.list(sessionId, { order: 'asc', limit: 100 })) {
    events.push(ev);
    if (events.length >= 400) break;
  }
  const parsed = parseProbeEvents(events);
  // Finished means the turn ended: an idle session that has not yet been
  // sent its message (the moment between create and send) is not done.
  // No retry: an empty turn is reported as one, with the raw request.
  const finished = session.status === 'terminated'
    || (session.status === 'idle' && (parsed.stopReason || parsed.result || parsed.errors.length));
  if (finished && !session.archived_at) {
    try { await client.beta.sessions.archive(sessionId); } catch (e) { /* ignore */ }
  }
  const cost = session.usage && session.usage.list_cost;
  return ok({
    sessionId, status: session.status, done: !!finished, retrying: false,
    result: parsed.result, verdict: verdict(parsed.result), errors: parsed.errors,
    stopReason: parsed.stopReason,
    transcript: finished && !parsed.result ? parsed.transcript : [],
    eventTypes: finished && !parsed.result ? parsed.eventTypes : [],
    raw: finished ? parsed.raw : null,
    emptyTurn: !!(finished && !parsed.result && parsed.emptyTurn),
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

// ── The route ────────────────────────────────────────────────────────────
const ACTIONS = {
  probe:    { GET: (who, key, req) => probeResult(who, key, req.query.get('sessionId')), POST: (who, key, req, body) => probeStart(who, key, body) },
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

module.exports = {
  deps, handler, identify, gate, siteUrl, budget, agentSpec, ensureAgent, ensureEnvironment,
  environmentName, environmentConfig, parseConn, credFile, systemPrompt, MODE_TEXT, makeRedactor,
  ourStatus, publicSession, chunkId,
  sessionStart, sessionMessage, sessionEvents, sessionMode, sessionStop, sessionList, sessionGet,
  sessionUpload, sessionOutputs, sessionDownload, safeName, uniquePath, decodeBase64, UPLOAD_DIR, UPLOAD_MAX, DOWNLOAD_MAX,
  validateProbe, probeScript, probeInstruction, parseProbeEvents, verdict, probeStart, probeResult, fromAnthropic,
  _reset: () => { gateCache.clear(); resolved.clear(); },
  AGENT_SPEC, IP_ECHO_HOST, PACKAGE_HOSTS, CRED_PATH, CONTAINER, EVENT_CHUNK, MASK,
};
