// sql-entra.js — Microsoft Entra authentication for Azure SQL connections.
//
// An Azure SQL server can be set to "Microsoft Entra-only authentication",
// which disables SQL logins entirely. Every connection Cygenix opened was
// server/database/user/password, so against such a server every scheduled run,
// every Connect & test and every schema read failed at login.
//
// This module does not replace the existing connection-string parsers. It takes
// the config they already produce and, when the string asks for Entra, swaps
// the username/password for a token. Keeping it additive means SQL-auth
// connections take exactly the same path they always did.
//
// ── Connection-string syntax ───────────────────────────────────────────────
// URL form, the Function App's own identity (Managed Identity):
//   mssql://cygenix.database.windows.net:1433/MyDb?authentication=ActiveDirectoryDefault
//
// URL form, an app registration:
//   mssql://<clientId>:<clientSecret>@host:1433/MyDb
//       ?authentication=ActiveDirectoryServicePrincipal&tenantId=<tenant guid>
//
// ADO.NET form (what the Azure portal hands you):
//   Server=host;Database=MyDb;Authentication=Active Directory Default;Encrypt=True;
//   Server=host;Database=MyDb;Authentication=Active Directory Service Principal;
//       User Id=<clientId>;Password=<secret>;Tenant Id=<guid>;
//
// ── Where each mode works ──────────────────────────────────────────────────
// ActiveDirectoryDefault resolves through DefaultAzureCredential, so it picks
// up a Managed Identity when the code runs in Azure — which the Function App
// does, and which is how scheduled runs should connect. On Netlify there is no
// managed identity, so Default only works there if AZURE_CLIENT_ID /
// AZURE_TENANT_ID / AZURE_CLIENT_SECRET are set in the environment.
// ServicePrincipal works anywhere, because the credentials are explicit.
//
// ── Keep in sync ───────────────────────────────────────────────────────────
// An identical copy lives at azure-function/src/sql-entra.js. The two runtimes
// deploy separately so the file cannot simply be shared; tests/sql-entra.test.js
// asserts the copies have not drifted.

// Everything below the protocol is compared case- and punctuation-insensitively,
// so "Active Directory Default", "ActiveDirectoryDefault" and
// "azure-active-directory-default" all mean the same thing.
function canonical(s) {
  return String(s == null ? '' : s).toLowerCase().replace(/[^a-z]/g, '');
}

const MODES = {
  // The identity the process is already running as.
  activedirectorydefault:            'default',
  azureactivedirectorydefault:       'default',
  activedirectorymanagedidentity:    'default',
  activedirectorymsi:                'default',
  azureactivedirectorymsiappservice: 'default',
  managedidentity:                   'default',
  msi:                               'default',
  entra:                             'default',
  // An app registration: client id + secret + tenant.
  activedirectoryserviceprincipal:              'service-principal',
  azureactivedirectoryserviceprincipalsecret:   'service-principal',
  serviceprincipal:                             'service-principal',
};

// Modes that only make sense with a human at the keyboard. Naming one is a
// configuration mistake worth reporting rather than silently falling back to
// SQL auth, which would fail later with a confusing login error.
const INTERACTIVE = new Set([
  'activedirectoryinteractive',
  'activedirectoryintegrated',
  'activedirectorypassword',          // ROPC — blocked by MFA, and Entra-only
  'azureactivedirectorypassword',     // servers are exactly where MFA is on
]);

// Read the Entra request out of a connection string, from either syntax.
// Returns null when the string does not ask for Entra at all.
function entraModeFrom(connStr) {
  const cs = String(connStr == null ? '' : connStr).trim();
  if (!cs) return null;

  const get = (names) => {
    for (const n of names) {
      // URL query (?authentication=…) or ADO.NET pair (;Authentication=…).
      // Both are matched with one expression so the two syntaxes cannot drift.
      // `^` matters: an ADO.NET string may lead with the key, as the Azure
      // portal's "Authentication=Active Directory Default;Server=…" does.
      const re = new RegExp('(^|[?&;])\\s*' + n + '\\s*=\\s*([^&;]+)', 'i');
      const m = re.exec(cs);
      if (m) return decodeURIComponent(m[2].trim());
    }
    return '';
  };

  // Names are regex fragments: ADO.NET spells these with a space ("Tenant Id"),
  // URL query strings without one ("tenantId"), so both have to match.
  const raw = get(['authentication', 'auth']);
  if (!raw) return null;

  const key = canonical(raw);
  if (INTERACTIVE.has(key)) {
    return { mode: 'unsupported', requested: raw };
  }
  const mode = MODES[key];
  if (!mode) return { mode: 'unknown', requested: raw };

  return {
    mode,
    tenantId:     get(['tenant\\s*id', 'tenant']) || process.env.AZURE_TENANT_ID || '',
    clientId:     get(['client\\s*id', 'application\\s*id']) || '',
    clientSecret: get(['client\\s*secret']) || '',
  };
}

function isEntraConn(connStr) {
  const m = entraModeFrom(connStr);
  return !!m && (m.mode === 'default' || m.mode === 'service-principal');
}

// Acquire an Azure SQL access token.
//
// Returning a token and using 'azure-active-directory-access-token' — rather
// than handing tedious a credential type — matches how the rest of the Azure
// Function App already connects, and keeps one code path for both modes.
async function acquireSqlToken(desc) {
  const credential = cachedCredential(desc);

  const res = await credential.getToken('https://database.windows.net/.default');
  if (!res || !res.token) {
    throw new Error('Entra returned no access token for the database scope.');
  }
  return res.token;
}

// One credential instance per distinct identity, held for the life of the
// process. @azure/identity caches tokens per credential INSTANCE, so a fresh
// credential per request re-runs the whole acquisition — and for
// DefaultAzureCredential that means walking the probe chain (including a
// managed-identity IMDS probe that, on Netlify, has nothing to answer it and
// must time out) before every single query.
const _credCache = new Map();
function cachedCredential(desc) {
  const key = [desc.mode, desc.tenantId || '', desc.clientId || ''].join('|');
  if (_credCache.has(key)) return _credCache.get(key);

  let identity;
  try {
    identity = require('@azure/identity');
  } catch {
    throw new Error(
      'Entra authentication needs the @azure/identity package, which is not installed in this runtime.'
    );
  }

  let credential;
  if (desc.mode === 'service-principal') {
    // Credentials can come from the connection string or, preferably, the
    // environment — a secret in a connection string ends up in Cosmos.
    const tenantId     = desc.tenantId     || process.env.AZURE_TENANT_ID     || '';
    const clientId     = desc.clientId     || process.env.AZURE_CLIENT_ID     || '';
    const clientSecret = desc.clientSecret || process.env.AZURE_CLIENT_SECRET || '';
    const missing = [
      !tenantId     && 'tenant id',
      !clientId     && 'client id',
      !clientSecret && 'client secret',
    ].filter(Boolean);
    if (missing.length) {
      throw new Error(
        'Entra service-principal authentication needs a ' + missing.join(', ') +
        '. Supply them in the connection string (User Id / Password / Tenant Id) ' +
        'or as AZURE_CLIENT_ID / AZURE_CLIENT_SECRET / AZURE_TENANT_ID.'
      );
    }
    credential = new identity.ClientSecretCredential(tenantId, clientId, clientSecret);
  } else {
    // A user-assigned managed identity needs to be named; a system-assigned
    // one is picked up without any client id.
    credential = desc.clientId
      ? new identity.DefaultAzureCredential({ managedIdentityClientId: desc.clientId })
      : new identity.DefaultAzureCredential();
  }
  _credCache.set(key, credential);
  return credential;
}

// Turn a SQL-auth mssql config into an Entra one, if the string asks for it.
//
// Returns the config unchanged when Entra was not requested, so every existing
// SQL-auth connection is byte-for-byte the same as before.
async function applyEntraAuth(config, connStr) {
  const desc = entraModeFrom(connStr);
  if (!desc) return config;

  if (desc.mode === 'unsupported') {
    throw new Error(
      '"' + desc.requested + '" needs a person at a browser and cannot be used by a ' +
      'scheduled run or a server-side connection. Use Active Directory Default ' +
      '(the app\'s managed identity) or Active Directory Service Principal.'
    );
  }
  if (desc.mode === 'unknown') {
    throw new Error(
      'Unrecognised authentication mode "' + desc.requested + '". Supported: ' +
      'Active Directory Default, Active Directory Service Principal.'
    );
  }

  // In service-principal mode the username/password slots carry the client id
  // and secret, which is how the Azure portal formats the string too.
  if (desc.mode === 'service-principal') {
    desc.clientId     = desc.clientId     || config.user     || '';
    desc.clientSecret = desc.clientSecret || config.password || '';
  }

  const token = await acquireSqlToken(desc);
  const out = { ...config };
  // Leaving these set makes tedious attempt SQL auth regardless of the
  // authentication block, so they have to go.
  delete out.user;
  delete out.password;
  out.options = {
    ...(config.options || {}),
    // Entra always requires TLS. A string that said encrypt=false would
    // otherwise produce a connection the server rejects.
    encrypt: true,
  };
  out.authentication = {
    type: 'azure-active-directory-access-token',
    options: { token },
  };
  return out;
}

// ── The app's own identity ────────────────────────────────────────────────
// Inside the Azure Function App the platform provides a system-assigned
// managed identity, and SQL_SERVER / SQL_DATABASE name the database it has
// been granted on. Nothing else is needed: no username, no password, no
// connection string.
//
// This uses tedious' 'azure-active-directory-default' type rather than
// pre-fetching a token, because the driver then owns the token's lifetime and
// re-acquires it when it reconnects. A token fetched once and pinned into a
// pool config goes stale after about an hour, and the failure shows up as a
// login error on whichever request happens to trigger a reconnect.
function managedIdentityTarget() {
  const server   = process.env.SQL_SERVER   || '';
  const database = process.env.SQL_DATABASE || '';
  return (server && database) ? { server, database } : null;
}

function managedIdentityConfig(target) {
  const t = target || managedIdentityTarget();
  if (!t) {
    throw new Error(
      'Managed-identity connection needs SQL_SERVER and SQL_DATABASE to be set. ' +
      'They are app settings on the Azure Function App and are not available in other runtimes.'
    );
  }
  return {
    server:   t.server,
    database: t.database,
    port:     1433,
    options: { encrypt: true, trustServerCertificate: false, enableArithAbort: true },
    authentication: { type: 'azure-active-directory-default' },
  };
}

// Decide how a caller should connect, given whatever connection string it has.
//
//   1. The string names an Entra mode      → token, as before.
//   2. The string carries no credentials   → the app's managed identity, using
//      (or there is no string at all)        the string's server/database when
//                                            it has them, otherwise the env.
//   3. Anything else                       → untouched; SQL auth as before.
//
// Rule 2 is what makes an Entra-only server work without inventing a syntax:
// a connection string with no username cannot be a SQL login, so the only
// thing it can mean is "connect as whoever this app is".
async function resolveSqlConfig(baseConfig, connStr) {
  const cs = String(connStr == null ? '' : connStr).trim();

  if (entraModeFrom(cs)) return applyEntraAuth(baseConfig, cs);

  const hasCredentials = !!((baseConfig && baseConfig.user) || (baseConfig && baseConfig.password));
  if (hasCredentials) return baseConfig;

  const target = (baseConfig && baseConfig.server && baseConfig.database)
    ? { server: baseConfig.server, database: baseConfig.database }
    : managedIdentityTarget();

  const mi = managedIdentityConfig(target);
  // Preserve the caller's timeouts — the migration runner allows hours per
  // statement and must not silently fall back to the driver default. Both
  // conventions count: mssql's top-level keys and tedious' options.*, which
  // is where db-connect.js writes them. Merging options (rather than letting
  // managedIdentityConfig's hardcoded block replace the caller's) is what
  // carries the options-convention values through.
  const base = baseConfig || {};
  mi.options = { ...(base.options || {}), ...mi.options };
  const reqT  = base.requestTimeout    ?? (base.options && base.options.requestTimeout);
  const connT = base.connectionTimeout ?? (base.options && base.options.connectTimeout);
  if (reqT)  mi.requestTimeout    = reqT;
  if (connT) mi.connectionTimeout = connT;
  if (base.pool) mi.pool = base.pool;
  if (base.port) mi.port = base.port;
  return withSqlResilience(mi);
}


// ── Resilient connections ─────────────────────────────────────────────────
// Azure SQL serverless auto-pauses when idle and takes 30-60 seconds to
// resume; during the resume, connects either hang or fail with 40613. Three
// separate knobs must ALL outlast that window, or the first request after a
// pause times out no matter what the other two say:
//
//   connectionTimeout          mssql default 15s
//   pool.createTimeoutMillis   tarn default 30s -- silently caps every
//                              connect even when connectionTimeout is higher
//   pool.acquireTimeoutMillis  tarn default 30s
//
// And because a resume can outlast even a generous single attempt, transient
// failures need a retry: a single-shot connect against a resuming database
// fails 100% of the time however long it waits.
const SQL_CONNECT_TIMEOUT = 90000;
const SQL_REQUEST_TIMEOUT = 120000;

// Fill in the timeouts a config does not set, without overriding ones it
// does. Two conventions exist in this codebase: mssql's top-level keys and
// tedious' options.connectTimeout / options.requestTimeout. The driver reads
// options.* first (mssql/lib/tedious/connection-pool.js maps top-level keys
// into options only when options has none), so an explicit value in either
// place is honoured here the same way the driver would honour it.
function withSqlResilience(config) {
  const c = { ...(config || {}) };
  const opts = c.options || {};
  if (opts.connectTimeout == null && c.connectionTimeout == null) c.connectionTimeout = SQL_CONNECT_TIMEOUT;
  if (opts.requestTimeout == null && c.requestTimeout == null)    c.requestTimeout    = SQL_REQUEST_TIMEOUT;
  c.pool = {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000,
    acquireTimeoutMillis: SQL_CONNECT_TIMEOUT,
    createTimeoutMillis:  SQL_CONNECT_TIMEOUT,
    ...(c.pool || {}),
  };
  return c;
}

// The errors worth retrying: Azure's documented transient codes (40613 is
// the serverless-resume one) plus the socket-level ways the same conditions
// present. Anything else -- bad login, bad database name -- fails fast.
const TRANSIENT_SQL_NUMBERS = new Set([40613, 40197, 40501, 40540, 10928, 10929, 49918, 49919, 49920, 4060, 233, 64]);
const TRANSIENT_SQL_CODES   = new Set(['ETIMEOUT', 'ESOCKET', 'ECONNRESET', 'ECONNCLOSED', 'ECONNREFUSED']);

function isTransientSqlError(e) {
  if (!e) return false;
  const n = e.number ?? (e.originalError && e.originalError.info && e.originalError.info.number);
  if (TRANSIENT_SQL_NUMBERS.has(n)) return true;
  if (TRANSIENT_SQL_CODES.has(e.code)) return true;
  // AggregateError from a pool that collected several attempts.
  if (Array.isArray(e.errors)) return e.errors.some(isTransientSqlError);
  return false;
}

// Open a pool, retrying transient failures with exponential backoff. Takes
// the mssql module as an argument so this file stays dependency-free for the
// callers that only need the identity helpers.
//
// The pool object is recreated per attempt: a ConnectionPool whose connect()
// rejected is done for, and tarn can leave a half-open socket behind unless
// close() is called, which matters under exactly the burst of failures a
// resume produces.
async function connectWithRetry(mssql, config, { attempts = 4, log } = {}) {
  const cfg = withSqlResilience(config);
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    const pool = new mssql.ConnectionPool(cfg);
    try {
      return await pool.connect();
    } catch (e) {
      lastErr = e;
      try { await pool.close(); } catch {}
      if (!isTransientSqlError(e) || i === attempts - 1) throw e;
      const delay = Math.min(1000 * 2 ** i, 8000) + Math.floor(Math.random() * 500);
      if (log) log('SQL connect attempt ' + (i + 1) + ' failed transiently (' + (e.number || e.code || e.message) + '); retrying in ' + delay + 'ms');
      await new Promise((r) => setTimeout(r, delay));
    }
  }
  throw lastErr;
}

module.exports = {
  entraModeFrom,
  isEntraConn,
  applyEntraAuth,
  acquireSqlToken,
  managedIdentityTarget,
  managedIdentityConfig,
  resolveSqlConfig,
  withSqlResilience,
  isTransientSqlError,
  connectWithRetry,
  SQL_CONNECT_TIMEOUT,
  SQL_REQUEST_TIMEOUT,
  // Exported for tests.
  canonical,
};
