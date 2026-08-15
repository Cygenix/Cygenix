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

  const res = await credential.getToken('https://database.windows.net/.default');
  if (!res || !res.token) {
    throw new Error('Entra returned no access token for the database scope.');
  }
  return res.token;
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

module.exports = {
  entraModeFrom,
  isEntraConn,
  applyEntraAuth,
  acquireSqlToken,
  // Exported for tests.
  canonical,
};
