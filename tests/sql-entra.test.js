// Tests Microsoft Entra authentication for Azure SQL connections.
//
// An Azure SQL server set to "Microsoft Entra-only authentication" refuses SQL
// logins outright. Every connection Cygenix opened was
// server/database/user/password, so against such a server scheduled runs,
// Connect & test and schema reads all failed at login — the Task Manager could
// not work against it at all.
//
// The rule these tests hold to: a connection string that does NOT ask for
// Entra must come out the other side byte-for-byte unchanged, so nothing about
// the existing SQL-auth path moves.
const fs = require('fs');
const path = require('path');
const Module = require('module');

const NETLIFY_COPY = path.join(__dirname, '..', 'netlify', 'functions', 'lib', 'sql-entra.js');
const AZURE_COPY   = path.join(__dirname, '..', 'azure-function', 'src', 'sql-entra.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

// Stub @azure/identity so no network call and no real Azure credentials are
// needed. The stub records how it was constructed, which is how the tests
// below check that the right credential type was chosen.
//
// Injected into the require cache rather than patched onto Module.require:
// sql-entra requires @azure/identity lazily, inside acquireSqlToken, so a hook
// that is removed after the initial load would miss it and the tests would
// reach for a real managed identity.
let lastCredential = null;
const STUB = {
  DefaultAzureCredential: class {
    constructor(opts) { lastCredential = { kind: 'default', opts: opts || null }; }
    async getToken() { return { token: 'TOKEN_FROM_DEFAULT' }; }
  },
  ClientSecretCredential: class {
    constructor(tenantId, clientId, clientSecret) {
      lastCredential = { kind: 'service-principal', tenantId, clientId, clientSecret };
    }
    async getToken() { return { token: 'TOKEN_FROM_SP' }; }
  },
};
const identityPath = require.resolve('@azure/identity');
require.cache[identityPath] = new Module(identityPath, null);
require.cache[identityPath].filename = identityPath;
require.cache[identityPath].loaded = true;
require.cache[identityPath].exports = STUB;

delete require.cache[NETLIFY_COPY];
const E = require(NETLIFY_COPY);

const SQL_CFG = () => ({
  server: 'cygenix.database.windows.net', port: 1433, database: 'Conversion_DM',
  user: 'mtvne', password: 'secret',
  options: { encrypt: true, trustServerCertificate: false, enableArithAbort: true },
});

console.log('Azure SQL — Microsoft Entra authentication\n');

(async () => {
  // ── SQL auth is untouched ──────────────────────────────────────────────
  {
    const cfg = SQL_CFG();
    const out = await E.applyEntraAuth(cfg, 'mssql://mtvne:secret@cygenix.database.windows.net:1433/Conversion_DM');
    check('a SQL-auth connection string is returned unchanged', out === cfg);
    check('and keeps its username and password', out.user === 'mtvne' && out.password === 'secret');
    check('and gains no authentication block', out.authentication === undefined);
  }
  check('a SQL-auth string is not detected as Entra',
    E.isEntraConn('mssql://u:p@host/db') === false);
  check('an empty string is not detected as Entra', E.isEntraConn('') === false);
  check('null is not detected as Entra', E.isEntraConn(null) === false);

  // ── Managed identity (how a scheduled run should connect) ──────────────
  {
    lastCredential = null;
    const out = await E.applyEntraAuth(SQL_CFG(),
      'mssql://cygenix.database.windows.net:1433/Conversion_DM?authentication=ActiveDirectoryDefault');
    check('a default-credential string produces a token', out.authentication &&
      out.authentication.type === 'azure-active-directory-access-token', JSON.stringify(out.authentication));
    check('the token is the one the credential returned',
      out.authentication.options.token === 'TOKEN_FROM_DEFAULT');
    check('it uses DefaultAzureCredential', lastCredential && lastCredential.kind === 'default');
    // Left in place, tedious attempts SQL auth regardless of the auth block.
    check('the username is removed', out.user === undefined);
    check('the password is removed', out.password === undefined);
    check('the other config is preserved',
      out.server === 'cygenix.database.windows.net' && out.database === 'Conversion_DM' && out.port === 1433);
  }

  // Entra always requires TLS, so a string asking for encrypt=false must not
  // produce a connection the server will reject.
  {
    const cfg = SQL_CFG(); cfg.options.encrypt = false;
    const out = await E.applyEntraAuth(cfg, 'mssql://host/db?authentication=ActiveDirectoryDefault&encrypt=false');
    check('encryption is forced on for Entra', out.options.encrypt === true);
    check('the original config object is not mutated', cfg.options.encrypt === false);
  }

  // A user-assigned managed identity has to be named.
  {
    lastCredential = null;
    await E.applyEntraAuth(SQL_CFG(),
      'mssql://host/db?authentication=ActiveDirectoryDefault&clientId=1111-2222');
    check('a user-assigned identity passes its client id',
      lastCredential.opts && lastCredential.opts.managedIdentityClientId === '1111-2222',
      JSON.stringify(lastCredential));
  }

  // ── Service principal ──────────────────────────────────────────────────
  {
    lastCredential = null;
    // The config is what the existing parser would produce from this URL —
    // the client id lands in `user` and the secret in `password`.
    const parsed = { ...SQL_CFG(), user: 'APPID', password: 'APPSECRET' };
    const out = await E.applyEntraAuth(parsed,
      'mssql://APPID:APPSECRET@host:1433/db?authentication=ActiveDirectoryServicePrincipal&tenantId=TENANT');
    check('a service-principal string produces a token',
      out.authentication.options.token === 'TOKEN_FROM_SP');
    check('it uses ClientSecretCredential', lastCredential.kind === 'service-principal');
    // The portal formats the string this way: client id in User Id, secret in
    // Password. The parser puts them in config.user/password, so they have to
    // be picked up from there.
    check('the client id comes from the username slot', lastCredential.clientId === 'APPID');
    check('the secret comes from the password slot', lastCredential.clientSecret === 'APPSECRET');
    check('the tenant comes from the string', lastCredential.tenantId === 'TENANT');
  }

  // Explicit query parameters beat the username/password slots.
  {
    lastCredential = null;
    await E.applyEntraAuth(SQL_CFG(),
      'mssql://x:y@host/db?authentication=ServicePrincipal&clientId=EXPLICIT&clientSecret=SEKRIT&tenantId=T');
    check('explicit clientId/clientSecret win over the URL credentials',
      lastCredential.clientId === 'EXPLICIT' && lastCredential.clientSecret === 'SEKRIT');
  }

  // Missing pieces must say which, not fail at login with a vague error.
  {
    let msg = '';
    try {
      await E.applyEntraAuth({ server: 'h', database: 'd' },
        'mssql://host/db?authentication=ActiveDirectoryServicePrincipal');
    } catch (e) { msg = e.message; }
    check('a service principal with nothing supplied names what is missing',
      /tenant id/.test(msg) && /client id/.test(msg) && /client secret/.test(msg), msg);
  }

  // ── The ADO.NET syntax the Azure portal hands you ──────────────────────
  {
    lastCredential = null;
    const out = await E.applyEntraAuth(SQL_CFG(),
      'Server=cygenix.database.windows.net;Database=Conversion_DM;Authentication=Active Directory Default;Encrypt=True;');
    check('the portal\'s "Active Directory Default" spelling is understood',
      out.authentication && out.authentication.options.token === 'TOKEN_FROM_DEFAULT');
  }
  {
    lastCredential = null;
    await E.applyEntraAuth(SQL_CFG(),
      'Server=h;Database=d;Authentication=Active Directory Service Principal;User Id=CID;Password=SEC;Tenant Id=TID;');
    check('the portal\'s service-principal spelling is understood',
      lastCredential.kind === 'service-principal' && lastCredential.tenantId === 'TID',
      JSON.stringify(lastCredential));
  }
  // An ADO.NET string can lead with the key, with no separator before it.
  {
    lastCredential = null;
    const out = await E.applyEntraAuth(SQL_CFG(),
      'Authentication=Active Directory Default;Server=h;Database=d;');
    check('Authentication as the first token is still found',
      out.authentication !== undefined, JSON.stringify(out.authentication));
  }

  // ── Spelling tolerance ─────────────────────────────────────────────────
  for (const spelling of ['ActiveDirectoryDefault', 'Active Directory Default',
                          'azure-active-directory-default', 'ManagedIdentity',
                          'Active Directory Managed Identity', 'MSI']) {
    check('"' + spelling + '" means managed identity',
      E.isEntraConn('mssql://h/d?authentication=' + encodeURIComponent(spelling)) === true);
  }

  // ── Modes that need a human ────────────────────────────────────────────
  // Silently falling back to SQL auth would fail later with a confusing login
  // error; better to say why the mode cannot work unattended.
  for (const mode of ['Active Directory Interactive', 'Active Directory Integrated',
                      'Active Directory Password']) {
    let msg = '';
    try {
      await E.applyEntraAuth(SQL_CFG(), 'mssql://h/d?authentication=' + encodeURIComponent(mode));
    } catch (e) { msg = e.message; }
    check('"' + mode + '" is refused with a reason',
      /cannot be used by a scheduled run|needs a person/i.test(msg), msg);
  }
  {
    let msg = '';
    try { await E.applyEntraAuth(SQL_CFG(), 'mssql://h/d?authentication=Nonsense'); }
    catch (e) { msg = e.message; }
    check('an unrecognised mode lists what is supported',
      /Unrecognised authentication mode/.test(msg) && /Service Principal/.test(msg), msg);
  }

  // ── Environment fallback ───────────────────────────────────────────────
  // Preferred over putting a secret in a connection string, since the string
  // is stored on the schedule document.
  {
    const saved = [process.env.AZURE_TENANT_ID, process.env.AZURE_CLIENT_ID, process.env.AZURE_CLIENT_SECRET];
    process.env.AZURE_TENANT_ID = 'ENV_T';
    process.env.AZURE_CLIENT_ID = 'ENV_C';
    process.env.AZURE_CLIENT_SECRET = 'ENV_S';
    lastCredential = null;
    await E.applyEntraAuth({ server: 'h', database: 'd' },
      'mssql://host/db?authentication=ActiveDirectoryServicePrincipal');
    check('service-principal credentials can come from the environment',
      lastCredential.tenantId === 'ENV_T' && lastCredential.clientId === 'ENV_C' &&
      lastCredential.clientSecret === 'ENV_S', JSON.stringify(lastCredential));
    [process.env.AZURE_TENANT_ID, process.env.AZURE_CLIENT_ID, process.env.AZURE_CLIENT_SECRET] = saved;
    if (saved[0] === undefined) delete process.env.AZURE_TENANT_ID;
    if (saved[1] === undefined) delete process.env.AZURE_CLIENT_ID;
    if (saved[2] === undefined) delete process.env.AZURE_CLIENT_SECRET;
  }

  // ── The two copies must not drift ──────────────────────────────────────
  // The Netlify function and the Azure Function App deploy separately, so the
  // file cannot be shared. Everything below the header comment must match.
  {
    const strip = f => fs.readFileSync(f, 'utf8').split('\n').slice(37).join('\n');
    check('the Netlify and Azure copies are identical below the header',
      strip(NETLIFY_COPY) === strip(AZURE_COPY));
    check('both copies exist', fs.existsSync(NETLIFY_COPY) && fs.existsSync(AZURE_COPY));
  }

  // ── Wiring ─────────────────────────────────────────────────────────────
  // The scheduled run is the path the report was about; assert it goes
  // through the new code rather than trusting the edit.
  {
    const runMig = fs.readFileSync(
      path.join(__dirname, '..', 'azure-function', 'src', 'run-migration.js'), 'utf8');
    check('the scheduled runner imports the Entra helper',
      /require\('\.\/sql-entra'\)/.test(runMig));
    // resolveSqlConfig, not applyEntraAuth: it also covers the credential-free
    // case, where the only possible meaning is the app's managed identity.
    check('openPool resolves its auth before connecting',
      /resolveSqlConfig\(parseMssqlUrl\(connStr\), connStr\)/.test(runMig));

    const dbc = fs.readFileSync(
      path.join(__dirname, '..', 'netlify', 'functions', 'db-connect.js'), 'utf8');
    check('Connect & test applies it too, so the string can be verified in the UI',
      /require\('\.\/lib\/sql-entra'\)/.test(dbc) && /applyEntraAuth\(config, connectionString\)/.test(dbc));
  }

  // @azure/identity has to be declared, not merely present transitively, or
  // Netlify's bundler will not ship it.
  {
    const root = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'));
    const fns  = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'netlify', 'functions', 'package.json'), 'utf8'));
    const az   = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'azure-function', 'package.json'), 'utf8'));
    check('@azure/identity is declared for the Netlify functions',
      !!root.dependencies['@azure/identity'] && !!fns.dependencies['@azure/identity']);
    check('@azure/identity is declared for the Azure Function App',
      !!az.dependencies['@azure/identity']);
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})();
