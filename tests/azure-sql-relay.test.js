// Tests the Azure SQL relay and the managed-identity config resolver.
//
// The Cygenix database sits on a Microsoft Entra-only Azure SQL server, so
// there is no username and password. The Azure Function App can reach it via
// its system-assigned managed identity; Netlify cannot, and never will.
//
// Two pieces are under test:
//   * resolveSqlConfig — how code INSIDE the Function App decides to connect.
//   * the relay        — how db-connect, on Netlify, hands the query to the
//                        Function App instead of connecting itself.
//
// The rule both must respect: a connection string that carries credentials is
// a different server and must be left completely alone.
const fs = require('fs');
const path = require('path');
const Module = require('module');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

// @azure/identity is never reached by these paths, but sql-entra requires it
// lazily and a real managed-identity probe would hang.
const identityPath = require.resolve('@azure/identity');
require.cache[identityPath] = new Module(identityPath, null);
require.cache[identityPath].filename = identityPath;
require.cache[identityPath].loaded = true;
require.cache[identityPath].exports = {
  DefaultAzureCredential: class { async getToken() { return { token: 'TOK' }; } },
  ClientSecretCredential: class { async getToken() { return { token: 'TOK' }; } },
};

const E = require(path.join(__dirname, '..', 'netlify', 'functions', 'lib', 'sql-entra.js'));
const R = require(path.join(__dirname, '..', 'netlify', 'functions', 'lib', 'azure-sql-relay.js'));

const withEnv = async (vars, fn) => {
  const saved = {};
  for (const k of Object.keys(vars)) { saved[k] = process.env[k]; process.env[k] = vars[k]; }
  try { return await fn(); }
  finally {
    for (const k of Object.keys(vars)) {
      if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k];
    }
  }
};

console.log('Azure SQL — managed identity and the Netlify relay\n');

(async () => {
  // ── resolveSqlConfig: inside the Function App ──────────────────────────
  await withEnv({ SQL_SERVER: 'cygenix.database.windows.net', SQL_DATABASE: 'cygenix' }, async () => {
    // A string with no credentials cannot be a SQL login, so it means the
    // app's own identity.
    const cfg = await E.resolveSqlConfig(
      { server: 'cygenix.database.windows.net', database: 'cygenix', port: 1433, user: '', password: '' },
      'mssql://cygenix.database.windows.net:1433/cygenix');
    check('a credential-free string resolves to managed identity',
      cfg.authentication && cfg.authentication.type === 'azure-active-directory-default',
      JSON.stringify(cfg.authentication));
    check('it keeps the server and database from the string',
      cfg.server === 'cygenix.database.windows.net' && cfg.database === 'cygenix');
    check('encryption is on', cfg.options.encrypt === true);
    check('no username or password is sent',
      cfg.user === undefined && cfg.password === undefined);

    // The migration runner allows hours per statement; the resolver must not
    // silently drop that back to the driver default.
    const timed = await E.resolveSqlConfig(
      { server: 's', database: 'd', requestTimeout: 14_400_000, connectionTimeout: 30_000 }, '');
    check('caller timeouts survive the swap',
      timed.requestTimeout === 14_400_000 && timed.connectionTimeout === 30_000);

    // No string at all → the env target.
    const envOnly = await E.resolveSqlConfig({}, '');
    check('an empty string falls back to SQL_SERVER / SQL_DATABASE',
      envOnly.server === 'cygenix.database.windows.net' && envOnly.database === 'cygenix');

    // A string WITH credentials is a different server. Untouched.
    const sqlAuth = { server: 'other.example.com', database: 'Legacy', user: 'sa', password: 'pw' };
    const out = await E.resolveSqlConfig(sqlAuth, 'mssql://sa:pw@other.example.com/Legacy');
    check('a credentialled string is returned as the same object', out === sqlAuth);
    check('and keeps its credentials', out.user === 'sa' && out.password === 'pw');
    check('and gains no authentication block', out.authentication === undefined);

    // An explicit Entra mode still routes through the token path.
    const explicit = await E.resolveSqlConfig({ server: 's', database: 'd' },
      'mssql://s/d?authentication=ActiveDirectoryDefault');
    check('an explicit ?authentication= mode is still honoured',
      explicit.authentication.type === 'azure-active-directory-access-token',
      JSON.stringify(explicit.authentication));
  });

  // Outside the Function App there is nothing to fall back to, and the error
  // must say so rather than producing a config that fails at login.
  await withEnv({ SQL_SERVER: '', SQL_DATABASE: '' }, async () => {
    let msg = '';
    try { await E.resolveSqlConfig({}, ''); } catch (e) { msg = e.message; }
    check('with no SQL_SERVER/SQL_DATABASE it explains rather than guessing',
      /SQL_SERVER and SQL_DATABASE/.test(msg), msg);
  });

  // ── When the relay engages ─────────────────────────────────────────────
  await withEnv({ AZURE_FUNCTION_KEY: '' }, async () => {
    check('the relay is off with no function key configured',
      R.relayConfigured() === false);
    check('and db-connect therefore connects directly',
      R.shouldRelay('mssql', { server: 's', database: 'd' }) === false);
  });

  await withEnv({ AZURE_FUNCTION_KEY: 'KEY' }, async () => {
    check('a credential-free mssql request relays',
      R.shouldRelay('mssql', { server: 's', database: 'd' }) === true);
    // The relay only ever reaches the one database the Function App serves.
    // Sending someone else's server there would answer with the wrong data.
    check('a request with a username does NOT relay',
      R.shouldRelay('mssql', { user: 'sa', database: 'd' }) === false);
    check('a request with a password does NOT relay',
      R.shouldRelay('mssql', { password: 'pw', database: 'd' }) === false);
    check('Postgres never relays — the route is SQL Server only',
      R.shouldRelay('postgres', { server: 's' }) === false);
  });

  // ── The pool facade ────────────────────────────────────────────────────
  // db-connect has fifteen query sites and a lot of response shaping. The
  // relay is shaped like a pool so none of that had to change.
  await withEnv({ AZURE_FUNCTION_KEY: 'KEY', AZURE_DB_API_URL: 'https://fn.example/api' }, async () => {
    const calls = [];
    const realFetch = global.fetch;
    global.fetch = async (url, opts) => {
      calls.push({ url, headers: opts.headers, body: JSON.parse(opts.body) });
      return {
        ok: true, status: 200,
        text: async () => JSON.stringify({
          success: true, rowsAffected: 3,
          recordset: [{ TABLE_NAME: 'Cases' }],
        }),
      };
    };
    try {
      const pool = R.createRelayPool({ server: 'cygenix.database.windows.net', database: 'cygenix' });

      const r = await pool.request().query('SELECT 1');
      check('query returns an mssql-shaped result',
        Array.isArray(r.recordset) && Array.isArray(r.rowsAffected), JSON.stringify(r));
      check('the recordset comes through', r.recordset[0].TABLE_NAME === 'Cases');
      check('rowsAffected is an array, as the driver returns', r.rowsAffected[0] === 3);
      check('it posts to the db/execute route',
        calls[0].url === 'https://fn.example/api/db/execute', calls[0].url);
      check('it sends the function key', calls[0].headers['x-functions-key'] === 'KEY');
      check('it names the database it expects, so a mismatch is an error',
        calls[0].body.expectServer === 'cygenix.database.windows.net' &&
        calls[0].body.expectDatabase === 'cygenix');

      // Parameters stay parameters. Inlining them would mean hand-rolling
      // escaping for values that came from the browser.
      calls.length = 0;
      await pool.request()
        .input('s', 'NVARCHAR', 'dbo')
        .input('t', 'NVARCHAR', 'Cases')
        .query('SELECT * FROM x WHERE a=@s AND b=@t');
      check('input() values are forwarded as parameters, not inlined',
        JSON.stringify(calls[0].body.params) ===
        JSON.stringify([{ name: 's', value: 'dbo' }, { name: 't', value: 'Cases' }]),
        JSON.stringify(calls[0].body.params));
      check('the SQL still carries its placeholders',
        /@s/.test(calls[0].body.sql) && /@t/.test(calls[0].body.sql));
      // mssql also allows input(name, value) with no type.
      calls.length = 0;
      await pool.request().input('n', 42).query('SELECT @n');
      check('the two-argument input() form works too',
        calls[0].body.params[0].value === 42, JSON.stringify(calls[0].body.params));

      calls.length = 0;
      await pool.request().batch('GO batch');
      check('batch posts to the db/batch route',
        calls[0].url === 'https://fn.example/api/db/batch');

      // db-connect closes the pool in a finally block.
      await pool.close();
      check('close() is a safe no-op', true);
    } finally { global.fetch = realFetch; }
  });

  // ── Failures are reported, not swallowed ───────────────────────────────
  await withEnv({ AZURE_FUNCTION_KEY: 'KEY', AZURE_DB_API_URL: 'https://fn.example/api' }, async () => {
    const realFetch = global.fetch;

    // A SQL error must arrive with the driver's fields, so db-connect's
    // existing hint logic sees what it would from a local connection.
    global.fetch = async () => ({
      ok: false, status: 500,
      text: async () => JSON.stringify({ success: false, error: 'Invalid object name', sqlNumber: 208 }),
    });
    let e = null;
    try { await R.createRelayPool().request().query('SELECT * FROM nope'); } catch (x) { e = x; }
    check('a SQL error surfaces its message', e && /Invalid object name/.test(e.message), e && e.message);
    check('and its SQL Server error number', e && e.number === 208);

    // A wrong key gets an HTML page from the Functions host, not JSON.
    global.fetch = async () => ({ ok: false, status: 401, text: async () => '<html>401</html>' });
    e = null;
    try { await R.createRelayPool().request().query('SELECT 1'); } catch (x) { e = x; }
    check('a 401 points at the key rather than reporting a JSON parse failure',
      e && /AZURE_FUNCTION_KEY/.test(e.message), e && e.message);

    // The database guard on the Azure side comes back as a 409.
    global.fetch = async () => ({
      ok: false, status: 409,
      text: async () => JSON.stringify({ success: false, error: 'This endpoint serves cygenix on x, not other on y.' }),
    });
    e = null;
    try { await R.createRelayPool().request().query('SELECT 1'); } catch (x) { e = x; }
    check('a wrong-database response is an error, never silent wrong data',
      e && /This endpoint serves/.test(e.message), e && e.message);

    global.fetch = async () => { throw new Error('getaddrinfo ENOTFOUND'); };
    e = null;
    try { await R.createRelayPool().request().query('SELECT 1'); } catch (x) { e = x; }
    check('an unreachable Function App says so',
      e && /Could not reach the Cygenix database API/.test(e.message), e && e.message);

    global.fetch = realFetch;
  });

  // ── Wiring ─────────────────────────────────────────────────────────────
  {
    const dbc = fs.readFileSync(
      path.join(__dirname, '..', 'netlify', 'functions', 'db-connect.js'), 'utf8');
    check('db-connect chooses the transport before authenticating',
      dbc.indexOf('const viaRelay = shouldRelay') < dbc.indexOf('if (!viaRelay) config = await applyEntraAuth'));
    check('db-connect uses the relay pool when relaying',
      /if \(viaRelay\) \{[\s\S]{0,400}createRelayPool/.test(dbc));
    check('the direct pool is still there for every other server',
      /new mssql\.ConnectionPool\(config\)\.connect\(\)/.test(dbc));

    const idx = fs.readFileSync(
      path.join(__dirname, '..', 'azure-function', 'src', 'index.js'), 'utf8');
    check('the Azure db route uses azure-active-directory-default',
      /authentication: \{ type: 'azure-active-directory-default' \}/.test(idx));
    check('it no longer pins a pre-fetched token into the pool config',
      !/options: \{ token: tokenResp\.token \}/.test(idx));
    check('it forwards relayed parameters', /Array\.isArray\(body\.params\)/.test(idx));
    check('it refuses to serve a database the caller did not ask for',
      /expectDatabase/.test(idx) && /status: 409/.test(idx));

    const rm = fs.readFileSync(
      path.join(__dirname, '..', 'azure-function', 'src', 'run-migration.js'), 'utf8');
    check('the scheduled runner resolves its config the same way',
      /resolveSqlConfig\(parseMssqlUrl\(connStr\), connStr\)/.test(rm));
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})();
