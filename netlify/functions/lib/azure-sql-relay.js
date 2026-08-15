// azure-sql-relay.js — run SQL through the Azure Function App instead of
// connecting from Netlify.
//
// ── Why ───────────────────────────────────────────────────────────────────
// The Cygenix database is on a Microsoft Entra-only Azure SQL server, so there
// is no username and password to connect with. The Azure Function App can
// reach it because it has a system-assigned managed identity that has been
// granted a database user. Netlify has no managed identity and never will, so
// db-connect cannot authenticate on its own — the only way for Connect & test,
// schema browsing and the SQL editor to work against that database is to ask
// the Function App to run the query.
//
// ── How ───────────────────────────────────────────────────────────────────
// This exports an object shaped like enough of an mssql ConnectionPool that
// db-connect's existing code does not know the difference:
//
//     pool.request().query(sql)                  → POST db/execute
//     pool.request().input(n, type, v).query(sql) → POST db/execute + params
//     pool.request().batch(sql)                   → POST db/batch
//     pool.close()                                → no-op
//
// Doing it at the pool level rather than at each call site means all fifteen
// query sites, and every response-shaping function around them, are untouched.
// The transport changes; nothing else does.
//
// Parameters are forwarded as parameters. Inlining them into the SQL would
// mean hand-rolling string escaping for values that came from the browser,
// which is the thing bound parameters exist to avoid.
//
// ── Configuration ─────────────────────────────────────────────────────────
//   AZURE_DB_API_URL   base URL of the Function App's /api (optional; the
//                      known deployment is the default)
//   AZURE_FUNCTION_KEY the Function App's host key. Its `db` route is
//                      authLevel:'function', so without this the relay is off
//                      and db-connect falls back to connecting directly.

const DEFAULT_BASE = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api';

function relayBase() {
  return String(process.env.AZURE_DB_API_URL || DEFAULT_BASE).replace(/\/+$/, '');
}

function relayKey() {
  return String(process.env.AZURE_FUNCTION_KEY || '').trim();
}

// The relay can only serve the one database the Function App is wired to, so
// it is off unless a key is configured. Callers fall back to a direct
// connection, which is the right behaviour for every other server.
function relayConfigured() {
  return !!relayKey();
}

// Should this request go through the relay?
//
// Only when the caller cannot connect on its own: an mssql connection string
// carrying no username and no password is not a SQL login, so the only thing
// it can mean is "connect as the application". Anything with credentials is
// left alone — that is a different server and the relay would answer with the
// wrong database's data.
function shouldRelay(dialect, config) {
  if (dialect !== 'mssql') return false;      // the route is SQL Server only
  if (!relayConfigured()) return false;
  if (config && (config.user || config.password)) return false;
  return true;
}

async function post(action, payload, timeoutMs) {
  const ctl = new AbortController();
  const to  = setTimeout(() => ctl.abort(), timeoutMs || 120_000);
  let res;
  try {
    res = await fetch(relayBase() + '/db/' + action, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-functions-key': relayKey(),
      },
      body: JSON.stringify({ action, ...payload }),
      signal: ctl.signal,
    });
  } catch (e) {
    if (e && e.name === 'AbortError') {
      throw new Error('The Cygenix database API did not respond in time.');
    }
    throw new Error('Could not reach the Cygenix database API: ' + (e && e.message || e));
  } finally {
    clearTimeout(to);
  }

  const text = await res.text();
  let data;
  try { data = JSON.parse(text); }
  catch {
    // A non-JSON body is almost always the Functions host itself answering —
    // a 401 from a wrong key, or an HTML error page. Say that rather than
    // reporting a JSON parse failure.
    throw new Error('Cygenix database API returned HTTP ' + res.status +
      (res.status === 401 ? ' — check AZURE_FUNCTION_KEY.' : ': ' + text.slice(0, 200)));
  }
  if (!res.ok || data.success === false) {
    const e = new Error(data.error || ('Cygenix database API returned HTTP ' + res.status));
    // Carry the driver's SQL error fields through, so db-connect's existing
    // hint logic sees the same shape it would from a local connection.
    if (data.sqlNumber !== undefined) e.number = data.sqlNumber;
    if (data.sqlState  !== undefined) e.state  = data.sqlState;
    if (data.sqlLine   !== undefined) e.lineNumber = data.sqlLine;
    throw e;
  }
  return data;
}

// A stand-in for mssql's ConnectionPool, covering exactly the surface
// db-connect uses. `expect` names the database the caller believes it is
// talking to; the Azure route rejects the request if that is not the database
// it serves, so a mismatch is an error rather than silently wrong data.
function createRelayPool(expect) {
  const expectFields = {
    expectServer:   (expect && expect.server)   || '',
    expectDatabase: (expect && expect.database) || '',
  };

  const makeRequest = () => {
    const params = [];
    const api = {
      // mssql's signature is input(name, type, value) or input(name, value).
      // The type is dropped: the value crosses as JSON and the driver on the
      // far side infers, which is what it would have done for these anyway.
      input(name, a, b) {
        params.push({ name, value: (b === undefined ? a : b) });
        return api;
      },
      async query(sqlText) {
        const d = await post('execute', { sql: sqlText, params, ...expectFields });
        return {
          recordset:    d.recordset || [],
          recordsets:   [d.recordset || []],
          rowsAffected: [d.rowsAffected || 0],
        };
      },
      async batch(sqlText) {
        const d = await post('batch', { sql: sqlText, ...expectFields });
        return {
          recordset:    d.recordset || [],
          recordsets:   [d.recordset || []],
          rowsAffected: [d.rowsAffected || 0],
        };
      },
    };
    return api;
  };

  return {
    request: makeRequest,
    // Nothing is held open, so closing is a no-op — but db-connect calls it in
    // a finally block and would throw on undefined.
    async close() {},
    on() {},
    connected: true,
    _isRelay: true,
  };
}

module.exports = {
  relayConfigured,
  shouldRelay,
  createRelayPool,
  relayBase,
};
