/* ============================================================================
   cygenix-conn-builder.js — compose a connection string from its parts, and
   take one back apart.
   ----------------------------------------------------------------------------
   Connections were paste-only. That is fine when somebody has a string in
   front of them and wrong the rest of the time: most people are reading
   credentials off a page — host here, port there, database, user, password —
   and have to assemble the syntax themselves, differently for each engine,
   getting the punctuation right first time with no feedback until the connect
   fails. Every database tool worth using offers both.

   WHAT THIS IS NOT
   It is not a second way to store a connection. There is exactly one field of
   record — the connection string input the page already had — and the builder
   writes into it. Everything downstream (test, save, jobs, schedules, the
   agent) reads that same string and cannot tell how it was produced. A form
   that fed a parallel representation would be a second source of truth about
   the one thing in this product that must not be ambiguous.

   BOTH DIRECTIONS, ON PURPOSE
   parse() is what makes the toggle honest. Switching to the form with a string
   already pasted decomposes it into the fields, so the form is a view of the
   string rather than a rival to it, and somebody who inherited a string can
   see what is in it — which is also the fastest way to spot the typo.

   Node-requirable, and the tests round-trip compose() through the REAL server
   parser in netlify/functions/db-connect.js. A builder that produces a string
   this product cannot read would be worse than no builder.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object') root.CygenixConnBuilder = api;
})(typeof window !== 'undefined' ? window : this, function () {
'use strict';

var ENGINES = [
  { id: 'mssql',    label: 'SQL Server / Azure SQL', port: 1433 },
  { id: 'postgres', label: 'PostgreSQL',             port: 5432 },
];

function defaultPort(engine) {
  for (var i = 0; i < ENGINES.length; i++) if (ENGINES[i].id === engine) return ENGINES[i].port;
  return 1433;
}

function s(v) { return v == null ? '' : String(v).trim(); }

/* SQL Server's own escape. A value containing ; or = or leading/trailing space
   ends the token early and the driver reports something unrelated to the real
   problem — a password with a semicolon in it is the classic version of this,
   and it looks like a wrong password rather than a quoting bug. */
function mssqlValue(v) {
  var val = String(v == null ? '' : v);
  if (!/[;=}]|^\s|\s$|^\{/.test(val)) return val;
  return '{' + val.replace(/\}/g, '}}') + '}';
}
function unMssqlValue(v) {
  var val = String(v == null ? '' : v).trim();
  if (val.length >= 2 && val.charAt(0) === '{' && val.charAt(val.length - 1) === '}') {
    return val.slice(1, -1).replace(/\}\}/g, '}');
  }
  return val;
}

/* ── compose ─────────────────────────────────────────────────────────────── */

/**
 * Build a connection string from fields. Returns '' when there is not yet
 * enough to build one, rather than a half-formed string that would fail with a
 * confusing error if somebody pressed Test.
 */
/* Is this string something a database layer could dial at all? The form
   writes into the one field of record, and Save as… persists whatever is in
   it, so a value that is not a connection — a label, a nickname, half a
   hostname — must never get that far. Deliberately generous: it asks whether
   the string has the SHAPE of a connection, not whether the credentials in
   it are right, which only the server can answer.

   The mssql:// branch carries its weight. A great many saved connections on
   this product are driver URLs with no semicolons and no `server=` in them,
   so a check written only for key=value strings would reject the very
   connections that work. */
function looksLikeConnection(v) {
  var raw = s(v);
  if (!raw) return false;
  if (/^(https?|mssql|sqlserver|postgres|postgresql):\/\//i.test(raw)) return true;
  if (/(^|;|\s)(server|data source|host|addr|address)\s*=/i.test(raw)) return true;
  return false;
}

function compose(f) {
  f = f || {};
  var engine = f.engine === 'postgres' ? 'postgres' : 'mssql';
  var host = s(f.host), database = s(f.database), user = s(f.user);
  var password = f.password == null ? '' : String(f.password);
  var port = s(f.port);

  if (!host || !database) return '';

  if (engine === 'postgres') {
    // URL form: the shape the server's parser handles best and the one every
    // Postgres host quotes in its own docs.
    var auth = '';
    if (user) {
      auth = encodeURIComponent(user);
      if (password) auth += ':' + encodeURIComponent(password);
      auth += '@';
    }
    var p = port && String(parseInt(port, 10)) === port ? port : String(defaultPort('postgres'));
    var url = 'postgres://' + auth + host + ':' + p + '/' + encodeURIComponent(database);

    var q = [];
    if (f.sslmode && f.sslmode !== 'auto') q.push('sslmode=' + encodeURIComponent(f.sslmode));
    // search_path, so unqualified table names in the SQL Editor resolve where
    // the user expects. Introspection lists every schema regardless — this
    // changes what `SELECT * FROM thing` means, not what Cygenix can see.
    if (s(f.schema)) q.push('options=' + encodeURIComponent('-c search_path=' + s(f.schema)));
    return url + (q.length ? '?' + q.join('&') : '');
  }

  /* SQL Server has two spellings and this product uses both. A string that
     arrived as a driver URL goes back out as one: rewriting somebody's
     mssql:// connection into keyword form the first time they touch a field
     is a change they did not ask for, in the one value that must not be
     ambiguous. The server reads either (parseMssqlConnectionString), so the
     choice is purely about leaving what is stored recognisable. */
  if (f.form === 'url') {
    var msAuth = '';
    if (user) {
      msAuth = encodeURIComponent(user);
      if (password) msAuth += ':' + encodeURIComponent(password);
      msAuth += '@';
    }
    var msPort = port && String(parseInt(port, 10)) === port ? port : String(defaultPort('mssql'));
    var msUrl = 'mssql://' + msAuth + host + ':' + msPort + '/' + encodeURIComponent(database);
    var msQ = [];
    // Only the non-defaults are written. The server reads a missing encrypt
    // as true, and treats an unencrypted connection as trusting the cert, so
    // spelling those out would add noise that changes nothing.
    if (f.encrypt === false) msQ.push('encrypt=false');
    else if (f.trustCert) msQ.push('trustServerCertificate=true');
    return msUrl + (msQ.length ? '?' + msQ.join('&') : '');
  }

  var parts = [];
  parts.push('Server=' + mssqlValue(host + (port && port !== '1433' ? ',' + port : '')));
  parts.push('Database=' + mssqlValue(database));
  if (user) parts.push('User Id=' + mssqlValue(user));
  if (password) parts.push('Password=' + mssqlValue(password));
  if (f.encrypt !== false) parts.push('Encrypt=true');
  if (f.trustCert) parts.push('TrustServerCertificate=true');
  return parts.join(';');
}

/* ── parse ───────────────────────────────────────────────────────────────── */

/** Take a connection string apart into the same fields compose() takes. */
/* Split a driver URL into credentials, host, port and database. Shared by the
   two URL branches below because they differ only in their defaults, and
   written to match netlify/functions/db-connect.js — credentials before the
   LAST '@' (a password may contain one), the port after the LAST ':' unless
   the host is bracketed IPv6, everything after the first '/' the database. */
function splitUrl(rest, defPort) {
  var out = { user: '', password: '', host: '', port: '', database: '', query: '' };
  var q = rest.indexOf('?');
  var base = q >= 0 ? rest.slice(0, q) : rest;
  out.query = q >= 0 ? rest.slice(q + 1) : '';
  var at = base.lastIndexOf('@');
  if (at >= 0) {
    var creds = base.slice(0, at);
    var c = creds.indexOf(':');
    out.user = decodeURIComponent(c >= 0 ? creds.slice(0, c) : creds);
    out.password = c >= 0 ? decodeURIComponent(creds.slice(c + 1)) : '';
  }
  var hostPart = at >= 0 ? base.slice(at + 1) : base;
  var slash = hostPart.indexOf('/');
  var hostPort = slash >= 0 ? hostPart.slice(0, slash) : hostPart;
  out.database = slash >= 0 ? decodeURIComponent(hostPart.slice(slash + 1)) : '';
  var lastColon = hostPort.lastIndexOf(':');
  if (lastColon >= 0 && hostPort.indexOf('[') === -1) {
    out.host = hostPort.slice(0, lastColon);
    out.port = hostPort.slice(lastColon + 1);
  } else {
    out.host = hostPort;
    out.port = String(defPort);
  }
  return out;
}
function eachQueryPair(query, fn) {
  String(query || '').split('&').forEach(function (pair) {
    if (!pair) return;
    var eq = pair.indexOf('=');
    var k = decodeURIComponent(eq >= 0 ? pair.slice(0, eq) : pair).toLowerCase();
    var v = eq >= 0 ? decodeURIComponent(pair.slice(eq + 1)) : '';
    fn(k, v);
  });
}

function parse(cs) {
  var raw = s(cs);
  /* `form` records which of SQL Server's two spellings this string was
     written in, so compose() can hand back the same one. Without it, the
     form was a one-way door: open it on a driver URL and the next keystroke
     replaced the URL with a keyword string. */
  var out = { engine: 'mssql', form: 'kv', host: '', port: '', database: '', user: '', password: '',
              schema: '', sslmode: 'auto', encrypt: true, trustCert: false };
  if (!raw) return out;

  /* SQL Server's URL spelling. db-connect.js has accepted it since the
     beginning and its own error message advertises it, but this parser did
     not know it: every field came back empty, the form showed a blank
     connection, and one keystroke composed that blank back over the string.
     A working connection was replaced by nothing at all, silently, and the
     next Save persisted it. */
  if (/^(mssql|sqlserver):\/\//i.test(raw)) {
    out.form = 'url';
    var u = splitUrl(raw.replace(/^(mssql|sqlserver):\/\//i, ''), defaultPort('mssql'));
    out.user = u.user; out.password = u.password;
    out.host = u.host; out.port = u.port; out.database = u.database;
    eachQueryPair(u.query, function (k, v) {
      if (k === 'encrypt') out.encrypt = !/^false$/i.test(v);
      else if (k === 'trustservercertificate') out.trustCert = /^true$/i.test(v);
      else if ((k === 'database' || k === 'initial catalog') && !out.database) out.database = v;
    });
    // The server treats an unencrypted connection as one that cannot be
    // checking a certificate either. The form says the same thing rather
    // than showing a box the connection does not honour.
    if (!out.encrypt) out.trustCert = true;
    if (!/^\d+$/.test(out.port)) out.port = String(defaultPort('mssql'));
    return out;
  }

  if (/^(postgres|postgresql):\/\//i.test(raw)) {
    out.engine = 'postgres';
    out.form = 'url';
    out.sslmode = 'auto';
    var pg = splitUrl(raw.replace(/^(postgres|postgresql):\/\//i, ''), defaultPort('postgres'));
    out.user = pg.user; out.password = pg.password;
    out.host = pg.host; out.port = pg.port; out.database = pg.database;
    eachQueryPair(pg.query, function (k, v) {
      if (k === 'sslmode') out.sslmode = v.toLowerCase();
      if (k === 'options') {
        var m = /search_path\s*=\s*([^\s,]+)/i.exec(v);
        if (m) out.schema = m[1];
      }
    });
    return out;
  }

  // SQL Server keyword form. Values may be brace-quoted, and a brace-quoted
  // value may legally contain a semicolon — so split on the delimiters rather
  // than on every ';'.
  var re = /([^=;]+?)\s*=\s*(\{(?:[^}]|\}\})*\}|[^;]*)/g;
  var m2;
  while ((m2 = re.exec(raw)) !== null) {
    var key = m2[1].trim().toLowerCase().replace(/\s+/g, ' ');
    var val = unMssqlValue(m2[2]);
    if (key === 'server' || key === 'data source' || key === 'addr' || key === 'address') {
      var comma = val.lastIndexOf(',');
      if (comma > 0 && /^\d+$/.test(val.slice(comma + 1))) {
        out.host = val.slice(0, comma).trim();
        out.port = val.slice(comma + 1).trim();
      } else { out.host = val; }
    }
    else if (key === 'database' || key === 'initial catalog') out.database = val;
    else if (key === 'user id' || key === 'uid' || key === 'user') out.user = val;
    else if (key === 'password' || key === 'pwd') out.password = val;
    else if (key === 'encrypt') out.encrypt = !/^(false|no|0)$/i.test(val);
    else if (key === 'trustservercertificate') out.trustCert = !/^(false|no|0)$/i.test(val);
  }
  if (!out.port) out.port = String(defaultPort('mssql'));
  return out;
}

/* ── mask ────────────────────────────────────────────────────────────────── */

/**
 * The preview. A password on screen is a password over somebody's shoulder and
 * in every screenshot of a support ticket, and the preview exists to show the
 * SHAPE of the string — that the punctuation is right — which it does just as
 * well without.
 */
function mask(cs) {
  var raw = s(cs);
  if (!raw) return '';
  /* Every driver URL this product uses, not only the postgres one. The first
     version named postgres alone, so an mssql:// connection — the commonest
     shape here — had its password printed in full in the preview, which is
     the one place the comment above promises it never appears. The scheme is
     matched generically because the next one added would have had the same
     hole. */
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(raw)) {
    return raw.replace(/^([a-z][a-z0-9+.-]*:\/\/[^:@/]+):[^@/]*@/i, '$1:••••••@');
  }
  return raw.replace(/(\b(?:password|pwd)\s*=\s*)(\{(?:[^}]|\}\})*\}|[^;]*)/gi, '$1••••••');
}

/** Enough to try a connection with? Used to enable Test rather than to gate. */
function isComplete(f) {
  return !!(f && s(f.host) && s(f.database));
}

return {
  ENGINES: ENGINES,
  compose: compose,
  parse: parse,
  mask: mask,
  isComplete: isComplete,
  looksLikeConnection: looksLikeConnection,
  defaultPort: defaultPort,
  __core: { mssqlValue: mssqlValue, unMssqlValue: unMssqlValue },
};
});
