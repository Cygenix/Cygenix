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
function parse(cs) {
  var raw = s(cs);
  var out = { engine: 'mssql', host: '', port: '', database: '', user: '', password: '',
              schema: '', sslmode: 'auto', encrypt: true, trustCert: false };
  if (!raw) return out;

  if (/^(postgres|postgresql):\/\//i.test(raw)) {
    out.engine = 'postgres';
    out.sslmode = 'auto';
    var rest = raw.replace(/^(postgres|postgresql):\/\//i, '');
    var qIdx = rest.indexOf('?');
    var query = qIdx >= 0 ? rest.slice(qIdx + 1) : '';
    var base = qIdx >= 0 ? rest.slice(0, qIdx) : rest;

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
      out.port = String(defaultPort('postgres'));
    }
    query.split('&').forEach(function (pair) {
      if (!pair) return;
      var eq = pair.indexOf('=');
      var k = decodeURIComponent(eq >= 0 ? pair.slice(0, eq) : pair).toLowerCase();
      var v = eq >= 0 ? decodeURIComponent(pair.slice(eq + 1)) : '';
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
  if (/^(postgres|postgresql):\/\//i.test(raw)) {
    return raw.replace(/^((?:postgres|postgresql):\/\/[^:@/]+):[^@]*@/i, '$1:••••••@');
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
  defaultPort: defaultPort,
  __core: { mssqlValue: mssqlValue, unMssqlValue: unMssqlValue },
};
});
