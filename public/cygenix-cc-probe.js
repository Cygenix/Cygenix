/* ============================================================================
   cygenix-cc-probe.js — the Claude Code connection test, browser side
   ----------------------------------------------------------------------------
   Sep-2026. Before the Claude Code console is switched on, an Owner or
   Platform Administrator checks that Anthropic's cloud workspace can reach
   the database at all: the Managed Agents documentation does not say whether
   a workspace may open a raw TCP connection to port 1433 or 5432, nor from
   which address it arrives. azure-function/src/claude-code.js runs the test;
   this file prepares it and reads the answer.

   WHAT LEAVES THE BROWSER
   A host name, a port and a database type. The connection string is parsed
   HERE, for exactly those three, and goes nowhere — the test needs no login
   and is given none, so there is no password to protect on the way.

   No DOM, no storage, no network in the parser: the page does those, and
   Node tests the rest (tests/claude-code.test.js).
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixCcProbe) root.CygenixCcProbe = api;
})(typeof globalThis !== 'undefined' ? globalThis : (typeof window !== 'undefined' ? window : this), function () {
'use strict';

var DEFAULT_PORT = { sqlserver: 1433, postgres: 5432 };
var HOST_RE = /^(?=.{1,253}$)[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$/;

function trim(v) { return v == null ? '' : String(v).trim(); }

/* ── Where does this connection point? ────────────────────────────────────
   Every shape the product stores a connection string in:
     ADO / ODBC       Server=tcp:host,1433;Database=…    (also Data Source=, Address=)
     URL              mssql://u:p@host:1433/db   sqlserver://host:1433;…
                      postgres(ql)://u:p@host:5432/db
     libpq keywords   host=… port=… dbname=…
   Returns { ok, host, port, kind, note } — or { ok:false, why } in words. */
function endpointOf(connString, mode) {
  if (mode === 'azure') {
    return { ok: false, why: 'This connection goes through a Cygenix Function App, not straight to the database. Enter the database server name by hand.' };
  }
  var s = trim(connString);
  if (!s) return { ok: false, why: 'This connection has no connection string in this browser.' };

  var url = /^(mssql|sqlserver|postgres|postgresql):\/\//i.exec(s);
  if (url) {
    var kind = /^postgres/i.test(url[1]) ? 'postgres' : 'sqlserver';
    var rest = s.slice(url[0].length);
    // The authority ends at the first / ; or ?, and any credentials end at
    // the last @ before that.
    var end = rest.search(/[\/;?]/);
    if (end === -1) end = rest.length;
    var at = rest.lastIndexOf('@', end);
    var hostPart = rest.slice(at + 1, end);
    return fromHostPort(hostPart, kind, ':');
  }

  var ado = /(?:^|;)\s*(?:server|data source|address|addr|network address)\s*=\s*([^;]+)/i.exec(s);
  if (ado) {
    var v = trim(ado[1]).replace(/^tcp:/i, '');
    return fromHostPort(v, 'sqlserver', ',');
  }

  var pgHost = /(?:^|\s)host\s*=\s*'?([^\s']+)'?/i.exec(s);
  if (pgHost) {
    var pgPort = /(?:^|\s)port\s*=\s*'?(\d+)'?/i.exec(s);
    return finish(pgHost[1], pgPort ? Number(pgPort[1]) : null, 'postgres', null);
  }
  return { ok: false, why: 'Could not find a server name in this connection string. Enter it by hand.' };
}

function fromHostPort(v, kind, sep) {
  var host = trim(v), port = null, note = null;
  // SQL Server named instance: host\INSTANCE. Its port is chosen by the
  // SQL Browser service at runtime, which the test cannot ask; say so.
  var inst = host.indexOf('\\');
  if (inst !== -1) {
    note = 'Named instance "' + host.slice(inst + 1) + '": its port is not in the connection string. 1433 is assumed; change it if the instance listens elsewhere.';
    host = host.slice(0, inst);
  }
  var i = host.lastIndexOf(sep);
  if (i !== -1 && /^\d+$/.test(host.slice(i + 1))) { port = Number(host.slice(i + 1)); host = host.slice(0, i); }
  return finish(host, port, kind, note);
}

function finish(host, port, kind, note) {
  host = trim(host).replace(/^\[|\]$/g, '').toLowerCase();
  if (host === '.' || host === '(local)' || host === 'localhost' || /^127\./.test(host)) {
    return { ok: false, why: 'This connection points at this computer (' + host + '). Anthropic\'s workspace cannot reach it.' };
  }
  if (!HOST_RE.test(host)) return { ok: false, why: 'The server name "' + host + '" is not one the test can use. Enter it by hand.' };
  return { ok: true, host: host, port: port || DEFAULT_PORT[kind] || 1433, kind: kind, note: note };
}

/* Checks the page applies before it sends anything — the same rules the
   server applies, so the person sees the sentence without a round trip. */
function validate(host, port) {
  var h = trim(host).toLowerCase();
  var p = Number(port);
  if (!HOST_RE.test(h)) return 'Enter a host name or IPv4 address, without a scheme, port or path.';
  if (!(p >= 1 && p <= 65535 && Math.floor(p) === p)) return 'Enter a port between 1 and 65535.';
  return '';
}

/* ── Reading the answer ────────────────────────────────────────────────── */
function rows(result) {
  var r = result || {};
  var out = [];
  if (r.dns) out.push(['Name resolves to', r.dns.join(', ')]);
  if (r.dns_error) out.push(['Name lookup', r.dns_error]);
  if (r.tcp) out.push(['Connection', r.tcp === 'open' ? 'Opened in ' + r.tcp_ms + ' ms' : 'Failed after ' + r.tcp_ms + ' ms — ' + (r.tcp_error || '')]);
  if (r.handshake) out.push(['Database reply', r.handshake === 'sqlserver-replied' ? 'SQL Server answered'
    : r.handshake === 'postgres-replied' ? 'PostgreSQL answered' : r.handshake]);
  if (r.egress_ip) out.push(['Workspace IP address', r.egress_ip]);
  if (r.egress_ip_error) out.push(['Workspace IP address', 'Unknown — ' + r.egress_ip_error]);
  return out;
}

/* The firewall sentence the brief asked for, with the address when the test
   learned it — a person who has to ask for a firewall rule needs the IP. */
function firewallHelp(result) {
  var r = result || {};
  var s = 'Your database may only accept known IP addresses — the Anthropic workspace may need allowing.';
  if (r.egress_ip) s += ' This test arrived from ' + r.egress_ip + '. Anthropic does not publish its workspace addresses, so run the test more than once before relying on this one.';
  return s;
}

function costText(cents) {
  if (cents == null || isNaN(cents)) return '';
  return 'US$' + (Number(cents) / 100).toFixed(2);
}

return {
  DEFAULT_PORT: DEFAULT_PORT,
  endpointOf: endpointOf,
  validate: validate,
  rows: rows,
  firewallHelp: firewallHelp,
  costText: costText,
};
});
