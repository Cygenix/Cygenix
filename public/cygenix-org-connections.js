/* ============================================================================
   cygenix-org-connections.js — the organisation's connection register, from
   the browser's side.
   ----------------------------------------------------------------------------
   Phase A of docs/design/server-held-connections.md. A connection is set up
   ONCE for the organisation and bound to profiles; any member with rights to
   a profile uses it. This module is the browser's view of that register:

     CygenixOrgConnections.list({ force })   → Promise<{ ok, connections }>
     CygenixOrgConnections.create(record)    → Promise<{ ok, connection | error }>
     CygenixOrgConnections.update(id, patch) → same
     CygenixOrgConnections.retire(id)        → same
     CygenixOrgConnections.cached()          → the last list, synchronously
     CygenixOrgConnections.status()          → { state, message, at }

   It talks to /.netlify/functions/org-connections, which decides permission
   where the roles live and writes where the data lives. It never holds, sends
   or receives a secret: the register is metadata, and the Function App
   refuses a body carrying a password, string or key.

   Guards, because this posts: one write in flight at a time and three seconds
   between writes; the list is cached for thirty seconds and shares one
   request among concurrent callers. The status is written by each request's
   outcome only and gates nothing, so a callback cannot reset a flag that
   decides whether the next call runs.

   Pages listen for 'cygenix:org-connections-changed' and re-render. This
   module never listens to that event, so it cannot call itself.
   ========================================================================== */
(function (root) {
  'use strict';

  var ENDPOINT = '/.netlify/functions/org-connections';
  var LIST_TTL_MS = 30000;
  var MIN_WRITE_INTERVAL_MS = 3000;

  var _list = null;            // last successful list
  var _listAt = 0;
  var _listInflight = null;
  var _writeInflight = null;
  var _lastWriteAt = 0;
  var _status = { state: 'unknown', message: '', at: 0, code: '' };

  function token() {
    try { return (typeof root.getCygenixIdToken === 'function') ? root.getCygenixIdToken() : ''; }
    catch (e) { return ''; }
  }
  function note(state, message, code) {
    _status = { state: state, message: message || '', code: code || '', at: Date.now() };
  }
  function status() { return Object.assign({}, _status); }
  function cached() { return _list ? _list.slice() : []; }
  function announce(detail) {
    try { root.dispatchEvent(new CustomEvent('cygenix:org-connections-changed', { detail: detail || {} })); }
    catch (e) { /* no event API */ }
  }

  // One call, classified. Never throws.
  function call(method, body, qs) {
    var t = token();
    if (!t) { note('signed-out', '', 'no-token'); return Promise.resolve({ ok: false, code: 'no-token', message: 'Sign in first.' }); }
    var init = { method: method, headers: { Authorization: 'Bearer ' + t } };
    if (body) { init.headers['Content-Type'] = 'application/json'; init.body = JSON.stringify(body); }
    return fetch(ENDPOINT + (qs ? '?' + qs : ''), init).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (d) {
        if (r.ok) return { ok: true, data: d };
        var msg = d.error || ('HTTP ' + r.status);
        var code = d.code || (r.status === 403 ? 'denied' : r.status === 409 ? 'locked' : r.status === 503 ? 'config' : 'http');
        return { ok: false, status: r.status, code: code, message: msg };
      });
    }).catch(function (e) {
      return { ok: false, status: 0, code: 'network', message: 'Could not reach the server: ' + (e && e.message || e) };
    });
  }

  function list(opts) {
    var o = opts || {};
    if (_listInflight) return _listInflight;
    if (!o.force && _list && Date.now() - _listAt < LIST_TTL_MS) {
      return Promise.resolve({ ok: true, connections: _list.slice(), cached: true });
    }
    _listInflight = call('GET', null, o.includeRetired ? 'includeRetired=1' : '').then(function (r) {
      if (!r.ok) {
        if (r.code !== 'no-token') note(r.code === 'denied' ? 'denied' : 'error', r.message, r.code);
        return { ok: false, code: r.code, message: r.message, connections: cached() };
      }
      var next = Array.isArray(r.data && r.data.connections) ? r.data.connections : [];
      var changed = JSON.stringify(next) !== JSON.stringify(_list || []);
      _list = next; _listAt = Date.now();
      note('ok', '', '');
      if (changed) announce({ source: 'list', count: next.length });
      return { ok: true, connections: next.slice() };
    }).then(function (r) { _listInflight = null; return r; },
            function (e) { _listInflight = null; throw e; });
    return _listInflight;
  }

  // Writes share one in-flight slot and a minimum interval. A second write
  // inside the window is refused with 'throttled' rather than queued: the
  // page shows a form, and a person pressing Save twice wants one record.
  function write(body) {
    if (_writeInflight) return Promise.resolve({ ok: false, code: 'busy', message: 'Another change is still being saved.' });
    var wait = MIN_WRITE_INTERVAL_MS - (Date.now() - _lastWriteAt);
    if (wait > 0) return Promise.resolve({ ok: false, code: 'throttled', message: 'Wait a moment before saving again.' });
    _lastWriteAt = Date.now();
    _writeInflight = call('POST', body).then(function (r) {
      if (r.ok) {
        var rec = r.data && r.data.connection;
        note('ok', '', '');
        // Fold the answer into the cache so the page can redraw without a
        // second round trip; the next forced list is the truth regardless.
        if (rec && _list) {
          var i = _list.findIndex(function (c) { return c.id === rec.id; });
          if (i === -1) _list.push(rec); else _list[i] = rec;
          if (rec.retiredAt) _list = _list.filter(function (c) { return !c.retiredAt; });
        }
        announce({ source: body.op, id: rec && rec.id });
        return { ok: true, connection: rec, data: r.data };
      }
      if (r.code !== 'no-token') note(r.code === 'denied' ? 'denied' : 'error', r.message, r.code);
      return { ok: false, code: r.code, status: r.status, message: r.message };
    }).then(function (r) { _writeInflight = null; return r; },
            function (e) { _writeInflight = null; throw e; });
    return _writeInflight;
  }

  function create(record) { return write({ op: 'create', connection: record || {} }); }
  function update(id, patch) { return write({ op: 'update', id: id, patch: patch || {} }); }
  function retire(id) { return write({ op: 'retire', id: id }); }

  // Labels a page can share, so two screens never spell a kind differently.
  var KIND_LABELS = { sqlserver: 'SQL Server', postgres: 'PostgreSQL', azurefn: 'Azure Function' };
  var AUTH_LABELS = { sql: 'SQL login', entra: 'Entra ID', key: 'Function key' };
  function endpointOf(c) {
    if (!c) return '';
    if (c.kind === 'azurefn') return c.endpoint || '';
    return (c.server || '') + (c.port && c.port !== (c.kind === 'postgres' ? 5432 : 1433) ? ':' + c.port : '') + (c.database ? '/' + c.database : '');
  }

  root.CygenixOrgConnections = {
    list: list, create: create, update: update, retire: retire,
    cached: cached, status: status,
    KIND_LABELS: KIND_LABELS, AUTH_LABELS: AUTH_LABELS, endpointOf: endpointOf,
    // For the tests — not for pages.
    _reset: function () { _list = null; _listAt = 0; _listInflight = null; _writeInflight = null; _lastWriteAt = 0; note('unknown', '', ''); },
  };
})(typeof window !== 'undefined' ? window : this);
