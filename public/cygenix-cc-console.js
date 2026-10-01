/* ============================================================================
   cygenix-cc-console.js — the Claude Code console's reading of a session
   ----------------------------------------------------------------------------
   Sep-2026. The server (azure-function/src/claude-code.js) relays Anthropic
   Managed Agents events, redacted, as they are. This module turns them into
   what the page shows: the chat on the left, the tool activity on the right,
   a status word, and the two warnings worth raising — the spend cap, and a
   database that would not let the workspace in.

   No DOM, no storage, no network. The page renders what this returns, and
   Node tests it (tests/claude-code.test.js).

   THE TWO COLUMNS
   Chat is what was SAID: the person's messages, Claude's replies, and the
   mode notices (a system message is Cygenix telling Claude the rules
   changed, and the person should see that it did). Output is what was DONE:
   each command run and each file written, with what came back — collapsed
   by default because a long session runs hundreds of them, and a table when
   the result reads as one, because a query's rows are the point of most of
   these sessions.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixCcConsole) root.CygenixCcConsole = api;
})(typeof globalThis !== 'undefined' ? globalThis : (typeof window !== 'undefined' ? window : this), function () {
'use strict';

var STATUS_WORDS = { idle: 'Idle', running: 'Working', stopped: 'Stopped', error: 'Error', none: 'No session' };
var TABLE_MAX_ROWS = 200;
var TABLE_MAX_COLS = 40;

function textOf(ev) {
  return (Array.isArray(ev && ev.content) ? ev.content : [])
    .map(function (b) { return b && b.type === 'text' && typeof b.text === 'string' ? b.text : ''; })
    .filter(Boolean).join('\n');
}

/* ── Is this text a table? ────────────────────────────────────────────────
   Three shapes a script prints rows in: a JSON array of flat objects, a
   CSV/TSV with a header and a consistent column count, and the pipe table
   a text formatter draws. Anything else stays text — a false table is worse
   than none. */
function tableFrom(text) {
  var s = String(text == null ? '' : text).trim();
  if (!s) return null;
  var t = fromJson(s) || fromDelimited(s, '\t') || fromDelimited(s, ',') || fromPipes(s);
  if (!t || !t.columns.length || t.columns.length > TABLE_MAX_COLS || !t.rows.length) return null;
  if (t.rows.length > TABLE_MAX_ROWS) { t.truncated = t.rows.length; t.rows = t.rows.slice(0, TABLE_MAX_ROWS); }
  return t;
}
function fromJson(s) {
  if (s[0] !== '[') return null;
  var arr;
  try { arr = JSON.parse(s); } catch (e) { return null; }
  if (!Array.isArray(arr) || !arr.length || !arr.every(function (r) { return r && typeof r === 'object' && !Array.isArray(r); })) return null;
  var cols = [];
  arr.forEach(function (r) { Object.keys(r).forEach(function (k) { if (cols.indexOf(k) === -1) cols.push(k); }); });
  var flat = arr.every(function (r) { return Object.keys(r).every(function (k) { var v = r[k]; return v == null || typeof v !== 'object'; }); });
  if (!flat) return null;
  return { columns: cols, rows: arr.map(function (r) { return cols.map(function (c) { return r[c] == null ? '' : String(r[c]); }); }) };
}
function splitDelimited(line, d) {
  var out = [], cur = '', q = false;
  for (var i = 0; i < line.length; i++) {
    var ch = line[i];
    if (q) { if (ch === '"') { if (line[i + 1] === '"') { cur += '"'; i++; } else q = false; } else cur += ch; continue; }
    if (ch === '"') { q = true; continue; }
    if (ch === d) { out.push(cur); cur = ''; continue; }
    cur += ch;
  }
  out.push(cur);
  return out.map(function (v) { return v.trim(); });
}
function fromDelimited(s, d) {
  var lines = s.split(/\r?\n/).filter(function (l) { return l.trim() !== ''; });
  if (lines.length < 2 || lines[0].indexOf(d) === -1) return null;
  var head = splitDelimited(lines[0], d);
  if (head.length < 2 || head.some(function (h) { return !h; })) return null;
  var rows = [];
  for (var i = 1; i < lines.length; i++) {
    var r = splitDelimited(lines[i], d);
    if (r.length !== head.length) return null;
    rows.push(r);
  }
  return { columns: head, rows: rows };
}
function fromPipes(s) {
  var lines = s.split(/\r?\n/).filter(function (l) { return l.trim() !== ''; });
  if (lines.length < 2 || !/^\s*\|.*\|\s*$/.test(lines[0])) return null;
  var cells = function (l) { return l.trim().replace(/^\||\|$/g, '').split('|').map(function (v) { return v.trim(); }); };
  var head = cells(lines[0]);
  var rows = [];
  for (var i = 1; i < lines.length; i++) {
    if (/^\s*\|?[\s:|-]+\|?\s*$/.test(lines[i])) continue;          // the ---|--- rule
    if (!/^\s*\|.*\|\s*$/.test(lines[i])) return null;
    var r = cells(lines[i]);
    if (r.length !== head.length) return null;
    rows.push(r);
  }
  return { columns: head, rows: rows };
}

/* ── Did the database let the workspace in? ─────────────────────────────── */
var FAILURE_RE = /timed? ?out|connection refused|could not connect|unable to connect|cannot connect|login timeout|adaptive server is unavailable|no route to host|network is unreachable|ETIMEDOUT|ECONNREFUSED|ENETUNREACH|EHOSTUNREACH|server closed the connection|is the server running|name or service not known|getaddrinfo|not allowed to access the server|client with ip address|firewall/i;
function looksLikeConnectionFailure(text) { return FAILURE_RE.test(String(text == null ? '' : text)); }

/* ── Events into the two columns ─────────────────────────────────────────── */
function toolBlock(ev) {
  var input = ev.input || {};
  var name = String(ev.name || ev.tool_name || 'tool');
  var b = { id: ev.id, kind: 'command', title: name, body: '', toolUseId: ev.id };
  if (name === 'bash') { b.kind = 'command'; b.title = 'Command run'; b.body = String(input.command || ''); }
  else if (name === 'write') { b.kind = 'code'; b.title = 'Wrote ' + (input.path || input.file_path || 'a file'); b.body = String(input.content || input.file_text || ''); }
  else if (name === 'edit') { b.kind = 'code'; b.title = 'Edited ' + (input.path || input.file_path || 'a file'); b.body = 'Replaced:\n' + String(input.old_string || input.old_str || '') + '\n\nWith:\n' + String(input.new_string || input.new_str || ''); }
  else if (name === 'read') { b.kind = 'command'; b.title = 'Read ' + (input.path || input.file_path || 'a file'); b.body = ''; }
  else if (name === 'glob' || name === 'grep') { b.kind = 'command'; b.title = name + ' ' + (input.pattern || ''); b.body = JSON.stringify(input); }
  else { b.body = JSON.stringify(input, null, 2); }
  return b;
}

/* The database bridge (Oct-2026): Claude's queries are MCP tool calls to
   Cygenix, not commands in the workspace. The call shows its SQL; the
   answer, which the bridge returns as { columns, rows, ... }, shows as a
   table straight away rather than as JSON. */
var BRIDGE_TITLES = { run_query: 'Query run', list_tables: 'Listed tables', describe_table: 'Described a table' };
function mcpBlock(ev) {
  var input = ev.input || {};
  var name = String(ev.name || 'tool');
  var b = { id: ev.id, kind: 'command', title: BRIDGE_TITLES[name] || name, body: '', toolUseId: ev.id };
  if (name === 'run_query') { b.kind = 'code'; b.body = String(input.sql || ''); }
  else if (name === 'describe_table') { b.title = 'Described ' + (input.schema ? input.schema + '.' : '') + (input.table || 'a table'); }
  else if (name === 'list_tables') { b.title = 'Listed tables' + (input.schema ? ' in ' + input.schema : ''); }
  else { b.body = JSON.stringify(input, null, 2); }
  return b;
}
function bridgeTable(text) {
  var j;
  try { j = JSON.parse(text); } catch (e) { return null; }
  if (!j || !Array.isArray(j.columns) || !Array.isArray(j.rows)) return null;
  return {
    columns: j.columns.map(String),
    rows: j.rows.map(function (r) { return (r || []).map(function (v) { return v === null || v === undefined ? 'NULL' : String(v); }); }),
    truncated: j.truncated ? (j.total_rows_read || 0) : 0,
  };
}

function blocksFrom(events) {
  var chat = [], output = [], flags = { budgetReached: false, connectionFailure: false, lastStopReason: null, thinking: false };
  var byToolUse = {};
  (events || []).forEach(function (ev) {
    if (!ev || !ev.type) return;
    switch (ev.type) {
      case 'user.message': chat.push({ id: ev.id, role: 'user', text: textOf(ev) }); break;
      case 'agent.message': chat.push({ id: ev.id, role: 'assistant', text: textOf(ev) }); flags.thinking = false; break;
      case 'system.message': {
        var t = textOf(ev);
        chat.push({ id: ev.id, role: 'notice', text: /CHANGES ALLOWED/.test(t) ? 'Changes to data are now allowed for this session.'
          : /READ-ONLY/.test(t) ? 'This session is read-only again: Claude has been told not to change data.' : t });
        break;
      }
      case 'agent.thinking': flags.thinking = true; break;
      case 'agent.tool_use': { var tb = toolBlock(ev); byToolUse[ev.id] = tb; output.push(tb); flags.thinking = false; break; }
      case 'agent.tool_result': {
        var text = textOf(ev);
        var rb = { id: ev.id, kind: ev.is_error ? 'error' : 'result', title: ev.is_error ? 'Failed' : 'Output', body: text,
                   table: ev.is_error ? null : tableFrom(text), toolUseId: ev.tool_use_id || null };
        var parent = ev.tool_use_id && byToolUse[ev.tool_use_id];
        if (parent) parent.result = rb; else output.push(rb);
        if (looksLikeConnectionFailure(text)) flags.connectionFailure = true;
        break;
      }
      case 'agent.mcp_tool_use': { var mb = mcpBlock(ev); byToolUse[ev.id] = mb; output.push(mb); flags.thinking = false; break; }
      case 'agent.mcp_tool_result': {
        var mt = textOf(ev);
        var mtab = ev.is_error ? null : bridgeTable(mt);
        var mr = { id: ev.id, kind: ev.is_error ? 'error' : 'result', title: ev.is_error ? 'Failed' : (mtab ? mtab.rows.length + ' row' + (mtab.rows.length === 1 ? '' : 's') : 'Output'),
                   body: mtab ? '' : mt, table: mtab, toolUseId: ev.mcp_tool_use_id || null };
        var mp = ev.mcp_tool_use_id && byToolUse[ev.mcp_tool_use_id];
        if (mp) mp.result = mr; else output.push(mr);
        if (ev.is_error && looksLikeConnectionFailure(mt)) flags.connectionFailure = true;
        break;
      }
      case 'session.error': {
        var er = ev.error || {};
        output.push({ id: ev.id, kind: 'error', title: 'Session error', body: String(er.message || er.type || 'unknown error') });
        break;
      }
      case 'session.status_idle': {
        var sr = ev.stop_reason && ev.stop_reason.type;
        flags.lastStopReason = sr || null; flags.thinking = false;
        if (sr === 'budget_reached') { flags.budgetReached = true; output.push({ id: ev.id, kind: 'notice', title: 'Spend cap reached', body: 'This session has reached its spend cap and has paused. Start a new session to carry on.' }); }
        break;
      }
      default: break;
    }
  });
  return { chat: chat, output: output, flags: flags };
}

function statusWord(status) { return STATUS_WORDS[status] || STATUS_WORDS.none; }

/* The first-use notice, remembered per user in one localStorage key holding
   a map: { "<user>": "<iso date>" }. Pure helpers; the page does the storage. */
var NOTICE_KEY = 'cygenix_cc_notice';
function noticeDismissed(raw, user) {
  try { var m = JSON.parse(raw || '{}'); return !!(m && user && m[user]); } catch (e) { return false; }
}
function noticeDismiss(raw, user, at) {
  var m; try { m = JSON.parse(raw || '{}'); } catch (e) { m = {}; }
  if (!m || typeof m !== 'object') m = {};
  if (user) m[user] = at || new Date().toISOString();
  return JSON.stringify(m);
}

/* The confirmation the toggle asks for. A production profile (PRD, the red
   state) must be named back; anything else is a plain yes/no. */
function confirmSpec(profile, connection) {
  var name = (profile && profile.name) || 'the active profile';
  var env = (profile && profile.envClass) || '';
  return {
    prod: env === 'PRD',
    title: 'Allow changes to data this session?',
    text: 'Claude will be allowed to run code that changes data on "' + ((connection && connection.name) || 'the selected connection')
      + '" under profile "' + name + '"' + (env ? ' (' + env + ')' : '') + '. This is recorded in the audit log.',
    typeToConfirm: env === 'PRD' ? name : '',
  };
}
function confirmAccepts(spec, typed) {
  if (!spec.prod) return true;
  return String(typed || '').trim() === String(spec.typeToConfirm || '').trim() && !!spec.typeToConfirm;
}

return {
  STATUS_WORDS: STATUS_WORDS, statusWord: statusWord,
  textOf: textOf, tableFrom: tableFrom, looksLikeConnectionFailure: looksLikeConnectionFailure, bridgeTable: bridgeTable, mcpBlock: mcpBlock,
  blocksFrom: blocksFrom, toolBlock: toolBlock,
  NOTICE_KEY: NOTICE_KEY, noticeDismissed: noticeDismissed, noticeDismiss: noticeDismiss,
  confirmSpec: confirmSpec, confirmAccepts: confirmAccepts,
};
});
