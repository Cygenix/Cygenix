/* ============================================================================
   claude-code-resume.js — a past Dev Console conversation, rebuilt as
   context for a new workspace when a session is continued
   ----------------------------------------------------------------------------
   Oct-2026. A Dev Console session can now be continued from the Sessions
   list. When its Anthropic session is still alive (idle), the conversation
   carries on IN it, and Claude remembers everything by itself — nothing here
   is used. But a session that was stopped is archived at Anthropic, which is
   permanent and read-only; one that ended, or reached its spend cap, cannot
   take another message either. Those continue in a NEW workspace, and the
   only record of what happened is the transcript Cygenix kept in Cosmos.

   Claude must not start fresh: it must know what it already queried, what it
   proposed and what it was told. A new Managed Agents session can only be
   seeded with plain user messages (initial_events take no system message and
   no tool history), and a recap sent as a message would be answered as if it
   were a question. So the transcript goes into the session's instructions —
   the system prompt override, which the API caps at 100,000 characters —
   written as a record: who said what, which tool was called with what, and
   what came back.

   TOO BIG TO FIT
   The newest turns are kept word for word: every message in full, and each
   tool call with its input. A tool result in them is cut at RESULT_MAX
   characters, with the cut said, because one 1,000-row query would otherwise
   take the whole allowance and push every message out. The OLDEST turns are
   condensed, one paragraph each: what the user asked, which queries ran and
   whether they returned rows or failed, and the start of Claude's reply.
   If even the condensed turns do not fit, the earliest are left out and
   COUNTED. Nothing is dropped without being said: the stats come back to the
   route, which shows them in the chat, and the context itself says what was
   condensed and what was left out.

   The transcript was redacted before it was stored (passwords and
   connection strings are already '••••••'), so nothing here can put a
   secret into the prompt. Notices Cygenix itself added to the transcript
   (marked cygenix: 'resume') are not replayed as if Claude had seen them.

   Pure: no network, no storage. Tested in tests/claude-code.test.js.
   ========================================================================== */
'use strict';

// The whole system prompt must stay under the API's 100,000 characters; the
// route passes what is left after the ordinary instructions.
const SYSTEM_MAX = 95000;
const RESULT_MAX = 4000;          // one tool result, in a verbatim turn
const INPUT_MAX = 6000;           // one tool call's input, in a verbatim turn
const VERBATIM_SHARE = 0.75;      // of the allowance, for the newest turns
const C_ASK = 300, C_SQL = 200, C_REPLY = 400, C_ERR = 160;

function textOf(ev) {
  return (Array.isArray(ev && ev.content) ? ev.content : [])
    .map(b => (b && b.type === 'text' && typeof b.text === 'string') ? b.text : '')
    .filter(Boolean).join('\n');
}
function cut(s, n) {
  const t = String(s == null ? '' : s);
  return t.length <= n ? t : t.slice(0, n) + ' […' + (t.length - n) + ' more characters not kept]';
}
function oneLine(s, n) { return cut(String(s == null ? '' : s).replace(/\s+/g, ' ').trim(), n); }
function inputText(ev) {
  const i = ev.input || {};
  if (typeof i.sql === 'string') return i.sql;
  if (typeof i.command === 'string') return i.command;
  try { return JSON.stringify(i); } catch (e) { return ''; }
}
// What a bridge result was, in a few words: "12 rows", or the error.
function resultGist(ev) {
  const t = textOf(ev);
  if (ev.is_error) return 'failed: ' + oneLine(t, C_ERR);
  try {
    const j = JSON.parse(t);
    if (j && Array.isArray(j.rows)) return j.rows.length + ' row' + (j.rows.length === 1 ? '' : 's') + (j.truncated ? ' (cut)' : '');
  } catch (e) { /* not a table */ }
  return t ? oneLine(t, 80) : 'no output';
}

/* turnsOf(events) → [{ events }] — a turn starts at each user message.
   Anything before the first one (a mode note, say) is a turn of its own. */
function turnsOf(events) {
  const turns = [];
  let cur = null;
  (events || []).forEach(ev => {
    if (!ev || !ev.type || ev.cygenix === 'resume') return;
    if (ev.type === 'user.message' || !cur) { cur = { events: [] }; turns.push(cur); }
    cur.events.push(ev);
  });
  return turns.filter(t => t.events.some(ev => /^(user|agent)\./.test(ev.type) || ev.type === 'system.message' || ev.type === 'session.error'));
}

// One turn, word for word (tool results cut at RESULT_MAX).
function verbatim(turn, n) {
  const lines = ['--- Turn ' + n + ' ---'];
  turn.events.forEach(ev => {
    switch (ev.type) {
      case 'user.message': lines.push('USER: ' + textOf(ev)); break;
      case 'agent.message': lines.push('CLAUDE: ' + textOf(ev)); break;
      case 'system.message': lines.push('NOTE TO CLAUDE: ' + textOf(ev)); break;
      case 'agent.mcp_tool_use': lines.push('CLAUDE CALLED ' + String(ev.name || 'tool') + ': ' + cut(inputText(ev), INPUT_MAX)); break;
      case 'agent.tool_use': lines.push('CLAUDE RAN IN THE WORKSPACE (' + String(ev.name || 'tool') + '): ' + cut(inputText(ev), INPUT_MAX)); break;
      case 'agent.mcp_tool_result':
      case 'agent.tool_result': lines.push((ev.is_error ? 'IT FAILED: ' : 'IT RETURNED: ') + cut(textOf(ev), RESULT_MAX)); break;
      case 'session.error': lines.push('SESSION ERROR: ' + oneLine((ev.error && (ev.error.message || ev.error.type)) || 'unknown', 400)); break;
      default: break;
    }
  });
  return lines.join('\n');
}

// One turn, condensed to a paragraph.
function condensed(turn, n) {
  const asked = turn.events.filter(e => e.type === 'user.message').map(textOf).join(' ');
  const said = turn.events.filter(e => e.type === 'agent.message').map(textOf).join(' ');
  const calls = [];
  const byId = {};
  turn.events.forEach(ev => {
    if (ev.type === 'agent.mcp_tool_use' || ev.type === 'agent.tool_use') {
      const c = { name: String(ev.name || 'tool'), input: oneLine(inputText(ev), C_SQL), gist: 'no result recorded' };
      byId[ev.id] = c; calls.push(c);
    } else if (ev.type === 'agent.mcp_tool_result' || ev.type === 'agent.tool_result') {
      const c = byId[ev.mcp_tool_use_id || ev.tool_use_id];
      if (c) c.gist = resultGist(ev);
    }
  });
  const bits = ['Turn ' + n + ' (condensed).'];
  if (asked) bits.push('The user asked: "' + oneLine(asked, C_ASK) + '".');
  if (calls.length) bits.push('Claude called: ' + calls.map(c => c.name + (c.input ? ' [' + c.input + ']' : '') + ' → ' + c.gist).join('; ') + '.');
  if (said) bits.push('Claude replied: "' + oneLine(said, C_REPLY) + '".');
  return bits.join(' ');
}

/* resumeContext(events, opts) → { text, stats }
   opts.budget: characters available for the text (default SYSTEM_MAX).
   stats: { turns, verbatim, condensed, omitted, resultsCut } */
function resumeContext(events, opts) {
  const o = opts || {};
  const budget = Math.max(2000, Number(o.budget) || SYSTEM_MAX);
  const turns = turnsOf(events);
  const stats = { turns: turns.length, verbatim: 0, condensed: 0, omitted: 0, resultsCut: 0 };
  if (!turns.length) return { text: '', stats };

  const full = turns.map((t, i) => verbatim(t, i + 1));
  stats.resultsCut = turns.reduce((n, t) => n + t.events.filter(e => /tool_result$/.test(e.type) && textOf(e).length > RESULT_MAX).length, 0);
  const head = (s) => 'THE CONVERSATION SO FAR'
    + '\nThis session is being continued. Everything below already happened, in this order: what the user said, what you '
    + 'said, every tool you called and what it returned. Treat it as your own memory of the session. The queries in it ran '
    + 'when they were made; the data may have changed since, so run a query again before relying on an old result. '
    + 'It is a record, not a request: answer only the new messages that follow it.'
    + (s ? '\n' + s : '');
  const total = full.reduce((n, s) => n + s.length + 2, 0);
  if (head('').length + total <= budget) {
    stats.verbatim = turns.length;
    return { text: head('') + '\n\n' + full.join('\n\n'), stats };
  }

  // Newest turns verbatim, up to VERBATIM_SHARE of the allowance — always at
  // least the newest one, cut to fit if it must be.
  const keep = [];
  let used = 0;
  const vBudget = Math.floor(budget * VERBATIM_SHARE);
  for (let i = full.length - 1; i >= 0; i--) {
    if (keep.length && used + full[i].length + 2 > vBudget) break;
    keep.unshift(i); used += full[i].length + 2;
  }
  const firstKept = keep[0];
  // Older turns condensed, newest first, until the allowance is spent; the
  // earliest beyond it are counted, not silently lost.
  const older = [];
  let left = budget - used - 600;
  for (let i = firstKept - 1; i >= 0; i--) {
    const c = condensed(turns[i], i + 1);
    if (c.length + 1 > left) break;
    older.unshift(c); left -= c.length + 1;
  }
  stats.verbatim = keep.length;
  stats.condensed = older.length;
  stats.omitted = firstKept - older.length;
  const say = [];
  if (stats.omitted) say.push('The earliest ' + stats.omitted + ' turn' + (stats.omitted === 1 ? ' is' : 's are') + ' left out to fit; ask the user if you need them.');
  if (stats.condensed) say.push('The ' + stats.condensed + ' turn' + (stats.condensed === 1 ? '' : 's') + ' after ' + (stats.omitted ? 'that' : 'the start') + ' are condensed to one paragraph each.');
  say.push('The newest ' + stats.verbatim + ' turn' + (stats.verbatim === 1 ? ' is' : 's are') + ' word for word.');
  let text = head(say.join(' ')) + (older.length ? '\n\n' + older.join('\n') : '') + '\n\n' + keep.map(i => full[i]).join('\n\n');
  if (text.length > budget) text = text.slice(0, budget - 60) + '\n[…the rest of this turn did not fit]';
  return { text, stats };
}

// The one line the person sees in the chat about what Claude was given.
function contextNotice(stats) {
  if (!stats || !stats.turns) return '';
  if (!stats.condensed && !stats.omitted) return 'Claude has the whole earlier conversation (' + stats.turns + ' turn' + (stats.turns === 1 ? '' : 's') + ')'
    + (stats.resultsCut ? ', with ' + stats.resultsCut + ' long result' + (stats.resultsCut === 1 ? '' : 's') + ' shortened' : '') + '.';
  return 'The earlier conversation was too long to give Claude in full: the newest ' + stats.verbatim + ' turn' + (stats.verbatim === 1 ? ' is' : 's are') + ' word for word'
    + (stats.condensed ? ', ' + stats.condensed + ' older one' + (stats.condensed === 1 ? ' is' : 's are') + ' condensed' : '')
    + (stats.omitted ? ', and the earliest ' + stats.omitted + (stats.omitted === 1 ? ' is' : ' are') + ' left out' : '') + '.';
}

module.exports = { resumeContext, contextNotice, turnsOf, verbatim, condensed, SYSTEM_MAX, RESULT_MAX };
