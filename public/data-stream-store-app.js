/* data-stream-store-app.js — the Change store screen's own code, moved out of the page.
 *
 * WHY THIS IS A FILE AND NOT A <script> BLOCK
 * It used to sit inline at about 41% of the way through data_stream_store.html, right
 * after the modules it reads from (datastream, datastream-ui, datastream-page). An inline script cannot be
 * deferred, and it needed those modules at parse time, so THEY could not be
 * deferred either. The browser therefore stopped parsing the page one tenth
 * of the way in, fetched and ran 182KB of JavaScript, and only then carried
 * on to the markup that is the actual screen. On a 5 Mbps line that was the
 * difference between a page that appears and one that sits blank.
 *
 * As an external file it is loaded with `defer`, so it runs in document order
 * after the modules — which are now deferred too — and after the whole page
 * has parsed. Nothing about scope changes: this is a classic script, so its
 * top-level functions are still the globals the page's onclick= handlers
 * call, exactly as they were inline. It also gets a content stamp from
 * scripts/stamp-assets.js and a cache entry of its own, which inline code
 * never had.
 */
'use strict';
const DS = window.CygenixDataStream;
const U  = window.CygenixDataStreamUI;
const P  = window.CygenixDataStreamPage;

let openTopic = null;
let queryId = DS.CANNED_QUERIES[0].id;

document.addEventListener('DOMContentLoaded', () => {
  P.boot();
  P.wireCopy();
  const q = P.query();
  if (q.topic) openTopic = q.topic;
  P.onTick(paint);
  P.start();
  paint();
});

function paint(state) {
  const s = state || P.state;
  if (!s) return;
  paintScope();
  paintKpis(s);
  paintTopics(s);
  paintQuery(s);
  paintRetention(s);
}

/* This profile / All profiles. The Store is a view over the streams in
   scope, so the same toggle as the Streams page, and the same reset rule. */
function paintScope() {
  const host = document.getElementById('ds-scope-host');
  if (host) host.innerHTML = U.scopeToggle(P.scope(), P.activeProfile());
}
function dsScope(mode) { P.setScope(mode); paint(); P.announce(mode === 'all' ? 'Showing every profile.' : 'Showing this profile only.'); }

function paintKpis(s) {
  const t = DS.storeTotals(s, P.visible(s));
  const words = U.scopeWords(P.scope(), P.activeProfile());
  const tile = (label, figure, context) =>
    '<div class="ds-kpi" role="group"><div class="ds-kpi-label">' + label + '</div>'
    + '<div class="ds-kpi-figure">' + figure + '</div>'
    + '<div class="ds-kpi-context">' + context + '</div></div>';
  const window_ = t.minRetentionHours === t.maxRetentionHours
    ? t.maxRetentionHours + 'h'
    : t.minRetentionHours + '–' + t.maxRetentionHours + 'h';
  document.getElementById('ds-kpis').innerHTML =
      tile('Records retained', DS.formatCount(t.records), t.topics + ' topic(s) · ' + words)
    + tile('Store size', DS.formatBytes(t.bytes), 'change records, not table data')
    + tile('Retention window', window_, 'how far back a replay can reach')
    + tile('Oldest record', t.oldestRecordAt ? DS.relativeTime(t.oldestRecordAt, s.clockNow) : '—',
        t.oldestRecordAt ? 'anything older has been evicted' : 'nothing retained yet');
}

function paintTopics(s) {
  const topics = DS.topicsOf(s, P.visible(s));
  const el = document.getElementById('st-topics');
  document.getElementById('st-sub').textContent = topics.length + ' topic(s), one per streamed object · '
    + U.scopeWords(P.scope(), P.activeProfile());
  if (!topics.length) {
    el.innerHTML = U.emptyState({ icon: 'database', title: 'The Stream Store is empty',
      body: 'Every captured change is written here before it is delivered, and kept for the stream’s '
        + 'retention window. Nothing is streaming yet, so there is nothing to hold.',
      actionHtml: '<a class="btn btn-primary" href="/data-stream">Go to Streams</a>' });
    return;
  }
  el.innerHTML = '<div class="ds-tablewrap"><table class="ds-table">'
    + '<thead><tr><th>Topic</th><th>Stream</th><th class="ds-right">Records</th><th class="ds-right">Bytes</th>'
    + '<th>Oldest → newest</th><th>Checkpoint</th><th class="ds-right">Consumer lag</th></tr></thead><tbody>'
    + topics.map(t => topicRow(t, s)).join('') + '</tbody></table></div>';
  el.querySelectorAll('tbody tr[data-topic]').forEach(tr => {
    tr.addEventListener('click', () => toggleTopic(tr.getAttribute('data-topic')));
  });
}

function topicRow(t, s) {
  const behind = (t.consumers[0] || {}).offsetBehind || 0;
  const row = '<tr data-topic="' + U.esc(t.topic) + '"' + (openTopic === t.topic ? ' class="sel"' : '') + '>'
    + '<td><div class="ds-cell-name" style="font-family:var(--mono);font-size:11.5px">' + U.esc(t.topic) + '</div>'
      + '<div class="ds-cell-sub">' + t.retentionHours + 'h retention</div></td>'
    + '<td><span class="ds-num">' + U.esc(t.streamName) + '</span></td>'
    + '<td class="ds-right"><span class="ds-num">' + t.records.toLocaleString() + '</span></td>'
    + '<td class="ds-right"><span class="ds-num">' + DS.formatBytes(t.bytes) + '</span></td>'
    + '<td><span class="ds-num">' + (t.oldestRecordAt ? U.esc(t.oldestRecordAt.slice(5, 16).replace('T', ' ')) : '—')
      + ' → ' + (t.newestRecordAt ? U.esc(t.newestRecordAt.slice(5, 16).replace('T', ' ')) : '—') + '</span></td>'
    + '<td>' + U.monoCopy(t.checkpoint, { max: 16 }) + '</td>'
    + '<td class="ds-right"><span class="ds-num' + (behind > 500 ? ' ds-lag-amber' : '') + '">'
      + behind.toLocaleString() + '</span></td></tr>';
  if (openTopic !== t.topic) return row;
  return row + '<tr><td colspan="7" style="padding:0"><div class="st-detail">'
    + '<div class="ds-grid2">'
      + '<div><div class="ds-kpi-label">Checkpoint</div><div class="ds-num">'
        + U.monoCopy(t.checkpoint, { max: 30 }) + '</div>'
        + '<div class="ds-kpi-context">Delivery has committed everything up to here.</div></div>'
      + '<div><div class="ds-kpi-label">Consumer</div><div class="ds-num">'
        + U.esc((t.consumers[0] || {}).name || '—') + '</div>'
        + '<div class="ds-kpi-context">' + behind.toLocaleString() + ' record(s) behind the head.</div></div>'
      + '<div><div class="ds-kpi-label">Retention</div><div class="ds-num">' + t.retentionHours + ' hours</div>'
        + '<div class="ds-kpi-context">Replay cannot reach further back than this.</div></div>'
    + '</div>'
    + '<div style="margin-top:0.6rem;display:flex;gap:0.4rem;flex-wrap:wrap">'
      + '<a class="btn" href="/data-stream-events'
        + DS.buildQuery({ stream: t.streamId, table: t.table }) + '">See these change records →</a>'
      + '<a class="btn" href="/data-stream' + DS.buildQuery({ stream: t.streamId })
        + '">Open the stream →</a></div>'
    + '</div></td></tr>';
}
function toggleTopic(topic) {
  openTopic = openTopic === topic ? null : topic;
  P.setUrl({ topic: openTopic });
  paint();
}

/* ── Continuous query ───────────────────────────────────────────────────── */
function paintQuery(s) {
  document.getElementById('st-qlist').innerHTML = DS.CANNED_QUERIES.map(q =>
    '<button type="button" class="' + (queryId === q.id ? 'on' : '') + '" onclick="setQuery(\''
    + q.id + '\')">' + U.esc(q.label) + '</button>').join('');
  const q = DS.CANNED_QUERIES.filter(x => x.id === queryId)[0];
  document.getElementById('st-qsql').textContent = q.sql;

  let result;
  try { result = DS.runCannedQuery(s, queryId); }
  catch (e) {
    document.getElementById('st-qresult').innerHTML = '<div class="ds-note error">' + U.esc(e.message) + '</div>';
    return;
  }
  const el = document.getElementById('st-qresult');
  if (!result.rows.length) {
    el.innerHTML = '<div class="ds-note">No rows — nothing has been captured in that window yet.</div>';
    return;
  }
  el.innerHTML = '<div class="ds-tablewrap" style="max-height:280px;overflow:auto">'
    + '<table class="ds-table"><thead><tr>'
    + result.columns.map(c => '<th>' + U.esc(c) + '</th>').join('') + '</tr></thead><tbody>'
    + result.rows.slice(0, 60).map(r => '<tr style="cursor:default">'
      + r.map(v => '<td><span class="ds-num">' + U.esc(v) + '</span></td>').join('') + '</tr>').join('')
    + '</tbody></table></div>'
    + '<div class="ds-kpi-context" style="margin-top:0.4rem">' + result.rows.length
      + ' row(s) · re-evaluated every ' + (DS.TICK_MS / 1000) + 's against the live store</div>';
}
function setQuery(id) { queryId = id; paint(); }

/* Hands the query to the SQL Editor rather than pretending this panel is one.
   The editor is where SQL is written in this console; a second one here would
   be a second place to maintain. */
function stOpenInEditor() {
  const q = DS.CANNED_QUERIES.filter(x => x.id === queryId)[0];
  const preamble = '-- Continuous query from the Cygenix Stream Store.\n'
    + '-- stream_events is the retained change buffer; rewrite it against your\n'
    + '-- own tables before running it on a database.\n\n';
  try { localStorage.setItem('cygenix_sql_editor_draft', preamble + q.sql); } catch (e) { /* storage full */ }
  location.href = '/sql-editor';
}

/* ── Retention ──────────────────────────────────────────────────────────── */
function paintRetention(s) {
  const el = document.getElementById('st-retention');
  if (!s.streams.length) { el.innerHTML = '<div class="ds-insp-body ds-dim">No streams yet.</div>'; return; }
  el.innerHTML = '<div class="ds-tablewrap"><table class="ds-table">'
    + '<thead><tr><th>Stream</th><th class="ds-right">Retained</th><th>Retention</th>'
    + '<th>Eviction policy</th><th></th></tr></thead><tbody>'
    + s.streams.map(st => '<tr style="cursor:default">'
      + '<td><div class="ds-cell-name">' + U.esc(st.name) + '</div>'
        + '<div class="ds-cell-sub">' + U.statusPill(st) + '</div></td>'
      + '<td class="ds-right"><span class="ds-num">' + st.metrics.storeRecords.toLocaleString() + '</span></td>'
      + '<td><input class="ds-input" type="number" min="1" style="width:88px" value="'
        + ((st.profile || {}).retentionHours || 72) + '" id="ret-' + U.esc(st.id) + '"> hours</td>'
      + '<td><select class="ds-input" onchange="setEviction(\'' + U.escArg(st.id) + '\', this.value)">'
        + [['drop-oldest', 'Drop the oldest records'], ['pause-capture', 'Pause capture instead']]
          .map(([v, l]) => '<option value="' + v + '"'
            + (((st.profile || {}).eviction || 'drop-oldest') === v ? ' selected' : '') + '>' + l + '</option>').join('')
        + '</select></td>'
      + '<td><button class="btn" onclick="applyRetention(\'' + U.escArg(st.id) + '\')">Apply</button></td>'
      + '</tr>').join('')
    + '</tbody></table></div>';
}
function applyRetention(id) {
  const input = document.getElementById('ret-' + id);
  const hours = Math.max(1, parseInt(input.value, 10) || 1);
  const s = DS.getStream(P.state, id);
  const was = (s.profile || {}).retentionHours || 72;
  if (hours === was) return;
  const impact = DS.retentionImpact(P.state, id, hours);
  // Shortening retention destroys replay history. Say how much, before.
  if (impact.losesHistory && !confirm(U.confirmText('retention',
      { from: was, to: hours, recordsLost: impact.recordsLost }))) {
    input.value = was;
    return;
  }
  DS.setRetention(P.state, id, hours);
  P.persist();
  paint();
  P.announce('Retention on ' + s.name + ' set to ' + hours + ' hours.');
}
function setEviction(id, policy) {
  DS.setEvictionPolicy(P.state, id, policy);
  P.persist();
  P.announce('Eviction policy updated.');
}
