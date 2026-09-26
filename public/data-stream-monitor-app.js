/* data-stream-monitor-app.js — the Monitor screen's own code, moved out of the page.
 *
 * WHY THIS IS A FILE AND NOT A <script> BLOCK
 * It used to sit inline at about 50% of the way through data_stream_monitor.html, right
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

/* The time range is a count of 2s points, so every chart on the screen covers
   exactly the same window — charts that disagreed about "an hour" would be
   worse than no charts. */
const RANGES = [
  { id: '15m', label: '15m', points: 450 },
  { id: '1h',  label: '1h',  points: 1800 },
  { id: '24h', label: '24h', points: 43200 },
  { id: '7d',  label: '7d',  points: 302400 },
];
let range = '15m';

document.addEventListener('DOMContentLoaded', () => {
  P.boot();
  const q = P.query();
  if (RANGES.some(r => r.id === q.range)) range = q.range;
  P.onTick(paint);
  P.start();
  paint();
});

function points(s) {
  const want = (RANGES.filter(r => r.id === range)[0] || RANGES[0]).points;
  return (s.points || []).slice(-want);
}
/* Points are per stream per tick, so collapse them onto one series per tick
   before charting — otherwise five streams would draw a sawtooth. */
function byTick(s, pick) {
  const pts = points(s);
  const buckets = new Map();
  pts.forEach(p => {
    const k = p.t;
    buckets.set(k, (buckets.get(k) || 0) + pick(p));
  });
  return Array.from(buckets.values());
}

function paint(state) {
  const s0 = state || P.state;
  if (!s0) return;
  paintScope();
  // Everything on this screen is about the streams in scope, so the charts,
  // the heat map and the alerts read a view of the state narrowed to them.
  // The module's own audit list stays whole: it is a record, not a view.
  const s = scoped(s0);
  paintRange();
  paintKpis(s);
  paintCharts(s);
  paintHeat(s);
  paintAlerts(s);
  paintAudit(s0);
}
function scoped(s) {
  const ids = P.visibleIds(s);
  return Object.assign({}, s, {
    streams: P.visible(s),
    events: (s.events || []).filter(e => ids[e.streamId]),
    points: (s.points || []).filter(p => ids[p.streamId]),
  });
}
function paintScope() {
  const host = document.getElementById('ds-scope-host');
  if (host) host.innerHTML = U.scopeToggle(P.scope(), P.activeProfile());
}
function dsScope(mode) { P.setScope(mode); paint(); P.announce(mode === 'all' ? 'Showing every profile.' : 'Showing this profile only.'); }

function paintRange() {
  document.getElementById('mon-range').innerHTML =
    '<div style="display:flex;gap:0.3rem">' + RANGES.map(r =>
      '<button class="btn' + (range === r.id ? ' on' : '') + '" onclick="setRange(\'' + r.id + '\')">'
      + r.label + '</button>').join('') + '</div>';
}
function setRange(r) {
  range = r;
  P.setUrl({ range: r === '15m' ? null : r });
  paint();
}

function paintKpis(s) {
  const k = DS.kpis(s);
  const words = U.scopeWords(P.scope(), P.activeProfile());
  const tile = (label, figure, cls, context) =>
    '<div class="ds-kpi" role="group"><div class="ds-kpi-label">' + label + '</div>'
    + '<div class="ds-kpi-figure' + (cls ? ' ' + cls : '') + '">' + figure + '</div>'
    + '<div class="ds-kpi-context">' + context + '</div></div>';
  const level = DS.lagLevel(k.maxLagSeconds, 30);
  document.getElementById('ds-kpis').innerHTML =
      tile('Streams running', k.running + ' of ' + k.total, '', k.paused + ' paused · ' + k.failed + ' failed · ' + words)
    + tile('Events / min', DS.formatCount(k.eventsPerMin), '', 'captured across running streams · ' + words)
    + tile('Max lag', DS.formatLag(k.maxLagSeconds),
        level === 'red' ? 'bad' : level === 'amber' ? 'warn' : '',
        k.worstStream ? U.esc(k.worstStream) : 'no stream is behind')
    + tile('Dead letters', String(k.dlqDepth), k.dlqDepth ? 'bad' : '', 'waiting for a requeue');
}

function paintCharts(s) {
  const ins = byTick(s, p => p.in);
  const outs = byTick(s, p => p.out);
  const rows = ins.map((v, i) => ({ a: outs[i] || 0, b: Math.max(0, v - (outs[i] || 0)) }));
  document.getElementById('mon-throughput').innerHTML =
    U.barChart(rows.slice(-90), { label: 'Events captured and delivered per tick', height: 110 });

  // Lag is the WORST stream's lag, not an average: an average hides the one
  // stream that is in trouble behind four that are fine.
  const worstLag = [];
  const pts = points(s);
  const perTick = new Map();
  pts.forEach(p => perTick.set(p.t, Math.max(perTick.get(p.t) || 0, p.lagSeconds)));
  perTick.forEach(v => worstLag.push(v));
  document.getElementById('mon-lag').innerHTML =
    U.lineChart(worstLag.slice(-90), { threshold: 30, label: 'Worst lag in seconds', height: 110 });
  document.getElementById('mon-lag-sub').textContent = 'the worst stream at each moment · 30s rule';

  document.getElementById('mon-depth').innerHTML =
    U.lineChart(byTick(s, p => p.storeDepth).slice(-90), { label: 'Records held in the Stream Store', height: 110 });

  const errs = byTick(s, p => p.errors);
  document.getElementById('mon-errors').innerHTML = errs.some(v => v > 0)
    ? U.lineChart(errs.slice(-90), { label: 'Streams in a failed state', height: 110 })
    : '<div class="ds-note ok">No stream has failed in this window.</div>';
}

function paintHeat(s) {
  const grid = DS.heatGrid(s, 300000, 12);
  const el = document.getElementById('mon-heat');
  if (!grid.rows.length) {
    el.innerHTML = '<div class="ds-note">Nothing captured yet, so there is no volume to shade.</div>';
    return;
  }
  document.getElementById('mon-heat-sub').textContent =
    grid.rows.length + ' table(s) · five-minute buckets · peak ' + grid.peak + ' per cell';
  const heads = Array.from({ length: grid.buckets }, (_, i) => {
    const t = new Date(grid.endsAt - (grid.buckets - i) * grid.bucketMs);
    return '<th>' + String(t.getUTCHours()).padStart(2, '0') + ':'
      + String(t.getUTCMinutes()).padStart(2, '0') + '</th>';
  }).join('');
  el.innerHTML = '<div class="ds-tablewrap"><table class="ds-heat">'
    + '<thead><tr><th></th>' + heads + '</tr></thead><tbody>'
    + grid.rows.slice(0, 14).map(r => '<tr><td class="lbl">' + U.esc(r.table) + '</td>'
      + r.cells.map(c => '<td class="cell" title="' + c + ' event(s)"><i style="opacity:'
        + (c ? (0.18 + 0.82 * (c / (grid.peak || 1))).toFixed(2) : '0.06') + '"></i></td>').join('')
      + '</tr>').join('')
    + '</tbody></table></div>';
}

function paintAlerts(s) {
  const alerts = DS.alertsOf(s);
  const firing = alerts.filter(a => a.state === 'firing');
  document.getElementById('mon-alert-sub').textContent =
    firing.length ? firing.length + ' firing' : 'all quiet';
  document.getElementById('mon-alerts').innerHTML = alerts.map(a => {
    const cls = a.state === 'firing' ? 'ds-st-failed' : 'ds-st-running';
    return '<div class="mon-alert">'
      + '<span class="st ds-pill ' + cls + '"><i class="ic ic-dot"></i>'
        + (a.state === 'firing' ? 'Firing' : 'OK') + '</span>'
      + '<div class="txt"><div class="h">' + U.esc(a.label) + '</div>'
      + '<div class="b">' + (a.state === 'firing'
        ? a.streams.map(x => U.esc(x.name)).join(', ') + ' · last fired '
          + DS.relativeTime(a.lastFiredAt, s.clockNow)
        : 'No stream meets this condition.') + '</div></div></div>';
  }).join('');
}

/* Every state change this module made, newest first. The organisation's
   hash-chained trail lives in Report & Govern → Audit Log; this is the
   module's own record, and it is what makes a replay accountable after the
   fact rather than just possible. */
function paintAudit(s) {
  const el = document.getElementById('mon-audit');
  const rows = (s.audit || []).slice(0, 60);
  if (!rows.length) { el.innerHTML = '<div class="ds-insp-body ds-dim">Nothing yet.</div>'; return; }
  el.innerHTML = rows.map(a => {
    const stream = a.streamId ? DS.getStream(s, a.streamId) : null;
    return '<div class="row"><span class="t" title="' + U.esc(a.at) + '">'
      + U.esc(String(a.at).slice(11, 19)) + '</span>'
      + '<span class="a">' + U.esc(a.action) + '</span>'
      + '<span>' + U.esc(stream ? stream.name : (a.streamId || '')) + detailText(a) + '</span>'
      + '<span class="who">' + U.esc(String(a.actor || '').split('@')[0]) + '</span></div>';
  }).join('');
}
function detailText(a) {
  const d = a.detail;
  if (!d) return '';
  if (d.records !== undefined) return ' · ' + Number(d.records).toLocaleString() + ' record(s)';
  if (d.table) return ' · ' + d.table;
  if (d.to !== undefined && d.from !== undefined) return ' · ' + d.from + 'h → ' + d.to + 'h';
  if (d.pendingInStore !== undefined) return ' · ' + Number(d.pendingInStore).toLocaleString() + ' pending';
  return '';
}
