/* ============================================================================
   cygenix-datastream-ui.js — the module's shared vocabulary, as markup.
   ----------------------------------------------------------------------------
   Five screens show the same handful of things: what a stream's status is,
   which side it reads from, how far behind it is, what an operation was. Left
   to each page, those drift — the Monitor ends up calling amber what the
   Streams list calls red, and the SOURCE badge on one screen is a different
   colour from the SOURCE badge on another. That is not a cosmetic problem:
   "which side is this reading from" is the first question a migration
   engineer asks, and an answer that changes between screens is worse than no
   answer.

   So every one of those pieces is built exactly once, here.

   Everything in this file is a pure string function of its arguments — no DOM
   access, no page state — which is what lets the tests assert on the markup
   directly rather than by driving a browser.

   ACCESSIBILITY, NOT DECORATION
   Every status pill carries its own words. Colour is a second signal on top
   of the label, never the only one; a pill that said its state in green alone
   would be unreadable to a colourblind operator and invisible in print.
   ========================================================================== */
(function (root, factory) {
  var DS = (typeof module !== 'undefined' && module.exports)
    ? require('./cygenix-datastream.js')
    : (root && root.CygenixDataStream);
  var api = factory(DS);
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixDataStreamUI) root.CygenixDataStreamUI = api;
})(typeof window !== 'undefined' ? window : this, function (DS) {
'use strict';

function esc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}
/* For a value going into a single-quoted onclick argument. */
function escArg(s) { return String(s == null ? '' : s).replace(/\\/g, '\\\\').replace(/'/g, "\\'"); }

/* ── Status ──────────────────────────────────────────────────────────────
   The pill is the status, in words, with a coloured dot beside it. The class
   drives the colour; the text carries the meaning on its own. */
function statusPill(stream) {
  var s = stream && stream.status;
  var label = DS.statusLabel(s, stream);
  return '<span class="ds-pill ds-st-' + esc(s) + '">'
    + '<i class="ic ic-dot"></i>' + esc(label) + '</span>';
}

/* ── Side ────────────────────────────────────────────────────────────────
   The single most important glyph on the Streams screen. Teal for source,
   purple for target, and never the two swapped — hence one definition. */
function sideBadge(side) {
  var src = side === 'source';
  return '<span class="ds-side ds-side-' + (src ? 'source' : 'target') + '">'
    + (src ? 'SOURCE' : 'TARGET') + '</span>';
}

/* ── Lag ─────────────────────────────────────────────────────────────────
   Mono, right aligned, and coloured against the STREAM's own threshold. */
function lagCell(stream) {
  var sec = stream.metrics.lagSeconds;
  var thr = (stream.delivery && stream.delivery.lagThresholdSeconds) || 30;
  var level = DS.lagLevel(sec, thr);
  var live = DS.isLive(stream.status) || stream.status === 'failed';
  if (!live) return '<span class="ds-lag">—</span>';
  return '<span class="ds-lag ds-lag-' + level + '" title="threshold ' + thr + 's">'
    + esc(DS.formatLag(sec)) + '</span>';
}

/* ── Operation badge ─────────────────────────────────────────────────────
   I green, U accent, D red, DDL purple, T amber. Letters, not colours. */
var OP_TITLE = { I: 'Insert', U: 'Update', D: 'Delete', T: 'Truncate', DDL: 'Schema change' };
function opBadge(op) {
  return '<span class="ds-op ds-op-' + esc(op) + '" title="' + esc(OP_TITLE[op] || op) + '">'
    + esc(op) + '</span>';
}

function deliveryPill(state) {
  var label = { pending: 'Pending', delivered: 'Delivered', retrying: 'Retrying', dead: 'Dead' }[state] || state;
  return '<span class="ds-pill ds-dl-' + esc(state) + '">' + esc(label) + '</span>';
}

/* ── Sparkline ───────────────────────────────────────────────────────────
   Twelve points of throughput as an inline SVG polyline. Deliberately
   unlabelled: it is a shape, and the number beside it is the figure. */
function sparkline(values, opts) {
  var o = opts || {};
  var w = o.width || 62, h = o.height || 16;
  var vals = (values || []).slice(-12);
  if (vals.length < 2) return '<svg class="ds-spark" width="' + w + '" height="' + h + '" aria-hidden="true"></svg>';
  var max = Math.max.apply(null, vals) || 1;
  var step = w / (vals.length - 1);
  var pts = vals.map(function (v, i) {
    return (i * step).toFixed(1) + ',' + (h - 1 - (v / max) * (h - 2)).toFixed(1);
  }).join(' ');
  return '<svg class="ds-spark" width="' + w + '" height="' + h + '" viewBox="0 0 ' + w + ' ' + h + '"'
    + ' aria-hidden="true" focusable="false">'
    + '<polyline points="' + pts + '" fill="none" stroke="currentColor" stroke-width="1.2"'
    + ' stroke-linecap="round" stroke-linejoin="round"/></svg>';
}

/* ── Line chart ──────────────────────────────────────────────────────────
   Used for the inspector's lag chart and the Monitor. The threshold rule is
   drawn as a dashed line so "over the line" is literally visible. */
function lineChart(series, opts) {
  var o = opts || {};
  var w = o.width || 300, h = o.height || 90, pad = 4;
  var vals = (series || []).slice(-o.points || -60);
  if (!vals.length) return '<div class="ds-chart-empty">Nothing recorded yet.</div>';
  var max = Math.max(o.min || 0, Math.max.apply(null, vals), o.threshold || 0) || 1;
  var step = vals.length > 1 ? (w - pad * 2) / (vals.length - 1) : 0;
  var y = function (v) { return h - pad - (v / max) * (h - pad * 2); };
  var pts = vals.map(function (v, i) { return (pad + i * step).toFixed(1) + ',' + y(v).toFixed(1); }).join(' ');
  var rule = o.threshold
    ? '<line x1="' + pad + '" x2="' + (w - pad) + '" y1="' + y(o.threshold).toFixed(1)
      + '" y2="' + y(o.threshold).toFixed(1) + '" class="ds-chart-rule"/>'
    : '';
  return '<svg class="ds-chart" viewBox="0 0 ' + w + ' ' + h + '" preserveAspectRatio="none"'
    + ' role="img" aria-label="' + esc(o.label || 'chart') + '">'
    + rule
    + '<polyline points="' + pts + '" fill="none" stroke="currentColor" stroke-width="1.4"'
    + ' stroke-linejoin="round" stroke-linecap="round"/></svg>';
}

/* ── Stacked bars (events in vs out) ─────────────────────────────────── */
function barChart(rows, opts) {
  var o = opts || {};
  var w = o.width || 300, h = o.height || 90, pad = 4;
  if (!rows || !rows.length) return '<div class="ds-chart-empty">Nothing recorded yet.</div>';
  var max = rows.reduce(function (m, r) { return Math.max(m, r.a + r.b); }, 0) || 1;
  var bw = (w - pad * 2) / rows.length;
  var bars = rows.map(function (r, i) {
    var x = pad + i * bw;
    var ha = ((r.a / max) * (h - pad * 2));
    var hb = ((r.b / max) * (h - pad * 2));
    return '<rect class="ds-bar-a" x="' + x.toFixed(1) + '" y="' + (h - pad - ha).toFixed(1)
         + '" width="' + Math.max(0.8, bw - 1).toFixed(1) + '" height="' + ha.toFixed(1) + '"/>'
         + '<rect class="ds-bar-b" x="' + x.toFixed(1) + '" y="' + (h - pad - ha - hb).toFixed(1)
         + '" width="' + Math.max(0.8, bw - 1).toFixed(1) + '" height="' + hb.toFixed(1) + '"/>';
  }).join('');
  return '<svg class="ds-chart" viewBox="0 0 ' + w + ' ' + h + '" preserveAspectRatio="none"'
    + ' role="img" aria-label="' + esc(o.label || 'chart') + '">' + bars + '</svg>';
}

/* ── Relative time with the absolute in the title ────────────────────────
   "4s ago" is what you read; the UTC instant is what you check. */
function timeAgo(iso, now) {
  if (!iso) return '<span class="ds-dim">never</span>';
  return '<span class="ds-ago" title="' + esc(iso) + '">' + esc(DS.relativeTime(iso, now)) + '</span>';
}

/* ── Mono value with a copy button ───────────────────────────────────────
   LSNs are long and are copied more often than they are read. */
function monoCopy(value, opts) {
  var o = opts || {};
  var text = String(value == null ? '' : value);
  var shown = o.max && text.length > o.max ? text.slice(0, o.max) + '…' : text;
  if (!text) return '<span class="ds-dim">—</span>';
  return '<span class="ds-mono-copy"><code title="' + esc(text) + '">' + esc(shown) + '</code>'
    + '<button class="ds-copy" type="button" title="Copy" aria-label="Copy value"'
    + ' data-copy="' + esc(text) + '"><i class="ic ic-clipboard"></i></button></span>';
}

/* ── The Flow diagram ────────────────────────────────────────────────────
   Three cards and two arrows. This is the picture that explains capture →
   Stream Store → delivery without words, so the rates on the arrows have to
   be the real ones and the bottleneck has to be the real bottleneck. */
function flowDiagram(stream) {
  var f = DS.flowOf(stream);
  var arrow = function (which, label) {
    var hot = f.bottleneck === which;
    return '<div class="ds-flow-arrow' + (hot ? ' hot' : '') + '">'
      + '<div class="ds-flow-rate">' + esc(label) + '</div>'
      + '<div class="ds-flow-line" aria-hidden="true"></div>'
      + (hot ? '<div class="ds-flow-warn">bottleneck</div>' : '')
      + '</div>';
  };
  return '<div class="ds-flow">'
    + '<div class="ds-flow-node">'
      + '<div class="ds-flow-kind">' + sideBadge(f.capture.side) + '</div>'
      + '<div class="ds-flow-name">' + esc(f.capture.label) + '</div>'
      + '<div class="ds-flow-meta">' + esc(f.capture.method) + '</div>'
    + '</div>'
    + arrow('capture', 'capture ' + DS.formatCount(f.capture.rate) + '/min')
    + '<div class="ds-flow-node ds-flow-store">'
      + '<div class="ds-flow-kind"><i class="ic ic-database"></i> STREAM STORE</div>'
      + '<div class="ds-flow-name">' + DS.formatCount(f.store.pending) + ' pending</div>'
      + '<div class="ds-flow-meta">' + esc(f.store.retentionHours) + 'h retention · '
        + DS.formatCount(f.store.records) + ' retained</div>'
    + '</div>'
    + arrow('delivery', 'deliver ' + DS.formatCount(f.destination.rate) + '/min')
    + '<div class="ds-flow-node">'
      + '<div class="ds-flow-kind">' + esc((DS.DEST_KIND_LABEL[f.destination.kind] || f.destination.kind).toUpperCase()) + '</div>'
      + '<div class="ds-flow-name">' + esc(f.destination.label) + '</div>'
      + '<div class="ds-flow-meta">' + esc(f.destination.writeMode) + '</div>'
    + '</div>'
    + '</div>';
}

/* ── Empty state ─────────────────────────────────────────────────────────
   Says what the screen is for and gives the one action that fills it — a
   screen that just says "nothing here" leaves the user to guess. */
function emptyState(opts) {
  var o = opts || {};
  return '<div class="ds-empty">'
    + '<div class="ds-empty-icon"><i class="ic ic-lg ic-' + esc(o.icon || 'stream') + '"></i></div>'
    + '<div class="ds-empty-title">' + esc(o.title || '') + '</div>'
    + '<div class="ds-empty-body">' + (o.bodyHtml || esc(o.body || '')) + '</div>'
    + (o.actionHtml ? '<div class="ds-empty-action">' + o.actionHtml + '</div>' : '')
    + '</div>';
}

/* Skeleton rows rather than a spinner: the shape of what is coming beats a
   circle that says only "wait". */
function skeletonRows(n, cols) {
  var out = [];
  for (var i = 0; i < (n || 5); i++) {
    var cells = [];
    for (var c = 0; c < (cols || 6); c++) cells.push('<td><span class="ds-skel"></span></td>');
    out.push('<tr class="ds-skel-row">' + cells.join('') + '</tr>');
  }
  return out.join('');
}

/* ── Confirm copy ────────────────────────────────────────────────────────
   Destructive actions state their blast radius. "Are you sure?" is not a
   confirmation, it is a speed bump; the number is the confirmation. */
function confirmText(kind, detail) {
  var d = detail || {};
  switch (kind) {
    case 'start':
      return 'Start "' + d.name + '"?\n\nThis begins reading ' + d.connection
        + ' continuously and delivering changes to ' + d.destination + '.'
        + (d.snapshotNote ? '\n\n' + d.snapshotNote : '');
    case 'pause':
      return 'Pause "' + d.name + '"?\n\nCapture stops. The '
        + Number(d.pending || 0).toLocaleString() + ' record(s) already in the Stream Store are kept, '
        + 'and delivery resumes from the same position.';
    case 'stop':
      return 'Stop "' + d.name + '"?\n\nCapture and delivery both stop. Restarting reads from the '
        + 'last committed position, so nothing is lost, but the gap will have to be caught up.';
    case 'delete':
      return 'Delete "' + d.name + '"?\n\nThe stream, its position and its retained Stream Store '
        + 'records are removed. The source and destination databases are not touched.';
    case 'replay':
      return 'This will re-deliver ' + Number(d.records || 0).toLocaleString()
        + ' record(s) to ' + d.destination + '.\n\nRecords already delivered will be sent again. '
        + 'Continue?';
    case 'requeue':
      return 'Requeue ' + Number(d.records || 0).toLocaleString() + ' dead-lettered record(s) to '
        + d.destination + '?\n\nThey failed once already; if the cause has not been fixed they '
        + 'will fail again.';
    case 'resync':
      return 'Resync ' + d.table + '?\n\nCygenix re-reads all '
        + Number(d.rows || 0).toLocaleString() + ' row(s) of that table and re-delivers them. '
        + 'The rest of the stream keeps running.';
    case 'retention':
      return 'Shorten retention from ' + d.from + 'h to ' + d.to + 'h?\n\nAbout '
        + Number(d.recordsLost || 0).toLocaleString() + ' retained record(s) are dropped, and '
        + 'you will no longer be able to replay further back than ' + d.to + ' hours.';
    default:
      return 'Continue?';
  }
}

return {
  esc: esc, escArg: escArg,
  statusPill: statusPill, sideBadge: sideBadge, lagCell: lagCell,
  opBadge: opBadge, deliveryPill: deliveryPill,
  sparkline: sparkline, lineChart: lineChart, barChart: barChart,
  timeAgo: timeAgo, monoCopy: monoCopy, flowDiagram: flowDiagram,
  emptyState: emptyState, skeletonRows: skeletonRows, confirmText: confirmText,
  OP_TITLE: OP_TITLE,
};
});
