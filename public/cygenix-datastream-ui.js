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
  /* A severed line for a hop that is delivering nothing. A solid arrow with
     "0/min" written on it still reads as a connection: the picture says the
     records are flowing and the label says they are not, and people read the
     picture. So when delivery has stopped the line is broken, and the
     destination's own words are attached to the hop that stopped. */
  var stalled = f.destination.rate === 0 && f.capture.rate > 0;
  var arrow = function (which, label) {
    var hot = f.bottleneck === which;
    var cut = which === 'delivery' && stalled;
    return '<div class="ds-flow-arrow' + (hot ? ' hot' : '') + (cut ? ' cut' : '') + '">'
      + '<div class="ds-flow-rate">' + esc(cut ? 'delivering nothing' : label) + '</div>'
      + '<div class="ds-flow-line" aria-hidden="true"></div>'
      + (cut && stream.lastError
          ? '<div class="ds-flow-err" title="' + esc(stream.lastError) + '">'
            + esc(stream.lastError) + '</div>' : '')
      + (hot ? '<div class="ds-flow-warn">' + (cut ? 'broken' : 'bottleneck') + '</div>' : '')
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
    case 'checkpoint':
      return 'Reset "' + d.name + '" to its last good checkpoint?\n\nDelivery restarts from '
        + (d.position || 'the last committed position') + '. The '
        + Number(d.pending || 0).toLocaleString() + ' record(s) already in the Stream Store are '
        + 're-delivered to ' + (d.destination || 'the destination')
        + ', so records it already holds may arrive again.';
    case 'retention':
      return 'Shorten retention from ' + d.from + 'h to ' + d.to + 'h?\n\nAbout '
        + Number(d.recordsLost || 0).toLocaleString() + ' retained record(s) are dropped, and '
        + 'you will no longer be able to replay further back than ' + d.to + ' hours.';
    default:
      return 'Continue?';
  }
}

/* ══════════════════════════════════════════════════════════════════════════
   THE BLOCKED BAND
   ══════════════════════════════════════════════════════════════════════════
   Written as sentences, in the order an operator asks the questions:
   what stopped → why → what is piling up → what it means → how long is left
   → what to do. Field labels would make it a form; this has to read.

   The destination's own words are never paraphrased and never replaced by a
   friendlier version. They sit in monospace beside the plain description,
   selectable, because they are what goes in the ticket.

   Shared by /data-stream and Home so the two cannot describe the same
   incident differently.
   ══════════════════════════════════════════════════════════════════════════ */
function blockedBand(DS, state, opts) {
  var o = opts || {};
  var list = DS.blockedStreams(state);
  if (!list.length) return '';
  var idx = Math.min(o.index || 0, list.length - 1);
  var entry = list[idx];
  var s = entry.stream, b = entry.blocked;
  var d = b.deadline;
  var hard = b.level === 'blocked';

  // A paused stream nobody expects to be running does not get a live
  // "13 minutes and counting" — that clock is frozen and labelled.
  var stopped = b.paused
    ? 'paused while blocked'
    : (b.stoppedForSeconds === null ? 'an unknown time'
       : DS.durationWords(b.stoppedForSeconds));

  var target = DS.parseErrorTarget(s.lastError);
  var errHtml = s.lastError
    ? esc(s.lastError).replace(target ? esc(target.full) : '\u0000',
        '<b class="ds-band-target">' + esc(target ? target.full : '') + '</b>')
    : 'No error message was recorded — the destination stopped accepting without saying why.';

  var actions = o.actions === false
    ? '<a class="btn btn-sm" href="/data-stream?stream=' + esc(s.id) + '">Open this stream →</a>'
    : '<a class="btn btn-sm" href="/data-stream-events'
        + DS.buildQuery({ stream: s.id, state: 'dead' }) + '">View dead letters</a>'
      + '<button class="btn btn-sm" onclick="dsBandRequeue(\'' + escArg(s.id) + '\')"'
        + (d.expired || !s.metrics.dlqDepth ? ' disabled' : '')
        + (d.expired ? ' title="The backlog has passed its retention window, so there is nothing left to requeue — a full resync is the only way back."' : '')
        + '>Requeue dead letters</button>'
      + '<button class="btn btn-sm" onclick="dsBandResync(\'' + escArg(s.id) + '\')">'
        + (d.expired ? 'Full resync required' : 'Resync') + '</button>'
      + '<button class="btn btn-sm" onclick="dsBandCopy(\'' + escArg(s.id) + '\')">Copy diagnostics</button>';

  return '<div class="ds-band ' + (hard ? 'hard' : 'soft') + '" role="region"'
    + ' aria-label="' + esc((hard ? 'Blocked stream: ' : 'Stream delivering with errors: ') + s.name) + '">'
    + '<div class="ds-band-head">'
      + '<span class="ds-band-tag">' + (hard ? 'Blocked' : 'Delivering with errors') + '</span>'
      + (list.length > 1
          ? '<span class="ds-band-count">' + (idx + 1) + ' of ' + list.length + ' affected streams</span>'
            + '<button class="ds-band-cycle" onclick="dsBandCycle(1)" aria-label="Show the next affected stream">Next →</button>'
          : '')
      + (o.dismissable === false ? ''
          : '<button class="ds-band-x" onclick="dsBandDismiss()" aria-label="Dismiss for this session">✕</button>')
    + '</div>'

    /* WHAT STOPPED */
    + '<div class="ds-band-what">' + esc(s.name) + ' has '
      + (hard ? 'delivered nothing for ' : 'been losing records for ') + esc(stopped) + '.</div>'

    /* WHY — verbatim, selectable, never paraphrased */
    + '<pre class="ds-band-err">' + errHtml + '</pre>'

    /* WHAT IS PILING UP */
    + '<div class="ds-band-line">'
      + esc(Number(s.metrics.pendingInStore || 0).toLocaleString()) + ' change'
      + ((s.metrics.pendingInStore || 0) === 1 ? ' is' : 's are') + ' queued and '
      + esc(Number(s.metrics.dlqDepth || 0).toLocaleString()) + ' ha'
      + ((s.metrics.dlqDepth || 0) === 1 ? 's' : 've') + ' been given up on.</div>'

    /* WHAT IT MEANS */
    + '<div class="ds-band-line means">' + esc(DS.blockedConsequence(state, s)) + '</div>'

    /* DEADLINE */
    + '<div class="ds-band-deadline ' + d.band + '">' + esc(d.line) + '</div>'

    + '<div class="ds-band-actions">' + actions + '</div>'
    + '</div>';
}

return {
  esc: esc, escArg: escArg, blockedBand: blockedBand,
  statusPill: statusPill, sideBadge: sideBadge, lagCell: lagCell,
  opBadge: opBadge, deliveryPill: deliveryPill,
  sparkline: sparkline, lineChart: lineChart, barChart: barChart,
  timeAgo: timeAgo, monoCopy: monoCopy, flowDiagram: flowDiagram,
  emptyState: emptyState, skeletonRows: skeletonRows, confirmText: confirmText,
  OP_TITLE: OP_TITLE,
};
});
