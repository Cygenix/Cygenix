/* ============================================================================
   cygenix-data-maps.js — four additional map models for the Schema Explorer
   ----------------------------------------------------------------------------
   Drop-in, no dependencies, no build step. Follows the same shape as the other
   cygenix-*.js modules: one global namespace, no side effects until install().

     <script src="cygenix-data-maps.js?v=1"></script>

   It registers four tabs on the existing .mode-bar (Estate map, Relationship
   map, Radial burst, Constellation) and renders them from the live schema
   returned by smTables(). If smTables() is unavailable it falls back to
   window.CYGENIX_SNAPSHOT when one has been loaded.

   Public API
     CygenixDataMaps.install()                  register the tabs (call on load)
     CygenixDataMaps.mount(el, data)            render into any element
     CygenixDataMaps.fromSchema(tables, edges)  adapt smTables() output to data
     CygenixDataMaps.refresh()                  re-read the schema and redraw

   Colour: six chromatic slots in a fixed, CVD-validated order carry the six
   business domains; System & audit and Other take neutrals. Every layout
   orders its groups by that same index, so on-screen neighbours are the pairs
   the palette was validated against. Colour follows the domain, never its
   current rank — filtering never repaints the survivors.

   Surfaces follow the console. The --cdm-* tokens below resolve against the
   app's own --bg2/--text/--border variables and only fall back to the light
   literals when the module is used outside it (the standalone prototype), so
   the maps track the theme instead of punching a white hole in a dark page.
   ========================================================================== */
window.CygenixDataMaps = (function () {
"use strict";

/* ---------- domain classification (name patterns, same vocabulary as the
   Schema map's domain view) ------------------------------------------------ */
var DOMAIN_NAMES = ['Finance & AR','Time & billing','Matters & clients','Documents & content',
                    'People & HR','Reference & lookup','System & audit','Other'];
function classify(name){
  var s = String(name).toLowerCase();
  if (/^(cv_|conv|_|zz)|audit|log$|logs|sync|temp|tmp|migration|job|queue|error|sysdiag|meta|stage|archive|backup|hist/.test(s)) return 6;
  if (/matter|client|engagement|party|contact|opportunit|intake|conflict|prospect/.test(s)) return 2;
  if (/time|wip|proforma|timekeeper|rate|narrative|charge|chrg|work|billsched/.test(s)) return 1;
  if (/^(ar|ap|gl)|invoice|bill|cash|payment|cost|ledger|post|tax|curr|budget|credit|debit|remit|disburse|fee|revenue|writeoff|trust/.test(s)) return 0;
  if (/doc|attach|file|image|template|note|letter|mail|msg|corresp/.test(s)) return 3;
  if (/employee|user|staff|person|people|hr$|resource|role|security|permission|group|team/.test(s)) return 4;
  if (/list$|code$|type$|status|lookup|ref$|country|language|currency|region|unit|calendar|format/.test(s)) return 5;
  return 7;
}
function familyKey(name){ return String(name).replace(/[_-]?\d+$/, '').replace(/\d+/g, '#'); }

/* Which table points at which. The console's graph carries the real answer on
   its edge list ({from, to} table keys), so when edges are handed over we use
   them. Without them all we have is each table's fkColumns — the names of the
   columns that take part in a foreign key, not the tables they reference — and
   the reference has to be guessed from the column name, which is how a
   standalone snapshot has to do it. Edges win whenever they are available:
   naming a target table is not the same as naming a column. */
function refPairs(tables, edges){
  var out = [];
  if (edges && edges.length) {
    var nameByKey = {};
    tables.forEach(function (t) { if (t.key) nameByKey[t.key] = t.name; });
    edges.forEach(function (e) {
      var a = nameByKey[e.from], b = nameByKey[e.to];
      if (a && b) out.push([a, b]);
    });
    if (out.length) return out;
  }
  tables.forEach(function (t) {
    (t.fkColumns || []).forEach(function (ref) { out.push([t.name, ref]); });
  });
  return out;
}

/* ---------- adapter: smTables() → the shape the maps draw from ------------- */
function fromSchema(tables, edges){
  var groups = {}, domains = DOMAIN_NAMES.map(function (n) {
    return { name:n, tables:0, rows:0, empty:0 };
  });
  var totalRows = 0, totalEmpty = 0;

  tables.forEach(function (t) {
    var rows = t.rowCount || 0, d = classify(t.name), k = familyKey(t.name);
    var g = groups[k] || (groups[k] = { name:k, rows:0, n:0, cols:0, d:d, view:t.kind === 'view' });
    g.rows += rows; g.n++; g.cols = Math.max(g.cols, (t.columns || []).length);
    domains[d].tables++; domains[d].rows += rows;
    if (!rows) { domains[d].empty++; totalEmpty++; }
    totalRows += rows;
  });

  var all = Object.keys(groups).map(function (k) { return groups[k]; });
  var families = all.filter(function (g) { return g.rows > 0; })
                    .sort(function (a, b) { return b.rows - a.rows; })
                    .slice(0, MAX_FAMILIES);
  families.forEach(function (f, i) { f.i = i; });

  var idx = {};
  families.forEach(function (f) { if (!(f.name in idx)) idx[f.name] = f.i; });

  var matrix = domains.map(function () { return domains.map(function () { return 0; }); });
  var pairs = {};
  refPairs(tables, edges).forEach(function (p) {
    var a = classify(p[0]), b = classify(p[1]);
    matrix[a][b]++;
    var fa = idx[familyKey(p[0])], fb = idx[familyKey(p[1])];
    if (fa !== undefined && fb !== undefined && fa !== fb) {
      var key = fa + '-' + fb; pairs[key] = (pairs[key] || 0) + 1;
    }
  });
  var links = Object.keys(pairs).map(function (k) {
    var ij = k.split('-'); return { s:+ij[0], t:+ij[1], w:pairs[k] };
  });

  domains.forEach(function (d, i) {
    var lr = 0, lt = 0;
    families.forEach(function (f) { if (f.d === i) { lr += f.rows; lt += f.n; } });
    d.listedRows = lr; d.listedTables = lt;
    d.tailRows = Math.max(0, d.rows - lr);
    d.tailTables = Math.max(0, d.tables - lt);
  });

  return {
    domains: domains,
    totals: { tables:tables.length, rows:totalRows, empty:totalEmpty, families:all.length, listed:families.length },
    families: families, matrix: matrix, links: links
  };
}

/* How many table families each map draws. Raising this is safe — the layouts
   scale — but past a few hundred the marks stop being individually readable. */
var MAX_FAMILIES = 260;

/* Six chromatic slots in the validated fixed order, then two neutrals for the
   residual domains. Slot order is the CVD-safety mechanism, not decoration. */
var COL  = ['#2a78d6','#eb6834','#1baf7a','#eda100','#e87ba4','#008300','#8A9099','#C2C7CD'];

/* SVG cannot read a CSS variable through a presentation attribute, so the two
   colours the drawing code needs — the surface a label halo is knocked out of,
   and the ink a selected mark is ringed in — are resolved from the live tokens
   once per render. On a dark console a hardcoded white halo would draw a white
   fringe round every label. */
var SURF = '#FFFFFF', INK = '#1A1D21';
function resolveSurface(){
  if (!root || typeof window.getComputedStyle !== 'function') return;
  var cs = window.getComputedStyle(root);
  var s = (cs.getPropertyValue('--cdm-surface') || '').trim();
  var i = (cs.getPropertyValue('--cdm-text') || '').trim();
  if (s) SURF = s;
  if (i) INK = i;
}

var S, D, F, T, ui = {}, root = null;

/* ---------- formatting --------------------------------------------------- */
function fmt(n){
  if (n >= 1e9) return (n/1e9).toFixed(n>=1e10?0:1).replace(/\.0$/,'') + 'bn';
  if (n >= 1e6) return (n/1e6).toFixed(n>=1e7?0:1).replace(/\.0$/,'') + 'm';
  if (n >= 1e3) return (n/1e3).toFixed(n>=1e4?0:1).replace(/\.0$/,'') + 'k';
  return String(n);
}
function comma(n){ return n.toLocaleString('en-GB'); }
function pct(a,b){ return b ? (a/b*100).toFixed(a/b<0.01?2:1) + '%' : '0%'; }

/* ---------- state -------------------------------------------------------- */
var TABS = [
  { id:'estate',  label:'◍ Estate map',
    title:'Estate map — every table family nested in its domain',
    sub:'Area is proportional to the metric. One circle per table family; families that differ only by a trailing number are packed as a single cell.',
    note:function () {
      var top = D.slice().sort(function (a, b) { return b.rows - a.rows; })[0];
      var f = F[0];
      if (!top || !f) return '';
      return 'Reads like a population cartogram: the estate is not evenly distributed. '
        + top.name + ' holds ' + pct(top.rows, T.rows) + ' of every row in the database, and one family — '
        + f.name + ', ' + comma(f.n) + ' near-identical table' + (f.n === 1 ? '' : 's')
        + ' — carries ' + pct(f.rows, T.rows) + ' of the whole estate on its own.';
    } },
  { id:'flow',    label:'⇄ Relationship map',
    title:'Relationship map — where the foreign keys actually point',
    sub:function () {
      return 'Chord: FK references between domains, counted across all ' + comma(T.tables)
        + ' tables. Flow: how the estate splits by volume band and move plan.';
    },
    note:'Ribbon width is the number of FK references, not rows. Self-referencing ribbons stay inside their own arc — a domain that mostly points at itself is a domain you can lift in one piece.' },
  { id:'burst',   label:'✳ Radial burst',
    title:'Radial burst — one spoke per table family',
    sub:'Spoke length is log-scaled, so four orders of magnitude fit in one ring. Sectors are domains, ordered clockwise.',
    note:'The long tail is the point: most spokes are short. A log scale is used because a linear one would render everything except the top ten as a flat rim.' },
  { id:'constellation', label:'◎ Constellation',
    title:'Constellation — orbital rings by order of magnitude',
    sub:'Each ring is a decade of row count; the heaviest family sits at the centre. Bubble area is proportional to rows.',
    note:'Rings answer the question the estate map cannot: how many objects sit at each order of magnitude, and which domains own them.' }
];
var st = {
  tab:'estate',
  doms:new Set([0,1,2,3,4,5,6,7]),
  metric:'rows',
  view:'map',
  mode:'chord',
  q:'',
  sel:null
};
function el(tag, attrs, kids){
  var SVGTAGS = { svg:1, g:1, circle:1, path:1, text:1, line:1, rect:1, defs:1, radialGradient:1, stop:1, tspan:1 };
  var n = SVGTAGS[tag]
        ? document.createElementNS('http://www.w3.org/2000/svg', tag)
        : document.createElement(tag);
  for (var k in attrs) if (attrs[k] !== null && attrs[k] !== undefined) {
    if (k === 'text') n.textContent = attrs[k];
    else if (k === 'html') n.innerHTML = attrs[k];
    else if (k.slice(0,2) === 'on') n.addEventListener(k.slice(2), attrs[k]);
    else n.setAttribute(k, attrs[k]);
  }
  (kids||[]).forEach(function(c){ if (c) n.appendChild(c); });
  return n;
}

/* ---------- data selectors ---------------------------------------------- */
function val(f){ return st.metric === 'rows' ? f.rows : f.n; }
function domVal(i){ return st.metric === 'rows' ? D[i].rows : D[i].tables; }
function domListedVal(i){ return st.metric === 'rows' ? D[i].listedRows : D[i].listedTables; }
function domTailVal(i){ return st.metric === 'rows' ? D[i].tailRows : D[i].tailTables; }
function activeDoms(){ return [0,1,2,3,4,5,6,7].filter(function(i){ return st.doms.has(i); }); }
function matches(f){ return !st.q || f.name.toLowerCase().indexOf(st.q) >= 0; }
function fams(){ return F.filter(function(f){ return st.doms.has(f.d) && matches(f); }); }

/* ---------- tooltip ------------------------------------------------------ */
var tip = null, tipOn = false;
function showTip(html, ev){
  tip.innerHTML = html; tip.style.opacity = 1; tipOn = true; moveTip(ev);
}
function moveTip(ev){
  if (!tipOn) return;
  var pad = 14, w = tip.offsetWidth, h = tip.offsetHeight;
  var x = ev.clientX + pad, y = ev.clientY + pad;
  if (x + w > window.innerWidth - 8) x = ev.clientX - w - pad;
  if (y + h > window.innerHeight - 8) y = ev.clientY - h - pad;
  tip.style.left = x + 'px'; tip.style.top = y + 'px';
}
function hideTip(){ tip.style.opacity = 0; tipOn = false; }
window.addEventListener('mousemove', moveTip, { passive:true });
function famTip(f){
  return '<div class="cdm-t">' + f.name + (f.n>1 ? ' <span style="opacity:.7">×' + f.n + '</span>' : '') + '</div>'
    + '<div class="cdm-r"><span>Rows</span><b>' + comma(f.rows) + '</b></div>'
    + '<div class="cdm-r"><span>Tables</span><b>' + comma(f.n) + '</b></div>'
    + '<div class="cdm-r"><span>Domain</span><b>' + D[f.d].name + '</b></div>'
    + '<div class="cdm-r"><span>Share of estate</span><b>' + pct(f.rows, T.rows) + '</b></div>';
}

/* ---------- selection rail ---------------------------------------------- */
function select(kind, obj){ st.sel = { kind:kind, obj:obj }; renderDetail(); }
function renderDetail(){
  var box = ui.detail; box.innerHTML = '';
  if (!st.sel) {
    box.appendChild(el('p', { class:'cdm-hint', text:'Hover any mark for detail; click one to pin it here.' }));
    return;
  }
  var rows = [], head;
  if (st.sel.kind === 'family') {
    var f = st.sel.obj; head = f.name;
    rows = [['Rows', comma(f.rows)], ['Tables in family', comma(f.n)],
            ['Domain', D[f.d].name], ['Share of estate', pct(f.rows, T.rows)],
            ['Object type', f.view ? 'View' : 'Table']];
  } else {
    var d = D[st.sel.obj]; head = d.name;
    rows = [['Rows', comma(d.rows)], ['Tables', comma(d.tables)],
            ['Empty tables', comma(d.empty)], ['Share of rows', pct(d.rows, T.rows)],
            ['FK refs out', comma(S.matrix[st.sel.obj].reduce(function(a,b){return a+b;},0))]];
  }
  box.appendChild(el('div', { class:'cdm-nm', text:head }));
  var dl = el('dl');
  rows.forEach(function(r){ dl.appendChild(el('dt',{text:r[0]})); dl.appendChild(el('dd',{text:r[1]})); });
  box.appendChild(dl);
}

/* =======================================================================
   Layout helpers — no charting library; the packing is a tiny deterministic
   relaxation so the file stays self-contained and drops straight into the app.
   ======================================================================= */

/* Golden-angle seeding + collision relaxation. Deterministic: no Math.random,
   so the same estate always renders the same map. */
function relax(nodes, cx, cy, bound, iter, pull, pad, seed){
  var GA = Math.PI * (3 - Math.sqrt(5));
  nodes.forEach(function (n, i) {
    if (n.x === undefined) {
      var a = i * GA, rr = (seed || bound || 1) * 0.62 * Math.sqrt((i + 0.5) / nodes.length);
      n.x = cx + Math.cos(a) * rr; n.y = cy + Math.sin(a) * rr;
    }
  });
  /* Two phases: pull-and-separate, then a pure separation settle. Without the
     settle, the centring pull holds a permanent residual overlap — small at
     first glance, but on a lopsided estate it swallows the smaller groups. */
  var total = iter + Math.round(iter * 0.35);
  for (var k = 0; k < total; k++) {
    var strength = k < iter ? pull * (1 - k / iter * 0.4) : 0;
    for (var i = 0; i < nodes.length; i++) {
      var a = nodes[i];
      a.x += (cx - a.x) * strength; a.y += (cy - a.y) * strength;
      for (var j = i + 1; j < nodes.length; j++) {
        var b = nodes[j], dx = b.x - a.x, dy = b.y - a.y;
        var d2 = dx*dx + dy*dy, need = a.r + b.r + (pad || 2);   /* surface gap */
        if (d2 === 0) {                     /* coincident: nudge deterministically */
          var ang = (i * 2.399963 + j * 0.7);
          dx = Math.cos(ang) * 0.5; dy = Math.sin(ang) * 0.5;
          b.x += dx; b.y += dy; d2 = 0.25;
        }
        if (d2 < need*need) {
          var d = Math.sqrt(d2) || 0.01, push = (need - d) / d * 0.62;
          dx *= push; dy *= push;
          a.x -= dx; a.y -= dy; b.x += dx; b.y += dy;
        }
      }
      /* keep inside the parent circle */
      if (bound) {
        var ox = a.x - cx, oy = a.y - cy, od = Math.sqrt(ox*ox + oy*oy);
        var max = bound - a.r - 1;
        if (od > max && od > 0) { a.x = cx + ox / od * max; a.y = cy + oy / od * max; }
      }
    }
  }
  return nodes;
}

/* Greedy label placer: a label is only drawn if its box is clear, so nothing
   ever overlaps and nothing is ever clipped. Values the placer drops stay
   reachable through hover, the selection rail and the table view. */
function Labeller(){ this.taken = []; }
Labeller.prototype.fits = function (x, y, w, h) {
  for (var i = 0; i < this.taken.length; i++) {
    var t = this.taken[i];
    if (Math.abs(x - t.x) * 2 < (w + t.w) && Math.abs(y - t.y) * 2 < (h + t.h)) return false;
  }
  return true;
};
Labeller.prototype.claim = function (x, y, w, h) {
  if (!this.fits(x, y, w, h)) return false;
  this.taken.push({ x:x, y:y, w:w, h:h });
  return true;
};
function halo(attrs){
  attrs.stroke = SURF; attrs['stroke-width'] = 3.5;
  attrs['paint-order'] = 'stroke'; attrs['stroke-linejoin'] = 'round';
  attrs['pointer-events'] = 'none';
  return el('text', attrs);
}
function textW(str, size){ return str.length * size * 0.56; }

function svg(w, h, label){
  return el('svg', { viewBox:'0 0 ' + w + ' ' + h, role:'img', 'aria-label':label,
                     style:'max-height:' + h + 'px' });
}

/* =======================================================================
   MAP 1 — Estate map (nested circle pack)
   ======================================================================= */
function drawEstate(){
  var W = 900, H = 640, list = activeDoms();
  var g = svg(W, H, 'Estate map: table families packed inside their domain, area proportional to ' + st.metric);
  if (!list.length) return g;

  /* 1. pack each domain's children in local coordinates, then let the domain
        circle hug what it actually contains */
  var totalV = list.reduce(function (a, i) { return a + domVal(i); }, 0);
  var area = W * H * 0.40;
  var groups = list.map(function (i) {
    var v = Math.max(domVal(i), totalV * 0.005);
    var R = Math.sqrt(v / totalV * area / Math.PI);
    var kids = F.filter(function (f) { return f.d === i && matches(f); })
                .map(function (f) { return { f:f, v:Math.max(val(f), 1) }; });
    var tailV = domTailVal(i);
    if (tailV > 0 && !st.q) kids.push({ tail:true, v:tailV });
    var sum = kids.reduce(function (a, k) { return a + k.v; }, 0) || 1;
    var inner = Math.PI * R * R * 0.74;
    kids.forEach(function (k) { k.r = Math.max(1.5, Math.sqrt(k.v / sum * inner / Math.PI)); });
    kids.sort(function (a, b) { return b.r - a.r; });
    relax(kids, 0, 0, R, kids.length > 60 ? 240 : 340, 0.075);
    var hull = 0;
    kids.forEach(function (k) { hull = Math.max(hull, Math.sqrt(k.x*k.x + k.y*k.y) + k.r); });
    return { i:i, v:v, kids:kids, r:Math.max(hull + 5, 12) };
  });

  /* 2. lay the domain circles out, then fit the result to the canvas */
  groups.sort(function (a, b) { return b.r - a.r; });
  relax(groups, W/2, H/2, 0, 520, 0.04, 14, 460);
  var x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
  groups.forEach(function (G) {
    x0 = Math.min(x0, G.x - G.r); x1 = Math.max(x1, G.x + G.r);
    y0 = Math.min(y0, G.y - G.r); y1 = Math.max(y1, G.y + G.r);
  });
  var padL = 26, padT = 40, padB = 30;
  var k = Math.min((W - padL*2) / (x1 - x0), (H - padT - padB) / (y1 - y0));
  var ox = padL + ((W - padL*2) - (x1 - x0) * k) / 2 - x0 * k;
  var oy = padT + ((H - padT - padB) - (y1 - y0) * k) / 2 - y0 * k;
  var root = el('g', { transform:'translate(' + ox + ',' + oy + ') scale(' + k + ')' });
  g.appendChild(root);

  var labels = [];
  groups.sort(function (a, b) { return a.i - b.i; });
  groups.forEach(function (G) {
    var d = D[G.i], colour = COL[G.i];
    var grp = el('g', { transform:'translate(' + G.x + ',' + G.y + ')' });
    grp.appendChild(el('circle', { cx:0, cy:0, r:G.r, fill:colour, 'fill-opacity':0.09 }));

    G.kids.forEach(function (kd) {
      var isSel = st.sel && st.sel.kind === 'family' && kd.f && st.sel.obj === kd.f;
      var c = el('circle', {
        class:'cdm-node', cx:kd.x, cy:kd.y, r:kd.r,
        fill:colour, 'fill-opacity':kd.tail ? 0.34 : 0.92,
        stroke:isSel ? INK : SURF, 'stroke-width':(isSel ? 2 : (kd.r > 3 ? 1.6 : 0)) / k,
        tabindex:kd.r * k > 7 ? 0 : null,
        'aria-label':kd.tail ? d.name + ' long tail' : (kd.f.name + ', ' + comma(kd.f.rows) + ' rows')
      });
      var html = kd.tail
        ? '<div class="cdm-t">' + d.name + ' — long tail</div>'
          + '<div class="cdm-r"><span>Tables</span><b>' + comma(d.tables - d.listedTables) + '</b></div>'
          + '<div class="cdm-r"><span>Rows</span><b>' + comma(d.tailRows) + '</b></div>'
          + '<div class="cdm-r"><span>of which empty</span><b>' + comma(d.empty) + '</b></div>'
        : famTip(kd.f);
      c.addEventListener('mouseenter', function (ev) { showTip(html, ev); });
      c.addEventListener('mouseleave', hideTip);
      c.addEventListener('focus', function () { if (kd.f) select('family', kd.f); });
      c.addEventListener('click', function () { kd.f ? select('family', kd.f) : select('domain', G.i); render(); });
      grp.appendChild(c);

      /* direct label only where it genuinely fits after the fit-to-canvas scale */
      if (kd.f && kd.r * k > 26) {
        var fs = Math.min(13, kd.r * k / 3.4);
        var maxc = Math.floor(kd.r * k * 1.7 / fs);
        var nm = kd.f.name.length > maxc ? kd.f.name.slice(0, Math.max(3, maxc - 1)) + '…' : kd.f.name;
        grp.appendChild(el('text', { x:kd.x, y:kd.y - 1/k, 'text-anchor':'middle', 'font-size':fs / k,
          fill:'#fff', 'font-weight':600, 'pointer-events':'none', text:nm }));
        if (kd.r * k > 38) grp.appendChild(el('text', { x:kd.x, y:kd.y + (fs + 3) / k, 'text-anchor':'middle',
          'font-size':Math.min(11, kd.r * k / 4.6) / k, fill:'#fff', 'fill-opacity':0.85,
          'pointer-events':'none', text:fmt(st.metric === 'rows' ? kd.f.rows : kd.f.n) }));
      }
    });
    root.appendChild(grp);

    labels.push({ x:ox + G.x * k, y:oy + (G.y - G.r) * k - 8, i:G.i, r:G.r * k });
  });

  /* 3. group labels last, on top, each with a surface halo so it stays legible
        wherever it lands; nudged apart when two groups sit close together */
  labels.sort(function (a, b) { return a.y - b.y; });
  labels.forEach(function (L, n) {
    for (var m = 0; m < n; m++) {
      var P = labels[m];
      if (Math.abs(P.x - L.x) < 120 && Math.abs(P.y - L.y) < 26) L.y = P.y + 26;
    }
    if (L.y < 14) L.y = 14;
    var t1 = el('text', { class:'cdm-glabel', x:L.x, y:L.y, 'text-anchor':'middle', text:D[L.i].name,
      stroke:SURF, 'stroke-width':3.5, 'paint-order':'stroke', 'stroke-linejoin':'round', 'pointer-events':'none' });
    var t2 = el('text', { class:'cdm-gsub', x:L.x, y:L.y + 12, 'text-anchor':'middle',
      text:fmt(domVal(L.i)) + ' ' + (st.metric === 'rows' ? 'rows' : 'tables') + ' · ' + pct(domVal(L.i), st.metric === 'rows' ? T.rows : T.tables),
      stroke:SURF, 'stroke-width':3.5, 'paint-order':'stroke', 'stroke-linejoin':'round', 'pointer-events':'none' });
    g.appendChild(t1); g.appendChild(t2);
  });
  return g;
}

/* =======================================================================
   MAP 2 — Relationship map: chord (FK traffic) and flow (volume → plan)
   ======================================================================= */
function polar(cx, cy, r, a){ return [cx + Math.cos(a) * r, cy + Math.sin(a) * r]; }
function arcPath(cx, cy, r, a0, a1){
  var p0 = polar(cx, cy, r, a0), p1 = polar(cx, cy, r, a1);
  return 'A' + r + ',' + r + ' 0 ' + (a1 - a0 > Math.PI ? 1 : 0) + ' 1 ' + p1[0] + ',' + p1[1];
}
function drawChord(){
  var W = 900, H = 580, cx = W/2, cy = H/2, R = 186, TH = 14, lab = new Labeller();
  var g = svg(W, H, 'Chord diagram of foreign-key references between domains');
  var list = activeDoms(), M = S.matrix;
  var sums = list.map(function (i) {
    return list.reduce(function (a, j) { return a + M[i][j]; }, 0);
  });
  var total = sums.reduce(function (a, b) { return a + b; }, 0);
  if (!total) return g;

  var pad = 0.028, span = Math.PI * 2 - pad * list.length, a = -Math.PI/2 + pad/2;
  var arcs = {}, segs = {};
  list.forEach(function (i, k) {
    var w = sums[k] / total * span;
    arcs[i] = { a0:a, a1:a + w };
    var s = a;
    segs[i] = {};
    list.forEach(function (j) {
      var sw = M[i][j] / total * span;
      segs[i][j] = { a0:s, a1:s + sw }; s += sw;
    });
    a += w + pad;
  });

  /* ribbons first, arcs on top */
  var ribbons = [];
  list.forEach(function (i) { list.forEach(function (j) {
    if (M[i][j] === 0 && M[j][i] === 0) return;
    if (j < i) return;
    ribbons.push({ i:i, j:j, w:M[i][j] + M[j][i] });
  }); });
  ribbons.sort(function (p, q) { return q.w - p.w; });

  ribbons.forEach(function (rb) {
    var s = segs[rb.i][rb.j], t = segs[rb.j][rb.i], r = R - TH - 2;
    var p0 = polar(cx, cy, r, s.a0), p1 = polar(cx, cy, r, t.a0);
    var d = 'M' + p0[0] + ',' + p0[1] + arcPath(cx, cy, r, s.a0, s.a1)
          + 'Q' + cx + ',' + cy + ' ' + p1[0] + ',' + p1[1]
          + arcPath(cx, cy, r, t.a0, t.a1)
          + 'Q' + cx + ',' + cy + ' ' + p0[0] + ',' + p0[1] + 'Z';
    var owner = M[rb.i][rb.j] >= M[rb.j][rb.i] ? rb.i : rb.j;
    var path = el('path', { class:'cdm-node', d:d, fill:COL[owner], 'fill-opacity':rb.i === rb.j ? 0.34 : 0.44,
      tabindex:0, 'aria-label':D[rb.i].name + ' and ' + D[rb.j].name + ', ' + rb.w + ' foreign-key references' });
    var html = '<div class="cdm-t">' + D[rb.i].name + ' ⇄ ' + D[rb.j].name + '</div>'
      + '<div class="cdm-r"><span>' + D[rb.i].name + ' → ' + D[rb.j].name + '</span><b>' + comma(M[rb.i][rb.j]) + '</b></div>'
      + '<div class="cdm-r"><span>' + D[rb.j].name + ' → ' + D[rb.i].name + '</span><b>' + comma(M[rb.j][rb.i]) + '</b></div>';
    path.addEventListener('mouseenter', function (ev) { path.setAttribute('fill-opacity', 0.75); showTip(html, ev); });
    path.addEventListener('mouseleave', function () { path.setAttribute('fill-opacity', rb.i === rb.j ? 0.34 : 0.44); hideTip(); });
    path.addEventListener('click', function () { select('domain', rb.i); });
    g.appendChild(path);
  });

  list.forEach(function (i, k) {
    /* A domain with no foreign keys has a zero-width arc. Drawing it is a
       no-op, but labelling it is not: the label has nowhere of its own to sit,
       so it stacks with every other empty domain at the top of the ring and
       the placer pushes the pile up out of the plot. The legend and the table
       view still carry the domain and its nil count. */
    if (!sums[k]) return;
    var A = arcs[i], p0 = polar(cx, cy, R, A.a0), p1 = polar(cx, cy, R - TH, A.a1);
    var d = 'M' + p0[0] + ',' + p0[1] + arcPath(cx, cy, R, A.a0, A.a1)
          + 'L' + p1[0] + ',' + p1[1]
          + 'A' + (R-TH) + ',' + (R-TH) + ' 0 ' + (A.a1 - A.a0 > Math.PI ? 1 : 0) + ' 0 '
          + polar(cx, cy, R - TH, A.a0).join(',') + 'Z';
    var arc = el('path', { class:'cdm-node', d:d, fill:COL[i], stroke:SURF, 'stroke-width':2, tabindex:0,
      'aria-label':D[i].name + ', ' + sums[k] + ' outgoing foreign-key references' });
    var html = '<div class="cdm-t">' + D[i].name + '</div>'
      + '<div class="cdm-r"><span>FK refs out</span><b>' + comma(sums[k]) + '</b></div>'
      + '<div class="cdm-r"><span>Self-referencing</span><b>' + comma(M[i][i]) + ' (' + pct(M[i][i], sums[k]) + ')</b></div>'
      + '<div class="cdm-r"><span>Tables</span><b>' + comma(D[i].tables) + '</b></div>';
    arc.addEventListener('mouseenter', function (ev) { showTip(html, ev); });
    arc.addEventListener('mouseleave', hideTip);
    arc.addEventListener('click', function () { select('domain', i); });
    g.appendChild(arc);

    var mid = (A.a0 + A.a1) / 2, lp = polar(cx, cy, R + 12, mid);
    var ca = Math.cos(mid);
    var anchor = ca > 0.25 ? 'start' : (ca < -0.25 ? 'end' : 'middle');
    var dy = Math.sin(mid) < 0 ? -6 : 12;
    var ly = lp[1] + (anchor === 'middle' ? dy : 0);
    for (var guard = 0; guard < 24 && !lab.fits(lp[0], ly + 6, 150, 30); guard++) ly += Math.sin(mid) < 0 ? -7 : 7;
    ly = Math.max(14, Math.min(H - 20, ly));   /* never nudged off the canvas */
    lab.claim(lp[0], ly + 6, 150, 30);
    g.appendChild(halo({ class:'cdm-glabel', x:lp[0], y:ly,
      'text-anchor':anchor, 'dominant-baseline':'middle', text:D[i].name }));
    g.appendChild(halo({ class:'cdm-gsub', x:lp[0], y:ly + 13,
      'text-anchor':anchor, 'dominant-baseline':'middle', text:comma(sums[k]) + ' refs' }));
  });

  g.appendChild(el('text', { class:'cdm-gsub', x:cx, y:cy - 6, 'text-anchor':'middle', text:comma(total) }));
  g.appendChild(el('text', { class:'cdm-gsub', x:cx, y:cy + 8, 'text-anchor':'middle', text:'FK references' }));
  return g;
}

var BANDS = [
  { k:'≥ 1m rows',        lo:1e6,  hi:Infinity, plan:0 },
  { k:'100k – 1m rows',   lo:1e5,  hi:1e6,      plan:0 },
  { k:'10k – 100k rows',  lo:1e4,  hi:1e5,      plan:1 },
  { k:'1k – 10k rows',    lo:1e3,  hi:1e4,      plan:1 },
  { k:'< 1k rows / empty',lo:-1,   hi:1e3,      plan:2 }
];
var PLANS = ['Core load — run window', 'Bulk tail — batch load', 'Confirm out of scope'];

function drawFlow(){
  var W = 900, H = 620, g = svg(W, H, 'Flow of the estate from domain to volume band to move plan');
  var list = activeDoms();
  var ab = {}, bc = {}, aTot = {}, bTot = {}, cTot = {};
  list.forEach(function (i) { aTot[i] = 0; });
  BANDS.forEach(function (b, bi) { bTot[bi] = 0; });
  PLANS.forEach(function (p, pi) { cTot[pi] = 0; });

  function add(i, bi, v){
    if (!v) return;
    ab[i + ':' + bi] = (ab[i + ':' + bi] || 0) + v;
    var pi = BANDS[bi].plan;
    bc[bi + ':' + pi] = (bc[bi + ':' + pi] || 0) + v;
    aTot[i] += v; bTot[bi] += v; cTot[pi] += v;
  }
  F.forEach(function (f) {
    if (!st.doms.has(f.d) || !matches(f)) return;
    var bi = 0; while (bi < BANDS.length - 1 && f.rows < BANDS[bi].lo) bi++;
    add(f.d, bi, val(f));
  });
  if (!st.q) list.forEach(function (i) { add(i, 4, domTailVal(i)); });

  var grand = list.reduce(function (a, i) { return a + aTot[i]; }, 0);
  if (!grand) return g;

  var top = 34, bot = H - 26, NW = 13;
  function column(keys, totals, x, colour){
    var vals = keys.filter(function (k) { return totals[k] > 0; });
    var gap = 12, avail = (bot - top) - gap * (vals.length - 1);
    var y = top, out = {};
    vals.forEach(function (k) {
      var h = Math.max(3, totals[k] / grand * avail);
      out[k] = { y:y, h:h, x:x, cursor:y };
      y += h + gap;
    });
    return out;
  }
  var A = column(list, aTot, 24);
  var B = column(BANDS.map(function (_, i) { return i; }), bTot, 430);
  var C = column(PLANS.map(function (_, i) { return i; }), cTot, 700);

  function ribbon(sx, sy, sh, tx, ty, th, fill, op, label, html){
    var mx = (sx + tx) / 2;
    var d = 'M' + sx + ',' + sy + 'C' + mx + ',' + sy + ' ' + mx + ',' + ty + ' ' + tx + ',' + ty
          + 'L' + tx + ',' + (ty + th) + 'C' + mx + ',' + (ty + th) + ' ' + mx + ',' + (sy + sh) + ' ' + sx + ',' + (sy + sh) + 'Z';
    var p = el('path', { class:'cdm-node', d:d, fill:fill, 'fill-opacity':op, tabindex:0, 'aria-label':label });
    p.addEventListener('mouseenter', function (ev) { p.setAttribute('fill-opacity', Math.min(0.9, op + 0.3)); showTip(html, ev); });
    p.addEventListener('mouseleave', function () { p.setAttribute('fill-opacity', op); hideTip(); });
    return p;
  }
  var unit = st.metric === 'rows' ? 'rows' : 'tables';

  list.forEach(function (i) {
    BANDS.forEach(function (b, bi) {
      var v = ab[i + ':' + bi]; if (!v || !A[i] || !B[bi]) return;
      var sh = v / aTot[i] * A[i].h, th = v / bTot[bi] * B[bi].h;
      g.appendChild(ribbon(A[i].x + NW, A[i].cursor, sh, B[bi].x, B[bi].cursor, th, COL[i], 0.42,
        D[i].name + ' to ' + b.k,
        '<div class="cdm-t">' + D[i].name + ' → ' + b.k + '</div><div class="cdm-r"><span>' + unit + '</span><b>' + comma(v) + '</b></div>'));
      A[i].cursor += sh; B[bi].cursor += th;
    });
  });
  BANDS.forEach(function (b, bi) {
    var pi = b.plan, v = bc[bi + ':' + pi];
    if (!v || !B[bi] || !C[pi]) return;
    var sh = B[bi].h * (v / bTot[bi]), th = v / cTot[pi] * C[pi].h;
    g.appendChild(ribbon(B[bi].x + NW, B[bi].y + (B[bi].cursor2 || 0), sh, C[pi].x, C[pi].cursor, th, '#B6BBC2', 0.5,
      b.k + ' to ' + PLANS[pi],
      '<div class="cdm-t">' + b.k + ' → ' + PLANS[pi] + '</div><div class="cdm-r"><span>' + unit + '</span><b>' + comma(v) + '</b></div>'));
    B[bi].cursor2 = (B[bi].cursor2 || 0) + sh; C[pi].cursor += th;
  });

  var lab = new Labeller();
  function node(box, fill, label, sub, anchor){
    if (!box) return;
    g.appendChild(el('rect', { x:box.x, y:box.y, width:NW, height:box.h, rx:3, fill:fill }));
    var lx = box.x + NW + 8, ly = box.y + 11;
    /* nudge down until the label box is clear — small nodes crowd together */
    for (var guard = 0; guard < 40 && !lab.fits(lx + 60, ly + 6, 150, 28); guard++) ly += 6;
    lab.claim(lx + 60, ly + 6, 150, 28);
    g.appendChild(halo({ class:'cdm-glabel', x:lx, y:ly, 'text-anchor':'start', text:label }));
    g.appendChild(halo({ class:'cdm-gsub', x:lx, y:ly + 13, 'text-anchor':'start', text:sub }));
  }
  list.forEach(function (i) { node(A[i], COL[i], D[i].name, fmt(aTot[i]) + ' ' + unit, 'start'); });
  BANDS.forEach(function (b, bi) { node(B[bi], '#8A9099', b.k, fmt(bTot[bi]) + ' ' + unit, 'start'); });
  PLANS.forEach(function (p, pi) { node(C[pi], '#5D636B', p, fmt(cTot[pi]) + ' ' + unit + ' · ' + pct(cTot[pi], grand), 'start'); });
  return g;
}

/* =======================================================================
   MAP 3 — Radial burst: one spoke per family, sectors are domains
   ======================================================================= */
function drawBurst(){
  var W = 900, H = 660, cx = W/2, cy = H/2, r0 = 54, r1 = 216, lab = new Labeller();
  var g = svg(W, H, 'Radial burst: one spoke per table family, length log-scaled by ' + st.metric);
  var list = activeDoms();
  var rows = list.map(function (i) {
    return { i:i, items:F.filter(function (f) { return f.d === i && matches(f); })
                        .sort(function (a, b) { return b.rows - a.rows; }) };
  }).filter(function (r) { return r.items.length; });
  var n = rows.reduce(function (a, r) { return a + r.items.length; }, 0);
  if (!n) return g;

  var maxV = Math.max.apply(null, F.map(val));
  function len(v){
    var t = Math.log10(Math.max(v, 1)) / Math.log10(Math.max(maxV, 10));
    return r0 + t * (r1 - r0);
  }
  var gap = 0.055, span = Math.PI*2 - gap*rows.length, a = -Math.PI/2 + gap/2;

  /* centre fade — the poster's soft core, drawn once */
  var defs = el('defs');
  var rg = el('radialGradient', { id:'core' });
  rg.appendChild(el('stop', { offset:'0%',  'stop-color':SURF, 'stop-opacity':1 }));
  rg.appendChild(el('stop', { offset:'62%', 'stop-color':SURF, 'stop-opacity':0.92 }));
  rg.appendChild(el('stop', { offset:'100%','stop-color':SURF, 'stop-opacity':0 }));
  defs.appendChild(rg); g.appendChild(defs);

  rows.forEach(function (row) {
    var w = row.items.length / n * span, a0 = a, a1 = a + w;
    var grp = el('g');
    row.items.forEach(function (f, k) {
      var ang = a0 + (k + 0.5) / row.items.length * w;
      var L = len(val(f)), p0 = polar(cx, cy, r0 - 6, ang), p1 = polar(cx, cy, L, ang);
      var isSel = st.sel && st.sel.kind === 'family' && st.sel.obj === f;
      var line = el('line', { class:'cdm-node', x1:p0[0], y1:p0[1], x2:p1[0], y2:p1[1],
        stroke:isSel ? INK : COL[row.i], 'stroke-width':isSel ? 3.2 : 2, 'stroke-linecap':'round',
        tabindex:0, 'aria-label':f.name + ', ' + comma(f.rows) + ' rows, ' + D[f.d].name });
      var html = famTip(f);
      line.addEventListener('mouseenter', function (ev) { line.setAttribute('stroke-width', 4); showTip(html, ev); });
      line.addEventListener('mouseleave', function () { line.setAttribute('stroke-width', isSel ? 3.2 : 2); hideTip(); });
      line.addEventListener('click', function () { select('family', f); render(); });
      line.addEventListener('focus', function () { select('family', f); });
      grp.appendChild(line);

      /* label selectively — the extreme of each sector, and only when clear */
      if (k === 0) {
        var lp = polar(cx, cy, L + 6, ang), flip2 = Math.cos(ang) < 0;
        var nm = f.name.length > 22 ? f.name.slice(0, 21) + '…' : f.name;
        if (lab.claim(lp[0] + (flip2 ? -1 : 1) * textW(nm, 10.5)/2, lp[1], textW(nm, 10.5) + 8, 14)) {
          grp.appendChild(halo({ class:'cdm-vlabel', x:lp[0], y:lp[1], 'dominant-baseline':'middle',
            'text-anchor':flip2 ? 'end' : 'start', text:nm }));
        }
      }
    });

    /* sector label, just outside the rim, anchored by side so it never runs off */
    var mid = (a0 + a1) / 2, lp2 = polar(cx, cy, r1 + 30, mid), ca = Math.cos(mid);
    var an = ca > 0.3 ? 'start' : (ca < -0.3 ? 'end' : 'middle');
    var ly = lp2[1] + (an === 'middle' ? (Math.sin(mid) < 0 ? -8 : 14) : 0);
    var ly0 = ly;
    for (var guard = 0; guard < 30 && !lab.fits(lp2[0], ly + 6, 150, 30); guard++) ly += 7;
    lab.claim(lp2[0], ly + 6, 150, 30);
    if (ly - ly0 > 10) {
      var anchorPt = polar(cx, cy, r1 + 6, mid);
      grp.appendChild(el('path', { d:'M' + anchorPt[0] + ',' + anchorPt[1] + 'L' + lp2[0] + ',' + (ly - 4),
        stroke:'rgba(22,26,32,0.22)', 'stroke-width':1, fill:'none' }));
    }
    grp.appendChild(halo({ class:'cdm-glabel', x:lp2[0], y:ly, 'text-anchor':an, 'dominant-baseline':'middle',
      text:D[row.i].name }));
    grp.appendChild(halo({ class:'cdm-gsub', x:lp2[0], y:ly + 13, 'text-anchor':an, 'dominant-baseline':'middle',
      text:row.items.length + ' families · ' + fmt(domVal(row.i)) }));
    g.appendChild(grp);
    a = a1 + gap;
  });

  g.appendChild(el('circle', { cx:cx, cy:cy, r:r0 + 26, fill:'url(#core)', 'pointer-events':'none' }));
  /* log rings — recessive, solid hairlines, so the scale is readable */
  [1e3, 1e5, 1e7].forEach(function (v) {
    if (v > maxV) return;
    g.appendChild(el('circle', { cx:cx, cy:cy, r:len(v), fill:'none', stroke:'rgba(22,26,32,0.16)', 'stroke-width':1 }));
    g.appendChild(halo({ class:'cdm-vlabel', x:cx + 4, y:cy - len(v) - 4, text:fmt(v) + ' rows' }));
  });
  g.appendChild(el('text', { class:'cdm-glabel', x:cx, y:cy - 4, 'text-anchor':'middle', text:comma(n) }));
  g.appendChild(el('text', { class:'cdm-gsub', x:cx, y:cy + 10, 'text-anchor':'middle', text:'families' }));
  return g;
}

/* =======================================================================
   MAP 4 — Constellation: orbital rings by order of magnitude
   ======================================================================= */
function drawConstellation(){
  var W = 900, H = 640, cx = W/2, cy = H/2;
  var g = svg(W, H, 'Constellation: table families on orbital rings, one ring per order of magnitude');
  var list = activeDoms();
  var items = fams().slice().sort(function (a, b) { return b.rows - a.rows; });
  if (!items.length) return g;

  var hero = items[0], rest = items.slice(1);
  var DEC = [3,4,5,6,7];                       /* 1k, 10k, 100k, 1m, 10m+ */
  var ringR = { 7:118, 6:168, 5:216, 4:258, 3:296 };
  function dec(v){ return Math.max(3, Math.min(7, Math.floor(Math.log10(Math.max(v,1))))); }

  var lab = new Labeller();
  DEC.forEach(function (d) {
    g.appendChild(el('circle', { cx:cx, cy:cy, r:ringR[d], fill:'none', stroke:'rgba(22,26,32,0.10)', 'stroke-width':1 }));
    lab.taken.push({ x:cx + 42, y:cy - ringR[d] - 9, w:84, h:18 });  /* reserved, drawn last */
  });

  /* each domain owns an angular wedge, in palette order, so neighbouring
     bubbles are neighbouring slots */
  var counts = {}, wedge = {}, aa = -Math.PI/2;
  list.forEach(function (i) { counts[i] = rest.filter(function (f) { return f.d === i; }).length; });
  var tot = list.reduce(function (a, i) { return a + counts[i]; }, 0) || 1;
  list.forEach(function (i) {
    var w = Math.max(counts[i] / tot * Math.PI*2, 0.06);
    wedge[i] = { a0:aa, a1:aa + w }; aa += w;
  });
  var scale = Math.max.apply(null, rest.map(function (f) { return f.rows; }));
  function rad(v){ return Math.max(2.6, Math.sqrt(v / scale) * 26 + 2.4); }

  var nodes = [];
  list.forEach(function (i) {
    var mine = rest.filter(function (f) { return f.d === i; });
    var byDec = {};
    mine.forEach(function (f) { (byDec[dec(f.rows)] = byDec[dec(f.rows)] || []).push(f); });
    Object.keys(byDec).forEach(function (d) {
      var arr = byDec[d], R = ringR[d], W2 = wedge[i];
      arr.forEach(function (f, k) {
        var ang = W2.a0 + (k + 0.5) / arr.length * (W2.a1 - W2.a0);
        var jitter = ((k % 3) - 1) * 7;
        nodes.push({ f:f, i:i, r:rad(f.rows), x:cx + Math.cos(ang) * (R + jitter), y:cy + Math.sin(ang) * (R + jitter) });
      });
    });
  });
  /* light collision pass only — orbits must stay recognisable */
  for (var it = 0; it < 90; it++) {
    for (var p = 0; p < nodes.length; p++) for (var q = p+1; q < nodes.length; q++) {
      var A2 = nodes[p], B2 = nodes[q], dx = B2.x - A2.x, dy = B2.y - A2.y;
      var need = A2.r + B2.r + 2, d2 = dx*dx + dy*dy;
      if (d2 < need*need && d2 > 0) {
        var dd = Math.sqrt(d2), push = (need - dd) / dd * 0.34;
        A2.x -= dx*push; A2.y -= dy*push; B2.x += dx*push; B2.y += dy*push;
      }
    }
  }

  nodes.sort(function (a, b) { return b.r - a.r; });
  /* seed the placer with every bubble, so a name never lands on a mark */
  nodes.forEach(function (nd) { lab.taken.push({ x:nd.x, y:nd.y, w:nd.r*2, h:nd.r*2 }); });
  lab.taken.push({ x:cx, y:cy, w:120, h:120 });
  nodes.forEach(function (nd) {
    var isSel = st.sel && st.sel.kind === 'family' && st.sel.obj === nd.f;
    var c = el('circle', { class:'cdm-node', cx:nd.x, cy:nd.y, r:nd.r, fill:COL[nd.i],
      'fill-opacity':0.9, stroke:isSel ? INK : SURF, 'stroke-width':2,
      tabindex:nd.r > 6 ? 0 : null, 'aria-label':nd.f.name + ', ' + comma(nd.f.rows) + ' rows' });
    var html = famTip(nd.f);
    c.addEventListener('mouseenter', function (ev) { showTip(html, ev); });
    c.addEventListener('mouseleave', hideTip);
    c.addEventListener('click', function () { select('family', nd.f); render(); });
    c.addEventListener('focus', function () { select('family', nd.f); });
    g.appendChild(c);
    if (nd.r > 10) {
      var nm = nd.f.name.length > 20 ? nd.f.name.slice(0, 19) + '…' : nd.f.name;
      var w = textW(nm, 10.5) + 10;
      /* try below, then above, then either side — first clear slot wins */
      var cands = [[nd.x, nd.y + nd.r + 12, 'middle'], [nd.x, nd.y - nd.r - 6, 'middle'],
                   [nd.x + nd.r + 6, nd.y + 4, 'start'], [nd.x - nd.r - 6, nd.y + 4, 'end']];
      for (var ci = 0; ci < cands.length; ci++) {
        var c2 = cands[ci];
        var bx = c2[2] === 'middle' ? c2[0] : (c2[2] === 'start' ? c2[0] + w/2 : c2[0] - w/2);
        if (lab.claim(bx, c2[1] - 4, w, 16)) {
          g.appendChild(halo({ class:'cdm-vlabel', x:c2[0], y:c2[1], 'text-anchor':c2[2], text:nm }));
          break;
        }
      }
    }
  });

  /* ring scale labels last, so they sit on top of the orbits they name */
  DEC.forEach(function (d) {
    g.appendChild(halo({ class:'cdm-vlabel', x:cx + 6, y:cy - ringR[d] - 5, text:fmt(Math.pow(10, d)) + '+ rows' }));
  });

  /* the hero at the centre — the estate's single dominant object */
  var hr = 54;
  g.appendChild(el('circle', { class:'cdm-node', cx:cx, cy:cy, r:hr, fill:COL[hero.d], stroke:SURF, 'stroke-width':3,
    tabindex:0, 'aria-label':hero.name + ', ' + comma(hero.rows) + ' rows' }));
  var hero1 = el('text', { x:cx, y:cy - 4, 'text-anchor':'middle', 'font-size':13, 'font-weight':600, fill:'#fff',
    'pointer-events':'none', text:hero.name });
  g.appendChild(hero1);
  g.appendChild(el('text', { x:cx, y:cy + 13, 'text-anchor':'middle', 'font-size':15, 'font-weight':600, fill:'#fff',
    'pointer-events':'none', text:fmt(hero.rows) }));
  g.appendChild(el('text', { x:cx, y:cy + 28, 'text-anchor':'middle', 'font-size':10.5, fill:'#fff', 'fill-opacity':0.85,
    'pointer-events':'none', text:hero.n + ' tables · ' + pct(hero.rows, T.rows) }));
  var heroHit = g.lastChild;
  return g;
}

/* =======================================================================
   Table views — every map has a WCAG-clean twin. Nothing is gated behind
   colour or a tooltip.
   ======================================================================= */
function tbl(head, rows){
  var t = el('table');
  var thead = el('thead'), tr = el('tr');
  head.forEach(function (h) { tr.appendChild(el('th', { text:h })); });
  thead.appendChild(tr); t.appendChild(thead);
  var tb = el('tbody');
  rows.forEach(function (r) {
    var x = el('tr');
    r.forEach(function (c, i) {
      var td = el('td');
      if (i === 0 && c && c.sw !== undefined) {
        td.appendChild(el('span', { class:'cdm-sw', style:'background:' + c.sw }));
        td.appendChild(document.createTextNode(c.text));
        if (c.mono) td.className = 'cdm-mono';
      } else td.textContent = c;
      x.appendChild(td);
    });
    tb.appendChild(x);
  });
  t.appendChild(tb);
  return el('div', { class:'cdm-tblwrap' }, [t]);
}
function drawTable(){
  if (st.tab === 'flow' && st.mode === 'chord') {
    var list = activeDoms();
    return tbl(['FK references from ↓ / to →'].concat(list.map(function (i) { return D[i].name; })).concat(['Total out']),
      list.map(function (i) {
        var row = [{ sw:COL[i], text:D[i].name }];
        var tot = 0;
        list.forEach(function (j) { row.push(comma(S.matrix[i][j])); tot += S.matrix[i][j]; });
        row.push(comma(tot));
        return row;
      }));
  }
  if (st.tab === 'flow') {
    var list2 = activeDoms(), head = ['Domain'].concat(BANDS.map(function (b) { return b.k; })).concat(['Total']);
    return tbl(head, list2.map(function (i) {
      var row = [{ sw:COL[i], text:D[i].name }], tot = 0;
      BANDS.forEach(function (b, bi) {
        var v = 0;
        F.forEach(function (f) {
          if (f.d !== i || !matches(f)) return;
          var k = 0; while (k < BANDS.length - 1 && f.rows < BANDS[k].lo) k++;
          if (k === bi) v += val(f);
        });
        if (bi === 4 && !st.q) v += domTailVal(i);
        tot += v; row.push(comma(v));
      });
      row.push(comma(tot));
      return row;
    }));
  }
  var items = fams().slice().sort(function (a, b) { return val(b) - val(a); });
  return tbl(['Table family', 'Domain', 'Rows', 'Tables', 'Share of estate'],
    items.map(function (f) {
      return [{ sw:COL[f.d], text:f.name, mono:true }, D[f.d].name, comma(f.rows), comma(f.n), pct(f.rows, T.rows)];
    }));
}

/* =======================================================================
   Chrome — one filter row above everything it scopes
   ======================================================================= */

/* =======================================================================
   Chrome — one filter row above everything it scopes, stat tiles, legend,
   selection rail, and the map/table switch. Built by the module so the host
   page only has to provide an empty element.
   ======================================================================= */
function elh(tag, cls, txt){
  var n = document.createElement(tag);
  if (cls) n.className = cls;
  if (txt !== undefined) n.textContent = txt;
  return n;
}
function buildChrome(host, opts){
  host.innerHTML = '';
  host.classList.add('cdm-root');
  var strip = elh('div', 'cdm-tabs');
  strip.setAttribute('role', 'tablist');
  if (!opts.showTabs) strip.style.display = 'none';
  var filters = elh('div', 'cdm-filters');
  var tiles   = elh('div', 'cdm-tiles');
  var layout  = elh('div', 'cdm-layout');
  var main    = document.createElement('main');
  var card    = elh('div', 'cdm-card');
  var hd      = elh('div', 'cdm-hd');
  var hdText  = document.createElement('div');
  var h2      = elh('h2'); var sub = elh('p');
  var seg     = elh('div', 'cdm-seg'); seg.setAttribute('role', 'group'); seg.setAttribute('aria-label', 'View');
  var plot    = elh('div', 'cdm-plot');
  var note    = elh('p', 'cdm-note');
  var rail    = elh('aside', 'cdm-rail');
  var lgCard  = elh('div', 'cdm-card cdm-pad');
  var legend  = elh('div', 'cdm-lg');
  var dtCard  = elh('div', 'cdm-card cdm-pad cdm-detail');
  var detail  = document.createElement('div');

  hdText.appendChild(h2); hdText.appendChild(sub);
  hd.appendChild(hdText); hd.appendChild(seg);
  card.appendChild(hd); card.appendChild(plot);
  main.appendChild(card); main.appendChild(note);
  lgCard.appendChild(elh('h3', null, 'Domains')); lgCard.appendChild(legend);
  dtCard.appendChild(elh('h3', null, 'Selection')); dtCard.appendChild(detail);
  rail.appendChild(lgCard); rail.appendChild(dtCard);
  layout.appendChild(main); layout.appendChild(rail);
  host.appendChild(strip); host.appendChild(filters); host.appendChild(tiles); host.appendChild(layout);

  if (!document.querySelector('.cdm-tip')) {
    var t = elh('div', 'cdm-tip'); t.setAttribute('role', 'status'); t.setAttribute('aria-live', 'polite');
    document.body.appendChild(t);
  }
  tip = document.querySelector('.cdm-tip');
  ui = { host:host, strip:strip, filters:filters, tiles:tiles, seg:seg, plot:plot,
         note:note, legend:legend, detail:detail, title:h2, sub:sub };
}

function renderFilters(){
  var box = ui.filters; box.innerHTML = '';
  var gd = elh('div', 'cdm-fgroup');
  gd.appendChild(elh('span', 'cdm-flabel', 'Domains'));
  D.forEach(function (d, i) {
    var b = elh('button', 'cdm-chip');
    b.type = 'button';
    b.setAttribute('aria-pressed', st.doms.has(i) ? 'true' : 'false');
    b.title = comma(d.rows) + ' rows across ' + comma(d.tables) + ' tables';
    var sw = elh('span', 'cdm-sw'); sw.style.background = COL[i];
    b.appendChild(sw); b.appendChild(document.createTextNode(d.name));
    b.addEventListener('click', function () {
      if (st.doms.has(i)) st.doms['delete'](i); else st.doms.add(i);
      if (!st.doms.size) st.doms = new Set([0,1,2,3,4,5,6,7]);
      st.sel = null; render();
    });
    gd.appendChild(b);
  });
  box.appendChild(gd);

  var gm = elh('div', 'cdm-fgroup');
  gm.appendChild(elh('span', 'cdm-flabel', 'Size by'));
  var seg = elh('div', 'cdm-seg'); seg.setAttribute('role', 'group'); seg.setAttribute('aria-label', 'Metric');
  [['rows','Rows'],['tables','Tables']].forEach(function (m) {
    var b = elh('button', null, m[1]); b.type = 'button';
    b.setAttribute('aria-pressed', st.metric === m[0] ? 'true' : 'false');
    b.addEventListener('click', function () { st.metric = m[0]; render(); });
    seg.appendChild(b);
  });
  gm.appendChild(seg); box.appendChild(gm);

  var inp = document.createElement('input');
  inp.type = 'search'; inp.placeholder = 'Filter table families…'; inp.value = st.q;
  inp.setAttribute('aria-label', 'Filter table families');
  inp.addEventListener('input', function () { st.q = inp.value.trim().toLowerCase(); render(true); });
  var gs = elh('div', 'cdm-fgroup'); gs.appendChild(inp); box.appendChild(gs);

  var reset = elh('button', 'cdm-chip', 'Reset'); reset.type = 'button';
  reset.setAttribute('aria-pressed', 'true');
  reset.addEventListener('click', function () {
    st.doms = new Set([0,1,2,3,4,5,6,7]); st.q = ''; st.sel = null; st.metric = 'rows'; render();
  });
  box.appendChild(reset);
}

function renderTiles(){
  var box = ui.tiles; box.innerHTML = '';
  var list = activeDoms();
  var rows   = list.reduce(function (a, i) { return a + D[i].rows; }, 0);
  var tables = list.reduce(function (a, i) { return a + D[i].tables; }, 0);
  var empty  = list.reduce(function (a, i) { return a + D[i].empty; }, 0);
  var items = fams().slice().sort(function (a, b) { return b.rows - a.rows; });
  var acc = 0, k = 0;
  while (k < items.length && acc < rows * 0.95) { acc += items[k].rows; k++; }
  var coreTables = items.slice(0, k).reduce(function (a, f) { return a + f.n; }, 0);
  [['Rows in scope', fmt(rows), comma(rows) + ' across ' + comma(tables) + ' tables'],
   ['The 95% core', comma(coreTables) + ' tables', k + ' families — ' + pct(coreTables, tables) + ' of the estate'],
   ['Empty tables', comma(empty), pct(empty, tables) + ' of tables in scope, no rows at all'],
   ['Heaviest family', items.length ? items[0].name : '—',
    items.length ? comma(items[0].rows) + ' rows · ' + items[0].n + ' tables' : '']
  ].forEach(function (t) {
    var tile = elh('div', 'cdm-tile');
    tile.appendChild(elh('div', 'cdm-k', t[0]));
    var v = elh('div', 'cdm-v', t[1]);
    if (t[1].length > 14) { v.style.fontSize = '16px'; v.style.lineHeight = '1.3'; v.style.marginTop = '5px'; }
    tile.appendChild(v); tile.appendChild(elh('div', 'cdm-s', t[2]));
    box.appendChild(tile);
  });
}

function renderLegend(){
  var box = ui.legend; box.innerHTML = '';
  activeDoms().forEach(function (i) {
    var r = elh('div', 'cdm-row');
    var sw = elh('span', 'cdm-sw'); sw.style.background = COL[i];
    r.appendChild(sw); r.appendChild(document.createTextNode(D[i].name));
    r.appendChild(elh('span', 'cdm-num', fmt(domVal(i))));
    box.appendChild(r);
  });
  var off = [0,1,2,3,4,5,6,7].filter(function (i) { return !st.doms.has(i); });
  if (off.length) box.appendChild(elh('p', 'cdm-hint', off.length + ' domain' + (off.length > 1 ? 's' : '') + ' filtered out'));
}

function renderStrip(){
  var box = ui.strip; box.innerHTML = '';
  TABS.forEach(function (t) {
    var b = elh('button', 'cdm-tab', t.label);
    b.type = 'button'; b.setAttribute('role', 'tab');
    b.setAttribute('aria-selected', st.tab === t.id ? 'true' : 'false');
    b.addEventListener('click', function () { st.tab = t.id; st.sel = null; render(); });
    box.appendChild(b);
  });
}

function render(keepFilters){
  resolveSurface();
  renderStrip();
  if (!keepFilters) renderFilters();
  renderTiles(); renderLegend(); renderDetail();

  var t = TABS.filter(function (x) { return x.id === st.tab; })[0];
  ui.title.textContent = t.title;
  ui.sub.textContent = typeof t.sub === 'function' ? t.sub() : t.sub;
  var noteText = typeof t.note === 'function' ? t.note() : t.note;
  ui.note.textContent = (st.tab === 'flow' && st.mode === 'flow')
    ? 'Volume bands and the move plan are derived from row counts alone — a sizing heuristic for run planning, not a record of mapping decisions. Switch “Size by” to Tables for the same split by object count.'
    : noteText;

  var seg = ui.seg; seg.innerHTML = '';
  if (st.tab === 'flow') {
    [['chord','Chord'],['flow','Flow']].forEach(function (m) {
      var b = elh('button', null, m[1]); b.type = 'button';
      b.setAttribute('aria-pressed', st.mode === m[0] ? 'true' : 'false');
      b.addEventListener('click', function () { st.mode = m[0]; render(); });
      seg.appendChild(b);
    });
    var divider = elh('span'); divider.style.width = '1px';
    divider.style.background = 'var(--cdm-border-2)';
    seg.appendChild(divider);
  }
  [['map','Map'],['table','Table']].forEach(function (v) {
    var b = elh('button', null, v[1]); b.type = 'button';
    b.setAttribute('aria-pressed', st.view === v[0] ? 'true' : 'false');
    b.addEventListener('click', function () { st.view = v[0]; render(); });
    seg.appendChild(b);
  });

  ui.plot.innerHTML = '';
  var node;
  if (st.view === 'table') node = drawTable();
  else if (st.tab === 'estate') node = drawEstate();
  else if (st.tab === 'flow') node = st.mode === 'chord' ? drawChord() : drawFlow();
  else if (st.tab === 'burst') node = drawBurst();
  else node = drawConstellation();
  ui.plot.appendChild(node);
  hideTip();
}

/* =======================================================================
   Styles — injected once, every rule scoped to .cdm-root so nothing leaks
   into the rest of the console. Tokens mirror the app's own light surface.
   ======================================================================= */
var CSS = [
/* Each token defers to the console's own variable and falls back to the light
   literal it was validated against, so the same file serves the app in either
   theme and the standalone prototype unchanged. */
'.cdm-root{--cdm-surface:var(--bg2,#FFFFFF);--cdm-surface-2:var(--bg3,#FAFAFB);',
'  --cdm-text:var(--text,#1A1D21);--cdm-text-2:var(--text2,#4B5158);',
'  --cdm-text-3:var(--text3,#767C84);--cdm-border:var(--border,rgba(22,26,32,0.10));',
'  --cdm-border-2:var(--border2,rgba(22,26,32,0.16));',
'  --cdm-accent:var(--accent,#4A5BD6);color:var(--cdm-text);font-size:14px;line-height:1.45}',
'.cdm-root *{box-sizing:border-box}',
'.cdm-root svg{display:block;width:100%;height:auto;overflow:visible}',
'.cdm-tabs{display:flex;flex-wrap:wrap;gap:2px;border-bottom:1px solid var(--cdm-border)}',
'.cdm-tab{appearance:none;background:none;border:0;border-bottom:2px solid transparent;padding:9px 13px;font:inherit;font-size:13.5px;color:var(--cdm-text-2);cursor:pointer;border-radius:6px 6px 0 0}',
'.cdm-tab:hover{background:var(--cdm-surface-2);color:var(--cdm-text)}',
'.cdm-tab[aria-selected="true"]{color:var(--cdm-accent);border-bottom-color:var(--cdm-accent);font-weight:500}',
'.cdm-filters{display:flex;flex-wrap:wrap;align-items:center;gap:8px 10px;padding:12px 0 14px;border-bottom:1px solid var(--cdm-border)}',
'.cdm-fgroup{display:flex;align-items:center;gap:6px}',
'.cdm-flabel{font-size:11.5px;letter-spacing:.06em;text-transform:uppercase;color:var(--cdm-text-3);font-weight:500}',
'.cdm-chip{appearance:none;border:1px solid var(--cdm-border-2);background:var(--cdm-surface);border-radius:999px;padding:4px 10px 4px 7px;font:inherit;font-size:12.5px;color:var(--cdm-text-2);cursor:pointer;display:inline-flex;align-items:center;gap:6px;min-height:26px}',
'.cdm-chip .cdm-sw{width:9px;height:9px;border-radius:2px;flex:none}',
'.cdm-chip[aria-pressed="false"]{opacity:.42}',
'.cdm-chip:hover{border-color:var(--cdm-text-3)}',
'.cdm-seg{display:inline-flex;border:1px solid var(--cdm-border-2);border-radius:7px;overflow:hidden;background:var(--cdm-surface)}',
'.cdm-seg button{appearance:none;background:none;border:0;padding:5px 11px;font:inherit;font-size:12.5px;color:var(--cdm-text-2);cursor:pointer;min-height:28px}',
'.cdm-seg button[aria-pressed="true"]{background:var(--cdm-accent);color:#fff}',
'.cdm-root input[type=search]{border:1px solid var(--cdm-border-2);background:var(--cdm-surface);border-radius:7px;padding:5px 10px;font:inherit;font-size:12.5px;min-width:190px;min-height:28px;color:var(--cdm-text)}',
'.cdm-tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:1px;background:var(--cdm-border);border:1px solid var(--cdm-border);border-radius:10px;overflow:hidden;margin:16px 0}',
'.cdm-tile{background:var(--cdm-surface);padding:12px 14px}',
'.cdm-k{font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:var(--cdm-text-3);font-weight:500}',
'.cdm-v{font-size:25px;font-weight:600;letter-spacing:-0.02em;margin-top:2px}',
'.cdm-s{font-size:12px;color:var(--cdm-text-3)}',
'.cdm-layout{display:grid;grid-template-columns:minmax(0,1fr) 268px;gap:16px;align-items:start}',
'@media(max-width:980px){.cdm-layout{grid-template-columns:minmax(0,1fr)}}',
'.cdm-card{background:var(--cdm-surface);border:1px solid var(--cdm-border);border-radius:12px;overflow:hidden}',
'.cdm-pad{padding:13px 14px}',
'.cdm-hd{display:flex;align-items:baseline;justify-content:space-between;gap:12px;padding:13px 16px 0}',
'.cdm-hd h2{font-size:14.5px;font-weight:600;margin:0}',
'.cdm-hd p{font-size:12.5px;color:var(--cdm-text-3);margin:2px 0 0}',
'.cdm-plot{padding:6px 10px 14px}',
'.cdm-rail{display:flex;flex-direction:column;gap:12px}',
'.cdm-rail h3{font-size:11.5px;letter-spacing:.06em;text-transform:uppercase;color:var(--cdm-text-3);font-weight:500;margin:0 0 9px}',
'.cdm-lg{display:flex;flex-direction:column;gap:6px}',
'.cdm-row{display:flex;align-items:center;gap:8px;font-size:12.5px;color:var(--cdm-text-2)}',
'.cdm-row .cdm-sw{width:10px;height:10px;border-radius:2px;flex:none}',
'.cdm-num{margin-left:auto;font-variant-numeric:tabular-nums;color:var(--cdm-text-3);font-size:12px}',
'.cdm-detail dl{margin:0;display:grid;grid-template-columns:auto 1fr;gap:4px 10px;font-size:12.5px}',
'.cdm-detail dt{color:var(--cdm-text-3)}',
'.cdm-detail dd{margin:0;text-align:right;font-variant-numeric:tabular-nums}',
'.cdm-detail .cdm-nm{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12.5px;word-break:break-all;margin-bottom:8px}',
'.cdm-hint,.cdm-note{font-size:12px;color:var(--cdm-text-3);margin:8px 0 0;max-width:72ch}',
'.cdm-root table{border-collapse:collapse;width:100%;font-size:12.5px}',
'.cdm-root th,.cdm-root td{text-align:right;padding:6px 10px;border-bottom:1px solid var(--cdm-border);font-variant-numeric:tabular-nums}',
'.cdm-root th{font-weight:500;color:var(--cdm-text-3);font-size:11.5px;letter-spacing:.04em;text-transform:uppercase;position:sticky;top:0;background:var(--cdm-surface)}',
'.cdm-root th:first-child,.cdm-root td:first-child{text-align:left;font-variant-numeric:normal}',
'.cdm-root td .cdm-sw{width:9px;height:9px;border-radius:2px;display:inline-block;margin-right:7px}',
'.cdm-tblwrap{max-height:460px;overflow:auto}',
'.cdm-mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}',
'.cdm-node{cursor:pointer}',
'.cdm-glabel{font-size:11.5px;font-weight:600;fill:var(--cdm-text)}',
'.cdm-gsub{font-size:10.5px;fill:var(--cdm-text-3)}',
'.cdm-vlabel{font-size:10.5px;fill:var(--cdm-text-2);font-variant-numeric:tabular-nums}',
'.cdm-tip{position:fixed;z-index:9999;pointer-events:none;background:#1A1D21;color:#fff;border-radius:7px;padding:7px 10px;font-size:12.5px;line-height:1.4;max-width:280px;opacity:0;transition:opacity .09s;font-family:inherit}',
'.cdm-tip .cdm-t{font-weight:600;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}',
'.cdm-tip .cdm-r{color:#C9CDD3;display:flex;gap:10px;justify-content:space-between}',
'.cdm-tip b{font-variant-numeric:tabular-nums;color:#fff;font-weight:500}'
].join('\n');

function injectCSS(){
  if (document.getElementById('cdm-styles')) return;
  var s = document.createElement('style');
  s.id = 'cdm-styles'; s.textContent = CSS;
  document.head.appendChild(s);
}

/* =======================================================================
   Public API
   ======================================================================= */
function setData(data){
  S = data; D = S.domains; F = S.families; T = S.totals;
}
/* An empty table list is not data — before the schema has loaded smTables()
   answers [], and adapting that would draw four convincingly empty maps
   instead of saying the schema is not here yet. */
function liveData(){
  if (typeof window.smTables === 'function') {
    try {
      var tables = window.smTables();
      if (tables && tables.length) {
        var edges = typeof window.smEdges === 'function' ? window.smEdges() : null;
        return fromSchema(tables, edges);
      }
    } catch (e) {}
  }
  if (window.CYGENIX_SNAPSHOT) return window.CYGENIX_SNAPSHOT;
  return null;
}
function mount(host, data, opts){
  opts = opts || {};
  injectCSS();
  data = data || liveData();
  if (!data) { host.textContent = 'No schema loaded.'; return; }
  setData(data);
  root = host;
  buildChrome(host, { showTabs: opts.showTabs !== false });
  if (opts.tab) st.tab = opts.tab;
  render();
  return { render:render, state:st };
}
/* --- registration on the Schema Explorer's own .mode-bar ------------------
   The console's tabs are <button id="se-tab-KEY" aria-controls="se-view-KEY">
   inside .mode-bar, and seSwitchTab(KEY) shows the matching panel. We add four
   buttons of the same shape sharing one panel, and wrap seSwitchTab so that
   selecting a native tab stands our panel down. Adjust MODE_BAR / PANEL_HOST
   here if those hooks ever move — nothing else in the file depends on them. */
var MODE_BAR = '.mode-bar';
var PANEL_ID = 'se-view-datamaps';

var panelEl = null, mounted = false, loading = false, wanted = null;

function drawPanel(tab){
  if (!panelEl) return;
  if (tab) { st.tab = tab; st.sel = null; }
  if (mounted) { render(); return; }
  panelEl.innerHTML = '';
  mounted = !!mount(panelEl, null, { showTabs:false, tab:st.tab });
}

/* The schema is loaded lazily by the Schema map, so the first click on one of
   our tabs usually lands before there is anything to draw. Ask the console to
   load it, say so in the panel meanwhile, and draw whichever of our tabs the
   user is on by the time it arrives. */
function showPanel(tab){
  wanted = tab || st.tab;
  if (liveData()) { drawPanel(wanted); return; }
  if (!mounted) panelEl.textContent = 'Loading schema…';
  if (loading || typeof window.smEnsureLoaded !== 'function') return;
  loading = true;
  Promise.resolve(window.smEnsureLoaded()).then(function () {
    loading = false;
    drawPanel(wanted);
  }, function () {
    loading = false;
    if (!mounted) panelEl.textContent = 'Could not load the schema.';
  });
}

function refresh(){
  if (!mounted) { if (panelEl && panelEl.style.display !== 'none') showPanel(null); return; }
  var data = liveData();
  if (!data || !root) return;
  setData(data);
  render();
}

function install(){
  var bar = document.querySelector(MODE_BAR);
  if (!bar || document.getElementById('se-tab-dm-estate')) return false;
  var native = [].slice.call(bar.querySelectorAll('button'));
  var firstPanel = document.getElementById(native[0] && native[0].getAttribute('aria-controls'));
  if (!firstPanel) return false;

  var panel = document.createElement('div');
  panel.id = PANEL_ID;
  panel.style.display = 'none';
  panel.style.padding = '4px 0 24px';
  firstPanel.parentElement.appendChild(panel);
  panelEl = panel;

  /* The console paints the selected tab with an .active class, not with
     aria-selected, so the two have to move together in both directions —
     otherwise the tab you left stays lit while one of ours is showing. */
  function setOn(b, on){
    b.classList.toggle('active', on);
    b.setAttribute('aria-selected', String(on));
  }
  function standDownNative(){
    native.forEach(function (b) {
      setOn(b, false);
      var p = document.getElementById(b.getAttribute('aria-controls'));
      if (p) p.style.display = 'none';
    });
  }
  function standDownOurs(){
    panel.style.display = 'none';
    TABS.forEach(function (t) {
      var b = document.getElementById('se-tab-dm-' + t.id);
      if (b) setOn(b, false);
    });
  }

  TABS.forEach(function (t) {
    var b = document.createElement('button');
    /* Shaped like the native buttons, minus their selected state: native[0]
       is the tab that happens to be open, and cloning its class wholesale
       would light all four of ours up before they had ever been clicked. */
    b.className = (native[0] ? native[0].className : 'mode-btn').replace(/\bactive\b/g, '').trim();
    b.id = 'se-tab-dm-' + t.id;
    b.type = 'button';
    b.setAttribute('role', 'tab');
    b.setAttribute('aria-selected', 'false');
    b.setAttribute('aria-controls', PANEL_ID);
    b.textContent = t.label;
    b.addEventListener('click', function () {
      standDownNative(); standDownOurs();
      setOn(b, true);
      panel.style.display = 'block';
      showPanel(t.id);
    });
    bar.appendChild(b);
  });

  var original = window.seSwitchTab;
  if (typeof original === 'function') {
    window.seSwitchTab = function () {
      standDownOurs();
      return original.apply(this, arguments);
    };
  }
  return true;
}

/* Is one of our tabs the one on screen? The console's own seReloadSchema()
   asks, so that ↻ Reload schema reloads what the user is actually looking at. */
function isActive(){ return !!(panelEl && panelEl.style.display !== 'none'); }

return {
  install: install,
  mount: mount,
  refresh: refresh,
  isActive: isActive,
  fromSchema: fromSchema,
  refPairs: refPairs,
  classify: classify,
  familyKey: familyKey,
  TABS: TABS,
  DOMAIN_NAMES: DOMAIN_NAMES,
  PALETTE: COL,
  get state(){ return st; }
};
})();

/* Auto-register once the console's tab bar exists. Remove this block if you
   would rather call CygenixDataMaps.install() from your own bootstrap. */
(function () {
  function go(){ if (window.CygenixDataMaps.install()) return true; return false; }
  if (!go()) {
    var tries = 0;
    var iv = setInterval(function () { if (go() || ++tries > 40) clearInterval(iv); }, 250);
  }
})();
