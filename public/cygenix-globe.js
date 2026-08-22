/* ============================================================================
   cygenix-globe.js — where the estate's data sits, on a globe
   ----------------------------------------------------------------------------
   The eighth Schema Explorer view. Registers a 🌐 Globe tab on the same tab
   bar as the rest, discovers which tables carry geography, aggregates them by
   region, and draws the result as extruded bars standing off a rotatable
   sphere — filterable and groupable by the same business areas the Data map
   and the data maps use.

     CygenixGlobe.install()   register the tab (called automatically)
     CygenixGlobe.mount(el)   render into any element
     CygenixGlobe.refresh()   re-read the schema and redraw
     CygenixGlobe.isActive()  is this the view on screen?

   Rendering is a hand-rolled orthographic projection onto a 2D canvas rather
   than WebGL. It costs no dependency, it is the same zero-library approach as
   every other map here, and it is its own fallback: there is no second
   renderer to keep in step for machines without WebGL. The drawing surface is
   reached only through project() and the draw* functions, so a GPU backend
   could be dropped in behind them.

   Privacy: nothing on this globe is finer than a region count. Every query is
   a GROUP BY with a COUNT (see cygenix-geo-index.js), so no address, client
   or matter can reach the screen even if one is in the data.
   ========================================================================== */
window.CygenixGlobe = (function () {
'use strict';

var GEO_PATH = '/geo/';
var PANEL_ID = 'se-view-globe';
var MODE_BAR = '.mode-bar';
var TAB_ID   = 'se-tab-globe';

var DOMAINS = ['Finance & AR','Time & billing','Matters & clients','Documents & content',
               'People & HR','Reference & lookup','System & audit','Other'];
var COL = ['#2a78d6','#eb6834','#1baf7a','#eda100','#e87ba4','#008300','#8A9099','#C2C7CD'];

/* The data maps own the shared vocabulary — take theirs when it is loaded so
   a business area means one thing across the whole Explorer. */
function palette(){ return (window.CygenixDataMaps && window.CygenixDataMaps.PALETTE) || COL; }
function domainNames(){ return (window.CygenixDataMaps && window.CygenixDataMaps.DOMAIN_NAMES) || DOMAINS; }
function classify(n){ return window.CygenixDataMaps ? window.CygenixDataMaps.classify(n) : 7; }
function familyKey(n){ return window.CygenixDataMaps ? window.CygenixDataMaps.familyKey(n) : n; }

var st = {
  doms: new Set([0,1,2,3,4,5,6,7]),
  metric: 'rows',              /* rows | distinct | tables */
  view: 'globe',               /* globe | table | sources | unresolved */
  includeShadow: false,
  minCell: 0,
  q: '',
  drill: null,                 /* ISO3 once drilled in */
  drill2: null,                /* admin-1 name once drilled again */
  sel: null,
  rot: { lon: -10, lat: 25 },
  zoom: 1,
  spin: 0,
};

var G = {
  root: null, ui: {}, index: null, ledger: null, world: null,
  admin1: {}, admin1Index: null, seed: null,
  status: '', running: false, error: null, side: 'tgt', database: null,
  raf: null, drag: null, vel: 0, hover: null, need: true, fs: false,
};

function GI(){ return window.CygenixGeoIndex; }

/* db-connect answers an execute with { success, rowsAffected, recordset } —
   the driver's own shape. Reading only .rows returns nothing from every query
   while every query reports success, which is the most confusing failure this
   view can have: a full set of answers and an empty map. */
function resultRows(res){
  return (res && (res.recordset || res.rows)) || [];
}
function fmt(n){
  if (n >= 1e9) return (n/1e9).toFixed(n>=1e10?0:1).replace(/\.0$/,'') + 'bn';
  if (n >= 1e6) return (n/1e6).toFixed(n>=1e7?0:1).replace(/\.0$/,'') + 'm';
  if (n >= 1e3) return (n/1e3).toFixed(n>=1e4?0:1).replace(/\.0$/,'') + 'k';
  return String(n);
}
function comma(n){ return Number(n || 0).toLocaleString('en-GB'); }
function pct(a, b){ return b ? (a / b * 100).toFixed(a / b < 0.01 ? 2 : 1) + '%' : '0%'; }
function reduced(){
  return !!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches);
}

/* ---------- tiny DOM helpers -------------------------------------------- */
function elh(tag, cls, txt){
  var n = document.createElement(tag);
  if (cls) n.className = cls;
  if (txt !== undefined) n.textContent = txt;
  return n;
}
function clear(n){ while (n && n.firstChild) n.removeChild(n.firstChild); return n; }

/* =======================================================================
   Data — discovery, then aggregation
   ======================================================================= */
function liveTables(){
  try { return (typeof window.smTables === 'function' ? window.smTables() : []) || []; }
  catch (e) { return []; }
}
function liveEdges(){
  try { return (typeof window.smEdges === 'function' ? window.smEdges() : []) || []; }
  catch (e) { return []; }
}
/* The Schema map's state is a top-level `const`, which lives in the global
   lexical scope and never becomes a property of window — reachable by name
   from another script, but not as window.SM. */
function smState(){
  try { return typeof SM !== 'undefined' ? SM : null; } catch (e) { return null; }
}

/* The harvest carries every table's columns and keys, paged and cached in
   IndexedDB. It is the same cache the Migration map fills, under the same
   key, so whichever view is opened first pays for it once.

   Nothing is written back onto the graph's nodes: a node with a columns array
   is a node the Schema map's inspector believes it has already loaded in
   full, and a list of bare names is not that. The harvest is kept here. */
function ensureColumns(){
  var H = window.CygenixSubjectHarvest, SG = window.CygenixSchemaGraph;
  if (!H || !SG) return Promise.resolve(null);
  var side = G.side, db = G.database || 'unknown';
  return H.shEnsure({
    execute: function (sql) { return SG.execute(side, sql); },
    database: db,
    cacheKey: side + '|' + db,
    onProgress: function (p) { say('Reading the catalogue — ' + p.stage + ' ' + comma(p.n)); },
  }).then(function (h) { G.harvest = h; return h; }, function () { return null; });
}

/* One descriptor per table, columns filled from whichever source has them:
   the node itself, the harvest signature, or the harvest. */
function describe(t){
  var cols = null, pk = (t.primaryKeys || []).slice();
  if (t.columns && t.columns.length) {
    cols = t.columns.map(function (c) { return { name: typeof c === 'string' ? c : c.name }; });
  }
  var sig = t.colsig;
  if (!sig && G.harvest && G.harvest.tables) {
    var rec = G.harvest.tables[t.key] || G.harvest.tables[(t.schema || 'dbo') + '.' + t.name];
    if (rec) {
      sig = rec.c;
      if (!pk.length && rec.pk) pk = rec.pk.slice();
    }
  }
  if (!cols && sig) {
    cols = String(sig).split(',').map(function (c) { return { name: c.split(':')[0] }; });
  }
  return { name: t.name, key: t.key, schema: t.schema, kind: t.kind,
           rowCount: t.rowCount || 0, columns: cols || [], primaryKeys: pk };
}

function discover(){
  var tables = liveTables();
  if (!tables.length) return null;
  return GI().geoIndex(tables.map(describe), liveEdges(), {
    includeShadow: st.includeShadow, classify: classify, familyKey: familyKey,
  });
}

/* The estate's own Country table is the authority for what its codes mean. */
function seedFromEstate(){
  var SG = window.CygenixSchemaGraph;
  var ref = liveTables().filter(function (t) { return GI().isCountryRef(t.name); })[0];
  if (!ref || !SG) return Promise.resolve(null);
  var parts = String(ref.key || ref.name).split('.');
  var sql = 'SELECT TOP 400 * FROM [' + (parts[0] || 'dbo') + '].[' + (parts[1] || ref.name) + ']';
  return SG.execute(G.side, sql)
    .then(function (res) { return GI().geoSeedFromCountryTable(resultRows(res)); })
    .catch(function () { return null; });
}

function run(force){
  if (G.running) return;
  var SG = window.CygenixSchemaGraph;
  if (!SG) { G.error = 'The schema graph is not loaded.'; render(); return; }
  G.running = true; G.error = null;
  say('Looking for geography…');
  render();

  Promise.resolve(liveTables().length ? ensureColumns() : null)
    .then(function () {
      G.index = discover();
      if (!G.index || !G.index.length) throw new Error('No table in this schema carries geography.');
      render();
      return seedFromEstate();
    })
    .then(function (seed) {
      G.seed = seed;
      var enabled = G.index.filter(function (e) { return e.enabled !== false; });
      say('Aggregating ' + enabled.length + ' table' + (enabled.length === 1 ? '' : 's') + '…');
      render();
      return GI().geoRun(G.index, {
        exec: function (sql) {
          return SG.execute(G.side, sql).then(resultRows);
        },
        metric: st.metric === 'distinct' ? 'distinct' : 'rows',
        seed: seed,
        minCell: st.minCell,
        concurrency: 3,
        onProgress: function (d, n, table) {
          say('Aggregating — ' + d + ' of ' + n + ' · ' + table);
          if (G.ui.status) G.ui.status.textContent = G.status;
        },
      });
    })
    .then(function (ledger) {
      G.ledger = ledger;
      G.running = false;
      say('');
      return loadWorld();
    })
    .then(function () { render(); })
    .catch(function (err) {
      G.running = false;
      G.error = (err && err.message) || String(err);
      say('');
      render();
    });
}

/* ---------- boundaries, fetched once and kept for the session ------------ */
function loadWorld(){
  if (G.world) return Promise.resolve(G.world);
  return fetch(GEO_PATH + 'countries-110m.json')
    .then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
    .then(function (d) {
      G.world = d;
      G.worldById = {};
      d.features.forEach(function (f) { G.worldById[f.id] = f; });
      return d;
    })
    .catch(function () { G.world = null; return null; });
}
var _a1inflight = {};
function loadAdmin1(iso3){
  if (G.admin1[iso3]) return Promise.resolve(G.admin1[iso3]);
  if (_a1inflight[iso3]) return _a1inflight[iso3];
  _a1inflight[iso3] = fetch(GEO_PATH + 'admin1/' + iso3 + '.json')
    .then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
    .then(function (d) { G.admin1[iso3] = d; delete _a1inflight[iso3]; render(); return d; })
    .catch(function () { G.admin1[iso3] = { features: [], missing: true }; delete _a1inflight[iso3]; render(); return null; });
  return _a1inflight[iso3];
}

function say(s){ G.status = s; if (G.ui.status) G.ui.status.textContent = s; }

/* =======================================================================
   The numbers on screen
   ======================================================================= */
function domainFiltered(bucket){
  /* A bucket counts by domain; the chips decide which of those count. */
  if (!bucket) return 0;
  if (st.doms.size === 8) return bucket.n;
  var n = 0;
  Object.keys(bucket.domains || {}).forEach(function (d) {
    if (st.doms.has(Number(d))) n += bucket.domains[d];
  });
  return n;
}
function bucketValue(bucket){
  if (st.metric === 'tables') {
    var keys = Object.keys(bucket.tables || {});
    if (st.doms.size === 8) return keys.length;
    return keys.filter(function (t) {
      var e = (G.index || []).filter(function (x) { return x.table === t; })[0];
      return !e || st.doms.has(e.domain);
    }).length;
  }
  return domainFiltered(bucket);
}
function countries(){
  if (!G.ledger) return [];
  var list = G.ledger.countryList.map(function (c) {
    return { iso3: c.iso3, name: c.name, v: bucketValue(c), raw: c };
  }).filter(function (c) { return c.v > 0; });
  if (st.q) {
    var q = st.q.toLowerCase();
    list = list.filter(function (c) { return c.name.toLowerCase().indexOf(q) >= 0
      || c.iso3.toLowerCase().indexOf(q) >= 0; });
  }
  return list.sort(function (a, b) { return b.v - a.v; });
}
function admin1List(iso3){
  var c = G.ledger && G.ledger.countries[iso3];
  if (!c) return [];
  return Object.keys(c.admin1).map(function (k) {
    return { name: k, v: bucketValue(c.admin1[k]), raw: c.admin1[k] };
  }).filter(function (x) { return x.v > 0; }).sort(function (a, b) { return b.v - a.v; });
}
function admin2List(iso3, a1){
  var c = G.ledger && G.ledger.countries[iso3];
  var unit = c && c.admin1[a1];
  if (!unit) return [];
  return Object.keys(unit.admin2).map(function (k) {
    return { name: k, v: bucketValue(unit.admin2[k]), raw: unit.admin2[k] };
  }).filter(function (x) { return x.v > 0; }).sort(function (a, b) { return b.v - a.v; });
}
function dominantDomain(bucket){
  var best = -1, bestN = -1;
  Object.keys(bucket.domains || {}).forEach(function (d) {
    if (!st.doms.has(Number(d))) return;
    if (bucket.domains[d] > bestN) { bestN = bucket.domains[d]; best = Number(d); }
  });
  return best < 0 ? 7 : best;
}
function unitLabel(){
  return st.metric === 'tables' ? 'tables' : (st.metric === 'distinct' ? 'entities' : 'records');
}

/* =======================================================================
   Orthographic projection — the whole of the 3D
   ======================================================================= */
function rad(d){ return d * Math.PI / 180; }

/* lon/lat → a point on the unit sphere, rotated into view space.
   z > 0 is the near hemisphere. */
function toView(lon, lat){
  var la = rad(lat), lo = rad(lon - st.rot.lon);
  var x = Math.cos(la) * Math.sin(lo);
  var y = Math.sin(la);
  var z = Math.cos(la) * Math.cos(lo);
  var t = rad(st.rot.lat);
  var y2 = y * Math.cos(t) - z * Math.sin(t);
  var z2 = y * Math.sin(t) + z * Math.cos(t);
  return { x: x, y: y2, z: z2 };
}
function screenOf(v, geom, alt){
  var s = geom.r * (1 + (alt || 0));
  return { x: geom.cx + v.x * s, y: geom.cy - v.y * s, z: v.z };
}
function project(lon, lat, geom, alt){
  var v = toView(lon, lat);
  var p = screenOf(v, geom, alt);
  p.visible = v.z > 0;
  return p;
}
/* Where a segment crosses the limb, so a half-visible coastline ends on the
   edge of the disc instead of jumping across it. */
function limbPoint(a, b, geom){
  var t = a.z / (a.z - b.z);
  var x = a.x + (b.x - a.x) * t, y = a.y + (b.y - a.y) * t;
  var m = Math.sqrt(x * x + y * y) || 1;
  return { x: geom.cx + (x / m) * geom.r, y: geom.cy - (y / m) * geom.r };
}
function ringPath(ctx, ring, geom){
  var started = false, prev = null, prevV = null;
  for (var i = 0; i < ring.length; i++) {
    var v = toView(ring[i][0], ring[i][1]);
    var p = screenOf(v, geom);
    if (v.z > 0) {
      if (!started) { ctx.moveTo(p.x, p.y); started = true; }
      else if (prevV && prevV.z <= 0) { var e = limbPoint(prevV, v, geom); ctx.lineTo(e.x, e.y); ctx.lineTo(p.x, p.y); }
      else ctx.lineTo(p.x, p.y);
    } else if (started && prevV && prevV.z > 0) {
      var q = limbPoint(prevV, v, geom);
      ctx.lineTo(q.x, q.y);
    }
    prev = p; prevV = v;
  }
  return started;
}

/* =======================================================================
   Drawing
   ======================================================================= */
function css(name, fallback){
  if (!G.root || typeof window.getComputedStyle !== 'function') return fallback;
  var v = window.getComputedStyle(G.root).getPropertyValue(name);
  return (v && v.trim()) || fallback;
}
function geometry(canvas){
  var w = canvas.width / (window.devicePixelRatio || 1);
  var h = canvas.height / (window.devicePixelRatio || 1);
  return { cx: w / 2, cy: h / 2, r: Math.min(w, h) * 0.42 * st.zoom, w: w, h: h };
}

/* Log scale, because the estate spans a thousand records to ten million and a
   linear bar would flatten everything below the largest into the surface. */
function barHeight(v, max){
  if (v <= 0 || max <= 0) return 0;
  return 0.04 + 0.42 * (Math.log10(1 + v) / Math.log10(1 + max));
}

function draw(){
  var canvas = G.ui.canvas;
  if (!canvas) return;
  var ctx = canvas.getContext('2d');
  if (!ctx) return;
  var dpr = window.devicePixelRatio || 1;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  var geom = geometry(canvas);
  ctx.clearRect(0, 0, geom.w, geom.h);

  var ink   = css('--cdm-text', '#1A1D21');
  var faint = css('--cdm-border', 'rgba(22,26,32,0.10)');
  var sea   = css('--cdm-surface-2', '#F3F4F5');
  var land  = css('--cdm-border-2', 'rgba(22,26,32,0.16)');

  /* Sphere */
  ctx.beginPath();
  ctx.arc(geom.cx, geom.cy, geom.r, 0, Math.PI * 2);
  ctx.fillStyle = sea; ctx.fill();
  ctx.strokeStyle = faint; ctx.lineWidth = 1; ctx.stroke();

  /* Graticule — 30° apart, enough to read the rotation and no more. */
  ctx.strokeStyle = faint; ctx.lineWidth = 0.5;
  for (var lat = -60; lat <= 60; lat += 30) {
    ctx.beginPath();
    var open = false;
    for (var lon = -180; lon <= 180; lon += 4) {
      var v = toView(lon, lat), p = screenOf(v, geom);
      if (v.z > 0) { open ? ctx.lineTo(p.x, p.y) : (ctx.moveTo(p.x, p.y), open = true); }
      else open = false;
    }
    ctx.stroke();
  }
  for (var lon2 = -180; lon2 < 180; lon2 += 30) {
    ctx.beginPath();
    var open2 = false;
    for (var la2 = -90; la2 <= 90; la2 += 4) {
      var v2 = toView(lon2, la2), p2 = screenOf(v2, geom);
      if (v2.z > 0) { open2 ? ctx.lineTo(p2.x, p2.y) : (ctx.moveTo(p2.x, p2.y), open2 = true); }
      else open2 = false;
    }
    ctx.stroke();
  }

  var list = countries();
  var byIso = {};
  list.forEach(function (c) { byIso[c.iso3] = c; });
  var max = list.length ? list[0].v : 0;

  /* Land */
  if (G.world) {
    G.world.features.forEach(function (f) {
      var lit = !!byIso[f.id];
      ctx.beginPath();
      var any = false;
      f.r.forEach(function (ring) { if (ringPath(ctx, ring, geom)) { any = true; ctx.closePath(); } });
      if (!any) return;
      ctx.fillStyle = lit ? withAlpha(palette()[dominantDomain(byIso[f.id].raw)], 0.18) : land;
      ctx.fill();
      ctx.strokeStyle = faint; ctx.lineWidth = 0.6; ctx.stroke();
    });
  }

  /* Drilled country: its admin-1 units, filled by value */
  if (st.drill && G.admin1[st.drill] && G.admin1[st.drill].features) {
    var units = admin1List(st.drill);
    var byKey = {};
    units.forEach(function (u) { byKey[normKey(u.name)] = u; });
    var umax = units.length ? units[0].v : 0;
    G.admin1[st.drill].features.forEach(function (f) {
      var hit = matchUnit(f, byKey);
      ctx.beginPath();
      var any = false;
      f.r.forEach(function (ring) { if (ringPath(ctx, ring, geom)) { any = true; ctx.closePath(); } });
      if (!any) return;
      var sel = hit && st.drill2 && normKey(st.drill2) === normKey(hit.name);
      ctx.fillStyle = hit
        ? withAlpha(palette()[dominantDomain(hit.raw)], sel ? 0.75 : 0.25 + 0.45 * (hit.v / (umax || 1)))
        : withAlpha(land, 0.5);
      ctx.fill();
      ctx.strokeStyle = faint; ctx.lineWidth = 0.5; ctx.stroke();
    });
  }

  /* Bars — far ones first, so the near ones overdraw them */
  var bars = [];
  if (st.drill && G.admin1[st.drill] && G.admin1[st.drill].features) {
    var u2 = admin1List(st.drill), byKey2 = {};
    u2.forEach(function (u) { byKey2[normKey(u.name)] = u; });
    var umax2 = u2.length ? u2[0].v : 0;
    G.admin1[st.drill].features.forEach(function (f) {
      var hit = matchUnit(f, byKey2);
      if (!hit) return;
      bars.push({ lon: f.c[0], lat: f.c[1], v: hit.v, max: umax2,
                  domain: dominantDomain(hit.raw), label: hit.name, kind: 'admin1' });
    });
  } else {
    list.forEach(function (c) {
      var f = G.worldById && G.worldById[c.iso3];
      if (!f) return;
      bars.push({ lon: f.c[0], lat: f.c[1], v: c.v, max: max,
                  domain: dominantDomain(c.raw), label: c.name, iso3: c.iso3, kind: 'country' });
    });
  }
  bars.forEach(function (b) {
    var v = toView(b.lon, b.lat);
    b.z = v.z;
    b.base = screenOf(v, geom);
    b.tip = screenOf(v, geom, barHeight(b.v, b.max));
    b.visible = v.z > -0.05;
  });
  bars.sort(function (a, b) { return a.z - b.z; });

  bars.forEach(function (b) {
    if (!b.visible) return;
    var c = palette()[b.domain];
    var hot = G.hover && G.hover.label === b.label;
    ctx.globalAlpha = b.z > 0 ? 1 : 0.25;
    ctx.strokeStyle = c;
    ctx.lineWidth = hot ? 5 : 3.2;
    ctx.lineCap = 'round';
    ctx.beginPath();
    ctx.moveTo(b.base.x, b.base.y);
    ctx.lineTo(b.tip.x, b.tip.y);
    ctx.stroke();
    ctx.beginPath();
    ctx.arc(b.tip.x, b.tip.y, hot ? 4.5 : 3, 0, Math.PI * 2);
    ctx.fillStyle = c; ctx.fill();
    if (hot) {
      ctx.strokeStyle = ink; ctx.lineWidth = 1.2; ctx.stroke();
    }
    ctx.globalAlpha = 1;
  });
  G.bars = bars;

  /* Direct labels on the heaviest few — identity never rests on hue alone. */
  ctx.font = '600 11px ' + (css('--serif', 'system-ui'));
  ctx.textAlign = 'center';
  var labelled = bars.filter(function (b) { return b.z > 0.15; })
    .sort(function (a, b) { return b.v - a.v; }).slice(0, 8);
  labelled.forEach(function (b) {
    var text = b.label + ' · ' + fmt(b.v);
    ctx.lineWidth = 3.5; ctx.strokeStyle = sea; ctx.lineJoin = 'round';
    ctx.strokeText(text, b.tip.x, b.tip.y - 8);
    ctx.fillStyle = ink;
    ctx.fillText(text, b.tip.x, b.tip.y - 8);
  });
}

function withAlpha(colour, a){
  var c = String(colour || '').trim();
  var m = c.match(/^#([0-9a-f]{6})$/i);
  if (m) {
    var n = parseInt(m[1], 16);
    return 'rgba(' + ((n >> 16) & 255) + ',' + ((n >> 8) & 255) + ',' + (n & 255) + ',' + a + ')';
  }
  var rgba = c.match(/^rgba?\(([^)]+)\)$/);
  if (rgba) {
    var parts = rgba[1].split(',').map(function (x) { return x.trim(); });
    return 'rgba(' + parts[0] + ',' + parts[1] + ',' + parts[2] + ',' + a + ')';
  }
  return c;
}
function normKey(s){ return String(s || '').toLowerCase().replace(/[^a-z0-9]+/g, ''); }
function matchUnit(feature, byKey){
  var keys = (feature.k || []).concat([normKey(feature.name)]);
  for (var i = 0; i < keys.length; i++) if (byKey[keys[i]]) return byKey[keys[i]];
  return null;
}

/* ---------- the loop ----------------------------------------------------- */
function tick(){
  G.raf = null;
  if (!isActive() || document.hidden) return;      /* nothing spins off screen */
  var moving = false;
  if (Math.abs(G.vel) > 0.01 && !G.drag) {
    st.rot.lon += G.vel;
    G.vel *= reduced() ? 0 : 0.94;
    moving = true;
  }
  if (G.fly) {
    var f = G.fly;
    f.t = Math.min(1, f.t + (reduced() ? 1 : 0.08));
    var e = f.t * f.t * (3 - 2 * f.t);
    st.rot.lon = f.lon0 + (f.lon1 - f.lon0) * e;
    st.rot.lat = f.lat0 + (f.lat1 - f.lat0) * e;
    if (f.t >= 1) G.fly = null;
    moving = true;
  }
  draw();
  if (moving) schedule();
}
function schedule(){
  if (G.raf || !isActive() || document.hidden) return;
  G.raf = requestAnimationFrame(tick);
}

/* =======================================================================
   Interaction
   ======================================================================= */
function wireCanvas(canvas){
  canvas.addEventListener('pointerdown', function (ev) {
    canvas.setPointerCapture(ev.pointerId);
    G.drag = { x: ev.clientX, y: ev.clientY, lon: st.rot.lon, lat: st.rot.lat, moved: 0 };
    G.vel = 0; G.fly = null;
  });
  canvas.addEventListener('pointermove', function (ev) {
    if (G.drag) {
      var dx = ev.clientX - G.drag.x, dy = ev.clientY - G.drag.y;
      G.drag.moved = Math.max(G.drag.moved, Math.abs(dx) + Math.abs(dy));
      var k = 0.32 / st.zoom;
      var lon = G.drag.lon + dx * k;
      G.vel = (lon - st.rot.lon) * 0.5;
      st.rot.lon = lon;
      st.rot.lat = Math.max(-85, Math.min(85, G.drag.lat - dy * k));
      schedule();
      return;
    }
    hoverAt(ev);
  });
  var end = function (ev) {
    if (G.drag && G.drag.moved < 4) pick(ev);
    G.drag = null;
    if (reduced()) G.vel = 0;
    schedule();
  };
  canvas.addEventListener('pointerup', end);
  canvas.addEventListener('pointercancel', function () { G.drag = null; });
  canvas.addEventListener('pointerleave', function () { hideTip(); G.hover = null; schedule(); });
  canvas.addEventListener('wheel', function (ev) {
    ev.preventDefault();
    st.zoom = Math.max(0.6, Math.min(6, st.zoom * (ev.deltaY > 0 ? 0.9 : 1.1)));
    schedule();
  }, { passive: false });
  canvas.addEventListener('dblclick', function (ev) {
    var b = nearestBar(ev);
    if (b) flyTo(b.lon, b.lat);
  });
  canvas.addEventListener('keydown', function (ev) {
    var step = 6;
    if (ev.key === 'ArrowLeft')  { st.rot.lon -= step; ev.preventDefault(); }
    else if (ev.key === 'ArrowRight') { st.rot.lon += step; ev.preventDefault(); }
    else if (ev.key === 'ArrowUp')    { st.rot.lat = Math.min(85, st.rot.lat + step); ev.preventDefault(); }
    else if (ev.key === 'ArrowDown')  { st.rot.lat = Math.max(-85, st.rot.lat - step); ev.preventDefault(); }
    else if (ev.key === '+' || ev.key === '=') { st.zoom = Math.min(6, st.zoom * 1.15); ev.preventDefault(); }
    else if (ev.key === '-') { st.zoom = Math.max(0.6, st.zoom / 1.15); ev.preventDefault(); }
    else if (ev.key === 'Escape' && (st.drill || st.drill2)) { drillUp(); ev.preventDefault(); }
    else return;
    schedule();
  });
}
function localPoint(ev){
  var rect = G.ui.canvas.getBoundingClientRect();
  return { x: ev.clientX - rect.left, y: ev.clientY - rect.top };
}
function nearestBar(ev){
  var p = localPoint(ev), best = null, bestD = 18 * 18;
  (G.bars || []).forEach(function (b) {
    if (b.z <= 0) return;
    var d = Math.min(dist2(p, b.tip), dist2(p, b.base));
    if (d < bestD) { bestD = d; best = b; }
  });
  return best;
}
function dist2(a, b){ var dx = a.x - b.x, dy = a.y - b.y; return dx * dx + dy * dy; }

function hoverAt(ev){
  var b = nearestBar(ev);
  if (b === G.hover) { if (b) moveTip(ev); return; }
  G.hover = b;
  if (!b) { hideTip(); schedule(); return; }
  showTip(tipHtml(b), ev);
  schedule();
}
function pick(ev){
  var b = nearestBar(ev);
  if (!b) return;
  if (b.kind === 'country') { st.sel = { kind: 'country', iso3: b.iso3, name: b.label }; }
  else { st.sel = { kind: 'admin1', name: b.label }; }
  render();
}

function flyTo(lon, lat){
  G.fly = { t: 0, lon0: st.rot.lon, lat0: st.rot.lat, lon1: lon, lat1: Math.max(-70, Math.min(70, lat)) };
  schedule();
}
function drillTo(iso3){
  st.drill = iso3; st.drill2 = null; st.sel = null;
  loadAdmin1(iso3);
  var f = G.worldById && G.worldById[iso3];
  if (f) { flyTo(f.c[0], f.c[1]); st.zoom = Math.max(st.zoom, 2.2); }
  render();
}
function drillUp(){
  if (st.drill2) st.drill2 = null;
  else if (st.drill) { st.drill = null; st.zoom = 1; }
  st.sel = null;
  render();
}

/* ---------- tooltip, inside the container so full screen keeps it -------- */
function showTip(html, ev){
  var tip = G.ui.tip;
  if (!tip) return;
  tip.innerHTML = html;
  tip.style.opacity = 1;
  moveTip(ev);
}
function moveTip(ev){
  var tip = G.ui.tip;
  if (!tip || !G.ui.plot) return;
  var host = G.ui.plot.getBoundingClientRect();
  var x = ev.clientX - host.left + 14, y = ev.clientY - host.top + 14;
  if (x + tip.offsetWidth > host.width - 8) x = ev.clientX - host.left - tip.offsetWidth - 14;
  if (y + tip.offsetHeight > host.height - 8) y = ev.clientY - host.top - tip.offsetHeight - 14;
  tip.style.left = x + 'px'; tip.style.top = y + 'px';
}
function hideTip(){ if (G.ui.tip) G.ui.tip.style.opacity = 0; }

function tipHtml(b){
  var bucket = b.kind === 'country'
    ? (G.ledger.countries[b.iso3] || null)
    : (admin1List(st.drill).filter(function (u) { return u.name === b.label; })[0] || {}).raw;
  var total = totalValue();
  var out = '<div class="cdm-t">' + esc(b.label) + '</div>'
    + '<div class="cdm-r"><span>' + unitLabel() + '</span><b>' + comma(b.v) + '</b></div>'
    + '<div class="cdm-r"><span>share</span><b>' + pct(b.v, total) + '</b></div>';
  if (bucket) {
    var names = domainNames();
    Object.keys(bucket.domains || {}).filter(function (d) { return st.doms.has(Number(d)); })
      .sort(function (a, b2) { return bucket.domains[b2] - bucket.domains[a]; })
      .slice(0, 3)
      .forEach(function (d) {
        out += '<div class="cdm-r"><span>' + esc(names[d]) + '</span><b>' + comma(bucket.domains[d]) + '</b></div>';
      });
    var tables = Object.keys(bucket.tables || {})
      .sort(function (a, b2) { return bucket.tables[b2] - bucket.tables[a]; }).slice(0, 2);
    if (tables.length) out += '<div class="cdm-r"><span>top table</span><b>' + esc(tables[0]) + '</b></div>';
  }
  if (b.kind === 'country') out += '<div class="cdm-r"><span>double-click</span><b>to centre</b></div>';
  return out;
}
function esc(s){
  return String(s == null ? '' : s).replace(/[&<>"]/g, function (c) {
    return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c];
  });
}
function totalValue(){
  return countries().reduce(function (a, c) { return a + c.v; }, 0);
}

/* =======================================================================
   Chrome
   ======================================================================= */
function buildChrome(host){
  clear(host);
  host.classList.add('cdm-root', 'cg-root');

  var filters = elh('div', 'cdm-filters');
  var tiles   = elh('div', 'cdm-tiles');
  var layout  = elh('div', 'cdm-layout');
  var main    = document.createElement('main');
  var card    = elh('div', 'cdm-card');
  var hd      = elh('div', 'cdm-hd');
  var hdText  = document.createElement('div');
  var h2      = elh('h2'); var sub = elh('p');
  var crumb   = elh('div', 'cg-crumb');
  var seg     = elh('div', 'cdm-seg');
  seg.setAttribute('role', 'group'); seg.setAttribute('aria-label', 'View');
  var plot    = elh('div', 'cdm-plot cg-plot');
  var canvas  = document.createElement('canvas');
  canvas.className = 'cg-canvas';
  canvas.tabIndex = 0;
  canvas.setAttribute('role', 'img');
  canvas.setAttribute('aria-label', 'Globe of the estate by region. Drag to rotate, scroll to zoom, '
    + 'arrow keys to turn. Every value is also in the Table view.');
  var tip     = elh('div', 'cdm-tip cg-tip');
  var table   = elh('div', 'cg-table');
  var note    = elh('p', 'cdm-note');
  var status  = elh('div', 'cg-status');
  var rail    = elh('aside', 'cdm-rail');
  var lgCard  = elh('div', 'cdm-card cdm-pad');
  var legend  = elh('div', 'cdm-lg');
  var dtCard  = elh('div', 'cdm-card cdm-pad cdm-detail');
  var detail  = document.createElement('div');

  hdText.appendChild(h2); hdText.appendChild(sub); hdText.appendChild(crumb);
  hd.appendChild(hdText); hd.appendChild(seg);
  plot.appendChild(canvas); plot.appendChild(tip); plot.appendChild(table);
  card.appendChild(hd); card.appendChild(plot);
  main.appendChild(card); main.appendChild(note); main.appendChild(status);
  lgCard.appendChild(elh('h3', null, 'Domains')); lgCard.appendChild(legend);
  dtCard.appendChild(elh('h3', null, 'Selection')); dtCard.appendChild(detail);
  rail.appendChild(lgCard); rail.appendChild(dtCard);
  layout.appendChild(main); layout.appendChild(rail);
  host.appendChild(filters); host.appendChild(tiles); host.appendChild(layout);

  G.ui = { host: host, filters: filters, tiles: tiles, card: card, hd: hd, seg: seg,
           title: h2, sub: sub, crumb: crumb, plot: plot, canvas: canvas, tip: tip,
           table: table, note: note, status: status, legend: legend, detail: detail, rail: rail };

  wireCanvas(canvas);
  wireFullscreen(card, hd, seg);
  if (typeof ResizeObserver === 'function') {
    var ro = new ResizeObserver(function () { sizeCanvas(); schedule(); });
    ro.observe(plot);
  }
  document.addEventListener('visibilitychange', function () { if (!document.hidden) schedule(); });
}

function wireFullscreen(card, hd, seg){
  if (!window.CygenixFullscreen) return;
  var btn = elh('button', 'btn cdm-fsbtn', '⤢ Full screen');
  btn.type = 'button';
  btn.title = 'Full screen (F, or Esc to leave)';
  hd.insertBefore(btn, seg.nextSibling);
  window.CygenixFullscreen.attach({
    button: btn, target: card,
    adopt: ['.cg-root .cdm-rail'],
    visible: function () { return isActive(); },
    onChange: function (on) { G.fs = on; sizeCanvas(); schedule(); },
  });
}

function sizeCanvas(){
  var canvas = G.ui.canvas, plot = G.ui.plot;
  if (!canvas || !plot) return;
  var dpr = window.devicePixelRatio || 1;
  var w = Math.max(240, plot.clientWidth - 20);
  var h = G.fs ? Math.max(320, plot.clientHeight - 20) : 560;
  canvas.style.width = w + 'px';
  canvas.style.height = h + 'px';
  canvas.width = Math.round(w * dpr);
  canvas.height = Math.round(h * dpr);
}

/* ---------- filters ------------------------------------------------------ */
function renderFilters(){
  var box = clear(G.ui.filters);
  var names = domainNames(), pal = palette();

  var gd = elh('div', 'cdm-fgroup');
  gd.appendChild(elh('span', 'cdm-flabel', 'Business area'));
  names.forEach(function (n, i) {
    var b = elh('button', 'cdm-chip');
    b.type = 'button';
    b.setAttribute('aria-pressed', st.doms.has(i) ? 'true' : 'false');
    var sw = elh('span', 'cdm-sw'); sw.style.background = pal[i];
    b.appendChild(sw); b.appendChild(document.createTextNode(n));
    b.addEventListener('click', function () {
      st.doms.has(i) ? st.doms.delete(i) : st.doms.add(i);
      if (!st.doms.size) st.doms = new Set([0,1,2,3,4,5,6,7]);
      render();
    });
    gd.appendChild(b);
  });
  box.appendChild(gd);

  var gm = elh('div', 'cdm-fgroup');
  gm.appendChild(elh('span', 'cdm-flabel', 'Count'));
  var segm = elh('div', 'cdm-seg');
  [['rows','Records'],['distinct','Entities'],['tables','Tables']].forEach(function (m) {
    var b = elh('button', null, m[1]); b.type = 'button';
    b.setAttribute('aria-pressed', st.metric === m[0] ? 'true' : 'false');
    b.title = m[0] === 'distinct'
      ? 'Count each entity once — the tables with a single key answer this; the rest fall back to records'
      : (m[0] === 'tables' ? 'How many tables contribute to each region'
                           : 'Raw record count, which over-weights audit and log tables');
    b.addEventListener('click', function () {
      if (st.metric === m[0]) return;
      var wasQueryMetric = st.metric === 'distinct' || m[0] === 'distinct';
      st.metric = m[0];
      /* Records and entities are different queries; tables is the same data
         counted differently, so only the first needs a re-run. */
      if (wasQueryMetric && G.ledger) run(true); else render();
    });
    segm.appendChild(b);
  });
  gm.appendChild(segm);
  box.appendChild(gm);

  var gt = elh('div', 'cdm-fgroup');
  var shadow = elh('label', 'cg-check');
  var cb = document.createElement('input'); cb.type = 'checkbox'; cb.checked = st.includeShadow;
  cb.addEventListener('change', function () { st.includeShadow = cb.checked; run(true); });
  shadow.appendChild(cb); shadow.appendChild(document.createTextNode('Include _draft / _template / _audit'));
  shadow.title = 'Shadow tables duplicate the geography of the table they shadow, so they are out by default';
  gt.appendChild(shadow);

  var supp = elh('label', 'cg-check');
  var sc = document.createElement('input'); sc.type = 'checkbox'; sc.checked = st.minCell > 0;
  sc.addEventListener('change', function () { st.minCell = sc.checked ? 5 : 0; run(true); });
  supp.appendChild(sc); supp.appendChild(document.createTextNode('Roll up regions under 5'));
  supp.title = 'Suppresses small cells into their parent region. Off by default because it changes '
    + 'the totals on screen, which would then disagree with the Data map.';
  gt.appendChild(supp);
  box.appendChild(gt);

  var gs = elh('div', 'cdm-fgroup');
  var inp = document.createElement('input');
  inp.type = 'search'; inp.placeholder = 'Filter countries…'; inp.value = st.q;
  inp.setAttribute('aria-label', 'Filter countries');
  inp.addEventListener('input', function () { st.q = inp.value.trim(); render(true); });
  gs.appendChild(inp);
  box.appendChild(gs);
}

function renderTiles(){
  var box = clear(G.ui.tiles);
  var L = G.ledger;
  var add = function (k, v, s){
    var t = elh('div', 'cdm-tile');
    t.appendChild(elh('div', 'cdm-k', k));
    t.appendChild(elh('div', 'cdm-v', v));
    if (s) t.appendChild(elh('div', 'cdm-s', s));
    box.appendChild(t);
  };
  if (!L) {
    add('Geo-bearing tables', G.index ? String(G.index.length) : '—',
        G.index ? 'found by column name and by key into the Country table'
                : 'the catalogue is read when you run the scan');
    return;
  }
  var list = countries();
  var total = totalValue();
  add(unitLabel().replace(/^./, function (c) { return c.toUpperCase(); }) + ' placed', fmt(total),
      comma(total) + ' across ' + list.length + ' countries');
  add('Countries', String(list.length),
      list.length ? list[0].name + ' leads with ' + pct(list[0].v, total) : 'nothing resolved');
  add('Unresolved', fmt(L.unresolved.n),
      L.total ? pct(L.unresolved.n, L.total) + ' of what was counted'
              : 'nothing came back to resolve');
  /* Two different numbers used to share this tile. It counts the geo-bearing
     tables the scan queried — say so, rather than showing "121 / 121" and
     leaving the reader to guess what 121 is. */
  var failed = (L.failures && L.failures.length) || 0;
  add('Geo-bearing tables', String(L.planned),
      failed ? (L.queried + ' answered, ' + failed + ' failed — see Sources')
             : ('all ' + L.queried + ' answered'));
}

function renderLegend(){
  var box = clear(G.ui.legend);
  var names = domainNames(), pal = palette();
  var per = {};
  countries().forEach(function (c) {
    Object.keys(c.raw.domains || {}).forEach(function (d) {
      if (!st.doms.has(Number(d))) return;
      per[d] = (per[d] || 0) + c.raw.domains[d];
    });
  });
  names.forEach(function (n, i) {
    if (!st.doms.has(i)) return;
    var row = elh('div', 'cdm-row');
    var sw = elh('span', 'cdm-sw'); sw.style.background = pal[i];
    row.appendChild(sw);
    row.appendChild(document.createTextNode(n));
    row.appendChild(elh('span', 'cdm-num', per[i] ? fmt(per[i]) : '—'));
    box.appendChild(row);
  });
  var scale = elh('p', 'cdm-hint',
    'Bar height is log-scaled: the estate spans a few hundred records to millions, and a linear '
    + 'scale would flatten everything except the largest into the surface. Colour is the business '
    + 'area that contributes most to that region.');
  box.appendChild(scale);
}

function renderDetail(){
  var box = clear(G.ui.detail);
  if (!G.ledger) {
    box.appendChild(elh('p', 'cdm-hint', 'Run the scan to see where the estate sits.'));
    return;
  }
  if (!st.sel) {
    box.appendChild(elh('p', 'cdm-hint',
      'Hover a bar for detail; click one to pin it here. Double-click to centre the globe on it.'));
    return;
  }
  if (st.sel.kind === 'country') {
    var c = G.ledger.countries[st.sel.iso3];
    box.appendChild(elh('div', 'cdm-nm', st.sel.name + ' · ' + st.sel.iso3));
    var dl = document.createElement('dl');
    var row = function (k, v){
      var dt = elh('dt', null, k), dd = elh('dd', null, v);
      dl.appendChild(dt); dl.appendChild(dd);
    };
    row(unitLabel(), comma(bucketValue(c)));
    row('share', pct(bucketValue(c), totalValue()));
    row('tables', String(Object.keys(c.tables).length));
    var a1 = admin1List(st.sel.iso3);
    row('regions with data', String(a1.length));
    box.appendChild(dl);
    var btn = elh('button', 'btn', a1.length ? '↳ Drill into ' + st.sel.name : 'No region data');
    btn.type = 'button';
    btn.disabled = !a1.length;
    btn.title = a1.length
      ? 'Show this country\'s regions'
      : 'No table in this estate carries a state, province or county value for ' + st.sel.name;
    btn.addEventListener('click', function () { drillTo(st.sel.iso3); });
    box.appendChild(btn);
    if (!a1.length) {
      box.appendChild(elh('p', 'cdm-hint',
        'The drill is greyed out because the data has no sub-region for this country, not because '
        + 'the map cannot draw it.'));
    }
  } else {
    box.appendChild(elh('div', 'cdm-nm', st.sel.name));
    var a2 = admin2List(st.drill, st.sel.name);
    var dl2 = document.createElement('dl');
    var unit = admin1List(st.drill).filter(function (u) { return u.name === st.sel.name; })[0];
    if (unit) {
      dl2.appendChild(elh('dt', null, unitLabel()));
      dl2.appendChild(elh('dd', null, comma(unit.v)));
    }
    dl2.appendChild(elh('dt', null, 'sub-regions'));
    dl2.appendChild(elh('dd', null, String(a2.length)));
    box.appendChild(dl2);
    if (a2.length) {
      var b2 = elh('button', 'btn', st.drill2 === st.sel.name ? '↰ Hide sub-regions' : '↳ List sub-regions');
      b2.type = 'button';
      b2.addEventListener('click', function () {
        st.drill2 = st.drill2 === st.sel.name ? null : st.sel.name;
        render();
      });
      box.appendChild(b2);
    }
  }
}

/* ---------- the table view, and the two ledgers -------------------------- */
function renderTable(){
  var box = clear(G.ui.table);
  if (!G.ledger) return;

  if (st.view === 'sources') {
    box.appendChild(sourcesTable());
    return;
  }
  if (st.view === 'unresolved') {
    box.appendChild(unresolvedTable());
    return;
  }
  var wrap = elh('div', 'cdm-tblwrap');
  var tbl = document.createElement('table');
  var head = document.createElement('thead');
  var hr = document.createElement('tr');
  ['Region', 'Business area', unitLabel(), 'Share', 'Tables'].forEach(function (h, i) {
    var th = elh('th', null, h);
    if (i === 0 || i === 1) th.style.textAlign = 'left';
    hr.appendChild(th);
  });
  head.appendChild(hr); tbl.appendChild(head);
  var body = document.createElement('tbody');
  var total = totalValue();
  var rows = st.drill
    ? admin1List(st.drill).map(function (u) { return { name: u.name, v: u.v, raw: u.raw }; })
    : countries().map(function (c) { return { name: c.name + ' · ' + c.iso3, v: c.v, raw: c.raw, iso3: c.iso3 }; });
  rows.forEach(function (r) {
    var tr = document.createElement('tr');
    var td0 = elh('td', null, r.name); td0.style.textAlign = 'left';
    var sw = elh('span', 'cdm-sw'); sw.style.background = palette()[dominantDomain(r.raw)];
    td0.insertBefore(sw, td0.firstChild);
    tr.appendChild(td0);
    var td1 = elh('td', null, domainNames()[dominantDomain(r.raw)]); td1.style.textAlign = 'left';
    tr.appendChild(td1);
    tr.appendChild(elh('td', null, comma(r.v)));
    tr.appendChild(elh('td', null, pct(r.v, total)));
    tr.appendChild(elh('td', null, String(Object.keys(r.raw.tables || {}).length)));
    if (r.iso3) {
      tr.style.cursor = 'pointer';
      tr.addEventListener('click', function () {
        st.sel = { kind: 'country', iso3: r.iso3, name: r.name.split(' · ')[0] };
        render();
      });
    }
    body.appendChild(tr);
  });
  tbl.appendChild(body);
  wrap.appendChild(tbl);
  box.appendChild(wrap);
}

function sourcesTable(){
  var wrap = elh('div', 'cdm-tblwrap');
  var tbl = document.createElement('table');
  var head = document.createElement('thead');
  var hr = document.createElement('tr');
  ['', 'Table', 'Business area', 'Found via', 'Columns', 'Level', 'Rows'].forEach(function (h, i) {
    var th = elh('th', null, h);
    if (i <= 4) th.style.textAlign = 'left';
    hr.appendChild(th);
  });
  head.appendChild(hr); tbl.appendChild(head);
  var body = document.createElement('tbody');
  var failed = {};
  ((G.ledger && G.ledger.failures) || []).forEach(function (f) { failed[f.table] = f.reason; });
  (G.index || []).forEach(function (e) {
    var tr = document.createElement('tr');
    var td0 = document.createElement('td');
    var cb = document.createElement('input');
    cb.type = 'checkbox'; cb.checked = e.enabled !== false;
    cb.setAttribute('aria-label', 'Include ' + e.table);
    cb.addEventListener('change', function () { e.enabled = cb.checked; render(); });
    td0.appendChild(cb); td0.style.textAlign = 'left';
    tr.appendChild(td0);
    var td1 = elh('td', null, e.table); td1.style.textAlign = 'left'; td1.className = 'cdm-mono';
    if (failed[e.table]) {
      var warn = elh('span', 'cg-warn', ' failed: ' + failed[e.table]);
      td1.appendChild(warn);
    }
    tr.appendChild(td1);
    var td2 = elh('td', null, domainNames()[e.domain]); td2.style.textAlign = 'left';
    tr.appendChild(td2);
    var td3 = elh('td', null, e.via); td3.style.textAlign = 'left';
    tr.appendChild(td3);
    var cols = ['country', 'state', 'county', 'postcode'].filter(function (k) { return e.columns[k]; })
      .map(function (k) { return e.columns[k]; }).join(', ');
    var td4 = elh('td', null, cols || '—'); td4.style.textAlign = 'left'; td4.className = 'cdm-mono';
    tr.appendChild(td4);
    tr.appendChild(elh('td', null, String(e.level)));
    tr.appendChild(elh('td', null, comma(e.rows)));
    body.appendChild(tr);
  });
  tbl.appendChild(body);
  wrap.appendChild(tbl);
  var box = document.createElement('div');
  box.appendChild(wrap);
  var p = elh('p', 'cdm-hint',
    'Detection is a heuristic over column names, plus any key into this estate\'s own Country table. '
    + 'Untick anything it got wrong and re-run — a silently wrong map is worse than an editable one.');
  box.appendChild(p);
  var btn = elh('button', 'btn', '↻ Re-run with these sources');
  btn.type = 'button';
  btn.addEventListener('click', function () { run(true); });
  box.appendChild(btn);
  return box;
}

function unresolvedTable(){
  var box = document.createElement('div');
  var u = G.ledger.unresolved;
  box.appendChild(elh('p', 'cdm-hint',
    comma(u.n) + ' ' + unitLabel() + ' carry a country value that does not resolve to a country — '
    + pct(u.n, G.ledger.total) + ' of everything counted. These are not dropped from the totals and '
    + 'they are not guessed at; they are a data-quality finding for the migration.'));
  var wrap = elh('div', 'cdm-tblwrap');
  var tbl = document.createElement('table');
  var head = document.createElement('thead');
  var hr = document.createElement('tr');
  ['Value', unitLabel()].forEach(function (h, i) {
    var th = elh('th', null, h);
    if (!i) th.style.textAlign = 'left';
    hr.appendChild(th);
  });
  head.appendChild(hr); tbl.appendChild(head);
  var body = document.createElement('tbody');
  u.examples.forEach(function (x) {
    var tr = document.createElement('tr');
    var td = elh('td', null, x.value); td.style.textAlign = 'left'; td.className = 'cdm-mono';
    tr.appendChild(td);
    tr.appendChild(elh('td', null, comma(x.n)));
    body.appendChild(tr);
  });
  tbl.appendChild(body); wrap.appendChild(tbl);
  box.appendChild(wrap);
  var byTable = Object.keys(u.tables).sort(function (a, b) { return u.tables[b] - u.tables[a]; });
  if (byTable.length) {
    box.appendChild(elh('p', 'cdm-hint', 'Mostly from ' + byTable.slice(0, 3).map(function (t) {
      return t + ' (' + comma(u.tables[t]) + ')';
    }).join(', ') + '.'));
  }
  return box;
}

/* ---------- breadcrumb --------------------------------------------------- */
function renderCrumb(){
  var box = clear(G.ui.crumb);
  if (!st.drill) { box.style.display = 'none'; return; }
  box.style.display = '';
  var mk = function (label, fn){
    var a = elh('button', 'cg-crumb-btn', label);
    a.type = 'button';
    if (fn) a.addEventListener('click', fn);
    else a.disabled = true;
    return a;
  };
  box.appendChild(mk('World', function () { st.drill = null; st.drill2 = null; st.zoom = 1; render(); }));
  box.appendChild(elh('span', 'cg-crumb-sep', '›'));
  var name = GI().geoCountryName(st.drill);
  box.appendChild(mk(name, st.drill2 ? function () { st.drill2 = null; render(); } : null));
  if (st.drill2) {
    box.appendChild(elh('span', 'cg-crumb-sep', '›'));
    box.appendChild(mk(st.drill2, null));
  }
  box.appendChild(elh('span', 'cdm-num', 'Esc to go back'));
}

/* =======================================================================
   Render
   ======================================================================= */
function render(keepFilters){
  if (!G.root) return;
  if (!keepFilters) renderFilters();
  renderTiles(); renderLegend(); renderDetail(); renderCrumb();

  var seg = clear(G.ui.seg);
  [['globe','Globe'],['table','Table'],['sources','Sources'],['unresolved','Unresolved']]
    .forEach(function (v) {
      var b = elh('button', null, v[1]); b.type = 'button';
      b.setAttribute('aria-pressed', st.view === v[0] ? 'true' : 'false');
      b.disabled = !G.ledger && v[0] !== 'globe';
      b.addEventListener('click', function () { st.view = v[0]; render(); });
      seg.appendChild(b);
    });

  var drilled = st.drill ? GI().geoCountryName(st.drill) : null;
  G.ui.title.textContent = drilled
    ? 'Globe — ' + drilled + ', by region'
    : 'Globe — where the estate\'s data sits';
  G.ui.sub.textContent = G.ledger
    ? (drilled
        ? 'Regions of ' + drilled + '. Bar height is log-scaled ' + unitLabel() + '; colour is the '
          + 'business area contributing most.'
        : 'One bar per country, standing off its centroid. Height is log-scaled ' + unitLabel()
          + '; colour is the business area contributing most.')
    : 'Reads the geography already in the schema — country, state, county and postcode columns, and '
      + 'any key into this estate\'s own Country table. Aggregate counts only; no address is read.';

  var showGlobe = st.view === 'globe';
  G.ui.canvas.style.display = showGlobe ? '' : 'none';
  G.ui.table.style.display = showGlobe ? 'none' : '';
  if (!showGlobe) hideTip();

  renderTable();
  renderNote();
  renderStatus();
  if (showGlobe) { sizeCanvas(); schedule(); }
}

function renderNote(){
  var n = G.ui.note;
  if (!G.ledger) {
    n.textContent = 'Nothing is queried until you run the scan, and what runs is one GROUP BY per '
      + 'geo-bearing table. Rows are never read — the finest thing that comes back is a count per region.';
    return;
  }
  var parts = [];
  /* Every table answering and nothing landing is the one outcome that looks
     like success and is not: say it plainly instead of drawing a bare globe
     beside a row of zeros. */
  if (G.ledger.queried > 0 && G.ledger.total === 0) {
    parts.push(G.ledger.queried + ' table' + (G.ledger.queried === 1 ? '' : 's')
      + ' answered but returned no rows at all. Either the geography columns are empty in this '
      + 'database, or the tables the scan picked are the wrong ones — the Sources view lists what '
      + 'it chose and why, and each one can be switched off.');
  } else {
    parts.push('Aggregated from ' + G.ledger.queried + ' table' + (G.ledger.queried === 1 ? '' : 's')
      + ' with one GROUP BY each. ' + comma(G.ledger.resolved) + ' of ' + comma(G.ledger.total)
      + ' ' + unitLabel() + ' resolved to a country.');
  }
  if (st.drill && G.admin1[st.drill] && G.admin1[st.drill].missing) {
    parts.push('No boundary file is bundled for ' + GI().geoCountryName(st.drill)
      + ', so its regions are listed rather than drawn.');
  }
  if (st.drill2) {
    parts.push('Sub-regions of ' + st.drill2 + ' are listed in the rail: Natural Earth carries no '
      + 'admin-2 boundaries here, so they are counted rather than drawn.');
  }
  if (G.ledger.suppressed) {
    parts.push('Regions under ' + G.ledger.suppressed + ' ' + unitLabel()
      + ' are rolled into their parent, so these totals sit below the Data map\'s.');
  }
  n.textContent = parts.join(' ');
}

function renderStatus(){
  var s = clear(G.ui.status);
  if (G.error) {
    var e = elh('div', 'cg-error', G.error);
    s.appendChild(e);
  }
  if (G.status) s.appendChild(elh('div', 'cg-run', G.status));
  if (!G.ledger && !G.running) {
    var btn = elh('button', 'btn btn-primary', G.index ? '▶ Aggregate ' + G.index.length + ' tables' : '▶ Scan for geography');
    btn.type = 'button';
    btn.addEventListener('click', function () { run(true); });
    s.appendChild(btn);
  } else if (G.ledger && !G.running) {
    var again = elh('button', 'btn', '↻ Re-run');
    again.type = 'button';
    again.addEventListener('click', function () { run(true); });
    s.appendChild(again);
  }
}

/* =======================================================================
   Styles — .cdm-* comes from the data maps; these are the globe's own
   ======================================================================= */
var CSS = [
  '.cg-root .cg-plot{position:relative;min-height:300px}',
  '.cg-canvas{display:block;margin:0 auto;touch-action:none;cursor:grab;border-radius:8px}',
  '.cg-canvas:active{cursor:grabbing}',
  '.cg-canvas:focus-visible{outline:2px solid var(--cdm-accent);outline-offset:2px}',
  '.cg-tip{position:absolute;left:0;top:0}',
  '.cg-crumb{display:flex;align-items:center;gap:6px;margin-top:6px;flex-wrap:wrap}',
  '.cg-crumb-btn{appearance:none;border:0;background:none;padding:0;font:inherit;font-size:12.5px;',
  '  color:var(--cdm-accent);cursor:pointer;text-decoration:underline}',
  '.cg-crumb-btn:disabled{color:var(--cdm-text);text-decoration:none;cursor:default;font-weight:600}',
  '.cg-crumb-sep{color:var(--cdm-text-3);font-size:12.5px}',
  '.cg-check{display:inline-flex;align-items:center;gap:6px;font-size:12.5px;color:var(--cdm-text-2);cursor:pointer}',
  '.cg-status{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-top:10px}',
  '.cg-run{font-size:12.5px;color:var(--cdm-text-3);font-variant-numeric:tabular-nums}',
  '.cg-error{font-size:12.5px;color:#C0392B}',
  '.cg-warn{font-size:11px;color:#B26A00}',
  '.cg-table{min-height:0}',
  '.cg-root .cdm-detail .btn{margin-top:10px;width:100%}',
].join('\n');
function injectCSS(){
  /* The .cdm-* chrome is the data maps' stylesheet; borrowing it keeps the two
     views identical rather than nearly so. It injects itself on install, but
     ask anyway so the order the tabs register in cannot matter. */
  if (window.CygenixDataMaps && window.CygenixDataMaps.injectStyles) {
    try { window.CygenixDataMaps.injectStyles(); } catch (e) {}
  }
  if (document.getElementById('cg-styles')) return;
  var s = document.createElement('style');
  s.id = 'cg-styles'; s.textContent = CSS;
  document.head.appendChild(s);
}

/* =======================================================================
   Lifecycle
   ======================================================================= */
function mount(host){
  injectCSS();
  G.root = host;
  buildChrome(host);
  var sm = smState();
  G.side = (sm && sm.side) || 'tgt';
  G.database = (sm && sm.graph && sm.graph.database) || null;
  /* Discovery waits for the catalogue. Running it against the graph alone
     would find only the tables with a key into the Country table and report
     that as the answer. */
  loadWorld().then(function () { render(); });
  render();
  return { render: render, state: st };
}
function refresh(){
  if (!G.root) return;
  G.index = null; G.ledger = null; G.error = null;
  var sm2 = smState();
  G.side = (sm2 && sm2.side) || 'tgt';
  G.database = (sm2 && sm2.graph && sm2.graph.database) || null;
  render();
}

var panelEl = null, mounted = false, loading = false;
function isActive(){ return !!(panelEl && panelEl.style.display !== 'none'); }

function drawPanel(){
  if (!panelEl) return;
  if (mounted) { render(); return; }
  panelEl.innerHTML = '';
  mounted = !!mount(panelEl);
}
function showPanel(){
  if (liveTables().length) { drawPanel(); return; }
  if (!mounted) panelEl.textContent = 'Loading schema…';
  if (loading || typeof window.smEnsureLoaded !== 'function') return;
  loading = true;
  Promise.resolve(window.smEnsureLoaded()).then(function () {
    loading = false; drawPanel();
  }, function () {
    loading = false;
    if (!mounted) panelEl.textContent = 'Could not load the schema.';
  });
}

function install(){
  var bar = document.querySelector(MODE_BAR);
  if (!bar || document.getElementById(TAB_ID)) return false;
  var native = [].slice.call(bar.querySelectorAll('button'));
  if (!native.length) return false;
  var firstPanel = document.getElementById(native[0].getAttribute('aria-controls'));
  if (!firstPanel) return false;

  var panel = document.createElement('div');
  panel.id = PANEL_ID;
  panel.style.display = 'none';
  panel.style.padding = '4px 0 24px';
  firstPanel.parentElement.appendChild(panel);
  panelEl = panel;

  function setOn(b, on){
    b.classList.toggle('active', on);
    b.setAttribute('aria-selected', String(on));
  }
  function standDownOthers(){
    [].slice.call(bar.querySelectorAll('button')).forEach(function (b) {
      if (b.id === TAB_ID) return;
      setOn(b, false);
      var p = document.getElementById(b.getAttribute('aria-controls'));
      if (p && p !== panel) p.style.display = 'none';
    });
  }

  var btn = document.createElement('button');
  btn.className = (native[0].className || 'mode-btn').replace(/\bactive\b/g, '').trim();
  btn.id = TAB_ID;
  btn.type = 'button';
  btn.setAttribute('role', 'tab');
  btn.setAttribute('aria-selected', 'false');
  btn.setAttribute('aria-controls', PANEL_ID);
  btn.textContent = '🌐 Globe';
  btn.addEventListener('click', function () {
    standDownOthers();
    setOn(btn, true);
    panel.style.display = 'block';
    showPanel();
  });
  bar.appendChild(btn);

  /* Both the console's own tab switch and the data maps' wrap have to stand
     this panel down; wrapping whatever is there now chains onto theirs. */
  var original = window.seSwitchTab;
  if (typeof original === 'function') {
    window.seSwitchTab = function () {
      panel.style.display = 'none';
      setOn(btn, false);
      return original.apply(this, arguments);
    };
  }
  var others = [].slice.call(bar.querySelectorAll('button'));
  others.forEach(function (b) {
    if (b.id === TAB_ID) return;
    b.addEventListener('click', function () { panel.style.display = 'none'; setOn(btn, false); });
  });
  return true;
}

return {
  install: install,
  mount: mount,
  refresh: refresh,
  isActive: isActive,
  run: run,
  /* Exposed for the tests and for anyone wanting the numbers without the map. */
  __render: render,
  __draw: draw,
  __project: project,
  __barHeight: barHeight,
  get index(){ return G.index; },
  get ledger(){ return G.ledger; },
  get state(){ return st; },
};
})();

/* Auto-register once the console's tab bar exists, after the data maps so the
   Globe sits at the end of the row. */
(function () {
  function go(){ return window.CygenixGlobe.install(); }
  if (!go()) {
    var tries = 0;
    var iv = setInterval(function () { if (go() || ++tries > 40) clearInterval(iv); }, 250);
  }
})();
