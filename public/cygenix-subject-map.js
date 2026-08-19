/* ─────────────────────────────────────────────────────────────────────────────
   cygenix-subject-map.js — semantic subject-area mapping for disparate schemas
   Drop-in replacement for the name-pattern classifier in schema_explorer.html.

   Why it exists
   -------------
   Lexical matching cannot bridge two products. On son_db_hk (Elite) vs the 3E
   target, exact column-name overlap explains almost nothing: 27 table-name twins
   out of 2,545, and the two schemas share no identifier convention
   (mmatter / matter_uno  vs  MattIndex / matterid). What they DO share is a
   business vocabulary. So every identifier is segmented into morphemes, mapped
   to concepts, and compared in concept space.

   Input (both sides, one INFORMATION_SCHEMA pull each — no data values read):
     { tables:[{key,schema,name,cols:[{name,type}],pk:[colName],rows:Number}] }

   Usage:
     const model = SubjectMap.build(source, target);
     model.concepts   // the spine: entity concepts + the key columns on each side
     model.areas      // business-area rollup, source vs target volume
     model.queue      // source tables ranked by rows at stake, with candidates
     SubjectMap.accept(model, srcKey, tgtKey)   // record a decision (re-ranks)
   ───────────────────────────────────────────────────────────────────────────*/
(function (g) {
'use strict';

/* 1 ── domain vocabulary ─────────────────────────────────────────────────────
   Extend RAW for a new vertical; nothing else in the engine is domain-aware. */
const RAW = {
  matter:['matter','matt','mttr','mtr','mat','case','engagement'],
  client:['client','clnt','clno','clnum','cl','cust','customer','clien'],
  timekeeper:['timekeeper','tkpr','tkinit','tkid','tk','ttk','atty','attorney','lawyer','feeearner','biller'],
  employee:['employee','empl','emp','staff','person','people','hr','payroll'],
  invoice:['invoice','invc','inv','bill','billing','billed','proforma','statement'],
  wip:['wip','unbilled','workinprogress'],
  time:['time','hrs','hour','hours','worked','workhrs','tmcd','timecard','duration','elapsed'],
  receivable:['receivable','recv','receiv','ar','aged','aging','ageing'],
  payable:['payable','ap','vendor','voucher','vchr','vo','supplier'],
  cash:['cash','receipt','receipts','remit','remittance','deposit'],
  check:['check','cheque','chk','ck','draft'],
  ledger:['ledger','ledg','gl','posting','post','chart'],
  journal:['journal','journ','jrnl','gj','mj','entry'],
  account:['account','acct','acc','glnum','coa'],
  budget:['budget','bud','forecast','target'],
  tax:['tax','vat','gst','withholding','1099'],
  currency:['currency','curr','fx','exch','exchange','euro'],
  rate:['rate','rates','tariff','pricing','price'],
  cost:['cost','costs','cp','expense','exp'],
  disbursement:['disbursement','disb','disbs','cdisb','outlay'],
  trust:['trust','escrow','iolta','clientmoney'],
  document:['document','doc','docs','dms','file','attach','attachment','image','folder'],
  narrative:['narrative','narr','note','notes','memo','comment','desc','description'],
  address:['address','addr','street','city','state','zip','postal','county','country'],
  phone:['phone','tel','telephone','fax','mobile'],
  email:['email','mail','smtp'],
  contact:['contact','relationship','rel'],
  office:['office','loc','location','branch','site','region'],
  department:['department','dept','division','div','unit'],
  practice:['practice','prac','specialty','discipline'],
  payment:['payment','pay','pmt','settle','settlement'],
  batch:['batch','bt','run','job'],
  period:['period','fiscal','fy','year','yr','month','mo','quarter','qtr','week','wk','calendar'],
  security:['security','sec','user','usr','login','logon','permission','perm','role','group','access'],
  audit:['audit','log','history','hist','archive','trail','changelog'],
  template:['template','tmpl','format','fmt','layout','style','design'],
  report:['report','rpt','rept','output','extract'],
  reference:['reference','ref','code','type','category','cat','class','lookup','list','status'],
  language:['language','lang','translation','locale','culture'],
  staging:['staging','stg','conversion','conv','cv','temp','tmp','tempo','test','bak','backup','old','scratch','load'],
  transaction:['transaction','tran','trans','txn','trn'],
  entity:['entity','org','organisation','organization','company','firm','comp','corp'],
  project:['project','proj','task','phase','activity','event'],
  integration:['integration','interface','import','export','feed','sync','api'],
};
/* structural morphemes: they shape a name but carry no subject meaning */
const GENERIC = new Set(('id no num uno index idx key seq ord line row date dt tm stamp ts name nm first last mid '
 +'flag is has amt amount total sum qty value val text sort level parent child link map detail det head hdr master '
 +'main new all def default data info misc other field col column pct percent from to start end open close begin '
 +'final print show use used').split(' '));

/* concept → business area (presentation only; matching never uses it) */
const AREA = {
  matter:'Matters & clients', client:'Matters & clients', contact:'Matters & clients',
  address:'Matters & clients', entity:'Matters & clients', phone:'Matters & clients', email:'Matters & clients',
  timekeeper:'People & timekeepers', employee:'People & timekeepers', security:'People & timekeepers',
  time:'Time & WIP', wip:'Time & WIP', project:'Time & WIP',
  invoice:'Billing & AR', receivable:'Billing & AR', payment:'Billing & AR', cash:'Billing & AR', rate:'Billing & AR',
  ledger:'Finance & ledger', journal:'Finance & ledger', account:'Finance & ledger', budget:'Finance & ledger',
  payable:'Finance & ledger', check:'Finance & ledger', cost:'Finance & ledger', disbursement:'Finance & ledger',
  trust:'Finance & ledger', tax:'Finance & ledger', currency:'Finance & ledger', transaction:'Finance & ledger',
  document:'Documents & content', narrative:'Documents & content', template:'Documents & content', report:'Documents & content',
  staging:'Staging & conversion', integration:'Staging & conversion', batch:'Staging & conversion',
  audit:'System & audit',
  period:'Reference & lookup', reference:'Reference & lookup', language:'Reference & lookup',
  office:'Reference & lookup', department:'Reference & lookup', practice:'Reference & lookup',
};
/* concepts too weak to name a table on their own */
const WEAK = new Set(['reference','period','language','template','report','narrative','staging','audit','batch','integration','security']);

const MORPH = new Map();
for (const [c, list] of Object.entries(RAW)) for (const m of list) if (!MORPH.has(m)) MORPH.set(m, c);
const VOCAB = [...MORPH.keys(), ...GENERIC].sort((a, b) => b.length - a.length);

/* 2 ── segmentation: split glued legacy identifiers into known morphemes ──── */
const segCache = new Map();
function segment(word) {
  if (segCache.has(word)) return segCache.get(word);
  const out = []; let i = 0;
  while (i < word.length) {
    let hit = null;
    for (const v of VOCAB) if (v.length <= word.length - i && word.startsWith(v, i)) { hit = v; break; }
    if (hit) { out.push(hit); i += hit.length; }
    else {
      let j = i + 1;
      while (j < word.length) { let any = false; for (const v of VOCAB) if (word.startsWith(v, j)) { any = true; break; } if (any) break; j++; }
      out.push(word.slice(i, j)); i = j;
    }
  }
  segCache.set(word, out); return out;
}
const idents = s => String(s || '').replace(/([a-z0-9])([A-Z])/g, '$1 $2').toLowerCase()
  .split(/[^a-z0-9]+/).filter(Boolean).flatMap(w => segment(w.replace(/\d+/g, '')));
const conceptsOf = s => idents(s).map(m => MORPH.get(m)).filter(Boolean);

/* 3 ── table profile: weighted concept bag (name ×3, PK ×2, column ×1) ────── */
function profile(t) {
  const bag = new Map(), add = (c, w) => bag.set(c, (bag.get(c) || 0) + w);
  conceptsOf(t.name).forEach(c => add(c, 3));
  t.cols.forEach(col => conceptsOf(col.name).forEach(c => add(c, 1)));
  (t.pk || []).forEach(col => conceptsOf(col).forEach(c => add(c, 2)));
  t.concepts = bag;
  const e = [...bag.entries()].sort((a, b) => b[1] - a[1]);
  const strong = e.filter(x => !WEAK.has(x[0]));
  t.primary = (strong[0] || e[0] || [null])[0];
  t.area = t.primary ? (AREA[t.primary] || 'Other') : 'Unclassified';
  // Reset on every profile: build() mutates the caller's table objects, and a
  // stale 'user' mark from a previous build with different overrides would
  // otherwise survive into a rebuild that no longer pins this table.
  t.areaSource = 'derived';
  return t;
}

/* 4 ── entity hubs: identifier columns that recur and act as keys ─────────── */
function hubs(tables) {
  const df = new Map(), pk = new Map();
  for (const t of tables) {
    for (const c of new Set(t.cols.map(c => c.name.toLowerCase()))) df.set(c, (df.get(c) || 0) + 1);
    for (const c of (t.pk || [])) pk.set(c.toLowerCase(), (pk.get(c.toLowerCase()) || 0) + 1);
  }
  const N = tables.length, out = [];
  for (const [col, n] of df) {
    if (n < 3 || n > N * 0.3) continue;                      // neither unique nor ubiquitous
    if (!(/(uno|id|no|num|index|idx|key|init|code)$/.test(col) || pk.has(col))) continue;
    const cs = conceptsOf(col).filter(c => c !== 'reference');
    if (!cs.length) continue;
    out.push({ col, concept: cs[0], tables: n, isPk: pk.get(col) || 0, weight: n * Math.log(N / n) });
  }
  return out.sort((a, b) => b.weight - a.weight);
}

/* 5 ── candidate scoring ─────────────────────────────────────────────────── */
const DECOY = /(_?template|_?draft|_?backup|_?bak\b|_?old\b|view$|^avr|_test|_tmp|_temp)/i;
function trigrams(s) { const x = '  ' + s.toLowerCase().replace(/[^a-z0-9]/g, '') + '  ', o = new Set();
  for (let i = 0; i < x.length - 2; i++) o.add(x.slice(i, i + 3)); return o; }
function dice(a, b) { let h = 0; for (const gr of a) if (b.has(gr)) h++; return (2 * h) / (a.size + b.size || 1); }

const W = { concept: 0.55, name: 0.20, cols: 0.15, rows: 0.10, exactBonus: 0.18, decoyPenalty: 0.10 };

function score(s, t, cos) {
  const nameSim = dice(s._tri, t._tri);
  const colAff = 1 - Math.min(1, Math.abs(Math.log2((s.cols.length + 1) / (t.cols.length + 1))) / 3);
  const rowAff = 1 - Math.min(1, Math.abs(Math.log10(1 + s.rows) - Math.log10(1 + t.rows)) / 4);
  const exact = s.name.toLowerCase() === t.name.toLowerCase() ? 1 : 0;
  const decoy = DECOY.test(t.name) ? 1 : 0;
  const base = W.concept * cos + W.name * nameSim + W.cols * colAff + W.rows * rowAff;
  return { adj: base + W.exactBonus * exact - W.decoyPenalty * decoy, cos, nameSim, colAff, rowAff, exact, decoy };
}
/* Conservative on purpose: a confident wrong mapping costs more than a flagged one. */
function tier(e) {
  if (!e) return 'none';
  if ((e.exact && e.cos >= 0.35) || (e.cos >= 0.60 && e.nameSim >= 0.45)) return 'strong';
  if (e.cos >= 0.50 && (e.nameSim >= 0.25 || e.rowAff >= 0.85)) return 'likely';
  if (e.cos >= 0.40) return 'possible';
  return 'weak';
}

/* 6 ── build ─────────────────────────────────────────────────────────────
   Restructured into stages so build() (synchronous, for tests and small
   estates) and buildAsync() (chunked, so a 14k-table estate never freezes
   the page) run EXACTLY the same code. The stages share no hidden state:
   everything travels in the ctx object. */

// Bump when scoring, vocabulary or segmentation changes — cached models are
// keyed on it so a stale build can never masquerade as the current engine.
const ENGINE_VERSION = 1;

/* A manual area assignment (the console's existing override path) is a fact,
   not an estimate: it is applied after profiling and never re-derived. */
function applyOverrides(tables, overrides) {
  if (!overrides) return;
  for (const t of tables) {
    const o = overrides[String(t.key || '').toLowerCase()];
    if (o && o !== 'oos') { t.area = o; t.areaSource = 'user'; }
  }
}

function prepare(source, target, opts) {
  const src = source.tables.map(profile), tgt = target.tables.map(profile);
  applyOverrides(src, opts.srcOverrides);
  applyOverrides(tgt, opts.tgtOverrides);
  [...src, ...tgt].forEach(t => { t._tri = trigrams(t.name); });

  const all = src.concat(tgt), cdf = new Map();
  for (const t of all) for (const c of t.concepts.keys()) cdf.set(c, (cdf.get(c) || 0) + 1);
  const cidf = c => Math.log(all.length / (1 + (cdf.get(c) || 0)));
  for (const t of all) {
    let n = 0; t._v = new Map();
    for (const [c, w] of t.concepts) { const x = Math.log(1 + w) * cidf(c); t._v.set(c, x); n += x * x; }
    t._n = Math.sqrt(n) || 1;
  }
  const cos = (a, b) => { let d = 0; for (const [c, x] of a._v) { const y = b._v.get(c); if (y) d += x * y; } return d / (a._n * b._n); };

  /* candidates are searched inside the aligned area only — 12k targets → hundreds */
  const byArea = new Map(); for (const t of tgt) { let a = byArea.get(t.area); if (!a) byArea.set(t.area, a = []); a.push(t); }
  const nameIdx = new Map(); for (const t of tgt) { const k = t.name.toLowerCase(); let a = nameIdx.get(k); if (!a) nameIdx.set(k, a = []); a.push(t); }
  const floor = opts.cosFloor != null ? opts.cosFloor : 0.25;
  return { src, tgt, cos, byArea, nameIdx, floor, topN: opts.topN || 5 };
}

function alignSpine(ctx) {
  const hS = hubs(ctx.src), hT = hubs(ctx.tgt);
  const spine = new Map();
  for (const side of [['src', ctx.src, hS], ['tgt', ctx.tgt, hT]]) {
    const [k, tables, hh] = side;
    for (const t of tables) {
      const c = t.primary || 'none';
      let r = spine.get(c);
      if (!r) spine.set(c, r = { concept: c, area: AREA[c] || 'Other', srcTables: 0, srcRows: 0, tgtTables: 0, tgtRows: 0, srcKeys: [], tgtKeys: [] });
      if (k === 'src') { r.srcTables++; r.srcRows += t.rows; } else { r.tgtTables++; r.tgtRows += t.rows; }
    }
    for (const h of hh) { const r = spine.get(h.concept); if (r) (k === 'src' ? r.srcKeys : r.tgtKeys).push(h.col); }
  }
  for (const r of spine.values()) r.status = (r.srcTables && r.tgtTables) ? 'aligned' : (r.srcTables ? 'source only' : 'target only');
  return { hS, hT, spine };
}

function scoreOne(s, ctx) {
  const pool = ctx.byArea.get(s.area) || [], out = [];
  for (const t of pool) { const c = ctx.cos(s, t); if (c < ctx.floor) continue; out.push({ table: t, ev: score(s, t, c) }); }
  for (const t of (ctx.nameIdx.get(s.name.toLowerCase()) || []))   // name twins always considered
    if (!out.some(o => o.table === t)) out.push({ table: t, ev: score(s, t, ctx.cos(s, t)) });
  out.sort((a, b) => b.ev.adj - a.ev.adj);
  s.candidates = out.slice(0, ctx.topN);
  s.tier = tier(s.candidates[0] && s.candidates[0].ev);
  s.margin = s.candidates.length > 1 ? s.candidates[0].ev.adj - s.candidates[1].ev.adj : (s.candidates.length ? 1 : 0);
}

function assemble(ctx, spineParts) {
  const areas = new Map();
  for (const [side, list] of [['src', ctx.src], ['tgt', ctx.tgt]]) for (const t of list) {
    let a = areas.get(t.area);
    if (!a) areas.set(t.area, a = { area: t.area, srcTables: 0, srcRows: 0, tgtTables: 0, tgtRows: 0 });
    if (side === 'src') { a.srcTables++; a.srcRows += t.rows; } else { a.tgtTables++; a.tgtRows += t.rows; }
  }
  return {
    engineVersion: ENGINE_VERSION,
    src: ctx.src, tgt: ctx.tgt, hubs: { src: spineParts.hS, tgt: spineParts.hT },
    concepts: [...spineParts.spine.values()].sort((a, b) => b.srcRows - a.srcRows),
    areas: [...areas.values()].sort((a, b) => b.srcRows - a.srcRows),
    queue: ctx.src.slice().sort((a, b) => b.rows - a.rows),  // leverage order: rows at stake
    decisions: {},
  };
}

function build(source, target, opts) {
  opts = opts || {};
  const ctx = prepare(source, target, opts);
  const spineParts = alignSpine(ctx);
  for (const s of ctx.src) scoreOne(s, ctx);
  return assemble(ctx, spineParts);
}

/* Same pipeline, chunked. Profiling and scoring measured at ~7s and ~14s on
   a 2,545 × 12,245 estate — synchronously that freezes the console, so the
   scoring loop yields between chunks and reports determinate progress:
   onProgress({stage, done, total}). */
function buildAsync(source, target, opts, onProgress) {
  opts = opts || {};
  const tick = () => new Promise(r => setTimeout(r, 0));
  const say = (stage, done, total) => { try { if (onProgress) onProgress({ stage, done, total }); } catch (e) {} };
  const CHUNK = opts.chunk || 80;
  return (async () => {
    say('profiling', 0, 1);
    await tick();
    const ctx = prepare(source, target, opts);
    say('hubs', 0, 1);
    await tick();
    const spineParts = alignSpine(ctx);
    for (let i = 0; i < ctx.src.length; i += CHUNK) {
      for (let j = i; j < Math.min(i + CHUNK, ctx.src.length); j++) scoreOne(ctx.src[j], ctx);
      say('scoring', Math.min(i + CHUNK, ctx.src.length), ctx.src.length);
      await tick();
    }
    say('done', ctx.src.length, ctx.src.length);
    return assemble(ctx, spineParts);
  })();
}

/* 7 ── decisions. Accepting a pair is evidence: it locks the mapping and
   promotes same-concept siblings of the accepted target in later ranking. */
function accept(model, srcKey, tgtKey) {
  model.decisions[srcKey] = { target: tgtKey, at: Date.now(), by: 'analyst' };
  const t = model.tgt.find(x => x.key === tgtKey);
  if (t) for (const s of model.src) {
    if (model.decisions[s.key] || s.primary !== t.primary) continue;
    for (const c of s.candidates) if (c.table.schema === t.schema) c.ev.adj += 0.02;   // gentle nudge
    s.candidates.sort((a, b) => b.ev.adj - a.ev.adj);
  }
  return model;
}
function reject(model, srcKey) { model.decisions[srcKey] = { target: null, at: Date.now(), by: 'analyst' }; return model; }

/* coverage that is honest: rows whose destination a human has confirmed */
function coverage(model) {
  const total = model.src.reduce((a, t) => a + t.rows, 0);
  const decided = model.src.filter(t => model.decisions[t.key] && model.decisions[t.key].target)
                           .reduce((a, t) => a + t.rows, 0);
  const classified = model.src.filter(t => t.area !== 'Unclassified').reduce((a, t) => a + t.rows, 0);
  return { total, decided, classified, pctDecided: decided / total, pctClassified: classified / total };
}

const api = { build, buildAsync, accept, reject, coverage, segment, conceptsOf, hubs, profile, tier, AREA, RAW, W, ENGINE_VERSION };
g.SubjectMap = api;
if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
