// agent-dependency-planner.js
//
// POST   /api/agent/plan                          — generate a new plan
// GET    /api/agent/plan/{planId}                 — fetch overview
// GET    /api/agent/plan/{planId}/level/{level}   — fetch one level's detail
// DELETE /api/agent/plan/{planId}                 — delete plan + all levels
// GET    /api/agent/plan                          — list plans for project
//
// Computes a deterministic load order for migrating an entire source schema.
// Pure JS — no LLM. The agent's mapping phase consumes the approved plan;
// this module's job is to figure out what goes before what.
//
// Spec: docs/agent-dependency-planner-spec.md
// Validation: docs/agent-dependency-planner-validation.md
//
// Cosmos containers:
//   agent_plans         partitioned on /userId   — one overview doc per plan
//   agent_plan_levels   partitioned on /planId   — N detail docs per plan
//
// Auth: x-user-id header is the source of truth for userId. Client-supplied
// values on documents are ignored, same pattern as the other agent modules.

const { app } = require('@azure/functions');

// ── CORS ────────────────────────────────────────────────────────────────────
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, x-user-id',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
  'Content-Type':                 'application/json'
};

const ok  = (body)             => ({ status: 200, headers: CORS, body: JSON.stringify(body) });
const err = (code, msg, stack) => ({
  status: code,
  headers: CORS,
  body: JSON.stringify({ error: msg, ...(stack ? { stack } : {}) })
});

// ── Cosmos client (lazy singleton, same pattern as other modules) ──────────
let _cosmos = null;
function getContainer(name) {
  if (!_cosmos) {
    const { CosmosClient } = require('@azure/cosmos');
    _cosmos = new CosmosClient({
      endpoint: process.env.COSMOS_ENDPOINT,
      key:      process.env.COSMOS_KEY
    });
  }
  return _cosmos
    .database(process.env.COSMOS_DATABASE || 'cygenix')
    .container(name);
}

// ── Helpers ─────────────────────────────────────────────────────────────────
function getUserId(req) {
  return req.headers.get('x-user-id') || req.query.get('userId') || null;
}

function nowIso() { return new Date().toISOString(); }

function shortId(prefix) {
  const crypto = require('crypto');
  return `${prefix}_${crypto.randomBytes(6).toString('hex')}`;
}

function sha256(s) {
  const crypto = require('crypto');
  return crypto.createHash('sha256').update(String(s)).digest('hex');
}

// ── Noise pattern defaults ──────────────────────────────────────────────────
// Tables matching any of these are flagged isNoise: true but still included
// in the plan. The user can filter them out in the UI; we never silently
// drop tables here. See spec §8.
const DEFAULT_NOISE_PATTERNS = [
  /_bak$/i, /_backup$/i, /_old$/i, /_tmp$/i, /_temp$/i, /_copy$/i,
  /^tmp_/i, /^temp_/i,
];

// ── Soft-reference column patterns ──────────────────────────────────────────
// Columns matching these look like FKs but are "stamped by" rather than
// load-order constraints. We tag the edges they produce as softReference and
// the topological sort ignores them. See spec §5.4.
const SOFT_REFERENCE_PATTERNS = [
  /_by$/i,        // created_by, modified_by, updated_by, deleted_by
  /_seq$/i,       // sequence numbers
  /_no$/i,        // numeric counters (but careful: phone_no isn't a counter; we accept the false positive — phone_no won't match a PK anyway)
  /_version$/i,
];

function isSoftReferenceCol(colName) {
  if (!colName) return false;
  return SOFT_REFERENCE_PATTERNS.some(rx => rx.test(colName));
}

// ── Inference engine ───────────────────────────────────────────────────────
//
// Two patterns supported:
//   Suffix:  col_name ending _id|_num|_code → look for table whose PK has the
//            same base name, OR whose table name contains the base.
//   Prefix:  col_name starts with another table's column-prefix (e.g. mtr_id
//            references arc_matter, which has all columns prefixed mtr_).
//
// Confidence scoring per spec §5.3.
//
// Inputs to inferEdges:
//   tables: [{ schema, name, fullName, rows, cols: [{name, dataType}], colNames }]
//   declaredEdgeKeys: Set of "from.fullName -> to.fullName" already declared
//   tablesByFullName: Map fullName -> table (used as PK lookup)
//   pkByBaseName: Map "base" -> [table objects whose PK column name endsWith _id/_num/_code with that base]
//   prefixByPrefix: Map "mtr_" -> table whose columns share that prefix
//
// Returns: array of { from, to, via, declared:false, confidence, ambiguous?, candidates? }

function buildInferenceIndices(tables) {
  // PK lookup: for each table, identify its likely PK column(s). We have no
  // sys.indexes data here, just column names — so we use heuristics:
  //   - column named exactly "id"
  //   - column named "<tablename>_id" or "<prefix>id"
  //   - first column ending in _id (fallback)
  // pkByBaseName maps "client" -> [tables where 'client_id' is a PK candidate]
  const pkByBaseName = new Map();

  // For prefix inference, find each table's column prefix (e.g. "mtr_" for arc_matter).
  // A prefix is the longest common prefix shared by at least 2 columns and
  // ending in "_" — that's what Elite-style schemas look like.
  const tablePrefix = new Map();   // fullName -> "mtr_"
  const prefixOwners = new Map();  // "mtr_" -> [fullNames]

  for (const t of tables) {
    const colNames = (t.colNames || []).map(c => String(c).toLowerCase());

    // Detect this table's column prefix first — it's the strongest identity
    // signal in Elite-style schemas. arc_matter's columns are mtr_id, mtr_status,
    // mtr_open_date — its prefix is "mtr_", and its identity-base is "mtr".
    // Detection requires ≥3 columns sharing the prefix AND ≥50% of all cols,
    // so junction tables (with mixed prefixes like mtp_id, mtr_id, client_id)
    // return null here — they get no identity-base, which is correct.
    const prefix = detectColumnPrefix(colNames);
    if (prefix) {
      tablePrefix.set(t.fullName, prefix);
      if (!prefixOwners.has(prefix)) prefixOwners.set(prefix, []);
      prefixOwners.get(prefix).push(t.fullName);
    }
    const prefixBase = prefix ? prefix.replace(/_$/, '') : null;

    // PK heuristic.
    //
    // Priority 1: column prefix. If the table's prefix is "mtr_" and one of
    // its columns is "mtr_id", that's the PK and "mtr" is the identity-base.
    //
    // Priority 2: a single id-like column whose base relates to the table
    // name. Take only ONE base per table here — the first id-like column
    // whose base appears in the (possibly prefix-stripped) table name. This
    // prevents junction tables (mtr_party with cols mtp_id/mtr_id/client_id)
    // from claiming PK-ownership of every base their FK columns name. The
    // junction's own PK (mtp_id) wins; the others are correctly seen as FKs.
    const tName = (t.name || '').toLowerCase();
    const stripped = tName.replace(/^(arc_|arcs?_|cv_|t_|wv_|cm_|hb|hbm|wt_)/i, '');

    const pkCandidates = new Set();

    // Priority 1: prefix-based PK.
    if (prefixBase) {
      for (const c of colNames) {
        if (c === `${prefixBase}_id` || c === `${prefixBase}_num` || c === `${prefixBase}_code`) {
          pkCandidates.add(prefixBase);
          break;
        }
      }
    }

    // Priority 2: walk id-like columns in order; the first column whose base
    // appears at the START of the (possibly prefix-stripped) table name claims
    // identity. "Contains" was too loose — mtr_party (a junction) "contains"
    // mtr but is not the entity table for mtr. "Starts with" is the correct
    // signal: client_id on `client` and `clientload` both claim 'client' as
    // identity-base (both start with client → user gets to disambiguate via
    // the ambiguous-column flag), but mtr_id on mtr_party does NOT claim mtr
    // (mtr_party doesn't start with 'mtr_id' — it starts with 'mtr_' but the
    // word continues with 'party', not with the bare base).
    if (pkCandidates.size === 0) {
      for (const c of colNames) {
        if (c === 'id') {
          if (tName) pkCandidates.add(tName);
          if (stripped && stripped !== tName) pkCandidates.add(stripped);
          break;
        }
        const m = c.match(/^([a-z][a-z0-9_]*)_(id|num|code)$/i);
        if (m) {
          const base = m[1];
          // Strict "leading word" test:
          //   - the (stripped) table name equals the base, OR
          //   - the (stripped) table name starts with `base` followed by end
          //     of string (i.e. exact match — handled above) — we explicitly
          //     don't accept `base_*` because that's a junction-style name
          //     ("mtr_party" should not claim "mtr"; "clientload" claims
          //     "client" because clientload starts with client and 'load' is
          //     a non-underscore continuation).
          const baseMatchesTable =
            stripped === base ||
            tName === base ||
            (stripped.startsWith(base) && !stripped.startsWith(base + '_')) ||
            (tName.startsWith(base)    && !tName.startsWith(base + '_'));
          if (baseMatchesTable) {
            pkCandidates.add(base);
            break;
          }
        }
      }
    }

    for (const base of pkCandidates) {
      if (!base || base.length < 2) continue;
      if (!pkByBaseName.has(base)) pkByBaseName.set(base, []);
      pkByBaseName.get(base).push(t);
    }
  }

  return { pkByBaseName, tablePrefix, prefixOwners };
}

function detectColumnPrefix(colNames) {
  if (!colNames || colNames.length < 2) return null;
  // Group columns by their prefix-up-to-first-underscore.
  const counts = new Map();
  for (const c of colNames) {
    const m = c.match(/^([a-z]{2,8}_)/i);
    if (m) counts.set(m[1], (counts.get(m[1]) || 0) + 1);
  }
  // Pick a prefix that covers at least half the columns AND at least 3 columns.
  // Avoids picking accidental prefixes from 1-2 columns that happen to share.
  const total = colNames.length;
  let best = null;
  let bestCount = 0;
  for (const [pfx, n] of counts) {
    if (n >= 3 && n / total >= 0.5 && n > bestCount) {
      best = pfx;
      bestCount = n;
    }
  }
  return best;
}

function inferEdges(tables, declaredEdgeKeys, indices) {
  const { pkByBaseName, tablePrefix, prefixOwners } = indices;
  const inferred = [];

  for (const t of tables) {
    const tCols = t.colNames || [];
    for (const colName of tCols) {
      const cLower = String(colName).toLowerCase();

      // Skip soft references entirely — they get tagged on output for
      // completeness but contribute no candidates to the FK graph.
      const isSoft = isSoftReferenceCol(cLower);

      // SUFFIX PATTERN
      // Column ends _id/_num/_code; look up PK base name.
      let suffixCandidates = [];
      const suffixMatch = cLower.match(/^(.+)_(id|num|code)$/);
      if (suffixMatch) {
        const base = suffixMatch[1];
        if (base.length >= 2 && pkByBaseName.has(base)) {
          for (const target of pkByBaseName.get(base)) {
            if (target.fullName === t.fullName) continue;  // self-reference handled separately
            suffixCandidates.push({
              targetFullName: target.fullName,
              targetRows:     target.rows || 0,
              source:         'suffix',
              base
            });
          }
        }
      }

      // PREFIX PATTERN
      // Column starts with a prefix that owns columns in another table.
      // The column must ALSO end in _id/_num/_code — otherwise we'd treat
      // every prefix-conforming column (e.g. mtr_status, mtr_open_date) as
      // an FK to its prefix-owning table, which is wrong: those are data
      // columns, not foreign keys.
      let prefixCandidates = [];
      const isIdSuffixed = /_(id|num|code)$/i.test(cLower);
      const pfxMatch = isIdSuffixed ? cLower.match(/^([a-z]{2,8}_)/i) : null;
      if (pfxMatch) {
        const pfx = pfxMatch[1];
        if (prefixOwners.has(pfx)) {
          for (const ownerFullName of prefixOwners.get(pfx)) {
            if (ownerFullName === t.fullName) continue;
            // Only count this owner if its OWN prefix is pfx (not just one column starting with it)
            if (tablePrefix.get(ownerFullName) !== pfx) continue;
            // And only if the owner has a PK-like column matching this one
            // (e.g. mtr_id column on the FK side -> arc_matter must have an mtr_id column)
            const ownerTable = tables.find(x => x.fullName === ownerFullName);
            if (!ownerTable) continue;
            const ownerHasCol = (ownerTable.colNames || []).some(c => c.toLowerCase() === cLower);
            if (!ownerHasCol) continue;
            prefixCandidates.push({
              targetFullName: ownerTable.fullName,
              targetRows:     ownerTable.rows || 0,
              source:         'prefix',
              base:           pfx
            });
          }
        }
      }

      // Merge candidates; dedupe by targetFullName.
      const allByTarget = new Map();
      for (const c of [...suffixCandidates, ...prefixCandidates]) {
        if (!allByTarget.has(c.targetFullName)) {
          allByTarget.set(c.targetFullName, { ...c, sources: [c.source] });
        } else {
          allByTarget.get(c.targetFullName).sources.push(c.source);
        }
      }
      if (allByTarget.size === 0) continue;

      // Score each candidate
      const scored = [];
      for (const cand of allByTarget.values()) {
        let conf = 0.7;
        if (cand.sources.length >= 2) conf = 0.9;  // both patterns agree
        // Tiny bonus for target tables that have rows (suggests real, alive table)
        if (cand.targetRows > 0) conf += 0.02;
        // Small penalty if the target rows are very low — might be defunct
        if (cand.targetRows === 0) conf -= 0.05;
        scored.push({ ...cand, confidence: Math.min(0.95, Math.max(0, conf)) });
      }

      // If the column is already covered by a declared edge to one of these targets,
      // drop the inferred candidate for that target — declared wins.
      const remaining = scored.filter(c => {
        const key = `${t.fullName}->${c.targetFullName}`;
        return !declaredEdgeKeys.has(key);
      });
      if (remaining.length === 0) continue;

      // Soft references: still record them in the output but mark them soft.
      // Topological sort will ignore them.
      if (remaining.length === 1) {
        // Single unambiguous candidate
        const c = remaining[0];
        if (c.confidence >= 0.5) {
          inferred.push({
            from:           t.fullName,
            to:             c.targetFullName,
            via:            colName,
            declared:       false,
            confidence:     c.confidence,
            softReference:  isSoft,
            ambiguous:      false
          });
        }
      } else {
        // Multiple candidates — flag ambiguous. Do NOT add to graph; let the
        // user resolve during plan review. Surface in ambiguousColumns on the
        // table doc instead.
        inferred.push({
          from:         t.fullName,
          to:           null,   // unresolved
          via:          colName,
          declared:     false,
          softReference: isSoft,
          ambiguous:    true,
          candidates:   remaining.map(c => ({
            table:      c.targetFullName,
            rows:       c.targetRows,
            confidence: c.confidence
          })).sort((a, b) => b.confidence - a.confidence)
        });
      }
    }
  }

  return inferred;
}

// ── Topological sort (Kahn's algorithm) ─────────────────────────────────────
// Operates on a directed graph where an edge from A → B means "A depends on B"
// (A must load AFTER B). Returns levels: an array of arrays, where level[i]
// contains tables whose deepest dependency is at level i-1.
//
// Tables in cycles are left out of `levels` and returned in `cyclical`.
function topoSortByLevel(allNodes, edges) {
  // Build adjacency: dependsOn[X] = Set of nodes X depends on
  const dependsOn = new Map();
  const dependedOnBy = new Map();
  for (const n of allNodes) {
    dependsOn.set(n, new Set());
    dependedOnBy.set(n, new Set());
  }
  for (const e of edges) {
    if (!dependsOn.has(e.from) || !dependsOn.has(e.to)) continue;
    if (e.from === e.to) continue; // self-loops can't be sorted; treat as cycle separately
    dependsOn.get(e.from).add(e.to);
    dependedOnBy.get(e.to).add(e.from);
  }

  // Kahn: repeatedly pluck nodes with zero outgoing dependencies into the next level.
  const levels = [];
  const placed = new Set();
  while (true) {
    const thisLevel = [];
    for (const n of allNodes) {
      if (placed.has(n)) continue;
      const deps = dependsOn.get(n);
      // A node belongs in this level if every dep it has is already placed.
      let canPlace = true;
      for (const d of deps) {
        if (!placed.has(d)) { canPlace = false; break; }
      }
      if (canPlace) thisLevel.push(n);
    }
    if (thisLevel.length === 0) break;
    for (const n of thisLevel) placed.add(n);
    levels.push(thisLevel);
  }

  // Anything not placed is in a cycle (or transitively depends on one).
  const cyclical = allNodes.filter(n => !placed.has(n));
  return { levels, cyclical };
}

// ── Tarjan's SCC for cycle detection ────────────────────────────────────────
// Standard algorithm. Returns an array of arrays (each inner array is one SCC).
// SCCs of size > 1 are cycles; we ignore singletons (those are just regular nodes).
function findCycles(allNodes, edges) {
  const adj = new Map();
  for (const n of allNodes) adj.set(n, []);
  for (const e of edges) {
    if (!adj.has(e.from) || !adj.has(e.to)) continue;
    if (e.from === e.to) continue;
    adj.get(e.from).push(e.to);
  }

  let idx = 0;
  const indexOf = new Map();
  const lowlink = new Map();
  const onStack = new Set();
  const stack = [];
  const sccs = [];

  function strongConnect(v) {
    indexOf.set(v, idx);
    lowlink.set(v, idx);
    idx++;
    stack.push(v);
    onStack.add(v);

    for (const w of adj.get(v) || []) {
      if (!indexOf.has(w)) {
        strongConnect(w);
        lowlink.set(v, Math.min(lowlink.get(v), lowlink.get(w)));
      } else if (onStack.has(w)) {
        lowlink.set(v, Math.min(lowlink.get(v), indexOf.get(w)));
      }
    }

    if (lowlink.get(v) === indexOf.get(v)) {
      const scc = [];
      let w;
      do {
        w = stack.pop();
        onStack.delete(w);
        scc.push(w);
      } while (w !== v);
      if (scc.length > 1) sccs.push(scc);
    }
  }

  for (const n of allNodes) {
    if (!indexOf.has(n)) strongConnect(n);
  }

  return sccs;
}

// ── Cycle resolution ────────────────────────────────────────────────────────
// For each cycle, propose ONE edge to break. Preference order:
//   1. Inferred edge over declared.
//   2. Lower confidence.
//   3. Edge whose 'via' column appears nullable (we don't have nullability info
//      so we approximate by saying inferred edges are presumed nullable —
//      they're rarely PK columns).
// Returns: { tables: [...], edges: [{from, to, via, declared, confidence}],
//            proposedResolution: { break: {...}, strategy: 'load-null-then-update' | ... } }
function resolveCycle(sccNodes, allEdges) {
  // Find all edges within this SCC
  const sccSet = new Set(sccNodes);
  const cycleEdges = allEdges.filter(e =>
    sccSet.has(e.from) && sccSet.has(e.to)
  );

  // Pick the break edge per preference rules
  const sorted = [...cycleEdges].sort((a, b) => {
    if (a.declared !== b.declared) return a.declared ? 1 : -1;  // inferred first
    return (a.confidence || 1) - (b.confidence || 1);            // lower confidence first
  });
  const breakEdge = sorted[0];

  let strategy, rationale;
  if (!breakEdge) {
    strategy = 'manual-required';
    rationale = 'Cycle detected but no edges found to break (this should not happen — please report).';
  } else if (!breakEdge.declared) {
    strategy = 'load-null-then-update';
    rationale = `Break the inferred edge (confidence ${(breakEdge.confidence || 0).toFixed(2)}, lower than the declared edge in this cycle). Load ${breakEdge.from} with ${breakEdge.via} = NULL, then update after ${breakEdge.to} is loaded.`;
  } else if (cycleEdges.every(e => e.declared)) {
    strategy = 'defer-constraint';
    rationale = 'All edges in this cycle are declared foreign keys. Disable the chosen FK constraint, load both tables, then re-enable.';
  } else {
    strategy = 'load-null-then-update';
    rationale = `Break the lowest-confidence edge in the cycle. Load ${breakEdge.from} with ${breakEdge.via} = NULL, then update.`;
  }

  return {
    tables: sccNodes,
    edges:  cycleEdges,
    proposedResolution: {
      break: breakEdge ? { from: breakEdge.from, to: breakEdge.to, via: breakEdge.via } : null,
      strategy,
      rationale
    }
  };
}

// ── Theme classification helper ─────────────────────────────────────────────
// We don't re-classify (agent-source-schema does that). We just accept the
// theme from the input if provided; otherwise leave it as 'other'.
function getTheme(t) {
  return t.theme || 'other';
}

// ── Noise detection ─────────────────────────────────────────────────────────
function isNoiseTable(name, noisePatterns) {
  if (!name) return false;
  return noisePatterns.some(rx => rx.test(name));
}

function compileNoisePatterns(input) {
  if (!Array.isArray(input)) return DEFAULT_NOISE_PATTERNS;
  const out = [];
  for (const pat of input) {
    try { out.push(new RegExp(pat, 'i')); }
    catch (_e) { /* skip bad pattern */ }
  }
  return out.length ? out : DEFAULT_NOISE_PATTERNS;
}

// ── Main planner ────────────────────────────────────────────────────────────
//
// Takes a schema snapshot (tables + declared edges) and produces:
//   - overview document
//   - array of per-level documents
//
// All synchronous CPU work; no I/O. Caller handles Cosmos writes.
function generatePlan({ tables, edges, options, userId, projectId }) {
  const noisePatterns = compileNoisePatterns(options && options.noisePatterns);
  const inferenceEnabled = options ? (options.inferenceEnabled !== false) : true;

  // Normalize tables. Each table needs fullName for graph keys.
  const tablesNormalized = tables.map(t => ({
    schema:    t.schema || 'dbo',
    name:      t.name,
    fullName:  t.fullName || `${t.schema || 'dbo'}.${t.name}`,
    rows:      Number(t.rows || 0),
    colNames:  Array.isArray(t.colNames) ? t.colNames :
               Array.isArray(t.cols)     ? t.cols.map(c => c.name) : [],
    theme:     t.theme || 'other'
  }));

  const fullNames = tablesNormalized.map(t => t.fullName);
  const tablesByFullName = new Map(tablesNormalized.map(t => [t.fullName, t]));

  // Normalize declared edges. Caller may pass them without 'declared' flag.
  const declaredEdges = (Array.isArray(edges) ? edges : []).map(e => ({
    from:       e.from,
    to:         e.to,
    via:        e.via || null,
    declared:   true,
    confidence: 1.0
  }));
  const declaredEdgeKeys = new Set(declaredEdges.map(e => `${e.from}->${e.to}`));

  // Inferred edges
  let inferredEdges = [];
  let ambiguousEntries = [];
  if (inferenceEnabled) {
    const indices = buildInferenceIndices(tablesNormalized);
    const inferenceResult = inferEdges(tablesNormalized, declaredEdgeKeys, indices);
    for (const entry of inferenceResult) {
      if (entry.ambiguous) ambiguousEntries.push(entry);
      else inferredEdges.push(entry);
    }
  }

  // Edges used for topological sort: declared + inferred (non-soft, confidence ≥ 0.5).
  // Soft references appear in the output but don't constrain load order.
  const hardEdges = [
    ...declaredEdges,
    ...inferredEdges.filter(e => !e.softReference && e.confidence >= 0.5)
  ];

  // Cycle detection BEFORE topological sort — we need to know which nodes
  // belong to cycles so we can resolve them and place them at sensible levels.
  const sccs = findCycles(fullNames, hardEdges);
  const cyclicalNodes = new Set();
  const cycleResolutions = sccs.map(scc => {
    const resolution = resolveCycle(scc, hardEdges);
    for (const n of scc) cyclicalNodes.add(n);
    return resolution;
  });

  // For topological sort, remove the break edges so the graph becomes acyclic.
  const breakEdgeKeys = new Set();
  for (const c of cycleResolutions) {
    if (c.proposedResolution && c.proposedResolution.break) {
      const b = c.proposedResolution.break;
      breakEdgeKeys.add(`${b.from}->${b.to}`);
    }
  }
  const edgesForSort = hardEdges.filter(e => !breakEdgeKeys.has(`${e.from}->${e.to}`));

  // Topological sort
  const { levels, cyclical: stillCyclical } = topoSortByLevel(fullNames, edgesForSort);

  // Any node still cyclical after cycle resolution = the cycle resolution
  // didn't fully break things (e.g. multi-cycle interdependence). Place them
  // at the end as a synthetic "Level N+1: manual-required" level.
  let manualLevel = null;
  if (stillCyclical.length > 0) {
    manualLevel = stillCyclical;
  }

  // Build per-level table detail
  const planId = shortId('plan');
  const fingerprintInput = JSON.stringify({
    tables: tablesNormalized.map(t => ({ n: t.fullName, c: (t.colNames || []).length })),
    edges:  declaredEdges.map(e => `${e.from}->${e.to}`).sort()
  });
  const schemaFingerprint = sha256(fingerprintInput);

  // Build outgoing-edge index for level rendering
  const outgoingByTable = new Map();
  for (const e of [...hardEdges, ...inferredEdges.filter(x => x.softReference && !x.ambiguous)]) {
    if (!outgoingByTable.has(e.from)) outgoingByTable.set(e.from, []);
    outgoingByTable.get(e.from).push(e);
  }
  // Ambiguous entries indexed by source table
  const ambiguousByTable = new Map();
  for (const a of ambiguousEntries) {
    if (!ambiguousByTable.has(a.from)) ambiguousByTable.set(a.from, []);
    ambiguousByTable.get(a.from).push(a);
  }

  // Table level lookup (for annotating targetLevel on each edge)
  const levelByTable = new Map();
  levels.forEach((lvl, i) => { for (const n of lvl) levelByTable.set(n, i); });
  if (manualLevel) for (const n of manualLevel) levelByTable.set(n, levels.length);

  // Build level docs
  const levelDocs = [];
  const allLevels = manualLevel ? [...levels, manualLevel] : levels;
  for (let i = 0; i < allLevels.length; i++) {
    const tablesInLevel = allLevels[i].map(fullName => {
      const t = tablesByFullName.get(fullName);
      const outgoing = (outgoingByTable.get(fullName) || []).map(e => ({
        to:             e.to,
        via:            e.via,
        declared:       e.declared,
        confidence:     e.confidence,
        softReference:  !!e.softReference,
        targetLevel:    levelByTable.has(e.to) ? levelByTable.get(e.to) : null
      }));
      const ambiguousCols = (ambiguousByTable.get(fullName) || []).map(a => ({
        column:     a.via,
        candidates: a.candidates,
        resolution: 'unresolved'
      }));
      return {
        schema:           t.schema,
        name:             t.name,
        fullName:         t.fullName,
        rows:             t.rows,
        theme:            getTheme(t),
        isNoise:          isNoiseTable(t.name, noisePatterns),
        inCycle:          cyclicalNodes.has(fullName),
        outgoingEdges:    outgoing,
        ambiguousColumns: ambiguousCols
      };
    });
    levelDocs.push({
      id:      `${planId}_level_${i}`,
      planId,
      level:   i,
      isManualLevel: !!(manualLevel && i === allLevels.length - 1),
      tableCount: tablesInLevel.length,
      tables:  tablesInLevel
    });
  }

  // Identify isolated tables: zero outgoing AND zero incoming edges (hard
  // edges only — soft references don't count, neither does inferred ambiguity).
  const hasOutgoing = new Set(hardEdges.map(e => e.from));
  const hasIncoming = new Set(hardEdges.map(e => e.to));
  const isolatedTables = fullNames.filter(fn =>
    !hasOutgoing.has(fn) && !hasIncoming.has(fn)
  );

  // Noise tables
  const noiseTables = tablesNormalized
    .filter(t => isNoiseTable(t.name, noisePatterns))
    .map(t => t.fullName);

  // Theme breakdown per level (for the overview)
  const levelSummaries = allLevels.map((lvl, i) => {
    const themeBreakdown = {};
    for (const fn of lvl) {
      const theme = getTheme(tablesByFullName.get(fn));
      themeBreakdown[theme] = (themeBreakdown[theme] || 0) + 1;
    }
    const label = i === 0 ? 'No dependencies (reference/root data)' :
                  manualLevel && i === allLevels.length - 1 ? 'Manual review required (unresolved cycles)' :
                  `Depend only on Level 0..${i - 1}`;
    return { level: i, tableCount: lvl.length, label, themeBreakdown };
  });

  // Overview doc
  const overview = {
    id:             `${userId}_${planId}`,
    userId,
    projectId:      projectId || '',
    planId,
    createdAt:      nowIso(),
    schemaFingerprint,
    summary: {
      totalTables:          tablesNormalized.length,
      totalDeclaredEdges:   declaredEdges.length,
      totalInferredEdges:   inferredEdges.length,
      totalLevels:          allLevels.length,
      cycleCount:           cycleResolutions.length,
      isolatedTableCount:   isolatedTables.length,
      noiseTableCount:      noiseTables.length,
      ambiguousColumnCount: ambiguousEntries.length
    },
    levels:         levelSummaries,
    cycles:         cycleResolutions.map((c, i) => ({
      id:                 `cycle-${i + 1}`,
      tables:             c.tables,
      edges:              c.edges.map(e => ({
        from: e.from, to: e.to, via: e.via,
        declared: e.declared, confidence: e.confidence
      })),
      proposedResolution: c.proposedResolution
    })),
    noiseTables,
    isolatedTables
  };

  return { overview, levelDocs };
}

// ── Persist plan to Cosmos ──────────────────────────────────────────────────
async function persistPlan(overview, levelDocs, ctx) {
  // Write overview first
  await getContainer('agent_plans').items.upsert(overview);

  // Then per-level docs in parallel (bounded — Cosmos rate limit at low RU)
  const CHUNK = 4;
  for (let i = 0; i < levelDocs.length; i += CHUNK) {
    const slice = levelDocs.slice(i, i + CHUNK);
    await Promise.all(slice.map(doc =>
      getContainer('agent_plan_levels').items.upsert(doc)
        .catch(e => {
          ctx.log(`[planner] level upsert failed for ${doc.id}: ${e.message}`);
          throw e;
        })
    ));
  }
}

// ── HTTP handlers ──────────────────────────────────────────────────────────

// POST /api/agent/plan — generate a new plan
async function handlePost(req, ctx) {
  const userId = getUserId(req);
  if (!userId) return err(401, 'x-user-id header is required');

  let body;
  try { body = await req.json(); }
  catch { return err(400, 'request body must be valid JSON'); }

  if (!body || typeof body !== 'object') return err(400, 'request body must be a JSON object');
  if (!Array.isArray(body.tables) || body.tables.length === 0) {
    return err(400, 'tables array is required (the source schema). Call /api/agent/source-schema first and pass its tables and edges.');
  }

  const projectId = (body.projectId || '').trim();

  const t0 = Date.now();
  let plan;
  try {
    plan = generatePlan({
      tables:    body.tables,
      edges:     body.edges || [],
      options:   body.options || {},
      userId,
      projectId
    });
  } catch (e) {
    ctx.log(`[planner] generate failed: ${e.message}\n${e.stack || ''}`);
    return err(500, `Plan generation failed: ${e.message}`,
      (e.stack || '').split('\n').slice(0, 6).join('\n'));
  }
  const genMs = Date.now() - t0;
  ctx.log(`[planner] plan ${plan.overview.planId} generated in ${genMs}ms (${plan.overview.summary.totalTables} tables, ${plan.overview.summary.totalLevels} levels, ${plan.overview.summary.cycleCount} cycles)`);

  try {
    await persistPlan(plan.overview, plan.levelDocs, ctx);
  } catch (e) {
    ctx.log(`[planner] persist failed: ${e.message}`);
    return err(500, `Plan generated but storage failed: ${e.message}`,
      (e.stack || '').split('\n').slice(0, 6).join('\n'));
  }

  return ok({
    planId:    plan.overview.planId,
    overview:  plan.overview,
    levelCount: plan.levelDocs.length,
    generationMs: genMs
  });
}

// GET /api/agent/plan/{planId} — overview
async function handleGetOverview(req, ctx, planId) {
  const userId = getUserId(req);
  if (!userId) return err(401, 'x-user-id header is required');

  try {
    const { resource } = await getContainer('agent_plans')
      .item(`${userId}_${planId}`, userId).read();
    if (!resource) return err(404, `plan ${planId} not found`);
    return ok(resource);
  } catch (e) {
    if (e.code === 404) return err(404, `plan ${planId} not found`);
    ctx.log(`[planner] overview read failed: ${e.message}`);
    return err(500, e.message);
  }
}

// GET /api/agent/plan/{planId}/level/{level}
async function handleGetLevel(req, ctx, planId, level) {
  const userId = getUserId(req);
  if (!userId) return err(401, 'x-user-id header is required');

  // Confirm the plan belongs to this user before serving level data.
  // (agent_plan_levels is partitioned on /planId so it doesn't enforce userId
  // on its own.)
  try {
    const { resource: overview } = await getContainer('agent_plans')
      .item(`${userId}_${planId}`, userId).read();
    if (!overview) return err(404, `plan ${planId} not found`);
  } catch (e) {
    if (e.code === 404) return err(404, `plan ${planId} not found`);
    throw e;
  }

  const levelInt = parseInt(level, 10);
  if (Number.isNaN(levelInt) || levelInt < 0) return err(400, 'level must be a non-negative integer');

  try {
    const docId = `${planId}_level_${levelInt}`;
    const { resource } = await getContainer('agent_plan_levels')
      .item(docId, planId).read();
    if (!resource) return err(404, `level ${levelInt} not found for plan ${planId}`);
    return ok(resource);
  } catch (e) {
    if (e.code === 404) return err(404, `level ${level} not found for plan ${planId}`);
    ctx.log(`[planner] level read failed: ${e.message}`);
    return err(500, e.message);
  }
}

// DELETE /api/agent/plan/{planId}
async function handleDelete(req, ctx, planId) {
  const userId = getUserId(req);
  if (!userId) return err(401, 'x-user-id header is required');

  // Delete all level docs first (find them by partition key planId).
  let levelsDeleted = 0;
  try {
    const { resources } = await getContainer('agent_plan_levels').items
      .query({
        query: 'SELECT c.id FROM c WHERE c.planId = @pid',
        parameters: [{ name: '@pid', value: planId }]
      }, { partitionKey: planId })
      .fetchAll();
    for (const d of (resources || [])) {
      try {
        await getContainer('agent_plan_levels').item(d.id, planId).delete();
        levelsDeleted++;
      } catch (e) {
        ctx.log(`[planner] level delete failed for ${d.id}: ${e.message}`);
      }
    }
  } catch (e) {
    ctx.log(`[planner] levels listing failed: ${e.message}`);
  }

  // Then delete overview
  try {
    await getContainer('agent_plans').item(`${userId}_${planId}`, userId).delete();
  } catch (e) {
    if (e.code !== 404) {
      ctx.log(`[planner] overview delete failed: ${e.message}`);
      return err(500, e.message);
    }
  }

  return ok({ deleted: true, levelsDeleted });
}

// GET /api/agent/plan?projectId=...  — list plans
async function handleList(req, ctx) {
  const userId = getUserId(req);
  if (!userId) return err(401, 'x-user-id header is required');

  const projectId = req.query.get('projectId');

  let querySpec;
  if (projectId) {
    querySpec = {
      query: `SELECT c.planId, c.projectId, c.createdAt, c.summary, c.schemaFingerprint
              FROM c
              WHERE c.userId = @uid AND c.projectId = @pid
              ORDER BY c.createdAt DESC`,
      parameters: [
        { name: '@uid', value: userId },
        { name: '@pid', value: projectId }
      ]
    };
  } else {
    querySpec = {
      query: `SELECT c.planId, c.projectId, c.createdAt, c.summary, c.schemaFingerprint
              FROM c
              WHERE c.userId = @uid
              ORDER BY c.createdAt DESC`,
      parameters: [{ name: '@uid', value: userId }]
    };
  }

  try {
    const { resources } = await getContainer('agent_plans').items
      .query(querySpec, { partitionKey: userId })
      .fetchAll();
    return ok({ plans: resources || [] });
  } catch (e) {
    ctx.log(`[planner] list failed: ${e.message}`);
    return err(500, e.message);
  }
}

// ── Route registration ───────────────────────────────────────────────────────
// One route handles POST/GET/DELETE at the collection level (/agent/plan).
app.http('agent-plan', {
  methods:   ['GET', 'POST', 'OPTIONS'],
  authLevel: 'function',
  route:     'agent/plan',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 204, headers: CORS, body: '' };
    try {
      if (req.method === 'POST') return await handlePost(req, ctx);
      if (req.method === 'GET')  return await handleList(req, ctx);
      return err(405, `method ${req.method} not allowed on /agent/plan`);
    } catch (e) {
      ctx.log(`[planner] uncaught: ${e.message}\n${e.stack || ''}`);
      return err(500, e.message, (e.stack || '').split('\n').slice(0, 6).join('\n'));
    }
  }
});

// Per-plan routes: GET overview, DELETE plan.
app.http('agent-plan-item', {
  methods:   ['GET', 'DELETE', 'OPTIONS'],
  authLevel: 'function',
  route:     'agent/plan/{planId}',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 204, headers: CORS, body: '' };
    const planId = req.params.planId;
    if (!planId) return err(400, 'planId required');
    try {
      if (req.method === 'GET')    return await handleGetOverview(req, ctx, planId);
      if (req.method === 'DELETE') return await handleDelete(req, ctx, planId);
      return err(405, `method ${req.method} not allowed`);
    } catch (e) {
      ctx.log(`[planner] uncaught: ${e.message}\n${e.stack || ''}`);
      return err(500, e.message, (e.stack || '').split('\n').slice(0, 6).join('\n'));
    }
  }
});

// Per-level fetch: GET /agent/plan/{planId}/level/{level}
app.http('agent-plan-level', {
  methods:   ['GET', 'OPTIONS'],
  authLevel: 'function',
  route:     'agent/plan/{planId}/level/{level}',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 204, headers: CORS, body: '' };
    const { planId, level } = req.params;
    if (!planId) return err(400, 'planId required');
    if (level === undefined || level === null) return err(400, 'level required');
    try {
      return await handleGetLevel(req, ctx, planId, level);
    } catch (e) {
      ctx.log(`[planner] uncaught: ${e.message}\n${e.stack || ''}`);
      return err(500, e.message, (e.stack || '').split('\n').slice(0, 6).join('\n'));
    }
  }
});

// Export the pure-JS planner for unit testing / direct invocation from other
// modules (e.g. when the agent run flow consumes a plan).
module.exports = { generatePlan };
