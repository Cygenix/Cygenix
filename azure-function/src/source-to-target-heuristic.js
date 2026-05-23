// matchSourceToTarget
//
// Given a source table name and a list of target table candidates,
// returns ranked target candidates with confidence scores.
//
// Heuristic strategy (in priority order):
//   1. Exact-name match — confidence 1.0
//   2. Strip _DM suffix on source, exact match in target — confidence 0.95
//   3. Strip _DM, abbreviation expansion (Matt → Matter, Cli → Client,
//      Tkpr → Timekeeper, etc.) — confidence 0.80
//   4. Strip _DM, fuzzy match against target names — confidence by score
//   5. No match — returns empty candidates list (caller falls back to
//      manual search field)
//
// Returns: { sourceTable, candidates: [{ target, confidence, reason }], top }
//          where top is the highest-scoring candidate or null
//
// IMPORTANT: this runs purely client-side in the browser. No backend
// involvement. The user reviews each suggestion and confirms / edits /
// rejects before any pair is committed to the scope.

// Known 3E abbreviation expansions seen in DM staging tables. Extend as
// patterns emerge from real engagements.
const ABBREV_EXPANSIONS = {
  'matt':   'matter',
  'mtr':    'matter',
  'cli':    'client',
  'tkpr':   'timekeeper',
  'tkp':    'timekeeper',
  'addr':   'address',
  'addl':   'additional',
  'info':   'information',
  'org':    'organisation',  // or organization — both match
  'wip':    'workinprogress',
  'chrg':   'charge',
  'spv':    'supervisor',
  'prlf':   'profile',       // educated guess — Matt PrlF in 3E DM
  'rate':   'rate',
  'date':   'date',
  'recv':   'receive',
  'rcpt':   'receipt'
};

function normalizeForMatch(s) {
  return String(s || '').toLowerCase().replace(/[^a-z0-9]/g, '');
}

function stripDmSuffix(name) {
  return name.replace(/_DM$/i, '').replace(/_dm$/i, '');
}

// Expand abbreviations found at word boundaries. Operates on CamelCase
// segments — splits on uppercase boundaries, expands each segment if it
// matches an abbreviation in lowercase form.
function expandAbbreviations(name) {
  // Split on uppercase transitions: "MattAddInfo" -> ["Matt", "Add", "Info"]
  const parts = name.replace(/([A-Z])/g, ' $1').trim().split(/\s+/);
  const expanded = parts.map(p => {
    const key = p.toLowerCase();
    if (ABBREV_EXPANSIONS[key]) {
      const repl = ABBREV_EXPANSIONS[key];
      return repl.charAt(0).toUpperCase() + repl.slice(1);
    }
    return p;
  }).join('');
  return expanded;
}

// Fuzzy subsequence score, normalised to [0,1]. Higher = better match.
//
// Three cases ranked highest-to-lowest:
//   - exact: 1.0
//   - candidate contains query: 0.85, light length-difference penalty
//   - query contains candidate: scored by what FRACTION of the query the
//     candidate covers — so "MatterAdditionalInfo" beats "Matter" when
//     query is "MatterAdditionalInformation". This is what makes compound
//     source names map to compound targets.
//   - subsequence: lower band, by coverage and cohesion
function fuzzyScore(query, candidate) {
  const q = normalizeForMatch(query);
  const c = normalizeForMatch(candidate);
  if (!q || !c)               return 0;
  if (c === q)                return 1.0;
  if (c.includes(q))          return 0.85 - Math.min(0.3, (c.length - q.length) / c.length * 0.3);
  if (q.includes(c)) {
    // Source name is longer than candidate. Score by how much of the
    // source name the candidate covers — longer candidate = better fit.
    // Range: 0.50 (tiny candidate covers little of source) -> 0.82
    // (candidate covers almost all of source).
    const coverage = c.length / q.length;
    return 0.50 + 0.32 * coverage;
  }
  // Subsequence match
  let qi = 0, ci = 0, runs = 0, lastIdx = -2;
  while (ci < c.length && qi < q.length) {
    if (c[ci] === q[qi]) {
      if (ci !== lastIdx + 1) runs++;
      lastIdx = ci;
      qi++;
    }
    ci++;
  }
  if (qi !== q.length)        return 0;
  const coverage = q.length / c.length;
  const cohesion = 1 - (runs - 1) / Math.max(q.length - 1, 1);
  return Math.max(0, Math.min(0.65, 0.4 * coverage + 0.25 * cohesion));
}

function matchSourceToTarget(sourceName, targetNames, options) {
  options = options || {};
  const topK = options.topK || 5;
  const minConfidence = options.minConfidence || 0.3;

  const sourceShort = sourceName.split('.').pop();      // dbo.Matter_DM → Matter_DM
  const sourceStripped = stripDmSuffix(sourceShort);    // Matter_DM → Matter
  const sourceExpanded = expandAbbreviations(sourceStripped); // MattAdd → MatterAdditional

  const candidates = [];
  for (const tgt of targetNames) {
    const tgtShort = tgt.split('.').pop();
    let bestScore = 0;
    let bestReason = null;

    // 1. Exact-name match on raw source
    if (tgtShort === sourceShort) {
      bestScore = 1.0;
      bestReason = 'exact match';
    }
    // 2. Strip _DM, exact match
    else if (tgtShort.toLowerCase() === sourceStripped.toLowerCase()) {
      bestScore = 0.95;
      bestReason = '_DM stripped, exact';
    }
    // 3. Abbreviation-expanded exact match
    else if (tgtShort.toLowerCase() === sourceExpanded.toLowerCase()) {
      bestScore = 0.80;
      bestReason = 'abbreviations expanded';
    }
    else {
      // 4. Fuzzy match — try both stripped and expanded against target
      const s1 = fuzzyScore(sourceStripped, tgtShort);
      const s2 = fuzzyScore(sourceExpanded, tgtShort);
      if (s1 >= s2 && s1 > 0) { bestScore = s1; bestReason = 'fuzzy (_DM stripped)'; }
      else if (s2 > 0)        { bestScore = s2; bestReason = 'fuzzy (expanded)'; }
    }

    if (bestScore >= minConfidence) {
      candidates.push({ target: tgt, confidence: bestScore, reason: bestReason });
    }
  }

  candidates.sort((a, b) => b.confidence - a.confidence || a.target.localeCompare(b.target));
  const top = candidates.length > 0 ? candidates[0] : null;
  return {
    sourceTable: sourceName,
    sourceStripped,
    sourceExpanded,
    candidates: candidates.slice(0, topK),
    top
  };
}

// Convenience: confidence bucket for UI display
function confidenceBucket(score) {
  if (score >= 0.90) return 'high';
  if (score >= 0.60) return 'medium';
  if (score >= 0.30) return 'low';
  return 'none';
}

module.exports = { matchSourceToTarget, confidenceBucket };
