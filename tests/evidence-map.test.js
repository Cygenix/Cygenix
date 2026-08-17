// tests/evidence-map.test.js — evidence-based auto-mapping.
//
// The promise: propose mappings from measured evidence — value overlap,
// format fingerprints, cardinality, type behaviour — with name similarity
// as one signal among five, and the reasoning spelled out. The headline
// test is the pitch's own: cust_ref → client_id on overlap + shape, a
// pairing name matching cannot see; and the reverse — a well-named column
// holding the wrong kind of data loses to the evidence.

'use strict';

const E = require('../public/cygenix-evidence-map.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Evidence-based auto-mapping — engine\n');

// ── Fingerprints ──────────────────────────────────────────────────────────
check('AB-1234 fingerprints to A{2}-9{4}', E.emFingerprint('AB-1234') === 'A{2}-9{4}');
check('case does not split patterns', E.emFingerprint('ab-1234') === E.emFingerprint('AB-1234'));
check('an 8-char alphanumeric describes itself',
  E.emDescribePattern(E.emFingerprint('C1234567'), 8) === '8-char alphanumeric');
check('an all-digit pattern reads as an n-digit number',
  E.emDescribePattern(E.emFingerprint('12345'), 5) === '5-digit number');

// ── Profiling ─────────────────────────────────────────────────────────────
const refs = Array.from({ length: 100 }, (_, i) => 'C' + String(1000000 + i));
const pRefs = E.emProfile(refs);
check('a unique reference column profiles as unique text with one pattern',
  pRefs.distinctRatio === 1 && pRefs.typeGuess === 'text'
  && pRefs.patterns[0].share === 1 && pRefs.lenAvg === 8);
check('a status code column profiles as low cardinality',
  E.emProfile(Array.from({ length: 100 }, (_, i) => ['ACTIVE', 'CLOSED', 'HOLD'][i % 3])).distinctRatio < 0.1);
check('nulls are counted, not classified',
  (() => { const p = E.emProfile(['a', null, null, 'b']); return p.nonNull === 2 && p.nullRate === 0.5; })());
check('integer-looking strings guess int; mixed with decimals guesses decimal',
  E.emProfile(['1', '2', '3']).typeGuess === 'int'
  && E.emProfile(['1', '2.5', '3']).typeGuess === 'decimal');
check('ISO dates guess date', E.emProfile(['2026-01-02', '2025-11-30']).typeGuess === 'date');

// ── Overlap ───────────────────────────────────────────────────────────────
check('overlap is containment of source in target, case-blind',
  E.emOverlap(E.emProfile(['A1', 'A2', 'A3', 'A4']), E.emProfile(['a1', 'a2', 'a3', 'zz'])) === 0.75);
check('an empty target yields no overlap verdict, not zero',
  E.emOverlap(E.emProfile(['A1']), E.emProfile([])) === null);

// ── Name affinity ─────────────────────────────────────────────────────────
check('identical names score 1', E.emNameScore('customer_id', 'CustomerId') === 1);
check('abbreviations expand: cust_ref ~ customer_reference',
  E.emNameScore('cust_ref', 'customer_reference') === 1);
check('unrelated names score low', E.emNameScore('cust_ref', 'invoice_total') < 0.3);

// ── Type compatibility ────────────────────────────────────────────────────
check('text values into INT is a clash; digits into INT aligns',
  E.emTypeCompat('text', 'INT') === 0 && E.emTypeCompat('int', 'INT') === 1);
check('anything fits a string column; int widens to decimal',
  E.emTypeCompat('date', 'NVARCHAR(50)') === 1 && E.emTypeCompat('int', 'DECIMAL(10,2)') === 1);

// ── The pitch case: evidence beats names ──────────────────────────────────
// Source cust_ref: 8-char alphanumeric, unique. Target client_id holds the
// SAME values (a consolidation target with data). Target also has a
// tempting decoy: customer_ref, well named but holding integers.
const srcProfiles = {
  cust_ref: E.emProfile(refs),
  amount:   E.emProfile(Array.from({ length: 100 }, (_, i) => (i * 1.5).toFixed(2))),
};
const tgtCols = [
  { name: 'client_id',    type: 'VARCHAR(8)' },
  { name: 'customer_ref', type: 'INT' },
  { name: 'net_amount',   type: 'DECIMAL(10,2)' },
];
const tgtProfiles = {
  client_id:    E.emProfile(refs.slice(0, 95).concat(['X9999991', 'X9999992'])),  // ~real overlap
  customer_ref: E.emProfile(Array.from({ length: 100 }, (_, i) => String(i))),    // integers — wrong data
  net_amount:   E.emProfile(Array.from({ length: 100 }, (_, i) => (i * 2.5).toFixed(2))),
};

const scored = E.emScorePair('cust_ref', srcProfiles.cust_ref, tgtCols[0], tgtProfiles.client_id);
check('cust_ref → client_id scores HIGH on overlap + shape, despite the names',
  scored.score >= 80, JSON.stringify(scored));
check('and the reasoning says why: overlap, shape, uniqueness',
  scored.reasons.some(r => /% of sampled source values/.test(r))
  && scored.reasons.some(r => /8-char alphanumeric/.test(r))
  && scored.reasons.some(r => /effectively unique/.test(r)), JSON.stringify(scored.reasons));

check('strong overlap silences the name signal — a rename cannot drag proven data down',
  (() => {
    const withBadName = E.emScorePair('zzz_x42', srcProfiles.cust_ref, tgtCols[0], tgtProfiles.client_id);
    return withBadName.score === scored.score;
  })());

const decoy = E.emScorePair('cust_ref', srcProfiles.cust_ref, tgtCols[1], tgtProfiles.customer_ref);
check('the well-named decoy holding integers is capped by the type clash',
  decoy.score <= 25 && decoy.reasons.some(r => /type clash/.test(r)), JSON.stringify(decoy));

const proposals = E.emPropose(srcProfiles, tgtCols, tgtProfiles, {});
const byTgt = {}; proposals.forEach(p => { byTgt[p.tgtCol] = p; });
check('the proposal picks client_id, not the name-alike decoy',
  byTgt.client_id.srcCol === 'cust_ref' && byTgt.customer_ref.srcCol !== 'cust_ref',
  JSON.stringify(proposals));
check('amount lands on net_amount with a numeric rationale',
  byTgt.net_amount.srcCol === 'amount' && byTgt.net_amount.match !== '');
check('every proposal carries a score and reasons',
  proposals.every(p => p._isIdentity || (p.evidence && Array.isArray(p.evidence.reasons))));

// ── Assignment discipline ─────────────────────────────────────────────────
check('a source column is claimed once — greedy best-first',
  (() => {
    const sp = { a: E.emProfile(['x1', 'x2', 'x3']) };
    const tc = [{ name: 'c1', type: 'VARCHAR(9)' }, { name: 'c2', type: 'VARCHAR(9)' }];
    const tp = { c1: E.emProfile(['x1', 'x2', 'x3']), c2: E.emProfile(['x1', 'x2']) };
    const out = E.emPropose(sp, tc, tp, {});
    return out.filter(p => p.srcCol === 'a').length === 1;
  })());
check('identity targets come back locked, exactly as autoMap emits them',
  (() => { const out = E.emPropose({}, [{ name: 'id', type: 'INT', isIdentity: true }], {}, {});
           return out[0]._isIdentity === true && out[0].srcCol === ''; })());
check('below the credibility floor nothing is proposed, and it says so',
  (() => {
    const out = E.emPropose({ zz: E.emProfile(['completely', 'different', 'things']) },
      [{ name: 'invoice_total', type: 'DECIMAL(10,2)' }], { invoice_total: E.emProfile(['1.10', '2.20']) }, {});
    return out[0].srcCol === '' && /no credible candidate/.test(out[0].evidence.reasons[0]);
  })());
check('grades map from scores: 80 HIGH, 55 MEDIUM, 35 LOW',
  E.emGrade(80) === 'HIGH' && E.emGrade(55) === 'MEDIUM' && E.emGrade(35) === 'LOW' && E.emGrade(20) === '');

// ── Schema-only fallback ──────────────────────────────────────────────────
check('with an empty target, scoring still works from source shape + names',
  (() => {
    const r = E.emScorePair('customer_id', E.emProfile(['1', '2', '3']),
      { name: 'customer_id', type: 'INT' }, E.emProfile([]));
    return r.score >= 55 && r.components.overlap === null;
  })());

// ── Orchestrator over a stubbed wire ─────────────────────────────────────
(async () => {
  const calls = [];
  const dbCall = async (conn, body) => {
    calls.push(body.sql);
    if (conn === 'SRC') return { recordset: refs.slice(0, 50).map((r, i) => ({ cust_ref: r, amount: i + 0.5 })) };
    return { recordset: refs.slice(0, 40).map((r, i) => ({ client_id: r, customer_ref: i, net_amount: i * 2 })) };
  };
  const res = await E.emRun(
    { fullName: 'src.customers', columns: [{ name: 'cust_ref' }, { name: 'amount' }] },
    { fullName: 'dbo.clients', columns: tgtCols },
    { srcConn: 'SRC', tgtConn: 'TGT', dbCall, sampleSize: 50 });
  check('the orchestrator profiles both sides and proposes with evidence',
    res.tgtHasData && res.proposals.find(p => p.tgtCol === 'client_id').srcCol === 'cust_ref');
  check('all issued SQL is TOP-N SELECT, read-only',
    calls.every(s => /^SELECT TOP \(\d+\)/.test(s)), JSON.stringify(calls));

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
