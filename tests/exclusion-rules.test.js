// Tests for cygenix-exclusion-rules.js — the pre-run exclusion picker's
// rule library. Everything here is pure: matching, counting, custom-rule
// compilation, module-prefix families and profile application, including
// the first-class rescue that pulls one table back out of a rule's net.
'use strict';

const XR = require('../public/cygenix-exclusion-rules.js');

let pass = 0, fail = 0;
const check = (name, ok, detail) => {
  console.log((ok ? '  PASS  ' : '  FAIL  ') + name + (ok || !detail ? '' : '  [' + detail + ']'));
  ok ? pass++ : fail++;
};

const T = (name, extra) => Object.assign(
  { key: 'dbo.' + name, name, schema: 'dbo', rows: 1000, hasPk: true, lane: 'MIGRATE' }, extra);
const rule = (id) => XR.XR_SOURCE_RULES.find(r => r.id === id) || XR.XR_TARGET_RULES.find(r => r.id === id);

// ── The rule library is data, not code ──────────────────────────────────────
check('the source library ships the five measured junk families plus the meta rules',
  ['backup_snapshot', 'test_temp', 'dated_dump', 'masked_snr', 'conversion_staging', 'zero_row', 'no_pk']
    .every(id => XR.XR_SOURCE_RULES.some(r => r.id === id)));
check('the target library works on different axes — a vendor schema is clean by name',
  ['tgt_empty', 'tgt_low_row', 'tgt_review', 'tgt_prefix'].every(id => XR.XR_TARGET_RULES.some(r => r.id === id)));
check('the low-row rule is flagged as a trap; the ratio guard is the recommended one',
  rule('tgt_low_row').warn === true && XR.XR_RATIO_GUARD.recommended === true
  && XR.XR_RATIO_GUARD.params.minSourceRows === 1000 && XR.XR_RATIO_GUARD.params.maxRatio === 1000
  && /reference and lookup targets are supposed to be tiny/i.test(XR.XR_LOW_ROW_WARNING));

// ── Matching, against the estate's real junk names ──────────────────────────
check('backup patterns catch the measured offenders',
  XR.xrRuleHits(rule('backup_snapshot'), T('_BAKTAB_20230421_vbmsg'))
  && XR.xrRuleHits(rule('backup_snapshot'), T('ClarityIntegration_backup'))
  && XR.xrRuleHits(rule('backup_snapshot'), T('rbcost_20191104')));
check('test/temp catches csddt_test and cv_temp_csddt1',
  XR.xrRuleHits(rule('test_temp'), T('csddt_test'))
  && XR.xrRuleHits(rule('test_temp'), T('cv_temp_csddt1'))
  && XR.xrRuleHits(rule('test_temp'), T('scratchpad_x')));
check('dated warehouse dumps: dw_<digits> and df_ prefixes',
  XR.xrRuleHits(rule('dated_dump'), T('dw_121609'))
  && XR.xrRuleHits(rule('dated_dump'), T('df_dwmtkbilled_0109'))
  && !XR.xrRuleHits(rule('dated_dump'), T('dwelling')));
check('masked / SNR copies', XR.xrRuleHits(rule('masked_snr'), T('SNR_RBcostdesc'))
  && XR.xrRuleHits(rule('masked_snr'), T('mask_client')));
check('the crown jewels never match a junk name rule',
  ['matter', 'client', 'timecard', 'gj'].every(n =>
    XR.XR_SOURCE_RULES.filter(r => r.re).every(r => !XR.xrRuleHits(r, T(n)))));
check('meta rules read the metadata, not the name',
  XR.xrRuleHits(rule('zero_row'), T('anything', { rows: 0 }))
  && !XR.xrRuleHits(rule('zero_row'), T('anything', { rows: 1 }))
  && XR.xrRuleHits(rule('no_pk'), T('heap', { hasPk: false })));
check('target axes: empty, under-10, review lane, licensed-module prefix',
  XR.xrRuleHits(rule('tgt_empty'), T('NxA', { rows: 0 }))
  && XR.xrRuleHits(rule('tgt_low_row'), T('EmailType', { rows: 2 }))
  && !XR.xrRuleHits(rule('tgt_low_row'), T('Matter', { rows: 55000 }))
  && XR.xrRuleHits(rule('tgt_review'), T('X', { lane: 'REVIEW' }))
  && XR.xrRuleHits(Object.assign({}, rule('tgt_prefix'), { params: { prefixes: ['Nx'] } }), T('NxBudget'))
  && !XR.xrRuleHits(Object.assign({}, rule('tgt_prefix'), { params: { prefixes: ['Nx'] } }), T('Matter')));

// ── Custom rules: regex or glob ─────────────────────────────────────────────
check('a glob is anchored: dw_* matches dw_121609, never xdw_1',
  XR.xrCompileCustom('dw_*').re.test('dw_121609')
  && !XR.xrCompileCustom('dw_*').re.test('xdw_1'));
check('anything else is a raw regex, unanchored',
  XR.xrCompileCustom('^rpt_').re.test('rpt_billing')
  && XR.xrCompileCustom('cost').re.test('rbcostdesc'));
check('an invalid pattern compiles to null, never a throw',
  XR.xrCompileCustom('([') === null && XR.xrCompileCustom('') === null);

// ── Live counts and prefix families ─────────────────────────────────────────
{
  const est = [T('_BAKTAB_1_a'), T('x_backup', { rows: 500 }), T('matter', { rows: 62000 })];
  const c = XR.xrCount(rule('backup_snapshot'), est);
  check('chip counts are live: tables and rows the rule would take',
    c.n === 2 && c.rows === 1500);
  const fam = XR.xrPrefixes(
    [...Array.from({ length: 9 }, (_, i) => T('NxThing' + i)),
     ...Array.from({ length: 8 }, (_, i) => T('RMItem' + i)), T('Matter')], 8);
  check('prefix families need real size — a firm drops a whole module in one action',
    fam.some(f => f.prefix.toLowerCase() === 'nx' && f.n === 9)
    && fam.some(f => f.prefix.toLowerCase() === 'rm')
    && !fam.some(f => f.prefix.toLowerCase() === 'ma'));
}

// ── Profile application: reasons, rescue, manual, ratio guard ───────────────
{
  const src = [T('_BAKTAB_20230421_vbmsg'), T('dw_commitcmr_2019'), T('matter', { rows: 62000 })];
  const tgt = [T('NxEmpty', { rows: 0 }), T('Matter', { rows: 55000 })];
  const prof = XR.xrNewProfile('son_db_hk', 'hgji9opp', 'analyst@x', 2);
  prof.rules = [
    { id: 'backup_snapshot', side: 'source', enabled: true },
    { id: 'dated_dump', side: 'source', enabled: true },
    { id: 'test_temp', side: 'source', enabled: false },
    { id: 'tgt_empty', side: 'target', enabled: true },
    { id: 'ratio_guard', side: 'target', enabled: true, params: { minSourceRows: 500, maxRatio: 1000 } },
  ];
  prof.manual.exclude = [{ key: 'dbo.matter', side: 'source', reason: 'demo only', by: 'analyst@x' }];
  const res = XR.xrApply(prof, src, tgt);
  check('every exclusion carries its reason code — rule id or MANUAL',
    res.src.get('dbo._baktab_20230421_vbmsg') === 'RULE:backup_snapshot'
    && res.src.get('dbo.dw_commitcmr_2019') === 'RULE:dated_dump'
    && res.src.get('dbo.matter') === 'MANUAL'
    && res.tgt.get('dbo.nxempty') === 'RULE:tgt_empty');
  check('a disabled rule does nothing', ![...res.src.values()].includes('RULE:test_temp'));
  check('the ratio guard passes through with its params, defaults filled',
    res.ratioGuard.minSourceRows === 500 && res.ratioGuard.maxRatio === 1000);

  // Rescue: first-class, beats every rule AND the manual list.
  prof.manual.rescue = [{ key: 'dbo.dw_commitcmr_2019', reason: 'live reporting table', by: 'analyst@x' },
                        { key: 'dbo.matter', reason: 'oops', by: 'analyst@x' }];
  const res2 = XR.xrApply(prof, src, tgt);
  check('rescue pulls a table back out of the net without editing the rule',
    !res2.src.has('dbo.dw_commitcmr_2019') && res2.src.has('dbo._baktab_20230421_vbmsg'));
  check('rescue beats a manual exclusion too — it is the way back',
    !res2.src.has('dbo.matter'));

  check('a custom rule applies with its own id',
    XR.xrApply(Object.assign({}, prof, {
      rules: [{ id: 'custom', side: 'source', enabled: true, custom: '^dw_' }],
      manual: { exclude: [], rescue: [] },
    }), src, tgt).src.get('dbo.dw_commitcmr_2019') === 'RULE:custom');
  check('no profile means no exclusions and no guard',
    XR.xrApply(null, src, tgt).src.size === 0 && XR.xrApply(null, src, tgt).ratioGuard === null);
  check('profiles are versioned from zero and stamp their engine',
    prof.version === 0 && prof.engineVersion === 2 && prof.connectionPair.source === 'son_db_hk');
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
