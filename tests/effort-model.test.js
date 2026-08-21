// Tests for the Effort Estimator — cygenix-effort-model.js (the pure
// estimation engine) plus structural pins on effort_estimator.html.
// The calibration checks pin the reference sheet's visible figures.
'use strict';

const fs = require('fs');
const path = require('path');
const EM = require('../public/cygenix-effort-model.js');

let pass = 0, fail = 0;
const check = (name, ok, detail) => {
  console.log((ok ? '  PASS  ' : '  FAIL  ') + name + (ok || !detail ? '' : '  [' + detail + ']'));
  ok ? pass++ : fail++;
};

// ── The model's shape ───────────────────────────────────────────────────────
check('the seven use cases are FIXED — that is the "standard" in the name',
  EM.EM_USE_CASES.length === 7
  && EM.EM_USE_CASES[0].name.startsWith('Initial analysis')
  && EM.EM_USE_CASES.map(u => u.id).join(',') === 'analysis,scoping,mapping,uat,design,client,scripts');
check('the default module set is the sheet\'s, AP through Custom',
  EM.EM_DEFAULT_MODULES.includes('AP') && EM.EM_DEFAULT_MODULES.includes('AR')
  && EM.EM_DEFAULT_MODULES.includes('Chart of Accounts (GL)')
  && EM.EM_DEFAULT_MODULES.includes('Business Intelligence')
  && EM.EM_DEFAULT_MODULES[EM.EM_DEFAULT_MODULES.length - 1] === 'Custom');
check('the default variables are the sheet\'s five, every one neutral at its default',
  EM.EM_DEFAULT_VARIABLES.length === 5
  && Math.abs(EM.emVariableFactor(EM.EM_DEFAULT_VARIABLES) - 1) < 1e-9);
check('a new estimate starts with NOTHING ticked',
  Object.keys(EM.emNewDoc().ticks).length === 0);

// ── Calibration against the reference sheet ─────────────────────────────────
{
  const d = EM.emNewDoc();
  for (const m of ['Addresses', 'AP', 'AP Master', 'AR', 'Card Summary', 'Chart of Accounts (GL)'])
    d.ticks['analysis|' + m] = 1;
  d.ticks['design|Addresses'] = 1;
  d.ticks['scripts|Addresses'] = 1;
  const r = EM.emCompute(d);
  const fp = Object.fromEntries(r.perUseCase.map(u => [u.id, u.fp]));
  check('six modules on Initial analysis = 13.5 FP, as the sheet shows',
    fp.analysis === 13.5);
  check('one module on Design and documentation = 9.0, one on Script development = 26.5',
    fp.design === 9.0 && fp.scripts === 26.5);
  check('an untouched use case costs nothing, whatever its base weight',
    fp.scoping === 0 && fp.uat === 0);
  check('TUCFP totals 49.0 → 67 working days → 3.44 calendar months, exactly the sheet',
    r.tucfp === 49.0 && r.wd === 67 && r.cm === 3.44);
  check('hours follow the working days', r.hours === 67 * 8);

  d.meta.startDate = '2026-08-21'; d.meta.dueDate = '2024-06-03';
  const r2 = EM.emCompute(d);
  check('a due date before the start reads as NEGATIVE days available, like the sheet',
    r2.available < -500 && r2.fits === false);
  d.meta.dueDate = '2027-06-03';
  const r3 = EM.emCompute(d);
  check('a real due date flips the verdict and projects a delivery date',
    r3.fits === true && /^2026-/.test(r3.delivery));
}

// ── Variables, TC and data FP move the number ───────────────────────────────
{
  const d = EM.emNewDoc();
  d.ticks['analysis|AP'] = 1;
  const base = EM.emCompute(d).tucfp;                     // (1.5 + 2) × 1 × 1 = 3.5
  d.variables.find(v => v.id === 'migtype').value = 4;    // ×1.75
  const scaled = EM.emCompute(d).tucfp;
  check('migration type 4 multiplies effort by 1.75', base === 3.5 && scaled === 6.1);
  d.variables.find(v => v.id === 'staging').value = 1;    // flag: ×1.15
  check('the staging flag stacks multiplicatively', EM.emCompute(d).tucfp === 7.0);
  d.tc.analysis = 2;
  check('technical complexity scales the one use case', EM.emCompute(d).tucfp === 14.1);

  d.data['sources|AP'] = 2; d.data['attrs|AP'] = 100; d.data['oto|AP'] = 4; d.data['otm|AP'] = 3;
  const r = EM.emCompute(d);
  check('data FP: sources ×3 + attributes ×0.05 + 1:1 ×0.5 + 1:N ×1, times the variables',
    r.tdp === Math.round((2 * 3 + 100 * 0.05 + 4 * 0.5 + 3 * 1) * 1.75 * 1.15 * 10) / 10
    && r.tfp === Math.round((r.tucfp + r.tdp) * 10) / 10);
}

// ── Accrued days, resources, date math ──────────────────────────────────────
{
  const d = EM.emNewDoc();
  d.ticks['scripts|AP'] = 1;                              // 26.5 FP → 36 wd
  d.accruedDays = 10; d.useAccrued = true;
  check('accrued days come off the working days only when Use accrued is on',
    EM.emCompute(d).wd === Math.round(26.5 * 1.37) - 10);
  d.useAccrued = false;
  d.resources = 2;
  const r = EM.emCompute(d);
  check('resources in parallel halve the shared estimate, ceilinged',
    r.wdShared === Math.ceil(r.wd / 2));

  check('working-day walking skips weekends',
    EM.emAddWorkingDays('2026-08-21', 1) === '2026-08-24');   // Fri → Mon
  check('working days between two dates count only weekdays, signed',
    EM.emWorkingDaysBetween('2026-08-21', '2026-08-28') === 5
    && EM.emWorkingDaysBetween('2026-08-28', '2026-08-21') === -5);
  check('garbage dates return null, never a throw',
    EM.emAddWorkingDays('nope', 5) === null && EM.emWorkingDaysBetween('x', 'y') === null);
}

// ── Normalization ───────────────────────────────────────────────────────────
{
  const messy = EM.emNormalize({
    name: 'X',
    modules: ['AP', 'AR'],
    ticks: { 'analysis|AP': 1, 'analysis|Matters': 1, 'nonsense|AP': 1, 'design|AR': 0 },
    tc: { analysis: 500, nonsense: 2 },
    data: { 'sources|AP': 3, 'sources|Matters': 9, 'bogus|AP': 1, 'attrs|AR': -5 },
    variables: [{ id: 'v', name: 'V', kind: 'flag', weight: 9, value: 1 }],
    rates: { daysPerFP: -1 },
    resources: 0,
  });
  check('ticks against removed modules, unknown use cases or falsy values are dropped',
    Object.keys(messy.ticks).join(',') === 'analysis|AP');
  check('TC clamps into 0.1–99 and unknown use cases are dropped',
    messy.tc.analysis === 99 && !('nonsense' in messy.tc));
  check('data counts follow the same rules — live modules, known measures, positive only',
    Object.keys(messy.data).join(',') === 'sources|AP');
  check('weights, rates and resources clamp to sane ranges',
    messy.variables[0].weight === 2 && messy.rates.daysPerFP === EM.EM_DEFAULT_RATES.daysPerFP
    && messy.resources === 1);
  check('normalizing nothing yields a working default', EM.emNormalize(null).modules.length > 0);
}

// ── Baseline: the standard quote ────────────────────────────────────────────
{
  const d = EM.emNewDoc();
  check('the default baseline is the firm\'s typical 3 months',
    d.baseline.value === 3 && d.baseline.unit === 'months'
    && EM.emBaselineDays(d.baseline, d.rates) === 58.5);
  check('the unit is the user\'s choice — days, weeks or months',
    EM.emBaselineDays({ value: 10, unit: 'days' }, d.rates) === 10
    && EM.emBaselineDays({ value: 4, unit: 'weeks' }, d.rates) === 20
    && EM.EM_BASELINE_UNITS.join(',') === 'days,weeks,months');

  d.ticks['scripts|AP'] = 1;                            // 26.5 FP → 36 wd
  const r = EM.emCompute(d);
  check('the baseline verdict works before any dates exist — a quote goes out first',
    r.fitsBaseline === true && r.baselineDays === 58.5 && r.baselineDelta === 22.5);
  for (const uc of EM.EM_USE_CASES) for (const m of d.modules) d.ticks[uc.id + '|' + m] = 1;
  check('full scope at the sheet rate blows a 3-month baseline — that is the point of calibrating',
    EM.emCompute(d).fitsBaseline === false);

  // Calibration: full scope — every module on every use case — must land
  // exactly inside the baseline, so partial scopes price proportionally.
  const rate = EM.emCalibrateToBaseline(d);
  d.rates.daysPerFP = rate;
  const full = EM.emCompute(d);
  check('calibrating the rate makes a fully ticked estimate fit the baseline',
    rate > 0 && rate < EM.EM_DEFAULT_RATES.daysPerFP
    && full.fitsBaseline === true && full.wd <= full.baselineDays);
  check('and a half scope then sits comfortably inside the standard quote',
    (() => { const h = EM.emNormalize(d); h.ticks = { 'scripts|AP': 1 }; h.rates.daysPerFP = rate;
      return EM.emCompute(h).wd < full.wd / 4; })());
  check('a zero baseline reads as no verdict, never a divide-by-zero',
    (() => { const z = EM.emNewDoc(); z.baseline.value = 0;
      return EM.emCompute(z).fitsBaseline === null && EM.emCalibrateToBaseline(z) === null; })());
  check('normalization clamps the baseline and rejects unknown units',
    EM.emNormalize({ baseline: { value: -3, unit: 'fortnights' } }).baseline.unit === 'months');
}

// ── Module complexity weights ───────────────────────────────────────────────
// Proforma is inherently simpler than Trust, and the estimate should say so:
// a ticked module contributes perModule × its weight, default ×1.
{
  const d = EM.emNewDoc();
  d.ticks['analysis|AP'] = 1; d.ticks['analysis|Trust'] = 1;
  check('all-default weights are exactly the calibrated standard',
    EM.emCompute(d).tucfp === 5.5 && EM.emModuleWeight(d, 'AP') === 1);
  d.moduleWeights = { Trust: 1.5, Proforma: 0.5 };
  check('a hard module contributes more — Trust ×1.5 lifts the use case',
    EM.emCompute(d).tucfp === 6.5);
  d.moduleWeights.AP = 0.5;
  check('a light module contributes less, and the weights combine',
    EM.emCompute(d).tucfp === 5.5);
  check('a lone light module still OPENS the use case — the gate is the tick, not the weight',
    (() => { const e = EM.emNewDoc(); e.ticks['uat|Proforma'] = 1;
      e.moduleWeights = { Proforma: 0.1 };
      const fp = EM.emCompute(e).perUseCase.find(u => u.id === 'uat').fp;
      return fp === 8.2; })());              // (8 + 2×0.1) — base still applies
  check('weights ride through baseline calibration — a heavier estate needs a slower rate',
    EM.emCalibrateToBaseline(d) !== EM.emCalibrateToBaseline(EM.emNewDoc()));

  const norm = EM.emNormalize({ modules: ['AP', 'AR'],
    moduleWeights: { AP: 0.5, AR: 1, Matters: 3, ghost: 2, junk: -1 } });
  check('normalization keeps only live modules, clamps, and never stores the default',
    JSON.stringify(norm.moduleWeights) === '{"AP":0.5}'
    && EM.emNormalize({ modules: ['AP'], moduleWeights: { AP: 999 } }).moduleWeights.AP === 10);
  check('the CSV names every non-default weight',
    /Module weights,,,"?Trust ×1\.5; Proforma ×0\.5; AP ×0\.5/.test(EM.emCsv(EM.emNormalize(d))));
}

// ── Cost: hours × hourly rate ───────────────────────────────────────────────
{
  const d = EM.emNewDoc();
  d.ticks['scripts|AP'] = 1;                            // 36 wd × 8h = 288h
  check('no rate set means no cost shown — a £0 quote is worse than none',
    EM.emCompute(d).cost === null);
  d.rates.hourlyRate = 120;
  const r = EM.emCompute(d);
  check('the estimated cost is total hours × the hourly rate',
    r.cost === r.hours * 120 && r.hours === r.wd * 8);
  check('the rate survives normalization and clamps to sanity',
    EM.emNormalize(d).rates.hourlyRate === 120
    && EM.emNormalize({ rates: { hourlyRate: -5 } }).rates.hourlyRate === 0);
}

// ── CSV ─────────────────────────────────────────────────────────────────────
{
  const d = EM.emNewDoc();
  d.ticks['analysis|AP'] = 1;
  d.rates.hourlyRate = 100;
  const csv = EM.emCsv(d);
  check('the CSV walks use cases to totals to the verdict',
    /Initial analysis/.test(csv) && /TOTAL COST IN FUNCTION POINTS/.test(csv)
    && /Working days/.test(csv) && /Fits the timeline/.test(csv));
  check('the CSV carries the baseline verdict and the estimated cost',
    /Baseline \(3 months\)/.test(csv) && /Fits the baseline,,,YES/.test(csv)
    && /Estimated cost,,,\d+/.test(csv));
}

// ── Page pins ───────────────────────────────────────────────────────────────
{
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'effort_estimator.html'), 'utf8');
  check('the page ships the standard chrome and the Cygenix brand — no Elite anywhere',
    /auth-gate\.js/.test(html) && /cygenix-mobile\.css/.test(html)
    && /data-active="effort-estimator"/.test(html)
    && /CYGENIX/.test(html) && !/Elite/i.test(html)
    && /Data Conversion Standard Estimation Model/.test(html));
  check('the engine module is loaded and the store is versioned',
    /cygenix-effort-model\.js/.test(html) && /cygenix_effort_estimates_v1/.test(html));
  check('the grid uses checkboxes, not zeros and ones',
    /type="checkbox"/.test(html) && /esTick\(/.test(html));
  check('user text is escaped at render', /esc\(uc\.name\)/.test(html) && /esc\(m\)/.test(html)
    && /esc\(v\.name\)/.test(html));
  check('modules and variables are configurable on the page',
    /esAddModule/.test(html) && /esRemoveModule/.test(html)
    && /esAddVariable/.test(html) && /esRemoveVariable/.test(html));
  check('the verdict states fit or overrun against the due date',
    /Does not fit/.test(html) && /over by/.test(html) && /Fits:/.test(html));
  check('every weight is visible and editable — the method panel says so',
    /an estimate nobody can inspect is an estimate nobody trusts/i.test(html));
  check('print hides the chrome', /@media print/.test(html));
  check('house weight defaults: saved once, applied to every new estimate',
    /cygenix_house_weights_v1/.test(html) && /esSaveHouseWeights/.test(html)
    && /esApplyHouseWeights/.test(html)
    && /esApplyHouseTo\(EM\.emNormalize\(EM\.emNewDoc\(name\)\)\)/.test(html)
    && (html.match(/esApplyHouseTo\(EM\.emNewDoc\('New estimate'\)\)/g) || []).length === 2);
  check('estimates save to a JSON file and load back, name-collision safe',
    /esSaveEstimate/.test(html) && /esLoadEstimate/.test(html)
    && /esImportEstimateText/.test(html) && /cygenix-estimate-/.test(html)
    && /accept="\.json,application\/json"/.test(html)
    && /not a saved Cygenix estimate/.test(html));
  check('module weights are editable on the chips and badge the grid headers',
    /esModuleWeight/.test(html) && /Complexity weight — 1 is standard/.test(html)
    && /emModuleWeight\(doc, m\)/.test(html) && /×' \+ w \+ '<\/span>'/.test(html));
  check('the baseline is on the page: value, unit and the calibrate action',
    /id="es-baseline-value"/.test(html) && /id="es-baseline-unit"/.test(html)
    && /Calibrate rate to baseline/.test(html) && /esCalibrate/.test(html)
    && /id="es-baseline-note"/.test(html));
  check('the hourly rate feeds an estimated cost tile',
    /Hourly rate/.test(html) && /Estimated cost/.test(html) && /hourlyRate/.test(html));

  const sidebar = fs.readFileSync(path.join(__dirname, '..', 'public', 'cygenix-sidebar.js'), 'utf8');
  check('Effort Estimator leads the Project group below Home',
    /key:'project-group',\s*label:'Project'/.test(sidebar)
    && /key:'effort-estimator',\s*label:'Effort Estimator',\s*href:'\/effort_estimator\.html'/.test(sidebar));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
