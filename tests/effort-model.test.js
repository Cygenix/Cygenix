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

// ── CSV ─────────────────────────────────────────────────────────────────────
{
  const d = EM.emNewDoc();
  d.ticks['analysis|AP'] = 1;
  const csv = EM.emCsv(d);
  check('the CSV walks use cases to totals to the verdict',
    /Initial analysis/.test(csv) && /TOTAL COST IN FUNCTION POINTS/.test(csv)
    && /Working days/.test(csv) && /Fits the timeline/.test(csv));
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

  const sidebar = fs.readFileSync(path.join(__dirname, '..', 'public', 'cygenix-sidebar.js'), 'utf8');
  check('Effort Estimator lives under Reports in the rail',
    /key:'effort-estimator',\s*label:'Effort Estimator',\s*href:'\/effort_estimator\.html'/.test(sidebar));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
