/* cygenix-effort-model.js — the Data Conversion Standard Estimation Model.
 *
 * A faithful, transparent rebuild of the estimation spreadsheet: fixed use
 * cases down the side, configurable product modules and project variables
 * across the top, a tick per use-case × module, and function points rolling
 * up to working days, calendar months, a projected delivery date and a
 * fits-the-timeline verdict.
 *
 * Everything the spreadsheet hid in macros is a documented formula here:
 *
 *   use-case FP = (base + perModule × ticked modules) × variables × TC
 *                 …and zero when no module is ticked — untouched work
 *                 costs nothing.
 *   data FP     = Σ per module (sources×3 + attributes×0.05 + 1:1×0.5 + 1:N×1)
 *   TFP         = use-case FP + data FP
 *   working days = TFP × daysPerFP        (default 1.37)
 *   months       = working days / 19.5    (working days per month)
 *   delivery     = start + working days + holidays, skipping weekends
 *
 * Calibration: the reference sheet shows 6 modules on "Initial analysis"
 * = 13.5 FP, 1 module on "Design and documentation" = 9.0, 1 on "Script
 * development" = 26.5, and 49 FP → 67 working days → 3.44 months. The
 * default weights reproduce exactly those figures with every variable at
 * its default and TC at 1.
 *
 * USE CASES ARE FIXED — that is the standard in "Standard Estimation
 * Model". Modules and variables are data and fully configurable.
 */
(function () {
  'use strict';

  const EM_VERSION = 1;

  // ── Fixed use cases, with the calibrated weights ─────────────────────────
  const EM_USE_CASES = [
    { id: 'analysis', name: 'Initial analysis, business review, documentation', base: 1.5,  perModule: 2.0 },
    { id: 'scoping',  name: 'High level scoping',                               base: 3.0,  perModule: 1.0 },
    { id: 'mapping',  name: 'Detailed mapping of source to target',             base: 6.0,  perModule: 3.0 },
    { id: 'uat',      name: 'UAT',                                              base: 8.0,  perModule: 2.0 },
    { id: 'design',   name: 'Design and documentation',                         base: 7.0,  perModule: 2.0 },
    { id: 'client',   name: 'Client Activities',                                base: 4.0,  perModule: 1.5 },
    { id: 'scripts',  name: 'Script development',                               base: 24.5, perModule: 2.0 },
  ];

  // ── Default module set (configurable per estimate) ───────────────────────
  const EM_DEFAULT_MODULES = ['Addresses', 'AP', 'AP Master', 'AR', 'Card Summary',
    'Chart of Accounts (GL)', 'Clients', 'Configuration', 'Ethical Walls',
    'Exception Rates', 'GJ', 'Matters', 'Records', 'Purge History', 'Proforma',
    'Timekeepers', 'Trust', 'UDFs', 'Attachments', 'Users', 'WIP',
    'Business Intelligence', 'Custom'];

  // ── Default variables (configurable per estimate) ────────────────────────
  // kind 'scale': factor = 1 + weight × (value − 1), so the default value 1
  // is always neutral. kind 'flag': factor = 1 + weight when on.
  // Every default multiplies out to exactly 1.0 — the sheet's baseline.
  const EM_DEFAULT_VARIABLES = [
    { id: 'scale',    name: 'Project Scale',                 kind: 'scale', weight: 0.10, value: 1, min: 1, max: 10 },
    { id: 'staging',  name: 'Staging Area Used?',            kind: 'flag',  weight: 0.15, value: 0 },
    { id: 'migtype',  name: 'Migration Type 1 to 4',         kind: 'scale', weight: 0.25, value: 1, min: 1, max: 4 },
    { id: 'channels', name: 'No of Channels',                kind: 'scale', weight: 0.10, value: 1, min: 1, max: 20 },
    { id: 'history',  name: 'No of Years Historical data',   kind: 'scale', weight: 0.05, value: 1, min: 1, max: 30 },
  ];

  // ── Rates: how FP becomes time (and money) ───────────────────────────────
  const EM_DEFAULT_RATES = {
    daysPerFP: 1.37,            // 49 FP → 67 working days, per the sheet
    workingDaysPerMonth: 19.5,  // 67 WD → 3.44 calendar months, per the sheet
    hoursPerDay: 8,
    hourlyRate: 0,              // 0 = no cost shown until the user sets one
  };

  // ── Baseline: the quoted time for a TYPICAL project ──────────────────────
  // "Our baseline is normally 3 months" — most quotes go out on that basis.
  // The unit is the user's choice; working days is the common currency
  // (weeks are 5 working days, months use workingDaysPerMonth).
  const EM_DEFAULT_BASELINE = { value: 3, unit: 'months' };
  const EM_BASELINE_UNITS = ['days', 'weeks', 'months'];

  function emBaselineDays(baseline, rates) {
    const b = baseline || EM_DEFAULT_BASELINE;
    const n = Math.max(0, Number(b.value) || 0);
    if (b.unit === 'days') return n;
    if (b.unit === 'weeks') return n * 5;
    return n * ((rates && rates.workingDaysPerMonth) || EM_DEFAULT_RATES.workingDaysPerMonth);
  }

  // Data-model measures: counts per module, weighted into FP.
  const EM_DATA_MEASURES = [
    { id: 'sources', name: 'No. of Source Systems',      fp: 3.0 },
    { id: 'attrs',   name: 'No. of Attributes (NA)',     fp: 0.05 },
    { id: 'oto',     name: 'No. of 1:1 Relationships (OTO)', fp: 0.5 },
    { id: 'otm',     name: 'No. of 1:N Relationships (1TM)', fp: 1.0 },
  ];

  const emId = () => 'em' + Math.random().toString(36).slice(2, 9);
  const round1 = (n) => Math.round(n * 10) / 10;

  // ── Working-day date math (weekends skipped, holidays as a count) ────────
  function emParseDate(iso) {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(iso || ''));
    if (!m) return null;
    const d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
    return isNaN(d.getTime()) ? null : d;
  }
  const emIso = (d) => d.toISOString().slice(0, 10);
  const isWeekend = (d) => d.getUTCDay() === 0 || d.getUTCDay() === 6;

  function emAddWorkingDays(startIso, days) {
    const d = emParseDate(startIso);
    if (!d) return null;
    let left = Math.max(0, Math.ceil(days));
    while (left > 0) {
      d.setUTCDate(d.getUTCDate() + 1);
      if (!isWeekend(d)) left--;
    }
    return emIso(d);
  }
  // Working days from a (exclusive) to b (inclusive); negative when b < a —
  // the sheet's −580 "Working Days Available" is a feature, not an error.
  function emWorkingDaysBetween(aIso, bIso) {
    const a = emParseDate(aIso), b = emParseDate(bIso);
    if (!a || !b) return null;
    const sign = b >= a ? 1 : -1;
    const [lo, hi] = sign === 1 ? [a, b] : [b, a];
    let n = 0;
    const d = new Date(lo.getTime());
    while (d < hi) {
      d.setUTCDate(d.getUTCDate() + 1);
      if (!isWeekend(d)) n++;
    }
    return n * sign;
  }

  // ── Document shape ───────────────────────────────────────────────────────
  function emNewDoc(name) {
    return {
      v: EM_VERSION,
      name: name || 'New estimate',
      meta: { date: '', clientCode: '', clientName: '', reference: '',
        employee: '', startDate: '', dueDate: '' },
      modules: EM_DEFAULT_MODULES.slice(),
      variables: EM_DEFAULT_VARIABLES.map(v => Object.assign({}, v)),
      rates: Object.assign({}, EM_DEFAULT_RATES),
      // ticks: { '<useCaseId>|<module>': 1 } — default NONE ticked.
      ticks: {},
      // tc: { '<useCaseId>': 1.0 } — technical complexity per use case.
      tc: {},
      // data: { '<measureId>|<module>': count }
      data: {},
      holidayDays: 0,
      accruedDays: 0, useAccrued: false,
      resources: 1,
      baseline: Object.assign({}, EM_DEFAULT_BASELINE),
    };
  }

  function emNormalize(doc) {
    const d = (doc && typeof doc === 'object') ? doc : {};
    const out = emNewDoc(d.name);
    out.name = String(d.name || out.name).slice(0, 80);
    const m = d.meta || {};
    for (const k of Object.keys(out.meta)) out.meta[k] = String(m[k] || '').slice(0, 120);
    out.modules = (Array.isArray(d.modules) ? d.modules : EM_DEFAULT_MODULES)
      .map(x => String(x).slice(0, 40)).filter(Boolean).slice(0, 64);
    if (!out.modules.length) out.modules = EM_DEFAULT_MODULES.slice();
    out.variables = (Array.isArray(d.variables) && d.variables.length
      ? d.variables : EM_DEFAULT_VARIABLES).map(v => ({
        id: String(v && v.id || emId()),
        name: String(v && v.name || 'Variable').slice(0, 60),
        kind: v && v.kind === 'flag' ? 'flag' : 'scale',
        weight: Math.max(0, Math.min(2, Number(v && v.weight) || 0)),
        value: Math.max(0, Math.min(99, Number(v && v.value) || 0)),
        min: Number(v && v.min) || 0, max: Number(v && v.max) || 99,
      })).slice(0, 16);
    // A rate that is not a positive number is nonsense — restore the default
    // rather than clamping to a near-zero that silently flattens the estimate.
    const rate = (v, dflt) => { const n = Number(v); return n > 0 ? n : dflt; };
    out.rates = {
      daysPerFP: rate(d.rates && d.rates.daysPerFP, EM_DEFAULT_RATES.daysPerFP),
      workingDaysPerMonth: rate(d.rates && d.rates.workingDaysPerMonth, EM_DEFAULT_RATES.workingDaysPerMonth),
      hoursPerDay: rate(d.rates && d.rates.hoursPerDay, EM_DEFAULT_RATES.hoursPerDay),
      // Zero is a real choice here: no rate set, no cost shown.
      hourlyRate: Math.max(0, Math.min(100000, Number(d.rates && d.rates.hourlyRate) || 0)),
    };
    out.baseline = {
      value: Math.max(0, Math.min(9999, Number(d.baseline && d.baseline.value) || EM_DEFAULT_BASELINE.value)),
      unit: EM_BASELINE_UNITS.includes(d.baseline && d.baseline.unit) ? d.baseline.unit : EM_DEFAULT_BASELINE.unit,
    };
    const modSet = new Set(out.modules);
    const ucSet = new Set(EM_USE_CASES.map(u => u.id));
    for (const [k, v] of Object.entries(d.ticks || {})) {
      const [uc, mod] = String(k).split('|');
      if (ucSet.has(uc) && modSet.has(mod) && v) out.ticks[k] = 1;
    }
    for (const [k, v] of Object.entries(d.tc || {})) {
      if (ucSet.has(k)) out.tc[k] = Math.max(0.1, Math.min(99, Number(v) || 1));
    }
    const measSet = new Set(EM_DATA_MEASURES.map(x => x.id));
    for (const [k, v] of Object.entries(d.data || {})) {
      const [meas, mod] = String(k).split('|');
      const n = Number(v);
      if (measSet.has(meas) && modSet.has(mod) && n > 0) out.data[k] = Math.min(1e6, n);
    }
    out.holidayDays = Math.max(0, Math.min(365, Number(d.holidayDays) || 0));
    out.accruedDays = Math.max(0, Math.min(9999, Number(d.accruedDays) || 0));
    out.useAccrued = !!d.useAccrued;
    out.resources = Math.max(1, Math.min(50, Number(d.resources) || 1));
    return out;
  }

  // ── The model ────────────────────────────────────────────────────────────
  function emVariableFactor(variables) {
    let f = 1;
    for (const v of variables || []) {
      if (v.kind === 'flag') f *= v.value ? 1 + v.weight : 1;
      else f *= 1 + v.weight * (Math.max(0, v.value) - 1);
    }
    return f;
  }

  function emCompute(doc) {
    const vf = emVariableFactor(doc.variables);
    const perUseCase = EM_USE_CASES.map(uc => {
      const n = doc.modules.filter(mod => doc.ticks[uc.id + '|' + mod]).length;
      const tc = doc.tc[uc.id] || 1;
      const fp = n > 0 ? round1((uc.base + uc.perModule * n) * vf * tc) : 0;
      return { id: uc.id, name: uc.name, modules: n, tc, fp };
    });
    const tucfp = round1(perUseCase.reduce((s, u) => s + u.fp, 0));

    let tdp = 0;
    for (const meas of EM_DATA_MEASURES)
      for (const mod of doc.modules)
        tdp += (doc.data[meas.id + '|' + mod] || 0) * meas.fp;
    tdp = round1(tdp * vf);

    const tfp = round1(tucfp + tdp);
    let wd = Math.round(tfp * doc.rates.daysPerFP);
    if (doc.useAccrued) wd = Math.max(0, wd - Math.floor(doc.accruedDays));
    const wdShared = Math.ceil(wd / (doc.resources || 1));
    const cm = Math.round((wd / doc.rates.workingDaysPerMonth) * 100) / 100;
    const hours = wd * doc.rates.hoursPerDay;

    const start = doc.meta.startDate, due = doc.meta.dueDate;
    const delivery = start ? emAddWorkingDays(start, wd + doc.holidayDays) : null;
    const deliveryShared = start && doc.resources > 1
      ? emAddWorkingDays(start, wdShared + doc.holidayDays) : null;
    const available = (start && due) ? emWorkingDaysBetween(start, due) - doc.holidayDays : null;
    const fits = (available != null) ? wd <= available : null;
    const fitsShared = (available != null && doc.resources > 1) ? wdShared <= available : null;

    // Baseline: the standard quote. Compared against the estimate whether or
    // not dates are set — a quote goes out before a start date exists.
    const baselineDays = Math.round(emBaselineDays(doc.baseline, doc.rates) * 10) / 10;
    const fitsBaseline = baselineDays > 0 ? wd <= baselineDays : null;
    const baselineDelta = baselineDays > 0 ? Math.round((baselineDays - wd) * 10) / 10 : null;

    // Money: total hours × hourly rate. No rate, no number — a cost of £0
    // on a quote is worse than no cost at all.
    const cost = doc.rates.hourlyRate > 0 ? Math.round(hours * doc.rates.hourlyRate) : null;

    return { perUseCase, tucfp, tdp, tfp, wd, wdShared, cm, hours,
      variableFactor: Math.round(vf * 1000) / 1000,
      delivery, deliveryShared, available, fits, fitsShared,
      baselineDays, fitsBaseline, baselineDelta, cost };
  }

  // ── Baseline calibration ─────────────────────────────────────────────────
  // "If every module is ticked, it should fit the quoted time for a typical
  // project." Full scope = every use case × every module, with the current
  // variables and TC. The returned days-per-FP makes exactly that estimate
  // land on the baseline, so every partial scope prices proportionally
  // inside the standard quote.
  function emFullScopeFp(doc) {
    const full = emNormalize(JSON.parse(JSON.stringify(doc)));
    full.ticks = {};
    for (const uc of EM_USE_CASES)
      for (const mod of full.modules) full.ticks[uc.id + '|' + mod] = 1;
    return emCompute(full).tfp;
  }
  function emCalibrateToBaseline(doc) {
    const target = emBaselineDays(doc.baseline, doc.rates);
    const fullFp = emFullScopeFp(doc);
    if (!(target > 0) || !(fullFp > 0)) return null;
    // FLOOR to 3dp, never round: rounding up can overshoot the baseline by
    // a fraction of a day, and "fits the quote" must mean fits.
    return Math.max(0.001, Math.floor((target / fullFp) * 1000) / 1000);
  }

  // ── CSV export ───────────────────────────────────────────────────────────
  const emCsvCell = (v) => {
    const s = String(v == null ? '' : v);
    return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  };
  function emCsv(doc) {
    const r = emCompute(doc);
    const lines = [];
    lines.push(['use case', 'modules ticked', 'technical complexity', 'function points']
      .map(emCsvCell).join(','));
    for (const u of r.perUseCase)
      lines.push([u.name, u.modules, u.tc, u.fp].map(emCsvCell).join(','));
    lines.push(['TOTAL USE CASE FUNCTION POINTS', '', '', r.tucfp].map(emCsvCell).join(','));
    lines.push(['TOTAL DATA FUNCTION POINTS', '', '', r.tdp].map(emCsvCell).join(','));
    lines.push(['TOTAL COST IN FUNCTION POINTS', '', '', r.tfp].map(emCsvCell).join(','));
    lines.push(['Working days', '', '', r.wd].map(emCsvCell).join(','));
    lines.push(['Calendar months', '', '', r.cm].map(emCsvCell).join(','));
    lines.push(['Estimated hours', '', '', r.hours].map(emCsvCell).join(','));
    lines.push(['Calculated delivery date', '', '', r.delivery || 'n/a'].map(emCsvCell).join(','));
    lines.push(['Working days available', '', '', r.available == null ? 'n/a' : r.available].map(emCsvCell).join(','));
    lines.push(['Fits the timeline', '', '', r.fits == null ? 'n/a' : (r.fits ? 'YES' : 'NO')].map(emCsvCell).join(','));
    lines.push(['Baseline (' + doc.baseline.value + ' ' + doc.baseline.unit + ')', '', '',
      r.baselineDays + ' working days'].map(emCsvCell).join(','));
    lines.push(['Fits the baseline', '', '',
      r.fitsBaseline == null ? 'n/a' : (r.fitsBaseline ? 'YES' : 'NO')].map(emCsvCell).join(','));
    lines.push(['Hourly rate', '', '', doc.rates.hourlyRate || 'not set'].map(emCsvCell).join(','));
    lines.push(['Estimated cost', '', '', r.cost == null ? 'n/a' : r.cost].map(emCsvCell).join(','));
    return lines.join('\n') + '\n';
  }

  const api = {
    EM_VERSION, EM_USE_CASES, EM_DEFAULT_MODULES, EM_DEFAULT_VARIABLES,
    EM_DEFAULT_RATES, EM_DATA_MEASURES, EM_DEFAULT_BASELINE, EM_BASELINE_UNITS,
    emId, emNewDoc, emNormalize, emVariableFactor, emCompute, emCsv,
    emAddWorkingDays, emWorkingDaysBetween,
    emBaselineDays, emFullScopeFp, emCalibrateToBaseline,
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (typeof window !== 'undefined') window.CygenixEffortModel = api;
})();
