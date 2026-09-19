// tests/assurance-design.test.js — Assurance in the console's design language.
//
// Design review, Sep-2026, Phase 3. The page already had a layout contract
// (one status object, one CTA, a ribbon derived from it, an audience split);
// what this phase changed is how that contract is drawn. This file keeps the
// drawing true to the handoff's §5 so a later "tidy-up" cannot quietly put
// the pills, the 10px mono sub-lines and the filled panels back:
//
//   · the header is kicker / title / sentence, and the status band is the one
//     blueprint frame on the screen;
//   · the ribbon is the auditor's four questions, with figures for notes;
//   · coverage is four measures that can each reach their ceiling, over the
//     detail that says where the gaps are;
//   · the checks table leads with a sentence and an 8px state square, and the
//     five states are words in the status colours, not pass/fail pills;
//   · a breach leads with the sentence it falsifies, then four measures, then
//     decisions — Send to cleansing filled, the rest bordered;
//   · nothing on the page is set below 13px body / 11px table-header, and the
//     page fetches no fonts from Google.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const PUB = path.join(__dirname, '..', 'public');
const PAGE = fs.readFileSync(path.join(PUB, 'assurance.html'), 'utf8');
const A = require(path.join(PUB, 'cygenix-assurance.js'));
const CSS = (PAGE.match(/<style>([\s\S]*?)<\/style>/) || [])[1] || '';
const cssNoComments = CSS.replace(/\/\*[\s\S]*?\*\//g, '');

console.log('Assurance — the console design language\n');

/* ── 1. Header and status band ──────────────────────────────────────────── */
check('the header is kicker QUALITY, title ASSURANCE, and the sentence at 16px / 86ch',
  /<div class="cx-kicker">Quality<\/div>\s*<h1 class="as-title">Assurance<\/h1>/.test(PAGE)
  && /\.as-sub\{font-size:16px[^}]*max-width:86ch/.test(CSS)
  && !/✓ Assurance/.test(PAGE));
check('the status band is the one blueprint frame on the screen, with its four corners',
  /class="as-status cx-blueprint"/.test(PAGE)
  && (PAGE.match(/cx-blueprint/g) || []).length === 1
  && ['tl', 'tr', 'bl', 'br'].every(c => PAGE.includes('<span class="cx-corner ' + c + '"></span>')));
check('the band is 18px 22px, space-between, a 26px Condensed line and a 15px consequence',
  /\.as-status\{[^}]*justify-content:space-between[^}]*padding:18px 22px/.test(CSS)
  && /\.as-status \.line\{font-family:var\(--font-heading\)[^}]*font-size:26px[^}]*text-transform:uppercase/.test(CSS)
  && /\.as-status \.subnote\{font-size:15px/.test(CSS));
check('exactly one CTA sits in the band, 40px tall, 22px padding, 16px label',
  (PAGE.match(/id="as-cta"/g) || []).length === 1
  && /class="btn btn-primary as-cta" id="as-cta"/.test(PAGE)
  && /\.as-cta\{height:40px;padding:0 22px;font-size:16px/.test(CSS));
check('the breach state says what is open, what it blocks, and how many to review',
  /' of ' \+ st\.checksWritten \+ ' check'/.test(PAGE)
  && PAGE.includes('The evidence pack cannot be published while a critical breach is open.')
  && /'Review ' \+ n \+ ' breach' \+ \(n === 1 \? '' : 'es'\)/.test(PAGE));

/* ── 2. The ribbon ─────────────────────────────────────────────────────── */
check('four equal bordered cells, 8px gap, number at 13px, title at 19px Condensed, note at 13px',
  /\.ribbon\{display:grid;grid-template-columns:repeat\(4,1fr\);gap:8px/.test(CSS)
  && /\.rstep \.idx\{[^}]*font-size:13px[^}]*letter-spacing:\.1em/.test(CSS)
  && /\.rstep \.t\{[^}]*font-size:19px[^}]*text-transform:uppercase/.test(CSS)
  && /\.rstep \.n\{font-size:13px[^}]*var\(--color-neutral-700\)/.test(CSS));
check('the current step takes the accent border and accent-100 ground; an unreached title is neutral-600',
  /\.rstep\.now\{border-color:var\(--color-accent\);background:var\(--color-accent-100\)\}/.test(CSS)
  && /\.rstep\.now \.idx,\.rstep\.now \.t\{color:var\(--color-accent-900\)\}/.test(CSS)
  && /\.rstep\.now \.n\{color:var\(--color-accent-800\)\}/.test(CSS)
  && /\.rstep\.todo \.t\{color:var\(--color-neutral-600\)\}/.test(CSS));
check('the ribbon number is not caught by the table\'s right-aligned .num helper',
  /class="rstep ' \+ s\.state \+ '"><div class="idx">/.test(PAGE));
const st = A.asStatus(A.asNewStore(1), { connections: 2 });
check('the engine names the four steps the auditor asks about, in order',
  A.asRibbon(st).map(s => s.label).join('|') === 'Checks written|Run against live data|Breaches cleared|Evidence published');

/* ── 3. The audience control ───────────────────────────────────────────── */
check('Engineer | Evidence is the segmented control, left-aligned, active on accent-900',
  /\.as-seg\{display:inline-flex;border:1px solid var\(--color-divider\)/.test(CSS)
  && /\.as-seg button\.active\{background:var\(--color-accent-900\);color:var\(--color-bg\)\}/.test(CSS)
  && /id="as-tab-eng" class="active"/.test(PAGE));

/* ── 4. Coverage ───────────────────────────────────────────────────────── */
check('four measures in a 22px-gap grid: tables with a check, rows under check, money columns, unchecked',
  /\.covgrid\{display:grid;grid-template-columns:repeat\(4,1fr\);gap:22px/.test(CSS)
  && ['Tables with a check', 'Rows under check', 'Money columns', 'Unchecked tables'].every(k => PAGE.includes("'" + k + "'")));
check('a 5px bar under each but the last, which carries an explanatory line, in state-warn when non-zero',
  /\.cov \.bar\{height:5px/.test(CSS) && /\.cov \.v\.warn\{color:var\(--state-warn\)\}/.test(CSS)
  && /i === 3 && Number\(v\) > 0 \? ' warn'/.test(PAGE));
check('the strip paints its frame before the figures — dashes with the reason, never Loading',
  /cx-shimmer/.test(PAGE) && PAGE.includes('No target schema loaded, so coverage cannot be computed')
  && !/Reading the target schema…/.test(PAGE));
check('both coverage paths draw the strip from the same covGrid, over the gap detail',
  (PAGE.match(/host\.innerHTML = covGrid\(\[/g) || []).length === 3
  && (PAGE.match(/<div class="covdetail">/g) || []).length === 2);
const mc = A.asMoneyCoverage(A.asNewStore(1), { tables: [
  { db: 'tgt', schema: 'fin', table: 'posting', columns: ['id', 'amount', 'net_value'] },
  { db: 'tgt', schema: 'ref', table: 'ccy', columns: ['code'] },
  { db: 'src', schema: 'fin', table: 'posting', columns: ['amount'] }] }, { db: 'tgt' });
check('money coverage counts money columns in the target and says none when there are none',
  mc.total === 2 && mc.checked === 0 && mc.pct === 0
  && A.asMoneyCoverage(A.asNewStore(1), { tables: [] }, { db: 'tgt' }).pct === null);
(() => {
  const s = A.asNewStore(1);
  A.asSaveRule(s, { name: 'sum', category: 'finance', check: 'recon.column_aggregate', params: { fn: 'sum' },
    binding: { mode: 'explicit', targets: [{ db: 'tgt', schema: 'fin', table: 'posting', column: 'amount' }] } }, 'u', 1);
  const cat = { tables: [{ db: 'tgt', schema: 'fin', table: 'posting', columns: ['id', 'amount', 'net_value'] },
                         { db: 'tgt', schema: 'fin', table: 'fee', columns: ['fee_amount'] }] };
  const m = A.asMoneyCoverage(s, cat, { db: 'tgt' });
  check('a rule bound to a table puts every money column on that table under check',
    m.total === 3 && m.checked === 2 && m.pct === 67, JSON.stringify(m));
})();

/* ── 5. Suggested checks ───────────────────────────────────────────────── */
check('each suggestion row is a category tag, a 17px Condensed title, a 15px explanation, Dismiss / Add rule',
  /\.bundle b\{[^}]*font-size:17px[^}]*text-transform:uppercase/.test(CSS)
  && /\.bundle p\{font-size:15px/.test(CSS)
  && /<div class="bundle"><span class="chip">' \+ esc\(catLabel\(/.test(PAGE)
  && PAGE.includes('>Add rule</button>') && !PAGE.includes('Review &amp; add'));
check('Re-profile is a link in the section header', /class="btn link" onclick="asProfileNow\(\)">↻ Re-profile/.test(PAGE));

/* ── 6. Your checks ────────────────────────────────────────────────────── */
check('the filter chips take accent border, accent-800 text, accent-100 ground when on',
  /\.fchip\.on\{color:var\(--color-accent-800\);border-color:var\(--color-accent\);background:var\(--color-accent-100\)\}/.test(CSS));
check('the columns run What it proves · Scope · Runs · State · SQL · On',
  /<th>What it proves<\/th><th>Scope<\/th><th>Runs<\/th><th>State<\/th><th>SQL<\/th><th>On<\/th>/.test(PAGE));
check('the first column is the 15px sentence with an 8px state square before it',
  /\.rulename\{display:block;font-size:15px/.test(CSS)
  && /<span class="rulename">' \+ stateSquare\(r\) \+ esc\(r\.proves \|\| r\.name\)/.test(PAGE)
  && /function stateSquare\(r\)/.test(PAGE) && /cx-dot-fail/.test(PAGE));
check('the five states are words with squares in the handoff\'s colours',
  /\.st-derived\{color:var\(--color-neutral-700\)\}/.test(CSS)
  && /\.st-armed\{color:var\(--color-accent-800\)\}\.st-armed::before\{background:var\(--color-accent\)\}/.test(CSS)
  && /\.st-proven\{color:var\(--state-ok\)\}/.test(CSS)
  && /\.st-open\{color:var\(--state-fail\)\}/.test(CSS)
  && /\.st-dormant\{color:var\(--state-warn\)\}/.test(CSS)
  && /\.st-error\{color:var\(--state-warn\)\}/.test(CSS)
  && /\.st::before\{content:'';width:8px;height:8px;background:var\(--color-neutral-400\)/.test(CSS));
check('the SQL column says Generated or Custom, not a bare SQL button',
  /r\.check === 'custom\.sql' \? 'Custom' : 'Generated'/.test(PAGE));

/* ── 7. Breaches ───────────────────────────────────────────────────────── */
check('the split is 300px 1fr inside one border — no panel-body padding around it',
  /\.split\{display:grid;grid-template-columns:300px minmax\(0,1fr\)/.test(CSS)
  && /\.split > \.bq\{border-right:1px solid var\(--color-divider\)\}/.test(CSS)
  && /<div class="split">\s*<div class="bq" id="as-bq">/.test(PAGE));
check('a queue entry is severity, timestamp, a 17px Condensed name and a 13px row count',
  /<div class="ts">' \+ \(done \? '<span class="chip ok">Resolved<\/span>' : sevChip\(b\.severity\)\)/.test(PAGE)
  && /\.bq-item \.t\{[^}]*font-size:17px[^}]*text-transform:uppercase/.test(CSS)
  && /\.bq-item \.m\{font-size:13px/.test(CSS));
check('the selected entry takes accent-100 and a 3px state-fail left rule; resolved ones go neutral-600',
  /\.bq-item\.on\{background:var\(--color-accent-100\);border-left-color:var\(--state-fail\)\}/.test(CSS)
  && /border-left:3px solid transparent/.test(CSS)
  && /\.bq-item\.done \.t,\.bq-item\.done \.m,\.bq-item\.done \.ts\{color:var\(--color-neutral-600\)\}/.test(CSS));
check('the detail leads with the proves sentence at 23px Condensed and a 15px explanation naming the counts',
  /\.bd-proves\{[^}]*font-size:23px[^}]*text-transform:uppercase/.test(CSS)
  && /\.bd-why\{font-size:15px/.test(CSS)
  && /<div class="bd-proves">' \+ esc\(r\.proves \|\| r\.name\)/.test(PAGE)
  && /fmtInt\(failed\) \+ \(scanned \? ' of ' \+ fmtInt\(scanned\) : ''\)/.test(PAGE));
check('four measures between two dividers: Rows scanned · Rows failed · First seen · Run hash',
  /\.bd-measures\{display:grid;grid-template-columns:repeat\(4,1fr\)[^}]*border-top:1px solid[^}]*border-bottom:1px solid/.test(CSS)
  && ['Rows scanned', 'Rows failed', 'First seen', 'Run hash'].every(k => PAGE.includes('<div class="k">' + k + '</div>'))
  && /run\.runHash\.slice\(0, 16\)/.test(PAGE));
check('the read-only note, then the decisions: Send to cleansing filled, the rest bordered',
  PAGE.includes('Nothing on this screen writes to either database.')
  && /class="btn btn-primary" onclick="asCleanse\(/.test(PAGE)
  && /class="btn btn-ghost" onclick="asBdToggle\(\\'as-bd-rows\\'\)/.test(PAGE)
  && />View the ' \+ fmtInt\(failed\) \+ ' row'/.test(PAGE)
  && />Show the SQL<\/button>/.test(PAGE)
  && /onclick="asSuppressBreach\(\\'' \+ b\.ruleId \+ '\\'\)"[^>]*>Accept risk<\/button>/.test(PAGE));
check('Accept risk is the existing suppression — a reason and an expiry, on the audit trail',
  /function asSuppressBreach\(ruleId\)\{\s*const reason = prompt\('Suppression reason \(required/.test(PAGE));
check('the rows and the SQL are folded and only open on request; a run without a sample disables the button',
  /function asBdToggle\(id\)/.test(PAGE) && /<details id="as-bd-rows">/.test(PAGE) && /<details id="as-bd-sql">/.test(PAGE)
  && /disabled title="No sampled rows are retained for this breach"/.test(PAGE));
check('the diagnosis and the audit trail survive beneath the decisions',
  PAGE.indexOf('<div class="bd-more">') < PAGE.indexOf('<div class="diag"><div class="diag-h">Diagnosis')
  && /it cannot open, close or change a breach/.test(PAGE) && /<div class="panel-title">Audit trail<\/div>/.test(PAGE));

/* ── 8. Drawer and evidence ────────────────────────────────────────────── */
check('the drawer\'s first required field is still "What it proves — one plain-English sentence, present tense"',
  /<label>What it proves — one plain-English sentence, present tense<\/label>\s*<input class="inp" id="as-dw-proves"/.test(PAGE));
check('the drawer, the save gate and the evidence exports are untouched',
  /id="as-dw-save" onclick="asSaveFromDrawer\(\)" disabled/.test(PAGE) && /asDryRun\(\)/.test(PAGE)
  && /asExportStatementPdf/.test(PAGE) && /asExportRunLog/.test(PAGE));

/* ── 9. Type discipline and fonts ──────────────────────────────────────── */
const small = (cssNoComments.match(/font-size:\s*(\d+(?:\.\d+)?)px/g) || [])
  .map(m => Number(m.match(/[\d.]+/)[0])).filter(n => n < 11);
check('nothing in the page stylesheet is set below 11px', small.length === 0, small.join(', '));
const inlineSmall = (PAGE.replace(CSS, '').match(/font-size:\s*(\d+(?:\.\d+)?)px/g) || [])
  .map(m => Number(m.match(/[\d.]+/)[0])).filter(n => n < 13);
check('and no inline style in the markup or the renderers goes below 13px', inlineSmall.length === 0, inlineSmall.join(', '));
check('no border-radius, no rgba() colours, no Google Fonts — the tokens do the theming',
  !/border-radius/.test(cssNoComments) && !/rgba\(/.test(cssNoComments)
  && !/fonts\.googleapis\.com/.test(PAGE) && /font-family:var\(--font-body\)/.test(CSS));
check('the page keeps the shared tokens out of its :root',
  !/:root\{[^}]*--(bg|text|accent|green|red)\s*:/.test(CSS));
check('every visible button is filled, bordered or a link — no border-transparent ghosts',
  /\.btn:not\(\.btn-primary\):not\(\.link\)\{border-color:var\(--color-divider\)/.test(CSS)
  && !/class="btn"/.test(PAGE));
check('the sidebar mounts under assurance and the tab strip has somewhere to go',
  /data-active="assurance"/.test(PAGE) && /id="cyg-subnav-mount"/.test(PAGE));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
if (fail) process.exit(1);
