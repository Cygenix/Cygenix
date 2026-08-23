// Tests for cygenix-enrichment.js — the Data Enrichment engine.
//
// The stakes are the spec's honesty rules: verify-only fields are
// provider-attested or nothing; inferred values are stamped, capped at 0.80
// and can never pass as verified; rows without an anchor are counted, not
// dropped; the compiled JSON — not the English — executes, and what cannot
// be compiled comes back as a question; nothing here ever writes to a
// database — apply is staged SQL plus a working rollback.
const E = require('../public/cygenix-enrichment.js');
const fs = require('fs');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Data Enrichment — engine\n');

// ── 1. Anchors ──────────────────────────────────────────────────────────────
const anchors = E.enDetectAnchors([
  { name: 'contact_email', nullPct: 8 },
  { name: 'account_name', nullPct: 0 },
  { name: 'tel_1', nullPct: 56 },
  { name: 'notes', nullPct: 10 },
]);
check('email, company name, derived domain and phone are detected with coverage',
  anchors.length === 4
  && anchors.some(a => a.anchor === 'Email' && a.column === 'contact_email' && a.coverage === 92)
  && anchors.some(a => a.anchor === 'Company name' && a.coverage === 100)
  && anchors.some(a => a.anchor === 'Domain' && /derived from contact_email/.test(a.column))
  && anchors.some(a => a.anchor === 'Phone' && a.coverage === 44),
  JSON.stringify(anchors));
check('low-coverage anchors arrive disabled, not hidden',
  anchors.find(a => a.anchor === 'Phone').enabled === false
  && anchors.find(a => a.anchor === 'Email').enabled === true);
check('a table with no plausible anchor columns detects nothing — the page must refuse, not guess',
  E.enDetectAnchors([{ name: 'amount', nullPct: 0 }, { name: 'posted_at', nullPct: 0 }]).length === 0);

// ── 2. Normalisation and inference primitives ───────────────────────────────
check('company names normalise across suffix and case ("ACME NORTHERN LTD." ≡ "Acme Northern Limited")',
  E.enNormCompany('ACME NORTHERN LTD.') === E.enNormCompany('acme northern limited')
  && E.enNormCompany('Acme Northern') === 'acme northern');
check('titles expand and re-case: "Hd. of Fin." → "Head of Finance"',
  E.enNormTitle('Hd. of Fin.') === 'Head of Finance'
  && E.enNormTitle('it ops mgr') === 'IT Operations Manager');
check('seniority bands from titles',
  E.enSeniorityOf('Chief Technology Officer') === 'C-level'
  && E.enSeniorityOf('Head of Finance') === 'Director'
  && E.enSeniorityOf('IT Operations Manager') === 'Manager'
  && E.enSeniorityOf('Software Engineer') === 'IC'
  && E.enSeniorityOf('') === null);
check('department from titles, honest Other fallback',
  E.enDepartmentOf('Head of Finance') === 'Finance'
  && E.enDepartmentOf('IT Operations Manager') === 'IT'
  && E.enDepartmentOf('Poet in Residence') === 'Other');
check('country from a country TLD only — .com is honestly unknown',
  E.enCountryOfDomain('acme.co.uk') === 'GB'
  && E.enCountryOfDomain('acme.de') === 'DE'
  && E.enCountryOfDomain('acme.com') === null);
check('free email domains are recognised so they never become a company domain',
  E.enDomainOf('j.doe@gmail.com') === 'gmail.com');

// ── 3. Natural-language rules compile to JSON — or to questions ────────────
const ctx = { phoneColumn: 'tel_1', countryColumn: 'hq_country', statusColumn: 'company_status' };
const comp = E.enCompileRules(
  "Only enrich UK accounts that are missing a phone number. Don't touch anything where the account is "
  + 'marked dissolved. If revenue comes back in USD, convert to GBP at the FY25 average rate and note it. '
  + 'Prefer Companies House over anything else for legal names.', ctx);
check('the spec example compiles to three filters, one transform and a precedence pin',
  comp.ruleJson.filters.length === 3
  && comp.ruleJson.filters.some(f => f.column === 'hq_country' && f.op === 'in' && f.value.includes('GB'))
  && comp.ruleJson.filters.some(f => f.column === 'tel_1' && f.op === 'is_null')
  && comp.ruleJson.filters.some(f => f.column === 'company_status' && f.op === 'neq' && f.value === 'dissolved')
  && comp.ruleJson.transforms[0].op === 'fx_convert' && comp.ruleJson.transforms[0].rate_basis === 'FY25_avg'
  && comp.ruleJson.provider_precedence.company_name_legal[0] === 'companies_house',
  JSON.stringify(comp.ruleJson));
check('nothing came back as a question for fully-compilable text', comp.questions.length === 0);
check('an uncompilable sentence becomes a QUESTION, never a guess',
  (() => { const c = E.enCompileRules('Make the data feel more premium.', ctx);
    return c.ruleJson.filters.length === 0 && c.questions.length === 1
      && /could not compile/.test(c.questions[0]); })());

check('compiled filters render as a read-only WHERE predicate on both dialects',
  /\[hq_country\] IN \('GB', 'UK'\)/.test(E.enFiltersToSql(comp.ruleJson, 'sqlserver'))
  && /"hq_country" IN/.test(E.enFiltersToSql(comp.ruleJson, 'postgres'))
  && /\[tel_1\] IS NULL/.test(E.enFiltersToSql(comp.ruleJson, 'sqlserver')));
check('a quote inside a filter value cannot break out',
  /'O''Brien'/.test(E.enFiltersToSql({ filters: [{ column: 'name', op: 'eq', value: "O'Brien" }] }, 'sqlserver')));
check('no filters → 1=1, so an empty rule set never silently narrows scope',
  E.enFiltersToSql({ filters: [] }, 'sqlserver') === '1=1');

// ── 4. Reconciliation: waterfall, precedence, conflicts ────────────────────
const MAPPINGS = [
  { targetColumn: 'job_title', attr: 'job_title', policy: 'if-null' },
  { targetColumn: 'company_legal', attr: 'company_name_legal', policy: 'if-null' },
  { targetColumn: 'tel_direct', attr: 'phone_direct', policy: 'if-null' },
  { targetColumn: 'seniority', attr: 'seniority', policy: 'if-null' },
  { targetColumn: 'domain', attr: 'company_domain', policy: 'if-null' },
];
const ROW = { key: '1042', values: { job_title: null, company_legal: null, tel_direct: null,
  seniority: null, domain: null }, anchors: { email: 'j.smith@acme.co.uk' } };
const CANDS = [
  { provider: 'internal_ref', providerOrder: 1, matchScore: 0.95,
    attrs: { job_title: 'hd of fin' } },
  { provider: 'companies_house', providerOrder: 2, matchScore: 0.99,
    attrs: { company_name_legal: 'ACME NORTHERN LIMITED' } },
  { provider: 'contact_provider_a', providerOrder: 3, matchScore: 0.93,
    attrs: { job_title: 'Finance Director', company_name_legal: 'Acme Northern Ltd' } },
];
const rec = E.enReconcile(ROW, MAPPINGS, CANDS, { floor: 0.75,
  ruleJson: comp.ruleJson, inferenceEnabled: true });
const byAttr = {};
rec.proposals.forEach(p => byAttr[p.attr] = p);
check('the waterfall wins on order: the internal reference beats a later provider',
  byAttr.job_title.source === 'internal_ref' && byAttr.job_title.origin === 'internal');
check('the winning title is normalised on the way through',
  byAttr.job_title.proposed === 'Head of Finance', byAttr.job_title.proposed);
check('a genuine disagreement lands in Conflicts with the losing value and a reason',
  rec.conflicts.some(c => c.name === 'job_title' && c.rejected[0].value === 'Finance Director'
    && /waterfall/.test(c.reason)));
check('rule precedence pins company_name_legal to Companies House, and says so',
  byAttr.company_name_legal.source === 'companies_house');
check('LTD vs LIMITED is NOT a conflict — comparison is normalised',
  !rec.conflicts.some(c => c.name === 'company_name_legal'),
  JSON.stringify(rec.conflicts));

// ── 5. The honesty rules ────────────────────────────────────────────────────
check('a verify-only field with no provider value proposes NOTHING, with the reason recorded',
  byAttr.phone_direct.nothingProposed === true && byAttr.phone_direct.proposed === null
  && /verify-only/.test(byAttr.phone_direct.reason));
check('inference fills what honesty allows: seniority from the enriched title context, domain from the email',
  byAttr.seniority && byAttr.seniority.origin === 'inferred'
  && byAttr.company_domain && byAttr.company_domain.proposed === 'acme.co.uk');
check('every inferred value is capped at 0.80 and says it is excluded from accept-all-verified',
  rec.proposals.filter(p => p.origin === 'inferred')
    .every(p => p.confidence <= E.INFERENCE_CAP && /excluded from "accept all verified"/.test(p.reason)));
check('a free email domain never becomes a company domain',
  (() => { const r = E.enReconcile({ key: '9', values: { domain: null },
    anchors: { email: 'j.doe@gmail.com' } },
    [{ targetColumn: 'domain', attr: 'company_domain', policy: 'if-null' }], [], { inferenceEnabled: true });
    return !r.proposals.some(p => p.attr === 'company_domain' && p.proposed); })());
check('the if-null policy leaves a filled column alone',
  (() => { const r = E.enReconcile({ key: '9', values: { job_title: 'CFO' }, anchors: {} },
    [{ targetColumn: 'job_title', attr: 'job_title', policy: 'if-null' }], CANDS, {});
    return r.proposals.length === 0; })());
check('the project-level switch turns inference off entirely',
  (() => { const r = E.enReconcile(ROW, MAPPINGS, [], { inferenceEnabled: false });
    return !r.proposals.some(p => p.origin === 'inferred'); })());
check('a proposal under the confidence floor is flagged needs-review, not hidden',
  (() => { const r = E.enReconcile(ROW,
    [{ targetColumn: 'job_title', attr: 'job_title', policy: 'if-null' }],
    [{ provider: 'contact_provider_a', providerOrder: 1, matchScore: 0.6, attrs: { job_title: 'x' } }],
    { floor: 0.75 });
    return r.proposals[0].needsReview === true; })());

// ── 6. Estimate ─────────────────────────────────────────────────────────────
const est = E.enEstimate(4812, ['internal_ref', 'contact_provider_a', 'phone_verify'], 6);
check('the estimate prices the run before it starts — internal reference is free',
  est.lookups === 4812 * 3 && est.providerCost === Math.round(4812 * 0.012 * 100) / 100);

// ── 7. Apply + rollback SQL (staged, never executed here) ──────────────────
const accepted = [
  { rowKey: '1042', targetColumn: 'job_title', proposed: "Head of Finance", current: null,
    source: 'internal_ref', confidence: 0.93 },
  { rowKey: '1210', targetColumn: 'company_legal', proposed: "O'Brien & Co", current: 'obrien co',
    source: 'companies_house', confidence: 0.98 },
];
const apply = E.enApplySql(accepted, { schema: 'dbo', table: 'crm_contacts', keyColumn: 'row_key',
  dialect: 'sqlserver', runId: 'enr_x', date: '2026-08-23' });
check('apply SQL updates the value plus the provenance shadow columns',
  /UPDATE \[dbo\]\.\[crm_contacts\] SET \[job_title\] = 'Head of Finance', \[job_title_source\] = 'internal_ref', \[job_title_confidence\] = 0.93/.test(apply)
  && /WHERE \[row_key\] = '1042';/.test(apply));
check('a quote in a proposed value cannot break the script',
  /'O''Brien & Co'/.test(apply));
const rb = E.enRollbackSql(accepted, { schema: 'dbo', table: 'crm_contacts', keyColumn: 'row_key',
  dialect: 'sqlserver', runId: 'enr_x' });
check('the rollback restores every pre-enrichment value — NULL stays NULL',
  /SET \[job_title\] = NULL WHERE \[row_key\] = '1042'/.test(rb)
  && /SET \[company_legal\] = 'obrien co' WHERE \[row_key\] = '1210'/.test(rb));
check('both scripts open with the run id so a script on disk names its run',
  /run enr_x/.test(apply) && /run enr_x/.test(rb));

// ── 8. Store: verdicts are a trail, erasure leaves a tombstone ─────────────
const now = 1000000000000;
const store = E.enNewStore(now);
const run = E.enRecordRun(store, { projectId: 'p1', sourceTable: 'dbo.crm_contacts',
  rowCount: 100, rowsEnriched: 90, rowsUnenrichable: 10, seed: 41889 }, now);
E.enStageProposals(store, run.runId, rec.proposals, now);
check('a run is recorded with its seed — same inputs, same seed, same proposals',
  run.seed === 41889 && store.proposals.length === rec.proposals.length);
const pid = store.proposals[0].proposalId;
E.enVerdict(store, pid, 'accepted', null, 'op@x', now + 1);
E.enVerdict(store, pid, 'rejected', null, 'op@x', now + 2);
check('a changed verdict appends to the trail — the audit keeps both decisions',
  store.proposals[0].verdict === 'rejected' && store.proposals[0].verdictTrail.length === 2
  && store.proposals[0].verdictTrail[0].verdict === 'accepted');
const removed = E.enErase(store, run.runId, '1042', 'dpo@x', now + 3);
check('right-to-erasure purges the row\'s proposals and leaves a tombstone event',
  removed > 0 && !store.proposals.some(p => p.rowKey === '1042')
  && store.events.some(e => e.type === 'row.erased' && e.rowKey === '1042' && e.removed === removed));
const rs1 = E.enSaveRuleset(store, 'uk-only', 'Only UK', comp.ruleJson, 'op@x', now);
const rs2 = E.enSaveRuleset(store, 'uk-only', 'Only UK v2', comp.ruleJson, 'op@x', now + 5);
check('rulesets version rather than overwrite', rs1.version === 1 && rs2.version === 2);

// ── 9. Wiring ───────────────────────────────────────────────────────────────
const PAGE = fs.existsSync(__dirname + '/../public/data-enrichment.html')
  ? fs.readFileSync(__dirname + '/../public/data-enrichment.html', 'utf8') : '';
const SIDE = fs.readFileSync(__dirname + '/../public/cygenix-sidebar.js', 'utf8');
check('the sidebar offers Data Enrichment after Cleansing, before Validation',
  /key:'data-enrichment'/.test(SIDE)
  && SIDE.indexOf("key:'data-cleansing'") < SIDE.indexOf("key:'data-enrichment'")
  && SIDE.indexOf("key:'data-enrichment'") < SIDE.indexOf("key:'validation'"));
check('the page exists, loads the engine and mounts the sidebar under its key',
  /cygenix-enrichment\.js/.test(PAGE) && /data-active="data-enrichment"/.test(PAGE));
check('the five steps and the four result tabs are present',
  ['Source', 'Anchors', 'Fields to complete', 'Providers', 'Run'].every(s => PAGE.includes(s))
  && ['Proposals', 'Conflicts', 'Unenrichable', 'Audit'].every(s => PAGE.includes(s)));
check('the pre-run disclosure names what is transmitted and requires acknowledgement',
  /will be (sent|transmitted)/i.test(PAGE) && /disclosureAck|acknowledge/i.test(PAGE));
check('verify-only fields are called out on the page, and bulk actions include reject-AI-inferred',
  /provider-attested or nothing/i.test(PAGE) && /Reject.*AI-inferred/i.test(PAGE)
  && /Accept.*verified/i.test(PAGE));
check('apply registers staged SQL as jobs — the page itself never executes a write',
  /cygenix_jobs/.test(PAGE) && /enRollbackSql/.test(PAGE) && /enApplySql/.test(PAGE)
  && !/action:\s*'execute'[^}]*enApplySql/.test(PAGE));
check('runs write a profile run record when profiles are in force',
  /cpRecordRun/.test(PAGE));
check('external providers are honest about being unconfigured, not silently skipped',
  /Configure in Integrations|not configured/i.test(PAGE));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
