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
const applyStmts = E.enApplyStatements(accepted, { schema: 'dbo', table: 'crm_contacts',
  keyColumn: 'row_key', dialect: 'sqlserver', runId: 'enr_1', date: '2026-08-23' });
check('the statement list is one statement per proposal, and the script is that list with a header',
  applyStmts.length === accepted.length
  && applyStmts.every(s => /^UPDATE \[dbo\]\.\[crm_contacts\] SET /.test(s) && /;$/.test(s))
  && applyStmts.every(s => apply.includes(s)),
  JSON.stringify(applyStmts));
check('the rollback list matches its script too — batched execution cannot drift from the artifact',
  E.enRollbackStatements(accepted, { schema: 'dbo', table: 'crm_contacts', keyColumn: 'row_key',
    dialect: 'sqlserver', runId: 'enr_1' }).every(s => rb.includes(s)));
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

// executing an apply stamps what was written — the accept AND the write survive
const store2 = E.enNewStore(now);
const run2 = E.enRecordRun(store2, { projectId: 'p1', sourceTable: 'dbo.crm_contacts', seed: 41889 }, now);
E.enStageProposals(store2, run2.runId, rec.proposals, now);
const ids2 = store2.proposals.slice(0, 2).map(p => p.proposalId);
E.enVerdict(store2, ids2[0], 'accepted', null, 'op@x', now + 1);
const marked = E.enMarkApplied(store2, ids2, { via: 'page-execute', rollbackJob: 'enrichment_rollback_r1' },
  'op@x', now + 2);
check('applied proposals are stamped with who, when and the rollback that reverses them',
  marked === 2 && store2.proposals[0].appliedAt === now + 2
  && store2.proposals[0].appliedBy === 'op@x'
  && store2.proposals[0].rollbackJob === 'enrichment_rollback_r1'
  && store2.proposals[0].verdictTrail.map(t => t.verdict).join(',') === 'accepted,applied',
  JSON.stringify(store2.proposals[0].verdictTrail));
check('a second apply cannot double-stamp what is already written',
  E.enMarkApplied(store2, ids2, {}, 'op@x', now + 9) === 0
  && store2.proposals[0].appliedAt === now + 2);
check('only the ids handed in are stamped — a partial write marks only what went through',
  store2.proposals.slice(2).every(p => !p.appliedAt));

// ── 9. Sessions: leave and come back ────────────────────────────────────────
const SETUP = { side: 'src', table: 'dbo.crm_contacts',
  columns: [{ name: 'row_key', nullPct: 0 }, { name: 'job_title', nullPct: 60 }],
  rowCount: 4812, anchors: [{ anchor: 'Email', column: 'contact_email', coverage: 92, enabled: true }],
  keyColumn: 'row_key', mappings: MAPPINGS, inference: false, refTable: 'dbo.ref_accounts_clean',
  refAnchor: 'company', floor: 0.82, nlText: 'Only UK accounts', ruleJson: comp.ruleJson,
  runId: 'enr_x1', projectId: 'p1' };
const s3 = E.enNewStore(now);
const sesA = E.enSaveSession(s3, 'Finance backfill', SETUP, 'op@x', now);
check('a saved session carries the whole screen, columns included, so resuming needs no database',
  sesA.table === 'dbo.crm_contacts' && sesA.columns.length === 2 && sesA.rowCount === 4812
  && sesA.mappings.length === MAPPINGS.length && sesA.floor === 0.82
  && sesA.inference === false && sesA.ruleJson === comp.ruleJson && sesA.runId === 'enr_x1',
  JSON.stringify(Object.keys(sesA)));
const sesB = E.enSaveSession(s3, 'Finance backfill',
  Object.assign({}, SETUP, { floor: 0.6 }), 'op@x', now + 10);
check('saving over a name versions it rather than destroying the earlier copy',
  sesB.version === 2 && s3.sessions.length === 2 && s3.sessions[0].floor === 0.82);
check('the session list offers the newest version of each name, most recent first',
  E.enSessions(s3, 'p1').length === 1 && E.enSessions(s3, 'p1')[0].version === 2);
E.enSaveSession(s3, 'Other project', Object.assign({}, SETUP, { projectId: 'p2' }), 'op@x', now + 20);
check('sessions are scoped to their project', E.enSessions(s3, 'p1').length === 1
  && E.enSessions(s3, 'p2').length === 1 && E.enSessions(s3).length === 2);
check('a saved session is an audit event, not a silent write',
  s3.events.filter(e => e.type === 'session.saved').length === 3);
check('deleting a name removes every version — the oldest copy does not resurface',
  E.enDeleteSession(s3, 'Finance backfill', 'op@x', now + 30) === 2
  && E.enSessions(s3, 'p1').length === 0
  && s3.events.some(e => e.type === 'session.deleted' && e.versions === 2));

const s4 = E.enNewStore(now);
E.enSetDraft(s4, 'p1', SETUP, now);
E.enSetDraft(s4, 'p2', Object.assign({}, SETUP, { table: 'dbo.leads' }), now + 1);
check('the draft is per project, so two projects do not overwrite each other',
  E.enDraft(s4, 'p1').table === 'dbo.crm_contacts' && E.enDraft(s4, 'p2').table === 'dbo.leads'
  && E.enDraft(s4, 'p1').savedAt === now);
E.enClearDraft(s4, 'p1');
check('starting fresh clears only that project\'s draft',
  E.enDraft(s4, 'p1') === null && E.enDraft(s4, 'p2') !== null);
check('a store written before sessions existed still loads',
  (() => { const old = { v: 1, runs: [], proposals: [], rulesets: [], events: [], settings: {} };
    return E.enSessions(old).length === 0 && E.enDraft(old, 'p1') === null; })());

// a reopened run must rebuild all three result tabs, so the run keeps them
const s5 = E.enNewStore(now);
const run5 = E.enRecordRun(s5, { projectId: 'p1', sourceTable: 'dbo.crm_contacts', rowCount: 3,
  conflicts: [{ rowKey: 'R1', name: 'industry', chosen: 'Manufacturing', rejected: [], reason: 'waterfall' }],
  unenrichable: [{ rowKey: 'R2', anchorValue: '(null)', reason: 'no anchor' }] }, now);
check('a run keeps its conflicts and unenrichable rows — reopening shows the same three tabs',
  E.enRun(s5, run5.runId).conflicts.length === 1
  && E.enRun(s5, run5.runId).unenrichable.length === 1
  && E.enRun(s5, run5.runId).conflictsTruncated === 0);
const big = Array.from({ length: 640 }, (_, i) => ({ rowKey: 'R' + i, reason: 'x' }));
const run6 = E.enRecordRun(s5, { projectId: 'p1', unenrichable: big }, now + 1);
check('an enormous run is capped, and says how much it dropped rather than pretending',
  run6.unenrichable.length === 500 && run6.unenrichableTruncated === 140);
check('the resume list is newest first and scoped to the project',
  E.enRuns(s5, 'p1')[0].runId === run6.runId && E.enRuns(s5, 'p9').length === 0
  && E.enRun(s5, 'nope') === null);

// ── 10. Table picker ────────────────────────────────────────────────────────
// The source table is searched, not remembered. Ranking, inline completion
// and default-schema resolution are decisions, so they live in the engine.
const CATALOG = [
  { schema: 'dbo', table: 'accounts', full: 'dbo.accounts' },
  { schema: 'dbo', table: 'crm_contacts', full: 'dbo.crm_contacts' },
  { schema: 'dbo', table: 'ref_accounts_clean', full: 'dbo.ref_accounts_clean' },
  { schema: 'sales', table: 'crm_leads', full: 'sales.crm_leads' },
  { schema: 'stg', table: 'accounts', full: 'stg.accounts' },
];
const names = (q) => E.enRankTables(CATALOG, q).map(t => t.full);
check('an empty query lists the whole catalogue', E.enRankTables(CATALOG, '').length === 5);
check('typing a schema narrows to that schema',
  JSON.stringify(names('dbo')) === JSON.stringify(['dbo.accounts', 'dbo.crm_contacts', 'dbo.ref_accounts_clean']),
  JSON.stringify(names('dbo')));
check('a bare fragment searches table names across every schema, prefix matches first',
  JSON.stringify(names('crm')) === JSON.stringify(['dbo.crm_contacts', 'sales.crm_leads']),
  JSON.stringify(names('crm')));
check('a mid-word fragment still finds the table — searching beats remembering',
  JSON.stringify(names('contacts')) === JSON.stringify(['dbo.crm_contacts']), JSON.stringify(names('contacts')));
check('a qualified fragment respects the schema the operator typed',
  JSON.stringify(names('sales.crm')) === JSON.stringify(['sales.crm_leads'])
  && names('sales.acc').length === 0, JSON.stringify(names('sales.crm')));
check('a fragment matching nothing ranks nothing — no consolation guesses',
  E.enRankTables(CATALOG, 'invoices').length === 0);
check('inline autofill only ever extends what was typed',
  E.enInlineCompletion('dbo.crm', CATALOG) === 'dbo.crm_contacts'
  && E.enInlineCompletion('crm', CATALOG) === null
  && E.enInlineCompletion('dbo.accounts', CATALOG) === null,
  String(E.enInlineCompletion('crm', CATALOG)));
check('a bare name takes the default schema',
  E.enResolveTable('crm_contacts', CATALOG, 'dbo') === 'dbo.crm_contacts'
  && E.enResolveTable('not_a_table', CATALOG, 'dbo') === 'dbo.not_a_table');
check('a bare name that exists in exactly one other schema resolves there, not to dbo',
  E.enResolveTable('crm_leads', CATALOG, 'dbo') === 'sales.crm_leads');
check('a bare name in several schemas takes the default rather than picking a favourite',
  E.enResolveTable('accounts', CATALOG, 'dbo') === 'dbo.accounts');
check('postgres gets its own default schema',
  E.enResolveTable('crm_contacts', [], 'public') === 'public.crm_contacts');
check('bracket-quoted input is accepted and echoed in the catalogue\'s casing',
  E.enResolveTable('[dbo].[CRM_Contacts]', CATALOG, 'dbo') === 'dbo.crm_contacts'
  && E.enResolveTable('', CATALOG, 'dbo') === '');

// ── 11. Wiring ──────────────────────────────────────────────────────────────
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
check('apply offers both routes: stage the pair as jobs, or execute from the page',
  /cygenix_jobs/.test(PAGE) && /enRollbackSql/.test(PAGE) && /enApplySql/.test(PAGE)
  && /onclick="enRegisterJobs\(\)"/.test(PAGE) && /onclick="enExecuteApply\(\)"/.test(PAGE));
// Everything before the Apply dialog is read-only. Executing is the one write,
// and it is gated: profile guard, explicit confirm, rollback saved first.
const EXEC = (PAGE.match(/async function enExecuteApply\(\)\{[\s\S]*?\n\}/) || [''])[0];
check('executing asks for confirmation and passes the connection-profile write guard',
  /confirm\(/.test(EXEC) && /cpPageGuardWrite\('Data Enrichment apply'\)/.test(EXEC)
  && /cpGuardWrite/.test(PAGE) && /requiresTypedConfirm/.test(PAGE));
check('the rollback job is registered BEFORE the first UPDATE is sent',
  EXEC.indexOf('enRegisterJobs(true)') > -1
  && EXEC.indexOf('enRegisterJobs(true)') < EXEC.indexOf('dbExec(batch'),
  String(EXEC.indexOf('enRegisterJobs(true)')) + ' vs ' + String(EXEC.indexOf('dbExec(batch')));
check('the write goes out in batches and only what actually landed is marked applied',
  /E_APPLY_BATCH/.test(EXEC) && /enApplyStatements/.test(EXEC)
  && /accepted\.slice\(0, doneStmts\)/.test(EXEC) && /enMarkApplied/.test(EXEC));
check('an executed apply is recorded as an event and a profile run record, error included',
  /apply\.executed/.test(EXEC) && /cpRecordRun/.test(EXEC) && /rollbackJob: rollbackJob\.name/.test(EXEC));
check('a written proposal is spent: no re-verdict, no re-apply, and it says so in the row',
  /if \(p && p\.appliedAt\) return;/.test(PAGE)
  && /filter\(p => !p\.appliedAt\)/.test(PAGE)
  && /b-applied">written/.test(PAGE)
  && /p\.proposed != null && !p\.appliedAt/.test(PAGE));
check('runs write a profile run record when profiles are in force',
  /cpRecordRun/.test(PAGE));
check('external providers are honest about being unconfigured, not silently skipped',
  /Configure in Integrations|not configured/i.test(PAGE));
check('both table fields are comboboxes over the live catalogue, not free text',
  /id="en-table"[^>]*role="combobox"/.test(PAGE) && /id="en-ref-table"[^>]*role="combobox"/.test(PAGE)
  && /INFORMATION_SCHEMA\.TABLES/.test(PAGE) && /enRankTables/.test(PAGE)
  && /enInlineCompletion/.test(PAGE) && /enResolveTable/.test(PAGE));
check('the picker menu escapes the panel\'s overflow:hidden instead of being clipped by it',
  /\.cb-menu\{position:fixed/.test(PAGE)
  && PAGE.indexOf('<div class="cb-menu" id="en-table-menu"') > PAGE.indexOf('</div>\n\n<div class="scrim"') - 400);
check('the page saves and restores sessions, and resumes the draft on boot',
  /onclick="enSaveSessionAs\(\)"/.test(PAGE) && /onclick="enOpenSessions\(\)"/.test(PAGE)
  && /onclick="enNewSession\(\)"/.test(PAGE)
  && /E\.enDraft\(store, projectId\(\)\)/.test(PAGE) && /restoreSetup\(draft,/.test(PAGE)
  && /Resumed where you left off/.test(PAGE));
check('the draft is written on every state change, debounced, not on a Save button alone',
  (PAGE.match(/touchDraft\(\)/g) || []).length >= 10
  && /clearTimeout\(draftTimer\)/.test(PAGE));
check('the debounce can never cost a last edit — hiding or leaving the page flushes it',
  /function flushDraft/.test(PAGE) && /addEventListener\('pagehide', flushDraft\)/.test(PAGE)
  && /visibilityState === 'hidden'/.test(PAGE)
  && /draftTimer = setTimeout\(flushDraft, 400\)/.test(PAGE));
check('restoring redraws from the saved columns instead of re-reading the database',
  /function restoreSetup/.test(PAGE)
  && !/restoreSetup[\s\S]{0,1200}?dbExec/.test(PAGE)
  && /columns = s\.columns \|\| \[\]/.test(PAGE)
  && /\(restored\)/.test(PAGE));
check('a run can be reopened for review, with its conflicts and unenrichable rows',
  /function reopenRun/.test(PAGE) && /E\.enRun\(store, runId\)/.test(PAGE)
  && /run\.conflicts \|\| \[\]/.test(PAGE) && /conflicts: allConflicts, unenrichable: unenrichable/.test(PAGE)
  && /conflictsTruncated/.test(PAGE));
check('the default schema is dialect-aware, so postgres is not told its tables live in dbo',
  /function defaultSchema\(\)\{ return dialectOf\(\) === 'postgres' \? 'public' : 'dbo'; \}/.test(PAGE)
  && !/parts\.length > 1 \? parts\[0\] : 'dbo'/.test(PAGE));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
