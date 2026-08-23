// Tests for cygenix-profiles.js — connection profiles as a first-class object.
//
// The stakes are the handover's five rules: every runnable artifact has
// exactly one binding and an unbound artifact cannot run; resolution comes
// from the binding, never UI state; active profiles are immutable in their
// connection references; run time asserts identity before work; every run
// writes a record with expected AND actual identity. Plus the sentinel SQL
// pitfalls that were found the hard way — they must survive generation.
const P = require('../public/cygenix-profiles.js');
const fs = require('fs');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};
const throws = (fn, re) => {
  try { fn(); return false; } catch (e) { return re ? re.test(String(e.message)) : true; }
};

console.log('Connection profiles — engine\n');

const now = 1000000000000;
const CONNS = [
  { id: 'sc_src_prd', side: 'src', mode: 'direct', name: 'Finance prod source',
    connString: 'mssql://svc_fin:pw@sqlprod01.internal:1433/Conversion_DM' },
  { id: 'sc_tgt_prd', side: 'tgt', mode: 'azure', name: 'cygenix-cloud',
    fnUrl: 'https://cygenix-cloud.azurewebsites.net/api/sql', fnKey: 'k123' },
  { id: 'sc_src_dev', side: 'src', mode: 'direct', name: 'Dev source',
    connString: 'Server=dev01;Database=fin_dev;User Id=u;Password=p;' },
  { id: 'sc_tgt_dev', side: 'tgt', mode: 'direct', name: 'Dev target',
    connString: 'postgres://u:p@dev02/fin_dev_tgt' },
];

// ── 1. Connection meta and the standard name ────────────────────────────────
const store = P.cpNewStore(now);
P.cpSetConnMeta(store, 'sc_src_prd', { envClass: 'PRD', systemDomain: 'FINANCE', owner: 'C Morris' }, 'u1', now);
P.cpSetConnMeta(store, 'sc_tgt_prd', { envClass: 'PRD', systemDomain: 'FINANCE', owner: 'C Morris' }, 'u1', now);
P.cpSetConnMeta(store, 'sc_src_dev', { envClass: 'DEV', systemDomain: 'FINANCE' }, 'u1', now);
check('unclassified connections read as UNKNOWN — never a silent default environment',
  P.cpConnMeta(store, 'sc_tgt_dev').envClass === 'UNKNOWN');
check('the standard name is ENV_ROLE_SYSTEM_DATABASE — environment first, sorted by blast radius',
  P.cpStandardName(store, CONNS[0]) === 'PRD_SRC_FINANCE_Conversion_DM',
  P.cpStandardName(store, CONNS[0]));
check('an azure-function connection derives its database token from the endpoint host',
  P.cpStandardName(store, CONNS[1]) === 'PRD_TGT_FINANCE_cygenix-cloud');
check('ADO.NET and postgres-URL strings both yield the database name',
  P.cpDatabaseOf(CONNS[2]) === 'fin_dev' && P.cpDatabaseOf(CONNS[3]) === 'fin_dev_tgt');

// ── 2. Profiles: validation, immutability, supersession ────────────────────
check('a cross-environment pairing is a validation error, named in plain words',
  P.cpValidateProfile(store, { id: 'X', envClass: 'PRD', srcConnId: 'sc_src_dev', tgtConnId: 'sc_tgt_prd' }, CONNS)
    .some(e => /classified DEV but the profile is PRD/.test(e)));
check('an UNKNOWN-classified connection cannot join an active-bound profile',
  P.cpValidateProfile(store, { id: 'X', envClass: 'PRD', srcConnId: 'sc_src_prd', tgtConnId: 'sc_tgt_dev' }, CONNS)
    .some(e => /classified UNKNOWN/.test(e)));
check('sides are enforced — a src-saved connection cannot be a profile target',
  P.cpValidateProfile(store, { id: 'X', envClass: 'PRD', srcConnId: 'sc_src_prd', tgtConnId: 'sc_src_prd' }, CONNS)
    .some(e => /saved as a src connection/.test(e)));

const prof = P.cpSaveProfile(store, { id: 'FIN-PRD-01', name: 'Finance production', envClass: 'PRD',
  srcConnId: 'sc_src_prd', tgtConnId: 'sc_tgt_prd', credentialRef: 'svc_cyg_fin_prd',
  owner: 'C Morris', approver: 'Data Owner' }, 'u1', now);
check('a new profile is a draft — nothing runs under it yet', prof.status === 'draft');
check('a valid profile activates with an effective-from stamp',
  P.cpActivateProfile(store, 'FIN-PRD-01', CONNS, 'u1', now + 1).status === 'active'
  && prof.effectiveFrom === now + 1);
check('an active profile refuses connection changes — a changed endpoint is a NEW profile',
  throws(() => P.cpSaveProfile(store, { id: 'FIN-PRD-01', tgtConnId: 'sc_tgt_dev' }, CONNS, 'u1', now + 2),
    /immutable.*supersedes/i));
check('non-connection fields (owner, approver) stay editable on an active profile',
  P.cpSaveProfile(store, { id: 'FIN-PRD-01', approver: 'New Approver' }, CONNS, 'u1', now + 2).approver === 'New Approver');
check('a connection referenced by a non-retired profile is locked',
  P.cpIsConnLocked(store, 'sc_tgt_prd') === true && P.cpIsConnLocked(store, 'sc_tgt_dev') === false);
check('activation refuses an invalid profile',
  (() => { P.cpSaveProfile(store, { id: 'BAD-01', envClass: 'PRD', srcConnId: 'sc_src_dev', tgtConnId: 'sc_tgt_prd' }, 'u1', now);
    return throws(() => P.cpActivateProfile(store, 'BAD-01', CONNS, 'u1', now), /cannot activate/i); })());

// supersession: retire, create v2 pointing back
P.cpSaveProfile(store, { id: 'FIN-PRD-02', name: 'Finance production v2', envClass: 'PRD',
  srcConnId: 'sc_src_prd', tgtConnId: 'sc_tgt_prd', supersedesProfileId: 'FIN-PRD-01' }, 'u1', now + 3);
check('supersession is recorded as lineage, not an edit',
  store.profiles.find(p => p.id === 'FIN-PRD-02').supersedesProfileId === 'FIN-PRD-01');

// ── 3. Bindings and the choke point ─────────────────────────────────────────
const empty = P.cpNewStore(now);
check('before any profile exists, resolution reports the ambient fallback honestly',
  P.cpResolve(empty, 'job', 'job_1', CONNS, {}).checked === false);

P.cpBind(store, 'job', 'job_42', 'FIN-PRD-01', 'u1', now + 4);
check('a binding is exactly one per artifact — rebinding replaces, with an event',
  (P.cpBind(store, 'job', 'job_42', 'FIN-PRD-01', 'u2', now + 5),
   store.bindings.filter(b => b.artifactId === 'job_42').length === 1
   && store.events.some(e => e.type === 'binding.rebound')));

const res = P.cpResolve(store, 'job', 'job_42', CONNS, { intent: 'write' });
check('resolution comes from the binding: profile, env class and BOTH connection values',
  res.ok && res.profile.id === 'FIN-PRD-01' && res.envClass === 'PRD'
  && res.src === CONNS[0].connString
  && res.tgt === 'https://cygenix-cloud.azurewebsites.net/api/sql?code=k123');
check('an unbound artifact cannot run — with the fix named',
  (() => { const r = P.cpResolve(store, 'job', 'job_unbound', CONNS, {});
    return !r.ok && /not bound/.test(r.why) && /Profiles page/.test(r.why); })());
check('a draft profile cannot run',
  (() => { P.cpBind(store, 'job', 'job_draft', 'FIN-PRD-02', 'u1', now);
    const r = P.cpResolve(store, 'job', 'job_draft', CONNS, {});
    return !r.ok && /draft/.test(r.why); })());
check('a retired profile refuses and points at its successor path',
  (() => { const s = P.cpNewStore(now);
    P.cpSetConnMeta(s, 'sc_src_prd', { envClass: 'PRD', systemDomain: 'FIN' }, 'u', now);
    P.cpSetConnMeta(s, 'sc_tgt_prd', { envClass: 'PRD', systemDomain: 'FIN' }, 'u', now);
    P.cpSaveProfile(s, { id: 'P1', envClass: 'PRD', srcConnId: 'sc_src_prd', tgtConnId: 'sc_tgt_prd' }, 'u', now);
    P.cpActivateProfile(s, 'P1', CONNS, 'u', now);
    P.cpBind(s, 'map', 'm1', 'P1', 'u', now);
    P.cpRetireProfile(s, 'P1', 'u', now + 1);
    const r = P.cpResolve(s, 'map', 'm1', CONNS, {});
    return !r.ok && /retired/.test(r.why) && /superseded/.test(r.why); })());
check('an UNKNOWN environment blocks writes but permits reads (migration §6)',
  (() => { const s = P.cpNewStore(now);
    P.cpSetConnMeta(s, 'sc_src_prd', { envClass: 'UNKNOWN' }, 'u', now);
    P.cpSaveProfile(s, { id: 'U1', envClass: 'UNKNOWN', srcConnId: 'sc_src_prd', tgtConnId: 'sc_tgt_prd' }, 'u', now);
    s.profiles[0].status = 'active';       /* forced past validation, as migration data would be */
    P.cpBind(s, 'job', 'j', 'U1', 'u', now);
    const w = P.cpResolve(s, 'job', 'j', CONNS, { intent: 'write' });
    const r = P.cpResolve(s, 'job', 'j', CONNS, { intent: 'read' });
    return !w.ok && /UNKNOWN/.test(w.why) && r.ok; })());
check('a profile whose connection was deleted from the saved list refuses with the id',
  (() => { const r = P.cpResolve(store, 'job', 'job_42', CONNS.slice(1), {});
    return !r.ok && /sc_src_prd/.test(r.why); })());

// ── 4. The write guard for tool pages ───────────────────────────────────────
check('unadopted store: the guard is permissive and says why',
  (() => { const g = P.cpGuardWrite(P.cpNewStore(now), {});
    return g.ok && g.checked === false && /ambient/.test(g.why); })());
check('adopted but no active selection: writes are blocked',
  (() => { const s = P.cpNewStore(now);
    P.cpSaveProfile(s, { id: 'X', envClass: 'DEV', srcConnId: 'a', tgtConnId: 'b' }, 'u', now);
    const g = P.cpGuardWrite(s, {});
    return !g.ok && /none is selected/.test(g.why); })());
check('a PRD profile passes the guard but demands a typed confirmation',
  (() => { store.settings.activeProfileId = 'FIN-PRD-01';
    const g = P.cpGuardWrite(store, {});
    return g.ok && g.requiresTypedConfirm === true && g.envClass === 'PRD'; })());

// ── 5. The sentinel SQL — pitfalls must survive generation ─────────────────
const exp = { profileId: 'FIN-PRD-01', envClass: 'PRD', role: 'TGT' };
const ms = P.cpAssertionSql('sqlserver', exp);
check('T-SQL asserts via sp_executesql — a static read of a missing table dies at compile time',
  /sp_executesql/.test(ms) && /OBJECT_ID\('dbo\._cyg_env_marker'/.test(ms));
check('the empty table is guarded explicitly — NULL <> value is UNKNOWN and would let the run through',
  /ENV SENTINEL EMPTY/.test(ms) && /@actual_profile IS NULL/.test(ms));
check('THROW, never RAISERROR — no % substitution on data-built messages',
  /THROW 5000[123]/.test(ms) && !/RAISERROR\s*\(/.test(ms));
check('NO GO anywhere — through ADO.NET/JDBC/ODBC it is a syntax error that silently disables the check',
  !/^\s*GO\s*$/mi.test(ms));
check('the expected triple is embedded and quoted',
  ms.includes("'FIN-PRD-01'") && ms.includes("'PRD'") && ms.includes("'TGT'"));
check("a quote inside a profile id cannot break out",
  P.cpAssertionSql('sqlserver', { profileId: "O'Brien", envClass: 'PRD', role: 'TGT' }).includes("'O''Brien'"));

const pg = P.cpAssertionSql('postgres', exp);
check('PostgreSQL reads via EXECUTE…INTO — a static SELECT resolves at DO-block compile time',
  /EXECUTE 'SELECT profile_id/.test(pg));
check('GET DIAGNOSTICS, not NOT FOUND — EXECUTE does not set FOUND',
  /GET DIAGNOSTICS n_rows = ROW_COUNT/.test(pg) && !/IF NOT FOUND/.test(pg));
check('IS DISTINCT FROM, not <> — a NULL operand must not let the run through',
  /IS DISTINCT FROM/.test(pg) && !/a_profile\s*<>/.test(pg));
check("relkind IN ('r','p') — to_regclass alone matches views and sequences",
  /relkind IN \('r', 'p'\)/.test(pg));

const inst = P.cpSentinelInstallSql('sqlserver', { envClass: 'PRD', role: 'TGT', profileId: 'FIN-PRD-01',
  systemDomain: 'FINANCE', standardName: 'PRD_TGT_FINANCE_cygenix-cloud', owner: 'C Morris' });
check('the install seeds by MERGE with the single-row check constraint and the Azure-safe set_by default',
  /CHECK \(marker_id = 1\)/.test(inst) && /MERGE dbo\._cyg_env_marker/.test(inst)
  && /ISNULL\(SUSER_SNAME\(\), USER_NAME\(\)\)/.test(inst));
check('the install carries the restore warning — the one real hole in the design',
  /after ANY restore/.test(inst) && /after ANY restore/.test(P.cpSentinelInstallSql('postgres', exp)));
check('the fingerprint fallback is SELECT-only on both dialects',
  /^SELECT/i.test(P.cpFingerprintSql('sqlserver')) && /^SELECT/i.test(P.cpFingerprintSql('postgres')));

// ── 6. The sentinel classifier ──────────────────────────────────────────────
const good = { rows: [{ profile_id: 'FIN-PRD-01', env_class: 'PRD', role_in_profile: 'TGT', standard_name: 'PRD_TGT_FINANCE_cygenix-cloud' }] };
check('matching marker row → pass, with the actual identity captured',
  (() => { const c = P.cpClassifySentinel(exp, good);
    return c.result === 'pass' && c.actual.name === 'PRD_TGT_FINANCE_cygenix-cloud'; })());
check('a different profile in the marker → mismatch with expected-vs-actual prose',
  (() => { const c = P.cpClassifySentinel(exp, { rows: [{ profile_id: 'FIN-DEV-01', env_class: 'DEV', role_in_profile: 'TGT', standard_name: 'x' }] });
    return c.result === 'mismatch' && /Expected FIN-PRD-01 \/ PRD \/ TGT but connected to FIN-DEV-01/.test(c.detail); })());
check('no rows → sentinel-empty; a NULL field → sentinel-empty (never a silent pass)',
  P.cpClassifySentinel(exp, { rows: [] }).result === 'sentinel-empty'
  && P.cpClassifySentinel(exp, { rows: [{ profile_id: null }] }).result === 'sentinel-empty');
check('"invalid object name" / "relation does not exist" → sentinel-missing',
  P.cpClassifySentinel(exp, { error: "Invalid object name 'dbo._cyg_env_marker'." }).result === 'sentinel-missing'
  && P.cpClassifySentinel(exp, { error: 'relation "public._cyg_env_marker" does not exist' }).result === 'sentinel-missing');
check('the generated assertion errors classify themselves',
  P.cpClassifySentinel(exp, { error: 'ENV MISMATCH - run aborted. …' }).result === 'mismatch'
  && P.cpClassifySentinel(exp, { error: 'ENV SENTINEL EMPTY - …' }).result === 'sentinel-empty');
check('an unrelated error is not-checked — never claimed as a pass',
  P.cpClassifySentinel(exp, { error: 'timeout' }).result === 'not-checked');

// ── 7. Run records ──────────────────────────────────────────────────────────
const rr = P.cpRecordRun(store, { artifactType: 'quality-review', artifactId: 'rep1',
  profileId: 'FIN-PRD-01', actualServer: 'sqlprod01', actualDatabase: 'Conversion_DM',
  actualLogin: 'svc_fin', assertionResult: 'pass', outcome: 'success', runBy: 'u1' }, now + 10);
check('a run record carries expected AND actual identity',
  rr.profileId === 'FIN-PRD-01' && rr.actualDatabase === 'Conversion_DM'
  && rr.assertionResult === 'pass' && /^run_/.test(rr.runId));
check('run records are capped, oldest first out',
  (() => { const s = P.cpNewStore(now);
    for (let i = 0; i < P.RUN_RECORD_CAP + 20; i++) P.cpRecordRun(s, { artifactId: 'a' + i }, now + i);
    return s.runRecords.length === P.RUN_RECORD_CAP
      && s.runRecords[0].artifactId === 'a20'; })());

// ── 8. Banner and report naming ─────────────────────────────────────────────
check('unadopted: the banner says connections are ambient, neutral tier',
  (() => { const b = P.cpBannerInfo(P.cpNewStore(now));
    return b.tier === 'neutral' && /ambient/.test(b.text); })());
check('adopted with no selection: a warning banner',
  (() => { const s = P.cpNewStore(now);
    P.cpSaveProfile(s, { id: 'X', envClass: 'DEV', srcConnId: 'a', tgtConnId: 'b' }, 'u', now);
    return P.cpBannerInfo(s).tier === 'warn'; })());
check('a PRD profile banners in the danger tier with its id',
  (() => { const b = P.cpBannerInfo(store);
    return b.tier === 'danger' && b.profileId === 'FIN-PRD-01' && /FIN-PRD-01 · PRD/.test(b.text); })());
check('report names lead with the profile id — a governance record names its database',
  P.cpReportName(store, 'Source DB', new Date(Date.UTC(2026, 7, 23, 9, 58))).startsWith('FIN-PRD-01_2026-08-23_')
  && P.cpReportName(P.cpNewStore(now), 'Source DB', new Date(Date.UTC(2026, 7, 23, 9, 58))).startsWith('Source DB_'));

// ── 9. Wiring ───────────────────────────────────────────────────────────────
const PAGE = fs.existsSync(__dirname + '/../public/profiles.html')
  ? fs.readFileSync(__dirname + '/../public/profiles.html', 'utf8') : '';
const SIDE = fs.readFileSync(__dirname + '/../public/cygenix-sidebar.js', 'utf8');
const DASH = fs.readFileSync(__dirname + '/../public/dashboard-app.js', 'utf8');
const DQ = fs.readFileSync(__dirname + '/../public/data-quality.html', 'utf8');
const DG = fs.readFileSync(__dirname + '/../public/data-generator.html', 'utf8');

check('the Profiles page exists, loads the engine and mounts the sidebar',
  /cygenix-profiles\.js/.test(PAGE) && /data-active="profiles"/.test(PAGE));
check('the sidebar offers Profiles beside Connections and renders the environment banner',
  /key:'profiles'/.test(SIDE) && /cyg-envbar/.test(SIDE) && /cygenix_profiles_v1/.test(SIDE));
check('Save as… asks for the environment class and stores connection meta',
  /cpSetConnMeta|envClass/.test(DASH) && /UNKNOWN/.test(DASH));
check('locked connections refuse rename and delete',
  /cpIsConnLocked/.test(DASH));
check('Clear all is barred while active profiles reference the saved set',
  /active profile/i.test(DASH.slice(DASH.indexOf('function clearProjectConnections'),
    DASH.indexOf('function clearProjectConnections') + 1500)));
check('import, restore and linked-server writes pass the profile guard',
  (DASH.match(/cpPageGuardWrite\(/g) || []).length >= 4);
check('quality review names reports by profile and records expected-vs-actual identity',
  /cpReportName/.test(DQ) && /cpRecordRun/.test(DQ) && /cpClassifySentinel|cpSentinelReadSql/.test(DQ));
check('the data generator refuses unclassified writes and PRD without typed confirmation',
  /cpPageGuardWrite\(/.test(DG));
check('nothing on the profiles read path writes to the connections stores',
  !/setItem\('cygenix_project_connections'/.test(PAGE)
  && !/setItem\('cygenix_saved_connections'/.test(PAGE));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
