// tests/capabilities.test.js — the copy cannot get ahead of the code.
//
// FOUR CONTRADICTIONS, FOUND FROM OUTSIDE IN AN AFTERNOON
//
//   1. MySQL / MariaDB was sold on Pro, Business and Enterprise. There is no
//      MySQL driver in this repository — package.json declares mssql and pg.
//   2. PostgreSQL was sold from Pro upward and IS fully implemented, but the
//      help page said Cygenix connects "to your source and target SQL Server
//      databases". An evaluator reading the docs concluded the opposite of
//      the truth.
//   3. Server-level migration is the lead feature on the homepage and a
//      2,900-line module in the product. The help page mentioned it once.
//   4. The site claims role-based access; the help page told an administrator
//      to set a role in Netlify Identity — a product this codebase no longer
//      uses — in front of a ten-role matrix it did not mention.
//
// Two of those overclaim and two underclaim, which is the tell: nobody was
// exaggerating, the pages were simply written once and the product moved.
// Correcting the copy would buy months. This file is the mechanism that makes
// it structural: one manifest, and a build that fails when a page and the
// manifest disagree.
'use strict';

const fs = require('fs');
const path = require('path');
const CAP = require('../public/cygenix-capabilities.js');
const docs = require('../scripts/build-capability-docs.js');

const P = (...p) => path.join(__dirname, '..', ...p);
const read = (...p) => fs.readFileSync(P(...p), 'utf8');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};

console.log('Capabilities — one manifest, and pages that cannot contradict it\n');

/* ══ 1. The manifest is coherent ════════════════════════════════════════ */
{
  const bad = [];
  const seen = (list, kind) => {
    const ids = new Set();
    for (const e of list) {
      if (!e.id || !e.label) bad.push(kind + ' entry with no id/label');
      if (ids.has(e.id)) bad.push(kind + ' duplicate id: ' + e.id);
      ids.add(e.id);
      if (e.status && CAP.STATUSES.indexOf(e.status) === -1) {
        bad.push(kind + ' ' + e.id + ' has unknown status "' + e.status + '"');
      }
    }
  };
  seen(CAP.SOURCES, 'source'); seen(CAP.TARGETS, 'target');
  seen(CAP.FEATURES, 'feature'); seen(CAP.TIERS, 'tier');
  seen(CAP.SERVER_OBJECTS, 'server object');
  check('every manifest entry has an id, a label and a legal status', bad.length === 0, bad.join('; '));
}
{
  const orphans = CAP.PAIRS.filter(p => !CAP.source(p.source) || !CAP.target(p.target));
  check('every pair names a source and target that exist',
    orphans.length === 0, orphans.map(p => p.source + '→' + p.target).join(', '));
}
{
  // A status is a promise. Anything ga or beta must name what makes it true,
  // so "it is GA" is checkable rather than assertable.
  const unevidenced = [].concat(CAP.SOURCES, CAP.TARGETS, CAP.FEATURES)
    .filter(e => CAP.isSellable(e.status) && (!e.evidence || e.evidence === 'none'));
  check('everything sellable names the code that makes it true',
    unevidenced.length === 0, unevidenced.map(e => e.id).join(', '));

  // Something not sellable may still have code behind it — Data Stream does,
  // it simply runs on seeded data. What it must carry is a `detail` saying
  // what the limitation is, so "preview" is never left to the reader to
  // interpret.
  const unexplained = [].concat(CAP.SOURCES, CAP.TARGETS, CAP.FEATURES)
    .filter(e => !CAP.isSellable(e.status) && !e.detail);
  check('and anything not sellable explains what the limitation is',
    unexplained.length === 0, unexplained.map(e => e.id).join(', '));
}
{
  const badTier = CAP.FEATURES.filter(f => f.minTier && CAP.TIER_ORDER.indexOf(f.minTier) === -1);
  check('every feature\'s minimum plan is a real plan', badTier.length === 0,
    badTier.map(f => f.id + '→' + f.minTier).join(', '));
}

/* ══ 2. The manifest matches the code it describes ══════════════════════ */
{
  // The point of `evidence` is that it is checkable. These are the two that
  // caused contradictions 1 and 2, so they are checked directly.
  const pkg = JSON.parse(read('package.json'));
  const deps = Object.assign({}, pkg.dependencies, pkg.devDependencies);

  check('PostgreSQL is GA and the pg driver is really a dependency',
    CAP.source('postgres').status === 'ga' && !!deps.pg,
    'status=' + CAP.source('postgres').status + ' pg=' + deps.pg);
  check('SQL Server is GA and the mssql driver is really a dependency',
    CAP.source('mssql').status === 'ga' && !!deps.mssql);

  // The one that was being sold. If a driver ever lands, this fails and the
  // manifest should be promoted — which is the right way round.
  const mysqlDriver = !!(deps.mysql || deps.mysql2 || deps.mariadb);
  check('MySQL is marked planned, and there is genuinely no driver',
    CAP.source('mysql').status === 'planned' && !mysqlDriver,
    'driver present: ' + mysqlDriver);

  check('Server-level migration is claimed only because the module exists',
    CAP.feature('server_objects').status === 'beta'
    && fs.existsSync(P('public', 'server-migration.js')));
  check('RBAC is claimed only because the matrix exists',
    CAP.feature('rbac').status === 'ga' && /const MATRIX =/.test(read('netlify', 'functions', 'lib', 'rbac.js')));
  check('the audit trail is claimed only because the chain exists',
    /function chainEntry/.test(read('netlify', 'functions', 'lib', 'rbac.js')));

  // My own module must not be over-claimed either. Data Stream runs on seeded
  // demo data; `preview` is the honest status and the manifest says why.
  check('Data Stream is preview, and says it has no real connectivity',
    CAP.feature('data_stream').status === 'preview'
    && /demo data/i.test(CAP.feature('data_stream').detail || ''));
  check('row-level reconciliation is marked planned, because it is not built',
    CAP.feature('reconciliation_row_level').status === 'planned');
  check('shared workspaces are marked planned, because stores key per user',
    CAP.feature('tenancy').status === 'planned'
    && /projects\.js/.test(CAP.feature('tenancy').evidence || ''));
}

/* ══ 3. The generated documentation is current ══════════════════════════ */
{
  const help = read('public', 'help.html');
  check('help.html carries the generated-section markers',
    help.includes(docs.START) && help.includes(docs.END));
  const a = help.indexOf(docs.START), b = help.indexOf(docs.END);
  if (a !== -1 && b !== -1) {
    check('the committed matrix is exactly what the generator produces',
      help.slice(a, b + docs.END.length) === docs.render(),
      're-run node scripts/build-capability-docs.js and commit');
  }
  check('the matrix lists every source, target and feature',
    CAP.SOURCES.every(s => help.includes('>' + s.label + '<') || help.includes(s.label))
    && CAP.FEATURES.every(f => help.includes(f.label)));
}

/* ══ 4. THE CHECK — no page claims what the manifest does not support ═══
   For each feature, look for its claim phrases on the public pages. A phrase
   whose feature is not ga or beta must sit near a qualifier, or the build
   fails. This is what stops the copy drifting ahead again. */
{
  const PUBLIC_PAGES = ['index.html', 'pricing.html', 'help.html', 'about.html',
                        'privacy-security.html', 'pick-plan.html'];
  const QUALIFIER = /\b(planned|coming soon|on the roadmap|not yet|preview|in preview|beta|forthcoming|we do not|does not yet)\b/i;

  const offenders = [];
  for (const page of PUBLIC_PAGES) {
    if (!fs.existsSync(P('public', page))) continue;
    const html = read('public', page);
    // Strip the generated block: it labels its own statuses, and a chip that
    // says "Planned" would otherwise read as an unqualified claim.
    const a = html.indexOf(docs.START), b = html.indexOf(docs.END);
    const body = (a !== -1 && b !== -1) ? html.slice(0, a) + html.slice(b) : html;
    const text = body.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');

    for (const f of CAP.FEATURES) {
      if (CAP.isSellable(f.status)) continue;
      for (const phrase of (f.claims || [])) {
        const re = new RegExp(phrase.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
        const m = re.exec(text);
        if (!m) continue;
        // A qualifier within ~120 characters either side counts as labelled.
        const around = text.slice(Math.max(0, m.index - 120), m.index + phrase.length + 120);
        if (QUALIFIER.test(around)) continue;
        offenders.push(page + ' claims "' + phrase + '" (' + f.id + ' is ' + f.status
          + ') without a qualifier: …' + around.slice(60, 200).trim() + '…');
      }
    }
  }
  check('no public page claims a capability the manifest marks planned or unsupported',
    offenders.length === 0, offenders.slice(0, 3).join(' || '));
}

{
  // The same rule for sources and targets, which is what contradiction 1 was.
  const PUBLIC_PAGES = ['index.html', 'pricing.html', 'pick-plan.html'];
  const offenders = [];
  for (const page of PUBLIC_PAGES) {
    if (!fs.existsSync(P('public', page))) continue;
    const text = read('public', page).replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');
    for (const e of [].concat(CAP.SOURCES, CAP.TARGETS)) {
      if (CAP.isSellable(e.status)) continue;
      // Match the engine name on its own, not inside a longer word.
      const re = new RegExp('\\b' + e.label.split(' / ')[0].replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b', 'i');
      if (re.test(text)) offenders.push(page + ' offers ' + e.label + ' (' + e.status + ')');
    }
  }
  check('no public page offers a source or target that is not shipped',
    offenders.length === 0, [...new Set(offenders)].join(', '));
}

/* ══ 5. The three contradictions are actually resolved ═════════════════ */
{
  const help = read('public', 'help.html');
  const pricing = read('public', 'pricing.html');

  check('CONTRADICTION 1 — MySQL is no longer sold anywhere',
    !/mysql|mariadb/i.test(pricing.replace(/<!--[\s\S]*?-->/g, ''))
    && !/mysql|mariadb/i.test(read('public', 'index.html').replace(/<!--[\s\S]*?-->/g, '')));

  check('CONTRADICTION 2 — the help overview no longer says SQL Server only',
    !/connects to your source and target SQL Server databases/i.test(help)
    && /PostgreSQL/i.test(help));

  check('CONTRADICTION 3 — server-level migration is documented, not just sold',
    /id="server-migration"/.test(help) && /SQL Agent jobs/i.test(help)
    && /sys\.server_principals/.test(help));

  check('CONTRADICTION 4 — the role instructions no longer point at Netlify Identity',
    !/Netlify Identity/i.test(help) && !/app_metadata/.test(help)
    && /Users &amp; Roles|Users & Roles/.test(help));
  check('and they describe the ten roles that actually exist',
    /Migration Lead/.test(help) && /Automation Principal/.test(help));
}

/* ══ 6. The manifest is the ONLY place a capability is asserted ════════ */
{
  // A hard-coded engine list somewhere else is how the manifest becomes one
  // more thing to keep in step rather than the thing that keeps them in step.
  const offenders = [];
  const files = fs.readdirSync(P('public')).filter(f => /\.(js|html)$/.test(f));
  for (const f of files) {
    if (f === 'cygenix-capabilities.js' || f === 'help.html') continue;
    if (f === 'cygenix-restore.js') continue;   // its own documented matrix — see below
    read('public', f).split('\n').forEach((ln, i) => {
      const t = ln.trim();
      if (/^(\*|\/\/|<!--)/.test(t)) return;
      // An inline EngineEdition classification is the specific duplication
      // that would drift: two places deciding what "Azure SQL Database" means.
      if (/EngineEdition/.test(ln) && /===\s*5|===\s*8/.test(ln)) {
        offenders.push(f + ':' + (i + 1));
      }
    });
  }
  check('engine classification is not duplicated outside the manifest',
    offenders.length === 0, offenders.join(', '));

  // The Restore module predates the manifest and has its own matrix for a
  // different question (backup/restore, not server objects). It is allowed,
  // and pinned here so that if it ever starts answering the SAME question the
  // duplication is visible rather than silent.
  const restore = read('public', 'cygenix-restore.js');
  check('the Restore module\'s matrix is about restore, not server objects',
    /fromDisk|logChain|bacpac/.test(restore) && !/agent_jobs|linked_servers/.test(restore));
}

/* ══ 7. Gating behaves ═════════════════════════════════════════════════ */
{
  const ok = CAP.pairStatus('mssql', 'azuresqldb');
  check('a supported pair reports ga', ok.status === 'ga');

  const planned = CAP.pairStatus('mysql', 'mssql');
  check('a planned source is refused, naming itself',
    !CAP.isSellable(planned.status) && /MySQL/i.test(planned.reason), planned.reason);

  const unassessed = CAP.pairStatus('blob', 'postgres');
  check('an unassessed pair is refused, and says that is why',
    unassessed.status === 'unsupported' && /nobody has proved it works/.test(unassessed.reason),
    unassessed.reason);

  const unknown = CAP.pairStatus('mssql', 'oracle');
  check('an unknown target is refused rather than assumed',
    unknown.status === 'unsupported' && /Unknown target/.test(unknown.reason));
}
{
  // The matrix that makes server-object migration honest.
  const db = CAP.serverObjectPlan('azuresqldb');
  check('Azure SQL Database can hold no server-level objects at all',
    db.verdict === 'none' && db.movableCount === 0, JSON.stringify(db.verdict));
  check('and every one names the equivalent construct instead',
    db.rows.filter(r => r.verdict === 'requires_equivalent').every(r => r.equivalent),
    db.rows.filter(r => r.verdict === 'requires_equivalent' && !r.equivalent).map(r => r.id).join(', '));
  check('the Agent-job equivalent is named specifically, not vaguely',
    /Elastic Job/i.test(CAP.serverObjectSupport('agent_jobs', 'azuresqldb').equivalent || ''));

  const mssql = CAP.serverObjectPlan('mssql');
  check('SQL Server accepts all of them', mssql.verdict === 'all');
  const mi = CAP.serverObjectPlan('azuresqlmi');
  check('Managed Instance accepts most, with changes on some',
    mi.verdict === 'all' && mi.rows.some(r => r.verdict === 'supported_with_changes'),
    mi.verdict);
  check('a login moving to Azure SQL DB explains WHY, not just that it cannot',
    /no server-level principals/i.test(CAP.serverObjectSupport('logins', 'azuresqldb').reason || ''));
}
{
  // Engine detection, shared so two screens cannot disagree.
  check('EngineEdition 5 is Azure SQL Database', CAP.classifyEngine(5).id === 'azuresqldb');
  check('EngineEdition 8 is Managed Instance',   CAP.classifyEngine(8).id === 'azuresqlmi');
  check('EngineEdition 3 is SQL Server',         CAP.classifyEngine(3).id === 'mssql');
  check('an unrecognised edition is unsupported, not guessed',
    CAP.classifyEngine(99).status === 'unsupported' && /99/.test(CAP.classifyEngine(99).label));
  check('there is one probe SQL, and it reads EngineEdition',
    /SERVERPROPERTY\('EngineEdition'\)/.test(CAP.ENGINE_PROBE_SQL));
}
{
  // Tier gating and job admission.
  check('server objects are gated to Business and above',
    !CAP.tierAllows('pro', 'server_objects').allowed
    && CAP.tierAllows('business', 'server_objects').allowed);
  check('and the refusal names the plan it needs',
    /Business/.test(CAP.tierAllows('starter', 'server_objects').reason || ''));
  check('a planned feature is refused on every plan, including Enterprise',
    !CAP.tierAllows('enterprise', 'reconciliation_row_level').allowed);

  const over = CAP.admitJob('starter', { source: 'mssql', target: 'mssql', rows: 5000000, running: 0, submittedThisMonth: 0 });
  check('a job over the row limit is refused, naming the limit and the plan',
    !over.ok && over.code === 'rows_exceeded' && /Starter/.test(over.reason) && /1,000,000/.test(over.reason),
    over.reason);
  const busy = CAP.admitJob('starter', { source: 'mssql', target: 'mssql', rows: 10, running: 1, submittedThisMonth: 0 });
  check('a job over the concurrency limit is refused with what to do',
    !busy.ok && busy.code === 'concurrency_exceeded' && /Wait for one to finish/.test(busy.reason));
  const badPair = CAP.admitJob('business', { source: 'mysql', target: 'mssql', rows: 10, running: 0, submittedThisMonth: 0 });
  check('a job on an unshipped source is refused at admission',
    !badPair.ok && badPair.code === 'pair_unsupported');
  const fine = CAP.admitJob('enterprise', { source: 'mssql', target: 'mssql', rows: 999999999, running: 99, submittedThisMonth: 99999 });
  check('Enterprise has no numeric limits, and a valid job is admitted', fine.ok === true);
}

/* ══ 8. The UI actually reads it ═══════════════════════════════════════ */
{
  const sm = read('public', 'server-migration.js');
  check('Server Migration assesses the target before discovery',
    /function assessTarget/.test(sm) && /serverObjectPlan/.test(sm));
  check('it reads the engine edition through the shared probe',
    /CAP\.ENGINE_PROBE_SQL/.test(sm) && /classifyEngine/.test(sm));
  check('a target that can hold nothing blocks the run rather than warning',
    /plan\.verdict === 'none'/.test(sm) && /proceedBtn\.disabled = true/.test(sm));
  check('and a missing manifest degrades to the old behaviour rather than throwing',
    /if \(!CAP\) return;/.test(sm));

  const pages = fs.readdirSync(P('public')).filter(f => f.endsWith('.html'));
  const consolePages = pages.filter(f => /cygenix-sidebar\.js/.test(read('public', f)));
  const missing = consolePages.filter(f => !/cygenix-capabilities\.js/.test(read('public', f)));
  check('every console page loads the manifest', missing.length === 0, missing.join(', '));
}

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
