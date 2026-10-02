// Tests public/cygenix-object-names.js — matching a SAVED table or column name
// against the live list a connection returned, the way SQL Server does — and
// the Object Mapping wiring that uses it.
//
// The bug this pins (Oct-2026): a map saved as "dbo.STG_client" would not open
// against a database holding "dbo.STG_Client". Object Mapping compared names
// with ===, found nothing, and told the user to reconnect a connection that
// was working. Every case below is one of: the case of a letter differs; the
// schema is missing; two tables fit and the page must ask; nothing fits and
// the page must say what is near; the connection really is down.
const fs = require('fs');
const path = require('path');
const N = require('../public/cygenix-object-names.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra !== undefined ? '  → ' + JSON.stringify(extra) : '')); }
};
const section = (s) => console.log('\n' + s);

// The shape connectSrc() builds.
const T = (schema, name) => ({ value: schema + '.' + name, label: schema + '.' + name, schema, name, fullName: schema + '.' + name });
const LIVE = [T('dbo', 'STG_Client'), T('dbo', 'Matter'), T('dbo', 'Timecard'), T('stg', 'Fee'), T('dbo', 'Fee'), T('dbo', 'ClientAddress')];

section('1. The reported case, and the ways a name can differ only in spelling');
{
  const r = N.findObjectByName(LIVE, 'dbo.STG_client');
  check('"dbo.STG_client" finds dbo.STG_Client — the bug as reported', r.status === 'unique' && r.match.value === 'dbo.STG_Client', r);
  const r2 = N.findObjectByName(LIVE, 'STG_client');
  check('"STG_client" with no schema finds dbo.STG_Client', r2.status === 'unique' && r2.match.value === 'dbo.STG_Client', r2);
  const r3 = N.findObjectByName(LIVE, 'DBO.stg_client');
  check('a differently-cased schema is ignored too', r3.status === 'unique' && r3.match.value === 'dbo.STG_Client');
  const r4 = N.findObjectByName(LIVE, '[dbo].[STG_Client]');
  check('bracket quoting is not part of the name', r4.status === 'unique' && r4.match.value === 'dbo.STG_Client');
  const r5 = N.findObjectByName(LIVE, 'dbo.Matter');
  check('an exact match is reported as exact', r5.status === 'exact' && r5.match.value === 'dbo.Matter');
}

section('2. Exact text wins over a looser fit');
{
  // A case-SENSITIVE database can really hold both. The one that was saved
  // must open — not whichever the loose rule happens to meet first.
  const cs = [T('dbo', 'Client'), T('dbo', 'client')];
  const r = N.findObjectByName(cs, 'dbo.client');
  check('both dbo.Client and dbo.client exist: the saved "dbo.client" opens dbo.client', r.status === 'exact' && r.match.value === 'dbo.client');
  const r2 = N.findObjectByName(cs, 'dbo.CLIENT');
  check('…and "dbo.CLIENT", which matches neither exactly, is ambiguous, not guessed', r2.status === 'ambiguous' && r2.candidates.length === 2);
}

section('3. More than one fit: do not guess');
{
  const r = N.findObjectByName(LIVE, 'fee');
  check('"fee" with no schema fits stg.Fee and dbo.Fee: ambiguous, both listed',
    r.status === 'ambiguous' && r.match === null && r.candidates.map(t => t.value).sort().join() === 'dbo.Fee,stg.Fee', r);
  const r2 = N.findObjectByName(LIVE, 'stg.fee');
  check('"stg.fee" names its schema, so only stg.Fee fits', r2.status === 'unique' && r2.match.value === 'stg.Fee');
}

section('4. Nothing fits: say what is near');
{
  const r = N.findObjectByName(LIVE, 'dbo.Clinet');
  check('a typo is missing, not matched', r.status === 'missing' && r.match === null);
  const r2 = N.findObjectByName(LIVE, 'stg.STG_Client');
  check('a saved schema is NOT ignored when it names a different schema — stg.STG_Client is missing',
    r2.status === 'missing', r2);
  check('…and the same name in the other schema is the first close match',
    r2.close.length && r2.close[0].value === 'dbo.STG_Client', r2.close.map(t => t.value));
  const r3 = N.findObjectByName(LIVE, 'dbo.Matters');
  check('a name a letter away is offered', r3.status === 'missing' && r3.close.some(t => t.value === 'dbo.Matter'), r3.close.map(t => t.value));
  const r4 = N.findObjectByName(LIVE, 'dbo.STGClient');
  check('a name differing only in punctuation is offered', r4.close.some(t => t.value === 'dbo.STG_Client'));
  const r5 = N.findObjectByName(LIVE, 'dbo.Invoice');
  check('a name with nothing near has no close matches', r5.status === 'missing' && r5.close.length === 0, r5.close.map(t => t.value));
  const big = []; for (let i = 0; i < 40; i++) big.push(T('dbo', 'Fee' + i));
  check('close matches are capped at five', N.findObjectByName(big, 'dbo.Fe').close.length <= 5);
  check('an empty saved name is missing, not a match for anything', N.findObjectByName(LIVE, '').status === 'missing');
  check('an empty live list is missing', N.findObjectByName([], 'dbo.X').status === 'missing');
}

section('5. Column names');
{
  const cols = [{ name: 'ClientID' }, { name: 'ClientName' }, { name: 'OpenDate' }];
  check('a column differing only in case takes the live spelling', N.resolveColumnName(cols, 'clientname') === 'ClientName');
  check('string column lists work too', N.resolveColumnName(['Code', 'Desc'], 'code') === 'Code');
  check('an exact column is unchanged', N.resolveColumnName(cols, 'OpenDate') === 'OpenDate');
  check('an unknown column is left exactly as saved', N.resolveColumnName(cols, 'Nope') === 'Nope');
  check('two columns differing only in case: left as saved, not guessed', N.resolveColumnName(['a', 'A'], 'x') === 'x' && N.resolveColumnName(['ab', 'AB'], 'Ab') === 'Ab');

  const saved = [
    { srcCol: 'clientid', tgtCol: 'CLIENT_ID', transform: 'NONE' },
    { srcCol: 'j1.Code', tgtCol: 'code' },
    { srcCol: "ISNULL(x,'')", tgtCol: 'Name' },
    { srcCol: '', tgtCol: 'missing_col', literalValue: '1' },
  ];
  const before = JSON.stringify(saved);
  const r = N.canonicaliseMapping(saved, [{ name: 'ClientID' }, { name: 'Code' }], [{ name: 'Client_ID' }, { name: 'Code' }, { name: 'Name' }]);
  check('srcCol and tgtCol take the live spelling', r.mapping[0].srcCol === 'ClientID' && r.mapping[0].tgtCol === 'Client_ID', r.mapping[0]);
  check('a joined column ("j1.Code") is not rewritten as a base column', r.mapping[1].srcCol === 'j1.Code' && r.mapping[1].tgtCol === 'Code');
  check('an expression is left alone', r.mapping[2].srcCol === "ISNULL(x,'')");
  check('a target column that is not there is left as saved', r.mapping[3].tgtCol === 'missing_col' && r.mapping[3].literalValue === '1');
  check('the count of rewritten names is reported', r.changed === 3, r.changed);
  check('the saved mapping itself is not mutated', JSON.stringify(saved) === before);
  check('other fields ride along', r.mapping[0].transform === 'NONE');
}

section('6. Object Mapping uses the helper, and the messages say the right thing');
{
  const app = fs.readFileSync(path.join(__dirname, '..', 'public', 'object-mapping-app.js'), 'utf8');
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'object_mapping.html'), 'utf8');
  check('object_mapping.html loads the helper before object-mapping-app.js',
    html.indexOf('/cygenix-object-names.js') > 0 && html.indexOf('/cygenix-object-names.js') < html.indexOf('/object-mapping-app.js'));
  check('the old exact-text message is gone', !/not found — reconnect (source|target) DB/.test(app));
  check('the old exact-text lookups of a saved map are gone',
    !/srcAllTables\.find\(t=>t\.value===srcFull\|\|t\.label===srcFull\)/.test(app)
    && !/tgtAllTables\.find\(t=>t\.value===tgtFull\|\|t\.label===tgtFull\)/.test(app)
    && !/tgtAllTables\.find\(t=>t\.value===jtt\.name/.test(app)
    && !/t\.value === d\.srcFullName \|\| t\.label === d\.srcFullName/.test(app)
    && !/x\.value === stt\.name \|\| x\.label === stt\.name/.test(app));
  const editMode = app.slice(app.indexOf('async function checkEditMode()'), app.indexOf('async function restoreJobMapping('));
  check('opening a saved map resolves source and target through omResolveTable',
    /await omResolveTable\('src', srcFull\)/.test(editMode) && /await omResolveTable\('tgt', tgtFull\)/.test(editMode));
  check('…and checks the connection before waiting for tables', /omConnProblem\('src', srcFull\)/.test(editMode));
  const otm = fs.readFileSync(path.join(__dirname, '..', 'public', 'one-to-many.html'), 'utf8');
  check('one-to-many.html loads the helper and resolves saved names through it',
    /\/cygenix-object-names\.js/.test(otm) && /otmResolveSaved\(srcAllTables, pendingEditCfg\.srcTable/.test(otm)
    && /otmResolveSaved\(tgtAllTables, tt\.fullName/.test(otm)
    && !/t\.value === pendingEditCfg\.srcTable \|\| t\.label === pendingEditCfg\.srcTable/.test(otm)
    && !/t\.value === tt\.fullName \|\| t\.label === tt\.fullName/.test(otm));

  // Run the message builders and the resolver against a stubbed page.
  const start = app.indexOf('// ── Finding a saved table name in the live list');
  const end = app.indexOf('// ── Edit mode — load saved job');
  const vm = require('vm');
  const statuses = [];
  const sb = {
    CygenixObjectNames: N, srcAllTables: LIVE, tgtAllTables: [T('dbo', 'Client')],
    srcSchema: { database: 'CONVERSION_DM' }, tgtSchema: { database: 'TE_3E' }, srcConn: '', tgtConn: '',
    _connState: { src: 'ok', tgt: 'ok' }, parseDbName: () => '',
    showStatus: (m, t) => statuses.push([m, t]), $: () => null, document: {}, Promise, Object, Array, String, Number,
  };
  vm.createContext(sb);
  vm.runInContext(app.slice(start, end) + '\nthis.omResolveTable = omResolveTable; this.omConnProblem = omConnProblem; this.omMissingMessage = omMissingMessage;', sb);

  (async () => {
    const hit = await sb.omResolveTable('src', 'dbo.STG_client');
    check('the page opens dbo.STG_Client for a map saved as dbo.STG_client, with no message',
      hit && hit.value === 'dbo.STG_Client' && statuses.length === 0, statuses);

    statuses.length = 0;
    const miss = await sb.omResolveTable('src', 'dbo.Clinet');
    const m = (statuses[0] || [])[0] || '';
    check('a table that is not there: "isn\'t in <database>. Pick it from the Source table list."',
      miss === null && m.indexOf('Table "dbo.Clinet" isn\'t in CONVERSION_DM. Pick it from the Source table list.') === 0, m);
    check('…with no "reconnect" in it', !/reconnect/i.test(m));
    statuses.length = 0;
    await sb.omResolveTable('src', 'stg.STG_Client');
    check('…and close matches named when there are any', /Close matches: dbo\.STG_Client/.test((statuses[0] || [])[0] || ''), statuses);

    statuses.length = 0;
    sb._connState.src = 'err';
    await sb.omResolveTable('src', 'dbo.Anything');
    check('a failed connection still says "Reconnect source DB"', /Reconnect source DB/.test((statuses[0] || [])[0] || ''), statuses);
    check('omConnProblem reports a failed connection', /connection failed/.test(sb.omConnProblem('src', 'x') || ''));
    sb._connState.src = 'ok'; sb.srcAllTables = [];
    vm.runInContext('srcAllTables = []', sb);
    check('…and a connection that listed no tables', /returned no tables\. Reconnect source DB/.test(sb.omConnProblem('src', 'x') || ''));
    sb._connState.src = 'connecting';
    check('…but says nothing while still connecting', sb.omConnProblem('src', 'x') === null);

    console.log('\n' + pass + ' passed, ' + fail + ' failed');
    process.exit(fail ? 1 : 0);
  })().catch(e => { console.log('FAIL threw: ' + e.stack); process.exit(1); });
}
