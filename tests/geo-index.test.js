// Tests for the Globe view's discovery and aggregation engine.
//
// This is the risky half of the globe: the data model, not the rendering. A
// map drawn from a wrong index is confidently wrong, so what is pinned here
// is which columns count as geography, which tables are reached through the
// estate's own Country table, what SQL that produces, and — the part that
// decides whether the totals can be believed — that nothing unresolved is
// ever quietly dropped.
const G = require('../public/cygenix-geo-index.js');
const fs = require('fs');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Globe — geo discovery and aggregation\n');

// ── 1. Column roles ─────────────────────────────────────────────────────────
const ROLES = [
  ['Country', 'country'], ['CountryCode', 'country'], ['CountryId', 'country'],
  ['ISO2', 'country'], ['ISO3', 'country'], ['Nationality', 'country'], ['Jurisdiction', 'country'],
  ['AddressCountry', 'country'], ['BillToCountryCode', 'country'],
  ['State', 'state'], ['Province', 'state'], ['Region', 'state'], ['StateProvince', 'state'],
  ['County', 'county'], ['City', 'city'], ['Town', 'city'], ['Locality', 'city'],
  ['PostCode', 'postal'], ['PostalCode', 'postal'], ['Zip', 'postal'], ['ZipCode', 'postal'],
  ['Latitude', 'lat'], ['Longitude', 'lng'], ['Lat', 'lat'], ['Lng', 'lng'],
  ['MatterId', null], ['Amount', null], ['Description', null], ['StatusCode', null],
];
const wrongRole = ROLES.filter(([n, r]) => G.geoColumnRole(n) !== r)
  .map(([n, r]) => n + ' → ' + G.geoColumnRole(n) + ' (want ' + r + ')');
check('every geo column name pattern takes its role (' + ROLES.length + ')',
  wrongRole.length === 0, wrongRole.join(', '));
check('roles are case- and punctuation-insensitive',
  G.geoColumnRole('POST_CODE') === 'postal' && G.geoColumnRole('country_code') === 'country');
// Words, not substrings: "RealEstate" contains "state" and is not a place.
check('a word inside a longer word is not a match',
  G.geoColumnRole('RealEstate') === null && G.geoColumnRole('Latest') === null
  && G.geoColumnRole('Statement') === null, [G.geoColumnRole('RealEstate'),
  G.geoColumnRole('Latest'), G.geoColumnRole('Statement')].join(','));
check('and an ordinary column is left alone',
  G.geoColumnRole('AccountId') === null && G.geoColumnRole('Amount') === null);

// ── 2. Shadow tables ────────────────────────────────────────────────────────
['Client_draft', 'Matter_template', 'Address_audit', 'Client_bak', 'Matter_old']
  .forEach(n => check('a shadow table is recognised: ' + n, G.isShadowTable(n)));
check('a real table is not', !G.isShadowTable('Address') && !G.isShadowTable('ClientContact'));
check('the estate\'s ISO reference table is recognised',
  G.isCountryRef('Country') && G.isCountryRef('CountryList') && !G.isCountryRef('CmPatentCountry'));

// ── 3. The index ────────────────────────────────────────────────────────────
const t = (name, rowCount, columns) => ({ name, key: 'dbo.' + name, rowCount, kind: 'table', columns });
const TABLES = [
  t('Address', 31194, ['AddressId', 'Line1', 'City', 'County', 'PostCode', 'CountryId']),
  t('Client', 30000, ['ClientId', 'Name', 'CountryCode', 'State']),
  t('Matter', 62000, ['MatterId', 'ClientId', 'OpenDate']),                  // no geography
  t('CmPatentCountry', 4000, ['PatentId', 'Country']),
  t('Country', 247, ['CountryId', 'Code', 'Name']),
  t('OfficeLocation', 40, ['OfficeId', 'City', 'Country', 'Latitude', 'Longitude']),
  t('Client_draft', 900, ['ClientId', 'CountryCode']),                       // shadow
  t('Invoice', 310000, ['InvoiceId', 'ClientId', 'Amount']),                 // FK to Country only
];
const EDGES = [
  { from: 'dbo.Address', to: 'dbo.Country', fromColumn: 'CountryId', toColumn: 'CountryId' },
  { from: 'dbo.Invoice', to: 'dbo.Country', fromColumn: 'BillCountryId', toColumn: 'CountryId' },
  { from: 'dbo.Matter',  to: 'dbo.Client',  fromColumn: 'ClientId',  toColumn: 'ClientId' },
];
const idx = G.geoIndex(TABLES, EDGES, { classify: n => (/client|matter/i.test(n) ? 2 : 5),
                                        familyKey: n => n });
const names = idx.map(e => e.table);

check('a table with no geography at all is left out', !names.includes('Matter'));
check('shadow tables are out by default', !names.includes('Client_draft'));
check('and can be put back', G.geoIndex(TABLES, EDGES, { includeShadow: true })
  .some(e => e.table === 'Client_draft'));
check('a table with a country column is in', names.includes('Client'));
check('so is one with only city and postcode', names.includes('Address'));
check('a table with an FK to Country but no geo column of its own is in',
  names.includes('Invoice'), names.join(', '));
check('and says how it was reached',
  idx.find(e => e.table === 'Invoice').via === 'fk:Country',
  idx.find(e => e.table === 'Invoice').via);
check('a direct column says so', idx.find(e => e.table === 'Client').via === 'direct');
check('an FK that does not point at Country does not make a table geographic',
  !names.includes('Matter'));

const addr = idx.find(e => e.table === 'Address');
check('the columns found are named, so the detection can be checked',
  addr.columns.postcode === 'PostCode' && addr.columns.state === 'County'
  && addr.columns.county === 'City', JSON.stringify(addr.columns));
check('with no state column, county IS the top division and city sits under it',
  addr.level === 2);
// The US sense, where a county is inside a state.
const us = G.geoIndex([t('UsSite', 10, ['State', 'County', 'City', 'Country'])], [])[0];
check('with a state column, county drops to admin-2 and city is dropped',
  us.columns.state === 'State' && us.columns.county === 'County' && us.level === 2,
  JSON.stringify(us.columns));
check('grouping by an id would return row ids, so the FK join is used instead',
  addr.columns.country === null && addr.via === 'fk:Country', addr.via);
check('a country-only table sits at world level',
  idx.find(e => e.table === 'CmPatentCountry').level === 0);
check('every entry carries the business area, so the globe groups like the rest of the app',
  idx.every(e => typeof e.domain === 'number') && idx.find(e => e.table === 'Client').domain === 2);
check('the index leads with the tables that will decide the shape of the map',
  idx[0].rows >= idx[idx.length - 1].rows && idx[0].table === 'Invoice', idx[0].table);
check('entries start enabled and overridable', idx.every(e => e.enabled === true));

// ── 4. The SQL ──────────────────────────────────────────────────────────────
const sqlClient = G.geoSql(idx.find(e => e.table === 'Client'));
check('a direct table groups on its own columns',
  /SELECT TOP 5000 t\.\[CountryCode\] AS \[country\], t\.\[State\] AS \[state\], COUNT\(\*\) AS \[n\]/.test(sqlClient),
  sqlClient);
check('from the schema-qualified table', /FROM \[dbo\]\.\[Client\] t/.test(sqlClient), sqlClient);
check('and groups by exactly what it selects',
  /GROUP BY t\.\[CountryCode\], t\.\[State\]/.test(sqlClient), sqlClient);
check('heaviest first, so a truncated result is still the useful part',
  /ORDER BY COUNT\(\*\) DESC/.test(sqlClient));

const sqlInv = G.geoSql(idx.find(e => e.table === 'Invoice'));
check('an FK table joins the reference table for the code — an id is not a place',
  /LEFT JOIN \[dbo\]\.\[Country\] c ON t\.\[BillCountryId\] = c\.\[CountryId\]/.test(sqlInv), sqlInv);
check('and selects the code from there', /c\.\[Code\] AS \[country\]/.test(sqlInv), sqlInv);

// Row count over-weights an address book against a client list.
const keyed = { table: 'Client', key: 'dbo.Client', entityKey: 'ClientId',
                columns: { country: 'CountryCode' } };
check('the distinct metric counts entities, not rows',
  /COUNT\(DISTINCT t\.\[ClientId\]\) AS \[n\]/.test(G.geoSql(keyed, { metric: 'distinct' })));
check('and falls back to rows where the table has no single key to count',
  /COUNT\(\*\) AS \[n\]/.test(G.geoSql({ table: 'X', key: 'dbo.X', columns: { country: 'C' } },
    { metric: 'distinct' })));
check('rows is what it counts by default', /COUNT\(\*\) AS \[n\]/.test(G.geoSql(keyed)));
check('a single-column key becomes the entity key',
  G.geoIndex([{ name: 'C', key: 'dbo.C', rowCount: 1, columns: ['Country'], primaryKeys: ['CId'] }],
    [])[0].entityKey === 'CId');
check('a composite key does not — there is no one thing to count',
  G.geoIndex([{ name: 'C', key: 'dbo.C', rowCount: 1, columns: ['Country'], primaryKeys: ['A', 'B'] }],
    [])[0].entityKey === null);

check('identifiers are quoted, and a bracket inside one is doubled',
  /\[Odd\]\]Name\]/.test(G.geoSql({ table: 'X', key: 'dbo.X', columns: { country: 'Odd]Name' } })));
check('a table with nothing groupable produces no query',
  G.geoSql({ table: 'X', key: 'dbo.X', columns: { lat: 'Lat', lng: 'Lng' } }) === null);
check('the plan skips entries the user switched off',
  G.geoPlan(idx.map(e => Object.assign({}, e, { enabled: e.table !== 'Client' }))).length === idx.length - 1);

// ── 5. ISO normalisation ────────────────────────────────────────────────────
check('the bundled list covers the ISO countries', G.isoCount() >= 240, String(G.isoCount()));
const ISO_CASES = [
  ['United Kingdom', 'GBR'], ['UK', 'GBR'], ['U.K.', 'GBR'], ['Great Britain', 'GBR'],
  ['England', 'GBR'], ['GB', 'GBR'], ['GBR', 'GBR'],
  ['USA', 'USA'], ['United States', 'USA'], ['U.S.A.', 'USA'], ['US', 'USA'], ['America', 'USA'],
  ['Germany', 'DEU'], ['Deutschland', 'DEU'], ['de', 'DEU'], ['deu', 'DEU'],
  ['France', 'FRA'], ['Holland', 'NLD'], ['Netherlands', 'NLD'], ['Eire', 'IRL'],
  ['Ireland', 'IRL'], ['Ivory Coast', 'CIV'], ['Czech Republic', 'CZE'],
];
const wrongIso = ISO_CASES.filter(([v, i]) => G.geoNormaliseCountry(v) !== i)
  .map(([v, i]) => v + ' → ' + G.geoNormaliseCountry(v) + ' (want ' + i + ')');
check('free text resolves to alpha-3 (' + ISO_CASES.length + ' spellings)', wrongIso.length === 0,
  wrongIso.join(', '));
const BLANKS = ['', '  ', 'N/A', 'Unknown', 'TBC', 'ZZ', 'Other', 'Various', null, undefined];
check('placeholders never resolve to a country',
  BLANKS.every(v => G.geoNormaliseCountry(v) === null),
  BLANKS.filter(v => G.geoNormaliseCountry(v) !== null).join(','));
check('a real value that is not a country stays unresolved',
  G.geoNormaliseCountry('Neverland') === null && G.geoNormaliseCountry('Head Office') === null);
check('alpha-3 comes back with a display name', G.geoCountryName('GBR') === 'United Kingdom');

// The estate's own Country table is the authority for this project.
const seed = G.geoSeedFromCountryTable([
  { CountryId: 1, Code: 'GB', Name: 'United Kingdom' },
  { CountryId: 2, Code: 'ENG', Name: 'England' },      // a house code, not ISO
  { CountryId: 3, Code: 'US', Name: 'United States' },
]);
check('a code the client actually uses is seeded from their Country table',
  G.geoNormaliseCountry('ENG', seed) === 'GBR', JSON.stringify(seed));
check('and the bundled list still answers everything else',
  G.geoNormaliseCountry('France', seed) === 'FRA');

// ── 6. Postcodes, as far as admin-1 and no further ──────────────────────────
check('a Scottish postcode area resolves to Scotland', G.geoPostcodeAdmin1('EH1 2AB', 'GBR') === 'Scotland');
check('a Welsh one to Wales', G.geoPostcodeAdmin1('CF10 1AA', 'GBR') === 'Wales');
check('a Northern Irish one to Northern Ireland', G.geoPostcodeAdmin1('BT1 5GS', 'GBR') === 'Northern Ireland');
check('the residual is England', G.geoPostcodeAdmin1('SW1A 1AA', 'GBR') === 'England');
check('a US ZIP resolves to its state',
  G.geoPostcodeAdmin1('10001', 'USA') === 'NY' && G.geoPostcodeAdmin1('90210', 'USA') === 'CA');
check('no other country is attempted — a postcode is not a coordinate',
  G.geoPostcodeAdmin1('75008', 'FRA') === null && G.geoPostcodeAdmin1('2000', 'AUS') === null);
check('rubbish resolves to nothing', G.geoPostcodeAdmin1('???', 'GBR') === null);

// ── 7. Folding, and the unresolved bucket ───────────────────────────────────
const e1 = { table: 'Client', domain: 2 }, e2 = { table: 'Address', domain: 5 };
const ledger = G.geoFold([
  { entry: e1, rows: [
    { country: 'United Kingdom', state: 'England', n: 100 },
    { country: 'UK', state: 'Scotland', n: 40 },
    { country: 'France', n: 25 },
    { country: 'Neverland', n: 7 },
    { country: '', n: 3 },
  ] },
  { entry: e2, rows: [
    { country: 'GB', state: 'England', county: 'Kent', n: 10 },
    { country: 'GB', state: 'England', county: 'Surrey', n: 5 },
    { country: 'Neverland', n: 1 },
  ] },
]);
check('every row is counted somewhere', ledger.total === 191, String(ledger.total));
check('resolved and unresolved add up to the total',
  ledger.resolved + ledger.unresolved.n === ledger.total,
  ledger.resolved + ' + ' + ledger.unresolved.n);
check('spellings of the same country land in one bucket',
  ledger.countries.GBR.n === 155, String(ledger.countries.GBR && ledger.countries.GBR.n));
check('admin-1 rolls up inside its country',
  ledger.countries.GBR.admin1.England.n === 115, JSON.stringify(Object.keys(ledger.countries.GBR.admin1)));
check('and admin-2 inside that', ledger.countries.GBR.admin1.England.admin2.Kent.n === 10);
check('a country with no sub-region data has none — the drill has to say so',
  Object.keys(ledger.countries.FRA.admin1).length === 0);
check('each bucket knows which business areas contributed',
  ledger.countries.GBR.domains[2] === 140 && ledger.countries.GBR.domains[5] === 15,
  JSON.stringify(ledger.countries.GBR.domains));
check('and which tables', ledger.countries.GBR.tables.Client === 140);
check('unresolved values are never dropped', ledger.unresolved.n === 11, String(ledger.unresolved.n));
check('and come back with examples and counts, as a data-quality finding',
  ledger.unresolved.examples[0].value === 'Neverland' && ledger.unresolved.examples[0].n === 8,
  JSON.stringify(ledger.unresolved.examples));
check('a blank is shown as blank rather than hidden',
  ledger.unresolved.examples.some(x => x.value === '(blank)'));
check('countries come back heaviest first',
  ledger.countryList[0].iso3 === 'GBR' && ledger.countryList[0].name === 'United Kingdom');

// ── 8. Minimum-cell suppression, off unless asked for ───────────────────────
const before = JSON.parse(JSON.stringify(ledger.countries.GBR.admin1.England.admin2));
check('nothing is suppressed by default', Object.keys(before).length === 2);
const suppressed = G.geoSuppress(G.geoFold([{ entry: e2, rows: [
  { country: 'GB', state: 'England', county: 'Kent', n: 10 },
  { country: 'GB', state: 'England', county: 'Rutland', n: 2 },
] }]), 5);
check('with a floor, a region under it rolls up to its parent',
  Object.keys(suppressed.countries.GBR.admin1.England.admin2).join(',') === 'Kent');
check('and the parent total still carries those records',
  suppressed.countries.GBR.admin1.England.n === 12);
check('the ledger records that it was suppressed, so the totals can be explained',
  suppressed.suppressed === 5);

// ── 9. Running the plan ─────────────────────────────────────────────────────
(async function run(){
  let live = 0, peak = 0;
  const seen = [];
  const exec = (sql, job) => {
    live++; peak = Math.max(peak, live);
    seen.push(job.table);
    return new Promise(r => setTimeout(() => {
      live--;
      r(job.table === 'Client' ? [{ country: 'UK', state: 'England', n: 50 }] : [{ country: 'FR', n: 5 }]);
    }, 5));
  };
  const progress = [];
  const out = await G.geoRun(idx, { exec, concurrency: 2, onProgress: (d, n) => progress.push(d + '/' + n) });
  check('every enabled table is queried', out.queried === idx.length, out.queried + ' of ' + idx.length);
  check('never more at once than the cap allows', peak <= 2, 'peak ' + peak);
  check('progress is reported per table, because some of these are large',
    progress.length === idx.length && progress[progress.length - 1] === idx.length + '/' + idx.length,
    progress.join(' '));
  check('the results fold into one ledger', out.total > 0 && out.countryList.length >= 2);

  // One table failing must not sink the map.
  const flaky = (sql, job) => job.table === 'Address'
    ? Promise.reject(new Error('Invalid column name'))
    : Promise.resolve([{ country: 'DE', n: 3 }]);
  const partial = await G.geoRun(idx, { exec: flaky, concurrency: 3 });
  check('a table that fails is recorded, not swallowed',
    partial.failures.length === 1 && partial.failures[0].table === 'Address',
    JSON.stringify(partial.failures));
  check('and the rest of the estate still maps',
    partial.countries.DEU && partial.countries.DEU.n > 0);
  check('the ledger says how many tables were planned against how many answered',
    partial.planned === idx.length && partial.queried === idx.length - 1);

  // ── 10. Nothing here reaches for a row ────────────────────────────────────
  const SRC = fs.readFileSync(__dirname + '/../public/cygenix-geo-index.js', 'utf8');
  check('every query is an aggregate — no query selects rows',
    !/SELECT\s+(?!TOP \d+ )[^']*FROM/i.test(SRC.replace(/^.*ISO_RAW[\s\S]*?;$/m, '')));
  check('every generated query counts and groups',
    G.geoPlan(idx).every(j => /COUNT\(\*\)/.test(j.sql) && /GROUP BY/.test(j.sql)));
  check('the module fetches nothing and stores nothing',
    !/\bfetch\s*\(/.test(SRC) && !/localStorage|sessionStorage/.test(SRC));
  check('and touches no DOM, so it can be tested where it is cheap',
    !/\bdocument\./.test(SRC));

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
