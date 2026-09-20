// tests/conn-builder.test.js — the connection form and the string agree.
//
// Connections were paste-only. That is fine when somebody has a string in
// front of them and wrong the rest of the time: most people are reading
// credentials off a page and had to assemble the syntax themselves, per
// engine, with no feedback until the connect failed.
//
// THE PROPERTY THAT MATTERS
// A builder that emits a string this product cannot read would be worse than
// no builder — it would turn a syntax mistake the user could see into one they
// could not. So the round-trip below is not a unit test of my own regex
// against my own regex: it composes with the browser module and parses with
// the ACTUAL server parser from netlify/functions/db-connect.js. If the two
// ever disagree, this fails.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const P = (...p) => path.join(__dirname, '..', ...p);
const read = (...p) => fs.readFileSync(P(...p), 'utf8');

const B = require('../public/cygenix-conn-builder.js');

console.log('Connection builder — the form and the string are the same thing\n');

/* The server's own postgres parser, lifted out of the Netlify function so the
   round-trip runs against the real thing rather than a copy of it. The module
   is a Netlify handler and pulls in drivers on require, so the two functions
   are extracted by source rather than imported. */
const dbConnect = read('netlify', 'functions', 'db-connect.js');
function lift(name) {
  const at = dbConnect.indexOf('function ' + name + '(');
  if (at === -1) throw new Error('could not find ' + name + ' in db-connect.js');
  // Step over the PARAMETER list before looking for the body's opening brace.
  // buildPgConfig destructures its argument — function f({ a, b }) — so the
  // first '{' after the name belongs to the parameters, and a walker that
  // started there would return the signature and nothing else.
  let i = dbConnect.indexOf('(', at), paren = 0;
  for (; i < dbConnect.length; i++) {
    if (dbConnect[i] === '(') paren++;
    else if (dbConnect[i] === ')') { paren--; if (paren === 0) { i++; break; } }
  }
  let depth = 0;
  i = dbConnect.indexOf('{', i);
  for (; i < dbConnect.length; i++) {
    if (dbConnect[i] === '{') depth++;
    else if (dbConnect[i] === '}') { depth--; if (depth === 0) { i++; break; } }
  }
  return dbConnect.slice(at, i);
}
const serverParse = new Function(
  lift('parsePostgresConnectionString') + '\n' + lift('buildPgConfig') +
  '\nreturn parsePostgresConnectionString;')();
const serverDetect = new Function(lift('detectDialect') + '\nreturn detectDialect;')();

/* ── 1. Postgres, composed here and parsed there ─────────────────────────── */

// Shaped after a real public read-only database (EBI's RNAcentral mirror),
// with a placeholder password — a credential does not belong in a repository
// even when it is published elsewhere.
const PG = { engine: 'postgres', host: 'hh-pgsql-public.ebi.ac.uk', port: '5432',
             database: 'pfmegrnargs', user: 'reader', password: 'placeholder-pw',
             schema: 'rnacen' };
const pgString = B.compose(PG);

check('the composed postgres string is a URL the server recognises as postgres',
  serverDetect(pgString) === 'postgres', pgString);

const pgCfg = serverParse(pgString);
check('host survives the round trip', pgCfg.host === PG.host, pgCfg.host);
check('port survives', pgCfg.port === 5432, pgCfg.port);
check('database survives', pgCfg.database === PG.database, pgCfg.database);
check('user survives', pgCfg.user === PG.user, pgCfg.user);
check('password survives', pgCfg.password === PG.password, pgCfg.password ? '(set)' : '(lost)');
check('and the schema reaches the driver as a search_path, not decoration',
  pgCfg.options === '-c search_path=rnacen', pgCfg.options);

check('a postgres connection with no schema sets no search_path',
  serverParse(B.compose({ ...PG, schema: '' })).options === undefined);
check('sslmode=require is carried through to the driver',
  serverParse(B.compose({ ...PG, sslmode: 'require' })).ssl
  && serverParse(B.compose({ ...PG, sslmode: 'require' })).ssl.rejectUnauthorized === false);
check('sslmode=disable is too',
  serverParse(B.compose({ ...PG, sslmode: 'disable' })).ssl === false);

/* Awkward but legal values. A password with an @ or a / in it breaks a URL
   that does not encode — and reads to the user as a wrong password. */
for (const [what, pw] of [['an @', 'p@ss'], ['a slash', 'a/b'], ['a colon', 'a:b'],
                          ['a hash', 'a#b'], ['a question mark', 'a?b'],
                          ['spaces and unicode', 'pa ss wörd']]) {
  const cs = B.compose({ ...PG, password: pw });
  check('a postgres password containing ' + what + ' survives the round trip',
    serverParse(cs).password === pw, serverParse(cs).password);
}
check('a database name with a space survives',
  serverParse(B.compose({ ...PG, database: 'my db' })).database === 'my db');

/* ── 2. SQL Server ───────────────────────────────────────────────────────── */

const MS = { engine: 'mssql', host: 'sql01.example.com', port: '1433',
             database: 'Finance', user: 'sa', password: 'placeholder-pw' };
const msString = B.compose(MS);
check('the composed SQL Server string is not mistaken for postgres',
  serverDetect(msString) === 'mssql', msString);
check('it reads as a keyword string in the order a person expects',
  /^Server=sql01\.example\.com;Database=Finance;User Id=sa;Password=/.test(msString), msString);
check('the default port is left off, because naming it changes nothing',
  msString.indexOf(',1433') === -1, msString);
check('a non-default port is attached to the server with a comma, as SQL Server wants',
  B.compose({ ...MS, port: '14330' }).indexOf('Server=sql01.example.com,14330') === 0);
check('Encrypt is on unless it is turned off',
  /Encrypt=true/.test(msString) && !/Encrypt=true/.test(B.compose({ ...MS, encrypt: false })));
check('TrustServerCertificate appears only when asked for',
  !/TrustServerCertificate/.test(msString)
  && /TrustServerCertificate=true/.test(B.compose({ ...MS, trustCert: true })));

/* The one that bites people: a semicolon in a password ends the token early,
   and the driver then reports something that looks nothing like a quoting bug. */
const semi = B.compose({ ...MS, password: 'pa;ss=word' });
check('a SQL Server password containing ; and = is brace-quoted',
  /Password=\{pa;ss=word\}/.test(semi), semi);
check('and comes back out of parse() intact', B.parse(semi).password === 'pa;ss=word');
check('a closing brace inside a password is doubled, per the SQL Server rule',
  B.compose({ ...MS, password: 'a}b' }).indexOf('Password={a}}b}') !== -1
  && B.parse(B.compose({ ...MS, password: 'a}b' })).password === 'a}b');

/* ── 3. parse() is the other half of the toggle ──────────────────────────── */

for (const [label, fields] of [['postgres', PG], ['SQL Server', MS]]) {
  const back = B.parse(B.compose(fields));
  const same = ['engine', 'host', 'database', 'user', 'password']
    .filter((k) => back[k] !== fields[k]);
  check('a ' + label + ' string composed then parsed returns its own fields',
    same.length === 0, same.map((k) => k + ': ' + back[k] + ' ≠ ' + fields[k]).join(', '));
}
check('parsing a hand-written string finds its parts',
  (() => {
    const f = B.parse('Data Source=db1,1435;Initial Catalog=Sales;UID=app;PWD=x;Encrypt=false');
    return f.host === 'db1' && f.port === '1435' && f.database === 'Sales'
      && f.user === 'app' && f.password === 'x' && f.encrypt === false;
  })(), JSON.stringify(B.parse('Data Source=db1,1435;Initial Catalog=Sales;UID=app;PWD=x;Encrypt=false')));
check('parsing a postgres URL with a search_path recovers the schema',
  B.parse(pgString).schema === 'rnacen');
check('parsing nothing yields empty fields rather than throwing',
  B.parse('').host === '' && B.parse(null).engine === 'mssql');
check('an incomplete form composes nothing, rather than a string that fails oddly',
  B.compose({ engine: 'mssql', host: 'only-a-host' }) === ''
  && B.compose({ engine: 'postgres', database: 'only-a-db' }) === '');

/* ── 4. The preview shows the shape, never the password ──────────────────── */

check('the postgres preview hides the password', (() => {
  const m = B.mask(pgString);
  return m.indexOf('placeholder-pw') === -1 && m.indexOf('reader') !== -1
    && m.indexOf('hh-pgsql-public.ebi.ac.uk') !== -1;
})(), B.mask(pgString));
check('the SQL Server preview hides it too', (() => {
  const m = B.mask(msString);
  return m.indexOf('placeholder-pw') === -1 && m.indexOf('Finance') !== -1;
})(), B.mask(msString));
check('a brace-quoted password is hidden whole, not half',
  B.mask(semi).indexOf('pa;ss=word') === -1, B.mask(semi));
check('masking something with no password at all does not mangle it',
  B.mask('Server=a;Database=b') === 'Server=a;Database=b');
// The mssql:// case is in section 6, with the rest of that spelling.

/* ── 5. Wiring ───────────────────────────────────────────────────────────── */

const dash = read('public', 'dashboard.html');
const app = read('public', 'dashboard-app.js');

check('the dashboard loads the builder', /<script src="\/cygenix-conn-builder\.js/.test(dash));
for (const side of ['src', 'tgt']) {
  check('the ' + side + ' side offers both ways in',
    dash.indexOf('id="' + side + '-entry-paste"') !== -1
    && dash.indexOf('id="' + side + '-entry-build"') !== -1);
  check('the ' + side + ' form has every field the string needs',
    ['engine', 'host', 'port', 'db', 'user', 'pw', 'schema']
      .every((f) => dash.indexOf('id="' + side + '-b-' + f + '"') !== -1));
  check('and a preview of what it is building',
    dash.indexOf('id="' + side + '-b-preview"') !== -1);
}
check('the form writes into the connection string field, not beside it',
  /getElementById\('proj-' \+ side \+ '-cs'\)/.test(app)
  || /proj-\$\{side\}-cs/.test(app) || /'proj-' \+ side \+ '-cs'/.test(app),
  'there must be exactly one field of record for a connection');
check('switching to the form parses whatever is already in that field',
  /function connBuildLoadFromString/.test(app) && /B\.parse\(target\.value\)/.test(app));

/* The form is what Edit opens, so it is filled in without the user asking.
   That makes the write-back dangerous in a way it was not when only a
   deliberate click could reach it: compose() knows the keywords it models and
   no others, so a stored string carrying MultipleActiveResultSets, Application
   Name, Connection Timeout or an Authentication= mode would come back trimmed,
   and the next Save would persist the trimmed version with nobody seeing it
   happen. Opening the form reads; only editing a field writes. */
check('the second way in is called Settings',
  dash.indexOf('>Settings</button>') !== -1 && dash.indexOf('Build from details') === -1);
check('Edit opens Settings rather than the raw string',
  /if \(typeof setConnEntry === 'function'\) setConnEntry\(side, connEntry\[side\] \|\| 'build'\)/.test(app)
  && /var connEntry = \{ src: 'build', tgt: 'build' \}/.test(app));
check('with the cursor in the first field of the form, not in a hidden input',
  /getElementById\(side \+ '-b-host'\)/.test(app));
check('and opening it does NOT rewrite the stored string — only editing does',
  /function connBuildChanged\(side, engineChanged, quiet\)/.test(app)
  && /!quiet[^\n]*target\.value = cs;/.test(app)
  && /connBuildChanged\(side, false, true\)/.test(app),
  'a form that silently trims a keyword it does not model is worse than no form');
check('AND AN INCOMPLETE FORM NEVER ERASES A STORED CONNECTION',
  /if \(target && !quiet && \(cs \|\| !target\.value\)\) target\.value = cs;/.test(app),
  'compose() returns "" until the form has a host and a database; writing that '
  + 'through deleted the connection the user had opened in order to edit it');
check('and the preview shows the string that is really stored, not the recomposed one',
  /var shown = \(quiet && stored\) \? stored : \(cs \|\| stored\);/.test(app));
check('a side with nothing configured gets the same default, without pressing Edit',
  /setConnEntry\('src', connEntry\.src\)/.test(app) && /setConnEntry\('tgt', connEntry\.tgt\)/.test(app));
check('the preview is the masked form, so a password is never drawn on screen',
  /prev\.textContent = shown \? B\.mask\(shown\)/.test(app));
check('and the form writes through to the string field on every keystroke',
  /target\.value = cs/.test(app) && /oninput="connBuildChanged/.test(dash));
check('the engine choice only replaces a port the user has not chosen',
  /portEl\.value === other/.test(app),
  'retyping a deliberate port would be the form arguing with the user');

/* ── 6. SQL Server's OTHER spelling, and what not knowing it cost ────────────
   db-connect.js has always accepted mssql:// URLs — its own error message
   advertises the form — and a great many saved connections on this product
   are written that way. This parser did not know it. Every field came back
   empty, so the Settings form showed a blank connection, and one keystroke
   composed that blank back over the field of record. A working connection
   was replaced with nothing, silently, and the next Save persisted it.

   The round-trip below runs against the REAL server parser, lifted from
   db-connect.js, for the same reason the postgres one does.               */
const serverMssql = new Function(lift('parseMssqlConnectionString') + '\nreturn parseMssqlConnectionString;')();

const MS_URL = 'mssql://svc_fin:S3cret-pw@fin-dm.database.windows.net:1433/FIN_DM';
const msUrlF = B.parse(MS_URL);
check('AN mssql:// URL PARSES INTO ITS PARTS instead of coming back empty',
  msUrlF.host === 'fin-dm.database.windows.net' && msUrlF.port === '1433'
  && msUrlF.database === 'FIN_DM' && msUrlF.user === 'svc_fin' && msUrlF.password === 'S3cret-pw',
  JSON.stringify(msUrlF));
check('sqlserver:// is the same string by another name',
  B.parse(MS_URL.replace(/^mssql/, 'sqlserver')).host === 'fin-dm.database.windows.net');
check('AND COMPOSING IT BACK RETURNS THE SAME STRING, byte for byte',
  B.compose(msUrlF) === MS_URL, B.compose(msUrlF));
check('a URL-form connection is not silently rewritten into keyword form',
  B.compose(msUrlF).indexOf('mssql://') === 0 && msUrlF.form === 'url'
  && B.parse(msString).form === 'kv');
check('and a keyword-form connection is not turned into a URL either',
  B.compose(B.parse(msString)).indexOf('Server=') === 0);
check('the server reads what we compose — host, database and credentials all arrive',
  (() => {
    const c = serverMssql(B.compose(msUrlF));
    return c.server === 'fin-dm.database.windows.net' && c.port === 1433
      && c.database === 'FIN_DM' && c.user === 'svc_fin' && c.password === 'S3cret-pw';
  })(), JSON.stringify(serverMssql(B.compose(msUrlF))));
check('a password with a URL-significant character survives the round trip',
  (() => {
    const f = { ...msUrlF, password: 'p@ss/w:rd?&x' };
    return serverMssql(B.compose(f)).password === 'p@ss/w:rd?&x';
  })());
check('encrypt=false is carried through, and the server agrees the cert is not being checked',
  (() => {
    const f = B.parse(MS_URL + '?encrypt=false');
    const c = serverMssql(B.compose(f));
    return f.encrypt === false && f.trustCert === true
      && c.options.encrypt === false && c.options.trustServerCertificate === true;
  })());
check('trustServerCertificate=true alone is carried through as well',
  serverMssql(B.compose(B.parse(MS_URL + '?trustServerCertificate=true')))
    .options.trustServerCertificate === true);
check('the defaults are left off, because spelling them out changes nothing',
  B.compose(msUrlF).indexOf('?') === -1);
check('a URL with no port still names one, so the form is not blank where the server has a default',
  B.parse('mssql://h.example/DB').port === '1433');

/* The preview named postgres by name, so the commonest URL shape on this
   product printed its password in full — in the one place this function
   exists to stop that happening. It went unnoticed because the form could
   not read an mssql:// URL at all, so nobody ever saw the preview of one. */
check('AN mssql:// PASSWORD IS HIDDEN TOO, not only a postgres one',
  B.mask(MS_URL).indexOf('S3cret-pw') === -1
  && B.mask(MS_URL).indexOf('fin-dm.database.windows.net') !== -1
  && B.mask(MS_URL).indexOf('svc_fin') !== -1, B.mask(MS_URL));
check('and a URL carrying no credentials is left exactly as it is',
  B.mask('mssql://h.example:1433/DB') === 'mssql://h.example:1433/DB');

/* ── 7. A value that is not a connection never reaches the store ──────────── */
const LC = B.looksLikeConnection;
check('a label is not a connection, however confidently it was typed',
  LC('API') === false && LC('Finance source') === false && LC('') === false
  && LC(null) === false && LC('   ') === false);
check('every shape this product actually dials IS one',
  LC(MS_URL) && LC(msString) && LC(pgString)
  && LC('https://cygenix-db-api.azurewebsites.net/api/db')
  && LC('Data Source=db1;Initial Catalog=Sales;') && LC('host=h dbname=d'));
check('SAVING REFUSES A VALUE THAT IS NOT A CONNECTION, and says what the shapes are',
  /const connLooksReal = /.test(app)
  && /if \(!connLooksReal\(cs\)\)\{/.test(app)
  && /That is not a connection string/.test(app)
  && /mssql:\/\/user:pass@host:1433\/database/.test(app),
  'one profile reached production holding the word "API" in this field');
check('and an Azure Function entry is refused unless it carries a URL',
  /if \(!\/\^https\?:\\\/\\\/\/i\.test\(url\)\)\{/.test(app));
check('the refusal happens before anything is written, not after',
  app.indexOf('const connLooksReal =') < app.indexOf('entry.connString = cs;')
  && app.indexOf("if (!connLooksReal(cs)){") < app.indexOf('entry.connString = cs;'));
check('a saved entry holding a label is drawn as broken rather than vouched for as MSSQL',
  /function sconnUsability\(entry\)\{/.test(app)
  && /return 'Not a connection — re-save it';/.test(app)
  && /if \(sconnUsability\(entry\) === 'bad'\) return 'CHECK';/.test(app));

/* The collation card carries its own copy of this check, because it runs on
   pages where the builder is not loaded. Two copies of one rule drift, so
   they are held to the same answers here — the same arrangement as the
   collation rules module and its Function App twin. */
const collation = read('public', 'cygenix-collation.js');
const collLooks = new Function(
  collation.slice(collation.indexOf('function looksLikeConnection(value)'),
                  collation.indexOf('function isSqlServer(engine)'))
    .replace(/\/\* Which engine[\s\S]*$/, '')
  + '\nreturn looksLikeConnection;')();
const SHARED = [MS_URL, msString, pgString, 'https://x.azurewebsites.net/api/db',
  'Data Source=db1;Initial Catalog=Sales;', 'host=h dbname=d', 'sqlserver://h/db',
  'API', 'Finance source', '', '   ', 'db.example.com', 'localhost:1433'];
const disagree = SHARED.filter((v) => !!LC(v) !== !!collLooks(v));
check('THE TWO COPIES OF THE CHECK AGREE, on every shape either one will meet',
  disagree.length === 0, disagree.join(' | '));

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
