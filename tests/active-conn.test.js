// tests/active-conn.test.js — one answer to "what am I connected to?", and
// the URL bug that made the Schema Explorer answer 401.
//
// WHY THIS FILE EXISTS
//
// Two defects, both of which read as "no connection" to a person.
//
//   1. THE DOUBLED KEY. Every page composed the Function App URL by
//      concatenating '?code=' onto a stored URL. That is correct exactly
//      once. The stored URL frequently ALREADY carried a code — because the
//      helper's own srcConn getter composes one and callers save the
//      composed value back — so the second append produced ?code=A&code=B,
//      the Function App read the last one, and the answer was 401. Twenty
//      four of the twenty seven sites also hardcoded '?', so a URL with any
//      query string at all became a double-? malformed address.
//
//   2. FOUR DISAGREEING RESOLVERS. The live blob holds a legacy flat shape
//      and a per-user block, and different readers preferred different ones,
//      so two screens in one tab could address two different databases.
//
// The module is EXECUTED here against a fake window, because the claims are
// about what it returns for storage it has never seen before.

'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));
const read = (...p) => fs.readFileSync(path.join(__dirname, '..', ...p), 'utf8');

const SRC = read('public', 'cygenix-active-conn.js');
const PUBLIC = path.join(__dirname, '..', 'public');

// ── A browser, in the small ───────────────────────────────────────────────
function browser(seed) {
  const local = Object.assign({}, (seed && seed.local) || {});
  const session = {};
  const store = (obj) => ({
    getItem: (k) => (k in obj ? obj[k] : null),
    setItem: (k, v) => { obj[k] = String(v); },
    removeItem: (k) => { delete obj[k]; },
    key: (i) => Object.keys(obj)[i],
    get length() { return Object.keys(obj).length; },
  });
  const win = {
    localStorage: store(local),
    sessionStorage: store(session),
    URL, URLSearchParams, Date, JSON, Math, Object, Array, String, Number, RegExp, Error, Promise, console,
    setTimeout: () => {},
    CygenixProfiles: (seed && seed.profiles) || undefined,
    CygenixConnections: (seed && seed.conns) || undefined,
    CygenixSavedConnSecrets: (seed && seed.secrets) || undefined,
  };
  win.window = win;
  vm.runInNewContext(SRC, win);
  return { win, A: win.CygenixActiveConn, local, session };
}

// Collaborators shaped like the real ones.
const profilesOf = (store) => ({ cpLoad: () => store });
const connsOf = (saved, live, user) => ({
  savedGetById: (id) => saved.find((c) => c.id === id) || null,
  savedGetAll: () => saved.slice(),
  currentUserTag: () => user,
  get: () => live || {},
});
const secretsOf = (map) => ({ get: (id) => map[id] || null });

const U = 'demo@cygenix.test';
const FN = 'https://fn.azurewebsites.net/api/db';

(async () => {
  console.log('The active connection — one resolver, one composer\n');

  /* ── 1. The composer ───────────────────────────────────────────────── */
  section('1. Composing the Function App URL');
  const { A } = browser({});

  check('a plain URL plus a key gets exactly one code',
    A.compose(FN, 'K1') === FN + '?code=K1', A.compose(FN, 'K1'));
  check('THE KEY IS NOT ADDED TWICE when the URL already carries one',
    A.compose(FN + '?code=K1', 'K2') === FN + '?code=K1', A.compose(FN + '?code=K1', 'K2'));
  check('…and that is true however the first one got there',
    (A.compose(FN + '?code=K1', 'K1').match(/code=/g) || []).length === 1);
  check('A URL WITH ANOTHER QUERY STRING DOES NOT GET A SECOND ?',
    A.compose(FN + '?x=1', 'K1') === FN + '?x=1&code=K1', A.compose(FN + '?x=1', 'K1'));
  check('the key is escaped', A.compose(FN, 'a b&c') === FN + '?code=a+b%26c', A.compose(FN, 'a b&c'));
  check('no key means no code parameter', A.compose(FN, '') === FN);
  check('A NON-HTTP VALUE IS NOT AN ADDRESS — "API" is refused, not called',
    A.compose('API', 'K1') === '' && A.compose('API', '') === '');
  check('nor is a connection string that was filed in the URL box',
    A.compose('Server=x;Database=y;User Id=u;Password=p;', 'K') === '');
  check('nor an empty or missing value', A.compose('', 'K') === '' && A.compose(null, 'K') === '' && A.compose(undefined, undefined) === '');
  check('a malformed URL is refused rather than thrown', A.compose('https://', 'K') === '' || typeof A.compose('https://', 'K') === 'string');

  /* ── 2. Resolution order ───────────────────────────────────────────── */
  section('2. Which connection wins');

  const SAVED = [
    { id: 'c_src', name: 'Profile Source', connString: '' },
    { id: 'c_tgt', name: 'Profile Target', fnUrl: FN },
  ];
  const SECRETS = {
    c_src: { connString: 'Server=profile-src;Database=P;User Id=u;Password=p;' },
    c_tgt: { fnKey: 'PROFKEY' },
  };
  const STORE = {
    profiles: [{ id: 'prof_1', status: 'active', envClass: 'DEV', srcConnId: 'c_src', tgtConnId: 'c_tgt' }],
    settings: { activeProfileId: 'prof_1' },
  };
  const LIVE = {
    srcConnString: 'Server=live-src;Database=L;User Id=u;Password=p;', srcConnMode: 'direct',
    tgtConnString: '', tgtConnMode: 'azure', tgtFnUrl: 'https://live.example.net/api/db', tgtFnKey: 'LIVEKEY',
  };

  let b = browser({
    profiles: profilesOf(STORE),
    conns: connsOf(SAVED, LIVE, U),
    secrets: secretsOf(SECRETS),
    local: { cygenix_project_connections: JSON.stringify({ [U]: LIVE }) },
  });
  let src = b.A.getActiveConnection('src');
  let tgt = b.A.getActiveConnection('tgt');
  check('THE ACTIVE PROFILE WINS over the live pair, on both sides',
    src.source === 'profile' && tgt.source === 'profile', JSON.stringify({ s: src.source, t: tgt.source }));
  check('the source carries the secret from the local store, not from the saved entry',
    /profile-src/.test(src.connString) && src.mode === 'direct', src.connString);
  check('the target is the profile\'s function, with its key composed once',
    b.A.connUrl('tgt') === FN + '?code=PROFKEY', b.A.connUrl('tgt'));
  check('the descriptor names the connection and the profile it came from',
    src.connId === 'c_src' && src.profileId === 'prof_1' && tgt.connId === 'c_tgt');

  // No profile selected → the live pair.
  b = browser({
    profiles: profilesOf({ profiles: STORE.profiles, settings: { activeProfileId: null } }),
    conns: connsOf(SAVED, LIVE, U),
    secrets: secretsOf(SECRETS),
    local: { cygenix_project_connections: JSON.stringify({ [U]: LIVE }) },
  });
  check('with nothing selected the live pair is used', b.A.getActiveConnection('src').source === 'live');
  check('and its function URL is composed once too',
    b.A.connUrl('tgt') === 'https://live.example.net/api/db?code=LIVEKEY', b.A.connUrl('tgt'));

  // Legacy top-level only.
  b = browser({
    conns: connsOf([], null, U),
    local: { cygenix_project_connections: JSON.stringify({
      srcConnString: 'Server=legacy;Database=O;User Id=u;Password=p;', tgtFnUrl: FN, tgtFnKey: 'LEG' }) },
  });
  check('THE LEGACY TOP-LEVEL FIELDS ARE THE LAST FALLBACK, not ignored',
    b.A.getActiveConnection('src').source === 'legacy' && /legacy/.test(b.A.getActiveConnection('src').connString));
  check('and the per-user block beats them when both exist', (() => {
    const c = browser({
      conns: connsOf([], null, U),
      local: { cygenix_project_connections: JSON.stringify({
        srcConnString: 'Server=legacy;Database=O;User Id=u;Password=p;',
        [U]: { srcConnString: 'Server=peruser;Database=N;User Id=u;Password=p;' } }) },
    });
    return /peruser/.test(c.A.getActiveConnection('src').connString);
  })());

  /* ── 3. A credential this browser does not have ────────────────────── */
  section('3. Named, but not on this device');
  b = browser({
    profiles: profilesOf(STORE),
    conns: connsOf(SAVED, LIVE, U),
    secrets: secretsOf({}),                    // no secrets at all
    local: { cygenix_project_connections: JSON.stringify({ [U]: LIVE }) },
  });
  src = b.A.getActiveConnection('src');
  check('the side is NOT usable', src.ok === false);
  check('IT DOES NOT SILENTLY FALL BACK TO THE LIVE PAIR — that would connect to the wrong database',
    !/live-src/.test(src.connString || ''), src.connString);
  check('it names the connection a person has to finish', src.needsSecret === true && /Profile Source/.test(src.why), src.why);
  check('and connUrl gives nothing, so a caller cannot call it', b.A.connUrl('src') === '');
  // A function key is SOFT, deliberately, and this matches normaliseEntry in
  // cygenix-profile-apply.js: a Function App may legitimately be anonymous,
  // so a missing key is reported but not treated as "cannot connect". The
  // connection string is the opposite — it IS the secret, so its absence is
  // hard. Diverging here would make the two files disagree about the same
  // saved entry.
  check('a missing FUNCTION KEY is soft: the URL is still offered, without a code',
    b.A.connUrl('tgt') === FN && !/code=/.test(b.A.connUrl('tgt')), b.A.connUrl('tgt'));
  check('while a missing CONNECTION STRING is hard, because the string is the secret',
    b.A.connUrl('src') === '' && b.A.getActiveConnection('src').needsSecret === true);

  /* ── 4. A retired profile ──────────────────────────────────────────── */
  section('4. Retired and missing profiles');
  b = browser({
    profiles: profilesOf({ profiles: [Object.assign({}, STORE.profiles[0], { status: 'retired' })], settings: { activeProfileId: 'prof_1' } }),
    conns: connsOf(SAVED, LIVE, U),
    secrets: secretsOf(SECRETS),
    local: { cygenix_project_connections: JSON.stringify({ [U]: LIVE }) },
  });
  check('a retired profile is ignored and the live pair is used', b.A.getActiveConnection('src').source === 'live');
  b = browser({
    profiles: profilesOf({ profiles: [{ id: 'prof_1', status: 'active', srcConnId: 'gone', tgtConnId: 'gone' }], settings: { activeProfileId: 'prof_1' } }),
    conns: connsOf([], null, U),
    local: {},
  });
  src = b.A.getActiveConnection('src');
  check('a profile naming a connection that no longer exists says so', !src.ok && /not in the saved list/.test(src.why), src.why);

  /* ── 5. Storage that misbehaves ────────────────────────────────────── */
  section('5. Nothing here may throw');
  b = browser({ local: { cygenix_project_connections: '{not json' } });
  check('a corrupt blob resolves to nothing rather than throwing', b.A.getActiveConnection('src').ok === false);
  b = browser({ profiles: { cpLoad: () => { throw new Error('boom'); } }, local: {} });
  check('a profile store that throws is survived', b.A.getActiveConnection('src').ok === false);
  b = browser({ conns: { savedGetById: () => { throw new Error('boom'); }, currentUserTag: () => U }, profiles: profilesOf(STORE), local: {} });
  check('a saved-connection lookup that throws is survived', b.A.getActiveConnection('src').ok === false);

  /* ── 6. Every call site converted ──────────────────────────────────── */
  section('6. No page composes a URL by hand any more');
  const files = fs.readdirSync(PUBLIC).filter((f) => /\.(js|html)$/.test(f));
  const offenders = [];
  for (const f of files) {
    if (f === 'cygenix-active-conn.js') continue;          // its header quotes the old shape
    const s = fs.readFileSync(path.join(PUBLIC, f), 'utf8');
    s.split('\n').forEach((line, i) => {
      if (/\.replace\(|\.exec\(|placeholder|^\s*\/\/|^\s*\*/.test(line)) return;
      if (/\+\s*'[?&]code='|'[?&]code='\s*\+|\+\s*"[?&]code="|'code='\s*\+\s*encodeURI/.test(line)) {
        offenders.push(f + ':' + (i + 1) + '  ' + line.trim().slice(0, 90));
      }
    });
  }
  check('NOT ONE SITE STILL CONCATENATES A code= PARAMETER', offenders.length === 0, offenders.join(' | '));

  const composers = files.filter((f) => /CygenixActiveConn\.compose\(/.test(fs.readFileSync(path.join(PUBLIC, f), 'utf8')));
  check('the composer is used across the product, not in one place', composers.length >= 12, composers.length + ' files');

  // The three sides the sweep found that never appended a key at all.
  const pb = read('public', 'project-builder-app.js');
  check('the project builder composes a SOURCE key, which it never used to',
    /globalSrcConn[\s\S]{0,400}CygenixActiveConn\.compose\(c\.srcFnUrl, c\.srcFnKey\)/.test(pb));
  const bal = read('public', 'balancing.html');
  check('Balancing accepts an azure SOURCE, which it used to ignore',
    /srcConn = c\.srcConnString \|\| CygenixActiveConn\.compose\(c\.srcFnUrl, c\.srcFnKey\)/.test(bal));
  const asr = read('public', 'assurance.html');
  check('Assurance accepts an azure connection on either side', (asr.match(/CygenixActiveConn\.compose/g) || []).length === 2);
  const diag = read('public', 'cygenix-diagnostics.js');
  check('the diagnostics test sends the key, so it stops failing when the product works',
    /pick\(c\.srcFnUrl, c\.srcFnKey, c\.srcConnString\)/.test(diag));

  /* ── 7. Loaded where it is needed ──────────────────────────────────── */
  section('7. Loaded everywhere connections are read');
  const pages = files.filter((f) => f.endsWith('.html') && /<script src="\/connections\.js/.test(fs.readFileSync(path.join(PUBLIC, f), 'utf8')));
  const without = pages.filter((f) => !/cygenix-active-conn\.js/.test(fs.readFileSync(path.join(PUBLIC, f), 'utf8')));
  check('every page that loads connections.js also loads the resolver', without.length === 0, without.join(', '));
  check('and there are as many of them as the product has connection pages', pages.length >= 29, pages.length);

  /* ── 8. The guards ─────────────────────────────────────────────────── */
  section('8. The test call is guarded');
  check('one in flight at a time', /if \(_testInflight\[s\]\) return _testInflight\[s\];/.test(SRC));
  check('a three second minimum between calls', /TEST_MIN_INTERVAL_MS = 3000/.test(SRC) && /Date\.now\(\) - _testAt\[s\]\) < TEST_MIN_INTERVAL_MS/.test(SRC));
  check('the clock is stamped BEFORE the request, not in its callback',
    /_testAt\[s\] = Date\.now\(\);\s*\/\/ stamped BEFORE the request/.test(SRC));
  check('the one-shot status flag is set before the work and never cleared by a callback',
    /_verified = true;\s*\/\/ set BEFORE the work it guards/.test(SRC) && !/_verified = false;[\s\S]{0,200}then\(/.test(SRC));
  check('the result is cached across pages, keyed by a digest and not the credential',
    /TEST_CACHE_KEY = 'cygenix_conn_test_v1'/.test(SRC) && /function identity\(conn\)/.test(SRC) && /16777619/.test(SRC));
  check('and it reports through the status bar\'s own seam rather than drawing',
    /bar\.report\('conn-' \+ side/.test(SRC) && !/innerHTML/.test(SRC));
  check('the module writes nothing to storage but its own test cache',
    (SRC.match(/localStorage\.setItem/g) || []).length === 0);

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
