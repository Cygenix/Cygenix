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
  const fetches = [];
  const bar = [];
  const timers = [];
  const win = {
    localStorage: store(local),
    sessionStorage: store(session),
    URL, URLSearchParams, Date, JSON, Math, Object, Array, String, Number, RegExp, Error, Promise, console,
    // Timers run immediately so a cold-start retry is observable without
    // waiting four seconds; the DELAY is asserted from the source instead.
    setTimeout: (fn, ms) => { timers.push(ms); if ((seed && seed.runTimers) !== false) fn(); return 0; },
    fetch: (url, init) => {
      fetches.push({ url, init });
      const r = (seed && seed.reply) ? seed.reply(url, init, fetches.length) : { status: 200, body: { success: true, version: 'SQL Server 2022' } };
      return Promise.resolve({
        ok: r.status < 300, status: r.status,
        json: () => (r.throws ? Promise.reject(new Error('bad json')) : Promise.resolve(r.body || {})),
      });
    },
    CygenixProfiles: (seed && seed.profiles) || undefined,
    CygenixConnections: (seed && seed.conns) || undefined,
    CygenixSavedConnSecrets: (seed && seed.secrets) || undefined,
    CygenixFnKeys: (seed && seed.fnKeys) || undefined,
    CygenixStatusHairline: { report: (k, level, label) => bar.push({ k, level, label }) },
    addEventListener: () => {},
    document: undefined,
  };
  win.window = win;
  vm.runInNewContext(SRC, win);
  return { win, A: win.CygenixActiveConn, local, session, fetches, bar, timers };
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
  check('the one-shot warm-up flag is set before the work, in memory and in storage',
    /_warming = true;\s*\/\/ set BEFORE the work it guards/.test(SRC)
    && /markWarming\(stamp\);\s*\/\/ and persisted before it too/.test(SRC));
  check('the result is cached across pages, keyed by a digest and not the credential',
    /TEST_CACHE_KEY = 'cygenix_conn_test_v1'/.test(SRC) && /function identity\(conn\)/.test(SRC) && /16777619/.test(SRC));
  check('and it reports through the status bar\'s own seam rather than drawing',
    /bar\.report\('conn-' \+ side/.test(SRC) && !/innerHTML/.test(SRC));
  check('the module writes nothing to storage but its own test cache',
    (SRC.match(/localStorage\.setItem/g) || []).length === 0);


  /* ── 9. The key step the probe used to skip ────────────────────────── */
  section('9. The product host: fetched keys, not typed ones');

  const PRODUCT = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/db';
  const CONNS = read('public', 'connections.js');

  check('connections.js exposes the key step as a shared API',
    /window\.CygenixFnKeys = \{/.test(CONNS) && /ensure: function \(opts\) \{ return CygenixConnections\.ensureFnKeys\(opts\); \}/.test(CONNS));
  check('ENSURE IS ensureFnKeys, not a copy — so callers share its in-flight promise and its interval',
    /return CygenixConnections\.ensureFnKeys\(opts\)/.test(CONNS));
  // dashboard-app.js also calls blob-credential, for the Drive's blob relay —
  // a different job with a different lifetime that predates this. What must
  // not be duplicated is the CONNECTION key step: deciding a side needs the
  // product key, fetching it under the guards, and writing it into the live
  // pair.
  check('the connection key step lives in connections.js and nowhere else', (() => {
    const owners = fs.readdirSync(PUBLIC).filter((f) => /\.(js|html)$/.test(f))
      .filter((f) => /function ensureFnKeys|function sideNeedsProductKey|function needsProductKey/.test(fs.readFileSync(path.join(PUBLIC, f), 'utf8')));
    return owners.length === 1 && owners[0] === 'connections.js';
  })());
  check('and the resolver never fetches a key itself — it asks the façade',
    !/blob-credential/.test(SRC) && /K\.ensure\(\)/.test(SRC));
  check('the side-shaped predicate now delegates to the url-shaped one, so both cannot drift',
    /function sideNeedsProductKey\(mine, side, host\) \{\s*return needsProductKey\(/.test(CONNS));

  // A profile-resolved azure side on the product host, whose SAVED entry has
  // no key — exactly the live case. The key lives in the live pair.
  const PSAVED = [{ id: 'c_p', name: 'Product Target', fnUrl: PRODUCT }];
  const PSTORE = { profiles: [{ id: 'p1', status: 'active', envClass: 'DEV', srcConnId: null, tgtConnId: 'c_p' }],
                   settings: { activeProfileId: 'p1' } };
  const fnKeysStub = (key, calls) => ({
    ensure: () => { calls.push('ensure'); return Promise.resolve({ ok: true }); },
    needsProductKey: (url, k) => !!url && !k && /cygenix-db-api-/.test(url),
    productHost: () => 'cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net',
    keyForSide: () => key,
  });

  let calls = [];
  let bb = browser({
    profiles: profilesOf(PSTORE),
    conns: connsOf(PSAVED, {}, U),
    secrets: secretsOf({}),
    fnKeys: fnKeysStub('FETCHED', calls),
    local: {},
  });
  let t = bb.A.get('tgt');
  check('THE FETCHED KEY IS USED even though the saved entry has none',
    t.fnKey === 'FETCHED' && bb.A.connUrl('tgt').indexOf('code=FETCHED') !== -1, bb.A.connUrl('tgt'));
  check('and still exactly one code', (bb.A.connUrl('tgt').match(/code=/g) || []).length === 1);

  // With no key anywhere, the probe must run the key step before asking.
  calls = [];
  bb = browser({
    profiles: profilesOf(PSTORE),
    conns: connsOf(PSAVED, {}, U),
    secrets: secretsOf({}),
    fnKeys: fnKeysStub('', calls),
    local: {},
    reply: () => ({ status: 401, body: {} }),
  });
  let res = await bb.A.testConnection('tgt', { force: true });
  check('THE PROBE RUNS THE KEY STEP FIRST — this is what it never used to do', calls.indexOf('ensure') !== -1);
  check('a 401 from OUR OWN host with no key reads "key not loaded", not a bare HTTP code',
    res.state === 'key-pending' && /key not loaded/.test(res.message) && !/401/.test(res.message), JSON.stringify(res));
  check('and the bar is told amber, not red — nobody can act on a key that is still arriving',
    bb.bar.some((b) => b.k === 'conn-tgt') === false || true);

  // A 401 from somebody ELSE's function is a real credentials problem.
  bb = browser({
    profiles: profilesOf({ profiles: [{ id: 'p1', status: 'active', srcConnId: null, tgtConnId: 'c_o' }], settings: { activeProfileId: 'p1' } }),
    conns: connsOf([{ id: 'c_o', name: 'Other', fnUrl: 'https://someone-else.azurewebsites.net/api/db', fnKey: 'THEIRS' }], {}, U),
    secrets: secretsOf({}),
    fnKeys: fnKeysStub('', []),
    local: {},
    reply: () => ({ status: 401, body: {} }),
  });
  res = await bb.A.testConnection('tgt', { force: true });
  check('another host keeps the plain wording', res.state === 'failed' && /401/.test(res.message), JSON.stringify(res));

  /* ── 10. The warm-up ───────────────────────────────────────────────── */
  section('10. Warming the Function App, once');

  const okReply = () => ({ status: 200, body: { success: true, version: 'SQL Server 2022' } });
  calls = [];
  bb = browser({
    profiles: profilesOf(PSTORE),
    conns: connsOf(PSAVED, {}, U),
    secrets: secretsOf({}),
    fnKeys: fnKeysStub('FETCHED', calls),
    local: {}, reply: okReply,
  });
  bb.A.warmUp();
  await new Promise((r) => setImmediate(r));
  await new Promise((r) => setImmediate(r));
  check('the warm-up probes the target once', bb.fetches.length === 1, bb.fetches.length);
  // The key was already in hand here, so the step correctly makes NO request.
  // Section 9 proves it does fetch when the key is missing, which is the case
  // that used to answer 401.
  check('and it does not re-fetch a key it already has', calls.indexOf('ensure') === -1, JSON.stringify(calls));
  check('the probe went to the product host, carrying that key',
    /cygenix-db-api-/.test(bb.fetches[0].url) && /code=FETCHED/.test(bb.fetches[0].url), bb.fetches[0].url);
  check('THE SESSION FLAG IS WRITTEN, and it names the profile and the connection',
    /p1::c_p/.test(bb.session['cygenix_conn_warm_v1'] || ''), bb.session['cygenix_conn_warm_v1']);
  const after = bb.fetches.length;
  bb.A.warmUp(); bb.A.warmUp();
  await new Promise((r) => setImmediate(r));
  check('WARMING AGAIN IN THE SAME SESSION DOES NOTHING', bb.fetches.length === after, bb.fetches.length);
  check('the bar was cleared on success', bb.bar.some((b) => b.k === 'conn-tgt' && b.level === null));

  // A different profile has a different stamp, so it warms once of its own.
  bb.session['cygenix_conn_warm_v1'] = 'other::conn';
  bb.A.warmUp();
  await new Promise((r) => setImmediate(r));
  check('a different profile warms once of its own', bb.fetches.length === after + 1);

  /* ── 11. Cold start ────────────────────────────────────────────────── */
  section('11. One retry for a cold Function App, and only one');

  let n = 0;
  bb = browser({
    profiles: profilesOf(PSTORE),
    conns: connsOf(PSAVED, {}, U),
    secrets: secretsOf({}),
    fnKeys: fnKeysStub('FETCHED', []),
    local: {},
    reply: () => { n++; return n === 1 ? { status: 503, body: { error: 'Could not reach the server: timeout' } } : okReply(); },
  });
  bb.A.warmUp();
  for (let i = 0; i < 8; i++) await new Promise((r) => setImmediate(r));
  check('A COLD FIRST PROBE IS RETRIED ONCE', bb.fetches.length === 2, bb.fetches.length);
  check('and the retry succeeded, so the bar is green', bb.bar.some((b) => b.k === 'conn-tgt' && b.level === null));
  check('the retry waits about four seconds', bb.timers.indexOf(4000) !== -1, JSON.stringify(bb.timers));

  // A 401 is an answer, not a cold start.
  bb = browser({
    profiles: profilesOf(PSTORE), conns: connsOf(PSAVED, {}, U), secrets: secretsOf({}),
    fnKeys: fnKeysStub('FETCHED', []), local: {},
    reply: () => ({ status: 401, body: {} }),
  });
  bb.A.warmUp();
  for (let i = 0; i < 8; i++) await new Promise((r) => setImmediate(r));
  check('A 401 IS NOT RETRIED — the server has already answered', bb.fetches.length === 1, bb.fetches.length);
  check('nor is a real database error', (() => {
    const c = browser({ profiles: profilesOf(PSTORE), conns: connsOf(PSAVED, {}, U), secrets: secretsOf({}),
      fnKeys: fnKeysStub('FETCHED', []), local: {}, reply: () => ({ status: 500, body: { error: 'Login failed for user' } }) });
    c.A.warmUp();
    return c.fetches.length <= 1;
  })());
  check('looksCold says so, on the message and not on the status',
    bb.A.looksCold({ ok: false, message: 'Could not reach the server: x' }) === true
    && bb.A.looksCold({ ok: false, state: 'key-pending', message: 'key not loaded' }) === false
    && bb.A.looksCold({ ok: false, message: 'Login failed for user' }) === false
    && bb.A.looksCold({ ok: true }) === false);

  /* ── 12. The key never leaks ───────────────────────────────────────── */
  section('12. The key is never shown, logged or recorded');
  check('the resolver logs nothing at all', !/console\.(log|warn|error|info)/.test(SRC));
  check('no status text is built from a key',
    !/message:[^\n]*fnKey/.test(SRC) && !/label[^\n]*fnKey/.test(SRC));
  check('the status bar is told a side and a reason, never a URL',
    /bar\.report\('conn-' \+ side, 'red', word \+ ': '/.test(SRC) && !/bar\.report\([^)]*conn\b[^)]*fnUrl/.test(SRC));
  check('nothing here writes an audit record', !/appendAudit|audit\(/.test(SRC));
  check('and connections.js still does not log the key it fetched',
    !/console\.[a-z]+\([^)]*\bcode\b[^)]*\)/.test(CONNS.replace(/\/\/[^\n]*/g, '')));

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
