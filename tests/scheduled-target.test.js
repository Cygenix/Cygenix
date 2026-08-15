// Tests the target/source connection contract between the two runners.
//
// A schedule is fired by netlify/functions/scheduled-runner.js, which decides
// whether the schedule is runnable and then dispatches it to the Azure
// Function App, where azure-function/src/run-migration.js executes it.
//
// Those two files each validate the connection independently, and they had
// drifted: the Netlify side accepted an https:// target (the Connections
// page's "Azure Function mode", which stores the Cygenix db API URL instead
// of a connection string) and dispatched the run, then the Azure side
// rejected the very same value with "Target must be a mssql:// connection".
// Every scheduled run against an Azure Function target was queued and then
// immediately failed.
//
// The rule this file pins down: anything scheduled-runner is willing to
// dispatch, run-migration must be willing to execute. The functions are
// pulled out of the shipped sources verbatim rather than reimplemented, so a
// change to either file that reopens the gap fails here.
const fs   = require('fs');
const path = require('path');
const vm   = require('vm');
const Module = require('module');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

// sql-entra requires @azure/identity lazily, inside the token path. Nothing
// here reaches that path, but a real managed-identity probe would hang if it
// ever did, so it is stubbed before the module is loaded.
const identityPath = require.resolve('@azure/identity');
require.cache[identityPath] = new Module(identityPath, null);
require.cache[identityPath].filename = identityPath;
require.cache[identityPath].loaded = true;
require.cache[identityPath].exports = {
  DefaultAzureCredential: class { async getToken() { return { token: 'TOK' }; } },
  ClientSecretCredential: class { async getToken() { return { token: 'TOK' }; } },
};
const { resolveSqlConfig, connectWithRetry } = require(
  path.join(__dirname, '..', 'netlify', 'functions', 'lib', 'sql-entra.js'));

const RM_PATH = path.join(__dirname, '..', 'azure-function', 'src', 'run-migration.js');
const SR_PATH = path.join(__dirname, '..', 'netlify', 'functions', 'scheduled-runner.js');
const rm = fs.readFileSync(RM_PATH, 'utf8');
const sr = fs.readFileSync(SR_PATH, 'utf8');

// ── Pull the shipped code out of the sources ────────────────────────────────
// Neither file can simply be required: run-migration pulls in @azure/functions
// and registers an HTTP route on import, and scheduled-runner reaches for
// Cosmos. Extracting the pieces keeps the test running the real text.
function block(src, marker, label) {
  const at = src.indexOf(marker);
  if (at < 0) throw new Error('could not find ' + label + ' (' + marker + ') in the source');
  const open = src.indexOf('{', at);
  let depth = 0;
  for (let i = open; i < src.length; i++) {
    const c = src[i];
    if (c === '{') depth++;
    else if (c === '}') { depth--; if (!depth) return src.slice(at, i + 1); }
  }
  throw new Error('unbalanced braces in ' + label);
}

const ctx = vm.createContext({
  process, URL, console, Array, JSON, Error,
  // Injected for openPool.
  resolveSqlConfig,
  connectWithRetry: (sqlMod, cfg, opts) => connectWithRetry(sqlMod, cfg, opts),
  parseMssqlUrl: null,
  sql: null,
  // Injected for the validation blocks.
  markRunFailed: null,
  containers: {}, run: {},
});
// run-migration names the Functions logging context `ctx`; the validation
// blocks pass it straight through to markRunFailed and never read it.
ctx.ctx = {};

vm.runInContext([
  rm.match(/^const isHttpUrl\s+=.*$/m)[0],
  rm.match(/^const isMssqlConn\s+=.*$/m)[0],
  block(rm, 'function isOwnDbApi(url)', 'isOwnDbApi'),
  block(rm, 'async function openPool(connStr)', 'openPool'),
  block(rm, 'function parseDbConnFromMssqlUrl(connStr)', 'parseDbConnFromMssqlUrl'),
  // The two validation gates, wrapped so a rejection is observable. Each
  // block ends in `return;` on the failure path, so reaching the tail means
  // the connection was accepted.
  'async function checkTarget(tgtConn) {\n' + block(rm, 'if (!tgtConn ||', 'target check') + '\n  return "ok";\n}',
  'async function checkSource(srcConn, needsSrc) {\n' + block(rm, 'if (needsSrc &&', 'source check') + '\n  return "ok";\n}',
].join('\n\n'), ctx, { filename: 'run-migration-extract.js' });

// The Netlify side's gate, from its own source, with its own helpers.
const srCtx = vm.createContext({});
vm.runInContext([
  sr.match(/^const isHttpUrl\s+=.*$/m)[0],
  sr.match(/^const isMssqlConn\s+=.*$/m)[0],
  'function runnerRejects(schedule) {\n' +
    block(sr, 'if (!schedule.tgtConn ||', 'runner target check') + '\n  return false;\n}',
].join('\n\n'), srCtx, { filename: 'scheduled-runner-extract.js' });

const withEnv = async (vars, fn) => {
  const saved = {};
  for (const k of Object.keys(vars)) { saved[k] = process.env[k]; process.env[k] = vars[k]; }
  try { return await fn(); }
  finally {
    for (const k of Object.keys(vars)) {
      if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k];
    }
  }
};

// Records what markRunFailed was told, and returns the message (or null when
// the connection was accepted).
async function rejection(fn) {
  let msg = null;
  ctx.markRunFailed = async (_c, _r, m) => { msg = m; };
  const out = await fn();
  return out === 'ok' ? null : (msg == null ? '(rejected with no message)' : msg);
}

const HOST    = 'cygenix-db-api-abc123.northeurope-01.azurewebsites.net';
const OWN_API = 'https://' + HOST + '/api/db';
const MSSQL   = 'mssql://sa:pw@other.example.com:1433/Legacy';

console.log('Scheduled runs — Azure Function mode targets\n');

(async () => {
  await withEnv({
    WEBSITE_HOSTNAME: HOST,
    WEBSITE_SITE_NAME: 'cygenix-db-api',
    SQL_SERVER: 'cygenix.database.windows.net',
    SQL_DATABASE: 'cygenix',
  }, async () => {
    // ── The bug ────────────────────────────────────────────────────────────
    check('a target in Azure Function mode is accepted',
      await rejection(() => ctx.checkTarget(OWN_API)) === null);
    check('an mssql:// target still works',
      await rejection(() => ctx.checkTarget(MSSQL)) === null);

    const empty = await rejection(() => ctx.checkTarget(''));
    check('an empty target is still rejected', empty !== null, String(empty));
    check('and the message names both accepted forms',
      /mssql:\/\//.test(empty) && /database API/i.test(empty), String(empty));

    // ── Someone else's API is not this app's database ──────────────────────
    // isOwnDbApi exists so an https:// URL pointing anywhere else fails with
    // a clear message instead of quietly reading the Function App's own
    // database and reporting it as the user's target.
    const foreign = await rejection(() => ctx.checkTarget('https://evil.example/api/db'));
    check('an https:// target that is not this deployment is rejected',
      foreign !== null, String(foreign));
    check('and the message says which URL was wrong',
      /evil\.example/.test(foreign), String(foreign));

    // The same app under a different hostname — a slot, or the older
    // <app>.azurewebsites.net form — is still this app, and a run should not
    // fail over a cosmetic difference in a URL saved months ago.
    check('the same app under another azurewebsites.net hostname is accepted',
      await rejection(() => ctx.checkTarget('https://cygenix-db-api.azurewebsites.net/api/db')) === null);
    check('a lookalike host outside azurewebsites.net is not',
      await rejection(() => ctx.checkTarget('https://cygenix-db-api.evil.example/api/db')) !== null);
    check('and neither is a different app that merely shares a prefix',
      await rejection(() => ctx.checkTarget('https://cygenix-db-apiary.azurewebsites.net/api/db')) !== null);

    // ── The source gate moves in step ──────────────────────────────────────
    check('a source in Azure Function mode is accepted',
      await rejection(() => ctx.checkSource(OWN_API, true)) === null);
    check('a missing source is still rejected when a step needs one',
      await rejection(() => ctx.checkSource('', true)) !== null);
    check('and is ignored when no step needs a source',
      await rejection(() => ctx.checkSource('', false)) === null);

    // ── The report names the database, not a blank ─────────────────────────
    const desc = ctx.parseDbConnFromMssqlUrl(OWN_API);
    check('the run report resolves Azure Function mode to the real database',
      desc && desc.server === 'cygenix.database.windows.net' && desc.database === 'cygenix',
      JSON.stringify(desc));
    const desc2 = ctx.parseDbConnFromMssqlUrl(MSSQL);
    check('an mssql:// string is still parsed from the URL',
      desc2 && desc2.server === 'other.example.com' && desc2.database === 'Legacy',
      JSON.stringify(desc2));

    // ── openPool connects as the app, not over HTTP to itself ──────────────
    {
      const connects = [];
      ctx.parseMssqlUrl = () => { throw new Error('parseMssqlUrl must not be used for an API URL'); };
      ctx.sql = { ConnectionPool: class {
        constructor(cfg) { this.cfg = cfg; connects.push(cfg); }
        async connect() { return this; }
      } };
      const pool = await ctx.openPool(OWN_API);
      const cfg = pool.cfg;
      check('openPool authenticates with the managed identity',
        cfg.authentication && cfg.authentication.type === 'azure-active-directory-default',
        JSON.stringify(cfg.authentication));
      check('against SQL_SERVER / SQL_DATABASE',
        cfg.server === 'cygenix.database.windows.net' && cfg.database === 'cygenix');
      check('with no username or password',
        cfg.user === undefined && cfg.password === undefined);
      check('encryption on', cfg.options && cfg.options.encrypt === true);
      // A migration statement is allowed hours. Dropping back to the driver
      // default here would kill long runs that used to work.
      check('and the long-running-migration timeouts preserved',
        cfg.requestTimeout === 14_400_000 && cfg.connectionTimeout === 90_000,
        cfg.requestTimeout + '/' + cfg.connectionTimeout);

      // The mssql:// path is untouched.
      ctx.parseMssqlUrl = (s) => ({ server: 'other.example.com', database: 'Legacy', user: 'sa', password: 'pw', _from: s });
      const p2 = await ctx.openPool(MSSQL);
      check('an mssql:// connection string still goes through parseMssqlUrl',
        p2.cfg._from === MSSQL && p2.cfg.user === 'sa', JSON.stringify(p2.cfg));
    }

    // ── The two runners agree ──────────────────────────────────────────────
    // This is the assertion that would have caught the original bug: every
    // target scheduled-runner is willing to dispatch must be one
    // run-migration is willing to execute.
    for (const tgt of [OWN_API, MSSQL, 'https://' + HOST + '/api/db/execute']) {
      const dispatched = !srCtx.runnerRejects({ tgtConn: tgt });
      const executed   = await rejection(() => ctx.checkTarget(tgt)) === null;
      check('dispatch and execution agree on ' + tgt,
        dispatched === executed, 'dispatched=' + dispatched + ' executed=' + executed);
    }
    for (const tgt of ['', 'postgres://x/y', 'not a connection']) {
      check('both runners reject ' + JSON.stringify(tgt),
        srCtx.runnerRejects({ tgtConn: tgt }) &&
        await rejection(() => ctx.checkTarget(tgt)) !== null);
    }
  });

  // ── Outside Azure ────────────────────────────────────────────────────────
  // WEBSITE_HOSTNAME is set by the Functions host. Running locally there is
  // nothing to compare against, and Azure Function mode has never meant
  // anything but the Cygenix API, so the URL is trusted rather than blocking
  // every local run.
  await withEnv({
    WEBSITE_HOSTNAME: '', WEBSITE_SITE_NAME: '', SQL_SERVER: 'x', SQL_DATABASE: 'y',
  }, async () => {
    check('with no WEBSITE_HOSTNAME an API URL is still accepted',
      await rejection(() => ctx.checkTarget(OWN_API)) === null);
  });

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})();
