// tests/audit-schema.test.js — the audit event shape, its categories and,
// above all, its redaction.
//
// The redaction block is the one that matters. Everything else in this file
// tests a convenience; that block tests the property that cannot be fixed
// after the fact. The chain is append-only and tamper-evident by design, so
// a credential written into it cannot be removed later without breaking
// verification for every entry after it. There is no rollback. The tests
// below therefore go looking for the ways a secret gets in around the
// obvious `password` field: nested, in an array, inside a connection string
// whose own field name is innocent, and in a before/after diff.

'use strict';

const A = require('../netlify/functions/lib/audit-schema');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Audit schema — categories, redaction and entry construction\n');

// ── Categories ────────────────────────────────────────────────────────────
console.log('Categories');

check('all eleven categories from the brief exist',
  A.CATEGORY_KEYS.length === 11);
check('the four always-on categories are security, access, prod and audit',
  JSON.stringify(A.ALWAYS_ON.slice().sort()) ===
  JSON.stringify(['access', 'audit', 'prod', 'security']));
check('isAlwaysOn is true for security and false for settings',
  A.isAlwaysOn('security') === true && A.isAlwaysOn('settings') === false);
check('an unknown category is not always-on (deny the exemption, not the event)',
  A.isAlwaysOn('nonsense') === false);

check('categoryFor files a role change under access',
  A.categoryFor('role.assign') === 'access');
check('categoryFor files a stream pause under stream',
  A.categoryFor('stream.pause') === 'stream');
check('categoryFor files the log\'s own events under audit',
  A.categoryFor('audit.export') === 'audit');
check('an unrecognised action is filed, never dropped',
  A.CATEGORY_KEYS.indexOf(A.categoryFor('wibble.frobnicate')) !== -1);

// PROD is the interesting one: a settings change against Production is a
// Production change first, otherwise a pause of the `settings` category
// would swallow it — and `prod` is always-on precisely so it cannot be.
check('a PROD settings change is categorised prod, not settings',
  A.categoryFor('sysparam.update', 'PROD') === 'prod');
check('a DEV settings change stays in settings',
  A.categoryFor('sysparam.update', 'DEV') === 'settings');

// ── The client allowlist ──────────────────────────────────────────────────
console.log('\nClient allowlist');

check('an export is something the browser may assert',
  A.isClientAction('data.export-scripts'));
check('a role assignment is NOT something the browser may assert',
  !A.isClientAction('role.assign'));
check('a SQL write is NOT something the browser may assert',
  !A.isClientAction('sql.write'));
check('an unknown action is refused rather than waved through',
  !A.isClientAction('anything.at.all'));
check('every allowlisted action names a real category',
  Object.values(A.CLIENT_ACTIONS).every(c => A.CATEGORY_KEYS.indexOf(c) !== -1));

// ── Redaction ─────────────────────────────────────────────────────────────
console.log('\nRedaction');

check('a field called password is redacted',
  A.redact({ password: 'hunter2' }).password === A.REDACTED);
check('a field called apiKey is redacted',
  A.redact({ apiKey: 'sk-live-abc' }).apiKey === A.REDACTED);
check('a field called clientSecret is redacted',
  A.redact({ clientSecret: 'abc' }).clientSecret === A.REDACTED);
check('a field called idToken is redacted',
  A.redact({ idToken: 'eyJ...' }).idToken === A.REDACTED);
check('a field called connectionString is redacted',
  A.redact({ connectionString: 'Server=x' }).connectionString === A.REDACTED);
check('a field called connStr is redacted',
  A.redact({ connStr: 'Server=x' }).connStr === A.REDACTED);
check('a field called credentials is redacted',
  A.redact({ credentials: 'x' }).credentials === A.REDACTED);
check('an innocent field survives untouched',
  A.redact({ batchSize: 5000 }).batchSize === 5000);

check('a secret nested three levels down is still redacted',
  A.redact({ a: { b: { sqlPassword: 'x' } } }).a.b.sqlPassword === A.REDACTED);
check('secrets inside an array of objects are redacted',
  A.redact({ conns: [{ name: 'a', password: 'p1' }, { name: 'b', password: 'p2' }] })
    .conns.every(c => c.password === A.REDACTED));
check('an array under a secret-named key has every member redacted',
  A.redact({ tokens: ['a', 'b'] }).tokens.every(v => v === A.REDACTED));
check('a numeric secret is redacted too (a PIN is not safe for being a number)',
  A.redact({ passcode: 1234 }).passcode === A.REDACTED);
check('structure survives redaction — keys are kept, values replaced',
  Object.keys(A.redact({ password: 'x', user: 'sa' })).join(',') === 'password,user');
check('a cycle terminates rather than hanging the function',
  (() => { const o = { name: 'x' }; o.self = o; return A.redact(o).self === '(circular)'; })());

// The dangerous case: the FIELD name is innocent and the secret is inside
// the value. Nothing about a key called `target` says "credential".
console.log('\nRedaction — secrets hiding inside innocent fields');

check('an ADO connection string in a field called target loses its password',
  !/hunter2/.test(A.redact({ target: 'Server=db1;Database=x;User Id=sa;Password=hunter2;' }).target));
check('the same string keeps the parts an auditor needs',
  /Server=db1/.test(A.redact({ target: 'Server=db1;Database=x;User Id=sa;Password=hunter2;' }).target));
check('the pwd= short form is caught',
  !/s3cret/.test(A.redactConnectionString('server=x;uid=sa;pwd=s3cret;')));
check('a braced password value is caught',
  !/w;th;semis/.test(A.redactConnectionString('Server=x;Pwd={w;th;semis};')));
check('a postgres URI loses its password but keeps its user and host',
  (() => { const r = A.redactConnectionString('postgres://appuser:letmein@db.example.com:5432/sales');
           return !/letmein/.test(r) && /appuser/.test(r) && /db\.example\.com/.test(r); })());
check('an Azure storage AccountKey is stripped',
  !/AbC123==/.test(A.redactConnectionString(
    'DefaultEndpointsProtocol=https;AccountName=cyg;AccountKey=AbC123==;')));
check('a SAS signature is stripped',
  !/deadbeef/.test(A.redactConnectionString('BlobEndpoint=https://x;SharedAccessSignature=sig=deadbeef;')));
check('an ordinary sentence is not mangled by the connection-string rules',
  A.redact({ summary: 'Changed batch size 5,000 to 10,000' }).summary
    === 'Changed batch size 5,000 to 10,000');

// ── Diffs ─────────────────────────────────────────────────────────────────
console.log('\nBefore/after diffs');

const d = A.diff({ batchSize: 5000, timeout: 30, name: 'x' },
                 { batchSize: 10000, timeout: 30, name: 'x' });
check('only changed fields appear', d.length === 1 && d[0].field === 'batchSize');
check('the diff carries both sides', d[0].before === 5000 && d[0].after === 10000);
check('an unchanged object produces no rows',
  A.diff({ a: 1 }, { a: 1 }).length === 0);
check('a field added from nothing shows before: null',
  (() => { const r = A.diff({}, { a: 1 }); return r.length === 1 && r[0].before === null; })());
check('a field restricted by the fields argument is the only one compared',
  A.diff({ a: 1, b: 1 }, { a: 2, b: 2 }, ['a']).length === 1);
check('a type change is a change (5000 is not "5000")',
  A.diff({ a: 5000 }, { a: '5000' }).length === 1);

// A password change must be VISIBLE as a change and OPAQUE as a value.
// Suppressing the row entirely would hide that a credential was rotated,
// which is itself the auditable fact.
const dp = A.diff({ password: 'old' }, { password: 'new' });
check('a changed password still produces a diff row', dp.length === 1);
check('but both sides of it are redacted',
  dp[0].before === A.REDACTED && dp[0].after === A.REDACTED);

// A caller can hand in a changes array it built itself. The field name then
// lives in `row.field` rather than in the key holding the value, so a plain
// deep redact would test the literal keys "before"/"after" and wave a
// password straight through. redactChanges is what stops that.
const rc = A.redactChanges([{ field: 'apiKey', before: 'sk-old', after: 'sk-new' }]);
check('redactChanges reads the field name out of row.field',
  rc[0].before === A.REDACTED && rc[0].after === A.REDACTED);
check('and leaves an innocent field alone',
  A.redactChanges([{ field: 'batchSize', before: 1, after: 2 }])[0].after === 2);
check('a missing side becomes null rather than undefined (JSON drops undefined)',
  A.redactChanges([{ field: 'a', after: 2 }])[0].before === null);

// ── Identifiers ───────────────────────────────────────────────────────────
console.log('\nIdentifiers');

check('a ulid is 26 characters of Crockford base32',
  /^[0-9A-HJKMNP-TV-Z]{26}$/.test(A.ulid()));
check('ulids sort in time order',
  A.ulid(1000000000000) < A.ulid(2000000000000));
check('two ulids in the same millisecond still differ',
  A.ulid() !== A.ulid());

// ── Entry construction ────────────────────────────────────────────────────
console.log('\nEntry construction');

const ctx = {
  actor: { oid: 'oid-1', email: 'lead@example.com', name: 'A Lead', roles: ['ML'] },
  tenantId: 'ten-1', route: 'audit', ip: '203.0.113.7', userAgent: 'UA/1.0',
  now: Date.parse('2026-09-11T13:38:00.000Z'),
};

const e = A.buildEntry({
  action: 'sysparam.update', category: 'settings',
  target: { type: 'system_parameter', id: 'batch_size', label: 'System Parameters > Batch size' },
  environment: 'DEV', outcome: 'allowed',
  changes: [{ field: 'batchSize', before: 5000, after: 10000 }],
  summary: 'Changed batch size 5,000 to 10,000',
}, ctx);

check('every field the existing chain carries is still present',
  ['occurredAt', 'actorOid', 'actorEmail', 'effectiveRoles', 'action',
   'resourceType', 'resourceId', 'environment', 'outcome', 'severity', 'detail']
    .every(k => Object.prototype.hasOwnProperty.call(e, k)));
check('occurredAt is not renamed to ts (entries already in the chain use it)',
  e.occurredAt === '2026-09-11T13:38:00.000Z' && e.ts === undefined);
check('the actor comes from the context, not the input',
  e.actorEmail === 'lead@example.com' && e.actorOid === 'oid-1');
check('effective roles are recorded as at the act',
  JSON.stringify(e.effectiveRoles) === JSON.stringify(['ML']));
check('a target is projected onto resourceType/resourceId for existing readers',
  e.resourceType === 'system_parameter' && e.resourceId === 'batch_size');
check('and kept whole for new ones', e.target.label.indexOf('Batch size') !== -1);
check('context carries ip, user agent and route',
  e.context.ip === '203.0.113.7' && e.context.userAgent === 'UA/1.0' && e.context.route === 'audit');
check('a server-side entry is marked source: server', e.source === 'server');
check('actorType defaults to user', e.actorType === 'user');
check('onBehalfOf is null for a human actor', e.onBehalfOf === null);
check('a DEV allowed act is info severity', e.severity === 'info');

// An actor supplied in the BODY is the attack this drops on the floor.
const spoof = A.buildEntry({
  action: 'jobs.reorder',
  actorOid: 'oid-victim', actorEmail: 'someone.else@example.com',
  actor: { email: 'someone.else@example.com' },
  effectiveRoles: ['OW'],
}, ctx);
check('an actorEmail in the request body is ignored',
  spoof.actorEmail === 'lead@example.com');
check('an actorOid in the request body is ignored',
  spoof.actorOid === 'oid-1');
check('a roles claim in the request body is ignored',
  JSON.stringify(spoof.effectiveRoles) === JSON.stringify(['ML']));

// Ask Cygenix acts for a person. Both facts are recorded, in two fields,
// so they can never silently become one.
const asst = A.buildEntry({ action: 'mapping.ai-apply', actorType: 'assistant' }, ctx);
check('an assistant act records actor.type = assistant', asst.actorType === 'assistant');
check('and names the person it acted for', asst.onBehalfOf === 'lead@example.com');

const prod = A.buildEntry({ action: 'sysparam.update', category: 'settings', environment: 'PROD' }, ctx);
check('a PROD act is forced into the always-on prod category even when the caller said settings',
  prod.category === 'prod');
check('a PROD allowed act is high severity', prod.severity === 'high');

const bad = A.buildEntry({ action: 'x.y', outcome: 'banana', environment: 'QA' }, ctx);
check('an unknown outcome falls back to allowed rather than storing nonsense',
  bad.outcome === 'allowed');
check('an environment outside the four classifications is stored as null',
  bad.environment === null);

// Redaction is applied on the way IN, before hashing — not on the way out.
const secretive = A.buildEntry({
  action: 'connection.edit',
  detail: { server: 'db1', password: 'hunter2' },
  changes: [{ field: 'connectionString', before: 'Server=a;Pwd=old;', after: 'Server=a;Pwd=new;' }],
}, ctx);
check('a secret in detail is redacted before the entry is built',
  secretive.detail.password === A.REDACTED);
check('and a secret in a change row with it',
  secretive.changes[0].before === A.REDACTED);
check('no part of the built entry contains the plaintext secret',
  JSON.stringify(secretive).indexOf('hunter2') === -1);

// ── Index rows ────────────────────────────────────────────────────────────
console.log('\nIndex rows');

const row = A.indexRow(e, 42);
check('an index row carries the sequence it points at', row.seq === 42);
check('it carries the fields every filter needs',
  row.a === 'lead@example.com' && row.c === 'settings' && row.o === 'allowed' && row.e === 'DEV');
check('the search text is pre-lowered so queries do not lower every row',
  row.t === row.t.toLowerCase());
check('the search text spans action, actor, summary and target',
  row.t.indexOf('sysparam.update') !== -1 && row.t.indexOf('batch size') !== -1);
check('an index row stays small (a query reads thousands of them)',
  JSON.stringify(row).length < 400);
check('the index key is the entry\'s own month',
  A.indexKeyFor('2026-09-11T13:38:00.000Z') === 'audit/idx/2026-09');

// ── Retention checkpoint ──────────────────────────────────────────────────
console.log('\nRetention checkpoint');

const cp = A.checkpoint({ seq: 500, entryHash: 'abc', occurredAt: '2026-01-01T00:00:00.000Z' }, 499, 365);
check('a checkpoint anchors on a surviving entry, not on the empty string',
  cp.anchorSeq === 500 && cp.anchorHash === 'abc');
check('it records how many entries the purge removed', cp.purgedCount === 499);
check('it names the boundary verification must start after',
  cp.purgedThroughSeq === 499);
check('it is versioned, so the format can change without silent misreads',
  cp.version === 1);

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
