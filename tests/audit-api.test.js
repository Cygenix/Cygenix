// tests/audit-api.test.js — the audit endpoints, driven through the real
// handler with a faked world underneath.
//
// This file exists because the interesting claims in the brief are about
// what the API REFUSES, and a refusal is not provable from the source. So
// the handler is loaded for real — its authorisation, its validation, its
// hashing, its storage — with only the token verifier and the blob store
// swapped out. Every acceptance criterion below is exercised by calling the
// endpoint the way a browser would, and checking the status code and the
// bytes that came back.
//
// The role set is the product's own. There is no role called "Admin": OW
// and PA configure the log, AU reads all of it and configures none of it,
// and the delivery roles (ML/EN/…) hold the matrix's 'L' grant, which
// resolves to their own entries only.

'use strict';

const path = require('path');
const Module = require('module');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

const FN_DIR = path.join(__dirname, '..', 'netlify', 'functions');

// ── The faked world ───────────────────────────────────────────────────────

let ACTOR = { oid: 'oid-1', email: 'owner@acme.test', name: 'An Owner', roles: ['OW', 'PA'] };
let TOKEN_OK = true;

const CLASSIFICATIONS = {};
const MEM = new Map();
const BLOBS = {
  get: async (k) => (MEM.has(k) ? JSON.parse(MEM.get(k)) : null),
  setJSON: async (k, v) => { MEM.set(k, JSON.stringify(v)); },
  set: async (k, v) => { MEM.set(k, JSON.stringify(String(v))); },
  delete: async (k) => { MEM.delete(k); },
  list: async () => ({ blobs: [...MEM.keys()].map(key => ({ key })) }),
};

const realRequire = Module.prototype.require;
Module.prototype.require = function (id) {
  if (id === '@netlify/blobs') return { getStore: () => BLOBS };
  if (id === './lib/entra-auth' || id === './entra-auth') return {
    verifyAuthHeader: async (event) => {
      const h = (event && event.headers) || {};
      if (!(h.authorization || h.Authorization) || !TOKEN_OK) throw new Error('invalid token');
      return { oid: ACTOR.oid, sub: ACTOR.oid, email: ACTOR.email, name: ACTOR.name };
    },
  };
  if (id === './lib/org-store' || id === './org-store') {
    const real = realRequire.call(this, path.join(FN_DIR, 'lib', 'org-store.js'));
    // Everything about the audit chain is REAL — the point of this file is
    // that hashing, chaining, indexing and the drop decision run. Only the
    // directory lookup is swapped, so the actor's roles are controllable.
    return Object.assign({}, real, {
      orgStore: () => BLOBS,
      resolveActor: async () => ({
        oid: ACTOR.oid, email: ACTOR.email, name: ACTOR.name,
        roles: ACTOR.roles.slice(), isActive: true, user: { isActive: true },
      }),
      // classifications is returned BY REFERENCE so a handler writing into it
      // (rbac-admin's classify op does) is visible on the next read. A fresh
      // {} each call would quietly make every classification look like the
      // first one, and the before/after this file checks would be a fiction.
      loadAll: async () => ({
        users: { [ACTOR.oid]: { email: ACTOR.email, isActive: true } },
        assignments: ACTOR.roles.map(r => ({ id: 'ra_' + r, oid: ACTOR.oid, role: r, revokedAt: null })),
        classifications: CLASSIFICATIONS,
      }),
      invalidate: () => {},
    });
  }
  return realRequire.apply(this, arguments);
};

process.env.NETLIFY_SITE_ID = 'site-test';
process.env.NETLIFY_API_TOKEN = 'token-test';

const handler = require(path.join(FN_DIR, 'audit.js')).handler;
const org = require(path.join(FN_DIR, 'lib', 'org-store.js'));
const tenancy = require(path.join(FN_DIR, 'lib', 'tenancy.js'));

function as(roles, email) {
  ACTOR = { oid: 'oid-' + (email || roles.join('')), email: email || (roles.join('').toLowerCase() + '@acme.test'),
            name: roles.join('+'), roles: roles.slice() };
  org.invalidateAuditConfig();
  tenancy.invalidate();
}

async function call(ev) {
  const res = await handler(Object.assign(
    { httpMethod: 'GET', headers: { authorization: 'Bearer valid', 'user-agent': 'Test/1.0',
                                    'x-nf-client-connection-ip': '203.0.113.7' },
      queryStringParameters: {} }, ev));
  let json = null;
  try { json = JSON.parse(res.body); } catch {}
  return { status: res.statusCode, json, headers: res.headers, raw: res.body };
}

const get  = (qs) => call({ httpMethod: 'GET', queryStringParameters: qs });
const post = (body) => call({ httpMethod: 'POST', body: JSON.stringify(body) });

const is2xx = (r) => r.status >= 200 && r.status < 300;

(async () => {
  console.log('Audit API — the endpoints, and what they refuse\n');

  // ── Access control ──────────────────────────────────────────────────────
  section('1. Who may read, configure and export');

  as(['OW', 'PA']);
  check('an Organisation Owner reads the status', is2xx(await get({ what: 'status' })));
  check('and is told they may configure capture',
    (await get({ what: 'status' })).json.canConfigure === true);

  as(['AU']);
  const auditorStatus = await get({ what: 'status' });
  check('an Auditor reads the whole organisation trail, not a slice',
    auditorStatus.json.scope === 'organisation');
  check('but may not configure it — independence is the point of the role',
    auditorStatus.json.canConfigure === false);
  check('an Auditor attempting a pause is refused',
    (await post({ op: 'status', state: 'paused', reason: 'testing', pauseMinutes: 30 })).status === 403);
  check('an Auditor may export', auditorStatus.json.canExport === true);

  as(['EN']);
  const engStatus = await get({ what: 'status' });
  check('an Engineer gets their own slice, not the organisation\'s',
    engStatus.json.scope === 'self');
  check('and is refused chain verification, which is a whole-chain claim',
    (await get({ what: 'verify' })).status === 403);
  check('and is refused configuration',
    (await post({ op: 'status', state: 'off', reason: 'testing', confirm: 'OFF' })).status === 403);

  as(['MB']);
  check('a baseline Member may not export the trail',
    (await get({ what: 'export' })).status === 403);

  as([]);
  check('a signed-in caller with no role assignment reads nothing (A-01)',
    (await get({ what: 'status' })).status === 403);
  check('and cannot record either',
    (await post({ op: 'record', action: 'jobs.reorder' })).status === 403);

  TOKEN_OK = false;
  check('no token, no service', (await get({ what: 'status' })).status === 401);
  TOKEN_OK = true;

  // A denied view is itself an event (brief Section 2).
  as(['OW', 'PA']);
  const afterDenials = await get({ what: 'events', category: 'audit', limit: 100 });
  check('every refused view was recorded as audit.view.denied',
    afterDenials.json.entries.some(e => e.action === 'audit.view.denied'));
  check('and the refusal names what was attempted',
    afterDenials.json.entries.some(e => e.action === 'audit.view.denied'
      && e.detail && e.detail.attempted === 'audit.configure'));

  // ── Recording ───────────────────────────────────────────────────────────
  section('2. Recording an event');

  as(['ML'], 'lead@acme.test');
  const rec = await post({
    op: 'record', action: 'sysparam.update', environment: 'DEV',
    target: { type: 'system_parameter', id: 'batch_size', label: 'System Parameters > Batch size' },
    changes: [{ field: 'batchSize', before: 5000, after: 10000 }],
    summary: 'Changed batch size 5,000 to 10,000',
  });
  check('a signed-in member may record a client-side event', is2xx(rec));
  check('and gets the stable id back', /^[0-9A-HJKMNP-TV-Z]{26}$/.test(rec.json.id));

  as(['OW', 'PA']);
  const evs = await get({ what: 'events', action: 'sysparam.update' });
  const e = evs.json.entries[0];
  check('the event is readable back', !!e);
  check('with the actor taken from the token, not the body',
    e.actorEmail === 'lead@acme.test');
  check('the before and after are both there',
    e.changes[0].before === 5000 && e.changes[0].after === 10000);
  check('the environment is recorded', e.environment === 'DEV');
  check('the caller\'s IP is in the context', e.context.ip === '203.0.113.7');
  check('and it is marked as a claim the browser made, not a fact the server saw',
    e.source === 'client');

  // The allowlist: the browser cannot assert an act it does not perform.
  as(['ML'], 'lead@acme.test');
  check('the browser may not assert a role assignment',
    (await post({ op: 'record', action: 'role.assign', target: { id: 'oid-9' } })).status === 400);
  check('nor a SQL write',
    (await post({ op: 'record', action: 'sql.write' })).status === 400);
  check('nor an invented action',
    (await post({ op: 'record', action: 'wibble.frobnicate' })).status === 400);

  as(['OW', 'PA']);
  check('but the refusal is itself recorded',
    (await get({ what: 'events', action: 'audit.record.refused' })).json.total === 3);

  // Actor spoofing, through the wire this time.
  as(['EN'], 'engineer@acme.test');
  await post({ op: 'record', action: 'jobs.reorder',
               actorEmail: 'owner@acme.test', actorOid: 'oid-1',
               effectiveRoles: ['OW'], summary: 'Reordered a job' });
  as(['OW', 'PA']);
  const spoofed = (await get({ what: 'events', action: 'jobs.reorder' })).json.entries[0];
  check('an actorEmail in the request body is ignored',
    spoofed.actorEmail === 'engineer@acme.test');
  check('a roles claim in the request body is ignored',
    JSON.stringify(spoofed.effectiveRoles) === JSON.stringify(['EN']));

  // Secrets, through the wire.
  as(['ML'], 'lead@acme.test');
  await post({ op: 'record', action: 'connection.test',
               detail: { server: 'db1', password: 'hunter2' },
               changes: [{ field: 'connectionString',
                           before: 'Server=a;Pwd=old;', after: 'Server=a;Pwd=new;' }] });
  check('no plaintext secret reaches the stored blob',
    [...MEM.values()].join(' ').indexOf('hunter2') === -1);

  // ── Capture state ───────────────────────────────────────────────────────
  section('3. Pause, off, and what survives them');

  as(['OW', 'PA']);
  check('a pause with no reason is refused',
    (await post({ op: 'status', state: 'paused', pauseMinutes: 60 })).status === 400);
  check('a pause with no duration is refused',
    (await post({ op: 'status', state: 'paused', reason: 'bulk import' })).status === 400);
  check('a pause beyond the ceiling is refused',
    (await post({ op: 'status', state: 'paused', reason: 'bulk import', pauseMinutes: 600 })).status === 400);

  const paused = await post({ op: 'status', state: 'paused', reason: 'bulk import', pauseMinutes: 120 });
  check('a pause with a reason and a duration is accepted', is2xx(paused));
  check('and reports when it ends', !!paused.json.pausedUntil);
  check('the status now says paused', (await get({ what: 'status' })).json.state === 'paused');

  // The acceptance criterion, end to end.
  as(['ML'], 'lead@acme.test');
  const duringPause = await post({ op: 'record', action: 'sysparam.update',
                                   summary: 'A settings change during the pause' });
  check('while paused, a settings change is NOT recorded',
    duringPause.json.recorded === false);
  check('and the caller is told why, rather than being told it worked',
    /paused/i.test(duringPause.json.reason));

  const prodDuringPause = await post({ op: 'record', action: 'sysparam.update',
                                       environment: 'PROD', summary: 'A PROD change during the pause' });
  check('while paused, a PROD change IS recorded',
    prodDuringPause.json.recorded === true);

  as(['OW', 'PA']);
  const roleDuringPause = await org.appendAudit(BLOBS, { action: 'role.assign', outcome: 'allowed',
    actorEmail: 'owner@acme.test', effectiveRoles: ['OW'] });
  check('while paused, a role change IS recorded', !!roleDuringPause.entryHash);
  const signinDuringPause = await org.appendAudit(BLOBS, { action: 'auth.signin', outcome: 'allowed',
    actorEmail: 'owner@acme.test', effectiveRoles: ['OW'] });
  check('while paused, a sign-in IS recorded', !!signinDuringPause.entryHash);

  check('the pause is itself in the log',
    (await get({ what: 'events', action: 'audit.pause' })).json.total === 1);
  check('with the reason somebody gave for it',
    (await get({ what: 'events', action: 'audit.pause' })).json.entries[0].detail.reason === 'bulk import');
  check('the status card can draw the gap window',
    (await get({ what: 'status' })).json.gaps.some(g => g.kind === 'paused' && g.open === true));

  // Resume.
  check('resuming is accepted', is2xx(await post({ op: 'status', state: 'recording' })));
  check('and the status says recording again',
    (await get({ what: 'status' })).json.state === 'recording');
  check('resuming again is refused rather than logged as a no-op',
    (await post({ op: 'status', state: 'recording' })).status === 400);

  // Off.
  check('turning capture off needs a reason',
    (await post({ op: 'status', state: 'off', confirm: 'OFF' })).status === 400);
  check('turning capture off needs OFF typed out',
    (await post({ op: 'status', state: 'off', reason: 'decommissioning' })).status === 400);
  const off = await post({ op: 'status', state: 'off', reason: 'decommissioning', confirm: 'OFF' });
  check('reason plus OFF is accepted', is2xx(off));
  // send-email.js has no server-held SMTP credentials, so the brief's
  // "emails every admin" cannot be honoured today. Reported, not faked.
  check('the response states plainly that administrators were not emailed',
    off.json.notify && off.json.notify.adminsEmailed === false);
  check('and says why', /SMTP/i.test(off.json.notify.reason));

  as(['ML'], 'lead@acme.test');
  check('while off, a job event is not recorded',
    (await post({ op: 'record', action: 'jobs.reorder' })).json.recorded === false);
  as(['OW', 'PA']);
  check('while off, the log can still be read', is2xx(await get({ what: 'status' })));
  check('off has no end time — it stays off until somebody turns it on',
    (await get({ what: 'status' })).json.pausedUntil === null);
  check('turning it back on writes audit.enable',
    (await post({ op: 'status', state: 'recording' })).json.state === 'recording' &&
    (await get({ what: 'events', action: 'audit.enable' })).json.total === 1);

  // ── Settings ────────────────────────────────────────────────────────────
  section('4. Settings, and the categories that cannot be switched off');

  as(['OW', 'PA']);
  for (const locked of ['security', 'access', 'prod', 'audit']) {
    check('the API refuses to disable the ' + locked + ' category',
      (await post({ op: 'settings', categories: { [locked]: false } })).status === 400);
  }
  check('every locked refusal is recorded at high severity',
    (await get({ what: 'events', action: 'audit.settings.refused' })).json.entries
      .every(e => e.severity === 'high'));

  const set = await post({ op: 'settings', categories: { mapping: false }, retentionDays: 90 });
  check('an optional category can be turned off', is2xx(set));
  check('and the change is reflected back',
    set.json.settings.categories.mapping === false && set.json.settings.retentionDays === 90);
  check('the settings change is itself in the log',
    (await get({ what: 'events', action: 'audit.settings' })).json.total === 1);
  check('with a before/after diff',
    (await get({ what: 'events', action: 'audit.settings' })).json.entries[0]
      .changes.some(c => c.field === 'retentionDays' && c.after === 90));

  as(['ML'], 'lead@acme.test');
  check('with mapping disabled, a mapping event is dropped',
    (await post({ op: 'record', action: 'mapping.save' })).json.recorded === false);
  check('but a jobs event still lands',
    (await post({ op: 'record', action: 'jobs.validate' })).json.recorded === true);

  as(['OW', 'PA']);
  check('a retention value outside the three offered is refused',
    (await post({ op: 'settings', retentionDays: 4 })).status === 400);
  check('an unchanged settings write is a no-op, not a log entry',
    (await post({ op: 'settings', retentionDays: 90 })).json.unchanged === true);
  check('PUT works as well as POST for settings',
    is2xx(await call({ httpMethod: 'PUT', body: JSON.stringify({ retentionDays: 365 }) })));

  // ── Verify ──────────────────────────────────────────────────────────────
  section('5. Verification');

  const ver = await get({ what: 'verify' });
  check('the chain verifies after everything above', ver.json.ok === true);
  check('over a non-trivial number of entries', ver.json.count > 20);
  check('and the verification is itself recorded',
    (await get({ what: 'events', action: 'audit.verify' })).json.total >= 1);

  // Tamper with one row in place, exactly as the brief asks.
  const head = JSON.parse(MEM.get('audit/head'));
  const victimKey = 'audit/e/' + String(Math.max(2, head.seq - 3)).padStart(10, '0');
  const victim = JSON.parse(MEM.get(victimKey));
  MEM.set(victimKey, JSON.stringify({ ...victim, summary: 'quietly edited' }));
  const broken = await get({ what: 'verify' });
  check('editing a stored row is detected', broken.json.ok === false);
  check('and the first break is named', broken.json.brokenAt === victim.seq);
  MEM.set(victimKey, JSON.stringify(victim));
  check('restoring it makes the chain verify again', (await get({ what: 'verify' })).json.ok === true);

  // ── Export ──────────────────────────────────────────────────────────────
  section('6. Export');

  const filtered = await get({ what: 'events', action: 'jobs.validate' });
  const exported = await get({ what: 'export', action: 'jobs.validate', format: 'json' });
  check('export returns the same rows as the matching filter',
    JSON.parse(exported.raw).count === filtered.json.total);
  check('it is served as a download',
    /attachment/.test(exported.headers['Content-Disposition']));
  check('and the export is itself in the log',
    (await get({ what: 'events', action: 'audit.export' })).json.total >= 1);
  check('recording which filter produced it',
    (await get({ what: 'events', action: 'audit.export' })).json.entries[0]
      .detail.filters.action === 'jobs.validate');

  const csv = await get({ what: 'export', format: 'csv' });
  check('CSV is served as text/csv', /text\/csv/.test(csv.headers['Content-Type']));
  check('with a BOM, so Excel reads UTF-8 rather than the system code page',
    csv.raw.charCodeAt(0) === 0xFEFF);
  check('CRLF line endings, per RFC 4180', csv.raw.indexOf('\r\n') !== -1);
  check('a header row naming the hash columns, so a recipient can re-verify',
    /prev_hash,hash/.test(csv.raw.split('\r\n')[0]));
  check('and no plaintext secret in the export either',
    csv.raw.indexOf('hunter2') === -1);

  // ── Paging and filters ──────────────────────────────────────────────────
  section('7. Paging and filters');

  const p1 = await get({ what: 'events', limit: '5' });
  check('a page honours its limit', p1.json.entries.length === 5);
  check('newest first', Date.parse(p1.json.entries[0].occurredAt)
    >= Date.parse(p1.json.entries[4].occurredAt));
  check('and hands back a cursor', !!p1.json.nextCursor);
  const p2 = await get({ what: 'events', limit: '5', cursor: String(p1.json.nextCursor) });
  check('the cursor continues rather than repeating',
    p2.json.entries[0].seq < p1.json.entries[4].seq);

  check('free text search matches the summary',
    (await get({ what: 'events', q: 'batch size' })).json.total >= 1);
  check('filtering by outcome works',
    (await get({ what: 'events', outcome: 'denied' })).json.entries
      .every(x => x.outcome === 'denied'));
  check('filtering by environment works',
    (await get({ what: 'events', env: 'PROD' })).json.entries
      .every(x => x.environment === 'PROD'));

  // A delivery role's slice is not a filter they chose, so it cannot be
  // widened by asking for somebody else.
  as(['EN'], 'engineer@acme.test');
  const mine = await get({ what: 'events', actor: 'owner@acme.test' });
  check('an Engineer asking for the Owner\'s entries still gets only their own',
    mine.json.entries.every(x => x.actorEmail === 'engineer@acme.test'));

  // ── The classification hole ─────────────────────────────────────────────
  section('9. Downgrading a connection\'s classification');
  //
  // Classification decides whether the Production guardrails apply to a
  // target at all, so downgrading one from PROD to DEV is an authorisation
  // change wearing a connection's clothes. Filed under the optional
  // `connections` category it could be dropped during a pause — which is
  // exactly the pause somebody would take first. It belongs in always-on
  // `access`. The upgrade direction was already safe, because environment
  // PROD files an event under the always-on `prod` category; this is the
  // other direction.
  {
    const rbacAdmin = require(path.join(FN_DIR, 'rbac-admin.js')).handler;
    as(['OW', 'PA']);
    const classify = (environment) => rbacAdmin({
      httpMethod: 'POST', headers: { authorization: 'Bearer valid' },
      queryStringParameters: {},
      body: JSON.stringify({ op: 'classify', server: 'crm-prod', database: 'sales', environment }),
    });

    await classify('PROD');
    // Pause capture, then downgrade. The event must survive.
    await post({ op: 'status', state: 'paused', reason: 'checking the hole', pauseMinutes: 30 });
    const res = await classify('DEV');
    check('the downgrade itself is allowed for an administrator', res.statusCode === 200);

    const found = await get({ what: 'events', action: 'connection.classify', limit: '20' });
    const downgrade = found.json.entries.filter(
      (e) => e.changes && e.changes.some((c) => c.after === 'DEV'))[0];
    check('a downgrade taken DURING a pause is still recorded', !!downgrade);
    check('because it is filed under the always-on access category, not connections',
      downgrade && downgrade.category === 'access');
    check('and it records what the classification was before',
      downgrade && downgrade.changes.some((c) => c.field === 'environment' && c.before === 'PROD'));

    await post({ op: 'status', state: 'recording' });
  }

  // ── Retention through the endpoint ──────────────────────────────────────
  section('10. Running retention on demand');
  //
  // The nightly job is tested against a real chain in audit-retention.test.js.
  // What matters HERE is the door: who may open it, and what the caller is
  // told.
  as(['AU']);
  check('an Auditor may not purge — it is the one operation that removes ' +
        'entries, so it belongs to the roles that answer for the tenant',
    (await post({ op: 'purge' })).status === 403);
  as(['EN']);
  check('nor a delivery role', (await post({ op: 'purge' })).status === 403);
  as([]);
  check('nor a caller with no role at all', (await post({ op: 'purge' })).status === 403);

  as(['OW', 'PA']);
  const purge = await post({ op: 'purge' });
  check('an Owner may run it', is2xx(purge));
  check('and on a chain with nothing aged out it purges nothing rather than erroring',
    purge.json.purged === 0);
  check('saying why', /aged out|empty|only remaining/.test(purge.json.reason || ''));
  check('and reporting the checkpoint state before and after',
    !!purge.json.checkpointBefore && !!purge.json.checkpoint);
  check('a chain that has never been purged says so',
    purge.json.checkpoint.purged === false && purge.json.checkpoint.chainStartsAt === 1);

  check('the status card carries the checkpoint, so the screen can say what ' +
        'retention has DONE rather than what it is set to',
    (await get({ what: 'status' })).json.checkpoint.purged === false);

  check('the archive-before-purge choice is a stored setting',
    (await get({ what: 'status' })).json.settings.archiveBeforePurge === true);
  const erase = await post({ op: 'settings', archiveBeforePurge: false });
  check('and switching to outright erasure is accepted', is2xx(erase));
  check('recorded as a settings change, so the decision is on the record',
    (await get({ what: 'events', action: 'audit.settings' })).json.entries
      .some((e) => (e.changes || []).some((c) => c.field === 'archiveBeforePurge')));
  await post({ op: 'settings', archiveBeforePurge: true });

  section('8. Bad input');
  as(['OW', 'PA']);
  check('an unknown query is a 400, not a 500',
    (await get({ what: 'wibble' })).status === 400);
  check('an unknown op is a 400', (await post({ op: 'wibble' })).status === 400);
  check('a malformed body is a 400',
    (await call({ httpMethod: 'POST', body: '{not json' })).status === 400);
  check('DELETE is not a method this endpoint has',
    (await call({ httpMethod: 'DELETE' })).status === 405);
  check('there is no way to edit or delete an entry through this API',
    (await post({ op: 'delete', seq: 1 })).status === 400 &&
    (await post({ op: 'edit', seq: 1 })).status === 400);

  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
